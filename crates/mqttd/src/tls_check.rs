//! Offline TLS material checks for `mqttd --check-tls` ([ADR 0081](../../../docs/adr/0081-admin-api.md) T12).
//!
//! `--check-config` proves the config parses and the builders accept it; it does not say
//! that the certificate expires on Friday, that the chain file lists the intermediate
//! before the leaf, or which names the leaf answers to. This module reads every TLS
//! material set the config names — the client listeners' `[tls]` and the cluster bus's
//! `[cluster.peer_tls]` — and reports one finding per check:
//!
//! - the files load, and the key matches the certificate (by building the same rustls
//!   acceptor/connector startup builds, so the verdict cannot differ from a boot);
//! - the chain is in order (each certificate issued by the next);
//! - every certificate's validity window: expired or not-yet-valid is a failure, less
//!   than [`EXPIRY_WARN_DAYS`] left is a warning;
//! - the leaf's subject CN and SANs, so "which names does this serve" is answered;
//! - a CA bundle is non-empty, and a CRL parses and is not past its `nextUpdate`.
//!
//! Nothing here binds, dials or writes.

use std::fmt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use mqtt_config::Config;
use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::FromDer;

/// A certificate with fewer days than this left is reported as a warning.
pub const EXPIRY_WARN_DAYS: i64 = 30;

const DAY_SECS: i64 = 86_400;

/// How serious a finding is. Any [`Level::Fail`] makes `--check-tls` exit non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// The check passed (or the line is informational).
    Ok,
    /// Works today, but needs attention (expiry soon, no SANs, stale CRL).
    Warn,
    /// Startup would fail, or clients will refuse the certificate.
    Fail,
}

/// One checked fact about one piece of TLS material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// The verdict.
    pub level: Level,
    /// Which setting the finding is about, e.g. `tls.cert` or `cluster.peer_tls.ca`.
    pub what: String,
    /// What was found.
    pub message: String,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tag = match self.level {
            Level::Ok => "[ ok ]",
            Level::Warn => "[warn]",
            Level::Fail => "[FAIL]",
        };
        write!(f, "{tag} {}: {}", self.what, self.message)
    }
}

/// Collects findings for one run.
#[derive(Default)]
struct Report(Vec<Finding>);

impl Report {
    fn push(&mut self, level: Level, what: &str, message: impl Into<String>) {
        self.0.push(Finding {
            level,
            what: what.to_string(),
            message: message.into(),
        });
    }
}

/// Check every TLS material set `config` names, against the clock `now`.
///
/// A config with no TLS material yields a single informational finding.
#[must_use]
pub fn check_config(config: &Config, now: SystemTime) -> Vec<Finding> {
    let now = unix_secs(now);
    let mut r = Report::default();

    let tls = &config.tls;
    if tls.cert.is_some() || tls.key.is_some() {
        check_server_set(
            &mut r,
            "tls",
            tls.cert.as_deref(),
            tls.key.as_deref(),
            tls.client_ca.as_deref(),
            tls.crl.as_deref(),
            now,
        );
    }

    let peer = &config.cluster.peer_tls;
    if peer.ca.is_some() || peer.cert.is_some() || peer.key.is_some() {
        check_server_set(
            &mut r,
            "cluster.peer_tls",
            peer.cert.as_deref(),
            peer.key.as_deref(),
            peer.ca.as_deref(),
            peer.crl.as_deref(),
            now,
        );
        // The dialing side: the same material as a client (ADR 0002).
        if let (Some(ca), Some(cert), Some(key)) = (&peer.ca, &peer.cert, &peer.key) {
            match mqtt_net::tls::client_connector(Path::new(ca), Path::new(cert), Path::new(key)) {
                Ok(_) => r.push(
                    Level::Ok,
                    "cluster.peer_tls",
                    "dialing side builds (CA + client certificate + key)",
                ),
                Err(e) => r.push(
                    Level::Fail,
                    "cluster.peer_tls",
                    format!("dialing side: {e}"),
                ),
            }
        }
    }

    if r.0.is_empty() {
        r.push(
            Level::Ok,
            "tls",
            "no TLS material configured ([tls] and [cluster.peer_tls] are unset)",
        );
    }
    r.0
}

/// One server-side set: a certificate chain + key, with an optional client CA (mTLS)
/// and CRL. `prefix` names the config section in every finding.
fn check_server_set(
    r: &mut Report,
    prefix: &str,
    cert: Option<&str>,
    key: Option<&str>,
    ca: Option<&str>,
    crl: Option<&str>,
    now: i64,
) {
    let cert_what = format!("{prefix}.cert");
    let key_what = format!("{prefix}.key");
    let (Some(cert), Some(key)) = (cert, key) else {
        let missing = if cert.is_none() {
            &cert_what
        } else {
            &key_what
        };
        r.push(
            Level::Fail,
            missing,
            "unset, but the rest of the set is configured",
        );
        return;
    };

    let chain_ok = check_chain(r, &cert_what, cert, now);

    // Key ↔ certificate: exactly the acceptor startup builds, without the client CA so
    // a bad CA is reported against the CA, not the key.
    if chain_ok {
        match mqtt_net::tls::server_acceptor(Path::new(cert), Path::new(key), None) {
            Ok(_) => r.push(Level::Ok, &key_what, "loads and matches the certificate"),
            Err(e) => r.push(Level::Fail, &key_what, e.to_string()),
        }
    }

    if let Some(ca) = ca {
        // The setting's own name: `[tls] client_ca`, `[cluster.peer_tls] ca`.
        let ca_what = if prefix == "tls" {
            "tls.client_ca".to_string()
        } else {
            format!("{prefix}.ca")
        };
        check_ca_bundle(r, &ca_what, ca, now);
        if chain_ok {
            let built = mqtt_net::tls::server_acceptor_with_crl(
                Path::new(cert),
                Path::new(key),
                Some(Path::new(ca)),
                crl.map(Path::new),
            );
            if let Err(e) = built {
                r.push(
                    Level::Fail,
                    &ca_what,
                    format!("the mTLS acceptor refuses it: {e}"),
                );
            }
        }
    }
    if let Some(crl) = crl {
        let crl_what = format!("{prefix}.crl");
        if ca.is_none() {
            r.push(
                Level::Fail,
                &crl_what,
                "set without a client CA: a CRL only means something with mTLS",
            );
        }
        check_crl(r, &crl_what, crl, now);
    }
}

/// Parse a certificate chain file and check order, validity and the leaf's names.
/// Returns whether the file held at least one parseable certificate.
fn check_chain(r: &mut Report, what: &str, path: &str, now: i64) -> bool {
    let ders = match read_pem_blocks(path, "CERTIFICATE") {
        Ok(d) if d.is_empty() => {
            r.push(
                Level::Fail,
                what,
                format!("no certificates found in {path}"),
            );
            return false;
        }
        Ok(d) => d,
        Err(e) => {
            r.push(Level::Fail, what, e);
            return false;
        }
    };
    let mut certs = Vec::with_capacity(ders.len());
    for (i, der) in ders.iter().enumerate() {
        match X509Certificate::from_der(der) {
            Ok((_, c)) => certs.push(c),
            Err(e) => {
                r.push(
                    Level::Fail,
                    what,
                    format!("certificate #{} in {path} does not parse: {e}", i + 1),
                );
                return false;
            }
        }
    }
    r.push(
        Level::Ok,
        what,
        format!(
            "{path}: {} certificate(s), leaf subject {}",
            certs.len(),
            certs[0].subject()
        ),
    );

    // Names the leaf answers to. Modern TLS clients match SANs only; a CN-only leaf
    // is refused by most of them.
    let sans = subject_alt_names(&certs[0]);
    if sans.is_empty() {
        r.push(
            Level::Warn,
            what,
            "leaf has no subjectAltName; most TLS clients ignore the CN and will refuse it",
        );
    } else {
        r.push(Level::Ok, what, format!("leaf SANs: {}", sans.join(", ")));
    }

    for pair in certs.windows(2) {
        if pair[0].issuer() != pair[1].subject() {
            r.push(
                Level::Fail,
                what,
                format!(
                    "chain out of order: {} is not issued by the next certificate ({})",
                    pair[0].subject(),
                    pair[1].subject()
                ),
            );
        }
    }

    for (i, c) in certs.iter().enumerate() {
        let role = if i == 0 { "leaf" } else { "chain certificate" };
        check_validity(r, what, role, c, now);
    }
    true
}

/// A CA bundle: non-empty, parseable, and every CA inside its validity window.
fn check_ca_bundle(r: &mut Report, what: &str, path: &str, now: i64) {
    let ders = match read_pem_blocks(path, "CERTIFICATE") {
        Ok(d) => d,
        Err(e) => {
            r.push(Level::Fail, what, e);
            return;
        }
    };
    if ders.is_empty() {
        r.push(
            Level::Fail,
            what,
            format!("{path} holds no certificates: an empty trust store admits nobody"),
        );
        return;
    }
    r.push(
        Level::Ok,
        what,
        format!("{path}: {} CA certificate(s)", ders.len()),
    );
    for der in &ders {
        match X509Certificate::from_der(der) {
            Ok((_, c)) => check_validity(r, what, "CA", &c, now),
            Err(e) => r.push(
                Level::Fail,
                what,
                format!("a certificate does not parse: {e}"),
            ),
        }
    }
}

/// A CRL: parses, and is not past its `nextUpdate` (a stale CRL still loads, but
/// revocations published since are not in it).
fn check_crl(r: &mut Report, what: &str, path: &str, now: i64) {
    let ders = match read_pem_blocks(path, "X509 CRL") {
        Ok(d) => d,
        Err(e) => {
            r.push(Level::Fail, what, e);
            return;
        }
    };
    let Some(der) = ders.first() else {
        r.push(Level::Fail, what, format!("no CRL found in {path}"));
        return;
    };
    match x509_parser::revocation_list::CertificateRevocationList::from_der(der) {
        Ok((_, crl)) => {
            let revoked = crl.iter_revoked_certificates().count();
            r.push(
                Level::Ok,
                what,
                format!(
                    "{path}: issued by {}, {revoked} revoked serial(s)",
                    crl.issuer()
                ),
            );
            if let Some(next) = crl.next_update() {
                if next.timestamp() < now {
                    r.push(
                        Level::Warn,
                        what,
                        format!("past its nextUpdate ({next}): publish a fresh CRL"),
                    );
                }
            }
        }
        Err(e) => r.push(Level::Fail, what, format!("{path} does not parse: {e}")),
    }
}

fn check_validity(r: &mut Report, what: &str, role: &str, c: &X509Certificate<'_>, now: i64) {
    let v = c.validity();
    let subject = c.subject();
    if v.not_before.timestamp() > now {
        r.push(
            Level::Fail,
            what,
            format!("{role} {subject} is not valid until {}", v.not_before),
        );
        return;
    }
    let left = v.not_after.timestamp() - now;
    if left < 0 {
        r.push(
            Level::Fail,
            what,
            format!("{role} {subject} EXPIRED on {}", v.not_after),
        );
    } else if left < EXPIRY_WARN_DAYS * DAY_SECS {
        r.push(
            Level::Warn,
            what,
            format!(
                "{role} {subject} expires in {} day(s), on {}",
                left / DAY_SECS,
                v.not_after
            ),
        );
    } else {
        r.push(
            Level::Ok,
            what,
            format!(
                "{role} {subject} valid until {} ({} days)",
                v.not_after,
                left / DAY_SECS
            ),
        );
    }
}

fn subject_alt_names(c: &X509Certificate<'_>) -> Vec<String> {
    let Ok(Some(ext)) = c.subject_alternative_name() else {
        return Vec::new();
    };
    ext.value
        .general_names
        .iter()
        .map(|n| match n {
            GeneralName::DNSName(d) => format!("DNS:{d}"),
            GeneralName::IPAddress(bytes) => match bytes.len() {
                4 => format!(
                    "IP:{}",
                    std::net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])
                ),
                16 => {
                    let mut a = [0u8; 16];
                    a.copy_from_slice(bytes);
                    format!("IP:{}", std::net::Ipv6Addr::from(a))
                }
                _ => "IP:<malformed>".to_string(),
            },
            GeneralName::URI(u) => format!("URI:{u}"),
            GeneralName::RFC822Name(e) => format!("email:{e}"),
            other => format!("{other:?}"),
        })
        .collect()
}

/// Every PEM block labelled `label` in the file at `path`, as DER.
fn read_pem_blocks(path: &str, label: &str) -> Result<Vec<Vec<u8>>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("cannot read {path}: {e}"))?;
    let mut out = Vec::new();
    for pem in x509_parser::pem::Pem::iter_from_buffer(&bytes) {
        let pem = pem.map_err(|e| format!("{path} is not valid PEM: {e}"))?;
        if pem.label == label {
            out.push(pem.contents);
        }
    }
    Ok(out)
}

fn unix_secs(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{check_config, Level};
    use mqtt_config::Config;
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    struct Material {
        _dir: tempfile::TempDir,
        ca: String,
        cert: String,
        key: String,
        other_key: String,
    }

    /// A CA and a leaf it issued, valid from `not_before` to `not_after`.
    fn material(not_before: SystemTime, not_after: SystemTime) -> Material {
        let dir = tempfile::tempdir().unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-ca");
        let issuer = rcgen::CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut leaf = rcgen::CertificateParams::new(vec!["broker.example".into()]).unwrap();
        leaf.distinguished_name
            .push(rcgen::DnType::CommonName, "broker");
        leaf.not_before = not_before.into();
        leaf.not_after = not_after.into();
        let leaf_cert = leaf.signed_by(&leaf_key, &issuer).unwrap();

        let write = |name: &str, body: String| {
            let p = dir.path().join(name);
            std::fs::write(&p, body).unwrap();
            p.to_string_lossy().into_owned()
        };
        Material {
            ca: write("ca.pem", issuer.pem()),
            cert: write("cert.pem", leaf_cert.pem()),
            key: write("key.pem", leaf_key.serialize_pem()),
            other_key: write(
                "other.pem",
                rcgen::KeyPair::generate().unwrap().serialize_pem(),
            ),
            _dir: dir,
        }
    }

    fn config(m: &Material, key: &str) -> Config {
        let mut c = Config::default();
        c.tls.cert = Some(m.cert.clone());
        c.tls.key = Some(key.to_string());
        c.tls.client_ca = Some(m.ca.clone());
        c
    }

    const DAY: Duration = Duration::from_secs(86_400);

    #[test]
    fn valid_material_passes_and_reports_names() {
        let now = SystemTime::now();
        let m = material(now - DAY, now + 365 * DAY);
        let findings = check_config(&config(&m, &m.key), now);
        assert!(
            findings.iter().all(|f| f.level == Level::Ok),
            "{findings:#?}"
        );
        assert!(findings
            .iter()
            .any(|f| f.message.contains("DNS:broker.example")));
    }

    #[test]
    fn expired_and_expiring_leaves_are_flagged() {
        let now = SystemTime::now();
        let m = material(now - 30 * DAY, now - DAY);
        let findings = check_config(&config(&m, &m.key), now);
        assert!(
            findings
                .iter()
                .any(|f| f.level == Level::Fail && f.message.contains("EXPIRED")),
            "{findings:#?}"
        );

        let m = material(now - DAY, now + 10 * DAY);
        let findings = check_config(&config(&m, &m.key), now);
        assert!(
            findings
                .iter()
                .any(|f| f.level == Level::Warn && f.message.contains("expires in")),
            "{findings:#?}"
        );
        assert!(findings.iter().all(|f| f.level != Level::Fail));
    }

    #[test]
    fn a_key_that_does_not_match_fails() {
        let now = SystemTime::now();
        let m = material(now - DAY, now + 365 * DAY);
        let findings = check_config(&config(&m, &m.other_key), now);
        assert!(
            findings
                .iter()
                .any(|f| f.level == Level::Fail && f.what == "tls.key"),
            "{findings:#?}"
        );
    }

    #[test]
    fn a_chain_in_the_wrong_order_fails() {
        let now = SystemTime::now();
        let m = material(now - DAY, now + 365 * DAY);
        // CA first, then the leaf: each certificate must be issued by the next.
        let swapped = Path::new(&m.cert).with_file_name("swapped.pem");
        std::fs::write(
            &swapped,
            std::fs::read_to_string(&m.ca).unwrap() + &std::fs::read_to_string(&m.cert).unwrap(),
        )
        .unwrap();
        let mut c = config(&m, &m.key);
        c.tls.cert = Some(swapped.to_string_lossy().into_owned());
        let findings = check_config(&c, now);
        assert!(
            findings
                .iter()
                .any(|f| f.level == Level::Fail && f.message.contains("out of order")),
            "{findings:#?}"
        );
    }

    #[test]
    fn nothing_configured_is_a_single_ok_line() {
        let findings = check_config(&Config::default(), SystemTime::now());
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].level, Level::Ok);
    }
}
