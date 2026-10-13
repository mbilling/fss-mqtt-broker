//! Offline TLS material checks for `mqttd --check-tls` ([ADR 0081](../../../docs/adr/0081-admin-api.md) T12).
//!
//! `--check-config` proves the config parses and the builders accept it; it does not say
//! that the certificate expires on Friday, that the chain file lists the intermediate
//! before the leaf, or which names the leaf answers to. This module reads every TLS
//! material set the config names — the client listeners' `[tls]`, the cluster bus's
//! `[cluster.peer_tls]` and the admin listener's `[admin]` — and reports one finding per
//! check:
//!
//! - the files load, and the key matches the certificate (by building the same rustls
//!   acceptor/connector startup builds, so the verdict cannot differ from a boot);
//! - the chain is in order (each certificate issued by the next);
//! - every certificate's validity window: expired or not-yet-valid is a failure, and one
//!   that has spent `tls.expiry_warn_permille` of its lifetime is a warning — the same
//!   threshold the running broker warns at ([ADR 0088](../../../docs/adr/0088-credential-lifetimes-and-rotation.md) D2);
//! - the leaf's subject CN and SANs, so "which names does this serve" is answered;
//! - a CA bundle is non-empty, and a CRL parses, is not past its `nextUpdate`, and has
//!   not spent that same fraction of its `thisUpdate..nextUpdate` window.
//!
//! Nothing here binds, dials or writes.

use std::fmt;
use std::path::Path;
use std::time::SystemTime;

use mqtt_config::Config;
use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::FromDer;

use crate::tls_expiry::{self, Deciding, Kind};

/// The clock and the warning threshold one run checks against.
#[derive(Clone, Copy)]
struct Against {
    /// Now, in Unix seconds.
    now: i64,
    /// `tls.expiry_warn_permille`.
    warn_permille: u16,
}

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
    let warn_permille = config.tls.expiry_warn_permille;
    let now = Against {
        now: tls_expiry::unix_secs(now),
        warn_permille,
    };
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

    // The admin listener's set (ADR 0081): its certificate and key, and the CAs its
    // clients are verified against.
    let admin = &config.admin;
    if admin.cert.is_some() || admin.key.is_some() {
        check_server_set(
            &mut r,
            "admin",
            admin.cert.as_deref(),
            admin.key.as_deref(),
            admin.client_ca.as_deref(),
            None,
            now,
        );
    }

    if r.0.is_empty() {
        r.push(
            Level::Ok,
            "tls",
            "no TLS material configured ([tls], [cluster.peer_tls] and [admin] are unset)",
        );
    } else {
        // The threshold every expiry warning above was decided against (ADR 0088 D2).
        r.push(
            Level::Ok,
            "tls.expiry_warn_permille",
            format!(
                "{warn_permille}: a certificate, CA or CRL is warned about once {} of its \
                 lifetime is spent, here and by the running broker",
                tls_expiry::threshold_percent(warn_permille)
            ),
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
    now: Against,
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
        // The setting's own name: `[tls] client_ca`, `[admin] client_ca`,
        // `[cluster.peer_tls] ca`.
        let ca_what = if prefix == "cluster.peer_tls" {
            format!("{prefix}.ca")
        } else {
            format!("{prefix}.client_ca")
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
fn check_chain(r: &mut Report, what: &str, path: &str, now: Against) -> bool {
    let ders = match tls_expiry::read_pem_blocks(Path::new(path), "CERTIFICATE") {
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
fn check_ca_bundle(r: &mut Report, what: &str, path: &str, now: Against) {
    let ders = match tls_expiry::read_pem_blocks(Path::new(path), "CERTIFICATE") {
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
/// revocations published since are not in it) nor past the warning threshold of its
/// `thisUpdate..nextUpdate` window. Both are warnings: the broker starts with either.
fn check_crl(r: &mut Report, what: &str, path: &str, now: Against) {
    let ders = match tls_expiry::read_pem_blocks(Path::new(path), "X509 CRL") {
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
            match (tls_expiry::crl_window(&crl), crl.next_update()) {
                (Some(window), Some(next)) => {
                    let deciding = Deciding {
                        window,
                        what: "the CRL".to_string(),
                        until: next.to_string(),
                    };
                    if let Some(message) =
                        tls_expiry::describe(Kind::Crl, &deciding, now.warn_permille, now.now)
                    {
                        r.push(Level::Warn, what, format!("{message}: publish a fresh CRL"));
                    }
                }
                _ => r.push(
                    Level::Ok,
                    what,
                    "states no nextUpdate: its age is neither checked here nor exported \
                     by the broker",
                ),
            }
        }
        Err(e) => r.push(Level::Fail, what, format!("{path} does not parse: {e}")),
    }
}

/// One certificate's validity window: not yet valid or expired fails; past the warning
/// threshold of its lifetime warns, with the fraction spent and the time left.
fn check_validity(r: &mut Report, what: &str, role: &str, c: &X509Certificate<'_>, now: Against) {
    let v = c.validity();
    let subject = c.subject();
    let window = tls_expiry::certificate_window(c);
    if window.not_yet_begun(now.now) {
        r.push(
            Level::Fail,
            what,
            format!("{role} {subject} is not valid until {}", v.not_before),
        );
        return;
    }
    let deciding = Deciding {
        window,
        what: format!("{role} {subject}"),
        until: v.not_after.to_string(),
    };
    match tls_expiry::describe(Kind::Certificate, &deciding, now.warn_permille, now.now) {
        Some(message) if window.ended(now.now) => r.push(Level::Fail, what, message),
        Some(message) => r.push(Level::Warn, what, message),
        None => r.push(
            Level::Ok,
            what,
            format!(
                "{role} {subject} valid until {} ({} left, {} of its lifetime spent)",
                v.not_after,
                tls_expiry::span(window.seconds_left(now.now)),
                tls_expiry::percent(window.spent(now.now))
            ),
        ),
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

#[cfg(test)]
mod tests {
    use super::{check_config, Finding, Level};
    use crate::tls_expiry::testing::{at, ca, crl, intermediate, leaf, write, DAY, HOUR, T0};
    use mqtt_config::Config;

    type Issuer = rcgen::CertifiedIssuer<'static, rcgen::KeyPair>;

    /// A CA that is young at `T0` and outlives every test here.
    fn root() -> Issuer {
        ca("test-ca", T0 - 30 * DAY, T0 + 3650 * DAY)
    }

    /// `[tls]` with a leaf valid over `[from, to]`, issued by `issuer` and verified
    /// against `client_ca`.
    fn tls_config(dir: &std::path::Path, issuer: &Issuer, from: u64, to: u64) -> Config {
        let (cert, key) = leaf("broker", from, to, issuer);
        let mut c = Config::default();
        c.tls.cert = Some(write(dir, "cert.pem", &cert).display().to_string());
        c.tls.key = Some(write(dir, "key.pem", &key).display().to_string());
        c.tls.client_ca = Some(write(dir, "ca.pem", &issuer.pem()).display().to_string());
        c
    }

    fn has(findings: &[Finding], level: Level, what: &str, needle: &str) -> bool {
        findings
            .iter()
            .any(|f| f.level == level && f.what == what && f.message.contains(needle))
    }

    #[test]
    fn valid_material_passes_reports_names_and_states_the_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let c = tls_config(dir.path(), &root(), T0 - DAY, T0 + 364 * DAY);
        let findings = check_config(&c, at(T0));
        assert!(
            findings.iter().all(|f| f.level == Level::Ok),
            "{findings:#?}"
        );
        assert!(has(&findings, Level::Ok, "tls.cert", "DNS:broker.example"));
        assert!(
            has(
                &findings,
                Level::Ok,
                "tls.cert",
                "(364d 0h left, 0.3% of its lifetime spent)"
            ),
            "{findings:#?}"
        );
        // The threshold the warnings are decided against is said, once, last.
        let last = findings.last().unwrap();
        assert_eq!(last.what, "tls.expiry_warn_permille");
        assert!(
            last.message.starts_with("667:") && last.message.contains("66.7%"),
            "{last}"
        );
    }

    /// The threshold is a fraction of the lifetime, so it means the same for a
    /// certificate that lives a year and one that lives an hour: neither warns while
    /// young, both warn past two thirds, and an expired one fails.
    #[test]
    fn a_leaf_warns_past_the_threshold_of_its_own_lifetime_and_fails_once_expired() {
        let dir = tempfile::tempdir().unwrap();
        let ca = root();

        // One hour long. Ten minutes in: fine, though it has under an hour left.
        let c = tls_config(dir.path(), &ca, T0, T0 + HOUR);
        let findings = check_config(&c, at(T0 + 600));
        assert!(
            findings.iter().all(|f| f.level == Level::Ok),
            "{findings:#?}"
        );
        // Forty-five minutes in: three quarters spent.
        let findings = check_config(&c, at(T0 + 2_700));
        assert!(
            has(
                &findings,
                Level::Warn,
                "tls.cert",
                "leaf CN=broker has spent 75.0% of its lifetime (warning threshold 66.7%): \
                 it expires in 15m 0s"
            ),
            "{findings:#?}"
        );
        assert!(findings.iter().all(|f| f.level != Level::Fail));
        // A second past its end.
        let findings = check_config(&c, at(T0 + HOUR + 1));
        assert!(
            has(
                &findings,
                Level::Fail,
                "tls.cert",
                "leaf CN=broker EXPIRED 1s ago"
            ),
            "{findings:#?}"
        );

        // A year long, with seven months left: fine. With three: a warning.
        let c = tls_config(dir.path(), &ca, T0, T0 + 360 * DAY);
        assert!(check_config(&c, at(T0 + 150 * DAY))
            .iter()
            .all(|f| f.level == Level::Ok));
        let findings = check_config(&c, at(T0 + 270 * DAY));
        assert!(
            has(
                &findings,
                Level::Warn,
                "tls.cert",
                "has spent 75.0% of its lifetime (warning threshold 66.7%): it expires in 90d 0h"
            ),
            "{findings:#?}"
        );
        // Not yet valid still fails, as before.
        let findings = check_config(&c, at(T0 - DAY));
        assert!(
            has(&findings, Level::Fail, "tls.cert", "is not valid until"),
            "{findings:#?}"
        );
    }

    #[test]
    fn the_threshold_is_the_configured_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = tls_config(dir.path(), &root(), T0, T0 + 100 * DAY);
        let now = at(T0 + 80 * DAY);
        assert!(has(
            &check_config(&c, now),
            Level::Warn,
            "tls.cert",
            "has spent 80.0%"
        ));
        c.tls.expiry_warn_permille = 900;
        let findings = check_config(&c, now);
        assert!(
            findings.iter().all(|f| f.level == Level::Ok),
            "{findings:#?}"
        );
        assert!(has(
            &findings,
            Level::Ok,
            "tls.expiry_warn_permille",
            "90.0%"
        ));
    }

    /// A chain file is checked certificate by certificate: an intermediate that runs out
    /// before its leaf is named, and so is the older of two CAs in a bundle.
    #[test]
    fn an_intermediate_and_a_ca_nearer_their_end_than_the_leaf_are_named() {
        let dir = tempfile::tempdir().unwrap();
        let ca_root = root();
        let issuing = intermediate("issuing", T0 - 90 * DAY, T0 + 10 * DAY, &ca_root);
        let (cert, key) = leaf("broker", T0 - DAY, T0 + 30 * DAY, &issuing);
        let old_root = ca("old-root", T0 - 3000 * DAY, T0 + 100 * DAY);
        let mut c = Config::default();
        c.tls.cert = Some(
            write(dir.path(), "chain.pem", &format!("{cert}{}", issuing.pem()))
                .display()
                .to_string(),
        );
        c.tls.key = Some(write(dir.path(), "key.pem", &key).display().to_string());
        c.tls.client_ca = Some(
            write(
                dir.path(),
                "ca.pem",
                &format!("{}{}", ca_root.pem(), old_root.pem()),
            )
            .display()
            .to_string(),
        );
        let findings = check_config(&c, at(T0));
        assert!(
            has(
                &findings,
                Level::Warn,
                "tls.cert",
                "chain certificate CN=issuing has spent 90.0% of its lifetime"
            ),
            "{findings:#?}"
        );
        assert!(has(
            &findings,
            Level::Ok,
            "tls.cert",
            "leaf CN=broker valid until"
        ));
        assert!(
            has(
                &findings,
                Level::Warn,
                "tls.client_ca",
                "CA CN=old-root has spent 96.8% of its lifetime"
            ),
            "{findings:#?}"
        );
        assert!(has(
            &findings,
            Level::Ok,
            "tls.client_ca",
            "CA CN=test-ca valid until"
        ));
        assert!(findings.iter().all(|f| f.level != Level::Fail));
        // Once the old CA is over, the bundle fails.
        let findings = check_config(&c, at(T0 + 101 * DAY));
        assert!(
            has(
                &findings,
                Level::Fail,
                "tls.client_ca",
                "CA CN=old-root EXPIRED"
            ),
            "{findings:#?}"
        );
    }

    /// A revocation list nearing or past its `nextUpdate` is a warning, never a failure:
    /// the broker starts with it, and the exit status says so.
    #[test]
    fn a_revocation_list_warns_past_the_threshold_and_past_its_next_update() {
        let dir = tempfile::tempdir().unwrap();
        let ca = root();
        let mut c = tls_config(dir.path(), &ca, T0 - DAY, T0 + 364 * DAY);
        c.tls.crl = Some(
            write(dir.path(), "clients.crl", &crl(&ca, T0, T0 + 10 * DAY))
                .display()
                .to_string(),
        );
        let findings = check_config(&c, at(T0 + DAY));
        assert!(
            findings.iter().all(|f| f.level == Level::Ok),
            "{findings:#?}"
        );
        let findings = check_config(&c, at(T0 + 8 * DAY));
        assert!(
            has(
                &findings,
                Level::Warn,
                "tls.crl",
                "the CRL has spent 80.0% of its thisUpdate..nextUpdate window (warning \
                 threshold 66.7%): its next update is due in 2d 0h"
            ),
            "{findings:#?}"
        );
        let findings = check_config(&c, at(T0 + 11 * DAY));
        assert!(
            has(
                &findings,
                Level::Warn,
                "tls.crl",
                "the CRL is 1d 0h past its nextUpdate"
            ),
            "{findings:#?}"
        );
        assert!(findings.iter().all(|f| f.level != Level::Fail));
    }

    #[test]
    fn the_cluster_and_admin_sets_are_checked_against_the_same_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let ca = root();
        let ca_path = write(dir.path(), "ca.pem", &ca.pem()).display().to_string();
        let (node, node_key) = leaf("node-1", T0, T0 + 24 * HOUR, &ca);
        let (admin, admin_key) = leaf("admin", T0, T0 + 24 * HOUR, &ca);
        let mut c = Config::default();
        c.cluster.peer_tls.ca = Some(ca_path.clone());
        c.cluster.peer_tls.cert = Some(write(dir.path(), "node.crt", &node).display().to_string());
        c.cluster.peer_tls.key = Some(
            write(dir.path(), "node.key", &node_key)
                .display()
                .to_string(),
        );
        c.admin.cert = Some(write(dir.path(), "admin.crt", &admin).display().to_string());
        c.admin.key = Some(
            write(dir.path(), "admin.key", &admin_key)
                .display()
                .to_string(),
        );
        c.admin.client_ca = Some(ca_path);
        let findings = check_config(&c, at(T0 + 18 * HOUR));
        for what in ["cluster.peer_tls.cert", "admin.cert"] {
            assert!(
                has(
                    &findings,
                    Level::Warn,
                    what,
                    "has spent 75.0% of its lifetime (warning threshold 66.7%): it expires in 6h 0m"
                ),
                "{what}: {findings:#?}"
            );
        }
        assert!(has(&findings, Level::Ok, "admin.key", "matches"));
        assert!(has(
            &findings,
            Level::Ok,
            "admin.client_ca",
            "1 CA certificate"
        ));
        assert!(findings.iter().all(|f| f.level != Level::Fail));
    }

    #[test]
    fn a_key_that_does_not_match_fails() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = tls_config(dir.path(), &root(), T0 - DAY, T0 + 364 * DAY);
        let other = rcgen::KeyPair::generate().unwrap().serialize_pem();
        c.tls.key = Some(write(dir.path(), "other.pem", &other).display().to_string());
        let findings = check_config(&c, at(T0));
        assert!(
            findings
                .iter()
                .any(|f| f.level == Level::Fail && f.what == "tls.key"),
            "{findings:#?}"
        );
    }

    #[test]
    fn a_chain_in_the_wrong_order_fails() {
        let dir = tempfile::tempdir().unwrap();
        let ca = root();
        let mut c = tls_config(dir.path(), &ca, T0 - DAY, T0 + 364 * DAY);
        // CA first, then the leaf: each certificate must be issued by the next.
        let leaf_pem = std::fs::read_to_string(c.tls.cert.as_deref().unwrap()).unwrap();
        c.tls.cert = Some(
            write(
                dir.path(),
                "swapped.pem",
                &format!("{}{leaf_pem}", ca.pem()),
            )
            .display()
            .to_string(),
        );
        let findings = check_config(&c, at(T0));
        assert!(
            findings
                .iter()
                .any(|f| f.level == Level::Fail && f.message.contains("out of order")),
            "{findings:#?}"
        );
    }

    #[test]
    fn nothing_configured_is_a_single_ok_line() {
        let findings = check_config(&Config::default(), at(T0));
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].level, Level::Ok);
    }
}
