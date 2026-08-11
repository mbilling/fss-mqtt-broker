//! TLS configuration — the single audited place TLS is built (ADR 0002).
//!
//! Every listener and dialer in the broker obtains its TLS state from this
//! module: PEM material is loaded from files, servers are **TLS 1.3 only** (the
//! `tls12` cargo feature is not even compiled in), and client-certificate
//! verification is the default posture. There is deliberately no "skip
//! verification" or "accept any certificate" code path — tests mint real
//! throwaway CAs instead.

use crate::NetError;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// The default TLS versions this broker speaks: 1.3 only. TLS 1.2 exists strictly as a
/// per-listener opt-in (ADR 0002, amended 2026-08-11) for fleets whose device firmware
/// cannot negotiate 1.3 — it is never the default, and the cluster bus never speaks it.
static TLS_VERSIONS: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13];
static TLS_VERSIONS_WITH_12: &[&rustls::SupportedProtocolVersion] =
    &[&rustls::version::TLS13, &rustls::version::TLS12];

/// The TLS 1.2 posture of a listener (ADR 0002 amendment).
///
/// TLS 1.2 through this module is structurally hardened by what rustls simply does not
/// implement: every 1.2 suite the provider offers is ECDHE + AEAD (forward secrecy; no
/// CBC, no RC4/3DES, no static-RSA key exchange — the POODLE/Lucky13/Sweet32/ROBOT
/// classes have no surface), and there is no renegotiation, no compression, no export
/// anything. A test pins the suite property so a provider change cannot quietly regress
/// it.
///
/// The one hazard rustls leaves to the caller is **Extended Master Secret** (RFC 7627):
/// without it a 1.2 session is subject to the triple-handshake family of attacks.
/// [`Tls12::Hardened`] therefore REQUIRES EMS — a legacy client that cannot do EMS is
/// refused. [`Tls12::UnsafeLegacyFeatures`] relaxes exactly that one requirement, exists
/// only because some ancient firmware predates RFC 7627, and is loudly logged wherever it
/// is enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Tls12 {
    /// TLS 1.3 only — the default posture.
    #[default]
    Off,
    /// Admit TLS 1.2, hardened: ECDHE+AEAD suites only (structural) and EMS required.
    Hardened,
    /// Admit TLS 1.2 without requiring EMS. The triple-handshake protection is gone for
    /// clients that do not offer EMS; every client that does offer it still gets it.
    UnsafeLegacyFeatures,
}

/// Default size of the TLS 1.3 session-resumption cache, per listener.
///
/// rustls resumes statefully (two tickets per connection into an in-memory cache) and its
/// own default cache holds 256 sessions — which for a device fleet is no resumption at
/// all: 10k devices cycle the cache long before any of them reconnects, and every
/// battery-powered client pays the full handshake every time. 32k entries is ~a few MB
/// (an entry is a ticket plus resumption secrets, well under 100 bytes) and covers a
/// mid-sized fleet; `MQTTD_TLS_SESSION_CACHE` sizes it to yours, and `0` disables
/// resumption entirely.
pub const DEFAULT_SESSION_CACHE: usize = 32 * 1024;

/// Ceiling on how long a cached session stays resumable — RFC 5246 §F.1.4's 24 hours.
/// rustls' memory cache holds entries until capacity evicts them, which on a quiet
/// broker means a resumption secret could stay redeemable for months; an entry past this
/// age is refused and the client simply performs a full handshake.
pub const SESSION_TTL: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// [`rustls::server::StoresServerSessions`] wrapper that stamps every entry with its
/// insertion time and refuses to return one older than `ttl`.
///
/// Values are stored as `8-byte BE unix-seconds ‖ payload`. A malformed or stale entry
/// yields `None`, which to rustls means "no resumable session" — the failure mode is a
/// full handshake, never an error.
#[derive(Debug)]
struct ExpiringSessionCache {
    inner: Arc<dyn rustls::server::StoresServerSessions>,
    ttl: std::time::Duration,
}

impl ExpiringSessionCache {
    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }

    fn unwrap_fresh(&self, stored: &[u8], now: u64) -> Option<Vec<u8>> {
        let (stamp, payload) = stored.split_at_checked(8)?;
        let stored_at = u64::from_be_bytes(stamp.try_into().ok()?);
        if now.saturating_sub(stored_at) > self.ttl.as_secs() {
            return None;
        }
        Some(payload.to_vec())
    }
}

impl rustls::server::StoresServerSessions for ExpiringSessionCache {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        let mut stamped = Vec::with_capacity(8 + value.len());
        stamped.extend_from_slice(&Self::now_secs().to_be_bytes());
        stamped.extend_from_slice(&value);
        self.inner.put(key, stamped)
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.unwrap_fresh(&self.inner.get(key)?, Self::now_secs())
    }

    fn take(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.unwrap_fresh(&self.inner.take(key)?, Self::now_secs())
    }

    fn can_cache(&self) -> bool {
        self.inner.can_cache()
    }
}

/// This broker's rustls crypto provider — `aws-lc-rs`, selected **explicitly** rather
/// than via rustls' process-default auto-detection (ADR 0053). It is the same provider
/// the OTLP exporter's reqwest/rustls chain compiles in, so the whole build carries
/// exactly one crypto stack; naming it here keeps every broker-built TLS config
/// unambiguous and provider-stable regardless of process-default installation order.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Build a server-side acceptor from PEM files.
///
/// `client_ca` selects the posture:
/// - `Some(path)` — **mTLS**: connections must present a certificate issued by
///   a CA in that bundle.
/// - `None` — server-only TLS: clients are not certificate-authenticated.
///
/// # Errors
/// [`NetError::Tls`] if any file is missing/unparseable, the key does not match,
/// or the client CA bundle is empty (an empty trust store must fail loudly, not
/// silently admit nobody/everybody).
pub fn server_acceptor(
    cert_chain: &Path,
    key: &Path,
    client_ca: Option<&Path>,
) -> Result<TlsAcceptor, NetError> {
    server_acceptor_with_crl(cert_chain, key, client_ca, None)
}

/// Like [`server_acceptor`], but also feeds a **certificate revocation list** into the mTLS
/// client-cert verifier (ADR 0002 T8 / 0032 §5). A client presenting a certificate listed in
/// `crl` is rejected at the TLS handshake — before any MQTT bytes are read. Built on the same
/// reloadable acceptor seam (ADR 0032), so a renewed CRL is served on the next handshake when
/// re-read on `SIGHUP`.
///
/// `crl` is only meaningful with `client_ca` (mTLS): a CRL without client-certificate auth is a
/// configuration error and fails loudly rather than being silently ignored.
///
/// # Errors
/// [`NetError::Tls`] on the same conditions as [`server_acceptor`], plus an unreadable/empty
/// CRL file, or a CRL supplied without `client_ca`.
pub fn server_acceptor_with_crl(
    cert_chain: &Path,
    key: &Path,
    client_ca: Option<&Path>,
    crl: Option<&Path>,
) -> Result<TlsAcceptor, NetError> {
    server_acceptor_full(cert_chain, key, client_ca, crl, DEFAULT_SESSION_CACHE)
}

/// [`server_acceptor_with_crl`] with the session-resumption cache sized by the caller —
/// the listener plumbs `MQTTD_TLS_SESSION_CACHE` through here.
///
/// # Errors
/// As [`server_acceptor_with_crl`].
pub fn server_acceptor_full(
    cert_chain: &Path,
    key: &Path,
    client_ca: Option<&Path>,
    crl: Option<&Path>,
    session_cache: usize,
) -> Result<TlsAcceptor, NetError> {
    server_acceptor_versions(cert_chain, key, client_ca, crl, session_cache, Tls12::Off)
}

/// [`server_acceptor_full`] with the TLS 1.2 opt-in — see [`server_config_versions`].
///
/// # Errors
/// As [`server_acceptor_full`].
pub fn server_acceptor_versions(
    cert_chain: &Path,
    key: &Path,
    client_ca: Option<&Path>,
    crl: Option<&Path>,
    session_cache: usize,
    tls12: Tls12,
) -> Result<TlsAcceptor, NetError> {
    Ok(TlsAcceptor::from(Arc::new(server_config_versions(
        cert_chain,
        key,
        client_ca,
        crl,
        session_cache,
        tls12,
    )?)))
}

/// Build the rustls [`ServerConfig`] underlying [`server_acceptor`] — TLS 1.3 only, `ring`
/// provider, with optional mTLS client-cert verification. Exposed so the QUIC listener
/// (ADR 0036) can build its endpoint from the *same* audited config (adding ALPN `mqtt`),
/// keeping a single TLS configuration path in the broker.
///
/// # Errors
/// [`NetError::Tls`] on the same conditions as [`server_acceptor`].
pub fn server_config(
    cert_chain: &Path,
    key: &Path,
    client_ca: Option<&Path>,
) -> Result<ServerConfig, NetError> {
    server_config_with_crl(cert_chain, key, client_ca, None)
}

/// Build the rustls [`ServerConfig`] with an optional **certificate revocation list** fed into
/// the mTLS client-cert verifier (ADR 0002 T8). See [`server_acceptor_with_crl`] for the
/// posture; this is the shared builder so the QUIC listener (ADR 0036) can revoke client certs
/// through the very same path.
///
/// Revocation is checked **end-entity only** (the presented client leaf): the operational use
/// is revoking a compromised client credential, and end-entity-only checking does not require
/// a CRL for every issuer in the chain. Unknown revocation status for the leaf is an **error**
/// (rustls' default) — a deny-by-default broker treats "cannot determine" as "reject".
///
/// # Errors
/// [`NetError::Tls`] on the same conditions as [`server_config`], plus an unreadable/empty CRL
/// file, or a CRL supplied without `client_ca` (a meaningless, likely-mistaken configuration).
pub fn server_config_with_crl(
    cert_chain: &Path,
    key: &Path,
    client_ca: Option<&Path>,
    crl: Option<&Path>,
) -> Result<ServerConfig, NetError> {
    server_config_full(cert_chain, key, client_ca, crl, DEFAULT_SESSION_CACHE)
}

/// The full-surface builder every other `server_*` constructor delegates to: PEM material,
/// optional mTLS + CRL, and the **session-resumption cache size** (`0` disables
/// resumption). One function owns the `ServerConfig` so the audited path stays single.
///
/// # Errors
/// As [`server_config_with_crl`].
pub fn server_config_full(
    cert_chain: &Path,
    key: &Path,
    client_ca: Option<&Path>,
    crl: Option<&Path>,
    session_cache: usize,
) -> Result<ServerConfig, NetError> {
    server_config_versions(cert_chain, key, client_ca, crl, session_cache, Tls12::Off)
}

/// [`server_config_full`] plus the one degradation this module permits: `allow_tls12`
/// admits TLS 1.2 clients on this listener (ADR 0002 amendment). Off everywhere by
/// default; the caller that turns it on owes the operator a loud log line, and the
/// cluster bus builders never call this with `true` — peer links are 1.3, not
/// negotiable.
///
/// # Errors
/// As [`server_config_full`].
pub fn server_config_versions(
    cert_chain: &Path,
    key: &Path,
    client_ca: Option<&Path>,
    crl: Option<&Path>,
    session_cache: usize,
    tls12: Tls12,
) -> Result<ServerConfig, NetError> {
    // A CRL is only meaningful with mTLS; reject the meaningless (likely-mistaken) combination
    // up front rather than silently ignoring it.
    if client_ca.is_none() && crl.is_some() {
        return Err(NetError::Tls(
            "a CRL (MQTTD_TLS_CRL) requires client-certificate auth (set MQTTD_TLS_CLIENT_CA)"
                .to_string(),
        ));
    }
    let certs = load_certs(cert_chain)?;
    let key = load_key(key)?;
    let versions = if tls12 == Tls12::Off {
        TLS_VERSIONS
    } else {
        TLS_VERSIONS_WITH_12
    };
    let builder = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(versions)
        .map_err(|e| tls_err("TLS server configuration", cert_chain, &e))?;
    let configured = if let Some(ca) = client_ca {
        let roots = load_roots(ca)?;
        let mut verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider());
        if let Some(crl_path) = crl {
            verifier = verifier
                .with_crls(load_crls(crl_path)?)
                .only_check_end_entity_revocation();
        }
        let verifier = verifier
            .build()
            .map_err(|e| tls_err("client certificate verifier", ca, &e))?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let mut config = configured
        .with_single_cert(certs, key)
        .map_err(|e| tls_err("server certificate/key", cert_chain, &e))?;
    // TLS 1.3 resumption is stateful in rustls (tickets into this cache). The rustls
    // default of 256 entries is fleet-hostile — see [`DEFAULT_SESSION_CACHE`] — so the
    // size is explicit here, and `0` turns resumption off for deployments that want
    // every connection fully re-verified.
    config.session_storage = if session_cache == 0 {
        Arc::new(rustls::server::NoServerSessionStorage {})
    } else {
        // Wrapped with the 24-hour ceiling (RFC 5246 §F.1.4): capacity alone is not an
        // expiry policy, and a resumption secret must not stay redeemable for months on
        // a quiet broker.
        Arc::new(ExpiringSessionCache {
            inner: rustls::server::ServerSessionMemoryCache::new(session_cache),
            ttl: SESSION_TTL,
        })
    };
    // RFC 7627 Extended Master Secret: REQUIRED whenever TLS 1.2 is admitted, unless the
    // operator explicitly opted into the legacy relaxation — rustls' own default on this
    // provider is false, which would leave the triple-handshake surface open silently.
    // (The field is meaningless for pure 1.3, where the property is built into the
    // protocol.)
    config.require_ems = tls12 == Tls12::Hardened;
    Ok(config)
}

/// Build a dialing-side connector for the cluster bus: verifies the remote
/// against `ca` and presents `cert_chain`/`key` as our client identity (mTLS).
///
/// # Errors
/// [`NetError::Tls`] on unreadable/unparseable PEM material or a key mismatch.
pub fn client_connector(
    ca: &Path,
    cert_chain: &Path,
    key: &Path,
) -> Result<TlsConnector, NetError> {
    let roots = load_roots(ca)?;
    let certs = load_certs(cert_chain)?;
    let key = load_key(key)?;
    let config = ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(TLS_VERSIONS)
        .map_err(|e| tls_err("TLS client configuration", cert_chain, &e))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| tls_err("client certificate/key", cert_chain, &e))?;
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Parse the host part of `addr` (`host:port`, `[v6]:port`, or bare host) into
/// the [`ServerName`] to verify a dialed peer's certificate against.
///
/// # Errors
/// [`NetError::Tls`] if the host is neither a valid DNS name nor an IP address.
pub fn server_name(addr: &str) -> Result<ServerName<'static>, NetError> {
    // Socket-address forms first ("127.0.0.1:7001", "[::1]:7001"): IPv6 hosts
    // contain colons, so naive host:port splitting would mangle them.
    if let Ok(sock) = addr.parse::<std::net::SocketAddr>() {
        return Ok(ServerName::IpAddress(sock.ip().into()));
    }
    // A bare IP address ("::1", "10.0.0.1").
    if let Ok(ip) = addr.parse::<std::net::IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    // Otherwise a DNS name, optionally with a port to strip.
    let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    ServerName::try_from(host.to_string())
        .map_err(|_| NetError::Tls(format!("invalid TLS server name: {host:?}")))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, NetError> {
    let certs: Vec<_> = CertificateDer::pem_file_iter(path)
        .map_err(|e| tls_err("certificate file", path, &e))?
        .collect::<Result<_, _>>()
        .map_err(|e| tls_err("certificate PEM", path, &e))?;
    if certs.is_empty() {
        return Err(NetError::Tls(format!(
            "no certificates found in {}",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, NetError> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| tls_err("private key PEM", path, &e))
}

/// The first certificate in a PEM file as raw DER — the cluster CA or a node leaf, used to
/// build the signed-gossip signer/verifier (ADR 0022).
///
/// # Errors
/// [`NetError::Tls`] if the file cannot be read or contains no certificate.
pub fn first_cert_der(path: &Path) -> Result<Vec<u8>, NetError> {
    Ok(load_certs(path)?[0].as_ref().to_vec())
}

/// A private key from a PEM file as raw DER (PKCS#8 / SEC1 as stored), for the signed-gossip
/// signing key (ADR 0022).
///
/// # Errors
/// [`NetError::Tls`] if the file cannot be read or parsed as a private key.
pub fn private_key_der(path: &Path) -> Result<Vec<u8>, NetError> {
    Ok(load_key(path)?.secret_der().to_vec())
}

/// The first CRL in a PEM file as raw DER — for the signed-gossip revocation check
/// (ADR 0022 T7), which parses it with `x509-parser` rather than rustls.
///
/// # Errors
/// [`NetError::Tls`] if the file cannot be read or contains no CRL.
pub fn first_crl_der(path: &Path) -> Result<Vec<u8>, NetError> {
    Ok(load_crls(path)?[0].as_ref().to_vec())
}

fn load_crls(path: &Path) -> Result<Vec<CertificateRevocationListDer<'static>>, NetError> {
    let crls: Vec<_> = CertificateRevocationListDer::pem_file_iter(path)
        .map_err(|e| tls_err("CRL file", path, &e))?
        .collect::<Result<_, _>>()
        .map_err(|e| tls_err("CRL PEM", path, &e))?;
    if crls.is_empty() {
        return Err(NetError::Tls(format!(
            "no CRLs found in {}",
            path.display()
        )));
    }
    Ok(crls)
}

fn load_roots(path: &Path) -> Result<RootCertStore, NetError> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|e| tls_err("CA certificate", path, &e))?;
    }
    Ok(roots)
}

fn tls_err(what: &str, path: &Path, err: &dyn std::fmt::Display) -> NetError {
    NetError::Tls(format!("{what} ({}): {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{client_connector, server_acceptor, server_name};
    use std::path::PathBuf;

    /// Write a throwaway CA + leaf cert/key as PEM files under a unique dir.
    fn mint_pki(tag: &str) -> (PathBuf, PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("mqtt-net-tls-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf_params =
            rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

        let ca = dir.join("ca.pem");
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        std::fs::write(&ca, ca_cert.pem()).unwrap();
        std::fs::write(&cert, leaf_cert.pem()).unwrap();
        std::fs::write(&key, leaf_key.serialize_pem()).unwrap();
        (ca, cert, key)
    }

    #[test]
    fn builds_server_acceptor_with_and_without_client_auth() {
        let (ca, cert, key) = mint_pki("acceptor");
        server_acceptor(&cert, &key, None).unwrap();
        server_acceptor(&cert, &key, Some(&ca)).unwrap();
    }

    #[test]
    fn builds_mtls_client_connector() {
        let (ca, cert, key) = mint_pki("connector");
        client_connector(&ca, &cert, &key).unwrap();
    }

    #[test]
    fn missing_or_empty_material_fails_loudly() {
        let (_ca, cert, key) = mint_pki("badpaths");
        let missing = PathBuf::from("/nonexistent/of-course.pem");
        assert!(server_acceptor(&missing, &key, None).is_err());
        assert!(server_acceptor(&cert, &missing, None).is_err());
        // An empty client-CA bundle must fail, not silently disable mTLS.
        let empty = std::env::temp_dir().join(format!("mqtt-net-tls-empty-{}", std::process::id()));
        std::fs::write(&empty, "").unwrap();
        assert!(server_acceptor(&cert, &key, Some(&empty)).is_err());
    }

    #[test]
    fn a_crl_without_client_auth_is_rejected() {
        // A CRL only makes sense with mTLS; supplying one without a client CA is a likely
        // misconfiguration and must fail loudly, not be silently ignored (ADR 0002 T8).
        let (_ca, cert, key) = mint_pki("crl-no-ca");
        let bogus_crl = PathBuf::from("/nonexistent/crl.pem");
        assert!(super::server_acceptor_with_crl(&cert, &key, None, Some(&bogus_crl)).is_err());
    }

    #[test]
    fn an_empty_or_missing_crl_file_is_rejected() {
        // An empty CRL bundle must fail rather than silently disabling revocation checking.
        let (ca, cert, key) = mint_pki("crl-empty");
        let missing = PathBuf::from("/nonexistent/crl.pem");
        assert!(super::server_acceptor_with_crl(&cert, &key, Some(&ca), Some(&missing)).is_err());
        let empty = std::env::temp_dir().join(format!("mqtt-net-crl-empty-{}", std::process::id()));
        std::fs::write(&empty, "").unwrap();
        assert!(super::server_acceptor_with_crl(&cert, &key, Some(&ca), Some(&empty)).is_err());
    }

    #[test]
    fn server_name_parses_dns_and_ip_hosts() {
        assert!(server_name("broker.example.com:8883").is_ok());
        assert!(server_name("127.0.0.1:7001").is_ok());
        assert!(server_name("broker.example.com").is_ok());
        assert!(server_name("not a hostname:1").is_err());
    }

    #[test]
    fn server_name_handles_ipv6_hosts() {
        use rustls::pki_types::ServerName;
        // Bracketed socket-address form and bare-address forms must all resolve
        // to IP server names, not be mangled by host:port splitting.
        for addr in ["[::1]:7001", "::1", "2001:db8::1", "[2001:db8::1]:8883"] {
            match server_name(addr) {
                Ok(ServerName::IpAddress(_)) => {}
                other => panic!("{addr:?} should parse as an IP server name, got {other:?}"),
            }
        }
    }
}

#[cfg(test)]
mod tls12_hardening_tests {
    use super::*;

    /// The TLS 1.2 suite set is a STRICT ALLOWLIST, not a property check: exactly the six
    /// ECDHE+AEAD suites, by name. A blocklist of known-bad suites is never complete; an
    /// allowlist means a provider upgrade that adds ANY new 1.2 suite — CBC, `CCM_8`,
    /// whatever — fails this test and forces a human decision.
    #[test]
    fn the_tls12_suites_are_exactly_the_allowlist() {
        let mut tls12: Vec<String> = provider()
            .cipher_suites
            .iter()
            .filter(|s| matches!(s, rustls::SupportedCipherSuite::Tls12(_)))
            .map(|s| format!("{:?}", s.suite()))
            .collect();
        tls12.sort();
        let mut expected = vec![
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        ];
        expected.sort_unstable();
        assert_eq!(
            tls12, expected,
            "the 1.2 suite set drifted from the allowlist — every entry must be ECDHE \
             (forward secrecy; no static-RSA/ROBOT) and AEAD (no CBC padding oracles, no \
             Sweet32, no RC4)"
        );
    }

    /// The classical key-exchange groups are exactly x25519, secp256r1, secp384r1 — the
    /// allowlist. (The ML-KEM hybrids in the provider are TLS 1.3-only `key_share` entries;
    /// a 1.2 `ClientHello` cannot negotiate them.) No FFDHE means no Logjam/small-subgroup
    /// surface; no binary/small curves by construction.
    #[test]
    fn classical_kx_groups_are_exactly_the_allowlist() {
        let mut classical: Vec<String> = provider()
            .kx_groups
            .iter()
            .map(|g| format!("{:?}", g.name()))
            .filter(|n| !n.contains("MLKEM") && !n.contains("Unknown"))
            .collect();
        classical.sort();
        assert_eq!(
            classical,
            vec!["X25519", "secp256r1", "secp384r1"],
            "the classical group set drifted from the allowlist"
        );
    }

    /// RFC 5077 session tickets stay OFF for TLS 1.2: an unrotated ticket key silently
    /// destroys forward secrecy for every session it covers, and this broker has no
    /// ticket-key rotation infrastructure — so the safe setting is the only setting.
    #[test]
    fn tls12_session_tickets_are_off() {
        let dir = std::env::temp_dir().join(format!("mqttd-tick-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let (cp, kp) = (dir.join("c.pem"), dir.join("k.pem"));
        std::fs::write(&cp, cert.pem()).unwrap();
        std::fs::write(&kp, key.serialize_pem()).unwrap();
        let cfg = server_config_versions(&cp, &kp, None, None, 64, Tls12::Hardened).unwrap();
        assert!(
            !cfg.ticketer.enabled(),
            "a ticketer appeared — TLS 1.2 tickets without key rotation destroy forward secrecy"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The resumption cache refuses entries older than 24 hours (RFC 5246 §F.1.4):
    /// capacity eviction alone would let a resumption secret stay redeemable for months
    /// on a quiet broker.
    #[test]
    fn cached_sessions_expire_at_the_rfc_ceiling() {
        use rustls::server::StoresServerSessions as _;
        let cache = ExpiringSessionCache {
            inner: rustls::server::ServerSessionMemoryCache::new(8),
            ttl: SESSION_TTL,
        };
        assert!(cache.put(b"k".to_vec(), b"secret".to_vec()));
        assert_eq!(
            cache.get(b"k").as_deref(),
            Some(b"secret".as_slice()),
            "fresh: served"
        );

        // The same entry, restamped as 25 hours old, must be refused.
        let old = ExpiringSessionCache {
            inner: rustls::server::ServerSessionMemoryCache::new(8),
            ttl: SESSION_TTL,
        };
        let stale_stamp = (ExpiringSessionCache::now_secs() - 25 * 3600).to_be_bytes();
        let mut stamped = stale_stamp.to_vec();
        stamped.extend_from_slice(b"secret");
        old.inner.put(b"k".to_vec(), stamped);
        assert_eq!(old.get(b"k"), None, "a 25-hour-old session must not resume");
        assert_eq!(old.take(b"k"), None, "take path honours the ceiling too");
    }

    /// The relaxable half: EMS (RFC 7627) is REQUIRED under the hardened posture and
    /// relaxed only by the explicit unsafe opt-in — rustls' own default on this provider
    /// is false, which would have left the triple-handshake surface open silently.
    #[test]
    fn ems_follows_the_declared_posture() {
        let dir = std::env::temp_dir().join(format!("mqttd-ems-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let cert_path = dir.join("c.pem");
        let key_path = dir.join("k.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();

        let hardened =
            server_config_versions(&cert_path, &key_path, None, None, 64, Tls12::Hardened).unwrap();
        assert!(hardened.require_ems, "hardened 1.2 must require EMS");

        let legacy = server_config_versions(
            &cert_path,
            &key_path,
            None,
            None,
            64,
            Tls12::UnsafeLegacyFeatures,
        )
        .unwrap();
        assert!(!legacy.require_ems, "the unsafe opt-in relaxes exactly EMS");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
