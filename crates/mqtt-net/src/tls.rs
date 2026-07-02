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

/// The TLS versions this broker speaks. TLS 1.2 support is a deliberate
/// non-feature until a deployment demands it (ADR 0002).
static TLS_VERSIONS: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13];

/// This broker's rustls crypto provider — `ring`, selected **explicitly** rather than
/// via rustls' process-default auto-detection. The OTLP exporter (ADR 0020 T9) pulls
/// reqwest's rustls, which adds a second provider (`aws-lc-rs`) to the build; naming
/// `ring` here keeps every broker-built TLS config unambiguous and provider-stable.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
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
    Ok(TlsAcceptor::from(Arc::new(server_config_with_crl(
        cert_chain, key, client_ca, crl,
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
    let builder = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(TLS_VERSIONS)
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
    let config = configured
        .with_single_cert(certs, key)
        .map_err(|e| tls_err("server certificate/key", cert_chain, &e))?;
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
