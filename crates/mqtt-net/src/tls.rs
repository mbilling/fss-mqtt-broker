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
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
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
    let certs = load_certs(cert_chain)?;
    let key = load_key(key)?;
    let builder = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(TLS_VERSIONS)
        .map_err(|e| tls_err("TLS server configuration", cert_chain, &e))?;
    let config = match client_ca {
        Some(ca) => {
            let roots = load_roots(ca)?;
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
                .build()
                .map_err(|e| tls_err("client certificate verifier", ca, &e))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    }
    .with_single_cert(certs, key)
    .map_err(|e| tls_err("server certificate/key", cert_chain, &e))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
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
