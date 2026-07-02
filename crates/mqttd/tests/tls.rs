//! Transport-security integration tests (ADR 0002): clients over TLS 1.3 (with
//! and without client-certificate auth) and the cluster bus over mutual TLS.
//!
//! Tests mint a real throwaway CA with `rcgen` — there is no
//! "skip verification" path anywhere, including here.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqtt_storage::MemorySessionStore;
use mqttd::peer::PeerTls;
use mqttd::Hub;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_rustls::{TlsAcceptor, TlsConnector};

const V4: ProtocolVersion = ProtocolVersion::V311;

// --- throwaway PKI ----------------------------------------------------------

/// PEM files for one CA and one leaf, written under a unique temp dir.
struct Pki {
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
}

/// Mint a CA plus a leaf certificate for `127.0.0.1` valid for both server and
/// client auth (cluster nodes act as both; reused for client tests too).
///
/// Every call gets its own directory: tests run concurrently in one process,
/// and two PKIs sharing files would overwrite each other's CA mid-handshake.
fn mint_pki(tag: &str) -> (Pki, rcgen::Certificate, rcgen::KeyPair) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static UNIQUE: AtomicU64 = AtomicU64::new(0);
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("mqttd-tls-{}-{tag}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let mut leaf_params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    leaf_params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

    let pki = Pki {
        ca: dir.join("ca.pem"),
        cert: dir.join("cert.pem"),
        key: dir.join("key.pem"),
    };
    std::fs::write(&pki.ca, ca_cert.pem()).unwrap();
    std::fs::write(&pki.cert, leaf_cert.pem()).unwrap();
    std::fs::write(&pki.key, leaf_key.serialize_pem()).unwrap();
    (pki, ca_cert, ca_key)
}

/// A test-side TLS connector trusting `ca`, optionally presenting a client cert.
fn test_connector(ca: &Path, client_identity: Option<(&Path, &Path)>) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    for cert in pem_certs(ca) {
        roots.add(cert).unwrap();
    }
    // Explicit `ring` provider: the OTLP exporter adds a second rustls provider to the
    // build, so auto-detection would be ambiguous (matches mqtt-net::tls).
    let builder = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots);
    let config = match client_identity {
        Some((cert, key)) => {
            let key = PrivateKeyDer::from_pem_file(key).unwrap();
            builder.with_client_auth_cert(pem_certs(cert), key).unwrap()
        }
        None => builder.with_no_client_auth(),
    };
    TlsConnector::from(Arc::new(config))
}

fn pem_certs(path: &Path) -> Vec<CertificateDer<'static>> {
    CertificateDer::pem_file_iter(path)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

// --- broker harness ----------------------------------------------------------

/// Start a single node with a TLS client listener; returns its address.
async fn start_tls_node(acceptor: TlsAcceptor) -> SocketAddr {
    let (hub, hub_tx) = Hub::with_config(
        NodeId("tls-node".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(stream).await {
                    let auth = Arc::new(mqtt_auth::basic::BasicAuthenticator {
                        allow_anonymous: true,
                    });
                    let policy = Arc::new(mqttd::conn::ConnPolicy {
                        auth: mqttd::conn::auth_handle(auth),
                        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
                        audit: Arc::new(mqtt_observability::AuditLog::new()),
                        proxy: None,
                        store: None,
                        connect_timeout: std::time::Duration::from_secs(10),
                        shutdown: None,
                        metrics: None,
                        enhanced: None,
                    });
                    mqttd::conn::handle_stream(tls, Some(peer), None, policy, hub).await;
                }
            });
        }
    });
    addr
}

// --- generic MQTT test client -------------------------------------------------

struct Client<S> {
    reader: mqtt_net::FrameReader<tokio::io::ReadHalf<S>>,
    writer: mqtt_net::FrameWriter<tokio::io::WriteHalf<S>>,
}

impl<S: AsyncRead + AsyncWrite> Client<S> {
    async fn connect(stream: S, id: &str) -> Self {
        let (rh, wh) = tokio::io::split(stream);
        let mut c = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        c.writer
            .send(&Packet::Connect(Connect {
                properties: mqtt_codec::Properties::new(),
                protocol: V4,
                clean_session: true,
                keep_alive: 30,
                client_id: id.to_string(),
                last_will: None,
                username: None,
                password: None,
            }))
            .await
            .unwrap();
        assert!(matches!(c.recv().await, Some(Packet::ConnAck(_))));
        c
    }

    async fn subscribe(&mut self, filter: &str) {
        self.writer
            .send(&Packet::Subscribe(Subscribe {
                properties: mqtt_codec::Properties::new(),
                pkid: 1,
                filters: vec![SubscribeFilter {
                    options: mqtt_codec::SubscriptionOptions::default(),
                    path: filter.to_string(),
                    qos: QoS::AtMostOnce,
                }],
            }))
            .await
            .unwrap();
        assert!(matches!(self.recv().await, Some(Packet::SubAck(_))));
    }

    async fn publish(&mut self, topic: &str, payload: &'static [u8]) {
        self.writer
            .send(&Packet::Publish(Publish {
                properties: mqtt_codec::Properties::new(),
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: topic.to_string(),
                pkid: None,
                payload: bytes::Bytes::from_static(payload),
            }))
            .await
            .unwrap();
    }

    async fn recv(&mut self) -> Option<Packet> {
        timeout(Duration::from_millis(300), self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

async fn tls_connect(
    addr: SocketAddr,
    connector: &TlsConnector,
) -> std::io::Result<impl AsyncRead + AsyncWrite> {
    let tcp = TcpStream::connect(addr).await?;
    let name = ServerName::try_from("127.0.0.1").unwrap();
    connector.connect(name, tcp).await
}

// --- client-listener tests -----------------------------------------------------

#[tokio::test]
async fn tls_pubsub_roundtrip() {
    let (pki, _, _) = mint_pki("roundtrip");
    let acceptor = mqtt_net::tls::server_acceptor(&pki.cert, &pki.key, None).unwrap();
    let addr = start_tls_node(acceptor).await;
    let connector = test_connector(&pki.ca, None);

    let mut sub = Client::connect(tls_connect(addr, &connector).await.unwrap(), "sub").await;
    sub.subscribe("secure/+/data").await;
    let mut publ = Client::connect(tls_connect(addr, &connector).await.unwrap(), "pub").await;
    publ.publish("secure/zone/data", b"over-tls").await;

    match sub.recv().await {
        Some(Packet::Publish(p)) => {
            assert_eq!(p.topic, "secure/zone/data");
            assert_eq!(&p.payload[..], b"over-tls");
        }
        other => panic!("expected publish over TLS, got {other:?}"),
    }
}

#[tokio::test]
async fn mtls_listener_rejects_clients_without_certificates() {
    let (pki, _, _) = mint_pki("mtls");
    let acceptor = mqtt_net::tls::server_acceptor(&pki.cert, &pki.key, Some(&pki.ca)).unwrap();
    let addr = start_tls_node(acceptor).await;

    // Without a client certificate: no MQTT session may form. The TLS 1.3
    // rejection can surface at handshake or on first use of the stream.
    let connector = test_connector(&pki.ca, None);
    match tls_connect(addr, &connector).await {
        Err(_) => {} // rejected at handshake
        Ok(stream) => {
            let (rh, wh) = tokio::io::split(stream);
            let mut writer = mqtt_net::FrameWriter::new(wh, V4);
            let _ = writer
                .send(&Packet::Connect(Connect {
                    properties: mqtt_codec::Properties::new(),
                    protocol: V4,
                    clean_session: true,
                    keep_alive: 30,
                    client_id: "no-cert".into(),
                    last_will: None,
                    username: None,
                    password: None,
                }))
                .await;
            let mut reader = mqtt_net::FrameReader::new(rh, V4);
            let got_connack = matches!(
                timeout(Duration::from_millis(500), reader.next_packet()).await,
                Ok(Ok(Some(Packet::ConnAck(_))))
            );
            assert!(!got_connack, "client without certificate got a CONNACK");
        }
    }

    // With a cluster-CA-issued certificate the same listener serves normally.
    let connector = test_connector(&pki.ca, Some((&pki.cert, &pki.key)));
    let mut c = Client::connect(tls_connect(addr, &connector).await.unwrap(), "with-cert").await;
    c.subscribe("ok/#").await;
}

#[tokio::test]
async fn plaintext_client_on_tls_port_is_rejected() {
    let (pki, _, _) = mint_pki("plaintext-reject");
    let acceptor = mqtt_net::tls::server_acceptor(&pki.cert, &pki.key, None).unwrap();
    let addr = start_tls_node(acceptor).await;

    // Speak plaintext MQTT at the TLS port: the handshake fails server-side and
    // the connection closes without ever producing a CONNACK.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let (rh, wh) = stream.split();
    let mut writer = mqtt_net::FrameWriter::new(wh, V4);
    let _ = writer
        .send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 30,
            client_id: "plain".into(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await;
    let mut rh = rh;
    let mut buf = [0u8; 64];
    let n = timeout(Duration::from_secs(2), rh.read(&mut buf))
        .await
        .expect("server should close the connection promptly")
        .unwrap_or(0);
    // Anything the server sent back is a TLS alert, not an MQTT CONNACK (0x20).
    assert!(n == 0 || buf[0] != 0x20, "plaintext client got a CONNACK");
}

// --- cluster-bus (peer mTLS) tests ----------------------------------------------

/// Mint a per-node peer leaf (CN == node id, SAN 127.0.0.1, server+client auth)
/// signed by the shared cluster CA, and build its mTLS context. The CN binding
/// (ADR 0004 step 5) requires each node's certificate CN to equal its node id.
fn node_peer_tls(
    ca_pem: &Path,
    ca_cert: &rcgen::Certificate,
    ca_key: &rcgen::KeyPair,
    node_id: &str,
) -> PeerTls {
    use std::sync::atomic::{AtomicU64, Ordering};
    static UNIQUE: AtomicU64 = AtomicU64::new(0);
    // Unique per call: concurrent cluster harnesses must not share cert files.
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("mqttd-peer-{}-{node_id}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, node_id);
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = params.signed_by(&key, ca_cert, ca_key).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    PeerTls {
        acceptor: mqtt_net::tls::server_acceptor(&cert_path, &key_path, Some(ca_pem)).unwrap(),
        connector: mqtt_net::tls::client_connector(ca_pem, &cert_path, &key_path).unwrap(),
        ca_der: mqtt_net::tls::first_cert_der(ca_pem).unwrap(),
        cert_der: mqtt_net::tls::first_cert_der(&cert_path).unwrap(),
        key_der: mqtt_net::tls::private_key_der(&key_path).unwrap(),
        gossip_crl: std::sync::Arc::new(std::sync::RwLock::new(None)),
        crl_path: None,
    }
}

/// Two-node cluster whose peer links run mutual TLS; returns client addresses
/// and node B's peer-listener address (for the rejection test).
async fn start_mtls_cluster() -> (SocketAddr, SocketAddr, SocketAddr) {
    let (pki, ca_cert, ca_key) = mint_pki("cluster");
    let tls_a = node_peer_tls(&pki.ca, &ca_cert, &ca_key, "mtls-a");
    let tls_b = node_peer_tls(&pki.ca, &ca_cert, &ca_key, "mtls-b");

    let peer_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let paddr_a = peer_a.local_addr().unwrap();
    let paddr_b = peer_b.local_addr().unwrap();
    let cli_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cli_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let caddr_a = cli_a.local_addr().unwrap();
    let caddr_b = cli_b.local_addr().unwrap();

    let id_a = NodeId("mtls-a".into());
    let id_b = NodeId("mtls-b".into());
    let (hub_a, tx_a) =
        Hub::with_config(id_a.clone(), std::sync::Arc::new(MemorySessionStore::new()));
    let (hub_b, tx_b) =
        Hub::with_config(id_b.clone(), std::sync::Arc::new(MemorySessionStore::new()));
    tokio::spawn(hub_a.run());
    tokio::spawn(hub_b.run());

    for (cli, tx) in [(cli_a, tx_a.clone()), (cli_b, tx_b.clone())] {
        tokio::spawn(async move {
            loop {
                let (stream, _) = cli.accept().await.unwrap();
                tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
            }
        });
    }

    tokio::spawn(mqttd::peer::serve_listener(
        peer_a,
        id_a.clone(),
        tx_a.clone(),
        Some(tls_a.clone()),
        None,
    ));
    tokio::spawn(mqttd::peer::serve_listener(
        peer_b,
        id_b.clone(),
        tx_b.clone(),
        Some(tls_b.clone()),
        None,
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_b.to_string(),
        id_a,
        tx_a,
        Some(tls_a),
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_a.to_string(),
        id_b,
        tx_b,
        Some(tls_b),
    ));

    (caddr_a, caddr_b, paddr_b)
}

#[tokio::test]
async fn publish_routes_across_mtls_peer_links() {
    let (addr_a, addr_b, _) = start_mtls_cluster().await;

    let mut sub = Client::connect(TcpStream::connect(addr_a).await.unwrap(), "sub").await;
    sub.subscribe("mtls/+/data").await;
    let mut publ = Client::connect(TcpStream::connect(addr_b).await.unwrap(), "pub").await;

    for attempt in 0..50 {
        publ.publish("mtls/zone1/data", b"sealed").await;
        if let Some(Packet::Publish(p)) = sub.recv().await {
            assert_eq!(p.topic, "mtls/zone1/data");
            assert_eq!(&p.payload[..], b"sealed");
            return;
        }
        assert!(attempt < 49, "message never crossed the mTLS cluster");
    }
}

#[tokio::test]
async fn plaintext_peer_is_rejected_by_mtls_listener() {
    let (_, _, peer_addr) = start_mtls_cluster().await;

    // A plaintext "node" tries to join the mesh: the mTLS listener must drop it
    // without speaking the peer protocol.
    let mut stream = TcpStream::connect(peer_addr).await.unwrap();
    let mut hello = Vec::new();
    mqtt_cluster::peer::encode(
        &mqtt_cluster::peer::PeerMessage::Hello {
            node_id: "intruder".into(),
        },
        &mut hello,
    )
    .unwrap();
    stream.write_all(&hello).await.unwrap();

    let mut buf = [0u8; 64];
    let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("mTLS peer listener should close plaintext links promptly")
        .unwrap_or(0);
    // No peer Hello may come back — a peer frame starts with a 4-byte length
    // prefix (first byte 0x00 for any sane frame); a TLS alert starts with 0x15.
    assert!(
        n == 0 || buf[0] != 0x00,
        "plaintext intruder received a peer-protocol reply"
    );
}

// --- mTLS identity (ADR 0004) ---------------------------------------------------

/// Start a node whose TLS listener enforces the production auth path: client
/// certificates are required (`client_ca`), the verified leaf's CN becomes the
/// broker identity via `conn::tls_identity`, and anonymous access is denied.
async fn start_identity_node(pki: &Pki) -> SocketAddr {
    let acceptor = mqtt_net::tls::server_acceptor(&pki.cert, &pki.key, Some(&pki.ca)).unwrap();
    let (hub, hub_tx) = Hub::with_config(
        NodeId("id-node".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(stream).await {
                    let identity = mqttd::conn::tls_identity(&tls);
                    let auth = Arc::new(mqtt_auth::basic::BasicAuthenticator {
                        allow_anonymous: false,
                    });
                    let policy = Arc::new(mqttd::conn::ConnPolicy {
                        auth: mqttd::conn::auth_handle(auth),
                        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
                        audit: Arc::new(mqtt_observability::AuditLog::new()),
                        proxy: None,
                        store: None,
                        connect_timeout: std::time::Duration::from_secs(10),
                        shutdown: None,
                        metrics: None,
                        enhanced: None,
                    });
                    mqttd::conn::handle_stream(tls, Some(peer), identity, policy, hub).await;
                }
            });
        }
    });
    addr
}

/// Mint a client leaf signed by the test CA with an explicit Common Name.
fn mint_client_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &rcgen::KeyPair,
    cn: &str,
    dir_tag: &str,
) -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!("mqttd-cn-{}-{dir_tag}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let cert = params.signed_by(&key, ca_cert, ca_key).unwrap();
    let cert_path = dir.join("client-cert.pem");
    let key_path = dir.join("client-key.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

/// End to end (ADR 0004): with anonymous denied, a client presenting a
/// CA-issued certificate is admitted — its CN is the broker identity — and a
/// full pub/sub session works over the same connection pair.
#[tokio::test]
async fn mtls_common_name_identity_is_admitted_under_deny_anonymous() {
    let (pki, ca_cert, ca_key) = mint_pki("cn-identity");
    let (client_cert, client_key) = mint_client_cert(&ca_cert, &ca_key, "device-7", "admit");
    let addr = start_identity_node(&pki).await;
    let connector = test_connector(&pki.ca, Some((&client_cert, &client_key)));

    let mut sub = Client::connect(tls_connect(addr, &connector).await.unwrap(), "sub").await;
    sub.subscribe("id/+/data").await;
    let mut publ = Client::connect(tls_connect(addr, &connector).await.unwrap(), "pub").await;
    publ.publish("id/zone/data", b"authenticated").await;

    match sub.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(&p.payload[..], b"authenticated"),
        other => panic!("expected publish over authenticated session, got {other:?}"),
    }
}

/// End to end (ADR 0004): the same deny-anonymous policy on a server-only TLS
/// listener (no client CA, so no identity) refuses the CONNECT with 0x05.
#[tokio::test]
async fn tls_without_client_cert_is_not_authorized_under_deny_anonymous() {
    let (pki, _, _) = mint_pki("cn-deny");
    // Server-only TLS: handshake succeeds, but no identity is available.
    let acceptor = mqtt_net::tls::server_acceptor(&pki.cert, &pki.key, None).unwrap();
    let (hub, hub_tx) = Hub::with_config(
        NodeId("deny-node".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(stream).await {
                    let identity = mqttd::conn::tls_identity(&tls);
                    let auth = Arc::new(mqtt_auth::basic::BasicAuthenticator {
                        allow_anonymous: false,
                    });
                    let policy = Arc::new(mqttd::conn::ConnPolicy {
                        auth: mqttd::conn::auth_handle(auth),
                        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
                        audit: Arc::new(mqtt_observability::AuditLog::new()),
                        proxy: None,
                        store: None,
                        connect_timeout: std::time::Duration::from_secs(10),
                        shutdown: None,
                        metrics: None,
                        enhanced: None,
                    });
                    mqttd::conn::handle_stream(tls, Some(peer), identity, policy, hub).await;
                }
            });
        }
    });

    let connector = test_connector(&pki.ca, None);
    let stream = tls_connect(addr, &connector).await.unwrap();
    let (rh, wh) = tokio::io::split(stream);
    let mut writer = mqtt_net::FrameWriter::new(wh, V4);
    writer
        .send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 30,
            client_id: "anon".into(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await
        .unwrap();
    let mut reader = mqtt_net::FrameReader::new(rh, V4);
    match timeout(Duration::from_millis(500), reader.next_packet()).await {
        Ok(Ok(Some(Packet::ConnAck(ack)))) => {
            assert_eq!(ack.code, 0x05, "expected not-authorized");
            assert!(!ack.session_present);
        }
        other => panic!("expected CONNACK 0x05, got {other:?}"),
    }
    // The connection closes after the rejection.
    match timeout(Duration::from_millis(500), reader.next_packet()).await {
        Ok(Ok(None) | Err(_)) => {}
        other => panic!("expected the connection to close, got {other:?}"),
    }
}

// --- CRL (certificate revocation) tests (ADR 0002 T8) ---------------------------

/// A CRL-capable PKI: a CA that signs CRLs, a server leaf, two client leaves (one of
/// which the CRL revokes), and the CRL itself. Each leaf gets an explicit serial so the
/// CRL can name it.
struct CrlPki {
    ca: PathBuf,
    server_cert: PathBuf,
    server_key: PathBuf,
    revoked_cert: PathBuf,
    revoked_key: PathBuf,
    valid_cert: PathBuf,
    valid_key: PathBuf,
    crl: PathBuf,
}

/// Issue a client leaf for `127.0.0.1` (clientAuth) with serial `serial`, signed by `ca`.
fn client_leaf(
    dir: &Path,
    name: &str,
    serial: u64,
    ca_cert: &rcgen::Certificate,
    ca_key: &rcgen::KeyPair,
) -> (PathBuf, PathBuf) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    params.serial_number = Some(rcgen::SerialNumber::from(serial));
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let cert = params.signed_by(&key, ca_cert, ca_key).unwrap();
    let cert_path = dir.join(format!("{name}.pem"));
    let key_path = dir.join(format!("{name}.key"));
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

fn mint_crl_pki(tag: &str) -> CrlPki {
    use std::sync::atomic::{AtomicU64, Ordering};
    static UNIQUE: AtomicU64 = AtomicU64::new(0);
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("mqttd-crl-{}-{tag}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // CA must carry CrlSign (rcgen refuses to sign a CRL otherwise) plus KeyCertSign.
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_path = dir.join("ca.pem");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();

    // Server leaf.
    let server_key = rcgen::KeyPair::generate().unwrap();
    let mut sp = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    sp.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let server_cert = sp.signed_by(&server_key, &ca_cert, &ca_key).unwrap();
    let server_cert_path = dir.join("server.pem");
    let server_key_path = dir.join("server.key");
    std::fs::write(&server_cert_path, server_cert.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();

    // Two client leaves with distinct serials; the CRL revokes the first.
    let revoked_serial = 0x1001;
    let (revoked_cert, revoked_key) =
        client_leaf(&dir, "revoked", revoked_serial, &ca_cert, &ca_key);
    let (valid_cert, valid_key) = client_leaf(&dir, "valid", 0x1002, &ca_cert, &ca_key);

    // CRL listing the revoked serial, signed by the CA. Wide validity window so the test is
    // time-independent (webpki checks list membership, not wall-clock expiry, but keep it sane).
    let crl_params = rcgen::CertificateRevocationListParams {
        this_update: rcgen::date_time_ymd(2020, 1, 1),
        next_update: rcgen::date_time_ymd(2099, 1, 1),
        crl_number: rcgen::SerialNumber::from(1u64),
        issuing_distribution_point: None,
        revoked_certs: vec![rcgen::RevokedCertParams {
            serial_number: rcgen::SerialNumber::from(revoked_serial),
            revocation_time: rcgen::date_time_ymd(2020, 1, 2),
            reason_code: Some(rcgen::RevocationReason::KeyCompromise),
            invalidity_date: None,
        }],
        key_identifier_method: rcgen::KeyIdMethod::Sha256,
    };
    let crl = crl_params.signed_by(&ca_cert, &ca_key).unwrap();
    let crl_path = dir.join("crl.pem");
    std::fs::write(&crl_path, crl.pem().unwrap()).unwrap();

    CrlPki {
        ca: dir.join("ca.pem"),
        server_cert: server_cert_path,
        server_key: server_key_path,
        revoked_cert,
        revoked_key,
        valid_cert,
        valid_key,
        crl: crl_path,
    }
}

/// Assert that presenting `connector`'s client cert yields no MQTT session: the TLS 1.3
/// rejection can surface at handshake or as a closed stream with no CONNACK.
async fn assert_no_session(addr: SocketAddr, connector: &TlsConnector) {
    match tls_connect(addr, connector).await {
        Err(_) => {} // rejected at handshake
        Ok(stream) => {
            let (rh, wh) = tokio::io::split(stream);
            let mut writer = mqtt_net::FrameWriter::new(wh, V4);
            let _ = writer
                .send(&Packet::Connect(Connect {
                    properties: mqtt_codec::Properties::new(),
                    protocol: V4,
                    clean_session: true,
                    keep_alive: 30,
                    client_id: "revoked".into(),
                    last_will: None,
                    username: None,
                    password: None,
                }))
                .await;
            let mut reader = mqtt_net::FrameReader::new(rh, V4);
            let got_connack = matches!(
                timeout(Duration::from_millis(500), reader.next_packet()).await,
                Ok(Ok(Some(Packet::ConnAck(_))))
            );
            assert!(!got_connack, "a revoked client certificate got a CONNACK");
        }
    }
}

/// A client whose certificate is on the CRL is rejected at the TLS handshake — before any
/// MQTT bytes — while a non-revoked certificate from the same CA still connects normally.
#[tokio::test]
async fn crl_rejects_a_revoked_client_certificate() {
    let pki = mint_crl_pki("reject");
    let acceptor = mqtt_net::tls::server_acceptor_with_crl(
        &pki.server_cert,
        &pki.server_key,
        Some(&pki.ca),
        Some(&pki.crl),
    )
    .unwrap();
    let addr = start_tls_node(acceptor).await;

    // Revoked: no session forms.
    let revoked = test_connector(&pki.ca, Some((&pki.revoked_cert, &pki.revoked_key)));
    assert_no_session(addr, &revoked).await;

    // Valid (same CA, not revoked): connects and can subscribe.
    let valid = test_connector(&pki.ca, Some((&pki.valid_cert, &pki.valid_key)));
    let mut c = Client::connect(tls_connect(addr, &valid).await.unwrap(), "valid").await;
    c.subscribe("ok/#").await;
}

/// Without a CRL configured, the very same revoked certificate is accepted — proving the
/// rejection above is the CRL's doing, not some unrelated property of the cert.
#[tokio::test]
async fn without_a_crl_the_same_certificate_is_accepted() {
    let pki = mint_crl_pki("baseline");
    let acceptor =
        mqtt_net::tls::server_acceptor(&pki.server_cert, &pki.server_key, Some(&pki.ca)).unwrap();
    let addr = start_tls_node(acceptor).await;

    let connector = test_connector(&pki.ca, Some((&pki.revoked_cert, &pki.revoked_key)));
    let mut c = Client::connect(tls_connect(addr, &connector).await.unwrap(), "no-crl").await;
    c.subscribe("ok/#").await;
}

/// Start an mTLS node whose acceptor is rebuilt by a [`reload::Reloader`]; the CRL is applied
/// only when `crl_active` is set, so a reload can switch revocation on in place (ADR 0032 §5).
async fn start_reloadable_mtls_node(
    pki: &CrlPki,
    crl_active: Arc<std::sync::atomic::AtomicBool>,
) -> (SocketAddr, mqttd::reload::Reloader) {
    use std::sync::atomic::Ordering;
    let make_policy = || -> mqttd::reload::BuildResult {
        Ok((
            Arc::new(mqtt_auth::AllowAll) as Arc<dyn mqtt_auth::Authorizer>,
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }) as Arc<dyn mqtt_auth::Authenticator>,
        ))
    };
    let audit = Arc::new(mqtt_observability::AuditLog::new());
    let (mut reloader, handles) =
        mqttd::reload::Reloader::new(make_policy().unwrap(), audit, make_policy);

    let (cert, key, ca, crl) = (
        pki.server_cert.clone(),
        pki.server_key.clone(),
        pki.ca.clone(),
        pki.crl.clone(),
    );
    let build = move || {
        let crl_opt = crl_active.load(Ordering::SeqCst).then_some(crl.as_path());
        mqtt_net::tls::server_acceptor_with_crl(&cert, &key, Some(&ca), crl_opt)
            .map_err(|e| e.to_string())
    };
    let acceptor = build().expect("initial acceptor");
    let acceptor_rx = reloader.attach_tls(acceptor, build);

    let (hub, hub_tx) = Hub::with_config(
        NodeId("crlreload-node".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let acceptor = acceptor_rx.borrow().clone();
            let auth = mqttd::conn::auth_handle(handles.auth.borrow().clone());
            let authz = mqttd::conn::authz_handle(handles.authz.borrow().clone());
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(stream).await {
                    let policy = Arc::new(mqttd::conn::ConnPolicy {
                        auth,
                        authz,
                        audit: Arc::new(mqtt_observability::AuditLog::new()),
                        proxy: None,
                        store: None,
                        connect_timeout: Duration::from_secs(10),
                        shutdown: None,
                        metrics: None,
                        enhanced: None,
                    });
                    mqttd::conn::handle_stream(tls, Some(peer), None, policy, hub).await;
                }
            });
        }
    });
    (addr, reloader)
}

/// Publishing a CRL via reload takes effect on the next handshake: a client cert that
/// connected fine before the reload is rejected after, with no restart (ADR 0032 §5).
#[tokio::test]
async fn reloading_a_crl_revokes_a_client_in_place() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let pki = mint_crl_pki("reload");
    let crl_active = Arc::new(AtomicBool::new(false));
    let (addr, reloader) = start_reloadable_mtls_node(&pki, crl_active.clone()).await;

    // Before the CRL is active, the to-be-revoked cert connects normally.
    let revoked = test_connector(&pki.ca, Some((&pki.revoked_cert, &pki.revoked_key)));
    let mut c = Client::connect(tls_connect(addr, &revoked).await.unwrap(), "pre").await;
    c.subscribe("ok/#").await;
    drop(c);

    // Activate the CRL and reload in place.
    crl_active.store(true, Ordering::SeqCst);
    assert!(reloader.reload("signal"), "the CRL reload should apply");

    // The same cert is now rejected on a fresh handshake...
    assert_no_session(addr, &revoked).await;
    // ...while a non-revoked cert from the same CA still connects.
    let valid = test_connector(&pki.ca, Some((&pki.valid_cert, &pki.valid_key)));
    let mut ok = Client::connect(tls_connect(addr, &valid).await.unwrap(), "post").await;
    ok.subscribe("ok/#").await;
}
