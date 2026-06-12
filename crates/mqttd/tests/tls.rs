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
    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
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
        Box::new(MemorySessionStore::new()),
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
                    mqttd::conn::handle_stream(
                        tls,
                        Some(peer),
                        None,
                        auth,
                        Arc::new(mqtt_auth::AllowAll),
                        hub,
                    )
                    .await;
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
                pkid: 1,
                filters: vec![SubscribeFilter {
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

fn peer_tls(pki: &Pki) -> PeerTls {
    PeerTls {
        acceptor: mqtt_net::tls::server_acceptor(&pki.cert, &pki.key, Some(&pki.ca)).unwrap(),
        connector: mqtt_net::tls::client_connector(&pki.ca, &pki.cert, &pki.key).unwrap(),
    }
}

/// Two-node cluster whose peer links run mutual TLS; returns client addresses
/// and node B's peer-listener address (for the rejection test).
async fn start_mtls_cluster() -> (SocketAddr, SocketAddr, SocketAddr) {
    let (pki, _, _) = mint_pki("cluster");

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
    let (hub_a, tx_a) = Hub::with_config(id_a.clone(), Box::new(MemorySessionStore::new()));
    let (hub_b, tx_b) = Hub::with_config(id_b.clone(), Box::new(MemorySessionStore::new()));
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
        Some(peer_tls(&pki)),
    ));
    tokio::spawn(mqttd::peer::serve_listener(
        peer_b,
        id_b.clone(),
        tx_b.clone(),
        Some(peer_tls(&pki)),
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_b.to_string(),
        id_a,
        tx_a,
        Some(peer_tls(&pki)),
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_a.to_string(),
        id_b,
        tx_b,
        Some(peer_tls(&pki)),
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
        Box::new(MemorySessionStore::new()),
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
                    mqttd::conn::handle_stream(
                        tls,
                        Some(peer),
                        identity,
                        auth,
                        Arc::new(mqtt_auth::AllowAll),
                        hub,
                    )
                    .await;
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
        Box::new(MemorySessionStore::new()),
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
                    mqttd::conn::handle_stream(
                        tls,
                        Some(peer),
                        identity,
                        auth,
                        Arc::new(mqtt_auth::AllowAll),
                        hub,
                    )
                    .await;
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
