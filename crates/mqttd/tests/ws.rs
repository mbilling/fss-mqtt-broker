//! MQTT-over-WebSocket transport integration tests (ADR 0035): a real WebSocket client does a
//! pub/sub round-trip against the broker over plaintext `ws://` and TLS `wss://` (with mTLS),
//! and a client that omits the `mqtt` subprotocol is refused at the handshake.
//!
//! The client uses the same `tokio-tungstenite` the broker uses server-side, wrapped with the
//! crate's own `WsByteStream` so it drives the standard `FrameReader`/`FrameWriter` — exactly
//! as a TCP client would, proving the WebSocket framing is transparent to the MQTT engine.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqtt_net::ws::WsByteStream;
use mqtt_storage::MemorySessionStore;
use mqttd::conn::{auth_handle, authz_handle, ConnPolicy};
use mqttd::Hub;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

const V4: ProtocolVersion = ProtocolVersion::V311;

// --- broker harness ----------------------------------------------------------

fn permissive_policy() -> Arc<ConnPolicy> {
    Arc::new(ConnPolicy {
        auth: auth_handle(Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        })),
        authz: authz_handle(Arc::new(mqtt_auth::AllowAll)),
        audit: Arc::new(mqtt_observability::AuditLog::new()),
        proxy: None,
        store: None,
        connect_timeout: Duration::from_secs(10),
        shutdown: None,
        metrics: None,
        enhanced: None,
    })
}

/// Start a node with a plaintext WebSocket client listener; returns its address.
async fn start_ws_node() -> SocketAddr {
    let (hub, hub_tx) = Hub::with_config(
        NodeId("ws-node".into()),
        Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                if let Ok(ws) = mqtt_net::ws::accept(stream).await {
                    mqttd::conn::handle_stream(ws, Some(peer), None, permissive_policy(), hub)
                        .await;
                }
            });
        }
    });
    addr
}

/// Start a node with a `wss://` listener requiring a client certificate (mTLS); returns its
/// address. The client identity is the verified leaf CN, exactly as for a TCP TLS client.
async fn start_wss_node(acceptor: tokio_rustls::TlsAcceptor) -> SocketAddr {
    let (hub, hub_tx) = Hub::with_config(
        NodeId("wss-node".into()),
        Arc::new(MemorySessionStore::new()),
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
                let Ok(tls) = acceptor.accept(stream).await else {
                    return;
                };
                let identity = mqttd::conn::tls_admission(&tls);
                if let Ok(ws) = mqtt_net::ws::accept(tls).await {
                    mqttd::conn::handle_stream(ws, Some(peer), identity, permissive_policy(), hub)
                        .await;
                }
            });
        }
    });
    addr
}

// --- throwaway PKI (mirrors tls.rs) ------------------------------------------

struct Pki {
    ca: std::path::PathBuf,
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
}

fn mint_pki(tag: &str) -> Pki {
    use std::sync::atomic::{AtomicU64, Ordering};
    static UNIQUE: AtomicU64 = AtomicU64::new(0);
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("mqttd-ws-{}-{tag}-{n}", std::process::id()));
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
    pki
}

fn pem_certs(path: &Path) -> Vec<CertificateDer<'static>> {
    CertificateDer::pem_file_iter(path)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn test_connector(ca: &Path, client_identity: Option<(&Path, &Path)>) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    for cert in pem_certs(ca) {
        roots.add(cert).unwrap();
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
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

// --- WebSocket MQTT client ---------------------------------------------------

/// A client `Sec-WebSocket-Protocol: mqtt` request for `addr`.
fn mqtt_ws_request(addr: SocketAddr) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    let mut req = format!("ws://{addr}/").into_client_request().unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", HeaderValue::from_static("mqtt"));
    req
}

struct Client<S> {
    reader: mqtt_net::FrameReader<tokio::io::ReadHalf<S>>,
    writer: mqtt_net::FrameWriter<tokio::io::WriteHalf<S>>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<S> {
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
        match c.recv().await {
            Some(Packet::ConnAck(ack)) if ack.code == 0 => c,
            other => panic!("expected CONNACK 0x00 over WebSocket, got {other:?}"),
        }
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
        timeout(Duration::from_millis(500), self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

// --- tests -------------------------------------------------------------------

/// A pub/sub round-trip over plaintext `ws://`: WebSocket framing is transparent to MQTT.
#[tokio::test]
async fn ws_pubsub_roundtrip() {
    let addr = start_ws_node().await;

    let tcp = TcpStream::connect(addr).await.unwrap();
    let (ws, _) = tokio_tungstenite::client_async(mqtt_ws_request(addr), tcp)
        .await
        .unwrap();
    let mut sub = Client::connect(WsByteStream::wrap(ws), "ws-sub").await;
    sub.subscribe("ws/+/data").await;

    let tcp = TcpStream::connect(addr).await.unwrap();
    let (ws, _) = tokio_tungstenite::client_async(mqtt_ws_request(addr), tcp)
        .await
        .unwrap();
    let mut publ = Client::connect(WsByteStream::wrap(ws), "ws-pub").await;
    publ.publish("ws/zone/data", b"over-websocket").await;

    match sub.recv().await {
        Some(Packet::Publish(p)) => {
            assert_eq!(p.topic, "ws/zone/data");
            assert_eq!(&p.payload[..], b"over-websocket");
        }
        other => panic!("expected the publish over WebSocket, got {other:?}"),
    }
}

/// A round-trip over `wss://` with mutual TLS: the client presents a CA-signed cert (its CN
/// becomes the session identity, as for a TCP TLS client) and the WebSocket runs over the TLS.
#[tokio::test]
async fn wss_mtls_pubsub_roundtrip() {
    let pki = mint_pki("wss");
    let acceptor = mqtt_net::tls::server_acceptor(&pki.cert, &pki.key, Some(&pki.ca)).unwrap();
    let addr = start_wss_node(acceptor).await;
    let connector = test_connector(&pki.ca, Some((&pki.cert, &pki.key)));
    let name = ServerName::try_from("127.0.0.1").unwrap();

    let tls = connector
        .connect(name.clone(), TcpStream::connect(addr).await.unwrap())
        .await
        .unwrap();
    let (ws, _) = tokio_tungstenite::client_async(mqtt_ws_request(addr), tls)
        .await
        .unwrap();
    let mut sub = Client::connect(WsByteStream::wrap(ws), "wss-sub").await;
    sub.subscribe("secure/#").await;

    let tls = connector
        .connect(name, TcpStream::connect(addr).await.unwrap())
        .await
        .unwrap();
    let (ws, _) = tokio_tungstenite::client_async(mqtt_ws_request(addr), tls)
        .await
        .unwrap();
    let mut publ = Client::connect(WsByteStream::wrap(ws), "wss-pub").await;
    publ.publish("secure/room", b"over-wss-mtls").await;

    match sub.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(&p.payload[..], b"over-wss-mtls"),
        other => panic!("expected the publish over wss+mTLS, got {other:?}"),
    }
}

/// A WebSocket client that does NOT offer the `mqtt` subprotocol is refused at the handshake —
/// the listener is for MQTT clients only.
#[tokio::test]
async fn ws_without_mqtt_subprotocol_is_refused() {
    let addr = start_ws_node().await;
    let tcp = TcpStream::connect(addr).await.unwrap();
    // A plain WS request (no Sec-WebSocket-Protocol: mqtt).
    let req = format!("ws://{addr}/").into_client_request().unwrap();
    let result = tokio_tungstenite::client_async(req, tcp).await;
    assert!(
        result.is_err(),
        "a WebSocket client without the mqtt subprotocol must be rejected"
    );
}
