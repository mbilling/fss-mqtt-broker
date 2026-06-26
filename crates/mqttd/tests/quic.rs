//! MQTT-over-QUIC transport integration tests (ADR 0036): a real `quinn` client opens a QUIC
//! connection (ALPN `mqtt`, presenting a client certificate) and a bidirectional control
//! stream, then completes a pub/sub round-trip — its leaf-cert CN becoming the session
//! identity. A client without a certificate is refused (QUIC mTLS).
//!
//! The MQTT session runs over the QUIC bidi stream via `mqtt_net::quic::byte_stream`, driving
//! the standard `FrameReader`/`FrameWriter` — exactly as a TCP client, proving the QUIC stream
//! is transparent to the MQTT engine.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqtt_storage::MemorySessionStore;
use mqttd::conn::{auth_handle, authz_handle, ConnPolicy};
use mqttd::Hub;
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::time::timeout;

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

/// Start a QUIC node requiring a client certificate (mTLS); returns its UDP address. (Sync —
/// it only builds the endpoint and spawns the accept loop; must be called within a runtime.)
fn start_quic_node(cert: &Path, key: &Path, ca: &Path) -> SocketAddr {
    let endpoint =
        mqtt_net::quic::server_endpoint("127.0.0.1:0".parse().unwrap(), cert, key, Some(ca))
            .unwrap();
    let addr = endpoint.local_addr().unwrap();

    let (hub, hub_tx) = Hub::with_config(
        NodeId("quic-node".into()),
        Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else { return };
                let peer = conn.remote_address();
                let identity = mqtt_net::quic::peer_leaf_cert(&conn)
                    .and_then(|c| mqtt_auth::mtls::identity_from_cert(&c).ok());
                if let Ok((send, recv)) = conn.accept_bi().await {
                    let stream = mqtt_net::quic::byte_stream(send, recv);
                    mqttd::conn::handle_stream(
                        stream,
                        Some(peer),
                        identity,
                        permissive_policy(),
                        hub,
                    )
                    .await;
                }
            });
        }
    });
    addr
}

// --- throwaway PKI ------------------------------------------------------------

struct Pki {
    ca: std::path::PathBuf,
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
}

fn mint_pki(tag: &str) -> Pki {
    use std::sync::atomic::{AtomicU64, Ordering};
    static UNIQUE: AtomicU64 = AtomicU64::new(0);
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("mqttd-quic-{}-{tag}-{n}", std::process::id()));
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

/// A QUIC client endpoint that trusts `ca`, with ALPN `mqtt`, optionally presenting a client
/// certificate.
fn quic_client(ca: &Path, client_identity: Option<(&Path, &Path)>) -> quinn::Endpoint {
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
    let mut crypto = match client_identity {
        Some((cert, key)) => {
            let key = PrivateKeyDer::from_pem_file(key).unwrap();
            builder.with_client_auth_cert(pem_certs(cert), key).unwrap()
        }
        None => builder.with_no_client_auth(),
    };
    crypto.alpn_protocols = vec![b"mqtt".to_vec()];

    let qcc = QuicClientConfig::try_from(crypto).unwrap();
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(qcc)));
    endpoint
}

// --- QUIC MQTT client --------------------------------------------------------

type QuicStream = tokio::io::Join<quinn::RecvStream, quinn::SendStream>;

struct Client {
    reader: mqtt_net::FrameReader<tokio::io::ReadHalf<QuicStream>>,
    writer: mqtt_net::FrameWriter<tokio::io::WriteHalf<QuicStream>>,
}

impl Client {
    /// Connect over QUIC: open a connection + the control (bidi) stream, send CONNECT, assert
    /// CONNACK 0x00.
    async fn connect(endpoint: &quinn::Endpoint, addr: SocketAddr, id: &str) -> Self {
        let conn = endpoint
            .connect(addr, "127.0.0.1")
            .unwrap()
            .await
            .expect("QUIC connect");
        let (send, recv) = conn.open_bi().await.expect("open control stream");
        let (rh, wh) = tokio::io::split(mqtt_net::quic::byte_stream(send, recv));
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
            other => panic!("expected CONNACK 0x00 over QUIC, got {other:?}"),
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
        timeout(Duration::from_millis(800), self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

// --- tests -------------------------------------------------------------------

/// A pub/sub round-trip over QUIC with mTLS: the client presents a cert, the control stream
/// carries the MQTT session, and a publish reaches a subscriber.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_mtls_pubsub_roundtrip() {
    let pki = mint_pki("rt");
    let addr = start_quic_node(&pki.cert, &pki.key, &pki.ca);
    let client = quic_client(&pki.ca, Some((&pki.cert, &pki.key)));

    let mut sub = Client::connect(&client, addr, "quic-sub").await;
    sub.subscribe("quic/+/data").await;

    let mut publ = Client::connect(&client, addr, "quic-pub").await;
    publ.publish("quic/zone/data", b"over-quic").await;

    match sub.recv().await {
        Some(Packet::Publish(p)) => {
            assert_eq!(p.topic, "quic/zone/data");
            assert_eq!(&p.payload[..], b"over-quic");
        }
        other => panic!("expected the publish over QUIC, got {other:?}"),
    }
}

/// A client that presents no certificate is refused — QUIC mTLS is enforced. The server's
/// handshake rejects a certless client, so no MQTT session forms: even if the client
/// optimistically opens a stream and writes a CONNECT (QUIC stream writes are buffered
/// locally), **no CONNACK ever comes back**. That MQTT-layer check is the reliable one — a raw
/// `open_bi`/`write` can succeed locally before the server's rejection propagates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_without_client_cert_is_refused() {
    let pki = mint_pki("nocert");
    let addr = start_quic_node(&pki.cert, &pki.key, &pki.ca);
    let client = quic_client(&pki.ca, None); // no client identity

    // Drive the full MQTT CONNECT over QUIC and assert no CONNACK arrives.
    let got_connack = async {
        let conn = client.connect(addr, "127.0.0.1").ok()?.await.ok()?;
        let (send, recv) = conn.open_bi().await.ok()?;
        let (rh, wh) = tokio::io::split(mqtt_net::quic::byte_stream(send, recv));
        let mut writer = mqtt_net::FrameWriter::new(wh, V4);
        let mut reader = mqtt_net::FrameReader::new(rh, V4);
        writer
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
            .await
            .ok()?;
        matches!(reader.next_packet().await, Ok(Some(Packet::ConnAck(_)))).then_some(())
    };
    let outcome = timeout(Duration::from_secs(3), got_connack).await;
    assert!(
        !matches!(outcome, Ok(Some(()))),
        "a QUIC client without a certificate must never receive a CONNACK (mTLS enforced)"
    );
}
