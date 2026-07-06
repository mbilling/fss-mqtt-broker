//! MQTT-over-QUIC transport integration tests (ADR 0036): a real `quinn` client opens a QUIC
//! connection (ALPN `mqtt`, presenting a client certificate) and completes a pub/sub round-trip
//! — its leaf-cert CN becoming the session identity; a certless client is refused (QUIC mTLS);
//! and **multi-stream** publishes across two data streams feed one session without
//! head-of-line blocking.
//!
//! The server serves each connection through `mqtt_net::quic::accept_mux`; the MQTT session runs
//! over the standard `FrameReader`/`FrameWriter` — proving the QUIC streams are transparent to
//! the MQTT engine.

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
                    .and_then(|c| mqttd::conn::cert_admission(&c));
                // The multi-stream mux: control stream + any data streams feed one session.
                if let Ok(mux) = mqtt_net::quic::accept_mux(conn).await {
                    mqttd::conn::handle_stream(mux, Some(peer), identity, permissive_policy(), hub)
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

struct Client<S> {
    reader: mqtt_net::FrameReader<tokio::io::ReadHalf<S>>,
    writer: mqtt_net::FrameWriter<tokio::io::WriteHalf<S>>,
    conn: quinn::Connection,
}

impl Client<QuicStream> {
    /// Connect over a single control bidi stream (no multi-stream mux) — a plain client.
    async fn connect(endpoint: &quinn::Endpoint, addr: SocketAddr, id: &str) -> Self {
        let conn = endpoint
            .connect(addr, "127.0.0.1")
            .unwrap()
            .await
            .expect("QUIC connect");
        let (send, recv) = conn.open_bi().await.expect("open control stream");
        Self::handshake(conn.clone(), mqtt_net::quic::byte_stream(send, recv), id).await
    }
}

impl Client<mqtt_net::quic::QuicMux> {
    /// Connect over the symmetric multi-stream mux: signals capability and accepts broker-opened
    /// data streams, so the broker may fan its deliveries back across streams.
    async fn connect_mux(endpoint: &quinn::Endpoint, addr: SocketAddr, id: &str) -> Self {
        let conn = endpoint
            .connect(addr, "127.0.0.1")
            .unwrap()
            .await
            .expect("QUIC connect");
        let mux = mqtt_net::quic::connect_mux(&conn)
            .await
            .expect("connect mux");
        Self::handshake(conn, mux, id).await
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Client<S> {
    /// Send CONNECT over an already-open QUIC byte stream and assert CONNACK 0x00.
    async fn handshake(conn: quinn::Connection, stream: S, id: &str) -> Self {
        let (rh, wh) = tokio::io::split(stream);
        let mut c = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
            conn,
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

    /// Publish at `QoS` 1 and wait for the matching PUBACK, returning `true` if it arrives. Used to
    /// prove the broker→client return path works (e.g. across a connection migration).
    async fn publish_qos1_acked(&mut self, topic: &str, payload: &'static [u8], pkid: u16) -> bool {
        self.writer
            .send(&Packet::Publish(Publish {
                properties: mqtt_codec::Properties::new(),
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: topic.to_string(),
                pkid: Some(pkid),
                payload: bytes::Bytes::from_static(payload),
            }))
            .await
            .unwrap();
        matches!(self.recv().await, Some(Packet::PubAck(ack)) if ack.pkid == pkid)
    }

    /// Open a new QUIC data stream on this connection and return its send half (for raw
    /// PUBLISH bytes, exercising the multi-stream mux).
    async fn open_data_stream(&self) -> quinn::SendStream {
        let (send, _recv) = self.conn.open_bi().await.expect("open data stream");
        send
    }

    async fn recv(&mut self) -> Option<Packet> {
        timeout(Duration::from_millis(800), self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

/// Encode a QoS-0 PUBLISH to its wire bytes (for writing raw onto a QUIC data stream).
fn encode_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    Packet::Publish(Publish {
        properties: mqtt_codec::Properties::new(),
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: topic.to_string(),
        pkid: None,
        payload: bytes::Bytes::copy_from_slice(payload),
    })
    .encode(&mut out, V4)
    .unwrap();
    out
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

/// Multi-stream demux (ADR 0036): two QUIC data streams carry independent PUBLISH flows into
/// **one** MQTT session, and a stalled/incomplete publish on one stream does **not** block
/// delivery of a complete publish on the other — QUIC's no-head-of-line-blocking benefit,
/// which a single TCP/TLS byte stream cannot give.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_multistream_demux_no_head_of_line_blocking() {
    let pki = mint_pki("mux");
    let addr = start_quic_node(&pki.cert, &pki.key, &pki.ca);
    let client = quic_client(&pki.ca, Some((&pki.cert, &pki.key)));

    let mut sub = Client::connect(&client, addr, "mux-sub").await;
    sub.subscribe("t/#").await;

    // The publisher's CONNECT goes on its control stream; PUBLISHes go on data streams.
    let publ = Client::connect(&client, addr, "mux-pub").await;

    // Data stream A: a LARGE publish, but only its first bytes — an *incomplete* frame the mux's
    // per-stream reader must buffer (the remaining-length header promises ~100 KB more).
    let big = encode_publish("t/slow", &vec![b'x'; 100_000]);
    let mut stream_a = publ.open_data_stream().await;
    stream_a.write_all(&big[..32]).await.unwrap(); // header + topic + a little payload; rest held

    // Data stream B: a COMPLETE small publish, sent while A is still mid-frame.
    let mut stream_b = publ.open_data_stream().await;
    stream_b
        .write_all(&encode_publish("t/fast", b"fast"))
        .await
        .unwrap();
    let _ = stream_b.finish();

    // B must arrive even though A's frame is incomplete — independent streams, no HoL blocking.
    match sub.recv().await {
        Some(Packet::Publish(p)) => {
            assert_eq!(p.topic, "t/fast", "the unstalled stream must deliver first");
            assert_eq!(&p.payload[..], b"fast");
        }
        other => panic!("expected t/fast while the other stream is stalled, got {other:?}"),
    }

    // Completing A's frame demuxes its publish into the same session — proving N streams feed
    // one session.
    stream_a.write_all(&big[32..]).await.unwrap();
    let _ = stream_a.finish();
    match sub.recv().await {
        Some(Packet::Publish(p)) => {
            assert_eq!(p.topic, "t/slow");
            assert_eq!(
                p.payload.len(),
                100_000,
                "the large publish survives the demux intact"
            );
        }
        other => panic!("expected t/slow after completion, got {other:?}"),
    }
}

/// Outbound multi-stream fan-out (ADR 0036 §3a): a capability-signalling subscriber (the
/// symmetric mux) receives **every** broker→client delivery — fanned across broker-opened QUIC
/// data streams and re-merged by the subscriber's mux. (A plain single-stream client is never
/// stranded — it gets its publishes on the control stream, see `quic_mtls_pubsub_roundtrip`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_outbound_fans_publishes_across_streams() {
    let pki = mint_pki("out");
    let addr = start_quic_node(&pki.cert, &pki.key, &pki.ca);
    let client = quic_client(&pki.ca, Some((&pki.cert, &pki.key)));

    let mut sub = Client::connect_mux(&client, addr, "out-sub").await;
    sub.subscribe("t/#").await;
    // Signal multi-stream capability AFTER connect (CONNECT must be the first packet): open a
    // data stream and send a PINGREQ. The broker then fans this subscriber's deliveries across
    // data streams (and the subscriber's mux accepts those broker-opened streams).
    let mut sig = sub.open_data_stream().await;
    sig.write_all(&[0xC0, 0x00]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut publ = Client::connect(&client, addr, "out-pub").await;
    let topics = ["t/a", "t/b", "t/c", "t/d", "t/e", "t/f"];
    for t in topics {
        publ.publish(t, b"payload").await;
    }

    // Every publish must arrive intact at the subscriber — proving the broker fanned them across
    // data streams and the subscriber's mux re-merged them (a delivery on an unaccepted stream
    // would be lost). The PINGRESP from the capability signal is skipped.
    let mut got = std::collections::HashSet::new();
    for _ in 0..(topics.len() + 3) {
        match sub.recv().await {
            Some(Packet::Publish(p)) => {
                assert_eq!(&p.payload[..], b"payload");
                got.insert(p.topic);
            }
            Some(Packet::PingResp) => {}
            None => break,
            other => panic!("unexpected packet over the outbound mux: {other:?}"),
        }
        if got.len() == topics.len() {
            break;
        }
    }
    assert_eq!(
        got.len(),
        topics.len(),
        "every fanned-out publish arrives intact via the symmetric mux"
    );
}

/// Connection migration (ADR 0036 §3b): QUIC keeps a connection alive across a **client path
/// change**. We rebind the publisher's endpoint to a fresh UDP socket (a new source address on the
/// *same* connection — what a Wi-Fi↔cellular handover or NAT rebind looks like server-side), then
/// assert the session survived without re-establishing: the connection object is unchanged
/// (`stable_id`), the local path actually moved, and **both directions** keep working on the new
/// path — a forward PUBLISH reaches the subscriber and a QoS-1 PUBLISH gets its PUBACK back. A
/// reconnect could fake none of these together.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_connection_migration_survives_path_change() {
    let pki = mint_pki("migrate");
    let addr = start_quic_node(&pki.cert, &pki.key, &pki.ca);

    // Separate endpoints so rebinding the publisher's socket does not touch the subscriber.
    let sub_client = quic_client(&pki.ca, Some((&pki.cert, &pki.key)));
    let pub_client = quic_client(&pki.ca, Some((&pki.cert, &pki.key)));

    let mut sub = Client::connect(&sub_client, addr, "migrate-sub").await;
    sub.subscribe("migrate/#").await;

    let mut publ = Client::connect(&pub_client, addr, "migrate-pub").await;
    publ.publish("migrate/before", b"before").await;
    match sub.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(&p.payload[..], b"before"),
        other => panic!("expected the pre-migration publish, got {other:?}"),
    }

    // Move the path: rebind the publisher's endpoint to a new ephemeral UDP port. quinn migrates
    // the live connection onto it; the next packet leaves from a new source address.
    let id_before = publ.conn.stable_id();
    let addr_before = pub_client.local_addr().unwrap();
    pub_client
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .expect("rebind to a new local socket");
    let addr_after = pub_client.local_addr().unwrap();

    assert_ne!(
        addr_before, addr_after,
        "the rebind must actually move the client's local path"
    );
    assert_eq!(
        id_before,
        publ.conn.stable_id(),
        "migration keeps the SAME connection — not a reconnect"
    );

    // Forward path on the new route: a publish still reaches the subscriber.
    publ.publish("migrate/after", b"after").await;
    match sub.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(
            &p.payload[..],
            b"after",
            "delivery continues over the migrated path"
        ),
        other => panic!("expected the post-migration publish, got {other:?}"),
    }

    // Return path on the new route: a QoS-1 publish gets its PUBACK back from the broker.
    assert!(
        publ.publish_qos1_acked("migrate/ack", b"q1", 1).await,
        "the broker→client return path (PUBACK) must work after migration"
    );
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
