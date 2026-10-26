//! A subscriber that writes its PUBACKs before it reads again must not freeze with its
//! connection: the broker has to keep reading a connection it is blocked writing to.
//!
//! Found by the rig2 msg/s ramp (ADR 0090): clean-session `QoS` 1 through `$share`
//! groups past the knee. 47 of 60 consumers stopped receiving anything for good while
//! their connections stayed up; each of their sessions held exactly 65 535 unacked
//! in-flight deliveries (the whole Receive Maximum) and a full 10 000-message backlog,
//! and the generator could not stop them (its consumer tasks never returned).
//!
//! The mechanism is a write/write deadlock between the two ends of one connection.
//! The connection task (`conn.rs`, `serve`) runs inbound and outbound in ONE select
//! loop: the outbound arm writes up to `OUTBOUND_BATCH_MAX` (1024) packets and then
//! awaits `flush_queued` — `write_all` + `flush`, no timeout — while the inbound arm
//! reads ONE packet per turn. A consumer behind by a large window therefore acks ~1024
//! messages for every one or two PUBACKs the broker reads, so its PUBACKs pile up in
//! the broker's receive buffer. Once that buffer and the client's send buffer are
//! full, a client that writes its acks before reading again (mqtt-load does:
//! `write_all`, then `read_buf`) blocks in that write and stops reading; the broker's
//! write then blocks too, and neither side reads again. The keep-alive timer is not
//! polled while the outbound arm awaits the flush, so nothing ever closes it.
//!
//! The test: a `QoS` 1 subscriber with Receive Maximum 65 535 lets a backlog of
//! acknowledged messages queue up, then consumes like mqtt-load — decode every whole
//! packet read, write all their PUBACKs, read again. Every message must arrive. The
//! subscriber's connection is an in-memory duplex with a fixed buffer per direction,
//! standing in for the kernel's socket buffers so the result does not depend on the
//! host's TCP autotuning (a Linux socket starts at 128 KiB receive / 16 KiB send and
//! grows only as fast as its reader drains it; this broker is not draining it).
//!
//! The same scenario runs over TLS and over WebSocket on the same duplex: the fix
//! (issue #1117) makes the socket write an arm of the connection's select, which must
//! hold for every transport's writer, not only the plain TCP one.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscribeFilter};
use mqtt_codec::{Packet, Properties, Property, ProtocolVersion, QoS, SubscriptionOptions};
use mqttd::conn::ConnPolicy;
use mqttd::hub::{Hub, HubCommand};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

const V5: ProtocolVersion = ProtocolVersion::V5;
/// Within the subscriber's Receive Maximum (so all of them go out unacked at once),
/// and their PUBACKs (4 bytes each, 240 KB) far exceed both directions' buffers.
const MESSAGES: u32 = 60_000;
const PAYLOAD: usize = 256;
/// Bytes each direction of the subscriber's connection buffers.
const PIPE_BUF: usize = 64 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// How long one `write_all` of a batch of PUBACKs may take before the subscriber
/// concludes the broker has stopped reading its connection.
const ACK_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

fn policy() -> Arc<ConnPolicy> {
    Arc::new(ConnPolicy {
        anonymous: None,
        auth: mqttd::conn::auth_handle(Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        })),
        enhanced: None,
        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
        identity_source: mqtt_auth::mtls::IdentitySource::default(),
        audit: Arc::new(mqtt_observability::AuditLog::new()),
        proxy: None,
        node: None,
        store: None,
        connect_timeout: IO_TIMEOUT,
        shutdown: None,
        metrics: None,
        ingress: None,
        rules: None,
    })
}

async fn start_broker() -> (
    std::net::SocketAddr,
    mpsc::UnboundedSender<HubCommand>,
    Arc<mqtt_observability::metrics::Metrics>,
) {
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("ad"));
    let (mut hub, hub_tx) = Hub::new();
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
        }
    });
    (addr, hub_tx, metrics)
}

async fn send<S: AsyncWrite + Unpin>(stream: &mut S, p: &Packet) {
    let mut out = Vec::new();
    p.encode(&mut out, V5).unwrap();
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
}

async fn recv<S: AsyncRead + Unpin>(stream: &mut S, buf: &mut BytesMut) -> Packet {
    tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            if let Some(p) = Packet::decode(buf, V5).unwrap() {
                return p;
            }
            assert!(stream.read_buf(buf).await.unwrap() > 0, "broker closed");
        }
    })
    .await
    .expect("a packet within the timeout")
}

/// CONNECT (clean, MQTT 5, Receive Maximum 65535) and CONNACK; returns the broker's
/// Receive Maximum.
async fn connect<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    id: &str,
) -> u16 {
    send(
        stream,
        &Packet::Connect(Connect {
            protocol: V5,
            clean_session: true,
            keep_alive: 60,
            client_id: id.into(),
            last_will: None,
            username: None,
            password: None,
            properties: vec![Property::ReceiveMaximum(u16::MAX)].into(),
        }),
    )
    .await;
    match recv(stream, buf).await {
        Packet::ConnAck(a) if a.code == 0 => a.properties.receive_maximum().unwrap_or(u16::MAX),
        other => panic!("expected a successful CONNACK, got {other:?}"),
    }
}

fn delivered_total(metrics: &mqtt_observability::metrics::Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .filter(|l| l.starts_with("mqttd_publish_delivered_total"))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

/// Publish `MESSAGES` `QoS` 1 messages (payload: the index, big-endian, then zeros),
/// a window at a time, each acknowledged before the next window.
async fn publish_all(addr: std::net::SocketAddr) {
    let mut publ = TcpStream::connect(addr).await.unwrap();
    let mut buf = BytesMut::new();
    let window = u32::from(connect(&mut publ, &mut buf, "ad-pub").await.min(100));
    let mut next = 0u32;
    while next < MESSAGES {
        let n = window.min(MESSAGES - next);
        let mut out = Vec::new();
        for i in next..next + n {
            let mut payload = vec![0u8; PAYLOAD];
            payload[..4].copy_from_slice(&i.to_be_bytes());
            Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: "ad/t".into(),
                pkid: Some(u16::try_from(i % 60_000 + 1).unwrap()),
                payload: Bytes::from(payload),
                properties: Properties::new(),
            })
            .encode(&mut out, V5)
            .unwrap();
        }
        publ.write_all(&out).await.unwrap();
        for _ in 0..n {
            match recv(&mut publ, &mut buf).await {
                Packet::PubAck(a) => assert!(a.reason < 0x80, "PUBACK refused: {a:?}"),
                other => panic!("expected PUBACK, got {other:?}"),
            }
        }
        next += n;
    }
}

/// How the consumer's loop ended.
#[derive(Debug, PartialEq, Eq)]
enum End {
    /// Every message arrived.
    AllIn,
    /// A `write_all` of PUBACKs did not complete within [`ACK_WRITE_TIMEOUT`]: the
    /// broker is not reading this connection.
    AckWriteBlocked,
    /// Nothing arrived for [`IO_TIMEOUT`].
    Quiet,
}

/// Consume like mqtt-load's subscriber: decode every whole packet read, write all
/// their PUBACKs in one `write_all`, then read again. Returns the indices received
/// and how the loop ended.
async fn consume<S: AsyncRead + AsyncWrite + Unpin>(
    sub: &mut S,
    buf: &mut BytesMut,
) -> (BTreeSet<u32>, End) {
    let mut got = BTreeSet::new();
    let mut acks = Vec::new();
    loop {
        while let Some(p) = Packet::decode(buf, V5).unwrap() {
            let Packet::Publish(p) = p else { continue };
            let pkid = p.pkid.expect("QoS 1 delivery has a packet id");
            got.insert(u32::from_be_bytes(p.payload[..4].try_into().unwrap()));
            let [hi, lo] = pkid.to_be_bytes();
            acks.extend_from_slice(&[0x40, 0x02, hi, lo]);
        }
        if !acks.is_empty() {
            // The flush is a no-op on the plain duplex; TLS and WebSocket send on it.
            let write = async {
                sub.write_all(&acks).await?;
                sub.flush().await
            };
            match tokio::time::timeout(ACK_WRITE_TIMEOUT, write).await {
                Ok(r) => r.unwrap(),
                Err(_) => return (got, End::AckWriteBlocked),
            }
            acks.clear();
        }
        if got.len() == MESSAGES as usize {
            return (got, End::AllIn);
        }
        buf.reserve(64 * 1024);
        match tokio::time::timeout(IO_TIMEOUT, sub.read_buf(buf)).await {
            Ok(Ok(n)) if n > 0 => {}
            Ok(r) => panic!("broker closed the subscriber's connection: {r:?}"),
            Err(_) => return (got, End::Quiet),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_that_acks_before_reading_again_receives_its_whole_window() {
    let (addr, hub_tx, metrics) = start_broker().await;
    let (sub, broker_side) = tokio::io::duplex(PIPE_BUF);
    tokio::spawn(mqttd::conn::handle_stream(
        broker_side,
        None,
        None,
        policy(),
        hub_tx,
    ));
    receives_whole_window(sub, addr, &metrics).await;
}

/// The same subscriber over TLS 1.3: the broker's writer is a rustls stream, which
/// buffers records of its own and sends them on flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_tls_a_subscriber_that_acks_before_reading_again_receives_its_whole_window() {
    let (addr, hub_tx, metrics) = start_broker().await;
    let (acceptor, connector) = tls_pair();
    let (sub, broker_side) = tokio::io::duplex(PIPE_BUF);
    tokio::spawn(async move {
        let tls = acceptor.accept(broker_side).await.expect("TLS handshake");
        mqttd::conn::handle_stream(tls, None, None, policy(), hub_tx).await
    });
    let sub = connector
        .connect(
            rustls_pki_types::ServerName::try_from("localhost").unwrap(),
            sub,
        )
        .await
        .expect("TLS handshake");
    receives_whole_window(sub, addr, &metrics).await;
}

/// The same subscriber over WebSocket: the broker's writer turns each write into a
/// binary frame and sends it on flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_websocket_a_subscriber_that_acks_before_reading_again_receives_its_whole_window() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let (addr, hub_tx, metrics) = start_broker().await;
    let (sub, broker_side) = tokio::io::duplex(PIPE_BUF);
    tokio::spawn(async move {
        let ws = mqtt_net::ws::accept(broker_side)
            .await
            .expect("WebSocket upgrade");
        mqttd::conn::handle_stream(ws, None, None, policy(), hub_tx).await
    });
    let mut req = "ws://localhost/".into_client_request().unwrap();
    req.headers_mut().insert(
        "sec-websocket-protocol",
        tokio_tungstenite::tungstenite::http::HeaderValue::from_static("mqtt"),
    );
    let (ws, _) = tokio_tungstenite::client_async(req, sub)
        .await
        .expect("WebSocket upgrade");
    receives_whole_window(mqtt_net::ws::WsByteStream::wrap(ws), addr, &metrics).await;
}

/// A throwaway self-signed TLS 1.3 server for `localhost`, and a client trusting it.
fn tls_pair() -> (tokio_rustls::TlsAcceptor, tokio_rustls::TlsConnector) {
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let server = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            rustls_pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (
        tokio_rustls::TlsAcceptor::from(Arc::new(server)),
        tokio_rustls::TlsConnector::from(Arc::new(client)),
    )
}

/// Subscribe over `sub`, let the whole window queue up unread, then consume it the way
/// a write-then-read client does: every message must arrive.
async fn receives_whole_window<S: AsyncRead + AsyncWrite + Unpin>(
    mut sub: S,
    addr: std::net::SocketAddr,
    metrics: &mqtt_observability::metrics::Metrics,
) {
    let mut sub_buf = BytesMut::new();
    connect(&mut sub, &mut sub_buf, "ad-sub").await;
    send(
        &mut sub,
        &Packet::Subscribe(Subscribe {
            pkid: 1,
            filters: vec![SubscribeFilter {
                path: "ad/t".into(),
                qos: QoS::AtLeastOnce,
                options: SubscriptionOptions::default(),
            }],
            properties: Properties::new(),
        }),
    )
    .await;
    match recv(&mut sub, &mut sub_buf).await {
        Packet::SubAck(a) => assert_eq!(a.return_codes, vec![1], "QoS 1 granted"),
        other => panic!("expected SUBACK, got {other:?}"),
    }

    // Every message acknowledged to its publisher; the consumer reads nothing meanwhile,
    // so all of them are in flight to it at once (within its Receive Maximum).
    publish_all(addr).await;
    let deadline = Instant::now() + IO_TIMEOUT;
    while delivered_total(metrics) < u64::from(MESSAGES) {
        assert!(
            Instant::now() < deadline,
            "the hub never counted every message delivered: {}",
            delivered_total(metrics)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let started = Instant::now();
    let (got, end) = consume(&mut sub, &mut sub_buf).await;
    let inflight = metrics
        .render()
        .lines()
        .find(|l| l.starts_with("mqttd_inflight_messages "))
        .unwrap_or("mqttd_inflight_messages absent")
        .to_string();
    assert_eq!(
        end,
        End::AllIn,
        "the subscriber got {} of {MESSAGES} messages, then {:?} after {:?}: its PUBACK \
         write blocked because the broker stopped reading the connection while blocked \
         writing to it ({inflight})",
        got.len(),
        end,
        started.elapsed(),
    );
}
