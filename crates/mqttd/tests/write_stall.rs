//! A connection keeps reading its peer while a write to it is blocked, and closes a
//! peer that stops reading for good (issue #1117).
//!
//! The connection task used to await its socket write inside the outbound arm of its
//! select loop, so while a write was blocked nothing read the peer and the keepalive
//! timer was not polled. That deadlocked a subscriber that writes its PUBACKs before it
//! reads again (`outbound_ack_deadlock.rs`), and it held a peer that never reads again
//! forever. The write is now an arm of its own, and a peer that has accepted no bytes
//! for `limits.write_stall_keepalives` keepalive periods is closed and counted.
//!
//! Each subscriber here is an in-memory duplex with a small fixed buffer per direction,
//! so "the peer is not reading" fills it at once and deterministically. This binary sets
//! the process-wide wire limits once: four keepalive periods, so a keepalive-1 client
//! stalls out at 4 s, well after its own 1.5 s keepalive grace.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Ack, Connect, Publish, Subscribe, SubscribeFilter};
use mqtt_codec::{Packet, Properties, Property, ProtocolVersion, QoS, SubscriptionOptions};
use mqtt_observability::metrics::Metrics;
use mqttd::conn::ConnPolicy;
use mqttd::hub::{Hub, HubCommand};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::Instant;

const V5: ProtocolVersion = ProtocolVersion::V5;
/// Bytes each direction of a subscriber's duplex buffers: far less than one burst.
const PIPE_BUF: usize = 16 * 1024;
const MESSAGES: u32 = 2_000;
const PAYLOAD: usize = 256;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// `limits.write_stall_keepalives` for this binary.
const STALL_KEEPALIVES: u16 = 4;

fn set_limits() {
    mqttd::conn::set_wire_limits(mqttd::conn::WireLimits {
        write_stall_keepalives: STALL_KEEPALIVES,
        ..mqttd::conn::WireLimits::default()
    });
}

fn policy(metrics: &Arc<Metrics>) -> Arc<ConnPolicy> {
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
        metrics: Some(metrics.clone()),
        ingress: None,
        rules: None,
    })
}

struct Broker {
    addr: std::net::SocketAddr,
    hub_tx: mpsc::UnboundedSender<HubCommand>,
    metrics: Arc<Metrics>,
}

async fn start_broker() -> Broker {
    set_limits();
    let metrics = Arc::new(Metrics::new("ws"));
    let (mut hub, hub_tx) = Hub::new();
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, m) = (hub_tx.clone(), metrics.clone());
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle_stream(
                stream,
                None,
                None,
                policy(&m),
                tx.clone(),
            ));
        }
    });
    Broker {
        addr,
        hub_tx,
        metrics,
    }
}

impl Broker {
    /// A subscriber on an in-memory duplex of `PIPE_BUF` bytes per direction.
    fn duplex_client(&self) -> tokio::io::DuplexStream {
        let (client, broker_side) = tokio::io::duplex(PIPE_BUF);
        tokio::spawn(mqttd::conn::handle_stream(
            broker_side,
            None,
            None,
            policy(&self.metrics),
            self.hub_tx.clone(),
        ));
        client
    }

    /// `mqttd_connection_errors_total{reason}`.
    fn connection_errors(&self, reason: &str) -> u64 {
        let prefix = format!("mqttd_connection_errors_total{{reason=\"{reason}\"}} ");
        self.metrics
            .render()
            .lines()
            .find_map(|l| l.strip_prefix(&prefix)?.parse().ok())
            .unwrap_or(0)
    }

    /// Wait until `connection_errors(reason)` is 1; returns when that was seen.
    async fn closed_for(&self, reason: &str, within: Duration) -> Instant {
        let deadline = Instant::now() + within;
        loop {
            if self.connection_errors(reason) == 1 {
                return Instant::now();
            }
            assert!(
                Instant::now() < deadline,
                "no connection closed for {reason:?} within {within:?} (write-stall {}, keepalive {})",
                self.connection_errors("write-stall"),
                self.connection_errors("keepalive"),
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn encode(p: &Packet) -> Vec<u8> {
    let mut out = Vec::new();
    p.encode(&mut out, V5).unwrap();
    out
}

async fn send<S: AsyncWrite + Unpin>(stream: &mut S, p: &Packet) {
    stream.write_all(&encode(p)).await.unwrap();
}

async fn recv<S: AsyncRead + Unpin>(stream: &mut S, buf: &mut BytesMut) -> Option<Packet> {
    tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            if let Some(p) = Packet::decode(buf, V5).unwrap() {
                return Some(p);
            }
            match stream.read_buf(buf).await {
                Ok(n) if n > 0 => {}
                _ => return None,
            }
        }
    })
    .await
    .expect("a packet (or the close) within the timeout")
}

/// CONNECT (clean, v5, Receive Maximum 65535, `keep_alive`), expect a CONNACK, and
/// SUBSCRIBE `filter` at `QoS` 1.
async fn connect_and_subscribe<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    id: &str,
    keep_alive: u16,
    filter: &str,
) {
    send(
        stream,
        &Packet::Connect(Connect {
            protocol: V5,
            clean_session: true,
            keep_alive,
            client_id: id.into(),
            last_will: None,
            username: None,
            password: None,
            properties: vec![Property::ReceiveMaximum(u16::MAX)].into(),
        }),
    )
    .await;
    match recv(stream, buf).await {
        Some(Packet::ConnAck(a)) if a.code == 0 => {}
        other => panic!("expected a successful CONNACK, got {other:?}"),
    }
    send(
        stream,
        &Packet::Subscribe(Subscribe {
            pkid: 1,
            filters: vec![SubscribeFilter {
                path: filter.into(),
                qos: QoS::AtLeastOnce,
                options: SubscriptionOptions::default(),
            }],
            properties: Properties::new(),
        }),
    )
    .await;
    match recv(stream, buf).await {
        Some(Packet::SubAck(a)) => assert_eq!(a.return_codes, vec![1], "QoS 1 granted"),
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

fn publish(topic: &str, index: u32) -> Packet {
    let mut payload = vec![0u8; PAYLOAD];
    payload[..4].copy_from_slice(&index.to_be_bytes());
    Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: topic.into(),
        pkid: Some(u16::try_from(index % 60_000 + 1).unwrap()),
        payload: Bytes::from(payload),
        properties: Properties::new(),
    })
}

fn index_of(p: &Publish) -> u32 {
    u32::from_be_bytes(p.payload[..4].try_into().unwrap())
}

/// Publish `MESSAGES` `QoS` 1 messages to `topic` from a plain TCP client, 100 at a
/// time, each window acknowledged before the next.
async fn publish_all(addr: std::net::SocketAddr, topic: &str) {
    let mut publ = TcpStream::connect(addr).await.unwrap();
    let mut buf = BytesMut::new();
    send(
        &mut publ,
        &Packet::Connect(Connect {
            protocol: V5,
            clean_session: true,
            keep_alive: 60,
            client_id: format!("pub-{topic}"),
            last_will: None,
            username: None,
            password: None,
            properties: Properties::new(),
        }),
    )
    .await;
    assert!(matches!(recv(&mut publ, &mut buf).await, Some(Packet::ConnAck(a)) if a.code == 0));
    let mut next = 0;
    while next < MESSAGES {
        let n = 100.min(MESSAGES - next);
        let mut out = Vec::new();
        for i in next..next + n {
            out.extend(encode(&publish(topic, i)));
        }
        publ.write_all(&out).await.unwrap();
        for _ in 0..n {
            match recv(&mut publ, &mut buf).await {
                Some(Packet::PubAck(a)) => assert!(a.reason < 0x80, "PUBACK refused: {a:?}"),
                other => panic!("expected PUBACK, got {other:?}"),
            }
        }
        next += n;
    }
}

/// Write a PINGREQ every `every` until `until`; returns how many were written.
async fn ping<W: AsyncWrite + Unpin>(wr: &mut W, every: Duration, until: Instant) -> u32 {
    let mut sent = 0;
    while Instant::now() < until {
        tokio::time::timeout(IO_TIMEOUT, send(wr, &Packet::PingReq))
            .await
            .expect("a PINGREQ write must not block: the broker keeps reading");
        sent += 1;
        // SETTLE(ping-cadence): the pause is the client's ping RATE, not a wait for
        // state — a live client pings on a timer, and the keepalive under test is
        // measured against exactly that cadence.
        tokio::time::sleep(every).await;
    }
    sent
}

/// A peer that keeps pinging but never reads again is closed once it has accepted no
/// bytes for the write-stall bound, and counted as `write-stall`. Its keepalive never
/// expires — the broker reads every PINGREQ while it is blocked writing — so before
/// issue #1117 nothing ever closed it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_that_stops_reading_is_closed_after_the_write_stall_bound_and_counted() {
    let broker = start_broker().await;
    let mut sub = broker.duplex_client();
    let mut buf = BytesMut::new();
    connect_and_subscribe(&mut sub, &mut buf, "stall-sub", 1, "stall/t").await;
    let (mut rd, mut wr) = tokio::io::split(sub);

    // The broker can have bytes it cannot write for this peer from here on, no sooner.
    let started = Instant::now();
    let pinger = tokio::spawn(async move {
        ping(
            &mut wr,
            Duration::from_millis(250),
            started + Duration::from_secs(12),
        )
        .await;
        wr
    });
    publish_all(broker.addr, "stall/t").await;
    let published = Instant::now();

    let bound = Duration::from_secs(u64::from(STALL_KEEPALIVES));
    let closed = broker
        .closed_for("write-stall", bound + Duration::from_secs(8))
        .await;
    assert!(
        closed - started >= bound,
        "closed {:?} after the first byte it could not take; the bound is {bound:?}",
        closed - started
    );
    assert!(
        closed - published <= bound + Duration::from_secs(3),
        "closed {:?} after the last publish; the bound is {bound:?}",
        closed - published
    );
    assert_eq!(
        broker.connection_errors("keepalive"),
        0,
        "the peer pinged throughout: its keepalive must not have expired"
    );
    // Closed for real: what it had in its buffer, then the end of the stream.
    let drained = tokio::time::timeout(IO_TIMEOUT, async {
        let mut sink = Vec::new();
        rd.read_to_end(&mut sink).await
    })
    .await
    .expect("the connection must end");
    assert!(drained.is_ok(), "{drained:?}");
    pinger.abort();
}

/// A peer that neither reads nor sends is still closed by its keepalive while the
/// broker is blocked writing to it. Before issue #1117 the keepalive timer was not
/// polled during the blocked write, so this connection was never closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_keepalive_still_closes_a_silent_peer_the_broker_is_blocked_writing_to() {
    let broker = start_broker().await;
    let mut sub = broker.duplex_client();
    let mut buf = BytesMut::new();
    connect_and_subscribe(&mut sub, &mut buf, "silent-sub", 1, "silent/t").await;
    let last_heard = Instant::now();
    publish_all(broker.addr, "silent/t").await;

    let closed = broker.closed_for("keepalive", IO_TIMEOUT).await;
    let bound = Duration::from_secs(u64::from(STALL_KEEPALIVES));
    assert!(
        closed - last_heard < bound,
        "closed {:?} after the peer last sent anything: the 1.5 s keepalive grace, not \
         the {bound:?} write-stall bound, ends a silent peer",
        closed - last_heard
    );
    assert_eq!(broker.connection_errors("write-stall"), 0);
    drop(sub);
}

/// A peer that keeps pinging while it does not read is kept open past its keepalive
/// grace, below the write-stall bound, and gets every message and every PINGRESP once
/// it reads again: the broker read and answered the PINGREQs while it was blocked
/// writing to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pings_are_read_and_answered_while_the_broker_is_blocked_writing() {
    let broker = start_broker().await;
    let mut sub = broker.duplex_client();
    let mut buf = BytesMut::new();
    // Keepalive 2: a 3 s grace and an 8 s write-stall bound.
    connect_and_subscribe(&mut sub, &mut buf, "ping-sub", 2, "ping/t").await;
    let (mut rd, mut wr) = tokio::io::split(sub);
    publish_all(broker.addr, "ping/t").await;

    // Past the 3 s keepalive grace without reading a byte.
    let pinged = ping(
        &mut wr,
        Duration::from_millis(500),
        Instant::now() + Duration::from_secs(4),
    )
    .await;

    let (mut got, mut answered) = (0u32, 0u32);
    while got < MESSAGES || answered < pinged {
        match recv(&mut rd, &mut buf).await {
            Some(Packet::Publish(p)) => {
                assert_eq!(index_of(&p), got, "deliveries in publish order");
                got += 1;
            }
            Some(Packet::PingResp) => answered += 1,
            other => panic!(
                "expected a delivery or a PINGRESP, got {other:?} ({got} delivered, \
                 {answered} of {pinged} PINGRESPs)"
            ),
        }
    }
    // Still open, and still answering.
    send(&mut wr, &Packet::PingReq).await;
    assert_eq!(recv(&mut rd, &mut buf).await, Some(Packet::PingResp));
    assert_eq!(broker.connection_errors("keepalive"), 0);
    assert_eq!(broker.connection_errors("write-stall"), 0);
}

/// Order is unchanged for a client that reads normally and publishes to its own
/// subscription: its deliveries arrive in publish order, and the broker's PUBACKs for
/// its publishes in publish order. Both now go through the queued write, interleaved
/// on one buffer, so this pins that queueing kept the order the awaited writes had.
///
/// Not asserted: that a publish's own PUBACK precedes its delivery back to the same
/// client. It does not hold without this change either (the delivery can be queued
/// before the hub resolves the ack), so it is not this change's to pin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reading_client_sees_acks_and_deliveries_in_order() {
    const N: u32 = 3_000;
    let broker = start_broker().await;
    let mut c = TcpStream::connect(broker.addr).await.unwrap();
    let mut buf = BytesMut::new();
    connect_and_subscribe(&mut c, &mut buf, "order-sub", 60, "order/t").await;

    let mut delivered = Vec::new();
    let mut next = 0;
    while next < N {
        let n = 100.min(N - next);
        let mut out = Vec::new();
        for i in next..next + n {
            out.extend(encode(&publish("order/t", i)));
        }
        c.write_all(&out).await.unwrap();
        // This window's acks and deliveries, acking each delivery as it comes.
        let (mut acks, mut deliveries) = (0, 0);
        while acks < n || deliveries < n {
            match recv(&mut c, &mut buf).await {
                Some(Packet::PubAck(a)) => {
                    let index = next + acks;
                    assert_eq!(
                        a.pkid,
                        u16::try_from(index % 60_000 + 1).unwrap(),
                        "PUBACKs in publish order"
                    );
                    acks += 1;
                }
                Some(Packet::Publish(p)) => {
                    delivered.push(index_of(&p));
                    send(&mut c, &Packet::PubAck(Ack::new(p.pkid.unwrap()))).await;
                    deliveries += 1;
                }
                other => panic!("expected a PUBACK or a delivery, got {other:?}"),
            }
        }
        next += n;
    }

    assert_eq!(
        delivered,
        (0..N).collect::<Vec<_>>(),
        "deliveries in publish order"
    );
}
