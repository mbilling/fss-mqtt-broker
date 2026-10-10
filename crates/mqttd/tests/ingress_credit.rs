//! Ingress credit against the real hub (ADR 0082 T3): while client publishes are
//! credit-blocked, `/livez`, the hub's control lane and a fresh connection all still
//! answer promptly, and a paused `QoS` 1 publisher resumes.
//!
//! The hub is made genuinely slow — every publish fans out to [`SUBSCRIBERS`] sessions —
//! and [`PUBLISHERS`] clients write `QoS` 0 as fast as TCP takes it, so the hub falls
//! behind and the credit pool, not the hub, decides how much waits for it.
//!
//! A client that hangs up while paused is reaped promptly though the pool stays full
//! (#825).

mod common;

use common::Client;
use mqtt_codec::{packet::Ack, Packet, QoS};
use mqtt_observability::metrics::Metrics;
use mqttd::health::HealthState;
use mqttd::hub::{Hub, HubCommand};
use mqttd::ingress::{IngressCredit, OverloadMode};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SUBSCRIBERS: usize = 200;
const PUBLISHERS: usize = 8;
const POOL: usize = 64 * 1024;

/// One `/livez` round trip: its status line and how long it took.
async fn livez(health: SocketAddr) -> (String, Duration) {
    let start = Instant::now();
    let mut s = TcpStream::connect(health).await.unwrap();
    s.write_all(b"GET /livez HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut body = Vec::new();
    s.read_to_end(&mut body).await.unwrap();
    let status = String::from_utf8_lossy(&body)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    (status, start.elapsed())
}

fn paused(metrics: &Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .find_map(|l| {
            l.strip_prefix("mqttd_ingress_paused_total ")?
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0)
}

/// A real hub and client listener with `credit` installed, and `/livez` served beside
/// them. Returns the MQTT address, the health address and the hub's sender.
async fn start(
    credit: &Arc<IngressCredit>,
    metrics: &Arc<Metrics>,
) -> (
    SocketAddr,
    SocketAddr,
    tokio::sync::mpsc::UnboundedSender<HubCommand>,
) {
    let (mut hub, hub_tx) = Hub::new();
    hub.attach_ingress(credit.clone());
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());

    let base = common::permissive_policy(mqttd::conn::DEFAULT_CONNECT_TIMEOUT);
    let policy = Arc::new(mqttd::conn::ConnPolicy {
        ingress: Some(credit.clone()),
        rules: None,
        metrics: Some(metrics.clone()),
        ..(*base).clone()
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let hub_tx = hub_tx.clone();
        tokio::spawn(async move {
            loop {
                let (stream, peer) = listener.accept().await.unwrap();
                // As the production listeners do: watch the raw socket (#825).
                let watch = mqttd::conn::PeerClosedWatch::new(&stream);
                tokio::spawn(mqttd::conn::handle_stream_watched(
                    stream,
                    Some(peer),
                    mqttd::conn::Arrival::default(),
                    None,
                    policy.clone(),
                    hub_tx.clone(),
                    watch,
                ));
            }
        });
    }
    let health_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health = health_listener.local_addr().unwrap();
    tokio::spawn(mqttd::health::serve(
        health_listener,
        HealthState::new(hub_tx.clone(), None, None, 1),
    ));
    (addr, health, hub_tx)
}

/// The load: [`SUBSCRIBERS`] sessions on `load/#` make each publish expensive for the
/// hub, and [`PUBLISHERS`] clients write `QoS` 0 to it as fast as TCP takes it.
async fn overload(addr: SocketAddr, payload: &[u8]) {
    for i in 0..SUBSCRIBERS {
        let mut sub = Client::connect(addr, &format!("sub-{i}")).await;
        sub.subscribe(1, "load/#", QoS::AtMostOnce).await;
        tokio::spawn(async move {
            while !matches!(
                sub.recv_bounded(Duration::from_secs(30)).await,
                common::Recv::Closed
            ) {}
        });
    }
    for i in 0..PUBLISHERS {
        let mut publisher = Client::connect(addr, &format!("pub-{i}")).await;
        let payload = payload.to_vec();
        tokio::spawn(async move {
            loop {
                publisher
                    .publish("load/t", &payload, QoS::AtMostOnce, None, vec![])
                    .await;
            }
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livez_and_the_control_lane_answer_while_publishes_are_credit_blocked() {
    let credit = Arc::new(IngressCredit::new(POOL, 16 * 1024, OverloadMode::Pause));
    let metrics = Arc::new(Metrics::new("test"));
    let (addr, health, hub_tx) = start(&credit, &metrics).await;
    let payload = vec![7u8; 200];
    overload(addr, &payload).await;

    // Wait for overload: publishers pausing on a full pool.
    let deadline = Instant::now() + Duration::from_secs(10);
    while paused(&metrics) < PUBLISHERS as u64 {
        assert!(
            Instant::now() < deadline,
            "the hub never fell behind: {} pauses, {} of {POOL} bytes in use",
            paused(&metrics),
            credit.in_use()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut worst_livez = Duration::ZERO;
    let mut worst_ping = Duration::ZERO;
    for _ in 0..20 {
        let (status, took) = livez(health).await;
        assert!(status.contains("200"), "/livez under overload: {status}");
        worst_livez = worst_livez.max(took);
        // The control lane directly: what /livez rides on.
        let (tx, rx) = tokio::sync::oneshot::channel();
        let start = Instant::now();
        hub_tx.send(HubCommand::Ping { reply: tx }).unwrap();
        rx.await.unwrap();
        worst_ping = worst_ping.max(start.elapsed());
    }

    // A paused QoS 1 publisher resumes: its PUBACK arrives through the same pressure,
    // and a fresh connection (data lane) gets its CONNACK behind at most a pool's worth.
    let start = Instant::now();
    let mut late = Client::connect(addr, "late").await;
    let connect_took = start.elapsed();
    let start = Instant::now();
    for id in 1..=20u16 {
        late.publish("load/t", &payload, QoS::AtLeastOnce, Some(id), vec![])
            .await;
    }
    let mut acked = 0;
    while acked < 20 {
        match late.recv().await {
            Packet::PubAck(Ack { .. }) => acked += 1,
            Packet::Publish(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    let qos1_took = start.elapsed();
    let in_use = credit.in_use();
    eprintln!(
        "under overload: /livez worst {worst_livez:?}, ping worst {worst_ping:?}, \
         CONNECT {connect_took:?}, 20 QoS 1 acked in {qos1_took:?}, \
         {} pauses, {in_use} of {POOL} credit bytes in use",
        paused(&metrics)
    );
    assert!(
        worst_livez < Duration::from_millis(500),
        "/livez slowed to {worst_livez:?} (LIVE_TIMEOUT is 2 s)"
    );
    assert!(
        worst_ping < Duration::from_millis(250),
        "ping took {worst_ping:?}"
    );
    assert!(
        connect_took < Duration::from_secs(2),
        "CONNECT took {connect_took:?}"
    );
    assert!(in_use <= POOL);
}

/// #825: a client that hangs up while paused for credit, with publishes still unread in
/// its socket, is reaped promptly though the pool stays full: its Will fires, nothing
/// it sent is acknowledged, and no credit leaks. Before the fix the paused read loop
/// never saw the FIN — keepalive is disarmed while paused — so the connection lived
/// until credit freed, which here is never.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_hangs_up_while_paused_is_reaped_though_the_pool_stays_full() {
    const SMALL_POOL: usize = 4096;
    let credit = Arc::new(IngressCredit::new(
        SMALL_POOL,
        SMALL_POOL,
        OverloadMode::Pause,
    ));
    let metrics = Arc::new(Metrics::new("test"));
    let (addr, _health, _hub_tx) = start(&credit, &metrics).await;

    let mut watcher = Client::connect(addr, "watcher").await;
    watcher.subscribe(1, "will/#", QoS::AtLeastOnce).await;

    // Something else holds the whole pool for the rest of the test.
    let hog = credit
        .connection()
        .try_acquire(u32::try_from(SMALL_POOL).unwrap())
        .expect("the pool starts empty");

    let mut publisher = Client::open(addr, mqtt_codec::ProtocolVersion::V311).await;
    publisher
        .connect_with_will("paused", "will/paused", b"gone")
        .await;
    // The first parks on the empty pool; the rest stay unread in the socket.
    for id in 1..=8u16 {
        publisher
            .publish("data/t", &[1u8; 256], QoS::AtLeastOnce, Some(id), vec![])
            .await;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while paused(&metrics) < 1 {
        assert!(Instant::now() < deadline, "the publisher never paused");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Hang up without DISCONNECT: a FIN behind the unread publishes.
    let hung_up = Instant::now();
    drop(publisher);

    let will = match watcher.recv_bounded(Duration::from_secs(5)).await {
        common::Recv::Packet(Packet::Publish(p)) => p,
        common::Recv::Packet(other) => panic!("unexpected {other:?}"),
        common::Recv::Quiet => panic!(
            "the paused connection was not reaped {:?} after its client hung up",
            hung_up.elapsed()
        ),
        common::Recv::Closed => panic!("the watcher was closed"),
    };
    assert_eq!(will.topic, "will/paused");
    assert_eq!(will.payload.as_ref(), b"gone");
    eprintln!(
        "reaped (Will delivered) {:?} after the hang-up",
        hung_up.elapsed()
    );
    // Reaped while the pool was still full, and the parked publish took none of it.
    assert_eq!(credit.in_use(), SMALL_POOL);
    drop(hog);
    assert_eq!(credit.in_use(), 0, "the reaped connection leaked credit");
}
