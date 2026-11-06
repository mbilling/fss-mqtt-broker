//! The load generator against the production binary ([ADR 0090](../../docs/adr/0090-benchmark-rig-redesign.md)
//! T1, issue #1109).
//!
//! `mqtt-load` judges brokers, so it is itself checked against one: a real `mqttd`
//! process with its data dir on (durable plane on, as deployed), spawned here. Three
//! properties a benchmark verdict leans on:
//!
//! - **The ledger closes on a healthy run** — durable `QoS` 1 through a `$share` group,
//!   and clean-session `QoS` 0 fan-out: every identity sent is acked and delivered,
//!   nothing unexpected, and "sent" is exactly what the schedule offered.
//! - **The ledger sees loss** — the same run with the consumers' broker connections
//!   cut midway reports the identities it never received as missing, not as a pass.
//! - **The latencies are real** — through a proxy that holds every byte for a known
//!   delay each way, ack latency (PUBLISH out, PUBACK back) and end-to-end latency
//!   (PUBLISH out, delivery back, both through the proxy) each grow by two delays.
//!
//! The runs are seconds long and lightly loaded (a few hundred msg/s): what is tested
//! is the generator's accounting, not anybody's throughput. The saturation thresholds
//! are relaxed here because a debug-built generator on a shared runner is not the
//! subject; the generator's own health judgment is unit-tested in the crate.

mod common;
mod proc_common;

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use mqtt_load::{Config, Outcome, Phase, Sessions, Shape, Summary, Thresholds};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Broker {
    _child: tokio::process::Child,
    client: SocketAddr,
    log: std::path::PathBuf,
}

async fn spawn_broker(dir: &Path) -> Broker {
    let client = SocketAddr::from(([127, 0, 0, 1], proc_common::free_tcp_port()));
    let health = SocketAddr::from(([127, 0, 0, 1], proc_common::free_tcp_port()));
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let log = dir.join("mqttd.log");
    let out = std::fs::File::create(&log).unwrap();
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_mqttd"))
        .kill_on_drop(true)
        .env("MQTTD_NODE_ID", "loadgen")
        .env("MQTTD_PLAINTEXT_BIND", client.to_string())
        .env("MQTTD_HEALTH_BIND", health.to_string())
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("MQTTD_DATA_DIR", &data)
        .env("MQTTD_SHUTDOWN_GRACE", "0")
        .env("RUST_LOG", "warn")
        .stdout(std::process::Stdio::from(out.try_clone().unwrap()))
        .stderr(std::process::Stdio::from(out))
        .spawn()
        .expect("spawn mqttd");
    // Bounded poll: ready when /readyz says so.
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        if let Some(body) = proc_common::http_get(health, "/readyz").await {
            if body.contains("\"ready\":true") {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "mqttd never became ready:\n{}",
            proc_common::log_tail(&log)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Broker {
        _child: child,
        client,
        log,
    }
}

fn workload(broker: SocketAddr, rate: f64, publishers: u32) -> Config {
    let mut c = Config::new(vec![broker], rate, publishers);
    c.phases = vec![Phase {
        rate,
        warmup_ns: 500_000_000,
        measure_ns: 2_000_000_000,
    }];
    c.window_ns = 1_000_000_000;
    c.drain_timeout_ns = 20_000_000_000;
    c.drain_quiet_ns = 5_000_000_000;
    c.thresholds = Thresholds {
        cpu_fraction: 1000.0,
        thread_fraction: 1000.0,
        sched_lag_p99_us: 60_000_000,
        squeeze_per_s: f64::INFINITY,
    };
    c
}

fn assert_closed(s: &Summary, broker: &Broker) {
    let l = &s.ledger;
    let ctx = || {
        format!(
            "{}\n--- mqttd log ---\n{}",
            serde_json::to_string_pretty(s).unwrap(),
            proc_common::log_tail(&broker.log)
        )
    };
    assert!(l.closed, "ledger did not close:\n{}", ctx());
    assert_eq!(s.outcome, Outcome::Ok, "{}", ctx());
    assert_eq!((l.missing, l.unexpected, l.unacked), (0, 0, 0), "{}", ctx());
    // Open loop: everything the schedule offered over the run was sent.
    let offered: u64 = s.windows.iter().map(|w| w.agg.offered).sum();
    assert_eq!(l.sent, offered, "{}", ctx());
    assert_eq!(l.unique_received, l.expected, "{}", ctx());
    assert_eq!(s.drain.ended_by, "complete", "{}", ctx());
    assert_eq!(s.drain.outstanding_deliveries, 0, "{}", ctx());
    // A complete drain settles at once: nothing was still owed.
    assert_eq!(s.drain.settle.ended_by, "complete", "{}", ctx());
    assert_eq!(s.drain.settle.received, 0, "{}", ctx());
}

/// Durable `QoS` 1 through `$share` groups: the ledger closes and the latencies are
/// recorded for every message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn qos1_durable_share_run_closes_its_ledger() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let mut c = workload(broker.client, 400.0, 8);
    c.qos = 1;
    c.topics = 2;
    c.shape = Shape::Share;
    c.consumers = 2;
    c.groups = 2;
    c.sessions = Sessions::Durable;
    let s = mqtt_load::run(c).await.expect("run");
    assert_closed(&s, &broker);
    let l = &s.ledger;
    // 400 msg/s for 2.5 s; two groups per topic, so each message is delivered twice.
    assert_eq!(l.sent, 1000);
    assert_eq!(l.acked, 1000);
    assert_eq!(l.expected, 2000);
    assert_eq!(s.workload.deliveries_per_message, 2);
    let acks: u64 = s.windows.iter().map(|w| w.agg.ack_latency.count).sum();
    let e2e: u64 = s.windows.iter().map(|w| w.agg.e2e_latency.count).sum();
    assert_eq!(acks, 1000);
    assert_eq!(e2e, l.received);
    assert!(s.totals.ack_latency.p50_us > 0);
    assert!(s.windows.iter().filter(|w| w.agg.sent > 0).count() >= 3);
    // Schema v2: the measured span's histograms travel as buckets (for merging across
    // generator processes), and the schedule start is stamped on the wall clock.
    let buckets = |h: &Option<mqtt_load::hist::HistogramExport>| -> u64 {
        h.as_ref()
            .expect("totals carry buckets")
            .buckets
            .iter()
            .map(|[_, c]| c)
            .sum()
    };
    assert_eq!(buckets(&s.totals.ack_histogram), s.totals.ack_latency.count);
    assert_eq!(buckets(&s.totals.e2e_histogram), s.totals.e2e_latency.count);
    assert_eq!(
        buckets(&s.phases[0].agg.ack_histogram),
        s.phases[0].agg.ack_latency.count
    );
    assert!(s.windows.iter().all(|w| w.agg.ack_histogram.is_none()));
    assert!(s.schedule_start_unix_ms >= s.started_unix_ms);
    assert_eq!(s.schema, "mqtt-load.summary.v2");
}

/// One bridge connection carrying four topics, each to its own `$share` group of
/// durable consumers: every message arrives on its own topic, once, and every topic
/// carries a share, so no consumer is idle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bridge_connection_carries_every_topic_and_closes_its_ledger() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let mut c = workload(broker.client, 400.0, 1);
    c.qos = 1;
    c.inflight = 64;
    c.topics = 4;
    c.bridge = true;
    c.shape = Shape::Share;
    c.consumers = 2;
    c.groups = 1;
    c.sessions = Sessions::Durable;
    let s = mqtt_load::run(c).await.expect("run");
    assert_closed(&s, &broker);
    let l = &s.ledger;
    // 400 msg/s for 2.5 s on the one connection, one group per topic.
    assert_eq!(l.sent, 1000);
    assert_eq!(l.acked, 1000);
    assert_eq!(l.expected, 1000);
    assert_eq!(s.connections.publishers, 1, "one connection");
    let spread = s.spread.expect("deliveries were made");
    assert_eq!(spread.idle_consumers, 0, "every topic carried messages");
}

/// The power-plant shape: a bridge connection per site, each carrying a block of
/// topics of its own, payloads of varying size, and one durable `$share` group over
/// every topic. Every message arrives once, whichever topic it was on, and every member
/// takes a share.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn site_bridges_into_one_group_over_every_topic_close_their_ledger() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let mut c = workload(broker.client, 600.0, 3);
    c.qos = 1;
    c.inflight = 64;
    c.bridge = true;
    c.topics_per_connection = 5;
    c.topics = 15;
    c.payload = 50;
    c.payload_max = 200;
    c.shape = Shape::ShareAll;
    c.consumers = 3;
    c.groups = 1;
    c.sessions = Sessions::Durable;
    let s = mqtt_load::run(c).await.expect("run");
    assert_closed(&s, &broker);
    let l = &s.ledger;
    // 600 msg/s for 2.5 s over three connections; one group, so each message once.
    assert_eq!(l.sent, 1500);
    assert_eq!(l.acked, 1500);
    assert_eq!(l.expected, 1500);
    assert_eq!(s.workload.deliveries_per_message, 1);
    assert_eq!(
        (s.workload.topics, s.workload.topics_per_connection),
        (15, 5)
    );
    assert_eq!(
        (s.workload.payload_bytes, s.workload.payload_max_bytes),
        (50, 200)
    );
    assert_eq!(s.connections.publishers, 3, "one connection a site");
    let spread = s.spread.expect("deliveries were made");
    assert_eq!(spread.idle_consumers, 0, "every member took a share");
}

/// Clean-session `QoS` 0 fan-out: no acks to count, every subscriber gets every message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn qos0_clean_fanout_run_closes_its_ledger() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let mut c = workload(broker.client, 200.0, 4);
    c.qos = 0;
    c.topics = 2;
    c.shape = Shape::Fanout;
    c.consumers = 3;
    c.sessions = Sessions::Clean;
    let s = mqtt_load::run(c).await.expect("run");
    assert_closed(&s, &broker);
    assert_eq!(s.ledger.sent, 500);
    assert_eq!(s.ledger.acked, 0);
    assert_eq!(s.ledger.expected, 1500);
}

/// Two generator processes' worth of runs share one start through the ready/start
/// files, although one of them starts connecting only once the other is ready: their schedules start
/// within milliseconds of each other and of the time the "rig" named, so their
/// measured windows coincide (the 10-node run of 2026-10-27 lost a step to windows
/// that overlapped 51.6 s of 60).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generators_share_a_start_through_the_start_file() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let gen = |g: usize| {
        let mut c = workload(broker.client, 200.0, 4);
        c.qos = 1;
        c.topics = 2;
        c.shape = Shape::Share;
        c.consumers = 1;
        c.sessions = Sessions::Clean;
        c.topic_prefix = format!("sync/g{g}");
        c.client_prefix = Some(format!("sync-g{g}"));
        c.ready_file = Some(dir.path().join(format!("gen-{g}.ready")));
        c.start_file = Some(dir.path().join(format!("gen-{g}.start")));
        c.start_wait_ns = 20_000_000_000;
        c
    };
    let (c0, c1) = (gen(0), gen(1));
    let ready = [
        c0.ready_file.clone().unwrap(),
        c1.ready_file.clone().unwrap(),
    ];
    let starts = [
        c0.start_file.clone().unwrap(),
        c1.start_file.clone().unwrap(),
    ];
    // Generator 1 starts only once generator 0 is connected and waiting: the slow one.
    let g0 = tokio::spawn(mqtt_load::run(c0));
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready[0].is_file() {
        assert!(Instant::now() < deadline, "generator 0 never got ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let g1 = tokio::spawn(mqtt_load::run(c1));
    // The rig: every generator ready (bounded poll), then one start 500 ms ahead.
    while !ready.iter().all(|p| p.is_file()) {
        assert!(Instant::now() < deadline, "the generators never got ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let at = mqtt_load::start::unix_ms_now() + 500;
    for p in &starts {
        mqtt_load::start::write_atomic(p, &format!("{at}\n")).unwrap();
    }
    let s0 = g0.await.unwrap().expect("run 0");
    let s1 = g1.await.unwrap().expect("run 1");
    for s in [&s0, &s1] {
        assert_closed(s, &broker);
        assert_eq!(s.start.by, "shared", "{:?}", s.start);
        assert_eq!(s.start.requested_unix_ms, Some(at));
        // The process clock maps onto the wall clock within a few ms.
        assert!(
            s.schedule_start_unix_ms.abs_diff(at) <= 25,
            "{} vs {at}",
            s.schedule_start_unix_ms
        );
    }
    // Generator 0 waited for generator 1's connections and the lead, generator 1 only
    // for the lead.
    assert!(
        s0.start.waited_s > s1.start.waited_s,
        "{:?} {:?}",
        s0.start,
        s1.start
    );
    assert!(s0.started_unix_ms <= s1.started_unix_ms);
}

/// A TCP proxy that holds every chunk for `delay` in each direction, in order, and —
/// with `cut_after` — closes a connection once it has forwarded that many bytes from
/// the broker to the client.
async fn proxy(target: SocketAddr, delay: Duration, cut_after: Option<usize>) -> SocketAddr {
    async fn one_way(
        mut from: tokio::net::tcp::OwnedReadHalf,
        mut to: tokio::net::tcp::OwnedWriteHalf,
        delay: Duration,
        cut_after: Option<usize>,
    ) {
        let (tx, mut rx) =
            tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
        let writer = tokio::spawn(async move {
            let mut forwarded = 0usize;
            while let Some((due, bytes)) = rx.recv().await {
                // SETTLE(loadgen-delay-proxy): the delay IS the injected fault. The proxy
                // models a fixed network latency, so each chunk must genuinely be held
                // until its due time; the test then checks that the generator's measured
                // ack and end-to-end latency reflect exactly this hold.
                tokio::time::sleep_until(due).await;
                if to.write_all(&bytes).await.is_err() {
                    return;
                }
                forwarded += bytes.len();
                if cut_after.is_some_and(|c| forwarded >= c) {
                    return;
                }
            }
        });
        let mut buf = vec![0u8; 64 * 1024];
        let reader = async {
            loop {
                match from.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tx
                            .send((tokio::time::Instant::now() + delay, buf[..n].to_vec()))
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
        };
        // Either side finishing ends this direction: EOF, or the cut.
        tokio::select! {
            () = reader => {}
            _ = writer => {}
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(target).await else {
                    return;
                };
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (ir, iw) = inbound.into_split();
                let (or, ow) = outbound.into_split();
                // Either direction ending closes the whole relayed connection.
                tokio::select! {
                    () = one_way(ir, ow, delay, None) => {}
                    () = one_way(or, iw, delay, cut_after) => {}
                }
            });
        }
    });
    addr
}

/// A proxy that plays a cluster closing connections: it relays both ways, and on each of
/// its first `times` connections, after `after` packets of MQTT type `kind` from the
/// broker to the client, it sends the client `goodbye` (if any) and closes both sides;
/// later connections it relays untouched. It frames the broker's stream into whole
/// packets, so `goodbye` never lands inside one. Two closes it plays: a session the
/// cluster moves (ADR 0005; after PUBLISH packets, kind 3, a DISCONNECT 0x9C: Use
/// another server), and a publisher whose ack the broker withholds (after PUBACKs, kind
/// 4, nothing: fail closed, ADR 0041 T5). (A proxy that closed every connection could
/// close each before the consumer's PUBACKs reached the broker, and the resumed session
/// would redeliver the same messages forever: a cluster moves a session once per
/// ownership change.)
async fn closing_proxy(
    target: SocketAddr,
    kind: u8,
    after: usize,
    times: usize,
    goodbye: Option<Vec<u8>>,
) -> SocketAddr {
    /// The length of the whole packet at the front of `buf`, once all of it is there.
    fn frame_len(buf: &[u8]) -> Option<usize> {
        let (mut len, mut mult) = (0usize, 1usize);
        for i in 1..5 {
            let b = *buf.get(i)?;
            len += usize::from(b & 0x7f) * mult;
            if b & 0x80 == 0 {
                return Some(1 + i + len).filter(|&n| buf.len() >= n);
            }
            mult *= 128;
        }
        None
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let rehomed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                return;
            };
            let goodbye = goodbye.clone();
            let rehomed = rehomed.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(target).await else {
                    return;
                };
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (mut ir, mut iw) = inbound.into_split();
                let (mut or, mut ow) = outbound.into_split();
                let up = async {
                    let _ = tokio::io::copy(&mut ir, &mut ow).await;
                };
                let down = async {
                    let mut buf = bytes::BytesMut::with_capacity(64 * 1024);
                    let mut seen = 0usize;
                    loop {
                        while let Some(n) = frame_len(&buf) {
                            let frame = buf.split_to(n);
                            if iw.write_all(&frame).await.is_err() {
                                return;
                            }
                            if frame[0] >> 4 == kind {
                                seen += 1;
                                if seen == after
                                    && rehomed.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                                        < times
                                {
                                    if let Some(g) = &goodbye {
                                        let _ = iw.write_all(g).await;
                                    }
                                    return;
                                }
                            }
                        }
                        match or.read_buf(&mut buf).await {
                            Ok(n) if n > 0 => {}
                            _ => return,
                        }
                    }
                };
                // Either direction ending closes the whole relayed connection.
                tokio::select! {
                    () = up => {}
                    () = down => {}
                }
            });
        }
    });
    addr
}

/// With `--reconnect`, a consumer whose broker closes its connection comes back. Here
/// every connection runs through a proxy that behaves like a cluster moving the session
/// five times: after 20 deliveries it says "use another server" (DISCONNECT 0x9C) and
/// closes. Each time the consumer connects again with its client id, resumes its
/// durable session — the broker kept what it had not acknowledged — and in the end the
/// ledger closes: nothing lost, the run not an error, and every close counted by the
/// reason the broker gave, in the windows where it happened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumer_told_to_use_another_server_resumes_its_session_and_closes_the_ledger() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let mut goodbye = Vec::new();
    mqtt_codec::Packet::Disconnect(mqtt_codec::Disconnect {
        reason: 0x9c, // reason-code: imitated by the proxy, the broker's is not provoked here
        properties: mqtt_codec::Properties::new(),
    })
    .encode(&mut goodbye, mqtt_codec::ProtocolVersion::V5)
    .unwrap();
    let rehome = closing_proxy(broker.client, 3, 20, 5, Some(goodbye)).await;
    // Every connection goes through the proxy; only the consumer is ever closed, as
    // the publishers receive no PUBLISH packets.
    let mut c = workload(rehome, 200.0, 2);
    c.qos = 1;
    c.topics = 1;
    c.shape = Shape::Share;
    c.consumers = 1;
    c.groups = 1;
    c.sessions = Sessions::Durable;
    c.session_expiry = 30;
    c.reconnect_within_ns = Some(10_000_000_000);
    let s = mqtt_load::run(c).await.expect("run");
    assert_closed(&s, &broker);
    let r = &s.reconnect;
    let report = || serde_json::to_string_pretty(&s).unwrap();
    assert!(r.enabled, "{}", report());
    assert_eq!(r.reconnects, 5, "one per move: {}", report());
    // A close the consumer met while writing its PUBACKs reads what the broker said
    // first; the socket's reset can still discard it, and that close is `write`.
    assert!(
        r.by_reason.get("0x9c").is_some_and(|&n| n >= 1)
            && r.by_reason.keys().all(|k| k == "0x9c" || k == "write")
            && r.by_reason.values().sum::<u64>() == r.reconnects,
        "every close named by its reason: {}",
        report()
    );
    assert_eq!((r.sessions_lost, r.failed), (0, 0), "{}", report());
    assert!(
        r.gap.count == r.reconnects && r.gap.max_us > 0,
        "{}",
        report()
    );
    assert_eq!(
        (s.errors.disconnects, s.errors.server_disconnects),
        (0, 0),
        "a consumer that came back is not a lost connection: {}",
        report()
    );
    let in_windows: u64 = s.windows.iter().map(|w| w.agg.reconnects).sum();
    assert!(
        in_windows > 0 && in_windows <= r.reconnects,
        "the windows place the reconnects in time: {}",
        report()
    );
}

/// With `--reconnect`, a publisher whose broker closes its connection comes back and
/// sends again what it had in flight. Here every connection runs through a proxy that
/// closes a connection after 20 PUBACKs, without a word, three times — what a broker
/// does to a publisher whose ack it withholds (fail closed, ADR 0041 T5), as a cluster
/// moving the session a message is for does. Each time the publisher connects again
/// (a clean session) and publishes its unacknowledged messages again: the ledger closes,
/// with no message unacknowledged and every one delivered, duplicates allowed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publisher_closed_with_its_acks_withheld_reconnects_and_sends_again() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    // Only publishers receive PUBACKs, so only they are ever closed.
    let closing = closing_proxy(broker.client, 4, 20, 3, None).await;
    let mut c = workload(closing, 200.0, 2);
    c.qos = 1;
    c.topics = 1;
    c.shape = Shape::Share;
    c.consumers = 1;
    c.groups = 1;
    c.sessions = Sessions::Durable;
    c.session_expiry = 30;
    c.reconnect_within_ns = Some(10_000_000_000);
    let s = mqtt_load::run(c).await.expect("run");
    assert_closed(&s, &broker);
    let p = &s.reconnect.publishers;
    let report = || serde_json::to_string_pretty(&s).unwrap();
    assert_eq!(p.reconnects, 3, "one per close: {}", report());
    assert_eq!(p.failed, 0, "{}", report());
    assert_eq!(
        p.by_reason.values().sum::<u64>(),
        3,
        "every close counted: {}",
        report()
    );
    assert!(
        p.by_reason.keys().all(|k| k == "closed" || k == "write"),
        "no DISCONNECT was sent: {}",
        report()
    );
    assert_eq!(p.gap.count, 3, "{}", report());
    assert_eq!(
        s.reconnect.reconnects,
        0,
        "no consumer was closed: {}",
        report()
    );
    assert_eq!(
        (s.errors.disconnects, s.errors.write),
        (0, 0),
        "a publisher that came back is not a lost connection: {}",
        report()
    );
    let in_windows: u64 = s.windows.iter().map(|w| w.agg.publisher_reconnects).sum();
    assert!(
        in_windows > 0 && in_windows <= p.reconnects,
        "the windows place the reconnects in time: {}",
        report()
    );
}

/// The ledger sees loss: the only consumer's connection is cut early in the run (it
/// reaches the broker through a proxy that closes it after 4 KiB of deliveries), and
/// the identities it never received are reported missing — the run does not pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_consumer_opens_the_ledger() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let cut = proxy(broker.client, Duration::ZERO, Some(4096)).await;
    // Connections alternate over [cut, direct]: the one consumer goes through the
    // cutting proxy, and so do publishers 0 and 2 (their PUBACK streams stay far below
    // 4 KiB, so they are not cut).
    let mut c = workload(cut, 200.0, 4);
    c.brokers = vec![cut, broker.client];
    c.topics = 1;
    c.consumers = 1;
    c.drain_quiet_ns = 2_000_000_000;
    let s = mqtt_load::run(c).await.expect("run");
    let l = &s.ledger;
    let report = || serde_json::to_string_pretty(&s).unwrap();
    assert!(
        !l.closed,
        "a run that lost its consumer closed its ledger:\n{}",
        report()
    );
    assert!(l.missing > 0, "{}", report());
    assert_eq!(l.missing, l.expected - l.unique_received, "{}", report());
    assert_eq!(l.unacked, 0, "the publishers were not cut: {}", report());
    // The drain says what it gave up on: every missing delivery was still owed when it
    // went quiet, and nothing was left unacknowledged.
    assert_ne!(s.drain.ended_by, "complete", "{}", report());
    assert_eq!(s.drain.outstanding_deliveries, l.missing, "{}", report());
    assert_eq!(s.drain.outstanding_acks, 0, "{}", report());
    assert!(s.drain.quiet_s >= 2.0, "{}", report());
    // The cut consumer's task had ended, so the settle did not wait another quiet
    // window for deliveries nobody could read.
    assert_eq!(s.drain.settle.ended_by, "closed", "{}", report());
    assert!(s.errors.disconnects >= 1, "{}", report());
    assert_eq!(s.outcome, Outcome::Error, "{}", report());
}

/// A drain cut short does not cut the consumers off what the broker still holds for
/// them. The publishers close first and the consumers read on (the settle) before the
/// ledger is reconciled: here member 1 of the `$share` group reaches the broker through
/// a proxy that holds every chunk for 500 ms, the drain gets no time at all, and the
/// deliveries still in the proxy at its end arrive in the settle. When the ledger was
/// reconciled at the drain's end, and the consumers closed with the publishers, those
/// deliveries were missing — acknowledged ones counted as loss — and a durable member's
/// closing (its session ended with expiry 0) discarded what the broker still queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_drain_settles_what_the_broker_holds_before_the_ledger_is_reconciled() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let delayed = proxy(broker.client, Duration::from_millis(500), None).await;
    // The one publisher and member 0 are direct (broker 0); member j of stream 0 is on
    // broker j, so member 1 is behind the proxy.
    let mut c = workload(broker.client, 200.0, 1);
    c.brokers = vec![broker.client, delayed];
    c.qos = 1;
    c.topics = 1;
    c.shape = Shape::Share;
    c.consumers = 2;
    c.groups = 1;
    c.sessions = Sessions::Durable;
    c.drain_timeout_ns = 0;
    // The two endpoints are one broker, so it deals between the members evenly; the
    // generator's skew guard assumes a broker per endpoint and would refuse the shape.
    c.max_skew = 0.0;
    let s = mqtt_load::run(c).await.expect("run");
    let l = &s.ledger;
    let report = || {
        format!(
            "{}\n--- mqttd log ---\n{}",
            serde_json::to_string_pretty(&s).unwrap(),
            proc_common::log_tail(&broker.log)
        )
    };
    assert_eq!(s.drain.ended_by, "timeout", "{}", report());
    assert!(
        s.drain.outstanding_deliveries > 0,
        "the drain ended with deliveries still in the proxy: {}",
        report()
    );
    assert_eq!(s.drain.settle.ended_by, "complete", "{}", report());
    assert_eq!(
        s.drain.settle.received,
        s.drain.outstanding_deliveries,
        "every delivery the drain left owed arrived in the settle: {}",
        report()
    );
    assert_eq!(
        (l.missing, l.missing_acked, l.unexpected),
        (0, 0, 0),
        "{}",
        report()
    );
    assert_eq!(l.unique_received, l.expected, "{}", report());
    assert_eq!(s.errors.disconnects, 0, "{}", report());
}

/// Publishers keep their connections through a drain longer than the broker's
/// keep-alive allowance. The consumer is cut early, so the drain waits its whole quiet
/// window (6 s) with nothing left to publish; at a 2 s keep-alive the broker closes a
/// connection silent for 3 s. Before the publishers pinged through the drain, every one
/// of them was closed so (the 10-node run of 2026-10-27 lost 5,200 at once); now the
/// cut consumer is the only connection lost, and every publish is acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publishers_stay_connected_through_a_drain_longer_than_the_keep_alive() {
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let cut = proxy(broker.client, Duration::ZERO, Some(4096)).await;
    // The consumer (connection 0) goes through the cutting proxy; the publishers
    // alternate over [cut, direct], their PUBACK streams far below the cut.
    let mut c = workload(cut, 200.0, 4);
    c.brokers = vec![cut, broker.client];
    c.topics = 1;
    c.consumers = 1;
    c.keepalive = 2;
    c.drain_quiet_ns = 6_000_000_000;
    let s = mqtt_load::run(c).await.expect("run");
    let report = || {
        format!(
            "{}\n--- mqttd log ---\n{}",
            serde_json::to_string_pretty(&s).unwrap(),
            proc_common::log_tail(&broker.log)
        )
    };
    assert_eq!(s.drain.ended_by, "quiet", "{}", report());
    assert!(s.drain.quiet_s >= 6.0, "{}", report());
    assert_eq!(s.ledger.unacked, 0, "{}", report());
    assert_eq!(
        s.errors.disconnects,
        1,
        "only the cut consumer may be lost: {}",
        report()
    );
}

/// Ack and end-to-end latency through a proxy adding a known delay each way: both
/// grow by the round trip the proxy adds, and a direct run shows the baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn measured_latency_reflects_an_injected_delay() {
    const DELAY_MS: u64 = 25;
    let dir = common::TempDir::new();
    let broker = spawn_broker(dir.path()).await;
    let proxy = proxy(broker.client, Duration::from_millis(DELAY_MS), None).await;
    let shaped = |addr| {
        let mut c = workload(addr, 200.0, 4);
        c.topics = 1;
        c.consumers = 1;
        c
    };
    let direct = mqtt_load::run(shaped(broker.client))
        .await
        .expect("direct run");
    assert_closed(&direct, &broker);
    let delayed = mqtt_load::run(shaped(proxy)).await.expect("delayed run");
    assert_closed(&delayed, &broker);
    let round_trip_us = 2 * DELAY_MS * 1000;
    let (base_ack, base_e2e) = (
        direct.totals.ack_latency.p50_us,
        direct.totals.e2e_latency.p50_us,
    );
    let (ack, e2e) = (
        delayed.totals.ack_latency.p50_us,
        delayed.totals.e2e_latency.p50_us,
    );
    eprintln!(
        "p50 ack/e2e: direct {base_ack}/{base_e2e} us, through the {DELAY_MS} ms proxy {ack}/{e2e} us"
    );
    // PUBLISH crosses the proxy once to the broker, its PUBACK (or the delivery, to a
    // consumer also behind the proxy) once back: one added round trip for both. The
    // floor is exact (the proxy cannot deliver early). The ceiling is the direct run's
    // own p99 plus the round trip plus 30 ms for the runner's scheduling jitter: the
    // measurement may add noise, never a second round trip.
    let ceiling = |base_p99: u64| base_p99 + round_trip_us + 30_000;
    for (name, base_p99, got) in [
        ("ack", direct.totals.ack_latency.p99_us, ack),
        ("e2e", direct.totals.e2e_latency.p99_us, e2e),
    ] {
        assert!(
            got >= round_trip_us,
            "{name} p50 {got} us is below the injected {round_trip_us} us round trip"
        );
        assert!(
            got <= ceiling(base_p99),
            "{name} p50 {got} us exceeds the direct p99 {base_p99} us + {round_trip_us} us + 30 ms"
        );
    }
    assert!(
        base_ack < round_trip_us && base_e2e < round_trip_us,
        "direct baseline {base_ack}/{base_e2e} us"
    );
}
