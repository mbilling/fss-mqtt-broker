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
    assert!(s.errors.disconnects >= 1, "{}", report());
    assert_eq!(s.outcome, Outcome::Error, "{}", report());
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
