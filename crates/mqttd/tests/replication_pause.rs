//! ADR 0080 T3: how long durable writes pause when one replica stalls or dies,
//! at a replication factor of 2 against 3.
//!
//! At R = 2 an append needs BOTH copies (`⌊2 / 2⌋ + 1 = 2`), so a replica that is
//! slow or merely suspected holds its groups' writes until it recovers or SWIM
//! declares it dead; at R = 3 one copy may lag. ADR 0080 states that trade; this
//! measures it on three real `mqttd` processes:
//!
//! - **Suspend:** `SIGSTOP` one non-founder node for [`SUSPEND`], then `SIGCONT`.
//! - **Crash:** `SIGKILL` one non-founder node for good.
//!
//! One fault per fresh cluster, so a fault's aftermath never leaks into another's
//! figures. The figure is, per topic, the longest gap between two successful
//! acks from the fault on.
//!
//! Load: [`TOPICS`] persistent subscribers, offline, so every publish is a durable
//! enqueue on its session's group; one publisher task per topic sends a `QoS` 1
//! message every [`INTERVAL`] and times its own PUBACK, so a stalled group stalls
//! only its own task. Publishers connect to the two nodes that are never faulted.
//!
//! It asserts only what must hold at any R — every topic is acked again after
//! the fault — and prints the pause figures. `#[ignore]`: timed measurements
//! (about a minute and a half each), run on demand:
//!
//!     cargo test -p mqttd --test replication_pause -- --ignored --nocapture --test-threads 1

mod common;
mod proc_common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mqtt_codec::{Packet, QoS};
use proc_common::*;

/// Durable sessions (one per topic), spread over the placement groups.
const TOPICS: usize = 48;
/// Publish cadence per topic.
const INTERVAL: Duration = Duration::from_millis(100);
/// How long an ack may take before the publish counts as timed out.
const ACK_TIMEOUT: Duration = Duration::from_secs(30);
/// Warm-up before the fault, excluded from the figures (sessions settle).
const WARM: Duration = Duration::from_secs(10);
/// How long a suspended node stays stopped.
const SUSPEND: Duration = Duration::from_secs(8);
/// Observation after the fault is applied.
const OBSERVE: Duration = Duration::from_secs(40);
/// An ack slower than this counts as the group being paused.
const PAUSED: Duration = Duration::from_secs(1);
/// [`PAUSED`] in milliseconds, for comparing against sample times.
const PAUSED_MS: u64 = 1_000;

/// One publish: when it was sent (ms since start) and how long its ack took
/// (`None` = no ack within [`ACK_TIMEOUT`], or the connection dropped).
#[derive(Clone, Copy)]
struct Sample {
    topic: usize,
    sent_ms: u64,
    ack_ms: Option<u64>,
}

async fn persistent_subscriber(addr: std::net::SocketAddr, id: &str, topic: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some((mut c, _)) =
            common::Client::connect_v311_within(addr, id, false, Duration::from_secs(10)).await
        {
            let ack = c.subscribe(1, topic, QoS::AtLeastOnce).await;
            if ack.return_codes.iter().all(|rc| *rc != 0x80) {
                c.disconnect().await;
                return;
            }
        }
        assert!(Instant::now() < deadline, "{id} never subscribed durably");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn publisher(
    addr: std::net::SocketAddr,
    topic_idx: usize,
    started: Instant,
    stop: Arc<AtomicBool>,
    out: Arc<Mutex<Vec<Sample>>>,
) {
    let topic = format!("rp/{topic_idx}");
    let id = format!("rp-pub-{topic_idx}");
    let mut conn: Option<common::Client> = None;
    let mut pkid: u16 = 0;
    while !stop.load(Ordering::Relaxed) {
        let tick = Instant::now();
        if conn.is_none() {
            conn = common::Client::connect_v311_within(addr, &id, true, Duration::from_secs(5))
                .await
                .map(|(c, _)| c);
        }
        let Some(c) = conn.as_mut() else {
            tokio::time::sleep(INTERVAL).await;
            continue;
        };
        pkid = pkid.wrapping_add(1).max(1);
        let sent = Instant::now();
        let sent_ms = u64::try_from(sent.duration_since(started).as_millis()).unwrap_or(u64::MAX);
        c.publish(&topic, b"pause-probe", QoS::AtLeastOnce, Some(pkid), vec![])
            .await;
        let ack_ms = loop {
            let left = ACK_TIMEOUT.saturating_sub(sent.elapsed());
            match c.recv_bounded(left).await {
                common::Recv::Packet(Packet::PubAck(a)) if a.pkid == pkid => {
                    break u64::try_from(sent.elapsed().as_millis()).ok();
                }
                common::Recv::Packet(_) => {}
                common::Recv::Quiet | common::Recv::Closed => {
                    conn = None;
                    break None;
                }
            }
        };
        out.lock().unwrap().push(Sample {
            topic: topic_idx,
            sent_ms,
            ack_ms,
        });
        tokio::time::sleep(INTERVAL.saturating_sub(tick.elapsed())).await;
    }
}

fn signal(pid: u32, sig: &str) {
    let ok = std::process::Command::new("kill")
        .args([sig, &pid.to_string()])
        .status()
        .is_ok_and(|s| s.success());
    assert!(ok, "kill {sig} {pid} failed");
}

/// The fault a run applies to one non-founder node.
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// `SIGSTOP` for [`SUSPEND`], then `SIGCONT`: a node that is slow, or
    /// suspected, and comes back.
    Suspend,
    /// `SIGKILL`: a node that is gone.
    Crash,
}

/// Per topic, the longest gap between two SUCCESSFUL acks (by completion time)
/// from `from_ms` on — how long that topic's durable writes were held, however
/// the publishes in between failed (timed out, or dropped with a relocation).
fn gaps(samples: &[Sample], from_ms: u64) -> Vec<u64> {
    (0..TOPICS)
        .map(|t| {
            let mut done: Vec<u64> = samples
                .iter()
                .filter(|s| s.topic == t)
                .filter_map(|s| s.ack_ms.map(|a| s.sent_ms + a))
                .collect();
            done.sort_unstable();
            // The last ack before the fault anchors the first gap.
            let before = done.iter().copied().filter(|d| *d < from_ms).max();
            let after: Vec<u64> = done.into_iter().filter(|d| *d >= from_ms).collect();
            let mut prev = before.unwrap_or(from_ms);
            let mut worst = 0;
            for d in after {
                worst = worst.max(d - prev);
                prev = d;
            }
            worst
        })
        .collect()
}

async fn measure(replicas: u8, fault: Fault, seed: u64) {
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    for n in &mut nodes {
        n.extra_env
            .push(("MQTTD_REPLICAS".to_string(), replicas.to_string()));
    }
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&mut nodes, seed).await;
    let victim = 2; // a non-founder
    let entries = [nodes[0].client_addr, nodes[1].client_addr];

    for t in 0..TOPICS {
        persistent_subscriber(
            entries[t % 2],
            &format!("rp-sub-{seed}-{t}"),
            &format!("rp/{t}"),
        )
        .await;
    }

    let started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let tasks: Vec<_> = (0..TOPICS)
        .map(|t| {
            tokio::spawn(publisher(
                entries[t % 2],
                t,
                started,
                stop.clone(),
                samples.clone(),
            ))
        })
        .collect();

    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    // SETTLE(pause-warm): a measurement schedule, not a wait for state — steady load runs
    // before the fault; a slow machine only lowers the load, never the verdict.
    tokio::time::sleep(WARM).await;
    let fault_ms = ms(started.elapsed());
    match fault {
        Fault::Suspend => {
            let pid = nodes[victim].pid().expect("victim running");
            signal(pid, "-STOP");
            // SETTLE(pause-suspend): the fault IS a duration — the node stays stopped this long, and
            // the pause it causes is what is measured; nothing to poll for.
            tokio::time::sleep(SUSPEND).await;
            signal(pid, "-CONT");
        }
        Fault::Crash => nodes[victim].kill().await,
    }
    // SETTLE(pause-after-crash): the observation window the figures are read over; the only
    // assertion (every topic acked again) is checked on the samples after it.
    tokio::time::sleep(OBSERVE).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    let samples = samples.lock().unwrap().clone();
    for n in &mut nodes {
        n.kill().await;
    }

    let mut g = gaps(&samples, fault_ms);
    let resumed = (0..TOPICS).all(|t| {
        samples
            .iter()
            .any(|s| s.topic == t && s.ack_ms.is_some() && s.sent_ms > fault_ms)
    });
    g.sort_unstable();
    let paused: Vec<u64> = g.iter().copied().filter(|x| *x >= PAUSED_MS).collect();
    let median = paused.get(paused.len() / 2).copied().unwrap_or(0);
    eprintln!(
        "R={replicas} {fault:?}: {}/{TOPICS} topics paused >= {}s; longest {} ms, median of paused {} ms",
        paused.len(),
        PAUSED.as_secs(),
        g.last().copied().unwrap_or(0),
        median
    );
    assert!(
        resumed,
        "R={replicas} {fault:?}: durable writes to some topic never resumed after one node's fault"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timed measurement (ADR 0080 T3); run with --ignored --nocapture --test-threads 1"]
async fn write_pause_two_replicas_suspend() {
    measure(2, Fault::Suspend, 802).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timed measurement (ADR 0080 T3); run with --ignored --nocapture --test-threads 1"]
async fn write_pause_two_replicas_crash() {
    measure(2, Fault::Crash, 812).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timed measurement (ADR 0080 T3); run with --ignored --nocapture --test-threads 1"]
async fn write_pause_three_replicas_suspend() {
    measure(3, Fault::Suspend, 803).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timed measurement (ADR 0080 T3); run with --ignored --nocapture --test-threads 1"]
async fn write_pause_three_replicas_crash() {
    measure(3, Fault::Crash, 813).await;
}
