//! The **soak**: hours of sustained mixed load against a real spawned cluster,
//! judged on **drift** ([ADR 0044](../../docs/adr/0044-release-readiness-assurance.md) P4).
//!
//! Where the fault schedules ask "does anything acked get lost?", the soak asks
//! the question CI-minutes suites cannot: "does anything *rot*?" — memory
//! creeping, file descriptors leaking, tail latency sagging as state
//! accumulates. Three spawned production-binary nodes serve continuous `QoS` 1
//! publishes (each ack RTT recorded), retained sets, and subscriber churn for
//! `MQTTD_SOAK_SECS` (default 60 for a smoke pass; the nightly tier runs an
//! hour). Every ~5s each node's `VmRSS` and open-FD count are sampled from
//! `/proc`.
//!
//! **Declared drift watermarks**, enforced at the end against the post-warm-up
//! baseline (the first samples after load began — cold-start allocation is not
//! drift):
//!
//! - RSS: `end ≤ warm × 1.5 + 64MB` per node — steady-state load on bounded
//!   state (queues drain, retained topics are a fixed set, session logs
//!   truncate on ack) must plateau, not climb;
//! - FDs: `end ≤ warm + 32` per node — links and clients fluctuate, leaks
//!   accumulate;
//! - ack p99: `last quarter ≤ max(first quarter × 5, 250ms)` and `< 5s` hard —
//!   the tail must not sag as the run ages (loose bounds on purpose: shared
//!   runners are noisy; the target is rot, not benchmarking — 0044-P6 owns
//!   precise numbers).
//!
//! The acked-facts oracle still applies: every ack collected across the whole
//! soak must have been delivered by the end. `#[ignore]` in the per-PR profile;
//! the nightly tier (0044-P4) runs `--ignored` with `MQTTD_SOAK_SECS=3600`.

mod common;
mod proc_common;

use std::time::{Duration, Instant};

use mqtt_codec::{Packet, QoS};
use proc_common::{build_topology, establish_subscribers, proc_over, wait_all_ready, ProcNode};

/// Post-warm RSS growth bound: `end ≤ warm × 3/2 + RSS_SLACK_BYTES`.
const RSS_SLACK_BYTES: u64 = 64 * 1024 * 1024;
/// Post-warm FD growth bound: `end ≤ warm + FD_SLACK`.
const FD_SLACK: usize = 32;
/// Tail-latency sag bound: `last-quarter p99 ≤ max(first × P99_FACTOR, P99_FLOOR)`.
const P99_FACTOR: f64 = 5.0;
const P99_FLOOR: Duration = Duration::from_millis(250);
/// Absolute wedge detector on the aged tail.
const P99_HARD_CAP: Duration = Duration::from_secs(5);

fn soak_secs() -> u64 {
    std::env::var("MQTTD_SOAK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60)
}

/// One `/proc` sample of a spawned node: resident set + open descriptors.
#[derive(Clone, Copy, Debug)]
struct ProcSample {
    rss_bytes: u64,
    fds: usize,
}

fn sample(node: &ProcNode) -> Option<ProcSample> {
    let pid = node.child.as_ref()?.id()?;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss_kb: u64 = status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?.count();
    Some(ProcSample {
        rss_bytes: rss_kb * 1024,
        fds,
    })
}

fn p99(mut samples: Vec<Duration>) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples.sort_unstable();
    let idx = (samples.len().saturating_sub(1)) * 99 / 100;
    samples[idx]
}

/// The soak (ADR 0044 P4): sustained mixed load, `/proc` drift sampling, and
/// the declared watermarks — plus the standing acked-facts oracle.
// One linear story — bring-up, the load loop, the drift verdicts, the oracle.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "long-running by design; the nightly tier (0044-P4) runs it with MQTTD_SOAK_SECS=3600"]
async fn a_soak_under_sustained_load_shows_no_drift() {
    let seed = 8888;
    let secs = soak_secs();
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    establish_subscribers(&mut proc, 2).await;

    let started = Instant::now();
    let deadline = started + Duration::from_secs(secs);
    // Warm-up boundary: samples taken before 10% of the run (min 5s) are
    // cold-start allocation, not drift.
    let warm_until = started + Duration::from_secs((secs / 10).max(5));

    let mut warm: Vec<Option<ProcSample>> = vec![None; proc.nodes.len()];
    let mut last: Vec<Option<ProcSample>> = vec![None; proc.nodes.len()];
    let mut rtts: Vec<(Duration, Duration)> = Vec::new(); // (elapsed, ack RTT)
    let mut next_sample = started;
    let mut next_churn = started + Duration::from_secs(30);
    let mut publisher: Option<common::Client> = None;
    let mut seq = 0u64;

    while Instant::now() < deadline {
        // Sustained publish load: one QoS 1 publish per pass, RTT recorded.
        seq += 1;
        let s = usize::try_from(seq % 2).unwrap();
        let topic = proc.subs[s].topic.clone();
        let payload = format!("soak-{seed}-{seq}").into_bytes();
        if publisher.is_none() {
            publisher = common::Client::connect_v311_within(
                proc.nodes[usize::try_from(seq % 3).unwrap()].client_addr,
                &format!("soak-pub-{seed}"),
                true,
                Duration::from_secs(8),
            )
            .await
            .map(|(c, _)| c);
        }
        if let Some(p) = publisher.as_mut() {
            let sent = Instant::now();
            p.publish(&topic, &payload, QoS::AtLeastOnce, Some(7), vec![])
                .await;
            let ack_deadline = Instant::now() + Duration::from_secs(10);
            let mut closed = false;
            let acked = loop {
                let left = ack_deadline.saturating_duration_since(Instant::now());
                match p.recv_bounded(left).await {
                    common::Recv::Packet(Packet::PubAck(a)) if a.pkid == 7 => break true,
                    common::Recv::Packet(_) => {}
                    common::Recv::Closed => {
                        closed = true;
                        break false;
                    }
                    common::Recv::Quiet => break false,
                }
            };
            if closed {
                publisher = None;
            }
            if acked {
                rtts.push((started.elapsed(), sent.elapsed()));
                proc.acked.entry(topic.clone()).or_default().push(payload);
            }
        }
        // Every ~8th pass: a retained set (bounded topic set — steady state).
        if seq % 8 == 0 {
            proc.retained_step().await;
        }
        // Drain subscribers so delivery keeps pace with load.
        for i in 0..proc.subs.len() {
            proc.drain_subscriber(i).await;
        }
        // Churn one subscriber every ~30s: resumes ride the aged cluster.
        if Instant::now() >= next_churn {
            proc.churn_step().await;
            next_churn = Instant::now() + Duration::from_secs(30);
        }
        // Drift sampling every ~5s; the first post-warm-up sample is baseline.
        if Instant::now() >= next_sample {
            for (i, node) in proc.nodes.iter().enumerate() {
                if let Some(s) = sample(node) {
                    if warm[i].is_none() && Instant::now() >= warm_until {
                        warm[i] = Some(s);
                    }
                    last[i] = Some(s);
                }
            }
            next_sample = Instant::now() + Duration::from_secs(5);
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // ---- Drift verdicts against the declared watermarks ----
    let mut report = Vec::new();
    for i in 0..proc.nodes.len() {
        let (Some(w), Some(l)) = (warm[i], last[i]) else {
            proc.fail(&format!("node {i}: no drift samples collected"));
        };
        // Integer arithmetic (× 3 / 2 for the 1.5 factor): no float casts.
        let rss_cap = w.rss_bytes.saturating_mul(3) / 2 + RSS_SLACK_BYTES;
        let fd_cap = w.fds + FD_SLACK;
        report.push(format!(
            "{}: RSS {}MB → {}MB (cap {}MB), FDs {} → {} (cap {})",
            proc.nodes[i].id,
            w.rss_bytes / (1024 * 1024),
            l.rss_bytes / (1024 * 1024),
            rss_cap / (1024 * 1024),
            w.fds,
            l.fds,
            fd_cap,
        ));
        if l.rss_bytes > rss_cap {
            proc.fail(&format!(
                "RSS drift on {}: warm {} → end {} exceeds cap {rss_cap}",
                proc.nodes[i].id, w.rss_bytes, l.rss_bytes
            ));
        }
        if l.fds > fd_cap {
            proc.fail(&format!(
                "FD drift on {}: warm {} → end {} exceeds cap {fd_cap}",
                proc.nodes[i].id, w.fds, l.fds
            ));
        }
    }
    let total = started.elapsed();
    let quarter = total / 4;
    let first: Vec<Duration> = rtts
        .iter()
        .filter(|(at, _)| *at < quarter)
        .map(|(_, rtt)| *rtt)
        .collect();
    let last_q: Vec<Duration> = rtts
        .iter()
        .filter(|(at, _)| *at >= total.saturating_sub(quarter))
        .map(|(_, rtt)| *rtt)
        .collect();
    let (p99_first, p99_last) = (p99(first), p99(last_q));
    report.push(format!(
        "ack p99: first quarter {p99_first:?} → last quarter {p99_last:?} ({} acked RTTs)",
        rtts.len()
    ));
    let sag_cap = P99_FLOOR.max(p99_first.mul_f64(P99_FACTOR));
    if p99_last > sag_cap || p99_last > P99_HARD_CAP {
        proc.fail(&format!(
            "tail-latency sag: first-quarter p99 {p99_first:?} → last-quarter {p99_last:?} \
             (cap {sag_cap:?}, hard cap {P99_HARD_CAP:?})"
        ));
    }

    // ---- The standing oracle: everything acked across the soak delivered ----
    for i in 0..proc.subs.len() {
        proc.drain_subscriber(i).await;
        let topic = proc.subs[i].topic.clone();
        let owed = proc.acked.get(&topic).cloned().unwrap_or_default();
        let missing = owed
            .iter()
            .filter(|p| !proc.subs[i].received.contains(*p))
            .count();
        // An offline-churned subscriber holds its queue durably; resume + drain.
        if missing > 0 {
            proc.bring_subscriber_online(i, true).await;
            proc.drain_subscriber(i).await;
            proc.drain_subscriber(i).await;
        }
        let still_missing = owed
            .iter()
            .filter(|p| !proc.subs[i].received.contains(*p))
            .count();
        if still_missing > 0 {
            proc.fail(&format!(
                "acked durability violated for {topic}: {still_missing} acked payload(s) \
                 never delivered after the soak"
            ));
        }
    }
    eprintln!(
        "cluster_soak: {}s, {} acked publishes\n  {}",
        total.as_secs(),
        rtts.len(),
        report.join("\n  ")
    );
    for node in &mut proc.nodes {
        node.kill().await;
    }
}
