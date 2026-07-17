//! **Out-of-process** cluster harness over real spawned `mqttd` binaries
//! ([ADR 0044](../../docs/adr/0044-release-readiness-assurance.md) P1).
//!
//! Where `cluster_stress.rs` assembles durable nodes *in one process sharing
//! one binary*, this tier spawns the **compiled production binary**
//! (`CARGO_BIN_EXE_mqttd`) per node — real processes with real data dirs, real
//! TCP/MQTT listeners, real UDP gossip sockets, configured purely through the
//! documented `MQTTD_*` environment exactly as an operator would. What that
//! buys over the in-process tier:
//!
//! - **kill is `SIGKILL`**: the kernel reclaims the process — file handles,
//!   sockets, everything — with no in-process stand-in deciding what "crash"
//!   means; a restart reopens the surviving data dir cold (ADR 0018);
//! - **the config surface is the tested surface**: node assembly is `main.rs`
//!   itself, not a test-side mirror of it;
//! - **readiness is the operator's**: bring-up, quiesce, and restart gates all
//!   read `/readyz` (ADR 0020), never internal state;
//! - **placement is invisible**: clients attach through ANY node and the
//!   ADR 0005 owner-relay routes them — the production client path, black-box.
//!
//! Each node's peer listener is fronted by an **unprivileged TCP relay** in
//! the test process, advertised via `MQTTD_PEER_ADVERTISE`. The 0044-P2 fault
//! vocabulary drives it — sever (asymmetric partition / half-open link) and
//! slow (browned-out link) — and adds the faults only the OS can deliver:
//! `SIGKILL` mid-burst, the kernel's `SIGXFSZ` disk-full death on a real
//! `RLIMIT_FSIZE` bound (0018-T7), and kill/respawn flapping faster than
//! death confirmation (0007-T8).
//!
//! The schedule vocabulary and the **acked-facts oracle** are the ADR 0042
//! ones, ported: a payload is owed only from its PUBACK; a retained value
//! converges from its last acked set onward; every resume of an established
//! session must report `session_present = true` (ADR 0017). Timings differ
//! from the in-process tier — spawned nodes run the production SWIM defaults
//! (seconds-scale death confirmation), so schedules here are shorter and the
//! windows more generous; the seed reproduces the scenario, not the timing.

mod common;
mod proc_common;

use std::time::{Duration, Instant};

use mqtt_codec::{Packet, QoS};
use proc_common::*;

/// Set to `Some(seed)` to run a single seed (e.g. to reproduce a reported failure).
const REPRO_SEED: Option<u64> = None;

/// One spawned cluster at a time: each test stands up 3 broker PROCESSES and
/// judges them against real-time windows (ack deadlines, bring-up bounds);
/// three clusters contending for one runner starve each other into flakes.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Spawned processes with production SWIM timings are expensive (~2 min per
/// seed), so the CI profile runs ONE seed; `MQTTD_PROC_SEEDS=N` widens the
/// sweep for the nightly tier (ADR 0044 P4).
const DEFAULT_SEEDS: u64 = 1;

fn seeds() -> Vec<u64> {
    if let Some(s) = REPRO_SEED {
        return vec![s];
    }
    let n = std::env::var("MQTTD_PROC_SEEDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEEDS);
    (0..n).collect()
}

/// One full seeded schedule over spawned processes: bring up a real 3-node
/// cluster, run the seeded workload with a mid-burst SIGKILL and a restart,
/// quiesce on `/readyz`, and run the acked-facts oracle black-box.
// One deliberately linear narrative — bring-up, schedule, quiesce, oracle —
// matching the in-process harness; splitting it would scatter the seed's story.
#[allow(clippy::too_many_lines)]
async fn run_schedule(seed: u64) {
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    establish_subscribers(&mut proc, 2).await;

    // The seeded schedule: ~10 steps with a SIGKILL at a seeded position and a
    // restart a couple of steps later — every seed exercises the whole
    // crash/recover cycle; the mix between them is seeded.
    let steps = proc.rng.range(8, 12);
    let kill_at = proc.rng.range(2, steps - 3);
    let restart_at = kill_at + proc.rng.range(2, 3);
    for step in 0..steps {
        if step == kill_at {
            proc.kill_step().await;
            continue;
        }
        if step == restart_at {
            proc.restart_step().await;
            continue;
        }
        match proc.rng.range(0, 100) {
            0..=34 => proc.publish_step().await,
            35..=57 => proc.retained_step().await,
            58..=73 => proc.churn_step().await,
            74..=87 => proc.flap_step(),
            _ => proc.slow_step(),
        }
    }
    let count = |needle: &str| proc.trace.iter().filter(|l| l.contains(needle)).count();
    eprintln!(
        "cluster_proc: seed {seed} schedule: {} publishes ({} owed), {} retained, \
         {} sigkills (mid-burst), {} restarts, {} severs, {} slows",
        count("publish #"),
        count("ACKED (obligation)"),
        count("retained set #"),
        count("SIGKILLED"),
        count("RESTARTED"),
        count("SEVERED"),
        count("SLOWED"),
    );

    // Heal any (P2-vocabulary) severs and quiesce on /readyz.
    for i in proc.alive_nodes() {
        proc.nodes[i].relay.heal();
    }
    proc.quiesce().await;

    // ---- The oracle (post-quiesce facts only, all black-box) ----

    // 1. Acked durability + recovery honesty.
    oracle_acked_facts(&mut proc).await;

    // 2. Retained convergence: every alive node serves the same value, never
    //    behind the last acked set; fan-out is eventually consistent, so poll.
    let mut probe = 0u64;
    for (topic, history) in proc.retained.clone() {
        let Some(last_acked) = history.iter().rposition(|r| r.acked) else {
            continue; // nothing was ever promised for this topic
        };
        let candidates: Vec<&Vec<u8>> = history[last_acked..].iter().map(|r| &r.payload).collect();

        let deadline = Instant::now() + Duration::from_secs(20);
        let (converged, last_seen) = loop {
            let mut values: Vec<(String, Option<Vec<u8>>)> = Vec::new();
            for i in proc.alive_nodes() {
                probe += 1;
                let observed = retained_seen(
                    proc.nodes[i].client_addr,
                    &format!("probe-{seed}-{probe}"),
                    &topic,
                )
                .await;
                values.push((proc.nodes[i].id.clone(), observed));
            }
            let all_good = values
                .iter()
                .all(|(_, v)| v.as_ref().is_some_and(|value| candidates.contains(&value)))
                && values.windows(2).all(|w| w[0].1 == w[1].1);
            if all_good {
                break (true, values);
            }
            if Instant::now() >= deadline {
                break (false, values);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        if !converged {
            let detail: Vec<String> = last_seen
                .iter()
                .map(|(node, v)| {
                    format!(
                        "{node}: {:?}",
                        v.as_ref().map(|p| String::from_utf8_lossy(p).into_owned())
                    )
                })
                .collect();
            proc.fail(&format!(
                "retained convergence violated for {topic}: nodes never converged \
                 on a value at or beyond the last acked set: {detail:?}"
            ));
        }
    }

    // Tear the cluster down (SIGKILL — the dirs are temp).
    for node in &mut proc.nodes {
        node.kill().await;
    }
}

/// The P1 skeleton test: real spawned binaries, a SIGKILL, a cold restart over
/// the surviving dir, and the acked-facts oracle — black-box end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn spawned_process_schedules_hold_acked_facts() {
    let _serial = SERIAL.lock().await;
    for seed in seeds() {
        run_schedule(seed).await;
    }
}

/// 0018-T7, un-deferred (ADR 0044 P2): the **disk-full crash mid-write**. One
/// node runs under a kernel-enforced `RLIMIT_FSIZE` (a real filesystem bound,
/// no privileges); acked 64KB publishes to an offline durable subscriber grow
/// every replica's store until the bounded node's next write crosses the limit
/// and the kernel delivers `SIGXFSZ` — death exactly ON a write syscall, the
/// harshest honest form of "the disk ran out mid-operation". The survivors
/// keep quorum (acks keep flowing); the restart reopens the possibly-torn dir
/// UNBOUNDED, redb must roll back any torn write on reopen, catch-up (ADR 0043
/// P1) back-fills what the node missed while dead, and every acked payload
/// must replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_disk_bound_crash_mid_write_loses_no_acked_fact() {
    let _serial = SERIAL.lock().await;
    let seed = 918;
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    // 16384 × 512B blocks = 8MB per file: roomy for formation, fatal under the
    // blast (each 64KB enqueue lands on every replica's store — R=3 on 3 nodes).
    nodes[2].file_size_limit_blocks = Some(16384);
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    establish_subscribers(&mut proc, 1).await;

    // Take the subscriber offline: every acked publish from here is a durable
    // offline enqueue — quorum-replicated bytes on disk, nothing in a session.
    proc.drain_subscriber(0).await;
    if let Some(mut conn) = proc.subs[0].conn.take() {
        conn.disconnect().await;
    }

    // Blast through the UNBOUNDED nodes until the kernel kills the bounded one.
    let topic = proc.subs[0].topic.clone();
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut publisher: Option<common::Client> = None;
    let mut i = 0u64;
    loop {
        if proc.nodes[2].died() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bounded node never crossed its file-size limit under the blast"
        );
        i += 1;
        let mut payload = format!("dd-{seed}-{i}-").into_bytes();
        payload.resize(64 * 1024, b'x');
        if publisher.is_none() {
            publisher = common::Client::connect_v311_within(
                proc.nodes[0].client_addr,
                &format!("dd-pub-{seed}"),
                true,
                Duration::from_secs(8),
            )
            .await
            .map(|(c, _)| c);
        }
        let Some(p) = publisher.as_mut() else {
            continue;
        };
        p.publish(&topic, &payload, QoS::AtLeastOnce, Some(7), vec![])
            .await;
        let ack_deadline = Instant::now() + Duration::from_secs(10);
        let mut closed = false;
        let got = loop {
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
            publisher = None; // reconnect on the next pass
        }
        if got {
            proc.acked.entry(topic.clone()).or_default().push(payload);
        }
    }
    proc.alive[2] = false;
    let owed = proc.acked.get(&topic).map_or(0, Vec::len);
    proc.note(format!(
        "bounded node died on a write (SIGXFSZ) after {owed} acked 64KB publishes"
    ));
    assert!(owed > 0, "vacuous: nothing was acked before the crash");

    // Restart UNBOUNDED over the surviving (possibly torn) dir and verify.
    proc.nodes[2].file_size_limit_blocks = None;
    proc.restart_step().await;
    proc.quiesce().await;
    oracle_acked_facts(&mut proc).await;
    eprintln!("cluster_proc: disk-bound crash held {owed} acked 64KB obligations");
    for node in &mut proc.nodes {
        node.kill().await;
    }
}

/// 0007-T8, un-deferred (ADR 0044 P2): **membership flap at SWIM-confusing
/// rates**. Three cycles of SIGKILL + IMMEDIATE respawn — faster than
/// suspicion can confirm a death, the fast-restart shape that produced the
/// 0043-P4 void-ack exhibit — with acked publishes flowing through the
/// survivors while the flapped node rejoins. Every ack collected anywhere in
/// the storm is a hard obligation; the oracle runs after the last cycle
/// settles.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rapid_kill_restart_flapping_loses_no_acked_fact() {
    let _serial = SERIAL.lock().await;
    let seed = 717;
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    establish_subscribers(&mut proc, 2).await;

    for cycle in 0..3usize {
        let victim = 1 + (cycle % 2); // one node down at a time; the founder anchors
        proc.nodes[victim].kill().await;
        for sub in &mut proc.subs {
            if sub.conn.is_some() && sub.via_node == victim {
                sub.conn = None;
            }
        }
        // IMMEDIATE respawn over the surviving dir — no death-confirmation
        // wait, the exact window the fast-restart honesty fixes guard.
        proc.nodes[victim].spawn();
        let id = proc.nodes[victim].id.clone();
        proc.note(format!(
            "FLAP cycle {cycle}: SIGKILL + immediate respawn of {id}"
        ));
        // Acked load while the flapped node rejoins.
        proc.publish_step().await;
        proc.publish_step().await;
        // Re-admission before the next flap (never two nodes down at once). A
        // respawn that lost the port-rebind race is respawned once more.
        if !proc
            .wait_node_serving(victim, Duration::from_secs(30))
            .await
            && proc.nodes[victim].died()
        {
            proc.nodes[victim].spawn();
        }
        assert!(
            proc.wait_node_serving(victim, Duration::from_secs(60))
                .await,
            "flapped node {id} never re-admitted (cycle {cycle})"
        );
    }
    proc.quiesce().await;
    oracle_acked_facts(&mut proc).await;
    let count = |needle: &str| proc.trace.iter().filter(|l| l.contains(needle)).count();
    eprintln!(
        "cluster_proc: flap storm: 3 kill/respawn cycles, {} publishes ({} owed)",
        count("publish #"),
        count("ACKED (obligation)"),
    );
    for node in &mut proc.nodes {
        node.kill().await;
    }
}
