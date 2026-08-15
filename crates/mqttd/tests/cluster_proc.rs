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

/// The retained anti-entropy cadence: every node offers its digest each
/// `RETAINED_ANTIENTROPY_EVERY` (30) sweep ticks of ~1s (`hub.rs`). A divergence
/// left by a fan-out frame that died with its sender can legitimately persist for
/// one full period before the repair even STARTS — any convergence deadline must
/// sit OUTSIDE this, and the failure verdict is judged against it.
const ANTIENTROPY_PERIOD: Duration = Duration::from_secs(30);

/// How long the retained-convergence oracle polls before declaring failure: the
/// anti-entropy period plus margin for the exchange itself. The old 20s deadline
/// sat INSIDE the repair period and could not tell "converging slowly" from
/// "never converging" (issue #214) — widened only together with the diagnostic
/// that says which side of that line a failure landed on, per the issue's rule.
const CONVERGENCE_DEADLINE: Duration = Duration::from_secs(45);

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
    wait_all_ready(&mut nodes, seed).await;
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

        let started = Instant::now();
        let deadline = started + CONVERGENCE_DEADLINE;
        // Every poll round is recorded as `(elapsed, per-node observation)`; the
        // failure report prints the rounds where the picture CHANGED, so a reader
        // sees at a glance whether convergence was in motion (deadline too tight
        // on a loaded runner) or dead in the water (the repair path is broken).
        let mut rounds: Vec<(Duration, String)> = Vec::new();
        let converged = loop {
            let mut values: Vec<(String, Option<Vec<u8>>)> = Vec::new();
            for i in proc.alive_nodes() {
                probe += 1;
                let observed = retained_seen(
                    proc.nodes[i].client_addr,
                    &format!("probe-{seed}-{probe}"),
                    &topic,
                )
                .await;
                let ev = history_check::Event::RetainedProbe {
                    at_ms: proc.at_ms(),
                    node: proc.nodes[i].id.clone(),
                    topic: topic.clone(),
                    payload: observed
                        .as_ref()
                        .map(|p| String::from_utf8_lossy(p).into_owned()),
                };
                proc.record(ev);
                values.push((proc.nodes[i].id.clone(), observed));
            }
            let all_good = values
                .iter()
                .all(|(_, v)| v.as_ref().is_some_and(|value| candidates.contains(&value)))
                && values.windows(2).all(|w| w[0].1 == w[1].1);
            let picture = values
                .iter()
                .map(|(node, v)| {
                    format!(
                        "{node}={}",
                        v.as_ref()
                            .map_or("<none>".into(), |p| String::from_utf8_lossy(p).into_owned())
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            rounds.push((started.elapsed(), picture));
            if all_good {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        if !converged {
            // WHAT was awaited: any common value at or beyond the last acked set.
            let awaited = candidates
                .iter()
                .map(|p| String::from_utf8_lossy(p).into_owned())
                .collect::<Vec<_>>()
                .join(" | ");
            // WHAT was observed, compressed to the rounds where anything changed.
            let mut timeline = Vec::new();
            let mut last_change_at = Duration::ZERO;
            for (i, (t, picture)) in rounds.iter().enumerate() {
                if i == 0 || *picture != rounds[i - 1].1 {
                    timeline.push(format!("t+{:>5.1}s  {picture}", t.as_secs_f64()));
                    last_change_at = *t;
                }
            }
            let (final_t, final_picture) = rounds.last().expect("at least one round ran");
            let stable_for = final_t.saturating_sub(last_change_at);
            // THE VERDICT the two CI flakes could not deliver: was convergence
            // still in motion when the deadline hit, or provably not coming?
            let verdict = if stable_for >= ANTIENTROPY_PERIOD {
                format!(
                    "STABLE divergence: the picture did not change for the final \
                     {:.1}s — at least one full anti-entropy period (~{}s) passed \
                     with every repair round leaving the nodes as they were. More \
                     deadline would not have converged this; suspect the retained \
                     repair path, not the runner.",
                    stable_for.as_secs_f64(),
                    ANTIENTROPY_PERIOD.as_secs()
                )
            } else {
                format!(
                    "the picture last changed {:.1}s before the deadline (under one \
                     anti-entropy period of ~{}s) — convergence may still have been \
                     in motion; a loaded runner stretching the exchange is plausible.",
                    stable_for.as_secs_f64(),
                    ANTIENTROPY_PERIOD.as_secs()
                )
            };
            // The operator-visible state of every node at failure time.
            let mut readyz = Vec::new();
            for i in proc.alive_nodes() {
                let r = proc.nodes[i].readyz().await;
                readyz.push(format!("{}: {}", proc.nodes[i].id, describe_readyz(r)));
            }
            proc.fail(&format!(
                "retained convergence violated for {topic}\n\
                 awaited: any common value in [{awaited}] (the last acked set onward)\n\
                 final:   {final_picture} (after {:.1}s, {} poll rounds)\n\
                 verdict: {verdict}\n\
                 observation timeline (rounds where the picture changed):\n  {}\n\
                 /readyz at failure:\n  {}",
                final_t.as_secs_f64(),
                rounds.len(),
                timeline.join("\n  "),
                readyz.join("\n  ")
            ));
        }
    }

    // ---- The INDEPENDENT verdict (issue #231, ADR 0044) ----
    //
    // Everything above was judged by this harness; the recorded client-visible
    // history is now re-judged by `history-check`, a crate that shares nothing
    // with the broker or this file's oracles. Every CI run of every seed passes
    // through it; MQTTD_PROC_HISTORY_DIR additionally persists the JSONL so the
    // nightly job archives what was checked (and the checker binary can re-run
    // it standalone).
    let violations = history_check::check(&proc.history);
    if let Ok(dir) = std::env::var("MQTTD_PROC_HISTORY_DIR") {
        let _ = std::fs::create_dir_all(&dir);
        let path = std::path::Path::new(&dir).join(format!("seed-{seed}.jsonl"));
        let jsonl: String = proc
            .history
            .iter()
            .map(|e| serde_json::to_string(e).expect("history events serialize") + "\n")
            .collect();
        if let Err(e) = std::fs::write(&path, jsonl) {
            eprintln!(
                "cluster_proc: could not persist the history to {}: {e}",
                path.display()
            );
        } else {
            eprintln!("cluster_proc: history persisted to {}", path.display());
        }
    }
    if !violations.is_empty() {
        let detail: Vec<String> = violations.iter().map(ToString::to_string).collect();
        proc.fail(&format!(
            "the INDEPENDENT history check found {} violation(s) the in-harness \
             oracles did not:\n  {}",
            violations.len(),
            detail.join("\n  ")
        ));
    }
    eprintln!(
        "cluster_proc: seed {seed}: independent history check OK ({} events)",
        proc.history.len()
    );

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
    wait_all_ready(&mut nodes, seed).await;
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
    wait_all_ready(&mut nodes, seed).await;
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

/// The rendered series the floor's refusal moves — `hub.rs`'s `durable_failure_reason`
/// maps `StorageError::Unavailable` (which is what `ReplError::UnderReplicated` becomes) to
/// `unavailable`. Asserting on this label, not merely on a missing PUBACK, is what
/// distinguishes the floor from `NotOwner` (`not-owner`) and recovery (`no-quorum`).
const FLOOR_REFUSALS: &str = "mqttd_durable_append_failures_total{reason=\"unavailable\"}";

/// Issue #239, end to end on the real binary with the SHIPPED default config: the
/// min-replicas write floor arms itself from the membership the node knows, refuses the
/// single-copy durable promise that is the defect, and costs nothing while a majority is
/// up.
///
/// Four parts, in order of how load-bearing they are:
///
/// 1. **The default is on.** A real 3-node cluster, no `MQTTD_MIN_REPLICAS` anywhere,
///    reports `write_floor: 2, write_floor_source: "derived"` on every node. Before the
///    fix this is `1` forever — this is the assertion that fails on the old default.
/// 2. **It costs no availability.** SIGKILL one node: two live members still hold two
///    copies (`quorum = len/2+1` already needed two acks), so a `QoS` 1 publish is still
///    acked *and the subscriber actually receives it*. This is the rolling-maintenance
///    guarantee, and it is what an unconditional floor of 3 would have broken.
/// 3. **The lone survivor REFUSES, client-visibly.** SIGKILL a second: the survivor
///    reports `min_actual (1) < write_floor (2)`, derived — the page condition
///    `docs/OPERATIONS.md` alerts on — and a `QoS` 1 publish for a durable session it
///    still owns gets **no PUBACK** for a whole budget, with the floor's own refusal in
///    the log and `mqttd_durable_append_failures_total{reason="unavailable"}` moving once
///    per attempt. Turn the floor off (`MQTTD_MIN_REPLICAS=1`) and the same publish is
///    acked on ONE copy: that is issue #239, and it is the one-env-var falsification of
///    this part.
/// 4. **The refusal is transient.** Restart a node and the ack flows again — and the
///    message is delivered — with no operator action.
///
/// **Ownership is chosen, not hoped for.** The refusal only happens where the survivor
/// still *leases* the group: `LocalLeaseSource::epoch_for` is a purely local read of the
/// applied lease map (no leadership, quorum or TTL check), so a survivor keeps writing to
/// the groups it already held while reassignment waits for a quorum that is gone. So this
/// test picks one durable client id **per node** from the broker's own HRW hash before
/// anything spawns, and connects each subscriber to *its* owner (no ADR 0005 relocation,
/// session hosted there, socket survives the other nodes' deaths). An earlier version used
/// one arbitrary subscriber, drew a session owned by a node it was about to kill, and saw
/// the publish acked-and-dropped by the no-known-subscriber path — which is a different
/// gap (README Limitations), not evidence about the floor.
///
/// **The `replica_groups` gate is required, not decoration.**
/// `ReplicatedSessionStore::enqueue_with_expiry` reads (`live_range`) before it appends,
/// and the read is NOT floor-gated, so until the ADR 0043 P1 catch-up sweep re-stamps the
/// shrunken replica sets the enqueue fails EARLIER with `NoQuorum` and the floor never
/// gets a turn. Part 3 therefore polls `/statusz` `replica_groups.current == tracked`
/// before probing, and asserts on the *reason* (the floor's own log line plus
/// `{reason="unavailable"}`) rather than on the absence of a packet — no other failure mode
/// satisfies that. `NotOwner`/`NoQuorum` would render `{reason="not-owner"}` /
/// `{reason="no-quorum"}` and a different error string, which is what excludes them; the
/// `durable ownership split` diagnostic cannot serve here because it is a `debug!` and this
/// harness runs the nodes at `info`.
///
/// Every availability probe RETRIES until its budget runs out. A single-shot probe is not
/// an availability observation: a fresh publish landing in a takeover window is refused for
/// reasons that have nothing to do with `min_replicas` (this harness saw exactly that —
/// one attempt refused, the next acked 0.5s later), so only "eventually acked" shows
/// availability. The refusal in part 3 is the mirror image: a whole budget of refusals,
/// every one of them carrying the floor's reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)] // a four-part scenario over spawned binaries
async fn the_default_write_floor_arms_itself_and_refuses_the_single_copy_promise() {
    let _serial = SERIAL.lock().await;
    let seed = 239;
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    let ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
    // One durable client id per node, each in a placement group that node HRW-owns —
    // computed from the broker's OWN hash over the fixed node ids, before anything spawns.
    let owned = one_durable_client_per_node(seed, &ids);
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&mut nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    for (i, client) in owned.iter().enumerate() {
        subscribe_on_owner(&mut proc, i, client).await;
    }

    // --- 1. The default posture is ON and derived, on every node ---
    for i in 0..proc.nodes.len() {
        let observed = poll_replication(&proc.nodes[i], |(members, _, floor, derived)| {
            members == 3 && floor == 2 && derived
        })
        .await;
        assert!(
            observed.is_ok(),
            "node {} never reported the derived write floor of 2 under the shipped \
             default; {}\n{}",
            proc.nodes[i].id,
            replication_poll_failure(observed.unwrap_err()),
            log_tail(&proc.nodes[i].log_path)
        );
    }
    proc.note("every node derives write_floor=2 from the 3-member roster".to_string());

    // The survivor is a raft FOLLOWER (the leader works too — the shorter, calmer path).
    // Its own durable session is `owned[survivor]`, hosted on it and still connected.
    let mut leader = None;
    for (i, node) in proc.nodes.iter().enumerate() {
        if http_get(node.health_addr, "/statusz")
            .await
            .unwrap_or_default()
            .contains("\"lease\":{\"leader\":true")
        {
            leader = Some(i);
        }
    }
    let leader = leader.expect("no node reported itself the lease-group raft leader");
    let followers: Vec<usize> = (0..proc.nodes.len()).filter(|i| *i != leader).collect();
    let survivor = followers[0];
    // Kill the other follower first, so part 2 runs with the raft leader still up; the
    // leader dies in part 3, leaving the survivor with frozen committed leases.
    let victims = [followers[1], leader];
    assert!(
        !std::fs::read_to_string(&proc.nodes[survivor].log_path)
            .unwrap_or_default()
            .lines()
            .any(|l| l.contains("relocating persistent session") && l.contains(&owned[survivor])),
        "the survivor's own durable session was relocated away — the scenario cannot \
         judge the floor; log tail:\n{}",
        log_tail(&proc.nodes[survivor].log_path)
    );
    proc.note(format!(
        "survivor {} (follower), victims {:?}, its own durable session {}",
        proc.nodes[survivor].id,
        victims.map(|v| proc.nodes[v].id.clone()),
        owned[survivor]
    ));

    // --- 2. One node down: the floor refuses nothing, and the promise is kept ---
    kill_and_disconnect(&mut proc, victims[0]).await;
    let two_live = poll_replication(&proc.nodes[survivor], |(members, min_actual, floor, _)| {
        members == 2 && min_actual == 2 && floor == 2
    })
    .await;
    assert!(
        two_live.is_ok(),
        "the survivors never converged on 2 members holding 2 copies; {}\n{}",
        replication_poll_failure(two_live.unwrap_err()),
        log_tail(&proc.nodes[survivor].log_path)
    );
    assert_acked_and_delivered(
        &mut proc,
        survivor,
        survivor,
        "two-live",
        "a floor of 2 must not refuse anything while two members are live — this is the \
         one-at-a-time rolling-upgrade guarantee",
    )
    .await;
    proc.note("one node down: QoS 1 acked AND delivered at the floor".to_string());

    // --- 3. Two down: the lone survivor refuses the single-copy promise ---
    kill_and_disconnect(&mut proc, victims[1]).await;
    let refusing = poll_replication(
        &proc.nodes[survivor],
        |(members, min_actual, floor, derived)| {
            members == 1 && min_actual < floor && floor == 2 && derived
        },
    )
    .await;
    assert!(
        refusing.is_ok(),
        "the lone survivor never reported min_actual < write_floor — the documented \
         page condition (docs/OPERATIONS.md); {}\n{}",
        replication_poll_failure(refusing.unwrap_err()),
        log_tail(&proc.nodes[survivor].log_path)
    );
    // The same condition on the GAUGES the page rule actually reads — which is what pins
    // the hub metrics tick to `set_replication_health`'s third argument. Polled, not read
    // once: `/statusz` reads placement live while the gauges are published on the hub's
    // ~1s metrics tick, so a single read right after the `/statusz` poll observes the
    // previous tick.
    let gauges_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let floor = proc.nodes[survivor]
            .metric("mqttd_replication_write_floor")
            .await;
        let min_actual = proc.nodes[survivor]
            .metric("mqttd_replication_min_actual")
            .await;
        if floor == Some(2) && min_actual == Some(1) {
            break;
        }
        assert!(
            Instant::now() < gauges_deadline,
            "the page rule's gauges never showed the refusal: \
             mqttd_replication_write_floor={floor:?} (want 2), \
             mqttd_replication_min_actual={min_actual:?} (want 1). \
             docs/OPERATIONS.md pages on exactly this pair"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    proc.note("two nodes down: min_actual < write_floor on /statusz and /metrics".to_string());

    // The ADR 0043 P1 re-stamp, polled on the operator's own surface. Without it the
    // enqueue fails at the ungated `live_range` with NoQuorum and the floor is masked, so
    // a withheld PUBACK here would be evidence about recovery, not about the floor.
    let stamp_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let groups = proc.nodes[survivor].statusz_replica_groups().await;
        if matches!(groups, Some((current, tracked)) if tracked > 0 && current == tracked) {
            break;
        }
        assert!(
            Instant::now() < stamp_deadline,
            "SETUP: the survivor's replica sets were never re-stamped current for the \
             shrunken membership (/statusz replica_groups = {groups:?}), so a refused \
             publish below could not be attributed to the write floor\n\
             ---- log tail ----\n{}",
            log_tail(&proc.nodes[survivor].log_path)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Absent = zero: the registry only renders label sets that have been touched.
    let before = proc.nodes[survivor]
        .metric(FLOOR_REFUSALS)
        .await
        .unwrap_or(0);
    let (acked_attempt, attempts) = qos1_first_ack_within(
        proc.nodes[survivor].client_addr,
        &format!("f-{seed}-refuse"),
        &proc.subs[survivor].topic,
        b"floor-probe-refused",
        Duration::from_secs(45),
    )
    .await;
    // The refusal is LOGGED by the connection task and COUNTED by the hub, so the two are not
    // simultaneous. Reading the counter once, immediately after the last attempt, therefore
    // measures scheduler luck as much as the broker: this test failed twice inside a full
    // `cargo test --all` on a loaded machine (`0 -> 0`) and passed 2/2 in isolation, which is
    // the signature of that race rather than of a broken counter. Poll it, bounded — the
    // assertion below is unchanged in strength (a counter that never moves still fails, and
    // still says the alertable surface is broken), it just no longer depends on which task the
    // scheduler ran first. Issue #260's own rule, applied to the test that needed it.
    let mut after = before;
    for _ in 0..40 {
        after = proc.nodes[survivor]
            .metric(FLOOR_REFUSALS)
            .await
            .unwrap_or(0);
        if after > before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let log = std::fs::read_to_string(&proc.nodes[survivor].log_path).unwrap_or_default();
    let refusal = log.lines().find(|l| {
        l.contains("withholding the publisher's ack")
            && l.contains("replica set holds 1 of the configured floor of 2 copies")
            && l.contains(&owned[survivor])
    });
    assert!(
        acked_attempt.is_none(),
        "the lone survivor of a three-member cluster ACKED a durable QoS 1 publish on \
         attempt {:?} of {attempts} — a promise held on ONE copy, which is issue #239 \
         itself. Run this with MQTTD_MIN_REPLICAS=1 and this is the expected result; \
         under the shipped default it must not happen\n---- log tail ----\n{}",
        acked_attempt,
        log_tail(&proc.nodes[survivor].log_path)
    );
    assert!(
        refusal.is_some(),
        "no PUBACK arrived in {attempts} attempts, but NOT because of the write floor — \
         no log line names the floor's refusal for {}. A withheld ack alone cannot \
         distinguish the floor from NotOwner/NoQuorum, so this is a setup failure, not a \
         pass\n---- log tail ----\n{}",
        owned[survivor],
        log_tail(&proc.nodes[survivor].log_path)
    );
    assert!(
        after > before,
        "the floor refused in the log but {FLOOR_REFUSALS} did not move ({before} -> {after}) — \
         the only alertable surface for the refusal is broken"
    );
    proc.note(format!(
        "lone survivor: {attempts} QoS 1 attempts, 0 PUBACKs, {FLOOR_REFUSALS} {before} -> {after}, \
         refusal line: {}",
        refusal.unwrap_or_default().trim()
    ));

    // --- 4. The refusal is transient: capacity returning lifts it, unattended ---
    proc.restart_step().await;
    let lifted = poll_replication(&proc.nodes[survivor], |(members, min_actual, floor, _)| {
        members >= 2 && min_actual >= floor
    })
    .await;
    assert!(
        lifted.is_ok(),
        "capacity returned but the survivor still reports a starved replica set; {}\n{}",
        replication_poll_failure(lifted.unwrap_err()),
        log_tail(&proc.nodes[survivor].log_path)
    );
    assert_acked_and_delivered(
        &mut proc,
        survivor,
        survivor,
        "restored",
        "the refusal must lift by itself once the replica set is back at the floor",
    )
    .await;

    for node in &mut proc.nodes {
        node.kill().await;
    }
}

/// One durable client id per node, each in a placement group that node HRW-owns: `ids[i]`
/// owns `returned[i]`. Computed with the broker's OWN placement hash
/// (`placement::group_of` + `Placement::hrw_owner`) over the fixed node ids, before
/// anything spawns — so which node leases which session is a decision of this test, not
/// a coincidence of the run. The lease assigner drives ownership to the HRW owner, so in
/// steady state the HRW owner is the committed lease holder.
fn one_durable_client_per_node(seed: u64, ids: &[String]) -> Vec<String> {
    use mqtt_cluster::placement::{group_of, Placement};
    use mqtt_cluster::swim::MemberState;
    use mqtt_cluster::NodeId;

    let mut ring = Placement::new(NodeId(ids[0].clone()), 3);
    for id in &ids[1..] {
        ring.observe(&NodeId(id.clone()), MemberState::Alive, "127.0.0.1:1", None);
    }
    let mut owned = vec![String::new(); ids.len()];
    for k in 0..100_000u32 {
        let candidate = format!("fsub-{seed}-{k}");
        let hrw = ring.hrw_owner(group_of(&candidate)).0;
        if let Some(slot) = ids.iter().position(|n| *n == hrw) {
            if owned[slot].is_empty() {
                owned[slot] = candidate;
            }
        }
        if owned.iter().all(|s| !s.is_empty()) {
            return owned;
        }
    }
    panic!("no client id found for some node in {ids:?}");
}

/// Register subscriber `i` as `client`, connected to node `i` — the node that HRW-owns its
/// placement group — with `clean_session = false` and a `QoS` 1 subscription. Connecting to
/// the OWNER is the point: the session is hosted there (no ADR 0005 relocation), so it is
/// still owned and still connected when every other node is dead.
async fn subscribe_on_owner(proc: &mut proc_common::Proc, i: usize, client: &str) {
    let seed = proc.seed;
    let topic = format!("fl/{seed}/{i}");
    let addr = proc.nodes[i].client_addr;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some((mut conn, present)) =
            common::Client::connect_v311_within(addr, client, false, Duration::from_secs(10)).await
        {
            // The SUBACK is durability-gated, so retry until it is granted.
            let ack = conn.subscribe(1, &topic, QoS::AtLeastOnce).await;
            if ack.return_codes.iter().all(|rc| *rc != 0x80) {
                proc.subs.push(proc_common::Subscriber {
                    id: client.to_string(),
                    topic: topic.clone(),
                    conn: Some(conn),
                    via_node: i,
                    established: true,
                    received: std::collections::BTreeSet::new(),
                });
                let at_ms = proc.at_ms();
                let via_node = proc.nodes[i].id.clone();
                proc.record(history_check::Event::Connect {
                    at_ms,
                    client: client.to_string(),
                    via_node,
                    session_present: present,
                });
                proc.record(history_check::Event::Subscribe {
                    at_ms,
                    client: client.to_string(),
                    topic,
                });
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "durable SUBSCRIBE for {client} on its owner {} was never granted",
            proc.nodes[i].id
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The full availability oracle for one probe: a `QoS` 1 publish through node `via` for
/// subscriber `sub`'s topic must be ACKED **and** its payload must actually reach the
/// subscriber.
///
/// The second half is not ceremony. `hub.rs` acks and drops a publish with no known
/// subscriber, so asserting only the PUBACK lets "the floor costs no availability" pass on
/// an ack that promised nothing — the exact vacuity this file's own part-3 comment used to
/// describe.
async fn assert_acked_and_delivered(
    proc: &mut proc_common::Proc,
    via: usize,
    sub: usize,
    tag: &str,
    why: &str,
) {
    let payload = format!("floor-probe-{}-{tag}", proc.seed).into_bytes();
    let topic = proc.subs[sub].topic.clone();
    let addr = proc.nodes[via].client_addr;
    let (acked, attempts) = qos1_first_ack_within(
        addr,
        &format!("f-{}-{tag}", proc.seed),
        &topic,
        &payload,
        Duration::from_secs(60),
    )
    .await;
    assert!(
        acked.is_some(),
        "{why}: no PUBACK in {attempts} attempts\n---- log tail ----\n{}",
        log_tail(&proc.nodes[via].log_path)
    );
    // The subscriber may have lost its socket to a kill; a durable session resumes.
    if proc.subs[sub].conn.is_none() {
        proc.bring_subscriber_online(sub, true).await;
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        proc.drain_subscriber(sub).await;
        if proc.subs[sub].received.contains(&payload) {
            return;
        }
        if Instant::now() >= deadline {
            break;
        }
        if proc.subs[sub].conn.is_none() {
            proc.bring_subscriber_online(sub, true).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!(
        "{why}: the publish was ACKED but {} never received it — an ack that promised \
         nothing is not availability\n---- log tail ----\n{}",
        proc.subs[sub].id,
        log_tail(&proc.nodes[via].log_path)
    );
}

/// `SIGKILL` node `idx` and forget the subscriber connections that ran through it — the
/// bookkeeping every kill in this file does, so the floor test's own body stays about the
/// floor.
async fn kill_and_disconnect(proc: &mut proc_common::Proc, idx: usize) {
    proc.nodes[idx].kill().await;
    proc.alive[idx] = false;
    for sub in &mut proc.subs {
        if sub.conn.is_some() && sub.via_node == idx {
            sub.conn = None;
        }
    }
}

/// Poll a node's `/statusz` replication view until `want` holds, returning the matching
/// view (or `None` at the deadline). Polling — never sleeping — is what keeps the
/// assertions above judging a *converged* state rather than a race.
/// Poll `/statusz`'s replication block until `want` holds.
///
/// Returns `Err(last_view)` on timeout, where `last_view` is `None` only if the endpoint was
/// never reachable at all. That distinction is load-bearing for the failure message: a
/// swallowed `http_get` error and a floor that never armed are different bugs, and reporting
/// the first as the second sent a reviewer chasing the wrong one under machine load.
async fn poll_replication(
    node: &proc_common::ProcNode,
    want: impl Fn((usize, usize, usize, bool)) -> bool,
) -> Result<(usize, usize, usize, bool), Option<(usize, usize, usize, bool)>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = None;
    loop {
        if let Some(view) = node.statusz_replication().await {
            last = Some(view);
            if want(view) {
                return Ok(view);
            }
        }
        if Instant::now() >= deadline {
            return Err(last);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Render `poll_replication`'s failure so the two cases cannot be confused.
fn replication_poll_failure(last: Option<(usize, usize, usize, bool)>) -> String {
    match last {
        Some(view) => format!("last view: {view:?}"),
        None => "/statusz was NEVER reachable on this node — this is a harness/liveness \
                 failure, not evidence about the write floor"
            .to_string(),
    }
}

/// Publish `payload` at `QoS` 1 to `topic` through `addr` on a FRESH connection per attempt
/// until a PUBACK arrives or `budget` runs out. Returns `(the attempt that was acked,
/// attempts made)`. The payload is the caller's so a delivery oracle can look for exactly
/// what was acked.
///
/// A fresh connection per attempt is required, not tidiness: when the hub withholds the
/// ack it drops the `done` sender and `conn.rs` closes the publisher's connection, so a
/// refused publisher is a disconnected publisher — which is also the client-visible shape
/// an operator reports (see docs/TROUBLESHOOTING.md).
async fn qos1_first_ack_within(
    addr: std::net::SocketAddr,
    client_id: &str,
    topic: &str,
    payload: &[u8],
    budget: Duration,
) -> (Option<u32>, u32) {
    let deadline = Instant::now() + budget;
    let mut attempt = 0u32;
    while Instant::now() < deadline {
        attempt += 1;
        let id = format!("{client_id}-{attempt}");
        if let Some((mut publisher, _)) =
            common::Client::connect_v311_within(addr, &id, true, Duration::from_secs(8)).await
        {
            publisher
                .publish(topic, payload, QoS::AtLeastOnce, Some(9), vec![])
                .await;
            let attempt_end = (Instant::now() + Duration::from_secs(10)).min(deadline);
            loop {
                let left = attempt_end.saturating_duration_since(Instant::now());
                match publisher.recv_bounded(left).await {
                    common::Recv::Packet(Packet::PubAck(a)) if a.pkid == 9 => {
                        return (Some(attempt), attempt)
                    }
                    common::Recv::Packet(_) => {}
                    common::Recv::Quiet | common::Recv::Closed => break,
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    (None, attempt)
}

/// The harness must not draw spawned-node ports from the kernel's ephemeral
/// range. It used to, and the cost was a red CI run: a node lost the race to an
/// outbound source port, died with `AddrInUse`, and the cluster "never became
/// ready". Ports below 32768 are never auto-assigned, so probing one free means
/// it stays free until the child binds it.
#[test]
fn spawned_node_ports_come_from_outside_the_ephemeral_range() {
    // 32768 is the low end of the Linux default ip_local_port_range; macOS
    // starts higher still. Anything below it is ours alone.
    const EPHEMERAL_LO: u16 = 32_768;
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..200 {
        for port in [proc_common::free_tcp_port(), proc_common::free_udp_port()] {
            assert!(
                port < EPHEMERAL_LO,
                "port {port} is inside the ephemeral range — the kernel can hand it \
                 to an outbound connection between our probe and the child's bind"
            );
            assert!(seen.insert(port), "port {port} handed out twice");
        }
    }
}
