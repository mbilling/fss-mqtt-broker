//! Deterministic, seed-reproducible **simulation of the durable plane's pure core**
//! ([ADR 0042](../../../docs/adr/0042-durable-plane-stress-harness.md) T2).
//!
//! ADR 0024 built this discipline for SWIM (`swim_sim`) and recorded the
//! lease/replication layer as the deferred extension; this is that extension, at
//! the layer where it needs no seam at all: [`ReplicaState`] is a pure apply
//! function, [`merge_replica_logs`] a pure merge, [`LeaseMap`] a pure Raft state
//! machine, and [`Placement`] a pure HRW ring. A seeded schedule generator (the
//! same hand-rolled xorshift as `swim_sim`; no new dependencies) drives them
//! through the fault vocabulary the wall-clock tests can only script one point of:
//! per-replica delivery fates (**counted accept**, **accept with the ack lost**,
//! **drop**, **defer** — a deferred op arrives arbitrarily later, possibly at a
//! stale epoch), offset reuse after failed quorum, takeover after takeover with
//! seeded read-quorum choices and read *orders*.
//!
//! The oracle is the ADR 0042 T1 invariant catalog (`mqtt_cluster::invariants`) —
//! scenarios choose what to do, never what must hold. Every scenario sweeps many
//! seeds; a violation panics with the offending seed, which reruns the identical
//! schedule (set [`REPRO_SEED`] to focus it).
//!
//! ## Exhibits found and fixed (the full catalog is enforced, no waivers)
//!
//! On its first sweep this harness found two **real defects** in the replication
//! glue, recorded as exhibits ② and ③ in the delivery doc's ledger — both fixed:
//!
//! - **② Adopted-orphan gap loss** — fixed by 0042-T6 (takeover re-commit: the
//!   new owner re-replicates the recovered base to a write quorum at its epoch
//!   before serving, `ClusterLog::recommit_key`; plus truncation-floor offset
//!   continuation). The sim's takeover models both.
//! - **③ Offset-reuse divergence** — fixed by 0042-T7 (epoch-tagged entries):
//!   every entry carries its writing `(epoch, seq)`; a replica keeps the higher
//!   version of an offset, and the recovery merge resolves same-offset conflicts
//!   by tag and truncates a tail whose tag regresses (log matching) — so a
//!   failed attempt whose offset was reused can never shadow the acked record,
//!   and a deposed owner's orphans can never regress a recovered retained value
//!   behind an acked one. The sim allocates seqs exactly as `ClusterLog` does
//!   (every attempt bumps; re-commit re-tags 1..=n at the new epoch).
//!
//! These sweeps enforce the **entire** T1 catalog on every seed, unwaived — the
//! state the ADR calls done. A future violation here is a new defect, not a
//! known exhibit.

use mqtt_cluster::cluster_log::{merge_replica_logs, ReplOp, ReplicaRead, ReplicaState};
use mqtt_cluster::invariants::{AckLedger, FenceLog, LeaseLog, TokenLog, Violation};
use mqtt_cluster::lease_raft::{LeaseMap, LeaseRequest};
use mqtt_cluster::placement::{group_of_key, Placement, NUM_GROUPS};
use mqtt_cluster::swim::MemberState;
use mqtt_cluster::NodeId;
use std::collections::BTreeMap;

/// Set to `Some(seed)` to run a single seed (e.g. to reproduce a reported failure).
const REPRO_SEED: Option<u64> = None;

/// How many seeds each scenario sweeps (the pure core is cheap; a full sweep runs
/// in seconds). `REPRO_SEED` narrows it to one.
const SEED_SWEEP: u64 = 1000;

fn seeds() -> Vec<u64> {
    match REPRO_SEED {
        Some(s) => vec![s],
        None => (0..SEED_SWEEP).collect(),
    }
}

/// A seeded xorshift64 RNG — deterministic, matching `swim_sim` (no `rand`).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng((seed ^ 0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// `true` with probability `num/den`.
    fn chance(&mut self, num: u64, den: u64) -> bool {
        den == 0 || self.next() % den < num
    }
    /// A value in `[lo, hi)` (or `lo` if the range is empty).
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            lo
        } else {
            lo + self.next() % (hi - lo)
        }
    }
    fn pick(&mut self, len: usize) -> usize {
        usize::try_from(self.range(0, len as u64)).unwrap()
    }
}

/// Panic with the catalog's violations, tagged with the reproducing seed.
fn assert_seed_holds(seed: u64, what: &str, violations: &[Violation]) {
    assert!(
        violations.is_empty(),
        "seed {seed}: {what} violated (re-run with REPRO_SEED = Some({seed})):\n{}",
        violations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ---------------------------------------------------------------------------
// The replication-plane simulator: 3 replicas, an adversarial delivery layer,
// owner generations with seeded takeover merges.
// ---------------------------------------------------------------------------

/// The write quorum for a replica set of 3 (matches `ClusterLog`).
const QUORUM: usize = 2;

/// One op deferred in flight: it will reach `replica` later — possibly after the
/// epoch has advanced, which is exactly the stale-delivery race.
struct Deferred {
    replica: usize,
    epoch: u64,
    op: ReplOp,
}

/// The simulated replication plane: three replicas behind an adversarial
/// delivery layer, with the T1 catalog observing every decision and every ack.
struct ReplSim {
    rng: Rng,
    replicas: Vec<ReplicaState>,
    deferred: Vec<Deferred>,
    /// One fence log **per replica**: fencing is a per-replica invariant (a
    /// replica that slept through an epoch may legitimately accept it late; what
    /// it must never do is accept an epoch below one *it* acknowledged).
    fences: Vec<FenceLog>,
    ledger: AckLedger,
    /// Per key: the next offset the current owner will assign (a failed-quorum
    /// append leaves it unchanged — the next append **reuses the offset**, as
    /// `ClusterLog` does).
    next_offset: BTreeMap<String, u64>,
    /// Per key: the highest acked offset (bounds truncation choices).
    acked_high: BTreeMap<String, u64>,
    /// Distinguishes the bytes of successive attempts (a reused offset must carry
    /// *different* bytes to make silent replacement detectable).
    attempt: u64,
    /// Per key: the owner's write-attempt seq counter, allocated exactly as
    /// `ClusterLog` does — bumped on EVERY attempt (failed ones included), reset
    /// to the re-committed entry count at takeover (ADR 0042 T7).
    seq: BTreeMap<String, u64>,
}

impl ReplSim {
    fn new(seed: u64) -> Self {
        Self {
            rng: Rng::new(seed),
            replicas: (0..3).map(|_| ReplicaState::new()).collect(),
            deferred: Vec::new(),
            fences: (0..3).map(|_| FenceLog::new()).collect(),
            ledger: AckLedger::new(),
            next_offset: BTreeMap::new(),
            acked_high: BTreeMap::new(),
            attempt: 0,
            seq: BTreeMap::new(),
        }
    }

    /// Allocate the next write-attempt seq for `key` (every attempt bumps).
    fn next_seq(&mut self, key: &str) -> u64 {
        let c = self.seq.entry(key.to_string()).or_insert(0);
        *c += 1;
        *c
    }

    /// Deliver `op` at `epoch` to one replica right now, recording the decision
    /// in that replica's fence log.
    fn deliver(&mut self, replica: usize, epoch: u64, op: &ReplOp) -> bool {
        let accepted = self.replicas[replica].apply(epoch, op);
        self.fences[replica].observe(group_of_key(op_key(op)), epoch, accepted);
        accepted
    }

    /// Fan `op` out to all replicas with a seeded fate per delivery: counted
    /// accept, accept-with-ack-lost (the replica applied; the owner never heard),
    /// drop, or defer (arrives later, possibly stale). Returns the counted accepts.
    fn send(&mut self, epoch: u64, op: &ReplOp) -> usize {
        let mut counted = 0;
        for replica in 0..self.replicas.len() {
            match self.rng.range(0, 100) {
                // 55%: delivered, ack heard.
                0..=54 => {
                    if self.deliver(replica, epoch, op) {
                        counted += 1;
                    }
                }
                // 15%: delivered and applied, but the ack is lost in transit.
                55..=69 => {
                    self.deliver(replica, epoch, op);
                }
                // 15%: dropped entirely.
                70..=84 => {}
                // 15%: deferred — arrives at a later seeded point.
                _ => self.deferred.push(Deferred {
                    replica,
                    epoch,
                    op: op.clone(),
                }),
            }
        }
        counted
    }

    /// Release a few deferred deliveries (seeded), interleaving stale arrivals
    /// with current traffic.
    fn release_some_deferred(&mut self) {
        while !self.deferred.is_empty() && self.rng.chance(1, 3) {
            let i = self.rng.pick(self.deferred.len());
            let d = self.deferred.swap_remove(i);
            self.deliver(d.replica, d.epoch, &d.op);
        }
    }

    /// Drain every deferred delivery in seeded order (end of schedule).
    fn drain_deferred(&mut self) {
        while !self.deferred.is_empty() {
            let i = self.rng.pick(self.deferred.len());
            let d = self.deferred.swap_remove(i);
            self.deliver(d.replica, d.epoch, &d.op);
        }
    }

    /// One owner append to `key` at `epoch`: acked (and the offset advanced) only
    /// on a counted write quorum; otherwise the next append **reuses the offset
    /// with different bytes** — the `ClusterLog` behavior under a failed quorum
    /// (the next attempt's higher seq supersedes the failed one, ADR 0042 T7).
    /// Returns the acked offset, if acked.
    fn owner_append(&mut self, epoch: u64, key: &str) -> Option<u64> {
        let offset = *self.next_offset.entry(key.to_string()).or_insert(1);
        self.attempt += 1;
        let seq = self.next_seq(key);
        let record = format!("e{epoch}-{key}-o{offset}-a{}", self.attempt).into_bytes();
        let op = ReplOp::Append {
            key: key.to_string(),
            offset,
            seq,
            record: record.clone(),
        };
        if self.send(epoch, &op) >= QUORUM {
            self.ledger.ack_append(key, offset, &record);
            self.acked_high
                .entry(key.to_string())
                .and_modify(|h| *h = (*h).max(offset))
                .or_insert(offset);
            self.next_offset.insert(key.to_string(), offset + 1);
            Some(offset)
        } else {
            None
        }
    }

    /// One owner truncation of `key` up to a seeded acked offset. Issuing it
    /// discharges the covered promises regardless of outcome (a truncation is
    /// only issued for client-acked data); reaching quorum also sets the
    /// no-resurrection floor.
    fn owner_truncate(&mut self, epoch: u64, key: &str) {
        let Some(&high) = self.acked_high.get(key) else {
            return;
        };
        let up_to = self.rng.range(1, high + 1);
        self.ledger.note_truncate_issued(key, up_to);
        let op = ReplOp::Truncate {
            key: key.to_string(),
            up_to,
        };
        if self.send(epoch, &op) >= QUORUM {
            self.ledger.ack_truncate(key, up_to);
        }
    }

    /// A takeover's recovery read: merge a seeded 2-replica quorum in a seeded
    /// *order* (the merge must not depend on which replica is read first), verify
    /// the ledger against it, **re-commit the recovered base to a write quorum at
    /// the new owner's `epoch`** (the 0042-T6 fix for exhibit ② — retried through
    /// the adversarial fates until quorum, as the real recovery retries on
    /// `NoQuorum`; entries re-tagged with seqs 1..=n, as `recommit_key` does),
    /// and hand the recovered log to the next owner (which continues after the
    /// recovered high-water, as `ClusterLog::recovered` does).
    fn takeover_recover(
        &mut self,
        key: &str,
        epoch: u64,
    ) -> (Vec<mqtt_storage::repl::LogEntry>, Vec<Violation>) {
        let first = self.rng.pick(3);
        let second = self.rng.pick(2);
        let second = if second >= first { second + 1 } else { second };
        let read = |r: &ReplicaState| ReplicaRead {
            watermark: r.watermark(key),
            entries: r.epoch_entries(key),
        };
        let reads = [read(&self.replicas[first]), read(&self.replicas[second])];
        let floor = reads.iter().map(|r| r.watermark).max().unwrap_or(0);
        let merged = merge_replica_logs(&reads);
        let violations = self.ledger.verify_recovered(key, &merged);
        // Re-commit at the new epoch, re-tagged with seqs 1..=n; the owner's seq
        // counter continues above n (the recommit_key/seed_key convention).
        for (i, entry) in merged.iter().enumerate() {
            let op = ReplOp::Append {
                key: key.to_string(),
                offset: entry.offset,
                seq: (i as u64) + 1,
                record: entry.record.clone(),
            };
            while self.send(epoch, &op) < QUORUM {}
        }
        self.seq.insert(key.to_string(), merged.len() as u64);
        // The offset space continues above BOTH the recovered high-water and the
        // reads' truncation floor (exhibit ②'s second face, also fixed by T6): a
        // fully-truncated key merges empty, but restarting at offset 1 would put
        // new acked writes below some replica's durable watermark — silently
        // dropped by any later merge that reads it.
        self.next_offset.insert(
            key.to_string(),
            merged.last().map_or(1, |e| e.offset + 1).max(floor + 1),
        );
        (merged, violations)
    }

    /// Verify the whole catalog: per-replica fencing over every decision the
    /// plane carried, and the ledger against every ordered read-quorum choice
    /// (all six), so a divergence visible only under one read order cannot hide.
    fn verify_all(&self, key: &str) -> Vec<Violation> {
        let mut all: Vec<Violation> = self.fences.iter().flat_map(FenceLog::verify).collect();
        for first in 0..3 {
            for second in 0..3 {
                if first == second {
                    continue;
                }
                let read = |r: &ReplicaState| ReplicaRead {
                    watermark: r.watermark(key),
                    entries: r.epoch_entries(key),
                };
                let merged = merge_replica_logs(&[
                    read(&self.replicas[first]),
                    read(&self.replicas[second]),
                ]);
                all.extend(self.ledger.verify_recovered(key, &merged));
            }
        }
        all
    }
}

/// The logical key an op addresses (mirror of the module-private helper).
fn op_key(op: &ReplOp) -> &str {
    match op {
        ReplOp::Append { key, .. } | ReplOp::Truncate { key, .. } | ReplOp::Remove { key } => key,
    }
}

/// One full seeded replication schedule over `keys`: four owner generations at
/// increasing epochs append, truncate, and lose deliveries; deferred (stale) ops
/// land throughout and drain at the end; each takeover recovers from a seeded
/// read quorum in a seeded order, re-committing its base. The full catalog is
/// checked at every takeover and, over every read order, at the end.
fn run_replication_schedule(seed: u64, keys: &[&str]) {
    let mut sim = ReplSim::new(seed);
    for generation in 1..=4u64 {
        let epoch = generation;
        if generation > 1 {
            for key in keys {
                let (_, violations) = sim.takeover_recover(key, epoch);
                assert_seed_holds(seed, "takeover recovery", &violations);
            }
        }
        let steps = sim.rng.range(3, 9);
        for _ in 0..steps {
            let key = keys[sim.rng.pick(keys.len())];
            if sim.rng.chance(1, 6) {
                sim.owner_truncate(epoch, key);
            } else {
                sim.owner_append(epoch, key);
            }
            sim.release_some_deferred();
        }
    }
    sim.drain_deferred();
    for key in keys {
        assert_seed_holds(seed, "end-of-schedule catalog", &sim.verify_all(key));
    }
}

/// Fencing + acked durability + integrity across seeded takeover schedules —
/// the full T1 catalog, no waivers (exhibits ② and ③ are fixed; module docs).
#[test]
fn fencing_and_acked_durability_hold_across_seeded_takeover_schedules() {
    for seed in seeds() {
        run_replication_schedule(seed, &["q/sim-a", "q/sim-b"]);
    }
}

/// Lease minting under seeded assignment schedules: epochs stay strictly
/// increasing across groups (the shared-counter fence contract), the map agrees
/// with the mint history at every step, and a replica replaying the identical
/// committed sequence reaches the identical state (the Raft state-machine
/// determinism requirement).
#[test]
fn lease_minting_is_monotonic_and_replay_is_deterministic() {
    for seed in seeds() {
        let mut rng = Rng::new(seed);
        let mut map = LeaseMap::new();
        let mut log = LeaseLog::new();
        let mut committed: Vec<LeaseRequest> = Vec::new();

        let steps = rng.range(10, 40);
        for _ in 0..steps {
            // Batches commit the same per-assignment sequence a single Assign
            // does, so decompose them for observation and replay alike.
            let assignments: Vec<(u64, u64)> = if rng.chance(1, 4) {
                (0..rng.range(1, 5))
                    .map(|_| (rng.range(0, 8), rng.range(0, 5)))
                    .collect()
            } else {
                vec![(rng.range(0, 8), rng.range(0, 5))]
            };
            for (group, node) in assignments {
                let req = LeaseRequest::Assign { group, node };
                log.observe(map.apply(&req).expect("an assignment mints a lease"));
                committed.push(req);
            }
            assert_seed_holds(seed, "lease epoch monotonicity", &log.verify());
            assert_seed_holds(seed, "lease map agreement", &log.verify_map(&map));
        }

        // Replica replay: the same committed sequence, applied fresh, reaches the
        // same table — checked through the same catalog lens.
        let mut replica = LeaseMap::new();
        for req in &committed {
            replica.apply(req);
        }
        assert_seed_holds(
            seed,
            "replayed lease map agreement",
            &log.verify_map(&replica),
        );
        assert_eq!(
            replica.high_epoch(),
            map.high_epoch(),
            "seed {seed}: replay diverged on the epoch counter \
             (re-run with REPRO_SEED = Some({seed}))"
        );
    }
}

/// Retained convergence tokens across seeded takeover schedules: retained sets
/// are appends on an `r/` key whose records carry their `(epoch, offset)` token;
/// every acked set's token feeds one [`TokenLog`] across all four owner
/// generations — strictly increasing per topic or it panics — and each takeover's
/// recovered token must never be behind the last acked one (the exhibit-①
/// scenario test's claim, generalized over schedules; guaranteed by T7's
/// log-matching merge, which truncates a deposed owner's stale orphan tail).
#[test]
fn retained_tokens_never_regress_across_seeded_takeover_schedules() {
    let topic_key = "r/sim-topic";
    for seed in seeds() {
        let mut sim = ReplSim::new(seed);
        let mut tokens = TokenLog::new();
        let mut last_acked: Option<(u64, u64)> = None;

        for generation in 1..=4u64 {
            let epoch = generation;
            if generation > 1 {
                let (merged, violations) = sim.takeover_recover(topic_key, epoch);
                assert_seed_holds(seed, "retained takeover recovery", &violations);
                if let (Some(entry), Some(acked)) = (merged.last(), last_acked) {
                    let recovered = decode_token(&entry.record);
                    assert!(
                        recovered >= acked,
                        "seed {seed}: takeover recovered token {recovered:?} behind the \
                         acked {acked:?} (re-run with REPRO_SEED = Some({seed}))"
                    );
                }
            }
            let sets = sim.rng.range(1, 5);
            for _ in 0..sets {
                let offset = *sim.next_offset.entry(topic_key.to_string()).or_insert(1);
                let seq = sim.next_seq(topic_key);
                let record = encode_token(epoch, offset);
                let op = ReplOp::Append {
                    key: topic_key.to_string(),
                    offset,
                    seq,
                    record: record.clone(),
                };
                if sim.send(epoch, &op) >= QUORUM {
                    sim.ledger.ack_append(topic_key, offset, &record);
                    sim.acked_high
                        .entry(topic_key.to_string())
                        .and_modify(|h| *h = (*h).max(offset))
                        .or_insert(offset);
                    sim.next_offset.insert(topic_key.to_string(), offset + 1);
                    tokens.observe_applied(topic_key, (epoch, offset));
                    last_acked = Some((epoch, offset));
                }
                sim.release_some_deferred();
            }
        }
        sim.drain_deferred();
        assert_seed_holds(seed, "retained token monotonicity", &tokens.verify());
        assert_seed_holds(seed, "end-of-schedule catalog", &sim.verify_all(topic_key));
    }
}

/// Encode a retained record carrying its convergence token (the real retained
/// rows carry theirs inside the record bytes; the sim mirrors that shape).
fn encode_token(epoch: u64, offset: u64) -> Vec<u8> {
    format!("{epoch}:{offset}").into_bytes()
}

fn decode_token(record: &[u8]) -> (u64, u64) {
    let s = std::str::from_utf8(record).expect("sim records are utf8");
    let (e, o) = s.split_once(':').expect("sim records carry a token");
    (e.parse().unwrap(), o.parse().unwrap())
}

/// HRW placement under seeded membership: nodes that observed the same members in
/// different orders agree on every group's owner and replica set, and killing one
/// node moves only the groups it owned (minimal disruption — the property that
/// keeps a member change from re-homing the whole keyspace).
#[test]
fn placement_agrees_across_observation_orders_and_disrupts_minimally() {
    for seed in seeds() {
        let mut rng = Rng::new(seed);
        let n = usize::try_from(rng.range(3, 8)).unwrap();
        let ids: Vec<NodeId> = (0..n).map(|i| NodeId(format!("sim-n{i}"))).collect();

        // Two nodes build their rings observing the same membership in different
        // seeded orders.
        let build = |local: usize, rng: &mut Rng| {
            let mut p = Placement::new(ids[local].clone(), 3);
            let mut order: Vec<usize> = (0..n).filter(|i| *i != local).collect();
            for i in (1..order.len()).rev() {
                let j = rng.pick(i + 1);
                order.swap(i, j);
            }
            for i in order {
                p.observe(
                    &ids[i],
                    MemberState::Alive,
                    &format!("{}:7", ids[i].0),
                    None,
                );
            }
            p
        };
        let mut a = build(0, &mut rng);
        let mut b = build(1, &mut rng);

        let owners =
            |p: &Placement| -> Vec<NodeId> { (0..NUM_GROUPS).map(|g| p.group_owner(g)).collect() };
        let before = owners(&a);
        assert_eq!(
            before,
            owners(&b),
            "seed {seed}: two observation orders disagree on group owners \
             (re-run with REPRO_SEED = Some({seed}))"
        );
        for g in 0..NUM_GROUPS {
            let set = a.group_replica_set(g);
            assert!(
                set.contains(&a.group_owner(g)) && set == b.group_replica_set(g),
                "seed {seed}: group {g} replica set broken or divergent \
                 (re-run with REPRO_SEED = Some({seed}))"
            );
        }

        // Kill one node that is neither ring's local: only its groups move.
        let victim = usize::try_from(rng.range(2, n as u64)).unwrap();
        a.observe(&ids[victim], MemberState::Dead, "", None);
        b.observe(&ids[victim], MemberState::Dead, "", None);
        let after = owners(&a);
        assert_eq!(
            after,
            owners(&b),
            "seed {seed}: rings diverged after a death \
             (re-run with REPRO_SEED = Some({seed}))"
        );
        for g in 0..usize::try_from(NUM_GROUPS).unwrap() {
            if before[g] == ids[victim] {
                assert_ne!(
                    after[g], ids[victim],
                    "seed {seed}: group {g} still owned by a dead node \
                     (re-run with REPRO_SEED = Some({seed}))"
                );
            } else {
                assert_eq!(
                    after[g], before[g],
                    "seed {seed}: group {g} moved although its owner survived \
                     (re-run with REPRO_SEED = Some({seed}))"
                );
            }
        }
    }
}
