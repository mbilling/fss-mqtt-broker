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
//! ## Known exhibits (waived, tracked, red under `--ignored`)
//!
//! On its first sweep this harness found two **real defects** in the replication
//! glue, recorded as exhibits ② and ③ in the delivery doc's ledger:
//!
//! - **② Adopted-orphan gap loss** (`acked-durability`): a takeover merge adopts
//!   a single-replica orphan into the committed run and the new owner builds on
//!   top of it without re-replicating it; a later takeover whose read quorum
//!   misses the orphan-holder hits a gap at that offset, and the contiguity rule
//!   discards the **acked tail above it**. Fix: 0042-T6 (takeover re-commit).
//! - **③ Offset-reuse divergence** (`acked-integrity`): after `NoQuorum` the next
//!   (different) record reuses the offset; a replica that stored the failed
//!   attempt and missed the reuse holds different bytes at an acked offset, and
//!   [`merge_replica_logs`] resolves the conflict by read order — the stale
//!   record can win. The same unfenced adoption has a **retained face** the
//!   ledger cannot see: a deposed owner's never-acked writes at *higher* offsets
//!   can be adopted above a newer owner's acked write, regressing the recovered
//!   retained value/token behind an acked one (entries carry no epoch, so the
//!   merge cannot apply the log-matching rule and truncate the stale tail).
//!   Fix: 0042-T7 (epoch-tagged entries + conflict/tail resolution).
//!
//! The sweeping tests waive exactly these two violation classes (loudly, by
//! count) so every *other* invariant stays enforced on every push;
//! [`replication_catalog_holds_with_no_waivers`] runs the same schedules
//! unwaived and is `#[ignore]`d until T6/T7 remove the defects — un-ignoring it
//! is those tasks' acceptance.

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

/// The violation classes covered by the recorded exhibits ② and ③ (module docs).
/// Removed by 0042-T6/T7; nothing else is ever waived.
const WAIVED_KNOWN_EXHIBITS: &[&str] = &["acked-durability", "acked-integrity"];

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

/// Split off the violations covered by the recorded exhibits (when waiving);
/// everything else must hold. Returns `(waived_count, remaining)`.
fn split_waived(violations: Vec<Violation>, waive: bool) -> (usize, Vec<Violation>) {
    if !waive {
        return (0, violations);
    }
    let (waived, remaining): (Vec<_>, Vec<_>) = violations
        .into_iter()
        .partition(|v| WAIVED_KNOWN_EXHIBITS.contains(&v.invariant));
    (waived.len(), remaining)
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
    /// Exhibit-waived violations seen so far (counted loudly at scenario end).
    waived: usize,
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
            waived: 0,
        }
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
    /// with different bytes** — the `ClusterLog` behavior under a failed quorum.
    /// Returns the acked offset, if acked.
    fn owner_append(&mut self, epoch: u64, key: &str) -> Option<u64> {
        let offset = *self.next_offset.entry(key.to_string()).or_insert(1);
        self.attempt += 1;
        let record = format!("e{epoch}-{key}-o{offset}-a{}", self.attempt).into_bytes();
        let op = ReplOp::Append {
            key: key.to_string(),
            offset,
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
    /// the ledger against it, and hand the recovered log to the next owner (which
    /// continues after the recovered high-water, as `ClusterLog::recovered` does).
    fn takeover_recover(
        &mut self,
        key: &str,
        waive: bool,
    ) -> (Vec<mqtt_storage::repl::LogEntry>, Vec<Violation>) {
        let first = self.rng.pick(3);
        let second = self.rng.pick(2);
        let second = if second >= first { second + 1 } else { second };
        let read = |r: &ReplicaState| ReplicaRead {
            watermark: r.watermark(key),
            entries: r.entries(key),
        };
        let merged =
            merge_replica_logs(&[read(&self.replicas[first]), read(&self.replicas[second])]);
        let (waived, violations) = split_waived(self.ledger.verify_recovered(key, &merged), waive);
        self.waived += waived;
        self.next_offset
            .insert(key.to_string(), merged.last().map_or(1, |e| e.offset + 1));
        (merged, violations)
    }

    /// Verify the whole catalog: per-replica fencing over every decision the
    /// plane carried, and the ledger against every ordered read-quorum choice
    /// (all six), so a divergence visible only under one read order cannot hide.
    fn verify_all(&mut self, key: &str, waive: bool) -> Vec<Violation> {
        let mut all: Vec<Violation> = self.fences.iter().flat_map(FenceLog::verify).collect();
        for first in 0..3 {
            for second in 0..3 {
                if first == second {
                    continue;
                }
                let read = |r: &ReplicaState| ReplicaRead {
                    watermark: r.watermark(key),
                    entries: r.entries(key),
                };
                let merged = merge_replica_logs(&[
                    read(&self.replicas[first]),
                    read(&self.replicas[second]),
                ]);
                all.extend(self.ledger.verify_recovered(key, &merged));
            }
        }
        let (waived, remaining) = split_waived(all, waive);
        self.waived += waived;
        remaining
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
/// read quorum in a seeded order. The catalog is checked at every takeover and,
/// over every read order, at the end. Returns the waived-exhibit count.
fn run_replication_schedule(seed: u64, keys: &[&str], waive: bool) -> usize {
    let mut sim = ReplSim::new(seed);
    for generation in 1..=4u64 {
        let epoch = generation;
        if generation > 1 {
            for key in keys {
                let (_, violations) = sim.takeover_recover(key, waive);
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
        let violations = sim.verify_all(key, waive);
        assert_seed_holds(seed, "end-of-schedule catalog", &violations);
    }
    sim.waived
}

/// Fencing + acked durability across seeded takeover schedules, with the two
/// recorded exhibits waived (module docs) — every other invariant enforced on
/// every seed.
#[test]
fn fencing_and_acked_durability_hold_across_seeded_takeover_schedules() {
    let mut waived = 0;
    for seed in seeds() {
        waived += run_replication_schedule(seed, &["q/sim-a", "q/sim-b"], true);
    }
    // Loud, not silent: the waiver's footprint is visible under --nocapture and
    // shrinks to zero when 0042-T6/T7 land.
    eprintln!("durable_sim: waived {waived} known-exhibit violations (exhibits 2-3)");
}

/// The same schedules with **no waivers**: red today, by design — exhibits ② and
/// ③ are real. 0042-T6/T7's acceptance is un-ignoring this test.
#[test]
#[ignore = "exhibits 2-3 (0042-T6/T7): adopted-orphan gap loss + offset-reuse divergence"]
fn replication_catalog_holds_with_no_waivers() {
    for seed in seeds() {
        run_replication_schedule(seed, &["q/sim-a", "q/sim-b"], false);
        run_retained_schedule(seed, false);
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
/// recovered token must not be behind the last acked one (the exhibit test's
/// claim, generalized over schedules; skipped for a recovery the waived exhibits
/// already flagged as lossy, since a lost tail legally lowers the recovered
/// high-water).
#[test]
fn retained_tokens_never_regress_across_seeded_takeover_schedules() {
    let mut waived_total = 0;
    for seed in seeds() {
        waived_total += run_retained_schedule(seed, true);
    }
    eprintln!("durable_sim(retained): waived {waived_total} known-exhibit violations");
}

/// One full seeded retained schedule (see the test above). Returns the count of
/// waived known-exhibit events (ledger shapes plus recovered-token regressions —
/// exhibit ③'s retained face).
fn run_retained_schedule(seed: u64, waive: bool) -> usize {
    let topic_key = "r/sim-topic";
    let mut sim = ReplSim::new(seed);
    let mut tokens = TokenLog::new();
    let mut last_acked: Option<(u64, u64)> = None;

    for generation in 1..=4u64 {
        let epoch = generation;
        if generation > 1 {
            let waived_before = sim.waived;
            let (merged, violations) = sim.takeover_recover(topic_key, waive);
            assert_seed_holds(seed, "retained takeover recovery", &violations);
            let lossy = sim.waived > waived_before;
            if let (Some(entry), Some(acked), false) = (merged.last(), last_acked, lossy) {
                let recovered = decode_token(&entry.record);
                if recovered < acked {
                    // Exhibit ③, retained face: a stale-epoch orphan adopted
                    // above the acked write regressed the recovered value.
                    assert!(
                        waive,
                        "seed {seed}: takeover recovered token {recovered:?} behind the \
                         acked {acked:?} (re-run with REPRO_SEED = Some({seed}))"
                    );
                    sim.waived += 1;
                }
            }
        }
        let sets = sim.rng.range(1, 5);
        for _ in 0..sets {
            let offset = *sim.next_offset.entry(topic_key.to_string()).or_insert(1);
            let record = encode_token(epoch, offset);
            let op = ReplOp::Append {
                key: topic_key.to_string(),
                offset,
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
    let violations = sim.verify_all(topic_key, waive);
    assert_seed_holds(seed, "end-of-schedule catalog", &violations);
    sim.waived
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
