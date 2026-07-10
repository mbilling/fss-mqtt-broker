//! The durable plane's invariant catalog, as executable checkers
//! ([ADR 0042](../../../docs/adr/0042-durable-plane-stress-harness.md) T1).
//!
//! One place states what the durable plane guarantees; scenario tests and the
//! harness layers (ADR 0042 T2/T3) choose *what to do* and assert through these
//! checkers, never re-deriving *what must hold*. Each checker either observes
//! events as they happen (the ledger types) or checks a state snapshot (the pure
//! functions); all of them report [`Violation`]s — empty means the invariant holds
//! — and [`assert_holds`] panics with every violation's detail for test use.
//!
//! The catalog:
//!
//! - **Acked durability** ([`AckLedger`], ADR 0006/0018): everything acknowledged
//!   to a caller — a quorum-acked append, an acked truncation — is present, byte
//!   identical, in any later recovery of that log; nothing at or below an acked
//!   truncation ever resurrects.
//! - **Epoch fencing** ([`FenceLog`], ADR 0006 §1): once a replica acknowledges an
//!   op at epoch E for a group, it never accepts an op at an epoch `< E` for that
//!   group. Refusals are always allowed (a persist failure refuses a current
//!   epoch); *acceptance of a stale epoch* is the violation.
//! - **Lease monotonicity** ([`LeaseLog`], ADR 0006/0021): minted lease epochs are
//!   strictly increasing (one shared counter — this is what makes an epoch a fence
//!   token), and the [`LeaseMap`] agrees with the mint history: one holder per
//!   group, the latest assignment.
//! - **Retained token monotonicity + convergence** ([`TokenLog`],
//!   [`check_retained_convergence`], ADR 0037 §3): a retained cache applies
//!   strictly increasing `(epoch, offset)` tokens per topic — a stale value never
//!   overwrites a newer one, a cleared topic never resurrects — and after a heal
//!   every node's cache is identical.
//! - **Session singularity** ([`check_session_singularity`], ADR 0005/0031): one
//!   client id has at most one live session cluster-wide.
//! - **Recovery honesty** ([`check_recovery_honesty`], ADR 0017): an attach
//!   reports a session present only when the durable truth is present, absent only
//!   when it is absent, and refuses loudly (`Unavailable`) rather than guess —
//!   never a fabricated clean session over a recoverable one.
//! - **Bounded structures** ([`check_bound`], ADR 0041): a bounded queue, table,
//!   or map holds its bound through fault schedules, not only in steady state.

use crate::lease::Epoch;
use crate::lease_raft::{GroupId, LeaseMap, LeaseResponse, RaftNodeId};
use mqtt_storage::repl::LogEntry;
use mqtt_storage::Offset;
use std::collections::BTreeMap;

/// A retained convergence token: the `(epoch, offset)` a value committed at
/// (ADR 0037 §3). Ordered lexicographically, which is exactly the supersedes rule.
pub type Token = (u64, u64);

/// One invariant violation: which invariant, and what was observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The invariant that failed (a stable, greppable name).
    pub invariant: &'static str,
    /// What was observed, with enough context to debug from.
    pub detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.invariant, self.detail)
    }
}

/// Panic with every violation if any were reported. The test-side terminal for
/// every checker: `assert_holds(&ledger.verify_recovered(...))`.
///
/// # Panics
/// If `violations` is non-empty, listing each one.
pub fn assert_holds(violations: &[Violation]) {
    assert!(
        violations.is_empty(),
        "durable-plane invariant violations:\n{}",
        violations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn violation(invariant: &'static str, detail: String) -> Violation {
    Violation { invariant, detail }
}

// ---------------------------------------------------------------------------
// Acked durability
// ---------------------------------------------------------------------------

/// Per-key ledger state: what has been acknowledged for one log.
#[derive(Debug, Default, Clone)]
struct KeyAcks {
    /// Acked appends: offset → record bytes, as acknowledged to the caller.
    appends: BTreeMap<Offset, Vec<u8>>,
    /// The highest acked truncation (`up_to`, inclusive); 0 = none.
    truncated: Offset,
}

/// The acked-durability ledger (ADR 0006/0018): record what the durable plane
/// *acknowledged* — the quorum-acked appends a PUBACK was released on, the acked
/// truncations — then verify a recovery (takeover merge, restart) against it.
///
/// The ledger is the caller's view: only record an append **after** it returned
/// `Ok(offset)`, a truncation after it returned `Ok`. Entries the plane committed
/// but never acknowledged may legitimately appear in a recovery (a below-quorum
/// write that reached one replica); the ledger does not flag them — the contract
/// is about what was *promised*, in both directions.
#[derive(Debug, Default, Clone)]
pub struct AckLedger {
    keys: BTreeMap<String, KeyAcks>,
}

impl AckLedger {
    /// An empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `key`'s append of `record` was acknowledged at `offset`.
    pub fn ack_append(&mut self, key: &str, offset: Offset, record: &[u8]) {
        self.keys
            .entry(key.to_string())
            .or_default()
            .appends
            .insert(offset, record.to_vec());
    }

    /// Record that `key`'s truncation up to `up_to` (inclusive) was acknowledged.
    pub fn ack_truncate(&mut self, key: &str, up_to: Offset) {
        let k = self.keys.entry(key.to_string()).or_default();
        k.truncated = k.truncated.max(up_to);
    }

    /// Record that `key`'s log removal was acknowledged: every promise made so far
    /// for the key is discharged (the caller asked for the data to go away).
    pub fn ack_remove(&mut self, key: &str) {
        self.keys.remove(key);
    }

    /// Verify a recovered copy of `key`'s log — a takeover's
    /// [`merge_replica_logs`](crate::cluster_log::merge_replica_logs) output, a
    /// reopened replica's entries — against everything acknowledged for it.
    ///
    /// Checks: recovered offsets strictly increase (well-formedness); every acked
    /// append above the acked truncation floor is present with identical bytes
    /// (durability); no recovered entry sits at or below the floor (resurrection);
    /// a recovered entry at an acked offset carries the acked bytes (integrity).
    #[must_use]
    pub fn verify_recovered(&self, key: &str, recovered: &[LogEntry]) -> Vec<Violation> {
        let mut out = Vec::new();
        let acks = self.keys.get(key).cloned().unwrap_or_default();

        let mut prev: Option<Offset> = None;
        for e in recovered {
            if let Some(p) = prev {
                if e.offset <= p {
                    out.push(violation(
                        "log-well-formed",
                        format!("{key}: offset {} follows {p} (not increasing)", e.offset),
                    ));
                }
            }
            prev = Some(e.offset);

            if e.offset <= acks.truncated {
                out.push(violation(
                    "no-resurrection",
                    format!(
                        "{key}: offset {} resurfaced at or below the acked truncation {}",
                        e.offset, acks.truncated
                    ),
                ));
            }
            if let Some(acked) = acks.appends.get(&e.offset) {
                if acked != &e.record {
                    out.push(violation(
                        "acked-integrity",
                        format!(
                            "{key}: offset {} recovered with different bytes than were acked",
                            e.offset
                        ),
                    ));
                }
            }
        }

        for (offset, _) in acks.appends.range((acks.truncated + 1)..) {
            if !recovered.iter().any(|e| e.offset == *offset) {
                out.push(violation(
                    "acked-durability",
                    format!("{key}: acked offset {offset} missing from the recovered log"),
                ));
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Epoch fencing
// ---------------------------------------------------------------------------

/// The epoch-fencing observer (ADR 0006 §1): feed it every replica accept/refuse
/// decision **in the order the replica made them**, then [`verify`](Self::verify).
///
/// The invariant is one-directional: once an op at epoch E was *accepted* for a
/// group, accepting a later op at an epoch `< E` for that group is a violation
/// (a deposed holder wrote through the fence). Refusals are always legal — a
/// replica may refuse a current-epoch op (persist failure, unreachable).
#[derive(Debug, Default, Clone)]
pub struct FenceLog {
    /// `(group, epoch, accepted)` in decision order.
    decisions: Vec<(GroupId, Epoch, bool)>,
}

impl FenceLog {
    /// An empty fence log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one replica decision: an op for `group` at `epoch` was `accepted`.
    pub fn observe(&mut self, group: GroupId, epoch: Epoch, accepted: bool) {
        self.decisions.push((group, epoch, accepted));
    }

    /// Verify the fencing invariant over every recorded decision.
    #[must_use]
    pub fn verify(&self) -> Vec<Violation> {
        let mut out = Vec::new();
        let mut high: BTreeMap<GroupId, Epoch> = BTreeMap::new();
        for (group, epoch, accepted) in &self.decisions {
            if !accepted {
                continue;
            }
            let fence = high.entry(*group).or_insert(0);
            if *epoch < *fence {
                out.push(violation(
                    "epoch-fencing",
                    format!(
                        "group {group}: accepted an op at stale epoch {epoch} after \
                         acknowledging epoch {fence}"
                    ),
                ));
            }
            *fence = (*fence).max(*epoch);
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Lease monotonicity
// ---------------------------------------------------------------------------

/// The lease-mint observer (ADR 0006/0021): feed it every [`LeaseResponse`] in
/// commit order, then verify the epoch counter and (optionally) the live map.
#[derive(Debug, Default, Clone)]
pub struct LeaseLog {
    minted: Vec<LeaseResponse>,
}

impl LeaseLog {
    /// An empty lease log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one committed lease assignment.
    pub fn observe(&mut self, response: LeaseResponse) {
        self.minted.push(response);
    }

    /// Verify that minted epochs are **strictly increasing** across all groups —
    /// the one-shared-counter contract that makes an epoch usable as a fence token.
    #[must_use]
    pub fn verify(&self) -> Vec<Violation> {
        let mut out = Vec::new();
        let mut prev: Option<Epoch> = None;
        for r in &self.minted {
            if let Some(p) = prev {
                if r.epoch <= p {
                    out.push(violation(
                        "lease-epoch-monotonic",
                        format!(
                            "group {}: minted epoch {} does not exceed the previous mint {p}",
                            r.group, r.epoch
                        ),
                    ));
                }
            }
            prev = Some(r.epoch);
        }
        out
    }

    /// Verify `map` agrees with the mint history: each group's current lease is
    /// exactly the **last** assignment observed for it (one holder per group), and
    /// the map's high-water epoch is at least every minted epoch.
    #[must_use]
    pub fn verify_map(&self, map: &LeaseMap) -> Vec<Violation> {
        let mut out = Vec::new();
        let mut last: BTreeMap<GroupId, (RaftNodeId, Epoch)> = BTreeMap::new();
        for r in &self.minted {
            last.insert(r.group, (r.holder, r.epoch));
        }
        for (group, (holder, epoch)) in &last {
            match map.get(*group) {
                None => out.push(violation(
                    "lease-map-agreement",
                    format!("group {group}: assigned to node {holder} but the map has no lease"),
                )),
                Some(rec) if rec.holder != *holder || rec.epoch != *epoch => out.push(violation(
                    "lease-map-agreement",
                    format!(
                        "group {group}: map holds (node {}, epoch {}) but the last mint was \
                         (node {holder}, epoch {epoch})",
                        rec.holder, rec.epoch
                    ),
                )),
                Some(_) => {}
            }
        }
        if let Some(max) = self.minted.iter().map(|r| r.epoch).max() {
            if map.high_epoch() < max {
                out.push(violation(
                    "lease-map-agreement",
                    format!(
                        "map high epoch {} is behind the minted epoch {max}",
                        map.high_epoch()
                    ),
                ));
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Retained token monotonicity + convergence
// ---------------------------------------------------------------------------

/// The retained-application observer (ADR 0037 §3): feed it every token a cache
/// **applied** (fan-out, back-fill, recovery — anything that changed the cache),
/// per topic, in application order. Verification is immediate: an application
/// whose token does not strictly exceed the topic's held token is the violation
/// (a stale value overwrote a newer one — the resurrection path).
#[derive(Debug, Default, Clone)]
pub struct TokenLog {
    applied: Vec<(String, Token)>,
}

impl TokenLog {
    /// An empty token log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a cache applied `token` for `topic`.
    pub fn observe_applied(&mut self, topic: &str, token: Token) {
        self.applied.push((topic.to_string(), token));
    }

    /// Verify per-topic strict monotonicity over every recorded application.
    #[must_use]
    pub fn verify(&self) -> Vec<Violation> {
        let mut out = Vec::new();
        let mut held: BTreeMap<&str, Token> = BTreeMap::new();
        for (topic, token) in &self.applied {
            if let Some(h) = held.get(topic.as_str()) {
                if token <= h {
                    out.push(violation(
                        "retained-token-monotonic",
                        format!("{topic}: applied token {token:?} at or below the held {h:?}"),
                    ));
                    continue; // the held token stays the high-water
                }
            }
            held.insert(topic.as_str(), *token);
        }
        out
    }
}

/// One node's retained cache snapshot for convergence checking: per topic, the
/// held token and payload bytes.
pub type RetainedSnapshot = BTreeMap<String, (Token, Vec<u8>)>;

/// Verify retained convergence (ADR 0037): after a heal + quiesce, every node's
/// cache must be identical — same topics, same tokens, same payloads. `caches` is
/// `(node label, snapshot)`; the first snapshot is the reference.
#[must_use]
pub fn check_retained_convergence(caches: &[(&str, RetainedSnapshot)]) -> Vec<Violation> {
    let mut out = Vec::new();
    let Some(((first_node, reference), rest)) = caches.split_first() else {
        return out;
    };
    for (node, cache) in rest {
        for (topic, held) in reference {
            match cache.get(topic) {
                None => out.push(violation(
                    "retained-convergence",
                    format!("{topic}: present on {first_node}, missing on {node}"),
                )),
                Some(other) if other != held => out.push(violation(
                    "retained-convergence",
                    format!(
                        "{topic}: {first_node} holds token {:?}, {node} holds {:?} \
                         (or differing payloads)",
                        held.0, other.0
                    ),
                )),
                Some(_) => {}
            }
        }
        for topic in cache.keys() {
            if !reference.contains_key(topic) {
                out.push(violation(
                    "retained-convergence",
                    format!("{topic}: present on {node}, missing on {first_node}"),
                ));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Session singularity
// ---------------------------------------------------------------------------

/// Verify session singularity (ADR 0005/0031): across `live` — one `(node label,
/// client id)` pair per **live** session — no client id appears more than once
/// (neither on two nodes nor twice on one).
#[must_use]
pub fn check_session_singularity(live: &[(&str, &str)]) -> Vec<Violation> {
    let mut out = Vec::new();
    let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
    for (node, client) in live {
        if let Some(first) = seen.get(client) {
            out.push(violation(
                "session-singularity",
                format!("client {client:?} live on {first} and {node} at once"),
            ));
        } else {
            seen.insert(client, node);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Recovery honesty
// ---------------------------------------------------------------------------

/// The durable truth about a session at the moment of an attach, as the scenario
/// knows it (it created the state, so it knows).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableTruth {
    /// A recoverable session exists for the client.
    Present,
    /// No session exists for the client.
    Absent,
    /// The scenario cannot say (e.g. a fault window made the outcome racy).
    Unknown,
}

/// What the attach reported to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachReport {
    /// CONNACK sent with this `session present` flag.
    SessionPresent(bool),
    /// The attach refused loudly (`Unavailable`) rather than answer.
    RefusedUnavailable,
}

/// Verify recovery honesty (ADR 0017): the attach never fabricates a clean
/// session over a recoverable one, never invents a session that does not exist,
/// and may always refuse loudly instead of guessing.
#[must_use]
pub fn check_recovery_honesty(
    client: &str,
    truth: DurableTruth,
    report: AttachReport,
) -> Vec<Violation> {
    match (truth, report) {
        (DurableTruth::Present, AttachReport::SessionPresent(false)) => vec![violation(
            "recovery-honesty",
            format!("client {client:?}: a recoverable session was answered `present = false` (fabricated clean session, ADR 0017)"),
        )],
        (DurableTruth::Absent, AttachReport::SessionPresent(true)) => vec![violation(
            "recovery-honesty",
            format!("client {client:?}: no session exists but the attach answered `present = true`"),
        )],
        // A loud refusal is always honest; Unknown truth constrains nothing.
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Bounded structures
// ---------------------------------------------------------------------------

/// Verify a bounded structure holds its bound (ADR 0041): `len > bound` is the
/// violation. Named so a schedule-end sweep reads as a catalog check.
#[must_use]
pub fn check_bound(name: &str, len: usize, bound: usize) -> Vec<Violation> {
    if len > bound {
        vec![violation(
            "bounded-structures",
            format!("{name}: length {len} exceeds its bound {bound}"),
        )]
    } else {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease_raft::LeaseRequest;

    fn entry(offset: Offset, record: &[u8]) -> LogEntry {
        LogEntry {
            offset,
            record: record.to_vec(),
        }
    }

    /// The ledger passes a faithful recovery — including unacked extras a
    /// below-quorum write may have left — and an acked-remove discharges promises.
    #[test]
    fn ack_ledger_accepts_a_faithful_recovery() {
        let mut ledger = AckLedger::new();
        ledger.ack_append("k", 1, b"a");
        ledger.ack_append("k", 2, b"b");
        ledger.ack_truncate("k", 1);
        // Offset 3 was committed but never acked: allowed to appear.
        let recovered = [entry(2, b"b"), entry(3, b"unacked")];
        assert_holds(&ledger.verify_recovered("k", &recovered));

        ledger.ack_remove("k");
        assert_holds(&ledger.verify_recovered("k", &[]));
    }

    /// Each failure shape is caught and named: a lost acked entry, corrupted acked
    /// bytes, a resurrected truncated offset, and a non-increasing log.
    #[test]
    fn ack_ledger_catches_loss_corruption_resurrection_and_disorder() {
        let mut ledger = AckLedger::new();
        ledger.ack_append("k", 1, b"a");
        ledger.ack_append("k", 2, b"b");
        ledger.ack_truncate("k", 1);

        let lost = ledger.verify_recovered("k", &[]);
        assert_eq!(lost.len(), 1, "{lost:?}");
        assert_eq!(lost[0].invariant, "acked-durability");

        let corrupt = ledger.verify_recovered("k", &[entry(2, b"MANGLED")]);
        assert!(corrupt.iter().any(|v| v.invariant == "acked-integrity"));

        let resurrected = ledger.verify_recovered("k", &[entry(1, b"a"), entry(2, b"b")]);
        assert!(resurrected.iter().any(|v| v.invariant == "no-resurrection"));

        let disordered = ledger.verify_recovered("k", &[entry(2, b"b"), entry(2, b"b")]);
        assert!(disordered.iter().any(|v| v.invariant == "log-well-formed"));
    }

    /// Fencing allows monotone acceptance and any refusal, and catches exactly a
    /// stale-epoch acceptance — per group, not across groups.
    #[test]
    fn fence_log_catches_a_stale_acceptance_only() {
        let mut ok = FenceLog::new();
        ok.observe(1, 5, true);
        ok.observe(1, 5, true); // same epoch again: fine (>= fence)
        ok.observe(1, 3, false); // stale refused: exactly right
        ok.observe(2, 3, true); // another group at a lower epoch: independent
        ok.observe(1, 7, true);
        assert_holds(&ok.verify());

        let mut bad = FenceLog::new();
        bad.observe(1, 5, true);
        bad.observe(1, 3, true); // deposed holder wrote through the fence
        let violations = bad.verify();
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert_eq!(violations[0].invariant, "epoch-fencing");
    }

    /// Lease epochs must strictly increase across groups (one shared counter), and
    /// the map must agree with the mint history; both directions are caught.
    #[test]
    fn lease_log_checks_the_counter_and_the_map() {
        let mut map = LeaseMap::new();
        let mut log = LeaseLog::new();
        for (group, node) in [(1u64, 10u64), (2, 11), (1, 12)] {
            log.observe(map.apply(&LeaseRequest::Assign { group, node }).unwrap());
        }
        assert_holds(&log.verify());
        assert_holds(&log.verify_map(&map));

        // A reused epoch (two mints at 3) violates the counter contract.
        let mut reused = LeaseLog::new();
        reused.observe(LeaseResponse {
            group: 1,
            holder: 10,
            epoch: 3,
        });
        reused.observe(LeaseResponse {
            group: 2,
            holder: 11,
            epoch: 3,
        });
        assert!(reused
            .verify()
            .iter()
            .any(|v| v.invariant == "lease-epoch-monotonic"));

        // A map that missed the last assignment disagrees with the history.
        let stale_map = LeaseMap::new();
        assert!(log
            .verify_map(&stale_map)
            .iter()
            .any(|v| v.invariant == "lease-map-agreement"));
    }

    /// Token applications must strictly increase per topic; equal or lower tokens
    /// are the ADR 0037 resurrection shape and are caught.
    #[test]
    fn token_log_catches_non_monotonic_application() {
        let mut ok = TokenLog::new();
        ok.observe_applied("t", (1, 1));
        ok.observe_applied("t", (1, 2));
        ok.observe_applied("u", (1, 1)); // per topic, not global
        ok.observe_applied("t", (5, 1)); // epoch beats offset lexicographically
        assert_holds(&ok.verify());

        let mut bad = TokenLog::new();
        bad.observe_applied("t", (2, 2));
        bad.observe_applied("t", (2, 2)); // idempotent re-apply must be skipped, not applied
        bad.observe_applied("t", (1, 9)); // stale epoch resurrection
        let violations = bad.verify();
        assert_eq!(violations.len(), 2, "{violations:?}");
        assert!(violations
            .iter()
            .all(|v| v.invariant == "retained-token-monotonic"));
    }

    /// Convergence compares topics, tokens, and payloads across every node, in
    /// both directions (missing on either side is caught).
    #[test]
    fn retained_convergence_catches_divergence_both_ways() {
        let snap = |pairs: &[(&str, Token, &[u8])]| -> RetainedSnapshot {
            pairs
                .iter()
                .map(|(t, tok, p)| ((*t).to_string(), (*tok, p.to_vec())))
                .collect()
        };
        let a = snap(&[("t", (2, 2), b"v2")]);
        let same = snap(&[("t", (2, 2), b"v2")]);
        assert_holds(&check_retained_convergence(&[
            ("a", a.clone()),
            ("b", same),
        ]));

        let stale = snap(&[("t", (1, 1), b"v1")]);
        let extra = snap(&[("t", (2, 2), b"v2"), ("ghost", (1, 3), b"boo")]);
        let empty = snap(&[]);
        for other in [stale, extra, empty] {
            let violations = check_retained_convergence(&[("a", a.clone()), ("b", other)]);
            assert!(
                violations
                    .iter()
                    .all(|v| v.invariant == "retained-convergence")
                    && !violations.is_empty(),
                "{violations:?}"
            );
        }
    }

    /// A client id live twice — on two nodes or twice on one — is caught.
    #[test]
    fn session_singularity_catches_a_doubled_client() {
        assert_holds(&check_session_singularity(&[
            ("a", "c1"),
            ("a", "c2"),
            ("b", "c3"),
        ]));
        let cross = check_session_singularity(&[("a", "c1"), ("b", "c1")]);
        assert_eq!(cross.len(), 1, "{cross:?}");
        assert_eq!(cross[0].invariant, "session-singularity");
        let same_node = check_session_singularity(&[("a", "c1"), ("a", "c1")]);
        assert_eq!(same_node.len(), 1, "{same_node:?}");
    }

    /// Honesty: fabrication and invention are violations; a loud refusal never is,
    /// and an unknown truth constrains nothing.
    #[test]
    fn recovery_honesty_allows_refusal_catches_fabrication() {
        use AttachReport::{RefusedUnavailable, SessionPresent};
        use DurableTruth::{Absent, Present, Unknown};

        assert_holds(&check_recovery_honesty("c", Present, SessionPresent(true)));
        assert_holds(&check_recovery_honesty("c", Absent, SessionPresent(false)));
        assert_holds(&check_recovery_honesty("c", Present, RefusedUnavailable));
        assert_holds(&check_recovery_honesty("c", Unknown, SessionPresent(false)));

        let fabricated = check_recovery_honesty("c", Present, SessionPresent(false));
        assert_eq!(fabricated.len(), 1, "{fabricated:?}");
        assert_eq!(fabricated[0].invariant, "recovery-honesty");
        let invented = check_recovery_honesty("c", Absent, SessionPresent(true));
        assert_eq!(invented.len(), 1, "{invented:?}");
    }

    /// The bound check is exact: at the bound holds, one past it is caught.
    #[test]
    fn bound_check_is_exact() {
        assert_holds(&check_bound("q", 100, 100));
        let over = check_bound("q", 101, 100);
        assert_eq!(over.len(), 1, "{over:?}");
        assert_eq!(over[0].invariant, "bounded-structures");
    }
}
