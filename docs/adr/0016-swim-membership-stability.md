# ADR 0016 — SWIM membership stability (dead-node fencing + false-positive resistance)

- **Status:** Accepted
- **Date:** 2026-06-18
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0016-swim-membership-stability.md](../delivery/0016-swim-membership-stability.md) — phases, progress, and changelog
- **Related:** [ADR 0003](0003-gossip-authentication.md) (SWIM datagram auth),
  [ADR 0005](0005-session-affinity.md) (placement owns relocation),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (durable sessions whose recovery depends on a correct replica set),
  [ADR 0017](0017-durable-attach-readiness.md) (the attach-path half of the failover gap)

> This record states the decision only. The phased rollout and how far along it is live
> in the [delivery doc](../delivery/0016-swim-membership-stability.md).

## Context

Placement (and therefore the durable session store's replica sets) is derived directly
from SWIM membership. A diagnosed failure (the durable client-observable-failover gap)
traced to **membership instability**: after a node is killed, a survivor's `members()`
flaps to a *wrong* set that **still lists the killed node** (resurrected) and has **dropped
a live survivor** (falsely evicted). The resulting replica set has no live quorum, so
session recovery reads the dead node, times out, and fails — stalling the first client
reconnect ~10s with `session_present=false`.

The SWIM implementation already had the core mechanisms (`swim.rs`): per-node
**incarnation** numbers, **suspicion** (a probe failure marks `Suspect`, not `Dead`), and
**self-refutation** (a node hearing itself suspected bumps its incarnation and re-asserts
`Alive`). Two specific gaps remained — exactly the two halves of the diagnosed bug:

1. **`Dead` is not terminal.** A `Dead` member stayed in the map and was *resurrected* by
   any later higher-incarnation update. A node that refuted to a high incarnation just
   before dying left that `Alive(high)` gossip in flight; it arrived after the `Dead`
   declaration and revived the corpse. No tombstone, no pruning.
2. **Suspicion → `Dead` is single-source with fixed timeouts.** One prober's timeout was
   enough to drive a peer to `Dead`. A CPU-starved prober declares healthy peers dead, and
   a starved victim cannot refute within the fixed window. Nothing adapted timeouts to
   local health or required independent confirmation.

These are well-understood problems with well-understood fixes (the SWIM paper's terminal
`Dead`/tombstone, and the **Lifeguard** extensions). This ADR adopts them, scoped to what
closes the gap.

## Decision

### 1. `Dead` is a tombstoned terminal state (fixes resurrection)

When a member reaches `Dead` it becomes a **tombstone**: kept with a `tombstone_deadline =
now + DEAD_TTL`, and during that window **no gossiped update can revive it** —
`Alive`/`Suspect` updates *about a tombstoned node* are dropped regardless of incarnation.
After `DEAD_TTL` the tombstone is **pruned**.

`DEAD_TTL` is set comfortably above the gossip drain time, so a stale pre-death refutation
cannot outlive the tombstone — the resurrection is impossible. A node that genuinely
restarts rejoins **after** the tombstone is pruned, or under a fresh id; it does not need
to out-race stale gossip. (Self-refutation for the *local* node is unaffected — a node
always defends its own liveness; the tombstone governs only third-party claims about
*others*.) This mirrors the dead-node interval in mature gossip stacks (memberlist/Serf).

### 2. Lifeguard local-health awareness (reduces false positives)

Adopt Lifeguard's **self-awareness multiplier**: the local node tracks a small `awareness`
score that rises when its *own* probes go unanswered or it has to refute itself (signals
that *it* is the slow one) and decays on healthy rounds. Both `ack_timeout` and
`suspicion_timeout` scale by `(1 + awareness)`, so a locally-degraded node waits **longer**
before suspecting peers, and as a victim benefits from peers' longer suspicion windows.
This attacks false-eviction without weakening detection on a healthy node (awareness = 0 ⇒
unchanged timeouts).

### 3. Independent-suspicion confirmation before `Dead`

Also from Lifeguard: a node's **suspicion timeout shrinks as independent suspicions
accumulate** (and starts long). A single prober's suspicion holds the full window; only
when multiple distinct nodes independently suspect the same peer does it fast-track to
`Dead`. One contended prober can no longer unilaterally kill a healthy peer. (Refutation
still overrides a false suspicion outright.)

### 4. Keep the existing incarnation/refutation core

Incarnation precedence and self-refutation are correct and stay. The changes are
*additive*: a terminal/tombstoned `Dead`, awareness-scaled timeouts, and
confirmation-scaled suspicion.

## Consequences

- **Good:** closes the membership half of the durable failover gap (recovery sees a live
  quorum); placement/replica-set correctness no longer depends on perfectly-timed gossip;
  fewer spurious ownership churns; grounded in published, battle-tested algorithms.
- **Cost / limits:** a node that is **genuinely** restarted (same id) must wait out
  `DEAD_TTL` before rejoining, or use a fresh id. A node *falsely* declared `Dead` is
  tombstoned for `DEAD_TTL` too; §3 makes that rare but not impossible, so `DEAD_TTL` is a
  bounded, tuned value. Awareness adds a little per-node state and slows detection on a
  degraded node (by design).
- **Risk:** this is correctness-critical membership code feeding durability. It must be
  developed **test-first against the pure `Swim` state machine** (no network), reproducing
  resurrection and false-eviction as deterministic unit tests *before* any wiring. A wrong
  change here can silently corrupt the durability guarantees, so it warrants its own focused
  workstream — not a reactive edit.

## Alternatives considered

- **Just loosen the test's SWIM timings.** Reduces the *false-eviction* rate but does
  nothing about *resurrection*, and papers over a real production fragility. Rejected.
- **Prune `Dead` immediately (no tombstone).** A re-learned `Alive` still re-inserts the
  node. The tombstone is what makes `Dead` stick.
- **Full Lifeguard (buddy-system probing, NACK-aware, all knobs).** More than this gap
  needs; §2–§3 take the parts that address the observed failures and leave the rest as a
  later option.
- **Make recovery tolerate a bad replica set (retry / shrink quorum).** Retrying against a
  set with no live quorum cannot succeed, and shrinking the quorum breaks the safety
  (intersection) property. Membership must be correct; recovery must not paper over it.
