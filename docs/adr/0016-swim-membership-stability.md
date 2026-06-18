# ADR 0016 — SWIM membership stability (dead-node fencing + false-positive resistance)

- **Status:** Accepted; **phase 1 implemented** (2026-06-18); phase 2 pending
- **Date:** 2026-06-18
- **Deciders:** project maintainers
- **Related:** [ADR 0003](0003-gossip-authentication.md) (SWIM datagram auth),
  [ADR 0005](0005-session-affinity.md) (placement owns relocation),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (durable sessions whose recovery depends on a correct replica set),
  [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md)

## Context

Placement (and therefore the durable session store's replica sets) is derived
directly from SWIM membership. A diagnosed failure (`docs/TEST-PLAN.md`, the
durable client-observable-failover gap) traced to **membership instability**, not to
placement or recovery logic: after a node is killed, a survivor's `members()` flaps
to a *wrong* set that **still lists the killed node** (resurrected) and has **dropped
a live survivor** (falsely evicted). The resulting replica set has no live quorum, so
session recovery reads the dead node, times out, and fails — stalling the first
client reconnect ~10s with `session_present=false`.

The SWIM implementation already has the core mechanisms (`swim.rs`): per-node
**incarnation** numbers, **suspicion** (a probe failure marks `Suspect`, not `Dead`),
and **self-refutation** (a node hearing itself suspected bumps its incarnation and
re-asserts `Alive`). Two specific gaps remain, and they are exactly the two halves of
the diagnosed bug:

1. **`Dead` is not terminal.** A `Dead` member stays in the map and is *resurrected*
   by any later update with a higher incarnation (`apply_update`'s `supersedes` rule).
   A node that refuted to a high incarnation just before dying leaves that
   `Alive(high)` gossip in flight; it arrives after the `Dead` declaration and revives
   the corpse. There is no tombstone and no pruning of `Dead`.
2. **Suspicion → `Dead` is single-source with fixed timeouts.** One prober's timeout
   (`ack_timeout_ms`, then `suspicion_timeout_ms`) is enough to drive a peer to
   `Dead`. A prober that is itself CPU-starved declares healthy peers dead, and a
   victim that is starved cannot refute within the fixed window. Nothing adapts the
   timeouts to local health or requires independent confirmation.

These are well-understood problems with well-understood fixes (the SWIM paper's
terminal `Dead`/tombstone, and the **Lifeguard** extensions to SWIM). This ADR adopts
them, scoped to what closes the gap.

## Decision

### 1. `Dead` is a tombstoned terminal state (fixes resurrection)

When a member reaches `Dead`, it becomes a **tombstone**: kept with a
`tombstone_deadline = now + DEAD_TTL`, and during that window **no gossiped update can
revive it** — `Alive`/`Suspect` updates *about a tombstoned node* are dropped
regardless of incarnation. After `DEAD_TTL` the tombstone is **pruned** from the map.

`DEAD_TTL` is set comfortably above the gossip drain time (several protocol periods),
so a stale pre-death refutation cannot outlive the tombstone — the `dur-c`
resurrection is impossible. A node that genuinely restarts rejoins **after** the
tombstone is pruned, or under a fresh node id; it does not need to out-race stale
gossip. (Self-refutation for the *local* node is unaffected — a node always defends
its own liveness; the tombstone governs only third-party claims about *others*.)

This mirrors the dead-node interval in mature gossip stacks (memberlist/Serf).

### 2. Lifeguard local-health awareness (reduces false positives)

Adopt Lifeguard's **self-awareness multiplier**: the local node tracks a small
`awareness` score that rises when its *own* probes go unanswered or it has to refute
itself (signals that *it* is the slow one) and decays back down on healthy rounds.
Both `ack_timeout` and `suspicion_timeout` are scaled by `(1 + awareness)`, so a
locally-degraded node:

- waits **longer** before suspecting peers (it stops blaming others for its own
  slowness), and
- as a victim, benefits from peers' longer suspicion windows, giving its refutation
  time to land.

This directly attacks the `dur-a` false-eviction without weakening detection on a
healthy node (awareness = 0 ⇒ today's timeouts).

### 3. Independent-suspicion confirmation before `Dead` (phase 2)

Also from Lifeguard: a node's **suspicion timeout shrinks as independent suspicions of
it accumulate** (and starts long). A single prober's suspicion holds the full window;
only when multiple distinct nodes independently suspect the same peer does it
fast-track to `Dead`. One contended prober can no longer unilaterally kill a healthy
peer. (Refutation, §SWIM, still overrides a false suspicion outright.)

### 4. Keep the existing incarnation/refutation core

Incarnation precedence and self-refutation are correct and stay. The changes are
*additive*: a terminal/tombstoned `Dead`, awareness-scaled timeouts, and
confirmation-scaled suspicion.

### Phasing

- **Phase 1 — tombstone `Dead` (§1).** Smallest change; on its own it removes the
  *resurrection* half (a dead node stays dead), which is the harder-to-mitigate half.
- **Phase 2 — Lifeguard awareness + confirmation (§2, §3).** Reduces *false-positive*
  Dead declarations so tombstoning a live node is rare.

Together they make membership converge to the live set and stay there, which is what
the durable failover (and any placement-derived routing) needs.

## Consequences

- **Good:** closes the durable client-observable-failover gap (recovery sees a live
  quorum); placement/replica-set correctness no longer depends on perfectly-timed
  gossip; fewer spurious ownership churns cluster-wide; grounded in published,
  battle-tested algorithms.
- **Cost / limits:** a node that is **genuinely** restarted (same id) must wait out
  `DEAD_TTL` before rejoining, or use a fresh id — acceptable and standard. A node
  *falsely* declared `Dead` is tombstoned for `DEAD_TTL` too; phase 2 makes that rare
  but not impossible, so `DEAD_TTL` is a bounded, tuned value, not infinite. Awareness
  adds a little per-node state and a multiplier on timeouts (slower detection on a
  degraded node — by design).
- **Risk:** this is correctness-critical membership code feeding durability. It must
  be developed **test-first against the pure `Swim` state machine** (no network),
  reproducing the resurrection and false-eviction scenarios as deterministic unit
  tests *before* any wiring, then validated by the durable failover integration test
  (which becomes deterministic). A wrong change here can silently corrupt the
  durability guarantees, so it warrants its own focused workstream — not a reactive
  edit.

## Update — phase 1 implemented (2026-06-18)

Phase 1 (§1, tombstone `Dead`) is implemented in `swim.rs`, developed test-first on
the pure state machine:

- `Member.tombstone_deadline: Option<u64>`, set to `now + dead_ttl_ms` when a member
  becomes `Dead`; while set, `apply_update` drops any non-`Dead` gossip about the
  member regardless of incarnation; `tick` prunes expired tombstones.
- Unit tests (a) a high-incarnation `Alive` after `Dead` does **not** revive, and
  (b) a tombstone prunes after `dead_ttl_ms` and the id can rejoin.

**Validated against the durable-failover scenario, and the diagnosis was refined.**
With tombstoning, the new owner's membership after a takeover is now correct — the
killed node is no longer resurrected into the replica set, the live survivor is
present, and the store recovers the session (meta + subscriptions) from a quorum in
~1s. So the **membership half of the gap is closed**, as designed.

It does **not** make the client-observable failover test pass "for free" as the
Consequences section optimistically claimed — that revealed a *separate* bug outside
SWIM: during the ~1s before the group's lease is reassigned to the new owner,
`ensure_session` returns a transient `NotOwner`, and the hub's attach path swallows it
(`ensure_session(...).unwrap_or(false)`), so a client reconnecting in that window
CONNACKs `session_present=false` and starts a fresh session instead of waiting for the
lease. Closing the client-observable gap therefore needs an **attach-path** change
(treat a transient lease error as "not ready, retry/wait"), which alters CONNACK
latency semantics and warrants its own decision — tracked separately, not part of this
ADR. Phase 2 (§2–§3) remains worthwhile to stop a *live* node being falsely evicted
under load, but is not what blocks that test now.

## Alternatives considered

- **Just loosen the test's SWIM timings.** Rejected as the fix: it reduces the
  *false-eviction* rate but does nothing about *resurrection* (a stale high-incarnation
  `Alive` still revives a dead node), and it papers over a real production fragility.
- **Prune `Dead` immediately (no tombstone).** This is essentially today's behaviour
  (a `Dead` member lingering and revivable); removing it without a tombstone still
  lets a re-learned `Alive` re-insert the node. The tombstone is what makes `Dead`
  stick.
- **Full Lifeguard (buddy-system probing, NACK-aware, all knobs).** More than this gap
  needs now; §2–§3 take the parts that address the observed failures and leave the rest
  as a later option.
- **Make recovery tolerate a bad replica set (retry / shrink quorum).** Rejected:
  retrying against a set that lacks a live quorum cannot succeed, and shrinking the
  quorum would break the safety (intersection) property the durable log relies on. The
  membership must be correct; recovery must not paper over it.

## Implementation notes (for the workstream, not this ADR)

- New per-member `tombstone_deadline: Option<u64>`; `apply_update` drops third-party
  reviving updates while tombstoned; a prune pass in `tick` removes expired tombstones.
- `awareness: u8` on `Swim`; bump on self-probe failure / self-refutation, decay on a
  clean round; scale `ack_timeout`/`suspicion_timeout` by `(1 + awareness)`.
- Track per-suspect independent-suspicion count; scale the effective suspicion timeout
  down toward a floor as it grows.
- Unit tests on `Swim` directly: (a) a high-incarnation `Alive` after `Dead` does **not**
  revive; (b) a tombstone prunes after `DEAD_TTL` and the id can rejoin; (c) a slow
  local node (forced awareness) does not declare a healthy peer dead; (d) one prober's
  suspicion alone does not fast-track `Dead`.
