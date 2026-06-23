# ADR 0026 — Lease-group raft timing tolerant of durable-storage latency

- **Status:** Accepted
- **Date:** 2026-06-24
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0026-lease-timing-durable-storage.md](../delivery/0026-lease-timing-durable-storage.md) — plan, progress, and changelog
- **Related:** [ADR 0006](0006-consensus-and-replication.md) (the lease consensus group this
  re-tunes), [ADR 0007](0007-durable-store-integration.md) (durable sessions, the feature it
  unbreaks), [ADR 0018](0018-on-disk-persistence.md) (the fsync-on-commit persistent stores
  whose latency triggered it), [ADR 0021](0021-bounded-lease-voters.md) (bounded voters,
  which separately reduces reconfiguration churn), [ADR 0024](0024-deterministic-testing.md)
  (the test discipline the new fault-injection coverage follows)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0026-lease-timing-durable-storage.md).

## Context

A 3-node **durable** cluster on real disk never holds a stable lease leader: openraft
re-elects roughly once a second (the consensus `epoch`/term climbs without bound), durable
session recovery times out, and the durable feature is effectively unusable. The 3-node
demo surfaced it.

Bisection pinned the cause precisely:

- The lease group reaches a **stable leader** with the **in-memory** lease store
  (`epoch = 1`, flat).
- With the **persistent (redb) store** it churns — **mildly even on tmpfs**, and **severely
  on a real disk volume** (epoch → 34+ in 30s).
- The persistent and in-memory stores keep **identical in-memory structures** (the log map,
  the cached vote); reads come from memory in both. The **only** difference is that the
  persistent store adds an **`Durability::Immediate` (fsync) commit on every write**
  (`save_vote`, `append_to_log`). So this is a **write-latency** problem, not a store
  correctness bug.

The lease raft is configured for in-memory speed — **heartbeat 100ms, election 300–600ms**.
A fsync-on-commit write on a container disk takes tens of milliseconds; the leader cannot
persist-and-replicate within the ~300–600ms leader-lease window, so followers' lease expires
and they re-elect. The 200ms reconcile/assign driver, unable to commit during the churn,
re-proposes each tick and amplifies it ("the cluster is already undergoing a configuration
change").

It shipped because the durable integration tests build the node with the **in-memory** store
(`data_dir = None`) — the fsync path had **zero coverage**.

## Decision

Make the lease group stable on durable storage, and prove it.

### 1. Size the raft timing to durable-write latency

Relax the lease-group raft config so a fsync-on-commit persist comfortably fits inside the
heartbeat and leader-lease budget: **heartbeat 500ms, election 1500–3000ms** (was
100 / 300–600). The leader then sustains its lease across slow commits; a dead leader is
still detected within a few seconds — well inside the durable-takeover budget, which is
already multi-second (recovery rebuilds a committed log from a quorum). In-memory clusters
are unaffected except that failover is a little less eager.

### 2. Reduce churn-amplifying load

Slow the lease driver's reconcile/assign tick from 200ms to ~1s (the work is a no-op in
steady state, ADR 0007), and ensure the membership reconciler does not re-propose a
configuration change while one is still in flight. This removes the feedback that turns a
single slow commit into sustained churn.

### 3. Test the persistent path under injected latency

The gap that hid this was that no test exercised durable storage. Add coverage that drives a
multi-node lease group with a **fault-injected commit latency** (the store's `persist` is
`async`, so a configurable delay before it returns simulates a slow fsync without a real
disk). The test asserts the group forms a single leader and the term stays **stable** over an
observation window under that latency.

This is coverage of the **slow-commit write path** — which had none — not yet a deterministic
guard for the churn itself. The churn is driven by heartbeat/lease maintenance over a network
with latency; the in-process test router delivers every raft RPC instantly, and the injected
`commit_delay` only reaches the persist path (`save_vote`/`append_to_log`), so empty
steady-state heartbeats never feel it. The same test consequently passes under both the old
and the relaxed timing (confirmed live at delays up to 700ms). A true deterministic regression
guard needs **network-latency injection** into the raft RPCs — a madsim/turmoil simulation
harness (ADR 0024 T7), deferred. The timing fix itself was **validated live in the demo**: a
persistent 3-node group held `lease_leader = 1` / `lease_epoch = 1` flat over 20s with zero
vote churn (the pre-fix demo churned past epoch 200).

## Consequences

- **Good:** durable sessions actually work on disk — a stable leader, no election churn,
  recoverable sessions. The slow-commit write path, previously untested, now has coverage, so
  it cannot silently rot in the obvious ways; a fully deterministic churn guard awaits the
  network-latency simulation harness (ADR 0024 T7).
- **Cost:** failover detection on the lease group goes from sub-second to ~1.5–3s. For a
  durability feature whose takeover already costs seconds that is a sound trade; for very
  latency-sensitive lease handoff it is the relevant knob to revisit.
- **Risk:** correctness-critical consensus timing — the fix was **validated live in the demo**
  (the only place the churn reproduces) and the full gate is green. The timing only affects
  liveness/failover latency, never safety (Raft safety is independent of these timeouts).

## Alternatives considered

- **Drop fsync / relax durability on the lease writes.** Rejected: persist-before-acknowledge
  is a Raft safety requirement (a restarted voter must not double-vote; a committed lease
  must survive a crash). The latency is the price of correctness; the fix is to budget for
  it, not remove it.
- **Keep the fast timing, only run durable in-memory.** That is the current de-facto state
  (and what the demo fell back to) — but it abandons on-disk durability (ADR 0018), the whole
  point of the persistent stores. Rejected.
- **Group-commit / coalesce the raft log writes to cut fsync count.** A real lever and kept
  as a later option (delivery T5) if the relaxed timing proves insufficient on very slow
  storage; openraft already batches AppendEntries, so the marginal win is bounded. Not needed
  to make the group stable, so deferred.
- **Adaptive timing (measure commit latency, scale timeouts).** More robust in principle but
  more machinery and harder to reason about; a fixed, generous budget is simpler and
  sufficient. Revisit only if deployments span wildly different storage speeds.
