# ADR 0027 — Group-commit for the durable replica apply path

- **Status:** Accepted
- **Date:** 2026-06-24
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0027-replica-group-commit.md](../delivery/0027-replica-group-commit.md) — plan, progress, and changelog
- **Related:** [ADR 0007](0007-durable-store-integration.md) (the durable replication path this
  re-shapes), [ADR 0018](0018-on-disk-persistence.md) (the `Durability::Immediate` fsync each
  replica write does), [ADR 0026](0026-lease-timing-durable-storage.md) (which surfaced the
  under-load churn this addresses — its T5), [ADR 0024](0024-deterministic-testing.md) (the
  test discipline, and why the device-contention effect can only be validated live)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0027-replica-group-commit.md).

## Context

[ADR 0026](0026-lease-timing-durable-storage.md) made the durable lease group hold a stable
leader **at rest**. Re-enabling durable in the demo (its T4) showed a residual problem **under
load**: even the demo's gentle ~2–3 QoS-1 publishes/sec make the lease term/epoch climb slowly
(the same node usually re-wins, so `lease_leader` *looks* stable while the term churns). Stop
the load and the epoch goes flat. So this is a load-induced effect, separate from the
raft-timing-vs-fsync mismatch ADR 0026 fixed.

Tracing the durable write paths pinned the structural amplifier:

- The **lease store** already coalesces: a raft write batches its mutations into one
  `Vec<WriteOp>` and commits them in a single fsync'd transaction
  ([`lease_store::apply_ops`](../../crates/mqtt-cluster/src/lease_store.rs)).
- The **follower replica apply** does **not**:
  [`ReplicaState::apply`](../../crates/mqtt-cluster/src/cluster_log.rs) calls `persist`, which
  opens a `Durability::Immediate` (fsync) transaction **per `ReplOp`**. Every replicated
  message is its own fsync. Worse, each inbound `Replicate` frame is handled in its own task
  (`hub::handle_durable_frame` spawns), and they all contend on the single
  `Arc<Mutex<ReplicaState>>` — so N concurrent replicated messages become N fsyncs serialized
  on that mutex.

Both the lease store (`lease.redb`) and the replica store (`replicas.redb`) live on the same
data volume and share the tokio blocking pool, so those per-message fsyncs contend — at the
storage device and in the blocking pool — with the lease group's own (rarer) fsyncs and with
timely raft RPC servicing. The result is the slow election churn under load.

Note what this is **not**: it is *not* peer-link head-of-line blocking. The production peer
pump spawns each frame (`handle_durable_frame`), so a slow replica fsync does not stall the
next raft heartbeat frame at the dispatch level. The amplifier is the sheer count of
per-message fsyncs and the contention they create, not in-order frame processing.

## Decision

**Group-commit the follower replica apply.** Coalesce a burst of replication ops into a single
fsync'd transaction, the way the lease store already does for raft writes.

### 1. A batch apply on `ReplicaState`

Add `ReplicaState::apply_batch(&mut [(Epoch, ReplOp)]) -> Vec<bool>`: fence-check each op in
order (identical per-op semantics to `apply`), persist **all accepted ops in one
`Durability::Immediate` transaction**, then mutate the in-memory copy. The persist-before-ack
invariant is preserved at batch granularity: a `true` ack means the op is on disk, because the
single transaction committed before any ack is returned; if the commit fails, every op in the
batch is rejected (nothing was durably stored). `apply` becomes the one-element case.

### 2. A single replica-writer task that coalesces

Replace the per-frame `spawn_blocking(apply one op)` in
[`DurablePlane::handle`](../../crates/mqtt-cluster/src/durable_plane.rs) with a send to one
**replica-writer**: a task that `recv`s a pending op, drains everything else already queued
(`try_recv`), applies the whole set with `apply_batch` (one fsync), and then answers each
waiting frame with its accept/fence result. Under load this collapses N per-message fsyncs into
one per batch; at rest (one op at a time) it is exactly today's behaviour. The writer owns the
write side of `ReplicaState`; recovery-reads still take the shared lock between batches.

### 3. Validate live

As with [ADR 0026](0026-lease-timing-durable-storage.md) T2, the in-process test harness
delivers RPCs instantly and cannot reproduce the device-level fsync contention, so the
**stabilisation** is validated in the demo (durable overlay, under the loadgen: the lease
term/epoch must stay flat). The **correctness** of group-commit — durability, fencing,
ordering, ack-iff-committed — is covered by deterministic unit tests on `apply_batch`.

## Consequences

- **Good:** under load the replica path issues far fewer fsyncs (one per batch, not one per
  message), cutting the device + blocking-pool + mutex contention that was starving the lease
  group of timely commits. Durability is unchanged — every acked op is still fsync'd before the
  ack. The single writer also removes the N-way mutex contention on `ReplicaState`.
- **Cost:** a small added latency for a replicated op — it waits for the current batch's single
  fsync rather than racing its own. Bounded by one fsync interval and net-positive under load
  (fewer fsyncs overall). One new concurrency component (the writer task) in a
  correctness-critical path, so it carries dedicated tests and a bounded, back-pressured queue.
- **Risk:** batching the durability transaction is the delicate part. It is changed test-first,
  with `apply_batch` proven to (a) fsync once for the batch, (b) reject the whole batch on a
  persist failure, (c) honour per-op fencing identically to `apply`, and (d) preserve offset
  order. The live demo is the end-to-end proof that the churn is gone.

## Alternatives considered

- **Relax replica durability** (periodic/`Eventual` fsync instead of per-op `Immediate`).
  Rejected: an ack that is not yet on disk breaks the durability contract — a crash could lose
  an acknowledged, quorum-counted message. Group-commit cuts the fsync *count* without
  weakening *when* the ack is allowed.
- **Put `lease.redb` and `replicas.redb` on separate volumes.** An ops-level mitigation that
  does nothing when both sit on one physical device (the common case), and addresses only the
  device half of the contention, not the per-message fsync count. Orthogonal; not the fix.
- **Prioritise lease-group I/O over replication I/O** (a priority queue across the two stores).
  More machinery and harder to reason about than simply not generating the excess fsyncs in the
  first place. Revisit only if group-commit proves insufficient.
- **Do nothing / keep durable demo-only at rest.** Rejected: durable sessions that destabilise
  under ordinary load are not a usable feature.
