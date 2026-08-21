# 0071. Owner-side group commit: one durable-write serializer per node

Date: 2026-08-21
Status: Accepted

## Context

An acked durable publish is fsync'd and quorum-replicated before the PUBACK
(ack-after-durable). ADR 0027 group-committed the **follower** half of that
promise: inbound `Replicate` frames queue into a single replica-writer task
that drains concurrent bursts into one fsync'd `apply_batch` transaction. The
**owner** half was left un-batched — `ClusterLog::local_ack` applied each op in
its own one-element `Durability::Immediate` transaction, so every owner-side
append paid a full disk barrier.

Two measurements made the case with numbers:

- The single-host micro-benchmark (`store_append_floor`, DURABLE-PATH.md): one
  durable append costs ~5 ms; 32-way concurrency does **not** beat serial
  (~179–203/s either way) — the barrier is per-volume, and without batching
  concurrency buys nothing.
- The published scaling curve (SCALE-CURVE.md, v1.0.1 on dedicated NVMe):
  durable QoS 1 saturates at ~1951 msg/s single-node against a measured
  ~2226 barriers/s disk floor — the broker sits at the physics of one barrier
  per append, and the only lever left is amortization.

Two latent defects sat on the same path: the owner's fsync ran **inline on a
tokio async worker thread** (every other redb writer in the tree uses
`spawn_blocking`), and the owner and follower writers contended on the same
`ReplicaState` mutex as two independent lockers.

## Decision

**One node-wide durable-write serializer per node, shared by both halves.**

1. `ClusterLog` accepts an optional writer handle
   (`with_owner_writer` / `maybe_owner_writer`). With it attached, `local_ack`
   sends `(epoch, op, oneshot)` into the plane's existing replica-writer task
   and awaits the result instead of applying inline. The writer drains
   whatever is queued — owner appends across all 256 groups **and** follower
   replica applies — into one `ReplicaState::apply_batch` call: one
   `Durability::Immediate` transaction, one barrier, per batch.
2. **Semantics unchanged, per op.** `apply_batch` already enforces identical
   per-op fencing to `apply` (ADR 0027 rule 1); each op's oneshot returns its
   own accept/fence verdict; an op's self-ack counts toward quorum exactly as
   before (ADR 0042 T8). A closed writer (shutdown) reads as *not durable* —
   fail closed, matching the follower path.
3. **Local-durable-before-fan-out is preserved.** `ClusterLog::append` awaits
   the local ack *before* starting the follower fan-out, exactly as the inline
   version did. An op never reaches a follower unless the owner's own store
   accepted and fsync'd it. (Overlapping the two was considered and rejected:
   it opens a reconciliation window — an op replicated to followers that the
   owner's store later fences — for a latency win batching mostly delivers
   anyway.)
4. The writer's fsync runs on the blocking pool (`spawn_blocking`) — fixing
   the inline-fsync-on-async-worker defect for the owner path as a side
   effect — and both halves now write through one serializer, ending the
   two-locker contention on `ReplicaState`.
5. **Observability:** the serializer publishes counters (`WriterStats`:
   batches, ops, max batch) polled by `mqttd` into Prometheus as
   `mqttd_durable_writer_batches` / `_ops` / `_max_batch` — ops/batches is the
   live mean batch size, 1.0 at rest and rising exactly when group commit
   pays. ADR 0027's writer had no metrics; this covers both halves.

At rest (one op in flight) the writer degenerates to the previous
one-op-per-commit behavior — the batch of one — so idle latency is unchanged.

## Consequences

- Owner-side durable throughput is no longer bounded by one barrier per
  append but by one barrier per *batch*; under concurrent load (many sessions,
  many groups) the acked rate can approach `barrier_rate × mean_batch_size`.
  The delivery note records before/after numbers.
- Per-op ack latency under saturation *includes* up to one extra batch wait
  (an op arriving mid-fsync waits for the next batch). This is the standard
  group-commit trade; the uncontended path is one queue hop.
- `local_ack` is now async; recovery-time call sites (takeover fence,
  re-commit, truncate, remove) route through the same writer and batch with
  ongoing traffic.
- The in-memory harness path (`local_store: None`) is untouched, as is the
  single-node non-cluster backend (`PersistentLog`, `sessions.redb`) — the
  latter still commits per append and is the known follow-up if single-node
  durable throughput ever matters as much as clustered.

## Alternatives considered

- **Same-group append pipelining** (splitting offset assignment from the
  quorum wait so one group's appends overlap): larger surgery on
  `ClusterLog::append`'s locking for a win that cross-group batching already
  captures at realistic fan-outs (sessions hash across 256 groups). Revisit if
  single-group hot keys dominate real workloads.
- **A second, owner-only writer task**: preserves the two-locker contention
  and doubles the metrics surface for no benefit — both halves write the same
  store.
- **Relaxing durability** (`Durability::Eventual`, deferred fsync): a
  different contract, not an optimization — see ADR 0072.
