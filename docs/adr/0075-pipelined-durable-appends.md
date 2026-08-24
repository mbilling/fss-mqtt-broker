# 0075. Pipelined durable appends — the window stops paying the round trip per message

Date: 2026-08-24
Status: Accepted

## Context

With ADR 0074's detached truncate shipped, the v1.0.5 hardware curve located
the durable path's next limiter — and it is not the disk. At 1-node
saturation: 8,503 msg/s at a client p50 of **45 ms** while a single append
completes in **4.7 ms**, the group-commit writer averaging 25.5 ops/batch and
the volume ~30% barrier-busy. 45 ≈ 8 × 4.7 is the signature: a publisher's
window-8 messages queue **behind each other**, one full durability round trip
at a time. The falsifier proved it — the same 384 total in-flight, respread:

| shape | acked msg/s | p50 RTT |
|---|---|---|
| 48 publishers × window 8 | 4,119 | 85 ms |
| 384 publishers × window 1 | 7,917 | 14 ms |

Three stacked serializations produce this (issue #402; the third was found
during delivery, when fixing the first two moved the needle only 16%):

1. `conn.rs` awaits each inbound `QoS` 1 publish's full durability round
   trip (`done.await`) before reading the next packet — a publisher
   connection has at most ONE publish inside the broker regardless of its
   window. This is the outermost serializer and the reason the falsifier's
   384 window-1 *connections* outran 48 window-8 ones.
2. `append_lane_worker` is a strict serial executor: each job's
   `enqueue_with_expiry` — the whole quorum round trip — is awaited before
   the next job is even popped.
3. `ClusterLog::append_tiered` holds the per-group state lock across
   `local_ack` **and** the quorum fan-out, and assigns `offset =
   committed + 1` — a scheme in which a second in-flight append for the same
   key cannot even know its offset. This is exactly the "same-group append
   pipelining (splitting offset assignment from the quorum wait)" that ADR
   0071 recorded as out of scope.

What makes pipelining safe today: the follower plane is already
sparse-tolerant. `ReplicaState::apply` stores per-`(key, offset)` with
`(epoch, seq)` supersedence and no gap check; recovery reads merge a union
and detect gaps explicitly (issue #390). Order therefore has to be enforced
in exactly one place — the owner's commit watermark.

## Decision

Durable appends become **two-phase**: an ordered, cheap *submit* and a
parallel *durability wait*, with the owner committing strictly in offset
order.

1. **`ClusterLog::submit_tiered`** (new): under a short state lock, assign
   `offset = assigned + 1` (a new per-key high-water mark ≥ `committed`),
   stage the entry, bump `seq` (ADR 0042 T7 unchanged), **send the op to the
   ADR 0071 writer**, and — when the tier's requirement cannot already be
   met locally — start the follower fan-out. The lock is released before any
   waiting. The caller receives a `'static` future (the *pending append*)
   that resolves to the offset.
2. **In-order commit watermark.** A pending append whose acks are in parks
   until `committed == offset − 1`, then advances `committed` and resolves.
   Completions therefore cascade in offset order while the underlying writer
   batches and replication round trips overlap. A success is only ever
   reported for an offset at or below the watermark — an acked append can
   never be retroactively lost to an earlier failure.
3. **Tail-fail, no holes.** If an append fails its requirement, every
   pending append for that key at a **higher** offset also fails
   (fail-closed; the publishers retry, exactly as they do today for a
   refused append), their staged entries are removed, and once the key's
   pipeline drains, `assigned` resets to `committed`. Offsets are reused by
   retries with a higher `seq`, so replicas holding a failed attempt are
   superseded by the existing ADR 0042 T7 machinery. Earlier offsets are
   unaffected. The committed range stays gap-free by construction.
4. **The lane pipelines appends only.** `append_lane_worker` keeps up to
   `LANE_PIPELINE_DEPTH` pending appends per session in flight (submitting
   serially — submission order is offset order is delivery order), posting
   `AppendDone` as each resolves. Every non-append lane job (QoS 2 outbound
   records, spill, discard, remove) remains a **barrier**: the pipeline
   drains first, preserving ADR 0061's total order for them verbatim.
5. **Trait surface, default-eager.** `ReplicatedLog::submit_tiered` and
   `SessionStore::submit_enqueue_with_expiry` get default implementations
   that perform the whole operation eagerly and return an already-resolved
   pending — a backend that does not override them behaves exactly as
   before. Only the clustered store overrides. The lane completes
   already-resolved pendings inline (`try_ready`), keeping the
   non-clustered paths on their old timing to the poll.
6. **The connection pipelines its `QoS` 1 acks, strictly in order.** Instead
   of awaiting the hub inline, the reader parks `(ack, done)` in a
   per-connection FIFO and keeps reading; `serve`'s drain branch writes each
   PUBACK — refusal semantics (ADR 0041 T4/T11: v5 reason byte, v3.1.1
   close-no-ack) applied verbatim at drain time — only ever from the front.
   ACL denials (issue #246) ride the same queue with a pre-decided verdict,
   so acks can never overtake each other. Before ANY outbound packet is
   written, already-resolved acks are flushed first: the hub releases a
   publish's ack before queueing its fan-out, so a message's own PUBACK
   still observably precedes any delivery that followed from it. A `QoS` 1
   publish now occupies a Receive-Maximum slot until its ack is written
   (ADR 0012/0041 accounting extended); past a hard per-connection cap
   (256) the reader resolves the oldest inline — backpressure, bounded
   memory. **`QoS` 2 keeps its inline awaits**: its exactly-once window
   semantics stay exactly as reviewed, the same conservatism as ADR 0074's
   Decision 2.

The pending future captures only Arc'd plain state, the writer's completion
channel, and the transport — never a store handle — and it is driven by the
lane worker itself (a hub-owned task), so shutdown-abort semantics are
inherited and the ADR 0061/issue #242 teardown rule (no free-floating tasks
holding the store) is upheld by construction.

## Consequences

- Measured (same machine, same 48×8×48 shape, release): **4,119 →
  26,115 msg/s (6.3×), saturated p50 85 → 14.5 ms**, appends running at
  ~113 ops per barrier on a ~230-barriers/s volume — the store is finally
  the limiter again, the base that issue #403's sharding then multiplies.
- The publisher-visible contract is unchanged: an ack still means the
  message is durable per its tier; a refusal still means retry. What
  changes is only that a window's messages wait **concurrently**.
- `offset = committed + 1` becomes `assigned + 1`; the single-op case
  degrades to today's behavior exactly (fail → pipeline drains → `assigned`
  resets → the retry reuses the offset at a higher `seq`).
- Failure of a mid-pipeline append fails its tail. Publishers whose later
  messages were refused re-publish in their own order, so per-publisher
  ordering holds; this matches today's semantics, where a failed append is
  retried while later lane jobs proceed.

## Alternatives considered

- **Pipeline in the lane only, keep `ClusterLog` serial:** no gain — the
  per-group lock across the quorum wait re-serializes everything the lane
  parallelized.
- **Assign offsets at writer-commit time:** makes holes impossible without
  a watermark, but moves offset knowledge after replication fan-out starts,
  forcing a second follower round trip or reordering local-durable-before-
  fan-out (ADR 0071's kept invariant). The in-order watermark achieves the
  same no-holes guarantee without either cost.
- **A per-log driver task instead of lane-driven futures:** another owned
  task and channel for no behavioral difference; lane-driven inherits the
  right lifecycle for free.
