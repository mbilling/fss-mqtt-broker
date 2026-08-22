# 0074. The subscriber-ack truncate leaves the hub loop's critical path

Date: 2026-08-23
Status: Accepted

## Context

Two independent hardware draws on the v1.0.3 curve pinned durable throughput to
the disk's **barrier rate**, not its barrier *capacity*:

| draw | durable msg/s | slowest disk barriers/s | ratio |
|---|---|---|---|
| v1.0.3, 1 node | 3,406 | 2,575 | 1.32 |
| v1.0.3, 3 nodes | 934 | 706 | 1.32 |
| v1.0.2, 1 node | 2,791 | 2,072 | 1.35 |
| v1.0.2, 3 nodes | 1,753 | 2,168 | 0.81 |

Meanwhile the ADR 0071 group-commit writer ran at ~3,700 barriers/s committing
only **2.29 ops per batch** — capacity to spare, starved of concurrent ops. The
ratio glued to ~1× barrier rate across draws is the signature of **one
serialized barrier-wait per message on the critical path**, and it is the
subscriber-ack truncate (ADR 0061's named residual, quantified during 0071's
delivery): every acknowledged delivery drives `Hub::truncate_acked` →
`store.ack(client, up_to)` — a quorum-replicated, fsync'd log truncation —
**awaited inline on the hub loop**. Group commit batches the fsync but cannot
help a waiter: the loop pays one truncate round-trip per message, one message
at a time, so msg/s ≈ barrier rate regardless of batch depth.

Two facts make the await removable:

1. **The failure path already documents replay tolerance.** `truncate_acked`
   on error: *"Not fatal: the entries stay in the log and are replayed on the
   next resume. A duplicate at QoS 1 is spec-legal; losing one would not be."*
   An await whose failure is tolerated by replay is not load-bearing for
   correctness — it is only load-bearing for latency, in the wrong direction.
2. **The truncate is a monotonic watermark.** `Inflight::advance_ack` yields
   the contiguous acked prefix; `ack(up_to)` deletes everything at or below
   it. Later watermarks supersede earlier ones, lower ones are no-ops —
   perfectly coalescible and idempotent, safe to run concurrently with appends
   (which occupy strictly higher offsets than any acked delivery).

## Decision

**QoS 1 ack truncation is coalesced into a per-session watermark and flushed
off-loop; the publisher/subscriber ack paths never wait on it.**

1. `Hub::truncate_acked` becomes synchronous bookkeeping: advance the
   watermark, hand `(client, up_to)` to the hub's **truncate flusher** — a
   task owned by the hub's task set (aborted with the hub), holding one
   `latest: HashMap<ClientId, Offset>` merged from the channel (max wins — a
   burst of N acks flushes as ONE truncate at the final watermark) and
   flushing with small bounded concurrency. Flush failures keep today's
   debug-level tolerance verbatim.
2. **QoS 2 completion keeps the synchronous truncate** (`pub_comp` calls the
   old inline path). QoS 2's exactly-once rests on the durable outbound
   id-state (ADR 0057); the pre-existing crash window between `clear_outbound`
   and the truncate stays exactly the width it is today — this ADR widens no
   QoS 2 window. QoS 2 is also not the hot path this ADR exists for.
3. **Bounds, stated:** the flusher's map holds at most one offset per session;
   per-session disk lag is bounded by the flush cadence (milliseconds at the
   measured truncate latency), and entries above the flushed watermark are
   exactly the store's existing replay inventory, governed by the existing
   store watermarks (ADR 0041). A dropped flush (shutdown) self-heals: the
   entries replay at next resume, the subscriber re-acks, the watermark
   re-advances — the same recovery the error path has always priced in.

### What changes for a client, honestly

The crash-replay window for *acknowledged* deliveries widens from ~one
truncate round-trip to ~one flush cadence: a subscriber that acks and then
sees the broker crash may be redelivered a few more already-acked QoS 1
messages on resume than before. That is at-least-once delivery working as
specified [MQTT-4.3.2]; no acknowledged message can be *lost* by this change,
because the entries being kept longer are kept *durably*. QoS 2 windows are
untouched (see Decision 2).

## Consequences

- The hub loop's ack cost drops to bookkeeping (the dispatch histogram's `ack`
  class measures it); appends actually pipeline, so the 0071 writer sees the
  concurrency it was built for. Predicted, falsifiably: durable msg/s
  decouples from the barrier rate and re-couples to writer capacity — the
  measured A/B is this ADR's delivery evidence, and if the number does not
  move, this ADR is wrong and says so.
- Disk holds acked entries for milliseconds longer; store-size honesty is
  unchanged (the entries were always resident until truncation — only the
  cadence moves).
- The remaining on-loop durable waits are QoS 2's phase writes (ADR 0057,
  deliberately kept) — a future ADR may batch those with the same watermark
  discipline if QoS 2 throughput ever matters.

## Alternatives considered

- **Fire-and-forget per-ack truncates (no coalescing):** removes the wait but
  floods the writer with one op per ack — the writer's batches absorb it, but
  it is O(messages) ops for O(sessions) information. The watermark is strictly
  better.
- **Truncate on a timer only (no channel):** simplest, but couples cleanup
  latency to the sweep tick (seconds) for no gain; the channel keeps cadence
  at milliseconds under load and idle-quiet otherwise.
- **Detaching QoS 2's truncate too:** rejected here — it shares a path with
  the exactly-once id-state and widening that window needs its own analysis;
  the hot path does not need it.
- **Doing nothing:** the measured ceiling — durable throughput pinned to the
  slowest disk's barrier rate with the group-commit writer idling — is the
  scale curve's single largest per-node loss, twice measured.

## Delivery

[docs/delivery/0074-detached-ack-truncate.md](../delivery/0074-detached-ack-truncate.md):
T1 the watermark flusher + detached QoS 1 path (+ parked-store and coalescing
tests, replay honesty covered by the existing truncation-prefix suites);
T2 the measured A/B (`durable_bench`, and the next curve run's durable rows).
