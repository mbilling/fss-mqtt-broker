# ADR 0061 — Off-loop durable appends: per-session lanes for the publish path

- **Status:** Accepted
- **Date:** 2026-08-14
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0061-off-loop-durable-appends.md](../delivery/0061-off-loop-durable-appends.md) — plan, progress, and changelog
- **Related:** [ADR 0017](0017-durable-attach-readiness.md) (the precedent: attach
  recovery moved off-loop for exactly this reason — this ADR applies its shape to the
  publish path), [ADR 0041](0041-resource-governance.md) / issue #238 (the plan/commit
  atomicity behind effect-free refusals, which this motion must preserve),
  [ADR 0057](0057-durable-outbound-inflight.md) / #124 (ack-after-durable),
  [ADR 0042 T9](0042-durable-plane-stress-harness.md) (the pending-publish obligation
  table the lanes plug into), issue #242 (the review finding).

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0061-off-loop-durable-appends.md).

## Context

The hub is a single-threaded actor: `run()` dispatches one command at a time and every
await inside a dispatch runs inline. The publish path's durable appends
(`enqueue_with_expiry` — in cluster mode a quorum replication RPC bounded at **5 s**,
`mqtt-cluster/src/repl_net.rs`) were awaited inline in that dispatch. One placement
group with a degraded follower set therefore parked the loop up to 5 s **per append**
(K×5 s for a fan-out of K subscribers in the group) and head-of-line-blocked **every
client on the node**: connects, subscribes, acks, and publishes whose subscribers live
in perfectly healthy groups. This is the 3 a.m. problem issue #242 names, and it is the
same defect shape ADR 0017 removed from the attach path.

The hard part is not the motion; it is what the inline await was silently load-bearing
for:

- **ACK-AFTER-DURABLE** (#124/ADR 0057): the durable record exists before the wire
  send, and the publisher's ack releases only after every durability obligation.
- **#238 plan/commit atomicity**: a refusal (brownout) is decided before any side
  effect, and the decide and commit passes observe the same `brownout` flag and
  routing table — which held *because* nothing interleaved with the inline awaits.
  The #238 remediation explicitly named "a fan-out moved off the hub loop" as the
  motion that silently breaks it.
- **Per-session ordering**: a session's durable queue order is replay order.
- QoS 2 exactly-once scoping, shared-subscription selection, the retained plan gate.

## Decision

ADR 0017's exact shape, applied to appends: **the decision stays on-loop, the store
I/O leaves it, and completion re-enters as a command.**

### 1. Per-SESSION append lanes

Each subscriber session gets, on first use, an **append lane**: a bounded FIFO
(`LANE_QUEUE_CAP = 256`) to a dedicated worker task holding only
`(Arc<dyn SessionStore>, hub self_tx, metrics)`. Keyed by the SUBSCRIBER's client id —
never by topic or placement group — because all of one session's durable keys live in
one group: per-session lanes give exact failure-domain isolation (a stalled group
stalls only its own sessions' lanes) *and* make per-session append order structural
(one FIFO, one serial worker). Lanes are reaped by the sweep when idle.

### 2. The freeze point: plan + submit, one synchronous span

The whole decision pass of one publish — `plan_refusal`'s brownout read, the routing
targets, the shared peek/select, the retained gate, and now the lane submissions
(`submit_append`, a synchronous `try_send`) — executes inside **one dispatch**, and
`run()` awaits each dispatch to completion, so nothing can interleave another command
between the brownout read and the last submission (the same single-owner-actor
argument the inline code relied on, now stated as the span's contract). The job
carries everything frozen: message, target, receipt-time expiry deadline (ADR 0009 §3), the planned
connection, and its on-loop continuation. The worker structurally cannot read hub
state, and its outcome vocabulary (`LaneOutcome`) **has no `Refused` variant** — a
refusal decided off-loop cannot even be expressed. A `SetBrownout` (or attach, detach,
takeover) queued behind the dispatch therefore governs the NEXT publish, never an
admitted job: every interleaving linearizes to "publish committed first, then the flag
flipped", observationally identical to the inline code. The old brownout arm and its
`debug_assert` tripwire moved verbatim to `submit_append`.

### 3. Completion re-enters the loop: `HubCommand::AppendDone`

The handler (single-threaded, like every dispatch) does all mutation: increments
`durable_writes`, sets the gate's `stored` directly (retiring the counter-snapshot
trick), performs the **post-durable live send** — the packet literally receives its
offset from the completion, so durable-before-wire holds by dataflow — and resolves
the continuation: the pending publish's new `appends_outstanding` obligation
(`try_complete_pending` gains the conjunct; `Failed` → withhold via `drop_pending`),
or a peer verdict aggregate (`(node, seq)`), so a peer hears `Stored` only after the
store actually stored.

Reconnect race: a completion whose planned connection is gone live-sends only when the
attach replay **provably did not cover the offset** (every replayed entry raises the
session's high-water; offsets are monotone) and the session is still persistent —
otherwise the durable copy is the replay's to deliver. While a connect is mid-recovery,
nothing is sent.

### 4. Backpressure: bounded, loud, order-preserving

At the lane cap the **newest** job is rejected at submit (evicting an older one would
break FIFO and orphan another publish's gate): answerable → the ack is withheld (fail
closed, the publisher retries; *not* `Refused`, which would falsely claim "stored
nowhere" while sibling appends may have stored); unanswerable → a counted drop. Both
log and count `publish_dropped{reason="append-backlog-full"}`; `append_lane_jobs` is
the pre-drop gauge. `refuse_pending` additionally may not *speak* a refusal while
`appends_outstanding > 0` — it degrades to a withhold (the named trade: a v5 publisher
racing a peer refusal against its own in-flight append loses the actionable `0x97`;
a false refusal would be a defect, a withhold is not).

### 5. Order guards beyond the lane

Any append-free send to a client whose lane has jobs in flight is routed through the
lane as a **passthrough** (no store work), so it cannot overtake an earlier append's
post-durable live send; a saturated lane sheds it rather than reorder. This covers a
`QoS` 0 send AND an **unanswerable brownout-refused delivery** — a Will or a
retained-window back-fill under brownout (#242 finding B): both accept loss
(at-most-once, or a refusal already decided), and reordering is what nothing permits.
A `QoS` 0 passthrough completion additionally re-checks at send time: if a staged
outbound-id record, an in-flight packet-id reservation, or a non-empty backlog is
pending for the client, the `QoS` 0 is **parked in the backlog** (the one per-client
ordering buffer) and drains in exact FIFO order; a `QoS` 0 parked this way is dropped,
not spilled, at detach — at-most-once dies with the connection.

Session lifecycle store work rides the lane too, so it cannot race the session's own
in-flight appends (#242 finding C): a **discard** (clean-start takeover, zero-expiry
detach, expiry sweep) serializes the durable `remove` behind every admitted append
(`LaneJob::Discard` / `LaneJob::Remove`) so a late append cannot re-create the queue
it just emptied (idle lane: a spawned off-loop remove, no ordering to defend); the
**detach spill** of a persistent session's never-sent backlog rides as one
`LaneWork::Spill` job — off-loop AND strictly behind in-flight appends, which is
exactly the pre-motion replay order. `LANE_CONTROL_HEADROOM = 16` channel slots
beyond `LANE_QUEUE_CAP` are reserved for these control jobs, so a delivery-saturated
lane still admits them; overflow beyond cap+headroom stays loud (spill entries shed
and counted `append-backlog-full`; discard falls back to a spawned remove, warned).

### 6. The second lane stage: outbound-id records and packet-id reservations

The post-durable live send itself owed two more store writes, and inlining them in
`AppendDone` (a publish-class dispatch) would re-create the stall the motion removed
(#242 finding A):

- **ADR 0057's outbound-id record** (`record_outbound`, every `QoS` 2 delivery with a
  durable offset) becomes a second lane stage: the send site stages the delivery as
  `AwaitingIdRecord` (quota reserved, packet id pinned), `records_pending` gates the
  client's backlog drain and forces later sends to queue behind it, the lane worker
  runs the write off-loop, and only the completion — holding the store's `Ok` behind
  the same exact-conn/mid-recovery fence as `AppendDone` — puts the packet on the
  wire. Durable-before-wire holds by dataflow, exactly as ADR 0057 demands; the
  failure arm is ADR 0057's relocated verbatim (entry re-queued at the backlog
  front, `publish_dropped{reason="outbound-id-write-failed"}` counted). Staged
  entries are dropped at detach and at takeover (the durable copy is the replay's to
  deliver), and acks for a staged, never-sent id are ignored — including an explicit
  `PUBCOMP` guard, so a forged PUBCOMP cannot race the in-flight record.
- **Packet-id block reservation** (`reserve_packet_ids`, once per 1024 `QoS` ≥ 1
  deliveries per session) becomes reserve-at-spent: an exhausted block defers the
  delivery to the backlog front and fires a single-flight off-loop reserve whose
  completion (`PkidBlockReserved`, classed `publish`) banks the next base and drains.

The whole send chain (`send_qos_publish` → `drain_backlog` → `outbound_record_done` →
`pkid_block_reserved`) is a plain `fn`, not `async fn` — zero awaits,
compiler-enforced.

### 7. Instrumentation: the regression tripwire

`mqttd_hub_dispatch_seconds{command=<class>}` — time on loop per dispatch, coarse
7-value class label, exponential buckets 100 µs → ~13 s (the 5 s RPC bound is
on-scale). After this ADR the publish class is plan+submit plus await-free completion
handling (µs–ms); a publish-class tail means an inline await regressed (the one named
exception: the backlog-overflow eviction truncate, see the residuals). Alert rows
live in
[OPERATIONS](../OPERATIONS.md#monitoring-for-the-operator-and-humans); sizing math in
[SIZING](../SIZING.md#what-actually-writes-to-disk-on-the-publish-path).

### 8. Shutdown posture: the lanes die with the hub, because they hold the store's lock

A worker holds an `Arc` of the session store, and a redb store is **exclusive-locked by
its handle**. That makes worker lifetime a correctness property, not hygiene: a worker
outliving its node keeps the data dir locked, and the next start fails with *"Database
already open. Cannot acquire lock."*

A node stops by having its hub task **aborted** (or by the process exiting). The loop's
`None` arm cannot fire — the hub holds a clone of its own command sender — so there is no
graceful-exit path to drain, and the abort must cascade. It does, by ownership: the hub
holds its store-touching tasks in a `tokio::task::JoinSet` (`owned_tasks`), aborting the
task drops `self`, and dropping a `JoinSet` aborts every task in it. Every handle is
released at once, exactly as the OS reclaims a killed process's files. The sweep also
polls `JoinSet::try_join_next` so finished slots do not accumulate.

**Every** task that captures the session store goes through `Hub::spawn_owned`, not
`tokio::spawn` — the lane workers, the packet-id reserve, the off-loop session removes,
the clean-start discards, the session recoveries, and the inherited-session scan. Three
of those predate this ADR and were spawned bare: they were latently exposed to the same
lock leak, and it did not bite only because they usually finish fast. That reasoning is
invalid at a full-cluster stop, where **every** store call blocks for the replication
bound — so "it finishes quickly" is exactly the assumption to distrust. The store Arc
reaches the lease store transitively (a replicated session store owns the cluster log,
which owns the lease store), which is why the symptom named `lease-store` while the
handle being leaked was the session store's.

An in-flight append is therefore **abandoned** at a crash stop, and that is the honest
trade rather than a loss: the publisher's ack was already **withheld** (fail closed, never
fabricated), and a crash is precisely the event whose torn writes the durable plane
recovers from (ADR 0044). The rejected alternative — letting the worker finish — means
waiting out a quorum append that *cannot* reach quorum during a full-cluster stop, up to
the 5 s replication bound, with the lock held the whole time.

**This was a real escape, recorded because it shows the shape of the risk.** The first
implementation spawned workers bare, so they survived the abort; every local run passed
(the drop won the race) and CI's slower
`cluster_stress::a_full_cluster_stop_start_recovers_every_acked_fact` failed on the held
lock. Before this ADR the append was awaited *inline on the loop*, so an abort killed it
for free — the off-loop motion is what turned an implicit guarantee into one that has to
be stated and owned. It is now pinned deterministically at the unit tier
(`a_crashed_hub_releases_the_store_so_the_node_can_restart`, mutation-proven against the
bare-spawn shape) rather than left to a stop/start race.

`main`'s ordering (connections drain before the durable plane closes) is unchanged; a job
racing plane shutdown fails like any store error → withheld.

## What deliberately does NOT move (named residuals, with their dispatch class)

Each residual is named with the dispatch-histogram class its await lands in — that
class is its watchdog, and the class is what the operator alert should point at.

- **Ack-path store writes** (`ack` class): `pub_ack`/`pub_comp` completion →
  `truncate_acked` (`store.ack`), `pub_rec` → `advance_outbound`, `pub_comp` →
  `clear_outbound`. A degraded group can still hold the loop through them; an
  `ack`-class tail is the signal. Moving them is future work with its own
  invariants. (ADR 0057's outbound-id writes and `reserve_packet_ids` are NOT in
  this list — §6 moved both off-loop.)
- **Recovery/replay work** (`attach` class): `finish_attach`'s `pending`/`outbound`
  reads (post-recovery, warm lease, ADR 0017), the replay's `truncate_acked`, the
  orphaned-QoS 2-id `clear_outbound`, and the `set_session_expiry(None)` deadline
  clear on a persistent attach.
- **Session-expiry persistence at detach** (`control` class): the finite-expiry
  detach's `set_session_expiry(Some(deadline))` write (ADR 0009 §3).
- **Backlog-overflow eviction truncate** (`publish` class, narrow): evicting the
  oldest entry from a flow-control backlog at `MAX_BACKLOG` (10 000) awaits
  `truncate_acked` inline — the one remaining publish-class store await, reachable
  only when a single session's in-memory backlog overflows.
- **The local (non-durable) `retained.set`** stays on-loop: local fsync, no 5 s RPC.
  The durable-retained authority commit was already off-loop (ADR 0037).
- **Bounded-replay corner:** a session with more backlog than one replay window that
  reconnects *while an append is in flight* can receive that completion's live send
  ahead of not-yet-replayed older entries — the same overtake a fresh live publish
  already exhibits today; at-least-once holds, strict order across the replay-window
  boundary does not. Recorded, not fixed.
- **Beyond cap + headroom fallbacks** (loud, nearly unreachable): a detach spill that
  cannot enter the lane sheds its entries (counted `append-backlog-full`); a discard
  that cannot enter falls back to a spawned remove, which can race a still-in-flight
  append (one-ghost-message window, warned).

## Named trades

- **Per-session `QoS` 2 sends remain serial**: one 5 s-bounded outbound-id record
  round-trip per message before its wire send — unchanged from the inline era; the
  serialization moved from the shared loop to the per-session lane. The observable
  is per-session `QoS` 2 delivery latency plus
  `publish_dropped{reason="outbound-id-write-failed"}`, not a dispatch tail.
- **Once-per-1024 packet-id-reserve deferral** per session: the delivery that
  exhausts a block waits one off-loop reserve round-trip (follow-up: low-water
  prefetch would hide it).
- **Record jobs share `LANE_QUEUE_CAP` with appends**: a record-heavy session spends
  the same 256-job budget.
- **A `QoS` 0 parked for ordering is dropped, not spilled, at detach**
  (at-most-once dies with the connection).
- **A detach spill beyond cap + headroom sheds** (counted `append-backlog-full`).

## Consequences

- **Good:** a degraded placement group delays only its own sessions' publishes and
  deliveries (bounded: 5 s per append or outbound-id record, FIFO per session,
  256-job lane); connects, subscribes, and other groups' publishes are unaffected.
  Hub dispatch time is independent of replication latency on the publish path —
  online delivery included (§6) — with the ack-class and other residuals named
  above; and it is now measured. Every #238/#124 invariant is preserved and newly
  pinned by tests (see delivery doc).
- **Cost:** one extra loop round-trip per durable delivery (lane push → `AppendDone`),
  and a second one per `QoS` 2 delivery (record stage → `outbound_record_done`);
  `deliver_latency_seconds` is re-scoped to the on-loop fan-out only. New state:
  lanes, `appends_outstanding`, `records_pending`, the banked packet-id base, the
  peer-verdict aggregate. Memory bound: sessions × 256 jobs, payloads refcounted
  ([SIZING](../SIZING.md)).
- **Risk:** the freeze span is a discipline ("no await between the brownout read and
  the last submission") that a future edit could break silently; the `submit_append`
  `debug_assert`, the `Refused`-free `LaneOutcome` vocabulary, and the brownout-flip
  test are its tripwires.

## Alternatives considered

- **A fixed pool of N hashed lanes** (design B's sketch): fewer tasks, but hash
  collisions re-create cross-session HOL between unrelated sessions and make the
  isolation claim probabilistic; per-session lanes are exact, and their count is
  already bounded by the session quota. Rejected.
- **Lanes keyed by placement group**: breaks per-session ordering the moment one
  subscriber's messages ride two groups' speeds, and needs placement knowledge in the
  hub. Rejected (named in both designs as the wrong key).
- **Blocking `send` into the lane instead of `try_send`**: re-creates the inline stall
  the moment a lane fills. Rejected; reject-newest + withhold is the fail-closed
  answer the store's own error path already taught publishers.
- **Moving the whole publish dispatch off-loop**: it mutates routing, shared cursors,
  pending state — would need locks the actor model exists to avoid. Rejected.
