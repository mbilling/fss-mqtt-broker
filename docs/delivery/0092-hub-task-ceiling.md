---
adr: "0092"
title: "The hub task bounds a node: a pinned thread, less work per message, consumer PUBACKs off the loop"
adr_status: Accepted
tasks:
  - id: 0092-T1
    title: "The hub model as a script: µs per publish and per consumer PUBACK, and the time lost to wake-ups, from a rig run's scrapes; the report cites it"
    status: done
    issue: 1159
    date: 2026-10-31
    evidence: "#1165: bench/scale/rig2/hub-model.py; test-hub-model.py (9 tests, in CI); verdict.py cites it under every ladder of report.md; the five AWS runs' baseline under Progress, and the T2 run read with it."
    notes: "Size S. bench/scale/rig2/hub-model.py reads a run's 2 s scrapes (mqttd_hub_dispatch_seconds by class, the received counter, mqttd_runtime_wake_seconds, mqttd_hub_queue_depth) over each round's measured window and prints, per step, the busiest node's busy share, the hub-µs per received message, the publish and ack (consumer PUBACK) µs with their dispatches per message, the wake probe's lateness, the queue and the saturated nodes, then the loop's busy ceiling; verdict.py cites it under every ladder of report.md; test-hub-model.py (6 tests) runs in CI. The yardstick for T2–T4; the five AWS runs' figures are under Progress."
  - id: 0092-T2
    title: "The hub loop on its own OS thread, pinned to a core where the host allows (D1): MQTTD_HUB_THREAD and MQTTD_HUB_CORE, named on /statusz and a gauge; the shared runtime keeps everything else"
    status: done
    issue: 1160
    date: 2026-10-31
    evidence: "#1167: Hub::spawn_dedicated, node.hub_thread / MQTTD_HUB_THREAD and node.hub_core / MQTTD_HUB_CORE, mqttd_hub_thread_info and the /statusz hub block, five tests; measured on run h3-20261031T155400Z against the baseline h3-20261030T171933Z (a regression, under Progress): shipped off by default, D1 withdrawn by ADR 0092 Amendment 1."
    notes: "Size M. The maintainer's item 1. The three tokio::spawn(hub.run()) sites in crates/mqttd/src/main.rs; a std thread with a current-thread runtime or LocalSet; rustix sched_setaffinity (no unsafe); mqtt-config, CONFIGURATION, OPERATIONS. Built: Hub::spawn_dedicated (a std thread with a current-thread runtime, rustix sched_setaffinity for the pin), every hub-internal spawn routed to the shared runtime through Hub::spawner, node.hub_thread / MQTTD_HUB_THREAD and node.hub_core / MQTTD_HUB_CORE, mqttd_hub_thread_info and the /statusz hub block, placed in one place_hub in main.rs. Measured with T1 on one m6i.8xlarge (run h3-20261031T155400Z, see Progress): a regression, the knee 68,600 against 88,700, so the thread ships OFF by default and D1 is withdrawn by the ADR's Amendment 1. The pinned arm and the Genoa host were not run: they cannot change the mechanism."
  - id: 0092-T3
    title: "Less work per message on the hub loop, in the order the frame-pointer profile gives (D2): the hasher, one clock read per dispatch, batched metric observes, the payload and per-delivery allocations; no behaviour change"
    status: in-progress
    issue: 1161
    notes: "Size M–L. The maintainer's item 2. The frame-pointer profile (run h3-20261031T205637Z, under Progress) ordered the work: the per-command dispatch future's copies (memcpy, 14% of the hub task), the shared-subscription plan (11%), SipHash (10%), allocations (7–9%), the sub-id walk per delivery (5%), the metric label building (~1.5%); clock reads are 0.5% and stay. First batch: the hot commands on a small dispatch future and the rare arms boxed (dispatch_timed 5,832 → 2,488 B, the PUBACK with no future at all; pinned by a test), the labelled metric handles resolved once, the sub-id walk only for subscriptions that carry one, the elision write key built only while a write is in flight, the emptied per-session hold queues pooled, the empty-backlog drain one lookup — measured on run h3-20261031T215218Z: the knee 92,400 → 105,500 msg/s on one m6i.8xlarge, a publish 6.7 → 5.3 µs at 67,500 msg/s (under Progress). Second batch (the hasher, the trie's wildcard children as fields) measured on run h3-20261031T225644Z: the knee 105,500 → 112,100, a publish 5.3 → 4.8 µs at 67,500 msg/s. Target about 7 → about 5 µs per publish on m6i, each batch re-fitted with T1 on the rig; hub tests green."
  - id: 0092-T4
    title: "Consumer PUBACKs complete on the connection task (D3): it owns the session's outbound in-flight state; the hub learns of acknowledgements in batches only for durable truncation, elision discharge and the obligation ledger; QoS 2 unchanged"
    status: done
    issue: 1162
    notes: "Size L. The maintainer's item 3. hub/mod.rs dispatch_pub_ack and complete_pending; the connection task's outbound state. Tests first: ADR 0089 T1's order tests, inflight_durability (#124), cluster_proc and false_dead_delivery with elision on. Done when the ack class is below 5% of hub time by T1 and no acknowledged message is lost on the fault suites. Plan (2026-11-01), two steps each measured with T1: (1) the connection task hands the hub its PUBACKs in batches — one HubCommand::PubAcks per drained read buffer, so no ack waits — and the hub completes a batch in one dispatch with the per-session lookups hoisted out of the loop, the truncation and the drain once per batch; the state stays where it is, so no invariant moves. (2) Planned 2026-11-01 from step 1's reading — a read carries one ack on h3, so a batch cannot amortize the ack's command: its cost is the loop's ingest, two clock reads and two histograms, five cold lookups (the hold queue, the backoff, the in-flight entry, the online entry twice) and the publisher's wake. Two slices, each measured with T1 on one node and three. (2a) A held delivery's obligation settles on the subscriber's connection task: the hub hands the task a Hold with the held packet (hub/hold.rs); the PUBACK settles it there — one compare-and-swap decides between the ack and the window's fallback write, never both — and fires the publisher's answer through the PublishGate the publish's pending entry shares with every hold of its fan-out, armed by the hub once nothing else is owed; the hub retires the armed entry at the arming and drops the hold when the ack batch reaches it, on its existing per-ack work; a peer's forward still settles on the hub, its verdict being the hub's to send. A first cut that told the hub of each firing through a channel and pruned settled holds at their deadlines measured 105,500 on run h3-20261101T105009Z (−6% on T3): the ack dispatch fell 1.8 → 0.7 µs, but 1.2 µs moved into the loop's own passes, the busy ceiling fell 0.82 → 0.71 and the queue burst to 17,000 at 60% busy — the second cut moves nothing out of the dispatch's timing, and measured 100,300 on run h3-20261101T121633Z with the same ceiling: the loop gets 60% of the wall-clock where T3's got 76%, read as the publisher's wake from the subscriber's task landing in that worker's LIFO slot, unstealable, behind which the hub waits. The same binary with the hub on its own thread (MQTTD_HUB_THREAD=true, run h3-20261101T131207Z) measured 78,100: the loop's share came back (ceiling 0.88) but every hand-off out of the hub became a remote wake, 10.4 µs per message. D3's discharge off the loop withdrawn (Amendment 3); #1175 not merged; step 1 stays. (2b) The in-flight table, the packet-id block, the receive-maximum quota, the backlog and the truncation watermark move to the connection task and are handed back at detach; then no QoS 1 PUBACK reaches the loop on the local path at all. Done 2026-11-01 with step 1 (#1174) as its content: 2b is not built, since the state move would still issue the publisher's wake off the loop; the acceptance's 5% is unreachable on this runtime (Amendment 3), and the ack's remaining 1.8 µs of 6.9 on the loop is D5's (#1163). Evidence: the three runs under Progress and in the evidence table."
  - id: 0092-T5
    title: "The control-plane/data-plane ADR (D5): K data shards per node and one control task, with the two-shard hand-off cost measured before it is written; not started before T2–T4 are measured"
    status: in-progress
    issue: 1163
    notes: "Size M for the ADR, after a two-shard prototype of size M. The maintainer's item 4, sequenced after items 1–3 (2026-10-31). Contents in #1163: shard ownership by placement group, immutable snapshots from the control task, hand-off and completion cost, hot-group dealing and # subscribers, shard failure modes, the node's ceilings as functions of ADR 0091's numbers with the ENA packet-rate ceiling from netprobe. Started 2026-11-01 (T2–T4 measured; the maintainer's go). Prototype plan: two data shards per node as two hub loops behind one HubTx demultiplexer keyed by the client's placement group — a connection attaches to its shard, a publish enters on the publisher's shard; the filter trie, the shared groups and the placement as immutable snapshots behind an arc-swap, published by one control task that owns subscribe, unsubscribe, attach and detach; a publish matched on its own shard, its own sessions delivered there, the rest handed to the other shard as one ShardDeliver command (the message and its clients), whose durable completions come back as one ShardDone — two hand-offs per cross-shard message; read with T1's model per shard (busy, µs per message, a hand-off class) on the h3 shape and on fanin-qos1 against main, the shards unpinned and pinned to disjoint cores, since Amendment 3's floor says a hand-off's receiver runs next only on the sender's worker. The prototype is done when the cost per hand-off and the two-shard knee against one loop's are in this doc, whatever the numbers; the ADR is written after. Baseline first: fanin-qos1 on main, one node."
---

# Delivery 0092 — the hub task bounds a node

**ADR:** [docs/adr/0092-hub-task-ceiling.md](../adr/0092-hub-task-ceiling.md)

The plan, progress, and changelog for ADR 0092. Task status lives in the frontmatter
above; the table below is generated from it.

## Status

<!-- status-table:0092 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0092-T1 | ✅ done | [#1159](https://github.com/mbilling/fss-mqtt-broker/issues/1159) | 2026-10-31 | "#1165: bench/scale/rig2/hub-model.py; test-hub-model.py (9 tests, in CI); verdict.py cites it under every ladder of report.md; the five AWS runs' baseline under Progress, and the T2 run read with it." |
| 0092-T2 | ✅ done | [#1160](https://github.com/mbilling/fss-mqtt-broker/issues/1160) | 2026-10-31 | "#1167: Hub::spawn_dedicated, node.hub_thread / MQTTD_HUB_THREAD and node.hub_core / MQTTD_HUB_CORE, mqttd_hub_thread_info and the /statusz hub block, five tests; measured on run h3-20261031T155400Z against the baseline h3-20261030T171933Z (a regression, under Progress): shipped off by default, D1 withdrawn by ADR 0092 Amendment 1." |
| 0092-T3 | 🚧 in-progress | [#1161](https://github.com/mbilling/fss-mqtt-broker/issues/1161) | — | "Size M–L. The maintainer's item 2. The frame-pointer profile (run h3-20261031T205637Z, under Progress) ordered the work: the per-command dispatch future's copies (memcpy, 14% of the hub task), the shared-subscription plan (11%), SipHash (10%), allocations (7–9%), the sub-id walk per delivery (5%), the metric label building (~1.5%); clock reads are 0.5% and stay. First batch: the hot commands on a small dispatch future and the rare arms boxed (dispatch_timed 5,832 → 2,488 B, the PUBACK with no future at all; pinned by a test), the labelled metric handles resolved once, the sub-id walk only for subscriptions that carry one, the elision write key built only while a write is in flight, the emptied per-session hold queues pooled, the empty-backlog drain one lookup — measured on run h3-20261031T215218Z: the knee 92,400 → 105,500 msg/s on one m6i.8xlarge, a publish 6.7 → 5.3 µs at 67,500 msg/s (under Progress). Second batch (the hasher, the trie's wildcard children as fields) measured on run h3-20261031T225644Z: the knee 105,500 → 112,100, a publish 5.3 → 4.8 µs at 67,500 msg/s. Target about 7 → about 5 µs per publish on m6i, each batch re-fitted with T1 on the rig; hub tests green." |
| 0092-T4 | ✅ done | [#1162](https://github.com/mbilling/fss-mqtt-broker/issues/1162) | — | "Size L. The maintainer's item 3. hub/mod.rs dispatch_pub_ack and complete_pending; the connection task's outbound state. Tests first: ADR 0089 T1's order tests, inflight_durability (#124), cluster_proc and false_dead_delivery with elision on. Done when the ack class is below 5% of hub time by T1 and no acknowledged message is lost on the fault suites. Plan (2026-11-01), two steps each measured with T1: (1) the connection task hands the hub its PUBACKs in batches — one HubCommand::PubAcks per drained read buffer, so no ack waits — and the hub completes a batch in one dispatch with the per-session lookups hoisted out of the loop, the truncation and the drain once per batch; the state stays where it is, so no invariant moves. (2) Planned 2026-11-01 from step 1's reading — a read carries one ack on h3, so a batch cannot amortize the ack's command: its cost is the loop's ingest, two clock reads and two histograms, five cold lookups (the hold queue, the backoff, the in-flight entry, the online entry twice) and the publisher's wake. Two slices, each measured with T1 on one node and three. (2a) A held delivery's obligation settles on the subscriber's connection task: the hub hands the task a Hold with the held packet (hub/hold.rs); the PUBACK settles it there — one compare-and-swap decides between the ack and the window's fallback write, never both — and fires the publisher's answer through the PublishGate the publish's pending entry shares with every hold of its fan-out, armed by the hub once nothing else is owed; the hub retires the armed entry at the arming and drops the hold when the ack batch reaches it, on its existing per-ack work; a peer's forward still settles on the hub, its verdict being the hub's to send. A first cut that told the hub of each firing through a channel and pruned settled holds at their deadlines measured 105,500 on run h3-20261101T105009Z (−6% on T3): the ack dispatch fell 1.8 → 0.7 µs, but 1.2 µs moved into the loop's own passes, the busy ceiling fell 0.82 → 0.71 and the queue burst to 17,000 at 60% busy — the second cut moves nothing out of the dispatch's timing, and measured 100,300 on run h3-20261101T121633Z with the same ceiling: the loop gets 60% of the wall-clock where T3's got 76%, read as the publisher's wake from the subscriber's task landing in that worker's LIFO slot, unstealable, behind which the hub waits. The same binary with the hub on its own thread (MQTTD_HUB_THREAD=true, run h3-20261101T131207Z) measured 78,100: the loop's share came back (ceiling 0.88) but every hand-off out of the hub became a remote wake, 10.4 µs per message. D3's discharge off the loop withdrawn (Amendment 3); #1175 not merged; step 1 stays. (2b) The in-flight table, the packet-id block, the receive-maximum quota, the backlog and the truncation watermark move to the connection task and are handed back at detach; then no QoS 1 PUBACK reaches the loop on the local path at all. Done 2026-11-01 with step 1 (#1174) as its content: 2b is not built, since the state move would still issue the publisher's wake off the loop; the acceptance's 5% is unreachable on this runtime (Amendment 3), and the ack's remaining 1.8 µs of 6.9 on the loop is D5's (#1163). Evidence: the three runs under Progress and in the evidence table." |
| 0092-T5 | ⬜ planned | [#1163](https://github.com/mbilling/fss-mqtt-broker/issues/1163) | — | "Size M for the ADR, after a two-shard prototype of size M. The maintainer's item 4, sequenced after items 1–3 (2026-10-31). Contents in #1163: shard ownership by placement group, immutable snapshots from the control task, hand-off and completion cost, hot-group dealing and # subscribers, shard failure modes, the node's ceilings as functions of ADR 0091's numbers with the ENA packet-rate ceiling from netprobe." |
<!-- /status-table:0092 -->

## Plan

The order is the maintainer's: the model first, because every other task is judged by it;
then the three changes to one hub loop, smallest and best-measured first; then the ADR for
several loops, written only when the profile after those three says what is left.

- **T1, the model as a script.** The fit busy = load × (a per publish + b per consumer
  PUBACK) and the share of the hub's time lost to wake-ups, from a run's 2-second scrapes,
  printed per node and per step and cited by the report. Acceptance: a test on a recorded
  run; the numbers for the five AWS runs recorded under Progress.
- **T2, the dedicated pinned thread.** The hub on a std thread of its own, pinned when
  configured, with the rest of the node on the shared runtime. Acceptance: hub tests
  unchanged; one `m6i.8xlarge` and one `m7a.8xlarge` measured before and after on the rig,
  with T1's figures for both.
- **T3, less work per message.** The frame-pointer profile, then the items in its order.
  Acceptance: each change re-fitted with T1 on the rig; the per-node knee on one
  `m6i.8xlarge` re-measured; no behaviour change.
- **T4, PUBACKs off the loop.** The connection task completes deliveries; the hub is told in
  batches where it must act. Acceptance: ADR 0089 T1's order tests, `inflight_durability`,
  `cluster_proc` and `false_dead_delivery` green with elision on; the `ack` class below 5%
  of hub time; the knee re-measured.
- **T5, the split as its own ADR.** Written after T2–T4 are measured, from a two-shard
  prototype's hand-off cost. Not started before.

The README's per-node figures move only by the rig's results, task by task, through
`docs/benchmarks/RIG2-RESULTS.md` and `docs/benchmarks/data/headline.csv`.

## Progress

**Baseline (T1),** `bench/scale/rig2/hub-model.py` over the AWS runs of 2026-10-30 and the
bisect of 2026-10-31 (`docs/benchmarks/RIG2-RESULTS.md`). At each run's highest carried
step: the busiest node's busy share, the hub-µs per received message (every class), the
mean µs of a `publish` dispatch and of an `ack` (consumer PUBACK) dispatch, and the wake
probe's mean lateness. The ceiling is the highest busy share a node reached in a step
whose queue did not keep up; what is left of wall-clock there is the loop's own cost:
ingest, the yield it takes every 64 commands and the wake it then waits for, migrations.

| shape | highest carried | busy | µs/msg | publish µs | ack µs | wake µs | busy ceiling | not dispatching |
|---|---|---|---|---|---|---|---|---|
| 1 × `m6i.8xlarge` (Ice Lake), one copy | 88,700 | 0.81 | 9.1 | 7.0 | 2.1 | 24 | 0.83 | 17% |
| 1 × `m7a.8xlarge` (EPYC Genoa), one copy | 69,700 | 0.57 | 8.1 | 6.2 | 1.9 | 360 | 0.70 | 30% |
| 3 × `m6i.8xlarge` | 270,800 | 0.81 | 9.0 | 7.0 | 1.9 | 28 | 0.83 | 17% |
| 6 × `m6i.4xlarge`, first ladder | 337,500 | 0.75 | 12.7 | 9.7 | 2.9 | 30 | 0.82 | 18% |
| 6 × `m6i.4xlarge`, bisect | 390,300 | 0.82 | 12.5 | 9.7 | 2.7 | 40 | 0.82 | 18% |
| 1 × `m6i.8xlarge`, main `d4afe97d` re-measured (`h3-20261031T194739Z`) | 92,400 | 0.82 | 8.8 | 6.8 | 2.0 | 25 | 0.83 | 17% |
| 1 × `m6i.8xlarge`, **T3 first batch** (`h3-20261031T215218Z`) | 105,500 | 0.79 | 7.5 | 5.6 | 1.9 | 37 | 0.83 | 17% |
| 1 × `m6i.8xlarge`, **T3 second batch** (`h3-20261031T225644Z`) | 112,100 | 0.78 | 6.9 | 5.1 | 1.8 | 53 | 0.82 | 18% |
| 3 × `m6i.8xlarge`, **T3 first batch** (`h3-20261031T234941Z`) | 299,300 | 0.81 | 8.1 | 6.2 | 1.9 | 31 | 0.83 | 17% |
| 3 × `m6i.8xlarge`, **T3 second batch** (`h3-20261101T012140Z`) | 306,400 | 0.78 | 7.6 | 5.6 | 1.9 | 37 | 0.80 | 20% |
| 1 × `m6i.8xlarge`, **T4 step 2a, first cut** (`h3-20261101T105009Z`) | 105,500 | 0.62 | 5.8 | 5.1 | 0.7 | 46 | 0.71 | 29% |
| 1 × `m6i.8xlarge`, **T4 step 2a, second cut** (`h3-20261101T121633Z`) | 100,300 | 0.64 | 6.3 | 5.3 | 1.0 | 70 | 0.71 | 29% |
| 1 × `m6i.8xlarge`, **T4 step 2a, second cut, hub on its own thread** (`h3-20261101T131207Z`) | 78,100 | 0.82 | 10.4 | 5.2 × 1.6 | 2.0 | 6,868 | 0.88 | 12% |

Two readings the script settles. The Genoa node's loss is 30% at its ceiling, not the
41% read off its carried step (0.57 busy): its ceiling rises with the offer, from 0.57 at
69,700 to 0.70 at 101,300, which a fixed share would not do. And the wake probe names the
mechanism: every 64 commands the loop yields, and each yield waits 24–50 µs on Ice Lake
and 200–360 µs on Genoa, against 0.6 ms of dispatching between yields. T2 removes that
wait by giving the loop a runtime with nothing else to run.

**The hub task's profile (T3),** run `h3-20261031T205637Z`: one `m6i.8xlarge`, a
frame-pointer build of main `19f36004`, `perf record --call-graph fp` at 999 Hz for 25 s
inside the measured window of two one-round steps, 67,500 and 84,400 msg/s (both carried,
at ack p99 3.1 and 119 ms). 97,159 samples at the knee, 20,732 of them with the hub task
on the stack (21%); the shares below are of those, and the 67,500 step reads the same
within a point, so this is work per message, not a deep queue. What the loop does per
command: `dispatch` 84%, of which `dispatch_publish` 65% (`route_publish` 64%, `publish`
54%, `deliver_shared` 52% — the h3 workload is `$share` groups — `deliver_to_client_noting`
38%, `send_qos_publish` 19%, `send_from` 8%) and `dispatch_pub_ack` 17% (answering the
publisher through `try_complete_pending` 8%, the oneshot wake 1.8%). By leaf: `memcpy`
13.5% — the dispatch future is built and moved once per command, and it is 5,832 B
(`dispatch` 5,264 B, from its attach, cluster and sweep arms; `dispatch_publish` 1,928 B;
`route_publish` 1,400 B), plus the 256 B command's own moves and the `PendingOut` and
`PendingPublish` removals; SipHash 10% — the shared table's `&str` lookups 1.9%,
`expire_elided` 1.4%, the `ClientId` maps the rest; malloc/free 6.9% — `Box<Outgoing>`
1.1%, the `Message` topic copies 0.9%, the shared matcher's `Vec<&str>` 0.7%, the plan's
scratch 0.65%, the per-session hold queue rebuilt on every publish 0.55%; `plan_shared`
11% inclusive; `matching_sub_ids` 5.3% — a topic walk over every subscription of the
subscriber for identifiers it does not carry; the prometheus observes 2.1% plus about
1.5% of label `String`s and family lookups; the wakes of the two connection tasks 3.4%;
clock reads 0.5%. Also on the hub's stacks: the kernel's network softirqs 8.6%, the
receive path running on whichever core the loop happens to be on. The order of the work
is that list, largest first; the clock reads the plan named are not worth a change.

**T3, the evidence per batch.** Every batch is measured on both shapes the README cites,
one `m6i.8xlarge` and three, against the previous row; a batch's run and its figures are
the row, and a cell is filled only by a run.

| step | binary | 1 node: knee, publish µs at 67,500 | 3 nodes: knee | runs |
|---|---|---|---|---|
| main before T3 | `19f36004` / `d4afe97d` | 88,700 / 92,400; 6.6 / 6.7 µs | 270,800 | `h3-20261030T171933Z`, `h3-20261031T194739Z`; `h3-20261030T183536Z` |
| batch 1: the dispatch future, metric handles, the sub-id walk, the elision key and queues, the empty-backlog drain (#1170) | `1db8c42e` | 105,500 (+14%); 5.3 µs | 299,300 (+10.5%), ack p99 270 ms; a publish 7.0 → 6.2 µs at the knee | `h3-20261031T215218Z`; `h3-20261031T234941Z` |
| batch 2: the hasher, the trie's wildcard children as fields (#1171) | `1f58398b` | 112,100 (+6% on batch 1, +21% on main); 4.8 µs | 306,400 (+2.4% on batch 1, +13% on main), ack p99 92 ms; a publish 6.2 → 5.6 µs at the knee | `h3-20261031T225644Z`; `h3-20261101T012140Z` |
| T4 step 1 + 2a, first cut: the discharge off the loop, the entry retired through a channel, holds pruned at their deadlines (#1175, `356c6c2a`) | `356c6c2a` | 105,500 (−6% on batch 2); 5.3 µs | not run: a regression is not carried to three nodes | `h3-20261101T105009Z` |
| T4 step 1 + 2a, second cut: the entry retired at the arming, the hold dropped with the ack batch, the bounds on the hub (#1175, `f954a6fe`) | `f954a6fe` | 100,300 (−10% on batch 2); 5.3 µs | not run: held, see the reading below | `h3-20261101T121633Z` |
| T4 step 1 + 2a, second cut, with the hub on its own thread (`MQTTD_HUB_THREAD=true`, T2's mechanism) | `f954a6fe` | 78,100 (−30% on batch 2); 5.2 µs × 1.6 dispatches | not run | `h3-20261101T131207Z` |

**T4 step 2a, first cut, measured** (run `h3-20261101T105009Z`, one `m6i.8xlarge`, the h3
ladder, binary `356c6c2a`): the mechanism worked — 50.8 million deliveries discharged on
the connection tasks over the run, 78 fallback writes — and the `ack` dispatch fell
from 1.8 to 0.7 µs, the hub-µs per message from 7.2 to 5.8 at 105,500. The knee still
fell, 112,100 → 105,500, because the work had moved rather than gone: retiring each
fired gate's entry through a channel and pruning settled holds at their deadlines ran
in the loop's own passes, outside the dispatch timing, and the busy ceiling fell from
0.82 to 0.71 (29% of the loop's time not dispatching); the bounds counters shared with
the connection tasks added 0.3–0.5 µs to a publish; and the queue burst to 17,000
commands at 60% busy where T3's stayed under 500, with the pending-publish cap
evicting answered entries at every rung above the knee. The second cut retires the
armed entry at the arming (fallback writes carry the gate, `AppendThen::Hold`), drops
the hold from the queue when the ack batch reaches the hub, on the per-ack work that
exists anyway, and keeps the bounds on the hub: nothing is deferred to a pass of its
own, and the connection task touches only the gate and the hold's state.

**T4 step 2a, second cut, measured** (run `h3-20261101T121633Z`, one `m6i.8xlarge`, the
h3 ladder, binary `f954a6fe`): the knee 100,300, bracketed at 102,900 — 10% under T3's
112,100 — with the same busy ceiling of 0.71. The dispatch cost is lower than T3's at
every rung (6.3 µs per message at 100,300 against 7.2; the `ack` dispatch 1.0 µs against
1.8), so the loss is not in the dispatching: at 105,500 the loop dispatched 195,000
commands a second at 60% busy while its queue grew by 15,000 every two seconds from the
step's first second, until the cap at 235,800 and the spiral behind it (publishers time
out, reconnect, open a takeover window; every later publish is then kept for the settle
window and the pending-publish cap evicts them as `pending-cap-replay`). The loop is
not overloaded; it is not being run: it gets about 60% of the wall-clock where T3's got
76%, in both cuts alike. The one thing both cuts do and T3 does not is to wake the
publisher from the subscriber's connection task: a wake from a running task lands in
that worker's LIFO slot and runs next, unstealable, so when the hub's worker runs a
subscriber task after the hub's yield it runs the publisher behind it too, and the hub
comes back later; when the hub woke both (T3) they sat in the worker's stealable queue
and the idle workers took most of them. The state move (slice 2b) would not change
this.

**The same binary with the hub on its own thread, measured** (run
`h3-20261101T131207Z`, `MQTTD_HUB_THREAD=true`, T2's mechanism; the gauge confirms
`thread="dedicated"`): the knee 78,100, bracketed at 80,200 — 30% under T3. No
subscriber task runs on the hub's thread there, and the loop's share came back (busy
0.82 at the knee, the ceiling 0.88), but what the loop then does per message costs
more than the discharge saved: 10.4 µs per received message against 6.3 on the shared
runtime, a publish dispatched 1.6 times per message (the retransmissions of a
reconnect storm behind 2.4 s of ack latency), the wake probe 6.9 ms late. Every hand-off
out of the hub is a remote wake on its own thread, as Amendment 1 found for T2 alone,
and the gate removes one hand-off per message where the thread adds the cost to every
other. Two placements, three runs, all below T3: D3's discharge off the loop is
withdrawn (ADR 0092 Amendment 3). The PUBACK stays a hub command; step 1's batch stays
(it pays where a consumer acks tens of deliveries a read); #1175 is not merged.

**T3, first batch, measured** (run `h3-20261031T215218Z`, one `m6i.8xlarge`, the h3
ladder, binary `1db8c42e`): the knee moved from 92,400 msg/s (main, re-measured the same
day) and 88,700 (the baseline) to 105,500, bracketed at 107,200 (a 1.6% gap): 14% more
than main. At 67,500 msg/s a publish dispatch went from 6.7 to 5.3 µs and the hub's
busy share from 0.58 to 0.48; at 84,400 the publisher's ack p99 went from 103–119 ms to
5.3 ms, and at the knee a publish costs 5.6 µs against 7.0 before. The loop's busy
ceiling is unchanged at 0.83: the 17% it does not dispatch is the work it wakes
(Amendment 1), not the per-message cost, and is what T4 and D5 are for.

## Changelog

- **2026-10-31** — Delivery doc created with T1–T5 planned, issues #1159–#1163, from the
  maintainer's decision of the same day.
- **2026-10-31** — T1 in progress: `hub-model.py`, its six tests in CI, the report hook in
  `verdict.py`, and the five runs' baseline above.
- **2026-10-31** — T2 built and measured (run `h3-20261031T155400Z`, one `m6i.8xlarge`,
  the hub on its own thread, unpinned, against the baseline `h3-20261030T171933Z`): the
  knee 68,600 msg/s at ack p99 533 ms against 88,700, 23% less; at the same 67,500 msg/s
  the publisher ack went from 0.20 to 63.9 ms, a delivery's wait for its connection task
  from 0.01 to 17.2 ms, the shared runtime's wake lateness from 0.02 to 6.9 ms, the append
  from 3.4 to 77 ms, and 22% of elision holds timed out; the loop's busy ceiling rose from
  0.83 to 0.85. Every hand-off out of the loop had become a remote wake. The maintainer:
  ship it off by default and withdraw D1 (ADR 0092 Amendment 1). The pinned arm and the
  Genoa host were not run.
- **2026-10-31** — T1 done (#1165 merged) and T2 done (#1167 merged, the thread off by
  default under Amendment 1); #1159 and #1160 closed.
- **2026-10-31** — main `d4afe97d` (T2 merged, the thread off) re-measured on one
  `m6i.8xlarge` (run `h3-20261031T194739Z`): knee 92,400 msg/s at ack p99 640 ms against the
  baseline's 88,700, inside the rig's noise; at 67,500 busy 0.58, 6.7 µs per publish, 1.9 per
  PUBACK, wake 32 µs, ceiling 0.83: the baseline's figures, with `thread="shared"` in the
  scrapes. The shipped default is the old path.
- **2026-10-31** — T3 in progress: the frame-pointer profile of the hub task (run
  `h3-20261031T205637Z`, under Progress; a first attempt, `h3-20261031T204124Z`, lost its
  call graphs to `perf script`'s inline expansion) and the first batch of changes in its
  order: the publish and PUBACK commands dispatched on a small future with the rare arms
  boxed, the labelled metric handles resolved once, the sub-id walk only for subscriptions
  carrying an identifier, the elision write key built only while a write is in flight, the
  emptied hold queues pooled, the empty-backlog drain one lookup.
- **2026-10-31** — T3 first batch measured (run `h3-20261031T215218Z`, under Progress): the
  per-node knee 92,400 → 105,500 msg/s on one `m6i.8xlarge`, a publish dispatch 6.7 → 5.3 µs
  at 67,500 msg/s, the busy ceiling unchanged at 0.83.
- **2026-11-01** — T4 started, step 1: the connection task hands the hub its PUBACKs as one
  `HubCommand::PubAcks` per drained read (flushed before the task waits for the socket, at 32
  acks, or when the connection ends), and the hub completes a batch in one dispatch with the
  truncation and the drain once per batch; the state stays where it is. Measured next with T1;
  on the h3 shape (20,000 subscribers at a few messages a second each) a read carries one ack,
  so the step is the transport for step 2 more than a gain of its own.
- **2026-11-01** — T4 step 2 planned from that reading (a batch of one cannot amortize the
  ack's command): slice 2a settles a held delivery's obligation on the subscriber's
  connection task through a `PublishGate` shared with the publish's pending entry
  (`hub/hold.rs`), slice 2b moves the outbound in-flight state there; the plan is in T4's
  notes. Its first cut measured 105,500 on run `h3-20261101T105009Z`, a regression on
  T3's 112,100 with the ack dispatch down to 0.7 µs: the entry's retirement through a
  channel and the holds' pruning at their deadlines ran outside the dispatch timing and
  took the busy ceiling to 0.71; reworked the same day to retire at the arming and drop
  the hold with the ack batch. The second cut measured 100,300 on run
  `h3-20261101T121633Z`, the same ceiling: the loop gets 60% of the wall-clock where
  T3's got 76%, read as the publisher's wake landing in the subscriber worker's LIFO
  slot. #1172, #1173 and #1174 merged the same day. The same binary with the hub on its
  own thread then measured 78,100 (run `h3-20261101T131207Z`): the loop's share came
  back and every hand-off out of it became a remote wake. D3's discharge off the loop
  withdrawn, ADR 0092 Amendment 3; #1175 not merged; step 1 stays. The fan-in workload `fanin-qos1` added for the consumer-heavy shape, EMQX's
  `mqttbs` shared-subscription scenario at the h3 client count: 20,000 publishers into one
  `$share` group of 108 persistent consumers.
- **2026-10-31** — T3 second batch, on the profile's next two items: the hub's maps keyed
  by a client, a node, a filter or an id, the ordinary and shared subscription tables and
  the filter trie on a keyed fast hasher (`mqtt_core::FastMap`, `ahash`; SipHash was 10% of
  the hub task), and the trie's `+` and `#` children as fields rather than map entries
  (two hashed lookups per topic level per publish gone). Measured on run
  `h3-20261031T225644Z` (one `m6i.8xlarge`): the knee 105,500 → 112,100 msg/s, bracketed
  at 115,400; a publish 5.3 → 4.8 µs at 67,500 msg/s, the hub's busy share there
  0.48 → 0.44; the busy ceiling 0.82.
- **2026-11-01** — T3 on three nodes, both batches (runs `h3-20261031T234941Z` and
  `h3-20261101T012140Z`, 3 × `m6i.8xlarge`, the h3 ladder, under Progress): the knee
  270,800 → 299,300 → 306,400 msg/s, at ack p99 1.17 s → 270 ms → 92 ms; a publish
  dispatch at the knee 7.0 → 6.2 → 5.6 µs. The cluster gains less than one node (13%
  against 21%): a cluster's per-message work includes the peer frames, which the batches
  touched only through the hasher. The README's three-node and one-node figures move to
  306,400 and 112,100 (RIG2-RESULTS has both cards).
