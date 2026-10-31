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
    status: planned
    issue: 1161
    notes: "Size M–L. The maintainer's item 2. First the frame-pointer profile run (the build exists; about $0.6 of rig time), then the items in profile order. Target about 7 → about 5 µs per publish on m6i, each change re-fitted with T1 on the rig; hub tests green."
  - id: 0092-T4
    title: "Consumer PUBACKs complete on the connection task (D3): it owns the session's outbound in-flight state; the hub learns of acknowledgements in batches only for durable truncation, elision discharge and the obligation ledger; QoS 2 unchanged"
    status: planned
    issue: 1162
    notes: "Size L. The maintainer's item 3. hub/mod.rs dispatch_pub_ack and complete_pending; the connection task's outbound state. Tests first: ADR 0089 T1's order tests, inflight_durability (#124), cluster_proc and false_dead_delivery with elision on. Done when the ack class is below 5% of hub time by T1 and no acknowledged message is lost on the fault suites."
  - id: 0092-T5
    title: "The control-plane/data-plane ADR (D5): K data shards per node and one control task, with the two-shard hand-off cost measured before it is written; not started before T2–T4 are measured"
    status: planned
    issue: 1163
    notes: "Size M for the ADR, after a two-shard prototype of size M. The maintainer's item 4, sequenced after items 1–3 (2026-10-31). Contents in #1163: shard ownership by placement group, immutable snapshots from the control task, hand-off and completion cost, hot-group dealing and # subscribers, shard failure modes, the node's ceilings as functions of ADR 0091's numbers with the ENA packet-rate ceiling from netprobe."
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
| 0092-T3 | ⬜ planned | [#1161](https://github.com/mbilling/fss-mqtt-broker/issues/1161) | — | "Size M–L. The maintainer's item 2. First the frame-pointer profile run (the build exists; about $0.6 of rig time), then the items in profile order. Target about 7 → about 5 µs per publish on m6i, each change re-fitted with T1 on the rig; hub tests green." |
| 0092-T4 | ⬜ planned | [#1162](https://github.com/mbilling/fss-mqtt-broker/issues/1162) | — | "Size L. The maintainer's item 3. hub/mod.rs dispatch_pub_ack and complete_pending; the connection task's outbound state. Tests first: ADR 0089 T1's order tests, inflight_durability (#124), cluster_proc and false_dead_delivery with elision on. Done when the ack class is below 5% of hub time by T1 and no acknowledged message is lost on the fault suites." |
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

Two readings the script settles. The Genoa node's loss is 30% at its ceiling, not the
41% read off its carried step (0.57 busy): its ceiling rises with the offer, from 0.57 at
69,700 to 0.70 at 101,300, which a fixed share would not do. And the wake probe names the
mechanism: every 64 commands the loop yields, and each yield waits 24–50 µs on Ice Lake
and 200–360 µs on Genoa, against 0.6 ms of dispatching between yields. T2 removes that
wait by giving the loop a runtime with nothing else to run.

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
