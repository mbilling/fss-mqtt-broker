---
adr: "0092"
title: "The hub task bounds a node: a pinned thread, less work per message, consumer PUBACKs off the loop"
adr_status: Accepted
tasks:
  - id: 0092-T1
    title: "The hub model as a script: µs per publish and per consumer PUBACK, and the time lost to wake-ups, from a rig run's scrapes; the report cites it"
    status: planned
    issue: 1159
    notes: "Size S. bench/scale/rig2; reads the 2 s scrapes (mqttd_hub_dispatch_seconds by class, the publish and ack counters, mqttd_runtime_wake_seconds). The yardstick for T2–T4: today a = 6.4–7.3 µs and b ≈ 2 µs on m6i.8xlarge, 9.7 and 3.0 on m6i.4xlarge, the loss 18% (Ice Lake) and 41% (Genoa). A test on a recorded run; the four AWS runs of 2026-10-30 and the bisect of 2026-10-31 recorded here."
  - id: 0092-T2
    title: "The hub loop on its own OS thread, pinned to a core where the host allows (D1): MQTTD_HUB_THREAD and MQTTD_HUB_CORE, named on /statusz and a gauge; the shared runtime keeps everything else"
    status: planned
    issue: 1160
    notes: "Size M. The maintainer's item 1. The three tokio::spawn(hub.run()) sites in crates/mqttd/src/main.rs; a std thread with a current-thread runtime or LocalSet; rustix sched_setaffinity (no unsafe); mqtt-config, CONFIGURATION, OPERATIONS. Measured before and after on one m6i.8xlarge and one m7a.8xlarge (the 30% case) with T1; the upside is bounded by what the loop spends not dispatching at its ceiling, 17% of wall-clock on Ice Lake and 30% on Genoa."
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
| 0092-T1 | ⬜ planned | [#1159](https://github.com/mbilling/fss-mqtt-broker/issues/1159) | — | "Size S. bench/scale/rig2; reads the 2 s scrapes (mqttd_hub_dispatch_seconds by class, the publish and ack counters, mqttd_runtime_wake_seconds). The yardstick for T2–T4: today a = 6.4–7.3 µs and b ≈ 2 µs on m6i.8xlarge, 9.7 and 3.0 on m6i.4xlarge, the loss 18% (Ice Lake) and 41% (Genoa). A test on a recorded run; the four AWS runs of 2026-10-30 and the bisect of 2026-10-31 recorded here." |
| 0092-T2 | ⬜ planned | [#1160](https://github.com/mbilling/fss-mqtt-broker/issues/1160) | — | "Size M. The maintainer's item 1. The three tokio::spawn(hub.run()) sites in crates/mqttd/src/main.rs; a std thread with a current-thread runtime or LocalSet; rustix sched_setaffinity (no unsafe); mqtt-config, CONFIGURATION, OPERATIONS. Measured before and after on one m6i.8xlarge and one m7a.8xlarge (the 30% case) with T1; the upside is bounded by what the loop spends not dispatching at its ceiling, 17% of wall-clock on Ice Lake and 30% on Genoa." |
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

Baseline, from the AWS runs of 2026-10-30 and the bisect of 2026-10-31
(`docs/benchmarks/RIG2-RESULTS.md`):

| shape | durable QoS 1 | per node | hub at the knee |
|---|---|---|---|
| 1 × `m6i.8xlarge`, one copy | 88,700 msg/s | 88,700 | a = 6.4–7.3 µs, b ≈ 2 µs; 18% lost to wake-ups |
| 1 × `m7a.8xlarge`, one copy | 69,700 msg/s | 69,700 | dispatch 8% cheaper; 41% lost to wake-ups |
| 3 × `m6i.8xlarge` | 270,800 msg/s | 90,300 | |
| 6 × `m6i.4xlarge` | 390,300 msg/s | 65,050 | a = 9.7 µs, b = 3.0 µs; dispatch 82% of wall-clock |

## Changelog

- **2026-10-31** — Delivery doc created with T1–T5 planned, issues #1159–#1163, from the
  maintainer's decision of the same day.
