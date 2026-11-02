---
adr: "0093"
title: "K data shards per node and one control task: the hub's split"
adr_status: Proposed
tasks:
  - id: 0093-T1
    title: "The control task and its immutable snapshots (D2): subscribe, unsubscribe, attach, detach, evict, membership and presence on one task; the trie, the groups and the presence map behind one atomic pointer swap; a shard loads the snapshot once per dispatch batch; the answer written after the snapshot that includes the change"
    status: planned
    issue: 1180
    notes: "Size L. Replaces the prototype's ShardRoute mirror (hub/shards.rs mirror_routing). Tests first: the sharded hub tests (hub/tests/sharded.rs) with the mirror removed, a subscribe answered before and after a publish on another shard, a $share member's presence seen by a foreign origin within one batch. Measured with T1's model on h3 and fan-in: the control class under 2% of hub time at the knee."
  - id: 0093-T2
    title: "Hand-offs batched per dispatch batch, carrying credit, completing back to the origin (D3): one ShardDeliver and one ShardDone per (origin, target) pair per batch; a per-pair byte credit acquired by the origin and released by the target, charged to the origin's ingress pool when exhausted; the pending-publish bound as ADR 0091 D6's time bound with the count as its floor"
    status: planned
    issue: 1180
    notes: "Size M. The prototype's flush_handoffs sends per message on an unbounded channel; the collapse above the knee (ADR 0093 Context) is what this task removes. Tests: a slow target shard parks the origin's publishers through the ingress credit; no eviction with the ack withheld while the hand-off answers within the stall budget. Measured on h3 and fan-in against the prototype's rows: above the knee the pair must hold its connections (OFFER NOT TAKEN, as the one loop does), never DISCONNECTED."
  - id: 0093-T3
    title: "The worker capture removed, by measurement (D4): the runtime without the LIFO slot against a hand-off wake through the injection queue once per batch; the shards stay tasks of the shared runtime"
    status: planned
    issue: 1180
    notes: "Size S to build, M to measure. Two arms on one m6i.8xlarge, h3 and fan-in, read per shard: the acceptance is the summed busy of two shards approaching 2 × 0.82 on h3 before the knee, and a shard above its knee saturating at its ceiling instead of collapsing. The prototype's pair summed 1.02 at the knee: one worker's worth."
  - id: 0093-T4
    title: "The ENA packet-rate ceiling measured with netprobe on AWS (D8's P): m6i.8xlarge and m6i.4xlarge, UDP packet rate, tcp_rr at 1,000, 10,000 and 20,000 connections, bulk bandwidth; the figure beside the CPU term in this doc and in RIG2-RESULTS"
    status: planned
    issue: 1180
    notes: "Size S, about USD 2 a run (bench/scale/netprobe, PROVIDER=aws). The Hetzner probe (PR #1156) found loss by connection count from about 10,000 connections with packets in flight; no AWS run has shown it at 20,000 clients, and no AWS probe has run. Until it has, the README claims no figure for K > 1."
  - id: 0093-T5
    title: "The cluster, retained, relay and admin paths on shards (D5): a peer's forward enters its origin shard like a client's publish and is gated like one; the retained index and the relay become the control task's; the admin API lists every shard's sessions; the fault suites with shards on; the peers guard lifted"
    status: planned
    issue: 1180
    notes: "Size L. Nothing in a cluster changes before this task: a node that names peers refuses hub_shards > 1 (main.rs start_shards). Measured on three m6i.8xlarge against ADR 0092's 306,400 row."
  - id: 0093-T6
    title: "A shard's failure modes (D7): fail-stop on a shard's panic, the drain barrier joined across shards, the Ping that names a shard that did not answer, per-shard queue, busy, wake and lane depths on /statusz; tests for a slow shard and a stuck dispatch"
    status: planned
    issue: 1180
    notes: "Size M. Tests: a target shard held busy parks the origin's publishers and nothing else on the node waits; a shard's panic ends the process as the one loop's does (ADR 0061 §8)."
  - id: 0093-T7
    title: "The four-shard knee against D8 (D9): hub_shards = 4 on one m6i.8xlarge, h3 and fan-in, the measured knee beside the CPU term (317,000 and 410,000) and the network term; the README's per-node figures move by the rig's numbers only, and the default follows"
    status: planned
    issue: 1180
    notes: "Size S, two rig runs. After T2, T3 and T4. The default stays hub_shards = 1 until two shards carry more than one loop at an ack p99 within the one loop's band, holding their connections above the knee."
  - id: 0093-T8
    title: "The hot-group and #-subscriber shapes measured (D6): one $share group taking most of a node's publishes with its members across shards, and one # subscriber on another shard, each on one m6i.8xlarge; the per-session ceiling of a broad subscriber stated beside the measurement"
    status: planned
    issue: 1180
    notes: "Size M: two rig2 workloads (a hot-group arm of fanin-qos1 with the members' placement groups spread; a wildcard arm with one # subscriber) and two runs. A broad subscriber's capacity is one shard's delivery capacity (ADR 0093 D6); the measurement states the number."
---

# Delivery 0093 — K data shards per node and one control task

**ADR:** [docs/adr/0093-data-shards-and-a-control-task.md](../adr/0093-data-shards-and-a-control-task.md)

The plan, progress, and changelog for ADR 0093, the hub's split (ADR 0092 D5). Task status
lives in the frontmatter above; the table below is generated from it. The two-shard
prototype that the record is written from, and its four rig runs, are ADR 0092 T5's: the
rows and the readings are in [docs/delivery/0092-hub-task-ceiling.md](0092-hub-task-ceiling.md),
and every task here is measured against them on the rig of ADR 0090.

## Plan

The order is the dependency order of the decisions. T1 (the control task and its
snapshots) and T2 (hand-offs with credit and batching) replace the two parts of the
prototype that are not the design, the mirror and the per-message unbounded hand-off; T3
removes the worker capture the pair measured, by a measurement between two candidates; T4
supplies the network term the ceilings need before any figure for `K > 1` is claimed; T5
moves the cluster paths onto shards and lifts the peers guard; T6 makes a shard's failure
modes tested; T7 measures four shards against D8 and moves the default; T8 measures the
two shapes D6 states limits for. A task is done when its mechanism exists and its
measurement is in this doc, whatever the number (ADR 0092 D4).

## Status

<!-- status-table:0093 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0093-T1 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size L. Replaces the prototype's ShardRoute mirror (hub/shards.rs mirror_routing). Tests first: the sharded hub tests (hub/tests/sharded.rs) with the mirror removed, a subscribe answered before and after a publish on another shard, a $share member's presence seen by a foreign origin within one batch. Measured with T1's model on h3 and fan-in: the control class under 2% of hub time at the knee." |
| 0093-T2 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size M. The prototype's flush_handoffs sends per message on an unbounded channel; the collapse above the knee (ADR 0093 Context) is what this task removes. Tests: a slow target shard parks the origin's publishers through the ingress credit; no eviction with the ack withheld while the hand-off answers within the stall budget. Measured on h3 and fan-in against the prototype's rows: above the knee the pair must hold its connections (OFFER NOT TAKEN, as the one loop does), never DISCONNECTED." |
| 0093-T3 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size S to build, M to measure. Two arms on one m6i.8xlarge, h3 and fan-in, read per shard: the acceptance is the summed busy of two shards approaching 2 × 0.82 on h3 before the knee, and a shard above its knee saturating at its ceiling instead of collapsing. The prototype's pair summed 1.02 at the knee: one worker's worth." |
| 0093-T4 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size S, about USD 2 a run (bench/scale/netprobe, PROVIDER=aws). The Hetzner probe (PR #1156) found loss by connection count from about 10,000 connections with packets in flight; no AWS run has shown it at 20,000 clients, and no AWS probe has run. Until it has, the README claims no figure for K > 1." |
| 0093-T5 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size L. Nothing in a cluster changes before this task: a node that names peers refuses hub_shards > 1 (main.rs start_shards). Measured on three m6i.8xlarge against ADR 0092's 306,400 row." |
| 0093-T6 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size M. Tests: a target shard held busy parks the origin's publishers and nothing else on the node waits; a shard's panic ends the process as the one loop's does (ADR 0061 §8)." |
| 0093-T7 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size S, two rig runs. After T2, T3 and T4. The default stays hub_shards = 1 until two shards carry more than one loop at an ack p99 within the one loop's band, holding their connections above the knee." |
| 0093-T8 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size M: two rig2 workloads (a hot-group arm of fanin-qos1 with the members' placement groups spread; a wildcard arm with one # subscriber) and two runs. A broad subscriber's capacity is one shard's delivery capacity (ADR 0093 D6); the measurement states the number." |
<!-- /status-table:0093 -->

## Progress

Nothing built yet. The prototype's four runs, the baseline every task measures against,
are under Progress in [0092-hub-task-ceiling.md](0092-hub-task-ceiling.md): one
`m6i.8xlarge`, two shards unpinned — h3 118,700 msg/s at ack p99 835 ms against the one
loop's 112,100 at 354, fan-in 120,400 at 1,038 ms against 120,400 at 18.7; the hand-off
2.3 and 1.8 µs at one dispatch a message; the collapse above the knee — and two shards on
their own threads, 97,700 and 92,900.

## Changelog

- **2026-11-02** — ADR 0093 written from the prototype's four runs (ADR 0092 T5, #1163),
  as Proposed; the eight tasks filed under #1180.
