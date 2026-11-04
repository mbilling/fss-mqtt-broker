---
adr: "0093"
title: "K data shards per node and one control task: the hub's split"
adr_status: Accepted
tasks:
  - id: 0093-T1
    title: "The control task and its immutable snapshots (D2): subscribe, unsubscribe, attach, detach, evict, membership and presence on one task; the trie, the groups and the presence map behind one atomic pointer swap; a shard loads the snapshot once per dispatch batch; the answer written after the snapshot that includes the change"
    status: planned
    issue: 1180
    notes: "Size L. Replaces the prototype's ShardRoute mirror (hub/shards.rs mirror_routing). Tests first: the sharded hub tests (hub/tests/sharded.rs) with the mirror removed, a subscribe answered before and after a publish on another shard, a $share member's presence seen by a foreign origin within one batch. Measured with T1's model on h3 and fan-in: the control class under 2% of hub time at the knee."
  - id: 0093-T2
    title: "Hand-offs batched per dispatch batch, carrying credit, completing back to the origin (D3): one ShardDeliver and one ShardDone per (origin, target) pair per batch; a per-pair byte credit acquired by the origin and released by the target, charged to the origin's ingress pool when exhausted; the pending-publish bound as ADR 0091 D6's time bound with the count as its floor"
    status: in-progress
    issue: 1180
    notes: "Size M. The prototype's flush_handoffs sends per message on an unbounded channel; the collapse above the knee (ADR 0093 Context) is what this task removes. Tests: a slow target shard parks the origin's publishers through the ingress credit; no eviction with the ack withheld while the hand-off answers within the stall budget. Measured on h3 and fan-in against the prototype's rows: above the knee the pair must hold its connections (OFFER NOT TAKEN, as the one loop does), never DISCONNECTED."
  - id: 0093-T3
    title: "The worker capture removed, by measurement (D4): the runtime without the LIFO slot against a hand-off wake through the injection queue once per batch; the shards stay tasks of the shared runtime"
    status: in-progress
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
  - id: 0093-T9
    title: "Shard-local $share dealing (Amendment 1): with locality on, a shard deals among the online members it owns — own online members a prefix of each group's online prefix, so the deal stays one index — and hands across only when it owns none online; measured on h3 and fan-in against T2 and T3's rows"
    status: in-progress
    issue: 1180
    notes: "Size S to build, two rig runs. mqtt-core shared.rs: GroupMembers keeps three regions (own online, another shard's online, offline) and select_own_online; the table records another shard's members (subscribe_member with own = false, from the routing mirror). hub delivery.rs plan_local_fast deals among own online members when the node runs shards. Tests: the region invariant checked against a model after every subscribe, attach, detach, mirror and leave; a group with a member on each shard deals on the publisher's shard and crosses only when it has no member online; a group wholly on the other shard still takes every publish under the window. Finding of the eight-shard run (2026-11-03, h3-20261103T192204Z): with a group's members hashed over eight shards and each shard dealing only among its own, the busiest consumer took 1.7–2.2× the mean consumer's deliveries in every round (the four-shard run reported no unevenness), and its append lane (256 jobs) overflowed from 386,800 while the loops ran at 0.41 busy — the eight-shard knee is this deal's. T9's own measurement found no knee from keeping the deal local, so the lever to weigh is the deal over every online member again (the hand-off is 0.9% of the knee's time) or a local deal weighted by the shard's share of the group. Correction 2026-11-04: the overflow was not the deal's. Every drop of that run landed in a round's first eight seconds, in a write storm that every round from 375,000 up shared, carried or failed (ADR 0089 Amendment 4, #1209: a session's packet-id block was reserved only once spent, so every durable session's first second of deliveries was written); the uneven deal only decided which lanes filled first. Its unevenness stands as a fairness finding, with the same two levers."
  - id: 0093-T10
    title: "The bounds that push back before the ones that drop (Amendment 2): a publish's ingress credit held on its pending entry until its publisher is answered; an unconfigured pool at most the pending table's floor a shard (65,536 × the per-publish overhead); the periodic reconcile scan out of write elision's takeover window (ADR 0089 Amendment 2) and run by each shard at its own phase of the cycle; measured on h3 with four shards and with two"
    status: in-progress
    issue: 1180
    notes: "Size S to build, two rig runs. hub/mod.rs route_publish holds the credit (hold_credit) and try_complete_pending releases it at the answer; ingress.rs MAX_SHARD_POOL_BYTES caps pool_from_config's derived pool; hub/mod.rs takeover_window_open and periodic_scan_phase, elision.rs may_elide on the window. Tests (hub/tests/held_credit.rs): the credit held through a parked durable write and an elision hold and returned at the answer, at dispatch for a publish nobody waits on, at the answer for an entry a settle window keeps; the window open for the boot, a takeover's scan and the eager window and closed for the periodic scan; a delivery eligible through the periodic scan; each shard's phase. Measured: the four-shard knee above 164,900 and a durable path that falls behind shown as OFFER NOT TAKEN, never DISCONNECTED. The pool as a time bound (open after the four-shard runs) was weighed on 2026-11-03 and withdrawn: at an offer equal to the capacity the backlog that built during the ramp is conserved, so a smaller pool only moves the wait from the shard's queue into the sockets and the generator measures the same ack latency; the knee's latency is that backlog over the rate, and the levers are capacity (eight shards took 375,000 from 3.1 s to 23 ms) and the ladder's resolution near the knee (the band bisect, ADR 0090)."
  - id: 0093-T11
    title: "The loop yields by its queue's depth (Amendment 3): every 64 commands below node.hub_yield_deep queued (1,024, one ingest batch), every 1,024 from it; the housekeeping still every 64; mqttd_hub_yields_total{shard}"
    status: in-progress
    issue: 1180
    date: 2026-11-03
    notes: "Size S. hub/mod.rs (HUB_YIELD_EVERY_DEEP, yield_every, the loop's since_yield and since_housekeeping counters, set_hub_yield_deep), mqtt-config node.hub_yield_deep / MQTTD_HUB_YIELD_DEEP, main.rs wiring, mqtt-observability hub_yields_total, CONFIGURATION regenerated. Tests: the rule's unit test, the config's default, TOML and env cases. Measured 2026-11-03 on h3 with four shards together with ADR 0089 Amendment 3 (run h3-20261103T162448Z, the paragraph under Progress): each loop's busy ceiling 0.86 against 0.70, the knee 375,000 against 327,100, the broker taking about 400,000 when offered more, 300,000 at 52.7 ms."
  - id: 0093-T12
    title: "Eight shards (Amendment 3): MAX_SHARDS from four to eight, the metrics' shard labels with it"
    status: in-progress
    issue: 1180
    date: 2026-11-03
    notes: "Size XS. hub/shards.rs MAX_SHARDS, mqtt-observability HUB_SHARDS and SHARD_LABELS. Measured 2026-11-03 on h3 with two generator hosts (run h3-20261103T192204Z, the paragraph under Progress): the lossless knee 380,900 against four shards' 375,000, at ack p99 23 ms where four shards read 3.1 s, so every latency band reads 380,900 (four shards: 300,000); each loop at 0.41 busy at the knee, 8.6 µs a message; the wall is a loss, not the hub — the shard-local deal (T9) loads one hot member's append lane until it overflows (append-backlog-full drops of 17–478 a round from 386,800) — and 468,800 ended DISCONNECTED on its retry. Correction 2026-11-04: every drop fell in a round's first eight seconds, in the start-of-round write storm of reserve-at-spent packet-id blocks that ADR 0089 Amendment 4 removes (#1209), not in the deal's steady state; to be rerun with it."
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
| 0093-T2 | 🚧 in-progress | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size M. The prototype's flush_handoffs sends per message on an unbounded channel; the collapse above the knee (ADR 0093 Context) is what this task removes. Tests: a slow target shard parks the origin's publishers through the ingress credit; no eviction with the ack withheld while the hand-off answers within the stall budget. Measured on h3 and fan-in against the prototype's rows: above the knee the pair must hold its connections (OFFER NOT TAKEN, as the one loop does), never DISCONNECTED." |
| 0093-T3 | 🚧 in-progress | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size S to build, M to measure. Two arms on one m6i.8xlarge, h3 and fan-in, read per shard: the acceptance is the summed busy of two shards approaching 2 × 0.82 on h3 before the knee, and a shard above its knee saturating at its ceiling instead of collapsing. The prototype's pair summed 1.02 at the knee: one worker's worth." |
| 0093-T4 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size S, about USD 2 a run (bench/scale/netprobe, PROVIDER=aws). The Hetzner probe (PR #1156) found loss by connection count from about 10,000 connections with packets in flight; no AWS run has shown it at 20,000 clients, and no AWS probe has run. Until it has, the README claims no figure for K > 1." |
| 0093-T5 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size L. Nothing in a cluster changes before this task: a node that names peers refuses hub_shards > 1 (main.rs start_shards). Measured on three m6i.8xlarge against ADR 0092's 306,400 row." |
| 0093-T6 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size M. Tests: a target shard held busy parks the origin's publishers and nothing else on the node waits; a shard's panic ends the process as the one loop's does (ADR 0061 §8)." |
| 0093-T7 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size S, two rig runs. After T2, T3 and T4. The default stays hub_shards = 1 until two shards carry more than one loop at an ack p99 within the one loop's band, holding their connections above the knee." |
| 0093-T8 | ⬜ planned | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size M: two rig2 workloads (a hot-group arm of fanin-qos1 with the members' placement groups spread; a wildcard arm with one # subscriber) and two runs. A broad subscriber's capacity is one shard's delivery capacity (ADR 0093 D6); the measurement states the number." |
| 0093-T9 | 🚧 in-progress | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size S to build, two rig runs. mqtt-core shared.rs: GroupMembers keeps three regions (own online, another shard's online, offline) and select_own_online; the table records another shard's members (subscribe_member with own = false, from the routing mirror). hub delivery.rs plan_local_fast deals among own online members when the node runs shards. Tests: the region invariant checked against a model after every subscribe, attach, detach, mirror and leave; a group with a member on each shard deals on the publisher's shard and crosses only when it has no member online; a group wholly on the other shard still takes every publish under the window. Finding of the eight-shard run (2026-11-03, h3-20261103T192204Z): with a group's members hashed over eight shards and each shard dealing only among its own, the busiest consumer took 1.7–2.2× the mean consumer's deliveries in every round (the four-shard run reported no unevenness), and its append lane (256 jobs) overflowed from 386,800 while the loops ran at 0.41 busy — the eight-shard knee is this deal's. T9's own measurement found no knee from keeping the deal local, so the lever to weigh is the deal over every online member again (the hand-off is 0.9% of the knee's time) or a local deal weighted by the shard's share of the group. Correction 2026-11-04: the overflow was not the deal's. Every drop of that run landed in a round's first eight seconds, in a write storm that every round from 375,000 up shared, carried or failed (ADR 0089 Amendment 4, #1209: a session's packet-id block was reserved only once spent, so every durable session's first second of deliveries was written); the uneven deal only decided which lanes filled first. Its unevenness stands as a fairness finding, with the same two levers." |
| 0093-T10 | 🚧 in-progress | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | — | "Size S to build, two rig runs. hub/mod.rs route_publish holds the credit (hold_credit) and try_complete_pending releases it at the answer; ingress.rs MAX_SHARD_POOL_BYTES caps pool_from_config's derived pool; hub/mod.rs takeover_window_open and periodic_scan_phase, elision.rs may_elide on the window. Tests (hub/tests/held_credit.rs): the credit held through a parked durable write and an elision hold and returned at the answer, at dispatch for a publish nobody waits on, at the answer for an entry a settle window keeps; the window open for the boot, a takeover's scan and the eager window and closed for the periodic scan; a delivery eligible through the periodic scan; each shard's phase. Measured: the four-shard knee above 164,900 and a durable path that falls behind shown as OFFER NOT TAKEN, never DISCONNECTED. The pool as a time bound (open after the four-shard runs) was weighed on 2026-11-03 and withdrawn: at an offer equal to the capacity the backlog that built during the ramp is conserved, so a smaller pool only moves the wait from the shard's queue into the sockets and the generator measures the same ack latency; the knee's latency is that backlog over the rate, and the levers are capacity (eight shards took 375,000 from 3.1 s to 23 ms) and the ladder's resolution near the knee (the band bisect, ADR 0090)." |
| 0093-T11 | 🚧 in-progress | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | 2026-11-03 | "Size S. hub/mod.rs (HUB_YIELD_EVERY_DEEP, yield_every, the loop's since_yield and since_housekeeping counters, set_hub_yield_deep), mqtt-config node.hub_yield_deep / MQTTD_HUB_YIELD_DEEP, main.rs wiring, mqtt-observability hub_yields_total, CONFIGURATION regenerated. Tests: the rule's unit test, the config's default, TOML and env cases. Measured 2026-11-03 on h3 with four shards together with ADR 0089 Amendment 3 (run h3-20261103T162448Z, the paragraph under Progress): each loop's busy ceiling 0.86 against 0.70, the knee 375,000 against 327,100, the broker taking about 400,000 when offered more, 300,000 at 52.7 ms." |
| 0093-T12 | 🚧 in-progress | [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) | 2026-11-03 | "Size XS. hub/shards.rs MAX_SHARDS, mqtt-observability HUB_SHARDS and SHARD_LABELS. Measured 2026-11-03 on h3 with two generator hosts (run h3-20261103T192204Z, the paragraph under Progress): the lossless knee 380,900 against four shards' 375,000, at ack p99 23 ms where four shards read 3.1 s, so every latency band reads 380,900 (four shards: 300,000); each loop at 0.41 busy at the knee, 8.6 µs a message; the wall is a loss, not the hub — the shard-local deal (T9) loads one hot member's append lane until it overflows (append-backlog-full drops of 17–478 a round from 386,800) — and 468,800 ended DISCONNECTED on its retry. Correction 2026-11-04: every drop fell in a round's first eight seconds, in the start-of-round write storm of reserve-at-spent packet-id blocks that ADR 0089 Amendment 4 removes (#1209), not in the deal's steady state; to be rerun with it." |
<!-- /status-table:0093 -->

## Progress

The prototype's four runs, the baseline every task measures against, are under Progress
in [0092-hub-task-ceiling.md](0092-hub-task-ceiling.md): one `m6i.8xlarge`, two shards
unpinned — h3 118,700 msg/s at ack p99 835 ms against the one loop's 112,100 at 354,
fan-in 120,400 at 1,038 ms against 120,400 at 18.7; the hand-off 2.3 and 1.8 µs at one
dispatch a message; the collapse above the knee — and two shards on their own threads,
97,700 and 92,900.

**T2 and T3, built** (2026-11-02, branch `0093-t2-t3`). T2, the hand-off with credit,
batched: a publish's hand-offs are collected per target shard during the dispatch batch
(`Hub::flush_handoffs` fills `handoff_batches`) and sent as one `ShardDeliver` per target
at the batch's end — at the yield every 64 commands and before the loop parks
(`Hub::flush_shard_batches`) — and the settled hand-offs go back the same way, one
`ShardDone` per origin per batch. The publish's ingress credit (ADR 0082 T3) travels with
the hand-off: a gated publish's is held beside its pending entry
(`PendingTable::hold_credit`) until the last shard answers or the entry goes, an ungated
one's inside the batch until the target has dispatched it, so the pool is charged for the
hand-off's lifetime and a shard that answers late parks the publishers that feed it. The
per-pair credit (`node.hub_handoff_inflight`, 16,384 by default, `0` for none): each
shard counts the gated hand-offs every other shard still owes it an answer for, and a
target at the cap parks the origin's data lane — its publishers' commands wait with
their credit, and the pool parks the connections behind them — until an answer comes;
the hand-off batches and their answers travel on the control lane, so two parked lanes
can never wait for each other's batches. Built second, after run 1 of 2026-11-02 showed
the pool alone does not bound the right thing: 256 MiB of credit is 2.5 s of offer, a
hand-off crosses two queues of it, its pending entry ages past the stall budget, and the
bound evicts with the acks withheld — which closes the publishers (a withheld
acknowledgement closes the connection, v5 and v3.1.1 alike, by ADR 0042's design). The
pending-publish bound is ADR 0091 D6's form: the count a floor, an entry over it evicted
only once it has waited past the stall budget of 1 s (`PENDING_STALL_BUDGET`), the byte
bound hard. T3, the two arms, both in the same binary:
`MQTTD_HUB_LIFO_SLOT=off` builds the runtime without tokio's LIFO slot — the process
entry builds the runtime itself now, reading that one setting from the environment
before the config exists, and the option is tokio's unstable API, so it takes effect only
in a build with `RUSTFLAGS="--cfg tokio_unstable"` (the rig binary's) and is reported and
ignored elsewhere; `MQTTD_HUB_HANDOFF_WAKE=remote` (`node.hub_handoff_wake`) posts a
shard's batches to its mailbox without waking it and has the mailman, a thread of the
node's own, wake it through the runtime's injection queue, once per batch. Tests:
`hub/tests/sharded.rs` — the credit held until the other shard answers (its write parked
by the store), a burst of 40 going over in fewer `shard` dispatches than messages, a
hand-off through the mailbox and the mailman — and the pending-table bound test
rewritten for the floor.

**One ingress pool per shard** (2026-11-02, the third cut): the node's credit is `K`
pools of a `K`-th of the configured bytes (`IngressCredit::sharded`), and a client
connection draws on the pool of the shard that owns its client (`connection_for`); peer
links draw on the first shard's, where their traffic enters (D5). Built after runs 3 and 4
of the same day showed, with and without the per-pair gate, that one shared pool goes to
whichever shard falls behind: about 70 s into the round at 105,500 msg/s, one shard's data
lane grew to 235,000 commands — the whole 256 MiB — while its partner's drained to zero
and starved, both loops fell to 0.35 busy, and the pair took 72,000. Per shard, a shard
behind parks only its own publishers. Test: one shard holding its whole pool leaves the
other's untouched.

**T2 and T3 measured, h3** (2026-11-02, one `m6i.8xlarge`, two shards, read with T1's
model per shard; the ladders start at 105,500 msg/s, a rung of the usual grid, and step
down when it does not carry). Against the prototype's 118,700 at 835 ms and the one loop's
112,100 at 354:

| run | build and arm | knee | ack p99 at the knee | above the knee | the loops at the knee | wake at the knee |
|---|---|---|---|---|---|---|
| `h3-20261102T104739Z` | T2 first cut (`2936491b`): batching, the credit on the pending entry, the pending floor | 108,800 | 552 ms | DISCONNECTED: the floor evicts, the publishers close | 0.52 each (1.04) | 267 µs |
| `h3-20261102T112127Z` | the same, the pool at 32 MB | 97,700 | 170 ms | OFFER NOT TAKEN at ~76,000 | 0.47 each (0.93) | 233 µs |
| `h3-20261102T115730Z` | + the per-pair gate (`e4d3d5ef`, 16,384) | 95,000 | 74 ms | OFFER NOT TAKEN at ~72,000 | 0.45 each (0.90) | 194 µs |
| `h3-20261102T123158Z` | the same, no gate, `MQTTD_HUB_LIFO_SLOT=off` | 97,700 | 972 ms | OFFER NOT TAKEN at ~72,000–80,000 | 0.44 and 0.45 (0.89) | 315 µs |
| `h3-20261102T131047Z` | + one pool per shard (`d36204c0`), the gate, the slot on | 100,300 | 383 ms | OFFER NOT TAKEN at ~70,000 | 0.46 each (0.92) | 271 µs |
| `h3-20261102T134730Z` | the same, `MQTTD_HUB_HANDOFF_WAKE=remote` | 100,300 | 397 ms | OFFER NOT TAKEN at ~71,000–74,000 | 0.46 and 0.47 (0.93) | 298 µs |

What they show. **The collapse is gone** once a shard's backlog parks its publishers
before the pending bound can evict for it: every run after the first holds its
connections above the knee (OFFER NOT TAKEN, never DISCONNECTED). The first cut kept the
default 256 MiB pool, which is 2.5 s of offer; a hand-off crosses two queues of it, its
pending entry aged past the 1 s budget, the floor evicted 42,661 entries in one round with
the acks withheld, and a withheld acknowledgement closes the publisher's connection — 9,763
of 10,000 in seconds. **But the pair does not hold its knee above it**: past the knee it
drops to a plateau of 70,000–77,000 instead of taking what it can, as the one loop takes
105,000–107,000 at its own. With one shared pool the shard that falls behind keeps the
credit and starves its partner (runs 3 and 4, with and without the gate: one data lane at
235,000 commands, the other at zero); one pool per shard ends that (run 5: both lanes at
their half), and the plateau stays. **The limiter is the scheduling, not the
back-pressure.** At every knee the two loops sum to one worker's worth (0.89–1.04), and in
the plateau each sits at 0.35 with its queue full: it dispatches its 64 commands, about
0.3 ms, yields, and its wake comes 0.4–0.56 ms late — 0.3 ÷ (0.3 + 0.56) is the 0.35
measured. A shard that hands a batch to the other wakes it onto its own worker, so the two
take turns there. **Turning the LIFO slot off (D4's first arm) does not separate them** —
the woken task still lands on the waker's worker, at the back of its queue instead of
next — and costs 10% (97,700 against 108,800, though that binary also moved the hand-off
batches to the control lane). **Waking the other shard from outside the runtime (D4's
second arm) does not separate them either**: the mailman's wake goes through the injection
queue, and run 6 measured exactly what run 5 did — the same knee, the same 0.46 a loop,
the same plateau at 0.37 with the wake 499 µs late. So the cross-shard wake is not what
holds the pair on one worker. Two loops that never park at load only ever yield
(`yield_now` every 64 commands), and a yielded task goes back on its own worker's queue;
once both are there they alternate, and neither arm changes that. The batching made the
hand-off no cheaper: 9.1–9.6 hub-µs a message across the six runs' carried steps, against
the prototype's 8.6–9.0 at the same offers.

**What T2 and T3 leave.** T2's acceptance on h3 holds with one pool per shard: above the
knee the pair keeps every connection (OFFER NOT TAKEN), where the prototype collapsed. It
does not hold its knee: 100,300 against the prototype's 118,700 and the one loop's
112,100, and a plateau near 70,000 above it. T3's acceptance — two shards summing toward
2 × 0.82 — is not met by either arm: every knee sums to 0.89–1.04. On this runtime the
split as built does not scale a node, and the next lever is where a loop yields and on
which worker it resumes, not how a hand-off wakes its target: a yield by time or by
tokio's cooperative budget instead of every 64 commands, or a worker (or runtime) per
shard that its own connection tasks share. That is a decision for the ADR (an amendment
to D4) before more is built. Separately, at overload the pending table sits at its
65,536 floor full of answered entries kept for the settle window's replay, and each
eviction logs a warning — journald suppressed some 80,000 a minute from one broker; the
one-loop baseline does the same (235,000–657,000 such evictions a round at 115,400–131,900)
and still holds its knee, so it is a log flood to fix on its own, not the pair's limit.
The fan-in runs wait for that decision.

**T9, shard-local `$share` dealing, measured on h3** (2026-11-02, Amendment 1, commit
`7f7219e2`). Two runs, one built as T2 and T3's were and one without the flag they all
carried:

| run | build | knee | ack p99 at the knee | hub-µs a message at the knee | the loops at the knee | wake at the knee |
|---|---|---|---|---|---|---|
| `h3-20261102T145322Z` | `--cfg tokio_unstable`, as runs 1–6 | 92,400 | 145 ms | 8.3 (hand-off 0) | 0.35 each (0.70) | 503 µs |
| `h3-20261102T153403Z` | the standard build | 97,700 | 90 ms | 7.6 (hand-off 0) | 0.37 and 0.38 (0.75) | 339 µs |

The deal stays local as built — the `shard` class is zero, the consumers' spread 1.09–1.22×
where every earlier run read 1.00× — and the work a message costs falls by the hand-off's
1.5 µs. The knee does not rise: it falls, 97,700 against run 5's 100,300 (and run 5 carried
the flag). **The flag itself**, which every T2 and T3 binary carried for the LIFO-slot arm
and neither the prototype nor the one loop did, costs about 5% of a message (8.8 against 9.2
µs at the same plateau) and most of the latency below the knee: 84,400 at 9 ms against 25.
So runs 1–6 understate their builds by about that much; their order stands.

**What the eight runs say together.** Take the hand-off's time out of each knee and the
pair does the same amount of other hub work at every one of them: 0.74 worker-seconds a
second in the prototype, 0.76 in run 1, 0.68 in run 5, 0.75 in run 8 — about what one loop
does alone (0.78 at its knee). The knee then follows the rest of a message's cost: 6.3 µs
in the prototype, the cheapest, and its 118,700 the highest; 7.6 in run 8 and 97,700. The
hand-off's own time rides on top, which is why the summed busy reached 1.02 with it and
0.75 without. Two loops on this runtime do one loop's work between them, whatever deals,
wakes or pools they use, and a lever on the hand-off cannot move that. The question is now
what the two loops share — a lock, the store, a cache line, the workers they wake onto —
and a CPU profile of the process at the knee is the next measurement, not another arm.

**The CPU profile** (2026-11-02, `h3-20261102T165107Z`, commit `bd3d62ad` in the standard
build with frame pointers, `MQTTD_HUB_SHARDS=2` as run 8). perf sampled the whole process
at 999 Hz with call graphs inside the measured window, beside every thread's schedstat and
the machine's counters. 84,400 carried (ack p99 14.2 ms with the profiler attached); 97,700
did not, twice (74,787 and 72,111 received, ack p99 15–16 s: the plateau), where run 8
carried it without the profiler, so recording costs a few percent of the knee. The one-loop
column is the 2026-10-31 profile of an older build (main `19f36004`) at the same rate:

| | one loop, 84,400 | two loops, 84,400 | two loops, 97,700 offered |
|---|---|---|---|
| mqttd CPU | 3.9 cores | 5.5 cores | 10.1–10.2 cores |
| the hub loops on CPU, both | 0.83 | 0.81 | 0.74–0.75 |
| a contended lock's slow path (all of it from the connection loop) | 0.7% | 5.4% | 9.2–9.5% |
| the kernel's context-switch and wake paths | 12.3% | 18.7% | 34% |
| the workers' voluntary context switches | — | 170,000/s | 586,000–592,000/s |

The machine is never short of CPU (8 and 13 of its 32 cores busy), the store is idle (the
durable writer commits about 2,000 ops a second at every knee, one loop or two, because
elision keeps the rest out of it), and the hub loops are not the limit: above the knee each
is on CPU about 37% of the time while the rest of the process burns nine cores. What the
pair shares is a lock every connection takes. `serve_loop`'s `select!` builds
`drain_signal(policy)` — `cancelled()` on the graceful-shutdown token in the policy every
connection shares — afresh on every pass, and tokio's `Notify` locks its waiter list to
register that future, again on every poll of it, and again to drop it. The token has one
`Notify`, so that is one mutex taken two or three times per pass of every connection's loop
on all 32 workers. One loop leaves it mostly uncontended; two loops at the same 84,400
contend it eight times as hard, and above the knee its waits become a storm of futex parks
and wakes that takes a third of the CPU and slows every connection task, which leaves the
pair at 72,000–75,000. The prototype carried 118,700 with the same connection code; whether
it simply stayed below the point where the convoy starts is not measured. The fix is per
connection: a child token made once when the connection starts, its `cancelled()` future
pinned outside the loop, so a poll takes a lock no other connection takes. The knee with it
is the next measurement, before any amendment to D4.

**The drain fix, measured on h3** (2026-11-02, `h3-20261102T192407Z`, commit `573ea754` in
the standard build, `MQTTD_HUB_SHARDS=2` as run 8):

| offered | verdict | ack p99 | the loops' busy | µs a message | wake |
|---|---|---|---|---|---|
| 105,500 | carried | 1.6 ms | 0.38 and 0.37 | 7.1 | 25 µs |
| 131,900 | carried | 2.3 ms | 0.48 and 0.48 | 7.3 | 37 µs |
| 164,900 | carried | 11.3 ms | 0.62 and 0.62 | 7.5 | 37 µs |
| **206,100** | **carried, the knee** | 303 ms | 0.76 and 0.76 | 7.4 | 75 µs |
| 209,400–231,900 | offer not taken | 1.5–7.9 s | 0.76–0.79 each | 7.4–7.7 | 60–90 µs |
| 257,600 | disconnected | 7.9 s | 0.47 each | 8.1 | 95 µs |

The knee is 206,100: run 8 carried 97,700 on the same code without the fix, the prototype
118,700 and one loop 112,100. The pair now does what D1 set out to buy. Each loop reaches
one loop's busy ceiling, 0.76–0.79, so the summed busy at the knee is 1.52, twice one
loop's 0.78; a message still costs 7.4 µs, and 1.52 ÷ 7.4 µs is the knee. Wake lateness
stays under 100 µs all the way up, where run 8's was 339 µs at its knee, so the wake arms'
question does not arise on h3. Above the knee the pair refuses the offer and takes
201,000–205,000 up to 231,900. At 257,600, 25% over, the pending table reaches its count
bound (171,537 evictions at the cap and 227,284 of answered entries) with the acks
withheld, and the generator's 10,000 clients disconnect, none of them closed by the
broker: run 1's mechanism at twice its rate, an ingress pool deeper than the stall budget
once the queues are that long. That bound is the open overload item (D3's time bound),
not a limit of the pair. The fan-in runs are the next measurement.

**Four shards, measured on h3** (2026-11-02, `h3-20261102T214752Z`, commit `573ea754` in
the standard build, `MQTTD_HUB_SHARDS=4`, the ladder from 206,100; the first run with the
rig's host samples, #1188):

| offered | verdict | received | ack p99 |
|---|---|---|---|
| **164,900** | **carried, the knee** (3 rounds) | 164,900 | 2.5 ms |
| 167,500 | disconnected in the second round (the first carried at 68.5 ms) | 137,941 | 1.7 s |
| 170,100 | disconnected | 121,315 | 2.0 s |
| 175,200 | disconnected, twice | 133,037–146,427 | 2.5–3.1 s |
| 185,500 | disconnected, twice | 29,754–58,065 | — |
| 206,100 | disconnected | 112,948 | 1.7 s |

The knee is 164,900, below the pair's 206,100, and no resource sets it. At the knee the
broker uses 7.2 of the host's 32 CPUs (43.7 µs of CPU a message; the machine 10.9 busy),
each loop is 0.37 busy, and the data disk writes 2 MB/s; at 206,100, before its round
failed, the broker used 9.05. Below the knee four shards are quicker than two (ack p99
2.5 ms at 164,900 against the pair's 11.3 ms). Every failed round goes the same way, and
it starts on a 30-second clock. Every `EXPIRY_RECONCILE_EVERY` sweep ticks each shard
spawns the inherited-session scan, a full enumeration of the sessions through the store,
and while it is in flight `takeover_open()` is true (the rig's lone node is `clustered()`:
peer networking is configured), so `may_elide` refuses every hold and every delivery is
written for the scan's duration. Every round shows it, one `cluster` command a shard and a
burst of 8,000–16,000 appends every 30 s; the pair absorbs it, and so do four shards at
164,900. From 167,500 the burst does not end: four scans read the store the writer is
filling, the writer takes 70,000–165,000 appends a second, its commits take 0.1–0.7 s, the
data disk (gp3, 125 MB/s provisioned) writes 109–180 MB/s, and an append takes about a
second. A session with a write in flight on a topic has its next delivery there written
too (ADR 0089 Amendment 1, A5), so with the offer above what the writer can do it never
catches up: at
175,200 the flood held to the end of the round. The pending entries pass the stall
budget, the bound evicts the oldest with the acks withheld, and the generator drops
thousands of its 20,000 clients (4,868 at 206,100, 6,870 at 185,500); the broker closes
none.

What would stop it is the order of the bounds, not their size. The pending table is
bounded (65,536 entries a shard, past them the 1 s budget, under a hard 64 MiB) and so is
elision (16,384 holds and 64 MiB a shard), but nothing between them pushes back on the
publishers: a publish's ingress credit returns to the pool when the hub dispatches it, not
when it is answered, so a stalled durable path keeps every socket read at the full offer
until the pending bound evicts. Three changes, not decided yet: hold the credit until the
PUBACK goes out (the ack path takes none, so there is no cycle; the hand-offs already hold
theirs, D3); size the pool to run dry before an entry can reach the budget (a shard's
64 MiB is about 58,000 publishes, 1.2 s here); and stop the periodic scan from opening a
takeover for elision, only a real takeover window should, with one scan a node rather than
one a shard. The same scan writes every delivery for its duration on one loop too; there
it is absorbed.

**T10 built** (Amendment 2, the maintainer's "fix"; branch `0093-t2-t3`). The publish's
ingress credit stays on its pending entry until its publisher is answered
(`route_publish`, released in `try_complete_pending` or with the entry), so the pool now
covers the answer as well as the queue. A pool nobody configured is at most
`MAX_SHARD_POOL_BYTES` a shard, 65,536 × `COMMAND_OVERHEAD` (about 56 MiB): with one shard
the default falls from 256 MiB to that, with four from 256 MiB to 224 MiB, and a full pool
of the cheapest publishes is the pending table's floor. Write elision's takeover window is
`takeover_window_open`: the eager window, the boot scan or a scan the window started, never
the periodic reconcile; the forwards #1053 holds still wait on any scan. Shard `k` of `K`
runs the periodic scan at tick `k × 30 / K` of the cycle. The one scan a node proposed
above became this stagger: routing one scan's results to every shard would put the scan
state #1053's forwards wait on behind a shared path, for store reads alone once elision
stays on. Tests in `hub/tests/held_credit.rs`; with the three changes undone, the four
tests aimed at them fail.

**T10 measured on h3** (2026-11-02, commit `0b4e243c` in the standard build: four shards in
`h3-20261102T230920Z`, two in `h3-20261102T235219Z`; both ladders from 206,100):

| shards | offered | verdict | received | ack p99 | each loop's busy | hub µs a message | broker CPU |
|---|---|---|---|---|---|---|---|
| 4 | 206,100 | carried | 206,100 | 2.8 ms | 0.46–0.48 | 9.1 | 8.6 cores |
| 4 | 257,600 | carried | 257,599 | 25.4 ms | 0.65–0.66 | 10.2 | 10.8 cores |
| 4 | 289,800 | carried | 289,695 | 400 ms | 0.72–0.73 | 10.0 | 12.7 cores |
| 4 | **297,900** | **carried, the knee** | 297,823 | 612 ms | 0.72 | 9.7 | 13.1 cores |
| 4 | 305,900 | offer not taken | 302,219 | 3.4 s | 0.70–0.73 | 9.4 | 14.0 cores |
| 4 | 322,000 | offer not taken, twice | 295,860–304,413 | 4.8–4.9 s | 0.70 | 9.3 | 14.7 cores |
| 2 | 164,900 | carried | 164,901 | 8.5 ms | 0.62–0.63 | 7.6 | 6.3 cores |
| 2 | 185,500 | carried | 185,515 | 86.5 ms | 0.75 | 8.1 | 7.4 cores |
| 2 | 195,800 | carried | 195,871 | 163 ms | 0.77 | 7.8 | 7.9 cores |
| 2 | **201,000** | **carried, the knee** | 200,958 | 239 ms | 0.77 | 7.7 | 8.1 cores |
| 2 | 206,100 | offer not taken | 201,457 | 4.0 s | 0.75–0.78 | 7.6 | 8.8 cores |

Four shards' knee is 297,900, from 164,900 before T10 and above the pair's 206,100: 9,309
msg/s per vCPU. The scan's bursts are gone: at 206,100 the four shards' scans arrive one
at a time, spread over the cycle, and the writer takes at most 900 appends a second where
every scan used to send it 8,000–16,000; the data disk writes under 1.5 MB/s at every step.
Every step over a knee is OFFER NOT TAKEN, with no client disconnected and nothing dropped:
the pool runs dry and the publishers wait at their sockets (at 206,100 the pair's busier
shard held its whole 58.7 MB pool with 51,578 commands queued), where every earlier build
disconnected them. The pair's knee is 201,000 against 206,100 before, one bisect step
lower: its loops are at their ceiling there either way, and the smaller pool now refuses the
2% that the deeper one used to queue at a 300 ms ack p99. The loops set both knees, the
pair's at 0.77 busy and the four's at 0.72, where 22–27% of a loop's time is the yield every
64 commands and its wake (41–104 µs mean lateness); a message costs the hub 7.7 µs with two
shards and 9.7 with four. The machine is not the limit: at the four-shard knee the broker
uses 13.1 cores, 44 µs of CPU a message, and the host 14.6 of its 32 CPUs, about its 16
physical cores. Not measured: an overload 25% past either knee, where run 1 and the drain
fix's 257,600 disconnected.

**Sixteen workers measured on h3** (2026-11-03, `h3-20261103T103426Z`: the T10 binary of
`0b4e243c`, four shards, `TOKIO_WORKER_THREADS=16`, the m6i.8xlarge's physical cores,
against tokio's default of its 32 vCPUs in `h3-20261102T230920Z`; the ladder from 206,100):

| workers | offered | verdict | received | ack p99 | each loop's busy | hub µs a message | broker CPU |
|---|---|---|---|---|---|---|---|
| 16 | 206,100 | carried | 206,100 | 2.2 ms | 0.42–0.43 | 8.2 | 7.9 cores |
| 16 | 257,600 | carried (round 3 collapsed; its retry carried) | 257,599 | 45.2 ms | 0.50–0.52 | 8.8 | 9.2 cores |
| 16 | 322,000 | carried | 321,961 | 497 ms | 0.69–0.70 | 8.6 | 11.8 cores |
| 16 | **327,100** | **carried, the knee** | 327,756 | 584 ms | 0.69–0.70 | 8.5 | 12.0 cores |
| 16 | 332,100 | failed (round 2 collapsed) | 237,018 | 10.3 s | 0.61–0.62 | 9.4 | 11.0 cores |
| 16 | 342,200 | offer not taken (round 2) | 339,943 | 1.6 s | 0.69 | 8.1 | 12.7 cores |
| 16 | 362,300 | offer not taken | 333,849 | 4.4 s | 0.67–0.68 | 8.1 | 13.2 cores |
| 16 | 402,500 | offer not taken | 335,270 | 4.4 s | 0.68 | 8.1 | 13.2 cores |

The knee is 327,100 against 297,900 with 32 workers, +9.8%: 10,222 msg/s per vCPU. Sixteen
workers do the same work for less. At 322,000, which the 32 refused, the loops cost 8.6 µs a
message against 9.3, the broker 36.7 µs of CPU a message against 49.0 (11.8 cores against
14.7), the host switched context 161,000 times a second against 224,500, and a yielded loop
was back in 79 µs against 112; at the two knees, 8.5 against 9.7 µs of loop a message and
36.6 against 44.0 of CPU. The loops set this knee too, at 0.70 busy with their queues
behind. Below the knee the tail is fatter (45 ms against 25 at 257,600; 2.2 against 2.8 ms
at 206,100 the other way), where a yield waits longer and nothing else is saturated. The
scheduler did not keep the 16 to one thread a core: at the knee 12.3 vCPUs were busy with
both siblings of a core active for 0.4–2.8 core-equivalents of the time, so the
one-thread-a-core case (`CPUAffinity=0-15`) is unmeasured.

Two of the 24 rounds (257,600 round 3, 332,100 round 2) fell into write mode in their first
seconds and stayed there: the writer took 490,000–660,000 ops per 4 s at about 550 ms a
commit, one delivery in eight was elided, the credit pool was 100% in use from the offer's
second second, acks ran 9–11 s, and the drain timed out. The mechanism is ADR 0089
Amendment 1's A5: a delivery whose topic has a write in flight is written too, a write is
in flight for its whole commit, and a 550 ms commit keeps every busy topic in write mode,
which keeps the writer saturated — the bistable path, entered this time where the round's
start found consumers without a free send slot (the 16 workers ramp the connection tasks
slower: 3,119 attach batches were still committing at the offer's start against 1,523 in
the carried round). The 32-worker ladder entered it in none of its 18 rounds. Nothing was
dropped under offered load: every drop counter in both rounds (18,285 and 235,383) moved in
the teardown's last six seconds, after the generator's drain gave up and ended the
consumers' sessions with expiry 0 — hand-offs to members just gone (withheld: the
publisher is closed without an ack, `shards.rs`), publishes after the consumers had left
(no member), and the append lane in the last two seconds — and the ledger's
acknowledged-but-missing messages (10,475 and 1,895) were the consumers' backlogs at that
moment (1.6 MB at 200 B), discarded at the generator's own request. Decided (2026-11-03):
the writer's batch gets a bound, so that a commit, and with it A5's hold on a topic, stays
short; the runtime's workers default to the physical cores; the generator drains its
publishers before its consumers. Each in its own PR, with the amendment of the ADR that
owns it (0089 for the writer, 0092 for the runtime).

**The combined build measured on h3** (2026-11-03, `h3-20261103T130326Z`: this branch with
#1190's writer batch bound, #1193's worker default and #1195's drain order merged locally;
four shards, the ladder from 300,000, stopped by the maintainer after 122,900's three rounds
and 138,300's first; 39 min, about USD 1.60):

| offered | verdict | received | ack p99 | each loop's busy | hub µs a message | elided | the writer |
|---|---|---|---|---|---|---|---|
| 300,000 | failed | 110,979 | 13.1 s | 0.35 | 12.4 | one in four | ~102,000 ops/s, 1,024-op batches at 11–13 ms |
| 240,000 | failed | 123,688 | 12.2 s | 0.37 | 11.7 | one in four | the same |
| 192,000 | failed | 131,479 | 13.1 s | 0.39 | 11.8 | one in three | the same |
| 153,600 | offer not taken | 107,474 | 14.6 s | 0.36 | 12.5 | — | the same |
| **122,900** | **carried** | 122,900 | 1.7 ms | 0.26 | 8.1 | all | idle after the round's start |
| 138,300 | carried (one round; stopped) | 138,300 | 1.9 ms | 0.29 | 8.2 | all | idle |

The writer's batch bound backfires. Every round from 153,600 up, and the 300,000 prime, fell
into write mode in its first seconds and stayed there, where the morning's unbounded build
stayed elided in 22 of 24 rounds up to 342,200. The bound did what it says — batches of
exactly 1,024 ops, commits of 11–13 ms instead of about 550 — and that is the problem: a
write is in flight for its wait in the queue, not its commit alone, and with one fsync per
1,024 ops the writer clears a backlog at about 102,000 ops/s, where the unbounded one, paying
one fsync for everything queued, cleared it at 120,000–165,000. Write mode starts the same way
in both builds (the round's attach writes and first-delivery block reservations, then A5),
and a round leaves it only when the writer catches up: the bounded writer cannot above about
120,000 msg/s, the unbounded one could up to 327,100. The 30 ms the amendment assumed between
two deliveries on one topic was one publisher's interval; a session's topic gets a delivery
every 1.7 ms at 300,000. Neither the disk (63 MB/s of 125, 37% busy) nor the CPU (7.5–8.2
broker cores) was the limit; the loops ran at 0.35 busy with their queues empty, waiting on
credit held by writes. Below the ceiling the broker is unharmed: 122,900 and 138,300 carried
at 1.7–1.9 ms with every delivery elided, and a collapsed round left no hangover for the
next. #1190 is to be withdrawn, its evidence recorded in ADR 0075. The way out of write mode
is A5 itself — a delivery behind a write in flight queued behind it, still elided — which is
the next decision, with the levers for the knee: yielding less often when the loop's queue
is deep (22–27% of a loop's time is the yield every 64 commands and its wake), and eight
shards (the config caps at four; the hand-off bitmask allows eight), with the connection
path's 28 of the 36.6 µs of CPU a message after that. Dealing a `$share` publish to a
member on the publisher's own shard was listed here too, on the reading that three of four
publishes take a hand-off with four shards; the knee's counters say otherwise (the `shard`
class is 0.009 dispatches a publish at 327,100, round 1), since Amendment 1's dealing
already keeps the deal on the origin's shard, so the 8.5 µs a message against 7.7 with two
shards is the dispatches themselves, not the hand-off, and that lever is withdrawn.

The other two held up. The runtime found the 16 physical cores on its own (`runtime: 16
worker threads (the physical cores)` in the broker's log; one bug: the line prints before
`mqttd --version` answers, to fix in #1193). The drain order did what it was built for: in
every collapsed round the consumers received 213,000–226,000 more deliveries after the
publishers closed, no broker drop counter moved, and the ledger's acknowledged-but-missing
count was 0 — the missing were publishes the broker never read before their sockets closed,
which the verdict still calls FAILED ("ledger did not close") where it is the offer not
taken; a refinement for verdict.py.

**Amendment 3 measured on h3, with ADR 0089 Amendment 3** (2026-11-03,
`h3-20261103T162448Z`: this branch — the yield by the queue's depth at its default 1,024,
eight shards allowed — with ADR 0089 Amendment 3 (a delivery behind a write in flight waits
for it, #1198), #1193's worker default and #1195's drain order merged locally; four shards,
the ladder from 300,000; 43 min, about USD 1.80). The per-round counters are the broker's
pre-round scrapes (`scratchpad/round-counters.py`); the hub model is the rig's report:

| offered | verdict | received | ack p99 | each loop's busy | hub µs a message | yields per 1,000 commands | waited behind a write, a round | written: head / timeout | the writer |
|---|---|---|---|---|---|---|---|---|---|
| 300,000 | carried, 3 rounds | 300,004 | 52.7 ± 4.0 ms | 0.62–0.64 | 8.4 | 13.1 | 375,000 | 135,000 / 145,000 | 29 ops a batch, 1.2 ms a commit |
| **375,000** | **carried, 3 rounds** | 378,265 | 3,087 ± 665 ms | 0.82–0.86 | 8.8 | 1.8–2.5 | 274,000–382,000 | 93,000–124,000 / 116,000–158,000 | 24–30 ops, 1.2–1.3 ms |
| 468,800 | offer not taken | 400,841 | 4,399 ms | 0.84 | 8.4 | 1.03 | 210,000 | 66,000 / 94,000 | 24 ops, 1.2 ms |
| 421,900 | void, four rounds on two generator hosts | 390,000–400,000 | 4.0–4.8 s | 0.83–0.84 | 8.4 | — | — | — | — |

The knee is 375,000 against the 16-worker run's 327,100, +14.6%, and the broker took about
400,000 whenever it was offered more. The yield rule did what the amendment said: each loop's
busy ceiling is 0.86 where it was 0.70, and 4 × 0.86 / 8.8 µs is 391,000, the 390,000–400,841
the broker took at 421,900 and 468,800. The yields read the regime: 13.1 per 1,000 commands
at 300,000 (one per 76, the shallow rule with deep spells) and 1.0–2.5 from 375,000 (one per
1,024). The cost of a message did not move (8.4–8.8 µs against 8.5). Latency moved the other
way at the old knee's rate: 300,000 at 52.7 ms where the 16-worker run had 327,100 at 584 ms,
but the ladder's ×1.25 skipped the 330,000–360,000 band and 375,000 carried at 3.1 s, so the
≤ 1 s band reads 300,000 (it was 327,100). ADR 0089 Amendment 3 held every round out of
write mode: 95.9–97.3% of deliveries were acknowledged inside the window, 210,000–382,000 a
round waited behind a write instead of being written, and the writer stayed at 24–30 ops a
batch and 1.2 ms a commit at every rung, against 35,000–73,000 ops and 550 ms in the two
collapsed rounds of the morning; what was still written is the window's timeouts (0.3–0.5%)
and the head rule (0.2–0.4%). The overload is OFFER NOT TAKEN at 468,800 with the
connections kept.

The upper bracket is the generator's, not the broker's: the m6i.4xlarge ran at 77% CPU at
375,000 and 83% at 468,800 with a scheduling-lag p99 of 12.9 ms, and 421,900 voided four
times on two hosts (the generator saturated, sched_lag), so an h3 ladder cannot be judged
above about 400,000 with one generator. The run ended there: the rig's private-net mesh retry
after the second host replacement rebooted the broker host (16:59Z; the one-way-attach retry
of #368 rebooted the wrong side), mqttd was gone, and the ladder was stopped by hand at
17:06Z; the teardown and both label audits were clean. The per-round scrape series and the
host CPU samples (#1188) went with the replaced generator hosts, so this run has no broker
CPU figure. Eight shards (T12) were then run with two generators, the next paragraph; with
the loops at 0.86 the next limit on this host was expected to be its sixteen cores (36.6 µs
of CPU a message on the morning's run is 13.7 cores at 375,000).

**Eight shards measured on h3, with two generator hosts** (2026-11-03,
`h3-20261103T192204Z`: the same build as the four-shard run above, `MQTTD_HUB_SHARDS=8`,
two m6i.4xlarge generators, the ladder from 300,000; 47 min, about USD 2.60). The per-round
counters are the broker's pre-round scrapes (`scratchpad/round-counters.py`); the hub model,
the drops and the busiest consumer's share are the rig's report:

| offered | verdict | received | ack p99 | each loop's busy | hub µs a message | busiest `$share` consumer | dropped, `append-backlog-full` | the writer |
|---|---|---|---|---|---|---|---|---|
| 300,000 | carried, 3 rounds | 299,996 | 2.1 ± 0.0 ms | 0.29–0.31 | 8.1 | up to 2.1× the mean | 0 | — |
| 375,000 | carried, 3 rounds, the third on its retry | 374,995 | 22.5 ± 0.7 ms | 0.38–0.40 | 8.3 | up to 2.2× | 63 in the round that failed | 47 ops a batch, 1.4 ms a commit |
| **380,900** | **carried, 3 rounds** | 380,893 | 23.0 ± 0.4 ms | 0.40–0.41 | 8.6 | up to 2.2× | 0 | 45 ops, 1.4 ms |
| 386,800 | failed in round 2 | 386,222 | 24.7 ± 0.8 ms | 0.41–0.45 | 8.8 | up to 2.2× | 190 | 49 ops, 1.4 ms |
| 398,500 | failed | 397,874 | 22.6 ms | 0.41–0.46 | 8.6 | up to 2.1× | 27 | — |
| 421,900 | failed | 421,501 | 34.8 ms | 0.44–0.51 | 8.8 | up to 2.1× | 17 | — |
| 468,800 | failed, then DISCONNECTED on its retry | 463,427–466,618 | 512 ms; 4.1 s | 0.51–0.58 | 9.5 | up to 2.1× | 478 | 136 ops, 6.1 ms |

The knee moved from 375,000 to 380,900 (+1.6%) and its latency from 3.1 s to 23 ms: every
band (≤ 1 s, ≤ 250 ms, ≤ 100 ms) reads 380,900 where four shards read 300,000, and 300,000
runs at 2.1 ms (52.7 ms before). The loops are not the limit — 0.41 busy each at the knee
against the four-shard run's 0.86 ceiling, 8.6 µs a message, queue maxima about 1,000 — and
nor are the generators (26–41% CPU, lag p99 1.3–2.1 ms). What ends the ladder is loss: from
386,800 every step dropped 17–478 messages as `append-backlog-full`, one hot durable
member's append lane (256 jobs) overflowing, and the rounds name the cause. The busiest
`$share` consumer received 1.7–2.2× the mean consumer's deliveries in every round of this
run, where the four-shard run reported no unevenness at all: T9's shard-local deal. A group's
members hash over eight shards unevenly, each shard deals only among its own, and a shard
with few members of a group loads each of them harder. The write path behind those members
shows the same thing at 468,800: the writer went from 45 ops a batch and 1.4 ms a commit to
136 and 6.1 ms, elision timeouts doubled (793,000 against 392,000 a round), and the retry
round ended DISCONNECTED (missing 4,068, unacked 27,107, ack p99 4.1 s) with the loops at
0.51–0.58 and 917 µs of wake lateness. The hub's capacity at eight shards, 8 × 0.86 / 8.6 µs,
is about 800,000 a second; the next limit after the deal is the host's sixteen cores.

Next is the deal, not the hub: every online member again (the hand-off costs 0.9% of the
knee's time and T9's own measurement found no knee from keeping it local), or a local deal
weighted by the shard's share of the group; the same ladder then says how far the knee climbs
toward the host's CPU. The default stays `hub_shards = 1` (T7) until the cluster paths (T5)
and the control task (T1) are in. The pool as a time bound is withdrawn under T10: at an
offer equal to the capacity the backlog is conserved and only moves into the sockets.

**Corrected 2026-11-04: the loss was the round's start, not the deal.** Read scrape by
scrape, every round of the run from 375,000 up, the three that carried at 380,900 among
them, began with the same storm: in its first eight seconds appends ran at
105,000–160,000 a second at 0.5–1 s each, elision timeouts at 68,000–160,000 a second, and
20,000–29,000 jobs queued in session lanes; from the twelfth second on nearly every delivery
was elided and an append took 1.7 ms. Every `append-backlog-full` drop of the run landed in
those first seconds, and whether a round lost one was chance, so 380,900 against 386,800 is
the transient's tail, not a capacity. The cause is in ADR 0089 (Amendment 4, #1209): a
session's packet-id block was reserved only once spent, so every durable session's first
delivery after its attach was written, and every one after it until the reservation landed.
The uneven deal decided which lanes filled first; its unevenness remains a fairness
finding, not the knee. The next eight-shard run measures the ladder with blocks reserved
ahead.

## Changelog

- **2026-11-02** — ADR 0093 written from the prototype's four runs (ADR 0092 T5, #1163),
  as Proposed; the eight tasks filed under #1180.
- **2026-11-02** — Accepted by the maintainer. T2 and T3 built (branch `0093-t2-t3`,
  under Progress): hand-offs batched per dispatch batch and carrying the publish's
  ingress credit, the pending bound as a time bound, and the two wake arms behind
  `MQTTD_HUB_LIFO_SLOT` and `MQTTD_HUB_HANDOFF_WAKE`. Their rig runs follow.
- **2026-11-02** — Six h3 runs of T2 and T3 (under Progress); the per-pair hand-off credit
  and one ingress pool per shard built on the way. The collapse is gone — the pair holds
  its connections above the knee — but the knee is 100,300 against the prototype's
  118,700, and neither wake arm lifts the pair above one worker's worth (0.89–1.04 summed
  at every knee). The yield's placement is the open question, for an amendment to D4;
  the fan-in runs wait for it.
- **2026-11-02** — Amendment 1 (shard-local `$share` dealing, T9) built and measured on h3
  in two builds (under Progress): the deal stays local and a message costs 1.5 µs less, but
  the knee falls to 97,700 on the standard build. The `tokio_unstable` flag every T2 and T3
  binary carried costs about 5%. Across the eight runs the pair does one loop's worth of
  non-hand-off work at every knee; a CPU profile is the next measurement.
- **2026-11-02** — The CPU profile at the knee (under Progress): the hub loops are not the
  limit. Every connection locks the graceful-shutdown token's one `Notify` on every pass of
  its loop; two loops contend it eight times as hard as one at the same rate, and above
  the knee its futex waits take a third of the CPU. A child token per connection is the
  fix to measure next.
- **2026-11-02** — The drain fix (a child token per connection, `573ea754`) measured on h3
  (under Progress): the knee is 206,100, twice run 8's 97,700 on the same code, with each
  loop at one loop's busy ceiling (1.52 summed). At 25% over the knee the pending bound
  evicts with the acks withheld and the clients disconnect, as in run 1: the pool as a
  time bound is the open overload item.
- **2026-11-02** — Four shards measured on h3 (under Progress), the first run with the
  rig's host samples: the knee is 164,900, with 7.2 of 32 CPUs busy and each loop at 0.37.
  The 30-second inherited-session scan opens a takeover on every shard, which turns
  elision off; from 167,500 the writes swamp the writer and the data disk, the pending
  bound evicts with the acks withheld, and clients disconnect. Holding the ingress credit
  until the PUBACK, a pool that runs dry before the stall budget, and a scan that leaves
  elision on are the changes to decide.
- **2026-11-02** — Amendment 2 accepted ("fix") and T10 built (under Progress): a
  publish's ingress credit held until its publisher is answered, an unconfigured pool at
  most the pending table's floor a shard, and the periodic reconcile scan out of write
  elision's takeover window (ADR 0089 Amendment 2) and staggered over the shards. The
  four-shard and two-shard rig runs are next.
- **2026-11-02** — T10 measured on h3 (under Progress): four shards' knee is 297,900, from
  164,900, and every overload step is OFFER NOT TAKEN with no client disconnected; the
  pair's knee is 201,000, one bisect step below its 206,100. The hub loops set both knees,
  and the machine is at about its physical cores at the four-shard one.
- **2026-11-03** — Sixteen workers measured on h3 (under Progress): with
  `TOKIO_WORKER_THREADS=16`, the physical cores, four shards' knee is 327,100 against
  297,900, for 17% less CPU a message; two rounds fell into write mode (A5 over 550 ms
  commits), and every drop was the teardown's. Decided: bound the writer's batch, workers
  default to the physical cores, the generator drains publishers before consumers.
- **2026-11-03** — The combined build measured on h3 (under Progress): #1190's writer batch
  bound backfires — every round from 153,600 up stayed in write mode, the bounded writer
  clearing a backlog at 102,000 ops/s against 120,000–165,000 unbounded, and the knee fell
  from 327,100 to between 138,300 and 153,600; to be withdrawn. #1193 found the 16 cores;
  #1195's settle received 213,000–226,000 deliveries a round after the publishers closed,
  with no drop and no acknowledged loss.
- 2026-11-03 — Amendment 3: the loop yields by its queue's depth (T11: every 64 commands
  below `node.hub_yield_deep` queued, every 1,024 from it, the housekeeping still every 64,
  `mqttd_hub_yields_total{shard}`) and eight shards (T12: `MAX_SHARDS` 8). The shard-local
  dealing lever withdrawn: the knee's `shard` class is 0.009 a publish, Amendment 1 already
  keeps the deal on the origin's shard. To be measured on h3 with four shards together with
  ADR 0089 Amendment 3 (a delivery behind a write in flight waits for it), then with eight.
- 2026-11-03 — Amendment 3 measured on h3 with ADR 0089 Amendment 3 (`h3-20261103T162448Z`,
  four shards): the knee 375,000 against 327,100 (+14.6%), each loop's busy ceiling 0.86
  against 0.70, 300,000 at 52.7 ms; no round in write mode, 210,000–382,000 deliveries a
  round waiting behind a write instead of being written; the broker took about 400,000 when
  offered more and pushed back (OFFER NOT TAKEN); the h3 generator at its own limit from
  420,000 offered (four void rounds on two hosts), and the rig's mesh retry after the second
  host replacement rebooted the broker host, which ended the run. Eight shards wait for a
  second generator.
- 2026-11-03 — Eight shards measured on h3 with two generator hosts (`h3-20261103T192204Z`):
  the knee 380,900 against four shards' 375,000, at ack p99 23 ms against 3.1 s, so every
  latency band reads 380,900 (four shards: 300,000) and 300,000 runs at 2.1 ms; the loops at
  0.41 busy; the ladder ended by loss — the shard-local deal (T9) loads the busiest `$share`
  member 1.7–2.2× the mean and its append lane overflows from 386,800 (`append-backlog-full`)
  — and 468,800 ended DISCONNECTED on its retry. The pool as a time bound weighed and
  withdrawn under T10.
- 2026-11-04 — Corrected: the eight-shard run's drops all fell in a round's first eight
  seconds, in a write storm every round from 375,000 up shared; the cause is a packet-id
  block reserved only once spent (ADR 0089 Amendment 4, #1209), and the uneven deal only
  decided which lanes filled first. T9's and T12's notes carry the correction.
