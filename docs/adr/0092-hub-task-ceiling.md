# ADR 0092 — The hub task bounds a node: a pinned thread, less work per message, consumer PUBACKs off the loop

- **Status:** Accepted
- **Date:** 2026-10-31 (accepted the same day)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0092-hub-task-ceiling.md](../delivery/0092-hub-task-ceiling.md) — plan, progress, and changelog
- **Issues:** [#1159](https://github.com/mbilling/fss-mqtt-broker/issues/1159) (the model),
  [#1160](https://github.com/mbilling/fss-mqtt-broker/issues/1160) (the thread),
  [#1161](https://github.com/mbilling/fss-mqtt-broker/issues/1161) (the per-message work),
  [#1162](https://github.com/mbilling/fss-mqtt-broker/issues/1162) (PUBACKs),
  [#1163](https://github.com/mbilling/fss-mqtt-broker/issues/1163) (the split, as a later ADR)
- **Related:** [ADR 0082](0082-bounded-hub-ingress.md) (the control lane and ingress credits),
  [ADR 0061](0061-off-loop-durable-appends.md) (appends already off the loop),
  [ADR 0071](0071-owner-side-group-commit.md) (one writer per node),
  [ADR 0089](0089-durable-write-elision.md) (what a PUBACK discharges),
  [ADR 0091](0091-capacity-model.md) (the per-node CPU term this ADR supplies),
  [ADR 0090](0090-benchmark-rig-redesign.md) (the rig every task is measured on),
  [docs/benchmarks/RIG2-RESULTS.md](../benchmarks/RIG2-RESULTS.md) (the AWS runs)

> **Decided 2026-10-31** by the maintainer, from the AWS runs of 2026-10-30 and 2026-10-31.
> Of the four options put to them — a dedicated pinned hub thread, less work per message,
> consumer PUBACKs off the hub, and a control-plane/data-plane split — "I like 1-3", and
> "fix 1-3 before doing the work on the ADR in point 4".

## Context

A node's durable QoS 1 throughput does not follow its vCPU. On the rig of ADR 0090, one
4-vCPU Hetzner CCX23 carries 92,900 msg/s with one copy and one 32-vCPU AWS `m6i.8xlarge`
88,700. In clusters of two copies, 4-vCPU nodes carry 9,817 msg/s per vCPU, 16-vCPU nodes
4,066 and 32-vCPU nodes 2,821, level with HiveMQ 4.18's published 2,812 on the same shape.
Six 16-vCPU nodes knee at 390,300 msg/s with every node's hub dispatching 82% of wall-clock
time and a standing queue; the step above lost messages in one round of three because a
20 s durable-write transient on one node filled a hub queue that had no headroom to drain.

The hub is one task per node: `tokio::spawn(hub.run())` on the shared multi-thread runtime,
a loop that ingests commands, dispatches them by class (`publish`, `ack`, `attach`,
`subscribe`, `cluster`, `control`, `sweep`) and yields every 64. Its per-message cost,
fitted from the runs' 2-second scrapes as busy = load × (a per publish + b per consumer
PUBACK):

| host | a, per publish | b, per consumer PUBACK | busy at the loop's ceiling |
|---|---|---|---|
| `m6i.8xlarge` (Ice Lake) | 6.4–7.3 µs | about 2 µs | 0.83 |
| `m6i.4xlarge` (Ice Lake) | 9.7 µs | 2.7–3.0 µs | 0.82 |
| `m7a.8xlarge` (EPYC Genoa) | 8% cheaper than m6i | | 0.70 |

Three things in that table are the decision:

- **The loop waits for the runtime.** It yields every 64 commands, and on the shared
  runtime each yield waits for a wake: 24–50 µs on Ice Lake and 200–360 µs on the four-CCD
  Genoa (`mqttd_runtime_wake_seconds`), against about 0.6 ms of dispatching between yields.
  At its ceiling the loop dispatches 83% of wall-clock on Ice Lake and 70% on Genoa; the
  rest, 17% and 30%, is that wait, ingest and migrations. It is why the `m7a` node is
  slower (69,700 msg/s) although each of its dispatches is cheaper.
- **Most of the per-message time is not routing.** The flat profile at 67,500 msg/s on
  `m6i.8xlarge`: kernel 39%, the tokio runtime 18%, hub-named symbols 11.7%, memcpy and
  allocation 10%, connection tasks 6.5%, context switches and wake-ups about 14%, vDSO
  clock reads 1.5%, `Sleep::poll` 1.1% and an AppArmor hook 0.8% per packet.
- **A subscriber's PUBACK is a hub command.** `HubCommand::PubAck` is dispatched on the loop
  (`dispatch_pub_ack`, `complete_pending`), about 2–3 µs each: about 20% of the hub's time at
  QoS 1 with one subscriber per message.

Nothing else is the limit. The durable writer is at most a quarter busy, the delivery-stage
timers of #1153 are sub-millisecond, the AWS network is clean, and ADR 0061 and ADR 0082
already moved appends and ingress back-pressure off the loop. The hub's own time per
message, and where that time runs, is what sets every per-node figure in the README.

## Decision

### D1. The hub loop runs on its own OS thread, pinned to a core where the host allows

The hub gets a dedicated std thread with a current-thread runtime (or a `LocalSet`); the
connection tasks, lanes, peers and the writer stay on the shared runtime, and the hub's
channels do not change. Pinning uses `rustix`'s `sched_setaffinity` (the workspace forbids
`unsafe`) and is configured, not assumed: `MQTTD_HUB_THREAD` (own thread, on by default)
and `MQTTD_HUB_CORE` (pinning, off by default). `/statusz` and a gauge name the thread and
the core. The upside is bounded by what the loop spends not dispatching at its ceiling, 17%
of wall-clock on Ice Lake and 30% on Genoa, and is taken before anything else because the
change is small and the measurement exists.

### D2. Less work per message on the loop, in the order the profile gives

With frame pointers in the build, the profile is read again and the loop's per-message work
is cut in that order: the hasher behind the topic and client maps, one clock read per
dispatch instead of per metric, histogram observes batched per dispatch, the payload and
per-delivery allocations. No behaviour changes. The target is about 7 → about 5 µs per
publish on `m6i`, and the yardstick is the model's a and b, re-fitted on the rig after each
change, not a micro-benchmark.

### D3. Consumer PUBACKs complete on the connection task

The connection task owns its session's outbound in-flight state and completes a delivery
when the PUBACK arrives. The hub learns of it in batches, and only where it has to act: the
durable truncation of the acknowledged copy, the elision discharge of ADR 0089, the
obligation ledger. QoS 2 stays on the hub. The invariants that move with it are the ones
ADR 0089 T1 pinned (durable before the wire, the ledger), `inflight_durability`'s crash case
and the cluster fault suites with elision on; they are the acceptance.

### D4. The model is a script, and every task is measured by it

The fit of a and b from a run's scrapes, and the share of the hub's time lost to wake-ups,
becomes a script under `bench/scale/rig2/` that the report cites. D1–D3 are each measured on
the rig of ADR 0090, on one `m6i.8xlarge` (the per-node ceiling, one copy) and on the
shapes already in RIG2-RESULTS, with the same workload, before and after. A task is done
when its mechanism exists and its measurement is in the delivery doc, whatever the number.

### D5. The control-plane/data-plane split is a later ADR, after D1–D3 are measured

Sharding the hub (K data shards per node owning disjoint placement groups, one control task
publishing immutable routing, subscription and placement snapshots) is what lets a node's
throughput follow its cores. It also changes invariants the single loop gives for free:
per-session order, the pending table, the settle window, `$share` dealing. It is a decision
record of its own, written when the profile after D1–D3 says what the per-message work is,
and it is not started before then. What that ADR must contain is listed in #1163; the
two-shard prototype that measures the hand-off cost comes before any of it is written.

## Consequences

- **A core is the hub's.** With D1 on, a node's capacity planning counts one core for the
  loop; `/statusz` says which. ADR 0091's "per-node CPU limit" gets its formula from D4:
  about 1 ÷ (a + b × PUBACKs per message) msg/s per node, with a and b measured per host.
- **The README's per-node figures move with each task**, and only by the rig's figures: the
  delivery doc records the before and after of each, and the README keeps citing
  RIG2-RESULTS.
- **The PUBACK path crosses a boundary it did not cross before.** D3 is the one task that can
  lose a message if the obligation ledger and the truncation order are wrong; its tests come
  first, as ADR 0089 T1's did.
- **The split waits.** D5 fixes the order the maintainer set: a node is made cheaper to run
  as one loop before it is made into several.

## Alternatives considered

- **The split first.** It is the structural fix and the largest upside. Rejected as the first
  step: D1–D3 are small, each is measured in a day, and the split's own cost per message
  (two hand-offs) cannot be estimated until the profile shows what the per-message work is.
- **More runtime workers.** The hub is one task; more workers give it nothing and raise the
  migration loss D1 removes.
- **A different async runtime.** No evidence that the runtime, rather than where the hub task
  runs on it, is the cost; D1 settles that question cheaply.
- **QoS 0 straight from connection to connection, bypassing the hub.** Out of scope: the
  README's prime target is durable QoS 1 (the maintainer, 2026-10-21), and routing without the
  hub's view of the subscription table is a different design.

## Delivery

Five tasks: the model as a script (T1), the dedicated pinned thread (T2), the per-message
work (T3), PUBACKs off the loop (T4), and the control-plane/data-plane ADR after T2–T4 are
measured (T5). Status in the delivery doc.
