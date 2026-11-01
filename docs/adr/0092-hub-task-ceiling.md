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

*Withdrawn by Amendment 1 (2026-10-31, below): built, measured, a regression. The knob
ships off by default.*

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

## Amendment 1 (2026-10-31): D1 withdrawn on its own measurement

D1 was built (T2: `Hub::spawn_dedicated`, `node.hub_thread`, `node.hub_core`, the gauge
and the `/statusz` block) and measured on the rig the way D4 requires, on one
`m6i.8xlarge` with the h3 workload, against the baseline run of 2026-10-30 on the same host
(run `h3-20261031T155400Z` against `h3-20261030T171933Z`). At the same 67,500 msg/s:

| | hub a task of the shared runtime | hub on its own thread |
|---|---|---|
| publisher ack, mean | 0.20 ms | 63.9 ms |
| a delivery waiting for its connection task | 0.01 ms | 17.2 ms |
| the shared runtime's wake lateness | 0.02 ms | 6.9 ms |
| append: lane queue / local durable / fsync | 0.02 / 3.4 / 0.9 ms | 23 / 67 / 1.6 ms |
| elision holds that timed out | 0% | 22% |
| hub busy share | 0.59 | 0.80 |

The knee fell from 88,700 msg/s at ack p99 in the milliseconds to 68,600 at 533 ms, 23%
less. The loop's busy ceiling did rise, from 0.83 to 0.85, so the yield's wait was real
and was removed. What the measurement shows is what that wait had been paying for: on the
shared runtime, a task the loop wakes (a connection writer, an append lane) ran on the
loop's own worker at once, through tokio's LIFO slot; the 17% of wall-clock the loop did
not dispatch was that work. From a thread of its own, every such wake is a remote wake
through the runtime's injection queue: about 3 µs more per message on the loop's side, and
7 ms of lateness for everything it hands off, which the durable path turns into stalled
lanes, oversized batches and expired elision holds.

**The decision changes in two places.** D1 is withdrawn: the hub stays a task of the shared
runtime. The thread and the pin ship as an operator experiment, off by default, because
the mechanism and its measurement are the record and a later runtime may change the
figures. D2 and D3 stand, and D3 gains a reason: a hand-off per message is a cost on
either side of a runtime boundary, so the PUBACK that no longer crosses the loop is worth
more than its 2 µs. D5's split inherits the lesson before it is written: shards that hand
messages to each other pay this same wake unless the hand-off is batched and the
receiver already running. Pinning (A') and the Genoa host (B, B') were not run: they
cannot change the mechanism.

## Amendment 2 (2026-10-31): D2 in its profile's order, measured; the ceiling; what is not yet measured

D2 named its work from the flat profile of 2026-10-30: the hasher, one clock read per
dispatch, batched observes, the allocations. The frame-pointer call graph of the hub task
(run `h3-20261031T205637Z`, one `m6i.8xlarge`, 97,159 samples with the hub task on 21% of
them; the whole breakdown is in the delivery doc) orders it differently, and the two
largest items were not on D2's list:

| share of the hub task at 84,400 msg/s | what |
|---|---|
| 13.5% | `memcpy`: the command's dispatch future, 5,832 B, built and moved once per command — `dispatch`'s future carries the locals of every arm |
| 11% | the `$share` plan: `plan_shared` and the shared table's index walk |
| 10% | SipHash on the `ClientId`, `NodeId`, filter and topic-level keys |
| 7–9% | allocation: `Box<Outgoing>`, the `Message` topic copies, the index's level vector, the per-session hold queue rebuilt on every publish |
| 5.3% | the sub-id walk over every subscription of a subscriber, per delivery, for identifiers it does not carry |
| 3.5% | the prometheus observes with their label `String`s |
| 0.5% | clock reads |

**The first batch** — the dispatch future to 2,488 B with the PUBACK taking none and the
rare arms boxed, the metric handles resolved once, the sub-id walk only for subscriptions
that carry an identifier, the elision key and queues, the empty-backlog drain (#1170) —
measured the way D4 requires, on run `h3-20261031T215218Z` against main re-measured the
same day (`h3-20261031T194739Z`): a publish dispatch 6.7 → 5.3 µs at 67,500 msg/s, the
hub's busy share there 0.58 → 0.48, the publisher's ack p99 at 84,400 msg/s 103–119 ms →
5.3 ms, the knee 92,400 → 105,500 msg/s on one `m6i.8xlarge`, bracketed at 107,200. The
busy ceiling did not move: 0.83. **The second batch** — the hasher and the filter trie's
wildcard children as fields (#1171) — measured on run `h3-20261031T225644Z` the same way:
a publish 5.3 → 4.8 µs at 67,500 msg/s, the busy share there 0.48 → 0.44, the knee
105,500 → 112,100 msg/s, bracketed at 115,400; the ceiling 0.82. Against main, the two
batches together: a publish 6.7 → 4.8 µs, the knee 92,400 → 112,100 (+21%) — D2's
target, about 5 µs, reached. On three `m6i.8xlarge` (runs `h3-20261031T234941Z` and
`h3-20261101T012140Z`) the knee went 270,800 → 299,300 → 306,400 msg/s, at ack p99
1.17 s → 270 ms → 92 ms, and a publish at the knee 7.0 → 6.2 → 5.6 µs: 13% against the
node's 21%, because a cluster's per-message work includes the peer frames, which the two
batches touched only through the hasher. The delivery doc's evidence table holds the four
runs.

**D2 changes in three places.**

1. The order is the table's; the clock reads are struck from it.
2. A stop rule, which D2 lacked: T3 ends when the next item on the profile is under 2% of
   the hub task, or when a batch moves the knee by less than the rig's 3% bracket. What is
   left after the second batch — the plan's scratch allocations, the topic copies, the
   stale hold deadlines, about 1% each — is below that line and not worth a rig run.
3. The yardstick gains the ceiling. A node's limit is the loop's busy ceiling divided by
   (a + b × PUBACKs per message), not 1 divided by it: 0.83 ÷ (5.6 + 1.9) µs is about
   110,000 msg/s on `m6i`, which is where the knee is. ADR 0091's per-node CPU limit takes
   this form.

**What the profile adds to D3 and D5.** The ceiling did not follow the per-message cost
down, so it is now the larger of the two levers. The 17% the loop does not dispatch is the
work it wakes (Amendment 1): two wakes per message, the delivery to the subscriber's
connection task and the publisher's ack through its oneshot, 3.4% of the hub task on the
loop's side and the rest on the receivers'. D3 removes the PUBACK's crossing; the delivery
and the ack crossings have no decision, and batching them per connection is the next lever
on the ceiling — the same cost D5's shards pay for every hand-off, in the same measure.
The kernel's receive softirq also runs on whichever core the loop is on, 8.6% of the hub
task's stacks; nothing steers it while the hub is an unpinned task, and D5's shards, if
pinned, could have it steered away. The "different runtime" alternative was answered by
Amendment 1, not by D1: the loop gains from this runtime's LIFO slot, and a runtime
without it would pay what the dedicated thread paid.

**What is not yet measured.** The profile and both batches are one node under the h3
`$share` workload, where the shared plan is 52% of a publish's time: a and b are for that
shape, and the ordinary-subscriber path, which builds a set of matching clients per
publish, has no profile. In a cluster, peer frames are about 45% of the commands and that
path now runs boxed (one allocation per frame); D4's before-and-after on the shapes in
RIG2-RESULTS — three `m6i.8xlarge`, six `m6i.4xlarge` — is owed, and the README's cluster
figures do not move before it. The per-node figure moves with #1170.

## Consequences

- **The hub stays where it was.** No core is reserved; `/statusz` and the gauge say so
  (`thread="shared"`). ADR 0091's "per-node CPU limit" gets its formula from D4:
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

Five tasks: the model as a script (T1), the dedicated pinned thread (T2, built and
measured, shipped off by default under Amendment 1), the per-message work (T3), PUBACKs
off the loop (T4), and the control-plane/data-plane ADR after T3 and T4 are measured (T5).
Status in the delivery doc.
