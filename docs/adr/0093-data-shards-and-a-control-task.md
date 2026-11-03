# ADR 0093 — K data shards per node and one control task: the hub's split

- **Status:** Accepted
- **Date:** 2026-11-02 (accepted the same day)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0093-data-shards-and-a-control-task.md](../delivery/0093-data-shards-and-a-control-task.md) — plan, progress, and changelog
- **Issues:** [#1163](https://github.com/mbilling/fss-mqtt-broker/issues/1163) (this record, ADR 0092 T5),
  [#1180](https://github.com/mbilling/fss-mqtt-broker/issues/1180) (the tasks),
  [#1178](https://github.com/mbilling/fss-mqtt-broker/pull/1178) (the two-shard prototype)
- **Related:** [ADR 0092](0092-hub-task-ceiling.md) (the hub task bounds a node; D5 is this
  record, Amendments 1 and 3 its floor), [ADR 0082](0082-bounded-hub-ingress.md) (the
  ingress credit a hand-off must carry), [ADR 0061](0061-off-loop-durable-appends.md) (the
  append lanes a shard owns), [ADR 0089](0089-durable-write-elision.md) (the settle window a
  hand-off crosses), [ADR 0015](0015-cluster-shared-subscriptions.md) (`$share` dealing),
  [ADR 0091](0091-capacity-model.md) (the numbers every ceiling here is a function of),
  [ADR 0007](0007-durable-store-integration.md) (placement groups), [ADR 0090](0090-benchmark-rig-redesign.md)
  (the rig every task is measured on),
  [docs/delivery/0092-hub-task-ceiling.md](../delivery/0092-hub-task-ceiling.md) (the four
  prototype runs this record is written from)

> **Written 2026-11-02** from the two-shard prototype's four runs on the rig, the
> maintainer's item 4 of 2026-10-31, after items 1–3 were measured: "fix 1-3 before doing
> the work on the ADR in point 4". It decides the shape of the split and names what only a
> measurement can decide, the way ADR 0092 D4 requires.

## Context

### What ADR 0092 left

One hub loop per node is why a 32-vCPU node carries little more than a 4-vCPU one. After
ADR 0092's T3 the loop costs 6.9 µs of hub time per message on the h3 shape (a publish 5.1,
a consumer PUBACK 1.8) and reaches a busy ceiling of 0.82 before its queue stops keeping
up, so one `m6i.8xlarge` knees at 112,100 msg/s: 3,503 msg/s per vCPU, against 9,817 on a
4-vCPU Hetzner node and HiveMQ 4.18's published 2,812 on the same 32-vCPU shape. The two
levers ADR 0092 tried on the loop's own share of the wall-clock were both withdrawn on
their measurement: a thread of its own (Amendment 1) and the consumer PUBACK off the loop
(Amendment 3). What they found is this record's floor. On this runtime a task's hand-off
is cheap only when the receiver runs next on the sender's worker, in tokio's LIFO slot;
from any other thread every wake is remote, milliseconds late under load, and the durable
path turns that lateness into stalled lanes and expired holds.

### The prototype and its four runs

ADR 0092 D5 required the hand-off's cost to be measured on a two-shard prototype before
this record was written. The prototype (#1178, `node.hub_shards`, `hub/shards.rs`) runs
`K` hub loops on one node behind one `HubTx`: a session belongs to the shard of its
placement group (ADR 0007: `group_of(client) mod K`), a connection pins itself to that
shard after CONNECT, a publish is matched on its publisher's shard, the subscribers that
shard owns are delivered there, and the rest are handed to their shard as one
`ShardDeliver` whose settled deliveries come back as one `ShardDone` — two hand-offs per
cross-shard message, the publisher's ack waiting for them as it waits for a peer's
answer, through the lanes and the elision window alike. Routing state was mirrored per
change (`ShardRoute`), not snapshotted. Measured on one `m6i.8xlarge` against the one
loop's rows, on the h3 shape (20 topics, 500-member `$share` groups, 10,000 publishers)
and on `fanin-qos1` (one topic, one group of 108 members, 20,000 publishers):

| run | knee | ack p99 | hub-µs a message | the hand-off (`shard` class) | the loops at the knee |
|---|---|---|---|---|---|
| one loop, h3 (`h3-20261031T225644Z`) | 112,100 | 354 ms | 6.9 | — | 0.78, ceiling 0.82 |
| one loop, fan-in (`h3-20261101T143016Z`) | 120,400 | 18.7 ms | 5.3 | — | 0.64, ceiling 0.73 |
| two shards, h3 (`h3-20261101T222335Z`) | 118,700 | 835 ms | 8.6 | 2.3 µs × 1.0 a message | 0.51 each, summed 1.02 |
| two shards, fan-in (`h3-20261101T230921Z`) | 120,400 | 1,038 ms | 7.1 | 1.8 µs × 1.0 | 0.43 each, summed 0.85 |
| two shards on their own threads, cores 2–3, h3 (`h3-20261102T063603Z`) | 97,700 | 1,319 ms | 15.2 | 2.9 µs × 1.0 | 0.66 and 0.83, summed 1.49 |
| two shards on their own threads, fan-in (`h3-20261102T073116Z`) | 92,900 | 30.5 ms | 9.2 | 1.7 µs × 1.0 | 0.39 and 0.46, summed 0.85 |

Three things in that table are the decision.

- **The hand-off costs about 2 µs a message and the model predicts it.** Half the deals
  cross shards and each crossing is two dispatches, so the `shard` class runs at one
  dispatch per message: 2.2–2.4 µs on h3, 1.6–1.8 on fan-in. One loop's cost plus the
  hand-off's — 6.9 + 2.3, 5.3 + 1.8 — is what the pair measured, 8.6 and 7.1. The
  origin's publish dispatch is cheaper than the one loop's (4.3 against 5.1 on h3: it
  delivers to nobody when the member is foreign), the receiver's delivery is the
  hand-off's. The second loop buys 6% on h3 and nothing on fan-in.
- **Above its knee the pair does not saturate; it collapses.** No loop passes 0.53 busy
  — 47% of its time not dispatching, the wake after its yield 138–159 µs late against
  the one loop's 37–53 — while its queue holds 100,000 to 300,000 commands, the
  hand-off's round trip reaches a second, the pending-publish bound (65,536 entries a
  loop) evicts the oldest entries with their acks withheld (145,487 in one round at
  131,900 on h3, 158,640 at 152,000 on fan-in), the publishers retry, and the generator's
  clients close (9,984 client-side closes in one round). The one loop at the same offers
  keeps every connection and takes what it can at OFFER NOT TAKEN: ADR 0082's ingress
  credit parks a publisher when the loop is behind, and the hand-off commands take none,
  so nothing holds a shard's queue back. The pair's summed busy at the knee, 1.02 on h3,
  is one worker's worth: a hand-off's wake lands in the sender's worker's LIFO slot, the
  receiver runs there when the sender yields, its own hand-offs wake the sender into the
  same slot, and the two loops take turns on one worker. On fan-in the summed busy at the
  knee, 0.85, is the one loop's 0.64 plus the hand-off's share, with the same ceiling of
  0.43 a loop. The recovery after a collapse is not clean: a retried step on fan-in took
  72 msg/s and FAILED on 67,896 `append-backlog-full` refusals with the brokers holding
  nothing after the drain.
- **Shards on threads of their own lose on every column**, as D1 did in Amendment 1. Every
  dispatch is dearer (the `publish` class at two dispatches a message, the append
  completions coming back one by one), the wake after a yield is 3.5–7.8 ms late, the
  loops are uneven. On h3 that is 18% less throughput at 1.3 s; on fan-in 23% less, and
  the failure changes kind: no collapse, but loss — a consumer's append lane (256 jobs,
  0.3 s of its 860 deliveries a second) overflows behind the remote wake with the loops
  at half busy and their queues under 1,200. Pinning to disjoint cores does not change the
  mechanism.

### What this record must contain

#1163 lists it: shard ownership by placement group; immutable routing, subscription and
placement snapshots published by one control task; the hand-off and completion cost per
message, measured before anything else is written; hot-group dealing and `#`
subscribers; a shard's failure modes; the node's ceilings as functions of ADR 0091's
numbers, with the ENA packet-rate ceiling measured with netprobe on AWS.

## Decision

### D1. A session belongs to one data shard, by placement group

A node runs `K` data shards (`node.hub_shards`, 1 to 4; 1 is today's loop and the
default until D9's acceptance holds). The shard of a client is its placement group folded
onto the node, `group_of(client) mod K`, the same answer on every shard and every
connection from the client id alone, so a connection pins itself to its shard after
CONNECT and nothing on its message path routes. Everything the loop holds for a session
today lives on its shard: the in-flight and outbound state, the backlog, the append lanes
(ADR 0061), the hold queues (ADR 0089), the pending entries of the publishes it
originates. Per-session order is what it is today, the shard's single thread, and a
publisher's stream reaches a subscriber on another shard through one FIFO channel per
ordered pair of shards, so no publisher's messages are reordered on the way. A shard is
the unit of the metrics (`shard` label on the dispatch, queue and lane families) and of
the model: a node's busy is its busiest shard's, its hub-µs per message every shard's.

### D2. One control task owns the routing state and publishes immutable snapshots

Subscribe, unsubscribe, attach, detach, evict, `$share` membership, presence and
placement go to one control task, which owns the filter trie, the shared groups and the
presence map and publishes them as one immutable snapshot behind an atomic pointer swap.
A data shard loads the current snapshot once per dispatch batch — one atomic load, not one
per publish — and matches on it owning nothing else about a foreign client. The
prototype's mirror, one `ShardRoute` to every shard at every change, is replaced: a
change costs one snapshot publication whatever `K` is, and the shards read, never copy.
Two rules make the snapshot safe to read late. A SUBACK, UNSUBACK or CONNACK is written
to the wire after the snapshot that includes the change is published, so a publish that
follows the answer is matched with it; and a session's own shard applies the change to
its session state (the subscription's QoS, its identifier, the backlog's attach) through
a command from the control task, in order with the snapshot. A change is visible to
every shard within one dispatch batch; the control task is a task of the shared runtime
like the shards, and its rate is bounded by connections, not by the publish rate. The
control task also owns what one loop owns today and the shards must agree on: the drain
barrier, the quotas and brownout, the rules and the authorizer, the identity sweep.

### D3. Hand-offs are batched per dispatch batch, carry credit, and complete back to the origin

A publish is matched on its origin shard against the snapshot; the targets the origin
owns are delivered there; the rest are collected per target shard and handed off once
per dispatch batch — one `ShardDeliver` per (origin, target) pair per batch of at most 64
commands, carrying every message of the batch with its targets — and their settled
outcomes come back the same way, one `ShardDone` per pair per batch. The origin's pending
entry waits for every target shard it handed to, as the prototype's does, and the
publisher's ack is released when the last of them answers, through the lanes and the
elision window alike (`AppendGate::Shard`, the peer gate's twin). The hand-off channel is
bounded the way ADR 0082 bounds ingress: a per-pair byte credit that the origin acquires
before it hands off and the target releases after the delivery is settled. An origin
without credit for a target stops taking publishes from the connections that need it —
the credit it cannot get is charged to its ingress pool — so a slow shard parks the
publishers that feed it, as a slow loop parks its own today, and no shard's queue grows
past what its credit allows. The pending-publish bound follows ADR 0091 D6: a time bound
with the stall budget, a count as the floor, so a hand-off that answers in a hundred
milliseconds does not evict entries the way the prototype's did.

### D4. The shards are tasks of the shared runtime, and a hand-off's wake must not capture the receiver

Threads of their own are rejected on the prototype's measurement, as D1 of ADR 0092 was.
The shards stay tasks of the shared runtime, where their wakes of connection tasks and
lanes are the cheap local wakes the one loop has. What the pair measured is that this
runtime also lets two shards fall onto one worker: a wake from a running task is placed
in that worker's LIFO slot and runs next on it, unstealable, so the receiver of a hand-off
runs on the sender's worker and the two loops take turns. The remedy is measured, not
assumed, in T3, between two candidates that each keep the shards on the runtime: a wake
that bypasses the LIFO slot — the runtime built with the slot disabled, or the hand-off's
wake issued through the injection queue once per batch, which D3's batching amortizes
where Amendment 1's per-message wake could not — and the acceptance is the model's: the
summed busy of `K` shards on h3 approaches `K × 0.82` before the knee, and a shard above
its knee saturates at its ceiling instead of collapsing.

### D5. The cluster, retained, relay and admin paths run on the first shard until they are moved

The prototype routes everything peer-facing, retained, relayed and administrative to the
first shard, hands a peer's forward off ungated, lets a relaxed ack wait for the hand-off,
does not deal to an offline persistent `$share` member on another shard, and runs the
rules' note and expiry tasks on the first shard. This record keeps those limits and the
guard that enforces them — a node that names peers refuses `hub_shards > 1` — until T5
moves them: a peer's forward enters its origin shard like a client's publish and is
gated like one, the retained index and the relay become the control task's, the admin API
lists every shard's sessions, and the cluster fault suites run with shards on. Nothing in
a cluster changes before then.

### D6. `$share` dealing runs on the origin; a `#` subscriber is one shard's load

A `$share` publish is dealt on its origin shard against the snapshot, with every member's
presence and the lane and outbound depth its owner shard publishes per batch, so ADR 0091
D4's rule — skip a member that cannot keep up — applies across shards as it does on one
loop, and ADR 0015's selection is unchanged. A hot group's dealing cost, 52% of a publish
on h3, therefore parallelises by publisher: origins are spread over the shards by their
publishers' placement groups, and the delivery to the dealt member stays on the member's
shard. A `#` subscriber, or any filter broad enough to match most of the node's
publishes, receives every publish of every shard through a hand-off: its capacity is one
shard's delivery capacity, the per-session ceiling of ADR 0091, and every publish on the
node pays a hand-off for it. That is the limit this record states for such a subscriber
and T8 measures; a wildcard-heavy workload does not follow the node's cores.

### D7. A shard's failure modes

A shard that panics takes the node down: fail-stop, as the one loop today (ADR 0061 §8),
because its sessions' state cannot be rebuilt from the other shards' snapshots and the
sessions' replicas on other nodes are the recovery (ADR 0006). A slow shard is isolated by
D3's credit: the origins that hand to it park their publishers, its own clients wait on
its queue, and nothing else on the node waits. A stuck dispatch on a shard stalls its own
clients and the hand-offs into it, bounded by the credit, and the control task's `Ping`
names the shard that did not answer. The drain barrier joins every shard, as the
prototype's does. A shard's queue, busy share, wake lateness and lane depths are read per
shard, so the model names the shard that fell behind and the operator sees it on
`/statusz`.

### D8. The node's ceilings, as functions of ADR 0091's numbers

With ADR 0091's symbols and these, measured per host and shape by T1's model:

| Symbol | What | Measured on `m6i.8xlarge` |
|---|---|---|
| C | a shard's busy ceiling | 0.82 on h3, 0.73 on fan-in |
| m | one loop's hub-µs per message | 6.9 on h3, 5.3 on fan-in |
| h | one hand-off dispatch | 2.3 µs on h3, 1.8 on fan-in |
| x | the share of messages that cross shards | 1 − 1/K for a uniform deal; 1 for a `#` subscriber on another shard |
| p | packets a message, both directions | about 6 at QoS 1 (the Hetzner probe: 182,000 packets/s at 30,000 msg/s) |
| P | the host's packet rate without loss | the ENA term, owed by T4 |

- **The CPU term:** a node carries at most `K × C ÷ (m + 2 × h × x)` msg/s. The pair's
  measured cost, 8.6 and 7.1 hub-µs a message, is within 7% of `m + 2hx`. At `K = 2` the
  term is 178,000 on h3 and 231,000 on fan-in; the prototype reached 118,700 and 120,400
  because its loops ran at 0.51 and 0.43, not C, which is what D3 and D4 are for. At
  `K = 4` the term is 317,000 on h3 and 410,000 on fan-in.
- **The network term:** `R × p ≤ P`. At 112,100 msg/s the node moved about 680,000
  packets a second without a drop counter moving, a floor for P, not its value; at
  `K = 4` the CPU term asks for about 1.9 million. Which bound the node reaches first is
  the probe's to say, and the Hetzner probe's finding stands until AWS's is in: loss there
  starts once about 10,000 connections have packets in flight, by connection count, not
  by packet rate, and no AWS run has shown it at 20,000 clients.
- **The session terms** are ADR 0091's, unchanged: a session's rate is one shard's, its
  lane and outbound bounds are time bounds, and a subscriber that matches everything is
  bounded by them whatever `K` is (D6).
- **The cluster terms** are ADR 0091 D2's: disk and replication bytes per node do not
  change with `K`; peer frames are about 45% of a cluster node's commands, and they enter
  the first shard until D5's T5 moves them.

The expected capacity of a node is the least of these, and the rig prints the prediction
before it measures, as ADR 0091 D7 requires; a gap over 25% is a missing term or a defect.

### D9. Every task is measured, and K stays 1 until the pair's knee holds

Each task is measured the way ADR 0092 D4 requires: one `m6i.8xlarge`, the h3 ladder and
the fan-in ladder, read with T1's model per shard, against the prototype's rows in ADR
0092's delivery doc. The default stays `hub_shards = 1` until D3 and D4 are measured and
two shards carry more than one loop at an ack p99 within the one loop's band, holding
their connections above the knee; then the README's per-node figures move, by the rig's
numbers only.

## Amendment 1 (2026-11-02): a shard deals `$share` among its own members first

**What was measured.** T2 and T3's six h3 runs (one `m6i.8xlarge`, two shards; the rows
are in the delivery doc) fit one ratio at every step to within 1%: the knee is the two
loops' summed busy divided by the hub-µs a message. The prototype reached 118,700 at 1.02
busy and 8.6 µs; T2's builds stayed at 0.92–1.04 busy and paid 9.2–9.5 µs, against the
one loop's 6.9. About 2.4 µs of every message is the hand-off, and under D6 as written half
of all deals cross shards on h3 and on `fanin-qos1`: a publish is dealt among every member
of the node, and half of any group's members sit on the other shard.

**The decision changes in one place, D6.** With locality on (`cluster.shared_prefer_local`,
the default), a shard deals a `$share` publish among the online members it owns, by the
same constant-time index — its own online members are kept as a prefix of the group's
online prefix — and hands the publish to the other shard's members only when it owns no
member online. Each shard turns its own cursor, so its members take equal turns: fairness
is kept within a shard and given up across shards, the trade ADR 0077's prefer-local makes
across nodes. Members split unevenly over the shards (a 500-member group about 250 ± 11 a
shard, a 108-member one 54 ± 5), so a member on the less populated side takes a few
percent more, up to about 10% in a small group; ADR 0091 D4's skip of a member that
cannot keep up is unchanged. With locality off, dealing is the node-wide round-robin of
D6. An ordinary subscriber, or a `#` subscriber, on the other shard still takes a
hand-off: D6's limit for a broad subscriber stands.

**What it should buy, to be measured.** On h3 and on `fanin-qos1` no `$share` deal
crosses, so the pair's cost per message falls toward the one loop's 6.9 µs; at the 0.92–1.04
summed busy measured so far, the knee is 133,000–150,000. The cross-shard wakes, the main
suspect for the loops sharing one worker, go with the hand-offs, so the busy may rise too.
One h3 run and one fan-in run against T2 and T3's rows (task T9).

## Amendment 2 (2026-11-02): the bounds that push back come before the ones that drop

**What was measured.** Four shards on h3 (`h3-20261102T214752Z`, the first run with the
rig's host samples; rows in the delivery doc): the knee is 164,900, with the broker at 7.2
of the host's 32 CPUs and each loop at 0.37 busy, so no resource set it. Every failed round
went the same way. Each shard's periodic inherited-session scan turned write elision off for
its flight (ADR 0089 Amendment 2), every 30 s; from 167,500 the writes swamped the writer
and the data disk (gp3, 125 MB/s provisioned), an append took about a second, the pending
entries passed the stall budget, and the bound evicted them with their acks withheld. The
generator then dropped thousands of its 20,000 clients. Nothing pushed back first: a
publish's ingress credit returned to the pool when the hub had dispatched it, so every
socket was read at the full offer until the bound dropped acks. The maintainer's rule, the
same evening: no time bound without a buffer bound that pushes back before it.

**The decision changes in three places.**

- **D3: the credit is held until the answer.** A publish whose publisher waits for an
  answer keeps its ingress credit on its pending entry until that answer goes out, through
  the append lanes, the elision window, the hand-offs and a peer's verdict alike; one nobody
  waits for releases it at dispatch, as before. Nothing an answer waits for takes credit (an
  append, a subscriber's PUBACK, a shard's or a peer's answer), so there is no cycle. A
  durable path that falls behind now parks the publishers at their sockets, OFFER NOT
  TAKEN, where it filled the pending table.
- **D3: the pool is sized to the pending table's floor.** A pool nobody configured (256 MiB,
  or 1/8 of the memory watermark) is at most 65,536 × the per-publish overhead a shard,
  about 56 MiB. A full pool of the cheapest publishes is then the table's count floor, and
  the stall budget, which evicts only over the floor, cannot drop a client publish's ack.
  A configured pool is used as given, with a warning at startup when it is larger.
- **The periodic scan runs on each shard in turn.** Shard `k` of `K` runs it at `k × 30 / K`
  seconds into the 30-second reconcile cycle, so a node's shards no longer enumerate the
  store at once. One scan a node, its results routed to the shards, was the other way. It
  would move every shard's scan state, the flag #1053's held forwards wait on among it, into
  a shared path, to save store reads alone once the scan leaves elision on.

**What it should buy, to be measured.** The four-shard knee rising from 164,900 toward the
loops' ceiling, and a durable path that falls behind shown as OFFER NOT TAKEN, never
DISCONNECTED: one h3 run with four shards and one with two (task T10).

## Consequences

- **The split's cost is known before it is built:** about 2 µs a message for the hand-off,
  a quarter of one loop's work on h3, and the model predicts a node's CPU term from it.
- **The invariants the one loop gives for free are named and kept:** per-session order by
  ownership (D1), the pending table by the origin's bit per shard (D3), the settle window
  by the shard gate (D3), `$share` dealing by the origin against the snapshot (D6).
- **The prototype is not the design.** Its mirror (`ShardRoute`), its per-message hand-off,
  its unbounded hand-off channel and its count-bounded pending table are each replaced
  (D2, D3). What it measured — the hand-off's cost, the collapse, the threads — stands.
- **A node's throughput follows its cores only up to the network's packet rate.** T4's
  probe is the term that says where; until it is measured the README does not claim a
  figure for `K > 1`.
- **A broad subscriber is a stated limit**, not a defect to be fixed by `K`.
- **A node with peers runs one loop** until T5. The README's cluster figures do not move
  before it.

## Alternatives considered

- **Shards on threads of their own, pinned.** Measured twice (ADR 0092 Amendment 1 and the
  prototype's runs 3 and 4): every wake out of a shard becomes remote, every dispatch
  dearer, 18–23% less throughput, and on fan-in loss at the lanes. Rejected on the
  measurement; the knobs stay as the operator experiment they are.
- **A runtime per shard.** The same remote wakes for the connection tasks and lanes a shard
  wakes, which are most of its wakes; rejected without a run, as Amendment 2 answered it
  for one loop.
- **Mirror the routing state per change (the prototype).** `K − 1` commands per change on
  every shard's queue, in front of data; the control path's cost grows with `K` and the
  mirrors can disagree during a change. Replaced by D2's snapshot.
- **Shard by topic instead of by client.** A publish would need no deal across shards, but
  a session's deliveries come from many topics, so its in-flight state, backlog and lanes
  would be split across shards and per-session order would need a merge the loop gives
  for free today. Rejected.
- **Raise the pending-publish bound.** ADR 0091's lesson: a larger count moves the cliff
  and uses more memory; a time bound holds the same stall at every rate (D3).
- **Leave the hand-off channel unbounded and rely on the pending bound.** That is the
  prototype, and the collapse is what it measures. Rejected.
- **QoS 0 straight from connection to connection.** Out of scope, as in ADR 0092: the
  prime target is durable QoS 1.

## Delivery

Nine tasks: the control task and its snapshots (T1), hand-offs with credit and batching
(T2), the worker capture removed by measurement (T3), the ENA packet-rate probe on AWS
(T4), the cluster paths on shards (T5), a shard's failure modes (T6), the four-shard knee
against D8 (T7), the hot-group and `#`-subscriber shapes (T8), and shard-local `$share`
dealing (T9, Amendment 1). Status in the delivery doc.
