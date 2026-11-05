# ADR 0037 — Durable single-owner retained messages (clock-free convergence)

- **Status:** Accepted
- **Date:** 2026-07-03 (accepted 2026-07-04 — all phases delivered)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0037-durable-retained-messages.md](../delivery/0037-durable-retained-messages.md) — plan, progress, and changelog
- **Amended:** [Amendment 1](#amendment-1-2026-11-05-eventual-by-default-strong-by-configuration)
  (2026-11-05, issue [#1214](https://github.com/mbilling/fss-mqtt-broker/issues/1214)):
  retained messages are `eventual` by default, and the decision below becomes the `strong`
  mode, chosen by configuration
- **Related:** [ADR 0014](0014-cross-node-retained.md) (the best-effort broadcast model this
  revises — its §3 gap-fill and the 0014-T7 divergence question are the motivation),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (the lease-owner + group-log machinery this reuses), [ADR 0021](0021-bounded-lease-voters.md)
  (bounded voters those groups run on), [ADR 0018](0018-on-disk-persistence.md) (the durable
  log that makes retained state restart-safe), [ADR 0023](0023-gossip-anti-replay.md) (the
  clock-free stance this preserves), [ADR 0029](0029-durable-by-default.md) (durable is the
  default, so this is the default retained behaviour)

> This record states the decision only. How it is being built and how far along it is live in
> the [delivery doc](../delivery/0037-durable-retained-messages.md).

## Context

ADR 0014 replicates retained messages **best-effort**: a retained publish fans out to every
peer, each node applies whatever arrives, and link-up back-fill is deliberately
**gap-fill-only** (a topic you already hold is never overwritten). That model has two
divergence classes with no repair path:

1. **Partition heal** (the recorded 0014-T7 gap): both sides retain different values for the
   same topic during a partition; after the heal, gap-fill keeps each side's own value —
   **permanently**, until some client happens to republish that topic. A subscriber sees
   device state X or Y depending on which node it lands on.
2. **The everyday race**: two concurrent retained publishes to the same topic on different
   nodes cross-forward, and each node applies the *other's* value last — divergence with no
   partition at all.

Both are invisible today (nothing detects them) and both violate the single-logical-broker
expectation that is the whole point of clustering retained messages — the last-value-cache
use (device shadows, status topics) is exactly where two confident, contradictory answers
hurt most.

Resolving divergence **after the fact** requires deciding which write was "last" across
concurrent writers. Every such rule (last-write-wins on wall-clock, or bounded by hybrid
logical clocks) puts **clocks into the trust base** — a fast-clocked node wins retroactively
— and **silently discards an acknowledged publish**. This codebase has deliberately kept
wall-clock out of correctness (ADR 0023's anti-replay is clock-free for exactly this
reason). We choose to keep it that way.

## Decision

**Prevent retained conflicts from forming instead of resolving them: route every retained
write through the topic's placement-group lease-owner and commit it in the group's
quorum-replicated log. Convergence tokens are consensus-issued (lease epoch + log offset) —
no wall-clock anywhere. Every node keeps a local retained cache, warmed by commit fan-out
and healed by an offset-aware digest, so subscribe-time replay stays a local read.**

This deliberately **revises ADR 0014's "rejected as the default"** verdict on durable-plane
retained: that judgment predates the T7 divergence analysis, the everyday-race finding, and
the no-clock resolution decision. What stands from 0014 is the *read* model (local replay,
broadcast-warmed) and its back-fill machinery (digest + chunking, 0014-T6/T8), which this
ADR re-bases on committed values rather than raw gossip.

### 1. Authority: the group lease-owner commits retained writes

`group_of(topic)` (the existing placement hash) assigns each topic to one of the 256
placement groups; the group's **lease-holder** (ADR 0007, epoch-fenced, bounded voters per
ADR 0021) is the single writer for that topic's retained state. A retained publish landing
on any node routes the *retained set/clear* to the owner through the same group-routed path
session ops use; the owner appends it to the group log under a retained key
(`ret/<topic>`), quorum-commits it, and compacts the key to its last value (a zero-length
clear is a committed tombstone, versioned like any value). Fencing, takeover, and
restart-recovery are inherited verbatim from the session store — no new consensus code.

The **live delivery** of the publish (to current subscribers, ADR 0014 §1/§2) is unchanged
and stays best-effort/immediate; only the *retained state mutation* gains an authority.

### 2. The convergence token: (lease epoch, log offset)

Every committed retained value carries the pair `(epoch, offset)` from its commit — a total
order per topic issued by consensus, not by any clock. All cache/back-fill decisions below
reduce to "higher `(epoch, offset)` wins", which is deterministic on every node and
tolerates owner changes (a new owner's higher epoch supersedes).

### 3. Node-local caches, warmed by commit fan-out

Subscribe-time replay must stay a **local read** (ADR 0014 rightly rejected
fetch-on-subscribe). Every node keeps its retained store (in-memory or redb, as today) as a
**cache of committed values**: after commit, the owner fans the update out to all peers —
the ADR 0014 broadcast, now carrying the token and sent *post-commit* — and each cache
applies it only if the token exceeds what it holds for that topic (monotonic, idempotent,
order-insensitive).

### 4. Heal and join: the digest becomes offset-aware

Link-up back-fill keeps the 0014-T6/T8 shape (digest offer → pull → chunked snapshot) with
one change: entries carry the token, and the receiver takes the **higher-token value per
topic** instead of gap-filling only missing topics. Divergent caches therefore converge
deterministically on heal; the authoritative copy in the group log makes the outcome
identical no matter which pairs of nodes exchange first.

**As delivered — tokens across a restart (issues #214, #183).** The per-topic token map
this section's decisions ride on is in-memory and dies with the process, while the cache
(and the keyspace) do not — an asymmetry that produced two real bugs. A restarted node
exported its cached values *untokened*, so peers holding the previous value's token fenced
the repair out as stale, permanently (#214); and a **tombstone** leaves no cache entry at
all, so a restarted node stopped advertising or exporting its clears entirely — a peer
that missed the clear kept the deleted value until the topic's next write (#183):
retraction was not durable in any exportable sense. The delivered rule: **the keyspace is
the recovery source for the token map, not just the write-time authority.** On the
anti-entropy cadence and before building any snapshot, a node re-adopts every committed
record it can read (`topics()` scan; values *and* tombstones) whose token it no longer
holds — re-arming fences, repairing a reopened cache that predates its own commits, and
restoring the §4 export guarantee after a restart. Per-topic authority re-reads at export
time cover the window before the first warm. No schema was added: the committed record
already carried everything (0037-P10's finding, extended from read-through to
enumeration).

**Tombstone retention (the growth bound — as delivered, issue #229).** A time-based
floor was rejected here from the start: a node absent longer than any fixed floor
returns un-fenced and resurrects the deleted value. The delivered bound is
**convergence-gated discharge**: the anti-entropy digest exchange already yields an
observable "this pair has converged" instant per peer, and the durable raft membership
ROSTER (voters and learners — a crashed member stays on it until decommissioned, a
member the process cannot even name blocks outright) is exactly the set of nodes that
may still return holding pre-clear state. Once every roster member's digest has matched
after a clear was observed, and a full anti-entropy period has passed, the fence is
redundant by construction: each node discharges its in-memory token independently off
the same signals, and the topic's group owner also removes the keyspace record (a
record resurfacing from an old replica merely re-arms a redundant fence). The
`retained_tombstones` gauge counts fences still held — sustained growth means an absent
roster member or chronic divergence, both alert-worthy on their own. Re-attaching a
DECOMMISSIONED node's old data dir is outside this contract (its departure was drained
and content-verified; the wipe-and-rejoin posture applies).

### 5. Partition semantics: queue-until-heal (CP, bounded)

Single-owner is a CP choice: on the ownerless/minority side of a partition a retained write
cannot commit. The publish itself is still delivered live to local subscribers; its
**retained mutation is queued** (bounded per node; oldest dropped with a loud counter when
the bound is hit) and submitted to the owner on heal, where it commits in arrival order.
The trade, explicitly: today a partition costs retained **consistency** (permanent
divergence); under this ADR it costs retained **freshness** on the minority side (staleness
that self-heals). Tentative local application — instant visibility at the price of
reintroducing divergence — is rejected.

### 6. Durable-off fallback

With durable sessions explicitly opted out (`MQTTD_DURABLE_SESSIONS=0`, ADR 0029) there is
no lease plane; retained falls back to ADR 0014 best-effort behaviour unchanged, and the
divergence caveat applies. Durable is the default, so the default gets convergence.

### 7. Detection first — the migration's measuring stick

Phase 1 lands **divergence detection** independently of the migration: the link-up digest
gains a value hash, and a divergence between peers is surfaced as a `warn!` plus a
`retained_divergence_total` metric. This gives a baseline (how often divergence really
happens), a regression alarm, and — after the migration — the proof that convergence holds
(the counter stops moving).

## Consequences

- **Good:** retained conflicts cannot form; a healed partition converges deterministically;
  no wall-clock in correctness (epochs/offsets only); no acknowledged write is *silently*
  discarded (a queued minority write either commits on heal or is dropped **loudly** at the
  bound); restart recovery and owner takeover are inherited from the battle-tested session
  machinery; parity with mature clustered brokers on retained convergence.
- **Cost:** a retained mutation now waits for a quorum commit (retained publishes are
  infrequent; live delivery is not delayed); the group log gains a retained keyspace with
  last-value compaction; caches add token bookkeeping; the 0014-T6 digest is extended.
- **Trade-off accepted:** minority-side retained **staleness during partitions** (bounded
  queue, self-healing) in exchange for never diverging — the right trade for
  device-state/last-value use.
- **Risk:** this touches the durable plane's scope and the retained path end to end. It is
  built strictly test-first on the existing pure cores, phased so each step lands green, and
  the detection metric (§7) guards the cutover.

## Alternatives considered

- **Last-write-wins on wall-clock (or HLC) timestamps.** Resolves after the fact; puts
  clocks into the trust base (a fast clock wins retroactively; HLC bounds but does not
  remove this and adds machinery) and silently discards acknowledged writes. Rejected —
  and with it the whole resolve-later family, since retained values admit no semantic merge.
- **Owner orders, gossip stores (no durable log).** The owner stamps per-topic sequences but
  storage stays gossip-replicated. Lighter on paper, but owner failover then needs its own
  high-water recovery and epoch fencing — re-implementing precisely the parts of the durable
  plane that are hard, without its tests. Rejected.
- **One global retained owner.** A single node serializes all retained writes: a throughput
  bottleneck and a failover pinch-point, strictly worse than the per-group ownership
  placement already provides. Rejected.
- **Keep ADR 0014 as-is (accept divergence).** Fails the single-logical-broker expectation
  for exactly the state retained messages exist to cache; divergence is silent and
  unbounded in time. Rejected as the default; retained under explicit durable-off keeps it
  (§6).
- **Fetch retained from the owner on subscribe.** Re-rejected for the same latency reasons
  as ADR 0014: it taxes the common subscribe path to serve the rare heal.

## Amendment 1 (2026-11-05): eventual by default, strong by configuration

> **Decided 2026-11-05** by the maintainer, after sizing a deployment that retains every
> reading: "we should be AP by default for retained messages, but it should be possible to
> configure CP for retained messages." Issue
> [#1214](https://github.com/mbilling/fss-mqtt-broker/issues/1214).

### Why

This record's decision is a CP design. Its cost was accepted on the assumption that
retained publishes are infrequent. What one retained publish costs in 1.3.0, read from the
code:

- **The publisher waits for it.** A QoS 1 retained publish's ack waits until the topic's
  owner has committed the value with both copies (`hub/mod.rs`, `obligations_done`:
  `!p.awaiting_retained`).
- **One at a time per node.** A node keeps one hand-off of its own in flight, and one
  commit slot that the mutations peers route to it share (`hub/retained.rs`,
  `kick_retained_queue`). A commit is two replicated writes: the value, then its compaction
  (`mqtt-storage/src/retained_log.rs`, `write`).
- **Every node flushes every update on its hub loop.** The owner sends the committed value
  to every peer. Each applies it on the hub loop through `PersistentRetainedStore::set`,
  which is one redb transaction with an immediate fsync, and the loop waits for it
  (`hub/mod.rs`, `RemoteRetainedUpdate`; `mqtt-storage/src/persistent_retained.rs`,
  `persist`).
- **The queue drops on every path.** Every mutation passes the 1,024-entry queue. When it
  is full, the oldest mutation is dropped with its publish, which then never acks
  (`hub/retained.rs`, `enqueue_retained_mutation`). It is not only §5's heal-time queue,
  which is how SIZING.md describes it.

That chain is about five flushes and three round trips for each commit. From it, a cluster
carries a few hundred retained publishes a second, and as it nears that, every node's hub
loop spends most of its time waiting on flushes. This is an estimate from the code's
ordering; nothing has measured it. A power plant whose bridge retains every reading
publishes 10,000 a second on its own.

The brokers mqttd is compared with keep retained state available instead (read on
2026-11-05). Mosquitto holds it in memory unless persistence is on, and then saves it every
30 minutes by default, or every 5 seconds with its SQLite plugin. EMQX writes it to every
core node without a transaction, in memory by default, and its autoheal restarts the
minority side of a healed split. HiveMQ keeps 2 copies and describes its cluster as
favouring availability. A last-value cache mostly needs this: the newest value, on every
node, soon, kept through the loss of a node.

### Decision

**1. Two modes, one per cluster.** `cluster.retained_consistency`
(`MQTTD_RETAINED_CONSISTENCY`) takes two values:

- `eventual`, the default: points 2 to 5.
- `strong`: this record's decision, §1 to §7, with point 6.

The names are rule state's (ADR 0085): `eventual` means every node holds the data and the
copies converge, and `strong` is the mode that waits. With durable sessions off (§6) there
is no lease plane to be strong with, so retained keeps ADR 0014's behaviour whatever this
is set to. Whether that path should move to `eventual` as well is left open.

**2. Eventual: the write.** The node a retained publish lands on stamps the value with its
hybrid logical clock (ADR 0085 D6, `mqtt_rules::crdt::Hlc`), applies it to its own retained
cache at once, and marks the topic changed for every peer. No other node is asked. The
publisher's ack waits for point 4's local write and, as before, for the publish's durable
deliveries. It never waits for another node's retained copy.

**3. Eventual: the copies.** This is ADR 0085 D15's shape, applied to retained values:

- **Sending.** A task per peer link sends the changed topics' latest values in batches and
  clears their marks. What waits to be sent is bounded by the number of topics, not the
  number of publishes: a topic written 40 times while a batch is in flight is sent once. A
  congested link makes the task wait. Nothing is dropped, and no publisher is paused.
- **Receiving.** The peer link's reader applies each value to the cache directly, if its
  stamp is higher than the one held, with ties broken by node id (ADR 0085's order). It is
  not a hub command, so it waits in none of the hub's lanes and never holds the hub loop.
- **Skew.** A stamp more than 60 seconds ahead of the receiver's wall clock is refused and
  counted, and repair retries it later (ADR 0085's `MAX_FORWARD_SKEW_MS`).
- **Repair.** The digest exchange of ADR 0014 T6/T8 and its 2026-08-06 amendment still runs
  at link-up and every 30 seconds. It is re-based on stamps, so each topic converges to its
  highest stamp.
- **Clears** are stamped tombstones, discharged by §4's convergence-gated rule (#229).

**4. Eventual: what a crash keeps.** A writer task off the hub loop persists the cache in
batches, one redb transaction and one flush per batch. A retained publish's ack waits for
the flush of the batch that holds its value. Copies received from peers go through the
same writer less often, coalesced per topic, because repair can refill them. So:

- an acknowledged retained value survives a restart of the node that took it, and reaches
  every other node with its next batch;
- a value is lost only if the node that took it loses its disk before any batch has left
  it. The topic then keeps its previous value. The window is one batch long.

**5. Eventual: partitions.** Both sides keep taking and serving retained writes. When the
partition heals, repair gives each topic the value with the highest stamp, which is the
latest one while the nodes' clocks agree. An acknowledged value can be replaced that way
without notice, and repair counts it on `mqttd_retained_divergence_total`. This is the
trade the maintainer chose: retained state stays writable on every node, and a partition
can cost a write that a concurrent one replaced.

**6. Strong: the same guarantee, in batches.** The strong mode keeps §1 to §7 and stops
doing one thing at a time:

- **Coalescing.** A mutation for a topic that already has one queued replaces it, and the
  replaced publish's ack waits for the newer value's commit. The guarantee is restated: an
  acknowledged retained value was committed, or was replaced by a later value of its topic
  that was. Nothing can read the replaced value once the newer one has committed.
- **Batching.** A commit carries every queued topic of a placement group in one append.
  The post-commit fan-out carries every topic of a commit in one frame per peer.
- **Pipelining.** Order is kept per topic instead of per node, so a node can have several
  hand-offs in flight. P6 serialized commits to keep two publishes of one topic in order,
  and T8 keeps one hand-off in flight to keep a publisher's order. Retained state needs
  only the first: a topic's value has no order relative to another topic's.
- **Off the hub.** Each node applies the fan-out's batches through point 4's writer, not
  on the hub loop.

**7. Agreement and upgrades.**

- **The wire.** Point 3's frames are new, at the next peer protocol (15), and additive.
  `PROTO_MIN` stays 6 (ADR 0038, ADR 0039).
- **The mode in force** is computed by every node on every sweep, the way ADR 0073 §4
  computes the ownership domain. It is `eventual` only while every placement member has
  negotiated protocol 15 and is configured `eventual`; otherwise it is `strong`. `/statusz`
  shows the mode in force beside the configured one, and a node logs which members hold
  the cluster in `strong`.
- **The mode in force decides which version wins.** Under `eventual` a stamp beats a
  commit token, and under `strong` a commit token beats a stamp. Copies can disagree for up
  to one repair period after a switch.
- **A rolling upgrade from 1.3** stays strong until the last node runs the new release,
  and then turns eventual, the new default. That is a change of behaviour at upgrade, and
  the release notes must say so. An operator who wants strong sets it before the roll.
- **Strong to eventual** needs no migration. Every cache holds committed values, and they
  stay until the first stamped write of their topic.
- **Eventual to strong** starts with a repair round with every member. Then each owner
  commits the highest-stamped value it knows for every topic of its groups. Until a
  topic's commit lands, its copies keep their stamped value.

**8. What proves it.**

- **The harness** (ADR 0042) gains an oracle for the eventual mode. No node ever moves a
  topic to a lower stamp. Once writes stop and partitions heal, every node holds the same
  value within one repair period. An acknowledged retained value survives `kill -9` of the
  node that took it, unless a later value replaced it. The strong mode keeps the
  acked-facts oracle, which learns point 6's "replaced".
- **A rolling upgrade test** from 1.3.0 (`BASELINE_REF`) through the change of default.
- **The rig** measures both modes as a ladder of retained publishes a second (mqtt-load
  gains `--retain`), with the prediction below beside each result (ADR 0091 D7).

### The formulas

ADR 0091 D2's symbols, plus P, the cluster's retained publishes a second; T, the retained
topics; and b, the sending task's batch interval.

| Quantity | `eventual` | `strong` (point 6) |
|---|---|---|
| a retained publish's ack waits for | the local batch's flush | its topic's batch commit at the owner, with both copies |
| values each node applies a second | at most min(P, T ÷ b), off the hub loop | at most min(P, T ÷ the commit interval), off the hub loop |
| frames each node sends a second | about (N − 1) ÷ b | about (N − 1) × its own commits a second |
| how old another node's copy can be | about b + r | about one commit + r |

A fleet of 100 plants at full size has P = 1,000,000 and T = 25,000. With b = 100 ms, that
is at most 250,000 cache updates a second on each node, each a map operation off the hub
loop.

### Consequences

- **The default changes.** Retained values can differ between nodes for a moment, and
  across a partition a concurrent write can replace an acknowledged one. The release notes
  and GUIDE's limitations say so.
- **Clocks.** This record's "no wall-clock anywhere" now holds for the strong mode only.
  The eventual mode orders writes by the hybrid clock, as rule state already does (ADR 0085
  D6), so the nodes' clock agreement matters: run NTP.
- **Cost.** The eventual mode removes the owner round trip, the one-at-a-time chain and
  the flush per update on every node. Each node still applies every changed topic, in
  batches, off the hub loop.
- **The strong guarantee is restated** by point 6's coalescing.
- **Docs that change with the delivery:** CONFIGURATION, SIZING (the queue's description
  and these formulas), COMPARISON's partition row, THREAT-MODEL's clock note, GUIDE's
  limitations, and the release notes.

### Alternatives considered

- **Keep strong only, and make it fast (point 6 alone).** The ack would still wait for the
  owner and both copies, and the minority side of a partition could not change a retained
  value at all. That is the availability the maintainer wants by default.
- **A Lamport counter instead of the hybrid clock.** It is clock-free and orders the
  writes a node has seen. But a client that moves to another node can publish there before
  its previous value has arrived, and if that node's counter is lower, the stale value
  wins. The hybrid clock orders those writes by time, as long as the clocks agree to within
  the time a reconnect takes.
- **Vector clocks with sibling values.** A last-value cache has nothing to merge siblings
  with, as ADR 0085 found for its `lww` type.
- **A choice per publish,** like ADR 0072's tiers. One topic's values would then carry both
  kinds of version. A mode per cluster keeps one order per topic; a choice per topic prefix
  could come later.
- **Ack before the local flush.** It is faster again, and it is what EMQX's default and
  Mosquitto do. But a crash would then lose acknowledged values that never left the node,
  and one batch flush is cheap enough to keep that guarantee.
- **Fetch on subscribe.** Rejected again, for ADR 0014's reasons.

### Delivery

Tasks 0037-T15 to 0037-T23 in the
[delivery doc](../delivery/0037-durable-retained-messages.md), issue
[#1214](https://github.com/mbilling/fss-mqtt-broker/issues/1214).
