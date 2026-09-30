# 0080. The replication factor is cluster state: configurable, default 2, changeable live

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0080-replication-factor.md](../delivery/0080-replication-factor.md) — plan, progress, and changelog
- **Related:** [ADR 0006](0006-consensus-and-replication.md) (quorum replication, R=3),
  [ADR 0043](0043-elastic-cluster-resize.md) (catch-up and caught-up stamps),
  [ADR 0073](0073-scale-out-durable-ownership.md) (capability-gated cluster changes),
  [ADR 0078](0078-replica-segment-log.md) (the 2026-09-29 calibration)

## Context

Every durable placement group keeps its log on **R** replicas, and an append is
acknowledged once a majority of them hold it. R is a compile-time constant,
`placement::DEFAULT_REPLICAS = 3`, set once in `main.rs` and never changed. There is no
setting; `durable.min_replicas` is the write *floor*, a different thing.

**Why R matters for capacity.** On a cluster of **N** nodes, each node stores a share
**R / N** of every durable message (its appends as owner or follower, and the truncates
that follow). The 2026-09-29 calibration (3 × CCX23, log store, #662) had N = 3 and R = 3,
so R / N = 3 / 3 = 1: every node stored every message, and each node's writer carried the
whole cluster's durable rate, about 98k operations/s at the ceiling of about 90k msg/s
delivered. At R = 2 the share is 2 / 3, so the same nodes should carry about
3 / 2 = 1.5× the durable rate before the same per-node limit.

**Why 2 as the default.** It matches what an operator compares us with: HiveMQ keeps 2
copies by default. It survives the loss of any one node without losing an acknowledged
message, which is the durability promise most deployments need. And it is the capacity
figure a like-for-like comparison has to use (ADR 0048 comparison rules).

**What R = 2 costs.** The write quorum is `⌊L / 2⌋ + 1` acks, where **L** is the number of
nodes in the group's replica set. For L = 3 that is ⌊3 / 2⌋ + 1 = 2, so one slow or dead
replica is tolerated. For L = 2 it is ⌊2 / 2⌋ + 1 = 2, so **both** copies must ack:
- a replica that is slow, or SWIM-*suspected*, stalls durable writes for its groups until it
  recovers or is declared dead (`MemberState::Suspect` stays in the replica set,
  placement.rs:460);
- when it is declared dead, the group's set is re-formed and writes resume after the
  survivor's catch-up sweep stamps the new set.

Recovery itself is sound at L = 2: every acknowledged entry is on both copies, so the
survivor holds all of them, and `recover_key` needs one *complete* read anchoring a read
quorum, which the survivor plus the newcomer provide once the sweep has stamped
(cluster_store.rs:402–477). The cost is the write pause, not data.

**Why R cannot just be a per-node setting.** R has to be the same on every node:
1. **Quorum intersection.** An R = 3 owner acks at 2 of 3; an entry can end up on
   {owner, X} only. A later R = 2 owner whose set excludes X reads 2 of 2, finds a gap-free
   copy, and silently truncates the tail (the intersection argument of
   `merge_replica_logs` assumes both sides use one set).
2. **Replicas that never heal.** A node that believes it is a replica but is outside the
   owner's (smaller) set requests catch-up the owner never serves.
3. **Divergent verdicts.** Each node computes completeness, the write floor and the
   `replication_desired` gauge against its own R.

A rolling restart that changes a per-node constant creates exactly that mixture.

## Decision

### 1. R is cluster state, committed through the lease Raft

- The lease state machine gains a replication record: `replicas: Option<u8>` (the
  committed R) and `change: Option<(from, to)>` (a phase-2 change in progress). Three
  commands write it:
  - `LeaseRequest::SetReplicas { r }`: the founding value (phase 1). Accepted only while
    `replicas` is unset.
  - `LeaseRequest::BeginReplicaChange { from, to }`: opens the joint phase (§4). Accepted
    only when `replicas == Some(from)` (or unset and `from == 3`) and no change is open.
  - `LeaseRequest::CommitReplicaChange { to }`: closes it, setting `replicas = Some(to)` and
    clearing `change`. Accepted only when the open change's `to` matches.
- Every node's `Placement` uses the **committed** record: R from `replicas`, and while
  `change` is set, both the old and the new replica set of every group. A node's configured
  value is only a proposal.
- **No committed value means R = 3.** Every cluster that exists today has none, so an
  upgrade changes nothing.
- The command is **capability-gated** like ADR 0073's ownership flag: it is proposed only
  when every member advertises a peer protocol that knows it (`PROTO_MAX` 8 → 9). A member
  on an older build would fail to apply an entry it cannot decode.
- `Placement.replicas` becomes updatable: a committed change bumps the placement's
  version, so memoised replica sets are recomputed.

### 2. The setting

- `durable.replicas` / `MQTTD_REPLICAS`: an integer from 2 to 7. **Default 2.**
- It takes effect when a cluster is **founded**: the founder commits it as its first lease
  entry, before it reports ready, so no durable write is ever made under another R.
- On a node joining an existing cluster, a configured value that differs from the
  committed one is logged at startup ("this cluster runs at R = n") and changes nothing
  until phase 2 (§4) makes it a proposal.
- `durable.min_replicas` (an explicit count) is validated against the committed R, not the
  constant; `min_replicas > R` stays a startup error.
- Even values above 2 are allowed with a warning: R = 4 needs ⌊4 / 2⌋ + 1 = 3 acks and still
  tolerates one failure, the same as R = 3, at a third more storage and replication.

### 3. Phase 1: configurable R, default 2 for new clusters

Delivered and validated before any default changes:
- the setting, the lease command, the capability gate, the updatable placement (§1, §2);
- the durability, crash, chaos and failover suites (`cluster_stress`, `cluster_proc`,
  `durable_sessions`, `cluster_chaos`, `decommission`) passing at R = 2 on both stores;
- the stated write pause measured: how long durable writes to the affected groups stop
  when one of two replicas is killed, and when it is only suspended (SIGSTOP);
- then the default flips to 2. Existing clusters keep R = 3 until an operator changes it.

### 4. Phase 2: changing R on a running cluster

An operator changes `durable.replicas` and reloads (or calls the admin API). The node
proposes `BeginReplicaChange { from: R, to: R′ }`; the change then runs per group, using the ADR 0043 machinery:

1. **Joint phase.** The node proposes `BeginReplicaChange { from: R, to: R′ }`. Once it
   commits, every node holds `change = Some((R, R′))`, so for each group both the old set S
   and the new set S′ are known. HRW makes the smaller set a
   prefix of the larger (`owner_led_replica_set` truncates one fixed order), so S′ ⊂ S when
   shrinking and S ⊂ S′ when growing. While a group is in the joint phase, an append is
   durable only when it holds a quorum of S **and** a quorum of S′ (the Raft
   joint-consensus rule), so any later recovery over either set intersects it.
2. **Catch-up.** Each member of S′ must hold every acknowledged entry of the group before
   the group leaves the joint phase. Growing, the new members are filled by the existing
   sweep. Shrinking, the kept members are checked gap-free against the dropped ones and
   filled from them if not: an entry acked at 2 of 3 may live only on the copy being
   dropped.
3. **Switch.** When every group's S′ is stamped caught-up, the lease leader commits
   `CommitReplicaChange { to: R′ }`; the joint phase ends and quorums use S′ only. The
   leader, not the proposer, drives it, so a proposer that dies mid-change does not strand
   the cluster in the joint phase.
4. **Collect.** Nodes that left a group's set delete its entries (today nothing reclaims a
   dropped copy).

A reload is a proposal, not an order: it is refused if the cluster has fewer eligible
nodes than R′, or if a change is already in progress; the refusal is logged and exposed on
`/statusz`.

## Consequences

- New clusters store about a third less per node at N = 3 and should carry about 1.5× the
  durable rate; the comparison with HiveMQ uses equal copy counts.
- New clusters trade write availability for that: a slow, suspected or dead node pauses
  durable writes for every group it holds a copy of, where R = 3 pauses only the groups it
  owns. How long the pause lasts is set by failover, not by R (measured below). The
  documentation, the startup log and `/statusz` state which factor a cluster runs.
- R is no longer a constant anywhere it is read (placement, the write floor, the startup
  log, the gauges); tests that assumed 3 name it explicitly.
- The lease state machine gains a replication record and three commands, and the peer protocol a version (9).

## Amendment, 2026-09-30 — the write pause, measured (T3)

Context and Consequences above assumed R = 3 rides through a single failure without a
pause and R = 2 pauses for "a few seconds". Measured on three `mqttd` processes
(`crates/mqttd/tests/replication_pause.rs`): 48 durable sessions, one publisher per
topic sending a `QoS` 1 message every 100 ms, one fault per fresh cluster on a non-founder
node. The figure is, per topic, the longest gap between two successful acks from the fault
on; a topic counts as paused when that gap is at least 1 s.

| fault | R = 3 | R = 2 |
|---|---|---|
| crash (`SIGKILL`) | 30 / 48 paused, longest 14.9 s, median 14.9 s | 38 / 48 paused, longest 15.3 s, median 15.1 s |
| stall (`SIGSTOP` 8 s, then `SIGCONT`) | 39 / 48 paused, longest 23.9 s, median 18.6 s | 39 / 48 paused, longest 22.3 s, median 21.3 s |

So R = 3 also pauses: the groups the lost node OWNED wait out failover (failure detection,
lease reassignment, recovery) whatever the factor, and R = 3 rides through only the loss of
a non-owning copy. R = 2 pauses more groups (38 against 30 on a crash) for about as long.
The trade is breadth, not length. Failover time itself, about 15 s, is a property of the
cluster to shorten independently of this ADR.

Validating R = 2 also found a durability defect that existed at any factor below the
member count: a caught-up stamp outlived the node's membership of the group's replica set,
so a node that re-entered a set could answer a recovery read "complete" for history it
never received. It lost acknowledged messages once in about 30 runs (#727). Fixed with T3:
a node clears its stamp when it leaves a set, and an adopted factor arms the catch-up sweep.

## Amendment, 2026-09-30 — the live change as built (T5)

§4 holds with five refinements, found while building it:

- **The trigger is a reload only.** [ADR 0081](0081-admin-api.md) keeps configuration in the
  file, so its admin API does not set `durable.replicas`. Only the lease leader can write to
  the lease group, so the leader proposes when its own reload asks for the change: reload
  every node (a config-map roll does). A follower's reload is held until that node leads or
  the change lands. Refusals (fewer eligible nodes than R′, a change already running) and
  holds (not the leader, a member not yet speaking proto 9) are logged and shown on
  `/statusz` under `replication_factor`, with the open change's progress.
- **Opening the change re-mints every lease.** `BeginReplicaChange` records `since`, the
  highest epoch minted before it, and assigns every lease to its current holder at a fresh
  epoch in the same entry. An owner reads the record together with its lease epoch from one
  applied state, so every log at an epoch above `since` counts acks by the joint rule, and
  recovery reads the same joint quorum. Each node that applies the change also raises its
  replica fences above `since` for every group, durably, so an owner that has not applied it
  yet cannot commit through that node on the old quorum. Only entries at or below `since` can
  sit on too few members of S′.
- **The catch-up condition is a quorum of S′, not every member.** Before the switch, every
  entry at or below `since` that a joint-quorum recovery would return must be held by a
  majority of S′: that is what a recovery over S′ alone needs to see it. Requiring every
  member of S′ would not converge under load, since joint writes reach a majority, not
  everyone. The lease leader checks it per key with the existing key-discovery and recovery
  reads. A key that falls short goes to its owner, whose recovery and re-commit rewrite it
  at the current epoch under the joint rule. The ADR 0043 stamps are unchanged. Growing, the
  sweep stamps the wider set. Shrinking, the pure-shrink rule re-stamps the kept members,
  which were in the set throughout. The first round waits 5 s after the change opens, so an
  append a pre-change log had already sent resolves before it is checked.
- **Collection is scoped to the change.** After a shrink commits, a node deletes its copies
  of the groups the shrink took it out of, and only once every member of the group's new set
  holds everything the copy holds (the decommission drain's check, ADR 0043 P3). A copy left
  by other membership moves stays: the #390 roster sweep reads former holders. A crash
  between the commit and the collection leaves a copy on disk, which costs space, not
  safety.
- **Opening costs a short pause.** Re-minting every lease makes each owner re-recover a key
  on its first touch, as after a failover that moves nothing. Growing also waits for the
  sweep to stamp the wider set (about a second in the three-node test). Not yet measured
  under load.

## Alternatives considered

- **Keep R = 3 and document the difference.** Honest, but it leaves a third of every
  node's writer and replication capacity spent on a copy HiveMQ does not keep, and every
  capacity comparison would need a caveat.
- **A per-node setting.** Rejected: mixed R can lose acknowledged data (Context, point 1).
- **R in the replica store's format stamp, checked on the peer handshake.** Safe for
  phase 1, but phase 2 needs one agreed value and a joint phase anyway, which only the
  lease Raft can provide; storing it twice would need reconciling.
- **Asynchronous second copy (ack on one).** Faster, but it acknowledges a message a
  single disk failure can lose. That weakens the durability promise rather than matching
  HiveMQ's.
