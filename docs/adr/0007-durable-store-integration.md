# ADR 0007 — Wiring the durable cluster session store into the broker

- **Status:** Accepted (design); implementation phased (workstream E step 4)
- **Date:** 2026-06-15
- **Deciders:** project maintainers
- **Related:** [ADR 0001](0001-session-durability.md), [ADR 0005](0005-session-affinity.md),
  [ADR 0006](0006-consensus-and-replication.md),
  [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md) workstream E step 4

## Context

Workstream E built every *component* of [ADR 0006](0006-consensus-and-replication.md)
and validated each in isolation: the epoch-fenced quorum-replicated session log
(`cluster_log`), its networked transport (`repl_net`), the openraft lease state
machine + conformance-tested store (`lease_raft`/`lease_store`), a live lease group
over the peer wire (`lease_group`/`raft_mesh`), the replicated exactly-once state
(`logged`), and the `NodeId ↔ RaftNodeId` mapping (`node_registry`).

None of it is connected to the running broker. `mqttd` still serves persistent
sessions from an in-memory `MemorySessionStore`; ADR 0005's relocation is still
**ephemeral** (an owner's death drops its sessions). Step 4 assembles the
components into the live broker so that guarantee finally upgrades to durable.

Unlike the components, the *integration* has open questions ADR 0006 left as "wire
it in" — they are decided here:

1. **Granularity.** Leases and replica sets per *what*? Per session is too many
   consensus entries; per node loses placement locality.
2. **Membership.** SWIM is weakly consistent and churns; openraft needs an explicit
   voter set. How does one drive the other without a reconfiguration storm?
3. **Lease acquisition.** When does a node take a lease and build a `ClusterLog`?
4. **Hub wiring.** Two request/reply RPC systems (consensus + replication) must ride
   the existing single-link-per-pair peer mesh and its hub actor.
5. **Store swap & the QoS-2 hot path.** How is the durable backend selected, and how
   does the connection's exactly-once dedup become durable without a per-packet hub
   round-trip?

## Decision

### 1. Placement *groups* (shards), not per-session ownership

Ownership granularity becomes the **placement group**: a fixed number of shards
`NUM_GROUPS` (default 256). `group(client) = stable_hash(client_id) % NUM_GROUPS`
(the version-stable hash from `node_registry`/`hrw`, identical on every node). A
group's replica set is `HRW("group/<id>")` over the live members (R=3); its **owner**
is `replica_set[0]`.

This bounds the system to `NUM_GROUPS` leases and `NUM_GROUPS` stable replica sets,
regardless of session count, and gives group locality (all sessions in a group share
an owner, a lease, and a replica set). **It refines [ADR 0005](0005-session-affinity.md):**
a persistent session is now relocated to its *group* owner (not a per-client owner),
which is also the node that holds the group's lease and writes its session logs —
unifying "who serves the session" with "who may write it."

### 2. One cluster-wide lease consensus group; membership reconciled from SWIM

A **single** openraft group (the `lease_group`) manages *all* group leases — its
`LeaseMap` is keyed by `GroupId`. This is consistent with ADR 0006: consensus is
only for lease assignment (rare, tiny), never on the publish/enqueue path, so the
single group's leader is **not** a throughput bottleneck. Per-shard *raft* groups
(one heartbeating cluster per shard) were rejected by ADR 0006 as untenable; the
single low-traffic lease group is the bounded-consensus slice it prescribes.

Membership is driven by a dedicated **reconciler** task:

- It observes SWIM `MembershipEvent`s (the same stream that feeds `Placement`) and
  maintains the desired voter set = the stable `Alive` members (mapped to
  `RaftNodeId` via `node_registry`).
- **Bootstrap** is deterministic: the node with the lexicographically smallest
  `NodeId` among the initial members calls `Raft::initialize`. Others join as
  *learners* and are promoted via `change_membership`.
- Changes are **debounced** (apply after membership is stable for a short window) to
  avoid a reconfiguration storm under churn, and only the current leader issues
  `change_membership`.
- **Conservative under churn (accepted limitation):** v1 handles join/leave of stable
  members; rapid flapping or a lost-quorum lease group degrades to ADR 0005's
  ephemeral mode for affected groups rather than risking split-brain. Full dynamic
  reconfiguration hardening is future work, called out in the plan.

### 3. Lease acquisition is lazy, on first owned-group activity

When the broker first needs to durably write a session whose group it owns, the
owner `client_write(Assign { group, node: self })`s to the lease group, receives the
group's epoch, and constructs (or refreshes) a `ClusterLog` for that group at that
epoch over the group's replica set. A rebalance (the group's owner changed) triggers
re-acquisition by the new owner; the superseded owner is **fenced** by the new epoch
(the mechanism `cluster_log`/`lease` already enforce). A node that is not the group
owner, or cannot reach the lease group, serves the session in ephemeral mode (ADR
0005 §5 "degrade, don't refuse").

### 4. The hub hosts the consensus + replication endpoints

The hub actor gains: the local lease `Raft` handle, a `MeshRaftNetwork`, a
`PeerReplicaTransport`, a `ReplicaState` (this node's follower copy of logs it
replicates for other owners), and the `NodeRegistry`. The existing peer-link
lifecycle carries both RPC systems:

- **`PeerConnected { node, tx }`** → `register` the peer's `tx` with both the
  `MeshRaftNetwork` and the `PeerReplicaTransport` (keyed by `raft_id(node)` /
  `node`).
- **`forward_inbound`** routes the four new `PeerMessage` variants:
  `RaftRpc` → `raft_mesh::dispatch` → reply `RaftRpcReply`; `RaftRpcReply` →
  `complete_reply`; `Replicate` → apply to `ReplicaState` → reply `ReplicateAck`;
  `ReplicateAck` → `complete_ack`.
- **`PeerDisconnected` / `PeerDead`** → `fail_node` on both, so in-flight RPCs/appends
  resolve instead of hanging.

These handlers exist already (`raft_mesh`, `repl_net`); step 4 connects their three
handles to these three hub events.

### 5. `Arc<dyn SessionStore>`, shared with connections; opt-in durable backend

The hub's `Box<dyn SessionStore>` becomes `Arc<dyn SessionStore>`, shared with each
connection. The QoS-2 dedup window and outbound packet-id counter must be durable
*before* the broker releases PUBREC/sends a packet, so the connection calls
`record_received` / `clear_received` / `next_packet_id` on the shared store
directly — no per-packet hub round-trip, and the store is already async and
internally synchronized. `conn.rs`'s local `HashSet` dedup is removed.

The durable backend is **opt-in and loudly logged** like every other cluster
feature: `MQTTD_DURABLE_SESSIONS=1` (requires the consensus/peer mesh to be
configured) builds the durable cluster store — a `ReplicatedSessionStore` over a
per-group `ClusterLog` whose epoch and replica set come from the group's lease.
Unset, or single-node, keeps `MemorySessionStore` (the existing default), so the
single-node and non-durable paths are unchanged.

## Consequences

- **The headline guarantee lands:** persistent sessions survive an owner's death
  (within a group's replica set), retiring ADR 0005's ephemeral-mode caveat for
  durable deployments.
- The single lease group is a cluster-wide coordinator for *lease assignment only*;
  if it loses quorum, no new leases are granted (existing leases keep working until
  a rebalance), and affected groups degrade to ephemeral. This is an accepted
  availability trade for split-brain safety.
- `NUM_GROUPS` is a fixed sharding constant; changing it reshuffles group ownership,
  so it is a cluster-wide constant, not per-node tunable (v1).
- Two RPC systems now share the peer link; the hub actor's `forward_inbound` grows
  but stays a simple dispatch.
- Connections gain a hot-path dependency on the store for QoS-2; a storage error
  there fails the publish (the correct conservative behaviour for exactly-once).

## Alternatives considered

- **Per-session leases / per-session raft groups** — too many consensus entries /
  heartbeating groups (ADR 0006 rejected the latter). Placement groups bound both.
- **Per-shard raft groups** (one raft cluster per group) — N groups × heartbeats is
  the cost ADR 0006 rejected; a single low-traffic lease group avoids it.
- **All nodes as permanent voters with no reconciliation** — a fixed voter set can't
  follow membership; rejected. Static voter *seeds* with dynamic learners is the
  middle ground chosen.
- **Routing QoS-2 dedup through the hub** (a `HubCommand` per PUBLISH) — adds a hot-
  path round-trip and serializes on the actor; sharing the store with the connection
  is simpler and faster.
- **Auto-enabling the durable store whenever clustering is on** — violates
  secure/predictable defaults; an explicit opt-in (loudly logged) matches the rest
  of the broker.

## Implementation phasing (workstream E step 4)

Each sub-step is independently shippable and test-first; the live store swap is last
so nothing destabilizes the running broker until the stack underneath is proven.

- **4a — `NodeId ↔ RaftNodeId` mapping** ✅ *(done)*: `node_registry`.
- **4b — placement groups**: `group(client)`, per-group replica set / owner over
  `Placement`; refine relocation (ADR 0005) to the group owner. Pure, unit-tested.
- **4c — durable-plane endpoint** ✅ *(done)*: `mqtt-cluster::durable_plane::DurablePlane`
  bundles the lease `Raft` + `MeshRaftNetwork` + `PeerReplicaTransport` +
  `ReplicaState` behind `register` / `fail` / `handle(frame) -> Option<reply>`.
  *Refinement:* rather than routing these frames through the hub actor's serial
  command loop (where `handle`'s `await` would block all hub commands), the plane is
  a **shared handle the peer-link tasks call directly** — the consensus/replication
  plane stays off the actor. The live peer-pump I/O wiring (and registration on
  connect / `fail` on disconnect) lands in 4f. Proven by a two-node duplex test that
  elects + commits a lease and quorum-replicates a session-log append, all through
  the plane.
- **4d — membership reconciler**: SWIM membership → openraft voters (debounced,
  deterministic bootstrap). Tested with an in-memory membership feed.
- **4e — the durable cluster `SessionStore`**: assemble lease → epoch → per-group
  `ClusterLog` → `ReplicatedSessionStore`; lazy lease acquisition. A multi-node test
  shows an enqueue surviving an owner's death (the durability claim).
- **4f — wire into `mqttd`**: `Arc<dyn SessionStore>`; `MQTTD_DURABLE_SESSIONS` builds
  the durable store; connections use the store for QoS-2 dedup / packet ids;
  single-node path unchanged.

Then **workstream F** (takeover/handoff) follows: on an owner's `Dead` event a
replica is promoted (it already holds the quorum-replicated log) and serves the next
reconnect.
