# Cluster Durability — Implementation Plan

Concrete, sequenced plan for the unbuilt portions of
[ADR 0001 — Session durability](adr/0001-session-durability.md). ADR 0001 fixes
the *design* (HRW ownership, a quorum-replicated per-session enqueue log, lazy
local truncation, takeover); this document breaks the remaining work into
ordered, independently-shippable workstreams with their dependencies, the
decisions still open, and the test strategy.

## Where we are

Implemented (ADR 0001 roadmap step 1):

- `mqtt-storage::SessionStore` — the incremental async seam
  (`ensure_session` / `enqueue` / `pending` / `ack` / subscriptions), and
  `MemorySessionStore` against it.
- The broker wires persistent-session handling through that seam: offline
  queueing, replay on reconnect, QoS 1/2 in-flight resume.
- `mqtt-cluster::hrw` — rendezvous placement, with tests, **but nothing calls
  it**.
- SWIM membership exposes the live, authenticated member set.

Not yet built (ADR 0001 roadmap steps 2–3): ownership over membership, the
replicated log backend, the takeover protocol, queue caps, MQTT 5 expiry, and
shared subscriptions.

## Guiding constraints

- **The `SessionStore` trait does not change.** Every workstream below either
  hardens the memory backend or adds a new backend behind the same trait. The
  broker core stays backend-agnostic (the point of ADR 0001 §7).
- **Ship value before consensus.** Sharding sessions by owner delivers linear
  session *capacity* without any replication; durability (surviving an owner
  crash) is the expensive part and is layered on top.
- **Two hard prerequisites gate parts of this plan:** a consensus/replication
  mechanism (a new ADR) and the MQTT 5.0 codec (a separate milestone). The
  ordering below front-loads everything that needs neither.

## Workstreams

| # | Workstream | Status |
|---|------------|--------|
| A | Bounded queues & overload policy | ✅ Done |
| B | Ownership ring over live membership | ✅ Done |
| C | Session affinity & redirect ([ADR 0005](adr/0005-session-affinity.md)) | ✅ Done — **ephemeral mode** |
| D | Consensus / replication decision ([ADR 0006](adr/0006-consensus-and-replication.md)) | ✅ Done |
| E | Replicated session-log backend | 🔶 In progress — components done (1–2, 3a, 3b, 3c) + **4a, 4b, 4c**; **4d–4f** ([ADR 0007](adr/0007-durable-store-integration.md)) remain |
| F | Takeover / handoff protocol | ⬜ Not started (needs E) |
| G | MQTT 5 expiry & shared subscriptions | ⬜ Blocked on the v5 codec |

### A — Bounded queues & overload policy  ✅ *(done)*

ADR 0001 §6, roadmap step 3. The memory queue was unbounded — a
dead-but-persistent client was an OOM/DoS vector.

- `OverflowPolicy` (`DropOldest` default / `RejectNewest`) + per-session
  `QueueLimits` in `mqtt-storage`; `SessionStore::enqueue` now returns an
  `Enqueued { Stored { offset, evicted } | Rejected }` so callers observe the
  cap. Bounded by default (100k messages); `MemorySessionStore::with_limits`
  for explicit control. Config via `MQTTD_MAX_QUEUED_MESSAGES` /
  `MQTTD_QUEUE_OVERFLOW`.
- The hub logs evictions/rejections (a metrics counter is the eventual operator
  signal); offsets stay monotonic across eviction so `pending`/`ack` are
  unaffected.
- **Tested:** each policy; cap enforcement; eviction preserves offset
  monotonicity and ack/replay correctness; hub-level offline overflow replays
  only the newest cap-many messages.

### B — Ownership ring over live membership  *(needs A's semantics settled)*

ADR 0001 §1. Connect the dormant `hrw` module to SWIM.

- A `Placement` service: given the current **alive** member set (subscribe to
  `MembershipEvent`s), compute `owner(client_id)` and the ordered replica set
  (R configurable, default 3). Recomputed as membership changes.
- Pure, deterministic, sans-I/O — the same testability discipline as the SWIM
  state machine.
- **Tests:** owner stability under unrelated membership churn; minimal
  reassignment when a node joins/leaves (rendezvous property); replica set
  shrinks gracefully below R members.
- **Delivers:** the foundation for C and F; no behavior change alone.

### C — Session affinity & redirect  ✅ *(done — ADR 0005)*

ADR 0001 §5, alternative "no replication". Make each session live on exactly one
node *without* replication yet.

- On CONNECT, the landing node consults `Placement`. If it is not the owner and
  the owner is alive, redirect/proxy the session to the owner (MQTT has no
  native redirect in 3.1.1 — start with **proxy** the link to the owner;
  evaluate MQTT 5 Server-Redirect later).
- Takeover-on-reconnect works for the owner-alive case (the existing
  same-client-id disconnect already does the local half).
- Owner death loses that owner's queues — this is the explicit, documented
  **ephemeral-sessions** mode. Loud about its durability guarantee (none across
  owner loss).
- **Tests:** a session opened on a non-owner is served by the owner; publishes
  from any node reach it; owner-alive reconnect resumes the session; killing the
  owner drops the session (asserting the documented limitation).
- **Delivers:** linear session *capacity* (sharded, no node holds all sessions)
  — a real scalability milestone — before any consensus work.

### D — Decision: consensus / replication mechanism  ✅ *(done — ADR 0006)*

Capability Plan §8 open question; ADR 0001 §4 reserves consensus for "ownership
+ the enqueue log only." This was a hard, expensive-to-reverse choice and got
its own ADR. The decision ([ADR 0006](adr/0006-consensus-and-replication.md)):

- **Consensus is scoped to ownership leases, not per log entry.** A small
  consensus group establishes, per placement group, which node holds the lease
  and at what epoch; the lease-holder then does **epoch-fenced quorum
  replication** of the per-session append-log — one quorum round-trip per append,
  not a leader election per entry. (Per-shard, never one global group — ADR 0001
  rejects the global-leader ceiling.)
- **Use a proven engine; do not hand-roll** leader election / fencing /
  membership change. `openraft` is the leading candidate; the final library is
  gated on a `cargo-deny` review + a focused spike (the first task of E, which
  may ratify or amend the ADR).
- An external store stays an operator-selectable `ReplicatedLog` backend (ADR
  0001's externalized-store alternative), not the default.

Decision criteria weighed: split-brain safety on ownership, write-amplification
cost, operational complexity, dependency/supply-chain weight (must pass
`cargo-deny`), and cheap per-shard group lifecycle as membership moves.
**Output:** ADR 0006 + the `ReplicatedLog` seam — a generic async append-log
trait (`append` / `read` / `truncate` / `remove` over keyed, offset-addressed
byte records) in `mqtt-storage::repl`, with `InMemoryReplicatedLog` shipping now
for development/tests/non-clustered use; the consensus-backed and external-store
backends target E.

### E — Replicated session-log backend  *(needs B + D; in progress)*

ADR 0001 §2–§3, sequenced as the four steps in [ADR 0006](adr/0006-consensus-and-replication.md)'s
phasing. The durable backend implementing `SessionStore`.

- **Step 2 — `SessionStore` over `ReplicatedLog`** ✅ *(done, landed first)*:
  `ReplicatedSessionStore` (`mqtt-storage::logged`) implements the full
  `SessionStore` purely over the `ReplicatedLog` seam, with no store-local durable
  state — the queue lives in a `q/{client}` log, session metadata in `m/{client}`.
  A test pins the layering (a second store over the same log sees the first's
  sessions whole), so swapping `InMemoryReplicatedLog` for the consensus-backed log
  makes sessions durable with **no change to this layer**. Done ahead of the engine
  spike because it needs neither network nor an engine choice and de-risks the seam.
- **Step 1 — engine spike** ✅ *(done)*: `cargo-deny` review of openraft (79
  crates, **gate-clean**) vs raft-rs (15 crates but **fails** on an active
  protobuf DoS advisory) ratified **openraft** as the engine; the engine-agnostic
  ownership-lease / epoch-fencing state machine landed as `mqtt-cluster::lease`
  (`LeaseGroup`, split-brain-safety pinned by tests). openraft does not enter the
  build until step 3 wires it. Details in [ADR 0006](adr/0006-consensus-and-replication.md)
  *Spike outcome*.
- **Step 3 — the consensus-backed `ReplicatedLog`**, decomposed:
  - **3a — quorum-append core** ✅ *(done)*: `mqtt-cluster::cluster_log` —
    `ClusterLog` quorum-replicates each append across the replica set behind a
    `ReplicaTransport` seam, epoch-fenced via `ReplicaState`. Enqueue is
    **quorum-durable before commit** (gating the QoS≥1 PUBACK); ack/truncate are
    local-first and lazy. A deterministic loss-injecting sim pins the contract
    (single-replica-loss survival at R=3/q=2, below-quorum rejection with no
    committed hole, stale-leader fencing), and the step-2 `ReplicatedSessionStore`
    runs unchanged on top — durable sessions, end to end, sans network.
  - **3b-i — networked transport** ✅ *(done)*: `mqtt-cluster::repl_net` —
    `PeerReplicaTransport` realizes the `ReplicaTransport` seam over the peer mesh
    via new `PeerMessage::Replicate` / `ReplicateAck` frames, with `req_id` ack
    correlation and `fail_node` on link drop. Tested over real framed streams
    (round-trip apply, stale-epoch fencing, unreachable replica, in-flight failure
    on disconnect). The three handles (outbound `tx`, ack routing, disconnect) map
    onto the existing hub mesh; driven directly until step 4 wires them in.
  - **3b-ii — openraft lease manager**: *state machine + type binding* ✅ *(done)* —
    openraft is now in the build (and through `cargo-deny`); `mqtt-cluster::lease_raft`
    holds the replicated `LeaseMap` (`group -> (holder, epoch)`, monotonic epoch =
    the fence source) bound to openraft via `declare_raft_types!(LeaseConfig)` over
    numeric `RaftNodeId`s, compile-asserted as a valid `RaftTypeConfig`.
    *Storage* ✅ *(done)*: `lease_store::LeaseStore` implements openraft's
    `RaftStorage` over `LeaseMap` (log/vote/applied-state/snapshots, in memory) and
    **passes openraft's own conformance `Suite`** — every storage method checked
    against the protocol's correctness requirements. *Network + bring-up* ✅
    *(done)*: `lease_group` implements openraft's `RaftNetwork` and brings up a real
    group — a three-node group **elects a leader and replicates a committed lease to
    every replica**, through real consensus, into the `LeaseMap` (validated with an
    in-memory router). *Mesh network* ✅ *(done)*: `raft_mesh` carries the same RPCs
    over the peer bus (`PeerMessage::RaftRpc`/`RaftRpcReply`); a test elects a leader
    and replicates a committed lease across **two nodes over a serialized duplex
    link**. *Next:* 3c and step 4.
  - **3c — replicated exactly-once state** ✅ *(done)*: `SessionStore` gained
    `record_received` / `clear_received` / `received` / `next_packet_id`;
    `ReplicatedSessionStore` keeps the **QoS-2 dedup window + outbound packet-id
    counter** in the replicated `m/{client}` snapshot, so a failover replica sees
    them (a test pins it: a second store over the same log treats an already-received
    id as a duplicate and continues the counter without collision). *Remaining
    (minor):* a rebuildable per-key cap index to retire the O(n) enqueue count.
  - **Step 4 — wire the stack into the live broker** *(designed in
    [ADR 0007](adr/0007-durable-store-integration.md))*. The components are all
    built; step 4 assembles them into `mqttd`. Sub-steps, each shippable/test-first:
    - **4a — `NodeId ↔ RaftNodeId` mapping** ✅ *(done)*: `node_registry`.
    - **4b — placement groups** ✅ *(done)*: `placement::group_of(client) =
      stable_hash % NUM_GROUPS` (256); `Placement` gained `group_owner` /
      `group_replica_set` / `owns_group`, and the per-client queries now resolve
      through the client's group — so a session is owned by and relocated to its
      *group* owner (refining ADR 0005). The rendezvous properties (minimal
      reassignment on join/leave) hold at group granularity.
    - **4c — durable-plane endpoint** ✅ *(done)*: `mqtt-cluster::durable_plane::DurablePlane`
      bundles the node's lease `Raft` + `MeshRaftNetwork` + `PeerReplicaTransport` +
      `ReplicaState` and exposes `register` (on peer connect) / `fail` (on disconnect)
      / `handle(frame) -> Option<reply>` for the four consensus/replication frames. A
      two-node test over a duplex link runs **both** planes end to end through the
      plane: the lease group elects + commits a lease, and a session-log append
      quorum-replicates. *(Refines ADR 0007 §4: the plane is a shared handle the peer
      links route to, keeping consensus/replication off the hub actor's serial loop;
      the live peer-pump I/O wiring lands in 4f with the store swap.)*
    - **4d — membership reconciler**: SWIM membership → openraft voters (debounced,
      deterministic bootstrap).
    - **4e — durable cluster `SessionStore`**: lease → epoch → per-group `ClusterLog`
      → `ReplicatedSessionStore`; lazy lease acquisition. Multi-node test: an enqueue
      survives the owner's death.
    - **4f — wire into `mqttd`**: `Arc<dyn SessionStore>`; `MQTTD_DURABLE_SESSIONS`
      builds the durable store; connections use it for QoS-2 dedup / packet ids;
      single-node path unchanged.
- **Delivers:** durable sessions — ADR 0001's headline guarantee.

### F — Takeover / handoff protocol  *(needs B + E)*

ADR 0001 §5. Owner-dead path.

- On owner death (a SWIM `Dead` event), a replica is promoted per the ring and
  replays its log from the last durably-truncated offset to rebuild the queue;
  the next reconnect lands on the new owner.
- Reconciles with the existing local takeover (same-client-id disconnect).
- **Tests:** kill the owner mid-session → a replica serves the reconnect with no
  loss of durably-enqueued messages; concurrent reconnect during promotion does
  not double-own (split-brain check); redelivery of in-flight QoS-1 is bounded
  and spec-legal.

### G — MQTT 5 expiry & shared subscriptions  *(gated on the MQTT 5.0 codec)*

ADR 0001 §6, roadmap step 3 — **blocked on the v5 codec milestone**, listed here
for completeness and sequencing.

- **Session Expiry Interval** → GC sessions; **Message Expiry Interval** → drop
  stale queued messages. Both are v5 properties; the storage hooks (a TTL on
  sessions and per-message expiry) can be *designed* into A/E now and *activated*
  when the codec lands.
- **Shared subscriptions** (`$share/<group>/<filter>`) — the in-protocol lever
  for *consumer* linear scale (Capability Plan §4). Routing-layer work (group
  membership, load-balanced delivery across group members) on top of the v5
  codec; interacts with placement but is largely independent of the durability
  backend.

## Sequencing

```
A (queue caps) ─▶ B (ownership ring) ─▶ C (affinity / ephemeral mode)
                          │
                          ├─▶ D (ADR 0006: consensus choice) ─▶ E (replicated log) ─▶ F (takeover)
                          │
                  (MQTT 5.0 codec milestone) ─────────────────▶ G (expiry, shared subs)
```

- **A → B → C** is the near-term path: each ships independently, none needs
  consensus or MQTT 5, and C is a genuine scalability milestone (sharded session
  capacity) on its own.
- **D → E → F** is the durability core; gated on the consensus ADR. Treat D as a
  spike + decision before committing to E.
- **G** waits on the MQTT 5.0 codec and can proceed in parallel with D–F once
  that lands.

## Adjacent, out of scope here

- **Subscription interest digests (bloom)** for sub-linear publish fan-out
  (Capability Plan §4) — a *routing-efficiency* concern, not durability. Today's
  interest gossip is a full snapshot per change; the digest is separate work.
- **Retained-state replication** across nodes — related cluster state, but a
  distinct mechanism from the session log.

## Carried limitations & where they resolve

Each known gap in what has shipped, with the workstream that closes it. None is a
silent cut — they are the explicit price of shipping capacity before durability.

| Limitation | Impact | Resolved by |
|------------|--------|-------------|
| Sessions are **ephemeral** — an owner's death drops its queues | No durability across owner loss (sharded capacity only) | **E step 3b** (networked durable log) + **F** (takeover) |
| ~~Consensus engine (openraft) unratified~~ | — | ✅ **E step 1** — openraft ratified |
| The pieces exist and are wire-tested in isolation, but are **not yet connected or run by the live hub**: the lease group (`raft_mesh`/`lease_group`) mints epochs, the session-log transport (`repl_net`) replicates, but nothing yet feeds the lease epoch into a `ClusterLog` or runs either over the broker's real peer links | Consensus + replication proven over framed streams in tests, but not exercised by the running broker | **E step 4** (hub wiring: lease group → epoch → `ClusterLog` → mqttd's `SessionStore`) |
| ~~QoS-2 dedup set + next-packet-id counter not yet replicated state~~ | — | ✅ **E step 3c** — now in the replicated `m/{client}` snapshot |
| `ReplicatedSessionStore` enqueue counts the queue via an **O(n) read** for cap enforcement (exact only under serialized per-key appends, which the lease guarantees) | A larger constant on enqueue; no correctness/durability impact | **E step 3c remainder** (a rebuildable per-key cap index) |
| `ReplicatedSessionStore` / `ClusterLog` are **not wired into `mqttd`** (the broker still uses `MemorySessionStore`) | Layering proven in isolation, not in the live server | **E step 4** (swap the backend in) |
| Session-proxy splice is **best-effort on half-close**; no delivery/lifecycle hardening | Edge-case message loss at relay teardown | **C hardening** (ADR 0005 step 2 follow-up) |
| Audit **`via=<node>` vouching detail** not recorded | Vouched relocations not yet attributable in the audit log | **C hardening** (ADR 0005 §3 mitigation) |

**Cross-cutting: remote + CI now live** ✅. The repo has a remote and the
`.github/` gate (fmt, clippy, `cargo-deny`, `cargo-audit`) runs on every push to
`main` and on pull requests. The consensus dependency that lands in E will be
supply-chain-checked in CI, not only locally.

## Test discipline

Every workstream is test-first, mirroring the rest of the repo: pure logic
(placement, queue policy, log semantics) gets exhaustive unit tests; multi-node
behavior gets deterministic simulation harnesses with injectable loss/reorder
(as the SWIM cluster tests already do) rather than timing-dependent integration
alone. The durability claims (no loss of quorum-durable messages; split-brain-
safe ownership; bounded, spec-legal redelivery) are each pinned by a test that
fails if the guarantee regresses.
