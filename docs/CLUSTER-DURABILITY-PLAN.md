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
| E | Replicated session-log backend | ✅ **Done** — durable, consensus-backed store, proven over a real 3-node cluster, shippable behind `MQTTD_DURABLE_SESSIONS` |
| F | Takeover / handoff protocol | ✅ Done — **F-a–F-d** (recovery mechanism + recovery-read RPC + rebuild on takeover + owner-death integration test) |
| G | MQTT 5 expiry & shared subscriptions | 🔶 In progress — v5 codec done (ADR 0008); **session expiry** done (ADR 0009 phase 1); **message expiry** done (ADR 0009 phase 2); **shared subscriptions** done (ADR 0010); **topic aliases** done (ADR 0011); flow control, durable expiry deadline (0009 phase 3) remain |

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
    - **4d — membership reconciler** ✅ *(done)*: `mqtt-cluster::lease_membership` —
      a pure `MembershipReconciler::decide(view, desired) -> MembershipAction`
      (deterministic bootstrap by smallest id; only the leader reconciles voters;
      learners-then-promote), plus `apply_action` (initialize / add_learner +
      change_membership) and `raft_view` (from metrics). Tested: the pure policy
      exhaustively, and a live test where the reconciler bootstraps a node, grows the
      group to a second over the wire, and a committed lease replicates to the new
      voter. (Debounce is the driver's job, 4f.)
    - **4e — durable cluster `SessionStore`** ✅ *(done)*: `mqtt-cluster::cluster_store::GroupRoutedLog`
      implements `ReplicatedLog` by routing each key to its group's `ClusterLog`,
      built lazily (lease epoch from a `LeaseSource`, replica set from `Placement`);
      a non-owned group returns `NotOwner`. Wrap it in `ReplicatedSessionStore` →
      the durable cluster store. Test: an enqueue through the store **quorum-
      replicates to a follower** (the message survives the owner's loss), and a
      foreign group is refused. (The `LeaseSource` is stubbed here — the consensus
      group is already proven in 4c/4d; the openraft-backed source wires in at 4f.)
    - **4f — wire into `mqttd`** 🔶 *(in progress)*. The invasive final integration.
      Sub-pieces:
      - *store is `Arc`-shared* ✅ *(done)*: the hub's `Box<dyn SessionStore>` is now
        `Arc<dyn SessionStore>` (zero behavior change; all suites green), so
        connections can share it.
      - *real `LeaseSource`* ✅ *(done)*: `LocalLeaseSource` reads the leader-assigned
        lease epoch from the local `LeaseStore` (ADR 0007 §3 leader-driven model — no
        app-level write forwarding).
      - *leader-driven lease assignment* ✅ *(done)*: `mqtt-cluster::lease_assign::LeaseAssigner`
        — `pending(store)` (pure: groups whose committed holder ≠ placement owner)
        and `reconcile(raft, store)` (leader-only: `Assign` each pending group to its
        owner; idempotent). A live test: the leader assigns all `NUM_GROUPS` to the
        sole owner, then reconcile is a no-op.
      - *node assembly* ✅ *(done)*: `mqtt-cluster::durable_node::build_durable_node`
        ties the lease group + `DurablePlane` + durable store together and spawns the
        driver (a tick-loop that reconciles voters off the live `Placement` membership
        and, as leader, assigns each group's lease to its owner). A single-node smoke
        test: the assembly bootstraps itself, then an enqueue commits and replays.
      - *integration test* ✅ *(done)*: `tests/durable_sessions.rs` boots **three
        durable nodes over the real peer mesh + SWIM** (founder + two joiners) and a
        durable `enqueue` commits on the group owner — which on a 3-node group
        *requires* quorum (owner + ≥1 follower), so the message provably replicated
        to a peer. Non-flaky (8/8, ~0.8s). Also fixed a real split-brain hazard: a
        **founder gate** (`can_bootstrap`, a node with no SWIM seeds) so independently
        starting nodes don't each create a rival single-node lease group.
      - *`main.rs` gate* ✅ *(done)*: `MQTTD_DURABLE_SESSIONS=1` builds the durable
        node, hands its store to the hub, and `attach_durable_plane`s it (founder =
        no SWIM seeds); default-off keeps `MemorySessionStore`. The binary boots in
        durable mode and serves clients.
      - *conn QoS-2 dedup* ✅ *(done)*: `ConnPolicy` carries the shared store; the
        connection records/clears the QoS-2 inbound packet id in the store
        (`record_received` before PUBREC, `clear_received` on PUBREL) instead of a
        per-connection set — so the exactly-once window is durable. `None` keeps the
        in-memory fallback. A conn test drives QoS-2 over a store-backed policy and
        asserts the window lives in the store.

  **Workstream E is complete**: a durable, consensus-backed, quorum-replicated
  session store — built, proven over a real 3-node cluster, and shippable behind
  `MQTTD_DURABLE_SESSIONS`. (MQTT-observable session *survival* after an owner's
  death is workstream **F**, takeover.)
      - *hub plane routing* ✅ *(done)*: the hub holds an optional `DurablePlane`
        (`attach_durable_plane`); `forward_inbound` routes the four durable-plane
        frames to a new `HubCommand::DurableFrame`, which the hub **spawns** to
        `plane.handle` (off the actor loop) and replies over the peer's link;
        `register`/`fail` ride `PeerConnected`/`PeerDisconnected`/`PeerDead`. No-op
        until a plane is attached (single-node path unchanged; all suites green).
      - *membership reconciler driver* off SWIM (debounced).
      - *connection QoS-2 dedup* through the shared store.
      Warrants multi-node integration tests (swim-routing style).
- **Delivers:** durable sessions — ADR 0001's headline guarantee.

### F — Takeover / handoff protocol  ✅ *(done; needs B + E)*

ADR 0001 §5. The owner-dead path: on a SWIM `Dead` event, placement re-elects the
next replica as owner and the lease driver reassigns the group at a **new epoch**
(fencing the dead owner — the E mechanism). The missing piece is **log recovery**:
the new owner — previously a replica — must rebuild the group's committed log before
serving. Decomposed:

- **F-a — log recovery mechanism** ✅ *(done)*: `mqtt-cluster::cluster_log` gains
  `ClusterLog::recovered(...)` (seed a log's committed state from gathered entries,
  appends continue from the watermark) and `merge_replica_logs(reads)` (the pure
  quorum-recovery merge: union by offset, take the contiguous run, stop at the first
  gap — an uncommitted tail). Reading from a quorum guarantees no committed entry is
  lost. Tested: the merge (contiguous run, gap-drop, truncated prefix, empty) and a
  recovered log that replays its seeded entries and keeps appending.
- **F-b — recovery-read RPC** ✅ *(done)*: `PeerMessage::ReplicaRead{key}` /
  `ReplicaReadReply{entries}` (entries as `(offset, record)` tuples, so storage stays
  serde-free). `ReplicaTransport` gains `read_replica(replica, key) -> Option<Vec<
  LogEntry>>` (default `None`); `PeerReplicaTransport` implements it with `req_id`
  correlation + `fail_node` on link drop; `DurablePlane::handle` answers a
  `ReplicaRead` from its local `ReplicaState`. A test over the wire: after replicating
  to a follower, the owner reads that replica's log back; an unreachable peer → `None`.
- **F-c — rebuild on takeover** ✅ *(done)*: `GroupRoutedLog` recovers each key once,
  on its first touch after a takeover. The new owner was a replica, so its committed
  copy lives in the follower `ReplicaState` it shares with the `DurablePlane`;
  `recover_key` reads that local copy plus a quorum of peers (`read_replica`), merges
  them (`merge_replica_logs`), and `seed_key`s the group's `ClusterLog` so the
  recovered queue replays and appends continue at the next offset — `NoQuorum` if
  fewer than a quorum can be read (recovery would be unsafe). `build_durable_node`
  now threads one shared `ReplicaState` into both the plane and the store. Tested:
  a `GroupRoutedLog` whose shared `ReplicaState` is pre-seeded with a key's committed
  entries recovers and replays them, then continues appending past the watermark.
  *Carried:* the per-group log cache is not epoch-invalidated (a regain-after-loss
  self-fences but never serves a stale cache divergently); recovery adds one quorum
  read on the first touch of each key (a fresh session simply recovers to empty).
- **F-d — integration test** ✅ *(done)*: `durable_sessions::a_replica_serves_the_
  session_after_the_owner_dies` — a 3-node SWIM cluster durably enqueues on a
  client's owner, waits until the lease group has grown to all three voters (so
  losing one still leaves a raft quorum), then **crashes the owner** (aborts all its
  tasks). The survivors detect it dead, drop it from placement, re-elect the lease
  leader, and reassign the group to a surviving replica at a new epoch; the new owner
  — previously a replica — rebuilds the committed log from a quorum and serves the
  session, replaying the enqueued message with no loss. The double-own / split-brain
  safety is covered by the epoch-fencing unit test (`cluster_log::stale_leader_is_
  fenced`): a superseded holder cannot reach quorum at its old epoch. Non-flaky over
  repeated runs. *Deferred to a later hardening pass:* a client-facing reconnect
  through the MQTT front end during promotion (here the takeover-serve is asserted
  through the store, which is what recovers); spec-legal QoS-1 redelivery bounds.

### G — MQTT 5 expiry & shared subscriptions  *(gated on the MQTT 5.0 codec)*

ADR 0001 §6, roadmap step 3 — **blocked on the v5 codec milestone**, listed here
for completeness and sequencing.

- **Session Expiry Interval** → GC sessions (done, ADR 0009 phase 1); **Message
  Expiry Interval** → drop stale queued messages (done, ADR 0009 phase 2). The
  publisher's interval rides into the stored queue entry as an absolute deadline;
  expired copies are dropped at replay and the remaining interval is forwarded on
  the rest. Carried limitation: the deadline is durable in the log but the
  in-memory session-expiry timer restarts on takeover (ADR 0009 phase 3).
- **Shared subscriptions** (`$share/<group>/<filter>`) — done (ADR 0010): named
  groups with round-robin, online-preferring single delivery; reuses the QoS,
  persistence, and expiry machinery; retained messages are not replayed to a
  shared subscription. Carried limitation: cross-node delivery is one-per-node,
  not cluster-wide — true cluster-wide single delivery is deferred to the
  placement/ownership work (ADR 0005).
- **Topic aliases** — done (ADR 0011): negotiated per connection in both
  directions and resolved entirely at the connection edge, so routing, storage,
  and the cluster only ever see full topic names. We advertise an inbound maximum
  in CONNACK and resolve/validate inbound aliases; outbound, we assign-until-full
  up to the client's maximum. Carried limitations: no outbound LRU eviction, and
  an invalid alias closes the connection rather than sending DISCONNECT `0x94`
  (pending the v5 reason-code work).
- **Flow control** (Receive Maximum) and the **durable expiry deadline**
  (ADR 0009 phase 3) remain.

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
| With `MQTTD_DURABLE_SESSIONS` **off** (the default), persistent sessions are ephemeral — an owner's death drops its queues | No durability across owner loss on the default path (sharded capacity only) | enable the durable store (E done); survival across owner loss done in **F** (takeover) |
| ~~A durable session is not yet **served after its owner dies**~~ | — | ✅ **F** — a surviving replica rebuilds the committed log from a quorum and serves the session (`a_replica_serves_the_session_after_the_owner_dies`) |
| ~~A wedged-but-connected peer (half-open link) could hang an append's quorum wait or a takeover recovery-read indefinitely~~ | — | ✅ **hardening** — the replication transport bounds every RPC (`DEFAULT_RPC_TIMEOUT`); on timeout the request resolves as unreachable and is reaped |
| ~~The per-group `ClusterLog` cache was not epoch-invalidated: a group lost then regained kept serving at the stale epoch and self-fenced forever~~ | — | ✅ **hardening** — `GroupRoutedLog` reads the lease epoch on every op and rebuilds (and re-recovers) the group's log when it advances |
| ~~`ClusterLog::append` replicated to followers **sequentially**, so one slow/wedged replica added its full timeout to every append's latency~~ | — | ✅ **hardening** — append now fans out to all followers concurrently (`JoinSet`) and commits as soon as a quorum acks, abandoning stragglers (frames already sent → best-effort spread) |
| ~~Consensus engine (openraft) unratified~~ | — | ✅ **E step 1** — openraft ratified |
| ~~Consensus + replication not run by the live hub~~ | — | ✅ **E step 4** — the durable stack runs in the broker; proven by the 3-node integration test |
| ~~QoS-2 dedup set + next-packet-id counter not yet replicated state~~ | — | ✅ **E step 3c** — now in the replicated `m/{client}` snapshot |
| ~~`ReplicatedSessionStore` enqueue counted the queue via an **O(n) read** (materializing the whole queue) for cap enforcement~~ | — | ✅ **hardening** — counts via `ReplicatedLog::live_range`, an O(1) offset-watermark query on the real backends; nothing materializes the queue on the hot path |
| ~~Lease assignment issued one consensus write **per group** (all `NUM_GROUPS` on first leadership / a rebalance)~~ | — | ✅ **hardening** — `reconcile` batches every pending assignment into one `AssignMany` entry (each still mints its own fresh epoch); the lease log no longer bursts |
| ~~Session-proxy splice was **best-effort on half-close** (a select over two one-way copies dropped in-flight bytes the instant either direction ended)~~ | — | ✅ **hardening** — splice uses `copy_bidirectional`, which half-closes properly: a final PUBLISH/PUBACK/DISCONNECT the owner sends after the client stops writing still reaches the client |
| ~~Audit did not record the **vouching node** of a relocated session~~ | — | ✅ **hardening** — `ProxyHello` carries the landing node's id; the owner records `(relayed by node <id>)` on the session's `auth.success` audit event |

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
