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
| E | Replicated session-log backend | 🔶 In progress — steps 1–2 + **3a** done (engine ratified; layering, fencing, **quorum-append core**); **3b networked transport / openraft next** |
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
  - **3b — networked transport + openraft** *(next)*: openraft manages the
    ownership lease/epoch (entering the build here); `ReplicaTransport` is realized
    over the mTLS peer mesh.
  - **3c — replicated exactly-once state**: the **QoS-2 received-packet-id dedup
    set** + next-packet-id counter join the replicated state (a `SessionStore`
    surface extension), and the queue-cap count moves to a rebuildable per-key
    index, so exactly-once survives failover and cap enforcement is exact.
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
| The quorum-append core is **not yet networked** — `ClusterLog` runs only over the in-process sim transport | Durability proven deterministically, not yet over the wire | **E step 3b** (`ReplicaTransport` over the mTLS peer mesh + openraft lease) |
| QoS-2 dedup set + next-packet-id counter **not yet replicated state** (not on the `SessionStore` trait surface) | Exactly-once would not survive failover yet | **E step 3c** (extends the replicated state) |
| `ReplicatedSessionStore` cap enforcement is exact only under **serialized per-key appends**; the in-memory backend counts via an O(n) read | Soft policy may mis-evict under concurrent appends to one key; no correctness/durability impact | **E step 3c** (lease serializes appends; backend keeps a rebuildable per-key index) |
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
