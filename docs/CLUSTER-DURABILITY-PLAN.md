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

### A — Bounded queues & overload policy  *(now; no prerequisites)*

ADR 0001 §6, roadmap step 3. The memory queue is unbounded today — a
dead-but-persistent client is an OOM/DoS vector, which "security is the product"
should not tolerate even pre-cluster.

- Add a per-session cap and an overflow policy (`drop-oldest` /
  `reject-newest`) to `SessionStore::enqueue` semantics; surface the chosen
  policy + cap in config.
- Implement in `MemorySessionStore`; emit an audit/metric event on overflow.
- **Tests:** cap enforced; each overflow policy; overflow does not corrupt
  offset monotonicity or replay order.
- **Why first:** highest safety value, zero dependencies, and it nails down the
  queue-bound semantics every later backend must honor.

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

### C — Session affinity & redirect  *(needs B; ships the "ephemeral" mode)*

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

### D — Decision: consensus / replication mechanism  *(ADR 0005, blocks E)*

Capability Plan §8 open question; ADR 0001 §4 reserves consensus for "ownership
+ the enqueue log only." This is a hard, expensive-to-reverse choice and gets
its own ADR. Options to weigh:

- A Rust Raft library (`openraft`, `raft-rs`) running **one small group per
  shard** — not one global group (ADR 0001 rejects the global-leader ceiling).
- A purpose-built quorum-append for an idempotent, offset-addressed log (the log
  is simpler than general Raft state-machine replication; enqueue is an
  append, truncation is idempotent).
- Reuse of an external store as a `SessionStore` backend (ADR 0001's
  externalized-store alternative) for operators who already run one.

Decision criteria: split-brain safety on ownership, write-amplification cost,
operational complexity, dependency/supply-chain weight (must pass `cargo-deny`),
and whether per-shard groups can be created/torn down cheaply as membership
moves. **Output:** ADR 0005 + a `ReplicatedLog` interface the backend targets.

### E — Replicated session-log backend  *(needs B + D)*

ADR 0001 §2–§3. The durable backend implementing `SessionStore`.

- Enqueue = append to the session's per-shard log, **quorum-replicated before
  the producer's QoS≥1 PUBACK is released** (the durability contract).
- Dequeue/ack = local-first, lazily truncated; no synchronous cross-node hop on
  the ack path.
- Replicated state includes the **QoS-2 received-packet-id dedup set** and the
  next-packet-id counter, so exactly-once survives failover.
- **Tests:** enqueue survives a single replica loss (R=3, quorum=2); ack
  truncation does not lose un-quorumed writes; QoS-2 dedup holds across a
  simulated failover; a deterministic multi-node simulation (injectable loss /
  reorder) mirroring the SWIM-sim approach.
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
                          ├─▶ D (ADR 0005: consensus choice) ─▶ E (replicated log) ─▶ F (takeover)
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

## Test discipline

Every workstream is test-first, mirroring the rest of the repo: pure logic
(placement, queue policy, log semantics) gets exhaustive unit tests; multi-node
behavior gets deterministic simulation harnesses with injectable loss/reorder
(as the SWIM cluster tests already do) rather than timing-dependent integration
alone. The durability claims (no loss of quorum-durable messages; split-brain-
safe ownership; bounded, spec-legal redelivery) are each pinned by a test that
fails if the guarantee regresses.
