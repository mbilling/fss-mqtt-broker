# ADR 0006 — Consensus & replication for durable sessions

- **Status:** Accepted; engine **ratified (openraft)** by the workstream-E spike
- **Date:** 2026-06-13 (engine ratified 2026-06-15)
- **Deciders:** project maintainers
- **Related:** [ADR 0001](0001-session-durability.md) §4, [ADR 0005](0005-session-affinity.md),
  [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md) workstream D,
  Capability Plan §8

## Context

Workstreams A–C delivered bounded queues, a placement ring, and session
relocation: a persistent session is sharded to and served by its owner. But
session state is **in-memory and non-durable** — an owner's death loses its
sessions (ADR 0005's "ephemeral mode"). ADR 0001 designed the way out: per-
session ownership, a replicated append-log per session, quorum-durable enqueue
gating the QoS≥1 PUBACK, lazy local truncation, and replicated QoS-2 dedup
state. ADR 0001 §4 scoped consensus to "session ownership and the enqueue log…
while message fan-out stays coordinator-free."

The hard, unsolved part is **split-brain-safe ownership**. HRW over SWIM
([ADR 0001](0001-session-durability.md) §1, workstream B) gives a *placement
preference*, but SWIM is weakly consistent: partitioned nodes can disagree on
who owns a session, and two writers to one session's log diverge it. Durable,
single-owner sessions need consensus. This ADR decides *what provides it*,
*build vs. buy*, and *the seam that insulates the broker from the choice*.

## Decision

1. **Consensus is scoped to ownership leases, not to every log entry.** A small,
   low-traffic consensus layer establishes, per placement group, *which node
   holds the ownership lease* and at *what epoch*. The lease-holder then
   replicates the per-session append-log by **epoch-fenced quorum replication**
   over the R-node replica set — one quorum round-trip per append, not a leader
   election per entry. Putting full per-entry consensus on the QoS≥1 PUBACK path
   would tax every persistent-delivery message; a lease plus fenced quorum-append
   keeps the steady-state cost to a single quorum round-trip. This is exactly
   ADR 0001 §4: consensus for *ownership and the log's integrity*, never on the
   fan-out path.

2. **Use a proven consensus engine; do not hand-roll.** The ownership-lease /
   epoch layer is built on an established Rust consensus library — **openraft**
   is the leading candidate (async-native, actively maintained, fits the tokio
   codebase). Hand-rolling leader election, fencing, and membership change is
   precisely the class of subtle distributed-systems bug a correctness- and
   security-first broker must not own. The fencing logic *we* write (rejecting
   appends at a superseded epoch) sits on top of the engine's primitives.
   **The workstream-E spike ratified openraft** (see *Spike outcome* below): it is
   the only mature async-native Raft that passes our `cargo-deny` gate clean. The
   fencing layer is prototyped engine-agnostically in `mqtt-cluster::lease`.

3. **`ReplicatedLog` is the seam.** A generic async append-log trait
   (`append` / `read` / `truncate` / `remove` over keyed, offset-addressed byte
   records, `mqtt-storage::repl`) separates MQTT session/queue semantics (the
   `SessionStore` backend, workstream E) from the replication mechanism. Three
   backends:
   - `InMemoryReplicatedLog` — single-node, always-owner; ships **now** for
     development, tests, and non-clustered deployments.
   - the consensus-backed cluster log — workstream E's production target.
   - an external-store adapter (Redis / FoundationDB / …) — the operator option
     ADR 0001 keeps for shops that already run such a store.

4. **The durability contract** the cluster backend must honor (specified now for
   E): `append` returns only once the record is epoch-fenced and quorum-durable
   across the replica set (R=3, quorum=2 default) — this is what gates the
   producer's QoS≥1 PUBACK. `truncate` is local-first and lazy (ack truncation
   needs no synchronous cross-node round-trip). The QoS-2 received-packet-id
   dedup set is part of the replicated state, so exactly-once survives failover.
   A stale lease-holder after a partition heals is **fenced**: replicas reject
   appends at a superseded epoch, so it cannot reach quorum and cannot diverge
   the log.

## Spike outcome (workstream E step 1)

The spike evaluated the two candidate engines against our `deny.toml` and built the
engine-agnostic fencing prototype. Findings (2026-06-15):

| Engine | Net-new crates | `cargo-deny` | Model | Verdict |
|--------|---------------:|--------------|-------|---------|
| **openraft** 0.9.24 | 79 | **clean** (advisories / licenses / bans / sources all ok) | async-native, actively maintained | **ratified** |
| raft-rs (`raft`) 0.7.0 | 15 | **FAILS** — RUSTSEC-2024-0437 (protobuf 2.28 uncontrolled-recursion **DoS**, unfixed upstream in 0.7) + RUSTSEC-2025-0057 (`fxhash` unmaintained) | sync, protobuf-driven | rejected |

- **openraft is ratified.** It is heavier (79 transitive crates — clap, chrono,
  `rust_decimal`, `validit`, `anyerror` among them; ~+56% over the current
  141-crate workspace) but it is the only mature async-native option and it passes
  the supply-chain gate with **no new policy exceptions**. The weight is the
  accepted cost ADR 0001/this ADR already anticipated; it is revisited if a
  lighter, gate-clean alternative (e.g. a stable openraft 0.10, currently
  alpha-only) appears before E step 3 binds it.
- **raft-rs is rejected.** Despite a far smaller tree it ships an *active* DoS
  vulnerability pinned through `raft-proto`'s protobuf 2.x — disqualifying for a
  security-first broker.
- **The engine is not yet a workspace dependency.** The spike measured it on a
  throwaway branch and reverted; openraft lands for real at E step 3 (the
  consensus-backed `ReplicatedLog`), so the 79 crates do not enter the build until
  the code that needs them does.
- **The fencing layer is built and proven** independent of the engine:
  `mqtt-cluster::lease` (`LeaseGroup` / `OwnershipLease` / epoch fencing) is a pure,
  sans-I/O state machine whose tests pin the split-brain-safety property (two
  epochs can never both reach quorum; a superseded holder is fenced). It maps onto
  openraft's leadership term at E step 3.

This ratifies the ADR as written; no amendment was required.

## Consequences

- Durable, split-brain-safe sessions become buildable (workstream E), and
  cross-node takeover (F) follows; ADR 0005's ephemeral relocation upgrades to
  durable.
- One quorum round-trip and R× write amplification on QoS≥1 enqueue — ADR 0001's
  accepted cost. QoS-0 and non-persistent paths are unaffected.
- A real consensus dependency enters the supply chain; reviewed via `cargo-deny`,
  with dependency weight and FIPS considerations (cf. ADR 0002) weighed at the
  spike.
- The `ReplicatedLog` interface is the **v1 seam** and may evolve when the spike
  surfaces real implementation constraints — stated honestly rather than frozen
  prematurely.

## Alternatives considered

- **One global Raft group for all sessions** — a single leader is a throughput
  ceiling, the opposite of linear scale. Rejected (also ADR 0001's rejected
  alternative).
- **Per-session Raft groups** — thousands of groups, each heartbeating, is
  untenable. Rejected; placement groups / a bounded partition count cap the
  number of consensus groups.
- **Hand-rolled quorum + fencing as the default mechanism** — maximal control,
  but owning a consensus implementation is the wrong risk for this project.
  Rejected as the default; the thin fencing we write rides on the proven engine.
- **An external store as the default backend** — contradicts shared-nothing and
  moves the bottleneck into the store. Kept as an operator-selectable
  `ReplicatedLog` backend, not the default.

## Phasing (workstream E)

1. **Spike + decide the engine** ✅ *(done)*: `cargo-deny` review of openraft and
   raft-rs plus the engine-agnostic ownership-lease/fencing prototype
   (`mqtt-cluster::lease`). Outcome above — openraft ratified, ADR unchanged.
2. **`SessionStore` over `ReplicatedLog`** ✅ *(done)*: `ReplicatedSessionStore`
   (`mqtt-storage::logged`) implements the full `SessionStore` over a
   `ReplicatedLog`, holding no durable state of its own — queue in a `q/{client}`
   log, session metadata in `m/{client}`. A test pins the layering: a second store
   over the same log sees the first's sessions in full, so a durable log yields
   durable sessions. Done **first** (ahead of the spike): it needs no network and
   no engine choice, and it validates the seam shape before any dependency lands.
3. **The consensus-backed `ReplicatedLog`**, in three sub-steps:
   - **3a — epoch-fenced quorum-append core** ✅ *(done)*: `mqtt-cluster::cluster_log`
     — `ClusterLog` implements `ReplicatedLog` by quorum-replicating each append
     across the replica set behind a `ReplicaTransport` seam, gated on the
     lease-holder's epoch (`ReplicaState` fences a stale holder). Sans-I/O, with a
     deterministic loss-injecting simulation pinning the contract: quorum-durable
     append, single-replica-loss survival (R=3/q=2), below-quorum rejection with no
     committed hole, stale-leader fencing, lazy local truncation, and the step-2
     `ReplicatedSessionStore` running unchanged on top.
   - **3b-i — networked transport** ✅ *(done)*: `mqtt-cluster::repl_net` —
     `PeerReplicaTransport` realizes the `ReplicaTransport` seam over the peer mesh
     (`PeerMessage::Replicate` / `ReplicateAck`, with `req_id` ack correlation and
     `fail_node` on link drop). Pinned by tests over real framed streams: append
     round-trip + follower apply, stale-epoch fencing over the wire, unreachable
     replica, and in-flight failure on disconnect. Driven directly until the live
     hub is wired (step 4).
   - **3b-ii — openraft lease manager**, in turn:
     - **state machine + type binding** ✅ *(done)*: openraft is now a real
       dependency (in the build, through `cargo-deny`). `mqtt-cluster::lease_raft`
       defines the replicated `LeaseMap` (`group -> (holder, epoch)`, monotonic
       epoch — the fence source) and binds it to openraft via
       `declare_raft_types!(LeaseConfig)` over numeric `RaftNodeId`s, with a
       compile-assert that it is a valid `RaftTypeConfig`.
     - **storage** ✅ *(done)*: `mqtt-cluster::lease_store::LeaseStore` implements
       openraft's `RaftStorage` over `LeaseMap` (log, vote, applied state, snapshots —
       in memory). Validated by openraft's own conformance `Suite`, which exercises
       every storage method against the protocol's correctness requirements.
     - **network + bring-up** ✅ *(done, in-memory)*: `mqtt-cluster::lease_group`
       implements openraft's `RaftNetwork` (append-entries / vote / install-snapshot)
       and brings up a real group. Tests prove the full stack end to end: a
       single-node group elects itself and commits a lease, and a **three-node group
       elects a leader and replicates a committed lease to every replica** — through
       real consensus, into our `LeaseMap`.
     - **mesh network**: carry the same RPCs over the mTLS peer bus (replacing the
       in-memory router), mapping the cluster's string `NodeId` ↔ numeric
       `RaftNodeId` (openraft node ids are `Copy`).
   - **3c — replicated exactly-once state**: extend the session state with the
     **QoS-2 received-packet-id dedup set and the next-packet-id counter** (not on
     the `SessionStore` trait surface today, so exactly-once does not yet survive
     failover), and replace the in-memory backend's O(n) cap count with a
     rebuildable per-key index (the lease serializes per-key appends, making cap
     enforcement exact).
4. **Wire it in**: swap `mqttd`'s `MemorySessionStore` for the durable backend so
   relocated-session owners (ADR 0005) write through it — ephemeral sessions become
   durable — then cross-node takeover (workstream F).
