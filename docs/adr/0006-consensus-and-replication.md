# ADR 0006 — Consensus & replication for durable sessions

- **Status:** Accepted (architecture); engine choice gated on a spike (workstream E)
- **Date:** 2026-06-13
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
   appends at a superseded epoch) sits on top of the engine's primitives. The
   final library is gated on (a) a `cargo-deny` dependency review and (b) a
   focused spike — the first task of workstream E — which may ratify or amend
   this ADR.

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

1. **Spike + decide the engine**: `cargo-deny` review of openraft (and
   alternatives) plus a prototype ownership-lease group; ratify or amend this ADR.
2. **`SessionStore` over `ReplicatedLog`** ✅ *(done)*: `ReplicatedSessionStore`
   (`mqtt-storage::logged`) implements the full `SessionStore` over a
   `ReplicatedLog`, holding no durable state of its own — queue in a `q/{client}`
   log, session metadata in `m/{client}`. A test pins the layering: a second store
   over the same log sees the first's sessions in full, so a durable log yields
   durable sessions. Done **first** (ahead of the spike): it needs no network and
   no engine choice, and it validates the seam shape before any dependency lands.
3. **The consensus-backed `ReplicatedLog`**: ownership lease + epoch-fenced
   quorum-append, and the cluster `SessionStore` backend over it. This step also
   extends the replicated session state with the **QoS-2 received-packet-id dedup
   set and the next-packet-id counter** — not on the `SessionStore` trait surface
   today, so exactly-once does not yet survive failover — and replaces the
   in-memory backend's O(n) cap count with a rebuildable per-key index (the lease
   serializes per-key appends, making cap enforcement exact rather than
   best-effort).
4. **Wire it in**: swap `mqttd`'s `MemorySessionStore` for the durable backend so
   relocated-session owners (ADR 0005) write through it — ephemeral sessions become
   durable — then cross-node takeover (workstream F).
