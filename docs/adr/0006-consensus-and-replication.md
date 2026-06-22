# ADR 0006 — Consensus & replication for durable sessions

- **Status:** Accepted
- **Date:** 2026-06-13
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0006-consensus-and-replication.md](../delivery/0006-consensus-and-replication.md) — plan, progress, and changelog
- **Related:** [ADR 0001](0001-session-durability.md) §4, [ADR 0005](0005-session-affinity.md)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0006-consensus-and-replication.md).

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

2. **Use a proven consensus engine; do not hand-roll — specifically openraft.**
   The ownership-lease / epoch layer is built on **openraft** (async-native,
   actively maintained, fits the tokio codebase). Hand-rolling leader election,
   fencing, and membership change is precisely the class of subtle
   distributed-systems bug a correctness- and security-first broker must not own.
   The fencing logic *we* write (rejecting appends at a superseded epoch) sits on
   top of the engine's primitives, prototyped engine-agnostically in
   `mqtt-cluster::lease`. openraft is chosen over the alternative async-incompatible
   `raft-rs`: openraft is the only mature async-native Raft that passes our
   `cargo-deny` gate clean, whereas `raft-rs` ships an *active* DoS vulnerability
   (RUSTSEC-2024-0437, protobuf 2.x uncontrolled-recursion pinned through
   `raft-proto`, unfixed upstream) plus an unmaintained `fxhash` (RUSTSEC-2025-0057)
   — disqualifying for a security-first broker. The accepted cost is openraft's
   heavier transitive tree (~79 net-new crates), which the durability investment
   already anticipated; it is revisited if a lighter, gate-clean alternative appears.

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
