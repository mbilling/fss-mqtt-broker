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

   **As delivered — degradation is visible, and gating it is the operator's
   choice (issue #167).** *(2026-08-12 — **partly superseded**: the default is no
   longer off. Read the 2026-08-13 / issue #239 note below for the shipped posture;
   everything here about the mechanism still holds.)* "R=3, quorum=2" holds only
   while three nodes are
   placement-eligible: replica sets truncate to `min(R, members)`, so a
   shrinking cluster keeps acking durable writes on smaller sets — down to
   quorum-of-1 on a lone node — which is availability silently spending the
   configured durability. Delivered in two parts. *Visibility*: cluster-wide
   replication health (configured R vs the smallest replica set any group
   holds) is exported as the `replication_desired` / `replication_min_actual`
   gauges, in the `/statusz` `replication` block, and as an edge-triggered
   WARN/recovery log from the reconcile driver. *Gating* (`durable.min_replicas`,
   default 1 = off **at the time of this note; now `"majority"` — see below**): a
   group whose replica set is below the operator's floor
   REFUSES `append` — transient like `NoQuorum`, so QoS≥1 acks are withheld
   (sources redeliver) and retained mutations queue until capacity returns;
   reads, acked-driven truncation and removal stay served, degrading the node
   to serving what it already promised rather than dying. The default of this note
   kept the standing requirement that a lone node is fully operational when alone —
   the derived default below keeps that requirement while arming the floor
   everywhere else; a floor above the replication factor is rejected at startup as
   unsatisfiable.

   **As delivered (2026-08-13, issue #239) — the floor is ON by default, derived
   from the membership the node knows.** Leaving the gate opt-in left the default
   posture exactly the one the audit called out: a group degraded to one live
   member acked durable writes on ONE copy while the operator believed R=3. The
   default is now `durable.min_replicas = "majority"`, which resolves per node to
   `min(R, witness) / 2 + 1`, where the *witness* is the largest membership this
   node can justify believing in: the **quorum-committed durable raft roster**
   (issue #229) when it has been pushed, otherwise the high-water observed
   gossip membership — and never below the operator-declared
   `runtime.ready_min_members`. So the floor is **1** while a node has never
   known a peer (a fresh single node keeps acking — the standing requirement is
   untouched) and **2** once it knows it belongs to a cluster of two or more (the
   lone survivor of a three-node group refuses). An explicit integer keeps #167's
   absolute-floor meaning, with `1` as the documented opt-out.

   *Why the roster and not liveness.* Any witness that follows liveness *down*
   disarms itself during the very outage it guards: burying two peers would
   "un-know" them and drop the floor back to 1. The raft roster cannot shrink
   without a lease quorum, so it cannot shrink while a majority is down; it
   survives restart via the on-disk lease state, so a node that cold-boots alone
   out of a three-member group is still armed; and it *does* shrink on a consented
   decommission, so a deliberate resize needs no operator edit.

   *Why this costs no availability.* The write quorum is already
   `replica_set.len() / 2 + 1`, so at two or three live members a floor of 2
   refuses nothing that a one-at-a-time roll did not already require — the witness
   is capped at R, which makes the derived floor satisfiable by construction.
   That is the load-bearing reason the default could be turned on at all; an
   unconditional floor of 2 (breaking the fresh single node and founder bootstrap)
   or a strict R=3 (breaking the documented roll) could not have been.

   *What the gate enforces, exactly.* The floor bounds the **size of the replica
   set** an append replicates over, not the number of copies the commit waits for —
   `ClusterLog`'s write quorum is `replica_set.len() / 2 + 1`. For the derived
   default the two coincide at every satisfiable size (a set of 2 or 3 needs 2 acks
   and the floor is 2), so the promise "no single-copy durable acks once the node
   knows it has peers" is exactly what is enforced. An **explicit** integer is
   looser than its wording suggests: `min_replicas = 3` passes on a 3-member set and
   then commits once 2 of the 3 hold the record. That is #167's carried semantics;
   the config docs, the reference TOML and the startup log all say so rather than
   promising the stricter reading.

   *Residual gaps, stated rather than papered over.* (i) A bare-metal node that
   boots alone with `ready_min_members = 1` but is really part of a mesh resolves
   the floor to 1 until its first peer observation or roster push — a seconds-long
   window. Folding the declared member count into the witness closes it for every
   deploy path that renders one (the operator and Helm chart do); persisting a
   gossip-only latch instead is rejected (ADR 0038 pre-1.0 format freeze — and it
   would buy nothing here, because the roster below already produces the
   restart-armed behaviour, DR case included).

   (ii) **A shrink to one member needs explicit consent, and the roster arms the
   floor by itself.** Two distinct paths reach it. A *consented* decommission
   commits a 1-member roster, and the roster is the authority in
   `Placement::min_replicas`, so the floor follows it down with no operator edit —
   `ready_min_members` is then the only thing that can still hold it at 2, and
   lowering it to 1 suffices. But an **unconsented** loss (two of three nodes gone
   for good, an AZ loss, a DR restore of a single node's `data_dir`) leaves the
   committed roster naming three members, so the floor stays at 2 **whatever
   `ready_min_members` says** — that is the witness doing its job, and it is the
   case an operator hits at 3am. The remedy for both, and the one the runbooks
   name, is `durable.min_replicas = 1`; it is a restart-scoped `[durable]` edit
   (`requires_restart`), so a SIGHUP reload stages it without applying it. See
   OPERATIONS (Scaling) and TROUBLESHOOTING.

   (iii) **None — the refusal is reachable end to end at R=3 and is asserted
   there.** An earlier draft of this note claimed the opposite ("every state whose
   replica set is 1 also has no lease quorum, so the refusal is not reachable at
   this cluster size"); that claim was false and has been withdrawn. A lone
   survivor of a three-member cluster still holds the committed lease for the
   groups it owned — `LocalLeaseSource::epoch_for` is a local read of the applied
   lease map with no leadership, quorum or TTL check, and reassignment is what needs
   a quorum — so a QoS 1 publish for a durable session in one of those groups
   reaches the append, is refused by the floor, and the publisher's PUBACK is
   withheld (`mqttd_durable_append_failures_total{reason="unavailable"}`; log:
   `replica set holds 1 of the configured floor of 2 copies`). Measured on spawned
   binaries while settling this question — 30/30 attempts refused, the counter
   moving once per attempt, on both a follower and a leader survivor — and now
   asserted every run by
   `cluster_proc::the_default_write_floor_arms_itself_and_refuses_the_single_copy_promise`.
   With the floor off (`MQTTD_MIN_REPLICAS=1`) the same publish is ACKED on a single
   copy: issue #239's defect, and the one-env-var falsification of that test. One sequencing caveat belongs in the record:
   `enqueue_with_expiry` reads (`live_range`) before it appends and the read is not
   floor-gated, so until the ADR 0043 P1 catch-up sweep re-stamps the shrunken
   replica set the enqueue fails earlier with `NoQuorum`; the refusal is what the
   state settles on, and the process-tier test gates on `/statusz`
   `replica_groups.current == tracked` before probing.

   (iv) **The floor covers only writes that reach a group this node leases.** A
   publish for a durable session whose owner is gone is still acked and dropped by
   the pre-existing no-known-subscriber path, with no refusal logged and no
   `publish_dropped{reason}` distinguishing it. So "too thin to promise ⇒ refuse"
   is true of the leased path and not of that one; README Limitations says so, and
   changing it is out of scope for #239.

   *Operator surface.* The resolved floor is exported as the
   `replication_write_floor` gauge beside `replication_desired` /
   `replication_min_actual`, and `/statusz`'s `replication` block carries
   `write_floor` plus `write_floor_source` (`derived` | `configured`) so a floor of
   1 reads unambiguously: "this node is alone" versus "someone disabled it".
   `min_actual < write_floor` is the page condition (durable writes are being
   REFUSED); `min_actual < desired` remains the warn. With durable sessions off the
   resolved floor is 1, so that rule cannot fire on a cluster that refuses nothing.
   `--check-config` **and the ADR 0046 T4 live-reload acceptance gate** now reject
   an unsatisfiable explicit floor before a rollout instead of at the first refused
   write — the reload gate matters because `durable` is restart-scoped, so an
   accepted-but-unsatisfiable floor would brick the next boot hours later.

   *Rollback (ADR 0058 §E).* This widened an existing key's value shape from
   integer to integer-or-`"majority"`, and the reference config ships the word
   form. A config carrying `min_replicas = "majority"` therefore **cannot be read
   by the previous release**: it is a type mismatch, which
   `runtime.config_unknown_keys = "warn"` does not rescue. Pre-1.0 that is allowed, and it is recorded
   here so it is a decision rather than a surprise: to keep a config
   roll-back-safe within this release, spell the floor as an integer (or leave the
   key out and take the default).

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
