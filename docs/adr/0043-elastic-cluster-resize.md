# ADR 0043 — Elastic cluster resize (grow, shrink, replace)

- **Status:** Proposed
- **Date:** 2026-07-13
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0043-elastic-cluster-resize.md](../delivery/0043-elastic-cluster-resize.md) — plan, progress, and changelog
- **Related:** [ADR 0021](0021-bounded-lease-voters.md) (voter cap, stickiness, demote-to-learner —
  the consensus half of resize, already built), [ADR 0016](0016-swim-membership-stability.md)
  (failure-domain-aware voter spread), [ADR 0028](0028-link-gated-voter-admission.md) (a joiner
  becomes a voter only when reachable), [ADR 0026](0026-lease-group-raft-timing.md) (joint-consensus
  membership changes), [ADR 0019](0019-graceful-shutdown.md) (leave/drain/flush — extended here with
  data handoff), [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (the quorum-replicated group logs whose replica sets resize moves), [ADR 0017](0017-durable-attach-readiness.md)
  (recovery honesty across ownership moves), [ADR 0042](0042-durable-plane-stress-harness.md)
  (the harness whose oracle verifies all of this), [ADR 0039](0039-versioning-and-upgrade-policy.md)
  (rolling binary upgrades ride the same one-node-at-a-time motion),
  [ADR 0005](0005-session-affinity.md) (sessions attach on their owner — ownership moves are
  client-visible until the proxy lands)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0043-elastic-cluster-resize.md).

## Context

"Adding a node adds throughput" is capability claim #3
([CAPABILITY-PLAN](../CAPABILITY-PLAN.md)), and the upgrade path it implies — start on a
laptop, grow to a server pair, grow to a zone-spread five, shrink back when the bill
matters — is a core sell to early adopters. The **consensus half** of that story already
works and is unit-tested: the membership reconciler admits joiners as learners, openraft
catches them up on the lease log, vacancy-fill respects failure domains, voters demote to
learners on shrink, all within the voter cap (ADR 0016/0021/0026/0028).

The **data half does not exist yet**. A 2026-07-13 inventory of the code and test suite
found, for every resize direction:

- **New replicas are hollow.** When the placement ring changes, a node entering a group's
  replica set receives only *new* appends — nothing back-fills the group's history. The
  hollow replica still counts toward quorum, so a later takeover recovery can assemble a
  "quorum" of {empty newcomer, lagging survivor} and silently drop committed entries that
  live only on the third copy. The only repair path (`recover_key`) fires on an
  owner-epoch change, never on a replica-set change.
- **Ownership moves are lazy.** A group's new owner recovers each key on first touch;
  between the ring change and that touch it serves nothing and migrates nothing — the same
  window the ADR 0042 harness exposed for takeovers (exhibit ⑥) reopened by resize.
- **Leaving loses copies.** ADR 0019's graceful leave announces departure and flushes
  local writes, but hands off neither leases nor replica data. Remove two of a group's
  three replicas and its committed data can walk out the door with them.
- **Laptop-mode data never re-replicates.** A single-node broker writes single-replica
  (quorum = 1) groups; the replicas added as the cluster grows never receive that history.
- **No integration test resizes a running durable cluster.** Every multi-node test forms
  its cluster before serving; the stress harness kills and restarts within a fixed
  membership.

One more honesty item: with two members, replica sets clamp to two and quorum is 2-of-2 —
a two-node durable cluster has *strictly worse* write availability than one node. The
upgrade story must say so.

## Decision

Resize — grow, shrink, and their composition (rolling host replacement) — becomes a
**first-class, data-safe operation**, verified by the ADR 0042 harness under the same
acked-facts oracle as every other fault. Five parts:

### 1. A replica is not a replica until it has caught up

A node entering a group's replica set starts as a **catch-up replica**: it back-fills the
group's existing log (the `ReplicaKeys` key-discovery + `recover_key` quorum-read
machinery from ADR 0042 T9, pointed at the group instead of a session) before it counts
toward *any* quorum — append quorums and recovery-read quorums alike. Until then the
group's effective replica set is the old one. The caught-up watermark is durable state,
not a guess: a restart mid-catch-up resumes or restarts the back-fill, never fakes
completion. This closes the hollow-replica hazard and, with it, the laptop-data case —
growing 1→N back-fills the single-replica history as a plain instance of the same rule.

### 2. Ring changes migrate eagerly, not on first touch

A membership change triggers the same eager materialization the harness forced for
takeovers (ADR 0042 T9): new owners scan, recover, and advertise the groups they gained —
sessions, retained state, interest — without waiting for a client or publish to touch
them. The takeover window machinery (settle/re-route, mesh-whole ack rule) already holds
acks honest while this runs; resize inherits it unchanged.

### 3. Shrink is a decommission, not a disappearance

Removing a node on purpose becomes an explicit **decommission**: the node (a) stops
accepting new sessions, (b) waits until every group it owns has a caught-up successor
and every group it replicates retains its full replica count *among the remaining
members* (rule 1 does the copying), (c) demotes from voter per ADR 0021, then (d) leaves
per ADR 0019. Decommission is observable (progress via the health endpoint) and
interruptible (a crash mid-decommission is just a crash — the survivors recover as for
any death). How the operator requests it (signal vs. admin endpoint) is a delivery
decision.

### 4. The two-node truth is documented, not papered over

2 nodes = quorum 2-of-2 = no write fault-tolerance. The operator docs state it, the
recommended upgrade is **1→3 in one motion**, and 2 nodes is supported as an explicit
waypoint (e.g. mid-way through 1→3) whose degraded margin the health endpoint reports.

### 5. Resize joins the harness vocabulary

The ADR 0042 stress harness gains `join` and `decommission` schedule steps, and dedicated
upgrade-path tests cover 1→3, 3→5, 5→3, and the rolling host replacement (add one,
decommission another) — all under the unchanged acked-obligations oracle, which is
already sufficient to catch resize data loss. Rolling binary upgrade (ADR 0039) rides the
same one-node-at-a-time motion and gets a whole-cluster test exercising the proto
negotiation window.

## Consequences

- Grow becomes safe *and* boring: start the new node with `MQTTD_SWIM_SEEDS` pointing at
  any member, and the cluster does the rest — the operator guide is one paragraph.
- Writes to a group pay no new cost in steady state; catch-up cost is paid once per
  joined replica, off the hot path.
- Until part 1 lands, resize of a durable cluster is **unsupported and unsafe**; the
  README says so to keep early adopters off the hazard.
- Decommission gives the 5→3 cost-reduction exercise a supported path; pulling plugs
  remains crash semantics, handled by the existing takeover machinery.

## Alternatives considered

- **A standing rebalancer / anti-entropy daemon** (Cassandra-style repair): continuous
  background reconciliation of every replica. More machinery than the problem needs —
  replica sets change only on membership events here, so event-driven catch-up with a
  durable watermark covers the same safety with none of the steady-state cost. Revisit if
  per-group replica placement ever becomes dynamic.
- **Operator-driven manual migration** (copy the data dir, edit seeds): error-prone
  exactly where the product promises ease, and impossible to make atomic against live
  writes.
- **Consistent-hash rings with virtual-node movement** to minimize data motion: HRW
  already moves the minimum set of groups per membership change; the gap is copying
  bytes, not choosing fewer of them.
- **Do nothing (status quo):** silently violates the durability contract on the exact
  operation the capability plan advertises.
