# ADR 0021 — Bounded lease-consensus voter set

- **Status:** Proposed
- **Date:** 2026-06-19
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0021-bounded-lease-voters.md](../delivery/0021-bounded-lease-voters.md) — plan, progress, and changelog
- **Related:** [ADR 0005](0005-session-affinity.md) (placement/ownership),
  [ADR 0006](0006-consensus-and-replication.md) (lease consensus + per-group replication),
  [ADR 0007](0007-durable-store-integration.md) (durable node assembly),
  [ADR 0016](0016-swim-membership-stability.md) (membership),
  [ADR 0018](0018-on-disk-persistence.md) (persistent lease store)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0021-bounded-lease-voters.md).

## Context

The lease consensus group is the openraft group that agrees **which node owns which
placement group** (one committed entry per ownership change, ADR 0006). Today its voter
set is *every* cluster member: the reconcile loop computes

```rust
// crates/mqtt-cluster/src/durable_node.rs
let desired: BTreeSet<RaftNodeId> = placement.members().iter().map(raft_id).collect();
```

and `MembershipReconciler` drives the Raft voter set to exactly that. So an N-node
cluster forms an **N-voter Raft group**. At 20 nodes that means a **20-voter group with
quorum 11**: every lease assignment needs 11 acks and every election coordinates 20
nodes. Raft is designed for small voter sets (3/5/7); a 20-voter group has high commit
latency, heavier elections, and more reconfiguration churn. The design **conflates
cluster membership with consensus voters** — they should be decoupled.

Two clarifications about blast radius:

- **This is the *lease* group only.** Session-data durability is a *separate* mechanism:
  each session's log is quorum-replicated over a per-group replica set of size
  `DEFAULT_REPLICAS` (3) via the `ClusterLog` (ADR 0006), **not** over the lease voters.
  Bounding lease voters does not touch session-data replication or its durability.
- **Ownership is independent of voting.** Any node can own placement groups and route
  traffic; ownership (HRW, ADR 0005) is assigned *by* the lease group, not *restricted
  to* its voters.

The lease group is **low-traffic** (assignments happen on ownership change, not per
message), so a small fixed voter set is more than enough to serve the whole cluster.

## Decision

**Bound the lease-consensus voter set to a small fixed size `N` (default 5, odd,
configurable via `MQTTD_LEASE_VOTERS`); every other member joins the group as a
non-voting *learner*.** Learners still receive the replicated lease log — so every node
can read current lease assignments for routing and recovery, and can hold ownership — but
only the `N` voters participate in elections and quorum. Quorum stays `⌊N/2⌋ + 1` (3 for
N = 5) **regardless of cluster size**.

### 1. Voter selection: sticky, with vacancy-fill

Voters are **sticky** to minimise Raft reconfiguration:

- A node that is a voter stays a voter while it is alive.
- When the live voter count is below `N` (initial growth, or a voter died), promote the
  **lowest-id alive learner(s)** until there are `N` voters (or all live members are
  voters, if the cluster is smaller than `N`).
- A voter that leaves the cluster (gone from `placement.members()`) is removed; a voter
  that is alive is never demoted just because a new node joined.

This is a deterministic function of *(current committed voter config, alive members)* —
which every node, and every successive leader, computes identically — so reconcilers do
not disagree. New nodes therefore join as learners and only become voters to fill a real
vacancy; a steady-state cluster sees **zero voter churn** as nodes come and go outside the
voter set.

### 2. All members are at least learners

Every eligible member is added as a learner if not already known, so the committed lease
log replicates to all of them. A learner that HRW makes a placement owner reads its
assigned lease epoch from that replicated log exactly as a voter would — it does not need
to vote to own a group or run its `ClusterLog` at the granted epoch.

### 3. Reconciler reshape

`MembershipReconciler::decide` changes from "make every member a voter" to computing a
target *(voters, learners)* split per §1, and `apply_action` reconciles toward it:

- add new members as learners (`add_learner`, blocking so they catch up first);
- `change_membership(target_voters)` — promoting filled vacancies and **demoting** any
  removed voter to a *learner* (retain it; it is still a cluster member that should keep
  the log), not dropping it;
- drop members that have actually left the cluster.

Transitions stay quorum-safe by changing the voter set incrementally (openraft's
`change_membership` performs the safe membership transition); we never reduce below
quorum because at most one voter is replaced at a time and a fill promotes a
*caught-up* learner.

### 4. Founder & bootstrap unaffected

The founder (the seedless, lowest-id node, ADR 0007) still bootstraps the group with
itself as the sole voter; the vacancy-fill rule then grows the voter set to `N` as
members join — the same path that already grows it today, just capped at `N` instead of
"everyone".

### Configuration

`MQTTD_LEASE_VOTERS` (default `5`). Effective voters = `min(N, live_eligible_count)`.
Recommend an odd value; `1` degrades to a single-voter (no fault tolerance) group, `3`
tolerates one voter loss, `5` tolerates two.

## Consequences

- **Good:** the lease group scales to large clusters — quorum is fixed (3 at N = 5) no
  matter whether the cluster is 7 or 200 nodes, so assignment latency and election cost
  stop growing with cluster size. Sticky voters minimise disruptive Raft reconfigurations.
  Cluster membership and consensus voting are cleanly decoupled. Session-data durability
  (per-group R = 3) and ownership are unchanged.
- **Cost:** a real voter-selection policy and a more nuanced reconciler (target
  voters + learners, demote-to-learner) instead of "all members are voters". A learner
  that becomes an owner depends on learner replication latency to learn its lease — already
  the steady-state path, and bounded by the same replication as a voter.
- **Risk:** membership-change correctness is consensus-critical. It is gated by openraft's
  safe `change_membership` and developed test-first, and the persistent vote (ADR
  0018) already makes voter restarts safe. A degenerate config (`N = 1`, or `N` larger
  than the cluster) must behave sanely (single voter / all-voters), covered by tests.

## Alternatives considered

- **Keep all members as voters (today).** Simple, but does not scale past a handful of
  nodes — the motivating problem.
- **HRW / hash-based voter selection.** Distributes voter role by hash, but the lease
  group is low-load so load distribution is a non-goal, and HRW churns the voter set when
  *any* node joins/leaves (re-hash), causing more Raft reconfiguration than sticky
  vacancy-fill. Rejected for more churn at no benefit.
- **Sharded / multi-Raft lease groups.** Partition groups across several small Raft
  groups for horizontal consensus throughput. Overkill: lease assignment is rare, so one
  small group serves any realistic cluster. Revisit only if assignment throughput (not
  cluster size) ever becomes the bottleneck.
- **External consensus (etcd/consul) for leases.** A heavy external dependency that cuts
  against the self-contained, single-binary, minimal-supply-chain design (ADR 0002/0018).
