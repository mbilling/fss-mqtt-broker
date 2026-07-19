# ADR 0049 — Durable ownership must be lease-eligible, and a degraded durable plane must be visible

- **Status:** Accepted
- **Date:** 2026-07-19 (accepted 2026-07-19)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0049-voter-eligible-durable-ownership.md](../delivery/0049-voter-eligible-durable-ownership.md) — plan, progress, and changelog
- **Related:** [ADR 0021](0021-bounded-lease-voters.md) (bounded lease voters — this ADR
  **amends its §2**: a learner cannot in fact serve durable ownership), [ADR 0005](0005-session-affinity.md)
  (HRW placement/ownership), [ADR 0017](0017-durable-attach-readiness.md) (the fail-closed
  attach path that correctly refuses but, against a learner owner, refuses *forever*),
  [ADR 0006](0006-consensus-and-replication.md) (lease group + per-group replica set),
  [ADR 0020](0020-metrics-and-observability.md) (the readiness + metrics this makes honest),
  [ADR 0026](0026-lease-timing-durable-storage.md) / [ADR 0027](0027-replica-group-commit.md)
  (the fsync-bound commit path behind the incident),
  [7-node post-mortem](../postmortems/2026-07-14-ha-bridge-durable-refused.md) (the evidence)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0049-voter-eligible-durable-ownership.md).

## Context

The [2026-07-14 post-mortem](../postmortems/2026-07-14-ha-bridge-durable-refused.md) surfaced
two real defects in the durable plane, one availability bug and one observability gap. They
share a single root and are fixed together.

**1. Ownership can land on a node that can never serve it.** HRW placement
(`Placement`, `crates/mqtt-cluster/src/placement.rs`) selects a group's owner
(`group_owner`, line 161) by hashing over `self.eligible` — *this node plus every non-`Dead`
SWIM peer* (`observe`, line 115). `Placement` has **no awareness of the lease voter set**.
When `MQTTD_LEASE_VOTERS` (`voter_cap`, default 5) is smaller than the cluster, the surplus
members are non-voting **learners** (ADR 0021), yet HRW still picks them as owners:
`LeaseAssigner::pending` (`lease_assign.rs:62`) assigns the group lease to `group_owner(group)`,
learner or not. A learner cannot hold a servable lease, so `claim_session` on it returns a
transient `NotOwner`/`NoQuorum` forever; the ADR 0017 recovery loop (`recover_until_ready`,
`hub.rs:4765`) burns its 5 s budget and the persistent CONNECT is refused with CONNACK
**0x88** — *every time, deterministically*, for any client id whose group hashes to a learner.
In the incident ~2/7 of all session ids were structurally unrecoverable.

This **directly contradicts [ADR 0021 §2](0021-bounded-lease-voters.md)**, which asserts a
learner "that HRW makes a placement owner reads its assigned lease epoch from that replicated
log exactly as a voter would — it does not need to vote to own a group or run its `ClusterLog`."
The incident proves that path does not actually serve durable recovery. This ADR amends that
claim: **owning a durable group requires being lease-eligible.**

**2. A dead durable plane reports healthy.** `/readyz` (`health.rs:172`) gates on
`lease_group_ready` (`durable_plane.rs:183`), which is `there is a leader AND this local node
is itself a voter`. It samples *leadership + local voter membership* — never *"can a
persistent session actually attach?"*. So a cluster that refused 100% of durable attaches for
11 hours reported green throughout, and `durable_append_failures_total` never moved because a
*recovery* failure is not an *append*. The failure was invisible to every automated signal.

Note the fail-closed behaviour itself was **correct** (ADR 0017): no session was silently
downgraded to clean; clients got a retryable 0x88. The bug is that the retry could never
succeed, and nothing said so.

## Decision

### 1. Durable ownership is restricted to lease-eligible nodes

A durable group's **owner** is selected by HRW over the **voter-eligible set** — the current
lease voters — not over the full SWIM-eligible set. Every group therefore has an owner that
can hold a servable lease, so no session id is structurally unrecoverable. Concretely:

- `Placement` gains a `voters` set, pushed each reconcile tick by `run_driver`
  (`durable_node.rs:391`), which already holds both `RaftView.voters`
  (`lease_membership.rs:270`, mapped `RaftNodeId → NodeId`) and the `Placement` handle.
- `group_owner`/`owner`/`owner_route` hash over `voters` (∩ eligible). **Bootstrap safety:**
  if the voter set is not yet known (empty), fall back to the eligible set exactly as today,
  so a fresh/single-node cluster is unaffected.
- **Settle before restricting.** The restriction is applied only once the voter set has held
  steady for a few reconcile ticks; while it is still growing (founder bootstraps as *sole
  voter*, then grows to `voter_cap`), ownership falls back to the eligible set. Restricting
  mid-growth would concentrate *every* group on the founder and then thrash it out via a
  mass lease migration — pathological under load (a disk-stressed founder never converges).
  By the time the set settles in a small all-voter cluster, `voters == eligible`, so the
  restriction is a no-op there; a bounded cluster gets it once stable.

### 2. Session-data replication is unchanged — the owner just leads the set

ADR 0021 deliberately decoupled *data durability* (a per-group replica set of size `R = 3`,
ADR 0006) from the *lease voter set*, and that stays true: the replica set still spans the
full eligible member set, so learners continue to hold replica data and a large cluster keeps
its wide durability domain. The only change is that the **voter owner leads its group's
replica set** (owner-first, as today, where owner was always `replica_set[0]`). Ownership
moves to a voter; the R replicas that hold the data do not shrink to the voter set.

When a learner is promoted to fill a voter vacancy (ADR 0021 sticky vacancy-fill), groups may
re-own onto it; the existing eager-migration machinery (ADR 0043 P2) moves ownership and data
exactly as it already does for any ring change — no new migration path.

### 3. A degraded durable plane is measurable

The blind spot is closed with **metrics first, readiness second** — deliberately in that
order, because flipping `/readyz` to NotReady under transient load would evict healthy nodes
and cause orchestration churn (the cure worse than the disease):

- **`durable_recovery_failures_total`** (ADR 0020 registry, `metrics.rs`, modelled on
  `durable_append_failed`; incremented at the attach refusal in the hub's `Unavailable` arm)
  — the *direct* fingerprint: a persistent attach refused with 0x88, distinct from an
  *append* failure (which stayed at zero through the incident).
- **`lease_quorum_ack_ms`** gauge — the *leading* indicator, mirrored from openraft's
  `millis_since_quorum_ack` each gauge refresh. **Design note:** the draft proposed a
  `lease_rpc_timeouts_total` counter, but the incident's degradation was follower *fsync*
  slowness with a healthy network — openraft's RPC timeout fires while our `MeshConn` send
  still eventually gets a late reply, so a network-level timeout counter cannot see this
  failure mode. `millis_since_quorum_ack` (a growing "time since the leader last reached
  quorum") measures the degradation *directly* and reads cleanly from raft metrics — the
  accurate instrument. (`mqtt-cluster` has no `mqtt-observability` dependency, so the plane
  exposes `quorum_ack_age_ms()` and the hub mirrors it into the gauge — no layering
  inversion.)
- Together they make the *green-but-degraded* state alertable: recovery refusals climbing (or
  the quorum-ack age growing) while appends stay flat is exactly the incident's fingerprint.
- Readiness is **augmented, not inverted**: `/readyz` keeps its status contract (it must not
  flap a healthy node), but its JSON body additionally reports durable-serviceability signals
  (voter count, quorum-ack age) so an operator probing a suspect node sees the truth.

### 4. Demo sizing is documented, and a stale log line fixed

- The demo/quickstart docs state that **≥5 durable nodes on a single host is fsync-bound**
  and may degrade this way; the default demo stays small (or sets `MQTTD_LEASE_VOTERS`
  appropriately). This is an honesty note, not a code change.
- `hub.rs` `note_session_ownership` logs "(ephemeral mode)" on a durable cluster — corrected,
  since it actively misleads during exactly this kind of incident.

Out of scope (tracked, not fixed here): the leader `/readyz` hang observed once at capture
time (metrics served while readyz hung) — unexplained and needs its own investigation.

## Consequences

- **The availability bug is closed:** no persistent session id can map to an owner that
  cannot serve it, so the deterministic-0x88-forever failure mode is gone regardless of
  `voter_cap` vs cluster size.
- **ADR 0021's replication-independence is preserved:** data still replicates across the
  whole eligible set; only ownership is voter-restricted. The change is narrow and rides the
  existing migration machinery.
- **The plane is observable:** the degradation that hid for 11 h is now two counters an alert
  can watch, without destabilising the k8s-facing `/readyz` contract.
- **Cost / risk:** ownership selection is consensus-adjacent and on the takeover path; it is
  developed test-first with the real cluster harness, asserting the core invariant (below)
  and that ownership converges as voters change. The voter set must be plumbed into
  `Placement` and kept current each tick — a small, well-scoped addition.

**Invariant the tests enforce:** *for any cluster size N and any `voter_cap ≤ N`, every
placement group's owner is a current voter, and every persistent session attaches (or is told
to retry for a genuinely transient reason) — never refused forever because its owner cannot
serve.*

## Alternatives considered

- **Learners proxy recovery to the lease group (post-mortem follow-up 1, option B).** Keep
  learners as owners but have them forward `claim_session` to a voter. Preserves ADR 0021 §2
  literally, but adds a hop on the hot attach path, a new failure surface (proxy target
  selection, its own timeouts), and leaves ownership on a node that still cannot *hold* the
  lease. Restricting ownership to voters is simpler, removes the failure mode at the source,
  and needs no new RPC. Rejected in favour of §1.
- **Restrict the whole replica set to voters (owner *and* replicas).** Simplest to implement
  (just intersect `Placement.nodes()` with voters), but it shrinks the data-durability domain
  to ≤ `voter_cap` nodes and violates ADR 0021's explicit decoupling of replication from
  voting. Rejected: durability spread should not depend on the consensus cap.
- **Flip `/readyz` to sample durable-serviceability and NotReady the node when degraded.**
  Tempting, but a node cannot cheaply prove *every* group it might be asked to serve is
  recoverable, and a readiness that flaps under transient fsync pressure would evict healthy
  nodes and amplify the outage. Metrics + an alertable signal, with readiness augmented (not
  inverted), is the safer instrument. Rejected in favour of §3.
- **Leave it to operators (size the cluster so `voter_cap ≥ N`).** That is a real mitigation
  (and the demo note in §4), but it makes a *deterministic data-availability bug* the
  operator's responsibility and still fails silently. The broker must not have a config in
  which a slice of session ids is permanently unusable. Rejected as the primary fix.
