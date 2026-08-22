# 0073. Scale-out durable ownership: the voter set is a control plane, not the data path

Date: 2026-08-22
Status: Proposed

## Context

The v1.0.2 scale curve made the ceiling exact: durable throughput does not
scale with node count, **by design**. ADR 0049 restricted durable ownership to
the lease-voter set (`MQTTD_LEASE_VOTERS`, default 5, ADR 0021), so at most
five nodes ever do owner-side work — coordination, the owner's fsync, the ack
path — no matter how many nodes the cluster has. 1/3/5 is the entire uncapped
regime; node six adds zero durable capacity. Meanwhile the same curve shows
the axes that DO scale (fan-out ~linear 1→3→5, connections flat per node), and
the competitive scale-out story (EMQX's 100M / HiveMQ's 200M benchmarks —
docs/COMPARISON.md context) is precisely about adding cheap nodes instead of
buying bigger ones.

Two facts, both already true in the code, frame the way out:

1. **Replication was never voter-bounded.** ADR 0021 §2 and ADR 0049 §2 kept
   the per-group replica set (R=3, ADR 0006) spanning the *full* eligible
   member set: learners hold replica data today. The fsync capacity of the
   cluster already spreads; only *ownership* concentrates.
2. **ADR 0049's restriction was an incident fix, not an architecture.** The
   2026-07-14 post-mortem found a learner that HRW made an owner could not
   *serve*: `claim_session` against it returned `NotOwner`/`NoQuorum` forever,
   and every session id hashing to it was structurally refused. The recorded
   decision fixed the availability bug by shrinking the ownership domain to
   voters — correct triage — but the underlying defect ("a learner cannot act
   on an assigned lease") was contained, not repaired. Its cost has now been
   measured: the durable curve is flat past the voter cap.

What actually requires a *quorum vote* in an owner's life is tiny and rare:
lease grant, lease renewal, epoch bump at takeover. What is *hot* — appends,
fsyncs, acks, delivery — needs only (a) a committed lease to act under and
(b) the group's own R-replica quorum, which is independent of the voter set.
This is the classic control-plane/data-plane split: a small fixed consensus
steering placement while data capacity scales with members (the shape Kafka's
KRaft controller quorum vs. broker/partition layer proved at fleet scale).

### Why not partitioned voter domains (the obvious alternative)

Sharding the 256 placement groups across K independent 5-voter lease groups
("domains") also scales ownership, and was seriously considered. Its drawback
list is long and structural — recorded here so the trade is auditable:

- **K consensuses multiply every failure mode.** Readiness, brownout, the
  min-replicas floor, formation, upgrade and backup logic all become
  domain-vectors; "partially formed" becomes a *normal* state to reason about.
- **Blast radius reshapes into the 0049 incident.** Lose one domain's
  majority on a 10-node cluster and *half the client-id space* deterministically
  refuses (0x88) while the cluster looks 70% healthy — the exact
  structurally-unrecoverable-subset failure 0049 called a defect, now by design.
- **Domain assignment is a meta-consensus.** Static assignment defeats
  "just add nodes"; dynamic assignment is a new consensus *above* the
  consensuses, and re-sharding groups between domains on resize means
  migrating replicated logs between raft groups under load — the exact
  reshape-in-place motion the 1.0 freeze closed.
- **Capacity arrives in stairs** (nothing at 7 nodes, 2× at 10, 3× at 15),
  each stair bounded by its domain's slowest disk, and hot-key skew can idle
  whole domains.
- **It multiplies the least-stable layer.** Issue #368's membership split was
  diagnosed only this week; K lease groups is K instances of that surface.

The control/data split gets the same scaling WITHOUT any of this: one lease
group, one membership plane, no domains, no stairs, no re-sharding.

## Decision (proposed)

**Make every admitted member a servable durable owner; keep the bounded voter
set as the control plane that grants and arbitrates leases.** Concretely:

### 1. Learners act on leases through the leader

The lease log is already replicated to learners (ADR 0021: "every node can
read current lease assignments"). What a learner-owner lacked was the *write*
path: lease claim/renewal/epoch operations are raft writes, and a non-voter
cannot propose locally. The fix is standard raft practice: **forward the
proposal to the lease leader over the existing mesh**, wait for commit, read
the committed result from the local replica of the log. Voters vote; owners
merely *ask*. The hot path is untouched: appends and acks run under an
already-committed lease against the group's own replica set, exactly as today.

- Serving gate: an owner (voter or learner) serves a group's sessions iff the
  *committed* lease map names it AND its lease term is current. The 0049
  fail-closed behavior stays: no committed lease, no service, retryable 0x88.
- Renewal liveness: a learner-owner that cannot reach the leader for a term
  renewal lets the lease lapse and stops serving — indistinguishable, by
  design, from today's voter losing quorum. Fail-closed is preserved; only
  the set of nodes that can *hold* a lease grows.

### 2. Ownership hashes over all admitted members again

`Placement::group_owner` returns to HRW over the full eligible set (the
pre-0049 domain), restoring 1/N ownership spread — with 0049's settle
discipline kept (no restriction/expansion flapping while membership churns).
The eager-migration machinery (ADR 0043 P2) already moves ownership and data
on any ring change; growing 5→10 nodes rebalances ~half the groups, the same
motion any join causes today. No stairs: every added node adds owner capacity.

### 3. The 0049 observability contract extends to learner-owners

`durable_recovery_failures_total`, `lease_quorum_ack_ms`, and the /readyz
body's durable-serviceability block apply to every owner. One new signal:
**`lease_forward_failures_total`** — a learner-owner's failed lease proposals
(the new way to be green-but-degraded), the direct fingerprint of "owner
cannot reach the control plane", alertable exactly like the 0049 pair.

### 4. Mixed versions cannot split ownership

An old node computes owners over voters; a new node over everyone — two HRW
domains in one cluster would dual-own groups. The expansion therefore gates on
a **cluster-wide capability**, not a local flag: ownership stays voter-bounded
until every member advertises the new peer-proto capability (ADR 0038's
version surface), and the flip is committed *through the lease log itself* (a
control record), so every node changes domain at the same lease epoch. Rolling
upgrades (BASELINE_REF oracle) see voter-bounded ownership until the roll
completes, then one committed flip — never two simultaneous truths.

## What this does NOT promise ("infinite", honestly)

- **Lease-transition throughput** still bounds how fast ownership can *churn*
  (grants/renewals/takeovers through one 5-voter raft). Steady-state renewal
  load is O(groups/term), trivial for 256 groups; mass-failover storms are
  paced by the control plane — same as today, and the right failure shape
  (slow takeover beats split brain).
- **256 placement groups cap useful owner parallelism** at 256 nodes in
  theory and far earlier in practice (placement granularity). Group count as a
  formation-time knob is future work, noted, not proposed here.
- **SWIM/gossip and the peer mesh** have their own fleet-size ceilings
  (hundreds, not thousands) — unchanged by this ADR and honestly out of scope.
- **Per-message cost still exists.** Scale-out multiplies owners; ADR 0071's
  group commit and issue #376-class fixes are what make each owner worth
  multiplying. The two levers compose; neither replaces the other.

## Consequences

- Durable throughput scales with node count until disk/NIC/control-plane
  limits — the scale curve's flat line becomes a slope, measurable on the
  existing rig (`run.sh full 7` with the PR #375 voters variant becomes the
  A/B: capped vs. uncapped ownership on identical hardware).
- The blast radius of losing the voter majority is *control only*: committed
  leases keep serving until term expiry; new attaches and takeovers refuse,
  fail-closed. Contrast with domains, where data quorums die with their domain.
- ADR 0021 §2's original claim ("a learner can own") becomes TRUE, with the
  serving path 0049 proved missing actually built; ADR 0049's ownership
  restriction is superseded by this ADR once T4's capability flip ships, its
  observability contract (§3) carried forward wholesale.
- The 0049 incident cannot recur in its original form: the serving gate is
  the *committed lease map*, not voter status — a node that cannot act never
  holds a committed lease, so HRW can no longer assign service to a dead end.

## Alternatives considered

- **Partitioned voter domains** — rejected above; the drawback list is the
  bulk of this ADR's context and is what this design must beat.
- **Raise `MQTTD_LEASE_VOTERS` to N** — a majority-of-N vote on every lease
  operation, growing latency and shrinking partition tolerance as the cluster
  grows; measured next on the 7-node curve (PR #375's variant) as the
  documented anti-pattern it is expected to be.
- **Operator-level cells** (K independent clusters, client-side sharding) —
  viable today, zero code, physically independent failure domains; but no
  single endpoint, no cross-cell subscriptions, K× operations. Remains the
  honest recommendation for tenant-isolated fleets; this ADR is for one
  cluster that must scale.
- **Status quo + per-node optimization only** — 0071/#376 keep paying, but
  the ceiling stays 5×(per-node); the up-vs-out economics case
  (10 CCX23 ≙ 1 CCX63 at €1.72/h) cannot be argued while out is capped.

## Delivery

Staged so every step is independently shippable and falsifiable —
[docs/delivery/0073-scale-out-durable-ownership.md](../delivery/0073-scale-out-durable-ownership.md):
T1 leader-forwarded lease operations for learners (behind the capability, off);
T2 the committed ownership-domain flip + mixed-version oracle coverage;
T3 placement over all members + migration soak; T4 the measured curve
(7/10-node, capped-vs-uncapped A/B) and the SCALE-CURVE.md/COMPARISON.md
publication that makes the scale-out claim with evidence.
