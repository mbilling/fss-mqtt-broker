# ADR 0016 — SWIM membership stability (dead-node fencing + false-positive resistance)

- **Status:** Accepted
- **Date:** 2026-06-18
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0016-swim-membership-stability.md](../delivery/0016-swim-membership-stability.md) — phases, progress, and changelog
- **Related:** [ADR 0003](0003-gossip-authentication.md) (SWIM datagram auth),
  [ADR 0005](0005-session-affinity.md) (placement owns relocation),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (durable sessions whose recovery depends on a correct replica set),
  [ADR 0017](0017-durable-attach-readiness.md) (the attach-path half of the failover gap),
  [ADR 0021](0021-bounded-lease-voters.md) (the bounded voter set §5 spreads across
  failure domains), [ADR 0022](0022-signed-gossip.md) (the authenticated gossip that
  carries §5's domain labels)

> This record states the decision only. The phased rollout and how far along it is live
> in the [delivery doc](../delivery/0016-swim-membership-stability.md).

## Context

Placement (and therefore the durable session store's replica sets) is derived directly
from SWIM membership. A diagnosed failure (the durable client-observable-failover gap)
traced to **membership instability**: after a node is killed, a survivor's `members()`
flaps to a *wrong* set that **still lists the killed node** (resurrected) and has **dropped
a live survivor** (falsely evicted). The resulting replica set has no live quorum, so
session recovery reads the dead node, times out, and fails — stalling the first client
reconnect ~10s with `session_present=false`.

The SWIM implementation already had the core mechanisms (`swim.rs`): per-node
**incarnation** numbers, **suspicion** (a probe failure marks `Suspect`, not `Dead`), and
**self-refutation** (a node hearing itself suspected bumps its incarnation and re-asserts
`Alive`). Two specific gaps remained — exactly the two halves of the diagnosed bug:

1. **`Dead` is not terminal.** A `Dead` member stayed in the map and was *resurrected* by
   any later higher-incarnation update. A node that refuted to a high incarnation just
   before dying left that `Alive(high)` gossip in flight; it arrived after the `Dead`
   declaration and revived the corpse. No tombstone, no pruning.
2. **Suspicion → `Dead` is single-source with fixed timeouts.** One prober's timeout was
   enough to drive a peer to `Dead`. A CPU-starved prober declares healthy peers dead, and
   a starved victim cannot refute within the fixed window. Nothing adapted timeouts to
   local health or required independent confirmation.

These are well-understood problems with well-understood fixes (the SWIM paper's terminal
`Dead`/tombstone, and the **Lifeguard** extensions). This ADR adopts them, scoped to what
closes the gap.

## Decision

### 1. `Dead` is a tombstoned terminal state (fixes resurrection)

When a member reaches `Dead` it becomes a **tombstone**: kept with a `tombstone_deadline =
now + DEAD_TTL`, and during that window **no gossiped update can revive it** —
`Alive`/`Suspect` updates *about a tombstoned node* are dropped regardless of incarnation.
After `DEAD_TTL` the tombstone is **pruned**.

`DEAD_TTL` is set comfortably above the gossip drain time, so a stale pre-death refutation
cannot outlive the tombstone — the resurrection is impossible. A node that genuinely
restarts rejoins **after** the tombstone is pruned, or under a fresh id; it does not need
to out-race stale gossip. (Self-refutation for the *local* node is unaffected — a node
always defends its own liveness; the tombstone governs only third-party claims about
*others*.) This mirrors the dead-node interval in mature gossip stacks (memberlist/Serf).

### 2. Lifeguard local-health awareness (reduces false positives)

Adopt Lifeguard's **self-awareness multiplier**: the local node tracks a small `awareness`
score that rises when its *own* probes go unanswered or it has to refute itself (signals
that *it* is the slow one) and decays on healthy rounds. Both `ack_timeout` and
`suspicion_timeout` scale by `(1 + awareness)`, so a locally-degraded node waits **longer**
before suspecting peers, and as a victim benefits from peers' longer suspicion windows.
This attacks false-eviction without weakening detection on a healthy node (awareness = 0 ⇒
unchanged timeouts).

### 3. Independent-suspicion confirmation before `Dead`

Also from Lifeguard: a node's **suspicion timeout shrinks as independent suspicions
accumulate** (and starts long). A single prober's suspicion holds the full window; only
when multiple distinct nodes independently suspect the same peer does it fast-track to
`Dead`. One contended prober can no longer unilaterally kill a healthy peer. (Refutation
still overrides a false suspicion outright.)

### 4. Keep the existing incarnation/refutation core

Incarnation precedence and self-refutation are correct and stay. The changes are
*additive*: a terminal/tombstoned `Dead`, awareness-scaled timeouts, and
confirmation-scaled suspicion.

### 5. Failure-domain-aware voter placement (T4/T5, added after acceptance)

[ADR 0021](0021-bounded-lease-voters.md)'s bounded voter set introduced a concentration
risk the full-membership model never had: a topology-blind (lowest-id) fill can put a
quorum of the N voters in one rack/zone, so that domain's loss halts lease consensus for
the whole cluster. Two additive tasks close this:

- **Selection (T4):** voter selection is **sticky first, balanced second**. A live,
  eligible voter is never demoted to improve the spread (voter changes are Raft membership
  changes — churn there costs more than an imperfect spread); only *free* decisions —
  filling a vacancy, or the one-time shrink when adopting the cap — pick from the
  **least-represented failure domain**, tie-breaking by lowest id so every successive
  leader computes the same target deterministically. An unlabelled node is its own
  singleton domain; with no labels at all the selection is bit-for-bit the prior
  id-ordered fill.
- **Topology source (T5):** each node advertises **only its own** `MQTTD_FAILURE_DOMAIN`
  label inside the authenticated SWIM gossip payload (HMAC + per-node signature +
  anti-replay, ADR 0003/0022/0023), learned non-erasingly like the routing address. The
  lease driver reads the assembled map live each reconcile tick, overlaid on the optional
  static `MQTTD_FAILURE_DOMAINS` seed (gossip wins) — the topology self-assembles and
  tracks membership. The selection algorithm is source-agnostic (`decide_with_domains`
  takes any map), which is the seam that let T5 swap the source without touching T4.

This mirrors proven operational designs — Consul autopilot's redundancy zones (Serf-tag
driven zone-aware voters) and Kubernetes zone-spread — rather than inventing new theory.

**Honest limits of §5** (stated so operators do not assume stronger properties than are
delivered):

1. **The spread is eventual, not an invariant.** Stickiness means an already-concentrated
   voter set (e.g. 3-of-5 voters in one rack because those nodes joined first) *stays*
   concentrated until natural churn opens vacancies. The system converges toward balance;
   it never forces it. Do not read "voters spread across domains" as a guarantee that
   holds from the moment labels are configured.
2. **Arithmetic beats topology at fewer than 3 domains.** Spreading 5 voters over 2 racks
   still leaves ≥3 in one rack, so that rack's loss still takes quorum. Domain-loss
   tolerance requires **≥ 3 failure domains** (just as Raft needs ≥ 3 voters for one node
   loss). The mechanism spreads voters; it cannot beat the majority arithmetic.
3. **T5 trades "identical map by construction" for eventual consistency.** T4's static
   cluster-uniform map guaranteed every leader computed the same target from the same
   input. Gossip-learned labels can transiently differ between nodes, so a leadership
   change mid-propagation may briefly produce a different target. Accepted because the
   reconciler is leader-only, debounced, vacancy-driven and sticky — divergence yields at
   worst a transient extra membership proposal, never a safety violation (Raft joint
   consensus owns safety) — and the views converge as gossip settles.
4. **Domain labels are self-asserted.** ADR 0022 signing authenticates *which node* made
   a claim, not that the claim is *true*: a compromised (but validly-certified) node can
   claim any domain — e.g. a unique fake rack to make itself the balancing algorithm's
   most attractive voter pick, or a victim domain's label to dilute its representation.
   Impact is bounded (voter placement skew → availability degradation, not a safety/
   forgery issue, since consensus safety still needs a quorum), and it matches the plane's
   trust model: an authenticated member is trusted for its own metadata, exactly as it is
   for its own address. Hardening options and their costs are recorded as **0016-T6**
   (deferred) in the delivery doc.
5. **A gossiped label silently overrides the static seed.** When both sources are set and
   disagree, gossip wins without a warning today — a one-line `warn!` on mismatch would
   fit the "weaker/surprising states are loud" house rule and is noted in 0016-T6's scope.

## Consequences

- **Good:** closes the membership half of the durable failover gap (recovery sees a live
  quorum); placement/replica-set correctness no longer depends on perfectly-timed gossip;
  fewer spurious ownership churns; grounded in published, battle-tested algorithms.
- **Cost / limits:** a node that is **genuinely** restarted (same id) must wait out
  `DEAD_TTL` before rejoining, or use a fresh id. A node *falsely* declared `Dead` is
  tombstoned for `DEAD_TTL` too; §3 makes that rare but not impossible, so `DEAD_TTL` is a
  bounded, tuned value. Awareness adds a little per-node state and slows detection on a
  degraded node (by design).
- **Risk:** this is correctness-critical membership code feeding durability. It must be
  developed **test-first against the pure `Swim` state machine** (no network), reproducing
  resurrection and false-eviction as deterministic unit tests *before* any wiring. A wrong
  change here can silently corrupt the durability guarantees, so it warrants its own focused
  workstream — not a reactive edit.

## Alternatives considered

- **Just loosen the test's SWIM timings.** Reduces the *false-eviction* rate but does
  nothing about *resurrection*, and papers over a real production fragility. Rejected.
- **Prune `Dead` immediately (no tombstone).** A re-learned `Alive` still re-inserts the
  node. The tombstone is what makes `Dead` stick.
- **Full Lifeguard (buddy-system probing, NACK-aware, all knobs).** More than this gap
  needs; §2–§3 take the parts that address the observed failures and leave the rest as a
  later option.
- **Make recovery tolerate a bad replica set (retry / shrink quorum).** Retrying against a
  set with no live quorum cannot succeed, and shrinking the quorum breaks the safety
  (intersection) property. Membership must be correct; recovery must not paper over it.
