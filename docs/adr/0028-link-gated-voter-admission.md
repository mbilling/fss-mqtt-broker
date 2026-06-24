# ADR 0028 — Link-gated lease-group voter admission

- **Status:** Accepted
- **Date:** 2026-06-24
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0028-link-gated-voter-admission.md](../delivery/0028-link-gated-voter-admission.md) — plan, progress, and changelog
- **Related:** [ADR 0007](0007-durable-store-integration.md) (the lease-group driver this
  refines), [ADR 0016](0016-swim-membership-stability.md) (SWIM membership, the source of the
  desired set), [ADR 0026](0026-lease-timing-durable-storage.md) (durable lease timing) and
  [ADR 0027](0027-replica-group-commit.md) (replica group-commit) — the two that made the
  *steady state* stable; this fixes the remaining *formation* churn

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0028-link-gated-voter-admission.md).

## Context

After ADR 0026 (timing) and ADR 0027 (replica group-commit) a durable 3-node cluster is
rock-stable in **steady state** — a 35-minute soak under load held the lease term flat for the
last 27 minutes with no churn. But the same soak exposed a **formation** problem: for the first
~8 minutes after startup the lease term climbed from 7 to 71 (one node alone triggered 68
elections) before settling. A churn confined to bring-up, but one that would hit *every*
startup.

The logs pin the cause. The founder bootstraps its single-voter group, then — as soon as SWIM
gossip lists the other two nodes — immediately runs `change_membership(ReplaceAllVoters{all
three})`. But SWIM discovery (UDP gossip) routinely outpaces the **raft peer-link** setup (the
TCP mesh the lease RPCs ride). So the leader admits voters it **cannot yet reach**: it can no
longer collect a quorum of heartbeat acks, its leader lease expires (`leader lease(3s) will
expire after 0ns`), and the group re-elects — repeatedly — until the mesh finally converges.
The driver computed the desired voter set purely from placement membership, with no regard for
whether each member's lease-RPC link was actually up.

## Decision

**Admit a node into the lease-group voter set only once its raft link is established.**

The lease driver's desired voter set becomes placement members **filtered by reachability**:

```
desired = { id ∈ placement.members() : id == local
                                       ∨ id ∈ current_voters
                                       ∨ raft_link_connected(id) }
```

- `raft_link_connected` is read from the [`MeshRaftNetwork`](../../crates/mqtt-cluster/src/raft_mesh.rs)
  peer registry (`is_connected`) — a peer is connected exactly when its outbound link channel
  is registered, which is when the lease RPCs to it can actually be delivered.
- The gate is **admission-only**. A node that is *already* a voter is **not** dropped when its
  link blips — it is removed only when it both loses its link *and* is evicted from placement
  by SWIM's dead-detection (ADR 0016). So a transient blip never churns the voter set; only a
  confirmed-dead member shrinks it.
- `local` is always reachable to itself, so the founder still bootstraps immediately.

The leader therefore grows the group incrementally as links come up — `{founder}` →
`{founder, A}` → `{founder, A, B}` — and never holds a voter it cannot reach, so it keeps its
quorum lease throughout bring-up.

## Consequences

- **Good:** the formation election storm is removed — the leader only ever depends on voters it
  can actually reach, so its lease holds from the first election onward. Durable cluster
  bring-up becomes quick and quiet instead of an ~8-minute churn, which is the precondition for
  durable being a sane default (the reason it is still opt-in until this is soak-proven).
- **Cost:** a node joins the voter set a little later — only after its mesh link is up rather
  than the instant SWIM sees it. That is strictly desirable here (an unreachable voter is worse
  than a not-yet-voter), and the extra delay is one mesh-connect, not a user-visible cost.
- **Risk:** the gate must not over-fire and drop healthy voters on a blip (that would *cause*
  churn). It is admission-only and unit-tested for exactly that: a current voter survives a
  full link blackout, and only a placement eviction removes it.

## Alternatives considered

- **Add new members as learners, promote when caught up.** openraft's `add_learner` already
  blocks until the learner catches up — but it is invoked with the same membership desire that
  was computed before the link was up, so it can still target an unreachable node and stall.
  Gating the *desire* on reachability is the cleaner, earlier cut; learner-then-promote still
  happens underneath for the admitted set.
- **Slow the driver tick / widen raft timeouts further.** Treats the symptom (re-election
  cadence) not the cause (unreachable voters), and trades away failover responsiveness. ADR
  0026 already widened the timing as far as is reasonable.
- **Wait for full mesh connectivity before any membership change.** Simpler to state but worse
  operationally — it blocks a 2-of-3 cluster from forming when one node is slow or down.
  Per-member admission lets the reachable majority form immediately and absorbs the laggard
  when its link arrives.
