# Post-mortem: kind runtime smoke — durable retained write refused forever (ring/lease ownership split)

**Date:** 2026-07-20 · **Environment:** ADR 0047 `kube-smoke` (kind, 3-node StatefulSet, `lease_voters=3`, `cpu request 50m`) · **Status:** root cause confirmed and FIXED (SWIM driver learns the datagram source address); regression test added

> Related decisions: [ADR 0037](../adr/0037-durable-retained-messages.md) (durable retained keyspace),
> [ADR 0049](../adr/0049-voter-eligible-durable-ownership.md) (voter-eligible ownership — the `voters`
> derivation implicated here), [ADR 0016](../adr/0016-self-forming-cluster.md) (self-forming gossip mesh),
> [ADR 0047](../adr/0047-kubernetes-deployment.md) (the deployment whose runtime smoke caught this).
> Sibling incident: [2026-07-14](2026-07-14-ha-bridge-durable-refused.md) — same symptom family
> (durable `NotOwner`/refused), a *different* root cause (fsync-bound RPC timeouts + learner placement).

## Summary

On the ADR 0047 kind runtime smoke, the 3-node cluster formed correctly (raft converged to 3 voters
in ~17 s, all pods reached `Ready`), but a single durable **retained** publish (`smoke/state`) could
never commit: the owning node logged `retained durable commit failed … not the owning node for this
group` once per second for ~80 s until the smoke's read-back timed out. It never healed.

Root cause: **one broker's gossip/placement membership was isolated (`members = [self]`) while it was a
fully-functioning lease-raft voter.** Its HRW placement ring therefore believed it owned *every* group,
which disagreed with the committed lease map (the raft-agreed truth). The durable data path trusts the
local HRW ring, so the write was routed to a node that did not hold the group's lease, was refused, and
could not be re-routed — the isolated node had no peer link to the real lease holder either.

## Impact

- Durable retained publishes are silently unreliable on a broker whose gossip view has collapsed to
  itself, even though it reports `Ready` and participates in consensus. QoS-1 `PUBACK` is returned to
  the client while the retained value is queued and never commits — a **durability lie** to the client
  (unlike the 2026-07-14 incident, which fail-closed with 0x88).
- Scope observed: the `kube-smoke` retained round-trip. Session durability is exposed to the same class
  of bug (same `owns_group`/placement authority), not yet observed here.
- No production deployments affected (pre-release; caught by the runtime smoke before first release).

## What the evidence shows

In-cluster diagnostics (commit `43dcf6d`), on the writer `smoke-mqttd-1`, repeated for ~40 s:

```
DIAG durable split: placement ring owns this group but the lease store refuses its epoch
  key="r/smoke/state" group=93 local=smoke-mqttd-1 ring_owner=smoke-mqttd-1
  voters=["smoke-mqttd-1"] members=["smoke-mqttd-1"] error=NotOwner
DIAG lease store refuses group epoch to its placement-ring owner
  group=93 local=4879074360594750294(smoke-mqttd-1)
  lease_holder=Some(15579416126498775515 = smoke-mqttd-2) lease_epoch=Some(427)
```

1. **The writer's placement is isolated.** `members = ["smoke-mqttd-1"]` — its SWIM gossip never
   incorporated its peers, so its eligible set (and therefore its derived voter set) collapsed to
   itself. Its HRW ring makes it the owner of every group.
2. **Consensus was healthy and disagreed.** The committed lease for group 93 is held by `smoke-mqttd-2`.
   `smoke-mqttd-1` *received* that committed lease (it is a raft voter) — so it simultaneously "knows"
   (via the lease log) that mqttd-2 owns group 93 and "believes" (via its HRW ring) that it owns it.
3. **The lease was flapping.** `lease_epoch = 427` in ~2 minutes: the leader's `LeaseAssigner` reassigned
   group 93 hundreds of times, consistent with cluster-wide membership churn feeding an unstable HRW.
4. **The raft mesh converged; gossip did not.** mqttd-1 exchanged raft `AppendEntries` with the founder
   (TCP :7001) but its SWIM membership (UDP :7946) never populated peers into placement.

## Root cause (assessed)

**Two independent defects; the second turns the first from a transient into permanent data loss.**

**Primary — SWIM advertises an unroutable gossip address, and the driver trusts it (CONFIRMED).** The
instrumented run (`mqtt_cluster::swim_driver=debug`) showed `smoke-mqttd-1` *sending* gossip (the
founder learned it and added it as a voter) but receiving **nothing** back — no membership events, no
`swim_driver` drops. The cause: `swim_driver::run` reads `(n, src)` from `recv_from` but **never used
`src`** — it learned a peer's gossip address purely from the peer's *self-claimed* `msg.from_addr`.
Under the chart's `[cluster.swim] bind = "0.0.0.0:7946"`, every node advertises `from_addr =
0.0.0.0:7946`. So joiners greet the founder at its routable **seed** address (the founder learns them,
and the raft/peer mesh forms over the separately-advertised routable `peer_advertise` FQDN), but every
gossip **reply and dissemination** targets `0.0.0.0:7946` and is black-holed. Only the founder — which
everyone greets directly — ends up with the full view; joiners stay isolated (`members=[self]`). This
is the **same bug class as the `peer_advertise` fix earlier in ADR 0047-T5, but for the SWIM gossip
address** — and unlike the TCP peer address (which genuinely cannot be derived from an inbound UDP
datagram), the gossip address *can*: it is where the datagram came from. **Fix:** the driver now sets
`msg.from_addr = src` before feeding the state machine, so a peer is always learned at its real
datagram source regardless of what it binds/advertises (standard SWIM practice). The lease-epoch
flapping (`epoch=427`) was a downstream symptom of the resulting membership churn and stops once gossip
converges. (The `cpu request 50m` was *not* implicated — gossip convergence is not fsync/CPU-bound.)

**Secondary — durable ownership follows gossip, not consensus.** Durable group ownership is computed
*twice from two independently-evolving snapshots*: the **desired** owner (placement HRW ring, a hash of
the gossip-derived node set) and the **actual** owner (the committed lease map, replicated by the lease
raft). The `LeaseAssigner` correctly drives actual→desired. But the **data path** (`Hub` retained
routing + `ClusterStore::owns_group`) reads the *desired* HRW ring, while the commit gate
(`LocalLeaseSource::epoch_for`) reads the *actual* lease. When they disagree — which any gossip skew can
cause — the write is routed to the ring's owner and refused by the lease, with no reconciliation. The
control-plane target (HRW) is being used as a data-plane routing oracle; it should not be. Worse, the
driver derives the durable voter set by *filtering the authoritative committed raft voters through the
gossip `members()`* (`durable_node.rs`), so gossip isolation actively corrupts durable ownership rather
than being ignored by it.

## Detection — and could a unit test have found this?

**Yes — the confirmed root cause is an integration test we simply never wrote; the design fragility it
exposed is separately unit-testable.** (Added now: `swim_cluster.rs::
nodes_that_advertise_an_unroutable_gossip_address_still_converge` — it fails on the pre-fix driver and
passes on the fixed one.)

- The **root cause is a driver-level integration property** exercisable with real UDP loopback (no
  cluster, no kind): spin up nodes that *advertise* an unroutable `0.0.0.0` gossip address but bind
  real sockets, and assert they still converge. The existing `three_nodes_converge_then_detect_failure`
  test came within one line of catching it but always advertised each node's **real** bound address —
  the single case the bug does not hit. The gap was a missing *adversarial address* in the harness, not
  a missing capability. This is the general lesson: convergence tests must advertise addresses that
  differ from the bind (NAT, `0.0.0.0`, containers), because that is what real deployments do.
- The **secondary design fragility** (durable ownership follows the gossip HRW ring instead of the
  committed lease) remains separately unit-testable and is tracked as follow-up #1 — it is what turned
  this gossip bug into *permanent* data loss rather than a transient, and would harden the plane against
  any future membership skew.

**Historical note (superseded).** The originally-suspected trigger — SWIM heartbeat starvation under the
`50m` CPU cap — was wrong; the instrumented run disproved it (the joiner sent fine and received nothing,
a routing problem, not a timing one).

- The **secondary defect is unit-testable and should have been caught there.** The invariant "a node
  whose gossip view is isolated must not compute itself the durable owner of a group the committed voter
  set assigns elsewhere" can be asserted in-process: build a `Placement`, feed it an isolated gossip
  view, feed the durable driver the real committed voter set, and assert durable ownership follows
  consensus. That test fails on today's code. It was never written because every existing durable unit/
  sim test constructs placement and lease views that are **mutually consistent by fiat** — the harness
  hands the same node set to both the router and the lease layer, so the two-oracle split cannot appear.
  The whole class of "router and lease-assigner disagree" was outside the test model.
- The **primary trigger is an integration/environment property a unit test cannot surface.** That real
  SWIM gossip *actually* fails to converge (or evicts peers) under kind's UDP + a 50m CPU cap is a
  property of the OS network, the scheduler, and resource limits. A pure unit test can *model* "gossip
  is isolated" as an input (and should — see above), but it cannot *discover* that the deployment will
  isolate gossip. Only a real cluster (the kind runtime smoke) or a fault-injecting simulation that
  models asymmetric/flapping gossip membership would surface it. This is exactly the gap the ADR 0047
  runtime smoke exists to close — and did.

Both prior durable `NotOwner` incidents (this and 2026-07-14) escaped the deterministic pure-core sim
(ADR 0042) and the out-of-process harness (ADR 0044) for the same reason: **both harnesses form a clean,
consistent membership before exercising durability.** The failures live in the *formation and membership-
skew* window, which neither harness perturbs.

## Follow-ups (proposed)

1. **Durable ownership must follow consensus, not gossip (bug, primary design fix).** The durable data
   path (routing, `owns_group`) must derive from the committed lease / raft voter set, not the gossip
   HRW ring; and the driver must not filter the committed voter set through gossip `members()`. Add the
   in-process regression test described above as the guard. *(Owns the secondary defect.)*
2. **Gossip convergence robustness (bug, primary trigger) — DONE.** Root cause found: `swim_driver`
   ignored the datagram source and trusted the peer's self-claimed `from_addr`, so a `0.0.0.0`-bind
   advertise black-holed all return gossip. Fixed by learning the source address; regression test added
   (`nodes_that_advertise_an_unroutable_gossip_address_still_converge`).
3. **Client-facing durability honesty (bug).** A QoS-1 retained publish that cannot durably commit must
   not have already returned `PUBACK`. Either withhold the ack until the durable commit (or a bounded
   queue-until-heal) succeeds, or the readiness gate must pull a node whose durable routing is
   inconsistent with consensus out of the client Service.
4. **Harness: model membership skew (test-harness gap — the motivating ask).** Add fault vocabulary to
   the durable sim (ADR 0042) and/or the out-of-process harness (ADR 0044) for *asymmetric gossip
   partition* and *placement-vs-lease membership skew* — inject an isolated/flapping gossip view while
   consensus stays healthy, and assert every durable write still commits to the committed owner. This is
   the general lever to "plough through the issues still hiding": the two durable `NotOwner` incidents
   both lived in the membership-formation/skew window that no current harness perturbs.
5. **Readiness blind spot (recurrence of 2026-07-14 follow-up 2).** `/readyz` reported ready on a node
   whose durable routing was inconsistent with consensus. Consider gating on "my placement voter set
   matches the committed raft voters" in addition to `lease_group_ready`.

## Evidence index

- Instrumented nightly run `29723412973` (commit `43dcf6d`), job "kubernetes runtime smoke (kind)":
  the two `DIAG` lines above, 40 occurrences each, `local=smoke-mqttd-1` throughout.
- raft-id map: `smoke-mqttd-0 = 672425597555301948`, `smoke-mqttd-1 = 4879074360594750294`,
  `smoke-mqttd-2 = 15579416126498775515`.
- Prior clean run `29707822254` confirmed formation is correct (raft `ReplaceAllVoters({0,1,2})`
  committed ~17 s after founder start) — the split is a post-formation membership-skew failure.
