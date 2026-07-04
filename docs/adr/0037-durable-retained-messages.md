# ADR 0037 — Durable single-owner retained messages (clock-free convergence)

- **Status:** Accepted
- **Date:** 2026-07-03 (accepted 2026-07-04 — all phases delivered)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0037-durable-retained-messages.md](../delivery/0037-durable-retained-messages.md) — plan, progress, and changelog
- **Related:** [ADR 0014](0014-cross-node-retained.md) (the best-effort broadcast model this
  revises — its §3 gap-fill and the 0014-T7 divergence question are the motivation),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (the lease-owner + group-log machinery this reuses), [ADR 0021](0021-bounded-lease-voters.md)
  (bounded voters those groups run on), [ADR 0018](0018-on-disk-persistence.md) (the durable
  log that makes retained state restart-safe), [ADR 0023](0023-gossip-anti-replay.md) (the
  clock-free stance this preserves), [ADR 0029](0029-durable-by-default.md) (durable is the
  default, so this is the default retained behaviour)

> This record states the decision only. How it is being built and how far along it is live in
> the [delivery doc](../delivery/0037-durable-retained-messages.md).

## Context

ADR 0014 replicates retained messages **best-effort**: a retained publish fans out to every
peer, each node applies whatever arrives, and link-up back-fill is deliberately
**gap-fill-only** (a topic you already hold is never overwritten). That model has two
divergence classes with no repair path:

1. **Partition heal** (the recorded 0014-T7 gap): both sides retain different values for the
   same topic during a partition; after the heal, gap-fill keeps each side's own value —
   **permanently**, until some client happens to republish that topic. A subscriber sees
   device state X or Y depending on which node it lands on.
2. **The everyday race**: two concurrent retained publishes to the same topic on different
   nodes cross-forward, and each node applies the *other's* value last — divergence with no
   partition at all.

Both are invisible today (nothing detects them) and both violate the single-logical-broker
expectation that is the whole point of clustering retained messages — the last-value-cache
use (device shadows, status topics) is exactly where two confident, contradictory answers
hurt most.

Resolving divergence **after the fact** requires deciding which write was "last" across
concurrent writers. Every such rule (last-write-wins on wall-clock, or bounded by hybrid
logical clocks) puts **clocks into the trust base** — a fast-clocked node wins retroactively
— and **silently discards an acknowledged publish**. This codebase has deliberately kept
wall-clock out of correctness (ADR 0023's anti-replay is clock-free for exactly this
reason). We choose to keep it that way.

## Decision

**Prevent retained conflicts from forming instead of resolving them: route every retained
write through the topic's placement-group lease-owner and commit it in the group's
quorum-replicated log. Convergence tokens are consensus-issued (lease epoch + log offset) —
no wall-clock anywhere. Every node keeps a local retained cache, warmed by commit fan-out
and healed by an offset-aware digest, so subscribe-time replay stays a local read.**

This deliberately **revises ADR 0014's "rejected as the default"** verdict on durable-plane
retained: that judgment predates the T7 divergence analysis, the everyday-race finding, and
the no-clock resolution decision. What stands from 0014 is the *read* model (local replay,
broadcast-warmed) and its back-fill machinery (digest + chunking, 0014-T6/T8), which this
ADR re-bases on committed values rather than raw gossip.

### 1. Authority: the group lease-owner commits retained writes

`group_of(topic)` (the existing placement hash) assigns each topic to one of the 256
placement groups; the group's **lease-holder** (ADR 0007, epoch-fenced, bounded voters per
ADR 0021) is the single writer for that topic's retained state. A retained publish landing
on any node routes the *retained set/clear* to the owner through the same group-routed path
session ops use; the owner appends it to the group log under a retained key
(`ret/<topic>`), quorum-commits it, and compacts the key to its last value (a zero-length
clear is a committed tombstone, versioned like any value). Fencing, takeover, and
restart-recovery are inherited verbatim from the session store — no new consensus code.

The **live delivery** of the publish (to current subscribers, ADR 0014 §1/§2) is unchanged
and stays best-effort/immediate; only the *retained state mutation* gains an authority.

### 2. The convergence token: (lease epoch, log offset)

Every committed retained value carries the pair `(epoch, offset)` from its commit — a total
order per topic issued by consensus, not by any clock. All cache/back-fill decisions below
reduce to "higher `(epoch, offset)` wins", which is deterministic on every node and
tolerates owner changes (a new owner's higher epoch supersedes).

### 3. Node-local caches, warmed by commit fan-out

Subscribe-time replay must stay a **local read** (ADR 0014 rightly rejected
fetch-on-subscribe). Every node keeps its retained store (in-memory or redb, as today) as a
**cache of committed values**: after commit, the owner fans the update out to all peers —
the ADR 0014 broadcast, now carrying the token and sent *post-commit* — and each cache
applies it only if the token exceeds what it holds for that topic (monotonic, idempotent,
order-insensitive).

### 4. Heal and join: the digest becomes offset-aware

Link-up back-fill keeps the 0014-T6/T8 shape (digest offer → pull → chunked snapshot) with
one change: entries carry the token, and the receiver takes the **higher-token value per
topic** instead of gap-filling only missing topics. Divergent caches therefore converge
deterministically on heal; the authoritative copy in the group log makes the outcome
identical no matter which pairs of nodes exchange first.

### 5. Partition semantics: queue-until-heal (CP, bounded)

Single-owner is a CP choice: on the ownerless/minority side of a partition a retained write
cannot commit. The publish itself is still delivered live to local subscribers; its
**retained mutation is queued** (bounded per node; oldest dropped with a loud counter when
the bound is hit) and submitted to the owner on heal, where it commits in arrival order.
The trade, explicitly: today a partition costs retained **consistency** (permanent
divergence); under this ADR it costs retained **freshness** on the minority side (staleness
that self-heals). Tentative local application — instant visibility at the price of
reintroducing divergence — is rejected.

### 6. Durable-off fallback

With durable sessions explicitly opted out (`MQTTD_DURABLE_SESSIONS=0`, ADR 0029) there is
no lease plane; retained falls back to ADR 0014 best-effort behaviour unchanged, and the
divergence caveat applies. Durable is the default, so the default gets convergence.

### 7. Detection first — the migration's measuring stick

Phase 1 lands **divergence detection** independently of the migration: the link-up digest
gains a value hash, and a divergence between peers is surfaced as a `warn!` plus a
`retained_divergence_total` metric. This gives a baseline (how often divergence really
happens), a regression alarm, and — after the migration — the proof that convergence holds
(the counter stops moving).

## Consequences

- **Good:** retained conflicts cannot form; a healed partition converges deterministically;
  no wall-clock in correctness (epochs/offsets only); no acknowledged write is *silently*
  discarded (a queued minority write either commits on heal or is dropped **loudly** at the
  bound); restart recovery and owner takeover are inherited from the battle-tested session
  machinery; parity with mature clustered brokers on retained convergence.
- **Cost:** a retained mutation now waits for a quorum commit (retained publishes are
  infrequent; live delivery is not delayed); the group log gains a retained keyspace with
  last-value compaction; caches add token bookkeeping; the 0014-T6 digest is extended.
- **Trade-off accepted:** minority-side retained **staleness during partitions** (bounded
  queue, self-healing) in exchange for never diverging — the right trade for
  device-state/last-value use.
- **Risk:** this touches the durable plane's scope and the retained path end to end. It is
  built strictly test-first on the existing pure cores, phased so each step lands green, and
  the detection metric (§7) guards the cutover.

## Alternatives considered

- **Last-write-wins on wall-clock (or HLC) timestamps.** Resolves after the fact; puts
  clocks into the trust base (a fast clock wins retroactively; HLC bounds but does not
  remove this and adds machinery) and silently discards acknowledged writes. Rejected —
  and with it the whole resolve-later family, since retained values admit no semantic merge.
- **Owner orders, gossip stores (no durable log).** The owner stamps per-topic sequences but
  storage stays gossip-replicated. Lighter on paper, but owner failover then needs its own
  high-water recovery and epoch fencing — re-implementing precisely the parts of the durable
  plane that are hard, without its tests. Rejected.
- **One global retained owner.** A single node serializes all retained writes: a throughput
  bottleneck and a failover pinch-point, strictly worse than the per-group ownership
  placement already provides. Rejected.
- **Keep ADR 0014 as-is (accept divergence).** Fails the single-logical-broker expectation
  for exactly the state retained messages exist to cache; divergence is silent and
  unbounded in time. Rejected as the default; retained under explicit durable-off keeps it
  (§6).
- **Fetch retained from the owner on subscribe.** Re-rejected for the same latency reasons
  as ADR 0014: it taxes the common subscribe path to serve the rare heal.
