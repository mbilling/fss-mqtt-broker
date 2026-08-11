# ADR 0059 — Bridge HA topology and message ordering

- **Status:** Proposed
- **Date:** 2026-08-11
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0059-bridge-ha-topology-and-ordering.md](../delivery/0059-bridge-ha-topology-and-ordering.md) — plan, progress, and changelog
- **Amends:** [ADR 0025 §5](0025-boundary-bridge.md) (HA via cluster-side shared subscriptions), whose model this record found is correct only for one direction.
- **Related:** [ADR 0010](0010-shared-subscriptions.md) / [ADR 0015](0015-cluster-shared-subscriptions.md) (the `$share` mechanism 0025 §5 relied on), [ADR 0025 §7](0025-boundary-bridge.md) (the store-and-forward whose replay interacts with ordering).

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0059-bridge-ha-topology-and-ordering.md).

## Context

ADR 0025 §5 gives the bridge high availability by running ≥2 instances behind a **cluster-side
shared subscription** (`$share/<group>/<filter>`): the broker load-balances one copy of each
message across the instances, so no duplicate is forwarded and a brief restart loses nothing.
A code audit found that model is sound **only for `out` rules**, and that it silently forfeits
per-topic ordering. Two gaps, one root cause.

### Gap 1 — HA only de-duplicates outbound

The `$share` wrap is applied to the **subscribing side**, and for an `out` rule that side is
our own cluster, which supports `$share`. For an **`in` rule the subscribing side is the
foreign broker**. The implementation wraps only the local subscription (`forward.rs`
`local_subscriptions`) and issues the upstream subscription with a **bare** filter
(`upstream_subscriptions`); `share_group` is never consulted on the upstream path. So:

- Two instances, any `in`/`both` rule ⇒ **every inbound message is delivered into the local
  cluster twice**. There is no message-level de-duplication anywhere in the bridge.
- Even where the foreign broker *does* support `$share`, the bridge cannot use it — the
  upstream subscription is hard-wired to the bare filter.
- Many cloud platforms and older brokers (e.g. AWS IoT Core) do not offer `$share` at all, so
  "just wrap the upstream too" is not a general answer.

This violates ADR 0025's own Consequences bar: "reconnect/spool must not lose or **duplicate**
beyond the QoS contract."

### Gap 2 — per-topic ordering is silently forfeited

`$share` load-balances **per message, not per topic key**. Two messages on the same topic can
be handled by different instances and arrive at the destination out of order. This is inherent
to the §5 design and cannot be fixed inside it. Compounding paths: the spool replay/connect
race, and fire-and-forget QoS-1 forwards with multiple in-flight. ADR 0025 makes **no** ordering
claim, but "MQTT preserves order per topic per publisher" is exactly the assumption a migrating
Mosquitto/EMQX user carries over — an unstated regression.

Both gaps share a root: `$share` per-message load-balancing is the wrong primitive for
multi-instance bridging when you need inbound dedup **or** ordering.

## Decision

Replace per-message shared-subscription load-balancing with **hash-partitioned rule ownership**
across instances, for every direction, as the bridge's HA model. Keep `$share` only as an
optional optimisation on the local (`out`) side where the cluster supports it and ordering is
not required.

### 1. Deterministic partition of the key space

Each running bridge instance is assigned an **instance index** `k` of `N` (from config or the
orchestrator: `MQTTD_BRIDGE_INSTANCE`/`_TOTAL`, or a StatefulSet ordinal). For every rule and
every concrete topic, the instance computes `owner = hash(partition_key) mod N` and **forwards
only the topics it owns**. `partition_key` defaults to the **topic name** (per-topic ordering)
and is configurable to a topic-level or a shared-key-extractor for cases where a coarser or
finer grain is wanted.

- Each instance **subscribes to the full filter** on both sides (so every instance can serve
  after a peer dies) but **drops at the forward step** any topic it does not own. Ownership is
  pure and local — no coordination, no election, no shared state (the boundary design forbids
  shared state, ADR 0025 §1).
- Because a given topic is owned by exactly one instance, inbound is delivered **once** and
  **per-topic order is preserved** (one owner, one ordered stream) — the same guarantee for
  `in`, `out`, and `both`. This is the single move that closes both gaps.

### 2. Failover

When an instance dies, the topics it owned are re-derived by the survivors only after `N`
is reconfigured (scale event) — i.e. this is **static partitioning with operator-driven
rebalance**, not automatic. A dead instance's topics are not served until `N` drops or a
replacement with the same index returns. This is the deliberate CP-side trade: no invented
election (ADR 0025 §1), and each side's **persistent session** buffers during the gap. An
optional **active/passive** mode (one owner, others hot-standby taking over the whole key
space on liveness loss) is offered for small fleets that prefer availability over the
no-coordination property; it requires a liveness signal and is documented as such.

### 3. `$share` becomes an opt-in local optimisation

For `out` rules where the cluster supports `$share` and per-topic ordering is **not** required,
an instance may still use `$share` on the local subscription to spread load without partitioning.
This is now an explicit per-rule/global option (`ha = "partitioned" | "shared" | "active-passive"`),
defaulting to **`partitioned`** so the safe behaviour (dedup + ordering, all directions) is the
default and the lossy-ordering optimisation is a deliberate choice.

### 4. Ordering, stated plainly

With `partitioned` HA and `partition_key = topic`, per-topic-per-publisher order is preserved
end to end **except** across a spool replay boundary (a reconnect re-injects spooled messages
ahead of nothing, but a message that raced the connect flip can land after later live traffic —
a bounded, documented window, tracked with the durability work in ADR 0060). Under `shared` HA,
ordering is explicitly **not** preserved. Both are stated in the bridge docs and the ADR 0025
Consequences amendment.

## Consequences

- **Good:** inbound HA no longer duplicates; per-topic ordering holds for all directions under
  the default; no dependence on the foreign broker supporting `$share`; no invented election or
  shared state.
- **Cost:** ownership is static — a dead instance's topics wait for an operator rebalance (or
  active/passive mode); every instance subscribes to the full stream and discards non-owned
  topics (bandwidth, not correctness). `partition_key` is a new config surface.
- **Risk:** built **test-first** to ADR 0025's adversarial bar. The defining tests: 2 instances
  + an `in` rule deliver each message **exactly once** (red today: twice); a single publisher's
  messages on one topic arrive **in order** under load; a scale event rebalances ownership with
  no gap beyond the persistent-session window.

## Alternatives considered

- **Wrap the upstream subscription in `$share` too.** Fails for the common brokers that do not
  support `$share` (AWS IoT Core, older/embedded brokers), and still forfeits per-topic order
  (per-message balancing). A non-starter as the general model.
- **Cross-instance de-duplication (shared idempotency store).** Would let all instances forward
  and drop duplicates by message id, but it reintroduces exactly the shared state the boundary
  design removes (ADR 0025 §1), needs a durable dedup store on the DMZ host, and still does not
  give ordering. Rejected.
- **Automatic ownership election among instances.** Reintroduces a coordinator/election into a
  component explicitly designed to have none (ADR 0025 §1, which rejected in-broker owner
  election for the same reason). Static partitioning + optional active/passive covers the need
  without it.
- **Do nothing, document the limitation.** Leaves a silent double-delivery and ordering
  regression in a security-crossing component whose ADR promised no duplication. Rejected —
  at minimum the default must be safe.
