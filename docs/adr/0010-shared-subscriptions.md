# ADR 0010 — Shared subscriptions

- **Status:** Accepted (design); implementation phased (workstream G)
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Related:** [ADR 0001](0001-session-durability.md) (session/queue lifecycle),
  [ADR 0005](0005-session-affinity.md) (cross-node placement),
  [ADR 0008](0008-mqtt-5-codec.md) (the v5 wire),
  [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md) workstream G

## Context

A normal subscription fans a matching message out to **every** subscriber. A
**shared subscription** is the in-protocol lever for *consumer* scale (Capability
Plan §4): a named group of sessions where each matching message is delivered to
**exactly one** member, load-balancing a topic's traffic across a worker pool.

The wire form is a topic filter of the shape `$share/{ShareName}/{filter}`:

- `{ShareName}` is an opaque group label — it must be non-empty and contain no
  `/`, `+`, or `#`.
- `{filter}` is an ordinary topic filter (wildcards allowed) and is what actually
  matches published topics.
- Two sessions that subscribe with the **same** `(ShareName, filter)` pair join the
  same group; a message matching `{filter}` goes to one member of that group.
- Groups are independent: a message matching the filter is delivered once **per
  group** plus to every ordinary (non-shared) subscriber whose filter matches.

Shared subscriptions are an MQTT 5.0 feature, but the `$share/...` form is just a
topic filter on the wire; we accept it under both protocol versions (matching common
broker behaviour) since the routing machinery is version-agnostic.

The questions this fixes: where group state and the load-balancing selection live,
how selection interacts with online/offline sessions and QoS, what happens to
retained messages, and how shared delivery behaves across a cluster.

## Decision

### 1. A dedicated `SharedSubscriptionTable` in `mqtt-core`, beside the plain table

Plain subscriptions keep their existing `SubscriptionTable` (filter → set of
clients, fan-out-to-all). Shared subscriptions get a separate, pure structure keyed
by `(ShareName, filter)`. Each group holds its members **with their granted QoS** in
insertion order plus a **round-robin cursor**. Keeping it separate means the plain
fast path is untouched, and the group/rotation state has one clear owner.

Parsing is a pure function `parse_shared(filter) -> Option<(group, filter)>` that
validates the `ShareName` (non-empty, no `/ + #`) and a non-empty remaining filter;
a malformed `$share/...` filter is **rejected** at SUBSCRIBE with reason `0x80`
rather than silently treated as a literal topic.

### 2. The table rotates; the hub selects

Selection policy needs to know which members are online — state the pure core must
not hold. So responsibility splits:

- The table answers `rotations(topic)`: for each group whose `{filter}` matches the
  topic, it advances that group's cursor by one and returns the group's members in
  **rotated order** (the newly-selected member first). Round-robin lives here.
- The hub picks, from each rotation, the first member that can actually receive: an
  **online** member if any, else a **persistent offline** member (so the message is
  queued for it on reconnect), else none (the group has no reachable member; the
  message is dropped for that group). Online-preference lives here.

This yields fair round-robin across the group while never black-holing a message to
an offline session when an online one is available.

### 3. Retained messages are not replayed to shared subscriptions

On a new ordinary subscription the broker replays matching retained messages
[MQTT-3.3.1-6]. A new shared subscription does **not** receive retained messages
[MQTT-3.8.4]: retained replay would hit every joining member and defeat the
single-delivery contract. Shared subscribe skips the retained path entirely.

### 4. QoS, persistence, and lifecycle reuse the session machinery

The granted QoS per member is stored on the group entry; downstream delivery is
`min(publish QoS, granted QoS)` as for ordinary subscriptions, with QoS > 0 tracked
in the same in-flight table. A persistent session's shared memberships are persisted
(the full `$share/...` filter string in the existing `Subscription` record) and
reconstructed on reconnect/restart. Clean Start, zero-expiry disconnect, and the
expiry sweep tear down a client's shared memberships alongside its plain ones.

### 5. Cluster: per-node single delivery (~~carried limitation~~ — superseded)

> **Superseded by [ADR 0015](0015-cluster-shared-subscriptions.md).** Shared
> subscriptions now deliver **once cluster-wide**: the publishing node selects one
> member across gossiped global membership and targets it directly. The original
> per-node behaviour is described below for history.

A node gossips a shared group's underlying `{filter}` as ordinary interest, so a peer
forwards matching publishes to it, and the receiving node delivers to one of **its
local** group members. With members spread across N nodes this means up to **one
delivery per node that has a member**, not one cluster-wide.

## Consequences

- **Good:** consumer-pool scale-out within a node; fair round-robin with
  online-preference; no retained-message storm on join; reuses QoS/persistence/expiry
  unchanged; pure, unit-testable group logic in `mqtt-core`.
- **Cost / limits:** cross-node delivery is per-node, not cluster-wide (§5); `rotations`
  clones the matching groups' member lists per publish (small in practice; an indexed
  selection is a later optimization); no Subscription-Identifier handling yet.

## Alternatives considered

- **Fold shared groups into `SubscriptionTable`.** Rejected: round-robin cursor state
  and single-pick selection don't fit the fan-out-to-all `matching_clients` contract,
  and would slow the plain path.
- **Select inside the core table.** Rejected: correct online-preference needs live
  session state the pure core deliberately doesn't hold; injecting it as a predicate
  is clumsier than returning a rotation and letting the hub pick.
- **Cluster-wide single delivery now.** Deferred: needs cross-node coordination that
  belongs with the placement/ownership work, not the routing-layer lever.
