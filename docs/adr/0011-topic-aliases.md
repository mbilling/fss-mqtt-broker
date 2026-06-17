# ADR 0011 — MQTT 5.0 topic aliases

- **Status:** Accepted (design); implementation phased (workstream G)
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Related:** [ADR 0008](0008-mqtt-5-codec.md) (the v5 wire that carries the
  properties), [ADR 0009](0009-mqtt5-expiry.md) (sibling v5-semantics work),
  [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md) workstream G

## Context

A topic alias lets a client and server replace a (potentially long) topic name
with a two-byte integer for the rest of a network connection, shrinking every
PUBLISH after the first on a given topic. It is governed by two MQTT 5.0
properties:

- **Topic Alias Maximum** (`0x22`, in CONNECT and CONNACK) — the highest alias the
  *sender of this property* is willing to **accept** from the other side. Absent or
  `0` means "do not send me aliases." The two directions are negotiated
  independently: the client's value (in CONNECT) bounds server→client aliases; the
  server's value (in CONNACK) bounds client→server aliases.
- **Topic Alias** (`0x23`, a PUBLISH property) — the alias in use on that PUBLISH.

The mapping is **per network connection**, not per session: it is established
fresh on each connection and never persisted, replicated, or shared across the
cluster. A PUBLISH that carries a non-empty topic name *and* an alias **sets** the
mapping; one with an empty topic name *and* an alias **references** an existing
mapping. An alias of `0`, an alias above the receiver's maximum, or a reference to
an unmapped alias is a protocol error.

This is therefore a purely connection-edge concern: the hub and the cluster only
ever see fully-resolved topic names.

## Decision

### 1. Keep aliases entirely in the connection layer; the hub never sees them

The alias maps live in two small per-connection structures (`mqttd::aliases`).
Inbound PUBLISHes are resolved to a full topic name *before* anything reaches the
hub; outbound PUBLISHes are rewritten *after* they leave the hub, in the
connection's writer path. Routing, persistence, retained storage, offline queues,
and cross-node forwarding all continue to operate on full topic names only —
nothing about aliases leaks past the socket.

### 2. Inbound (client → server): advertise a maximum, resolve, validate

For a v5 connection the CONNACK advertises a Topic Alias Maximum of
`SERVER_TOPIC_ALIAS_MAX` (a fixed small constant). For each inbound PUBLISH:

- no alias → use the topic name unchanged;
- alias set with a non-empty topic → record `alias → topic` and use the topic;
- alias set with an empty topic → look the alias up and use the mapped topic;
- alias `0`, alias `> max`, or an unmapped reference → **protocol error**: log and
  close the connection (consistent with how the wildcard-topic violation is
  handled today; emitting a DISCONNECT with reason `0x94` is folded into the
  later "act on v5 reason codes" work).

v3.1.1 connections advertise no maximum and never carry the property, so the
inbound resolver is simply created with `max = 0` and is inert.

### 3. Outbound (server → client): assign up to the client's maximum, never evict

If the client advertised a non-zero Topic Alias Maximum, the writer path may alias
the PUBLISHes it sends. The policy is **assign-until-full, no eviction**:

- a topic already mapped → send an empty topic name + its alias;
- an unmapped topic with a free slot (`next ≤ max`) → assign the next alias, send
  the full topic name + the alias (establishing it);
- an unmapped topic with the table full → send the full topic name, no alias.

No eviction keeps the rewrite O(1) and the map bounded by the client's maximum; the
cost is that long-tail topics past the cap never get aliased. An LRU policy is a
possible later refinement. Using aliases at all is optional per spec, so this is
always safe.

### 4. State is per-connection and dies with it

Both maps are owned by the connection task and dropped when it ends. A takeover or
reconnect starts from an empty map, exactly as the spec requires. Nothing is
written to the session store or gossiped.

## Consequences

- **Good:** real wire savings on repeated topics in both directions; zero impact on
  routing/storage/cluster code (they stay alias-free); correct per-connection reset
  semantics for free; pure, unit-testable map logic.
- **Cost / limits:** outbound assignment never evicts, so topic sets larger than the
  client's maximum are partially un-aliased (§3); an invalid alias closes the
  connection rather than sending DISCONNECT `0x94` (§2), pending the reason-code
  work; the server's advertised maximum is a fixed constant, not yet configurable.

## Alternatives considered

- **Resolve aliases in the hub.** Rejected: aliases are per-connection and the hub
  is shared across connections and nodes; it would have to track a map per
  connection and unwind it before routing — strictly worse than doing it at the edge.
- **Outbound LRU eviction.** Deferred: more state and per-PUBLISH bookkeeping for a
  marginal gain over assign-until-full; revisit if profiling shows it matters.
- **Don't implement outbound aliasing.** Rejected: the broker fans the same topics
  out repeatedly, so the server→client direction is where most of the savings are.
