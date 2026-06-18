# ADR 0015 — Cluster-wide shared subscriptions

- **Status:** Accepted
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Supersedes:** the cluster limitation in [ADR 0010](0010-shared-subscriptions.md) §5
- **Related:** [ADR 0001](0001-session-durability.md) (cluster routing),
  [ADR 0014](0014-cross-node-retained.md) (the other cross-node fix),
  [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md)

## Context

ADR 0010 implemented shared subscriptions but, in a cluster, delivered a matching
message to one member on **every** node that had a group member — up to one delivery
*per node*, not the single cluster-wide delivery the spec intends. The cause: each
node selected independently from its *local* members, because the only thing
gossiped about a shared group was its underlying `{filter}` (as ordinary interest),
and a forwarded publish ran each receiving node's own local shared selection.

ADR 0010 §5 deferred the fix as needing "a designated-node-per-group or a cross-node
claim protocol." This ADR delivers it.

## Decision

### 1. The originating node selects one member globally and targets it

Selection moves to the node that **receives the publish from a client**. That node
holds the global membership of each shared group (its own members plus members
gossiped from peers, §2), runs the round-robin over the *whole* group, and:

- if the chosen member is **local**, delivers to it directly;
- if the chosen member is on a **peer**, sends a targeted
  `PeerMessage::SharedDeliver { client, topic, payload, qos }` to that peer, which
  delivers to exactly that named client.

Because exactly one node (the publisher's) selects for any given publish, there is no
double-delivery and no cross-node consensus on a cursor: the round-robin cursor lives
on the selecting node, per `(ShareName, filter)`. Per-publisher-node fairness is
sufficient — the spec leaves member selection implementation-defined.

### 2. Shared-group membership is gossiped with node + client attribution

A node gossips its shared groups as `PeerMessage::SharedInterest` — a full snapshot of
`(ShareName, filter, [(client, granted QoS)])` — on the same triggers as the ordinary
interest snapshot (subscribe/unsubscribe/detach, and on peer link-up). Each node keeps
the latest snapshot per peer; a dead/closed link drops it. The global member list for a
group is the local members followed by each peer's members (peers in node-id order, for
a stable cursor).

Shared filters are **no longer** folded into the ordinary interest snapshot: ordinary
forwarding (`forward_to_peers`) is for non-shared subscribers only, and shared delivery
rides exclusively on the targeted `SharedDeliver` path. This also removes the wasteful
ordinary-forward to nodes that had only shared members.

### 3. Delivery paths are cleanly separated

- **Ordinary**: local fan-out + interest-filtered forward to peers; a received
  `RemotePublish` delivers to ordinary subscribers only (it no longer runs shared
  selection — that was the double-delivery).
- **Shared**: the originating node's global selection (§1), delivering locally or via
  `SharedDeliver`. A received `SharedDeliver` delivers to one named client (online →
  send, persistent-offline → queue), bypassing selection.

A single `deliver_to_client` helper implements the online-or-queue logic for one named
recipient and backs both an ordinary target and a shared target.

### 4. Selection policy preserves the single-node semantics

Round-robin over the global list, preferring a member that can receive immediately — a
**local online** member or **any remote** member (the peer delivers or queues) — and
falling back to a **local persistent-offline** member (queued locally) before giving
up. With no peers this is exactly the ADR 0010 online-preferring local round-robin, so
single-node behaviour is unchanged.

## Consequences

- **Good:** a shared publish is delivered exactly once cluster-wide; no double-delivery;
  no consensus needed (the publisher's node decides); single-node behaviour and the
  QoS / retained-skip / offline-queue rules from ADR 0010 are preserved; ordinary
  forwarding is now strictly for ordinary subscribers.
- **Cost / limits:** membership is eventually consistent (a just-joined member may miss
  a publish until its `SharedInterest` propagates); round-robin fairness is per
  publishing node, not a single global sequence; cross-node `SharedDeliver` carries no
  message-expiry deadline (same carried limitation as `RemotePublish`); remote member
  liveness is not known to the selector, so it may target a member that is offline on
  its home node (which then queues it) even when a local member is online — an
  acceptable, spec-permitted selection quality trade-off.

## Alternatives considered

- **Designated owner node per group** (route all of a group's traffic through one node
  that selects). Rejected as heavier: it adds a routing hop for every shared publish and
  couples shared delivery to the placement ring; originator-selects needs only
  membership gossip already analogous to interest gossip.
- **Cross-node claim/lock per message.** Rejected: per-message coordination latency for
  no real gain over letting the single originating node decide.
- **Leave it per-node (ADR 0010 §5).** Rejected: it violates the single-delivery
  contract the feature exists to provide.
