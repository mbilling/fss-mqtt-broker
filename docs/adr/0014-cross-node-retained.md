# ADR 0014 — Cross-node retained-message replication

- **Status:** Accepted
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0014-cross-node-retained.md](../delivery/0014-cross-node-retained.md) — plan, progress, and changelog
- **Related:** [ADR 0001](0001-session-durability.md) (cluster routing model),
  [ADR 0010](0010-shared-subscriptions.md) (the other cross-node routing limitation)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0014-cross-node-retained.md).

## Context

Each node keeps its own `RetainedStore`. A retained PUBLISH was stored only on the
node that received it, and the cross-node forward (`PeerMessage::Publish`) dropped
the retain flag — `RemotePublish` delivered live but never stored. So a client that
published a retained message on node A and a client that later subscribed on node B
got nothing: retained state was effectively node-local. The integration suite
(`cluster_chaos.rs`) pinned this as a gap.

This breaks a basic MQTT expectation in a cluster: retained messages are supposed to
be the topic's last-known value for *any* future subscriber, independent of which
node they land on.

The forwarding mechanics make this fixable cheaply. The only subtlety is *when* a
node needs the retained state: a normal (non-retained) publish is forwarded only to
peers with matching interest (live delivery), but a retained message must reach a
node even if it has **no** current interest, because a subscriber may arrive there
later.

## Decision

### 1. Retained publishes are forwarded to every peer; non-retained stays interest-filtered

`forward_to_peers` now iterates the connected peers. A non-retained message goes
only to peers whose announced interest matches the topic (unchanged — efficient live
delivery). A **retained** message goes to *every* connected peer regardless of
interest, so each node can store it for its own future subscribers.

### 2. A received retained publish updates the receiving node's store

`HubCommand::RemotePublish` carries the `retain` flag, and the handler applies the
message through the same `deliver` path as a local publish — storing/clearing
retained state **and** delivering to local subscribers — but never re-forwards
(no relay loop). A zero-length retained payload clears the entry on every node, the
same as locally [MQTT-3.3.1-10].

This keeps one code path (`deliver`) for "apply a message on this node"; `publish`
is just `deliver` + `forward_to_peers`.

### 3. Replication is at publish time, plus back-fill on join

A retained message replicates to the nodes that are **peers at publish time**.
Propagation is asynchronous (it rides the peer link), so a subscriber on another
node sees it *eventually*, not synchronously — the test re-subscribes until it
arrives.

A node that joins the cluster **after** a retained message was published is
**back-filled on link establishment**: a node sends its full retained set
(`PeerMessage::RetainedSnapshot`, via `RetainedStore::all`) to each new peer, and the
receiver applies it **gap-fill** — it sets a retained message only for a topic it
does not already hold, so a peer's snapshot never clobbers our own (possibly newer)
value. A fresh joiner therefore catches up on the whole existing retained set; an
established node ignores topics it already has. This is a full-snapshot exchange, not
a digest diff — simpler, and correct for the join case; a digest-based diff (to avoid
re-sending the whole set on every link-up) is a possible later optimization.

**Conflict on partition heal** (two nodes holding *different* values for the same
topic) is left unresolved: gap-fill keeps each side's own value, so they stay
divergent until the next publish. Resolving that needs per-message timestamps /
version vectors and is out of scope. Snapshot size is bounded by the peer frame
limit; chunking a very large retained set is deferred.

## Consequences

- **Good:** retained messages behave cluster-wide — both for members present at
  publish time and for nodes that join later (back-filled on link-up, §3); one
  `deliver` path for local and remote application; clears propagate; no relay loops;
  non-retained forwarding is unchanged.
- **Cost / limits:** every retained publish fans out to all peers (O(nodes); retained
  publishes are typically infrequent); back-fill re-sends the full retained set on
  each link-up (a digest diff is a later optimization, §3); partition-heal divergence
  on the same topic is not reconciled (§3); cross-node delivery still carries no
  message-expiry deadline (the peer link does not yet carry the interval — pre-existing
  carried limitation).

## Alternatives considered

- **Fetch retained from peers on subscribe.** Rejected: every new subscription would
  trigger a cross-node query and wait, adding latency to the common case to serve the
  rare late-join; broadcasting on publish keeps subscribe local and fast.
- **Replicate the retained store via the durable plane (consensus).** Rejected as the
  default: retained messages do not need linearizable consensus, and routing every
  retained publish through raft would be far heavier than a best-effort broadcast.
  The durable plane remains the right home for the *back-fill digest* (§3) when that
  lands.
- **Leave it node-local.** Rejected: it silently violates retained semantics in a
  cluster, which is the whole point of the feature.
