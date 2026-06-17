# ADR 0014 — Cross-node retained-message replication

- **Status:** Accepted
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Related:** [ADR 0001](0001-session-durability.md) (cluster routing model),
  [ADR 0010](0010-shared-subscriptions.md) (the other cross-node routing limitation),
  [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md)

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

### 3. Replication is at publish time, to current members (eventual, not back-filled)

A retained message replicates to the nodes that are **peers at publish time**.
Propagation is asynchronous (it rides the peer link), so a subscriber on another
node sees it *eventually*, not synchronously — the test re-subscribes until it
arrives.

A node that joins the cluster **after** a retained message was published does **not**
receive the existing retained state: there is no anti-entropy / on-join sync of the
retained store. Back-filling a late-joining node needs a retained-state digest
exchanged on link establishment (the same shape as the durable-session anti-entropy
work, ADR 0006/0007) and is deferred. For steady-state clusters and any node present
when the message is published, retained now works cluster-wide.

## Consequences

- **Good:** retained messages behave cluster-wide for the common case (members
  present at publish time); one `deliver` path for local and remote application;
  clears propagate; no relay loops; non-retained forwarding is unchanged.
- **Cost / limits:** every retained publish fans out to all peers (O(nodes); retained
  publishes are typically infrequent); a node joining after a publish misses the
  existing retained state until that topic is re-published (no on-join back-fill —
  see §3); cross-node delivery still carries no message-expiry deadline (the peer
  link does not yet carry the interval — pre-existing carried limitation).

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
