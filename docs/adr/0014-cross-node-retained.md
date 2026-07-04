# ADR 0014 — Cross-node retained-message replication

- **Status:** Accepted
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0014-cross-node-retained.md](../delivery/0014-cross-node-retained.md) — plan, progress, and changelog
- **Related:** [ADR 0001](0001-session-durability.md) (cluster routing model),
  [ADR 0010](0010-shared-subscriptions.md) (the other cross-node routing limitation)
- **Revised by:** [ADR 0037](0037-durable-retained-messages.md) — under durable
  sessions (the default) retained *writes* commit through the group lease-owner and
  caches converge by token; this ADR's read model and back-fill machinery stand, and
  its full behaviour remains the explicit durable-off fallback (see the revision
  notes at the end)

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
**back-filled on link establishment**, in two steps (as revised by T6/T8 — the
original design sent the full set unconditionally in one frame):

1. **Digest offer (T6).** On link-up each side sends an order-independent digest of
   its retained *topic set* (`PeerMessage::RetainedDigest`: topic count + XOR of each
   topic's stable 64-bit hash). If the receiver's own digest matches, the sets are
   identical and **nothing further is transferred** — the common steady-state link-up
   (or link flap) costs one small frame instead of the whole retained set. Topics
   only: under the gap-fill rule below a receiver can only ever accept topics it
   lacks, so payload digests would add no information. If the digests differ, the
   receiver pulls with `PeerMessage::RetainedRequest`.
2. **Chunked snapshot (T8).** The pulled set is sent as **bounded chunks** (each well
   under the peer frame limit), because chunks are independent and idempotent under
   gap-fill — no ordering or completion marker is needed. One unbounded frame was a
   latent outage: a retained set beyond the 16 MiB frame limit would be rejected by
   the *receiver*, tearing down the link, and the link-up back-fill would then re-kill
   it on every reconnect — a permanent, data-volume-triggered link flap severing all
   peer traffic. The frame bound is now also enforced at *encode* (sender side), and
   the peer write loop drops an oversized frame with a warning rather than dying; a
   single retained message that could never fit a frame is skipped with a warning,
   not sent.

The receiver applies a snapshot **gap-fill** — it sets a retained message only for a
topic it does not already hold, so a peer's snapshot never clobbers our own (possibly
newer) value. A fresh joiner therefore catches up on the whole existing retained set;
an established node ignores topics it already has.

**Conflict on partition heal** (two nodes holding *different* values for the same
topic) was originally left unresolved: gap-fill kept each side's own value, so they
stayed divergent until the next publish (tracked as T7). **Resolved by
[ADR 0037](0037-durable-retained-messages.md)** — see the revision notes below.

## Consequences

- **Good:** retained messages behave cluster-wide — both for members present at
  publish time and for nodes that join later (back-filled on link-up, §3); one
  `deliver` path for local and remote application; clears propagate; no relay loops;
  non-retained forwarding is unchanged.
- **Cost / limits:** every retained publish fans out to all peers (O(nodes); retained
  publishes are typically infrequent) — under durable retained (ADR 0037) this
  broadcast is interest-only and cache warming rides the post-commit fan-out instead;
  a link-up between *differing* sets still transfers the sender's whole set, chunked
  (topic-level diffing is a possible further refinement of §3's digest step); a single
  retained message too large for a peer frame is skipped from back-fill (loudly)
  rather than sent.

## Revision notes — ADR 0037 (durable single-owner retained)

[ADR 0037](0037-durable-retained-messages.md) (Accepted, delivered) revises this
record's **write model** for the durable-by-default configuration; with durable
sessions explicitly opted out (`MQTTD_DURABLE_SESSIONS=0`) everything in this ADR
applies unchanged.

**What stands from this ADR** (and what 0037 builds on):

- The **read model**: subscribe-time replay is a local cache read on every node —
  fetch-on-subscribe stays rejected.
- The **back-fill machinery** (§3): the link-up digest offer (T6) and the chunked
  snapshot (T8) carry the convergence data; 0037 extends the digest with a value hash
  (divergence detection) and the snapshot entries with `(epoch, offset)` tokens.
- Live delivery of a retained publish to current subscribers: unchanged, undelayed.

**What 0037 revises** (durable mode):

- Retained *mutations* commit through the topic's placement-group **lease-owner**
  into the quorum-replicated group log — conflicts are prevented, not resolved; a
  non-owner queues (bounded, loud at the cap) rather than writing divergently.
- Node caches are warmed by the owner's **post-commit fan-out** carrying a clock-free
  `(epoch, offset)` token, applied monotonically per topic; the raw §1 broadcast no
  longer writes caches (it forwards for live delivery only, interest-filtered).
- The §3 **gap-fill rule is replaced by higher-token-wins**: on link-up divergent
  caches converge deterministically to the committed value, and committed clears
  back-fill as tombstone entries so a peer that missed a clear drops the topic.
- **T7 is closed**: divergence across a partition heals — proven by the everyday-race
  and partition heal-convergence integration tests (0037 P4/P6).

## Alternatives considered

- **Fetch retained from peers on subscribe.** Rejected: every new subscription would
  trigger a cross-node query and wait, adding latency to the common case to serve the
  rare late-join; broadcasting on publish keeps subscribe local and fast.
- **Replicate the retained store via the durable plane (consensus).** Rejected as the
  default *at the time*: retained messages do not need linearizable consensus, and routing
  every retained publish through raft would be far heavier than a best-effort broadcast.
  The durable plane remains the right home for the *back-fill digest* (§3) when that
  lands. **Revised by [ADR 0037](0037-durable-retained-messages.md):** the T7 divergence
  analysis (permanent post-heal divergence *and* an everyday concurrent-publish race) plus
  the decision to keep clocks out of correctness reversed this verdict — retained
  *mutations* now commit through the group lease-owner, while this ADR's live delivery and
  local-read model stand.
- **Leave it node-local.** Rejected: it silently violates retained semantics in a
  cluster, which is the whole point of the feature.
