---
adr: "0014"
title: Cross-node retained-message replication
adr_status: Accepted
tasks:
  - id: 0014-T1
    title: Retained publishes forwarded to every peer; non-retained stays interest-filtered
    status: done
    date: 2026-06-17
    evidence: hub.rs forward_to_peers (retain fans out to all peers)
  - id: 0014-T2
    title: Received retained publish updates the receiving node's store via the deliver path (no re-forward)
    status: done
    date: 2026-06-17
    evidence: HubCommand::RemotePublish handler -> self.deliver(... retain ...); retained_message_replicates_across_nodes
  - id: 0014-T3
    title: Zero-length retained payload clears the entry on every node
    status: done
    date: 2026-06-17
    evidence: zero_length_retained_publish_clears_retained_message
  - id: 0014-T4
    title: Back-fill on link-up — full RetainedSnapshot sent to each new peer
    status: done
    date: 2026-06-17
    evidence: hub.rs send via PeerMessage::RetainedSnapshot (RetainedStore::all); retained_snapshot_is_sent_to_a_new_peer
  - id: 0014-T5
    title: Snapshot applied gap-fill — never clobbers a topic the node already holds
    status: done
    date: 2026-06-17
    evidence: RemoteRetainedSnapshot gap-fill handler; back-fill gap-fill unit test (hub.rs ~2317); retained_back_fills_a_node_that_joins_after_the_publish
  - id: 0014-T6
    title: Digest-diff back-fill (avoid re-sending the whole retained set on every link-up)
    status: deferred
    notes: ADR §3 leaves this as a later optimization; current back-fill re-sends the full set on each link-up (no digest code in the tree).
  - id: 0014-T7
    title: Partition-heal conflict reconciliation (two nodes holding different values for the same topic)
    status: deferred
    notes: ADR §3 leaves divergence unresolved — gap-fill keeps each side's own value; reconciling needs per-message timestamps / version vectors, out of scope.
  - id: 0014-T8
    title: Chunking a very large retained snapshot beyond the peer frame limit
    status: deferred
    notes: ADR §3 — snapshot size is bounded by the peer frame limit; chunking is deferred.
  - id: 0014-T9
    title: Carry message-expiry interval on the cross-node peer link
    status: deferred
    notes: ADR Consequences — cross-node delivery carries no message-expiry deadline (the peer link does not yet carry the interval); pre-existing carried limitation.
---

# Delivery — ADR 0014: Cross-node retained-message replication

Decision: [docs/adr/0014-cross-node-retained.md](../adr/0014-cross-node-retained.md).

## Plan

The decision's three numbered sections — publish-time fan-out, apply-on-receive via the
`deliver` path, and back-fill-on-join with gap-fill — are all built and proven by the
cluster-chaos integration suite that originally pinned the gap. The deferred items are
the optimizations and unreconciled edges the ADR itself flags as out of scope.

| Task | Acceptance criterion |
|------|----------------------|
| **0014-T1** Publish-time fan-out | `forward_to_peers` sends a retained message to *every* connected peer regardless of interest; a non-retained message stays interest-filtered. |
| **0014-T2** Apply on receive | `HubCommand::RemotePublish` carries `retain` and applies through the same `deliver` path (store/clear + local deliver) but never re-forwards — no relay loop. |
| **0014-T3** Clear propagation | A zero-length retained payload clears the entry on every node, as locally [MQTT-3.3.1-10]. |
| **0014-T4** Back-fill on link-up | On link establishment a node sends its full retained set (`PeerMessage::RetainedSnapshot` via `RetainedStore::all`) to each new peer. |
| **0014-T5** Gap-fill apply | The receiver sets a retained message only for a topic it does not already hold, so a peer's snapshot never clobbers a possibly-newer local value. |
| **0014-T6** Digest diff | A digest-based diff avoids re-sending the whole retained set on every link-up. |
| **0014-T7** Partition-heal reconcile | Divergent same-topic values across a healed partition are reconciled (timestamps / version vectors). |
| **0014-T8** Snapshot chunking | A retained set larger than the peer frame limit is chunked. |
| **0014-T9** Cross-node expiry | The peer link carries the message-expiry interval so cross-node delivery honours an expiry deadline. |

## Progress

<!-- status-table:0014 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0014-T1 | ✅ done | 2026-06-17 | hub.rs forward_to_peers (retain fans out to all peers) |
| 0014-T2 | ✅ done | 2026-06-17 | HubCommand::RemotePublish handler -> self.deliver(... retain ...); retained_message_replicates_across_nodes |
| 0014-T3 | ✅ done | 2026-06-17 | zero_length_retained_publish_clears_retained_message |
| 0014-T4 | ✅ done | 2026-06-17 | hub.rs send via PeerMessage::RetainedSnapshot (RetainedStore::all); retained_snapshot_is_sent_to_a_new_peer |
| 0014-T5 | ✅ done | 2026-06-17 | RemoteRetainedSnapshot gap-fill handler; back-fill gap-fill unit test (hub.rs ~2317); retained_back_fills_a_node_that_joins_after_the_publish |
| 0014-T6 | 💤 deferred | — | ADR §3 leaves this as a later optimization; current back-fill re-sends the full set on each link-up (no digest code in the tree). |
| 0014-T7 | 💤 deferred | — | ADR §3 leaves divergence unresolved — gap-fill keeps each side's own value; reconciling needs per-message timestamps / version vectors, out of scope. |
| 0014-T8 | 💤 deferred | — | ADR §3 — snapshot size is bounded by the peer frame limit; chunking is deferred. |
| 0014-T9 | 💤 deferred | — | ADR Consequences — cross-node delivery carries no message-expiry deadline (the peer link does not yet carry the interval); pre-existing carried limitation. |
<!-- /status-table:0014 -->

## Changelog

- **2026-06-17** — Cross-node retained replication landed: publish-time fan-out to all
  peers (T1), apply-on-receive via the single `deliver` path with no relay loop (T2),
  clear propagation (T3), full-snapshot back-fill on link-up (T4), and gap-fill apply
  that never clobbers a local value (T5) — proven in `cluster_chaos.rs` and hub unit
  tests. Digest-diff (T6), partition-heal reconciliation (T7), snapshot chunking (T8),
  and cross-node message-expiry (T9) recorded as deferred per the ADR.
</content>
