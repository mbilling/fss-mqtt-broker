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
    status: done
    date: 2026-07-02
    evidence: "On link-up the hub now offers PeerMessage::RetainedDigest (topic count + XOR of stable_id(topic) — order-independent; topics only, since gap-fill can only ever accept missing topics, so payload digests add nothing) instead of pushing the snapshot; the receiver compares against its own set's digest and pulls with PeerMessage::RetainedRequest only when they differ, answered with the chunked (T8) snapshot. A steady-state link-up or flap between synced nodes transfers one small frame instead of the whole retained set; an empty node offers nothing and pulls when it sees a peer's digest. Tests: hub retained_digest_is_offered_and_a_request_pulls_the_snapshot, a_matching_retained_digest_skips_the_back_fill, a_differing_retained_digest_pulls_the_peers_set, the_retained_digest_is_order_independent_and_set_sensitive; codec roundtrips for both new frames; integration retained_back_fills_a_node_that_joins_after_the_publish now exercises digest->request->snapshot over real links."
  - id: 0014-T7
    title: Partition-heal conflict reconciliation (two nodes holding different values for the same topic)
    status: deferred
    notes: "Resolved by decision in ADR 0037 (Proposed): divergence is PREVENTED rather than reconciled — retained mutations commit through the topic's placement-group lease-owner with clock-free (epoch, offset) convergence tokens; LWW/HLC timestamp reconciliation was considered and rejected (clocks in the trust base, silently dropped acked writes). This task closes when 0037-P5/P6 land (offset-aware back-fill + heal-convergence integration tests)."
  - id: 0014-T8
    title: Chunking a very large retained snapshot beyond the peer frame limit
    status: done
    date: 2026-07-02
    evidence: "This was a latent outage, not just an optimization: send_retained_snapshot sent the whole set in ONE frame; encode only failed past u32::MAX, so a set > MAX_FRAME (16 MiB, e.g. 16k topics x 1 KiB) was written whole, rejected by the RECEIVER (FrameTooLarge), tearing down the link — and the link-up back-fill re-sent it on every reconnect: a permanent, data-volume-triggered link-kill loop severing all peer traffic. Now: chunk_retained splits the snapshot under RETAINED_CHUNK_BYTES (4 MiB) per frame — chunks are independent/idempotent under gap-fill so no ordering or completion marker is needed; a single message that could never fit is skipped with a warn (missing one back-fill beats severing the link); encode enforces MAX_FRAME on the SENDING side too; and the peer write loop drops an unencodable frame with a warn instead of dying. Tests: a_large_retained_set_is_chunked_under_the_frame_budget, an_oversized_single_retained_message_is_skipped_not_sent, an_oversized_frame_is_rejected_at_encode."
  - id: 0014-T9
    title: Carry message-expiry interval on the cross-node peer link
    status: done
    date: 2026-06-24
    evidence: "PeerMessage::Publish gained a message_expiry: Option<u32> field (carried over the peer link); hub forward_to_peers passes the publisher's interval and the RemotePublish handler applies it to the local enqueue instead of None. Test forwarded_publish_carries_message_expiry; peer roundtrip covers the wire field."
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
| **0014-T4** Back-fill on link-up | On link establishment a node offers its retained set to each new peer (since T6: a digest offer the peer pulls against, answered with the T8-chunked snapshot). |
| **0014-T5** Gap-fill apply | The receiver sets a retained message only for a topic it does not already hold, so a peer's snapshot never clobbers a possibly-newer local value. |
| **0014-T6** Digest diff | On link-up a topic-set digest is offered instead of the snapshot; matching digests transfer nothing (the steady-state flap costs one small frame); differing digests trigger a pull. |
| **0014-T7** Partition-heal reconcile | Divergent same-topic values across a healed partition are reconciled (timestamps / version vectors). |
| **0014-T8** Snapshot chunking | The snapshot is sent in bounded chunks (independent + idempotent under gap-fill); no frame can approach the peer frame limit, closing the oversized-frame link-kill loop; the frame bound is also enforced at encode and an oversized single message is skipped loudly. |
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
| 0014-T6 | ✅ done | 2026-07-02 | "On link-up the hub now offers PeerMessage::RetainedDigest (topic count + XOR of stable_id(topic) — order-independent; topics only, since gap-fill can only ever accept missing topics, so payload digests add nothing) instead of pushing the snapshot; the receiver compares against its own set's digest and pulls with PeerMessage::RetainedRequest only when they differ, answered with the chunked (T8) snapshot. A steady-state link-up or flap between synced nodes transfers one small frame instead of the whole retained set; an empty node offers nothing and pulls when it sees a peer's digest. Tests: hub retained_digest_is_offered_and_a_request_pulls_the_snapshot, a_matching_retained_digest_skips_the_back_fill, a_differing_retained_digest_pulls_the_peers_set, the_retained_digest_is_order_independent_and_set_sensitive; codec roundtrips for both new frames; integration retained_back_fills_a_node_that_joins_after_the_publish now exercises digest->request->snapshot over real links." |
| 0014-T7 | 💤 deferred | — | "Resolved by decision in ADR 0037 (Proposed): divergence is PREVENTED rather than reconciled — retained mutations commit through the topic's placement-group lease-owner with clock-free (epoch, offset) convergence tokens; LWW/HLC timestamp reconciliation was considered and rejected (clocks in the trust base, silently dropped acked writes). This task closes when 0037-P5/P6 land (offset-aware back-fill + heal-convergence integration tests)." |
| 0014-T8 | ✅ done | 2026-07-02 | "This was a latent outage, not just an optimization: send_retained_snapshot sent the whole set in ONE frame; encode only failed past u32::MAX, so a set > MAX_FRAME (16 MiB, e.g. 16k topics x 1 KiB) was written whole, rejected by the RECEIVER (FrameTooLarge), tearing down the link — and the link-up back-fill re-sent it on every reconnect: a permanent, data-volume-triggered link-kill loop severing all peer traffic. Now: chunk_retained splits the snapshot under RETAINED_CHUNK_BYTES (4 MiB) per frame — chunks are independent/idempotent under gap-fill so no ordering or completion marker is needed; a single message that could never fit is skipped with a warn (missing one back-fill beats severing the link); encode enforces MAX_FRAME on the SENDING side too; and the peer write loop drops an unencodable frame with a warn instead of dying. Tests: a_large_retained_set_is_chunked_under_the_frame_budget, an_oversized_single_retained_message_is_skipped_not_sent, an_oversized_frame_is_rejected_at_encode." |
| 0014-T9 | ✅ done | 2026-06-24 | "PeerMessage::Publish gained a message_expiry: Option<u32> field (carried over the peer link); hub forward_to_peers passes the publisher's interval and the RemotePublish handler applies it to the local enqueue instead of None. Test forwarded_publish_carries_message_expiry; peer roundtrip covers the wire field." |
<!-- /status-table:0014 -->

## Changelog

- **2026-07-02** — T8 + T6 landed together (one subsystem). Investigation showed T8 was
  a **latent outage**, not an optimization: the link-up back-fill sent the whole retained
  set in one frame, the sender's `encode` only failed past `u32::MAX`, and the receiver's
  16 MiB frame limit then tore down the link — with the back-fill re-sent on every
  reconnect, a retained set past 16 MiB put the peer link in a permanent kill loop,
  severing *all* traffic between the two nodes. Now the snapshot is **chunked** (4 MiB
  budget per frame; chunks independent and idempotent under gap-fill; an
  impossible-to-fit single message is skipped loudly; the frame bound is also enforced
  at encode and the write loop drops an unencodable frame instead of dying), and link-up
  starts with a **topic-set digest** instead of the snapshot — matching digests (the
  steady-state link flap) transfer nothing; differing digests trigger a pull answered
  with the chunked snapshot. Existing back-fill/gap-fill integration tests pass
  unchanged over the new flow; T7 (partition-heal reconciliation) remains the one open
  0014 item, queued as its own ADR (retained-value versioning).
- **2026-06-17** — Cross-node retained replication landed: publish-time fan-out to all
  peers (T1), apply-on-receive via the single `deliver` path with no relay loop (T2),
  clear propagation (T3), full-snapshot back-fill on link-up (T4), and gap-fill apply
  that never clobbers a local value (T5) — proven in `cluster_chaos.rs` and hub unit
  tests. Digest-diff (T6), partition-heal reconciliation (T7), snapshot chunking (T8),
  and cross-node message-expiry (T9) recorded as deferred per the ADR.
</content>
