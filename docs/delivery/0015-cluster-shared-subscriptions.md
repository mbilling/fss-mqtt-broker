---
adr: "0015"
title: Cluster-wide shared subscriptions
adr_status: Accepted
tasks:
  - id: 0015-T1
    title: Originating node selects one member globally and targets it (local deliver or SharedDeliver to peer)
    status: done
    date: 2026-06-17
    evidence: hub.rs cluster-wide shared selection (~1133) -> deliver_to_client / PeerMessage::SharedDeliver
  - id: 0015-T2
    title: Shared-group membership gossiped with node + client attribution (PeerMessage::SharedInterest snapshot)
    status: done
    date: 2026-06-17
    evidence: PeerMessage::SharedInterest send/handle; RemoteSharedInterest handler; per-peer snapshot in node-id order
  - id: 0015-T3
    title: Shared filters removed from the ordinary interest snapshot — delivery paths cleanly separated
    status: done
    date: 2026-06-17
    evidence: RemotePublish delivers to ordinary subscribers only (no shared selection); shared rides SharedDeliver only
  - id: 0015-T4
    title: Received SharedDeliver delivers to one named client (online send / persistent-offline queue)
    status: done
    date: 2026-06-17
    evidence: RemoteSharedDeliver -> deliver_to_client; deliver_to_client online-or-queue helper
  - id: 0015-T5
    title: Selection policy preserves single-node semantics (online-preferring global round-robin, local-offline fallback)
    status: done
    date: 2026-06-17
    evidence: shared_selection_round_robins_local_and_remote_member; shared_subscription_round_robins_one_member
  - id: 0015-T6
    title: Exactly-once cluster-wide delivery proven end to end (no double-delivery)
    status: done
    date: 2026-06-17
    evidence: shared_subscription_delivers_once_cluster_wide (cluster_chaos.rs); v5_shared_subscription_round_robins_one_member_each
  - id: 0015-T7
    title: Carry message-expiry deadline on cross-node SharedDeliver
    status: done
    date: 2026-06-24
    evidence: "PeerMessage::SharedDeliver gained message_expiry: Option<u32>; send_shared_to_peer carries the publisher's interval and the RemoteSharedDeliver handler applies it to deliver_to_client instead of None. Peer roundtrip covers the wire field."
  - id: 0015-T8
    title: Remote-member liveness awareness in the selector
    status: deferred
    notes: ADR Consequences — selector does not know a remote member's liveness, so it may target a member offline on its home node (which then queues) even when a local member is online; an accepted, spec-permitted selection-quality trade-off.
---

# Delivery — ADR 0015: Cluster-wide shared subscriptions

Decision: [docs/adr/0015-cluster-shared-subscriptions.md](../adr/0015-cluster-shared-subscriptions.md).

## Plan

The decision's four numbered sections — originator-selects-globally, attributed
membership gossip, separated delivery paths, and the single-node-preserving selection
policy — are all built and proven that a shared publish is delivered exactly once
cluster-wide. The two deferred items are accepted selection-quality trade-offs the ADR
flags as costs, not unbuilt mechanism.

| Task | Acceptance criterion |
|------|----------------------|
| **0015-T1** Originator selects globally | The node receiving the client publish runs round-robin over the whole group; a local pick is delivered directly, a peer pick is sent as a targeted `PeerMessage::SharedDeliver`. The cursor lives on the selecting node, per `(ShareName, filter)`. |
| **0015-T2** Attributed gossip | `PeerMessage::SharedInterest` snapshots `(ShareName, filter, [(client, granted QoS)])` on the same triggers as ordinary interest plus link-up; each node keeps the latest per-peer snapshot, dropped on a dead link; global list is local then peers in node-id order. |
| **0015-T3** Separated paths | Shared filters no longer fold into the ordinary interest snapshot; ordinary `forward_to_peers` is for non-shared subscribers only and `RemotePublish` runs no shared selection (the old double-delivery). |
| **0015-T4** SharedDeliver apply | A received `SharedDeliver` delivers to exactly one named client (online → send, persistent-offline → queue) via the shared `deliver_to_client` helper, bypassing selection. |
| **0015-T5** Selection policy | Round-robin over the global list prefers a member that can receive now (local online or any remote), falling back to a local persistent-offline member; with no peers this is exactly the ADR 0010 local round-robin. |
| **0015-T6** Exactly-once proof | An integration test with a member on each of two nodes shows each publish reaches exactly one member cluster-wide — never both. |
| **0015-T7** Cross-node expiry | `SharedDeliver` carries a message-expiry deadline across the peer link. |
| **0015-T8** Liveness-aware selection | The selector knows a remote member's liveness and avoids targeting an offline-at-home member when a local online member exists. |

## Progress

<!-- status-table:0015 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0015-T1 | ✅ done | 2026-06-17 | hub.rs cluster-wide shared selection (~1133) -> deliver_to_client / PeerMessage::SharedDeliver |
| 0015-T2 | ✅ done | 2026-06-17 | PeerMessage::SharedInterest send/handle; RemoteSharedInterest handler; per-peer snapshot in node-id order |
| 0015-T3 | ✅ done | 2026-06-17 | RemotePublish delivers to ordinary subscribers only (no shared selection); shared rides SharedDeliver only |
| 0015-T4 | ✅ done | 2026-06-17 | RemoteSharedDeliver -> deliver_to_client; deliver_to_client online-or-queue helper |
| 0015-T5 | ✅ done | 2026-06-17 | shared_selection_round_robins_local_and_remote_member; shared_subscription_round_robins_one_member |
| 0015-T6 | ✅ done | 2026-06-17 | shared_subscription_delivers_once_cluster_wide (cluster_chaos.rs); v5_shared_subscription_round_robins_one_member_each |
| 0015-T7 | ✅ done | 2026-06-24 | "PeerMessage::SharedDeliver gained message_expiry: Option<u32>; send_shared_to_peer carries the publisher's interval and the RemoteSharedDeliver handler applies it to deliver_to_client instead of None. Peer roundtrip covers the wire field." |
| 0015-T8 | 💤 deferred | — | ADR Consequences — selector does not know a remote member's liveness, so it may target a member offline on its home node (which then queues) even when a local member is online; an accepted, spec-permitted selection-quality trade-off. |
<!-- /status-table:0015 -->

## Changelog

- **2026-06-17** — Cluster-wide shared subscriptions landed: originator selects one member
  globally and targets it (T1), attributed `SharedInterest` membership gossip (T2),
  cleanly separated ordinary vs. shared delivery paths removing the double-delivery (T3),
  one-named-recipient `SharedDeliver` apply (T4), and the single-node-preserving selection
  policy (T5) — proven exactly-once cluster-wide in `cluster_chaos.rs` (T6). Cross-node
  message-expiry (T7) and liveness-aware selection (T8) recorded as deferred trade-offs.
</content>
