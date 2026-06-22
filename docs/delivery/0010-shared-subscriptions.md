---
adr: "0010"
title: Shared subscriptions
adr_status: Accepted
tasks:
  - id: 0010-T1
    title: SharedSubscriptionTable in mqtt-core beside the plain table
    status: done
    date: 2026-06-17
    evidence: SharedSubscriptionTable in mqtt-core/src/shared.rs (matching_reports_group_members_in_order_with_qos)
  - id: 0010-T2
    title: parse_shared validates ShareName/filter; malformed rejected
    status: done
    date: 2026-06-17
    evidence: parse_shared / parse_accepts_wellformed_and_rejects_malformed
  - id: 0010-T3
    title: Round-robin rotation + hub online-preference selection
    status: done
    date: 2026-06-17
    evidence: select_shared (hub.rs); v5_shared_subscription_round_robins_one_member_each
  - id: 0010-T4
    title: Retained messages not replayed to shared subscriptions
    status: done
    date: 2026-06-17
    evidence: v5_shared_subscription_skips_retained_but_ordinary_gets_it
  - id: 0010-T5
    title: QoS downgrade, persistence and lifecycle reuse session machinery
    status: done
    date: 2026-06-17
    evidence: min_qos in deliver_shared (hub.rs); shared membership rebuilt from persisted subscriptions (hub.rs reconcile loop, parse_shared)
  - id: 0010-T6
    title: Cluster-wide single delivery (selection in the hub over gossiped membership)
    status: done
    date: 2026-06-17
    evidence: shared_subscription_delivers_once_cluster_wide (cluster_chaos.rs)
    notes: original per-node §5 limitation superseded by ADR 0015; selection now spans gossiped global membership
  - id: 0010-T7
    title: Subscription-Identifier handling for shared subscriptions
    status: deferred
    notes: ADR 0010 Consequences notes no Subscription-Identifier handling yet; out of scope for the routing lever
  - id: 0010-T8
    title: Indexed shared-group selection (avoid per-publish member-list clone)
    status: deferred
    notes: matching/snapshot clone matching groups' member lists per publish; small in practice, ADR 0010 flags indexed selection as a later optimization
---

# Delivery — ADR 0010: Shared subscriptions

Decision: [docs/adr/0010-shared-subscriptions.md](../adr/0010-shared-subscriptions.md).

## Plan

The decision's five numbered parts decompose into these tasks. Each carries a stable
id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0010-T1** Dedicated table | A separate pure `SharedSubscriptionTable` in `mqtt-core` keyed by `(ShareName, filter)`, holding members with granted QoS in insertion order; the plain `SubscriptionTable` fast path is untouched. |
| **0010-T2** Parse + reject | `parse_shared` validates a non-empty `ShareName` (no `/ + #`) and non-empty remaining filter; a malformed `$share/...` is rejected, not treated as a literal topic. |
| **0010-T3** Rotate + select | Round-robin advances a per-group cursor; the hub picks the first reachable member — an online member if any, else a persistent offline member, else none — yielding fair rotation without black-holing to an offline session. |
| **0010-T4** No retained replay | A new shared subscription receives no retained messages [MQTT-3.8.4]; an ordinary subscriber still does. |
| **0010-T5** QoS/persistence/lifecycle | Delivery QoS is `min(publish, granted)`; persistent shared memberships are persisted in the `Subscription` record and reconstructed on reconnect/restart; clean start / expiry tear them down with plain ones. |
| **0010-T6** Cluster delivery | A matching publish reaches exactly one member cluster-wide, selected over gossiped global membership (ADR 0015 supersedes the original per-node §5). |
| **0010-T7** Subscription-Identifier | Shared subscriptions carry and echo MQTT 5.0 Subscription Identifiers. |
| **0010-T8** Indexed selection | Replace the per-publish clone of matching groups' member lists with an indexed selection. |

## Progress

<!-- status-table:0010 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0010-T1 | ✅ done | 2026-06-17 | SharedSubscriptionTable in mqtt-core/src/shared.rs (matching_reports_group_members_in_order_with_qos) |
| 0010-T2 | ✅ done | 2026-06-17 | parse_shared / parse_accepts_wellformed_and_rejects_malformed |
| 0010-T3 | ✅ done | 2026-06-17 | select_shared (hub.rs); v5_shared_subscription_round_robins_one_member_each |
| 0010-T4 | ✅ done | 2026-06-17 | v5_shared_subscription_skips_retained_but_ordinary_gets_it |
| 0010-T5 | ✅ done | 2026-06-17 | min_qos in deliver_shared (hub.rs); shared membership rebuilt from persisted subscriptions (hub.rs reconcile loop, parse_shared) |
| 0010-T6 | ✅ done | 2026-06-17 | shared_subscription_delivers_once_cluster_wide (cluster_chaos.rs) |
| 0010-T7 | 💤 deferred | — | ADR 0010 Consequences notes no Subscription-Identifier handling yet; out of scope for the routing lever |
| 0010-T8 | 💤 deferred | — | matching/snapshot clone matching groups' member lists per publish; small in practice, ADR 0010 flags indexed selection as a later optimization |
<!-- /status-table:0010 -->

**Note:** ADR 0010 §5's per-node single-delivery limitation is explicitly superseded by
ADR 0015; selection now spans gossiped global membership and lives in the hub
(`select_shared` / `shared_candidates`), so the pure core table only reports matching
groups and members.

## Changelog

- **2026-06-17** — Shared subscriptions landed: pure `SharedSubscriptionTable` in
  `mqtt-core` (T1), `parse_shared` validation (T2), hub round-robin + online-preference
  selection (T3), retained-replay skip (T4), QoS downgrade + persistence reuse (T5), and
  cluster-wide single delivery over gossiped membership (T6, per ADR 0015). T7
  (Subscription-Identifier) and T8 (indexed selection) deferred.
