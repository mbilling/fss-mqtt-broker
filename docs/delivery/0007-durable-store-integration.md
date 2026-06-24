---
adr: "0007"
title: Wiring the durable cluster session store into the broker
adr_status: Accepted
tasks:
  - id: 0007-T1
    title: NodeId <-> RaftNodeId mapping (step 4a)
    status: done
    date: 2026-06-22
    evidence: node_registry::registry_round_trips_and_is_idempotent
  - id: 0007-T2
    title: Placement groups + group-owner relocation refinement (step 4b)
    status: done
    date: 2026-06-22
    evidence: placement::clients_in_a_group_share_owner_and_replica_set
  - id: 0007-T3
    title: Durable-plane endpoint over the peer wire (step 4c)
    status: done
    date: 2026-06-22
    evidence: plane_carries_consensus_and_replication_over_the_wire
  - id: 0007-T4
    title: Membership reconciler (SWIM -> openraft voters, debounced, deterministic bootstrap) (step 4d)
    status: done
    date: 2026-06-22
    evidence: lease_membership::reconciler_bootstraps_then_grows_the_group
  - id: 0007-T5
    title: Durable cluster SessionStore (GroupRoutedLog + LocalLeaseSource) (step 4e)
    status: done
    date: 2026-06-22
    evidence: enqueue_replicates_to_a_follower
  - id: 0007-T6
    title: Wire into mqttd (MQTTD_DURABLE_SESSIONS, shared Arc store, QoS-2 dedup via store) (step 4f)
    status: done
    date: 2026-06-22
    evidence: qos2_dedup_window_is_backed_by_the_store; durable_sessions::enqueue_is_durable_across_a_three_node_cluster
  - id: 0007-T7
    title: Workstream F takeover/handoff (replica promoted on owner Dead)
    status: done
    date: 2026-06-22
    evidence: cluster_store::takeover_recovers_a_keys_log_from_the_shared_replica_state; durable_sessions::a_replica_serves_the_session_after_the_owner_dies
  - id: 0007-T8
    title: Dynamic-reconfiguration hardening under rapid churn (flap -> ephemeral degrade)
    status: deferred
    notes: v1 debounces stable join/leave; rapid flapping / lost-quorum degrades to ADR 0005 ephemeral per the accepted limitation; no flap-stress proof exists yet
  - id: 0007-T9
    title: Connection-driven next_packet_id over the durable store
    status: deferred
    notes: store impls next_packet_id but conn.rs never calls it; outbound packet-id allocation stays hub-side, so the per-packet durable path is record_received/clear_received only
---

# Delivery — ADR 0007: Wiring the durable cluster session store into the broker

Decision: [docs/adr/0007-durable-store-integration.md](../adr/0007-durable-store-integration.md).

## Plan

The decision's workstream-E step 4 decomposes into its own sub-steps 4a–4f, each
independently shippable and test-first, with the live store swap last. Workstream F
(takeover) follows. Each task carries a stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0007-T1** Node mapping (4a) | A deterministic, idempotent `NodeId <-> RaftNodeId` registry so consensus and placement agree on identity. |
| **0007-T2** Placement groups (4b) | `group(client) = stable_hash % NUM_GROUPS` (256), per-group replica set + owner over `Placement`; ADR 0005 relocation refined to the *group* owner. Pure, unit-tested. |
| **0007-T3** Durable plane (4c) | A shared `DurablePlane` handle (`register`/`fail`/`handle`) bundling the lease `Raft` + mesh raft network + replica transport + replica state, called directly by peer-link tasks (off the hub actor). A two-node duplex test elects + commits a lease and quorum-replicates a session-log append. |
| **0007-T4** Reconciler (4d) | SWIM membership drives the openraft voter set: deterministic smallest-id bootstrap, learners promoted via `change_membership`, debounced under churn, leader-only. |
| **0007-T5** Durable store (4e) | `GroupRoutedLog` routes each key to its group's `ClusterLog` (lease from `LocalLeaseSource`, replica set from `Placement`) under `ReplicatedSessionStore`; an enqueue replicates to a follower. |
| **0007-T6** mqttd wiring (4f) | `Arc<dyn SessionStore>` shared with connections; `MQTTD_DURABLE_SESSIONS` builds the durable store (loudly logged); connections use the store for QoS-2 dedup; single-node/memory path unchanged. |
| **0007-T7** Takeover (workstream F) | On an owner's `Dead` event a replica is promoted (it already holds the quorum-replicated log) and serves the next reconnect, fenced by a fresh lease epoch. |
| **0007-T8** Churn hardening | Full dynamic reconfiguration under rapid flapping / lost-quorum without degrading affected groups to ephemeral mode. |
| **0007-T9** Connection packet ids | The connection allocates outbound packet ids through the durable store rather than hub-side. |

## Progress

<!-- status-table:0007 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0007-T1 | ✅ done | 2026-06-22 | node_registry::registry_round_trips_and_is_idempotent |
| 0007-T2 | ✅ done | 2026-06-22 | placement::clients_in_a_group_share_owner_and_replica_set |
| 0007-T3 | ✅ done | 2026-06-22 | plane_carries_consensus_and_replication_over_the_wire |
| 0007-T4 | ✅ done | 2026-06-22 | lease_membership::reconciler_bootstraps_then_grows_the_group |
| 0007-T5 | ✅ done | 2026-06-22 | enqueue_replicates_to_a_follower |
| 0007-T6 | ✅ done | 2026-06-22 | qos2_dedup_window_is_backed_by_the_store; durable_sessions::enqueue_is_durable_across_a_three_node_cluster |
| 0007-T7 | ✅ done | 2026-06-22 | cluster_store::takeover_recovers_a_keys_log_from_the_shared_replica_state; durable_sessions::a_replica_serves_the_session_after_the_owner_dies |
| 0007-T8 | 💤 deferred | — | v1 debounces stable join/leave; rapid flapping / lost-quorum degrades to ADR 0005 ephemeral per the accepted limitation; no flap-stress proof exists yet |
| 0007-T9 | 💤 deferred | — | store impls next_packet_id but conn.rs never calls it; outbound packet-id allocation stays hub-side, so the per-packet durable path is record_received/clear_received only |
<!-- /status-table:0007 -->

**Deviation note (carried from T6):** the ADR said `conn.rs`'s local `HashSet` dedup is
removed; in practice it became the no-store fallback dedup window and is bypassed when a
store is configured, rather than being deleted. The connection's durable hot-path is
`record_received`/`clear_received`; `next_packet_id` from the connection is tracked as T9.

**Lease-timing ↔ storage-latency constraint (ADR 0026):** the lease group this wires up is
tuned for fsync-on-commit latency, not in-memory speed. Its raft heartbeat/election timing
([`lease_group::config`](../../crates/mqtt-cluster/src/lease_group.rs)) and the reconcile
[`DRIVER_TICK`](../../crates/mqtt-cluster/src/durable_node.rs) are budgeted so a durable
(redb) store holds a stable leader; do not retune them for faster failover without
re-checking the persistent path. See [ADR 0026](../adr/0026-lease-timing-durable-storage.md)
(and its T5, coalescing raft writes, for the residual under-load churn).

## Changelog

- **2026-06-22** — Migration audit: steps 4a–4f and workstream F verified built against
  code and tests. T6 wires `MQTTD_DURABLE_SESSIONS` and the shared `Arc` store with
  three-node durable e2e proofs; T7 takeover proven (`a_replica_serves_the_session_after_the_owner_dies`).
  T8 (churn hardening) and T9 (connection-driven packet ids) split out as deferred gaps.
