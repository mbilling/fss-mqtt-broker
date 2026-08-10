---
adr: "0001"
title: "Session durability in a horizontally-scalable cluster"
adr_status: Accepted
tasks:
  - id: 0001-T1
    title: Session ownership by rendezvous (HRW) hashing over SWIM membership with a bounded replica set (default R=3)
    status: done
    date: 2026-06-22
    evidence: hrw.rs owner/replica_set; placement.rs Placement; replica_set_starts_with_owner_and_has_no_dups; a_dead_node_only_moves_the_keys_it_owned
  - id: 0001-T2
    title: Offline queue is a replicated log per session; enqueue is quorum-replicated before PUBACK
    status: done
    date: 2026-06-22
    evidence: cluster_log.rs ClusterLog quorum = len/2+1; append_is_quorum_durable_and_assigns_offsets; append_below_quorum_is_rejected_and_leaves_no_committed_hole
  - id: 0001-T3
    title: Acks/dequeue local-first and lazily truncated; failover replays from last truncated offset; QoS-2 dedup set is part of replicated state
    status: done
    date: 2026-06-22
    evidence: truncate_is_local_first_and_propagates; qos2_dedup_and_packet_id_allocation; pending replay over the m/{client} snapshot
  - id: 0001-T4
    title: Consensus scoped to ownership + the enqueue log only (fan-out stays coordinator-free), via openraft leases
    status: done
    date: 2026-06-22
    evidence: lease.rs LeaseGroup (superseded_holder_is_fenced); lease_store.rs passes_openraft_conformance_suite_in_memory; raft_mesh two_nodes_elect_and_replicate_over_the_wire
  - id: 0001-T5
    title: Takeover / handoff — owner-alive proxy/redirect; owner-dead replica promotion + log replay; existing same-client-id connection disconnected first
    status: done
    date: 2026-06-22
    evidence: conn.rs proxy_to_owner; hub.rs takeover_replaces_connection_and_ignores_stale_detach; a_replica_serves_the_session_after_the_owner_dies
  - id: 0001-T6
    title: Bounded queues (anti-OOM) — per-session caps + overflow policy, MQTT5 session/message expiry, shared subscriptions ($share/)
    status: done
    date: 2026-06-22
    evidence: drop_oldest_evicts_oldest_and_keeps_newest; hub sweep_expired_sessions; shared.rs SharedSubscriptionTable matching_reports_group_members_in_order_with_qos
  - id: 0001-T7
    title: Incremental async SessionStore trait + in-memory impl, with ReplicatedSessionStore expressed over the ReplicatedLog seam (ADR 0006 refinement)
    status: done
    date: 2026-06-22
    evidence: mqtt-storage/src/lib.rs SessionStore trait + MemorySessionStore; logged.rs ReplicatedSessionStore over InMemoryReplicatedLog (q/{client}, m/{client})
  - id: 0001-T8
    title: Consensus-backed durable session log on disk (extends replicated state with QoS-2 dedup + packet-id counter)
    status: done
    date: 2026-06-22
    evidence: persistent_log.rs PersistentLog (state_survives_reopen); durable_node a_persistent_durable_node_restarts_from_its_data_dir; realized by ADR 0017/0018
  - id: 0001-T9
    title: Default-on durable sessions (retire the ephemeral default)
    status: done
    date: 2026-06-24
    evidence: "Done by ADR 0029 — MQTTD_DURABLE_SESSIONS is now default-on (opt-out), so the shipping default is the consensus-backed replicated store. Stability prerequisites landed first: ADR 0026 (timing), 0027 (replica group-commit), 0028 (link-gated voter admission). See docs/delivery/0029-durable-by-default.md."
  - id: 0001-T10
    title: Durable session-expiry deadline across takeover (ADR 0009 phase 3)
    status: done
    date: 2026-06-24
    evidence: "ADR 0009 phase 3. SessionMeta persists session_expiry_at (absolute epoch); the hub's expiring map + sweep use absolute wall-clock (Clock) so deadlines are portable; detach persists the deadline, attach (persistent only) clears it; the sweep reconciles store.expiring_sessions() for OWNED, offline, untracked sessions every EXPIRY_RECONCILE_EVERY ticks so a new owner inherits orphaned deadlines after a takeover and expires them at the original time. Tests inherited_session_expiry_is_swept_after_takeover, session_expiry_finite_retains_then_expires (clock-driven), session_expiry_persists_and_enumerates, decodes_pre_expiry_meta_records; full workspace green."
  - id: 0001-T11
    title: Client-facing reconnect during promotion + spec-legal QoS-1 redelivery bounds (takeover hardening)
    status: deferred
    notes: takeover-serve is proven through the store (F-d); client-facing MQTT reconnect mid-promotion and redelivery bounds deferred to a later hardening pass
  - id: 0001-T12
    title: The durable append covers ONLINE subscribers, not only the offline queue (issue #124)
    status: done
    date: 2026-08-10
    evidence: "ADR 0001 amendment 2026-08-10. deliver_to_client appends via durable_append BEFORE the live send for QoS>0 to a persistent subscriber and carries the offset through PendingOut/Backlog; the log is truncated only through the contiguous acked prefix (Inflight::advance_ack) on PUBACK/PUBCOMP, never on send; finish_attach skips replaying offsets the in-flight table still owns so the DUP resume does not duplicate. Tests: inflight_durability.rs (against the real binary, SIGKILL mid-flight — was #[ignore]d as the reproduction, now green and unignored), the_log_truncates_only_through_the_contiguous_acked_prefix, a_dropped_replay_entry_does_not_pin_the_log, a_live_qos1_delivery_to_a_persistent_subscriber_is_durable_until_acked, a_live_qos1_delivery_to_a_clean_session_writes_nothing, quota_backlog_spills_to_store_on_persistent_detach (extended: no duplicate). Not covered, tracked as #130: the outbound packet id and ack phase are still in memory, so a post-crash redelivery is at-least-once even at QoS 2."
---

# Delivery — ADR 0001: Session durability in a horizontally-scalable cluster

Decision: [docs/adr/0001-session-durability.md](../adr/0001-session-durability.md).

This is the foundational durability design. Most of its decisions are realized through later
ADRs — session affinity (0005), consensus/replication (0006), durable-store integration
(0007), the storage-error contract (0017), and on-disk persistence (0018). Each task below
points at the concrete code/test
that realizes its slice.

## Plan

The seven numbered decisions map to T1–T7; T8 is the durable backend the ADR's roadmap
listed as "next" (now landed via ADR 0017/0018). T9–T11 carry the limitations the ADR and
the implementation plan document explicitly. Each id is stable and referenced by commits,
tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0001-T1** Ownership ring | `owner(client_id)` is chosen by rendezvous (HRW) hashing over SWIM membership; each session has a bounded replica set of R nodes (default R=3), sharded across the cluster so adding nodes adds capacity. |
| **0001-T2** Replicated enqueue | The offline queue is an append-only log per session; enqueue is quorum-replicated across the replica set before the producer's QoS≥1 PUBLISH is PUBACK'd. |
| **0001-T3** Lazy ack + dedup | Acks truncate the log lazily without a synchronous cross-node hop; on failover a replica replays from the last truncated offset; QoS-2 exactly-once survives because the received-packet-id dedup set is replicated state. |
| **0001-T4** Scoped consensus | Session ownership and the enqueue log go through quorum/consensus (split-brain-safe), while fan-out/routing stays coordinator-free — realized as openraft ownership leases. |
| **0001-T5** Takeover | On reconnect the landing node consults the ownership ring: owner alive → proxy/redirect; owner dead → promote a replica and replay the log. An existing connection for the same client-id is disconnected first. |
| **0001-T6** Bounded queues | Per-session caps with an overflow policy (drop-oldest / reject), MQTT 5 Session-Expiry GC, MQTT 5 Message-Expiry drop, and `$share/` shared subscriptions keep a dead-but-persistent client from growing a queue without limit. |
| **0001-T7** Storage seam | `mqtt-storage::SessionStore` is an incremental async trait (`enqueue`/`pending`/`ack`), with an in-memory single-node impl and `ReplicatedSessionStore` expressed over the generic `ReplicatedLog` seam (queue `q/{client}`, metadata `m/{client}`), so the consensus-backed log substitutes underneath without changing session semantics. |
| **0001-T8** Durable backend | A consensus-backed durable log persists committed session state on disk and extends the replicated state with the QoS-2 dedup set and packet-id counter. |
| **0001-T9** Durable-by-default | Durable sessions are the shipping default rather than an opt-in. |
| **0001-T10** Durable expiry | The session-expiry deadline survives takeover rather than restarting its timer. |
| **0001-T11** Takeover reconnect | A client-facing MQTT reconnect during promotion is handled, with spec-legal QoS-1 redelivery bounds. |
| **0001-T12** In-flight durability | The durable append happens before the wire send for a `QoS` > 0 delivery to a persistent subscriber, so §2's contract holds for a message in flight to an **online** subscriber and not only for one queued for a disconnected one. The log is truncated when that subscriber acknowledges, never on send. |

## Progress

<!-- status-table:0001 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0001-T1 | ✅ done | 2026-06-22 | hrw.rs owner/replica_set; placement.rs Placement; replica_set_starts_with_owner_and_has_no_dups; a_dead_node_only_moves_the_keys_it_owned |
| 0001-T2 | ✅ done | 2026-06-22 | cluster_log.rs ClusterLog quorum = len/2+1; append_is_quorum_durable_and_assigns_offsets; append_below_quorum_is_rejected_and_leaves_no_committed_hole |
| 0001-T3 | ✅ done | 2026-06-22 | truncate_is_local_first_and_propagates; qos2_dedup_and_packet_id_allocation; pending replay over the m/{client} snapshot |
| 0001-T4 | ✅ done | 2026-06-22 | lease.rs LeaseGroup (superseded_holder_is_fenced); lease_store.rs passes_openraft_conformance_suite_in_memory; raft_mesh two_nodes_elect_and_replicate_over_the_wire |
| 0001-T5 | ✅ done | 2026-06-22 | conn.rs proxy_to_owner; hub.rs takeover_replaces_connection_and_ignores_stale_detach; a_replica_serves_the_session_after_the_owner_dies |
| 0001-T6 | ✅ done | 2026-06-22 | drop_oldest_evicts_oldest_and_keeps_newest; hub sweep_expired_sessions; shared.rs SharedSubscriptionTable matching_reports_group_members_in_order_with_qos |
| 0001-T7 | ✅ done | 2026-06-22 | mqtt-storage/src/lib.rs SessionStore trait + MemorySessionStore; logged.rs ReplicatedSessionStore over InMemoryReplicatedLog (q/{client}, m/{client}) |
| 0001-T8 | ✅ done | 2026-06-22 | persistent_log.rs PersistentLog (state_survives_reopen); durable_node a_persistent_durable_node_restarts_from_its_data_dir; realized by ADR 0017/0018 |
| 0001-T9 | ✅ done | 2026-06-24 | "Done by ADR 0029 — MQTTD_DURABLE_SESSIONS is now default-on (opt-out), so the shipping default is the consensus-backed replicated store. Stability prerequisites landed first: ADR 0026 (timing), 0027 (replica group-commit), 0028 (link-gated voter admission). See docs/delivery/0029-durable-by-default.md." |
| 0001-T10 | ✅ done | 2026-06-24 | "ADR 0009 phase 3. SessionMeta persists session_expiry_at (absolute epoch); the hub's expiring map + sweep use absolute wall-clock (Clock) so deadlines are portable; detach persists the deadline, attach (persistent only) clears it; the sweep reconciles store.expiring_sessions() for OWNED, offline, untracked sessions every EXPIRY_RECONCILE_EVERY ticks so a new owner inherits orphaned deadlines after a takeover and expires them at the original time. Tests inherited_session_expiry_is_swept_after_takeover, session_expiry_finite_retains_then_expires (clock-driven), session_expiry_persists_and_enumerates, decodes_pre_expiry_meta_records; full workspace green." |
| 0001-T11 | 💤 deferred | — | takeover-serve is proven through the store (F-d); client-facing MQTT reconnect mid-promotion and redelivery bounds deferred to a later hardening pass |
| 0001-T12 | ✅ done | 2026-08-10 | "ADR 0001 amendment 2026-08-10. deliver_to_client appends via durable_append BEFORE the live send for QoS>0 to a persistent subscriber and carries the offset through PendingOut/Backlog; the log is truncated only through the contiguous acked prefix (Inflight::advance_ack) on PUBACK/PUBCOMP, never on send; finish_attach skips replaying offsets the in-flight table still owns so the DUP resume does not duplicate. Tests: inflight_durability.rs (against the real binary, SIGKILL mid-flight — was #[ignore]d as the reproduction, now green and unignored), the_log_truncates_only_through_the_contiguous_acked_prefix, a_dropped_replay_entry_does_not_pin_the_log, a_live_qos1_delivery_to_a_persistent_subscriber_is_durable_until_acked, a_live_qos1_delivery_to_a_clean_session_writes_nothing, quota_backlog_spills_to_store_on_persistent_detach (extended: no duplicate). Not covered, tracked as #130: the outbound packet id and ack phase are still in memory, so a post-crash redelivery is at-least-once even at QoS 2." |
<!-- /status-table:0001 -->

**Architectural note.** Single-node restart-durability of *session content* is not
possible — a lone durable node holds committed entries in the leader's in-memory log until a
follower has them, so durability of queued content needs R≥2 (proven at store level in
`cluster_log`). T8's restart proof therefore asserts lease/durable-node recovery; the
three-node path (`enqueue_is_durable_across_a_three_node_cluster`) carries queue durability.

## Changelog

- **2026-08-10** — T12: §2's contract now holds on the online path too (issue #124). The
  ADR always said a PUBACK means the message is durably enqueued for every matching
  persistent session; `deliver_to_client` took the online branch first and left the
  unacknowledged message in memory alone, so a crash between the publisher's PUBACK and
  the subscriber's lost it with no trace. The append moved ahead of the wire send and the
  truncation moved to the subscriber's acknowledgement. The reproduction that shipped
  `#[ignore]`d with #127 is now the regression test, unignored and green. The cost — a
  durable write per `QoS` > 0 delivery to a *persistent* subscriber — is stated in the
  ADR amendment and in the README; clean sessions and `QoS` 0 do not pay it. Outbound
  packet ids remain in memory, so a post-crash redelivery is at-least-once even at
  `QoS` 2 (#130).
- **2026-06-22** — All seven decisions are realized and the roadmap's "next" durable backend
  has landed: HRW ownership (T1), quorum-replicated enqueue (T2), lazy-ack + QoS-2 dedup
  (T3), openraft-scoped consensus (T4), takeover/handoff (T5), bounded queues + MQTT 5
  expiry + shared subscriptions (T6), the `SessionStore`/`ReplicatedSessionStore` seam (T7),
  and the on-disk consensus-backed durable log via ADR 0017/0018 (T8). Remaining gaps split
  out as deferrals: durable-by-default (T9), durable session-expiry across takeover (T10),
  and client-facing reconnect hardening (T11).
- **2026-06-02** — ADR accepted as design. Roadmap step 1 (incremental async `SessionStore`
  trait + in-memory impl; persistent-session wiring) and step 2 (bounded queues + overflow,
  HRW ownership over SWIM, session affinity/ephemeral mode per ADR 0005, the consensus
  decision per ADR 0006, and the `ReplicatedLog` seam) recorded as already done.
