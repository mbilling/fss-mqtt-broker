---
adr: "0019"
title: Graceful shutdown and connection draining
adr_status: Accepted
tasks:
  - id: 0019-T1
    title: SIGTERM/SIGINT handling + second-signal escalation
    status: done
    date: 2026-06-21
    evidence: wait_for_shutdown_signal / graceful_shutdown
  - id: 0019-T2
    title: Cancellation token + bounded connection drain (MQTTD_SHUTDOWN_GRACE)
    status: done
    date: 2026-06-21
    evidence: graceful_shutdown_drains_an_established_connection
  - id: 0019-T3
    title: Readiness flips to not-ready while draining
    status: done
    date: 2026-06-21
    evidence: health.rs draining AtomicBool
  - id: 0019-T4
    title: v5 Server DISCONNECT 0x8B on drain
    status: done
    date: 2026-06-22
    evidence: graceful_shutdown_sends_v5_server_shutting_down_disconnect
  - id: 0019-T5
    title: Lease-group driver stop + clean openraft shutdown
    status: done
    date: 2026-06-22
    evidence: main.rs graceful_shutdown (driver.abort + raft shutdown)
  - id: 0019-T6
    title: SWIM graceful leave on shutdown
    status: done
    date: 2026-06-22
    evidence: a_graceful_leave_is_seen_dead_faster_than_failure_detection
  - id: 0019-T7
    title: Node-level restart proofs (single-node + cluster path)
    status: done
    date: 2026-06-22
    evidence: persistence.rs; durable_node::a_persistent_durable_node_restarts_from_its_data_dir
  - id: 0019-T8
    title: Lease-leadership transfer when the leaving node is the Raft leader
    status: deferred
    notes: avoids one election (~300-600ms) on a leaving leader; needs openraft 0.9 transfer-API evaluation first
  - id: 0019-T9
    title: In-flight QoS settle / hub Drain command
    status: deferred
    notes: drain closes after current packet; durable state already protected by ADR 0018 + raft shutdown
---

# Delivery — ADR 0019: Graceful shutdown and connection draining

Decision: [docs/adr/0019-graceful-shutdown.md](../adr/0019-graceful-shutdown.md).

## Plan

The decision's five shutdown stages decompose into these tasks. Each carries a stable
id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0019-T1** Signal handling | `SIGTERM` or `SIGINT` begins drain; a second signal during the grace window forces immediate exit. |
| **0019-T2** Bounded drain | One cancellation token stops the accept loops and is carried into every connection; a `TaskTracker` waits live connections out, bounded by `MQTTD_SHUTDOWN_GRACE` (default 30s); deadline elapse logs loudly and exits. A draining connection closes cleanly — no will fired, session retained. |
| **0019-T3** Readiness flip | A shared `draining` flag is set before the token is cancelled, so `/readyz` reports not-ready while connections drain. |
| **0019-T4** v5 DISCONNECT | A draining v5 connection receives a `0x8B Server shutting down` DISCONNECT before close; v3.1.1 is just closed. |
| **0019-T5** Driver + raft stop | The lease-group driver is stopped *before* the raft, then `raft().shutdown()` flushes/stops consensus cleanly. |
| **0019-T6** SWIM leave | On shutdown the node announces itself `Dead` directly to peers so survivors drop it from the ring without failure-detection latency; lease handoff falls out via survivors' existing reconcile/assign. |
| **0019-T7** Restart proofs | A real node persists state, shuts down (releasing redb locks), restarts from the same data dir, and recovers it — both the single-node persistent and the durable-cluster lease paths. |
| **0019-T8** Leader transfer | A leaving Raft *leader* transfers leadership before departing, removing the one-election gap. |
| **0019-T9** QoS settle | The drain actively completes outstanding QoS 2 handshakes within the grace window rather than closing after the current packet. |

## Progress

<!-- status-table:0019 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0019-T1 | ✅ done | 2026-06-21 | wait_for_shutdown_signal / graceful_shutdown |
| 0019-T2 | ✅ done | 2026-06-21 | graceful_shutdown_drains_an_established_connection |
| 0019-T3 | ✅ done | 2026-06-21 | health.rs draining AtomicBool |
| 0019-T4 | ✅ done | 2026-06-22 | graceful_shutdown_sends_v5_server_shutting_down_disconnect |
| 0019-T5 | ✅ done | 2026-06-22 | main.rs graceful_shutdown (driver.abort + raft shutdown) |
| 0019-T6 | ✅ done | 2026-06-22 | a_graceful_leave_is_seen_dead_faster_than_failure_detection |
| 0019-T7 | ✅ done | 2026-06-22 | persistence.rs; durable_node::a_persistent_durable_node_restarts_from_its_data_dir |
| 0019-T8 | 💤 deferred | — | avoids one election (~300-600ms) on a leaving leader; needs openraft 0.9 transfer-API evaluation first |
| 0019-T9 | 💤 deferred | — | drain closes after current packet; durable state already protected by ADR 0018 + raft shutdown |
<!-- /status-table:0019 -->

## Changelog

- **2026-06-22** — T6 SWIM graceful leave landed (self-declared `Dead`, `leaving` flag,
  generic-future driver shutdown); lease handoff confirmed to fall out of survivor
  reconciliation. T8 (leader transfer) split out as the remaining cluster-leave gap.
- **2026-06-22** — T4 v5 DISCONNECT `0x8B`, T5 driver/raft stop, T7 cluster-path restart
  proof landed.
- **2026-06-21** — Core landed: T1 signals, T2 bounded drain, T3 readiness flip, and the
  single-node restart proof (part of T7). `MQTTD_SHUTDOWN_GRACE` added.
