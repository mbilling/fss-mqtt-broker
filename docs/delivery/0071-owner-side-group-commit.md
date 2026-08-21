---
adr: "0071"
title: "Owner-side group commit: one durable-write serializer per node"
adr_status: Accepted
tasks:
  - id: 0071-T1
    title: Route ClusterLog::local_ack through the plane's shared replica-writer — owner appends across all groups and follower replica applies group-commit into single fsync'd apply_batch transactions; local-durable-before-fan-out preserved; fail closed on a closed writer
    status: done
    date: 2026-08-21
    evidence: "ClusterLog::local_ack routes (epoch, op, oneshot) through DurablePlane's replica-writer (with_owner_writer / maybe_owner_writer builders, plumbed GroupRoutedLog -> ClusterLog in durable_node); the writer's recv+try_recv drain batches owner appends across all 256 groups WITH follower replica applies into one Durability::Immediate apply_batch txn (identical per-op fence semantics — ADR 0027 rule 1 — each op's oneshot returns its own verdict); append awaits the local ack BEFORE follower fan-out, so an op never reaches a follower the owner has not fsync'd; closed writer = not-durable (fail closed); the owner's fsync moves onto the blocking pool (it ran inline on an async worker before) and the two-locker ReplicaState contention ends. Unit: owner_writes_group_commit_with_follower_applies (30 owner + 20 follower concurrent ops — all accepted, all durably present, batches < ops, max_batch >= 2); full mqtt-cluster suite green (260). MEASURED (same Mac, same params — 16 publishers x window 8, 20 s, 2 reps, release, 3 spawned nodes): qos1-durable-owner 46.9 -> 73.6 acked msg/s (+57%) with p50 2804 -> 1804 ms; qos2 12.8 -> 21.5 (+68%) with p50 12.2 -> 6.8 s; broker append mean 48.7 -> 22.5 ms; clean-session control unchanged (~114k vs ~112k, driver-bound both). Batch depth scales with concurrency x fsync time, so the win grows on faster disks at higher fan-in — the dedicated-hardware quantification rides the next scale-curve run."
  - id: 0071-T2
    title: Writer observability — batches/ops/max-batch counters on the shared serializer (covering the ADR 0027 follower half too, which had none), polled into Prometheus as mqttd_durable_writer_batches/_ops/_max_batch
    status: done
    date: 2026-08-21
    evidence: "WriterStats (atomics, no observability dep in mqtt-cluster) published by DurablePlane::writer_stats(); mqttd polls every 5s and advances true counters by delta (Metrics::durable_writer_progress), max batch as a gauge. ops/batches = live mean batch size: 1.0 at rest, rising exactly when group commit pays."
  - id: 0071-T3
    title: Single-node backend (PersistentLog / sessions.redb) batching
    status: planned
    notes: "The non-cluster backend still commits one Immediate txn per append; its run() closure is the seam. Worth doing when single-node durable throughput matters as much as clustered."
---

# Delivery: ADR 0071 — Owner-side group commit

[ADR 0071](../adr/0071-owner-side-group-commit.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0071 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0071-T1 | ✅ done | 2026-08-21 | "ClusterLog::local_ack routes (epoch, op, oneshot) through DurablePlane's replica-writer (with_owner_writer / maybe_owner_writer builders, plumbed GroupRoutedLog -> ClusterLog in durable_node); the writer's recv+try_recv drain batches owner appends across all 256 groups WITH follower replica applies into one Durability::Immediate apply_batch txn (identical per-op fence semantics — ADR 0027 rule 1 — each op's oneshot returns its own verdict); append awaits the local ack BEFORE follower fan-out, so an op never reaches a follower the owner has not fsync'd; closed writer = not-durable (fail closed); the owner's fsync moves onto the blocking pool (it ran inline on an async worker before) and the two-locker ReplicaState contention ends. Unit: owner_writes_group_commit_with_follower_applies (30 owner + 20 follower concurrent ops — all accepted, all durably present, batches < ops, max_batch >= 2); full mqtt-cluster suite green (260). MEASURED (same Mac, same params — 16 publishers x window 8, 20 s, 2 reps, release, 3 spawned nodes): qos1-durable-owner 46.9 -> 73.6 acked msg/s (+57%) with p50 2804 -> 1804 ms; qos2 12.8 -> 21.5 (+68%) with p50 12.2 -> 6.8 s; broker append mean 48.7 -> 22.5 ms; clean-session control unchanged (~114k vs ~112k, driver-bound both). Batch depth scales with concurrency x fsync time, so the win grows on faster disks at higher fan-in — the dedicated-hardware quantification rides the next scale-curve run." |
| 0071-T2 | ✅ done | 2026-08-21 | "WriterStats (atomics, no observability dep in mqtt-cluster) published by DurablePlane::writer_stats(); mqttd polls every 5s and advances true counters by delta (Metrics::durable_writer_progress), max batch as a gauge. ops/batches = live mean batch size: 1.0 at rest, rising exactly when group commit pays." |
| 0071-T3 | ⬜ planned | — | "The non-cluster backend still commits one Immediate txn per append; its run() closure is the seam. Worth doing when single-node durable throughput matters as much as clustered." |
<!-- /status-table:0071 -->

## Changelog

- **2026-08-21** — T1/T2 landed together: the owner path joins the follower's
  writer (one node-wide durable-write serializer), and the serializer gains the
  metrics ADR 0027's half never had.
