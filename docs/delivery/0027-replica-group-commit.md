---
adr: "0027"
title: Group-commit for the durable replica apply path
adr_status: Accepted
tasks:
  - id: 0027-T1
    title: ReplicaState::apply_batch — persist a batch of ops in one fsync'd transaction, fence/order-preserving, ack-iff-committed
    status: done
    date: 2026-06-24
    evidence: "cluster_log::ReplicaState::apply_batch persists all accepted ops in one Durability::Immediate transaction; apply delegates to the shared persist_batch/apply_in_memory helpers. Tests: apply_batch_persists_a_whole_burst_in_one_commit (durable across reopen), apply_batch_fences_stale_ops_in_slice_order (per-op fencing == apply), apply_batch_applies_in_order_with_a_trailing_truncate (order + watermark survive reopen), apply_batch_of_one_equals_apply."
  - id: 0027-T2
    title: Single replica-writer task that coalesces queued Replicate ops into one apply_batch; wire DurablePlane::handle to it
    status: done
    date: 2026-06-24
    evidence: "durable_plane::spawn_replica_writer drains its mpsc backlog (recv + try_recv) and applies the burst with one apply_batch off a spawn_blocking; DurablePlane::handle's Replicate arm sends to it and awaits a oneshot instead of per-op spawn_blocking. Test replica_writer_group_commits_a_concurrent_burst (50 concurrent frames all accepted + durable); plane_carries_consensus_and_replication_over_the_wire still green through the new path."
  - id: 0027-T3
    title: Validate live in the demo (durable overlay under loadgen) that the lease term/epoch stays flat
    status: done
    date: 2026-06-24
    evidence: "Durable overlay rebuilt with the writer, run under the loadgen: after formation the lease epoch went flat at 8 with a stable leader for 3+ minutes UNDER LOAD. Before group-commit the same load climbed the epoch continuously (8 -> 23 in 2 min, never settling). Steady-state under-load churn eliminated; only a one-time formation transient remains."
  - id: 0027-T4
    title: Cross-reference from ADR 0026 T5 (mark its concern addressed here)
    status: done
    date: 2026-06-24
    evidence: ADR 0026 delivery T5 marked done, pointing here as the implementation.
---

# Delivery — ADR 0027: Group-commit for the durable replica apply path

Decision: [docs/adr/0027-replica-group-commit.md](../adr/0027-replica-group-commit.md).

ADR 0026 T4 surfaced residual under-load churn: the follower replica apply does one
`Durability::Immediate` fsync **per replicated message**, contending with the lease group's
fsyncs. The lease store already batches its raft writes; mirror that on the replica path with a
group-commit writer.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0027-T1** Batch apply | `ReplicaState::apply_batch` persists all accepted ops in **one** fsync'd transaction; per-op fencing matches `apply`; a persist failure rejects the whole batch; offset order preserved. `apply` delegates to it. Deterministic unit tests. |
| **0027-T2** Writer | A single replica-writer task drains its queue and calls `apply_batch` once per drained burst; `DurablePlane::handle` sends `Replicate` ops to it (with a oneshot reply) instead of `spawn_blocking`-applying each. Bounded/back-pressured queue. |
| **0027-T3** Demo | With the durable overlay under the loadgen, the lease term/epoch stays flat (the churn ADR 0026 T4 found is gone). Validated live (the in-process harness cannot reproduce device contention). |
| **0027-T4** Docs | ADR 0026 T5 cross-references this ADR as the place its concern is addressed. |

## Progress

<!-- status-table:0027 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0027-T1 | ✅ done | 2026-06-24 | "cluster_log::ReplicaState::apply_batch persists all accepted ops in one Durability::Immediate transaction; apply delegates to the shared persist_batch/apply_in_memory helpers. Tests: apply_batch_persists_a_whole_burst_in_one_commit (durable across reopen), apply_batch_fences_stale_ops_in_slice_order (per-op fencing == apply), apply_batch_applies_in_order_with_a_trailing_truncate (order + watermark survive reopen), apply_batch_of_one_equals_apply." |
| 0027-T2 | ✅ done | 2026-06-24 | "durable_plane::spawn_replica_writer drains its mpsc backlog (recv + try_recv) and applies the burst with one apply_batch off a spawn_blocking; DurablePlane::handle's Replicate arm sends to it and awaits a oneshot instead of per-op spawn_blocking. Test replica_writer_group_commits_a_concurrent_burst (50 concurrent frames all accepted + durable); plane_carries_consensus_and_replication_over_the_wire still green through the new path." |
| 0027-T3 | ✅ done | 2026-06-24 | "Durable overlay rebuilt with the writer, run under the loadgen: after formation the lease epoch went flat at 8 with a stable leader for 3+ minutes UNDER LOAD. Before group-commit the same load climbed the epoch continuously (8 -> 23 in 2 min, never settling). Steady-state under-load churn eliminated; only a one-time formation transient remains." |
| 0027-T4 | ✅ done | 2026-06-24 | ADR 0026 delivery T5 marked done, pointing here as the implementation. |
<!-- /status-table:0027 -->

## Changelog

- **2026-06-24** — ADR accepted (ADR 0026 T5 promoted into its own ADR) and fully delivered.
  T1 (`apply_batch`, test-first) and T2 (the `spawn_replica_writer` group-commit task wired
  into `DurablePlane::handle`) landed, then T3 validated live: under the demo loadgen the
  durable lease epoch goes flat (was continuously climbing). mqtt-cluster lib suite 150 green
  (5 new tests); fmt + clippy `-D warnings` clean.
