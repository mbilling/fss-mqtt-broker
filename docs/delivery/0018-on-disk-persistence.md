---
adr: "0018"
title: On-disk persistence for durable state
adr_status: Accepted
tasks:
  - id: 0018-P1
    title: Single-node session persistence (PersistentLog over redb)
    status: done
    date: 2026-06-19
    evidence: persistent_log.rs survives-reopen test
  - id: 0018-P2
    title: Lease store on disk (redb RaftStorage)
    status: done
    date: 2026-06-19
    evidence: lease_store.rs openraft Suite + vote_and_lease_survive_reopen
  - id: 0018-P3
    title: Replicated session log on disk (ReplicaState over redb)
    status: done
    date: 2026-06-19
    evidence: cluster_log.rs persistent replica reopen test
  - id: 0018-P3b
    title: Asymmetric stale-replica safety (per-key truncation watermark)
    status: done
    date: 2026-06-19
    evidence: merge_replica_logs does-not-resurrect-truncated-prefix test
  - id: 0018-P4
    title: Retained store on disk (PersistentRetainedStore over redb)
    status: done
    date: 2026-06-19
    evidence: persistent_retained.rs reopen + wildcard/QoS fidelity test
  - id: 0018-P5
    title: Operational hardening (data-dir node-id guard, compaction policy, store-level restart proof)
    status: done
    date: 2026-06-21
    evidence: data_dir.rs guard test; a_durable_session_log_survives_a_full_restart_via_persisted_replicas
  - id: 0018-T6
    title: Node-level restart proofs (single-node persistent + durable-cluster paths)
    status: done
    date: 2026-06-22
    evidence: persistence.rs; durable_node::a_persistent_durable_node_restarts_from_its_data_dir
  - id: 0018-T7
    title: Process-kill (SIGKILL mid-write) crash-consistency test
    status: done
    date: 2026-07-16
    evidence: "Delivered by the ADR 0044 P2 out-of-process harness (the machinery whose absence deferred it): cluster_proc::a_disk_bound_crash_mid_write_loses_no_acked_fact runs one spawned production-binary node under a kernel-enforced RLIMIT_FSIZE (8MB per file, sh ulimit -f — unprivileged) and blasts acked 64KB durable enqueues until a store write crosses the bound and the kernel delivers SIGXFSZ — the process dies exactly ON a write syscall, the sharpest mid-write crash point available (no timed SIGKILL guessing). The survivors keep quorum; the restart reopens the possibly-torn dir UNBOUNDED, redb rolls back any torn write on reopen, ADR 0043 P1 catch-up back-fills the gap, and every acked payload (~29 × 64KB per run) replays to the resumed subscriber. The seeded cluster_proc schedules additionally SIGKILL a node mid-acked-burst every seed."
---

# Delivery — ADR 0018: On-disk persistence for durable state

Decision: [docs/adr/0018-on-disk-persistence.md](../adr/0018-on-disk-persistence.md).

## Plan

Persistence lands one trait seam at a time, smallest blast radius first, so each layer is
proven (engine, fsync semantics, reopen recovery) before the next builds on it.

| Task | Acceptance criterion |
|------|----------------------|
| **0018-P1** Session log | A `redb`-backed `ReplicatedLog` behind the existing trait; fsync-on-commit; committed state + monotonic offset counter survive close/reopen. A single-node broker is truly durable. |
| **0018-P2** Lease store | `LeaseStore` gains a `redb` `RaftStorage`; persist-before-acknowledge; passes openraft's conformance Suite on the disk paths **and** a vote+lease reopen test. Restores Raft restart safety. |
| **0018-P3** Replica log | The follower's committed replica copy persists; write-through fsync before `ReplicateAck`; entries + fence survive reopen. Cluster session recovery survives full restart. |
| **0018-P3b** Stale-replica safety | A per-key truncation low-water is persisted and carried in recovery reads; `merge_replica_logs` drops entries at/below the highest watermark so a stale replica's truncated prefix cannot be resurrected. |
| **0018-P4** Retained store | `redb`-backed `RetainedStore`; write-through fsync (incl. empty-payload clear); map + matching/QoS fidelity survive reopen. |
| **0018-P5** Operational hardening | Data-dir node-id guard refuses a foreign volume; compaction bounded by snapshot/truncation + redb page reuse; a store-level full-cluster-restart proof recovers leases + replicas + sessions. |
| **0018-T6** Node-level restart | A real node over TCP persists state, shuts down (releasing redb locks), restarts from the same data dir, and a client observes recovery — single-node persistent and durable-cluster paths. |
| **0018-T7** Crash test | A SIGKILL-mid-write subprocess test proves torn-write rollback on reopen. |

## Progress

<!-- status-table:0018 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0018-P1 | ✅ done | 2026-06-19 | persistent_log.rs survives-reopen test |
| 0018-P2 | ✅ done | 2026-06-19 | lease_store.rs openraft Suite + vote_and_lease_survive_reopen |
| 0018-P3 | ✅ done | 2026-06-19 | cluster_log.rs persistent replica reopen test |
| 0018-P3b | ✅ done | 2026-06-19 | merge_replica_logs does-not-resurrect-truncated-prefix test |
| 0018-P4 | ✅ done | 2026-06-19 | persistent_retained.rs reopen + wildcard/QoS fidelity test |
| 0018-P5 | ✅ done | 2026-06-21 | data_dir.rs guard test; a_durable_session_log_survives_a_full_restart_via_persisted_replicas |
| 0018-T6 | ✅ done | 2026-06-22 | persistence.rs; durable_node::a_persistent_durable_node_restarts_from_its_data_dir |
| 0018-T7 | ✅ done | 2026-07-16 | "Delivered by the ADR 0044 P2 out-of-process harness (the machinery whose absence deferred it): cluster_proc::a_disk_bound_crash_mid_write_loses_no_acked_fact runs one spawned production-binary node under a kernel-enforced RLIMIT_FSIZE (8MB per file, sh ulimit -f — unprivileged) and blasts acked 64KB durable enqueues until a store write crosses the bound and the kernel delivers SIGXFSZ — the process dies exactly ON a write syscall, the sharpest mid-write crash point available (no timed SIGKILL guessing). The survivors keep quorum; the restart reopens the possibly-torn dir UNBOUNDED, redb rolls back any torn write on reopen, ADR 0043 P1 catch-up back-fills the gap, and every acked payload (~29 × 64KB per run) replays to the resumed subscriber. The seeded cluster_proc schedules additionally SIGKILL a node mid-acked-burst every seed." |
<!-- /status-table:0018 -->

**Architectural note carried from P5:** single-node session-content restart-durability is
not possible — a lone durable node keeps committed entries in the leader's *in-memory* log
until a follower has them, so restart-durability of session content needs **R≥2** (proven
at store level in `cluster_log`). T6's cluster-path test therefore asserts the *lease*
state survives a restart, not the queue.

**Storage-latency ↔ lease-timing constraint (ADR 0026):** the `Durability::Immediate`
(fsync) commit each persistent store does on every raft write (P2's lease store, P3's
replica log) costs tens of milliseconds on real disk. The lease group's raft timing is
sized for that latency, not in-memory speed — see
[ADR 0026](../adr/0026-lease-timing-durable-storage.md). If a future change makes the
on-disk commit slower (larger transactions, more groups, slower media), the lease timing
budget is the thing to re-verify; under sustained load, session-log fsyncs contend with the
lease raft (ADR 0026 T5 tracks coalescing the raft writes).

## Changelog

- **2026-06-22** — T6 node-level restart proofs landed (single-node persistent path in
  `persistence.rs`; durable-cluster path once ADR 0019 graceful shutdown released the redb
  locks). T7 (process-kill) split out and deferred.
- **2026-06-21** — P5 operational hardening: node-id guard, compaction policy, store-level
  full-cluster-restart proof.
- **2026-06-19** — P1–P4 + P3b landed in sequence: session log, lease store, replica log,
  stale-replica watermark safety, retained store — each with a reopen-recovery test.
