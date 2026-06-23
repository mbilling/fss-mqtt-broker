---
adr: "0026"
title: Lease-group raft timing tolerant of durable-storage latency
adr_status: Accepted
tasks:
  - id: 0026-T1
    title: Relax lease-group raft timing (heartbeat 500ms, election 1500-3000ms) for fsync-on-commit latency
    status: done
    date: 2026-06-24
    evidence: lease_group::config heartbeat_interval 500 / election_timeout 1500-3000 (was 100/300-600); validated live in the demo on a persistent 3-node group — lease_leader=1, lease_epoch=1 flat over 20s, zero vote churn (the pre-fix demo churned to epoch 218); lease_group/lease_store/durable_node unit suites green under the new timing.
  - id: 0026-T2
    title: Persistent-path durable test with injected commit latency asserting a stable (non-climbing) lease term
    status: done
    date: 2026-06-24
    evidence: durable_sessions::lease_group_forms_and_is_stable_under_slow_durable_commits injects a 200ms per-commit delay via LeaseStore::with_commit_delay (an async sleep in persist) from bring-up, then asserts exactly one leader and the term does not climb over a 5s window. Scope-honest — see scope note; not a deterministic churn guard (in-process router delivers RPCs instantly; same test passes under old + new timing up to 700ms delay), true guard needs network-latency injection (ADR 0024 T7).
  - id: 0026-T7
    title: "Bug: durable lease group only bootstraps if the founder is also the minimum raft id (lease_membership::decide gates Initialize on min == self)"
    status: planned
    notes: Discovered while writing the T2 test (worked around there by sorting names so the founder is the min id). An operator's chosen founder that does not hash to the global min raft id never forms the durable cluster (term stays 0). Decouple bootstrap eligibility from the min-id tiebreak.
  - id: 0026-T3
    title: Slow the lease driver tick and guard the reconciler against re-proposing an in-flight config change
    status: planned
  - id: 0026-T4
    title: Re-enable persistent durable in the demo and restore the lease/durable dashboard panels
    status: planned
  - id: 0026-T5
    title: Group-commit / coalesce raft log writes to cut fsync count (only if relaxed timing is insufficient)
    status: deferred
    notes: openraft already batches AppendEntries, so the marginal win is bounded; revisit only if very slow storage still churns under the relaxed timing.
  - id: 0026-T6
    title: Cross-reference the timing/storage-latency constraint from the 0007 and 0018 delivery docs
    status: planned
---

# Delivery — ADR 0026: Lease-group raft timing tolerant of durable-storage latency

Decision: [docs/adr/0026-lease-timing-durable-storage.md](../adr/0026-lease-timing-durable-storage.md).

The durable lease group churns on disk because the raft timing (heartbeat 100ms, election
300–600ms) is tuned for in-memory speed while the persistent store fsyncs every write.
Budget the timing for fsync latency, cut the churn-amplifying load, and — the gap that hid
it — test the persistent path under injected commit latency.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0026-T1** Timing | `lease_group::config` uses heartbeat 500ms / election 1500–3000ms; a persistent multi-node lease group holds a stable leader. |
| **0026-T2** Test | A multi-node durable test injects a `commit_delay` (an `async` sleep in `persist`, simulating slow fsync) from bring-up and asserts the lease term does **not** climb over an observation window. Covers the slow-commit write path; see scope note below. |
| **0026-T7** Bug | Bootstrap eligibility is decoupled from the min-raft-id tiebreak so any founder forms the durable group. |
| **0026-T3** Driver | `DRIVER_TICK` slowed to ~1s; the membership reconciler does not re-propose a config change while one is in flight (no "already undergoing a configuration change" spam). |
| **0026-T4** Demo | Persistent durable re-enabled in `demo/` (per-node data dir); the `lease_*` / `durable_append_*` panels populate with a stable leader. |
| **0026-T5** Fsync | If very slow storage still churns under the relaxed timing, coalesce raft log writes (group-commit) to cut the fsync count. |
| **0026-T6** Docs | The 0007 and 0018 delivery docs cross-reference this timing↔storage-latency constraint. |

## Progress

<!-- status-table:0026 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0026-T1 | ✅ done | 2026-06-24 | lease_group::config heartbeat_interval 500 / election_timeout 1500-3000 (was 100/300-600); validated live in the demo on a persistent 3-node group — lease_leader=1, lease_epoch=1 flat over 20s, zero vote churn (the pre-fix demo churned to epoch 218); lease_group/lease_store/durable_node unit suites green under the new timing. |
| 0026-T2 | ✅ done | 2026-06-24 | durable_sessions::lease_group_forms_and_is_stable_under_slow_durable_commits injects a 200ms per-commit delay via LeaseStore::with_commit_delay (an async sleep in persist) from bring-up, then asserts exactly one leader and the term does not climb over a 5s window. Scope-honest — see scope note; not a deterministic churn guard (in-process router delivers RPCs instantly; same test passes under old + new timing up to 700ms delay), true guard needs network-latency injection (ADR 0024 T7). |
| 0026-T7 | ⬜ planned | — | Discovered while writing the T2 test (worked around there by sorting names so the founder is the min id). An operator's chosen founder that does not hash to the global min raft id never forms the durable cluster (term stays 0). Decouple bootstrap eligibility from the min-id tiebreak. |
| 0026-T3 | ⬜ planned | — |  |
| 0026-T4 | ⬜ planned | — |  |
| 0026-T5 | 💤 deferred | — | openraft already batches AppendEntries, so the marginal win is bounded; revisit only if very slow storage still churns under the relaxed timing. |
| 0026-T6 | ⬜ planned | — |  |
<!-- /status-table:0026 -->

## Scope note (T2)

The T2 test proves the persistent (slow-commit) write path *forms and serves* under injected
fsync latency — coverage the in-memory tests never give. It is **not** a deterministic guard
for the churn itself: the demo churn is driven by heartbeat/lease maintenance over a network
with latency, but the test harness's in-process router delivers every raft RPC instantly, and
`commit_delay` only delays the persist path (`save_vote`/`append_to_log`) — empty steady-state
heartbeats never persist, so the latency never reaches the lease-maintenance path. The same
test passes under both the old and the relaxed timing (confirmed live at delays up to 700ms).
A true regression guard needs network-latency injection into the raft RPCs (a madsim/turmoil
harness — ADR 0024 T7, deferred). The timing fix itself was validated live in the demo:
persistent durable stable at `lease_leader=1` / `lease_epoch=1` over 20s, zero vote churn.

## Changelog

- **2026-06-24** — ADR accepted after bisecting a 3-node durable churn to fsync-on-commit
  latency vs in-memory-tuned raft timing (in-memory stable; persistent churns, worst on real
  disk). T1 (timing) done — relaxed timing validated live in the demo. T2 (persistent-path
  fault-injection test) done, with the scope note above. Filed T7: a second, latent bug found
  while writing T2 — the durable group only bootstraps if the founder is also the minimum raft
  id. T3–T6 follow.
