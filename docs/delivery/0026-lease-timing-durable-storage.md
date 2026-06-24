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
    status: done
    date: 2026-06-24
    evidence: lease_membership::decide now bootstraps on can_bootstrap alone (the min-id tiebreak removed — it never prevented the multi-founder race it targeted and wrongly blocked a non-min founder). Unit test any_founder_bootstraps_with_itself_regardless_of_id_rank; integration guard durable_sessions::lease_group_forms_and_is_stable_under_slow_durable_commits now deliberately makes the founder the MAX raft id (previously hung at term 0).
  - id: 0026-T3
    title: Slow the lease driver tick and guard the reconciler against re-proposing an in-flight config change
    status: done
    date: 2026-06-24
    evidence: DRIVER_TICK 200ms -> 1s (durable_node.rs); RaftView.changing (membership().get_joint_config().len() > 1) makes decide() return None while a change is in joint consensus. Unit test a_leader_does_not_re_propose_while_a_change_is_in_flight; full durable_sessions suite (7/7) green under the slower tick.
  - id: 0026-T4
    title: Re-enable persistent durable in the demo (opt-in) and restore the lease/durable dashboard panels
    status: done
    date: 2026-06-24
    evidence: demo/durable.yml opt-in override (durable sessions + per-node /data volumes); lease_* / durable_append_* panels populate with a stable leader at rest (verified live — lease epoch flat once load is stopped). Kept opt-in, not default, because of the under-load churn finding below (T5). README + demo comments document the overlay.
  - id: 0026-T5
    title: Group-commit / coalesce raft log writes to cut fsync count (residual under-load churn)
    status: planned
    notes: "Promoted from deferred — T4 surfaced concrete evidence it is needed: a durable 3-node demo holds a stable leader AT REST, but under even light sustained QoS-1 load the lease epoch/term climbs slowly (session-log fsyncs contend with the lease raft's fsyncs, delaying heartbeats into election timeouts). openraft already batches AppendEntries so the marginal win is bounded; may also need to isolate the durable session-log I/O from the lease-group raft I/O."
  - id: 0026-T6
    title: Cross-reference the timing/storage-latency constraint from the 0007 and 0018 delivery docs
    status: done
    date: 2026-06-24
    evidence: 0007 and 0018 delivery docs each carry a lease-timing↔storage-latency note pointing at ADR 0026 (and T5 for the under-load churn).
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
| **0026-T4** Demo | Persistent durable available in `demo/` via an opt-in override (per-node data dir); the `lease_*` / `durable_append_*` panels populate with a stable leader at rest. |
| **0026-T5** Fsync | Coalesce raft log writes (group-commit) to cut the fsync count, addressing the residual under-load churn T4 surfaced (and isolate session-log I/O from the lease raft if needed). |
| **0026-T6** Docs | The 0007 and 0018 delivery docs cross-reference this timing↔storage-latency constraint. |

## Progress

<!-- status-table:0026 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0026-T1 | ✅ done | 2026-06-24 | lease_group::config heartbeat_interval 500 / election_timeout 1500-3000 (was 100/300-600); validated live in the demo on a persistent 3-node group — lease_leader=1, lease_epoch=1 flat over 20s, zero vote churn (the pre-fix demo churned to epoch 218); lease_group/lease_store/durable_node unit suites green under the new timing. |
| 0026-T2 | ✅ done | 2026-06-24 | durable_sessions::lease_group_forms_and_is_stable_under_slow_durable_commits injects a 200ms per-commit delay via LeaseStore::with_commit_delay (an async sleep in persist) from bring-up, then asserts exactly one leader and the term does not climb over a 5s window. Scope-honest — see scope note; not a deterministic churn guard (in-process router delivers RPCs instantly; same test passes under old + new timing up to 700ms delay), true guard needs network-latency injection (ADR 0024 T7). |
| 0026-T7 | ✅ done | 2026-06-24 | lease_membership::decide now bootstraps on can_bootstrap alone (the min-id tiebreak removed — it never prevented the multi-founder race it targeted and wrongly blocked a non-min founder). Unit test any_founder_bootstraps_with_itself_regardless_of_id_rank; integration guard durable_sessions::lease_group_forms_and_is_stable_under_slow_durable_commits now deliberately makes the founder the MAX raft id (previously hung at term 0). |
| 0026-T3 | ✅ done | 2026-06-24 | DRIVER_TICK 200ms -> 1s (durable_node.rs); RaftView.changing (membership().get_joint_config().len() > 1) makes decide() return None while a change is in joint consensus. Unit test a_leader_does_not_re_propose_while_a_change_is_in_flight; full durable_sessions suite (7/7) green under the slower tick. |
| 0026-T4 | ✅ done | 2026-06-24 | demo/durable.yml opt-in override (durable sessions + per-node /data volumes); lease_* / durable_append_* panels populate with a stable leader at rest (verified live — lease epoch flat once load is stopped). Kept opt-in, not default, because of the under-load churn finding below (T5). README + demo comments document the overlay. |
| 0026-T5 | ⬜ planned | — | "Promoted from deferred — T4 surfaced concrete evidence it is needed: a durable 3-node demo holds a stable leader AT REST, but under even light sustained QoS-1 load the lease epoch/term climbs slowly (session-log fsyncs contend with the lease raft's fsyncs, delaying heartbeats into election timeouts). openraft already batches AppendEntries so the marginal win is bounded; may also need to isolate the durable session-log I/O from the lease-group raft I/O." |
| 0026-T6 | ✅ done | 2026-06-24 | 0007 and 0018 delivery docs each carry a lease-timing↔storage-latency note pointing at ADR 0026 (and T5 for the under-load churn). |
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

## Under-load finding (T4 → T5)

Re-enabling durable in the demo (T4) produced a sharper picture than the at-rest validation.
**At rest** (loadgen stopped) the durable 3-node lease group is stable — the lease epoch goes
flat and the leader holds; the ADR 0026 timing fix works. **Under sustained client load** —
even the demo's gentle ~2–3 QoS-1 publishes/sec — the lease term/epoch climbs slowly: the
durable session-log fsyncs contend with the lease group's raft fsyncs, delaying heartbeats
past the follower election timeout, so the group keeps re-electing (the same node usually
re-wins, so `lease_leader` looks stable while the term churns). This is a *different, milder*
failure than the original (epoch 218 churn → now a slow climb), and it is contention, not the
raft-timing-vs-fsync mismatch T1 fixed. The fix lives in **T5** (coalesce raft log writes;
possibly isolate the session-log I/O from the lease raft). Until then durable is an **opt-in**
demo overlay (`demo/durable.yml`), not the default.

## Changelog

- **2026-06-24** — All of T1–T7 except T5 landed. T1 (timing) + T2 (fault-injection test, with
  the scope note above) shipped first. T3 (1s `DRIVER_TICK` + in-flight/joint-consensus guard
  in `decide`) and T7 (founder bootstraps on `can_bootstrap` alone — the min-id tiebreak
  removed; the durable test now uses a max-id founder as the regression guard) landed together.
  T4 re-enabled durable in the demo as an opt-in override and, in doing so, surfaced the
  under-load contention finding above — which promoted **T5** from deferred to planned with
  concrete evidence. T6 cross-referenced the constraint from the 0007/0018 delivery docs.
- **2026-06-24** — ADR accepted after bisecting a 3-node durable churn to fsync-on-commit
  latency vs in-memory-tuned raft timing (in-memory stable; persistent churns, worst on real
  disk).
