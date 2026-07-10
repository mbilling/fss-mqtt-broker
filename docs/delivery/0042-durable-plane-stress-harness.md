---
adr: "0042"
title: Durable-plane stress and simulation harness
adr_status: Proposed
tasks:
  - id: 0042-T1
    title: Invariant catalog — the durable plane's guarantees as executable checkers (acked durability, epoch fencing, session singularity, recovery honesty, retained convergence, bounded structures), shared with existing scenario tests
    status: planned
  - id: 0042-T2
    title: Deterministic simulation of the pure core — seeded many-seed schedules (reorder/duplicate/drop/interleave) over LeaseMap, cluster_log fencing/replay, retained token application, HRW placement; failing seed reruns identically
    status: planned
  - id: 0042-T3
    title: Seeded whole-cluster stress harness — in-process multi-node cluster under seed-composed fault schedules (kill/restart, partition/heal, frame delay/drop, client churn, takeover storms) with a seeded workload; post-quiesce invariant + convergence oracle; failure prints seed + schedule trace
    status: planned
  - id: 0042-T4
    title: Crash/restart/disk faults — process kill with surviving data dir, full-cluster stop/start recovery, disk-full and write-error injection (FlakyStore promoted to a shared fixture), brownout entry/exit mid-workload
    status: planned
  - id: 0042-T5
    title: Profiles + exhibits + closure — bounded CI profile on every push, env-tunable soak profile, exhibit ledger opened with the takeover flake (reproduced-and-fixed or explained), TEST-PLAN/docs updated, ADR acceptance
    status: planned
---

# Delivery — ADR 0042: Durable-plane stress and simulation harness

Decision: [docs/adr/0042-durable-plane-stress-harness.md](../adr/0042-durable-plane-stress-harness.md).

Pre-release area ④ (see ADR 0038's changelog for the four-area plan) — the last of the
four. The durable plane (lease consensus, quorum session-log replication, epoch-fenced
takeover, retained convergence tokens) is tested today by single-fault scenario tests
that synchronize on real time; one already flaked under load. This delivery states the
plane's invariants once as executable checkers, verifies them deterministically over the
pure state machines (many seeds, identical replay — extending ADR 0024's `swim_sim` to
the layer it deferred) and under seed-reproducible fault *schedules* over real in-process
nodes, then gates a bounded profile in CI with a soak profile behind env knobs.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0042-T1** Invariant catalog | The catalog exists as checker code (not prose) covering acked durability, epoch fencing, session singularity, recovery honesty (ADR 0017), retained convergence (ADR 0037), and ADR 0041 bounds; at least one existing scenario test asserts through it, proving the checkers run against real node state. |
| **0042-T2** Pure-core simulation | Seeded schedule generators drive `LeaseMap`, the `cluster_log` fencing/replay logic, and retained token application through reorder/duplicate/drop schedules across ≥1000 seeds each, asserting the catalog after every step; a violating seed panics with the seed and reruns the identical schedule (`REPRO_SEED` knob, `swim_sim` style, no new dependencies). |
| **0042-T3** Whole-cluster stress | One seed composes a fault schedule (kill/restart, partition/heal, frame delay/drop, client churn, takeover storms) and a workload over an in-process multi-node cluster; after quiesce+heal the full catalog plus convergence holds; a failure prints the seed and schedule trace. Green across the CI seed budget. |
| **0042-T4** Crash/restart/disk | Schedules include hard process kill with the redb data dir surviving into restart, full-cluster stop/start, disk-full/write-error injection at the storage seam (shared `FlakyStore` fixture), and brownout entry/exit mid-workload — with acked data present, fencing intact, recovery honest, and nothing resurrected afterwards. |
| **0042-T5** Profiles + closure | The bounded CI profile runs on every push inside the suite's runtime budget; `MQTTD_SIM_*` env knobs select the soak profile (more seeds, longer schedules); the exhibit ledger opens with the `a_takeover_recovers_the_retained_value_and_its_token` flake — reproduced under the harness and fixed (seed as regression test) or explained and recorded; TEST-PLAN.md updated; ADR 0042 flips to Accepted. |

## Progress

<!-- status-table:0042 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0042-T1 | ⬜ planned | — |  |
| 0042-T2 | ⬜ planned | — |  |
| 0042-T3 | ⬜ planned | — |  |
| 0042-T4 | ⬜ planned | — |  |
| 0042-T5 | ⬜ planned | — |  |
<!-- /status-table:0042 -->

## Exhibit ledger

Known load-dependent flakes in the durable plane, tracked as harness inputs (ADR §6):

| # | Exhibit | Source | Status |
|---|---------|--------|--------|
| 1 | `a_takeover_recovers_the_retained_value_and_its_token` (`mqtt-cluster/src/cluster_store.rs`) failed once under full-workspace parallel load; green 5/5 isolated + on rerun | Recorded in [0041-T1 evidence](0041-resource-governance.md) (2026-07-06) | open — T3/T5 target |

## Changelog

- **2026-07-10** — ADR proposed and delivery opened. Scope fixed by a survey of the
  existing verification surface: the durable plane is covered by single-fault scenario
  tests (`cluster.rs`, `cluster_chaos.rs`, `durable_sessions.rs`, `persistence.rs`,
  in-crate `mqtt-cluster` tests) that script one fault at one point and synchronize on
  real time; ADR 0024's deterministic harness reached SWIM (`swim_sim.rs`) and recorded
  the lease/replication layer as deferred ("async-I/O-entangled — the natural extension
  once a seam exists"); the plane's invariants are asserted implicitly and scattered,
  never as one executable catalog; and one load-dependent flake in exactly this layer is
  already on record (the exhibit ledger above). Ordering: T1 (the oracle) → T2 (pure
  core, deterministic) → T3 (whole-cluster stress) → T4 (crash/disk vocabulary) → T5
  (CI/soak profiles + exhibits + closure).
