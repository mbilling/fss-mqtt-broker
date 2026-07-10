---
adr: "0042"
title: Durable-plane stress and simulation harness
adr_status: Proposed
tasks:
  - id: 0042-T1
    title: Invariant catalog — the durable plane's guarantees as executable checkers (acked durability, epoch fencing, session singularity, recovery honesty, retained convergence, bounded structures), shared with existing scenario tests
    status: done
    date: 2026-07-10
    evidence: "New mqtt_cluster::invariants module: the durable plane's guarantees stated once, as checkers reporting Violation lists (empty = holds; assert_holds panics with every detail). Ledger types observe events, pure functions check snapshots. The catalog: ACKED DURABILITY (AckLedger — record what was acknowledged: quorum-acked appends, acked truncations, acked removes; verify_recovered checks a takeover merge or restart read for presence with byte-identical records above the acked truncation floor, no resurrection at or below it, well-formed strictly-increasing offsets, and integrity at acked offsets; committed-but-unacked extras are legal in both directions — the contract is about promises). EPOCH FENCING (FenceLog — replica accept/refuse decisions in decision order; the one violation is ACCEPTING a stale epoch after a newer one was acknowledged for the group; refusals are always legal, per group never cross-group). LEASE MONOTONICITY (LeaseLog — minted epochs strictly increasing across all groups, the shared-counter contract that makes an epoch a fence token; verify_map pins the LeaseMap to the mint history: last assignment per group, high-water not behind). RETAINED TOKENS (TokenLog — per-topic strictly increasing (epoch, offset) applications, the ADR 0037 no-resurrection rule; check_retained_convergence compares node snapshots topic/token/payload in both directions). SESSION SINGULARITY (check_session_singularity — a client id live twice, cross-node or same-node, is the violation). RECOVERY HONESTY (check_recovery_honesty — DurableTruth x AttachReport: fabricating a clean session over a recoverable one and inventing a session are violations; a loud Unavailable refusal never is; Unknown truth constrains nothing — exactly ADR 0017). BOUNDED STRUCTURES (check_bound). Nine self-tests prove each checker CATCHES its violation shapes (loss, corruption, resurrection, disorder, stale acceptance, epoch reuse, map drift, non-monotonic tokens, divergent caches, doubled client, fabrication) and passes faithful data. Wired into three real scenario tests: the cluster_log SimCluster transport now records every reachable delivery decision into a FenceLog and stale_leader_is_fenced closes with assert_fencing_held() (the fencing invariant checked over EVERY decision the transport carried, not just the scripted one); a_durable_session_log_survives_a_full_restart_via_persisted_replicas verifies both the takeover merge and the served read against an AckLedger of the acked appends (real redb replicas, reopened from disk); a_takeover_recovers_the_retained_value_and_its_token — the exhibit-ledger test — feeds every applied token plus the post-takeover write through one TokenLog, proving no token reissue or regression across the takeover. Workspace green (807 tests), clippy zero warnings."
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
| 0042-T1 | ✅ done | 2026-07-10 | "New mqtt_cluster::invariants module: the durable plane's guarantees stated once, as checkers reporting Violation lists (empty = holds; assert_holds panics with every detail). Ledger types observe events, pure functions check snapshots. The catalog: ACKED DURABILITY (AckLedger — record what was acknowledged: quorum-acked appends, acked truncations, acked removes; verify_recovered checks a takeover merge or restart read for presence with byte-identical records above the acked truncation floor, no resurrection at or below it, well-formed strictly-increasing offsets, and integrity at acked offsets; committed-but-unacked extras are legal in both directions — the contract is about promises). EPOCH FENCING (FenceLog — replica accept/refuse decisions in decision order; the one violation is ACCEPTING a stale epoch after a newer one was acknowledged for the group; refusals are always legal, per group never cross-group). LEASE MONOTONICITY (LeaseLog — minted epochs strictly increasing across all groups, the shared-counter contract that makes an epoch a fence token; verify_map pins the LeaseMap to the mint history: last assignment per group, high-water not behind). RETAINED TOKENS (TokenLog — per-topic strictly increasing (epoch, offset) applications, the ADR 0037 no-resurrection rule; check_retained_convergence compares node snapshots topic/token/payload in both directions). SESSION SINGULARITY (check_session_singularity — a client id live twice, cross-node or same-node, is the violation). RECOVERY HONESTY (check_recovery_honesty — DurableTruth x AttachReport: fabricating a clean session over a recoverable one and inventing a session are violations; a loud Unavailable refusal never is; Unknown truth constrains nothing — exactly ADR 0017). BOUNDED STRUCTURES (check_bound). Nine self-tests prove each checker CATCHES its violation shapes (loss, corruption, resurrection, disorder, stale acceptance, epoch reuse, map drift, non-monotonic tokens, divergent caches, doubled client, fabrication) and passes faithful data. Wired into three real scenario tests: the cluster_log SimCluster transport now records every reachable delivery decision into a FenceLog and stale_leader_is_fenced closes with assert_fencing_held() (the fencing invariant checked over EVERY decision the transport carried, not just the scripted one); a_durable_session_log_survives_a_full_restart_via_persisted_replicas verifies both the takeover merge and the served read against an AckLedger of the acked appends (real redb replicas, reopened from disk); a_takeover_recovers_the_retained_value_and_its_token — the exhibit-ledger test — feeds every applied token plus the post-takeover write through one TokenLog, proving no token reissue or regression across the takeover. Workspace green (807 tests), clippy zero warnings." |
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

- **2026-07-10** — T1 (invariant catalog) landed: the durable plane's guarantees now
  exist once, as executable checkers (`mqtt_cluster::invariants`) — acked durability
  verified against recovery merges, epoch fencing over every replica decision, lease
  epoch monotonicity + map agreement, retained token monotonicity + cross-node
  convergence, session singularity, recovery honesty, bounded structures. Each checker
  is self-tested to *catch* its violation shapes, and three real scenario tests now
  close through the catalog — including the exhibit-ledger takeover test, whose token
  history is now checked end-to-end rather than at two scripted points.
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
