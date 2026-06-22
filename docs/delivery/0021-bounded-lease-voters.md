---
adr: "0021"
title: Bounded lease-consensus voter set
adr_status: Proposed
tasks:
  - id: 0021-T1
    title: MQTTD_LEASE_VOTERS config (default 5, odd; effective = min(N, live_eligible))
    status: planned
  - id: 0021-T2
    title: durable_node.rs - replace desired=all-members with alive set + RaftView passed to reconciler
    status: planned
  - id: 0021-T3
    title: Sticky vacancy-fill voter selection (promote lowest-id alive learner; never demote a live voter on join)
    status: planned
  - id: 0021-T4
    title: All members added as learners so the committed lease log replicates to every node
    status: planned
  - id: 0021-T5
    title: Reconciler reshape - decide returns target (voters, learners); apply_action adds/promotes/demotes-to-learner/drops-departed
    status: planned
  - id: 0021-T6
    title: Founder/bootstrap unaffected (sole-voter bootstrap then grows capped at N)
    status: planned
  - id: 0021-T7
    title: Pure policy tests (>N -> exactly N voters; dead voter replaced by lowest-id learner; high-id join no voter change; learner-owner reads lease; N>cluster all-voters; N=1 single voter)
    status: planned
  - id: 0021-T8
    title: Integration - 5+-node durable cluster with bounded voter set; learner-owned session survives a non-voter and a voter failure
    status: planned
  - id: 0021-T9
    title: Re-run openraft storage conformance (asserted unaffected)
    status: planned
---

# Delivery — ADR 0021: Bounded lease-consensus voter set

Decision: [docs/adr/0021-bounded-lease-voters.md](../adr/0021-bounded-lease-voters.md).

## Plan

The decision's numbered parts and implementation-notes workstream decompose into these
tasks. Each carries a stable id used by commits, tests, and the dashboard. The ADR is
**Proposed** (design only / awaiting ratification), so all tasks are `planned`.

| Task | Acceptance criterion |
|------|----------------------|
| **0021-T1** Config | `MQTTD_LEASE_VOTERS` (default `5`, recommend odd) bounds the voter set; effective voters = `min(N, live_eligible_count)`; quorum is `⌊N/2⌋+1` regardless of cluster size. |
| **0021-T2** durable_node wiring | `durable_node.rs` stops computing `desired = all members`; it passes the alive member set and the current `RaftView` (voters) to the reconciler so the sticky policy and `N` cap can apply. |
| **0021-T3** Sticky vacancy-fill | A live voter stays a voter; when live voters < `N`, promote the lowest-id alive learner(s) until `N` (or all live members); a departed voter is removed; a deterministic function of *(committed voter config, alive members)* so reconcilers agree. |
| **0021-T4** All-learners | Every eligible member is added as a learner so the committed lease log replicates to all; a learner that HRW makes an owner reads its lease epoch from that log without voting. |
| **0021-T5** Reconciler reshape | `decide` returns a target *(voters, learners)*; `apply_action` adds learners (blocking catch-up), `change_membership` promotes fills and demotes removed voters to learners (retain), drops departed members; quorum-safe via incremental `change_membership`. |
| **0021-T6** Founder/bootstrap | The founder bootstraps as sole voter; vacancy-fill grows the voter set to `N` as members join — same growth path, capped at `N`. |
| **0021-T7** Policy tests | Pure-where-possible tests: `>N` members → exactly `N` voters; dead voter replaced by lowest-id learner (count restored); high-id join → learner, no voter change; learner-owner reads/serves its lease; `N > cluster` → all-voters; `N = 1` → sane single voter. |
| **0021-T8** Integration | A 5+-node durable cluster forms with a bounded voter set and a learner-owned session survives a non-voter and a voter failure. |
| **0021-T9** Conformance | openraft's storage conformance suite re-runs and is asserted unaffected. |

## Progress

<!-- status-table:0021 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0021-T1 | ⬜ planned | — |  |
| 0021-T2 | ⬜ planned | — |  |
| 0021-T3 | ⬜ planned | — |  |
| 0021-T4 | ⬜ planned | — |  |
| 0021-T5 | ⬜ planned | — |  |
| 0021-T6 | ⬜ planned | — |  |
| 0021-T7 | ⬜ planned | — |  |
| 0021-T8 | ⬜ planned | — |  |
| 0021-T9 | ⬜ planned | — |  |
<!-- /status-table:0021 -->

## Changelog

- **2026-06-19** — Delivery doc opened from the Proposed (design-only) ADR; all tasks
  `planned` pending ratification.
