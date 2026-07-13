---
adr: "0043"
title: Elastic cluster resize (grow, shrink, replace)
adr_status: Proposed
tasks:
  - id: 0043-P1
    title: Catch-up replicas — a node entering a group's replica set back-fills the group's log (ReplicaKeys discovery + recover_key quorum reads) behind a durable caught-up watermark, and counts toward NO quorum (append or recovery) until caught up; growing 1→N back-fills laptop-mode single-replica history as the same rule
    status: planned
  - id: 0043-P2
    title: Eager migration on ring change — membership growth triggers the takeover-scan materialization (sessions, retained, interest) for groups whose owner moved, instead of first-touch; the settle/re-route + mesh-whole ack machinery holds acks honest during the window
    status: planned
  - id: 0043-P3
    title: Decommission — an explicit drain (stop new sessions → successors caught up + replica counts restored among remaining members → voter demotion → ADR 0019 leave), observable via the health endpoint, interruptible (crash mid-drain = crash); operator trigger chosen here (signal vs admin endpoint)
    status: planned
  - id: 0043-P4
    title: Resize test vocabulary — join/decommission steps in the ADR 0042 stress schedules, plus dedicated upgrade-path tests (1→3 laptop-to-server, 3→5 zone-spread, 5→3 cost reduction, rolling host replacement, rolling binary upgrade across the proto window) under the unchanged acked-obligations oracle
    status: planned
  - id: 0043-P5
    title: Operator docs — the "grow your broker" guide (one paragraph per direction), the two-node-waypoint honesty note (quorum 2-of-2; recommend 1→3), README interim warning that durable resize is unsupported until P1 lands
    status: planned
---

# Delivery: ADR 0043 — Elastic cluster resize

[ADR 0043](../adr/0043-elastic-cluster-resize.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0043 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0043-P1 | ⬜ planned | — |  |
| 0043-P2 | ⬜ planned | — |  |
| 0043-P3 | ⬜ planned | — |  |
| 0043-P4 | ⬜ planned | — |  |
| 0043-P5 | ⬜ planned | — |  |
<!-- /status-table:0043 -->

## Plan

| Task | Done means |
|---|---|
| **0043-P1** Catch-up replicas | A replica-set joiner back-fills before counting toward any quorum, behind a durable watermark that survives restart; the hollow-replica recovery hazard (a "quorum" of {empty joiner, lagging survivor} silently truncating) is closed by construction; growing a 1-node broker re-replicates its history. Unit tests at the cluster_log/cluster_store seams + a stress-harness join under load. |
| **0043-P2** Eager migration | A ring change materializes moved groups (sessions, retained, interest) without waiting for first touch — the ADR 0042 T9 takeover scan generalized to membership growth; acks stay honest through the window via the existing settle/re-route + mesh-whole rules. |
| **0043-P3** Decommission | An operator-triggered drain completes with every owned group handed to a caught-up successor and every replicated group at full replica count among the survivors, then demotes and leaves; progress observable, crash-safe at every point. |
| **0043-P4** Resize vocabulary | The stress harness composes join/decommission into seeded schedules; dedicated tests drive 1→3, 3→5 (zone-spread), 5→3, rolling replacement, and a rolling binary upgrade; all hold the acked-obligations oracle with zero waivers. |
| **0043-P5** Operator docs | The grow/shrink/replace guide ships with the two-node honesty note; the README carries an interim "durable resize unsupported" warning until P1 lands, removed by this task. |

Order: P1 → P2 → P3 (each unblocks the next), P4 grows alongside each, P5 last.

## Exhibits / findings ledger

| # | Finding | Where | Status |
|---|---|---|---|
| — | 2026-07-13 inventory: consensus-plane resize built and unit-tested (ADR 0016/0021/0026/0028); data-plane resize missing — hollow replicas count toward quorum, ownership moves are first-touch, graceful leave hands off nothing, laptop-mode history never re-replicates; no integration test resizes a running durable cluster | code/test survey (see ADR context) | open — this ADR is the plan |

## Changelog

- **2026-07-13** — ADR 0043 drafted with delivery plan P1–P5, from the cluster-resize
  inventory (consensus half ready, data half missing). Motivated by the capability
  plan's "adding a node adds throughput" claim and the laptop→server upgrade sell.
