---
adr: "0059"
title: "Bridge HA topology and message ordering"
adr_status: Proposed
tasks:
  - id: 0059-T1
    title: "Red tests: 2 instances + an `in` rule deliver each message twice; a single publisher's per-topic order is not preserved under `$share` HA"
    status: planned
  - id: 0059-T2
    title: "Instance identity (`MQTTD_BRIDGE_INSTANCE`/`_TOTAL`) + pure `owns(topic, N, k)` ownership function with `partition_key` config"
    status: planned
  - id: 0059-T3
    title: "Partitioned forwarding: every instance subscribes the full filter both sides, drops non-owned topics at the forward step; inbound delivered exactly once, per-topic order preserved (green)"
    status: planned
  - id: 0059-T4
    title: "`ha = partitioned | shared | active-passive` per-rule/global; default `partitioned`; `shared` keeps the `$share` local optimisation with ordering explicitly forfeited"
    status: planned
  - id: 0059-T5
    title: "Optional active/passive mode (liveness signal, whole-key-space takeover) for small fleets"
    status: planned
  - id: 0059-T6
    title: "Docs: bridge HA/ordering guarantees per mode; ADR 0025 §5 Consequences amendment cross-link"
    status: planned
---

# Delivery — ADR 0059

> **Generated** progress table is produced by `scripts/gen-status.py`. This file holds the
> plan and its frontmatter; the dashboard renders from the task list above.

<!-- status-table:0059 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0059-T1 | ⬜ planned | — |  |
| 0059-T2 | ⬜ planned | — |  |
| 0059-T3 | ⬜ planned | — |  |
| 0059-T4 | ⬜ planned | — |  |
| 0059-T5 | ⬜ planned | — |  |
| 0059-T6 | ⬜ planned | — |  |
<!-- /status-table:0059 -->

Closes the HA/ordering half of the bridge audit (epic #186, findings #3 + #8, issue #187).
Design in [ADR 0059](../adr/0059-bridge-ha-topology-and-ordering.md). Built test-first: T1 is
the red pair that reproduces inbound double-delivery and ordering loss; T3 turns them green.
