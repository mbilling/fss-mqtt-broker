---
adr: "0080"
title: "The replication factor is cluster state: configurable, default 2, changeable live"
adr_status: Accepted
tasks:
  - id: 0080-T1
    title: "R is lease state: SetReplicas command, capability gate (proto 9), updatable placement; unset means 3"
    status: planned
    issue: 700
    notes: "No behaviour change on its own: every existing cluster has no committed value and stays at R=3."
  - id: 0080-T2
    title: "durable.replicas / MQTTD_REPLICAS; the founder commits it before ready; min_replicas validated against the committed R"
    status: planned
    issue: 701
    notes: "Default 3 until T4. Startup log, /statusz, CONFIGURATION.md and mqttd.example.toml stop hard-coding R=3."
  - id: 0080-T3
    title: "R=2 validated: durability, crash, chaos and failover suites on both stores; the write pause measured"
    status: planned
    issue: 702
    notes: "Measure how long durable writes to the affected groups stop when one of two replicas is killed, and when it is only suspended."
  - id: 0080-T4
    title: "Default replication factor 2 for new clusters"
    status: planned
    issue: 703
    notes: "After T3. Existing clusters keep R=3."
  - id: 0080-T5
    title: "Change R on a running cluster: joint phase, catch-up, switch, collect"
    status: planned
    issue: 704
    notes: "Config reload or admin API proposes; refused below R' eligible nodes or while a change runs; progress on /statusz."
  - id: 0080-T6
    title: "Evidence: R=2 against R=3 on the calibration shape (paid)"
    status: planned
    issue: 705
    notes: "3 × CCX23, durable QoS 1, log store, one provisioning. Expected ~1.5x the durable ceiling at N=3."
---

# Delivery: ADR 0080 — the replication factor is cluster state

[ADR 0080](../adr/0080-replication-factor.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0080 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0080-T1 | ⬜ planned | [#700](https://github.com/mbilling/fss-mqtt-broker/issues/700) | — | "No behaviour change on its own: every existing cluster has no committed value and stays at R=3." |
| 0080-T2 | ⬜ planned | [#701](https://github.com/mbilling/fss-mqtt-broker/issues/701) | — | "Default 3 until T4. Startup log, /statusz, CONFIGURATION.md and mqttd.example.toml stop hard-coding R=3." |
| 0080-T3 | ⬜ planned | [#702](https://github.com/mbilling/fss-mqtt-broker/issues/702) | — | "Measure how long durable writes to the affected groups stop when one of two replicas is killed, and when it is only suspended." |
| 0080-T4 | ⬜ planned | [#703](https://github.com/mbilling/fss-mqtt-broker/issues/703) | — | "After T3. Existing clusters keep R=3." |
| 0080-T5 | ⬜ planned | [#704](https://github.com/mbilling/fss-mqtt-broker/issues/704) | — | "Config reload or admin API proposes; refused below R' eligible nodes or while a change runs; progress on /statusz." |
| 0080-T6 | ⬜ planned | [#705](https://github.com/mbilling/fss-mqtt-broker/issues/705) | — | "3 × CCX23, durable QoS 1, log store, one provisioning. Expected ~1.5x the durable ceiling at N=3." |
<!-- /status-table:0080 -->

## Plan

1. **T1, T2: configurable R with no behaviour change.** R moves from a constant into the
   lease state machine; the setting exists and defaults to 3.
2. **T3: validate R = 2.** The durability and failover suites at R = 2 on both stores, and
   the write pause during a replica failure measured, so the trade the ADR states has a
   number.
3. **T4: the default becomes 2** for new clusters.
4. **T5: live change**, so an operator can move an existing cluster between R values
   without a restart.
5. **T6: the paid evidence**, R = 2 against R = 3 on one provisioning.

## Changelog

- 2026-09-29: ADR accepted; tasks and issues #700–#705 filed.
