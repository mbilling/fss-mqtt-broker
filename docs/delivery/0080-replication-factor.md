---
adr: "0080"
title: "The replication factor is cluster state: configurable, default 2, changeable live"
adr_status: Accepted
tasks:
  - id: 0080-T1
    title: "R is lease state: SetReplicas command, capability gate (proto 9), updatable placement; unset means 3"
    status: done
    issue: 700
    date: 2026-09-29
    evidence: "PR #709. lease_raft: ReplicationRecord (committed factor + open change) and SetReplicas / BeginReplicaChange / CommitReplicaChange with deterministic no-op preconditions; unset reads as 3. encode_state/decode_state at all four LeaseStore sites: byte-identical to the pre-0080 table while unset, extended once set, strict dual decode. Placement::set_replicas (memo invalidated, owners unmoved), pushed from the durable tick. Peer proto 9 capability marker; hub replication_capable flag. Tests: founding/joint preconditions, byte-identity + fail-closed decode (mutation-checked), reopen + snapshot install, 3->2 reshape keeps owners (mutation-checked), proto-9 flag. cluster_upgrade --include-ignored 2/2 (baseline + proto-9 mesh under acked durable load)."
  - id: 0080-T2
    title: "durable.replicas / MQTTD_REPLICAS; the founder commits it before ready; min_replicas validated against the committed R"
    status: done
    issue: 701
    date: 2026-09-29
    evidence: "PR #725. durable.replicas / MQTTD_REPLICAS (2..=7, default 3 until T4); min_replicas validated against it (config + --check-config). lease_assign::Founding: a fresh cluster's leader commits SetReplicas before its first AssignMany (the entry mints no epoch), holds up to 10 s for the hub's proto-9 flag (now shared from main.rs), then founds without a factor (legacy 3) with a warning; a cluster that already minted leases is never founded. The durable tick adopts the committed factor BEFORE pushing lease owners. Warnings: node setting != committed factor (once), absolute floor above an adopted factor, even factor > 2. Tests: founding before any lease, hold-then-fallback, no founding of an existing cluster, config bounds + env, and a real persistent node founded at 2 that stays at 2 after a restart configured for 3. cluster_upgrade --include-ignored 2/2."
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
| 0080-T1 | ✅ done | [#700](https://github.com/mbilling/fss-mqtt-broker/issues/700) | 2026-09-29 | "PR #709. lease_raft: ReplicationRecord (committed factor + open change) and SetReplicas / BeginReplicaChange / CommitReplicaChange with deterministic no-op preconditions; unset reads as 3. encode_state/decode_state at all four LeaseStore sites: byte-identical to the pre-0080 table while unset, extended once set, strict dual decode. Placement::set_replicas (memo invalidated, owners unmoved), pushed from the durable tick. Peer proto 9 capability marker; hub replication_capable flag. Tests: founding/joint preconditions, byte-identity + fail-closed decode (mutation-checked), reopen + snapshot install, 3->2 reshape keeps owners (mutation-checked), proto-9 flag. cluster_upgrade --include-ignored 2/2 (baseline + proto-9 mesh under acked durable load)." |
| 0080-T2 | ✅ done | [#701](https://github.com/mbilling/fss-mqtt-broker/issues/701) | 2026-09-29 | "PR #725. durable.replicas / MQTTD_REPLICAS (2..=7, default 3 until T4); min_replicas validated against it (config + --check-config). lease_assign::Founding: a fresh cluster's leader commits SetReplicas before its first AssignMany (the entry mints no epoch), holds up to 10 s for the hub's proto-9 flag (now shared from main.rs), then founds without a factor (legacy 3) with a warning; a cluster that already minted leases is never founded. The durable tick adopts the committed factor BEFORE pushing lease owners. Warnings: node setting != committed factor (once), absolute floor above an adopted factor, even factor > 2. Tests: founding before any lease, hold-then-fallback, no founding of an existing cluster, config bounds + env, and a real persistent node founded at 2 that stays at 2 after a restart configured for 3. cluster_upgrade --include-ignored 2/2." |
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
- 2026-09-29: T1 done (#709): the replication factor is lease state; no behaviour change.
- 2026-09-29: T2 done (#725): durable.replicas founds a new cluster's factor; default still 3.
