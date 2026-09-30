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
    status: done
    issue: 702
    date: 2026-09-30
    evidence: "PR #740. R=2 validated on redb and the log store: cluster 6, durable_sessions 13, cluster_stress 13, cluster_proc 5, cluster_chaos 6, decommission 3, inflight_durability 3, persistence 2, backup_restore 8 — plus default suites and cluster_upgrade 2/2. Harnesses found at MQTTD_REPLICAS and readiness requires every node to report the founded factor. Two tests that assumed R=3 on three nodes made R-agnostic. Found and fixed #727: a caught-up stamp outlived the node's membership of the group's set, so a re-entering node could answer a recovery read 'complete' for history it never received (acked data lost once in ~30 runs); the stamp is now cleared on leaving a set and an adopted factor arms the sweep (0 complete-but-empty reads in 198 recoveries vs ~1 in 9; regression test fails with either half removed). Write pause measured (replication_pause.rs): R=2 pauses more groups than R=3 (38 vs 30 of 48 on a crash) for about as long (~15 s, failover — #738); ADR amended."
  - id: 0080-T4
    title: "Default replication factor 2 for new clusters"
    status: done
    issue: 703
    date: 2026-09-30
    evidence: "PR #742. durable.replicas / MQTTD_REPLICAS defaults to 2 for NEW clusters; existing clusters have no committed factor and keep 3 (moving them is T5). Example config, COMPARISON (quorum R=3 -> 2 copies, both ack, configurable 2-7) and CONFIGURATION.md follow. Validated at the new default with no MQTTD_REPLICAS set, both stores: cluster, durable_sessions, cluster_stress, cluster_proc, cluster_chaos, decommission, inflight_durability, persistence, backup_restore, check_config all pass; cluster_upgrade --include-ignored 2/2 (a baseline-founded cluster keeps its legacy 3)."
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
| 0080-T3 | ✅ done | [#702](https://github.com/mbilling/fss-mqtt-broker/issues/702) | 2026-09-30 | "PR #740. R=2 validated on redb and the log store: cluster 6, durable_sessions 13, cluster_stress 13, cluster_proc 5, cluster_chaos 6, decommission 3, inflight_durability 3, persistence 2, backup_restore 8 — plus default suites and cluster_upgrade 2/2. Harnesses found at MQTTD_REPLICAS and readiness requires every node to report the founded factor. Two tests that assumed R=3 on three nodes made R-agnostic. Found and fixed #727: a caught-up stamp outlived the node's membership of the group's set, so a re-entering node could answer a recovery read 'complete' for history it never received (acked data lost once in ~30 runs); the stamp is now cleared on leaving a set and an adopted factor arms the sweep (0 complete-but-empty reads in 198 recoveries vs ~1 in 9; regression test fails with either half removed). Write pause measured (replication_pause.rs): R=2 pauses more groups than R=3 (38 vs 30 of 48 on a crash) for about as long (~15 s, failover — #738); ADR amended." |
| 0080-T4 | ✅ done | [#703](https://github.com/mbilling/fss-mqtt-broker/issues/703) | 2026-09-30 | "PR #742. durable.replicas / MQTTD_REPLICAS defaults to 2 for NEW clusters; existing clusters have no committed factor and keep 3 (moving them is T5). Example config, COMPARISON (quorum R=3 -> 2 copies, both ack, configurable 2-7) and CONFIGURATION.md follow. Validated at the new default with no MQTTD_REPLICAS set, both stores: cluster, durable_sessions, cluster_stress, cluster_proc, cluster_chaos, decommission, inflight_durability, persistence, backup_restore, check_config all pass; cluster_upgrade --include-ignored 2/2 (a baseline-founded cluster keeps its legacy 3)." |
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
- 2026-09-30: T3 done (#740): R=2 validated; #727 found and fixed; the write pause measured and the ADR amended.
- 2026-09-30: T4 done (#742): new clusters default to 2 replicas.
