---
adr: "0059"
title: "Bridge HA topology and message ordering"
adr_status: Proposed
tasks:
  - id: 0059-T1
    title: "Test: 2 partitioned instances + an `in` rule deliver each message exactly once (was: twice)"
    status: done
    date: 2026-08-11
    evidence: "crates/mqtt-bridge/tests/engine.rs — two_partitioned_instances_deliver_each_inbound_message_exactly_once: 2 instances (total=2, instance 0/1), an `in` rule, 30 topics; a local subscriber receives each payload exactly once (no double-deliver, no gap). Diagnosed and fixed a same-process client-id collision in the test (distinct upstream client_id per instance mirrors distinct pod hostnames)."
  - id: 0059-T2
    title: "Instance identity (`MQTTD_BRIDGE_INSTANCE`/`_TOTAL`, pod-ordinal fallback) + pure `owns(topic, total, instance)` ownership fn"
    status: done
    date: 2026-08-11
    evidence: "forward.rs owns(topic,total,instance) = total<=1 || fnv1a(topic)%total==instance, with unit tests ownership_partitions_every_topic_to_exactly_one_instance and ownership_spreads_load_across_instances. main.rs resolves instance from MQTTD_BRIDGE_INSTANCE, else the HOSTNAME pod ordinal (hostname_ordinal); total from MQTTD_BRIDGE_TOTAL; re-validated (instance<total)."
  - id: 0059-T3
    title: "Partitioned forwarding: every instance subscribes the full filter plain both sides, drops non-owned topics at the forward step"
    status: done
    date: 2026-08-11
    evidence: "engine.rs router skips a message when ha==Partitioned && !owns(topic,total,instance). local_subscriptions is plain under Partitioned (only $share under Shared). Inbound delivered exactly once, per-topic ownership preserves order (one owner = one ordered stream)."
  - id: 0059-T4
    title: "`ha = partitioned | shared`, default `partitioned`; `shared` keeps the `$share` local optimisation with ordering forfeited"
    status: done
    date: 2026-08-11
    evidence: "config.rs HaMode { Partitioned (default), Shared }; instance/total fields with validation (total>=1, instance<total). Default flip is a pre-1.0 change; existing shared tests updated to set ha=\"shared\" explicitly."
  - id: 0059-T5
    title: "Optional active/passive mode (liveness signal, whole-key-space takeover) for small fleets"
    status: planned
  - id: 0059-T6
    title: "Docs: bridge HA/ordering guarantees per mode; Helm wiring (`MQTTD_BRIDGE_TOTAL` from replicaCount)"
    status: done
    date: 2026-08-11
    evidence: "docs/BRIDGE.md HA section leads with partitioned (default) and documents shared as the opt-in with its ordering/inbound caveat; Helm bridge.yaml sets MQTTD_BRIDGE_TOTAL from replicaCount and the bridge derives instance from the pod ordinal."
---

# Delivery — ADR 0059

> **Generated** progress table is produced by `scripts/gen-status.py`. This file holds the
> plan and its frontmatter; the dashboard renders from the task list above.

<!-- status-table:0059 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0059-T1 | ✅ done | 2026-08-11 | "crates/mqtt-bridge/tests/engine.rs — two_partitioned_instances_deliver_each_inbound_message_exactly_once: 2 instances (total=2, instance 0/1), an `in` rule, 30 topics; a local subscriber receives each payload exactly once (no double-deliver, no gap). Diagnosed and fixed a same-process client-id collision in the test (distinct upstream client_id per instance mirrors distinct pod hostnames)." |
| 0059-T2 | ✅ done | 2026-08-11 | "forward.rs owns(topic,total,instance) = total<=1 || fnv1a(topic)%total==instance, with unit tests ownership_partitions_every_topic_to_exactly_one_instance and ownership_spreads_load_across_instances. main.rs resolves instance from MQTTD_BRIDGE_INSTANCE, else the HOSTNAME pod ordinal (hostname_ordinal); total from MQTTD_BRIDGE_TOTAL; re-validated (instance<total)." |
| 0059-T3 | ✅ done | 2026-08-11 | "engine.rs router skips a message when ha==Partitioned && !owns(topic,total,instance). local_subscriptions is plain under Partitioned (only $share under Shared). Inbound delivered exactly once, per-topic ownership preserves order (one owner = one ordered stream)." |
| 0059-T4 | ✅ done | 2026-08-11 | "config.rs HaMode { Partitioned (default), Shared }; instance/total fields with validation (total>=1, instance<total). Default flip is a pre-1.0 change; existing shared tests updated to set ha=\"shared\" explicitly." |
| 0059-T5 | ⬜ planned | — |  |
| 0059-T6 | ✅ done | 2026-08-11 | "docs/BRIDGE.md HA section leads with partitioned (default) and documents shared as the opt-in with its ordering/inbound caveat; Helm bridge.yaml sets MQTTD_BRIDGE_TOTAL from replicaCount and the bridge derives instance from the pod ordinal." |
<!-- /status-table:0059 -->

Closes the HA/ordering half of the bridge audit (epic #186, findings #3 + #8, issue #187).
Design in [ADR 0059](../adr/0059-bridge-ha-topology-and-ordering.md). Built test-first: T1 is
the red pair that reproduces inbound double-delivery and ordering loss; T3 turns them green.
