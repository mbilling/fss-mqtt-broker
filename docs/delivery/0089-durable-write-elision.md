---
adr: "0089"
title: "Write elision on the durable QoS 1 path: a message the subscriber has acknowledged need never be written"
adr_status: Accepted
tasks:
  - id: 0089-T1
    title: "Characterisation first: hub tests that pin today's order (durable before the wire) and the obligation ledger, and a cluster_proc schedule that kills a session's owner with deliveries in flight; the tests D1-D5 will flip"
    status: in-progress
    issue: 1068
    notes: "Size M. hub/lanes.rs, hub/forwarding.rs, hub/delivery.rs tests; proc_common (a kill aimed at an owner with an online persistent subscriber under load). No behaviour change. First piece: hub::tests::a_durable_qos1_delivery_reaches_the_wire_only_after_its_append (an append held by FlakyStore::hold_enqueue keeps the live PUBLISH off the wire and the publisher unanswered; released, both proceed and the copy stays until the PUBACK). The crash half is inflight_durability::an_acked_qos1_in_flight_to_an_online_subscriber_survives_a_crash (#124), which T2 must keep green with elision on."
  - id: 0089-T2
    title: "D1-D5 on one node behind durable.write_elision_ms (default 0): eligibility, the held window, the fallback to today's append, the per-session head rule, fan-out over several owed sessions"
    status: in-progress
    issue: 1068
    notes: "Size L. The obligation ledger gains a second discharge (the subscriber's PUBACK); every path that settles a publish must respect it. mqtt-config (the key), CONFIGURATION. First cut: hub/elision.rs; durable.write_elision_ms / MQTTD_DURABLE_WRITE_ELISION_MS (default 0, a startup WARN when set). Eligible: QoS 1, a local publish or a peer's first forward, the session online on its owner with nothing durable owed, a packet id and a send slot free, no brownout, drain or open takeover, room in the bound (16,384 entries, 64 MiB). A session's held deliveries fall back together in send order (timer, Detach, brownout, drain); the fallback append carries the in-flight packet id, so it never resends and links its offset for the late PUBACK. Tests: three hub tests (an acknowledged delivery is never written; an unacknowledged one is written at its window end and truncated by the late PUBACK; held deliveries fall back and replay in order), inflight_durability's #124 crash test with elision on, cluster_proc (7/7, 4 seeds) with elision on. Local durable_bench, release, 3 nodes on one Mac, same build, 16 publishers x window 8, 16 subscribers: qos1-durable-owner 4,068 -> 36,338 msg/s (8.9x), relay 4,347 -> 34,177 (7.9x), p50 30.7 -> 0.16 ms; p95/p99 unchanged near 40/47 ms (the share still written); clean-session 141k either way. Direction only: a laptop disk (F_FULLFSYNC ~30 ms) exaggerates the write's cost against the rig's. Open: a metric for discharged and fallback counts (T4), the cluster cases (T3)."
  - id: 0089-T3
    title: "D6-D7 in a cluster: the owner decides, a forwarded publish is answered when discharged there, held forwards and ADR 0087 holds stay ineligible; the per-node bound"
    status: planned
    issue: 1068
    notes: "Size M. hub/forwarding.rs verdicts, the #1053 hold, the ingress-credit style bound. cluster_stress and false_dead_delivery with elision on."
  - id: 0089-T4
    title: "D9: metrics, /statusz, OPERATIONS and the durability docs (what an acknowledgement means with elision on)"
    status: planned
    issue: 1068
    notes: "Size S-M. mqtt-observability, health.rs, OPERATIONS, README durability wording, THREAT-MODEL row."
  - id: 0089-T5
    title: "The evidence and the default: the rig A/B of 'What counts as done' (keeping-up, lagging and offline consumers; redb and the segment log; one provisioning), the fault suites with elision on, then a decision on the default"
    status: planned
    issue: 1068
    notes: "Size M plus rig time. A bench/scale card. The default changes only on all four criteria."
---

# Delivery 0089 — write elision on the durable QoS 1 path

**ADR:** [docs/adr/0089-durable-write-elision.md](../adr/0089-durable-write-elision.md)

The plan, progress, and changelog for ADR 0089. Task status lives in the frontmatter
above; the table below is generated from it.

## Status

<!-- status-table:0089 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0089-T1 | 🚧 in-progress | [#1068](https://github.com/mbilling/fss-mqtt-broker/issues/1068) | — | "Size M. hub/lanes.rs, hub/forwarding.rs, hub/delivery.rs tests; proc_common (a kill aimed at an owner with an online persistent subscriber under load). No behaviour change. First piece: hub::tests::a_durable_qos1_delivery_reaches_the_wire_only_after_its_append (an append held by FlakyStore::hold_enqueue keeps the live PUBLISH off the wire and the publisher unanswered; released, both proceed and the copy stays until the PUBACK). The crash half is inflight_durability::an_acked_qos1_in_flight_to_an_online_subscriber_survives_a_crash (#124), which T2 must keep green with elision on." |
| 0089-T2 | 🚧 in-progress | [#1068](https://github.com/mbilling/fss-mqtt-broker/issues/1068) | — | "Size L. The obligation ledger gains a second discharge (the subscriber's PUBACK); every path that settles a publish must respect it. mqtt-config (the key), CONFIGURATION. First cut: hub/elision.rs; durable.write_elision_ms / MQTTD_DURABLE_WRITE_ELISION_MS (default 0, a startup WARN when set). Eligible: QoS 1, a local publish or a peer's first forward, the session online on its owner with nothing durable owed, a packet id and a send slot free, no brownout, drain or open takeover, room in the bound (16,384 entries, 64 MiB). A session's held deliveries fall back together in send order (timer, Detach, brownout, drain); the fallback append carries the in-flight packet id, so it never resends and links its offset for the late PUBACK. Tests: three hub tests (an acknowledged delivery is never written; an unacknowledged one is written at its window end and truncated by the late PUBACK; held deliveries fall back and replay in order), inflight_durability's #124 crash test with elision on, cluster_proc (7/7, 4 seeds) with elision on. Local durable_bench, release, 3 nodes on one Mac, same build, 16 publishers x window 8, 16 subscribers: qos1-durable-owner 4,068 -> 36,338 msg/s (8.9x), relay 4,347 -> 34,177 (7.9x), p50 30.7 -> 0.16 ms; p95/p99 unchanged near 40/47 ms (the share still written); clean-session 141k either way. Direction only: a laptop disk (F_FULLFSYNC ~30 ms) exaggerates the write's cost against the rig's. Open: a metric for discharged and fallback counts (T4), the cluster cases (T3)." |
| 0089-T3 | ⬜ planned | [#1068](https://github.com/mbilling/fss-mqtt-broker/issues/1068) | — | "Size M. hub/forwarding.rs verdicts, the #1053 hold, the ingress-credit style bound. cluster_stress and false_dead_delivery with elision on." |
| 0089-T4 | ⬜ planned | [#1068](https://github.com/mbilling/fss-mqtt-broker/issues/1068) | — | "Size S-M. mqtt-observability, health.rs, OPERATIONS, README durability wording, THREAT-MODEL row." |
| 0089-T5 | ⬜ planned | [#1068](https://github.com/mbilling/fss-mqtt-broker/issues/1068) | — | "Size M plus rig time. A bench/scale card. The default changes only on all four criteria." |
<!-- /status-table:0089 -->
