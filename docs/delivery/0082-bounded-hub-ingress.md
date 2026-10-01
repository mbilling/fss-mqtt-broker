---
adr: "0082"
title: "Bounded hub ingress: a control lane that never waits behind data, and byte credits that push back on publishers"
adr_status: Accepted
tasks:
  - id: 0082-T1
    title: "Measure and expose: hub queue depth and bytes per class, the per-command memory cost, and the overload harness"
    status: planned
    issue: 814
    notes: "The 2026-10-01 re-run measured 2,553,413 queued commands at 2.60 GB RSS (about 1,019 B each at 200 B payloads). The harness is the before / overload / idle / control sequence #504's acceptance names, asserting RSS, /livez and the control rung."
  - id: 0082-T2
    title: "Split the hub command channel: a control lane drained first, a data lane for publishes"
    status: done
    issue: 815
    date: 2026-10-02
    evidence: "PR #821. The hub sorts arrivals into a control lane (completions and acks, the durable plane, Ping, admin) dispatched before the data lane; everything earlier data can change stays ordered on data (ADR amendment 2026-10-02). HubCommand::lane is an exhaustive match; Flush is the data-lane barrier; mqttd_hub_lane_depth{lane}. A ping behind 100,000 publishes is reached after <=98 data dispatches (with one FIFO, 100,000; mutation-proven); lib 517, cluster_stress, durable_sessions, cluster, admin, binary_smoke pass."
  - id: 0082-T3
    title: "Client ingress credits: a global byte pool plus a per-connection cap; a connection over its credit stops reading its socket"
    status: in-progress
    issue: 816
    notes: "The permit travels inside the command and is released when the hub drops it. The keepalive deadline does not run during broker-imposed pauses. QoS 1/2 semantics and Receive Maximum are unchanged. Implements MQTTD_INGRESS_OVERLOAD=pause|shed-qos0 (default pause; QoS 1/2 always pause), decided 2026-10-02. Charge = topic + payload + 800 B, from #504's measured 1,019 B per queued command, until T1 measures it directly. Brings forward T5's three settings with CONFIGURATION.md, SIZING.md and the example TOML."
  - id: 0082-T4
    title: "Peer ingress: shed remote QoS 0 data past the credit (counted); never pause a peer link"
    status: planned
    issue: 817
    notes: "publish_dropped{reason=hub-ingress}. Remote QoS >= 1 is uncharged (the origin's pending table bounds it). Control and durable frames are never charged."
  - id: 0082-T5
    title: "Configuration, defaults and documentation: MQTTD_HUB_INGRESS_BYTES and MQTTD_CONN_INGRESS_BYTES, SIZING, OPERATIONS and metrics"
    status: planned
    issue: 818
    notes: "Also MQTTD_INGRESS_OVERLOAD ([limits] ingress_overload) in mqtt-config and the docs, with the pause versus shed-qos0 trade-off stated. Defaults accepted 2026-10-02: the pool is 1/8 of MQTTD_MEMORY_MAX_BYTES or 256 MiB; 1 MiB per connection. The three settings, CONFIGURATION.md, SIZING.md and the example TOML land with T3 (#816); OPERATIONS.md (runbook, alerts on ingress_credit_bytes and ingress_paused_seconds) remains."
  - id: 0082-T6
    title: "Acceptance: the #504 cloud shape at 1.5-2x overload holds RSS bounded and recovers without a restart"
    status: planned
    issue: 819
    notes: "Closes #504 and #535 when it passes."
---

# Delivery: ADR 0082 — bounded hub ingress

The plan, progress, and changelog for [ADR 0082](../adr/0082-bounded-hub-ingress.md). Task
status lives in the frontmatter above. The table below is generated from it.

<!-- status-table:0082 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0082-T1 | ⬜ planned | [#814](https://github.com/mbilling/fss-mqtt-broker/issues/814) | — | "The 2026-10-01 re-run measured 2,553,413 queued commands at 2.60 GB RSS (about 1,019 B each at 200 B payloads). The harness is the before / overload / idle / control sequence #504's acceptance names, asserting RSS, /livez and the control rung." |
| 0082-T2 | ✅ done | [#815](https://github.com/mbilling/fss-mqtt-broker/issues/815) | 2026-10-02 | "PR #821. The hub sorts arrivals into a control lane (completions and acks, the durable plane, Ping, admin) dispatched before the data lane; everything earlier data can change stays ordered on data (ADR amendment 2026-10-02). HubCommand::lane is an exhaustive match; Flush is the data-lane barrier; mqttd_hub_lane_depth{lane}. A ping behind 100,000 publishes is reached after <=98 data dispatches (with one FIFO, 100,000; mutation-proven); lib 517, cluster_stress, durable_sessions, cluster, admin, binary_smoke pass." |
| 0082-T3 | 🚧 in-progress | [#816](https://github.com/mbilling/fss-mqtt-broker/issues/816) | — | "The permit travels inside the command and is released when the hub drops it. The keepalive deadline does not run during broker-imposed pauses. QoS 1/2 semantics and Receive Maximum are unchanged. Implements MQTTD_INGRESS_OVERLOAD=pause|shed-qos0 (default pause; QoS 1/2 always pause), decided 2026-10-02. Charge = topic + payload + 800 B, from #504's measured 1,019 B per queued command, until T1 measures it directly. Brings forward T5's three settings with CONFIGURATION.md, SIZING.md and the example TOML." |
| 0082-T4 | ⬜ planned | [#817](https://github.com/mbilling/fss-mqtt-broker/issues/817) | — | "publish_dropped{reason=hub-ingress}. Remote QoS >= 1 is uncharged (the origin's pending table bounds it). Control and durable frames are never charged." |
| 0082-T5 | ⬜ planned | [#818](https://github.com/mbilling/fss-mqtt-broker/issues/818) | — | "Also MQTTD_INGRESS_OVERLOAD ([limits] ingress_overload) in mqtt-config and the docs, with the pause versus shed-qos0 trade-off stated. Defaults accepted 2026-10-02: the pool is 1/8 of MQTTD_MEMORY_MAX_BYTES or 256 MiB; 1 MiB per connection. The three settings, CONFIGURATION.md, SIZING.md and the example TOML land with T3 (#816); OPERATIONS.md (runbook, alerts on ingress_credit_bytes and ingress_paused_seconds) remains." |
| 0082-T6 | ⬜ planned | [#819](https://github.com/mbilling/fss-mqtt-broker/issues/819) | — | "Closes #504 and #535 when it passes." |
<!-- /status-table:0082 -->

## Sequencing

1. **T1 first.** The credit charge must match what a queued command really retains, and
   every later task asserts with T1's harness.
2. **T2 ships on its own first** (decided 2026-10-02). Splitting the lanes changes no
   admission, and on its own it removes the consensus and health starvation seen under load.
3. **T3 and T4** can land in either order; they touch different producers. T3 is the one
   that bounds the 10 GB.
4. **T5** lands with or right after T3/T4.
5. **T6** is the cloud acceptance. It closes #504 and #535.

## Changelog

- 2026-10-01: ADR proposed after the #504 cloud re-run measured the hub command channel as
  the unbounded queue (2.5M commands, 2.6 GB, drained to 705 on the broker that stayed under
  its limit). Tasks and issues filed (#814–#819).
- 2026-10-02: ADR accepted. Overload behaviour is configurable (`MQTTD_INGRESS_OVERLOAD`,
  default `pause`), the defaults stand, and T2 ships first.
- 2026-10-02: T2 delivered (PR #821 merged): the control lane.
