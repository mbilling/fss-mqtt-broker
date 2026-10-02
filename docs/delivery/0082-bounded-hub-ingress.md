---
adr: "0082"
title: "Bounded hub ingress: a control lane that never waits behind data, and byte credits that push back on publishers"
adr_status: Accepted
tasks:
  - id: 0082-T1
    title: "Measure and expose: hub queue depth and bytes per class, the per-command memory cost, and the overload harness"
    status: done
    issue: 814
    date: 2026-10-02
    evidence: "PR #833. tests/ingress_cost.rs measures RSS per queued command on the connection's own decode path under mimalloc: 455-610 B beyond topic+payload on the hub channel, 731-1,076 B in a lane just past a doubling (408 B slot). COMMAND_OVERHEAD = 2 x size_of::<HubCommand>() + 384 (1,200 B); the test asserts held <= charged <= 3x held and fails with one slot (mutation-checked); passes on macOS and Linux CI. Properties charged with the payload (client and peer); alias-only publishes charged for the resolved topic; the sweep shrinks idle lanes. Queue depth per lane (T2) and queued bytes (ingress_credit_bytes, T3) were already exported. The process-level before/overload/idle/control harness is the scale rig's knee harness, run by T6."
    notes: "The 2026-10-01 re-run measured 2,553,413 queued commands at 2.60 GB RSS (about 1,019 B each at 200 B payloads). T3 charges topic + payload + 800 B from that figure; it uses the wire topic, so an alias-only PUBLISH is undercharged by its topic length. Calibrate both here. The harness is the before / overload / idle / control sequence #504's acceptance names, asserting RSS, /livez and the control rung."
  - id: 0082-T2
    title: "Split the hub command channel: a control lane drained first, a data lane for publishes"
    status: done
    issue: 815
    date: 2026-10-02
    evidence: "PR #821. The hub sorts arrivals into a control lane (completions and acks, the durable plane, Ping, admin) dispatched before the data lane; everything earlier data can change stays ordered on data (ADR amendment 2026-10-02). HubCommand::lane is an exhaustive match; Flush is the data-lane barrier; mqttd_hub_lane_depth{lane}. A ping behind 100,000 publishes is reached after <=98 data dispatches (with one FIFO, 100,000; mutation-proven); lib 517, cluster_stress, durable_sessions, cluster, admin, binary_smoke pass."
  - id: 0082-T3
    title: "Client ingress credits: a global byte pool plus a per-connection cap; a connection over its credit stops reading its socket"
    status: done
    issue: 816
    date: 2026-10-02
    evidence: "PR #823. Node pool plus a 1 MiB cap per connection; the permit rides in HubCommand::Publish and is dropped by the hub after dispatch. A connection without credit parks the publish with its socket unread; keepalive is disarmed while parked. MQTTD_INGRESS_OVERLOAD=pause|shed-qos0. Against a stalled hub, 8 publishers held the queue at 65 commands (the 64 KiB pool) under both settings: QoS 1 2,400/2,400 dispatched and acked; shed-qos0 QoS 0 65 dispatched + 15,935 shed = 16,000 sent. Without credit: 2,056 (QoS 1) and 16,000 (QoS 0). Real hub under a full pool: /livez worst ~11 ms, ping <2 ms. Mutation-proven (admit always granting; keepalive armed while parked)."
    notes: "The permit travels inside the command and is released when the hub drops it. The keepalive deadline does not run during broker-imposed pauses. QoS 1/2 semantics and Receive Maximum are unchanged. Implements MQTTD_INGRESS_OVERLOAD=pause|shed-qos0 (default pause; QoS 1/2 always pause), decided 2026-10-02. Charge = topic + payload + 800 B, from #504's measured 1,019 B per queued command, until T1 measures it directly. Brings forward T5's three settings with CONFIGURATION.md, SIZING.md and the example TOML."
  - id: 0082-T4
    title: "Peer ingress: shed remote QoS 0 data past the credit (counted); never pause a peer link"
    status: done
    issue: 817
    date: 2026-10-02
    evidence: "PR #829. Inbound peer QoS 0 publishes and shared deliveries take node-pool credit (no per-link cap); without it they are shed, counted on the credit and exported by the hub sweep as publish_dropped{reason=hub-ingress}. The reader never waits. QoS >= 1, retained forwards (ADR §3 amendment) and control/durable frames are uncharged. Mutation-proven (charging retained; uncounted shed); lib 531, cluster, cluster_stress, durable_sessions, ingress_credit pass."
    notes: "publish_dropped{reason=hub-ingress}. Remote QoS >= 1 is uncharged (the origin's pending table bounds it). Control and durable frames are never charged. Retained forwards are uncharged at any QoS (ADR §3 amendment 2026-10-02). Pool only, no per-link cap; the reader counts sheds on the credit and the hub sweep exports them."
  - id: 0082-T5
    title: "Configuration, defaults and documentation: MQTTD_HUB_INGRESS_BYTES and MQTTD_CONN_INGRESS_BYTES, SIZING, OPERATIONS and metrics"
    status: done
    issue: 818
    date: 2026-10-02
    evidence: "PRs #823 (the three settings, CONFIGURATION.md, SIZING.md, the example TOML) and #831 (OPERATIONS.md: the overload section and runbook; three shipped warn alerts in the chart PrometheusRule: MqttdIngressCreditSaturated, MqttdIngressPausesLong, MqttdIngressShedding; ADR §4 corrected to the as-built metric names). helm lint and helm template pass; the mqttui bundle is re-vendored."
    notes: "Also MQTTD_INGRESS_OVERLOAD ([limits] ingress_overload) in mqtt-config and the docs, with the pause versus shed-qos0 trade-off stated. Defaults accepted 2026-10-02: the pool is 1/8 of MQTTD_MEMORY_MAX_BYTES or 256 MiB; 1 MiB per connection. The three settings, CONFIGURATION.md, SIZING.md and the example TOML land with T3 (#816); OPERATIONS.md (runbook, alerts on ingress_credit_bytes and ingress_paused_seconds) remains."
  - id: 0082-T6
    title: "Acceptance: the #504 cloud shape at 1.5-2x overload holds RSS bounded and recovers without a restart"
    status: done
    issue: 819
    date: 2026-10-02
    evidence: "Passed 2026-10-02 on bench-candidate-a3a71da (T2+T3 defaults: pause, 256 MiB pool, 1 MiB/conn), 3xCCX23 + 12xCCX33. 3-node arm: overload at 18 and 24 sites; every scrape answered; hub queue held at about 263k commands = the pool; RSS 0.47-0.91 GB (was 10.7 GB frozen); the 6-site control matched its baseline exactly (180,000 msg/s, p99 <=5 ms, GREEN) after the reset gate (1 conn, 0 sessions). 1-node arm: 2-4x overload with 0 dropped under pause; control 89,977 vs baseline 89,983, p99 <=500 ms. Run exited 0, teardown audited clean. Evidence: the #504 comment; follow-up #825 (slow reaping of credit-paused connections)."
---

# Delivery: ADR 0082 — bounded hub ingress

The plan, progress, and changelog for [ADR 0082](../adr/0082-bounded-hub-ingress.md). Task
status lives in the frontmatter above. The table below is generated from it.

<!-- status-table:0082 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0082-T1 | ✅ done | [#814](https://github.com/mbilling/fss-mqtt-broker/issues/814) | 2026-10-02 | "PR #833. tests/ingress_cost.rs measures RSS per queued command on the connection's own decode path under mimalloc: 455-610 B beyond topic+payload on the hub channel, 731-1,076 B in a lane just past a doubling (408 B slot). COMMAND_OVERHEAD = 2 x size_of::<HubCommand>() + 384 (1,200 B); the test asserts held <= charged <= 3x held and fails with one slot (mutation-checked); passes on macOS and Linux CI. Properties charged with the payload (client and peer); alias-only publishes charged for the resolved topic; the sweep shrinks idle lanes. Queue depth per lane (T2) and queued bytes (ingress_credit_bytes, T3) were already exported. The process-level before/overload/idle/control harness is the scale rig's knee harness, run by T6." |
| 0082-T2 | ✅ done | [#815](https://github.com/mbilling/fss-mqtt-broker/issues/815) | 2026-10-02 | "PR #821. The hub sorts arrivals into a control lane (completions and acks, the durable plane, Ping, admin) dispatched before the data lane; everything earlier data can change stays ordered on data (ADR amendment 2026-10-02). HubCommand::lane is an exhaustive match; Flush is the data-lane barrier; mqttd_hub_lane_depth{lane}. A ping behind 100,000 publishes is reached after <=98 data dispatches (with one FIFO, 100,000; mutation-proven); lib 517, cluster_stress, durable_sessions, cluster, admin, binary_smoke pass." |
| 0082-T3 | ✅ done | [#816](https://github.com/mbilling/fss-mqtt-broker/issues/816) | 2026-10-02 | "PR #823. Node pool plus a 1 MiB cap per connection; the permit rides in HubCommand::Publish and is dropped by the hub after dispatch. A connection without credit parks the publish with its socket unread; keepalive is disarmed while parked. MQTTD_INGRESS_OVERLOAD=pause|shed-qos0. Against a stalled hub, 8 publishers held the queue at 65 commands (the 64 KiB pool) under both settings: QoS 1 2,400/2,400 dispatched and acked; shed-qos0 QoS 0 65 dispatched + 15,935 shed = 16,000 sent. Without credit: 2,056 (QoS 1) and 16,000 (QoS 0). Real hub under a full pool: /livez worst ~11 ms, ping <2 ms. Mutation-proven (admit always granting; keepalive armed while parked)." |
| 0082-T4 | ✅ done | [#817](https://github.com/mbilling/fss-mqtt-broker/issues/817) | 2026-10-02 | "PR #829. Inbound peer QoS 0 publishes and shared deliveries take node-pool credit (no per-link cap); without it they are shed, counted on the credit and exported by the hub sweep as publish_dropped{reason=hub-ingress}. The reader never waits. QoS >= 1, retained forwards (ADR §3 amendment) and control/durable frames are uncharged. Mutation-proven (charging retained; uncounted shed); lib 531, cluster, cluster_stress, durable_sessions, ingress_credit pass." |
| 0082-T5 | ✅ done | [#818](https://github.com/mbilling/fss-mqtt-broker/issues/818) | 2026-10-02 | "PRs #823 (the three settings, CONFIGURATION.md, SIZING.md, the example TOML) and #831 (OPERATIONS.md: the overload section and runbook; three shipped warn alerts in the chart PrometheusRule: MqttdIngressCreditSaturated, MqttdIngressPausesLong, MqttdIngressShedding; ADR §4 corrected to the as-built metric names). helm lint and helm template pass; the mqttui bundle is re-vendored." |
| 0082-T6 | ✅ done | [#819](https://github.com/mbilling/fss-mqtt-broker/issues/819) | 2026-10-02 | "Passed 2026-10-02 on bench-candidate-a3a71da (T2+T3 defaults: pause, 256 MiB pool, 1 MiB/conn), 3xCCX23 + 12xCCX33. 3-node arm: overload at 18 and 24 sites; every scrape answered; hub queue held at about 263k commands = the pool; RSS 0.47-0.91 GB (was 10.7 GB frozen); the 6-site control matched its baseline exactly (180,000 msg/s, p99 <=5 ms, GREEN) after the reset gate (1 conn, 0 sessions). 1-node arm: 2-4x overload with 0 dropped under pause; control 89,977 vs baseline 89,983, p99 <=500 ms. Run exited 0, teardown audited clean. Evidence: the #504 comment; follow-up #825 (slow reaping of credit-paused connections)." |
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
- 2026-10-02: T3 delivered (PR #823 merged): client ingress credit, with the overload knob
  and T5's three settings, CONFIGURATION.md and SIZING.md.
- 2026-10-02: T6 passed: the #504 cloud acceptance on both arms (RSS bounded, every scrape
  answered, control rungs matched their baselines without a restart). #504, #535 and #819 are
  closed; follow-up #825 tracks slow reaping of credit-paused connections.
- 2026-10-02: #825 fixed: a paused connection watches its transport (TCP, TLS, WS, WSS,
  QUIC) for the client leaving and is reaped at once, with the Will fired; keepalive stays
  disarmed while paused (ADR amendment 2026-10-02).
- 2026-10-02: T4 delivered (PR #829 merged): peer QoS 0 sheds past the pool credit, counted;
  peer links are never paused, and retained forwards are uncharged.
- 2026-10-02: T5 delivered (PR #831 merged): the overload runbook and three shipped alerts.
- 2026-10-02: T1 delivered (PR #833 merged): the charge calibrated from measured retention
  (1,200 B overhead, derived from the command slot), properties and aliased topics charged,
  idle lanes shrunk. Every ADR 0082 task is done.
