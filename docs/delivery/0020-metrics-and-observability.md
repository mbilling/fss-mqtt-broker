---
adr: "0020"
title: Metrics and runtime observability
adr_status: Accepted
tasks:
  - id: 0020-T1
    title: Add prometheus-client to mqtt-observability; Metrics registry + typed handles + render()
    status: done
    date: 2026-06-22
    evidence: mqtt-observability/src/metrics.rs Metrics (prometheus-client 0.22, registry with_prefix mqttd) + render(); render_produces_valid_openmetrics_exposition; counters_and_gauges_move_and_render; no_unbounded_label_keys_are_used; cargo deny clean
  - id: 0020-T2
    title: Serve GET /metrics from health.rs (replace the 404 + its test); MQTTD_METRICS_BIND option
    status: done
    date: 2026-06-22
    evidence: health.rs route /metrics -> Metrics::render() (OpenMetrics content-type); HealthState::with_metrics; main.rs builds Metrics + MQTTD_METRICS_BIND separate listener; metrics_endpoint_serves_exposition_when_enabled; unknown_paths_are_404 (disabled case)
  - id: 0020-T3
    title: Instrument connections/handshakes/auth/ACL/keepalive in conn.rs
    status: planned
  - id: 0020-T4
    title: Instrument publish/deliver, queue depth, evictions, inflight, retained/subs gauges in hub.rs
    status: planned
  - id: 0020-T5
    title: Instrument listener accepts/errors in main.rs
    status: planned
  - id: 0020-T6
    title: Instrument cluster (members/states, peer links, lease role/epoch, durable append latency/failures)
    status: planned
  - id: 0020-T7
    title: Cardinality discipline (no per-client/per-topic labels; fixed small label sets)
    status: planned
  - id: 0020-T8
    title: Tests (valid exposition render; publish round-trip moves counters; assert no high-cardinality labels)
    status: planned
  - id: 0020-T9
    title: Later OpenTelemetry/OTLP export behind the same registry
    status: deferred
    notes: explicitly out of scope now; addable later without changing instrumentation per the ADR
---

# Delivery — ADR 0020: Metrics and runtime observability

Decision: [docs/adr/0020-metrics-and-observability.md](../adr/0020-metrics-and-observability.md).

## Plan

The decision's implementation-notes workstream decomposes into these tasks. Each carries a
stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0020-T1** Registry | `mqtt-observability` depends on `prometheus-client`; a `Metrics` struct owns the registry + typed handles, built once in `main` and shared (`Arc`); a `render() -> String` produces the exposition text. |
| **0020-T2** Endpoint | `GET /metrics` is served from the existing health HTTP server returning `metrics.render()` (replacing the deliberate 404 and its asserting test), with an optional separate `MQTTD_METRICS_BIND`. |
| **0020-T3** Conn instrumentation | `conn.rs` updates connection/handshake counters and gauges, auth/ACL outcome counters, and keepalive-reap counters. |
| **0020-T4** Hub instrumentation | `hub.rs` updates publish/deliver counters (by qos), deliver-latency histogram, queue-depth/inflight/evicted, and retained/subscription gauges read off in-memory maps. |
| **0020-T5** Listener instrumentation | Listeners in `main.rs` count accepts and accept/handshake errors by class. |
| **0020-T6** Cluster instrumentation | `mqtt-cluster` exposes member count by state, peer-link up/down, lease-group role/epoch, durable-append latency histogram and failure counters (no-quorum, not-owner). |
| **0020-T7** Cardinality discipline | Labels are limited to small fixed sets (qos, protocol version, reason class, member state); no per-client or per-topic labels. |
| **0020-T8** Tests | A unit test asserts `/metrics` renders valid exposition, a publish round-trip moves the expected counters, and no high-cardinality labels are present. |
| **0020-T9** OTLP later | An OpenTelemetry/OTLP exporter behind the same registry, added without changing the instrumentation. |

## Progress

<!-- status-table:0020 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0020-T1 | ✅ done | 2026-06-22 | mqtt-observability/src/metrics.rs Metrics (prometheus-client 0.22, registry with_prefix mqttd) + render(); render_produces_valid_openmetrics_exposition; counters_and_gauges_move_and_render; no_unbounded_label_keys_are_used; cargo deny clean |
| 0020-T2 | ✅ done | 2026-06-22 | health.rs route /metrics -> Metrics::render() (OpenMetrics content-type); HealthState::with_metrics; main.rs builds Metrics + MQTTD_METRICS_BIND separate listener; metrics_endpoint_serves_exposition_when_enabled; unknown_paths_are_404 (disabled case) |
| 0020-T3 | ⬜ planned | — |  |
| 0020-T4 | ⬜ planned | — |  |
| 0020-T5 | ⬜ planned | — |  |
| 0020-T6 | ⬜ planned | — |  |
| 0020-T7 | ⬜ planned | — |  |
| 0020-T8 | ⬜ planned | — |  |
| 0020-T9 | 💤 deferred | — | explicitly out of scope now; addable later without changing instrumentation per the ADR |
<!-- /status-table:0020 -->

## Changelog

- **2026-06-19** — Delivery doc opened from the Proposed ADR; all tasks `planned` pending
  ratification. T9 (OTLP export) recorded as deferred per the ADR's later-addition scope.
