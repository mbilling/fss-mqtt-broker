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
    status: done
    date: 2026-06-22
    evidence: ConnPolicy carries Option<Arc<Metrics>>; count_connection_opened (by protocol after CONNACK) / count_connection_closed (on teardown) / count_connection_error (bounded reason class) helpers wired through conn.rs — connection_opened/closed on the active gauge + per-protocol total; connection_errors{reason} for auth (single-shot + enhanced), acl (will-topic deny) and keepalive expiry. Tests: connection_lifecycle_moves_the_metrics_counters; rejected_auth_increments_the_error_counter
  - id: 0020-T4
    title: Instrument publish/deliver, queue depth, evictions, inflight, retained/subs gauges in hub.rs
    status: done
    date: 2026-06-22
    evidence: hub.rs publish_received/delivered (by qos) + publish_dropped (queue-overflow); deliver-latency histogram observed over the on-loop fan-out per publish; sessions/subscriptions/retained_messages/inflight_messages gauges snapshot the in-memory maps on the 1s sweep tick via Hub::refresh_gauges (RetainedStore::count added, cheap override in both stores). Tests: publish_round_trip_moves_the_metrics_counters (now also asserts deliver_latency_seconds_count); gauge_refresh_snapshots_sessions_and_subscriptions
  - id: 0020-T5
    title: Instrument listener accepts/errors in main.rs
    status: done
    date: 2026-06-22
    evidence: accepts_total{listener} counter (tls/plaintext) incremented per TCP accept in serve_tls_clients/serve_plaintext_clients (read off policy.metrics — no signature change); accept failures and TLS handshake failures counted via connection_errors{reason=accept|tls}. The accepts-vs-connections gap surfaces handshake/connect drop-off. Test: metrics.rs counters_and_gauges_move_and_render asserts accepts_total{listener=tls} and connection_errors{reason=tls}
  - id: 0020-T6
    title: Instrument cluster (members/states, peer links, lease role/epoch, durable append latency/failures)
    status: done
    date: 2026-06-23
    evidence: "Done in mqttd (no mqtt-observability dependency added to mqtt-cluster): cluster_members + peer_links gauges on the hub sweep; members_by_state{alive|suspect|dead} gauge tracked from the MembershipEvent stream in maintain_peer_links (test member_states_drive_the_gauge); lease_leader + lease_epoch gauges sampled in Hub::refresh_gauges from a new read-only DurablePlane::lease_role() (openraft metrics); durable_append_latency_seconds histogram (gated to durable mode) and durable_append_failures{reason} counter at the hub enqueue path, with StorageError enriched to distinct NotOwner/NoQuorum variants (is_transient preserved) so reasons no-quorum/not-owner/unavailable/backend are exact (tests a_failed_durable_append_is_counted_by_reason, durable_failure_reasons_are_bounded). metrics.rs counters_and_gauges_move_and_render asserts members{state}, lease_leader/epoch, durable_append_latency_count and durable_append_failures{reason}."
  - id: 0020-T7
    title: Cardinality discipline (no per-client/per-topic labels; fixed small label sets)
    status: done
    date: 2026-06-22
    evidence: all label families are fixed small sets (protocol/qos/reason/version); metrics.rs no_unbounded_label_keys_are_used + hub publish_round_trip asserts no client=/topic= labels
  - id: 0020-T8
    title: Tests (valid exposition render; publish round-trip moves counters; assert no high-cardinality labels)
    status: done
    date: 2026-06-22
    evidence: metrics.rs render_produces_valid_openmetrics_exposition; hub publish_round_trip_moves_the_metrics_counters; no-high-cardinality assertions in both
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
| 0020-T3 | ✅ done | 2026-06-22 | ConnPolicy carries Option<Arc<Metrics>>; count_connection_opened (by protocol after CONNACK) / count_connection_closed (on teardown) / count_connection_error (bounded reason class) helpers wired through conn.rs — connection_opened/closed on the active gauge + per-protocol total; connection_errors{reason} for auth (single-shot + enhanced), acl (will-topic deny) and keepalive expiry. Tests: connection_lifecycle_moves_the_metrics_counters; rejected_auth_increments_the_error_counter |
| 0020-T4 | ✅ done | 2026-06-22 | hub.rs publish_received/delivered (by qos) + publish_dropped (queue-overflow); deliver-latency histogram observed over the on-loop fan-out per publish; sessions/subscriptions/retained_messages/inflight_messages gauges snapshot the in-memory maps on the 1s sweep tick via Hub::refresh_gauges (RetainedStore::count added, cheap override in both stores). Tests: publish_round_trip_moves_the_metrics_counters (now also asserts deliver_latency_seconds_count); gauge_refresh_snapshots_sessions_and_subscriptions |
| 0020-T5 | ✅ done | 2026-06-22 | accepts_total{listener} counter (tls/plaintext) incremented per TCP accept in serve_tls_clients/serve_plaintext_clients (read off policy.metrics — no signature change); accept failures and TLS handshake failures counted via connection_errors{reason=accept|tls}. The accepts-vs-connections gap surfaces handshake/connect drop-off. Test: metrics.rs counters_and_gauges_move_and_render asserts accepts_total{listener=tls} and connection_errors{reason=tls} |
| 0020-T6 | ✅ done | 2026-06-23 | "Done in mqttd (no mqtt-observability dependency added to mqtt-cluster): cluster_members + peer_links gauges on the hub sweep; members_by_state{alive|suspect|dead} gauge tracked from the MembershipEvent stream in maintain_peer_links (test member_states_drive_the_gauge); lease_leader + lease_epoch gauges sampled in Hub::refresh_gauges from a new read-only DurablePlane::lease_role() (openraft metrics); durable_append_latency_seconds histogram (gated to durable mode) and durable_append_failures{reason} counter at the hub enqueue path, with StorageError enriched to distinct NotOwner/NoQuorum variants (is_transient preserved) so reasons no-quorum/not-owner/unavailable/backend are exact (tests a_failed_durable_append_is_counted_by_reason, durable_failure_reasons_are_bounded). metrics.rs counters_and_gauges_move_and_render asserts members{state}, lease_leader/epoch, durable_append_latency_count and durable_append_failures{reason}." |
| 0020-T7 | ✅ done | 2026-06-22 | all label families are fixed small sets (protocol/qos/reason/version); metrics.rs no_unbounded_label_keys_are_used + hub publish_round_trip asserts no client=/topic= labels |
| 0020-T8 | ✅ done | 2026-06-22 | metrics.rs render_produces_valid_openmetrics_exposition; hub publish_round_trip_moves_the_metrics_counters; no-high-cardinality assertions in both |
| 0020-T9 | 💤 deferred | — | explicitly out of scope now; addable later without changing instrumentation per the ADR |
<!-- /status-table:0020 -->

## Changelog

- **2026-06-19** — Delivery doc opened from the Proposed ADR; all tasks `planned` pending
  ratification. T9 (OTLP export) recorded as deferred per the ADR's later-addition scope.
- **2026-06-23** — T6 completed. The remaining cluster metrics (member-by-state, lease
  role/epoch, durable-append latency/failures) were instrumented entirely in `mqttd` — via
  `maintain_peer_links`, the hub sweep, and the hub's durable-store call path, plus a
  read-only `DurablePlane::lease_role()` accessor — so `mqtt-cluster` gained **no**
  dependency on `mqtt-observability`. `StorageError` was enriched with distinct
  `NotOwner`/`NoQuorum` variants (retry semantics preserved) so the append-failure reasons
  are exact. Only T9 (OTLP) remains, deferred.
