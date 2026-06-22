# ADR 0020 — Metrics and runtime observability

- **Status:** Proposed
- **Date:** 2026-06-19
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0020-metrics-and-observability.md](../delivery/0020-metrics-and-observability.md) — plan, progress, and changelog
- **Related:** [ADR 0004](0004-identity-and-authentication.md) (audit log),
  the health endpoints in `crates/mqttd/src/health.rs`,
  [ADR 0019](0019-graceful-shutdown.md) (lifecycle signals to surface)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0020-metrics-and-observability.md).

## Context

The broker has **structured logging** (`tracing`), a **hash-chained audit log**
(ADR 0004), and Kubernetes-style **health probes** (`/livez`, `/readyz`,
`/healthz` in `crates/mqttd/src/health.rs`). It has **no metrics**: there are no
counters, gauges, or histograms, and `/metrics` deliberately returns `404` (a test
asserts this). Membership size is exposed only inside the `/readyz` JSON body.

You cannot run a message broker in production blind to:

- connection count and churn, accept/handshake errors;
- message throughput (in/out, by QoS) and end-to-end publish→deliver latency;
- offline-queue depth, in-flight/backlog occupancy, drop/evict counts (the ADR 0001/0012
  bounds firing);
- retained-store size, subscription-tree size;
- cluster health: member count and states, peer-link up/down, lease-group role/epoch,
  durable append latency and `NoQuorum`/`NotOwner` rates;
- auth/ACL failure rates (security signal), keepalive reaps, shutdown drain duration.

Without these there is no alerting, no capacity planning, no regression detection, and no
way to confirm the bounds and guarantees the other ADRs implement are actually holding in
production.

## Decision

Expose **Prometheus metrics** over HTTP, instrumenting the hot paths, with strict
cardinality discipline.

### 1. Client library: `prometheus-client` (pure-Rust, Prometheus-org)

Use [`prometheus-client`](https://crates.io/crates/prometheus-client) — the pure-Rust
crate maintained by the Prometheus project. Pure-Rust keeps the `cargo deny` surface
clean (consistent with ADR 0002/0018); OpenMetrics/Prometheus text exposition is the de
facto standard and integrates with every monitoring stack. (OpenTelemetry export is a
possible later addition behind the same registry; not required now.)

### 2. Exposition endpoint

Serve `/metrics` from the **existing health HTTP server** (`health.rs`) so there is one
operational listener, or on a separate bind via `MQTTD_METRICS_BIND` if an operator wants
to isolate it. Plaintext on the internal/ops network (same trust model as the health
endpoints); document that it should not be exposed publicly.

### 3. Metric set (initial, cardinality-bounded)

Counters/gauges/histograms live in a registry owned by `mqtt-observability` and are
updated from the hub, connection, listener, and cluster code. **No per-client or
per-topic labels** (unbounded cardinality); labels are limited to small fixed sets (qos,
protocol version, reason class, member state).

- **Connections:** `mqttd_connections_active` (gauge), `mqttd_connections_total`
  (counter, by protocol version), `mqttd_connection_errors_total` (by class),
  `mqttd_tls_handshake_seconds` (histogram).
- **Messages:** `mqttd_publish_received_total` / `mqttd_publish_delivered_total` (by qos),
  `mqttd_publish_dropped_total` (by reason: expired, queue-overflow, no-subscriber),
  `mqttd_deliver_latency_seconds` (histogram).
- **Sessions/queues:** `mqttd_sessions_persistent` (gauge),
  `mqttd_offline_queue_depth` (gauge or histogram of per-session depth, sampled),
  `mqttd_inflight_messages` (gauge), `mqttd_queue_evicted_total`.
- **Retained/subs:** `mqttd_retained_messages` (gauge), `mqttd_subscriptions` (gauge).
- **Cluster:** `mqttd_cluster_members` (gauge, by state alive/suspect/dead),
  `mqttd_peer_links` (gauge), `mqttd_lease_groups_owned` (gauge),
  `mqttd_durable_append_seconds` (histogram),
  `mqttd_durable_append_failures_total` (by reason: no-quorum, not-owner).
- **Security:** `mqttd_auth_failures_total` (by reason), `mqttd_acl_denied_total`
  (by action), `mqttd_keepalive_reaped_total`.
- **Lifecycle:** `mqttd_shutdown_drain_seconds`, `mqttd_build_info` (version/commit gauge).

### 4. Implementation shape

A `Metrics` struct in `mqtt-observability` holding the registry and typed handles, built
once in `main` and shared (`Arc`) into the hub, listeners, and cluster tasks — the same
wiring pattern already used for the `SessionStore`/placement. Hot-path updates are
lock-free atomic increments (`prometheus-client` families are cheap). The hub already
serializes state, so gauges like queue depth and subscription count are read off its
in-memory maps on scrape (or updated on change).

## Consequences

- **Good:** operators get throughput/latency/error/saturation visibility, alerting, and
  capacity signals; the bounds and guarantees from ADRs 0001/0012/0016/0017 become
  *observable* (e.g. queue-evicted and durable-append-failure rates). Standard Prometheus
  scrape; no new infra.
- **Cost:** a small per-event atomic update on hot paths (negligible) and one pure-Rust
  dependency. Care to keep label cardinality bounded — the main footgun.
- **Risk:** low. Metrics are read-only side effects; the only real hazard is cardinality
  blowup, which the no-per-client/per-topic-label rule prevents.

## Alternatives considered

- **OpenTelemetry first.** More flexible (traces + metrics, OTLP), but a heavier
  dependency and most deployments still scrape Prometheus. Start with Prometheus
  exposition behind a registry; an OTLP exporter can be added later without changing the
  instrumentation.
- **Logs-only (parse `tracing` for metrics).** Brittle, high-overhead, and not how
  operators run brokers. The audit log stays for security events; metrics are a separate,
  purpose-built surface.
- **A metrics framework (`metrics` facade + exporter).** Reasonable, but the direct
  `prometheus-client` registry is fewer moving parts and a smaller, Prometheus-owned
  dependency.
