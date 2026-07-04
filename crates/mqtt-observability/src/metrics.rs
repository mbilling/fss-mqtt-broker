//! Prometheus metrics ([ADR 0020](../../../docs/adr/0020-metrics-and-observability.md)).
//!
//! A single [`Metrics`] owns a `prometheus-client` registry plus typed metric handles. It is
//! built once in `main`, shared (`Arc`) into the hub, connection, listener, and cluster code,
//! and rendered as Prometheus text exposition on `GET /metrics`.
//!
//! **Cardinality discipline (ADR 0020 §3):** labels are limited to small fixed sets (qos,
//! protocol version, reason class, member state). There are **no per-client or per-topic
//! labels** — the one real footgun of metrics — so every family is bounded.

use opentelemetry::metrics::{
    Counter as OtelCounter, Gauge as OtelGauge, Histogram as OtelHistogram, Meter, UpDownCounter,
};
use opentelemetry::KeyValue;
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

/// `{version}` label for `mqttd_build_info`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct VersionLabel {
    version: String,
}

/// `{protocol}` label — a bounded set: `3.1.1` or `5`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ProtocolLabel {
    protocol: String,
}

/// `{qos}` label — a bounded set: `0`, `1`, `2`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QosLabel {
    qos: String,
}

/// `{reason}` label — a small fixed set of reason classes (never free-form text).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ReasonLabel {
    reason: String,
}

/// `{listener}` label — a bounded set: `tls`, `plaintext`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ListenerLabel {
    listener: String,
}

/// `{state}` label — the bounded SWIM member states: `alive`, `suspect`, `dead`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StateLabel {
    state: String,
}

/// `{outcome, trigger}` label for hot reloads — a bounded set: outcome `ok`/`rejected`,
/// trigger `signal` (SIGHUP) / `watch` (filesystem auto-reload, ADR 0033).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabel {
    outcome: String,
    trigger: String,
}

/// The OpenTelemetry mirror of every metric, recorded alongside the Prometheus handles
/// so the same measurement is exported via OTLP (ADR 0020). Built from a real SDK meter
/// when OTLP is enabled, or a no-op meter otherwise (then every record is a no-op).
struct OtelInstruments {
    connections_active: UpDownCounter<i64>,
    connections: OtelCounter<u64>,
    accepts: OtelCounter<u64>,
    connection_errors: OtelCounter<u64>,
    publish_received: OtelCounter<u64>,
    publish_delivered: OtelCounter<u64>,
    publish_dropped: OtelCounter<u64>,
    deliver_latency: OtelHistogram<f64>,
    sessions: OtelGauge<i64>,
    subscriptions: OtelGauge<i64>,
    retained_messages: OtelGauge<i64>,
    inflight_messages: OtelGauge<i64>,
    cluster_members: OtelGauge<i64>,
    peer_links: OtelGauge<i64>,
    members: OtelGauge<i64>,
    lease_leader: OtelGauge<i64>,
    lease_epoch: OtelGauge<i64>,
    durable_append_latency: OtelHistogram<f64>,
    durable_append_failures: OtelCounter<u64>,
    gossip_rejected: OtelCounter<u64>,
    security_reloads: OtelCounter<u64>,
    quic_path_migrations: OtelCounter<u64>,
    retained_divergence: OtelCounter<u64>,
    retained_queue_dropped: OtelCounter<u64>,
}

impl OtelInstruments {
    /// Create every instrument from `meter`, naming each to match its Prometheus
    /// counterpart (the `mqttd` prefix is carried by the OTLP resource `service.name`).
    fn new(meter: &Meter) -> Self {
        Self {
            connections_active: meter.i64_up_down_counter("connections_active").build(),
            connections: meter.u64_counter("connections").build(),
            accepts: meter.u64_counter("accepts").build(),
            connection_errors: meter.u64_counter("connection_errors").build(),
            publish_received: meter.u64_counter("publish_received").build(),
            publish_delivered: meter.u64_counter("publish_delivered").build(),
            publish_dropped: meter.u64_counter("publish_dropped").build(),
            deliver_latency: meter.f64_histogram("deliver_latency_seconds").build(),
            sessions: meter.i64_gauge("sessions").build(),
            subscriptions: meter.i64_gauge("subscriptions").build(),
            retained_messages: meter.i64_gauge("retained_messages").build(),
            inflight_messages: meter.i64_gauge("inflight_messages").build(),
            cluster_members: meter.i64_gauge("cluster_members").build(),
            peer_links: meter.i64_gauge("peer_links").build(),
            members: meter.i64_gauge("members").build(),
            lease_leader: meter.i64_gauge("lease_leader").build(),
            lease_epoch: meter.i64_gauge("lease_epoch").build(),
            durable_append_latency: meter
                .f64_histogram("durable_append_latency_seconds")
                .build(),
            durable_append_failures: meter.u64_counter("durable_append_failures").build(),
            gossip_rejected: meter.u64_counter("gossip_rejected").build(),
            security_reloads: meter.u64_counter("security_reloads").build(),
            quic_path_migrations: meter.u64_counter("quic_path_migrations").build(),
            retained_divergence: meter.u64_counter("retained_divergence").build(),
            retained_queue_dropped: meter.u64_counter("retained_queue_dropped").build(),
        }
    }
}

impl std::fmt::Debug for OtelInstruments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelInstruments").finish_non_exhaustive()
    }
}

/// The broker's metric registry and typed handles. Cheap to share behind an `Arc`; all
/// updates are lock-free atomic operations on the metric families.
#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    /// The OTLP mirror, recorded alongside every Prometheus update.
    otel: OtelInstruments,
    /// The SDK meter provider, held to keep the OTLP export task alive (and for
    /// `flush`/shutdown). `None` when OTLP is disabled (a no-op meter is used).
    provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    connections_active: Gauge,
    connections_total: Family<ProtocolLabel, Counter>,
    accepts_total: Family<ListenerLabel, Counter>,
    connection_errors_total: Family<ReasonLabel, Counter>,
    publish_received_total: Family<QosLabel, Counter>,
    publish_delivered_total: Family<QosLabel, Counter>,
    publish_dropped_total: Family<ReasonLabel, Counter>,
    deliver_latency_seconds: Histogram,
    sessions: Gauge,
    subscriptions: Gauge,
    retained_messages: Gauge,
    inflight_messages: Gauge,
    cluster_members: Gauge,
    peer_links: Gauge,
    members_by_state: Family<StateLabel, Gauge>,
    lease_leader: Gauge,
    lease_epoch: Gauge,
    durable_append_latency_seconds: Histogram,
    durable_append_failures_total: Family<ReasonLabel, Counter>,
    gossip_rejected_total: Family<ReasonLabel, Counter>,
    security_reloads_total: Family<OutcomeLabel, Counter>,
    quic_path_migrations_total: Counter,
    retained_divergence_total: Counter,
    retained_queue_dropped_total: Counter,
}

impl Metrics {
    /// A metrics set with **no** OTLP export — the Prometheus `/metrics` endpoint only.
    /// Used by tests and by a broker without `MQTTD_OTLP_ENDPOINT` configured.
    #[must_use]
    pub fn new(version: &str) -> Self {
        let noop = opentelemetry::metrics::noop::NoopMeterProvider::new();
        let meter = opentelemetry::metrics::MeterProvider::meter(&noop, "mqttd");
        Self::build(version, &meter, None)
    }

    /// A metrics set that also exports via OTLP/HTTP to `endpoint` (the OTLP base URL,
    /// e.g. `http://collector:4318`; the exporter appends `/v1/metrics`), pushing every
    /// `interval`. The Prometheus endpoint stays available. Must be called within a Tokio
    /// runtime (the periodic export task is spawned on it).
    ///
    /// # Errors
    /// Returns an error if the OTLP exporter cannot be built (e.g. a malformed endpoint).
    pub fn with_otlp(
        version: &str,
        endpoint: &str,
        interval: std::time::Duration,
    ) -> Result<Self, opentelemetry_otlp::ExporterBuildError> {
        let provider = build_otlp_provider(endpoint, interval)?;
        let meter = opentelemetry::metrics::MeterProvider::meter(&provider, "mqttd");
        Ok(Self::build(version, &meter, Some(provider)))
    }

    /// Build the registry, register every metric, stamp `mqttd_build_info{version}`, and
    /// create the OTLP instrument mirror from `meter`.
    // A flat, branch-free list of metric registrations: long by count, not by complexity.
    #[allow(clippy::too_many_lines)]
    fn build(
        version: &str,
        meter: &Meter,
        provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    ) -> Self {
        let mut registry = Registry::with_prefix("mqttd");

        let connections_active = register_gauge(
            &mut registry,
            "connections_active",
            "Currently open client connections",
        );

        let connections_total = register_family(
            &mut registry,
            "connections",
            "Client connections accepted, by protocol version",
        );
        let accepts_total = register_family(
            &mut registry,
            "accepts",
            "TCP connections accepted, by listener (before TLS/CONNECT)",
        );
        let connection_errors_total = register_family(
            &mut registry,
            "connection_errors",
            "Connection setup failures, by reason class",
        );
        let publish_received_total = register_family(
            &mut registry,
            "publish_received",
            "PUBLISH packets received from clients, by QoS",
        );
        let publish_delivered_total = register_family(
            &mut registry,
            "publish_delivered",
            "PUBLISH packets delivered to subscribers, by QoS",
        );
        let publish_dropped_total = register_family(
            &mut registry,
            "publish_dropped",
            "Messages dropped, by reason (expired, queue-overflow, no-subscriber)",
        );

        let deliver_latency_seconds = register_latency_histogram(
            &mut registry,
            "deliver_latency_seconds",
            "Publish-to-deliver latency",
        );

        let sessions = register_gauge(
            &mut registry,
            "sessions",
            "Known client sessions (connected or retained)",
        );
        let subscriptions = register_gauge(
            &mut registry,
            "subscriptions",
            "Active topic-filter subscriptions across all sessions",
        );
        let retained_messages = register_gauge(
            &mut registry,
            "retained_messages",
            "Retained messages held by the broker",
        );
        let inflight_messages = register_gauge(
            &mut registry,
            "inflight_messages",
            "Unacknowledged QoS>0 messages outstanding to clients",
        );
        let cluster_members = register_gauge(
            &mut registry,
            "cluster_members",
            "Cluster members eligible for placement (this node plus non-dead peers)",
        );
        let peer_links = register_gauge(
            &mut registry,
            "peer_links",
            "Currently connected inter-node peer links",
        );

        let members_by_state = register_gauge_family(
            &mut registry,
            "members",
            "Cluster members by SWIM state (alive/suspect/dead)",
        );
        let lease_leader = register_gauge(
            &mut registry,
            "lease_leader",
            "1 if this node is the leader of its lease group, else 0",
        );
        let lease_epoch = register_gauge(
            &mut registry,
            "lease_epoch",
            "Current lease-group consensus term (epoch)",
        );
        let durable_append_latency_seconds = register_latency_histogram(
            &mut registry,
            "durable_append_latency_seconds",
            "Durable (quorum) append latency",
        );
        let durable_append_failures_total = register_family(
            &mut registry,
            "durable_append_failures",
            "Durable append failures, by reason (no-quorum, not-owner, backend)",
        );
        let gossip_rejected_total = register_family(
            &mut registry,
            "gossip_rejected",
            "SWIM gossip datagrams dropped, by reason (auth, decode, identity, replay, \
             expired, revoked, domain, cert-miss)",
        );
        let security_reloads_total = register_family(
            &mut registry,
            "security_reloads",
            "Hot reloads of the security policy, by outcome (ok, rejected) and trigger (signal, watch)",
        );
        let quic_path_migrations_total = register_counter(
            &mut registry,
            "quic_path_migrations",
            "QUIC connection path migrations observed (client address changed; same connection and session kept)",
        );
        let retained_divergence_total = register_counter(
            &mut registry,
            "retained_divergence",
            "Retained-message divergences detected between peers (same topic, different value \
             — ADR 0037 P1); should stay at zero once single-owner retained lands",
        );

        let retained_queue_dropped_total = register_counter(
            &mut registry,
            "retained_queue_dropped",
            "Retained mutations dropped because the queue-until-heal bound was hit \
             (ADR 0037 §5): the oldest queued mutation discarded, loudly — non-zero \
             means a partition outlasted the queue's capacity",
        );

        let build_info = Family::<VersionLabel, Gauge>::default();
        registry.register("build_info", "Build information", build_info.clone());
        build_info
            .get_or_create(&VersionLabel {
                version: version.to_string(),
            })
            .set(1);

        Self {
            otel: OtelInstruments::new(meter),
            provider,
            registry,
            connections_active,
            connections_total,
            accepts_total,
            connection_errors_total,
            publish_received_total,
            publish_delivered_total,
            publish_dropped_total,
            deliver_latency_seconds,
            sessions,
            subscriptions,
            retained_messages,
            inflight_messages,
            cluster_members,
            peer_links,
            members_by_state,
            lease_leader,
            lease_epoch,
            durable_append_latency_seconds,
            durable_append_failures_total,
            gossip_rejected_total,
            security_reloads_total,
            quic_path_migrations_total,
            retained_divergence_total,
            retained_queue_dropped_total,
        }
    }

    /// Render the current metrics as Prometheus text exposition (for `GET /metrics`).
    ///
    /// # Panics
    /// Panics only if formatting into a `String` fails, which the standard library does not do.
    #[must_use]
    pub fn render(&self) -> String {
        let mut buf = String::new();
        // Encoding into a `String` cannot fail.
        encode(&mut buf, &self.registry).expect("encode metrics");
        buf
    }

    /// A client connection was accepted (`protocol` is `"3.1.1"` or `"5"`).
    pub fn connection_opened(&self, protocol: &str) {
        self.connections_active.inc();
        self.connections_total
            .get_or_create(&ProtocolLabel {
                protocol: protocol.to_string(),
            })
            .inc();
        self.otel.connections_active.add(1, &[]);
        self.otel
            .connections
            .add(1, &[KeyValue::new("protocol", protocol.to_string())]);
    }

    /// A client connection closed.
    pub fn connection_closed(&self) {
        self.connections_active.dec();
        self.otel.connections_active.add(-1, &[]);
    }

    /// A TCP connection was accepted on `listener` (`"tls"` or `"plaintext"`), before
    /// the TLS handshake and MQTT CONNECT — the gap to `connections_total` is the
    /// handshake/connect drop-off (ADR 0020).
    pub fn connection_accepted(&self, listener: &str) {
        self.accepts_total
            .get_or_create(&ListenerLabel {
                listener: listener.to_string(),
            })
            .inc();
        self.otel
            .accepts
            .add(1, &[KeyValue::new("listener", listener.to_string())]);
    }

    /// A connection failed to set up (`reason` is a bounded class, e.g. `"tls"`, `"auth"`).
    pub fn connection_error(&self, reason: &str) {
        self.connection_errors_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .connection_errors
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// A PUBLISH was received from a client at `qos` (0/1/2).
    pub fn publish_received(&self, qos: u8) {
        self.publish_received_total
            .get_or_create(&QosLabel {
                qos: qos.to_string(),
            })
            .inc();
        self.otel
            .publish_received
            .add(1, &[KeyValue::new("qos", qos.to_string())]);
    }

    /// A PUBLISH was delivered to a subscriber at `qos` (0/1/2).
    pub fn publish_delivered(&self, qos: u8) {
        self.publish_delivered_total
            .get_or_create(&QosLabel {
                qos: qos.to_string(),
            })
            .inc();
        self.otel
            .publish_delivered
            .add(1, &[KeyValue::new("qos", qos.to_string())]);
    }

    /// A message was dropped (`reason` a bounded class: `"expired"`, `"queue-overflow"`,
    /// `"no-subscriber"`).
    pub fn publish_dropped(&self, reason: &str) {
        self.publish_dropped_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .publish_dropped
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// Observe a publish-to-deliver latency in seconds.
    pub fn observe_deliver_latency(&self, seconds: f64) {
        self.deliver_latency_seconds.observe(seconds);
        self.otel.deliver_latency.record(seconds, &[]);
    }

    /// Set the current session count (snapshot of an in-memory map; ADR 0020).
    pub fn set_sessions(&self, n: usize) {
        self.sessions.set(clamp_gauge(n));
        self.otel.sessions.record(clamp_gauge(n), &[]);
    }

    /// Set the current active-subscription count.
    pub fn set_subscriptions(&self, n: usize) {
        self.subscriptions.set(clamp_gauge(n));
        self.otel.subscriptions.record(clamp_gauge(n), &[]);
    }

    /// Set the current retained-message count.
    pub fn set_retained_messages(&self, n: usize) {
        self.retained_messages.set(clamp_gauge(n));
        self.otel.retained_messages.record(clamp_gauge(n), &[]);
    }

    /// Set the current count of unacknowledged QoS>0 messages outstanding to clients.
    pub fn set_inflight_messages(&self, n: usize) {
        self.inflight_messages.set(clamp_gauge(n));
        self.otel.inflight_messages.record(clamp_gauge(n), &[]);
    }

    /// Set the current count of placement-eligible cluster members (ADR 0020-T6).
    pub fn set_cluster_members(&self, n: usize) {
        self.cluster_members.set(clamp_gauge(n));
        self.otel.cluster_members.record(clamp_gauge(n), &[]);
    }

    /// Set the current count of connected inter-node peer links.
    pub fn set_peer_links(&self, n: usize) {
        self.peer_links.set(clamp_gauge(n));
        self.otel.peer_links.record(clamp_gauge(n), &[]);
    }

    /// Set the member count for one bounded SWIM `state` (`"alive"`/`"suspect"`/`"dead"`).
    pub fn set_members_in_state(&self, state: &str, n: usize) {
        self.members_by_state
            .get_or_create(&StateLabel {
                state: state.to_string(),
            })
            .set(clamp_gauge(n));
        self.otel
            .members
            .record(clamp_gauge(n), &[KeyValue::new("state", state.to_string())]);
    }

    /// Record this node's lease-group role (`leader`) and consensus epoch (term).
    pub fn set_lease_role(&self, is_leader: bool, epoch: u64) {
        self.lease_leader.set(i64::from(is_leader));
        self.lease_epoch.set(clamp_gauge_u64(epoch));
        self.otel.lease_leader.record(i64::from(is_leader), &[]);
        self.otel.lease_epoch.record(clamp_gauge_u64(epoch), &[]);
    }

    /// Observe a durable (quorum) append latency in seconds.
    pub fn observe_durable_append_latency(&self, seconds: f64) {
        self.durable_append_latency_seconds.observe(seconds);
        self.otel.durable_append_latency.record(seconds, &[]);
    }

    /// A durable append failed; `reason` is a bounded class (`"no-quorum"`, `"not-owner"`,
    /// `"backend"`).
    pub fn durable_append_failed(&self, reason: &str) {
        self.durable_append_failures_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .durable_append_failures
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// A SWIM gossip datagram was dropped (ADR 0003); `reason` is a bounded class
    /// (`"auth"`, `"decode"`, `"identity"`, `"replay"`).
    pub fn gossip_rejected(&self, reason: &str) {
        self.gossip_rejected_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .gossip_rejected
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// A hot reload of the security policy completed with `outcome` (`"ok"` for an applied
    /// swap, `"rejected"` for a validate-before-swap failure that kept the running policy),
    /// fired by `trigger` (`"signal"` for SIGHUP, `"watch"` for the filesystem watcher,
    /// ADR 0033).
    pub fn security_reload(&self, outcome: &str, trigger: &str) {
        self.security_reloads_total
            .get_or_create(&OutcomeLabel {
                outcome: outcome.to_string(),
                trigger: trigger.to_string(),
            })
            .inc();
        self.otel.security_reloads.add(
            1,
            &[
                KeyValue::new("outcome", outcome.to_string()),
                KeyValue::new("trigger", trigger.to_string()),
            ],
        );
    }

    /// A QUIC connection migrated to a new client path (ADR 0036 §3b): the peer's remote address
    /// changed while the *same* connection — and its MQTT session and mTLS identity — continued,
    /// with no new handshake or CONNECT (e.g. a Wi-Fi↔cellular handover or a NAT rebind).
    pub fn quic_path_migrated(&self) {
        self.quic_path_migrations_total.inc();
        self.otel.quic_path_migrations.add(1, &[]);
    }

    /// A retained-message divergence was detected: a peer holds a **different value** for a
    /// topic this node also retains (ADR 0037 P1). Divergence is possible under the
    /// best-effort ADR 0014 replication (concurrent publishes, partition heals); once
    /// single-owner retained (ADR 0037) lands this counter staying at zero is the
    /// convergence proof.
    pub fn retained_divergence(&self) {
        self.retained_divergence_total.inc();
        self.otel.retained_divergence.add(1, &[]);
    }

    /// A queued retained mutation was dropped at the queue-until-heal bound
    /// (ADR 0037 §5) — the loud half of the CP trade.
    pub fn retained_queue_dropped(&self) {
        self.retained_queue_dropped_total.inc();
        self.otel.retained_queue_dropped.add(1, &[]);
    }

    /// Force any pending OTLP export to be pushed now (a no-op without OTLP). Best-effort;
    /// used on graceful shutdown and in tests to flush deterministically.
    pub fn flush(&self) {
        if let Some(p) = &self.provider {
            let _ = p.force_flush();
        }
    }
}

/// Build an OTLP/HTTP metric exporter, a periodic reader pushing every `interval`, and the
/// SDK meter provider that drives them. `endpoint` is the OTLP base URL (the exporter
/// appends `/v1/metrics`); `service.name=mqttd` namespaces the metrics at the backend.
fn build_otlp_provider(
    endpoint: &str,
    interval: std::time::Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, opentelemetry_otlp::ExporterBuildError> {
    use opentelemetry_otlp::WithExportConfig;
    // `with_endpoint` is used verbatim (unlike the env-var path, it does not append the
    // signal path), so append `/v1/metrics` to the OTLP base ourselves.
    let url = format!("{}/v1/metrics", endpoint.trim_end_matches('/'));
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(url)
        .build()?;
    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval)
        .build();
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name("mqttd")
        .build();
    Ok(opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build())
}

/// Cast an in-memory map length to the gauge's signed counter, saturating rather
/// than wrapping for the (unreachable) case of a count beyond `i64::MAX`.
fn clamp_gauge(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Cast a `u64` (e.g. a consensus term) to the gauge's signed counter, saturating.
fn clamp_gauge_u64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Register a fresh gauge under `name`/`help` and return a handle to it.
fn register_gauge(registry: &mut Registry, name: &'static str, help: &'static str) -> Gauge {
    let gauge = Gauge::default();
    registry.register(name, help, gauge.clone());
    gauge
}

/// Register a fresh unlabelled counter under `name`/`help` and return a handle to it.
fn register_counter(registry: &mut Registry, name: &'static str, help: &'static str) -> Counter {
    let counter = Counter::default();
    registry.register(name, help, counter.clone());
    counter
}

/// Register a fresh labelled counter family under `name`/`help` and return a handle.
fn register_family<L>(
    registry: &mut Registry,
    name: &'static str,
    help: &'static str,
) -> Family<L, Counter>
where
    L: Clone + std::hash::Hash + Eq + EncodeLabelSet + Send + Sync + std::fmt::Debug + 'static,
{
    let family = Family::<L, Counter>::default();
    registry.register(name, help, family.clone());
    family
}

/// Register a fresh labelled gauge family under `name`/`help` and return a handle.
fn register_gauge_family<L>(
    registry: &mut Registry,
    name: &'static str,
    help: &'static str,
) -> Family<L, Gauge>
where
    L: Clone + std::hash::Hash + Eq + EncodeLabelSet + Send + Sync + std::fmt::Debug + 'static,
{
    let family = Family::<L, Gauge>::default();
    registry.register(name, help, family.clone());
    family
}

/// Register a latency histogram (exponential buckets ~100us..3s) under `name`/`help`.
fn register_latency_histogram(
    registry: &mut Registry,
    name: &'static str,
    help: &'static str,
) -> Histogram {
    let h = Histogram::new(exponential_buckets(0.0001, 2.0, 16));
    registry.register(name, help, h.clone());
    h
}

#[cfg(test)]
mod tests {
    use super::Metrics;

    #[test]
    fn render_produces_valid_openmetrics_exposition() {
        let m = Metrics::new("1.2.3");
        let out = m.render();
        // Build info is stamped at construction with the version label.
        assert!(
            out.contains("mqttd_build_info{version=\"1.2.3\"} 1"),
            "build_info missing:\n{out}"
        );
        // Registered metrics carry their HELP/TYPE lines (the `_total` counter suffix is
        // added to the sample line, not the metric family name).
        assert!(out.contains("# TYPE mqttd_connections_active gauge"));
        assert!(out.contains("# HELP mqttd_publish_received "));
        assert!(out.contains("# TYPE mqttd_publish_received counter"));
        // The OpenMetrics exposition terminates with the EOF marker.
        assert!(out.trim_end().ends_with("# EOF"), "missing # EOF:\n{out}");
    }

    #[test]
    fn counters_and_gauges_move_and_render() {
        let m = Metrics::new("t");
        m.connection_opened("5");
        m.connection_opened("3.1.1");
        m.connection_closed();
        m.publish_received(1);
        m.publish_received(1);
        m.publish_delivered(0);
        m.publish_dropped("no-subscriber");
        m.connection_accepted("tls");
        m.connection_error("tls");
        m.set_sessions(3);
        m.set_retained_messages(7);
        m.set_cluster_members(2);
        m.set_peer_links(1);
        m.set_members_in_state("alive", 2);
        m.set_members_in_state("suspect", 1);
        m.set_lease_role(true, 7);
        m.observe_durable_append_latency(0.002);
        m.durable_append_failed("no-quorum");
        m.gossip_rejected("replay");
        let out = m.render();

        assert!(out.contains("mqttd_connections_active 1"), "{out}");
        assert!(
            out.contains("mqttd_connections_total{protocol=\"5\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_connections_total{protocol=\"3.1.1\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_publish_received_total{qos=\"1\"} 2"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_publish_dropped_total{reason=\"no-subscriber\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_accepts_total{listener=\"tls\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_connection_errors_total{reason=\"tls\"} 1"),
            "{out}"
        );
        assert!(out.contains("mqttd_sessions 3"), "{out}");
        assert!(out.contains("mqttd_retained_messages 7"), "{out}");
        assert!(out.contains("mqttd_cluster_members 2"), "{out}");
        assert!(out.contains("mqttd_peer_links 1"), "{out}");
        assert!(out.contains("mqttd_members{state=\"alive\"} 2"), "{out}");
        assert!(out.contains("mqttd_members{state=\"suspect\"} 1"), "{out}");
        assert!(out.contains("mqttd_lease_leader 1"), "{out}");
        assert!(out.contains("mqttd_lease_epoch 7"), "{out}");
        assert!(
            out.contains("mqttd_durable_append_latency_seconds_count 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_durable_append_failures_total{reason=\"no-quorum\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_gossip_rejected_total{reason=\"replay\"} 1"),
            "{out}"
        );
    }

    /// Cardinality guard (ADR 0020 §3): label *keys* are only ever from the fixed set; no
    /// per-client/per-topic label names appear in the exposition.
    #[test]
    fn no_unbounded_label_keys_are_used() {
        let m = Metrics::new("t");
        m.connection_opened("5");
        m.publish_received(2);
        let out = m.render();
        for forbidden in ["client", "topic", "client_id", "session"] {
            assert!(
                !out.contains(&format!("{forbidden}=")),
                "unbounded label key {forbidden:?} present:\n{out}"
            );
        }
    }

    /// ADR 0020 (T9): with OTLP configured, recording a metric and flushing pushes an
    /// OTLP/HTTP POST to `/v1/metrics` at the endpoint — proven end-to-end against a
    /// local socket that captures the request. Multi-thread runtime so the synchronous
    /// `flush` (`force_flush`) does not block the exporter's async push.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn otlp_export_posts_to_the_endpoint() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
        let sink = captured.clone();
        let server = tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                *sink.lock().await = String::from_utf8_lossy(&buf[..n]).into_owned();
                // A minimal 200 so the exporter sees success.
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
            }
        });

        let endpoint = format!("http://{addr}");
        // Long interval so the only push is the explicit flush below.
        let m = Metrics::with_otlp("t", &endpoint, Duration::from_secs(3600)).unwrap();
        m.connection_opened("5");
        m.gossip_rejected("replay");
        m.flush();

        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("OTLP export never reached the endpoint")
            .unwrap();
        let req = captured.lock().await.clone();
        // The exporter POSTed to /v1/metrics and the serialized payload carries our
        // service name and at least one recorded instrument.
        assert!(
            req.contains("/v1/metrics"),
            "not a /v1/metrics request:\n{req}"
        );
        assert!(
            req.contains("mqttd"),
            "OTLP payload missing service.name:\n{req}"
        );
        assert!(
            req.contains("connections") || req.contains("gossip_rejected"),
            "OTLP payload missing a recorded instrument:\n{req}"
        );
        // The Prometheus endpoint still works alongside OTLP.
        assert!(m
            .render()
            .contains("mqttd_connections_total{protocol=\"5\"} 1"));
    }
}
