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
struct StoreLabel {
    store: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabel {
    outcome: String,
    trigger: String,
}

/// `{axis}` label for resource-watermark state gauges (ADR 0054): a bounded set —
/// `disk` today, `memory` when the ADR 0041 amendment's RSS watermark lands.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct AxisLabel {
    axis: String,
}

/// `{cluster_id}` label for `mqttd_cluster_info` (ADR 0054 T2): exactly one live
/// series per node (the `build_info` pattern) — the value every node in a healthy
/// cluster must agree on. Two values across a fleet = split brain.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ClusterIdLabel {
    cluster_id: String,
}

/// `{checksum}` label for `mqttd_config_info` (ADR 0054 T3): the sha-256 of the
/// loaded config file. One live series; the previous series is zeroed on change
/// so a scrape shows exactly one checksum at 1 per node.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ChecksumLabel {
    checksum: String,
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
    replication_desired: OtelGauge<i64>,
    replication_min_actual: OtelGauge<i64>,
    replication_write_floor: OtelGauge<i64>,
    retained_tombstones: OtelGauge<i64>,
    members: OtelGauge<i64>,
    lease_leader: OtelGauge<i64>,
    lease_epoch: OtelGauge<i64>,
    durable_append_latency: OtelHistogram<f64>,
    http_auth_latency: OtelHistogram<f64>,
    durable_append_failures: OtelCounter<u64>,
    durable_recovery_failures: OtelCounter<u64>,
    lease_quorum_ack_ms: OtelGauge<i64>,
    gossip_rejected: OtelCounter<u64>,
    security_reloads: OtelCounter<u64>,
    revocation_evictions: OtelCounter<u64>,
    admission_rejected: OtelCounter<u64>,
    quota_rejections: OtelCounter<u64>,
    store_bytes: OtelGauge<i64>,
    quic_path_migrations: OtelCounter<u64>,
    retained_divergence: OtelCounter<u64>,
    retained_apply_failed: OtelCounter<u64>,
    retained_queue_dropped: OtelCounter<u64>,
    brownout: OtelGauge<i64>,
    store_max_bytes: OtelGauge<i64>,
    process_resident_bytes: OtelGauge<i64>,
    memory_max_bytes: OtelGauge<i64>,
    decommission_state: OtelGauge<i64>,
    decommission_pending: OtelGauge<i64>,
    voters: OtelGauge<i64>,
    replica_groups_current: OtelGauge<i64>,
    replica_groups_tracked: OtelGauge<i64>,
    cluster_info: OtelGauge<i64>,
    founder: OtelGauge<i64>,
    refound_quarantine: OtelGauge<i64>,
    foundings: OtelCounter<u64>,
    config_info: OtelGauge<i64>,
    swim_keys_accepted: OtelGauge<i64>,
    peer_proto_min: OtelGauge<i64>,
    peer_proto_max: OtelGauge<i64>,
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
            replication_desired: meter.i64_gauge("replication_desired").build(),
            replication_min_actual: meter.i64_gauge("replication_min_actual").build(),
            replication_write_floor: meter.i64_gauge("replication_write_floor").build(),
            retained_tombstones: meter.i64_gauge("retained_tombstones").build(),
            members: meter.i64_gauge("members").build(),
            lease_leader: meter.i64_gauge("lease_leader").build(),
            lease_epoch: meter.i64_gauge("lease_epoch").build(),
            http_auth_latency: meter.f64_histogram("http_auth_latency_seconds").build(),
            durable_append_latency: meter
                .f64_histogram("durable_append_latency_seconds")
                .build(),
            durable_append_failures: meter.u64_counter("durable_append_failures").build(),
            durable_recovery_failures: meter.u64_counter("durable_recovery_failures").build(),
            lease_quorum_ack_ms: meter.i64_gauge("lease_quorum_ack_ms").build(),
            gossip_rejected: meter.u64_counter("gossip_rejected").build(),
            security_reloads: meter.u64_counter("security_reloads").build(),
            revocation_evictions: meter.u64_counter("revocation_evictions").build(),
            admission_rejected: meter.u64_counter("admission_rejected").build(),
            quota_rejections: meter.u64_counter("quota_rejections").build(),
            store_bytes: meter.i64_gauge("store_bytes").build(),
            quic_path_migrations: meter.u64_counter("quic_path_migrations").build(),
            retained_divergence: meter.u64_counter("retained_divergence").build(),
            retained_apply_failed: meter.u64_counter("retained_apply_failed").build(),
            retained_queue_dropped: meter.u64_counter("retained_queue_dropped").build(),
            brownout: meter.i64_gauge("brownout").build(),
            store_max_bytes: meter.i64_gauge("store_max_bytes").build(),
            process_resident_bytes: meter.i64_gauge("process_resident_bytes").build(),
            memory_max_bytes: meter.i64_gauge("memory_max_bytes").build(),
            decommission_state: meter.i64_gauge("decommission_state").build(),
            decommission_pending: meter.i64_gauge("decommission_pending").build(),
            voters: meter.i64_gauge("voters").build(),
            replica_groups_current: meter.i64_gauge("replica_groups_current").build(),
            replica_groups_tracked: meter.i64_gauge("replica_groups_tracked").build(),
            cluster_info: meter.i64_gauge("cluster_info").build(),
            founder: meter.i64_gauge("founder").build(),
            refound_quarantine: meter.i64_gauge("refound_quarantine").build(),
            foundings: meter.u64_counter("foundings").build(),
            config_info: meter.i64_gauge("config_info").build(),
            swim_keys_accepted: meter.i64_gauge("swim_keys_accepted").build(),
            peer_proto_min: meter.i64_gauge("peer_proto_min").build(),
            peer_proto_max: meter.i64_gauge("peer_proto_max").build(),
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
    replication_desired: Gauge,
    replication_min_actual: Gauge,
    replication_write_floor: Gauge,
    retained_tombstones: Gauge,
    peer_links: Gauge,
    members_by_state: Family<StateLabel, Gauge>,
    lease_leader: Gauge,
    lease_epoch: Gauge,
    durable_append_latency_seconds: Histogram,
    /// HTTP auth hook round-trip latency in seconds (ADR 0004 T16). On the CONNECT path,
    /// so its tail IS connection-setup latency.
    http_auth_latency_seconds: Histogram,
    /// HTTP auth hook outcomes by result (`allow`, `deny`, `error`, `cache-hit`).
    /// `error` is a denial — the hook failed closed.
    http_auth_outcomes_total: Family<ReasonLabel, Counter>,
    durable_append_failures_total: Family<ReasonLabel, Counter>,
    /// Durable session *recovery* refusals (ADR 0049): a persistent attach that stayed
    /// unavailable past its deadline and got CONNACK 0x88. Distinct from an *append*
    /// failure — the signal that was silent for 11 h in the 2026-07-14 incident.
    durable_recovery_failures_total: Family<ReasonLabel, Counter>,
    /// Milliseconds since the lease-group leader last had a quorum ack (ADR 0049),
    /// mirrored from openraft. A growing value is the fsync-bound degradation that
    /// preceded the incident — alertable before any session is refused.
    lease_quorum_ack_ms: Gauge,
    gossip_rejected_total: Family<ReasonLabel, Counter>,
    security_reloads_total: Family<OutcomeLabel, Counter>,
    revocation_evictions_total: Family<ReasonLabel, Counter>,
    admission_rejected_total: Family<ReasonLabel, Counter>,
    quota_rejections_total: Family<ReasonLabel, Counter>,
    store_bytes: Family<StoreLabel, Gauge>,
    quic_path_migrations_total: Counter,
    retained_divergence_total: Counter,
    retained_apply_failed_total: Counter,
    retained_queue_dropped_total: Counter,
    /// Brownout STATE (ADR 0054): 1 while growth writes are refused on `axis`
    /// (`disk`, `memory`), 0 otherwise. The rejection counters record symptoms; this
    /// gauge is the condition itself — an idle browned-out broker is visible.
    brownout: Family<AxisLabel, Gauge>,
    /// The configured disk high-water mark in bytes (0 = no watermark), so
    /// utilization is computable from `store_bytes / store_max_bytes` in `PromQL`.
    store_max_bytes: Gauge,
    /// This process's resident set size in bytes (ADR 0041 T8). Absent on platforms
    /// where RSS cannot be read, rather than reported as zero.
    process_resident_bytes: Gauge,
    /// The configured memory high-water mark in bytes (0 = no watermark), so headroom
    /// is `process_resident_bytes / memory_max_bytes` in `PromQL`.
    memory_max_bytes: Gauge,
    /// Decommission drain state (ADR 0054): 0 = none, 1 = draining, 2 = complete.
    decommission_state: Gauge,
    /// Hand-offs still pending in an active decommission drain.
    decommission_pending: Gauge,
    /// Current lease-group voter count (previously only in the `/readyz` body).
    voters: Gauge,
    /// Replica catch-up summary (ADR 0054): of the replicated groups this node
    /// tracks, how many list it as caught up. `tracked - current` is this node's
    /// replication lag in groups — the takeover-safety signal.
    replica_groups_current: Gauge,
    replica_groups_tracked: Gauge,
    /// The cluster identity (ADR 0054 T2), `build_info`-style: one series with the
    /// id as its label, value 1. Every node in a healthy cluster agrees on it.
    cluster_info: Family<ClusterIdLabel, Gauge>,
    /// 1 when this node is the cluster founder (started seedless), else 0.
    founder: Gauge,
    refound_quarantine: Gauge,
    /// Founding events: this process minted a NEW cluster identity. Exactly one,
    /// ever, on a healthy cluster's first boot — any increment after day one is
    /// the split-brain alarm.
    foundings_total: Counter,
    /// The loaded config's checksum (ADR 0054 T3), `build_info`-style. The
    /// convergence check: after a config roll, every node reports the same value.
    config_info: Family<ChecksumLabel, Gauge>,
    /// The previously exported checksum label, zeroed when a reload changes it
    /// (so exactly one series is ever at 1).
    config_info_prev: std::sync::Mutex<Option<String>>,
    /// How many SWIM gossip keys this node currently accepts (ADR 0054 T3):
    /// 1 = steady state, 2 = a rotation window is open. Alert when it stays > 1
    /// longer than a rotation should take.
    swim_keys_accepted: Gauge,
    /// The peer-bus protocol range this build speaks (ADR 0038/0054) — a mixed-
    /// version fleet is visible per node.
    peer_proto_min: Gauge,
    peer_proto_max: Gauge,
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
    /// `instance_id` (the node id) becomes the `OTel` resource `service.instance.id` — without
    /// it every node in a cluster pushes the *same* series identity (`service.name=mqttd`),
    /// and their cumulative counters collide into one meaningless stream at the backend.
    /// Collectors map it to the Prometheus `instance` label, matching the scraped path.
    ///
    /// # Errors
    /// Returns an error if the OTLP exporter cannot be built (e.g. a malformed endpoint).
    pub fn with_otlp(
        version: &str,
        endpoint: &str,
        interval: std::time::Duration,
        instance_id: &str,
    ) -> Result<Self, opentelemetry_otlp::ExporterBuildError> {
        let provider = build_otlp_provider(endpoint, interval, instance_id)?;
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
            "Messages dropped, by reason (no-subscriber, queue-overflow, backlog-overflow, outbound-full, outbound-id-write-failed, pending-cap, brownout, too-large, retained-replay-client-offline, retained-replay-read-failed)",
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
        let replication_desired = register_gauge(
            &mut registry,
            "replication_desired",
            "Configured replication factor R for durable placement groups",
        );
        let replication_min_actual = register_gauge(
            &mut registry,
            "replication_min_actual",
            "Smallest replica-set size any placement group currently has — the \
             worst-case durability; below replication_desired means at least one \
             group is under-replicated (issue #167)",
        );
        let replication_write_floor = register_gauge(
            &mut registry,
            "replication_write_floor",
            "Smallest replica set a durable append may commit on (issue #239); \
             replication_min_actual < replication_write_floor means durable writes \
             are being REFUSED — QoS>=1 publishers get no ack and redeliver, \
             retained mutations queue",
        );
        let retained_tombstones = register_gauge(
            &mut registry,
            "retained_tombstones",
            "Retained tombstone fences currently held awaiting cluster-wide \
             convergence (issue #229); sustained growth means an absent durable \
             member or chronic divergence",
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
        let http_auth_latency_seconds = register_latency_histogram(
            &mut registry,
            "http_auth_latency_seconds",
            "HTTP authentication hook round-trip latency (ADR 0004 T16). This sits on the \
             CONNECT path, so its tail is connection-setup latency",
        );
        let http_auth_outcomes_total = register_family(
            &mut registry,
            "http_auth_outcomes",
            "HTTP auth hook outcomes, by result (allow, deny, error, cache-hit). `error` \
             is a DENIAL — the hook failed closed",
        );
        let durable_append_failures_total = register_family(
            &mut registry,
            "durable_append_failures",
            "Durable append failures, by reason (no-quorum, not-owner, backend)",
        );
        let durable_recovery_failures_total = register_family(
            &mut registry,
            "durable_recovery_failures",
            "Durable session recovery refusals (persistent attach rejected with 0x88), by reason",
        );
        let lease_quorum_ack_ms = register_gauge(
            &mut registry,
            "lease_quorum_ack_ms",
            "Milliseconds since the lease-group leader last had a quorum ack (0 when not leader)",
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
        let revocation_evictions_total = register_family(
            &mut registry,
            "revocation_evictions",
            "Live state revoked by a policy-reload sweep (ADR 0040), by kind \
             (cert-revoked, user-removed, connect-denied, grant-revoked, peer-revoked)",
        );
        let admission_rejected_total = register_family(
            &mut registry,
            "admission_rejected",
            "Connections refused at accept by an admission cap (ADR 0041), by reason \
             (max-connections, per-ip)",
        );
        let quota_rejections_total = register_family(
            &mut registry,
            "quota_rejections",
            "Operations refused by a per-client or global quota (ADR 0041), by reason \
             (subscriptions, retained, sessions, brownout, brownout-publish)",
        );
        let store_bytes = register_gauge_family(
            &mut registry,
            "store_bytes",
            "On-disk size of each redb store in bytes (ADR 0041 T5), by store \
             (sessions, retained, replicas, lease)",
        );
        let quic_path_migrations_total = register_counter(
            &mut registry,
            "quic_path_migrations",
            "QUIC connection path migrations observed (client address changed; same connection and session kept)",
        );
        let retained_apply_failed_total = register_counter(
            &mut registry,
            "retained_apply_failed",
            "Committed retained updates the local store REFUSED to write. Non-zero means \
             this node is missing retained values its peers hold; the topic is repaired by \
             the next commit or the periodic digest, but a persistent rate means the store \
             is unhealthy and this node serves stale retained state",
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

        let brownout = register_gauge_family(
            &mut registry,
            "brownout",
            "1 while growth writes are refused on this axis (ADR 0041 §5 / ADR 0054), \
             by axis (disk, memory); 0 otherwise — the state, not the rejection symptoms",
        );
        let store_max_bytes = register_gauge(
            &mut registry,
            "store_max_bytes",
            "The configured disk high-water mark in bytes (MQTTD_STORE_MAX_BYTES); \
             0 = no watermark configured",
        );
        let process_resident_bytes = register_gauge(
            &mut registry,
            "process_resident_bytes",
            "Resident set size of the broker process in bytes (ADR 0041 T8)",
        );
        let memory_max_bytes = register_gauge(
            &mut registry,
            "memory_max_bytes",
            "The configured memory high-water mark in bytes (MQTTD_MEMORY_MAX_BYTES); \
             0 = no watermark configured",
        );
        let decommission_state = register_gauge(
            &mut registry,
            "decommission_state",
            "Decommission drain state (ADR 0043/0054): 0 = none, 1 = draining, 2 = complete",
        );
        let decommission_pending = register_gauge(
            &mut registry,
            "decommission_pending",
            "Hand-offs still pending in an active decommission drain",
        );
        let voters = register_gauge(
            &mut registry,
            "voters",
            "Current lease-group voter count (ADR 0049/0054)",
        );
        let replica_groups_current = register_gauge(
            &mut registry,
            "replica_groups_current",
            "Replicated groups this node tracks that list it as caught up (ADR 0054); \
             tracked minus current is this node's replication lag in groups",
        );
        let replica_groups_tracked = register_gauge(
            &mut registry,
            "replica_groups_tracked",
            "Replicated groups this node tracks a caught-up set for (ADR 0054)",
        );

        let cluster_info = register_gauge_family(
            &mut registry,
            "cluster_info",
            "The cluster identity this node belongs to (ADR 0054 T2), as a \
             build_info-style label; two distinct values across a fleet = split brain",
        );
        let founder = register_gauge(
            &mut registry,
            "founder",
            "1 if this node founded the cluster (started seedless), else 0",
        );
        let refound_quarantine = register_gauge(
            &mut registry,
            "refound_quarantine",
            "1 if this node re-founded a cluster beside a live one and has taken \
             ITSELF out of rotation (it will not become ready again without the \
             documented wipe-and-rejoin), else 0",
        );
        let foundings_total = register_counter(
            &mut registry,
            "foundings",
            "Cluster-identity foundings by this process; anything beyond the very \
             first boot of a brand-new cluster indicates a split-brain founding",
        );

        let config_info = register_gauge_family(
            &mut registry,
            "config_info",
            "Checksum of the loaded config file (ADR 0054 T3), build_info-style; \
             after a config roll every node must report the same value",
        );
        let swim_keys_accepted = register_gauge(
            &mut registry,
            "swim_keys_accepted",
            "SWIM gossip keys currently accepted (ADR 0054 T3): 1 steady, 2 = a \
             rotation window is open",
        );
        let peer_proto_min = register_gauge(
            &mut registry,
            "peer_proto_min",
            "Oldest peer-bus protocol version this build speaks (ADR 0038)",
        );
        let peer_proto_max = register_gauge(
            &mut registry,
            "peer_proto_max",
            "Newest peer-bus protocol version this build speaks (ADR 0038)",
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
            replication_desired,
            replication_min_actual,
            replication_write_floor,
            retained_tombstones,
            members_by_state,
            lease_leader,
            lease_epoch,
            durable_append_latency_seconds,
            http_auth_latency_seconds,
            http_auth_outcomes_total,
            durable_append_failures_total,
            durable_recovery_failures_total,
            lease_quorum_ack_ms,
            gossip_rejected_total,
            security_reloads_total,
            revocation_evictions_total,
            admission_rejected_total,
            quota_rejections_total,
            store_bytes,
            quic_path_migrations_total,
            retained_divergence_total,
            retained_apply_failed_total,
            retained_queue_dropped_total,
            brownout,
            store_max_bytes,
            process_resident_bytes,
            memory_max_bytes,
            decommission_state,
            decommission_pending,
            voters,
            replica_groups_current,
            replica_groups_tracked,
            cluster_info,
            founder,
            refound_quarantine,
            foundings_total,
            config_info,
            config_info_prev: std::sync::Mutex::new(None),
            swim_keys_accepted,
            peer_proto_min,
            peer_proto_max,
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

    /// A message was dropped. `reason` is a **bounded** class — the label set is fixed by
    /// these call sites, never by anything a client controls:
    ///
    /// | reason | where |
    /// |---|---|
    /// | `no-subscriber` | nothing matched the topic |
    /// | `queue-overflow` | the durable session queue hit its cap (ADR 0001 §6) |
    /// | `backlog-overflow` | the flow-control backlog hit `MAX_BACKLOG` (ADR 0012) |
    /// | `outbound-full` | a `QoS` 0 shed for a subscriber that stopped reading (#123) |
    /// | `pending-cap` | the pending-publish table hit `PENDING_PUBLISH_CAP`, so the oldest unacknowledged publish was dropped and its publisher's ack withheld (ADR 0042 T9) |
    /// | `brownout` | a durable copy lost above the watermark that NOBODY was told about: a `QoS` 0 offline enqueue (nothing was owed), or an UNGATED publish with no publisher to answer — a Will, a retained-window back-fill — whose live delivery still happens. A `QoS` >= 1 refusal a publisher IS told about is `quota_rejections_total{reason="brownout-publish"}` instead, because it was answered rather than lost (issue #238) |
    /// | `too-large` | the encoded packet exceeded that subscriber's Maximum Packet Size |
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

    /// Set cluster-wide replication health (issues #167, #239): the configured
    /// replication factor, the smallest replica set any placement group currently has,
    /// and the resolved write floor.
    ///
    /// Two distinct operator conditions, which is why all three gauges exist:
    /// `min_actual < desired` is **warn** (a group holds fewer copies than configured —
    /// restore the node), and `min_actual < write_floor` is **page** (durable writes are
    /// being REFUSED: QoS>=1 publishers get no ack and redeliver, retained mutations
    /// queue). The floor is uniform across groups and `min_actual` is the minimum over
    /// groups, so the second condition is exactly "some group is refusing".
    pub fn set_replication_health(&self, desired: usize, min_actual: usize, write_floor: usize) {
        self.replication_desired.set(clamp_gauge(desired));
        self.replication_min_actual.set(clamp_gauge(min_actual));
        self.replication_write_floor.set(clamp_gauge(write_floor));
        self.otel
            .replication_desired
            .record(clamp_gauge(desired), &[]);
        self.otel
            .replication_min_actual
            .record(clamp_gauge(min_actual), &[]);
        self.otel
            .replication_write_floor
            .record(clamp_gauge(write_floor), &[]);
    }

    /// Set the count of retained tombstone fences held awaiting cluster-wide
    /// convergence (issue #229).
    pub fn set_retained_tombstones(&self, n: usize) {
        self.retained_tombstones.set(clamp_gauge(n));
        self.otel.retained_tombstones.record(clamp_gauge(n), &[]);
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

    /// Observe an HTTP auth hook round trip in seconds (ADR 0004 T16).
    pub fn observe_http_auth_latency(&self, seconds: f64) {
        self.http_auth_latency_seconds.observe(seconds);
        self.otel.http_auth_latency.record(seconds, &[]);
    }

    /// Record an HTTP auth hook outcome. `outcome` is a bounded class: `"allow"`,
    /// `"deny"`, `"error"` (which is also a denial — the hook failed closed), or
    /// `"cache-hit"`.
    pub fn http_auth_outcome(&self, outcome: &str) {
        self.http_auth_outcomes_total
            .get_or_create(&ReasonLabel {
                reason: outcome.to_string(),
            })
            .inc();
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

    /// A durable session recovery was refused (persistent attach rejected with 0x88,
    /// ADR 0049); `reason` is a bounded class (e.g. `"deadline"`). Distinct from an
    /// append failure — this is the signal that stayed silent through the 2026-07-14
    /// incident while the durable plane refused every attach.
    pub fn durable_recovery_failed(&self, reason: &str) {
        self.durable_recovery_failures_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .durable_recovery_failures
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// Set the lease-group quorum-ack age gauge (ADR 0049): milliseconds since the
    /// leader last had a quorum ack, mirrored from openraft each gauge refresh. A
    /// growing value is the fsync-bound degradation that precedes durable refusals.
    pub fn set_lease_quorum_ack_ms(&self, ms: i64) {
        self.lease_quorum_ack_ms.set(ms);
        self.otel.lease_quorum_ack_ms.record(ms, &[]);
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

    /// A connection was refused at accept by an admission cap (ADR 0041 T1),
    /// before any TLS handshake work. Bounded reasons only (`max-connections`,
    /// `per-ip`) — never a per-client or per-address value.
    pub fn admission_rejected(&self, reason: &str) {
        self.admission_rejected_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .admission_rejected
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// The on-disk size of one redb store (ADR 0041 T5). Bounded store names only.
    pub fn set_store_bytes(&self, store: &str, bytes: u64) {
        self.store_bytes
            .get_or_create(&StoreLabel {
                store: store.to_string(),
            })
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
        self.otel.store_bytes.record(
            i64::try_from(bytes).unwrap_or(i64::MAX),
            &[KeyValue::new("store", store.to_string())],
        );
    }

    /// Set the brownout STATE for `axis` (ADR 0054). Bounded axes only (`disk`,
    /// later `memory`).
    pub fn set_brownout(&self, axis: &str, on: bool) {
        self.brownout
            .get_or_create(&AxisLabel {
                axis: axis.to_string(),
            })
            .set(i64::from(on));
        self.otel
            .brownout
            .record(i64::from(on), &[KeyValue::new("axis", axis.to_string())]);
    }

    /// Record the configured disk high-water mark (0 = unset).
    pub fn set_store_max_bytes(&self, bytes: u64) {
        let v = i64::try_from(bytes).unwrap_or(i64::MAX);
        self.store_max_bytes.set(v);
        self.otel.store_max_bytes.record(v, &[]);
    }

    /// Record this process's resident set size in bytes (ADR 0041 T8).
    pub fn set_process_resident_bytes(&self, bytes: u64) {
        let v = i64::try_from(bytes).unwrap_or(i64::MAX);
        self.process_resident_bytes.set(v);
        self.otel.process_resident_bytes.record(v, &[]);
    }

    /// Record the configured memory high-water mark (0 = unset).
    pub fn set_memory_max_bytes(&self, bytes: u64) {
        let v = i64::try_from(bytes).unwrap_or(i64::MAX);
        self.memory_max_bytes.set(v);
        self.otel.memory_max_bytes.record(v, &[]);
    }

    /// Record decommission drain progress: `state` 0 = none, 1 = draining,
    /// 2 = complete; `pending` = hand-offs outstanding.
    pub fn set_decommission(&self, state: i64, pending: usize) {
        self.decommission_state.set(state);
        self.decommission_pending.set(clamp_gauge(pending));
        self.otel.decommission_state.record(state, &[]);
        self.otel
            .decommission_pending
            .record(clamp_gauge(pending), &[]);
    }

    /// Record the current lease-group voter count.
    pub fn set_voters(&self, n: usize) {
        self.voters.set(clamp_gauge(n));
        self.otel.voters.record(clamp_gauge(n), &[]);
    }

    /// Record the replica catch-up summary: of `tracked` groups, `current` list
    /// this node as caught up.
    pub fn set_replica_groups(&self, current: usize, tracked: usize) {
        self.replica_groups_current.set(clamp_gauge(current));
        self.replica_groups_tracked.set(clamp_gauge(tracked));
        self.otel
            .replica_groups_current
            .record(clamp_gauge(current), &[]);
        self.otel
            .replica_groups_tracked
            .record(clamp_gauge(tracked), &[]);
    }

    /// Record the cluster identity (ADR 0054 T2) — one series, value 1.
    pub fn set_cluster_info(&self, cluster_id: &str) {
        self.cluster_info
            .get_or_create(&ClusterIdLabel {
                cluster_id: cluster_id.to_string(),
            })
            .set(1);
        self.otel
            .cluster_info
            .record(1, &[KeyValue::new("cluster_id", cluster_id.to_string())]);
    }

    /// Record whether this node founded the cluster.
    pub fn set_founder(&self, founder: bool) {
        self.founder.set(i64::from(founder));
        self.otel.founder.record(i64::from(founder), &[]);
    }

    /// Publish the re-found self-quarantine state (issue #92 follow-up). `true` means
    /// this node minted its own identity and then heard gossip from another cluster, so
    /// it refuses to serve. Alert on it: unlike an ordinary `NotReady` pod, this one never
    /// recovers on its own.
    pub fn set_refound_quarantine(&self, quarantined: bool) {
        self.refound_quarantine.set(i64::from(quarantined));
        self.otel
            .refound_quarantine
            .record(i64::from(quarantined), &[]);
    }

    /// A founding event: this process minted a new cluster identity.
    pub fn founding(&self) {
        self.foundings_total.inc();
        self.otel.foundings.add(1, &[]);
    }

    /// Record the loaded config's checksum (ADR 0054 T3). Zeroes the previously
    /// exported series so exactly one checksum reads 1 per node.
    ///
    /// # Panics
    /// Never in practice: the internal mutex is only held for these few lines.
    pub fn set_config_info(&self, checksum: &str) {
        let mut prev = self
            .config_info_prev
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if prev.as_deref() == Some(checksum) {
            return;
        }
        if let Some(old) = prev.replace(checksum.to_string()) {
            self.config_info
                .get_or_create(&ChecksumLabel { checksum: old })
                .set(0);
        }
        drop(prev);
        self.config_info
            .get_or_create(&ChecksumLabel {
                checksum: checksum.to_string(),
            })
            .set(1);
        self.otel
            .config_info
            .record(1, &[KeyValue::new("checksum", checksum.to_string())]);
    }

    /// Record how many SWIM gossip keys this node accepts (rotation posture).
    pub fn set_swim_keys_accepted(&self, n: usize) {
        self.swim_keys_accepted.set(clamp_gauge(n));
        self.otel.swim_keys_accepted.record(clamp_gauge(n), &[]);
    }

    /// Record the peer-bus protocol range this build speaks.
    pub fn set_peer_proto(&self, min: u32, max: u32) {
        self.peer_proto_min.set(i64::from(min));
        self.peer_proto_max.set(i64::from(max));
        self.otel.peer_proto_min.record(i64::from(min), &[]);
        self.otel.peer_proto_max.record(i64::from(max), &[]);
    }

    /// An operation was refused by a quota (ADR 0041 T3/T4/T5). Bounded kinds only —
    /// never a per-client value:
    ///
    /// | kind | where |
    /// |---|---|
    /// | `subscriptions` | a SUBSCRIBE beyond the per-session filter cap (T3) |
    /// | `retained` | a retained publish creating a NEW topic beyond the cap (T4) |
    /// | `sessions` | a CONNECT creating a NEW session beyond `max_sessions` (T5) |
    /// | `brownout` | a CONNECT creating a NEW session above a watermark (T5) |
    /// | `brownout-publish` | a `QoS` >= 1 publish whose durable enqueue was refused above a watermark. The publisher is TOLD (v5 `0x97`, v3.1.1 no ack + close), so it is an ANSWERED refusal rather than a silent loss — which is why it is not `publish_dropped` (ADR 0041 T5/T11, issue #238). Whether the message is re-sent is not the broker's to promise: a v5 reason >= 0x80 COMPLETES the packet-id lifecycle, so re-delivery is an application decision, and a v3.1.1 publisher resends only if it used `CleanSession=0`. Counts ATTEMPTS, so a resending publisher increments it once per attempt |
    pub fn quota_rejected(&self, kind: &str) {
        self.quota_rejections_total
            .get_or_create(&ReasonLabel {
                reason: kind.to_string(),
            })
            .inc();
        self.otel
            .quota_rejections
            .add(1, &[KeyValue::new("kind", kind.to_string())]);
    }

    /// A policy-reload sweep revoked live state (ADR 0040): a session evicted
    /// (`cert-revoked` / `user-removed` / `connect-denied`), a subscription grant
    /// removed (`grant-revoked`), or an established peer link torn down
    /// (`peer-revoked`). Bounded kinds only — never a per-client value.
    pub fn revocation_eviction(&self, kind: &str) {
        self.revocation_evictions_total
            .get_or_create(&ReasonLabel {
                reason: kind.to_string(),
            })
            .inc();
        self.otel
            .revocation_evictions
            .add(1, &[KeyValue::new("kind", kind.to_string())]);
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

    /// A committed retained update could not be written to the local store (issue #87).
    ///
    /// The token is deliberately NOT recorded when this fires, so the topic stays
    /// repairable — but the node is serving stale retained state for it until the next
    /// commit or digest repair, and nothing else surfaces that. Alert on a sustained rate.
    pub fn retained_apply_failed(&self) {
        self.retained_apply_failed_total.inc();
        self.otel.retained_apply_failed.add(1, &[]);
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
/// appends `/v1/metrics`); `service.name=mqttd` namespaces the metrics at the backend and
/// `service.instance.id=instance_id` distinguishes this node's series from its cluster peers'
/// (collectors map it to the Prometheus `instance` label).
fn build_otlp_provider(
    endpoint: &str,
    interval: std::time::Duration,
    instance_id: &str,
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
        .with_attribute(KeyValue::new(
            "service.instance.id",
            instance_id.to_string(),
        ))
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
        // Issue #239: the three replication gauges move together, from one setter. The
        // write floor is the alertable one — docs/OPERATIONS.md makes
        // `min_actual < write_floor` the PAGE rule — so its NAME and the fact that it is
        // populated at all are pinned here, where a rename or a dropped `.set()` fails.
        m.set_replication_health(3, 1, 2);
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
        assert!(out.contains("mqttd_replication_desired 3"), "{out}");
        assert!(out.contains("mqttd_replication_min_actual 1"), "{out}");
        assert!(out.contains("mqttd_replication_write_floor 2"), "{out}");
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
        let m = Metrics::with_otlp("t", &endpoint, Duration::from_secs(3600), "node-test").unwrap();
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
        // The per-node resource identity: without service.instance.id every cluster node's
        // series collide at the backend into one meaningless stream.
        assert!(
            req.contains("node-test"),
            "OTLP payload missing service.instance.id:\n{req}"
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
