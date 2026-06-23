//! Prometheus metrics ([ADR 0020](../../../docs/adr/0020-metrics-and-observability.md)).
//!
//! A single [`Metrics`] owns a `prometheus-client` registry plus typed metric handles. It is
//! built once in `main`, shared (`Arc`) into the hub, connection, listener, and cluster code,
//! and rendered as Prometheus text exposition on `GET /metrics`.
//!
//! **Cardinality discipline (ADR 0020 §3):** labels are limited to small fixed sets (qos,
//! protocol version, reason class, member state). There are **no per-client or per-topic
//! labels** — the one real footgun of metrics — so every family is bounded.

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

/// The broker's metric registry and typed handles. Cheap to share behind an `Arc`; all
/// updates are lock-free atomic operations on the metric families.
#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
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
}

impl Metrics {
    /// Build the registry, register every metric, and stamp `mqttd_build_info{version}`.
    // A flat, branch-free list of metric registrations: long by count, not by complexity.
    #[allow(clippy::too_many_lines)]
    #[must_use]
    pub fn new(version: &str) -> Self {
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

        let build_info = Family::<VersionLabel, Gauge>::default();
        registry.register("build_info", "Build information", build_info.clone());
        build_info
            .get_or_create(&VersionLabel {
                version: version.to_string(),
            })
            .set(1);

        Self {
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
    }

    /// A client connection closed.
    pub fn connection_closed(&self) {
        self.connections_active.dec();
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
    }

    /// A connection failed to set up (`reason` is a bounded class, e.g. `"tls"`, `"auth"`).
    pub fn connection_error(&self, reason: &str) {
        self.connection_errors_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
    }

    /// A PUBLISH was received from a client at `qos` (0/1/2).
    pub fn publish_received(&self, qos: u8) {
        self.publish_received_total
            .get_or_create(&QosLabel {
                qos: qos.to_string(),
            })
            .inc();
    }

    /// A PUBLISH was delivered to a subscriber at `qos` (0/1/2).
    pub fn publish_delivered(&self, qos: u8) {
        self.publish_delivered_total
            .get_or_create(&QosLabel {
                qos: qos.to_string(),
            })
            .inc();
    }

    /// A message was dropped (`reason` a bounded class: `"expired"`, `"queue-overflow"`,
    /// `"no-subscriber"`).
    pub fn publish_dropped(&self, reason: &str) {
        self.publish_dropped_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
    }

    /// Observe a publish-to-deliver latency in seconds.
    pub fn observe_deliver_latency(&self, seconds: f64) {
        self.deliver_latency_seconds.observe(seconds);
    }

    /// Set the current session count (snapshot of an in-memory map; ADR 0020).
    pub fn set_sessions(&self, n: usize) {
        self.sessions.set(clamp_gauge(n));
    }

    /// Set the current active-subscription count.
    pub fn set_subscriptions(&self, n: usize) {
        self.subscriptions.set(clamp_gauge(n));
    }

    /// Set the current retained-message count.
    pub fn set_retained_messages(&self, n: usize) {
        self.retained_messages.set(clamp_gauge(n));
    }

    /// Set the current count of unacknowledged QoS>0 messages outstanding to clients.
    pub fn set_inflight_messages(&self, n: usize) {
        self.inflight_messages.set(clamp_gauge(n));
    }

    /// Set the current count of placement-eligible cluster members (ADR 0020-T6).
    pub fn set_cluster_members(&self, n: usize) {
        self.cluster_members.set(clamp_gauge(n));
    }

    /// Set the current count of connected inter-node peer links.
    pub fn set_peer_links(&self, n: usize) {
        self.peer_links.set(clamp_gauge(n));
    }

    /// Set the member count for one bounded SWIM `state` (`"alive"`/`"suspect"`/`"dead"`).
    pub fn set_members_in_state(&self, state: &str, n: usize) {
        self.members_by_state
            .get_or_create(&StateLabel {
                state: state.to_string(),
            })
            .set(clamp_gauge(n));
    }

    /// Record this node's lease-group role (`leader`) and consensus epoch (term).
    pub fn set_lease_role(&self, is_leader: bool, epoch: u64) {
        self.lease_leader.set(i64::from(is_leader));
        self.lease_epoch.set(clamp_gauge_u64(epoch));
    }

    /// Observe a durable (quorum) append latency in seconds.
    pub fn observe_durable_append_latency(&self, seconds: f64) {
        self.durable_append_latency_seconds.observe(seconds);
    }

    /// A durable append failed; `reason` is a bounded class (`"no-quorum"`, `"not-owner"`,
    /// `"backend"`).
    pub fn durable_append_failed(&self, reason: &str) {
        self.durable_append_failures_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
    }
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
}
