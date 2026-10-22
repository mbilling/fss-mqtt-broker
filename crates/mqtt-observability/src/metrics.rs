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

use crate::validity::{unix_secs, ValidityWindow};

/// The validity windows behind the TLS expiry gauges (ADR 0088 D2), by role. Shared
/// between the Prometheus render and the OTLP callbacks, which each compute the gauges
/// from it at the moment they are read.
type TlsValidity = std::sync::Arc<std::sync::Mutex<Vec<(&'static str, ValidityWindow)>>>;

/// `{role}` for the TLS expiry gauges (ADR 0088 D2): which loaded material the series is
/// about, from a fixed set — `listener`, `client_ca`, `client_crl`, `peer`, `peer_ca`,
/// `peer_crl`, `admin`, `admin_ca`. Never a file path, a subject or a serial.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RoleLabel {
    role: String,
}

/// `{version}` label for `mqttd_build_info`.
/// `{peer, stat}` for `mqttd_peer_link_stat` (#662): one inter-node link's
/// cumulative work counters, `stat` a bounded set (see `set_peer_link_stat`).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PeerLinkStatLabel {
    peer: String,
    stat: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct VersionLabel {
    version: String,
}

/// `{namespace}` for the replication series of rule state (ADR 0085 D10): a name from
/// the operator's rules file (a declared namespace, or `rule:<id>`), never from a
/// client. The same bounded exception to ADR 0020 as the `rule` label.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StateNamespaceLabel {
    namespace: String,
}

/// `{namespace, reason}` for `mqttd_rule_state_replication_refused_total`: `reason` is
/// one of a fixed set (see `set_rule_state_replication_refused`).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StateRefusedLabel {
    namespace: String,
    reason: String,
}

/// `{module}` label for `crypto_module_info` (ADR 0068): which crypto module
/// the running binary uses — the metric half of the fips build's runtime
/// visibility (version line and startup log are the other two).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CryptoModuleLabel {
    module: String,
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

/// `{tier}` label — the ADR 0072 durability tiers: `quorum`, `local`, `relaxed`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TierLabel {
    tier: String,
}

/// `{reason}` label — a small fixed set of reason classes (never free-form text).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ReasonLabel {
    reason: String,
}

/// `{lane}` label — a bounded set: `control`, `data` (ADR 0082 T2).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct LaneLabel {
    lane: String,
}

/// `{listener}` label — a bounded set: `tls`, `plaintext`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ListenerLabel {
    listener: String,
}

/// `{stage}` label for `mqttd_durable_stage_seconds` — a bounded set, one per stage of a
/// durable append ([`DURABLE_STAGES`]).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StageLabel {
    stage: String,
}

/// `{probe}` label for `mqttd_runtime_wake_seconds` (#662): `yield` or `channel`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ProbeLabel {
    probe: String,
}

/// Every `{stage}` value the broker records. Each one's histogram handle is resolved
/// once and cached ([`Metrics::observe_durable_stage`] runs several times per durable
/// append); a label outside this set still works, through the family lookup.
const DURABLE_STAGES: [&str; 10] = [
    "lane_queue",
    "local_durable",
    "quorum",
    "order",
    "commit",
    "fsync",
    "replicate_rtt",
    "replica_apply",
    "replicate_queue",
    "ack_queue",
];

/// `{command}` label for `mqttd_hub_dispatch_seconds` (issue #242) — the COARSE hub
/// command classes, a bounded 7-value set (`attach`, `publish`, `ack`, `subscribe`,
/// `control`, `cluster`, `sweep`), never per-variant.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CommandLabel {
    command: String,
}

/// `{locality}` label for `mqttd_shared_selected_total` (issue #613 item 3.5) —
/// exactly two values: `local`, `remote`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct LocalityLabel {
    locality: String,
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

/// `{outcome}` label for `mqttd_backup_runs_total` (ADR 0062): a bounded pair — `ok` for
/// an export renamed into place, `error` for a run that wrote nothing (an incomplete
/// session scan, an unwritable directory). A run counted `error` deliberately does NOT
/// advance `backup_last_success_timestamp_seconds`, so the RPO alert fires.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RunOutcomeLabel {
    outcome: String,
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

/// `{rule, result}` for `mqttd_rule_evaluations_total` and `mqttd_rule_actions_total`
/// (ADR 0083). The one label here that is not a fixed vocabulary, and still bounded:
/// `rule` is a rule id from the operator's rules file (at most 1024 of them), never a
/// value a client or a topic can mint. `result` is a fixed set per family.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RuleResultLabel {
    rule: String,
    result: String,
}

/// `{rule, guard}` for `mqttd_rule_recursive_republish_total` (ADR 0083): a rule id,
/// bounded as in [`RuleResultLabel`], and `same_rule` or `depth`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RuleGuardLabel {
    rule: String,
    guard: String,
}

/// `{rule}` for `mqttd_rule_eval_seconds_total`: a rule id from the operator's rules
/// file, bounded as in [`RuleResultLabel`].
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RuleLabel {
    rule: String,
}

/// `{function}` for the per-function counters of sandboxed rule functions (ADR 0086):
/// `jq`, and later the functions of the modules an operator's rules file declares.
/// Bounded by that file.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct FunctionLabel {
    function: String,
}

/// `{function, kind}` for `mqttd_rule_function_errors_total` (ADR 0086): a function as
/// in [`FunctionLabel`], and one of a fixed set of kinds.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct FunctionKindLabel {
    function: String,
    kind: String,
}

/// `{module}` for the per-module gauges of the rule-function sandbox (ADR 0086).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ModuleLabel {
    module: String,
}

/// `{namespace, op, result}` for `mqttd_rule_state_ops_total` (ADR 0085 D10): a
/// namespace as in [`StateNamespaceLabel`], one of the store's thirteen operations, and
/// `ok` or one of the store's five reasons for refusing a call.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StateOpLabel {
    namespace: String,
    op: String,
    result: String,
}

/// One rule's cumulative counts since the process started (ADR 0084), as
/// `mqttd_rule_evaluations_total` and `mqttd_rule_actions_total` hold them; see
/// [`Metrics::rule_counts`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuleCounts {
    /// Evaluations whose SQL produced output.
    pub passed: u64,
    /// Evaluations whose `WHERE` filtered the message out.
    pub no_result: u64,
    /// Evaluations whose SQL failed.
    pub failed: u64,
    /// Actions that ran: a console line logged, a republish routed.
    pub actions_ok: u64,
    /// Actions that failed: at render time, or a republish refused or not routed.
    pub actions_failed: u64,
    /// Time spent evaluating it, in nanoseconds: its SQL and its actions' rendering,
    /// summed over every live evaluation (`mqttd_rule_eval_seconds_total`).
    pub eval_ns: u64,
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
    publish_resolved_undeliverable: OtelCounter<u64>,
    publish_forwarded: OtelCounter<u64>,
    deliver_latency: OtelHistogram<f64>,
    hub_dispatch: OtelHistogram<f64>,
    hub_fanout: OtelHistogram<f64>,
    hub_fanout_peer_visits: OtelCounter<u64>,
    hub_queue_depth: OtelGauge<i64>,
    hub_lane_depth: OtelGauge<i64>,
    ingress_paused: OtelCounter<u64>,
    ingress_paused_seconds: OtelHistogram<f64>,
    ingress_credit_bytes: OtelGauge<i64>,
    routing_unsettled: OtelGauge<i64>,
    pending_publishes_awaiting_settle: OtelGauge<i64>,
    publish_held_for_unreachable: OtelGauge<i64>,
    shared_selected: OtelCounter<u64>,
    append_lane_jobs: OtelGauge<i64>,
    pending_publishes: OtelGauge<i64>,
    pending_publish_bytes: OtelGauge<i64>,
    sessions: OtelGauge<i64>,
    subscriptions: OtelGauge<i64>,
    retained_messages: OtelGauge<i64>,
    inflight_messages: OtelGauge<i64>,
    backlog_bytes: OtelGauge<i64>,
    backlog_bytes_max: OtelGauge<i64>,
    cluster_members: OtelGauge<i64>,
    peer_links: OtelGauge<i64>,
    peer_forwards_in_flight: OtelGauge<i64>,
    replication_desired: OtelGauge<i64>,
    replication_min_actual: OtelGauge<i64>,
    replication_write_floor: OtelGauge<i64>,
    retained_tombstones: OtelGauge<i64>,
    misplaced_sessions: OtelGauge<i64>,
    members: OtelGauge<i64>,
    lease_leader: OtelGauge<i64>,
    lease_epoch: OtelGauge<i64>,
    durable_append_latency: OtelHistogram<f64>,
    durable_stage: OtelHistogram<f64>,
    publish_ack: OtelHistogram<f64>,
    http_auth_latency: OtelHistogram<f64>,
    durable_append_failures: OtelCounter<u64>,
    durable_elided: OtelCounter<u64>,
    durable_elision_fallbacks: OtelCounter<u64>,
    durable_elision_hold: OtelHistogram<f64>,
    durable_elision_backoffs: OtelCounter<u64>,
    durable_recovery_failures: OtelCounter<u64>,
    lease_quorum_ack_ms: OtelGauge<i64>,
    gossip_rejected: OtelCounter<u64>,
    gossip_cert_requests: OtelCounter<u64>,
    swim_last_probes: OtelCounter<u64>,
    swim_stalls: OtelCounter<u64>,
    swim_stalled_milliseconds: OtelCounter<u64>,
    swim_awareness: OtelGauge<i64>,
    security_reloads: OtelCounter<u64>,
    revocation_evictions: OtelCounter<u64>,
    session_rehomes: OtelCounter<u64>,
    session_expiry_unpersisted: OtelCounter<u64>,
    admission_rejected: OtelCounter<u64>,
    quota_rejections: OtelCounter<u64>,
    store_bytes: OtelGauge<i64>,
    quic_path_migrations: OtelCounter<u64>,
    retained_divergence: OtelCounter<u64>,
    retained_apply_failed: OtelCounter<u64>,
    retained_queue_dropped: OtelCounter<u64>,
    audit_export_dropped: OtelCounter<u64>,
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
    backup_runs: OtelCounter<u64>,
    backup_last_success_timestamp_seconds: OtelGauge<i64>,
    backup_duration_ms: OtelGauge<i64>,
    restore_state: OtelGauge<i64>,
    foundings: OtelCounter<u64>,
    config_info: OtelGauge<i64>,
    rule_evaluations: OtelCounter<u64>,
    rule_actions: OtelCounter<u64>,
    rule_recursive_republish: OtelCounter<u64>,
    rule_eval_seconds: OtelCounter<f64>,
    rule_function_calls: OtelCounter<u64>,
    rule_function_errors: OtelCounter<u64>,
    rule_function_seconds: OtelCounter<f64>,
    rule_module_instances: OtelGauge<i64>,
    rule_module_memory_bytes: OtelGauge<i64>,
    rule_module_instances_replaced: OtelCounter<u64>,
    rule_state_keys: OtelGauge<i64>,
    rule_state_bytes: OtelGauge<i64>,
    rule_state_ops: OtelCounter<u64>,
    rule_state_expired: OtelCounter<u64>,
    rule_state_expiry_backlog: OtelGauge<i64>,
    rule_state_dictionary_bytes: OtelGauge<i64>,
    rule_state_dictionary_refused: OtelCounter<u64>,
    rules_loaded: OtelGauge<i64>,
    rules_info: OtelGauge<i64>,
    swim_keys_accepted: OtelGauge<i64>,
    swim_isolated: OtelGauge<i64>,
    peer_proto_min: OtelGauge<i64>,
    peer_proto_max: OtelGauge<i64>,
    /// Observed at export time from the stored validity windows, so they keep moving
    /// between reloads (and the threshold is in every export, whatever the temporality).
    /// Held so the callbacks stay registered.
    _tls_expiry_warn_ratio: opentelemetry::metrics::ObservableGauge<f64>,
    _tls_expiry_seconds: opentelemetry::metrics::ObservableGauge<i64>,
    _tls_lifetime_spent_ratio: opentelemetry::metrics::ObservableGauge<f64>,
}

impl OtelInstruments {
    /// Create every instrument from `meter`, naming each to match its Prometheus
    /// counterpart (the `mqttd` prefix is carried by the OTLP resource `service.name`).
    #[allow(clippy::too_many_lines)] // one line per instrument
    fn new(
        meter: &Meter,
        tls_validity: &TlsValidity,
        tls_warn_permille: &std::sync::Arc<std::sync::atomic::AtomicU32>,
    ) -> Self {
        let (for_seconds, for_ratio) = (tls_validity.clone(), tls_validity.clone());
        let warn = tls_warn_permille.clone();
        Self {
            _tls_expiry_warn_ratio: meter
                .f64_observable_gauge("tls_expiry_warn_ratio")
                .with_callback(move |observer| {
                    // Zero is "not set": the threshold is at least 1.
                    let permille = warn.load(std::sync::atomic::Ordering::Relaxed);
                    if permille > 0 {
                        observer.observe(f64::from(permille) / 1000.0, &[]);
                    }
                })
                .build(),
            _tls_expiry_seconds: meter
                .i64_observable_gauge("tls_expiry_seconds")
                .with_callback(move |observer| {
                    let now = unix_secs(std::time::SystemTime::now());
                    for (role, window) in lock_validity(&for_seconds).iter() {
                        observer.observe(window.seconds_left(now), &[KeyValue::new("role", *role)]);
                    }
                })
                .build(),
            _tls_lifetime_spent_ratio: meter
                .f64_observable_gauge("tls_lifetime_spent_ratio")
                .with_callback(move |observer| {
                    let now = unix_secs(std::time::SystemTime::now());
                    for (role, window) in lock_validity(&for_ratio).iter() {
                        observer.observe(window.spent(now), &[KeyValue::new("role", *role)]);
                    }
                })
                .build(),
            connections_active: meter.i64_up_down_counter("connections_active").build(),
            connections: meter.u64_counter("connections").build(),
            accepts: meter.u64_counter("accepts").build(),
            connection_errors: meter.u64_counter("connection_errors").build(),
            publish_received: meter.u64_counter("publish_received").build(),
            publish_delivered: meter.u64_counter("publish_delivered").build(),
            publish_dropped: meter.u64_counter("publish_dropped").build(),
            publish_resolved_undeliverable: meter
                .u64_counter("publish_resolved_undeliverable")
                .build(),
            publish_forwarded: meter.u64_counter("publish_forwarded").build(),
            deliver_latency: meter.f64_histogram("deliver_latency_seconds").build(),
            hub_dispatch: meter.f64_histogram("hub_dispatch_seconds").build(),
            hub_fanout: meter.f64_histogram("hub_fanout_seconds").build(),
            hub_fanout_peer_visits: meter.u64_counter("hub_fanout_peer_visits").build(),
            hub_queue_depth: meter.i64_gauge("hub_queue_depth").build(),
            hub_lane_depth: meter.i64_gauge("hub_lane_depth").build(),
            ingress_paused: meter.u64_counter("ingress_paused").build(),
            ingress_paused_seconds: meter.f64_histogram("ingress_paused_seconds").build(),
            ingress_credit_bytes: meter.i64_gauge("ingress_credit_bytes").build(),
            routing_unsettled: meter.i64_gauge("routing_unsettled").build(),
            pending_publishes_awaiting_settle: meter
                .i64_gauge("pending_publishes_awaiting_settle")
                .build(),
            publish_held_for_unreachable: meter.i64_gauge("publish_held_for_unreachable").build(),
            shared_selected: meter.u64_counter("shared_selected").build(),
            append_lane_jobs: meter.i64_gauge("append_lane_jobs").build(),
            pending_publishes: meter.i64_gauge("pending_publishes").build(),
            pending_publish_bytes: meter.i64_gauge("pending_publish_bytes").build(),
            sessions: meter.i64_gauge("sessions").build(),
            subscriptions: meter.i64_gauge("subscriptions").build(),
            retained_messages: meter.i64_gauge("retained_messages").build(),
            inflight_messages: meter.i64_gauge("inflight_messages").build(),
            backlog_bytes: meter.i64_gauge("backlog_bytes").build(),
            backlog_bytes_max: meter.i64_gauge("backlog_bytes_max").build(),
            cluster_members: meter.i64_gauge("cluster_members").build(),
            peer_links: meter.i64_gauge("peer_links").build(),
            peer_forwards_in_flight: meter.i64_gauge("peer_forwards_in_flight").build(),
            replication_desired: meter.i64_gauge("replication_desired").build(),
            replication_min_actual: meter.i64_gauge("replication_min_actual").build(),
            replication_write_floor: meter.i64_gauge("replication_write_floor").build(),
            retained_tombstones: meter.i64_gauge("retained_tombstones").build(),
            misplaced_sessions: meter.i64_gauge("misplaced_sessions").build(),
            members: meter.i64_gauge("members").build(),
            lease_leader: meter.i64_gauge("lease_leader").build(),
            lease_epoch: meter.i64_gauge("lease_epoch").build(),
            http_auth_latency: meter.f64_histogram("http_auth_latency_seconds").build(),
            durable_stage: meter.f64_histogram("durable_stage_seconds").build(),
            publish_ack: meter.f64_histogram("publish_ack_seconds").build(),
            durable_append_latency: meter
                .f64_histogram("durable_append_latency_seconds")
                .build(),
            durable_append_failures: meter.u64_counter("durable_append_failures").build(),
            durable_elided: meter.u64_counter("durable_elided").build(),
            durable_elision_fallbacks: meter.u64_counter("durable_elision_fallback").build(),
            durable_elision_hold: meter.f64_histogram("durable_elision_hold_seconds").build(),
            durable_elision_backoffs: meter.u64_counter("durable_elision_backoff").build(),
            durable_recovery_failures: meter.u64_counter("durable_recovery_failures").build(),
            lease_quorum_ack_ms: meter.i64_gauge("lease_quorum_ack_ms").build(),
            gossip_rejected: meter.u64_counter("gossip_rejected").build(),
            gossip_cert_requests: meter.u64_counter("gossip_cert_requests").build(),
            swim_last_probes: meter.u64_counter("swim_last_probes").build(),
            swim_stalls: meter.u64_counter("swim_stalls").build(),
            swim_stalled_milliseconds: meter.u64_counter("swim_stalled_milliseconds").build(),
            swim_awareness: meter.i64_gauge("swim_awareness").build(),
            security_reloads: meter.u64_counter("security_reloads").build(),
            revocation_evictions: meter.u64_counter("revocation_evictions").build(),
            session_rehomes: meter.u64_counter("session_rehomes").build(),
            session_expiry_unpersisted: meter.u64_counter("session_expiry_unpersisted").build(),
            admission_rejected: meter.u64_counter("admission_rejected").build(),
            quota_rejections: meter.u64_counter("quota_rejections").build(),
            store_bytes: meter.i64_gauge("store_bytes").build(),
            quic_path_migrations: meter.u64_counter("quic_path_migrations").build(),
            retained_divergence: meter.u64_counter("retained_divergence").build(),
            retained_apply_failed: meter.u64_counter("retained_apply_failed").build(),
            retained_queue_dropped: meter.u64_counter("retained_queue_dropped").build(),
            audit_export_dropped: meter.u64_counter("audit_export_dropped").build(),
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
            backup_runs: meter.u64_counter("backup_runs").build(),
            backup_last_success_timestamp_seconds: meter
                .i64_gauge("backup_last_success_timestamp_seconds")
                .build(),
            backup_duration_ms: meter.i64_gauge("backup_duration_ms").build(),
            restore_state: meter.i64_gauge("restore_state").build(),
            foundings: meter.u64_counter("foundings").build(),
            config_info: meter.i64_gauge("config_info").build(),
            rule_evaluations: meter.u64_counter("rule_evaluations").build(),
            rule_actions: meter.u64_counter("rule_actions").build(),
            rule_recursive_republish: meter.u64_counter("rule_recursive_republish").build(),
            rule_eval_seconds: meter.f64_counter("rule_eval_seconds").build(),
            rule_function_calls: meter.u64_counter("rule_function_calls").build(),
            rule_function_errors: meter.u64_counter("rule_function_errors").build(),
            rule_function_seconds: meter.f64_counter("rule_function_seconds").build(),
            rule_module_instances: meter.i64_gauge("rule_module_instances").build(),
            rule_module_memory_bytes: meter.i64_gauge("rule_module_memory_bytes").build(),
            rule_module_instances_replaced: meter
                .u64_counter("rule_module_instances_replaced")
                .build(),
            rule_state_keys: meter.i64_gauge("rule_state_keys").build(),
            rule_state_bytes: meter.i64_gauge("rule_state_bytes").build(),
            rule_state_ops: meter.u64_counter("rule_state_ops").build(),
            rule_state_expired: meter.u64_counter("rule_state_expired").build(),
            rule_state_expiry_backlog: meter.i64_gauge("rule_state_expiry_backlog").build(),
            rule_state_dictionary_bytes: meter.i64_gauge("rule_state_dictionary_bytes").build(),
            rule_state_dictionary_refused: meter
                .u64_counter("rule_state_dictionary_refused")
                .build(),
            rules_loaded: meter.i64_gauge("rules_loaded").build(),
            rules_info: meter.i64_gauge("rules_info").build(),
            swim_keys_accepted: meter.i64_gauge("swim_keys_accepted").build(),
            swim_isolated: meter.i64_gauge("swim_isolated").build(),
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
    publish_resolved_undeliverable_total: Family<ReasonLabel, Counter>,
    publish_forwarded_total: Family<ReasonLabel, Counter>,
    deliver_latency_seconds: Histogram,
    hub_dispatch_seconds: Family<CommandLabel, Histogram>,
    /// Issue #613 item 1.6: the peer fan-out's own time and the number of peer
    /// links it visited, split out of `hub_dispatch_seconds` so "a fan-out walked
    /// zero links" and "no fan-outs happened" are different readings.
    hub_fanout_seconds: Histogram,
    hub_fanout_peer_visits_total: Counter,
    /// Issue #613 item 3.5: the hub's inbound queue depth. The hub is a single
    /// task, so this is its saturation, full stop.
    hub_queue_depth: Gauge,
    /// ADR 0082 T2: the hub's queued commands by lane, `control` and `data`.
    hub_lane_depth: Family<LaneLabel, Gauge>,
    /// ADR 0082 T3: client connections that stopped reading for want of ingress
    /// credit, and for how long each pause lasted.
    ingress_paused_total: Counter,
    ingress_paused_seconds: Histogram,
    /// ADR 0082 T3: the ingress credit pool, by `state` — `in_use` (held by queued
    /// client publishes) and `capacity`.
    ingress_credit_bytes: Family<StateLabel, Gauge>,
    /// Issue #613 item 2.5: one series per term of `routing_unsettled()`, all of
    /// them emitted every scrape so a disappearing series is never read as 0.
    routing_unsettled: Family<ReasonLabel, Gauge>,
    pending_publishes_awaiting_settle: Gauge,
    /// Gated publishes with a forward owed to a node that has no link.
    publish_held_for_unreachable: Gauge,
    /// Issue #613 item 3.5: shared selections by locality, incremented exactly
    /// once per DELIVERED message.
    shared_selected_total: Family<LocalityLabel, Counter>,
    append_lane_jobs: Gauge,
    pending_publishes: Gauge,
    pending_publish_bytes: Gauge,
    sessions: Gauge,
    subscriptions: Gauge,
    retained_messages: Gauge,
    inflight_messages: Gauge,
    backlog_bytes: Gauge,
    backlog_bytes_max: Gauge,
    cluster_members: Gauge,
    replication_desired: Gauge,
    replication_min_actual: Gauge,
    replication_write_floor: Gauge,
    retained_tombstones: Gauge,
    misplaced_sessions: Gauge,
    peer_links: Gauge,
    /// Per-link work counters (`peer_link_stat{peer, stat}`), cumulative per link.
    peer_link_stat: Family<PeerLinkStatLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    /// Replication of `eventual` rule state (ADR 0085 D10), per namespace.
    rule_state_replication_lag_seconds:
        Family<StateNamespaceLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    rule_state_replication_pending: Family<StateNamespaceLabel, Gauge>,
    rule_state_replication_refused_total: Family<StateRefusedLabel, Counter>,
    peer_forwards_in_flight: Gauge,
    members_by_state: Family<StateLabel, Gauge>,
    lease_leader: Gauge,
    lease_epoch: Gauge,
    durable_append_latency_seconds: Histogram,
    /// Each stage of a durable append, finely bucketed (`durable_stage_seconds`).
    durable_stage_seconds: Family<StageLabel, Histogram>,
    /// [`DURABLE_STAGES`]' handles into that family, resolved on first use so an
    /// unrecorded stage still renders no series.
    durable_stage_cache: [std::sync::OnceLock<Histogram>; DURABLE_STAGES.len()],
    /// Server-side publish → PUBACK release for gated (`QoS` 1/2) publishes.
    publish_ack_seconds: Histogram,
    /// Async-runtime scheduling delay, sampled by `mqttd::runtime_probe`.
    runtime_wake_seconds: Family<ProbeLabel, Histogram>,
    /// HTTP auth hook round-trip latency in seconds (ADR 0004 T16). On the CONNECT path,
    /// so its tail IS connection-setup latency.
    http_auth_latency_seconds: Histogram,
    /// HTTP auth hook outcomes by result (`allow`, `deny`, `error`, `cache-hit`).
    /// `error` is a denial — the hook failed closed.
    http_auth_outcomes_total: Family<ReasonLabel, Counter>,
    durable_append_failures_total: Family<ReasonLabel, Counter>,
    durable_elided_total: Counter,
    durable_elision_fallback_total: Family<ReasonLabel, Counter>,
    durable_elision_hold_seconds: Histogram,
    durable_elision_backoff_total: Counter,
    /// Durable session *recovery* refusals (ADR 0049): a persistent attach that stayed
    /// unavailable past its deadline and got CONNACK 0x88. Distinct from an *append*
    /// failure — the signal that was silent for 11 h in the 2026-07-14 incident.
    durable_recovery_failures_total: Family<ReasonLabel, Counter>,
    /// Milliseconds since the lease-group leader last had a quorum ack (ADR 0049),
    /// mirrored from openraft. A growing value is the fsync-bound degradation that
    /// preceded the incident — alertable before any session is refused.
    lease_quorum_ack_ms: Gauge,
    gossip_rejected_total: Family<ReasonLabel, Counter>,
    /// Certificate requests sent: a `Join` to the source of a datagram dropped as
    /// `cert-miss` (issue #971). It follows `gossip_rejected_total{reason="cert-miss"}`
    /// one for one while a peer's certificate is being learned and stops when it is;
    /// `cert-miss` climbing while this stands still is the rate limit holding.
    gossip_cert_requests_total: Counter,
    /// The last direct probes this node sent to suspects before declaring them `Dead`
    /// (ADR 0016), by `outcome`: `sent`; `spared`, the suspect answered and was kept;
    /// `silent`, it did not and was declared `Dead`. No member is named, so the series
    /// stay three. `spared` climbing is a member, or a path to one, that keeps going
    /// quiet for seconds at a time and coming back — the `suspect` gauge shows that
    /// only if it is read at the right moment.
    swim_last_probes_total: Family<RunOutcomeLabel, Counter>,
    /// Times the SWIM driver found that this node itself had been stalled: its loop
    /// was not scheduled for more than two of its ticks. A stall moves no
    /// timer and reaches no verdict about a peer (the driver reads its socket before
    /// it ticks); it is time in which this node answered nobody.
    swim_stalls_total: Counter,
    /// The milliseconds those stalls lasted, summed: `rate()` of it over 1000 is the
    /// fraction of time the membership task was not running, and it over
    /// `swim_stalls_total` is the length of a stall.
    swim_stalled_milliseconds_total: Counter,
    /// This node's local-health awareness (ADR 0016 §2): 0 when healthy, up to 8. Its
    /// ack timeouts and suspicion windows are (1 + this) times the release's, so a
    /// node above 0 is slower to declare a peer dead; it rises when the node has to
    /// refute a suspicion of itself and falls with each probe a peer answers.
    swim_awareness: Gauge,
    security_reloads_total: Family<OutcomeLabel, Counter>,
    revocation_evictions_total: Family<ReasonLabel, Counter>,
    session_rehomes_total: Family<ReasonLabel, Counter>,
    session_expiry_unpersisted_total: Family<ReasonLabel, Counter>,
    admission_rejected_total: Family<ReasonLabel, Counter>,
    quota_rejections_total: Family<ReasonLabel, Counter>,
    store_bytes: Family<StoreLabel, Gauge>,
    quic_path_migrations_total: Counter,
    retained_divergence_total: Counter,
    retained_apply_failed_total: Counter,
    retained_queue_dropped_total: Counter,
    audit_export_dropped_total: Counter,
    /// Publisher-selected durability tiers on gated publishes (ADR 0072), by
    /// tier (`quorum`, `local`, `relaxed`) — non-default tiers appear only
    /// under the operator's `MQTTD_ALLOW_RELAXED_PUBLISH` opt-in.
    publish_tier_total: Family<TierLabel, Counter>,
    /// Durable-write serializer (ADR 0071): fsync'd batches committed and ops
    /// applied across them (owner appends + follower replica applies share one
    /// writer). ops/batches = mean group-commit batch size.
    durable_writer_batches_total: Counter,
    durable_writer_ops_total: Counter,
    /// Largest single batch since boot — how deep the coalescing gets under load.
    durable_writer_max_batch: Gauge,
    durable_writer_commit_micros_total: Counter,
    store_barrier_floor: Gauge,
    store_barrier_floor_4stream: Gauge,
    /// Files the replica store spans (ADR 0076 T2) and, when the volume's
    /// measurements outgrow it, the count they suggest — advisory only.
    store_shards: Gauge,
    store_reshard_advice: Gauge,
    crypto_module_info: Family<CryptoModuleLabel, Gauge>,
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
    backup_runs_total: Family<RunOutcomeLabel, Counter>,
    backup_last_success_timestamp_seconds: Gauge,
    backup_duration_ms: Gauge,
    restore_state: Gauge,
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
    /// Rule-engine evaluations (ADR 0083), by rule and result: `passed`, `no_result`
    /// (the FROM matched, the WHERE did not), `failed` (the SQL raised an error).
    rule_evaluations_total: Family<RuleResultLabel, Counter>,
    /// Rule actions run (ADR 0083), by rule and result (`ok`, `failed`).
    rule_actions_total: Family<RuleResultLabel, Counter>,
    /// Republish actions not run because their message would loop (ADR 0083), by rule
    /// and guard (`same_rule`, `depth`).
    rule_recursive_republish_total: Family<RuleGuardLabel, Counter>,
    /// Time spent evaluating each rule, in seconds (ADR 0084).
    rule_eval_seconds_total: Family<RuleLabel, Counter<f64, std::sync::atomic::AtomicU64>>,
    /// Calls of sandboxed rule functions (`jq`, ADR 0086), by function.
    rule_function_calls_total: Family<FunctionLabel, Counter>,
    /// Those that failed, by function and kind.
    rule_function_errors_total: Family<FunctionKindLabel, Counter>,
    /// Time spent in them, in seconds, by function.
    rule_function_seconds_total: Family<FunctionLabel, Counter<f64, std::sync::atomic::AtomicU64>>,
    /// Sandbox instances held per module, and their linear memory (ADR 0086).
    rule_module_instances: Family<ModuleLabel, Gauge>,
    rule_module_memory_bytes: Family<ModuleLabel, Gauge>,
    /// Instances given up after a call, per module.
    rule_module_instances_replaced_total: Family<ModuleLabel, Counter>,
    /// The rule state store (ADR 0085 D10), per namespace: keys held, bytes accounted,
    /// calls by operation and result, keys removed at their expiry, and keys past
    /// their expiry that are still held.
    rule_state_keys: Family<StateNamespaceLabel, Gauge>,
    rule_state_bytes: Family<StateNamespaceLabel, Gauge>,
    rule_state_ops_total: Family<StateOpLabel, Counter>,
    rule_state_expired_total: Family<StateNamespaceLabel, Counter>,
    rule_state_expiry_backlog: Family<StateNamespaceLabel, Gauge>,
    /// What connections' dictionaries (`proc_dict_*`) hold, and how often one was
    /// refused room.
    rule_state_dictionary_bytes: Gauge,
    rule_state_dictionary_refused_total: Counter,
    /// Enabled rules loaded on this node (ADR 0083).
    rules_loaded: Gauge,
    /// The loaded rules file's checksum (ADR 0083), `config_info`-style: after a
    /// rules roll every node must report the same value.
    rules_info: Family<ChecksumLabel, Gauge>,
    rules_info_prev: std::sync::Mutex<Option<String>>,
    /// How many SWIM gossip keys this node currently accepts (ADR 0054 T3):
    /// 1 = steady state, 2 = a rotation window is open. Alert when it stays > 1
    /// longer than a rotation should take.
    swim_keys_accepted: Gauge,
    swim_isolated: Gauge,
    /// The peer-bus protocol range this build speaks (ADR 0038/0054) — a mixed-
    /// version fleet is visible per node.
    peer_proto_min: Gauge,
    peer_proto_max: Gauge,
    /// Time to expiry of each loaded certificate, CA bundle and revocation list
    /// (ADR 0088 D2), by role: seconds left (negative once over) and the fraction of the
    /// validity window spent. Computed from `tls_validity` whenever the metrics are
    /// read, never stored as a number that would stand still between reloads.
    tls_expiry_seconds: Family<RoleLabel, Gauge>,
    tls_lifetime_spent_ratio: Family<RoleLabel, Gauge<f64, std::sync::atomic::AtomicU64>>,
    /// The fraction of a lifetime past which this node warns (`tls.expiry_warn_permille`).
    tls_expiry_warn_ratio: Gauge<f64, std::sync::atomic::AtomicU64>,
    tls_validity: TlsValidity,
    /// `tls.expiry_warn_permille` as set, for the OTLP callback; 0 until it is.
    tls_warn_permille: std::sync::Arc<std::sync::atomic::AtomicU32>,
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
            "Messages dropped, by reason (no-subscriber, queue-overflow, backlog-overflow, outbound-full, outbound-id-write-failed, pending-cap, pending-cap-replay, settle-replay, settle-replay-refused, append-backlog-full, brownout, too-large, retained-replay-client-offline, retained-replay-read-failed, reserved, peer-unreachable)",
        );
        let publish_resolved_undeliverable_total = register_family(
            &mut registry,
            "publish_resolved_undeliverable",
            "QoS >= 1 publishes acknowledged to their publisher although a node that \
             owned a subscriber never accepted them, because that node is gone \
             (ADR 0087 D2, D9), by reason: peer-gone (membership confirmed the node \
             gone: its tombstone pruned unanswered, or a new generation joined), \
             no-shared-member (a $share group whose last members were on it), \
             hold-expired (durable mode, until 0087-T5: the node stayed out of reach \
             for the eight-tick hold)",
        );

        // Issue #480: the fraction of publishes that cross a node boundary, which
        // `received`/`delivered` structurally cannot express.
        let publish_forwarded_total = register_family(
            &mut registry,
            "publish_forwarded",
            "Publishes handed to a peer link rather than delivered locally, by reason (shared-remote, subscriber-remote)",
        );

        let deliver_latency_seconds = register_latency_histogram(
            &mut registry,
            "deliver_latency_seconds",
            "Publish-to-deliver latency (the hub's ON-LOOP fan-out: plan, lane \
             submissions, peer forwards — the durable appends themselves run \
             off-loop since issue #242 and are timed by durable_append_latency_seconds)",
        );
        let hub_dispatch_seconds = register_wide_latency_histogram_family(
            &mut registry,
            "hub_dispatch_seconds",
            "Time the single-threaded hub loop spent inside one command dispatch, by \
             coarse command class (issue #242). Every client on the node queues behind \
             a long dispatch, so a sustained p99 above ~100ms means something is \
             blocking the loop again — see docs/OPERATIONS.md",
        );
        // MICRO buckets, not the 100µs-start ones every other latency histogram in
        // this file uses (issue #613 item 1.6). Those measure a round trip; this
        // measures a per-message CPU term of single-digit microseconds at N=7. With
        // a 100µs first bucket every observation would land in bucket one both
        // before and after the fan-out gets cheaper, and the series could not show
        // the one thing it exists to show.
        let hub_fanout_seconds = register_micro_latency_histogram(
            &mut registry,
            "hub_fanout_seconds",
            "Time one publish's PEER fan-out spent on the hub loop (issue #613 item \
             1.6), split out of hub_dispatch_seconds so the O(peers) term can be \
             read on its own. A fan-out that visited no links still observes here, \
             with zero peer visits",
        );
        let hub_fanout_peer_visits_total = register_counter(
            &mut registry,
            "hub_fanout_peer_visits",
            "Peer links visited by the publish fan-out, summed (issue #613 item \
             1.6). Divided by hub_fanout_seconds_count it is the average number of \
             links one publish walks — the quantity items 1.3 and 1.4 reduce",
        );
        let hub_queue_depth = register_gauge(
            &mut registry,
            "hub_queue_depth",
            "Commands queued for the single-threaded hub loop (issue #613 item \
             3.5). The hub is one task, so this IS the statement \"work is arriving \
             faster than it is retired\"",
        );
        let hub_lane_depth = register_gauge_family(
            &mut registry,
            "hub_lane_depth",
            "Commands the hub has sorted and not yet dispatched, by lane (ADR 0082 \
             T2): `control` (acks, completions, durable-plane and membership frames, \
             the /livez ping, admin) is always dispatched before `data` (publishes, \
             subscriptions, session lifecycle). A growing `data` lane with a near-zero \
             `control` lane is overload that consensus and health no longer wait \
             behind",
        );
        let ingress_paused_total = register_counter(
            &mut registry,
            "ingress_paused",
            "Times a client connection stopped reading its socket because it had no \
             ingress credit (ADR 0082 T3): the hub is behind and the bytes already \
             queued for it hit the node pool or the connection's cap. The publisher \
             waits in TCP, losslessly. QoS 0 shed instead under \
             MQTTD_INGRESS_OVERLOAD=shed-qos0 is publish_dropped{reason=\"hub-ingress\"}",
        );
        let ingress_paused_seconds = register_latency_histogram(
            &mut registry,
            "ingress_paused_seconds",
            "How long each ingress-credit pause lasted (ADR 0082 T3), from the publish \
             that found no credit to the credit freeing",
        );
        let ingress_credit_bytes = register_gauge_family(
            &mut registry,
            "ingress_credit_bytes",
            "The node's ingress credit pool (ADR 0082 T3), by state: `in_use` is the \
             bytes held by client publishes read but not yet dispatched by the hub, \
             `capacity` the pool. in_use near capacity means publishers are pausing",
        );
        let routing_unsettled = register_gauge_family(
            &mut registry,
            "routing_unsettled",
            "Whether this node's ROUTING VIEW is still settling, 1 or 0, by which \
             term is holding it (issue #613 item 2.5): takeover, scan, \
             scan-incomplete, interest, mesh. Every series is emitted on every \
             scrape, so an absent series means the broker is not reporting rather \
             than that the term is false",
        );
        let pending_publishes_awaiting_settle = register_gauge(
            &mut registry,
            "pending_publishes_awaiting_settle",
            "The subset of pending_publishes still held by the settle window \
             (issue #613 item 2.1). Always <= pending_publishes; a persistently \
             high ratio is a routing view that is not converging, not load",
        );
        let publish_held_for_unreachable = register_gauge(
            &mut registry,
            "publish_held_for_unreachable",
            "Publishes whose acknowledgement is held for a peer this node cannot \
             reach: a forward is owed to a node whose link is down, or that \
             membership holds dead (ADR 0087). A subset of pending_publishes. Above \
             zero for longer than a link takes to return, a peer is out of reach \
             and the publishers of what its subscribers are owed are waiting",
        );
        let shared_selected_total = register_family(
            &mut registry,
            "shared_selected",
            "Shared-subscription selections by locality (local, remote) — issue \
             #613 item 3.5. Exactly one increment per DELIVERED message, so \
             local+remote can never exceed messages delivered",
        );

        let append_lane_jobs = register_gauge(
            &mut registry,
            "append_lane_jobs",
            "Durable-append lane jobs admitted and not yet completed, summed over \
             sessions (issue #242); sustained growth means a placement group's \
             followers are not keeping up — the warning before \
             publish_dropped{reason=\"append-backlog-full\"} fires",
        );
        let pending_publishes = register_gauge(
            &mut registry,
            "pending_publishes",
            "Publishes whose acknowledgement is still gated on durability (ADR 0042 T9). \
             Bounded by an entry cap and a byte cap; at either the OLDEST is evicted with \
             its ack withheld — the warning before publish_dropped{reason=\"pending-cap\"} \
             fires (issue #633)",
        );
        let pending_publish_bytes = register_gauge(
            &mut registry,
            "pending_publish_bytes",
            "Bytes charged to the pending-publish table (entry + topic + payload, kept for \
             retransmission); the byte half of the same bound (issue #633)",
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
        let backlog_bytes = register_gauge(
            &mut registry,
            "backlog_bytes",
            "Accounted bytes held in flow-control backlogs across sessions (sampled on the \
             session sweep, not live)",
        );
        let backlog_bytes_max = register_gauge(
            &mut registry,
            "backlog_bytes_max",
            "Accounted bytes held by the LARGEST single session's flow-control backlog \
             (sampled on the session sweep, not live) — the number to size a PER-SUBSCRIBER \
             cap against, since backlog_bytes sums every session",
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
        let peer_link_stat =
            Family::<PeerLinkStatLabel, Gauge<f64, std::sync::atomic::AtomicU64>>::default();
        registry.register(
            "peer_link_stat",
            "One inter-node link's work, cumulative since it connected (read deltas over a \
             window): busy_seconds (on-CPU time of the single task that carries the link: \
             TLS, syscalls, codec, dispatch, never its waiting; busy_seconds per second near \
             1 means the link is saturated), write_wait_seconds (wall time inside each \
             batch's socket write and flush, pending included, so a write blocked on TCP \
             backpressure shows here; not the encode or batching yield; a write still \
             pending is counted only once it completes), polls, \
             frames_out, writes, frames_in, reads \
             (frames per write/read is the batching per I/O call)",
            peer_link_stat.clone(),
        );
        let rule_state_replication_lag_seconds =
            Family::<StateNamespaceLabel, Gauge<f64, std::sync::atomic::AtomicU64>>::default();
        registry.register(
            "rule_state_replication_lag_seconds",
            "How long the peer link furthest behind has had rule state of this `eventual` \
             namespace to send (ADR 0085): the time since that link last had sent every \
             changed key. 0 when every link has. It grows while a peer is slow or its link \
             is busy with other traffic; nothing is queued meanwhile, the link sends each \
             changed key once when it gets to it",
            rule_state_replication_lag_seconds.clone(),
        );
        let rule_state_replication_pending = register_gauge_family(
            &mut registry,
            "rule_state_replication_pending",
            "Keys of this `eventual` rule-state namespace changed on this node and not yet \
             sent to the peer link furthest behind (ADR 0085): at most the keys the namespace \
             holds, however often they were written",
        );
        let rule_state_replication_refused_total = register_family(
            &mut registry,
            "rule_state_replication_refused",
            "Rule state a peer sent for this `eventual` namespace that this node did not \
             take (ADR 0085), by reason: declaration (the peer's rules file declares the \
             namespace otherwise: a whole frame), clock_skew (a state stamped more than 60 \
             seconds ahead of this node's clock), namespace_full and store_full (a key new \
             to this node, or one that would grow, and no room under max_keys, max_bytes or \
             rules.state_max_bytes), brownout (a key new to this node while it is over its \
             memory watermark), malformed (not a key, a state or a summary). The peers \
             offer what was refused again when they next compare. Frames for a namespace \
             this node does not declare `eventual` at all are counted under the namespace \
             `(undeclared)` and the reason `undeclared`",
        );
        // Issue #504: the peer links are `mpsc::unbounded_channel()`, so a publish
        // forwarded to a peer is counted `received` here and then reported by
        // NOTHING until the remote node delivers it — not delivered, not dropped,
        // and not in `backlog_bytes` (that is the per-subscriber egress queue).
        // The lane E ladder showed a fifth of the stream sitting in exactly this
        // gap while every drop counter read zero. Unbounded also means this is the
        // memory that grows when a peer stops draining, so the gauge is both the
        // accounting fix and the leading indicator.
        let peer_forwards_in_flight = register_gauge(
            &mut registry,
            "peer_forwards_in_flight",
            "Frames queued on inter-node peer links, not yet written to the wire",
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
        let misplaced_sessions = register_gauge(
            &mut registry,
            "misplaced_sessions",
            "Live persistent sessions hosted on a node that does NOT own their \
             placement group (issue #284). Non-zero for longer than a convergence \
             window means sessions that cannot be rehomed (the owner's peer address \
             is unknown) — those sessions are undeliverable and their publishers \
             are withheld",
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
        let durable_stage_seconds = Family::<StageLabel, Histogram>::new_with_constructor(|| {
            Histogram::new(fine_latency_buckets())
        });
        registry.register(
            "durable_stage_seconds",
            "Where a durable append's time goes, by stage: lane_queue (queued in the \
             session's append lane before submit), local_durable (submit until this node's \
             own copy is durable), quorum (own copy durable until the quorum is met — the \
             follower's fsync and the round trip), order (quorum met until the in-order \
             commit releases it), commit (one group commit on a shard writer, fsync \
             included), fsync (the data sync alone), replicate_rtt (leader, per follower: \
             Replicate queued to the peer link until its ack is back), replica_apply \
             (follower: Replicate received until its ack is queued), replicate_queue \
             (leader: Replicate queued on the peer link until the link has handed it to the \
             kernel, write and flush returned), ack_queue (follower: the same for its \
             ReplicateAck); both sampled, about one frame in 16. Fine buckets from 20us so a \
             millisecond-scale fsync is resolved, not rounded to the next power of two",
            durable_stage_seconds.clone(),
        );
        let runtime_wake_seconds = Family::<ProbeLabel, Histogram>::new_with_constructor(|| {
            Histogram::new(fine_latency_buckets())
        });
        registry.register(
            "runtime_wake_seconds",
            "How late a ready task runs on the async runtime, sampled 100 times a second per \
             probe: yield (a yield_now round trip: the wait behind other ready tasks) and \
             channel (a message stamped by one task until another wakes to receive it: the \
             hand-off every durable append makes several times). Fine buckets from 20us",
            runtime_wake_seconds.clone(),
        );
        let publish_ack_seconds = Histogram::new(fine_latency_buckets());
        registry.register(
            "publish_ack_seconds",
            "Server-side latency of a gated (QoS 1/2) publish: registered by the hub until \
             its PUBACK/PUBREC is released — durability, settle and peer acks included, the \
             publisher's own network excluded",
            publish_ack_seconds.clone(),
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
        let durable_elided_total = register_counter(
            &mut registry,
            "durable_elided",
            "Durable QoS 1 deliveries the subscriber acknowledged inside the write-elision \
             window, so never written (ADR 0089)",
        );
        let durable_elision_fallback_total = register_family(
            &mut registry,
            "durable_elision_fallback",
            "Held durable QoS 1 deliveries written after all, by reason (timeout, \
             disconnect, brownout, drain; ADR 0089)",
        );
        let durable_elision_hold_seconds = Histogram::new(fine_latency_buckets());
        registry.register(
            "durable_elision_hold_seconds",
            "How long an elided delivery was held before the subscriber's PUBACK discharged \
             it: the subscriber's acknowledgement latency (ADR 0089)",
            durable_elision_hold_seconds.clone(),
        );
        let durable_elision_backoff_total = register_counter(
            &mut registry,
            "durable_elision_backoff",
            "Sessions that kept missing the write-elision window and were written directly \
             for a while instead of held (ADR 0089 Amendment 1 A3)",
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
             expired, revoked, domain, cert-miss, cluster-mismatch)",
        );
        let gossip_cert_requests_total = register_counter(
            &mut registry,
            "gossip_cert_requests",
            "Certificate requests sent: a Join greeting to the source of a SWIM datagram \
             dropped as cert-miss, so both sides learn the other's certificate",
        );
        let swim_last_probes_total = register_family(
            &mut registry,
            "swim_last_probes",
            "Last direct probes of a suspect before it is declared dead, by outcome (sent; \
             spared: the suspect answered and was kept; silent: it was declared dead)",
        );
        let swim_stalls_total = register_counter(
            &mut registry,
            "swim_stalls",
            "Times this node's SWIM driver was not scheduled for more than two of its ticks",
        );
        let swim_stalled_milliseconds_total = register_counter(
            &mut registry,
            "swim_stalled_milliseconds",
            "Milliseconds this node's SWIM driver was not scheduled, summed over its stalls",
        );
        let swim_awareness = register_gauge(
            &mut registry,
            "swim_awareness",
            "SWIM local-health awareness (0 healthy; ack timeouts and suspicion windows \
             are multiplied by 1 + this)",
        );
        let security_reloads_total = register_family(
            &mut registry,
            "security_reloads",
            "Hot reloads of the security policy, by outcome (ok, rejected) and trigger (signal, watch, \
             admin, admin-rules)",
        );
        let revocation_evictions_total = register_family(
            &mut registry,
            "revocation_evictions",
            "Live state revoked by a policy-reload sweep (ADR 0040), by kind \
             (cert-revoked, user-removed, connect-denied, grant-revoked, peer-revoked), or \
             ended at a certificate's expiry (ADR 0088: peer-cert-expired, own-cert-expired, \
             cert-expired)",
        );
        let session_rehomes_total = register_family(
            &mut registry,
            "session_rehomes",
            "Rehome-on-settle decisions for a live session hosted on a non-owning \
             node (issue #284), by reason: stale-owner (closed, so the client \
             relocates to the owner), unrelocatable (kept and served locally because \
             the owner's address is unknown — ADR 0005 degrade-don't-refuse), \
             cooldown (a repeat close suppressed), deferred (over the per-tick close \
             cap, retried on a later tick — counted once per session per deferral \
             episode, not once per tick). Each stale-owner close also publishes the \
             client's Last Will",
        );
        let session_expiry_unpersisted_total = register_family(
            &mut registry,
            "session_expiry_unpersisted",
            "Detaches whose ADR 0009 §3 absolute session-expiry deadline could NOT be \
             persisted, by reason: not-owner (this node does not hold the session \
             group's lease — the structural case after a rehome, issue #284) or error \
             (the write was attempted and failed). The new owner inherits a session \
             record with no deadline, so a client that never returns leaves a \
             persistent session behind",
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

        let audit_export_dropped_total = register_counter(
            &mut registry,
            "audit_export_dropped",
            "Audit records the SIEM exporter shed because its bounded queue was full \
             (ADR 0066 T3): the chain itself is intact at the source, and the export's \
             seq gap makes the shed detectable downstream — non-zero means the export \
             endpoint is slower than the audit rate",
        );

        let publish_tier_total = register_family(
            &mut registry,
            "publish_tier",
            "Gated publishes by publisher-selected durability tier (ADR 0072): \
             quorum (the default full ack-after-quorum), local (ack after the \
             owner's fsync), relaxed (ack at accept+submit). Non-default tiers \
             require the operator's MQTTD_ALLOW_RELAXED_PUBLISH opt-in",
        );

        let durable_writer_batches_total = register_counter(
            &mut registry,
            "durable_writer_batches",
            "Fsync'd batches the node-wide durable-write serializer committed \
             (ADR 0071): each batch is one Durability::Immediate transaction \
             covering owner appends and follower replica applies that arrived \
             while the previous fsync ran",
        );
        let durable_writer_ops_total = register_counter(
            &mut registry,
            "durable_writer_ops",
            "Durable ops applied across all serializer batches (ADR 0071); \
             divided by durable_writer_batches this is the mean group-commit \
             batch size — 1.0 at rest, rising under load as coalescing pays",
        );
        let durable_writer_max_batch = register_gauge(
            &mut registry,
            "durable_writer_max_batch",
            "Largest single group-commit batch since boot (ADR 0071)",
        );
        let durable_writer_commit_micros_total = register_counter(
            &mut registry,
            "durable_writer_commit_micros",
            "Cumulative microseconds the durable-write serializer spent \
             committing batches (ADR 0076): divided by durable_writer_batches \
             over the same window this is the LIVE mean commit (barrier) \
             latency — the passive volume-health signal, measured from real \
             traffic",
        );
        let store_barrier_floor = register_gauge(
            &mut registry,
            "store_barrier_floor",
            "Boot-probed single-writer barrier rate (fsync round trips per \
             second) of the data-dir volume (ADR 0076): the denominator every \
             durable throughput figure should be read against; 0 until probed",
        );
        let store_barrier_floor_4stream = register_gauge(
            &mut registry,
            "store_barrier_floor_4stream",
            "Boot-probed AGGREGATE barrier rate across 4 concurrent writers on \
             separate files (ADR 0076): how much parallel-stream headroom the \
             volume has beyond one fsync stream — the sharding signal; 0 until \
             probed",
        );

        let store_shards = register_gauge(
            &mut registry,
            "store_shards",
            "How many files the replica store spans (ADR 0076 T2): committed at \
             first boot from this volume's measured barrier knee and fixed for \
             the life of the data dir; 1 for a single-file store",
        );
        let store_reshard_advice = register_gauge(
            &mut registry,
            "store_reshard_advice",
            "The shard count this volume's measurements now suggest (ADR 0076 \
             T2), when it differs from the committed one — an ADVISORY for the \
             operator, never an automatic migration; 0 when the committed \
             layout still fits",
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
        // Backup + restore (ADR 0062). `backup_last_success_timestamp_seconds` is the
        // series the RPO alert reads: an unconfigured backup exports a literal 0, so every
        // rule over it needs the `> 0` guard clause the watermark rules taught.
        let backup_runs_total = register_family(
            &mut registry,
            "backup_runs",
            "Online backup runs, by outcome (ok = an export was fsynced and renamed into \
             place; error = the run wrote nothing, e.g. an incomplete session scan)",
        );
        let backup_last_success_timestamp_seconds = register_gauge(
            &mut registry,
            "backup_last_success_timestamp_seconds",
            "Unix time the newest SUCCESSFUL export started (ADR 0062); 0 = no backup has \
             ever succeeded in this process. Its age bounds the RPO",
        );
        let backup_duration_ms = register_gauge(
            &mut registry,
            "backup_duration_ms",
            "Wall-clock milliseconds the last export took. An UPPER BOUND on the \
             consistency window, not the window itself — the window is the span the records \
             were actually read over, exported separately (ADR 0062)",
        );
        let restore_state = register_gauge(
            &mut registry,
            "restore_state",
            "Restore-from-backup state (ADR 0062): 0 none, 1 in progress (the node is \
             NotReady and has bound no client listener), 2 completed, 3 failed",
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
        let rule_evaluations_total = register_family(
            &mut registry,
            "rule_evaluations",
            "Rule-engine evaluations (ADR 0083), by rule id and result: passed (the SQL \
             produced output and the actions ran), no_result (FROM matched, WHERE or an \
             empty FOREACH produced nothing), failed (the SQL raised an error — the \
             triggering message is unaffected)",
        );
        let rule_actions_total = register_family(
            &mut registry,
            "rule_actions",
            "Rule actions run (ADR 0083), by rule id and result: ok (a console action \
             logged; a republish the broker accepted and routed), failed (the action could \
             not render — a bad topic, qos or retain, a payload that is not text — or went \
             over a message's limits, or the broker refused or dropped its message — a \
             brownout, a refused original, an ingress shed, a failed durable write; the \
             triggering message is unaffected)",
        );
        let rule_recursive_republish_total = register_family(
            &mut registry,
            "rule_recursive_republish",
            "Republish actions not run because their message would loop (ADR 0083), by \
             rule id and guard: same_rule (the message re-entering the rules is one this \
             rule republished — EMQX's recursive_republish_detected; counted as an ok \
             action, as EMQX counts it), depth (it is 32 republishes from the message \
             that started the chain; counted as a failed action)",
        );
        let rule_eval_seconds_total =
            Family::<RuleLabel, Counter<f64, std::sync::atomic::AtomicU64>>::default();
        registry.register(
            "rule_eval_seconds",
            "Time spent evaluating each rule (ADR 0084), in seconds, by rule id: its SQL \
             plus rendering its actions' effects, summed over live evaluations (dry runs \
             excluded). Divided by the growth of mqttd_rule_evaluations_total it is the \
             rule's average cost per message; routing a republished message is not in it",
            rule_eval_seconds_total.clone(),
        );
        let rule_function_calls_total = register_family(
            &mut registry,
            "rule_function_calls",
            "Calls of rule functions that run in the WebAssembly sandbox (ADR 0086), by \
             function: jq, and the functions of the modules the rules file declares. Live \
             evaluations only (dry runs excluded)",
        );
        let rule_function_errors_total = register_family(
            &mut registry,
            "rule_function_errors",
            "Failed calls of sandboxed rule functions (ADR 0086), by function and kind. For \
             jq: compile, parse, process, timeout, system (EMQX's error tags); memory, \
             output, stack, compile_timeout (a limit of the sandbox); trap (the module \
             broke); argument (a program that is not a string, a bad timeout); decode (an \
             output the rule engine's JSON reader refuses). For a declared module's \
             function: module (its own refusal), timeout, memory, output, stack, trap, \
             argument, decode. Each fails its rule, never the message",
        );
        let rule_function_seconds_total =
            Family::<FunctionLabel, Counter<f64, std::sync::atomic::AtomicU64>>::default();
        registry.register(
            "rule_function_seconds",
            "Time spent in sandboxed rule functions (ADR 0086), in seconds, by function; \
             part of the calling rule's mqttd_rule_eval_seconds_total",
            rule_function_seconds_total.clone(),
        );
        let rule_module_instances = register_gauge_family(
            &mut registry,
            "rule_module_instances",
            "Sandbox instances a rule-function module holds (ADR 0086), waiting or in a \
             call, by module; as of the module's last call",
        );
        let rule_module_memory_bytes = register_gauge_family(
            &mut registry,
            "rule_module_memory_bytes",
            "Linear memory of a rule-function module's instances (ADR 0086), in bytes, by \
             module; as of the last call",
        );
        let rule_module_instances_replaced_total = register_family(
            &mut registry,
            "rule_module_instances_replaced",
            "Sandbox instances given up after a call (ADR 0086), by module: stopped at a \
             limit (time, memory, stack) or grown too large to keep",
        );
        let rule_state_keys = register_gauge_family(
            &mut registry,
            "rule_state_keys",
            "Keys a namespace of the rule state store holds (ADR 0085), by namespace: a \
             name the rules file declares, kv_store (EMQX's table), or rule:<id> for a \
             rule's own. Keys past their expiry that are not removed yet are counted",
        );
        let rule_state_bytes = register_gauge_family(
            &mut registry,
            "rule_state_bytes",
            "Bytes accounted to a namespace's keys and values (ADR 0085), by namespace: \
             what max_bytes and rules.state_max_bytes bound",
        );
        let rule_state_ops_total = register_family(
            &mut registry,
            "rule_state_ops",
            "State calls that reached the store (ADR 0085), by namespace, operation (get, \
             set, set_once, incr, max, min, add, del, ttl, held, changed, hll_add, \
             hll_count; kv_store_put, kv_store_get and kv_store_del count as set, get and \
             del) and result: ok, or why the store refused the call (namespace_full, \
             store_full, brownout, wrong_type, refused). A refused call fails its rule. \
             A call the rule engine refused for its arguments is not counted here",
        );
        let rule_state_expired_total = register_family(
            &mut registry,
            "rule_state_expired",
            "Keys removed because their expiry had passed (ADR 0085), by namespace",
        );
        let rule_state_expiry_backlog = register_gauge_family(
            &mut registry,
            "rule_state_expiry_backlog",
            "Keys whose expiry has passed and that are still held (ADR 0085), by \
             namespace: they read as absent and are removed by the next call that finds \
             them or needs room",
        );
        let rule_state_dictionary_bytes = register_gauge(
            &mut registry,
            "rule_state_dictionary_bytes",
            "Bytes the connections' dictionaries hold together (proc_dict_*, ADR 0085): \
             in no namespace, counted against rules.state_max_bytes",
        );
        let rule_state_dictionary_refused_total = register_counter(
            &mut registry,
            "rule_state_dictionary_refused",
            "proc_dict_put calls refused room (ADR 0085): the store at its ceiling, or a \
             memory brownout. Each fails its rule",
        );
        let rules_loaded = register_gauge(
            &mut registry,
            "rules_loaded",
            "Enabled rules loaded on this node (ADR 0083)",
        );
        let rules_info = register_gauge_family(
            &mut registry,
            "rules_info",
            "Checksum of the loaded rules file (ADR 0083), config_info-style; after a \
             rules roll every node must report the same value",
        );
        let swim_keys_accepted = register_gauge(
            &mut registry,
            "swim_keys_accepted",
            "SWIM gossip keys currently accepted (ADR 0054 T3): 1 steady, 2 = a \
             rotation window is open",
        );
        let swim_isolated = register_gauge(
            &mut registry,
            "swim_isolated",
            "1 while this node's own SWIM probes go unanswered past the isolation \
             threshold (issue #368): its membership view is unconfirmed and peers \
             are likely evicting it — a one-way network failure looks exactly like \
             this and like nothing else",
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

        let tls_expiry_seconds = register_gauge_family(
            &mut registry,
            "tls_expiry_seconds",
            "Seconds until the loaded TLS material in this role expires (ADR 0088): a \
             certificate file's or CA bundle's earliest notAfter, a revocation list's \
             nextUpdate. Negative once it is over",
        );
        let tls_lifetime_spent_ratio =
            Family::<RoleLabel, Gauge<f64, std::sync::atomic::AtomicU64>>::default();
        registry.register(
            "tls_lifetime_spent_ratio",
            "Fraction of its validity window the loaded TLS material in this role has \
             spent (ADR 0088): 0 when new, 1 at expiry, above 1 after it",
            tls_lifetime_spent_ratio.clone(),
        );
        let tls_expiry_warn_ratio = Gauge::<f64, std::sync::atomic::AtomicU64>::default();
        registry.register(
            "tls_expiry_warn_ratio",
            "Fraction of a lifetime past which this node warns about TLS material \
             (tls.expiry_warn_permille, ADR 0088)",
            tls_expiry_warn_ratio.clone(),
        );
        let tls_validity = TlsValidity::default();
        let tls_warn_permille = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

        let build_info = Family::<VersionLabel, Gauge>::default();
        registry.register("build_info", "Build information", build_info.clone());
        let crypto_module_info = Family::<CryptoModuleLabel, Gauge>::default();
        registry.register(
            "crypto_module_info",
            "The crypto module this binary runs (ADR 0068): aws-lc-rs, or the \
             FIPS-validated module in a fips build — so an auditor verifies the \
             RUNNING binary, not the artifact's name",
            crypto_module_info.clone(),
        );
        build_info
            .get_or_create(&VersionLabel {
                version: version.to_string(),
            })
            .set(1);

        Self {
            otel: OtelInstruments::new(meter, &tls_validity, &tls_warn_permille),
            provider,
            registry,
            connections_active,
            connections_total,
            accepts_total,
            connection_errors_total,
            publish_received_total,
            publish_delivered_total,
            publish_dropped_total,
            publish_resolved_undeliverable_total,
            publish_forwarded_total,
            deliver_latency_seconds,
            hub_dispatch_seconds,
            hub_fanout_seconds,
            hub_fanout_peer_visits_total,
            hub_queue_depth,
            hub_lane_depth,
            ingress_paused_total,
            ingress_paused_seconds,
            ingress_credit_bytes,
            routing_unsettled,
            pending_publishes_awaiting_settle,
            publish_held_for_unreachable,
            shared_selected_total,
            append_lane_jobs,
            pending_publishes,
            pending_publish_bytes,
            sessions,
            subscriptions,
            retained_messages,
            inflight_messages,
            backlog_bytes,
            backlog_bytes_max,
            cluster_members,
            peer_links,
            peer_link_stat,
            rule_state_replication_lag_seconds,
            rule_state_replication_pending,
            rule_state_replication_refused_total,
            peer_forwards_in_flight,
            replication_desired,
            replication_min_actual,
            replication_write_floor,
            retained_tombstones,
            misplaced_sessions,
            members_by_state,
            lease_leader,
            lease_epoch,
            durable_append_latency_seconds,
            durable_stage_seconds,
            durable_stage_cache: Default::default(),
            publish_ack_seconds,
            runtime_wake_seconds,
            http_auth_latency_seconds,
            http_auth_outcomes_total,
            durable_append_failures_total,
            durable_elided_total,
            durable_elision_fallback_total,
            durable_elision_hold_seconds,
            durable_elision_backoff_total,
            durable_recovery_failures_total,
            lease_quorum_ack_ms,
            gossip_rejected_total,
            gossip_cert_requests_total,
            swim_last_probes_total,
            swim_stalls_total,
            swim_stalled_milliseconds_total,
            swim_awareness,
            security_reloads_total,
            revocation_evictions_total,
            session_rehomes_total,
            session_expiry_unpersisted_total,
            admission_rejected_total,
            quota_rejections_total,
            store_bytes,
            quic_path_migrations_total,
            retained_divergence_total,
            retained_apply_failed_total,
            retained_queue_dropped_total,
            audit_export_dropped_total,
            publish_tier_total,
            durable_writer_batches_total,
            durable_writer_commit_micros_total,
            store_barrier_floor,
            store_barrier_floor_4stream,
            store_shards,
            store_reshard_advice,
            durable_writer_ops_total,
            durable_writer_max_batch,
            crypto_module_info,
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
            backup_runs_total,
            backup_last_success_timestamp_seconds,
            backup_duration_ms,
            restore_state,
            foundings_total,
            config_info,
            config_info_prev: std::sync::Mutex::new(None),
            rule_evaluations_total,
            rule_actions_total,
            rule_recursive_republish_total,
            rule_eval_seconds_total,
            rule_function_calls_total,
            rule_function_errors_total,
            rule_function_seconds_total,
            rule_module_instances,
            rule_module_memory_bytes,
            rule_module_instances_replaced_total,
            rule_state_keys,
            rule_state_bytes,
            rule_state_ops_total,
            rule_state_expired_total,
            rule_state_expiry_backlog,
            rule_state_dictionary_bytes,
            rule_state_dictionary_refused_total,
            rules_loaded,
            rules_info,
            rules_info_prev: std::sync::Mutex::new(None),
            swim_keys_accepted,
            swim_isolated,
            peer_proto_min,
            peer_proto_max,
            tls_expiry_seconds,
            tls_lifetime_spent_ratio,
            tls_expiry_warn_ratio,
            tls_validity,
            tls_warn_permille,
        }
    }

    /// Render the current metrics as Prometheus text exposition (for `GET /metrics`).
    ///
    /// # Panics
    /// Panics only if formatting into a `String` fails, which the standard library does not do.
    #[must_use]
    pub fn render(&self) -> String {
        self.render_at(std::time::SystemTime::now())
    }

    /// [`render`](Self::render) as of `now`: the gauges that are a distance in time from
    /// `now` (the TLS expiry gauges, ADR 0088) are computed against it.
    ///
    /// # Panics
    /// Panics only if formatting into a `String` fails, which the standard library does not do.
    #[must_use]
    pub fn render_at(&self, now: std::time::SystemTime) -> String {
        self.refresh_tls_expiry(unix_secs(now));
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
    /// | `backlog-overflow` | the flow-control backlog hit one of its configured bounds — `MQTTD_MAX_BACKLOG_MESSAGES` or `MQTTD_MAX_BACKLOG_BYTES` (ADR 0012, 0041-T10, issue #241). Already-acked entries are truncated and the publisher is NOT told; the WARN line names which bound fired (`bound="messages"`, `"bytes"`, or `"messages+bytes"` when one arrival tripped both) and how many entries went. A byte bound below `MQTTD_MAX_PACKET_SIZE` makes this routine |
    /// | `outbound-full` | a `QoS` 0 shed for a subscriber that stopped reading (#123) — at the fixed 10 000-packet cap or at `MQTTD_MAX_OUTBOUND_BYTES`; the WARN line names which |
    /// | `pending-cap` | the pending-publish table was at `PENDING_PUBLISH_CAP` or `PENDING_PUBLISH_MAX_BYTES`, so its OLDEST entry was evicted while that publisher was still waiting: its ack is withheld and it retries (ADR 0042 T9, bounds from 3571682) |
    /// | `pending-cap-replay` | the same eviction, but the oldest entry had ALREADY been acknowledged and survived only for the settle window's replay (issue #613 item 2.1). No publisher lost an answer; a session materialised during that window will not receive the message |
    /// | `settle-replay` | a settle-window replay was abandoned for a publish whose ack had already been released against real fan-out evidence (issue #613 item 2.1). Nothing is withheld — the publisher keeps its truthful `Accepted` — but a session materialised during the window will not receive that message |
    /// | `settle-replay-refused` | as `settle-replay`, but the replay ended in a REFUSAL (a brownout entered during the window). Nothing is claimed to the publisher, which was already answered |
    /// | `append-backlog-full` | a session's durable-append lane hit `LANE_QUEUE_CAP` (issue #242): the NEWEST job was rejected at submit (reject-newest keeps the lane FIFO). An answerable publish is WITHHELD (fail closed, the publisher retries); an unanswerable one is a genuine drop. Watch `append_lane_jobs` for the pre-drop warning |
    /// | `brownout` | a durable copy lost above the watermark that NOBODY was told about: a `QoS` 0 offline enqueue (nothing was owed), or an UNGATED publish with no publisher to answer — a Will, a retained-window back-fill — whose live delivery still happens. A `QoS` >= 1 refusal a publisher IS told about is `quota_rejections_total{reason="brownout-publish"}` instead, because it was answered rather than lost (issue #238) |
    /// | `too-large` | the encoded packet exceeded that subscriber's Maximum Packet Size |
    /// A publish handed to a PEER LINK rather than delivered locally (issue #480).
    ///
    /// `received` and `delivered` cannot answer "what fraction crossed a node
    /// boundary": summed cluster-wide they are equal either way, because a message
    /// received at A and delivered by B increments A's received and B's delivered
    /// exactly as a local message would. Per broker they are near-equal under any
    /// even client spread, so the ratio says nothing either.
    ///
    /// That gap cost real time. ADR 0077's forwarding ceiling had to be established
    /// by INFERENCE — CPU-per-message deltas across cluster sizes, a harness
    /// experiment pinning every site to one broker, and finally a config flag —
    /// because three fully-collected runs on disk could not say how much traffic
    /// crossed the bus. `forwarded / received` per broker answers it from any run
    /// directory, and gives a locality change an immediate before/after instead of
    /// a topology experiment.
    pub fn publish_forwarded(&self, reason: &str) {
        self.publish_forwarded_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .publish_forwarded
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
    }

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

    /// Count `n` dropped publishes at once (a batch counted elsewhere and moved in by a
    /// sweep). `n == 0` touches nothing, so an idle reason never appears as a series.
    pub fn publish_dropped_by(&self, reason: &str, n: u64) {
        if n == 0 {
            return;
        }
        self.publish_dropped_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc_by(n);
        self.otel
            .publish_dropped
            .add(n, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// Count `n` publishes acknowledged although a node that owned a subscriber never
    /// accepted them, because that node is gone (ADR 0087 D2, D9). `n == 0` touches
    /// nothing, so an idle reason never appears as a series.
    pub fn publish_resolved_undeliverable_by(&self, reason: &str, n: u64) {
        if n == 0 {
            return;
        }
        self.publish_resolved_undeliverable_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc_by(n);
        self.otel
            .publish_resolved_undeliverable
            .add(n, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// Observe a publish-to-deliver latency in seconds.
    pub fn observe_deliver_latency(&self, seconds: f64) {
        self.deliver_latency_seconds.observe(seconds);
        self.otel.deliver_latency.record(seconds, &[]);
    }

    /// Observe one hub command dispatch's time on the single-threaded loop
    /// (issue #242). `command` is a **bounded** class — `attach`, `publish`, `ack`,
    /// `subscribe`, `control`, `cluster`, `sweep` — never a per-variant name.
    ///
    /// This is the regression tripwire for head-of-line blocking: the hub serves every
    /// client on the node from one loop, so a dispatch that blocks (an inline store or
    /// quorum await, the defect issue #242 removed) shows up here as tail mass. Alert:
    /// p99 over ~100ms sustained for 5m (docs/OPERATIONS.md).
    pub fn observe_hub_dispatch(&self, command: &str, seconds: f64) {
        self.hub_dispatch_seconds
            .get_or_create(&CommandLabel {
                command: command.to_string(),
            })
            .observe(seconds);
        self.otel
            .hub_dispatch
            .record(seconds, &[KeyValue::new("command", command.to_string())]);
    }

    /// Observe one publish's PEER fan-out: the time it spent on the hub loop and
    /// how many peer links it actually visited (issue #613 item 1.6).
    ///
    /// Split out of [`observe_hub_dispatch`](Self::observe_hub_dispatch) rather
    /// than folded into it, because the O(peers) term is what items 1.3 and 1.4
    /// remove and a dispatch histogram cannot separate it from the rest of the
    /// publish. `peer_visits` is the number of peer-link iterations actually
    /// executed — **0 when the fan-out early-returns**, which is a real
    /// observation and must still be recorded.
    ///
    /// The caller takes its `Instant` pair INSIDE its own `Some(metrics)` guard,
    /// so a metrics-less hub (benches, most unit tests) pays nothing. And it calls
    /// this ABOVE any early return: below it, `hub_fanout_seconds_count` stops
    /// advancing and the reading silently changes from "a fan-out walked zero
    /// links" to "no fan-outs happened" — identical in a dashboard, opposite in
    /// meaning.
    pub fn observe_hub_fanout(&self, seconds: f64, peer_visits: usize) {
        self.hub_fanout_seconds.observe(seconds);
        let visits = u64::try_from(peer_visits).unwrap_or(u64::MAX);
        self.hub_fanout_peer_visits_total.inc_by(visits);
        self.otel.hub_fanout.record(seconds, &[]);
        self.otel.hub_fanout_peer_visits.add(visits, &[]);
    }

    /// Set the depth of the hub's inbound command queue (issue #613 item 3.5).
    ///
    /// Fed from one `rx.len()` read per sweep tick: reading it twice would export
    /// a number the broker never acted on.
    pub fn set_hub_queue_depth(&self, n: usize) {
        self.hub_queue_depth.set(clamp_gauge(n));
        self.otel.hub_queue_depth.record(clamp_gauge(n), &[]);
    }

    /// Set one lane of `hub_lane_depth` (ADR 0082 T2). `lane` is `control` or `data`;
    /// the caller sets both on every refresh.
    pub fn set_hub_lane_depth(&self, lane: &str, n: usize) {
        self.hub_lane_depth
            .get_or_create(&LaneLabel {
                lane: lane.to_string(),
            })
            .set(clamp_gauge(n));
        self.otel
            .hub_lane_depth
            .record(clamp_gauge(n), &[KeyValue::new("lane", lane.to_string())]);
    }

    /// A client connection paused for want of ingress credit (ADR 0082 T3).
    pub fn ingress_paused(&self) {
        self.ingress_paused_total.inc();
        self.otel.ingress_paused.add(1, &[]);
    }

    /// An ingress-credit pause ended after `seconds` (ADR 0082 T3).
    pub fn observe_ingress_paused(&self, seconds: f64) {
        self.ingress_paused_seconds.observe(seconds);
        self.otel.ingress_paused_seconds.record(seconds, &[]);
    }

    /// Set the ingress credit pool gauges (ADR 0082 T3): bytes in use and capacity.
    pub fn set_ingress_credit(&self, in_use: usize, capacity: usize) {
        for (state, n) in [("in_use", in_use), ("capacity", capacity)] {
            self.ingress_credit_bytes
                .get_or_create(&StateLabel {
                    state: state.to_string(),
                })
                .set(clamp_gauge(n));
            self.otel
                .ingress_credit_bytes
                .record(clamp_gauge(n), &[KeyValue::new("state", state.to_string())]);
        }
    }

    /// Set one term of `routing_unsettled()` (issue #613 item 2.5). `reason` is a
    /// bounded set of exactly five values, one per term of that predicate:
    ///
    /// | reason | term |
    /// |---|---|
    /// | `takeover` | `takeover_reconcile_ticks > 0` |
    /// | `scan` | `inherited_scan_inflight` |
    /// | `scan-incomplete` | `!last_scan_complete` |
    /// | `interest` | `!interest_authoritative` |
    /// | `mesh` | `!mesh_settled()` |
    ///
    /// The caller sets ALL FIVE on every refresh, including the false ones. A
    /// series that stops being written keeps its last value in the registry, and
    /// an operator reading a stale 1 — or an absent series as a 0 — learns the
    /// opposite of the truth.
    pub fn set_routing_unsettled(&self, reason: &str, on: bool) {
        self.routing_unsettled
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .set(i64::from(on));
        self.otel.routing_unsettled.record(
            i64::from(on),
            &[KeyValue::new("reason", reason.to_string())],
        );
    }

    /// Set how many pending publishes the settle window is still HOLDING the ack
    /// of (issue #613 items 2.1/2.5) — the subset of `pending_publishes` that is
    /// waiting on the routing view rather than on durability. Read beside
    /// `pending_publishes`: a persistently high ratio is a view that is not
    /// converging, not load.
    pub fn set_pending_publishes_awaiting_settle(&self, n: usize) {
        self.pending_publishes_awaiting_settle.set(clamp_gauge(n));
        self.otel
            .pending_publishes_awaiting_settle
            .record(clamp_gauge(n), &[]);
    }

    /// Set how many publishes are held for a peer this node cannot reach: gated
    /// publishes with a forward owed to a node whose link is down or that
    /// membership holds dead (ADR 0087 D1, D9). A subset of `pending_publishes`.
    pub fn set_publish_held_for_unreachable(&self, n: usize) {
        self.publish_held_for_unreachable.set(clamp_gauge(n));
        self.otel
            .publish_held_for_unreachable
            .record(clamp_gauge(n), &[]);
    }

    /// A shared-subscription delivery was placed on a `local` or a `remote`
    /// member (issue #613 item 3.5). Exactly two label values.
    ///
    /// Incremented ONCE per DELIVERED message, at the commit sites — never at a
    /// plan site. `plan_shared` is also peeked, under brownout, by
    /// `shared_plan_owes_durable`, so counting there would count refused and
    /// merely-peeked publishes; and a second increment on a re-selection would
    /// make `local + remote` exceed messages delivered, contradicting the
    /// "exactly one member per group" invariant this counter is supposed to
    /// corroborate.
    pub fn shared_selected(&self, locality: &str) {
        self.shared_selected_total
            .get_or_create(&LocalityLabel {
                locality: locality.to_string(),
            })
            .inc();
        self.otel
            .shared_selected
            .add(1, &[KeyValue::new("locality", locality.to_string())]);
    }

    /// Set the current count of admitted-but-uncompleted durable-append lane jobs,
    /// summed over sessions (issue #242). Sustained growth = a placement group's
    /// followers are not keeping up; the pre-drop warning for
    /// `publish_dropped{reason="append-backlog-full"}`.
    pub fn set_append_lane_jobs(&self, n: usize) {
        self.append_lane_jobs.set(clamp_gauge(n));
        self.otel.append_lane_jobs.record(clamp_gauge(n), &[]);
    }

    /// The pending-publish table's depth and charged bytes (issue #633). Both
    /// halves of one bound: the OLDEST entry is evicted, ack withheld, when either
    /// is reached — so these are the warning before
    /// `publish_dropped{reason="pending-cap"}` moves, which used to be the first
    /// an operator heard of it.
    pub fn set_pending_publishes(&self, entries: usize, bytes: usize) {
        self.pending_publishes.set(clamp_gauge(entries));
        self.otel
            .pending_publishes
            .record(clamp_gauge(entries), &[]);
        self.pending_publish_bytes.set(clamp_gauge(bytes));
        self.otel
            .pending_publish_bytes
            .record(clamp_gauge(bytes), &[]);
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

    /// Set the accounted bytes currently held in flow-control backlogs, summed across
    /// sessions (issue #241).
    ///
    /// **Sampled on the session sweep**, not live: it is a sizing and capacity-planning
    /// signal — the number an operator reads *before* choosing `MQTTD_MAX_BACKLOG_BYTES`
    /// and watches after — not an alerting edge. "Accounted" is the size definition in
    /// `mqttd::backpressure`: the per-entry envelope plus topic, payload and forwarded
    /// application properties; it is a sum of message bytes, not a heap measurement, so
    /// real RSS is somewhat higher.
    pub fn set_backlog_bytes(&self, n: usize) {
        self.backlog_bytes.set(clamp_gauge(n));
        self.otel.backlog_bytes.record(clamp_gauge(n), &[]);
    }

    /// Set the LARGEST single session's accounted backlog bytes.
    ///
    /// This exists because [`set_backlog_bytes`](Self::set_backlog_bytes) is a node-wide SUM,
    /// and the cap an operator sizes from it — `MQTTD_MAX_BACKLOG_BYTES` — is **per
    /// subscriber**. On a node with many sessions the sum is arbitrarily larger than what any
    /// one subscriber holds, so sizing a per-subscriber cap from it yields a number far too
    /// large (found in review: four documents told the operator to do exactly that). The max
    /// is the honest input to that decision; the sum still answers "how much RAM is in
    /// backlogs on this node".
    pub fn set_backlog_bytes_max(&self, n: usize) {
        self.backlog_bytes_max.set(clamp_gauge(n));
        self.otel.backlog_bytes_max.record(clamp_gauge(n), &[]);
    }

    /// Set the current count of placement-eligible cluster members (ADR 0020-T6).
    pub fn set_cluster_members(&self, n: usize) {
        self.cluster_members.set(clamp_gauge(n));
        self.otel.cluster_members.record(clamp_gauge(n), &[]);
    }

    /// Set one link's cumulative work counter. `stat` is one of `busy_seconds`,
    /// `write_wait_seconds`, `polls`, `frames_out`, `writes`, `frames_in`, `reads`.
    pub fn set_peer_link_stat(&self, peer: &str, stat: &'static str, value: f64) {
        self.peer_link_stat
            .get_or_create(&PeerLinkStatLabel {
                peer: peer.to_string(),
                stat: stat.to_string(),
            })
            .set(value);
    }

    /// How replication of one `eventual` rule-state namespace stands (ADR 0085 D10):
    /// how long its slowest link has been behind, and how many keys that link has
    /// yet to send.
    pub fn set_rule_state_replication(&self, namespace: &str, lag_secs: f64, pending: u64) {
        let label = StateNamespaceLabel {
            namespace: namespace.to_string(),
        };
        self.rule_state_replication_lag_seconds
            .get_or_create(&label)
            .set(lag_secs);
        self.rule_state_replication_pending
            .get_or_create(&label)
            .set(i64::try_from(pending).unwrap_or(i64::MAX));
    }

    /// The running count of rule state refused for `reason` in `namespace` (ADR 0085
    /// D10). `reason` is one of `declaration`, `clock_skew`, `namespace_full`,
    /// `store_full`, `brownout`, `malformed`, or `undeclared`. The count only rises.
    pub fn set_rule_state_replication_refused(
        &self,
        namespace: &str,
        reason: &'static str,
        total: u64,
    ) {
        let counter = self
            .rule_state_replication_refused_total
            .get_or_create(&StateRefusedLabel {
                namespace: namespace.to_string(),
                reason: reason.to_string(),
            })
            .clone();
        let so_far = counter.get();
        if total > so_far {
            counter.inc_by(total - so_far);
        }
    }

    pub fn set_peer_links(&self, n: usize) {
        self.peer_links.set(clamp_gauge(n));
        self.otel.peer_links.record(clamp_gauge(n), &[]);
    }

    /// Set the number of frames queued on peer links but not yet written
    /// (issue #504). This is the population that is counted `received` and is not
    /// yet `delivered`, and without it the difference is unattributable.
    pub fn set_peer_forwards_in_flight(&self, n: usize) {
        self.peer_forwards_in_flight.set(clamp_gauge(n));
        self.otel
            .peer_forwards_in_flight
            .record(clamp_gauge(n), &[]);
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

    /// Set the count of live persistent sessions currently hosted on a node that does
    /// not own their placement group (issue #284). Transiently non-zero around an
    /// ownership move — the sessions are closed so they relocate. **Sustained**
    /// non-zero is the one wedge shape rehome-on-settle cannot heal: the owner's peer
    /// address is unknown, so ADR 0005 §5 keeps serving locally rather than kicking
    /// the client into a reconnect loop, and those sessions are undeliverable (their
    /// publishers' acks are withheld with `not the owning node for this group`).
    pub fn set_misplaced_sessions(&self, n: usize) {
        self.misplaced_sessions.set(clamp_gauge(n));
        self.otel.misplaced_sessions.record(clamp_gauge(n), &[]);
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

    /// Observe one stage of a durable append (`stage` is the bounded set documented on
    /// `mqttd_durable_stage_seconds`).
    pub fn observe_durable_stage(&self, stage: &'static str, seconds: f64) {
        let label = || StageLabel {
            stage: stage.to_string(),
        };
        match DURABLE_STAGES.iter().position(|s| *s == stage) {
            Some(i) => self.durable_stage_cache[i]
                .get_or_init(|| self.durable_stage_seconds.get_or_create(&label()).clone())
                .observe(seconds),
            None => self
                .durable_stage_seconds
                .get_or_create(&label())
                .observe(seconds),
        }
        self.otel
            .durable_stage
            .record(seconds, &[KeyValue::new("stage", stage)]);
    }

    /// Observe one gated publish's server-side publish → ack release.
    /// Observe one runtime scheduling-delay sample (`probe` is `yield` or `channel`).
    pub fn observe_runtime_wake(&self, probe: &'static str, seconds: f64) {
        self.runtime_wake_seconds
            .get_or_create(&ProbeLabel {
                probe: probe.to_string(),
            })
            .observe(seconds);
    }

    pub fn observe_publish_ack(&self, seconds: f64) {
        self.publish_ack_seconds.observe(seconds);
        self.otel.publish_ack.record(seconds, &[]);
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

    /// A held delivery was acknowledged inside the write-elision window after `held_seconds`
    /// and was never written (ADR 0089).
    pub fn durable_elided(&self, held_seconds: f64) {
        self.durable_elided_total.inc();
        self.durable_elision_hold_seconds.observe(held_seconds);
        self.otel.durable_elided.add(1, &[]);
        self.otel.durable_elision_hold.record(held_seconds, &[]);
    }

    /// A held delivery fell back to a durable write; `reason` is a bounded class
    /// (`"timeout"`, `"disconnect"`, `"brownout"`, `"drain"`).
    pub fn durable_elision_fallback(&self, reason: &str) {
        self.durable_elision_fallback_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .durable_elision_fallbacks
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// A session kept missing the write-elision window and is written directly for a
    /// while (ADR 0089 Amendment 1 A3).
    pub fn durable_elision_backoff(&self) {
        self.durable_elision_backoff_total.inc();
        self.otel.durable_elision_backoffs.add(1, &[]);
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

    /// This node asked a `cert-miss` sender for its certificate (issue #971).
    pub fn gossip_cert_requested(&self) {
        self.gossip_cert_requests_total.inc();
        self.otel.gossip_cert_requests.add(1, &[]);
    }

    /// `n` last direct probes of a suspect had `outcome` — a bounded class: `"sent"`,
    /// `"spared"` (the suspect answered) or `"silent"` (it was declared dead)
    /// (ADR 0016).
    pub fn swim_last_probes(&self, outcome: &str, n: u64) {
        self.swim_last_probes_total
            .get_or_create(&RunOutcomeLabel {
                outcome: outcome.to_string(),
            })
            .inc_by(n);
        self.otel
            .swim_last_probes
            .add(n, &[KeyValue::new("outcome", outcome.to_string())]);
    }

    /// The SWIM driver found that this node had not been scheduled for `stalled_ms`
    /// (ADR 0016).
    pub fn swim_stalled(&self, stalled_ms: u64) {
        self.swim_stalls_total.inc();
        self.otel.swim_stalls.add(1, &[]);
        self.swim_stalled_milliseconds_total.inc_by(stalled_ms);
        self.otel.swim_stalled_milliseconds.add(stalled_ms, &[]);
    }

    /// This node's SWIM local-health awareness changed (ADR 0016 §2).
    pub fn set_swim_awareness(&self, awareness: u8) {
        self.swim_awareness.set(i64::from(awareness));
        self.otel.swim_awareness.record(i64::from(awareness), &[]);
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

    /// Record a finished online-backup run (ADR 0062). `outcome` is `"ok"` or `"error"`;
    /// `duration_ms` is the run's wall clock. Only an `ok` run advances the last-success
    /// timestamp — `started_unix` is the export's OWN start, not now, because the RPO the
    /// operator alerts on is measured from the instant the cut began.
    pub fn backup_run(&self, outcome: &str, duration_ms: u64, started_unix: Option<u64>) {
        self.backup_runs_total
            .get_or_create(&RunOutcomeLabel {
                outcome: outcome.to_string(),
            })
            .inc();
        self.otel
            .backup_runs
            .add(1, &[KeyValue::new("outcome", outcome.to_string())]);
        let ms = i64::try_from(duration_ms).unwrap_or(i64::MAX);
        self.backup_duration_ms.set(ms);
        self.otel.backup_duration_ms.record(ms, &[]);
        if let Some(started) = started_unix {
            let at = i64::try_from(started).unwrap_or(i64::MAX);
            self.backup_last_success_timestamp_seconds.set(at);
            self.otel
                .backup_last_success_timestamp_seconds
                .record(at, &[]);
        }
    }

    /// Publish the restore state (ADR 0062): 0 none, 1 in progress, 2 completed, 3 failed.
    pub fn set_restore_state(&self, state: i64) {
        self.restore_state.set(state);
        self.otel.restore_state.record(state, &[]);
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

    /// One rule evaluation (ADR 0083): `result` is `passed`, `no_result` or `failed`.
    /// `rule` is an id from the operator's rules file — bounded by that file.
    pub fn rule_evaluated(&self, rule: &str, result: &'static str) {
        self.rule_evaluations_total
            .get_or_create(&RuleResultLabel {
                rule: rule.to_string(),
                result: result.to_string(),
            })
            .inc();
        self.otel.rule_evaluations.add(
            1,
            &[
                KeyValue::new("rule", rule.to_string()),
                KeyValue::new("result", result),
            ],
        );
    }

    /// One rule action run (ADR 0083): `result` is `ok` or `failed`.
    pub fn rule_action(&self, rule: &str, result: &'static str) {
        self.rule_actions_total
            .get_or_create(&RuleResultLabel {
                rule: rule.to_string(),
                result: result.to_string(),
            })
            .inc();
        self.otel.rule_actions.add(
            1,
            &[
                KeyValue::new("rule", rule.to_string()),
                KeyValue::new("result", result),
            ],
        );
    }

    /// One republish action of `rule` not run because its message would loop (ADR
    /// 0083): `guard` is `same_rule` or `depth`.
    pub fn rule_recursive_republish(&self, rule: &str, guard: &'static str) {
        self.rule_recursive_republish_total
            .get_or_create(&RuleGuardLabel {
                rule: rule.to_string(),
                guard: guard.to_string(),
            })
            .inc();
        self.otel.rule_recursive_republish.add(
            1,
            &[
                KeyValue::new("rule", rule.to_string()),
                KeyValue::new("guard", guard),
            ],
        );
    }

    /// The time one live evaluation of `rule` took (ADR 0084): its SQL and its actions'
    /// rendering.
    pub fn rule_eval_time(&self, rule: &str, elapsed: std::time::Duration) {
        let secs = elapsed.as_secs_f64();
        self.rule_eval_seconds_total
            .get_or_create(&RuleLabel {
                rule: rule.to_string(),
            })
            .inc_by(secs);
        self.otel
            .rule_eval_seconds
            .add(secs, &[KeyValue::new("rule", rule.to_string())]);
    }

    /// One call of a sandboxed rule function (ADR 0086): how long it took and, when it
    /// failed, the kind of failure.
    pub fn rule_function_call(
        &self,
        function: &str,
        elapsed: std::time::Duration,
        error: Option<&'static str>,
    ) {
        let label = FunctionLabel {
            function: function.to_string(),
        };
        let secs = elapsed.as_secs_f64();
        self.rule_function_calls_total.get_or_create(&label).inc();
        self.rule_function_seconds_total
            .get_or_create(&label)
            .inc_by(secs);
        let of = [KeyValue::new("function", function.to_string())];
        self.otel.rule_function_calls.add(1, &of);
        self.otel.rule_function_seconds.add(secs, &of);
        if let Some(kind) = error {
            self.rule_function_errors_total
                .get_or_create(&FunctionKindLabel {
                    function: function.to_string(),
                    kind: kind.to_string(),
                })
                .inc();
            self.otel.rule_function_errors.add(
                1,
                &[
                    KeyValue::new("function", function.to_string()),
                    KeyValue::new("kind", kind),
                ],
            );
        }
    }

    /// What a namespace of the rule state store holds now (ADR 0085 D10): its keys,
    /// their bytes, and the keys past their expiry that are still held. Creates the
    /// namespace's `mqttd_rule_state_expired_total` series at zero, so a namespace
    /// that is there has every series a dashboard reads.
    pub fn set_rule_state(&self, namespace: &str, keys: u64, bytes: u64, expiry_backlog: u64) {
        let label = StateNamespaceLabel {
            namespace: namespace.to_string(),
        };
        let of = [KeyValue::new("namespace", namespace.to_string())];
        let int = |n: u64| i64::try_from(n).unwrap_or(i64::MAX);
        self.rule_state_keys.get_or_create(&label).set(int(keys));
        self.rule_state_bytes.get_or_create(&label).set(int(bytes));
        self.rule_state_expiry_backlog
            .get_or_create(&label)
            .set(int(expiry_backlog));
        drop(self.rule_state_expired_total.get_or_create(&label));
        self.otel.rule_state_keys.record(int(keys), &of);
        self.otel.rule_state_bytes.record(int(bytes), &of);
        self.otel
            .rule_state_expiry_backlog
            .record(int(expiry_backlog), &of);
    }

    /// A namespace the rules no longer hold: its gauges go, so a dropped namespace
    /// does not read as holding its last keys for ever. Its counters stay, as
    /// counters do.
    pub fn forget_rule_state(&self, namespace: &str) {
        let label = StateNamespaceLabel {
            namespace: namespace.to_string(),
        };
        self.rule_state_keys.remove(&label);
        self.rule_state_bytes.remove(&label);
        self.rule_state_expiry_backlog.remove(&label);
    }

    /// `n` more state calls on `namespace` of operation `op` that ended as `result`
    /// (`mqttd_rule_state_ops_total`).
    pub fn rule_state_ops(&self, namespace: &str, op: &'static str, result: &'static str, n: u64) {
        self.rule_state_ops_total
            .get_or_create(&StateOpLabel {
                namespace: namespace.to_string(),
                op: op.to_string(),
                result: result.to_string(),
            })
            .inc_by(n);
        self.otel.rule_state_ops.add(
            n,
            &[
                KeyValue::new("namespace", namespace.to_string()),
                KeyValue::new("op", op),
                KeyValue::new("result", result),
            ],
        );
    }

    /// `n` more keys of `namespace` removed at their expiry
    /// (`mqttd_rule_state_expired_total`).
    pub fn rule_state_expired(&self, namespace: &str, n: u64) {
        self.rule_state_expired_total
            .get_or_create(&StateNamespaceLabel {
                namespace: namespace.to_string(),
            })
            .inc_by(n);
        self.otel
            .rule_state_expired
            .add(n, &[KeyValue::new("namespace", namespace.to_string())]);
    }

    /// What the connections' dictionaries hold now (`proc_dict_*`, ADR 0085), and
    /// `refused` more puts that were refused room.
    pub fn rule_state_dictionaries(&self, bytes: u64, refused: u64) {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        self.rule_state_dictionary_bytes.set(bytes);
        self.otel.rule_state_dictionary_bytes.record(bytes, &[]);
        self.rule_state_dictionary_refused_total.inc_by(refused);
        if refused > 0 {
            self.otel.rule_state_dictionary_refused.add(refused, &[]);
        }
    }

    /// What a rule-function module's instance pool holds now (ADR 0086), and how many
    /// instances it has given up since it was loaded (`replaced_total`, cumulative: the
    /// counter is moved up to it, never back).
    pub fn rule_module(
        &self,
        module: &str,
        instances: u64,
        memory_bytes: u64,
        replaced_total: u64,
    ) {
        let label = ModuleLabel {
            module: module.to_string(),
        };
        let (instances, memory) = (
            i64::try_from(instances).unwrap_or(i64::MAX),
            i64::try_from(memory_bytes).unwrap_or(i64::MAX),
        );
        self.rule_module_instances
            .get_or_create(&label)
            .set(instances);
        self.rule_module_memory_bytes
            .get_or_create(&label)
            .set(memory);
        let of = [KeyValue::new("module", module.to_string())];
        self.otel.rule_module_instances.record(instances, &of);
        self.otel.rule_module_memory_bytes.record(memory, &of);
        // Callers on several threads report the same cumulative count in any order:
        // whichever moves the counter up accounts for exactly the step it made.
        let before = self
            .rule_module_instances_replaced_total
            .get_or_create(&label)
            .inner()
            .fetch_max(replaced_total, std::sync::atomic::Ordering::Relaxed);
        if replaced_total > before {
            self.otel
                .rule_module_instances_replaced
                .add(replaced_total - before, &of);
        }
    }

    /// One rule's counts so far (ADR 0084), read back from its three families without
    /// creating a series: a rule that never matched has none, and still reads zeros.
    /// Each lookup's read guard is dropped before the next is taken — the family lock
    /// is not re-entrant while a writer waits, and an evaluation creating a series is
    /// one.
    #[must_use]
    pub fn rule_counts(&self, rule: &str) -> RuleCounts {
        let get = |family: &Family<RuleResultLabel, Counter>, result: &str| {
            family
                .get(&RuleResultLabel {
                    rule: rule.to_string(),
                    result: result.to_string(),
                })
                .map_or(0, |c| c.get())
        };
        RuleCounts {
            passed: get(&self.rule_evaluations_total, "passed"),
            no_result: get(&self.rule_evaluations_total, "no_result"),
            failed: get(&self.rule_evaluations_total, "failed"),
            actions_ok: get(&self.rule_actions_total, "ok"),
            actions_failed: get(&self.rule_actions_total, "failed"),
            eval_ns: self
                .rule_eval_seconds_total
                .get(&RuleLabel {
                    rule: rule.to_string(),
                })
                .map_or(0, |c| secs_to_ns(c.get())),
        }
    }

    /// The rules now loaded (ADR 0083): how many are enabled, and the file's
    /// checksum (empty when no rules file is configured). The previous checksum's
    /// series is zeroed so exactly one reads 1 per node.
    ///
    /// # Panics
    /// Never in practice: the internal mutex is only held for these few lines.
    pub fn set_rules_loaded(&self, enabled: usize, checksum: &str) {
        let n = i64::try_from(enabled).unwrap_or(i64::MAX);
        self.rules_loaded.set(n);
        self.otel.rules_loaded.record(n, &[]);
        let mut prev = self
            .rules_info_prev
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if prev.as_deref() == Some(checksum) {
            return;
        }
        if let Some(old) = prev.replace(checksum.to_string()) {
            self.rules_info
                .get_or_create(&ChecksumLabel { checksum: old })
                .set(0);
        }
        drop(prev);
        self.rules_info
            .get_or_create(&ChecksumLabel {
                checksum: checksum.to_string(),
            })
            .set(1);
        self.otel
            .rules_info
            .record(1, &[KeyValue::new("checksum", checksum.to_string())]);
    }

    /// Record whether this node currently considers itself SWIM-isolated
    /// (issue #368): its own probes go unanswered while inbound gossip still
    /// paints a fresh-looking view.
    pub fn set_swim_isolated(&self, isolated: bool) {
        self.swim_isolated.set(i64::from(isolated));
        self.otel.swim_isolated.record(i64::from(isolated), &[]);
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

    /// A rehome-on-settle decision for a live session found hosted on a node that does
    /// not own its placement group (issue #284). Bounded reasons only — never a
    /// per-client value:
    ///
    /// * `stale-owner` — the session was closed, so the client's next CONNECT relocates
    ///   it to the committed owner. Expected in ones after a node roll.
    /// * `unrelocatable` — the condition holds but the owner's peer address is unknown,
    ///   so ADR 0005 §5 keeps serving locally instead of kicking into a reconnect loop.
    ///   These sessions stay undeliverable; see [`set_misplaced_sessions`](Self::set_misplaced_sessions).
    /// * `cooldown` — a repeat close suppressed within the per-session cooldown, so a
    ///   flapping placement cannot turn into a close loop.
    /// * `deferred` — the session was over this tick's close cap and is retried on a
    ///   later tick, so a mass ownership move drains at a paced rate instead of closing
    ///   (and will-publishing for) every session in one dispatch. Counted **once per
    ///   session per deferral episode**, not once per tick: the pass re-derives its
    ///   candidates every tick, so per-tick counting would report ~n²/(2·cap) for an
    ///   n-session move — ~45 000 samples for the 1700-session resize this cap exists
    ///   for — and an operator sizing a drain from it would overestimate the backlog by
    ///   more than an order of magnitude.
    ///
    /// Every `stale-owner` close also publishes the client's **Last Will**: a server
    /// DISCONNECT is not a client DISCONNECT, so [MQTT-3.1.2-8] / §3.14.4 keep the will
    /// armed, exactly as for session takeover and `evict`. A roll or resize therefore
    /// emits one LWT per rehomed session — treat this counter as the suppressor signal
    /// for device-offline alerting while it climbs.
    pub fn session_rehomed(&self, reason: &str) {
        self.session_rehomes_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .session_rehomes
            .add(1, &[KeyValue::new("kind", reason.to_string())]);
    }

    /// A detach whose ADR 0009 §3 absolute expiry deadline could not be persisted, by
    /// bounded `reason`:
    ///
    /// * `not-owner` — this node does not hold the session group's lease, so the
    ///   group-routed write is refused by construction. The structural case after a
    ///   rehome close (issue #284): the deadline is deliberately NOT attempted rather
    ///   than attempted-and-swallowed.
    /// * `error` — the write was attempted and failed for another reason (no quorum, a
    ///   backend error).
    ///
    /// Either way the durable session record is left without a deadline, so a client that
    /// never returns leaves a persistent session (and its queue) behind. The deadline is
    /// re-established by the client's next CONNECT wherever it lands; this counter is the
    /// visibility for the case where it never does.
    pub fn session_expiry_unpersisted(&self, reason: &str) {
        self.session_expiry_unpersisted_total
            .get_or_create(&ReasonLabel {
                reason: reason.to_string(),
            })
            .inc();
        self.otel
            .session_expiry_unpersisted
            .add(1, &[KeyValue::new("kind", reason.to_string())]);
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

    /// Record which crypto module this binary runs (ADR 0068) — called once at
    /// startup with `mqtt_net::tls::crypto_module()`'s answer.
    pub fn set_crypto_module(&self, module: &str) {
        self.crypto_module_info
            .get_or_create(&CryptoModuleLabel {
                module: module.to_string(),
            })
            .set(1);
    }

    /// One audit record shed by the SIEM exporter's bounded queue (ADR 0066 T3).
    pub fn audit_export_dropped(&self) {
        self.audit_export_dropped_total.inc();
        self.otel.audit_export_dropped.add(1, &[]);
    }

    /// One gated publish at `tier` (ADR 0072): `quorum`, `local`, or `relaxed`.
    pub fn publish_tier(&self, tier: &str) {
        self.publish_tier_total
            .get_or_create(&TierLabel {
                tier: tier.to_string(),
            })
            .inc();
    }

    /// Advance the durable-write serializer's counters (ADR 0071) by the deltas a
    /// poll observed, and refresh the max-batch gauge. Deltas, so the exposed
    /// series stay true monotonic counters over a poll-based source.
    pub fn durable_writer_progress(
        &self,
        batches: u64,
        ops: u64,
        max_batch: u64,
        commit_micros: u64,
    ) {
        self.durable_writer_batches_total.inc_by(batches);
        self.durable_writer_ops_total.inc_by(ops);
        self.durable_writer_max_batch.set(clamp_gauge(
            usize::try_from(max_batch).unwrap_or(usize::MAX),
        ));
        self.durable_writer_commit_micros_total
            .inc_by(commit_micros);
    }

    /// Record the boot-time volume probe (ADR 0076): the single-writer barrier
    /// floor and the 4-stream aggregate of the data-dir volume.
    pub fn set_store_barrier_probe(&self, single_per_sec: u64, four_stream_per_sec: u64) {
        self.store_barrier_floor.set(clamp_gauge(
            usize::try_from(single_per_sec).unwrap_or(usize::MAX),
        ));
        self.store_barrier_floor_4stream.set(clamp_gauge(
            usize::try_from(four_stream_per_sec).unwrap_or(usize::MAX),
        ));
    }

    /// Publish the replica store's committed layout and, when the volume's own
    /// measurements disagree with it, the shard count they suggest (ADR 0076
    /// T2). `advice` is `None` while the committed layout still fits — the
    /// advisor is silent by default, and never reshards anything itself.
    pub fn set_store_shards(&self, shards: usize, advice: Option<usize>) {
        self.store_shards.set(clamp_gauge(shards));
        self.store_reshard_advice
            .set(clamp_gauge(advice.unwrap_or(0)));
    }

    /// Replace the validity windows behind the TLS expiry gauges (ADR 0088 D2) with
    /// `windows`, one per role: what this node has loaded, as of startup or the reload
    /// that just committed. A role that is no longer loaded stops being exported.
    ///
    /// `role` is one of a fixed set (see `RoleLabel`). The gauges themselves are
    /// computed from these windows each time the metrics are read, so they keep counting
    /// down between reloads at no cost to anything else.
    pub fn set_tls_validity(&self, windows: Vec<(&'static str, ValidityWindow)>) {
        let mut held = lock_validity(&self.tls_validity);
        for (role, _) in held.iter() {
            if !windows.iter().any(|(r, _)| r == role) {
                let label = RoleLabel {
                    role: (*role).to_string(),
                };
                self.tls_expiry_seconds.remove(&label);
                self.tls_lifetime_spent_ratio.remove(&label);
            }
        }
        *held = windows;
    }

    /// Export the fraction of a lifetime past which this node warns about its TLS
    /// material (`tls.expiry_warn_permille`), so an alert can compare against the
    /// node's own setting instead of a copy of it.
    pub fn set_tls_expiry_warn_permille(&self, permille: u16) {
        let ratio = f64::from(permille) / 1000.0;
        self.tls_expiry_warn_ratio.set(ratio);
        self.tls_warn_permille
            .store(u32::from(permille), std::sync::atomic::Ordering::Relaxed);
    }

    /// Set the TLS expiry gauges from the stored windows, as of `now`.
    fn refresh_tls_expiry(&self, now: i64) {
        for (role, window) in lock_validity(&self.tls_validity).iter() {
            let label = RoleLabel {
                role: (*role).to_string(),
            };
            self.tls_expiry_seconds
                .get_or_create(&label)
                .set(window.seconds_left(now));
            self.tls_lifetime_spent_ratio
                .get_or_create(&label)
                .set(window.spent(now));
        }
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

/// Poison-tolerant lock of the TLS validity windows: a panic elsewhere must not take the
/// scrape down with it.
fn lock_validity(
    windows: &TlsValidity,
) -> std::sync::MutexGuard<'_, Vec<(&'static str, ValidityWindow)>> {
    windows
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
/// Whole nanoseconds in `secs` of a seconds counter. A float-to-int `as` saturates
/// (negative and NaN to 0), and a counter is never negative anyway.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn secs_to_ns(secs: f64) -> u64 {
    (secs * 1e9).round() as u64
}

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

/// Register a MICROSECOND-scale latency histogram (exponential buckets ~1µs..0.5s)
/// under `name`/`help` (issue #613 item 1.6).
///
/// Every other latency histogram here starts at 100µs because it measures a round
/// trip — an fsync, an HTTP hook, a quorum append. A per-message CPU term does not:
/// the cross-node fan-out costs single-digit microseconds per peer, so with a 100µs
/// first bucket every observation would land in bucket one both before and after a
/// change to it, and the series would be structurally unable to show the difference
/// it exists to show.
fn register_micro_latency_histogram(
    registry: &mut Registry,
    name: &'static str,
    help: &'static str,
) -> Histogram {
    let h = Histogram::new(exponential_buckets(1e-6, 2.0, 20));
    registry.register(name, help, h.clone());
    h
}

/// Register a labelled latency histogram family with WIDE exponential buckets
/// (~100µs..13s), so a dispatch parked on a full replication RPC timeout (5s,
/// `mqtt-cluster/src/repl_net.rs`) is on-scale rather than clamped into the top
/// bucket (issue #242).
/// Fine latency buckets for stage timing: 20µs × 1.5ⁿ, 32 buckets (≈20µs … 5.8s). The
/// 2× buckets elsewhere jump 25 → 51 → 102ms, which cannot tell a 1ms fsync from a 2ms
/// one — exactly the distinction a durable-path stage breakdown exists to make.
fn fine_latency_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(0.00002, 1.5, 32)
}

fn register_wide_latency_histogram_family<L>(
    registry: &mut Registry,
    name: &'static str,
    help: &'static str,
) -> Family<L, Histogram>
where
    L: Clone + std::hash::Hash + Eq + EncodeLabelSet + Send + Sync + std::fmt::Debug + 'static,
{
    let family = Family::<L, Histogram>::new_with_constructor(|| {
        Histogram::new(exponential_buckets(0.0001, 2.0, 18))
    });
    registry.register(name, help, family.clone());
    family
}

#[cfg(test)]
mod tests {
    use super::{Metrics, RuleCounts};

    /// ADR 0084: the `$SYS` statistics read a rule's counts back without creating a
    /// series, so "a rule with no series has never matched" (docs/RULES.md) stays true.
    #[test]
    fn rule_counts_read_back_without_creating_a_series() {
        let m = Metrics::new("test");
        assert_eq!(m.rule_counts("never"), RuleCounts::default());
        assert!(
            !m.render().contains("rule=\"never\""),
            "reading a never-matched rule created a series:\n{}",
            m.render()
        );
        for _ in 0..3 {
            m.rule_evaluated("r", "passed");
        }
        m.rule_evaluated("r", "no_result");
        m.rule_evaluated("r", "failed");
        m.rule_evaluated("r", "failed");
        for _ in 0..4 {
            m.rule_action("r", "ok");
        }
        m.rule_action("r", "failed");
        m.rule_evaluated("other", "passed");
        m.rule_eval_time("r", std::time::Duration::from_micros(150));
        m.rule_eval_time("r", std::time::Duration::from_nanos(2_500));
        assert_eq!(
            m.rule_counts("r"),
            RuleCounts {
                passed: 3,
                no_result: 1,
                failed: 2,
                actions_ok: 4,
                actions_failed: 1,
                eval_ns: 152_500,
            }
        );
        assert!(
            m.render()
                .contains("mqttd_rule_eval_seconds_total{rule=\"r\"} 0.0001525"),
            "the time is exported in seconds:\n{}",
            m.render()
        );
        assert!(
            !m.render().contains("rule=\"never\""),
            "still no series for the never-matched rule"
        );
    }

    /// ADR 0086 D10: per function, calls, errors by kind and time; per module, what its
    /// instance pool holds and how many instances it gave up.
    #[test]
    fn sandboxed_function_calls_are_counted_by_function_and_kind() {
        let m = Metrics::new("test");
        assert!(
            !m.render().contains("function=\"jq\""),
            "no series before the first call"
        );
        m.rule_function_call("jq", std::time::Duration::from_micros(50), None);
        m.rule_function_call("jq", std::time::Duration::from_micros(150), Some("timeout"));
        m.rule_function_call("jq", std::time::Duration::from_micros(50), Some("timeout"));
        m.rule_function_call("jq", std::time::Duration::from_micros(250), Some("memory"));
        m.rule_module("jq", 2, 7 << 20, 1);
        // A report that lags another's does not move the counter back.
        m.rule_module("jq", 3, 9 << 20, 3);
        m.rule_module("jq", 3, 9 << 20, 2);
        let text = m.render();
        for line in [
            "mqttd_rule_function_calls_total{function=\"jq\"} 4",
            "mqttd_rule_function_errors_total{function=\"jq\",kind=\"timeout\"} 2",
            "mqttd_rule_function_errors_total{function=\"jq\",kind=\"memory\"} 1",
            "mqttd_rule_function_seconds_total{function=\"jq\"} 0.0005",
            "mqttd_rule_module_instances{module=\"jq\"} 3",
            "mqttd_rule_module_memory_bytes{module=\"jq\"} 9437184",
            "mqttd_rule_module_instances_replaced_total{module=\"jq\"} 3",
        ] {
            assert!(text.contains(line), "missing {line}:\n{text}");
        }
    }

    /// ADR 0085 D10: replication of `eventual` rule state, per namespace. The refused
    /// count is a counter fed with running totals: it follows them up and never back.
    #[test]
    fn rule_state_replication_is_reported_per_namespace() {
        let m = Metrics::new("test");
        assert!(
            !m.render().contains("namespace=\"alerts\""),
            "no series before a namespace is reported"
        );
        m.set_rule_state_replication("alerts", 1.5, 42);
        m.set_rule_state_replication_refused("alerts", "namespace_full", 3);
        m.set_rule_state_replication_refused("alerts", "namespace_full", 7);
        m.set_rule_state_replication_refused("alerts", "namespace_full", 5);
        m.set_rule_state_replication_refused("alerts", "clock_skew", 0);
        m.set_rule_state_replication_refused("(undeclared)", "undeclared", 2);
        let text = m.render();
        for line in [
            "mqttd_rule_state_replication_lag_seconds{namespace=\"alerts\"} 1.5",
            "mqttd_rule_state_replication_pending{namespace=\"alerts\"} 42",
            "mqttd_rule_state_replication_refused_total{namespace=\"alerts\",reason=\"namespace_full\"} 7",
            "mqttd_rule_state_replication_refused_total{namespace=\"alerts\",reason=\"clock_skew\"} 0",
            "mqttd_rule_state_replication_refused_total{namespace=\"(undeclared)\",reason=\"undeclared\"} 2",
        ] {
            assert!(text.contains(line), "missing {line}:\n{text}");
        }
    }

    /// ADR 0085 D10: the rule state series, by namespace; a namespace that is there
    /// has its expiry counter at zero, a call is a series once it happened, and a
    /// namespace that is gone keeps its counters and loses its gauges.
    #[test]
    fn rule_state_is_exported_by_namespace_operation_and_result() {
        let m = Metrics::new("test");
        assert!(
            !m.render().contains("namespace=\""),
            "no series before a namespace is reported"
        );
        m.set_rule_state("alerts", 3, 4096, 1);
        m.rule_state_ops("alerts", "incr", "ok", 7);
        m.rule_state_ops("alerts", "incr", "namespace_full", 2);
        m.rule_state_ops("rule:hot", "held", "ok", 1);
        m.rule_state_expired("alerts", 5);
        m.rule_state_dictionaries(640, 2);
        let text = m.render();
        for line in [
            "mqttd_rule_state_keys{namespace=\"alerts\"} 3",
            "mqttd_rule_state_bytes{namespace=\"alerts\"} 4096",
            "mqttd_rule_state_expiry_backlog{namespace=\"alerts\"} 1",
            "mqttd_rule_state_expired_total{namespace=\"alerts\"} 5",
            "mqttd_rule_state_ops_total{namespace=\"alerts\",op=\"incr\",result=\"ok\"} 7",
            "mqttd_rule_state_ops_total{namespace=\"alerts\",op=\"incr\",result=\"namespace_full\"} 2",
            "mqttd_rule_state_ops_total{namespace=\"rule:hot\",op=\"held\",result=\"ok\"} 1",
            "mqttd_rule_state_dictionary_bytes 640",
            "mqttd_rule_state_dictionary_refused_total 2",
        ] {
            assert!(text.contains(line), "missing {line}:\n{text}");
        }
        m.set_rule_state("quiet", 0, 0, 0);
        assert!(m
            .render()
            .contains("mqttd_rule_state_expired_total{namespace=\"quiet\"} 0"));
        m.forget_rule_state("alerts");
        let text = m.render();
        assert!(
            !text.contains("mqttd_rule_state_keys{namespace=\"alerts\"}")
                && !text.contains("mqttd_rule_state_bytes{namespace=\"alerts\"}")
                && !text.contains("mqttd_rule_state_expiry_backlog{namespace=\"alerts\"}"),
            "a dropped namespace's gauges are gone:\n{text}"
        );
        assert!(text.contains("mqttd_rule_state_expired_total{namespace=\"alerts\"} 5"));
    }

    #[test]
    fn render_produces_valid_openmetrics_exposition() {
        let m = Metrics::new("1.2.3");
        let out = m.render();
        // Build info is stamped at construction with the version label.
        assert!(
            out.contains("mqttd_build_info{version=\"1.2.3\"} 1"),
            "build_info missing:\n{out}"
        );
        // Plain (unlabelled) metrics carry their HELP/TYPE lines from boot.
        assert!(out.contains("# TYPE mqttd_connections_active gauge"));
        // prometheus-client 0.25: a label FAMILY with no observed series is omitted
        // entirely — no HELP/TYPE until the first observation. Operators scraping a
        // fresh broker will not see e.g. publish_received before the first PUBLISH;
        // dashboards must treat the series as absent-until-first-event.
        assert!(
            !out.contains("mqttd_publish_received"),
            "an unobserved family rendered — upstream reverted the empty-family \
             omission, revisit this pin:\n{out}"
        );
        // Once a series exists, the family renders fully (the `_total` counter
        // suffix is added to the sample line, not the metric family name).
        m.publish_received(1);
        let out = m.render();
        assert!(out.contains("# HELP mqttd_publish_received "));
        assert!(out.contains("# TYPE mqttd_publish_received counter"));
        // The OpenMetrics exposition terminates with the EOF marker.
        assert!(out.trim_end().ends_with("# EOF"), "missing # EOF:\n{out}");
    }

    /// ADR 0089: write elision's counters (T4) and the backoff counter (Amendment 1 A3).
    #[test]
    fn elision_counters_move_and_render() {
        let m = Metrics::new("t");
        m.durable_elided(0.001);
        m.durable_elision_fallback("timeout");
        m.durable_elision_backoff();
        let out = m.render();
        assert!(out.contains("mqttd_durable_elided_total 1"), "{out}");
        assert!(
            out.contains("mqttd_durable_elision_backoff_total 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_durable_elision_fallback_total{reason=\"timeout\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_durable_elision_hold_seconds_count 1"),
            "{out}"
        );
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
        m.swim_last_probes("sent", 3);
        m.swim_last_probes("spared", 2);
        m.swim_last_probes("silent", 1);
        m.swim_stalled(300);
        m.swim_stalled(1200);
        m.set_swim_awareness(2);
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
        for series in [
            "mqttd_swim_last_probes_total{outcome=\"sent\"} 3",
            "mqttd_swim_last_probes_total{outcome=\"spared\"} 2",
            "mqttd_swim_last_probes_total{outcome=\"silent\"} 1",
            "mqttd_swim_stalls_total 2",
            "mqttd_swim_stalled_milliseconds_total 1500",
            "mqttd_swim_awareness 2",
        ] {
            assert!(out.contains(series), "{series}: {out}");
        }
        assert!(out.contains("mqttd_replication_desired 3"), "{out}");
        assert!(out.contains("mqttd_replication_min_actual 1"), "{out}");
        assert!(out.contains("mqttd_replication_write_floor 2"), "{out}");
    }

    /// Issue #242: the hub time-on-loop histogram and the append-lane gauge are the
    /// head-of-line regression tripwires docs/OPERATIONS.md alerts on — their NAMES,
    /// the bounded `{command}` label, and the fact that they are populated at all are
    /// pinned here, where a rename or a dropped observation fails.
    #[test]
    fn durable_stage_and_publish_ack_histograms_render() {
        let m = Metrics::new("1.2.3");
        m.observe_durable_stage("fsync", 0.0012);
        m.observe_durable_stage("quorum", 0.0031);
        m.observe_publish_ack(0.004);
        let out = m.render();
        assert!(
            out.contains("mqttd_durable_stage_seconds_count{stage=\"fsync\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_durable_stage_seconds_count{stage=\"quorum\"} 1"),
            "{out}"
        );
        assert!(out.contains("mqttd_publish_ack_seconds_count 1"), "{out}");
        // Fine buckets: the first bucket holding the 1.2ms fsync is within 1.5x of it,
        // where the 2x buckets elsewhere would put it up to 2x away.
        let first_le = out
            .lines()
            .filter(|l| {
                l.starts_with("mqttd_durable_stage_seconds_bucket{")
                    && l.contains("stage=\"fsync\"")
            })
            .find(|l| l.ends_with(" 1"))
            .and_then(|l| {
                l.split("le=\"")
                    .nth(1)?
                    .split('"')
                    .next()?
                    .parse::<f64>()
                    .ok()
            })
            .expect("an fsync bucket holding the observation");
        assert!(
            (0.0012..=0.0018).contains(&first_le),
            "first fsync bucket {first_le}"
        );
    }

    #[test]
    fn hub_dispatch_and_append_lane_metrics_render() {
        let m = Metrics::new("t");
        m.observe_hub_dispatch("publish", 0.0004);
        m.set_append_lane_jobs(3);
        m.publish_dropped("append-backlog-full");
        let out = m.render();
        assert!(
            out.contains("mqttd_hub_dispatch_seconds_count{command=\"publish\"} 1"),
            "{out}"
        );
        assert!(out.contains("mqttd_append_lane_jobs 3"), "{out}");
        m.set_pending_publishes(7, 4096);
        let out = m.render();
        assert!(out.contains("mqttd_pending_publishes 7"), "{out}");
        assert!(out.contains("mqttd_pending_publish_bytes 4096"), "{out}");
        assert!(
            out.contains("mqttd_publish_dropped_total{reason=\"append-backlog-full\"} 1"),
            "{out}"
        );
    }

    /// Issue #613: the scaling work's observables. Their NAMES and label keys are the
    /// contract that the hub/forwarding/delivery call sites and every wave proof-test
    /// read back, so a rename or a dropped record fails HERE rather than in a
    /// dashboard months later.
    #[test]
    fn scaling_metrics_render_with_their_documented_names() {
        let m = Metrics::new("t");
        m.observe_hub_fanout(4e-6, 7);
        m.observe_hub_fanout(1e-6, 0);
        m.set_hub_queue_depth(12);
        for (reason, on) in [
            ("takeover", true),
            ("scan", false),
            ("scan-incomplete", false),
            ("interest", false),
            ("mesh", false),
        ] {
            m.set_routing_unsettled(reason, on);
        }
        m.set_pending_publishes(40, 4096);
        m.set_pending_publishes_awaiting_settle(3);
        m.set_publish_held_for_unreachable(2);
        m.shared_selected("local");
        m.shared_selected("local");
        m.shared_selected("remote");
        m.publish_dropped("pending-cap-replay");
        let out = m.render();

        // The O(N) term is its OWN family: two fan-outs observed, seven links walked.
        assert!(
            out.contains("mqttd_hub_fanout_seconds_count 2"),
            "fan-out span not observed:\n{out}"
        );
        assert!(
            out.contains("mqttd_hub_fanout_peer_visits_total 7"),
            "peer visits not summed:\n{out}"
        );
        // ...and hub_dispatch_seconds is untouched, so the MqttdHubLoopHeld page
        // (an unselectored rate() over the whole family) keeps meaning what its
        // rule says. An empty family renders nothing at all here.
        assert!(
            !out.contains("mqttd_hub_dispatch_seconds"),
            "the fan-out split leaked into the dispatch family:\n{out}"
        );
        // The two observations must land in DIFFERENT buckets. With the 100µs-start
        // buckets of the other latency histograms both would sit in bucket one,
        // every bucket line would read 2, and this series could never show a
        // per-message microsecond term getting cheaper.
        assert!(
            out.lines()
                .filter(|l| l.starts_with("mqttd_hub_fanout_seconds_bucket"))
                .any(|l| l.ends_with(" 1")),
            "micro buckets missing — the O(N) term is unresolvable:\n{out}"
        );
        assert!(out.contains("mqttd_hub_queue_depth 12"), "{out}");
        // Every term is published, including the false ones — a gauge only ever set
        // to 1 never comes back down.
        assert!(
            out.contains("mqttd_routing_unsettled{reason=\"takeover\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_routing_unsettled{reason=\"scan-incomplete\"} 0"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_routing_unsettled{reason=\"mesh\"} 0"),
            "{out}"
        );
        assert!(out.contains("mqttd_pending_publishes 40"), "{out}");
        assert!(
            out.contains("mqttd_pending_publishes_awaiting_settle 3"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_publish_held_for_unreachable 2"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_shared_selected_total{locality=\"local\"} 2"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_shared_selected_total{locality=\"remote\"} 1"),
            "{out}"
        );
        // The admission refusal is a DISTINCT reason from the eviction backstop:
        // one is answered, the other loses a message whose publisher still waits.
        assert!(
            out.contains("mqttd_publish_dropped_total{reason=\"pending-cap-replay\"} 1"),
            "{out}"
        );
    }

    /// ADR 0088 D2: the expiry gauges are computed when the metrics are read, from the
    /// validity windows stored at load — so they count down between reloads, go negative
    /// (and past 1) after expiry, follow a reload, and drop a role that is gone.
    #[test]
    fn tls_expiry_gauges_are_computed_at_read_time_and_follow_a_reload() {
        use crate::validity::ValidityWindow;
        use std::time::{Duration, UNIX_EPOCH};

        let m = Metrics::new("t");
        let at = |secs: u64| UNIX_EPOCH + Duration::from_secs(secs);
        assert!(
            !m.render_at(at(0)).contains("mqttd_tls_expiry_seconds"),
            "no TLS material loaded exports no series"
        );
        m.set_tls_expiry_warn_permille(667);
        m.set_tls_validity(vec![
            (
                "peer",
                ValidityWindow {
                    not_before: 1_000,
                    not_after: 4_000,
                },
            ),
            (
                "peer_crl",
                ValidityWindow {
                    not_before: 0,
                    not_after: 10_000,
                },
            ),
        ]);
        let out = m.render_at(at(2_500));
        assert!(out.contains("mqttd_tls_expiry_warn_ratio 0.667"), "{out}");
        assert!(
            out.contains("mqttd_tls_expiry_seconds{role=\"peer\"} 1500"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_tls_lifetime_spent_ratio{role=\"peer\"} 0.5"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_tls_lifetime_spent_ratio{role=\"peer_crl\"} 0.25"),
            "{out}"
        );
        // Nothing was set in between: the same windows, read later.
        let out = m.render_at(at(5_500));
        assert!(
            out.contains("mqttd_tls_expiry_seconds{role=\"peer\"} -1500"),
            "expired is negative:\n{out}"
        );
        assert!(
            out.contains("mqttd_tls_lifetime_spent_ratio{role=\"peer\"} 1.5"),
            "{out}"
        );
        // A reload to a new certificate, and the revocation list no longer configured.
        m.set_tls_validity(vec![(
            "peer",
            ValidityWindow {
                not_before: 5_000,
                not_after: 9_000,
            },
        )]);
        let out = m.render_at(at(5_500));
        assert!(
            out.contains("mqttd_tls_expiry_seconds{role=\"peer\"} 3500"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_tls_lifetime_spent_ratio{role=\"peer\"} 0.125"),
            "{out}"
        );
        assert!(!out.contains("peer_crl"), "a dropped role is gone:\n{out}");
        // The only label is the fixed role.
        for forbidden in ["path=", "subject=", "serial=", "file="] {
            assert!(!out.contains(forbidden), "{forbidden} in:\n{out}");
        }
    }

    /// ADR 0088 D2: the expiry gauges reach an OTLP backend too. They are observable
    /// instruments, read from the stored windows when the export runs, so nothing has to
    /// record them in between. Multi-thread runtime for the synchronous `flush`, as in
    /// `otlp_export_posts_to_the_endpoint`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tls_expiry_gauges_are_exported_over_otlp() {
        use crate::validity::ValidityWindow;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel::<String>();
        let server = tokio::spawn(async move {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            // Read until both instrument names have arrived (the request may come in
            // several segments), then answer 200 so the exporter sees success.
            let mut got = Vec::new();
            let mut buf = vec![0u8; 8192];
            loop {
                let text = String::from_utf8_lossy(&got).into_owned();
                if [
                    "tls_expiry_seconds",
                    "tls_lifetime_spent_ratio",
                    "tls_expiry_warn_ratio",
                ]
                .iter()
                .all(|name| text.contains(name))
                {
                    break;
                }
                match sock.read(&mut buf).await {
                    Ok(n) if n > 0 => got.extend_from_slice(&buf[..n]),
                    _ => break,
                }
            }
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await;
            let _ = seen_tx.send(String::from_utf8_lossy(&got).into_owned());
        });

        let endpoint = format!("http://{addr}");
        let m = Metrics::with_otlp("t", &endpoint, Duration::from_hours(1), "node-test").unwrap();
        m.set_tls_expiry_warn_permille(667);
        m.set_tls_validity(vec![(
            "peer",
            ValidityWindow {
                not_before: 0,
                not_after: i64::MAX / 2,
            },
        )]);
        m.flush();

        let req = tokio::time::timeout(Duration::from_secs(5), seen_rx)
            .await
            .expect("OTLP export never reached the endpoint")
            .expect("the capture ended without a request");
        server.await.unwrap();
        for name in [
            "tls_expiry_seconds",
            "tls_lifetime_spent_ratio",
            "tls_expiry_warn_ratio",
            "peer",
        ] {
            assert!(req.contains(name), "OTLP payload missing {name}:\n{req}");
        }
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
        let m = Metrics::with_otlp("t", &endpoint, Duration::from_hours(1), "node-test").unwrap();
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
