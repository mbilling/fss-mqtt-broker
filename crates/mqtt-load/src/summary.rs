//! The final JSON summary — schema `mqtt-load.summary.v2`, documented in
//! `crates/mqtt-load/README.md`. Fields are only ever added within a schema version;
//! a removal or a change of meaning bumps the version.

use serde::Serialize;

use crate::config::{Sessions, Shape, Thresholds};
use crate::ctx::ErrorCounts;
use crate::hist::{HistogramExport, LatencySummary};
use crate::ledger::{LedgerReport, MissingId};

/// The schema identifier.
///
/// v2 (2026-10-26) added `schedule_start_unix_ms` and the bucket exports
/// `ack_histogram` / `e2e_histogram` on phases and totals, so that a rig running several
/// generator processes can merge their latency histograms exactly. Every v1 field is
/// unchanged.
pub const SCHEMA: &str = "mqtt-load.summary.v2";

/// The whole run.
#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    /// [`SCHEMA`].
    pub schema: &'static str,
    /// The tool and its version.
    pub tool: Tool,
    /// The run id, hex.
    pub run_id: String,
    /// Wall-clock start, Unix ms (for correlation with broker metrics only; no figure
    /// in this summary is computed from wall-clock time).
    pub started_unix_ms: u64,
    /// Wall-clock time the schedule started (offset 0 of every window's `start_s`),
    /// Unix ms: `started_unix_ms` plus the process clock's reading from then to the
    /// schedule start. For lining windows up with broker scrapes only.
    pub schedule_start_unix_ms: u64,
    /// The workload as run.
    pub workload: Workload,
    /// What was connected.
    pub connections: Connections,
    /// How the schedule's start was set.
    pub start: StartReport,
    /// Every window in time order: warm-ups, measured windows, and the drain.
    pub windows: Vec<WindowReport>,
    /// Per phase (step), aggregated over its measured windows.
    pub phases: Vec<PhaseReport>,
    /// Over every measured window of the run.
    pub totals: Aggregate,
    /// How the drain ended.
    pub drain: DrainReport,
    /// The identity ledger.
    pub ledger: LedgerReport,
    /// The first missing identities (`--report-missing`), for diagnosis.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub missing_sample: Vec<MissingId>,
    /// How evenly the broker spread deliveries over the consumers; absent without
    /// consumers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spread: Option<Spread>,
    /// The generator's own health.
    pub generator: GeneratorReport,
    /// Connection failures.
    pub errors: ErrorCounts,
    /// `ok`, `loss` (the ledger did not close), `void` (the generator saturated in a
    /// measured window — the run says nothing about the broker), or `error`
    /// (connections failed during the run). The process exit code follows it.
    pub outcome: Outcome,
}

/// The run's outcome, by precedence: error, void, loss, ok.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    /// Ledger closed, generator healthy, no connection failures.
    Ok,
    /// The ledger did not close.
    Loss,
    /// The generator saturated in a measured window.
    Void,
    /// Connections failed during the run.
    Error,
}

impl Outcome {
    /// The process exit code: 0 ok, 2 loss, 3 void, 4 error (1 is a run that could
    /// not start).
    #[must_use]
    pub fn exit_code(self) -> i32 {
        match self {
            Outcome::Ok => 0,
            Outcome::Loss => 2,
            Outcome::Void => 3,
            Outcome::Error => 4,
        }
    }
}

/// Name and version.
#[derive(Debug, Clone, Serialize)]
pub struct Tool {
    /// `mqtt-load`.
    pub name: &'static str,
    /// Crate version.
    pub version: &'static str,
}

/// The workload echo.
#[derive(Debug, Clone, Serialize)]
pub struct Workload {
    /// Per phase: target rate (msg/s), warm-up and measured seconds.
    pub phases: Vec<PhaseSpec>,
    /// Measured-window length, seconds.
    pub window_s: f64,
    /// Publisher connections.
    pub publishers: u32,
    /// Configured in-flight window per publisher.
    pub inflight: u16,
    /// `QoS`.
    pub qos: u8,
    /// Payload bytes.
    pub payload_bytes: usize,
    /// Topics.
    pub topics: usize,
    /// Topic prefix.
    pub topic_prefix: String,
    /// Subscription shape.
    pub shape: Shape,
    /// Consumers per topic (per group for `share`).
    pub consumers: usize,
    /// `$share` groups per topic.
    pub groups: usize,
    /// Consumer sessions.
    pub sessions: Sessions,
    /// Session expiry of durable consumers, seconds.
    pub session_expiry_s: u32,
    /// The busiest consumer session's expected load over the mean session's
    /// ([`crate::config::Config::session_skew`]); 1.0 is an even spread.
    pub session_skew: f64,
    /// Deliveries each published message should produce (streams per topic): received
    /// rates divide by this to compare with the publish rate.
    pub deliveries_per_message: usize,
    /// Broker endpoints.
    pub brokers: Vec<String>,
}

/// Deliveries per consumer connection over the whole run, against the share each
/// should get. With an even deal ([`crate::config::Config::session_skew`] near 1) an
/// uneven spread is the broker's doing: reported, not judged.
#[derive(Debug, Clone, Serialize)]
pub struct Spread {
    /// The busiest consumer's deliveries over the mean consumer's.
    pub consumer_skew: f64,
    /// The busiest consumer, by index (its client id ends in `-c<index>`).
    pub busiest_consumer: usize,
    /// The broker it was connected to, as an index into the workload's brokers.
    pub busiest_broker: usize,
    /// Consumers that received nothing.
    pub idle_consumers: usize,
}

impl Spread {
    /// The spread of `received` (per consumer, in `specs` order); `None` without
    /// consumers or deliveries.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn of(specs: &[crate::config::ConsumerSpec], received: &[u64]) -> Option<Self> {
        let total: u64 = received.iter().sum();
        if received.is_empty() || total == 0 {
            return None;
        }
        let (busiest, &max) = received.iter().enumerate().max_by_key(|&(_, n)| *n)?;
        let mean = total as f64 / received.len() as f64;
        Some(Self {
            consumer_skew: (max as f64 / mean * 100.0).round() / 100.0,
            busiest_consumer: busiest,
            busiest_broker: specs.get(busiest).map_or(0, |s| s.broker),
            idle_consumers: received.iter().filter(|&&n| n == 0).count(),
        })
    }
}

/// One phase as configured.
#[derive(Debug, Clone, Serialize)]
pub struct PhaseSpec {
    /// Target msg/s.
    pub rate: f64,
    /// Warm-up seconds.
    pub warmup_s: f64,
    /// Measured seconds.
    pub measure_s: f64,
}

/// What was connected.
#[derive(Debug, Clone, Serialize)]
pub struct Connections {
    /// Publisher connections.
    pub publishers: usize,
    /// Consumer connections.
    pub consumers: usize,
    /// Smallest Receive Maximum a broker granted a publisher.
    pub receive_maximum_min: u16,
    /// Smallest effective in-flight window (configured, capped by Receive Maximum).
    pub inflight_window_min: u16,
    /// Seconds spent establishing every connection.
    pub connect_s: f64,
}

/// How the schedule's start was set (`--start-file`, [`crate::start`]).
#[derive(Debug, Clone, Serialize)]
pub struct StartReport {
    /// `own` (no start file: as soon as connected), `shared` (the start file's time),
    /// `late` (the start file's time had passed when it was read: started at once) or
    /// `timeout` (the start file named no start within `--start-wait`: started at once).
    pub by: &'static str,
    /// The start the start file named, Unix ms.
    pub requested_unix_ms: Option<u64>,
    /// Seconds waited for the start file.
    pub waited_s: f64,
    /// How late the start file's time was when it was read, ms (`late` only).
    pub late_ms: u64,
}

/// Counts, rates and latencies over a span.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Aggregate {
    /// Seconds covered.
    pub duration_s: f64,
    /// Messages the schedule offered (due) in the span.
    pub offered: u64,
    /// Messages written.
    pub sent: u64,
    /// Positive PUBACKs read.
    pub acked: u64,
    /// Deliveries read (all streams; duplicates included).
    pub received: u64,
    /// `offered / duration_s`.
    pub offered_rate: f64,
    /// `sent / duration_s`.
    pub sent_rate: f64,
    /// `acked / duration_s`.
    pub acked_rate: f64,
    /// `received / duration_s`.
    pub received_rate: f64,
    /// PUBLISH written → PUBACK read.
    pub ack_latency: LatencySummary,
    /// PUBLISH written → delivery read.
    pub e2e_latency: LatencySummary,
    /// The generator's own lateness: due → written, for sends nothing outside the
    /// generator held back.
    pub sched_lag: LatencySummary,
    /// Due → written, every send (includes waiting for credit or the socket).
    pub send_delay: LatencySummary,
    /// The ack-latency histogram's buckets (phases and totals only; absent on windows).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ack_histogram: Option<HistogramExport>,
    /// The end-to-end-latency histogram's buckets (phases and totals only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub e2e_histogram: Option<HistogramExport>,
}

/// One window.
#[derive(Debug, Clone, Serialize)]
pub struct WindowReport {
    /// Position in [`Summary::windows`].
    pub index: usize,
    /// The phase it belongs to (the drain carries the last phase's index).
    pub phase: usize,
    /// `warmup`, `measure` or `drain`.
    pub kind: WindowKind,
    /// Start, seconds after the schedule's start.
    pub start_s: f64,
    /// End, seconds after the schedule's start.
    pub end_s: f64,
    /// The phase's target rate (0 for the drain).
    pub target_rate: f64,
    /// The window's figures.
    #[serde(flatten)]
    pub agg: Aggregate,
    /// Generator health over the window.
    pub health: WindowHealth,
}

/// The kind of window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowKind {
    /// Warm-up: excluded from phase and run totals.
    Warmup,
    /// Measured.
    Measure,
    /// After the schedule ended, until the ledger settled.
    Drain,
}

/// Generator health over a window.
#[derive(Debug, Clone, Default, Serialize)]
pub struct WindowHealth {
    /// Process CPU, cores.
    pub cpu_cores: f64,
    /// Process CPU as a fraction of the runtime's worker threads.
    pub cpu_fraction: f64,
    /// The busiest worker thread, fraction of a core (null where not measured).
    pub busiest_worker: Option<f64>,
    /// Share of publisher-time spent with due messages and no send credit (the
    /// broker holding acks back — not a generator problem).
    pub credit_stall_fraction: f64,
    /// Writes that found the socket buffer full.
    pub socket_blocked: u64,
    /// Share of publisher-time spent waiting on a full socket.
    pub socket_blocked_fraction: f64,
    /// Due-but-unsent messages at the window's end.
    pub backlog: u64,
    /// The generator crossed a saturation threshold in this window.
    pub saturated: bool,
    /// Which thresholds, e.g. `cpu`, `worker_thread`, `sched_lag`.
    pub reasons: Vec<String>,
}

/// One phase (step).
#[derive(Debug, Clone, Serialize)]
pub struct PhaseReport {
    /// Phase index.
    pub index: usize,
    /// Target msg/s.
    pub target_rate: f64,
    /// Aggregated over the phase's measured windows.
    #[serde(flatten)]
    pub agg: Aggregate,
    /// Any of its measured windows saturated the generator.
    pub saturated: bool,
}

/// How the drain ended.
#[derive(Debug, Clone, Serialize)]
pub struct DrainReport {
    /// Seconds from the schedule's end.
    pub duration_s: f64,
    /// `complete` (nothing in flight, every expected delivery seen), `quiet` (nothing
    /// acked or delivered for `quiet_s`; deliveries may still be owed, see
    /// `outstanding_deliveries`) or `timeout` (deliveries were still arriving).
    pub ended_by: &'static str,
    /// The quiet window this drain applied, seconds: `--drain-quiet`, or
    /// `--drain-quiet-factor` times the slowest measured p99 if longer, capped by the
    /// timeout.
    pub quiet_s: f64,
    /// Expected deliveries not yet seen when the drain ended (0 when `complete`).
    pub outstanding_deliveries: u64,
    /// `QoS` 1 publishes still unacknowledged when the drain ended.
    pub outstanding_acks: u64,
    /// Seconds between the last ack or delivery read and the drain's end.
    pub since_progress_s: f64,
}

/// The generator's health over the run.
#[derive(Debug, Clone, Serialize)]
pub struct GeneratorReport {
    /// Any measured window saturated.
    pub saturated: bool,
    /// Indexes of the saturated measured windows.
    pub saturated_windows: Vec<usize>,
    /// The thresholds applied.
    pub thresholds: Thresholds,
    /// Runtime worker threads.
    pub worker_threads: usize,
    /// Whether per-thread CPU was measured (Linux).
    pub per_thread_cpu: bool,
    /// Highest process CPU of any measured window, cores.
    pub cpu_cores_max: f64,
    /// Messages written per CPU-second of the whole generator process (publishers,
    /// consumers and collector together) over the measured windows: msg/s per core,
    /// the figure for sizing generator hosts.
    pub sent_per_cpu_second: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spread_names_the_busiest_consumer_and_its_broker() {
        let specs: Vec<_> = (0..4)
            .map(|i| crate::config::ConsumerSpec {
                stream: 0,
                filter: String::new(),
                broker: i % 2,
            })
            .collect();
        // One member of four takes 1000 of 1200 deliveries.
        let s = Spread::of(&specs, &[100, 1000, 0, 100]).unwrap();
        assert!((s.consumer_skew - 3.33).abs() < 1e-9, "{}", s.consumer_skew);
        assert_eq!(
            (s.busiest_consumer, s.busiest_broker, s.idle_consumers),
            (1, 1, 1)
        );
        assert!(Spread::of(&specs, &[0, 0, 0, 0]).is_none());
        assert!(Spread::of(&[], &[]).is_none());
    }
}
