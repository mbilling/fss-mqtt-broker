//! Prometheus `/metrics` during the run.
//!
//! The collector renders the text once per tick; the server hands out the latest
//! rendering. A scrape never touches the shards, so it cannot perturb the run.

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::ctx::ErrorCounts;
use crate::hist::Latency;
use crate::ledger::LedgerReport;

/// Histogram bucket bounds, seconds.
const BUCKETS_S: [f64; 16] = [
    0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    10.0, 30.0,
];

/// Everything one rendering shows.
#[derive(Debug, Default)]
pub struct View<'a> {
    /// Messages the schedule has offered so far.
    pub offered: u64,
    /// Written.
    pub sent: u64,
    /// Positive PUBACKs.
    pub acked: u64,
    /// Deliveries.
    pub received: u64,
    /// The ledger's live counters (duplicates, unexpected, ...).
    pub ledger: LedgerReport,
    /// Errors.
    pub errors: ErrorCounts,
    /// Messages in flight.
    pub inflight: u64,
    /// The current phase.
    pub phase: usize,
    /// Its target rate.
    pub target_rate: f64,
    /// Process CPU over the last tick, cores.
    pub cpu_cores: f64,
    /// The current window is saturated so far.
    pub saturated: bool,
    /// Cumulative histograms.
    pub ack: Option<&'a Latency>,
    /// Cumulative histograms.
    pub e2e: Option<&'a Latency>,
    /// Cumulative histograms.
    pub sched_lag: Option<&'a Latency>,
}

fn counter(out: &mut String, name: &str, help: &str, v: u64) {
    let _ = writeln!(
        out,
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}"
    );
}

fn gauge(out: &mut String, name: &str, help: &str, v: f64) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}");
}

#[allow(clippy::cast_precision_loss)]
fn histogram(out: &mut String, name: &str, help: &str, h: &Latency) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} histogram");
    for b in BUCKETS_S {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let us = (b * 1e6).round() as u64;
        let _ = writeln!(
            out,
            "{name}_bucket{{le=\"{b}\"}} {}",
            h.count_at_or_below(us)
        );
    }
    let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {}", h.count());
    let _ = writeln!(out, "{name}_sum {}", h.sum_us() / 1e6);
    let _ = writeln!(out, "{name}_count {}", h.count());
}

/// Render `v` in the Prometheus text format.
#[must_use]
#[allow(clippy::too_many_lines)] // one line per series
pub fn render(v: &View<'_>) -> String {
    let mut o = String::with_capacity(4096);
    counter(
        &mut o,
        "mqload_offered_total",
        "Messages the schedule offered (due).",
        v.offered,
    );
    counter(&mut o, "mqload_sent_total", "PUBLISHes written.", v.sent);
    counter(
        &mut o,
        "mqload_acked_total",
        "Positive PUBACKs read.",
        v.acked,
    );
    counter(
        &mut o,
        "mqload_received_total",
        "Deliveries read, all streams.",
        v.received,
    );
    counter(
        &mut o,
        "mqload_duplicates_total",
        "Repeat deliveries of an identity.",
        v.ledger.duplicates,
    );
    counter(
        &mut o,
        "mqload_unexpected_total",
        "Deliveries of identities never sent to that stream.",
        v.ledger.unexpected,
    );
    counter(
        &mut o,
        "mqload_negative_acks_total",
        "PUBACKs with a failure reason code.",
        v.ledger.negative_acks,
    );
    counter(
        &mut o,
        "mqload_unexpected_acks_total",
        "PUBACKs for nothing in flight.",
        v.ledger.unexpected_acks,
    );
    counter(
        &mut o,
        "mqload_pkid_gaps_total",
        "QoS 1 deliveries whose packet id skipped on their connection.",
        v.ledger.pkid_gaps,
    );
    let _ = writeln!(
        o,
        "# HELP mqload_errors_total Connection failures by kind.\n# TYPE mqload_errors_total counter"
    );
    for (k, n) in [
        ("disconnect", v.errors.disconnects),
        ("server_disconnect", v.errors.server_disconnects),
        ("protocol", v.errors.protocol),
        ("write", v.errors.write),
    ] {
        let _ = writeln!(o, "mqload_errors_total{{kind=\"{k}\"}} {n}");
    }
    #[allow(clippy::cast_precision_loss)]
    {
        gauge(
            &mut o,
            "mqload_inflight",
            "QoS 1 messages in flight.",
            v.inflight as f64,
        );
        gauge(
            &mut o,
            "mqload_backlog",
            "Messages due and not yet written.",
            v.offered.saturating_sub(v.sent) as f64,
        );
        gauge(
            &mut o,
            "mqload_phase",
            "Current phase (step) index.",
            v.phase as f64,
        );
    }
    gauge(
        &mut o,
        "mqload_target_rate",
        "Current phase's target msg/s.",
        v.target_rate,
    );
    gauge(
        &mut o,
        "mqload_cpu_cores",
        "Generator process CPU over the last tick, cores.",
        v.cpu_cores,
    );
    gauge(
        &mut o,
        "mqload_saturated",
        "1 if the current window has crossed a generator saturation threshold.",
        if v.saturated { 1.0 } else { 0.0 },
    );
    if let Some(h) = v.ack {
        histogram(
            &mut o,
            "mqload_ack_latency_seconds",
            "PUBLISH written to PUBACK read.",
            h,
        );
    }
    if let Some(h) = v.e2e {
        histogram(
            &mut o,
            "mqload_e2e_latency_seconds",
            "PUBLISH written to delivery read.",
            h,
        );
    }
    if let Some(h) = v.sched_lag {
        histogram(
            &mut o,
            "mqload_sched_lag_seconds",
            "Due to written, sends the generator alone delayed.",
            h,
        );
    }
    o
}

/// Serve the latest rendering at `addr` (any path) until the task is aborted.
///
/// # Errors
/// If the address cannot be bound.
pub async fn serve(
    addr: SocketAddr,
    text: Arc<Mutex<String>>,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                continue;
            };
            let text = text.clone();
            tokio::spawn(async move {
                let mut req = [0u8; 2048];
                let _ = s.read(&mut req).await;
                let body = text.lock().map(|t| t.clone()).unwrap_or_default();
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(body.as_bytes()).await;
                let _ = s.shutdown().await;
            });
        }
    });
    Ok((local, task))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendering_has_counters_and_cumulative_buckets() {
        let mut h = Latency::new();
        h.record_ns(2_000_000); // 2 ms
        h.record_ns(200_000_000); // 200 ms
        let text = render(&View {
            offered: 10,
            sent: 9,
            acked: 8,
            ack: Some(&h),
            ..View::default()
        });
        assert!(text.contains("mqload_offered_total 10\n"));
        assert!(text.contains("mqload_backlog 1\n"));
        assert!(text.contains("mqload_ack_latency_seconds_bucket{le=\"0.001\"} 0\n"));
        assert!(text.contains("mqload_ack_latency_seconds_bucket{le=\"0.0025\"} 1\n"));
        assert!(text.contains("mqload_ack_latency_seconds_bucket{le=\"0.25\"} 2\n"));
        assert!(text.contains("mqload_ack_latency_seconds_count 2\n"));
        assert!(!text.contains("mqload_e2e_latency_seconds"));
    }
}
