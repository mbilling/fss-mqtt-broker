//! Bridge observability and audit
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §8, T8–T9).
//!
//! A small, self-contained metrics surface for the bridge process: how many messages were
//! forwarded each way, how many were dropped at the hop limit, and how many times each side
//! reconnected. Rendered as Prometheus text (the same format the broker exposes, ADR 0020),
//! so the crossing is observable without coupling to the broker's metric set. Every forward
//! is also written to the **audit** log (`bridge::audit` target) recording what crossed and
//! in which direction (§8) — the record a security boundary must keep.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::info;

/// The direction a message crossed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossDirection {
    /// Local cluster → upstream.
    Out,
    /// Upstream → local cluster.
    In,
}

impl CrossDirection {
    fn as_str(self) -> &'static str {
        match self {
            CrossDirection::Out => "out",
            CrossDirection::In => "in",
        }
    }
}

/// Bridge counters. Cheap atomics; share one `Arc<BridgeMetrics>` across the engine tasks.
#[derive(Debug, Default)]
pub struct BridgeMetrics {
    forwarded_out: AtomicU64,
    forwarded_in: AtomicU64,
    dropped_hop_limit: AtomicU64,
    reconnects: AtomicU64,
}

impl BridgeMetrics {
    /// A fresh, zeroed metrics set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record (and audit) one forwarded message: `upstream` is the external broker involved,
    /// `dir` the direction, `src`/`dst` the source and (remapped) destination topics.
    pub fn forwarded(&self, upstream: &str, dir: CrossDirection, src: &str, dst: &str) {
        match dir {
            CrossDirection::Out => &self.forwarded_out,
            CrossDirection::In => &self.forwarded_in,
        }
        .fetch_add(1, Ordering::Relaxed);
        // The audit record (§8): what crossed, in which direction, across which boundary.
        info!(
            target: "bridge::audit",
            upstream,
            direction = dir.as_str(),
            src,
            dst,
            "forwarded across the boundary"
        );
    }

    /// Record one message dropped because it reached the hop-count limit (a bounded loop, §6).
    pub fn dropped_hop_limit(&self) {
        self.dropped_hop_limit.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one (re)connection of a side.
    pub fn reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    /// Messages forwarded local→upstream.
    #[must_use]
    pub fn forwarded_out_count(&self) -> u64 {
        self.forwarded_out.load(Ordering::Relaxed)
    }
    /// Messages forwarded upstream→local.
    #[must_use]
    pub fn forwarded_in_count(&self) -> u64 {
        self.forwarded_in.load(Ordering::Relaxed)
    }
    /// Messages dropped at the hop limit.
    #[must_use]
    pub fn dropped_hop_limit_count(&self) -> u64 {
        self.dropped_hop_limit.load(Ordering::Relaxed)
    }
    /// Total (re)connections across all sides.
    #[must_use]
    pub fn reconnect_count(&self) -> u64 {
        self.reconnects.load(Ordering::Relaxed)
    }

    /// Render the counters as Prometheus exposition text (ADR 0020 format).
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP fss_bridge_forwarded_total Messages forwarded across the boundary.\n");
        out.push_str("# TYPE fss_bridge_forwarded_total counter\n");
        let _ = writeln!(
            out,
            "fss_bridge_forwarded_total{{direction=\"out\"}} {}",
            self.forwarded_out_count()
        );
        let _ = writeln!(
            out,
            "fss_bridge_forwarded_total{{direction=\"in\"}} {}",
            self.forwarded_in_count()
        );
        out.push_str("# HELP fss_bridge_dropped_total Messages dropped before forwarding.\n");
        out.push_str("# TYPE fss_bridge_dropped_total counter\n");
        let _ = writeln!(
            out,
            "fss_bridge_dropped_total{{reason=\"hop-limit\"}} {}",
            self.dropped_hop_limit_count()
        );
        out.push_str("# HELP fss_bridge_reconnects_total Side (re)connections.\n");
        out.push_str("# TYPE fss_bridge_reconnects_total counter\n");
        let _ = writeln!(
            out,
            "fss_bridge_reconnects_total {}",
            self.reconnect_count()
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment_and_render() {
        let m = BridgeMetrics::new();
        m.forwarded(
            "partner",
            CrossDirection::Out,
            "telemetry/x",
            "org/telemetry/x",
        );
        m.forwarded(
            "partner",
            CrossDirection::Out,
            "telemetry/y",
            "org/telemetry/y",
        );
        m.forwarded("partner", CrossDirection::In, "commands/z", "commands/z");
        m.dropped_hop_limit();
        m.reconnect();
        m.reconnect();

        assert_eq!(m.forwarded_out_count(), 2);
        assert_eq!(m.forwarded_in_count(), 1);
        assert_eq!(m.dropped_hop_limit_count(), 1);
        assert_eq!(m.reconnect_count(), 2);

        let text = m.render();
        assert!(text.contains("fss_bridge_forwarded_total{direction=\"out\"} 2"));
        assert!(text.contains("fss_bridge_forwarded_total{direction=\"in\"} 1"));
        assert!(text.contains("fss_bridge_dropped_total{reason=\"hop-limit\"} 1"));
        assert!(text.contains("fss_bridge_reconnects_total 2"));
    }
}
