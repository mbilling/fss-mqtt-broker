//! Bridge observability and audit
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §8, T8–T9).
//!
//! Rendered as Prometheus text (the same format the broker exposes, ADR 0020), so the
//! crossing is observable without coupling to the broker's metric set. Every forward is
//! also written to the **audit** log (`bridge::audit` target) recording what crossed and
//! in which direction (§8) — the record a security boundary must keep.
//!
//! # What this exports, and why it grew
//!
//! The first version exported three unlabelled counters: forwarded (by direction only),
//! dropped-at-hop-limit, and reconnects. That left an operator unable to answer the
//! questions a bridge actually raises:
//!
//! - **Is it connected right now?** `reconnects_total` says it reconnected at some point,
//!   not whether the link is up. The engine already tracked a per-side `AtomicBool`; it
//!   was simply never published.
//! - **How much is buffered?** Store-and-forward is the bridge's durability story and its
//!   depth was invisible, though `Spool::len()` existed all along.
//! - **Which boundary?** `forwarded()` accepted an `upstream` argument and passed it only
//!   to the audit log, so every upstream collapsed into two process-wide counters — with
//!   two upstreams you could not tell which one was moving traffic, and `Upstream::name`
//!   was documented as being "used in logs, metrics, audit" while reaching only two of them.
//! - **Is it silently shedding?** The spool drops the oldest message at capacity and
//!   returned `Ok(())`; nothing counted it. See [`crate::spool::Spool::dropped_count`].
//!
//! # Rates are the reader's job, not this module's
//!
//! There are deliberately no 1/5/15-minute counters here. Windowed rates belong in the
//! query layer — `rate(fss_bridge_forwarded_total[5m])` — because Prometheus computes them
//! correctly across scrapes, restarts (counter resets) and multiple bridge replicas, none
//! of which an in-process window can do. Monotonic counters are the right primitive to
//! export; the dashboard in `demo/grafana/dashboards/mqttd-bridge.json` supplies the
//! windows.
//!
//! # Cardinality
//!
//! Label values are bounded by configuration, never by traffic: upstream names come from
//! the config file and are registered once at construction. A forward naming an upstream
//! that was never registered is counted under `unknown` rather than creating a new series,
//! so a bug upstream of here cannot turn into unbounded cardinality in the TSDB.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use tracing::info;

use crate::spool::Spool;

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

/// The counters kept per upstream, so traffic is attributable to a boundary.
#[derive(Debug, Default)]
struct UpstreamCounters {
    forwarded_out: AtomicU64,
    forwarded_in: AtomicU64,
    bytes_out: AtomicU64,
    bytes_in: AtomicU64,
}

/// Live per-side state the engine owns and this module only reads.
///
/// Registered once, after the engine has built its spools and connection flags. Held as
/// `Arc` clones of the very objects the engine uses, so a gauge cannot drift from the
/// state it describes — there is no second copy to keep in sync.
struct SideHandles {
    names: Vec<String>,
    connected: Vec<Arc<AtomicBool>>,
    spools: Vec<Arc<Spool>>,
}

/// Bridge metrics. Cheap atomics; share one `Arc<BridgeMetrics>` across the engine tasks.
#[derive(Debug)]
pub struct BridgeMetrics {
    /// Registered upstream names, in config order. `per_upstream` is parallel to this.
    upstream_names: Vec<String>,
    per_upstream: Vec<UpstreamCounters>,
    /// Forwards naming an unregistered upstream (a bug, but bounded rather than unbounded).
    unknown: UpstreamCounters,
    dropped_hop_limit: AtomicU64,
    /// Reconnects per side, parallel to `SideHandles::names` once registered.
    reconnects: Vec<AtomicU64>,
    sides: OnceLock<SideHandles>,
}

impl std::fmt::Debug for SideHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SideHandles")
            .field("names", &self.names)
            .finish_non_exhaustive()
    }
}

impl Default for BridgeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl BridgeMetrics {
    /// A metrics set with no upstreams registered (tests, and the no-upstream config).
    #[must_use]
    pub fn new() -> Self {
        Self::for_upstreams(&[])
    }

    /// A metrics set whose label values are fixed by `upstream_names`.
    #[must_use]
    pub fn for_upstreams(upstream_names: &[String]) -> Self {
        // One reconnect counter per side: the local side plus one per upstream.
        let reconnects = (0..=upstream_names.len())
            .map(|_| AtomicU64::new(0))
            .collect();
        Self {
            upstream_names: upstream_names.to_vec(),
            per_upstream: upstream_names
                .iter()
                .map(|_| UpstreamCounters::default())
                .collect(),
            unknown: UpstreamCounters::default(),
            dropped_hop_limit: AtomicU64::new(0),
            reconnects,
            sides: OnceLock::new(),
        }
    }

    /// Publish the engine's live per-side state so the gauges can read it.
    ///
    /// Called once by the engine with clones of the flags and spools it uses itself.
    /// A second call is ignored rather than panicking — a metrics surface must never be
    /// the thing that takes the process down.
    pub fn register_sides(
        &self,
        names: Vec<String>,
        connected: Vec<Arc<AtomicBool>>,
        spools: Vec<Arc<Spool>>,
    ) {
        let _ = self.sides.set(SideHandles {
            names,
            connected,
            spools,
        });
    }

    fn counters_for(&self, upstream: &str) -> &UpstreamCounters {
        self.upstream_names
            .iter()
            .position(|n| n == upstream)
            .and_then(|i| self.per_upstream.get(i))
            .unwrap_or(&self.unknown)
    }

    /// Record (and audit) one forwarded message: `upstream` is the external broker involved,
    /// `dir` the direction, `src`/`dst` the source and (remapped) destination topics, and
    /// `bytes` the payload size.
    pub fn forwarded(
        &self,
        upstream: &str,
        dir: CrossDirection,
        src: &str,
        dst: &str,
        bytes: usize,
    ) {
        let c = self.counters_for(upstream);
        let (count, byte_total) = match dir {
            CrossDirection::Out => (&c.forwarded_out, &c.bytes_out),
            CrossDirection::In => (&c.forwarded_in, &c.bytes_in),
        };
        count.fetch_add(1, Ordering::Relaxed);
        byte_total.fetch_add(bytes as u64, Ordering::Relaxed);
        // The audit record (§8): what crossed, in which direction, across which boundary.
        info!(
            target: "bridge::audit",
            upstream,
            direction = dir.as_str(),
            src,
            dst,
            bytes,
            "forwarded across the boundary"
        );
    }

    /// Record one message dropped because it reached the hop-count limit (a bounded loop, §6).
    ///
    /// Audited with the topic and hop value (#191): the `fss-bridge-hop-count` property is
    /// publisher-settable, so a drop here can be a genuine loop **or** a publisher stamping the
    /// limit to make its own messages vanish at the crossing. Either way it must be visible and
    /// attributable, not a bare counter — an operator watching this rise can see *what* is being
    /// dropped rather than only *that* something is.
    pub fn dropped_hop_limit(&self, topic: &str, hop: u32) {
        self.dropped_hop_limit.fetch_add(1, Ordering::Relaxed);
        info!(
            target: "bridge::audit",
            topic,
            hop,
            "dropped at the hop-count limit"
        );
    }

    /// Record one (re)connection of the side at `side_index` (0 = local).
    pub fn reconnect(&self, side_index: usize) {
        if let Some(c) = self.reconnects.get(side_index) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Messages forwarded local→upstream, across every upstream.
    #[must_use]
    pub fn forwarded_out_count(&self) -> u64 {
        self.per_upstream
            .iter()
            .chain(std::iter::once(&self.unknown))
            .map(|c| c.forwarded_out.load(Ordering::Relaxed))
            .sum()
    }

    /// Messages forwarded upstream→local, across every upstream.
    #[must_use]
    pub fn forwarded_in_count(&self) -> u64 {
        self.per_upstream
            .iter()
            .chain(std::iter::once(&self.unknown))
            .map(|c| c.forwarded_in.load(Ordering::Relaxed))
            .sum()
    }

    /// Messages dropped at the hop limit.
    #[must_use]
    pub fn dropped_hop_limit_count(&self) -> u64 {
        self.dropped_hop_limit.load(Ordering::Relaxed)
    }

    /// Side (re)connections, summed across sides.
    #[must_use]
    pub fn reconnect_count(&self) -> u64 {
        self.reconnects
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }

    /// Render the metrics as Prometheus exposition text (ADR 0020 format).
    #[must_use]
    #[allow(clippy::too_many_lines)] // one straight-line block per metric family; splitting it hides the shape
    pub fn render(&self) -> String {
        let mut out = String::new();
        // A label value could in principle carry a quote or backslash from a config file;
        // escape rather than emit exposition text a scraper would reject.
        let esc = |s: &str| s.replace('\\', r"\\").replace('"', "\\\"");

        let named: Vec<(&str, &UpstreamCounters)> = self
            .upstream_names
            .iter()
            .map(String::as_str)
            .zip(self.per_upstream.iter())
            .chain(std::iter::once(("unknown", &self.unknown)))
            .collect();

        out.push_str("# HELP fss_bridge_forwarded_total Messages forwarded across the boundary.\n");
        out.push_str("# TYPE fss_bridge_forwarded_total counter\n");
        for (name, c) in &named {
            for (dir, v) in [
                ("out", c.forwarded_out.load(Ordering::Relaxed)),
                ("in", c.forwarded_in.load(Ordering::Relaxed)),
            ] {
                let _ = writeln!(
                    out,
                    "fss_bridge_forwarded_total{{upstream=\"{}\",direction=\"{dir}\"}} {v}",
                    esc(name)
                );
            }
        }

        out.push_str(
            "# HELP fss_bridge_forwarded_bytes_total Payload bytes forwarded across the boundary.\n",
        );
        out.push_str("# TYPE fss_bridge_forwarded_bytes_total counter\n");
        for (name, c) in &named {
            for (dir, v) in [
                ("out", c.bytes_out.load(Ordering::Relaxed)),
                ("in", c.bytes_in.load(Ordering::Relaxed)),
            ] {
                let _ = writeln!(
                    out,
                    "fss_bridge_forwarded_bytes_total{{upstream=\"{}\",direction=\"{dir}\"}} {v}",
                    esc(name)
                );
            }
        }

        out.push_str("# HELP fss_bridge_dropped_total Messages dropped before forwarding.\n");
        out.push_str("# TYPE fss_bridge_dropped_total counter\n");
        let _ = writeln!(
            out,
            "fss_bridge_dropped_total{{reason=\"hop-limit\"}} {}",
            self.dropped_hop_limit_count()
        );
        // Spool shedding, per side. Zero is the normal reading; anything else means
        // store-and-forward hit its bound and threw the oldest queued messages away.
        if let Some(sides) = self.sides.get() {
            for (i, spool) in sides.spools.iter().enumerate() {
                let name = sides.names.get(i).map_or("unknown", String::as_str);
                let _ = writeln!(
                    out,
                    "fss_bridge_dropped_total{{reason=\"spool-full\",side=\"{}\"}} {}",
                    esc(name),
                    spool.dropped_count()
                );
            }
        }

        out.push_str("# HELP fss_bridge_reconnects_total Side (re)connections.\n");
        out.push_str("# TYPE fss_bridge_reconnects_total counter\n");
        if let Some(sides) = self.sides.get() {
            for (i, c) in self.reconnects.iter().enumerate() {
                let name = sides.names.get(i).map_or("unknown", String::as_str);
                let _ = writeln!(
                    out,
                    "fss_bridge_reconnects_total{{side=\"{}\"}} {}",
                    esc(name),
                    c.load(Ordering::Relaxed)
                );
            }
        } else {
            let _ = writeln!(
                out,
                "fss_bridge_reconnects_total {}",
                self.reconnect_count()
            );
        }

        // The two gauges the engine already had and never published.
        if let Some(sides) = self.sides.get() {
            out.push_str(
                "# HELP fss_bridge_connected Whether this side's MQTT connection is up (1) or down (0).\n",
            );
            out.push_str("# TYPE fss_bridge_connected gauge\n");
            for (i, flag) in sides.connected.iter().enumerate() {
                let name = sides.names.get(i).map_or("unknown", String::as_str);
                let _ = writeln!(
                    out,
                    "fss_bridge_connected{{side=\"{}\"}} {}",
                    esc(name),
                    u8::from(flag.load(Ordering::SeqCst))
                );
            }

            out.push_str(
                "# HELP fss_bridge_spool_depth Messages currently buffered for this side.\n",
            );
            out.push_str("# TYPE fss_bridge_spool_depth gauge\n");
            out.push_str(
                "# HELP fss_bridge_spool_capacity Configured spool bound for this side, in messages.\n",
            );
            out.push_str("# TYPE fss_bridge_spool_capacity gauge\n");
            for (i, spool) in sides.spools.iter().enumerate() {
                let name = sides.names.get(i).map_or("unknown", String::as_str);
                // A disk-backed spool can fail to report; skip the sample rather than
                // publish a zero that would read as "nothing buffered".
                if let Ok(depth) = spool.len() {
                    let _ = writeln!(
                        out,
                        "fss_bridge_spool_depth{{side=\"{}\"}} {depth}",
                        esc(name)
                    );
                }
                let _ = writeln!(
                    out,
                    "fss_bridge_spool_capacity{{side=\"{}\"}} {}",
                    esc(name),
                    spool.capacity()
                );
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::{Spool, SpooledMessage};

    fn msg(topic: &str) -> SpooledMessage {
        SpooledMessage {
            topic: topic.to_string(),
            payload: vec![1, 2, 3],
            qos: 0,
            retain: false,
            user_properties: Vec::new(),
        }
    }

    #[test]
    fn counters_are_attributed_to_the_upstream_that_carried_them() {
        // The defect this replaces: `forwarded` took an `upstream` argument and sent it
        // only to the audit log, so two upstreams shared one pair of counters and neither
        // could be told apart.
        let names = vec!["partner-a".to_string(), "partner-b".to_string()];
        let m = BridgeMetrics::for_upstreams(&names);
        m.forwarded("partner-a", CrossDirection::Out, "s", "d", 10);
        m.forwarded("partner-a", CrossDirection::Out, "s", "d", 10);
        m.forwarded("partner-b", CrossDirection::In, "s", "d", 7);

        let text = m.render();
        assert!(
            text.contains(r#"fss_bridge_forwarded_total{upstream="partner-a",direction="out"} 2"#)
        );
        assert!(
            text.contains(r#"fss_bridge_forwarded_total{upstream="partner-b",direction="in"} 1"#)
        );
        // and the same split for bytes
        assert!(text.contains(
            r#"fss_bridge_forwarded_bytes_total{upstream="partner-a",direction="out"} 20"#
        ));
        assert!(text.contains(
            r#"fss_bridge_forwarded_bytes_total{upstream="partner-b",direction="in"} 7"#
        ));
    }

    #[test]
    fn an_unregistered_upstream_cannot_create_unbounded_series() {
        // Label values must be bounded by config, never by traffic: a name that was never
        // registered is folded into `unknown` instead of minting a new series per value.
        let m = BridgeMetrics::for_upstreams(&["known".to_string()]);
        for i in 0..50 {
            m.forwarded(&format!("attacker-{i}"), CrossDirection::In, "s", "d", 1);
        }
        let text = m.render();
        assert!(
            text.contains(r#"fss_bridge_forwarded_total{upstream="unknown",direction="in"} 50"#)
        );
        assert!(!text.contains("attacker-0"));
    }

    #[test]
    fn connection_state_and_spool_depth_are_published() {
        // Both of these already existed inside the engine and were never exported, which is
        // why an operator could not answer "is it up?" or "how much is queued?".
        let m = BridgeMetrics::new();
        let up = Arc::new(AtomicBool::new(true));
        let down = Arc::new(AtomicBool::new(false));
        let spool_a = Arc::new(Spool::in_memory(100));
        let spool_b = Arc::new(Spool::in_memory(100));
        spool_b.push(&msg("a/b")).unwrap();
        spool_b.push(&msg("a/c")).unwrap();
        m.register_sides(
            vec!["local".to_string(), "partner".to_string()],
            vec![up, down],
            vec![spool_a, spool_b],
        );

        let text = m.render();
        assert!(text.contains(r#"fss_bridge_connected{side="local"} 1"#));
        assert!(text.contains(r#"fss_bridge_connected{side="partner"} 0"#));
        assert!(text.contains(r#"fss_bridge_spool_depth{side="local"} 0"#));
        assert!(text.contains(r#"fss_bridge_spool_depth{side="partner"} 2"#));
        assert!(text.contains(r#"fss_bridge_spool_capacity{side="partner"} 100"#));
    }

    #[test]
    fn a_spool_shedding_at_its_bound_is_counted_not_silent() {
        // THE regression this exists for: the spool drops the oldest message at capacity
        // and returns Ok(()). Before this, that happened with no counter, no log and no
        // error — store-and-forward could be losing messages with nothing to show for it.
        let m = BridgeMetrics::new();
        let spool = Arc::new(Spool::in_memory(3));
        for i in 0..10 {
            spool.push(&msg(&format!("t/{i}"))).unwrap();
        }
        m.register_sides(
            vec!["partner".to_string()],
            vec![Arc::new(AtomicBool::new(true))],
            vec![spool],
        );

        let text = m.render();
        // 10 pushed into a spool bounded at 3 → 7 discarded.
        assert!(
            text.contains(r#"fss_bridge_dropped_total{reason="spool-full",side="partner"} 7"#),
            "spool shedding must be visible; got:\n{text}"
        );
        assert!(text.contains(r#"fss_bridge_spool_depth{side="partner"} 3"#));
    }

    #[test]
    fn reconnects_are_per_side() {
        let m = BridgeMetrics::for_upstreams(&["partner".to_string()]);
        m.reconnect(0);
        m.reconnect(1);
        m.reconnect(1);
        m.register_sides(
            vec!["local".to_string(), "partner".to_string()],
            vec![
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(true)),
            ],
            vec![
                Arc::new(Spool::in_memory(10)),
                Arc::new(Spool::in_memory(10)),
            ],
        );
        let text = m.render();
        assert!(text.contains(r#"fss_bridge_reconnects_total{side="local"} 1"#));
        assert!(text.contains(r#"fss_bridge_reconnects_total{side="partner"} 2"#));
    }

    #[test]
    fn a_label_value_cannot_break_the_exposition_format() {
        let m = BridgeMetrics::for_upstreams(&[r#"we"ird"#.to_string()]);
        m.forwarded(r#"we"ird"#, CrossDirection::Out, "s", "d", 1);
        assert!(m.render().contains(r#"upstream="we\"ird""#));
    }
}
