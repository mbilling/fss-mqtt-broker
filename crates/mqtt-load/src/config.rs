//! A workload (ADR 0090 D3): what is offered, to whom, and how it is judged.

use std::net::SocketAddr;

use crate::schedule::Phase;

/// How consumers subscribe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Shape {
    /// `$share/<group>/<topic>`: each message to one of `consumers` members, per group.
    Share,
    /// `<topic>`: each message to every one of `consumers` subscribers.
    Fanout,
    /// No subscribers: publish only.
    None,
}

/// Consumer sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Sessions {
    /// `clean_start = false` with a session expiry: the broker keeps the session (and,
    /// with a data dir, persists it).
    Durable,
    /// `clean_start = true`, session expiry 0.
    Clean,
}

/// When the generator itself is the bottleneck (ADR 0090 D2): a window that crosses a
/// threshold is marked saturated, and a run with a saturated measured window is VOID.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct Thresholds {
    /// Process CPU over the window, as a fraction of the runtime's worker threads.
    pub cpu_fraction: f64,
    /// The busiest runtime worker thread's CPU over the window, as a fraction of one
    /// core (Linux only; elsewhere per-thread CPU is not read).
    pub thread_fraction: f64,
    /// p99 of the generator's own scheduling lag (due → written, for sends nothing
    /// outside the generator held back), µs.
    pub sched_lag_p99_us: u64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            cpu_fraction: 0.85,
            thread_fraction: 0.95,
            sched_lag_p99_us: 10_000,
        }
    }
}

/// One consumer connection's place in the subscription shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerSpec {
    /// The ledger stream its deliveries count toward.
    pub stream: usize,
    /// The filter it subscribes to.
    pub filter: String,
}

/// A whole run.
#[derive(Debug, Clone)]
pub struct Config {
    /// Brokers; connections are spread over them round-robin.
    pub brokers: Vec<SocketAddr>,
    /// The offered-rate schedule.
    pub phases: Vec<Phase>,
    /// Measured-window length, ns.
    pub window_ns: u64,
    /// Publisher connections.
    pub publishers: u32,
    /// `QoS` 1 messages in flight per publisher, at most (also capped by the broker's
    /// Receive Maximum).
    pub inflight: u16,
    /// 0 or 1.
    pub qos: u8,
    /// Payload bytes (at least [`crate::payload::HEADER_LEN`]).
    pub payload: usize,
    /// Topics; publisher `i` publishes to topic `i % topics`.
    pub topics: usize,
    /// Topic `t` is `<topic_prefix>/<t>`.
    pub topic_prefix: String,
    /// Subscription shape.
    pub shape: Shape,
    /// Consumers per topic: per group for `share`, in all for `fanout`.
    pub consumers: usize,
    /// `$share` groups per topic.
    pub groups: usize,
    /// Group `g` is `<group_prefix><g>`.
    pub group_prefix: String,
    /// Consumer sessions.
    pub sessions: Sessions,
    /// Session expiry for durable consumers, seconds.
    pub session_expiry: u32,
    /// Leave durable sessions on the broker at the end (default: end them with a
    /// DISCONNECT carrying session expiry 0).
    pub keep_sessions: bool,
    /// Client id prefix; default `mqload-<run id>`.
    pub client_prefix: Option<String>,
    /// Keep-alive, seconds.
    pub keepalive: u16,
    /// Username for every connection.
    pub username: Option<String>,
    /// Password for every connection.
    pub password: Option<String>,
    /// Longest the drain may take after the schedule ends, ns.
    pub drain_timeout_ns: u64,
    /// The drain also ends when nothing was acked or delivered for this long, ns: the
    /// floor of the quiet window.
    pub drain_quiet_ns: u64,
    /// The quiet window grows to this many times the slowest measured window's ack or
    /// end-to-end p99 (capped by the timeout): a step whose deliveries ran 10 s behind
    /// is not over after 5 s of silence. 0 keeps the floor alone.
    pub drain_quiet_factor: f64,
    /// List up to this many missing identities in the summary.
    pub report_missing: usize,
    /// Connections being established at once.
    pub connect_concurrency: usize,
    /// Per-connection CONNECT/SUBSCRIBE timeout, ns.
    pub connect_timeout_ns: u64,
    /// Saturation thresholds.
    pub thresholds: Thresholds,
    /// Serve Prometheus `/metrics` here during the run.
    pub metrics_bind: Option<SocketAddr>,
    /// The runtime's worker threads (the denominator of the CPU fraction).
    pub worker_threads: usize,
    /// Print a line per window to stderr.
    pub progress: bool,
}

impl Config {
    /// A small default workload against `brokers`: 1 phase of `rate` msg/s.
    #[must_use]
    pub fn new(brokers: Vec<SocketAddr>, rate: f64, publishers: u32) -> Self {
        Self {
            brokers,
            phases: vec![Phase {
                rate,
                warmup_ns: 1_000_000_000,
                measure_ns: 5_000_000_000,
            }],
            window_ns: 5_000_000_000,
            publishers,
            inflight: 32,
            qos: 1,
            payload: 128,
            topics: 1,
            topic_prefix: "load".into(),
            shape: Shape::Share,
            consumers: 2,
            groups: 1,
            group_prefix: "g".into(),
            sessions: Sessions::Durable,
            session_expiry: 300,
            keep_sessions: false,
            client_prefix: None,
            keepalive: 60,
            username: None,
            password: None,
            drain_timeout_ns: 30_000_000_000,
            drain_quiet_ns: 5_000_000_000,
            drain_quiet_factor: 2.0,
            report_missing: 32,
            connect_concurrency: 128,
            connect_timeout_ns: 10_000_000_000,
            thresholds: Thresholds::default(),
            metrics_bind: None,
            worker_threads: std::thread::available_parallelism().map_or(1, usize::from),
            progress: false,
        }
    }

    /// Check the workload is runnable.
    ///
    /// # Errors
    /// A description of the first problem found.
    pub fn validate(&self) -> Result<(), String> {
        if self.brokers.is_empty() {
            return Err("no broker endpoints".into());
        }
        if self.phases.is_empty() {
            return Err("no phases".into());
        }
        if self
            .phases
            .iter()
            .any(|p| !p.rate.is_finite() || p.rate < 0.0)
        {
            return Err("rates must be finite and >= 0".into());
        }
        if self.publishers == 0 {
            return Err("at least one publisher".into());
        }
        if self.qos > 1 {
            return Err("QoS must be 0 or 1 (QoS 2 is not implemented yet)".into());
        }
        if self.payload < crate::payload::HEADER_LEN {
            return Err(format!(
                "payload must be at least {} bytes (the identity header)",
                crate::payload::HEADER_LEN
            ));
        }
        if self.topics == 0 {
            return Err("at least one topic".into());
        }
        if (self.publishers as usize) < self.topics {
            return Err(format!(
                "{} publishers cannot cover {} topics (publisher i publishes to topic i % topics)",
                self.publishers, self.topics
            ));
        }
        if self.shape != Shape::None && (self.consumers == 0 || self.groups == 0) {
            return Err("consumers and groups must be at least 1".into());
        }
        if self.inflight == 0 {
            return Err("in-flight window must be at least 1".into());
        }
        if self.window_ns == 0 {
            return Err("window must be > 0".into());
        }
        if !(self.drain_quiet_factor.is_finite() && self.drain_quiet_factor >= 0.0) {
            return Err("drain quiet factor must be finite and >= 0".into());
        }
        Ok(())
    }

    /// The drain's quiet window, ns: the floor, or `drain_quiet_factor` times the
    /// slowest measured p99 (`slowest_p99_us`) if that is longer, never past the timeout.
    #[must_use]
    pub fn drain_quiet_window_ns(&self, slowest_p99_us: u64) -> u64 {
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let scaled = (slowest_p99_us as f64 * 1000.0 * self.drain_quiet_factor) as u64;
        self.drain_quiet_ns
            .max(scaled)
            .min(self.drain_timeout_ns.max(self.drain_quiet_ns))
    }

    /// Topic `t`'s name.
    #[must_use]
    pub fn topic(&self, t: usize) -> String {
        format!("{}/{t}", self.topic_prefix)
    }

    /// The ledger's streams, each by its topic index.
    #[must_use]
    pub fn stream_topics(&self) -> Vec<usize> {
        match self.shape {
            Shape::None => Vec::new(),
            Shape::Share => (0..self.groups).flat_map(|_| 0..self.topics).collect(),
            Shape::Fanout => (0..self.topics)
                .flat_map(|t| std::iter::repeat_n(t, self.consumers))
                .collect(),
        }
    }

    /// Every consumer connection, in stream order.
    #[must_use]
    pub fn consumer_specs(&self) -> Vec<ConsumerSpec> {
        match self.shape {
            Shape::None => Vec::new(),
            Shape::Share => {
                let mut v = Vec::new();
                for g in 0..self.groups {
                    for t in 0..self.topics {
                        let stream = g * self.topics + t;
                        let filter = format!("$share/{}{g}/{}", self.group_prefix, self.topic(t));
                        for _ in 0..self.consumers {
                            v.push(ConsumerSpec {
                                stream,
                                filter: filter.clone(),
                            });
                        }
                    }
                }
                v
            }
            Shape::Fanout => (0..self.topics * self.consumers)
                .map(|stream| ConsumerSpec {
                    stream,
                    filter: self.topic(stream / self.consumers),
                })
                .collect(),
        }
    }

    /// The highest rate of any phase.
    #[must_use]
    pub fn max_rate(&self) -> f64 {
        self.phases.iter().map(|p| p.rate).fold(0.0, f64::max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config::new(vec!["127.0.0.1:1883".parse().unwrap()], 100.0, 6)
    }

    #[test]
    fn the_drain_quiet_window_follows_the_slowest_p99_between_floor_and_timeout() {
        let mut c = cfg();
        c.drain_quiet_ns = 10_000_000_000;
        c.drain_timeout_ns = 120_000_000_000;
        c.drain_quiet_factor = 2.0;
        // Fast steps keep the floor.
        assert_eq!(c.drain_quiet_window_ns(2_000), 10_000_000_000);
        // Deliveries 17.9 s behind: 35.8 s of silence before the drain calls it quiet.
        assert_eq!(c.drain_quiet_window_ns(17_900_000), 35_800_000_000);
        // Never past the timeout.
        assert_eq!(c.drain_quiet_window_ns(90_000_000), 120_000_000_000);
        // Factor 0: the floor alone.
        c.drain_quiet_factor = 0.0;
        assert_eq!(c.drain_quiet_window_ns(17_900_000), 10_000_000_000);
        c.drain_quiet_factor = f64::NAN;
        assert!(c.validate().is_err());
    }

    #[test]
    fn share_shape_is_one_stream_per_group_and_topic() {
        let mut c = cfg();
        c.topics = 3;
        c.groups = 2;
        c.consumers = 2;
        assert_eq!(c.stream_topics(), vec![0, 1, 2, 0, 1, 2]);
        let specs = c.consumer_specs();
        assert_eq!(specs.len(), 12);
        assert_eq!(specs[0].filter, "$share/g0/load/0");
        assert_eq!(specs[1].stream, 0);
        assert_eq!(specs[11].filter, "$share/g1/load/2");
        assert_eq!(specs[11].stream, 5);
        assert!(specs.iter().all(|s| c.stream_topics()[s.stream]
            == s.filter
                .rsplit('/')
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap()));
    }

    #[test]
    fn fanout_shape_is_one_stream_per_subscriber() {
        let mut c = cfg();
        c.shape = Shape::Fanout;
        c.topics = 2;
        c.consumers = 3;
        assert_eq!(c.stream_topics(), vec![0, 0, 0, 1, 1, 1]);
        let specs = c.consumer_specs();
        assert_eq!(specs.len(), 6);
        assert_eq!(specs[4].filter, "load/1");
        assert_eq!(specs[4].stream, 4);
    }

    #[test]
    fn validation_catches_unrunnable_workloads() {
        assert!(cfg().validate().is_ok());
        let mut c = cfg();
        c.topics = 7;
        assert!(c.validate().unwrap_err().contains("cover"));
        let mut c = cfg();
        c.payload = 8;
        assert!(c.validate().is_err());
        let mut c = cfg();
        c.qos = 2;
        assert!(c.validate().is_err());
        let mut c = cfg();
        c.shape = Shape::None;
        c.consumers = 0;
        assert!(c.validate().is_ok());
    }
}
