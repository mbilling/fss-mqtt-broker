//! A workload (ADR 0090 D3): what is offered, to whom, and how it is judged.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::schedule::Phase;

/// How consumers subscribe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Shape {
    /// `$share/<group>/<topic>`: each message to one of `consumers` members, per group.
    Share,
    /// `$share/<group>/<topic-prefix>/#`: one group shares every topic, as a consumer
    /// application takes a whole fleet's data. Each message to one of `consumers`
    /// members, per group.
    #[serde(rename = "share-all")]
    ShareAll,
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
    /// The host's softnet squeezes per second (Linux only; [`crate::health`]).
    pub squeeze_per_s: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            cpu_fraction: 0.85,
            thread_fraction: 0.95,
            sched_lag_p99_us: 10_000,
            squeeze_per_s: 50.0,
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
    /// Index into [`Config::brokers`] it connects to ([`Config::consumer_specs`]).
    pub broker: usize,
}

/// A whole run.
#[derive(Debug, Clone)]
pub struct Config {
    /// Brokers; every topic's publishers and every stream's consumers are spread over
    /// all of them ([`Config::publisher_broker`], [`Config::consumer_specs`]).
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
    /// Payload bytes (at least [`crate::payload::HEADER_LEN`]): every payload's size, or
    /// the smallest when [`payload_max`](Self::payload_max) is larger.
    pub payload: usize,
    /// The largest payload. Each message's size is drawn uniformly from
    /// `payload..=payload_max`; equal to `payload`, every payload has that size.
    pub payload_max: usize,
    /// Topics; publisher `i` publishes to topic `i % topics`. With
    /// [`topics_per_connection`](Self::topics_per_connection), `publishers` times it.
    pub topics: usize,
    /// Every publisher connection carries every topic, its messages dealt round-robin
    /// over them: the shape of a bridge forwarding many topics over one connection. The
    /// ledger then keeps one logical publisher per (connection, topic), with its own
    /// sequence, so each still publishes to exactly one topic.
    pub bridge: bool,
    /// As a bridge, each connection carries this many topics of its own instead of every
    /// topic: connection `c` carries topics `c·k .. c·k + k`, as each site's bridge
    /// carries that site's data. Topic `t` is then `<topic_prefix>/<t / k>/<t % k>`
    /// (site, then tag), and its one publisher is ledger publisher `t`. 0: every
    /// connection carries every topic.
    pub topics_per_connection: usize,
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
    /// The highest [`Config::session_skew`] the workload may have; 0 accepts any.
    pub max_skew: f64,
    /// Serve Prometheus `/metrics` here during the run.
    pub metrics_bind: Option<SocketAddr>,
    /// Written (Unix ms) once every client is connected ([`crate::start`]).
    pub ready_file: Option<PathBuf>,
    /// Waited for once connected: the Unix ms at which the schedule starts
    /// ([`crate::start`]).
    pub start_file: Option<PathBuf>,
    /// The longest wait for `start_file`, ns; then the schedule starts on its own.
    pub start_wait_ns: u64,
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
            payload_max: 128,
            topics: 1,
            bridge: false,
            topics_per_connection: 0,
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
            max_skew: 1.25,
            metrics_bind: None,
            ready_file: None,
            start_file: None,
            start_wait_ns: 30_000_000_000,
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
        if self.payload_max < self.payload {
            return Err(format!(
                "the largest payload ({}) is smaller than the smallest ({})",
                self.payload_max, self.payload
            ));
        }
        if self.topics == 0 {
            return Err("at least one topic".into());
        }
        if self.topics_per_connection > 0 {
            if !self.bridge {
                return Err("topics per connection needs a bridge connection (--bridge)".into());
            }
            if (self.publishers as usize).checked_mul(self.topics_per_connection)
                != Some(self.topics)
            {
                return Err(format!(
                    "{} connections of {} topics each are {} topics, not {}",
                    self.publishers,
                    self.topics_per_connection,
                    (self.publishers as usize).saturating_mul(self.topics_per_connection),
                    self.topics
                ));
            }
            if u32::try_from(self.topics).is_err() {
                return Err("connections x topics per connection must fit in 32 bits".into());
            }
        }
        // With `bridge` every connection covers every topic; without, publisher i
        // publishes to topic i % topics, so it takes a publisher per topic.
        if !self.bridge && (self.publishers as usize) < self.topics {
            return Err(format!(
                "{} publishers cannot cover {} topics (publisher i publishes to topic i % topics)",
                self.publishers, self.topics
            ));
        }
        if self.bridge
            && self.topics_per_connection == 0
            && u32::try_from(self.topics)
                .ok()
                .and_then(|t| self.publishers.checked_mul(t))
                .is_none()
        {
            return Err("publishers x topics must fit in 32 bits".into());
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
        // Publishers do not write while they wait for the start: past the keep-alive
        // the broker would drop them (it allows 1.5x).
        if self.start_file.is_some()
            && self.keepalive > 0
            && self.start_wait_ns > u64::from(self.keepalive) * 1_000_000_000
        {
            return Err(format!(
                "start wait must not exceed the keep-alive ({} s): publishers are silent while they wait",
                self.keepalive
            ));
        }
        self.check_skew()
    }

    /// The skew half of [`Config::validate`]: refuse a workload whose busiest consumer
    /// session would carry more than [`Config::max_skew`] times the mean.
    fn check_skew(&self) -> Result<(), String> {
        if !(self.max_skew.is_finite() && self.max_skew >= 0.0) {
            return Err("max skew must be finite and >= 0".into());
        }
        let skew = self.session_skew();
        if self.max_skew > 0.0 && skew > self.max_skew {
            // A connection of a bridge, or of any publisher under one group that shares
            // every topic, loads the members on its broker whatever its topic: a whole
            // number of connections per broker is then the even deal.
            let per_broker = self.bridge || self.shape == Shape::ShareAll;
            return Err(format!(
                "the busiest consumer session would carry {skew:.2}x the mean session's load \
                 (limit {:.2}): {} publishers over {} topics and {} brokers do not divide evenly \
                 — use a multiple of {} ({}) or raise --max-skew",
                self.max_skew,
                self.publishers,
                self.topics,
                self.brokers.len(),
                if per_broker {
                    "brokers"
                } else {
                    "topics x brokers"
                },
                if per_broker {
                    self.brokers.len()
                } else {
                    self.topics * self.brokers.len()
                }
            ));
        }
        Ok(())
    }

    /// The topics connection `conn` publishes to: its own block with
    /// [`topics_per_connection`](Self::topics_per_connection), every topic as a bridge,
    /// and otherwise topic `conn % topics`.
    #[must_use]
    pub fn connection_topics(&self, conn: u32) -> std::ops::Range<usize> {
        let c = conn as usize;
        if self.topics_per_connection > 0 {
            let k = self.topics_per_connection;
            c * k..c * k + k
        } else if self.bridge {
            0..self.topics
        } else {
            let t = c % self.topics.max(1);
            t..t + 1
        }
    }

    /// Deliveries each published message should produce: one per group with `$share`,
    /// one per subscriber with fan-out.
    #[must_use]
    pub fn deliveries_per_message(&self) -> usize {
        match self.shape {
            Shape::None => 0,
            Shape::Share | Shape::ShareAll => self.groups,
            Shape::Fanout => self.consumers,
        }
    }

    /// The broker publisher `i` connects to, as an index into [`Config::brokers`].
    /// Publisher `i` publishes to topic `i % topics`; its broker steps once per round of
    /// topics, so every topic's publishers are spread over every broker. (Dealing
    /// connections `i % brokers` instead puts all of a topic's publishers on ONE broker
    /// whenever the broker count divides the topic count, and a locality-preferring
    /// `$share` then hands that topic to the one member on that broker.)
    #[must_use]
    pub fn publisher_broker(&self, i: usize) -> usize {
        (i % self.topics + i / self.topics) % self.brokers.len()
    }

    /// The busiest consumer session's expected load over the mean session's, for a
    /// broker that hands a `$share` publish to a member on the publisher's own broker
    /// when there is one (prefer-local; otherwise to any member). 1.0 is an even
    /// spread; [`Config::validate`] refuses a workload above [`Config::max_skew`]. The
    /// load counts publishers, which all publish at the same rate.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn session_skew(&self) -> f64 {
        let brokers = self.brokers.len();
        if brokers == 0 || self.topics == 0 {
            return 1.0;
        }
        // Publishers per topic and broker. A bridge connection carries each of its topics
        // at a share of its rate; counting it once on each scales every load of the
        // shape alike, which leaves the ratio this returns as it is.
        let mut pubs = vec![vec![0usize; brokers]; self.topics];
        for i in 0..self.publishers {
            let b = self.publisher_broker(i as usize);
            for t in self.connection_topics(i) {
                pubs[t][b] += 1;
            }
        }
        let specs = self.consumer_specs();
        let mut loads: Vec<f64> = Vec::with_capacity(specs.len());
        match self.shape {
            Shape::None => return 1.0,
            Shape::ShareAll => {
                // Every connection loads the members on its broker, whatever its topics,
                // and every connection publishes at the same rate.
                let mut on = vec![0usize; brokers];
                for i in 0..self.publishers as usize {
                    on[self.publisher_broker(i)] += 1;
                }
                let mut members = vec![vec![0usize; brokers]; self.groups];
                for spec in &specs {
                    members[spec.stream][spec.broker] += 1;
                }
                for spec in &specs {
                    let here = &members[spec.stream];
                    let remote: usize = (0..brokers).filter(|&b| here[b] == 0).map(|b| on[b]).sum();
                    loads.push(
                        on[spec.broker] as f64 / here[spec.broker] as f64
                            + remote as f64 / self.consumers as f64,
                    );
                }
            }
            Shape::Fanout => {
                for spec in &specs {
                    let t = spec.stream / self.consumers;
                    loads.push(pubs[t].iter().sum::<usize>() as f64);
                }
            }
            Shape::Share => {
                let topics = self.stream_topics();
                let mut members = vec![vec![0usize; brokers]; topics.len()];
                for spec in &specs {
                    members[spec.stream][spec.broker] += 1;
                }
                for spec in &specs {
                    let Some(t) = topics[spec.stream] else {
                        continue;
                    };
                    let on = &pubs[t];
                    let here = &members[spec.stream];
                    // Publishers on a broker with no member of the group go to any member.
                    let remote: usize = (0..brokers).filter(|&b| here[b] == 0).map(|b| on[b]).sum();
                    loads.push(
                        on[spec.broker] as f64 / here[spec.broker] as f64
                            + remote as f64 / self.consumers as f64,
                    );
                }
            }
        }
        let mean = loads.iter().sum::<f64>() / loads.len().max(1) as f64;
        if mean > 0.0 {
            loads.iter().copied().fold(0.0, f64::max) / mean
        } else {
            1.0
        }
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

    /// Topic `t`'s name: `<topic_prefix>/<t>`, or `<topic_prefix>/<site>/<tag>` with
    /// [`topics_per_connection`](Self::topics_per_connection).
    #[must_use]
    pub fn topic(&self, t: usize) -> String {
        if self.topics_per_connection > 0 {
            let k = self.topics_per_connection;
            format!("{}/{}/{}", self.topic_prefix, t / k, t % k)
        } else {
            format!("{}/{t}", self.topic_prefix)
        }
    }

    /// The ledger's publishers: one per connection, or one per (connection, topic)
    /// when [`bridge`](Self::bridge) puts every topic on every connection, or one per
    /// topic when each connection has [`topics_per_connection`](Self::topics_per_connection)
    /// of its own.
    #[must_use]
    pub fn logical_publishers(&self) -> u32 {
        if self.topics_per_connection > 0 {
            u32::try_from(self.topics).unwrap_or(u32::MAX)
        } else if self.bridge {
            self.publishers
                .saturating_mul(u32::try_from(self.topics).unwrap_or(u32::MAX))
        } else {
            self.publishers
        }
    }

    /// The ledger publisher of connection `conn`'s messages to topic `t`. Its index
    /// modulo `topics` is `t`, so the ledger's topic of a publisher holds either way.
    #[must_use]
    pub fn logical(&self, conn: u32, t: usize) -> u32 {
        if self.topics_per_connection > 0 {
            u32::try_from(t).unwrap_or(u32::MAX)
        } else if self.bridge {
            conn * u32::try_from(self.topics).unwrap_or(u32::MAX) + u32::try_from(t).unwrap_or(0)
        } else {
            conn
        }
    }

    /// The ledger's streams, each by the topic it receives, or `None` for a group that
    /// shares every topic ([`Shape::ShareAll`]).
    #[must_use]
    pub fn stream_topics(&self) -> Vec<Option<usize>> {
        match self.shape {
            Shape::None => Vec::new(),
            Shape::Share => (0..self.groups)
                .flat_map(|_| (0..self.topics).map(Some))
                .collect(),
            Shape::ShareAll => vec![None; self.groups],
            Shape::Fanout => (0..self.topics)
                .flat_map(|t| std::iter::repeat_n(Some(t), self.consumers))
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
                        // Member j of a stream on broker (stream + j): with as many
                        // members as brokers, every broker holds one member of every
                        // group.
                        for j in 0..self.consumers {
                            v.push(ConsumerSpec {
                                stream,
                                filter: filter.clone(),
                                broker: (stream + j) % self.brokers.len(),
                            });
                        }
                    }
                }
                v
            }
            Shape::ShareAll => {
                let mut v = Vec::with_capacity(self.groups * self.consumers);
                for g in 0..self.groups {
                    let filter = format!("$share/{}{g}/{}/#", self.group_prefix, self.topic_prefix);
                    // Member j of group g on broker (g + j), as for a group per topic.
                    for j in 0..self.consumers {
                        v.push(ConsumerSpec {
                            stream: g,
                            filter: filter.clone(),
                            broker: (g + j) % self.brokers.len(),
                        });
                    }
                }
                v
            }
            Shape::Fanout => (0..self.topics * self.consumers)
                .map(|stream| ConsumerSpec {
                    stream,
                    filter: self.topic(stream / self.consumers),
                    broker: (stream / self.consumers + stream % self.consumers)
                        % self.brokers.len(),
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
    fn a_bridge_connection_carries_every_topic_as_one_logical_publisher_each() {
        let mut c = cfg();
        c.publishers = 2;
        c.topics = 3;
        assert!(
            c.validate().is_err(),
            "without a bridge, two publishers cannot cover three topics"
        );
        c.bridge = true;
        c.validate()
            .expect("a bridge connection covers every topic");
        assert_eq!(c.logical_publishers(), 6);
        // Connection 1's topic-2 messages are ledger publisher 5, whose topic is 5 % 3.
        assert_eq!(c.logical(1, 2), 5);
        assert!((0..2).all(|conn| (0..3).all(|t| c.logical(conn, t) as usize % 3 == t)));
        c.bridge = false;
        c.publishers = 6;
        assert_eq!(c.logical_publishers(), 6);
        assert_eq!(
            c.logical(4, 1),
            4,
            "without a bridge a connection is its publisher"
        );
    }

    #[test]
    fn share_shape_is_one_stream_per_group_and_topic() {
        let mut c = cfg();
        c.topics = 3;
        c.groups = 2;
        c.consumers = 2;
        assert_eq!(c.stream_topics(), [0, 1, 2, 0, 1, 2].map(Some).to_vec());
        let specs = c.consumer_specs();
        assert_eq!(specs.len(), 12);
        assert_eq!(specs[0].filter, "$share/g0/load/0");
        assert_eq!(specs[1].stream, 0);
        assert_eq!(specs[11].filter, "$share/g1/load/2");
        assert_eq!(specs[11].stream, 5);
        assert!(specs.iter().all(|s| c.stream_topics()[s.stream]
            == s.filter.rsplit('/').next().unwrap().parse::<usize>().ok()));
        assert_eq!(c.deliveries_per_message(), 2);
    }

    #[test]
    fn share_all_is_one_stream_per_group_over_every_topic() {
        let mut c = cfg();
        c.shape = Shape::ShareAll;
        c.topics = 5;
        c.groups = 2;
        c.consumers = 3;
        c.validate()
            .expect("a group sharing every topic is runnable");
        assert_eq!(c.stream_topics(), vec![None, None]);
        let specs = c.consumer_specs();
        assert_eq!(specs.len(), 6, "3 members in each of 2 groups");
        assert!(specs[..3]
            .iter()
            .all(|s| s.stream == 0 && s.filter == "$share/g0/load/#"));
        assert!(specs[3..]
            .iter()
            .all(|s| s.stream == 1 && s.filter == "$share/g1/load/#"));
        assert_eq!(c.deliveries_per_message(), 2, "once per group");
    }

    #[test]
    fn a_site_bridge_carries_a_block_of_topics_of_its_own() {
        let mut c = cfg();
        c.publishers = 3;
        c.bridge = true;
        c.topics_per_connection = 4;
        c.topics = 12;
        c.validate().expect("3 sites of 4 topics");
        assert_eq!(c.connection_topics(1), 4..8);
        assert_eq!(c.topic(5), "load/1/1", "site 1, tag 1");
        assert_eq!(c.logical_publishers(), 12, "one ledger publisher per topic");
        // Every topic has exactly one connection, and its ledger publisher is the topic.
        let mut owner = [None; 12];
        for conn in 0..3 {
            for t in c.connection_topics(conn) {
                assert_eq!(owner[t], None, "topic {t} has two connections");
                owner[t] = Some(conn);
                assert_eq!(c.logical(conn, t) as usize, t);
            }
        }
        assert!(owner.iter().all(Option::is_some));
        c.topics = 10;
        assert!(c.validate().unwrap_err().contains("not 10"));
        c.topics = 12;
        c.bridge = false;
        assert!(c.validate().unwrap_err().contains("--bridge"));
    }

    #[test]
    fn a_group_sharing_every_topic_is_even_with_whole_connections_per_broker() {
        // Three brokers, one member of the group on each, a site bridge per connection.
        let site = |publishers: u32| {
            let mut c = cluster(3, publishers);
            c.shape = Shape::ShareAll;
            c.consumers = 3;
            c.bridge = true;
            c.topics_per_connection = 250;
            c.topics = publishers as usize * 250;
            c
        };
        assert!((site(6).session_skew() - 1.0).abs() < 1e-9);
        // Seven sites: one broker carries three, the others two.
        let c = site(7);
        assert!(c.session_skew() > 1.25, "{}", c.session_skew());
        assert!(c
            .validate()
            .unwrap_err()
            .contains("multiple of brokers (3)"));
    }

    #[test]
    fn fanout_shape_is_one_stream_per_subscriber() {
        let mut c = cfg();
        c.shape = Shape::Fanout;
        c.topics = 2;
        c.consumers = 3;
        assert_eq!(c.stream_topics(), [0, 0, 0, 1, 1, 1].map(Some).to_vec());
        let specs = c.consumer_specs();
        assert_eq!(specs.len(), 6);
        assert_eq!(specs[4].filter, "load/1");
        assert_eq!(specs[4].stream, 4);
        assert_eq!(c.deliveries_per_message(), 3);
    }

    /// `brokers` local endpoints, 20 topics, one `$share` group with a member on every
    /// broker: the 10-node rig2 shape.
    fn cluster(brokers: u16, publishers: u32) -> Config {
        let eps = (0..brokers)
            .map(|b| format!("10.99.1.{}:1883", 11 + b).parse().unwrap())
            .collect();
        let mut c = Config::new(eps, 50.0 * f64::from(publishers), publishers);
        c.topics = 20;
        c.consumers = usize::from(brokers);
        c
    }

    #[test]
    fn every_topic_has_publishers_on_every_broker() {
        // 1014 publishers over 20 topics and 10 brokers: the 152k msg/s step of the
        // 10-node run that put every publisher of a topic on one broker (dealt
        // i % brokers, with 10 dividing 20).
        for (brokers, publishers) in [(10, 1014), (10, 1200), (3, 1014), (4, 333), (1, 40)] {
            let c = cluster(brokers, publishers);
            let mut on = vec![vec![0u32; usize::from(brokers)]; c.topics];
            for i in 0..publishers as usize {
                on[i % c.topics][c.publisher_broker(i)] += 1;
            }
            for (t, per_broker) in on.iter().enumerate() {
                let (lo, hi) = (
                    per_broker.iter().min().unwrap(),
                    per_broker.iter().max().unwrap(),
                );
                assert!(
                    hi - lo <= 1,
                    "{brokers} brokers, {publishers} publishers: topic {t} spread {per_broker:?}"
                );
            }
        }
    }

    #[test]
    fn every_group_has_a_member_on_every_broker() {
        let mut c = cluster(10, 1200);
        c.groups = 2;
        let specs = c.consumer_specs();
        for stream in 0..c.stream_topics().len() {
            let mut brokers: Vec<usize> = specs
                .iter()
                .filter(|s| s.stream == stream)
                .map(|s| s.broker)
                .collect();
            brokers.sort_unstable();
            assert_eq!(brokers, (0..10).collect::<Vec<_>>(), "stream {stream}");
        }
    }

    #[test]
    fn session_skew_is_one_for_a_whole_number_of_publishers_per_topic_and_broker() {
        assert!((cluster(10, 1200).session_skew() - 1.0).abs() < 1e-9);
        assert!((cluster(3, 1020).session_skew() - 1.0).abs() < 1e-9);
        // One publisher short of whole: 5 or 6 per member, under the default limit.
        let c = cluster(10, 1014);
        assert!(
            c.session_skew() > 1.0 && c.session_skew() < 1.25,
            "{}",
            c.session_skew()
        );
        assert!(c.validate().is_ok());
    }

    #[test]
    fn a_hot_session_is_refused_unless_asked_for() {
        // 30 publishers over 20 topics: topics 0-9 get two, topics 10-19 one, so the
        // members of the first ten groups carry twice the others' load.
        let mut c = cluster(10, 30);
        assert!(c.session_skew() > 1.25, "{}", c.session_skew());
        let e = c.validate().unwrap_err();
        assert!(e.contains("busiest consumer session"), "{e}");
        assert!(e.contains("multiple of topics x brokers (200)"), "{e}");
        // --max-skew 0: measure a hot session on purpose.
        c.max_skew = 0.0;
        assert!(c.validate().is_ok());
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
        c.payload = 200;
        c.payload_max = 50;
        assert!(c
            .validate()
            .unwrap_err()
            .contains("smaller than the smallest"));
        let mut c = cfg();
        c.qos = 2;
        assert!(c.validate().is_err());
        let mut c = cfg();
        c.shape = Shape::None;
        c.consumers = 0;
        assert!(c.validate().is_ok());
    }
}
