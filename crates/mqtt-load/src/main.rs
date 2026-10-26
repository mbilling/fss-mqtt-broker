//! `mqtt-load` binary: parse the workload, run it, print the summary.
//!
//! Exit codes: 0 ok, 1 could not start, 2 loss (the ledger did not close), 3 void
//! (the generator saturated in a measured window), 4 connection errors during the run.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use mqtt_load::health::WORKER_THREAD_NAME;
use mqtt_load::{Config, Phase, Sessions, Shape, Thresholds};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Paced open-loop MQTT load generator: msg/s offered, ack and end-to-end latency on
/// one clock, an exact identity ledger, and its own health (ADR 0090).
#[derive(Debug, Parser)]
#[command(name = "mqtt-load", version, about, long_about = None)]
// The doc comments are the --help text, so they stay plain prose.
#[allow(clippy::struct_excessive_bools, clippy::doc_markdown)]
struct Cli {
    /// Broker endpoints, host:port, comma-separated or repeated. Connections are spread
    /// over them round-robin.
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:1883")]
    broker: Vec<String>,

    /// Target total msg/s (one phase). Ignored when --steps is given.
    #[arg(long, default_value_t = 1000.0)]
    rate: f64,

    /// A step schedule: target msg/s per step, comma-separated (e.g. 5000,10000,20000).
    /// Each step gets its own --warmup and --duration; the publisher count is fixed
    /// for the run, so the rate per publisher steps with it.
    #[arg(long, value_delimiter = ',')]
    steps: Vec<f64>,

    /// Measured seconds per phase.
    #[arg(long, default_value_t = 60.0)]
    duration: f64,

    /// Warm-up seconds per phase (offered and sent, not measured).
    #[arg(long, default_value_t = 10.0)]
    warmup: f64,

    /// Measured-window length, seconds (each phase's measured span is cut into these).
    #[arg(long, default_value_t = 10.0)]
    window: f64,

    /// Publisher connections. Default: ceil(highest rate / --rate-per-publisher).
    #[arg(long)]
    publishers: Option<u32>,

    /// msg/s per publisher used to derive --publishers.
    #[arg(long, default_value_t = 10.0)]
    rate_per_publisher: f64,

    /// QoS 1 messages in flight per publisher, at most (also capped by the broker's
    /// Receive Maximum). Sustains rate-per-publisher x ack latency: 32 at 10 msg/s
    /// holds the offer up to 3.2 s of ack latency.
    #[arg(long, default_value_t = 32)]
    inflight: u16,

    /// QoS: 0 or 1.
    #[arg(long, default_value_t = 1)]
    qos: u8,

    /// Payload bytes (at least 32: the identity header).
    #[arg(long, default_value_t = 128)]
    payload: usize,

    /// Topics; publisher i publishes to topic i % topics.
    #[arg(long, default_value_t = 10)]
    topics: usize,

    /// Topic t is <topic-prefix>/<t>.
    #[arg(long, default_value = "load")]
    topic_prefix: String,

    /// Subscription shape.
    #[arg(long, value_enum, default_value_t = Shape::Share)]
    shape: Shape,

    /// Consumers per topic: members per $share group, or fan-out subscribers.
    #[arg(long, default_value_t = 2)]
    consumers: usize,

    /// $share groups per topic.
    #[arg(long, default_value_t = 1)]
    groups: usize,

    /// $share group g is <group-prefix><g>.
    #[arg(long, default_value = "g")]
    group_prefix: String,

    /// Consumer sessions.
    #[arg(long, value_enum, default_value_t = Sessions::Durable)]
    sessions: Sessions,

    /// Session expiry of durable consumers, seconds.
    #[arg(long, default_value_t = 300)]
    session_expiry: u32,

    /// Leave durable sessions on the broker at the end (default: end them).
    #[arg(long)]
    keep_sessions: bool,

    /// Client id prefix. Default: mqload-<run id>.
    #[arg(long)]
    client_prefix: Option<String>,

    /// Keep-alive, seconds.
    #[arg(long, default_value_t = 60)]
    keepalive: u16,

    /// Username for every connection.
    #[arg(long)]
    username: Option<String>,

    /// Password for every connection.
    #[arg(long, env = "MQTT_LOAD_PASSWORD", hide_env_values = true)]
    password: Option<String>,

    /// Longest drain after the schedule ends, seconds.
    #[arg(long, default_value_t = 30.0)]
    drain_timeout: f64,

    /// The drain also ends after this many seconds with nothing acked or delivered
    /// (the floor of the quiet window).
    #[arg(long, default_value_t = 5.0)]
    drain_quiet: f64,

    /// The quiet window grows to this many times the slowest measured window's ack or
    /// end-to-end p99, capped by --drain-timeout; 0 keeps --drain-quiet alone.
    #[arg(long, default_value_t = 2.0)]
    drain_quiet_factor: f64,

    /// List up to this many missing identities (stream, publisher, seq) in the
    /// summary's missing_sample.
    #[arg(long, default_value_t = 32)]
    report_missing: usize,

    /// Connections being established at once.
    #[arg(long, default_value_t = 128)]
    connect_concurrency: usize,

    /// Per-connection handshake timeout, seconds.
    #[arg(long, default_value_t = 10.0)]
    connect_timeout: f64,

    /// Saturation: process CPU above this fraction of the worker threads.
    #[arg(long, default_value_t = Thresholds::default().cpu_fraction)]
    sat_cpu: f64,

    /// Saturation: busiest worker thread above this fraction of a core (Linux).
    #[arg(long, default_value_t = Thresholds::default().thread_fraction)]
    sat_thread: f64,

    /// Saturation: p99 of the generator's own scheduling lag above this, ms.
    #[arg(long, default_value_t = 10.0)]
    sat_lag_ms: f64,

    /// Serve Prometheus metrics here during the run (e.g. 0.0.0.0:9464).
    #[arg(long)]
    metrics: Option<SocketAddr>,

    /// Tokio worker threads. Default: the machine's available parallelism.
    #[arg(long)]
    threads: Option<usize>,

    /// Write the JSON summary here (default: stdout).
    #[arg(long)]
    out: Option<PathBuf>,

    /// No per-window progress lines on stderr.
    #[arg(long)]
    quiet: bool,
}

fn secs_ns(s: f64) -> Result<u64, String> {
    if s.is_finite() && s >= 0.0 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Ok((s * 1e9).round() as u64)
    } else {
        Err(format!("not a duration: {s}"))
    }
}

fn config(cli: Cli, threads: usize) -> Result<(Config, Option<PathBuf>), String> {
    let mut brokers = Vec::new();
    for b in &cli.broker {
        let addr = b
            .to_socket_addrs()
            .map_err(|e| format!("broker {b}: {e}"))?
            .next()
            .ok_or_else(|| format!("broker {b}: no address"))?;
        brokers.push(addr);
    }
    let rates = if cli.steps.is_empty() {
        vec![cli.rate]
    } else {
        cli.steps.clone()
    };
    let (warmup_ns, measure_ns) = (secs_ns(cli.warmup)?, secs_ns(cli.duration)?);
    let phases: Vec<Phase> = rates
        .iter()
        .map(|&rate| Phase {
            rate,
            warmup_ns,
            measure_ns,
        })
        .collect();
    let max_rate = rates.iter().copied().fold(0.0, f64::max);
    let publishers = if let Some(p) = cli.publishers {
        p
    } else {
        if cli.rate_per_publisher.is_nan() || cli.rate_per_publisher <= 0.0 {
            return Err("--rate-per-publisher must be > 0".into());
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let p = (max_rate / cli.rate_per_publisher).ceil().max(1.0) as u32;
        // At least one publisher per topic, so every topic carries traffic.
        p.max(u32::try_from(cli.topics).unwrap_or(u32::MAX))
    };
    let mut c = Config::new(brokers, max_rate, publishers);
    c.phases = phases;
    c.window_ns = secs_ns(cli.window)?;
    c.inflight = cli.inflight;
    c.qos = cli.qos;
    c.payload = cli.payload;
    c.topics = cli.topics;
    c.topic_prefix = cli.topic_prefix;
    c.shape = cli.shape;
    c.consumers = cli.consumers;
    c.groups = cli.groups;
    c.group_prefix = cli.group_prefix;
    c.sessions = cli.sessions;
    c.session_expiry = cli.session_expiry;
    c.keep_sessions = cli.keep_sessions;
    c.client_prefix = cli.client_prefix;
    c.keepalive = cli.keepalive;
    c.username = cli.username;
    c.password = cli.password;
    c.drain_timeout_ns = secs_ns(cli.drain_timeout)?;
    c.drain_quiet_ns = secs_ns(cli.drain_quiet)?;
    c.drain_quiet_factor = cli.drain_quiet_factor;
    c.report_missing = cli.report_missing;
    c.connect_concurrency = cli.connect_concurrency;
    c.connect_timeout_ns = secs_ns(cli.connect_timeout)?;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let lag_us = (cli.sat_lag_ms * 1000.0).round() as u64;
    c.thresholds = Thresholds {
        cpu_fraction: cli.sat_cpu,
        thread_fraction: cli.sat_thread,
        sched_lag_p99_us: lag_us,
    };
    c.metrics_bind = cli.metrics;
    c.worker_threads = threads;
    c.progress = !cli.quiet;
    c.validate()?;
    Ok((c, cli.out))
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let threads = cli
        .threads
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from))
        .max(1);
    let (cfg, out) = match config(cli, threads) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("mqtt-load: {e}");
            return ExitCode::from(1);
        }
    };
    if cfg.progress {
        eprintln!(
            "mqtt-load: {} publishers, {} consumers, {} phase(s), peak {} msg/s, QoS {}, {} B, {:?}/{:?}, {} worker threads",
            cfg.publishers,
            cfg.consumer_specs().len(),
            cfg.phases.len(),
            cfg.max_rate(),
            cfg.qos,
            cfg.payload,
            cfg.shape,
            cfg.sessions,
            threads
        );
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name(WORKER_THREAD_NAME)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let summary = match rt.block_on(mqtt_load::run(cfg)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("mqtt-load: {e}");
            return ExitCode::from(1);
        }
    };
    let json = serde_json::to_string_pretty(&summary).expect("summary serialises");
    match out {
        Some(path) => {
            if let Err(e) = std::fs::write(&path, json + "\n") {
                eprintln!("mqtt-load: writing {}: {e}", path.display());
                return ExitCode::from(1);
            }
        }
        None => println!("{json}"),
    }
    let t = &summary.totals;
    let l = &summary.ledger;
    eprintln!(
        "mqtt-load: {:?}: offered {:.1}/s sent {:.1}/s acked {:.1}/s received {:.1}/s; ack p50/p99/p999 {}/{}/{} us; e2e p50/p99/p999 {}/{}/{} us; ledger sent {} missing {} unexpected {} unacked {} duplicates {} pkid gaps {}; generator max {:.2} cores, {} msg/s per core{}",
        summary.outcome,
        t.offered_rate,
        t.sent_rate,
        t.acked_rate,
        t.received_rate,
        t.ack_latency.p50_us,
        t.ack_latency.p99_us,
        t.ack_latency.p999_us,
        t.e2e_latency.p50_us,
        t.e2e_latency.p99_us,
        t.e2e_latency.p999_us,
        l.sent,
        l.missing,
        l.unexpected,
        l.unacked,
        l.duplicates,
        l.pkid_gaps,
        summary.generator.cpu_cores_max,
        summary.generator.sent_per_cpu_second,
        if summary.generator.saturated { " (SATURATED: run is VOID)" } else { "" },
    );
    ExitCode::from(u8::try_from(summary.outcome.exit_code()).unwrap_or(1))
}
