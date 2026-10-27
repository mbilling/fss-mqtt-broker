//! One run: connect, pace, collect windows, drain, reconcile, report.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mqtt_codec::QoS;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;

use crate::client::{self, ConnectOpts};
use crate::clock::{self, now_ns, sleep_until_ns};
use crate::config::{Config, Sessions, Shape};
use crate::ctx::{Ctx, Errors};
use crate::health::{utilisation, CpuSample, CpuSampler};
use crate::ledger::Ledger;
use crate::metrics::{self, View};
use crate::publisher::{self, PubShared};
use crate::schedule::Schedule;
use crate::start;
use crate::stats::{Counters, Histos, Stats};
use crate::subscriber;
use crate::summary::{
    Aggregate, Connections, DrainReport, GeneratorReport, Outcome, PhaseReport, PhaseSpec, Spread,
    StartReport, Summary, Tool, WindowHealth, WindowKind, WindowReport, Workload, SCHEMA,
};

/// A run that could not start.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The workload is not runnable.
    #[error("invalid workload: {0}")]
    Config(String),
    /// A connection could not be established.
    #[error("connection {who} to {addr} failed: {err}")]
    Connect {
        /// Which connection.
        who: String,
        /// Where to.
        addr: std::net::SocketAddr,
        /// Why.
        err: client::ClientError,
    },
    /// The metrics endpoint could not be bound.
    #[error("metrics endpoint: {0}")]
    Metrics(std::io::Error),
    /// The ready file could not be written.
    #[error("ready file: {0}")]
    Ready(std::io::Error),
}

const TICK_NS: u64 = 1_000_000_000;
const NS: f64 = 1e9;

fn run_id() -> u32 {
    // Not cryptographic: distinct across runs is all it needs to be.
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    #[allow(clippy::cast_possible_truncation)]
    let mut x = (t as u64) ^ (u64::from(std::process::id()) << 32);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    #[allow(clippy::cast_possible_truncation)]
    let id = x as u32;
    id
}

struct Pub {
    shared: Arc<PubShared>,
    writer: JoinHandle<tokio::net::tcp::OwnedWriteHalf>,
    reader: JoinHandle<()>,
}

struct PlannedWindow {
    phase: usize,
    kind: WindowKind,
    start: u64,
    end: u64,
}

fn plan(cfg: &Config, sched: &Schedule) -> Vec<PlannedWindow> {
    let mut v = Vec::new();
    for (j, p) in cfg.phases.iter().enumerate() {
        let s = sched.phase_start_ns(j);
        if p.warmup_ns > 0 {
            v.push(PlannedWindow {
                phase: j,
                kind: WindowKind::Warmup,
                start: s,
                end: s + p.warmup_ns,
            });
        }
        let mut at = s + p.warmup_ns;
        let end = s + p.len_ns();
        while at < end {
            let e = (at + cfg.window_ns).min(end);
            v.push(PlannedWindow {
                phase: j,
                kind: WindowKind::Measure,
                start: at,
                end: e,
            });
            at = e;
        }
    }
    v
}

#[allow(clippy::cast_precision_loss)]
fn aggregate(dur_ns: u64, offered: u64, c: &Counters, h: &Histos, buckets: bool) -> Aggregate {
    let secs = dur_ns as f64 / NS;
    let rate = |n: u64| {
        if secs > 0.0 {
            (n as f64 / secs * 10.0).round() / 10.0
        } else {
            0.0
        }
    };
    Aggregate {
        duration_s: secs,
        offered,
        sent: c.sent,
        acked: c.acked,
        received: c.received,
        offered_rate: rate(offered),
        sent_rate: rate(c.sent),
        acked_rate: rate(c.acked),
        received_rate: rate(c.received),
        ack_latency: h.ack.summary(),
        e2e_latency: h.e2e.summary(),
        sched_lag: h.sched_lag.summary(),
        send_delay: h.send_delay.summary(),
        ack_histogram: buckets.then(|| h.ack.export()),
        e2e_histogram: buckets.then(|| h.e2e.export()),
    }
}

/// The collector's state between ticks.
struct Collector {
    ctx: Arc<Ctx>,
    sched: Schedule,
    pubs: Vec<Arc<PubShared>>,
    sampler: CpuSampler,
    tick: Histos,
    all: Histos,
    text: Arc<Mutex<String>>,
    last_cpu: Option<(u64, CpuSample)>,
    last_cores: f64,
}

impl Collector {
    fn inflight(&self) -> u64 {
        self.pubs.iter().map(|p| p.outstanding() as u64).sum()
    }

    /// Drain the shards into `cur` (and the run-wide histograms), sample CPU, and
    /// refresh the metrics text.
    fn tick(&mut self, cur: &mut Histos, phase: usize, target: f64, saturated: bool) -> Counters {
        let c = self.ctx.stats.drain(&mut self.tick);
        cur.add(&self.tick);
        self.all.add(&self.tick);
        self.tick = Histos::default();
        let now = now_ns();
        if let Some(s) = self.sampler.sample() {
            if let Some((t, prev)) = &self.last_cpu {
                self.last_cores = utilisation(prev, &s, now - t, false).cores;
            }
            self.last_cpu = Some((now, s));
        }
        let view = View {
            offered: self.sched.offered_total_by(now),
            sent: c.sent,
            acked: c.acked,
            received: c.received,
            ledger: live_ledger(&self.ctx.ledger),
            errors: self.ctx.errors.counts(),
            inflight: self.inflight(),
            phase,
            target_rate: target,
            cpu_cores: self.last_cores,
            saturated,
            ack: Some(&self.all.ack),
            e2e: Some(&self.all.e2e),
            sched_lag: Some(&self.all.sched_lag),
        };
        if let Ok(mut t) = self.text.lock() {
            *t = metrics::render(&view);
        }
        c
    }
}

/// The ledger's live counters (no bitset scan: cheap enough to run every tick).
fn live_ledger(l: &Ledger) -> crate::ledger::LedgerReport {
    crate::ledger::LedgerReport {
        duplicates: l
            .streams
            .iter()
            .map(|s| s.duplicates.load(Ordering::Relaxed))
            .sum(),
        unexpected: l
            .streams
            .iter()
            .map(|s| s.unexpected.load(Ordering::Relaxed))
            .sum(),
        negative_acks: l.negative_acks.load(Ordering::Relaxed),
        unexpected_acks: l.unexpected_acks.load(Ordering::Relaxed),
        pkid_gaps: l.pkid_gaps.load(Ordering::Relaxed),
        ..Default::default()
    }
}

/// Judge a window's generator health.
#[allow(clippy::cast_precision_loss)]
fn health(
    cfg: &Config,
    dur_ns: u64,
    c: &Counters,
    h: &Histos,
    cpu: Option<crate::health::CpuUse>,
    backlog: u64,
) -> WindowHealth {
    let pub_ns = (u64::from(cfg.publishers) * dur_ns).max(1) as f64;
    let cores = cpu.map_or(0.0, |u| u.cores);
    let cpu_fraction = cores / cfg.worker_threads.max(1) as f64;
    let busiest = cpu.and_then(|u| u.busiest_worker);
    let mut reasons = Vec::new();
    if cpu_fraction > cfg.thresholds.cpu_fraction {
        reasons.push("cpu".to_string());
    }
    if busiest.is_some_and(|b| b > cfg.thresholds.thread_fraction) {
        reasons.push("worker_thread".to_string());
    }
    if h.sched_lag.count() > 0 && h.sched_lag.quantile_us(0.99) > cfg.thresholds.sched_lag_p99_us {
        reasons.push("sched_lag".to_string());
    }
    let squeeze = cpu.and_then(|u| u.squeeze_per_s);
    if squeeze.is_some_and(|q| q > cfg.thresholds.squeeze_per_s) {
        reasons.push("softnet".to_string());
    }
    let round3 = |x: f64| (x * 1000.0).round() / 1000.0;
    WindowHealth {
        cpu_cores: round3(cores),
        cpu_fraction: round3(cpu_fraction),
        busiest_worker: busiest.map(round3),
        softnet_squeeze_per_s: squeeze.map(f64::round),
        credit_stall_fraction: round3(c.credit_stall_ns as f64 / pub_ns),
        socket_blocked: c.socket_blocked,
        socket_blocked_fraction: round3(c.socket_blocked_ns as f64 / pub_ns),
        backlog,
        saturated: !reasons.is_empty(),
        reasons,
    }
}

async fn connect_with_limit<T, F, Fut>(n: usize, limit: usize, f: F) -> Vec<Result<T, RunError>>
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = Result<T, RunError>> + Send + 'static,
    T: Send + 'static,
{
    let sem = Arc::new(Semaphore::new(limit.max(1)));
    let mut tasks = Vec::with_capacity(n);
    for i in 0..n {
        let permit = sem.clone().acquire_owned().await.expect("semaphore open");
        let fut = f(i);
        tasks.push(tokio::spawn(async move {
            let r = fut.await;
            drop(permit);
            r
        }));
    }
    let mut out = Vec::with_capacity(n);
    for t in tasks {
        out.push(t.await.expect("connect task panicked"));
    }
    out
}

/// Run `cfg` to completion.
///
/// # Errors
/// If the workload is invalid or a connection cannot be established before the
/// schedule starts. Failures during the run are reported in the summary instead.
///
/// # Panics
/// If a connection task panicked.
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub async fn run(cfg: Config) -> Result<Summary, RunError> {
    clock::init();
    cfg.validate().map_err(RunError::Config)?;
    let run = run_id();
    let started_ns = now_ns();
    let started_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0);
    let prefix = cfg
        .client_prefix
        .clone()
        .unwrap_or_else(|| format!("mqload-{run:08x}"));
    let counting = Schedule::new(&cfg.phases, cfg.publishers, 0);
    let capacity: Vec<u64> = (0..cfg.publishers).map(|p| counting.total_for(p)).collect();
    let stream_topics = cfg.stream_topics();
    let streams_per_topic = stream_topics.len().checked_div(cfg.topics).unwrap_or(0);
    let shards = (cfg.worker_threads * 4).max(4);
    let ctx = Arc::new(Ctx {
        ledger: Ledger::new(&capacity, cfg.topics, &stream_topics),
        stats: Stats::new(shards),
        filler: (0..cfg.payload)
            .map(|i| b'a' + u8::try_from(i % 26).unwrap_or(0))
            .collect(),
        topic_sent: (0..cfg.topics).map(|_| AtomicU64::new(0)).collect(),
        consumer_received: (0..cfg.consumer_specs().len())
            .map(|_| AtomicU64::new(0))
            .collect(),
        last_progress_ns: AtomicU64::new(0),
        errors: Errors::default(),
        stopping: AtomicBool::new(false),
        run,
        cfg,
    });
    let cfg = &ctx.cfg;
    let text = Arc::new(Mutex::new(String::new()));
    let metrics_task = match cfg.metrics_bind {
        Some(addr) => {
            let (local, task) = metrics::serve(addr, text.clone())
                .await
                .map_err(RunError::Metrics)?;
            if cfg.progress {
                eprintln!("mqtt-load: metrics on http://{local}/metrics");
            }
            Some(task)
        }
        None => None,
    };
    let timeout = Duration::from_nanos(cfg.connect_timeout_ns);
    let connect_start = now_ns();

    // Consumers first: every subscription exists before the first PUBLISH.
    let (stop_tx, stop_rx) = watch::channel(false);
    let specs = cfg.consumer_specs();
    let durable = cfg.sessions == Sessions::Durable;
    let qos = QoS::from_u8(cfg.qos).expect("validated");
    let consumers = connect_with_limit(specs.len(), cfg.connect_concurrency, |i| {
        let ctx = ctx.clone();
        let spec = specs[i].clone();
        let addr = ctx.cfg.brokers[spec.broker];
        let id = format!("{prefix}-c{i}");
        let stop = stop_rx.clone();
        async move {
            let c = &ctx.cfg;
            let opts = ConnectOpts {
                client_id: &id,
                clean: !durable,
                session_expiry: if durable { c.session_expiry } else { 0 },
                keepalive: c.keepalive,
                receive_maximum: u16::MAX,
                username: c.username.as_deref(),
                password: c.password.as_deref(),
            };
            let fail = |err| RunError::Connect {
                who: id.clone(),
                addr,
                err,
            };
            let mut conn = client::connect(addr, &opts, timeout).await.map_err(fail)?;
            let early = client::subscribe(&mut conn, &spec.filter, qos, timeout)
                .await
                .map_err(fail)?;
            let end_session = durable && !c.keep_sessions;
            let shard = i;
            Ok(tokio::spawn(subscriber::consumer(
                ctx.clone(),
                conn.stream,
                conn.buf,
                early,
                spec.stream,
                shard,
                end_session,
                stop,
            )))
        }
    })
    .await;
    let mut consumer_tasks = Vec::new();
    let mut first_err = None;
    for c in consumers {
        match c {
            Ok(t) => consumer_tasks.push(t),
            Err(e) => {
                first_err.get_or_insert(e);
            }
        }
    }
    if let Some(e) = first_err {
        ctx.stopping.store(true, Ordering::Relaxed);
        let _ = stop_tx.send(true);
        return Err(e);
    }

    // Publishers.
    let conns = connect_with_limit(cfg.publishers as usize, cfg.connect_concurrency, |i| {
        let ctx = ctx.clone();
        let addr = ctx.cfg.brokers[ctx.cfg.publisher_broker(i)];
        let id = format!("{prefix}-p{i}");
        async move {
            let c = &ctx.cfg;
            let opts = ConnectOpts {
                client_id: &id,
                clean: true,
                session_expiry: 0,
                keepalive: c.keepalive,
                receive_maximum: u16::MAX,
                username: c.username.as_deref(),
                password: c.password.as_deref(),
            };
            client::connect(addr, &opts, timeout)
                .await
                .map_err(|err| RunError::Connect { who: id, addr, err })
        }
    })
    .await;
    let mut pub_conns = Vec::with_capacity(conns.len());
    for c in conns {
        match c {
            Ok(c) => pub_conns.push(c),
            Err(e) => {
                ctx.stopping.store(true, Ordering::Relaxed);
                let _ = stop_tx.send(true);
                return Err(e);
            }
        }
    }
    let connect_s = (now_ns() - connect_start) as f64 / NS;
    let receive_maximum_min = pub_conns
        .iter()
        .map(|c| c.granted.receive_maximum)
        .min()
        .unwrap_or(u16::MAX);
    let window = cfg.inflight.min(receive_maximum_min).max(1);

    // Start the schedule a moment from now, so every writer starts on time; or, with a
    // start file, at the time it names (several generators sharing one start).
    if let Some(p) = &cfg.ready_file {
        if let Err(e) = start::write_atomic(p, &format!("{}\n", start::unix_ms_now())) {
            ctx.stopping.store(true, Ordering::Relaxed);
            let _ = stop_tx.send(true);
            return Err(RunError::Ready(e));
        }
    }
    let (start, start_report) = match &cfg.start_file {
        None => (
            now_ns() + start::OWN_LEAD_NS,
            StartReport {
                by: "own",
                requested_unix_ms: None,
                waited_s: 0.0,
                late_ms: 0,
            },
        ),
        Some(p) => {
            let (at, late_ms, w) = start::await_start(p, cfg.start_wait_ns).await;
            let by = if w.timed_out {
                "timeout"
            } else if late_ms > 0 {
                "late"
            } else {
                "shared"
            };
            if by != "shared" {
                eprintln!(
                    "mqtt-load: start {by}: {}",
                    if w.timed_out {
                        format!(
                            "no start in {} after {:.1} s",
                            p.display(),
                            w.waited_ns as f64 / NS
                        )
                    } else {
                        format!(
                            "the start named in {} had passed {late_ms} ms before",
                            p.display()
                        )
                    }
                );
            }
            (
                at,
                StartReport {
                    by,
                    requested_unix_ms: w.requested_unix_ms,
                    waited_s: w.waited_ns as f64 / NS,
                    late_ms,
                },
            )
        }
    };
    let sched = Schedule::new(&cfg.phases, cfg.publishers, start);
    let mut pubs = Vec::with_capacity(pub_conns.len());
    for (i, c) in pub_conns.into_iter().enumerate() {
        let id = u32::try_from(i).expect("publisher count fits u32");
        let w = c.granted.receive_maximum.min(cfg.inflight).max(1);
        let state = Arc::new(PubShared::new(w));
        let (r, wr) = c.stream.into_split();
        let slot = specs.len() + i;
        let writer = tokio::spawn(publisher::writer(
            ctx.clone(),
            id,
            state.clone(),
            wr,
            sched.cursor(id),
            slot,
        ));
        let reader = tokio::spawn(publisher::reader(
            ctx.clone(),
            id,
            state.clone(),
            r,
            c.buf,
            slot,
        ));
        pubs.push(Pub {
            shared: state,
            writer,
            reader,
        });
    }

    let mut col = Collector {
        ctx: ctx.clone(),
        sched: sched.clone(),
        pubs: pubs.iter().map(|p| p.shared.clone()).collect(),
        sampler: CpuSampler::new(),
        tick: Histos::default(),
        all: Histos::default(),
        text: text.clone(),
        last_cpu: None,
        last_cores: 0.0,
    };
    let per_thread = col.sampler.per_thread();
    let num_phases = cfg.phases.len();
    let mut phase_hist: Vec<Histos> = (0..num_phases).map(|_| Histos::default()).collect();
    let mut phase_counts: Vec<(u64, u64, Counters)> = (0..num_phases)
        .map(|_| (0, 0, Counters::default()))
        .collect();
    let mut phase_saturated = vec![false; num_phases];
    let mut windows = Vec::new();
    let mut prev = Counters::default();
    let mut cpu_prev = col.sampler.sample().map(|s| (now_ns(), s));
    let mut cpu_cores_max: f64 = 0.0;
    let mut measured_cpu_ns = 0u64;
    let mut measured_sent = 0u64;

    sleep_until_ns(start).await;
    let planned = plan(cfg, &sched);
    let mut finish_window = |col: &mut Collector,
                             pw: &PlannedWindow,
                             cur: &Histos,
                             c: Counters,
                             windows: &mut Vec<WindowReport>| {
        let delta = c.since(&prev);
        prev = c;
        let offered = sched.offered_total_by(pw.end - 1) - sched.offered_total_by(pw.start - 1);
        let now = now_ns();
        let cpu_now = col.sampler.sample().map(|s| (now, s));
        let cpu = match (&cpu_prev, &cpu_now) {
            (Some((t0, a)), Some((t1, b))) => Some((
                utilisation(a, b, t1 - t0, per_thread),
                b.process_ns - a.process_ns,
            )),
            _ => None,
        };
        cpu_prev = cpu_now;
        let backlog = sched.offered_total_by(pw.end - 1).saturating_sub(c.sent);
        let dur = pw.end - pw.start;
        let health = health(&col.ctx.cfg, dur, &delta, cur, cpu.map(|(u, _)| u), backlog);
        let target = if pw.kind == WindowKind::Drain {
            0.0
        } else {
            col.ctx.cfg.phases[pw.phase].rate
        };
        if pw.kind == WindowKind::Measure {
            phase_hist[pw.phase].add(cur);
            let pc = &mut phase_counts[pw.phase];
            pc.0 += dur;
            pc.1 += offered;
            pc.2 = Counters {
                sent: pc.2.sent + delta.sent,
                acked: pc.2.acked + delta.acked,
                received: pc.2.received + delta.received,
                credit_stall_ns: pc.2.credit_stall_ns + delta.credit_stall_ns,
                socket_blocked: pc.2.socket_blocked + delta.socket_blocked,
                socket_blocked_ns: pc.2.socket_blocked_ns + delta.socket_blocked_ns,
            };
            phase_saturated[pw.phase] |= health.saturated;
            cpu_cores_max = cpu_cores_max.max(health.cpu_cores);
            if let Some((_, ns)) = cpu {
                measured_cpu_ns += ns;
            }
            measured_sent += delta.sent;
        }
        let rel = |t: u64| (t.saturating_sub(start)) as f64 / NS;
        let w = WindowReport {
            index: windows.len(),
            phase: pw.phase,
            kind: pw.kind,
            start_s: rel(pw.start),
            end_s: rel(pw.end),
            target_rate: target,
            agg: aggregate(dur, offered, &delta, cur, false),
            health,
        };
        if col.ctx.cfg.progress {
            eprintln!(
                "mqtt-load: [{:>3}] {:<7} {:>7.1}-{:>7.1}s target {:>9.0} offered {:>9.1}/s sent {:>9.1}/s acked {:>9.1}/s recv {:>9.1}/s ack p99 {:>7}us e2e p99 {:>7}us lag p99 {:>6}us cpu {:.2}{}",
                w.index,
                format!("{:?}", w.kind).to_lowercase(),
                w.start_s,
                w.end_s,
                w.target_rate,
                w.agg.offered_rate,
                w.agg.sent_rate,
                w.agg.acked_rate,
                w.agg.received_rate,
                w.agg.ack_latency.p99_us,
                w.agg.e2e_latency.p99_us,
                w.agg.sched_lag.p99_us,
                w.health.cpu_cores,
                if w.health.saturated { format!(" SATURATED {:?}", w.health.reasons) } else { String::new() },
            );
        }
        windows.push(w);
    };

    for pw in &planned {
        let mut cur = Histos::default();
        let target = cfg.phases[pw.phase].rate;
        let c = loop {
            let next = (now_ns() + TICK_NS).min(pw.end);
            sleep_until_ns(next).await;
            let c = col.tick(&mut cur, pw.phase, target, false);
            if now_ns() >= pw.end {
                break c;
            }
        };
        finish_window(&mut col, pw, &cur, c, &mut windows);
    }

    // Drain: until nothing is in flight and every expected delivery has arrived, or
    // nothing moved for the quiet window, or the timeout. The quiet window scales with
    // the slowest measured p99: after a step whose deliveries ran seconds behind, a few
    // seconds of silence do not mean the brokers hold nothing more for us.
    let slowest_p99_us = windows
        .iter()
        .filter(|w| w.kind == WindowKind::Measure)
        .map(|w| w.agg.ack_latency.p99_us.max(w.agg.e2e_latency.p99_us))
        .max()
        .unwrap_or(0);
    let quiet_ns = cfg.drain_quiet_window_ns(slowest_p99_us);
    let drain_start = sched.end_ns();
    let mut cur = Histos::default();
    let mut last_tick = now_ns();
    let ended_by = loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let now = now_ns();
        let inflight_done = col
            .pubs
            .iter()
            .all(|p| p.outstanding() == 0 || p.is_closed());
        let writers_done = pubs
            .iter()
            .all(|p| p.shared.schedule_done() || p.writer.is_finished());
        let deliveries_done = ctx.unique_deliveries() >= ctx.expected_deliveries();
        if writers_done && inflight_done && deliveries_done {
            break "complete";
        }
        let last = ctx
            .last_progress_ns
            .load(Ordering::Relaxed)
            .max(drain_start);
        if writers_done && now.saturating_sub(last) >= quiet_ns {
            break "quiet";
        }
        if now.saturating_sub(drain_start) >= cfg.drain_timeout_ns {
            break "timeout";
        }
        if now - last_tick >= TICK_NS {
            last_tick = now;
            col.tick(&mut cur, num_phases - 1, 0.0, false);
        }
    };
    let drain_end = now_ns();
    let outstanding_deliveries = ctx
        .expected_deliveries()
        .saturating_sub(ctx.unique_deliveries());
    let outstanding_acks: u64 = col
        .pubs
        .iter()
        .filter(|p| !p.is_closed())
        .map(|p| p.outstanding() as u64)
        .sum();
    let since_progress_ns = drain_end.saturating_sub(
        ctx.last_progress_ns
            .load(Ordering::Relaxed)
            .max(drain_start),
    );
    let c = col.tick(&mut cur, num_phases - 1, 0.0, false);
    let drain_pw = PlannedWindow {
        phase: num_phases - 1,
        kind: WindowKind::Drain,
        start: drain_start,
        end: drain_end.max(drain_start + 1),
    };
    finish_window(&mut col, &drain_pw, &cur, c, &mut windows);

    // Reconcile before closing anything, so the end of the run cannot change it.
    let ledger = ctx.ledger.report(cfg.qos == 1);
    let missing_sample = if ledger.missing > 0 {
        ctx.ledger.missing_sample(cfg.report_missing)
    } else {
        Vec::new()
    };
    ctx.stopping.store(true, Ordering::Relaxed);
    let _ = stop_tx.send(true);
    // Writers ping through the drain; stop them all, then take each write half back
    // for its DISCONNECT, under one deadline (a writer stuck on a full socket is
    // aborted at it).
    for p in &pubs {
        p.shared.stop();
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    for mut p in pubs {
        match tokio::time::timeout_at(deadline, &mut p.writer).await {
            Ok(Ok(mut w)) => {
                use tokio::io::AsyncWriteExt;
                let _ = w.write_all(&client::disconnect_bytes(false)).await;
                let _ = w.shutdown().await;
            }
            _ => p.writer.abort(),
        }
        p.reader.abort();
    }
    // One deadline for all of them: a consumer whose broker stopped reading its socket
    // cannot hold the exit for 5 s each (60 stuck consumers were 5 minutes).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    for t in consumer_tasks {
        let _ = tokio::time::timeout_at(deadline, t).await;
    }

    let phases: Vec<PhaseReport> = (0..num_phases)
        .map(|j| PhaseReport {
            index: j,
            target_rate: cfg.phases[j].rate,
            agg: aggregate(
                phase_counts[j].0,
                phase_counts[j].1,
                &phase_counts[j].2,
                &phase_hist[j],
                true,
            ),
            saturated: phase_saturated[j],
        })
        .collect();
    let mut tot_h = Histos::default();
    let mut tot_c = Counters::default();
    let (mut tot_d, mut tot_o) = (0, 0);
    for j in 0..num_phases {
        tot_h.add(&phase_hist[j]);
        tot_d += phase_counts[j].0;
        tot_o += phase_counts[j].1;
        let c = &phase_counts[j].2;
        tot_c.sent += c.sent;
        tot_c.acked += c.acked;
        tot_c.received += c.received;
    }
    let saturated_windows: Vec<usize> = windows
        .iter()
        .filter(|w| w.kind == WindowKind::Measure && w.health.saturated)
        .map(|w| w.index)
        .collect();
    let errors = ctx.errors.counts();
    let saturated = !saturated_windows.is_empty();
    let outcome =
        if errors.disconnects + errors.server_disconnects + errors.protocol + errors.write > 0 {
            Outcome::Error
        } else if saturated {
            Outcome::Void
        } else if !ledger.closed {
            Outcome::Loss
        } else {
            Outcome::Ok
        };
    if let Some(t) = metrics_task {
        t.abort();
    }
    let window_min = window;
    Ok(Summary {
        schema: SCHEMA,
        tool: Tool {
            name: "mqtt-load",
            version: env!("CARGO_PKG_VERSION"),
        },
        run_id: format!("{run:08x}"),
        started_unix_ms,
        schedule_start_unix_ms: started_unix_ms + start.saturating_sub(started_ns) / 1_000_000,
        workload: Workload {
            phases: cfg
                .phases
                .iter()
                .map(|p| PhaseSpec {
                    rate: p.rate,
                    warmup_s: p.warmup_ns as f64 / NS,
                    measure_s: p.measure_ns as f64 / NS,
                })
                .collect(),
            window_s: cfg.window_ns as f64 / NS,
            publishers: cfg.publishers,
            inflight: cfg.inflight,
            qos: cfg.qos,
            payload_bytes: cfg.payload,
            topics: cfg.topics,
            topic_prefix: cfg.topic_prefix.clone(),
            shape: cfg.shape,
            consumers: if cfg.shape == Shape::None {
                0
            } else {
                cfg.consumers
            },
            groups: if cfg.shape == Shape::Share {
                cfg.groups
            } else {
                0
            },
            sessions: cfg.sessions,
            session_expiry_s: if durable { cfg.session_expiry } else { 0 },
            session_skew: cfg.session_skew(),
            deliveries_per_message: streams_per_topic,
            brokers: cfg.brokers.iter().map(ToString::to_string).collect(),
        },
        connections: Connections {
            publishers: cfg.publishers as usize,
            consumers: specs.len(),
            receive_maximum_min,
            inflight_window_min: window_min,
            connect_s,
        },
        start: start_report,
        windows,
        phases,
        totals: aggregate(tot_d, tot_o, &tot_c, &tot_h, true),
        drain: DrainReport {
            duration_s: (drain_end.saturating_sub(drain_start)) as f64 / NS,
            ended_by,
            quiet_s: quiet_ns as f64 / NS,
            outstanding_deliveries,
            outstanding_acks,
            since_progress_s: since_progress_ns as f64 / NS,
        },
        ledger,
        missing_sample,
        spread: Spread::of(
            &cfg.consumer_specs(),
            &ctx.consumer_received
                .iter()
                .map(|n| n.load(Ordering::Relaxed))
                .collect::<Vec<_>>(),
        ),
        generator: GeneratorReport {
            saturated,
            saturated_windows,
            thresholds: cfg.thresholds,
            worker_threads: cfg.worker_threads,
            per_thread_cpu: per_thread,
            cpu_cores_max,
            sent_per_cpu_second: if measured_cpu_ns > 0 {
                (measured_sent as f64 / (measured_cpu_ns as f64 / NS)).round()
            } else {
                0.0
            },
        },
        errors,
        outcome,
    })
}
