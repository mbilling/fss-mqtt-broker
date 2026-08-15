//! The **durable-path macro-benchmark** ([ADR 0048](../../docs/adr/0048-comparative-benchmarking.md)
//! T5, issue #244): end-to-end **acked** `QoS` 1 / `QoS` 2 publish throughput and latency
//! against a real quorum with the durable plane ON, measured through the production
//! binary's own client port.
//!
//! ADR 0044 P6's baselines are per-operation CPU costs; `bench/` measures a single
//! non-durable node against other brokers. Neither answers the question an adopter asks:
//! *how many durable, quorum-acked messages per second, at what tail?* This file answers it,
//! and — because the answer is only as good as its disclosure — it also **judges its own run**
//! and prints the verdict beside the numbers.
//!
//! # Two lanes, one driver
//!
//! The measurement code does not know whether the cluster it drives was spawned here or
//! provisioned on separate hosts:
//!
//! - **Single host (default).** `MQTTD_BENCH_NODES` (default 3) production binaries are
//!   spawned as real OS processes over real data dirs, configured through the documented
//!   `MQTTD_*` environment (the `proc_common` tier, ADR 0044 P1) — durable plane on, since
//!   nothing sets `MQTTD_DURABLE_SESSIONS=0`.
//! - **Multi host.** With `MQTTD_BENCH_BROKERS` / `MQTTD_BENCH_HEALTH` / `MQTTD_BENCH_NODE_IDS`
//!   set (parallel comma lists, same order), **nothing is spawned**: the driver connects to
//!   brokers an operator provisioned, scrapes their `/metrics`, and reports the same table.
//!   This is the lane ADR 0048 §2 requires for any scaling claim. The exact invocation is in
//!   [`docs/benchmarks/DURABLE-PATH.md`](../../docs/benchmarks/DURABLE-PATH.md).
//!
//! # Method
//!
//! Closed-loop, saturating: `MQTTD_BENCH_PUBLISHERS` publisher connections each keep a
//! sliding window of `MQTTD_BENCH_WINDOW` publishes in flight (one packet id per window slot,
//! re-used the instant its ack lands), so the offered rate *is* the achieved rate and the
//! measured latency is a real per-message ack round trip — for `QoS` 1 the PUBACK, for
//! `QoS` 2 the PUBCOMP that completes the four-packet handshake. Because the broker acks
//! after durable (#124 / ADR 0057), that round trip **is** the end-to-end durable-commit
//! latency as a publisher experiences it.
//!
//! Subscribers are real, online, and draining: `MQTTD_BENCH_SUBS` sessions, each on its own
//! topic, each acking every delivery. Their client ids are chosen with the broker's **own**
//! placement hash so every session is HRW-owned by node 0 (`ids_owned_by`) — which is what
//! makes "publish via the owner" and "publish via a non-owner (ADR 0005 relay)" separable
//! arms instead of an uninterpretable average.
//!
//! Warm-up is discarded by timestamp (samples are kept only if they completed inside the
//! measurement window), every arm is repeated `MQTTD_BENCH_REPS` times (default 3) and the
//! report prints median **and** min..max, because one number from one run is not evidence.
//!
//! # A run judges itself
//!
//! `RunStats::violations` fails a run loudly rather than letting it be quoted:
//!
//! - **debug profile** — the per-PR test profile is unoptimized; numbers from it are void;
//! - **client errors** — any timed-out or closed publisher connection;
//! - **`durable_append_failures_total`** moved, or
//!   **`publish_dropped_total{reason="append-backlog-full"}`** moved (lane saturation), or
//! - **`replication_min_actual < replication_write_floor`** on any node — a run measured
//!   under a refusal/brownout regime is not a throughput result;
//! - **driver-bound** — the driver process burned more CPU than the busiest broker, so the
//!   number bounds the *driver*, not the broker. Reported, not fatal: a driver-bound run is
//!   still a valid *floor* ("at least this fast"), never a capacity claim.
//!
//! Broker-side counters (`durable_append_latency_seconds`,
//! `hub_dispatch_seconds{command="publish"}`, `append_lane_jobs`) are differenced across the
//! arm, so the store's share of the ack RTT and the hub loop's own dispatch tail are printed
//! next to the client-side tail — the reader can see whether a run was store-bound or
//! loop-bound.
//!
//! `#[ignore]`d: these tests take minutes and the per-PR profile cannot produce a valid
//! number. Run them with `--ignored --nocapture --release`; see `docs/benchmarks/DURABLE-PATH.md`.

mod common;
mod proc_common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mqtt_cluster::placement::{group_of, Placement};
use mqtt_cluster::swim::MemberState;
use mqtt_cluster::NodeId;
use mqtt_codec::{Packet, QoS};
use proc_common::{free_tcp_port, free_udp_port, http_get, log_tail, ProcNode, SWIM_KEY};

// ---------------------------------------------------------------------------
// Configuration — every knob is an env var, so the documented invocation and the
// run that produced the published numbers are the same command.
// ---------------------------------------------------------------------------

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_list(key: &str) -> Option<Vec<String>> {
    let raw = std::env::var(key).ok()?;
    let items: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if items.is_empty() {
        None
    } else {
        Some(items)
    }
}

/// The benchmark's whole parameter set, resolved from the environment once and
/// **printed** at the top of every report so the numbers carry their own load.
#[derive(Debug, Clone)]
struct Cfg {
    nodes: usize,
    publishers: usize,
    subs: usize,
    window: usize,
    payload: usize,
    warmup: Duration,
    measure: Duration,
    reps: usize,
    node_workers: usize,
    log_level: String,
    /// How long the readiness gate waits for the cluster to become measurable.
    preflight: Duration,
}

impl Cfg {
    fn from_env() -> Self {
        Cfg {
            nodes: env_usize("MQTTD_BENCH_NODES", 3),
            publishers: env_usize("MQTTD_BENCH_PUBLISHERS", 8),
            subs: env_usize("MQTTD_BENCH_SUBS", 4),
            window: env_usize("MQTTD_BENCH_WINDOW", 16),
            payload: env_usize("MQTTD_BENCH_PAYLOAD", 256),
            warmup: Duration::from_secs(env_usize("MQTTD_BENCH_WARMUP_SECS", 5) as u64),
            measure: Duration::from_secs(env_usize("MQTTD_BENCH_SECS", 12) as u64),
            reps: env_usize("MQTTD_BENCH_REPS", 3),
            node_workers: env_usize("MQTTD_BENCH_NODE_WORKERS", 2),
            log_level: env_string("MQTTD_BENCH_LOG", "warn"),
            preflight: Duration::from_secs(env_usize("MQTTD_BENCH_PREFLIGHT_SECS", 120) as u64),
        }
    }

    fn describe(&self) -> String {
        format!(
            "publishers={} window={} (={}, in flight) subs={} payload={}B warmup={}s measure={}s reps={} \
             node_workers={} RUST_LOG={} profile={}",
            self.publishers,
            self.window,
            self.publishers * self.window,
            self.subs,
            self.payload,
            self.warmup.as_secs(),
            self.measure.as_secs(),
            self.reps,
            self.node_workers,
            self.log_level,
            profile()
        )
    }
}

fn profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug (UNOPTIMIZED — numbers from this profile are void)"
    } else {
        "release"
    }
}

// ---------------------------------------------------------------------------
// The cluster under measurement: spawned here, or provisioned elsewhere.
// ---------------------------------------------------------------------------

/// One broker the driver can reach: its node id (needed to compute placement),
/// its MQTT port, and its health/metrics port.
#[derive(Debug, Clone)]
struct Endpoint {
    id: String,
    client: SocketAddr,
    health: SocketAddr,
}

struct Cluster {
    endpoints: Vec<Endpoint>,
    /// Spawned processes (empty in multi-host mode) — owned so they are killed on drop.
    nodes: Vec<ProcNode>,
    _tmp: Option<common::TempDir>,
    external: bool,
}

impl Cluster {
    fn ring(&self) -> Placement {
        let ids: Vec<String> = self.endpoints.iter().map(|e| e.id.clone()).collect();
        ring_over(&ids)
    }

    /// Every spawned node's OS pid, for CPU accounting. Empty in multi-host mode
    /// (the brokers are on other machines; their CPU is the operator's to report).
    fn pids(&mut self) -> Vec<u32> {
        self.nodes
            .iter_mut()
            .filter_map(|n| n.child.as_ref().and_then(tokio::process::Child::id))
            .collect()
    }

    async fn kill_all(&mut self) {
        for n in &mut self.nodes {
            n.kill().await;
        }
    }
}

/// A `Placement` ring over the cluster's node ids — the broker's own hash, so
/// which node owns which session is this test's decision, not a coincidence.
fn ring_over(ids: &[String]) -> Placement {
    let mut ring = Placement::new(NodeId(ids[0].clone()), 3);
    for id in &ids[1..] {
        ring.observe(&NodeId(id.clone()), MemberState::Alive, "127.0.0.1:1", None);
    }
    ring
}

/// `n` client ids whose placement group `node` HRW-owns — computed before anything
/// connects (the trick `cluster_proc.rs` uses to pin ownership).
fn ids_owned_by(ring: &Placement, node: &str, prefix: &str, n: usize) -> Vec<String> {
    let mut out = Vec::new();
    for k in 0..400_000u32 {
        let candidate = format!("{prefix}{k}");
        if ring.hrw_owner(group_of(&candidate)).0 == node {
            out.push(candidate);
            if out.len() == n {
                return out;
            }
        }
    }
    panic!("no {n} client ids HRW-owned by {node} under prefix {prefix}");
}

/// Spawn one broker process. A local copy of `ProcNode::spawn` **on purpose**: a
/// benchmark must control (and disclose) the two settings that dominate a shared-host
/// measurement — the runtime's worker-thread count and the log level — and
/// `proc_common::ProcNode::spawn` hardcodes `RUST_LOG=info` with no thread bound.
/// Everything else is byte-for-byte the documented operator environment.
fn spawn_node(node: &mut ProcNode, cfg: &Cfg) {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&node.log_path)
        .expect("node log file");
    let child = tokio::process::Command::new(&node.binary)
        .env("MQTTD_NODE_ID", &node.id)
        .env("MQTTD_PLAINTEXT_BIND", node.client_addr.to_string())
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("MQTTD_PEER_BIND", node.peer_bind.to_string())
        .env("MQTTD_PEER_ADVERTISE", &node.relay_addr)
        .env("MQTTD_SWIM_BIND", &node.swim_bind)
        .env("MQTTD_SWIM_SEEDS", &node.swim_seeds)
        .env("MQTTD_SWIM_KEY", SWIM_KEY)
        .env("MQTTD_DATA_DIR", &node.data_dir)
        .env("MQTTD_HEALTH_BIND", node.health_addr.to_string())
        .env("MQTTD_SHUTDOWN_GRACE", "0")
        .env("RUST_LOG", &cfg.log_level)
        .env("TOKIO_WORKER_THREADS", cfg.node_workers.to_string())
        .stdout(std::process::Stdio::from(log.try_clone().unwrap()))
        .stderr(std::process::Stdio::from(log))
        .spawn()
        .expect("spawn mqttd binary");
    node.child = Some(child);
}

/// The peer-bus relay, with **`TCP_NODELAY` on both halves**.
///
/// `proc_common::spawn_relay` leaves Nagle on, which is harmless for a correctness
/// harness and ruinous for a benchmark: a replication round trip is a pair of small
/// writes, and Nagle + delayed ACK adds tens of milliseconds *per append* — measured
/// here at ~25 ms mean append latency against ~0.02 ms `fsync` on the same volume, i.e.
/// the harness, not the broker, would have been the number. `MQTTD_BENCH_RELAY=0`
/// removes the relay from the path altogether (peers dial the listener directly); the
/// isolation experiment needs it, so it is on by default.
async fn spawn_bench_relay(
    target: SocketAddr,
) -> (String, proc_common::RelayCtl, tokio::task::AbortHandle) {
    if env_usize("MQTTD_BENCH_RELAY", 1) == 0 {
        let (tx, _rx) = tokio::sync::watch::channel(proc_common::LinkMode::Healthy);
        let idle = tokio::spawn(std::future::pending::<()>());
        return (
            target.to_string(),
            proc_common::RelayCtl { mode: tx },
            idle.abort_handle(),
        );
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (mode_tx, mode_rx) = tokio::sync::watch::channel(proc_common::LinkMode::Healthy);
    let ctl = proc_common::RelayCtl { mode: mode_tx };
    let accept = tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                break;
            };
            if *mode_rx.borrow() == proc_common::LinkMode::Severed {
                continue;
            }
            let mut severed = mode_rx.clone();
            let mode = mode_rx.clone();
            tokio::spawn(async move {
                let Ok(outbound) = tokio::net::TcpStream::connect(target).await else {
                    return;
                };
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (in_r, in_w) = inbound.into_split();
                let (out_r, out_w) = outbound.into_split();
                tokio::select! {
                    () = proc_common::pump(in_r, out_w, mode.clone()) => {}
                    () = proc_common::pump(out_r, in_w, mode) => {}
                    _ = severed.wait_for(|s| *s == proc_common::LinkMode::Severed) => {}
                }
            });
        }
    });
    (addr, ctl, accept.abort_handle())
}

/// Build `n` nodes with relays fronting every peer listener and full cross-seeding
/// (`proc_common::build_topology`'s shape, widened past its hardcoded three so the
/// isolation experiment can have a replica set that excludes a node).
async fn build_bench_topology(seed: u64, root: &std::path::Path, n: usize) -> Vec<ProcNode> {
    let names: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
    let mut peer_binds = Vec::new();
    let mut swim_binds = Vec::new();
    for _ in 0..n {
        peer_binds.push(SocketAddr::from(([127, 0, 0, 1], free_tcp_port())));
        swim_binds.push(format!("127.0.0.1:{}", free_udp_port()));
    }
    let mut nodes = Vec::new();
    for (i, name) in names.iter().enumerate() {
        let id = format!("bench{seed}-{name}");
        let data_dir = root.join(name);
        std::fs::create_dir_all(&data_dir).expect("node dir");
        let (relay_addr, relay, relay_abort) = spawn_bench_relay(peer_binds[i]).await;
        // The founder is the node with no seeds (main.rs's rule).
        let swim_seeds = if i == 0 {
            String::new()
        } else {
            swim_binds
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, s)| s.clone())
                .collect::<Vec<_>>()
                .join(",")
        };
        nodes.push(ProcNode {
            log_path: root.join(format!("{id}.log")),
            id,
            child: None,
            data_dir,
            client_addr: SocketAddr::from(([127, 0, 0, 1], free_tcp_port())),
            peer_bind: peer_binds[i],
            swim_bind: swim_binds[i].clone(),
            health_addr: SocketAddr::from(([127, 0, 0, 1], free_tcp_port())),
            swim_seeds,
            relay_addr,
            relay,
            _relay_abort: relay_abort,
            file_size_limit_blocks: None,
            extra_env: Vec::new(),
            binary: PathBuf::from(env!("CARGO_BIN_EXE_mqttd")),
        });
    }
    nodes
}

/// Stand up (or attach to) the cluster the arms will be measured against.
async fn cluster(cfg: &Cfg) -> Cluster {
    if let (Some(brokers), Some(health), Some(ids)) = (
        env_list("MQTTD_BENCH_BROKERS"),
        env_list("MQTTD_BENCH_HEALTH"),
        env_list("MQTTD_BENCH_NODE_IDS"),
    ) {
        assert!(
            brokers.len() == health.len() && brokers.len() == ids.len(),
            "MQTTD_BENCH_BROKERS ({}), MQTTD_BENCH_HEALTH ({}) and MQTTD_BENCH_NODE_IDS ({}) \
             must be parallel lists of the same length, in the same node order",
            brokers.len(),
            health.len(),
            ids.len()
        );
        let endpoints: Vec<Endpoint> = (0..brokers.len())
            .map(|i| Endpoint {
                id: ids[i].clone(),
                client: resolve(&brokers[i]),
                health: resolve(&health[i]),
            })
            .collect();
        println!(
            "## Mode: MULTI-HOST — driving {} operator-provisioned brokers (nothing spawned here)",
            endpoints.len()
        );
        for e in &endpoints {
            println!("  - {} mqtt={} health={}", e.id, e.client, e.health);
        }
        let cl = Cluster {
            endpoints,
            nodes: Vec::new(),
            _tmp: None,
            external: true,
        };
        preflight(&cl, cfg.preflight).await;
        return cl;
    }

    println!(
        "## Mode: SINGLE HOST — spawning {} production-binary processes on this machine",
        cfg.nodes
    );
    let tmp = common::TempDir::new();
    println!("  data dirs + node logs: {}", tmp.path().display());
    let keep = env_usize("MQTTD_BENCH_KEEP_DIRS", 0) == 1;
    let seed = u64::from(std::process::id());
    let mut nodes = build_bench_topology(seed, tmp.path(), cfg.nodes).await;
    for n in &mut nodes {
        spawn_node(n, cfg);
    }
    let endpoints: Vec<Endpoint> = nodes
        .iter()
        .map(|n| Endpoint {
            id: n.id.clone(),
            client: n.client_addr,
            health: n.health_addr,
        })
        .collect();
    println!("  node env (identical per node except ids/ports):");
    println!(
        "    MQTTD_ALLOW_ANONYMOUS=1 MQTTD_SWIM_KEY=<fixed test key> MQTTD_SHUTDOWN_GRACE=0 \
         RUST_LOG={} TOKIO_WORKER_THREADS={}",
        cfg.log_level, cfg.node_workers
    );
    println!("    durable plane: ON (nothing sets MQTTD_DURABLE_SESSIONS=0)");
    for n in &nodes {
        println!(
            "    {} mqtt={} peer={} (advertised via relay {}) swim={} health={}",
            n.id, n.client_addr, n.peer_bind, n.relay_addr, n.swim_bind, n.health_addr
        );
    }
    let cl = Cluster {
        endpoints,
        nodes,
        _tmp: if keep {
            // MQTTD_BENCH_KEEP_DIRS=1 leaks the temp dir so the node logs survive the
            // run — the only way to diagnose a broker-side stall after the fact.
            std::mem::forget(tmp);
            None
        } else {
            Some(tmp)
        },
        external: false,
    };
    preflight(&cl, cfg.preflight).await;
    cl
}

fn resolve(s: &str) -> SocketAddr {
    use std::net::ToSocketAddrs;
    s.to_socket_addrs()
        .unwrap_or_else(|e| panic!("cannot resolve {s}: {e}"))
        .next()
        .unwrap_or_else(|| panic!("{s} resolved to nothing"))
}

/// A run may not start until the cluster is genuinely serving durable writes:
/// every node reports full membership and a ready lease group on `/readyz`
/// (ADR 0020), holds at least one replica group, and — the strong condition —
/// has every group it tracks stamped **current** for the present replica set
/// (ADR 0043 P1). Before that a durable read/write legitimately refuses, and
/// measuring through a refusal regime is measuring nothing.
///
/// The strong condition is a **gate with a budget, not a hard requirement**: on a
/// cluster formed incrementally it is not always reachable, because a node keeps
/// *tracking* groups it held under an earlier membership and no longer holds, so
/// `current` plateaus below `tracked` and stays there. Measured here: a 3-node
/// cluster reaches `current == tracked` in ~5 s, while a 5-node one plateaus at
/// ~75-85 % within 15 s and does not move for minutes. Blocking on it would make
/// the 5-node isolation experiment unrunnable for a reason that has nothing to do
/// with what it measures, so after the budget the harness proceeds and **prints
/// the shortfall**, and the arms' own functional gates (a granted durability-gated
/// SUBACK, then an acked warm-up publish that must be observed to arrive) carry
/// the burden of proving the cluster really serves durable traffic.
async fn preflight(cl: &Cluster, budget: Duration) {
    let want = cl.endpoints.len();
    let started = Instant::now();
    let deadline = started + budget;
    let mut last_report = Instant::now();
    // Sticky: five brokers plus the driver on 8 cores can starve a health endpoint past
    // its 2 s HTTP timeout for one poll, and failing a whole run because the LAST poll
    // before the deadline happened to be that one is a harness defect, not a finding.
    let mut serving_at: Option<Instant> = None;
    loop {
        let mut all = true;
        let mut serving = true;
        let mut observed = Vec::new();
        for e in &cl.endpoints {
            let r = readyz(e.health).await;
            let g = replica_groups(e.health).await;
            let ready_ok = matches!(r, Some((true, m, true)) if m == want);
            if !(ready_ok && matches!(g, Some((c, _)) if c > 0)) {
                serving = false;
            }
            if !(ready_ok && matches!(g, Some((c, t)) if c == t && t > 0)) {
                all = false;
            }
            observed.push(format!(
                "{}: readyz={} replica_groups={}",
                e.id,
                match r {
                    Some((ready, m, lease)) =>
                        format!("ready={ready} members={m} lease_group_ready={lease}"),
                    None => "unreachable".into(),
                },
                match g {
                    Some((c, t)) => format!("{c}/{t} current"),
                    None => "unreachable".into(),
                }
            ));
        }
        if serving {
            serving_at = Some(Instant::now());
        }
        if all {
            println!(
                "  preflight OK in {:.1}s: {want} members, lease group ready, replica groups current",
                started.elapsed().as_secs_f64()
            );
            return;
        }
        if Instant::now() >= deadline {
            // Ready + holding groups is the hard floor; the full current==tracked stamp
            // is the one that may plateau. Say exactly which one we are proceeding
            // without, so no reader has to guess what the run was measured against.
            let recently_serving =
                serving || serving_at.is_some_and(|t| t.elapsed() < Duration::from_secs(30));
            assert!(
                recently_serving,
                "cluster never became measurable within {:?} — no poll in the last 30s \
                 found every node ready with full membership, a ready lease group and at \
                 least one held replica group:\n  {}",
                started.elapsed(),
                observed.join("\n  ")
            );
            println!(
                "  !! preflight PROCEEDING WITHOUT full replica-group currency after {:.0}s \
                 (every node is ready, full membership, lease group ready, holding groups; \
                 some track groups from an earlier membership that will not re-stamp):\n  {}",
                started.elapsed().as_secs_f64(),
                observed.join("\n  ")
            );
            return;
        }
        // Convergence on a 5-node cluster takes tens of seconds and the sweep's progress
        // is the only way to tell "slow" from "stuck" before the deadline.
        if last_report.elapsed() >= Duration::from_secs(15) {
            last_report = Instant::now();
            println!(
                "  preflight at {:.0}s: {}",
                started.elapsed().as_secs_f64(),
                observed.join(" | ")
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ---------------------------------------------------------------------------
// Health/metrics reads that work against a REMOTE broker (so the multi-host lane
// runs the same code) — `proc_common`'s equivalents hang off a local `ProcNode`.
// ---------------------------------------------------------------------------

async fn readyz(health: SocketAddr) -> Option<(bool, usize, bool)> {
    let body = http_get(health, "/readyz").await?;
    let members = body
        .split("\"members\":")
        .nth(1)
        .and_then(|s| s.split([',', '}']).next())
        .and_then(|s| s.parse::<usize>().ok())?;
    Some((
        body.contains("\"ready\":true"),
        members,
        body.contains("\"lease_group_ready\":true"),
    ))
}

async fn replica_groups(health: SocketAddr) -> Option<(usize, usize)> {
    let body = http_get(health, "/statusz").await?;
    let frag = body
        .split("\"replica_groups\":{")
        .nth(1)?
        .split('}')
        .next()?;
    let field = |name: &str| -> Option<usize> {
        frag.split(&format!("\"{name}\":"))
            .nth(1)?
            .split([',', '}'])
            .next()?
            .parse()
            .ok()
    };
    Some((field("current")?, field("tracked")?))
}

/// One node's whole `/metrics` exposition as `(series-with-labels, value)` pairs.
#[derive(Debug, Clone, Default)]
struct Scrape(Vec<(String, f64)>);

impl Scrape {
    async fn of(health: SocketAddr) -> Scrape {
        let Some(body) = http_get(health, "/metrics").await else {
            return Scrape::default();
        };
        Scrape(
            body.lines()
                .filter(|l| !l.starts_with('#'))
                .filter_map(|l| l.rsplit_once(' '))
                .filter_map(|(name, v)| {
                    v.trim()
                        .parse::<f64>()
                        .ok()
                        .map(|v| (name.trim().to_string(), v))
                })
                .collect(),
        )
    }

    /// One sample by exact series name, `0.0` when absent (an untouched counter
    /// family renders no line at all).
    fn get(&self, series: &str) -> f64 {
        self.0
            .iter()
            .find(|(n, _)| n == series)
            .map_or(0.0, |(_, v)| *v)
    }

    /// The sum of every sample whose series starts with `prefix` — for a counter
    /// family whose label values are not known ahead of time.
    fn sum_prefix(&self, prefix: &str) -> f64 {
        self.0
            .iter()
            .filter(|(n, _)| n.starts_with(prefix))
            .map(|(_, v)| *v)
            .sum()
    }

    /// A histogram's cumulative buckets as `(le, count)`, ascending. `selector`
    /// narrows a labelled family (e.g. `command="publish"`).
    fn buckets(&self, name: &str, selector: Option<&str>) -> Vec<(f64, f64)> {
        let head = format!("{name}_bucket{{");
        let mut out: Vec<(f64, f64)> = self
            .0
            .iter()
            .filter(|(n, _)| n.starts_with(&head))
            .filter(|(n, _)| selector.is_none_or(|s| n.contains(s)))
            .filter_map(|(n, v)| {
                let le = n.split("le=\"").nth(1)?.split('"').next()?;
                let le = if le.starts_with("+Inf") {
                    f64::INFINITY
                } else {
                    le.parse().ok()?
                };
                Some((le, *v))
            })
            .collect();
        out.sort_by(|a, b| a.0.total_cmp(&b.0));
        out
    }

    fn hist_sum(&self, name: &str, selector: Option<&str>) -> f64 {
        self.labelled(&format!("{name}_sum"), selector)
    }

    fn hist_count(&self, name: &str, selector: Option<&str>) -> f64 {
        self.labelled(&format!("{name}_count"), selector)
    }

    fn labelled(&self, name: &str, selector: Option<&str>) -> f64 {
        self.0
            .iter()
            .filter(|(n, _)| n.starts_with(name))
            .filter(|(n, _)| selector.is_none_or(|s| n.contains(s)))
            .map(|(_, v)| *v)
            .sum()
    }
}

/// The quantile of a histogram's **delta** over an arm, as a bucket upper bound —
/// coarse on purpose (the true value is at most the bound, so it cannot flatter).
fn delta_quantile(before: &Scrape, after: &Scrape, name: &str, sel: Option<&str>, q: f64) -> f64 {
    let b = before.buckets(name, sel);
    let a = after.buckets(name, sel);
    if a.is_empty() {
        return f64::NAN;
    }
    let delta: Vec<(f64, f64)> = a
        .iter()
        .map(|(le, count)| {
            let was = b
                .iter()
                .find(|(l, _)| l.total_cmp(le) == std::cmp::Ordering::Equal)
                .map_or(0.0, |(_, c)| *c);
            (*le, count - was)
        })
        .collect();
    let total = delta.last().map_or(0.0, |(_, c)| *c);
    if total <= 0.0 {
        return f64::NAN;
    }
    let target = total * q;
    for (le, c) in &delta {
        if *c >= target {
            return *le * 1000.0; // seconds → ms
        }
    }
    f64::INFINITY
}

/// A process's cumulative CPU seconds (`ps -o cputime=`, centisecond resolution) —
/// differenced across an arm this is exactly the CPU the arm consumed, which
/// `%cpu` (a decaying lifetime average on Darwin) cannot give.
fn cpu_secs(pid: u32) -> Option<f64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "cputime=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if text.is_empty() {
        return None;
    }
    let mut secs = 0.0;
    for part in text.split(':') {
        secs = secs * 60.0 + part.parse::<f64>().ok()?;
    }
    Some(secs)
}

fn rss_bytes(pid: u32) -> Option<u64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let kb: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(kb * 1024)
}

// ---------------------------------------------------------------------------
// The load driver.
// ---------------------------------------------------------------------------

/// One completed publish: when its ack landed, and how long it took.
type Sample = (Instant, Duration);

struct PubResult {
    samples: Vec<Sample>,
    errors: usize,
    /// Why a run went wrong, in the publisher's own words — a benchmark that fails
    /// silently is worse than one that fails.
    diagnosis: Option<String>,
}

/// One publisher connection running a sliding window of `window` in-flight
/// publishes until `stop`. Packet id `k` is re-issued the moment its ack lands, so
/// exactly `window` publishes are outstanding for the whole run and the offered
/// rate equals the achieved rate by construction.
async fn publisher(
    addr: SocketAddr,
    id: String,
    topic: String,
    qos: QoS,
    window: usize,
    payload_len: usize,
    stop: Instant,
) -> PubResult {
    match common::Client::connect_v311_within(addr, &id, true, Duration::from_secs(20)).await {
        Some((c, _)) => run_publisher(c, id, topic, qos, window, payload_len, stop).await,
        None => PubResult {
            samples: Vec::new(),
            errors: 1,
            diagnosis: Some(format!(
                "{id}: CONNECT to {addr} never got a success CONNACK"
            )),
        },
    }
}

/// The publishing loop over an ALREADY-CONNECTED client. Split out so an experiment
/// can pay the CONNECT cost *before* starting its clock — under a fault, a CONNECT can
/// itself take seconds, and charging that to throughput measures the wrong thing.
async fn run_publisher(
    mut c: common::Client,
    id: String,
    topic: String,
    qos: QoS,
    window: usize,
    payload_len: usize,
    stop: Instant,
) -> PubResult {
    let mut out = PubResult {
        samples: Vec::with_capacity(1 << 14),
        errors: 0,
        diagnosis: None,
    };
    let payload = vec![b'x'; payload_len];
    let mut sent_at: Vec<Option<Instant>> = vec![None; window];
    let mut in_flight = 0usize;
    let (mut acks, mut recs, mut comps, mut unexpected) = (0usize, 0usize, 0usize, 0usize);
    let mut last_other: Option<String> = None;

    for (slot, at) in sent_at.iter_mut().enumerate().take(window) {
        let pkid = u16::try_from(slot + 1).expect("window fits u16");
        c.publish(&topic, &payload, qos, Some(pkid), vec![]).await;
        *at = Some(Instant::now());
        in_flight += 1;
    }

    loop {
        if in_flight == 0 {
            break;
        }
        let left = Duration::from_secs(30);
        match c.recv_bounded(left).await {
            common::Recv::Packet(Packet::PubAck(a)) if qos == QoS::AtLeastOnce => {
                acks += 1;
                let slot = usize::from(a.pkid - 1);
                if let Some(t) = sent_at.get_mut(slot).and_then(Option::take) {
                    let now = Instant::now();
                    out.samples.push((now, now.duration_since(t)));
                    in_flight -= 1;
                    if now < stop {
                        c.publish(&topic, &payload, qos, Some(a.pkid), vec![]).await;
                        sent_at[slot] = Some(Instant::now());
                        in_flight += 1;
                    }
                }
            }
            common::Recv::Packet(Packet::PubRec(a)) if qos == QoS::ExactlyOnce => {
                recs += 1;
                c.pubrel(a.pkid).await;
            }
            common::Recv::Packet(Packet::PubComp(a)) if qos == QoS::ExactlyOnce => {
                comps += 1;
                let slot = usize::from(a.pkid - 1);
                if let Some(t) = sent_at.get_mut(slot).and_then(Option::take) {
                    let now = Instant::now();
                    out.samples.push((now, now.duration_since(t)));
                    in_flight -= 1;
                    if now < stop {
                        c.publish(&topic, &payload, qos, Some(a.pkid), vec![]).await;
                        sent_at[slot] = Some(Instant::now());
                        in_flight += 1;
                    }
                }
            }
            common::Recv::Packet(other) => {
                unexpected += 1;
                last_other = Some(format!("{other:?}"));
            }
            // A 30s silence with publishes outstanding, or a closed socket, is a
            // failed run — not a slow one. Both void the arm.
            common::Recv::Quiet | common::Recv::Closed => {
                out.errors += 1;
                out.diagnosis = Some(format!(
                    "{id}: connection went {} with {in_flight} publish(es) outstanding after \
                     {} completions (acks seen: puback={acks} pubrec={recs} pubcomp={comps}, \
                     {unexpected} unexpected packet(s), last = {last_other:?})",
                    "quiet-or-closed",
                    out.samples.len()
                ));
                break;
            }
        }
    }
    out
}

/// One subscriber connection draining and acking every delivery until `stop`.
/// Returns how many publishes it received (a sanity check against the publishers'
/// count: a durable ack the subscriber never sees would be a correctness bug, and
/// the acked-facts oracle in `cluster_proc.rs` owns that judgement — here it is
/// simply reported).
async fn drainer(mut c: common::Client, stop: Instant) -> usize {
    let mut got = 0usize;
    loop {
        let left = stop.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return got;
        }
        match c.recv_bounded(left.min(Duration::from_millis(500))).await {
            common::Recv::Packet(Packet::Publish(p)) => {
                got += 1;
                if let Some(pkid) = p.pkid {
                    c.puback(pkid).await;
                }
            }
            common::Recv::Packet(_) | common::Recv::Quiet => {}
            common::Recv::Closed => return got,
        }
    }
}

// ---------------------------------------------------------------------------
// One arm of the matrix.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Arm {
    label: &'static str,
    qos: QoS,
    /// `false` = clean-session subscribers: the SAME load with nothing durable to
    /// write, so the durable path's cost is a measured delta, not an assertion.
    durable_subs: bool,
    /// `true` = publishers attach to the node that owns the subscriber sessions;
    /// `false` = to a different node, so the publish crosses the ADR 0005 relay.
    via_owner: bool,
}

#[derive(Debug, Clone)]
struct RunStats {
    completed: usize,
    delivered: usize,
    /// Length of the measurement window the rate was computed over.
    secs: f64,
    rate: f64,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    p999: Duration,
    max: Duration,
    /// Completions that took >= 1 s. On a healthy loopback cluster this should be
    /// zero; it is not, and the count is the honest way to say so.
    stalls_1s: usize,
    errors: usize,
    append_count: f64,
    append_mean_ms: f64,
    append_p99_ms: f64,
    dispatch_p99_ms: f64,
    lane_jobs: f64,
    append_failures: f64,
    backlog_drops: f64,
    floor_ok: bool,
    driver_cpu: f64,
    node_cpu: Vec<f64>,
    rss: Vec<u64>,
}

impl RunStats {
    /// Everything that makes this run unquotable, in the order a reader cares.
    fn violations(&self) -> Vec<String> {
        let mut v = Vec::new();
        if cfg!(debug_assertions) {
            v.push("debug profile (unoptimized) — numbers are void".to_string());
        }
        if self.errors > 0 {
            v.push(format!("{} publisher connection error(s)", self.errors));
        }
        if self.append_failures > 0.0 {
            v.push(format!(
                "durable_append_failures_total moved by {}",
                self.append_failures
            ));
        }
        if self.backlog_drops > 0.0 {
            v.push(format!(
                "publish_dropped{{append-backlog-full}} moved by {} (lane saturation)",
                self.backlog_drops
            ));
        }
        if !self.floor_ok {
            v.push(
                "replication_min_actual < write_floor on some node (refusal regime)".to_string(),
            );
        }
        if self.completed == 0 {
            v.push("no publish completed inside the measurement window".to_string());
        }
        v
    }

    /// Things that weaken a number without voiding it — printed, never hidden.
    fn caveats(&self) -> Vec<String> {
        let mut v = Vec::new();
        if self.completed > 0 && self.completed < 500 {
            v.push(format!(
                "thin sample ({}): p99 rests on ~{} point(s)",
                self.completed,
                (self.completed / 100).max(1)
            ));
        }
        match self.driver_bound() {
            Some(true) => {
                v.push("driver-bound: read the rate as a floor, not a capacity".to_string());
            }
            // The check needs the brokers' CPU, which the external (multi-host) lane
            // cannot see. Saying so is the point: a silent `false` there would present an
            // UNRUN check as a passed one, in the lane whose numbers matter most.
            None => v.push(format!(
                "driver-bound check DID NOT RUN — no broker CPU available (external \
                 cluster). The driver burned {:.1} CPU-seconds; if that approaches the \
                 window x cores, read the rate as a floor",
                self.driver_cpu
            )),
            Some(false) => {}
        }
        v
    }

    /// Did the driver out-burn the busiest broker? Then the number bounds the
    /// driver, and is a floor for the broker rather than its capacity.
    ///
    /// `None` when no broker CPU is available — the external multi-host lane, where
    /// `Cluster::pids()` is empty because the brokers are on other machines. That case
    /// must NOT collapse to `Some(false)`: it would report an unrun check as a passing
    /// one exactly where the driver is most likely to be the bottleneck (one local driver
    /// against N remote brokers).
    fn driver_bound(&self) -> Option<bool> {
        let busiest = self.node_cpu.iter().copied().fold(0.0f64, f64::max);
        if busiest <= 0.0 {
            return None;
        }
        Some(self.driver_cpu > busiest)
    }
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn pct(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Run one arm once: establish subscribers, warm interest, saturate, judge.
#[allow(clippy::too_many_lines)] // one straight-line experiment; splitting it hides the shape
#[allow(clippy::cast_precision_loss)]
async fn run_arm(cl: &mut Cluster, cfg: &Cfg, arm: &Arm, tag: &str) -> RunStats {
    let ring = cl.ring();
    let owner_id = cl.endpoints[0].id.clone();
    let owner_addr = cl.endpoints[0].client;
    let pub_addr = if arm.via_owner {
        owner_addr
    } else {
        cl.endpoints[1 % cl.endpoints.len()].client
    };

    // Subscriber sessions pinned to node 0's HRW-owned groups, fresh ids per run so
    // no state from an earlier arm confounds this one.
    let sub_ids = ids_owned_by(&ring, &owner_id, &format!("bs-{tag}-"), cfg.subs);
    let topics: Vec<String> = (0..cfg.subs).map(|i| format!("bench/{tag}/{i}")).collect();
    // PUBLISHER ids are pinned to the group owned by the node they attach to. This is
    // not cosmetic: an inbound QoS 2 PUBLISH writes a dedup record in the PUBLISHER's
    // own placement group, and on this build a publisher attached to a node that does
    // not own that group fails closed — "QoS2 dedup store write failed; withholding
    // PUBREC (fail closed) … not the owning node for this group" — and the connection
    // then hangs with no error to the client. Pinning keeps the QoS 2 arm measurable;
    // the defect itself is reported, not worked around silently.
    let pub_node_id = cl
        .endpoints
        .iter()
        .find(|e| e.client == pub_addr)
        .map(|e| e.id.clone())
        .expect("publisher endpoint");
    let pub_ids = ids_owned_by(&ring, &pub_node_id, &format!("bp-{tag}-"), cfg.publishers);

    let mut subs = Vec::new();
    for i in 0..cfg.subs {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some((mut c, _)) = common::Client::connect_v311_within(
                owner_addr,
                &sub_ids[i],
                !arm.durable_subs,
                Duration::from_secs(20),
            )
            .await
            {
                // The SUBACK is durability-gated in the durable arm: retry until granted.
                let ack = c.subscribe(1, &topics[i], QoS::AtLeastOnce).await;
                if ack.return_codes.iter().all(|rc| *rc != 0x80) {
                    subs.push(c);
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "arm {tag}: subscriber {} never got a granted SUBACK",
                sub_ids[i]
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    // Interest warm-up: cross-node interest is eventually consistent, so a publish
    // from the PUBLISHERS' node must be observed to arrive before the clock starts.
    for i in 0..cfg.subs {
        let warm = format!("warm-{tag}-{i}").into_bytes();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some((mut p, _)) = common::Client::connect_v311_within(
                pub_addr,
                &format!("warmpub-{tag}-{i}"),
                true,
                Duration::from_secs(20),
            )
            .await
            {
                p.publish(&topics[i], &warm, QoS::AtLeastOnce, Some(1), vec![])
                    .await;
                let _ = p.recv_bounded(Duration::from_secs(5)).await;
            }
            let mut seen = false;
            while let common::Recv::Packet(pkt) =
                subs[i].recv_bounded(Duration::from_millis(700)).await
            {
                if let Packet::Publish(pb) = pkt {
                    if let Some(pkid) = pb.pkid {
                        subs[i].puback(pkid).await;
                    }
                    if pb.payload.as_ref() == warm.as_slice() {
                        seen = true;
                    }
                }
            }
            if seen {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "arm {tag}: interest from the publisher node to sub {i} never converged"
            );
        }
    }

    // --- the measured window -------------------------------------------------
    let pids = cl.pids();
    let before: Vec<Scrape> = {
        let mut v = Vec::new();
        for e in &cl.endpoints {
            v.push(Scrape::of(e.health).await);
        }
        v
    };
    let cpu_before: Vec<f64> = pids.iter().filter_map(|p| cpu_secs(*p)).collect();
    let driver_cpu_before = cpu_secs(std::process::id()).unwrap_or(0.0);

    let started = Instant::now();
    let measure_from = started + cfg.warmup;
    let stop = measure_from + cfg.measure;

    let mut drains = Vec::new();
    for c in subs {
        drains.push(tokio::spawn(drainer(c, stop + Duration::from_secs(2))));
    }
    let mut pubs = Vec::new();
    for k in 0..cfg.publishers {
        pubs.push(tokio::spawn(publisher(
            pub_addr,
            pub_ids[k].clone(),
            topics[k % cfg.subs].clone(),
            arm.qos,
            cfg.window,
            cfg.payload,
            stop,
        )));
    }

    let mut samples: Vec<Duration> = Vec::new();
    let mut errors = 0usize;
    let mut diagnoses: Vec<String> = Vec::new();
    for h in pubs {
        let r = h.await.expect("publisher task");
        errors += r.errors;
        if let Some(d) = r.diagnosis {
            diagnoses.push(d);
        }
        for (done, rtt) in r.samples {
            if done >= measure_from && done <= stop {
                samples.push(rtt);
            }
        }
    }
    for d in diagnoses.iter().take(3) {
        println!("    !! {d}");
    }
    let mut delivered = 0usize;
    for h in drains {
        delivered += h.await.unwrap_or(0);
    }

    let after: Vec<Scrape> = {
        let mut v = Vec::new();
        for e in &cl.endpoints {
            v.push(Scrape::of(e.health).await);
        }
        v
    };
    let cpu_after: Vec<f64> = pids.iter().filter_map(|p| cpu_secs(*p)).collect();
    let driver_cpu_after = cpu_secs(std::process::id()).unwrap_or(0.0);
    let rss: Vec<u64> = pids.iter().filter_map(|p| rss_bytes(*p)).collect();

    samples.sort_unstable();
    let secs = cfg.measure.as_secs_f64();
    let completed = samples.len();

    let append_count: f64 = (0..cl.endpoints.len())
        .map(|i| {
            after[i].hist_count("mqttd_durable_append_latency_seconds", None)
                - before[i].hist_count("mqttd_durable_append_latency_seconds", None)
        })
        .sum();
    let append_sum: f64 = (0..cl.endpoints.len())
        .map(|i| {
            after[i].hist_sum("mqttd_durable_append_latency_seconds", None)
                - before[i].hist_sum("mqttd_durable_append_latency_seconds", None)
        })
        .sum();
    let append_p99_ms = delta_quantile(
        &before[0],
        &after[0],
        "mqttd_durable_append_latency_seconds",
        None,
        0.99,
    );
    let dispatch_p99_ms = delta_quantile(
        &before[0],
        &after[0],
        "mqttd_hub_dispatch_seconds",
        Some("command=\"publish\""),
        0.99,
    );
    let lane_jobs = (0..cl.endpoints.len())
        .map(|i| after[i].get("mqttd_append_lane_jobs"))
        .fold(0.0f64, f64::max);
    let append_failures: f64 = (0..cl.endpoints.len())
        .map(|i| {
            after[i].sum_prefix("mqttd_durable_append_failures_total")
                - before[i].sum_prefix("mqttd_durable_append_failures_total")
        })
        .sum();
    let backlog_drops: f64 = (0..cl.endpoints.len())
        .map(|i| {
            after[i].get("mqttd_publish_dropped_total{reason=\"append-backlog-full\"}")
                - before[i].get("mqttd_publish_dropped_total{reason=\"append-backlog-full\"}")
        })
        .sum();
    let floor_ok = (0..cl.endpoints.len()).all(|i| {
        after[i].get("mqttd_replication_min_actual")
            >= after[i].get("mqttd_replication_write_floor")
    });

    RunStats {
        completed,
        delivered,
        secs,
        rate: completed as f64 / secs,
        p50: pct(&samples, 0.50),
        p95: pct(&samples, 0.95),
        p99: pct(&samples, 0.99),
        p999: pct(&samples, 0.999),
        max: samples.last().copied().unwrap_or_default(),
        stalls_1s: samples
            .iter()
            .filter(|d| **d >= Duration::from_secs(1))
            .count(),
        errors,
        append_count,
        append_mean_ms: if append_count > 0.0 {
            append_sum / append_count * 1000.0
        } else {
            f64::NAN
        },
        append_p99_ms,
        dispatch_p99_ms,
        lane_jobs,
        append_failures,
        backlog_drops,
        floor_ok,
        driver_cpu: driver_cpu_after - driver_cpu_before,
        node_cpu: cpu_after
            .iter()
            .zip(&cpu_before)
            .map(|(a, b)| a - b)
            .collect(),
        rss,
    }
}

fn median_f64(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn spread(v: &[f64], unit: &str) -> String {
    if v.is_empty() {
        return "—".to_string();
    }
    let lo = v.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = v.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let med = median_f64(v.to_vec());
    // Adaptive precision: a sub-millisecond percentile printed as "0.0ms" is a lie of
    // rounding, and these tables span 0.03 ms to 5000 ms.
    let p = if med.abs() < 1.0 {
        3
    } else {
        usize::from(med.abs() < 100.0)
    };
    format!("{med:.p$} [{lo:.p$}..{hi:.p$}]{unit}")
}

// ---------------------------------------------------------------------------
// Test 1 — the durable-path floor.
// ---------------------------------------------------------------------------

/// Sustained **acked** durable publish throughput and latency against a real
/// quorum, over four arms whose differences are the point:
///
/// | arm | what the delta isolates |
/// |---|---|
/// | `qos1-durable-owner` | the durable path, publisher on the session owner |
/// | `qos1-durable-relay` | + one ADR 0005 owner-relay hop |
/// | `qos1-clean-owner` | the same load with **nothing durable to write** |
/// | `qos2-durable-owner` | + the exactly-once handshake and its outbound-id record |
///
/// Every arm runs `MQTTD_BENCH_REPS` times; the report prints median and min..max.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "macro-benchmark: minutes long, and only meaningful in --release"]
#[allow(clippy::too_many_lines)]
async fn durable_path_floor() {
    let cfg = Cfg::from_env();
    println!("\n# Durable-path floor (ADR 0048 T5 / issue #244)");
    println!("host: {}", host_facts());
    println!("load: {}", cfg.describe());
    if cfg!(debug_assertions) {
        println!(
            "\n!! REFUSING TO TREAT THIS RUN AS EVIDENCE: the test profile is unoptimized. \
             Re-run with --release.\n"
        );
    }

    let mut cl = cluster(&cfg).await;
    let arms = [
        Arm {
            label: "qos1-durable-owner",
            qos: QoS::AtLeastOnce,
            durable_subs: true,
            via_owner: true,
        },
        Arm {
            label: "qos1-durable-relay",
            qos: QoS::AtLeastOnce,
            durable_subs: true,
            via_owner: false,
        },
        Arm {
            label: "qos1-clean-owner",
            qos: QoS::AtLeastOnce,
            durable_subs: false,
            via_owner: true,
        },
        Arm {
            label: "qos2-durable-owner",
            qos: QoS::ExactlyOnce,
            durable_subs: true,
            via_owner: true,
        },
    ];

    let mut all: Vec<(&'static str, Vec<RunStats>)> =
        arms.iter().map(|a| (a.label, Vec::new())).collect();

    for rep in 0..cfg.reps {
        for (ai, arm) in arms.iter().enumerate() {
            let tag = format!("{}-r{rep}", arm.label);
            let st = run_arm(&mut cl, &cfg, arm, &tag).await;
            println!(
                "  rep {rep} {:<20} {:>9.0} msg/s  p50 {:>7.2}ms p95 {:>7.2}ms p99 {:>7.2}ms p999 {:>8.2}ms max {:>8.2}ms stalls>=1s {:>3}  \
                 appends {:>7.0} (mean {:>6.2}ms, p99<={:>7.2}ms)  dispatch(publish) p99<={:>7.2}ms  \
                 lane {:>4.0}  driverCPU {:>5.1}s nodeCPU {:?}  {}",
                arm.label,
                st.rate,
                ms(st.p50),
                ms(st.p95),
                ms(st.p99),
                ms(st.p999),
                ms(st.max),
                st.stalls_1s,
                st.append_count,
                st.append_mean_ms,
                st.append_p99_ms,
                st.dispatch_p99_ms,
                st.lane_jobs,
                st.driver_cpu,
                st.node_cpu
                    .iter()
                    .map(|c| format!("{c:.1}"))
                    .collect::<Vec<_>>(),
                if st.violations().is_empty() {
                    if st.caveats().is_empty() {
                        "VALID".to_string()
                    } else {
                        format!("VALID ({})", st.caveats().join("; "))
                    }
                } else {
                    format!("INVALID: {}", st.violations().join("; "))
                }
            );
            all[ai].1.push(st);
            // SETTLE(bench-inter-arm-quiesce): a MEASUREMENT boundary, not a wait for a state.
            // Rep N's redb write amplification and compaction continue after its last ack, and
            // charging that tail to rep N+1 is how a benchmark reports a regression that is
            // really its own predecessor. No observable exists for "the store has finished
            // catching up" — that is precisely what a storage engine does not expose — so the
            // arms are separated by a fixed quiesce instead. On a slow machine 2 s covers
            // proportionally less of the tail, which biases the numbers PESSIMISTIC (worse, not
            // better), and every run prints its own verdict rather than comparing across runs.
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    println!("\n## Summary — median [min..max] over {} reps\n", cfg.reps);
    println!(
        "| Arm | acked msg/s | p50 | p95 | p99 | p99.9 | max | stalls >=1s | broker append mean | verdict |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for (label, runs) in &all {
        let rates: Vec<f64> = runs.iter().map(|r| r.rate).collect();
        let p50: Vec<f64> = runs.iter().map(|r| ms(r.p50)).collect();
        let p95: Vec<f64> = runs.iter().map(|r| ms(r.p95)).collect();
        let p99: Vec<f64> = runs.iter().map(|r| ms(r.p99)).collect();
        let p999: Vec<f64> = runs.iter().map(|r| ms(r.p999)).collect();
        let mx: Vec<f64> = runs.iter().map(|r| ms(r.max)).collect();
        let stalls: Vec<String> = runs.iter().map(|r| r.stalls_1s.to_string()).collect();
        let app: Vec<f64> = runs
            .iter()
            .map(|r| r.append_mean_ms)
            .filter(|v| v.is_finite())
            .collect();
        let bad: Vec<String> = runs
            .iter()
            .enumerate()
            .filter(|(_, r)| !r.violations().is_empty())
            .map(|(i, r)| format!("rep{i}: {}", r.violations().join(", ")))
            .collect();
        let mut notes: Vec<String> = runs
            .iter()
            .flat_map(RunStats::caveats)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        notes.dedup();
        println!(
            "| `{label}` | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            spread(&rates, ""),
            spread(&p50, "ms"),
            spread(&p95, "ms"),
            spread(&p99, "ms"),
            spread(&p999, "ms"),
            spread(&mx, "ms"),
            stalls.join(" / "),
            spread(&app, "ms"),
            if !bad.is_empty() {
                format!("INVALID — {}", bad.join("; "))
            } else if notes.is_empty() {
                "valid".to_string()
            } else {
                format!("valid — {}", notes.join("; "))
            }
        );
    }

    println!("\n## Per-node RSS and CPU at the end of the last arm\n");
    if let Some((_, runs)) = all.last() {
        if let Some(last) = runs.last() {
            for (i, e) in cl.endpoints.iter().enumerate() {
                println!(
                    "  {} rss={} cpu_in_arm={}",
                    e.id,
                    last.rss
                        .get(i)
                        .map_or("n/a".to_string(), |b| format!("{} MiB", b / 1_048_576)),
                    last.node_cpu
                        .get(i)
                        .map_or("n/a".to_string(), |c| format!("{c:.1}s"))
                );
            }
            println!(
                "  driver cpu_in_arm={:.1}s (the driver shares these cores with every broker)",
                last.driver_cpu
            );
            println!(
                "  subscriber-side deliveries in the last arm: {} (publishers completed {} in a \
                 {:.0}s measurement window)",
                last.delivered, last.completed, last.secs
            );
        }
    }

    // An arm that never worked in ANY rep is a broken harness, not a slow broker, and
    // must fail rather than render as a beautiful table of nothing. A SINGLE bad rep is
    // not fatal: it is reported as INVALID above and kept in the spread, which is the
    // whole point of running every arm three times.
    for (label, runs) in &all {
        assert!(
            runs.iter().any(|r| r.completed > 0),
            "arm {label} completed no publishes in any of {} reps",
            runs.len()
        );
    }
    cl.kill_all().await;
}

fn host_facts() -> String {
    let out = |args: &[&str]| {
        std::process::Command::new(args[0])
            .args(&args[1..])
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default()
    };
    format!(
        "{} / {} cores / {} MiB RAM",
        out(&["uname", "-srm"]),
        out(&["sysctl", "-n", "hw.ncpu"]),
        out(&["sysctl", "-n", "hw.memsize"])
            .parse::<u64>()
            .map_or("?".to_string(), |b| (b / 1_048_576).to_string())
    )
}

// ---------------------------------------------------------------------------
// Test 2 — the ADR 0061 head-of-line property, as a number.
// ---------------------------------------------------------------------------

/// ADR 0061 claims that one placement group whose followers are degraded no longer
/// parks the hub loop: "publishes to other groups' sessions, connects, and
/// subscribes are unaffected" (`docs/SIZING.md`'s summary of it). Issue #242 is where
/// that was a defect. This measures it instead of asserting it.
///
/// Construction — all of it computed from the broker's **own** placement hash before
/// anything is slowed, so class membership is this test's decision, not a coincidence:
///
/// - `MQTTD_BENCH_NODES` (default 5) nodes, replication factor 3, quorum 2. Five is
///   the minimum that admits both classes: with R=3 over 4 nodes every group excludes
///   exactly one node, so no group can exclude *both* slowed nodes; over 3, none can.
/// - Two nodes' **inbound peer bus** is delayed by `MQTTD_BENCH_SLOW_MS` per chunk
///   (`RelayCtl::slow`) — degraded, not dead: SWIM is UDP and untouched, so nothing
///   confirms a death and the appends simply crawl toward the 5 s RPC bound.
/// - **Victim** ids live in groups whose replica set is `{owner, slow, slow}`: their
///   quorum of 2 needs a slowed follower.
/// - **Control** ids live in groups whose replica set excludes both slowed nodes.
/// - Both classes are owned by the **same** node, so they share one hub loop. That
///   shared loop is what the claim is about.
///
/// Every participant is class-pinned, which is the part that makes the result mean
/// anything: the subscriber sessions (whose group carries the durable append), the
/// **publishers' own** client ids, and the **CONNECT probers' own** client ids. An
/// earlier version pinned only the sessions, and its control arm collapsed — because
/// the control publishers' own attach was slow, since their own ids happened to hash
/// into degraded groups. That measures "a client in a degraded group is slow", which
/// nobody disputes, not "a client in a healthy group is slow because another group
/// is". Publishers are also connected **before** the phase clock starts, so a slow
/// CONNECT is never charged to throughput.
///
/// Reported: control publish p50/p95/p99 and throughput, and CONNECT latency for both
/// classes, baseline vs during the fault, as **ratios** — which is why this result
/// survives a shared 8-core host far better than any absolute number here does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "macro-benchmark: minutes long, and only meaningful in --release"]
#[allow(clippy::too_many_lines)]
async fn degraded_group_does_not_delay_other_groups() {
    let mut cfg = Cfg::from_env();
    if std::env::var("MQTTD_BENCH_NODES").is_err() {
        cfg.nodes = 5; // R=3 over 5 nodes is what makes a control group possible
    }
    if std::env::var("MQTTD_BENCH_PREFLIGHT_SECS").is_err() {
        // A 5-node cluster's ADR 0043 P1 replica-group sweep plateaus below
        // current==tracked (see `preflight`); 60 s is enough to reach the plateau.
        cfg.preflight = Duration::from_secs(60);
    }
    let slow_ms: u64 = env_usize("MQTTD_BENCH_SLOW_MS", 1500) as u64;
    let phase = Duration::from_secs(env_usize("MQTTD_BENCH_PHASE_SECS", 25) as u64);
    // Deliberately light: this experiment wants a CLEAN baseline far below the host's
    // durable ceiling, not saturation — the result is a ratio, and a saturated baseline
    // has a multi-second tail of its own that swamps the effect being measured.
    let iso_pubs = env_usize("MQTTD_BENCH_ISO_PUBS", 2).max(1);
    let iso_window = env_usize("MQTTD_BENCH_ISO_WINDOW", 1).max(1);

    println!("\n# Head-of-line isolation under a degraded placement group (ADR 0061 / issue #242)");
    println!("host: {}", host_facts());
    println!(
        "load: {} nodes, R=3 quorum=2, slow={slow_ms}ms/chunk on two nodes' inbound bus, \
         phase={}s, profile={}",
        cfg.nodes,
        phase.as_secs(),
        profile()
    );

    let mut cl = cluster(&cfg).await;
    assert!(
        !cl.external,
        "the isolation experiment injects a link fault with the harness's own relay, \
         so it needs locally spawned nodes (unset MQTTD_BENCH_BROKERS)"
    );
    assert!(
        cl.endpoints.len() >= 5,
        "need >=5 nodes for a control group to exist under R=3 (every group over 4 nodes \
         excludes exactly one, over 3 nodes none)"
    );

    let ids: Vec<String> = cl.endpoints.iter().map(|e| e.id.clone()).collect();
    let ring = ring_over(&ids);
    let slowed = [ids[ids.len() - 2].clone(), ids[ids.len() - 1].clone()];

    // Enough class-pinned ids for every role, from one owner. The prober burns one id
    // per probe, so it needs the most.
    let probes = usize::try_from(phase.as_secs()).unwrap_or(25) * 4 + 20;
    let want = 2 + iso_pubs + probes; // subs + publishers + probers, per class
    let mut chosen: Option<(String, Vec<String>, Vec<String>)> = None;
    for owner in ids.iter().filter(|i| !slowed.contains(i)) {
        let mut victim = Vec::new();
        let mut control = Vec::new();
        for k in 0..2_000_000u32 {
            let cand = format!("iso-{}-{k}", std::process::id());
            let g = group_of(&cand);
            if ring.hrw_owner(g).0 != *owner {
                continue;
            }
            let rs: Vec<String> = ring.group_replica_set(g).into_iter().map(|n| n.0).collect();
            if slowed.iter().all(|s| rs.contains(s)) && victim.len() < want {
                victim.push(cand);
            } else if slowed.iter().all(|s| !rs.contains(s)) && control.len() < want {
                control.push(cand);
            }
            if victim.len() == want && control.len() == want {
                break;
            }
        }
        if victim.len() == want && control.len() == want {
            chosen = Some((owner.clone(), victim, control));
            break;
        }
    }
    let (owner, victim_ids, control_ids) = chosen.expect(
        "no owner had enough ids in BOTH a fully-degraded and a fully-healthy placement group",
    );
    let owner_idx = ids.iter().position(|i| *i == owner).expect("owner index");
    let owner_addr = cl.endpoints[owner_idx].client;
    let owner_health = cl.endpoints[owner_idx].health;
    println!(
        "  owner={owner}  slowed={slowed:?}\n  \
         victim class: replica set = owner + BOTH slowed nodes (quorum needs a slowed follower)\n  \
         control class: replica set EXCLUDES both slowed nodes\n  \
         per class: 2 subscriber sessions, {iso_pubs} publisher(s) with a window of \
         {iso_window}, {probes} CONNECT probe ids — all class-pinned, all owned by {owner}"
    );

    // Subscriber sessions, victim class then control class, on the owner.
    let mut subs = Vec::new();
    let mut topics = Vec::new();
    for (class, id) in victim_ids[..2]
        .iter()
        .map(|i| ("victim", i))
        .chain(control_ids[..2].iter().map(|i| ("control", i)))
    {
        let topic = format!("iso/{class}/{id}");
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if let Some((mut c, _)) =
                common::Client::connect_v311_within(owner_addr, id, false, Duration::from_secs(20))
                    .await
            {
                let ack = c.subscribe(1, &topic, QoS::AtLeastOnce).await;
                if ack.return_codes.iter().all(|rc| *rc != 0x80) {
                    subs.push(c);
                    topics.push(topic.clone());
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "isolation: {class} subscriber {id} never got a granted SUBACK"
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    // Warm every topic's interest through the owner (all publishes go there).
    for (i, t) in topics.iter().enumerate() {
        let warm = format!("iso-warm-{i}").into_bytes();
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if let Some((mut p, _)) = common::Client::connect_v311_within(
                owner_addr,
                &format!("iso-warmpub-{i}"),
                true,
                Duration::from_secs(20),
            )
            .await
            {
                p.publish(t, &warm, QoS::AtLeastOnce, Some(1), vec![]).await;
                let _ = p.recv_bounded(Duration::from_secs(10)).await;
            }
            let mut seen = false;
            while let common::Recv::Packet(pkt) =
                subs[i].recv_bounded(Duration::from_millis(700)).await
            {
                if let Packet::Publish(pb) = pkt {
                    if let Some(pkid) = pb.pkid {
                        subs[i].puback(pkid).await;
                    }
                    if pb.payload.as_ref() == warm.as_slice() {
                        seen = true;
                    }
                }
            }
            if seen {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "isolation: interest for {t} never converged"
            );
        }
    }
    // Keep the subscriber sessions drained for the whole experiment, so nothing
    // measured here is really an unacked-queue effect.
    let experiment_end = Instant::now() + phase * 4 + Duration::from_secs(120);
    let mut drains = Vec::new();
    for c in subs {
        drains.push(tokio::spawn(drainer(c, experiment_end)));
    }

    let mut phases: Vec<(&str, PhaseResult)> = Vec::new();
    for (name, slow) in [("baseline", false), ("degraded", true), ("healed", false)] {
        for id in &slowed {
            let idx = ids.iter().position(|i| i == id).unwrap();
            if slow {
                cl.nodes[idx].relay.slow(slow_ms);
            } else {
                cl.nodes[idx].relay.heal();
            }
        }
        if name == "healed" {
            // The backlog has drained when the owner's own metrics say every replica group is
            // back at its write floor — `mqttd_replication_min_actual >= write_floor` is the
            // series this whole file is built on, and it is the exact condition a 15 s sleep was
            // hoping for. Polling it means the recovery phase starts when recovery has HAPPENED,
            // so the measurement is of a healed cluster rather than of whatever state 15 s
            // produced on this particular machine.
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                let s = Scrape::of(owner_health).await;
                if s.get("mqttd_replication_min_actual") >= s.get("mqttd_replication_write_floor") {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "the replication backlog never drained after the heal: min_actual {} stayed \
                     below write_floor {} for 120s, so no 'healed' measurement is meaningful",
                    s.get("mqttd_replication_min_actual"),
                    s.get("mqttd_replication_write_floor")
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        let r = isolation_phase(
            owner_addr,
            owner_health,
            IsoRoles {
                victim_topics: &topics[..2],
                control_topics: &topics[2..],
                victim_pubs: &victim_ids[2..2 + iso_pubs],
                control_pubs: &control_ids[2..2 + iso_pubs],
                victim_probes: &victim_ids[2 + iso_pubs..],
                control_probes: &control_ids[2 + iso_pubs..],
                window: iso_window,
            },
            phase,
            name,
        )
        .await;
        println!(
            "  {name:<9} control pub: {:>6.1} msg/s p50 {:>7.1}ms p95 {:>8.1}ms p99 {:>8.1}ms \
             max {:>8.1}ms (completed {}, errors {}) | victim pub: {:>6.1} msg/s p50 {:>8.1}ms \
             p99 {:>9.1}ms max {:>9.1}ms (completed {}, errors {}) | CONNECT control p50 \
             {:>6.1}ms p99 {:>8.1}ms / victim p50 \
             {:>7.1}ms p99 {:>8.1}ms | hub dispatch(publish) p99<={:>6.1}ms lane {:.0}",
            r.control_rate,
            ms(r.control_p50),
            ms(r.control_p95),
            ms(r.control_p99),
            ms(r.control_max),
            r.control_done,
            r.control_errors,
            r.victim_rate,
            ms(r.victim_p50),
            ms(r.victim_p99),
            ms(r.victim_max),
            r.victim_done,
            r.victim_errors,
            ms(r.connect_control_p50),
            ms(r.connect_control_p99),
            ms(r.connect_victim_p50),
            ms(r.connect_victim_p99),
            r.dispatch_p99_ms,
            r.lane_jobs
        );
        phases.push((name, r));
    }
    for d in drains {
        d.abort();
    }

    let base = &phases[0].1;
    let deg = &phases[1].1;
    let healed = &phases[2].1;
    println!("\n## The isolation ratio (this is the result)\n");
    println!("| metric | baseline | degraded | healed | degraded/baseline |");
    println!("|---|---|---|---|---|");
    let ratio = |a: f64, b: f64| {
        if a > 0.0 {
            format!("{:.2}x", b / a)
        } else {
            "—".to_string()
        }
    };
    let row = |label: &str, a: f64, b: f64, c: f64, unit: &str| {
        println!(
            "| {label} | {a:.1}{unit} | {b:.1}{unit} | {c:.1}{unit} | {} |",
            ratio(a, b)
        );
    };
    row(
        "control publish p50",
        ms(base.control_p50),
        ms(deg.control_p50),
        ms(healed.control_p50),
        "ms",
    );
    row(
        "control publish p99",
        ms(base.control_p99),
        ms(deg.control_p99),
        ms(healed.control_p99),
        "ms",
    );
    row(
        "control publish max",
        ms(base.control_max),
        ms(deg.control_max),
        ms(healed.control_max),
        "ms",
    );
    row(
        "control publish throughput",
        base.control_rate,
        deg.control_rate,
        healed.control_rate,
        " msg/s",
    );
    row(
        "CONNECT p99, client in a HEALTHY group",
        ms(base.connect_control_p99),
        ms(deg.connect_control_p99),
        ms(healed.connect_control_p99),
        "ms",
    );
    row(
        "CONNECT p99, client in the DEGRADED group",
        ms(base.connect_victim_p99),
        ms(deg.connect_victim_p99),
        ms(healed.connect_victim_p99),
        "ms",
    );
    row(
        "victim publish p99",
        ms(base.victim_p99),
        ms(deg.victim_p99),
        ms(healed.victim_p99),
        "ms",
    );
    row(
        "victim publish throughput",
        base.victim_rate,
        deg.victim_rate,
        healed.victim_rate,
        " msg/s",
    );
    println!(
        "| hub dispatch(publish) p99 (bucket bound) | {:.1}ms | {:.1}ms | {:.1}ms | {} |",
        base.dispatch_p99_ms,
        deg.dispatch_p99_ms,
        healed.dispatch_p99_ms,
        ratio(base.dispatch_p99_ms, deg.dispatch_p99_ms)
    );
    println!(
        "\nThe victim arm degrading (or stopping) while the control arm does not IS the \
         head-of-line property; the same table with both arms degraded together would be the \
         defect issue #242 reported. `hub dispatch(publish)` staying flat is the mechanism: \
         the appends are off the loop (ADR 0061)."
    );

    // The fault must have bitten the victim class, or the run proves nothing at all.
    // "Bitten" includes stopping completely — the strongest form.
    let bit = deg.victim_p99 > base.victim_p99 * 2
        || deg.victim_rate < base.victim_rate / 2.0
        || deg.victim_errors > base.victim_errors;
    assert!(
        bit,
        "the fault did not bite: the victim group's own publishes were not measurably \
         degraded (baseline {:.1} msg/s p99 {:.1}ms vs degraded {:.1} msg/s p99 {:.1}ms), so \
         this run proves nothing about isolation — raise MQTTD_BENCH_SLOW_MS",
        base.victim_rate,
        ms(base.victim_p99),
        deg.victim_rate,
        ms(deg.victim_p99)
    );
    cl.kill_all().await;
}

/// Who plays which role in one phase — all ids class-pinned by the caller.
struct IsoRoles<'a> {
    victim_topics: &'a [String],
    control_topics: &'a [String],
    victim_pubs: &'a [String],
    control_pubs: &'a [String],
    victim_probes: &'a [String],
    control_probes: &'a [String],
    /// Publishes in flight per publisher.
    window: usize,
}

struct PhaseResult {
    control_rate: f64,
    control_p50: Duration,
    control_p95: Duration,
    control_p99: Duration,
    control_max: Duration,
    control_done: usize,
    control_errors: usize,
    victim_rate: f64,
    victim_p50: Duration,
    victim_p99: Duration,
    victim_max: Duration,
    victim_done: usize,
    victim_errors: usize,
    connect_control_p50: Duration,
    connect_control_p99: Duration,
    connect_victim_p50: Duration,
    connect_victim_p99: Duration,
    dispatch_p99_ms: f64,
    lane_jobs: f64,
}

/// One phase: victim and control publishers run concurrently through the same node for
/// `dur`, with a fresh-CONNECT prober per class alongside. Publishers are connected
/// **before** the clock starts.
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
async fn isolation_phase(
    owner_addr: SocketAddr,
    owner_health: SocketAddr,
    roles: IsoRoles<'_>,
    dur: Duration,
    tag: &str,
) -> PhaseResult {
    let before = Scrape::of(owner_health).await;

    // Pre-connect every publisher: under the fault a CONNECT can take seconds, and
    // charging that to throughput would measure the connect, not the publish path.
    let mut conns: Vec<(String, String, Option<common::Client>)> = Vec::new();
    for (i, id) in roles.victim_pubs.iter().enumerate() {
        let c = common::Client::connect_v311_within(owner_addr, id, true, Duration::from_secs(30))
            .await
            .map(|(c, _)| c);
        conns.push((
            id.clone(),
            roles.victim_topics[i % roles.victim_topics.len()].clone(),
            c,
        ));
    }
    let victim_conns = conns.len();
    for (i, id) in roles.control_pubs.iter().enumerate() {
        let c = common::Client::connect_v311_within(owner_addr, id, true, Duration::from_secs(30))
            .await
            .map(|(c, _)| c);
        conns.push((
            id.clone(),
            roles.control_topics[i % roles.control_topics.len()].clone(),
            c,
        ));
    }

    let stop = Instant::now() + dur;
    let window = roles.window;
    let mut victim = Vec::new();
    let mut control = Vec::new();
    for (n, (id, topic, conn)) in conns.into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            match conn {
                Some(c) => run_publisher(c, id, topic, QoS::AtLeastOnce, window, 256, stop).await,
                None => PubResult {
                    samples: Vec::new(),
                    errors: 1,
                    diagnosis: Some(format!("{id}: CONNECT never completed before the phase")),
                },
            }
        });
        if n < victim_conns {
            victim.push(handle);
        } else {
            control.push(handle);
        }
    }

    // CONNECT latency per class: fresh clean clients, one at a time, for the phase.
    let probe = |ids: Vec<String>| {
        tokio::spawn(async move {
            let mut lat = Vec::new();
            for id in ids {
                if Instant::now() >= stop {
                    break;
                }
                let t0 = Instant::now();
                if let Some((mut c, _)) = common::Client::connect_v311_within(
                    owner_addr,
                    &id,
                    true,
                    Duration::from_secs(30),
                )
                .await
                {
                    lat.push(t0.elapsed());
                    let _ = c.recv_bounded(Duration::from_millis(1)).await;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            lat
        })
    };
    let probe_control = probe(roles.control_probes.to_vec());
    let probe_victim = probe(roles.victim_probes.to_vec());

    let collect = |class: &str, rs: Vec<PubResult>| {
        let mut lat: Vec<Duration> = Vec::new();
        let mut errs = 0;
        for r in &rs {
            errs += r.errors;
            lat.extend(r.samples.iter().map(|(_, d)| *d));
        }
        for r in &rs {
            if let Some(d) = &r.diagnosis {
                println!("    !! {tag} {class}: {d}");
            }
        }
        lat.sort_unstable();
        (lat, errs)
    };
    let mut vr = Vec::new();
    for h in victim {
        vr.push(h.await.expect("victim publisher"));
    }
    let mut cr = Vec::new();
    for h in control {
        cr.push(h.await.expect("control publisher"));
    }
    let mut conn_c = probe_control.await.unwrap_or_default();
    let mut conn_v = probe_victim.await.unwrap_or_default();
    conn_c.sort_unstable();
    conn_v.sort_unstable();
    let (vlat, verr) = collect("victim", vr);
    let (clat, cerr) = collect("control", cr);
    let after = Scrape::of(owner_health).await;
    let secs = dur.as_secs_f64();

    PhaseResult {
        control_rate: clat.len() as f64 / secs,
        control_p50: pct(&clat, 0.50),
        control_p95: pct(&clat, 0.95),
        control_p99: pct(&clat, 0.99),
        control_max: clat.last().copied().unwrap_or_default(),
        control_done: clat.len(),
        control_errors: cerr,
        victim_rate: vlat.len() as f64 / secs,
        victim_p50: pct(&vlat, 0.50),
        victim_p99: pct(&vlat, 0.99),
        victim_max: vlat.last().copied().unwrap_or_default(),
        victim_done: vlat.len(),
        victim_errors: verr,
        connect_control_p50: pct(&conn_c, 0.50),
        connect_control_p99: pct(&conn_c, 0.99),
        connect_victim_p50: pct(&conn_v, 0.50),
        connect_victim_p99: pct(&conn_v, 0.99),
        dispatch_p99_ms: delta_quantile(
            &before,
            &after,
            "mqttd_hub_dispatch_seconds",
            Some("command=\"publish\""),
            0.99,
        ),
        lane_jobs: after.get("mqttd_append_lane_jobs"),
    }
}

// ---------------------------------------------------------------------------
// Test 3 — the multi-host lane, exercised as code.
// ---------------------------------------------------------------------------

/// The multi-host invocation is not prose: this test runs the driver's *external*
/// path — endpoint parsing, reachability, and the readiness preflight every valid
/// run must pass — against whatever `MQTTD_BENCH_BROKERS` names, and prints the
/// exact command the maintainer then runs for numbers. Without the variable it
/// skips loudly, so the lane is visibly UNRUN rather than silently absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an operator-provisioned multi-host cluster"]
async fn multi_host_preflight() {
    let cfg = Cfg::from_env();
    if std::env::var("MQTTD_BENCH_BROKERS").is_err() {
        // Fatal under CI: if a job ever runs this with `--ignored`, a preflight that reports
        // success without contacting a single broker is exactly the vacuous pass issue #260
        // is about — the lane must be visibly UNRUN, not quietly OK.
        crate::skip_locally_or_fail_in_ci!(
            "\n# Multi-host lane: NOT RUN (no MQTTD_BENCH_BROKERS)\n\n\
             This is the lane ADR 0048 §2 requires for any scaling or capacity claim, and it\n\
             has no numbers in this repository. To produce them, provision one broker per host\n\
             (see docs/benchmarks/DURABLE-PATH.md for the per-host environment), then from a\n\
             SEPARATE driver host:\n\n\
             \x20 MQTTD_BENCH_BROKERS=10.0.0.1:1883,10.0.0.2:1883,10.0.0.3:1883 \\\n\
             \x20 MQTTD_BENCH_HEALTH=10.0.0.1:9090,10.0.0.2:9090,10.0.0.3:9090 \\\n\
             \x20 MQTTD_BENCH_NODE_IDS=n0,n1,n2 \\\n\
             \x20 MQTTD_BENCH_SECS=60 MQTTD_BENCH_REPS=3 \\\n\
             \x20 cargo test --release -p mqttd --test durable_bench durable_path_floor \\\n\
             \x20   -- --ignored --nocapture\n\n\
             The driver is the same code either way: with these set, nothing is spawned."
        );
    }
    let mut cl = cluster(&cfg).await;
    println!(
        "multi-host preflight OK: {} brokers reachable, ready, replica groups current",
        cl.endpoints.len()
    );
    for e in &cl.endpoints {
        let s = Scrape::of(e.health).await;
        println!(
            "  {} write_floor={} min_actual={} sessions={}",
            e.id,
            s.get("mqttd_replication_write_floor"),
            s.get("mqttd_replication_min_actual"),
            s.get("mqttd_sessions")
        );
    }
    cl.kill_all().await;
}

/// A failed spawn must say so with the node's own log, not as a timeout.
#[allow(dead_code)]
fn diagnose(node: &ProcNode) -> String {
    format!("---- {} ----\n{}", node.id, log_tail(&node.log_path))
}

// ---------------------------------------------------------------------------
// Test 4 — the store floor underneath everything above.
// ---------------------------------------------------------------------------

/// **Why the durable numbers look the way they do.** Every durable append is one
/// `redb` write transaction at `Durability::Immediate`
/// (`mqtt-storage/src/persistent_log.rs`), and `redb` admits one write transaction at
/// a time — so a node's durable append rate has a hard ceiling of
/// `1 / commit_time`, no matter how many sessions, lanes or publishers push at it.
///
/// This measures that ceiling directly, on the same volume as the cluster runs on,
/// serially and then with 32 concurrent appenders (which is what would reveal
/// batching, if the log batched). Publishing it beside the end-to-end table is the
/// difference between "the broker does ~N msg/s" and "the broker does ~N msg/s
/// *because* one commit costs X here".
///
/// It also puts the macOS `fsync` caveat in proportion: compare X against the
/// `fsync(2)` cost on this volume (see `docs/benchmarks/DURABLE-PATH.md`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "micro-benchmark for the durable store; only meaningful in --release"]
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
async fn store_append_floor() {
    use mqtt_storage::persistent_log::PersistentLog;
    use mqtt_storage::repl::ReplicatedLog;
    use std::sync::Arc;

    let n = env_usize("MQTTD_BENCH_STORE_OPS", 300);
    let conc = env_usize("MQTTD_BENCH_STORE_CONC", 32);
    let reps = env_usize("MQTTD_BENCH_REPS", 3);
    println!("\n# Durable store append floor (redb, Durability::Immediate)");
    println!("host: {}", host_facts());
    println!("profile: {}", profile());

    let mut serial_rates = Vec::new();
    let mut serial_p50 = Vec::new();
    let mut conc_rates = Vec::new();
    let mut multi_rates = Vec::new();
    for rep in 0..reps {
        let tmp = common::TempDir::new();
        let log = Arc::new(
            PersistentLog::open(tmp.path().join("sessions.redb")).expect("open persistent log"),
        );
        let record = vec![b'x'; 256];

        // Serial: one append at a time — the commit cost itself.
        let mut lat = Vec::with_capacity(n);
        let t0 = Instant::now();
        for i in 0..n {
            let s = Instant::now();
            log.append(&format!("k/{i}"), record.clone())
                .await
                .expect("append");
            lat.push(s.elapsed());
        }
        let serial = t0.elapsed();
        lat.sort_unstable();

        // Concurrent: `conc` appenders at once. If the log batched commits, this rate
        // would rise; if it serializes, it will not.
        let t1 = Instant::now();
        let mut tasks = Vec::new();
        for c in 0..conc {
            let log = log.clone();
            let record = record.clone();
            let each = n / conc.max(1);
            tasks.push(tokio::spawn(async move {
                for i in 0..each {
                    log.append(&format!("c{c}/{i}"), record.clone())
                        .await
                        .expect("append");
                }
            }));
        }
        for t in tasks {
            t.await.expect("appender");
        }
        let concurrent = t1.elapsed();
        let conc_ops = (n / conc.max(1)) * conc;

        // Three SEPARATE log files on the same device, one serial appender each — the
        // single-host cluster's shape. If the aggregate scales with the number of files,
        // the limit is the storage engine's per-database commit, not the device.
        let stores = env_usize("MQTTD_BENCH_STORE_FILES", 3);
        let per = (n / 3).max(20);
        let t2 = Instant::now();
        let mut tasks = Vec::new();
        for f in 0..stores {
            let dir = tmp.path().join(format!("multi{f}"));
            std::fs::create_dir_all(&dir).expect("store dir");
            let record = record.clone();
            tasks.push(tokio::spawn(async move {
                let log = PersistentLog::open(dir.join("sessions.redb")).expect("open");
                for i in 0..per {
                    log.append(&format!("m/{i}"), record.clone())
                        .await
                        .expect("append");
                }
            }));
        }
        for t in tasks {
            t.await.expect("multi-store appender");
        }
        let multi = t2.elapsed();
        let multi_rate = (per * stores) as f64 / multi.as_secs_f64();

        println!(
            "  rep {rep}: serial {:>7.2} ms/append (p50 {:>7.2} p99 {:>7.2} max {:>7.2}) = {:>7.0} appends/s | \
             {conc} concurrent on ONE store: {:>7.0} appends/s | {stores} stores, one device: {:>7.0} appends/s",
            serial.as_secs_f64() * 1000.0 / n as f64,
            ms(pct(&lat, 0.50)),
            ms(pct(&lat, 0.99)),
            ms(*lat.last().unwrap()),
            n as f64 / serial.as_secs_f64(),
            conc_ops as f64 / concurrent.as_secs_f64(),
            multi_rate,
        );
        serial_rates.push(n as f64 / serial.as_secs_f64());
        serial_p50.push(ms(pct(&lat, 0.50)));
        conc_rates.push(conc_ops as f64 / concurrent.as_secs_f64());
        multi_rates.push(multi_rate);
    }
    println!("\n| measurement | median [min..max] over {reps} reps |");
    println!("|---|---|");
    println!("| serial append p50 | {} |", spread(&serial_p50, "ms"));
    println!(
        "| serial append rate | {} appends/s |",
        spread(&serial_rates, "")
    );
    println!(
        "| {conc}-way concurrent append rate (one store) | {} appends/s |",
        spread(&conc_rates, "")
    );
    println!(
        "| 3 stores on one device, serial each | {} appends/s |",
        spread(&multi_rates, "")
    );
    println!(
        "\nA concurrent rate that does not exceed the serial rate is the ceiling: one write \
         transaction at a time, no group commit on this path. If the 3-store rate is ~3x the \
         serial rate, that ceiling is the storage ENGINE's per-database commit, not the device."
    );
}

// ---------------------------------------------------------------------------
// Test 5 — the device barrier under the store.
// ---------------------------------------------------------------------------

/// **What actually bounds a durable append here.** `mqtt-storage`'s log commits at
/// `Durability::Immediate`; redb's macOS backend implements that as
/// `std::fs::File::sync_data()` (its `F_BARRIERFSYNC` path is the *eventual* one), and
/// Rust's `sync_data` on macOS is a **full device cache flush**, not `fsync(2)`.
///
/// So this measures `File::sync_data()` directly — the exact call one durable append
/// waits on — serially and then with several writers on separate files. If the aggregate
/// rate does not rise with the number of writers, the barrier is **per volume**, and
/// every store on the machine (every node's session log, replica store, lease store)
/// shares one budget. That is the single most important fact about the single-host
/// numbers in `docs/benchmarks/DURABLE-PATH.md`, and it is why the multi-host lane is
/// not a formality: on separate hosts each node has its own barrier budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "micro-benchmark for the host's durability barrier"]
#[allow(clippy::cast_precision_loss)]
async fn device_barrier_floor() {
    use std::io::Write;

    let n = env_usize("MQTTD_BENCH_BARRIER_OPS", 150);
    println!("\n# Durability barrier floor (std File::sync_data — what redb commits on)");
    println!("host: {}", host_facts());
    let tmp = common::TempDir::new();
    println!("volume under test: {}", tmp.path().display());

    let one = |writers: usize| {
        let root = tmp.path().to_path_buf();
        async move {
            let started = Instant::now();
            let mut tasks = Vec::new();
            for w in 0..writers {
                let path = root.join(format!("barrier-{writers}-{w}.bin"));
                tasks.push(tokio::task::spawn_blocking(move || {
                    let mut f = std::fs::File::create(&path).expect("create");
                    let buf = [b'q'; 4096];
                    let mut lat = Vec::with_capacity(n);
                    for _ in 0..n {
                        f.write_all(&buf).expect("write");
                        let t = Instant::now();
                        f.sync_data().expect("sync_data");
                        lat.push(t.elapsed());
                    }
                    lat
                }));
            }
            let mut lat = Vec::new();
            for t in tasks {
                lat.extend(t.await.expect("barrier writer"));
            }
            lat.sort_unstable();
            (
                (writers * n) as f64 / started.elapsed().as_secs_f64(),
                pct(&lat, 0.50),
                pct(&lat, 0.99),
            )
        }
    };

    println!("\n| concurrent writers (separate files) | aggregate barriers/s | p50 | p99 |");
    println!("|---|---|---|---|");
    let mut rates = Vec::new();
    for writers in [1usize, 3, 8] {
        let (rate, p50, p99) = one(writers).await;
        println!(
            "| {writers} | {rate:.0} | {:.2}ms | {:.2}ms |",
            ms(p50),
            ms(p99)
        );
        rates.push(rate);
    }
    println!(
        "\nA flat column is the finding: the flush is a per-VOLUME device operation, so \
         concurrency buys nothing and every node co-located on this disk draws from the \
         same ~{:.0}/s budget.",
        median_f64(rates)
    );
}
