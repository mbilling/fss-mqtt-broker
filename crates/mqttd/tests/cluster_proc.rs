//! **Out-of-process** cluster harness over real spawned `mqttd` binaries
//! ([ADR 0044](../../docs/adr/0044-release-readiness-assurance.md) P1).
//!
//! Where `cluster_stress.rs` assembles durable nodes *in one process sharing
//! one binary*, this tier spawns the **compiled production binary**
//! (`CARGO_BIN_EXE_mqttd`) per node — real processes with real data dirs, real
//! TCP/MQTT listeners, real UDP gossip sockets, configured purely through the
//! documented `MQTTD_*` environment exactly as an operator would. What that
//! buys over the in-process tier:
//!
//! - **kill is `SIGKILL`**: the kernel reclaims the process — file handles,
//!   sockets, everything — with no in-process stand-in deciding what "crash"
//!   means; a restart reopens the surviving data dir cold (ADR 0018);
//! - **the config surface is the tested surface**: node assembly is `main.rs`
//!   itself, not a test-side mirror of it;
//! - **readiness is the operator's**: bring-up, quiesce, and restart gates all
//!   read `/readyz` (ADR 0020), never internal state;
//! - **placement is invisible**: clients attach through ANY node and the
//!   ADR 0005 owner-relay routes them — the production client path, black-box.
//!
//! Each node's peer listener is fronted by an **unprivileged TCP relay** in
//! the test process, advertised via `MQTTD_PEER_ADVERTISE`. The 0044-P2 fault
//! vocabulary drives it — sever (asymmetric partition / half-open link) and
//! slow (browned-out link) — and adds the faults only the OS can deliver:
//! `SIGKILL` mid-burst, the kernel's `SIGXFSZ` disk-full death on a real
//! `RLIMIT_FSIZE` bound (0018-T7), and kill/respawn flapping faster than
//! death confirmation (0007-T8).
//!
//! The schedule vocabulary and the **acked-facts oracle** are the ADR 0042
//! ones, ported: a payload is owed only from its PUBACK; a retained value
//! converges from its last acked set onward; every resume of an established
//! session must report `session_present = true` (ADR 0017). Timings differ
//! from the in-process tier — spawned nodes run the production SWIM defaults
//! (seconds-scale death confirmation), so schedules here are shorter and the
//! windows more generous; the seed reproduces the scenario, not the timing.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mqtt_cluster::invariants::{check_recovery_honesty, AttachReport, DurableTruth};
use mqtt_codec::{Packet, QoS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::AbortHandle;

/// Set to `Some(seed)` to run a single seed (e.g. to reproduce a reported failure).
const REPRO_SEED: Option<u64> = None;

/// One spawned cluster at a time: each test stands up 3 broker PROCESSES and
/// judges them against real-time windows (ack deadlines, bring-up bounds);
/// three clusters contending for one runner starve each other into flakes.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Spawned processes with production SWIM timings are expensive (~2 min per
/// seed), so the CI profile runs ONE seed; `MQTTD_PROC_SEEDS=N` widens the
/// sweep for the nightly tier (ADR 0044 P4).
const DEFAULT_SEEDS: u64 = 1;

fn seeds() -> Vec<u64> {
    if let Some(s) = REPRO_SEED {
        return vec![s];
    }
    let n = std::env::var("MQTTD_PROC_SEEDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEEDS);
    (0..n).collect()
}

/// A seeded xorshift64 RNG — deterministic, matching the T2 sim (no `rand`).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng((seed ^ 0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            lo
        } else {
            lo + self.next() % (hi - lo)
        }
    }
    fn pick(&mut self, len: usize) -> usize {
        usize::try_from(self.range(0, len as u64)).unwrap()
    }
}

// ---------------------------------------------------------------------------
// The link relay (ADR 0042 shape): an interceptable front for a node's peer
// listener, advertised to the mesh via MQTTD_PEER_ADVERTISE. It SURVIVES the
// node's kill/restart cycles (the relay's address is what peers know), so a
// restarted process re-binding the same peer port is reachable immediately.
// ---------------------------------------------------------------------------

/// One inbound peer link's condition (0044-P2 fault vocabulary).
#[derive(Clone, Copy, PartialEq, Eq)]
enum LinkMode {
    Healthy,
    /// Refuse new relayed connections and drop existing ones — an *asymmetric*
    /// bus fault (the node's own outbound dials stay up, SWIM keeps flowing):
    /// the half-open-link shape ADR 0037 T8 hardened the retained handoff
    /// against, now injected against a real process.
    Severed,
    /// Delay every relayed chunk — a browned-out (degraded, not dead) link:
    /// slow enough to stall replication RPC round-trips into their timeouts,
    /// alive enough that nothing detects a death.
    Slow(u64),
}

/// Controls one node's **inbound** peer-bus links.
#[derive(Clone)]
struct RelayCtl {
    mode: watch::Sender<LinkMode>,
}

impl RelayCtl {
    fn sever(&self) {
        let _ = self.mode.send(LinkMode::Severed);
    }
    fn slow(&self, per_chunk_ms: u64) {
        let _ = self.mode.send(LinkMode::Slow(per_chunk_ms));
    }
    fn heal(&self) {
        let _ = self.mode.send(LinkMode::Healthy);
    }
}

/// One direction of a relayed connection: copy chunks, honoring the link mode
/// (delay under `Slow`; the caller's select breaks the pump on `Severed`).
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mode: watch::Receiver<LinkMode>,
) {
    let mut buf = [0u8; 8192];
    loop {
        let Ok(n) = from.read(&mut buf).await else {
            return;
        };
        if n == 0 {
            return;
        }
        let current = *mode.borrow(); // copy out: the Ref must not span the await
        if let LinkMode::Slow(ms) = current {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

/// Spawn a relay in front of `target`; returns its public address + control.
async fn spawn_relay(target: SocketAddr) -> (String, RelayCtl, AbortHandle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (mode_tx, mode_rx) = watch::channel(LinkMode::Healthy);
    let ctl = RelayCtl { mode: mode_tx };
    let accept = tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                break;
            };
            if *mode_rx.borrow() == LinkMode::Severed {
                continue; // refuse while severed (the dial will retry)
            }
            let mut severed = mode_rx.clone();
            let mode = mode_rx.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(target).await else {
                    return;
                };
                let (in_r, in_w) = inbound.into_split();
                let (out_r, out_w) = outbound.into_split();
                tokio::select! {
                    () = pump(in_r, out_w, mode.clone()) => {}
                    () = pump(out_r, in_w, mode) => {}
                    // A sever mid-connection drops the relayed link on the floor.
                    _ = severed.wait_for(|s| *s == LinkMode::Severed) => {}
                }
            });
        }
    });
    (addr, ctl, accept.abort_handle())
}

// ---------------------------------------------------------------------------
// The spawned node: the production binary, configured through its documented
// environment, observed through its health endpoint.
// ---------------------------------------------------------------------------

/// One cluster member as a real OS process. Ports are fixed per node for the
/// test's lifetime so a restart reuses them (the relay target and the other
/// nodes' seed lists stay valid across kills).
struct ProcNode {
    id: String,
    child: Option<tokio::process::Child>,
    data_dir: PathBuf,
    log_path: PathBuf,
    client_addr: SocketAddr,
    peer_bind: SocketAddr,
    swim_bind: String,
    health_addr: SocketAddr,
    /// Seed list handed to every (re)spawn: the OTHER nodes' gossip addresses.
    swim_seeds: String,
    /// The relay fronting the peer listener; its address is what gossip
    /// advertises (`MQTTD_PEER_ADVERTISE`).
    relay_addr: String,
    relay: RelayCtl,
    _relay_abort: AbortHandle,
    /// When set, the process runs under an OS-enforced `RLIMIT_FSIZE` of this
    /// many 512-byte blocks (`sh -c 'ulimit -f N; exec …'` — unprivileged): a
    /// real filesystem bound. A write crossing it gets `SIGXFSZ` from the
    /// kernel — the process dies exactly ON a write syscall, the harshest
    /// honest form of "the disk ran out mid-operation" (0018-T7).
    file_size_limit_blocks: Option<u64>,
}

/// A fixed (test-only) gossip key so the mesh runs authenticated, as deployed.
const SWIM_KEY: &str = "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a";

/// Reserve a free TCP port by binding to :0 and dropping the listener. The
/// tiny release-to-reuse race is acceptable in tests (nothing else on the
/// runner races for ephemeral ports at this rate).
fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

impl ProcNode {
    /// Spawn (or respawn, after a kill) the broker process over this node's
    /// data dir and fixed ports. Stdout/stderr append to the node's log file,
    /// which a failing test names for post-mortem reading.
    fn spawn(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .expect("node log file");
        // With a file-size bound, run the binary under an OS-enforced
        // RLIMIT_FSIZE via the shell (`exec` keeps one process to kill/reap).
        let mut cmd = match self.file_size_limit_blocks {
            Some(blocks) => {
                let mut c = tokio::process::Command::new("/bin/sh");
                c.arg("-c")
                    .arg(format!("ulimit -f {blocks}; exec \"$0\""))
                    .arg(env!("CARGO_BIN_EXE_mqttd"));
                c
            }
            None => tokio::process::Command::new(env!("CARGO_BIN_EXE_mqttd")),
        };
        let child = cmd
            .env("MQTTD_NODE_ID", &self.id)
            .env("MQTTD_PLAINTEXT_BIND", self.client_addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_PEER_BIND", self.peer_bind.to_string())
            .env("MQTTD_PEER_ADVERTISE", &self.relay_addr)
            .env("MQTTD_SWIM_BIND", &self.swim_bind)
            .env("MQTTD_SWIM_SEEDS", &self.swim_seeds)
            .env("MQTTD_SWIM_KEY", SWIM_KEY)
            .env("MQTTD_DATA_DIR", &self.data_dir)
            .env("MQTTD_HEALTH_BIND", self.health_addr.to_string())
            .env("MQTTD_SHUTDOWN_GRACE", "0")
            .env("RUST_LOG", "info")
            .stdout(std::process::Stdio::from(log.try_clone().unwrap()))
            .stderr(std::process::Stdio::from(log))
            .spawn()
            .expect("spawn mqttd binary");
        self.child = Some(child);
    }

    /// `SIGKILL` the process and reap it — the kernel-mediated crash: no
    /// flushes, no goodbyes, file handles reclaimed by the OS (ADR 0044 P1).
    async fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }

    /// Whether the process has exited on its own (e.g. the kernel's `SIGXFSZ`
    /// on crossing a file-size bound — the disk-full death, 0018-T7). Reaps it.
    fn died(&mut self) -> bool {
        let Some(child) = self.child.as_mut() else {
            return true; // already killed/reaped
        };
        match child.try_wait() {
            Ok(Some(_)) => {
                self.child = None;
                true
            }
            _ => false,
        }
    }

    /// This node's `/readyz` snapshot: `(ready, members, lease_group_ready)`,
    /// or `None` while unreachable. Naive field scan — the shape is ours
    /// (`health.rs`), and a parse failure just reads as not-ready.
    async fn readyz(&self) -> Option<(bool, usize, bool)> {
        let body = http_get(self.health_addr, "/readyz").await?;
        let ready = body.contains("\"ready\":true");
        let members = body
            .split("\"members\":")
            .nth(1)
            .and_then(|s| s.split([',', '}']).next())
            .and_then(|s| s.parse::<usize>().ok())?;
        let lease = body.contains("\"lease_group_ready\":true");
        Some((ready, members, lease))
    }
}

/// The last ~4KB of a spawned node's log — printed on failure so a CI report
/// is self-diagnosing (the temp dirs, logs included, vanish with the unwind).
fn log_tail(path: &std::path::Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| format!("<unreadable: {e}>"));
    let start = text.len().saturating_sub(4096);
    text[start..].to_string()
}

/// Minimal HTTP GET (status line ignored beyond receipt; body returned) — the
/// health endpoint is plain HTTP/1.1 and this keeps the harness dependency-free.
async fn http_get(addr: SocketAddr, path: &str) -> Option<String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .ok()?
        .ok()?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut buf))
        .await
        .ok()?
        .ok()?;
    let text = String::from_utf8_lossy(&buf);
    text.split("\r\n\r\n").nth(1).map(str::to_string)
}

/// Build the three-node topology: fixed ports, per-node dirs under `root`,
/// relays fronting every peer listener, full cross-seeding. Nothing is
/// spawned yet.
async fn build_topology(seed: u64, root: &std::path::Path) -> Vec<ProcNode> {
    let names = ["a", "b", "c"];
    let mut peer_binds = Vec::new();
    let mut swim_binds = Vec::new();
    for _ in names {
        peer_binds.push(SocketAddr::from(([127, 0, 0, 1], free_tcp_port())));
        swim_binds.push(format!("127.0.0.1:{}", free_udp_port()));
    }
    let mut nodes = Vec::new();
    for (i, name) in names.iter().enumerate() {
        let id = format!("proc{seed}-{name}");
        let data_dir = root.join(name);
        std::fs::create_dir_all(&data_dir).expect("node dir");
        let (relay_addr, relay, relay_abort) = spawn_relay(peer_binds[i]).await;
        // The FOUNDER is the node with no seeds (main.rs's rule: it bootstraps
        // the lease group); the others seed off the whole topology. A restart
        // re-seeds every node (see `restart_step`) — a restarted founder joins
        // the existing group instead of re-bootstrapping.
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
        });
    }
    nodes
}

// ---------------------------------------------------------------------------
// The seeded schedule: the ADR 0042 vocabulary over spawned processes, with
// the same acked-facts obligations ledger.
// ---------------------------------------------------------------------------

/// One retained set the schedule issued and whether its PUBACK arrived.
#[derive(Clone)]
struct RetainedSet {
    payload: Vec<u8>,
    acked: bool,
}

/// One persistent `QoS` 1 subscriber churned through connect/disconnect/resume.
struct Subscriber {
    id: String,
    topic: String,
    conn: Option<common::Client>,
    /// Which node index the live connection was made through (dies with it).
    via_node: usize,
    established: bool,
    received: BTreeSet<Vec<u8>>,
}

struct Proc {
    seed: u64,
    rng: Rng,
    trace: Vec<String>,
    nodes: Vec<ProcNode>,
    alive: Vec<bool>,
    subs: Vec<Subscriber>,
    /// Per topic: every payload whose PUBACK arrived — hard obligations all
    /// (0042-T9: acked means durable, cluster-wide).
    acked: BTreeMap<String, Vec<Vec<u8>>>,
    /// Per retained topic: the set history, newest last.
    retained: BTreeMap<String, Vec<RetainedSet>>,
    /// Nodes whose inbound bus is currently severed / slowed (healed at quiesce).
    severed: Vec<usize>,
    slowed: Vec<usize>,
    payload_counter: u64,
}

impl Proc {
    fn note(&mut self, event: String) {
        self.trace.push(event);
    }

    fn fail(&self, what: &str) -> ! {
        for n in &self.nodes {
            eprintln!("---- log tail of {} ----\n{}", n.id, log_tail(&n.log_path));
        }
        panic!(
            "seed {}: {what} (re-run with REPRO_SEED = Some({}); log tails above)\n\
             schedule trace:\n  {}",
            self.seed,
            self.seed,
            self.trace.join("\n  ")
        );
    }

    fn alive_nodes(&self) -> Vec<usize> {
        (0..self.nodes.len()).filter(|i| self.alive[*i]).collect()
    }

    fn pick_alive(&mut self) -> usize {
        let alive = self.alive_nodes();
        alive[self.rng.pick(alive.len())]
    }

    /// Connect (or resume) subscriber `i` through any alive node — placement is
    /// deliberately invisible out-of-process: a non-owner landing relays to the
    /// owner (ADR 0005), the production client path. Recovery-honesty truth
    /// tracking matches the in-process harness: `Present` once any connect
    /// succeeded, `Unknown` after a failed attempt (it may have claimed the
    /// session durably before timing out), `Absent` only on the very first try.
    async fn bring_subscriber_online(&mut self, i: usize, must: bool) {
        let mut truth = if self.subs[i].established {
            DurableTruth::Present
        } else {
            DurableTruth::Absent
        };
        // Generous: production SWIM timings mean a resume inside a takeover
        // window waits out seconds-scale death confirmation plus re-election
        // and first-touch recovery.
        let deadline = Instant::now() + Duration::from_secs(90);
        let mut round = 0usize;
        loop {
            let alive = self.alive_nodes();
            let via = alive[round % alive.len()];
            round += 1;
            let addr = self.nodes[via].client_addr;
            if let Some((client, present)) = common::Client::connect_v311_within(
                addr,
                &self.subs[i].id,
                false,
                Duration::from_secs(10),
            )
            .await
            {
                let violations = check_recovery_honesty(
                    &self.subs[i].id,
                    truth,
                    AttachReport::SessionPresent(present),
                );
                if !violations.is_empty() {
                    let detail = violations
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("\n");
                    self.fail(&format!("recovery honesty on resume:\n{detail}"));
                }
                self.subs[i].conn = Some(client);
                self.subs[i].via_node = via;
                self.subs[i].established = true;
                self.note(format!(
                    "subscriber {} online via {} (present={present})",
                    self.subs[i].id, self.nodes[via].id
                ));
                return;
            }
            if matches!(truth, DurableTruth::Absent) {
                truth = DurableTruth::Unknown;
            }
            if Instant::now() >= deadline {
                if must {
                    self.fail("subscriber could not (re)connect within the deadline");
                }
                let id = self.subs[i].id.clone();
                self.note(format!(
                    "resume of {id} did not complete (legitimate mid-fault); staying offline"
                ));
                return;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    /// Drain everything queued on subscriber `i`'s live connection, acking
    /// each `QoS` 1 publish, until a short quiet window passes.
    async fn drain_subscriber(&mut self, i: usize) {
        loop {
            let Some(conn) = self.subs[i].conn.as_mut() else {
                return;
            };
            match conn.recv_bounded(Duration::from_millis(700)).await {
                common::Recv::Packet(Packet::Publish(p)) => {
                    self.subs[i].received.insert(p.payload.to_vec());
                    if let Some(pkid) = p.pkid {
                        if let Some(c) = self.subs[i].conn.as_mut() {
                            c.puback(pkid).await;
                        }
                    }
                }
                common::Recv::Packet(_) => {}
                common::Recv::Closed => {
                    self.subs[i].conn = None;
                    return;
                }
                common::Recv::Quiet => return,
            }
        }
    }

    /// One `QoS` 1 publish to a seeded subscriber's topic from a fresh publisher
    /// on a seeded alive node. The payload is owed ONLY if the PUBACK arrives.
    async fn publish_step(&mut self) {
        let s = self.rng.pick(self.subs.len());
        let node = self.pick_alive();
        self.payload_counter += 1;
        let payload = format!("m-{}-{}", self.seed, self.payload_counter).into_bytes();
        let topic = self.subs[s].topic.clone();
        let addr = self.nodes[node].client_addr;
        let pub_id = format!("pub-{}-{}", self.seed, self.payload_counter);

        let acked = async {
            let (mut publisher, _) =
                common::Client::connect_v311_within(addr, &pub_id, true, Duration::from_secs(8))
                    .await?;
            publisher
                .publish(&topic, &payload, QoS::AtLeastOnce, Some(7), vec![])
                .await;
            // Generous: an ack legitimately waits out a takeover window, which
            // runs on production SWIM timings here.
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let left = deadline.saturating_duration_since(Instant::now());
                match publisher.recv_bounded(left).await {
                    common::Recv::Packet(Packet::PubAck(a)) if a.pkid == 7 => return Some(()),
                    common::Recv::Packet(_) => {}
                    common::Recv::Quiet | common::Recv::Closed => return None,
                }
            }
        }
        .await
        .is_some();

        if acked {
            self.acked.entry(topic.clone()).or_default().push(payload);
            self.note(format!(
                "publish #{} to {topic} via {}: ACKED (obligation)",
                self.payload_counter, self.nodes[node].id,
            ));
        } else {
            self.note(format!(
                "publish #{} to {topic} via {}: unacked (no obligation)",
                self.payload_counter, self.nodes[node].id
            ));
        }
        self.drain_subscriber(s).await;
    }

    /// One retained set on a seeded retained topic, from a seeded alive node.
    async fn retained_step(&mut self) {
        let t = self.rng.range(0, 2);
        let topic = format!("rt/{}/{t}", self.seed);
        let node = self.pick_alive();
        self.payload_counter += 1;
        let payload = format!("r-{}-{}", self.seed, self.payload_counter).into_bytes();
        let addr = self.nodes[node].client_addr;
        let pub_id = format!("rpub-{}-{}", self.seed, self.payload_counter);

        let acked = async {
            let (mut publisher, _) =
                common::Client::connect_v311_within(addr, &pub_id, true, Duration::from_secs(8))
                    .await?;
            publisher
                .publish_full(&topic, &payload, QoS::AtLeastOnce, true, Some(9))
                .await;
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let left = deadline.saturating_duration_since(Instant::now());
                match publisher.recv_bounded(left).await {
                    common::Recv::Packet(Packet::PubAck(a)) if a.pkid == 9 => return Some(()),
                    common::Recv::Packet(_) => {}
                    common::Recv::Quiet | common::Recv::Closed => return None,
                }
            }
        }
        .await
        .is_some();

        self.retained
            .entry(topic.clone())
            .or_default()
            .push(RetainedSet { payload, acked });
        self.note(format!(
            "retained set #{} on {topic} via {}: {}",
            self.payload_counter,
            self.nodes[node].id,
            if acked { "ACKED" } else { "unacked" }
        ));
    }

    /// Churn a seeded subscriber: disconnect if online, resume if offline.
    async fn churn_step(&mut self) {
        let s = self.rng.pick(self.subs.len());
        if self.subs[s].conn.is_some() {
            self.drain_subscriber(s).await;
            if let Some(mut conn) = self.subs[s].conn.take() {
                conn.disconnect().await;
            }
            self.note(format!("subscriber {} disconnected", self.subs[s].id));
        } else {
            self.bring_subscriber_online(s, false).await;
            self.drain_subscriber(s).await;
        }
    }

    /// THE crash: `SIGKILL` a seeded alive node **while an acked publish burst
    /// is in flight through another** (0044-P2: the mid-write kill). The burst
    /// runs concurrently; the kill lands wherever the writes are — inside a
    /// quorum append, a replica fsync, an ack round-trip. Whatever the burst
    /// got ACKED before/through the crash is owed like any other ack.
    async fn kill_step(&mut self) {
        if self.alive_nodes().len() < 3 {
            return; // at most one node down at a time in this schedule
        }
        let victim = self.pick_alive();
        let s = self.rng.pick(self.subs.len());
        let via = *self
            .alive_nodes()
            .iter()
            .find(|i| **i != victim)
            .expect("two alive");
        let topic = self.subs[s].topic.clone();
        let addr = self.nodes[via].client_addr;
        let base = self.payload_counter + 1;
        self.payload_counter += 8;
        let seed = self.seed;
        let delay_ms = self.rng.range(50, 400);

        // The concurrent burst: 8 sequential QoS 1 publishes, each awaiting its
        // ack; returns every payload whose PUBACK arrived.
        let burst = tokio::spawn(async move {
            let mut acked = Vec::new();
            let Some((mut publisher, _)) = common::Client::connect_v311_within(
                addr,
                &format!("burst-{seed}-{base}"),
                true,
                Duration::from_secs(8),
            )
            .await
            else {
                return acked;
            };
            for k in 0..8u64 {
                let payload = format!("m-{seed}-{}", base + k).into_bytes();
                publisher
                    .publish(&topic, &payload, QoS::AtLeastOnce, Some(7), vec![])
                    .await;
                let deadline = Instant::now() + Duration::from_secs(15);
                let got = loop {
                    let left = deadline.saturating_duration_since(Instant::now());
                    match publisher.recv_bounded(left).await {
                        common::Recv::Packet(Packet::PubAck(a)) if a.pkid == 7 => break true,
                        common::Recv::Packet(_) => {}
                        common::Recv::Quiet | common::Recv::Closed => break false,
                    }
                };
                if got {
                    acked.push(payload);
                } else {
                    break; // connection wedged/closed: the rest never acked
                }
            }
            acked
        });

        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        self.nodes[victim].kill().await;
        self.alive[victim] = false;
        let id = self.nodes[victim].id.clone();
        for sub in &mut self.subs {
            if sub.conn.is_some() && sub.via_node == victim {
                sub.conn = None; // its connection died with the process
            }
        }
        let acked = burst.await.unwrap_or_default();
        let owed = acked.len();
        let topic = self.subs[s].topic.clone();
        self.acked.entry(topic.clone()).or_default().extend(acked);
        self.note(format!(
            "SIGKILLED {id} at {delay_ms}ms into a burst to {topic} ({owed}/8 acked → owed)"
        ));
    }

    /// A seeded asymmetric link fault: sever one alive node's inbound peer bus
    /// (healed by a later flap on the same node, or at quiesce).
    fn flap_step(&mut self) {
        let node = self.pick_alive();
        if self.severed.contains(&node) {
            self.nodes[node].relay.heal();
            self.severed.retain(|n| *n != node);
            self.note(format!("HEALED inbound bus of {}", self.nodes[node].id));
        } else {
            self.nodes[node].relay.sever();
            self.severed.push(node);
            self.note(format!("SEVERED inbound bus of {}", self.nodes[node].id));
        }
    }

    /// A seeded link brownout: delay every relayed chunk into one alive node —
    /// degraded, not dead; slow enough to stall replication round-trips, alive
    /// enough that membership never confirms a death.
    fn slow_step(&mut self) {
        let node = self.pick_alive();
        if self.slowed.contains(&node) {
            self.nodes[node].relay.heal();
            self.slowed.retain(|n| *n != node);
            self.note(format!("UNSLOWED inbound bus of {}", self.nodes[node].id));
        } else {
            self.nodes[node].relay.slow(250);
            self.slowed.push(node);
            self.note(format!(
                "SLOWED inbound bus of {} (250ms/chunk)",
                self.nodes[node].id
            ));
        }
    }

    /// Restart the killed process over its SURVIVING data dir and the same
    /// ports (the fronting relay keeps its advertised address valid). The
    /// redb stores reopen cold — the ADR 0018 crash path, kernel edition.
    async fn restart_step(&mut self) {
        let Some(dead) = (0..self.nodes.len()).find(|i| !self.alive[*i]) else {
            self.publish_step().await; // nothing to restart: schedule density
            return;
        };
        // The killed process's listening ports release on reap; a fast rebind
        // can still race a lingering socket, so the spawn is retried once.
        let id = self.nodes[dead].id.clone();
        // Re-seed off the whole topology: the restarted node (founder included)
        // must REJOIN the existing cluster, never re-bootstrap a rival one.
        self.nodes[dead].swim_seeds = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != dead)
            .map(|(_, n)| n.swim_bind.clone())
            .collect::<Vec<_>>()
            .join(",");
        self.nodes[dead].spawn();
        let ready = self.wait_node_serving(dead, Duration::from_secs(60)).await;
        if !ready {
            self.nodes[dead].kill().await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            self.nodes[dead].spawn();
            assert!(
                self.wait_node_serving(dead, Duration::from_secs(60)).await,
                "restarted node {id} never began serving"
            );
        }
        self.alive[dead] = true;
        self.note(format!("RESTARTED {id} over its surviving data dir"));
    }

    /// A restarted node is "serving" once its health endpoint answers and the
    /// mesh has re-admitted it (membership includes it again). Full readiness
    /// (lease group) is quiesce's business, not the schedule's.
    async fn wait_node_serving(&self, i: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some((_, members, _)) = self.nodes[i].readyz().await {
                if members >= self.alive_nodes().len() {
                    return true;
                }
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    /// Quiesce: every alive node reports the same full membership and a ready
    /// lease group on `/readyz` — the operator's own convergence signal.
    async fn quiesce(&mut self) {
        let expect = self.alive_nodes().len();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let mut all = true;
            for i in self.alive_nodes() {
                match self.nodes[i].readyz().await {
                    Some((_, members, lease)) => {
                        if members != expect || !lease {
                            all = false;
                        }
                    }
                    None => all = false,
                }
            }
            if all {
                self.note(format!("quiesced: {expect} members, lease group ready"));
                return;
            }
            if Instant::now() >= deadline {
                self.fail("survivors never quiesced on /readyz (membership + lease group)");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

/// Read the retained value a fresh clean-session subscriber sees on `addr`.
async fn retained_seen(addr: SocketAddr, client_id: &str, topic: &str) -> Option<Vec<u8>> {
    let (mut client, _) =
        common::Client::connect_v311_within(addr, client_id, true, Duration::from_secs(8)).await?;
    client.subscribe(1, topic, QoS::AtMostOnce).await;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match client.recv_bounded(left).await {
            common::Recv::Packet(Packet::Publish(p)) if p.topic == topic => {
                return Some(p.payload.to_vec())
            }
            common::Recv::Packet(_) => {}
            common::Recv::Quiet | common::Recv::Closed => return None,
        }
    }
}

/// Bring-up on the operator's signal: every spawned node's `/readyz` reports
/// full membership and a ready lease group.
async fn wait_all_ready(nodes: &[ProcNode], seed: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut all = true;
        for n in nodes {
            match n.readyz().await {
                Some((ready, members, _)) => {
                    if !ready || members != nodes.len() {
                        all = false;
                    }
                }
                None => all = false,
            }
        }
        if all {
            return;
        }
        if Instant::now() >= deadline {
            for n in nodes {
                eprintln!("---- log tail of {} ----\n{}", n.id, log_tail(&n.log_path));
            }
            panic!("seed {seed}: spawned cluster never became ready (log tails above)");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wrap a ready topology in the schedule/oracle state machine.
fn proc_over(seed: u64, nodes: Vec<ProcNode>) -> Proc {
    Proc {
        seed,
        rng: Rng::new(seed),
        trace: Vec::new(),
        alive: vec![true; nodes.len()],
        nodes,
        subs: Vec::new(),
        acked: BTreeMap::new(),
        retained: BTreeMap::new(),
        severed: Vec::new(),
        slowed: Vec::new(),
        payload_counter: 0,
    }
}

/// Establish `n` persistent subscribers: online, durably subscribed (the SUBACK
/// is durability-gated — retry until granted), and interest-warmed: a publish
/// from EVERY node to EVERY subscriber must deliver before the schedule starts
/// (cross-node interest gossip is eventually consistent; observable state, not
/// a sleep).
async fn establish_subscribers(proc: &mut Proc, n: usize) {
    let seed = proc.seed;
    for i in 0..n {
        proc.subs.push(Subscriber {
            id: format!("psub-{seed}-{i}"),
            topic: format!("pr/{seed}/{i}"),
            conn: None,
            via_node: 0,
            established: false,
            received: BTreeSet::new(),
        });
        proc.bring_subscriber_online(i, true).await;
        let topic = proc.subs[i].topic.clone();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let sub = proc.subs[i].conn.as_mut().unwrap();
            let ack = sub.subscribe(1, &topic, QoS::AtLeastOnce).await;
            if ack.return_codes.iter().all(|c| *c != 0x80) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "seed {seed}: durable SUBSCRIBE for sub {i} never granted"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    for node in 0..proc.nodes.len() {
        for i in 0..proc.subs.len() {
            let topic = proc.subs[i].topic.clone();
            let warm = format!("warm-{seed}-{node}-{i}").into_bytes();
            let addr = proc.nodes[node].client_addr;
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                if let Some((mut publisher, _)) = common::Client::connect_v311_within(
                    addr,
                    &format!("warm-pub-{seed}-{node}-{i}"),
                    true,
                    Duration::from_secs(20),
                )
                .await
                {
                    publisher
                        .publish(&topic, &warm, QoS::AtLeastOnce, Some(7), vec![])
                        .await;
                    let _ = publisher.recv_bounded(Duration::from_secs(2)).await;
                }
                proc.drain_subscriber(i).await;
                if proc.subs[i].received.contains(&warm) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "seed {seed}: interest warm-up from node {node} to sub {i} did not converge"
                );
            }
        }
    }
    proc.note("setup complete: subscribers online + subscribed + warmed".into());
}

/// Oracle part 1 — acked durability + recovery honesty: resume every
/// subscriber offline (so the resume replays its queue) and drain; every acked
/// payload for its topic must have been received at some point (dups legal).
async fn oracle_acked_facts(proc: &mut Proc) {
    for i in 0..proc.subs.len() {
        if proc.subs[i].conn.is_some() {
            proc.drain_subscriber(i).await;
            if let Some(mut conn) = proc.subs[i].conn.take() {
                conn.disconnect().await;
            }
        }
        proc.bring_subscriber_online(i, true).await;
        proc.drain_subscriber(i).await;
        proc.drain_subscriber(i).await; // settle a replay racing the window

        let topic = proc.subs[i].topic.clone();
        let owed = proc.acked.get(&topic).cloned().unwrap_or_default();
        let missing: Vec<String> = owed
            .iter()
            .filter(|p| !proc.subs[i].received.contains(*p))
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect();
        if !missing.is_empty() {
            proc.fail(&format!(
                "acked durability violated for {topic}: {} acked payload(s) never \
                 delivered: {missing:?}",
                missing.len()
            ));
        }
    }
}

/// One full seeded schedule over spawned processes: bring up a real 3-node
/// cluster, run the seeded workload with a mid-burst SIGKILL and a restart,
/// quiesce on `/readyz`, and run the acked-facts oracle black-box.
// One deliberately linear narrative — bring-up, schedule, quiesce, oracle —
// matching the in-process harness; splitting it would scatter the seed's story.
#[allow(clippy::too_many_lines)]
async fn run_schedule(seed: u64) {
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    establish_subscribers(&mut proc, 2).await;

    // The seeded schedule: ~10 steps with a SIGKILL at a seeded position and a
    // restart a couple of steps later — every seed exercises the whole
    // crash/recover cycle; the mix between them is seeded.
    let steps = proc.rng.range(8, 12);
    let kill_at = proc.rng.range(2, steps - 3);
    let restart_at = kill_at + proc.rng.range(2, 3);
    for step in 0..steps {
        if step == kill_at {
            proc.kill_step().await;
            continue;
        }
        if step == restart_at {
            proc.restart_step().await;
            continue;
        }
        match proc.rng.range(0, 100) {
            0..=34 => proc.publish_step().await,
            35..=57 => proc.retained_step().await,
            58..=73 => proc.churn_step().await,
            74..=87 => proc.flap_step(),
            _ => proc.slow_step(),
        }
    }
    let count = |needle: &str| proc.trace.iter().filter(|l| l.contains(needle)).count();
    eprintln!(
        "cluster_proc: seed {seed} schedule: {} publishes ({} owed), {} retained, \
         {} sigkills (mid-burst), {} restarts, {} severs, {} slows",
        count("publish #"),
        count("ACKED (obligation)"),
        count("retained set #"),
        count("SIGKILLED"),
        count("RESTARTED"),
        count("SEVERED"),
        count("SLOWED"),
    );

    // Heal any (P2-vocabulary) severs and quiesce on /readyz.
    for i in proc.alive_nodes() {
        proc.nodes[i].relay.heal();
    }
    proc.quiesce().await;

    // ---- The oracle (post-quiesce facts only, all black-box) ----

    // 1. Acked durability + recovery honesty.
    oracle_acked_facts(&mut proc).await;

    // 2. Retained convergence: every alive node serves the same value, never
    //    behind the last acked set; fan-out is eventually consistent, so poll.
    let mut probe = 0u64;
    for (topic, history) in proc.retained.clone() {
        let Some(last_acked) = history.iter().rposition(|r| r.acked) else {
            continue; // nothing was ever promised for this topic
        };
        let candidates: Vec<&Vec<u8>> = history[last_acked..].iter().map(|r| &r.payload).collect();

        let deadline = Instant::now() + Duration::from_secs(20);
        let (converged, last_seen) = loop {
            let mut values: Vec<(String, Option<Vec<u8>>)> = Vec::new();
            for i in proc.alive_nodes() {
                probe += 1;
                let observed = retained_seen(
                    proc.nodes[i].client_addr,
                    &format!("probe-{seed}-{probe}"),
                    &topic,
                )
                .await;
                values.push((proc.nodes[i].id.clone(), observed));
            }
            let all_good = values
                .iter()
                .all(|(_, v)| v.as_ref().is_some_and(|value| candidates.contains(&value)))
                && values.windows(2).all(|w| w[0].1 == w[1].1);
            if all_good {
                break (true, values);
            }
            if Instant::now() >= deadline {
                break (false, values);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        if !converged {
            let detail: Vec<String> = last_seen
                .iter()
                .map(|(node, v)| {
                    format!(
                        "{node}: {:?}",
                        v.as_ref().map(|p| String::from_utf8_lossy(p).into_owned())
                    )
                })
                .collect();
            proc.fail(&format!(
                "retained convergence violated for {topic}: nodes never converged \
                 on a value at or beyond the last acked set: {detail:?}"
            ));
        }
    }

    // Tear the cluster down (SIGKILL — the dirs are temp).
    for node in &mut proc.nodes {
        node.kill().await;
    }
}

/// The P1 skeleton test: real spawned binaries, a SIGKILL, a cold restart over
/// the surviving dir, and the acked-facts oracle — black-box end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn spawned_process_schedules_hold_acked_facts() {
    let _serial = SERIAL.lock().await;
    for seed in seeds() {
        run_schedule(seed).await;
    }
}

/// 0018-T7, un-deferred (ADR 0044 P2): the **disk-full crash mid-write**. One
/// node runs under a kernel-enforced `RLIMIT_FSIZE` (a real filesystem bound,
/// no privileges); acked 64KB publishes to an offline durable subscriber grow
/// every replica's store until the bounded node's next write crosses the limit
/// and the kernel delivers `SIGXFSZ` — death exactly ON a write syscall, the
/// harshest honest form of "the disk ran out mid-operation". The survivors
/// keep quorum (acks keep flowing); the restart reopens the possibly-torn dir
/// UNBOUNDED, redb must roll back any torn write on reopen, catch-up (ADR 0043
/// P1) back-fills what the node missed while dead, and every acked payload
/// must replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_disk_bound_crash_mid_write_loses_no_acked_fact() {
    let _serial = SERIAL.lock().await;
    let seed = 918;
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    // 16384 × 512B blocks = 8MB per file: roomy for formation, fatal under the
    // blast (each 64KB enqueue lands on every replica's store — R=3 on 3 nodes).
    nodes[2].file_size_limit_blocks = Some(16384);
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    establish_subscribers(&mut proc, 1).await;

    // Take the subscriber offline: every acked publish from here is a durable
    // offline enqueue — quorum-replicated bytes on disk, nothing in a session.
    proc.drain_subscriber(0).await;
    if let Some(mut conn) = proc.subs[0].conn.take() {
        conn.disconnect().await;
    }

    // Blast through the UNBOUNDED nodes until the kernel kills the bounded one.
    let topic = proc.subs[0].topic.clone();
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut publisher: Option<common::Client> = None;
    let mut i = 0u64;
    loop {
        if proc.nodes[2].died() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bounded node never crossed its file-size limit under the blast"
        );
        i += 1;
        let mut payload = format!("dd-{seed}-{i}-").into_bytes();
        payload.resize(64 * 1024, b'x');
        if publisher.is_none() {
            publisher = common::Client::connect_v311_within(
                proc.nodes[0].client_addr,
                &format!("dd-pub-{seed}"),
                true,
                Duration::from_secs(8),
            )
            .await
            .map(|(c, _)| c);
        }
        let Some(p) = publisher.as_mut() else {
            continue;
        };
        p.publish(&topic, &payload, QoS::AtLeastOnce, Some(7), vec![])
            .await;
        let ack_deadline = Instant::now() + Duration::from_secs(10);
        let mut closed = false;
        let got = loop {
            let left = ack_deadline.saturating_duration_since(Instant::now());
            match p.recv_bounded(left).await {
                common::Recv::Packet(Packet::PubAck(a)) if a.pkid == 7 => break true,
                common::Recv::Packet(_) => {}
                common::Recv::Closed => {
                    closed = true;
                    break false;
                }
                common::Recv::Quiet => break false,
            }
        };
        if closed {
            publisher = None; // reconnect on the next pass
        }
        if got {
            proc.acked.entry(topic.clone()).or_default().push(payload);
        }
    }
    proc.alive[2] = false;
    let owed = proc.acked.get(&topic).map_or(0, Vec::len);
    proc.note(format!(
        "bounded node died on a write (SIGXFSZ) after {owed} acked 64KB publishes"
    ));
    assert!(owed > 0, "vacuous: nothing was acked before the crash");

    // Restart UNBOUNDED over the surviving (possibly torn) dir and verify.
    proc.nodes[2].file_size_limit_blocks = None;
    proc.restart_step().await;
    proc.quiesce().await;
    oracle_acked_facts(&mut proc).await;
    eprintln!("cluster_proc: disk-bound crash held {owed} acked 64KB obligations");
    for node in &mut proc.nodes {
        node.kill().await;
    }
}

/// 0007-T8, un-deferred (ADR 0044 P2): **membership flap at SWIM-confusing
/// rates**. Three cycles of SIGKILL + IMMEDIATE respawn — faster than
/// suspicion can confirm a death, the fast-restart shape that produced the
/// 0043-P4 void-ack exhibit — with acked publishes flowing through the
/// survivors while the flapped node rejoins. Every ack collected anywhere in
/// the storm is a hard obligation; the oracle runs after the last cycle
/// settles.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rapid_kill_restart_flapping_loses_no_acked_fact() {
    let _serial = SERIAL.lock().await;
    let seed = 717;
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&nodes, seed).await;
    let mut proc = proc_over(seed, nodes);
    establish_subscribers(&mut proc, 2).await;

    for cycle in 0..3usize {
        let victim = 1 + (cycle % 2); // one node down at a time; the founder anchors
        proc.nodes[victim].kill().await;
        for sub in &mut proc.subs {
            if sub.conn.is_some() && sub.via_node == victim {
                sub.conn = None;
            }
        }
        // IMMEDIATE respawn over the surviving dir — no death-confirmation
        // wait, the exact window the fast-restart honesty fixes guard.
        proc.nodes[victim].spawn();
        let id = proc.nodes[victim].id.clone();
        proc.note(format!(
            "FLAP cycle {cycle}: SIGKILL + immediate respawn of {id}"
        ));
        // Acked load while the flapped node rejoins.
        proc.publish_step().await;
        proc.publish_step().await;
        // Re-admission before the next flap (never two nodes down at once). A
        // respawn that lost the port-rebind race is respawned once more.
        if !proc
            .wait_node_serving(victim, Duration::from_secs(30))
            .await
            && proc.nodes[victim].died()
        {
            proc.nodes[victim].spawn();
        }
        assert!(
            proc.wait_node_serving(victim, Duration::from_secs(60))
                .await,
            "flapped node {id} never re-admitted (cycle {cycle})"
        );
    }
    proc.quiesce().await;
    oracle_acked_facts(&mut proc).await;
    let count = |needle: &str| proc.trace.iter().filter(|l| l.contains(needle)).count();
    eprintln!(
        "cluster_proc: flap storm: 3 kill/respawn cycles, {} publishes ({} owed)",
        count("publish #"),
        count("ACKED (obligation)"),
    );
    for node in &mut proc.nodes {
        node.kill().await;
    }
}
