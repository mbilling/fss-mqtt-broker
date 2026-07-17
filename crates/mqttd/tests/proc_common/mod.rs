//! Shared out-of-process harness (ADR 0044): spawned production-binary nodes,
//! per-node relays, the schedule state machine, and the acked-facts oracle.
//! Used by `cluster_proc.rs` (P1/P2 fault schedules) and `cluster_upgrade.rs`
//! (P3 two-binary rolling upgrade).
#![allow(dead_code)] // each test binary uses its own subset of the harness

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

use crate::common;

/// A seeded xorshift64 RNG — deterministic, matching the T2 sim (no `rand`).
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng((seed ^ 0x9E37_79B9_7F4A_7C15) | 1)
    }
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            lo
        } else {
            lo + self.next() % (hi - lo)
        }
    }
    pub fn pick(&mut self, len: usize) -> usize {
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
pub enum LinkMode {
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
pub struct RelayCtl {
    pub mode: watch::Sender<LinkMode>,
}

impl RelayCtl {
    pub fn sever(&self) {
        let _ = self.mode.send(LinkMode::Severed);
    }
    pub fn slow(&self, per_chunk_ms: u64) {
        let _ = self.mode.send(LinkMode::Slow(per_chunk_ms));
    }
    pub fn heal(&self) {
        let _ = self.mode.send(LinkMode::Healthy);
    }
}

/// One direction of a relayed connection: copy chunks, honoring the link mode
/// (delay under `Slow`; the caller's select breaks the pump on `Severed`).
pub async fn pump(
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
pub async fn spawn_relay(target: SocketAddr) -> (String, RelayCtl, AbortHandle) {
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
pub struct ProcNode {
    pub id: String,
    pub child: Option<tokio::process::Child>,
    pub data_dir: PathBuf,
    pub log_path: PathBuf,
    pub client_addr: SocketAddr,
    pub peer_bind: SocketAddr,
    pub swim_bind: String,
    pub health_addr: SocketAddr,
    /// Seed list handed to every (re)spawn: the OTHER nodes' gossip addresses.
    pub swim_seeds: String,
    /// The relay fronting the peer listener; its address is what gossip
    /// advertises (`MQTTD_PEER_ADVERTISE`).
    pub relay_addr: String,
    pub relay: RelayCtl,
    pub _relay_abort: AbortHandle,
    /// When set, the process runs under an OS-enforced `RLIMIT_FSIZE` of this
    /// many 512-byte blocks (`sh -c 'ulimit -f N; exec …'` — unprivileged): a
    /// real filesystem bound. A write crossing it gets `SIGXFSZ` from the
    /// kernel — the process dies exactly ON a write syscall, the harshest
    /// honest form of "the disk ran out mid-operation" (0018-T7).
    pub file_size_limit_blocks: Option<u64>,
    /// The broker binary this node runs — HEAD (`CARGO_BIN_EXE_mqttd`) by
    /// default; the P3 rolling-upgrade test points it at a BASELINE build and
    /// back, one node at a time (ADR 0044 P3 / ADR 0039).
    pub binary: PathBuf,
}

/// A fixed (test-only) gossip key so the mesh runs authenticated, as deployed.
pub const SWIM_KEY: &str = "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a";

/// Reserve a free TCP port by binding to :0 and dropping the listener. The
/// tiny release-to-reuse race is acceptable in tests (nothing else on the
/// runner races for ephemeral ports at this rate).
pub fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub fn free_udp_port() -> u16 {
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
    pub fn spawn(&mut self) {
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
                    .arg(&self.binary);
                c
            }
            None => tokio::process::Command::new(&self.binary),
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
    pub async fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }

    /// Stop the process the OPERATOR'S way: `SIGTERM` → the ADR 0019 graceful
    /// shutdown (drain, flush, SWIM leave), with a bounded wait and a `SIGKILL`
    /// escalation exactly as an init system would. The rolling-upgrade motion
    /// (ADR 0039 / ADR 0044 P3) stops nodes like this, not by crash.
    pub async fn terminate(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if let Some(pid) = child.id() {
            let _ = std::process::Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .status();
            if tokio::time::timeout(Duration::from_secs(15), child.wait())
                .await
                .is_ok()
            {
                return;
            }
        }
        let _ = child.start_kill();
        let _ = child.wait().await;
    }

    /// Whether the process has exited on its own (e.g. the kernel's `SIGXFSZ`
    /// on crossing a file-size bound — the disk-full death, 0018-T7). Reaps it.
    pub fn died(&mut self) -> bool {
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
    pub async fn readyz(&self) -> Option<(bool, usize, bool)> {
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
pub fn log_tail(path: &std::path::Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| format!("<unreadable: {e}>"));
    let start = text.len().saturating_sub(4096);
    text[start..].to_string()
}

/// Minimal HTTP GET (status line ignored beyond receipt; body returned) — the
/// health endpoint is plain HTTP/1.1 and this keeps the harness dependency-free.
pub async fn http_get(addr: SocketAddr, path: &str) -> Option<String> {
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
pub async fn build_topology(seed: u64, root: &std::path::Path) -> Vec<ProcNode> {
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
            binary: PathBuf::from(env!("CARGO_BIN_EXE_mqttd")),
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
pub struct RetainedSet {
    pub payload: Vec<u8>,
    pub acked: bool,
}

/// One persistent `QoS` 1 subscriber churned through connect/disconnect/resume.
pub struct Subscriber {
    pub id: String,
    pub topic: String,
    pub conn: Option<common::Client>,
    /// Which node index the live connection was made through (dies with it).
    pub via_node: usize,
    pub established: bool,
    pub received: BTreeSet<Vec<u8>>,
}

pub struct Proc {
    pub seed: u64,
    pub rng: Rng,
    pub trace: Vec<String>,
    pub nodes: Vec<ProcNode>,
    pub alive: Vec<bool>,
    pub subs: Vec<Subscriber>,
    /// Per topic: every payload whose PUBACK arrived — hard obligations all
    /// (0042-T9: acked means durable, cluster-wide).
    pub acked: BTreeMap<String, Vec<Vec<u8>>>,
    /// Per retained topic: the set history, newest last.
    pub retained: BTreeMap<String, Vec<RetainedSet>>,
    /// Nodes whose inbound bus is currently severed / slowed (healed at quiesce).
    pub severed: Vec<usize>,
    pub slowed: Vec<usize>,
    pub payload_counter: u64,
}

impl Proc {
    pub fn note(&mut self, event: String) {
        self.trace.push(event);
    }

    pub fn fail(&self, what: &str) -> ! {
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

    pub fn alive_nodes(&self) -> Vec<usize> {
        (0..self.nodes.len()).filter(|i| self.alive[*i]).collect()
    }

    pub fn pick_alive(&mut self) -> usize {
        let alive = self.alive_nodes();
        alive[self.rng.pick(alive.len())]
    }

    /// Connect (or resume) subscriber `i` through any alive node — placement is
    /// deliberately invisible out-of-process: a non-owner landing relays to the
    /// owner (ADR 0005), the production client path. Recovery-honesty truth
    /// tracking matches the in-process harness: `Present` once any connect
    /// succeeded, `Unknown` after a failed attempt (it may have claimed the
    /// session durably before timing out), `Absent` only on the very first try.
    pub async fn bring_subscriber_online(&mut self, i: usize, must: bool) {
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
    pub async fn drain_subscriber(&mut self, i: usize) {
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
    pub async fn publish_step(&mut self) {
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
    pub async fn retained_step(&mut self) {
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
    pub async fn churn_step(&mut self) {
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
    pub async fn kill_step(&mut self) {
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
    pub fn flap_step(&mut self) {
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
    pub fn slow_step(&mut self) {
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
    pub async fn restart_step(&mut self) {
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
    pub async fn wait_node_serving(&self, i: usize, timeout: Duration) -> bool {
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
    pub async fn quiesce(&mut self) {
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
pub async fn retained_seen(addr: SocketAddr, client_id: &str, topic: &str) -> Option<Vec<u8>> {
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
pub async fn wait_all_ready(nodes: &[ProcNode], seed: u64) {
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
pub fn proc_over(seed: u64, nodes: Vec<ProcNode>) -> Proc {
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
pub async fn establish_subscribers(proc: &mut Proc, n: usize) {
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
pub async fn oracle_acked_facts(proc: &mut Proc) {
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
