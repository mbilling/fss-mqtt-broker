//! Seeded whole-cluster **stress harness** over real durable nodes
//! ([ADR 0042](../../docs/adr/0042-durable-plane-stress-harness.md) T3).
//!
//! Where the T2 simulation drives the pure replication core deterministically,
//! this layer attacks the **whole durable plane as it actually runs** — openraft
//! lease consensus, SWIM membership, the peer mesh, quorum replication, the hub —
//! with everything wired exactly as production wires it (the node assembly
//! mirrors `durable_sessions.rs`, which mirrors `main.rs`).
//!
//! One seed composes a **fault schedule** interleaved with a **workload**
//! (`QoS` 1 publishes to persistent subscribers, retained mutations, resumes),
//! while an obligations ledger records only **acked facts**: a payload becomes
//! a delivery obligation only when its PUBACK arrived; a retained value becomes
//! the expected converged value only from its acked set onward. The fault
//! vocabulary (ADR 0042 §4):
//!
//! - an **owner kill** mid-workload — a real takeover of live sessions;
//! - a **restart** of the killed node over its SURVIVING data dir (half the
//!   seeds): the redb lease/replica/session stores reopen and feed recovery —
//!   the ADR 0018 crash path inside a live, still-faulted cluster;
//! - **asymmetric peer-bus link flaps** through a relay in front of each node's
//!   peer listener;
//! - **disk write-fault injection** at the hub's session-store seam (the shared
//!   [`common::FlakyStore`] fixture): while on, durable session writes fail
//!   terminally and the broker must WITHHOLD the corresponding acks;
//! - **brownout entry/exit** (ADR 0041 T5), driven exactly as the store-size
//!   watcher drives it — under brownout an offline enqueue is refused-but-acked,
//!   ADR 0041's documented trade, so such acks are recorded as non-obligations;
//! - **client churn** — disconnects and resumes riding lease handoffs.
//!
//! A separate test drives the **full-cluster stop/start**: every node crashes,
//! every node restarts over its surviving dir, and every acked fact must be
//! there afterwards — session present, payloads replayed, retained served.
//!
//! Under an active partition a gated ack HOLDS (the mesh-whole rule, found by
//! seed 4 of this vocabulary): an alive-but-unreachable peer may hold interest
//! this node cannot see, so "nobody is owed this" is only concluded on a whole
//! mesh — the same CP posture as the durable attach path. A publisher that
//! times out simply retries; an unacked publish is never an obligation.
//!
//! After the schedule: heal everything, **quiesce on observable state** (never
//! wall-clock guesses — membership counts and cross-node owner agreement), then
//! run the oracle:
//!
//! - **Acked durability**: every acked `QoS` 1 payload reaches its subscriber —
//!   live, or replayed on resume after the takeover (duplicates are legal;
//!   loss is the violation).
//! - **Recovery honesty** ([`check_recovery_honesty`]): every resume of a
//!   session the schedule created must report `session_present = true` — a
//!   fabricated clean session is the violation (ADR 0017).
//! - **Retained convergence** ([`check_retained_convergence`]): after the heal,
//!   every surviving node serves the **same** retained value, and that value is
//!   never behind the last acked set (later unacked sets may legitimately have
//!   committed — the candidate window runs from the last acked set onward).
//!
//! Every ack is a **hard obligation** — acked means durable, cluster-wide
//! (0042-T9). This harness's first schedules found six real defects, all faces
//! of that one claim, each first waived-and-counted here and then fixed:
//!
//! - **Exhibit ⑤** (seed 0): the cross-node `QoS` 1 forward was fire-and-forget
//!   — fixed by acked forwards (`PublishAcked`/`PublishAck`, proto 3): the
//!   publisher's ack waits for each interested peer's durability-gated answer,
//!   with sweep-tick retransmission and takeover re-routing.
//! - **Exhibit ⑥** (seed 0): the new owner acked publishes into the void before
//!   the inherited session's first re-attach — fixed by eager materialization
//!   (the takeover scan registers inherited sessions' durable subscriptions
//!   before their clients return, discovering keys **cluster-wide** via
//!   `ReplicaKeys`, since quorum appends mean no single replica holds them
//!   all), plus interest gossip on attach-recovery and the settle/re-route
//!   passes that re-deliver held publishes once state materializes.
//! - **Exhibit ⑦** (seed 2): the retained `PUBACK` preceded the authority
//!   commit — fixed: the ack gates on the commit (local commit completion or
//!   the commit-gated handoff ack), riding the mutation through re-queues.
//! - **Exhibit ⑧** (seed 2): retained state sat stably divergent after a
//!   takeover — a symptom of ⑥/⑦/⑩, gone with them.
//! - **Exhibit ⑨**: the SUBACK preceded (and ignored the failure of) the
//!   durable subscription write, so the durable session could claim **no
//!   subscriptions** while the client held a granted SUBACK — every downstream
//!   durability promise built on sand. Fixed: the SUBACK is durability-gated
//!   (failure codes + routing-state rollback; the client retries).
//! - **Exhibit ⑩** (the root cause underneath most observed losses): durable
//!   replication REPLIES routed through the hub command queue **deadlocked
//!   with on-loop appends** — the append awaited acks queued behind its own
//!   dispatch, so every hub-path durable write (offline enqueue, subscription
//!   persist, expiry write) failed with "no replication quorum" after the RPC
//!   timeout on a perfectly healthy cluster. The pre-T9 suites never saw it:
//!   their takeover tests drive the store directly. Fixed: reply frames bypass
//!   the hub queue, straight from the link pump to the durable plane.
//!
//! This layer is **stress, honestly labelled** (ADR 0042 §3): tokio's scheduler
//! and real I/O mean a seed reproduces the *scenario*, not a bit-identical
//! interleaving. Every failure prints the seed and the full schedule trace, and
//! the oracle asserts only post-quiesce facts — never mid-schedule timing, the
//! exact class that produced exhibit ①'s flake. `MQTTD_STRESS_SEEDS` widens the
//! sweep (the soak profile, ADR 0042 §5); `REPRO_SEED` pins one schedule.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use mqtt_cluster::durable_node::build_durable_node;
use mqtt_cluster::invariants::{
    check_recovery_honesty, check_retained_convergence, AttachReport, DurableTruth,
    RetainedSnapshot, Violation,
};
use mqtt_cluster::placement::{Placement, DEFAULT_REPLICAS};
use mqtt_cluster::swim::{Config as SwimConfig, Swim};
use mqtt_cluster::swim_auth::{SwimAuth, KEY_LEN};
use mqtt_cluster::{swim_driver, NodeId};
use mqtt_codec::{Packet, QoS};
use mqttd::Hub;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;

/// Set to `Some(seed)` to run a single seed (e.g. to reproduce a reported failure).
const REPRO_SEED: Option<u64> = None;

/// Seeds swept by default. Real nodes are expensive (SWIM bring-up, lease
/// election, real fault windows: ~1-2 min per seed), so the CI profile runs ONE
/// seed; `MQTTD_STRESS_SEEDS=N` widens the sweep for a soak run (ADR 0042 §5).
const DEFAULT_SEEDS: u64 = 1;

fn seeds() -> Vec<u64> {
    if let Some(s) = REPRO_SEED {
        return vec![s];
    }
    let n = std::env::var("MQTTD_STRESS_SEEDS")
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

/// Tight SWIM timings so discovery and death detection converge quickly.
fn swim_cfg() -> SwimConfig {
    SwimConfig {
        protocol_period_ms: 150,
        ack_timeout_ms: 60,
        suspicion_timeout_ms: 500,
        suspicion_min_timeout_ms: 200,
        suspicion_confirmations: 3,
        dead_ttl_ms: 5000,
        indirect_probes: 2,
        gossip_fanout: 8,
        gossip_multiplier: 4,
        awareness_max: 8,
    }
}

// ---------------------------------------------------------------------------
// The link relay: an interceptable front for a node's peer listener.
// ---------------------------------------------------------------------------

/// Controls one node's **inbound** peer-bus links: peers dial the relay (SWIM
/// advertises its address), which forwards to the real listener. Severing drops
/// every relayed connection and refuses new ones — an *asymmetric* bus fault
/// (the node's own outbound dials stay up, SWIM keeps flowing): exactly the
/// half-open-link shape ADR 0037 T8 hardened the retained handoff against.
#[derive(Clone)]
struct RelayCtl {
    severed: watch::Sender<bool>,
}

impl RelayCtl {
    fn sever(&self) {
        let _ = self.severed.send(true);
    }
    fn heal(&self) {
        let _ = self.severed.send(false);
    }
}

/// Spawn a relay in front of `target`; returns its public address + control.
async fn spawn_relay(target: SocketAddr) -> (String, RelayCtl, AbortHandle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (severed_tx, severed_rx) = watch::channel(false);
    let ctl = RelayCtl {
        severed: severed_tx,
    };
    let accept = tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                break;
            };
            if *severed_rx.borrow() {
                continue; // refuse while severed (the dial will retry)
            }
            let mut severed = severed_rx.clone();
            tokio::spawn(async move {
                let Ok(mut outbound) = TcpStream::connect(target).await else {
                    return;
                };
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                    // A sever mid-connection drops the relayed link on the floor.
                    _ = severed.wait_for(|s| *s) => {}
                }
            });
        }
    });
    (addr, ctl, accept.abort_handle())
}

// ---------------------------------------------------------------------------
// The durable node assembly (mirrors durable_sessions.rs / production main.rs),
// plus the relay in front of the peer listener.
// ---------------------------------------------------------------------------

struct StressNode {
    node_id: NodeId,
    placement: Arc<RwLock<Placement>>,
    swim_addr: String,
    client_addr: SocketAddr,
    relay: RelayCtl,
    /// Kept to observe lease-group readiness (`voter_count`) at bring-up.
    /// `None` after a kill: the plane holds the node's redb handles, and a
    /// restart over the same data dir needs them RELEASED (ADR 0042 T4).
    plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
    /// The node's on-disk state (redb lease/replica/session stores) — SURVIVES a
    /// kill, so a restart over the same dir exercises the ADR 0018 recovery path.
    data_dir: std::path::PathBuf,
    /// Toggles write-error injection on the hub's session-store seam
    /// (ADR 0042 T4): while `true`, durable session writes fail `Backend` and
    /// the broker must withhold the corresponding acks (fail closed).
    disk_faults: Arc<std::sync::atomic::AtomicBool>,
    /// The hub's command channel — the harness drives brownout entry/exit
    /// through it (ADR 0041 T5), exactly like the store-size watcher does.
    hub_tx: mpsc::UnboundedSender<mqttd::hub::HubCommand>,
    aborts: Vec<AbortHandle>,
}

impl StressNode {
    /// Crash the node: abort every task it spawned, so peers detect it dead,
    /// and release every redb handle so the data dir can reopen on a restart
    /// (the on-disk state itself SURVIVES — that is the point). The raft core
    /// task is not ours to abort, so it gets an explicit shutdown — the
    /// in-process stand-in for the OS reclaiming a crashed process's file
    /// handles.
    async fn kill(&mut self) {
        for a in &self.aborts {
            a.abort();
        }
        if let Some(plane) = self.plane.take() {
            let _ = plane.raft().shutdown().await;
        }
    }
}

// One linear node assembly, mirroring durable_sessions/main.rs — splitting it
// would hide which pieces a real node wires.
#[allow(clippy::too_many_lines)]
async fn start_stress_node(
    id: &str,
    swim_seeds: Vec<String>,
    data_dir: &std::path::Path,
) -> StressNode {
    let node_id = NodeId(id.to_string());
    let can_bootstrap = swim_seeds.is_empty();
    let placement = Arc::new(RwLock::new(Placement::new(
        node_id.clone(),
        DEFAULT_REPLICAS,
    )));

    let (store, durable_retained, plane, driver) = build_durable_node(
        node_id.clone(),
        placement.clone(),
        can_bootstrap,
        5, // every node votes in this 3-node cluster (ADR 0021)
        &std::collections::BTreeMap::new(),
        Some(data_dir), // on-disk state: a kill leaves it for the restart (T4)
        None,
    )
    .await;
    // The hub's session-store seam, wrapped for write-error injection (T4):
    // while a disk fault is on, durable session writes fail `Backend` and the
    // broker withholds the corresponding acks — fail closed, never a lie.
    let store = common::FlakyStore::wrap(store);
    let disk_faults = store.fail_writes.clone();
    let plane_observer = plane.clone();
    let (mut hub, hub_tx) =
        Hub::with_config_and_placement(node_id.clone(), store, Some(placement.clone()));
    hub.attach_durable_plane(plane);
    hub.attach_durable_retained(durable_retained);
    // The disk-backed retained CACHE, exactly as production wires it with a
    // data dir (main.rs): after a full-cluster stop/start every in-memory
    // cache is gone, and this reopened copy is what serves retained state
    // until fan-out/back-fill warm it again (ADR 0018 phase 4).
    hub.attach_retained_store(Box::new(
        mqtt_storage::persistent_retained::PersistentRetainedStore::open(
            data_dir.join("retained.redb"),
        )
        .expect("retained store opens"),
    ));
    let mut aborts = vec![
        tokio::spawn(hub.run()).abort_handle(),
        driver.abort_handle(),
    ];

    // MQTT client listener.
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();
    {
        let tx = hub_tx.clone();
        aborts.push(
            tokio::spawn(async move {
                loop {
                    let (stream, _) = client_listener.accept().await.unwrap();
                    tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
                }
            })
            .abort_handle(),
        );
    }

    // Peer listener, fronted by the relay; SWIM advertises the RELAY's address,
    // so inbound peer links are severable per node.
    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer_listener.local_addr().unwrap();
    aborts.push(
        tokio::spawn(mqttd::peer::serve_listener(
            peer_listener,
            node_id.clone(),
            hub_tx.clone(),
            None,
            None,
            Some(plane_observer.clone()),
        ))
        .abort_handle(),
    );
    let (relay_addr, relay, relay_abort) = spawn_relay(peer_addr).await;
    aborts.push(relay_abort);

    // SWIM membership driving the peer mesh.
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let swim_addr = socket.local_addr().unwrap().to_string();
    let swim = Swim::new(
        node_id.clone(),
        swim_addr.clone(),
        relay_addr,
        None,
        swim_cfg(),
        swim_seeds,
    );
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let auth = SwimAuth::new(&[0x5A; KEY_LEN]);
    aborts.push(
        tokio::spawn(swim_driver::run(
            socket,
            swim,
            Duration::from_millis(20),
            event_tx,
            Some(auth),
            None,
            None,
            std::future::pending(),
        ))
        .abort_handle(),
    );
    aborts.push(
        tokio::spawn(mqttd::cluster::maintain_peer_links(
            event_rx,
            node_id.clone(),
            hub_tx.clone(),
            None,
            Some(placement.clone()),
            None,
            Some(plane_observer.clone()),
        ))
        .abort_handle(),
    );

    StressNode {
        node_id,
        placement,
        swim_addr,
        client_addr,
        relay,
        plane: Some(plane_observer),
        data_dir: data_dir.to_path_buf(),
        disk_faults,
        hub_tx,
        aborts,
    }
}

// ---------------------------------------------------------------------------
// The seeded schedule: workload + faults, with an acked-facts obligations ledger.
// ---------------------------------------------------------------------------

/// One retained set the schedule issued: its payload, whether the PUBACK
/// arrived. An acked set is durably committed — the retained `PUBACK` gates on
/// the authority commit (0042-T9, exhibit ⑦) — whatever node it landed on.
#[derive(Clone)]
struct RetainedSet {
    payload: Vec<u8>,
    acked: bool,
}

/// One persistent `QoS` 1 subscriber the schedule churns through connect /
/// disconnect / resume, with its cumulative received-payload set.
struct Subscriber {
    id: String,
    topic: String,
    conn: Option<common::Client>,
    /// Which node index the live connection is on (dies with that node).
    on_node: usize,
    /// Whether any connect for this id has ever succeeded: from then on the
    /// durable session certainly exists and every resume must say so.
    established: bool,
    received: BTreeSet<Vec<u8>>,
}

struct Stress {
    seed: u64,
    rng: Rng,
    trace: Vec<String>,
    nodes: Vec<StressNode>,
    alive: Vec<bool>,
    subs: Vec<Subscriber>,
    /// Per topic: every payload whose PUBACK arrived — ALL of them HARD delivery
    /// obligations (0042-T9: acked means durable, cluster-wide — whichever node
    /// the publish landed on, whatever the takeover state).
    acked: BTreeMap<String, Vec<Vec<u8>>>,
    /// Per retained topic: the set history, newest last. The expected converged
    /// value is any entry from the last acked one onward (the retained PUBACK
    /// gates on the authority commit — 0042-T9, exhibit ⑦).
    retained: BTreeMap<String, Vec<RetainedSet>>,
    /// Nodes whose inbound bus is currently severed.
    severed: Vec<usize>,
    /// Per node: whether the harness has driven it into brownout (ADR 0041 T5).
    brownout: Vec<bool>,
    payload_counter: u64,
}

impl Stress {
    fn note(&mut self, event: String) {
        self.trace.push(event);
    }

    fn fail(&self, what: &str) -> ! {
        panic!(
            "seed {}: {what} (re-run with REPRO_SEED = Some({}))\nschedule trace:\n  {}",
            self.seed,
            self.seed,
            self.trace.join("\n  ")
        );
    }

    fn fail_violations(&self, what: &str, violations: &[Violation]) {
        if !violations.is_empty() {
            let detail = violations
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            self.fail(&format!("{what}:\n{detail}"));
        }
    }

    fn alive_nodes(&self) -> Vec<usize> {
        (0..self.nodes.len()).filter(|i| self.alive[*i]).collect()
    }

    fn pick_alive(&mut self) -> usize {
        let alive = self.alive_nodes();
        alive[self.rng.pick(alive.len())]
    }

    /// The node currently owning `client_id`'s placement group, per the first
    /// alive node's ring (post-quiesce the oracle checks they all agree).
    fn owner_of(&self, client_id: &str) -> Option<usize> {
        let ring = self.alive_nodes().first().copied()?;
        let owner = self.nodes[ring].placement.read().unwrap().owner(client_id);
        self.nodes.iter().position(|n| n.node_id == owner)
    }

    /// Connect (or resume) subscriber `i` on its current owner, retrying through
    /// lease handoffs. The recovery-honesty truth is derived from what the
    /// scenario actually knows: `Present` once any connect for this id has ever
    /// succeeded; `Absent` on the very first attempt; **`Unknown` after a failed
    /// attempt** — a timed-out attach may still have claimed the session durably
    /// before the deadline, so the retry may legitimately resume it (the exact
    /// epistemic state `DurableTruth::Unknown` exists for).
    /// `must` distinguishes the two callers: the post-heal ORACLE requires the
    /// resume to succeed (`true` — everything is healed, unavailability would be
    /// a liveness violation); a MID-SCHEDULE churn resume may legitimately fail
    /// (`false`) — a kill combined with an active severed bus can partition the
    /// two survivors, and refusing session recovery without a reachable quorum
    /// is exactly the CP behavior the plane promises (observed live in seed 5:
    /// both survivors candidate, neither electable). The subscriber then simply
    /// stays offline until a later resume.
    async fn bring_subscriber_online(&mut self, i: usize, must: bool) {
        let mut truth = if self.subs[i].established {
            DurableTruth::Present
        } else {
            DurableTruth::Absent
        };
        // Generous: a resume that lands inside a takeover window legitimately
        // waits out SWIM confirmation, raft re-election, lease reassignment and
        // the group's first-touch recovery, on a machine also running the soak.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let Some(owner) = self.owner_of(&self.subs[i].id) else {
                self.fail("no alive node resolves a placement owner");
            };
            if !self.alive[owner] {
                // The ring still names the dead node mid-handoff; wait it out.
                if !must && Instant::now() >= deadline {
                    let id = self.subs[i].id.clone();
                    self.note(format!(
                        "resume of {id} did not complete (owner never reassigned — \
                         legitimate under an active partition); staying offline"
                    ));
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert!(Instant::now() < deadline, "owner never reassigned");
                continue;
            }
            let addr = self.nodes[owner].client_addr;
            if let Some((client, present)) = common::Client::connect_v311_within(
                addr,
                &self.subs[i].id,
                false,
                Duration::from_secs(8),
            )
            .await
            {
                // Recovery honesty (ADR 0017): the broker must never disagree
                // with what the scenario knows about this session.
                let violations = check_recovery_honesty(
                    &self.subs[i].id,
                    truth,
                    AttachReport::SessionPresent(present),
                );
                self.fail_violations("recovery honesty on resume", &violations);
                self.subs[i].conn = Some(client);
                self.subs[i].on_node = owner;
                self.subs[i].established = true;
                self.note(format!(
                    "subscriber {} online on {} (present={present})",
                    self.subs[i].id, self.nodes[owner].node_id.0
                ));
                return;
            }
            // The attempt failed — but it may have gotten far enough to claim
            // the session durably. From here the truth is Unknown, not Absent.
            if matches!(truth, DurableTruth::Absent) {
                truth = DurableTruth::Unknown;
            }
            if Instant::now() >= deadline {
                assert!(
                    !must,
                    "subscriber could not (re)connect within the deadline"
                );
                let id = self.subs[i].id.clone();
                self.note(format!(
                    "resume of {id} did not complete (no reachable quorum — \
                     legitimate under an active partition); staying offline"
                ));
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Drain everything currently queued on subscriber `i`'s live connection
    /// (`PUBACK`ing each `QoS` 1 publish, as a well-behaved client would), until a
    /// short quiet window passes.
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
                    // Connection died (e.g. its node was killed): back offline.
                    self.subs[i].conn = None;
                    return;
                }
                common::Recv::Quiet => return, // drained
            }
        }
    }

    /// One `QoS` 1 publish to a seeded subscriber's topic from a fresh publisher on
    /// a seeded alive node. The payload becomes an obligation ONLY if the PUBACK
    /// arrives (an unacked publish may be delivered — duplicates are legal — but
    /// is never owed).
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
                common::Client::connect_v311_within(addr, &pub_id, true, Duration::from_secs(5))
                    .await?;
            publisher
                .publish(&topic, &payload, QoS::AtLeastOnce, Some(7), vec![])
                .await;
            // Generous: under 0042-T9 an ack legitimately waits out a takeover
            // window (SWIM confirmation + the successor's inherited-session scan +
            // the re-route grace) before releasing. A publish still unacked after
            // this is simply no obligation — safe, the publisher would retry.
            let deadline = Instant::now() + Duration::from_secs(12);
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
            // Every ack is a HARD obligation (0042-T9): acked means durable,
            // cluster-wide — whichever node the publish landed on, whatever the
            // takeover state. ONE documented exception (ADR 0041 T5): under
            // brownout an offline enqueue is REFUSED BUT ACKED — the explicit,
            // loudly-counted availability trade — so an ack for an OFFLINE
            // subscriber while any node is browned out is not owed.
            let brownout_window = self.brownout.iter().any(|b| *b) && self.subs[s].conn.is_none();
            if brownout_window {
                self.note(format!(
                    "publish #{} to {topic} via {}: ACKED (brownout window — \
                     ADR 0041 documented trade, not owed)",
                    self.payload_counter, self.nodes[node].node_id.0,
                ));
            } else {
                self.acked.entry(topic.clone()).or_default().push(payload);
                self.note(format!(
                    "publish #{} to {topic} via {}: ACKED (obligation)",
                    self.payload_counter, self.nodes[node].node_id.0,
                ));
            }
        } else {
            self.note(format!(
                "publish #{} to {topic} via {}: unacked (no obligation)",
                self.payload_counter, self.nodes[node].node_id.0
            ));
        }
        // Opportunistically drain online subscribers so live deliveries land.
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
                common::Client::connect_v311_within(addr, &pub_id, true, Duration::from_secs(5))
                    .await?;
            publisher
                .publish_full(&topic, &payload, QoS::AtLeastOnce, true, Some(9))
                .await;
            // Generous: the retained PUBACK gates on the authority commit
            // (0042-T9, exhibit ⑦), which may wait out a takeover window.
            let deadline = Instant::now() + Duration::from_secs(12);
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
            self.nodes[node].node_id.0,
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

    /// THE takeover: kill the node owning a seeded subscriber's session.
    async fn kill_step(&mut self) {
        let s = self.rng.pick(self.subs.len());
        let Some(owner) = self.owner_of(&self.subs[s].id) else {
            self.fail("no owner resolvable for the kill step");
        };
        if !self.alive[owner] || self.alive_nodes().len() < 3 {
            return; // already killed one — the schedule kills at most one node
        }
        self.nodes[owner].kill().await;
        self.alive[owner] = false;
        self.note(format!(
            "KILLED {} (owner of {})",
            self.nodes[owner].node_id.0, self.subs[s].id
        ));
        // Connections to the dead node are gone; mark those subscribers offline.
        for sub in &mut self.subs {
            if sub.conn.is_some() && sub.on_node == owner {
                sub.conn = None;
            }
        }
    }

    /// Restart the killed node over its SURVIVING data dir (ADR 0042 T4): the
    /// redb lease/replica/session stores reopen and feed recovery — the
    /// ADR 0018 crash/restart path, inside a live cluster. New ports are fine;
    /// SWIM re-keys the node by its stable id. A no-op when nothing is dead.
    async fn restart_step(&mut self) {
        let Some(dead) = (0..self.nodes.len()).find(|i| !self.alive[*i]) else {
            self.publish_step().await; // nothing to restart: schedule density
            return;
        };
        // `kill()` released the plane's redb handles; the hub task holding the
        // store handle was aborted then too. A short grace lets any in-flight
        // blocking apply drop its file handle before the same dir reopens (the
        // single-node restart test's teardown discipline).
        tokio::time::sleep(Duration::from_millis(200)).await;
        let id = self.nodes[dead].node_id.0.clone();
        let dir = self.nodes[dead].data_dir.clone();
        let seeds: Vec<String> = self
            .alive_nodes()
            .into_iter()
            .map(|i| self.nodes[i].swim_addr.clone())
            .collect();
        self.nodes[dead] = start_stress_node(&id, seeds, &dir).await;
        self.alive[dead] = true;
        self.severed.retain(|n| *n != dead); // the old relay died with the node
        self.note(format!("RESTARTED {id} over its surviving data dir"));
    }

    /// Toggle write-error injection on one alive node's session-store seam
    /// (ADR 0042 T4): while on, that node's durable session writes fail
    /// terminally and the broker must withhold the corresponding acks. The
    /// obligations ledger needs no special case — an unacked publish is no
    /// obligation, and an acked one proves the write path did not lie.
    fn disk_fault_step(&mut self) {
        let node = self.pick_alive();
        let flag = &self.nodes[node].disk_faults;
        let on = !flag.load(std::sync::atomic::Ordering::SeqCst);
        flag.store(on, std::sync::atomic::Ordering::SeqCst);
        self.note(format!(
            "DISK FAULTS {} on {}",
            if on { "injected" } else { "cleared" },
            self.nodes[node].node_id.0
        ));
    }

    /// Toggle brownout on one alive node (ADR 0041 T5), as the store-size
    /// watcher would on a watermark transition. Under brownout, offline
    /// enqueues are REFUSED BUT ACKED — ADR 0041's explicit, loudly-counted
    /// availability trade — so publishes acked while any node is browned out
    /// are recorded as non-obligations (see `publish_step`).
    fn brownout_step(&mut self) {
        let node = self.pick_alive();
        let on = !self.brownout[node];
        self.brownout[node] = on;
        let _ = self.nodes[node]
            .hub_tx
            .send(mqttd::hub::HubCommand::SetBrownout(on));
        self.note(format!(
            "BROWNOUT {} on {}",
            if on { "entered" } else { "lifted" },
            self.nodes[node].node_id.0
        ));
    }

    /// A seeded asymmetric link flap: sever one alive node's inbound peer bus
    /// (healed at quiesce, or by a later flap step on the same node).
    fn flap_step(&mut self) {
        let node = self.pick_alive();
        if self.severed.contains(&node) {
            self.nodes[node].relay.heal();
            self.severed.retain(|n| *n != node);
            self.note(format!(
                "HEALED inbound bus of {}",
                self.nodes[node].node_id.0
            ));
        } else {
            self.nodes[node].relay.sever();
            self.severed.push(node);
            self.note(format!(
                "SEVERED inbound bus of {}",
                self.nodes[node].node_id.0
            ));
        }
    }
}

/// Poll `cond` until it holds or `timeout` elapses (returns whether it held).
async fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while !cond() {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    true
}

/// Read the retained value a fresh clean-session subscriber sees on `addr`, or
/// `None` after a quiet window.
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

/// One full seeded schedule: bring up a 3-node durable cluster, run the seeded
/// workload + faults, heal, quiesce, and run the oracle.
// One deliberately linear narrative — schedule, heal, oracle — like the hub
// dispatch: splitting it would scatter the seed's story across helpers.
#[allow(clippy::too_many_lines)]
async fn run_schedule(seed: u64) {
    // Per-node on-disk state (ADR 0042 T4): a kill leaves the redb stores on
    // disk, and a restart over the same dir must recover them (ADR 0018).
    let disk = tempfile::tempdir().expect("tempdir");
    let dir = |n: &str| {
        let d = disk.path().join(n);
        std::fs::create_dir_all(&d).expect("node dir");
        d
    };
    let a = start_stress_node(&format!("st{seed}-a"), vec![], &dir("a")).await;
    let b = start_stress_node(&format!("st{seed}-b"), vec![a.swim_addr.clone()], &dir("b")).await;
    let c = start_stress_node(&format!("st{seed}-c"), vec![a.swim_addr.clone()], &dir("c")).await;
    let nodes = vec![a, b, c];

    // Bring-up: full membership everywhere (the lease group follows; attaches
    // already wait for leases per ADR 0017).
    assert!(
        wait_until(Duration::from_secs(30), || {
            nodes
                .iter()
                .all(|n| n.placement.read().unwrap().member_count() == 3)
        })
        .await,
        "seed {seed}: cluster never formed"
    );
    assert!(
        wait_until(Duration::from_secs(30), || {
            nodes
                .iter()
                .all(|n| n.plane.as_ref().is_some_and(|p| p.voter_count() == 3))
        })
        .await,
        "seed {seed}: lease group never reached full membership"
    );

    let mut stress = Stress {
        seed,
        rng: Rng::new(seed),
        trace: Vec::new(),
        alive: vec![true; nodes.len()],
        nodes,
        subs: Vec::new(),
        acked: BTreeMap::new(),
        retained: BTreeMap::new(),
        severed: Vec::new(),
        brownout: vec![false; 3],
        payload_counter: 0,
    };

    // Three persistent subscribers, each on its own topic, established online
    // (their durable sessions + subscriptions exist from here on).
    for i in 0..3 {
        stress.subs.push(Subscriber {
            id: format!("sub-{seed}-{i}"),
            topic: format!("st/{seed}/{i}"),
            conn: None,
            on_node: 0,
            established: false,
            received: BTreeSet::new(),
        });
        stress.bring_subscriber_online(i, true).await;
        let topic = stress.subs[i].topic.clone();
        // The SUBACK is durability-gated (0042 T9): a failure code means the
        // durable subscription write could not reach quorum yet — retry until
        // granted, as a real client would.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let sub = stress.subs[i].conn.as_mut().unwrap();
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
    // Interest-propagation warm-up (observable state, not a sleep): a QoS 1
    // publish from EVERY node to EVERY subscriber must deliver before the
    // schedule starts. A SUBACK alone proves only the subscribed node's routing
    // state — cross-node interest gossip is eventually consistent, and a publish
    // racing it is silently unroutable (a noted semantic gap, not what this
    // harness stresses). Warm payloads are delivered but never become
    // obligations.
    for n in 0..stress.nodes.len() {
        for i in 0..3 {
            let topic = stress.subs[i].topic.clone();
            let warm = format!("warm-{seed}-{n}-{i}").into_bytes();
            let addr = stress.nodes[n].client_addr;
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                // Generous CONNECT deadline: a fresh clean-start id's CONNACK
                // gates on a durable discard, whose group may need a first-ever
                // lease grant (reconcile-driven, multi-second) — a real cold-path
                // latency this warm-up absorbs so the schedule never pays it.
                if let Some((mut publisher, _)) = common::Client::connect_v311_within(
                    addr,
                    &format!("warm-pub-{seed}-{n}-{i}"),
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
                stress.drain_subscriber(i).await;
                if stress.subs[i].received.contains(&warm) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "seed {seed}: interest warm-up from node {n} to sub {i} did not converge"
                );
            }
        }
    }
    stress.note("setup complete: 3 subscribers online + subscribed + warmed".into());

    // The seeded schedule: ~14 steps, one kill at a seeded position, flaps and
    // churn throughout.
    let steps = stress.rng.range(12, 17);
    let kill_at = stress.rng.range(3, steps - 2);
    // Half the seeds RESTART the killed node a few steps later (ADR 0042 T4):
    // its data dir survived the kill, so the restart drives the ADR 0018
    // crash/restart recovery inside a live, still-faulted cluster.
    let restart_at = if stress.rng.range(0, 2) == 0 {
        Some(kill_at + stress.rng.range(2, 4))
    } else {
        None
    };
    for step in 0..steps {
        if step == kill_at {
            stress.kill_step().await;
            continue;
        }
        if Some(step) == restart_at {
            stress.restart_step().await;
            continue;
        }
        match stress.rng.range(0, 100) {
            0..=39 => stress.publish_step().await,
            40..=57 => stress.retained_step().await,
            58..=74 => stress.churn_step().await,
            75..=84 => stress.flap_step(),
            85..=89 => stress.restart_step().await,
            90..=94 => stress.disk_fault_step(),
            _ => stress.brownout_step(),
        }
    }
    // A compact composition line per seed, so a green sweep still shows what
    // the schedules exercised (kills, restarts, disk faults, brownouts...).
    let count = |needle: &str| stress.trace.iter().filter(|l| l.contains(needle)).count();
    eprintln!(
        "cluster_stress: seed {seed} schedule: {} publishes ({} owed), {} retained, \
         {} kills, {} restarts, {} flaps, {} disk-fault toggles, {} brownout toggles",
        count("publish #"),
        count("ACKED (obligation)"),
        count("retained set #"),
        count("KILLED"),
        count("RESTARTED"),
        count("SEVERED"),
        count("DISK FAULTS"),
        count("BROWNOUT"),
    );
    // Clear injected faults before quiesce: the oracle judges the HEALED
    // cluster (disk faults and brownout are conditions, not obligations).
    for i in 0..stress.nodes.len() {
        stress.nodes[i]
            .disk_faults
            .store(false, std::sync::atomic::Ordering::SeqCst);
        if stress.brownout[i] {
            let _ = stress.nodes[i]
                .hub_tx
                .send(mqttd::hub::HubCommand::SetBrownout(false));
            stress.brownout[i] = false;
        }
    }

    // Heal every flap and quiesce on observable state: survivors agree the dead
    // node is gone and agree on every subscriber's owner.
    for i in stress.alive_nodes() {
        stress.nodes[i].relay.heal();
    }
    stress.note("heal + quiesce".into());
    let survivors = stress.alive_nodes();
    let expect_members = survivors.len();
    {
        let nodes = &stress.nodes;
        assert!(
            wait_until(Duration::from_secs(30), || {
                survivors
                    .iter()
                    .all(|i| nodes[*i].placement.read().unwrap().member_count() == expect_members)
            })
            .await,
            "seed {seed}: survivors never agreed on membership after the kill"
        );
        let sub_ids: Vec<String> = stress.subs.iter().map(|s| s.id.clone()).collect();
        assert!(
            wait_until(Duration::from_secs(20), || {
                sub_ids.iter().all(|id| {
                    let owners: BTreeSet<String> = survivors
                        .iter()
                        .map(|i| nodes[*i].placement.read().unwrap().owner(id).0)
                        .collect();
                    owners.len() == 1
                })
            })
            .await,
            "seed {seed}: survivors never converged on session owners"
        );
    }

    // ---- The oracle (post-quiesce facts only) ----

    // 1. Acked durability + recovery honesty: resume every subscriber (offline
    //    first, so the resume replays its queue) and drain; every acked payload
    //    for its topic must have been received at some point (dups legal).
    for i in 0..stress.subs.len() {
        if stress.subs[i].conn.is_some() {
            stress.drain_subscriber(i).await;
            if let Some(mut conn) = stress.subs[i].conn.take() {
                conn.disconnect().await;
            }
        }
        stress.bring_subscriber_online(i, true).await;
        stress.drain_subscriber(i).await;
        // A replay that raced the drain window settles with one more pass.
        stress.drain_subscriber(i).await;

        let topic = stress.subs[i].topic.clone();
        let owed = stress.acked.get(&topic).cloned().unwrap_or_default();
        let missing: Vec<String> = owed
            .iter()
            .filter(|p| !stress.subs[i].received.contains(*p))
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect();
        if !missing.is_empty() {
            stress.fail(&format!(
                "acked durability violated for {topic}: {} acked payload(s) never \
                 delivered: {missing:?}",
                missing.len()
            ));
        }
    }

    // 2. Retained convergence: every survivor serves the same value, and it is
    //    never behind the last acked set — a retained PUBACK now gates on the
    //    authority commit (0042-T9, exhibit ⑦ fixed), so an acked set is durable
    //    whatever happened to its landing node; later unacked sets may
    //    legitimately have committed too. Fan-out and back-fill are eventually
    //    consistent, so the oracle POLLS to a deadline instead of reading once.
    let mut probe = 0u64;
    for (topic, history) in stress.retained.clone() {
        let Some(last_acked) = history.iter().rposition(|r| r.acked) else {
            continue; // nothing was ever promised for this topic
        };
        let candidates: Vec<&Vec<u8>> = history[last_acked..].iter().map(|r| &r.payload).collect();

        let deadline = Instant::now() + Duration::from_secs(12);
        let (converged, last_seen) = loop {
            let mut values: Vec<(String, Option<Vec<u8>>)> = Vec::new();
            for i in stress.alive_nodes() {
                probe += 1;
                let observed = retained_seen(
                    stress.nodes[i].client_addr,
                    &format!("probe-{seed}-{probe}"),
                    &topic,
                )
                .await;
                values.push((stress.nodes[i].node_id.0.clone(), observed));
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
            stress.fail(&format!(
                "retained convergence violated for {topic}: survivors never \
                 converged on a value at or beyond the last acked set: {detail:?}"
            ));
        }
        // The catalog checker states the cross-node agreement claim once.
        let named: Vec<(String, RetainedSnapshot)> = last_seen
            .iter()
            .map(|(node, v)| {
                let mut snap = RetainedSnapshot::new();
                snap.insert(topic.clone(), ((0, 0), v.clone().unwrap_or_default()));
                (node.clone(), snap)
            })
            .collect();
        let named_refs: Vec<(&str, RetainedSnapshot)> =
            named.iter().map(|(n, s)| (n.as_str(), s.clone())).collect();
        let violations = check_retained_convergence(&named_refs);
        stress.fail_violations("retained convergence", &violations);
    }
    // Tear the cluster down so the next seed starts clean.
    for node in &mut stress.nodes {
        node.kill().await;
    }
}

/// Full-cluster stop/start (ADR 0042 T4, the ADR 0018 recovery path at cluster
/// scale): every node crashes, every node restarts over its surviving data dir,
/// and everything ACKED before the outage must be there after it — the durable
/// session resumes `present = true`, its acked payloads replay, and the acked
/// retained value is served. This is the "datacenter power cycle": no survivor
/// carries state across in memory; disk is all there is.
// One linear story — establish, ack, outage, restart, verify — like the
// seeded schedule; splitting it would scatter the acked facts from the checks.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_full_cluster_stop_start_recovers_every_acked_fact() {
    if std::env::var("MQTTD_STRESS_LOG").is_ok() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }
    let disk = tempfile::tempdir().expect("tempdir");
    let dir = |n: &str| {
        let d = disk.path().join(n);
        std::fs::create_dir_all(&d).expect("node dir");
        d
    };
    let mut a = start_stress_node("fc-a", vec![], &dir("a")).await;
    let mut b = start_stress_node("fc-b", vec![a.swim_addr.clone()], &dir("b")).await;
    let mut c = start_stress_node("fc-c", vec![a.swim_addr.clone()], &dir("c")).await;
    wait_cluster_ready(&[&a, &b, &c]).await;

    // A persistent subscriber establishes its durable session + subscription,
    // then goes OFFLINE — everything it is owed must ride the disk.
    let sub_id = "fc-sub";
    // A persistent session attaches ON its placement owner (the pre-proxy
    // contract, ADR 0005 step 2 pending) — resolve it like every client
    // helper in this harness does.
    let owner_addr = |nodes: &[&StressNode]| {
        let owner = nodes[0].placement.read().unwrap().owner(sub_id);
        nodes
            .iter()
            .find(|n| n.node_id == owner)
            .expect("owner is a live node")
            .client_addr
    };
    {
        // Retried: the first CONNECT for a fresh id can be refused while its
        // session group's first-ever lease grants (reconcile-driven).
        let addr = owner_addr(&[&a, &b, &c]);
        let deadline = Instant::now() + Duration::from_secs(60);
        let (mut sub, present) = loop {
            if let Some(ok) =
                common::Client::connect_v311_within(addr, sub_id, false, Duration::from_secs(10))
                    .await
            {
                break ok;
            }
            assert!(Instant::now() < deadline, "subscriber never connected");
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        assert!(!present, "brand-new session");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let ack = sub.subscribe(1, "fc/t", QoS::AtLeastOnce).await;
            if ack.return_codes.iter().all(|c| *c != 0x80) {
                break;
            }
            assert!(Instant::now() < deadline, "durable SUBSCRIBE never granted");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        sub.disconnect().await;
    }

    // Acked facts: three QoS 1 payloads for the offline subscriber (each retried
    // until its PUBACK arrives — acked means durably owed) and one retained set.
    let nodes = [&a, &b, &c];
    for (i, payload) in [b"fc-m1".as_slice(), b"fc-m2", b"fc-m3"].iter().enumerate() {
        let deadline = Instant::now() + Duration::from_secs(60);
        'acked: loop {
            if let Some((mut publisher, _)) = common::Client::connect_v311_within(
                nodes[i % 3].client_addr,
                &format!("fc-pub-{i}"),
                true,
                Duration::from_secs(20),
            )
            .await
            {
                publisher
                    .publish("fc/t", payload, QoS::AtLeastOnce, Some(7), vec![])
                    .await;
                let wait = Instant::now() + Duration::from_secs(12);
                loop {
                    let left = wait.saturating_duration_since(Instant::now());
                    match publisher.recv_bounded(left).await {
                        common::Recv::Packet(Packet::PubAck(k)) if k.pkid == 7 => break 'acked,
                        common::Recv::Packet(_) => {}
                        common::Recv::Quiet | common::Recv::Closed => break,
                    }
                }
            }
            assert!(Instant::now() < deadline, "publish {i} never acked");
        }
    }
    {
        let deadline = Instant::now() + Duration::from_secs(60);
        'acked: loop {
            if let Some((mut publisher, _)) = common::Client::connect_v311_within(
                b.client_addr,
                "fc-rpub",
                true,
                Duration::from_secs(20),
            )
            .await
            {
                publisher
                    .publish_full("fc/r", b"fc-retained", QoS::AtLeastOnce, true, Some(9))
                    .await;
                let wait = Instant::now() + Duration::from_secs(12);
                loop {
                    let left = wait.saturating_duration_since(Instant::now());
                    match publisher.recv_bounded(left).await {
                        common::Recv::Packet(Packet::PubAck(k)) if k.pkid == 9 => break 'acked,
                        common::Recv::Packet(_) => {}
                        common::Recv::Quiet | common::Recv::Closed => break,
                    }
                }
            }
            assert!(Instant::now() < deadline, "retained set never acked");
        }
    }

    // The outage: EVERY node crashes. No memory survives; the dirs do.
    a.kill().await;
    b.kill().await;
    c.kill().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The restart, over the same dirs.
    let a = start_stress_node("fc-a", vec![], &dir("a")).await;
    let b = start_stress_node("fc-b", vec![a.swim_addr.clone()], &dir("b")).await;
    let c = start_stress_node("fc-c", vec![a.swim_addr.clone()], &dir("c")).await;
    wait_cluster_ready(&[&a, &b, &c]).await;

    // Recovery honesty + acked durability: the session is PRESENT and replays
    // every acked payload; the acked retained value is served cluster-wide.
    let deadline = Instant::now() + Duration::from_secs(60);
    let resume_addr = owner_addr(&[&a, &b, &c]);
    let (mut sub, present) = loop {
        if let Some(ok) =
            common::Client::connect_v311_within(resume_addr, sub_id, false, Duration::from_secs(10))
                .await
        {
            break ok;
        }
        assert!(
            Instant::now() < deadline,
            "subscriber could not resume after the full-cluster restart"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(
        present,
        "recovery honesty: the durable session must survive a full-cluster stop/start"
    );
    let mut got: BTreeSet<Vec<u8>> = BTreeSet::new();
    let drain_deadline = Instant::now() + Duration::from_secs(20);
    while got.len() < 3 && Instant::now() < drain_deadline {
        match sub.recv_bounded(Duration::from_secs(2)).await {
            common::Recv::Packet(Packet::Publish(p)) => {
                if let Some(pkid) = p.pkid {
                    sub.send(&Packet::PubAck(pkid.into())).await;
                }
                got.insert(p.payload.to_vec());
            }
            common::Recv::Packet(_) | common::Recv::Quiet => {}
            common::Recv::Closed => break,
        }
    }
    for payload in [b"fc-m1".as_slice(), b"fc-m2", b"fc-m3"] {
        assert!(
            got.contains(payload),
            "acked payload {:?} lost across the full-cluster stop/start",
            String::from_utf8_lossy(payload)
        );
    }
    let probe_deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let seen = retained_seen(a.client_addr, "fc-probe", "fc/r").await;
        if seen.as_deref() == Some(b"fc-retained".as_slice()) {
            break;
        }
        assert!(
            Instant::now() < probe_deadline,
            "acked retained value not served after the full-cluster stop/start (got {seen:?})"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// ADR 0043 P1 — the laptop→server upgrade, end to end: a SINGLE durable node
/// accumulates acked facts (an offline durable session owed three acked `QoS 1`
/// payloads, plus an acked retained value), the cluster grows 1→3 under it, the
/// catch-up sweep back-fills both joiners' replica copies behind the durable
/// caught-up watermark — and then the FOUNDER dies, taking the only pre-grow
/// copy of that history with it. Every acked fact must survive on the pair.
// One linear story — laptop, ack, grow, catch up, founder dies, verify — like
// the stop/start test above; splitting it would scatter the acked facts.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn growing_one_node_to_three_back_fills_and_survives_the_founder() {
    if std::env::var("MQTTD_STRESS_LOG").is_ok() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }
    let disk = tempfile::tempdir().expect("tempdir");
    let dir = |n: &str| {
        let d = disk.path().join(n);
        std::fs::create_dir_all(&d).expect("node dir");
        d
    };

    // --- the laptop: one durable node, serving alone ---
    let mut a = start_stress_node("gw-a", vec![], &dir("a")).await;

    // A persistent subscriber establishes its durable session + subscription,
    // then goes OFFLINE. On a single node, that node owns everything.
    let sub_id = "gw-sub";
    {
        let deadline = Instant::now() + Duration::from_secs(60);
        let (mut sub, present) = loop {
            // Retried: a fresh single node grants its first lease (and stamps its
            // boot catch-up watermark) within its first driver ticks.
            if let Some(ok) = common::Client::connect_v311_within(
                a.client_addr,
                sub_id,
                false,
                Duration::from_secs(10),
            )
            .await
            {
                break ok;
            }
            assert!(Instant::now() < deadline, "subscriber never connected");
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        assert!(!present, "brand-new session");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let ack = sub.subscribe(1, "gw/t", QoS::AtLeastOnce).await;
            if ack.return_codes.iter().all(|c| *c != 0x80) {
                break;
            }
            assert!(Instant::now() < deadline, "durable SUBSCRIBE never granted");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        sub.disconnect().await;
    }

    // Acked facts, all committed at replica-set {a} / quorum 1 — history that
    // exists NOWHERE else until the catch-up back-fills it.
    for (i, payload) in [b"gw-m1".as_slice(), b"gw-m2", b"gw-m3"].iter().enumerate() {
        let deadline = Instant::now() + Duration::from_secs(60);
        'acked: loop {
            if let Some((mut publisher, _)) = common::Client::connect_v311_within(
                a.client_addr,
                &format!("gw-pub-{i}"),
                true,
                Duration::from_secs(20),
            )
            .await
            {
                publisher
                    .publish("gw/t", payload, QoS::AtLeastOnce, Some(7), vec![])
                    .await;
                let wait = Instant::now() + Duration::from_secs(12);
                loop {
                    let left = wait.saturating_duration_since(Instant::now());
                    match publisher.recv_bounded(left).await {
                        common::Recv::Packet(Packet::PubAck(k)) if k.pkid == 7 => break 'acked,
                        common::Recv::Packet(_) => {}
                        common::Recv::Quiet | common::Recv::Closed => break,
                    }
                }
            }
            assert!(Instant::now() < deadline, "publish {i} never acked");
        }
    }
    {
        let deadline = Instant::now() + Duration::from_secs(60);
        'acked: loop {
            if let Some((mut publisher, _)) = common::Client::connect_v311_within(
                a.client_addr,
                "gw-rpub",
                true,
                Duration::from_secs(20),
            )
            .await
            {
                publisher
                    .publish_full("gw/r", b"gw-retained", QoS::AtLeastOnce, true, Some(9))
                    .await;
                let wait = Instant::now() + Duration::from_secs(12);
                loop {
                    let left = wait.saturating_duration_since(Instant::now());
                    match publisher.recv_bounded(left).await {
                        common::Recv::Packet(Packet::PubAck(k)) if k.pkid == 9 => break 'acked,
                        common::Recv::Packet(_) => {}
                        common::Recv::Quiet | common::Recv::Closed => break,
                    }
                }
            }
            assert!(Instant::now() < deadline, "retained set never acked");
        }
    }

    // --- the upgrade: grow 1 → 3 while serving ---
    let b = start_stress_node("gw-b", vec![a.swim_addr.clone()], &dir("b")).await;
    let c = start_stress_node("gw-c", vec![a.swim_addr.clone()], &dir("c")).await;
    wait_cluster_ready(&[&a, &b, &c]).await;

    // The P1 catch-up: BOTH joiners must hold the laptop-era history — the
    // session's queue and metadata and the retained key — gap-free and stamped
    // current behind the durable caught-up watermark. Only then is losing the
    // founder survivable.
    {
        let keys = [
            format!("q/{sub_id}"),
            format!("m/{sub_id}"),
            "r/gw/r".to_string(),
        ];
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let caught_up = [&b, &c].iter().all(|n| {
                let plane = n.plane.as_ref().expect("plane alive");
                keys.iter().all(|k| plane.replica_caught_up(k))
            });
            if caught_up {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "joiners never caught up on the laptop-era history (ADR 0043 P1)"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    // --- the founder dies. Its disk — the only pre-grow copy — is gone. ---
    a.kill().await;

    // Recovery honesty + acked durability on the survivors: the session resumes
    // PRESENT on its new owner and replays every acked payload.
    let deadline = Instant::now() + Duration::from_secs(90);
    let (mut sub, present) = loop {
        // The session attaches on its (post-death) placement owner, once SWIM
        // has evicted the founder and ownership settled on a survivor.
        let owner = b.placement.read().unwrap().owner(sub_id);
        let addr = [&b, &c]
            .iter()
            .find(|n| n.node_id == owner)
            .map(|n| n.client_addr);
        if let Some(addr) = addr {
            if let Some(ok) =
                common::Client::connect_v311_within(addr, sub_id, false, Duration::from_secs(10))
                    .await
            {
                break ok;
            }
        }
        assert!(
            Instant::now() < deadline,
            "subscriber could not resume on the survivors after the founder died"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(
        present,
        "recovery honesty: the durable session must survive the founder via the back-filled replicas"
    );
    let mut got: BTreeSet<Vec<u8>> = BTreeSet::new();
    let drain_deadline = Instant::now() + Duration::from_secs(20);
    while got.len() < 3 && Instant::now() < drain_deadline {
        match sub.recv_bounded(Duration::from_secs(2)).await {
            common::Recv::Packet(Packet::Publish(p)) => {
                if let Some(pkid) = p.pkid {
                    sub.send(&Packet::PubAck(pkid.into())).await;
                }
                got.insert(p.payload.to_vec());
            }
            common::Recv::Packet(_) | common::Recv::Quiet => {}
            common::Recv::Closed => break,
        }
    }
    for payload in [b"gw-m1".as_slice(), b"gw-m2", b"gw-m3"] {
        assert!(
            got.contains(payload),
            "acked payload {:?} (committed on the 1-node cluster) lost after the founder died",
            String::from_utf8_lossy(payload)
        );
    }
    // The acked retained value serves from the survivors too.
    let probe_deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let seen = retained_seen(b.client_addr, "gw-probe", "gw/r").await;
        if seen.as_deref() == Some(b"gw-retained".as_slice()) {
            break;
        }
        assert!(
            Instant::now() < probe_deadline,
            "acked retained value not served by the survivors (got {seen:?})"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Bring-up gate shared by the stop/start test: full membership + full voters.
async fn wait_cluster_ready(nodes: &[&StressNode]) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let members = nodes
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 3);
        let voters = nodes
            .iter()
            .all(|n| n.plane.as_ref().is_some_and(|p| p.voter_count() == 3));
        if members && voters {
            return;
        }
        assert!(Instant::now() < deadline, "cluster never became ready");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The T3 stress sweep: every seed composes its own fault schedule + workload;
/// the T1 catalog (as MQTT-observable facts) is the post-quiesce oracle.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn seeded_fault_schedules_hold_the_catalog_post_quiesce() {
    // Debug aid: MQTTD_STRESS_LOG=1 wires broker tracing through to stderr.
    if std::env::var("MQTTD_STRESS_LOG").is_ok() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }
    for seed in seeds() {
        run_schedule(seed).await;
        eprintln!("cluster_stress: seed {seed} held the catalog");
    }
}
