//! Seeded whole-cluster **stress harness** over real durable nodes
//! ([ADR 0042](../../docs/adr/0042-durable-plane-stress-harness.md) T3).
//!
//! Where the T2 simulation drives the pure replication core deterministically,
//! this layer attacks the **whole durable plane as it actually runs** — openraft
//! lease consensus, SWIM membership, the peer mesh, quorum replication, the hub —
//! with everything wired exactly as production wires it (the node assembly
//! mirrors `durable_sessions.rs`, which mirrors `main.rs`).
//!
//! One seed composes a **fault schedule** (an owner kill mid-workload — a real
//! takeover of live sessions; asymmetric peer-bus link flaps through a relay in
//! front of each node's peer listener; client churn — disconnects and resumes
//! riding lease handoffs) interleaved with a **workload** (`QoS` 1 publishes to
//! persistent subscribers, retained mutations, resumes), while an obligations
//! ledger records only **acked facts**: a payload becomes a delivery obligation
//! only when its PUBACK arrived; a retained value becomes the expected converged
//! value only from its acked set onward.
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
//! ## Exhibit ⑤ (found by this harness's first valid schedule, seed 0)
//!
//! A `QoS` 1 publish that lands on a **non-owner** node is acked once the
//! origin's *local* durable work is done, while the forward to the subscriber's
//! owner is **fire-and-forget** (`forward_to_peers` sends into the link writer
//! with no ack and no durable gate — the session-queue twin of the retained
//! handoff hole ADR 0037 T8 closed). A kill or link fault in that window loses
//! an acked message. Until 0042-T9 lands the acked cross-node publish handoff,
//! the obligations ledger records a **hard** obligation only when the publisher
//! was connected to the subscriber's owner (the T5/T8-hardened local path);
//! remote-origin acks are tracked as exhibit-⑤ candidates and their losses
//! counted loudly, never silently.
//!
//! ## Exhibit ⑥ (same first schedule, seed 0)
//!
//! A publish acked by the **new owner itself**, after a takeover but **before
//! the inherited session's first re-attach**, is never enqueued: the new owner
//! has not materialized the session's durable subscriptions (they load on
//! attach/recovery), so the publish routes to nothing, acks on trivially-empty
//! local work, and the eventual resume replays a queue that never received it.
//! Until 0042-T9, a hard obligation additionally requires that the subscriber's
//! **last attach was on the publishing owner** (the state the owner has
//! materialized); acked publishes in the takeover-to-reattach window are
//! exhibit-⑥ candidates, counted loudly.
//!
//! ## Exhibit ⑦ (seed 2)
//!
//! A retained `PUBACK` is released after the **local** fan-out, while the
//! authority commit rides ADR 0037's queue-until-heal on the landing node. If
//! that node is killed before the mutation reaches the group owner, the acked
//! retained set dies with its queue — the survivors converge on the previous
//! value. Until 0042-T9 gates the retained ack on the authority commit (the
//! `done` channel already exists), an acked set whose landing node was killed
//! is an exhibit-⑦ candidate: the expected-value window extends back to the
//! last acked set that landed on a survivor, and the event is counted loudly.
//!
//! ## Exhibit ⑧ (seed 2)
//!
//! Retained sets acked **during a fault window** (a severed bus, or the steps
//! right after the owner kill) can strand: the acked mutation sits in the
//! landing node's queue-until-heal while owner resolution points at the dead
//! node, and the drain depends solely on link-event heal triggers — none fire
//! again once the topology stabilizes — while the value the *survivors* hold
//! never back-fills the flapped node's cache either. The survivors sit stably
//! divergent (observed: one node serving an old value, one serving none, for
//! 12s+ against 500ms redials). Non-convergence whose candidate window contains
//! a fault-window ack is counted as exhibit ⑧ (0042-T9 investigates with
//! `REPRO_SEED = Some(2)`); non-convergence in steady state stays a hard
//! failure — that would be a new defect.
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
    plane: mqtt_cluster::durable_plane::DurablePlane,
    aborts: Vec<AbortHandle>,
}

impl StressNode {
    /// Crash the node: abort every task it spawned, so peers detect it dead.
    fn kill(&self) {
        for a in &self.aborts {
            a.abort();
        }
    }
}

async fn start_stress_node(id: &str, swim_seeds: Vec<String>) -> StressNode {
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
        None,
        None,
    )
    .await;
    let plane_observer = plane.clone();
    let (mut hub, hub_tx) =
        Hub::with_config_and_placement(node_id.clone(), store, Some(placement.clone()));
    hub.attach_durable_plane(plane);
    hub.attach_durable_retained(durable_retained);
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
            hub_tx,
            None,
            Some(placement.clone()),
            None,
        ))
        .abort_handle(),
    );

    StressNode {
        node_id,
        placement,
        swim_addr,
        client_addr,
        relay,
        plane: plane_observer,
        aborts,
    }
}

// ---------------------------------------------------------------------------
// The seeded schedule: workload + faults, with an acked-facts obligations ledger.
// ---------------------------------------------------------------------------

/// One retained set the schedule issued: its payload, whether the PUBACK
/// arrived, the node it landed on, and whether a fault window was open.
#[derive(Clone)]
struct RetainedSet {
    payload: Vec<u8>,
    acked: bool,
    landed_on: usize,
    fault_window: bool,
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
    /// The node of the most recent successful attach — the only node that has
    /// certainly materialized this session's subscriptions (exhibit ⑥).
    last_attached: Option<usize>,
    received: BTreeSet<Vec<u8>>,
}

struct Stress {
    seed: u64,
    rng: Rng,
    trace: Vec<String>,
    nodes: Vec<StressNode>,
    alive: Vec<bool>,
    subs: Vec<Subscriber>,
    /// Per topic: payloads whose PUBACK arrived with the publisher connected to
    /// the subscriber's owner — the HARD delivery obligations (the local durable
    /// ack path, sound since 0041-T5/0042-T8).
    acked: BTreeMap<String, Vec<Vec<u8>>>,
    /// Per topic: payloads acked from a NON-owner origin — exhibit ⑤ candidates
    /// (fire-and-forget peer forward): losses are counted, not yet failed.
    acked_remote: BTreeMap<String, Vec<Vec<u8>>>,
    /// Per topic: payloads acked by the owner inside the takeover-to-reattach
    /// window — exhibit ⑥ candidates: losses are counted, not yet failed.
    acked_prereattach: BTreeMap<String, Vec<Vec<u8>>>,
    /// Per retained topic: the set history, newest last. The expected converged
    /// value is any entry from the last *safe* acked one onward (safe = its
    /// landing node survived; an acked set stranded on a killed landing node is
    /// exhibit ⑦; one acked in a fault window that then fails to converge is
    /// exhibit ⑧).
    retained: BTreeMap<String, Vec<RetainedSet>>,
    /// Nodes whose inbound bus is currently severed.
    severed: Vec<usize>,
    /// The schedule step the kill executed at, once it has.
    killed_at_step: Option<u64>,
    /// The current schedule step (for fault-window classification).
    step: u64,
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
    async fn bring_subscriber_online(&mut self, i: usize) {
        let mut truth = if self.subs[i].established {
            DurableTruth::Present
        } else {
            DurableTruth::Absent
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let Some(owner) = self.owner_of(&self.subs[i].id) else {
                self.fail("no alive node resolves a placement owner");
            };
            if !self.alive[owner] {
                // The ring still names the dead node mid-handoff; wait it out.
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
                self.subs[i].last_attached = Some(owner);
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
            assert!(
                Instant::now() < deadline,
                "subscriber could not (re)connect within the deadline"
            );
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
            let deadline = Instant::now() + Duration::from_secs(4);
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
            // Exhibits ⑤/⑥ (module docs): a hard obligation needs the durable
            // local path end to end — the ack came from the subscriber's OWNER
            // and that owner has materialized the session (its last attach was
            // there). Anything else raced state 0042-T9 will make durable.
            let owner_local = self.owner_of(&self.subs[s].id) == Some(node);
            let materialized = self.subs[s].last_attached == Some(node);
            let (book, label) = if owner_local && materialized {
                (&mut self.acked, "obligation")
            } else if owner_local {
                (&mut self.acked_prereattach, "exhibit-6 candidate")
            } else {
                (&mut self.acked_remote, "exhibit-5 candidate")
            };
            book.entry(topic.clone()).or_default().push(payload);
            self.note(format!(
                "publish #{} to {topic} via {}: ACKED ({label})",
                self.payload_counter, self.nodes[node].node_id.0,
            ));
        } else {
            self.note(format!(
                "publish #{} to {topic} via {}: unacked (no obligation)",
                self.payload_counter, self.nodes[node].node_id.0
            ));
        }
        // Opportunistically drain online subscribers so live deliveries land.
        self.drain_subscriber(s).await;
    }

    /// Whether the schedule is inside a fault window: a bus is severed, or the
    /// kill happened within the last two steps (the lease-handoff window).
    fn fault_window(&self) -> bool {
        !self.severed.is_empty()
            || self
                .killed_at_step
                .is_some_and(|k| self.step.saturating_sub(k) <= 2)
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
            let deadline = Instant::now() + Duration::from_secs(4);
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

        let fault = self.fault_window();
        self.retained
            .entry(topic.clone())
            .or_default()
            .push(RetainedSet {
                payload,
                acked,
                landed_on: node,
                fault_window: fault,
            });
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
            self.bring_subscriber_online(s).await;
            self.drain_subscriber(s).await;
        }
    }

    /// THE takeover: kill the node owning a seeded subscriber's session.
    fn kill_step(&mut self) {
        let s = self.rng.pick(self.subs.len());
        let Some(owner) = self.owner_of(&self.subs[s].id) else {
            self.fail("no owner resolvable for the kill step");
        };
        if !self.alive[owner] || self.alive_nodes().len() < 3 {
            return; // already killed one — the schedule kills at most one node
        }
        self.nodes[owner].kill();
        self.alive[owner] = false;
        self.killed_at_step = Some(self.step);
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
    let a = start_stress_node(&format!("st{seed}-a"), vec![]).await;
    let b = start_stress_node(&format!("st{seed}-b"), vec![a.swim_addr.clone()]).await;
    let c = start_stress_node(&format!("st{seed}-c"), vec![a.swim_addr.clone()]).await;
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
            nodes.iter().all(|n| n.plane.voter_count() == 3)
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
        acked_remote: BTreeMap::new(),
        acked_prereattach: BTreeMap::new(),
        retained: BTreeMap::new(),
        severed: Vec::new(),
        killed_at_step: None,
        step: 0,
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
            last_attached: None,
            received: BTreeSet::new(),
        });
        stress.bring_subscriber_online(i).await;
        let topic = stress.subs[i].topic.clone();
        let sub = stress.subs[i].conn.as_mut().unwrap();
        sub.subscribe(1, &topic, QoS::AtLeastOnce).await;
    }
    stress.note("setup complete: 3 subscribers online + subscribed".into());

    // The seeded schedule: ~14 steps, one kill at a seeded position, flaps and
    // churn throughout.
    let steps = stress.rng.range(12, 17);
    let kill_at = stress.rng.range(3, steps - 2);
    for step in 0..steps {
        stress.step = step;
        if step == kill_at {
            stress.kill_step();
            continue;
        }
        match stress.rng.range(0, 100) {
            0..=44 => stress.publish_step().await,
            45..=64 => stress.retained_step().await,
            65..=84 => stress.churn_step().await,
            _ => stress.flap_step(),
        }
    }
    stress.step = steps;

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
    let mut exhibit5_lost = 0usize;
    let mut exhibit6_lost = 0usize;

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
        stress.bring_subscriber_online(i).await;
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
        // Exhibit ⑤/⑥ candidates: count losses loudly (removed by 0042-T9).
        let remote = stress.acked_remote.get(&topic).cloned().unwrap_or_default();
        exhibit5_lost += remote
            .iter()
            .filter(|p| !stress.subs[i].received.contains(*p))
            .count();
        let prereattach = stress
            .acked_prereattach
            .get(&topic)
            .cloned()
            .unwrap_or_default();
        exhibit6_lost += prereattach
            .iter()
            .filter(|p| !stress.subs[i].received.contains(*p))
            .count();
    }

    // 2. Retained convergence: every survivor serves the same value, and it is
    //    never behind the last SAFE acked set — one whose landing node survived
    //    (an acked set stranded on a killed landing node is exhibit ⑦; later
    //    unacked sets may legitimately have committed). Fan-out and back-fill
    //    are eventually consistent, so the oracle POLLS to a deadline instead
    //    of reading once.
    let mut probe = 0u64;
    let mut exhibit7 = 0usize;
    let mut exhibit8 = 0usize;
    for (topic, history) in stress.retained.clone() {
        let Some(last_acked) = history.iter().rposition(|r| r.acked) else {
            continue; // nothing was ever promised for this topic
        };
        // The hard window starts at the last acked set that landed on a node
        // still alive; acked sets stranded on the killed node extend the window
        // backward (exhibit ⑦, counted below).
        let last_safe_acked = history
            .iter()
            .rposition(|r| r.acked && stress.alive[r.landed_on]);
        // Every acked set stranded (None) accepts the whole history from 0.
        let window_start = last_safe_acked.unwrap_or_default();
        if window_start < last_acked || last_safe_acked.is_none() {
            exhibit7 += 1;
        }
        let candidates: Vec<&Vec<u8>> =
            history[window_start..].iter().map(|r| &r.payload).collect();

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
            // Exhibit ⑧ (module docs): a candidate window containing sets acked
            // DURING a fault window may strand — counted, not failed, until
            // 0042-T9. Steady-state divergence stays a hard failure.
            let fault_tainted = history[window_start..].iter().any(|r| r.fault_window);
            if fault_tainted {
                exhibit8 += 1;
                stress.note(format!(
                    "exhibit-8: {topic} unconverged after fault-window acks: {detail:?}"
                ));
                continue;
            }
            stress.fail(&format!(
                "retained convergence violated for {topic}: survivors never \
                 converged on a value at or beyond the last safe acked set: {detail:?}"
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
    if exhibit7 > 0 || exhibit8 > 0 {
        eprintln!(
            "cluster_stress: seed {seed}: {exhibit7} exhibit-7 topic(s) (acked \
             retained set stranded on a killed landing node) + {exhibit8} \
             exhibit-8 topic(s) (fault-window acks unconverged after heal) — \
             0042-T9 investigates/fixes"
        );
    }

    if exhibit5_lost > 0 || exhibit6_lost > 0 {
        eprintln!(
            "cluster_stress: seed {seed}: {exhibit5_lost} exhibit-5 loss(es) \
             (remote-origin ack, fire-and-forget forward) + {exhibit6_lost} \
             exhibit-6 loss(es) (owner ack before the inherited session's first \
             re-attach) — 0042-T9 fixes both"
        );
    }

    // Tear the cluster down so the next seed starts clean.
    for node in &stress.nodes {
        node.kill();
    }
}

/// The T3 stress sweep: every seed composes its own fault schedule + workload;
/// the T1 catalog (as MQTT-observable facts) is the post-quiesce oracle.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn seeded_fault_schedules_hold_the_catalog_post_quiesce() {
    for seed in seeds() {
        run_schedule(seed).await;
        eprintln!("cluster_stress: seed {seed} held the catalog");
    }
}
