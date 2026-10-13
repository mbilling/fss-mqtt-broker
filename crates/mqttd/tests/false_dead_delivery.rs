//! What a `QoS` ≥ 1 publish does while one node holds a LIVE peer `Dead` (issue #973).
//!
//! Three brokers wired as production wires them — the placement-aware hub marked
//! cluster-configured, real SWIM, the real link manager, metrics — in two modes: plain
//! (in-memory sessions) and durable (the lease group and the replicated session store).
//! Node **A** is then told, on the membership event channel its own SWIM driver writes
//! to, that the live node **B** is `Dead`. B and the bystander **C** are told nothing:
//! B's clients stay connected, and the network between every pair works throughout.
//! That is the false verdict of #971/#973 in its smallest form — in a real cluster the
//! `Dead` claim is gossiped, so more than one node can hold it at once, and it ends when
//! B's refutation arrives first-hand or the tombstone is pruned (30 s).
//!
//! These tests began as characterisation of that defect — every row that lost a
//! publish asserted as [`Fate::AckedAndLost`] or [`Fate::HeldThenLost`], so that each
//! would fail when the behaviour changed in either direction. ADR 0087 holds the
//! decision (2026-10-13): a `QoS` ≥ 1 publish is acknowledged only when every node that
//! owns a subscriber has accepted it, and a `Dead` verdict no longer erases what the
//! publish is owed to. What has landed of it, and flipped those rows:
//!
//! - **T4 (D5), keep dialing.** The node that holds the verdict no longer stops its
//!   dialer, so the link is back about half a second after the verdict whichever of
//!   the pair dials it, and the two orientations of every episode assert the same
//!   rows ([`expect_the_link_returns_within_a_second`]).
//! - **T2 (D1, D4, D6), unreachable, not erased.** A peer under a verdict, or merely
//!   without a link, keeps its interest and its `$share` members; a publish that
//!   matches is owed to it and held, and goes out when the link is back. No row is
//!   `AckedAndLost` or `HeldThenLost` any more, `$share` rows included, and an
//!   episode with no verdict at all — the link alone is cut — joins the matrix
//!   ([`Fault::LinkCut`]).
//!
//! What these episodes still show and the design has not reached: in durable mode
//! the first `QoS` 2 publish after the verdict can be answered by closing the
//! connection (ADR 0087 D4 records it as unchanged), and a false verdict at the lease
//! leader moves the session's lease (T5, D8). Nothing here is `#[ignore]`d.

mod common;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use common::{Client, Recv};
use mqtt_cluster::durable_node::build_durable_node_with;
use mqtt_cluster::placement::{Placement, DEFAULT_REPLICAS};
use mqtt_cluster::swim::{Config as SwimConfig, MemberState, Swim};
use mqtt_cluster::swim_auth::SwimAuth;
use mqtt_cluster::swim_driver::MembershipEvent;
use mqtt_cluster::{swim_driver, NodeId};
use mqtt_codec::{Packet, QoS};
use mqtt_storage::{MemorySessionStore, SessionStore};
use mqttd::hub::{Hub, HubCommand};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, watch};

use futures_util::future::join_all;

// ---------------------------------------------------------------------------------
// The cluster
// ---------------------------------------------------------------------------------

/// Session storage, and with it which delivery guarantee is in force.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// `MQTTD_DURABLE=0`: sessions in memory on the node that hosts them.
    Plain,
    /// The default: the lease group and the quorum-replicated session store.
    Durable,
}

/// Which of the pair dials the peer link (the smaller node id does).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Link {
    /// The node holding the false verdict owns the link. Its dialer keeps dialing
    /// under the verdict (ADR 0087 D5); until that landed nothing redialed before
    /// `Alive`, and this was the orientation that lost everything.
    EvictorDials,
    /// The evicted node owns the link: its dialer redials on its own, and the node
    /// holding the verdict accepts — a link is admitted by its handshake.
    EvictedDials,
}

/// SWIM timings: quick to form (a short protocol period carries the gossip), and
/// **deaf to load**. Nothing in these tests needs real failure detection — the false
/// verdict is injected — and real detection firing would undo the experiment: a
/// `Suspect` A's own SWIM raised about B, refuted, comes back as an `Alive` event,
/// the link manager's `Alive` arm redials B, and every lossy row would flip as if a
/// fix had landed. So a probe waits ten seconds (direct, then indirect) before it
/// suspects anyone, and a suspicion would take two minutes to become a `Dead`. On top
/// of that every episode checks that A's SWIM raised nothing about B while the
/// verdict stood ([`Cluster::membership_moved_only_as_injected`]), so a run in which
/// it did fails saying so.
fn swim_cfg() -> SwimConfig {
    SwimConfig {
        protocol_period_ms: 150,
        ack_timeout_ms: 5000,
        suspicion_timeout_ms: 120_000,
        suspicion_min_timeout_ms: 120_000,
        dead_ttl_ms: 5000,
        indirect_probes: 2,
        gossip_fanout: 8,
        gossip_multiplier: 4,
        ..SwimConfig::default()
    }
}

/// A rule that republishes everything under `fd/in/` to `fd/out/<payload>` — the tests'
/// payload is the publish's own topic, so the derived topic ends as the original did —
/// at the publisher's `QoS` (ADR 0083: the derived message takes its own
/// acknowledgement gate).
const REPUBLISH: &str = r#"
[rules.fwd]
sql = '''
SELECT payload, qos
FROM "fd/in/#"
'''
actions = [{ function = "republish", args = { topic = "fd/out/${payload}", payload = "${payload}" } }]
"#;

struct Node {
    id: NodeId,
    client_addr: SocketAddr,
    swim_addr: String,
    peer_addr: String,
    placement: Arc<RwLock<Placement>>,
    /// The channel this node's link manager reads membership changes from. The SWIM
    /// driver's own events reach it through [`raised`](Self::raised)'s tap; the tests
    /// inject theirs here directly.
    events: mpsc::UnboundedSender<MembershipEvent>,
    /// Every membership change this node's own SWIM driver raised, in order — so a
    /// test can tell the verdict it injected from one real detection produced.
    raised: Arc<Mutex<Vec<(NodeId, MemberState)>>>,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
    plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
    /// The hub, to ask it whether it has caught up ([`HubCommand::Flush`]).
    hub: mpsc::UnboundedSender<HubCommand>,
    /// How many `Dead` verdicts this node's link manager has handed its hub.
    verdicts_handed_to_the_hub: Arc<AtomicUsize>,
    /// Whether the relay in front of this node's peer listener carries links
    /// ([`relay_peer_links`]); `false` cuts the ones it has and refuses new ones.
    links_carried: watch::Sender<bool>,
}

/// A TCP relay in front of a node's peer listener, at the address the node gossips
/// as its own, so that a test can cut a peer link and nothing else: no membership
/// event, no hub command, the gossip untouched. While `carried` is false every
/// relayed connection is closed and every new one is closed as soon as it is
/// accepted, which is what a dialer sees of a peer it cannot reach.
///
/// One link per pair, dialed by the smaller node id — so the relay of the larger of
/// two ids carries exactly the links dialed by smaller ones.
async fn relay_peer_links(to: SocketAddr) -> (String, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (carried_tx, carried) = watch::channel(true);
    tokio::spawn(async move {
        loop {
            let Ok((mut dialer, _)) = listener.accept().await else {
                return;
            };
            let mut carried = carried.clone();
            if !*carried.borrow_and_update() {
                continue;
            }
            tokio::spawn(async move {
                let Ok(mut listener_side) = TcpStream::connect(to).await else {
                    return;
                };
                let _ = dialer.set_nodelay(true);
                let _ = listener_side.set_nodelay(true);
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut dialer, &mut listener_side) => {}
                    _ = carried.wait_for(|carried| !*carried) => {}
                }
            });
        }
    });
    (addr, carried_tx)
}

// The node assembly mirrors production wiring end to end, as `durable_sessions.rs` does.
#[allow(clippy::too_many_lines)]
async fn start_node(id: &str, seeds: Vec<String>, mode: Mode) -> Node {
    let node_id = NodeId(id.to_string());
    let placement = Arc::new(RwLock::new(Placement::new(
        node_id.clone(),
        DEFAULT_REPLICAS,
    )));
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("fd"));

    let (store, plane, mut hub, hub_tx) = match mode {
        Mode::Plain => {
            let store: Arc<dyn SessionStore> = Arc::new(MemorySessionStore::new());
            let (hub, tx) = Hub::with_config_and_placement(
                node_id.clone(),
                store.clone(),
                Some(placement.clone()),
            );
            (store, None, hub, tx)
        }
        Mode::Durable => {
            let (store, durable_retained, plane, _driver) = build_durable_node_with(
                node_id.clone(),
                placement.clone(),
                seeds.is_empty(),
                5,
                &BTreeMap::new(),
                None,
                None,
                false,
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
                None,
                common::replica_store(),
                Some(common::founding()),
            )
            .await;
            let (mut hub, tx) = Hub::with_config_and_placement(
                node_id.clone(),
                store.clone(),
                Some(placement.clone()),
            );
            hub.attach_durable_plane(plane.clone());
            hub.attach_durable_retained(durable_retained);
            (store, Some(plane), hub, tx)
        }
    };
    // As `main` does for a node with peer networking: the cluster honesty gates apply
    // from the first moment (the settle window, the pending-publish gate).
    hub.set_cluster_configured();
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());

    let engine = {
        let set = Arc::new(
            mqtt_rules::RuleSet::parse(REPUBLISH)
                .unwrap_or_else(|e| panic!("test rules must load: {e}"))
                .rules,
        );
        // The sender is leaked on purpose: these rules are never reloaded.
        let (tx, rx) = tokio::sync::watch::channel(set);
        std::mem::forget(tx);
        let engine = mqttd::rules::Rules::new(rx, Arc::from(id), Some(metrics.clone()));
        hub_tx
            .send(HubCommand::AttachRules(engine.clone()))
            .unwrap();
        engine
    };
    let policy = Arc::new(mqttd::conn::ConnPolicy {
        anonymous: None,
        auth: mqttd::conn::auth_handle(Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        })),
        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
        identity_source: mqtt_auth::mtls::IdentitySource::default(),
        audit: Arc::new(mqtt_observability::AuditLog::new()),
        proxy: None,
        node: None,
        store: Some(store),
        connect_timeout: Duration::from_secs(10),
        enhanced: None,
        shutdown: None,
        metrics: Some(metrics.clone()),
        ingress: None,
        rules: Some(engine),
    });
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();
    {
        let tx = hub_tx.clone();
        tokio::spawn(async move {
            loop {
                let (stream, peer) = client_listener.accept().await.unwrap();
                tokio::spawn(mqttd::conn::handle_stream(
                    stream,
                    Some(peer),
                    None,
                    policy.clone(),
                    tx.clone(),
                ));
            }
        });
    }

    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    // What the node gossips as its routing address is the relay in front of it.
    let (peer_addr, links_carried) = relay_peer_links(peer_listener.local_addr().unwrap()).await;
    tokio::spawn(mqttd::peer::serve_listener(
        peer_listener,
        node_id.clone(),
        hub_tx.clone(),
        None,
        None,
        plane.clone(),
    ));

    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let swim_addr = socket.local_addr().unwrap().to_string();
    let swim = Swim::new(
        node_id.clone(),
        swim_addr.clone(),
        peer_addr.clone(),
        None,
        1,
        swim_cfg(),
        seeds,
    );
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (driver_tx, mut driver_rx) = mpsc::unbounded_channel::<MembershipEvent>();
    let raised = Arc::new(Mutex::new(Vec::new()));
    {
        let (raised, to_link_manager) = (raised.clone(), event_tx.clone());
        tokio::spawn(async move {
            while let Some(event) = driver_rx.recv().await {
                raised.lock().unwrap().push((event.id.clone(), event.state));
                if to_link_manager.send(event).is_err() {
                    break;
                }
            }
        });
    }
    tokio::spawn(swim_driver::run(
        socket,
        swim,
        Duration::from_millis(20),
        driver_tx,
        // This test's own key (#754): a recycled port cannot merge two tests' clusters.
        Some(SwimAuth::new(&common::this_tests_cluster_key())),
        None,
        None,
        None,
        None,
        std::future::pending(),
    ));
    // The link manager reaches the hub through a tap that counts the verdicts it
    // has handed on, so a test can wait for the hub to have APPLIED one. (The
    // members gauge is no such observable: the link manager publishes it before it
    // tells the hub.)
    let verdicts_handed_to_the_hub = Arc::new(AtomicUsize::new(0));
    let (to_hub, mut from_link_manager) = mpsc::unbounded_channel::<HubCommand>();
    {
        let (hub, handed) = (hub_tx.clone(), verdicts_handed_to_the_hub.clone());
        tokio::spawn(async move {
            while let Some(command) = from_link_manager.recv().await {
                let verdict = matches!(command, HubCommand::PeerDead { .. });
                if hub.send(command).is_err() {
                    break;
                }
                if verdict {
                    handed.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
    }
    tokio::spawn(mqttd::cluster::maintain_peer_links(
        event_rx,
        node_id.clone(),
        to_hub,
        None,
        Some(placement.clone()),
        Some(metrics.clone()),
        plane.clone(),
    ));

    Node {
        id: node_id,
        client_addr,
        swim_addr,
        peer_addr,
        placement,
        events: event_tx,
        raised,
        metrics,
        plane,
        hub: hub_tx,
        verdicts_handed_to_the_hub,
        links_carried,
    }
}

/// `a` holds the false verdict about `b`; `c` is a bystander that is told nothing.
struct Cluster {
    a: Node,
    b: Node,
    c: Node,
    mode: Mode,
    /// How many events A's own SWIM had raised when the verdict was injected, and
    /// when it was withdrawn (`usize::MAX` until then).
    raised_at_verdict: AtomicUsize,
    raised_at_heal: AtomicUsize,
}

/// Which node founds the cluster — in durable mode, the lease group's first leader.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Founder {
    Evictor,
    Bystander,
}

impl Cluster {
    async fn start(mode: Mode, link: Link, founder: Founder) -> Self {
        let (a_id, b_id) = match link {
            Link::EvictorDials => ("fd-1", "fd-2"),
            Link::EvictedDials => ("fd-2", "fd-1"),
        };
        let c_id = "fd-3";
        let (a, b, c) = match founder {
            Founder::Evictor => {
                let a = start_node(a_id, vec![], mode).await;
                let b = start_node(b_id, vec![a.swim_addr.clone()], mode).await;
                let c = start_node(c_id, vec![a.swim_addr.clone()], mode).await;
                (a, b, c)
            }
            Founder::Bystander => {
                let c = start_node(c_id, vec![], mode).await;
                let a = start_node(a_id, vec![c.swim_addr.clone()], mode).await;
                let b = start_node(b_id, vec![c.swim_addr.clone()], mode).await;
                (a, b, c)
            }
        };
        let cluster = Cluster {
            a,
            b,
            c,
            mode,
            raised_at_verdict: AtomicUsize::new(0),
            raised_at_heal: AtomicUsize::new(usize::MAX),
        };
        let nodes = cluster.nodes();
        wait_until(
            "every node counts three members",
            Duration::from_secs(30),
            || {
                nodes
                    .iter()
                    .all(|n| n.placement.read().unwrap().member_count() == 3)
            },
        )
        .await;
        if mode == Mode::Durable {
            wait_until(
                "the lease group has three voters",
                Duration::from_mins(1),
                || {
                    nodes
                        .iter()
                        .all(|n| n.plane.as_ref().is_some_and(|p| p.voter_count() == 3))
                },
            )
            .await;
        }
        cluster
    }

    fn nodes(&self) -> [&Node; 3] {
        [&self.a, &self.b, &self.c]
    }

    fn node(&self, on: On) -> &Node {
        match on {
            On::A => &self.a,
            On::B => &self.b,
            On::C => &self.c,
        }
    }

    /// A client id whose session `node` owns — in durable mode a persistent session is
    /// served only by its group's committed owner, so the subscriber "on B" must be one
    /// B owns. In plain mode any id is hosted wherever it connects.
    async fn client_id_on(&self, prefix: &str, node: &Node) -> String {
        if self.mode == Mode::Plain {
            return prefix.to_string();
        }
        let nodes = self.nodes();
        let mut found = None;
        wait_until(
            "a client id every node places on the wanted owner",
            Duration::from_mins(1),
            || {
                found = (0..2000).map(|i| format!("{prefix}-{i}")).find(|id| {
                    nodes.iter().all(|n| {
                        n.placement.read().unwrap().committed_session_owner(id)
                            == Some(node.id.clone())
                    })
                });
                found.is_some()
            },
        )
        .await;
        found.expect("checked by the wait")
    }

    fn event(&self, state: MemberState) -> MembershipEvent {
        MembershipEvent {
            id: self.b.id.clone(),
            addr: self.b.swim_addr.clone(),
            peer_addr: self.b.peer_addr.clone(),
            state,
            domain: None,
        }
    }

    /// A — and only A — is told B is dead: the event A's SWIM driver emits for a `Dead`.
    /// Returns once A's HUB has applied the verdict — the link to B dropped, and
    /// whatever else the verdict does to routing done — so that the first publish of
    /// every flow is made under the verdict and has to cross the redial. Returning
    /// on the members gauge alone would not do: the link manager publishes it before
    /// it tells the hub, and a publish made in between travels the old link and
    /// reads as delivered without having tested anything.
    ///
    /// Nothing waits for the link to be seen DOWN, in either orientation: whichever
    /// node dials it, dials it again within half a second (ADR 0087 D5), and each
    /// hub publishes its link count once a second.
    async fn falsely_declare_b_dead_on_a(&self) {
        self.raised_at_verdict
            .store(self.a.raised.lock().unwrap().len(), Ordering::SeqCst);
        let handed = self.a.verdicts_handed_to_the_hub.clone();
        let before = handed.load(Ordering::SeqCst);
        self.a.events.send(self.event(MemberState::Dead)).unwrap();
        let metrics = self.a.metrics.clone();
        wait_until(
            "A's members gauge counts one dead",
            Duration::from_secs(10),
            || metrics.render().contains("mqttd_members{state=\"dead\"} 1"),
        )
        .await;
        wait_until(
            "A's link manager has handed the verdict to A's hub",
            Duration::from_secs(10),
            || handed.load(Ordering::SeqCst) > before,
        )
        .await;
        // The verdict is in the hub's queue; this barrier is behind it.
        let (reply, applied) = tokio::sync::oneshot::channel();
        self.a.hub.send(HubCommand::Flush { reply }).unwrap();
        tokio::time::timeout(Duration::from_secs(10), applied)
            .await
            .expect("A's hub applies the verdict")
            .expect("A's hub is running");
    }

    /// The relay the A–B link runs through: the one in front of whichever of the two
    /// has the larger node id (the other dials it), which carries no other link —
    /// the bystander has the largest id of the three and dials nobody.
    fn link_between_a_and_b(&self) -> &watch::Sender<bool> {
        if self.a.id.0 < self.b.id.0 {
            &self.b.links_carried
        } else {
            &self.a.links_carried
        }
    }

    /// The link between A and B is cut and stays cut: no verdict, no membership
    /// event, the gossip between the two untouched. Returns once BOTH hubs have seen
    /// it go (each publishes its link count on its sweep tick).
    async fn cut_the_link(&self) {
        self.raised_at_verdict
            .store(self.a.raised.lock().unwrap().len(), Ordering::SeqCst);
        self.link_between_a_and_b().send_replace(false);
        for node in [&self.a, &self.b] {
            wait_until(
                "the hub's link count drops to the one link that is left",
                Duration::from_secs(20),
                || series(node).get("mqttd_peer_links").copied() == Some(1.0),
            )
            .await;
        }
    }

    /// The link is restored once an operator could have SEEN the hold, on both
    /// nodes: each reports publishes held for a peer it cannot reach. (A's are owed
    /// to B's subscribers, B's to A's.)
    async fn restore_the_link_once_the_hold_is_visible(&self) {
        for node in [&self.a, &self.b] {
            wait_until(
                "the node reports publishes held for an unreachable peer",
                Duration::from_secs(20),
                || {
                    series(node)
                        .get("mqttd_publish_held_for_unreachable")
                        .is_some_and(|held| *held >= 1.0)
                },
            )
            .await;
        }
        let _ = self.raised_at_heal.compare_exchange(
            usize::MAX,
            self.a.raised.lock().unwrap().len(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.link_between_a_and_b().send_replace(true);
    }

    /// A's membership heals: the `Alive` event a first-hand refutation raises. Only
    /// the first call marks the end of the verdict.
    fn heal(&self) {
        let _ = self.raised_at_heal.compare_exchange(
            usize::MAX,
            self.a.raised.lock().unwrap().len(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.a.events.send(self.event(MemberState::Alive)).unwrap();
    }

    /// The precondition every row of an episode rests on: between the injected `Dead`
    /// and the injected `Alive`, A's own SWIM raised nothing about B. If it did — a
    /// starved runner made a probe of B time out, B refuted, and the resulting real
    /// `Alive` redialed the link — the episode measured something else, and it fails
    /// HERE, naming that, rather than further on looking like a change in the broker.
    #[track_caller]
    fn membership_moved_only_as_injected(&self) {
        let raised = self.a.raised.lock().unwrap();
        let from = self.raised_at_verdict.load(Ordering::SeqCst);
        let to = self.raised_at_heal.load(Ordering::SeqCst).min(raised.len());
        let about_b: Vec<_> = raised[from..to]
            .iter()
            .filter(|(id, _)| *id == self.b.id)
            .collect();
        assert!(
            about_b.is_empty(),
            "VOID EPISODE, not a behaviour change: while the injected verdict stood, A's \
             own SWIM raised {about_b:?} about B (real failure detection fired, most \
             likely on a starved runner), so the routing state did not stay as the test \
             set it. Re-run; if this recurs, the SWIM timings in `swim_cfg` are too tight."
        );
    }
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------------

/// How a `QoS` ≥ 1 publish was answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// PUBACK (or PUBREC + PUBCOMP) with this v5 reason code, after this long.
    Acked { reason: u8, after: Duration },
    /// No acknowledgement within the bound; the connection stayed open.
    Withheld,
    /// The broker closed the connection instead of answering — how it withholds an
    /// acknowledgement it can neither give nor explain.
    Closed,
}

impl Answer {
    /// A success acknowledgement: the broker told the publisher the message is its own.
    fn acked_ok(self) -> bool {
        matches!(self, Answer::Acked { reason: 0, .. })
    }
    fn after(self) -> Option<Duration> {
        match self {
            Answer::Acked { after, .. } => Some(after),
            _ => None,
        }
    }
}

/// A v5 publisher (so a refusal's reason code would be visible) with its own packet ids.
struct Publisher {
    client: Client,
    addr: SocketAddr,
    id: String,
    next_pkid: u16,
    /// The packet id of the last publish, and the ids acknowledged (with success) only
    /// after the wait for them had ended — an acknowledgement the broker held.
    last_pkid: u16,
    late_acks: Vec<u16>,
}

impl Publisher {
    /// Connect a publisher to `on`, under a client id that node owns: a `QoS` 2
    /// publisher's dedup window is durable state of its own session's group.
    async fn connect(cluster: &Cluster, on: On, prefix: &str) -> Self {
        let node = cluster.node(on);
        let id = cluster.client_id_on(prefix, node).await;
        Self {
            client: Client::connect_v5_ok(node.client_addr, &id).await,
            addr: node.client_addr,
            id,
            next_pkid: 1,
            last_pkid: 0,
            late_acks: Vec::new(),
        }
    }

    /// Publish `topic` (the payload is the topic) and wait for the answer, completing
    /// the `QoS` 2 handshake if that is what arrives. A publisher the broker closed on
    /// reconnects, as a real client would, so the next publish has a connection.
    async fn publish(&mut self, topic: &str, qos: QoS, retain: bool, within: Duration) -> Answer {
        let pkid = self.next_pkid;
        self.next_pkid = if pkid >= 60_000 { 1 } else { pkid + 1 };
        self.last_pkid = pkid;
        self.client
            .publish_full(topic, topic.as_bytes(), qos, retain, Some(pkid))
            .await;
        let sent = Instant::now();
        let answer = loop {
            let Some(left) = within.checked_sub(sent.elapsed()) else {
                break Answer::Withheld;
            };
            match self.client.recv_bounded(left).await {
                Recv::Packet(Packet::PubAck(a) | Packet::PubComp(a)) if a.pkid == pkid => {
                    break Answer::Acked {
                        reason: a.reason,
                        after: sent.elapsed(),
                    }
                }
                Recv::Packet(Packet::PubRec(a)) if a.pkid == pkid => {
                    if a.reason >= 0x80 {
                        break Answer::Acked {
                            reason: a.reason,
                            after: sent.elapsed(),
                        };
                    }
                    self.client.pubrel(pkid).await;
                }
                // The answer to an earlier publish, arriving after its wait ended.
                Recv::Packet(Packet::PubAck(a)) if a.reason == 0 => self.late_acks.push(a.pkid),
                Recv::Packet(_) => {}
                Recv::Quiet => break Answer::Withheld,
                Recv::Closed => break Answer::Closed,
            }
        };
        if answer == Answer::Closed {
            self.client = Client::connect_v5_ok(self.addr, &self.id).await;
        }
        answer
    }
}

/// The two publishes of an episode a delivery can be one of, and the one made while
/// the cluster was still healthy (for a session that then went offline).
const TAGS: [&str; 3] = ["healthy", "first", "second"];

fn tag(topic: &str) -> Option<&'static str> {
    TAGS.into_iter().find(|t| topic.ends_with(t))
}

/// Which node of the [`Cluster`] a client is on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum On {
    A,
    B,
    C,
}

/// One subscriber and what it has received.
struct Sub {
    on: On,
    id: String,
    clean: bool,
    filter: String,
    qos: QoS,
    /// `None` while the client is disconnected (offline by design, or closed on).
    client: Option<Client>,
    /// The broker closed this subscriber's connection while the verdict stood.
    closed_during: bool,
    /// [`TAGS`] received while the verdict stood, and over the whole episode.
    during: Vec<&'static str>,
    ever: Vec<&'static str>,
}

impl Sub {
    async fn connect(
        cluster: &Cluster,
        on: On,
        prefix: &str,
        clean: bool,
        filter: &str,
        qos: QoS,
    ) -> Self {
        let node = cluster.node(on);
        let id = cluster.client_id_on(prefix, node).await;
        let mut last = String::new();
        // A durable attach may not be ready on the first try (ADR 0017).
        for _ in 0..60 {
            match Client::connect_v311_explained(
                node.client_addr,
                &id,
                clean,
                Duration::from_secs(8),
            )
            .await
            {
                Ok((mut client, _)) => {
                    client.subscribe(1, filter, qos).await;
                    return Sub {
                        on,
                        id,
                        clean,
                        filter: filter.to_string(),
                        qos,
                        client: Some(client),
                        closed_during: false,
                        during: Vec::new(),
                        ever: Vec::new(),
                    };
                }
                Err(e) => last = e,
            }
        }
        panic!("subscriber {id} never connected to {on:?}: {last}");
    }

    /// Read for `window`, acknowledging every delivery (`QoS` 1 and both `QoS` 2
    /// steps); returns the topics. `standing`: the false verdict is still in force.
    async fn drain(&mut self, window: Duration, standing: bool) -> Vec<String> {
        let Some(client) = self.client.as_mut() else {
            return Vec::new();
        };
        let until = Instant::now() + window;
        let mut topics = Vec::new();
        let mut closed = false;
        while let Some(left) = until.checked_duration_since(Instant::now()) {
            match client.recv_bounded(left).await {
                Recv::Packet(Packet::Publish(p)) => {
                    match (p.qos, p.pkid) {
                        (QoS::AtLeastOnce, Some(pkid)) => client.puback(pkid).await,
                        (QoS::ExactlyOnce, Some(pkid)) => client.pubrec(pkid).await,
                        _ => {}
                    }
                    topics.push(p.topic);
                }
                Recv::Packet(Packet::PubRel(a)) => client.pubcomp(a.pkid).await,
                Recv::Packet(_) => {}
                Recv::Quiet => break,
                Recv::Closed => {
                    closed = true;
                    break;
                }
            }
        }
        if closed {
            self.client = None;
            self.closed_during |= standing;
        }
        for t in topics.iter().filter_map(|t| tag(t)) {
            if standing {
                self.during.push(t);
            }
            self.ever.push(t);
        }
        topics
    }

    /// Come back after a disconnect: a persistent session resumes on whichever node
    /// serves it now (its home first); a clean one reconnects at home and subscribes
    /// again. Leaves the subscriber disconnected if no node takes it yet.
    async fn reconnect(&mut self, cluster: &Cluster) {
        let home = cluster.node(self.on);
        let candidates: Vec<&Node> = if self.clean {
            vec![home]
        } else {
            let mut all = vec![home];
            all.extend(cluster.nodes().into_iter().filter(|n| n.id != home.id));
            all
        };
        for node in candidates {
            if let Ok((mut client, present)) = Client::connect_v311_explained(
                node.client_addr,
                &self.id,
                self.clean,
                Duration::from_secs(4),
            )
            .await
            {
                if self.clean {
                    client.subscribe(1, &self.filter, self.qos).await;
                } else {
                    assert!(present, "persistent session {} must still exist", self.id);
                }
                self.client = Some(client);
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------------
// Flows: one publisher, the subscribers owed its publishes, and what happened
// ---------------------------------------------------------------------------------

/// What became of one acknowledged-or-not publish, as a client sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    /// Acknowledged, and delivered while the false verdict still stood.
    Delivered,
    /// Acknowledged, and delivered only once the verdict was withdrawn (to the
    /// connected subscriber, or to the session when it resumed).
    DeliveredOnHeal,
    /// **Acknowledged with a success code and never delivered to anyone.** The defect
    /// of issue #973. No episode allows it any more: one that meets it has found
    /// the defect again.
    AckedAndLost,
    /// Not acknowledged (withheld, the connection closed, or refused with a reason
    /// code) and not delivered: the publisher knows, and retries.
    NotAcked,
    /// The acknowledgement was HELD while the verdict stood; once it was withdrawn the
    /// message was delivered and then acknowledged. Nothing lost, nothing claimed
    /// early: the publisher waited.
    HeldUntilHeal,
    /// The acknowledgement was held while the verdict stood, then GIVEN once it was
    /// withdrawn — and the message was never delivered. The same defect as
    /// [`AckedAndLost`](Fate::AckedAndLost), later.
    HeldThenLost,
    /// Not acknowledged, yet delivered: the publisher's retry will be a duplicate,
    /// which `QoS` 1 permits.
    DeliveredUnacked,
}

/// One flow's outcome.
#[derive(Debug, Clone)]
struct Seen {
    /// Published the moment A holds B dead — inside A's routing-settle window.
    first: Answer,
    /// Published once the first was answered — the verdict still standing.
    second: Answer,
    /// Per subscriber (`"b"`, `"c"`, `"a"`): [`TAGS`] received while the verdict stood,
    /// and over the whole episode.
    during: BTreeMap<&'static str, Vec<&'static str>>,
    ever: BTreeMap<&'static str, Vec<&'static str>>,
    /// Whether each publish's acknowledgement arrived only after the wait for it ended.
    acked_late: [bool; 2],
    /// A subscriber of this flow had its connection closed while the verdict stood.
    subscriber_closed: bool,
    /// From the `Alive` event to the first publish that reached a subscriber again,
    /// and how many publishes in that interval went unacknowledged.
    healed_after: Option<Duration>,
    unanswered_after_heal: u32,
}

impl Seen {
    fn got(map: &BTreeMap<&'static str, Vec<&'static str>>, which: &str) -> bool {
        map.values().any(|tags| tags.contains(&which))
    }

    fn fate(&self, which: &str) -> Fate {
        let (answer, late) = if which == "first" {
            (self.first, self.acked_late[0])
        } else {
            (self.second, self.acked_late[1])
        };
        match (
            answer.acked_ok(),
            Self::got(&self.during, which),
            Self::got(&self.ever, which),
        ) {
            (true, true, _) => Fate::Delivered,
            (true, false, true) => Fate::DeliveredOnHeal,
            (true, false, false) => Fate::AckedAndLost,
            (false, _, false) if late => Fate::HeldThenLost,
            (false, _, false) => Fate::NotAcked,
            (false, _, true) if late => Fate::HeldUntilHeal,
            (false, _, true) => Fate::DeliveredUnacked,
        }
    }

    fn fates(&self) -> [Fate; 2] {
        [self.fate("first"), self.fate("second")]
    }
}

struct Flow {
    name: &'static str,
    /// Publishes go to `<publish_under>/<tag>`; a subscriber sees them under
    /// `arrives_under` (different only where a rule republishes).
    publish_under: &'static str,
    qos: QoS,
    publisher: Publisher,
    subs: Vec<Sub>,
    first: Answer,
    second: Answer,
    /// The packet ids of the two publishes, to recognise a late acknowledgement.
    pkids: [u16; 2],
    healed_after: Option<Duration>,
    unanswered_after_heal: u32,
}

impl Flow {
    async fn new(
        cluster: &Cluster,
        name: &'static str,
        from: On,
        publish_under: &'static str,
        qos: QoS,
        subs: Vec<Sub>,
    ) -> Self {
        Flow {
            name,
            publish_under,
            qos,
            publisher: Publisher::connect(cluster, from, &format!("pub-{name}")).await,
            subs,
            first: Answer::Withheld,
            second: Answer::Withheld,
            pkids: [0; 2],
            healed_after: None,
            unanswered_after_heal: 0,
        }
    }

    async fn publish(&mut self, suffix: &str, within: Duration) -> Answer {
        let topic = format!("{}/{suffix}", self.publish_under);
        self.publisher
            .publish(&topic, self.qos, false, within)
            .await
    }

    async fn drain(&mut self, window: Duration, standing: bool) -> Vec<String> {
        let mut all = Vec::new();
        for sub in &mut self.subs {
            all.extend(sub.drain(window, standing).await);
        }
        all
    }

    /// Healthy cluster: publish until a subscriber receives one, then let the rest
    /// arrive. Until then the publishing node has not learned the interest.
    async fn warm(&mut self) {
        for i in 0..200 {
            assert!(
                self.publish(&format!("warm-{i}"), ACK_BOUND)
                    .await
                    .acked_ok(),
                "{}: a healthy cluster acknowledges",
                self.name
            );
            if !self
                .drain(Duration::from_millis(150), false)
                .await
                .is_empty()
            {
                let _ = self.drain(Duration::from_millis(400), false).await;
                return;
            }
        }
        panic!(
            "{}: the healthy cluster never routed a publish to a subscriber",
            self.name
        );
    }

    /// After the heal: publish until a subscriber receives one again, bringing back
    /// subscribers whose connections are gone. Whatever the heal itself re-delivers
    /// arrives no later than that publish does.
    async fn heal(&mut self, cluster: &Cluster) {
        let healing = Instant::now();
        let mut i = 0;
        while healing.elapsed() < HEAL_BOUND {
            for sub in &mut self.subs {
                if sub.client.is_none() {
                    sub.reconnect(cluster).await;
                }
            }
            i += 1;
            if !self
                .publish(&format!("after-{i}"), ACK_BOUND)
                .await
                .acked_ok()
            {
                self.unanswered_after_heal += 1;
            }
            let got = self.drain(Duration::from_millis(250), false).await;
            if got.iter().any(|t| t.contains("/after-")) {
                self.healed_after = Some(healing.elapsed());
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn seen(&self) -> Seen {
        let label = |on: On| match on {
            On::A => "a",
            On::B => "b",
            On::C => "c",
        };
        Seen {
            first: self.first,
            second: self.second,
            acked_late: self.pkids.map(|p| self.publisher.late_acks.contains(&p)),
            during: self
                .subs
                .iter()
                .filter(|s| !s.during.is_empty())
                .map(|s| (label(s.on), s.during.clone()))
                .collect(),
            ever: self
                .subs
                .iter()
                .filter(|s| !s.ever.is_empty())
                .map(|s| (label(s.on), s.ever.clone()))
                .collect(),
            subscriber_closed: self.subs.iter().any(|s| s.closed_during),
            healed_after: self.healed_after,
            unanswered_after_heal: self.unanswered_after_heal,
        }
    }
}

/// The counters and gauges of one node, by exposition line (`name{labels}` → value),
/// without histogram series.
fn series(node: &Node) -> BTreeMap<String, f64> {
    node.metrics
        .render()
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| {
            let (name, value) = l.rsplit_once(' ')?;
            Some((name.to_string(), value.parse().ok()?))
        })
        .filter(|(name, _)| {
            !(name.contains("_bucket") || name.contains("_seconds") || name.ends_with("_sum"))
        })
        .collect()
}

/// The series whose value changed between two readings, as `after - before`.
fn moved(before: &BTreeMap<String, f64>, after: &BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    after
        .iter()
        .filter_map(|(name, v)| {
            let delta = v - before.get(name).copied().unwrap_or(0.0);
            (delta.abs() > f64::EPSILON).then(|| (name.clone(), delta))
        })
        .collect()
}

/// The topics a fresh clean subscriber on `node` is handed as retained for `filter`.
async fn retained_on(node: &Node, id: &str, filter: &str) -> Vec<String> {
    let (mut probe, _) = Client::connect_v311(node.client_addr, id, true).await;
    probe.subscribe(1, filter, QoS::AtLeastOnce).await;
    let mut topics = Vec::new();
    let until = Instant::now() + Duration::from_secs(1);
    while let Some(left) = until.checked_duration_since(Instant::now()) {
        match probe.recv_bounded(left).await {
            Recv::Packet(Packet::Publish(p)) => {
                if let Some(pkid) = p.pkid {
                    probe.puback(pkid).await;
                }
                topics.push(p.topic);
            }
            Recv::Packet(_) => {}
            Recv::Quiet | Recv::Closed => break,
        }
    }
    topics
}

// ---------------------------------------------------------------------------------
// The episode
// ---------------------------------------------------------------------------------

/// How long a publisher waits for an acknowledgement before it is called withheld.
/// The longest one that does come is the first publish's, held for the routing-settle
/// window: seven to ten seconds on an idle machine. The bound is three times that, so
/// a loaded runner does not turn a slow acknowledgement into a withheld one.
const ACK_BOUND: Duration = Duration::from_secs(30);
/// How long after A's `Alive` event a path may take to work again.
const HEAL_BOUND: Duration = Duration::from_mins(2);
/// How long subscribers are read for deliveries that should not come.
const QUIET: Duration = Duration::from_secs(2);

/// Everything one false-`Dead` episode showed.
struct Episode {
    flows: BTreeMap<&'static str, Seen>,
    /// A's series that moved between the verdict and the heal: what an operator
    /// watching A could have seen.
    moved_on_a: BTreeMap<String, f64>,
    /// The answer to the retained publish made on A while the verdict stood.
    retained: Answer,
    /// Whether a fresh subscriber was handed that retained value on B, and on the
    /// bystander C, before the heal; and on B after it.
    reached_b_standing: bool,
    reached_bystander: bool,
    reached_b_healed: bool,
    /// The committed owner of B's persistent session as A, B and C saw it while the
    /// verdict stood (`None` in plain mode, where there are no leases).
    owner_during: [Option<NodeId>; 3],
    /// B's node id.
    b_id: NodeId,
}

impl Episode {
    fn flow(&self, name: &str) -> &Seen {
        &self.flows[name]
    }

    /// The sum of every moved series of `metric` on A (any labels).
    fn moved(&self, metric: &str) -> f64 {
        self.moved_on_a
            .iter()
            .filter(|(name, _)| name.starts_with(metric))
            .map(|(_, v)| *v)
            .sum()
    }

    /// Assert the fate of `flow`'s two publishes.
    #[track_caller]
    fn expect(&self, flow: &str, fates: [Fate; 2]) {
        self.expect_one_of(flow, [&fates[..1], &fates[1..]]);
    }

    /// Assert that each of `flow`'s two publishes met one of the fates allowed for it
    /// — for the rows where two mechanisms race and either may win.
    #[track_caller]
    fn expect_one_of(&self, flow: &str, allowed: [&[Fate]; 2]) {
        let seen = self.flow(flow);
        let fates = seen.fates();
        assert!(
            allowed[0].contains(&fates[0]) && allowed[1].contains(&fates[1]),
            "flow `{flow}`: the [first, second] publish of the episode met {fates:?}; \
             this test allows {allowed:?}. AckedAndLost and HeldThenLost are the defect \
             of issue #973 — a publish acknowledged and delivered to nobody. \
             Observed: {seen:#?}"
        );
    }

    /// What an operator watching A could and could not have seen: the members gauge
    /// counted one dead, and no drop counter moved.
    #[track_caller]
    fn expect_no_drop_was_counted(&self) {
        assert!(
            (self.moved("mqttd_members{state=\"dead\"}") - 1.0).abs() < f64::EPSILON,
            "the members gauge shows the verdict: {:?}",
            self.moved_on_a
        );
        assert!(
            self.moved("mqttd_publish_dropped_total").abs() < f64::EPSILON,
            "nothing is dropped across a false verdict, so no `publish_dropped` reason \
             may move: {:?}",
            self.moved_on_a
        );
    }
}

/// What an episode does to the pair A–B.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fault {
    /// A is told B is `Dead` — the false verdict — and told `Alive` again at the end
    /// of the episode, or after `withdrawn_after` if that is set.
    FalseDead { withdrawn_after: Option<Duration> },
    /// The link between A and B is cut and nothing else: no verdict anywhere. It is
    /// restored once both nodes report publishes held for the peer they cannot
    /// reach. (ADR 0087, finding 3: until `$share` membership survived a lost link,
    /// this alone lost `$share` deliveries.)
    LinkCut,
}

/// The subscriber kinds of ADR 0087's table, each with its own publisher and topic
/// tree so that one episode observes them all:
///
/// | flow | publisher | owed to |
/// |---|---|---|
/// | `clean` | A | (a) a clean-session subscriber on B |
/// | `clean-qos2` | A, `QoS` 2 | the same, subscribed at `QoS` 2 |
/// | `persistent` | A | (b)/(c) a persistent session on B, connected |
/// | `offline` | A | (b)/(c) a persistent session on B, offline until the heal |
/// | `share-b` | A | (d) a `$share` group whose only member is on B |
/// | `share-bc` | A | (e) a `$share` group with a member on B and one on C |
/// | `rule` | A | (g) what a rule on A republishes, wanted only on B |
/// | `from-c` | C | a clean-session subscriber on B (the bystander's view of B) |
/// | `reverse` | B | a clean-session subscriber on A (the evicted node's publishes) |
/// | `reverse-share` | B | a `$share` group whose only member is on A |
///
/// (f), retained, is probed separately: see [`Episode::retained`].
// One linear timeline: flows, warm-up, verdict, two publishes each, heal, read back.
#[allow(clippy::too_many_lines)]
async fn episode(
    mode: Mode,
    link: Link,
    founder: Founder,
    heal_after: Option<Duration>,
) -> Episode {
    let fault = Fault::FalseDead {
        withdrawn_after: heal_after,
    };
    episode_under(mode, link, founder, fault).await
}

/// [`episode`], under any [`Fault`].
#[allow(clippy::too_many_lines)]
async fn episode_under(mode: Mode, link: Link, founder: Founder, fault: Fault) -> Episode {
    let cluster = Cluster::start(mode, link, founder).await;
    let q1 = QoS::AtLeastOnce;
    let sub =
        |on, prefix, clean, filter, qos| Sub::connect(&cluster, on, prefix, clean, filter, qos);
    let mut flows = vec![
        Flow::new(
            &cluster,
            "clean",
            On::A,
            "fd/clean",
            q1,
            vec![sub(On::B, "sub-clean", true, "fd/clean/#", q1).await],
        )
        .await,
        Flow::new(
            &cluster,
            "clean-qos2",
            On::A,
            "fd/q2",
            QoS::ExactlyOnce,
            vec![sub(On::B, "sub-q2", true, "fd/q2/#", QoS::ExactlyOnce).await],
        )
        .await,
        Flow::new(
            &cluster,
            "persistent",
            On::A,
            "fd/pers",
            q1,
            vec![sub(On::B, "sub-pers", false, "fd/pers/#", q1).await],
        )
        .await,
        Flow::new(
            &cluster,
            "offline",
            On::A,
            "fd/off",
            q1,
            vec![sub(On::B, "sub-off", false, "fd/off/#", q1).await],
        )
        .await,
        Flow::new(
            &cluster,
            "share-b",
            On::A,
            "fd/shb",
            q1,
            vec![sub(On::B, "sub-shb", true, "$share/gb/fd/shb/#", q1).await],
        )
        .await,
        Flow::new(
            &cluster,
            "share-bc",
            On::A,
            "fd/shbc",
            q1,
            vec![
                sub(On::B, "sub-shbc-b", true, "$share/gbc/fd/shbc/#", q1).await,
                sub(On::C, "sub-shbc-c", true, "$share/gbc/fd/shbc/#", q1).await,
            ],
        )
        .await,
        Flow::new(
            &cluster,
            "rule",
            On::A,
            "fd/in",
            q1,
            vec![sub(On::B, "sub-rule", true, "fd/out/#", q1).await],
        )
        .await,
        Flow::new(
            &cluster,
            "from-c",
            On::C,
            "fd/fromc",
            q1,
            vec![sub(On::B, "sub-fromc", true, "fd/fromc/#", q1).await],
        )
        .await,
        Flow::new(
            &cluster,
            "reverse",
            On::B,
            "fd/rev",
            q1,
            vec![sub(On::A, "sub-rev", true, "fd/rev/#", q1).await],
        )
        .await,
        Flow::new(
            &cluster,
            "reverse-share",
            On::B,
            "fd/rsh",
            q1,
            vec![sub(On::A, "sub-rsh", true, "$share/gr/fd/rsh/#", q1).await],
        )
        .await,
    ];
    let flow_named = |flows: &[Flow], name: &str| {
        flows
            .iter()
            .position(|f| f.name == name)
            .expect("a flow of that name")
    };
    let pers_id = flows[flow_named(&flows, "persistent")].subs[0].id.clone();

    for flow in &mut flows {
        flow.warm().await;
    }
    {
        // The offline session's client goes away; the session, its subscription and
        // B's advertised interest stay. One more healthy publish must be queued for it.
        let at = flow_named(&flows, "offline");
        let offline = &mut flows[at];
        offline.subs[0].client.take().unwrap().disconnect().await;
        assert!(offline.publish("healthy", ACK_BOUND).await.acked_ok());
    }
    let mut retained_pub = Publisher::connect(&cluster, On::A, "pub-retained").await;

    let before = series(&cluster.a);
    match fault {
        Fault::FalseDead { .. } => cluster.falsely_declare_b_dead_on_a().await,
        Fault::LinkCut => cluster.cut_the_link().await,
    }
    let verdict = Instant::now();

    // Every flow publishes at once, each on its own connection: an acknowledgement
    // one flow has to wait for holds no other flow back.
    let withdraw_early = async {
        match fault {
            Fault::FalseDead {
                withdrawn_after: Some(after),
            } => {
                // SETTLE(false-dead-verdict-withdrawn-early): not a wait for a state —
                // the time IS the stimulus. The test asks what a verdict that stands
                // for this long, and no longer, costs; the first publishes are sent,
                // held and answered while it elapses.
                tokio::time::sleep(after.saturating_sub(verdict.elapsed())).await;
                cluster.heal();
            }
            Fault::FalseDead {
                withdrawn_after: None,
            } => {}
            Fault::LinkCut => cluster.restore_the_link_once_the_hold_is_visible().await,
        }
    };
    let (firsts, ()) = tokio::join!(
        join_all(flows.iter_mut().map(|f| f.publish("first", ACK_BOUND))),
        withdraw_early
    );
    for (flow, answer) in flows.iter_mut().zip(firsts) {
        flow.first = answer;
        flow.pkids[0] = flow.publisher.last_pkid;
    }
    let seconds = join_all(flows.iter_mut().map(|f| f.publish("second", ACK_BOUND))).await;
    for (flow, answer) in flows.iter_mut().zip(seconds) {
        flow.second = answer;
        flow.pkids[1] = flow.publisher.last_pkid;
    }
    let standing = fault
        == Fault::FalseDead {
            withdrawn_after: None,
        };
    join_all(flows.iter_mut().map(|f| f.drain(QUIET, standing))).await;

    // (f) Retained: published on A, then asked for on B and on C.
    let retained = retained_pub
        .publish("fd/ret/value", q1, true, ACK_BOUND)
        .await;
    let reached_b_standing = !retained_on(&cluster.b, "ret-b-during", "fd/ret/#")
        .await
        .is_empty();
    let reached_bystander = !retained_on(&cluster.c, "ret-c-during", "fd/ret/#")
        .await
        .is_empty();

    let moved_on_a = moved(&before, &series(&cluster.a));
    let owner_during = cluster.nodes().map(|n| {
        n.placement
            .read()
            .unwrap()
            .committed_session_owner(&pers_id)
    });

    if matches!(fault, Fault::FalseDead { .. }) {
        cluster.heal();
    }
    join_all(flows.iter_mut().map(|f| f.heal(&cluster))).await;
    join_all(flows.iter_mut().map(|f| f.drain(QUIET, false))).await;
    let mut reached_b_healed = false;
    let asked = Instant::now();
    while !reached_b_healed && asked.elapsed() < Duration::from_secs(30) {
        reached_b_healed = !retained_on(&cluster.b, "ret-b-after", "fd/ret/#")
            .await
            .is_empty();
    }

    let episode = Episode {
        flows: flows.iter().map(|f| (f.name, f.seen())).collect(),
        moved_on_a,
        retained,
        reached_b_standing,
        reached_bystander,
        reached_b_healed,
        owner_during,
        b_id: cluster.b.id.clone(),
    };
    cluster.membership_moved_only_as_injected();
    println!("--- {mode:?}, {link:?}, founder {founder:?}, {fault:?}");
    for (name, seen) in &episode.flows {
        println!(
            "  {name:<11} {:?}  first {:?}  second {:?}  during {:?}  ever {:?}  closed {}  \
             healed after {:?} ({} unanswered)",
            seen.fates(),
            seen.first,
            seen.second,
            seen.during,
            seen.ever,
            seen.subscriber_closed,
            seen.healed_after,
            seen.unanswered_after_heal,
        );
    }
    println!(
        "  retained    {:?}  on B during {}  on C during {}  on B after {}",
        episode.retained,
        episode.reached_b_standing,
        episode.reached_bystander,
        episode.reached_b_healed
    );
    println!(
        "  owner of B's persistent session as A, B, C saw it: {:?}",
        episode.owner_during
    );
    println!(
        "  moved on A: {:?}",
        episode
            .moved_on_a
            .iter()
            .filter(|(n, _)| n.contains("_total") || n.contains("mqttd_members"))
            .collect::<Vec<_>>()
    );
    for (name, seen) in &episode.flows {
        assert!(
            seen.healed_after.is_some(),
            "flow `{name}` must work again within {HEAL_BOUND:?} of the heal: {seen:#?}"
        );
    }
    episode
}

// ---------------------------------------------------------------------------------
// What every episode shows, whichever node dials the link (ADR 0087 D5)
// ---------------------------------------------------------------------------------

use Fate::{Delivered, DeliveredOnHeal, NotAcked};

/// The rows that do not depend on the mode, asserted identically for both link
/// orientations — 0087-T4's acceptance is that the "A dials" and "B dials" columns
/// are the same.
///
/// A's hub drops the link; whichever node owns it dials again within its 500 ms
/// retry, the accept path asks membership nothing, and both sides announce their
/// interest again — so the data path heals in about half a second while A's
/// membership still says `Dead`. Nothing was erased meanwhile (0087-T2): B's interest
/// and its `$share` members stayed in A's tables as unreachable, the first publish of
/// each flow was owed to B and held, and it went out the moment B had said what it
/// routes. The second finds the link there.
///
/// That includes the `$share` rows, which were the last to lose a publish: a group
/// whose only member is on B (`share-b`), and — B having merely lost a link — B's
/// publishes for a group whose only member is on A (`reverse-share`).
#[track_caller]
fn expect_the_link_returns_within_a_second(e: &Episode) {
    for flow in [
        "clean",
        "rule",
        "from-c",
        "reverse",
        "share-b",
        "share-bc",
        "reverse-share",
    ] {
        e.expect(flow, [Delivered, Delivered]);
    }
    e.expect("offline", [DeliveredOnHeal, DeliveredOnHeal]);
    // The offline session lost nothing it was owed BEFORE the verdict.
    assert!(e.flow("offline").ever["b"].contains(&"healthy"));

    // How long a publisher's connection is held (ADR 0087 D3: acknowledgements leave
    // a connection in publish order, so one held publish holds every later one on
    // it). Each flow's first publish is held until B answers it — the redial and a
    // handshake, about half a second on an idle machine. Asserted relative to a
    // yardstick from the same episode, so that load cannot decide it: the `rule`
    // flow's first publish (the original, which nobody subscribes to) is held for
    // the whole routing-settle window, seven to ten seconds.
    let whole_window = e.flow("rule").first.after().expect("acked");
    for flow in ["clean", "offline", "reverse", "share-b", "reverse-share"] {
        let held = e.flow(flow).first.after().expect("acked");
        println!("  held: `{flow}` waited {held:?} for its first acknowledgement");
        assert!(
            held < whole_window,
            "`{flow}`: the first publish waits for the redial ({held:?}), not for the \
             whole window ({whole_window:?})"
        );
    }

    // The operator's view: the verdict is on the members gauge, and nothing was
    // dropped.
    e.expect_no_drop_was_counted();
    // (f) Retained reaches B while the verdict stands: the link is there to carry it.
    assert!(e.retained.acked_ok() && e.reached_b_standing && e.reached_b_healed);
}

// ---------------------------------------------------------------------------------
// Plain mode (`MQTTD_DURABLE=0`): sessions in memory on the node that hosts them
// ---------------------------------------------------------------------------------

/// Plain mode, either orientation: nothing is lost.
#[track_caller]
fn expect_plain_sessions_are_served(e: &Episode) {
    expect_the_link_returns_within_a_second(e);
    for flow in ["clean-qos2", "persistent"] {
        e.expect(flow, [Delivered, Delivered]);
    }
    assert!(
        (e.moved("mqttd_rule_actions_total{rule=\"fwd\",result=\"ok\"}") - 2.0).abs()
            < f64::EPSILON,
        "both republishes were delivered and counted: {:?}",
        e.moved_on_a
    );
}

/// The orientation that used to lose everything (issue #973): the node holding the
/// false verdict dials the pair's link. Until ADR 0087 D5 its `Dead` arm stopped its
/// own dialer, nothing redialed before membership healed, and every `QoS` 1 and
/// `QoS` 2 publish that entered at A for a subscriber only B has — clean session,
/// persistent session (connected or offline), the target of a rule's republish — was
/// acknowledged with a success code and delivered to nobody, the first after the
/// eight-second settle window and the rest at once. Now A keeps dialing a peer it
/// holds `Dead`, and the episode reads as the other orientation does.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn plain_the_evictor_dials_the_link_returns_and_nothing_is_lost() {
    let e = episode(Mode::Plain, Link::EvictorDials, Founder::Evictor, None).await;
    expect_plain_sessions_are_served(&e);
}

/// The other orientation: the evicted node dials the link, and the node holding the
/// verdict accepts and serves it — a link is admitted by its handshake, not by
/// membership.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn plain_the_evicted_peer_dials_the_link_returns_and_nothing_is_lost() {
    let e = episode(Mode::Plain, Link::EvictedDials, Founder::Evictor, None).await;
    expect_plain_sessions_are_served(&e);
}

/// A verdict withdrawn after two seconds, in the orientation where the node that
/// holds it dials. The link was back a second and a half before the withdrawal, so
/// the `Alive` finds a dialer that is already linked and changes nothing: every flow
/// is delivered, and the first publish was acknowledged when the link returned, not
/// when membership healed. (Every delivery of this episode is read after the
/// withdrawal, so a delivered row reads [`Fate::DeliveredOnHeal`] here.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn plain_a_verdict_withdrawn_after_two_seconds_finds_the_link_already_back() {
    let e = episode(
        Mode::Plain,
        Link::EvictorDials,
        Founder::Evictor,
        Some(Duration::from_secs(2)),
    )
    .await;
    for flow in ALL_FLOWS {
        e.expect(flow, [DeliveredOnHeal, DeliveredOnHeal]);
    }
    // The first publish was acknowledged when the link returned — well inside the two
    // seconds the verdict stood — not when the verdict was withdrawn.
    let at_return = e.flow("clean").first.after().expect("acked");
    let at_window_end = e.flow("rule").first.after().expect("acked");
    assert!(
        at_return < at_window_end,
        "acknowledged when the link returned ({at_return:?}), not at the end of the \
         window ({at_window_end:?})"
    );
}

/// Every flow of an episode.
const ALL_FLOWS: [&str; 10] = [
    "clean",
    "clean-qos2",
    "persistent",
    "offline",
    "share-b",
    "share-bc",
    "rule",
    "from-c",
    "reverse",
    "reverse-share",
];

/// The link-cut-only row of the matrix: no verdict anywhere. The link between A and
/// B is cut — nothing else — and stays cut until both nodes report publishes held for
/// the peer they cannot reach; then it is restored.
///
/// Every publish made while the link was down is HELD, not acknowledged, and is
/// delivered and acknowledged once the link is back — the ordinary flows as they
/// always were (a lost link has kept a peer's interest since ADR 0042 T9), and since
/// 0087-T2 the two `$share` flows with them. Until then a lost link dropped the
/// peer's `$share` membership (ADR 0087, finding 3): `share-b` and `reverse-share`
/// found no group, were held while the mesh was not whole, and were acknowledged
/// when the link returned, placed with nobody.
///
/// (Every delivery of this episode is read after the link is restored, so a
/// delivered row reads [`Fate::DeliveredOnHeal`]; `from-c` and `share-bc` never
/// depended on the cut link at all.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn plain_a_link_cut_with_no_verdict_holds_every_publish_and_delivers_it() {
    for link in [Link::EvictorDials, Link::EvictedDials] {
        let e = episode_under(Mode::Plain, link, Founder::Evictor, Fault::LinkCut).await;
        for flow in ALL_FLOWS {
            e.expect(flow, [DeliveredOnHeal, DeliveredOnHeal]);
        }
        assert!(e.flow("offline").ever["b"].contains(&"healthy"));
        // Held for as long as the link was down, and not for a routing-settle window
        // on top of it.
        for flow in ["clean", "share-b", "reverse", "reverse-share"] {
            let held = e.flow(flow).first.after().expect("acked");
            println!("  held: `{flow}` waited {held:?} with the link cut ({link:?})");
        }
        // No verdict was involved, and nothing was dropped.
        assert!(
            e.moved("mqttd_members{state=\"dead\"}").abs() < f64::EPSILON
                && e.moved("mqttd_publish_dropped_total").abs() < f64::EPSILON,
            "{link:?}: {:?}",
            e.moved_on_a
        );
        assert!(e.retained.acked_ok() && e.reached_b_healed);
    }
}

// ---------------------------------------------------------------------------------
// Durable mode (the default): the lease group and the replicated session store
// ---------------------------------------------------------------------------------

/// A `QoS` 2 publish also records its packet id durably, for the PUBLISHER's session;
/// with a replica of that group on the node A has just written off, the first such
/// write can fail, and the broker then closes the connection rather than acknowledge
/// (not delivered, and the publisher knows). ADR 0087 D4 records it as observed and
/// unchanged by the design; the second publish is delivered.
#[track_caller]
fn expect_durable_qos2(e: &Episode) {
    e.expect_one_of("clean-qos2", [&[Delivered, NotAcked], &[Delivered]]);
}

/// Durable mode, the false verdict held by a node that is NOT the lease group's
/// leader, which dials the link. Nothing moves the leases, and since ADR 0087 D5 the
/// link is back in a second: a persistent session that B's lease group owns — the
/// subscriber the README's durability claim is about — receives both publishes while
/// the verdict stands. (Until D5 this episode lost both, connected or offline, and
/// the durable plane never heard of them.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn durable_a_follower_evictor_dials_and_a_durable_session_is_served() {
    let e = episode(Mode::Durable, Link::EvictorDials, Founder::Bystander, None).await;

    expect_the_link_returns_within_a_second(&e);
    expect_durable_qos2(&e);
    e.expect("persistent", [Delivered, Delivered]);

    // The leases did not move: B and C still name B the session's owner. (A has no
    // committed owner for it at all — the owner is not a member in A's view.)
    let b = e.owner_during[1].clone().expect("B knows the owner");
    assert_eq!(e.owner_during, [None, Some(b.clone()), Some(b)]);
}

/// Durable mode, the false verdict held by the lease group's LEADER — either
/// orientation. Here the verdict does more than drop routing state: the leader
/// reassigns B's groups, so the durable session's committed owner becomes C while B
/// is alive and serving the session's connected client. The link is back within a
/// second and the data path with it, so nothing owed to a session is lost — B closes
/// the durable session's connection for its client to follow the lease, and the
/// publishes reach that client as it resumes (a duplicate is possible: the client is
/// moved).
///
/// That the lease moves at all on a false verdict is the availability cost ADR 0087
/// §4 names; what keeps a publish safe across it without relying on the redial is
/// T5 (D8).
#[track_caller]
fn expect_sessions_follow_the_lease(e: &Episode) {
    expect_the_link_returns_within_a_second(e);
    expect_durable_qos2(e);
    let reaches_the_session: &[Fate] = &[Delivered, DeliveredOnHeal];
    e.expect_one_of("persistent", [reaches_the_session, reaches_the_session]);

    // The false verdict moved the session's lease away from the node serving it.
    let [a_says, _, c_says] = e.owner_during.clone();
    assert_eq!(a_says, c_says, "the leader's reassignment is committed");
    assert_ne!(
        a_says.as_ref(),
        Some(&e.b_id),
        "and it is no longer B's: {:?}",
        e.owner_during
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn durable_a_leader_evictor_dials_the_lease_moves_and_sessions_follow_it() {
    let e = episode(Mode::Durable, Link::EvictorDials, Founder::Evictor, None).await;
    expect_sessions_follow_the_lease(&e);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn durable_the_evicted_peer_dials_the_link_returns_and_sessions_follow_the_lease() {
    let e = episode(Mode::Durable, Link::EvictedDials, Founder::Evictor, None).await;
    expect_sessions_follow_the_lease(&e);
}
