//! The broker hub: a single-owner actor that holds the subscription table, the
//! session store, retained messages, and every connected client's outbound
//! channel.
//!
//! Connection tasks never share mutable state directly; they send [`HubCommand`]s
//! to the hub, which owns routing and session lifecycle. This actor model maps
//! cleanly onto the cluster design (ADR 0001): a node owns its local clients, and
//! cross-node routing becomes another command source feeding the same hub.
//!
//! ## Delivery semantics
//! Downstream delivery honors `QoS`: the effective `QoS` per subscriber is
//! `min(publish QoS, granted QoS)` [MQTT-3.8.4-6]. `QoS` 1/2 messages are
//! tracked per session in an in-flight table until acknowledged, are redelivered
//! with `DUP` on session resume [MQTT-4.4.0-1], and `QoS` 2 runs the
//! PUBREC/PUBREL/PUBCOMP handshake. Retained messages [MQTT-3.3.1] are stored in
//! a [`RetainedStore`] and replayed (with the retain flag set) on every new
//! subscription. A will message attached at CONNECT is published on any
//! ungraceful end of the connection — including session takeover — and
//! discarded on clean DISCONNECT [MQTT-3.14.4-3].
//!
//! ## Persistent sessions
//! A client connecting with `clean_session = false` (MQTT 3.1.1) gets a session
//! that survives disconnects: subscriptions stay in the routing table, matching
//! messages are enqueued in the [`SessionStore`] while it is offline, and
//! unacknowledged in-flight messages are redelivered on reconnect.
//!
//! The per-session **offline queue** is bounded (ADR 0001 §6, workstream A): a
//! cap with a drop-oldest/reject-newest policy. The per-connection **outbound
//! socket channel** is still unbounded; a bounded channel with an overload
//! policy remains a hardening item.

use bytes::Bytes;
use mqtt_cluster::durable_plane::DurablePlane;
use mqtt_cluster::peer::PeerMessage;
use mqtt_cluster::placement::Placement;
use mqtt_cluster::NodeId;
use mqtt_codec::{packet::Publish, Packet, QoS};
use mqtt_core::{topic_matches, ClientId, Message, SubscriptionTable};
use mqtt_storage::{
    Enqueued, MemoryRetainedStore, MemorySessionStore, RetainedStore, SessionStore,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, info, warn};

/// Maximum number of queued messages replayed to a reconnecting session at once.
const REPLAY_LIMIT: usize = 10_000;

/// How often the hub sweeps for sessions whose MQTT 5.0 Session Expiry Interval has
/// elapsed (ADR 0009). Second-grained expiry does not need a finer cadence.
const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// MQTT 5.0 Session Expiry Interval meaning "never expire" (0xFFFFFFFF). v3.1.1
/// `clean_session=0` maps to this.
const SESSION_EXPIRY_NEVER: u32 = u32::MAX;

/// Sender for packets destined to a single client's socket.
pub type Outbound = mpsc::UnboundedSender<Packet>;

/// Sender for messages destined to a peer node's link.
pub type PeerOutbound = mpsc::UnboundedSender<PeerMessage>;

/// A currently-online client connection.
#[derive(Debug)]
struct Online {
    /// Unique per-connection id, used to resolve takeover/disconnect races.
    conn_id: u64,
    /// Channel to this connection's writer.
    tx: Outbound,
    /// Will message published if this connection ends ungracefully.
    will: Option<Message>,
}

/// Downstream acknowledgement state of an unacked `QoS` > 0 message.
// The shared `AwaitingPub*` prefix mirrors the MQTT packet names; renaming to
// satisfy the lint would only make the states harder to map to the spec.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutState {
    /// `QoS` 1: PUBLISH sent, waiting for PUBACK.
    AwaitingPubAck,
    /// `QoS` 2: PUBLISH sent, waiting for PUBREC.
    AwaitingPubRec,
    /// `QoS` 2: PUBREL sent, waiting for PUBCOMP.
    AwaitingPubComp,
}

/// An unacknowledged outbound message.
#[derive(Debug)]
struct PendingOut {
    message: Message,
    state: OutState,
}

/// Per-session outbound `QoS` bookkeeping. Survives disconnects so persistent
/// sessions can resume their in-flight messages (redelivered with `DUP`).
#[derive(Debug, Default)]
struct Inflight {
    next_pkid: u16,
    pending: BTreeMap<u16, PendingOut>,
}

impl Inflight {
    /// Allocate the next free packet id (1..=65535, skipping ids in flight).
    fn alloc_pkid(&mut self) -> u16 {
        loop {
            self.next_pkid = self.next_pkid.wrapping_add(1);
            if self.next_pkid == 0 {
                self.next_pkid = 1;
            }
            if !self.pending.contains_key(&self.next_pkid) {
                return self.next_pkid;
            }
        }
    }
}

/// A message from a connection task to the hub.
#[derive(Debug)]
pub enum HubCommand {
    /// A client finished CONNECT; register it and (for persistent sessions)
    /// restore subscriptions and replay queued messages.
    Attach {
        /// The client identifier.
        client: ClientId,
        /// Unique id for this physical connection.
        conn_id: u64,
        /// MQTT 5.0 Clean Start: discard any existing session before attaching
        /// (v3.1.1 `clean_session=1` maps to `true`).
        clean_start: bool,
        /// MQTT 5.0 Session Expiry Interval (seconds) — how long to keep the session
        /// after disconnect; `0` discards at disconnect, `u32::MAX` never expires.
        session_expiry: u32,
        /// Will message to publish if the connection ends ungracefully.
        will: Option<Message>,
        /// Channel the hub uses to deliver packets to this client.
        outbound: Outbound,
        /// Reply with `session_present` so the connection can send CONNACK.
        reply: oneshot::Sender<bool>,
    },
    /// Add subscriptions (filter + granted `QoS`) for a client.
    Subscribe {
        /// The subscribing client.
        client: ClientId,
        /// Topic filters being subscribed to, with their granted `QoS`.
        filters: Vec<(String, QoS)>,
    },
    /// Remove subscriptions for a client.
    Unsubscribe {
        /// The unsubscribing client.
        client: ClientId,
        /// Topic filters being removed.
        filters: Vec<String>,
    },
    /// Route an application message to matching subscribers.
    Publish {
        /// Destination topic.
        topic: String,
        /// Application payload.
        payload: Bytes,
        /// Publish `QoS` (each subscriber receives `min(qos, granted)`).
        qos: QoS,
        /// Whether to store the message as the topic's retained message.
        retain: bool,
    },
    /// A subscriber acknowledged a `QoS` 1 delivery.
    PubAck {
        /// The acknowledging client.
        client: ClientId,
        /// The packet id being acknowledged.
        pkid: u16,
    },
    /// A subscriber acknowledged receipt of a `QoS` 2 delivery (step 1 of 2).
    PubRec {
        /// The acknowledging client.
        client: ClientId,
        /// The packet id being acknowledged.
        pkid: u16,
    },
    /// A subscriber completed a `QoS` 2 delivery (step 2 of 2).
    PubComp {
        /// The completing client.
        client: ClientId,
        /// The packet id being completed.
        pkid: u16,
    },
    /// A client's connection ended; deregister it (honoring takeover).
    Detach {
        /// The departing client.
        client: ClientId,
        /// The connection id that is ending.
        conn_id: u64,
        /// `true` for a clean DISCONNECT (the will is discarded); `false` for
        /// any other end (the will is published) [MQTT-3.14.4-3].
        graceful: bool,
    },

    /// A peer node's link came up; register it and send our interest snapshot.
    PeerConnected {
        /// The remote node.
        node: NodeId,
        /// Unique id for this physical peer link.
        conn_id: u64,
        /// Channel to send messages to that peer.
        tx: PeerOutbound,
    },
    /// A peer node's link went down.
    PeerDisconnected {
        /// The remote node.
        node: NodeId,
        /// The link id that ended.
        conn_id: u64,
    },
    /// The failure detector declared a node dead: drop its link and interest
    /// unconditionally (no `conn_id` guard — membership outranks any live link).
    PeerDead {
        /// The dead node.
        node: NodeId,
    },
    /// A peer announced its current subscription interest (full snapshot).
    RemoteInterest {
        /// The announcing node.
        node: NodeId,
        /// Every topic filter with subscribers on that node.
        filters: Vec<String>,
    },
    /// A publish forwarded from a peer, for **local** delivery only (never re-forwarded).
    RemotePublish {
        /// Destination topic.
        topic: String,
        /// Application payload.
        payload: Bytes,
        /// The original publish `QoS` (local downgrade still applies).
        qos: QoS,
    },
    /// A durable-plane frame (consensus / session-log replication, ADR 0006/0007)
    /// from `node`, routed to the [`DurablePlane`]. The hub spawns its handling so
    /// the (potentially slow) raft dispatch never blocks the actor loop, and sends
    /// any reply back over `node`'s link.
    DurableFrame {
        /// The peer the frame arrived from (where a reply is sent).
        node: NodeId,
        /// The durable-plane frame to route.
        frame: PeerMessage,
    },
    /// Liveness probe (the health endpoint): the hub replies as soon as the actor
    /// loop dequeues this command, proving the loop is draining and not wedged.
    Ping {
        /// Replied to with `()` when the loop reaches this command.
        reply: oneshot::Sender<()>,
    },
}

/// A connected peer node's link.
#[derive(Debug)]
struct Peer {
    conn_id: u64,
    tx: PeerOutbound,
}

/// The smaller of two `QoS` levels (delivery downgrade rule [MQTT-3.8.4-6]).
fn min_qos(a: QoS, b: QoS) -> QoS {
    if (a as u8) <= (b as u8) {
        a
    } else {
        b
    }
}

/// The broker routing actor.
#[derive(Debug)]
pub struct Hub {
    rx: mpsc::UnboundedReceiver<HubCommand>,
    /// This node's identity.
    node_id: NodeId,
    /// Currently-connected clients.
    online: HashMap<ClientId, Online>,
    /// Retained sessions and their MQTT 5.0 Session Expiry Interval (seconds). A
    /// client is present here iff its session survives disconnect (expiry != 0);
    /// v3.1.1 `clean_session=0` maps to `u32::MAX` (never expire). See ADR 0009.
    session_expiry: HashMap<ClientId, u32>,
    /// Disconnected sessions with a finite expiry, and the instant they expire. The
    /// sweep discards those past due; a reconnect cancels the entry.
    expiring: HashMap<ClientId, Instant>,
    /// Per-client subscription filters with their granted `QoS`.
    subs_by_client: HashMap<ClientId, HashMap<String, QoS>>,
    /// Routing index covering online clients and offline persistent sessions.
    table: SubscriptionTable,
    /// Per-session outbound `QoS` > 0 in-flight state.
    inflight: HashMap<ClientId, Inflight>,
    /// Durable session/queue storage. `Arc` so connections can share it (e.g. for
    /// the durable QoS-2 dedup window) — ADR 0007 §5.
    store: Arc<dyn SessionStore>,
    /// The durable-plane endpoint (consensus + replication), when durable sessions
    /// are enabled (ADR 0007). `None` for the single-node / non-durable default.
    durable_plane: Option<DurablePlane>,
    /// Retained message storage.
    retained: Box<dyn RetainedStore>,
    /// Connected peer nodes.
    peers: HashMap<NodeId, Peer>,
    /// Each peer's last-announced subscription interest (filters).
    remote_interest: HashMap<NodeId, HashSet<String>>,
    /// Live session-placement ring (ADR 0005). `None` outside a cluster. Read at
    /// persistent CONNECT to identify the session's owner.
    placement: Option<Arc<RwLock<Placement>>>,
}

impl Hub {
    /// Create the hub (default node id and in-memory stores) and the sender
    /// that connection tasks use to reach it.
    #[must_use]
    pub fn new() -> (Self, mpsc::UnboundedSender<HubCommand>) {
        Self::with_config(
            NodeId("node-local".to_string()),
            Arc::new(MemorySessionStore::new()),
        )
    }

    /// Create the hub with an explicit node id and [`SessionStore`] backend.
    /// Retained messages use an in-memory store; a pluggable backend arrives
    /// with the persistence phase.
    #[must_use]
    pub fn with_config(
        node_id: NodeId,
        store: Arc<dyn SessionStore>,
    ) -> (Self, mpsc::UnboundedSender<HubCommand>) {
        Self::with_config_and_placement(node_id, store, None)
    }

    /// As [`with_config`](Self::with_config), with a shared session-placement
    /// ring (ADR 0005) so the hub can identify which node owns each persistent
    /// session.
    #[must_use]
    pub fn with_config_and_placement(
        node_id: NodeId,
        store: Arc<dyn SessionStore>,
        placement: Option<Arc<RwLock<Placement>>>,
    ) -> (Self, mpsc::UnboundedSender<HubCommand>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                rx,
                node_id,
                online: HashMap::new(),
                session_expiry: HashMap::new(),
                expiring: HashMap::new(),
                subs_by_client: HashMap::new(),
                table: SubscriptionTable::new(),
                inflight: HashMap::new(),
                store,
                durable_plane: None,
                retained: Box::new(MemoryRetainedStore::new()),
                peers: HashMap::new(),
                remote_interest: HashMap::new(),
                placement,
            },
            tx,
        )
    }

    /// Attach the durable-plane endpoint (consensus + replication) before
    /// [`run`](Self::run). Enables routing of [`HubCommand::DurableFrame`]s and
    /// peer (de)registration on the plane. Only set when durable sessions are on.
    pub fn attach_durable_plane(&mut self, plane: DurablePlane) {
        self.durable_plane = Some(plane);
    }

    /// Run the hub event loop: dispatch commands and periodically sweep expired
    /// sessions (ADR 0009), until all command senders are dropped.
    pub async fn run(mut self) {
        let mut sweep = tokio::time::interval(SESSION_SWEEP_INTERVAL);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                cmd = self.rx.recv() => match cmd {
                    Some(cmd) => self.dispatch(cmd).await,
                    None => break,
                },
                _ = sweep.tick() => self.sweep_expired_sessions().await,
            }
        }
    }

    /// Dispatch one command to its handler.
    async fn dispatch(&mut self, cmd: HubCommand) {
        match cmd {
            HubCommand::Attach {
                client,
                conn_id,
                clean_start,
                session_expiry,
                will,
                outbound,
                reply,
            } => {
                self.attach(
                    client,
                    conn_id,
                    clean_start,
                    session_expiry,
                    will,
                    outbound,
                    reply,
                )
                .await;
            }
            HubCommand::Subscribe { client, filters } => {
                self.subscribe(&client, filters).await;
            }
            HubCommand::Unsubscribe { client, filters } => {
                self.unsubscribe(&client, &filters).await;
            }
            HubCommand::Publish {
                topic,
                payload,
                qos,
                retain,
            } => {
                self.publish(&topic, &payload, qos, retain).await;
            }
            HubCommand::PubAck { client, pkid } => self.pub_ack(&client, pkid),
            HubCommand::PubRec { client, pkid } => self.pub_rec(&client, pkid),
            HubCommand::PubComp { client, pkid } => self.pub_comp(&client, pkid),
            HubCommand::RemotePublish {
                topic,
                payload,
                qos,
            } => {
                // Forwarded from a peer: local delivery only (no re-forward).
                self.deliver_local(&topic, &payload, qos).await;
            }
            HubCommand::Detach {
                client,
                conn_id,
                graceful,
            } => {
                self.detach(&client, conn_id, graceful).await;
            }
            HubCommand::PeerConnected { node, conn_id, tx } => {
                self.peer_connected(node, conn_id, tx);
            }
            HubCommand::PeerDisconnected { node, conn_id } => {
                self.peer_disconnected(&node, conn_id);
            }
            HubCommand::PeerDead { node } => {
                self.peer_dead(&node);
            }
            HubCommand::DurableFrame { node, frame } => {
                self.handle_durable_frame(&node, frame);
            }
            HubCommand::Ping { reply } => {
                // Reached the loop → it is live. The receiver may be gone if the
                // prober timed out; that is fine.
                let _ = reply.send(());
            }
            HubCommand::RemoteInterest { node, filters } => {
                debug!(node = %node.0, filters = filters.len(), "remote interest updated");
                self.remote_interest
                    .insert(node, filters.into_iter().collect());
            }
        }
    }

    /// Publish an application message: store/clear retained state, deliver to
    /// local subscribers, and forward to interested peers.
    async fn publish(&mut self, topic: &str, payload: &Bytes, qos: QoS, retain: bool) {
        if retain {
            // A zero-length retained payload clears the retained message
            // [MQTT-3.3.1-10]; `RetainedStore::set` implements both cases.
            let message = Message {
                topic: topic.to_string(),
                payload: payload.clone(),
                qos,
                retain: true,
            };
            if let Err(e) = self.retained.set(&message).await {
                warn!(topic = %topic, error = %e, "failed to update retained message");
            }
        }
        // Live deliveries carry retain=0 [MQTT-3.3.1-9].
        self.deliver_local(topic, payload, qos).await;
        self.forward_to_peers(topic, payload, qos, retain);
    }

    /// Log when a persistent session is served on a node that is not its
    /// placement owner (ADR 0005). Until the session-proxy lands, such a session
    /// is served locally — sharded by landing node, but not yet relocated to its
    /// owner, and lost if *this* node dies (the ephemeral-sessions mode).
    fn note_session_ownership(&self, client: &ClientId) {
        let Some(placement) = &self.placement else {
            return;
        };
        let Ok(p) = placement.read() else { return };
        if p.member_count() > 1 && !p.owns(&client.0) {
            warn!(
                client = %client.0,
                owner = %p.owner(&client.0).0,
                "persistent session served locally but owned by another node \
                 (ephemeral mode; cross-node affinity is ADR 0005 step 2)"
            );
        }
    }

    // The parameters mirror the `Attach` command's fields one-for-one; bundling them
    // into a struct would only move the destructuring, not remove it.
    #[allow(clippy::too_many_arguments)]
    async fn attach(
        &mut self,
        client: ClientId,
        conn_id: u64,
        clean_start: bool,
        session_expiry: u32,
        will: Option<Message>,
        outbound: Outbound,
        reply: oneshot::Sender<bool>,
    ) {
        // A reconnect cancels any pending expiry for this session (ADR 0009).
        self.expiring.remove(&client);

        let session_present = if clean_start {
            // Clean Start: discard any prior session state for this client.
            self.discard_session(&client).await;
            false
        } else {
            self.note_session_ownership(&client);
            let existed = self.store.ensure_session(&client).await.unwrap_or(false);
            // Reconcile the routing table with persisted subscriptions (needed
            // after a broker restart; idempotent otherwise).
            if let Ok(subs) = self.store.subscriptions(&client).await {
                let map = self.subs_by_client.entry(client.clone()).or_default();
                for s in subs {
                    self.table.subscribe(client.clone(), s.filter.clone());
                    map.insert(s.filter, s.max_qos);
                }
            }
            existed
        };

        // Record this session's retention: it survives disconnect iff the expiry
        // interval is non-zero. A zero interval (or v3.1.1 clean_session=1) means the
        // session is dropped at disconnect.
        if session_expiry == 0 {
            self.session_expiry.remove(&client);
        } else {
            self.session_expiry.insert(client.clone(), session_expiry);
        }

        // Registering replaces any previous connection for this id; dropping the
        // old `Outbound` closes the old writer loop (takeover). The server-side
        // disconnect is not a client DISCONNECT, so the old will is published.
        if let Some(old) = self.online.remove(&client) {
            warn!(client = %client.0, "session takeover: replacing existing connection");
            if let Some(w) = old.will {
                self.publish(&w.topic, &w.payload, w.qos, w.retain).await;
            }
        }
        self.online.insert(
            client.clone(),
            Online {
                conn_id,
                tx: outbound.clone(),
                will,
            },
        );
        info!(client = %client.0, persistent = session_expiry != 0, session_present, "client attached");

        // Tell the connection the result so it can CONNACK before any replay.
        let _ = reply.send(session_present);

        // Resume in-flight QoS state: unacked PUBLISHes go out again with DUP
        // [MQTT-4.4.0-1]; half-completed QoS 2 deliveries resume at PUBREL.
        if let Some(inf) = self.inflight.get(&client) {
            for (pkid, p) in &inf.pending {
                let packet = match p.state {
                    OutState::AwaitingPubAck | OutState::AwaitingPubRec => publish_packet(
                        &p.message.topic,
                        p.message.payload.clone(),
                        p.message.qos,
                        Some(*pkid),
                        true,
                        false,
                    ),
                    OutState::AwaitingPubComp => Packet::PubRel((*pkid).into()),
                };
                let _ = outbound.send(packet);
            }
        }

        // Replay queued messages (they land in the channel after CONNACK).
        if !clean_start {
            if let Ok(pending) = self.store.pending(&client, 0, REPLAY_LIMIT).await {
                let mut last = 0;
                for qm in pending {
                    self.send_to_client(&client, &outbound, &qm.message, false);
                    last = qm.offset;
                }
                if last > 0 {
                    debug!(client = %client.0, up_to = last, "replayed queued messages");
                    let _ = self.store.ack(&client, last).await;
                }
            }
        }
    }

    async fn subscribe(&mut self, client: &ClientId, filters: Vec<(String, QoS)>) {
        for (f, q) in &filters {
            debug!(client = %client.0, filter = %f, qos = *q as u8, "subscribe");
            self.table.subscribe(client.clone(), f.clone());
            self.subs_by_client
                .entry(client.clone())
                .or_default()
                .insert(f.clone(), *q);
        }
        self.persist_subscriptions(client).await;
        self.gossip_interest();

        // Replay retained messages for every new subscription, with the retain
        // flag set [MQTT-3.3.1-6].
        let mut replay: Vec<Message> = Vec::new();
        for (f, q) in &filters {
            if let Ok(matching) = self.retained.matching(f).await {
                for m in matching {
                    replay.push(Message {
                        qos: min_qos(m.qos, *q),
                        retain: true,
                        ..m
                    });
                }
            }
        }
        if let Some(tx) = self.online.get(client).map(|s| s.tx.clone()) {
            for m in replay {
                self.send_to_client(client, &tx, &m, true);
            }
        }
    }

    async fn unsubscribe(&mut self, client: &ClientId, filters: &[String]) {
        if let Some(map) = self.subs_by_client.get_mut(client) {
            for f in filters {
                self.table.unsubscribe(client, f);
                map.remove(f);
            }
        }
        self.persist_subscriptions(client).await;
        self.gossip_interest();
    }

    /// The highest `QoS` granted to `client` across its filters matching `topic`.
    fn granted_qos(&self, client: &ClientId, topic: &str) -> QoS {
        self.subs_by_client
            .get(client)
            .into_iter()
            .flatten()
            .filter(|(f, _)| topic_matches(f, topic))
            .map(|(_, q)| *q)
            .max_by_key(|q| *q as u8)
            .unwrap_or(QoS::AtMostOnce)
    }

    /// Deliver a message to this node's local subscribers at
    /// `min(qos, granted)` each: online clients get it live (with `QoS` > 0
    /// tracked in flight), offline persistent sessions have it queued.
    async fn deliver_local(&mut self, topic: &str, payload: &Bytes, qos: QoS) {
        let targets: Vec<ClientId> = self.table.matching_clients(topic).into_iter().collect();
        debug!(topic = %topic, local_subscribers = targets.len(), "local delivery");
        for c in targets {
            let message = Message {
                topic: topic.to_string(),
                payload: payload.clone(),
                qos: min_qos(qos, self.granted_qos(&c, topic)),
                retain: false,
            };
            if let Some(tx) = self.online.get(&c).map(|s| s.tx.clone()) {
                self.send_to_client(&c, &tx, &message, false);
            } else if self.is_persistent(&c) {
                // Offline but persistent: queue for replay on reconnect. The
                // queue is bounded (ADR 0001 §6); log when the cap drops
                // messages — a metrics counter is the proper operator signal
                // and arrives with the observability phase.
                match self.store.enqueue(&c, &message).await {
                    Ok(Enqueued::Stored { evicted, .. }) if evicted > 0 => {
                        warn!(client = %c.0, evicted, topic = %topic,
                              "offline queue full: evicted oldest message(s)");
                    }
                    Ok(Enqueued::Rejected) => {
                        warn!(client = %c.0, topic = %topic,
                              "offline queue full: dropped message (reject-newest)");
                    }
                    Ok(Enqueued::Stored { .. }) => {}
                    Err(e) => {
                        warn!(client = %c.0, error = %e, "failed to enqueue offline message");
                    }
                }
            }
        }
    }

    /// Send one message to an online client at its (already downgraded) `QoS`,
    /// registering `QoS` > 0 deliveries in the in-flight table.
    fn send_to_client(
        &mut self,
        client: &ClientId,
        tx: &Outbound,
        message: &Message,
        retain: bool,
    ) {
        match message.qos {
            QoS::AtMostOnce => {
                // Ignore send errors: a closed channel means the client is gone
                // and a Detach is already in flight.
                let _ = tx.send(publish_packet(
                    &message.topic,
                    message.payload.clone(),
                    QoS::AtMostOnce,
                    None,
                    false,
                    retain,
                ));
            }
            qos => {
                let inf = self.inflight.entry(client.clone()).or_default();
                let pkid = inf.alloc_pkid();
                let state = if qos == QoS::AtLeastOnce {
                    OutState::AwaitingPubAck
                } else {
                    OutState::AwaitingPubRec
                };
                inf.pending.insert(
                    pkid,
                    PendingOut {
                        message: message.clone(),
                        state,
                    },
                );
                let _ = tx.send(publish_packet(
                    &message.topic,
                    message.payload.clone(),
                    qos,
                    Some(pkid),
                    false,
                    retain,
                ));
            }
        }
    }

    /// PUBACK: completes a `QoS` 1 delivery.
    fn pub_ack(&mut self, client: &ClientId, pkid: u16) {
        if let Some(inf) = self.inflight.get_mut(client) {
            if inf
                .pending
                .get(&pkid)
                .is_some_and(|p| p.state == OutState::AwaitingPubAck)
            {
                inf.pending.remove(&pkid);
            }
        }
    }

    /// PUBREC: advances a `QoS` 2 delivery to the release phase (send PUBREL).
    fn pub_rec(&mut self, client: &ClientId, pkid: u16) {
        let advanced =
            self.inflight
                .get_mut(client)
                .is_some_and(|inf| match inf.pending.get_mut(&pkid) {
                    Some(p) if p.state == OutState::AwaitingPubRec => {
                        p.state = OutState::AwaitingPubComp;
                        true
                    }
                    _ => false,
                });
        if advanced {
            if let Some(sess) = self.online.get(client) {
                let _ = sess.tx.send(Packet::PubRel(pkid.into()));
            }
        }
    }

    /// PUBCOMP: completes a `QoS` 2 delivery.
    fn pub_comp(&mut self, client: &ClientId, pkid: u16) {
        if let Some(inf) = self.inflight.get_mut(client) {
            if inf
                .pending
                .get(&pkid)
                .is_some_and(|p| p.state == OutState::AwaitingPubComp)
            {
                inf.pending.remove(&pkid);
            }
        }
    }

    async fn detach(&mut self, client: &ClientId, conn_id: u64, graceful: bool) {
        // Only act if this is still the current connection; a stale detach from a
        // connection that was already taken over must not disturb the new one.
        if self.online.get(client).map(|s| s.conn_id) != Some(conn_id) {
            return;
        }
        let departed = self.online.remove(client);
        // Any end other than a clean DISCONNECT publishes the will
        // [MQTT-3.14.4-3]; DISCONNECT discards it [MQTT-3.14.4-3].
        if !graceful {
            if let Some(w) = departed.and_then(|o| o.will) {
                info!(client = %client.0, topic = %w.topic, "publishing will (ungraceful disconnect)");
                self.publish(&w.topic, &w.payload, w.qos, w.retain).await;
            }
        }
        // Session retention (ADR 0009): expiry 0 discards now; u32::MAX keeps the
        // session indefinitely; a finite interval schedules expiry for the sweep.
        match self.session_expiry.get(client).copied() {
            None | Some(0) => {
                self.discard_session(client).await;
                info!(client = %client.0, "client detached (session discarded)");
                // Our local interest may have shrunk; let peers know.
                self.gossip_interest();
            }
            Some(SESSION_EXPIRY_NEVER) => {
                info!(client = %client.0, "client detached (session retained)");
            }
            Some(secs) => {
                let deadline = Instant::now() + Duration::from_secs(u64::from(secs));
                self.expiring.insert(client.clone(), deadline);
                info!(client = %client.0, expires_in_s = secs, "client detached (session expiring)");
            }
        }
    }

    /// Whether `client` has a retained session (survives disconnect) — its MQTT 5.0
    /// Session Expiry Interval is non-zero (ADR 0009).
    fn is_persistent(&self, client: &ClientId) -> bool {
        self.session_expiry.contains_key(client)
    }

    /// Discard a session entirely: routing subscriptions, in-flight state, the stored
    /// queue/metadata, and all expiry bookkeeping. Used by Clean Start, a zero-expiry
    /// disconnect, and the expiry sweep.
    async fn discard_session(&mut self, client: &ClientId) {
        self.drop_subscriptions(client);
        self.inflight.remove(client);
        self.session_expiry.remove(client);
        self.expiring.remove(client);
        let _ = self.store.remove(client).await;
    }

    /// Discard every session whose MQTT 5.0 Session Expiry Interval has elapsed
    /// (ADR 0009). Runs on the hub's periodic sweep tick.
    async fn sweep_expired_sessions(&mut self) {
        let now = Instant::now();
        let expired: Vec<ClientId> = self
            .expiring
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(client, _)| client.clone())
            .collect();
        if expired.is_empty() {
            return;
        }
        for client in &expired {
            self.discard_session(client).await;
            info!(client = %client.0, "session expired and discarded");
        }
        // Interest may have shrunk now that expired subscriptions are gone.
        self.gossip_interest();
    }

    /// Persist the current subscription set for a client if its session is durable.
    async fn persist_subscriptions(&mut self, client: &ClientId) {
        if !self.is_persistent(client) {
            return;
        }
        let subs: Vec<mqtt_core::Subscription> = self
            .subs_by_client
            .get(client)
            .into_iter()
            .flatten()
            .map(|(f, q)| mqtt_core::Subscription {
                filter: f.clone(),
                max_qos: *q,
                no_local: false,
            })
            .collect();
        let _ = self.store.set_subscriptions(client, &subs).await;
    }

    /// Remove all of a client's subscriptions from the routing table.
    fn drop_subscriptions(&mut self, client: &ClientId) {
        self.subs_by_client.remove(client);
        self.table.remove_client(client);
    }

    // --- cluster ---------------------------------------------------------------

    /// Forward a locally-originated publish to every peer that has matching
    /// interest. Receivers deliver it locally only, so there is no relay/loop.
    fn forward_to_peers(&self, topic: &str, payload: &Bytes, qos: QoS, retain: bool) {
        for (node, filters) in &self.remote_interest {
            if filters.iter().any(|f| topic_matches(f, topic)) {
                if let Some(peer) = self.peers.get(node) {
                    let _ = peer.tx.send(PeerMessage::Publish {
                        topic: topic.to_string(),
                        payload: payload.to_vec(),
                        qos: qos as u8,
                        retain,
                    });
                }
            }
        }
    }

    fn peer_connected(&mut self, node: NodeId, conn_id: u64, tx: PeerOutbound) {
        info!(local = %self.node_id.0, peer = %node.0, "peer link established");
        // Send our current interest so the peer can route to us immediately.
        let _ = tx.send(PeerMessage::Interest {
            filters: self.table.filters(),
        });
        // Register the link with the durable plane (consensus + replication) so its
        // RPCs to this peer route over the same channel.
        if let Some(plane) = &self.durable_plane {
            plane.register(&node, tx.clone());
        }
        self.peers.insert(node, Peer { conn_id, tx });
    }

    fn peer_disconnected(&mut self, node: &NodeId, conn_id: u64) {
        // Ignore a stale disconnect from a link that was already replaced.
        if self.peers.get(node).map(|p| p.conn_id) != Some(conn_id) {
            return;
        }
        info!(peer = %node.0, "peer link lost");
        self.peers.remove(node);
        self.remote_interest.remove(node);
        if let Some(plane) = &self.durable_plane {
            plane.fail(node);
        }
    }

    /// Drop all routing state for a node the failure detector confirmed dead.
    ///
    /// Removing the peer entry also drops its outbound sender, which closes the
    /// link's pump on whichever side still holds the socket open.
    fn peer_dead(&mut self, node: &NodeId) {
        let had_link = self.peers.remove(node).is_some();
        let had_interest = self.remote_interest.remove(node).is_some();
        if had_link || had_interest {
            info!(peer = %node.0, "peer declared dead; routing state dropped");
        }
        if let Some(plane) = &self.durable_plane {
            plane.fail(node);
        }
    }

    /// Route a durable-plane frame from `node`: spawn its handling (so a slow raft
    /// dispatch never blocks the actor loop) and send any reply back over the peer's
    /// link. A no-op when no durable plane is attached.
    fn handle_durable_frame(&self, node: &NodeId, frame: PeerMessage) {
        let Some(plane) = self.durable_plane.clone() else {
            return;
        };
        let reply_to = self.peers.get(node).map(|p| p.tx.clone());
        tokio::spawn(async move {
            if let Some(reply) = plane.handle(frame).await {
                if let Some(tx) = reply_to {
                    let _ = tx.send(reply);
                }
            }
        });
    }

    /// Send this node's current interest snapshot to all connected peers.
    fn gossip_interest(&self) {
        if self.peers.is_empty() {
            return;
        }
        let filters = self.table.filters();
        for peer in self.peers.values() {
            let _ = peer.tx.send(PeerMessage::Interest {
                filters: filters.clone(),
            });
        }
    }
}

fn publish_packet(
    topic: &str,
    payload: Bytes,
    qos: QoS,
    pkid: Option<u16>,
    dup: bool,
    retain: bool,
) -> Packet {
    Packet::Publish(Publish {
        properties: mqtt_codec::Properties::new(),
        dup,
        qos,
        retain,
        topic: topic.to_string(),
        pkid,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::{Hub, HubCommand, Outbound, PeerOutbound, REPLAY_LIMIT};
    use bytes::Bytes;
    use mqtt_cluster::peer::PeerMessage;
    use mqtt_cluster::NodeId;
    use mqtt_codec::{Packet, QoS};
    use mqtt_core::ClientId;
    use mqtt_storage::{MemorySessionStore, OverflowPolicy, QueueLimits};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::timeout;

    type HubTx = mpsc::UnboundedSender<HubCommand>;

    fn start_hub() -> HubTx {
        start_hub_with_store(MemorySessionStore::new())
    }

    fn start_hub_with_store(store: MemorySessionStore) -> HubTx {
        let (hub, tx) = Hub::with_config(NodeId("hub-test".into()), std::sync::Arc::new(store));
        tokio::spawn(hub.run());
        tx
    }

    /// Attach with the v3.1.1 `clean_session` semantics (the common test case):
    /// `clean_session=1` → clean start + expire-at-disconnect; `0` → resume + never
    /// expire. `attach_v5` covers explicit Session Expiry Intervals.
    async fn attach(
        tx: &HubTx,
        client: &str,
        conn_id: u64,
        clean_session: bool,
    ) -> (mpsc::UnboundedReceiver<Packet>, bool) {
        let expiry = if clean_session { 0 } else { u32::MAX };
        attach_v5(tx, client, conn_id, clean_session, expiry).await
    }

    /// Attach with explicit MQTT 5.0 `(clean_start, session_expiry)`.
    async fn attach_v5(
        tx: &HubTx,
        client: &str,
        conn_id: u64,
        clean_start: bool,
        session_expiry: u32,
    ) -> (mpsc::UnboundedReceiver<Packet>, bool) {
        let (out_tx, out_rx): (Outbound, _) = mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(HubCommand::Attach {
            client: ClientId(client.into()),
            conn_id,
            clean_start,
            session_expiry,
            will: None,
            outbound: out_tx,
            reply: reply_tx,
        })
        .unwrap();
        let session_present = reply_rx.await.unwrap();
        (out_rx, session_present)
    }

    fn detach(tx: &HubTx, client: &str, conn_id: u64) {
        tx.send(HubCommand::Detach {
            client: ClientId(client.into()),
            conn_id,
            graceful: true,
        })
        .unwrap();
    }

    fn subscribe(tx: &HubTx, client: &str, filter: &str) {
        tx.send(HubCommand::Subscribe {
            client: ClientId(client.into()),
            filters: vec![(filter.into(), QoS::AtMostOnce)],
        })
        .unwrap();
    }

    fn publish(tx: &HubTx, topic: &str, payload: &'static [u8]) {
        tx.send(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::from_static(payload),
            qos: QoS::AtMostOnce,
            retain: false,
        })
        .unwrap();
    }

    fn connect_peer(tx: &HubTx, node: &str, conn_id: u64) -> mpsc::UnboundedReceiver<PeerMessage> {
        let (peer_tx, peer_rx): (PeerOutbound, _) = mpsc::unbounded_channel();
        tx.send(HubCommand::PeerConnected {
            node: NodeId(node.into()),
            conn_id,
            tx: peer_tx,
        })
        .unwrap();
        peer_rx
    }

    fn remote_interest(tx: &HubTx, node: &str, filters: &[&str]) {
        tx.send(HubCommand::RemoteInterest {
            node: NodeId(node.into()),
            filters: filters.iter().map(|f| (*f).to_string()).collect(),
        })
        .unwrap();
    }

    async fn recv_packet(rx: &mut mpsc::UnboundedReceiver<Packet>) -> Option<Packet> {
        timeout(Duration::from_millis(300), rx.recv()).await.ok()?
    }

    async fn recv_peer(rx: &mut mpsc::UnboundedReceiver<PeerMessage>) -> Option<PeerMessage> {
        timeout(Duration::from_millis(300), rx.recv()).await.ok()?
    }

    fn payload_of(packet: &Packet) -> &[u8] {
        match packet {
            Packet::Publish(p) => &p.payload,
            other => panic!("expected a publish, got {other:?}"),
        }
    }

    /// A second connection for the same client id takes the session over: the
    /// old channel closes, and a stale `Detach` from the replaced connection
    /// must not disturb the new one (the `conn_id` guard).
    #[tokio::test]
    async fn takeover_replaces_connection_and_ignores_stale_detach() {
        let tx = start_hub();
        let (mut rx1, _) = attach(&tx, "c", 1, false).await;
        subscribe(&tx, "c", "t");

        let (mut rx2, present) = attach(&tx, "c", 2, false).await;
        assert!(present, "persistent session is present on takeover");
        assert!(
            recv_packet(&mut rx1).await.is_none(),
            "old connection's channel must close on takeover"
        );

        publish(&tx, "t", b"after-takeover");
        assert_eq!(
            payload_of(&recv_packet(&mut rx2).await.unwrap()),
            b"after-takeover"
        );

        // The replaced connection's deferred Detach arrives late.
        tx.send(HubCommand::Detach {
            client: ClientId("c".into()),
            conn_id: 1,
            graceful: false,
        })
        .unwrap();
        publish(&tx, "t", b"still-live");
        assert_eq!(
            payload_of(&recv_packet(&mut rx2).await.unwrap()),
            b"still-live",
            "a stale detach must not deregister the new connection"
        );
    }

    /// `PeerDead` drops the link and interest unconditionally; a stale
    /// `PeerDisconnected` from the old link must not kill a replacement link.
    #[tokio::test]
    async fn peer_dead_drops_routing_and_stale_peer_disconnect_is_ignored() {
        let tx = start_hub();
        let mut p1 = connect_peer(&tx, "n", 1);
        assert!(
            matches!(recv_peer(&mut p1).await, Some(PeerMessage::Interest { .. })),
            "link setup sends our interest snapshot"
        );
        remote_interest(&tx, "n", &["t/#"]);
        publish(&tx, "t/x", b"1");
        assert!(matches!(
            recv_peer(&mut p1).await,
            Some(PeerMessage::Publish { .. })
        ));

        tx.send(HubCommand::PeerDead {
            node: NodeId("n".into()),
        })
        .unwrap();
        assert!(
            recv_peer(&mut p1).await.is_none(),
            "dropping the peer entry must close its outbound channel"
        );

        // The node rejoins on a new link; the old link's Detach is still in flight.
        let mut p2 = connect_peer(&tx, "n", 2);
        assert!(matches!(
            recv_peer(&mut p2).await,
            Some(PeerMessage::Interest { .. })
        ));
        remote_interest(&tx, "n", &["t/#"]);
        tx.send(HubCommand::PeerDisconnected {
            node: NodeId("n".into()),
            conn_id: 1,
        })
        .unwrap();
        publish(&tx, "t/y", b"2");
        assert!(
            matches!(recv_peer(&mut p2).await, Some(PeerMessage::Publish { .. })),
            "a stale disconnect must not deregister the replacement link"
        );
    }

    /// Offline messages queue for persistent sessions (and replay in order on
    /// reconnect); clean sessions lose everything at detach.
    #[tokio::test]
    async fn offline_messages_queue_only_for_persistent_sessions() {
        let tx = start_hub();

        let (_rx, present) = attach(&tx, "p", 1, false).await;
        assert!(!present);
        subscribe(&tx, "p", "q/1");
        detach(&tx, "p", 1);
        publish(&tx, "q/1", b"first");
        publish(&tx, "q/1", b"second");

        let (mut rx, present) = attach(&tx, "p", 2, false).await;
        assert!(present);
        assert_eq!(payload_of(&recv_packet(&mut rx).await.unwrap()), b"first");
        assert_eq!(payload_of(&recv_packet(&mut rx).await.unwrap()), b"second");

        // Clean session: subscription and queue die with the connection.
        let (_rx, _) = attach(&tx, "c", 3, true).await;
        subscribe(&tx, "c", "q/2");
        detach(&tx, "c", 3);
        publish(&tx, "q/2", b"lost");
        let (mut rx, present) = attach(&tx, "c", 4, true).await;
        assert!(!present);
        assert!(recv_packet(&mut rx).await.is_none());
    }

    /// Connecting with `clean_session=true` discards any prior persistent state
    /// for that client id.
    #[tokio::test]
    async fn clean_session_attach_wipes_prior_persistent_state() {
        let tx = start_hub();
        let (_rx, _) = attach(&tx, "w", 1, false).await;
        subscribe(&tx, "w", "w/t");
        detach(&tx, "w", 1);

        let (_rx, present) = attach(&tx, "w", 2, true).await;
        assert!(!present, "clean attach must not report a session");
        detach(&tx, "w", 2);

        publish(&tx, "w/t", b"gone");
        let (mut rx, present) = attach(&tx, "w", 3, false).await;
        assert!(!present, "the persistent session was wiped");
        assert!(recv_packet(&mut rx).await.is_none(), "nothing was queued");
    }

    /// MQTT 5.0 Session Expiry Interval 0 (clean start = false) keeps the session for
    /// the connection but discards it at disconnect — nothing is queued afterwards
    /// and the next connect sees no prior session (ADR 0009).
    #[tokio::test]
    async fn session_expiry_zero_discards_at_disconnect() {
        let tx = start_hub();
        let (_rx, _) = attach_v5(&tx, "z", 1, false, 0).await;
        subscribe(&tx, "z", "z/t");
        detach(&tx, "z", 1);
        publish(&tx, "z/t", b"lost");

        let (mut rx, present) = attach_v5(&tx, "z", 2, false, 0).await;
        assert!(
            !present,
            "a zero-expiry session must not survive disconnect"
        );
        assert!(recv_packet(&mut rx).await.is_none(), "nothing was queued");
    }

    /// A finite Session Expiry Interval retains the session (offline messages queue),
    /// then the sweep discards it once the interval elapses (ADR 0009).
    #[tokio::test(start_paused = true)]
    async fn session_expiry_finite_retains_then_expires() {
        let tx = start_hub();
        let (_rx, _) = attach_v5(&tx, "e", 1, false, 1).await;
        subscribe(&tx, "e", "e/t");
        detach(&tx, "e", 1);
        // Retained during the expiry window: the offline message queues.
        publish(&tx, "e/t", b"m");

        // Advance past the 1s interval; the hub's sweep discards the session.
        tokio::time::sleep(Duration::from_secs(3)).await;

        let (mut rx, present) = attach_v5(&tx, "e", 2, false, 1).await;
        assert!(!present, "the session must have expired");
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "the expired session's queue is gone"
        );
    }

    /// Reconnecting before the expiry interval elapses cancels the pending expiry:
    /// the session is still present, with its queued messages intact (ADR 0009).
    #[tokio::test(start_paused = true)]
    async fn session_expiry_reconnect_cancels_expiry() {
        let tx = start_hub();
        let (_rx, _) = attach_v5(&tx, "r", 1, false, 100).await;
        subscribe(&tx, "r", "r/t");
        detach(&tx, "r", 1);
        publish(&tx, "r/t", b"kept");

        // Well within the 100s window; the session must still be there.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let (mut rx, present) = attach_v5(&tx, "r", 2, false, 100).await;
        assert!(
            present,
            "the session must survive a reconnect within its expiry"
        );
        assert_eq!(payload_of(&recv_packet(&mut rx).await.unwrap()), b"kept");

        // It is no longer scheduled to expire: advancing past the original deadline
        // leaves the now-online session untouched.
        tokio::time::sleep(Duration::from_secs(200)).await;
        publish(&tx, "r/t", b"still-here");
        assert_eq!(
            payload_of(&recv_packet(&mut rx).await.unwrap()),
            b"still-here"
        );
    }

    /// Publishes fan out only to peers whose announced interest matches
    /// (wildcards honored), and a peer-forwarded publish is never re-forwarded.
    #[tokio::test]
    async fn publishes_forward_only_to_peers_with_matching_interest() {
        let tx = start_hub();
        let mut p1 = connect_peer(&tx, "n1", 1);
        let mut p2 = connect_peer(&tx, "n2", 2);
        recv_peer(&mut p1).await; // initial interest snapshots
        recv_peer(&mut p2).await;
        remote_interest(&tx, "n1", &["a/+/b"]);
        remote_interest(&tx, "n2", &["x/#"]);

        publish(&tx, "a/q/b", b"to-n1");
        match recv_peer(&mut p1).await {
            Some(PeerMessage::Publish { topic, .. }) => assert_eq!(topic, "a/q/b"),
            other => panic!("n1 should receive the publish, got {other:?}"),
        }

        publish(&tx, "x/1", b"to-n2");
        match recv_peer(&mut p2).await {
            Some(PeerMessage::Publish { topic, .. }) => assert_eq!(topic, "x/1"),
            other => panic!("n2 should receive the publish, got {other:?}"),
        }

        // A publish forwarded *from* a peer is delivered locally only.
        tx.send(HubCommand::RemotePublish {
            topic: "x/2".into(),
            payload: Bytes::from_static(b"no-relay"),
            qos: QoS::AtMostOnce,
        })
        .unwrap();
        // Neither peer may see anything further (n1's non-match included).
        assert!(recv_peer(&mut p2).await.is_none(), "remote publish relayed");
        assert!(p1.try_recv().is_err(), "n1 got a non-matching publish");
    }

    /// Local interest changes (subscribe / unsubscribe / clean-session detach)
    /// are gossiped to every connected peer as fresh snapshots.
    #[tokio::test]
    async fn interest_snapshots_follow_subscription_changes() {
        let tx = start_hub();
        let mut p = connect_peer(&tx, "n", 1);
        match recv_peer(&mut p).await {
            Some(PeerMessage::Interest { filters }) => assert!(filters.is_empty()),
            other => panic!("expected the initial snapshot, got {other:?}"),
        }

        let (_rx, _) = attach(&tx, "g", 1, true).await;
        subscribe(&tx, "g", "g/1");
        match recv_peer(&mut p).await {
            Some(PeerMessage::Interest { filters }) => assert_eq!(filters, vec!["g/1"]),
            other => panic!("expected updated interest, got {other:?}"),
        }

        tx.send(HubCommand::Unsubscribe {
            client: ClientId("g".into()),
            filters: vec!["g/1".into()],
        })
        .unwrap();
        match recv_peer(&mut p).await {
            Some(PeerMessage::Interest { filters }) => assert!(filters.is_empty()),
            other => panic!("expected emptied interest, got {other:?}"),
        }

        // A clean-session client disappearing also shrinks our interest.
        subscribe(&tx, "g", "g/2");
        recv_peer(&mut p).await; // snapshot with g/2
        detach(&tx, "g", 1);
        match recv_peer(&mut p).await {
            Some(PeerMessage::Interest { filters }) => assert!(filters.is_empty()),
            other => panic!("expected post-detach interest, got {other:?}"),
        }
    }

    /// Replay is bounded by `REPLAY_LIMIT` per reconnect; the remainder stays
    /// queued (unacked) for the next one.
    #[tokio::test]
    async fn replay_is_bounded_and_resumes_on_next_connect() {
        let tx = start_hub();
        let (_rx, _) = attach(&tx, "r", 1, false).await;
        subscribe(&tx, "r", "rl");
        detach(&tx, "r", 1);
        for _ in 0..(REPLAY_LIMIT + 2) {
            publish(&tx, "rl", b"m");
        }

        let (mut rx, _) = attach(&tx, "r", 2, false).await;
        let mut replayed = 0usize;
        while recv_packet(&mut rx).await.is_some() {
            replayed += 1;
        }
        assert_eq!(replayed, REPLAY_LIMIT);

        detach(&tx, "r", 2);
        let (mut rx, _) = attach(&tx, "r", 3, false).await;
        let mut rest = 0usize;
        while recv_packet(&mut rx).await.is_some() {
            rest += 1;
        }
        assert_eq!(rest, 2, "unreplayed tail must survive for the next connect");
    }

    /// A bounded offline queue (ADR 0001 §6) drops the oldest while a persistent
    /// subscriber is offline; on reconnect it replays only the newest messages
    /// within the cap, not an unbounded backlog.
    #[tokio::test]
    async fn offline_queue_is_bounded_and_replays_newest() {
        let tx = start_hub_with_store(MemorySessionStore::with_limits(QueueLimits {
            max_messages: 3,
            overflow: OverflowPolicy::DropOldest,
        }));
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe(&tx, "p", "t");
        detach(&tx, "p", 1);

        // Five messages arrive offline; the cap-3 queue keeps the newest three.
        for n in [b"m1", b"m2", b"m3", b"m4", b"m5"] {
            publish(&tx, "t", n);
        }

        let (mut rx, present) = attach(&tx, "p", 2, false).await;
        assert!(present);
        let mut got: Vec<Vec<u8>> = Vec::new();
        while let Some(pkt) = recv_packet(&mut rx).await {
            got.push(payload_of(&pkt).to_vec());
        }
        assert_eq!(
            got,
            vec![b"m3".to_vec(), b"m4".to_vec(), b"m5".to_vec()],
            "only the newest cap-many messages survive the offline window"
        );
    }
}
