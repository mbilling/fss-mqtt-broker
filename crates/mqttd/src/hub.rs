//! The broker hub: a single-owner actor that holds the subscription table, the
//! session store, and every connected client's outbound channel.
//!
//! Connection tasks never share mutable state directly; they send [`HubCommand`]s
//! to the hub, which owns routing and session lifecycle. This actor model maps
//! cleanly onto the cluster design (ADR 0001): a node owns its local clients, and
//! cross-node routing becomes another command source feeding the same hub.
//!
//! ## Persistent sessions
//! A client connecting with `clean_session = false` (MQTT 3.1.1) gets a session
//! that survives disconnects:
//! - its subscriptions stay in the routing table while it is offline, so matching
//!   messages are **enqueued** in the [`SessionStore`] instead of dropped;
//! - on reconnect the broker reports `session_present = true` and **replays** the
//!   queued messages before resuming live delivery.
//!
//! Note: downstream delivery is still `QoS` 0 in this milestone, so offline
//! queueing currently applies to all matching messages and replayed messages are
//! sent at `QoS` 0. `QoS`-aware queueing arrives with `QoS` 1/2 delivery.
//!
//! Outbound queues are currently *unbounded*; bounded queues with an overload
//! policy are a Phase-2 hardening item (and the per-session queue caps in ADR 0001).

use bytes::Bytes;
use mqtt_cluster::peer::PeerMessage;
use mqtt_cluster::NodeId;
use mqtt_codec::{packet::Publish, Packet, QoS};
use mqtt_core::{topic_matches, ClientId, Message, SubscriptionTable};
use mqtt_storage::{MemorySessionStore, SessionStore};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

/// Maximum number of queued messages replayed to a reconnecting session at once.
const REPLAY_LIMIT: usize = 10_000;

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
        /// `false` keeps the session across disconnects (MQTT `clean_session=0`).
        clean_session: bool,
        /// Channel the hub uses to deliver packets to this client.
        outbound: Outbound,
        /// Reply with `session_present` so the connection can send CONNACK.
        reply: oneshot::Sender<bool>,
    },
    /// Add subscriptions for a client.
    Subscribe {
        /// The subscribing client.
        client: ClientId,
        /// Topic filters being subscribed to.
        filters: Vec<String>,
    },
    /// Remove subscriptions for a client.
    Unsubscribe {
        /// The unsubscribing client.
        client: ClientId,
        /// Topic filters being removed.
        filters: Vec<String>,
    },
    /// Route an application message to matching subscribers (`QoS` 0 live,
    /// or enqueued for offline persistent sessions).
    Publish {
        /// Destination topic.
        topic: String,
        /// Application payload.
        payload: Bytes,
    },
    /// A client's connection ended; deregister it (honoring takeover).
    Detach {
        /// The departing client.
        client: ClientId,
        /// The connection id that is ending.
        conn_id: u64,
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
    },
}

/// A connected peer node's link.
#[derive(Debug)]
struct Peer {
    conn_id: u64,
    tx: PeerOutbound,
}

/// The broker routing actor.
#[derive(Debug)]
pub struct Hub {
    rx: mpsc::UnboundedReceiver<HubCommand>,
    /// This node's identity.
    node_id: NodeId,
    /// Currently-connected clients.
    online: HashMap<ClientId, Online>,
    /// Clients whose current session is persistent (`clean_session=0`).
    persistent: HashSet<ClientId>,
    /// Per-client subscription filters, for persistence and clean removal.
    subs_by_client: HashMap<ClientId, HashSet<String>>,
    /// Routing index covering online clients and offline persistent sessions.
    table: SubscriptionTable,
    /// Durable session/queue storage.
    store: Box<dyn SessionStore>,
    /// Connected peer nodes.
    peers: HashMap<NodeId, Peer>,
    /// Each peer's last-announced subscription interest (filters).
    remote_interest: HashMap<NodeId, HashSet<String>>,
}

impl Hub {
    /// Create the hub (default node id and in-memory session store) and the
    /// sender that connection tasks use to reach it.
    #[must_use]
    pub fn new() -> (Self, mpsc::UnboundedSender<HubCommand>) {
        Self::with_config(
            NodeId("node-local".to_string()),
            Box::new(MemorySessionStore::new()),
        )
    }

    /// Create the hub with an explicit node id and [`SessionStore`] backend.
    #[must_use]
    pub fn with_config(
        node_id: NodeId,
        store: Box<dyn SessionStore>,
    ) -> (Self, mpsc::UnboundedSender<HubCommand>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                rx,
                node_id,
                online: HashMap::new(),
                persistent: HashSet::new(),
                subs_by_client: HashMap::new(),
                table: SubscriptionTable::new(),
                store,
                peers: HashMap::new(),
                remote_interest: HashMap::new(),
            },
            tx,
        )
    }

    /// Run the hub event loop until all command senders are dropped.
    pub async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                HubCommand::Attach {
                    client,
                    conn_id,
                    clean_session,
                    outbound,
                    reply,
                } => {
                    self.attach(client, conn_id, clean_session, outbound, reply)
                        .await;
                }
                HubCommand::Subscribe { client, filters } => {
                    self.subscribe(&client, filters).await;
                }
                HubCommand::Unsubscribe { client, filters } => {
                    self.unsubscribe(&client, &filters).await;
                }
                HubCommand::Publish { topic, payload } => {
                    // Originated locally: deliver to local subscribers and forward
                    // to interested peers.
                    self.deliver_local(&topic, &payload).await;
                    self.forward_to_peers(&topic, &payload);
                }
                HubCommand::RemotePublish { topic, payload } => {
                    // Forwarded from a peer: local delivery only (no re-forward).
                    self.deliver_local(&topic, &payload).await;
                }
                HubCommand::Detach { client, conn_id } => {
                    self.detach(&client, conn_id).await;
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
                HubCommand::RemoteInterest { node, filters } => {
                    debug!(node = %node.0, filters = filters.len(), "remote interest updated");
                    self.remote_interest
                        .insert(node, filters.into_iter().collect());
                }
            }
        }
    }

    async fn attach(
        &mut self,
        client: ClientId,
        conn_id: u64,
        clean_session: bool,
        outbound: Outbound,
        reply: oneshot::Sender<bool>,
    ) {
        let session_present = if clean_session {
            // Discard any prior session state for this client.
            self.drop_subscriptions(&client);
            self.persistent.remove(&client);
            let _ = self.store.remove(&client).await;
            false
        } else {
            self.persistent.insert(client.clone());
            let existed = self.store.ensure_session(&client).await.unwrap_or(false);
            // Reconcile the routing table with persisted subscriptions (needed
            // after a broker restart; idempotent otherwise).
            if let Ok(subs) = self.store.subscriptions(&client).await {
                let set = self.subs_by_client.entry(client.clone()).or_default();
                for s in subs {
                    self.table.subscribe(client.clone(), s.filter.clone());
                    set.insert(s.filter);
                }
            }
            existed
        };

        // Registering replaces any previous connection for this id; dropping the
        // old `Outbound` causes the old connection's writer loop to close (takeover).
        if self.online.contains_key(&client) {
            warn!(client = %client.0, "session takeover: replacing existing connection");
        }
        self.online.insert(
            client.clone(),
            Online {
                conn_id,
                tx: outbound.clone(),
            },
        );
        info!(client = %client.0, persistent = !clean_session, session_present, "client attached");

        // Tell the connection the result so it can CONNACK before any replay.
        let _ = reply.send(session_present);

        // Replay queued messages (they land in the channel after CONNACK is sent).
        if !clean_session {
            if let Ok(pending) = self.store.pending(&client, 0, REPLAY_LIMIT).await {
                let mut last = 0;
                for qm in pending {
                    let _ = outbound.send(publish_packet(&qm.message.topic, qm.message.payload));
                    last = qm.offset;
                }
                if last > 0 {
                    debug!(client = %client.0, up_to = last, "replayed queued messages");
                    let _ = self.store.ack(&client, last).await;
                }
            }
        }
    }

    async fn subscribe(&mut self, client: &ClientId, filters: Vec<String>) {
        let set = self.subs_by_client.entry(client.clone()).or_default();
        for f in filters {
            debug!(client = %client.0, filter = %f, "subscribe");
            self.table.subscribe(client.clone(), f.clone());
            set.insert(f);
        }
        self.persist_subscriptions(client).await;
        self.gossip_interest();
    }

    async fn unsubscribe(&mut self, client: &ClientId, filters: &[String]) {
        if let Some(set) = self.subs_by_client.get_mut(client) {
            for f in filters {
                self.table.unsubscribe(client, f);
                set.remove(f);
            }
        }
        self.persist_subscriptions(client).await;
        self.gossip_interest();
    }

    /// Deliver a message to this node's local subscribers: online clients get it
    /// live, offline persistent sessions have it queued. Does not touch peers.
    async fn deliver_local(&mut self, topic: &str, payload: &Bytes) {
        let targets: Vec<ClientId> = self.table.matching_clients(topic).into_iter().collect();
        debug!(topic = %topic, local_subscribers = targets.len(), "local delivery");
        for c in targets {
            if let Some(sess) = self.online.get(&c) {
                // Ignore send errors: a closed channel means the client is gone
                // and a Detach is already in flight.
                let _ = sess.tx.send(publish_packet(topic, payload.clone()));
            } else if self.persistent.contains(&c) {
                // Offline but persistent: queue for replay on reconnect.
                let message = Message {
                    topic: topic.to_string(),
                    payload: payload.clone(),
                    qos: QoS::AtMostOnce,
                    retain: false,
                };
                if let Err(e) = self.store.enqueue(&c, &message).await {
                    warn!(client = %c.0, error = %e, "failed to enqueue offline message");
                }
            }
        }
    }

    async fn detach(&mut self, client: &ClientId, conn_id: u64) {
        // Only act if this is still the current connection; a stale detach from a
        // connection that was already taken over must not disturb the new one.
        if self.online.get(client).map(|s| s.conn_id) != Some(conn_id) {
            return;
        }
        self.online.remove(client);
        if self.persistent.contains(client) {
            // Keep subscriptions and queued state so messages are queued offline.
            info!(client = %client.0, "client detached (session retained)");
        } else {
            self.drop_subscriptions(client);
            let _ = self.store.remove(client).await;
            info!(client = %client.0, "client detached (session discarded)");
            // Our local interest may have shrunk; let peers know.
            self.gossip_interest();
        }
    }

    /// Persist the current subscription set for a client if its session is durable.
    async fn persist_subscriptions(&mut self, client: &ClientId) {
        if !self.persistent.contains(client) {
            return;
        }
        let subs: Vec<mqtt_core::Subscription> = self
            .subs_by_client
            .get(client)
            .into_iter()
            .flatten()
            .map(|f| mqtt_core::Subscription {
                filter: f.clone(),
                max_qos: QoS::AtMostOnce,
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
    fn forward_to_peers(&self, topic: &str, payload: &Bytes) {
        for (node, filters) in &self.remote_interest {
            if filters.iter().any(|f| topic_matches(f, topic)) {
                if let Some(peer) = self.peers.get(node) {
                    let _ = peer.tx.send(PeerMessage::Publish {
                        topic: topic.to_string(),
                        payload: payload.to_vec(),
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

fn publish_packet(topic: &str, payload: Bytes) -> Packet {
    Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: topic.to_string(),
        pkid: None,
        payload,
    })
}
