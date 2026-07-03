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
use mqtt_core::{
    parse_shared, topic_matches, AppProperties, ClientId, Message, SharedSubscriptionTable,
    Subscription, SubscriptionTable,
};
use mqtt_storage::{
    Enqueued, MemoryRetainedStore, MemorySessionStore, RetainedStore, SessionClaim, SessionStore,
    StorageError,
};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, info, warn};

/// Maximum number of queued messages replayed to a reconnecting session at once.
const REPLAY_LIMIT: usize = 10_000;

/// Default outbound Receive Maximum when the client advertised none — effectively
/// unlimited (ADR 0012). v3.1.1 sessions always use this.
const RECEIVE_MAXIMUM_DEFAULT: u16 = u16::MAX;

/// Maximum `QoS` > 0 messages held in a session's flow-control backlog before
/// drop-oldest evicts (ADR 0012). Bounds broker memory under a stalled consumer,
/// mirroring the offline-queue cap (ADR 0001 §6).
const MAX_BACKLOG: usize = 10_000;

/// How often the hub sweeps for sessions whose MQTT 5.0 Session Expiry Interval has
/// elapsed (ADR 0009). Second-grained expiry does not need a finer cadence.
const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// How many sweep ticks between reconciling persisted expiry deadlines from the durable
/// store (ADR 0009 §3). This inherits deadlines for sessions a takeover handed this node
/// without seeing their disconnect; takeover is rare and the scan is O(owned sessions), so
/// it runs at a coarse cadence rather than every second.
const EXPIRY_RECONCILE_EVERY: u32 = 30;

/// How many outbound packet ids are durably reserved per block (ADR 0007 T9). One durable
/// write covers this many `QoS` > 0 sends to a session, so the per-message path stays
/// write-free; a takeover wastes at most this many ids (negligible against the 65535 space,
/// and the counter simply wraps).
const PKID_BLOCK: u16 = 1024;

/// MQTT 5.0 Session Expiry Interval meaning "never expire" (0xFFFFFFFF). v3.1.1
/// `clean_session=0` maps to this.
const SESSION_EXPIRY_NEVER: u32 = u32::MAX;

/// How long a persistent attach waits for the durable store to give an *authoritative*
/// session answer before rejecting the CONNECT with Server-unavailable (ADR 0017).
/// Comfortably above the observed lease-handoff (~1s) after a takeover, below a typical
/// client connect timeout. The wait runs off the hub command loop, so it never freezes
/// the hub.
const ATTACH_RECOVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Initial / maximum backoff between durable-recovery retries during an attach (ADR
/// 0017). Short enough to resume promptly once the lease lands, capped so a long
/// outage does not busy-loop.
const ATTACH_RECOVERY_BACKOFF_START: Duration = Duration::from_millis(50);
const ATTACH_RECOVERY_BACKOFF_MAX: Duration = Duration::from_millis(250);

/// A shared subscription's identity: `(ShareName, filter)` (ADR 0015).
type SharedKey = (String, String);

/// A shared group keyed for selection, with its global candidate list (ADR 0015).
type SharedMatch = (SharedKey, Vec<SharedCandidate>);

/// One candidate recipient for a shared group's single cluster-wide delivery: a
/// local member (`node` = `None`) or a member on a peer (ADR 0015).
#[derive(Debug, Clone)]
struct SharedCandidate {
    node: Option<NodeId>,
    client: ClientId,
    qos: QoS,
    /// Whether this member is **online on the node that owns its connection** — locally
    /// from `self.online`, for a remote member from its home node's gossiped liveness
    /// (ADR 0015 T8). The selector prefers an online member so a publish is delivered now
    /// rather than queued on a member offline at its home.
    online: bool,
}

/// A peer's shared-group membership as gossiped to us — like [`SharedGroup`] but each
/// member carries its **liveness on that home node** (ADR 0015 T8), so the cross-node
/// selector can avoid choosing a member that is offline (and would only queue) there.
#[derive(Debug, Clone)]
pub struct RemoteSharedGroup {
    /// The share name.
    pub group: String,
    /// The underlying topic filter.
    pub filter: String,
    /// Members: `(client, granted QoS, online-on-home-node)`.
    pub members: Vec<(ClientId, QoS, bool)>,
}

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

/// A `QoS` > 0 message held back because the session's Receive Maximum quota is full
/// (ADR 0012). It has no packet id yet — one is assigned when it is finally sent.
#[derive(Debug)]
struct Backlog {
    message: Message,
    retain: bool,
    message_expiry: Option<u32>,
}

/// Per-session outbound `QoS` bookkeeping. Survives disconnects so persistent
/// sessions can resume their in-flight messages (redelivered with `DUP`).
#[derive(Debug)]
struct Inflight {
    /// The packet-id allocation cursor — the last id handed out. Seeded from the durable
    /// block reservation (ADR 0007 T9), so a fresh `Inflight` on a new owner resumes past
    /// the prior owner's reserved ids rather than restarting at 1.
    next_pkid: u16,
    /// Ids left in the current durable reservation before the next block must be reserved.
    block_remaining: u16,
    pending: BTreeMap<u16, PendingOut>,
    /// The client's MQTT 5.0 Receive Maximum: the most `QoS` > 0 publishes we may
    /// have unacked to it at once (ADR 0012).
    receive_maximum: u16,
    /// `QoS` > 0 messages waiting for quota; drained FIFO as PUBACK/PUBCOMP frees slots.
    backlog: VecDeque<Backlog>,
}

impl Default for Inflight {
    fn default() -> Self {
        Self {
            next_pkid: 0,
            block_remaining: 0,
            pending: BTreeMap::new(),
            receive_maximum: RECEIVE_MAXIMUM_DEFAULT,
            backlog: VecDeque::new(),
        }
    }
}

impl Inflight {
    /// Whether the `QoS` > 0 in-flight quota is exhausted (ADR 0012).
    fn quota_full(&self) -> bool {
        self.pending.len() >= self.receive_maximum as usize
    }

    /// Append to the flow-control backlog, evicting the oldest entry when the cap is
    /// reached (drop-oldest, ADR 0012). Returns `true` if a message was evicted.
    fn push_backlog(&mut self, entry: Backlog) -> bool {
        let evicted = self.backlog.len() >= MAX_BACKLOG;
        if evicted {
            self.backlog.pop_front();
        }
        self.backlog.push_back(entry);
        evicted
    }
}

/// The result the hub returns to a connection so it can send (or refuse) its CONNACK.
///
/// For a persistent session this is decided only once the durable store gives an
/// *authoritative* answer; a transient lease/quorum condition that never resolves
/// within the recovery deadline yields [`Self::Unavailable`] — never a false
/// `Present(false)` that would silently reset a recoverable session (ADR 0017).
#[derive(Debug)]
pub enum AttachOutcome {
    /// The session was resolved; the flag is MQTT `session_present`.
    Present(bool),
    /// The durable store stayed transiently unavailable (lease reassigning / quorum
    /// unreachable) past the recovery deadline. The connection must reject the CONNECT
    /// with Server-unavailable and let the client retry; the session is left intact.
    Unavailable,
    /// The persistent session is owned by a *different* authenticated identity, so this
    /// connection may not resume or take it over (ADR 0031). The connection must reject the
    /// CONNACK as Not-authorized; the existing session is left untouched.
    OwnerMismatch,
}

/// The outcome of the off-loop durable recovery for a persistent attach (ADR 0017).
#[derive(Debug)]
pub enum SessionRecovery {
    /// An authoritative answer: whether the session already existed, and its persisted
    /// subscriptions (fetched off-loop so on-loop registration does no durable read).
    Ready {
        /// MQTT `session_present`.
        present: bool,
        /// Persisted subscriptions to reconcile into routing.
        subscriptions: Vec<Subscription>,
    },
    /// A clean-start attach finished discarding the prior durable state (ADR 0017);
    /// register a fresh session (`session_present = false`, no replay).
    Cleaned,
    /// The store could not give an authoritative answer within the deadline.
    Unavailable,
    /// The persistent session is owned by a different authenticated identity; the claim was
    /// refused (ADR 0031). Carries the existing owner's subject for the audit record.
    Denied {
        /// The stable subject of the identity that owns the session.
        owner: String,
    },
}

/// The connection context carried across the off-loop session-recovery wait so the hub
/// can finish registration when [`HubCommand::SessionRecovered`] arrives (ADR 0017).
/// Only the hub constructs one (all fields private), so the `pub` variant cannot be
/// forged by other code.
#[derive(Debug)]
pub struct PendingAttach {
    /// The client identifier.
    client: ClientId,
    /// The authenticated principal's stable subject — the owner to bind/verify (ADR 0031).
    owner: String,
    /// Unique id for this physical connection (guards last-writer-wins on overlap).
    conn_id: u64,
    /// MQTT 5.0 Session Expiry Interval (seconds).
    session_expiry: u32,
    /// MQTT 5.0 Receive Maximum for this connection (ADR 0012).
    receive_maximum: u16,
    /// Will message to publish if the connection ends ungracefully.
    will: Option<Message>,
    /// Channel the hub uses to deliver packets to this client.
    outbound: Outbound,
    /// Reply channel the connection awaits before its CONNACK.
    reply: oneshot::Sender<AttachOutcome>,
}

/// A message from a connection task to the hub.
#[derive(Debug)]
pub enum HubCommand {
    /// A client finished CONNECT; register it and (for persistent sessions)
    /// restore subscriptions and replay queued messages.
    Attach {
        /// The client identifier.
        client: ClientId,
        /// The authenticated principal's stable subject (mTLS CN / username / token subject,
        /// or the shared `"anonymous"` principal). Binds the session to its owner (ADR 0031).
        owner: String,
        /// Unique id for this physical connection.
        conn_id: u64,
        /// MQTT 5.0 Clean Start: discard any existing session before attaching
        /// (v3.1.1 `clean_session=1` maps to `true`).
        clean_start: bool,
        /// MQTT 5.0 Session Expiry Interval (seconds) — how long to keep the session
        /// after disconnect; `0` discards at disconnect, `u32::MAX` never expires.
        session_expiry: u32,
        /// MQTT 5.0 Receive Maximum: the most unacked `QoS` > 0 publishes the server
        /// may have outstanding to this client at once (ADR 0012).
        receive_maximum: u16,
        /// Will message to publish if the connection ends ungracefully.
        will: Option<Message>,
        /// Channel the hub uses to deliver packets to this client.
        outbound: Outbound,
        /// Reply with the [`AttachOutcome`] so the connection can CONNACK (or reject).
        reply: oneshot::Sender<AttachOutcome>,
    },
    /// Internal: the off-loop durable recovery for a persistent [`Attach`](Self::Attach)
    /// finished; finish registration on the hub loop (ADR 0017). Not sent by
    /// connections — the hub posts it to itself.
    SessionRecovered {
        /// The connection context carried across the wait.
        pending: PendingAttach,
        /// The authoritative recovery result (or `Unavailable`).
        recovery: SessionRecovery,
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
        /// MQTT 5.0 Message Expiry Interval in seconds, if the publisher set one.
        /// A queued copy past its deadline is dropped on replay (ADR 0009 §3).
        message_expiry: Option<u32>,
        /// The publisher's forwardable MQTT 5 application properties (ADR 0030).
        app: AppProperties,
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
    /// A peer's shared-subscription membership snapshot (ADR 0015 §2), used to select
    /// one member per group across the cluster.
    RemoteSharedInterest {
        /// The announcing node.
        node: NodeId,
        /// That node's shared groups with members.
        groups: Vec<RemoteSharedGroup>,
    },
    /// A chunk of a peer's retained-message snapshot, back-filling a node that joined
    /// after a retained publish (ADR 0014 §3, chunked per 0014-T8). Applied gap-fill
    /// (topics we do not already retain), never overwriting our own — so chunks are
    /// independent and idempotent.
    RemoteRetainedSnapshot {
        /// The peer the snapshot came from (divergence attribution, ADR 0037 P1).
        node: NodeId,
        /// Each retained message as `(topic, payload, QoS)`.
        messages: Vec<(String, Bytes, QoS)>,
    },
    /// A peer's retained digest, sent on link-up instead of the full snapshot
    /// (0014-T6). If both the topic-set hash and the value hash match our own there is
    /// nothing to back-fill *and* nothing diverges; otherwise we pull with
    /// [`PeerMessage::RetainedRequest`] — to gap-fill missing topics and to detect
    /// divergent values (ADR 0037 P1).
    RemoteRetainedDigest {
        /// The peer that sent its digest.
        node: NodeId,
        /// Number of retained topics the peer holds.
        count: u64,
        /// Order-independent hash of the peer's retained topic set.
        hash: u64,
        /// Order-independent hash of the peer's retained `(topic, payload, qos)` values.
        value_hash: u64,
    },
    /// A peer asked for our retained set (its digest comparison found a difference);
    /// answer with chunked [`PeerMessage::RetainedSnapshot`]s (0014-T6/T8).
    RemoteRetainedRequest {
        /// The peer to send the snapshot to.
        node: NodeId,
    },
    /// A targeted shared-subscription delivery from a peer (ADR 0015 §1): deliver to
    /// exactly `client` (a local member), no further selection or re-forward.
    RemoteSharedDeliver {
        /// The chosen local group member.
        client: ClientId,
        /// Destination topic.
        topic: String,
        /// Application payload.
        payload: Bytes,
        /// Already-downgraded delivery `QoS`.
        qos: QoS,
        /// The publisher's Message Expiry Interval (seconds), carried across the link so
        /// the queued copy keeps its deadline (ADR 0015 T7). `None` = no expiry.
        message_expiry: Option<u32>,
        /// The publisher's forwardable MQTT 5 application properties (ADR 0030).
        app: AppProperties,
    },
    /// A publish forwarded from a peer, for **local** delivery only (never re-forwarded).
    RemotePublish {
        /// Destination topic.
        topic: String,
        /// Application payload.
        payload: Bytes,
        /// The original publish `QoS` (local downgrade still applies).
        qos: QoS,
        /// Whether to store this as the topic's retained message on this node, so a
        /// later subscriber here sees it (cross-node retained replication, ADR 0014).
        retain: bool,
        /// The publisher's Message Expiry Interval (seconds), carried across the link so
        /// the queued copy keeps its deadline (ADR 0014 T9). `None` = no expiry.
        message_expiry: Option<u32>,
        /// The publisher's forwardable MQTT 5 application properties (ADR 0030).
        app: AppProperties,
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
    /// Disconnected sessions with a finite expiry, and the **absolute Unix-epoch second**
    /// they expire at (ADR 0009 §3). The sweep discards those past due; a reconnect cancels
    /// the entry. An absolute wall-clock deadline (not a monotonic `Instant`) is what lets a
    /// new owner inherit the right deadline after a takeover — the same value is persisted in
    /// the durable session metadata.
    expiring: HashMap<ClientId, u64>,
    /// Sweep-tick counter that paces the durable expiry reconcile (ADR 0009 §3).
    expiry_reconcile_tick: u32,
    /// Per-client subscription filters with their granted `QoS`.
    subs_by_client: HashMap<ClientId, HashMap<String, QoS>>,
    /// Routing index covering online clients and offline persistent sessions.
    table: SubscriptionTable,
    /// Local shared-subscription groups (`$share/<group>/<filter>`) — this node's
    /// members (ADR 0010).
    shared: SharedSubscriptionTable,
    /// Each peer's last-announced shared-subscription membership, so this node can
    /// select one member per group across the whole cluster (ADR 0015 §2).
    remote_shared: HashMap<NodeId, Vec<RemoteSharedGroup>>,
    /// Per-group round-robin cursor for cluster-wide shared selection (ADR 0015).
    shared_cursor: HashMap<SharedKey, usize>,
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
    /// A clone of the hub's own command sender, so an off-loop session-recovery task
    /// can post [`HubCommand::SessionRecovered`] back to the loop (ADR 0017).
    self_tx: mpsc::UnboundedSender<HubCommand>,
    /// Persistent connections whose durable session is being recovered off-loop, mapped
    /// to the latest `conn_id` (ADR 0017). A `SessionRecovered` whose `conn_id` no longer
    /// matches was superseded by a newer connect and is dropped (last-writer-wins).
    connecting: HashMap<ClientId, u64>,
    /// Prometheus metrics (ADR 0020), when enabled. Updated on the publish/deliver paths.
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    /// Wall-clock source for absolute message-expiry deadlines (ADR 0009 §3).
    /// Injectable so expiry can be tested without real time passing.
    clock: Arc<dyn crate::clock::Clock>,
}

/// The bounded `{reason}` label for a durable-append failure (ADR 0020-T6).
fn durable_failure_reason(e: &StorageError) -> &'static str {
    match e {
        StorageError::NoQuorum => "no-quorum",
        StorageError::NotOwner => "not-owner",
        StorageError::Unavailable(_) => "unavailable",
        StorageError::Backend(_) => "backend",
        StorageError::NotFound => "not-found",
    }
}

/// Map a `QoS` to its wire numeric (0/1/2) for the `{qos}` metric label.
fn qos_num(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

/// Per-chunk byte budget for a retained-snapshot frame (0014-T8): well under the peer
/// frame limit (16 MiB, `mqtt_cluster::peer`), with headroom for bincode framing — a
/// frame at the limit would be rejected by the receiver and tear down the link.
const RETAINED_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// The order-independent digest of a retained set (0014-T6 + ADR 0037 P1): the topic
/// count, the XOR of each topic's stable 64-bit hash, and the XOR of each
/// `(topic, payload, qos)` **value** hash. Independent of iteration order and cheap to
/// compare (a collision merely skips a best-effort back-fill / detection). Equal topic
/// hashes with **differing value hashes** mean divergence: same topics, different values.
fn retained_digest<'a>(entries: impl Iterator<Item = (&'a str, &'a [u8], u8)>) -> (u64, u64, u64) {
    let mut count = 0u64;
    let mut hash = 0u64;
    let mut value_hash = 0u64;
    for (topic, payload, qos) in entries {
        count += 1;
        hash ^= mqtt_cluster::hrw::stable_id(topic.as_bytes());
        value_hash ^= retained_value_id(topic, payload, qos);
    }
    (count, hash, value_hash)
}

/// A stable 64-bit hash of one retained `(topic, payload, qos)` value (ADR 0037 P1).
/// The topic is length-prefixed so `("a", "bc")` and `("ab", "c")` cannot collide.
fn retained_value_id(topic: &str, payload: &[u8], qos: u8) -> u64 {
    let mut bytes = Vec::with_capacity(8 + topic.len() + payload.len() + 1);
    bytes.extend_from_slice(&(topic.len() as u64).to_be_bytes());
    bytes.extend_from_slice(topic.as_bytes());
    bytes.extend_from_slice(payload);
    bytes.push(qos);
    mqtt_cluster::hrw::stable_id(&bytes)
}

/// Split retained entries into chunks whose summed (topic + payload) size stays under
/// [`RETAINED_CHUNK_BYTES`] (0014-T8). A single entry larger than the whole budget is
/// skipped with a warning — it could never fit a frame, and sending it would sever the
/// link instead of just missing one back-fill.
fn chunk_retained(
    entries: impl Iterator<Item = (String, Vec<u8>, u8)>,
) -> Vec<Vec<(String, Vec<u8>, u8)>> {
    // Fixed per-entry overhead estimate for bincode length prefixes and the QoS byte.
    const ENTRY_OVERHEAD: usize = 32;
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for (topic, payload, qos) in entries {
        let size = topic.len() + payload.len() + ENTRY_OVERHEAD;
        if size > RETAINED_CHUNK_BYTES {
            warn!(
                topic = %topic,
                bytes = size,
                "retained message exceeds the snapshot chunk budget; skipping back-fill for it"
            );
            continue;
        }
        if current_bytes + size > RETAINED_CHUNK_BYTES && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += size;
        current.push((topic, payload, qos));
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
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
                self_tx: tx.clone(),
                connecting: HashMap::new(),
                node_id,
                online: HashMap::new(),
                session_expiry: HashMap::new(),
                expiring: HashMap::new(),
                expiry_reconcile_tick: 0,
                subs_by_client: HashMap::new(),
                table: SubscriptionTable::new(),
                shared: SharedSubscriptionTable::new(),
                remote_shared: HashMap::new(),
                shared_cursor: HashMap::new(),
                inflight: HashMap::new(),
                store,
                durable_plane: None,
                retained: Box::new(MemoryRetainedStore::new()),
                peers: HashMap::new(),
                remote_interest: HashMap::new(),
                placement,
                metrics: None,
                clock: crate::clock::system_clock(),
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

    /// Replace the retained-message store before [`run`](Self::run) — used to swap the
    /// in-memory default for the on-disk store when persistence is enabled (ADR 0018
    /// phase 4).
    pub fn attach_retained_store(&mut self, retained: Box<dyn RetainedStore>) {
        self.retained = retained;
    }

    /// Attach the Prometheus metrics registry before [`run`](Self::run) so the hub records
    /// publish/deliver/drop counts (ADR 0020).
    pub fn attach_metrics(&mut self, metrics: Arc<mqtt_observability::metrics::Metrics>) {
        self.metrics = Some(metrics);
    }

    /// Replace the wall-clock source before [`run`](Self::run). Production uses the
    /// default system clock; tests inject a controllable clock so absolute
    /// message-expiry deadlines (ADR 0009 §3) can be exercised without real time.
    pub fn attach_clock(&mut self, clock: Arc<dyn crate::clock::Clock>) {
        self.clock = clock;
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
                _ = sweep.tick() => {
                    self.sweep_expired_sessions().await;
                    self.refresh_gauges().await;
                }
            }
        }
    }

    /// Dispatch one command to its handler.
    async fn dispatch(&mut self, cmd: HubCommand) {
        match cmd {
            HubCommand::Attach {
                client,
                owner,
                conn_id,
                clean_start,
                session_expiry,
                receive_maximum,
                will,
                outbound,
                reply,
            } => {
                self.attach(
                    PendingAttach {
                        client,
                        owner,
                        conn_id,
                        session_expiry,
                        receive_maximum,
                        will,
                        outbound,
                        reply,
                    },
                    clean_start,
                );
            }
            HubCommand::SessionRecovered { pending, recovery } => {
                self.session_recovered(pending, recovery).await;
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
                message_expiry,
                app,
            } => {
                if let Some(m) = &self.metrics {
                    m.publish_received(qos_num(qos));
                }
                // Time the synchronous on-loop fan-out (local deliver + offline enqueue
                // + peer forward) as the hub's per-publish delivery latency (ADR 0020-T4).
                let started = Instant::now();
                self.publish(&topic, &payload, qos, retain, message_expiry, &app)
                    .await;
                if let Some(m) = &self.metrics {
                    m.observe_deliver_latency(started.elapsed().as_secs_f64());
                }
            }
            HubCommand::PubAck { client, pkid } => self.pub_ack(&client, pkid).await,
            HubCommand::PubRec { client, pkid } => self.pub_rec(&client, pkid),
            HubCommand::PubComp { client, pkid } => self.pub_comp(&client, pkid).await,
            HubCommand::Detach {
                client,
                conn_id,
                graceful,
            } => {
                self.detach(&client, conn_id, graceful).await;
            }
            // Peer- and cluster-facing commands.
            other => self.dispatch_cluster(other).await,
        }
    }

    /// Dispatch a peer-/cluster-facing command (forwarded publishes, peer link
    /// (de)registration, gossiped interest, durable frames). Split from
    /// [`dispatch`](Self::dispatch) to keep each handler focused.
    async fn dispatch_cluster(&mut self, cmd: HubCommand) {
        match cmd {
            HubCommand::RemotePublish {
                topic,
                payload,
                qos,
                retain,
                message_expiry,
                app,
            } => {
                // Forwarded from a peer: apply locally (deliver + store retained) but
                // never re-forward. A retained copy updates this node's store so a
                // later local subscriber sees it (ADR 0014). The publisher's message
                // expiry is carried over the link (ADR 0014 T9), so a queued cross-node
                // copy keeps the same deadline. User Properties ride along (ADR 0030).
                self.deliver(&topic, &payload, qos, retain, message_expiry, &app)
                    .await;
            }
            HubCommand::PeerConnected { node, conn_id, tx } => {
                self.peer_connected(node.clone(), conn_id, tx);
                // Offer the new peer our retained topic-set digest (ADR 0014 §3,
                // 0014-T6): it pulls the (chunked) snapshot only if the sets differ,
                // so a steady-state link-up or flap costs one small frame, not the
                // whole retained set.
                self.send_retained_digest(&node).await;
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
            HubCommand::RemoteSharedInterest { node, groups } => {
                debug!(node = %node.0, groups = groups.len(), "remote shared interest updated");
                self.remote_shared.insert(node, groups);
            }
            HubCommand::RemoteRetainedSnapshot { node, messages } => {
                self.apply_retained_snapshot(&node, messages).await;
            }
            HubCommand::RemoteRetainedDigest {
                node,
                count,
                hash,
                value_hash,
            } => {
                self.handle_retained_digest(&node, count, hash, value_hash)
                    .await;
            }
            HubCommand::RemoteRetainedRequest { node } => {
                self.send_retained_snapshot(&node).await;
            }
            HubCommand::RemoteSharedDeliver {
                client,
                topic,
                payload,
                qos,
                message_expiry,
                app,
            } => {
                // Targeted by a peer's shared selection: deliver to this one client
                // (ADR 0015), never re-selected or re-forwarded. The publisher's message
                // expiry is carried over the link (ADR 0015 T7) so a queued copy keeps its
                // deadline. Application properties ride along (ADR 0030).
                self.deliver_to_client(&client, &topic, &payload, qos, message_expiry, &app)
                    .await;
            }
            // Client/session commands are handled in `dispatch`; they never route here.
            _ => {}
        }
    }

    /// Publish a locally-originated message: apply it on this node, then forward to
    /// peers (interested peers for live delivery; **all** peers for retained, so each
    /// node stores it for its future subscribers — ADR 0014).
    async fn publish(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) {
        self.deliver(topic, payload, qos, retain, message_expiry, app)
            .await;
        // Shared subscriptions are selected once cluster-wide by the originating
        // node (ADR 0015), so this runs only for locally-originated publishes.
        self.deliver_shared(topic, payload, qos, message_expiry, app)
            .await;
        self.forward_to_peers(topic, payload, qos, retain, message_expiry, app);
    }

    /// Publish a client's Will message (on takeover or an ungraceful end). Carries the
    /// will's own application properties (ADR 0030); a will never sets a message-expiry.
    async fn publish_will(&mut self, w: &Message) {
        self.publish(&w.topic, &w.payload, w.qos, w.retain, None, &w.app)
            .await;
    }

    /// Apply a message on this node: store/clear retained state and deliver to local
    /// ordinary subscribers. Does **not** forward or run shared selection — used both
    /// for local publishes (via
    /// [`publish`](Self::publish)) and for publishes received from a peer, which must
    /// never be re-forwarded.
    async fn deliver(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) {
        if retain {
            // A zero-length retained payload clears the retained message
            // [MQTT-3.3.1-10]; `RetainedStore::set` implements both cases.
            let message = Message {
                topic: topic.to_string(),
                payload: payload.clone(),
                qos,
                retain: true,
                app: app.clone(),
            };
            if let Err(e) = self.retained.set(&message).await {
                warn!(topic = %topic, error = %e, "failed to update retained message");
            }
        }
        // Live deliveries carry retain=0 [MQTT-3.3.1-9].
        self.deliver_local(topic, payload, qos, message_expiry, app)
            .await;
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

    /// Begin attaching a connection. A clean-start session registers immediately; a
    /// persistent session first recovers its durable state **off the hub command loop**
    /// (ADR 0017) so the possibly-seconds-long lease/quorum wait cannot freeze the
    /// single-threaded hub. Recovery completes back on the loop via `SessionRecovered`.
    fn attach(&mut self, pending: PendingAttach, clean_start: bool) {
        // A reconnect cancels any pending expiry for this session (ADR 0009).
        self.expiring.remove(&pending.client);

        if clean_start {
            // Clean Start: wipe the in-memory session immediately (fast), then discard
            // the *durable* prior state **off the loop** (ADR 0017). The durable
            // `remove` can trigger a first-touch group recovery on the owner of a cold
            // group, which inline would freeze the hub and stall this CONNACK; the
            // CONNACK is still gated on the discard (via `SessionRecovered`) so the
            // clean-session wipe is observed before the client proceeds.
            self.discard_session_local(&pending.client);
            self.connecting
                .insert(pending.client.clone(), pending.conn_id);
            tokio::spawn(discard_session(
                self.store.clone(),
                self.self_tx.clone(),
                pending,
            ));
            return;
        }

        // Persistent: the durable store must answer authoritatively whether this session
        // exists. During a lease handoff that answer is momentarily `Unavailable`; we
        // must wait for it (never downgrade to "no session") and do so off-loop so the
        // wait does not stall every other client on this node.
        self.note_session_ownership(&pending.client);
        self.connecting
            .insert(pending.client.clone(), pending.conn_id);
        tokio::spawn(recover_session(
            self.store.clone(),
            self.self_tx.clone(),
            pending,
        ));
    }

    /// Handle the off-loop recovery result for a persistent attach (ADR 0017). Drops a
    /// superseded recovery (a newer connect won the id during the wait), rejects on
    /// `Unavailable` (never a false "no session"), otherwise finishes registration.
    async fn session_recovered(&mut self, pending: PendingAttach, recovery: SessionRecovery) {
        // Last-writer-wins: if a newer connect for this id arrived during the wait, this
        // recovery is stale — drop it (its reply is dropped, which closes that
        // connection). The newer connect's own recovery will register it.
        if self.connecting.get(&pending.client) != Some(&pending.conn_id) {
            debug!(client = %pending.client.0, "dropping superseded session recovery");
            return;
        }
        self.connecting.remove(&pending.client);

        match recovery {
            SessionRecovery::Ready {
                present,
                subscriptions,
            } => {
                self.finish_attach(pending, false, present, subscriptions)
                    .await;
            }
            SessionRecovery::Cleaned => {
                self.finish_attach(pending, true, false, Vec::new()).await;
            }
            SessionRecovery::Unavailable => {
                warn!(
                    client = %pending.client.0,
                    "durable session recovery stayed unavailable past deadline; rejecting CONNECT (ADR 0017)"
                );
                let _ = pending.reply.send(AttachOutcome::Unavailable);
            }
            SessionRecovery::Denied { owner } => {
                warn!(
                    client = %pending.client.0,
                    claimant = %pending.owner,
                    owner = %owner,
                    "session-identity mismatch: a different principal may not resume/take over \
                     this persistent session; rejecting CONNECT (ADR 0031)"
                );
                let _ = pending.reply.send(AttachOutcome::OwnerMismatch);
            }
        }
    }

    /// Finish a recovered (or clean-start) attach on the hub loop: reconcile
    /// subscriptions, register the connection (honoring takeover), reply so the
    /// connection can CONNACK, then resume in-flight `QoS` and replay queued messages.
    #[allow(clippy::too_many_lines)]
    async fn finish_attach(
        &mut self,
        pending: PendingAttach,
        clean_start: bool,
        session_present: bool,
        subscriptions: Vec<Subscription>,
    ) {
        let PendingAttach {
            client,
            // The owner was bound/verified during recovery (claim_session); not needed here.
            owner: _,
            conn_id,
            session_expiry,
            receive_maximum,
            will,
            outbound,
            reply,
        } = pending;

        // Reconcile the routing table with persisted subscriptions (idempotent; empty
        // for a clean start).
        for s in subscriptions {
            if let Some((group, filter)) = parse_shared(&s.filter) {
                self.shared
                    .subscribe(client.clone(), group, filter, s.max_qos);
            } else {
                self.table.subscribe(client.clone(), s.filter.clone());
            }
            self.subs_by_client
                .entry(client.clone())
                .or_default()
                .insert(s.filter, s.max_qos);
        }

        // Record this session's retention: it survives disconnect iff the expiry
        // interval is non-zero. A zero interval (or v3.1.1 clean_session=1) means the
        // session is dropped at disconnect.
        if session_expiry == 0 {
            self.session_expiry.remove(&client);
        } else {
            self.session_expiry.insert(client.clone(), session_expiry);
            // Connected again → the session must not expire while online. Clear any persisted
            // deadline (ADR 0009 §3); the next disconnect re-arms it. This also prevents a
            // restart-while-connected from inheriting a stale deadline and wrongly expiring an
            // active session. Only for a persistent session — a clean session has no durable
            // metadata, and writing a cleared deadline would wrongly materialize one.
            let _ = self.store.set_session_expiry(&client, None).await;
        }

        // Adopt this connection's outbound Receive Maximum quota (ADR 0012). A
        // reconnect may carry a different value than the prior one.
        self.inflight
            .entry(client.clone())
            .or_default()
            .receive_maximum = receive_maximum;

        // Registering replaces any previous connection for this id; dropping the
        // old `Outbound` closes the old writer loop (takeover). The server-side
        // disconnect is not a client DISCONNECT, so the old will is published.
        if let Some(old) = self.online.remove(&client) {
            warn!(client = %client.0, "session takeover: replacing existing connection");
            if let Some(w) = old.will {
                self.publish_will(&w).await;
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
        let _ = reply.send(AttachOutcome::Present(session_present));

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
                        None,
                        &p.message.app,
                    ),
                    OutState::AwaitingPubComp => Packet::PubRel((*pkid).into()),
                };
                let _ = outbound.send(packet);
            }
        }

        // Replay queued messages (they land in the channel after CONNACK). The lease is
        // warm (recovery just succeeded), so these reads are fast and local. A message
        // whose MQTT 5.0 expiry deadline has passed is dropped, not delivered, and the
        // remaining interval is forwarded on the rest (ADR 0009 §3).
        if !clean_start {
            if let Ok(pending) = self.store.pending(&client, 0, REPLAY_LIMIT).await {
                let now = self.clock.now_epoch_secs();
                let mut last = 0;
                for qm in pending {
                    last = qm.offset;
                    match qm.expiry_at {
                        Some(deadline) if deadline <= now => {
                            debug!(client = %client.0, offset = qm.offset, "dropping expired queued message");
                        }
                        Some(deadline) => {
                            let remaining = u32::try_from(deadline - now).unwrap_or(u32::MAX);
                            self.send_to_client(
                                &client,
                                &outbound,
                                &qm.message,
                                false,
                                Some(remaining),
                            )
                            .await;
                        }
                        None => {
                            self.send_to_client(&client, &outbound, &qm.message, false, None)
                                .await;
                        }
                    }
                }
                if last > 0 {
                    debug!(client = %client.0, up_to = last, "replayed queued messages");
                    let _ = self.store.ack(&client, last).await;
                }
            }
        }
    }

    async fn subscribe(&mut self, client: &ClientId, filters: Vec<(String, QoS)>) {
        // Retained messages are replayed only for ordinary subscriptions; a new
        // shared subscription does not receive them (ADR 0010 §3, [MQTT-3.8.4]).
        let mut replay: Vec<Message> = Vec::new();
        for (f, q) in &filters {
            // Keep the full filter string (including any `$share/` prefix) so it is
            // persisted; `$share/...` never matches a concrete topic in `granted_qos`.
            self.subs_by_client
                .entry(client.clone())
                .or_default()
                .insert(f.clone(), *q);
            if let Some((group, filter)) = parse_shared(f) {
                debug!(client = %client.0, group, filter, qos = *q as u8, "shared subscribe");
                self.shared.subscribe(client.clone(), group, filter, *q);
                continue;
            }
            debug!(client = %client.0, filter = %f, qos = *q as u8, "subscribe");
            self.table.subscribe(client.clone(), f.clone());
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
        self.persist_subscriptions(client).await;
        self.gossip_interest();

        if let Some(tx) = self.online.get(client).map(|s| s.tx.clone()) {
            for m in replay {
                self.send_to_client(client, &tx, &m, true, None).await;
            }
        }
    }

    async fn unsubscribe(&mut self, client: &ClientId, filters: &[String]) {
        for f in filters {
            if let Some(map) = self.subs_by_client.get_mut(client) {
                map.remove(f);
            }
            if let Some((group, filter)) = parse_shared(f) {
                self.shared.unsubscribe(client, group, filter);
            } else {
                self.table.unsubscribe(client, f);
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

    /// Deliver a message to this node's **ordinary** local subscribers at
    /// `min(qos, granted)` each. Shared subscriptions are routed separately by
    /// [`deliver_shared`](Self::deliver_shared) (ADR 0015).
    async fn deliver_local(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) {
        let targets: Vec<(ClientId, QoS)> = self
            .table
            .matching_clients(topic)
            .into_iter()
            .map(|c| {
                let granted = self.granted_qos(&c, topic);
                (c, granted)
            })
            .collect();
        debug!(topic = %topic, ordinary = targets.len(), "local delivery");
        for (c, granted) in targets {
            self.deliver_to_client(
                &c,
                topic,
                payload,
                min_qos(qos, granted),
                message_expiry,
                app,
            )
            .await;
        }
    }

    /// Deliver one message to a single named recipient: live if online (tracking
    /// `QoS` > 0 in flight), else queued if the session is persistent, else dropped.
    /// The unit of both ordinary and shared (ADR 0015) delivery; `qos` is the
    /// already-downgraded delivery `QoS`.
    async fn deliver_to_client(
        &mut self,
        client: &ClientId,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) {
        let message = Message {
            topic: topic.to_string(),
            payload: payload.clone(),
            qos,
            retain: false,
            app: app.clone(),
        };
        if let Some(tx) = self.online.get(client).map(|s| s.tx.clone()) {
            self.send_to_client(client, &tx, &message, false, message_expiry)
                .await;
            if let Some(m) = &self.metrics {
                m.publish_delivered(qos_num(qos));
            }
        } else if self.is_persistent(client) {
            // Offline but persistent: queue for replay on reconnect. The absolute
            // deadline (ADR 0009 §3) is receipt time plus the interval. The queue is
            // bounded (ADR 0001 §6); log when the cap drops messages.
            let expiry_at =
                message_expiry.map(|secs| self.clock.now_epoch_secs() + u64::from(secs));
            // Durable (quorum) append: time it and classify any failure (ADR 0020-T6).
            // The latency histogram is only meaningful when the store is the replicated
            // one, so gate it on durable mode; a failure reason is recorded either way.
            let durable = self.durable_plane.is_some();
            let started = Instant::now();
            let result = self
                .store
                .enqueue_with_expiry(client, &message, expiry_at)
                .await;
            if durable {
                if let Some(m) = &self.metrics {
                    m.observe_durable_append_latency(started.elapsed().as_secs_f64());
                }
            }
            match result {
                Ok(Enqueued::Stored { evicted, .. }) if evicted > 0 => {
                    warn!(client = %client.0, evicted, topic = %topic,
                          "offline queue full: evicted oldest message(s)");
                    if let Some(m) = &self.metrics {
                        m.publish_dropped("queue-overflow");
                    }
                }
                Ok(Enqueued::Rejected) => {
                    warn!(client = %client.0, topic = %topic,
                          "offline queue full: dropped message (reject-newest)");
                    if let Some(m) = &self.metrics {
                        m.publish_dropped("queue-overflow");
                    }
                }
                Ok(Enqueued::Stored { .. }) => {}
                Err(e) => {
                    if let Some(m) = &self.metrics {
                        m.durable_append_failed(durable_failure_reason(&e));
                    }
                    warn!(client = %client.0, error = %e, "failed to enqueue offline message");
                }
            }
        }
    }

    /// Route a message to the shared subscriptions matching `topic`: for each group,
    /// select exactly one member across the **whole cluster** (round-robin) and
    /// deliver to it — locally, or via a targeted `SharedDeliver` to the member's
    /// node (ADR 0015). The originating node is the sole selector, so there is no
    /// double delivery.
    async fn deliver_shared(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) {
        for (key, candidates) in self.shared_candidates(topic) {
            let Some(chosen) = self.select_shared(&key, &candidates) else {
                debug!(topic = %topic, "shared group has no reachable member");
                continue;
            };
            let delivered_qos = min_qos(qos, chosen.qos);
            match chosen.node {
                None => {
                    self.deliver_to_client(
                        &chosen.client,
                        topic,
                        payload,
                        delivered_qos,
                        message_expiry,
                        app,
                    )
                    .await;
                }
                Some(node) => {
                    self.send_shared_to_peer(
                        &node,
                        &chosen.client,
                        topic,
                        payload,
                        delivered_qos,
                        message_expiry,
                        app,
                    );
                }
            }
        }
    }

    /// The shared groups matching `topic`, each with its global candidate list:
    /// local members (`node` = None) first, then each peer's members in node-id
    /// order, so the round-robin cursor is stable (ADR 0015 §2).
    fn shared_candidates(&self, topic: &str) -> Vec<SharedMatch> {
        let mut by_key: BTreeMap<SharedKey, Vec<SharedCandidate>> = BTreeMap::new();
        // Borrow each matching group's members (ADR 0010 T8): clone only what we keep — the
        // key and each candidate — not the whole member list per publish.
        self.shared
            .for_each_matching(topic, |group, filter, members| {
                let entry = by_key
                    .entry((group.to_string(), filter.to_string()))
                    .or_default();
                for (client, qos) in members {
                    let online = self.online.contains_key(client);
                    entry.push(SharedCandidate {
                        node: None,
                        client: client.clone(),
                        qos: *qos,
                        online,
                    });
                }
            });
        for (node, groups) in self.remote_shared.iter().collect::<BTreeMap<_, _>>() {
            for g in groups {
                if !topic_matches(&g.filter, topic) {
                    continue;
                }
                let entry = by_key
                    .entry((g.group.clone(), g.filter.clone()))
                    .or_default();
                for (client, qos, online) in &g.members {
                    entry.push(SharedCandidate {
                        node: Some((*node).clone()),
                        client: client.clone(),
                        qos: *qos,
                        online: *online,
                    });
                }
            }
        }
        by_key.into_iter().collect()
    }

    /// Round-robin one member for a shared group, advancing the per-group cursor.
    /// Prefers a member that can receive now — a **local online** or **any remote**
    /// member — and falls back to a **local persistent** (queued) member (ADR 0015 §4).
    fn select_shared(
        &mut self,
        key: &SharedKey,
        candidates: &[SharedCandidate],
    ) -> Option<SharedCandidate> {
        let n = candidates.len();
        if n == 0 {
            return None;
        }
        let start = self.shared_cursor.get(key).copied().unwrap_or(0) % n;
        self.shared_cursor.insert(key.clone(), (start + 1) % n);
        let rotated = || candidates.iter().cycle().skip(start).take(n);
        // Immediately deliverable: any member online on its home node — local (our
        // `online`) or remote (its home node's gossiped liveness, ADR 0015 T8). Targeting a
        // member offline at home would only queue there while a live member could deliver now.
        let immediate = rotated().find(|c| c.online);
        immediate
            // No one online: a local persistent member queues for replay (ADR 0015 §4)...
            .or_else(|| rotated().find(|c| c.node.is_none() && self.is_persistent(&c.client)))
            // ...else a remote member (it queues at its home) so the message is not dropped.
            .or_else(|| rotated().find(|c| c.node.is_some()))
            .cloned()
    }

    /// Send one message to an online client at its (already downgraded) `QoS`,
    /// registering `QoS` > 0 deliveries in the in-flight table. `message_expiry` is
    /// the MQTT 5.0 Message Expiry Interval to forward (the remaining seconds), if any.
    async fn send_to_client(
        &mut self,
        client: &ClientId,
        tx: &Outbound,
        message: &Message,
        retain: bool,
        message_expiry: Option<u32>,
    ) {
        if message.qos == QoS::AtMostOnce {
            // Ignore send errors: a closed channel means the client is gone and a
            // Detach is already in flight.
            let _ = tx.send(publish_packet(
                &message.topic,
                message.payload.clone(),
                QoS::AtMostOnce,
                None,
                false,
                retain,
                message_expiry,
                &message.app,
            ));
            return;
        }

        // QoS > 0: respect the client's Receive Maximum (ADR 0012). If the quota is
        // full, hold the message until a PUBACK/PUBCOMP drains it; otherwise send now.
        let inf = self.inflight.entry(client.clone()).or_default();
        if inf.quota_full() {
            // The backlog is bounded (ADR 0012); drop-oldest on overflow so a stalled
            // consumer cannot force unbounded memory.
            let evicted = inf.push_backlog(Backlog {
                message: message.clone(),
                retain,
                message_expiry,
            });
            if evicted {
                warn!(client = %client.0, cap = MAX_BACKLOG,
                      "flow-control backlog full: evicted oldest message");
            }
        } else {
            self.send_qos_publish(client, tx, message, retain, message_expiry)
                .await;
        }
    }

    /// Put one `QoS` > 0 message on the wire: allocate a packet id, register it in the
    /// in-flight table, and send. The caller has already confirmed quota is available
    /// (ADR 0012).
    /// Allocate an outbound packet id for `client` (1..=65535, never 0, skipping ids still
    /// in flight). Ids come from a durably-reserved block (ADR 0007 T9): when the block is
    /// spent, the next block is reserved with one store write that advances the persisted
    /// high-water, so a takeover resumes past it. A reservation failure (or a non-durable /
    /// clean session, which returns base 0) degrades to a free-running in-memory counter
    /// rather than blocking delivery.
    async fn alloc_pkid(&mut self, client: &ClientId) -> u16 {
        loop {
            let spent = self
                .inflight
                .get(client)
                .is_none_or(|i| i.block_remaining == 0);
            if spent {
                match self.store.reserve_packet_ids(client, PKID_BLOCK).await {
                    // base = persisted high-water before the reservation; resume from it.
                    Ok(base) if base != 0 => {
                        let inf = self.inflight.entry(client.clone()).or_default();
                        inf.next_pkid = base;
                    }
                    // No durable session (clean / non-durable store) or a failed reserve:
                    // keep the in-memory cursor and just refill the local block.
                    Ok(_) => {}
                    Err(e) => {
                        debug!(client = %client.0, error = %e, "packet-id reservation failed; in-memory fallback");
                    }
                }
                self.inflight
                    .entry(client.clone())
                    .or_default()
                    .block_remaining = PKID_BLOCK;
            }
            let inf = self.inflight.entry(client.clone()).or_default();
            inf.next_pkid = inf.next_pkid.wrapping_add(1);
            if inf.next_pkid == 0 {
                inf.next_pkid = 1; // packet id 0 is invalid
            }
            inf.block_remaining = inf.block_remaining.saturating_sub(1);
            let id = inf.next_pkid;
            if !inf.pending.contains_key(&id) {
                return id;
            }
        }
    }

    async fn send_qos_publish(
        &mut self,
        client: &ClientId,
        tx: &Outbound,
        message: &Message,
        retain: bool,
        message_expiry: Option<u32>,
    ) {
        let pkid = self.alloc_pkid(client).await;
        let inf = self.inflight.entry(client.clone()).or_default();
        let state = if message.qos == QoS::AtLeastOnce {
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
            message.qos,
            Some(pkid),
            false,
            retain,
            message_expiry,
            &message.app,
        ));
    }

    /// Drain backlogged `QoS` > 0 messages onto the wire while the client is online and
    /// quota is available (ADR 0012). Called after a PUBACK/PUBCOMP frees a slot.
    async fn drain_backlog(&mut self, client: &ClientId) {
        let Some(tx) = self.online.get(client).map(|s| s.tx.clone()) else {
            return;
        };
        loop {
            let inf = self.inflight.entry(client.clone()).or_default();
            if inf.quota_full() {
                break;
            }
            let Some(entry) = inf.backlog.pop_front() else {
                break;
            };
            self.send_qos_publish(
                client,
                &tx,
                &entry.message,
                entry.retain,
                entry.message_expiry,
            )
            .await;
        }
    }

    /// PUBACK: completes a `QoS` 1 delivery, freeing a quota slot (ADR 0012).
    async fn pub_ack(&mut self, client: &ClientId, pkid: u16) {
        let completed = self.inflight.get_mut(client).is_some_and(|inf| {
            if inf
                .pending
                .get(&pkid)
                .is_some_and(|p| p.state == OutState::AwaitingPubAck)
            {
                inf.pending.remove(&pkid);
                true
            } else {
                false
            }
        });
        if completed {
            self.drain_backlog(client).await;
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

    /// PUBCOMP: completes a `QoS` 2 delivery, freeing a quota slot (ADR 0012).
    async fn pub_comp(&mut self, client: &ClientId, pkid: u16) {
        let completed = self.inflight.get_mut(client).is_some_and(|inf| {
            if inf
                .pending
                .get(&pkid)
                .is_some_and(|p| p.state == OutState::AwaitingPubComp)
            {
                inf.pending.remove(&pkid);
                true
            } else {
                false
            }
        });
        if completed {
            self.drain_backlog(client).await;
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
                self.publish_will(&w).await;
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
                self.flush_backlog_to_store(client).await;
                info!(client = %client.0, "client detached (session retained)");
            }
            Some(secs) => {
                self.flush_backlog_to_store(client).await;
                // Absolute wall-clock deadline, persisted durably so a new owner expires the
                // session at the right time after a takeover instead of restarting the clock
                // (ADR 0009 §3).
                let deadline = self.clock.now_epoch_secs() + u64::from(secs);
                let _ = self.store.set_session_expiry(client, Some(deadline)).await;
                self.expiring.insert(client.clone(), deadline);
                info!(client = %client.0, expires_in_s = secs, "client detached (session expiring)");
            }
        }
    }

    /// Spill a persistent session's never-sent backlog (`QoS` > 0 messages held for
    /// quota, ADR 0012) into the durable offline queue so they replay on reconnect
    /// rather than being lost when the connection ends. Already-sent in-flight
    /// entries keep their DUP-redelivery behaviour and are left untouched.
    async fn flush_backlog_to_store(&mut self, client: &ClientId) {
        let backlog: Vec<Backlog> = match self.inflight.get_mut(client) {
            Some(inf) if !inf.backlog.is_empty() => inf.backlog.drain(..).collect(),
            _ => return,
        };
        let now = self.clock.now_epoch_secs();
        for entry in backlog {
            let expiry_at = entry.message_expiry.map(|s| now + u64::from(s));
            if let Err(e) = self
                .store
                .enqueue_with_expiry(client, &entry.message, expiry_at)
                .await
            {
                warn!(client = %client.0, error = %e, "failed to spill backlog to store");
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
        self.discard_session_local(client);
        let _ = self.store.remove(client).await;
    }

    /// The in-memory half of discarding a session (routing, in-flight, expiry state).
    /// Fast and loop-safe; the durable `remove` is done separately (off-loop for a
    /// clean-start attach, ADR 0017).
    fn discard_session_local(&mut self, client: &ClientId) {
        self.drop_subscriptions(client);
        self.inflight.remove(client);
        self.session_expiry.remove(client);
        self.expiring.remove(client);
    }

    /// Discard every session whose MQTT 5.0 Session Expiry Interval has elapsed
    /// (ADR 0009). Runs on the hub's periodic sweep tick.
    async fn sweep_expired_sessions(&mut self) {
        // Periodically inherit persisted deadlines for owned sessions this node did not see
        // disconnect — those handed to it by a takeover (ADR 0009 §3).
        self.expiry_reconcile_tick = self.expiry_reconcile_tick.wrapping_add(1);
        if self.expiry_reconcile_tick % EXPIRY_RECONCILE_EVERY == 0 {
            self.reconcile_inherited_expiries().await;
        }

        let now = self.clock.now_epoch_secs();
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

    /// Pull persisted expiry deadlines from the durable store for sessions this node now
    /// **owns** but is not already tracking — the orphaned, never-reconnected sessions a
    /// takeover handed it. Without this, such a session would never expire on the new owner
    /// (the clock effectively restarts); with it, the new owner expires it at the original
    /// absolute deadline (ADR 0009 §3).
    async fn reconcile_inherited_expiries(&mut self) {
        let persisted = match self.store.expiring_sessions().await {
            Ok(v) => v,
            Err(e) => {
                debug!(error = %e, "could not read persisted session expiries");
                return;
            }
        };
        for (client, deadline) in persisted {
            // Skip ones we already handle (online here, or already scheduled), and ones we
            // do not own (a replica we hold for another node — its owner expires it).
            if self.online.contains_key(&client)
                || self.expiring.contains_key(&client)
                || !self.owns_session(&client)
            {
                continue;
            }
            // Schedule its expiry at the inherited absolute deadline; the sweep discards it
            // when due (discard_session removes the durable session and any local state).
            self.expiring.insert(client, deadline);
        }
    }

    /// Whether this node is the placement owner of `client`'s session. Outside a cluster
    /// (no placement, or a single member) every session is local, so it is always owned.
    fn owns_session(&self, client: &ClientId) -> bool {
        match &self.placement {
            None => true,
            Some(p) => p
                .read()
                .map_or(true, |p| p.member_count() <= 1 || p.owns(&client.0)),
        }
    }

    /// Refresh the broker state gauges (sessions, subscriptions, retained, inflight)
    /// from the in-memory maps. Run on the session sweep tick so the gauges track
    /// state cheaply without recomputing on every command (ADR 0020-T4).
    async fn refresh_gauges(&self) {
        let Some(m) = &self.metrics else { return };
        // Distinct sessions = connected clients plus offline persistent ones.
        let offline_persistent = self
            .session_expiry
            .keys()
            .filter(|c| !self.online.contains_key(*c))
            .count();
        m.set_sessions(self.online.len() + offline_persistent);
        m.set_subscriptions(self.subs_by_client.values().map(HashMap::len).sum());
        m.set_inflight_messages(self.inflight.values().map(|i| i.pending.len()).sum());
        if let Ok(n) = self.retained.count().await {
            m.set_retained_messages(n);
        }
        // Cluster shape (ADR 0020-T6): placement-eligible members and live peer links.
        m.set_peer_links(self.peers.len());
        if let Some(placement) = &self.placement {
            if let Ok(p) = placement.read() {
                m.set_cluster_members(p.member_count());
            }
        }
        // Lease-group role/epoch, read from the durable plane's raft metrics (durable mode).
        if let Some(plane) = &self.durable_plane {
            let (is_leader, epoch) = plane.lease_role();
            m.set_lease_role(is_leader, epoch);
        }
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
        self.shared.remove_client(client);
    }

    // --- cluster ---------------------------------------------------------------

    /// Forward a locally-originated publish to peers. A non-retained message goes
    /// only to peers whose announced interest matches (live delivery). A **retained**
    /// message goes to *every* peer regardless of current interest, so each node
    /// stores it for its future subscribers (ADR 0014). Receivers apply it locally
    /// only, so there is no relay/loop.
    fn forward_to_peers(
        &self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) {
        for (node, peer) in &self.peers {
            let interested = self
                .remote_interest
                .get(node)
                .is_some_and(|filters| filters.iter().any(|f| topic_matches(f, topic)));
            if retain || interested {
                let _ = peer.tx.send(PeerMessage::Publish {
                    topic: topic.to_string(),
                    payload: payload.to_vec(),
                    qos: qos as u8,
                    retain,
                    message_expiry,
                    app: app_to_wire(app),
                });
            }
        }
    }

    /// Offer `node` our retained topic-set digest (ADR 0014 §3, 0014-T6): the peer
    /// pulls the snapshot only if its own digest differs, so a link-up (or flap)
    /// between already-synced nodes transfers one small frame instead of the whole
    /// set. A no-op when we have no retained messages or the peer link is gone.
    async fn send_retained_digest(&self, node: &NodeId) {
        let Some(peer) = self.peers.get(node) else {
            return;
        };
        let Ok(retained) = self.retained.all().await else {
            return;
        };
        if retained.is_empty() {
            return;
        }
        let (count, hash, value_hash) = retained_digest(
            retained
                .iter()
                .map(|m| (m.topic.as_str(), m.payload.as_ref(), m.qos as u8)),
        );
        let _ = peer.tx.send(PeerMessage::RetainedDigest {
            count,
            hash,
            value_hash,
        });
    }

    /// Compare a peer's retained digest against our own (0014-T6 + ADR 0037 P1). Equal
    /// topic *and* value hashes mean the sets are identical — nothing to back-fill,
    /// nothing diverging, nothing transferred. Any difference: pull the peer's (chunked)
    /// snapshot — to gap-fill missing topics and to detect (count, warn) divergent
    /// values on topics both sides hold.
    async fn handle_retained_digest(&self, node: &NodeId, count: u64, hash: u64, value_hash: u64) {
        let Some(peer) = self.peers.get(node) else {
            return;
        };
        let Ok(retained) = self.retained.all().await else {
            return;
        };
        let ours = retained_digest(
            retained
                .iter()
                .map(|m| (m.topic.as_str(), m.payload.as_ref(), m.qos as u8)),
        );
        if ours == (count, hash, value_hash) {
            debug!(node = %node.0, topics = count, "retained sets already match; skipping back-fill");
            return;
        }
        let _ = peer.tx.send(PeerMessage::RetainedRequest);
    }

    /// Send our full retained set to `node` so it can back-fill any retained
    /// messages published before it joined (ADR 0014 §3), split into bounded
    /// chunks (0014-T8) so no frame can approach the peer frame limit — one
    /// oversized frame would kill the link on the receiving side, and the link-up
    /// back-fill would then re-kill it on every reconnect. Chunks are independent
    /// under the receiver's gap-fill rule, so no ordering or completion marker is
    /// needed. A no-op when we have no retained messages or the peer link is gone.
    async fn send_retained_snapshot(&self, node: &NodeId) {
        let Some(peer) = self.peers.get(node) else {
            return;
        };
        let Ok(retained) = self.retained.all().await else {
            return;
        };
        if retained.is_empty() {
            return;
        }
        let entries = retained
            .into_iter()
            .map(|m| (m.topic, m.payload.to_vec(), m.qos as u8));
        for messages in chunk_retained(entries) {
            let _ = peer.tx.send(PeerMessage::RetainedSnapshot { messages });
        }
    }

    /// Apply a peer's retained snapshot, **gap-fill** only: set a retained message
    /// for a topic only if we do not already retain that topic, so we never clobber
    /// our own (possibly newer) value with a peer's (ADR 0014 §3).
    ///
    /// Divergence detection (ADR 0037 P1): a topic both sides hold with **different**
    /// values is counted (`retained_divergence_total`) and surfaced with one `warn!` per
    /// snapshot chunk. Detection only — storage still follows the gap-fill rule until
    /// single-owner retained (ADR 0037) lands and makes divergence impossible.
    async fn apply_retained_snapshot(
        &mut self,
        node: &NodeId,
        messages: Vec<(String, Bytes, QoS)>,
    ) {
        let have: HashMap<String, u64> = match self.retained.all().await {
            Ok(all) => all
                .into_iter()
                .map(|m| {
                    let id = retained_value_id(&m.topic, m.payload.as_ref(), m.qos as u8);
                    (m.topic, id)
                })
                .collect(),
            Err(_) => return,
        };
        let mut filled = 0;
        let mut diverged = 0u64;
        for (topic, payload, qos) in messages {
            if let Some(ours) = have.get(&topic) {
                if *ours != retained_value_id(&topic, payload.as_ref(), qos as u8) {
                    diverged += 1;
                    debug!(node = %node.0, %topic, "retained value diverges from peer");
                    if let Some(m) = &self.metrics {
                        m.retained_divergence();
                    }
                }
                continue;
            }
            // The retained-snapshot wire does not carry application properties (a narrow
            // gap, like the persistent-retained codec); a back-filled retained message
            // has none.
            let message = Message {
                topic,
                payload,
                qos,
                retain: true,
                app: AppProperties::default(),
            };
            if self.retained.set(&message).await.is_ok() {
                filled += 1;
            }
        }
        if filled > 0 {
            debug!(filled, "back-filled retained messages from a peer snapshot");
        }
        if diverged > 0 {
            // One warn per chunk, not per topic — the per-topic detail is at debug and
            // the count is on the metric (bounded logging, ADR 0003-T6 style).
            warn!(
                node = %node.0,
                topics = diverged,
                "retained values DIVERGE from peer (same topic, different value) — \
                 best-effort replication kept each side's own value (ADR 0037 P1 detection)"
            );
        }
    }

    fn peer_connected(&mut self, node: NodeId, conn_id: u64, tx: PeerOutbound) {
        info!(local = %self.node_id.0, peer = %node.0, "peer link established");
        // Send our current interest + shared membership so the peer can route to us
        // immediately (ordinary fan-out and cluster-wide shared selection, ADR 0015).
        let _ = tx.send(PeerMessage::Interest {
            filters: self.local_interest(),
        });
        let _ = tx.send(PeerMessage::SharedInterest {
            groups: self.shared_snapshot(),
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
        self.remote_shared.remove(node);
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
        self.remote_shared.remove(node);
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

    /// This node's **ordinary** interest snapshot for cluster gossip. Shared-group
    /// filters are gossiped separately (ADR 0015 §2), not folded in here, since
    /// shared delivery rides the targeted `SharedDeliver` path, not ordinary forward.
    fn local_interest(&self) -> Vec<String> {
        self.table.filters()
    }

    /// This node's shared-subscription membership snapshot, in the peer wire form.
    fn shared_snapshot(&self) -> mqtt_cluster::peer::SharedGroupsWire {
        self.shared
            .snapshot()
            .into_iter()
            .map(|g| {
                // Tag each member with whether it is online here, so a peer's selector can
                // avoid choosing a member offline on its home node (ADR 0015 T8).
                let members = g
                    .members
                    .into_iter()
                    .map(|(c, q)| {
                        let online = self.online.contains_key(&c);
                        (c.0, q as u8, online)
                    })
                    .collect();
                (g.group, g.filter, members)
            })
            .collect()
    }

    /// Gossip this node's ordinary interest and shared membership to all peers.
    /// Called whenever local subscriptions change.
    fn gossip_interest(&self) {
        if self.peers.is_empty() {
            return;
        }
        let filters = self.local_interest();
        let groups = self.shared_snapshot();
        for peer in self.peers.values() {
            let _ = peer.tx.send(PeerMessage::Interest {
                filters: filters.clone(),
            });
            let _ = peer.tx.send(PeerMessage::SharedInterest {
                groups: groups.clone(),
            });
        }
    }

    /// Send a targeted shared delivery to a member on `node` (ADR 0015 §1).
    #[allow(clippy::too_many_arguments)] // mirrors the SharedDeliver wire fields
    fn send_shared_to_peer(
        &self,
        node: &NodeId,
        client: &ClientId,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) {
        if let Some(peer) = self.peers.get(node) {
            let _ = peer.tx.send(PeerMessage::SharedDeliver {
                client: client.0.clone(),
                topic: topic.to_string(),
                payload: payload.to_vec(),
                qos: qos as u8,
                message_expiry,
                app: app_to_wire(app),
            });
        }
    }
}

/// Convert in-memory application properties to their cross-node wire form (ADR 0030).
pub(crate) fn app_to_wire(a: &AppProperties) -> mqtt_cluster::peer::WireAppProps {
    mqtt_cluster::peer::WireAppProps {
        payload_format: a.payload_format,
        content_type: a.content_type.clone(),
        response_topic: a.response_topic.clone(),
        correlation_data: a.correlation_data.as_ref().map(|b| b.to_vec()),
        user_properties: a.user_properties.clone(),
    }
}

/// Convert cross-node wire application properties back to the in-memory form.
pub(crate) fn app_from_wire(w: mqtt_cluster::peer::WireAppProps) -> AppProperties {
    AppProperties {
        payload_format: w.payload_format,
        content_type: w.content_type,
        response_topic: w.response_topic,
        correlation_data: w.correlation_data.map(Bytes::from),
        user_properties: w.user_properties,
    }
}

#[allow(clippy::too_many_arguments)] // a thin PUBLISH constructor; all fields are the wire packet's
fn publish_packet(
    topic: &str,
    payload: Bytes,
    qos: QoS,
    pkid: Option<u16>,
    dup: bool,
    retain: bool,
    message_expiry: Option<u32>,
    app: &AppProperties,
) -> Packet {
    use mqtt_codec::Property;
    let mut properties = mqtt_codec::Properties::new();
    if let Some(secs) = message_expiry {
        properties.0.push(Property::MessageExpiryInterval(secs));
    }
    // Forward the publisher's application properties unaltered (MQTT-3.3.2-17, ADR 0030).
    if let Some(pf) = app.payload_format {
        properties.0.push(Property::PayloadFormatIndicator(pf));
    }
    if let Some(ct) = &app.content_type {
        properties.0.push(Property::ContentType(ct.clone()));
    }
    if let Some(rt) = &app.response_topic {
        properties.0.push(Property::ResponseTopic(rt.clone()));
    }
    if let Some(cd) = &app.correlation_data {
        properties.0.push(Property::CorrelationData(cd.clone()));
    }
    for (k, v) in &app.user_properties {
        properties
            .0
            .push(Property::UserProperty(k.clone(), v.clone()));
    }
    Packet::Publish(Publish {
        properties,
        dup,
        qos,
        retain,
        topic: topic.to_string(),
        pkid,
        payload,
    })
}

/// Recover a persistent session off the hub command loop and post the result back as
/// [`HubCommand::SessionRecovered`] (ADR 0017). Run in a spawned task so the bounded
/// lease/quorum wait never blocks the single-threaded hub.
async fn recover_session(
    store: Arc<dyn SessionStore>,
    self_tx: mpsc::UnboundedSender<HubCommand>,
    pending: PendingAttach,
) {
    let recovery = recover_until_ready(&store, &pending.client, &pending.owner).await;
    let _ = self_tx.send(HubCommand::SessionRecovered { pending, recovery });
}

/// Discard a clean-start client's prior **durable** state off the hub command loop, then
/// post `SessionRecovered::Cleaned` so the fresh session registers on the loop (ADR
/// 0017). The `remove` can do a first-touch group recovery on a cold owner; running it
/// here keeps that off the single-threaded hub. It is best-effort — a transient lease
/// error leaves any prior durable state to be reaped by a later discard/sweep — but the
/// in-memory wipe has already happened, so this session starts fresh regardless.
async fn discard_session(
    store: Arc<dyn SessionStore>,
    self_tx: mpsc::UnboundedSender<HubCommand>,
    pending: PendingAttach,
) {
    let _ = store.remove(&pending.client).await;
    let _ = self_tx.send(HubCommand::SessionRecovered {
        pending,
        recovery: SessionRecovery::Cleaned,
    });
}

/// Retry the durable session read until it answers authoritatively or the recovery
/// deadline elapses (ADR 0017). A transient `Unavailable` (lease reassigning / quorum
/// momentarily unreachable) is retried with capped backoff; a terminal error, or the
/// deadline, yields `Unavailable` so the attach rejects the CONNECT rather than
/// fabricate a clean session over a recoverable one.
async fn recover_until_ready(
    store: &Arc<dyn SessionStore>,
    client: &ClientId,
    owner: &str,
) -> SessionRecovery {
    let deadline = Instant::now() + ATTACH_RECOVERY_TIMEOUT;
    let mut backoff = ATTACH_RECOVERY_BACKOFF_START;
    loop {
        match recover_once(store, client, owner).await {
            Ok(ready) => return ready,
            // Transient and time remaining: back off and retry.
            Err(e) if e.is_transient() && Instant::now() < deadline => {}
            // Terminal failure, or the deadline passed: reject (never downgrade).
            Err(_) => return SessionRecovery::Unavailable,
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(ATTACH_RECOVERY_BACKOFF_MAX);
    }
}

/// One recovery attempt: `claim_session` (bind/verify the owning identity, ADR 0031) then
/// `subscriptions`, both authoritative durable reads. Surfaces the [`StorageError`] so the
/// caller can distinguish a transient condition from a terminal one. A claim refused because
/// the session belongs to another identity returns [`SessionRecovery::Denied`] — an
/// authoritative answer, not retried.
async fn recover_once(
    store: &Arc<dyn SessionStore>,
    client: &ClientId,
    owner: &str,
) -> Result<SessionRecovery, StorageError> {
    let present = match store.claim_session(client, owner).await? {
        SessionClaim::Granted { present } => present,
        SessionClaim::Denied { owner } => return Ok(SessionRecovery::Denied { owner }),
    };
    let subscriptions = store.subscriptions(client).await?;
    // Warm (and confirm the availability of) the offline-queue key as well, so the
    // inline replay in `finish_attach` reads a recovered queue and is never silently
    // skipped on a transient lease error — a resumed session must deliver its queued
    // messages on this connect, not only on a later reconnect (ADR 0017).
    let _ = store.pending(client, 0, 1).await?;
    Ok(SessionRecovery::Ready {
        present,
        subscriptions,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AttachOutcome, Backlog, Hub, HubCommand, Inflight, Outbound, PeerOutbound,
        RemoteSharedGroup, EXPIRY_RECONCILE_EVERY, MAX_BACKLOG, REPLAY_LIMIT,
    };
    use bytes::Bytes;
    use mqtt_cluster::peer::PeerMessage;
    use mqtt_cluster::NodeId;
    use mqtt_codec::{Packet, QoS};
    use mqtt_core::{AppProperties, ClientId};
    use mqtt_storage::{MemorySessionStore, OverflowPolicy, QueueLimits, SessionStore};
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

    /// A controllable wall clock for deterministic absolute-deadline tests: time only
    /// moves when the test calls [`advance`](TestClock::advance).
    #[derive(Debug, Clone)]
    struct TestClock(std::sync::Arc<std::sync::atomic::AtomicU64>);

    impl TestClock {
        fn new(start_epoch: u64) -> Self {
            Self(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                start_epoch,
            )))
        }
        fn advance(&self, secs: u64) {
            self.0.fetch_add(secs, std::sync::atomic::Ordering::Relaxed);
        }
    }

    impl crate::clock::Clock for TestClock {
        fn now_epoch_secs(&self) -> u64 {
            self.0.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    /// Spawn a hub whose wall clock is the returned [`TestClock`], so a test can move
    /// message-expiry deadlines forward without any real time passing.
    fn start_hub_with_clock() -> (HubTx, TestClock) {
        let clock = TestClock::new(1_000_000);
        let (mut hub, tx) = Hub::with_config(
            NodeId("hub-test".into()),
            std::sync::Arc::new(MemorySessionStore::new()),
        );
        hub.attach_clock(std::sync::Arc::new(clock.clone()));
        tokio::spawn(hub.run());
        (tx, clock)
    }

    fn start_hub_with_arc(store: std::sync::Arc<dyn mqtt_storage::SessionStore>) -> HubTx {
        let (hub, tx) = Hub::with_config(NodeId("hub-test".into()), store);
        tokio::spawn(hub.run());
        tx
    }

    /// Send a persistent (resume) `Attach` and return the raw [`AttachOutcome`] so a
    /// test can assert a reject (`Unavailable`) as well as a present/absent session.
    async fn attach_outcome(tx: &HubTx, client: &str, conn_id: u64) -> AttachOutcome {
        let (out_tx, _out_rx): (Outbound, _) = mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(HubCommand::Attach {
            client: ClientId(client.into()),
            owner: client.to_string(),
            conn_id,
            clean_start: false,
            session_expiry: u32::MAX,
            receive_maximum: u16::MAX,
            will: None,
            outbound: out_tx,
            reply: reply_tx,
        })
        .unwrap();
        reply_rx.await.unwrap()
    }

    /// Send a persistent (resume) `Attach` under an explicit owning identity `owner` — for
    /// the ADR 0031 session-identity-binding tests, which attach the *same* client id under
    /// *different* identities.
    async fn attach_outcome_as(
        tx: &HubTx,
        client: &str,
        owner: &str,
        conn_id: u64,
    ) -> AttachOutcome {
        let (out_tx, _out_rx): (Outbound, _) = mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(HubCommand::Attach {
            client: ClientId(client.into()),
            owner: owner.to_string(),
            conn_id,
            clean_start: false,
            session_expiry: u32::MAX,
            receive_maximum: u16::MAX,
            will: None,
            outbound: out_tx,
            reply: reply_tx,
        })
        .unwrap();
        reply_rx.await.unwrap()
    }

    // --- ADR 0031: session bound to the authenticated identity ---------------------

    /// A different authenticated identity may not resume another's persistent session; the
    /// owner can; the rejected identity inherits nothing.
    #[tokio::test]
    async fn a_different_identity_cannot_resume_a_persistent_session() {
        let tx = start_hub();

        // alice creates a persistent session for client id "shared".
        let first = attach_outcome_as(&tx, "shared", "alice", 1).await;
        assert!(
            matches!(first, AttachOutcome::Present(false)),
            "fresh: {first:?}"
        );
        detach(&tx, "shared", 1);

        // mallory, a different identity, may not resume it.
        let stolen = attach_outcome_as(&tx, "shared", "mallory", 2).await;
        assert!(
            matches!(stolen, AttachOutcome::OwnerMismatch),
            "a different identity must be refused, got {stolen:?}"
        );

        // alice, the owner, resumes it (session present).
        let resumed = attach_outcome_as(&tx, "shared", "alice", 3).await;
        assert!(
            matches!(resumed, AttachOutcome::Present(true)),
            "the owner must resume its own session, got {resumed:?}"
        );
    }

    /// A different identity may not take over a session that is *currently online*.
    #[tokio::test]
    async fn a_different_identity_cannot_take_over_an_online_session() {
        let tx = start_hub();

        // alice is online with "shared".
        let online = attach_outcome_as(&tx, "shared", "alice", 1).await;
        assert!(matches!(online, AttachOutcome::Present(false)));

        // mallory's takeover attempt (no detach — alice is still connected) is refused.
        let takeover = attach_outcome_as(&tx, "shared", "mallory", 2).await;
        assert!(
            matches!(takeover, AttachOutcome::OwnerMismatch),
            "a live session must not be seized by another identity, got {takeover:?}"
        );

        // alice can still take over her own session (legitimate reconnect).
        let reconnect = attach_outcome_as(&tx, "shared", "alice", 3).await;
        assert!(matches!(reconnect, AttachOutcome::Present(true)));
    }

    /// Under `allow_anonymous`, anonymous clients share one identity namespace (the documented
    /// insecure-by-toggle mode): the shared `"anonymous"` principal resumes its own session.
    #[tokio::test]
    async fn anonymous_clients_share_one_identity_namespace() {
        let tx = start_hub();

        let first = attach_outcome_as(&tx, "shared", "anonymous", 1).await;
        assert!(matches!(first, AttachOutcome::Present(false)));
        detach(&tx, "shared", 1);

        // Another anonymous connection is the *same* principal, so it resumes (no isolation
        // promised in this mode — ADR 0031 / ADR 0004).
        let second = attach_outcome_as(&tx, "shared", "anonymous", 2).await;
        assert!(
            matches!(second, AttachOutcome::Present(true)),
            "anonymous shares one namespace, got {second:?}"
        );
    }

    /// A `SessionStore` that fails the first `fail_ensure` `ensure_session` calls with
    /// the transient `Unavailable` error (modelling a lease handoff), then delegates to
    /// an in-memory store. The fault injection for the ADR 0017 readiness tests.
    #[derive(Debug)]
    struct FlakyStore {
        inner: MemorySessionStore,
        fail_remaining: std::sync::atomic::AtomicUsize,
        /// When set, every `enqueue_with_expiry` fails with `NoQuorum` (ADR 0020-T6).
        fail_enqueue_no_quorum: bool,
    }

    impl FlakyStore {
        fn new(fail_ensure: usize) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                inner: MemorySessionStore::new(),
                fail_remaining: std::sync::atomic::AtomicUsize::new(fail_ensure),
                fail_enqueue_no_quorum: false,
            })
        }

        /// A store whose durable append always fails with `NoQuorum` (everything else
        /// delegates to the in-memory store), for the append-failure metric test.
        fn new_no_quorum_enqueue() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                inner: MemorySessionStore::new(),
                fail_remaining: std::sync::atomic::AtomicUsize::new(0),
                fail_enqueue_no_quorum: true,
            })
        }
    }

    #[async_trait::async_trait]
    impl mqtt_storage::SessionStore for FlakyStore {
        async fn ensure_session(
            &self,
            client: &ClientId,
        ) -> Result<bool, mqtt_storage::StorageError> {
            use std::sync::atomic::Ordering;
            // Fail the first `fail_remaining` calls with the transient condition.
            if self
                .fail_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(mqtt_storage::StorageError::Unavailable(
                    "lease handing off".into(),
                ));
            }
            self.inner.ensure_session(client).await
        }

        async fn set_subscriptions(
            &self,
            client: &ClientId,
            subscriptions: &[mqtt_core::Subscription],
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.set_subscriptions(client, subscriptions).await
        }

        async fn subscriptions(
            &self,
            client: &ClientId,
        ) -> Result<Vec<mqtt_core::Subscription>, mqtt_storage::StorageError> {
            self.inner.subscriptions(client).await
        }

        async fn enqueue_with_expiry(
            &self,
            client: &ClientId,
            message: &mqtt_core::Message,
            expiry_at: Option<u64>,
        ) -> Result<mqtt_storage::Enqueued, mqtt_storage::StorageError> {
            if self.fail_enqueue_no_quorum {
                return Err(mqtt_storage::StorageError::NoQuorum);
            }
            self.inner
                .enqueue_with_expiry(client, message, expiry_at)
                .await
        }

        async fn pending(
            &self,
            client: &ClientId,
            after: mqtt_storage::Offset,
            limit: usize,
        ) -> Result<Vec<mqtt_storage::QueuedMessage>, mqtt_storage::StorageError> {
            self.inner.pending(client, after, limit).await
        }

        async fn ack(
            &self,
            client: &ClientId,
            up_to: mqtt_storage::Offset,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.ack(client, up_to).await
        }

        async fn record_received(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<bool, mqtt_storage::StorageError> {
            self.inner.record_received(client, packet_id).await
        }

        async fn clear_received(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.clear_received(client, packet_id).await
        }

        async fn received(
            &self,
            client: &ClientId,
        ) -> Result<Vec<u16>, mqtt_storage::StorageError> {
            self.inner.received(client).await
        }

        async fn next_packet_id(
            &self,
            client: &ClientId,
        ) -> Result<u16, mqtt_storage::StorageError> {
            self.inner.next_packet_id(client).await
        }

        async fn remove(&self, client: &ClientId) -> Result<(), mqtt_storage::StorageError> {
            self.inner.remove(client).await
        }
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

    /// Attach with explicit MQTT 5.0 `(clean_start, session_expiry)` and no outbound
    /// quota limit (the common case).
    async fn attach_v5(
        tx: &HubTx,
        client: &str,
        conn_id: u64,
        clean_start: bool,
        session_expiry: u32,
    ) -> (mpsc::UnboundedReceiver<Packet>, bool) {
        attach_full(tx, client, conn_id, clean_start, session_expiry, u16::MAX).await
    }

    /// Attach with an explicit Receive Maximum quota (ADR 0012), for flow-control tests.
    async fn attach_full(
        tx: &HubTx,
        client: &str,
        conn_id: u64,
        clean_start: bool,
        session_expiry: u32,
        receive_maximum: u16,
    ) -> (mpsc::UnboundedReceiver<Packet>, bool) {
        let (out_tx, out_rx): (Outbound, _) = mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(HubCommand::Attach {
            client: ClientId(client.into()),
            owner: client.to_string(),
            conn_id,
            clean_start,
            session_expiry,
            receive_maximum,
            will: None,
            outbound: out_tx,
            reply: reply_tx,
        })
        .unwrap();
        let session_present = match reply_rx.await.unwrap() {
            AttachOutcome::Present(present) => present,
            AttachOutcome::Unavailable => {
                panic!("in-memory store attach is never Unavailable")
            }
            AttachOutcome::OwnerMismatch => {
                panic!("same-owner attach is never an ownership mismatch")
            }
        };
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
        publish_with_expiry(tx, topic, payload, None);
    }

    fn publish_with_expiry(
        tx: &HubTx,
        topic: &str,
        payload: &'static [u8],
        message_expiry: Option<u32>,
    ) {
        tx.send(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::from_static(payload),
            qos: QoS::AtMostOnce,
            retain: false,
            message_expiry,
            app: AppProperties::default(),
        })
        .unwrap();
    }

    fn subscribe_qos(tx: &HubTx, client: &str, filter: &str, qos: QoS) {
        tx.send(HubCommand::Subscribe {
            client: ClientId(client.into()),
            filters: vec![(filter.into(), qos)],
        })
        .unwrap();
    }

    /// ADR 0020 (T8): a publish round-trip moves the metrics counters — the received and
    /// delivered counters both advance for the `QoS`, observable in the rendered exposition.
    #[tokio::test]
    async fn publish_round_trip_moves_the_metrics_counters() {
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (mut hub, tx) = Hub::with_config(
            NodeId("hub-test".into()),
            std::sync::Arc::new(MemorySessionStore::new()),
        );
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        let (mut out_rx, _) = attach_full(&tx, "sub", 1, true, 0, u16::MAX).await;
        subscribe(&tx, "sub", "t/1");
        publish(&tx, "t/1", b"hi"); // QoS 0

        // Receiving the delivered PUBLISH proves the publish was processed (counters moved).
        let pkt = timeout(Duration::from_millis(500), out_rx.recv())
            .await
            .expect("delivery")
            .expect("a packet");
        assert!(matches!(pkt, Packet::Publish(_)));

        let out = metrics.render();
        assert!(
            out.contains("mqttd_publish_received_total{qos=\"0\"} 1"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_publish_delivered_total{qos=\"0\"} 1"),
            "{out}"
        );
        // The publish path observed one deliver-latency sample (ADR 0020-T4).
        assert!(
            out.contains("mqttd_deliver_latency_seconds_count 1"),
            "{out}"
        );
        // No per-client/per-topic label leaked onto the message metrics.
        assert!(!out.contains("client="), "{out}");
        assert!(!out.contains("topic="), "{out}");
    }

    /// ADR 0020-T4: the periodic gauge refresh snapshots the in-memory maps onto the
    /// broker state gauges — a persistent session with two filters reads back as one
    /// session and two subscriptions in the rendered exposition.
    ///
    /// Deterministic via paused virtual time: the runtime drains the pending Subscribe
    /// commands (which need no timer) before it auto-advances the clock to fire the
    /// hub's sweep tick, so the sweep is guaranteed to observe the full state — no
    /// real-time polling or deadline.
    #[tokio::test(start_paused = true)]
    async fn gauge_refresh_snapshots_sessions_and_subscriptions() {
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (mut hub, tx) = Hub::with_config(
            NodeId("gauge-test".into()),
            std::sync::Arc::new(MemorySessionStore::new()),
        );
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        // A persistent session (clean_start=false, never-expire) with two filters.
        let (_out_rx, _) = attach_v5(&tx, "c1", 1, false, u32::MAX).await;
        subscribe(&tx, "c1", "a/b");
        subscribe(&tx, "c1", "c/d");

        // Advance past one sweep interval; the sweep refreshes the gauges off the maps.
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 2).await;

        let out = metrics.render();
        assert!(out.contains("mqttd_sessions 1"), "{out}");
        assert!(out.contains("mqttd_subscriptions 2"), "{out}");
    }

    fn publish_qos1(tx: &HubTx, topic: &str, payload: &'static [u8]) {
        tx.send(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::from_static(payload),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: AppProperties::default(),
        })
        .unwrap();
    }

    fn pub_ack(tx: &HubTx, client: &str, pkid: u16) {
        tx.send(HubCommand::PubAck {
            client: ClientId(client.into()),
            pkid,
        })
        .unwrap();
    }

    fn pkid_of(packet: &Packet) -> u16 {
        match packet {
            Packet::Publish(p) => p.pkid.expect("a QoS > 0 publish carries a packet id"),
            other => panic!("expected a publish, got {other:?}"),
        }
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

    /// Announce a peer's shared-group membership (one group, given members), all online
    /// on their home node.
    fn remote_shared_interest(tx: &HubTx, node: &str, group: &str, filter: &str, members: &[&str]) {
        let online: Vec<(&str, bool)> = members.iter().map(|c| (*c, true)).collect();
        remote_shared_interest_live(tx, node, group, filter, &online);
    }

    /// As [`remote_shared_interest`], but each member carries its liveness on the home
    /// node (ADR 0015 T8).
    fn remote_shared_interest_live(
        tx: &HubTx,
        node: &str,
        group: &str,
        filter: &str,
        members: &[(&str, bool)],
    ) {
        tx.send(HubCommand::RemoteSharedInterest {
            node: NodeId(node.into()),
            groups: vec![RemoteSharedGroup {
                group: group.into(),
                filter: filter.into(),
                members: members
                    .iter()
                    .map(|(c, online)| (ClientId((*c).into()), QoS::AtMostOnce, *online))
                    .collect(),
            }],
        })
        .unwrap();
    }

    /// The next `SharedDeliver` from a peer, skipping interest snapshots.
    async fn next_shared_deliver(rx: &mut mpsc::UnboundedReceiver<PeerMessage>) -> PeerMessage {
        loop {
            let msg = timeout(Duration::from_millis(300), rx.recv())
                .await
                .expect("a peer message")
                .expect("link open");
            if matches!(msg, PeerMessage::SharedDeliver { .. }) {
                return msg;
            }
        }
    }

    async fn recv_packet(rx: &mut mpsc::UnboundedReceiver<Packet>) -> Option<Packet> {
        timeout(Duration::from_millis(300), rx.recv()).await.ok()?
    }

    /// The next peer message, skipping the `SharedInterest` snapshots that now ride
    /// alongside every `Interest` gossip (ADR 0015) — these routing tests assert on
    /// ordinary interest and publishes, not shared membership.
    async fn recv_peer(rx: &mut mpsc::UnboundedReceiver<PeerMessage>) -> Option<PeerMessage> {
        loop {
            let msg = timeout(Duration::from_millis(300), rx.recv())
                .await
                .ok()??;
            if !matches!(msg, PeerMessage::SharedInterest { .. }) {
                return Some(msg);
            }
        }
    }

    fn payload_of(packet: &Packet) -> &[u8] {
        match packet {
            Packet::Publish(p) => &p.payload,
            other => panic!("expected a publish, got {other:?}"),
        }
    }

    fn message_expiry_of(packet: &Packet) -> Option<u32> {
        match packet {
            Packet::Publish(p) => p.properties.message_expiry_interval(),
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

    /// Cluster-wide shared selection (ADR 0015): with a local member and a peer
    /// member in the same group, the round-robin alternates — the local member is
    /// delivered to directly, and the remote pick goes out as a targeted
    /// `SharedDeliver` to the peer.
    #[tokio::test]
    async fn shared_selection_round_robins_local_and_remote_member() {
        let tx = start_hub();
        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));

        // A local member, and a member on peer "n", in the same group.
        let (mut ra, _) = attach(&tx, "ra", 1, true).await;
        subscribe(&tx, "ra", "$share/g/t");
        remote_shared_interest(&tx, "n", "g", "t", &["rb"]);

        // First publish: the local member (cursor 0) is delivered to directly.
        publish(&tx, "t", b"m1");
        assert_eq!(payload_of(&recv_packet(&mut ra).await.unwrap()), b"m1");

        // Second publish: the remote member (cursor 1) goes out as a SharedDeliver.
        publish(&tx, "t", b"m2");
        match next_shared_deliver(&mut peer).await {
            PeerMessage::SharedDeliver {
                client,
                topic,
                payload,
                ..
            } => {
                assert_eq!(client, "rb");
                assert_eq!(topic, "t");
                assert_eq!(&payload[..], b"m2");
            }
            other => panic!("expected SharedDeliver, got {other:?}"),
        }
        // The local member must not also have received the second publish.
        assert!(
            recv_packet(&mut ra).await.is_none(),
            "single delivery per publish"
        );
    }

    /// A remote member offline on its home node is skipped while a member online
    /// somewhere can deliver now (ADR 0015 T8): both publishes go to the local online
    /// member instead of queuing one at the offline remote member's home.
    #[tokio::test]
    async fn shared_selection_skips_an_offline_remote_member() {
        let tx = start_hub();
        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));

        let (mut ra, _) = attach(&tx, "ra", 1, true).await;
        subscribe(&tx, "ra", "$share/g/t");
        // A remote member "rb" that is OFFLINE on its home node "n".
        remote_shared_interest_live(&tx, "n", "g", "t", &[("rb", false)]);

        // Both publishes go to the local online member: were the offline remote chosen for
        // either (single delivery per publish), `ra` would miss that one.
        publish(&tx, "t", b"m1");
        assert_eq!(payload_of(&recv_packet(&mut ra).await.unwrap()), b"m1");
        publish(&tx, "t", b"m2");
        assert_eq!(payload_of(&recv_packet(&mut ra).await.unwrap()), b"m2");
    }

    /// On link-up the hub offers its retained topic-set **digest** (0014-T6), and a
    /// peer that pulls (its set differed) gets the retained snapshot, so a node that
    /// joined after a retained publish is back-filled (ADR 0014 §3).
    #[tokio::test]
    async fn retained_digest_is_offered_and_a_request_pulls_the_snapshot() {
        let tx = start_hub();
        publish_retained(&tx, "t", b"r");
        let mut peer = connect_peer(&tx, "n", 1);

        // The peer gets our interest snapshot, then our retained digest.
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));
        match recv_peer(&mut peer).await {
            Some(PeerMessage::RetainedDigest { count, hash, .. }) => {
                assert_eq!(count, 1);
                assert_ne!(hash, 0, "one topic hashes to a non-zero digest");
            }
            other => panic!("expected RetainedDigest, got {other:?}"),
        }

        // The peer's set differed, so it pulls — and gets the snapshot.
        tx.send(HubCommand::RemoteRetainedRequest {
            node: NodeId("n".into()),
        })
        .unwrap();
        match recv_peer(&mut peer).await {
            Some(PeerMessage::RetainedSnapshot { messages }) => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].0, "t");
                assert_eq!(&messages[0].1[..], b"r");
            }
            other => panic!("expected RetainedSnapshot, got {other:?}"),
        }
    }

    /// A peer whose digest matches ours is already in sync: no request, no snapshot —
    /// a steady-state link-up (or flap) transfers nothing (0014-T6).
    #[tokio::test]
    async fn a_matching_retained_digest_skips_the_back_fill() {
        let tx = start_hub();
        publish_retained(&tx, "t", b"r");
        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::RetainedDigest { .. })
        ));

        // The peer claims the same single (topic, value, qos) we hold: same digest, no pull.
        let (count, hash, value_hash) =
            super::retained_digest(std::iter::once(("t", b"r".as_ref(), 0u8)));
        tx.send(HubCommand::RemoteRetainedDigest {
            node: NodeId("n".into()),
            count,
            hash,
            value_hash,
        })
        .unwrap();
        // Nothing further arrives on the link (probe with a bounded wait).
        let quiet =
            tokio::time::timeout(std::time::Duration::from_millis(200), recv_peer(&mut peer)).await;
        assert!(quiet.is_err(), "matching digests must transfer nothing");
    }

    /// A digest that does NOT match ours makes us pull the peer's set (0014-T6).
    #[tokio::test]
    async fn a_differing_retained_digest_pulls_the_peers_set() {
        let tx = start_hub();
        publish_retained(&tx, "t", b"r");
        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::RetainedDigest { .. })
        ));

        // The peer holds a different set: we answer its digest with a pull.
        let (count, hash, value_hash) = super::retained_digest(
            [("t", b"r".as_ref(), 0u8), ("other", b"x".as_ref(), 0u8)].into_iter(),
        );
        tx.send(HubCommand::RemoteRetainedDigest {
            node: NodeId("n".into()),
            count,
            hash,
            value_hash,
        })
        .unwrap();
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::RetainedRequest)
        ));
    }

    /// ADR 0037 P1: identical topic *sets* but a differing **value** hash still triggers
    /// a pull — that is exactly the divergence case the old topics-only digest was blind
    /// to (and the pulled snapshot is what detection counts against).
    #[tokio::test]
    async fn a_value_only_digest_difference_triggers_a_pull() {
        let tx = start_hub();
        publish_retained(&tx, "t", b"ours");
        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::RetainedDigest { .. })
        ));

        // Same single topic, different value: set hash matches, value hash differs.
        let (count, hash, value_hash) =
            super::retained_digest(std::iter::once(("t", b"THEIRS".as_ref(), 0u8)));
        tx.send(HubCommand::RemoteRetainedDigest {
            node: NodeId("n".into()),
            count,
            hash,
            value_hash,
        })
        .unwrap();
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::RetainedRequest)
        ));
    }

    /// The snapshot is split into bounded chunks (0014-T8): a set larger than one
    /// chunk budget arrives as multiple frames, each under the budget, covering
    /// every topic exactly once. One oversized frame would kill the link on the
    /// receiving side — and the link-up back-fill would then re-kill every reconnect.
    #[test]
    fn a_large_retained_set_is_chunked_under_the_frame_budget() {
        // 9 entries of ~1 MiB against a 4 MiB budget → at least 3 chunks.
        let payload = vec![0u8; 1024 * 1024];
        let entries = (0..9).map(|i| (format!("t/{i}"), payload.clone(), 0u8));
        let chunks = super::chunk_retained(entries);
        assert!(chunks.len() >= 3, "9 MiB must not fit 2 chunks of 4 MiB");
        for chunk in &chunks {
            let bytes: usize = chunk.iter().map(|(t, p, _)| t.len() + p.len() + 32).sum();
            assert!(bytes <= super::RETAINED_CHUNK_BYTES, "chunk over budget");
        }
        let total: usize = chunks.iter().map(Vec::len).sum();
        assert_eq!(total, 9, "every entry appears in exactly one chunk");
    }

    /// A single retained message that could never fit a frame is skipped (with a
    /// warning), not sent — sending it would sever the link instead of just missing
    /// one back-fill (0014-T8).
    #[test]
    fn an_oversized_single_retained_message_is_skipped_not_sent() {
        let huge = vec![0u8; super::RETAINED_CHUNK_BYTES + 1];
        let entries = vec![
            ("ok".to_string(), vec![1u8; 8], 0u8),
            ("huge".to_string(), huge, 0u8),
        ];
        let chunks = super::chunk_retained(entries.into_iter());
        let all: Vec<&str> = chunks
            .iter()
            .flatten()
            .map(|(t, _, _)| t.as_str())
            .collect();
        assert_eq!(
            all,
            vec!["ok"],
            "the oversized entry is dropped, the rest kept"
        );
    }

    /// The digest is order-independent and topic-set-sensitive (0014-T6), and its value
    /// hash sees payload changes the topic-set hash ignores (ADR 0037 P1).
    #[test]
    fn the_retained_digest_is_order_independent_and_set_sensitive() {
        let one = ("x", b"1".as_ref(), 0u8);
        let two = ("y", b"2".as_ref(), 1u8);
        let three = ("z", b"3".as_ref(), 0u8);
        let full = super::retained_digest([one, two, three].into_iter());
        let shuffled = super::retained_digest([three, one, two].into_iter());
        assert_eq!(full, shuffled, "order must not matter");
        let subset = super::retained_digest([one, two].into_iter());
        assert_ne!(full, subset, "a different set must differ");
        // Same topics, different value: topic hash equal, value hash different.
        let two_changed = ("y", b"CHANGED".as_ref(), 1u8);
        let diverged = super::retained_digest([one, two_changed, three].into_iter());
        assert_eq!(full.1, diverged.1, "topic-set hash ignores values");
        assert_ne!(
            full.2, diverged.2,
            "value hash must see the changed payload"
        );
        assert_eq!(super::retained_digest(std::iter::empty()), (0, 0, 0));
    }

    /// A received retained snapshot back-fills the store, so a later local
    /// subscriber gets the message (ADR 0014 §3).
    #[tokio::test]
    async fn received_retained_snapshot_replays_on_subscribe() {
        let tx = start_hub();
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![("room/t".into(), Bytes::from_static(b"v"), QoS::AtMostOnce)],
        })
        .unwrap();

        let (mut rx, _) = attach(&tx, "c", 1, true).await;
        subscribe(&tx, "c", "room/t");
        let p = recv_packet(&mut rx).await.unwrap();
        assert_eq!(payload_of(&p), b"v");
    }

    /// ADR 0037 P1: a peer snapshot holding a **different value** for a topic we also
    /// retain is detected — `retained_divergence_total` increments — while storage still
    /// follows the gap-fill rule (our value is kept, detection only).
    #[tokio::test]
    async fn a_divergent_retained_value_is_detected_and_counted() {
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (mut hub, tx) = Hub::with_config(
            NodeId("hub-test".into()),
            std::sync::Arc::new(MemorySessionStore::new()),
        );
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        publish_retained(&tx, "dev/1", b"ours");
        // The peer's snapshot: one divergent value, one identical-topic-same-value
        // (no count), one new topic (gap-fill, no count).
        publish_retained(&tx, "dev/same", b"agreed");
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![
                (
                    "dev/1".into(),
                    Bytes::from_static(b"theirs"),
                    QoS::AtMostOnce,
                ),
                (
                    "dev/same".into(),
                    Bytes::from_static(b"agreed"),
                    QoS::AtMostOnce,
                ),
                ("dev/new".into(), Bytes::from_static(b"x"), QoS::AtMostOnce),
            ],
        })
        .unwrap();

        // Our value is kept (gap-fill unchanged) — proving via a subscriber replay.
        let (mut rx, _) = attach(&tx, "c", 1, true).await;
        subscribe(&tx, "c", "dev/1");
        assert_eq!(
            payload_of(&recv_packet(&mut rx).await.unwrap()),
            b"ours",
            "detection must not change storage"
        );

        // Exactly one divergence was counted (dev/1) — not the agreeing or new topics.
        let text = metrics.render();
        assert!(
            text.contains("retained_divergence_total 1"),
            "one divergent topic must count exactly once:\n{text}"
        );
    }

    /// Back-fill is gap-fill: a snapshot never overwrites a retained message we
    /// already hold with the peer's (possibly stale) value (ADR 0014 §3).
    #[tokio::test]
    async fn retained_snapshot_does_not_overwrite_existing() {
        let tx = start_hub();
        publish_retained(&tx, "t", b"local");
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![(
                "t".into(),
                Bytes::from_static(b"peer-stale"),
                QoS::AtMostOnce,
            )],
        })
        .unwrap();

        let (mut rx, _) = attach(&tx, "c", 1, true).await;
        subscribe(&tx, "c", "t");
        assert_eq!(
            payload_of(&recv_packet(&mut rx).await.unwrap()),
            b"local",
            "our own retained value is kept"
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

    /// A message published with an MQTT 5.0 expiry interval carries that
    /// interval to an online subscriber (ADR 0009 §3).
    #[tokio::test]
    async fn live_delivery_carries_message_expiry_interval() {
        let tx = start_hub();
        let (mut rx, _) = attach(&tx, "s", 1, true).await;
        subscribe(&tx, "s", "t");
        publish_with_expiry(&tx, "t", b"hi", Some(120));
        let pkt = recv_packet(&mut rx).await.unwrap();
        assert_eq!(payload_of(&pkt), b"hi");
        assert_eq!(message_expiry_of(&pkt), Some(120));
    }

    /// A queued message whose expiry deadline has passed is dropped at replay,
    /// not delivered (ADR 0009 §3). A 0-second interval expires the instant the
    /// message is received, so it is always stale by the time the session
    /// reconnects; the still-fresh message behind it replays normally.
    #[tokio::test]
    async fn expired_queued_message_is_dropped_at_replay() {
        let tx = start_hub();
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe(&tx, "p", "t");
        detach(&tx, "p", 1);
        publish_with_expiry(&tx, "t", b"stale", Some(0));
        publish_with_expiry(&tx, "t", b"fresh", Some(3600));

        let (mut rx, _) = attach(&tx, "p", 2, false).await;
        let pkt = recv_packet(&mut rx).await.unwrap();
        assert_eq!(
            payload_of(&pkt),
            b"fresh",
            "the expired message must be skipped"
        );
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "only the still-fresh message replays"
        );
    }

    /// Time-injected expiry (ADR 0009 §3): a message queued with a finite interval is
    /// dropped at replay once that interval has actually elapsed — exercised with an
    /// injected clock, so the real `now + interval` / `now >= deadline` arithmetic is
    /// tested without the `expiry=0` shortcut or any real wall-clock wait.
    #[tokio::test]
    async fn queued_message_expires_once_its_interval_elapses() {
        let (tx, clock) = start_hub_with_clock();
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe(&tx, "p", "t");
        detach(&tx, "p", 1);

        // Enqueue with a 10s interval (deadline = clock now + 10), still fresh.
        publish_with_expiry(&tx, "t", b"q", Some(10));
        // Barrier: a round-trip attach flushes the FIFO command queue, so the publish
        // above is enqueued at the *current* clock before we move it (otherwise the
        // synchronous advance could race ahead of the async enqueue).
        let _ = attach(&tx, "barrier", 99, true).await;

        // Move the clock 11s forward: the message is now past its absolute deadline.
        clock.advance(11);

        let (mut rx, _) = attach(&tx, "p", 2, false).await;
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "a message whose interval has elapsed must be dropped at replay"
        );
    }

    /// The companion to the above: the same message replays intact when the clock has
    /// *not* advanced past its deadline — proving the drop is the elapsed time, not the
    /// queueing itself.
    #[tokio::test]
    async fn queued_message_survives_while_its_interval_remains() {
        let (tx, clock) = start_hub_with_clock();
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe(&tx, "p", "t");
        detach(&tx, "p", 1);

        publish_with_expiry(&tx, "t", b"q", Some(10));
        let _ = attach(&tx, "barrier", 99, true).await; // flush the enqueue (see above)
        clock.advance(3); // well within the 10s window

        let (mut rx, _) = attach(&tx, "p", 2, false).await;
        assert_eq!(
            payload_of(&recv_packet(&mut rx).await.unwrap()),
            b"q",
            "a message still within its interval must replay"
        );
    }

    /// ADR 0020-T6: a durable append that fails surfaces on the failure counter under
    /// its bounded reason class — here `no-quorum` from the replicated store.
    #[tokio::test]
    async fn a_failed_durable_append_is_counted_by_reason() {
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (mut hub, tx) =
            Hub::with_config(NodeId("h".into()), FlakyStore::new_no_quorum_enqueue());
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        // A persistent, offline subscriber: a publish to it takes the durable-enqueue
        // path, which this store fails with NoQuorum.
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe(&tx, "p", "t");
        detach(&tx, "p", 1);
        publish(&tx, "t", b"x");

        // The publish is processed off-loop; poll the exposition until the counter moves.
        for _ in 0..200 {
            if metrics
                .render()
                .contains("mqttd_durable_append_failures_total{reason=\"no-quorum\"} 1")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "durable-append failure was never counted:\n{}",
            metrics.render()
        );
    }

    /// The append-failure reason classes are bounded and map each `StorageError`.
    #[test]
    fn durable_failure_reasons_are_bounded() {
        assert_eq!(
            super::durable_failure_reason(&mqtt_storage::StorageError::NoQuorum),
            "no-quorum"
        );
        assert_eq!(
            super::durable_failure_reason(&mqtt_storage::StorageError::NotOwner),
            "not-owner"
        );
        assert_eq!(
            super::durable_failure_reason(&mqtt_storage::StorageError::Unavailable("x".into())),
            "unavailable"
        );
        assert_eq!(
            super::durable_failure_reason(&mqtt_storage::StorageError::Backend("x".into())),
            "backend"
        );
        assert_eq!(
            super::durable_failure_reason(&mqtt_storage::StorageError::NotFound),
            "not-found"
        );
    }

    /// A queued message with a live deadline replays with the *remaining*
    /// interval, not the original one it was published with (ADR 0009 §3).
    #[tokio::test]
    async fn replayed_message_forwards_remaining_expiry_interval() {
        let tx = start_hub();
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe(&tx, "p", "t");
        detach(&tx, "p", 1);
        publish_with_expiry(&tx, "t", b"q", Some(3600));

        let (mut rx, _) = attach(&tx, "p", 2, false).await;
        let pkt = recv_packet(&mut rx).await.unwrap();
        assert_eq!(payload_of(&pkt), b"q");
        let remaining = message_expiry_of(&pkt).expect("a forwarded expiry interval");
        assert!(
            remaining > 0 && remaining <= 3600,
            "remaining interval within bounds: {remaining}"
        );
    }

    /// The flow-control backlog is bounded: past the cap it drops the oldest held
    /// message rather than growing without limit (ADR 0012).
    #[test]
    fn flow_control_backlog_is_bounded_drop_oldest() {
        let mut inf = Inflight::default();
        let entry = |topic: String| Backlog {
            message: mqtt_core::Message {
                topic,
                payload: Bytes::from_static(b"x"),
                qos: QoS::AtLeastOnce,
                retain: false,
                app: AppProperties::default(),
            },
            retain: false,
            message_expiry: None,
        };
        for i in 0..MAX_BACKLOG {
            assert!(
                !inf.push_backlog(entry(format!("t{i}"))),
                "no eviction under the cap"
            );
        }
        // At the cap, the next push evicts the oldest (t0) and stays bounded.
        assert!(
            inf.push_backlog(entry("overflow".into())),
            "eviction at the cap"
        );
        assert_eq!(inf.backlog.len(), MAX_BACKLOG, "backlog stays bounded");
        assert_eq!(
            inf.backlog.front().unwrap().message.topic,
            "t1",
            "oldest was dropped"
        );
        assert_eq!(inf.backlog.back().unwrap().message.topic, "overflow");
    }

    /// Receive Maximum bounds in-flight `QoS` > 0 deliveries: with a quota of 1, the
    /// second message waits until the first is acked, then drains (ADR 0012).
    #[tokio::test]
    async fn receive_maximum_holds_excess_until_acked() {
        let tx = start_hub();
        let (mut rx, _) = attach_full(&tx, "c", 1, true, 0, 1).await;
        subscribe_qos(&tx, "c", "t", QoS::AtLeastOnce);

        publish_qos1(&tx, "t", b"m1");
        publish_qos1(&tx, "t", b"m2");

        let p1 = recv_packet(&mut rx).await.unwrap();
        assert_eq!(payload_of(&p1), b"m1");
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "the second publish is held until the quota frees"
        );

        pub_ack(&tx, "c", pkid_of(&p1));
        let p2 = recv_packet(&mut rx).await.unwrap();
        assert_eq!(payload_of(&p2), b"m2", "acking drains the backlog");
    }

    /// `QoS` 0 is never throttled by Receive Maximum, even with the `QoS` > 0 quota full.
    #[tokio::test]
    async fn qos0_is_not_subject_to_receive_maximum() {
        let tx = start_hub();
        let (mut rx, _) = attach_full(&tx, "c", 1, true, 0, 1).await;
        subscribe_qos(&tx, "c", "t", QoS::AtLeastOnce);

        publish_qos1(&tx, "t", b"q1"); // fills the quota of 1
        assert_eq!(payload_of(&recv_packet(&mut rx).await.unwrap()), b"q1");

        publish(&tx, "t", b"zero"); // QoS 0 — flows despite the full quota
        assert_eq!(payload_of(&recv_packet(&mut rx).await.unwrap()), b"zero");
    }

    /// A persistent session's never-sent backlog spills to the durable queue on
    /// detach and replays on reconnect, after the DUP-redelivered in-flight (ADR 0012).
    #[tokio::test]
    async fn quota_backlog_spills_to_store_on_persistent_detach() {
        let tx = start_hub();
        let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 1).await;
        subscribe_qos(&tx, "c", "t", QoS::AtLeastOnce);
        publish_qos1(&tx, "t", b"m1");
        publish_qos1(&tx, "t", b"m2"); // backlogged behind the quota
        assert_eq!(payload_of(&recv_packet(&mut rx).await.unwrap()), b"m1");

        // Disconnect without acking m1: m1 stays in-flight, m2 spills to the store.
        detach(&tx, "c", 1);

        let (mut rx2, present) = attach_full(&tx, "c", 2, false, u32::MAX, 8).await;
        assert!(present);
        assert_eq!(
            payload_of(&recv_packet(&mut rx2).await.unwrap()),
            b"m1",
            "DUP resume first"
        );
        assert_eq!(
            payload_of(&recv_packet(&mut rx2).await.unwrap()),
            b"m2",
            "then the spilled backlog"
        );
    }

    fn publish_retained(tx: &HubTx, topic: &str, payload: &'static [u8]) {
        tx.send(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::from_static(payload),
            qos: QoS::AtMostOnce,
            retain: true,
            message_expiry: None,
            app: AppProperties::default(),
        })
        .unwrap();
    }

    /// A shared subscription (ADR 0010) delivers each matching message to exactly
    /// one group member, round-robin — not to every member.
    #[tokio::test]
    async fn shared_subscription_round_robins_one_member() {
        let tx = start_hub();
        let (mut a, _) = attach(&tx, "a", 1, true).await;
        let (mut b, _) = attach(&tx, "b", 2, true).await;
        subscribe(&tx, "a", "$share/grp/t/+");
        subscribe(&tx, "b", "$share/grp/t/+");

        publish(&tx, "t/1", b"m1");
        publish(&tx, "t/2", b"m2");

        // Round-robin in subscribe order: a gets the first, b the second, and
        // neither sees a duplicate.
        assert_eq!(payload_of(&recv_packet(&mut a).await.unwrap()), b"m1");
        assert_eq!(payload_of(&recv_packet(&mut b).await.unwrap()), b"m2");
        assert!(recv_packet(&mut a).await.is_none());
        assert!(recv_packet(&mut b).await.is_none());
    }

    /// An ordinary and a shared subscription matching the same topic are
    /// independent: both receive the message.
    #[tokio::test]
    async fn ordinary_and_shared_subscriptions_are_independent() {
        let tx = start_hub();
        let (mut ord, _) = attach(&tx, "o", 1, true).await;
        let (mut sh, _) = attach(&tx, "s", 2, true).await;
        subscribe(&tx, "o", "t");
        subscribe(&tx, "s", "$share/g/t");
        publish(&tx, "t", b"x");
        assert_eq!(payload_of(&recv_packet(&mut ord).await.unwrap()), b"x");
        assert_eq!(payload_of(&recv_packet(&mut sh).await.unwrap()), b"x");
    }

    /// A new shared subscription is not sent retained messages [MQTT-3.8.4];
    /// an ordinary one still is.
    #[tokio::test]
    async fn shared_subscription_skips_retained_messages() {
        let tx = start_hub();
        publish_retained(&tx, "t", b"r");

        let (mut sh, _) = attach(&tx, "s", 1, true).await;
        subscribe(&tx, "s", "$share/g/t");
        assert!(
            recv_packet(&mut sh).await.is_none(),
            "shared subscriptions receive no retained messages"
        );

        let (mut ord, _) = attach(&tx, "o", 2, true).await;
        subscribe(&tx, "o", "t");
        assert_eq!(payload_of(&recv_packet(&mut ord).await.unwrap()), b"r");
    }

    /// With no online member, a shared message queues for a persistent offline
    /// member and replays on its reconnect.
    #[tokio::test]
    async fn shared_message_queues_for_offline_persistent_member() {
        let tx = start_hub();
        let (_a, _) = attach(&tx, "a", 1, false).await;
        subscribe(&tx, "a", "$share/g/t");
        detach(&tx, "a", 1);

        publish(&tx, "t", b"queued");

        let (mut a, present) = attach(&tx, "a", 2, false).await;
        assert!(present);
        assert_eq!(payload_of(&recv_packet(&mut a).await.unwrap()), b"queued");
    }

    /// Selection prefers an online member over a persistent offline one, so a
    /// live consumer is never starved by round-robin landing on a sleeping peer.
    #[tokio::test]
    async fn shared_delivery_prefers_online_over_offline_member() {
        let tx = start_hub();
        let (_off, _) = attach(&tx, "off", 1, false).await;
        let (mut on, _) = attach(&tx, "on", 2, true).await;
        subscribe(&tx, "off", "$share/g/t");
        subscribe(&tx, "on", "$share/g/t");
        detach(&tx, "off", 1); // now offline but persistent

        publish(&tx, "t", b"1");
        publish(&tx, "t", b"2");
        assert_eq!(payload_of(&recv_packet(&mut on).await.unwrap()), b"1");
        assert_eq!(payload_of(&recv_packet(&mut on).await.unwrap()), b"2");
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
        let (tx, clock) = start_hub_with_clock();
        let (_rx, _) = attach_v5(&tx, "e", 1, false, 1).await;
        subscribe(&tx, "e", "e/t");
        detach(&tx, "e", 1);
        // Retained during the expiry window: the offline message queues.
        publish(&tx, "e/t", b"m");

        // Let the actor record the deadline (from the current clock) before advancing, so
        // the deadline is computed from "now", not the post-advance time.
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Past the 1s wall-clock interval (the deadline is absolute epoch now), then let a
        // sweep tick fire to discard the session.
        clock.advance(3);
        tokio::time::sleep(Duration::from_secs(2)).await;

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

    /// ADR 0009 §3 takeover: a session whose absolute expiry deadline was persisted by a
    /// *prior* owner (this hub never saw it connect or disconnect) is inherited from the
    /// durable store and expired at the **original** deadline — the clock does not restart.
    #[tokio::test(start_paused = true)]
    async fn inherited_session_expiry_is_swept_after_takeover() {
        use std::sync::Arc;
        // The durable store already holds a persistent session with a finite deadline, as if
        // a now-failed owner had persisted it before dying.
        let store = Arc::new(MemorySessionStore::new());
        let client = ClientId("orphan".into());
        store.ensure_session(&client).await.unwrap();
        store
            .set_session_expiry(&client, Some(1_000_050))
            .await
            .unwrap();

        // A fresh hub (the new owner) over that store, its wall clock just before the
        // deadline. No placement → it owns every session.
        let clock = TestClock::new(1_000_000);
        let (mut hub, _tx) = Hub::with_config(NodeId("new-owner".into()), store.clone());
        hub.attach_clock(Arc::new(clock.clone()));
        tokio::spawn(hub.run());

        // Past at least one reconcile cadence but before the deadline: the deadline is
        // inherited (scheduled) but the session is kept.
        tokio::time::sleep(Duration::from_secs(u64::from(EXPIRY_RECONCILE_EVERY + 2))).await;
        assert_eq!(
            store.expiring_sessions().await.unwrap().len(),
            1,
            "deadline still persisted before it elapses"
        );

        // Past the deadline: the next sweep discards the inherited session.
        clock.advance(100);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            store.expiring_sessions().await.unwrap().is_empty(),
            "inherited session expired at the original deadline"
        );
        assert!(
            store.subscriptions(&client).await.unwrap().is_empty(),
            "the durable session was removed"
        );
    }

    /// ADR 0007 T9: a new owner allocates outbound packet ids **past** the durable
    /// high-water it inherited, instead of restarting at 1 and risking reuse of an id the
    /// client still considers in flight from the prior owner.
    #[tokio::test]
    async fn outbound_packet_ids_resume_past_the_durable_high_water() {
        use std::sync::Arc;
        // A durable store where a prior owner already reserved ids up to 5000 for a
        // persistent subscriber.
        let store: Arc<dyn mqtt_storage::SessionStore> = Arc::new(MemorySessionStore::new());
        let sub = ClientId("sub".into());
        store.ensure_session(&sub).await.unwrap();
        store.reserve_packet_ids(&sub, 5000).await.unwrap();

        // A fresh hub (the takeover owner) over that store; the subscriber resumes.
        let tx = start_hub_with_arc(store);
        let (mut rx, _) = attach_v5(&tx, "sub", 1, false, 100).await;
        subscribe_qos(&tx, "sub", "t", QoS::AtLeastOnce);

        // The first QoS 1 delivery's packet id is past the inherited high-water, not 1.
        publish_qos1(&tx, "t", b"m");
        let pkid = pkid_of(&recv_packet(&mut rx).await.unwrap());
        assert!(
            pkid > 5000,
            "packet id {pkid} resumed past the inherited high-water"
        );
    }

    /// A publisher's Message Expiry Interval is carried on the cross-node forward, so a
    /// peer's queued copy keeps the same deadline (ADR 0014 T9).
    #[tokio::test]
    async fn forwarded_publish_carries_message_expiry() {
        let tx = start_hub();
        let mut p1 = connect_peer(&tx, "n1", 1);
        recv_peer(&mut p1).await; // initial interest snapshot
        remote_interest(&tx, "n1", &["a/#"]);

        publish_with_expiry(&tx, "a/x", b"ttl", Some(45));
        match recv_peer(&mut p1).await {
            Some(PeerMessage::Publish {
                topic,
                message_expiry,
                ..
            }) => {
                assert_eq!(topic, "a/x");
                assert_eq!(message_expiry, Some(45), "expiry carried over the link");
            }
            other => panic!("expected forwarded Publish, got {other:?}"),
        }
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
            retain: false,
            message_expiry: None,
            app: AppProperties::default(),
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

    // --- ADR 0017: durable attach readiness ----------------------------------

    /// A transient store condition (lease handoff) during a persistent attach must be
    /// *waited out*, never downgraded to a clean session: the attach resolves to a real
    /// `Present(_)` once the store recovers, and the session it creates is reported
    /// `present=true` on the next reconnect.
    #[tokio::test(start_paused = true)]
    async fn transient_lease_does_not_downgrade_a_persistent_attach() {
        let store = FlakyStore::new(3); // first 3 ensure_session calls fail transiently
        let tx = start_hub_with_arc(store);

        // First attach rides out the transient failures and resolves authoritatively
        // (a brand-new session, so present=false) — crucially NOT a reject.
        let outcome = attach_outcome(&tx, "c", 1).await;
        assert!(
            matches!(outcome, AttachOutcome::Present(false)),
            "transient errors must be waited out, not rejected/downgraded; got {outcome:?}"
        );
        detach(&tx, "c", 1);

        // The session was durably created; reconnecting reports it present.
        let outcome = attach_outcome(&tx, "c", 2).await;
        assert!(
            matches!(outcome, AttachOutcome::Present(true)),
            "the recovered persistent session must come up present; got {outcome:?}"
        );
    }

    /// A store that never becomes available within the recovery deadline must make the
    /// attach *reject* (so the client retries), never report a false `Present(false)`
    /// that would silently reset a recoverable session.
    #[tokio::test(start_paused = true)]
    async fn permanently_unavailable_store_rejects_rather_than_downgrades() {
        let store = FlakyStore::new(usize::MAX); // every ensure_session fails transiently
        let tx = start_hub_with_arc(store);

        let outcome = attach_outcome(&tx, "c", 1).await;
        assert!(
            matches!(outcome, AttachOutcome::Unavailable),
            "a never-ready store must reject the CONNECT, not downgrade; got {outcome:?}"
        );
    }

    /// The recovery wait runs off the hub command loop: while one client's persistent
    /// attach is still recovering, the hub keeps serving other commands (here, a second
    /// client's clean attach completes promptly).
    #[tokio::test(start_paused = true)]
    async fn recovery_wait_does_not_block_the_hub_loop() {
        let store = FlakyStore::new(usize::MAX); // "a" will recover forever
        let tx = start_hub_with_arc(store);

        // Kick off a persistent attach for "a" that will not resolve.
        let (out_tx, _out_rx): (Outbound, _) = mpsc::unbounded_channel();
        let (reply_tx, mut a_reply) = oneshot::channel();
        tx.send(HubCommand::Attach {
            client: ClientId("a".into()),
            owner: "a".to_string(),
            conn_id: 1,
            clean_start: false,
            session_expiry: u32::MAX,
            receive_maximum: u16::MAX,
            will: None,
            outbound: out_tx,
            reply: reply_tx,
        })
        .unwrap();

        // While "a" is mid-recovery, a clean attach for "b" must still complete quickly.
        let b = timeout(Duration::from_secs(1), attach(&tx, "b", 2, true)).await;
        let (_rx, present) = b.expect("the hub stayed responsive during a recovery wait");
        assert!(!present, "clean attach has no prior session");

        // "a" is still waiting (not yet resolved) — the loop was never blocked on it.
        assert!(
            a_reply.try_recv().is_err(),
            "the unresolved recovery must still be pending"
        );
    }

    /// Overlapping persistent connects for the same id: the newer one wins. The older
    /// recovery, if it lands late, is dropped rather than registering a stale session.
    #[tokio::test(start_paused = true)]
    async fn overlapping_connects_are_last_writer_wins() {
        let store = FlakyStore::new(0); // recovers immediately
        let tx = start_hub_with_arc(store);

        // Two connects for "c" in quick succession; conn 2 supersedes conn 1.
        let o1 = attach_outcome(&tx, "c", 1).await;
        let o2 = attach_outcome(&tx, "c", 2).await;
        assert!(matches!(o1, AttachOutcome::Present(_)));
        assert!(matches!(o2, AttachOutcome::Present(_)));

        // The live connection is conn 2: a detach of the stale conn 1 is ignored, while
        // a detach of conn 2 actually tears the session down (proving 2 is registered).
        detach(&tx, "c", 1);
        let still_present = attach_outcome(&tx, "c", 3).await;
        assert!(
            matches!(still_present, AttachOutcome::Present(true)),
            "the session survives a stale connection's detach; got {still_present:?}"
        );
    }
}
