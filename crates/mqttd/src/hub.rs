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
use mqtt_cluster::peer::{PeerMessage, RetainedWireEntry};
use mqtt_cluster::placement::Placement;
use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Disconnect, Publish},
    Packet, ProtocolVersion, QoS,
};
use mqtt_core::{
    parse_shared, topic_matches, AppProperties, ClientId, Message, SharedSubscriptionTable,
    Subscription, SubscriptionTable,
};
use mqtt_storage::app_props::AppProps;
use mqtt_storage::retained_log::DurableRetained;
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

/// How a connection authenticated (ADR 0040 T1): the credential class whose
/// server-side facts a policy-reload sweep can re-check. `Token`/`Enhanced`
/// credentials carry their own lifetime (a JWT's `exp`; a mechanism exchange) and
/// have no server-side store row to probe, so a sweep bounds them via the ACL only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    /// No credentials (only admitted when the policy opts in).
    Anonymous,
    /// Username/password against the credential store.
    Password,
    /// A bearer token (JWT/OIDC).
    Token,
    /// A TLS-verified client certificate (mTLS subject).
    Certificate,
    /// An MQTT 5 enhanced-auth exchange (ADR 0013).
    Enhanced,
}

/// The server-side revocable facts a connection was admitted under (ADR 0040 T1):
/// what a policy-reload sweep re-evaluates against the new policy. Recorded at
/// CONNECT and kept with the online entry — the broker retains *facts about* the
/// admission (subject, method, certificate serial), never replayable credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    /// The authenticated principal (subject + groups); its `subject` is the session
    /// owner (ADR 0031). The full identity is kept so sweep-time authorization checks
    /// see exactly what admission-time checks saw.
    pub identity: mqtt_auth::Identity,
    /// How the connection authenticated.
    pub method: AuthMethod,
    /// The mTLS leaf certificate's serial number (big-endian bytes as encoded in the
    /// certificate) when one was presented at *this* hop; `None` on plaintext, on
    /// no-cert listeners, and for proxied sessions (ADR 0005 — the landing node holds
    /// the actual TLS session and its serial).
    pub cert_serial: Option<Vec<u8>>,
    /// The connection's negotiated MQTT protocol version: an evicted v5 client is
    /// told why (DISCONNECT `0x87`); v3.1.1 has no server DISCONNECT, so it just
    /// gets the close.
    pub protocol: ProtocolVersion,
}

/// The new policy a successful security reload published, handed to the hub for
/// the identity sweep (ADR 0040 T2). Carries `Arc`s to exactly the values the
/// reload swapped into the live `watch` channels, so the sweep and the next
/// admission see the same policy.
pub struct SweepPolicy {
    /// The new authorizer (connect-ACL re-check).
    pub authorizer: Arc<dyn mqtt_auth::Authorizer>,
    /// The new authenticator (password-user existence probe).
    pub authenticator: Arc<dyn mqtt_auth::Authenticator>,
    /// The client-listener CRL's revoked serials (empty when none is configured).
    pub revoked: mqtt_auth::signed_gossip::RevocationList,
    /// The cluster CRL's revoked serials (ADR 0040 T4; empty when none is
    /// configured) — the peer sweep tears down established links these name.
    pub peer_revoked: mqtt_auth::signed_gossip::RevocationList,
    /// What fired the reload (`signal` / `watch`), for the audit trail.
    pub trigger: String,
    /// Audit sink for the per-eviction `security.evict` records.
    pub audit: Arc<dyn mqtt_observability::AuditSink>,
}

impl std::fmt::Debug for SweepPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SweepPolicy")
            .field("revoked", &self.revoked.len())
            .field("trigger", &self.trigger)
            .finish_non_exhaustive()
    }
}

/// The live authorizer handle the hub consults for resume-time grant re-checks
/// (ADR 0040 T3) — the same `watch` channel the connections read, so the hub and
/// the admission path always see the same policy. A newtype so [`HubCommand`]
/// stays `Debug`.
pub struct AuthzWatch(pub tokio::sync::watch::Receiver<Arc<dyn mqtt_auth::Authorizer>>);

impl std::fmt::Debug for AuthzWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthzWatch").finish_non_exhaustive()
    }
}

/// Per-client quota configuration (ADR 0041 T3), set once at startup via
/// [`HubCommand::SetQuotas`]. Unset caps admit everything — today's behavior.
#[derive(Debug, Clone, Copy, Default)]
pub struct Quotas {
    /// The most subscriptions one client may hold; a SUBSCRIBE filter beyond it
    /// is denied `0x97 Quota exceeded` (v5) / `0x80` (v3.1.1) in its SUBACK slot.
    /// Re-subscribing an already-held filter never consumes quota (it replaces).
    pub max_subscriptions_per_client: Option<usize>,
    /// The most retained topics this node stores (ADR 0041 T4). A retained
    /// publish creating a NEW topic beyond it is refused — the cap stops growth,
    /// never maintenance: overwriting or clearing an existing topic always works.
    pub max_retained_messages: Option<usize>,
    /// The most sessions (online + retained-offline) this node holds. A CONNECT
    /// creating a NEW session beyond it is refused (`0x97` v5 / Server
    /// unavailable v3.1.1); resuming an existing session always works.
    pub max_sessions: Option<usize>,
}

/// How the hub disposed of a publish, reported through the ack-gate channel
/// (ADR 0018/0041): the connection releases the publisher's acknowledgement —
/// or answers `0x97 Quota exceeded` — accordingly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// Fanned out (and durably appended where applicable) — ack normally.
    Accepted,
    /// A v5 retained publish would have created a new retained topic beyond the
    /// quota: nothing was delivered or retained — answer `0x97` (ADR 0041 T4).
    RetainedQuotaExceeded,
}

/// A currently-online client connection.
#[derive(Debug)]
struct Online {
    /// Unique per-connection id, used to resolve takeover/disconnect races.
    conn_id: u64,
    /// Channel to this connection's writer.
    tx: Outbound,
    /// Will message published if this connection ends ungracefully.
    will: Option<Message>,
    /// The revocable facts this connection was admitted under (ADR 0040 T1).
    admission: Admission,
    /// When this connection attached: a takeover-window re-delivery
    /// (ADR 0042 T9) skips clients attached BEFORE the publish first fanned
    /// out — they already received it live; re-sending would duplicate.
    attached_at: Instant,
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
    /// Creating this session would exceed the node's session quota (ADR 0041 T4).
    /// The connection rejects the CONNACK (`0x97` v5 / Server unavailable v3.1.1);
    /// resuming an existing session is never refused for quota.
    QuotaExceeded,
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
    /// The revocable facts the connection was admitted under (ADR 0040 T1); its
    /// `subject` is the owner to bind/verify (ADR 0031).
    admission: Admission,
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
        /// The revocable facts the connection was admitted under (ADR 0040 T1). Its
        /// `subject` (mTLS CN / username / token subject, or the shared `"anonymous"`
        /// principal) binds the session to its owner (ADR 0031).
        admission: Admission,
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
        /// When present, the hub answers with one flag per filter — `false` for a
        /// filter the subscription quota denied (ADR 0041 T3) — BEFORE any
        /// retained replay, so the connection's SUBACK precedes the replayed
        /// publishes. `None` skips the quota round-trip (internal callers).
        reply: Option<oneshot::Sender<Vec<bool>>>,
    },
    /// Set the per-client quotas (ADR 0041 T3). Sent once at startup, before any
    /// listener accepts.
    SetQuotas(Quotas),
    /// Enter or leave disk **brownout** (ADR 0041 T5): sent by the store-size
    /// watcher on watermark transitions. Under brownout, growth writes are
    /// refused with the quota behaviors while maintenance continues.
    SetBrownout(bool),
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
        /// Signalled once the on-loop fan-out — including any durable (fsync'd)
        /// offline-queue appends — has completed, so the connection releases a
        /// `QoS` ≥ 1 acknowledgement only for a message the broker durably owns
        /// (ADR 0018). `None` when no acknowledgement is gated on the fan-out.
        done: Option<oneshot::Sender<PublishOutcome>>,
        /// Whether the publisher speaks MQTT 5 (ADR 0041 T4): an over-quota
        /// retained publish is refused outright for v5 (the publisher gets
        /// `0x97`); v3.1.1 has no reason codes, so it is delivered live but not
        /// retained.
        v5: bool,
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
    /// Terminate a client's live session server-side (ADR 0040 T1): the eviction
    /// primitive the policy-reload sweeps drive. A v5 client is told why
    /// (DISCONNECT `0x87` Not authorized) before the close; v3.1.1 has no server
    /// DISCONNECT, so its connection just closes. Ends like any ungraceful
    /// disconnect: the will is published and session retention (ADR 0009)
    /// proceeds normally. Evicting an offline client is a no-op.
    Evict {
        /// The client whose session to terminate.
        client: ClientId,
        /// Why (for the log/audit trail), e.g. `cert-revoked`, `user-removed`.
        reason: String,
    },
    /// A successful security reload published a new policy — sweep the online table
    /// against it (ADR 0040 T2/T3): identity-level revocation terminates sessions;
    /// permission-level tightening removes subscription grants. Sent by the
    /// [`Reloader`](crate::reload::Reloader) after the swap.
    SweepIdentities(SweepPolicy),
    /// Hand the hub the live authorizer handle (ADR 0040 T3), consulted when a
    /// persistent session resumes: restored subscriptions are re-authorized under
    /// the resuming principal's full identity, so an offline session's tightened
    /// grants are revoked at the moment delivery could resume. Sent once at
    /// startup, before any listener accepts.
    AttachAuthorizer(AuthzWatch),

    /// A peer node's link came up; register it and send our interest snapshot.
    PeerConnected {
        /// The remote node.
        node: NodeId,
        /// Unique id for this physical peer link.
        conn_id: u64,
        /// Channel to send messages to that peer.
        tx: PeerOutbound,
        /// The remote leaf certificate's serial from the mTLS handshake
        /// (ADR 0040 T4); `None` on a plaintext mesh.
        cert_serial: Option<Vec<u8>>,
        /// The peer-bus protocol version negotiated on the link (ADR 0038): the
        /// hub sends a frame introduced in proto N only when `proto >= N`.
        proto: u32,
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
    /// A chunk of a peer's retained-message snapshot, back-filling a node on link-up
    /// (ADR 0014 §3, chunked per 0014-T8). Under durable retained each entry carries
    /// its `(epoch, offset)` token and applies only above the held one (ADR 0037 P5)
    /// — divergent caches converge to the committed value; an empty payload is a
    /// committed clear. Durable off keeps gap-fill (topics we do not already retain).
    /// Chunks are independent and idempotent either way.
    RemoteRetainedSnapshot {
        /// The peer the snapshot came from (divergence attribution, ADR 0037 P1).
        node: NodeId,
        /// The wire entries as received; token `(0, 0)` = uncommitted (gap-fill
        /// only). Application properties ride each entry (ADR 0038 T3).
        messages: Vec<RetainedWireEntry>,
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
    /// A retained mutation a peer routed here because this node owns the topic's
    /// placement group (ADR 0037 §1): commit it into the durable retained keyspace
    /// and answer with a commit-gated ack (T8). Live delivery already happened on the
    /// landing node — this is only the authority write.
    RemoteRetainedCommit {
        /// The routing peer (where the ack goes, and the dedup key half).
        node: NodeId,
        /// Destination topic.
        topic: String,
        /// The retained payload; empty = clear (versioned tombstone).
        payload: Bytes,
        /// The publish `QoS` as its 2-bit wire value.
        qos: u8,
        /// The publisher's forwardable application properties (ADR 0038 T3),
        /// committed with the value.
        app: AppProperties,
        /// The sender's handoff sequence (echoed in the ack; dedup key).
        seq: u64,
    },
    /// The owner's commit-gated answer to a handoff this node sent (ADR 0037 T8):
    /// `Some(token)` = committed (drop the held mutation), `None` = the receiver no
    /// longer owns the group (re-queue and re-resolve).
    RemoteRetainedCommitAck {
        /// The peer that answered.
        node: NodeId,
        /// The handoff sequence being answered.
        seq: u64,
        /// The commit token, or `None` for a not-owner NACK.
        token: Option<(u64, u64)>,
    },
    /// **Internal**: this (owner) node's off-loop durable retained commit finished —
    /// posted back to the loop by the spawned commit task, like
    /// [`SessionRecovered`](Self::SessionRecovered). On success the committed value
    /// warms the local cache and fans out to every peer with its token (ADR 0037 §3),
    /// and the queue head advances; on failure the mutation returns to the queue
    /// front and waits for a heal trigger (ADR 0037 §5).
    RetainedCommitDone {
        /// The committed topic.
        topic: String,
        /// The payload the commit was attempted with; empty = clear (tombstone).
        payload: Bytes,
        /// The publish `QoS` as its 2-bit wire value.
        qos: u8,
        /// The application properties the commit carried (ADR 0038 T3) — fanned out
        /// with the value on success, kept with the re-queued mutation on failure.
        app: AppProperties,
        /// `Some((epoch, offset))` on success; `None` = the commit failed and the
        /// mutation is re-queued.
        token: Option<(u64, u64)>,
        /// Set when a peer routed this mutation here (T8): the `(node, seq)` to send
        /// the commit-gated ack back to on success.
        reply: Option<(NodeId, u64)>,
        /// The pending publish gated on this commit (ADR 0042 T9, exhibit ⑦), if
        /// the mutation originated from a gated local publish.
        publish: Option<u64>,
    },
    /// A committed retained value fanned out by its topic's group owner
    /// (ADR 0037 §3): apply it to the local cache iff its `(epoch, offset)` token
    /// exceeds the held one — monotonic per topic, idempotent, order-insensitive.
    RemoteRetainedUpdate {
        /// The committed topic.
        topic: String,
        /// The committed payload; empty = cleared (tombstone).
        payload: Bytes,
        /// The publish `QoS` as its 2-bit wire value.
        qos: u8,
        /// The lease epoch the value committed under (token high half).
        epoch: u64,
        /// The committed log offset (token low half).
        offset: u64,
        /// The committed application properties (ADR 0038 T3).
        app: AppProperties,
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
    /// An **acknowledged** publish forward from a peer (ADR 0042 T9, exhibit ⑤;
    /// proto 3): local delivery only (never re-forwarded), answered with a
    /// durability-gated [`PeerMessage::PublishAck`] once the local fan-out —
    /// including any durable offline enqueue — has completed. Duplicates
    /// (retransmissions) are delivered again: legal at `QoS` 1.
    RemotePublishAcked {
        /// The peer the forward arrived from (where the ack is sent).
        node: NodeId,
        /// The sender's forward sequence (correlates the ack).
        seq: u64,
        /// Destination topic.
        topic: String,
        /// Application payload.
        payload: Bytes,
        /// The original publish `QoS` (local downgrade still applies).
        qos: QoS,
        /// Whether the publish carried the retain flag (same rules as
        /// [`RemotePublish`](Self::RemotePublish)).
        retain: bool,
        /// The publisher's Message Expiry Interval (seconds). `None` = no expiry.
        message_expiry: Option<u32>,
        /// The publisher's forwardable MQTT 5 application properties (ADR 0030).
        app: AppProperties,
    },
    /// A peer's durability-gated answer to a forwarded publish (ADR 0042 T9,
    /// exhibit ⑤): resolves the matching obligation on the pending publish that
    /// forwarded it, releasing the publisher's acknowledgement when it was the
    /// last one outstanding.
    RemotePublishAck {
        /// The peer that answered.
        node: NodeId,
        /// The forward sequence being answered.
        seq: u64,
        /// Whether the peer's local fan-out (durable appends included) succeeded.
        ok: bool,
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
    /// **Internal**: the off-loop inherited-session scan finished (ADR 0042 T9,
    /// exhibit ⑥) — every session the durable store holds, with subscriptions and
    /// expiry deadline. The loop materializes the OWNED, not-yet-known ones into
    /// the routing table so a publish arriving before the client's first re-attach
    /// enqueues instead of routing to nothing.
    InheritedSessions {
        /// `(client, subscriptions, expiry deadline)` per stored session.
        sessions: Vec<(ClientId, Vec<Subscription>, Option<u64>)>,
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
    /// The remote leaf certificate's serial (big-endian bytes) from the link's
    /// mTLS handshake — the fact a cluster-CRL revocation sweep re-checks
    /// (ADR 0040 T4). `None` on a plaintext mesh.
    cert_serial: Option<Vec<u8>>,
    /// The peer-bus protocol version negotiated on the link (ADR 0038).
    proto: u32,
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
    /// The durable retained keyspace (ADR 0037), when durable sessions are on: the
    /// owner-routed, quorum-committed **authority** for retained state, written in
    /// addition to the local cache above. `None` (durable off) keeps ADR 0014
    /// best-effort behaviour unchanged.
    durable_retained: Option<Arc<dyn DurableRetained>>,
    /// The live authorizer for resume-time grant re-checks (ADR 0040 T3); `None`
    /// (no re-check) until [`HubCommand::AttachAuthorizer`] arrives — harnesses
    /// without a reloadable policy keep today's restore-as-persisted behavior.
    authz: Option<AuthzWatch>,
    /// Disk brownout (ADR 0041 T5): set while the stores' on-disk size exceeds
    /// `MQTTD_STORE_MAX_BYTES`. Growth writes (new retained topics, new sessions,
    /// offline enqueues) are refused with the quota behaviors; acks, deletes,
    /// expiry, and resumes continue — read-mostly, not read-only, and never the
    /// disk-full cliff.
    brownout: bool,
    /// Per-client quotas (ADR 0041 T3); default = uncapped.
    quotas: Quotas,
    /// The `(epoch, offset)` convergence token each cached retained topic was applied
    /// at (ADR 0037 §3): a fan-out/back-fill value is applied only when its token
    /// exceeds the held one — monotonic per topic, idempotent, order-insensitive. A
    /// cleared topic keeps its tombstone's token here so a staler value cannot
    /// resurrect it. Only populated under durable retained; bounded by topic count
    /// (like the cache itself).
    retained_tokens: HashMap<String, (u64, u64)>,
    /// Retained mutations awaiting their authority commit (ADR 0037 §5), in arrival
    /// order: every mutation passes through here, so commits are **serialized per
    /// node** (one in flight at a time — two rapid publishes to one topic can never
    /// commit out of order), and one that cannot reach its group owner — partition,
    /// dead owner, no quorum — simply waits for a heal trigger instead of being
    /// dropped. Bounded at [`RETAINED_QUEUE_CAP`]; the bound drops the **oldest**,
    /// loudly (`retained_queue_dropped_total`).
    retained_queue: VecDeque<RetainedMutation>,
    /// Whether an owner-local durable retained commit is currently in flight
    /// (off-loop). The queue head advances only when it completes, preserving
    /// per-node commit order.
    retained_commit_inflight: bool,
    /// The one peer handoff currently awaiting its commit-gated ack (ADR 0037 T8):
    /// `(owner, seq, mutation)`. Held **outside** the queue; the mutation is dropped
    /// only on `Some(token)`, returned to the queue front on NACK or a lost link,
    /// and retransmitted (same `seq`) by the sweep tick while unanswered.
    retained_handoff: Option<(NodeId, u64, RetainedMutation)>,
    /// Per-node monotonic handoff sequence (the retransmission dedup key, T8).
    retained_handoff_seq: u64,
    /// Owner side (T8): the last handoff **committed** per routing peer, as
    /// `(seq, token)` — a retransmission of that seq re-sends the ack without
    /// recommitting. One entry per peer (senders hold one handoff in flight);
    /// cleared when the peer's link drops (a restarted peer restarts its counter —
    /// the worst case is then a benign idempotent re-commit, never a wrong dedup).
    retained_handoff_seen: HashMap<NodeId, (u64, (u64, u64))>,
    /// Owner side (T8): the handoff currently queued/committing per routing peer, so
    /// a retransmission that overtakes the commit is not enqueued twice.
    retained_handoff_pending: HashMap<NodeId, u64>,
    /// Publishes whose `QoS` 1 acknowledgement awaits cluster-wide durability
    /// (ADR 0042 T9): keyed by a monotonic id, ordered so the cap drops the
    /// oldest. Entries resolve via forward acks, the retained commit, and the
    /// local fan-out; the sweep tick retransmits and re-routes.
    pending_publishes: BTreeMap<u64, PendingPublish>,
    /// Monotonic pending-publish id source.
    publish_ids: u64,
    /// Per-node monotonic forward sequence (ADR 0042 T9, exhibit ⑤).
    forward_seq: u64,
    /// Forward seq → pending publish id, for ack resolution.
    forward_index: HashMap<u64, u64>,
    /// Whether an off-loop inherited-session scan is running (ADR 0042 T9,
    /// exhibit ⑥) — one at a time.
    inherited_scan_inflight: bool,
    /// Sweep ticks remaining of eager takeover reconciliation: set on `PeerDead`
    /// so inherited sessions materialize within seconds, not on the slow
    /// [`EXPIRY_RECONCILE_EVERY`] cadence.
    takeover_reconcile_ticks: u8,
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

/// The per-node bound on retained mutations queued awaiting their authority commit
/// (ADR 0037 §5, queue-until-heal). At the bound the **oldest** mutation is dropped,
/// loudly (`retained_queue_dropped_total`) — the explicit CP trade: a partition that
/// outlasts the queue costs the oldest minority-side retained writes, never silent
/// divergence. Count-bounded (like the session queue cap): retained values are
/// last-value device state, typically small and infrequent.
const RETAINED_QUEUE_CAP: usize = 1024;

/// A retained mutation awaiting its authority commit (ADR 0037 §5/T8).
#[derive(Debug, Clone)]
struct RetainedMutation {
    /// Destination topic.
    topic: String,
    /// The retained payload; empty = clear (versioned tombstone).
    payload: Bytes,
    /// The publish `QoS` as its 2-bit wire value.
    qos: u8,
    /// The publisher's forwardable application properties (ADR 0038 T3), committed
    /// into the durable record with the value.
    app: AppProperties,
    /// Set when a peer routed this mutation here (T8): the `(node, seq)` its
    /// commit-gated ack goes back to.
    reply: Option<(NodeId, u64)>,
    /// The pending publish whose acknowledgement is gated on this mutation's
    /// authority commit (ADR 0042 T9, exhibit ⑦). Survives re-queues and rides
    /// the handoff hold, so the gate holds however long the commit takes.
    publish: Option<u64>,
}

/// The bound on publishes whose acknowledgement awaits cluster-wide durability
/// (ADR 0042 T9). Publisher inflight windows (`receive_maximum`) bound this
/// naturally; the cap is a backstop against a partition outlasting every window.
/// At the cap the **oldest** pending publish is dropped loudly — its ack is
/// withheld, so the publisher retries (never an ack for an unowned message).
const PENDING_PUBLISH_CAP: usize = 4096;

/// Sweep ticks a pending publish waits after its forward target **died** with no
/// current remote interest in the topic, before concluding the interest genuinely
/// ended (session gone) rather than being mid-takeover: the dead owner's successor
/// materializes inherited sessions and re-advertises their filters (exhibit ⑥ fix)
/// within this window in any live cluster. Sized to outlast SWIM confirmation plus
/// the successor's inherited-session scan; the cost of the margin is only a slower
/// (withheld) ack for a publish whose subscriber genuinely no longer exists.
const REROUTE_GRACE_TICKS: u8 = 8;

// The bools are independent obligations, not an encodable state machine.
#[allow(clippy::struct_excessive_bools)]
/// A `QoS` 1 publish whose acknowledgement is gated on **cluster-wide** durability
/// (ADR 0042 T9): the local fan-out's durable appends (synchronous), the retained
/// authority commit (exhibit ⑦), and one durability-gated ack per acked peer
/// forward (exhibit ⑤). The ack releases only when every obligation resolves;
/// a terminal failure drops the entry, withholding the ack (the publisher retries).
#[derive(Debug)]
struct PendingPublish {
    /// Releases the publisher's acknowledgement (dropped = withheld).
    done: oneshot::Sender<PublishOutcome>,
    /// The forwarded frame, kept for retransmission and takeover re-routing.
    topic: String,
    payload: Bytes,
    qos: QoS,
    retain: bool,
    message_expiry: Option<u32>,
    app: AppProperties,
    /// Outstanding forward acks: forward seq → target node.
    awaiting: HashMap<u64, NodeId>,
    /// Peers whose durability ack already arrived — a takeover re-route never
    /// re-obligates them.
    acked_nodes: HashSet<NodeId>,
    /// Whether the retained authority commit is still outstanding (exhibit ⑦).
    awaiting_retained: bool,
    /// Set once the on-loop local fan-out (durable appends included) completed OK.
    local_done: bool,
    /// When the publish first fanned out — the cutoff for re-delivery (only
    /// clients attached or materialized AFTER this can have missed it).
    created_at: Instant,
    /// Engaged when a forward target died: counts down sweep ticks with no
    /// re-routable remote interest before the obligation is considered moot
    /// (see [`REROUTE_GRACE_TICKS`]).
    reroute_grace: Option<u8>,
    /// Set when the publish arrived during a takeover window (an inherited-session
    /// scan pending or running): the ack waits until the scan lands, then the
    /// publish re-delivers locally against the just-materialized subscriptions
    /// (exhibit ⑥'s ack-into-the-void window; duplicates are legal at `QoS` 1).
    awaiting_settle: bool,
}

/// The order-independent digest of a retained set (0014-T6 + ADR 0037 P1): the topic
/// count, the XOR of each topic's stable 64-bit hash, and the XOR of each
/// `(topic, payload, qos)` **value** hash. Independent of iteration order and cheap to
/// compare (a collision merely skips a best-effort back-fill / detection). Equal topic
/// hashes with **differing value hashes** mean divergence: same topics, different values.
fn retained_digest<'a>(
    entries: impl Iterator<Item = (&'a str, &'a [u8], u8, Vec<u8>)>,
) -> (u64, u64, u64) {
    let mut count = 0u64;
    let mut hash = 0u64;
    let mut value_hash = 0u64;
    for (topic, payload, qos, props) in entries {
        count += 1;
        hash ^= mqtt_cluster::hrw::stable_id(topic.as_bytes());
        value_hash ^= retained_value_id(topic, payload, qos, &props);
    }
    (count, hash, value_hash)
}

/// A stable 64-bit hash of one retained `(topic, payload, qos, props)` value
/// (ADR 0037 P1). The topic is length-prefixed so `("a", "bc")` and `("ab", "c")`
/// cannot collide; the canonical props encoding (ADR 0038 T3) is folded in so two
/// caches holding the same payload with different application properties still read
/// as divergent and reconcile by token.
fn retained_value_id(topic: &str, payload: &[u8], qos: u8, props: &[u8]) -> u64 {
    let mut bytes = Vec::with_capacity(8 + topic.len() + payload.len() + 1 + props.len());
    bytes.extend_from_slice(&(topic.len() as u64).to_be_bytes());
    bytes.extend_from_slice(topic.as_bytes());
    bytes.extend_from_slice(payload);
    bytes.push(qos);
    bytes.extend_from_slice(props);
    mqtt_cluster::hrw::stable_id(&bytes)
}

/// Split retained entries into chunks whose summed (topic + payload) size stays under
/// [`RETAINED_CHUNK_BYTES`] (0014-T8). A single entry larger than the whole budget is
/// skipped with a warning — it could never fit a frame, and sending it would sever the
/// link instead of just missing one back-fill.
fn chunk_retained(entries: impl Iterator<Item = RetainedWireEntry>) -> Vec<Vec<RetainedWireEntry>> {
    // Fixed per-entry overhead estimate for bincode length prefixes, the QoS byte,
    // and the two u64 token halves (ADR 0037 P5); the variable-length application
    // properties (ADR 0038 T3) are sized per entry.
    const ENTRY_OVERHEAD: usize = 48;
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for entry in entries {
        let size =
            entry.topic.len() + entry.payload.len() + entry.props.size_hint() + ENTRY_OVERHEAD;
        if size > RETAINED_CHUNK_BYTES {
            warn!(
                topic = %entry.topic,
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
        current.push(entry);
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
                durable_retained: None,
                authz: None,
                brownout: false,
                quotas: Quotas::default(),
                retained_tokens: HashMap::new(),
                retained_queue: VecDeque::new(),
                retained_commit_inflight: false,
                retained_handoff: None,
                retained_handoff_seq: 0,
                retained_handoff_seen: HashMap::new(),
                retained_handoff_pending: HashMap::new(),
                pending_publishes: BTreeMap::new(),
                publish_ids: 0,
                forward_seq: 0,
                forward_index: HashMap::new(),
                inherited_scan_inflight: false,
                // A boot window (like the post-PeerDead takeover window): a
                // restarted or newly-joined node may already own groups with
                // orphaned sessions, and eagerly recovering them BEFORE workload
                // arrives keeps the first-touch epoch bumps (which transiently
                // break quorum for concurrent appends) out of the hot path.
                takeover_reconcile_ticks: 8,
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

    /// Attach the durable retained keyspace before [`run`](Self::run) (ADR 0037): every
    /// locally-originated retained mutation is then also routed to its topic's group
    /// lease-owner and quorum-committed. Only set when durable sessions are on; left
    /// unset, retained keeps the ADR 0014 best-effort behaviour unchanged.
    pub fn attach_durable_retained(&mut self, retained: Arc<dyn DurableRetained>) {
        self.durable_retained = Some(retained);
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
        // The boot window's FIRST inherited-session scan runs immediately, not a
        // sweep tick later: on a fresh or restarted node it completes in
        // milliseconds and releases any publish acks gated on it (ADR 0042 T9).
        self.spawn_inherited_session_scan();
        loop {
            tokio::select! {
                cmd = self.rx.recv() => match cmd {
                    Some(cmd) => self.dispatch(cmd).await,
                    None => break,
                },
                _ = sweep.tick() => {
                    self.sweep_expired_sessions().await;
                    self.refresh_gauges().await;
                    // Retransmit an unanswered retained handoff (T8 — same seq, the
                    // owner dedups), then retry queued retained mutations (ADR 0037
                    // §5): covers heals with no link event — a lease landing locally,
                    // or quorum returning on links that never dropped. No-ops when idle.
                    self.retry_retained_handoff();
                    self.kick_retained_queue();
                    // Retransmit / re-route acked publish forwards (ADR 0042 T9,
                    // exhibit ⑤); no-op when none are pending.
                    self.sweep_pending_forwards().await;
                }
            }
        }
    }

    /// Dispatch one command to its handler.
    // One arm per command; a flat dispatch table, not a refactor smell.
    #[allow(clippy::too_many_lines)]
    async fn dispatch(&mut self, cmd: HubCommand) {
        match cmd {
            HubCommand::Attach {
                client,
                admission,
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
                        admission,
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
            HubCommand::Subscribe {
                client,
                filters,
                reply,
            } => {
                self.subscribe(&client, filters, reply).await;
            }
            HubCommand::SetQuotas(quotas) => {
                self.quotas = quotas;
            }
            HubCommand::SetBrownout(on) => {
                if on != self.brownout {
                    if on {
                        warn!(
                            "disk watermark exceeded: BROWNOUT — growth writes refused (ADR 0041)"
                        );
                    } else {
                        info!("disk usage back under the watermark: brownout lifted (ADR 0041)");
                    }
                }
                self.brownout = on;
            }
            HubCommand::Unsubscribe { client, filters } => {
                self.unsubscribe(&client, &filters).await;
            }
            HubCommand::Publish {
                topic,
                payload,
                qos,
                mut retain,
                message_expiry,
                app,
                done,
                v5,
            } => {
                if let Some(m) = &self.metrics {
                    m.publish_received(qos_num(qos));
                }
                // Retained quota (ADR 0041 T4): a retained publish that would CREATE
                // a new topic beyond the cap. Growth is refused; overwrite and clear
                // (empty payload) always work. v5: refuse outright (the publisher is
                // told 0x97); v3.1.1 has no reason codes: deliver live, retain nothing.
                if retain && !payload.is_empty() && self.retained_quota_exceeded(&topic).await {
                    if let Some(m) = &self.metrics {
                        m.quota_rejected("retained");
                    }
                    if v5 {
                        warn!(topic = %topic, "retained quota exceeded; publish refused 0x97 (ADR 0041)");
                        if let Some(done) = done {
                            let _ = done.send(PublishOutcome::RetainedQuotaExceeded);
                        }
                        return;
                    }
                    warn!(topic = %topic,
                          "retained quota exceeded; delivered live, NOT retained (v3.1.1, ADR 0041)");
                    retain = false;
                }
                // A gated publish registers a pending entry FIRST (ADR 0042 T9), so
                // the fan-out can attach its cluster-wide obligations: acked peer
                // forwards (exhibit ⑤) and the retained authority commit (exhibit ⑦).
                let gate = done.map(|done| {
                    self.register_pending(done, &topic, &payload, qos, retain, message_expiry, &app)
                });
                // Time the synchronous on-loop fan-out (local deliver + offline enqueue
                // + peer forward) as the hub's per-publish delivery latency (ADR 0020-T4).
                let started = Instant::now();
                let durable_ok = self
                    .publish(&topic, &payload, qos, retain, message_expiry, &app, gate)
                    .await;
                if let Some(m) = &self.metrics {
                    m.observe_deliver_latency(started.elapsed().as_secs_f64());
                }
                // The LOCAL fan-out (and its durable appends) is complete: resolve
                // that obligation — the ack releases once every cluster-wide
                // obligation has (ADR 0018 + ADR 0042 T9; the local-only publish
                // completes right here). A failed durable append WITHHOLDS the ack
                // instead (drop the entry): the publisher's connection closes
                // unacked and it retries — fail closed, never an ack for a message
                // a subscriber will never see (ADR 0041 T5).
                if let Some(id) = gate {
                    if durable_ok {
                        self.pending_local_done(id);
                    } else {
                        self.drop_pending(id);
                    }
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
            HubCommand::Evict { client, reason } => {
                self.evict(&client, &reason).await;
            }
            HubCommand::SweepIdentities(policy) => {
                let identities = self.sweep_identities(&policy).await;
                let grants = self.sweep_grants(&policy).await;
                let peers = self.sweep_peers(&policy);
                // One summary record per sweep (ADR 0040 T5), zeros included — the
                // proof the sweep ran is as valuable as what it did.
                policy.audit.record(
                    "security.sweep",
                    None,
                    &format!(
                        "identities={identities} grants={grants} peers={peers}                          (trigger={})",
                        policy.trigger
                    ),
                );
            }
            HubCommand::AttachAuthorizer(watch) => {
                self.authz = Some(watch);
            }
            // Peer- and cluster-facing commands.
            other => self.dispatch_cluster(other).await,
        }
    }

    /// Dispatch a peer-/cluster-facing command (forwarded publishes, peer link
    /// (de)registration, gossiped interest, durable frames). Split from
    /// [`dispatch`](Self::dispatch) to keep each handler focused.
    // One arm per cluster command — a flat dispatch table, not a refactor smell.
    #[allow(clippy::too_many_lines)]
    async fn dispatch_cluster(&mut self, cmd: HubCommand) {
        match cmd {
            HubCommand::RemotePublishAcked {
                node,
                seq,
                topic,
                payload,
                qos,
                retain,
                message_expiry,
                app,
            } => {
                // An acked forward (ADR 0042 T9, exhibit ⑤): apply locally like
                // RemotePublish, then answer with a durability-gated ack — sent only
                // after the local fan-out, durable offline enqueues included. A
                // retransmission is delivered again (duplicates are legal at QoS 1),
                // so no receiver dedup state is needed.
                let ok = self
                    .deliver(&topic, &payload, qos, retain, message_expiry, &app)
                    .await;
                if let Some(peer) = self.peers.get(&node) {
                    let _ = peer.tx.send(PeerMessage::PublishAck { seq, ok });
                }
            }
            HubCommand::RemotePublishAck { node, seq, ok } => {
                self.forward_acked(&node, seq, ok);
            }
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
            HubCommand::PeerConnected {
                node,
                conn_id,
                tx,
                cert_serial,
                proto,
            } => {
                self.peer_connected(node.clone(), conn_id, tx, cert_serial, proto);
                // Offer the new peer our retained topic-set digest (ADR 0014 §3,
                // 0014-T6): it pulls the (chunked) snapshot only if the sets differ,
                // so a steady-state link-up or flap costs one small frame, not the
                // whole retained set.
                self.send_retained_digest(&node).await;
                // A heal trigger (ADR 0037 §5): the new link may be — or reach — the
                // owner that queued retained mutations have been waiting for.
                self.kick_retained_queue();
            }
            HubCommand::PeerDisconnected { node, conn_id } => {
                self.peer_disconnected(&node, conn_id);
            }
            HubCommand::PeerDead { node } => {
                self.peer_dead(&node);
                // The takeover window (ADR 0042 T9, exhibit ⑥): reconcile inherited
                // sessions eagerly for the next several sweep ticks so their
                // subscriptions materialize within seconds of the owner's death.
                self.takeover_reconcile_ticks = 8;
            }
            HubCommand::DurableFrame { node, frame } => {
                self.handle_durable_frame(&node, frame);
            }
            HubCommand::InheritedSessions { sessions } => {
                self.inherit_sessions(sessions).await;
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
            HubCommand::RemoteRetainedCommit {
                node,
                topic,
                payload,
                qos,
                app,
                seq,
            } => {
                // A peer routed a retained mutation here because this node owns the
                // topic's group (ADR 0037 §1): dedup retransmissions, then run it
                // through the same queue as local mutations — serialized commit
                // order, retry-until-heal, and a NACK back if the lease moved (T8).
                self.accept_routed_retained(node, topic, payload, qos, app, seq);
            }
            HubCommand::RemoteRetainedCommitAck { node, seq, token } => {
                // The commit-gated answer to our in-flight handoff (T8). A stale or
                // foreign ack (link flap re-delivery, a dropped-at-cap entry) is
                // ignored: it must match exactly what we are holding.
                let Some((owner, held_seq, mutation)) = self.retained_handoff.take() else {
                    return;
                };
                if owner != node || held_seq != seq {
                    self.retained_handoff = Some((owner, held_seq, mutation));
                    return;
                }
                if token.is_some() {
                    // Committed by the owner (its fan-out warms the caches): the
                    // mutation is finally done — resolve the gated publish riding
                    // it (ADR 0042 T9, exhibit ⑦) and drive the next one.
                    if let Some(id) = mutation.publish {
                        self.pending_retained_done(id);
                    }
                    self.kick_retained_queue();
                } else {
                    // NACK: the owner's lease moved. Re-queue at the front and wait
                    // for the next trigger — placement catches up within a gossip
                    // round, and kicking immediately would hot-loop against the
                    // same stale owner.
                    self.retained_queue.push_front(mutation);
                }
            }
            HubCommand::RetainedCommitDone {
                topic,
                payload,
                qos,
                app,
                token,
                reply,
                publish,
            } => {
                self.retained_commit_inflight = false;
                if let Some((epoch, offset)) = token {
                    // The authority commit landed: resolve the gated publish riding
                    // this mutation (ADR 0042 T9, exhibit ⑦).
                    if let Some(id) = publish {
                        self.pending_retained_done(id);
                    }
                    // Committed: warm the local cache, fan the tokened value out to
                    // every peer (ADR 0037 §3 — best-effort; a peer that misses it
                    // converges via the P5 back-fill on the next link-up), and drive
                    // the next queued mutation. Application properties travel with
                    // the value everywhere (ADR 0038 T3).
                    self.apply_retained_update(&topic, &payload, qos, &app, (epoch, offset))
                        .await;
                    for peer in self.peers.values() {
                        let _ = peer.tx.send(PeerMessage::RetainedUpdate {
                            topic: topic.clone(),
                            payload: payload.to_vec(),
                            qos,
                            epoch,
                            offset,
                            props: app_to_wire(&app),
                        });
                    }
                    // A peer-routed mutation gets its commit-gated ack (T8); the
                    // committed (seq, token) is recorded so a retransmission whose
                    // ack was lost is re-acked without recommitting.
                    if let Some((node, seq)) = reply {
                        self.retained_handoff_seen
                            .insert(node.clone(), (seq, (epoch, offset)));
                        if self.retained_handoff_pending.get(&node) == Some(&seq) {
                            self.retained_handoff_pending.remove(&node);
                        }
                        self.send_retained_ack(&node, seq, Some((epoch, offset)));
                    }
                    self.kick_retained_queue();
                } else {
                    // Failed (no quorum / lease moved): back to the queue FRONT —
                    // order kept, reply tag kept (the ack flows once it commits) —
                    // and wait for a heal trigger rather than hot-retrying. The
                    // front slot may transiently exceed the cap by one (the entry
                    // was already counted when first admitted).
                    self.retained_queue.push_front(RetainedMutation {
                        topic,
                        payload,
                        qos,
                        app,
                        reply,
                        publish,
                    });
                }
            }
            HubCommand::RemoteRetainedUpdate {
                topic,
                payload,
                qos,
                epoch,
                offset,
                app,
            } => {
                self.apply_retained_update(&topic, &payload, qos, &app, (epoch, offset))
                    .await;
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
    /// Returns `false` when a durable offline enqueue failed — the dispatch then
    /// withholds the publisher's ack (ADR 0041 T5).
    #[allow(clippy::too_many_arguments)]
    async fn publish(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
        gate: Option<u64>,
    ) -> bool {
        let durable_ok = self
            .deliver(topic, payload, qos, retain, message_expiry, app)
            .await;
        // Shared subscriptions are selected once cluster-wide by the originating
        // node (ADR 0015), so this runs only for locally-originated publishes.
        self.deliver_shared(topic, payload, qos, message_expiry, app)
            .await;
        self.forward_to_peers(topic, payload, qos, retain, message_expiry, app, gate);
        // Durable retained (ADR 0037): after the live fan-out — which stays undelayed —
        // route the retained mutation to its topic's group lease-owner for the
        // quorum-committed authority write. Only the **landing** node routes (a
        // forwarded publish enters via `RemotePublish` → `deliver`, never here), so one
        // publish is exactly one authority commit. The gated publish's ack now waits
        // for this commit too (ADR 0042 T9, exhibit ⑦).
        if retain {
            self.route_retained_commit(topic, payload, qos_num(qos), app, gate);
        }
        durable_ok
    }

    /// Publish a client's Will message (on takeover or an ungraceful end). Carries the
    /// will's own application properties (ADR 0030); a will never sets a message-expiry.
    async fn publish_will(&mut self, w: &Message) {
        // No publisher waits on a will, so nothing gates on its durability.
        self.publish(&w.topic, &w.payload, w.qos, w.retain, None, &w.app, None)
            .await;
    }

    /// Apply a message on this node: store/clear retained state and deliver to local
    /// ordinary subscribers. Does **not** forward or run shared selection — used both
    /// for local publishes (via
    /// [`publish`](Self::publish)) and for publishes received from a peer, which must
    /// never be re-forwarded.
    /// Returns `false` when a durable offline enqueue failed (ADR 0041 T5).
    async fn deliver(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) -> bool {
        // Under durable retained (ADR 0037 §3) the cache is warmed exclusively by the
        // owner's post-commit, token-carrying fan-out — applying the raw (uncommitted,
        // untokened) flag here is exactly the everyday-race divergence the ADR removes.
        if retain && self.durable_retained.is_none() {
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
            .await
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
                    claimant = %pending.admission.identity.subject,
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
            // The owner (admission.subject) was bound/verified during recovery
            // (claim_session); the facts are kept with the online entry for the
            // reload sweep (ADR 0040).
            admission,
            conn_id,
            session_expiry,
            receive_maximum,
            will,
            outbound,
            reply,
        } = pending;

        // Session quota (ADR 0041 T4): refuse only a NEW session — a resume
        // (session_present) or an attach for a locally-known client id (takeover,
        // clean-start replacement) is never refused for quota. A full broker keeps
        // serving its existing fleet and refuses only strangers.
        if !session_present
            && !self.online.contains_key(&client)
            && !self.session_expiry.contains_key(&client)
        {
            let over_cap = self
                .quotas
                .max_sessions
                .is_some_and(|cap| self.session_count() >= cap);
            if over_cap || self.brownout {
                warn!(client = %client.0, brownout = self.brownout,
                      "session quota/brownout: new-session CONNECT refused (ADR 0041)");
                if let Some(m) = &self.metrics {
                    m.quota_rejected(if self.brownout {
                        "brownout"
                    } else {
                        "sessions"
                    });
                }
                // Recovery already ran claim_session, which CREATED this
                // stranger's durable record before we could refuse it. A
                // refused grant must not leave that growth behind (the whole
                // point of the cap/brownout), so reap the just-created empty
                // record off-loop; the refusal reply is gated on the reap so
                // the client cannot observe the refusal and reconnect into a
                // half-removed session.
                let store = self.store.clone();
                tokio::spawn(async move {
                    let _ = store.remove(&client).await;
                    let _ = reply.send(AttachOutcome::QuotaExceeded);
                });
                return;
            }
        }

        // Revocation reaches resumed grants (ADR 0040 T3): re-authorize each restored
        // subscription against the CURRENT policy, under the resuming principal's
        // full identity (fresh from authentication — groups included). A persistent
        // session that slept through a tightening reload has its revoked grants
        // removed at the moment delivery could resume; queued messages that only a
        // revoked grant admits are dropped below. No authorizer attached = no
        // re-check (harnesses without a reloadable policy).
        let (subscriptions, revoked_grants): (Vec<Subscription>, Vec<Subscription>) =
            match &self.authz {
                Some(rx) => {
                    let authorizer = rx.0.borrow().clone();
                    subscriptions.into_iter().partition(|s| {
                        authorizer.authorize_subscribe(&admission.identity, &s.filter)
                    })
                }
                None => (subscriptions, Vec::new()),
            };
        let revoked_grants: Vec<String> = revoked_grants.into_iter().map(|s| s.filter).collect();

        // Reconcile the routing table with persisted subscriptions (idempotent; empty
        // for a clean start).
        let recovered_any = !subscriptions.is_empty();
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
        // A resumed session registers filters WITHOUT a SUBSCRIBE, so peers must
        // learn them here (ADR 0042 T9): after a takeover, this advertisement is
        // what re-targets a peer's held acked forward to this node — the client
        // re-attaching is one of the two ways an inherited session materializes
        // (the other, the takeover scan, skips clients that are already attaching).
        if recovered_any {
            self.gossip_interest();
        }
        if !revoked_grants.is_empty() {
            warn!(
                client = %client.0,
                filters = ?revoked_grants,
                "resume: tightened ACL revokes persisted subscriptions (ADR 0040 T3)"
            );
            // A live routing table may still carry the offline session's revoked
            // grants (that is how offline queueing works) — remove them there too,
            // and persist the pruned set so the revocation is durable. AFTER the
            // reconcile above, so the persisted result is exactly the surviving set
            // (a fresh hub's empty maps would otherwise persist an empty set).
            self.unsubscribe(&client, &revoked_grants).await;
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
                admission,
                attached_at: Instant::now(),
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
                    // A queued message that only a revoked grant admits is dropped
                    // (ADR 0040 T3): delivering it would leak data the new policy
                    // denies. A topic a surviving grant also matches still replays.
                    if !revoked_grants.is_empty() {
                        let topic = &qm.message.topic;
                        let admits = |f: &String| {
                            let f = parse_shared(f).map_or(f.as_str(), |(_, inner)| inner);
                            topic_matches(f, topic)
                        };
                        let survives = self
                            .subs_by_client
                            .get(&client)
                            .is_some_and(|m| m.keys().any(admits));
                        if revoked_grants.iter().any(admits) && !survives {
                            debug!(client = %client.0, offset = qm.offset, %topic,
                                   "dropping queued message for a revoked grant (ADR 0040 T3)");
                            continue;
                        }
                    }
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

    async fn subscribe(
        &mut self,
        client: &ClientId,
        filters: Vec<(String, QoS)>,
        reply: Option<oneshot::Sender<Vec<bool>>>,
    ) {
        // Subscription quota (ADR 0041 T3): count how many NEW filters the cap
        // admits — an already-held filter replaces (never consumes quota). The
        // SUBACK itself is answered only after the durable persist below
        // (ADR 0042 T9): granted must mean durably granted.
        let filters: (Vec<(String, QoS)>, Vec<bool>) = {
            let held = self.subs_by_client.get(client);
            let mut admitted_new = 0;
            let verdicts: Vec<bool> = filters
                .iter()
                .map(|(f, _)| {
                    let replaces = held.is_some_and(|m| m.contains_key(f));
                    let admit = replaces
                        || match self.quotas.max_subscriptions_per_client {
                            None => true,
                            Some(cap) => held.map_or(0, HashMap::len) + admitted_new < cap,
                        };
                    if admit && !replaces {
                        admitted_new += 1;
                    }
                    admit
                })
                .collect();
            let denied = verdicts.iter().filter(|v| !**v).count();
            if denied > 0 {
                warn!(client = %client.0, denied,
                      "subscription quota exceeded; denied in SUBACK (ADR 0041)");
                if let Some(m) = &self.metrics {
                    for _ in 0..denied {
                        m.quota_rejected("subscriptions");
                    }
                }
            }
            let admitted = filters
                .into_iter()
                .zip(verdicts.iter())
                .filter_map(|(fq, ok)| ok.then_some(fq))
                .collect();
            (admitted, verdicts)
        };
        let (filters, verdicts) = filters;
        // Snapshot for rollback: a failed durable persist must leave the routing
        // state exactly as before, so the failure SUBACK tells the truth.
        let prior = self.subs_by_client.get(client).cloned();

        // Retained messages are replayed only for ordinary subscriptions; a new
        // shared subscription does not receive them (ADR 0010 §3, [MQTT-3.8.4]).
        let mut retained_replay: Vec<Message> = Vec::new();
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
                    retained_replay.push(Message {
                        qos: min_qos(m.qos, *q),
                        retain: true,
                        ..m
                    });
                }
            }
        }
        // The SUBACK is DURABILITY-GATED (ADR 0042 T9, exhibit ⑨): a persistent
        // session's subscription is a promise about future messages, so granting
        // it while the durable write failed builds every downstream durability
        // guarantee on sand — the owner's enqueue, the takeover materialization,
        // and the resume replay would all consult a durable record that says
        // "no subscriptions". Fail closed: roll the routing state back and
        // report failure codes; the client retries its SUBSCRIBE.
        if !self.persist_subscriptions(client).await {
            warn!(
                client = %client.0,
                "durable subscription write failed; SUBACK reports failure (fail closed, ADR 0042 T9)"
            );
            self.drop_subscriptions(client);
            if let Some(prior) = prior {
                for (f, q) in &prior {
                    if let Some((group, filter)) = parse_shared(f) {
                        self.shared.subscribe(client.clone(), group, filter, *q);
                    } else {
                        self.table.subscribe(client.clone(), f.clone());
                    }
                }
                self.subs_by_client.insert(client.clone(), prior);
            }
            if let Some(tx) = reply {
                let _ = tx.send(vec![false; verdicts.len()]);
            }
            return;
        }
        if let Some(tx) = reply {
            let _ = tx.send(verdicts);
        }
        self.gossip_interest();

        if let Some(tx) = self.online.get(client).map(|s| s.tx.clone()) {
            for m in retained_replay {
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
        // An UNSUBACK has no failure codes (v3.1.1); a failed durable removal
        // leaves the subscription durably present — the safe side (no loss,
        // possible extra deliveries until a later persist succeeds).
        let _ = self.persist_subscriptions(client).await;
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
    /// Returns `false` when any recipient's durable enqueue failed (ADR 0041 T5).
    async fn deliver_local(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) -> bool {
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
        let mut all_durable = true;
        for (c, granted) in targets {
            all_durable &= self
                .deliver_to_client(
                    &c,
                    topic,
                    payload,
                    min_qos(qos, granted),
                    message_expiry,
                    app,
                )
                .await;
        }
        all_durable
    }

    /// Deliver one message to a single named recipient: live if online (tracking
    /// `QoS` > 0 in flight), else queued if the session is persistent, else dropped.
    /// The unit of both ordinary and shared (ADR 0015) delivery; `qos` is the
    /// already-downgraded delivery `QoS`.
    /// Returns `false` when a durable offline enqueue failed terminally — the
    /// caller withholds the publisher's ack so it retries (ADR 0041 T5).
    async fn deliver_to_client(
        &mut self,
        client: &ClientId,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) -> bool {
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
            return true;
        }
        if self.is_persistent(client) {
            // Disk brownout (ADR 0041 T5): an offline enqueue GROWS the store —
            // refused above the watermark, counted, like a queue overflow.
            if self.brownout {
                warn!(client = %client.0, topic = %topic,
                      "brownout: offline enqueue refused (ADR 0041)");
                if let Some(m) = &self.metrics {
                    m.publish_dropped("brownout");
                }
                return true;
            }
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
                    warn!(client = %client.0, error = %e,
                          "failed to enqueue offline message; withholding the publisher's ack (ADR 0041 T5)");
                    // Fail closed like the local ack path (ADR 0018): the caller
                    // withholds the publisher's acknowledgement so it retries,
                    // instead of acking a message a subscriber will never see.
                    return false;
                }
            }
        }
        true
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

    /// Whether storing a retained value for `topic` would GROW the retained set
    /// beyond the quota (ADR 0041 T4). Overwrites (topic already retained) never
    /// count. Enforced against this node's local retained view.
    async fn retained_quota_exceeded(&self, topic: &str) -> bool {
        let over = if self.brownout {
            true // brownout (ADR 0041 T5): any retained GROWTH is refused
        } else if let Some(cap) = self.quotas.max_retained_messages {
            self.retained.count().await.unwrap_or(0) >= cap
        } else {
            false
        };
        if !over {
            return false;
        }
        // At the bound: only an overwrite of an existing topic may proceed.
        !self
            .retained
            .matching(topic)
            .await
            .is_ok_and(|m| m.iter().any(|r| r.topic == topic))
    }

    /// The node's session count for the quota (ADR 0041 T4): every online session
    /// plus retained-offline ones (the expiry map covers persistent sessions,
    /// online or not; the union avoids double-counting).
    fn session_count(&self) -> usize {
        self.online
            .keys()
            .filter(|c| !self.session_expiry.contains_key(*c))
            .count()
            + self.session_expiry.len()
    }

    /// The identity sweep (ADR 0040 T2): re-evaluate every online connection's
    /// admission facts against a freshly-reloaded policy and evict the sessions
    /// whose *identity* was revoked — presented certificate now on the CRL,
    /// password user gone from the credential store, or principal denied by the
    /// new connect-ACL. Permission-level changes are NOT swept here (the grant
    /// sweep, T3, handles subscriptions; publish checks are already per-operation).
    /// An unchanged policy evicts no one — every check re-derives the admission
    /// verdict, so only differences act.
    async fn sweep_identities(&mut self, policy: &SweepPolicy) -> usize {
        let victims: Vec<(ClientId, &'static str)> = self
            .online
            .iter()
            .filter_map(|(client, online)| {
                let a = &online.admission;
                if let Some(serial) = &a.cert_serial {
                    if policy.revoked.contains(serial) {
                        return Some((client.clone(), "cert-revoked"));
                    }
                }
                if a.method == AuthMethod::Password
                    && !policy
                        .authenticator
                        .password_subject_exists(&a.identity.subject)
                {
                    return Some((client.clone(), "user-removed"));
                }
                if !policy.authorizer.authorize_connect(&a.identity, client) {
                    return Some((client.clone(), "connect-denied"));
                }
                None
            })
            .collect();
        if victims.is_empty() {
            return 0;
        }
        info!(
            evictions = victims.len(),
            trigger = %policy.trigger,
            "identity sweep: policy reload revoked live sessions (ADR 0040)"
        );
        let evicted = victims.len();
        for (client, reason) in victims {
            policy.audit.record(
                "security.evict",
                Some(&client.0),
                &format!("{reason} (trigger={})", policy.trigger),
            );
            if let Some(m) = &self.metrics {
                m.revocation_eviction(reason);
            }
            self.evict(&client, reason).await;
        }
        evicted
    }

    /// The grant sweep (ADR 0040 T3): re-authorize every surviving online session's
    /// subscription grants against the freshly-reloaded ACL — under the identity the
    /// session was admitted with — and remove the grants the new policy denies, from
    /// live routing and the durable subscription set alike. The client is NOT
    /// disconnected: who it is remains valid, only what it may read shrank. Its next
    /// SUBSCRIBE re-attempt is denied at the admission-path check like any new
    /// operation. Offline sessions are re-checked at resume (see
    /// [`finish_attach`](Self::finish_attach)), where the resuming principal's full
    /// identity is available.
    async fn sweep_grants(&mut self, policy: &SweepPolicy) -> usize {
        // The same raw filter string the SUBSCRIBE-time check authorized (including
        // any `$share/` prefix), so sweep-time and admission-time verdicts align.
        let revocations: Vec<(ClientId, Vec<String>)> = self
            .online
            .iter()
            .filter_map(|(client, online)| {
                let identity = &online.admission.identity;
                let revoked: Vec<String> = self
                    .subs_by_client
                    .get(client)?
                    .keys()
                    .filter(|f| !policy.authorizer.authorize_subscribe(identity, f))
                    .cloned()
                    .collect();
                (!revoked.is_empty()).then(|| (client.clone(), revoked))
            })
            .collect();
        let mut revoked_grants = 0;
        for (client, filters) in revocations {
            warn!(
                client = %client.0,
                filters = ?filters,
                trigger = %policy.trigger,
                "grant sweep: tightened ACL revokes live subscriptions (ADR 0040)"
            );
            policy.audit.record(
                "security.evict",
                Some(&client.0),
                &format!("grant-revoked {filters:?} (trigger={})", policy.trigger),
            );
            if let Some(m) = &self.metrics {
                m.revocation_eviction("grant-revoked");
            }
            revoked_grants += filters.len();
            self.unsubscribe(&client, &filters).await;
        }
        revoked_grants
    }

    /// The peer sweep (ADR 0040 T4): tear down established peer links whose remote
    /// certificate the freshly-reloaded cluster CRL revokes. Removing the entry
    /// drops the link's outbound sender, which ends its pump task and closes the
    /// socket; the mesh reacts as to any link loss (SWIM — already refusing the
    /// node's datagrams per ADR 0022 T7 — marks it dead; placement and leases
    /// move), and the revoked node cannot re-handshake (both handshake sides gate
    /// on the same live CRL slot).
    fn sweep_peers(&mut self, policy: &SweepPolicy) -> usize {
        let victims: Vec<NodeId> = self
            .peers
            .iter()
            .filter(|(_, peer)| {
                peer.cert_serial
                    .as_ref()
                    .is_some_and(|serial| policy.peer_revoked.contains(serial))
            })
            .map(|(node, _)| node.clone())
            .collect();
        let torn_down = victims.len();
        for node in victims {
            warn!(
                peer = %node.0,
                trigger = %policy.trigger,
                "peer sweep: cluster CRL revokes an established link (ADR 0040)"
            );
            policy.audit.record(
                "security.evict",
                Some(&node.0),
                &format!("peer-revoked (trigger={})", policy.trigger),
            );
            if let Some(m) = &self.metrics {
                m.revocation_eviction("peer-revoked");
            }
            let conn_id = self.peers[&node].conn_id;
            self.peer_disconnected(&node, conn_id);
        }
        torn_down
    }

    /// Terminate a client's live session server-side (ADR 0040 T1). See
    /// [`HubCommand::Evict`]. Routes through [`detach`](Self::detach) so session
    /// retention, the will, and backlog spill behave exactly as for any other
    /// ungraceful end — the DISCONNECT (v5 only) is queued first and drains to the
    /// wire before the dropped outbound closes the writer.
    async fn evict(&mut self, client: &ClientId, reason: &str) {
        let Some(online) = self.online.get(client) else {
            return;
        };
        warn!(client = %client.0, reason, "evicting live session");
        if online.admission.protocol == ProtocolVersion::V5 {
            let _ = online.tx.send(Packet::Disconnect(Disconnect {
                reason: mqtt_codec::reason::NOT_AUTHORIZED,
                properties: mqtt_codec::Properties::new(),
            }));
        }
        let conn_id = online.conn_id;
        self.detach(client, conn_id, false).await;
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
        // Periodically inherit sessions this node did not see disconnect — those handed
        // to it by a takeover: their persisted expiry deadlines (ADR 0009 §3) AND their
        // routing subscriptions (ADR 0042 T9, exhibit ⑥). Eagerly for a few ticks after
        // a peer death (the takeover window), else on the slow reconcile cadence.
        self.expiry_reconcile_tick = self.expiry_reconcile_tick.wrapping_add(1);
        if self.takeover_reconcile_ticks > 0 {
            self.takeover_reconcile_ticks -= 1;
            self.spawn_inherited_session_scan();
        } else if self.expiry_reconcile_tick % EXPIRY_RECONCILE_EVERY == 0 {
            self.spawn_inherited_session_scan();
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

    /// Start the off-loop inherited-session scan (ADR 0042 T9, exhibit ⑥): enumerate
    /// every stored session with its subscriptions and expiry deadline, and post the
    /// result back to the loop as [`HubCommand::InheritedSessions`]. Off-loop because
    /// the enumeration reads through the durable store and may trigger first-touch
    /// group recovery (quorum reads) — exactly the eager recovery a takeover wants,
    /// but never on the actor loop. One scan at a time.
    fn spawn_inherited_session_scan(&mut self) {
        if self.inherited_scan_inflight {
            return;
        }
        self.inherited_scan_inflight = true;
        let store = self.store.clone();
        let tx = self.self_tx.clone();
        debug!("inherited-session scan started");
        tokio::spawn(async move {
            let sessions = match store.all_sessions().await {
                Ok(v) => v,
                Err(e) => {
                    debug!(error = %e, "inherited-session scan failed; retried next tick");
                    Vec::new()
                }
            };
            debug!(sessions = sessions.len(), "inherited-session scan finished");
            let _ = tx.send(HubCommand::InheritedSessions { sessions });
        });
    }

    /// Materialize sessions a takeover handed this node, before their clients
    /// re-attach (ADR 0042 T9, exhibit ⑥): register each OWNED, not-yet-known
    /// session's subscriptions into the routing table (so a publish arriving now
    /// enqueues durably instead of routing to nothing), mark it persistent, and
    /// schedule its inherited absolute expiry deadline (ADR 0009 §3 — without
    /// which an orphaned session would never expire on the new owner). A later
    /// real attach takes over cleanly: registration is idempotent and
    /// `finish_attach` overwrites the placeholder expiry interval.
    async fn inherit_sessions(
        &mut self,
        sessions: Vec<(ClientId, Vec<Subscription>, Option<u64>)>,
    ) {
        self.inherited_scan_inflight = false;
        let mut registered = false;
        for (client, subs, deadline) in sessions {
            // Skip ones already handled (online or attaching here) and ones this
            // node does not own (a replica held for another node — its owner
            // materializes it).
            if self.online.contains_key(&client)
                || self.connecting.contains_key(&client)
                || !self.owns_session(&client)
            {
                debug!(client = %client.0, "inherited-session scan: skipped (online/attaching/unowned)");
                continue;
            }
            if let Some(d) = deadline {
                self.expiring.entry(client.clone()).or_insert(d);
            }
            debug!(
                client = %client.0,
                subs = subs.len(),
                known = self.subs_by_client.contains_key(&client),
                "inherited-session scan: owned offline session"
            );
            if subs.is_empty() || self.subs_by_client.contains_key(&client) {
                continue; // nothing to route, or already materialized
            }
            for sub in subs {
                if let Some((group, filter)) = parse_shared(&sub.filter) {
                    self.shared
                        .subscribe(client.clone(), group, filter, sub.max_qos);
                } else {
                    self.table.subscribe(client.clone(), sub.filter.clone());
                }
                self.subs_by_client
                    .entry(client.clone())
                    .or_default()
                    .insert(sub.filter, sub.max_qos);
            }
            // Persistent from the routing path's point of view (offline enqueue).
            // The placeholder interval is corrected by the next real attach; the
            // inherited absolute deadline above still bounds the session's life.
            self.session_expiry
                .entry(client.clone())
                .or_insert(u32::MAX);
            debug!(client = %client.0, "inherited session materialized before re-attach (ADR 0042 T9)");
            registered = true;
        }
        if registered {
            // Peers must know this node now routes the inherited filters — this is
            // what re-targets acked forwards after a takeover (exhibit ⑤ re-route).
            self.gossip_interest();
        }
        self.settle_pending_publishes().await;
    }

    /// A takeover-window re-delivery of pending publish `id` (ADR 0042 T9):
    /// deliver the frame ONLY to routing state that could have missed the
    /// original fan-out — offline persistent sessions (materialized since) and
    /// clients attached after the publish. Clients online since BEFORE the
    /// publish already received it live; re-sending would duplicate (dups are
    /// legal at `QoS` 1, but a boot-window re-send to a steady subscriber is a
    /// gratuitous one — observed as duplicate bridge forwards). Returns `false`
    /// on a terminal durable-append failure (the caller withholds).
    async fn redeliver_pending(&mut self, id: u64) -> bool {
        let Some(p) = self.pending_publishes.get(&id) else {
            return true;
        };
        let (topic, payload, qos, expiry, app, since) = (
            p.topic.clone(),
            p.payload.clone(),
            p.qos,
            p.message_expiry,
            p.app.clone(),
            p.created_at,
        );
        let targets: Vec<(ClientId, QoS)> = self
            .table
            .matching_clients(&topic)
            .into_iter()
            .filter(|c| self.online.get(c).is_none_or(|o| o.attached_at > since))
            .map(|c| {
                let granted = self.granted_qos(&c, &topic);
                (c, granted)
            })
            .collect();
        let mut all_durable = true;
        for (c, granted) in targets {
            all_durable &= self
                .deliver_to_client(&c, &topic, &payload, min_qos(qos, granted), expiry, &app)
                .await;
        }
        all_durable
    }

    /// The takeover window closed for this node (an inherited-session scan just
    /// landed): every held pending publish re-delivers **locally** against the
    /// just-materialized subscriptions (duplicates are legal at `QoS` 1 — the
    /// alternative was an ack into the void, exhibit ⑥), then re-checks remote
    /// interest via the sweep's re-route path before its ack can release.
    async fn settle_pending_publishes(&mut self) {
        let held: Vec<u64> = self
            .pending_publishes
            .iter()
            .filter(|(_, p)| p.awaiting_settle || p.reroute_grace.is_some())
            .map(|(id, _)| *id)
            .collect();
        // The hold clears only when the whole takeover WINDOW is over: one scan
        // is not enough — the group leases reassign for seconds after the death,
        // and a scan that ran before a lease landed saw nothing. Every scan in
        // the window re-delivers (duplicates are legal); the last one releases.
        // And never on a broken mesh: an unreachable-but-alive peer may hold
        // interest this node cannot see (seed 4).
        let window_over = self.takeover_reconcile_ticks == 0 && self.mesh_whole();
        for id in held {
            if !self.redeliver_pending(id).await {
                // The re-delivery's durable append failed terminally: withhold.
                self.drop_pending(id);
                continue;
            }
            // The successor may have materialized the subscriber on ANOTHER node
            // and advertised its interest since this publish's original fan-out
            // (which found nothing): forward to it now — a publish that arrived
            // after the death dropped the dead node's interest has no obligation
            // to re-route, so this is where it re-targets.
            for node in self.reroute_candidates(id) {
                self.send_acked_forward(id, &node);
            }
            if window_over {
                if let Some(p) = self.pending_publishes.get_mut(&id) {
                    p.awaiting_settle = false;
                }
            }
            self.try_complete_pending(id);
        }
    }

    /// Whether every membership-alive peer has a live link (the mesh is
    /// WHOLE). While it is not — a peer is alive per membership but its link is
    /// down — this node cannot see that peer's interest, so a gated publish
    /// must not conclude "nobody is owed this" (ADR 0042 T4, seed 4: the
    /// takeover successor materialized the subscriber behind an active
    /// partition, and the grace expired into an ack for a message the
    /// partitioned owner never received). Withholding under partition is the
    /// same CP posture the durable attach path already takes.
    fn mesh_whole(&self) -> bool {
        let Some(placement) = &self.placement else {
            return true;
        };
        let members: Vec<NodeId> = {
            let p = placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            p.members()
        };
        members
            .iter()
            .filter(|m| **m != self.node_id)
            .all(|m| self.peers.contains_key(m))
    }

    /// Whether this node runs in a multi-node cluster (placement with >1 member).
    fn clustered(&self) -> bool {
        self.placement.as_ref().is_some_and(|p| {
            p.read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .member_count()
                > 1
        })
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
    async fn persist_subscriptions(&mut self, client: &ClientId) -> bool {
        if !self.is_persistent(client) {
            return true; // nothing durable is promised for a clean session
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
        match self.store.set_subscriptions(client, &subs).await {
            Ok(()) => true,
            Err(e) => {
                warn!(client = %client.0, error = %e, "durable subscription write failed");
                false
            }
        }
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
    ///
    /// Under durable retained (ADR 0037 §3) the retain flag no longer forces the
    /// broadcast: caches are warmed by the owner's post-commit fan-out instead, so a
    /// retained publish forwards like any other — to interested peers, for live
    /// delivery only.
    #[allow(clippy::too_many_arguments)]
    fn forward_to_peers(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
        gate: Option<u64>,
    ) {
        let retain_broadcasts = retain && self.durable_retained.is_none();
        // A gated QoS ≥ 1 forward is ACKED (ADR 0042 T9, exhibit ⑤): the
        // publisher's ack waits for each target's durability-gated answer, and
        // the sweep retransmits while unanswered. Targets come from the INTEREST
        // map, not the connected-peer map: a link-down (but not dead) peer's
        // subscribers are still owed the publish — the obligation is recorded
        // now, the frame flows when the link returns (sweep), or re-routes to
        // the successor when membership confirms death (`peer_dead`). A proto-2
        // peer (mixed-version window) keeps the fire-and-forget frame — the old
        // semantics, honestly.
        let gated = gate.is_some() && qos_num(qos) >= 1;
        if gated {
            let targets: Vec<NodeId> = self
                .remote_interest
                .iter()
                .filter(|(_, filters)| filters.iter().any(|f| topic_matches(f, topic)))
                .map(|(node, _)| node.clone())
                .collect();
            let id = gate.unwrap_or_default();
            for node in targets {
                if self.peers.get(&node).is_some_and(|p| p.proto < 3) {
                    // Connected but old: fire-and-forget, as before proto 3.
                    if let Some(peer) = self.peers.get(&node) {
                        let _ = peer.tx.send(PeerMessage::Publish {
                            topic: topic.to_string(),
                            payload: payload.to_vec(),
                            qos: qos as u8,
                            retain,
                            message_expiry,
                            app: app_to_wire(app),
                        });
                    }
                    continue;
                }
                self.send_acked_forward(id, &node);
            }
            if !retain_broadcasts {
                return;
            }
        }
        for (node, peer) in &self.peers {
            let interested = self
                .remote_interest
                .get(node)
                .is_some_and(|filters| filters.iter().any(|f| topic_matches(f, topic)));
            if gated && interested {
                continue; // already handled (acked or legacy) above
            }
            if !(retain_broadcasts || interested) {
                continue;
            }
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

    /// Peers that now advertise matching interest for pending publish `id` but
    /// have neither acked a forward nor have one outstanding — the re-route
    /// targets after a takeover (the dead owner's successor materializes the
    /// inherited sessions and re-advertises their filters).
    fn reroute_candidates(&self, id: u64) -> Vec<NodeId> {
        let Some(p) = self.pending_publishes.get(&id) else {
            return Vec::new();
        };
        self.peers
            .iter()
            .filter(|(n, peer)| {
                peer.proto >= 3
                    && !p.acked_nodes.contains(*n)
                    && !p.awaiting.values().any(|v| v == *n)
                    && self
                        .remote_interest
                        .get(*n)
                        .is_some_and(|fs| fs.iter().any(|f| topic_matches(f, &p.topic)))
            })
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Send (or re-send, on re-route) one acked forward of pending publish `id` to
    /// `node`, recording the obligation (ADR 0042 T9, exhibit ⑤).
    fn send_acked_forward(&mut self, id: u64, node: &NodeId) {
        self.forward_seq += 1;
        let seq = self.forward_seq;
        let Some(p) = self.pending_publishes.get_mut(&id) else {
            return;
        };
        p.awaiting.insert(seq, node.clone());
        self.forward_index.insert(seq, id);
        debug!(publish = id, seq, target = %node.0, topic = %p.topic, "acked forward recorded");
        let frame = PeerMessage::PublishAcked {
            seq,
            topic: p.topic.clone(),
            payload: p.payload.to_vec(),
            qos: p.qos as u8,
            retain: p.retain,
            message_expiry: p.message_expiry,
            app: app_to_wire(&p.app),
        };
        if let Some(peer) = self.peers.get(node) {
            let _ = peer.tx.send(frame);
        }
    }

    /// Register a `QoS` 1 publish whose acknowledgement is gated on cluster-wide
    /// durability (ADR 0042 T9). At the cap the oldest entry is dropped loudly —
    /// its ack withheld, so its publisher retries.
    #[allow(clippy::too_many_arguments)]
    fn register_pending(
        &mut self,
        done: oneshot::Sender<PublishOutcome>,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) -> u64 {
        if self.pending_publishes.len() >= PENDING_PUBLISH_CAP {
            if let Some((old_id, old)) = self.pending_publishes.pop_first() {
                warn!(
                    topic = %old.topic,
                    cap = PENDING_PUBLISH_CAP,
                    "pending-publish cap: dropped the OLDEST unacknowledged publish \
                     (ack withheld; its publisher retries — ADR 0042 T9)"
                );
                self.forward_index.retain(|_, pid| *pid != old_id);
                if let Some(m) = &self.metrics {
                    m.publish_dropped("pending-cap");
                }
            }
        }
        self.publish_ids += 1;
        let id = self.publish_ids;
        self.pending_publishes.insert(
            id,
            PendingPublish {
                done,
                topic: topic.to_string(),
                payload: payload.clone(),
                qos,
                retain,
                message_expiry,
                app: app.clone(),
                awaiting: HashMap::new(),
                acked_nodes: HashSet::new(),
                awaiting_retained: false,
                local_done: false,
                created_at: Instant::now(),
                reroute_grace: None,
                // During a takeover window the routing table may not yet hold the
                // sessions this node (or a successor) inherited — hold the ack
                // until the scan lands and the publish re-delivers (exhibit ⑥).
                // Only meaningful on a multi-node cluster: a standalone node has
                // no takeovers, and holding its boot-time acks would just delay
                // every early publish for nothing.
                awaiting_settle: self.clustered()
                    && (self.takeover_reconcile_ticks > 0
                        || self.inherited_scan_inflight
                        || !self.mesh_whole()),
            },
        );
        id
    }

    /// The local fan-out obligation resolved OK (durable appends included).
    fn pending_local_done(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(&id) {
            p.local_done = true;
        }
        self.try_complete_pending(id);
    }

    /// The retained authority commit obligation resolved (ADR 0042 T9, exhibit ⑦).
    fn pending_retained_done(&mut self, id: u64) {
        if let Some(p) = self.pending_publishes.get_mut(&id) {
            p.awaiting_retained = false;
        }
        self.try_complete_pending(id);
    }

    /// Drop a pending publish, WITHHOLDING its acknowledgement (the sender side
    /// of fail-closed: the publisher's connection sees no ack and retries).
    fn drop_pending(&mut self, id: u64) {
        if self.pending_publishes.remove(&id).is_some() {
            self.forward_index.retain(|_, pid| *pid != id);
        }
    }

    /// Release the publisher's acknowledgement iff every cluster-wide durability
    /// obligation has resolved (ADR 0042 T9).
    fn try_complete_pending(&mut self, id: u64) {
        let complete = self.pending_publishes.get(&id).is_some_and(|p| {
            p.local_done
                && !p.awaiting_retained
                && !p.awaiting_settle
                && p.awaiting.is_empty()
                && p.reroute_grace.unwrap_or(0) == 0
        });
        if complete {
            if let Some(p) = self.pending_publishes.remove(&id) {
                debug!(publish = id, topic = %p.topic, "pending publish complete; ack released");
                let _ = p.done.send(PublishOutcome::Accepted);
            }
        }
    }

    /// A peer's durability-gated answer to one acked forward (ADR 0042 T9,
    /// exhibit ⑤). `ok = false` is a terminal durable failure on the peer: the
    /// whole pending publish is dropped — ack withheld, publisher retries.
    fn forward_acked(&mut self, node: &NodeId, seq: u64, ok: bool) {
        let Some(id) = self.forward_index.remove(&seq) else {
            return; // stale ack (entry dropped or already resolved)
        };
        let Some(p) = self.pending_publishes.get_mut(&id) else {
            return;
        };
        if p.awaiting.get(&seq) != Some(node) {
            return; // not the node this seq was sent to — ignore
        }
        p.awaiting.remove(&seq);
        if ok {
            debug!(publish = id, seq, from = %node.0, "forward ack");
            p.acked_nodes.insert(node.clone());
            self.try_complete_pending(id);
        } else {
            warn!(
                peer = %node.0,
                "peer reported a terminal durable failure for a forwarded publish; \
                 ack withheld (the publisher retries — ADR 0042 T9)"
            );
            self.drop_pending(id);
        }
    }

    /// The sweep-tick half of acked forwards (ADR 0042 T9, exhibit ⑤): retransmit
    /// unanswered forwards whose target link is up (same seq — duplicates are
    /// legal at `QoS` 1), and drive takeover re-routes: a forward whose target
    /// DIED re-forwards to whichever peers now advertise matching interest (the
    /// dead owner's successor, once it materializes inherited sessions —
    /// exhibit ⑥); with no such interest for [`REROUTE_GRACE_TICKS`] ticks the
    /// obligation is moot (the interest genuinely ended) and the ack releases.
    // Retransmit, downgrade, re-route, grace: one linear sweep pass per pending —
    // splitting it would scatter the obligation lifecycle.
    #[allow(clippy::too_many_lines)]
    async fn sweep_pending_forwards(&mut self) {
        let ids: Vec<u64> = self.pending_publishes.keys().copied().collect();
        for id in ids {
            // Retransmit outstanding forwards over live links.
            let outstanding: Vec<(u64, NodeId)> = self
                .pending_publishes
                .get(&id)
                .map(|p| p.awaiting.iter().map(|(s, n)| (*s, n.clone())).collect())
                .unwrap_or_default();
            for (seq, node) in &outstanding {
                let Some(peer) = self.peers.get(node) else {
                    continue; // link down (not dead): wait for it to return
                };
                let Some(p) = self.pending_publishes.get(&id) else {
                    continue;
                };
                if peer.proto < 3 {
                    // The target reconnected speaking an older proto (mixed-version
                    // downgrade): fall back to fire-and-forget — the old semantics,
                    // honestly — and release the unfulfillable obligation.
                    let _ = peer.tx.send(PeerMessage::Publish {
                        topic: p.topic.clone(),
                        payload: p.payload.to_vec(),
                        qos: p.qos as u8,
                        retain: p.retain,
                        message_expiry: p.message_expiry,
                        app: app_to_wire(&p.app),
                    });
                    self.forward_index.remove(seq);
                    if let Some(p) = self.pending_publishes.get_mut(&id) {
                        p.awaiting.remove(seq);
                    }
                    self.try_complete_pending(id);
                    continue;
                }
                let _ = peer.tx.send(PeerMessage::PublishAcked {
                    seq: *seq,
                    topic: p.topic.clone(),
                    payload: p.payload.to_vec(),
                    qos: p.qos as u8,
                    retain: p.retain,
                    message_expiry: p.message_expiry,
                    app: app_to_wire(&p.app),
                });
            }
            // Re-route after a target death (grace engaged by peer_dead).
            let Some(p) = self.pending_publishes.get(&id) else {
                continue;
            };
            let Some(grace) = p.reroute_grace else {
                continue;
            };
            let candidates = self.reroute_candidates(id);
            if !candidates.is_empty() {
                debug!(
                    publish = id,
                    targets = candidates.len(),
                    "re-routing acked forward"
                );
                if let Some(p) = self.pending_publishes.get_mut(&id) {
                    p.reroute_grace = None;
                }
                for node in candidates {
                    self.send_acked_forward(id, &node);
                }
                continue;
            }
            let awaiting_empty = p.awaiting.is_empty();
            if awaiting_empty && grace <= 1 && !self.mesh_whole() {
                // An alive peer is unreachable: its interest is invisible, so
                // "no candidates" proves nothing. Hold at the last grace tick
                // until the mesh heals (seed 4) — the publisher waits, exactly
                // like a durable attach under partition.
                continue;
            }
            if awaiting_empty && grace <= 1 {
                // The grace ends with a FINAL local re-delivery: the subscriber
                // may have materialized HERE in the meantime — via this node's
                // takeover scan or its own re-attach — after this publish's
                // original local fan-out ran against a not-yet-materialized
                // table (exhibit ⑥'s race, both faces). Targeted: only routing
                // state that could have missed the original fan-out.
                debug!(
                    publish = id,
                    "re-route grace expired; final local re-delivery"
                );
                if !self.redeliver_pending(id).await {
                    self.drop_pending(id);
                    continue;
                }
                if let Some(p) = self.pending_publishes.get_mut(&id) {
                    p.reroute_grace = None;
                }
            } else if awaiting_empty {
                if let Some(p) = self.pending_publishes.get_mut(&id) {
                    p.reroute_grace = Some(grace - 1);
                }
            }
            self.try_complete_pending(id);
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
        // With no values AND no tombstone tokens there is nothing a peer could learn
        // from us. A tombstone-only state still offers its digest: a peer holding a
        // value for a topic we committed a clear for must see a difference and pull
        // the tombstone (ADR 0037 P5) — going silent would strand its stale value.
        if retained.is_empty() && self.retained_tokens.is_empty() {
            return;
        }
        let (count, hash, value_hash) = retained_digest(retained.iter().map(|m| {
            (
                m.topic.as_str(),
                m.payload.as_ref(),
                m.qos as u8,
                AppProps::from(&m.app).encode(),
            )
        }));
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
        let ours = retained_digest(retained.iter().map(|m| {
            (
                m.topic.as_str(),
                m.payload.as_ref(),
                m.qos as u8,
                AppProps::from(&m.app).encode(),
            )
        }));
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
        // Cached values carry their commit token (ADR 0037 P5); `(0, 0)` marks an
        // uncommitted (durable-off / pre-migration) value, which the receiver only
        // ever gap-fills with.
        let mut entries: Vec<RetainedWireEntry> = retained
            .into_iter()
            .map(|m| {
                let (epoch, offset) = self
                    .retained_tokens
                    .get(&m.topic)
                    .copied()
                    .unwrap_or((0, 0));
                RetainedWireEntry {
                    props: AppProps::from(&m.app),
                    topic: m.topic,
                    payload: m.payload.to_vec(),
                    qos: m.qos as u8,
                    epoch,
                    offset,
                }
            })
            .collect();
        // Committed clears back-fill too: a token held for a topic no longer cached
        // is a tombstone, sent as an empty-payload entry so a peer that missed the
        // clear drops the topic instead of keeping it forever (ADR 0037 P5).
        let cached: HashSet<&str> = entries.iter().map(|e| e.topic.as_str()).collect();
        let tombstones: Vec<RetainedWireEntry> = self
            .retained_tokens
            .iter()
            .filter(|(topic, _)| !cached.contains(topic.as_str()))
            .map(|(topic, (epoch, offset))| RetainedWireEntry {
                topic: topic.clone(),
                epoch: *epoch,
                offset: *offset,
                ..Default::default()
            })
            .collect();
        entries.extend(tombstones);
        if entries.is_empty() {
            return;
        }
        for messages in chunk_retained(entries.into_iter()) {
            let _ = peer.tx.send(PeerMessage::RetainedSnapshot { messages });
        }
    }

    /// Apply a peer's retained snapshot.
    ///
    /// Under **durable retained** (ADR 0037 P5) each entry applies only when its
    /// `(epoch, offset)` token beats what we hold for the topic — the same monotonic
    /// rule as the commit fan-out, so divergent caches converge deterministically to
    /// the committed value on link-up. An empty payload is a committed clear
    /// (tombstone): it drops the topic and its token fences staler values. An
    /// **untokened** entry (`(0, 0)`, from an uncommitted cache) only gap-fills an
    /// absent topic — it never overwrites anything.
    ///
    /// **Durable off** keeps the ADR 0014 §3 gap-fill rule verbatim: set a topic only
    /// if we do not already retain it, never clobbering our own value.
    ///
    /// Divergence detection (ADR 0037 P1) runs in both modes: a topic both sides hold
    /// differently is counted (`retained_divergence_total`) and surfaced with one
    /// `warn!` per snapshot chunk — under durable the same pass now also *resolves* it.
    async fn apply_retained_snapshot(&mut self, node: &NodeId, messages: Vec<RetainedWireEntry>) {
        let have: HashMap<String, u64> = match self.retained.all().await {
            Ok(all) => all
                .into_iter()
                .map(|m| {
                    let id = retained_value_id(
                        &m.topic,
                        m.payload.as_ref(),
                        m.qos as u8,
                        &AppProps::from(&m.app).encode(),
                    );
                    (m.topic, id)
                })
                .collect(),
            Err(_) => return,
        };
        let durable = self.durable_retained.is_some();
        let mut filled = 0;
        let mut diverged = 0u64;
        for entry in messages {
            let RetainedWireEntry {
                topic,
                payload,
                qos,
                epoch,
                offset,
                props,
            } = entry;
            let payload = Bytes::from(payload);
            let held_value = have.get(&topic);
            // Detection (P1): both sides hold the topic, with different values (an
            // incoming committed clear against our value counts too; differing
            // application properties on an equal payload count as well — ADR 0038 T3).
            if held_value.is_some_and(|ours| {
                *ours != retained_value_id(&topic, payload.as_ref(), qos, &props.encode())
            }) {
                diverged += 1;
                debug!(node = %node.0, %topic, "retained value diverges from peer");
                if let Some(m) = &self.metrics {
                    m.retained_divergence();
                }
            }
            if durable {
                // Token rule (P5): strictly-higher wins; an untokened local value
                // (no held token) loses to any committed token but an untokened
                // entry only gap-fills an absent topic.
                let token = (epoch, offset);
                let apply = match self.retained_tokens.get(&topic) {
                    Some(held) => token > *held,
                    None => token > (0, 0) || held_value.is_none(),
                };
                if !apply {
                    continue;
                }
                self.retained_tokens.insert(topic.clone(), token);
            } else if held_value.is_some() || payload.is_empty() {
                // Gap-fill only (ADR 0014 §3); a tombstone entry has nothing to fill.
                continue;
            }
            // An empty payload clears the topic [MQTT-3.3.1-10]. Application
            // properties back-fill with the value (ADR 0038 T3), so a replay from a
            // back-filled cache matches one from the origin node.
            let message = Message {
                topic,
                payload,
                qos: QoS::from_u8(qos).unwrap_or(QoS::AtMostOnce),
                retain: true,
                app: props.into(),
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
            if durable {
                warn!(
                    node = %node.0,
                    topics = diverged,
                    "retained values DIVERGED from peer (same topic, different value) — \
                     converged to the higher-token committed value (ADR 0037 P5)"
                );
            } else {
                warn!(
                    node = %node.0,
                    topics = diverged,
                    "retained values DIVERGE from peer (same topic, different value) — \
                     best-effort replication kept each side's own value (ADR 0037 P1 detection)"
                );
            }
        }
    }

    fn peer_connected(
        &mut self,
        node: NodeId,
        conn_id: u64,
        tx: PeerOutbound,
        cert_serial: Option<Vec<u8>>,
        proto: u32,
    ) {
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
        self.peers.insert(
            node,
            Peer {
                conn_id,
                tx,
                cert_serial,
                proto,
            },
        );
        // A link (re)forming while gated publishes are held: schedule a scan so
        // the settle pass re-runs against the now-visible peer state (its
        // Interest snapshot arrives with the link) and releases what it can.
        if !self.pending_publishes.is_empty() {
            self.takeover_reconcile_ticks = self.takeover_reconcile_ticks.max(2);
        }
    }

    fn peer_disconnected(&mut self, node: &NodeId, conn_id: u64) {
        // Ignore a stale disconnect from a link that was already replaced.
        if self.peers.get(node).map(|p| p.conn_id) != Some(conn_id) {
            return;
        }
        info!(peer = %node.0, "peer link lost");
        self.peers.remove(node);
        // The peer's INTEREST is kept (ADR 0042 T9): a link-down peer is not a
        // dead peer, and its subscribers are still owed matching publishes — a
        // gated forward to it becomes a held obligation that retransmits when the
        // link returns, or re-routes when membership confirms death (`peer_dead`,
        // which does drop the interest). Dropping interest here was exhibit ⑤'s
        // second face: a publish in the disconnect-to-confirmation window found
        // no interest anywhere and acked a trivially-empty fan-out.
        self.remote_shared.remove(node);
        if let Some(plane) = &self.durable_plane {
            plane.fail(node);
        }
        self.drop_retained_handoff_state(node);
    }

    /// Drop all routing state for a node the failure detector confirmed dead.
    ///
    /// Removing the peer entry also drops its outbound sender, which closes the
    /// link's pump on whichever side still holds the socket open.
    fn peer_dead(&mut self, node: &NodeId) {
        let had_link = self.peers.remove(node).is_some();
        let had_interest = self.remote_interest.remove(node).is_some();
        self.remote_shared.remove(node);
        // Acked forwards to the dead node re-route to its successor once it
        // advertises the inherited interest (ADR 0042 T9, exhibit ⑤ + ⑥): drop
        // the dead obligations and engage the sweep's re-route grace.
        let mut dead_seqs: Vec<u64> = Vec::new();
        for p in self.pending_publishes.values_mut() {
            let seqs: Vec<u64> = p
                .awaiting
                .iter()
                .filter(|(_, n)| *n == node)
                .map(|(s, _)| *s)
                .collect();
            if seqs.is_empty() {
                continue;
            }
            for seq in seqs {
                p.awaiting.remove(&seq);
                dead_seqs.push(seq);
            }
            debug!(peer = %node.0, topic = %p.topic, "forward target died; re-route grace engaged");
            p.reroute_grace = Some(REROUTE_GRACE_TICKS);
        }
        for seq in dead_seqs {
            self.forward_index.remove(&seq);
        }
        if had_link || had_interest {
            info!(peer = %node.0, "peer declared dead; routing state dropped");
        }
        if let Some(plane) = &self.durable_plane {
            plane.fail(node);
        }
        self.drop_retained_handoff_state(node);
    }

    /// The retained-handoff bookkeeping tied to a peer's **link session** (T8),
    /// dropped when the link goes: a handoff awaiting that peer's ack returns to the
    /// queue (the queue-until-heal path takes over), and the owner-side dedup state
    /// for the peer is cleared — a restarted peer restarts its seq counter, and a
    /// stale dedup entry could wrongly swallow its first new handoff. The cost of
    /// clearing is bounded and benign: a retransmission across the flap may commit
    /// the same value twice (idempotent, higher token).
    fn drop_retained_handoff_state(&mut self, node: &NodeId) {
        if self
            .retained_handoff
            .as_ref()
            .is_some_and(|(owner, ..)| owner == node)
        {
            if let Some((_, _, mutation)) = self.retained_handoff.take() {
                self.retained_queue.push_front(mutation);
            }
        }
        self.retained_handoff_seen.remove(node);
        self.retained_handoff_pending.remove(node);
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
    fn shared_snapshot(&self) -> Vec<mqtt_cluster::peer::SharedGroupWire> {
        use mqtt_cluster::peer::{SharedGroupWire, SharedMemberWire};
        self.shared
            .snapshot()
            .into_iter()
            .map(|g| {
                // Tag each member with whether it is online here, so a peer's selector can
                // avoid choosing a member offline on its home node (ADR 0015 T8).
                let members = g
                    .members
                    .into_iter()
                    .map(|(c, q)| SharedMemberWire {
                        online: self.online.contains_key(&c),
                        client: c.0,
                        qos: q as u8,
                    })
                    .collect();
                SharedGroupWire {
                    group: g.group,
                    filter: g.filter,
                    members,
                }
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

    /// Enqueue a retained mutation for its authority commit (ADR 0037 §1/§5). With
    /// durable off (`durable_retained` unset) this is a no-op and retained keeps the
    /// ADR 0014 best-effort behaviour. Every mutation — locally published or routed
    /// here by a peer — passes through the bounded per-node queue, which serializes
    /// commits (per-node order holds even for rapid same-topic publishes) and lets a
    /// mutation that cannot reach its owner wait for a heal instead of being dropped.
    /// At the bound the **oldest** is dropped, loudly.
    fn route_retained_commit(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: u8,
        app: &AppProperties,
        gate: Option<u64>,
    ) {
        if self.durable_retained.is_none() {
            return; // durable off: ADR 0014 behaviour, unchanged (ADR 0037 §6)
        }
        // The gated publish's ack now waits for this authority commit (ADR 0042 T9,
        // exhibit ⑦) — the obligation rides the mutation through re-queues and the
        // handoff hold, however long the commit takes.
        if let Some(id) = gate {
            if let Some(p) = self.pending_publishes.get_mut(&id) {
                p.awaiting_retained = true;
            }
        }
        self.enqueue_retained_mutation(RetainedMutation {
            topic: topic.to_string(),
            payload: payload.clone(),
            qos,
            app: app.clone(),
            reply: None,
            publish: gate,
        });
        self.kick_retained_queue();
    }

    /// Admit a mutation to the bounded queue, dropping the **oldest** loudly at the
    /// cap (ADR 0037 §5). A dropped peer-routed mutation also clears its pending
    /// marker, so the sender's retransmission can be admitted again later.
    fn enqueue_retained_mutation(&mut self, mutation: RetainedMutation) {
        if self.retained_queue.len() >= RETAINED_QUEUE_CAP {
            if let Some(dropped) = self.retained_queue.pop_front() {
                warn!(
                    topic = %dropped.topic,
                    cap = RETAINED_QUEUE_CAP,
                    "retained mutation queue full; dropped the OLDEST queued mutation \
                     (ADR 0037 §5 — the partition has outlasted the queue bound)"
                );
                if let Some((node, seq)) = dropped.reply {
                    if self.retained_handoff_pending.get(&node) == Some(&seq) {
                        self.retained_handoff_pending.remove(&node);
                    }
                }
                // A gated publish whose authority commit was dropped never acks
                // (ADR 0042 T9, exhibit ⑦): the publisher retries.
                if let Some(id) = dropped.publish {
                    self.drop_pending(id);
                }
            }
            if let Some(m) = &self.metrics {
                m.retained_queue_dropped();
            }
        }
        self.retained_queue.push_back(mutation);
    }

    /// Accept a retained mutation a peer routed to this node (ADR 0037 §1/T8):
    /// dedup retransmissions against the last committed handoff (re-ack, don't
    /// recommit) and against one still queued/committing, then run it through the
    /// same queue as local mutations.
    fn accept_routed_retained(
        &mut self,
        node: NodeId,
        topic: String,
        payload: Bytes,
        qos: u8,
        app: AppProperties,
        seq: u64,
    ) {
        if self.durable_retained.is_none() {
            return;
        }
        // The commit landed but the ack was lost: answer again, commit nothing.
        if let Some((last_seq, token)) = self.retained_handoff_seen.get(&node) {
            if *last_seq == seq {
                let token = *token;
                self.send_retained_ack(&node, seq, Some(token));
                return;
            }
        }
        // The original is still queued or committing: ignore the retransmission.
        if self.retained_handoff_pending.get(&node) == Some(&seq) {
            return;
        }
        self.retained_handoff_pending.insert(node.clone(), seq);
        self.enqueue_retained_mutation(RetainedMutation {
            topic,
            payload,
            qos,
            app,
            reply: Some((node, seq)),
            publish: None,
        });
        self.kick_retained_queue();
    }

    /// Send a commit-gated handoff ack (T8) back to `node`, if its link is up. A
    /// missing link is fine: the committed `(seq, token)` stays recorded in
    /// `retained_handoff_seen`, so the sender's retransmission gets the ack then.
    fn send_retained_ack(&self, node: &NodeId, seq: u64, token: Option<(u64, u64)>) {
        if let Some(peer) = self.peers.get(node) {
            let _ = peer.tx.send(PeerMessage::RetainedCommitAck { seq, token });
        }
    }

    /// Drive the retained mutation queue (ADR 0037 §5/T8): drain entries in order —
    /// an owner-local head starts the (single) off-loop commit; a peer-owned head is
    /// handed to its linked owner and **held until the commit-gated ack** (one
    /// handoff in flight, retransmitted by the sweep tick) — and stop at an entry
    /// whose owner is unreachable, leaving it queued for the next heal trigger (a
    /// peer link coming up, the sweep tick, or the next enqueue).
    fn kick_retained_queue(&mut self) {
        if self.retained_handoff.is_some() {
            return; // a handoff is awaiting its ack: order requires we wait
        }
        while !self.retained_commit_inflight {
            let Some(mutation) = self.retained_queue.pop_front() else {
                return;
            };
            // The owner of the topic's placement group; with no ring (single node /
            // no cluster), this node is trivially the owner. Resolved at drain time,
            // not enqueue time, so a lease that moved while queued re-routes.
            let owner = self.placement.as_ref().map_or_else(
                || self.node_id.clone(),
                |p| {
                    p.read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .owner(&mutation.topic)
                },
            );
            if owner == self.node_id {
                self.retained_commit_inflight = true;
                self.spawn_retained_commit(mutation);
                return;
            }
            // Peer-owned. A mutation a peer routed HERE for a group this node no
            // longer owns is NACKed back so the sender re-resolves (T8) — this node
            // must not relay it onward (the ack chain would break).
            if let Some((node, seq)) = mutation.reply {
                if self.retained_handoff_pending.get(&node) == Some(&seq) {
                    self.retained_handoff_pending.remove(&node);
                }
                self.send_retained_ack(&node, seq, None);
                continue;
            }
            if self.peers.contains_key(&owner) {
                // Hand the mutation to its owner and hold it until the commit-gated
                // ack (T8): a frame lost to a dying link is retransmitted, never
                // silently lost. One in flight keeps per-node publish order.
                self.retained_handoff_seq += 1;
                let seq = self.retained_handoff_seq;
                self.send_retained_handoff(&owner, seq, &mutation);
                self.retained_handoff = Some((owner, seq, mutation));
                return;
            }
            // Owner unreachable (partitioned or dead): queue-until-heal. Put the
            // entry back and wait for a trigger — never dropped silently.
            debug!(
                topic = %mutation.topic,
                owner = %owner.0,
                queued = self.retained_queue.len() + 1,
                "retained mutation owner unreachable; queued until heal (ADR 0037 §5)"
            );
            self.retained_queue.push_front(mutation);
            return;
        }
    }

    /// Write one handoff frame toward `owner` (first send and retransmissions alike).
    fn send_retained_handoff(&self, owner: &NodeId, seq: u64, mutation: &RetainedMutation) {
        if let Some(peer) = self.peers.get(owner) {
            let _ = peer.tx.send(PeerMessage::RetainedCommit {
                topic: mutation.topic.clone(),
                payload: mutation.payload.to_vec(),
                qos: mutation.qos,
                props: app_to_wire(&mutation.app),
                seq,
            });
        }
    }

    /// The sweep-tick half of the handoff protocol (T8): retransmit an unanswered
    /// handoff (same `seq` — the owner dedups), or reclaim it into the queue if the
    /// owner's link is gone (the regular queue-until-heal path takes over).
    fn retry_retained_handoff(&mut self) {
        let Some((owner, seq, mutation)) = self.retained_handoff.take() else {
            return;
        };
        if self.peers.contains_key(&owner) {
            debug!(topic = %mutation.topic, owner = %owner.0, seq, "retransmitting unanswered retained handoff");
            self.send_retained_handoff(&owner, seq, &mutation);
            self.retained_handoff = Some((owner, seq, mutation));
        } else {
            self.retained_queue.push_front(mutation);
        }
    }

    /// Start the off-loop durable commit for an owner-local retained mutation: the
    /// quorum round-trip must not stall the hub actor, and exactly one runs at a time
    /// (`retained_commit_inflight`) so commits keep queue order. A zero-length
    /// payload is the MQTT clear [MQTT-3.3.1-10] → a versioned tombstone (ADR 0037
    /// P2). Completion posts [`HubCommand::RetainedCommitDone`] back to the loop.
    fn spawn_retained_commit(&self, mutation: RetainedMutation) {
        let Some(durable) = self.durable_retained.clone() else {
            return;
        };
        let self_tx = self.self_tx.clone();
        let RetainedMutation {
            topic,
            payload,
            qos,
            app,
            reply,
            publish,
        } = mutation;
        tokio::spawn(async move {
            let result = if payload.is_empty() {
                durable.clear(&topic).await
            } else {
                durable
                    .set(&topic, &payload, qos, &AppProps::from(&app))
                    .await
            };
            let token = match result {
                Ok((epoch, offset)) => {
                    debug!(topic = %topic, epoch, offset, "retained mutation committed");
                    Some((epoch, offset))
                }
                // NotOwner: the lease moved after routing (the re-queued entry
                // re-resolves its owner on the next drain). NoQuorum: this side of a
                // partition cannot commit durably — queue until it heals.
                Err(e) => {
                    warn!(
                        topic = %topic,
                        error = %e,
                        "retained durable commit failed; mutation queued until heal (ADR 0037 §5)"
                    );
                    None
                }
            };
            let _ = self_tx.send(HubCommand::RetainedCommitDone {
                topic,
                payload,
                qos,
                app,
                token,
                reply,
                publish,
            });
        });
    }

    /// Apply a **committed** retained value to the local cache iff its token exceeds
    /// the held one (ADR 0037 §3): monotonic per topic, idempotent, order-insensitive.
    /// An empty payload is a committed clear — the cache drops the topic, but the
    /// tombstone's token is kept so a staler value cannot resurrect it.
    async fn apply_retained_update(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: u8,
        app: &AppProperties,
        token: (u64, u64),
    ) {
        if self
            .retained_tokens
            .get(topic)
            .is_some_and(|held| token <= *held)
        {
            debug!(topic = %topic, ?token, "stale/duplicate retained update skipped");
            return;
        }
        self.retained_tokens.insert(topic.to_string(), token);
        let message = Message {
            topic: topic.to_string(),
            payload: payload.clone(),
            qos: QoS::from_u8(qos).unwrap_or(QoS::AtMostOnce),
            retain: true,
            app: app.clone(),
        };
        if let Err(e) = self.retained.set(&message).await {
            warn!(topic = %topic, error = %e, "failed to apply committed retained update");
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
    let recovery =
        recover_until_ready(&store, &pending.client, &pending.admission.identity.subject).await;
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
    /// A committed retained snapshot entry with no application properties — the
    /// common test shape (props-bearing cases build the struct directly).
    fn snap(topic: &str, payload: &[u8], epoch: u64, offset: u64) -> RetainedWireEntry {
        RetainedWireEntry {
            topic: topic.into(),
            payload: payload.to_vec(),
            qos: 0,
            epoch,
            offset,
            props: mqtt_cluster::peer::WireAppProps::default(),
        }
    }
    /// The canonical empty-props bytes folded into digest entries (ADR 0038 T3).
    fn no_props() -> Vec<u8> {
        AppProps::default().encode()
    }

    use super::{
        Admission, AttachOutcome, AuthMethod, Backlog, Hub, HubCommand, Inflight, Outbound,
        PeerOutbound, ProtocolVersion, RemoteSharedGroup, EXPIRY_RECONCILE_EVERY, MAX_BACKLOG,
        REPLAY_LIMIT,
    };
    use bytes::Bytes;
    use mqtt_cluster::peer::{PeerMessage, RetainedWireEntry};
    use mqtt_cluster::placement::{Placement, DEFAULT_REPLICAS};
    use mqtt_cluster::swim::MemberState;
    use mqtt_cluster::NodeId;
    use mqtt_codec::{Packet, QoS};
    use mqtt_core::{AppProperties, ClientId};
    use mqtt_storage::app_props::AppProps;
    use mqtt_storage::repl::InMemoryReplicatedLog;
    use mqtt_storage::{MemorySessionStore, OverflowPolicy, QueueLimits, SessionStore};
    use std::sync::{Arc, RwLock};
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

    /// A password-admitted v3.1.1 [`Admission`] for `subject` — the common test shape.
    fn admission(subject: &str) -> Admission {
        Admission {
            identity: mqtt_auth::Identity {
                subject: subject.to_string(),
                groups: vec![],
            },
            method: AuthMethod::Password,
            cert_serial: None,
            protocol: ProtocolVersion::V311,
        }
    }

    /// Send a persistent (resume) `Attach` and return the raw [`AttachOutcome`] so a
    /// test can assert a reject (`Unavailable`) as well as a present/absent session.
    async fn attach_outcome(tx: &HubTx, client: &str, conn_id: u64) -> AttachOutcome {
        let (out_tx, _out_rx): (Outbound, _) = mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(HubCommand::Attach {
            client: ClientId(client.into()),
            admission: admission(client),
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
            admission: admission(owner),
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
            admission: admission(client),
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
            AttachOutcome::QuotaExceeded => {
                panic!("uncapped test hubs never refuse for quota")
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
            reply: None,
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
            done: None,
            v5: false,
        })
        .unwrap();
    }

    fn subscribe_qos(tx: &HubTx, client: &str, filter: &str, qos: QoS) {
        tx.send(HubCommand::Subscribe {
            client: ClientId(client.into()),
            filters: vec![(filter.into(), qos)],
            reply: None,
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
            done: None,
            v5: false,
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
            cert_serial: None,
            proto: mqtt_cluster::peer::PROTO_MAX,
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

    /// ADR 0040 T1: the eviction primitive. Evicting a live v5 client sends
    /// DISCONNECT 0x87 (Not authorized) and closes its connection; its will is
    /// published (an eviction is an ungraceful end, MQTT-3.14.4-3); an untouched
    /// client keeps flowing; and evicting an offline client is a no-op.
    #[tokio::test]
    async fn eviction_disconnects_the_target_and_leaves_others_undisturbed() {
        let tx = start_hub();

        // A bystander subscribed to the victim's will topic.
        let (mut watcher, _) = attach(&tx, "watcher", 1, true).await;
        subscribe(&tx, "watcher", "wills/victim");

        // The victim: a v5 client with a will, admitted by certificate.
        let (out_tx, mut victim): (Outbound, _) = mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(HubCommand::Attach {
            client: ClientId("victim".into()),
            admission: Admission {
                identity: mqtt_auth::Identity {
                    subject: "victim".into(),
                    groups: vec![],
                },
                method: AuthMethod::Certificate,
                cert_serial: Some(vec![0x0a, 0x0b]),
                protocol: ProtocolVersion::V5,
            },
            conn_id: 2,
            clean_start: true,
            session_expiry: 0,
            receive_maximum: u16::MAX,
            will: Some(mqtt_core::Message {
                topic: "wills/victim".into(),
                payload: Bytes::from_static(b"gone"),
                qos: QoS::AtMostOnce,
                retain: false,
                app: mqtt_core::AppProperties::default(),
            }),
            outbound: out_tx,
            reply: reply_tx,
        })
        .unwrap();
        reply_rx.await.unwrap();

        tx.send(HubCommand::Evict {
            client: ClientId("victim".into()),
            reason: "cert-revoked".into(),
        })
        .unwrap();

        // The victim is told why (v5), then its connection closes.
        match recv_packet(&mut victim).await {
            Some(Packet::Disconnect(d)) => assert_eq!(
                d.reason, 0x87,
                "an evicted v5 client gets DISCONNECT Not authorized"
            ),
            other => panic!("expected DISCONNECT 0x87, got {other:?}"),
        }
        assert!(
            recv_packet(&mut victim).await.is_none(),
            "the evicted connection must be closed"
        );

        // The will reached the bystander, whose own connection is untouched.
        match recv_packet(&mut watcher).await {
            Some(Packet::Publish(p)) => {
                assert_eq!(p.topic, "wills/victim");
                assert_eq!(&p.payload[..], b"gone");
            }
            other => panic!("expected the victim's will, got {other:?}"),
        }

        // Evicting an offline/unknown client is a no-op — the hub keeps serving.
        tx.send(HubCommand::Evict {
            client: ClientId("missing".into()),
            reason: "user-removed".into(),
        })
        .unwrap();
        publish(&tx, "wills/victim", b"still-serving");
        assert_eq!(
            payload_of(&recv_packet(&mut watcher).await.unwrap()),
            b"still-serving"
        );
    }

    /// ADR 0040 T2: the identity sweep. One reload-published policy evicts, in one
    /// pass, the session whose certificate serial is CRL'd, the session whose
    /// password user was removed, and the session the new connect-ACL denies — while
    /// an untouched session keeps flowing. Each eviction is audited with its reason.
    // One scenario deliberately covers all three eviction classes plus the summary.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn the_identity_sweep_evicts_revoked_sessions_and_spares_the_rest() {
        use mqtt_auth::signed_gossip::RevocationList;
        use mqtt_auth::{AuthError, Authenticator, Authorizer, Credentials, Identity};

        /// Denies connect for one subject; permits everything else.
        struct DenyConnectFor(&'static str);
        impl Authorizer for DenyConnectFor {
            fn authorize_publish(&self, _: &Identity, _: &String) -> bool {
                true
            }
            fn authorize_subscribe(&self, _: &Identity, _: &String) -> bool {
                true
            }
            fn authorize_connect(&self, identity: &Identity, _: &ClientId) -> bool {
                identity.subject != self.0
            }
        }
        /// A credential store that no longer knows one subject.
        struct UserGone(&'static str);
        impl Authenticator for UserGone {
            fn authenticate(
                &self,
                _: &ClientId,
                _: &Credentials<'_>,
            ) -> Result<Identity, AuthError> {
                Err(AuthError::Rejected)
            }
            fn password_subject_exists(&self, subject: &str) -> bool {
                subject != self.0
            }
        }

        let tx = start_hub();
        // Four live sessions, admitted under distinct facts.
        let attach_as = |client: &str, adm: Admission, conn_id: u64| {
            let tx = tx.clone();
            let client = client.to_string();
            async move {
                let (out_tx, out_rx): (Outbound, _) = mpsc::unbounded_channel();
                let (reply_tx, reply_rx) = oneshot::channel();
                tx.send(HubCommand::Attach {
                    client: ClientId(client),
                    admission: adm,
                    conn_id,
                    clean_start: true,
                    session_expiry: 0,
                    receive_maximum: u16::MAX,
                    will: None,
                    outbound: out_tx,
                    reply: reply_tx,
                })
                .unwrap();
                reply_rx.await.unwrap();
                out_rx
            }
        };
        let cert_admission = Admission {
            identity: mqtt_auth::Identity {
                subject: "cert-user".into(),
                groups: vec![],
            },
            method: AuthMethod::Certificate,
            cert_serial: Some(vec![0x42]),
            protocol: ProtocolVersion::V311,
        };
        let mut revoked_cert = attach_as("by-cert", cert_admission, 1).await;
        let mut removed_user = attach_as("by-user", admission("bob"), 2).await;
        let mut denied_connect = attach_as("by-acl", admission("evil"), 3).await;
        let (mut survivor, _) = attach(&tx, "keeper", 4, true).await;
        subscribe(&tx, "keeper", "t");

        let audit = Arc::new(mqtt_observability::RecordingAuditSink::default());
        tx.send(HubCommand::SweepIdentities(super::SweepPolicy {
            authorizer: Arc::new(DenyConnectFor("evil")),
            authenticator: Arc::new(UserGone("bob")),
            revoked: RevocationList::from_serials([vec![0x42]]),
            peer_revoked: RevocationList::default(),
            trigger: "signal".into(),
            audit: audit.clone(),
        }))
        .unwrap();

        for (rx, who) in [
            (&mut revoked_cert, "CRL'd certificate"),
            (&mut removed_user, "removed password user"),
            (&mut denied_connect, "connect-ACL denied principal"),
        ] {
            assert!(
                recv_packet(rx).await.is_none(),
                "the {who} session must be evicted by the sweep"
            );
        }
        // The untouched session still receives traffic.
        publish(&tx, "t", b"alive");
        assert_eq!(
            payload_of(&recv_packet(&mut survivor).await.unwrap()),
            b"alive"
        );
        // Each eviction was audited with its reason.
        let events = audit.events();
        for reason in ["cert-revoked", "user-removed", "connect-denied"] {
            assert!(
                events
                    .iter()
                    .any(|e| e.kind == "security.evict" && e.detail.contains(reason)),
                "missing security.evict audit for {reason}: {events:?}"
            );
        }
        // ...and the sweep leaves one summary record with the counts (ADR 0040 T5).
        assert!(
            events.iter().any(|e| e.kind == "security.sweep"
                && e.detail.contains("identities=3")
                && e.detail.contains("grants=0")
                && e.detail.contains("peers=0")),
            "missing the security.sweep summary: {events:?}"
        );
    }

    /// ADR 0040 T3: the grant sweep. A reload that tightens a subscriber's read
    /// access removes the revoked grant from live routing — delivery stops, durably —
    /// while the client stays CONNECTED and its untouched grants keep flowing. The
    /// grant removal is audited.
    #[tokio::test]
    async fn the_grant_sweep_removes_revoked_subscriptions_without_disconnecting() {
        use mqtt_auth::signed_gossip::RevocationList;
        use mqtt_auth::Identity;

        /// Denies subscriptions to `secret/#`; permits everything else.
        struct DenySecret;
        impl mqtt_auth::Authorizer for DenySecret {
            fn authorize_publish(&self, _: &Identity, _: &String) -> bool {
                true
            }
            fn authorize_subscribe(&self, _: &Identity, filter: &String) -> bool {
                !filter.starts_with("secret/")
            }
        }

        let tx = start_hub();
        let (mut reader, _) = attach(&tx, "reader", 1, true).await;
        subscribe(&tx, "reader", "secret/#");
        subscribe(&tx, "reader", "ok/#");
        publish(&tx, "secret/1", b"s1");
        assert_eq!(payload_of(&recv_packet(&mut reader).await.unwrap()), b"s1");

        let audit = Arc::new(mqtt_observability::RecordingAuditSink::default());
        tx.send(HubCommand::SweepIdentities(super::SweepPolicy {
            authorizer: Arc::new(DenySecret),
            authenticator: Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }),
            revoked: RevocationList::default(),
            peer_revoked: RevocationList::default(),
            trigger: "signal".into(),
            audit: audit.clone(),
        }))
        .unwrap();

        // The revoked grant stops delivering; the untouched grant and the
        // connection itself keep working.
        publish(&tx, "secret/2", b"s2");
        publish(&tx, "ok/1", b"fine");
        assert_eq!(
            payload_of(&recv_packet(&mut reader).await.unwrap()),
            b"fine",
            "only the surviving grant may deliver after the sweep"
        );
        assert!(
            audit
                .events()
                .iter()
                .any(|e| e.kind == "security.evict" && e.detail.contains("grant-revoked")),
            "the grant removal must be audited"
        );
    }

    /// ADR 0040 T3: resume-time grant revocation. A persistent session that slept
    /// through a tightening reload has its revoked grants removed when it resumes —
    /// re-checked under the resuming principal's identity against the CURRENT
    /// policy — and queued messages that only a revoked grant admits are dropped
    /// from the replay, durably.
    #[tokio::test]
    async fn a_resumed_session_loses_grants_the_current_policy_denies() {
        use mqtt_auth::{AllowAll, Identity};

        struct DenySecret;
        impl mqtt_auth::Authorizer for DenySecret {
            fn authorize_publish(&self, _: &Identity, _: &String) -> bool {
                true
            }
            fn authorize_subscribe(&self, _: &Identity, filter: &String) -> bool {
                !filter.starts_with("secret/")
            }
        }

        let tx = start_hub();
        // The hub consults this live handle at resume, exactly like the connections do.
        let (authz_tx, authz_rx) =
            tokio::sync::watch::channel(Arc::new(AllowAll) as Arc<dyn mqtt_auth::Authorizer>);
        tx.send(HubCommand::AttachAuthorizer(super::AuthzWatch(authz_rx)))
            .unwrap();

        // A persistent subscriber sleeps with two granted filters...
        let (_rx, _) = attach(&tx, "sleeper", 1, false).await;
        subscribe(&tx, "sleeper", "secret/#");
        subscribe(&tx, "sleeper", "ok/#");
        tx.send(HubCommand::Detach {
            client: ClientId("sleeper".into()),
            conn_id: 1,
            graceful: true,
        })
        .unwrap();

        // ...misses two QoS 1 messages (both queued)...
        publish_qos1(&tx, "secret/1", b"leaked?");
        publish_qos1(&tx, "ok/1", b"kept");

        // ...and the policy tightens while it sleeps.
        authz_tx
            .send(Arc::new(DenySecret) as Arc<dyn mqtt_auth::Authorizer>)
            .unwrap();

        // On resume: the revoked grant is gone, its queued message is NOT replayed,
        // the surviving grant's message is.
        let (mut rx, present) = attach(&tx, "sleeper", 2, false).await;
        assert!(present, "the persistent session must still be present");
        assert_eq!(
            payload_of(&recv_packet(&mut rx).await.unwrap()),
            b"kept",
            "only the surviving grant's queued message may replay"
        );
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "the revoked grant's queued message must not replay"
        );
        // New traffic on the revoked filter no longer routes to the session...
        publish(&tx, "secret/2", b"s2");
        assert!(recv_packet(&mut rx).await.is_none());
        // ...while the surviving grant keeps flowing.
        publish(&tx, "ok/2", b"still");
        assert_eq!(payload_of(&recv_packet(&mut rx).await.unwrap()), b"still");
    }

    /// ADR 0040 T1: v3.1.1 has no server DISCONNECT — an evicted v3.1.1 client's
    /// connection just closes, with no packet first.
    #[tokio::test]
    async fn evicting_a_v311_client_closes_without_a_disconnect_packet() {
        let tx = start_hub();
        // The test helpers admit at v3.1.1 (see `admission`).
        let (mut rx, _) = attach(&tx, "v3", 1, true).await;
        tx.send(HubCommand::Evict {
            client: ClientId("v3".into()),
            reason: "user-removed".into(),
        })
        .unwrap();
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "a v3.1.1 eviction is a bare close — no DISCONNECT exists to send"
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
                assert_eq!(messages[0].topic, "t");
                assert_eq!(&messages[0].payload[..], b"r");
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
            super::retained_digest(std::iter::once(("t", b"r".as_ref(), 0u8, no_props())));
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
            [
                ("t", b"r".as_ref(), 0u8, no_props()),
                ("other", b"x".as_ref(), 0u8, no_props()),
            ]
            .into_iter(),
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
            super::retained_digest(std::iter::once(("t", b"THEIRS".as_ref(), 0u8, no_props())));
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
        let entries = (0..9).map(|i| RetainedWireEntry {
            topic: format!("t/{i}"),
            payload: payload.clone(),
            ..Default::default()
        });
        let chunks = super::chunk_retained(entries);
        assert!(chunks.len() >= 3, "9 MiB must not fit 2 chunks of 4 MiB");
        for chunk in &chunks {
            let bytes: usize = chunk
                .iter()
                .map(|e| e.topic.len() + e.payload.len() + 48)
                .sum();
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
            RetainedWireEntry {
                topic: "ok".into(),
                payload: vec![1u8; 8],
                ..Default::default()
            },
            RetainedWireEntry {
                topic: "huge".into(),
                payload: huge,
                ..Default::default()
            },
        ];
        let chunks = super::chunk_retained(entries.into_iter());
        let all: Vec<&str> = chunks.iter().flatten().map(|e| e.topic.as_str()).collect();
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
        let one = ("x", b"1".as_ref(), 0u8, no_props());
        let two = ("y", b"2".as_ref(), 1u8, no_props());
        let three = ("z", b"3".as_ref(), 0u8, no_props());
        let full = super::retained_digest([one.clone(), two.clone(), three.clone()].into_iter());
        let shuffled =
            super::retained_digest([three.clone(), one.clone(), two.clone()].into_iter());
        assert_eq!(full, shuffled, "order must not matter");
        let subset = super::retained_digest([one.clone(), two.clone()].into_iter());
        assert_ne!(full, subset, "a different set must differ");
        // Same topics, different value: topic hash equal, value hash different.
        let two_changed = ("y", b"CHANGED".as_ref(), 1u8, no_props());
        let diverged =
            super::retained_digest([one.clone(), two_changed, three.clone()].into_iter());
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
            messages: vec![snap("room/t", b"v", 0, 0)],
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
                snap("dev/1", b"theirs", 0, 0),
                snap("dev/same", b"agreed", 0, 0),
                snap("dev/new", b"x", 0, 0),
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
            messages: vec![snap("t", b"peer-stale", 0, 0)],
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

    /// ADR 0041 T5 — a failed durable offline enqueue WITHHOLDS the publisher's
    /// ack (the sender is dropped, the publisher retries) instead of acking a
    /// message a subscriber will never see — fail closed, like the local path.
    #[tokio::test]
    async fn a_failed_offline_enqueue_withholds_the_publishers_ack() {
        let (hub, tx) = Hub::with_config(NodeId("h".into()), FlakyStore::new_no_quorum_enqueue());
        tokio::spawn(hub.run());

        // A persistent, offline subscriber: a publish to it takes the durable
        // enqueue path, which this store fails.
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe(&tx, "p", "fc/t");
        detach(&tx, "p", 1);

        let (done_tx, done_rx) = oneshot::channel();
        tx.send(HubCommand::Publish {
            topic: "fc/t".into(),
            payload: Bytes::from_static(b"x"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties::default(),
            done: Some(done_tx),
            v5: false,
        })
        .unwrap();
        assert!(
            done_rx.await.is_err(),
            "the ack must be withheld when the durable enqueue fails"
        );
    }

    /// ADR 0041 T5 — brownout: above the disk watermark, growth writes are
    /// refused (new retained topics, new sessions, offline enqueues) while
    /// maintenance continues (resume, retained overwrite), and recovery below
    /// the mark restores everything.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn brownout_refuses_growth_and_recovery_restores_it() {
        let tx = start_hub();

        // Pre-existing state: a persistent session with a QoS 1 subscription
        // (asleep through the brownout), and one retained topic.
        let (_rx, _) = attach(&tx, "sleeper", 1, false).await;
        subscribe(&tx, "sleeper", "b/q");
        detach(&tx, "sleeper", 1);
        let retained_publish = |topic: &str, payload: &'static [u8]| {
            let (done_tx, done_rx) = oneshot::channel();
            tx.send(HubCommand::Publish {
                topic: topic.into(),
                payload: Bytes::from_static(payload),
                qos: QoS::AtMostOnce,
                retain: true,
                message_expiry: None,
                app: mqtt_core::AppProperties::default(),
                done: Some(done_tx),
                v5: true,
            })
            .unwrap();
            done_rx
        };
        assert_eq!(
            retained_publish("b/r1", b"v1").await.unwrap(),
            super::PublishOutcome::Accepted
        );

        tx.send(HubCommand::SetBrownout(true)).unwrap();

        // Growth refused: a NEW retained topic...
        assert_eq!(
            retained_publish("b/r2", b"nope").await.unwrap(),
            super::PublishOutcome::RetainedQuotaExceeded,
            "a new retained topic must be refused under brownout"
        );
        // ...and a NEW session...
        assert!(
            matches!(
                attach_outcome(&tx, "stranger", 2).await,
                AttachOutcome::QuotaExceeded
            ),
            "a new session must be refused under brownout"
        );
        // ...and an offline enqueue (silently dropped, counted): this message
        // must NOT replay after recovery.
        publish_qos1(&tx, "b/q", b"browned-out");

        // Maintenance continues: an overwrite of the existing retained topic...
        assert_eq!(
            retained_publish("b/r1", b"v2").await.unwrap(),
            super::PublishOutcome::Accepted,
            "overwriting an existing retained topic is maintenance, not growth"
        );
        // ...and resuming the existing session.
        assert!(
            matches!(
                attach_outcome(&tx, "sleeper", 3).await,
                AttachOutcome::Present(true)
            ),
            "a resume is never refused under brownout"
        );
        detach(&tx, "sleeper", 3);

        // Recovery below the mark restores growth.
        tx.send(HubCommand::SetBrownout(false)).unwrap();
        assert_eq!(
            retained_publish("b/r2", b"now").await.unwrap(),
            super::PublishOutcome::Accepted
        );
        assert!(matches!(
            attach_outcome(&tx, "stranger", 4).await,
            AttachOutcome::Present(false)
        ));
        publish_qos1(&tx, "b/q", b"kept");

        // The sleeper replays ONLY the post-recovery message.
        let (mut rx, present) = attach(&tx, "sleeper", 5, false).await;
        assert!(present);
        assert_eq!(
            payload_of(&recv_packet(&mut rx).await.unwrap()),
            b"kept",
            "only the post-recovery enqueue may replay"
        );
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "the browned-out message must not have been queued"
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
            done: None,
            v5: false,
        })
        .unwrap();
    }

    // -----------------------------------------------------------------------
    // ADR 0037 P3: the owner write path — a locally-originated retained mutation
    // also commits into the durable retained keyspace, routed to the topic's
    // placement-group owner. Durable off (no keyspace attached) keeps the ADR 0014
    // best-effort behaviour byte-for-byte.
    // -----------------------------------------------------------------------

    type TestDurableRetained =
        std::sync::Arc<mqtt_storage::retained_log::ReplicatedRetained<InMemoryReplicatedLog>>;

    /// A hub with the durable retained keyspace attached (over an in-memory log —
    /// epoch 0) and a placement ring of this node plus `peers`. Returns the handle so
    /// tests can observe what was durably committed.
    fn start_hub_with_durable_retained(
        peers: &[&str],
    ) -> (HubTx, TestDurableRetained, Arc<RwLock<Placement>>) {
        let local = NodeId("hub-test".into());
        let mut p = Placement::new(local.clone(), DEFAULT_REPLICAS);
        for n in peers {
            p.observe(&NodeId((*n).into()), MemberState::Alive, "peer:7000", None);
        }
        let placement = Arc::new(RwLock::new(p));
        let (mut hub, tx) = Hub::with_config_and_placement(
            local,
            Arc::new(MemorySessionStore::new()),
            Some(placement.clone()),
        );
        let handle = Arc::new(mqtt_storage::retained_log::ReplicatedRetained::new(
            InMemoryReplicatedLog::new(),
        ));
        hub.attach_durable_retained(handle.clone());
        tokio::spawn(hub.run());
        (tx, handle, placement)
    }

    /// Poll the durable keyspace until `topic`'s committed entry satisfies `pred`
    /// (the commit runs off-loop), or fail after a bounded wait.
    async fn wait_durable_retained(
        handle: &TestDurableRetained,
        topic: &str,
        pred: impl Fn(&mqtt_storage::retained_log::RetainedEntry) -> bool,
    ) -> mqtt_storage::retained_log::RetainedEntry {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(e) = handle.get(topic).await.unwrap() {
                if pred(&e) {
                    return e;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "durable retained commit never landed for {topic}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A retained publish landing on the topic's group owner commits into the durable
    /// keyspace with its `(epoch, offset)` token — and live delivery to a subscriber
    /// happens as before (undelayed by the off-loop commit). A zero-length retained
    /// publish commits a **versioned tombstone**, not an absence.
    #[tokio::test]
    async fn a_local_retained_publish_commits_to_the_durable_keyspace() {
        // Single-node ring: this node owns every group.
        let (tx, durable, _placement) = start_hub_with_durable_retained(&[]);

        let (mut sub, _) = attach(&tx, "s", 1, true).await;
        subscribe(&tx, "s", "dev/1/state");

        publish_retained(&tx, "dev/1/state", b"open");
        // Live delivery is untouched by the authority write.
        assert_eq!(payload_of(&recv_packet(&mut sub).await.unwrap()), b"open");
        // The mutation committed durably with its token (in-memory log: epoch 0).
        let e = wait_durable_retained(&durable, "dev/1/state", |_| true).await;
        assert_eq!(e.payload, b"open");
        assert!(!e.tombstone);
        assert_eq!(e.token(), (0, 1));

        // The MQTT clear is a committed tombstone with the next token — versioned,
        // so a heal can order it against any concurrent value (ADR 0037 P2).
        publish_retained(&tx, "dev/1/state", b"");
        let e = wait_durable_retained(&durable, "dev/1/state", |e| e.tombstone).await;
        assert_eq!(e.token(), (0, 2));
    }

    /// A retained publish for a topic whose group a PEER owns routes the mutation to
    /// that owner as a targeted `RetainedCommit` — no local durable write (a non-owner
    /// append would diverge; the owner is the single writer, ADR 0037 §1). The
    /// live-delivery forward to an interested peer still precedes it on the same link
    /// (under durable the raw broadcast is interest-only — P4's fan-out warms caches).
    #[tokio::test]
    async fn a_foreign_topics_retained_publish_routes_the_commit_to_its_owner() {
        let (tx, durable, placement) = start_hub_with_durable_retained(&["n"]);
        let mut peer = connect_peer(&tx, "n", 1);

        // A topic whose placement group "n" owns.
        let topic = {
            let p = placement.read().unwrap();
            (0..100_000)
                .map(|i| format!("dev/{i}/state"))
                .find(|t| p.owner(t) == NodeId("n".into()))
                .expect("some topic is owned by the peer")
        };
        // The peer has a live subscriber for the topic, so the ordinary forward flows.
        remote_interest(&tx, "n", &[&topic]);

        tx.send(HubCommand::Publish {
            topic: topic.clone(),
            payload: Bytes::from_static(b"v"),
            qos: QoS::AtLeastOnce,
            retain: true,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: false,
        })
        .unwrap();

        // The link carries the live-delivery forward first, then the authority routing.
        let mut saw_forward = false;
        loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::Publish { retain, .. }) => saw_forward = retain,
                Some(PeerMessage::RetainedCommit {
                    topic: t,
                    payload,
                    qos,
                    ..
                }) => {
                    assert_eq!(t, topic);
                    assert_eq!(payload, b"v");
                    assert_eq!(qos, 1);
                    break;
                }
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        assert!(
            saw_forward,
            "the interested peer still gets the live forward"
        );
        // No local durable write for a foreign topic: the owner is the single writer.
        assert!(durable.get(&topic).await.unwrap().is_none());
    }

    /// The owner side of the routed write: a peer's `RetainedCommit` commits into
    /// this node's durable keyspace (value, then a zero-length clear as a tombstone).
    #[tokio::test]
    async fn a_remote_retained_commit_is_committed_by_the_owner() {
        let (tx, durable, _placement) = start_hub_with_durable_retained(&[]);

        tx.send(HubCommand::RemoteRetainedCommit {
            node: NodeId("n".into()),
            topic: "dev/9/state".into(),
            payload: Bytes::from_static(b"shut"),
            qos: 1,
            seq: 1,
            app: AppProperties::default(),
        })
        .unwrap();
        let e = wait_durable_retained(&durable, "dev/9/state", |_| true).await;
        assert_eq!(e.payload, b"shut");
        assert_eq!(e.qos, 1);
        assert_eq!(e.token(), (0, 1));

        tx.send(HubCommand::RemoteRetainedCommit {
            node: NodeId("n".into()),
            topic: "dev/9/state".into(),
            payload: Bytes::new(),
            qos: 0,
            seq: 2,
            app: AppProperties::default(),
        })
        .unwrap();
        let e = wait_durable_retained(&durable, "dev/9/state", |e| e.tombstone).await;
        assert_eq!(e.token(), (0, 2));
    }

    /// The retained value a fresh subscriber replays for `topic`, or `None` if the
    /// local cache holds nothing (bounded wait). Client names must be unique per test.
    async fn retained_replay(tx: &HubTx, client: &str, topic: &str) -> Option<Vec<u8>> {
        let (mut rx, _) = attach(tx, client, 99, true).await;
        subscribe(tx, client, topic);
        recv_packet(&mut rx).await.map(|p| payload_of(&p).to_vec())
    }

    /// ADR 0037 P4: a peer-fanned committed retained value applies to the local cache
    /// **monotonically per topic** — a higher token wins, a stale or duplicate token
    /// is skipped — so caches converge no matter the arrival order.
    #[tokio::test]
    async fn a_remote_retained_update_applies_monotonically_per_topic() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        let update = |payload: &'static [u8], epoch: u64, offset: u64| {
            tx.send(HubCommand::RemoteRetainedUpdate {
                topic: "t".into(),
                payload: Bytes::from_static(payload),
                qos: 0,
                epoch,
                offset,
                app: AppProperties::default(),
            })
            .unwrap();
        };

        update(b"v1", 1, 1);
        assert_eq!(retained_replay(&tx, "c1", "t").await.unwrap(), b"v1");

        // A higher token replaces the value.
        update(b"v3", 1, 3);
        assert_eq!(retained_replay(&tx, "c2", "t").await.unwrap(), b"v3");

        // A stale (lower-token) arrival is skipped — order-insensitive convergence.
        update(b"v2", 1, 2);
        assert_eq!(retained_replay(&tx, "c3", "t").await.unwrap(), b"v3");

        // A duplicate token is idempotent (redelivery cannot regress the cache).
        update(b"dup", 1, 3);
        assert_eq!(retained_replay(&tx, "c4", "t").await.unwrap(), b"v3");

        // A higher epoch outranks any offset (lexicographic token order).
        update(b"new-owner", 2, 1);
        assert_eq!(retained_replay(&tx, "c5", "t").await.unwrap(), b"new-owner");
    }

    /// ADR 0037 P4: a committed clear (empty payload) drops the topic from the cache
    /// but its token still fences — a staler value cannot resurrect the topic.
    #[tokio::test]
    async fn a_stale_value_cannot_resurrect_a_committed_clear() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "t".into(),
            payload: Bytes::from_static(b"v"),
            qos: 0,
            epoch: 1,
            offset: 4,
            app: AppProperties::default(),
        })
        .unwrap();
        assert_eq!(retained_replay(&tx, "c1", "t").await.unwrap(), b"v");

        // The committed clear wins by token...
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "t".into(),
            payload: Bytes::new(),
            qos: 0,
            epoch: 1,
            offset: 5,
            app: AppProperties::default(),
        })
        .unwrap();
        assert!(retained_replay(&tx, "c2", "t").await.is_none());

        // ...and a stale value arriving late cannot bring the topic back.
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "t".into(),
            payload: Bytes::from_static(b"zombie"),
            qos: 0,
            epoch: 1,
            offset: 4,
            app: AppProperties::default(),
        })
        .unwrap();
        assert!(retained_replay(&tx, "c3", "t").await.is_none());
    }

    /// ADR 0037 P4: after the owner's off-loop commit, the tokened value fans out to
    /// peers as `RetainedUpdate` — and under durable the raw broadcast no longer goes
    /// to a non-interested peer (the fan-out IS the cache warmer now). The owner's own
    /// cache warms from the same commit, so a later local subscriber replays it.
    #[tokio::test]
    async fn a_committed_retained_publish_fans_out_with_its_token() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));

        publish_retained(&tx, "t", b"v");

        // The non-interested peer gets the post-commit fan-out — and ONLY that (no
        // raw Publish broadcast under durable).
        match recv_peer(&mut peer).await {
            Some(PeerMessage::RetainedUpdate {
                topic,
                payload,
                qos,
                epoch,
                offset,
                ..
            }) => {
                assert_eq!(topic, "t");
                assert_eq!(payload, b"v");
                assert_eq!(qos, 0);
                assert_eq!((epoch, offset), (0, 1), "the commit's token rides along");
            }
            other => panic!("expected the tokened RetainedUpdate, got {other:?}"),
        }

        // The owner's own cache warmed from the commit: a late subscriber replays it.
        assert_eq!(retained_replay(&tx, "late", "t").await.unwrap(), b"v");
    }

    /// ADR 0037 P4: under durable retained, a peer's raw forwarded publish still
    /// live-delivers to local subscribers but no longer warms the retained cache —
    /// applying the raw (uncommitted, untokened) value is exactly the everyday-race
    /// divergence the fan-out replaces.
    #[tokio::test]
    async fn the_raw_broadcast_no_longer_warms_caches_under_durable() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        let (mut live, _) = attach(&tx, "live", 1, true).await;
        subscribe(&tx, "live", "t");

        tx.send(HubCommand::RemotePublish {
            topic: "t".into(),
            payload: Bytes::from_static(b"x"),
            qos: QoS::AtMostOnce,
            retain: true,
            message_expiry: None,
            app: AppProperties::default(),
        })
        .unwrap();

        // Live delivery is unchanged...
        assert_eq!(payload_of(&recv_packet(&mut live).await.unwrap()), b"x");
        // ...but the cache was not warmed: a fresh subscriber replays nothing.
        assert!(retained_replay(&tx, "late", "t").await.is_none());
    }

    /// ADR 0037 P5: a snapshot entry applies through the same token gate as the
    /// fan-out — the higher token wins per topic, a stale one is dropped — so two
    /// divergent caches converge deterministically to the committed value on link-up.
    #[tokio::test]
    async fn back_fill_takes_the_higher_token_value_per_topic() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        // We hold a committed value at (1, 2).
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "t".into(),
            payload: Bytes::from_static(b"ours"),
            qos: 0,
            epoch: 1,
            offset: 2,
            app: AppProperties::default(),
        })
        .unwrap();

        // The peer's snapshot carries a HIGHER-token value: it wins (divergence
        // resolved, not just detected).
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![snap("t", b"newer", 1, 5)],
        })
        .unwrap();
        assert_eq!(retained_replay(&tx, "c1", "t").await.unwrap(), b"newer");

        // A STALER snapshot entry is rejected — back-fill can never regress a topic.
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![snap("t", b"old", 1, 3)],
        })
        .unwrap();
        assert_eq!(retained_replay(&tx, "c2", "t").await.unwrap(), b"newer");
    }

    /// ADR 0037 P5: a committed clear back-fills as an empty-payload tombstone entry —
    /// the topic drops from the cache, and the tombstone's token keeps fencing staler
    /// values, so the cleared topic cannot be resurrected by a later stale snapshot.
    #[tokio::test]
    async fn a_committed_clear_back_fills_as_a_tombstone_and_fences() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "t".into(),
            payload: Bytes::from_static(b"v"),
            qos: 0,
            epoch: 1,
            offset: 3,
            app: AppProperties::default(),
        })
        .unwrap();
        assert_eq!(retained_replay(&tx, "c1", "t").await.unwrap(), b"v");

        // The peer committed a clear at (1, 6): our value drops.
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![snap("t", b"", 1, 6)],
        })
        .unwrap();
        assert!(retained_replay(&tx, "c2", "t").await.is_none());

        // A staler value (from a peer that missed the clear) cannot resurrect it.
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![snap("t", b"zombie", 1, 5)],
        })
        .unwrap();
        assert!(retained_replay(&tx, "c3", "t").await.is_none());
    }

    /// ADR 0037 P5: the outgoing snapshot carries each cached value's commit token,
    /// **and** a tombstone entry (empty payload + token) for every committed clear —
    /// a peer that missed the clear must see it, or it keeps the value forever.
    #[tokio::test]
    async fn the_snapshot_carries_tokens_and_tombstone_entries() {
        let (tx, durable, _placement) = start_hub_with_durable_retained(&[]);
        // Commit a value on "alive" ((0,1)); commit then clear "dead" ((0,1)→(0,2)).
        publish_retained(&tx, "alive", b"v");
        publish_retained(&tx, "dead", b"x");
        publish_retained(&tx, "dead", b"");
        // Wait for the off-loop commits to land (the clear leaves only the token).
        wait_durable_retained(&durable, "dead", |e| e.tombstone).await;
        wait_durable_retained(&durable, "alive", |_| true).await;

        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::RetainedDigest { .. })
        ));

        tx.send(HubCommand::RemoteRetainedRequest {
            node: NodeId("n".into()),
        })
        .unwrap();
        match recv_peer(&mut peer).await {
            Some(PeerMessage::RetainedSnapshot { mut messages }) => {
                messages.sort_by(|a, b| a.topic.cmp(&b.topic));
                assert_eq!(
                    messages.len(),
                    2,
                    "the value AND the tombstone: {messages:?}"
                );
                let e = &messages[0];
                assert_eq!((e.topic.as_str(), &e.payload[..]), ("alive", b"v".as_ref()));
                assert_eq!((e.epoch, e.offset), (0, 1), "the value carries its token");
                let e = &messages[1];
                assert_eq!(e.topic, "dead");
                assert!(e.payload.is_empty(), "the clear rides as a tombstone entry");
                assert_eq!((e.epoch, e.offset), (0, 2), "with the clear's token");
            }
            other => panic!("expected the retained snapshot, got {other:?}"),
        }
    }

    /// ADR 0037 P5: an **untokened** entry (`(0,0)`, an uncommitted / pre-migration
    /// cache value) gap-fills an absent topic but never overwrites anything — only
    /// committed tokens can replace state.
    #[tokio::test]
    async fn an_untokened_snapshot_entry_gap_fills_but_never_overwrites() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        // Absent topic: the untokened entry gap-fills.
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![snap("t", b"first", 0, 0)],
        })
        .unwrap();
        assert_eq!(retained_replay(&tx, "c1", "t").await.unwrap(), b"first");

        // Present topic: another untokened entry cannot overwrite it.
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![snap("t", b"second", 0, 0)],
        })
        .unwrap();
        assert_eq!(retained_replay(&tx, "c2", "t").await.unwrap(), b"first");

        // A committed token, however, beats the uncommitted value.
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![snap("t", b"committed", 2, 1)],
        })
        .unwrap();
        assert_eq!(retained_replay(&tx, "c3", "t").await.unwrap(), b"committed");
    }

    /// ADR 0037 P5: a node whose retained state is **only tombstones** (every value
    /// cleared) still offers its digest on link-up — going silent would strand a
    /// peer's stale value with nothing to pull the clear from.
    #[tokio::test]
    async fn a_tombstone_only_node_still_offers_its_digest() {
        let (tx, durable, _placement) = start_hub_with_durable_retained(&[]);
        publish_retained(&tx, "t", b"v");
        publish_retained(&tx, "t", b"");
        wait_durable_retained(&durable, "t", |e| e.tombstone).await;

        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));
        assert!(
            matches!(
                recv_peer(&mut peer).await,
                Some(PeerMessage::RetainedDigest { .. })
            ),
            "the digest must be offered even with an empty cache (tombstones held)"
        );
    }

    // -----------------------------------------------------------------------
    // ADR 0037 P6: bounded queue-until-heal for retained mutations.
    // -----------------------------------------------------------------------

    /// A durable retained authority that fails every commit until healed — the
    /// minority side of a partition (`NoQuorum`), from the hub's point of view.
    #[derive(Debug, Default)]
    struct FlakyRetained {
        healthy: std::sync::atomic::AtomicBool,
        /// Every successful commit, in order: `(topic, payload, tombstone)`.
        committed: std::sync::Mutex<Vec<(String, Vec<u8>, bool)>>,
    }

    impl FlakyRetained {
        fn heal(&self) {
            self.healthy
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn commit(
            &self,
            topic: &str,
            payload: &[u8],
            tombstone: bool,
        ) -> Result<(u64, u64), mqtt_storage::StorageError> {
            if !self.healthy.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(mqtt_storage::StorageError::NoQuorum);
            }
            let mut log = self.committed.lock().unwrap();
            log.push((topic.to_string(), payload.to_vec(), tombstone));
            Ok((0, log.len() as u64))
        }
    }

    #[async_trait::async_trait]
    impl mqtt_storage::retained_log::DurableRetained for FlakyRetained {
        async fn set(
            &self,
            topic: &str,
            payload: &[u8],
            _qos: u8,
            _props: &AppProps,
        ) -> Result<(u64, u64), mqtt_storage::StorageError> {
            self.commit(topic, payload, false)
        }

        async fn clear(&self, topic: &str) -> Result<(u64, u64), mqtt_storage::StorageError> {
            self.commit(topic, &[], true)
        }

        async fn get(
            &self,
            _topic: &str,
        ) -> Result<Option<mqtt_storage::retained_log::RetainedEntry>, mqtt_storage::StorageError>
        {
            Ok(None)
        }
    }

    /// ADR 0037 §5: a retained mutation whose group owner is unreachable **queues**
    /// (never silently dropped); when the owner's link comes up the queue drains to
    /// it in publish order.
    #[tokio::test]
    async fn an_unreachable_owner_queues_mutations_until_the_link_heals() {
        let (tx, durable, placement) = start_hub_with_durable_retained(&["n"]);
        let topic = {
            let p = placement.read().unwrap();
            (0..100_000)
                .map(|i| format!("dev/{i}/state"))
                .find(|t| p.owner(t) == NodeId("n".into()))
                .expect("some topic is owned by the peer")
        };

        // The owner is NOT linked: both mutations queue (nothing to observe yet).
        tx.send(HubCommand::Publish {
            topic: topic.clone(),
            payload: Bytes::from_static(b"v1"),
            qos: QoS::AtMostOnce,
            retain: true,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: false,
        })
        .unwrap();
        tx.send(HubCommand::Publish {
            topic: topic.clone(),
            payload: Bytes::from_static(b"v2"),
            qos: QoS::AtMostOnce,
            retain: true,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: false,
        })
        .unwrap();
        // No local durable write for a foreign topic while queued.
        assert!(durable.get(&topic).await.unwrap().is_none());

        // HEAL: the owner's link comes up — the queue drains to it in order, one
        // handoff at a time: each next mutation flows only after the previous one's
        // commit-gated ack (T8 keep-until-ack pacing).
        let mut peer = connect_peer(&tx, "n", 1);
        let mut got = Vec::new();
        while got.len() < 2 {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedCommit {
                    topic: t,
                    payload,
                    seq,
                    ..
                }) => {
                    assert_eq!(t, topic);
                    got.push(payload);
                    // Acknowledge the commit so the sender releases the next one.
                    tx.send(HubCommand::RemoteRetainedCommitAck {
                        node: NodeId("n".into()),
                        seq,
                        token: Some((1, got.len() as u64)),
                    })
                    .unwrap();
                }
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        assert_eq!(
            got,
            vec![b"v1".to_vec(), b"v2".to_vec()],
            "queue order held"
        );
    }

    /// ADR 0037 §5: the queue bound drops the **oldest** mutation loudly — the drop
    /// counter moves and the survivors drain in order on heal.
    #[tokio::test]
    async fn the_retained_queue_bound_drops_the_oldest_loudly() {
        // Manual assembly (the shared helper attaches no metrics).
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let local = NodeId("hub-test".into());
        let mut p = Placement::new(local.clone(), DEFAULT_REPLICAS);
        p.observe(&NodeId("n".into()), MemberState::Alive, "peer:7000", None);
        let placement = Arc::new(RwLock::new(p));
        let (mut hub, tx) = Hub::with_config_and_placement(
            local,
            Arc::new(MemorySessionStore::new()),
            Some(placement.clone()),
        );
        hub.attach_durable_retained(Arc::new(
            mqtt_storage::retained_log::ReplicatedRetained::new(InMemoryReplicatedLog::new()),
        ));
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        let topic = {
            let p = placement.read().unwrap();
            (0..100_000)
                .map(|i| format!("dev/{i}/state"))
                .find(|t| p.owner(t) == NodeId("n".into()))
                .expect("some topic is owned by the peer")
        };

        // Overfill the queue by 3 while the owner is unreachable.
        for i in 0..(super::RETAINED_QUEUE_CAP + 3) {
            tx.send(HubCommand::Publish {
                topic: topic.clone(),
                payload: Bytes::from(format!("m{i}").into_bytes()),
                qos: QoS::AtMostOnce,
                retain: true,
                message_expiry: None,
                app: AppProperties::default(),
                done: None,
                v5: false,
            })
            .unwrap();
        }

        // Heal and read the first drained mutation: the 3 oldest were dropped.
        let mut peer = connect_peer(&tx, "n", 1);
        loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedCommit { payload, .. }) => {
                    assert_eq!(payload, b"m3", "the oldest three must have been dropped");
                    break;
                }
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        let text = metrics.render();
        assert!(
            text.contains("retained_queue_dropped_total 3"),
            "exactly three loud drops:\n{text}"
        );
    }

    /// ADR 0037 §5: an owner-local commit that fails (no quorum — the minority side)
    /// re-queues and retries on the sweep tick; once quorum returns the whole queue
    /// commits **in publish order** and the committed values fan out.
    #[tokio::test(start_paused = true)]
    async fn a_failed_local_commit_retries_until_heal_and_keeps_order() {
        let flaky = Arc::new(FlakyRetained::default());
        let local = NodeId("hub-test".into());
        let placement = Arc::new(RwLock::new(Placement::new(local.clone(), DEFAULT_REPLICAS)));
        let (mut hub, tx) = Hub::with_config_and_placement(
            local,
            Arc::new(MemorySessionStore::new()),
            Some(placement),
        );
        hub.attach_durable_retained(flaky.clone());
        tokio::spawn(hub.run());
        let mut peer = connect_peer(&tx, "n", 1);

        // Three mutations while the authority has no quorum: value, value, clear.
        publish_retained(&tx, "t", b"v1");
        publish_retained(&tx, "t", b"v2");
        publish_retained(&tx, "t", b"");
        // Let the loop attempt (and fail) — nothing commits, nothing fans out.
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 2).await;
        assert!(flaky.committed.lock().unwrap().is_empty());

        // HEAL: quorum returns; the sweep tick retries and the queue drains in order.
        flaky.heal();
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 3).await;
        let committed = flaky.committed.lock().unwrap().clone();
        assert_eq!(
            committed,
            vec![
                ("t".to_string(), b"v1".to_vec(), false),
                ("t".to_string(), b"v2".to_vec(), false),
                ("t".to_string(), Vec::new(), true),
            ],
            "all queued mutations commit, in publish order"
        );

        // Each commit fanned out with its token; the last is the clear.
        let mut updates = Vec::new();
        while updates.len() < 3 {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedUpdate {
                    payload, offset, ..
                }) => {
                    updates.push((payload, offset));
                }
                Some(PeerMessage::Interest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        assert_eq!(
            updates[2],
            (Vec::new(), 3),
            "the clear fans out last, tokened"
        );
    }

    // -----------------------------------------------------------------------
    // ADR 0037 T8: the acknowledged handoff.
    // -----------------------------------------------------------------------

    /// T8: a handoff is kept by the sender and retransmitted (same seq) until the
    /// owner's commit-gated ack arrives — a frame lost to a dying link is retried,
    /// never silently lost — and the next mutation flows only after the ack.
    #[tokio::test(start_paused = true)]
    async fn a_handoff_is_retransmitted_until_the_owner_acks() {
        let (tx, _durable, placement) = start_hub_with_durable_retained(&["n"]);
        let mut peer = connect_peer(&tx, "n", 1);
        let topic = {
            let p = placement.read().unwrap();
            (0..100_000)
                .map(|i| format!("dev/{i}/state"))
                .find(|t| p.owner(t) == NodeId("n".into()))
                .expect("some topic is owned by the peer")
        };

        // Two mutations; only the FIRST is handed off (one in flight).
        for payload in [b"v1".as_ref(), b"v2".as_ref()] {
            tx.send(HubCommand::Publish {
                topic: topic.clone(),
                payload: Bytes::copy_from_slice(payload),
                qos: QoS::AtMostOnce,
                retain: true,
                message_expiry: None,
                app: AppProperties::default(),
                done: None,
                v5: false,
            })
            .unwrap();
        }
        let first_seq = loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedCommit { payload, seq, .. }) => {
                    assert_eq!(payload, b"v1");
                    break seq;
                }
                Some(PeerMessage::Interest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        };
        // Until the ack, nothing but retransmissions of the FIRST handoff may
        // appear — v2 must wait. (Under paused time, empty receives auto-advance
        // the clock, so sweep-tick retransmissions of v1 can legitimately land in
        // this window; what must never appear is a different seq.)
        loop {
            match recv_peer(&mut peer).await {
                None => break,
                Some(PeerMessage::RetainedCommit { payload, seq, .. }) => {
                    assert_eq!(payload, b"v1");
                    assert_eq!(
                        seq, first_seq,
                        "the second mutation must wait for the first ack"
                    );
                }
                other => panic!("unexpected peer frame {other:?}"),
            }
        }

        // Unanswered: the sweep tick retransmits with the SAME seq.
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 2).await;
        match recv_peer(&mut peer).await {
            Some(PeerMessage::RetainedCommit { payload, seq, .. }) => {
                assert_eq!(payload, b"v1");
                assert_eq!(
                    seq, first_seq,
                    "retransmission must reuse the seq (dedup key)"
                );
            }
            other => panic!("expected the retransmission, got {other:?}"),
        }

        // Ack releases the next mutation, with a fresh seq.
        tx.send(HubCommand::RemoteRetainedCommitAck {
            node: NodeId("n".into()),
            seq: first_seq,
            token: Some((1, 1)),
        })
        .unwrap();
        loop {
            match recv_peer(&mut peer).await {
                // Late retransmissions of the acked handoff may already be in the
                // channel (one per elapsed sweep) — the owner-side dedup would
                // swallow them; the test just skips past.
                Some(PeerMessage::RetainedCommit { seq, .. }) if seq == first_seq => {}
                Some(PeerMessage::RetainedCommit { payload, seq, .. }) => {
                    assert_eq!(payload, b"v2");
                    assert_ne!(seq, first_seq);
                    break;
                }
                other => panic!("expected the second handoff, got {other:?}"),
            }
        }
    }

    /// T8 (owner side): a retransmitted handoff is deduped — committed exactly once,
    /// re-acked with the recorded token, whether the duplicate overtakes the commit
    /// (pending) or arrives after it (seen).
    #[tokio::test]
    async fn an_owner_dedups_a_retransmitted_handoff() {
        let (tx, durable, _placement) = start_hub_with_durable_retained(&[]);
        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));

        let send = |seq: u64| {
            tx.send(HubCommand::RemoteRetainedCommit {
                node: NodeId("n".into()),
                topic: "t".into(),
                payload: Bytes::from_static(b"v"),
                qos: 0,
                app: AppProperties::default(),
                seq,
            })
            .unwrap();
        };
        // The duplicate overtakes the commit: pending-dedup swallows it.
        send(7);
        send(7);
        let e = wait_durable_retained(&durable, "t", |_| true).await;
        assert_eq!(e.token(), (0, 1), "committed exactly once");

        // The committed handoff answers: fan-out first, then the commit-gated ack.
        let mut acked = 0;
        for _ in 0..2 {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedUpdate { offset, .. }) => assert_eq!(offset, 1),
                Some(PeerMessage::RetainedCommitAck { seq, token }) => {
                    assert_eq!((seq, token), (7, Some((0, 1))));
                    acked += 1;
                }
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        assert_eq!(acked, 1);

        // A late retransmission (ack was lost): re-acked from `seen`, no recommit.
        send(7);
        match recv_peer(&mut peer).await {
            Some(PeerMessage::RetainedCommitAck { seq, token }) => {
                assert_eq!((seq, token), (7, Some((0, 1))));
            }
            other => panic!("expected the replayed ack, got {other:?}"),
        }
        let e = durable.get("t").await.unwrap().unwrap();
        assert_eq!(e.token(), (0, 1), "the duplicate must not have recommitted");
    }

    /// T8 (owner side): a routed mutation for a group this node does NOT own is
    /// answered with a NACK (`token = None`) and never committed locally — the
    /// sender re-resolves the owner; the ack chain never relays.
    #[tokio::test]
    async fn a_moved_lease_owner_nacks_a_routed_commit() {
        let (tx, durable, placement) = start_hub_with_durable_retained(&["n"]);
        let mut peer = connect_peer(&tx, "n", 1);
        assert!(matches!(
            recv_peer(&mut peer).await,
            Some(PeerMessage::Interest { .. })
        ));
        // A topic the PEER owns: this node must refuse the authority write.
        let topic = {
            let p = placement.read().unwrap();
            (0..100_000)
                .map(|i| format!("dev/{i}/state"))
                .find(|t| p.owner(t) == NodeId("n".into()))
                .expect("some topic is owned by the peer")
        };
        tx.send(HubCommand::RemoteRetainedCommit {
            node: NodeId("n".into()),
            topic: topic.clone(),
            payload: Bytes::from_static(b"v"),
            qos: 0,
            seq: 3,
            app: AppProperties::default(),
        })
        .unwrap();
        match recv_peer(&mut peer).await {
            Some(PeerMessage::RetainedCommitAck { seq, token }) => {
                assert_eq!((seq, token), (3, None), "a moved lease must NACK");
            }
            other => panic!("expected the NACK, got {other:?}"),
        }
        assert!(durable.get(&topic).await.unwrap().is_none());
    }

    /// T8: a NACK re-queues the mutation, and once placement catches up (the old
    /// owner died; this node now owns the group) the sweep retries and commits it
    /// locally — the moved-lease handoff self-heals.
    #[tokio::test(start_paused = true)]
    async fn a_nacked_handoff_re_routes_once_placement_catches_up() {
        let (tx, durable, placement) = start_hub_with_durable_retained(&["n"]);
        let mut peer = connect_peer(&tx, "n", 1);
        let topic = {
            let p = placement.read().unwrap();
            (0..100_000)
                .map(|i| format!("dev/{i}/state"))
                .find(|t| p.owner(t) == NodeId("n".into()))
                .expect("some topic is owned by the peer")
        };
        publish_retained_dynamic(&tx, &topic, b"v");
        let seq = loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedCommit { seq, .. }) => break seq,
                Some(PeerMessage::Interest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        };
        // The peer answers NACK (its lease moved away).
        tx.send(HubCommand::RemoteRetainedCommitAck {
            node: NodeId("n".into()),
            seq,
            token: None,
        })
        .unwrap();
        // Placement catches up: the peer is dead, this node owns the group now.
        placement
            .write()
            .unwrap()
            .observe(&NodeId("n".into()), MemberState::Dead, "", None);
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 2).await;
        let e = wait_durable_retained(&durable, &topic, |_| true).await;
        assert_eq!(
            e.payload, b"v",
            "the NACKed mutation must commit on the new owner"
        );
    }

    /// T8: a lost owner link reclaims the in-flight handoff into the queue; the next
    /// link-up hands it off again — nothing is lost across the flap.
    #[tokio::test]
    async fn a_lost_link_reclaims_the_handoff_and_the_next_link_resends() {
        let (tx, _durable, placement) = start_hub_with_durable_retained(&["n"]);
        let mut peer1 = connect_peer(&tx, "n", 1);
        let topic = {
            let p = placement.read().unwrap();
            (0..100_000)
                .map(|i| format!("dev/{i}/state"))
                .find(|t| p.owner(t) == NodeId("n".into()))
                .expect("some topic is owned by the peer")
        };
        publish_retained_dynamic(&tx, &topic, b"v");
        loop {
            match recv_peer(&mut peer1).await {
                Some(PeerMessage::RetainedCommit { payload, .. }) => {
                    assert_eq!(payload, b"v");
                    break;
                }
                Some(PeerMessage::Interest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        // The link dies unanswered: the handoff is reclaimed, not lost.
        tx.send(HubCommand::PeerDead {
            node: NodeId("n".into()),
        })
        .unwrap();
        // The owner relinks: the mutation is handed off again.
        let mut peer2 = connect_peer(&tx, "n", 2);
        loop {
            match recv_peer(&mut peer2).await {
                Some(PeerMessage::RetainedCommit { payload, .. }) => {
                    assert_eq!(payload, b"v", "the handoff survives the link flap");
                    break;
                }
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
    }

    /// A retained publish for a dynamic (non-static) topic string.
    fn publish_retained_dynamic(tx: &HubTx, topic: &str, payload: &[u8]) {
        tx.send(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::copy_from_slice(payload),
            qos: QoS::AtMostOnce,
            retain: true,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: false,
        })
        .unwrap();
    }

    /// Durable off (no keyspace attached): a retained publish behaves exactly as
    /// ADR 0014 today — the broadcast goes out, and no `RetainedCommit` ever does
    /// (the documented §6 fallback caveat).
    #[tokio::test]
    async fn durable_off_keeps_the_adr_0014_behaviour_with_no_retained_commit() {
        let tx = start_hub();
        let mut peer = connect_peer(&tx, "n", 1);
        publish_retained(&tx, "t", b"v");

        let mut saw_broadcast = false;
        while let Some(msg) = recv_peer(&mut peer).await {
            match msg {
                PeerMessage::Publish { retain, .. } => saw_broadcast = retain,
                PeerMessage::RetainedCommit { .. } => {
                    panic!("durable off must never route a RetainedCommit")
                }
                _ => {}
            }
        }
        assert!(saw_broadcast, "the ADR 0014 broadcast is unchanged");
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
            admission: admission("a"),
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
