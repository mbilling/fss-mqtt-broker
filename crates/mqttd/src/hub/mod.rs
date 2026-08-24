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
//! socket channel** stays unbounded by design — a bounded one would make the hub
//! block or drop control packets for one slow client — but its depth is tracked
//! ([`Outbound`]) so `QoS 0`, which nothing else bounds, is shed rather than
//! accumulated (#123).

use std::sync::atomic::{AtomicUsize, Ordering};

/// Re-exported so the binary wires the hub's own bounds type (issue #241).
pub use crate::backpressure::SubscriberLimits;
use crate::backpressure::{message_bytes, packet_bytes, BacklogBound, BacklogEntry, BacklogQueue};
use bytes::Bytes;
use mqtt_cluster::durable_plane::DurablePlane;
use mqtt_cluster::peer::{ForwardVerdict, PeerMessage, RetainedWireEntry};
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
    Enqueued, MemoryRetainedStore, MemorySessionStore, Offset, RetainedStore, SessionClaim,
    SessionStore, StorageError,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, info, warn};

mod forwarding;
use forwarding::{ForwardKind, ForwardObligation, PendingPublish};
mod delivery;
mod lanes;
#[allow(clippy::wildcard_imports)] // an intra-hub module split (#258): the five
// siblings share one type/state vocabulary by design, and enumerating it would
// re-couple every future hub change to six import lists. Scoped to these files.
use lanes::*;
pub use lanes::{AppendJob, AppendThen, LaneJob, LaneOutcome, LaneWork};
mod policy;
pub use policy::BrownoutAxis;
mod retained;
use retained::{retained_value_id, RetainedMutation, RetainedWindow};

/// Maximum number of queued messages replayed to a reconnecting session at once.
const REPLAY_LIMIT: usize = 10_000;

/// Default outbound Receive Maximum when the client advertised none — effectively
/// unlimited (ADR 0012). v3.1.1 sessions always use this.
const RECEIVE_MAXIMUM_DEFAULT: u16 = u16::MAX;

/// How often the hub sweeps for sessions whose MQTT 5.0 Session Expiry Interval has
/// elapsed (ADR 0009). Second-grained expiry does not need a finer cadence.
const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// How many consecutive sweep ticks a live session must be observed hosted on a node
/// that does NOT own its placement group before that node closes it (issue #284).
///
/// The grace is what separates a session PLACED ACROSS an ownership move from ordinary
/// convergence noise: a lease that lands elsewhere for a tick and comes straight back
/// (assigner rebalance, leader change) must not cost a live client its connection.
/// Two ticks ≈ 1s, which is well under any client's keepalive — the thing this fix
/// exists to beat — and well over the sub-second churn the lease assigner produces.
const MISPLACED_GRACE_TICKS: u8 = 2;

/// The minimum interval between two rehome closes of the SAME session (issue #284).
///
/// The close is only useful if the client's next CONNECT lands somewhere better. A
/// placement that flaps — or a load balancer that keeps returning the client to a
/// non-owning node — would otherwise turn rehome-on-settle into a close loop (and a
/// will-publish loop). Suppressed repeats are counted (`session_rehomes{reason="cooldown"}`)
/// and warned, so a standing flap is loud rather than silent.
const REHOME_COOLDOWN: Duration = Duration::from_secs(30);

/// How many sessions ONE sweep tick may close for rehoming (issue #284 round-2 finding
/// 4). The remainder is deferred to later ticks — the pass re-derives its candidates
/// every tick, so a deferral costs nothing but time.
///
/// The rehome is the LIVE mirror of elastic resize (ADR 0043 P2), where ~1/N of groups
/// change owner in one step: at the documented 5000-session sizing that is ~1700 live
/// sessions whose leases move together. Uncapped, one dispatch closed 400 of them in
/// 25-44 ms of on-loop sweep time, published 400 Last Wills in one breath, and sent 400
/// clients to reconnect on the same instant onto the newly-joined (coldest) node. The cap
/// converts that synchronized storm into a paced drain — and, because the will fires on
/// every rehome close ([MQTT-3.1.2-8]), it is the will-storm cap too. Deferrals are
/// counted (`session_rehomes{reason="deferred"}`) so a mass move is visible, not silent.
const REHOME_CLOSES_PER_TICK: usize = 32;

/// How many sweep ticks between offering every peer our retained digest — the
/// retained set's ANTI-ENTROPY cadence (issue #87).
///
/// Without it, retained convergence rests on a single unacked fan-out frame at commit
/// plus a digest exchanged only at link-up: one dropped or unencodable frame leaves a
/// peer permanently, silently divergent until something flaps the link. The digest is
/// small and idempotent — a peer whose set already matches early-returns and transfers
/// nothing — so re-offering it on a slow cadence costs one frame per peer per period in
/// the steady state and makes ANY missed update self-healing. Same shape as the fix for
/// issue #92: a transient loss must not become a permanent divergence.
const RETAINED_ANTIENTROPY_EVERY: u32 = 30;

/// How many sweep ticks between reconciling persisted expiry deadlines from the durable
/// store (ADR 0009 §3). This inherits deadlines for sessions a takeover handed this node
/// without seeing their disconnect; takeover is rare and the scan is O(owned sessions), so
/// it runs at a coarse cadence rather than every second.
const EXPIRY_RECONCILE_EVERY: u32 = 30;

/// How long a fresh subscription's retained-delivery window stays open (issue #219).
///
/// A retained update is delivered live by its landing node's interest-forward, which
/// needs that node to have SEEN this node's interest advertisement — a fresh
/// SUBSCRIBE's interest takes one gossip hop (sub-second) to reach every peer, and a
/// commit inside that gap was stored but never delivered to the one subscriber whose
/// interest was still in flight. While the window is open, the owner's post-commit
/// fan-out (which reaches every node regardless of interest) doubles as the delivery
/// vehicle for matching LOCAL subscribers, deduped against what the live path already
/// delivered. Three seconds covers the advertisement plus the forward hop under load
/// with margin; after it, the steady-state rule (interest-forward delivers, the apply
/// path never does — the #87 item 3 decision) is back in force, so a window that
/// closes early merely reverts to today's behaviour for the tail of the gap.
const RETAINED_INTEREST_WINDOW: Duration = Duration::from_secs(3);

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

/// Sender for packets destined to a single client's socket, carrying a live count
/// of what is queued but not yet written.
///
/// The channel is unbounded *by design* — a bounded one would make the hub block
/// or drop control packets, and the hub must never stall on one slow client. But
/// unbounded with no visibility was a memory leak with a nice name: `QoS 1/2` is
/// bounded by the flow-control backlog ([`BacklogQueue`]) via the flow-control path,
/// while **`QoS 0` went straight into this channel with no cap, no counter and no
/// shed policy** (#123). `QoS 0` is also exempt from Receive Maximum, so nothing else
/// applied backpressure — a subscriber that stopped reading a busy topic grew this
/// without limit.
///
/// So the depth is tracked here: cheap on the hot path (one relaxed add, one
/// relaxed sub), and it lets the hub shed `QoS 0` — which MQTT defines as
/// at-most-once, so dropping it is legal — while every other packet still flows.
///
/// Since issue #241 the queued **bytes** are tracked alongside the count, because a
/// packet count is not a memory budget under mixed-size traffic: at the 1 MiB default
/// packet ceiling the 10 000-packet cap alone allowed ~10 GiB to sit here.
/// `MQTTD_MAX_OUTBOUND_BYTES` bounds the bytes; the count cap still applies.
///
/// **Exactness class, stated honestly:** this counter is cross-task — the hub adds, the
/// connection's writer subtracts — so it equals the sum over packets the writer has not yet
/// DEQUEUED. `OutboundMeter::drained` subtracts on `recv()`, before the topic-alias rewrite
/// and before the write, so a packet currently being written has ALREADY been subtracted and
/// is excluded. The counter therefore under-counts resident bytes by at most one packet.
/// That direction is the safe one for a gate — it never over-states pressure and so never
/// sheds early — but it is the opposite of what this doc claimed until review caught it.
/// Same semantics [`depth`](Self::depth) has always had, and deliberately *weaker* than the
/// single-owner exactness of [`BacklogQueue::bytes`].
#[derive(Clone, Debug)]
pub struct Outbound {
    tx: mpsc::UnboundedSender<Packet>,
    depth: Arc<AtomicUsize>,
    bytes: Arc<AtomicUsize>,
}

/// The reader half of the outbound accounting: what a connection's writer calls for each
/// packet it dequeues, so the hub's view of that client's queue shrinks.
///
/// One type with one method rather than two loose atomics, so the depth and the byte
/// total structurally cannot be decremented in different places — or one of them
/// forgotten.
#[derive(Clone, Debug)]
pub struct OutboundMeter {
    depth: Arc<AtomicUsize>,
    bytes: Arc<AtomicUsize>,
}

impl OutboundMeter {
    /// One packet left the channel for the socket. Call it with the packet **as
    /// received**, before any outbound rewrite: add and subtract are then the same pure
    /// function of the same immutable packet, which is what makes the counter return to
    /// zero rather than drift.
    pub fn drained(&self, packet: &Packet) {
        self.depth.fetch_sub(1, Ordering::Relaxed);
        let n = packet_bytes(packet);
        // Saturating rather than wrapping: a counter that went momentarily negative
        // would read as ~18 EiB and pin the `QoS` 0 gate shut forever.
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |b| {
                Some(b.saturating_sub(n))
            });
    }
}

impl Outbound {
    /// Wrap a channel, returning the sender and the meter its reader must call as it
    /// drains.
    #[must_use]
    pub fn new(tx: mpsc::UnboundedSender<Packet>) -> (Self, OutboundMeter) {
        let depth = Arc::new(AtomicUsize::new(0));
        let bytes = Arc::new(AtomicUsize::new(0));
        (
            Self {
                tx,
                depth: depth.clone(),
                bytes: bytes.clone(),
            },
            OutboundMeter { depth, bytes },
        )
    }

    /// Queue a packet.
    ///
    /// `false` when the receiver is gone (the client left). Callers all treat that
    /// the same way — the packet is dropped and a Detach is already in flight — so
    /// this is a plain bool rather than a `Result` carrying a whole `Packet` back.
    pub fn send(&self, packet: Packet) -> bool {
        let n = packet_bytes(&packet);
        self.depth.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(n, Ordering::Relaxed);
        if self.tx.send(packet).is_err() {
            // Never queued, so never drained: keep both counts honest.
            self.depth.fetch_sub(1, Ordering::Relaxed);
            self.bytes.fetch_sub(n, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Packets queued for this client but not yet written to its socket.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.depth.load(Ordering::Relaxed)
    }

    /// Accounted bytes queued for this client but not yet written to its socket
    /// (issue #241). See the type docs for what "accounted" means and why this is a
    /// weaker exactness class than the backlog's.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// How many packets may sit unwritten for one client before `QoS 0` is shed.
///
/// Sized so it is unreachable by well-behaved clients and by the `QoS 1/2` paths
/// (which the flow-control backlog already bounds), and only bites a consumer that has
/// genuinely stopped reading. The **byte** dimension is the operator's
/// (`MQTTD_MAX_OUTBOUND_BYTES`, issue #241); this count stays fixed because bytes were
/// the missing dimension, not the count.
pub const MAX_OUTBOUND_QUEUE: usize = 10_000;

/// One WARN for a backlog eviction, naming **which bound fired** and how much it shed
/// (issue #241).
///
/// The bound goes in the log line, not into the metric's label set: the counter stays
/// `publish_dropped{reason="backlog-overflow"}` with exactly its existing labels, so
/// cardinality discipline holds and existing dashboards keep working. One line per push
/// rather than per entry — a byte bound can shed several at once, and `dropped` says how
/// many.
///
/// `bound` is derived from *every* entry that went, not from the first: one arrival can
/// trip the count bound and then still be over the byte bound, and a line that named only
/// the count would send the operator to the wrong knob.
fn warn_backlog_eviction(
    client: &ClientId,
    evicted: &[(BacklogEntry, BacklogBound)],
    bytes_now: usize,
    limits: &SubscriberLimits,
) {
    let by_bytes = evicted
        .iter()
        .filter(|(_, b)| *b == BacklogBound::Bytes)
        .count();
    let bound = match (by_bytes, evicted.len() - by_bytes) {
        (0, _) => BacklogBound::Messages.as_str(),
        (_, 0) => BacklogBound::Bytes.as_str(),
        _ => "messages+bytes",
    };
    warn!(
        client = %client.0,
        dropped = evicted.len(),
        bound,
        bytes = bytes_now,
        cap_messages = limits.max_backlog_messages,
        cap_bytes = ?limits.max_backlog_bytes,
        "flow-control backlog full: evicted the oldest already-acked message(s)"
    );
}

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
/// or answers the refusal — accordingly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// Fanned out (and durably appended where applicable) — ack normally.
    Accepted,
    /// Refused under a stated policy, with a reason the publisher can be TOLD.
    /// Distinct from withholding (the sender is dropped, the connection closes
    /// unacked), which is what a terminal *failure* still does because no reason
    /// code covers "I do not know what happened".
    Refused(PublishRefusal),
}

/// Why the hub refused a publish, and — because the two protocol versions have
/// different vocabularies — how to say so in each.
///
/// Each variant carries **two independent answers**: [`v5_reason`](Self::v5_reason)
/// for MQTT 5, which has a reason byte on PUBACK/PUBREC, and [`v311`](Self::v311)
/// for v3.1.1, which has none and must therefore choose between a plain ack and a
/// close. Adding a *hub* refusal is exactly that: one variant plus its two answers.
/// (Issue #246's ACL denial was delivered WITHOUT a variant here, on purpose: the
/// ACL decides in `conn.rs` before the publish ever reaches the hub, and its answer
/// — v5 `0x87 Not authorized`, v3.1.1 plain ack — is set right at `conn.rs`'s ack
/// arms; a variant here would carry a dead wire code the peer bus never sends.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishRefusal {
    /// A watermark is exceeded, so the durable append a `QoS` ≥ 1 subscriber is
    /// owed was refused (ADR 0041 T5/T11, issue #238). Nothing was stored, so a
    /// retry is the right move — "acked means durable" holds by not acking.
    Brownout,
    /// A retained publish would have created a NEW retained topic beyond the cap
    /// (ADR 0041 T4): nothing was delivered or retained.
    RetainedQuota,
}

impl PublishRefusal {
    /// The MQTT 5.0 PUBACK/PUBREC reason code. A code ≥ 0x80 ends the flow: per
    /// §4.9 and [MQTT-3.3.4-9] it releases the Receive-Maximum slot and makes the
    /// packet id reusable, so a refusal costs O(1) and accumulates no state.
    #[must_use]
    pub fn v5_reason(self) -> u8 {
        match self {
            Self::Brownout | Self::RetainedQuota => mqtt_codec::reason::QUOTA_EXCEEDED,
        }
    }

    /// The v3.1.1 disposition. See [`Refusal311`].
    #[must_use]
    pub fn v311(self) -> Refusal311 {
        match self {
            Self::Brownout => Refusal311::CloseNoAck,
            Self::RetainedQuota => Refusal311::PlainAck,
        }
    }

    /// The bounded metric/log label for this refusal.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Brownout => "brownout",
            Self::RetainedQuota => "retained-quota",
        }
    }

    /// The stable code this refusal travels under on the peer bus
    /// ([`ForwardVerdict::Refused`], proto 7 — 0041-T12, issue #238).
    ///
    /// Exhaustive on purpose in both directions: a new refusal variant cannot ship
    /// without picking a code, and a code cannot be reused. `0` is reserved so a
    /// zero-initialised field is never mistaken for a real refusal.
    #[must_use]
    pub fn wire_code(self) -> u16 {
        match self {
            Self::Brownout => 1,
            Self::RetainedQuota => 2,
        }
    }

    /// The inverse of [`wire_code`](Self::wire_code). `None` for a code this build
    /// does not know — a NEWER peer's refusal. The caller must then WITHHOLD the
    /// ack rather than invent a refusal: `Refused` makes the positive claim
    /// "nothing was stored", which an answer we cannot read does not support.
    #[must_use]
    pub fn from_wire_code(code: u16) -> Option<Self> {
        match code {
            1 => Some(Self::Brownout),
            2 => Some(Self::RetainedQuota),
            _ => None,
        }
    }
}

/// How a refusal is said to an MQTT 3.1.1 publisher, which has no reason byte.
///
/// The choice is not cosmetic: it is whether the refusal is *sayable* as success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal311 {
    /// A deliberate, terminal disposal the publisher gains nothing by retrying —
    /// the plain PUBACK is honest enough, because nothing is owed. (v3.1.1's
    /// retained-quota answer: the message was delivered live, just not retained.)
    PlainAck,
    /// The broker could not take the message, so an ack would be a lie: send none
    /// and close. The client resends on reconnect [MQTT-4.4.0-1], paced by its own
    /// backoff, at zero per-attempt cost to the broker — the same shape as the
    /// pre-existing store-error path.
    CloseNoAck,
}

/// A connection's Will, with the delay the client asked for.
///
/// The delay rides here rather than inside [`Message`] because `Message` is the
/// generic publish type and this timing is specific to a Will — the same reason
/// its `expires_at` is left `None` until the Will is actually published.
#[derive(Debug, Clone)]
pub struct Will {
    /// What to publish.
    pub message: Message,
    /// Will Delay Interval in seconds (§3.1.3.2.2); `0` publishes immediately.
    pub delay_secs: u32,
}

/// A currently-online client connection.
#[derive(Debug)]
struct Online {
    /// Unique per-connection id, used to resolve takeover/disconnect races.
    conn_id: u64,
    /// Channel to this connection's writer.
    tx: Outbound,
    /// Will message published if this connection ends ungracefully.
    will: Option<Will>,
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
    /// `QoS` 2 with a durable offset, staged (issue #242 finding A): the packet id
    /// is allocated (this entry pins it and reserves Receive-Maximum quota) but the
    /// PUBLISH has NOT been sent — its ADR 0057 outbound-id record is being written
    /// in the session's lane. Acks for this id are ignored (the client never saw
    /// it), and the entry is dropped at detach/takeover: the durable copy owns
    /// delivery on reattach.
    AwaitingIdRecord,
}

/// An unacknowledged outbound message.
#[derive(Debug)]
struct PendingOut {
    message: Message,
    state: OutState,
    /// Where this message lives in the session's durable log, if it was recorded
    /// there before being put on the wire (#124). The log is truncated through it
    /// only once the subscriber acknowledges, so a crash mid-flight redelivers it.
    /// `None` for a clean session, a retained replay, or a message the session queue
    /// cap rejected under `reject-newest` — none of which owe a redelivery. (Brownout no
    /// longer appears here: it refuses the publish rather than sending an unrecorded
    /// copy — 0041-T11, issue #238.)
    offset: Option<Offset>,
}

/// Extra lane-channel slots beyond [`LANE_QUEUE_CAP`] reserved for CONTROL jobs —
/// a lane-serialized discard ([`LaneJob::Discard`]/[`LaneJob::Remove`]) or the
/// detach spill ([`LaneWork::Spill`]) (issue #242 finding C). Delivery jobs are
/// capped by `outstanding` before touching the channel, so a saturated lane still
/// admits the control job whose whole purpose is to serialize behind that
/// saturation; only a pile-up beyond this headroom falls back to the loud,
/// documented fallbacks.
const LANE_CONTROL_HEADROOM: usize = 16;

/// What [`Hub::send_qos_publish`] did with one `QoS` > 0 delivery (issue #242
/// finding A).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QosSend {
    /// On the wire (pending entry registered).
    Sent,
    /// Consumed, but NOT on the wire yet: its ADR 0057 outbound-id record is staged
    /// in the session's lane ([`OutState::AwaitingIdRecord`]); the record's
    /// completion sends it and re-drains. The caller must stop draining — a later
    /// send would overtake the staged delivery.
    Staged,
    /// Not consumed: pushed back to the backlog FRONT (ordering holds) — the
    /// packet-id block is spent (reserve running off-loop) or the lane rejected the
    /// record job. The next drain retries.
    Deferred,
}

/// The durability verdict of a fan-out, as it travels back to the ack gate.
///
/// MQTT's acknowledgement is per-PUBLISH, not per-subscriber: there is no way to tell
/// a publisher "stored for two of your three subscribers". So ONE unmet obligation
/// refuses the whole publish — which is why these compose with [`and`](Self::and)
/// rather than being reported individually.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableOutcome {
    /// Every obligation this fan-out took on is met: release the ack.
    Ok,
    /// Refused under a stated policy — answerable per protocol version.
    Refused(PublishRefusal),
    /// A terminal failure. `Failed` DOMINATES a refusal: no reason code honestly
    /// covers it, so the ack is withheld (the connection closes) instead.
    Failed,
}

impl DurableOutcome {
    /// Combine two subscribers' verdicts. Precedence: `Failed` > `Refused` > `Ok`.
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Failed, _) | (_, Self::Failed) => Self::Failed,
            (Self::Refused(r), _) | (Self::Ok, Self::Refused(r)) => Self::Refused(r),
            (Self::Ok, Self::Ok) => Self::Ok,
        }
    }

    /// The peer-bus form of this verdict (proto 7, 0041-T12). Kept next to
    /// [`from_verdict`](Self::from_verdict) so the answering and the receiving side
    /// of the same wire cannot drift apart.
    fn to_verdict(self) -> ForwardVerdict {
        match self {
            Self::Ok => ForwardVerdict::Stored,
            Self::Refused(r) => ForwardVerdict::Refused {
                code: r.wire_code(),
            },
            Self::Failed => ForwardVerdict::Failed,
        }
    }

    /// Interpret a peer's verdict. An unknown refusal code degrades to
    /// [`Failed`](Self::Failed) — withhold, never a fabricated refusal and never an
    /// ack: `Failed` claims nothing about what the peer stored, which is the only
    /// honest thing to say about an answer this build cannot read.
    fn from_verdict(v: ForwardVerdict) -> Self {
        match v {
            ForwardVerdict::Stored => Self::Ok,
            ForwardVerdict::Refused { code } => match PublishRefusal::from_wire_code(code) {
                Some(r) => Self::Refused(r),
                None => Self::Failed,
            },
            ForwardVerdict::Failed => Self::Failed,
        }
    }
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
    ///
    /// Bounded in **both** messages and bytes by the operator's [`SubscriberLimits`]
    /// (issue #241), drop-oldest at either bound (ADR 0012, policy unchanged). The
    /// running byte total lives inside [`BacklogQueue`] with private fields precisely so
    /// this 18 000-line module cannot mutate the queue without adjusting it.
    backlog: BacklogQueue,
    /// Durable log offsets appended for this session and not yet acknowledged by the
    /// subscriber (#124). The log is truncated only through the **contiguous** acked
    /// prefix, so an out-of-order PUBACK cannot discard a message still in flight
    /// behind it.
    outstanding: BTreeSet<Offset>,
    /// The highest offset ever tracked, so a fully drained session truncates its whole
    /// log rather than stalling at the last released offset.
    high_water: Offset,
    /// The last offset already handed to [`SessionStore::ack`], so an advance that would
    /// not move the truncation point costs no store write.
    acked_through: Offset,
    /// Outbound-id record jobs staged in the session's lane and not yet completed
    /// (issue #242 finding A). While non-zero, every `QoS` > 0 send diverts into the
    /// backlog and the drain halts, so nothing overtakes the staged delivery.
    /// Incremented at stage time, decremented ONLY by the record job's completion —
    /// never reset — so the gate is exactly balanced even across reconnects (a stale
    /// completion still drains it).
    records_pending: usize,
    /// A durably-reserved packet-id block base banked by
    /// [`PkidBlockReserved`](HubCommand::PkidBlockReserved) and not yet adopted
    /// (ADR 0007 T9, reserved off-loop since issue #242). `Some(0)` means "refill the
    /// in-memory cursor without adopting" — no durable session, or the reserve
    /// failed: today's fallback semantics, verbatim.
    banked_base: Option<u16>,
    /// A single-flight `reserve_packet_ids` task is in flight for this session.
    reserve_outstanding: bool,
}

impl Default for Inflight {
    fn default() -> Self {
        Self {
            next_pkid: 0,
            block_remaining: 0,
            pending: BTreeMap::new(),
            receive_maximum: RECEIVE_MAXIMUM_DEFAULT,
            backlog: BacklogQueue::default(),
            outstanding: BTreeSet::new(),
            high_water: 0,
            acked_through: 0,
            records_pending: 0,
            banked_base: None,
            reserve_outstanding: false,
        }
    }
}

impl Inflight {
    /// Whether the `QoS` > 0 in-flight quota is exhausted (ADR 0012).
    fn quota_full(&self) -> bool {
        self.pending.len() >= self.receive_maximum as usize
    }

    /// Raise the truncation ceiling to cover `offset` without owing a delivery for it —
    /// for a replayed entry that is dropped rather than sent (expired, or admitted only
    /// by a revoked grant). Without this a session whose whole replay was dropped would
    /// never truncate those entries.
    fn note_offset(&mut self, offset: Offset) {
        self.high_water = self.high_water.max(offset);
    }

    /// Record that `offset` is owed to the subscriber until it acknowledges (#124).
    fn track(&mut self, offset: Offset) {
        self.note_offset(offset);
        self.outstanding.insert(offset);
    }

    /// The subscriber has acknowledged the message stored at `offset`.
    fn release(&mut self, offset: Offset) {
        self.outstanding.remove(&offset);
    }

    /// Advance the durable truncation point to the contiguous acked prefix, returning it
    /// only if it actually moved (so a redundant `ack` never reaches the store).
    fn advance_ack(&mut self) -> Option<Offset> {
        let safe = match self.outstanding.iter().next() {
            // Everything strictly below the oldest still-owed message is settled.
            Some(oldest) => oldest.saturating_sub(1),
            // Nothing owed: the whole log is settled.
            None => self.high_water,
        };
        (safe > self.acked_through).then(|| {
            self.acked_through = safe;
            safe
        })
    }

    /// Append to the flow-control backlog, evicting the oldest entries until BOTH the
    /// message and the byte bound hold (drop-oldest, ADR 0012, issue #241). Returns the
    /// evicted entries oldest-first with the bound that evicted each — the caller must
    /// [`release`](Self::release) every offset, since nothing will deliver those
    /// messages and an offset owed forever would stop the log ever being truncated.
    ///
    /// The byte bound may evict several entries where the count bound evicts one.
    fn push_backlog(
        &mut self,
        entry: BacklogEntry,
        limits: &SubscriberLimits,
    ) -> Vec<(BacklogEntry, BacklogBound)> {
        self.backlog.push_back_capped(entry, limits)
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
    will: Option<Will>,
    /// Channel the hub uses to deliver packets to this client.
    outbound: Outbound,
    /// Reply channel the connection awaits before its CONNACK.
    reply: oneshot::Sender<AttachOutcome>,
}

/// One atomic cut of this node's retained state for an export (ADR 0062): every retained
/// topic's message, paired with the `(epoch, offset)` convergence token it was applied from
/// (`None` under durable-off). An empty payload is a live tombstone.
pub type RetainedExportCut = Vec<(Message, Option<(u64, u64)>)>;

/// The hub's answer to [`HubCommand::RetainedExportSnapshot`]: the cut, or the reason there
/// is none. An error must NOT read as "this node holds no retained state" — that would ship
/// an export with every retained topic silently missing, which is the half-true backup the
/// whole feature exists not to produce. The exporter fails the run on `Err` and writes
/// nothing, so the last-success timestamp does not move and the RPO alert fires.
pub type RetainedExportAnswer = Result<RetainedExportCut, String>;

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
        /// Will message to publish if the connection ends ungracefully, with the
        /// delay the client asked for (§3.1.3.2.2).
        will: Option<Will>,
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
    /// Internal: a session append lane finished one job (issue #242 / ADR 0061); run
    /// its on-loop continuation — durable-writes bookkeeping, the post-durable live
    /// send, and the gate/verdict resolution. Not sent by connections — lane workers
    /// post it to the hub.
    AppendDone {
        /// The job as submitted (all decisions frozen at plan time).
        job: Box<AppendJob>,
        /// What the store did.
        outcome: LaneOutcome,
    },
    /// Internal: the off-loop packet-id block reservation finished (ADR 0007 T9 /
    /// issue #242 finding A) — bank the base and drain the deliveries deferred on
    /// it. Not sent by connections — the spawned single-flight reserve task posts
    /// it to the hub.
    PkidBlockReserved {
        /// The session the block was reserved for.
        client: ClientId,
        /// The store's answer: the persisted high-water before the reservation
        /// (0 = no durable session), or the error the in-memory fallback absorbs.
        result: Result<u16, mqtt_storage::StorageError>,
    },
    /// Add subscriptions (filter + granted `QoS`) for a client.
    Subscribe {
        /// The subscribing client.
        client: ClientId,
        /// Topic filters being subscribed to, with their granted `QoS`.
        filters: Vec<(String, QoS)>,
        /// The subset of `filters` subscribed with **No Local** set (#198), so the hub can
        /// suppress echoing a client's own publish back to it on those filters.
        no_local_filters: Vec<String>,
        /// The packet's Subscription Identifier (issue #266). One per SUBSCRIBE,
        /// applying to EVERY filter in it (§3.8.2.1.2) — a per-packet option, never
        /// a per-filter vector. `None` for v3.1.1 and for a v5 SUBSCRIBE carrying
        /// no identifier (replace-don't-merge then removes any stored id).
        sub_id: Option<u32>,
        /// The subset of `filters` subscribed with **Retain As Published** set (#198), so a
        /// message matching them keeps the RETAIN flag it was published with.
        rap_filters: Vec<String>,
        /// Per-filter **Retain Handling** (#198, MQTT 5 §3.8.3.1), parallel to `filters`:
        /// `0` send retained at subscribe (the default), `1` send only if the subscription did
        /// not already exist, `2` never send retained at subscribe. Empty = all `0` (internal
        /// callers and v3.1.1, which has no subscription options).
        retain_handling: Vec<u8>,
        /// When present, the hub answers with one flag per filter — `false` for a
        /// filter the subscription quota denied (ADR 0041 T3) — BEFORE any
        /// retained replay, so the connection's SUBACK precedes the replayed
        /// publishes. `None` skips the quota round-trip (internal callers).
        reply: Option<oneshot::Sender<Vec<bool>>>,
    },
    /// Set the per-client quotas (ADR 0041 T3). Sent once at startup, before any
    /// listener accepts.
    SetQuotas(Quotas),
    /// Enter or leave **brownout** on one axis (ADR 0041 T5 disk, T8 memory): sent by
    /// the corresponding watcher on watermark transitions. Under brownout, growth
    /// writes are refused with the quota behaviors while maintenance continues.
    ///
    /// Per-axis, because the axes are independent watchers. A single flag would let
    /// whichever watcher polled last decide: disk dropping under its watermark would
    /// lift a brownout that memory pressure is still asking for. The effective state is
    /// the OR — brownout while ANY axis is over.
    SetBrownout {
        /// Which watermark moved (`"disk"`, `"memory"`).
        axis: BrownoutAxis,
        /// Whether that axis is now over its watermark.
        on: bool,
    },
    /// Remove subscriptions for a client.
    Unsubscribe {
        /// The unsubscribing client.
        client: ClientId,
        /// Topic filters being removed.
        filters: Vec<String>,
        /// One answer per filter, in request order: `true` when a subscription
        /// existed and was removed, `false` when there was nothing to remove —
        /// the v5 UNSUBACK's `0x00` / `0x11 No subscription existed` split
        /// ([MQTT-3.11.3-1], issue #290). `None` for callers that do not answer
        /// a client (tests, internal sweeps).
        reply: Option<oneshot::Sender<Vec<bool>>>,
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
        /// The publishing client (#198): a message is not echoed back to it on any of its
        /// **No Local** subscriptions. `None` for internally-generated publishes (a Will, a
        /// peer-forwarded message) — those have no local publisher to exclude.
        publisher: Option<ClientId>,
    },
    /// Write one **restored** retained value as retained state (ADR 0062, issue #249):
    /// commit it through the topic's group lease-owner and warm the caches, with **no
    /// ordinary fan-out to subscribers**.
    ///
    /// This exists because `Publish { retain: true }` is the wrong tool for a restore. A
    /// publish is *two* facts — "this is the topic's retained value" and "deliver this to
    /// every matching subscriber now" — and a restore only owns the first. The second
    /// reaches durable OFFLINE sessions, whose queues the hub appends to with no client
    /// listener bound at all, so a restore that re-published its retained set gave every
    /// restored session one spurious queued message per matching retained topic per node —
    /// messages that were in no export, at whatever `QoS` the subscription granted. This
    /// command carries only the first fact: the durable authority commit (ADR 0037 §1/§5),
    /// then the token-carrying fan-out to peer CACHES. Restored sessions are untouched, and
    /// "the restored queues equal the exported queues" becomes an equality a test can state.
    ///
    /// With durable retained off (ADR 0014 best-effort) it writes the node-local retained
    /// store directly — every node imports the same set, so the caches still converge.
    RestoreRetained {
        /// Destination topic.
        topic: String,
        /// The retained payload (empty = a clear, MQTT-3.3.1-10).
        payload: Bytes,
        /// The `QoS` the value was published at.
        qos: QoS,
        /// MQTT 5.0 Message Expiry Interval in seconds, if the exported value had a
        /// remaining deadline.
        message_expiry: Option<u32>,
        /// The publisher's forwardable MQTT 5 application properties (ADR 0030).
        app: AppProperties,
        /// Signalled once the value is durably the topic's retained state (or refused).
        done: oneshot::Sender<PublishOutcome>,
    },
    /// Snapshot every retained topic this node holds **with its `(epoch, offset)`
    /// convergence token**, for an online export (ADR 0062).
    ///
    /// Taken here rather than off the `RetainedStore` handle because the value and its token
    /// live in two places — the cache and the hub's token map — and pairing a value from one
    /// instant with a token from another would produce a file whose ordering evidence is
    /// wrong, which is worse than a file with none. The hub is a single-threaded actor, so
    /// one dispatch cannot interleave with a retained mutation: this IS the atomic cut the
    /// export claims. Live tombstones are included as empty-payload entries, so a topic
    /// cleared after another node's export is not resurrected by the union.
    RetainedExportSnapshot {
        /// Each retained topic's message and its committed token, when there is one — or
        /// the reason the snapshot could not be taken.
        done: oneshot::Sender<RetainedExportAnswer>,
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
        /// A Session Expiry Interval carried on the client's DISCONNECT, which
        /// overrides the one agreed at CONNECT (§3.14.2.2.2, issue #298). `None`
        /// leaves the agreed interval in force — which is every other way a
        /// connection can end.
        session_expiry_override: Option<u32>,
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
        /// The link's control lane: drained before `tx` by the link pump, for
        /// raft RPCs and replication acks (issue #358).
        ctl: PeerOutbound,
        /// The remote leaf certificate's serial from the mTLS handshake
        /// (ADR 0040 T4); `None` on a plaintext mesh.
        cert_serial: Option<Vec<u8>>,
        /// The peer-bus protocol version this link negotiated (ADR 0038), so the hub
        /// can choose per-link between a proto-6 and a proto-7 frame (0041-T12).
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
        /// Absolute expiry deadline (Unix epoch seconds; issue #227). `None` = never.
        expires_at: Option<u64>,
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
        /// The absolute expiry deadline the commit carried (issue #227).
        expires_at: Option<u64>,
        /// Whether the mutation came from a RESTORE — see [`RetainedMutation::restore`]:
        /// the committed value warms every cache as usual, and NOTHING is delivered.
        restore: bool,
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
        /// The committed absolute expiry deadline (issue #227). `None` = never.
        expires_at: Option<u64>,
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
    /// A peer's proto-7 answer to a forwarded publish OR a forwarded shared
    /// delivery (0041-T12, issue #238): the superset of
    /// [`RemotePublishAck`](Self::RemotePublishAck) that can also say "refused under
    /// a stated policy, nothing stored", which the origin turns into an answer its
    /// publisher can act on (`0x97`) instead of a withheld ack.
    RemotePublishVerdict {
        /// The peer that answered.
        node: NodeId,
        /// The forward sequence being answered.
        seq: u64,
        /// What the peer did with the forward.
        verdict: ForwardVerdict,
    },
    /// An **answerable** targeted shared delivery from a peer (0041-T12, issue #238):
    /// like [`RemoteSharedDeliver`](Self::RemoteSharedDeliver), but the outcome is
    /// answered with a [`PeerMessage::PublishVerdict`] so the origin's publisher ack
    /// is gated on this node actually taking the message. Arrives only from a proto-7
    /// sender.
    RemoteSharedDeliverAcked {
        /// The peer the forward arrived from (where the verdict is sent).
        node: NodeId,
        /// The sender's forward sequence (correlates the verdict).
        seq: u64,
        /// The chosen local group member.
        client: ClientId,
        /// Destination topic.
        topic: String,
        /// Application payload.
        payload: Bytes,
        /// Already-downgraded delivery `QoS`.
        qos: QoS,
        /// The publisher's Message Expiry Interval (seconds). `None` = no expiry.
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
    /// **Internal**: the off-loop inherited-session scan finished (ADR 0042 T9,
    /// exhibit ⑥) — every session the durable store holds, with subscriptions and
    /// expiry deadline. The loop materializes the OWNED, not-yet-known ones into
    /// the routing table so a publish arriving before the client's first re-attach
    /// enqueues instead of routing to nothing.
    InheritedSessions {
        /// `(client, subscriptions, expiry deadline)` per stored session.
        sessions: Vec<(ClientId, Vec<Subscription>, Option<u64>)>,
        /// Whether the scan saw everything (0043-P4 exhibit ②): `false` when a
        /// key was skipped for a transient reason — the view may be missing
        /// sessions, so it must not settle the interest-authoritative flag.
        complete: bool,
    },
    /// Liveness probe (the health endpoint): the hub replies as soon as the actor
    /// loop dequeues this command, proving the loop is draining and not wedged.
    Ping {
        /// Replied to with `()` when the loop reaches this command.
        reply: oneshot::Sender<()>,
    },
}

impl HubCommand {
    /// The COARSE `{command}` label for `mqttd_hub_dispatch_seconds` (issue #242):
    /// a bounded ~6-value class, never per-variant (cardinality discipline,
    /// ADR 0020 §3). `AppendDone` counts as `publish` — it is the publish path's
    /// completion half, and any store await smuggled back into it must show up in
    /// the same series operators alert on.
    fn class(&self) -> &'static str {
        match self {
            Self::Attach { .. } | Self::SessionRecovered { .. } => "attach",
            Self::Publish { .. } | Self::AppendDone { .. } | Self::PkidBlockReserved { .. } => {
                "publish"
            }
            Self::PubAck { .. } | Self::PubRec { .. } | Self::PubComp { .. } => "ack",
            Self::Subscribe { .. } | Self::Unsubscribe { .. } => "subscribe",
            Self::SetQuotas(_)
            | Self::SetBrownout { .. }
            | Self::Detach { .. }
            | Self::Evict { .. }
            | Self::SweepIdentities(_)
            | Self::AttachAuthorizer(_)
            | Self::Ping { .. } => "control",
            _ => "cluster",
        }
    }
}

/// A live session seen hosted on a node that does not own its placement group
/// (issue #284) — the grace counter and last-close time behind rehome-on-settle.
#[derive(Debug, Default)]
struct Misplaced {
    /// Consecutive sweep ticks the condition has held, capped at
    /// [`MISPLACED_GRACE_TICKS`]. Reset to 0 when the condition clears, and after a
    /// close — the next episode earns its own grace.
    ticks: u8,
    /// When this session was last closed to make it relocate — the
    /// [`REHOME_COOLDOWN`] anchor. `None` until the first close. Outlives the
    /// condition clearing, or a re-attach to the same non-owning node would be closed
    /// again at once and the cooldown would bound nothing.
    last_kick: Option<Instant>,
    /// Whether the CURRENT non-actionable episode (unrelocatable, or cooling down) has
    /// already been warned and counted. A standing condition this node cannot resolve
    /// must not warn — or count — once a second.
    noted: bool,
    /// Whether this session has already been counted as deferred over
    /// [`REHOME_CLOSES_PER_TICK`] in the CURRENT deferral episode. The pass re-derives
    /// its candidates every tick, so without this the counter would report deferral
    /// EVENTS: an n-session move increments it ~n²/(2·cap) times, and the operator sizing
    /// a drain from it overestimates the backlog by more than an order of magnitude.
    /// Cleared with `ticks`/`noted` — on the close, and when candidacy ends.
    deferred: bool,
}

/// A connected peer node's link.
#[derive(Debug)]
struct Peer {
    conn_id: u64,
    tx: PeerOutbound,
    /// The link's CONTROL lane (issue #358): raft RPCs and replication acks jump
    /// the bulk queue here, so a heartbeat is never behind a retained snapshot.
    /// Same TCP connection — the pump drains this receiver first (`biased`).
    ctl: PeerOutbound,
    /// The peer-bus protocol version this LINK negotiated (ADR 0038). Frame choice
    /// is gated on it, not on what this build supports: the codec is strict, so
    /// sending a variant the peer does not know leaves it with an unknown variant
    /// index → `PeerCodecError::Serde` → `io::Error` → link teardown and redial, i.e.
    /// a flap loop. See [`Hub::peer_proto`].
    proto: u32,
    /// The remote leaf certificate's serial (big-endian bytes) from the link's
    /// mTLS handshake — the fact a cluster-CRL revocation sweep re-checks
    /// (ADR 0040 T4). `None` on a plaintext mesh.
    cert_serial: Option<Vec<u8>>,
    /// Whether this peer has sent an interest snapshot since the link formed
    /// (0043-P4 exhibit ②). A freshly-booted peer SUPPRESSES its snapshot until
    /// its own routing view is authoritative — so until one arrives, this node
    /// cannot distinguish "the peer routes nothing" from "the peer has not
    /// finished recovering what it routes", and a gated ack must not conclude
    /// "nobody is owed this" (see [`Hub::mesh_settled`]).
    interest_synced: bool,
}

/// Record one MQTT 5 subscription option (#198) for `client` over the filters it just
/// subscribed: `set` names the filters that carry the option; every other (re)subscribed
/// filter has it CLEARED, because a re-subscribe replaces a subscription's options
/// [MQTT-3.8.4-3]. The client's entry is dropped when no filter carries the option.
fn record_sub_option(
    map: &mut HashMap<ClientId, HashSet<String>>,
    client: &ClientId,
    filters: &[(String, QoS)],
    set: &[String],
) {
    let on: HashSet<&String> = set.iter().collect();
    let entry = map.entry(client.clone()).or_default();
    for (f, _) in filters {
        if on.contains(f) {
            entry.insert(f.clone());
        } else {
            entry.remove(f);
        }
    }
    if entry.is_empty() {
        map.remove(client);
    }
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
// Several independent one-way/observable state flags (brownout, scan-in-flight,
// interest authority, cluster-configuredness) — orthogonal facts about one actor,
// not an encoded state machine, so bools are the honest representation.
#[allow(clippy::struct_excessive_bools)]
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
    /// Wills held back by a Will Delay Interval (§3.1.3.2.2, issue #299), keyed by
    /// client, each with the instant it becomes due. The same sweep that expires
    /// sessions publishes them — one tick, not two.
    ///
    /// A monotonic [`Instant`], NOT the `Clock`'s epoch seconds, and the difference
    /// is observable: `now_epoch_secs` truncates, so a disconnect at `t.9` would
    /// arm `floor(t) + delay` and fire nearly a second EARLY — CI caught exactly
    /// that (4 s asked, 2.8 s measured). Session expiry uses epoch seconds because
    /// its deadline is persisted and must survive a takeover; this one is
    /// node-local and never written down, so it has no such constraint and can
    /// simply be accurate. The 1 s sweep cadence is then the only error, and it is
    /// in the safe direction — never early.
    ///
    /// **Node-local and in-memory, deliberately.** If this node dies inside the
    /// window the delayed Will is lost, and a session that relocates mid-window
    /// does not take it along. That is the same class as the in-memory outbound
    /// queue, and the honest first cut: firing a delayed Will from a node that no
    /// longer owns the session would be worse than not delaying at all. Recorded
    /// in `docs/TEST-PLAN.md`'s policy register rather than left to be discovered.
    pending_wills: HashMap<ClientId, (Will, Instant)>,
    /// Sweep-tick counter that paces the durable expiry reconcile (ADR 0009 §3).
    expiry_reconcile_tick: u32,
    /// Sweep-tick counter driving the retained anti-entropy cadence (issue #87),
    /// kept separate from the expiry counter so neither's phase perturbs the other.
    retained_antientropy_tick: u32,
    /// Per-client subscription filters with their granted `QoS`.
    subs_by_client: HashMap<ClientId, HashMap<String, QoS>>,
    /// Filters each client subscribed with **No Local** set (MQTT 5 §3.8.3.1, #198): a
    /// message is not delivered back to the connection that published it on such a filter —
    /// the unforgeable loop-prevention primitive the boundary bridge relies on (ADR 0059/0025).
    no_local: HashMap<ClientId, HashSet<String>>,
    /// Per-session Subscription Identifiers (issue #266, §3.8.2.1.2): `filter -> id`
    /// for every subscription made with one. Absence means the subscription carries
    /// no id (obligation: a match on only id-less subscriptions attaches NO property).
    /// Keyed by the FULL filter string (`$share/...` included), like
    /// [`subs_by_client`](Self::subs_by_client). Replace-don't-merge
    /// [MQTT-3.8.4-3]: a re-SUBSCRIBE of the filter with a different or ABSENT id
    /// overwrites or removes the entry.
    sub_ids: HashMap<ClientId, HashMap<String, u32>>,
    /// Filters each client subscribed with **Retain As Published** set (MQTT 5 §3.8.3.1,
    /// #198): a message forwarded because it matched such a filter keeps the RETAIN flag it
    /// was published with, instead of the flag being cleared [MQTT-3.3.1-9]. This is what lets
    /// a re-forwarder (the boundary bridge) carry *live* retained state across (#189).
    retain_as_published: HashMap<ClientId, HashSet<String>>,
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
    /// ADR 0072: honor the `mqttd-durability` user property on PUBLISH — the
    /// operator's `MQTTD_ALLOW_RELAXED_PUBLISH` opt-in. Off = the property is
    /// ignored and every ack keeps its full quorum-durable meaning.
    allow_relaxed_publish: bool,
    /// ADR 0073: the cluster-wide scale-out ownership capability. `Some((flag,
    /// enabled))` on a cluster node: `enabled` is the operator's
    /// `durable.ownership_domain = "members"` choice; `flag` is the shared verdict
    /// this hub recomputes each sweep — true iff enabled AND every placement
    /// member's last-negotiated peer proto >= [`mqtt_cluster::peer::PROTO_OWNERSHIP_DOMAIN`].
    /// The durable driver reads it to widen ownership; the plane reads it for
    /// readiness; /statusz reports it.
    ownership_domain: Option<(Arc<std::sync::atomic::AtomicBool>, bool)>,
    /// The last peer-bus proto each member NEGOTIATED, surviving link flaps
    /// (updated on link attach, removed only on confirmed death) — a transient
    /// redial must not flap the ownership domain and mass-migrate 256 groups
    /// (ADR 0073). Distinct from `peers` (live links only).
    known_peer_protos: HashMap<NodeId, u32>,
    /// Retained message storage. An `Arc` (not a `Box`) so the online backup task can
    /// hold a READ handle on the same store the hub writes (ADR 0062) — one redb handle,
    /// borrowed, never a second open on a dir this process already locks (ADR 0061).
    retained: Arc<dyn RetainedStore>,
    /// The durable retained keyspace (ADR 0037), when durable sessions are on: the
    /// owner-routed, quorum-committed **authority** for retained state, written in
    /// addition to the local cache above. `None` (durable off) keeps ADR 0014
    /// best-effort behaviour unchanged.
    durable_retained: Option<Arc<dyn DurableRetained>>,
    /// The live authorizer for resume-time grant re-checks (ADR 0040 T3); `None`
    /// (no re-check) until [`HubCommand::AttachAuthorizer`] arrives — harnesses
    /// without a reloadable policy keep today's restore-as-persisted behavior.
    authz: Option<AuthzWatch>,
    /// Brownout (ADR 0041 T5 disk, T8 memory): set while **any** watched resource is
    /// over its watermark — the stores' on-disk size above `MQTTD_STORE_MAX_BYTES`, or
    /// process RSS above `MQTTD_MEMORY_MAX_BYTES`. Growth writes (new retained topics,
    /// new sessions, offline enqueues) are refused with the quota behaviors; acks,
    /// deletes, expiry, and resumes continue — read-mostly, not read-only, and never
    /// the disk-full cliff.
    ///
    /// This is the OR of [`Self::brownout_axes`]; the per-axis state is kept separately
    /// so one watcher lifting its own pressure cannot clear another's.
    brownout: bool,
    /// Which axes are currently over their watermark. Empty = no brownout.
    brownout_axes: HashSet<BrownoutAxis>,
    /// Per-client quotas (ADR 0041 T3); default = uncapped.
    quotas: Quotas,
    /// The `(epoch, offset)` convergence token each cached retained topic was applied
    /// at (ADR 0037 §3): a fan-out/back-fill value is applied only when its token
    /// exceeds the held one — monotonic per topic, idempotent, order-insensitive. A
    /// cleared topic keeps its tombstone's token here so a staler value cannot
    /// resurrect it. Only populated under durable retained; bounded by topic count
    /// (like the cache itself).
    retained_tokens: HashMap<String, (u64, u64)>,
    /// Fresh-subscription retained-delivery windows (issue #219), per client. Opened
    /// (or refreshed) by an ordinary SUBSCRIBE and swept after
    /// [`RETAINED_INTEREST_WINDOW`]: while open, a committed retained fan-out is
    /// delivered to this client's matching subscriptions by the apply path, deduped
    /// through the window's ledger. Empty in the steady state, so the apply path's
    /// cost is one `is_empty()` check.
    retained_windows: HashMap<ClientId, RetainedWindow>,
    /// Per peer, the wall-clock second our retained digests last MATCHED theirs
    /// (issue #229): the observable "this pair has converged" instant the tombstone
    /// reap gates on. Wall clock (the clock seam), not `Instant`, so the gate is
    /// testable and comparable with tombstone observation times.
    retained_digest_matched_at: HashMap<NodeId, u64>,
    /// Per tombstoned topic, the wall-clock second this node first held the clear
    /// (issue #229). Present exactly while the fence is a tombstone (a re-set value
    /// discharges the entry); the reap discharges it once every roster member's
    /// digest has matched since.
    retained_tombstone_observed_at: HashMap<String, u64>,
    /// Whether any retained value currently held MIGHT carry an expiry deadline
    /// (issue #227) — flipped on when one lands, cleared by the reap scan finding
    /// none. Keeps the per-tick reap pay-for-use: a broker with no expiring
    /// retained values (every v3.1.1 deployment) never scans.
    retained_may_expire: bool,
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
    /// Forward seq → pending publish id, for answer resolution.
    forward_index: HashMap<u64, u64>,
    /// Count of durable session appends this hub has completed successfully.
    /// Incremented only by the `AppendDone` handler (issue #242) — the one place a
    /// lane append's outcome re-enters the loop — which also sets the completing
    /// publish's [`PendingPublish::stored`] directly (issue #238): only a publish
    /// stored NOWHERE may be answered `Refused`, because that answer asserts
    /// "nothing was stored, retry" — a falsehood for a message already durably owed
    /// to a subscriber, whose retry would then duplicate it there.
    durable_writes: u64,
    /// Whether an off-loop inherited-session scan is running (ADR 0042 T9,
    /// exhibit ⑥) — one at a time.
    inherited_scan_inflight: bool,
    /// Whether this hub's interest gossip is AUTHORITATIVE (0043-P4 exhibit ②):
    /// a fresh hub's routing table is empty until its boot scan materializes the
    /// durable sessions it owns, and gossiping that emptiness ERASES what peers
    /// still correctly know from before a fast restart (one quicker than SWIM
    /// death confirmation — no membership change, no takeover window anywhere).
    /// Until the first COMPLETE scan lands over a whole mesh, no interest
    /// snapshot is sent; peers keep their prior knowledge, their forwards keep
    /// flowing, and this node answers ones it cannot yet serve with a retriable
    /// refusal instead of a void OK.
    interest_authoritative: bool,
    /// Sweep ticks spent suppressed (liveness backstop: a node whose scans never
    /// complete — quorum lost for good — eventually gossips its live-only
    /// interest, loudly, rather than isolating its live clients forever).
    interest_suppressed_ticks: u32,
    /// Whether the LAST inherited-session scan saw everything (0043-P4 exhibit
    /// ②): while it skipped keys — a restarted owner whose lease has not been
    /// reassigned back yet, a group mid-recovery — this node's routing view is
    /// missing sessions it owns, and a gated ack must not conclude "nobody is
    /// owed this" no matter how many window ticks have elapsed. The settle
    /// window closes on this OBSERVABLE state, never on time alone.
    last_scan_complete: bool,
    /// Whether peer networking is configured (set at startup, before `run`):
    /// the stable half of [`clustered`](Self::clustered) — a restarted cluster
    /// node must not be mistaken for a standalone broker while SWIM re-learns
    /// its members (0043-P4 exhibit ②).
    cluster_configured: bool,
    /// Sweep ticks remaining of eager takeover reconciliation: set on `PeerDead`
    /// so inherited sessions materialize within seconds, not on the slow
    /// [`EXPIRY_RECONCILE_EVERY`] cadence.
    takeover_reconcile_ticks: u8,
    /// The placement member set as of the last sweep tick (ADR 0043 P2). A change
    /// — growth especially: `PeerDead` already arms the window for shrink — means
    /// group ownership moved, so the takeover window re-arms: moved sessions
    /// materialize eagerly on their NEW owners and un-materialize on their old
    /// ones, instead of waiting for first touch.
    known_members: BTreeSet<NodeId>,
    /// The [`Placement::ownership_epoch`] the SWEEP's window-armer last saw (issue
    /// #294) — distinct from [`ownership_epoch_seen`](Self::ownership_epoch_seen),
    /// which gates the rehome candidate scan. `None` until the first sweep, so boot
    /// observes the starting epoch without arming a spurious window on top of the
    /// boot scan's own.
    sweep_epoch_seen: Option<u64>,
    /// Live sessions observed hosted here for a group this node does not own (issue
    /// #284), with how many consecutive sweep ticks the condition has held and when
    /// this session was last closed for it. Entries are dropped the moment the
    /// condition clears, so the map is bounded by the session count and empty in the
    /// steady state.
    ///
    /// Non-empty is also the candidate pass's ESCAPE HATCH from its ownership-version
    /// skip, which is why `finish_attach` seeds an entry here for a persistent session
    /// that arrives on a non-owning node: a session that becomes misplaced by ARRIVING
    /// moves no lease, so nothing else would ever make the pass look at it.
    misplaced: HashMap<ClientId, Misplaced>,
    /// The [`Placement::ownership_epoch`] the last rehome candidate scan ran at (issue
    /// #284 round-2 finding 3). While it is unchanged no committed lease has moved and no
    /// membership relevant to ownership has changed, so the `O(online sessions)` pass is
    /// skipped entirely — the steady-state cost of rehome-on-settle is one `u64` read
    /// under a short lock per tick, not a scan.
    ownership_epoch_seen: Option<u64>,
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
    /// Per-session durable-append lanes (issue #242 / ADR 0061): the publish path's
    /// store writes run in these workers, off the command loop, so one placement
    /// group's degraded followers stall only its own sessions' appends — never every
    /// client on the node. Spawned on first submission; reaped by the sweep when idle.
    append_lanes: HashMap<ClientId, AppendLane>,
    /// The lane workers themselves, owned by the hub so their lifetime is the hub's.
    ///
    /// This ownership is load-bearing, not tidiness. A worker holds an `Arc` of the
    /// session store, and a redb store is exclusive-locked by its handle, so a worker
    /// that outlives the node keeps the data dir locked and the next start fails with
    /// "Database already open. Cannot acquire lock."
    ///
    /// A node always stops by having its hub task ABORTED (or by the process exiting):
    /// the loop's `None` arm cannot fire, because the hub holds a clone of its own
    /// command sender. Aborting drops `self`, and dropping a
    /// [`JoinSet`](tokio::task::JoinSet) aborts every task in it — so every store
    /// handle is released at once, exactly as the OS reclaims a killed process's files.
    /// An in-flight append is abandoned, which is honest: its publisher's ack was
    /// withheld, so nothing was falsely promised, and the alternative (finishing a
    /// quorum append that cannot reach quorum, up to the 5s RPC bound) is what leaked
    /// the lock past the stop and broke restarts.
    ///
    /// Before ADR 0061 the append was awaited INLINE on the loop, so an abort killed it
    /// for free; moving it off-loop is what made this ownership explicit. Found by CI:
    /// `cluster_stress::a_full_cluster_stop_start_recovers_every_acked_fact` restarts a
    /// stopped cluster over the same data dirs and failed on the held lock, where every
    /// local run had won the race.
    owned_tasks: tokio::task::JoinSet<()>,
    /// ADR 0074: sender into the truncate flusher — the task that coalesces
    /// per-session ack watermarks and flushes them to the store OFF the hub
    /// loop, so a subscriber ack never waits a truncate round-trip. `None`
    /// until [`run`](Self::run) spawns the flusher.
    truncate_tx: Option<mpsc::UnboundedSender<(ClientId, Offset)>>,
    /// Peer verdict aggregates for forwards whose fan-out submitted lane jobs
    /// (issue #242): `(origin, seq)` → what is still owed before the verdict can be
    /// answered. Entries drain via [`HubCommand::AppendDone`].
    remote_append_pending: HashMap<(NodeId, u64), RemoteAppendGate>,
    /// Prometheus metrics (ADR 0020), when enabled. Updated on the publish/deliver paths.
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    /// Shared brownout state for the `/statusz` body (ADR 0054), flipped alongside
    /// the internal flag on [`HubCommand::SetBrownout`] transitions.
    brownout_status: Option<Arc<crate::health::BrownoutStatus>>,
    /// Wall-clock source for absolute message-expiry deadlines (ADR 0009 §3).
    /// Injectable so expiry can be tested without real time passing.
    clock: Arc<dyn crate::clock::Clock>,
    /// The operator's per-subscriber in-memory bounds (issue #241, ADR 0041 T10). Set
    /// once before [`run`](Self::run); a reload reports `limits` as requires-restart
    /// (ADR 0041 §6), so this is never swapped mid-flight. `Default` is exactly the
    /// former hard-coded behaviour.
    subscriber_limits: SubscriberLimits,
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
/// frame limit (16 MiB, `mqtt_cluster::peer`), with headroom for codec framing — a
/// frame at the limit would be rejected by the receiver and tear down the link.
const RETAINED_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// The per-node bound on retained mutations queued awaiting their authority commit
/// (ADR 0037 §5, queue-until-heal). At the bound the **oldest** mutation is dropped,
/// loudly (`retained_queue_dropped_total`) — the explicit CP trade: a partition that
/// outlasts the queue costs the oldest minority-side retained writes, never silent
/// divergence. Count-bounded (like the session queue cap): retained values are
/// last-value device state, typically small and infrequent.
const RETAINED_QUEUE_CAP: usize = 1024;

/// The bound on publishes whose acknowledgement awaits cluster-wide durability
/// (ADR 0042 T9). Publisher inflight windows (`receive_maximum`) bound this
/// naturally; the cap is a backstop against a partition outlasting every window.
/// At the cap the **oldest** pending publish is dropped loudly — its ack is
/// withheld, so the publisher retries (never an ack for an unowned message).
const PENDING_PUBLISH_CAP: usize = 4096;

/// The first peer-bus proto that can carry a forward VERDICT rather than a bool
/// ([`PeerMessage::PublishVerdict`], [`PeerMessage::SharedDeliverAcked`] — 0041-T12,
/// issue #238).
///
/// Frame choice is gated on the LINK's negotiated proto, never on what this build
/// supports: the peer codec is strict, so an unknown variant index is a decode error →
/// `io::Error` → link teardown and redial, i.e. a flap loop rather than a graceful
/// degradation. A link whose proto is somehow unknown is treated as
/// [`peer::PROTO_MIN`](mqtt_cluster::peer::PROTO_MIN) — fail safe toward the old frame.
const PROTO_FORWARD_VERDICT: u32 = 7;

/// Sweep ticks a pending publish waits after its forward target **died** with no
/// current remote interest in the topic, before concluding the interest genuinely
/// ended (session gone) rather than being mid-takeover: the dead owner's successor
/// materializes inherited sessions and re-advertises their filters (exhibit ⑥ fix)
/// within this window in any live cluster. Sized to outlast SWIM confirmation plus
/// the successor's inherited-session scan; the cost of the margin is only a slower
/// (withheld) ack for a publish whose subscriber genuinely no longer exists.
const REROUTE_GRACE_TICKS: u8 = 8;

// The bools are independent obligations, not an encodable state machine.

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
                append_lanes: HashMap::new(),
                owned_tasks: tokio::task::JoinSet::new(),
                truncate_tx: None,
                remote_append_pending: HashMap::new(),
                node_id,
                online: HashMap::new(),
                pending_wills: HashMap::new(),
                session_expiry: HashMap::new(),
                expiring: HashMap::new(),
                expiry_reconcile_tick: 0,
                retained_antientropy_tick: 0,
                subs_by_client: HashMap::new(),
                no_local: HashMap::new(),
                sub_ids: HashMap::new(),
                retain_as_published: HashMap::new(),
                table: SubscriptionTable::new(),
                shared: SharedSubscriptionTable::new(),
                remote_shared: HashMap::new(),
                shared_cursor: HashMap::new(),
                inflight: HashMap::new(),
                store,
                durable_plane: None,
                allow_relaxed_publish: false,
                ownership_domain: None,
                known_peer_protos: HashMap::new(),
                retained: Arc::new(MemoryRetainedStore::new()),
                durable_retained: None,
                authz: None,
                brownout: false,
                brownout_axes: HashSet::new(),
                brownout_status: None,
                quotas: Quotas::default(),
                retained_tokens: HashMap::new(),
                retained_windows: HashMap::new(),
                retained_digest_matched_at: HashMap::new(),
                retained_tombstone_observed_at: HashMap::new(),
                retained_may_expire: false,
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
                durable_writes: 0,
                inherited_scan_inflight: false,
                interest_authoritative: false,
                interest_suppressed_ticks: 0,
                last_scan_complete: false,
                cluster_configured: false,
                // A boot window (like the post-PeerDead takeover window): a
                // restarted or newly-joined node may already own groups with
                // orphaned sessions, and eagerly recovering them BEFORE workload
                // arrives keeps the first-touch epoch bumps (which transiently
                // break quorum for concurrent appends) out of the hot path.
                takeover_reconcile_ticks: 8,
                // Seeded empty on purpose: the first sweep observes the real
                // member set as a "change", which (re)arms the boot window —
                // harmless overlap with the 8 ticks above.
                known_members: BTreeSet::new(),
                sweep_epoch_seen: None,
                misplaced: HashMap::new(),
                ownership_epoch_seen: None,
                peers: HashMap::new(),
                remote_interest: HashMap::new(),
                placement,
                metrics: None,
                clock: crate::clock::system_clock(),
                subscriber_limits: SubscriberLimits::default(),
            },
            tx,
        )
    }

    /// Attach the durable-plane endpoint (consensus + replication) before
    /// [`run`](Self::run). Enables routing of [`HubCommand::DurableFrame`]s and
    /// peer (de)registration on the plane. Only set when durable sessions are on.
    /// Honor per-message durability tiers (ADR 0072) — the operator's
    /// `MQTTD_ALLOW_RELAXED_PUBLISH` opt-in, threaded to the ack gate.
    pub fn set_allow_relaxed_publish(&mut self, on: bool) {
        self.allow_relaxed_publish = on;
    }

    /// ADR 0072 / issue #399: does this message ask for the relaxed tier, and
    /// does THIS node's operator allow it? Derived per node at the point it is
    /// acted on — a forwarded publish is re-derived here under the receiving
    /// node's own opt-in, exactly like the origin's freeze point.
    fn relaxed_requested(&self, app: &mqtt_core::AppProperties) -> bool {
        self.allow_relaxed_publish
            && app
                .user_properties
                .iter()
                .rev()
                .find(|(k, _)| k == mqtt_storage::repl::DURABILITY_PROPERTY)
                .and_then(|(_, v)| mqtt_storage::repl::DurabilityTier::parse(v))
                == Some(mqtt_storage::repl::DurabilityTier::Relaxed)
    }

    /// Wire the ADR 0073 scale-out ownership capability: `flag` is shared with the
    /// durable driver and plane; `enabled` is the operator's
    /// `durable.ownership_domain = "members"` choice (false = the "voters" escape
    /// hatch — the flag then never sets and ADR 0049's restriction holds).
    pub fn set_ownership_domain(
        &mut self,
        flag: Arc<std::sync::atomic::AtomicBool>,
        enabled: bool,
    ) {
        self.ownership_domain = Some((flag, enabled));
    }

    pub fn attach_durable_plane(&mut self, plane: DurablePlane) {
        self.durable_plane = Some(plane);
    }

    /// Mark this node CLUSTER-CONFIGURED before [`run`](Self::run) — peer
    /// networking (SWIM/peer bind/static peers) is set up, so the cluster
    /// honesty gates apply from the first moment, not only once SWIM has
    /// (re-)learned a second member (0043-P4 exhibit ②).
    pub fn set_cluster_configured(&mut self) {
        self.cluster_configured = true;
    }

    /// Replace the retained-message store before [`run`](Self::run) — used to swap the
    /// in-memory default for the on-disk store when persistence is enabled (ADR 0018
    /// phase 4).
    pub fn attach_retained_store(&mut self, retained: Arc<dyn RetainedStore>) {
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

    /// Set the per-subscriber in-memory bounds before [`run`](Self::run) (issue #241).
    ///
    /// Startup-only, like the other `attach_*` setters: a reload reports the `limits`
    /// section as requires-restart (ADR 0041 §6) rather than half-applying a bound to
    /// queues that are already at it.
    pub fn set_subscriber_limits(&mut self, limits: SubscriberLimits) {
        self.subscriber_limits = limits;
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
        // ADR 0074: the truncate flusher. Owned by `owned_tasks` so it aborts with
        // the hub; a send after abort is the documented not-fatal case (entries
        // replay at next resume, the subscriber re-acks, the watermark re-advances).
        let (truncate_tx, truncate_rx) = mpsc::unbounded_channel();
        self.truncate_tx = Some(truncate_tx);
        self.owned_tasks
            .spawn(run_truncate_flusher(self.store.clone(), truncate_rx));
        // The boot window's FIRST inherited-session scan runs immediately, not a
        // sweep tick later: on a fresh or restarted node it completes in
        // milliseconds and releases any publish acks gated on it (ADR 0042 T9).
        self.spawn_inherited_session_scan();
        loop {
            tokio::select! {
                cmd = self.rx.recv() => match cmd {
                    Some(cmd) => {
                        // Time-on-loop per dispatch (issue #242): the regression
                        // tripwire for any await smuggled back onto the single-
                        // threaded loop. Coarse command class, never per-variant.
                        let class = cmd.class();
                        let started = Instant::now();
                        self.dispatch(cmd).await;
                        if let Some(m) = &self.metrics {
                            m.observe_hub_dispatch(class, started.elapsed().as_secs_f64());
                        }
                    }
                    None => break,
                },
                _ = sweep.tick() => {
                    let started = Instant::now();
                    self.sweep_expired_sessions().await;
                    self.refresh_gauges().await;
                    self.refresh_ownership_domain();
                    // Retransmit an unanswered retained handoff (T8 — same seq, the
                    // owner dedups), then retry queued retained mutations (ADR 0037
                    // §5): covers heals with no link event — a lease landing locally,
                    // or quorum returning on links that never dropped. No-ops when idle.
                    self.retry_retained_handoff();
                    self.kick_retained_queue();
                    // Retransmit / re-route acked publish forwards (ADR 0042 T9,
                    // exhibit ⑤); no-op when none are pending.
                    self.sweep_pending_forwards();
                    // Reap idle append lanes (issue #242): dropping the sender ends
                    // the worker; a later submission re-spawns one. Only with zero
                    // outstanding jobs, so no completion is ever orphaned.
                    self.append_lanes.retain(|_, lane| lane.outstanding > 0);
                    // Reap the reaped lanes' finished workers too: a JoinSet holds a
                    // completed task's slot until polled, so without this the set grows
                    // by one per lane ever spawned.
                    while self.owned_tasks.try_join_next().is_some() {}
                    if let Some(m) = &self.metrics {
                        m.observe_hub_dispatch("sweep", started.elapsed().as_secs_f64());
                    }
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
                no_local_filters,
                sub_id,
                rap_filters,
                retain_handling,
                reply,
            } => {
                self.subscribe(
                    &client,
                    filters,
                    no_local_filters,
                    sub_id,
                    rap_filters,
                    retain_handling,
                    reply,
                )
                .await;
            }
            HubCommand::SetQuotas(quotas) => {
                self.quotas = quotas;
            }
            HubCommand::SetBrownout { axis, on } => {
                self.set_brownout_axis(axis, on);
            }
            HubCommand::Unsubscribe {
                client,
                filters,
                reply,
            } => {
                let existed = self.unsubscribe(&client, &filters).await;
                if let Some(reply) = reply {
                    // A dropped receiver means the connection died mid-unsubscribe;
                    // the removal itself already happened either way.
                    let _ = reply.send(existed);
                }
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
                publisher,
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
                            let _ =
                                done.send(PublishOutcome::Refused(PublishRefusal::RetainedQuota));
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
                // ADR 0072: the publisher may weaken ITS OWN ack per message via
                // `mqttd-durability` — only under the operator's opt-in. `relaxed`
                // releases the ack at local_done (everything still runs); `local`
                // is honored inside the store's append; v3.1.1 can't carry the
                // property, so it always gets the full quorum path. The property
                // itself is forwarded unaltered (MQTT-3.3.2-17).
                let tier = if self.allow_relaxed_publish {
                    app.user_properties
                        .iter()
                        .rev()
                        .find(|(k, _)| k == mqtt_storage::repl::DURABILITY_PROPERTY)
                        .and_then(|(_, v)| mqtt_storage::repl::DurabilityTier::parse(v))
                        .unwrap_or_default()
                } else {
                    mqtt_storage::repl::DurabilityTier::Quorum
                };
                if let Some(m) = &self.metrics {
                    m.publish_tier(tier.as_str());
                }
                if tier == mqtt_storage::repl::DurabilityTier::Relaxed {
                    if let Some(id) = gate {
                        self.pending_mark_relaxed(id);
                    }
                }
                // Time the synchronous on-loop fan-out (plan + lane submissions +
                // peer forward) as the hub's per-publish on-loop latency (ADR
                // 0020-T4; since issue #242 the durable appends themselves run
                // off-loop and are timed by `durable_append_latency_seconds`).
                let started = Instant::now();
                let durable = self
                    .publish(
                        &topic,
                        &payload,
                        qos,
                        retain,
                        message_expiry,
                        &app,
                        gate,
                        publisher.as_ref(),
                    )
                    .await;
                if let Some(m) = &self.metrics {
                    m.observe_deliver_latency(started.elapsed().as_secs_f64());
                }
                // The LOCAL fan-out pass is complete: every owed durable append is
                // now SUBMITTED to its session's lane (issue #242), counted in the
                // gate's `appends_outstanding` — so `pending_local_done` can fire
                // here while the ack still waits for every append's `AppendDone`
                // (ADR 0018 + ADR 0042 T9). A submission the lane REJECTED (full)
                // or a failed retained write WITHHOLDS the ack (drop the entry):
                // the publisher's connection closes unacked and it retries — fail
                // closed, never an ack for a message a subscriber will never see
                // (ADR 0041 T5). A stated-policy REFUSAL (brownout) was decided at
                // the plan pass, before any submission, and is told to the
                // publisher instead of withheld (0041-T11, issue #238).
                if let Some(id) = gate {
                    match durable {
                        DurableOutcome::Ok => self.pending_local_done(id),
                        DurableOutcome::Refused(r) => self.refuse_pending(id, r),
                        DurableOutcome::Failed => self.drop_pending(id),
                    }
                }
            }
            HubCommand::RestoreRetained {
                topic,
                payload,
                qos,
                message_expiry,
                app,
                done,
            } => {
                self.restore_retained(topic, payload, qos, message_expiry, app, done)
                    .await;
            }
            HubCommand::RetainedExportSnapshot { done } => {
                let snapshot = self.retained_export_snapshot().await;
                let _ = done.send(snapshot);
            }
            HubCommand::AppendDone { job, outcome } => {
                self.append_done(*job, outcome);
            }
            HubCommand::PkidBlockReserved { client, result } => {
                self.pkid_block_reserved(&client, result);
            }
            HubCommand::PubAck { client, pkid } => self.pub_ack(&client, pkid),
            HubCommand::PubRec { client, pkid } => self.pub_rec(&client, pkid).await,
            HubCommand::PubComp { client, pkid } => self.pub_comp(&client, pkid).await,
            HubCommand::Detach {
                client,
                conn_id,
                graceful,
                session_expiry_override,
            } => {
                self.detach(&client, conn_id, graceful, session_expiry_override)
                    .await;
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
                //
                // A fan-out that matched NOBODY while this node's routing view is
                // still settling (mid-boot, a takeover/membership window, a moved
                // session just released) answers `ok = false` (0043-P4 exhibit ②):
                // the sender forwarded because interest said someone here is owed
                // this, and "I found no one" is not yet a claim this node can stand
                // behind — the refusal makes the publisher retry until it is.
                //
                // The answer is a VERDICT at proto ≥ 7 (0041-T12, issue #238), so a
                // stated-policy REFUSAL on this node reaches the origin as one and its
                // publisher is told `0x97` instead of being closed on unacked. At proto 6
                // it collapses to `PublishAck { ok }` — today's behaviour verbatim, the
                // rolling-upgrade skew residual. The fan-out is `answerable` either way:
                // a verdict travels back, so a refusal must be effect-free here too.
                let (durable, matched) = self
                    .deliver(
                        &topic,
                        &payload,
                        qos,
                        retain,
                        message_expiry,
                        &app,
                        None,
                        &AppendGate::Peer {
                            node: node.clone(),
                            seq,
                        },
                    )
                    .await;
                // A fan-out that matched NOBODY mid-settle maps to FAILED, never
                // `Refused`: the message may genuinely be owed and delivered once the
                // view settles, so "nothing was stored, retry" would be a FALSE refusal
                // — as much a defect as a false ack.
                let sync = if matched == 0 && self.routing_unsettled() {
                    DurableOutcome::Failed
                } else {
                    durable
                };
                // Answered now if no lane job was submitted; otherwise folded into
                // the `(node, seq)` aggregate and answered when the last `AppendDone`
                // lands — a peer is never told `Stored` before the store actually
                // stored (issue #242). Exception, by explicit contract: a RELAXED
                // forward below the congestion threshold answers at
                // submit-acceptance (issue #399) — `Stored` then means what the
                // relaxed ack means.
                let relaxed = self.relaxed_requested(&app);
                self.finish_peer_verdict(&node, seq, sync, relaxed);
            }
            HubCommand::RemotePublishAck { node, seq, ok } => {
                // A proto-6 peer's boolean: `false` means only "not stored, reason
                // unknown", which is exactly `Failed` — withhold.
                let verdict = if ok {
                    ForwardVerdict::Stored
                } else {
                    ForwardVerdict::Failed
                };
                self.forward_answered(&node, seq, verdict);
            }
            HubCommand::RemotePublishVerdict { node, seq, verdict } => {
                self.forward_answered(&node, seq, verdict);
            }
            HubCommand::RemoteSharedDeliverAcked {
                node,
                seq,
                client,
                topic,
                payload,
                qos,
                message_expiry,
                app,
            } => {
                // The answerable form of `RemoteSharedDeliver` (0041-T12, issue #238):
                // the outcome is no longer discarded. A gated cross-node shared delivery
                // is durability-gated on the OWNING node and answered, so the origin can
                // re-select within the group instead of acking a message that reached
                // nobody. Arrives only from a proto-7 sender, so a verdict can always be
                // sent back.
                let out = self.deliver_to_client(
                    &client,
                    &topic,
                    &payload,
                    qos,
                    message_expiry,
                    &app,
                    false, // shared delivery clears RETAIN (#198)
                    &AppendGate::Peer {
                        node: node.clone(),
                        seq,
                    },
                );
                // Answered now, or at the append's `AppendDone` (issue #242) —
                // or, for an uncongested RELAXED delivery, at submit-acceptance
                // (issue #399).
                let relaxed = self.relaxed_requested(&app);
                self.finish_peer_verdict(&node, seq, out, relaxed);
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
                // Unanswerable: no publisher and no peer awaits an answer to a plain
                // `Publish` forward, so a refused durable copy must not cost the live
                // delivery (issue #238).
                let _ = self
                    .deliver(
                        &topic,
                        &payload,
                        qos,
                        retain,
                        message_expiry,
                        &app,
                        None,
                        &AppendGate::None,
                    )
                    .await;
            }
            HubCommand::PeerConnected {
                node,
                conn_id,
                tx,
                ctl,
                cert_serial,
                proto,
            } => {
                self.peer_connected(node.clone(), conn_id, tx, ctl, cert_serial, proto);
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
            HubCommand::InheritedSessions { sessions, complete } => {
                self.inherit_sessions(sessions, complete);
            }
            HubCommand::Ping { reply } => {
                // Reached the loop → it is live. The receiver may be gone if the
                // prober timed out; that is fine.
                let _ = reply.send(());
            }
            HubCommand::RemoteInterest { node, filters } => {
                debug!(node = %node.0, filters = filters.len(), "remote interest updated");
                // The peer's view is AUTHORITATIVE (it never gossips before it
                // is — 0043-P4 exhibit ②): its link now counts toward
                // `mesh_settled`, and a scan can settle held publishes against
                // its (possibly new) interest.
                if let Some(peer) = self.peers.get_mut(&node) {
                    if !peer.interest_synced {
                        peer.interest_synced = true;
                        if !self.pending_publishes.is_empty() {
                            self.takeover_reconcile_ticks = self.takeover_reconcile_ticks.max(2);
                        }
                    }
                }
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
                expires_at,
            } => {
                // A peer routed a retained mutation here because this node owns the
                // topic's group (ADR 0037 §1): dedup retransmissions, then run it
                // through the same queue as local mutations — serialized commit
                // order, retry-until-heal, and a NACK back if the lease moved (T8).
                self.accept_routed_retained(node, topic, payload, qos, app, seq, expires_at);
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
                expires_at,
                restore,
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
                    self.apply_committed_retained(
                        &topic,
                        &payload,
                        qos,
                        &app,
                        (epoch, offset),
                        expires_at,
                        // A restore delivers to nobody (see `RetainedMutation::restore`).
                        !restore,
                    )
                    .await;
                    for peer in self.peers.values() {
                        let _ = peer.tx.send(PeerMessage::RetainedUpdate {
                            topic: topic.clone(),
                            payload: payload.to_vec(),
                            qos,
                            epoch,
                            offset,
                            props: app_to_wire(&app),
                            expires_at,
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
                        expires_at,
                        restore,
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
                expires_at,
            } => {
                self.apply_retained_update(
                    &topic,
                    &payload,
                    qos,
                    &app,
                    (epoch, offset),
                    expires_at,
                )
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
                // retain=false: a shared-subscription delivery clears the flag (RAP is applied
                // on the ordinary-delivery path only, #198).
                // Unanswered by construction: this frame is only sent when nothing is
                // owed (`QoS` 0) or the link is proto 6 and cannot carry a verdict
                // ([`RemoteSharedDeliverAcked`](HubCommand::RemoteSharedDeliverAcked) is
                // the answerable form — 0041-T12). `answerable = false` is therefore the
                // truth, and it is what keeps the live send when the durable copy is
                // refused: nobody will be told and nobody will retry, so suppressing the
                // delivery would destroy the message rather than defer it.
                let _ = self.deliver_to_client(
                    &client,
                    &topic,
                    &payload,
                    qos,
                    message_expiry,
                    &app,
                    false,
                    &AppendGate::None,
                );
            }
            // Client/session commands are handled in `dispatch`; they never route here.
            _ => {}
        }
    }

    /// Publish a locally-originated message: apply it on this node, then forward to
    /// peers (interested peers for live delivery; **all** peers for retained, so each
    /// node stores it for its future subscribers — ADR 0014).
    /// Returns a non-`Ok` [`DurableOutcome`] when a durable enqueue failed — the
    /// dispatch then withholds the publisher's ack (ADR 0041 T5) — or was refused
    /// under a stated policy, which the dispatch turns into a reason the publisher
    /// is told (0041-T11, issue #238).
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
        publisher: Option<&ClientId>,
    ) -> DurableOutcome {
        // PLAN, then COMMIT (issue #238). A refusal must be EFFECT-FREE, so it is
        // decided before the first side effect of the whole publish — before
        // `deliver`'s `retained.set`, before any append, before any live send, before
        // `forward_to_peers`, before `route_retained_commit`. The SHARED half is
        // planned here because `deliver` cannot see it, and it is peeked rather than
        // selected so a refused publish does not consume a group member's turn.
        let append_gate = gate.map_or(AppendGate::None, AppendGate::Pending);
        if append_gate.answerable() && self.shared_plan_owes_durable(topic, qos) {
            if let Some(r) = self.plan_refusal(true) {
                self.count_refusal(r);
                return DurableOutcome::Refused(r);
            }
        }
        let (mut durable, _matched) = self
            .deliver(
                topic,
                payload,
                qos,
                retain,
                message_expiry,
                app,
                publisher,
                &append_gate,
            )
            .await;
        // `deliver` refused before taking any side effect, so there is nothing to
        // forward, nothing to commit and no member's turn to consume: return with the
        // publish exactly as unobserved as if it had never arrived.
        if let DurableOutcome::Refused(r) = durable {
            return DurableOutcome::Refused(r);
        }
        // Shared subscriptions are selected once cluster-wide by the originating
        // node (ADR 0015), so this runs only for locally-originated publishes. Its
        // durability gates the ack too (#164): a shared subscriber is a persistent
        // subscriber, and a failed enqueue for the chosen member must withhold the
        // publisher's ack exactly as an ordinary subscriber's would — otherwise the
        // publisher is told a message survived that was never recorded.
        durable = durable.and(self.deliver_shared(topic, payload, qos, message_expiry, app, gate));
        self.forward_to_peers(topic, payload, qos, retain, message_expiry, app, gate);
        // Durable retained (ADR 0037): after the live fan-out — which stays undelayed —
        // route the retained mutation to its topic's group lease-owner for the
        // quorum-committed authority write. Only the **landing** node routes (a
        // forwarded publish enters via `RemotePublish` → `deliver`, never here), so one
        // publish is exactly one authority commit. The gated publish's ack now waits
        // for this commit too (ADR 0042 T9, exhibit ⑦).
        if retain {
            // The absolute deadline commits WITH the value (issue #227), so every
            // cache expires it at the same instant and a replay can send the
            // remaining interval.
            let expires_at = message_expiry.map(|s| self.clock.now_epoch_secs() + u64::from(s));
            self.route_retained_commit(topic, payload, qos_num(qos), app, gate, expires_at, false);
        }
        durable
    }

    /// Publish a client's Will message (on takeover or an ungraceful end). Carries the
    /// will's own application properties (ADR 0030); a will never sets a message-expiry.
    ///
    /// UNGATED, and that is load-bearing (issue #238): there is no publisher to refuse
    /// and nothing that will retry, so a Will whose durable copy a watermark refuses is
    /// still delivered LIVE and counted as a genuine drop — never suppressed. A Will
    /// suppressed under brownout is a device that stays "online" on every dashboard
    /// through exactly the incident [MQTT-3.14.4-3] exists for.
    async fn publish_will(&mut self, w: &Message) {
        self.publish(
            &w.topic, &w.payload, w.qos, w.retain, None, &w.app, None, None,
        )
        .await;
    }

    /// Log when a persistent session attaches on a node that is not its placement
    /// owner (ADR 0005). Expected transiently: relocation is decided against the view
    /// at CONNECT, and ownership can move moments later (a readmitted node reclaiming
    /// its groups). It is USUALLY brief — a session left standing in this state is closed
    /// by [`rehome_misplaced_sessions`](Self::rehome_misplaced_sessions) (issue #284) so
    /// the client relocates, including when the ownership moved BEFORE the session arrived
    /// (`finish_attach` seeds the observation, which the pass would otherwise skip).
    ///
    /// **But this warning is not self-healing in every case it fires**, which is worth
    /// knowing when grepping for a stranded session: it is computed from the HRW-fallback
    /// view (`owns`/`owner` below), while the rehome pass is deliberately
    /// COMMITTED-lease-only. For a group with no committed lease — the transient ring/lease
    /// split during convergence — this warns and nothing closes the session. That is
    /// deliberate, and `a_group_with_no_committed_lease_is_never_rehomed` pins it: a
    /// lease-less group is not evidence that another node owns it, and closing on the ring
    /// alone would kick clients around a converging cluster. Diagnostic only; the sweep
    /// tick, not this warning, decides.
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
                 (session relocation / cross-node affinity is ADR 0005 step 2)"
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
            // Appends for this session still in flight in its lane (issue #242): route
            // the discard THROUGH the lane so it serializes AFTER them — a late append
            // landing post-remove would silently re-create the queue with a ghost
            // message. A full lane falls back to the spawn (the ghost residual is
            // accepted there, loudly), so the CONNACK can never wedge on a lane.
            if self
                .append_lanes
                .get(&pending.client)
                .is_some_and(|l| l.outstanding > 0)
            {
                let lane = self
                    .append_lanes
                    .get_mut(&pending.client)
                    .expect("checked just above");
                match lane.tx.try_send(LaneJob::Discard(Box::new(pending))) {
                    Ok(()) => {
                        lane.outstanding += 1;
                        return;
                    }
                    Err(e) => {
                        warn!(
                            "append lane full at clean-start discard; falling back to \
                             the spawned discard (issue #242)"
                        );
                        let (mpsc::error::TrySendError::Full(job)
                        | mpsc::error::TrySendError::Closed(job)) = e;
                        let LaneJob::Discard(pending) = job else {
                            unreachable!("the job sent just above is a Discard");
                        };
                        self.spawn_owned(discard_session(
                            self.store.clone(),
                            self.self_tx.clone(),
                            *pending,
                        ));
                        return;
                    }
                }
            }
            self.spawn_owned(discard_session(
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
        self.spawn_owned(recover_session(
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
                // ADR 0049: the exact fingerprint that was invisible for 11 h in the
                // 2026-07-14 incident — a persistent attach refused (0x88), not an append.
                if let Some(m) = &self.metrics {
                    m.durable_recovery_failed("deadline");
                }
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
                self.spawn_owned(async move {
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
                        // Under the resuming session's own client id, so a `%c` grant
                        // is re-checked against the handle it was written for.
                        authorizer.authorize_subscribe(&admission.identity, &client, &s.filter)
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
            if let Some(id) = s.sub_id {
                // Restored session state (issue #266, §4.1): the id survives
                // reconnect exactly like the subscription it belongs to.
                self.sub_ids
                    .entry(client.clone())
                    .or_default()
                    .insert(s.filter.clone(), id);
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
        //
        // `MQTTD_MAX_INFLIGHT_MESSAGES` caps it (issue #241). A client's Receive Maximum
        // is a ceiling on what the broker MAY send it, never a floor, so lowering it is
        // protocol-legal — and it is the LOSS-FREE lever: the in-flight table is the
        // second-largest per-subscriber structure (65 535 entries by default, since every
        // v3.1.1 client and any v5 client with no property gets `u16::MAX`), and an entry
        // there is on the wire under a packet id, so it could never be shed without
        // breaking DUP redelivery or the QoS 2 handshake. Capping the window diverts the
        // surplus into the (byte-bounded) backlog instead of dropping anything.
        self.inflight
            .entry(client.clone())
            .or_default()
            .receive_maximum = receive_maximum.min(
            self.subscriber_limits
                .max_inflight_messages
                .unwrap_or(u16::MAX),
        );

        // The client is back: cancel any Will this session was holding for its delay
        // (§3.1.3.2.2, issue #299). This is the whole point of the delay — without
        // the cancel it is only a slower announcement of a death that did not
        // happen. Done before the takeover branch below, so a client that returns by
        // taking over its own session also cancels.
        if self.pending_wills.remove(&client).is_some() {
            info!(client = %client.0, "client returned inside its will delay; will cancelled");
        }

        // Registering replaces any previous connection for this id; dropping the
        // old `Outbound` closes the old writer loop (takeover). The server-side
        // disconnect is not a client DISCONNECT, so the old will is published —
        // IMMEDIATELY, delay or not: a takeover ends the old session, and
        // §3.1.3.2.2 publishes on whichever of "delay elapsed" or "session ended"
        // comes first.
        if let Some(old) = self.online.remove(&client) {
            warn!(client = %client.0, "session takeover: replacing existing connection");
            if let Some(w) = old.will {
                self.publish_will(&w.message).await;
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

        // Issue #284 round 3: a session becomes misplaced by ARRIVING, not only by an
        // ownership move. `rehome_misplaced_sessions` skips its whole candidate pass while
        // the placement's ownership version is unchanged and nothing is under observation,
        // so a persistent session that attaches to a non-owning node AFTER the lease moved
        // would never be looked at again — the wedge, silently, with no counter moving.
        // Seed the observation here rather than by deleting that skip: one committed-owner
        // read per persistent attach, nothing per tick. `misplaced` being non-empty is
        // itself the pass's escape hatch, so it then runs until the episode ends, and if
        // the reading was stale the pass's own `retain` drops the entry on its next tick.
        if session_expiry != 0 {
            if let Some(placement) = &self.placement {
                let elsewhere = placement
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .committed_session_owner(&client.0)
                    .is_some_and(|owner| owner != self.node_id);
                if elsewhere {
                    self.misplaced.entry(client.clone()).or_default();
                }
            }
        }

        // Tell the connection the result so it can CONNACK before any replay.
        let _ = reply.send(AttachOutcome::Present(session_present));

        // Resume in-flight QoS state: unacked PUBLISHes go out again with DUP
        // [MQTT-4.4.0-1]; half-completed QoS 2 deliveries resume at PUBREL.
        //
        // Their log offsets (#124) are collected so the replay below does not send the
        // same messages a second time. The set is empty after a broker restart — the
        // in-flight table is memory — which is exactly when the replay must carry them.
        let mut resumed_offsets: HashSet<Offset> = HashSet::new();
        // Entries still staged behind an outbound-id record (issue #242 finding A)
        // never reached any wire, so there is nothing to DUP-resume: drop them —
        // this attach's replay below delivers their durable copies fresh (their
        // offsets stayed owed, so truncation never passed them). Covers the
        // takeover-without-detach path; a plain reconnect already dropped them at
        // detach.
        if let Some(inf) = self.inflight.get_mut(&client) {
            inf.pending
                .retain(|_, p| p.state != OutState::AwaitingIdRecord);
        }
        if let Some(inf) = self.inflight.get(&client) {
            for (pkid, p) in &inf.pending {
                if let Some(offset) = p.offset {
                    resumed_offsets.insert(offset);
                }
                let packet = match p.state {
                    // Dropped just above; nothing staged can appear here.
                    OutState::AwaitingIdRecord => continue,
                    OutState::AwaitingPubAck | OutState::AwaitingPubRec => publish_packet(
                        &p.message.topic,
                        p.message.payload.clone(),
                        p.message.qos,
                        Some(*pkid),
                        true,
                        false,
                        None,
                        &p.message.app,
                        &self.matching_sub_ids(&client, &p.message.topic),
                    ),
                    OutState::AwaitingPubComp => Packet::PubRel((*pkid).into()),
                };
                let _ = outbound.send(packet);
            }
        }

        // The durable outbound table (ADR 0057): `QoS` 2 deliveries whose packet id and
        // phase survived a broker restart. Keyed by offset so the replay below can marry
        // each entry back to its message. On a plain client reconnect these ids are also
        // in the in-memory table and were resumed above — `resumed_offsets` keeps the
        // replay away from them, so the map only acts when memory is gone, which is
        // exactly the restart this table exists for.
        let mut restored: std::collections::BTreeMap<Offset, (u16, bool)> =
            std::collections::BTreeMap::new();
        if !clean_start {
            if let Ok(entries) = self.store.outbound(&client).await {
                for e in entries {
                    restored.insert(e.offset, (e.packet_id, e.pubrec_seen));
                }
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
                    // Raise the truncation ceiling for every entry read, so the ones
                    // dropped below are let go of even though nothing will deliver
                    // them (#124). Entries that ARE sent additionally become owed, in
                    // `send_to_client`, and hold the ceiling down until acknowledged.
                    self.inflight
                        .entry(client.clone())
                        .or_default()
                        .note_offset(qm.offset);
                    // Still in the in-flight table means this is a client reconnect, not
                    // a broker restart: the DUP resume above already re-sent it under its
                    // original packet id — and for `QoS` 2 possibly as a bare PUBREL,
                    // because the client has acknowledged receipt and only owes the
                    // release. Replaying the PUBLISH here would deliver it twice.
                    if resumed_offsets.contains(&qm.offset) {
                        debug!(client = %client.0, offset = qm.offset,
                               "queued message is still in flight in memory; not replaying");
                        restored.remove(&qm.offset);
                        continue;
                    }
                    // A broker restart with a durably-recorded id (ADR 0057): resume the
                    // handshake mid-phrase UNDER THE ORIGINAL ID, never through a fresh
                    // allocation. Before PUBREC: the PUBLISH goes out again with DUP and
                    // the id the subscriber may already hold. After PUBREC: a bare
                    // PUBREL — the subscriber has the message; re-publishing it is the
                    // #130 duplicate this table exists to prevent.
                    if let Some((pkid, pubrec_seen)) = restored.remove(&qm.offset) {
                        let state = if pubrec_seen {
                            OutState::AwaitingPubComp
                        } else {
                            OutState::AwaitingPubRec
                        };
                        self.inflight
                            .entry(client.clone())
                            .or_default()
                            .pending
                            .insert(
                                pkid,
                                PendingOut {
                                    message: qm.message.clone(),
                                    state,
                                    offset: Some(qm.offset),
                                },
                            );
                        let packet = if pubrec_seen {
                            Packet::PubRel(pkid.into())
                        } else {
                            publish_packet(
                                &qm.message.topic,
                                qm.message.payload.clone(),
                                qm.message.qos,
                                Some(pkid),
                                true,
                                false,
                                None,
                                &qm.message.app,
                                &self.matching_sub_ids(&client, &qm.message.topic),
                            )
                        };
                        let _ = outbound.send(packet);
                        continue;
                    }
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
                                Some(qm.offset),
                            );
                        }
                        None => {
                            self.send_to_client(
                                &client,
                                &outbound,
                                &qm.message,
                                false,
                                None,
                                Some(qm.offset),
                            );
                        }
                    }
                }
                if last > 0 {
                    debug!(client = %client.0, up_to = last, "replayed queued messages");
                    // Truncate only the entries this replay let go of — a `QoS` > 0
                    // message that went on the wire is truncated when the subscriber
                    // acknowledges it, not when it was sent (#124).
                    self.truncate_acked(&client);
                }
            }

            // Table entries whose message is no longer in the queue — an earlier clear
            // failed and its truncation went through anyway (ADR 0057's tolerated
            // failure). Released phase: send the spurious PUBREL the tolerance priced
            // in; the subscriber's PUBCOMP (MQTT-4.3.3) clears the entry, because
            // pub_comp clears unconditionally. Unreleased phase: the PUBLISH cannot be
            // reconstructed AND the message left the log, which means it was let go of —
            // clear the id rather than carry it forever.
            for (offset, (pkid, pubrec_seen)) in restored {
                if pubrec_seen {
                    debug!(client = %client.0, pkid, offset,
                           "orphaned outbound QoS2 id in released phase; sending PUBREL");
                    let _ = outbound.send(Packet::PubRel(pkid.into()));
                } else if let Err(e) = self.store.clear_outbound(&client, pkid).await {
                    warn!(client = %client.0, pkid, error = %e,
                          "orphaned outbound QoS2 id could not be cleared");
                }
            }
        }
    }

    // Quota check, per-filter grant/deny, retained replay and its two now-counted
    // drop paths (#87 item 5) — one linear flow over the subscribe request.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)] // a subscription's full option set (#198 + #266)
    async fn subscribe(
        &mut self,
        client: &ClientId,
        filters: Vec<(String, QoS)>,
        no_local_filters: Vec<String>,
        sub_id: Option<u32>,
        rap_filters: Vec<String>,
        retain_handling: Vec<u8>,
        reply: Option<oneshot::Sender<Vec<bool>>>,
    ) {
        // Retain Handling per requested filter (#198), keyed by filter so it survives the
        // quota filtering below. Absent (v3.1.1 / internal callers) = 0, the default.
        let handling: HashMap<String, u8> = filters
            .iter()
            .zip(retain_handling.iter().copied().chain(std::iter::repeat(0)))
            .map(|((f, _), h)| (f.clone(), h))
            .collect();
        // MQTT 5 subscription options (#198): record which filters carry No Local / Retain As
        // Published, and CLEAR the option for any (re)subscribed filter that did not set it —
        // a re-subscribe replaces the options [MQTT-3.8.4-3]. Only ACL-granted filters reach
        // here; both options are applied on the ordinary-delivery path.
        record_sub_option(&mut self.no_local, client, &filters, &no_local_filters);
        record_sub_option(
            &mut self.retain_as_published,
            client,
            &filters,
            &rap_filters,
        );
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
        // #219: what the replay is about to show this client, keyed by topic — the
        // seed of its retained-delivery window's ledger (recorded with the STORE
        // message's qos/props, the identity the fan-out will present).
        let mut window_seeds: Vec<(String, u64)> = Vec::new();
        let mut ordinary_granted = false;
        for (f, q) in &filters {
            // Keep the full filter string (including any `$share/` prefix) so it is
            // persisted; `$share/...` never matches a concrete topic in `granted_qos`.
            self.subs_by_client
                .entry(client.clone())
                .or_default()
                .insert(f.clone(), *q);
            // Subscription Identifier, replace-don't-merge [MQTT-3.8.4-3] (issue
            // #266): the packet's one id applies to every filter it granted; a
            // re-SUBSCRIBE without one REMOVES a stored id rather than keeping it.
            match sub_id {
                Some(id) => {
                    self.sub_ids
                        .entry(client.clone())
                        .or_default()
                        .insert(f.clone(), id);
                }
                None => {
                    if let Some(ids) = self.sub_ids.get_mut(client) {
                        ids.remove(f);
                        if ids.is_empty() {
                            self.sub_ids.remove(client);
                        }
                    }
                }
            }
            if let Some((group, filter)) = parse_shared(f) {
                debug!(client = %client.0, group, filter, qos = *q as u8, "shared subscribe");
                self.shared.subscribe(client.clone(), group, filter, *q);
                continue;
            }
            debug!(client = %client.0, filter = %f, qos = *q as u8, "subscribe");
            ordinary_granted = true;
            let already_held = prior.as_ref().is_some_and(|m| m.contains_key(f));
            self.table.subscribe(client.clone(), f.clone());
            // Retain Handling (#198, MQTT 5 §3.8.3.1): 0 = send retained at subscribe
            // (default), 1 = send only if this subscription did not already exist, 2 = never.
            // A re-forwarder (the bridge) uses 2 to avoid a replay storm on reconnect.
            match handling.get(f).copied().unwrap_or(0) {
                2 => continue,
                1 if already_held => continue,
                _ => {}
            }
            match self.retained.matching(f).await {
                Ok(matching) => {
                    let now = self.clock.now_epoch_secs();
                    for m in matching {
                        // An expired retained copy is not replayed [MQTT-3.3.2-5];
                        // the GC sweep reaps it (issue #227).
                        if m.expires_at.is_some_and(|d| d <= now) {
                            continue;
                        }
                        window_seeds.push((
                            m.topic.clone(),
                            retained_value_id(
                                &m.topic,
                                m.payload.as_ref(),
                                m.qos as u8,
                                &AppProps::from(&m.app).encode(),
                            ),
                        ));
                        retained_replay.push(Message {
                            qos: min_qos(m.qos, *q),
                            retain: true,
                            ..m
                        });
                    }
                }
                // #87 item 5: a store error here silently dropped the retained replay — a
                // new subscriber saw no retained value and had no way to know one existed.
                // Count and log it (bounded reason) so the next such incident is visible
                // rather than invisible.
                Err(e) => {
                    warn!(client = %client.0, filter = %f, error = %e,
                          "retained replay skipped: the retained store could not be read");
                    if let Some(m) = &self.metrics {
                        m.publish_dropped("retained-replay-read-failed");
                    }
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

        // #219: the advertisement above takes a hop to reach every peer, and a
        // retained commit landing elsewhere inside that hop is stored but never
        // forwarded to THIS fresh subscription. Open (or refresh) the client's
        // retained-delivery window: while it is open, the commit fan-out delivers to
        // it from the apply path, deduped against the replay below and any live
        // copies through the ledger. Swept after RETAINED_INTEREST_WINDOW.
        if ordinary_granted {
            let window = self
                .retained_windows
                .entry(client.clone())
                .or_insert_with(|| RetainedWindow {
                    until: Instant::now(),
                    seen: HashMap::new(),
                });
            window.until = Instant::now() + RETAINED_INTEREST_WINDOW;
            for (topic, id) in window_seeds {
                window.seen.insert(topic, id);
            }
        }

        if let Some(tx) = self.online.get(client).map(|s| s.tx.clone()) {
            for m in retained_replay {
                // No session-log offset (#124): a retained value is durable in the
                // retained store, and a crash mid-delivery loses the delivery, not the
                // fact — the value is still there and is replayed to the next subscribe.
                // The replay carries the REMAINING expiry interval [MQTT-3.3.2-6]; a
                // value that expired between collection and send is dropped here
                // (issue #227).
                let now = self.clock.now_epoch_secs();
                let remaining = match m.expires_at {
                    Some(d) if d <= now => continue,
                    Some(d) => Some(u32::try_from(d - now).unwrap_or(u32::MAX)),
                    None => None,
                };
                self.send_to_client(client, &tx, &m, true, remaining, None);
            }
        } else if !retained_replay.is_empty() {
            // #87 item 5: the subscription resolved retained values but the client is not
            // online to receive them, so they are dropped here. This was invisible; count
            // and log it. It is not a durability loss — the retained values are still in
            // the store and replay on the next SUBSCRIBE — but a persistent gap between
            // "subscribed" and "online" that keeps hitting this is worth seeing.
            warn!(client = %client.0, dropped = retained_replay.len(),
                  "retained replay dropped: client subscribed but is not online");
            if let Some(m) = &self.metrics {
                m.publish_dropped("retained-replay-client-offline");
            }
        }
    }

    /// Returns, per filter in request order, whether a subscription existed and
    /// was removed (issue #290). `subs_by_client` is the authority: it keeps the
    /// full filter string — `$share/` prefix included — for every grant, so one
    /// `remove` answers ordinary and shared filters alike.
    async fn unsubscribe(&mut self, client: &ClientId, filters: &[String]) -> Vec<bool> {
        let mut existed = Vec::with_capacity(filters.len());
        for f in filters {
            existed.push(
                self.subs_by_client
                    .get_mut(client)
                    .is_some_and(|map| map.remove(f).is_some()),
            );
            // #198: drop the subscription options with the subscription.
            if let Some(ids) = self.sub_ids.get_mut(client) {
                ids.remove(f);
                if ids.is_empty() {
                    self.sub_ids.remove(client);
                }
            }
            for map in [&mut self.no_local, &mut self.retain_as_published] {
                if let Some(set) = map.get_mut(client) {
                    set.remove(f);
                    if set.is_empty() {
                        map.remove(client);
                    }
                }
            }
            if let Some((group, filter)) = parse_shared(f) {
                self.shared.unsubscribe(client, group, filter);
            } else {
                self.table.unsubscribe(client, f);
            }
        }
        // A failed durable removal is not surfaced in the UNSUBACK codes: it
        // leaves the subscription durably present — the safe side (no loss,
        // possible extra deliveries until a later persist succeeds) — and the
        // in-memory removal above already answered the client's question
        // ("did I hold this subscription?"), which is all `0x00`/`0x11` claim.
        let _ = self.persist_subscriptions(client).await;
        self.gossip_interest();
        existed
    }

    /// Answer a peer's forward verdict — immediately when its fan-out submitted no
    /// lane job, otherwise by folding this on-loop half into the `(node, seq)`
    /// aggregate that the last [`AppendDone`](HubCommand::AppendDone) completes
    /// (issue #242). Either way the peer hears `Stored` only after every owed append
    /// actually stored.
    /// `relaxed` is derived by THIS node from the forwarded message's own
    /// properties under THIS node's opt-in (ADR 0072's placement rule: the tier
    /// is derived where it is acted on).
    fn finish_peer_verdict(
        &mut self,
        node: &NodeId,
        seq: u64,
        sync: DurableOutcome,
        relaxed: bool,
    ) {
        if let Some(g) = self.remote_append_pending.get_mut(&(node.clone(), seq)) {
            g.worst = g.worst.and(sync);
            // The congestion valve's owner half (issue #399): a relaxed forward
            // whose every lane submit was admitted BELOW the congestion
            // threshold is answered `Stored` now, at submit-acceptance — which
            // is exactly what a relaxed ack means (ADR 0072), and what lets the
            // origin's relaxed pending complete on one peer round trip instead
            // of an append. A congested or already-degraded forward keeps
            // today's behavior: the verdict waits for the appends (the quorum
            // rule), so the origin's window throttles to this node's drain
            // rate. Refusals stay refusals either way.
            let answer_early =
                relaxed && !g.congested && !g.answered && matches!(g.worst, DurableOutcome::Ok);
            if answer_early {
                g.answered = true;
            }
            if answer_early {
                self.answer_forward(node, seq, ForwardVerdict::Stored);
            }
            return;
        }
        self.answer_forward(node, seq, sync.to_verdict());
    }

    /// PUBACK: completes a `QoS` 1 delivery, freeing a quota slot (ADR 0012) and
    /// releasing the message's durable log entry (#124).
    fn pub_ack(&mut self, client: &ClientId, pkid: u16) {
        let completed = self.complete_pending(client, pkid, OutState::AwaitingPubAck);
        if completed {
            self.truncate_acked(client);
            self.drain_backlog(client);
        }
    }

    /// Remove `pkid` from the in-flight table if it is in `expected` state, releasing its
    /// durable offset. Returns whether the delivery completed.
    fn complete_pending(&mut self, client: &ClientId, pkid: u16, expected: OutState) -> bool {
        let Some(inf) = self.inflight.get_mut(client) else {
            return false;
        };
        if !inf.pending.get(&pkid).is_some_and(|p| p.state == expected) {
            return false;
        }
        if let Some(offset) = inf.pending.remove(&pkid).and_then(|p| p.offset) {
            inf.release(offset);
        }
        true
    }

    /// Truncate the session's durable log through the **contiguous** prefix the subscriber
    /// has acknowledged (#124) — DETACHED (ADR 0074): the watermark is handed to the
    /// flusher and this path never waits a truncate round-trip. Measured twice on the
    /// scale curve, the inline await was the durable ceiling: one serialized barrier
    /// per message pinned msg/s to the slowest disk's barrier RATE while the ADR 0071
    /// writer idled at 2.3 ops/batch. A message is written before it goes on the wire,
    /// so truncation is what finally lets go of it; nothing is truncated on send.
    fn truncate_acked(&mut self, client: &ClientId) {
        let Some(up_to) = self
            .inflight
            .get_mut(client)
            .and_then(Inflight::advance_ack)
        else {
            return;
        };
        // A closed/absent flusher is the documented not-fatal case: the entries stay
        // in the log and are replayed on the next resume. A duplicate at QoS 1 is
        // spec-legal; losing one would not be.
        if let Some(tx) = &self.truncate_tx {
            let _ = tx.send((client.clone(), up_to));
        }
    }

    /// [`truncate_acked`](Self::truncate_acked), but awaited inline — the `QoS` 2
    /// completion path keeps this (ADR 0074 Decision 2): its exactly-once rests on
    /// the durable outbound id-state (ADR 0057), and the pre-existing crash window
    /// between the outbound-id clear and the truncate must stay exactly as wide as it is.
    async fn truncate_acked_now(&mut self, client: &ClientId) {
        let Some(up_to) = self
            .inflight
            .get_mut(client)
            .and_then(Inflight::advance_ack)
        else {
            return;
        };
        if let Err(e) = self.store.ack(client, up_to).await {
            // Not fatal: the entries stay in the log and are replayed on the next resume.
            // A duplicate at QoS 1 is spec-legal; losing one would not be.
            debug!(client = %client.0, up_to, error = %e,
                   "failed to truncate the acknowledged session log");
        }
    }

    /// PUBREC: advances a `QoS` 2 delivery to the release phase (send PUBREL).
    async fn pub_rec(&mut self, client: &ClientId, pkid: u16) {
        // Is this a durable-tracked delivery? (Only `QoS` 2 with an offset was recorded.)
        let durable = self
            .inflight
            .get(client)
            .and_then(|inf| inf.pending.get(&pkid))
            .is_some_and(|p| p.state == OutState::AwaitingPubRec && p.offset.is_some());
        if durable {
            // ADR 0057: the phase advances durably BEFORE the PUBREL goes out. If this
            // write fails the PUBREL is withheld and nothing moves — the subscriber
            // re-sends PUBREC (it is waiting on us), which retries this write. Sending
            // PUBREL on a failed write would mean a crash restores to `AwaitingPubRec`
            // and re-PUBLISHes a message the subscriber already released — a duplicate
            // manufactured by our own bookkeeping.
            if let Err(e) = self.store.advance_outbound(client, pkid).await {
                warn!(client = %client.0, pkid, error = %e,
                      "outbound QoS2 phase advance failed; PUBREL withheld");
                return;
            }
        }
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

    /// PUBCOMP: completes a `QoS` 2 delivery, freeing a quota slot (ADR 0012) and
    /// releasing the message's durable log entry (#124).
    async fn pub_comp(&mut self, client: &ClientId, pkid: u16) {
        // An id still in `AwaitingIdRecord` was never sent (issue #242 finding A):
        // a PUBCOMP for it can only be a confused or malicious client, and clearing
        // the durable record below would race the lane's in-flight `record_outbound`
        // for the very same id. Ignore it; the entry's own completion owns cleanup.
        if self
            .inflight
            .get(client)
            .and_then(|inf| inf.pending.get(&pkid))
            .is_some_and(|p| p.state == OutState::AwaitingIdRecord)
        {
            return;
        }
        let completed = self.complete_pending(client, pkid, OutState::AwaitingPubComp);
        // ADR 0057: release the durable id UNCONDITIONALLY, not only when an in-memory
        // entry completed. A PUBCOMP with no pending entry is how an ORPHANED table entry
        // (a clear that failed earlier, tolerated by design) finally releases: the
        // restore sent its spurious PUBREL, the subscriber answered (MQTT-4.3.3), and
        // this is the retry the tolerance was counting on. Clearing an id the store does
        // not hold is a no-op. A failure here is logged, and the same cycle retries it.
        if let Err(e) = self.store.clear_outbound(client, pkid).await {
            warn!(client = %client.0, pkid, error = %e,
                  "outbound QoS2 id clear failed; a restore may send one spurious PUBREL");
        }
        if completed {
            self.truncate_acked_now(client).await;
            self.drain_backlog(client);
        }
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
                    .filter(|f| !policy.authorizer.authorize_subscribe(identity, client, f))
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
        self.detach(client, conn_id, false, None).await;
    }

    async fn detach(
        &mut self,
        client: &ClientId,
        conn_id: u64,
        graceful: bool,
        session_expiry_override: Option<u32>,
    ) {
        // Only act if this is still the current connection; a stale detach from a
        // connection that was already taken over must not disturb the new one.
        if self.online.get(client).map(|s| s.conn_id) != Some(conn_id) {
            return;
        }
        let departed = self.online.remove(client);
        // Deliveries still staged behind an outbound-id record never reached this
        // connection's wire (issue #242 finding A): drop them — the durable copy
        // owns delivery on reattach, and their offsets stay owed (untouched in
        // `outstanding`) so the log cannot truncate past them. The stale lane
        // completion cleans up after itself (conn fence) and drains
        // `records_pending`.
        if let Some(inf) = self.inflight.get_mut(client) {
            inf.pending
                .retain(|_, p| p.state != OutState::AwaitingIdRecord);
        }
        // §3.14.2.2.2 (issue #298): a Session Expiry Interval on the DISCONNECT
        // replaces the one agreed at CONNECT, for this detach AND for the stored
        // session — a client that reconnects without naming an interval should get
        // the terms it last asked for, not the ones it has since revised. `conn.rs`
        // has already refused the zero-to-non-zero case, so anything arriving here
        // is legal to apply.
        //
        // Applied BEFORE the Will decision below, which bounds its delay by the
        // session's lifetime: the revised terms are the ones in force.
        if let Some(secs) = session_expiry_override {
            if secs == 0 {
                self.session_expiry.remove(client);
            } else {
                self.session_expiry.insert(client.clone(), secs);
            }
        }
        // Any end other than a clean DISCONNECT publishes the will
        // [MQTT-3.14.4-3]; DISCONNECT discards it [MQTT-3.14.4-3].
        if !graceful {
            if let Some(w) = departed.and_then(|o| o.will) {
                // §3.1.3.2.2 (issue #299): publish when the delay elapses OR the
                // session ends, whichever comes FIRST — so the hold is bounded by
                // the session's own lifetime, and a session that expires at once
                // (interval 0) publishes at once no matter what delay was asked
                // for. Will Delay exists so a brief reconnect does not announce a
                // death that did not happen; without the bound it could outlive
                // the very session it describes.
                let expiry = self.session_expiry.get(client).copied().unwrap_or(0);
                let hold = w.delay_secs.min(expiry);
                if hold == 0 {
                    info!(client = %client.0, topic = %w.message.topic, "publishing will (ungraceful disconnect)");
                    self.publish_will(&w.message).await;
                } else {
                    let due = Instant::now() + Duration::from_secs(u64::from(hold));
                    info!(
                        client = %client.0, topic = %w.message.topic, delay_s = hold,
                        "holding will (will delay interval)"
                    );
                    self.pending_wills.insert(client.clone(), (w, due));
                }
            }
        }
        // Session retention (ADR 0009): expiry 0 discards now; u32::MAX keeps the
        // session indefinitely; a finite interval schedules expiry for the sweep.
        match self.session_expiry.get(client).copied() {
            None | Some(0) => {
                self.discard_session(client);
                info!(client = %client.0, "client detached (session discarded)");
                // Our local interest may have shrunk; let peers know.
                self.gossip_interest();
            }
            Some(SESSION_EXPIRY_NEVER) => {
                self.flush_backlog_to_store(client);
                info!(client = %client.0, "client detached (session retained)");
            }
            Some(secs) => {
                self.flush_backlog_to_store(client);
                // Absolute wall-clock deadline, persisted durably so a new owner expires the
                // session at the right time after a takeover instead of restarting the clock
                // (ADR 0009 §3).
                let deadline = self.clock.now_epoch_secs() + u64::from(secs);
                self.persist_detach_deadline(client, deadline).await;
                self.expiring.insert(client.clone(), deadline);
                info!(client = %client.0, expires_in_s = secs, "client detached (session expiring)");
            }
        }
    }

    /// Persist a detaching session's ABSOLUTE expiry deadline (ADR 0009 §3), or say — out
    /// loud, and on a counter — that it could not be (issue #284 round-2 finding 5).
    ///
    /// The deadline is persisted so a NEW owner expires the session at the right time
    /// after a takeover instead of never expiring it. On a clustered durable node the
    /// write is group-routed, so it is refused with `NotOwner` whenever this node does not
    /// hold the session group's lease — which, after a rehome close, is true BY
    /// CONSTRUCTION. That case is now skipped deliberately rather than attempted and its
    /// error discarded: the outcome is identical, the log line and the counter are not.
    ///
    /// **The residual, named** (ADR 0009 §3's as-delivered note, 0043-P6): the new owner
    /// then holds a session record with NO deadline, so a client that never comes back
    /// leaves a persistent session and its queue behind. It is re-established the moment
    /// the client reconnects anywhere (its CONNECT carries the Session Expiry Interval, and
    /// that owner's next detach persists the deadline) — in the measured rehome that is
    /// ~100 ms later. It cannot be fixed here without a channel that does not exist: only
    /// the absolute deadline is persisted, never the INTERVAL, so an owner cannot re-derive
    /// it, and no peer frame carries a session's deadline. The same hole is pre-existing for
    /// every takeover of an ONLINE session (the deadline is cleared while connected, so a
    /// dead owner's successor inherits none); the named follow-up — persist the interval
    /// alongside the deadline — closes both at once.
    async fn persist_detach_deadline(&self, client: &ClientId, deadline: u64) {
        if self.durable_plane.is_some() && self.clustered() && !self.owns_session(client) {
            warn!(
                client = %client.0,
                deadline,
                "session expiry deadline NOT persisted: this node does not own the \
                 session's group, so the group-routed write cannot land (ADR 0009 §3). \
                 The new owner inherits no deadline until the client reconnects \
                 (issue #284)"
            );
            if let Some(m) = &self.metrics {
                m.session_expiry_unpersisted("not-owner");
            }
            return;
        }
        if let Err(e) = self.store.set_session_expiry(client, Some(deadline)).await {
            warn!(
                client = %client.0,
                deadline,
                error = %e,
                "failed to persist the session expiry deadline (ADR 0009 §3); the session \
                 may outlive its stated interval if its owner changes"
            );
            if let Some(m) = &self.metrics {
                m.session_expiry_unpersisted("error");
            }
        }
    }

    /// Whether `client` has a retained session (survives disconnect) — its MQTT 5.0
    /// Session Expiry Interval is non-zero (ADR 0009).
    fn is_persistent(&self, client: &ClientId) -> bool {
        self.session_expiry.contains_key(client)
    }

    /// Discard a session entirely: routing subscriptions, in-flight state, the stored
    /// queue/metadata, and all expiry bookkeeping. Used by a zero-expiry disconnect
    /// and the expiry sweep (Clean Start has its own lane-serialized path in
    /// [`attach`](Self::attach), gated on the CONNACK).
    ///
    /// The durable `remove` is serialized through the session's append lane when
    /// jobs are in flight (issue #242 finding C): a direct remove racing an admitted
    /// append lets the append land AFTER it and silently re-create the queue with a
    /// ghost message that resurrects the discarded session on a later persistent
    /// reconnect. An idle lane (or none) has nothing to race, so the remove is
    /// simply spawned off-loop — it was previously awaited INLINE here, the same
    /// class of loop stall the #242 motion exists to remove.
    fn discard_session(&mut self, client: &ClientId) {
        self.discard_session_local(client);
        if self
            .append_lanes
            .get(client)
            .is_some_and(|l| l.outstanding > 0)
        {
            let lane = self
                .append_lanes
                .get_mut(client)
                .expect("checked just above");
            // A control job: admitted into the LANE_CONTROL_HEADROOM slots even at
            // the delivery cap.
            if lane
                .tx
                .try_send(LaneJob::Remove {
                    client: client.clone(),
                })
                .is_ok()
            {
                lane.outstanding += 1;
                return;
            }
            warn!(client = %client.0,
                  "append lane full at session discard; falling back to a spawned \
                   remove — a still-in-flight append may re-create the queue \
                   (issue #242)");
        }
        let store = self.store.clone();
        let client = client.clone();
        self.spawn_owned(async move {
            let _ = store.remove(&client).await;
        });
    }

    /// The in-memory half of discarding a session (routing, in-flight, expiry state).
    /// Fast and loop-safe; the durable `remove` is done separately (off-loop for a
    /// clean-start attach, ADR 0017).
    fn discard_session_local(&mut self, client: &ClientId) {
        self.drop_subscriptions(client);
        self.inflight.remove(client);
        self.session_expiry.remove(client);
        self.expiring.remove(client);
        self.retained_windows.remove(client);
    }

    /// Discard every session whose MQTT 5.0 Session Expiry Interval has elapsed
    /// (ADR 0009). Runs on the hub's periodic sweep tick.
    // `%`-and-compare, not `is_multiple_of` (stable 1.87; MSRV 1.85) — drop the
    // allow when the floor rises.
    #[allow(clippy::manual_is_multiple_of)]
    async fn sweep_expired_sessions(&mut self) {
        // Wills held for their delay (§3.1.3.2.2, issue #299) ride this same tick
        // rather than each arming a timer: one clock for "time passed and something
        // is due" is easier to reason about than N, and the 1s cadence is already
        // the granularity session expiry is judged at. A client that reconnects in
        // the meantime removed its entry at attach.
        let now = Instant::now();
        let due: Vec<(ClientId, Will)> = self
            .pending_wills
            .iter()
            .filter(|(_, (_, at))| *at <= now)
            .map(|(c, (w, _))| (c.clone(), w.clone()))
            .collect();
        for (client, will) in due {
            self.pending_wills.remove(&client);
            info!(
                client = %client.0, topic = %will.message.topic,
                "publishing will (delay elapsed)"
            );
            self.publish_will(&will.message).await;
        }

        // Ring-change watch (ADR 0043 P2): any placement member-set change moves
        // group ownership (growth moves ~1/N of the groups onto the joiner), so it
        // re-arms the takeover window — every node then scans eagerly (the NEW
        // owner materializes the moved sessions and advertises their interest; the
        // OLD owner un-materializes them, below) and gated publish acks hold until
        // the window settles, instead of waiting for a client to touch the moved
        // groups. Shrink arrives here too (via SWIM eviction), overlapping the
        // `PeerDead` arm — harmless.
        if let Some(placement) = &self.placement {
            let (members, epoch): (BTreeSet<NodeId>, u64) = {
                let p = placement
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (p.members().into_iter().collect(), p.ownership_epoch())
            };
            if members != self.known_members {
                if !self.known_members.is_empty() {
                    info!(
                        members = members.len(),
                        "placement membership changed; eager migration window armed (ADR 0043 P2)"
                    );
                }
                self.known_members = members;
                self.takeover_reconcile_ticks = self.takeover_reconcile_ticks.max(8);
            }
            // Issue #294: a COMMITTED-lease move with no membership change — an
            // assigner rebalance, a lease-leader change, a paced resize drain —
            // used to arm nothing anywhere, so `release_moved_sessions` could
            // release a moved session's routing with no successor claim and a
            // `matched == 0` fan-out was answered `Stored` for a message nobody
            // held. The ownership epoch moves on exactly those events (and on
            // the membership ones above — double-arming is a no-op under max),
            // and arming HERE inherits the one pairing that makes it safe: the
            // very next block runs the inherited-session scan while the window
            // is open, and that scan's completion is what settles held acks.
            // Every durable node's driver pushes the same lease map within a
            // reconcile tick, so the window opens cluster-wide — including at a
            // third-node origin whose fan-out would otherwise conclude "nobody
            // is owed this" from its stale gossiped interest.
            if self.sweep_epoch_seen != Some(epoch) {
                if self.sweep_epoch_seen.is_some() {
                    info!(
                        epoch,
                        "committed ownership moved; eager migration window armed (issue #294)"
                    );
                    self.takeover_reconcile_ticks = self.takeover_reconcile_ticks.max(8);
                }
                self.sweep_epoch_seen = Some(epoch);
            }
        }

        // The LIVE half of the same rule (issue #284): a session already CONNECTED
        // here whose group's committed lease has moved away is closed, so the client
        // relocates instead of waiting out a keepalive on a stale placement. Runs
        // right after the membership watch above, so a membership-driven lease move
        // is evaluated on the very tick that arms the window.
        self.rehome_misplaced_sessions().await;

        // Liveness backstop for the interest-authoritative gate (0043-P4 exhibit
        // ②): a node whose scans NEVER complete (quorum lost for good) must not
        // isolate its live clean-session clients forever — after a generous bound
        // it gossips its live-only interest, loudly. In this degraded state the
        // durable sessions it is hiding cannot be durably enqueued-to anywhere
        // anyway (the same lost quorum withholds those acks).
        if !self.interest_authoritative {
            self.interest_suppressed_ticks += 1;
            if self.interest_suppressed_ticks >= EXPIRY_RECONCILE_EVERY {
                warn!(
                    ticks = self.interest_suppressed_ticks,
                    "interest gossip forced authoritative: the boot session scan never \
                     completed (degraded durable plane?) — advertising live interest only"
                );
                self.interest_authoritative = true;
                self.gossip_interest();
            }
        }

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

        // Retained anti-entropy (issue #87): re-offer our digest to every peer on a
        // slow cadence. A commit fans out ONE unacked frame; before this, a frame
        // dropped as oversized/unencodable — or lost with the link still up — left
        // that peer permanently divergent, because the only other reconciliation was
        // a digest at link-up. Peers in sync compare equal and transfer nothing.
        // #219: fresh-subscription retained-delivery windows expire on the sweep.
        if !self.retained_windows.is_empty() {
            let now = Instant::now();
            self.retained_windows.retain(|_, w| w.until > now);
        }
        self.retained_antientropy_tick = self.retained_antientropy_tick.wrapping_add(1);
        if self.retained_may_expire {
            self.reap_expired_retained().await;
        }
        if !self.retained_tombstone_observed_at.is_empty() {
            self.reap_discharged_tombstones().await;
        }

        if self.retained_antientropy_tick % RETAINED_ANTIENTROPY_EVERY == 0 {
            // Re-learn committed retained state this process no longer remembers
            // BEFORE offering the digest built from it (issue #183): a restart
            // empties the in-memory token map, and a tombstoned topic leaves no
            // cache entry to rediscover the fence from either.
            self.warm_retained_tokens_from_authority().await;
            self.broadcast_retained_digest().await;
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
            self.discard_session(client);
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
        self.spawn_owned(async move {
            let scan = match store.all_sessions().await {
                Ok(v) => v,
                Err(e) => {
                    debug!(error = %e, "inherited-session scan failed; retried next tick");
                    // A failed enumeration saw nothing — emphatically not complete.
                    mqtt_storage::SessionScan {
                        sessions: Vec::new(),
                        complete: false,
                    }
                }
            };
            debug!(
                sessions = scan.sessions.len(),
                complete = scan.complete,
                "inherited-session scan finished"
            );
            let _ = tx.send(HubCommand::InheritedSessions {
                sessions: scan.sessions,
                complete: scan.complete,
            });
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
    fn inherit_sessions(
        &mut self,
        sessions: Vec<(ClientId, Vec<Subscription>, Option<u64>)>,
        complete: bool,
    ) {
        self.inherited_scan_inflight = false;
        self.last_scan_complete = complete;
        // The interest-authoritative transition (0043-P4 exhibit ②): once one
        // COMPLETE scan has landed over a WHOLE mesh, this hub's routing table
        // reflects every durable session it owns, and its interest snapshots
        // stop being lies of omission — gossip opens (below and from now on).
        if !self.interest_authoritative && complete && self.mesh_whole() {
            self.interest_authoritative = true;
            debug!("interest gossip authoritative: first complete whole-mesh session scan landed");
        }
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
        if registered || self.interest_authoritative {
            // Peers must know this node now routes the inherited filters — this is
            // what re-targets acked forwards after a takeover (exhibit ⑤ re-route).
            // Sent unconditionally once authoritative: peers that connected while
            // gossip was suppressed have no snapshot yet (0043-P4 exhibit ②).
            self.gossip_interest();
        }
        self.release_moved_sessions();
        self.settle_pending_publishes();
    }

    /// The mirror of [`inherit_sessions`] for a ring that GREW (ADR 0043 P2):
    /// un-materialize offline sessions whose group this node no longer owns.
    /// Keeping them routed would keep attracting publishes and forwards this node
    /// can no longer durably enqueue (`NotOwner` — every such ack is withheld),
    /// and its stale gossiped interest would keep re-targeting them here. Only the
    /// in-memory routing state is dropped — the durable data stays (this node is
    /// usually still a replica); the NEW owner's own scan materializes and
    /// advertises the session, and the client re-attaches there (ADR 0005).
    /// Durable-cluster mode only: with local-only session storage the data cannot
    /// follow the ownership move, so dropping routing would drop delivery.
    ///
    /// Its ONLINE-excluding filter is the other half of one rule: the live case is
    /// converted into this offline one first, by
    /// [`rehome_misplaced_sessions`](Self::rehome_misplaced_sessions) (issue #284).
    /// Widening this filter instead would drop the routing out from under a connected
    /// client and leave it attached to a node that can no longer enqueue for it —
    /// silently undeliverable, which is the defect, not the fix.
    ///
    /// **A rehome (issue #284) adds a TRIGGER to this path, not a new one.** The rehome
    /// close turns a misplaced live session into exactly the offline, moved session this
    /// filter already matches, and then does nothing else: the routing stays until this
    /// function's own scan releases it, on its own pre-existing cadence. That is
    /// deliberate. While the routing is here, this node still advertises the session's
    /// filters, so every publish toward it is answered by a node that KNOWS the message
    /// is owed — the append fails `NotOwner` and the publisher's ack is WITHHELD, locally
    /// and as a peer forward alike. Releasing sooner (arming the eager window at the
    /// close, say) would shorten that honest window by widening the dishonest one below.
    ///
    /// **The release itself is unwitnessed** — justified by evidence that this node is
    /// not the owner, never by evidence that the new owner routes the session. Since
    /// issue #294 the window that opens is COVERED rather than silent: the sweep watches
    /// the committed ownership epoch and arms the same scan-paired eager window a
    /// membership change arms, on every durable node (each one's driver pushes the same
    /// replicated lease map within a tick). So this release can only run inside an armed
    /// window, a `matched == 0` fan-out during it is HELD (`routing_unsettled()`), a
    /// peer's forward is answered `Failed` ("cannot say, retry") instead of `Stored`,
    /// and the new owner's own armed scan materialises the session within the same
    /// window. The RESIDUAL, stated: if the owner never claims (dead mid-move, session
    /// expired there), a publish after the window settles reaches the documented
    /// no-known-subscriber ack-and-drop arm — closing that fully needs per-session
    /// hand-off evidence on the peer bus (`PeerMessage::Interest` carries FILTERS, not
    /// client ids), recorded in ADR 0043's amendment as the exit-1 follow-up. See
    /// 0043-P6. Note the pairing this function already gets right, which any armer must
    /// preserve: its one call site calls
    /// [`settle_pending_publishes`](Self::settle_pending_publishes) on the very next line
    /// — the only thing that clears a held ack.
    fn release_moved_sessions(&mut self) {
        if self.durable_plane.is_none() || !self.clustered() {
            return;
        }
        let moved: Vec<ClientId> = self
            .subs_by_client
            .keys()
            .filter(|c| {
                !self.online.contains_key(*c)
                    && !self.connecting.contains_key(*c)
                    && !self.owns_session(c)
            })
            .cloned()
            .collect();
        if moved.is_empty() {
            return;
        }
        for client in &moved {
            self.discard_session_local(client);
            debug!(
                client = %client.0,
                "session's group moved to another owner; local routing released (ADR 0043 P2)"
            );
        }
        // Peers must see the shrunken interest, so forwards stop targeting this
        // node for filters it no longer serves.
        self.gossip_interest();
    }

    /// The live persistent sessions hosted here whose group's COMMITTED lease names
    /// another node, as `(client, committed owner, whether this node can route the
    /// client's next CONNECT to that owner)` — the input to
    /// [`rehome_misplaced_sessions`](Self::rehome_misplaced_sessions).
    ///
    /// Runs against an ownership SNAPSHOT and takes **no lock at all** (issue #284 round-2
    /// finding 3). The placement lock is shared with the cluster driver's per-tick
    /// lease/voter pushes; the original form held its read guard across the whole pass, so
    /// that writer stalled behind an `O(online sessions)` scan once a second. The snapshot
    /// is `O(NUM_GROUPS + members)` to take and answers exactly the two questions this
    /// pass asks (`CommittedOwnership`).
    ///
    /// Note that `relocatable` no longer needs a second `owner_route` comparison:
    /// `committed_group_owner` is the eligible-filtered lease map, and `group_owner` is
    /// the same map with an HRW fallback used only where it is EMPTY — so wherever a
    /// committed owner exists, it *is* the routed owner. All that remains to check is
    /// whether its peer-link address is known.
    fn misplaced_candidates(
        &self,
        own: &mqtt_cluster::placement::CommittedOwnership,
    ) -> Vec<(ClientId, NodeId, bool)> {
        self.online
            .keys()
            // Persistent sessions only. A clean session is never relocated (ADR 0005
            // §1) and never durably enqueued to (`deliver_to_client` skips it — it has
            // nothing to resume into), so it cannot wedge this way, and closing it
            // would DESTROY it rather than move it.
            .filter(|c| {
                !matches!(self.session_expiry.get(*c).copied(), None | Some(0))
                    && !self.connecting.contains_key(*c)
            })
            .filter_map(|c| {
                let owner = own.session_owner(&c.0)?;
                if *owner == self.node_id {
                    return None;
                }
                // Can this node actually send the client somewhere better? If the
                // owner's peer address is unknown, the next CONNECT here is served
                // locally (ADR 0005 §5) and a close would only loop.
                Some((c.clone(), owner.clone(), own.is_relocatable(owner)))
            })
            .collect()
    }

    /// The committed-ownership snapshot for this tick, or `None` when the pass can be
    /// SKIPPED because nothing that could change a session's placement has changed since
    /// the last one (issue #284 round-2 finding 3).
    ///
    /// The skip is what removes the steady-state cost: measured at 5000 online sessions,
    /// the unconditional pass took the `sweep` dispatch from ~4.0 ms to ~6.6 ms/tick,
    /// forever, for a condition that only arises when a lease moves. The version
    /// ([`Placement::ownership_epoch`]) moves on exactly the events that can make a
    /// session misplaced, so a skipped tick cannot miss one.
    ///
    /// Sessions already under observation (`misplaced`, i.e. mid-grace, cooling down, or
    /// standing unrelocatable) keep the pass running: their grace has to be counted, and
    /// the epoch does not move again while they wait.
    ///
    /// The skip is edge-triggered on OWNERSHIP, which is only one of the two ways a session
    /// becomes misplaced; the other is the session ARRIVING on a non-owning node after the
    /// epoch settled, which moves no lease. That second edge is covered by `finish_attach`
    /// seeding the `misplaced` entry above, so this skip cannot lose it. Without that seed
    /// the pass never ran again and the issue #284 wedge returned silently, with
    /// `mqttd_misplaced_sessions` reading 0 — pinned by
    /// `a_session_that_becomes_misplaced_by_attaching_is_rehomed`.
    fn ownership_for_rehome(&mut self) -> Option<mqtt_cluster::placement::CommittedOwnership> {
        let placement = self.placement.as_ref()?;
        let p = placement
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.ownership_epoch_seen == Some(p.ownership_epoch()) && self.misplaced.is_empty() {
            return None;
        }
        let own = p.committed_ownership();
        drop(p);
        self.ownership_epoch_seen = Some(own.epoch());
        Some(own)
    }

    /// The LIVE mirror of [`release_moved_sessions`] (issue #284, 0043-P6): close a
    /// connected persistent session whose placement group this node does not own, so
    /// the client relocates to the real owner on the reconnect it already knows how to
    /// do — promptly, instead of sitting silently undeliverable until its keepalive
    /// fires on the dead air.
    ///
    /// **Why it is needed at all.** Relocation (ADR 0005) is decided ONCE, at CONNECT.
    /// A node readmitted after a roll rejoins SWIM membership — and turns `/readyz`
    /// green — before it is back in the lease voter set, so for a couple of seconds its
    /// groups' leases are still parked on the interim holder they were handed during
    /// its absence. A session that resumes inside that window is relocated onto the
    /// interim holder *correctly*, and is stranded there seconds later when the lease
    /// legitimately returns. From then on the interim holder refuses every publish
    /// toward that session (`NotOwner`, ack withheld — honest, but unavailable) and
    /// nothing re-evaluates a LIVE session's placement. Measured: unbounded, still
    /// wedged after two minutes, on a cluster reporting fully converged and ready.
    ///
    /// **Why the decision lives on the sweep tick** rather than on the refusal: the
    /// refusal is traffic-driven, so it cannot see an idle misplaced session (which is
    /// just as undeliverable, only nobody is watching), and the placement is pushed
    /// into the ring by the cluster driver with no channel to the hub. Re-deriving the
    /// condition each tick is self-correcting after a missed edge, and it does not have
    /// to be right the first time.
    ///
    /// **Why a close and not a transparent re-relocation:** ADR 0005's proxy is
    /// structurally one hop (`run_framed` refuses to re-proxy — a chain would loop),
    /// the CONNECT and its CONNACK (with a `session_present` computed against THIS
    /// node's state) are already on the wire with no vocabulary for replaying them
    /// elsewhere, and a hand-off would make exactly-one-owner a timing argument. The
    /// close is single-writer by construction: this node drops the session before the
    /// client can learn to reconnect, and the reconnect is an ordinary CONNECT through
    /// the existing takeover fence.
    ///
    /// **The close ends the CONNECTION and NOTHING ELSE** — no routing change, no
    /// interest change, no settle-window arming (issue #284 round 3). Releasing the
    /// routing here, as the first cut did by calling
    /// [`release_moved_sessions`](Self::release_moved_sessions) at the close, is an
    /// acked-but-dropped publish: a lease move arms no settle window on ANY node, so once
    /// this node stops advertising the filters and the owner has not started, a publisher
    /// (typically on a third node, which decides its fan-out purely from gossiped
    /// interest) matches nobody, concludes nothing is owed, and acks a message no node
    /// stored. So the close leaves the routing where it is and lets the PRE-EXISTING
    /// [`release_moved_sessions`](Self::release_moved_sessions) — which is paired with the
    /// only thing that clears a held ack — take the session on its own cadence. Until it
    /// does, this node still advertises the session's filters and every publish toward it
    /// is answered by a node that knows the message is owed: `NotOwner` → ack WITHHELD
    /// locally, `Failed` for a peer's forward, and at a third node the owner's `Stored`
    /// composed with our `Failed` still withholds (`forward_answered` is
    /// first-terminal-verdict-wins). The cost is stated rather than hidden: for as long as
    /// the old node holds the routing, BOTH nodes advertise the session's filters, so
    /// publishers to them are withheld and retry even once the session is healthy on the
    /// owner — bounded by that scan cadence (≈1 tick inside a roll, whose membership
    /// change arms eager scans on every node; up to `EXPIRY_RECONCILE_EVERY` for a lease
    /// move with no membership change).
    ///
    /// Arming the pre-existing `takeover_reconcile_ticks` here was CONSIDERED AND
    /// REJECTED (recorded so it is not re-litigated): it is a one-line reuse of a
    /// reviewed, scan-paired armer and it would shorten the double-advertise window above
    /// to ~2 ticks — but it does so by making this node RELEASE sooner, which widens
    /// `release_moved_sessions`' pre-existing unwitnessed-release window (the new owner
    /// may not have materialised the session yet). That buys availability with ack
    /// honesty, and an unbounded honest refusal beats a bounded lie.
    ///
    /// Nothing durable is repaired or moved: the session's queue is in its group's
    /// replicated log, which the real owner holds — the client's reconnect there replays
    /// it with `session_present = 1`. One durable ITEM does not survive the move: a finite
    /// Session Expiry Interval's persisted deadline (ADR 0009 §3) cannot be written from a
    /// non-owner, so it is deliberately skipped and counted rather than attempted and
    /// swallowed — see [`persist_detach_deadline`](Self::persist_detach_deadline).
    ///
    /// Four bounds keep a flapping placement from becoming a close loop:
    /// [`MISPLACED_GRACE_TICKS`] of continuous observation; a COMMITTED lease naming
    /// another node (never the HRW fallback — see
    /// [`Placement::committed_session_owner`]); the owner being relocatable at all (else
    /// ADR 0005 §5's degrade-don't-refuse would serve the next CONNECT locally again,
    /// forever); and a per-session [`REHOME_COOLDOWN`].
    ///
    /// A fifth bound is on the AGGREGATE: [`REHOME_CLOSES_PER_TICK`] closes per tick, the
    /// rest deferred to later ticks (and counted), so a resize that moves ~1/N of groups
    /// at once drains at a paced rate instead of closing — and will-publishing for —
    /// every affected session in one dispatch.
    ///
    /// Durable-cluster mode only, for [`release_moved_sessions`]' reason: with
    /// local-only session storage the data cannot follow the ownership move, so closing
    /// the client would strand it rather than heal it.
    async fn rehome_misplaced_sessions(&mut self) {
        if self.durable_plane.is_none() || !self.clustered() {
            self.misplaced.clear();
            return;
        }
        // Skip the O(online sessions) pass entirely while no committed lease has moved
        // (round-2 finding 3). Nothing is forgotten: the version moves on exactly the
        // events that can make a session misplaced, and a session already under
        // observation keeps the pass alive until its episode ends.
        let Some(own) = self.ownership_for_rehome() else {
            return;
        };
        let candidates = self.misplaced_candidates(&own);

        // Forget sessions whose condition cleared — but KEEP a cooldown that is still
        // running, or a re-attach to this same non-owning node would be closed again
        // immediately and the bound would be no bound at all.
        let now = Instant::now();
        self.misplaced.retain(|client, m| {
            let still = candidates.iter().any(|(c, _, _)| c == client);
            if !still {
                m.ticks = 0;
                m.noted = false;
                m.deferred = false;
            }
            still
                || m.last_kick
                    .is_some_and(|t| now.duration_since(t) < REHOME_COOLDOWN)
        });
        if let Some(m) = &self.metrics {
            m.set_misplaced_sessions(candidates.len());
        }

        let mut closed = 0usize;
        let mut deferred = 0usize;
        let mut newly_deferred = 0usize;
        for (client, owner, relocatable) in candidates {
            let entry = self.misplaced.entry(client.clone()).or_default();
            entry.ticks = entry.ticks.saturating_add(1).min(MISPLACED_GRACE_TICKS);
            if entry.ticks < MISPLACED_GRACE_TICKS {
                continue;
            }
            let cooling = entry
                .last_kick
                .is_some_and(|t| now.duration_since(t) < REHOME_COOLDOWN);
            if !relocatable || cooling {
                let first = !std::mem::replace(&mut entry.noted, true);
                if first {
                    self.note_unactionable_misplacement(&client, &owner, relocatable, cooling);
                }
                continue;
            }
            // The per-tick close cap (round-2 finding 4). The pass re-derives its
            // candidates every tick, so the remainder simply goes next tick; the cost of
            // deferring is one more second of a wedge that was measured unbounded, and
            // the benefit is that a mass ownership move does not close (and will-publish
            // for) every affected session in one dispatch. Counted, so it is visible.
            if closed >= REHOME_CLOSES_PER_TICK {
                deferred += 1;
                // Counted ONCE per session per deferral episode, not once per tick.
                if !std::mem::replace(&mut entry.deferred, true) {
                    newly_deferred += 1;
                }
                continue;
            }
            // Re-read the connection: an EARLIER candidate's `detach` was awaited on
            // this loop, so this one may have ended meanwhile. Nothing to close — and
            // no cooldown to charge a session that was never closed.
            let Some(online) = self.online.get(&client) else {
                continue;
            };
            let conn_id = online.conn_id;
            // MQTT 5 can be told why; 3.1.1 has no client-redirect mechanism and just
            // sees the close (ADR 0005 acknowledges this). No Server Reference
            // property: the placement holds PEER-BUS addresses, and handing a client
            // one would point it at the cluster's internal listener. `0x9C` is the
            // honest code — "temporarily use another server" — where `0x8E`
            // (session taken over) would be a lie: nothing took this session over.
            if online.admission.protocol == ProtocolVersion::V5 {
                let _ = online.tx.send(Packet::Disconnect(Disconnect {
                    reason: mqtt_codec::reason::USE_ANOTHER_SERVER,
                    properties: mqtt_codec::Properties::new(),
                }));
            }
            warn!(
                client = %client.0,
                owner = %owner.0,
                "session hosted on a node that does not own its group; closing it so \
                 the client relocates to the owner (issue #284, ADR 0005)"
            );
            if let Some(m) = &self.metrics {
                m.session_rehomed("stale-owner");
            }
            if let Some(m) = self.misplaced.get_mut(&client) {
                m.last_kick = Some(now);
                // The next episode earns its own grace (and its own warning), and its own
                // deferral count.
                m.ticks = 0;
                m.noted = false;
                m.deferred = false;
            }
            // The queued DISCONNECT drains to the wire before the dropped outbound
            // closes the writer — exactly `evict`'s shape.
            //
            // `graceful = false`, decided deliberately (round-2 finding 2). `graceful`
            // controls exactly one thing — will publication — and suppressing it here
            // would make the rehome the ONLY broker-initiated close that hides a will:
            // session takeover and `evict` both fire it, and issue #265 existed precisely
            // because broker-initiated closes were silently NOT firing it (its exit 1:
            // "every broker-initiated close fires the Will"; its exit 2, documenting
            // suppression, was rejected as a spec violation). The spec agrees:
            // [MQTT-3.1.2-8] / §3.14.4 delete the will only on a CLIENT DISCONNECT with
            // reason 0x00, which a server `0x9C` is not. The spec's own answer to "this
            // close is not a death" is the Will Delay Interval (0x18) — which this broker
            // decodes but does not honour, and honouring it across a CLUSTER needs the
            // delay and its cancellation to survive the client reconnecting on a
            // DIFFERENT node, which no peer frame or durable record expresses today. That
            // is the named follow-up; until then the cost (one LWT per rehomed session,
            // paced by REHOME_CLOSES_PER_TICK) is documented in OPERATIONS and
            // TROUBLESHOOTING and locked by a test.
            self.detach(&client, conn_id, false, None).await;
            // ...and NOTHING else. The close ends the CONNECTION; the session's routing,
            // its gossiped interest and the settle machinery are left exactly as they
            // were, so the session becomes an ordinary offline persistent session on a
            // node that no longer owns its group — precisely what
            // [`release_moved_sessions`](Self::release_moved_sessions) already handles, on
            // its own pre-existing cadence, from the inherited-session scan that also
            // clears held acks. Every publish toward it meanwhile is answered by a node
            // that still knows the message is owed: `NotOwner` locally, `Failed` as a peer
            // forward, and a withhold at a third node where the owner's `Stored` composes
            // with our `Failed`. See the module tests
            // `no_publish_toward_a_rehomed_session_is_ever_acked_while_this_node_routes_it`
            // and `a_third_node_composes_a_refusal_and_a_store_into_a_withhold`.
            closed += 1;
        }
        if deferred > 0 {
            info!(
                deferred,
                newly_deferred,
                cap = REHOME_CLOSES_PER_TICK,
                "rehome close cap reached this tick; the remaining misplaced sessions are \
                 deferred to later ticks (issue #284)"
            );
            if let Some(m) = &self.metrics {
                // Only the sessions deferred for the FIRST time in their current episode:
                // `deferred` is this tick's backlog, which the same sessions re-enter on
                // every tick until they are closed.
                for _ in 0..newly_deferred {
                    m.session_rehomed("deferred");
                }
            }
        }
    }

    /// Say — once per episode, not once a second — that a misplaced live session cannot be
    /// acted on: either its owner has no known address (ADR 0005 §5 keeps serving it
    /// locally rather than closing it into a reconnect loop) or it is inside its rehome
    /// cooldown. Either way the session stays undeliverable, so it is warned and counted.
    ///
    /// BOTH facts are passed, and the precedence is stated rather than inferred:
    /// **unrelocatable wins**, because an owner with no known address is a mesh problem an
    /// operator can act on, while a cooldown resolves itself. An earlier version took only
    /// `cooling` and was handed `relocatable` at its one call site — correct solely because
    /// the guard there (`!relocatable || cooling`) makes `relocatable == true` imply
    /// `cooling == true`. Two operator-facing alert rows key off the label this picks, so
    /// the coincidence is not worth keeping: widening that guard would have silently
    /// relabelled them.
    fn note_unactionable_misplacement(
        &self,
        client: &ClientId,
        owner: &NodeId,
        relocatable: bool,
        cooling: bool,
    ) {
        if relocatable && cooling {
            warn!(
                client = %client.0,
                owner = %owner.0,
                "session is misplaced again within the rehome cooldown; not closing it \
                 (placement flapping, or the client keeps landing on a non-owning node) \
                 — issue #284"
            );
        } else {
            warn!(
                client = %client.0,
                owner = %owner.0,
                "session hosted here but its group is owned by a node whose address is \
                 unknown; serving locally (ADR 0005 §5) — publishes toward it are REFUSED \
                 until the peer mesh heals (issue #284)"
            );
        }
        if let Some(m) = &self.metrics {
            m.session_rehomed(if relocatable && cooling {
                "cooldown"
            } else {
                "unrelocatable"
            });
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
        self.peers_all(|_| true)
    }

    /// Whether this node's ROUTING VIEW is still settling (ADR 0042 T9 /
    /// 0043 P2/P4): a takeover or membership window is open, a session scan is
    /// in flight, the mesh is not whole, or this hub's own boot scan has not
    /// yet made its interest authoritative. While true, "I found no interested
    /// party" is an unsafe conclusion — for gating a local publish's ack
    /// (`register_pending`) and for answering a peer's gated forward that
    /// matched nothing here (the receiver-side twin). Only meaningful on a
    /// multi-node cluster.
    fn routing_unsettled(&self) -> bool {
        self.clustered()
            && (self.takeover_reconcile_ticks > 0
                || self.inherited_scan_inflight
                || !self.interest_authoritative
                || !self.last_scan_complete
                || !self.mesh_settled())
    }

    /// [`mesh_whole`](Self::mesh_whole), strengthened (0043-P4 exhibit ②):
    /// every membership-alive peer has a live link AND has sent an interest
    /// snapshot on it. A freshly-restarted peer suppresses its snapshot until
    /// its own routing view is authoritative — so until it speaks, this node
    /// cannot tell "it routes nothing" from "it has not finished recovering
    /// what it routes", and no gated ack may conclude "nobody is owed this".
    fn mesh_settled(&self) -> bool {
        self.peers_all(|p| p.interest_synced)
    }

    /// The single definition of "every membership-alive peer's link satisfies
    /// `pred`" that both honesty gates above resolve through. Membership and the
    /// self-exclusion live HERE, once: any change to *which members count*
    /// (decommissioning nodes, learners, suspect handling) lands in one place, or
    /// [`mesh_whole`](Self::mesh_whole) and [`mesh_settled`](Self::mesh_settled)
    /// would silently disagree about the mesh they are judging. No placement ⇒
    /// standalone ⇒ trivially true.
    fn peers_all(&self, pred: impl Fn(&Peer) -> bool) -> bool {
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
            .all(|m| self.peers.get(m).is_some_and(&pred))
    }

    /// Whether this node is part of a cluster: peer networking is CONFIGURED
    /// (set at startup), or the live placement already shows more than one
    /// member. The configured flag matters on a freshly (re)started node
    /// (0043-P4 exhibit ②): for its first moments it sees a single-member ring
    /// — indistinguishable, by membership alone, from a standalone broker —
    /// and judging by live membership would switch every cluster honesty gate
    /// off exactly while its view is at its most incomplete.
    fn clustered(&self) -> bool {
        self.cluster_configured
            || self.placement.as_ref().is_some_and(|p| {
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
    /// Recompute the ADR 0073 scale-out ownership capability: every placement
    /// member must have last negotiated peer proto >=
    /// [`mqtt_cluster::peer::PROTO_OWNERSHIP_DOMAIN`] (this node's own build
    /// trivially qualifies). Conservative by construction: an unknown proto — a
    /// member seen in gossip whose link has not yet completed its first
    /// handshake, or a rolled-back binary — reads as not-capable, and the whole
    /// cluster holds ADR 0049's voter-bounded domain until it clears. Transitions
    /// are edge-logged; the committed lease map keeps arbitrating actual serving
    /// throughout, so a flip is a placement-preference change, never a
    /// serving-truth change.
    fn refresh_ownership_domain(&self) {
        let Some((flag, enabled)) = &self.ownership_domain else {
            return;
        };
        let all_capable = *enabled
            && self.placement.as_ref().is_some_and(|p| {
                let members: Vec<NodeId> = {
                    let p = p.read().unwrap_or_else(std::sync::PoisonError::into_inner);
                    p.members_snapshot()
                        .into_iter()
                        .map(|(id, _, _)| id)
                        .collect()
                };
                members.iter().all(|id| {
                    *id == self.node_id
                        || self.known_peer_protos.get(id).copied().unwrap_or(0)
                            >= mqtt_cluster::peer::PROTO_OWNERSHIP_DOMAIN
                })
            });
        let was = flag.swap(all_capable, std::sync::atomic::Ordering::Relaxed);
        if was != all_capable {
            if all_capable {
                info!(
                    "durable ownership domain EXPANDED to all members (ADR 0073): every \
                     member advertises the scale-out capability"
                );
            } else {
                warn!(
                    "durable ownership domain RESTRICTED to the lease voters (ADR 0049 \
                     posture): a member lacks the scale-out capability (rolled-back \
                     binary, or a first handshake still pending)"
                );
            }
        }
    }

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
        // Flow-control backlog bytes across sessions (issue #241): so an operator can SEE
        // the number before choosing a byte cap, and watch it after.
        // Both, from one pass: the SUM answers "how much RAM is in backlogs on this node",
        // while the MAX is what a PER-SUBSCRIBER cap must be sized against (issue #241 review
        // — the docs pointed operators at the sum for a per-subscriber decision).
        m.set_backlog_bytes(self.inflight.values().map(|i| i.backlog.bytes()).sum());
        m.set_backlog_bytes_max(
            self.inflight
                .values()
                .map(|i| i.backlog.bytes())
                .max()
                .unwrap_or(0),
        );
        // Append-lane saturation (issue #242): sustained growth here is the warning
        // BEFORE `publish_dropped{reason="append-backlog-full"}` starts firing.
        m.set_append_lane_jobs(self.append_lanes.values().map(|l| l.outstanding).sum());
        if let Ok(n) = self.retained.count().await {
            m.set_retained_messages(n);
        }
        // Cluster shape (ADR 0020-T6): placement-eligible members and live peer links.
        m.set_peer_links(self.peers.len());
        if let Some(placement) = &self.placement {
            if let Ok(p) = placement.read() {
                m.set_cluster_members(p.member_count());
                // Replication health (issue #167): the silent min(R, members)
                // degradation, surfaced. min_actual < desired = at least one group
                // commits on fewer copies than the operator configured.
                // The resolved write floor rides along (issue #239): min_actual below it
                // means durable writes are being REFUSED, which is a different (pageable)
                // condition from merely under-replicated. Read from the same guard.
                let health = p.replication_health();
                m.set_replication_health(health.desired, health.min_actual, p.min_replicas());
                // Held retained tombstones (issue #229): growth here means a
                // chronically absent roster member or chronic divergence — both
                // alert-worthy on their own.
                m.set_retained_tombstones(self.retained_tombstone_observed_at.len());
            }
        }
        // Lease-group role/epoch, read from the durable plane's raft metrics (durable mode).
        if let Some(plane) = &self.durable_plane {
            let (is_leader, epoch) = plane.lease_role();
            m.set_lease_role(is_leader, epoch);
            // ADR 0049: mirror the leader's quorum-ack age — the leading indicator of the
            // fsync-bound consensus degradation the 2026-07-14 incident hid. 0 when this
            // node is not the leader (only the leader has a quorum-ack clock).
            let ack_ms = plane
                .quorum_ack_age_ms()
                .and_then(|v| i64::try_from(v).ok())
                .unwrap_or(0);
            m.set_lease_quorum_ack_ms(ack_ms);
            // ADR 0054: voter count (previously only in the /readyz body) and the
            // replica catch-up summary — tracked minus current is this node's
            // replication lag in groups, the takeover-safety signal.
            m.set_voters(plane.voter_count());
            let (current, tracked) = plane.caught_up_summary();
            m.set_replica_groups(current, tracked);
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
                sub_id: self.sub_ids.get(client).and_then(|m| m.get(f)).copied(),
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
        self.no_local.remove(client);
        self.sub_ids.remove(client);
        self.retain_as_published.remove(client);
        self.table.remove_client(client);
        self.shared.remove_client(client);
    }

    // --- cluster ---------------------------------------------------------------

    /// Release the publisher's acknowledgement iff every cluster-wide durability
    /// obligation has resolved (ADR 0042 T9).
    fn try_complete_pending(&mut self, id: u64) {
        let complete = self.pending_publishes.get(&id).is_some_and(|p| {
            p.local_done
                // ADR 0072: relaxed acks at submit; obligations still run.
                // Issue #399 carves two exceptions, both congestion valves:
                // a CONGESTED publish (its own lane was deep at submit) falls
                // through to the quorum rule below, and REMOTE obligations are
                // always awaited — each owner answers an uncongested relaxed
                // forward at submit-acceptance (one peer round trip, still the
                // relaxed meaning), and holds a congested one to append
                // completion, so the publisher's window throttles to whichever
                // node is drowning. Refusals travel those same verdicts: a
                // relaxed ack can no longer outrun a remote refusal.
                && ((p.relaxed
                    && !p.congested
                    && p.awaiting.is_empty()
                    && !p.awaiting_settle
                    && p.reroute_grace.unwrap_or(0) == 0)
                    || (p.appends_outstanding == 0
                        && !p.awaiting_retained
                        && !p.awaiting_settle
                        && p.awaiting.is_empty()
                        && p.reroute_grace.unwrap_or(0) == 0))
        });
        if complete {
            if let Some(p) = self.pending_publishes.remove(&id) {
                debug!(publish = id, topic = %p.topic, "pending publish complete; ack released");
                let _ = p.done.send(PublishOutcome::Accepted);
            }
        }
    }

    fn peer_connected(
        &mut self,
        node: NodeId,
        conn_id: u64,
        tx: PeerOutbound,
        ctl: PeerOutbound,
        cert_serial: Option<Vec<u8>>,
        proto: u32,
    ) {
        info!(local = %self.node_id.0, peer = %node.0, proto, "peer link established");
        // Operator-visible rolling-upgrade skew (0041-T12, issue #238): a proto-6 link
        // cannot carry a refusal, so a cross-node publish refusal reaches this peer's
        // publishers as a WITHHELD ack (v3.1.1's answer) rather than `0x97`. Once per
        // LINK, not per publish, so it cannot flood.
        if proto < PROTO_FORWARD_VERDICT {
            warn!(
                peer = %node.0, proto, need = PROTO_FORWARD_VERDICT,
                "peer speaks an older peer-bus proto: cross-node publish REFUSALS will be \
                 answered to its publishers as a withheld ack and a close, not 0x97 \
                 (rolling-upgrade skew — 0041-T12)"
            );
        }
        // Send our current interest + shared membership so the peer can route to us
        // immediately (ordinary fan-out and cluster-wide shared selection, ADR 0015)
        // — unless this hub is fresh and its boot scan has not yet landed: an EMPTY
        // snapshot from a mid-boot hub erases interest the peer still correctly
        // holds from before a fast restart (0043-P4 exhibit ②). The peer keeps its
        // prior knowledge; the real snapshot follows when the scan settles.
        if self.interest_authoritative {
            let _ = tx.send(PeerMessage::Interest {
                filters: self.local_interest(),
            });
            let _ = tx.send(PeerMessage::SharedInterest {
                groups: self.shared_snapshot(),
            });
        }
        // Register the link with the durable plane: consensus RPCs ride the CONTROL
        // lane (drained first by the pump — issue #358: a raft heartbeat behind a
        // 16 MiB retained snapshot blows its 500 ms deadline and churns elections),
        // bulk replication data rides the ordinary lane.
        if let Some(plane) = &self.durable_plane {
            plane.register(&node, ctl.clone(), tx.clone());
        }
        // ADR 0073: remember the negotiated proto across link flaps (removed only
        // on confirmed death) — the ownership-domain capability check reads this.
        self.known_peer_protos.insert(node.clone(), proto);
        self.peers.insert(
            node,
            Peer {
                conn_id,
                tx,
                ctl,
                cert_serial,
                interest_synced: false,
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
        self.known_peer_protos.remove(node);
        let had_interest = self.remote_interest.remove(node).is_some();
        self.remote_shared.remove(node);
        // Acked forwards to the dead node re-route to its successor once it
        // advertises the inherited interest (ADR 0042 T9, exhibit ⑤ + ⑥): drop
        // the dead obligations and engage the sweep's re-route grace.
        //
        // A SHARED obligation is different (0041-T12): its target was one NAMED member
        // on that node, not "whoever there is interested", so the interest-based
        // re-route grace cannot substitute for it. It re-selects within its group
        // immediately — the same rebalance a refusal triggers.
        let mut dead_seqs: Vec<u64> = Vec::new();
        let mut dead_shared: Vec<(u64, ForwardObligation)> = Vec::new();
        for (id, p) in &mut self.pending_publishes {
            let seqs: Vec<u64> = p
                .awaiting
                .iter()
                .filter(|(_, o)| &o.node == node)
                .map(|(s, _)| *s)
                .collect();
            if seqs.is_empty() {
                continue;
            }
            let mut had_ordinary = false;
            for seq in seqs {
                let Some(o) = p.awaiting.remove(&seq) else {
                    continue;
                };
                match o.kind {
                    ForwardKind::Shared { .. } => dead_shared.push((*id, o)),
                    ForwardKind::Ordinary => had_ordinary = true,
                }
                dead_seqs.push(seq);
            }
            if had_ordinary {
                debug!(peer = %node.0, topic = %p.topic, "forward target died; re-route grace engaged");
                p.reroute_grace = Some(REROUTE_GRACE_TICKS);
            }
        }
        for seq in dead_seqs {
            self.forward_index.remove(&seq);
        }
        for (id, obligation) in dead_shared {
            // `Failed`, not a refusal: a dead node says nothing about what it stored.
            self.reselect_shared(id, obligation, DurableOutcome::Failed);
        }
        if had_link || had_interest {
            info!(peer = %node.0, "peer declared dead; routing state dropped");
        }
        if let Some(plane) = &self.durable_plane {
            plane.fail(node);
        }
        self.drop_retained_handoff_state(node);
    }

    /// Route a durable-plane frame from `node`: spawn its handling (so a slow raft
    /// dispatch never blocks the actor loop) and send any reply back over the peer's
    /// link. A no-op when no durable plane is attached.
    ///
    /// Reply routing (issue #358): consensus replies and replication acks are the
    /// frames a peer is actively BLOCKED on — openraft's 500 ms `AppendEntries`
    /// deadline, the 5 s replication RPC bound — so they take the control lane past
    /// any bulk backlog. Data-bearing replies (replica reads, key lists, catch-up)
    /// stay on the bulk lane: putting multi-megabyte frames on the control lane
    /// would recreate the very head-of-line blocking it exists to prevent.
    fn handle_durable_frame(&self, node: &NodeId, frame: PeerMessage) {
        let Some(plane) = self.durable_plane.clone() else {
            return;
        };
        // Queue-transit visibility (issue #358): the delta between the sender's
        // "queued to link" stamp and this one is the whole link+hub-queue path.
        if let PeerMessage::Replicate { req_id, .. } = &frame {
            debug!(req_id, from = %node.0, "replicate: dequeued from hub queue");
        }
        let reply_to = self.peers.get(node).map(|p| (p.tx.clone(), p.ctl.clone()));
        tokio::spawn(async move {
            if let Some(reply) = plane.handle(frame).await {
                if let Some((tx, ctl)) = reply_to {
                    let lane = match &reply {
                        PeerMessage::RaftRpcReply { .. } | PeerMessage::ReplicateAck { .. } => &ctl,
                        _ => &tx,
                    };
                    let _ = lane.send(reply);
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
        // No snapshots from a hub that cannot stand behind them (0043-P4
        // exhibit ②): until the boot scan has materialized every owned durable
        // session, an interest snapshot is a lie of omission that ERASES what
        // peers still correctly advertise-to and forward for.
        if !self.interest_authoritative || self.peers.is_empty() {
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

/// The peer frame for one outstanding obligation of a pending gated publish
/// (ADR 0042 T9; 0041-T12 for the shared kind), under `seq`.
///
/// The ONLY constructor of these frames: the original forward and the sweep's
/// retransmission both build here, so a change to what a frame carries (an expiry
/// decrement, a RAP flag, a new field's semantics) applies to first sends and
/// retransmits identically — the two paths can never drift. That now includes the
/// KIND: a shared obligation must retransmit `SharedDeliverAcked` targeted at its
/// chosen member, never a fan-out `PublishAcked` (which the receiver would deliver to
/// every matching ordinary subscriber instead).
fn forward_frame(p: &PendingPublish, seq: u64, obligation: &ForwardObligation) -> PeerMessage {
    match &obligation.kind {
        ForwardKind::Ordinary => PeerMessage::PublishAcked {
            seq,
            topic: p.topic.clone(),
            payload: p.payload.to_vec(),
            qos: p.qos as u8,
            retain: p.retain,
            message_expiry: p.message_expiry,
            app: app_to_wire(&p.app),
        },
        ForwardKind::Shared { client, qos, .. } => PeerMessage::SharedDeliverAcked {
            seq,
            client: client.0.clone(),
            topic: p.topic.clone(),
            payload: p.payload.to_vec(),
            qos: *qos as u8,
            message_expiry: p.message_expiry,
            app: app_to_wire(&p.app),
        },
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

// `pub(crate)` so `backpressure`'s byte-accounting identity test can assert against the packet
// the broker ACTUALLY sends, rather than a hand-assembled lookalike — the previous version of
// that test built its own packet and was therefore blind to a per-property disagreement.
#[allow(clippy::too_many_arguments)] // a thin PUBLISH constructor; all fields are the wire packet's
pub(crate) fn publish_packet(
    topic: &str,
    payload: Bytes,
    qos: QoS,
    pkid: Option<u16>,
    dup: bool,
    retain: bool,
    message_expiry: Option<u32>,
    app: &AppProperties,
    sub_ids: &[u32],
) -> Packet {
    use mqtt_codec::Property;
    let mut properties = mqtt_codec::Properties::new();
    // The ids of every matching subscription, in this one packet
    // ([MQTT-3.3.4-4], issue #266); empty = the property is absent entirely.
    for id in sub_ids {
        properties.0.push(Property::SubscriptionIdentifier(*id));
    }
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

/// The ADR 0074 truncate flusher: coalesce per-session ack watermarks and flush
/// them to the store off the hub loop. One map entry per session (max offset
/// wins — a burst of N acks becomes ONE truncate at the final watermark), a
/// small bounded number of concurrent flushes, and the documented not-fatal
/// tolerance on failure: the entries stay in the log and replay at next resume.
/// Truncates are monotonic and idempotent, so a flush racing a newer watermark
/// (or the `QoS` 2 path's inline truncate) is harmless — the higher offset wins
/// at the store, the lower one deletes nothing extra.
async fn run_truncate_flusher(
    store: Arc<dyn SessionStore>,
    mut rx: mpsc::UnboundedReceiver<(ClientId, Offset)>,
) {
    /// Concurrent flushes in flight: enough to keep the truncate pipeline busy
    /// across sessions without turning the flusher into an unbounded spawner.
    const FLUSH_CONCURRENCY: usize = 8;
    let mut latest: HashMap<ClientId, Offset> = HashMap::new();
    let mut flushes: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    loop {
        // Wait for work: a new watermark, or a finished flush freeing a slot.
        tokio::select! {
            msg = rx.recv() => {
                let Some((client, up_to)) = msg else { break }; // hub gone
                let slot = latest.entry(client).or_insert(up_to);
                *slot = (*slot).max(up_to);
            }
            Some(_) = flushes.join_next(), if !flushes.is_empty() => {}
        }
        // Opportunistically drain the channel so a burst coalesces before flushing.
        while let Ok((client, up_to)) = rx.try_recv() {
            let slot = latest.entry(client).or_insert(up_to);
            *slot = (*slot).max(up_to);
        }
        while flushes.len() < FLUSH_CONCURRENCY {
            let Some(client) = latest.keys().next().cloned() else {
                break;
            };
            let Some(up_to) = latest.remove(&client) else {
                break;
            };
            let store = store.clone();
            flushes.spawn(async move {
                if let Err(e) = store.ack(&client, up_to).await {
                    // Not fatal: the entries stay in the log and are replayed on the
                    // next resume. A duplicate at QoS 1 is spec-legal; losing one
                    // would not be.
                    debug!(client = %client.0, up_to, error = %e,
                           "detached truncate of the acknowledged session log failed");
                }
            });
        }
    }
    // Hub dropped its sender: flush what remains, best-effort, then stop.
    while flushes.join_next().await.is_some() {}
    for (client, up_to) in latest {
        let _ = store.ack(&client, up_to).await;
    }
}

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
    let mut backoff =
        mqtt_core::Backoff::new(ATTACH_RECOVERY_BACKOFF_START, ATTACH_RECOVERY_BACKOFF_MAX);
    loop {
        match recover_once(store, client, owner).await {
            Ok(ready) => return ready,
            // Transient and time remaining: back off and retry.
            Err(e) if e.is_transient() && Instant::now() < deadline => {}
            // Terminal failure, or the deadline passed: reject (never downgrade).
            Err(_) => return SessionRecovery::Unavailable,
        }
        tokio::time::sleep(backoff.next_delay()).await;
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
            expires_at: None,
        }
    }
    /// The canonical empty-props bytes folded into digest entries (ADR 0038 T3).
    fn no_props() -> Vec<u8> {
        AppProps::default().encode()
    }

    use super::{
        Admission, AttachOutcome, AuthMethod, BrownoutAxis, Hub, HubCommand, Inflight, Outbound,
        PeerOutbound, ProtocolVersion, PublishOutcome, PublishRefusal, RemoteSharedGroup, Will,
        EXPIRY_RECONCILE_EVERY, MAX_OUTBOUND_QUEUE, REPLAY_LIMIT,
    };
    use crate::backpressure::{
        BacklogBound, BacklogEntry, SubscriberLimits, DEFAULT_MAX_BACKLOG_MESSAGES,
    };
    use bytes::Bytes;
    use mqtt_cluster::peer::{ForwardVerdict, PeerMessage, RetainedWireEntry};
    use mqtt_cluster::placement::{Placement, DEFAULT_REPLICAS};
    use mqtt_cluster::swim::MemberState;
    use mqtt_cluster::NodeId;
    use mqtt_codec::{Packet, QoS};
    use mqtt_core::{AppProperties, ClientId, Message};
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
        let (out_tx, _out_rx) = {
            let (t, r) = mpsc::unbounded_channel();
            (Outbound::new(t).0, r)
        };
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
        let (out_tx, _out_rx) = {
            let (t, r) = mpsc::unbounded_channel();
            (Outbound::new(t).0, r)
        };
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
        /// While true, `record_outbound`/`advance_outbound` fail with `NoQuorum` — a
        /// SEPARATE lever from the enqueue one, because the append happens first and
        /// failing it would mask the very path ADR 0057's tests exist to exercise.
        fail_outbound: std::sync::atomic::AtomicBool,
    }

    impl FlakyStore {
        fn new(fail_ensure: usize) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                inner: MemorySessionStore::new(),
                fail_remaining: std::sync::atomic::AtomicUsize::new(fail_ensure),
                fail_enqueue_no_quorum: false,
                fail_outbound: std::sync::atomic::AtomicBool::new(false),
            })
        }

        /// A store whose durable append always fails with `NoQuorum` (everything else
        /// delegates to the in-memory store), for the append-failure metric test.
        fn new_no_quorum_enqueue() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                inner: MemorySessionStore::new(),
                fail_remaining: std::sync::atomic::AtomicUsize::new(0),
                fail_enqueue_no_quorum: true,
                fail_outbound: std::sync::atomic::AtomicBool::new(false),
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
        ) -> Result<mqtt_storage::InboundSighting, mqtt_storage::StorageError> {
            self.inner.record_received(client, packet_id).await
        }

        async fn ack_received(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.ack_received(client, packet_id).await
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

        async fn record_outbound(
            &self,
            client: &ClientId,
            packet_id: u16,
            offset: mqtt_storage::Offset,
        ) -> Result<(), mqtt_storage::StorageError> {
            // Its own lever, not the enqueue one: the durable append runs FIRST, and
            // failing both would mean the delivery never reaches this path at all.
            if self
                .fail_outbound
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err(mqtt_storage::StorageError::NoQuorum);
            }
            self.inner.record_outbound(client, packet_id, offset).await
        }

        async fn advance_outbound(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            if self
                .fail_outbound
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err(mqtt_storage::StorageError::NoQuorum);
            }
            self.inner.advance_outbound(client, packet_id).await
        }

        async fn clear_outbound(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.clear_outbound(client, packet_id).await
        }

        async fn outbound(
            &self,
            client: &ClientId,
        ) -> Result<Vec<mqtt_storage::OutboundInflight>, mqtt_storage::StorageError> {
            self.inner.outbound(client).await
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
        let (out_tx, out_rx) = {
            let (t, r) = mpsc::unbounded_channel();
            (Outbound::new(t).0, r)
        };
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

    /// [`attach`] with a Will, for the ungraceful-detach paths (issue #238, R3).
    async fn attach_with_will(
        tx: &HubTx,
        client: &str,
        conn_id: u64,
        clean_start: bool,
        will: Message,
    ) -> (mpsc::UnboundedReceiver<Packet>, bool) {
        // Delay 0: these predate Will Delay and assert the publish-at-once path.
        let will = Will {
            message: will,
            delay_secs: 0,
        };
        let (out_tx, out_rx) = {
            let (t, r) = mpsc::unbounded_channel();
            (Outbound::new(t).0, r)
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(HubCommand::Attach {
            client: ClientId(client.into()),
            admission: admission(client),
            conn_id,
            clean_start,
            session_expiry: if clean_start { 0 } else { u32::MAX },
            receive_maximum: u16::MAX,
            will: Some(will),
            outbound: out_tx,
            reply: reply_tx,
        })
        .unwrap();
        let present = matches!(reply_rx.await.unwrap(), AttachOutcome::Present(true));
        (out_rx, present)
    }

    fn detach(tx: &HubTx, client: &str, conn_id: u64) {
        tx.send(HubCommand::Detach {
            client: ClientId(client.into()),
            conn_id,
            graceful: true,
            session_expiry_override: None,
        })
        .unwrap();
    }

    fn subscribe(tx: &HubTx, client: &str, filter: &str) {
        tx.send(HubCommand::Subscribe {
            client: ClientId(client.into()),
            filters: vec![(filter.into(), QoS::AtMostOnce)],
            sub_id: None,
            no_local_filters: Vec::new(),
            rap_filters: Vec::new(),
            retain_handling: Vec::new(),
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
            publisher: None,
        })
        .unwrap();
    }

    fn subscribe_qos(tx: &HubTx, client: &str, filter: &str, qos: QoS) {
        tx.send(HubCommand::Subscribe {
            client: ClientId(client.into()),
            filters: vec![(filter.into(), qos)],
            sub_id: None,
            no_local_filters: Vec::new(),
            rap_filters: Vec::new(),
            retain_handling: Vec::new(),
            reply: None,
        })
        .unwrap();
    }

    /// A subscriber that stops reading must not be able to grow its outbound queue
    /// without limit (#123).
    ///
    /// `QoS 0` was the one delivery path with no bound at all: the `QoS 1/2` backlog is
    /// capped by `MAX_BACKLOG`, and Receive Maximum does not apply to `QoS 0`, so a
    /// stalled consumer on a busy topic accumulated packets until the process died.
    /// The README even documented `MAX_BACKLOG` as *the* per-connection bound,
    /// which was true for `QoS 1/2` and false here.
    ///
    /// The receiver is deliberately never drained — that is the whole scenario.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_qos0_subscriber_is_shed_not_queued_without_limit() {
        let tx = start_hub();
        // Held and never read from: this client has stopped consuming.
        let (mut stalled_rx, _) = attach(&tx, "stalled", 1, true).await;
        subscribe(&tx, "stalled", "flood/#");

        // Publish comfortably past the cap.
        let over = MAX_OUTBOUND_QUEUE + 5_000;
        for _ in 0..over {
            publish(&tx, "flood/x", b"drop-me");
        }
        // Let the hub work through them.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Count what actually accumulated.
        let mut queued = 0usize;
        while stalled_rx.try_recv().is_ok() {
            queued += 1;
        }

        assert!(
            queued <= MAX_OUTBOUND_QUEUE,
            "outbound queue grew to {queued} for a subscriber that never read — \
             the cap of {MAX_OUTBOUND_QUEUE} was not enforced"
        );
        assert!(
            queued > 0,
            "nothing was queued at all: the test never exercised the delivery path"
        );
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
            publisher: None,
        })
        .unwrap();
    }

    fn publish_qos2(tx: &HubTx, topic: &str, payload: &'static [u8]) {
        tx.send(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::from_static(payload),
            qos: QoS::ExactlyOnce,
            retain: false,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: false,
            publisher: None,
        })
        .unwrap();
    }

    fn pub_rec(tx: &HubTx, client: &str, pkid: u16) {
        tx.send(HubCommand::PubRec {
            client: ClientId(client.into()),
            pkid,
        })
        .unwrap();
    }

    fn pub_comp(tx: &HubTx, client: &str, pkid: u16) {
        tx.send(HubCommand::PubComp {
            client: ClientId(client.into()),
            pkid,
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
            ctl: peer_tx.clone(), // tests observe both lanes through one receiver
            tx: peer_tx,
            cert_serial: None,
            // The current ceiling: a test peer speaks what this build speaks, so the
            // proto-7 frames are exercised by default and the proto-6 collapse is opted
            // into explicitly (see `connect_peer_at_proto`).
            proto: mqtt_cluster::peer::PROTO_MAX,
        })
        .unwrap();
        peer_rx
    }

    /// [`connect_peer`] on a link that negotiated `proto` — the seam for the
    /// rolling-upgrade skew case (0041-T12): a proto-6 peer cannot be sent a verdict.
    fn connect_peer_at_proto(
        tx: &HubTx,
        node: &str,
        conn_id: u64,
        proto: u32,
    ) -> mpsc::UnboundedReceiver<PeerMessage> {
        let (peer_tx, peer_rx): (PeerOutbound, _) = mpsc::unbounded_channel();
        tx.send(HubCommand::PeerConnected {
            node: NodeId(node.into()),
            conn_id,
            ctl: peer_tx.clone(), // tests observe both lanes through one receiver
            tx: peer_tx,
            cert_serial: None,
            proto,
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

    /// As [`remote_shared_interest`], but each member carries a granted `QoS` — needed
    /// wherever the DELIVERED `QoS` decides behaviour (0041-T12: only a `QoS` ≥ 1 shared
    /// delivery is owed anything, so only it becomes an answerable obligation).
    fn remote_shared_interest_qos(
        tx: &HubTx,
        node: &str,
        group: &str,
        filter: &str,
        members: &[(&str, QoS)],
    ) {
        tx.send(HubCommand::RemoteSharedInterest {
            node: NodeId(node.into()),
            groups: vec![RemoteSharedGroup {
                group: group.into(),
                filter: filter.into(),
                members: members
                    .iter()
                    .map(|(c, q)| (ClientId((*c).into()), *q, true))
                    .collect(),
            }],
        })
        .unwrap();
    }

    /// The next frame on a peer link that is part of the forward/answer protocol,
    /// skipping the interest snapshots that ride alongside every gossip.
    async fn next_forward_answer(rx: &mut mpsc::UnboundedReceiver<PeerMessage>) -> PeerMessage {
        loop {
            let msg = timeout(Duration::from_millis(600), rx.recv())
                .await
                .expect("a peer message within the deadline")
                .expect("the link is open");
            if matches!(
                msg,
                PeerMessage::PublishAck { .. }
                    | PeerMessage::PublishVerdict { .. }
                    | PeerMessage::PublishAcked { .. }
                    | PeerMessage::SharedDeliver { .. }
                    | PeerMessage::SharedDeliverAcked { .. }
            ) {
                return msg;
            }
        }
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

    /// [`recv_peer`], additionally skipping the link-up handshake frames whose
    /// ORDER is not fixed: interest snapshots (which since 0043-P4 exhibit ②
    /// follow the boot scan instead of leading the link) and retained digest
    /// offers. For tests about data frames; tests about the handshake itself
    /// keep using [`recv_peer`].
    async fn recv_peer_data(rx: &mut mpsc::UnboundedReceiver<PeerMessage>) -> Option<PeerMessage> {
        loop {
            match recv_peer(rx).await {
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => return other,
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
            session_expiry_override: None,
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
        let (out_tx, mut victim) = {
            let (t, r) = mpsc::unbounded_channel();
            (Outbound::new(t).0, r)
        };
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
            will: Some(Will {
                delay_secs: 0,
                message: mqtt_core::Message {
                    topic: "wills/victim".into(),
                    payload: Bytes::from_static(b"gone"),
                    qos: QoS::AtMostOnce,
                    retain: false,
                    app: mqtt_core::AppProperties::default(),
                    expires_at: None,
                },
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
            fn authorize_publish(&self, _: &Identity, _: &ClientId, _: &String) -> bool {
                true
            }
            fn authorize_subscribe(&self, _: &Identity, _: &ClientId, _: &String) -> bool {
                true
            }
            fn authorize_connect(&self, identity: &Identity, _: &ClientId) -> bool {
                identity.subject != self.0
            }
        }
        /// A credential store that no longer knows one subject.
        struct UserGone(&'static str);
        #[async_trait::async_trait]
        impl Authenticator for UserGone {
            async fn authenticate(
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
                let (out_tx, out_rx) = {
                    let (t, r) = mpsc::unbounded_channel();
                    (Outbound::new(t).0, r)
                };
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
            fn authorize_publish(&self, _: &Identity, _: &ClientId, _: &String) -> bool {
                true
            }
            fn authorize_subscribe(&self, _: &Identity, _: &ClientId, filter: &String) -> bool {
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
            fn authorize_publish(&self, _: &Identity, _: &ClientId, _: &String) -> bool {
                true
            }
            fn authorize_subscribe(&self, _: &Identity, _: &ClientId, filter: &String) -> bool {
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
            session_expiry_override: None,
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

        // The peer gets our retained digest at link-up (the interest snapshot
        // arrives on its own schedule — after the boot scan — and is skipped).
        loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedDigest { count, hash, .. }) => {
                    assert_eq!(count, 1);
                    assert_ne!(hash, 0, "one topic hashes to a non-zero digest");
                    break;
                }
                Some(PeerMessage::Interest { .. }) => {}
                other => panic!("expected RetainedDigest, got {other:?}"),
            }
        }

        // The peer's set differed, so it pulls — and gets the snapshot.
        tx.send(HubCommand::RemoteRetainedRequest {
            node: NodeId("n".into()),
        })
        .unwrap();
        match recv_peer_data(&mut peer).await {
            Some(PeerMessage::RetainedSnapshot { messages }) => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].topic, "t");
                assert_eq!(&messages[0].payload[..], b"r");
            }
            other => panic!("expected RetainedSnapshot, got {other:?}"),
        }
    }

    /// A retained update whose fan-out frame never lands is HEALED by the periodic
    /// digest, with the link never flapping (issue #87).
    ///
    /// A commit fans out exactly one frame, unacked and never retransmitted, and it
    /// can legitimately be dropped (oversized/unencodable) with the link kept. Before
    /// this, the only other reconciliation was a digest at link-up, so that peer
    /// stayed silently divergent until something flapped the link — possibly forever.
    /// Here the peer links up, the frames it would have received are discarded, and
    /// the assertion is that the hub OFFERS ITS DIGEST AGAIN unprompted, which is what
    /// gives the peer another chance to notice the gap and pull.
    #[tokio::test(start_paused = true)]
    async fn a_missed_retained_fan_out_is_healed_by_periodic_anti_entropy() {
        let tx = start_hub();
        let mut peer = connect_peer(&tx, "n", 1);
        publish_retained(&tx, "t", b"r");

        // Everything the link-up and the commit produced is thrown away: this peer
        // is now missing the retained value, and nothing will flap its link.
        while tokio::time::timeout(Duration::from_millis(50), peer.recv())
            .await
            .is_ok()
        {}

        // Unprompted, on the anti-entropy cadence, our digest is offered again.
        let mut offered = None;
        for _ in 0..(super::RETAINED_ANTIENTROPY_EVERY + 2) {
            tokio::time::sleep(super::SESSION_SWEEP_INTERVAL).await;
            if let Ok(Some(PeerMessage::RetainedDigest { count, hash, .. })) =
                tokio::time::timeout(Duration::from_millis(50), peer.recv()).await
            {
                offered = Some((count, hash));
                break;
            }
        }
        let (count, hash) = offered.expect(
            "the retained digest must be re-offered without a link flap; \
             otherwise a single dropped fan-out frame diverges the peer forever",
        );
        assert_eq!(count, 1, "the digest describes the retained value we hold");
        assert_ne!(hash, 0);
    }

    /// The anti-entropy cadence stays SILENT when there is nothing to reconcile
    /// (issue #87): a node holding no retained state must not wake its peers every
    /// period just to say so.
    #[tokio::test(start_paused = true)]
    async fn periodic_anti_entropy_says_nothing_when_there_is_nothing_retained() {
        let tx = start_hub();
        let mut peer = connect_peer(&tx, "n", 1);
        while tokio::time::timeout(Duration::from_millis(50), peer.recv())
            .await
            .is_ok()
        {}

        for _ in 0..(super::RETAINED_ANTIENTROPY_EVERY + 2) {
            tokio::time::sleep(super::SESSION_SWEEP_INTERVAL).await;
        }
        // Other periodic traffic (interest gossip) is fine and expected; what must
        // never appear is a digest describing an empty set.
        while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(50), peer.recv()).await
        {
            assert!(
                !matches!(msg, PeerMessage::RetainedDigest { .. }),
                "no retained state means no digest to offer, got {msg:?}"
            );
        }
    }

    /// A peer whose digest matches ours is already in sync: no request, no snapshot —
    /// a steady-state link-up (or flap) transfers nothing (0014-T6).
    #[tokio::test]
    async fn a_matching_retained_digest_skips_the_back_fill() {
        let tx = start_hub();
        publish_retained(&tx, "t", b"r");
        let mut peer = connect_peer(&tx, "n", 1);

        // The peer claims the same single (topic, value, qos) we hold: same digest, no pull.
        let (count, hash, value_hash) = super::retained::retained_digest(std::iter::once((
            "t",
            b"r".as_ref(),
            0u8,
            no_props(),
        )));
        tx.send(HubCommand::RemoteRetainedDigest {
            node: NodeId("n".into()),
            count,
            hash,
            value_hash,
        })
        .unwrap();
        // No retained transfer follows — only the (order-free) handshake frames.
        let quiet = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            recv_peer_data(&mut peer),
        )
        .await;
        assert!(quiet.is_err(), "matching digests must transfer nothing");
    }

    /// A digest that does NOT match ours makes us pull the peer's set (0014-T6).
    #[tokio::test]
    async fn a_differing_retained_digest_pulls_the_peers_set() {
        let tx = start_hub();
        publish_retained(&tx, "t", b"r");
        let mut peer = connect_peer(&tx, "n", 1);

        // The peer holds a different set: we answer its digest with a pull.
        let (count, hash, value_hash) = super::retained::retained_digest(
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
            recv_peer_data(&mut peer).await,
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

        // Same single topic, different value: set hash matches, value hash differs.
        let (count, hash, value_hash) = super::retained::retained_digest(std::iter::once((
            "t",
            b"THEIRS".as_ref(),
            0u8,
            no_props(),
        )));
        tx.send(HubCommand::RemoteRetainedDigest {
            node: NodeId("n".into()),
            count,
            hash,
            value_hash,
        })
        .unwrap();
        assert!(matches!(
            recv_peer_data(&mut peer).await,
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
        let chunks = super::retained::chunk_retained(entries);
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
        let chunks = super::retained::chunk_retained(entries.into_iter());
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
        let full =
            super::retained::retained_digest([one.clone(), two.clone(), three.clone()].into_iter());
        let shuffled =
            super::retained::retained_digest([three.clone(), one.clone(), two.clone()].into_iter());
        assert_eq!(full, shuffled, "order must not matter");
        let subset = super::retained::retained_digest([one.clone(), two.clone()].into_iter());
        assert_ne!(full, subset, "a different set must differ");
        // Same topics, different value: topic hash equal, value hash different.
        let two_changed = ("y", b"CHANGED".as_ref(), 1u8, no_props());
        let diverged =
            super::retained::retained_digest([one.clone(), two_changed, three.clone()].into_iter());
        assert_eq!(full.1, diverged.1, "topic-set hash ignores values");
        assert_ne!(
            full.2, diverged.2,
            "value hash must see the changed payload"
        );
        assert_eq!(
            super::retained::retained_digest(std::iter::empty()),
            (0, 0, 0)
        );
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
            publisher: None,
        })
        .unwrap();
        assert!(
            done_rx.await.is_err(),
            "the ack must be withheld when the durable enqueue fails"
        );
    }

    /// #164 — the SHARED-subscription mirror of the test above. A shared subscriber is a
    /// persistent subscriber, so a failed durable enqueue for the chosen member must
    /// withhold the publisher's ack exactly as an ordinary subscriber's does. Before the
    /// fix, `deliver_shared` discarded `deliver_to_client`'s result and the publisher was
    /// acked for a message that was never recorded.
    #[tokio::test]
    async fn a_failed_shared_enqueue_withholds_the_publishers_ack() {
        let (hub, tx) = Hub::with_config(NodeId("h".into()), FlakyStore::new_no_quorum_enqueue());
        tokio::spawn(hub.run());

        // The ONLY match is an offline, persistent SHARED-group member, so selection
        // must choose it and its enqueue takes the failing durable path.
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe(&tx, "p", "$share/g/fc/t");
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
            publisher: None,
        })
        .unwrap();
        assert!(
            done_rx.await.is_err(),
            "the ack must be withheld when a shared member's durable enqueue fails (#164)"
        );
    }

    /// #203 (durable-store audit): a QoS≥1 **retained** publish whose retained-store write
    /// FAILS must withhold the publisher's ack. The single-node retained path was fail-open —
    /// it logged the store error and acked anyway, so a publisher was told its retained value
    /// was stored when it was not (inconsistent with the offline-enqueue and QoS-2 paths, which
    /// already fail closed).
    #[tokio::test]
    async fn a_failed_retained_store_write_withholds_the_publishers_ack() {
        let store = FailingRetainedStore::new();
        let (mut hub, tx) = Hub::with_config_and_placement(
            NodeId("h".into()),
            Arc::new(MemorySessionStore::new()),
            None,
        );
        hub.attach_retained_store(Arc::new(store.clone()));
        tokio::spawn(hub.run());

        // Healthy: a retained publish is acked normally (the fix must not break the happy path).
        let (ok_tx, ok_rx) = oneshot::channel();
        tx.send(HubCommand::Publish {
            topic: "r/ok".into(),
            payload: Bytes::from_static(b"v"),
            qos: QoS::AtLeastOnce,
            retain: true,
            message_expiry: None,
            app: mqtt_core::AppProperties::default(),
            done: Some(ok_tx),
            v5: false,
            publisher: None,
        })
        .unwrap();
        assert!(
            ok_rx.await.is_ok(),
            "a healthy retained write must still ack"
        );

        // Failing: the ack is withheld.
        store.fail_writes();
        let (fail_tx, fail_rx) = oneshot::channel();
        tx.send(HubCommand::Publish {
            topic: "r/fail".into(),
            payload: Bytes::from_static(b"v"),
            qos: QoS::AtLeastOnce,
            retain: true,
            message_expiry: None,
            app: mqtt_core::AppProperties::default(),
            done: Some(fail_tx),
            v5: false,
            publisher: None,
        })
        .unwrap();
        assert!(
            fail_rx.await.is_err(),
            "a failed retained-store write must withhold the publisher's ack (#203)"
        );
    }

    /// #198: **No Local** suppresses echoing a client's own publish back to it, while other
    /// subscribers still receive it — the unforgeable loop-prevention primitive (Mosquitto
    /// `try_private` / EMQX `bridge_mode`) the boundary bridge relies on (ADR 0059/0025).
    #[tokio::test]
    async fn no_local_suppresses_the_publishers_own_delivery() {
        let tx = start_hub();
        let (mut nl_rx, _) = attach(&tx, "pub-nl", 1, true).await;
        let (mut other_rx, _) = attach(&tx, "other", 2, true).await;

        // pub-nl subscribes to t/# WITH No Local; other subscribes without.
        tx.send(HubCommand::Subscribe {
            client: ClientId("pub-nl".into()),
            filters: vec![("t/#".into(), QoS::AtMostOnce)],
            sub_id: None,
            no_local_filters: vec!["t/#".into()],
            rap_filters: Vec::new(),
            retain_handling: Vec::new(),
            reply: None,
        })
        .unwrap();
        tx.send(HubCommand::Subscribe {
            client: ClientId("other".into()),
            filters: vec![("t/#".into(), QoS::AtMostOnce)],
            sub_id: None,
            no_local_filters: Vec::new(),
            rap_filters: Vec::new(),
            retain_handling: Vec::new(),
            reply: None,
        })
        .unwrap();

        // pub-nl publishes to t/x.
        tx.send(HubCommand::Publish {
            topic: "t/x".into(),
            payload: Bytes::from_static(b"m"),
            qos: QoS::AtMostOnce,
            retain: false,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: true,
            publisher: Some(ClientId("pub-nl".into())),
        })
        .unwrap();

        // The other subscriber receives it; the No Local publisher does NOT.
        assert!(
            matches!(recv_packet(&mut other_rx).await, Some(Packet::Publish(_))),
            "the non-No-Local subscriber must receive the message"
        );
        assert!(
            recv_packet(&mut nl_rx).await.is_none(),
            "No Local must suppress delivery to the publisher's own subscription"
        );
    }

    /// #198: **Retain As Published** — a subscriber that set RAP keeps the RETAIN flag the
    /// message was published with, while a subscriber without it gets the flag cleared
    /// [MQTT-3.3.1-9]. This is what lets a re-forwarder (the boundary bridge) carry *live*
    /// retained state across a boundary (#189): before it, a live retained publish reached the
    /// bridge with retain=0 and could not be re-published as retained.
    #[tokio::test]
    async fn retain_as_published_preserves_the_retain_flag_only_for_rap_subscribers() {
        let tx = start_hub();
        let (mut rap_rx, _) = attach(&tx, "rap-sub", 1, true).await;
        let (mut plain_rx, _) = attach(&tx, "plain-sub", 2, true).await;

        tx.send(HubCommand::Subscribe {
            client: ClientId("rap-sub".into()),
            filters: vec![("t/#".into(), QoS::AtMostOnce)],
            sub_id: None,
            no_local_filters: Vec::new(),
            rap_filters: vec!["t/#".into()],
            retain_handling: Vec::new(),
            reply: None,
        })
        .unwrap();
        tx.send(HubCommand::Subscribe {
            client: ClientId("plain-sub".into()),
            filters: vec![("t/#".into(), QoS::AtMostOnce)],
            sub_id: None,
            no_local_filters: Vec::new(),
            rap_filters: Vec::new(),
            retain_handling: Vec::new(),
            reply: None,
        })
        .unwrap();

        // A RETAINED publish, delivered live to both established subscriptions.
        tx.send(HubCommand::Publish {
            topic: "t/x".into(),
            payload: Bytes::from_static(b"v"),
            qos: QoS::AtMostOnce,
            retain: true,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: true,
            publisher: Some(ClientId("someone-else".into())),
        })
        .unwrap();

        match recv_packet(&mut rap_rx).await {
            Some(Packet::Publish(p)) => assert!(
                p.retain,
                "a RAP subscriber must keep the published RETAIN flag (#198)"
            ),
            other => panic!("the RAP subscriber received {other:?}"),
        }
        match recv_packet(&mut plain_rx).await {
            Some(Packet::Publish(p)) => assert!(
                !p.retain,
                "a non-RAP subscriber must have RETAIN cleared [MQTT-3.3.1-9]"
            ),
            other => panic!("the plain subscriber received {other:?}"),
        }
    }

    /// #198: **Retain Handling** (MQTT 5 §3.8.3.1) — `2` never replays retained at subscribe,
    /// `1` replays only for a NEW subscription, `0` (default) always replays. A re-forwarder
    /// (the boundary bridge) uses `2` to avoid a retained replay storm on every reconnect.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn retain_handling_controls_the_replay_at_subscribe() {
        let tx = start_hub();
        // Seed a retained value.
        tx.send(HubCommand::Publish {
            topic: "rh/x".into(),
            payload: Bytes::from_static(b"v"),
            qos: QoS::AtMostOnce,
            retain: true,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: true,
            publisher: None,
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let sub = |client: &str, handling: u8| HubCommand::Subscribe {
            client: ClientId(client.into()),
            filters: vec![("rh/#".into(), QoS::AtMostOnce)],
            sub_id: None,
            no_local_filters: Vec::new(),
            rap_filters: Vec::new(),
            retain_handling: vec![handling],
            reply: None,
        };

        // handling 2: never replay.
        let (mut never_rx, _) = attach(&tx, "rh-never", 1, true).await;
        tx.send(sub("rh-never", 2)).unwrap();
        assert!(
            recv_packet(&mut never_rx).await.is_none(),
            "retain handling 2 must not replay the retained message"
        );

        // handling 0: always replay.
        let (mut always_rx, _) = attach(&tx, "rh-always", 2, true).await;
        tx.send(sub("rh-always", 0)).unwrap();
        assert!(
            matches!(recv_packet(&mut always_rx).await, Some(Packet::Publish(p)) if p.retain),
            "retain handling 0 must replay the retained message"
        );

        // handling 1: replays for a NEW subscription, but not on a re-subscribe.
        let (mut new_rx, _) = attach(&tx, "rh-new", 3, true).await;
        tx.send(sub("rh-new", 1)).unwrap();
        assert!(
            matches!(recv_packet(&mut new_rx).await, Some(Packet::Publish(_))),
            "retain handling 1 must replay for a NEW subscription"
        );
        tx.send(sub("rh-new", 1)).unwrap(); // same filter again
        assert!(
            recv_packet(&mut new_rx).await.is_none(),
            "retain handling 1 must NOT replay when the subscription already existed"
        );
    }

    /// RAP only preserves a flag that was actually set: a NON-retained publish still delivers
    /// with retain=0 to a RAP subscriber (RAP preserves, it does not invent).
    #[tokio::test]
    async fn rap_does_not_invent_a_retain_flag_for_a_plain_publish() {
        let tx = start_hub();
        let (mut rx, _) = attach(&tx, "rap-sub2", 1, true).await;
        tx.send(HubCommand::Subscribe {
            client: ClientId("rap-sub2".into()),
            filters: vec![("t/#".into(), QoS::AtMostOnce)],
            sub_id: None,
            no_local_filters: Vec::new(),
            rap_filters: vec!["t/#".into()],
            retain_handling: Vec::new(),
            reply: None,
        })
        .unwrap();
        tx.send(HubCommand::Publish {
            topic: "t/x".into(),
            payload: Bytes::from_static(b"v"),
            qos: QoS::AtMostOnce,
            retain: false, // NOT retained
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: true,
            publisher: Some(ClientId("someone-else".into())),
        })
        .unwrap();
        match recv_packet(&mut rx).await {
            Some(Packet::Publish(p)) => assert!(
                !p.retain,
                "RAP must not set RETAIN on a message published without it"
            ),
            other => panic!("received {other:?}"),
        }
    }

    /// The control that makes the No Local test meaningful: WITHOUT No Local, a publisher that
    /// also subscribes to the topic DOES receive its own message (the MQTT default).
    #[tokio::test]
    async fn without_no_local_a_publisher_receives_its_own_delivery() {
        let tx = start_hub();
        let (mut rx, _) = attach(&tx, "pub-plain", 1, true).await;
        tx.send(HubCommand::Subscribe {
            client: ClientId("pub-plain".into()),
            filters: vec![("t/#".into(), QoS::AtMostOnce)],
            sub_id: None,
            no_local_filters: Vec::new(),
            rap_filters: Vec::new(),
            retain_handling: Vec::new(),
            reply: None,
        })
        .unwrap();
        tx.send(HubCommand::Publish {
            topic: "t/x".into(),
            payload: Bytes::from_static(b"m"),
            qos: QoS::AtMostOnce,
            retain: false,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: true,
            publisher: Some(ClientId("pub-plain".into())),
        })
        .unwrap();
        assert!(
            matches!(recv_packet(&mut rx).await, Some(Packet::Publish(_))),
            "without No Local the publisher must receive its own delivery"
        );
    }

    /// The other side of #164: when the shared enqueue SUCCEEDS, the publisher is acked
    /// normally — the fix must gate on failure without breaking the happy path.
    #[tokio::test]
    async fn a_successful_shared_enqueue_still_acks() {
        let tx = start_hub();
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe(&tx, "p", "$share/g/ok/t");
        detach(&tx, "p", 1);

        let (done_tx, done_rx) = oneshot::channel();
        tx.send(HubCommand::Publish {
            topic: "ok/t".into(),
            payload: Bytes::from_static(b"x"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties::default(),
            done: Some(done_tx),
            v5: false,
            publisher: None,
        })
        .unwrap();
        assert!(
            done_rx.await.is_ok(),
            "a shared delivery whose enqueue SUCCEEDED must still ack the publisher"
        );
    }

    /// Issue #238 — the LOCAL shared-member composition site: a gated publish whose
    /// only recipient is a local shared-group member owed a durable append is REFUSED
    /// under brownout (not acked over a discarded outcome, and not withheld), and
    /// recovery both acks and durably enqueues for the member.
    ///
    /// The refusal is enforced by two layers calling `plan_refusal` — `publish`'s
    /// shared plan peek (which also keeps a refused publish from consuming the group's
    /// round-robin turn) and `deliver_to_client`'s decide-before-commit gate behind it
    /// — so the reversion that reddens this test is the same two-layer pre-#238
    /// mutation the deterministic cluster oracle bites on (`plan_refusal` → `None`
    /// plus `durable_append`'s brownout arm → `Appended::Dropped`); either layer
    /// alone is absorbed by the other, by design. The outcome-DISCARD reversion
    /// (`deliver_shared` ignoring `deliver_to_client`'s result, the original #164
    /// defect) is pinned by `a_failed_shared_enqueue_withholds_the_publishers_ack`.
    #[tokio::test]
    async fn a_local_shared_member_under_brownout_refuses_the_publisher() {
        let tx = start_hub();
        // The ONLY match: an offline, persistent, QoS 1 shared-group member.
        let (_rx, _) = attach(&tx, "m", 1, false).await;
        subscribe_qos(&tx, "m", "$share/g/lb/t", QoS::AtLeastOnce);
        detach(&tx, "m", 1);

        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();
        assert_eq!(
            publish_gated(&tx, "lb/t", b"no", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout),
            "a shared subscriber is a persistent subscriber (#164): its refused \
             durable enqueue refuses the publisher"
        );

        // Recovery: the publish is acked and the member's queue replays exactly it.
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: false,
        })
        .unwrap();
        assert_eq!(
            publish_gated(&tx, "lb/t", b"yes", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Accepted
        );
        let (mut resumed, _) = attach(&tx, "m", 2, false).await;
        assert_eq!(
            payload_of(&recv_packet(&mut resumed).await.expect("the replayed copy")),
            b"yes",
            "only the ACKED payload was ever stored"
        );
        assert!(
            recv_packet(&mut resumed).await.is_none(),
            "the refused payload must not replay"
        );
    }

    /// Issue #238 — the DISTINCT effect of `publish`'s shared plan peek, which the
    /// test above cannot see: a refused publish must not consume the shared group's
    /// round-robin turn. With the peek disabled, `deliver_to_client`'s gate still
    /// refuses the publisher (so the test above stays green), but `select_shared` has
    /// already advanced the cursor — and the next accepted publish lands on the SAME
    /// member as the last one instead of rotating. Two members, publishes one/two with
    /// a refused attempt between them: each member must receive exactly one.
    #[tokio::test]
    async fn a_refused_shared_publish_does_not_consume_the_groups_round_robin_turn() {
        let tx = start_hub();
        // Two offline, persistent, QoS 1 members of the same group: every accepted
        // publish owes a durable append, and selection rotates between them.
        for m in ["rr-a", "rr-b"] {
            let (_rx, _) = attach(&tx, m, 1, false).await;
            subscribe_qos(&tx, m, "$share/g/rr/t", QoS::AtLeastOnce);
            detach(&tx, m, 1);
        }

        assert_eq!(
            publish_gated(&tx, "rr/t", b"one", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Accepted
        );
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();
        assert_eq!(
            publish_gated(&tx, "rr/t", b"refused", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout)
        );
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: false,
        })
        .unwrap();
        assert_eq!(
            publish_gated(&tx, "rr/t", b"two", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Accepted
        );

        // Ordering-agnostic: whichever member got "one", rotation gives "two" to the
        // other. A consumed turn skips a member, and one of them replays BOTH.
        let mut per_member = Vec::new();
        for m in ["rr-a", "rr-b"] {
            let (mut resumed, _) = attach(&tx, m, 2, false).await;
            let mut got = Vec::new();
            while let Some(p) = recv_packet(&mut resumed).await {
                got.push(payload_of(&p).to_vec());
            }
            per_member.push(got);
        }
        for got in &per_member {
            assert_eq!(
                got.len(),
                1,
                "a refused publish consumed a round-robin turn: {per_member:?}"
            );
        }
        let mut all: Vec<Vec<u8>> = per_member.into_iter().flatten().collect();
        all.sort();
        assert_eq!(all, vec![b"one".to_vec(), b"two".to_vec()]);
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
        // QoS 1, so the offline enqueue is an obligation the publisher's ack is
        // gated on — the whole point of the refusal below (#238).
        subscribe_qos(&tx, "sleeper", "b/q", QoS::AtLeastOnce);
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
                publisher: None,
            })
            .unwrap();
            done_rx
        };
        assert_eq!(
            retained_publish("b/r1", b"v1").await.unwrap(),
            super::PublishOutcome::Accepted
        );

        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();

        // Growth refused: a NEW retained topic...
        assert_eq!(
            retained_publish("b/r2", b"nope").await.unwrap(),
            super::PublishOutcome::Refused(super::PublishRefusal::RetainedQuota),
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
        // ...and an offline enqueue, which is REFUSED, not acked (#238): the
        // publisher is told `Brownout` and this message must NOT replay after
        // recovery, because it was never stored.
        assert_eq!(
            publish_gated(&tx, "b/q", b"browned-out", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            super::PublishOutcome::Refused(super::PublishRefusal::Brownout),
            "a refused offline enqueue must refuse the publisher's ack (#238)"
        );

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
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: false,
        })
        .unwrap();
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

    /// A gated `QoS` publish: the ack-gate receiver, so a test can observe the
    /// exact [`PublishOutcome`] the hub released (or its withholding).
    fn publish_gated(
        tx: &HubTx,
        topic: &str,
        payload: &'static [u8],
        qos: QoS,
        v5: bool,
    ) -> oneshot::Receiver<PublishOutcome> {
        let (done_tx, done_rx) = oneshot::channel();
        tx.send(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::from_static(payload),
            qos,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties::default(),
            done: Some(done_tx),
            v5,
            publisher: None,
        })
        .unwrap();
        done_rx
    }

    /// Issue #238 / 0041-T11 — the acceptance criterion. Under brownout a `QoS` 1
    /// publish whose only recipient is an OFFLINE persistent subscriber is never
    /// enqueued anywhere, so the publisher must not be acked: it is REFUSED, with
    /// a reason the connection can turn into `0x97` (v5) or a close (v3.1.1).
    #[tokio::test]
    async fn brownout_refuses_the_publishers_ack_for_an_offline_persistent_subscriber() {
        let tx = start_hub();

        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "b/q", QoS::AtLeastOnce);
        detach(&tx, "p", 1);

        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();

        assert_eq!(
            publish_gated(&tx, "b/q", b"x", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout),
            "a QoS 1 publish the store refuses to enqueue must not be acked (#238)"
        );
    }

    /// Issue #238 — the ONLINE persistent subscriber has the same answer, and the
    /// live send is withheld too: delivering live with no durable record promises a
    /// redelivery the store cannot honour (the rule `Appended::Failed` already
    /// follows).
    #[tokio::test]
    async fn brownout_refuses_the_ack_and_the_live_send_for_an_online_persistent_subscriber() {
        let tx = start_hub();

        let (mut rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "b/q", QoS::AtLeastOnce);

        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();

        assert_eq!(
            publish_gated(&tx, "b/q", b"x", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout),
            "an online persistent subscriber's durable record is owed too (#238)"
        );
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "nothing may reach the wire for a message with no durable record"
        );
    }

    /// The anti-over-refusal guard (#238): brownout costs exactly the growth write
    /// it refuses. A publish that owes NO durable record — a clean session, or
    /// `QoS` 0 — is still delivered and still acked while browned out.
    #[tokio::test]
    async fn a_publish_owing_no_durability_is_still_acked_under_brownout() {
        let tx = start_hub();

        // A clean-session subscriber (nothing to resume into) and a persistent,
        // offline one (owed a queue only for QoS > 0).
        let (mut clean_rx, _) = attach(&tx, "clean", 1, true).await;
        subscribe_qos(&tx, "clean", "nb/clean", QoS::AtLeastOnce);
        let (_p_rx, _) = attach(&tx, "p", 2, false).await;
        subscribe_qos(&tx, "p", "nb/off", QoS::AtLeastOnce);
        detach(&tx, "p", 2);

        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();

        assert_eq!(
            publish_gated(&tx, "nb/clean", b"live", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Accepted,
            "a clean session owes no redelivery, so brownout refuses nothing"
        );
        assert_eq!(
            payload_of(&recv_packet(&mut clean_rx).await.unwrap()),
            b"live",
            "and the live delivery still happens"
        );
        assert_eq!(
            publish_gated(&tx, "nb/off", b"fnf", QoS::AtMostOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Accepted,
            "QoS 0 owes no redelivery either — at-most-once is the spec-native \
             fire-and-forget"
        );
    }

    /// The semantic split an operator alerts on (#238): a refusal the publisher is
    /// TOLD about is a quota rejection, not a loss. Only the `QoS` 0 offline
    /// enqueue — where nothing was owed and nothing is acked — is a drop.
    #[tokio::test]
    async fn a_brownout_refusal_is_counted_as_a_quota_rejection_not_a_drop() {
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (mut hub, tx) = Hub::with_config(
            NodeId("h".into()),
            std::sync::Arc::new(MemorySessionStore::new()),
        );
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "bm/q", QoS::AtLeastOnce);
        detach(&tx, "p", 1);
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();

        let _ = publish_gated(&tx, "bm/q", b"refused", QoS::AtLeastOnce, true).await;
        let out = metrics.render();
        assert!(
            out.contains("mqttd_quota_rejections_total{reason=\"brownout-publish\"} 1"),
            "a refused QoS 1 publish is a quota rejection:\n{out}"
        );
        assert!(
            !out.contains("mqttd_publish_dropped_total{reason=\"brownout\"}"),
            "a refusal the publisher is told about is not a loss:\n{out}"
        );

        // QoS 0 to the same offline subscriber IS a drop: nothing was owed, and
        // nothing is acked, so there is no retry to over-count.
        assert_eq!(
            publish_gated(&tx, "bm/q", b"dropped", QoS::AtMostOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Accepted
        );
        for _ in 0..200 {
            if metrics
                .render()
                .contains("mqttd_publish_dropped_total{reason=\"brownout\"} 1")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "a QoS 0 offline enqueue above the watermark is a drop:\n{}",
            metrics.render()
        );
    }

    /// THE HOIST (issue #238): a refusal must be EFFECT-FREE. Under brownout a publish
    /// one of whose recipients owes a refused durable append is refused WHOLESALE —
    /// including the live copy for a subscriber that owed no durability at all.
    ///
    /// That is the correct reading of a per-PUBLISH acknowledgement: there is no way to
    /// say "delivered to two of your three subscribers", and the publisher still owns the
    /// message and is expected to retry it. It is also what makes the retry idempotent —
    /// a refused attempt leaves nothing behind to duplicate, which is the whole basis of
    /// the `QoS` 2 half of this fix.
    #[tokio::test]
    async fn a_brownout_refusal_delivers_to_nobody_even_when_a_subscriber_owed_no_durability() {
        let tx = start_hub();

        // X: online CLEAN session, QoS 1 — owes no durable record, so it is the witness
        // that survived the refusal path before the hoist.
        let (mut x_rx, _) = attach(&tx, "x", 1, true).await;
        subscribe_qos(&tx, "x", "h/t", QoS::AtLeastOnce);
        // Y: persistent, offline, QoS 1 — the recipient whose append brownout refuses.
        let (_y_rx, _) = attach(&tx, "y", 2, false).await;
        subscribe_qos(&tx, "y", "h/t", QoS::AtLeastOnce);
        detach(&tx, "y", 2);

        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();

        assert_eq!(
            publish_gated(&tx, "h/t", b"refused", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout)
        );
        assert!(
            recv_packet(&mut x_rx).await.is_none(),
            "a refused publish must reach NOBODY: the clean-session subscriber's live \
             copy is a side effect of a publish the broker did not accept"
        );

        // Recovery: the same publish now reaches X exactly once and is owed to Y.
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: false,
        })
        .unwrap();
        assert_eq!(
            publish_gated(&tx, "h/t", b"kept", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Accepted
        );
        assert_eq!(
            payload_of(
                &recv_packet(&mut x_rx)
                    .await
                    .expect("delivered after recovery")
            ),
            b"kept"
        );
        assert!(
            recv_packet(&mut x_rx).await.is_none(),
            "exactly once — the refused attempt left nothing to duplicate"
        );
    }

    /// Issue #238 — the retained-overwrite ordering. A publish answered "not accepted"
    /// must not already have mutated durable retained state that every future subscriber
    /// will see. `retained_quota_exceeded` returns false for a topic that already exists,
    /// so an OVERWRITE proceeds under brownout unless the refusal is decided first.
    #[tokio::test]
    async fn a_retained_publish_refused_by_brownout_leaves_the_previous_retained_value_intact() {
        let tx = start_hub();

        let retained_publish = |topic: &'static str, payload: &'static [u8]| {
            let (done_tx, done_rx) = oneshot::channel();
            tx.send(HubCommand::Publish {
                topic: topic.into(),
                payload: Bytes::from_static(payload),
                qos: QoS::AtLeastOnce,
                retain: true,
                message_expiry: None,
                app: mqtt_core::AppProperties::default(),
                done: Some(done_tx),
                v5: true,
                publisher: None,
            })
            .unwrap();
            done_rx
        };

        // `ro/t` already retained with A, and an offline persistent QoS 1 subscriber on it
        // — so the publish below owes a durable append the brownout will refuse.
        assert_eq!(
            retained_publish("ro/t", b"A").await.unwrap(),
            PublishOutcome::Accepted
        );
        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "ro/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);
        // The reader attaches BEFORE the watermark is crossed — a browned-out broker
        // refuses NEW sessions (T5 growth), which would prove nothing about the retained
        // value. Its SUBSCRIBE (and the retained replay it triggers) comes after.
        let (mut fresh_rx, _) = attach(&tx, "fresh", 2, true).await;

        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();
        assert_eq!(
            retained_publish("ro/t", b"B").await.unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout)
        );

        // The retained replay must still see A: `0x97` says the message was NOT accepted,
        // and a publish that permanently changed broker state was.
        subscribe_qos(&tx, "fresh", "ro/t", QoS::AtLeastOnce);
        assert_eq!(
            payload_of(&recv_packet(&mut fresh_rx).await.expect("a retained replay")),
            b"A",
            "the refused retain=1 publish must not have overwritten the retained value"
        );
    }

    /// Issue #238 — an UNGATED publish has nobody to refuse, so a refused durable copy
    /// must not cost the LIVE delivery. The Will is the case that matters: suppressing it
    /// under brownout leaves every device "online" on the dashboard through exactly the
    /// incident [MQTT-3.14.4-3] exists for.
    #[tokio::test]
    async fn a_will_is_still_delivered_live_under_brownout_and_counted_as_a_drop() {
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (mut hub, tx) = Hub::with_config(
            NodeId("h".into()),
            std::sync::Arc::new(MemorySessionStore::new()),
        );
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        // A monitoring client: ONLINE and PERSISTENT at QoS 1, so its copy of the will
        // owes a durable record — the append brownout refuses.
        let (mut watcher_rx, _) = attach(&tx, "watcher", 1, false).await;
        subscribe_qos(&tx, "watcher", "dev/+/status", QoS::AtLeastOnce);

        // A device with a will, then an UNGRACEFUL end while browned out.
        let (_dev_rx, _) = attach_with_will(
            &tx,
            "dev42",
            2,
            false,
            Message {
                topic: "dev/42/status".into(),
                payload: Bytes::from_static(b"offline"),
                qos: QoS::AtLeastOnce,
                retain: false,
                app: mqtt_core::AppProperties::default(),
                expires_at: None,
            },
        )
        .await;
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();
        tx.send(HubCommand::Detach {
            client: ClientId("dev42".into()),
            conn_id: 2,
            graceful: false,
            session_expiry_override: None,
        })
        .unwrap();

        assert_eq!(
            payload_of(
                &recv_packet(&mut watcher_rx)
                    .await
                    .expect("the will must still be delivered live")
            ),
            b"offline",
            "an ungated publish has no publisher to refuse, so suppressing its live \
             delivery would destroy the message rather than defer it"
        );
        // And the lost durable copy is counted as the DROP it is — not as a refusal the
        // publisher can retry, because there is no publisher.
        for _ in 0..200 {
            let out = metrics.render();
            if out.contains("mqttd_publish_dropped_total{reason=\"brownout\"} 1") {
                assert!(
                    !out.contains("mqttd_quota_rejections_total{reason=\"brownout-publish\"}"),
                    "nobody was told, so this is a loss and not a retryable refusal:\n{out}"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "a will's refused durable copy must be counted as a drop:\n{}",
            metrics.render()
        );
    }

    /// Issue #238 — the MEMORY axis (ADR 0041 T8) answers the publisher exactly as the
    /// disk axis does. Every other brownout-ack test drives `BrownoutAxis::Disk`, so the
    /// axis-agnostic claim was asserted nowhere.
    #[tokio::test]
    async fn the_memory_axis_refuses_the_publishers_ack_just_as_the_disk_axis_does() {
        let tx = start_hub();
        let (_rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "ma/q", QoS::AtLeastOnce);
        detach(&tx, "p", 1);

        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Memory,
            on: true,
        })
        .unwrap();
        assert_eq!(
            publish_gated(&tx, "ma/q", b"x", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout),
            "the refusal follows the brownout FLAG, not the axis that raised it"
        );
    }

    /// Issue #238 — 0041-T12's wire choice, both directions. A refusal is a verdict on a
    /// proto-7 link and collapses to today's boolean on a proto-6 one, and each link sees
    /// exactly one of the two frames.
    #[tokio::test]
    async fn a_proto_6_peer_is_answered_with_the_boolean_and_a_proto_7_peer_with_the_verdict() {
        let tx = start_hub();
        let mut old = connect_peer_at_proto(&tx, "old", 1, 6);
        let mut new = connect_peer_at_proto(&tx, "new", 2, 7);

        // One offline persistent QoS 1 subscriber, so a forwarded publish owes a durable
        // append here — which brownout refuses.
        let (_rx, _) = attach(&tx, "s", 9, false).await;
        subscribe_qos(&tx, "s", "pv/t", QoS::AtLeastOnce);
        detach(&tx, "s", 9);
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();

        for (node, seq) in [("old", 1u64), ("new", 2u64)] {
            tx.send(HubCommand::RemotePublishAcked {
                node: NodeId(node.into()),
                seq,
                topic: "pv/t".into(),
                payload: Bytes::from_static(b"x"),
                qos: QoS::AtLeastOnce,
                retain: false,
                message_expiry: None,
                app: mqtt_core::AppProperties::default(),
            })
            .unwrap();
        }

        match next_forward_answer(&mut old).await {
            PeerMessage::PublishAck { seq, ok } => {
                assert_eq!((seq, ok), (1, false), "proto 6 can only say 'not stored'");
            }
            other => panic!("a proto-6 link must never be sent a verdict, got {other:?}"),
        }
        match next_forward_answer(&mut new).await {
            PeerMessage::PublishVerdict { seq, verdict } => assert_eq!(
                (seq, verdict),
                (
                    2,
                    ForwardVerdict::Refused {
                        code: PublishRefusal::Brownout.wire_code()
                    }
                ),
                "proto 7 carries WHY, so the origin can tell its publisher 0x97"
            ),
            other => panic!("expected a PublishVerdict, got {other:?}"),
        }
    }

    /// Issue #238 (C1) — a peer's REFUSAL refuses the publisher rather than closing on it.
    /// Partnered with the withhold direction in the same test, because the two answers
    /// must stay distinguishable: `Failed` (and an unknown refusal code) still drops the
    /// gate.
    #[tokio::test]
    async fn a_peers_refusal_refuses_the_publisher_instead_of_dropping_the_gate() {
        let tx = start_hub();
        let _peer = connect_peer_at_proto(&tx, "n2", 1, 7);
        remote_interest(&tx, "n2", &["cv/t"]);

        // Refused → the publisher is TOLD.
        let done = publish_gated(&tx, "cv/t", b"a", QoS::AtLeastOnce, true);
        tx.send(HubCommand::RemotePublishVerdict {
            node: NodeId("n2".into()),
            seq: 1,
            verdict: ForwardVerdict::Refused {
                code: PublishRefusal::Brownout.wire_code(),
            },
        })
        .unwrap();
        assert_eq!(
            done.await.unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout),
            "a peer's stated refusal must reach the publisher as one (0041-T12)"
        );

        // Failed → withheld, exactly as before.
        let done = publish_gated(&tx, "cv/t", b"b", QoS::AtLeastOnce, true);
        tx.send(HubCommand::RemotePublishVerdict {
            node: NodeId("n2".into()),
            seq: 2,
            verdict: ForwardVerdict::Failed,
        })
        .unwrap();
        assert!(
            done.await.is_err(),
            "a terminal failure still withholds: no reason code honestly covers it"
        );

        // A proto-6 peer's `ok: false` is the same withhold — the skew fallback.
        let done = publish_gated(&tx, "cv/t", b"c", QoS::AtLeastOnce, true);
        tx.send(HubCommand::RemotePublishAck {
            node: NodeId("n2".into()),
            seq: 3,
            ok: false,
        })
        .unwrap();
        assert!(done.await.is_err(), "the boolean can only mean 'withhold'");
    }

    /// Issue #238 — an unknown refusal code (a NEWER peer refusing for a reason this
    /// build cannot name) WITHHOLDS. It must never become an ack, and never a fabricated
    /// refusal either: `Refused` asserts "nothing was stored", which an answer we cannot
    /// read does not support.
    #[tokio::test]
    async fn an_unknown_refusal_code_withholds_and_never_acks() {
        let tx = start_hub();
        let _peer = connect_peer_at_proto(&tx, "n2", 1, 7);
        remote_interest(&tx, "n2", &["uv/t"]);

        let done = publish_gated(&tx, "uv/t", b"a", QoS::AtLeastOnce, true);
        tx.send(HubCommand::RemotePublishVerdict {
            node: NodeId("n2".into()),
            seq: 1,
            verdict: ForwardVerdict::Refused { code: 0xFFFF },
        })
        .unwrap();
        match done.await {
            Err(_) => {}
            Ok(other) => panic!("an unreadable refusal must withhold, got {other:?}"),
        }

        // Non-vacuity: a code this build DOES know resolves as a refusal.
        let done = publish_gated(&tx, "uv/t", b"b", QoS::AtLeastOnce, true);
        tx.send(HubCommand::RemotePublishVerdict {
            node: NodeId("n2".into()),
            seq: 2,
            verdict: ForwardVerdict::Refused {
                code: PublishRefusal::Brownout.wire_code(),
            },
        })
        .unwrap();
        assert_eq!(
            done.await.unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout)
        );
    }

    /// Issue #238 (R2) — a cross-node shared delivery is ANSWERABLE, and a refusing
    /// member's node causes a RE-SELECTION within the group rather than a lost message.
    ///
    /// Before this, `deliver_shared`'s remote branch fired one unacked `SharedDeliver` and
    /// released the ack: the receiving node refused the append, skipped the wire send too,
    /// and the message reached nobody while the publisher was told it was stored.
    #[tokio::test]
    async fn a_cross_node_shared_delivery_is_answered_and_reselects_before_refusing() {
        // Case A — the group's only member lives on a proto-7 peer.
        let tx = start_hub();
        let mut peer = connect_peer_at_proto(&tx, "n2", 1, 7);
        remote_shared_interest_qos(&tx, "n2", "g", "sh/t", &[("m1", QoS::AtLeastOnce)]);

        let done = publish_gated(&tx, "sh/t", b"a", QoS::AtLeastOnce, true);
        let seq = match next_forward_answer(&mut peer).await {
            PeerMessage::SharedDeliverAcked { seq, client, .. } => {
                assert_eq!(client, "m1");
                seq
            }
            other => panic!("a gated QoS 1 shared delivery must be answerable, got {other:?}"),
        };
        assert!(
            timeout(Duration::from_millis(200), &mut Box::pin(async {}))
                .await
                .is_ok(),
            "sanity"
        );
        tx.send(HubCommand::RemotePublishVerdict {
            node: NodeId("n2".into()),
            seq,
            verdict: ForwardVerdict::Refused {
                code: PublishRefusal::Brownout.wire_code(),
            },
        })
        .unwrap();
        assert_eq!(
            done.await.unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout),
            "with no other candidate the group's refusal reaches the publisher"
        );

        // Case B — a second member exists LOCALLY (offline persistent): the peer's
        // refusal re-balances onto it and the publisher IS acked.
        let tx = start_hub();
        let mut peer = connect_peer_at_proto(&tx, "n2", 1, 7);
        remote_shared_interest_qos(&tx, "n2", "g", "sh/t", &[("m1", QoS::AtLeastOnce)]);
        let (_rx, _) = attach(&tx, "local", 2, false).await;
        subscribe_qos(&tx, "local", "$share/g/sh/t", QoS::AtLeastOnce);
        detach(&tx, "local", 2);

        let done = publish_gated(&tx, "sh/t", b"b", QoS::AtLeastOnce, true);
        let seq = match next_forward_answer(&mut peer).await {
            PeerMessage::SharedDeliverAcked { seq, .. } => seq,
            other => panic!("expected SharedDeliverAcked, got {other:?}"),
        };
        tx.send(HubCommand::RemotePublishVerdict {
            node: NodeId("n2".into()),
            seq,
            verdict: ForwardVerdict::Refused {
                code: PublishRefusal::Brownout.wire_code(),
            },
        })
        .unwrap();
        assert_eq!(
            done.await.unwrap(),
            PublishOutcome::Accepted,
            "one member's node refusing is a re-balance, not a cluster-wide refusal"
        );
        // And the re-selection actually enqueued for the local member.
        let (mut resumed, _) = attach(&tx, "local", 3, false).await;
        assert_eq!(
            payload_of(&recv_packet(&mut resumed).await.expect("the replayed copy")),
            b"b"
        );

        // Case C — a proto-6 peer member keeps today's unacked SharedDeliver, and the ack
        // releases: the link cannot carry an answer, so nothing is owed on it.
        let tx = start_hub();
        let mut peer = connect_peer_at_proto(&tx, "n2", 1, 6);
        remote_shared_interest_qos(&tx, "n2", "g", "sh/t", &[("m1", QoS::AtLeastOnce)]);
        let done = publish_gated(&tx, "sh/t", b"c", QoS::AtLeastOnce, true);
        match next_forward_answer(&mut peer).await {
            PeerMessage::SharedDeliver { client, .. } => assert_eq!(client, "m1"),
            other => panic!("a proto-6 link must keep the unacked frame, got {other:?}"),
        }
        assert_eq!(done.await.unwrap(), PublishOutcome::Accepted);
    }

    /// Issue #238 — an unanswered shared obligation HOLDS the ack and retransmits the
    /// SHARED frame under the same seq. The single-constructor invariant across kinds: a
    /// retransmit that ignored the obligation's kind would fan a `PublishAcked` out to
    /// every matching ordinary subscriber on the peer instead of the chosen member.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn an_unanswered_shared_forward_holds_the_ack_and_retransmits_the_shared_frame() {
        let tx = start_hub();
        let mut peer = connect_peer_at_proto(&tx, "n2", 1, 7);
        remote_shared_interest_qos(&tx, "n2", "g", "sh/t", &[("m1", QoS::AtLeastOnce)]);

        let mut done = publish_gated(&tx, "sh/t", b"a", QoS::AtLeastOnce, true);
        let first = match next_forward_answer(&mut peer).await {
            PeerMessage::SharedDeliverAcked { seq, .. } => seq,
            other => panic!("expected SharedDeliverAcked, got {other:?}"),
        };
        // Several sweep ticks with no answer: the frame repeats under the SAME seq and
        // the ack never releases.
        for _ in 0..3 {
            tokio::time::sleep(super::SESSION_SWEEP_INTERVAL + Duration::from_millis(50)).await;
            match next_forward_answer(&mut peer).await {
                PeerMessage::SharedDeliverAcked { seq, client, .. } => {
                    assert_eq!(
                        (seq, client.as_str()),
                        (first, "m1"),
                        "same seq, same member"
                    );
                }
                other => panic!("a shared obligation must retransmit its OWN frame, got {other:?}"),
            }
            assert!(
                done.try_recv().is_err(),
                "the ack must not release while the obligation is unanswered"
            );
        }
    }

    /// Issue #238 — only a genuinely UNSTORED publish may be refused. A brownout entered
    /// during a takeover window must not answer `0x97` ("not accepted") for a message the
    /// original fan-out already stored durably: the ack is WITHHELD instead, which claims
    /// nothing, because an application retry would duplicate it for the subscriber that
    /// already holds it.
    #[tokio::test]
    async fn a_publish_already_stored_is_withheld_not_refused_when_a_later_pass_is_refused() {
        let tx = start_hub();
        let _peer = connect_peer_at_proto(&tx, "n2", 1, 7);
        remote_interest(&tx, "n2", &["rp/t"]);

        // Local offline persistent subscriber: the first fan-out DOES store a copy.
        let (_rx, _) = attach(&tx, "s", 2, false).await;
        subscribe_qos(&tx, "s", "rp/t", QoS::AtLeastOnce);
        detach(&tx, "s", 2);

        let done = publish_gated(&tx, "rp/t", b"a", QoS::AtLeastOnce, true);
        // The peer then refuses its half — a refusal arriving for a publish that IS held.
        tx.send(HubCommand::RemotePublishVerdict {
            node: NodeId("n2".into()),
            seq: 1,
            verdict: ForwardVerdict::Refused {
                code: PublishRefusal::Brownout.wire_code(),
            },
        })
        .unwrap();
        assert!(
            done.await.is_err(),
            "a publish already stored durably may only be WITHHELD: `Refused` asserts \
             'nothing was stored', and a retry on that basis duplicates it"
        );
    }

    /// Brownout is the OR across axes (ADR 0041 T5 disk, T8 memory).
    ///
    /// The axes are independent pollers on independent watermarks. With one shared flag,
    /// whichever polled last would decide — the disk watcher's routine "still fine" every
    /// 10s would silently lift a brownout that memory pressure is still asking for, and
    /// the broker would resume accepting growth writes straight into an OOM. This test is
    /// the reason the state is a set and not a bool.
    #[tokio::test]
    async fn one_axis_recovering_does_not_lift_another_axis_brownout() {
        let tx = start_hub();
        attach_full(&tx, "sleeper", 1, false, u32::MAX, 8).await;
        detach(&tx, "sleeper", 1);

        let set = |axis, on| {
            tx.send(HubCommand::SetBrownout { axis, on }).unwrap();
        };
        // A new session is refused under brownout and accepted otherwise, so it reads the
        // aggregate flag without reaching into the hub's internals.
        let new_session_refused = |tx: &HubTx, id: &'static str, conn: u64| {
            let tx = tx.clone();
            async move {
                matches!(
                    attach_outcome(&tx, id, conn).await,
                    AttachOutcome::QuotaExceeded
                )
            }
        };

        set(BrownoutAxis::Disk, true);
        set(BrownoutAxis::Memory, true);
        assert!(
            new_session_refused(&tx, "a", 10).await,
            "both axes over: browned out"
        );

        // Disk recovers. Memory has NOT.
        set(BrownoutAxis::Disk, false);
        assert!(
            new_session_refused(&tx, "b", 11).await,
            "memory is still over its watermark — the disk axis recovering must not \
             lift the brownout"
        );

        // Memory recovers too: only now does growth resume.
        set(BrownoutAxis::Memory, false);
        assert!(
            !new_session_refused(&tx, "c", 12).await,
            "with every axis back under its watermark, growth writes resume"
        );
    }

    /// A single axis still behaves exactly as it did before axes existed — set, refused;
    /// cleared, accepted — so the generalisation did not change the T5 disk contract.
    #[tokio::test]
    async fn a_single_axis_still_toggles_brownout_on_its_own() {
        let tx = start_hub();
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Memory,
            on: true,
        })
        .unwrap();
        assert!(matches!(
            attach_outcome(&tx, "x", 1).await,
            AttachOutcome::QuotaExceeded
        ));
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Memory,
            on: false,
        })
        .unwrap();
        assert!(matches!(
            attach_outcome(&tx, "x", 2).await,
            AttachOutcome::Present(false)
        ));
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
    ///
    /// Kept verbatim modulo the issue #241 API rename — it is the **regression witness**
    /// for the no-silent-change criterion: with the DEFAULT limits, the boundary is still
    /// exactly 10 000 pushes with no eviction and one eviction on the 10 001st.
    #[test]
    fn flow_control_backlog_is_bounded_drop_oldest() {
        let limits = SubscriberLimits::default();
        let mut inf = Inflight::default();
        let entry = |topic: String| {
            BacklogEntry::new(
                mqtt_core::Message {
                    topic,
                    payload: Bytes::from_static(b"x"),
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    app: AppProperties::default(),
                    expires_at: None,
                },
                false,
                None,
                None,
            )
        };
        for i in 0..DEFAULT_MAX_BACKLOG_MESSAGES {
            assert!(
                inf.push_backlog(entry(format!("t{i}")), &limits).is_empty(),
                "no eviction under the cap"
            );
        }
        // At the cap, the next push evicts the oldest (t0) and stays bounded.
        let evicted = inf.push_backlog(entry("overflow".into()), &limits);
        assert_eq!(
            evicted.len(),
            1,
            "exactly one entry goes at the count bound"
        );
        assert_eq!(
            evicted[0].0.message.topic, "t0",
            "the oldest is the one evicted at the cap"
        );
        assert_eq!(evicted[0].1, BacklogBound::Messages);
        assert_eq!(
            inf.backlog.len(),
            DEFAULT_MAX_BACKLOG_MESSAGES,
            "backlog stays bounded"
        );
        assert_eq!(
            inf.backlog.front().unwrap().message.topic,
            "t1",
            "oldest was dropped"
        );
        assert_eq!(inf.backlog.back().unwrap().message.topic, "overflow");
    }

    /// A hub with metrics and explicit per-subscriber bounds (issue #241).
    fn start_hub_with_limits(
        limits: SubscriberLimits,
    ) -> (HubTx, std::sync::Arc<mqtt_observability::metrics::Metrics>) {
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (mut hub, tx) = Hub::with_config(
            NodeId("hub-test".into()),
            std::sync::Arc::new(MemorySessionStore::new()),
        );
        hub.attach_metrics(metrics.clone());
        hub.set_subscriber_limits(limits);
        tokio::spawn(hub.run());
        (tx, metrics)
    }

    /// A publish whose message accounts for exactly `bytes` (issue #241's size
    /// definition: envelope + topic + payload + properties).
    fn publish_sized(tx: &HubTx, topic: &str, qos: QoS, bytes: usize, marker: u8) {
        let fixed = crate::backpressure::ENTRY_OVERHEAD + topic.len();
        tx.send(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::from(vec![marker; bytes - fixed]),
            qos,
            retain: false,
            message_expiry: None,
            app: AppProperties::default(),
            done: None,
            v5: false,
            publisher: None,
        })
        .unwrap();
    }

    /// The delivered `QoS` of a PUBLISH, so a test can tell a shed-legal class from one
    /// that must never be shed.
    fn qos_of(packet: &Packet) -> QoS {
        match packet {
            Packet::Publish(p) => p.qos,
            other => panic!("expected a publish, got {other:?}"),
        }
    }

    /// How many times `reason` was counted, read out of the rendered exposition.
    fn dropped_for(metrics: &mqtt_observability::metrics::Metrics, reason: &str) -> u64 {
        let needle = format!("mqttd_publish_dropped_total{{reason=\"{reason}\"}} ");
        metrics
            .render()
            .lines()
            .find_map(|l| l.strip_prefix(&needle))
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or(0)
    }

    /// Issue #241, end-to-end on the real hub: with a BYTE bound set, a subscriber that
    /// stops acknowledging sheds already-acked entries at that bound — long before the
    /// 10 000-message count bound — counted as `publish_dropped{reason="backlog-overflow"}`
    /// and with the NEWEST messages surviving in FIFO order.
    ///
    /// It also pins what did NOT change: `queue-overflow` belongs to the DURABLE offline
    /// queue, which this change does not touch, and `outbound-full` to the `QoS` 0
    /// channel. Neither moves.
    #[tokio::test]
    async fn the_backlog_byte_bound_sheds_acked_entries_and_counts_them() {
        // 1 KiB per message; a 4 KiB cap therefore holds 4.
        let (tx, metrics) = start_hub_with_limits(SubscriberLimits {
            max_backlog_bytes: Some(4096),
            ..SubscriberLimits::default()
        });
        // Receive Maximum 1 and never a PUBACK: everything after the first message piles
        // into the flow-control backlog.
        let (mut rx, _) = attach_full(&tx, "slow", 1, true, 0, 1).await;
        subscribe_qos(&tx, "slow", "t/1", QoS::AtLeastOnce);
        // One warm-up delivery first: a session's FIRST QoS>0 send finds its packet-id
        // block spent and parks in the backlog while the reservation runs off-loop
        // (issue #242 finding A). Draining that deferral before the run under test keeps
        // this about the byte bound rather than about the id reservation.
        publish_sized(&tx, "t/1", QoS::AtLeastOnce, 1024, b'W');
        let warm = recv_packet(&mut rx).await.expect("the warm-up delivery");
        assert_eq!(payload_of(&warm)[0], b'W');
        pub_ack(&tx, "slow", pkid_of(&warm));

        for i in 0..8u8 {
            publish_sized(&tx, "t/1", QoS::AtLeastOnce, 1024, b'a' + i);
        }

        // The first is on the wire under the quota of one.
        let first = recv_packet(&mut rx).await.expect("the first delivery");
        assert_eq!(payload_of(&first)[0], b'a');
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "the quota holds the rest"
        );

        // Seven pushes into a queue that holds four: three evictions at the BYTE bound.
        assert_eq!(
            dropped_for(&metrics, "backlog-overflow"),
            3,
            "eviction at the byte bound, not the count bound: {}",
            metrics.render()
        );
        assert_eq!(
            dropped_for(&metrics, "queue-overflow"),
            0,
            "the DURABLE queue's counter is untouched by a RAM bound"
        );
        assert_eq!(dropped_for(&metrics, "outbound-full"), 0);

        // Drop-oldest: what survived is the NEWEST four, delivered in FIFO order as acks
        // free the quota.
        let mut delivered = vec![payload_of(&first)[0]];
        let mut pkid = pkid_of(&first);
        for _ in 0..4 {
            pub_ack(&tx, "slow", pkid);
            let Some(pkt) = recv_packet(&mut rx).await else {
                break;
            };
            delivered.push(payload_of(&pkt)[0]);
            pkid = pkid_of(&pkt);
        }
        assert_eq!(
            delivered,
            vec![b'a', b'e', b'f', b'g', b'h'],
            "the three oldest held messages (b, c, d) were the ones shed"
        );
    }

    /// Issue #241: the outbound channel's byte bound sheds `QoS` 0 well before the fixed
    /// 10 000-PACKET cap — and it gates ONLY that shed-legal class, so a `QoS` 1 delivery
    /// in the same state still reaches the wire.
    #[tokio::test]
    async fn the_outbound_byte_cap_sheds_qos0_before_the_packet_count_cap() {
        let (tx, metrics) = start_hub_with_limits(SubscriberLimits {
            max_outbound_bytes: Some(4096),
            ..SubscriberLimits::default()
        });
        // Nothing ever calls the meter for this receiver, so the channel never drains —
        // exactly a subscriber that stopped reading.
        let (mut rx, _) = attach_full(&tx, "deaf", 1, true, 0, u16::MAX).await;
        subscribe_qos(&tx, "deaf", "t/1", QoS::AtLeastOnce);

        for i in 0..8u8 {
            publish_sized(&tx, "t/1", QoS::AtMostOnce, 1024, b'a' + i);
        }
        // A QoS 1 arriving while the channel is over its byte bound is NOT shed.
        publish_sized(&tx, "t/1", QoS::AtLeastOnce, 1024, b'Z');

        let mut got = Vec::new();
        while let Some(pkt) = recv_packet(&mut rx).await {
            got.push((payload_of(&pkt)[0], qos_of(&pkt)));
        }
        assert_eq!(
            dropped_for(&metrics, "outbound-full"),
            4,
            "shed at the byte bound with the packet count nowhere near 10 000: {}",
            metrics.render()
        );
        assert_eq!(
            got,
            vec![
                (b'a', QoS::AtMostOnce),
                (b'b', QoS::AtMostOnce),
                (b'c', QoS::AtMostOnce),
                (b'd', QoS::AtMostOnce),
                (b'Z', QoS::AtLeastOnce),
            ],
            "four QoS 0 fit the 4 KiB budget; the QoS 1 flows past it"
        );
        assert_eq!(dropped_for(&metrics, "backlog-overflow"), 0);
    }

    /// Issue #241: the outbound byte counter is exactly the accounted sum of what the
    /// writer has not yet dequeued, and it returns to ZERO once it drains. A counter that
    /// drifted upward would pin the `QoS` 0 gate shut for the rest of the connection.
    #[test]
    fn the_outbound_byte_counter_returns_to_zero_when_the_writer_drains() {
        use crate::backpressure::packet_bytes;
        let (t, mut r) = mpsc::unbounded_channel();
        let (out, meter) = Outbound::new(t);

        let publish = |marker: u8, payload: usize| {
            Packet::Publish(mqtt_codec::packet::Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: "t/1".into(),
                pkid: Some(u16::from(marker)),
                properties: mqtt_codec::Properties::new(),
                payload: Bytes::from(vec![marker; payload]),
            })
        };
        let packets = vec![
            publish(1, 100),
            publish(2, 5000),
            Packet::PubAck(mqtt_codec::packet::Ack::new(3)),
        ];
        let expected: usize = packets.iter().map(packet_bytes).sum();
        for p in &packets {
            assert!(out.send(p.clone()));
        }
        assert_eq!(out.bytes(), expected, "the accounted sum of what is queued");
        assert_eq!(out.depth(), 3);

        // Drained one at a time: each subtraction is that packet's own size.
        let mut remaining = expected;
        while let Ok(pkt) = r.try_recv() {
            meter.drained(&pkt);
            remaining -= packet_bytes(&pkt);
            assert_eq!(out.bytes(), remaining);
        }
        assert_eq!(out.bytes(), 0, "a fully drained channel holds zero bytes");
        assert_eq!(out.depth(), 0);

        // A send that never queued (the client left) leaves both counters alone.
        drop(r);
        assert!(!out.send(publish(9, 42)));
        assert_eq!(out.bytes(), 0);
        assert_eq!(out.depth(), 0);
    }

    /// Issue #241: `MQTTD_MAX_INFLIGHT_MESSAGES` caps the client's OWN Receive Maximum —
    /// legal, because a client's Receive Maximum is a ceiling on what the broker may send,
    /// never a floor. It is the LOSS-FREE lever: the surplus waits in the backlog and
    /// nothing is dropped.
    #[tokio::test]
    async fn an_inflight_ceiling_caps_the_clients_own_receive_maximum() {
        let (tx, metrics) = start_hub_with_limits(SubscriberLimits {
            max_inflight_messages: Some(2),
            ..SubscriberLimits::default()
        });
        let (mut rx, _) = attach_full(&tx, "fast", 1, true, 0, 100).await;
        subscribe_qos(&tx, "fast", "t/1", QoS::AtLeastOnce);
        for i in 0..5u8 {
            publish_sized(&tx, "t/1", QoS::AtLeastOnce, 1024, b'a' + i);
        }

        let a = recv_packet(&mut rx).await.expect("the first");
        let b = recv_packet(&mut rx).await.expect("the second");
        assert_eq!((payload_of(&a)[0], payload_of(&b)[0]), (b'a', b'b'));
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "the ceiling holds the third off the wire even though the client allowed 100"
        );

        // Acking one releases exactly one more — a gate, not a drop.
        pub_ack(&tx, "fast", pkid_of(&a));
        let c = recv_packet(&mut rx)
            .await
            .expect("the third, after one ack");
        assert_eq!(payload_of(&c)[0], b'c');
        assert!(recv_packet(&mut rx).await.is_none());
        assert_eq!(
            dropped_for(&metrics, "backlog-overflow"),
            0,
            "the in-flight ceiling drops nothing"
        );
        assert_eq!(dropped_for(&metrics, "outbound-full"), 0);
        assert_eq!(dropped_for(&metrics, "queue-overflow"), 0);

        // Unset, the client's own 100 stays in force: all five go straight out.
        let (tx, _m) = start_hub_with_limits(SubscriberLimits::default());
        let (mut rx, _) = attach_full(&tx, "fast", 1, true, 0, 100).await;
        subscribe_qos(&tx, "fast", "t/1", QoS::AtLeastOnce);
        for i in 0..5u8 {
            publish_sized(&tx, "t/1", QoS::AtLeastOnce, 1024, b'a' + i);
        }
        for want in b"abcde" {
            let pkt = recv_packet(&mut rx).await.expect("an unthrottled delivery");
            assert_eq!(payload_of(&pkt)[0], *want);
        }
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
        assert!(
            recv_packet(&mut rx2).await.is_none(),
            "neither message may arrive a second time: both are in the durable log \
             (#124) AND held in memory, and the replay must not duplicate what the \
             DUP resume already sent"
        );
    }

    /// The truncation arithmetic behind #124, in isolation, because it is the part that
    /// silently loses data if it is wrong: the log may only be truncated through the
    /// **contiguous** acknowledged prefix. An out-of-order PUBACK for a later message
    /// must not take an earlier, still-unacknowledged one with it.
    #[test]
    fn the_log_truncates_only_through_the_contiguous_acked_prefix() {
        let mut inf = Inflight::default();
        for offset in 1..=3 {
            inf.track(offset);
        }
        assert_eq!(
            inf.advance_ack(),
            None,
            "nothing acknowledged yet: offset 1 is still owed, so there is no prefix"
        );

        // The MIDDLE message is acknowledged first — legal, since Receive Maximum > 1
        // allows several in flight and MQTT does not require ordered acknowledgement.
        inf.release(2);
        assert_eq!(
            inf.advance_ack(),
            None,
            "offset 1 is still owed, so 2 may not be truncated away behind it"
        );

        inf.release(1);
        assert_eq!(
            inf.advance_ack(),
            Some(2),
            "1 and 2 are now both settled; 3 is still owed and holds the point at 2"
        );
        assert_eq!(inf.advance_ack(), None, "no advance means no store write");

        inf.release(3);
        assert_eq!(
            inf.advance_ack(),
            Some(3),
            "with nothing owed the whole log is settled, up to the high-water mark"
        );
    }

    /// An entry that was read from the log but deliberately NOT delivered (expired, or
    /// admitted only by a revoked grant) must still be let go of — otherwise a session
    /// whose entire replay was dropped would hold its log forever.
    #[test]
    fn a_dropped_replay_entry_does_not_pin_the_log() {
        let mut inf = Inflight::default();
        inf.note_offset(1); // read and dropped
        inf.note_offset(2);
        inf.track(2); // read and sent
        assert_eq!(inf.advance_ack(), Some(1), "the dropped entry is settled");
        inf.release(2);
        assert_eq!(inf.advance_ack(), Some(2));
    }

    /// #124: a `QoS` 1 message delivered LIVE to a persistent subscriber is in the
    /// durable log before it reaches the wire, and stays there until that subscriber
    /// acknowledges it — which is what makes it survive a crash in between.
    #[tokio::test]
    async fn a_live_qos1_delivery_to_a_persistent_subscriber_is_durable_until_acked() {
        let store = std::sync::Arc::new(MemorySessionStore::new());
        let tx = start_hub_with_arc(store.clone());
        let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
        subscribe_qos(&tx, "c", "t", QoS::AtLeastOnce);

        publish_qos1(&tx, "t", b"live");
        let delivered = recv_packet(&mut rx).await.unwrap();
        assert_eq!(payload_of(&delivered), b"live");

        let queued = store.pending(&ClientId("c".into()), 0, 16).await.unwrap();
        assert_eq!(
            queued.len(),
            1,
            "the message must be in the durable log WHILE it is in flight — the \
             publisher has been acked and is owed a redelivery if the broker dies"
        );
        assert_eq!(&queued[0].message.payload[..], b"live");

        pub_ack(&tx, "c", pkid_of(&delivered));
        // The PUBACK is processed on the hub loop; a round-trip through it orders this
        // read after the truncation rather than racing it.
        subscribe_qos(&tx, "c", "sync", QoS::AtMostOnce);
        attach_full(&tx, "sync-probe", 9, true, 0, 8).await;

        let after = store.pending(&ClientId("c".into()), 0, 16).await.unwrap();
        assert!(
            after.is_empty(),
            "the subscriber acknowledged, so the log entry is released: {after:?}"
        );
    }

    /// The other half of the #124 rule: durability follows the SESSION. A clean session
    /// has nothing to resume into, so it must not pay for a durable write per delivery —
    /// this is the escape hatch the README points throughput-sensitive users at.
    #[tokio::test]
    async fn a_live_qos1_delivery_to_a_clean_session_writes_nothing() {
        let store = std::sync::Arc::new(MemorySessionStore::new());
        let tx = start_hub_with_arc(store.clone());
        let (mut rx, _) = attach_full(&tx, "c", 1, true, 0, 8).await;
        subscribe_qos(&tx, "c", "t", QoS::AtLeastOnce);

        publish_qos1(&tx, "t", b"live");
        assert_eq!(payload_of(&recv_packet(&mut rx).await.unwrap()), b"live");

        assert!(
            store
                .pending(&ClientId("c".into()), 0, 16)
                .await
                .unwrap()
                .is_empty(),
            "a clean session owes no redelivery, so nothing is written"
        );
    }

    /// ADR 0057: a live `QoS` 2 delivery records its packet id durably BEFORE the wire,
    /// advances the record at PUBREC, and releases it at PUBCOMP. The id in the store must
    /// BE the id on the wire — that identity is the whole point.
    #[tokio::test]
    async fn a_live_qos2_delivery_records_its_packet_id_until_pubcomp() {
        let store = std::sync::Arc::new(MemorySessionStore::new());
        let tx = start_hub_with_arc(store.clone());
        let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
        subscribe_qos(&tx, "c", "t", QoS::ExactlyOnce);

        publish_qos2(&tx, "t", b"exactly-once");
        let delivered = recv_packet(&mut rx).await.unwrap();
        let pkid = pkid_of(&delivered);

        let c = ClientId("c".into());
        let inflight = store.outbound(&c).await.unwrap();
        assert_eq!(
            inflight.len(),
            1,
            "the id is durable while the PUBLISH is in flight"
        );
        assert_eq!(inflight[0].packet_id, pkid, "the durable id IS the wire id");
        assert!(!inflight[0].pubrec_seen, "phase: awaiting PUBREC");
        let logged = store.pending(&c, 0, 16).await.unwrap();
        assert_eq!(
            inflight[0].offset, logged[0].offset,
            "the id points at its message"
        );

        pub_rec(&tx, "c", pkid);
        let rel = recv_packet(&mut rx).await.unwrap();
        assert!(
            matches!(&rel, Packet::PubRel(k) if k.pkid == pkid),
            "{rel:?}"
        );
        let inflight = store.outbound(&c).await.unwrap();
        assert!(inflight[0].pubrec_seen, "phase advanced durably at PUBREC");

        pub_comp(&tx, "c", pkid);
        // Round-trip through the hub loop so the reads below are ordered after it.
        attach_full(&tx, "sync-probe", 9, true, 0, 8).await;
        assert!(
            store.outbound(&c).await.unwrap().is_empty(),
            "PUBCOMP releases the durable id"
        );
        assert!(
            store.pending(&c, 0, 16).await.unwrap().is_empty(),
            "…and the message log entry (#124)"
        );
    }

    /// ADR 0057, fail closed: if the outbound id cannot be made durable, the PUBLISH is
    /// withheld — sent under an unsurvivable id, a crash would replay it under a fresh
    /// one, which is exactly the #130 defect. The delivery defers (message stays durable)
    /// and goes out IN ORDER once the store recovers, with traffic as the retry clock.
    #[tokio::test]
    async fn a_failed_outbound_id_write_defers_the_publish_in_order() {
        let store = FlakyStore::new(0);
        let tx = start_hub_with_arc(store.clone());
        let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
        subscribe_qos(&tx, "c", "t", QoS::ExactlyOnce);

        store
            .fail_outbound
            .store(true, std::sync::atomic::Ordering::Relaxed);
        publish_qos2(&tx, "t", b"first");
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "the PUBLISH must be withheld while its id cannot be made durable"
        );
        let c = ClientId("c".into());
        assert_eq!(
            store.pending(&c, 0, 16).await.unwrap().len(),
            1,
            "the message itself is already durable (#124) — nothing is lost, only deferred"
        );
        assert!(store.outbound(&c).await.unwrap().is_empty());

        // Store recovers; the NEXT publish must not overtake the deferred one.
        store
            .fail_outbound
            .store(false, std::sync::atomic::Ordering::Relaxed);
        publish_qos2(&tx, "t", b"second");
        let a = recv_packet(&mut rx)
            .await
            .expect("deferred delivery drains first");
        let b = recv_packet(&mut rx).await.expect("then the new one");
        assert_eq!(
            payload_of(&a),
            b"first",
            "per-client order holds across the deferral"
        );
        assert_eq!(payload_of(&b), b"second");
        assert_eq!(
            store.outbound(&c).await.unwrap().len(),
            2,
            "both deliveries recorded their ids once the store allowed it"
        );
    }

    /// ADR 0057, fail closed at PUBREC: if the phase cannot advance durably, the PUBREL
    /// is withheld — the subscriber re-sends PUBREC, and that retry is the write's retry.
    /// A PUBREL sent on a failed write would restore into `AwaitingPubRec` and re-PUBLISH
    /// a message the subscriber already released: a duplicate of our own making.
    #[tokio::test]
    async fn a_failed_phase_advance_withholds_the_pubrel() {
        let store = FlakyStore::new(0);
        let tx = start_hub_with_arc(store.clone());
        let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
        subscribe_qos(&tx, "c", "t", QoS::ExactlyOnce);

        publish_qos2(&tx, "t", b"m");
        let pkid = pkid_of(&recv_packet(&mut rx).await.unwrap());

        store
            .fail_outbound
            .store(true, std::sync::atomic::Ordering::Relaxed);
        pub_rec(&tx, "c", pkid);
        assert!(recv_packet(&mut rx).await.is_none(), "PUBREL withheld");
        let c = ClientId("c".into());
        assert!(
            !store.outbound(&c).await.unwrap()[0].pubrec_seen,
            "the durable phase did not move either — memory and store agree"
        );

        store
            .fail_outbound
            .store(false, std::sync::atomic::Ordering::Relaxed);
        pub_rec(&tx, "c", pkid);
        let rel = recv_packet(&mut rx).await.unwrap();
        assert!(matches!(&rel, Packet::PubRel(k) if k.pkid == pkid));
        assert!(store.outbound(&c).await.unwrap()[0].pubrec_seen);
    }

    /// ADR 0057 excludes `QoS` 1 by design: a fresh-id DUP redelivery is what
    /// at-least-once means, so persisting its id would buy nothing.
    #[tokio::test]
    async fn a_qos1_delivery_records_no_outbound_id() {
        let store = std::sync::Arc::new(MemorySessionStore::new());
        let tx = start_hub_with_arc(store.clone());
        let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
        subscribe_qos(&tx, "c", "t", QoS::AtLeastOnce);

        publish_qos1(&tx, "t", b"m");
        recv_packet(&mut rx).await.unwrap();
        let c = ClientId("c".into());
        assert_eq!(
            store.pending(&c, 0, 16).await.unwrap().len(),
            1,
            "durable (#124)"
        );
        assert!(
            store.outbound(&c).await.unwrap().is_empty(),
            "no outbound id is recorded at `QoS` 1"
        );
    }

    /// ADR 0057 T3, the #130 acceptance shape in-process: subscriber PUBRECs, the broker
    /// "restarts" (a second hub over the SAME store — memory gone, durable state not),
    /// and the resumed session receives a bare PUBREL **under the id it already knows**.
    /// Never a second PUBLISH: the subscriber holds the message and owes only release.
    #[tokio::test]
    async fn a_restart_after_pubrec_resumes_with_pubrel_under_the_original_id() {
        let store = std::sync::Arc::new(MemorySessionStore::new());
        let tx1 = start_hub_with_arc(store.clone());
        let (mut rx1, _) = attach_full(&tx1, "c", 1, false, u32::MAX, 8).await;
        subscribe_qos(&tx1, "c", "t", QoS::ExactlyOnce);
        publish_qos2(&tx1, "t", b"survivor");
        let pkid = pkid_of(&recv_packet(&mut rx1).await.unwrap());
        pub_rec(&tx1, "c", pkid);
        assert!(
            matches!(&recv_packet(&mut rx1).await.unwrap(), Packet::PubRel(k) if k.pkid == pkid)
        );
        // Crash here: PUBCOMP never arrives. hub2 shares only the durable store.

        let tx2 = start_hub_with_arc(store.clone());
        let (mut rx2, present) = attach_full(&tx2, "c", 2, false, u32::MAX, 8).await;
        assert!(present, "the durable session resumed");
        let resumed = recv_packet(&mut rx2).await.unwrap();
        assert!(
            matches!(&resumed, Packet::PubRel(k) if k.pkid == pkid),
            "expected PUBREL under the ORIGINAL id {pkid}, got {resumed:?} — a PUBLISH \
             here is the #130 duplicate"
        );
        assert!(
            recv_packet(&mut rx2).await.is_none(),
            "and nothing else: the replay must not also send the message under a fresh id"
        );

        pub_comp(&tx2, "c", pkid);
        attach_full(&tx2, "sync-probe", 9, true, 0, 8).await;
        let c = ClientId("c".into());
        assert!(store.outbound(&c).await.unwrap().is_empty(), "id released");
        assert!(
            store.pending(&c, 0, 16).await.unwrap().is_empty(),
            "log released (#124)"
        );
    }

    /// The other phase: a restart BEFORE the PUBREC arrived re-sends the PUBLISH with DUP
    /// under the original id — the subscriber may or may not have seen it, and its dedup
    /// window matches the id either way. Exactly one copy goes out.
    #[tokio::test]
    async fn a_restart_before_pubrec_republishes_dup_under_the_original_id() {
        let store = std::sync::Arc::new(MemorySessionStore::new());
        let tx1 = start_hub_with_arc(store.clone());
        let (mut rx1, _) = attach_full(&tx1, "c", 1, false, u32::MAX, 8).await;
        subscribe_qos(&tx1, "c", "t", QoS::ExactlyOnce);
        publish_qos2(&tx1, "t", b"pre-rec");
        let pkid = pkid_of(&recv_packet(&mut rx1).await.unwrap());
        // Crash before any PUBREC.

        let tx2 = start_hub_with_arc(store.clone());
        let (mut rx2, _) = attach_full(&tx2, "c", 2, false, u32::MAX, 8).await;
        let resumed = recv_packet(&mut rx2).await.unwrap();
        match &resumed {
            Packet::Publish(pb) => {
                assert_eq!(
                    pb.pkid,
                    Some(pkid),
                    "the ORIGINAL id, not a fresh allocation"
                );
                assert!(pb.dup, "a possible re-send carries DUP [MQTT-4.4.0-1]");
                assert_eq!(&pb.payload[..], b"pre-rec");
            }
            other => panic!("expected the re-published message, got {other:?}"),
        }
        assert!(recv_packet(&mut rx2).await.is_none(), "exactly one copy");

        // The handshake continues normally under that id.
        pub_rec(&tx2, "c", pkid);
        assert!(
            matches!(&recv_packet(&mut rx2).await.unwrap(), Packet::PubRel(k) if k.pkid == pkid)
        );

        // And a fresh delivery cannot collide with the restored id.
        publish_qos2(&tx2, "t", b"fresh");
        let fresh = recv_packet(&mut rx2).await.unwrap();
        assert_ne!(
            pkid_of(&fresh),
            pkid,
            "the allocator skips restored in-flight ids"
        );
    }

    /// An orphaned table entry — released phase, but its message already left the log
    /// (an earlier clear failed; ADR 0057 tolerates that). The restore sends the priced-in
    /// spurious PUBREL; the subscriber's PUBCOMP clears the orphan, because `pub_comp`
    /// clears unconditionally. This is the tolerance's other half — without it the orphan
    /// would send one PUBREL per restore, forever.
    #[tokio::test]
    async fn an_orphaned_released_id_is_cleared_by_the_spurious_pubrel_cycle() {
        let store = std::sync::Arc::new(MemorySessionStore::new());
        let c = ClientId("c".into());
        // A session whose table holds a released id with NO matching queue entry.
        store.ensure_session(&c).await.unwrap();
        store.record_outbound(&c, 55, 999).await.unwrap();
        store.advance_outbound(&c, 55).await.unwrap();

        let tx = start_hub_with_arc(store.clone());
        let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
        let spurious = recv_packet(&mut rx).await.unwrap();
        assert!(matches!(&spurious, Packet::PubRel(k) if k.pkid == 55));

        pub_comp(&tx, "c", 55);
        attach_full(&tx, "sync-probe", 9, true, 0, 8).await;
        assert!(
            store.outbound(&c).await.unwrap().is_empty(),
            "the PUBCOMP cleared the orphan even though no pending entry completed"
        );
    }

    /// Push a durable roster (issue #229) onto the hub's placement via the
    /// attached ring — the seam the reconcile driver uses in production.
    fn set_roster_on(placement: &Arc<RwLock<Placement>>, names: &[&str], unknown: usize) {
        let known = names.iter().map(|n| NodeId((*n).into())).collect();
        placement
            .write()
            .unwrap()
            .set_durable_roster(known, unknown);
    }

    /// Ask the hub for a fresh digest by requesting one through the peer seam:
    /// read `RetainedDigest` frames until the channel goes quiet, returning the
    /// LAST one seen (the freshest picture).
    async fn latest_digest(peer: &mut mpsc::UnboundedReceiver<PeerMessage>) -> (u64, u64, u64) {
        // The link-up offer is already in flight: poll frames until quiet and
        // keep the last digest (the freshest picture).
        let mut last = None;
        loop {
            match recv_peer(peer).await {
                Some(PeerMessage::RetainedDigest {
                    count,
                    hash,
                    value_hash,
                }) => last = Some((count, hash, value_hash)),
                Some(_) => {}
                None => break,
            }
        }
        last.expect("a digest frame must have been offered")
    }

    fn publish_retained_with_expiry(
        tx: &HubTx,
        topic: &str,
        payload: &'static [u8],
        message_expiry: Option<u32>,
    ) {
        tx.send(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::from_static(payload),
            qos: QoS::AtMostOnce,
            retain: true,
            message_expiry,
            app: AppProperties::default(),
            done: None,
            v5: false,
            publisher: None,
        })
        .unwrap();
    }

    /// A single-node durable-retained hub on a controllable wall clock (issue #227):
    /// this node owns every group, so owner-side expiry reaping is exercised.
    fn start_durable_hub_with_clock() -> (
        HubTx,
        TestDurableRetained,
        TestClock,
        Arc<RwLock<Placement>>,
    ) {
        let clock = TestClock::new(1_000_000);
        let local = NodeId("hub-test".into());
        let placement = Arc::new(RwLock::new(Placement::new(local.clone(), DEFAULT_REPLICAS)));
        let (mut hub, tx) = Hub::with_config_and_placement(
            local,
            Arc::new(MemorySessionStore::new()),
            Some(placement.clone()),
        );
        let handle = Arc::new(mqtt_storage::retained_log::ReplicatedRetained::new(
            InMemoryReplicatedLog::new(),
        ));
        hub.attach_durable_retained(handle.clone());
        hub.attach_clock(Arc::new(clock.clone()));
        tokio::spawn(hub.run());
        (tx, handle, clock, placement)
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
            publisher: None,
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
        start_hub_with_durable_retained_store(peers, Arc::new(MemorySessionStore::new()))
    }

    /// As above, over a session store the CALLER holds — so a test can read the offline
    /// queues the hub wrote and compare them, rather than infer them from deliveries.
    fn start_hub_with_durable_retained_store(
        peers: &[&str],
        store: Arc<dyn mqtt_storage::SessionStore>,
    ) -> (HubTx, TestDurableRetained, Arc<RwLock<Placement>>) {
        let local = NodeId("hub-test".into());
        let mut p = Placement::new(local.clone(), DEFAULT_REPLICAS);
        for n in peers {
            p.observe(&NodeId((*n).into()), MemberState::Alive, "peer:7000", None);
        }
        let placement = Arc::new(RwLock::new(p));
        let (mut hub, tx) = Hub::with_config_and_placement(local, store, Some(placement.clone()));
        let handle = Arc::new(mqtt_storage::retained_log::ReplicatedRetained::new(
            InMemoryReplicatedLog::new(),
        ));
        hub.attach_durable_retained(handle.clone());
        tokio::spawn(hub.run());
        (tx, handle, placement)
    }

    /// Write one retained value the way a RESTORE does (ADR 0062) and wait for the answer.
    async fn restore_retained(tx: &HubTx, topic: &str, payload: &'static [u8]) -> PublishOutcome {
        let (done, rx) = oneshot::channel();
        tx.send(HubCommand::RestoreRetained {
            topic: topic.into(),
            payload: Bytes::from_static(payload),
            qos: QoS::ExactlyOnce,
            message_expiry: None,
            app: AppProperties::default(),
            done,
        })
        .unwrap();
        timeout(Duration::from_secs(5), rx)
            .await
            .expect("the restore's retained write must be answered")
            .expect("the hub must not drop the answer")
    }

    /// The payloads in `client`'s offline queue, in order — the queue as a restore would
    /// have to reproduce it.
    async fn queued_payloads(
        store: &Arc<dyn mqtt_storage::SessionStore>,
        client: &str,
    ) -> Vec<Vec<u8>> {
        store
            .pending(&ClientId(client.into()), 0, 256)
            .await
            .expect("the offline queue reads")
            .into_iter()
            .map(|q| q.message.payload.to_vec())
            .collect()
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

    /// **A restore writes retained state; it does not publish** (ADR 0062, issue #249).
    ///
    /// The bug this pins was not a missing feature but an invented one: the restore
    /// re-published every exported retained value as `Publish { retain: true }`, and a
    /// publish fans out. An offline DURABLE session needs no connected listener to receive
    /// one — the hub appends straight to its queue — so every restored session whose
    /// restored subscription matched a retained topic gained one message per topic PER NODE
    /// that were in no export, at whatever `QoS` the subscription granted (an exactly-once
    /// violation introduced by the recovery tool itself, at `QoS` 2).
    ///
    /// So the assertion is an EQUALITY, not a presence check: the offline queue after the
    /// retained restore must be **byte-for-byte the queue before it**, while the retained
    /// values are nonetheless durably committed with their tokens and replay to a new
    /// subscriber. Count-based or set-based checks cannot see this defect — the injected
    /// copies are duplicates of a value that legitimately exists elsewhere.
    #[tokio::test]
    async fn a_restored_retained_value_is_retained_state_and_never_touches_a_restored_queue() {
        let store: Arc<dyn mqtt_storage::SessionStore> = Arc::new(MemorySessionStore::new());
        let (tx, durable, _placement) = start_hub_with_durable_retained_store(&[], store.clone());

        // A restored durable session: subscribed to `cfg/#` (the wildcard a config-topic
        // deployment really uses), offline, holding exactly the queue its export carried.
        let (_rx, _) = attach(&tx, "psub", 1, false).await;
        subscribe(&tx, "psub", "cfg/#");
        detach(&tx, "psub", 1);
        publish(&tx, "cfg/a", b"from-the-export");
        let exported = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                let q = queued_payloads(&store, "psub").await;
                if q.len() == 1 || tokio::time::Instant::now() >= deadline {
                    break q;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        assert_eq!(
            exported,
            vec![b"from-the-export".to_vec()],
            "the queue this test compares against must be the one the export carried"
        );

        // The restore's retained writes, on topics the restored subscription MATCHES —
        // and on the very topic one queued message came from.
        for (topic, payload) in [
            ("cfg/a", b"retained-a" as &'static [u8]),
            ("cfg/b", b"retained-b"),
        ] {
            assert_eq!(
                restore_retained(&tx, topic, payload).await,
                PublishOutcome::Accepted,
                "a restored retained value must be answered only once it is durably the \
                 topic's retained state"
            );
        }

        // (1) The queue is untouched — the whole point.
        assert_eq!(
            queued_payloads(&store, "psub").await,
            exported,
            "the restored session's queue must EQUAL the exported queue exactly; a restore \
             that fans its retained set out injects one message per matching topic per node"
        );

        // (2) And the retained state is really there: committed with a token, and replayed
        // to a subscriber that arrives afterwards.
        for topic in ["cfg/a", "cfg/b"] {
            let e = wait_durable_retained(&durable, topic, |_| true).await;
            assert!(!e.tombstone, "{topic} must hold a value, not a clear");
            assert!(e.token() > (0, 0), "{topic} must carry a convergence token");
        }
        let (mut fresh, _) = attach(&tx, "reader", 9, true).await;
        subscribe(&tx, "reader", "cfg/#");
        let mut seen: Vec<Vec<u8>> = Vec::new();
        while let Some(p) = recv_packet(&mut fresh).await {
            seen.push(payload_of(&p).to_vec());
            if seen.len() == 2 {
                break;
            }
        }
        seen.sort();
        assert_eq!(
            seen,
            vec![b"retained-a".to_vec(), b"retained-b".to_vec()],
            "a subscriber must see the restored values as RETAINED state"
        );
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
            publisher: None,
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
            expires_at: None,
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
            expires_at: None,
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
                expires_at: None,
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
            expires_at: None,
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
            expires_at: None,
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
            expires_at: None,
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
            expires_at: None,
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
            expires_at: None,
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

    /// A retained STORE whose writes can be made to fail — distinct from
    /// [`FlakyRetained`], which is the durable commit log. Cloneable and
    /// shared-state, so the test and the hub see the same store.
    #[derive(Debug, Clone)]
    struct FailingRetainedStore(Arc<FailingStoreState>);

    #[derive(Debug)]
    struct FailingStoreState {
        healthy: std::sync::atomic::AtomicBool,
        inner: mqtt_storage::MemoryRetainedStore,
    }

    impl FailingRetainedStore {
        fn new() -> Self {
            Self(Arc::new(FailingStoreState {
                healthy: std::sync::atomic::AtomicBool::new(true),
                inner: mqtt_storage::MemoryRetainedStore::new(),
            }))
        }
        fn fail_writes(&self) {
            self.0
                .healthy
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        fn heal(&self) {
            self.0
                .healthy
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        async fn held(&self, filter: &str) -> Vec<mqtt_core::Message> {
            use mqtt_storage::RetainedStore as _;
            self.0.inner.matching(filter).await.unwrap()
        }
    }

    #[async_trait::async_trait]
    impl mqtt_storage::RetainedStore for FailingRetainedStore {
        async fn set(
            &self,
            message: &mqtt_core::Message,
        ) -> Result<(), mqtt_storage::StorageError> {
            if !self.0.healthy.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(mqtt_storage::StorageError::NoQuorum);
            }
            self.0.inner.set(message).await
        }
        async fn matching(
            &self,
            filter: &str,
        ) -> Result<Vec<mqtt_core::Message>, mqtt_storage::StorageError> {
            self.0.inner.matching(filter).await
        }
        async fn all(&self) -> Result<Vec<mqtt_core::Message>, mqtt_storage::StorageError> {
            self.0.inner.all().await
        }
    }

    /// A durable retained authority that fails every commit until healed — the
    /// minority side of a partition (`NoQuorum`), from the hub's point of view.
    /// One committed entry in [`FlakyRetained`]: `(topic, payload, tombstone, expires_at)`.
    type FlakyCommit = (String, Vec<u8>, bool, Option<u64>);

    #[derive(Debug, Default)]
    struct FlakyRetained {
        healthy: std::sync::atomic::AtomicBool,
        /// Every successful commit, in order: `(topic, payload, tombstone, expires_at)`.
        committed: std::sync::Mutex<Vec<FlakyCommit>>,
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
            expires_at: Option<u64>,
        ) -> Result<(u64, u64), mqtt_storage::StorageError> {
            if !self.healthy.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(mqtt_storage::StorageError::NoQuorum);
            }
            let mut log = self.committed.lock().unwrap();
            log.push((topic.to_string(), payload.to_vec(), tombstone, expires_at));
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
            expires_at: Option<u64>,
        ) -> Result<(u64, u64), mqtt_storage::StorageError> {
            self.commit(topic, payload, false, expires_at)
        }

        async fn clear(&self, topic: &str) -> Result<(u64, u64), mqtt_storage::StorageError> {
            self.commit(topic, &[], true, None)
        }

        /// Answer from the committed log, as a real durable keyspace does.
        ///
        /// This returned `Ok(None)` unconditionally until 2026-08-07, which was harmless
        /// only because NOTHING IN PRODUCTION CALLED IT — the durable retained keyspace
        /// was written and never read. Issue #87 item 4 is the cost of that: the tombstone
        /// fences lived only in memory, so a restart lost them and deleted retained
        /// messages could be resurrected. The fence now reads back through here, so the
        /// double has to be honest.
        async fn get(
            &self,
            topic: &str,
        ) -> Result<Option<mqtt_storage::retained_log::RetainedEntry>, mqtt_storage::StorageError>
        {
            let log = self.committed.lock().unwrap();
            Ok(log
                .iter()
                .enumerate()
                .rev()
                .find(|(_, (t, _, _, _))| t == topic)
                .map(|(i, (_, payload, tombstone, expires_at))| {
                    mqtt_storage::retained_log::RetainedEntry {
                        payload: payload.clone(),
                        qos: 0,
                        tombstone: *tombstone,
                        props: AppProps::default(),
                        epoch: 0,
                        offset: (i + 1) as u64,
                        expires_at: *expires_at,
                    }
                }))
        }

        /// Reap a topic's record — tombstone discharge (issue #229).
        async fn reap(&self, topic: &str) -> Result<(), mqtt_storage::StorageError> {
            self.committed
                .lock()
                .unwrap()
                .retain(|(t, _, _, _)| t != topic);
            Ok(())
        }

        /// Enumerate committed topics, tombstones included — the issue #183 warm
        /// path reads back through here on a fresh process.
        async fn topics(&self) -> Result<Vec<String>, mqtt_storage::StorageError> {
            let log = self.committed.lock().unwrap();
            let mut topics: Vec<String> = log.iter().map(|(t, _, _, _)| t.clone()).collect();
            topics.sort();
            topics.dedup();
            Ok(topics)
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
            publisher: None,
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
            publisher: None,
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
                publisher: None,
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
    /// A CLEARED retained topic must stay cleared across a restart (issue #87 item 4).
    ///
    /// `retained_tokens` is in-memory. A clear committed while one node was down, followed
    /// by a restart of the survivors, left NOBODY holding the tombstone fence — so when the
    /// absent node returned with its stale value, the snapshot path applied it and the
    /// deleted retained message came back cluster-wide. Retraction was not durable. The
    /// periodic digest made it spread faster, not slower.
    ///
    /// This models the restart directly: a fresh hub (empty `retained_tokens`, as after a
    /// process start) with the durable record already holding the committed TOMBSTONE, then
    /// a peer snapshot offering the stale pre-clear value.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn a_cleared_retained_topic_is_not_resurrected_after_a_restart() {
        let durable = Arc::new(FlakyRetained::default());
        durable.heal();
        // The clear is committed durably, as it would have been before the restart.
        {
            use mqtt_storage::retained_log::DurableRetained as _;
            durable.clear("t").await.expect("commit the tombstone");
        }

        let (mut hub, tx) = Hub::with_config_and_placement(
            NodeId("hub-test".into()),
            Arc::new(MemorySessionStore::new()),
            None,
        );
        let store = FailingRetainedStore::new();
        hub.attach_retained_store(Arc::new(store.clone()));
        hub.attach_durable_retained(durable.clone());
        tokio::spawn(hub.run());

        // A peer that was absent for the clear offers its stale value. Its token is LOWER
        // than the tombstone's, which is exactly what the fence exists to notice — and the
        // fence is only available from the durable record on a fresh process.
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![RetainedWireEntry {
                topic: "t".into(),
                payload: b"stale".to_vec(),
                qos: 0,
                epoch: 0,
                offset: 0,
                props: AppProps::default(),
                expires_at: None,
            }],
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert!(
            store.held("t").await.is_empty(),
            "a deleted retained message was RESURRECTED by a peer snapshot after a restart \
             — the tombstone fence must survive the process that recorded it"
        );
    }

    /// A hub modelling a node whose process restarted: the durable keyspace already
    /// holds `commits` then `cleared` clears (committed before the crash), the
    /// persistent cache reopened with `cached`, and `retained_tokens` is empty — it
    /// is in-memory and did not survive. Returns the hub handle and the (observable)
    /// cache store.
    async fn start_restarted_owner(
        commits: &[(&str, &'static [u8])],
        cleared: &[&str],
        cached: &[(&str, &'static [u8])],
    ) -> (HubTx, FailingRetainedStore) {
        let durable = Arc::new(mqtt_storage::retained_log::ReplicatedRetained::new(
            InMemoryReplicatedLog::new(),
        ));
        for (topic, payload) in commits {
            durable
                .set(topic, payload, 0, &AppProps::default(), None)
                .await
                .expect("pre-crash commit");
        }
        for topic in cleared {
            durable.clear(topic).await.expect("pre-crash clear");
        }
        let store = FailingRetainedStore::new();
        {
            use mqtt_storage::RetainedStore as _;
            for (topic, payload) in cached {
                store
                    .set(&mqtt_core::Message {
                        topic: (*topic).to_string(),
                        payload: Bytes::from_static(payload),
                        qos: QoS::AtMostOnce,
                        retain: true,
                        app: AppProperties::default(),
                        expires_at: None,
                    })
                    .await
                    .expect("seed the reopened cache");
            }
        }
        let (mut hub, tx) = Hub::with_config_and_placement(
            NodeId("restarted".into()),
            Arc::new(MemorySessionStore::new()),
            None,
        );
        hub.attach_retained_store(Arc::new(store.clone()));
        hub.attach_durable_retained(durable);
        tokio::spawn(hub.run());
        (tx, store)
    }

    /// Issue #214 (the acked-facts proc tier's identical seed-0 double flake): a node
    /// committed a retained value, was `SIGKILL`ed before its fan-out reached anyone,
    /// and restarted. Its persistent cache serves the committed value — but
    /// `retained_tokens` is in-memory, so the anti-entropy snapshot used to export the
    /// value UNTOKENED (`(0, 0)`), and a peer that had applied the PREVIOUS value's
    /// fan-out still held that older token and fenced the untokened repair out as
    /// stale. Permanently: every digest round re-detected the divergence, re-refused
    /// the repair, and logged "converged". The snapshot must export the committed
    /// record under its COMMITTED token, re-read from the durable authority — and
    /// where the reopened cache itself predates the authority (the crash landed
    /// between the commit and the owner's own cache apply), re-adopt the committed
    /// record rather than exporting the stale cache value under the new token.
    #[tokio::test]
    async fn a_restarted_owners_snapshot_exports_committed_tokens_and_readopts_the_record() {
        // v1 then v2 committed pre-crash (tokens (0,1), (0,2)); the reopened cache
        // still holds v1 — the crash beat the owner's own cache apply of v2.
        let (tx, store) =
            start_restarted_owner(&[("rt/1", b"v1"), ("rt/1", b"v2")], &[], &[("rt/1", b"v1")])
                .await;
        let mut peer = connect_peer(&tx, "n", 1);
        tx.send(HubCommand::RemoteRetainedRequest {
            node: NodeId("n".into()),
        })
        .unwrap();
        loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedSnapshot { messages }) => {
                    let e = messages
                        .iter()
                        .find(|e| e.topic == "rt/1")
                        .expect("rt/1 exported");
                    assert_eq!(
                        (e.epoch, e.offset),
                        (0, 2),
                        "the snapshot must carry the COMMITTED token, not (0,0): an \
                         untokened export is refused by every peer holding an older \
                         applied token, and the divergence never heals"
                    );
                    assert_eq!(
                        e.payload, b"v2",
                        "the snapshot must carry the committed RECORD, not the \
                         pre-crash cache value"
                    );
                    break;
                }
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        // The export re-adopted the authority's record locally: this node now serves
        // what it acked before the crash.
        let held = store.held("rt/1").await;
        assert_eq!(held.len(), 1);
        assert_eq!(
            &held[0].payload[..],
            b"v2",
            "re-reading the authority must repair the reopened cache too"
        );
    }

    /// The receiving half of the issue #214 replay, end to end: the peer applied the
    /// last fan-out it ever saw (v1, token (0,1)); the restarted owner's snapshot —
    /// as the FIXED sender builds it — must move the peer to the committed v2. With
    /// the old untokened export this peer kept v1 forever while both sides logged
    /// convergence.
    #[tokio::test]
    async fn a_peer_that_missed_the_last_fanout_converges_from_a_restarted_owners_snapshot() {
        // The restarted owner (sender): v1, v2 committed; cache reopened with v2.
        let (owner_tx, _owner_store) =
            start_restarted_owner(&[("rt/1", b"v1"), ("rt/1", b"v2")], &[], &[("rt/1", b"v2")])
                .await;
        let mut owner_peer = connect_peer(&owner_tx, "peer", 1);

        // The peer (receiver): durable mode on, nothing committed in ITS keyspace for
        // the topic (a non-owner cannot read the authority); it applied v1's fan-out.
        let durable = Arc::new(FlakyRetained::default());
        durable.heal();
        let (mut hub, peer_tx) = Hub::with_config_and_placement(
            NodeId("peer".into()),
            Arc::new(MemorySessionStore::new()),
            None,
        );
        let peer_store = FailingRetainedStore::new();
        hub.attach_retained_store(Arc::new(peer_store.clone()));
        hub.attach_durable_retained(durable);
        tokio::spawn(hub.run());
        peer_tx
            .send(HubCommand::RemoteRetainedUpdate {
                topic: "rt/1".into(),
                payload: Bytes::from_static(b"v1"),
                qos: 0,
                epoch: 0,
                offset: 1,
                app: AppProperties::default(),
                expires_at: None,
            })
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let held = peer_store.held("rt/1").await;
            if held.len() == 1 && &held[0].payload[..] == b"v1" {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "precondition: the v1 fan-out must apply"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Pipe the owner's actual snapshot frames into the peer, verbatim.
        owner_tx
            .send(HubCommand::RemoteRetainedRequest {
                node: NodeId("peer".into()),
            })
            .unwrap();
        loop {
            match recv_peer(&mut owner_peer).await {
                Some(PeerMessage::RetainedSnapshot { messages }) => {
                    peer_tx
                        .send(HubCommand::RemoteRetainedSnapshot {
                            node: NodeId("restarted".into()),
                            messages,
                        })
                        .unwrap();
                    break;
                }
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let held = peer_store.held("rt/1").await;
            if held.len() == 1 && &held[0].payload[..] == b"v2" {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the peer must converge to the committed v2 from the restarted \
                 owner's snapshot — keeping v1 is issue #214's permanent divergence"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The untokened rule across a RESTART: `an_untokened_snapshot_entry_gap_fills_but_
    /// never_overwrites` holds within one process because the applied `(0,0)` is
    /// recorded in `retained_tokens` and fences the next untokened entry — but that map
    /// is in-memory. A fresh process holding the same value (reopened persistent cache,
    /// no tokens) used to let an incoming untokened entry OVERWRITE it: two restarted
    /// nodes holding different uncommitted values would swap them. The gap-fill-only
    /// rule must hold from the VALUES, not from a fence that dies with the process.
    #[tokio::test]
    async fn an_untokened_snapshot_entry_never_overwrites_across_a_restart() {
        let durable = Arc::new(FlakyRetained::default());
        durable.heal();
        let (mut hub, tx) = Hub::with_config_and_placement(
            NodeId("hub-test".into()),
            Arc::new(MemorySessionStore::new()),
            None,
        );
        let store = FailingRetainedStore::new();
        hub.attach_retained_store(Arc::new(store.clone()));
        hub.attach_durable_retained(durable);
        tokio::spawn(hub.run());

        // Hold an (uncommitted) value for rt/held; rt/absent has nothing.
        {
            use mqtt_storage::RetainedStore as _;
            store
                .set(&mqtt_core::Message {
                    topic: "rt/held".into(),
                    payload: Bytes::from_static(b"ours"),
                    qos: QoS::AtMostOnce,
                    retain: true,
                    app: AppProperties::default(),
                    expires_at: None,
                })
                .await
                .unwrap();
        }
        tx.send(HubCommand::RemoteRetainedSnapshot {
            node: NodeId("n".into()),
            messages: vec![
                RetainedWireEntry {
                    topic: "rt/held".into(),
                    payload: b"theirs".to_vec(),
                    qos: 0,
                    epoch: 0,
                    offset: 0,
                    props: AppProps::default(),
                    expires_at: None,
                },
                RetainedWireEntry {
                    topic: "rt/absent".into(),
                    payload: b"fill".to_vec(),
                    qos: 0,
                    epoch: 0,
                    offset: 0,
                    props: AppProps::default(),
                    expires_at: None,
                },
            ],
        })
        .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if !store.held("rt/absent").await.is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "an untokened entry must still gap-fill an absent topic"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let held = store.held("rt/held").await;
        assert_eq!(held.len(), 1);
        assert_eq!(
            &held[0].payload[..],
            b"ours",
            "an untokened entry must never overwrite a held value (ADR 0037 P5)"
        );
    }

    /// Issue #183, the acceptance sequence: **a cleared retained topic stays cleared
    /// across a survivor restart + stale-node return.** A clear leaves NOTHING for a
    /// fresh process to rediscover the fence from — no cache entry, and the token map
    /// died with the process — so a restarted node used to go digest-silent and its
    /// snapshots omitted the tombstone entirely: a peer that was down for the clear
    /// kept serving the deleted value until the topic's next committed write.
    /// `warm_retained_tokens_from_authority` re-arms the fence from the keyspace
    /// (the tombstone IS durably committed, ADR 0037 P2) before a digest or snapshot
    /// is built, so retraction survives in an exportable form.
    #[tokio::test]
    async fn a_cleared_topic_stays_cleared_for_a_peer_that_missed_the_clear() {
        // The restarted node: v committed then CLEARED pre-crash; the reopened cache
        // is (correctly) empty; the token map is empty — tombstone-only state.
        let (owner_tx, owner_store) =
            start_restarted_owner(&[("rt/c", b"v")], &["rt/c"], &[]).await;
        let mut owner_peer = connect_peer(&owner_tx, "peer", 1);

        // The peer that missed the clear: it applied v's fan-out and still serves it.
        let peer_durable = Arc::new(FlakyRetained::default());
        peer_durable.heal();
        let (mut hub, peer_tx) = Hub::with_config_and_placement(
            NodeId("peer".into()),
            Arc::new(MemorySessionStore::new()),
            None,
        );
        let peer_store = FailingRetainedStore::new();
        hub.attach_retained_store(Arc::new(peer_store.clone()));
        hub.attach_durable_retained(peer_durable);
        tokio::spawn(hub.run());
        peer_tx
            .send(HubCommand::RemoteRetainedUpdate {
                topic: "rt/c".into(),
                payload: Bytes::from_static(b"v"),
                qos: 0,
                epoch: 0,
                offset: 1,
                app: AppProperties::default(),
                expires_at: None,
            })
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if !peer_store.held("rt/c").await.is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "precondition: the peer must hold the pre-clear value"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // The peer pulls the restarted node's snapshot (the digest-difference path).
        // The snapshot must CARRY the clear: an empty-payload entry under the
        // tombstone's committed token — re-learned from the keyspace, since neither
        // the cache nor the token map remembers it.
        owner_tx
            .send(HubCommand::RemoteRetainedRequest {
                node: NodeId("peer".into()),
            })
            .unwrap();
        loop {
            match recv_peer(&mut owner_peer).await {
                Some(PeerMessage::RetainedSnapshot { messages }) => {
                    let e = messages
                        .iter()
                        .find(|e| e.topic == "rt/c")
                        .expect("the tombstone must be exported after a restart");
                    assert!(e.payload.is_empty(), "a clear rides as an empty payload");
                    assert_eq!(
                        (e.epoch, e.offset),
                        (0, 2),
                        "under the clear's committed token"
                    );
                    peer_tx
                        .send(HubCommand::RemoteRetainedSnapshot {
                            node: NodeId("restarted".into()),
                            messages,
                        })
                        .unwrap();
                    break;
                }
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        // The deleted value goes, and stays gone.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if peer_store.held("rt/c").await.is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the peer must drop the deleted value once the tombstone reaches it \
                 — this is issue #183's resurrection, un-fixed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // And the restarted node is no longer digest-silent: a LATER link-up offers
        // its (tombstone-only) digest, so peers that reconnect afterwards still
        // learn there is something to compare against.
        assert!(
            owner_store.held("rt/c").await.is_empty(),
            "the restarted node's own cache stays clear"
        );
        let mut late_peer = connect_peer(&owner_tx, "late", 2);
        assert!(matches!(
            recv_peer(&mut late_peer).await,
            Some(PeerMessage::Interest { .. })
        ));
        assert!(
            matches!(
                recv_peer(&mut late_peer).await,
                Some(PeerMessage::RetainedDigest { .. })
            ),
            "a tombstone-only RESTARTED node must offer its digest at link-up once \
             the warm has re-armed its fences"
        );
    }

    /// The harsher variant: the crash landed between the clear's commit and the
    /// owner's own cache apply, so the reopened cache still SERVES the deleted value.
    /// The committed tombstone must be re-adopted — dropping the value locally — and
    /// the clear exported, not the stale cache entry. (Here the topic is still
    /// discoverable FROM the cache, so the #214 per-topic authority read covers it
    /// even without the warm; the warm is load-bearing for the tombstone-ONLY state
    /// the previous test pins, where nothing names the topic anymore.)
    #[tokio::test]
    async fn a_reopened_cache_holding_a_value_the_cluster_cleared_drops_it() {
        let (tx, store) =
            start_restarted_owner(&[("rt/c", b"v")], &["rt/c"], &[("rt/c", b"v")]).await;
        let mut peer = connect_peer(&tx, "n", 1);
        tx.send(HubCommand::RemoteRetainedRequest {
            node: NodeId("n".into()),
        })
        .unwrap();
        loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedSnapshot { messages }) => {
                    let e = messages
                        .iter()
                        .find(|e| e.topic == "rt/c")
                        .expect("rt/c must appear in the snapshot");
                    assert!(
                        e.payload.is_empty(),
                        "the committed CLEAR must be exported, not the stale pre-clear \
                         cache value"
                    );
                    assert_eq!((e.epoch, e.offset), (0, 2));
                    break;
                }
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        assert!(
            store.held("rt/c").await.is_empty(),
            "re-adopting the tombstone must drop the deleted value from the reopened cache"
        );
    }

    /// Issue #219 acceptance, case 1: a retained value committed while a fresh
    /// subscription's interest was still propagating never got a live forward (the
    /// landing node did not know the subscriber) — the owner's fan-out, which
    /// reaches every node regardless of interest, must deliver it to the windowed
    /// subscriber. Exactly once.
    #[tokio::test]
    async fn a_retained_commit_in_the_interest_window_reaches_the_fresh_subscriber() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        let (mut sub, _) = attach(&tx, "fresh", 1, true).await;
        subscribe(&tx, "fresh", "iw/t");
        // The committed fan-out lands; no live copy ever arrived here.
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "iw/t".into(),
            payload: Bytes::from_static(b"v1"),
            qos: 0,
            epoch: 1,
            offset: 1,
            app: AppProperties::default(),
            expires_at: None,
        })
        .unwrap();
        let p = recv_packet(&mut sub)
            .await
            .expect("the windowed apply path must deliver the missed commit");
        assert_eq!(payload_of(&p), b"v1");
        assert!(
            recv_packet(&mut sub).await.is_none(),
            "exactly once — nothing further is owed"
        );
    }

    /// Issue #219 acceptance, case 2: the landing node DID know the fresh
    /// subscriber (interest arrived in time) and forwarded the live copy — the
    /// fan-out applying afterwards must not deliver the same value again. The
    /// live path records into the window's ledger; only the apply path defers.
    #[tokio::test]
    async fn the_windowed_apply_defers_to_a_live_copy_already_delivered() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        let (mut sub, _) = attach(&tx, "fresh", 1, true).await;
        subscribe(&tx, "fresh", "iw/t");
        // The interest-forwarded live copy (retain flag as published, cache cold
        // under durable — ADR 0037 P4) arrives first…
        tx.send(HubCommand::RemotePublish {
            topic: "iw/t".into(),
            payload: Bytes::from_static(b"v1"),
            qos: QoS::AtMostOnce,
            retain: true,
            message_expiry: None,
            app: AppProperties::default(),
        })
        .unwrap();
        assert_eq!(payload_of(&recv_packet(&mut sub).await.unwrap()), b"v1");
        // …then the owner's fan-out of that same committed value.
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "iw/t".into(),
            payload: Bytes::from_static(b"v1"),
            qos: 0,
            epoch: 1,
            offset: 1,
            app: AppProperties::default(),
            expires_at: None,
        })
        .unwrap();
        assert!(
            recv_packet(&mut sub).await.is_none(),
            "the apply path must not repeat what the live path delivered (#87 item 3)"
        );
    }

    /// Issue #219, the steady-state regression the #87 item 3 rejection protects:
    /// once the window has closed, the apply path never delivers — the
    /// interest-forward path is the only vehicle, so established subscribers see
    /// each retained update exactly once. The value still applies to the cache.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn the_apply_path_stays_silent_once_the_window_has_closed() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        let (mut sub, _) = attach(&tx, "settled", 1, true).await;
        subscribe(&tx, "settled", "iw/t");
        // Let the window lapse (RETAINED_INTEREST_WINDOW; the apply checks the
        // deadline itself, so no sweep tick is needed).
        tokio::time::sleep(super::RETAINED_INTEREST_WINDOW + Duration::from_millis(200)).await;
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "iw/t".into(),
            payload: Bytes::from_static(b"v1"),
            qos: 0,
            epoch: 1,
            offset: 1,
            app: AppProperties::default(),
            expires_at: None,
        })
        .unwrap();
        assert!(
            recv_packet(&mut sub).await.is_none(),
            "steady state: the interest-forward path alone delivers (#87 item 3)"
        );
        // The commit still warmed the cache — a later subscriber replays it.
        assert_eq!(retained_replay(&tx, "later", "iw/t").await.unwrap(), b"v1");
    }

    /// Issue #219: the subscribe-time replay seeds the window's ledger, so a
    /// re-commit of the SAME value (higher token, identical content) inside the
    /// window is not repeated to the subscriber who just replayed it — while a
    /// genuinely newer value still is.
    #[tokio::test]
    async fn the_subscribe_replay_seeds_the_windows_ledger() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        let update = |payload: &'static [u8], offset: u64| {
            tx.send(HubCommand::RemoteRetainedUpdate {
                topic: "iw/t".into(),
                payload: Bytes::from_static(payload),
                qos: 0,
                epoch: 1,
                offset,
                app: AppProperties::default(),
                expires_at: None,
            })
            .unwrap();
        };
        update(b"v1", 1); // in the cache before the subscriber arrives
        let (mut sub, _) = attach(&tx, "fresh", 1, true).await;
        subscribe(&tx, "fresh", "iw/t");
        assert_eq!(
            payload_of(&recv_packet(&mut sub).await.unwrap()),
            b"v1",
            "the subscribe replay shows the current value"
        );
        // The same value re-committed at a higher token: applied to the cache,
        // but the subscriber already saw exactly this value.
        update(b"v1", 2);
        assert!(
            recv_packet(&mut sub).await.is_none(),
            "a re-commit of the replayed value must not repeat"
        );
        // A genuinely newer value delivers.
        update(b"v2", 3);
        assert_eq!(payload_of(&recv_packet(&mut sub).await.unwrap()), b"v2");
    }

    /// Issue #219: an OFFLINE durable subscriber inside its window gets the missed
    /// commit through the ordinary queue semantics of `deliver_to_client`, so the
    /// resume replays it — the window closes the same gap for the queued path.
    #[tokio::test]
    async fn a_windowed_commit_reaches_an_offline_durable_subscriber_on_resume() {
        let (tx, _durable, _placement) = start_hub_with_durable_retained(&[]);
        let (_sub, _) = attach(&tx, "dur", 1, false).await;
        subscribe(&tx, "dur", "iw/t");
        detach(&tx, "dur", 1);
        // Committed elsewhere while the fresh subscription's interest was still
        // propagating — and its holder already offline.
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "iw/t".into(),
            payload: Bytes::from_static(b"v1"),
            qos: 1,
            epoch: 1,
            offset: 1,
            app: AppProperties::default(),
            expires_at: None,
        })
        .unwrap();
        let (mut resumed, present) = attach(&tx, "dur", 2, false).await;
        assert!(present, "the durable session resumes");
        let p = recv_packet(&mut resumed)
            .await
            .expect("the queued windowed delivery replays on resume");
        assert_eq!(payload_of(&p), b"v1");
    }

    /// Issue #229, the discharge: once every durable-roster member's digest has
    /// MATCHED ours after a clear was observed (and a full anti-entropy period has
    /// passed), the tombstone's fence is redundant — nobody who could return still
    /// holds the pre-clear value — so the fence drops and the owner removes the
    /// keyspace record. Durable retraction becomes bounded instead of forever.
    #[tokio::test]
    async fn a_discharged_tombstone_is_reaped_after_the_roster_converges() {
        let (tx, durable, clock, placement) = start_durable_hub_with_clock();
        // The roster: this node plus peer "n" — both must converge.
        set_roster_on(&placement, &["hub-test", "n"], 0);
        publish_retained_with_expiry(&tx, "gc/t", b"v", None);
        publish_retained_with_expiry(&tx, "gc/t", b"", None); // the clear
        wait_durable_retained(&durable, "gc/t", |e| e.tombstone).await;

        // The peer links up AFTER the clear (a tombstone-only state still offers
        // its digest — the #183 guarantee); echoing that digest back is the
        // convergence observation the reap gates on.
        clock.advance(2);
        let mut peer = connect_peer(&tx, "n", 1);
        let digest = latest_digest(&mut peer).await;
        tx.send(HubCommand::RemoteRetainedDigest {
            node: NodeId("n".into()),
            count: digest.0,
            hash: digest.1,
            value_hash: digest.2,
        })
        .unwrap();

        // One full anti-entropy period after the observation, the fence discharges
        // and the owner reaps the record.
        clock.advance(u64::from(super::RETAINED_ANTIENTROPY_EVERY) + 1);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if durable.get("gc/t").await.unwrap().is_none() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the converged-past tombstone must be REAPED from the keyspace"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Issue #229, the safety half: a roster member that has NOT been seen
    /// converged since the clear — or a member this process cannot even name —
    /// blocks the reap indefinitely. The fence outlives every possible stale
    /// holder; that is the whole point of gating on the durable roster instead of
    /// live gossip.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn an_unconverged_or_unnameable_roster_member_blocks_the_reap() {
        let (tx, durable, clock, placement) = start_durable_hub_with_clock();
        set_roster_on(&placement, &["hub-test", "n"], 0);
        publish_retained_with_expiry(&tx, "gc/t", b"v", None);
        publish_retained_with_expiry(&tx, "gc/t", b"", None);
        wait_durable_retained(&durable, "gc/t", |e| e.tombstone).await;

        // No digest from "n" ever matched: a period passing changes nothing.
        clock.advance(u64::from(super::RETAINED_ANTIENTROPY_EVERY) * 3);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(
            durable.get("gc/t").await.unwrap().is_some(),
            "an unconverged roster member must block the reap"
        );

        // Even a converged NAMED member cannot compensate for an unnameable one.
        let mut peer = connect_peer(&tx, "n", 1);
        clock.advance(2);
        let digest = latest_digest(&mut peer).await;
        tx.send(HubCommand::RemoteRetainedDigest {
            node: NodeId("n".into()),
            count: digest.0,
            hash: digest.1,
            value_hash: digest.2,
        })
        .unwrap();
        set_roster_on(&placement, &["hub-test", "n"], 1);
        clock.advance(u64::from(super::RETAINED_ANTIENTROPY_EVERY) + 1);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(
            durable.get("gc/t").await.unwrap().is_some(),
            "an unnameable roster member must block the reap"
        );
    }

    /// Issue #229: a value re-taking a cleared topic ends the tombstone's
    /// discharge bookkeeping — the fence is a value token again and is never
    /// reaped as if it were a clear.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn a_recreated_topic_leaves_the_discharge_clock() {
        let (tx, durable, clock, placement) = start_durable_hub_with_clock();
        set_roster_on(&placement, &["hub-test"], 0); // roster of one: vacuously converged
        publish_retained_with_expiry(&tx, "gc/t", b"v1", None);
        publish_retained_with_expiry(&tx, "gc/t", b"", None);
        wait_durable_retained(&durable, "gc/t", |e| e.tombstone).await;
        publish_retained_with_expiry(&tx, "gc/t", b"v2", None); // re-taken
        wait_durable_retained(&durable, "gc/t", |e| !e.tombstone).await;
        clock.advance(u64::from(super::RETAINED_ANTIENTROPY_EVERY) * 2);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let e = durable.get("gc/t").await.unwrap();
        assert!(
            e.is_some_and(|e| !e.tombstone),
            "a re-taken topic's record must never be reaped by the discharge pass"
        );
        // And a roster-of-one clear DOES discharge (vacuous convergence).
        publish_retained_with_expiry(&tx, "gc/t", b"", None);
        wait_durable_retained(&durable, "gc/t", |e| e.tombstone).await;
        clock.advance(u64::from(super::RETAINED_ANTIENTROPY_EVERY) + 1);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if durable.get("gc/t").await.unwrap().is_none() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "a lone-roster clear discharges after one period"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Issue #227 [MQTT-3.3.2-6]: a retained replay carries the REMAINING Message
    /// Expiry Interval — the received value minus the time the copy sat in the
    /// broker — never the original, and never silence.
    #[tokio::test]
    async fn a_retained_replay_carries_the_remaining_expiry_interval() {
        let (tx, clock) = start_hub_with_clock();
        publish_retained_with_expiry(&tx, "exp/t", b"v", Some(100));
        // Round-trip before advancing: the deadline is stamped when the hub
        // PROCESSES the publish, and the clock is not a hub command.
        assert_eq!(retained_replay(&tx, "warm", "exp/t").await.unwrap(), b"v");
        clock.advance(40);
        let (mut sub, _) = attach(&tx, "late", 1, true).await;
        subscribe(&tx, "late", "exp/t");
        let p = recv_packet(&mut sub).await.expect("fresh value replays");
        assert_eq!(payload_of(&p), b"v");
        assert_eq!(
            message_expiry_of(&p),
            Some(60),
            "the replay must carry the REMAINING interval [MQTT-3.3.2-6]"
        );
    }

    /// Issue #227: an EXPIRED retained value is not replayed to a new subscriber,
    /// and the reap deletes the dead row from the store (visible on the gauge) —
    /// the spec deletes the retained copy at expiry, not merely hides it.
    #[tokio::test]
    async fn an_expired_retained_value_is_not_replayed_and_is_reaped() {
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("test"));
        let clock = TestClock::new(1_000_000);
        let (mut hub, tx) = Hub::with_config(
            NodeId("hub-test".into()),
            std::sync::Arc::new(MemorySessionStore::new()),
        );
        hub.attach_clock(Arc::new(clock.clone()));
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        publish_retained_with_expiry(&tx, "exp/t", b"v", Some(10));
        assert_eq!(
            retained_replay(&tx, "c1", "exp/t").await.unwrap(),
            b"v",
            "fresh: the value replays"
        );
        clock.advance(11);
        assert!(
            retained_replay(&tx, "c2", "exp/t").await.is_none(),
            "expired: the value must not be replayed [MQTT-3.3.2-5]"
        );
        // The per-tick reap (armed by the deadline) deletes the dead row.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.render().contains("retained_messages 0") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the expired retained row must be REAPED from the store, not only \
                 hidden from replay:\n{}",
                metrics.render()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Issue #227 under durable retained: the committed record carries the absolute
    /// deadline, and the OWNER reaps an expired value as an ordinary committed
    /// CLEAR — every cache converges by token, exactly as for a client's
    /// zero-length publish, never by comparing clocks across nodes.
    #[tokio::test]
    async fn an_owner_reaps_an_expired_retained_value_as_a_committed_clear() {
        let (tx, durable, clock, _placement) = start_durable_hub_with_clock();
        publish_retained_with_expiry(&tx, "exp/t", b"v", Some(10));
        // The commit itself carries the deadline (start epoch 1_000_000 + 10).
        let e = wait_durable_retained(&durable, "exp/t", |e| !e.tombstone).await;
        assert_eq!(
            e.expires_at,
            Some(1_000_010),
            "the committed record must carry the absolute deadline"
        );
        clock.advance(11);
        // The reap lands as a committed tombstone through the ordinary queue.
        let e = wait_durable_retained(&durable, "exp/t", |e| e.tombstone).await;
        assert!(e.tombstone, "expiry reaps as a committed clear");
    }

    /// Issue #227: the post-commit fan-out and the anti-entropy snapshot both carry
    /// the committed deadline, so every peer cache expires the value at the same
    /// absolute instant.
    #[tokio::test]
    async fn the_fanout_and_snapshot_carry_the_deadline() {
        let (tx, _durable, clock, _placement) = start_durable_hub_with_clock();
        let _ = clock; // deadline fixed by the start epoch
        let mut peer = connect_peer(&tx, "n", 1);
        publish_retained_with_expiry(&tx, "exp/t", b"v", Some(100));
        loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedUpdate {
                    topic, expires_at, ..
                }) => {
                    assert_eq!(topic, "exp/t");
                    assert_eq!(
                        expires_at,
                        Some(1_000_100),
                        "the fan-out must carry the committed deadline"
                    );
                    break;
                }
                Some(
                    PeerMessage::Interest { .. }
                    | PeerMessage::SharedInterest { .. }
                    | PeerMessage::RetainedDigest { .. },
                ) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
        tx.send(HubCommand::RemoteRetainedRequest {
            node: NodeId("n".into()),
        })
        .unwrap();
        loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedSnapshot { messages }) => {
                    let e = messages.iter().find(|e| e.topic == "exp/t").unwrap();
                    assert_eq!(
                        e.expires_at,
                        Some(1_000_100),
                        "the snapshot entry must carry the committed deadline"
                    );
                    break;
                }
                Some(PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        }
    }

    /// A store write that FAILS must not fence the topic forever (issue #87 item 2).
    ///
    /// The token is the idempotence fence: anything at or below it is skipped. Recording it
    /// before the write meant a failed write left a fence with no value behind it — and
    /// nothing could get past. Re-delivery of the same commit is skipped by the guard, and
    /// the periodic digest (0014-T10) cannot repair it either, because the repairing
    /// snapshot carries the SAME token and `token > held` is false. One transient store
    /// error blackholed one topic on one node permanently, while its peers served the value
    /// normally, with no metric and no divergence signal — a node holding no value has
    /// nothing to compare.
    ///
    /// So the assertion is not merely "the write failed": it is that the topic is still
    /// REPAIRABLE afterwards, by the two paths that were previously fenced out.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn a_failed_retained_store_write_leaves_the_topic_repairable() {
        let store = FailingRetainedStore::new();
        let (mut hub, tx) = Hub::with_config_and_placement(
            NodeId("hub-test".into()),
            Arc::new(MemorySessionStore::new()),
            None,
        );
        hub.attach_retained_store(Arc::new(store.clone()));
        tokio::spawn(hub.run());

        // A committed retained update arrives while the store is refusing writes.
        store.fail_writes();
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "t".into(),
            payload: Bytes::from_static(b"v1"),
            qos: 0,
            epoch: 1,
            offset: 1,
            app: AppProperties::default(),
            expires_at: None,
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            store.held("t").await.is_empty(),
            "precondition: the write really did fail"
        );

        // REPAIR PATH 1 — the same commit re-delivered (a retransmit, or the snapshot the
        // digest pulls, both carrying the SAME token). Previously fenced: token <= held.
        store.heal();
        tx.send(HubCommand::RemoteRetainedUpdate {
            topic: "t".into(),
            payload: Bytes::from_static(b"v1"),
            qos: 0,
            epoch: 1,
            offset: 1,
            app: AppProperties::default(),
            expires_at: None,
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let held = store.held("t").await;
        assert_eq!(
            held.len(),
            1,
            "a re-delivery of the same commit must repair a topic whose write failed — \
             this is the case the periodic digest could not fix"
        );
        assert_eq!(&held[0].payload[..], b"v1");
    }

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
                ("t".to_string(), b"v1".to_vec(), false, None),
                ("t".to_string(), b"v2".to_vec(), false, None),
                ("t".to_string(), Vec::new(), true, None),
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
                publisher: None,
            })
            .unwrap();
        }
        let first_seq = loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::RetainedCommit { payload, seq, .. }) => {
                    assert_eq!(payload, b"v1");
                    break seq;
                }
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
                other => panic!("unexpected peer frame {other:?}"),
            }
        };
        // Until the ack, nothing but retransmissions of the FIRST handoff may
        // appear — v2 must wait. (Under paused time, empty receives auto-advance
        // the clock, so sweep-tick retransmissions of v1 can legitimately land in
        // this window; what must never appear is a different seq.)
        loop {
            match recv_peer_data(&mut peer).await {
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
        match recv_peer_data(&mut peer).await {
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
            match recv_peer_data(&mut peer).await {
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

        let send = |seq: u64| {
            tx.send(HubCommand::RemoteRetainedCommit {
                node: NodeId("n".into()),
                topic: "t".into(),
                payload: Bytes::from_static(b"v"),
                qos: 0,
                app: AppProperties::default(),
                seq,
                expires_at: None,
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
            match recv_peer_data(&mut peer).await {
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
        match recv_peer_data(&mut peer).await {
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
            expires_at: None,
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
                Some(PeerMessage::Interest { .. } | PeerMessage::RetainedDigest { .. }) => {}
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
            publisher: None,
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

    /// ADR 0043 P2: when the ring GROWS and a materialized offline session's group
    /// moves to the joiner, the old owner **releases** its local routing for it —
    /// observable as re-gossiped interest without the moved filter — instead of
    /// keeping stale interest that attracts forwards it can no longer durably
    /// enqueue (`NotOwner`). The durable data stays; the joiner's own scan
    /// materializes the session there.
    #[tokio::test(start_paused = true)]
    async fn growth_releases_moved_sessions_on_the_old_owner() {
        let local = NodeId("old-owner".into());
        let joiner = NodeId("joiner".into());
        // A client whose session the joiner owns once it enters the ring (and the
        // local node owns while alone — a single member owns everything).
        let mover = {
            let mut two = Placement::new(local.clone(), DEFAULT_REPLICAS);
            two.observe(&joiner, MemberState::Alive, "j:7000", None);
            (0..100_000)
                .map(|i| format!("mover-{i}"))
                .find(|c| two.owner(c) == joiner)
                .expect("some client moves to the joiner")
        };

        // The durable store already holds the persistent session + subscription
        // (as if it attached and disconnected before this hub's lifetime).
        let store = Arc::new(MemorySessionStore::new());
        let client = ClientId(mover.clone());
        store.ensure_session(&client).await.unwrap();
        store
            .set_subscriptions(
                &client,
                &[mqtt_core::Subscription {
                    filter: "mv/t".into(),
                    max_qos: QoS::AtLeastOnce,
                    no_local: false,
                    sub_id: None,
                }],
            )
            .await
            .unwrap();

        let placement = Arc::new(RwLock::new(Placement::new(local.clone(), DEFAULT_REPLICAS)));
        let (mut hub, tx) =
            Hub::with_config_and_placement(local.clone(), store, Some(placement.clone()));
        // The release path is durable-cluster-only: attach a real (idle) plane.
        let (_store, _retained, plane, driver) = mqtt_cluster::durable_node::build_durable_node(
            local.clone(),
            placement.clone(),
            false,
            5,
            &std::collections::BTreeMap::new(),
            None,
            None,
            false,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            None,
        )
        .await;
        driver.abort();
        hub.attach_durable_plane(plane);
        tokio::spawn(hub.run());
        let mut peer = connect_peer(&tx, "joiner", 1);

        // Boot scan: the owned offline session materializes and its filter gossips.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::Interest { filters }) if filters.contains(&"mv/t".into()) => {
                    break;
                }
                // A quiet 300ms (recv_peer's bound) or another frame: keep waiting.
                _ => assert!(
                    tokio::time::Instant::now() < deadline,
                    "the materialized session's filter was never gossiped"
                ),
            }
        }

        // The ring grows; the session's group now belongs to the joiner.
        placement
            .write()
            .unwrap()
            .observe(&joiner, MemberState::Alive, "j:7000", None);
        assert_eq!(placement.read().unwrap().owner(&mover), joiner);

        // The sweep observes the member-set change, re-arms the window, and the
        // next scan RELEASES the moved session: interest re-gossips without it.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            match recv_peer(&mut peer).await {
                Some(PeerMessage::Interest { filters }) if !filters.contains(&"mv/t".into()) => {
                    break;
                }
                _ => assert!(
                    tokio::time::Instant::now() < deadline,
                    "the moved session's filter was never released from gossiped interest"
                ),
            }
        }
    }

    /// Issue #305 — the PINNED narrowed promise: `Accepted` means the message was
    /// stored (or delivered) for **at least one** subscriber owed it, NOT for every
    /// one. With a co-subscribed filter — one LIVE subscriber and one durable
    /// session whose group's COMMITTED LEASE has moved with no membership change
    /// (the unmasked #294 window: nothing arms a settle hold) — the ack is released
    /// on the live co-subscriber's delivery once the reconcile scan releases the
    /// moved session's routing, while the moved session's copy is stored nowhere.
    /// The sole-subscriber form of the same window IS withheld (the `GroupGatedStore`
    /// tests below); the co-subscriber is what hides it, because
    /// `PendingPublish::stored` is one boolean, not a per-obligation ledger.
    ///
    /// This test asserts the CURRENT behaviour deliberately (issue #305 exit 2:
    /// state the narrowed claim — README, COMPARISON, OPERATIONS and the ADR 0041
    /// amendment carry it). If a per-obligation ledger ever strengthens the
    /// promise, this test FAILS and must be rewritten to assert the withhold —
    /// that divergence firing is its purpose.
    #[tokio::test]
    async fn a_co_subscribed_filter_releases_the_ack_while_a_moved_durable_copy_is_lost() {
        let local = NodeId("rehome-local".into());
        let remote = NodeId("rehome-remote".into());
        let placement = Arc::new(RwLock::new(Placement::new(local.clone(), DEFAULT_REPLICAS)));
        // The remote is a member from the START: the lease move below is the only
        // change during the test, so no membership-change settle window masks it.
        placement
            .write()
            .unwrap()
            .observe(&remote, MemberState::Alive, "r:7000", None);
        let store = GroupGatedStore::new(placement.clone());
        let (mut hub, tx) =
            Hub::with_config_and_placement(local.clone(), store.clone(), Some(placement.clone()));
        let (_store, _retained, plane, driver) = mqtt_cluster::durable_node::build_durable_node(
            local.clone(),
            placement.clone(),
            false,
            5,
            &std::collections::BTreeMap::new(),
            None,
            None,
            false,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            None,
        )
        .await;
        driver.abort();
        hub.attach_durable_plane(plane);
        tokio::spawn(hub.run());
        // Linked AND settled (a peer that never spoke holds every gated ack).
        let _peer = connect_peer(&tx, "rehome-remote", 1);
        peer_gossips_interest(&tx, &remote, &[]);
        await_routing_settled(&tx).await;

        // The durable co-subscriber: HRW-owned HERE, attaches persistent,
        // subscribes, and its CLIENT disconnects — offline, persistent, owned.
        let mover = (0..100_000)
            .map(|i| format!("co-mover-{i}"))
            .find(|c| placement.read().unwrap().owner(c) == local)
            .expect("some client is HRW-owned locally");
        let mover_id = ClientId(mover.clone());
        let (_mover_rx, _) = attach(&tx, &mover, 1, false).await;
        subscribe(&tx, &mover, "co/t");
        detach(&tx, &mover, 1);

        // The LIVE co-subscriber on the same filter, online throughout.
        let (mut live_rx, _) = attach(&tx, "co-live", 2, true).await;
        subscribe(&tx, "co-live", "co/t");

        // The unmasked trigger: the group's COMMITTED lease moves to the remote
        // (an assigner rebalance / lease-leader change). No member joins or
        // leaves, so nothing arms a settle window anywhere.
        commit_lease(&placement, &mover, Some(&remote));
        assert!(!placement.read().unwrap().owns(&mover));

        // Publish until the ack RELEASES. Pre-release, the fan-out still targets
        // the moved session here and its gated append refuses NotOwner — the
        // publisher is honestly withheld (done errors) or held briefly; once the
        // reconcile scan (~30 ticks) releases the routing, the ack rides the
        // live co-subscriber's delivery alone — the pinned gap.
        // Bounded by TIME, not attempts: pre-release withholds return instantly
        // (the honest NotOwner refusal drops the sender), so a fixed attempt
        // count burns out long before the ~30-tick reconcile scan releases the
        // routing. Each await is itself bounded for the held (not refused) shape.
        let mut accepted = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(100);
        while tokio::time::Instant::now() < deadline {
            let (done_tx, done_rx) = oneshot::channel();
            tx.send(HubCommand::Publish {
                topic: "co/t".into(),
                payload: Bytes::from_static(b"gap"),
                qos: QoS::AtLeastOnce,
                retain: false,
                message_expiry: None,
                app: mqtt_core::AppProperties::default(),
                done: Some(done_tx),
                v5: false,
                publisher: None,
            })
            .unwrap();
            // Anything else — withheld (Err), held past the bound, or a
            // non-Accepted outcome — is an honest pre-release answer: keep polling.
            if let Ok(Ok(PublishOutcome::Accepted)) =
                tokio::time::timeout(Duration::from_secs(45), done_rx).await
            {
                accepted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(
            accepted,
            "the ack must eventually release on the live co-subscriber's delivery \
             (the reconcile scan releases the moved session's routing)"
        );
        // The live co-subscriber genuinely got the publish — "at least one"
        // holds. (Earlier withheld attempts may also have delivered live
        // copies; `recv_packet` is itself bounded at 300ms, so this drain
        // terminates on quiet.)
        let mut delivered = false;
        while let Some(p) = recv_packet(&mut live_rx).await {
            if payload_of(&p) == b"gap" {
                delivered = true;
                break;
            }
        }
        assert!(delivered, "the live co-subscriber must receive the publish");
        // ... and the moved durable session's copy is stored NOWHERE, with the
        // publisher told Accepted: the co-subscribed gap, pinned.
        assert!(
            store
                .inner
                .pending(&mover_id, 0, 16)
                .await
                .unwrap()
                .is_empty(),
            "the moved durable session must hold no copy of the ACCEPTED message — \
             that absence, alongside the Accepted above, is the narrowed promise"
        );
    }

    /// Issue #294, the RED-FIRST cell: after `release_moved_sessions` drops a moved
    /// session's routing, a peer's forward for its filter used to be answered
    /// `Stored` — the origin then acks its publisher for a message stored NOWHERE.
    /// The `matched == 0 && routing_unsettled()` gate could not catch it because a
    /// pure lease move (no membership change: an assigner rebalance, a lease-leader
    /// change, a paced resize drain) armed no window anywhere.
    ///
    /// Now the sweep watches the committed ownership epoch and arms the SAME
    /// scan-paired window a membership change arms, so the release can only happen
    /// inside an armed window — and this forward, sent the moment the release is
    /// OBSERVED (the interest gossip losing the filter), is answered `Failed`
    /// ("cannot say, retry") rather than the lie. Before the fix this test observes
    /// `Stored` at the same point, deterministically: the release then happened on
    /// the slow reconcile with the view already settled.
    #[tokio::test]
    async fn a_forward_for_a_just_released_moved_session_is_not_answered_stored() {
        let local = NodeId("rehome-local".into());
        let remote = NodeId("rehome-remote".into());
        let placement = Arc::new(RwLock::new(Placement::new(local.clone(), DEFAULT_REPLICAS)));
        placement
            .write()
            .unwrap()
            .observe(&remote, MemberState::Alive, "r:7000", None);
        let store = GroupGatedStore::new(placement.clone());
        let (mut hub, tx) =
            Hub::with_config_and_placement(local.clone(), store.clone(), Some(placement.clone()));
        let (_store, _retained, plane, driver) = mqtt_cluster::durable_node::build_durable_node(
            local.clone(),
            placement.clone(),
            false,
            5,
            &std::collections::BTreeMap::new(),
            None,
            None,
            false,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            None,
        )
        .await;
        driver.abort();
        hub.attach_durable_plane(plane);
        tokio::spawn(hub.run());
        let mut peer = connect_peer(&tx, "rehome-remote", 1);
        peer_gossips_interest(&tx, &remote, &[]);
        await_routing_settled(&tx).await;

        // A persistent session, subscribed, whose client then disconnects — the
        // sole subscriber of its filter, still owned here.
        let mover = (0..100_000)
            .map(|i| format!("fw-mover-{i}"))
            .find(|c| placement.read().unwrap().owner(c) == local)
            .expect("some client is HRW-owned locally");
        let (_mover_rx, _) = attach(&tx, &mover, 1, false).await;
        subscribe(&tx, &mover, "fw/t");
        detach(&tx, &mover, 1);
        await_wire_interest(&mut peer, "fw/t", true).await;

        // The pure lease move: committed ownership goes to the remote, membership
        // untouched. Then wait until the RELEASE is observable on the wire — the
        // gossiped interest loses the filter (pre-fix: the ~30-tick reconcile;
        // post-fix: within the armed window's eager scans).
        commit_lease(&placement, &mover, Some(&remote));
        await_wire_interest(&mut peer, "fw/t", false).await;

        // A peer forward arriving RIGHT AFTER the release — from a node still
        // routing on the pre-release interest snapshot. Its verdict must never be
        // `Stored`: nothing here holds the message, and the session is still owed
        // it in the group's replicated log.
        tx.send(HubCommand::RemotePublishAcked {
            node: remote.clone(),
            seq: 7,
            topic: "fw/t".into(),
            payload: Bytes::from_static(b"owed"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties::default(),
        })
        .unwrap();
        // Read the verdict off the raw peer channel (`recv_peer` skips verdicts).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let verdict = loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "no PublishVerdict arrived for the forward"
            );
            match timeout(Duration::from_millis(300), peer.recv()).await {
                Ok(Some(PeerMessage::PublishVerdict { seq: 7, verdict })) => break verdict,
                Ok(Some(_)) | Err(_) => {}
                Ok(None) => panic!("peer channel closed before a verdict"),
            }
        };
        assert_ne!(
            verdict,
            mqtt_cluster::peer::ForwardVerdict::Stored,
            "a forward for a just-released moved session must not be answered Stored \
             — the session is owed the message and nothing anywhere holds it \
             (issue #294; Failed = 'cannot say, retry' is the honest answer)"
        );
    }

    // --- issue #284 / 0043-P6: rehome on settle -------------------------------

    /// A `SessionStore` that models the CLUSTERED durable store's group gate: every
    /// session **write** is refused with `StorageError::NotOwner` unless this node owns
    /// the client's placement group, exactly as `cluster_store::log_for_key` refuses it
    /// (`crates/mqtt-cluster/src/cluster_store.rs`, the `!placement.owns_group(group)`
    /// early return).
    ///
    /// Without this gate a unit fixture over a bare `MemorySessionStore` happily stores
    /// for a session whose group has moved away — which would hide the only thing that
    /// makes issue #284's hand-off observable (round-2 finding 1): a publish toward a
    /// rehomed session must fail its durable append and have its publisher's ack
    /// WITHHELD, not be acked for a message no node holds. Reads and the attach path are
    /// deliberately left ungated: on the real node those go through the same gate, but a
    /// unit fixture that refused them could not attach a session at all before moving
    /// its lease.
    #[derive(Debug)]
    struct GroupGatedStore {
        inner: MemorySessionStore,
        placement: Arc<RwLock<Placement>>,
    }

    impl GroupGatedStore {
        fn new(placement: Arc<RwLock<Placement>>) -> Arc<Self> {
            Arc::new(Self {
                inner: MemorySessionStore::new(),
                placement,
            })
        }

        /// The gate itself: `Placement::owns` is `group_owner(group_of(client)) == local`,
        /// i.e. the very predicate the replicated store fences writes on.
        fn owns(&self, client: &ClientId) -> bool {
            self.placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .owns(&client.0)
        }
    }

    #[async_trait::async_trait]
    impl mqtt_storage::SessionStore for GroupGatedStore {
        async fn ensure_session(
            &self,
            client: &ClientId,
        ) -> Result<bool, mqtt_storage::StorageError> {
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
            message: &Message,
            expiry_at: Option<u64>,
        ) -> Result<mqtt_storage::Enqueued, mqtt_storage::StorageError> {
            if !self.owns(client) {
                return Err(mqtt_storage::StorageError::NotOwner);
            }
            self.inner
                .enqueue_with_expiry(client, message, expiry_at)
                .await
        }

        async fn pending(
            &self,
            client: &ClientId,
            after: u64,
            limit: usize,
        ) -> Result<Vec<mqtt_storage::QueuedMessage>, mqtt_storage::StorageError> {
            self.inner.pending(client, after, limit).await
        }

        async fn ack(
            &self,
            client: &ClientId,
            up_to: u64,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.ack(client, up_to).await
        }

        async fn record_received(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<mqtt_storage::InboundSighting, mqtt_storage::StorageError> {
            self.inner.record_received(client, packet_id).await
        }

        async fn ack_received(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.ack_received(client, packet_id).await
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

        async fn record_outbound(
            &self,
            client: &ClientId,
            packet_id: u16,
            offset: u64,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.record_outbound(client, packet_id, offset).await
        }

        async fn advance_outbound(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.advance_outbound(client, packet_id).await
        }

        async fn clear_outbound(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.clear_outbound(client, packet_id).await
        }

        async fn outbound(
            &self,
            client: &ClientId,
        ) -> Result<Vec<mqtt_storage::OutboundInflight>, mqtt_storage::StorageError> {
            self.inner.outbound(client).await
        }

        async fn next_packet_id(
            &self,
            client: &ClientId,
        ) -> Result<u16, mqtt_storage::StorageError> {
            self.inner.next_packet_id(client).await
        }

        async fn reserve_packet_ids(
            &self,
            client: &ClientId,
            count: u16,
        ) -> Result<u16, mqtt_storage::StorageError> {
            self.inner.reserve_packet_ids(client, count).await
        }

        async fn remove(&self, client: &ClientId) -> Result<(), mqtt_storage::StorageError> {
            if !self.owns(client) {
                return Err(mqtt_storage::StorageError::NotOwner);
            }
            self.inner.remove(client).await
        }

        async fn set_session_expiry(
            &self,
            client: &ClientId,
            deadline: Option<u64>,
        ) -> Result<(), mqtt_storage::StorageError> {
            if !self.owns(client) {
                return Err(mqtt_storage::StorageError::NotOwner);
            }
            self.inner.set_session_expiry(client, deadline).await
        }

        async fn expiring_sessions(
            &self,
        ) -> Result<Vec<(ClientId, u64)>, mqtt_storage::StorageError> {
            self.inner.expiring_sessions().await
        }

        async fn all_sessions(
            &self,
        ) -> Result<mqtt_storage::SessionScan, mqtt_storage::StorageError> {
            self.inner.all_sessions().await
        }
    }

    /// A durable + clustered hub whose placement holds one remote peer and whose
    /// COMMITTED lease map the test controls — the rehome-on-settle fixture
    /// (issue #284). `remote_addr` empty makes the peer eligible with NO known
    /// peer-link address, which is the unrelocatable case.
    async fn start_rehome_hub(
        remote_addr: &str,
    ) -> (
        HubTx,
        Arc<RwLock<Placement>>,
        NodeId,
        Arc<mqtt_observability::metrics::Metrics>,
    ) {
        let (tx, placement, mut remotes, metrics) = start_rehome_hub_with(&[remote_addr]).await;
        (tx, placement, remotes.remove(0), metrics)
    }

    /// [`start_rehome_hub`] with one peer per address — two of them make this hub an
    /// ORIGIN that fans a publish out to a moved session's old node and its new owner at
    /// once, which is the third-node entry point neither of the earlier rounds tested.
    async fn start_rehome_hub_with(
        remote_addrs: &[&str],
    ) -> (
        HubTx,
        Arc<RwLock<Placement>>,
        Vec<NodeId>,
        Arc<mqtt_observability::metrics::Metrics>,
    ) {
        let local = NodeId("rehome-local".into());
        let placement = Arc::new(RwLock::new(Placement::new(local.clone(), DEFAULT_REPLICAS)));
        let remotes: Vec<NodeId> = (0..remote_addrs.len())
            .map(|i| {
                NodeId(if i == 0 {
                    "rehome-remote".to_string()
                } else {
                    format!("rehome-remote{}", i + 1)
                })
            })
            .collect();
        for (node, addr) in remotes.iter().zip(remote_addrs) {
            placement
                .write()
                .unwrap()
                .observe(node, MemberState::Alive, addr, None);
        }
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (mut hub, tx) = Hub::with_config_and_placement(
            local.clone(),
            // The group-gated store, not a bare memory one: a publish toward a session
            // whose group has moved away must FAIL its durable append here, exactly as
            // the replicated store fails it on the real node (round-2 finding 1).
            GroupGatedStore::new(placement.clone()),
            Some(placement.clone()),
        );
        // Rehome (like the release path it feeds) is durable-cluster-only: attach a
        // real, idle plane.
        let (_store, _retained, plane, driver) = mqtt_cluster::durable_node::build_durable_node(
            local.clone(),
            placement.clone(),
            false,
            5,
            &std::collections::BTreeMap::new(),
            None,
            None,
            false,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            None,
        )
        .await;
        driver.abort();
        hub.attach_durable_plane(plane);
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());
        (tx, placement, remotes, metrics)
    }

    /// Point `client`'s group's COMMITTED lease at `holder` (what the durable driver
    /// pushes each reconcile tick). An empty map restores pure-HRW routing.
    fn commit_lease(placement: &Arc<RwLock<Placement>>, client: &str, holder: Option<&NodeId>) {
        let mut owners = std::collections::BTreeMap::new();
        if let Some(h) = holder {
            owners.insert(mqtt_cluster::placement::group_of(client), h.clone());
        }
        placement.write().unwrap().set_lease_owners(owners);
    }

    /// Make the fixture's peer link count as SETTLED, the way a real peer does: gossip
    /// it an interest snapshot. A peer that has never spoken leaves `mesh_settled()`
    /// false, which holds EVERY gated ack — so without this an ack-honesty assertion
    /// could not tell issue #284's hand-off withhold from the mesh gate's.
    fn peer_gossips_interest(tx: &HubTx, node: &NodeId, filters: &[&str]) {
        tx.send(HubCommand::RemoteInterest {
            node: node.clone(),
            filters: filters.iter().map(|f| (*f).to_string()).collect(),
        })
        .unwrap();
    }

    /// Wait until `filter` is (or is not) in a gossiped `Interest` snapshot.
    async fn await_wire_interest(
        peer: &mut mpsc::UnboundedReceiver<PeerMessage>,
        filter: &str,
        present: bool,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            match recv_peer(peer).await {
                Some(PeerMessage::Interest { filters })
                    if filters.contains(&filter.to_string()) == present =>
                {
                    return;
                }
                _ => assert!(
                    tokio::time::Instant::now() < deadline,
                    "gossiped interest never reported {filter} present={present}"
                ),
            }
        }
    }

    /// Block until this hub's ROUTING VIEW reports settled, probed exactly the way the
    /// production ack gate reads it: a gated `QoS` 1 publish nobody subscribes to is
    /// released only once `routing_unsettled()` is false (`awaiting_settle`). A fresh hub
    /// arms an 8-tick boot window and re-arms it on the first membership observation, so
    /// this takes several seconds — and every ack-honesty assertion is meaningless before
    /// it, because the ack would be held by the pre-existing gate rather than by the
    /// behaviour under test.
    async fn await_routing_settled(tx: &HubTx) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            let probe = publish_gated(tx, "settle/probe/284", b"p", QoS::AtLeastOnce, true);
            if let Ok(Ok(PublishOutcome::Accepted)) =
                timeout(super::SESSION_SWEEP_INTERVAL, probe).await
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the hub's routing view never settled"
            );
        }
    }

    /// Wait for the rehome close (`0x9C` Use another server) on a v5 client's socket.
    async fn await_rehome_disconnect(out: &mut mpsc::UnboundedReceiver<Packet>) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            match recv_packet(out).await {
                Some(Packet::Disconnect(d)) => {
                    assert_eq!(
                        d.reason,
                        mqtt_codec::reason::USE_ANOTHER_SERVER,
                        "a rehomed v5 client is told to use another server"
                    );
                    return;
                }
                other => assert!(
                    tokio::time::Instant::now() < deadline,
                    "no rehome DISCONNECT; got {other:?}"
                ),
            }
        }
    }

    /// Attach a persistent **v5** session (so it can be told WHY it is closed).
    async fn attach_persistent_v5(
        tx: &HubTx,
        client: &str,
        conn_id: u64,
    ) -> mpsc::UnboundedReceiver<Packet> {
        attach_persistent_v5_full(tx, client, conn_id, u32::MAX, None).await
    }

    /// [`attach_persistent_v5`] with an explicit Session Expiry Interval and Will — the
    /// two session properties the rehome close has to answer for (ADR 0009 §3's persisted
    /// deadline, and [MQTT-3.1.2-8]'s will).
    async fn attach_persistent_v5_full(
        tx: &HubTx,
        client: &str,
        conn_id: u64,
        session_expiry: u32,
        will: Option<Will>,
    ) -> mpsc::UnboundedReceiver<Packet> {
        let (out_tx, out_rx) = {
            let (t, r) = mpsc::unbounded_channel();
            (Outbound::new(t).0, r)
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(HubCommand::Attach {
            client: ClientId(client.into()),
            admission: Admission {
                identity: mqtt_auth::Identity {
                    subject: client.to_string(),
                    groups: vec![],
                },
                method: AuthMethod::Password,
                cert_serial: None,
                protocol: ProtocolVersion::V5,
            },
            conn_id,
            clean_start: false,
            session_expiry,
            receive_maximum: u16::MAX,
            will,
            outbound: out_tx,
            reply: reply_tx,
        })
        .unwrap();
        reply_rx.await.unwrap();
        out_rx
    }

    /// Issue #284: an ONLINE persistent session whose group's committed lease has
    /// moved to another node is CLOSED, so the client relocates to the real owner on
    /// its next CONNECT — the wedge otherwise persists until the client's keepalive
    /// notices dead air (measured unbounded; still wedged after two minutes).
    ///
    /// A v5 client is told why (`0x9C` Use another server) and the session leaves
    /// `online`. The close does NOTHING ELSE: the session's filter is still advertised
    /// afterwards and a publish toward it is still WITHHELD, which is this fix's whole
    /// honesty story in one assertion. Releasing the routing at the close instead — the
    /// first cut — acked that publish with nothing stored anywhere; the pre-existing
    /// [`Hub::release_moved_sessions`] takes the session on its own scan cadence, paired
    /// with the only thing that clears a held ack.
    #[tokio::test]
    async fn an_online_session_whose_group_moved_is_closed_so_it_relocates() {
        let (tx, placement, remote, metrics) = start_rehome_hub("r:7000").await;
        let client = "mover-284";

        // The session is here and owned here: the committed lease names this node.
        commit_lease(&placement, client, None);
        let mut out = attach_persistent_v5(&tx, client, 1).await;
        subscribe_qos(&tx, client, "mv/284", QoS::AtLeastOnce);
        let mut peer = connect_peer(&tx, "rehome-remote", 1);
        // The owner-to-be is linked and has spoken, but claims nothing yet.
        peer_gossips_interest(&tx, &remote, &[]);

        // Its filter is gossiped, so the release below is observable as its removal.
        await_wire_interest(&mut peer, "mv/284", true).await;
        await_routing_settled(&tx).await;

        // Ownership settles elsewhere while the client stays connected — the roll
        // aftermath: the readmitted node takes its groups' leases back.
        commit_lease(&placement, client, Some(&remote));

        // The client is told to use another server, and the connection closes.
        await_rehome_disconnect(&mut out).await;
        assert!(
            recv_packet(&mut out).await.is_none(),
            "the rehomed connection must be closed"
        );
        let rendered = metrics.render();
        assert!(
            rendered.contains("mqttd_session_rehomes_total{reason=\"stale-owner\"} 1"),
            "{rendered}"
        );

        // ...and NOTHING else changed. The filter is still advertised (so the cluster can
        // still see that the message is owed), and the publisher's ack is WITHHELD rather
        // than given for a message this node cannot append and no other node claims yet.
        // A negative-in-time assertion on the gossip would prove nothing, so the routing
        // is probed the way it matters: by the answer a publisher gets.
        let held = publish_gated(&tx, "mv/284", b"held", QoS::AtLeastOnce, true);
        let got = timeout(super::SESSION_SWEEP_INTERVAL * 3, held).await;
        assert!(
            matches!(got, Err(_) | Ok(Err(_))),
            "after a rehome close this node still routes the session, so the publisher's \
             ack must be WITHHELD, got {got:?}"
        );
        // Since issue #294 the lease move arms the eager window, so the release —
        // and its shrunken gossip — may land within these same ticks. That is the
        // NEW honest shape: the shrink happens only inside an armed window, where
        // a zero-match forward is answered `Failed` rather than `Stored` (pinned
        // by `a_forward_for_a_just_released_moved_session_is_not_answered_stored`)
        // and local acks stay held, as the probe above just proved. The old pin
        // here ("no shrunken re-gossip after the close") asserted the pre-#294
        // mitigation — keep routing so the withhold happens via `NotOwner` — which
        // the covered window supersedes.
    }

    /// Issue #284: the grace. A committed lease that lands elsewhere for a single
    /// sweep tick and comes straight back (assigner rebalance, leader change) must
    /// NOT cost a live client its connection — that churn is ordinary convergence,
    /// not a session placed across an ownership move.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn a_transient_ownership_blip_does_not_close_a_live_session() {
        let (tx, placement, remote, metrics) = start_rehome_hub("r:7000").await;
        let client = "blip-284";

        commit_lease(&placement, client, None);
        let mut out = attach_persistent_v5(&tx, client, 1).await;
        // Settle: the session is online and owned here for a couple of ticks.
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 2).await;

        // Elsewhere and back inside one sweep interval: at most a single tick can
        // observe it, which is strictly less than the grace.
        commit_lease(&placement, client, Some(&remote));
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL / 3).await;
        commit_lease(&placement, client, None);

        // Well past the grace: nothing happened to the connection.
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 4).await;
        assert!(
            recv_packet(&mut out).await.is_none(),
            "a sub-tick ownership blip must not disturb a live session"
        );
        let rendered = metrics.render();
        assert!(
            !rendered.contains("mqttd_session_rehomes_total{"),
            "no rehome of any reason: {rendered}"
        );
        assert!(
            rendered.contains("mqttd_misplaced_sessions 0"),
            "the grace entry must be dropped, not merely reset: {rendered}"
        );
    }

    /// Issue #284: a group with NO committed lease is never rehomed. With the lease
    /// map empty the ring falls back to the desired (HRW) owner, which can name
    /// another node while the cluster has committed nothing — the transient
    /// ring/lease split the 2026-07-20 post-mortem describes. The data path is right
    /// to fail closed on it; DISRUPTING a live client on it would close healthy
    /// sessions during ordinary convergence.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn a_group_with_no_committed_lease_is_never_rehomed() {
        let (tx, placement, remote, metrics) = start_rehome_hub("r:7000").await;
        // A client the HRW ring hands to the remote node, with no lease anywhere.
        let client = {
            let p = placement.read().unwrap();
            (0..100_000)
                .map(|i| format!("hrw-284-{i}"))
                .find(|c| p.owner(c) == remote)
                .expect("some client's desired owner is the remote node")
        };
        commit_lease(&placement, &client, None);
        assert!(
            placement
                .read()
                .unwrap()
                .committed_session_owner(&client)
                .is_none(),
            "the fixture must have no committed lease for the group"
        );

        let mut out = attach_persistent_v5(&tx, &client, 1).await;
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 5).await;

        assert!(
            recv_packet(&mut out).await.is_none(),
            "an unsettled (lease-less) group must not close a live session"
        );
        let rendered = metrics.render();
        assert!(
            !rendered.contains("mqttd_session_rehomes_total{"),
            "no rehome of any reason: {rendered}"
        );
    }

    /// Issue #284: an owner this node cannot route to is COUNTED, not closed. With
    /// the owner's peer-link address unknown, the client's next CONNECT would be
    /// served locally again (ADR 0005 §5 degrade-don't-refuse) — closing it would be
    /// an unbounded close/reconnect loop. The session stays (still undeliverable),
    /// and says so through `session_rehomes{reason="unrelocatable"}` and the
    /// misplaced-sessions gauge.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn an_unrelocatable_owner_is_counted_not_closed() {
        // Eligible remote member, no address learned.
        let (tx, placement, remote, metrics) = start_rehome_hub("").await;
        let client = "stranded-284";

        commit_lease(&placement, client, None);
        let mut out = attach_persistent_v5(&tx, client, 1).await;
        commit_lease(&placement, client, Some(&remote));
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 5).await;

        assert!(
            recv_packet(&mut out).await.is_none(),
            "a session whose owner has no known address must be kept, not closed"
        );
        let rendered = metrics.render();
        assert!(
            rendered.contains("mqttd_session_rehomes_total{reason=\"unrelocatable\"} 1"),
            "counted exactly once per episode: {rendered}"
        );
        assert!(
            !rendered.contains("reason=\"stale-owner\""),
            "nothing was closed: {rendered}"
        );
        assert!(
            rendered.contains("mqttd_misplaced_sessions 1"),
            "the standing wedge must be visible on the gauge: {rendered}"
        );
    }

    /// Issue #284: the cooldown. A client that comes straight back to the SAME
    /// non-owning node (a flapping placement, or a load balancer that keeps
    /// returning it there) is not closed again immediately — otherwise the fix is a
    /// close loop, and a will-publish loop with it. The suppressed repeat is
    /// counted so a standing flap is loud rather than silent.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn a_rehomed_session_is_not_closed_again_within_the_cooldown() {
        let (tx, placement, remote, metrics) = start_rehome_hub("r:7000").await;
        let client = "flapper-284";

        commit_lease(&placement, client, None);
        let mut first = attach_persistent_v5(&tx, client, 1).await;
        commit_lease(&placement, client, Some(&remote));

        // First close: the ordinary rehome.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            match recv_packet(&mut first).await {
                Some(Packet::Disconnect(_)) => break,
                other => assert!(
                    tokio::time::Instant::now() < deadline,
                    "no first rehome DISCONNECT; got {other:?}"
                ),
            }
        }

        // The client lands right back here, still not the owner.
        let mut second = attach_persistent_v5(&tx, client, 2).await;
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 5).await;
        assert!(
            recv_packet(&mut second).await.is_none(),
            "a second close inside the cooldown would be a close loop"
        );
        let rendered = metrics.render();
        assert!(
            rendered.contains("mqttd_session_rehomes_total{reason=\"stale-owner\"} 1"),
            "exactly one close so far: {rendered}"
        );
        assert!(
            rendered.contains("mqttd_session_rehomes_total{reason=\"cooldown\"} 1"),
            "the suppressed repeat must be counted: {rendered}"
        );
    }

    /// Where a publish toward a rehomed session ENTERS this node — the enumeration axis
    /// that killed both earlier rounds of issue #284 (each sampled one entry point and
    /// missed a case).
    #[derive(Debug, Clone, Copy)]
    enum RehomeEntry {
        /// A client publishing here: the local gate, whose answer is the publisher's
        /// `PublishOutcome` (or its absence — a withhold).
        Local,
        /// A peer forwarding here off its interest snapshot: the answer is a
        /// `PublishVerdict` on the peer link.
        PeerForward,
    }

    /// What the committed OWNER is advertising while the probe runs. Filter-level interest
    /// is all a peer publishes (`PeerMessage::Interest` carries filters, not client ids),
    /// so "present" is also what a co-subscriber of the same filter already living on the
    /// owner looks like — and "present then withdrawn" is that co-subscriber going away,
    /// the shape that survived round 2's evidence check and produced an acked-but-dropped
    /// publish.
    #[derive(Debug, Clone, Copy)]
    enum OwnerInterest {
        Absent,
        Present,
        PresentThenWithdrawn,
    }

    /// Issue #284 round 3 — **the ack-honesty invariant, enumerated rather than sampled.**
    ///
    /// The rehome close ends the connection and touches nothing else, so from the close
    /// until the pre-existing `release_moved_sessions` scan takes it, this node still
    /// routes the moved session and still advertises its filters. That is what keeps every
    /// publish toward it honest: locally the durable append fails `NotOwner` and the
    /// publisher's ack is WITHHELD; as a peer's forward it is answered `Failed`, which is a
    /// withhold at the origin too. No cell may answer `Accepted` or `Stored`.
    ///
    /// Rounds 1 and 2 each closed the named hole and opened one in an adjacent mechanism,
    /// and each time the missed case was an entry point or a moment the test did not visit.
    /// So this test is a table: {local, peer-forward} x {owner interest absent, present,
    /// present-then-withdrawn} x {before the close, after the close}. The third-node entry
    /// point — where the owner's `Stored` must compose with this node's `Failed` into a
    /// withhold — cannot be posed on the rehoming node itself and is the separate
    /// [`a_third_node_composes_a_refusal_and_a_store_into_a_withhold`] test.
    #[tokio::test]
    async fn no_publish_toward_a_rehomed_session_is_ever_acked_while_this_node_routes_it() {
        let (tx, placement, remote, _metrics) = start_rehome_hub("r:7000").await;
        let mut peer = connect_peer(&tx, "rehome-remote", 1);
        peer_gossips_interest(&tx, &remote, &[]);
        await_routing_settled(&tx).await;
        let mut seq = 1_000u64;
        // THE CONTROL, without which every cell below could be passing for the wrong
        // reason. A forward that matches nothing here is answered `Stored` only while
        // `routing_unsettled()` is false, so this proves the routing view is settled — and
        // therefore that every `Failed` below comes from the session's append being refused
        // `NotOwner`, not from a pre-existing honesty gate.
        assert_eq!(
            forward_verdict_for(&tx, &mut peer, &remote, "nobody/284", &mut seq).await,
            ForwardVerdict::Stored,
            "the fixture's routing view must be settled before any cell runs, or a \
             withheld ack proves nothing about the rehome"
        );

        for (i, interest) in [
            OwnerInterest::Absent,
            OwnerInterest::Present,
            OwnerInterest::PresentThenWithdrawn,
        ]
        .into_iter()
        .enumerate()
        {
            let client = format!("enum-284-{i}");
            let filter = format!("enum/{i}/284");
            commit_lease(&placement, &client, None);
            let mut out = attach_persistent_v5(&tx, &client, 700 + i as u64).await;
            subscribe_qos(&tx, &client, &filter, QoS::AtLeastOnce);
            await_wire_interest(&mut peer, &filter, true).await;

            // Put the owner's advertised interest where this cell wants it. `Present` and
            // `PresentThenWithdrawn` differ only in the second frame, which is the whole
            // point: a claim that is observed and then retracted must not have discharged
            // anything.
            match interest {
                OwnerInterest::Absent => peer_gossips_interest(&tx, &remote, &[]),
                OwnerInterest::Present | OwnerInterest::PresentThenWithdrawn => {
                    peer_gossips_interest(&tx, &remote, &[&filter]);
                }
            }

            // Ownership moves away. The close needs MISPLACED_GRACE_TICKS of observation,
            // so the BEFORE-the-close cells run in the window that opens right here: the
            // session is still online and routed, and its append already fails `NotOwner`.
            commit_lease(&placement, &client, Some(&remote));
            // Pins the phase: the earliest possible close is a sweep tick away
            // (MISPLACED_GRACE_TICKS of continuous observation), so these two cells really
            // do run with the session still ONLINE. Their answers are immediate — the
            // group-gated append refuses synchronously — so they land well inside the grace.
            assert!(
                out.try_recv().is_err(),
                "the before-the-close cells must run before the close"
            );
            for entry in [RehomeEntry::Local, RehomeEntry::PeerForward] {
                assert_publish_is_withheld(
                    &tx,
                    &mut peer,
                    &remote,
                    &filter,
                    entry,
                    &mut seq,
                    &format!("{interest:?}/{entry:?}/before-the-close"),
                )
                .await;
            }

            // The close itself, and then the same probes on the other side of it.
            await_rehome_disconnect(&mut out).await;
            if matches!(interest, OwnerInterest::PresentThenWithdrawn) {
                // The owner's advertised interest disappears while the moved session is
                // still unmaterialised there — round 2's proven acked-but-dropped shape.
                peer_gossips_interest(&tx, &remote, &[]);
            }
            for entry in [RehomeEntry::Local, RehomeEntry::PeerForward] {
                assert_publish_is_withheld(
                    &tx,
                    &mut peer,
                    &remote,
                    &filter,
                    entry,
                    &mut seq,
                    &format!("{interest:?}/{entry:?}/after-the-close"),
                )
                .await;
            }
        }

        // The control, updated for issue #294: the lease move DOES arm the eager
        // window now — deliberately, scan-paired — so a zero-match forward during
        // it is answered `Failed` (retry). The pin that must survive is the round-2
        // hazard's actual teeth: the window DRAINS promptly (each armed tick runs
        // the scan that settles), so within a bounded few seconds the same forward
        // is answered `Stored` again. An unpaired armer — the round-2 blocking #1
        // shape, stalling every gated ack for ~30 s — fails this bound.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let v = forward_verdict_for(&tx, &mut peer, &remote, "nobody/284", &mut seq).await;
            if v == ForwardVerdict::Stored {
                break;
            }
            assert!(
                matches!(v, ForwardVerdict::Failed),
                "a zero-match forward during the armed window may only be Failed \
                 (cannot-say-retry), got {v:?}"
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "the lease-move window never drained: zero-match forwards were still \
                 not answered Stored after 15 s — the unpaired-armer stall shape"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Feed one peer forward for `topic` and read this node's verdict for it.
    async fn forward_verdict_for(
        tx: &HubTx,
        peer: &mut mpsc::UnboundedReceiver<PeerMessage>,
        remote: &NodeId,
        topic: &str,
        seq: &mut u64,
    ) -> ForwardVerdict {
        *seq += 1;
        let this = *seq;
        tx.send(HubCommand::RemotePublishAcked {
            node: remote.clone(),
            seq: this,
            topic: topic.into(),
            payload: Bytes::from_static(b"probe"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties::default(),
        })
        .unwrap();
        next_verdict(peer, this).await
    }

    /// One cell of [`no_publish_toward_a_rehomed_session_is_ever_acked_while_this_node_routes_it`]:
    /// publish toward `filter` through `entry` and require the ack to be WITHHELD.
    async fn assert_publish_is_withheld(
        tx: &HubTx,
        peer: &mut mpsc::UnboundedReceiver<PeerMessage>,
        remote: &NodeId,
        filter: &str,
        entry: RehomeEntry,
        seq: &mut u64,
        cell: &str,
    ) {
        match entry {
            RehomeEntry::Local => {
                let out = publish_gated(tx, filter, b"owed", QoS::AtLeastOnce, true);
                let got = timeout(Duration::from_millis(600), out).await;
                assert!(
                    matches!(got, Err(_) | Ok(Err(_))),
                    "[{cell}] a publish toward a session this node still routes must have \
                     its ack WITHHELD, never Accepted; got {got:?}"
                );
            }
            RehomeEntry::PeerForward => {
                assert_eq!(
                    forward_verdict_for(tx, peer, remote, filter, seq).await,
                    ForwardVerdict::Failed,
                    "[{cell}] a peer's forward toward a session this node still routes must \
                     be answered Failed (which withholds at the origin), never Stored"
                );
            }
        }
    }

    /// The verdict this node sent for one forward `seq`, skipping the frames it sends as
    /// an ORIGIN (its own forwards and interest gossip) which share the channel.
    async fn next_verdict(
        rx: &mut mpsc::UnboundedReceiver<PeerMessage>,
        seq: u64,
    ) -> ForwardVerdict {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match timeout(Duration::from_millis(300), rx.recv()).await {
                Ok(Some(PeerMessage::PublishVerdict { seq: s, verdict })) if s == seq => {
                    return verdict;
                }
                Ok(Some(PeerMessage::PublishAck { seq: s, ok })) if s == seq => {
                    return if ok {
                        ForwardVerdict::Stored
                    } else {
                        ForwardVerdict::Failed
                    };
                }
                _ => assert!(
                    tokio::time::Instant::now() < deadline,
                    "no answer for forward seq {seq}"
                ),
            }
        }
    }

    /// Issue #284 round 3 — **the composition rule the whole honesty story rests on**, and
    /// the entry point neither earlier round tested: a THIRD node.
    ///
    /// While a rehomed session's old node still routes it, both that node and the session's
    /// new owner advertise its filters, so a publisher entering at some third node fans out
    /// to both. The owner may answer `Stored` (it materialised the session) while the old
    /// node answers `Failed` (its append is refused `NotOwner`). The publisher must be left
    /// UNACKED either way round: `forward_answered` is first-terminal-verdict-wins, and
    /// `try_complete_pending` additionally requires every obligation to have resolved, so an
    /// early `Stored` cannot release the ack while the old node's obligation is outstanding.
    ///
    /// Round 2 asserted this composition in prose ("ours fails `NotOwner` and withholds,
    /// the publisher retries") and nothing tested it. Nothing else in this suite pins it.
    #[tokio::test]
    async fn a_third_node_composes_a_refusal_and_a_store_into_a_withhold() {
        let (tx, _placement, remotes, _metrics) =
            start_rehome_hub_with(&["owner:7000", "old:7000"]).await;
        let (owner, old) = (remotes[0].clone(), remotes[1].clone());
        let mut owner_link = connect_peer(&tx, &owner.0, 1);
        let mut old_link = connect_peer(&tx, &old.0, 2);
        // BOTH advertise the moved session's filter — the double-advertise window a rehome
        // close leaves open until the old node's own scan releases its routing.
        peer_gossips_interest(&tx, &owner, &["t/284"]);
        peer_gossips_interest(&tx, &old, &["t/284"]);
        await_routing_settled(&tx).await;

        // Both orders, because the composition must not depend on which answer lands first.
        for (first_stored, label) in [(true, "stored-then-failed"), (false, "failed-then-stored")] {
            let publisher = publish_gated(&tx, "t/284", b"owed", QoS::AtLeastOnce, true);
            let owner_seq = next_forward_seq(&mut owner_link, "t/284").await;
            let old_seq = next_forward_seq(&mut old_link, "t/284").await;

            let answers = if first_stored {
                [
                    (owner.clone(), owner_seq, ForwardVerdict::Stored),
                    (old.clone(), old_seq, ForwardVerdict::Failed),
                ]
            } else {
                [
                    (old.clone(), old_seq, ForwardVerdict::Failed),
                    (owner.clone(), owner_seq, ForwardVerdict::Stored),
                ]
            };
            for (node, seq, verdict) in answers {
                tx.send(HubCommand::RemotePublishVerdict { node, seq, verdict })
                    .unwrap();
            }

            let got = timeout(Duration::from_secs(5), publisher).await;
            assert!(
                matches!(got, Ok(Err(_))),
                "[{label}] one node storing does not discharge the other node's \
                 outstanding obligation: the publisher's outcome sender must be DROPPED \
                 (the withhold), never resolved to Accepted, got {got:?}"
            );
        }
    }

    /// The `seq` this node used for the forward of `topic` to one peer.
    async fn next_forward_seq(rx: &mut mpsc::UnboundedReceiver<PeerMessage>, topic: &str) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match timeout(Duration::from_millis(300), rx.recv()).await {
                Ok(Some(PeerMessage::PublishAcked { seq, topic: t, .. })) if t == topic => {
                    return seq;
                }
                _ => assert!(
                    tokio::time::Instant::now() < deadline,
                    "no acked forward of {topic} reached this peer"
                ),
            }
        }
    }

    /// Issue #284, round-2 finding 2 — **the Will fires on a rehome close, deliberately**,
    /// and the decision is locked here so a future suppression has to break a test rather
    /// than slip through.
    ///
    /// A server DISCONNECT is not a client DISCONNECT: [MQTT-3.1.2-8] / §3.14.4 delete the
    /// will only on a client DISCONNECT with reason `0x00`. Suppressing it would also make
    /// the rehome the ONLY broker-initiated close that hides a will — session takeover and
    /// `evict` both publish it, and issue #265 existed because broker-initiated closes were
    /// silently NOT publishing it.
    ///
    /// The control in the same test is the anti-overcorrection partner: a client's own
    /// clean DISCONNECT must still suppress its will. "Always fire" is as wrong as "never
    /// fire".
    #[tokio::test]
    async fn a_rehome_close_publishes_the_clients_will() {
        let (tx, placement, remote, _metrics) = start_rehome_hub("r:7000").await;
        let client = "willy-284";

        // A bystander watching the will topic — the fleet's device-offline dashboard.
        let mut watcher = attach_persistent_v5(&tx, "watcher-284", 9).await;
        subscribe(&tx, "watcher-284", "wills/284");

        commit_lease(&placement, client, None);
        let mut out = attach_persistent_v5_full(
            &tx,
            client,
            1,
            u32::MAX,
            Some(Will {
                delay_secs: 0,
                message: Message {
                    topic: "wills/284".into(),
                    payload: Bytes::from_static(b"offline"),
                    qos: QoS::AtMostOnce,
                    retain: false,
                    app: mqtt_core::AppProperties::default(),
                    expires_at: None,
                },
            }),
        )
        .await;

        // The lease moves; the rehome closes the connection.
        commit_lease(&placement, client, Some(&remote));
        await_rehome_disconnect(&mut out).await;

        assert_eq!(
            payload_of(
                &recv_packet(&mut watcher)
                    .await
                    .expect("a rehome close must publish the client's Last Will")
            ),
            b"offline",
            "a server DISCONNECT (0x9C) does not delete the will [MQTT-3.1.2-8]; the \
             rehome is consistent with takeover and evict (issue #265)"
        );

        // The control: a client's OWN clean DISCONNECT still suppresses its will.
        let control = "control-284";
        commit_lease(&placement, control, None);
        let _ctl = attach_persistent_v5_full(
            &tx,
            control,
            2,
            u32::MAX,
            Some(Will {
                delay_secs: 0,
                message: Message {
                    topic: "wills/284".into(),
                    payload: Bytes::from_static(b"should-not-fire"),
                    qos: QoS::AtMostOnce,
                    retain: false,
                    app: mqtt_core::AppProperties::default(),
                    expires_at: None,
                },
            }),
        )
        .await;
        detach(&tx, control, 2);
        assert!(
            recv_packet(&mut watcher).await.is_none(),
            "a clean client DISCONNECT must still delete the will — 'always fire' is as \
             wrong as 'never fire'"
        );
    }

    /// Issue #284, round-2 finding 4 — a mass ownership move is PACED. The rehome is the
    /// live mirror of elastic resize, where ~1/N of groups change owner in one step; with
    /// no aggregate bound one sweep tick closed every affected session at once, published
    /// one Last Will per close in the same breath, and sent them all to reconnect on the
    /// same instant. Over the cap the remainder is deferred to later ticks and counted.
    #[tokio::test]
    async fn one_tick_closes_at_most_the_per_tick_cap_and_defers_the_rest() {
        let (tx, placement, remote, metrics) = start_rehome_hub("r:7000").await;
        let n = super::REHOME_CLOSES_PER_TICK + 8;

        // Every session's group lease points at the remote node at once.
        let mut owners = std::collections::BTreeMap::new();
        let clients: Vec<String> = (0..n).map(|i| format!("mass-284-{i}")).collect();
        let mut outs = Vec::new();
        for (i, client) in clients.iter().enumerate() {
            outs.push(attach_persistent_v5(&tx, client, 100 + i as u64).await);
        }
        for client in &clients {
            owners.insert(mqtt_cluster::placement::group_of(client), remote.clone());
        }
        placement.write().unwrap().set_lease_owners(owners);

        // Wait for the first tick that closes anything, then read the cap off the metrics
        // before the next tick can add to it.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let rendered = metrics.render();
            if let Some(deferred) = counter_value(&rendered, "reason=\"deferred\"") {
                let closed = counter_value(&rendered, "reason=\"stale-owner\"").unwrap_or(0);
                assert!(
                    closed <= super::REHOME_CLOSES_PER_TICK as u64,
                    "one tick must not close more than the cap; closed {closed}, \
                     cap {}: {rendered}",
                    super::REHOME_CLOSES_PER_TICK
                );
                assert!(deferred > 0, "the remainder must be counted as deferred");
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the close cap never engaged: {}",
                metrics.render()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // ...and the deferral is a delay, not a drop: every session is closed eventually.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            let rendered = metrics.render();
            if counter_value(&rendered, "reason=\"stale-owner\"") == Some(n as u64) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the deferred sessions were never closed: {rendered}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(outs);
    }

    /// Issue #284 round 3 — `reason="deferred"` counts each SESSION once, not each
    /// deferral EVENT.
    ///
    /// The pass re-derives its candidates every tick, so a session over the cap is
    /// re-deferred on every tick until it is closed. Counting the events makes an
    /// n-session move report ~n²/(2·cap) samples — for the 1700-session resize this cap
    /// exists for, ~45 000 — and an operator sizing a drain from
    /// `increase(session_rehomes_total{reason="deferred"}[5m])` overestimates the backlog
    /// by more than an order of magnitude, with the error silently depending on the cap.
    /// With `n = 3·cap + 8` the two readings are far apart: 3 deferral ticks give
    /// `3·cap+8 - cap = 2·cap+8` distinct sessions but `(2·cap+8) + (cap+8) + 8` events.
    #[tokio::test]
    async fn a_mass_move_counts_each_deferred_session_once() {
        let (tx, placement, remote, metrics) = start_rehome_hub("r:7000").await;
        let cap = super::REHOME_CLOSES_PER_TICK;
        let n = cap * 3 + 8;

        let clients: Vec<String> = (0..n).map(|i| format!("once-284-{i}")).collect();
        let mut outs = Vec::new();
        for (i, client) in clients.iter().enumerate() {
            outs.push(attach_persistent_v5(&tx, client, 500 + i as u64).await);
        }
        // Every session's group lease moves in the same push — resize's ~1/N-of-groups
        // step, which is what this cap and this counter exist for.
        let mut owners = std::collections::BTreeMap::new();
        for client in &clients {
            owners.insert(mqtt_cluster::placement::group_of(client), remote.clone());
        }
        placement.write().unwrap().set_lease_owners(owners);

        // Let the whole drain finish, so the counter is read at its final value.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            let rendered = metrics.render();
            if counter_value(&rendered, "reason=\"stale-owner\"") == Some(n as u64) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the paced drain never finished: {rendered}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let rendered = metrics.render();
        let deferred = counter_value(&rendered, "reason=\"deferred\"")
            .expect("a mass move over the cap must count deferrals");
        assert!(
            deferred > 0,
            "the remainder over the cap must be visible: {rendered}"
        );
        assert!(
            deferred <= (n - cap) as u64,
            "reason=\"deferred\" must count each SESSION once per deferral episode, not \
             each per-tick deferral event: at most {} sessions could be deferred, the \
             counter says {deferred}: {rendered}",
            n - cap
        );
        drop(outs);
    }

    /// Read one `mqttd_session_rehomes_total{...}` sample out of a rendered registry.
    fn counter_value(rendered: &str, label: &str) -> Option<u64> {
        rendered
            .lines()
            .find(|l| l.starts_with("mqttd_session_rehomes_total{") && l.contains(label))
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
    }

    /// Issue #284, round-2 finding 5 — a rehomed session's ADR 0009 §3 expiry deadline
    /// cannot be persisted from a non-owner (the write is group-routed and this node is by
    /// construction not the owner), so the attempt is SKIPPED deliberately and counted
    /// instead of being attempted with its error discarded. The residual — the new owner
    /// inherits a session record with no deadline until the client reconnects — is named in
    /// ADR 0009's as-delivered note; what this test pins is that it is never silent.
    #[tokio::test]
    async fn a_rehomed_finite_expiry_session_reports_its_unpersisted_deadline() {
        let (tx, placement, remote, metrics) = start_rehome_hub("r:7000").await;
        let client = "expiring-284";

        commit_lease(&placement, client, None);
        // A FINITE Session Expiry Interval: this is the only case with a deadline to lose.
        let mut out = attach_persistent_v5_full(&tx, client, 1, 3600, None).await;
        commit_lease(&placement, client, Some(&remote));
        await_rehome_disconnect(&mut out).await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let rendered = metrics.render();
            if rendered.contains("mqttd_session_expiry_unpersisted_total{reason=\"not-owner\"} 1") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the undeliverable expiry write must be COUNTED, not discarded: {rendered}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Issue #284, round-2 finding 3 — the candidate pass is EVENT-GATED. It used to run
    /// unconditionally over every online session on every 1 s tick, which took the `sweep`
    /// dispatch from ~4.0 ms to ~6.6 ms at 5000 sessions, forever, for a condition that
    /// only arises when a lease moves. Gated on the placement's ownership version, a
    /// steady-state tick does no scanning at all — and (the half that could silently break)
    /// a lease that DOES move is still noticed on the very next tick.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn the_rehome_scan_is_skipped_while_ownership_has_not_moved() {
        let (tx, placement, remote, metrics) = start_rehome_hub("r:7000").await;
        let client = "epoch-284";

        commit_lease(&placement, client, None);
        let mut out = attach_persistent_v5(&tx, client, 1).await;
        // Several ticks of steady state: the version has not moved since the attach, so
        // the pass is skipped and the gauge stays where it was.
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 3).await;
        let epoch_before = placement.read().unwrap().ownership_epoch();
        // Re-pushing the SAME lease map (what the durable driver does every reconcile
        // tick) must not move the version — else the gate would never save anything.
        commit_lease(&placement, client, None);
        assert_eq!(
            placement.read().unwrap().ownership_epoch(),
            epoch_before,
            "an unchanged lease push must not move the ownership version"
        );
        assert!(
            metrics.render().contains("mqttd_misplaced_sessions 0"),
            "nothing is misplaced in the steady state"
        );

        // And the gate does not blind it: a real move is still acted on.
        commit_lease(&placement, client, Some(&remote));
        assert!(
            placement.read().unwrap().ownership_epoch() > epoch_before,
            "a real lease move must move the ownership version"
        );
        await_rehome_disconnect(&mut out).await;
    }

    /// Issue #284 round 3 — a session becomes misplaced by **ARRIVING**, not only by an
    /// ownership move, and the pass must fire for it too.
    ///
    /// The candidate pass is skipped while [`Placement::ownership_epoch`] is unchanged and
    /// no session is already under observation — a real saving, but it makes detection
    /// edge-triggered on OWNERSHIP. A session that attaches to a non-owning node AFTER the
    /// lease already moved therefore never enters `misplaced`, so the escape hatch cannot
    /// fire and the pass never runs again: the #284 wedge returns, silently, with
    /// `mqttd_misplaced_sessions` reading 0 and no counter moving. Production-reachable in
    /// exactly the window this task exists to survive — a node whose placement view is a
    /// few hundred ms stale relays a persistent client here, and `serve_proxied` runs with
    /// `allow_proxy = false`, so it is attached LOCALLY on a node whose epoch moved before
    /// the client arrived.
    ///
    /// The trigger is seeded at the attach instead of by deleting the skip: one committed
    /// -owner read per persistent attach, nothing per tick.
    // Virtual clock (issue #260): the waits in this test are exact advances of the
    // hub's own intervals, not wall-clock guesses. Paused time only advances when
    // every task is idle, so "the hub has settled, now move on" is deterministic
    // and free — which is strictly stronger than sleeping and hoping.
    #[tokio::test(start_paused = true)]
    async fn a_session_that_becomes_misplaced_by_attaching_is_rehomed() {
        let (tx, placement, remote, metrics) = start_rehome_hub("r:7000").await;
        let client = "latecomer-284";

        // Ownership moves FIRST, with nothing here to notice it, and several sweep ticks
        // consume the epoch bump while the candidate set is empty.
        commit_lease(&placement, client, Some(&remote));
        tokio::time::sleep(super::SESSION_SWEEP_INTERVAL * 4).await;
        assert!(
            metrics.render().contains("mqttd_misplaced_sessions 0"),
            "nothing is misplaced yet — the session has not arrived"
        );

        // Only NOW does the client land here, on a node that is already not the owner.
        let mut out = attach_persistent_v5(&tx, client, 1).await;
        subscribe_qos(&tx, client, "late/284", QoS::AtLeastOnce);

        // It must be rehomed — the arrival is the observation.
        await_rehome_disconnect(&mut out).await;
        let rendered = metrics.render();
        assert!(
            rendered.contains("mqttd_session_rehomes_total{reason=\"stale-owner\"} 1"),
            "a session misplaced by ATTACHING must be rehomed like any other: {rendered}"
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
        remote_interest(&tx, "n1", &["a/#"]);

        publish_with_expiry(&tx, "a/x", b"ttl", Some(45));
        match recv_peer_data(&mut p1).await {
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
        remote_interest(&tx, "n1", &["a/+/b"]);
        remote_interest(&tx, "n2", &["x/#"]);

        publish(&tx, "a/q/b", b"to-n1");
        match recv_peer_data(&mut p1).await {
            Some(PeerMessage::Publish { topic, .. }) => assert_eq!(topic, "a/q/b"),
            other => panic!("n1 should receive the publish, got {other:?}"),
        }

        publish(&tx, "x/1", b"to-n2");
        match recv_peer_data(&mut p2).await {
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
        // Neither peer may see any further DATA frame (n1's non-match included;
        // late handshake frames are order-free and skipped).
        assert!(
            recv_peer_data(&mut p2).await.is_none(),
            "remote publish relayed"
        );
        assert!(
            recv_peer_data(&mut p1).await.is_none(),
            "n1 got a non-matching publish"
        );
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
            reply: None,
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

    /// ADR 0049 P2: a durable session recovery refused past its deadline (CONNACK 0x88)
    /// increments `durable_recovery_failures_total` — the exact signal that was silent
    /// through the 2026-07-14 incident. An *append* failure does not move this counter.
    #[tokio::test(start_paused = true)]
    async fn a_refused_durable_recovery_is_counted() {
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (mut hub, tx) = Hub::with_config(NodeId("h".into()), FlakyStore::new(usize::MAX));
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        let outcome = attach_outcome(&tx, "c", 1).await;
        assert!(
            matches!(outcome, AttachOutcome::Unavailable),
            "a never-ready store must reject; got {outcome:?}"
        );

        let rendered = metrics.render();
        assert!(
            rendered.contains("mqttd_durable_recovery_failures_total{reason=\"deadline\"} 1"),
            "the recovery refusal must be counted; got:\n{rendered}"
        );
        // A recovery refusal is NOT an append failure — the counter that stayed at zero
        // through the incident must remain untouched here.
        assert!(
            !rendered.contains("mqttd_durable_append_failures_total{reason=\"deadline\"}"),
            "a recovery refusal must not be miscounted as an append failure"
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
        let (out_tx, _out_rx) = {
            let (t, r) = mpsc::unbounded_channel();
            (Outbound::new(t).0, r)
        };
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

    // --- issue #242 / ADR 0061: off-loop durable appends -----------------------------

    /// A [`SessionStore`] whose durable append for named clients PARKS until the test
    /// releases it — the deterministic stand-in for a placement group whose follower
    /// set cannot form quorum (the 5 s replication RPC bound, issue #242). Everything
    /// else delegates to an in-memory store. Completed store operations are logged in
    /// order so ordering tests can assert what the store actually observed.
    #[derive(Debug)]
    struct ParkingStore {
        inner: MemorySessionStore,
        /// client id → release gate: an enqueue for a client present here awaits `true`.
        gates:
            std::sync::Mutex<std::collections::HashMap<String, tokio::sync::watch::Receiver<bool>>>,
        /// Store operations in completion order: `(op, "client payload")`.
        ops: std::sync::Mutex<Vec<(String, String)>>,
        /// client id → delay applied to that client's FIRST enqueue only — the
        /// "two lanes of different speed" lever for ordering tests.
        slow_first: std::sync::Mutex<std::collections::HashMap<String, Duration>>,
        /// client id → release gate for `record_outbound` (issue #242 finding A):
        /// the stand-in for a degraded group stalling ADR 0057's outbound-id write.
        outbound_gates:
            std::sync::Mutex<std::collections::HashMap<String, tokio::sync::watch::Receiver<bool>>>,
        /// client id → release gate for `reserve_packet_ids` (issue #242 finding A).
        reserve_gates:
            std::sync::Mutex<std::collections::HashMap<String, tokio::sync::watch::Receiver<bool>>>,
        /// client id → number of upcoming enqueues to answer `Rejected` (the
        /// queue-cap reject-newest model, for the detach-spill tests).
        reject_next: std::sync::Mutex<std::collections::HashMap<String, usize>>,
        /// client id → release gate for `ack` (ADR 0074): the stand-in for a slow
        /// quorum truncate, parking the DETACHED flusher rather than the hub loop.
        ack_gates:
            std::sync::Mutex<std::collections::HashMap<String, tokio::sync::watch::Receiver<bool>>>,
    }

    impl ParkingStore {
        fn new() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                inner: MemorySessionStore::new(),
                gates: std::sync::Mutex::new(std::collections::HashMap::new()),
                ops: std::sync::Mutex::new(Vec::new()),
                slow_first: std::sync::Mutex::new(std::collections::HashMap::new()),
                outbound_gates: std::sync::Mutex::new(std::collections::HashMap::new()),
                reserve_gates: std::sync::Mutex::new(std::collections::HashMap::new()),
                reject_next: std::sync::Mutex::new(std::collections::HashMap::new()),
                ack_gates: std::sync::Mutex::new(std::collections::HashMap::new()),
            })
        }

        /// Delay `client`'s FIRST enqueue by `delay`; later ones run at full speed.
        fn slow_first(&self, client: &str, delay: Duration) {
            self.slow_first.lock().unwrap().insert(client.into(), delay);
        }

        /// Park every future `enqueue_with_expiry` for `client`; the returned sender
        /// releases them all with `send(true)`.
        fn park(&self, client: &str) -> tokio::sync::watch::Sender<bool> {
            let (tx, rx) = tokio::sync::watch::channel(false);
            self.gates.lock().unwrap().insert(client.into(), rx);
            tx
        }

        /// Park every future `record_outbound` for `client` (issue #242 finding A).
        fn park_outbound(&self, client: &str) -> tokio::sync::watch::Sender<bool> {
            let (tx, rx) = tokio::sync::watch::channel(false);
            self.outbound_gates
                .lock()
                .unwrap()
                .insert(client.into(), rx);
            tx
        }

        /// Park every future `reserve_packet_ids` for `client` (issue #242 finding A).
        fn park_reserve(&self, client: &str) -> tokio::sync::watch::Sender<bool> {
            let (tx, rx) = tokio::sync::watch::channel(false);
            self.reserve_gates.lock().unwrap().insert(client.into(), rx);
            tx
        }

        /// Park every future `ack` (truncate) for `client` (ADR 0074); the returned
        /// sender releases them all with `send(true)`.
        fn park_ack(&self, client: &str) -> tokio::sync::watch::Sender<bool> {
            let (tx, rx) = tokio::sync::watch::channel(false);
            self.ack_gates.lock().unwrap().insert(client.into(), rx);
            tx
        }

        /// Answer `client`'s next `n` enqueues with `Rejected` (the session queue
        /// cap under reject-newest, ADR 0001 §6).
        fn reject_next_enqueue(&self, client: &str, n: usize) {
            self.reject_next.lock().unwrap().insert(client.into(), n);
        }

        /// Await the release gate in `map` for `client`, if one is set.
        async fn await_gate(
            map: &std::sync::Mutex<
                std::collections::HashMap<String, tokio::sync::watch::Receiver<bool>>,
            >,
            client: &ClientId,
        ) {
            let gate = map.lock().unwrap().get(&client.0).cloned();
            if let Some(mut rx) = gate {
                while !*rx.borrow() {
                    if rx.changed().await.is_err() {
                        break; // sender dropped: released
                    }
                }
            }
        }

        /// The completed operations, in store order.
        fn ops(&self) -> Vec<(String, String)> {
            self.ops.lock().unwrap().clone()
        }

        fn log(&self, op: &str, detail: String) {
            self.ops.lock().unwrap().push((op.to_string(), detail));
        }
    }

    #[async_trait::async_trait]
    impl mqtt_storage::SessionStore for ParkingStore {
        async fn ensure_session(
            &self,
            client: &ClientId,
        ) -> Result<bool, mqtt_storage::StorageError> {
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
            let gate = self.gates.lock().unwrap().get(&client.0).cloned();
            if let Some(mut rx) = gate {
                while !*rx.borrow() {
                    if rx.changed().await.is_err() {
                        break; // sender dropped: released
                    }
                }
            }
            let delay = self.slow_first.lock().unwrap().remove(&client.0);
            if let Some(delay) = delay {
                // SETTLE(store-slow-first-enqueue): the delay IS the fault. `slow_first` makes
                // one enqueue slow so a caller racing it can be observed, and there is nothing
                // to poll for — the observation would be the latency being injected. The value
                // comes from the test that armed it, and a slow machine only makes the injected
                // store slower, which is still a valid instance of the fault.
                tokio::time::sleep(delay).await;
            }
            // The queue-cap reject-newest model (issue #242 finding C): the next
            // `n` enqueues are answered `Rejected` — a live-only delivery whose
            // backlog entry carries no offset, the one shape the detach spill owes.
            let reject = {
                let mut m = self.reject_next.lock().unwrap();
                match m.get_mut(&client.0) {
                    Some(n) if *n > 0 => {
                        *n -= 1;
                        true
                    }
                    _ => false,
                }
            };
            if reject {
                self.log(
                    "reject",
                    format!(
                        "{} {}",
                        client.0,
                        String::from_utf8_lossy(message.payload.as_ref())
                    ),
                );
                return Ok(mqtt_storage::Enqueued::Rejected);
            }
            let out = self
                .inner
                .enqueue_with_expiry(client, message, expiry_at)
                .await;
            if out.is_ok() {
                self.log(
                    "enqueue",
                    format!(
                        "{} {}",
                        client.0,
                        String::from_utf8_lossy(message.payload.as_ref())
                    ),
                );
            }
            out
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
            Self::await_gate(&self.ack_gates, client).await;
            self.log("ack", format!("{} {up_to}", client.0));
            self.inner.ack(client, up_to).await
        }
        async fn record_received(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<mqtt_storage::InboundSighting, mqtt_storage::StorageError> {
            self.inner.record_received(client, packet_id).await
        }
        async fn ack_received(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.ack_received(client, packet_id).await
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
        async fn record_outbound(
            &self,
            client: &ClientId,
            packet_id: u16,
            offset: mqtt_storage::Offset,
        ) -> Result<(), mqtt_storage::StorageError> {
            Self::await_gate(&self.outbound_gates, client).await;
            let out = self.inner.record_outbound(client, packet_id, offset).await;
            if out.is_ok() {
                self.log("record", format!("{} {packet_id}", client.0));
            }
            out
        }
        async fn advance_outbound(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.advance_outbound(client, packet_id).await
        }
        async fn reserve_packet_ids(
            &self,
            client: &ClientId,
            count: u16,
        ) -> Result<u16, mqtt_storage::StorageError> {
            Self::await_gate(&self.reserve_gates, client).await;
            let out = self.inner.reserve_packet_ids(client, count).await;
            if out.is_ok() {
                self.log("reserve", client.0.clone());
            }
            out
        }
        async fn clear_outbound(
            &self,
            client: &ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            let out = self.inner.clear_outbound(client, packet_id).await;
            if out.is_ok() {
                self.log("clear", format!("{} {packet_id}", client.0));
            }
            out
        }
        async fn outbound(
            &self,
            client: &ClientId,
        ) -> Result<Vec<mqtt_storage::OutboundInflight>, mqtt_storage::StorageError> {
            self.inner.outbound(client).await
        }
        async fn next_packet_id(
            &self,
            client: &ClientId,
        ) -> Result<u16, mqtt_storage::StorageError> {
            self.inner.next_packet_id(client).await
        }
        async fn remove(&self, client: &ClientId) -> Result<(), mqtt_storage::StorageError> {
            let out = self.inner.remove(client).await;
            if out.is_ok() {
                self.log("remove", client.0.clone());
            }
            out
        }
    }

    /// ADR 0072 — RELAXED tier: with the operator opt-in, a publish carrying
    /// `mqttd-durability: relaxed` acks at accept+submit, NOT after the durable
    /// append. The append is parked in the store; the ack must arrive anyway;
    /// releasing the store completes the (still-running) append — the write is
    /// weakened in ack MEANING only, never skipped.
    #[tokio::test]
    async fn a_relaxed_publish_acks_while_its_append_is_still_parked() {
        let store = ParkingStore::new();
        let release = store.park("r");
        let (mut hub, tx) = Hub::with_config(NodeId("hub-test".into()), store.clone());
        hub.set_allow_relaxed_publish(true);
        tokio::spawn(hub.run());

        let (_rx, _) = attach(&tx, "r", 1, false).await;
        subscribe_qos(&tx, "r", "rt/t", QoS::AtLeastOnce);
        detach(&tx, "r", 1);

        let (done_tx, done_rx) = oneshot::channel();
        tx.send(HubCommand::Publish {
            topic: "rt/t".into(),
            payload: Bytes::from_static(b"fast"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties {
                user_properties: vec![("mqttd-durability".into(), "relaxed".into())],
                ..Default::default()
            },
            done: Some(done_tx),
            v5: true,
            publisher: None,
        })
        .unwrap();

        // The ack arrives while the append is STILL parked.
        let out = timeout(Duration::from_millis(500), done_rx)
            .await
            .expect("a relaxed publish must ack at submit, not after the parked append")
            .unwrap();
        assert_eq!(out, PublishOutcome::Accepted);
        assert!(
            store.ops().iter().all(|(op, _)| op != "enqueue"),
            "the append must not have completed yet — the ack outran it by design"
        );

        // The write still happens: releasing the store lands the enqueue.
        release.send(true).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if store
                .ops()
                .iter()
                .any(|(op, d)| op == "enqueue" && d == "r fast")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the relaxed publish's append must still complete after release"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Issue #399 — the relaxed congestion valve: a relaxed publish whose
    /// append lane is already at [`crate::hub::lanes::RELAXED_CONGESTION_DEPTH`] does NOT
    /// ack at submit — it completes by the quorum rule, after its append
    /// lands. Without the valve, instant acks refill the publisher's window
    /// forever while the bounded lane only drains at disk speed; the lane
    /// overflows, the overflow fails the publish and closes the connection,
    /// and the curve measured the resulting reconnect storm. Relaxed grants
    /// latency below congestion, not immunity from capacity.
    #[tokio::test]
    async fn a_congested_relaxed_publish_waits_for_its_append() {
        let store = ParkingStore::new();
        let release = store.park("r");
        let (mut hub, tx) = Hub::with_config(NodeId("hub-test".into()), store.clone());
        hub.set_allow_relaxed_publish(true);
        tokio::spawn(hub.run());

        let (_rx, _) = attach(&tx, "r", 1, false).await;
        subscribe_qos(&tx, "r", "rt/t", QoS::AtLeastOnce);
        detach(&tx, "r", 1);

        // Park the lane at the threshold: after these strict publishes, the
        // NEXT submit observes a depth of exactly RELAXED_CONGESTION_DEPTH.
        for i in 0..(crate::hub::lanes::RELAXED_CONGESTION_DEPTH - 1) {
            tx.send(HubCommand::Publish {
                topic: "rt/t".into(),
                payload: Bytes::from(format!("fill{i}")),
                qos: QoS::AtLeastOnce,
                retain: false,
                message_expiry: None,
                app: mqtt_core::AppProperties::default(),
                done: None,
                v5: true,
                publisher: None,
            })
            .unwrap();
        }

        let (done_tx, done_rx) = oneshot::channel();
        tx.send(HubCommand::Publish {
            topic: "rt/t".into(),
            payload: Bytes::from_static(b"throttled"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties {
                user_properties: vec![("mqttd-durability".into(), "relaxed".into())],
                ..Default::default()
            },
            done: Some(done_tx),
            v5: true,
            publisher: None,
        })
        .unwrap();

        // The valve holds the ack: no early completion while the lane drains.
        let mut done_rx = done_rx;
        assert!(
            timeout(Duration::from_millis(300), &mut done_rx)
                .await
                .is_err(),
            "a congested relaxed publish must NOT ack at submit — that instant \
             refill is exactly the overflow-then-conn-close storm of issue #399"
        );

        // Releasing the store drains the lane; the ack now arrives by the
        // quorum rule — throttled, never refused.
        release.send(true).unwrap();
        let out = timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("a congested relaxed publish completes once its append lands")
            .unwrap();
        assert_eq!(out, PublishOutcome::Accepted);
    }

    /// Issue #399, the valve's owner half, fast path: a RELAXED forward whose
    /// lane submits were all admitted below the congestion threshold is
    /// answered `Stored` at submit-acceptance — while its append is still
    /// parked in the store. `Stored` then means exactly what the relaxed ack
    /// means (ADR 0072: accepted and submitted), and the origin's relaxed
    /// pending completes on one peer round trip instead of a durability wait.
    #[tokio::test]
    async fn an_uncongested_relaxed_forward_is_answered_at_submit() {
        let store = ParkingStore::new();
        let release = store.park("r");
        let (mut hub, tx) = Hub::with_config(NodeId("hub-test".into()), store.clone());
        hub.set_allow_relaxed_publish(true);
        tokio::spawn(hub.run());

        let (_rx, _) = attach(&tx, "r", 1, false).await;
        subscribe_qos(&tx, "r", "rt/t", QoS::AtLeastOnce);
        detach(&tx, "r", 1);
        let mut peer = connect_peer(&tx, "origin-node", 1);

        tx.send(HubCommand::RemotePublishAcked {
            node: NodeId("origin-node".into()),
            seq: 9,
            topic: "rt/t".into(),
            payload: Bytes::from_static(b"fast"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties {
                user_properties: vec![("mqttd-durability".into(), "relaxed".into())],
                ..Default::default()
            },
        })
        .unwrap();

        // The verdict arrives while the append is STILL parked.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let verdict = loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "no early PublishVerdict for the uncongested relaxed forward"
            );
            match timeout(Duration::from_millis(200), peer.recv()).await {
                Ok(Some(PeerMessage::PublishVerdict { seq: 9, verdict })) => break verdict,
                Ok(Some(_)) | Err(_) => {}
                Ok(None) => panic!("peer channel closed before a verdict"),
            }
        };
        assert_eq!(
            verdict,
            mqtt_cluster::peer::ForwardVerdict::Stored,
            "an uncongested relaxed forward answers Stored at submit-acceptance"
        );
        assert!(
            store.ops().iter().all(|(op, _)| op != "enqueue"),
            "the append must not have completed yet — the verdict outran it by design"
        );

        // The write still happens, and no second verdict is ever sent.
        release.send(true).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !store
            .ops()
            .iter()
            .any(|(op, d)| op == "enqueue" && d == "r fast")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the relaxed forward's append must still complete after release"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // SETTLE(early-verdict-uniqueness): the completed append's would-be
        // second answer is only observable by its absence; one drain of the
        // peer channel after the append provably landed is the check.
        tokio::time::sleep(Duration::from_millis(100)).await;
        while let Ok(msg) = peer.try_recv() {
            assert!(
                !matches!(msg, PeerMessage::PublishVerdict { seq: 9, .. }),
                "the early-answered forward must not be answered a second time"
            );
        }
    }

    /// Issue #399, the valve's owner half, throttle path: a RELAXED forward
    /// that finds its lane at the congestion threshold is answered only at
    /// append completion (the quorum rule) — the origin's publisher window
    /// then throttles to THIS node's drain rate, which it cannot see directly.
    #[tokio::test]
    async fn a_congested_relaxed_forward_is_answered_at_append_completion() {
        let store = ParkingStore::new();
        let release = store.park("r");
        let (mut hub, tx) = Hub::with_config(NodeId("hub-test".into()), store.clone());
        hub.set_allow_relaxed_publish(true);
        tokio::spawn(hub.run());

        let (_rx, _) = attach(&tx, "r", 1, false).await;
        subscribe_qos(&tx, "r", "rt/t", QoS::AtLeastOnce);
        detach(&tx, "r", 1);
        let mut peer = connect_peer(&tx, "origin-node", 1);

        // Park the lane at the threshold with local strict publishes.
        for i in 0..(crate::hub::lanes::RELAXED_CONGESTION_DEPTH - 1) {
            tx.send(HubCommand::Publish {
                topic: "rt/t".into(),
                payload: Bytes::from(format!("fill{i}")),
                qos: QoS::AtLeastOnce,
                retain: false,
                message_expiry: None,
                app: mqtt_core::AppProperties::default(),
                done: None,
                v5: true,
                publisher: None,
            })
            .unwrap();
        }

        tx.send(HubCommand::RemotePublishAcked {
            node: NodeId("origin-node".into()),
            seq: 11,
            topic: "rt/t".into(),
            payload: Bytes::from_static(b"throttled"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties {
                user_properties: vec![("mqttd-durability".into(), "relaxed".into())],
                ..Default::default()
            },
        })
        .unwrap();

        // No early verdict: the lane is congested, the valve holds it.
        let early = timeout(Duration::from_millis(300), async {
            loop {
                match peer.recv().await {
                    Some(PeerMessage::PublishVerdict { seq: 11, verdict }) => break verdict,
                    Some(_) => {}
                    None => panic!("peer channel closed"),
                }
            }
        })
        .await;
        assert!(
            early.is_err(),
            "a congested relaxed forward must NOT be answered at submit — \
             that instant refill is issue #399's cross-node overflow"
        );

        // Release: the appends land, and only then does Stored go out.
        release.send(true).unwrap();
        let verdict = timeout(Duration::from_secs(5), async {
            loop {
                match peer.recv().await {
                    Some(PeerMessage::PublishVerdict { seq: 11, verdict }) => break verdict,
                    Some(_) => {}
                    None => panic!("peer channel closed"),
                }
            }
        })
        .await
        .expect("the congested relaxed forward answers once its append lands");
        assert_eq!(verdict, mqtt_cluster::peer::ForwardVerdict::Stored);
    }

    /// ADR 0072 — the `mqttd-durability` property is INERT without the operator
    /// opt-in: the identical relaxed-tagged publish keeps the full
    /// ack-after-durable behavior (stronger than asked, never weaker). This is
    /// the default-path guarantee: no property parsing changes any ack unless
    /// `MQTTD_ALLOW_RELAXED_PUBLISH` is set.
    #[tokio::test]
    async fn the_durability_property_is_inert_without_the_operator_opt_in() {
        let store = ParkingStore::new();
        let release = store.park("s");
        let tx = start_hub_with_arc(store.clone());

        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "st/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);

        let (done_tx, mut done_rx) = oneshot::channel();
        tx.send(HubCommand::Publish {
            topic: "st/t".into(),
            payload: Bytes::from_static(b"strict"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: mqtt_core::AppProperties {
                user_properties: vec![("mqttd-durability".into(), "relaxed".into())],
                ..Default::default()
            },
            done: Some(done_tx),
            v5: true,
            publisher: None,
        })
        .unwrap();

        // SETTLE(inert-property-negative-window): proving an ack did NOT arrive has
        // no observable to poll — the assertion is the continued absence itself. The
        // window only needs to be long enough that an early-release bug would fire
        // within it; a slow machine shortens the effective window, which can only
        // make this negative check MORE tolerant, never a false failure.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            done_rx.try_recv().is_err(),
            "without MQTTD_ALLOW_RELAXED_PUBLISH the property must not weaken the ack"
        );
        release.send(true).unwrap();
        let out = timeout(Duration::from_secs(2), done_rx)
            .await
            .expect("released append completes the strict publish")
            .unwrap();
        assert_eq!(out, PublishOutcome::Accepted);
    }

    /// ADR 0074 — a subscriber's PUBACK completes without waiting the durable
    /// truncate: with the store's `ack` PARKED, the hub keeps delivering (the
    /// old inline await blocked the whole loop on exactly this gate — RED
    /// before the fix), and the released flusher then truncates at the right
    /// watermark. The failure path's tolerance is unchanged: entries outlive
    /// the ack only until the flush lands.
    #[tokio::test]
    async fn a_subscriber_ack_completes_while_its_truncate_is_still_parked() {
        let store = ParkingStore::new();
        let release = store.park_ack("s");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "tr/t", QoS::AtLeastOnce);

        publish_qos1(&tx, "tr/t", b"one");
        let pkid = pkid_of(
            &timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("first delivery")
                .unwrap(),
        );
        pub_ack(&tx, "s", pkid);

        // The loop must stay live while the truncate is parked: a second publish
        // still flows end to end. Under the old inline await this hung forever.
        publish_qos1(&tx, "tr/t", b"two");
        let pkid2 = pkid_of(
            &timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("the hub must not block on a parked truncate")
                .unwrap(),
        );
        assert_ne!(pkid, pkid2);

        release.send(true).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if store
                .ops()
                .iter()
                .any(|(op, d)| op == "ack" && d.starts_with("s "))
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the released flusher must truncate the acked prefix; ops: {:?}",
                store.ops()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// ADR 0074 — a burst of acks COALESCES: five acknowledged deliveries reach
    /// the store as at most two truncates (one that was already parked in
    /// flight, plus one carrying the final watermark), and the last truncate
    /// covers the entire acked prefix. This is the O(sessions)-not-O(messages)
    /// property the watermark exists for.
    #[tokio::test]
    async fn a_burst_of_acks_coalesces_into_one_watermark_truncate() {
        let store = ParkingStore::new();
        let release = store.park_ack("s");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "co/t", QoS::AtLeastOnce);

        let mut pkids = Vec::new();
        for _ in 0..5 {
            publish_qos1(&tx, "co/t", b"m");
            pkids.push(pkid_of(
                &timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .expect("delivery")
                    .unwrap(),
            ));
        }
        for pkid in pkids {
            pub_ack(&tx, "s", pkid);
        }

        release.send(true).unwrap();
        // The flusher settles when a truncate covering the FULL prefix has landed.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let final_up_to = loop {
            let acks: Vec<u64> = store
                .ops()
                .iter()
                .filter(|(op, d)| op == "ack" && d.starts_with("s "))
                .map(|(_, d)| d.split(' ').nth(1).unwrap().parse::<u64>().unwrap())
                .collect();
            if let Some(&last) = acks.last() {
                if acks.iter().all(|a| *a <= last) && !acks.is_empty() {
                    // Wait until no NEW ack has landed for a beat — then judge.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let settled: Vec<u64> = store
                        .ops()
                        .iter()
                        .filter(|(op, d)| op == "ack" && d.starts_with("s "))
                        .map(|(_, d)| d.split(' ').nth(1).unwrap().parse::<u64>().unwrap())
                        .collect();
                    if settled.len() == acks.len() {
                        assert!(
                            settled.len() <= 2,
                            "five acks must coalesce to at most two truncates, got {settled:?}"
                        );
                        break *settled.last().unwrap();
                    }
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no truncate landed after release; ops: {:?}",
                store.ops()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        // The final watermark covers everything the earlier flushes covered.
        let all: Vec<u64> = store
            .ops()
            .iter()
            .filter(|(op, d)| op == "ack" && d.starts_with("s "))
            .map(|(_, d)| d.split(' ').nth(1).unwrap().parse::<u64>().unwrap())
            .collect();
        assert_eq!(final_up_to, *all.iter().max().unwrap());
    }

    /// Issue #242 — the head-of-line acceptance criterion (RED before the fix): a
    /// durable append stalled on one placement group's degraded follower set must not
    /// delay a publish whose subscriber lives only in a healthy group, nor a CONNECT.
    /// Before the fix the hub awaited the stalled append INLINE in its single dispatch
    /// loop, so every queued command waited the full stall behind it.
    #[tokio::test]
    async fn a_stalled_group_a_append_does_not_delay_a_group_b_publish() {
        let store = ParkingStore::new();
        let release_a = store.park("a");
        let tx = start_hub_with_arc(store.clone());

        // Two offline persistent QoS 1 subscribers in different "groups" (per-session
        // stores, so per-client parking is exactly a one-group stall).
        let (_a_rx, _) = attach(&tx, "a", 1, false).await;
        subscribe_qos(&tx, "a", "ga/t", QoS::AtLeastOnce);
        detach(&tx, "a", 1);
        let (_b_rx, _) = attach(&tx, "b", 2, false).await;
        subscribe_qos(&tx, "b", "gb/t", QoS::AtLeastOnce);
        detach(&tx, "b", 2);

        // Group A stalls: this publish's durable append parks in the store.
        let a_done = publish_gated(&tx, "ga/t", b"held", QoS::AtLeastOnce, true);

        // Group B must not queue behind it (issue #242).
        let b_done = publish_gated(&tx, "gb/t", b"fast", QoS::AtLeastOnce, true);
        let b_out = timeout(Duration::from_millis(500), b_done)
            .await
            .expect("a group-B publish must not wait behind group A's stalled append (#242)")
            .unwrap();
        assert_eq!(b_out, PublishOutcome::Accepted);

        // A CONNECT during the stall completes too.
        let connect = timeout(Duration::from_secs(1), attach(&tx, "c", 3, true)).await;
        assert!(
            connect.is_ok(),
            "a CONNECT must not wait behind a stalled append (#242)"
        );

        // The stalled publish is deferred, not lost: releasing the group completes it.
        release_a.send(true).unwrap();
        let a_out = timeout(Duration::from_secs(1), a_done)
            .await
            .expect("the released append must complete the held publish")
            .unwrap();
        assert_eq!(a_out, PublishOutcome::Accepted);
    }

    /// PER-SESSION ORDERING (issue #242): two publishes matching the same offline
    /// subscriber append in arrival order even when the first is much slower — the
    /// lane is one FIFO per session, so the store call for message k+1 starts only
    /// after message k's returned. A spawn-per-append motion (the naive off-loop
    /// port) inverts this.
    #[tokio::test]
    async fn two_publishes_to_one_offline_subscriber_append_in_arrival_order() {
        let store = ParkingStore::new();
        store.slow_first("s", Duration::from_millis(200));
        let tx = start_hub_with_arc(store.clone());

        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "o/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);

        let d1 = publish_gated(&tx, "o/t", b"first", QoS::AtLeastOnce, true);
        let d2 = publish_gated(&tx, "o/t", b"second", QoS::AtLeastOnce, true);
        assert_eq!(d1.await.unwrap(), PublishOutcome::Accepted);
        assert_eq!(d2.await.unwrap(), PublishOutcome::Accepted);

        let enqueues: Vec<String> = store
            .ops()
            .into_iter()
            .filter(|(op, _)| op == "enqueue")
            .map(|(_, detail)| detail)
            .collect();
        assert_eq!(
            enqueues,
            vec!["s first".to_string(), "s second".to_string()],
            "arrival order is durable-queue order, whatever the appends' speeds (#242)"
        );
    }

    /// #238 PLAN/COMMIT ATOMICITY across the off-loop motion: a brownout flipped
    /// while an admitted append is still in flight must neither refuse the committed
    /// publish (its decision was frozen at the plan pass) nor admit the next one
    /// (the flip governs it). Never acked-and-dropped, never a false refusal.
    #[tokio::test]
    async fn brownout_flipped_mid_append_neither_refuses_the_committed_publish_nor_admits_the_next()
    {
        let store = ParkingStore::new();
        let release = store.park("s");
        let tx = start_hub_with_arc(store.clone());

        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "bo/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);

        // P1 is planned and admitted while brownout is OFF; its append parks.
        let p1 = publish_gated(&tx, "bo/t", b"pre-flip", QoS::AtLeastOnce, true);

        // The flip lands while P1 is still in flight (the loop is free — #242).
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();

        // P2, planned AFTER the flip, is refused effect-free.
        let p2 = publish_gated(&tx, "bo/t", b"post-flip", QoS::AtLeastOnce, true);
        assert_eq!(
            timeout(Duration::from_millis(500), p2)
                .await
                .expect("a refusal is decided on-loop, without waiting on any lane")
                .unwrap(),
            PublishOutcome::Refused(PublishRefusal::Brownout),
            "the flip governs the NEXT publish (#238)"
        );

        // P1 runs to its real store outcome under its frozen decision: Accepted.
        release.send(true).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), p1).await.unwrap().unwrap(),
            PublishOutcome::Accepted,
            "an admitted append is never un-decided by a later flip (#238)"
        );

        // Exactly one durable copy exists: P1's. P2's refusal was effect-free.
        let enqueues: Vec<String> = store
            .ops()
            .into_iter()
            .filter(|(op, _)| op == "enqueue")
            .map(|(_, d)| d)
            .collect();
        assert_eq!(enqueues, vec!["s pre-flip".to_string()]);
    }

    /// ACK-AFTER-DURABLE's wire half (#124), post-motion: an ONLINE persistent
    /// subscriber's PUBLISH packet reaches the conn channel only after its durable
    /// append resolved — the live send now lives in the `AppendDone` handler and
    /// carries the store's own offset.
    #[tokio::test]
    async fn an_online_persistent_subscriber_receives_the_wire_send_only_after_the_append_resolves()
    {
        let store = ParkingStore::new();
        let release = store.park("p");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "w/t", QoS::AtLeastOnce);

        let mut done = publish_gated(&tx, "w/t", b"gated", QoS::AtLeastOnce, true);

        // While the append is parked: nothing on the wire, no ack.
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "no packet may reach the wire before the durable record exists (#124)"
        );
        assert!(
            done.try_recv().is_err(),
            "the publisher's ack must wait for the append too"
        );

        release.send(true).unwrap();
        let packet = recv_packet(&mut rx).await.expect("delivered after durable");
        assert_eq!(payload_of(&packet), b"gated");
        assert_eq!(
            timeout(Duration::from_secs(1), done)
                .await
                .unwrap()
                .unwrap(),
            PublishOutcome::Accepted
        );
    }

    /// Backpressure (issue #242): a saturated lane rejects the NEWEST job — the
    /// publisher is withheld (never falsely acked, never falsely refused), the
    /// accepted jobs keep FIFO order, and every accepted publish still completes.
    #[tokio::test]
    async fn a_full_append_lane_withholds_the_publisher_and_reorders_nothing() {
        let store = ParkingStore::new();
        let release = store.park("s");
        let tx = start_hub_with_arc(store.clone());

        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "cap/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);

        // Overfill: the lane admits at most LANE_QUEUE_CAP queued jobs (plus the one
        // the parked worker holds); the rest are rejected at submit.
        let total = super::LANE_QUEUE_CAP + 40;
        let payloads: Vec<String> = (0..total).map(|i| format!("m{i:04}")).collect();
        let mut dones = Vec::new();
        for p in &payloads {
            let (done_tx, done_rx) = oneshot::channel();
            tx.send(HubCommand::Publish {
                topic: "cap/t".into(),
                payload: Bytes::from(p.clone().into_bytes()),
                qos: QoS::AtLeastOnce,
                retain: false,
                message_expiry: None,
                app: AppProperties::default(),
                done: Some(done_tx),
                v5: true,
                publisher: None,
            })
            .unwrap();
            dones.push(done_rx);
        }
        // Barrier: every submission is processed (and the overflow rejected) BEFORE
        // the stalled group is released — the loop is free to do this (#242).
        let (ping_tx, ping_rx) = oneshot::channel();
        tx.send(HubCommand::Ping { reply: ping_tx }).unwrap();
        timeout(Duration::from_secs(1), ping_rx)
            .await
            .expect("the loop must not be parked with the appends")
            .unwrap();
        release.send(true).unwrap();

        let mut outcomes = Vec::new();
        for done in dones {
            outcomes.push(timeout(Duration::from_secs(5), done).await.unwrap());
        }
        let accepted: Vec<usize> = outcomes
            .iter()
            .enumerate()
            .filter(|(_, o)| matches!(o, Ok(PublishOutcome::Accepted)))
            .map(|(i, _)| i)
            .collect();
        let withheld = outcomes.iter().filter(|o| o.is_err()).count();
        assert!(withheld > 0, "overfilling the lane must withhold, loudly");
        assert!(
            !accepted.is_empty(),
            "the admitted prefix must still complete"
        );
        // Reject-NEWEST: the accepted set is exactly a prefix of submission order.
        assert_eq!(
            accepted,
            (0..accepted.len()).collect::<Vec<_>>(),
            "rejecting anything but the newest would break FIFO admission (#242)"
        );
        // And the store observed the accepted jobs in submission order.
        let enqueues: Vec<String> = store
            .ops()
            .into_iter()
            .filter(|(op, _)| op == "enqueue")
            .map(|(_, d)| d)
            .collect();
        let expected: Vec<String> = accepted
            .iter()
            .map(|i| format!("s {}", payloads[*i]))
            .collect();
        assert_eq!(enqueues, expected, "lane order is store order (#242)");
    }

    /// A subscriber that ATTACHES while its append is in flight still receives the
    /// message exactly once (issue #242): the attach replay reads the queue before
    /// the append lands, so the completion handler delivers it — but only because the
    /// replayed high-water provably excludes the new offset; had the replay seen it,
    /// the completion would stay silent.
    #[tokio::test]
    async fn attach_during_inflight_append_delivers_the_message_exactly_once() {
        let store = ParkingStore::new();
        let release = store.park("s");
        let tx = start_hub_with_arc(store.clone());

        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "mid/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);

        let done = publish_gated(&tx, "mid/t", b"inflight", QoS::AtLeastOnce, true);

        // The subscriber reattaches while the append is parked; its replay sees an
        // empty queue. The CONNACK is not held hostage by the stalled group.
        let (mut rx2, present) = timeout(Duration::from_secs(1), attach(&tx, "s", 2, false))
            .await
            .expect("a reattach must not wait behind the session's stalled append");
        assert!(present, "the persistent session resumes");

        release.send(true).unwrap();
        let packet = recv_packet(&mut rx2)
            .await
            .expect("the in-flight append must be delivered to the reattached session (#242)");
        assert_eq!(payload_of(&packet), b"inflight");
        assert!(
            recv_packet(&mut rx2).await.is_none(),
            "exactly once: no duplicate from replay plus completion"
        );
        assert_eq!(
            timeout(Duration::from_secs(1), done)
                .await
                .unwrap()
                .unwrap(),
            PublishOutcome::Accepted
        );
    }

    /// A publisher that disconnects mid-append (its `done` receiver dropped) must not
    /// panic the hub, and the obligation still resolves: the store write lands and
    /// the loop keeps serving.
    #[tokio::test]
    async fn publisher_disconnect_mid_append_resolves_the_obligation_without_panic() {
        let store = ParkingStore::new();
        let release = store.park("s");
        let tx = start_hub_with_arc(store.clone());

        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "gone/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);

        let done = publish_gated(&tx, "gone/t", b"orphan", QoS::AtLeastOnce, true);
        drop(done); // the publisher's connection is gone

        release.send(true).unwrap();

        // The loop keeps serving (no panic), and the durable copy exists.
        let (_c_rx, _) = timeout(Duration::from_secs(1), attach(&tx, "c", 2, true))
            .await
            .expect("the hub must survive an orphaned publish completion");
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let stored = store
                .ops()
                .iter()
                .any(|(op, d)| op == "enqueue" && d == "s orphan");
            if stored {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the admitted append must still run to a real store outcome"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A CRASH STOP RELEASES THE STORE (issue #242 / ADR 0061 §8). A node stops by
    /// having its hub task aborted — the loop's `None` arm cannot fire, since the hub
    /// holds a clone of its own command sender — and a lane worker holds an `Arc` of
    /// the session store whose redb handle is an exclusive lock. So the abort MUST
    /// cascade to the lanes: the hub owns them in a `JoinSet`, and dropping `self`
    /// aborts them, releasing every handle at once, as the OS would for a killed
    /// process.
    ///
    /// This is the regression CI caught and no local run did
    /// (`cluster_stress::a_full_cluster_stop_start_recovers_every_acked_fact`, which
    /// restarts a stopped cluster over the same data dirs): at a full-cluster stop an
    /// in-flight append cannot reach quorum, so it parks for the 5s replication bound
    /// holding the handle, and the restart arrives first with
    /// "Database already open. Cannot acquire lock." Before the off-loop motion the
    /// append was awaited on the loop, so the abort killed it for free.
    ///
    /// Abandoning that write is the honest trade, not a loss: the publisher's ack was
    /// WITHHELD (asserted below), so nothing was falsely promised, and a crash is
    /// exactly the event whose torn writes the durable plane already recovers from
    /// (ADR 0044). Pinned at the unit tier because the stop/start tier makes it a race.
    #[tokio::test]
    async fn a_crashed_hub_releases_the_store_so_the_node_can_restart() {
        let store = ParkingStore::new();
        let _release = store.park("s");
        let arc = store.clone() as std::sync::Arc<dyn mqtt_storage::SessionStore>;
        let before = std::sync::Arc::strong_count(&arc);
        let (hub, tx) = Hub::with_config(NodeId("hub-test".into()), arc.clone());
        let hub_task = tokio::spawn(hub.run());

        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "shut/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);

        let done = publish_gated(&tx, "shut/t", b"parked", QoS::AtLeastOnce, true);
        // Barrier: the job is ADMITTED and parked in the lane before the crash, so a
        // worker is genuinely holding the store when the abort lands.
        let (ping_tx, ping_rx) = oneshot::channel();
        tx.send(HubCommand::Ping { reply: ping_tx }).unwrap();
        timeout(Duration::from_secs(1), ping_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(
            std::sync::Arc::strong_count(&arc) > before,
            "the lane worker must be holding the store for this test to mean anything"
        );

        // The crash: abort the hub task, exactly as the node teardown does.
        hub_task.abort();
        let _ = hub_task.await;

        // The store handle must come back WITHOUT waiting out the parked append (the
        // park is never released here — that is the point).
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if std::sync::Arc::strong_count(&arc) == before {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "a lane worker outlived the crashed hub and still holds the session \
                 store: redb stays locked and the next start over this data dir fails \
                 with \"Database already open\" (issue #242)"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // And the publisher was WITHHELD, never falsely acked.
        assert!(
            timeout(Duration::from_secs(1), done)
                .await
                .unwrap()
                .is_err(),
            "with the hub gone the ack is withheld (fail closed), never fabricated"
        );
    }

    /// Mixed-QoS per-client wire order survives the off-loop motion (issue #242): a
    /// `QoS` 0 delivery to a client whose lane holds an in-flight `QoS` 1 append is
    /// routed through the lane as a passthrough, so it cannot overtake the earlier
    /// message's post-durable live send.
    #[tokio::test]
    async fn a_qos0_send_behind_a_busy_lane_does_not_overtake_the_pending_qos1_delivery() {
        let store = ParkingStore::new();
        let release = store.park("p");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "ord/#", QoS::AtLeastOnce);

        let done = publish_gated(&tx, "ord/a", b"first-qos1", QoS::AtLeastOnce, true);
        publish(&tx, "ord/b", b"second-qos0");

        // While the QoS 1 append is parked, the QoS 0 message must NOT arrive first.
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "a QoS 0 send overtook an in-flight QoS 1 delivery to the same client (#242)"
        );

        release.send(true).unwrap();
        let p1 = recv_packet(&mut rx).await.expect("QoS 1 first");
        assert_eq!(payload_of(&p1), b"first-qos1");
        let p2 = recv_packet(&mut rx).await.expect("QoS 0 second");
        assert_eq!(payload_of(&p2), b"second-qos0");
        assert_eq!(
            timeout(Duration::from_secs(1), done)
                .await
                .unwrap()
                .unwrap(),
            PublishOutcome::Accepted
        );
    }

    /// The time-on-loop tripwire is EXPORTED and stays flat while a store is parked
    /// (issue #242): the publish dispatch is plan + submit only, so a stalled append
    /// contributes nothing to `mqttd_hub_dispatch_seconds{command="publish"}`.
    #[tokio::test]
    async fn hub_dispatch_time_is_exported_and_stays_flat_while_a_store_is_parked() {
        let store = ParkingStore::new();
        let release = store.park("s");
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("test"));
        let (mut hub, tx) = Hub::with_config(
            NodeId("hub-test".into()),
            store.clone() as std::sync::Arc<dyn mqtt_storage::SessionStore>,
        );
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "m/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);

        let done = publish_gated(&tx, "m/t", b"timed", QoS::AtLeastOnce, true);
        // Prove the loop already recorded the publish dispatch (it is NOT waiting on
        // the parked append) by round-tripping a later command...
        let (ping_tx, ping_rx) = oneshot::channel();
        tx.send(HubCommand::Ping { reply: ping_tx }).unwrap();
        timeout(Duration::from_millis(500), ping_rx)
            .await
            .expect("the loop must not be parked with the append")
            .unwrap();
        // ...then assert the exposition carries the publish observation, with every
        // recorded dispatch under the stall bound (the park is 30s-equivalent; any
        // inline await would land in the top buckets).
        let out = metrics.render();
        assert!(
            out.contains("mqttd_hub_dispatch_seconds_count{command=\"publish\"} 1"),
            "the publish dispatch must be observed exactly once while its append is \
             still parked:\n{out}"
        );
        // The whole distribution sits in the sub-100ms buckets: the +Inf count equals
        // the 0.1s-bucket cumulative count for the publish class.
        let le_100ms = bucket_count(&out, "publish", "0.1024");
        let total = bucket_count(&out, "publish", "+Inf");
        assert_eq!(
            le_100ms, total,
            "a publish dispatch exceeded 100ms while its append was parked — an \
             inline await is back on the loop (#242):\n{out}"
        );

        release.send(true).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), done)
                .await
                .unwrap()
                .unwrap(),
            PublishOutcome::Accepted
        );
    }

    /// Parse one cumulative bucket count from the exposition.
    fn bucket_count(exposition: &str, command: &str, le: &str) -> u64 {
        let needle =
            format!("mqttd_hub_dispatch_seconds_bucket{{le=\"{le}\",command=\"{command}\"}} ");
        exposition
            .lines()
            .find_map(|l| l.strip_prefix(&needle))
            .unwrap_or_else(|| panic!("bucket le={le} missing for {command}:\n{exposition}"))
            .trim()
            .parse()
            .unwrap()
    }

    // --- issue #242, round 2: the online-delivery half (findings A/B/C) --------------

    /// Poll the store's op log until `pred` holds (bounded), for ordering asserts on
    /// off-loop completions.
    async fn await_ops(
        store: &std::sync::Arc<ParkingStore>,
        pred: impl Fn(&[(String, String)]) -> bool,
    ) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if pred(&store.ops()) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "store ops never reached the expected state: {:?}",
                store.ops()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Issue #242 finding B: an UNANSWERABLE brownout-refused delivery (here a
    /// Will) to an online persistent subscriber must not be live-sent past the
    /// subscriber's in-flight appends — it rides the lane as a passthrough, exactly
    /// like the `QoS` 0 branch, because "sending directly would REORDER, which
    /// nothing permits".
    #[tokio::test]
    async fn a_brownout_refused_will_does_not_overtake_an_inflight_qos1_append() {
        let store = ParkingStore::new();
        let release = store.park("p");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "ord/#", QoS::AtLeastOnce);

        // The will-carrying client attaches BEFORE the brownout (a new session
        // would be refused under it).
        let will = Message {
            topic: "ord/z".into(),
            payload: Bytes::from_static(b"the-will"),
            qos: QoS::AtLeastOnce,
            retain: false,
            app: AppProperties::default(),
            expires_at: None,
        };
        let (_w_rx, _) = attach_with_will(&tx, "w", 7, true, will).await;

        // p's QoS 1 append parks in its lane.
        let done = publish_gated(&tx, "ord/a", b"first-qos1", QoS::AtLeastOnce, true);

        // Brownout flips on; the will fires (ungraceful detach). Its durable copy
        // is refused — unanswerable — and its live send must queue BEHIND the
        // in-flight append to the same subscriber.
        tx.send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();
        tx.send(HubCommand::Detach {
            client: ClientId("w".into()),
            conn_id: 7,
            graceful: false,
            session_expiry_override: None,
        })
        .unwrap();

        assert!(
            recv_packet(&mut rx).await.is_none(),
            "the refused will overtook an in-flight QoS 1 append to the same \
             subscriber (#242 finding B)"
        );

        release.send(true).unwrap();
        let p1 = recv_packet(&mut rx).await.expect("the QoS 1 message first");
        assert_eq!(payload_of(&p1), b"first-qos1");
        let p2 = recv_packet(&mut rx).await.expect("the will second");
        assert_eq!(payload_of(&p2), b"the-will");
        assert_eq!(
            timeout(Duration::from_secs(1), done)
                .await
                .unwrap()
                .unwrap(),
            PublishOutcome::Accepted,
            "the pre-flip publish keeps its frozen decision (#238)"
        );
    }

    /// Issue #242 finding C: the detach-time backlog spill runs OFF the loop —
    /// through the session's lane — so a parked store neither wedges the hub nor
    /// lets the spill overtake the session's in-flight appends (which would invert
    /// replay order relative to the pre-motion behaviour).
    #[tokio::test]
    async fn a_detach_spill_rides_the_lane_keeping_the_loop_live_and_replay_order() {
        let store = ParkingStore::new();
        let tx = start_hub_with_arc(store.clone());

        // Receive Maximum 1: the first delivery occupies the only quota slot, so
        // later ones park in the flow-control backlog.
        let (mut rx, _) = attach_full(&tx, "s", 1, false, u32::MAX, 1).await;
        subscribe_qos(&tx, "s", "sp/t", QoS::AtLeastOnce);

        // M0 takes the quota slot (and is never acked).
        assert_eq!(
            publish_gated(&tx, "sp/t", b"m0", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Accepted
        );
        assert_eq!(
            payload_of(&recv_packet(&mut rx).await.expect("m0 delivered")),
            b"m0"
        );

        // M1 is REJECTED by the queue cap (reject-newest): no durable copy, so its
        // backlog entry carries offset=None — the one shape the spill owes a write.
        store.reject_next_enqueue("s", 1);
        assert_eq!(
            publish_gated(&tx, "sp/t", b"m1", QoS::AtLeastOnce, true)
                .await
                .unwrap(),
            PublishOutcome::Accepted
        );

        // M2's durable append parks in s's lane.
        let release = store.park("s");
        let m2_done = publish_gated(&tx, "sp/t", b"m2", QoS::AtLeastOnce, true);

        // Detach: the spill of M1 must not run inline on the loop — s's store is
        // parked, and an inline spill would wedge every client on the node.
        detach(&tx, "s", 1);
        let (ping_tx, ping_rx) = oneshot::channel();
        tx.send(HubCommand::Ping { reply: ping_tx }).unwrap();
        timeout(Duration::from_millis(500), ping_rx)
            .await
            .expect("the detach spill parked the hub loop (#242 finding C)")
            .unwrap();

        release.send(true).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), m2_done)
                .await
                .unwrap()
                .unwrap(),
            PublishOutcome::Accepted
        );
        // The spill serialized BEHIND the in-flight append: store order is the
        // pre-motion (replay) order — m2 then m1, never inverted.
        await_ops(&store, |ops| {
            ops.iter().filter(|(op, _)| op == "enqueue").count() == 3
        })
        .await;
        let enqueues: Vec<String> = store
            .ops()
            .into_iter()
            .filter(|(op, _)| op == "enqueue")
            .map(|(_, d)| d)
            .collect();
        assert_eq!(
            enqueues,
            vec!["s m0".to_string(), "s m2".to_string(), "s m1".to_string()],
            "the detach spill must land AFTER the in-flight append (#242 finding C)"
        );
    }

    /// Issue #242 finding C: a session discarded (zero-expiry detach here; the
    /// expiry sweep shares the same `discard_session`) while one of its appends is
    /// still in flight must not be resurrected by that append — the durable remove
    /// serializes BEHIND it in the lane, so a later persistent attach finds nothing.
    #[tokio::test]
    async fn a_discard_with_an_inflight_append_leaves_no_ghost_queue() {
        let store = ParkingStore::new();
        let release = store.park("s");
        let tx = start_hub_with_arc(store.clone());

        let (_rx, _) = attach(&tx, "s", 1, false).await;
        subscribe_qos(&tx, "s", "gh/t", QoS::AtLeastOnce);
        detach(&tx, "s", 1);

        // The append parks in s's lane.
        let done = publish_gated(&tx, "gh/t", b"ghost", QoS::AtLeastOnce, true);

        // The client reattaches declaring expiry 0 (the session no longer survives
        // a disconnect), then detaches: a zero-expiry discard while the append is
        // still in flight.
        let (_rx2, _) = attach_v5(&tx, "s", 2, false, 0).await;
        detach(&tx, "s", 2);
        // Barrier: the discard is DISPATCHED (racing the still-parked append)
        // before the store is released — the exact window of the ghost race.
        let (ping_tx, ping_rx) = oneshot::channel();
        tx.send(HubCommand::Ping { reply: ping_tx }).unwrap();
        timeout(Duration::from_secs(1), ping_rx)
            .await
            .expect("the discard must not park the loop either (#242 finding C)")
            .unwrap();

        release.send(true).unwrap();
        // Both the admitted append and the discard run to real store outcomes...
        await_ops(&store, |ops| {
            ops.iter().any(|(op, d)| op == "remove" && d == "s")
                && ops.iter().any(|(op, d)| op == "enqueue" && d == "s ghost")
        })
        .await;
        // ...and the discard ran AFTER the admitted append — never before.
        let ops = store.ops();
        let enq = ops
            .iter()
            .position(|(op, d)| op == "enqueue" && d == "s ghost")
            .expect("the admitted append still ran to a store outcome");
        let rem = ops
            .iter()
            .position(|(op, d)| op == "remove" && d == "s")
            .expect("the discard ran");
        assert!(
            enq < rem,
            "the discard overtook an in-flight append — ghost-queue window \
             (#242 finding C): {ops:?}"
        );
        // The publisher's obligation still resolved (the append landed, then the
        // discard emptied the queue — discard beats delivery, as it always has).
        let _ = timeout(Duration::from_secs(1), done).await.unwrap();

        // The acid test: a later persistent attach replays NOTHING.
        let (mut rx3, _) = attach(&tx, "s", 3, false).await;
        assert!(
            recv_packet(&mut rx3).await.is_none(),
            "a ghost message resurrected a deliberately-discarded session \
             (#242 finding C)"
        );
    }

    /// Issue #242 finding A — the head-of-line acceptance criterion for the ONLINE
    /// delivery half: a `QoS` 2 delivery whose ADR 0057 outbound-id record stalls on
    /// one placement group must not delay a publish whose subscriber lives only in
    /// a healthy group. The record is a second lane stage; the loop stays live.
    #[tokio::test]
    async fn a_stalled_record_outbound_does_not_delay_a_group_b_publish() {
        let store = ParkingStore::new();
        let release = store.park_outbound("a");
        let tx = start_hub_with_arc(store.clone());

        let (mut a_rx, _) = attach(&tx, "a", 1, false).await;
        subscribe_qos(&tx, "a", "ga/t", QoS::ExactlyOnce);
        let (_b_rx, _) = attach(&tx, "b", 2, false).await;
        subscribe_qos(&tx, "b", "gb/t", QoS::AtLeastOnce);
        detach(&tx, "b", 2);

        // Group A's QoS 2 delivery: the append lands (publisher acked), the
        // outbound-id record parks in a's lane, the PUBLISH is withheld.
        let a_done = publish_gated(&tx, "ga/t", b"held", QoS::ExactlyOnce, true);
        assert_eq!(
            timeout(Duration::from_millis(500), a_done)
                .await
                .expect("the publisher's ack gates on the APPEND, not the record")
                .unwrap(),
            PublishOutcome::Accepted
        );
        assert!(
            recv_packet(&mut a_rx).await.is_none(),
            "no PUBLISH before its outbound id is durable (ADR 0057)"
        );

        // Group B's full publish round-trip completes while the record is parked.
        let b_done = publish_gated(&tx, "gb/t", b"fast", QoS::AtLeastOnce, true);
        assert_eq!(
            timeout(Duration::from_millis(500), b_done)
                .await
                .expect(
                    "a group-B publish must not wait behind group A's stalled \
                     outbound-id record (#242 finding A)"
                )
                .unwrap(),
            PublishOutcome::Accepted
        );

        // Deferred, not lost: releasing the group releases the delivery.
        release.send(true).unwrap();
        let p = recv_packet(&mut a_rx)
            .await
            .expect("the deferred QoS 2 delivery is released");
        assert_eq!(payload_of(&p), b"held");
    }

    /// ACK-AFTER-DURABLE across the record motion (#124 / ADR 0057, issue #242
    /// finding A): the `QoS` 2 PUBLISH reaches the conn channel only after the
    /// store holds its outbound-id record — the completion handler is the only
    /// send site and it carries the store's own Ok.
    #[tokio::test]
    async fn a_qos2_packet_reaches_the_wire_only_after_its_outbound_id_is_durable() {
        let store = ParkingStore::new();
        let release = store.park_outbound("p");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "w2/t", QoS::ExactlyOnce);

        publish_qos2(&tx, "w2/t", b"gated2");
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "the PUBLISH went on the wire while its outbound-id record was still \
             pending (ADR 0057 broken by the motion)"
        );

        release.send(true).unwrap();
        let packet = recv_packet(&mut rx).await.expect("delivered after durable");
        assert_eq!(payload_of(&packet), b"gated2");
        let pkid = pkid_of(&packet);
        let outbound = store.outbound(&ClientId("p".into())).await.unwrap();
        assert!(
            outbound.iter().any(|e| e.packet_id == pkid),
            "the wire pkid {pkid} is durably recorded before the send"
        );
    }

    /// Issue #242 finding A: while a `QoS` 2 delivery is staged behind its
    /// outbound-id record, a later `QoS` 1 send to the same client diverts into the
    /// backlog — the `records_pending` gate — so per-subscriber wire order holds.
    #[tokio::test]
    async fn a_qos1_send_does_not_overtake_a_pending_qos2_record_to_the_same_client() {
        let store = ParkingStore::new();
        let release = store.park_outbound("p");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "ord2/#", QoS::ExactlyOnce);

        // Warm the durable packet-id block first, so the staging below runs with
        // an empty backlog — the gate under test is `records_pending`, alone.
        publish_qos1(&tx, "ord2/warm", b"m0-warm");
        assert_eq!(
            payload_of(&recv_packet(&mut rx).await.expect("warm-up delivered")),
            b"m0-warm"
        );

        // M1 (QoS 2) then M2 (QoS 1), back to back: M1's record parks — M1 is
        // staged — and M2's completion must divert into the backlog, not overtake.
        publish_qos2(&tx, "ord2/a", b"m1-qos2");
        publish_qos1(&tx, "ord2/b", b"m2-qos1");
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "a QoS 1 send overtook a staged QoS 2 record to the same client \
             (#242 finding A)"
        );

        release.send(true).unwrap();
        let p1 = recv_packet(&mut rx).await.expect("M1 first");
        assert_eq!(payload_of(&p1), b"m1-qos2");
        let p2 = recv_packet(&mut rx).await.expect("M2 second");
        assert_eq!(payload_of(&p2), b"m2-qos1");
    }

    /// ADR 0057's failure arm, relocated with the record stage (issue #242 finding
    /// A): a failed outbound-id write withholds the PUBLISH (never sent under an
    /// unsurvivable id), counts `outbound-id-write-failed`, parks the delivery at
    /// the backlog FRONT, and the next traffic-driven drain delivers it exactly
    /// once, in order.
    #[tokio::test]
    async fn a_failed_outbound_id_write_is_counted_and_the_next_drain_retries() {
        let store = FlakyStore::new(0);
        store
            .fail_outbound
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("test"));
        let (mut hub, tx) = Hub::with_config(
            NodeId("hub-test".into()),
            store.clone() as std::sync::Arc<dyn mqtt_storage::SessionStore>,
        );
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());

        let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
        subscribe_qos(&tx, "c", "f/t", QoS::ExactlyOnce);

        publish_qos2(&tx, "f/t", b"deferred");
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "never sent under an id the store refused (ADR 0057)"
        );
        assert!(
            metrics
                .render()
                .contains("mqttd_publish_dropped_total{reason=\"outbound-id-write-failed\"} 1"),
            "the deferral is counted under its stated reason"
        );

        // Heal; the next publish is the retry clock, and order holds.
        store
            .fail_outbound
            .store(false, std::sync::atomic::Ordering::Relaxed);
        publish_qos2(&tx, "f/t", b"fresh");
        let a = recv_packet(&mut rx).await.expect("the deferred one first");
        assert_eq!(payload_of(&a), b"deferred");
        let b = recv_packet(&mut rx).await.expect("then the fresh one");
        assert_eq!(payload_of(&b), b"fresh");
        assert!(
            recv_packet(&mut rx).await.is_none(),
            "exactly once: the deferred delivery is not duplicated"
        );
    }

    /// Issue #242 finding A: a stalled durable packet-id block reservation
    /// (ADR 0007 T9) defers the delivery to the backlog — the loop stays live for
    /// other groups — and the released reservation still resumes past the durable
    /// high-water (the block invariant survives the off-loop motion).
    #[tokio::test]
    async fn a_stalled_packet_id_reservation_does_not_park_the_loop() {
        let store = ParkingStore::new();
        // Seed a prior owner's reservation BEFORE parking the reserve path.
        let a = ClientId("a".into());
        store.ensure_session(&a).await.unwrap();
        store.reserve_packet_ids(&a, 5000).await.unwrap();
        let release = store.park_reserve("a");
        let tx = start_hub_with_arc(store.clone());

        let (mut a_rx, _) = attach(&tx, "a", 1, false).await;
        subscribe_qos(&tx, "a", "ra/t", QoS::AtLeastOnce);
        let (_b_rx, _) = attach(&tx, "b", 2, false).await;
        subscribe_qos(&tx, "b", "rb/t", QoS::AtLeastOnce);
        detach(&tx, "b", 2);

        // a's first delivery spends the (empty) local block: the reserve parks,
        // the delivery defers, the PUBLISH is withheld.
        let a_done = publish_gated(&tx, "ra/t", b"blocked", QoS::AtLeastOnce, true);
        assert_eq!(
            timeout(Duration::from_millis(500), a_done)
                .await
                .expect("the publisher's ack gates on the append, not the reserve")
                .unwrap(),
            PublishOutcome::Accepted
        );
        assert!(
            recv_packet(&mut a_rx).await.is_none(),
            "no id goes on the wire without a durably reserved block (ADR 0007 T9)"
        );

        // The loop is live: a group-B publish completes during the stall.
        let b_done = publish_gated(&tx, "rb/t", b"fast", QoS::AtLeastOnce, true);
        assert_eq!(
            timeout(Duration::from_millis(500), b_done)
                .await
                .expect(
                    "a group-B publish must not wait behind group A's stalled \
                     packet-id reservation (#242 finding A)"
                )
                .unwrap(),
            PublishOutcome::Accepted
        );

        release.send(true).unwrap();
        let p = recv_packet(&mut a_rx).await.expect("the deferred delivery");
        assert_eq!(payload_of(&p), b"blocked");
        assert!(
            pkid_of(&p) > 5000,
            "packet id {} resumed past the inherited durable high-water",
            pkid_of(&p)
        );
    }

    /// Issue #242 finding A, the ack-handler guard: a PUBCOMP for a packet id that
    /// is still `AwaitingIdRecord` names a PUBLISH the client has never seen — only
    /// a confused or malicious client can send it — and its unconditional
    /// `clear_outbound` would race the lane's in-flight `record_outbound` for the
    /// very same id. It must be ignored; the staged delivery proceeds untouched.
    #[tokio::test]
    async fn a_pubcomp_for_a_staged_unsent_id_is_ignored() {
        let store = ParkingStore::new();
        let release = store.park_outbound("p");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "ig/t", QoS::ExactlyOnce);

        publish_qos2(&tx, "ig/t", b"staged");
        assert!(recv_packet(&mut rx).await.is_none(), "staged, not sent");

        // A fresh session's first allocation is pkid 1 — the id now staged.
        // The forged PUBCOMP must not reach the store's clear path.
        pub_comp(&tx, "p", 1);
        // Barrier: the PUBCOMP dispatch has run.
        let (ping_tx, ping_rx) = oneshot::channel();
        tx.send(HubCommand::Ping { reply: ping_tx }).unwrap();
        timeout(Duration::from_secs(1), ping_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !store.ops().iter().any(|(op, _)| op == "clear"),
            "a PUBCOMP for an unsent staged id reached clear_outbound — it races \
             the in-flight record for the same id (#242 finding A): {:?}",
            store.ops()
        );

        // The staged delivery is untouched: it completes normally.
        release.send(true).unwrap();
        let p = recv_packet(&mut rx)
            .await
            .expect("the staged delivery survives");
        assert_eq!(payload_of(&p), b"staged");
        assert_eq!(pkid_of(&p), 1);
    }

    /// Issue #242 finding A, the reconnect fence: a record completion planned
    /// against a dead connection never sends — the reattach replay owns the durable
    /// copy, so the new connection sees the message exactly once.
    #[tokio::test]
    async fn a_reconnect_mid_record_never_sends_to_the_new_connection() {
        let store = ParkingStore::new();
        let release = store.park_outbound("p");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx1, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "rc/t", QoS::ExactlyOnce);

        publish_qos2(&tx, "rc/t", b"mid-record");
        assert!(
            recv_packet(&mut rx1).await.is_none(),
            "staged behind the parked record"
        );

        // Reconnect while the record is parked: conn 1 dies, conn 2 replays.
        detach(&tx, "p", 1);
        let (mut rx2, present) = attach(&tx, "p", 2, false).await;
        assert!(present, "the persistent session resumes");

        release.send(true).unwrap();
        let packet = recv_packet(&mut rx2)
            .await
            .expect("the durable copy reaches the new connection");
        assert_eq!(payload_of(&packet), b"mid-record");
        assert!(
            recv_packet(&mut rx2).await.is_none(),
            "exactly once: the stale completion (planned for conn 1) must not \
             also send (#242 finding A)"
        );
    }

    /// Issue #242 finding A: a `QoS` 0 passthrough completion must not slip past a
    /// delivery admitted before it went on the wire — a staged record or a deferred
    /// backlog entry. It parks in the same backlog and drains in FIFO order.
    #[tokio::test]
    async fn a_qos0_passthrough_completion_does_not_overtake_a_staged_record() {
        let store = ParkingStore::new();
        let release = store.park_outbound("p");
        let tx = start_hub_with_arc(store.clone());

        let (mut rx, _) = attach(&tx, "p", 1, false).await;
        subscribe_qos(&tx, "p", "ord3/#", QoS::ExactlyOnce);

        // M1 and M2 (QoS 2) stage/queue behind the parked record; M3 (QoS 0) rides
        // the busy lane as a passthrough and must come out LAST.
        publish_qos2(&tx, "ord3/a", b"m1");
        publish_qos2(&tx, "ord3/b", b"m2");
        publish(&tx, "ord3/c", b"m3-qos0");

        assert!(
            recv_packet(&mut rx).await.is_none(),
            "nothing may reach the wire while the first record is parked — least \
             of all the QoS 0 (#242 finding A)"
        );

        release.send(true).unwrap();
        let order: Vec<Vec<u8>> = [
            recv_packet(&mut rx).await.expect("m1"),
            recv_packet(&mut rx).await.expect("m2"),
            recv_packet(&mut rx).await.expect("m3"),
        ]
        .iter()
        .map(|p| payload_of(p).to_vec())
        .collect();
        assert_eq!(
            order,
            vec![b"m1".to_vec(), b"m2".to_vec(), b"m3-qos0".to_vec()],
            "admission order is wire order — the QoS 0 never overtakes a staged \
             QoS 2 (#242 finding A)"
        );
    }

    /// ADR 0073: the ownership-domain capability sweep. The flag rises only when the
    /// operator kept "members" AND every placement member's last-negotiated peer
    /// proto carries the capability; an unknown proto (link not yet handshaken, or a
    /// rolled-back binary) holds the conservative voter domain; transitions are
    /// edge-driven both directions.
    #[tokio::test]
    async fn the_ownership_domain_flag_needs_every_member_capable_and_the_operator_choice() {
        use mqtt_cluster::swim::MemberState;
        use std::sync::atomic::{AtomicBool, Ordering};
        let placement = Arc::new(RwLock::new(Placement::new(
            NodeId("od-local".into()),
            mqtt_cluster::placement::DEFAULT_REPLICAS,
        )));
        placement.write().unwrap().observe(
            &NodeId("od-peer".into()),
            MemberState::Alive,
            "peer:7000",
            None,
        );
        let (mut hub, _tx) = Hub::with_config_and_placement(
            NodeId("od-local".into()),
            Arc::new(MemorySessionStore::new()),
            Some(placement),
        );
        let flag = Arc::new(AtomicBool::new(false));

        // Operator escape hatch: with "voters" chosen, capability never matters.
        hub.set_ownership_domain(flag.clone(), false);
        hub.known_peer_protos.insert(
            NodeId("od-peer".into()),
            mqtt_cluster::peer::PROTO_OWNERSHIP_DOMAIN,
        );
        hub.refresh_ownership_domain();
        assert!(
            !flag.load(Ordering::Relaxed),
            "the escape hatch pins voters"
        );

        // Enabled, but the peer's proto is unknown → conservative false.
        hub.set_ownership_domain(flag.clone(), true);
        hub.known_peer_protos.clear();
        hub.refresh_ownership_domain();
        assert!(
            !flag.load(Ordering::Relaxed),
            "unknown proto reads not-capable"
        );

        // The peer negotiated an OLD proto (a rolled-back binary) → still false.
        hub.known_peer_protos.insert(
            NodeId("od-peer".into()),
            mqtt_cluster::peer::PROTO_OWNERSHIP_DOMAIN - 1,
        );
        hub.refresh_ownership_domain();
        assert!(
            !flag.load(Ordering::Relaxed),
            "an old peer holds the voter domain"
        );

        // Every member capable (self is trivially capable) → the flag rises…
        hub.known_peer_protos.insert(
            NodeId("od-peer".into()),
            mqtt_cluster::peer::PROTO_OWNERSHIP_DOMAIN,
        );
        hub.refresh_ownership_domain();
        assert!(flag.load(Ordering::Relaxed), "all capable: domain expands");

        // …and a not-capable member joining drops it again (rollback correctness).
        hub.known_peer_protos.insert(
            NodeId("od-peer".into()),
            mqtt_cluster::peer::PROTO_OWNERSHIP_DOMAIN - 1,
        );
        hub.refresh_ownership_domain();
        assert!(
            !flag.load(Ordering::Relaxed),
            "a rollback restores the voter domain"
        );
    }
}
