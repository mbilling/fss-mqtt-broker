//! Inter-node ("peer") wire protocol.
//!
//! This is deliberately **separate** from the MQTT client protocol: it carries
//! node-to-node control and data — a `Hello` handshake, subscription interest
//! announcements, and forwarded publishes. Messages are length-prefixed
//! (`u32` big-endian) `postcard` frames (ADR 0052) — except the two ADR 0038
//! **frozen** bootstrap frames ([`PeerMessage::Hello`] and
//! [`PeerMessage::ProxyHello`]), which keep their original pinned byte layout
//! via the hand-rolled [`frozen`] codec: they are read before any protocol
//! version is negotiated, so their bytes can never change.
//!
//! Loop prevention is a protocol invariant enforced by the hub, not the codec: a
//! [`PeerMessage::Publish`] received from a peer is delivered to *local*
//! subscribers only and never re-forwarded (the cluster is a full mesh, so one
//! hop always suffices).

use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};

/// Maximum size of a single peer frame body, to bound memory from a bad peer.
const MAX_FRAME: usize = 16 * 1024 * 1024;

/// The oldest peer-bus protocol version this build can speak (ADR 0038).
///
/// **Release rule (ADR 0039)**: raising this is a MAJOR-release act — it is frozen
/// for the lifetime of a major, so every minor of a major negotiates with every
/// other. A new major sets it to the **gateway minor's** proto (the designated last
/// minor of the previous major, where known upgrade issues are fixed first); that is
/// what makes "upgrade to the gateway before rolling to the next major" fail closed
/// at `Hello` instead of being release-notes prose.
/// Pre-release history: proto 2 (ADR 0042 T7) tagged `ReplicaEntryWire` and
/// `ReplOp::Append` with the writing `(epoch, seq)`; proto 5 (ADR 0043)
/// reshaped [`ReplicaReadReply`](PeerMessage::ReplicaReadReply) with the
/// replica's completeness verdict and added the catch-up frames — incompatible
/// reshapes of the replication frames, so the floor rose with the ceiling both
/// times; proto 6 (ADR 0052) moved the frame body codec from bincode to
/// postcard — every frame's bytes changed except the frozen `Hello` /
/// `ProxyHello`, which keep their ADR 0038 T4 pinned layout so any two builds
/// still discover disagreement politely. Legal exactly because no release
/// exists yet: until 1.0.0 there is no version compatibility to keep, so
/// frames are reshaped in place rather than versioned side-by-side. After the
/// first release this kind of raise is the MAJOR-release act described above,
/// and additive changes ship as new frames under a raised [`PROTO_MAX`] with
/// per-link gating.
pub const PROTO_MIN: u32 = 6;
/// The newest peer-bus protocol version this build can speak (ADR 0038). A link's
/// negotiated version is `min(proto_max_a, proto_max_b)`.
///
/// **Release rule (ADR 0039)**: minors may bump this **additively** — new frames or
/// fields ship under the new proto while every proto back to [`PROTO_MIN`] is still
/// spoken in full. A bump that stops speaking an old proto is really a `PROTO_MIN`
/// raise: a MAJOR release.
pub const PROTO_MAX: u32 = 6;

/// Negotiate a link's protocol version from both sides' announced ranges
/// (ADR 0038): the newest version both can speak, or `None` when the ranges are
/// disjoint — the link must then be rejected (fail closed) rather than half-joined.
#[must_use]
pub fn negotiate_proto(local: (u32, u32), remote: (u32, u32)) -> Option<u32> {
    let candidate = local.1.min(remote.1);
    (candidate >= local.0 && candidate >= remote.0).then_some(candidate)
}

/// One shared-subscription group in a membership snapshot (ADR 0015 §2 wire shape,
/// named per ADR 0038 T4).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedGroupWire {
    /// The share name (the `<name>` in `$share/<name>/<filter>`).
    pub group: String,
    /// The underlying topic filter.
    pub filter: String,
    /// The group's members on the sending node.
    pub members: Vec<SharedMemberWire>,
}

/// One member of a [`SharedGroupWire`]. The per-member liveness lets a peer's
/// selector skip a member that is offline on its home node (ADR 0015 T8).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedMemberWire {
    /// The member's client id.
    pub client: String,
    /// The granted subscription `QoS` as its 2-bit wire value.
    pub qos: u8,
    /// Whether the member is currently online on the sending node.
    pub online: bool,
}

/// The forwardable MQTT 5 application properties carried cross-node (ADR 0030): the
/// publisher's User Properties plus the other message-level properties, so a peer re-emits
/// them to its subscribers exactly as the origin node would (MQTT-3.3.2-17). Mirrors
/// `mqtt_core::AppProperties` in a wire-friendly form (`Vec<u8>` correlation data).
///
/// One struct serves the peer frames and the durable/persistent retained record
/// codecs alike (ADR 0038 T3): it lives in `mqtt_storage` and is re-exported here
/// under the wire name, so the stored and transmitted shapes cannot drift apart.
pub use mqtt_storage::app_props::AppProps as WireAppProps;

/// One retained-snapshot entry (ADR 0037 P5 wire shape, named per ADR 0038 T4): a
/// retained value — or, with an empty payload, a committed clear (tombstone) — with
/// its `(epoch, offset)` convergence token and the publisher's application
/// properties (ADR 0038 T3).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedWireEntry {
    /// The retained topic.
    pub topic: String,
    /// The retained payload; empty = committed clear (tombstone).
    pub payload: Vec<u8>,
    /// The publish `QoS` as its 2-bit wire value.
    pub qos: u8,
    /// The lease epoch the value committed under (token high half); `0` with
    /// `offset 0` marks an uncommitted (durable-off) value.
    pub epoch: u64,
    /// The committed log offset (token low half).
    pub offset: u64,
    /// The publisher's forwardable MQTT 5 application properties.
    pub props: WireAppProps,
    /// Absolute expiry deadline (Unix epoch seconds; issue #227). `None` = never.
    pub expires_at: Option<u64>,
}

/// One stored log entry in a [`ReplicaReadReply`](PeerMessage::ReplicaReadReply)
/// (named per ADR 0038 T4). The wire keeps its own shape rather than reusing the
/// storage crate's `LogEntry`, so that type need not be serde-wire-encodable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaEntryWire {
    /// The entry's offset in the key's log.
    pub offset: u64,
    /// The leadership epoch the entry was delivered under (ADR 0042 T7; proto 2).
    pub epoch: u64,
    /// The leader's per-key write-attempt counter at delivery (ADR 0042 T7;
    /// proto 2). Together with `epoch` this orders every version of an offset,
    /// so the recovery merge resolves conflicts instead of trusting read order.
    pub seq: u64,
    /// The stored record bytes, opaque to the wire.
    pub record: Vec<u8>,
}

/// A message exchanged between broker nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerMessage {
    /// Sent first on a new link to announce the sender's node id and the peer-bus
    /// protocol range it speaks (ADR 0038). Ranges with no overlap reject the link.
    ///
    /// **Frozen frame**: `Hello`'s encoding must never change again — it is the
    /// bootstrap frame any two builds, of any future versions, must be able to
    /// exchange to discover disagreement. Everything after it is versioned.
    Hello {
        /// The sending node's identifier.
        node_id: String,
        /// The oldest protocol version the sender speaks.
        proto_min: u32,
        /// The newest protocol version the sender speaks.
        proto_max: u32,
    },
    /// A full snapshot of the sending node's local subscription interest.
    ///
    /// Replacing the whole set (rather than diffing) keeps the receiver's view
    /// convergent with no add/remove bookkeeping or drift.
    Interest {
        /// Every topic filter that has at least one subscriber on the sender.
        filters: Vec<String>,
    },
    /// A publish forwarded from the sending node for local delivery on the receiver.
    Publish {
        /// Destination topic (no wildcards).
        topic: String,
        /// Application payload.
        payload: Vec<u8>,
        /// Publish `QoS` as its 2-bit wire value (the receiver re-applies its
        /// own per-subscriber downgrade).
        qos: u8,
        /// Whether the message was published with the retain flag. The receiver
        /// stores it as retained too (cross-node replication, ADR 0014).
        retain: bool,
        /// The MQTT 5 Message Expiry Interval (seconds) the publisher set, if any, so
        /// the receiver applies the same deadline to its queued copy rather than
        /// dropping it (ADR 0014 T9). `None` = no expiry.
        message_expiry: Option<u32>,
        /// The publisher's forwardable MQTT 5 application properties (ADR 0030).
        app: WireAppProps,
    },
    /// A full snapshot of the sender's shared-subscription membership (ADR 0015 §2),
    /// so the receiver can select one member per group across the whole cluster.
    /// Sent on the same triggers as [`Interest`](PeerMessage::Interest).
    SharedInterest {
        /// Every shared group with at least one member on the sender.
        groups: Vec<SharedGroupWire>,
    },
    /// A **chunk** of the sender's retained-message set (ADR 0014 §3). Sent on link
    /// establishment when the digest exchange (see
    /// [`RetainedDigest`](PeerMessage::RetainedDigest)) shows the sets differ, split
    /// into bounded chunks so no snapshot can approach the frame limit (0014-T8; one
    /// oversized frame would kill the link on the receiving side, and the link-up
    /// back-fill would then kill every reconnect). Chunks are independent and
    /// idempotent — no ordering or completion marker is needed.
    ///
    /// Each entry carries its `(epoch, offset)` convergence token (ADR 0037 P5): a
    /// receiver with durable retained applies an entry only when its token exceeds
    /// the one held for that topic — divergent caches converge deterministically to
    /// the committed value on link-up, replacing the earlier gap-fill-only rule. A
    /// **committed clear** back-fills as an empty-payload entry with its tombstone's
    /// token, so a peer that missed the clear drops the topic instead of keeping it
    /// forever. Token `(0, 0)` marks an uncommitted (durable-off / pre-migration)
    /// value: it gap-fills an absent topic but never overwrites, and a durable-off
    /// receiver keeps exactly the ADR 0014 gap-fill behaviour.
    RetainedSnapshot {
        /// The entries; an empty payload is a committed clear (tombstone).
        messages: Vec<RetainedWireEntry>,
    },
    /// An order-independent digest of the sender's retained **topic set**, sent on link
    /// establishment instead of the full snapshot (0014-T6). If the receiver's own
    /// digest matches, the sets are identical and nothing is transferred — the common
    /// steady-state link-up (or flap) costs one small frame instead of the whole set.
    /// If it differs, the receiver pulls with
    /// [`RetainedRequest`](PeerMessage::RetainedRequest). Topics only: under gap-fill
    /// the receiver can only ever accept topics it lacks, so payload digests would add
    /// nothing (value divergence is 0014-T7's separate concern).
    RetainedDigest {
        /// Number of retained topics the sender holds.
        count: u64,
        /// XOR of a stable 64-bit hash of each retained topic (order-independent).
        hash: u64,
        /// XOR of a stable 64-bit hash of each retained `(topic, payload, qos)` value
        /// (order-independent; ADR 0037 P1). Equal topic sets with differing value hashes
        /// mean **divergence** — same topics, different values — which triggers a pull so
        /// the receiver can detect and count it (`retained_divergence_total`) and, under
        /// durable retained, resolve it by token from the pulled snapshot (ADR 0037 P5).
        value_hash: u64,
    },
    /// Pull the sender's retained set (sent back when a received
    /// [`RetainedDigest`](PeerMessage::RetainedDigest) did not match the local set);
    /// answered with chunked [`RetainedSnapshot`](PeerMessage::RetainedSnapshot)s.
    RetainedRequest,
    /// A targeted shared-subscription delivery (ADR 0015 §1): the sending node chose
    /// this `client` (a member on the receiver) for a shared group; the receiver
    /// delivers to exactly that client, with no further selection.
    SharedDeliver {
        /// The chosen group member on the receiving node.
        client: String,
        /// Destination topic (no wildcards).
        topic: String,
        /// Application payload.
        payload: Vec<u8>,
        /// Already-downgraded delivery `QoS` as its 2-bit wire value.
        qos: u8,
        /// The MQTT 5 Message Expiry Interval (seconds) the publisher set, if any, so the
        /// receiver applies the same deadline to a queued copy (ADR 0015 T7). `None` = none.
        message_expiry: Option<u32>,
        /// The publisher's forwardable MQTT 5 application properties (ADR 0030).
        app: WireAppProps,
    },
    /// First frame of a **session proxy** (ADR 0005): instead of a peer link,
    /// **Frozen frame** (ADR 0038): like [`Hello`](PeerMessage::Hello), this is a
    /// bootstrap frame read before any version is negotiated; its encoding must
    /// never change again. (The raw MQTT stream that follows carries its own
    /// protocol versioning.)
    ///
    /// this connection relocates a persistent client session to its placement
    /// owner. The remaining bytes on the connection are the raw MQTT stream of
    /// the proxied client, which the owner serves as a normal session.
    ///
    /// The connection arrived over the mutually-authenticated cluster bus, so
    /// the sending node is a verified mesh member; `identity` is the client
    /// identity that node **vouches** it already authenticated. The owner trusts
    /// it within the cluster-CA boundary and records the vouching node.
    ProxyHello {
        /// The vouched, already-authenticated client identity (its subject),
        /// or `None` if the client connected anonymously.
        identity: Option<String>,
        /// The id of the landing node relaying (vouching for) this session — the
        /// owner records it for audit attribution. `None` if unidentified.
        via: Option<String>,
    },
    /// A session-log replication op from a placement group's lease-holder to one of
    /// its replicas (ADR 0006 §1, workstream E step 3b). The `epoch` is the
    /// holder's leadership term; the replica fences a stale holder by rejecting an
    /// epoch below the one it has acknowledged. `req_id` correlates the
    /// [`ReplicateAck`](PeerMessage::ReplicateAck) the replica returns.
    Replicate {
        /// Correlates this request with its ack on the same link.
        req_id: u64,
        /// The lease-holder's leadership epoch (fence token).
        epoch: crate::lease::Epoch,
        /// The operation to apply (append / truncate / remove).
        op: crate::cluster_log::ReplOp,
    },
    /// A replica's response to a [`Replicate`](PeerMessage::Replicate): whether it
    /// accepted the op (`false` = fenced at a stale epoch). The lease-holder counts
    /// accepts to decide quorum durability.
    ReplicateAck {
        /// The `req_id` of the [`Replicate`](PeerMessage::Replicate) being answered.
        req_id: u64,
        /// Whether the replica applied the op (`false` if fenced).
        accepted: bool,
    },
    /// An ownership-lease consensus (openraft) RPC carried over the peer bus
    /// (ADR 0006 §1, workstream E step 3b-ii mesh network). The codec treats
    /// `payload` as opaque — it is a serialized Raft RPC, encoded/decoded by
    /// `raft_mesh`. `req_id` correlates the [`RaftRpcReply`](PeerMessage::RaftRpcReply).
    RaftRpc {
        /// Correlates this request with its reply on the same link.
        req_id: u64,
        /// The serialized Raft RPC (append-entries / vote / install-snapshot).
        payload: Vec<u8>,
    },
    /// The reply to a [`RaftRpc`](PeerMessage::RaftRpc): the serialized RPC response.
    RaftRpcReply {
        /// The `req_id` of the [`RaftRpc`](PeerMessage::RaftRpc) being answered.
        req_id: u64,
        /// The serialized Raft RPC response.
        payload: Vec<u8>,
    },
    /// A new owner's request to read a replica's stored log for `key`, to rebuild
    /// the committed log on takeover (workstream F). Answered with
    /// [`ReplicaReadReply`](PeerMessage::ReplicaReadReply).
    ReplicaRead {
        /// Correlates this request with its reply on the same link.
        req_id: u64,
        /// The log (session key) to read.
        key: String,
    },
    /// The reply to a [`ReplicaRead`](PeerMessage::ReplicaRead): the replica's stored
    /// entries for the key, its truncation low-water so a recovery cannot
    /// resurrect an already-acked prefix (ADR 0018 §3b), and its **completeness**
    /// verdict (ADR 0043 P1) — a recovery merge requires at least one complete
    /// copy, so a hollow joiner (entries above a hole it never received)
    /// contributes data but never authority.
    ReplicaReadReply {
        /// The `req_id` of the [`ReplicaRead`](PeerMessage::ReplicaRead) answered.
        req_id: u64,
        /// The replica's truncation low-water for the key.
        watermark: u64,
        /// Whether the replica's copy is gap-free from its low-water to its tail
        /// AND stamped caught-up for the key's group.
        complete: bool,
        /// The stored entries, in offset order.
        entries: Vec<ReplicaEntryWire>,
    },
    /// A retained mutation routed to the topic's placement-group lease-owner
    /// (ADR 0037 §1): the sender is the node the publish landed on, the receiver owns
    /// the topic's group and commits the mutation into the durable retained keyspace.
    /// Live delivery already happened on the sender; this frame carries only the
    /// *authority* write. A zero-length payload is the MQTT clear [MQTT-3.3.1-10],
    /// committed as a versioned tombstone.
    ///
    /// **Acknowledged** (ADR 0037 T8): the sender keeps the mutation until the owner
    /// answers with [`RetainedCommitAck`](PeerMessage::RetainedCommitAck), and
    /// retransmits (same `seq`) if no answer arrives — so a frame lost to a dying
    /// link is retried instead of silently lost. `seq` is a per-sender monotonic
    /// counter; the owner dedups on it, making retransmission idempotent.
    RetainedCommit {
        /// Destination topic (no wildcards).
        topic: String,
        /// The retained payload; empty = clear (versioned tombstone).
        payload: Vec<u8>,
        /// The publish `QoS` as its 2-bit wire value.
        qos: u8,
        /// The publisher's forwardable MQTT 5 application properties (ADR 0038 T3),
        /// committed into the durable record so any node's replay carries them.
        props: WireAppProps,
        /// Per-sender monotonic handoff sequence (dedup key for retransmissions).
        seq: u64,
        /// Absolute expiry deadline (Unix epoch seconds; issue #227), committed with
        /// the value. `None` = never.
        expires_at: Option<u64>,
    },
    /// The post-commit retained fan-out (ADR 0037 §3): the topic's group owner
    /// broadcasts every **committed** retained value with its `(epoch, offset)`
    /// convergence token; each node's local cache applies it only when the token
    /// exceeds the one it holds for the topic — monotonic per topic, idempotent,
    /// order-insensitive. This replaces the raw ADR 0014 broadcast as the cache
    /// warmer when durable retained is on. A zero-length payload is a committed
    /// clear (versioned tombstone): the cache drops the topic but its token still
    /// fences out any staler value.
    RetainedUpdate {
        /// The committed topic.
        topic: String,
        /// The committed payload; empty = cleared (tombstone).
        payload: Vec<u8>,
        /// The publish `QoS` as its 2-bit wire value.
        qos: u8,
        /// The lease epoch the value committed under (token high half).
        epoch: u64,
        /// The committed log offset (token low half).
        offset: u64,
        /// The committed application properties (ADR 0038 T3), applied to the cache
        /// with the value so a replay from any node carries them.
        props: WireAppProps,
        /// The committed absolute expiry deadline (issue #227). `None` = never.
        expires_at: Option<u64>,
    },
    /// The owner's **commit-gated** answer to a
    /// [`RetainedCommit`](PeerMessage::RetainedCommit) (ADR 0037 T8). Sent only once
    /// the mutation is quorum-committed (`token = Some`), or as a NACK
    /// (`token = None`) when the receiver no longer owns the topic's group — the
    /// sender then re-resolves the owner from placement and resends. Never sent
    /// optimistically: an ack means the write is durable.
    RetainedCommitAck {
        /// The `seq` of the [`RetainedCommit`](PeerMessage::RetainedCommit) answered.
        seq: u64,
        /// `Some((epoch, offset))` = committed with this token; `None` = not the
        /// owner (re-route).
        token: Option<(u64, u64)>,
    },
    /// An **acknowledged** cross-node publish forward (ADR 0042 T9, exhibit ⑤;
    /// proto 3). Semantically a [`Publish`](PeerMessage::Publish), but the sender
    /// holds the publisher's `QoS` 1 acknowledgement until the receiver answers
    /// with [`PublishAck`](PeerMessage::PublishAck) — sent only once the
    /// receiver's local fan-out, **including any durable offline enqueue**, has
    /// completed. Unanswered forwards are retransmitted (same `seq`); the
    /// receiver does not dedup — a duplicate delivery is legal at `QoS` 1
    /// (at-least-once), so retransmission needs no receiver state.
    PublishAcked {
        /// Per-sender monotonic forward sequence (correlates the ack).
        seq: u64,
        /// Destination topic (no wildcards).
        topic: String,
        /// Application payload.
        payload: Vec<u8>,
        /// Publish `QoS` as its 2-bit wire value (the receiver re-applies its
        /// own per-subscriber downgrade).
        qos: u8,
        /// Whether the message was published with the retain flag (the receiver
        /// applies its ADR 0014/0037 retained rules exactly as for `Publish`).
        retain: bool,
        /// The publisher's Message Expiry Interval (seconds), if any.
        message_expiry: Option<u32>,
        /// The publisher's forwardable MQTT 5 application properties.
        app: WireAppProps,
    },
    /// The durability-gated answer to a
    /// [`PublishAcked`](PeerMessage::PublishAcked) (ADR 0042 T9; proto 3). `ok`
    /// is `true` once the receiver's local fan-out and durable enqueues
    /// completed; `false` reports a terminal durable-append failure — the sender
    /// withholds the publisher's acknowledgement (the publisher retries).
    PublishAck {
        /// The `seq` of the [`PublishAcked`](PeerMessage::PublishAcked) answered.
        seq: u64,
        /// Whether the receiver durably owns the forwarded copy.
        ok: bool,
    },
    /// A request for every replicated log key the receiver holds locally
    /// (ADR 0042 T9, exhibit ⑥; proto 3). A new owner enumerating a group's
    /// sessions cannot rely on its own replica copies alone — quorum appends
    /// mean any single node may lack a key — so the takeover scan unions the
    /// key sets of the whole replica mesh before quorum-recovering each key.
    /// Key NAMES only; values still travel via
    /// [`ReplicaRead`](PeerMessage::ReplicaRead) quorum recovery.
    ReplicaKeys {
        /// Correlates this request with its reply on the same link.
        req_id: u64,
    },
    /// The reply to a [`ReplicaKeys`](PeerMessage::ReplicaKeys): the keys the
    /// replica holds locally.
    ReplicaKeysReply {
        /// The `req_id` of the [`ReplicaKeys`](PeerMessage::ReplicaKeys) answered.
        req_id: u64,
        /// Every replicated log key in the sender's local replica store.
        keys: Vec<String>,
    },
    /// A hollow replica's request that `key`'s group **owner** re-commit the
    /// key's committed log (ADR 0043 P1; proto 5): the owner re-delivers every
    /// committed entry through the normal fenced replication path (idempotent —
    /// replicas keep the highest `(epoch, seq)` version per offset), closing the
    /// requester's gap. Fire-and-forget: the requester's catch-up sweep retries
    /// while its copy stays incomplete. A non-owner receiver ignores it (the
    /// sweep re-resolves the owner from placement on the next pass).
    ReplicaCatchUp {
        /// The log key to re-commit.
        key: String,
    },
    /// A decommissioning node's request that `key`'s group **owner** re-commit
    /// the key's committed log to one **specific** node (ADR 0043 P3; proto 5):
    /// the drain hands its groups' data to the post-departure replica set, whose
    /// newcomers are not yet in the owner's fan-out. The owner re-delivers every
    /// committed entry (re-tagged at its epoch, like a catch-up re-commit) plus
    /// its truncation floor to `target` only — additive and idempotent; the
    /// drain verifies by reading the target back and re-asks until content-
    /// complete. A non-owner receiver ignores it.
    ReplicaCatchUpTo {
        /// The log key to re-commit.
        key: String,
        /// The node (its id) to re-commit the key to.
        target: String,
    },
}

/// The hand-rolled codec for the two ADR 0038 **frozen** bootstrap frames.
///
/// [`PeerMessage::Hello`] and [`PeerMessage::ProxyHello`] are decoded *before*
/// any protocol version is negotiated, and their encodings are pinned byte for
/// byte (the `the_frozen_frames_encode_byte_for_byte_stably` test) — so when
/// the body codec moved to postcard (ADR 0052), these two frames could not
/// move with it. Their pinned layout — a `u32`-LE variant tag, `u64`-LE
/// length-prefixed strings, one-byte `Option` tags — is reproduced here by
/// hand; it happens to be the layout the original codec produced, but the
/// bytes are the contract, not any library.
///
/// The decoders are strict: every inner length is checked against the
/// remaining body, strings must be valid UTF-8, and the body must be consumed
/// exactly — a frame that lies about a length is malformed, never a panic or
/// an over-read.
mod frozen {
    use super::PeerMessage;

    /// First byte of a frozen frame body (the low byte of the `u32`-LE variant
    /// tag). The other three tag bytes are zero.
    pub(super) const HELLO_TAG: u8 = 0;
    /// See [`HELLO_TAG`].
    pub(super) const PROXY_HELLO_TAG: u8 = 8;

    fn put_str(out: &mut Vec<u8>, s: &str) {
        out.extend_from_slice(&(s.len() as u64).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }

    fn put_opt_str(out: &mut Vec<u8>, s: Option<&str>) {
        match s {
            None => out.push(0),
            Some(s) => {
                out.push(1);
                put_str(out, s);
            }
        }
    }

    pub(super) fn encode_hello(node_id: &str, proto_min: u32, proto_max: u32) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + 8 + node_id.len() + 8);
        out.extend_from_slice(&u32::from(HELLO_TAG).to_le_bytes());
        put_str(&mut out, node_id);
        out.extend_from_slice(&proto_min.to_le_bytes());
        out.extend_from_slice(&proto_max.to_le_bytes());
        out
    }

    pub(super) fn encode_proxy_hello(identity: Option<&str>, via: Option<&str>) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&u32::from(PROXY_HELLO_TAG).to_le_bytes());
        put_opt_str(&mut out, identity);
        put_opt_str(&mut out, via);
        out
    }

    /// A bounds-checked cursor over a frozen frame body.
    struct Reader<'a>(&'a [u8]);

    impl<'a> Reader<'a> {
        fn take(&mut self, n: usize) -> Option<&'a [u8]> {
            (n <= self.0.len()).then(|| {
                let (head, rest) = self.0.split_at(n);
                self.0 = rest;
                head
            })
        }

        fn u32_le(&mut self) -> Option<u32> {
            self.take(4)
                .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        }

        fn string(&mut self) -> Option<String> {
            let len = self
                .take(8)
                .map(|b| u64::from_le_bytes(b.try_into().unwrap()))?;
            let len = usize::try_from(len).ok()?;
            let bytes = self.take(len)?;
            String::from_utf8(bytes.to_vec()).ok()
        }

        // The nesting is the point: outer `None` = malformed frame, inner
        // `Option` = the wire field's own optionality.
        #[allow(clippy::option_option)]
        fn opt_string(&mut self) -> Option<Option<String>> {
            match self.take(1)? {
                [0] => Some(None),
                [1] => self.string().map(Some),
                _ => None,
            }
        }

        fn finish(self) -> Option<()> {
            self.0.is_empty().then_some(())
        }
    }

    /// Decode a frozen frame body (including its 4-byte variant tag).
    /// `None` = malformed (fail closed).
    pub(super) fn decode(body: &[u8]) -> Option<PeerMessage> {
        let mut r = Reader(body);
        let msg = match r.u32_le()? {
            tag if tag == u32::from(HELLO_TAG) => PeerMessage::Hello {
                node_id: r.string()?,
                proto_min: r.u32_le()?,
                proto_max: r.u32_le()?,
            },
            tag if tag == u32::from(PROXY_HELLO_TAG) => PeerMessage::ProxyHello {
                identity: r.opt_string()?,
                via: r.opt_string()?,
            },
            _ => return None,
        };
        r.finish()?;
        Some(msg)
    }
}

/// Errors from peer-frame coding.
#[derive(Debug, thiserror::Error)]
pub enum PeerCodecError {
    /// The frame body could not be (de)serialized.
    #[error("peer frame serialization error: {0}")]
    Serde(String),
    /// A peer announced a frame larger than [`MAX_FRAME`].
    #[error("peer frame exceeds maximum size")]
    FrameTooLarge,
}

/// Encode a message as a length-prefixed frame appended to `out`.
///
/// # Errors
/// Returns [`PeerCodecError::Serde`] if serialization fails.
pub fn encode(msg: &PeerMessage, out: &mut Vec<u8>) -> Result<(), PeerCodecError> {
    let body = match msg {
        // The two frozen bootstrap frames keep their pinned pre-negotiation
        // bytes (ADR 0038 T4) — see [`frozen`].
        PeerMessage::Hello {
            node_id,
            proto_min,
            proto_max,
        } => frozen::encode_hello(node_id, *proto_min, *proto_max),
        PeerMessage::ProxyHello { identity, via } => {
            frozen::encode_proxy_hello(identity.as_deref(), via.as_deref())
        }
        msg => postcard::to_allocvec(msg).map_err(|e| PeerCodecError::Serde(e.to_string()))?,
    };
    // Enforce the frame bound on the SENDING side too: an oversized frame would not
    // fail here but on the receiver, which tears down the link — and a sender that
    // retries on reconnect (e.g. a link-up back-fill) would then kill the link in a
    // loop. Failing the send keeps the link (and every other message on it) alive.
    if body.len() > MAX_FRAME {
        return Err(PeerCodecError::FrameTooLarge);
    }
    let len = u32::try_from(body.len()).map_err(|_| PeerCodecError::FrameTooLarge)?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&body);
    Ok(())
}

/// Try to decode one frame from the front of `buf`, consuming it on success.
///
/// Returns `Ok(None)` if `buf` does not yet hold a complete frame.
///
/// # Errors
/// [`PeerCodecError::FrameTooLarge`] or [`PeerCodecError::Serde`] on a bad frame.
pub fn decode(buf: &mut BytesMut) -> Result<Option<PeerMessage>, PeerCodecError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > MAX_FRAME {
        return Err(PeerCodecError::FrameTooLarge);
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    buf.advance(4);
    let body = buf.split_to(len);
    // Dispatch on the first byte: the frozen bootstrap frames carry a u32-LE
    // variant tag whose low byte is 0 (Hello) or 8 (ProxyHello); postcard
    // bodies start with the variant index as a varint, and the encoder never
    // emits variants 0 or 8 through postcard, so the spaces are disjoint.
    let msg = match body.first() {
        Some(&frozen::HELLO_TAG | &frozen::PROXY_HELLO_TAG) => frozen::decode(&body)
            .ok_or_else(|| PeerCodecError::Serde("malformed frozen bootstrap frame".into()))?,
        _ => match postcard::take_from_bytes(&body) {
            // Strict: a valid message followed by trailing bytes is a
            // malformed frame, not a message plus slack.
            Ok((msg, [])) => msg,
            Ok(_) => {
                return Err(PeerCodecError::Serde(
                    "trailing bytes after peer frame body".into(),
                ))
            }
            Err(e) => return Err(PeerCodecError::Serde(e.to_string())),
        },
    };
    Ok(Some(msg))
}

#[cfg(test)]
mod tests {
    use super::{
        decode, encode, negotiate_proto, PeerCodecError, PeerMessage, ReplicaEntryWire,
        RetainedWireEntry, SharedGroupWire, SharedMemberWire, WireAppProps, MAX_FRAME, PROTO_MAX,
        PROTO_MIN,
    };
    use bytes::BytesMut;

    fn roundtrip(msg: &PeerMessage) {
        let mut out = Vec::new();
        encode(msg, &mut out).unwrap();
        let mut buf = BytesMut::from(&out[..]);
        assert_eq!(decode(&mut buf).unwrap().as_ref(), Some(msg));
        assert!(buf.is_empty());
    }

    // One roundtrip per wire variant — the length tracks the enum, not complexity.
    #[allow(clippy::too_many_lines)]
    #[test]
    fn roundtrips_all_variants() {
        roundtrip(&PeerMessage::Hello {
            node_id: "node-a".into(),
            proto_min: PROTO_MIN,
            proto_max: PROTO_MAX,
        });
        roundtrip(&PeerMessage::Interest {
            filters: vec!["a/#".into(), "b/+/c".into()],
        });
        roundtrip(&PeerMessage::Publish {
            topic: "sensors/temp".into(),
            payload: b"21.5C".to_vec(),
            qos: 1,
            retain: false,
            message_expiry: Some(30),
            app: WireAppProps {
                payload_format: Some(1),
                content_type: Some("text/plain".into()),
                response_topic: Some("resp/x".into()),
                correlation_data: Some(b"\x00corr".to_vec()),
                user_properties: vec![("trace".into(), "abc".into()), ("hop".into(), "1".into())],
            },
        });
        roundtrip(&PeerMessage::SharedInterest {
            groups: vec![SharedGroupWire {
                group: "grp".into(),
                filter: "t/+".into(),
                members: vec![
                    SharedMemberWire {
                        client: "c1".into(),
                        qos: 1,
                        online: true,
                    },
                    SharedMemberWire {
                        client: "c2".into(),
                        qos: 0,
                        online: false,
                    },
                ],
            }],
        });
        roundtrip(&PeerMessage::SharedDeliver {
            client: "c1".into(),
            topic: "t/x".into(),
            payload: b"hi".to_vec(),
            qos: 2,
            message_expiry: None,
            app: WireAppProps {
                user_properties: vec![("k".into(), "v".into())],
                ..Default::default()
            },
        });
        roundtrip(&PeerMessage::RetainedSnapshot {
            messages: vec![
                RetainedWireEntry {
                    topic: "t/a".into(),
                    payload: b"v".to_vec(),
                    qos: 1,
                    epoch: 7,
                    offset: 42,
                    expires_at: Some(1_755_000_000),
                    props: WireAppProps {
                        content_type: Some("application/cbor".into()),
                        user_properties: vec![("origin".into(), "n1".into())],
                        ..Default::default()
                    },
                },
                RetainedWireEntry {
                    topic: "$SYS/x".into(),
                    payload: b"w".to_vec(),
                    ..Default::default()
                },
                RetainedWireEntry {
                    topic: "t/cleared".into(), // a committed clear
                    epoch: 7,
                    offset: 43,
                    ..Default::default()
                },
            ],
        });
        roundtrip(&PeerMessage::RetainedDigest {
            count: 42,
            hash: 0xdead_beef_cafe_f00d,
            value_hash: 0x0123_4567_89ab_cdef,
        });
        roundtrip(&PeerMessage::RetainedRequest);
        roundtrip(&PeerMessage::ProxyHello {
            identity: Some("device-7".into()),
            via: Some("node-a".into()),
        });
        roundtrip(&PeerMessage::ProxyHello {
            identity: None,
            via: None,
        });
        roundtrip(&PeerMessage::Replicate {
            req_id: 42,
            epoch: 7,
            op: crate::cluster_log::ReplOp::Append {
                key: "client-x".into(),
                offset: 3,
                seq: 3,
                record: b"payload".to_vec(),
            },
        });
        roundtrip(&PeerMessage::ReplicateAck {
            req_id: 42,
            accepted: true,
        });
        roundtrip(&PeerMessage::RaftRpc {
            req_id: 7,
            payload: vec![1, 2, 3, 4],
        });
        roundtrip(&PeerMessage::RaftRpcReply {
            req_id: 7,
            payload: vec![9, 8, 7],
        });
        roundtrip(&PeerMessage::ReplicaRead {
            req_id: 3,
            key: "q/client-x".into(),
        });
        roundtrip(&PeerMessage::ReplicaReadReply {
            req_id: 3,
            watermark: 4,
            complete: true,
            entries: vec![
                ReplicaEntryWire {
                    offset: 1,
                    epoch: 7,
                    seq: 1,
                    record: vec![1, 2],
                },
                ReplicaEntryWire {
                    offset: 2,
                    epoch: 7,
                    seq: 2,
                    record: vec![3, 4],
                },
            ],
        });
        roundtrip(&PeerMessage::RetainedCommit {
            topic: "dev/1/state".into(),
            payload: b"open".to_vec(),
            qos: 1,
            props: WireAppProps {
                payload_format: Some(1),
                content_type: Some("application/json".into()),
                ..Default::default()
            },
            seq: 9,
            expires_at: Some(1_755_000_000),
        });
        roundtrip(&PeerMessage::RetainedCommit {
            topic: "dev/1/state".into(),
            payload: Vec::new(), // a clear (versioned tombstone)
            qos: 0,
            props: WireAppProps::default(),
            seq: 10,
            expires_at: None,
        });
        roundtrip(&PeerMessage::RetainedCommitAck {
            seq: 9,
            token: Some((7, 42)),
        });
        roundtrip(&PeerMessage::RetainedCommitAck {
            seq: 10,
            token: None, // NACK: not the owner, re-route
        });
        roundtrip(&PeerMessage::RetainedUpdate {
            topic: "dev/1/state".into(),
            payload: b"open".to_vec(),
            qos: 1,
            epoch: 7,
            offset: 42,
            props: WireAppProps {
                response_topic: Some("replies/dev1".into()),
                correlation_data: Some(vec![1, 2]),
                ..Default::default()
            },
            expires_at: Some(1_755_000_000),
        });
        roundtrip(&PeerMessage::RetainedUpdate {
            topic: "dev/1/state".into(),
            payload: Vec::new(), // a committed clear
            qos: 0,
            epoch: 7,
            offset: 43,
            props: WireAppProps::default(),
            expires_at: None,
        });
    }

    /// ADR 0038 T4: the two **frozen** frames' encodings, pinned byte for byte.
    /// `Hello` and `ProxyHello` are the bootstrap frames any two builds, of any
    /// versions, must exchange before a protocol is negotiated — if this test
    /// fails, the change is a cross-version wire break that no proto bump can
    /// carry, not something to fix by updating the expected bytes. (bincode
    /// encodes the enum variant *index*, so this also pins the rule that new
    /// frames are APPENDED to [`PeerMessage`], never inserted or reordered.)
    #[test]
    fn the_frozen_frames_encode_byte_for_byte_stably() {
        let mut hello = Vec::new();
        encode(
            &PeerMessage::Hello {
                node_id: "n1".into(),
                proto_min: 1,
                proto_max: 1,
            },
            &mut hello,
        )
        .unwrap();
        #[rustfmt::skip]
        let expected_hello = [
            0, 0, 0, 22,                          // frame length (u32 BE)
            0, 0, 0, 0,                           // variant index: Hello = 0
            2, 0, 0, 0, 0, 0, 0, 0, b'n', b'1',   // node_id (u64 LE len + bytes)
            1, 0, 0, 0,                           // proto_min (u32 LE)
            1, 0, 0, 0,                           // proto_max (u32 LE)
        ];
        assert_eq!(hello, expected_hello);

        let mut proxy = Vec::new();
        encode(
            &PeerMessage::ProxyHello {
                identity: Some("client-a".into()),
                via: Some("node-b".into()),
            },
            &mut proxy,
        )
        .unwrap();
        #[rustfmt::skip]
        let expected_proxy = [
            0, 0, 0, 36,                          // frame length (u32 BE)
            8, 0, 0, 0,                           // variant index: ProxyHello = 8
            1,                                    // identity: Some
            8, 0, 0, 0, 0, 0, 0, 0,               // identity length (u64 LE)
            b'c', b'l', b'i', b'e', b'n', b't', b'-', b'a',
            1,                                    // via: Some
            6, 0, 0, 0, 0, 0, 0, 0,               // via length (u64 LE)
            b'n', b'o', b'd', b'e', b'-', b'b',
        ];
        assert_eq!(proxy, expected_proxy);
    }

    /// A frozen-layout frame that lies about an inner length (a `node_id`
    /// length larger than the remaining body) is rejected as malformed —
    /// never an over-read or a panic (ADR 0052 strict decoders).
    #[test]
    fn a_frozen_frame_with_a_lying_inner_length_is_rejected() {
        #[rustfmt::skip]
        let mut lying = BytesMut::from(&[
            0, 0, 0, 14,                              // frame length (u32 BE)
            0, 0, 0, 0,                               // variant tag: Hello
            255, 0, 0, 0, 0, 0, 0, 0, b'n', b'1',     // node_id CLAIMS 255 bytes
        ][..]);
        assert!(matches!(decode(&mut lying), Err(PeerCodecError::Serde(_))));

        // A frozen body with valid fields but trailing garbage is also malformed.
        let mut trailing = Vec::new();
        encode(
            &PeerMessage::Hello {
                node_id: "n1".into(),
                proto_min: 1,
                proto_max: 1,
            },
            &mut trailing,
        )
        .unwrap();
        let body_len = trailing.len() - 4;
        trailing.push(0xAA); // one byte past the pinned layout
        trailing[..4].copy_from_slice(&u32::try_from(body_len + 1).unwrap().to_be_bytes());
        let mut buf = BytesMut::from(&trailing[..]);
        assert!(matches!(decode(&mut buf), Err(PeerCodecError::Serde(_))));
    }

    /// Trailing bytes after a valid postcard body are a malformed frame
    /// (ADR 0052): the decoder consumes the body exactly or rejects it.
    #[test]
    fn trailing_bytes_after_a_postcard_body_are_rejected() {
        let mut out = Vec::new();
        encode(&PeerMessage::RetainedRequest, &mut out).unwrap();
        let body_len = out.len() - 4;
        out.push(0x00);
        out[..4].copy_from_slice(&u32::try_from(body_len + 1).unwrap().to_be_bytes());
        let mut buf = BytesMut::from(&out[..]);
        assert!(matches!(decode(&mut buf), Err(PeerCodecError::Serde(_))));
    }

    /// ADR 0038: version negotiation picks the newest version both sides speak,
    /// and disjoint ranges yield `None` — the caller rejects the link, fail closed.
    #[test]
    fn proto_negotiation_picks_the_newest_common_version_or_rejects() {
        // Identical single-version builds (today's fleet).
        assert_eq!(negotiate_proto((1, 1), (1, 1)), Some(1));
        // Overlapping ranges: newest common wins.
        assert_eq!(negotiate_proto((1, 3), (2, 5)), Some(3));
        assert_eq!(negotiate_proto((2, 5), (1, 3)), Some(3));
        // Touching at one version.
        assert_eq!(negotiate_proto((1, 2), (2, 4)), Some(2));
        // Disjoint: an old build meets a too-new build (or vice versa).
        assert_eq!(negotiate_proto((1, 1), (2, 3)), None);
        assert_eq!(negotiate_proto((4, 6), (1, 3)), None);
        // This build's own constants form a valid range.
        assert_eq!(
            negotiate_proto((PROTO_MIN, PROTO_MAX), (PROTO_MIN, PROTO_MAX)),
            Some(PROTO_MAX)
        );
    }

    /// The frame bound is enforced on the SENDING side (0014-T8): a message that
    /// would exceed [`MAX_FRAME`] fails `encode` instead of being written and
    /// killing the link at the receiver.
    #[test]
    fn an_oversized_frame_is_rejected_at_encode() {
        let msg = PeerMessage::RetainedSnapshot {
            messages: vec![RetainedWireEntry {
                topic: "t".into(),
                payload: vec![0u8; MAX_FRAME + 1],
                ..Default::default()
            }],
        };
        let mut out = Vec::new();
        assert!(matches!(
            encode(&msg, &mut out),
            Err(PeerCodecError::FrameTooLarge)
        ));
        assert!(
            out.is_empty(),
            "nothing may be emitted for a rejected frame"
        );
    }

    #[test]
    fn partial_frame_returns_none() {
        let mut out = Vec::new();
        encode(
            &PeerMessage::Hello {
                node_id: "x".into(),
                proto_min: PROTO_MIN,
                proto_max: PROTO_MAX,
            },
            &mut out,
        )
        .unwrap();
        let mut buf = BytesMut::new();
        for &b in &out[..out.len() - 1] {
            buf.extend_from_slice(&[b]);
            assert_eq!(decode(&mut buf).unwrap(), None);
        }
        buf.extend_from_slice(&[out[out.len() - 1]]);
        assert!(decode(&mut buf).unwrap().is_some());
    }

    #[test]
    fn two_frames_in_one_buffer() {
        let mut out = Vec::new();
        encode(
            &PeerMessage::Hello {
                node_id: "a".into(),
                proto_min: PROTO_MIN,
                proto_max: PROTO_MAX,
            },
            &mut out,
        )
        .unwrap();
        encode(
            &PeerMessage::Publish {
                topic: "t".into(),
                payload: vec![1, 2, 3],
                qos: 0,
                retain: false,
                message_expiry: None,
                app: WireAppProps::default(),
            },
            &mut out,
        )
        .unwrap();
        let mut buf = BytesMut::from(&out[..]);
        assert!(matches!(
            decode(&mut buf).unwrap(),
            Some(PeerMessage::Hello { .. })
        ));
        assert!(matches!(
            decode(&mut buf).unwrap(),
            Some(PeerMessage::Publish { .. })
        ));
        assert_eq!(decode(&mut buf).unwrap(), None);
    }
}
