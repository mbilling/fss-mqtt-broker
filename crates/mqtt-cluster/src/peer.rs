//! Inter-node ("peer") wire protocol.
//!
//! This is deliberately **separate** from the MQTT client protocol: it carries
//! node-to-node control and data — a `Hello` handshake, subscription interest
//! announcements, and forwarded publishes. Messages are length-prefixed
//! (`u32` big-endian) `bincode` frames.
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
pub const PROTO_MIN: u32 = 1;
/// The newest peer-bus protocol version this build can speak (ADR 0038). A link's
/// negotiated version is `min(proto_max_a, proto_max_b)`.
///
/// **Release rule (ADR 0039)**: minors may bump this **additively** — new frames or
/// fields ship under the new proto while every proto back to [`PROTO_MIN`] is still
/// spoken in full. A bump that stops speaking an old proto is really a `PROTO_MIN`
/// raise: a MAJOR release.
pub const PROTO_MAX: u32 = 1;

/// Negotiate a link's protocol version from both sides' announced ranges
/// (ADR 0038): the newest version both can speak, or `None` when the ranges are
/// disjoint — the link must then be rejected (fail closed) rather than half-joined.
#[must_use]
pub fn negotiate_proto(local: (u32, u32), remote: (u32, u32)) -> Option<u32> {
    let candidate = local.1.min(remote.1);
    (candidate >= local.0 && candidate >= remote.0).then_some(candidate)
}

/// Wire form of a shared-subscription membership snapshot (ADR 0015 §2): each entry
/// is `(ShareName, filter, [(client id, granted QoS u8, online-on-this-node)])`. The
/// per-member liveness lets a peer's selector skip a member offline on its home node
/// (ADR 0015 T8).
pub type SharedGroupsWire = Vec<(String, String, Vec<(String, u8, bool)>)>;

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
        /// Each shared group: `(ShareName, filter, [(client id, granted QoS u8)])`.
        groups: SharedGroupsWire,
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
    /// entries for the key, as `(offset, record)` pairs (kept as tuples so the
    /// storage crate's `LogEntry` need not be serde-wire-encodable), plus its truncation
    /// low-water so a recovery cannot resurrect an already-acked prefix (ADR 0018 §3b).
    ReplicaReadReply {
        /// The `req_id` of the [`ReplicaRead`](PeerMessage::ReplicaRead) answered.
        req_id: u64,
        /// The replica's truncation low-water for the key.
        #[serde(default)]
        watermark: u64,
        /// The stored entries, in offset order.
        entries: Vec<(u64, Vec<u8>)>,
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
    let body = bincode::serialize(msg).map_err(|e| PeerCodecError::Serde(e.to_string()))?;
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
    let msg = bincode::deserialize(&body).map_err(|e| PeerCodecError::Serde(e.to_string()))?;
    Ok(Some(msg))
}

#[cfg(test)]
mod tests {
    use super::{
        decode, encode, negotiate_proto, PeerCodecError, PeerMessage, RetainedWireEntry,
        WireAppProps, MAX_FRAME, PROTO_MAX, PROTO_MIN,
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
            groups: vec![(
                "grp".into(),
                "t/+".into(),
                vec![("c1".into(), 1, true), ("c2".into(), 0, false)],
            )],
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
            entries: vec![(1, vec![1, 2]), (2, vec![3, 4])],
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
        });
        roundtrip(&PeerMessage::RetainedCommit {
            topic: "dev/1/state".into(),
            payload: Vec::new(), // a clear (versioned tombstone)
            qos: 0,
            props: WireAppProps::default(),
            seq: 10,
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
        });
        roundtrip(&PeerMessage::RetainedUpdate {
            topic: "dev/1/state".into(),
            payload: Vec::new(), // a committed clear
            qos: 0,
            epoch: 7,
            offset: 43,
            props: WireAppProps::default(),
        });
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
