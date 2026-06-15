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

/// A message exchanged between broker nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerMessage {
    /// Sent first on a new link to announce the sender's node id.
    Hello {
        /// The sending node's identifier.
        node_id: String,
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
        /// Whether the message was published with the retain flag. Retained
        /// state is not replicated yet (Phase 3); carried for the wire format.
        retain: bool,
    },
    /// First frame of a **session proxy** (ADR 0005): instead of a peer link,
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
    // `body.len()` is bounded by the message we built, so the cast is safe.
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
    use super::{decode, encode, PeerMessage};
    use bytes::BytesMut;

    fn roundtrip(msg: &PeerMessage) {
        let mut out = Vec::new();
        encode(msg, &mut out).unwrap();
        let mut buf = BytesMut::from(&out[..]);
        assert_eq!(decode(&mut buf).unwrap().as_ref(), Some(msg));
        assert!(buf.is_empty());
    }

    #[test]
    fn roundtrips_all_variants() {
        roundtrip(&PeerMessage::Hello {
            node_id: "node-a".into(),
        });
        roundtrip(&PeerMessage::Interest {
            filters: vec!["a/#".into(), "b/+/c".into()],
        });
        roundtrip(&PeerMessage::Publish {
            topic: "sensors/temp".into(),
            payload: b"21.5C".to_vec(),
            qos: 1,
            retain: false,
        });
        roundtrip(&PeerMessage::ProxyHello {
            identity: Some("device-7".into()),
        });
        roundtrip(&PeerMessage::ProxyHello { identity: None });
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
    }

    #[test]
    fn partial_frame_returns_none() {
        let mut out = Vec::new();
        encode(
            &PeerMessage::Hello {
                node_id: "x".into(),
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
