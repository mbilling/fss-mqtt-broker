//! The payload header and the hot PUBLISH encoder.
//!
//! Every payload starts with a fixed 32-byte header — the message's identity and its
//! send time on the process clock — followed by filler up to the configured size:
//!
//! | offset | size | field |
//! |---|---|---|
//! | 0 | 4 | magic `MQLG` |
//! | 4 | 4 | run id (a run ignores, and counts as *foreign*, any other run's messages) |
//! | 8 | 4 | publisher id |
//! | 12 | 4 | reserved, 0 |
//! | 16 | 8 | sequence number within the publisher, from 0 |
//! | 24 | 8 | send time, ns since the process epoch |
//!
//! All big-endian. The encoder writes a PUBLISH straight into the connection's write
//! buffer with the codec's own primitives, so the send path allocates nothing per
//! message; a test decodes its output with the codec's [`mqtt_codec::Packet::decode`].

use mqtt_codec::{io, varint, CodecError};

/// The header's size; the smallest payload the generator can send.
pub const HEADER_LEN: usize = 32;

const MAGIC: u32 = u32::from_be_bytes(*b"MQLG");

/// A message's identity and send time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// The run this message belongs to.
    pub run: u32,
    /// Publisher id, `0..publishers`.
    pub publisher: u32,
    /// Sequence number within the publisher.
    pub seq: u64,
    /// Send time, ns since the process epoch.
    pub send_ns: u64,
}

/// What a received payload turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decoded {
    /// One of this run's messages.
    Ours(Header),
    /// A generator payload from another run (a durable session's leftovers).
    OtherRun,
    /// Not a generator payload at all.
    Foreign,
}

impl Header {
    /// Write the header's 32 bytes.
    pub fn put(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&self.run.to_be_bytes());
        out.extend_from_slice(&self.publisher.to_be_bytes());
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&self.seq.to_be_bytes());
        out.extend_from_slice(&self.send_ns.to_be_bytes());
    }

    /// Read a payload's header, judged against this run's id.
    #[must_use]
    pub fn decode(payload: &[u8], run: u32) -> Decoded {
        let Some(h) = payload.get(..HEADER_LEN) else {
            return Decoded::Foreign;
        };
        let u32_at = |i: usize| u32::from_be_bytes([h[i], h[i + 1], h[i + 2], h[i + 3]]);
        let u64_at = |i: usize| (u64::from(u32_at(i)) << 32) | u64::from(u32_at(i + 4));
        if u32_at(0) != MAGIC {
            return Decoded::Foreign;
        }
        if u32_at(4) != run {
            return Decoded::OtherRun;
        }
        Decoded::Ours(Header {
            run,
            publisher: u32_at(8),
            seq: u64_at(16),
            send_ns: u64_at(24),
        })
    }
}

/// Append one PUBLISH to `out`: `topic`, `QoS` 0 or 1 (`pkid` required for 1), MQTT 5
/// (an empty property block) or 3.1.1, and a payload of `payload_len` bytes —
/// the header, then `filler`'s first `payload_len - HEADER_LEN` bytes.
///
/// # Errors
/// If the packet does not fit MQTT's limits.
///
/// # Panics
/// If `payload_len < HEADER_LEN` or `filler` is shorter than the padding needed.
#[allow(clippy::too_many_arguments)]
pub fn encode_publish(
    out: &mut Vec<u8>,
    topic: &[u8],
    qos: u8,
    pkid: Option<u16>,
    v5: bool,
    header: &Header,
    payload_len: usize,
    filler: &[u8],
) -> Result<(), CodecError> {
    assert!(payload_len >= HEADER_LEN);
    let pad = payload_len - HEADER_LEN;
    let body = 2 + topic.len() + usize::from(qos > 0) * 2 + usize::from(v5) + payload_len;
    let remaining =
        u32::try_from(body).map_err(|_| CodecError::ValueOutOfRange("remaining length"))?;
    io::put_u8(out, 0x30 | (qos << 1));
    varint::encode(remaining, out)?;
    io::put_binary(out, topic)?;
    if qos > 0 {
        io::put_u16(
            out,
            pkid.ok_or(CodecError::MalformedPacket("QoS > 0 needs a pkid"))?,
        );
    }
    if v5 {
        io::put_u8(out, 0); // empty property block
    }
    header.put(out);
    out.extend_from_slice(&filler[..pad]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use mqtt_codec::{Packet, ProtocolVersion, QoS};

    #[test]
    fn publish_round_trips_through_the_codec() {
        let filler = vec![0xAB; 1024];
        let h = Header {
            run: 7,
            publisher: 42,
            seq: 123_456_789_012,
            send_ns: 987_654_321,
        };
        for (qos, pkid, v5) in [(0, None, true), (1, Some(9), true), (1, Some(65535), false)] {
            let mut out = Vec::new();
            encode_publish(&mut out, b"load/3", qos, pkid, v5, &h, 200, &filler).unwrap();
            let version = if v5 {
                ProtocolVersion::V5
            } else {
                ProtocolVersion::V311
            };
            let mut buf = BytesMut::from(&out[..]);
            let Some(Packet::Publish(p)) = Packet::decode(&mut buf, version).unwrap() else {
                panic!("not a publish");
            };
            assert!(buf.is_empty());
            assert_eq!(p.topic, "load/3");
            assert_eq!(p.qos, QoS::from_u8(qos).unwrap());
            assert_eq!(p.pkid, pkid);
            assert_eq!(p.payload.len(), 200);
            assert_eq!(Header::decode(&p.payload, 7), Decoded::Ours(h));
            assert_eq!(Header::decode(&p.payload, 8), Decoded::OtherRun);
        }
    }

    #[test]
    fn short_or_alien_payloads_are_foreign() {
        assert_eq!(Header::decode(b"hello", 1), Decoded::Foreign);
        assert_eq!(Header::decode(&[0u8; 64], 1), Decoded::Foreign);
    }
}
