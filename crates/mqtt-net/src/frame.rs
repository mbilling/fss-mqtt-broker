//! Packet framing over async byte streams.
//!
//! [`FrameReader`] and [`FrameWriter`] are deliberately split so they can own the
//! two halves of a [`tokio::net::TcpStream`] independently — that lets a
//! connection task read inbound packets and write outbound packets concurrently
//! via `tokio::select!` without aliasing the same stream.

use crate::NetError;
use bytes::{Bytes, BytesMut};
use mqtt_codec::{Packet, ProtocolVersion};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// A cap on the read buffer to bound memory from a slow or hostile peer.
///
/// A real deployment will derive this from the negotiated MQTT 5 Maximum Packet
/// Size; for now it is a fixed safety ceiling.
const MAX_BUFFERED_BYTES: usize = 1024 * 1024;

/// Reads framed MQTT packets from an [`AsyncRead`].
#[derive(Debug)]
pub struct FrameReader<R> {
    inner: R,
    buf: BytesMut,
    version: ProtocolVersion,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// Create a reader over `inner` for the given protocol `version`.
    pub fn new(inner: R, version: ProtocolVersion) -> Self {
        Self {
            inner,
            buf: BytesMut::with_capacity(2048),
            version,
        }
    }

    /// Create a reader pre-seeded with already-read bytes. Used when a stream is
    /// handed off mid-flight (e.g. a session proxied to its owner, ADR 0005):
    /// `prefix` holds bytes read past the handoff marker that belong to the MQTT
    /// stream and must be parsed before reading more from `inner`.
    pub fn with_buffer(inner: R, version: ProtocolVersion, prefix: BytesMut) -> Self {
        Self {
            inner,
            buf: prefix,
            version,
        }
    }

    /// Decompose into the underlying reader and any bytes buffered past the last
    /// returned packet — for resuming raw I/O on the same stream (e.g. splicing
    /// a proxied session, ADR 0005).
    pub fn into_parts(self) -> (R, BytesMut) {
        (self.inner, self.buf)
    }

    /// Update the protocol version (e.g. after a CONNECT negotiates v5).
    pub fn set_version(&mut self, version: ProtocolVersion) {
        self.version = version;
    }

    /// Read the next packet.
    ///
    /// Returns `Ok(None)` on a clean end-of-stream at a packet boundary.
    ///
    /// # Errors
    /// - [`NetError::Codec`] if the peer sends a malformed packet.
    /// - [`NetError::UnexpectedEof`] if the stream ends mid-packet.
    /// - [`NetError::PacketTooLarge`](mqtt_codec::CodecError::PacketTooLarge)
    ///   (wrapped) if a single packet would exceed [`MAX_BUFFERED_BYTES`].
    /// - [`NetError::Io`] on a transport error.
    pub async fn next_packet(&mut self) -> Result<Option<Packet>, NetError> {
        loop {
            if let Some(packet) = Packet::decode(&mut self.buf, self.version)? {
                return Ok(Some(packet));
            }
            if self.buf.len() > MAX_BUFFERED_BYTES {
                return Err(NetError::Codec(mqtt_codec::CodecError::PacketTooLarge));
            }
            let n = self.inner.read_buf(&mut self.buf).await?;
            if n == 0 {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(NetError::UnexpectedEof)
                };
            }
        }
    }

    /// Read the next packet's **raw bytes** (fixed header + remaining-length + payload) without
    /// decoding it. Version-agnostic — the MQTT fixed-header framing is identical across 3.1.1
    /// and 5. Used by the QUIC multi-stream mux (ADR 0036) to merge *complete* packets from
    /// several streams into one byte stream without ever interleaving them at the byte level.
    ///
    /// Returns `Ok(None)` on a clean end-of-stream at a packet boundary.
    ///
    /// # Errors
    /// As [`next_packet`](Self::next_packet): malformed framing, EOF mid-packet, or a packet
    /// exceeding the buffer ceiling.
    pub async fn next_raw_frame(&mut self) -> Result<Option<Bytes>, NetError> {
        loop {
            if let Some(frame) = take_raw_frame(&mut self.buf)? {
                return Ok(Some(frame));
            }
            if self.buf.len() > MAX_BUFFERED_BYTES {
                return Err(NetError::Codec(mqtt_codec::CodecError::PacketTooLarge));
            }
            let n = self.inner.read_buf(&mut self.buf).await?;
            if n == 0 {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(NetError::UnexpectedEof)
                };
            }
        }
    }
}

/// Split off one complete MQTT packet's raw bytes from `buf`, or `Ok(None)` if `buf` does not
/// yet hold a whole packet. Parses the fixed header: the control byte plus the
/// remaining-length varint (1–4 bytes), then `remaining_length` payload bytes. Shared with the
/// QUIC mux (ADR 0036) for extracting complete outbound packets to route across streams.
pub(crate) fn take_raw_frame(buf: &mut BytesMut) -> Result<Option<Bytes>, NetError> {
    if buf.is_empty() {
        return Ok(None);
    }
    // Remaining-length varint starts at byte 1 (byte 0 is the packet-type/flags control byte).
    let mut remaining = 0usize;
    let mut multiplier = 1usize;
    let mut header_len = 1usize; // control byte
    loop {
        if header_len >= buf.len() {
            return Ok(None); // need more bytes to finish the length varint
        }
        let byte = buf[header_len];
        header_len += 1;
        remaining += (byte & 0x7f) as usize * multiplier;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        // A remaining-length is at most 4 bytes; a 5th continuation byte is malformed.
        if header_len > 4 {
            return Err(NetError::Codec(mqtt_codec::CodecError::MalformedPacket(
                "remaining length exceeds 4 bytes",
            )));
        }
    }
    let total = header_len + remaining;
    if total > MAX_BUFFERED_BYTES {
        return Err(NetError::Codec(mqtt_codec::CodecError::PacketTooLarge));
    }
    if buf.len() < total {
        return Ok(None); // whole packet not buffered yet
    }
    Ok(Some(buf.split_to(total).freeze()))
}

/// Writes framed MQTT packets to an [`AsyncWrite`].
#[derive(Debug)]
pub struct FrameWriter<W> {
    inner: W,
    version: ProtocolVersion,
}

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    /// Create a writer over `inner` for the given protocol `version`.
    pub fn new(inner: W, version: ProtocolVersion) -> Self {
        Self { inner, version }
    }

    /// Update the protocol version.
    pub fn set_version(&mut self, version: ProtocolVersion) {
        self.version = version;
    }

    /// The protocol version this writer encodes at (negotiated from the CONNECT).
    #[must_use]
    pub fn version(&self) -> ProtocolVersion {
        self.version
    }

    /// Recover the underlying writer (e.g. to resume raw I/O when splicing a
    /// proxied session, ADR 0005).
    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Encode and send a single packet, flushing the stream.
    ///
    /// # Errors
    /// [`NetError::Codec`] if the packet cannot be encoded, or [`NetError::Io`].
    pub async fn send(&mut self, packet: &Packet) -> Result<(), NetError> {
        let mut out = Vec::new();
        packet.encode(&mut out, self.version)?;
        self.inner.write_all(&out).await?;
        self.inner.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{FrameReader, FrameWriter, MAX_BUFFERED_BYTES};
    use crate::NetError;
    use mqtt_codec::{packet::ConnAck, CodecError, Packet, ProtocolVersion};
    use tokio::io::AsyncWriteExt;

    const V4: ProtocolVersion = ProtocolVersion::V311;

    // Sanity for the oversized-packet test: its claimed size (2 MiB) really is
    // beyond the buffer ceiling.
    const _: () = assert!(2_097_152 > MAX_BUFFERED_BYTES);

    #[tokio::test]
    async fn write_then_read_roundtrip_over_duplex() {
        let (client, server) = tokio::io::duplex(4096);
        let (cr, cw) = tokio::io::split(client);
        let (sr, _sw) = tokio::io::split(server);

        let mut writer = FrameWriter::new(cw, V4);
        writer.send(&Packet::PingReq).await.unwrap();
        writer
            .send(&Packet::ConnAck(ConnAck {
                properties: mqtt_codec::Properties::new(),
                session_present: false,
                code: 0,
            }))
            .await
            .unwrap();
        drop(writer);
        drop(cr);

        let mut reader = FrameReader::new(sr, V4);
        assert_eq!(reader.next_packet().await.unwrap(), Some(Packet::PingReq));
        assert_eq!(
            reader.next_packet().await.unwrap(),
            Some(Packet::ConnAck(ConnAck {
                properties: mqtt_codec::Properties::new(),
                session_present: false,
                code: 0
            }))
        );
        // Clean EOF once the writer is dropped.
        assert_eq!(reader.next_packet().await.unwrap(), None);
    }

    /// A peer declaring a packet larger than the buffer ceiling must be cut off
    /// with `PacketTooLarge`, not buffered without bound. The codec itself has
    /// no size cap — this reader is the enforcement point.
    #[tokio::test]
    async fn oversized_packet_is_rejected_not_buffered() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);

        // PUBLISH fixed header claiming a 2 MiB remaining length (varint), then
        // a stream of filler the reader will buffer while waiting for the rest.
        tokio::spawn(async move {
            let header: &[u8] = &[0x30, 0x80, 0x80, 0x80, 0x01]; // 2_097_152
            let _ = client.write_all(header).await;
            let chunk = vec![0u8; 64 * 1024];
            loop {
                if client.write_all(&chunk).await.is_err() {
                    return; // reader hung up after rejecting
                }
            }
        });

        let mut reader = FrameReader::new(server, V4);
        match reader.next_packet().await {
            Err(NetError::Codec(CodecError::PacketTooLarge)) => {}
            other => panic!("expected PacketTooLarge, got {other:?}"),
        }
    }

    /// `next_raw_frame` returns each packet's exact bytes (no interleaving across packets), and
    /// those bytes decode back to the original packet — the property the QUIC mux relies on.
    #[tokio::test]
    async fn next_raw_frame_returns_whole_decodable_packets() {
        use bytes::BytesMut;
        let (client, server) = tokio::io::duplex(4096);
        let (cr, cw) = tokio::io::split(client);
        let (sr, _sw) = tokio::io::split(server);

        let publish = Packet::Publish(mqtt_codec::packet::Publish {
            properties: mqtt_codec::Properties::new(),
            dup: false,
            qos: mqtt_codec::QoS::AtMostOnce,
            retain: false,
            topic: "t/x".into(),
            pkid: None,
            payload: bytes::Bytes::from_static(b"hello raw frame"),
        });
        let mut writer = FrameWriter::new(cw, V4);
        writer.send(&Packet::PingReq).await.unwrap();
        writer.send(&publish).await.unwrap();
        drop(writer);
        drop(cr);

        let mut reader = FrameReader::new(sr, V4);
        // First raw frame is the 2-byte PINGREQ; it decodes back to PingReq.
        let f1 = reader.next_raw_frame().await.unwrap().unwrap();
        assert_eq!(&f1[..], &[0xC0, 0x00]);
        assert_eq!(
            Packet::decode(&mut BytesMut::from(&f1[..]), V4).unwrap(),
            Some(Packet::PingReq)
        );
        // Second raw frame is the whole PUBLISH and decodes back to it.
        let f2 = reader.next_raw_frame().await.unwrap().unwrap();
        assert_eq!(
            Packet::decode(&mut BytesMut::from(&f2[..]), V4).unwrap(),
            Some(publish)
        );
        // Clean EOF at the packet boundary.
        assert!(reader.next_raw_frame().await.unwrap().is_none());
    }

    /// A stream ending in the middle of a packet is an error, not a clean EOF —
    /// silently dropping a half-received packet would mask truncation attacks.
    #[tokio::test]
    async fn eof_mid_packet_is_an_error() {
        let (mut client, server) = tokio::io::duplex(4096);

        // First three bytes of a four-byte CONNACK, then hang up. Dropping the
        // whole stream (not a split half, which would keep it open) is the EOF.
        client.write_all(&[0x20, 0x02, 0x00]).await.unwrap();
        drop(client);

        let mut reader = FrameReader::new(server, V4);
        match reader.next_packet().await {
            Err(NetError::UnexpectedEof) => {}
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }
}
