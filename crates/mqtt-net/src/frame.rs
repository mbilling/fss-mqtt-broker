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
const DEFAULT_MAX_PACKET_BYTES: usize = 1024 * 1024;

/// The process-wide inbound packet ceiling, settable once at startup
/// (ADR 0041 T4: `MQTTD_MAX_PACKET_SIZE`, advertised to v5 clients as the MQTT 5
/// Maximum Packet Size). Defaults to 1 MiB.
static MAX_PACKET_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Set the inbound packet ceiling once, at startup before any connection is served.
pub fn set_max_packet_bytes(bytes: usize) {
    let _ = MAX_PACKET_BYTES.set(bytes);
}

/// The configured inbound packet ceiling (ADR 0041 T4), or the 1 MiB default.
#[must_use]
pub fn max_packet_bytes() -> usize {
    *MAX_PACKET_BYTES.get_or_init(|| DEFAULT_MAX_PACKET_BYTES)
}

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
            buf: BytesMut::with_capacity(READ_BUF_INIT),
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
    ///   (wrapped) if a single packet would exceed [`max_packet_bytes`].
    /// - [`NetError::Io`] on a transport error.
    pub async fn next_packet(&mut self) -> Result<Option<Packet>, NetError> {
        loop {
            // The ceiling is checked against the DECLARED total before any decode
            // is attempted (issue #292): decode-first meant a complete oversized
            // packet was accepted — the buffered-length check below never ran once
            // the bytes were all in. Checking the header's own claim also refuses
            // an oversized packet before its body is buffered, so the bound holds
            // without ever holding the offending megabytes. Same arithmetic as
            // `take_raw_frame`, via the shared helper.
            if let Some(total) = declared_frame_len(&self.buf)? {
                if total > max_packet_bytes() {
                    return Err(NetError::Codec(mqtt_codec::CodecError::PacketTooLarge));
                }
            }
            if let Some(packet) = Packet::decode(&mut self.buf, self.version)? {
                return Ok(Some(packet));
            }
            // Belt-and-braces for a header that never completes: the declared
            // length can only be read once the varint is whole, so a peer
            // trickling header bytes is still bounded by the buffer ceiling.
            if self.buf.len() > max_packet_bytes() {
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
            if self.buf.len() > max_packet_bytes() {
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
    let Some(total) = declared_frame_len(buf)? else {
        return Ok(None); // fixed header not complete yet
    };
    if total > max_packet_bytes() {
        return Err(NetError::Codec(mqtt_codec::CodecError::PacketTooLarge));
    }
    if buf.len() < total {
        return Ok(None); // whole packet not buffered yet
    }
    Ok(Some(buf.split_to(total).freeze()))
}

/// The total length (fixed header + remaining-length payload) the buffered fixed header
/// DECLARES, or `Ok(None)` when the header itself is not complete yet. This is the one
/// place the remaining-length varint is parsed for framing; both the TCP path
/// ([`FrameReader::next_packet`], issue #292) and the raw-frame path ([`take_raw_frame`])
/// judge the packet-size ceiling against this claim BEFORE buffering or decoding a body.
pub(crate) fn declared_frame_len(buf: &BytesMut) -> Result<Option<usize>, NetError> {
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
    Ok(Some(header_len + remaining))
}

/// Writes framed MQTT packets to an [`AsyncWrite`].
#[derive(Debug)]
pub struct FrameWriter<W> {
    inner: W,
    version: ProtocolVersion,
    /// Reused encode buffer: the outbound drain encodes a whole backlog into it
    /// and writes it in one syscall (issue #443).
    ///
    /// Allocated on first use, not at construction. A connection that only ever
    /// publishes writes exactly one packet — its CONNACK — so an eager
    /// batch-sized buffer was pure per-connection overhead for the majority of
    /// clients in a fan-in deployment. The first `queue` takes
    /// [`SCRATCH_FIRST_USE`], and a real fan-out drain grows it to its own
    /// high-water mark on the first batch and keeps it there, because
    /// [`flush_queued`](FrameWriter::flush_queued) clears without shrinking.
    scratch: Vec<u8>,
}

/// What a reader reserves for inbound framing, eagerly, per connection.
///
/// Unlike the encode buffer this one is paid by every connection whether it
/// publishes or subscribes, because every connection reads. It is named rather
/// than inlined so it is visible as a per-connection cost and guarded below: at
/// 50,000 connections a node this is 100 MiB of the broker's floor.
pub(crate) const READ_BUF_INIT: usize = 2048;

/// What the encode buffer reserves the first time a packet is queued.
///
/// Sized for a control packet (CONNACK/AUTH/PUBACK), not for a fan-out batch:
/// the batch path grows past it once and then reuses the grown buffer, so the
/// only connections that ever hold a large buffer are the ones that fan out.
const SCRATCH_FIRST_USE: usize = 512;

/// Per-connection eager allocation budget, asserted at compile time.
///
/// These two buffers are allocated per connection and are the broker's
/// per-connection memory floor. The encode scratch was raised to 4,096 in
/// v1.0.8 for the fan-out drain (issue #443) and inflated every connection by
/// 23% for four releases before anyone measured it. Naming the numbers and
/// asserting them here is what makes the next such change a deliberate,
/// reviewed act: if one of these trips, raise the constant AND update the
/// formula in `docs/SIZING.md` in the same commit.
const _: () = assert!(READ_BUF_INIT <= 2048);
const _: () = assert!(SCRATCH_FIRST_USE <= 512);

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    /// Create a writer over `inner` for the given protocol `version`.
    pub fn new(inner: W, version: ProtocolVersion) -> Self {
        Self {
            inner,
            version,
            scratch: Vec::new(),
        }
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

    /// Encode `packet` onto the pending write buffer WITHOUT touching the socket.
    /// Pair with [`flush_queued`](Self::flush_queued) to write a whole batch in
    /// one syscall (issue #443). Encoding appends, so several packets accumulate.
    ///
    /// # Errors
    /// [`NetError::Codec`] if the packet cannot be encoded.
    pub fn queue(&mut self, packet: &Packet) -> Result<(), NetError> {
        if self.scratch.capacity() == 0 {
            self.scratch.reserve(SCRATCH_FIRST_USE);
        }
        packet.encode(&mut self.scratch, self.version)?;
        Ok(())
    }

    /// Bytes currently queued but not yet flushed — the offset a caller records
    /// before [`queue`](Self::queue) so it can [`truncate_queued`](Self::truncate_queued)
    /// a packet it decided, AFTER encoding it once, not to send (issue #443 5b:
    /// the client's Maximum Packet Size is now measured from the bytes actually
    /// queued, never a throwaway second encode).
    #[must_use]
    pub fn queued_len(&self) -> usize {
        self.scratch.len()
    }

    /// Drop everything queued past `len` (an offset from [`queued_len`](Self::queued_len)).
    pub fn truncate_queued(&mut self, len: usize) {
        self.scratch.truncate(len);
    }

    /// Write everything queued in one `write_all` + `flush`, then clear the
    /// buffer. A no-op when nothing is queued, so a drain that dropped every
    /// packet costs no syscall.
    ///
    /// # Errors
    /// [`NetError::Io`] on a transport error.
    pub async fn flush_queued(&mut self) -> Result<(), NetError> {
        if self.scratch.is_empty() {
            return Ok(());
        }
        self.inner.write_all(&self.scratch).await?;
        self.inner.flush().await?;
        self.scratch.clear();
        Ok(())
    }

    /// Encode and send a single packet, flushing the stream. Convenience for
    /// control packets (CONNACK/AUTH/…) written outside the batched drain.
    ///
    /// # Errors
    /// [`NetError::Codec`] if the packet cannot be encoded, or [`NetError::Io`].
    pub async fn send(&mut self, packet: &Packet) -> Result<(), NetError> {
        self.queue(packet)?;
        self.flush_queued().await
    }
}

#[cfg(test)]
mod tests {
    use super::{FrameReader, FrameWriter, READ_BUF_INIT, SCRATCH_FIRST_USE};
    use crate::NetError;
    use mqtt_codec::{packet::ConnAck, CodecError, Packet, ProtocolVersion};
    use tokio::io::AsyncWriteExt;

    const V4: ProtocolVersion = ProtocolVersion::V311;

    // Sanity for the oversized-packet test: its claimed size (2 MiB) really is
    // beyond the buffer ceiling.

    /// A connection that never writes never pays for an encode buffer.
    ///
    /// The scratch is the fan-out drain's batching buffer (issue #443). In a
    /// fan-in deployment most connections only publish and write exactly one
    /// packet — their CONNACK — so allocating a batch-sized buffer at
    /// construction charged the majority of clients for machinery only
    /// subscribers use.
    #[test]
    fn a_new_writer_allocates_no_encode_buffer() {
        let w = FrameWriter::new(Vec::<u8>::new(), V4);
        assert_eq!(
            w.scratch.capacity(),
            0,
            "the encode buffer must be allocated on first use, not at construction"
        );
    }

    /// The first packet takes a CONTROL-sized buffer, not a batch-sized one — a
    /// client that only ever writes a CONNACK keeps a small buffer for life.
    #[tokio::test]
    async fn the_first_queued_packet_takes_a_control_sized_buffer() {
        let mut w = FrameWriter::new(Vec::<u8>::new(), V4);
        w.queue(&Packet::ConnAck(ConnAck {
            properties: mqtt_codec::Properties::new(),
            session_present: false,
            code: 0,
        }))
        .unwrap();
        assert!(
            w.scratch.capacity() <= SCRATCH_FIRST_USE,
            "a control packet grew the buffer to {} bytes; it must stay within {SCRATCH_FIRST_USE}",
            w.scratch.capacity()
        );
    }

    /// A real drain still gets one buffer it reuses: capacity survives the flush,
    /// so the batching win (#443) is unchanged for the connections that fan out.
    #[tokio::test]
    async fn a_drain_grows_the_buffer_once_and_keeps_it() {
        let mut w = FrameWriter::new(Vec::<u8>::new(), V4);
        // A v3.1.1 CONNACK is 4 bytes on the wire, so this is ~1.2 KiB queued —
        // comfortably past the control-sized floor, which is the point.
        for _ in 0..300 {
            w.queue(&Packet::ConnAck(ConnAck {
                properties: mqtt_codec::Properties::new(),
                session_present: false,
                code: 0,
            }))
            .unwrap();
        }
        let grown = w.scratch.capacity();
        assert!(
            grown > SCRATCH_FIRST_USE,
            "a batch must grow past the floor"
        );
        w.flush_queued().await.unwrap();
        assert_eq!(
            w.scratch.capacity(),
            grown,
            "flushing clears without shrinking, so the next batch reuses the buffer"
        );
    }

    /// The reader's buffer IS eager, and deliberately so: every connection reads.
    /// Pinned so its per-connection cost stays visible.
    #[test]
    fn a_new_reader_takes_the_named_read_buffer() {
        let r = FrameReader::new(tokio::io::empty(), V4);
        assert!(
            r.buf.capacity() >= READ_BUF_INIT,
            "the read buffer is allocated up front at READ_BUF_INIT"
        );
    }

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
