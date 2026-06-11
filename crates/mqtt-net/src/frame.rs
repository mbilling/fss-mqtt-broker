//! Packet framing over async byte streams.
//!
//! [`FrameReader`] and [`FrameWriter`] are deliberately split so they can own the
//! two halves of a [`tokio::net::TcpStream`] independently — that lets a
//! connection task read inbound packets and write outbound packets concurrently
//! via `tokio::select!` without aliasing the same stream.

use crate::NetError;
use bytes::BytesMut;
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
