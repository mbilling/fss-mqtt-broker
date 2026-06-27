//! MQTT-over-QUIC transport ([ADR 0036](../../../docs/adr/0036-quic-transport.md)).
//!
//! QUIC (over UDP) mandates TLS 1.3 — there is no plaintext mode — so the endpoint is built
//! from the *same* audited rustls server config as the TLS listener (ADR 0002), adding ALPN
//! `mqtt`. The MQTT session runs over a QUIC **bidirectional stream**, which is just a byte
//! stream, so the connection engine (`conn::handle_stream<S>`) is unchanged.
//!
//! This module is the transport foundation: building the endpoint, joining a bidi stream into
//! a byte stream, and reading the mTLS peer certificate for identity. The control stream (the
//! first bidi stream) carries the session; multi-stream data streams (ADR 0036) layer on it.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::CertificateDer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

use crate::frame::take_raw_frame;
use crate::{FrameReader, NetError};
use mqtt_codec::ProtocolVersion;

/// How many broker-opened data streams to fan outbound PUBLISH across (ADR 0036 §3a). Topics
/// are hashed onto this fixed pool, so per-topic order is preserved and resource use is bounded.
const OUTBOUND_POOL: usize = 4;

/// The ALPN protocol id for MQTT-over-QUIC (matches the de-facto EMQX-style binding).
const ALPN_MQTT: &[u8] = b"mqtt";

/// Build a QUIC server [`Endpoint`] bound to `addr`, using the same audited rustls server
/// config as the TLS listener (TLS 1.3, `ring`, optional mTLS client-cert verification) with
/// ALPN `mqtt`. 0-RTT early data is left disabled (replay safety for the CONNECT exchange).
///
/// # Errors
/// [`NetError`] if the TLS material is unreadable/invalid, the QUIC crypto cannot be built, or
/// the UDP socket cannot be bound.
pub fn server_endpoint(
    addr: SocketAddr,
    cert: &Path,
    key: &Path,
    client_ca: Option<&Path>,
) -> Result<Endpoint, NetError> {
    let mut crypto = crate::tls::server_config(cert, key, client_ca)?;
    crypto.alpn_protocols = vec![ALPN_MQTT.to_vec()];
    crypto.max_early_data_size = 0; // no 0-RTT for the session-establishing exchange (replay).
    let qsc = QuicServerConfig::try_from(crypto)
        .map_err(|e| NetError::Tls(format!("quic server config: {e}")))?;
    let server_config = ServerConfig::with_crypto(Arc::new(qsc));
    Endpoint::server(server_config, addr).map_err(NetError::Io)
}

/// Join a QUIC bidirectional stream's halves into one `AsyncRead + AsyncWrite` byte stream, so
/// the MQTT engine runs over a QUIC stream exactly as it does over TCP or a WebSocket.
#[must_use]
pub fn byte_stream(send: SendStream, recv: RecvStream) -> tokio::io::Join<RecvStream, SendStream> {
    tokio::io::join(recv, send)
}

/// The peer's leaf certificate from an mTLS QUIC connection, for identity extraction (ADR
/// 0004). `None` when the client presented no certificate (anonymous; the default policy
/// denies it).
#[must_use]
pub fn peer_leaf_cert(conn: &Connection) -> Option<CertificateDer<'static>> {
    let certs = conn
        .peer_identity()?
        .downcast::<Vec<CertificateDer<'static>>>()
        .ok()?;
    certs.first().cloned()
}

/// Multi-stream demultiplexer for one MQTT-over-QUIC connection (ADR 0036): **one MQTT session
/// spread across many QUIC streams**, symmetric in both directions.
///
/// **Inbound:** each stream is read at *packet granularity* ([`FrameReader::next_raw_frame`]) by
/// its own task; complete packets are merged — never byte-interleaved — into one inbound stream,
/// so a stalled/large packet on one stream does not block another (no head-of-line blocking).
///
/// **Outbound (§3a):** PUBLISH is fanned across a small pool of *broker-opened* data streams,
/// **topic-hashed** (same topic → same stream, preserving per-topic order); control packets and
/// `QoS` acks stay on the control stream. Fan-out is **capability-gated**: it only happens once the
/// peer has itself opened a data stream (proving it runs this mux and will accept our streams) —
/// a plain single-control-stream client is never stranded; its outbound stays on the control
/// stream.
///
/// [`QuicMux`] is an `AsyncRead + AsyncWrite`, so `conn::handle_stream<S>` runs over it unchanged.
pub struct QuicMux {
    /// Complete inbound packets from every stream, in arrival order.
    rx: mpsc::UnboundedReceiver<Bytes>,
    /// The packet currently being served to the reader (drained as it is read).
    read_cur: Bytes,
    /// Outbound bytes accumulated until a complete packet can be routed to a stream.
    out_buf: BytesMut,
    /// Complete outbound packets handed to the writer task (which routes them to streams).
    out_tx: mpsc::UnboundedSender<Bytes>,
    /// Held so dropping the mux closes the connection, ending the writer/reader/accept tasks.
    conn: Connection,
}

impl std::fmt::Debug for QuicMux {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicMux").finish_non_exhaustive()
    }
}

/// Server side: accept the per-connection mux. Takes the **control stream** (first bidi the
/// client opens), then accepts every later data stream — all feeding one session — and fans
/// outbound PUBLISH back across broker-opened data streams once the client proves capable.
///
/// # Errors
/// [`NetError`] if the control stream cannot be accepted.
pub async fn accept_mux(conn: Connection) -> Result<QuicMux, NetError> {
    let (ctrl_send, ctrl_recv) = conn
        .accept_bi()
        .await
        .map_err(|e| NetError::Tls(format!("quic control stream: {e}")))?;
    Ok(build_mux(conn, ctrl_send, ctrl_recv))
}

/// Client side: open the control stream and build the symmetric mux. The client signals
/// multi-stream capability *after* CONNECT by opening a data stream (CONNECT must be the first
/// packet, so capability cannot be signalled before it); the broker then fans PUBLISH back
/// across data streams. A client that opens no data stream is served on the control stream.
///
/// # Errors
/// [`NetError`] if the control stream cannot be opened.
pub async fn connect_mux(conn: &Connection) -> Result<QuicMux, NetError> {
    let (ctrl_send, ctrl_recv) = conn
        .open_bi()
        .await
        .map_err(|e| NetError::Tls(format!("quic control stream: {e}")))?;
    Ok(build_mux(conn.clone(), ctrl_send, ctrl_recv))
}

/// Wire up the inbound forwarders (control + accepted data streams), the capability flag, and
/// the outbound writer task.
fn build_mux(conn: Connection, ctrl_send: SendStream, ctrl_recv: RecvStream) -> QuicMux {
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    spawn_frame_forwarder(ctrl_recv, in_tx.clone());

    // The peer is multi-stream capable once it opens a data stream (it then accepts ours).
    let capable = Arc::new(AtomicBool::new(false));

    let accept_conn = conn.clone();
    let accept_capable = capable.clone();
    tokio::spawn(async move {
        // Keep accepted data streams' send halves alive (dropping resets them); we read them.
        let mut keep = Vec::new();
        while let Ok((data_send, data_recv)) = accept_conn.accept_bi().await {
            accept_capable.store(true, Ordering::Relaxed);
            keep.push(data_send);
            spawn_frame_forwarder(data_recv, in_tx.clone());
        }
    });

    let (out_tx, out_rx) = mpsc::unbounded_channel();
    tokio::spawn(outbound_writer(conn.clone(), ctrl_send, capable, out_rx));

    QuicMux {
        rx: in_rx,
        read_cur: Bytes::new(),
        out_buf: BytesMut::new(),
        out_tx,
        conn,
    }
}

/// Read complete raw MQTT packets from one stream and forward them to the mux until EOF/error.
fn spawn_frame_forwarder(recv: RecvStream, tx: mpsc::UnboundedSender<Bytes>) {
    tokio::spawn(async move {
        // Version-agnostic: `next_raw_frame` parses only the fixed-header framing. The loop ends
        // on clean EOF or a framing error (the `Ok(Some(..))` pattern stops matching), or when
        // the session has gone (the channel send fails).
        let mut reader = FrameReader::new(recv, ProtocolVersion::V311);
        while let Ok(Some(frame)) = reader.next_raw_frame().await {
            if tx.send(frame).is_err() {
                break;
            }
        }
    });
}

/// Route outbound packets to QUIC streams: PUBLISH onto a topic-hashed data-stream pool (once
/// the peer is capable), everything else (control, `QoS` acks) on the control stream. Whole packets
/// only — never interleaved — so each stream carries a valid MQTT byte stream.
async fn outbound_writer(
    conn: Connection,
    mut ctrl_send: SendStream,
    capable: Arc<AtomicBool>,
    mut out_rx: mpsc::UnboundedReceiver<Bytes>,
) {
    let mut pool: Vec<Option<SendStream>> = (0..OUTBOUND_POOL).map(|_| None).collect();
    while let Some(frame) = out_rx.recv().await {
        // A PUBLISH (and only a PUBLISH) may fan out — and only to a capable peer.
        let slot = if capable.load(Ordering::Relaxed) {
            publish_topic(&frame).map(topic_slot)
        } else {
            None
        };
        let target: &mut SendStream = match slot {
            Some(i) => {
                if pool[i].is_none() {
                    if let Ok((send, _recv)) = conn.open_bi().await {
                        pool[i] = Some(send);
                    }
                }
                pool[i].as_mut().unwrap_or(&mut ctrl_send)
            }
            None => &mut ctrl_send,
        };
        if target.write_all(&frame).await.is_err() {
            break; // peer gone
        }
    }
}

/// The topic of a raw PUBLISH packet (`None` if `frame` is not a PUBLISH or is malformed).
fn publish_topic(frame: &[u8]) -> Option<&[u8]> {
    if frame.is_empty() || (frame[0] & 0xF0) != 0x30 {
        return None; // not a PUBLISH (control byte 0x3x)
    }
    // Skip the remaining-length varint (1–4 bytes).
    let mut i = 1usize;
    loop {
        let b = *frame.get(i)?;
        i += 1;
        if b & 0x80 == 0 {
            break;
        }
        if i > 4 {
            return None;
        }
    }
    // Topic = 2-byte big-endian length + that many bytes (the start of the variable header).
    let len = ((*frame.get(i)? as usize) << 8) | *frame.get(i + 1)? as usize;
    let start = i + 2;
    frame.get(start..start + len)
}

/// Hash a topic onto the outbound stream pool (FNV-1a; same topic → same slot → ordered).
fn topic_slot(topic: &[u8]) -> usize {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in topic {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // The modulus is < OUTBOUND_POOL (a small constant), so it always fits a usize.
    usize::try_from(h % OUTBOUND_POOL as u64).unwrap_or(0)
}

impl Drop for QuicMux {
    fn drop(&mut self) {
        // End the session: close the connection so the accept/reader/writer tasks stop.
        self.conn.close(0u32.into(), b"session ended");
    }
}

impl AsyncRead for QuicMux {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.read_cur.has_remaining() {
                let n = self.read_cur.remaining().min(buf.remaining());
                let chunk = self.read_cur.split_to(n);
                buf.put_slice(&chunk);
                return Poll::Ready(Ok(()));
            }
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(frame)) => self.read_cur = frame, // loop to serve it
                Poll::Ready(None) => return Poll::Ready(Ok(())),   // all streams closed → EOF
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for QuicMux {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Buffer the bytes, then hand every complete packet to the writer task, which routes each
        // to a stream (PUBLISH → topic-hashed data stream; else control). The actual send is the
        // writer task's job, so this never blocks the session loop.
        self.out_buf.extend_from_slice(buf);
        loop {
            match take_raw_frame(&mut self.out_buf) {
                Ok(Some(frame)) => {
                    if self.out_tx.send(frame).is_err() {
                        return Poll::Ready(Err(io::Error::other("quic outbound writer gone")));
                    }
                }
                Ok(None) => break, // need more bytes for the next packet
                Err(e) => return Poll::Ready(Err(io::Error::other(e))),
            }
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Each packet is written+sent by the writer task as it is dequeued; QUIC handles
        // reliable delivery, so there is nothing further to flush here.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Dropping the mux (closing out_tx + the connection) tears everything down.
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::{publish_topic, topic_slot, OUTBOUND_POOL};

    /// Build a minimal QoS-0 PUBLISH packet: control byte 0x30, remaining-length varint,
    /// then a 2-byte topic length prefix + topic bytes (no payload).
    fn publish(topic: &[u8]) -> Vec<u8> {
        let topic_len = u16::try_from(topic.len()).expect("test topic fits u16");
        let mut variable = Vec::new();
        variable.extend_from_slice(&topic_len.to_be_bytes());
        variable.extend_from_slice(topic);
        let mut frame = vec![0x30];
        let mut rem = variable.len();
        loop {
            let mut byte = u8::try_from(rem & 0x7f).expect("7-bit group fits u8");
            rem >>= 7;
            if rem > 0 {
                byte |= 0x80;
            }
            frame.push(byte);
            if rem == 0 {
                break;
            }
        }
        frame.extend_from_slice(&variable);
        frame
    }

    #[test]
    fn publish_topic_extracts_topic_across_varint_lengths() {
        assert_eq!(publish_topic(&publish(b"a/b")), Some(&b"a/b"[..]));
        // A long topic forces a 2-byte remaining-length varint; extraction must still land on it.
        let long = vec![b'x'; 300];
        assert_eq!(publish_topic(&publish(&long)), Some(&long[..]));
    }

    #[test]
    fn publish_topic_ignores_non_publish_packets() {
        assert_eq!(publish_topic(&[]), None);
        assert_eq!(publish_topic(&[0xC0, 0x00]), None); // PINGREQ
        assert_eq!(publish_topic(&[0x82, 0x00]), None); // SUBSCRIBE
    }

    #[test]
    fn topic_slot_is_stable_and_in_range() {
        for topic in [&b"quic/demo/a"[..], b"quic/demo/b", b"sensors/1/temp", b""] {
            let slot = topic_slot(topic);
            assert!(
                slot < OUTBOUND_POOL,
                "slot {slot} out of range for {topic:?}"
            );
            // Same topic always hashes to the same slot — this is what preserves per-topic order.
            assert_eq!(slot, topic_slot(topic));
        }
    }

    #[test]
    fn topic_slot_spreads_across_the_pool() {
        // Distinct topics should not all collapse onto a single stream.
        let slots: std::collections::HashSet<usize> = (0..64)
            .map(|i| topic_slot(format!("t/{i}").as_bytes()))
            .collect();
        assert!(slots.len() > 1, "topics all hashed to one slot: {slots:?}");
    }
}
