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
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::CertificateDer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

use crate::{FrameReader, NetError};
use mqtt_codec::ProtocolVersion;

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

/// Multi-stream demultiplexer for one MQTT-over-QUIC connection (ADR 0036): one MQTT session
/// spread across many QUIC streams. Each inbound stream is read at **packet granularity** (via
/// [`FrameReader::next_raw_frame`]) by its own task, and complete packets are merged — never
/// interleaved at the byte level — into one inbound byte stream. Because the per-stream readers
/// run concurrently, a stalled or large packet on one stream does **not** block packets on
/// another (QUIC's no-head-of-line-blocking benefit). Outbound packets are written on the
/// **control stream** (the first bidi stream); using data streams outbound is a later
/// enhancement. [`QuicMux`] is an `AsyncRead + AsyncWrite`, so `conn::handle_stream<S>` runs
/// over it unchanged.
#[derive(Debug)]
pub struct QuicMux {
    /// Complete inbound packets from every stream, in arrival order.
    rx: mpsc::UnboundedReceiver<Bytes>,
    /// The packet currently being served to the reader (drained as it is read).
    read_cur: Bytes,
    /// The control stream's send half — all outbound packets go here.
    ctrl_send: SendStream,
    /// Held so dropping the mux closes the connection, ending the accept/reader tasks.
    conn: Connection,
}

/// Accept the per-connection multi-stream mux: take the **control stream** (first bidi), spawn
/// a reader for it, and spawn an acceptor that takes every later **data stream** and spawns a
/// reader for it too — all forwarding complete packets into the mux. Returns once the control
/// stream is open.
///
/// # Errors
/// [`NetError`] if the control stream cannot be accepted.
pub async fn accept_mux(conn: Connection) -> Result<QuicMux, NetError> {
    let (ctrl_send, ctrl_recv) = conn
        .accept_bi()
        .await
        .map_err(|e| NetError::Tls(format!("quic control stream: {e}")))?;
    let (tx, rx) = mpsc::unbounded_channel();
    spawn_frame_forwarder(ctrl_recv, tx.clone());

    // Accept additional data streams for the life of the connection, each feeding the same
    // session. We read data streams; the outbound direction stays on the control stream (v1).
    // The data streams' send halves are kept alive (not dropped — dropping resets the stream,
    // which can abort the just-opened bidi before its inbound data is read).
    let accept_conn = conn.clone();
    tokio::spawn(async move {
        let mut data_sends = Vec::new();
        while let Ok((data_send, data_recv)) = accept_conn.accept_bi().await {
            data_sends.push(data_send);
            spawn_frame_forwarder(data_recv, tx.clone());
        }
    });

    Ok(QuicMux {
        rx,
        read_cur: Bytes::new(),
        ctrl_send,
        conn,
    })
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

impl Drop for QuicMux {
    fn drop(&mut self) {
        // End the session: close the connection so the accept loop and per-stream readers stop.
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
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Disambiguate to quinn's tokio `AsyncWrite` impl (its inherent `poll_write` returns
        // a different error type and would shadow the trait method).
        AsyncWrite::poll_write(Pin::new(&mut self.ctrl_send), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.ctrl_send), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.ctrl_send), cx)
    }
}
