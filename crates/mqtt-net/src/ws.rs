//! MQTT-over-WebSocket transport ([ADR 0035](../../../docs/adr/0035-websocket-transport.md)).
//!
//! Browsers can only speak MQTT-over-WebSockets (an HTTP `Upgrade` with subprotocol `mqtt`,
//! MQTT packets carried in binary frames). This module does the WebSocket handshake (via the
//! vetted `tokio-tungstenite` — the broker keeps owning the *MQTT* codec, not a second
//! network parser) and then presents the WebSocket as a plain byte stream
//! ([`WsByteStream`]) so the existing [`crate::FrameReader`]/[`crate::FrameWriter`] and
//! `conn::handle_stream` run over it **unchanged**.
//!
//! TLS for `wss://` is done by *our* rustls acceptor (ADR 0002) *before* the WebSocket
//! handshake, so `tokio-tungstenite` is built without any TLS feature — one TLS stack.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::{http, Message};
use tokio_tungstenite::{accept_hdr_async, WebSocketStream};

use crate::NetError;

/// Perform the server-side WebSocket handshake over `stream`, negotiating the `mqtt`
/// subprotocol, and return the connection as a byte stream ready for the MQTT engine.
///
/// # Errors
/// Fails if the client does not request the `mqtt` subprotocol, or the handshake errors.
// The handshake callback's `Err` is tungstenite's large `ErrorResponse` — its signature is
// fixed by the `Callback` trait, so the large-err lint does not apply to our design.
#[allow(clippy::result_large_err)]
pub async fn accept<S>(stream: S) -> Result<WsByteStream<S>, NetError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Negotiate the `mqtt` subprotocol (OASIS MQTT-over-WS binding): the client MUST offer it,
    // and we echo it back. A client that does not is not an MQTT client — reject the upgrade.
    let on_upgrade = |req: &Request, mut resp: Response| -> Result<Response, ErrorResponse> {
        let offers_mqtt = req
            .headers()
            .get_all("sec-websocket-protocol")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|p| p.trim().eq_ignore_ascii_case("mqtt"));
        if !offers_mqtt {
            let mut err = ErrorResponse::new(Some("missing 'mqtt' subprotocol".into()));
            *err.status_mut() = http::StatusCode::BAD_REQUEST;
            return Err(err);
        }
        resp.headers_mut().insert(
            "sec-websocket-protocol",
            http::HeaderValue::from_static("mqtt"),
        );
        Ok(resp)
    };

    let ws = accept_hdr_async(stream, on_upgrade)
        .await
        .map_err(|e| NetError::Tls(format!("websocket handshake: {e}")))?;
    Ok(WsByteStream::wrap(ws))
}

/// Adapts a [`WebSocketStream`] into an `AsyncRead + AsyncWrite` byte stream: inbound **binary**
/// frames are concatenated into the MQTT byte stream (a packet may span frames, a frame may
/// hold several packets); each write emits one binary frame; `Ping`/`Pong`/`Close` control
/// frames are handled transparently; a text frame is a protocol error (MQTT-over-WS is binary).
#[derive(Debug)]
pub struct WsByteStream<S> {
    ws: WebSocketStream<S>,
    /// Leftover bytes from the last binary frame not yet handed to the reader.
    read_rem: Vec<u8>,
    read_pos: usize,
}

impl<S> WsByteStream<S> {
    /// Wrap an already-handshaken [`WebSocketStream`] as an MQTT byte stream. [`accept`] uses
    /// this server-side; a client (or a test) that did its own WS handshake can wrap the
    /// resulting stream the same way and drive it with [`crate::FrameReader`]/[`FrameWriter`].
    ///
    /// [`FrameWriter`]: crate::FrameWriter
    pub fn wrap(ws: WebSocketStream<S>) -> Self {
        Self {
            ws,
            read_rem: Vec::new(),
            read_pos: 0,
        }
    }
}

impl<S> AsyncRead for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            // Serve any buffered bytes from a prior binary frame first.
            if self.read_pos < self.read_rem.len() {
                let n = (self.read_rem.len() - self.read_pos).min(buf.remaining());
                let start = self.read_pos;
                buf.put_slice(&self.read_rem[start..start + n]);
                self.read_pos += n;
                return Poll::Ready(Ok(()));
            }
            self.read_rem.clear();
            self.read_pos = 0;

            match self.ws.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(msg))) => match msg {
                    Message::Binary(data) => {
                        if data.is_empty() {
                            continue; // empty frame carries no MQTT bytes
                        }
                        self.read_rem = data;
                        // loop: serve from the freshly-buffered frame
                    }
                    // tungstenite auto-queues a Pong for an inbound Ping; nudge a flush so it
                    // is actually sent even if the app side is otherwise idle.
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                        let _ = self.ws.poll_flush_unpin(cx);
                    }
                    Message::Close(_) => return Poll::Ready(Ok(())), // EOF
                    Message::Text(_) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "text frame on an MQTT WebSocket (binary only)",
                        )));
                    }
                },
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(io::Error::other(e)));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())), // stream ended → EOF
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S> AsyncWrite for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Each write becomes one binary WebSocket frame. Gate on sink readiness, then enqueue;
        // the actual send happens on flush (which FrameWriter calls after a packet).
        match self.ws.poll_ready_unpin(cx) {
            Poll::Ready(Ok(())) => {
                let msg = Message::Binary(buf.to_vec());
                self.ws.start_send_unpin(msg).map_err(io::Error::other)?;
                Poll::Ready(Ok(buf.len()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(e))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.ws.poll_flush_unpin(cx).map_err(io::Error::other)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.ws.poll_close_unpin(cx).map_err(io::Error::other)
    }
}
