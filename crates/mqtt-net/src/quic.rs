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

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::CertificateDer;

use crate::NetError;

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
