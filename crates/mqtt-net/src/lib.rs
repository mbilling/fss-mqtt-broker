//! Connection and listener layer.
//!
//! Owns the lifecycle of client connections across transports: TCP, TLS 1.3
//! (via `rustls`), and WebSocket (later milestone). [`FrameReader`] and
//! [`FrameWriter`] read/write [`mqtt_codec::Packet`]s over any
//! `AsyncRead`/`AsyncWrite`; the [`tls`] module is the single place TLS
//! acceptors/connectors are built (ADR 0002).

mod frame;
pub mod tls;
pub use frame::{FrameReader, FrameWriter};

/// The transport a client connection arrived over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// Plaintext TCP — only available when explicitly enabled. Insecure.
    PlainTcp,
    /// TLS over TCP (the default, secure transport).
    Tls,
    /// WebSocket over TLS.
    WebSocketTls,
}

impl Transport {
    /// Whether this transport encrypts traffic in flight.
    #[must_use]
    pub fn is_encrypted(self) -> bool {
        matches!(self, Transport::Tls | Transport::WebSocketTls)
    }
}

/// Errors from the network layer.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    /// Binding a listener failed.
    #[error("failed to bind listener: {0}")]
    Bind(String),
    /// A TLS handshake or configuration error.
    #[error("tls error: {0}")]
    Tls(String),
    /// An underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// A malformed packet from the peer.
    #[error("codec error: {0}")]
    Codec(#[from] mqtt_codec::CodecError),
    /// The peer closed the connection in the middle of a packet.
    #[error("connection closed mid-packet")]
    UnexpectedEof,
}

#[cfg(test)]
mod tests {
    use super::Transport;

    #[test]
    fn only_tls_is_encrypted() {
        assert!(!Transport::PlainTcp.is_encrypted());
        assert!(Transport::Tls.is_encrypted());
        assert!(Transport::WebSocketTls.is_encrypted());
    }
}
