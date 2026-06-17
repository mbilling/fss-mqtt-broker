//! MQTT wire protocol codec for versions 3.1.1 and 5.0.
//!
//! This crate is the **untrusted-input boundary** of the broker: every byte that
//! arrives from a client is parsed here. It is therefore the highest-value fuzzing
//! target and is held to a strict no-panic, no-`unsafe` standard.
//!
//! # Layout
//! - [`varint`] — variable byte integer encode/decode (the remaining-length field)
//! - [`io`] — bounds-checked readers and writers for MQTT primitive types
//! - [`properties`] — the MQTT 5.0 typed property block (ADR 0008)
//! - [`packet`] — fixed header, the [`Packet`] enum, and per-packet codecs
//!
//! The top-level entry points are [`Packet::decode`] and [`Packet::encode`].

pub mod io;
pub mod packet;
pub mod properties;
pub mod varint;

pub use packet::{FixedHeader, Packet, PacketType};
pub use properties::{Properties, Property};

/// Supported MQTT protocol levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    /// MQTT 3.1.1 (protocol level 4).
    V311,
    /// MQTT 5.0 (protocol level 5).
    V5,
}

impl ProtocolVersion {
    /// The on-the-wire protocol level byte.
    #[must_use]
    pub fn level(self) -> u8 {
        match self {
            ProtocolVersion::V311 => 4,
            ProtocolVersion::V5 => 5,
        }
    }

    /// Parse a protocol level byte from a CONNECT packet.
    #[must_use]
    pub fn from_level(level: u8) -> Option<Self> {
        match level {
            4 => Some(ProtocolVersion::V311),
            5 => Some(ProtocolVersion::V5),
            _ => None,
        }
    }
}

/// `QoS` levels defined by the MQTT specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QoS {
    /// At most once delivery.
    AtMostOnce = 0,
    /// At least once delivery.
    AtLeastOnce = 1,
    /// Exactly once delivery.
    ExactlyOnce = 2,
}

impl QoS {
    /// Convert a raw 2-bit `QoS` value into a [`QoS`].
    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(QoS::AtMostOnce),
            1 => Some(QoS::AtLeastOnce),
            2 => Some(QoS::ExactlyOnce),
            _ => None,
        }
    }
}

/// Errors produced while decoding or encoding an MQTT packet.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// The buffer does not yet contain a full packet; caller should read more.
    #[error("incomplete packet: need more bytes")]
    Incomplete,
    /// A malformed remaining-length variable byte integer.
    #[error("malformed variable byte integer")]
    MalformedVarInt,
    /// The protocol level byte was not a supported version.
    #[error("unsupported protocol level: {0}")]
    UnsupportedProtocol(u8),
    /// A reserved or invalid value was encountered, with a human-readable reason.
    #[error("protocol violation: {0}")]
    ProtocolViolation(&'static str),
    /// A malformed packet that violates the structural rules of the spec.
    #[error("malformed packet: {0}")]
    MalformedPacket(&'static str),
    /// A length-prefixed string was not valid UTF-8.
    #[error("invalid UTF-8 in string field")]
    InvalidUtf8,
    /// The packet declared a remaining length larger than the configured maximum.
    #[error("packet exceeds maximum allowed size")]
    PacketTooLarge,
    /// A value did not encode within the bounds of its field (e.g. a string > 65535 bytes).
    #[error("value out of range for its field: {0}")]
    ValueOutOfRange(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_roundtrip() {
        assert_eq!(ProtocolVersion::from_level(4), Some(ProtocolVersion::V311));
        assert_eq!(ProtocolVersion::from_level(5), Some(ProtocolVersion::V5));
        assert_eq!(ProtocolVersion::from_level(3), None);
        assert_eq!(ProtocolVersion::V311.level(), 4);
        assert_eq!(ProtocolVersion::V5.level(), 5);
    }

    #[test]
    fn qos_from_u8() {
        assert_eq!(QoS::from_u8(0), Some(QoS::AtMostOnce));
        assert_eq!(QoS::from_u8(2), Some(QoS::ExactlyOnce));
        assert_eq!(QoS::from_u8(3), None);
    }
}
