//! MQTT 5.0 properties: the typed key/value block carried by most v5 packets
//! ([ADR 0008](../../../docs/adr/0008-mqtt-5-codec.md) §2).
//!
//! A v5 packet's variable header ends with a **properties block**: a variable byte
//! integer length, then that many bytes of `(identifier, value)` pairs. Each of the
//! 27 identifiers has a fixed value type (byte, two/four-byte integer, variable byte
//! integer, UTF-8 string, binary data, or a UTF-8 string pair).
//!
//! This module is the faithful wire model: a [`Property`] enum (one variant per
//! identifier, holding its typed value) and a [`Properties`] block codec. It is
//! **total and bounds-checked** like the rest of the codec — an attacker-controlled
//! identifier, length, or truncated value yields a [`CodecError`], never a panic.
//!
//! Which properties are valid on which packet, and the duplicate rules, are enforced
//! where the packet is assembled (the codec knows the packet type there); this layer
//! round-trips any well-formed block.

use crate::io::{self, Reader};
use crate::{varint, CodecError};
use bytes::Bytes;

/// A single MQTT 5.0 property: its identifier is implicit in the variant, its value
/// is the typed payload. Identifier bytes follow the spec (§2.2.2.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Property {
    /// 0x01 — payload is UTF-8 (1) or unspecified bytes (0).
    PayloadFormatIndicator(u8),
    /// 0x02 — message expiry interval, seconds.
    MessageExpiryInterval(u32),
    /// 0x03 — MIME content type of the payload.
    ContentType(String),
    /// 0x08 — topic for a request/response response.
    ResponseTopic(String),
    /// 0x09 — opaque correlation data for request/response.
    CorrelationData(Bytes),
    /// 0x0B — subscription identifier (variable byte integer).
    SubscriptionIdentifier(u32),
    /// 0x11 — session expiry interval, seconds.
    SessionExpiryInterval(u32),
    /// 0x12 — client identifier the server assigned.
    AssignedClientIdentifier(String),
    /// 0x13 — keep-alive the server is imposing, seconds.
    ServerKeepAlive(u16),
    /// 0x15 — extended-authentication method name.
    AuthenticationMethod(String),
    /// 0x16 — extended-authentication data.
    AuthenticationData(Bytes),
    /// 0x17 — whether the client wants problem information (reason strings, etc.).
    RequestProblemInformation(u8),
    /// 0x18 — will delay interval, seconds.
    WillDelayInterval(u32),
    /// 0x19 — whether the client wants response information.
    RequestResponseInformation(u8),
    /// 0x1A — response information (a response-topic prefix).
    ResponseInformation(String),
    /// 0x1C — another server the client should use.
    ServerReference(String),
    /// 0x1F — human-readable reason for a reason code.
    ReasonString(String),
    /// 0x21 — receive maximum (concurrent unacked `QoS` > 0).
    ReceiveMaximum(u16),
    /// 0x22 — highest topic alias the sender will accept.
    TopicAliasMaximum(u16),
    /// 0x23 — topic alias for this PUBLISH.
    TopicAlias(u16),
    /// 0x24 — maximum `QoS` the server supports.
    MaximumQoS(u8),
    /// 0x25 — whether the server supports retained messages.
    RetainAvailable(u8),
    /// 0x26 — a user-defined key/value pair (the only repeatable property; order is
    /// significant).
    UserProperty(String, String),
    /// 0x27 — maximum packet size the sender will accept.
    MaximumPacketSize(u32),
    /// 0x28 — whether the server supports wildcard subscriptions.
    WildcardSubscriptionAvailable(u8),
    /// 0x29 — whether the server supports subscription identifiers.
    SubscriptionIdentifierAvailable(u8),
    /// 0x2A — whether the server supports shared subscriptions.
    SharedSubscriptionAvailable(u8),
}

impl Property {
    /// The on-the-wire identifier byte.
    #[must_use]
    pub fn id(&self) -> u8 {
        match self {
            Property::PayloadFormatIndicator(_) => 0x01,
            Property::MessageExpiryInterval(_) => 0x02,
            Property::ContentType(_) => 0x03,
            Property::ResponseTopic(_) => 0x08,
            Property::CorrelationData(_) => 0x09,
            Property::SubscriptionIdentifier(_) => 0x0B,
            Property::SessionExpiryInterval(_) => 0x11,
            Property::AssignedClientIdentifier(_) => 0x12,
            Property::ServerKeepAlive(_) => 0x13,
            Property::AuthenticationMethod(_) => 0x15,
            Property::AuthenticationData(_) => 0x16,
            Property::RequestProblemInformation(_) => 0x17,
            Property::WillDelayInterval(_) => 0x18,
            Property::RequestResponseInformation(_) => 0x19,
            Property::ResponseInformation(_) => 0x1A,
            Property::ServerReference(_) => 0x1C,
            Property::ReasonString(_) => 0x1F,
            Property::ReceiveMaximum(_) => 0x21,
            Property::TopicAliasMaximum(_) => 0x22,
            Property::TopicAlias(_) => 0x23,
            Property::MaximumQoS(_) => 0x24,
            Property::RetainAvailable(_) => 0x25,
            Property::UserProperty(..) => 0x26,
            Property::MaximumPacketSize(_) => 0x27,
            Property::WildcardSubscriptionAvailable(_) => 0x28,
            Property::SubscriptionIdentifierAvailable(_) => 0x29,
            Property::SharedSubscriptionAvailable(_) => 0x2A,
        }
    }

    /// Append this property (identifier byte + value) to `out`.
    ///
    /// # Errors
    /// [`CodecError::ValueOutOfRange`] if a string/binary value exceeds 65,535 bytes.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        io::put_u8(out, self.id());
        match self {
            Property::PayloadFormatIndicator(v)
            | Property::RequestProblemInformation(v)
            | Property::RequestResponseInformation(v)
            | Property::MaximumQoS(v)
            | Property::RetainAvailable(v)
            | Property::WildcardSubscriptionAvailable(v)
            | Property::SubscriptionIdentifierAvailable(v)
            | Property::SharedSubscriptionAvailable(v) => io::put_u8(out, *v),
            Property::ServerKeepAlive(v)
            | Property::ReceiveMaximum(v)
            | Property::TopicAliasMaximum(v)
            | Property::TopicAlias(v) => io::put_u16(out, *v),
            Property::MessageExpiryInterval(v)
            | Property::SessionExpiryInterval(v)
            | Property::WillDelayInterval(v)
            | Property::MaximumPacketSize(v) => io::put_u32(out, *v),
            Property::SubscriptionIdentifier(v) => varint::encode(*v, out)?,
            Property::ContentType(v)
            | Property::ResponseTopic(v)
            | Property::AssignedClientIdentifier(v)
            | Property::AuthenticationMethod(v)
            | Property::ResponseInformation(v)
            | Property::ServerReference(v)
            | Property::ReasonString(v) => io::put_string(out, v)?,
            Property::CorrelationData(v) | Property::AuthenticationData(v) => {
                io::put_binary(out, v)?;
            }
            Property::UserProperty(key, value) => {
                io::put_string(out, key)?;
                io::put_string(out, value)?;
            }
        }
        Ok(())
    }

    /// Decode the property with identifier `id`, reading its value from `r`.
    ///
    /// # Errors
    /// [`CodecError::MalformedPacket`] for an unknown identifier or a truncated
    /// value; [`CodecError::InvalidUtf8`] for a non-UTF-8 string value.
    pub fn decode(id: u8, r: &mut Reader) -> Result<Self, CodecError> {
        Ok(match id {
            0x01 => Property::PayloadFormatIndicator(r.read_u8()?),
            0x02 => Property::MessageExpiryInterval(r.read_u32()?),
            0x03 => Property::ContentType(r.read_string()?),
            0x08 => Property::ResponseTopic(r.read_string()?),
            0x09 => Property::CorrelationData(r.read_binary()?),
            0x0B => Property::SubscriptionIdentifier(r.read_varint()?),
            0x11 => Property::SessionExpiryInterval(r.read_u32()?),
            0x12 => Property::AssignedClientIdentifier(r.read_string()?),
            0x13 => Property::ServerKeepAlive(r.read_u16()?),
            0x15 => Property::AuthenticationMethod(r.read_string()?),
            0x16 => Property::AuthenticationData(r.read_binary()?),
            0x17 => Property::RequestProblemInformation(r.read_u8()?),
            0x18 => Property::WillDelayInterval(r.read_u32()?),
            0x19 => Property::RequestResponseInformation(r.read_u8()?),
            0x1A => Property::ResponseInformation(r.read_string()?),
            0x1C => Property::ServerReference(r.read_string()?),
            0x1F => Property::ReasonString(r.read_string()?),
            0x21 => Property::ReceiveMaximum(r.read_u16()?),
            0x22 => Property::TopicAliasMaximum(r.read_u16()?),
            0x23 => Property::TopicAlias(r.read_u16()?),
            0x24 => Property::MaximumQoS(r.read_u8()?),
            0x25 => Property::RetainAvailable(r.read_u8()?),
            0x26 => Property::UserProperty(r.read_string()?, r.read_string()?),
            0x27 => Property::MaximumPacketSize(r.read_u32()?),
            0x28 => Property::WildcardSubscriptionAvailable(r.read_u8()?),
            0x29 => Property::SubscriptionIdentifierAvailable(r.read_u8()?),
            0x2A => Property::SharedSubscriptionAvailable(r.read_u8()?),
            _ => return Err(CodecError::MalformedPacket("unknown property identifier")),
        })
    }
}

/// A v5 properties block: a sequence of [`Property`]s, encoded with a variable byte
/// length prefix. An empty block is a valid `0x00` length byte — every v5 packet
/// that *can* carry properties always encodes the length, even when zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Properties(pub Vec<Property>);

impl Properties {
    /// An empty properties block.
    #[must_use]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Whether the block carries no properties.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The number of properties in the block.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The Session Expiry Interval (`0x11`) in seconds, if present (MQTT 5.0).
    #[must_use]
    pub fn session_expiry_interval(&self) -> Option<u32> {
        self.0.iter().find_map(|p| match p {
            Property::SessionExpiryInterval(v) => Some(*v),
            _ => None,
        })
    }

    /// The Message Expiry Interval (`0x02`) in seconds, if present (MQTT 5.0).
    #[must_use]
    pub fn message_expiry_interval(&self) -> Option<u32> {
        self.0.iter().find_map(|p| match p {
            Property::MessageExpiryInterval(v) => Some(*v),
            _ => None,
        })
    }

    /// The Authentication Method (`0x15`) for enhanced auth, if present (MQTT 5.0).
    #[must_use]
    pub fn authentication_method(&self) -> Option<&str> {
        self.0.iter().find_map(|p| match p {
            Property::AuthenticationMethod(s) => Some(s.as_str()),
            _ => None,
        })
    }

    /// The Authentication Data (`0x16`) for enhanced auth, if present (MQTT 5.0).
    #[must_use]
    pub fn authentication_data(&self) -> Option<&[u8]> {
        self.0.iter().find_map(|p| match p {
            Property::AuthenticationData(b) => Some(&b[..]),
            _ => None,
        })
    }

    /// The Receive Maximum (`0x21`) — concurrent unacked `QoS` > 0 the sender will
    /// accept — if present (MQTT 5.0).
    #[must_use]
    pub fn receive_maximum(&self) -> Option<u16> {
        self.0.iter().find_map(|p| match p {
            Property::ReceiveMaximum(v) => Some(*v),
            _ => None,
        })
    }

    /// The Topic Alias Maximum (`0x22`) the sender will accept, if present (MQTT 5.0).
    #[must_use]
    pub fn topic_alias_maximum(&self) -> Option<u16> {
        self.0.iter().find_map(|p| match p {
            Property::TopicAliasMaximum(v) => Some(*v),
            _ => None,
        })
    }

    /// The Maximum Packet Size (`0x27`) the sender will accept, if present (MQTT 5.0).
    #[must_use]
    pub fn maximum_packet_size(&self) -> Option<u32> {
        self.0.iter().find_map(|p| match p {
            Property::MaximumPacketSize(v) => Some(*v),
            _ => None,
        })
    }

    /// The Topic Alias (`0x23`) for this PUBLISH, if present (MQTT 5.0).
    #[must_use]
    pub fn topic_alias(&self) -> Option<u16> {
        self.0.iter().find_map(|p| match p {
            Property::TopicAlias(v) => Some(*v),
            _ => None,
        })
    }

    /// Append the length-prefixed block to `out`.
    ///
    /// # Errors
    /// [`CodecError`] if a property value is out of range, or the block exceeds the
    /// variable-byte-integer maximum.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        // Encode the body first so its length can prefix it.
        let mut body = Vec::new();
        for property in &self.0 {
            property.encode(&mut body)?;
        }
        let len = u32::try_from(body.len())
            .map_err(|_| CodecError::ValueOutOfRange("properties length"))?;
        varint::encode(len, out)?;
        out.extend_from_slice(&body);
        Ok(())
    }

    /// Decode a length-prefixed block from `r`, parsing exactly the declared number
    /// of bytes into properties.
    ///
    /// # Errors
    /// [`CodecError::MalformedPacket`] if the declared length overruns the packet, or
    /// a property within the block is truncated.
    pub fn decode(r: &mut Reader) -> Result<Self, CodecError> {
        let len = r.read_varint()? as usize;
        // Carve exactly the declared block so a property cannot read past it, and a
        // missing trailing byte is caught as an underflow within the sub-reader.
        let block = r.read_bytes(len)?;
        let mut block = Reader::new(block);
        let mut properties = Vec::new();
        while !block.is_empty() {
            let id = block.read_u8()?;
            properties.push(Property::decode(id, &mut block)?);
        }
        Ok(Self(properties))
    }

    /// [`decode`](Self::decode) the block, then [`validate_for`](Self::validate_for) the
    /// packet context — the form callers use when decoding a real packet (the codec knows
    /// the packet type there), so a property illegal on that packet is rejected at the
    /// wire boundary (ADR 0008 T7).
    ///
    /// # Errors
    /// As [`decode`](Self::decode), plus [`CodecError::ProtocolViolation`] if a property is
    /// not permitted on `ctx` or a non-repeatable property is duplicated.
    pub fn decode_for(r: &mut Reader, ctx: PropContext) -> Result<Self, CodecError> {
        let props = Self::decode(r)?;
        props.validate_for(ctx)?;
        Ok(props)
    }

    /// Validate this block against its packet context (MQTT 5.0 §2.2.2 / §3.x): every
    /// property must be permitted on that packet type, and a property that may not repeat
    /// must appear at most once. Either is a **Protocol Error**.
    ///
    /// The repeatable properties are User Property (everywhere) and Subscription Identifier
    /// (only on PUBLISH, where a single message may carry several).
    ///
    /// # Errors
    /// [`CodecError::ProtocolViolation`] on a disallowed or duplicated property.
    pub fn validate_for(&self, ctx: PropContext) -> Result<(), CodecError> {
        // Property identifiers are 0x01..=0x2A, so a u64 is a sufficient "seen" bitset.
        let mut seen: u64 = 0;
        for p in &self.0 {
            let id = p.id();
            if !ctx.allows(id) {
                return Err(CodecError::ProtocolViolation(
                    "property not allowed on this packet type",
                ));
            }
            if !ctx.repeatable(id) {
                let bit = 1u64 << id;
                if seen & bit != 0 {
                    return Err(CodecError::ProtocolViolation(
                        "duplicate of a non-repeatable property",
                    ));
                }
                seen |= bit;
            }
        }
        Ok(())
    }
}

/// The packet context a property block appears in — selects which properties are allowed
/// and which may repeat (MQTT 5.0 §3.x). `PubAck` covers PUBACK/PUBREC/PUBREL/PUBCOMP,
/// which share a property set; `Will` is the CONNECT payload's Will Properties block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropContext {
    /// CONNECT variable-header properties (§3.1.2.11).
    Connect,
    /// CONNECT payload Will Properties (§3.1.3.2).
    Will,
    /// CONNACK properties (§3.2.2.3).
    ConnAck,
    /// PUBLISH properties (§3.3.2.3).
    Publish,
    /// PUBACK / PUBREC / PUBREL / PUBCOMP properties (§3.4.2.2 etc.).
    PubAck,
    /// SUBSCRIBE properties (§3.8.2.1).
    Subscribe,
    /// SUBACK properties (§3.9.2.1).
    SubAck,
    /// UNSUBSCRIBE properties (§3.10.2.1).
    Unsubscribe,
    /// UNSUBACK properties (§3.11.2.1).
    UnsubAck,
    /// DISCONNECT properties (§3.14.2.2).
    Disconnect,
    /// AUTH properties (§3.15.2.2).
    Auth,
}

impl PropContext {
    /// Whether property identifier `id` is permitted on this packet type (MQTT 5.0 §3.x).
    #[must_use]
    pub fn allows(self, id: u8) -> bool {
        match self {
            // session-expiry, receive-max, max-packet-size, topic-alias-max, request-resp-info,
            // request-problem-info, user-property, auth-method, auth-data
            PropContext::Connect => {
                matches!(
                    id,
                    0x11 | 0x21 | 0x27 | 0x22 | 0x19 | 0x17 | 0x26 | 0x15 | 0x16
                )
            }
            // will-delay, payload-format, message-expiry, content-type, response-topic,
            // correlation-data, user-property
            PropContext::Will => matches!(id, 0x18 | 0x01 | 0x02 | 0x03 | 0x08 | 0x09 | 0x26),
            PropContext::ConnAck => matches!(
                id,
                0x11 | 0x21
                    | 0x24
                    | 0x25
                    | 0x27
                    | 0x12
                    | 0x22
                    | 0x1F
                    | 0x26
                    | 0x28
                    | 0x29
                    | 0x2A
                    | 0x13
                    | 0x1A
                    | 0x1C
                    | 0x15
                    | 0x16
            ),
            // payload-format, message-expiry, topic-alias, response-topic, correlation-data,
            // user-property, subscription-identifier, content-type
            PropContext::Publish => {
                matches!(id, 0x01 | 0x02 | 0x23 | 0x08 | 0x09 | 0x26 | 0x0B | 0x03)
            }
            // reason-string, user-property
            PropContext::PubAck | PropContext::SubAck | PropContext::UnsubAck => {
                matches!(id, 0x1F | 0x26)
            }
            // subscription-identifier, user-property
            PropContext::Subscribe => matches!(id, 0x0B | 0x26),
            // user-property only
            PropContext::Unsubscribe => id == 0x26,
            // session-expiry, reason-string, user-property, server-reference
            PropContext::Disconnect => matches!(id, 0x11 | 0x1F | 0x26 | 0x1C),
            // auth-method, auth-data, reason-string, user-property
            PropContext::Auth => matches!(id, 0x15 | 0x16 | 0x1F | 0x26),
        }
    }

    /// Whether property `id` may legitimately appear more than once on this packet:
    /// User Property (0x26) always; Subscription Identifier (0x0B) only on PUBLISH.
    #[must_use]
    pub fn repeatable(self, id: u8) -> bool {
        id == 0x26 || (id == 0x0B && self == PropContext::Publish)
    }
}

impl From<Vec<Property>> for Properties {
    fn from(properties: Vec<Property>) -> Self {
        Self(properties)
    }
}

#[cfg(test)]
mod tests {
    use super::{PropContext, Properties, Property};
    use crate::io::Reader;
    use crate::CodecError;
    use bytes::Bytes;

    /// Encode `props`, decode the bytes back, and assert the round-trip is exact.
    fn roundtrip(props: &Properties) {
        let mut out = Vec::new();
        props.encode(&mut out).unwrap();
        let mut r = Reader::new(Bytes::from(out));
        let back = Properties::decode(&mut r).unwrap();
        assert_eq!(&back, props);
        assert!(r.is_empty(), "decode must consume the whole block");
    }

    #[test]
    fn every_value_type_roundtrips() {
        roundtrip(&Properties(vec![
            Property::PayloadFormatIndicator(1),                   // byte
            Property::ServerKeepAlive(0x1234),                     // two-byte int
            Property::SessionExpiryInterval(0x1122_3344),          // four-byte int
            Property::SubscriptionIdentifier(268_435_455),         // varint (4-byte max)
            Property::ContentType("application/json".to_string()), // string
            Property::CorrelationData(Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF])), // binary
            Property::UserProperty("k".to_string(), "v".to_string()), // string pair
        ]));
    }

    // ---- packet-context validation (ADR 0008 T7) ----

    #[test]
    fn validate_accepts_properties_legal_on_the_packet() {
        // A representative legal CONNECT block.
        let p = Properties(vec![
            Property::SessionExpiryInterval(60),
            Property::ReceiveMaximum(10),
            Property::UserProperty("a".into(), "b".into()),
            Property::UserProperty("c".into(), "d".into()), // repeatable everywhere
        ]);
        assert!(p.validate_for(PropContext::Connect).is_ok());
    }

    #[test]
    fn validate_rejects_a_property_illegal_on_the_packet() {
        // ReasonString (0x1F) is not a CONNECT property.
        let p = Properties(vec![Property::ReasonString("nope".into())]);
        assert!(matches!(
            p.validate_for(PropContext::Connect),
            Err(CodecError::ProtocolViolation(_))
        ));
        // ...but it is legal on a DISCONNECT.
        assert!(p.validate_for(PropContext::Disconnect).is_ok());
    }

    #[test]
    fn validate_rejects_a_duplicated_non_repeatable_property() {
        let p = Properties(vec![
            Property::SessionExpiryInterval(1),
            Property::SessionExpiryInterval(2), // duplicate, non-repeatable
        ]);
        assert!(matches!(
            p.validate_for(PropContext::Connect),
            Err(CodecError::ProtocolViolation(_))
        ));
    }

    #[test]
    fn subscription_identifier_repeats_only_on_publish() {
        let two = Properties(vec![
            Property::SubscriptionIdentifier(1),
            Property::SubscriptionIdentifier(2),
        ]);
        // A PUBLISH may carry several (one per matching subscription).
        assert!(two.validate_for(PropContext::Publish).is_ok());
        // A SUBSCRIBE may carry at most one.
        assert!(matches!(
            two.validate_for(PropContext::Subscribe),
            Err(CodecError::ProtocolViolation(_))
        ));
    }

    #[test]
    fn decode_for_rejects_an_illegal_property_at_the_wire_boundary() {
        // Encode a ReasonString-only block, then decode it as a CONNECT context.
        let mut out = Vec::new();
        Properties(vec![Property::ReasonString("x".into())])
            .encode(&mut out)
            .unwrap();
        let mut r = Reader::new(Bytes::from(out));
        assert!(matches!(
            Properties::decode_for(&mut r, PropContext::Connect),
            Err(CodecError::ProtocolViolation(_))
        ));
    }

    #[test]
    fn empty_block_is_a_single_zero_length_byte() {
        let mut out = Vec::new();
        Properties::new().encode(&mut out).unwrap();
        assert_eq!(out, vec![0x00]);
        let mut r = Reader::new(Bytes::from(out));
        assert!(Properties::decode(&mut r).unwrap().is_empty());
    }

    #[test]
    fn user_properties_preserve_order_and_repeat() {
        // The only repeatable property; the codec keeps duplicates and their order.
        roundtrip(&Properties(vec![
            Property::UserProperty("a".to_string(), "1".to_string()),
            Property::UserProperty("a".to_string(), "2".to_string()),
            Property::UserProperty("b".to_string(), "3".to_string()),
        ]));
    }

    #[test]
    fn unknown_identifier_is_malformed() {
        // Length 1, identifier 0x99 (undefined).
        let mut r = Reader::new(Bytes::from_static(&[0x01, 0x99]));
        assert!(matches!(
            Properties::decode(&mut r),
            Err(CodecError::MalformedPacket(_))
        ));
    }

    #[test]
    fn truncated_value_is_malformed() {
        // Length 3: identifier 0x02 (four-byte int) but only two value bytes follow,
        // so reading the u32 underflows inside the carved block.
        let mut r = Reader::new(Bytes::from_static(&[0x03, 0x02, 0x00, 0x00]));
        assert!(matches!(
            Properties::decode(&mut r),
            Err(CodecError::MalformedPacket(_))
        ));
    }

    #[test]
    fn block_length_overrunning_the_packet_is_malformed() {
        // Declares 5 bytes of properties but only 2 follow.
        let mut r = Reader::new(Bytes::from_static(&[0x05, 0x01, 0x00]));
        assert!(matches!(
            Properties::decode(&mut r),
            Err(CodecError::MalformedPacket(_))
        ));
    }

    #[test]
    fn ids_match_the_spec() {
        assert_eq!(Property::PayloadFormatIndicator(0).id(), 0x01);
        assert_eq!(Property::SubscriptionIdentifier(1).id(), 0x0B);
        assert_eq!(
            Property::UserProperty(String::new(), String::new()).id(),
            0x26
        );
        assert_eq!(Property::SharedSubscriptionAvailable(1).id(), 0x2A);
    }
}
