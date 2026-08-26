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

    /// The exact byte length [`encode`](Self::encode) would append — identifier
    /// byte included — performing the SAME value validations, so a property that
    /// would fail to encode fails here first, before any caller has written a
    /// prefix it cannot honour (issue #445).
    pub(crate) fn encoded_len(&self) -> Result<usize, CodecError> {
        fn prefixed(len: usize) -> Result<usize, CodecError> {
            if u16::try_from(len).is_err() {
                return Err(CodecError::ValueOutOfRange("binary data length"));
            }
            Ok(2 + len)
        }
        Ok(1 + match self {
            Property::PayloadFormatIndicator(_)
            | Property::RequestProblemInformation(_)
            | Property::RequestResponseInformation(_)
            | Property::MaximumQoS(_)
            | Property::RetainAvailable(_)
            | Property::WildcardSubscriptionAvailable(_)
            | Property::SubscriptionIdentifierAvailable(_)
            | Property::SharedSubscriptionAvailable(_) => 1,
            Property::ServerKeepAlive(_)
            | Property::ReceiveMaximum(_)
            | Property::TopicAliasMaximum(_)
            | Property::TopicAlias(_) => 2,
            Property::MessageExpiryInterval(_)
            | Property::SessionExpiryInterval(_)
            | Property::WillDelayInterval(_)
            | Property::MaximumPacketSize(_) => 4,
            Property::SubscriptionIdentifier(v) => {
                if *v > varint::MAX {
                    return Err(CodecError::ValueOutOfRange("variable byte integer"));
                }
                varint::encoded_len(*v)
            }
            Property::ContentType(v)
            | Property::ResponseTopic(v)
            | Property::AssignedClientIdentifier(v)
            | Property::AuthenticationMethod(v)
            | Property::ResponseInformation(v)
            | Property::ServerReference(v)
            | Property::ReasonString(v) => prefixed(v.len())?,
            Property::CorrelationData(v) | Property::AuthenticationData(v) => prefixed(v.len())?,
            Property::UserProperty(key, value) => prefixed(key.len())? + prefixed(value.len())?,
        })
    }

    /// Decode the property with identifier `id`, reading its value from `r`.
    ///
    /// # Errors
    /// [`CodecError::MalformedPacket`] for an unknown identifier or a truncated
    /// value; [`CodecError::InvalidUtf8`] for a non-UTF-8 string value;
    /// [`CodecError::ProtocolViolation`] for a single-byte flag property carrying
    /// a value other than 0 or 1.
    pub fn decode(id: u8, r: &mut Reader) -> Result<Self, CodecError> {
        Ok(match id {
            0x01 => {
                Property::PayloadFormatIndicator(bool_byte(r.read_u8()?, "PayloadFormatIndicator")?)
            }
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
            0x17 => Property::RequestProblemInformation(bool_byte(
                r.read_u8()?,
                "RequestProblemInformation",
            )?),
            0x18 => Property::WillDelayInterval(r.read_u32()?),
            0x19 => Property::RequestResponseInformation(bool_byte(
                r.read_u8()?,
                "RequestResponseInformation",
            )?),
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

/// A single-byte flag property carries **only** 0 or 1; any other value is a
/// Protocol Error, not a "close enough to true" (spec §3.1.2.11.6, §3.1.2.11.7,
/// §3.3.2.3.2). Coercing `2` to true would let two wire encodings mean the same
/// thing, and a peer that round-trips the property would then alter it.
fn bool_byte(v: u8, what: &'static str) -> Result<u8, CodecError> {
    match v {
        0 | 1 => Ok(v),
        _ => Err(CodecError::ProtocolViolation(what)),
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

    /// The bytes this block contributes to a byte-based queue bound (issue #241): the
    /// variable-length halves only, with no per-property wire framing.
    ///
    /// The match is **exhaustive on purpose** — no `_ =>` arm — so a new property that
    /// carries a string or a binary blob fails to compile here rather than silently
    /// under-counting and letting a byte cap be evaded. Fixed-width properties
    /// (integers, flags) are covered by the per-entry envelope the caller adds, so they
    /// count 0 here.
    #[must_use]
    pub fn accounted_bytes(&self) -> usize {
        self.0
            .iter()
            .map(|p| match p {
                Property::ContentType(s)
                | Property::ResponseTopic(s)
                | Property::AssignedClientIdentifier(s)
                | Property::AuthenticationMethod(s)
                | Property::ResponseInformation(s)
                | Property::ServerReference(s)
                | Property::ReasonString(s) => s.len(),
                Property::CorrelationData(b) | Property::AuthenticationData(b) => b.len(),
                Property::UserProperty(k, v) => k.len() + v.len(),
                // ONE byte, matching `AppProperties::accounted_bytes`'s
                // `usize::from(self.payload_format.is_some())`. The two functions are
                // documented as one shared definition and a test pins the identity, but that
                // test built its packet with `Properties::new()` — so it could not observe
                // this property, and the two disagreed by exactly 1 for any message carrying
                // a payload-format indicator (which hub.rs sets on forwarded publishes).
                Property::PayloadFormatIndicator(_) => 1,
                Property::MessageExpiryInterval(_)
                | Property::SubscriptionIdentifier(_)
                | Property::SessionExpiryInterval(_)
                | Property::ServerKeepAlive(_)
                | Property::RequestProblemInformation(_)
                | Property::WillDelayInterval(_)
                | Property::RequestResponseInformation(_)
                | Property::ReceiveMaximum(_)
                | Property::TopicAliasMaximum(_)
                | Property::TopicAlias(_)
                | Property::MaximumQoS(_)
                | Property::RetainAvailable(_)
                | Property::MaximumPacketSize(_)
                | Property::WildcardSubscriptionAvailable(_)
                | Property::SubscriptionIdentifierAvailable(_)
                | Property::SharedSubscriptionAvailable(_) => 0,
            })
            .sum()
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

    /// Whether this block carries a Subscription Identifier (`0x0B`) at all (MQTT 5.0
    /// §3.8.2.1.2 on SUBSCRIBE, §3.3.2.3.8 on PUBLISH).
    ///
    /// The packet's Subscription Identifier, if one is present. SUBSCRIBE carries at
    /// most one (`decode_for` rejects duplicates there, 0008-T7), and it applies to
    /// every filter in the packet (§3.8.2.1.2) — so the first match IS the value
    /// (issue #266). The repeatable PUBLISH multiset still belongs with the code
    /// that delivers them (the broker attaches ids; it never reads them off a
    /// publish).
    #[must_use]
    pub fn subscription_identifier(&self) -> Option<u32> {
        self.0.iter().find_map(|p| match p {
            Property::SubscriptionIdentifier(v) => Some(*v),
            _ => None,
        })
    }

    /// Whether any Subscription Identifier is present — the refusal predicate the
    /// publisher-side `0x82` guard uses ([MQTT-3.3.4-6], issues #245/#266).
    #[must_use]
    pub fn has_subscription_identifier(&self) -> bool {
        self.0
            .iter()
            .any(|p| matches!(p, Property::SubscriptionIdentifier(_)))
    }

    /// Append the length-prefixed block to `out`.
    ///
    /// # Errors
    /// [`CodecError`] if a property value is out of range, or the block exceeds the
    /// variable-byte-integer maximum.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        // One pass to size, one to write — the temporary body Vec this replaces
        // copied every property twice (issue #445). Sizing first also front-runs
        // every value error, so nothing is appended unless the block encodes.
        let len = self.encoded_body_len()?;
        let len32 =
            u32::try_from(len).map_err(|_| CodecError::ValueOutOfRange("properties length"))?;
        varint::encode(len32, out)?;
        let start = out.len();
        for property in &self.0 {
            property.encode(out)?;
        }
        debug_assert_eq!(
            out.len() - start,
            len,
            "Property::encoded_len drifted from Property::encode"
        );
        Ok(())
    }

    /// The byte length of the property block's BODY (the bytes after the length
    /// prefix), with the same validations encoding performs (issue #445).
    pub(crate) fn encoded_body_len(&self) -> Result<usize, CodecError> {
        self.0.iter().try_fold(0, |n, p| Ok(n + p.encoded_len()?))
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
    /// One value-level rule lives here too, because the spec states it as a Protocol Error
    /// rather than a malformed value: a Subscription Identifier of 0 (§3.8.2.1.2,
    /// §3.3.2.3.8).
    ///
    /// # Errors
    /// [`CodecError::ProtocolViolation`] on a disallowed or duplicated property, or a
    /// Subscription Identifier of 0.
    pub fn validate_for(&self, ctx: PropContext) -> Result<(), CodecError> {
        // Property identifiers are 0x01..=0x2A, so a u64 is a sufficient "seen" bitset.
        let mut seen: u64 = 0;
        for p in &self.0 {
            // §3.8.2.1.2 (SUBSCRIBE) and §3.3.2.3.8 (PUBLISH), both verbatim: "The
            // Subscription Identifier can have the value of 1 to 268,435,455. It is a
            // Protocol Error if the Subscription Identifier has a value of 0." The upper
            // bound is the varint 4-byte maximum, already enforced by `varint::decode`,
            // so only the zero needs checking here. Context-independent on purpose: the
            // PUBLISH rule stands on its own, and `mqtt-bridge` decodes genuine
            // server->client PUBLISHes in `PropContext::Publish` (issue #245).
            if matches!(p, Property::SubscriptionIdentifier(0)) {
                return Err(CodecError::ProtocolViolation(
                    "subscription identifier of 0",
                ));
            }
            // §3.2.2.3.12, same sentence that makes an absent `0x29` mean "supported":
            // "It is a Protocol Error to include the Subscription Identifier Available
            // more than once, or to send a value other than 0 or 1." Duplication is
            // caught by `repeatable()` below; the VALUE was accepted unchecked. This is
            // the receiving half of the property mqttd now emits, so it matters for
            // `mqtt-bridge` reading a remote broker's CONNACK (issue #245 round 2).
            if let Property::SubscriptionIdentifierAvailable(v) = p {
                if *v > 1 {
                    return Err(CodecError::ProtocolViolation(
                        "subscription identifier available must be 0 or 1",
                    ));
                }
            }
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
            //
            // 0x0B stays allowed on PUBLISH in BOTH directions on purpose (issue #245): the
            // codec is a codec. The [MQTT-3.3.4-6] "a client must not send one" guard lives
            // in the broker's ingest path (mqttd conn.rs), so the encode side stays ready to
            // emit identifiers and `mqtt-bridge`'s inbound decode keeps working. Do not
            // "tidy" 0x0B out of this arm, nor out of `repeatable` below.
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

    // ---- subscription-identifier value range (issue #245) ----
    //
    // MQTT 5.0 §3.8.2.1.2 (SUBSCRIBE) and §3.3.2.3.8 (PUBLISH), both verbatim: "The
    // Subscription Identifier can have the value of 1 to 268,435,455. It is a Protocol
    // Error if the Subscription Identifier has a value of 0."

    /// Encode a `Properties` block carrying only `SubscriptionIdentifier(id)` and read it
    /// back through `decode_for` in `ctx` — the wire boundary a real peer crosses.
    fn decode_sub_id_for(id: u32, ctx: PropContext) -> Result<Properties, CodecError> {
        let mut out = Vec::new();
        Properties(vec![Property::SubscriptionIdentifier(id)])
            .encode(&mut out)
            .unwrap();
        let mut r = Reader::new(Bytes::from(out));
        Properties::decode_for(&mut r, ctx)
    }

    #[test]
    fn subscription_identifier_zero_is_a_protocol_error_on_subscribe() {
        assert!(matches!(
            decode_sub_id_for(0, PropContext::Subscribe),
            Err(CodecError::ProtocolViolation(_))
        ));
    }

    #[test]
    fn subscription_identifier_zero_is_a_protocol_error_on_publish() {
        // §3.3.2.3.8 states the value-0 Protocol Error for PUBLISH independently of
        // SUBSCRIBE. This is the assertion that protects `mqtt-bridge`, which decodes
        // genuine server->client PUBLISHes from remote brokers in exactly this context.
        assert!(matches!(
            decode_sub_id_for(0, PropContext::Publish),
            Err(CodecError::ProtocolViolation(_))
        ));
    }

    /// §3.2.2.3.12: "It is a Protocol Error … to send a value other than 0 or 1" for
    /// Subscription Identifier Available. This is the RECEIVING half of the property
    /// mqttd now emits — `mqtt-bridge` reads a remote broker's CONNACK — and the value
    /// was previously accepted unchecked (issue #245 round 2).
    #[test]
    fn subscription_identifier_available_rejects_values_other_than_zero_or_one() {
        for v in [2u8, 7, 255] {
            let props = Properties(vec![Property::SubscriptionIdentifierAvailable(v)]);
            assert!(
                matches!(
                    props.validate_for(PropContext::ConnAck),
                    Err(CodecError::ProtocolViolation(_))
                ),
                "0x29 = {v} must be a Protocol Error"
            );
        }
        // Both legal values still pass — the check must not be written over-broadly.
        for v in [0u8, 1] {
            let props = Properties(vec![Property::SubscriptionIdentifierAvailable(v)]);
            assert!(
                props.validate_for(PropContext::ConnAck).is_ok(),
                "0x29 = {v}"
            );
        }
    }

    /// Guard (passes today): the range check must not be written over-broadly. The
    /// spec's upper bound is the varint 4-byte maximum, already enforced by
    /// `varint::decode`, so 268,435,455 must still decode in both contexts.
    #[test]
    fn subscription_identifier_at_the_varint_maximum_still_decodes() {
        for ctx in [PropContext::Subscribe, PropContext::Publish] {
            let props = decode_sub_id_for(268_435_455, ctx).expect("the spec's upper bound");
            assert_eq!(
                props.0,
                vec![Property::SubscriptionIdentifier(268_435_455)],
                "round-trips unchanged in {ctx:?}"
            );
        }
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
