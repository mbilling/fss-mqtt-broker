//! Fixed header, the [`Packet`] enum, and per-packet encode/decode.
//!
//! This module implements the complete **MQTT 3.1.1** and **MQTT 5.0** control
//! packet sets (ADR 0008): every packet's variable header and payload, version-tagged
//! through [`Packet::encode`]/[`Packet::decode`]. The v5 additions — property blocks,
//! reason codes, subscription options, and the AUTH packet — are carried on the same
//! types, defaulted/empty for v3.1.1 so the older wire is unaffected.

use crate::io::{self, Reader};
use crate::{varint, CodecError, Properties, ProtocolVersion, QoS};
use bytes::{Buf, Bytes, BytesMut};

/// The fixed-header packet type (the high nibble of the first byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    /// Client request to connect to the server.
    Connect,
    /// Connect acknowledgment.
    ConnAck,
    /// Publish message.
    Publish,
    /// Publish acknowledgment (`QoS` 1).
    PubAck,
    /// Publish received (`QoS` 2, part 1).
    PubRec,
    /// Publish release (`QoS` 2, part 2).
    PubRel,
    /// Publish complete (`QoS` 2, part 3).
    PubComp,
    /// Client subscribe request.
    Subscribe,
    /// Subscribe acknowledgment.
    SubAck,
    /// Client unsubscribe request.
    Unsubscribe,
    /// Unsubscribe acknowledgment.
    UnsubAck,
    /// PING request.
    PingReq,
    /// PING response.
    PingResp,
    /// Disconnect notification.
    Disconnect,
    /// Authentication exchange (MQTT 5.0 only).
    Auth,
}

impl PacketType {
    fn from_nibble(n: u8) -> Result<Self, CodecError> {
        Ok(match n {
            1 => PacketType::Connect,
            2 => PacketType::ConnAck,
            3 => PacketType::Publish,
            4 => PacketType::PubAck,
            5 => PacketType::PubRec,
            6 => PacketType::PubRel,
            7 => PacketType::PubComp,
            8 => PacketType::Subscribe,
            9 => PacketType::SubAck,
            10 => PacketType::Unsubscribe,
            11 => PacketType::UnsubAck,
            12 => PacketType::PingReq,
            13 => PacketType::PingResp,
            14 => PacketType::Disconnect,
            15 => PacketType::Auth,
            _ => return Err(CodecError::MalformedPacket("invalid packet type 0")),
        })
    }

    fn to_nibble(self) -> u8 {
        match self {
            PacketType::Connect => 1,
            PacketType::ConnAck => 2,
            PacketType::Publish => 3,
            PacketType::PubAck => 4,
            PacketType::PubRec => 5,
            PacketType::PubRel => 6,
            PacketType::PubComp => 7,
            PacketType::Subscribe => 8,
            PacketType::SubAck => 9,
            PacketType::Unsubscribe => 10,
            PacketType::UnsubAck => 11,
            PacketType::PingReq => 12,
            PacketType::PingResp => 13,
            PacketType::Disconnect => 14,
            PacketType::Auth => 15,
        }
    }
}

/// A decoded MQTT fixed header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedHeader {
    /// The control packet type.
    pub packet_type: PacketType,
    /// The four low flag bits of the first byte (meaning is packet-type specific).
    pub flags: u8,
    /// The remaining length: bytes of variable header + payload following the header.
    pub remaining_len: u32,
}

impl FixedHeader {
    /// Attempt to decode a fixed header from the front of `buf` without consuming it.
    ///
    /// Returns `Ok(None)` if `buf` does not yet contain a complete fixed header.
    /// On success returns the header and the number of bytes it occupies.
    ///
    /// # Errors
    /// Returns [`CodecError`] if the packet type or remaining-length is malformed.
    pub fn decode(buf: &[u8]) -> Result<Option<(FixedHeader, usize)>, CodecError> {
        let Some(&first) = buf.first() else {
            return Ok(None);
        };
        let packet_type = PacketType::from_nibble(first >> 4)?;
        let flags = first & 0x0F;
        let (remaining_len, consumed) = match varint::decode(&buf[1..]) {
            Ok(v) => v,
            Err(CodecError::Incomplete) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(Some((
            FixedHeader {
                packet_type,
                flags,
                remaining_len,
            },
            1 + consumed,
        )))
    }
}

/// The Last Will and Testament carried in a CONNECT packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastWill {
    /// Topic the will message is published to on ungraceful disconnect.
    pub topic: String,
    /// Will message payload.
    pub payload: Bytes,
    /// `QoS` to publish the will at.
    pub qos: QoS,
    /// Whether the will is retained.
    pub retain: bool,
    /// Will properties (MQTT 5.0; e.g. will delay interval). Empty for v3.1.1.
    pub properties: Properties,
}

/// A CONNECT packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    /// Negotiated protocol version (from the wire).
    pub protocol: ProtocolVersion,
    /// Clean session (MQTT 3.1.1) / clean start (MQTT 5.0).
    pub clean_session: bool,
    /// Keepalive interval in seconds (0 disables).
    pub keep_alive: u16,
    /// Client identifier (may be empty for a server-assigned id).
    pub client_id: String,
    /// Optional Last Will and Testament.
    pub last_will: Option<LastWill>,
    /// Optional username.
    pub username: Option<String>,
    /// Optional password.
    pub password: Option<Bytes>,
    /// CONNECT properties (MQTT 5.0; e.g. session expiry, receive maximum). Empty
    /// for v3.1.1.
    pub properties: Properties,
}

/// A CONNACK packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    /// Whether the server already had stored session state for this client.
    pub session_present: bool,
    /// Return code (MQTT 3.1.1) / reason code (MQTT 5.0). 0 = success.
    pub code: u8,
    /// CONNACK properties (MQTT 5.0; e.g. assigned client id, server keep-alive).
    /// Empty for v3.1.1.
    pub properties: Properties,
}

/// A PUBLISH packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    /// Duplicate-delivery flag.
    pub dup: bool,
    /// Quality of service.
    pub qos: QoS,
    /// Retain flag.
    pub retain: bool,
    /// Destination topic name (no wildcards).
    pub topic: String,
    /// Packet identifier, present iff `qos > 0`.
    pub pkid: Option<u16>,
    /// PUBLISH properties (MQTT 5.0; e.g. topic alias, content type, message
    /// expiry). Empty for v3.1.1.
    pub properties: Properties,
    /// Application payload.
    pub payload: Bytes,
}

/// A PUBACK / PUBREC / PUBREL / PUBCOMP packet — the four `QoS` acknowledgements
/// share an identical shape: a packet identifier, plus (MQTT 5.0) a reason code and
/// a properties block. For v3.1.1 the packet is just the 2-byte `pkid`; the `reason`
/// (0 = success) and `properties` are unused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    /// Packet identifier being acknowledged.
    pub pkid: u16,
    /// Reason code (MQTT 5.0; 0 = success). Always 0 on v3.1.1.
    pub reason: u8,
    /// Acknowledgement properties (MQTT 5.0; e.g. reason string). Empty for v3.1.1.
    pub properties: Properties,
}

impl Ack {
    /// A success acknowledgement of `pkid` (reason 0, no properties) — the v3.1.1
    /// form and the v5 short form.
    #[must_use]
    pub fn new(pkid: u16) -> Self {
        Self {
            pkid,
            reason: 0,
            properties: Properties::new(),
        }
    }
}

impl From<u16> for Ack {
    fn from(pkid: u16) -> Self {
        Self::new(pkid)
    }
}

/// MQTT 5.0 subscription options carried in each SUBSCRIBE filter's option byte
/// (bits above the requested `QoS`). All default to the v3.1.1-equivalent meaning,
/// so v4 filters leave them unset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SubscriptionOptions {
    /// No Local: do not forward a message back to the connection that published it.
    pub no_local: bool,
    /// Retain As Published: keep the original PUBLISH retain flag when forwarding.
    pub retain_as_published: bool,
    /// Retain Handling (0/1/2): when retained messages are sent at subscribe time.
    pub retain_handling: u8,
}

/// A single entry in a SUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeFilter {
    /// Topic filter (may contain `+`/`#` wildcards).
    pub path: String,
    /// Maximum `QoS` requested for this subscription.
    pub qos: QoS,
    /// MQTT 5.0 subscription options; default (all off) for v3.1.1.
    pub options: SubscriptionOptions,
}

/// A SUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// Packet identifier.
    pub pkid: u16,
    /// One or more topic filters (a SUBSCRIBE with zero filters is malformed).
    pub filters: Vec<SubscribeFilter>,
    /// SUBSCRIBE properties (MQTT 5.0; e.g. subscription identifier). Empty for v3.1.1.
    pub properties: Properties,
}

/// A SUBACK packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    /// Packet identifier matching the SUBSCRIBE.
    pub pkid: u16,
    /// Per-filter return / reason codes (0/1/2 = granted `QoS`, 0x80 = failure;
    /// MQTT 5.0 adds a richer set in the same byte).
    pub return_codes: Vec<u8>,
    /// SUBACK properties (MQTT 5.0; e.g. reason string). Empty for v3.1.1.
    pub properties: Properties,
}

/// An UNSUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    /// Packet identifier.
    pub pkid: u16,
    /// Topic filters to remove (a packet with zero filters is malformed).
    pub filters: Vec<String>,
    /// UNSUBSCRIBE properties (MQTT 5.0; user properties only). Empty for v3.1.1.
    pub properties: Properties,
}

/// An UNSUBACK packet. v3.1.1 carries only the packet identifier; MQTT 5.0 adds a
/// properties block and a reason code per unsubscribed filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    /// Packet identifier matching the UNSUBSCRIBE.
    pub pkid: u16,
    /// Per-filter reason codes (MQTT 5.0; one per UNSUBSCRIBE filter). Empty for
    /// v3.1.1, which has no per-filter acknowledgement.
    pub reason_codes: Vec<u8>,
    /// UNSUBACK properties (MQTT 5.0; e.g. reason string). Empty for v3.1.1.
    pub properties: Properties,
}

impl UnsubAck {
    /// A v3.1.1-style acknowledgement of `pkid` (no reason codes or properties).
    #[must_use]
    pub fn new(pkid: u16) -> Self {
        Self {
            pkid,
            reason_codes: Vec::new(),
            properties: Properties::new(),
        }
    }
}

impl From<u16> for UnsubAck {
    fn from(pkid: u16) -> Self {
        Self::new(pkid)
    }
}

/// A DISCONNECT packet. v3.1.1 carries no payload; MQTT 5.0 adds an optional reason
/// code and properties — both omitted (the short form) when the reason is Normal
/// Disconnection (0) with no properties, giving the same empty body as v3.1.1.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Disconnect {
    /// Reason code (MQTT 5.0; 0 = Normal Disconnection). Always 0 on v3.1.1.
    pub reason: u8,
    /// DISCONNECT properties (MQTT 5.0; e.g. session expiry, reason string). Empty
    /// for v3.1.1.
    pub properties: Properties,
}

/// An AUTH packet — an extended-authentication exchange (**MQTT 5.0 only**). Carries
/// a reason code and properties; the short form omits both when the reason is Success
/// (0) with no properties.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Auth {
    /// Reason code (0 = Success, 0x18 = Continue authentication, 0x19 = Re-auth).
    pub reason: u8,
    /// AUTH properties (e.g. authentication method/data, reason string).
    pub properties: Properties,
}

/// A decoded MQTT control packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    /// CONNECT.
    Connect(Connect),
    /// CONNACK.
    ConnAck(ConnAck),
    /// PUBLISH.
    Publish(Publish),
    /// PUBACK (`QoS` 1 acknowledgement).
    PubAck(Ack),
    /// PUBREC (`QoS` 2, part 1).
    PubRec(Ack),
    /// PUBREL (`QoS` 2, part 2).
    PubRel(Ack),
    /// PUBCOMP (`QoS` 2, part 3).
    PubComp(Ack),
    /// SUBSCRIBE.
    Subscribe(Subscribe),
    /// SUBACK.
    SubAck(SubAck),
    /// UNSUBSCRIBE.
    Unsubscribe(Unsubscribe),
    /// UNSUBACK.
    UnsubAck(UnsubAck),
    /// PINGREQ.
    PingReq,
    /// PINGRESP.
    PingResp,
    /// DISCONNECT.
    Disconnect(Disconnect),
    /// AUTH (MQTT 5.0 only).
    Auth(Auth),
}

impl Packet {
    /// The packet type discriminant.
    #[must_use]
    pub fn packet_type(&self) -> PacketType {
        match self {
            Packet::Connect(_) => PacketType::Connect,
            Packet::ConnAck(_) => PacketType::ConnAck,
            Packet::Publish(_) => PacketType::Publish,
            Packet::PubAck(_) => PacketType::PubAck,
            Packet::PubRec(_) => PacketType::PubRec,
            Packet::PubRel(_) => PacketType::PubRel,
            Packet::PubComp(_) => PacketType::PubComp,
            Packet::Subscribe(_) => PacketType::Subscribe,
            Packet::SubAck(_) => PacketType::SubAck,
            Packet::Unsubscribe(_) => PacketType::Unsubscribe,
            Packet::UnsubAck(_) => PacketType::UnsubAck,
            Packet::PingReq => PacketType::PingReq,
            Packet::PingResp => PacketType::PingResp,
            Packet::Disconnect(_) => PacketType::Disconnect,
            Packet::Auth(_) => PacketType::Auth,
        }
    }

    /// Decode the next packet from a streaming buffer.
    ///
    /// Returns `Ok(None)` if `buf` does not yet hold a complete packet (the caller
    /// should read more bytes and retry). On success the packet's bytes are
    /// consumed from `buf` and the decoded [`Packet`] is returned.
    ///
    /// `version` is the connection's negotiated protocol version, used for every
    /// packet except CONNECT (which carries its own version on the wire).
    ///
    /// # Errors
    /// Returns [`CodecError`] for malformed input or unsupported protocol features.
    pub fn decode(
        buf: &mut BytesMut,
        version: ProtocolVersion,
    ) -> Result<Option<Packet>, CodecError> {
        let Some((header, header_len)) = FixedHeader::decode(buf)? else {
            return Ok(None);
        };
        let total = header_len + header.remaining_len as usize;
        if buf.len() < total {
            return Ok(None);
        }

        // Carve off exactly this packet; leave the rest of the stream in `buf`.
        let mut frame = buf.split_to(total).freeze();
        frame.advance(header_len);
        let mut r = Reader::new(frame);

        let packet = match header.packet_type {
            PacketType::Connect => Packet::Connect(decode_connect(&mut r)?),
            PacketType::ConnAck => {
                expect_flags(header.flags, 0)?;
                Packet::ConnAck(decode_connack(&mut r, version)?)
            }
            PacketType::Publish => Packet::Publish(decode_publish(version, header.flags, &mut r)?),
            PacketType::PubAck => {
                expect_flags(header.flags, 0)?;
                Packet::PubAck(decode_ack(version, &mut r)?)
            }
            PacketType::PubRec => {
                expect_flags(header.flags, 0)?;
                Packet::PubRec(decode_ack(version, &mut r)?)
            }
            PacketType::PubRel => {
                expect_flags(header.flags, 0x02)?;
                Packet::PubRel(decode_ack(version, &mut r)?)
            }
            PacketType::PubComp => {
                expect_flags(header.flags, 0)?;
                Packet::PubComp(decode_ack(version, &mut r)?)
            }
            PacketType::Subscribe => {
                expect_flags(header.flags, 0x02)?;
                Packet::Subscribe(decode_subscribe(version, &mut r)?)
            }
            PacketType::SubAck => {
                expect_flags(header.flags, 0)?;
                Packet::SubAck(decode_suback(version, &mut r)?)
            }
            PacketType::Unsubscribe => {
                expect_flags(header.flags, 0x02)?;
                Packet::Unsubscribe(decode_unsubscribe(version, &mut r)?)
            }
            PacketType::UnsubAck => {
                expect_flags(header.flags, 0)?;
                Packet::UnsubAck(decode_unsuback(version, &mut r)?)
            }
            PacketType::PingReq => {
                expect_flags(header.flags, 0)?;
                expect_empty(&r)?;
                Packet::PingReq
            }
            PacketType::PingResp => {
                expect_flags(header.flags, 0)?;
                expect_empty(&r)?;
                Packet::PingResp
            }
            PacketType::Disconnect => {
                expect_flags(header.flags, 0)?;
                Packet::Disconnect(decode_disconnect(version, &mut r)?)
            }
            PacketType::Auth => {
                // AUTH is an MQTT 5.0 control packet; it does not exist in v3.1.1.
                if version != ProtocolVersion::V5 {
                    return Err(CodecError::ProtocolViolation("AUTH is MQTT 5.0 only"));
                }
                expect_flags(header.flags, 0)?;
                let (reason, properties) = decode_reason_and_properties(&mut r)?;
                Packet::Auth(Auth { reason, properties })
            }
        };
        Ok(Some(packet))
    }

    /// Encode this packet into `out` for the given protocol `version`.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a field is out of range, or an MQTT 5.0-only packet
    /// (AUTH) is encoded for a v3.1.1 connection.
    pub fn encode(&self, out: &mut Vec<u8>, version: ProtocolVersion) -> Result<(), CodecError> {
        // AUTH does not exist in v3.1.1.
        if version != ProtocolVersion::V5 && matches!(self, Packet::Auth(_)) {
            return Err(CodecError::ProtocolViolation("AUTH is MQTT 5.0 only"));
        }
        // Build the body separately so we can prefix it with the remaining length.
        let mut body = Vec::new();
        let flags = match self {
            Packet::Connect(c) => {
                encode_connect(c, &mut body)?;
                0
            }
            Packet::ConnAck(a) => {
                encode_connack(a, &mut body, version)?;
                0
            }
            Packet::Publish(p) => {
                encode_publish(p, &mut body, version)?;
                publish_flags(p)
            }
            // PUBACK, PUBREC and PUBCOMP share an identical body and zero flags.
            Packet::PubAck(a) | Packet::PubRec(a) | Packet::PubComp(a) => {
                encode_ack(a, &mut body, version)?;
                0
            }
            // PUBREL is the same body but, like SUBSCRIBE/UNSUBSCRIBE, requires flags 0x02.
            Packet::PubRel(a) => {
                encode_ack(a, &mut body, version)?;
                0x02
            }
            Packet::UnsubAck(a) => {
                encode_unsuback(a, &mut body, version)?;
                0
            }
            Packet::Subscribe(s) => {
                encode_subscribe(s, &mut body, version)?;
                0x02
            }
            Packet::SubAck(a) => {
                encode_suback(a, &mut body, version)?;
                0
            }
            Packet::Unsubscribe(u) => {
                encode_unsubscribe(u, &mut body, version)?;
                0x02
            }
            Packet::Disconnect(d) => {
                encode_disconnect(d, &mut body, version)?;
                0
            }
            Packet::Auth(a) => {
                encode_reason_and_properties(a.reason, &a.properties, &mut body)?;
                0
            }
            Packet::PingReq | Packet::PingResp => 0,
        };

        let remaining = u32::try_from(body.len())
            .map_err(|_| CodecError::ValueOutOfRange("remaining length"))?;
        io::put_u8(out, (self.packet_type().to_nibble() << 4) | flags);
        varint::encode(remaining, out)?;
        out.extend_from_slice(&body);
        Ok(())
    }
}

fn expect_flags(actual: u8, expected: u8) -> Result<(), CodecError> {
    if actual == expected {
        Ok(())
    } else {
        Err(CodecError::MalformedPacket(
            "reserved fixed-header flags set",
        ))
    }
}

fn expect_empty(r: &Reader) -> Result<(), CodecError> {
    if r.is_empty() {
        Ok(())
    } else {
        Err(CodecError::MalformedPacket("unexpected payload bytes"))
    }
}

/// Decode a PUBACK/PUBREC/PUBREL/PUBCOMP body. v3.1.1 is just the packet id. v5
/// adds an optional reason code and properties: the reason and the property length
/// may both be omitted (Success, no properties) — remaining length 2 — and the
/// property length alone may be omitted when no properties follow.
fn decode_ack(version: ProtocolVersion, r: &mut Reader) -> Result<Ack, CodecError> {
    let pkid = r.read_u16()?;
    if version != ProtocolVersion::V5 {
        expect_empty(r)?;
        return Ok(Ack::new(pkid));
    }
    // v5 short form: remaining length 2 means reason 0x00 (Success), no properties.
    if r.is_empty() {
        return Ok(Ack::new(pkid));
    }
    let reason = r.read_u8()?;
    // Remaining length 3 (reason present, property length omitted) means no properties.
    let properties = if r.is_empty() {
        Properties::new()
    } else {
        Properties::decode(r)?
    };
    expect_empty(r)?;
    Ok(Ack {
        pkid,
        reason,
        properties,
    })
}

/// Encode a PUBACK/PUBREC/PUBREL/PUBCOMP body. v3.1.1 writes only the packet id; v5
/// adds the reason code and properties unless both are absent (Success, no
/// properties), in which case it emits the 2-byte short form.
fn encode_ack(a: &Ack, out: &mut Vec<u8>, version: ProtocolVersion) -> Result<(), CodecError> {
    io::put_u16(out, a.pkid);
    if version == ProtocolVersion::V5 && (a.reason != 0 || !a.properties.is_empty()) {
        io::put_u8(out, a.reason);
        a.properties.encode(out)?;
    }
    Ok(())
}

fn decode_connect(r: &mut Reader) -> Result<Connect, CodecError> {
    let proto_name = r.read_string()?;
    if proto_name != "MQTT" {
        return Err(CodecError::ProtocolViolation("unsupported protocol name"));
    }
    let level = r.read_u8()?;
    let protocol =
        ProtocolVersion::from_level(level).ok_or(CodecError::UnsupportedProtocol(level))?;
    let is_v5 = protocol == ProtocolVersion::V5;

    let flags = r.read_u8()?;
    if flags & 0x01 != 0 {
        return Err(CodecError::MalformedPacket("CONNECT reserved flag set"));
    }
    let clean_session = flags & 0x02 != 0;
    let will_flag = flags & 0x04 != 0;
    let will_qos_bits = (flags >> 3) & 0x03;
    let will_retain = flags & 0x20 != 0;
    let password_flag = flags & 0x40 != 0;
    let username_flag = flags & 0x80 != 0;

    if !will_flag && (will_qos_bits != 0 || will_retain) {
        return Err(CodecError::MalformedPacket(
            "will qos/retain set without will flag",
        ));
    }

    let keep_alive = r.read_u16()?;
    // The CONNECT properties block (v5) ends the variable header, before the payload.
    let properties = if is_v5 {
        Properties::decode(r)?
    } else {
        Properties::new()
    };
    let client_id = r.read_string()?;

    let last_will = if will_flag {
        let qos =
            QoS::from_u8(will_qos_bits).ok_or(CodecError::MalformedPacket("invalid will QoS"))?;
        // Will properties (v5) precede the will topic in the payload.
        let will_properties = if is_v5 {
            Properties::decode(r)?
        } else {
            Properties::new()
        };
        let topic = r.read_string()?;
        let payload = r.read_binary()?;
        Some(LastWill {
            topic,
            payload,
            qos,
            retain: will_retain,
            properties: will_properties,
        })
    } else {
        None
    };

    let username = if username_flag {
        Some(r.read_string()?)
    } else {
        None
    };
    // MQTT 3.1.1 allows a password only when a username is present.
    if password_flag && !username_flag {
        return Err(CodecError::MalformedPacket(
            "password flag set without username flag",
        ));
    }
    let password = if password_flag {
        Some(r.read_binary()?)
    } else {
        None
    };

    expect_empty(r)?;
    Ok(Connect {
        protocol,
        clean_session,
        keep_alive,
        client_id,
        last_will,
        username,
        password,
        properties,
    })
}

fn encode_connect(c: &Connect, out: &mut Vec<u8>) -> Result<(), CodecError> {
    let is_v5 = c.protocol == ProtocolVersion::V5;
    io::put_string(out, "MQTT")?;
    io::put_u8(out, c.protocol.level());

    let mut flags = 0u8;
    if c.clean_session {
        flags |= 0x02;
    }
    if let Some(w) = &c.last_will {
        flags |= 0x04;
        flags |= (w.qos as u8) << 3;
        if w.retain {
            flags |= 0x20;
        }
    }
    if c.username.is_some() {
        flags |= 0x80;
    }
    if c.password.is_some() {
        flags |= 0x40;
    }
    io::put_u8(out, flags);
    io::put_u16(out, c.keep_alive);
    if is_v5 {
        c.properties.encode(out)?;
    }

    io::put_string(out, &c.client_id)?;
    if let Some(w) = &c.last_will {
        if is_v5 {
            w.properties.encode(out)?;
        }
        io::put_string(out, &w.topic)?;
        io::put_binary(out, &w.payload)?;
    }
    if let Some(u) = &c.username {
        io::put_string(out, u)?;
    }
    if let Some(p) = &c.password {
        io::put_binary(out, p)?;
    }
    Ok(())
}

fn decode_connack(r: &mut Reader, version: ProtocolVersion) -> Result<ConnAck, CodecError> {
    let ack_flags = r.read_u8()?;
    if ack_flags & 0xFE != 0 {
        return Err(CodecError::MalformedPacket("CONNACK reserved flags set"));
    }
    let code = r.read_u8()?;
    let properties = if version == ProtocolVersion::V5 {
        Properties::decode(r)?
    } else {
        Properties::new()
    };
    expect_empty(r)?;
    Ok(ConnAck {
        session_present: ack_flags & 0x01 != 0,
        code,
        properties,
    })
}

fn encode_connack(
    a: &ConnAck,
    out: &mut Vec<u8>,
    version: ProtocolVersion,
) -> Result<(), CodecError> {
    io::put_u8(out, u8::from(a.session_present));
    io::put_u8(out, a.code);
    if version == ProtocolVersion::V5 {
        a.properties.encode(out)?;
    }
    Ok(())
}

fn publish_flags(p: &Publish) -> u8 {
    let mut f = (p.qos as u8) << 1;
    if p.dup {
        f |= 0x08;
    }
    if p.retain {
        f |= 0x01;
    }
    f
}

fn decode_publish(
    version: ProtocolVersion,
    flags: u8,
    r: &mut Reader,
) -> Result<Publish, CodecError> {
    let dup = flags & 0x08 != 0;
    let qos = QoS::from_u8((flags >> 1) & 0x03)
        .ok_or(CodecError::MalformedPacket("invalid PUBLISH QoS (3)"))?;
    let retain = flags & 0x01 != 0;
    if qos == QoS::AtMostOnce && dup {
        return Err(CodecError::MalformedPacket("DUP set on QoS 0 PUBLISH"));
    }

    let topic = r.read_string()?;
    let pkid = if qos == QoS::AtMostOnce {
        None
    } else {
        let id = r.read_u16()?;
        if id == 0 {
            return Err(CodecError::MalformedPacket(
                "zero packet id on QoS>0 PUBLISH",
            ));
        }
        Some(id)
    };
    // The PUBLISH properties block (v5) sits between the packet id and the payload.
    let properties = if version == ProtocolVersion::V5 {
        Properties::decode(r)?
    } else {
        Properties::new()
    };
    let payload = r.read_remaining();
    Ok(Publish {
        dup,
        qos,
        retain,
        topic,
        pkid,
        properties,
        payload,
    })
}

fn encode_publish(
    p: &Publish,
    out: &mut Vec<u8>,
    version: ProtocolVersion,
) -> Result<(), CodecError> {
    io::put_string(out, &p.topic)?;
    match (p.qos, p.pkid) {
        (QoS::AtMostOnce, None) => {}
        (QoS::AtMostOnce, Some(_)) => {
            return Err(CodecError::ProtocolViolation(
                "packet id present on QoS 0 PUBLISH",
            ))
        }
        (_, Some(id)) => io::put_u16(out, id),
        (_, None) => {
            return Err(CodecError::ProtocolViolation(
                "missing packet id on QoS>0 PUBLISH",
            ))
        }
    }
    if version == ProtocolVersion::V5 {
        p.properties.encode(out)?;
    }
    out.extend_from_slice(&p.payload);
    Ok(())
}

fn decode_subscribe(version: ProtocolVersion, r: &mut Reader) -> Result<Subscribe, CodecError> {
    let is_v5 = version == ProtocolVersion::V5;
    let pkid = r.read_u16()?;
    let properties = if is_v5 {
        Properties::decode(r)?
    } else {
        Properties::new()
    };
    let mut filters = Vec::new();
    while !r.is_empty() {
        let path = r.read_string()?;
        let byte = r.read_u8()?;
        let qos = QoS::from_u8(byte & 0x03)
            .ok_or(CodecError::MalformedPacket("invalid requested QoS"))?;
        let options = if is_v5 {
            // v5 option byte: bits 2 No-Local, 3 Retain-As-Published, 4-5 Retain
            // Handling (0/1/2), 6-7 reserved.
            if byte & 0xC0 != 0 {
                return Err(CodecError::MalformedPacket(
                    "reserved SUBSCRIBE option bits",
                ));
            }
            let retain_handling = (byte >> 4) & 0x03;
            if retain_handling == 3 {
                return Err(CodecError::ProtocolViolation(
                    "invalid SUBSCRIBE retain handling (3)",
                ));
            }
            SubscriptionOptions {
                no_local: byte & 0x04 != 0,
                retain_as_published: byte & 0x08 != 0,
                retain_handling,
            }
        } else {
            // v3.1.1: only the low two QoS bits are defined; the rest are reserved.
            if byte & 0xFC != 0 {
                return Err(CodecError::MalformedPacket(
                    "reserved SUBSCRIBE option bits",
                ));
            }
            SubscriptionOptions::default()
        };
        filters.push(SubscribeFilter { path, qos, options });
    }
    if filters.is_empty() {
        return Err(CodecError::ProtocolViolation("SUBSCRIBE with no filters"));
    }
    Ok(Subscribe {
        pkid,
        filters,
        properties,
    })
}

fn encode_subscribe(
    s: &Subscribe,
    out: &mut Vec<u8>,
    version: ProtocolVersion,
) -> Result<(), CodecError> {
    if s.filters.is_empty() {
        return Err(CodecError::ProtocolViolation("SUBSCRIBE with no filters"));
    }
    io::put_u16(out, s.pkid);
    if version == ProtocolVersion::V5 {
        s.properties.encode(out)?;
    }
    for f in &s.filters {
        io::put_string(out, &f.path)?;
        let mut byte = f.qos as u8;
        if version == ProtocolVersion::V5 {
            if f.options.no_local {
                byte |= 0x04;
            }
            if f.options.retain_as_published {
                byte |= 0x08;
            }
            byte |= (f.options.retain_handling & 0x03) << 4;
        }
        io::put_u8(out, byte);
    }
    Ok(())
}

fn decode_suback(version: ProtocolVersion, r: &mut Reader) -> Result<SubAck, CodecError> {
    let pkid = r.read_u16()?;
    let properties = if version == ProtocolVersion::V5 {
        Properties::decode(r)?
    } else {
        Properties::new()
    };
    let mut return_codes = Vec::new();
    while !r.is_empty() {
        return_codes.push(r.read_u8()?);
    }
    if return_codes.is_empty() {
        return Err(CodecError::ProtocolViolation("SUBACK with no return codes"));
    }
    Ok(SubAck {
        pkid,
        return_codes,
        properties,
    })
}

fn encode_suback(
    a: &SubAck,
    out: &mut Vec<u8>,
    version: ProtocolVersion,
) -> Result<(), CodecError> {
    io::put_u16(out, a.pkid);
    if version == ProtocolVersion::V5 {
        a.properties.encode(out)?;
    }
    out.extend_from_slice(&a.return_codes);
    Ok(())
}

fn decode_unsubscribe(version: ProtocolVersion, r: &mut Reader) -> Result<Unsubscribe, CodecError> {
    let pkid = r.read_u16()?;
    let properties = if version == ProtocolVersion::V5 {
        Properties::decode(r)?
    } else {
        Properties::new()
    };
    let mut filters = Vec::new();
    while !r.is_empty() {
        filters.push(r.read_string()?);
    }
    if filters.is_empty() {
        return Err(CodecError::ProtocolViolation("UNSUBSCRIBE with no filters"));
    }
    Ok(Unsubscribe {
        pkid,
        filters,
        properties,
    })
}

fn encode_unsubscribe(
    u: &Unsubscribe,
    out: &mut Vec<u8>,
    version: ProtocolVersion,
) -> Result<(), CodecError> {
    if u.filters.is_empty() {
        return Err(CodecError::ProtocolViolation("UNSUBSCRIBE with no filters"));
    }
    io::put_u16(out, u.pkid);
    if version == ProtocolVersion::V5 {
        u.properties.encode(out)?;
    }
    for f in &u.filters {
        io::put_string(out, f)?;
    }
    Ok(())
}

fn decode_unsuback(version: ProtocolVersion, r: &mut Reader) -> Result<UnsubAck, CodecError> {
    let pkid = r.read_u16()?;
    if version != ProtocolVersion::V5 {
        // v3.1.1 UNSUBACK is just the packet id — no per-filter codes.
        expect_empty(r)?;
        return Ok(UnsubAck::new(pkid));
    }
    let properties = Properties::decode(r)?;
    let mut reason_codes = Vec::new();
    while !r.is_empty() {
        reason_codes.push(r.read_u8()?);
    }
    if reason_codes.is_empty() {
        return Err(CodecError::ProtocolViolation(
            "UNSUBACK with no reason codes",
        ));
    }
    Ok(UnsubAck {
        pkid,
        reason_codes,
        properties,
    })
}

fn encode_unsuback(
    a: &UnsubAck,
    out: &mut Vec<u8>,
    version: ProtocolVersion,
) -> Result<(), CodecError> {
    io::put_u16(out, a.pkid);
    if version == ProtocolVersion::V5 {
        a.properties.encode(out)?;
        out.extend_from_slice(&a.reason_codes);
    }
    Ok(())
}

/// Decode a v5 "reason code + properties" body (shared by DISCONNECT and AUTH), with
/// the short forms: an empty body is reason 0 with no properties; a reason with no
/// trailing bytes omits the property length (no properties).
fn decode_reason_and_properties(r: &mut Reader) -> Result<(u8, Properties), CodecError> {
    if r.is_empty() {
        return Ok((0, Properties::new()));
    }
    let reason = r.read_u8()?;
    let properties = if r.is_empty() {
        Properties::new()
    } else {
        Properties::decode(r)?
    };
    expect_empty(r)?;
    Ok((reason, properties))
}

/// Encode a v5 "reason code + properties" body: omitted entirely (the short form)
/// when the reason is 0 and there are no properties.
fn encode_reason_and_properties(
    reason: u8,
    properties: &Properties,
    out: &mut Vec<u8>,
) -> Result<(), CodecError> {
    if reason != 0 || !properties.is_empty() {
        io::put_u8(out, reason);
        properties.encode(out)?;
    }
    Ok(())
}

fn decode_disconnect(version: ProtocolVersion, r: &mut Reader) -> Result<Disconnect, CodecError> {
    if version != ProtocolVersion::V5 {
        // v3.1.1 DISCONNECT has no payload.
        expect_empty(r)?;
        return Ok(Disconnect::default());
    }
    let (reason, properties) = decode_reason_and_properties(r)?;
    Ok(Disconnect { reason, properties })
}

fn encode_disconnect(
    d: &Disconnect,
    out: &mut Vec<u8>,
    version: ProtocolVersion,
) -> Result<(), CodecError> {
    if version == ProtocolVersion::V5 {
        encode_reason_and_properties(d.reason, &d.properties, out)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Property;

    const V4: ProtocolVersion = ProtocolVersion::V311;
    const V5: ProtocolVersion = ProtocolVersion::V5;

    fn roundtrip(packet: &Packet) {
        roundtrip_version(packet, V4);
    }

    fn roundtrip_version(packet: &Packet, version: ProtocolVersion) {
        let mut out = Vec::new();
        packet.encode(&mut out, version).unwrap();
        let mut buf = BytesMut::from(&out[..]);
        let decoded = Packet::decode(&mut buf, version).unwrap().unwrap();
        assert_eq!(&decoded, packet);
        assert!(buf.is_empty(), "decode left trailing bytes");
    }

    #[test]
    fn roundtrip_connect_minimal() {
        roundtrip(&Packet::Connect(Connect {
            protocol: V4,
            clean_session: true,
            keep_alive: 60,
            client_id: "client-1".into(),
            last_will: None,
            username: None,
            password: None,
            properties: Properties::new(),
        }));
    }

    #[test]
    fn roundtrip_connect_full() {
        roundtrip(&Packet::Connect(Connect {
            protocol: V4,
            clean_session: false,
            keep_alive: 30,
            client_id: "c".into(),
            last_will: Some(LastWill {
                topic: "will/topic".into(),
                payload: Bytes::from_static(b"bye"),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties: Properties::new(),
            }),
            username: Some("alice".into()),
            password: Some(Bytes::from_static(b"s3cret")),
            properties: Properties::new(),
        }));
    }

    #[test]
    fn roundtrip_connack() {
        roundtrip(&Packet::ConnAck(ConnAck {
            session_present: true,
            code: 0,
            properties: Properties::new(),
        }));
    }

    #[test]
    fn roundtrip_connect_v5_with_properties() {
        roundtrip_version(
            &Packet::Connect(Connect {
                protocol: V5,
                clean_session: true,
                keep_alive: 30,
                client_id: "c5".into(),
                last_will: Some(LastWill {
                    topic: "will/t".into(),
                    payload: Bytes::from_static(b"bye"),
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    properties: Properties(vec![Property::WillDelayInterval(10)]),
                }),
                username: Some("u".into()),
                password: Some(Bytes::from_static(b"p")),
                properties: Properties(vec![
                    Property::SessionExpiryInterval(3600),
                    Property::ReceiveMaximum(20),
                    Property::UserProperty("k".into(), "v".into()),
                ]),
            }),
            V5,
        );
    }

    #[test]
    fn roundtrip_connack_v5_with_properties() {
        roundtrip_version(
            &Packet::ConnAck(ConnAck {
                session_present: false,
                code: 0,
                properties: Properties(vec![
                    Property::AssignedClientIdentifier("auto-7".into()),
                    Property::ServerKeepAlive(45),
                ]),
            }),
            V5,
        );
    }

    /// A v4 CONNECT/CONNACK encodes byte-identically with or without the (empty) v5
    /// properties present — the new fields never leak into the v3.1.1 wire.
    #[test]
    fn v4_wire_is_unchanged_by_empty_properties() {
        let connect = Packet::Connect(Connect {
            protocol: V4,
            clean_session: true,
            keep_alive: 15,
            client_id: "c".into(),
            last_will: None,
            username: None,
            password: None,
            properties: Properties::new(),
        });
        let mut out = Vec::new();
        connect.encode(&mut out, V4).unwrap();
        // The v4 CONNECT variable header ends at keep-alive; no property-length byte.
        // 10-byte var header (proto name 6 + level 1 + flags 1 + keepalive 2) + 2-byte
        // client-id length + "c".
        assert_eq!(out.len(), 2 + 10 + 2 + 1);
    }

    #[test]
    fn roundtrip_publish_qos0() {
        roundtrip(&Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "a/b/c".into(),
            pkid: None,
            properties: Properties::new(),
            payload: Bytes::from_static(b"hello"),
        }));
    }

    #[test]
    fn roundtrip_publish_qos2() {
        roundtrip(&Packet::Publish(Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: "sensors/temp".into(),
            pkid: Some(42),
            properties: Properties::new(),
            payload: Bytes::from_static(b"21.5C"),
        }));
    }

    #[test]
    fn roundtrip_publish_v5_with_properties() {
        roundtrip_version(
            &Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: String::new(), // empty topic is valid when a topic alias is set
                pkid: Some(9),
                properties: Properties(vec![
                    Property::TopicAlias(3),
                    Property::PayloadFormatIndicator(1),
                    Property::ContentType("text/plain".into()),
                    Property::UserProperty("k".into(), "v".into()),
                ]),
                payload: Bytes::from_static(b"hi"),
            }),
            V5,
        );
    }

    #[test]
    fn roundtrip_ack_family() {
        roundtrip(&Packet::PubAck(7.into()));
        roundtrip(&Packet::PubRec(8.into()));
        roundtrip(&Packet::PubRel(9.into()));
        roundtrip(&Packet::PubComp(10.into()));
        roundtrip(&Packet::UnsubAck(11.into()));
    }

    #[test]
    fn roundtrip_ack_family_v5() {
        // v5 short form (reason 0, no properties) is byte-identical to v3.1.1.
        roundtrip_version(&Packet::PubAck(7.into()), V5);
        // A non-success reason with properties uses the long form.
        roundtrip_version(
            &Packet::PubRec(Ack {
                pkid: 8,
                reason: 0x87, // Not authorized
                properties: Properties(vec![Property::ReasonString("nope".into())]),
            }),
            V5,
        );
        // A non-success reason with no properties (the length-3 short form on decode).
        roundtrip_version(
            &Packet::PubComp(Ack {
                pkid: 9,
                reason: 0x92, // Packet identifier not found
                properties: Properties::new(),
            }),
            V5,
        );
    }

    /// A v5 PUBACK whose reason is non-zero but carries no properties decodes the same
    /// whether the sender wrote the 3-byte form (no property length) or the 4-byte
    /// form (explicit 0 property length).
    #[test]
    fn ack_v5_short_and_long_no_property_forms_agree() {
        // pkid 5, reason 0x10 (No matching subscribers): 3-byte body vs 4-byte body.
        let three = BytesMut::from(&[0x40u8, 0x03, 0x00, 0x05, 0x10][..]);
        let four = BytesMut::from(&[0x40u8, 0x04, 0x00, 0x05, 0x10, 0x00][..]);
        let want = Packet::PubAck(Ack {
            pkid: 5,
            reason: 0x10,
            properties: Properties::new(),
        });
        for mut buf in [three, four] {
            assert_eq!(Packet::decode(&mut buf, V5).unwrap(), Some(want.clone()));
        }
    }

    #[test]
    fn roundtrip_subscribe_suback() {
        roundtrip(&Packet::Subscribe(Subscribe {
            pkid: 1,
            filters: vec![
                SubscribeFilter {
                    path: "a/#".into(),
                    qos: QoS::AtLeastOnce,
                    options: SubscriptionOptions::default(),
                },
                SubscribeFilter {
                    path: "b/+/c".into(),
                    qos: QoS::ExactlyOnce,
                    options: SubscriptionOptions::default(),
                },
            ],
            properties: Properties::new(),
        }));
        roundtrip(&Packet::SubAck(SubAck {
            pkid: 1,
            return_codes: vec![0x00, 0x01, 0x80],
            properties: Properties::new(),
        }));
    }

    #[test]
    fn roundtrip_subscribe_v5_with_options_and_properties() {
        roundtrip_version(
            &Packet::Subscribe(Subscribe {
                pkid: 7,
                filters: vec![
                    SubscribeFilter {
                        path: "a/#".into(),
                        qos: QoS::ExactlyOnce,
                        options: SubscriptionOptions {
                            no_local: true,
                            retain_as_published: true,
                            retain_handling: 2,
                        },
                    },
                    SubscribeFilter {
                        path: "b/+".into(),
                        qos: QoS::AtMostOnce,
                        options: SubscriptionOptions::default(),
                    },
                ],
                properties: Properties(vec![Property::SubscriptionIdentifier(42)]),
            }),
            V5,
        );
    }

    #[test]
    fn subscribe_v5_reserved_and_bad_retain_handling_are_rejected() {
        // SUBSCRIBE (8<<4 | 0x02), pkid 1, empty props, filter "a" with an option
        // byte whose bit 7 (reserved) is set → malformed.
        let mut reserved =
            BytesMut::from(&[0x82u8, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, b'a', 0x80][..]);
        assert!(matches!(
            Packet::decode(&mut reserved, V5),
            Err(CodecError::MalformedPacket(_))
        ));
        // Retain handling bits = 3 (0x30) → protocol error.
        let mut bad_rh =
            BytesMut::from(&[0x82u8, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, b'a', 0x30][..]);
        assert!(matches!(
            Packet::decode(&mut bad_rh, V5),
            Err(CodecError::ProtocolViolation(_))
        ));
    }

    #[test]
    fn roundtrip_unsubscribe() {
        roundtrip(&Packet::Unsubscribe(Unsubscribe {
            pkid: 2,
            filters: vec!["a/#".into(), "b/c".into()],
            properties: Properties::new(),
        }));
    }

    #[test]
    fn roundtrip_suback_unsubscribe_unsuback_v5() {
        roundtrip_version(
            &Packet::SubAck(SubAck {
                pkid: 1,
                return_codes: vec![0x00, 0x02, 0x87], // granted QoS / granted / not authorized
                properties: Properties(vec![Property::ReasonString("ok".into())]),
            }),
            V5,
        );
        roundtrip_version(
            &Packet::Unsubscribe(Unsubscribe {
                pkid: 2,
                filters: vec!["a/#".into()],
                properties: Properties(vec![Property::UserProperty("k".into(), "v".into())]),
            }),
            V5,
        );
        roundtrip_version(
            &Packet::UnsubAck(UnsubAck {
                pkid: 3,
                reason_codes: vec![0x00, 0x11], // success / no subscription existed
                properties: Properties::new(),
            }),
            V5,
        );
    }

    /// The v3.1.1 UNSUBACK short form (just the packet id) round-trips, and a v5
    /// UNSUBACK with no reason codes is a protocol error.
    #[test]
    fn unsuback_v4_is_pkid_only_and_v5_requires_reason_codes() {
        roundtrip(&Packet::UnsubAck(7.into()));
        // UNSUBACK (11<<4), remaining 3: pkid 0x0007, property length 0, no codes.
        let mut empty = BytesMut::from(&[0xB0u8, 0x03, 0x00, 0x07, 0x00][..]);
        assert!(matches!(
            Packet::decode(&mut empty, V5),
            Err(CodecError::ProtocolViolation(_))
        ));
    }

    #[test]
    fn roundtrip_pings_and_disconnect() {
        roundtrip(&Packet::PingReq);
        roundtrip(&Packet::PingResp);
        roundtrip(&Packet::Disconnect(Disconnect::default()));
    }

    #[test]
    fn roundtrip_disconnect_and_auth_v5() {
        // PINGREQ/PINGRESP and the v4-style (empty) DISCONNECT also work over v5.
        roundtrip_version(&Packet::PingReq, V5);
        roundtrip_version(&Packet::Disconnect(Disconnect::default()), V5);
        // DISCONNECT with a reason + properties (the long form).
        roundtrip_version(
            &Packet::Disconnect(Disconnect {
                reason: 0x8D, // Keep Alive timeout
                properties: Properties(vec![Property::ReasonString("too slow".into())]),
            }),
            V5,
        );
        // AUTH short form (Success, no properties) and long form.
        roundtrip_version(&Packet::Auth(Auth::default()), V5);
        roundtrip_version(
            &Packet::Auth(Auth {
                reason: 0x18, // Continue authentication
                properties: Properties(vec![
                    Property::AuthenticationMethod("SCRAM-SHA-256".into()),
                    Property::AuthenticationData(Bytes::from_static(&[1, 2, 3])),
                ]),
            }),
            V5,
        );
    }

    #[test]
    fn auth_is_rejected_on_v3_1_1() {
        // Decoding an AUTH (15<<4) on a v4 connection is a protocol error.
        let mut buf = BytesMut::from(&[0xF0u8, 0x00][..]);
        assert!(matches!(
            Packet::decode(&mut buf, V4),
            Err(CodecError::ProtocolViolation(_))
        ));
        // Encoding one for v4 is likewise refused.
        let mut out = Vec::new();
        assert!(matches!(
            Packet::Auth(Auth::default()).encode(&mut out, V4),
            Err(CodecError::ProtocolViolation(_))
        ));
    }

    #[test]
    fn partial_buffer_returns_none() {
        // Encode a CONNECT, then feed it one byte at a time.
        let mut out = Vec::new();
        Packet::Connect(Connect {
            protocol: V4,
            clean_session: true,
            keep_alive: 10,
            client_id: "abc".into(),
            last_will: None,
            username: None,
            password: None,
            properties: Properties::new(),
        })
        .encode(&mut out, V4)
        .unwrap();

        let mut buf = BytesMut::new();
        for &byte in &out[..out.len() - 1] {
            buf.extend_from_slice(&[byte]);
            assert_eq!(Packet::decode(&mut buf, V4).unwrap(), None);
        }
        buf.extend_from_slice(&[out[out.len() - 1]]);
        assert!(Packet::decode(&mut buf, V4).unwrap().is_some());
    }

    #[test]
    fn two_packets_in_one_buffer() {
        let mut out = Vec::new();
        Packet::PingReq.encode(&mut out, V4).unwrap();
        Packet::Disconnect(Disconnect::default())
            .encode(&mut out, V4)
            .unwrap();
        let mut buf = BytesMut::from(&out[..]);
        assert_eq!(Packet::decode(&mut buf, V4).unwrap(), Some(Packet::PingReq));
        assert_eq!(
            Packet::decode(&mut buf, V4).unwrap(),
            Some(Packet::Disconnect(Disconnect::default()))
        );
        assert_eq!(Packet::decode(&mut buf, V4).unwrap(), None);
    }

    #[test]
    fn publish_qos3_is_malformed() {
        // First byte: PUBLISH (3<<4) with qos bits = 11 (0x06).
        let mut buf = BytesMut::from(&[0x36u8, 0x03, 0x00, 0x01, b'a'][..]);
        assert!(matches!(
            Packet::decode(&mut buf, V4),
            Err(CodecError::MalformedPacket(_))
        ));
    }

    #[test]
    fn subscribe_wrong_flags_rejected() {
        // SUBSCRIBE (8<<4) with flags 0 instead of required 0x02.
        let mut buf = BytesMut::from(&[0x80u8, 0x00][..]);
        assert!(matches!(
            Packet::decode(&mut buf, V4),
            Err(CodecError::MalformedPacket(_))
        ));
    }

    /// Every control packet now has a v5 form; a PINGREQ (unchanged on the wire)
    /// decodes over a v5 connection rather than being refused.
    #[test]
    fn ping_decodes_over_v5() {
        let mut buf = BytesMut::from(&[0xC0u8, 0x00][..]); // PINGREQ
        assert_eq!(Packet::decode(&mut buf, V5).unwrap(), Some(Packet::PingReq));
    }
}
