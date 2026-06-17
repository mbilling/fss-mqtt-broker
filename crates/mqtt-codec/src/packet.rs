//! Fixed header, the [`Packet`] enum, and per-packet encode/decode.
//!
//! This module implements the complete **MQTT 3.1.1** control packet set and the
//! **MQTT 5.0** framing for CONNECT/CONNACK (properties blocks, ADR 0008 phase 2).
//! Packets whose v5 form is not yet implemented are guarded with an explicit error
//! so a v5 packet is never silently mis-parsed as v4; the remaining v5 paths land in
//! later phases.

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

/// A single entry in a SUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeFilter {
    /// Topic filter (may contain `+`/`#` wildcards).
    pub path: String,
    /// Maximum `QoS` requested for this subscription.
    pub qos: QoS,
}

/// A SUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// Packet identifier.
    pub pkid: u16,
    /// One or more topic filters (a SUBSCRIBE with zero filters is malformed).
    pub filters: Vec<SubscribeFilter>,
}

/// A SUBACK packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    /// Packet identifier matching the SUBSCRIBE.
    pub pkid: u16,
    /// Per-filter return codes (0/1/2 = granted `QoS`, 0x80 = failure).
    pub return_codes: Vec<u8>,
}

/// An UNSUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    /// Packet identifier.
    pub pkid: u16,
    /// Topic filters to remove (a packet with zero filters is malformed).
    pub filters: Vec<String>,
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
    /// PUBACK (carries the packet identifier).
    PubAck(u16),
    /// PUBREC (carries the packet identifier).
    PubRec(u16),
    /// PUBREL (carries the packet identifier).
    PubRel(u16),
    /// PUBCOMP (carries the packet identifier).
    PubComp(u16),
    /// SUBSCRIBE.
    Subscribe(Subscribe),
    /// SUBACK.
    SubAck(SubAck),
    /// UNSUBSCRIBE.
    Unsubscribe(Unsubscribe),
    /// UNSUBACK (carries the packet identifier).
    UnsubAck(u16),
    /// PINGREQ.
    PingReq,
    /// PINGRESP.
    PingResp,
    /// DISCONNECT.
    Disconnect,
}

const V5_UNSUPPORTED: CodecError =
    CodecError::ProtocolViolation("MQTT 5.0 codec not yet implemented");

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
            Packet::Disconnect => PacketType::Disconnect,
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

        // CONNECT/CONNACK have v5 forms (ADR 0008 phase 2); other packets' v5 forms
        // are not implemented yet — refuse rather than mis-parse as v4.
        if version == ProtocolVersion::V5
            && !matches!(
                header.packet_type,
                PacketType::Connect | PacketType::ConnAck | PacketType::Publish
            )
        {
            return Err(V5_UNSUPPORTED);
        }

        let packet = match header.packet_type {
            PacketType::Connect => Packet::Connect(decode_connect(&mut r)?),
            PacketType::ConnAck => {
                expect_flags(header.flags, 0)?;
                Packet::ConnAck(decode_connack(&mut r, version)?)
            }
            PacketType::Publish => Packet::Publish(decode_publish(version, header.flags, &mut r)?),
            PacketType::PubAck => {
                expect_flags(header.flags, 0)?;
                Packet::PubAck(decode_pkid_only(&mut r)?)
            }
            PacketType::PubRec => {
                expect_flags(header.flags, 0)?;
                Packet::PubRec(decode_pkid_only(&mut r)?)
            }
            PacketType::PubRel => {
                expect_flags(header.flags, 0x02)?;
                Packet::PubRel(decode_pkid_only(&mut r)?)
            }
            PacketType::PubComp => {
                expect_flags(header.flags, 0)?;
                Packet::PubComp(decode_pkid_only(&mut r)?)
            }
            PacketType::Subscribe => {
                expect_flags(header.flags, 0x02)?;
                Packet::Subscribe(decode_subscribe(&mut r)?)
            }
            PacketType::SubAck => {
                expect_flags(header.flags, 0)?;
                Packet::SubAck(decode_suback(&mut r)?)
            }
            PacketType::Unsubscribe => {
                expect_flags(header.flags, 0x02)?;
                Packet::Unsubscribe(decode_unsubscribe(&mut r)?)
            }
            PacketType::UnsubAck => {
                expect_flags(header.flags, 0)?;
                Packet::UnsubAck(decode_pkid_only(&mut r)?)
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
                expect_empty(&r)?;
                Packet::Disconnect
            }
            PacketType::Auth => return Err(V5_UNSUPPORTED),
        };
        Ok(Some(packet))
    }

    /// Encode this packet into `out` for the given protocol `version`.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a field is out of range or the version is not
    /// yet supported.
    pub fn encode(&self, out: &mut Vec<u8>, version: ProtocolVersion) -> Result<(), CodecError> {
        if version == ProtocolVersion::V5
            && !matches!(
                self,
                Packet::Connect(_) | Packet::ConnAck(_) | Packet::Publish(_)
            )
        {
            return Err(V5_UNSUPPORTED);
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
            // PUBACK, PUBREC, PUBCOMP and UNSUBACK share an identical body and zero flags.
            Packet::PubAck(id)
            | Packet::PubRec(id)
            | Packet::PubComp(id)
            | Packet::UnsubAck(id) => {
                io::put_u16(&mut body, *id);
                0
            }
            // PUBREL is the same body but, like SUBSCRIBE/UNSUBSCRIBE, requires flags 0x02.
            Packet::PubRel(id) => {
                io::put_u16(&mut body, *id);
                0x02
            }
            Packet::Subscribe(s) => {
                encode_subscribe(s, &mut body)?;
                0x02
            }
            Packet::SubAck(a) => {
                encode_suback(a, &mut body);
                0
            }
            Packet::Unsubscribe(u) => {
                encode_unsubscribe(u, &mut body)?;
                0x02
            }
            Packet::PingReq | Packet::PingResp | Packet::Disconnect => 0,
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

fn decode_pkid_only(r: &mut Reader) -> Result<u16, CodecError> {
    let pkid = r.read_u16()?;
    expect_empty(r)?;
    Ok(pkid)
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

fn decode_subscribe(r: &mut Reader) -> Result<Subscribe, CodecError> {
    let pkid = r.read_u16()?;
    let mut filters = Vec::new();
    while !r.is_empty() {
        let path = r.read_string()?;
        let options = r.read_u8()?;
        // MQTT 3.1.1: only the low two QoS bits are defined; the rest are reserved.
        if options & 0xFC != 0 {
            return Err(CodecError::MalformedPacket(
                "reserved SUBSCRIBE option bits",
            ));
        }
        let qos = QoS::from_u8(options & 0x03)
            .ok_or(CodecError::MalformedPacket("invalid requested QoS"))?;
        filters.push(SubscribeFilter { path, qos });
    }
    if filters.is_empty() {
        return Err(CodecError::ProtocolViolation("SUBSCRIBE with no filters"));
    }
    Ok(Subscribe { pkid, filters })
}

fn encode_subscribe(s: &Subscribe, out: &mut Vec<u8>) -> Result<(), CodecError> {
    if s.filters.is_empty() {
        return Err(CodecError::ProtocolViolation("SUBSCRIBE with no filters"));
    }
    io::put_u16(out, s.pkid);
    for f in &s.filters {
        io::put_string(out, &f.path)?;
        io::put_u8(out, f.qos as u8);
    }
    Ok(())
}

fn decode_suback(r: &mut Reader) -> Result<SubAck, CodecError> {
    let pkid = r.read_u16()?;
    let mut return_codes = Vec::new();
    while !r.is_empty() {
        return_codes.push(r.read_u8()?);
    }
    if return_codes.is_empty() {
        return Err(CodecError::ProtocolViolation("SUBACK with no return codes"));
    }
    Ok(SubAck { pkid, return_codes })
}

fn encode_suback(a: &SubAck, out: &mut Vec<u8>) {
    io::put_u16(out, a.pkid);
    out.extend_from_slice(&a.return_codes);
}

fn decode_unsubscribe(r: &mut Reader) -> Result<Unsubscribe, CodecError> {
    let pkid = r.read_u16()?;
    let mut filters = Vec::new();
    while !r.is_empty() {
        filters.push(r.read_string()?);
    }
    if filters.is_empty() {
        return Err(CodecError::ProtocolViolation("UNSUBSCRIBE with no filters"));
    }
    Ok(Unsubscribe { pkid, filters })
}

fn encode_unsubscribe(u: &Unsubscribe, out: &mut Vec<u8>) -> Result<(), CodecError> {
    if u.filters.is_empty() {
        return Err(CodecError::ProtocolViolation("UNSUBSCRIBE with no filters"));
    }
    io::put_u16(out, u.pkid);
    for f in &u.filters {
        io::put_string(out, f)?;
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
        roundtrip(&Packet::PubAck(7));
        roundtrip(&Packet::PubRec(8));
        roundtrip(&Packet::PubRel(9));
        roundtrip(&Packet::PubComp(10));
        roundtrip(&Packet::UnsubAck(11));
    }

    #[test]
    fn roundtrip_subscribe_suback() {
        roundtrip(&Packet::Subscribe(Subscribe {
            pkid: 1,
            filters: vec![
                SubscribeFilter {
                    path: "a/#".into(),
                    qos: QoS::AtLeastOnce,
                },
                SubscribeFilter {
                    path: "b/+/c".into(),
                    qos: QoS::ExactlyOnce,
                },
            ],
        }));
        roundtrip(&Packet::SubAck(SubAck {
            pkid: 1,
            return_codes: vec![0x00, 0x01, 0x80],
        }));
    }

    #[test]
    fn roundtrip_unsubscribe() {
        roundtrip(&Packet::Unsubscribe(Unsubscribe {
            pkid: 2,
            filters: vec!["a/#".into(), "b/c".into()],
        }));
    }

    #[test]
    fn roundtrip_pings_and_disconnect() {
        roundtrip(&Packet::PingReq);
        roundtrip(&Packet::PingResp);
        roundtrip(&Packet::Disconnect);
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
        Packet::Disconnect.encode(&mut out, V4).unwrap();
        let mut buf = BytesMut::from(&out[..]);
        assert_eq!(Packet::decode(&mut buf, V4).unwrap(), Some(Packet::PingReq));
        assert_eq!(
            Packet::decode(&mut buf, V4).unwrap(),
            Some(Packet::Disconnect)
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

    #[test]
    fn v5_non_connect_is_rejected_for_now() {
        let mut buf = BytesMut::from(&[0xC0u8, 0x00][..]); // PINGREQ
        assert!(matches!(
            Packet::decode(&mut buf, ProtocolVersion::V5),
            Err(CodecError::ProtocolViolation(_))
        ));
    }
}
