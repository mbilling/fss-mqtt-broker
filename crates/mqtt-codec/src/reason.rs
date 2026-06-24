//! MQTT 5.0 reason codes ([§2.4]) as named `u8` constants.
//!
//! Reason codes are a single byte shared across CONNACK, PUBACK/PUBREC/PUBREL/PUBCOMP,
//! SUBACK, UNSUBACK, DISCONNECT and AUTH — the same value (e.g. `0x87` Not Authorized)
//! recurs on many packets. The codec carries them as a bare `u8` (their wire form); this
//! module gives every value the spec's name so call sites read as intent, not magic
//! numbers, and the set lives in one audited place ([ADR 0008](../../../docs/adr/0008-mqtt-5-codec.md) T8).
//!
//! Which codes are legal on which packet is the caller's concern — these are just the
//! catalogue. A reason code `< 0x80` is success/normal; `>= 0x80` is a failure (the
//! sender of a failing ack/disconnect closes or refuses).

/// `0x00` — Success / Normal disconnection / Granted `QoS` 0 (the meaning is by packet).
pub const SUCCESS: u8 = 0x00;
/// `0x01` — Granted `QoS` 1 (SUBACK).
pub const GRANTED_QOS_1: u8 = 0x01;
/// `0x02` — Granted `QoS` 2 (SUBACK).
pub const GRANTED_QOS_2: u8 = 0x02;
/// `0x04` — Disconnect with Will Message (client DISCONNECT).
pub const DISCONNECT_WITH_WILL: u8 = 0x04;
/// `0x10` — No matching subscribers (PUBACK/PUBREC).
pub const NO_MATCHING_SUBSCRIBERS: u8 = 0x10;
/// `0x11` — No subscription existed (UNSUBACK).
pub const NO_SUBSCRIPTION_EXISTED: u8 = 0x11;
/// `0x18` — Continue authentication (AUTH).
pub const CONTINUE_AUTHENTICATION: u8 = 0x18;
/// `0x19` — Re-authenticate (AUTH).
pub const REAUTHENTICATE: u8 = 0x19;

/// `0x80` — Unspecified error.
pub const UNSPECIFIED_ERROR: u8 = 0x80;
/// `0x81` — Malformed Packet.
pub const MALFORMED_PACKET: u8 = 0x81;
/// `0x82` — Protocol Error.
pub const PROTOCOL_ERROR: u8 = 0x82;
/// `0x83` — Implementation specific error.
pub const IMPLEMENTATION_SPECIFIC_ERROR: u8 = 0x83;
/// `0x84` — Unsupported Protocol Version (CONNACK).
pub const UNSUPPORTED_PROTOCOL_VERSION: u8 = 0x84;
/// `0x85` — Client Identifier not valid (CONNACK).
pub const CLIENT_IDENTIFIER_NOT_VALID: u8 = 0x85;
/// `0x86` — Bad User Name or Password (CONNACK).
pub const BAD_USER_NAME_OR_PASSWORD: u8 = 0x86;
/// `0x87` — Not authorized.
pub const NOT_AUTHORIZED: u8 = 0x87;
/// `0x88` — Server unavailable (CONNACK).
pub const SERVER_UNAVAILABLE: u8 = 0x88;
/// `0x89` — Server busy.
pub const SERVER_BUSY: u8 = 0x89;
/// `0x8A` — Banned (CONNACK).
pub const BANNED: u8 = 0x8A;
/// `0x8B` — Server shutting down (DISCONNECT).
pub const SERVER_SHUTTING_DOWN: u8 = 0x8B;
/// `0x8C` — Bad authentication method.
pub const BAD_AUTHENTICATION_METHOD: u8 = 0x8C;
/// `0x8D` — Keep Alive timeout (DISCONNECT).
pub const KEEP_ALIVE_TIMEOUT: u8 = 0x8D;
/// `0x8E` — Session taken over (DISCONNECT).
pub const SESSION_TAKEN_OVER: u8 = 0x8E;
/// `0x8F` — Topic Filter invalid (SUBACK/UNSUBACK/DISCONNECT).
pub const TOPIC_FILTER_INVALID: u8 = 0x8F;
/// `0x90` — Topic Name invalid (PUBACK/PUBREC/DISCONNECT).
pub const TOPIC_NAME_INVALID: u8 = 0x90;
/// `0x91` — Packet Identifier in use (PUBACK/PUBREC/SUBACK/UNSUBACK).
pub const PACKET_IDENTIFIER_IN_USE: u8 = 0x91;
/// `0x92` — Packet Identifier not found (PUBREL/PUBCOMP).
pub const PACKET_IDENTIFIER_NOT_FOUND: u8 = 0x92;
/// `0x93` — Receive Maximum exceeded (DISCONNECT).
pub const RECEIVE_MAXIMUM_EXCEEDED: u8 = 0x93;
/// `0x94` — Topic Alias invalid (DISCONNECT).
pub const TOPIC_ALIAS_INVALID: u8 = 0x94;
/// `0x95` — Packet too large (CONNACK/DISCONNECT).
pub const PACKET_TOO_LARGE: u8 = 0x95;
/// `0x96` — Message rate too high (DISCONNECT).
pub const MESSAGE_RATE_TOO_HIGH: u8 = 0x96;
/// `0x97` — Quota exceeded.
pub const QUOTA_EXCEEDED: u8 = 0x97;
/// `0x98` — Administrative action (DISCONNECT).
pub const ADMINISTRATIVE_ACTION: u8 = 0x98;
/// `0x99` — Payload format invalid (PUBACK/PUBREC/DISCONNECT).
pub const PAYLOAD_FORMAT_INVALID: u8 = 0x99;
/// `0x9A` — Retain not supported (CONNACK/DISCONNECT).
pub const RETAIN_NOT_SUPPORTED: u8 = 0x9A;
/// `0x9B` — `QoS` not supported (CONNACK/DISCONNECT).
pub const QOS_NOT_SUPPORTED: u8 = 0x9B;
/// `0x9C` — Use another server (CONNACK/DISCONNECT).
pub const USE_ANOTHER_SERVER: u8 = 0x9C;
/// `0x9D` — Server moved (CONNACK/DISCONNECT).
pub const SERVER_MOVED: u8 = 0x9D;
/// `0x9E` — Shared Subscriptions not supported (SUBACK/DISCONNECT).
pub const SHARED_SUBSCRIPTIONS_NOT_SUPPORTED: u8 = 0x9E;
/// `0x9F` — Connection rate exceeded (CONNACK/DISCONNECT).
pub const CONNECTION_RATE_EXCEEDED: u8 = 0x9F;
/// `0xA0` — Maximum connect time (DISCONNECT).
pub const MAXIMUM_CONNECT_TIME: u8 = 0xA0;
/// `0xA1` — Subscription Identifiers not supported (SUBACK/DISCONNECT).
pub const SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED: u8 = 0xA1;
/// `0xA2` — Wildcard Subscriptions not supported (SUBACK/DISCONNECT).
pub const WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED: u8 = 0xA2;

/// Whether a reason code denotes an error (`>= 0x80`), as opposed to success/normal.
#[must_use]
pub const fn is_error(code: u8) -> bool {
    code >= 0x80
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_wire_values_and_error_classification() {
        // A few canonical spec values, pinned to their wire byte.
        assert_eq!(SUCCESS, 0x00);
        assert_eq!(PROTOCOL_ERROR, 0x82);
        assert_eq!(NOT_AUTHORIZED, 0x87);
        assert_eq!(RECEIVE_MAXIMUM_EXCEEDED, 0x93);
        assert_eq!(TOPIC_ALIAS_INVALID, 0x94);
        // Success/normal codes are < 0x80; failures are >= 0x80.
        assert!(!is_error(SUCCESS));
        assert!(!is_error(CONTINUE_AUTHENTICATION));
        assert!(is_error(PROTOCOL_ERROR));
        assert!(is_error(TOPIC_ALIAS_INVALID));
    }
}
