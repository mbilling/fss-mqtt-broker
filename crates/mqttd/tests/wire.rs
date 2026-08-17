//! Suite WIRE — packet framing and codec conformance at the byte level.
//!
//! Every test here writes **hand-assembled bytes** to a real socket and asserts the
//! server's exact answer. That is the point: these frames cannot be produced by an
//! encoder, so no test that goes through the project's own codec can reach them. A
//! conformant client library will never send a 5-byte remaining length, an overlong
//! UTF-8 sequence, or a reserved packet type — which is precisely why an
//! implementation can be wrong about all three and stay green forever.
//!
//! Three outcomes are distinguished throughout, and they are NOT interchangeable:
//!
//! - **silent close** — the only correct answer to garbage arriving *before* a
//!   success CONNACK. The server may not send a DISCONNECT there
//!   [MQTT-3.14.0-1], and it cannot answer with a CONNACK a packet it could not
//!   parse, so silence is all that is left.
//! - **refused** — rejected *after* CONNACK, where the server both closes (the MUST)
//!   and says why (the SHOULD) [MQTT-4.13.2]. `expect_refused` asserts the reason
//!   code; `expect_disconnect_bytes` additionally requires the packet.
//! - **accepted** — the connection stays open and usable.
//!
//! Asserting merely that "an error happened" would pass against a server that hung
//! up for an unrelated reason, so each test names which of the three it requires.
//!
//! This suite found three real conformance defects on its first run, all fixed in
//! the same change that introduced it: an interior `U+0000` accepted in topic names,
//! a Payload Format Indicator of `2` accepted, and a zero-length topic filter
//! GRANTED (it then matched nothing, so the subscription was silently inert).

mod common;

use common::{
    connect_v5_bytes, frame, mqtt_bytes, mqtt_str, publish_v5_bytes, start_broker,
    subscribe_v5_bytes, vbi, RawClient, RawOutcome,
};
use std::time::Duration;

use mqtt_codec::reason::{
    CLIENT_IDENTIFIER_NOT_VALID, MALFORMED_PACKET, PACKET_TOO_LARGE, PROTOCOL_ERROR,
    TOPIC_FILTER_INVALID,
};

/// Assert the answer is a SUBACK whose (single) per-filter reason code is `reason`.
/// A per-filter refusal is deliberately NOT a connection close: the other filters in
/// the same SUBSCRIBE may be perfectly valid and are granted independently.
async fn expect_suback_reason(c: &mut RawClient, reason: u8) {
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => {
            assert_eq!(b[0], 0x90, "expected SUBACK, got {b:02x?}");
            assert_eq!(
                *b.last().unwrap(),
                reason,
                "expected per-filter reason {reason:#04x}, got {b:02x?}"
            );
        }
        other => panic!("expected SUBACK({reason:#04x}), got {other:?}"),
    }
}

/// Open a connection and complete a v5 CONNECT, so subsequent malformed input is
/// judged under "a session exists" rules (DISCONNECT, not silent close).
async fn connected(addr: std::net::SocketAddr, client_id: &str) -> RawClient {
    let mut c = RawClient::open(addr).await;
    c.send_bytes(&connect_v5_bytes(client_id)).await;
    c.expect_connack_bytes(0x00).await;
    c
}

// ---------------------------------------------------------------------------
// Harness sanity — if these fail, every other test in the file is meaningless.
// ---------------------------------------------------------------------------

/// WIRE-000: the hand-built CONNECT is genuinely valid, so a later test that
/// corrupts one byte of it is isolating that byte and not merely rediscovering a
/// broken builder. Without this, a builder bug reads as universal conformance.
#[tokio::test]
async fn the_hand_built_connect_is_accepted() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;
    c.send_bytes(&connect_v5_bytes("wire-sanity")).await;
    c.expect_connack_bytes(0x00).await;
}

/// The other two builders are valid too: a SUBSCRIBE is granted and a PUBLISH on
/// the subscribed topic comes back.
#[tokio::test]
async fn the_hand_built_subscribe_and_publish_round_trip() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-sanity-2").await;

    c.send_bytes(&subscribe_v5_bytes("wire/t")).await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => assert_eq!(b[0], 0x90, "expected SUBACK, got {b:02x?}"),
        other => panic!("expected SUBACK, got {other:?}"),
    }

    c.send_bytes(&publish_v5_bytes("wire/t", b"hi")).await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => assert_eq!(b[0], 0x30, "expected PUBLISH, got {b:02x?}"),
        other => panic!("expected the published message back, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 3.1 Fixed header
// ---------------------------------------------------------------------------

/// WIRE-001: packet type 0 is reserved and never legal.
#[tokio::test]
async fn reserved_packet_type_zero_is_malformed() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;
    c.send_bytes(&[0x00, 0x00]).await;
    c.expect_closed_silently().await;
}

/// WIRE-002: packet type 15 is reserved in MQTT 5.0 (it was AUTH's slot in no
/// version; 15 is explicitly reserved).
#[tokio::test]
async fn reserved_packet_type_fifteen_is_malformed() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;
    c.send_bytes(&[0xF0, 0x00]).await;
    c.expect_closed_silently().await;
}

/// WIRE-004: SUBSCRIBE's fixed-header flags are reserved and MUST be `0b0010`
/// [MQTT-3.8.1-1]. A server that ignores the flag bits accepts a packet the spec
/// requires it to reject.
#[tokio::test]
async fn subscribe_with_wrong_reserved_flags_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-sub-flags").await;

    // Correct SUBSCRIBE, but first byte 0x80 instead of 0x82.
    let mut bytes = subscribe_v5_bytes("wire/t");
    bytes[0] = 0x80;
    c.send_bytes(&bytes).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// WIRE-004 (PUBREL): same rule, different packet — PUBREL is `0b0010` too
/// [MQTT-3.6.1-1].
#[tokio::test]
async fn pubrel_with_wrong_reserved_flags_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-pubrel-flags").await;

    // PUBREL for packet id 1 with flags 0b0000 instead of 0b0010.
    c.send_bytes(&frame(0x60, &1u16.to_be_bytes())).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// WIRE-005: `QoS` bits `3` in a PUBLISH is not a `QoS` level [MQTT-3.3.1-4].
#[tokio::test]
async fn publish_with_qos_three_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-qos3").await;

    // 0x30 | (3 << 1) = 0x36.
    let mut body = mqtt_str("wire/t");
    body.extend_from_slice(&1u16.to_be_bytes()); // packet id (QoS > 0 carries one)
    body.push(0x00); // properties
    c.send_bytes(&frame(0x36, &body)).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// WIRE-006: DUP must be 0 on a `QoS` 0 PUBLISH [MQTT-3.3.1-2]. There is no
/// redelivery of a `QoS` 0 message, so a set DUP bit is meaningless by construction.
#[tokio::test]
async fn qos0_publish_with_dup_set_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-qos0-dup").await;

    // 0x30 | 0x08 (DUP) = 0x38, QoS bits still 0.
    let mut body = mqtt_str("wire/t");
    body.push(0x00); // properties
    body.extend_from_slice(b"x");
    c.send_bytes(&frame(0x38, &body)).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// CONN-077: PINGREQ carries no body; a non-zero remaining length is malformed.
#[tokio::test]
async fn pingreq_with_a_body_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-pingreq").await;

    c.send_bytes(&[0xC0, 0x01, 0xFF]).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

// ---------------------------------------------------------------------------
// 3.2 Remaining Length (variable byte integer)
// ---------------------------------------------------------------------------

/// WIRE-010: a Variable Byte Integer is at most 4 bytes [MQTT-1.5.5-1]. A 5-byte
/// encoding must be rejected rather than silently masked to 4 — the latter is how
/// a length-confusion bug starts.
#[tokio::test]
async fn five_byte_remaining_length_is_malformed() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;
    c.send_bytes(&[0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]).await;
    c.expect_closed_silently().await;
}

/// WIRE-011: the VBI encoding must be minimal — `0x80 0x00` also decodes to 0, and
/// accepting it means two byte sequences denote the same packet. That is a framing
/// ambiguity, which is a request-smuggling primitive in any protocol that has one.
#[tokio::test]
async fn non_minimal_remaining_length_is_malformed() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;
    // PINGREQ with remaining length 0 encoded non-minimally as 0x80 0x00.
    c.send_bytes(&[0xC0, 0x80, 0x00]).await;
    c.expect_closed_silently().await;
}

/// WIRE-012: a remaining length larger than the bytes actually sent must leave the
/// server waiting — not hang forever. The connect deadline is what bounds it; the
/// assertion is that the connection is eventually closed rather than pinned open
/// by an attacker who sends a header and stops.
#[tokio::test]
async fn a_remaining_length_larger_than_the_body_is_bounded_not_infinite() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;

    // Announce 200 bytes of CONNECT, send 10, then go quiet.
    let mut header = vec![0x10];
    header.extend_from_slice(&vbi(200));
    header.extend_from_slice(&[0x00; 10]);
    c.send_bytes(&header).await;

    // Still parsing: the server is legitimately waiting for the rest.
    c.expect_quiet(Duration::from_millis(300)).await;

    // But it must not wait forever. The permissive harness broker applies the
    // default connect deadline; assert the connection is closed within it rather
    // than asserting a specific duration, which would be a timing test.
    match c.read_outcome(Duration::from_secs(30)).await {
        RawOutcome::ClosedSilently | RawOutcome::Bytes(_) => {}
        RawOutcome::Quiet => {
            panic!("a half-sent packet held the connection open past the connect deadline")
        }
    }
}

// ---------------------------------------------------------------------------
// 3.3 Fragmentation and coalescing
// ---------------------------------------------------------------------------

/// WIRE-020: a CONNECT split into single bytes with gaps must still parse. MQTT is
/// a stream protocol; a decoder that assumes one read yields one packet works
/// perfectly on loopback and fails on a real network.
#[tokio::test]
async fn a_connect_split_into_single_bytes_is_parsed() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;
    c.send_fragmented(&connect_v5_bytes("wire-frag"), 1, Duration::from_millis(5))
        .await;
    c.expect_connack_bytes(0x00).await;
}

/// WIRE-023: the boundary landing exactly between the fixed header and the
/// remaining-length bytes — the specific split most likely to break a decoder that
/// peeks at a fixed offset.
#[tokio::test]
async fn a_split_between_the_type_byte_and_the_length_is_parsed() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;

    let bytes = connect_v5_bytes("wire-split");
    c.send_bytes(&bytes[..1]).await;
    // SETTLE(wire-split-read-events): the state being settled is "the broker has read byte 1
    // and is waiting for more", which is unobservable by construction — the decoder's internal
    // buffer is not on the wire and any packet the broker sent to report it would defeat the
    // test. The gap forces two distinct read events across the type-byte/length boundary, which
    // is the defect class this covers. The failure mode is strictly one-sided: a slower machine
    // makes the split MORE certain, never less, so this cannot become vacuous under load.
    tokio::time::sleep(Duration::from_millis(50)).await;
    c.send_bytes(&bytes[1..]).await;
    c.expect_connack_bytes(0x00).await;
}

/// WIRE-021: CONNECT + SUBSCRIBE + PUBLISH arriving in ONE segment must all be
/// processed, in order. A decoder that handles one packet per read event silently
/// drops the rest — and loopback tests that write each packet separately never see it.
#[tokio::test]
async fn three_packets_in_one_segment_are_all_processed_in_order() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;

    c.send_coalesced(&[
        &connect_v5_bytes("wire-coalesced"),
        &subscribe_v5_bytes("wire/c"),
        &publish_v5_bytes("wire/c", b"payload"),
    ])
    .await;

    // Read until all three answers have been seen; they may arrive coalesced too.
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && tokio::time::Instant::now() < deadline {
        match c.read_outcome(Duration::from_secs(2)).await {
            RawOutcome::Bytes(b) => {
                // Walk the buffer, recording each packet's type byte.
                let mut i = 0;
                while i < b.len() {
                    seen.push(b[i]);
                    // Decode this packet's remaining length to find the next one.
                    let mut mult = 1usize;
                    let mut len = 0usize;
                    let mut j = i + 1;
                    loop {
                        if j >= b.len() {
                            return assert_eq!(
                                seen,
                                vec![0x20, 0x90, 0x30],
                                "expected CONNACK, SUBACK, PUBLISH"
                            );
                        }
                        len += usize::from(b[j] & 0x7F) * mult;
                        mult *= 128;
                        let more = b[j] & 0x80 != 0;
                        j += 1;
                        if !more {
                            break;
                        }
                    }
                    i = j + len;
                }
            }
            other => panic!("expected responses to the coalesced batch, got {other:?}"),
        }
    }
    assert_eq!(
        seen,
        vec![0x20, 0x90, 0x30],
        "expected CONNACK, SUBACK, then the PUBLISH echoed back — in that order"
    );
}

// ---------------------------------------------------------------------------
// 3.4 UTF-8 encoded strings
// ---------------------------------------------------------------------------

/// WIRE-030: a string length prefix that runs past the packet is malformed.
#[tokio::test]
async fn a_string_length_beyond_the_packet_is_malformed() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;

    // CONNECT whose protocol-name length claims 40 bytes but supplies 4.
    let mut body = vec![0x00, 0x28];
    body.extend_from_slice(b"MQTT");
    body.push(5);
    body.push(0x02);
    body.extend_from_slice(&0u16.to_be_bytes());
    body.push(0x00);
    body.extend_from_slice(&mqtt_str("x"));
    c.send_bytes(&frame(0x10, &body)).await;
    c.expect_closed_silently().await;
}

/// WIRE-031: `U+0000` is forbidden in any MQTT UTF-8 string [MQTT-1.5.4-2].
#[tokio::test]
async fn an_embedded_null_in_a_topic_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-nul").await;

    let mut body = mqtt_bytes(b"wire/\x00/t");
    body.push(0x00);
    c.send_bytes(&frame(0x30, &body)).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// WIRE-032: an encoded UTF-16 surrogate (`ED A0 80` = `U+D800`) is not valid
/// UTF-8 [MQTT-1.5.4-1]. Rust's own `str` validation rejects these, but the check
/// must happen on the wire path — a decoder using `from_utf8_unchecked` for speed
/// would pass every other test in this file.
#[tokio::test]
async fn an_encoded_surrogate_in_a_topic_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-surrogate").await;

    let mut body = mqtt_bytes(b"wire/\xED\xA0\x80");
    body.push(0x00);
    c.send_bytes(&frame(0x30, &body)).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// WIRE-033: overlong encodings (`C0 80` for NUL) must be rejected. Accepting them
/// is the classic UTF-8 filter bypass: a topic that an ACL reads as harmless and a
/// decoder later resolves to `\0`.
#[tokio::test]
async fn an_overlong_utf8_encoding_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-overlong").await;

    let mut body = mqtt_bytes(b"wire/\xC0\x80");
    body.push(0x00);
    c.send_bytes(&frame(0x30, &body)).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// WIRE-034: a multi-byte sequence cut off at the end of the string.
#[tokio::test]
async fn a_truncated_utf8_sequence_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-truncated").await;

    // 0xE2 0x82 begins a 3-byte sequence (U+20AC) but the third byte is missing.
    let mut body = mqtt_bytes(b"wire/\xE2\x82");
    body.push(0x00);
    c.send_bytes(&frame(0x30, &body)).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// WIRE-035: a leading BOM (`U+FEFF`) is a legal character and MUST be preserved,
/// not stripped [MQTT-1.5.4-3]. Stripping it would make two distinct topics collide.
#[tokio::test]
async fn a_leading_bom_in_a_topic_is_preserved_not_stripped() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-bom").await;

    let bom_topic = "\u{FEFF}wire/bom";
    c.send_bytes(&subscribe_v5_bytes(bom_topic)).await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => assert_eq!(b[0], 0x90, "expected SUBACK, got {b:02x?}"),
        other => panic!("expected SUBACK for the BOM topic, got {other:?}"),
    }

    c.send_bytes(&publish_v5_bytes(bom_topic, b"v")).await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => {
            assert_eq!(b[0], 0x30, "expected the PUBLISH back, got {b:02x?}");
            let topic_len = usize::from(u16::from_be_bytes([b[2], b[3]]));
            let topic = &b[4..4 + topic_len];
            assert_eq!(
                topic,
                bom_topic.as_bytes(),
                "the BOM must survive the round trip byte-identically"
            );
        }
        other => panic!("expected the BOM-topic message back, got {other:?}"),
    }
}

/// WIRE-037: 4-byte sequences (emoji) round-trip byte-identically in both the
/// topic and the payload.
#[tokio::test]
async fn four_byte_utf8_round_trips_byte_identically() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-emoji").await;

    let topic = "wire/🛰/telemetry";
    let payload = "🌍🌏".as_bytes();
    c.send_bytes(&subscribe_v5_bytes(topic)).await;
    let _ = c.read_outcome(Duration::from_secs(5)).await;

    c.send_bytes(&publish_v5_bytes(topic, payload)).await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => {
            let topic_len = usize::from(u16::from_be_bytes([b[2], b[3]]));
            assert_eq!(
                &b[4..4 + topic_len],
                topic.as_bytes(),
                "topic must be exact"
            );
            assert!(
                b.ends_with(payload),
                "payload must survive byte-identically, got {b:02x?}"
            );
        }
        other => panic!("expected the emoji message back, got {other:?}"),
    }
}

/// WIRE-038 / SUB-003: a zero-length topic filter is a Protocol Error
/// [MQTT-4.7.3-1] — distinct from Malformed, because the packet parses fine and
/// it is the *value* that is illegal.
#[tokio::test]
async fn a_zero_length_topic_filter_is_refused() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-empty-filter").await;

    c.send_bytes(&subscribe_v5_bytes("")).await;
    expect_suback_reason(&mut c, TOPIC_FILTER_INVALID).await;
}

// ---------------------------------------------------------------------------
// 3.5 Properties
// ---------------------------------------------------------------------------

/// WIRE-042: an unknown property identifier is malformed [MQTT-2.2.2-1]. Skipping
/// unknown properties would be forward-compatible and is exactly what the spec
/// forbids — the length of an unknown property cannot be known, so a decoder that
/// tries to skip it is guessing at framing.
#[tokio::test]
async fn an_unknown_property_identifier_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-unknown-prop").await;

    // PUBLISH with a property block containing identifier 0x7F (unassigned).
    let mut props = vec![0x7F];
    props.extend_from_slice(&[0x00]);
    let mut body = mqtt_str("wire/t");
    body.extend_from_slice(&vbi(u32::try_from(props.len()).unwrap()));
    body.extend_from_slice(&props);
    c.send_bytes(&frame(0x30, &body)).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// WIRE-043: a property valid on a DIFFERENT packet type is refused — Will Delay
/// Interval (`0x18`) belongs to a Will's properties, not a PUBLISH.
///
/// We answer `0x82` Protocol Error. §2.2.2.2 can be read as making an
/// out-of-context property a *Malformed Packet* (`0x81`) instead, and the codec
/// classifies it as `ProtocolViolation`, which our mapping sends as `0x82`. The
/// refusal is what matters and is not in doubt; the code is the arguable half, so
/// it is asserted here and recorded in the policy register rather than left to be
/// discovered by whoever next reads the spec.
#[tokio::test]
async fn a_property_from_another_packet_type_is_refused() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-wrong-prop").await;

    let mut props = vec![0x18]; // Will Delay Interval
    props.extend_from_slice(&30u32.to_be_bytes());
    let mut body = mqtt_str("wire/t");
    body.extend_from_slice(&vbi(u32::try_from(props.len()).unwrap()));
    body.extend_from_slice(&props);
    c.send_bytes(&frame(0x30, &body)).await;
    c.expect_refused(PROTOCOL_ERROR).await;
}

/// WIRE-040: a property identifier repeated in one packet is a Protocol Error,
/// except User Property [MQTT-3.3.2-4]. Here: Payload Format Indicator twice.
#[tokio::test]
async fn a_duplicated_property_is_refused() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-dup-prop").await;

    let props = vec![0x01, 0x00, 0x01, 0x00]; // Payload Format Indicator = 0, twice
    let mut body = mqtt_str("wire/t");
    body.extend_from_slice(&vbi(u32::try_from(props.len()).unwrap()));
    body.extend_from_slice(&props);
    c.send_bytes(&frame(0x30, &body)).await;
    c.expect_refused(PROTOCOL_ERROR).await;
}

/// WIRE-044: a property block whose declared length disagrees with the properties
/// present is malformed — the decoder must not read past the block into the payload.
#[tokio::test]
async fn a_property_length_longer_than_the_block_is_malformed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-prop-len").await;

    let mut body = mqtt_str("wire/t");
    body.push(0x08); // claims 8 property bytes...
    body.extend_from_slice(&[0x01, 0x00]); // ...supplies 2
    c.send_bytes(&frame(0x30, &body)).await;
    c.expect_refused(MALFORMED_PACKET).await;
}

/// WIRE-045: a zero-length property block is legal everywhere properties are
/// permitted — the common case, and worth pinning so a stricter-than-spec decoder
/// does not regress it. (The sanity tests above rely on it, but state it directly.)
#[tokio::test]
async fn a_zero_length_property_block_is_accepted() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-zero-props").await;

    c.send_bytes(&publish_v5_bytes("wire/zero", b"x")).await;
    // No refusal: the connection stays open and usable.
    c.expect_quiet(Duration::from_millis(300)).await;
}

/// WIRE-046: Payload Format Indicator is a byte-valued property whose only legal
/// values are 0 and 1; anything else is a Protocol Error.
#[tokio::test]
async fn a_byte_property_outside_zero_or_one_is_refused() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-pfi").await;

    let props = vec![0x01, 0x02]; // Payload Format Indicator = 2
    let mut body = mqtt_str("wire/t");
    body.extend_from_slice(&vbi(u32::try_from(props.len()).unwrap()));
    body.extend_from_slice(&props);
    c.send_bytes(&frame(0x30, &body)).await;
    c.expect_refused(PROTOCOL_ERROR).await;
}

/// WIRE-048: a Subscription Identifier of 0 is a Protocol Error [MQTT-3.8.3-4].
#[tokio::test]
async fn a_zero_subscription_identifier_is_refused() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-subid-zero").await;

    let mut body = 1u16.to_be_bytes().to_vec(); // packet id
    let props = vec![0x0B, 0x00]; // Subscription Identifier = 0 (VBI)
    body.extend_from_slice(&vbi(u32::try_from(props.len()).unwrap()));
    body.extend_from_slice(&props);
    body.extend_from_slice(&mqtt_str("wire/t"));
    body.push(0x00);
    c.send_bytes(&frame(0x82, &body)).await;
    c.expect_refused(PROTOCOL_ERROR).await;
}

// ---------------------------------------------------------------------------
// Topic filter validity [MQTT-4.7.1] — SUB-004..007.
//
// These were all GRANTED before this suite existed. A granted-but-malformed
// filter is the worst outcome available: `topic_matches` returns false for it,
// so the subscription is silently inert and the client has a SUBACK saying
// otherwise. Refusing per-filter (0x8F) is what makes the failure visible.
// ---------------------------------------------------------------------------

/// SUB-004: `#` must occupy a whole level — `sport/tennis#` does not.
#[tokio::test]
async fn a_hash_sharing_its_level_is_an_invalid_filter() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-hash-level").await;
    c.send_bytes(&subscribe_v5_bytes("sport/tennis#")).await;
    expect_suback_reason(&mut c, TOPIC_FILTER_INVALID).await;
}

/// SUB-005: `#` must be the LAST level — `sport/#/ranking` is invalid.
#[tokio::test]
async fn a_hash_that_is_not_the_last_level_is_an_invalid_filter() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-hash-mid").await;
    c.send_bytes(&subscribe_v5_bytes("sport/#/ranking")).await;
    expect_suback_reason(&mut c, TOPIC_FILTER_INVALID).await;
}

/// SUB-006: `+` must occupy a whole level — `sport+` does not.
#[tokio::test]
async fn a_plus_sharing_its_level_is_an_invalid_filter() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-plus-level").await;
    c.send_bytes(&subscribe_v5_bytes("sport+")).await;
    expect_suback_reason(&mut c, TOPIC_FILTER_INVALID).await;
}

/// SUB-007: the legal wildcard forms stay legal. Without this the validator could
/// be "correct" by rejecting everything — the failure mode a refusal test cannot
/// see on its own.
#[tokio::test]
async fn the_legal_wildcard_filters_are_still_granted() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-legal-filters").await;
    for f in ["sport/+", "+", "#", "/#", "sport/#", "a/+/b", "a//b"] {
        c.send_bytes(&subscribe_v5_bytes(f)).await;
        match c.read_outcome(Duration::from_secs(5)).await {
            RawOutcome::Bytes(b) => assert!(
                b[0] == 0x90 && *b.last().unwrap() < 0x80,
                "{f:?} is a legal filter and must be granted, got {b:02x?}"
            ),
            other => panic!("expected SUBACK for {f:?}, got {other:?}"),
        }
    }
}

/// PUB-101 / MQTT-4.7.3-1: a zero-length PUBLISH topic name (with no alias to
/// resolve it) is a protocol violation, not an empty-string topic.
#[tokio::test]
async fn a_zero_length_publish_topic_is_refused() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-empty-topic").await;
    c.send_bytes(&publish_v5_bytes("", b"x")).await;
    c.expect_refused(PROTOCOL_ERROR).await;
}

// ---------------------------------------------------------------------------
// The CONNACK boundary — where the server may explain itself, and where it may not
// ---------------------------------------------------------------------------

/// After CONNACK, a decode failure is ANNOUNCED with `DISCONNECT(0x81)` before the
/// close, so a client debugging its encoder learns why [MQTT-4.13.2].
///
/// This is the pair to the test below: the same class of garbage gets a different
/// (and individually mandatory) answer on each side of the CONNACK boundary, so the
/// two are stated together.
#[tokio::test]
async fn malformed_input_after_connack_is_announced_then_closed() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-announce").await;

    // An unambiguously malformed frame: PINGREQ with a body.
    c.send_bytes(&[0xC0, 0x01, 0xFF]).await;
    c.expect_disconnect_bytes(MALFORMED_PACKET).await;
}

/// BEFORE a success CONNACK, the same garbage must be met with **silence**.
///
/// [MQTT-3.14.0-1]: the server must not send a DISCONNECT until it has sent a
/// success CONNACK. A client that has not been accepted has no session to be told
/// about, and answering would leak that the listener is a live MQTT broker to
/// anything that connects and writes a byte.
///
/// This is the constraint that stops "announce decode errors" from being applied
/// uniformly — the obvious refactor is a spec violation, so it is pinned.
#[tokio::test]
async fn malformed_input_before_connack_is_met_with_silence() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;

    // Same malformed PINGREQ, but no CONNECT has been sent.
    c.send_bytes(&[0xC0, 0x01, 0xFF]).await;
    c.expect_closed_silently().await;
}

/// A decode failure and a protocol violation get DIFFERENT reason codes: `0x81`
/// means "these bytes are not MQTT", `0x82` means "they are MQTT and they are
/// illegal". Collapsing the two would tell a client nothing it can act on.
#[tokio::test]
async fn malformed_and_protocol_errors_are_told_apart() {
    let addr = start_broker().await;

    // Not parseable as MQTT: a PINGREQ carrying a body.
    let mut c = connected(addr, "wire-split-malformed").await;
    c.send_bytes(&[0xC0, 0x01, 0xFF]).await;
    c.expect_disconnect_bytes(MALFORMED_PACKET).await;

    // Parseable, but says something the spec forbids: Subscription Identifier 0.
    let mut c = connected(addr, "wire-split-protocol").await;
    let mut body = 1u16.to_be_bytes().to_vec();
    let props = vec![0x0B, 0x00];
    body.extend_from_slice(&vbi(u32::try_from(props.len()).unwrap()));
    body.extend_from_slice(&props);
    body.extend_from_slice(&mqtt_str("wire/t"));
    body.push(0x00);
    c.send_bytes(&frame(0x82, &body)).await;
    c.expect_disconnect_bytes(PROTOCOL_ERROR).await;
}

// ---------------------------------------------------------------------------
// Reason-code provocations (Phase 3d) — codes the broker can emit that no test
// had ever actually observed on the wire.
// ---------------------------------------------------------------------------

/// FLOW-021 / `0x95`: a packet that grows past the inbound ceiling while still
/// incomplete is refused with Packet too large.
///
/// **Read the shape of this test carefully — it is narrower than its name suggests,
/// and the narrowness is the finding.** `next_packet` (every TCP/TLS client's path)
/// tries `Packet::decode` FIRST and only checks the ceiling when decode says
/// "incomplete". So the ceiling catches a packet that stalls oversized, which is
/// what this test provokes — but a *complete* oversized packet decodes successfully
/// and is **accepted**, ceiling and advertised Maximum Packet Size notwithstanding.
///
/// The declared-length check that would refuse from the header alone exists in
/// `take_raw_frame`, used only by the QUIC mux — and `frame.rs`'s own
/// `oversized_packet_is_rejected_not_buffered` exercises that function, so the
/// property is tested on the path clients do not use. Filed separately; the memory
/// is still bounded, so this is "refuses later and less often than it should", not
/// an unbounded-allocation hole.
#[tokio::test]
async fn a_packet_over_the_inbound_ceiling_is_refused() {
    let addr = start_broker().await;
    let mut c = connected(addr, "wire-too-large").await;

    // Declare 4 MiB and send 2 MiB of it: the buffer passes the ceiling while the
    // packet is still incomplete, which is the condition the check actually tests.
    let mut header = vec![0x30]; // PUBLISH
    header.extend_from_slice(&vbi(4 * 1024 * 1024));
    header.extend_from_slice(&mqtt_str("wire/big"));
    c.send_bytes(&header).await;
    c.send_bytes(&vec![b'x'; 2 * 1024 * 1024]).await;

    c.expect_disconnect_bytes(PACKET_TOO_LARGE).await;
}

/// CONN-012 / `0x85`: a zero-length Client Identifier is only meaningful with Clean
/// Start = 1, because the server assigns an id that exists for one connection. With
/// Clean Start = 0 the client is asking to resume a session it cannot name, so the
/// CONNECT is refused [MQTT-3.1.3-8].
///
/// The v3.1.1 form of this refusal (return code `0x02`) was already covered; the v5
/// form was not, and they are different code spaces.
#[tokio::test]
async fn an_empty_client_id_without_clean_start_is_refused() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;

    let mut body = mqtt_str("MQTT");
    body.push(5); // v5
    body.push(0x00); // connect flags: clean start CLEARED
    body.extend_from_slice(&0u16.to_be_bytes()); // keep alive
    body.push(0x00); // no properties
    body.extend_from_slice(&mqtt_str("")); // zero-length client id
    c.send_bytes(&frame(0x10, &body)).await;

    c.expect_connack_bytes(CLIENT_IDENTIFIER_NOT_VALID).await;
}

/// The same CONNECT with Clean Start SET is accepted, and the server assigns an
/// identifier. Without this the test above could pass against a broker that simply
/// rejects every empty client id — the refusal must be specific to the combination.
#[tokio::test]
async fn an_empty_client_id_with_clean_start_is_assigned_one() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;

    c.send_bytes(&connect_v5_bytes("")).await; // clean start is set by the builder
    c.expect_connack_bytes(0x00).await;
}

// ---------------------------------------------------------------------------
// UNSUBACK reason codes (issue #290, [MQTT-3.11.3-1])
// ---------------------------------------------------------------------------

/// A minimal, VALID v5 UNSUBSCRIBE for `filters`, packet id `pkid`.
fn unsubscribe_v5_bytes(pkid: u16, filters: &[&str]) -> Vec<u8> {
    let mut body = pkid.to_be_bytes().to_vec();
    body.push(0x00); // property length 0
    for f in filters {
        body.extend_from_slice(&mqtt_str(f));
    }
    frame(0xA2, &body)
}

/// Issue #290 — a v5 UNSUBACK carries EXACTLY one reason code per requested
/// filter, in request order [MQTT-3.11.3-1]: `0x00` where a subscription was
/// removed, `0x11 No subscription existed` where there was nothing to remove,
/// `0x8F Topic Filter invalid` for a structurally invalid filter. Asserted on
/// the raw bytes, because the typed client would hide a payload-length mismatch —
/// the defect was an UNSUBACK whose payload was structurally EMPTY.
#[tokio::test]
async fn v5_unsuback_answers_one_reason_code_per_filter_in_order() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;
    c.send_bytes(&connect_v5_bytes("unsub-codes")).await;
    c.expect_connack_bytes(0x00).await;
    c.send_bytes(&subscribe_v5_bytes("a/b")).await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => assert_eq!(b[0], 0x90, "expected SUBACK, got {b:02x?}"),
        other => panic!("expected SUBACK, got {other:?}"),
    }

    // Subscribed, never-subscribed, structurally invalid — one code each, in order.
    c.send_bytes(&unsubscribe_v5_bytes(7, &["a/b", "never/was", "a/#/b"]))
        .await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => assert_eq!(
            b,
            vec![0xB0, 0x06, 0x00, 0x07, 0x00, 0x00, 0x11, 0x8F],
            "UNSUBACK must be pkid 7, empty properties, then exactly \
             [0x00 removed, 0x11 no subscription existed, 0x8F filter invalid]"
        ),
        other => panic!("expected UNSUBACK, got {other:?}"),
    }

    // The 0x00 above must have MEANT removal: the same filter again is now 0x11.
    c.send_bytes(&unsubscribe_v5_bytes(8, &["a/b"])).await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => assert_eq!(
            b,
            vec![0xB0, 0x04, 0x00, 0x08, 0x00, 0x11],
            "re-unsubscribing a removed filter answers 0x11, proving the first \
             0x00 removed it"
        ),
        other => panic!("expected UNSUBACK, got {other:?}"),
    }
}

/// The version gate this fix must not break: a v3.1.1 UNSUBACK is EXACTLY
/// `0xB0 0x02 <pkid>` — no properties, no reason codes — whatever was
/// unsubscribed. The codes are still computed internally (the removal logic is
/// shared); the encoder drops them below v5, and this pins that.
#[tokio::test]
async fn v311_unsuback_stays_two_bytes_with_no_reason_codes() {
    let addr = start_broker().await;
    let mut c = RawClient::open(addr).await;
    // Hand-assembled v3.1.1 CONNECT: protocol level 4, no properties block.
    let mut body = mqtt_str("MQTT");
    body.push(4);
    body.push(0x02); // clean session
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&mqtt_str("unsub-v311"));
    c.send_bytes(&frame(0x10, &body)).await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => assert_eq!(b[0], 0x20, "expected CONNACK, got {b:02x?}"),
        other => panic!("expected CONNACK, got {other:?}"),
    }

    // v3.1.1 UNSUBSCRIBE: pkid + filters, NO property length byte.
    let mut ubody = 9u16.to_be_bytes().to_vec();
    ubody.extend_from_slice(&mqtt_str("never/was"));
    c.send_bytes(&frame(0xA2, &ubody)).await;
    match c.read_outcome(Duration::from_secs(5)).await {
        RawOutcome::Bytes(b) => assert_eq!(
            b,
            vec![0xB0, 0x02, 0x00, 0x09],
            "a v3.1.1 UNSUBACK carries the packet id and nothing else"
        ),
        other => panic!("expected UNSUBACK, got {other:?}"),
    }
}
