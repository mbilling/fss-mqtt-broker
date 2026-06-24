---
adr: "0008"
title: MQTT 5.0 codec
adr_status: Accepted
tasks:
  - id: 0008-P1
    title: Properties foundation (Property enum, Properties block codec, string-pair)
    status: done
    date: 2026-06-22
    evidence: properties::every_value_type_roundtrips; properties::truncated_value_is_malformed
  - id: 0008-P2
    title: CONNECT/CONNACK v5 (connect + will properties; connack reason + properties)
    status: done
    date: 2026-06-22
    evidence: roundtrip_connect_v5_with_properties; v4_wire_is_unchanged_by_empty_properties
  - id: 0008-P3
    title: PUBLISH + ack family v5 (reason + properties; short-form rules)
    status: done
    date: 2026-06-22
    evidence: roundtrip_publish_v5_with_properties; ack_v5_short_and_long_no_property_forms_agree
  - id: 0008-P4
    title: SUBSCRIBE/SUBACK/UNSUBSCRIBE/UNSUBACK v5 (subscription options byte; reason + properties)
    status: done
    date: 2026-06-22
    evidence: roundtrip_subscribe_v5_with_options_and_properties; subscribe_v5_reserved_and_bad_retain_handling_are_rejected
  - id: 0008-P5
    title: DISCONNECT + AUTH v5 (reason + properties; new AUTH packet)
    status: done
    date: 2026-06-22
    evidence: roundtrip_disconnect_and_auth_v5; auth_is_rejected_on_v3_1_1
  - id: 0008-P6
    title: Accept v5 in the broker (remove V5_UNSUPPORTED, negotiate at CONNECT)
    status: done
    date: 2026-06-22
    evidence: v5_protocol::v5_connect_and_pubsub_roundtrip; conn.rs set_version on negotiated CONNECT
  - id: 0008-T7
    title: Codec-owned property validation (allowed-on-packet-type + duplicate non-repeatable -> Protocol Error)
    status: done
    date: 2026-06-24
    evidence: "properties::PropContext + Properties::validate_for/decode_for enforce per-packet-type allowed properties and reject duplicate non-repeatable properties (User Property always repeatable; Subscription Identifier only on PUBLISH) as ProtocolViolation. Wired into every packet decode site in packet.rs. Tests: validate_rejects_a_property_illegal_on_the_packet, _duplicated_non_repeatable_property, subscription_identifier_repeats_only_on_publish, decode_for_rejects_an_illegal_property_at_the_wire_boundary."
  - id: 0008-T8
    title: Shared reason-code constants module (reason::SUCCESS, reason::NOT_AUTHORIZED, ...)
    status: done
    date: 2026-06-24
    evidence: "New mqtt_codec::reason module: named u8 constants for the MQTT 5 reason codes + is_error(); conn.rs sources every v5 reason code from it (the v3.1.1 CONNACK return codes stay distinct). Test canonical_wire_values_and_error_classification."
---

# Delivery — ADR 0008: MQTT 5.0 codec

Decision: [docs/adr/0008-mqtt-5-codec.md](../adr/0008-mqtt-5-codec.md).

## Plan

The decision's §5 phased implementation gives six tested, gated, committed phases on the
single version-tagged `Packet` enum, with the broker-acceptance flip last. Each task
carries a stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0008-P1** Properties foundation | A `Property` enum (one variant per identifier, typed value), a `Properties(Vec<Property>)` newtype owning the varint length-prefixed block codec parsed to exactly that length, and the string-pair primitive — decoded/encoded in isolation. User Property is the only repeatable id and order is preserved; out-of-bounds values are Malformed. |
| **0008-P2** CONNECT/CONNACK | Connect + will properties; connack reason + properties. v4 wire is byte-identical with empty properties. |
| **0008-P3** PUBLISH + acks | Publish properties; PUBACK/REC/REL/COMP gain `reason: u8` + properties with the short-form omit-trailing-fields rules. |
| **0008-P4** SUBSCRIBE family | Per-filter subscription-options byte (No-Local, Retain-As-Published, Retain-Handling) in place of bare QoS; reason codes + properties; reserved/invalid option bits rejected. |
| **0008-P5** DISCONNECT + AUTH | DISCONNECT reason + properties; the new AUTH packet; AUTH refused on v3.1.1. |
| **0008-P6** Broker acceptance | `V5_UNSUPPORTED` removed; version negotiated at CONNECT and v5 connections accepted, honouring the v5 behaviours the broker already has analogues for. |
| **0008-T7** Property validation | The codec rejects a property not allowed on its packet type and a duplicated non-repeatable property as Protocol Error. |
| **0008-T8** Reason constants | Named reason-code constants live in a shared `reason` module rather than bare `u8` literals. |

## Progress

<!-- status-table:0008 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0008-P1 | ✅ done | 2026-06-22 | properties::every_value_type_roundtrips; properties::truncated_value_is_malformed |
| 0008-P2 | ✅ done | 2026-06-22 | roundtrip_connect_v5_with_properties; v4_wire_is_unchanged_by_empty_properties |
| 0008-P3 | ✅ done | 2026-06-22 | roundtrip_publish_v5_with_properties; ack_v5_short_and_long_no_property_forms_agree |
| 0008-P4 | ✅ done | 2026-06-22 | roundtrip_subscribe_v5_with_options_and_properties; subscribe_v5_reserved_and_bad_retain_handling_are_rejected |
| 0008-P5 | ✅ done | 2026-06-22 | roundtrip_disconnect_and_auth_v5; auth_is_rejected_on_v3_1_1 |
| 0008-P6 | ✅ done | 2026-06-22 | v5_protocol::v5_connect_and_pubsub_roundtrip; conn.rs set_version on negotiated CONNECT |
| 0008-T7 | ✅ done | 2026-06-24 | "properties::PropContext + Properties::validate_for/decode_for enforce per-packet-type allowed properties and reject duplicate non-repeatable properties (User Property always repeatable; Subscription Identifier only on PUBLISH) as ProtocolViolation. Wired into every packet decode site in packet.rs. Tests: validate_rejects_a_property_illegal_on_the_packet, _duplicated_non_repeatable_property, subscription_identifier_repeats_only_on_publish, decode_for_rejects_an_illegal_property_at_the_wire_boundary." |
| 0008-T8 | ✅ done | 2026-06-24 | "New mqtt_codec::reason module: named u8 constants for the MQTT 5 reason codes + is_error(); conn.rs sources every v5 reason code from it (the v3.1.1 CONNACK return codes stay distinct). Test canonical_wire_values_and_error_classification." |
<!-- /status-table:0008 -->

**Coverage note:** the value-decode-within-bounds half of P1's codec-owned validation is
built and tested (`unknown_identifier_is_malformed`, `block_length_overrunning_the_packet_is_malformed`);
the two structural rules (allowed-on-packet-type, duplicate non-repeatable) are split out as
T7. Property decoding is fuzzed via `mqtt-codec/fuzz/fuzz_targets/packet_decode.rs`.

## Changelog

- **2026-06-22** — Migration audit: phases 1–6 verified built against the codec test suite
  and v5 end-to-end tests (`crates/mqttd/tests/v5_protocol.rs`). `V5_UNSUPPORTED` confirmed
  removed and v5 negotiated at CONNECT. Two design caveats split out as deferred: codec-owned
  property validation (T7) and a shared `reason::` constants module (T8), both intentionally
  not implemented at the wire layer.
