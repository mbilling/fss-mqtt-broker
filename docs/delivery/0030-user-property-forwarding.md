---
adr: "0030"
title: Forward MQTT 5 User Properties through delivery
adr_status: Accepted
tasks:
  - id: 0030-T1
    title: Message carries user_properties; single-node ingestion + delivery re-emits them
    status: done
    date: 2026-06-25
    evidence: "mqtt_core::Message.user_properties; conn ingestion captures inbound PUBLISH User Properties; publish_packet re-emits them. Tests: v5_protocol::v5_user_properties_are_forwarded_to_subscribers, mqtt-bridge client round-trip (hop-count property survives a broker hop)."
  - id: 0030-T2
    title: Offline/durable queue persists user properties (memory field + backward-compatible logged codec)
    status: done
    date: 2026-06-25
    evidence: "logged.rs encode_queued/decode_queued append the property pairs last, EOF-defaulted. Tests: queued_codec_round_trips_user_properties, queued_codec_reads_a_pre_0030_record_as_empty, user_properties_survive_enqueue_and_replay."
  - id: 0030-T3
    title: Cross-node + shared-subscription forwarding (PeerMessage::Publish/SharedDeliver carry user properties)
    status: done
    date: 2026-06-25
    evidence: "PeerMessage::Publish/SharedDeliver gained user_properties (serde); forward_to_peers/send_shared_to_peer + the peer→hub bridge thread them. Test: cluster::user_properties_survive_cross_node_delivery. (Shared path shares the deliver_to_client carrier.)"
  - id: 0030-T4
    title: Will-message user properties forwarded on a published will
    status: done
    date: 2026-06-25
    evidence: "into_will captures the LastWill's User Properties; the will publish carries them. Test: v5_protocol::v5_will_user_properties_are_forwarded."
  - id: 0030-T5
    title: Remaining message-level application properties (content-type, response-topic, correlation-data, payload-format)
    status: deferred
    notes: "Out of scope for the bridge-unblocking fix; User Properties are the explicit MUST (MQTT-3.3.2-17) and the only one ADR 0025 needs. Tracked here rather than silently omitted; same plumbing pattern as T1-T3 when done."
---

# Delivery — ADR 0030: Forward MQTT 5 User Properties

Decision: [docs/adr/0030-user-property-forwarding.md](../adr/0030-user-property-forwarding.md).

The broker silently drops a publisher's User Properties on delivery (the `Message` type has
no property fields; `publish_packet` rebuilds a fresh property block) — a violation of
MQTT-3.3.2-17 and a blocker for the boundary bridge's hop-count loop-prevention (ADR
0025-T5). This carries User Properties on `Message` and re-emits them on every delivery
path, test-first.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0030-T1** Message + single-node | `Message.user_properties: Vec<(String, String)>`; the inbound PUBLISH's User Properties are captured on ingest and re-emitted by `publish_packet`. A single-broker publish→deliver round-trips a User Property to a subscriber, in order. |
| **0030-T2** Queue persistence | The in-memory store carries the field; the durable queued-message codec appends the pairs backward-compatibly (a record without them decodes to empty). An offline subscriber's replayed message keeps its User Properties. |
| **0030-T3** Cluster | `PeerMessage::Publish` and `SharedDeliver` carry `user_properties`; a cross-node delivery and a shared-subscription delivery both re-emit the originating publisher's properties. |
| **0030-T4** Will | A connection's Will User Properties are captured; when the will fires, the published will message forwards them. |
| **0030-T5** Other app properties | *(deferred)* Forward Payload Format Indicator, Content Type, Response Topic, Correlation Data the same way. |

## Progress

<!-- status-table:0030 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0030-T1 | ✅ done | 2026-06-25 | "mqtt_core::Message.user_properties; conn ingestion captures inbound PUBLISH User Properties; publish_packet re-emits them. Tests: v5_protocol::v5_user_properties_are_forwarded_to_subscribers, mqtt-bridge client round-trip (hop-count property survives a broker hop)." |
| 0030-T2 | ✅ done | 2026-06-25 | "logged.rs encode_queued/decode_queued append the property pairs last, EOF-defaulted. Tests: queued_codec_round_trips_user_properties, queued_codec_reads_a_pre_0030_record_as_empty, user_properties_survive_enqueue_and_replay." |
| 0030-T3 | ✅ done | 2026-06-25 | "PeerMessage::Publish/SharedDeliver gained user_properties (serde); forward_to_peers/send_shared_to_peer + the peer→hub bridge thread them. Test: cluster::user_properties_survive_cross_node_delivery. (Shared path shares the deliver_to_client carrier.)" |
| 0030-T4 | ✅ done | 2026-06-25 | "into_will captures the LastWill's User Properties; the will publish carries them. Test: v5_protocol::v5_will_user_properties_are_forwarded." |
| 0030-T5 | 💤 deferred | — | "Out of scope for the bridge-unblocking fix; User Properties are the explicit MUST (MQTT-3.3.2-17) and the only one ADR 0025 needs. Tracked here rather than silently omitted; same plumbing pattern as T1-T3 when done." |
<!-- /status-table:0030 -->

## Changelog

- **2026-06-25** — ADR accepted and delivery opened. Surfaced while building the boundary
  bridge (ADR 0025): the hop-count loop-prevention needs User Properties to survive a broker
  hop, which our broker did not honour. Fixing the conformance gap first (this ADR), then
  resuming 0025.
