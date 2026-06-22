---
adr: "0011"
title: MQTT 5.0 topic aliases
adr_status: Accepted
tasks:
  - id: 0011-T1
    title: Per-connection alias maps in the connection layer; hub never sees aliases
    status: done
    date: 2026-06-17
    evidence: mqttd/src/aliases.rs (InboundAliases/OutboundAliases); resolved at edge in conn.rs
  - id: 0011-T2
    title: Inbound resolve + validate; advertise Topic Alias Maximum in CONNACK
    status: done
    date: 2026-06-17
    evidence: InboundAliases::resolve; v5_inbound_topic_alias_resolves_to_full_topic
  - id: 0011-T3
    title: Inbound protocol-error close on alias 0 / above max / unmapped reference
    status: done
    date: 2026-06-17
    evidence: topic_alias_zero_closes_connection / topic_alias_above_maximum_closes_connection / unmapped_topic_alias_reference_closes_connection
  - id: 0011-T4
    title: Outbound assign-until-full, no eviction, bounded by client maximum
    status: done
    date: 2026-06-17
    evidence: OutboundAliases::apply; v5_outbound_topic_alias_assigned_then_referenced; outbound_stops_assigning_when_full_but_keeps_existing
  - id: 0011-T5
    title: State per-connection, dropped on disconnect; v3.1.1 inert (max 0)
    status: done
    date: 2026-06-17
    evidence: aliases owned by connection task in conn.rs; InboundAliases::new(0) for non-v5 (inbound_with_zero_max_rejects_any_alias)
  - id: 0011-T6
    title: Configurable server Topic Alias Maximum
    status: deferred
    notes: SERVER_TOPIC_ALIAS_MAX is a fixed constant (16) in conn.rs, not yet configurable (ADR 0011 §2 / Consequences); still holds
  - id: 0011-T7
    title: Emit DISCONNECT 0x94 (Topic Alias Invalid) instead of bare close
    status: deferred
    notes: invalid alias closes the connection rather than sending DISCONNECT 0x94; folded into the later act-on-v5-reason-codes work (ADR 0011 §2)
---

# Delivery — ADR 0011: MQTT 5.0 topic aliases

Decision: [docs/adr/0011-topic-aliases.md](../adr/0011-topic-aliases.md).

## Plan

The decision's four numbered parts plus its two documented limits decompose into these
tasks. Each carries a stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0011-T1** Edge-only maps | Alias maps live in `mqttd::aliases`; inbound PUBLISHes are resolved to full topic names before the hub, outbound rewritten after; routing/persistence/cluster stay alias-free. |
| **0011-T2** Inbound resolve | CONNACK advertises a Topic Alias Maximum; an inbound PUBLISH with a topic + alias records the mapping, an empty topic + alias references it, and resolves to the full topic before the hub. |
| **0011-T3** Inbound validation | Alias `0`, alias `> max`, or an unmapped reference is a protocol error that closes the connection. |
| **0011-T4** Outbound assign | When the client advertised a non-zero maximum, an unmapped topic with a free slot is assigned the next alias (full name sent), a mapped topic is referenced (empty name), and a full table sends the topic un-aliased — assign-until-full, no eviction. |
| **0011-T5** Per-connection lifetime | Both maps are owned by the connection task and dropped on end; takeover/reconnect starts empty; v3.1.1 is created with `max = 0` and is inert. |
| **0011-T6** Configurable maximum | The server's advertised Topic Alias Maximum becomes configurable rather than a fixed constant. |
| **0011-T7** DISCONNECT 0x94 | An invalid alias sends a `0x94` Topic Alias Invalid DISCONNECT before closing. |

## Progress

<!-- status-table:0011 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0011-T1 | ✅ done | 2026-06-17 | mqttd/src/aliases.rs (InboundAliases/OutboundAliases); resolved at edge in conn.rs |
| 0011-T2 | ✅ done | 2026-06-17 | InboundAliases::resolve; v5_inbound_topic_alias_resolves_to_full_topic |
| 0011-T3 | ✅ done | 2026-06-17 | topic_alias_zero_closes_connection / topic_alias_above_maximum_closes_connection / unmapped_topic_alias_reference_closes_connection |
| 0011-T4 | ✅ done | 2026-06-17 | OutboundAliases::apply; v5_outbound_topic_alias_assigned_then_referenced; outbound_stops_assigning_when_full_but_keeps_existing |
| 0011-T5 | ✅ done | 2026-06-17 | aliases owned by connection task in conn.rs; InboundAliases::new(0) for non-v5 (inbound_with_zero_max_rejects_any_alias) |
| 0011-T6 | 💤 deferred | — | SERVER_TOPIC_ALIAS_MAX is a fixed constant (16) in conn.rs, not yet configurable (ADR 0011 §2 / Consequences); still holds |
| 0011-T7 | 💤 deferred | — | invalid alias closes the connection rather than sending DISCONNECT 0x94; folded into the later act-on-v5-reason-codes work (ADR 0011 §2) |
<!-- /status-table:0011 -->

**Documented limits still in force:** the server's advertised Topic Alias Maximum is the
fixed constant `SERVER_TOPIC_ALIAS_MAX = 16` in `conn.rs`, not configurable (T6); and an
invalid inbound alias closes the connection rather than emitting DISCONNECT `0x94` (T7).
Both confirmed against `conn.rs` and the protocol-violation tests.

## Changelog

- **2026-06-17** — Topic aliases landed: edge-only per-connection maps (T1), inbound
  resolve + advertised maximum (T2), inbound protocol-error close on invalid aliases (T3),
  outbound assign-until-full with no eviction (T4), and per-connection lifetime with inert
  v3.1.1 (T5). T6 (configurable maximum) and T7 (DISCONNECT `0x94`) deferred per the ADR's
  documented limits.
