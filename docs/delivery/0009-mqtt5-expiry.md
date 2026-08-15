---
adr: "0009"
title: MQTT 5.0 session & message expiry
adr_status: Accepted
tasks:
  - id: 0009-P1
    title: Session expiry (normalization, hub attach/detach lifecycle, sweep GC, accessor)
    status: done
    date: 2026-06-22
    evidence: hub::session_expiry_finite_retains_then_expires; hub::session_expiry_reconnect_cancels_expiry
  - id: 0009-P2
    title: Message expiry (stored absolute deadline, drop-on-expiry at replay, remaining-interval on delivery)
    status: done
    date: 2026-06-22
    evidence: logged::enqueue_with_expiry_round_trips_the_deadline; hub::replayed_message_forwards_remaining_expiry_interval
  - id: 0009-P3
    title: Durable expiry deadline (persist disconnect time so takeover preserves the clock)
    status: done
    date: 2026-06-24
    evidence: "ADR 0009 phase 3. SessionMeta persists session_expiry_at (absolute epoch); the hub's expiring map + sweep use absolute wall-clock (Clock) so deadlines are portable; detach persists the deadline, attach (persistent only) clears it; the sweep reconciles store.expiring_sessions() for OWNED, offline, untracked sessions every EXPIRY_RECONCILE_EVERY ticks so a new owner inherits orphaned deadlines after a takeover and expires them at the original time. Tests inherited_session_expiry_is_swept_after_takeover, session_expiry_finite_retains_then_expires (clock-driven), session_expiry_persists_and_enumerates, decodes_pre_expiry_meta_records; full workspace green."
  - id: 0009-P4
    title: "A DISCONNECT's Session Expiry Interval overrides the CONNECT's (§3.14.2.2.2, issue #298)"
    status: done
    date: 2026-08-15
    evidence: "FOUND BY THE INDEPENDENT ORACLE, not by us: the Eclipse paho.mqtt.testing suite adopted in 0034-T9 failed test_session_expiry on its first run. §3.14.2.2.2 says a Session Expiry Interval on the DISCONNECT OVERRIDES the one agreed at CONNECT — the documented way for a client to say 'I connected expecting to be brief, but hold my session, I'll be back'. conn.rs's DISCONNECT arm read `d.reason` and dropped `d.properties` on the floor, and session expiry was decided once at CONNECT (session_policy) and never revisited. The failure was SILENT: the client got a clean DISCONNECT and only discovered the loss on reconnect, subscriptions and queued messages gone. DELIVERED: the DISCONNECT arm reads session_expiry_interval() and carries it out through a new `session_expiry_override: Option<u32>` on HubCommand::Detach — the natural channel, since Detach already carries the client, conn_id and graceful flag; an out-param rather than a richer `serve` return type, because every `return Ok(..)` in that loop would otherwise have to carry a value only one of them can ever set. The hub's detach path applies the override to `session_expiry` before the retention match, so it governs BOTH this detach and the stored session: a client that reconnects without naming an interval gets the terms it last asked for, not the ones it has since revised. THE OTHER HALF, which is the easy one to skip: if the CONNECT's interval was 0, a non-zero one on the DISCONNECT is a Protocol Error — a session that agreed to expire immediately cannot be resurrected on the way out. That is refused with DISCONNECT 0x82 before the close (post-CONNACK, so announcing is correct per [MQTT-4.13.2]) and the override is NOT applied. The two internal detach call sites (eviction, rehome close) pass None: neither is a client DISCONNECT, so there is no override to carry. TESTS, red-first: v5_protocol::v5_disconnect_session_expiry_overrides_the_connect_value (connect at 1s, subscribe, disconnect at 300s, wait past 4s, reconnect → session_present) and v5_disconnect_cannot_raise_an_expiry_the_connect_set_to_zero (asserts the 0x82 on the wire AND that the refused override was not applied). Both proven RED before the fix. Reverse-mutation: neutering the hub's override arm turns the first test red while the second stays green, which is correct — its guard lives in conn.rs. New harness helper Client::disconnect_with(properties). ACCEPTANCE IS THE ORACLE ITSELF: ./scripts/interop/paho-testing.sh went from 5 declared failures to 4, and its test_session_expiry entry had to be DELETED or the gate would fail for the right reason — 'now PASSES; delete its EXPECTED entry'. That is the ledger discipline working live for the first time. 1,347 tests green; fmt and clippy clean."
---

# Delivery — ADR 0009: MQTT 5.0 session & message expiry

Decision: [docs/adr/0009-mqtt5-expiry.md](../adr/0009-mqtt5-expiry.md).

## Plan

The decision's §5 phased implementation gives three phases: session expiry, then the
storage-format message-expiry change, then the durable-deadline follow-up that closes the
§2 carried limitation. Each task carries a stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0009-P1** Session expiry | Both versions normalize to `(clean_start, session_expiry)` at the connection edge (v3.1.1 falls out as the `{0, 0xFFFFFFFF}` cases); the hub records the interval on attach (cancelling any pending deadline), and on detach discards immediately (`0`), keeps forever (`0xFFFFFFFF`), or arms `now + session_expiry`; a periodic sweep tick discards every session past its deadline via a single `discard_session` helper; a `session_expiry_interval()` accessor reads the property. |
| **0009-P2** Message expiry | A queued entry carries an absolute deadline (`now + interval` on enqueue, none if absent); replay/delivery drops past-deadline entries and sets the outbound Message Expiry Interval to the remaining seconds. A storage-format change. |
| **0009-P3** Durable deadline | The disconnect time is persisted in the session's durable meta snapshot so a replica takeover preserves the session-expiry clock instead of restarting it. |

## Progress

<!-- status-table:0009 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0009-P1 | ✅ done | 2026-06-22 | hub::session_expiry_finite_retains_then_expires; hub::session_expiry_reconnect_cancels_expiry |
| 0009-P2 | ✅ done | 2026-06-22 | logged::enqueue_with_expiry_round_trips_the_deadline; hub::replayed_message_forwards_remaining_expiry_interval |
| 0009-P3 | ✅ done | 2026-06-24 | "ADR 0009 phase 3. SessionMeta persists session_expiry_at (absolute epoch); the hub's expiring map + sweep use absolute wall-clock (Clock) so deadlines are portable; detach persists the deadline, attach (persistent only) clears it; the sweep reconciles store.expiring_sessions() for OWNED, offline, untracked sessions every EXPIRY_RECONCILE_EVERY ticks so a new owner inherits orphaned deadlines after a takeover and expires them at the original time. Tests inherited_session_expiry_is_swept_after_takeover, session_expiry_finite_retains_then_expires (clock-driven), session_expiry_persists_and_enumerates, decodes_pre_expiry_meta_records; full workspace green." |
| 0009-P4 | ✅ done | 2026-08-15 | "FOUND BY THE INDEPENDENT ORACLE, not by us: the Eclipse paho.mqtt.testing suite adopted in 0034-T9 failed test_session_expiry on its first run. §3.14.2.2.2 says a Session Expiry Interval on the DISCONNECT OVERRIDES the one agreed at CONNECT — the documented way for a client to say 'I connected expecting to be brief, but hold my session, I'll be back'. conn.rs's DISCONNECT arm read `d.reason` and dropped `d.properties` on the floor, and session expiry was decided once at CONNECT (session_policy) and never revisited. The failure was SILENT: the client got a clean DISCONNECT and only discovered the loss on reconnect, subscriptions and queued messages gone. DELIVERED: the DISCONNECT arm reads session_expiry_interval() and carries it out through a new `session_expiry_override: Option<u32>` on HubCommand::Detach — the natural channel, since Detach already carries the client, conn_id and graceful flag; an out-param rather than a richer `serve` return type, because every `return Ok(..)` in that loop would otherwise have to carry a value only one of them can ever set. The hub's detach path applies the override to `session_expiry` before the retention match, so it governs BOTH this detach and the stored session: a client that reconnects without naming an interval gets the terms it last asked for, not the ones it has since revised. THE OTHER HALF, which is the easy one to skip: if the CONNECT's interval was 0, a non-zero one on the DISCONNECT is a Protocol Error — a session that agreed to expire immediately cannot be resurrected on the way out. That is refused with DISCONNECT 0x82 before the close (post-CONNACK, so announcing is correct per [MQTT-4.13.2]) and the override is NOT applied. The two internal detach call sites (eviction, rehome close) pass None: neither is a client DISCONNECT, so there is no override to carry. TESTS, red-first: v5_protocol::v5_disconnect_session_expiry_overrides_the_connect_value (connect at 1s, subscribe, disconnect at 300s, wait past 4s, reconnect → session_present) and v5_disconnect_cannot_raise_an_expiry_the_connect_set_to_zero (asserts the 0x82 on the wire AND that the refused override was not applied). Both proven RED before the fix. Reverse-mutation: neutering the hub's override arm turns the first test red while the second stays green, which is correct — its guard lives in conn.rs. New harness helper Client::disconnect_with(properties). ACCEPTANCE IS THE ORACLE ITSELF: ./scripts/interop/paho-testing.sh went from 5 declared failures to 4, and its test_session_expiry entry had to be DELETED or the gate would fail for the right reason — 'now PASSES; delete its EXPECTED entry'. That is the ledger discipline working live for the first time. 1,347 tests green; fmt and clippy clean." |
<!-- /status-table:0009 -->

**Carried limitation (from §2):** the expiry deadline lives only in the owner's in-memory
`expiring: HashMap<ClientId, Instant>`; the replicated log carries the session data but not
the deadline, so on owner death + replica takeover the session survives while its clock
restarts. P3 closes this by persisting the disconnect time. The `session_policy`
normalization and the `session_expiry_interval()` / `message_expiry_interval()` accessors
are built but have no isolated unit tests — they are exercised through the hub tests and the
`v5_protocol.rs` integration suite.

## Changelog

- **2026-06-22** — Migration audit: P1 (session expiry — normalization, attach/detach
  lifecycle, sweep GC) and P2 (message expiry — stored absolute deadline, drop-on-replay,
  remaining-interval on delivery) verified built against hub and storage tests plus
  `v5_protocol.rs` e2e. P3 (durable disconnect-time deadline) confirmed not built and split
  out as a deferred §2 follow-up.
