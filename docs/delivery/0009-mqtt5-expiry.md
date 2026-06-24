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
