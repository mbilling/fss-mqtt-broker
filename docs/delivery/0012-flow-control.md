---
adr: "0012"
title: MQTT 5.0 flow control (Receive Maximum)
adr_status: Accepted
tasks:
  - id: 0012-T1
    title: Enforce server to client Receive Maximum in the hub via a backlog
    status: done
    date: 2026-06-17
    evidence: push_backlog/drain_backlog (hub.rs); v5_receive_maximum_limits_inflight_until_acked
  - id: 0012-T2
    title: Backlog survives disconnect — persistent session spills to durable queue
    status: done
    date: 2026-06-17
    evidence: flush_backlog_to_store (hub.rs); quota_backlog_spills_to_store_on_persistent_detach
  - id: 0012-T3
    title: Advertise server Receive Maximum in CONNACK; v3.1.1 unlimited default
    status: done
    date: 2026-06-17
    evidence: SERVER_RECEIVE_MAXIMUM in conn.rs; v5_receive_maximum_is_advertised_and_forwarded / v311_receive_maximum_defaults_to_unlimited
  - id: 0012-T4
    title: Quota counts unacked QoS>0 publishes (PUBLISH..PUBCOMP), not packet ids
    status: done
    date: 2026-06-17
    evidence: Inflight.pending.len() gates send (hub.rs); v5_receive_maximum_limits_inflight_until_acked
  - id: 0012-T5
    title: Backlog bounded by MAX_BACKLOG, drop-oldest overflow
    status: done
    date: 2026-06-17
    evidence: MAX_BACKLOG / push_backlog; flow_control_backlog_is_bounded_drop_oldest
  - id: 0012-T6
    title: Strictly enforce client to server Receive Maximum (DISCONNECT 0x93 on overrun)
    status: deferred
    notes: client to server direction is advertised but NOT strictly enforced; broker acks inbound promptly so it self-limits, DISCONNECT 0x93 folded into act-on-v5-reason-codes work (ADR 0012 §3); still holds
---

# Delivery — ADR 0012: MQTT 5.0 flow control (Receive Maximum)

Decision: [docs/adr/0012-flow-control.md](../adr/0012-flow-control.md).

## Plan

The decision's four numbered parts plus its bounded-backlog and inbound-enforcement limits
decompose into these tasks. Each carries a stable id used by commits, tests, and the
dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0012-T1** Outbound quota | When `send_to_client` would deliver a QoS > 0 message and `pending.len()` already equals the client's Receive Maximum, the message goes to a per-session backlog instead of the wire; a later PUBACK/PUBCOMP drains the backlog up to the quota. QoS 0 is never throttled. |
| **0012-T2** Backlog durability | On detach of a persistent session, never-sent backlog entries are flushed to the durable offline queue and replay on reconnect; already-sent `pending` entries keep DUP-on-resume behaviour; a clean/expired session drops its backlog. |
| **0012-T3** Advertise inbound | CONNACK advertises `SERVER_RECEIVE_MAXIMUM`; the client's CONNECT Receive Maximum is captured (default 65535, v3.1.1 effectively unlimited). |
| **0012-T4** Count publishes | The in-flight count is `Inflight.pending.len()` — one unacked QoS > 0 PUBLISH each, held PUBLISH→PUBACK (QoS1) / PUBLISH→PUBCOMP (QoS2) — not packet ids. |
| **0012-T5** Bounded backlog | The online backlog is bounded by `MAX_BACKLOG` with drop-oldest overflow (mirroring the offline queue), so a stalled QoS > 0 consumer cannot force unbounded memory. |
| **0012-T6** Strict inbound enforcement | Detect a client that overruns the advertised server Receive Maximum and answer with DISCONNECT reason `0x93`. |

## Progress

<!-- status-table:0012 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0012-T1 | ✅ done | 2026-06-17 | push_backlog/drain_backlog (hub.rs); v5_receive_maximum_limits_inflight_until_acked |
| 0012-T2 | ✅ done | 2026-06-17 | flush_backlog_to_store (hub.rs); quota_backlog_spills_to_store_on_persistent_detach |
| 0012-T3 | ✅ done | 2026-06-17 | SERVER_RECEIVE_MAXIMUM in conn.rs; v5_receive_maximum_is_advertised_and_forwarded / v311_receive_maximum_defaults_to_unlimited |
| 0012-T4 | ✅ done | 2026-06-17 | Inflight.pending.len() gates send (hub.rs); v5_receive_maximum_limits_inflight_until_acked |
| 0012-T5 | ✅ done | 2026-06-17 | MAX_BACKLOG / push_backlog; flow_control_backlog_is_bounded_drop_oldest |
| 0012-T6 | 💤 deferred | — | client to server direction is advertised but NOT strictly enforced; broker acks inbound promptly so it self-limits, DISCONNECT 0x93 folded into act-on-v5-reason-codes work (ADR 0012 §3); still holds |
<!-- /status-table:0012 -->

**Documented limit still in force:** the client→server (inbound) direction is advertised
via `SERVER_RECEIVE_MAXIMUM` but **not strictly enforced** — the broker acks inbound
QoS > 0 publishes promptly so it self-limits in normal operation, and detecting a
misbehaving client with DISCONNECT `0x93` is deferred (T6). Confirmed against `conn.rs`
and ADR 0012 §3.

## Changelog

- **2026-06-17** — Flow control landed: hub-enforced server→client quota with a per-session
  backlog (T1), persistent-session backlog spill to the durable queue (T2), advertised
  server Receive Maximum (T3), publish-count-based quota (T4), and a bounded drop-oldest
  backlog (T5). T6 (strict inbound enforcement + DISCONNECT `0x93`) deferred per the ADR's
  §3 documented limit.
