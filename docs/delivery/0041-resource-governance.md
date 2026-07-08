---
adr: "0041"
title: Resource governance (admission caps, per-client quotas, bounded state)
adr_status: Proposed
tasks:
  - id: 0041-T1
    title: Admission caps — global + per-IP connection limits enforced at accept (pre-TLS close), bounded per-IP accounting, metrics; env-tunable with generous defaults
    status: planned
  - id: 0041-T2
    title: Auth-failure pushback — per-source-IP decaying penalty box; penalized addresses closed at accept before any Argon2 work; audited + counted; bounded table
    status: planned
  - id: 0041-T3
    title: Per-client quotas — max subscriptions per client (per-filter 0x97/0x80 SUBACK), publish-rate token bucket with read-pause throttling, inbound QoS 1 Receive Maximum enforcement (closes the ADR 0012 §3 deferral)
    status: planned
  - id: 0041-T4
    title: Global state caps — retained-topic cap (growth refused, maintenance always allowed), max-sessions cap (resume always allowed), MQTT 5 Maximum Packet Size negotiated from the transport cap, QueueLimits env wiring
    status: planned
  - id: 0041-T5
    title: Disk watermark + closure — per-store size gauges, soft high-water brownout (growth writes refused, maintenance continues), uniform fail-closed disk-full paths, README config table, ADR acceptance
    status: planned
---

# Delivery — ADR 0041: Resource governance

Decision: [docs/adr/0041-resource-governance.md](../adr/0041-resource-governance.md).

Pre-release area ③ (see ADR 0038's changelog for the four-area plan). The broker bounds
what one frame or session object can cost but not what a client can have *many* of:
connections, auth attempts, subscriptions, publish rate, retained topics, sessions, and
disk are all ungoverned. This delivery caps each at its cheapest enforcement point, with
spec-shaped at-bound behavior (reason codes, TCP backpressure), generous env-tunable
defaults, and a metric per cap.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0041-T1** Admission caps | With `MQTTD_MAX_CONNECTIONS=N`: the N+1-th concurrent connection is closed at accept **before any TLS handshake work**, counted (`admission_rejected_total{reason="max-connections"}`), and a slot freed by a disconnect is reusable. Same per source IP via `MQTTD_MAX_CONNECTIONS_PER_IP`; the per-IP table is bounded. Unset = today's behavior. |
| **0041-T2** Auth pushback | After K failed CONNECTs from one address, the next connection from it is closed at accept (no auth work) while a *different* address still authenticates normally; the penalty decays (a later attempt succeeds); audited (`security.penalty`) and counted. The table never grows past its bound under an address-spraying attack. |
| **0041-T3** Client quotas | A SUBSCRIBE filter beyond `MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT` gets `0x97` (v5) / `0x80` (v3.1.1) in its SUBACK slot while in-cap filters in the same packet are granted; a publisher exceeding `MQTTD_MAX_PUBLISH_RATE` is slowed to the configured rate by read-pause (no drops, no disconnect, session stays healthy) while a second client is unaffected; a v5 QoS 1 overrun of the advertised Receive Maximum gets `DISCONNECT 0x93` (finishing ADR 0012 §3). |
| **0041-T4** State caps | A retained publish creating a new topic beyond `MQTTD_MAX_RETAINED_MESSAGES` is refused (v5 `0x97`; v3.1.1 delivered-not-retained, counted) while overwrite/clear of existing topics still works at the cap; a CONNECT creating a new session beyond `MQTTD_MAX_SESSIONS` is refused (v5 `0x97`, v3.1.1 `0x03`) while a resume succeeds at the cap; the CONNACK advertises `MQTTD_MAX_PACKET_SIZE` as Maximum Packet Size and an outbound message larger than the *client's* advertised maximum is dropped for that subscriber only, counted; `MQTTD_MAX_QUEUED_MESSAGES` wires `QueueLimits`. |
| **0041-T5** Disk + closure | Per-store redb size gauges exported; with `MQTTD_STORE_MAX_BYTES` set and exceeded, growth writes are refused with the T4 behaviors while acks/deletes/expiry/resume continue (proven by test), and recovery below the mark restores writes; the cross-node offline enqueue failure path is fail-closed like the local ack path; README documents every `MQTTD_*` cap; ADR 0041 flips to Accepted. |

## Progress

<!-- status-table:0041 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0041-T1 | ⬜ planned | — |  |
| 0041-T2 | ⬜ planned | — |  |
| 0041-T3 | ⬜ planned | — |  |
| 0041-T4 | ⬜ planned | — |  |
| 0041-T5 | ⬜ planned | — |  |
<!-- /status-table:0041 -->

## Changelog

- **2026-07-05** — ADR proposed and delivery opened. Scope fixed by a bounds survey:
  per-frame/per-session costs are bounded and tested (read buffer 1 MiB, peer frame
  16 MiB, backlog 10 000 drop-oldest, offline queue 100 000 drop-oldest, alias table,
  retained mutation queue, connect/auth timeouts), but everything a client can have
  *many* of is not — connections (no global or per-IP cap; `SERVER_BUSY`/`QUOTA_EXCEEDED`
  defined but never emitted), auth attempts (no rate limit despite Argon2 per-attempt
  cost), subscriptions (unbounded per client and per SUBSCRIBE packet), publish rate,
  retained topics, total sessions, and disk (no size visibility; inconsistent disk-full
  handling). Also recorded: the offline-queue cap exists but has no operator wiring, and
  the 1 MiB frame cap is a placeholder awaiting MQTT 5 Maximum Packet Size negotiation
  (both paid off here). Ordering: T1/T2 (admission plane, independent) → T3/T4 (protocol
  planes) → T5 (disk + closure).
