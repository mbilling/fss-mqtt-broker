---
adr: "0074"
title: "The subscriber-ack truncate leaves the hub loop's critical path"
adr_status: Accepted
tasks:
  - id: 0074-T1
    title: The coalesced, detached QoS 1 ack truncate — per-session watermarks flushed off-loop by the hub-owned truncate flusher; QoS 2 completion keeps the inline truncate; failure tolerance verbatim
    status: done
    date: 2026-08-23
    evidence: "truncate_acked is now synchronous bookkeeping (advance the Inflight watermark, send (client, up_to) to the flusher); run_truncate_flusher (owned by the hub's JoinSet, dies with it) merges watermarks max-wins per session and flushes with bounded concurrency 8, keeping the documented not-fatal tolerance (entries replay at next resume; a QoS 1 duplicate is spec-legal). pub_comp keeps truncate_acked_now inline (ADR 0074 Decision 2 — the QoS 2 clear_outbound/truncate crash window stays exactly today's width). TESTS: a_subscriber_ack_completes_while_its_truncate_is_still_parked (store.ack parked via the new ParkingStore park_ack gate: a second publish still flows end to end — RED under the old inline await, which parked the whole loop — and the released flusher truncates the acked prefix) and a_burst_of_acks_coalesces_into_one_watermark_truncate (five acks reach the store as at most two truncates, the last covering the full prefix — the O(sessions)-not-O(messages) property). Full hub lib (337), inflight_durability (11), persistence (3), durable_sessions suites green. MEASURED A/B (same machine, same shape, release, 1 node, 48x8x48): qos1-durable 192 -> 4,379 msg/s (22.8x, p99 2075 -> 233 ms), qos1-relay 202 -> 4,165, qos2 (deliberately still inline) 159 -> 751, clean control unchanged (~70k). Durable msg/s decoupled from the barrier rate exactly as the ADR predicted: from ~2x the macOS barrier rate to ~44x — the ADR 0071 writer finally fed."
  - id: 0074-T2
    title: "The curve evidence: durable rows re-measured on dedicated hardware (the barrier-rate pinning at 1.3x should break; the slow-disk-draw sensitivity should collapse), published in SCALE-CURVE.md"
    status: planned
    notes: "Rides the next paid curve run (v1.0.4). The falsifier is explicit in the ADR: if durable msg/s stays pinned to the slowest disk's barrier rate, the ADR is wrong and says so."
---

# Delivery: ADR 0074 — Detached ack truncate

[ADR 0074](../adr/0074-detached-ack-truncate.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0074 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0074-T1 | ✅ done | 2026-08-23 | "truncate_acked is now synchronous bookkeeping (advance the Inflight watermark, send (client, up_to) to the flusher); run_truncate_flusher (owned by the hub's JoinSet, dies with it) merges watermarks max-wins per session and flushes with bounded concurrency 8, keeping the documented not-fatal tolerance (entries replay at next resume; a QoS 1 duplicate is spec-legal). pub_comp keeps truncate_acked_now inline (ADR 0074 Decision 2 — the QoS 2 clear_outbound/truncate crash window stays exactly today's width). TESTS: a_subscriber_ack_completes_while_its_truncate_is_still_parked (store.ack parked via the new ParkingStore park_ack gate: a second publish still flows end to end — RED under the old inline await, which parked the whole loop — and the released flusher truncates the acked prefix) and a_burst_of_acks_coalesces_into_one_watermark_truncate (five acks reach the store as at most two truncates, the last covering the full prefix — the O(sessions)-not-O(messages) property). Full hub lib (337), inflight_durability (11), persistence (3), durable_sessions suites green. MEASURED A/B (same machine, same shape, release, 1 node, 48x8x48): qos1-durable 192 -> 4,379 msg/s (22.8x, p99 2075 -> 233 ms), qos1-relay 202 -> 4,165, qos2 (deliberately still inline) 159 -> 751, clean control unchanged (~70k). Durable msg/s decoupled from the barrier rate exactly as the ADR predicted: from ~2x the macOS barrier rate to ~44x — the ADR 0071 writer finally fed." |
| 0074-T2 | ⬜ planned | — | "Rides the next paid curve run (v1.0.4). The falsifier is explicit in the ADR: if durable msg/s stays pinned to the slowest disk's barrier rate, the ADR is wrong and says so." |
<!-- /status-table:0074 -->

## Changelog

- 2026-08-23 — ADR accepted and T1 shipped in one motion: the mechanism was
  fully diagnosed on the v1.0.3 curve (two hardware draws pinning durable
  throughput to ~1.3× the slowest disk's barrier rate while the 0071 writer
  batched only 2.29 ops), so the decision, the fix, the parked-store proofs,
  and the 22.8× local A/B landed together. T2 (the dedicated-hardware curve
  rows) awaits the next paid run.
