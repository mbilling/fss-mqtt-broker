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
    status: done
    date: 2026-08-23
    evidence: "The falsifier PASSED on the v1.0.4 Hetzner curve (runs 20260823T083536Z + the LANES=A 5-node rerun 20260823T143528Z, published in SCALE-CURVE.md). Durable QoS1: 1-node 11,833 [10,957..12,005] msg/s (v1.0.3: 3,048 -> 3.9x), 3-node 10,393 [10,325..10,398] (v1.0.3: 867 on a 706-barriers/s draw; v1.0.2: 1,753 -> 5.9x), 5-node 16,378 [16,333..16,679] — the first successful 5-node durable measurement ever, and the first size where the durable path scales OUT past one machine (1.38x a single node). The pinning is gone exactly as predicted: rows sit at 5.0x / 4.7x / 8.0x of the slowest member's barrier rate (previously 0.8-1.35x across four draws), so durable throughput no longer inherits the NVMe draw. Quorum tax at 3 nodes collapsed 72% -> 12%; saturating p99 140 -> 65 ms (1n) and 469 -> 84 ms (3n); QoS2 2.6x/2.7x (kept-inline truncate, ADR Decision 2, moving with the freed hub loop). Getting the 5-node cell took seven formations and produced issues #393/#396 (SWIM 0.0.0.0 advertise dissemination; rig now binds the private IP) — disclosed in the doc."
---

# Delivery: ADR 0074 — Detached ack truncate

[ADR 0074](../adr/0074-detached-ack-truncate.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0074 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0074-T1 | ✅ done | 2026-08-23 | "truncate_acked is now synchronous bookkeeping (advance the Inflight watermark, send (client, up_to) to the flusher); run_truncate_flusher (owned by the hub's JoinSet, dies with it) merges watermarks max-wins per session and flushes with bounded concurrency 8, keeping the documented not-fatal tolerance (entries replay at next resume; a QoS 1 duplicate is spec-legal). pub_comp keeps truncate_acked_now inline (ADR 0074 Decision 2 — the QoS 2 clear_outbound/truncate crash window stays exactly today's width). TESTS: a_subscriber_ack_completes_while_its_truncate_is_still_parked (store.ack parked via the new ParkingStore park_ack gate: a second publish still flows end to end — RED under the old inline await, which parked the whole loop — and the released flusher truncates the acked prefix) and a_burst_of_acks_coalesces_into_one_watermark_truncate (five acks reach the store as at most two truncates, the last covering the full prefix — the O(sessions)-not-O(messages) property). Full hub lib (337), inflight_durability (11), persistence (3), durable_sessions suites green. MEASURED A/B (same machine, same shape, release, 1 node, 48x8x48): qos1-durable 192 -> 4,379 msg/s (22.8x, p99 2075 -> 233 ms), qos1-relay 202 -> 4,165, qos2 (deliberately still inline) 159 -> 751, clean control unchanged (~70k). Durable msg/s decoupled from the barrier rate exactly as the ADR predicted: from ~2x the macOS barrier rate to ~44x — the ADR 0071 writer finally fed." |
| 0074-T2 | ✅ done | 2026-08-23 | "The falsifier PASSED on the v1.0.4 Hetzner curve (runs 20260823T083536Z + the LANES=A 5-node rerun 20260823T143528Z, published in SCALE-CURVE.md). Durable QoS1: 1-node 11,833 [10,957..12,005] msg/s (v1.0.3: 3,048 -> 3.9x), 3-node 10,393 [10,325..10,398] (v1.0.3: 867 on a 706-barriers/s draw; v1.0.2: 1,753 -> 5.9x), 5-node 16,378 [16,333..16,679] — the first successful 5-node durable measurement ever, and the first size where the durable path scales OUT past one machine (1.38x a single node). The pinning is gone exactly as predicted: rows sit at 5.0x / 4.7x / 8.0x of the slowest member's barrier rate (previously 0.8-1.35x across four draws), so durable throughput no longer inherits the NVMe draw. Quorum tax at 3 nodes collapsed 72% -> 12%; saturating p99 140 -> 65 ms (1n) and 469 -> 84 ms (3n); QoS2 2.6x/2.7x (kept-inline truncate, ADR Decision 2, moving with the freed hub loop). Getting the 5-node cell took seven formations and produced issues #393/#396 (SWIM 0.0.0.0 advertise dissemination; rig now binds the private IP) — disclosed in the doc." |
<!-- /status-table:0074 -->

## Changelog

- 2026-08-23 — ADR accepted and T1 shipped in one motion: the mechanism was
  fully diagnosed on the v1.0.3 curve (two hardware draws pinning durable
  throughput to ~1.3× the slowest disk's barrier rate while the 0071 writer
  batched only 2.29 ops), so the decision, the fix, the parked-store proofs,
  and the 22.8× local A/B landed together. T2 (the dedicated-hardware curve
  rows) awaits the next paid run.
- 2026-08-23 — T2 done: the v1.0.4 paid curve ran the falsifier and it
  passed — 3.9× (1 node), 12× (3 nodes, draw-adjusted 5.9× vs v1.0.2), and
  the first-ever 5-node durable point (16,378 msg/s, 1.38× a single node);
  barrier-rate pinning broken (rows at 4.7–8.0× the slowest disk's rate).
  Published in SCALE-CURVE.md with the #393/#396 formation saga disclosed.
