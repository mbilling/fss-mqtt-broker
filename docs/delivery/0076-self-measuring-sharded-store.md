---
adr: "0076"
title: "The self-measuring sharded store — the volume's capacity becomes the broker's business"
adr_status: Accepted
tasks:
  - id: 0076-T1
    title: "Self-measurement, exposed: the boot-time barrier probe and the writer's passive commit telemetry, on /metrics and /statusz"
    status: done
    date: 2026-08-24
    evidence: "store_probe.rs: a tiny (tens of fsyncs, sub-second) boot probe of the data-dir volume — single-writer barriers/s plus the 4-stream aggregate (the sharding-headroom signal) — run on the blocking pool 2s after start on durable nodes only, scratch removed, results into gauges store_barrier_floor / store_barrier_floor_4stream and the /statusz store block (probed:false until it lands). Passive half: WriterStats gains commit_nanos (measured around every fsync'd apply_batch, lock wait included); the ADR 0071 poller exports durable_writer_commit_micros so rate(commit_micros)/rate(batches) is the LIVE mean barrier latency per epoch from real traffic; /statusz store.writer reports batches/ops/mean_batch_x100/max_batch/commit_ms_mean. TESTS: probes_a_volume_and_cleans_up (real rate measured, 4-stream sanity bound, scratch removed), statusz_reports_the_store_self_measurement (block renders both before and after the probe; mean batch arithmetic pinned). SIZING.md gains the interpretation note. No behavior changes — measurement only, per the ADR's evidence-before-adaptation rule."
  - id: 0076-T2
    title: "The store shards into K files, K calibrated at first boot from the probe, committed in schema metadata; reshard advisor, never silent migration"
    status: planned
  - id: 0076-T3
    title: "Epoch-adaptive coalescing: a per-shard linger derived from the measured commit time, engaged only under saturation, pinnable off"
    status: planned
---

# Delivery: ADR 0076 — The self-measuring sharded store

[ADR 0076](../adr/0076-self-measuring-sharded-store.md) · tasks and status in
the frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0076 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0076-T1 | ✅ done | 2026-08-24 | "store_probe.rs: a tiny (tens of fsyncs, sub-second) boot probe of the data-dir volume — single-writer barriers/s plus the 4-stream aggregate (the sharding-headroom signal) — run on the blocking pool 2s after start on durable nodes only, scratch removed, results into gauges store_barrier_floor / store_barrier_floor_4stream and the /statusz store block (probed:false until it lands). Passive half: WriterStats gains commit_nanos (measured around every fsync'd apply_batch, lock wait included); the ADR 0071 poller exports durable_writer_commit_micros so rate(commit_micros)/rate(batches) is the LIVE mean barrier latency per epoch from real traffic; /statusz store.writer reports batches/ops/mean_batch_x100/max_batch/commit_ms_mean. TESTS: probes_a_volume_and_cleans_up (real rate measured, 4-stream sanity bound, scratch removed), statusz_reports_the_store_self_measurement (block renders both before and after the probe; mean batch arithmetic pinned). SIZING.md gains the interpretation note. No behavior changes — measurement only, per the ADR's evidence-before-adaptation rule." |
| 0076-T2 | ⬜ planned | — |  |
| 0076-T3 | ⬜ planned | — |  |
<!-- /status-table:0076 -->

## Changelog

- 2026-08-24 — ADR accepted on the issue #403 probe evidence (the curve hosts'
  device serves 3.7× more barriers at 8 streams than 1; one redb store reaches
  24k appends/s at 32-way concurrency; draw variance is large and real). T1
  shipped: measurement and exposure only, adaptation deliberately later.
