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
    title: "The store shards into K files — built, measured, and REJECTED as a default: throughput scales P(K)/K, so K=1 stands; the mechanism survives as an experimental pin, and the commit moves out of the state lock"
    status: done
    date: 2026-08-24
    evidence: "FALSIFICATION, not a feature. Built the sharded store end to end (ReplicaState over K redb files, group->shard by stable id, per-shard writers, schema-committed shard_count, fail-closed on a missing shard, shard-aware disk watermark + restore guard), then measured it against the same 48x8x48 release shape ADR 0075 used: K=1 25,270/24,884 msg/s, K=2 21,344 (0.85x), K=4 14,531 (0.58x). The mechanism is arithmetic: group commit turns in-flight work into batch DEPTH (throughput = D x barriers/s), sharding gives each shard D/K while the device multiplies barriers by only P(K), so sharded/single = P(K)/K -- and measured volumes give P(2)~1.7, P(4)~2.3. Predicted 0.85 / 0.58; measured 0.85 / 0.58. SHIPPED: K=1 by default with no first-boot calibration; MQTTD_STORE_SHARDS=<2..8> retained for a FRESH dir only, warned as experimental-and-slower, committed to schema for the store's life (an upgrade never reshards, and a single-file store stays single-file); the boot probe now measures the volume's parallel-barrier CURVE (1/2/4/8 streams) and applies the P(K)/K rule, so store_reshard_advice is a gauge whose silence is the finding; store_shards + statusz store.shards report the committed layout. KEPT REGARDLESS OF K: apply_batch_sharded decides under the state lock, fsyncs with it RELEASED, applies under it again -- safe because a shard's groups have exactly one writer -- so at K=1 every reader (recovery reads, /statusz, the catch-up sweep) stops queueing behind an 11ms fsync. TESTS: a_sharded_store_round_trips_every_key_and_survives_reopen, an_existing_single_file_store_is_never_resharded, a_missing_shard_fails_the_open_closed, a_key_lives_in_exactly_one_shard, the_unlocked_shard_writer_matches_apply_batch, the_parallel_barrier_curve_covers_every_stream_count, sharding_pays_only_when_parallel_streams_are_nearly_independent, plus the persistent-restart test now runs SHARDED (K writers, K locks to release)."
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
| 0076-T2 | ✅ done | 2026-08-24 | "FALSIFICATION, not a feature. Built the sharded store end to end (ReplicaState over K redb files, group->shard by stable id, per-shard writers, schema-committed shard_count, fail-closed on a missing shard, shard-aware disk watermark + restore guard), then measured it against the same 48x8x48 release shape ADR 0075 used: K=1 25,270/24,884 msg/s, K=2 21,344 (0.85x), K=4 14,531 (0.58x). The mechanism is arithmetic: group commit turns in-flight work into batch DEPTH (throughput = D x barriers/s), sharding gives each shard D/K while the device multiplies barriers by only P(K), so sharded/single = P(K)/K -- and measured volumes give P(2)~1.7, P(4)~2.3. Predicted 0.85 / 0.58; measured 0.85 / 0.58. SHIPPED: K=1 by default with no first-boot calibration; MQTTD_STORE_SHARDS=<2..8> retained for a FRESH dir only, warned as experimental-and-slower, committed to schema for the store's life (an upgrade never reshards, and a single-file store stays single-file); the boot probe now measures the volume's parallel-barrier CURVE (1/2/4/8 streams) and applies the P(K)/K rule, so store_reshard_advice is a gauge whose silence is the finding; store_shards + statusz store.shards report the committed layout. KEPT REGARDLESS OF K: apply_batch_sharded decides under the state lock, fsyncs with it RELEASED, applies under it again -- safe because a shard's groups have exactly one writer -- so at K=1 every reader (recovery reads, /statusz, the catch-up sweep) stops queueing behind an 11ms fsync. TESTS: a_sharded_store_round_trips_every_key_and_survives_reopen, an_existing_single_file_store_is_never_resharded, a_missing_shard_fails_the_open_closed, a_key_lives_in_exactly_one_shard, the_unlocked_shard_writer_matches_apply_batch, the_parallel_barrier_curve_covers_every_stream_count, sharding_pays_only_when_parallel_streams_are_nearly_independent, plus the persistent-restart test now runs SHARDED (K writers, K locks to release)." |
| 0076-T3 | ⬜ planned | — |  |
<!-- /status-table:0076 -->

## Changelog

- 2026-08-24 — ADR accepted on the issue #403 probe evidence (the curve hosts'
  device serves 3.7× more barriers at 8 streams than 1; one redb store reaches
  24k appends/s at 32-way concurrency; draw variance is large and real). T1
  shipped: measurement and exposure only, adaptation deliberately later.
- 2026-08-24 — **T2 built, measured, and rejected as a default.** Sharding
  divides the group-commit batch depth by K to buy `P(K)` more barriers, so
  throughput scales `P(K)/K` — and real volumes give `P(2)≈1.7`, `P(4)≈2.3`.
  Measured 0.85× at K=2 and 0.58× at K=4, matching the prediction. K=1 stands;
  the mechanism survives behind `MQTTD_STORE_SHARDS` so the finding stays
  falsifiable on hardware with independent per-file queues. The lock-free
  commit (decide locked → fsync unlocked → apply locked) ships at every K.
  Adaptation now points at T3, which raises batch depth instead of dividing it.
