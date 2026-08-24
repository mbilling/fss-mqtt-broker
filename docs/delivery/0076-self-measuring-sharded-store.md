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
    title: "Epoch-adaptive coalescing — built, measured, and REJECTED for the same reason as T2: the group-commit batch already self-balances at arrival-rate x commit-time, so there is nothing left for a linger to gather"
    status: done
    date: 2026-08-24
    evidence: "FALSIFICATION, like T2. The writer gained a per-shard linger derived from an EWMA of its own measured commit time, engaged only on a multi-op batch (never at rest, never before a commit has been measured). Measured at the ADR 0075 shape: linger off 24,488 msg/s p50 15.3ms; 0.25 of a commit 19,591 (0.80x) p50 18.8ms; 0.5 of a commit 21,303 (0.87x) p50 17.8ms -- every setting worse in BOTH throughput and latency. The reason is the same property that killed T2: the writer already coalesces every op that arrives during a commit, so depth self-balances at D = arrival rate x commit time and throughput D/commit is exactly the arrival rate. ADR 0075 did not leave headroom for a smarter batching policy, it removed the headroom by making the batch self-balancing -- dividing it (T2) loses K/P(K), delaying it (T3) loses the wait. SHIPPED: the linger implemented and OFF by default (MQTTD_STORE_LINGER=<0.0..1.0>, warned loudly when engaged, one f64 compare per batch when off), retained so the finding stays falsifiable on burstier arrivals than a saturating benchmark's. TEST: the_linger_is_off_unless_asked_for_and_rejects_nonsense."
---

# Delivery: ADR 0076 — The self-measuring sharded store

[ADR 0076](../adr/0076-self-measuring-sharded-store.md) · tasks and status in
the frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0076 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0076-T1 | ✅ done | 2026-08-24 | "store_probe.rs: a tiny (tens of fsyncs, sub-second) boot probe of the data-dir volume — single-writer barriers/s plus the 4-stream aggregate (the sharding-headroom signal) — run on the blocking pool 2s after start on durable nodes only, scratch removed, results into gauges store_barrier_floor / store_barrier_floor_4stream and the /statusz store block (probed:false until it lands). Passive half: WriterStats gains commit_nanos (measured around every fsync'd apply_batch, lock wait included); the ADR 0071 poller exports durable_writer_commit_micros so rate(commit_micros)/rate(batches) is the LIVE mean barrier latency per epoch from real traffic; /statusz store.writer reports batches/ops/mean_batch_x100/max_batch/commit_ms_mean. TESTS: probes_a_volume_and_cleans_up (real rate measured, 4-stream sanity bound, scratch removed), statusz_reports_the_store_self_measurement (block renders both before and after the probe; mean batch arithmetic pinned). SIZING.md gains the interpretation note. No behavior changes — measurement only, per the ADR's evidence-before-adaptation rule." |
| 0076-T2 | ✅ done | 2026-08-24 | "FALSIFICATION, not a feature. Built the sharded store end to end (ReplicaState over K redb files, group->shard by stable id, per-shard writers, schema-committed shard_count, fail-closed on a missing shard, shard-aware disk watermark + restore guard), then measured it against the same 48x8x48 release shape ADR 0075 used: K=1 25,270/24,884 msg/s, K=2 21,344 (0.85x), K=4 14,531 (0.58x). The mechanism is arithmetic: group commit turns in-flight work into batch DEPTH (throughput = D x barriers/s), sharding gives each shard D/K while the device multiplies barriers by only P(K), so sharded/single = P(K)/K -- and measured volumes give P(2)~1.7, P(4)~2.3. Predicted 0.85 / 0.58; measured 0.85 / 0.58. SHIPPED: K=1 by default with no first-boot calibration; MQTTD_STORE_SHARDS=<2..8> retained for a FRESH dir only, warned as experimental-and-slower, committed to schema for the store's life (an upgrade never reshards, and a single-file store stays single-file); the boot probe now measures the volume's parallel-barrier CURVE (1/2/4/8 streams) and applies the P(K)/K rule, so store_reshard_advice is a gauge whose silence is the finding; store_shards + statusz store.shards report the committed layout. KEPT REGARDLESS OF K: apply_batch_sharded decides under the state lock, fsyncs with it RELEASED, applies under it again -- safe because a shard's groups have exactly one writer -- so at K=1 every reader (recovery reads, /statusz, the catch-up sweep) stops queueing behind an 11ms fsync. TESTS: a_sharded_store_round_trips_every_key_and_survives_reopen, an_existing_single_file_store_is_never_resharded, a_missing_shard_fails_the_open_closed, a_key_lives_in_exactly_one_shard, the_unlocked_shard_writer_matches_apply_batch, the_parallel_barrier_curve_covers_every_stream_count, sharding_pays_only_when_parallel_streams_are_nearly_independent, plus the persistent-restart test now runs SHARDED (K writers, K locks to release)." |
| 0076-T3 | ✅ done | 2026-08-24 | "FALSIFICATION, like T2. The writer gained a per-shard linger derived from an EWMA of its own measured commit time, engaged only on a multi-op batch (never at rest, never before a commit has been measured). Measured at the ADR 0075 shape: linger off 24,488 msg/s p50 15.3ms; 0.25 of a commit 19,591 (0.80x) p50 18.8ms; 0.5 of a commit 21,303 (0.87x) p50 17.8ms -- every setting worse in BOTH throughput and latency. The reason is the same property that killed T2: the writer already coalesces every op that arrives during a commit, so depth self-balances at D = arrival rate x commit time and throughput D/commit is exactly the arrival rate. ADR 0075 did not leave headroom for a smarter batching policy, it removed the headroom by making the batch self-balancing -- dividing it (T2) loses K/P(K), delaying it (T3) loses the wait. SHIPPED: the linger implemented and OFF by default (MQTTD_STORE_LINGER=<0.0..1.0>, warned loudly when engaged, one f64 compare per batch when off), retained so the finding stays falsifiable on burstier arrivals than a saturating benchmark's. TEST: the_linger_is_off_unless_asked_for_and_rejects_nonsense." |
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
- 2026-08-24 — **T3 built, measured, and rejected too** — and the two
  rejections are one finding. The linger measured 0.80× and 0.87× of baseline
  (worse in latency as well), because the writer already gathers every op that
  arrives during a commit: depth self-balances at `arrival rate × commit
  time`, so throughput is already the arrival rate. ADR 0075's group commit did
  not leave room for a better batching policy — it removed the room. **ADR 0076
  is complete: T1 measures and publishes, T2 and T3 are falsified with the
  mechanisms retained off-by-default.** The durable path's next lead is not in
  the writer: it is the ~140k fan-out knee with idle CPU (issue #258).
