---
adr: "0075"
title: "Pipelined durable appends — the window stops paying the round trip per message"
adr_status: Accepted
tasks:
  - id: 0075-T1
    title: The two-phase append — ordered submit / parallel durability wait, in-order commit watermark, tail-fail; lanes and connections pipeline behind bounded depths
    status: done
    date: 2026-08-24
    evidence: "Three serializations removed in one seam-preserving motion. (1) conn.rs: QoS1 acks park in a per-connection FIFO (drain branch writes front-only; refusal semantics verbatim in apply_publish_outcome; ACL denials ride the queue pre-decided; flush-before-any-outbound keeps a message's own PUBACK observably ahead of its fan-out deliveries; Receive-Maximum slot held until the ack writes; hard cap 256 with inline backpressure; QoS2 stays inline — ADR 0074 Decision 2 conservatism). (2) lanes: append jobs submit in order and spawn their durability waits into a worker-owned JoinSet (depth 16, non-append jobs are barriers; already-resolved pendings complete inline via PendingEnqueue::try_ready — the eager/in-memory path keeps its old timing, which a 10k-publish replay test proved by regressing before the fix). (3) ClusterLog::submit_tiered: short-lock submit (offset = assigned+1, seq bump, writer send in offset order), lock-free quorum wait, IN-ORDER commit watermark (a success is only reported at/below the watermark), tail-fail with gap-free committed range and drain-reset offset reuse at higher seq (ADR 0042 T7 preserved). TESTS: pipelined_appends_overlap_and_commit_in_offset_order (reverse-order acks; nothing completes while offset 1 pends), a_failed_pipelined_append_fails_the_staged_tail_and_leaves_no_hole (offset 3's replication SUCCEEDS and still fails; retries reuse 2,3), an_earlier_pipelined_append_is_unaffected_by_a_later_failure; full mqtt-storage/mqtt-cluster/mqttd suites green. MEASURED (same machine, 48x8x48, release, 1 node): 4,119 -> 26,115 msg/s (6.3x), sat p50 85 -> 14.5 ms, ~113 ops/barrier on a ~230-barriers/s volume — beyond the ADR's 2x estimate because the conn-layer serializer (found when the first two fixes moved only +16%) multiplied it."
  - id: 0075-T2
    title: Amendment 1 — a bound on the writer's batch (1,024 ops or 4 MiB a commit), built, measured on h3 and withdrawn
    status: done
    issue: 1189
    date: 2026-11-03
    evidence: "Built in #1190 (closed unmerged): MAX_BATCH_OPS / MAX_BATCH_BYTES in durable_plane.rs, drain_bounded for both drains of a batch, the linger skipped at the bound, two tests. Measured on h3, run h3-20261103T130326Z, with ADR 0092 Amendment 4's worker default and ADR 0090 T8's drain order, four shards (ADR 0093's delivery doc, 'The combined build measured on h3'): every round from 153,600 up stuck in write mode, the bounded writer clearing about 102,000 ops/s against the unbounded one's 120,000–165,000, the knee down from 327,100 to between 138,300 and 153,600. Withdrawn by ADR 0075 Amendment 1: a session's topic sees a delivery about every 1.7 ms at 300,000 msg/s, so no bound is short enough, and a shallower batch lengthens write mode. The writer stays unbounded; the hold A5 imposes is ADR 0089's to shorten."
---

# Delivery: ADR 0075 — Pipelined durable appends

[ADR 0075](../adr/0075-pipelined-durable-appends.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0075 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0075-T1 | ✅ done | — | 2026-08-24 | "Three serializations removed in one seam-preserving motion. (1) conn.rs: QoS1 acks park in a per-connection FIFO (drain branch writes front-only; refusal semantics verbatim in apply_publish_outcome; ACL denials ride the queue pre-decided; flush-before-any-outbound keeps a message's own PUBACK observably ahead of its fan-out deliveries; Receive-Maximum slot held until the ack writes; hard cap 256 with inline backpressure; QoS2 stays inline — ADR 0074 Decision 2 conservatism). (2) lanes: append jobs submit in order and spawn their durability waits into a worker-owned JoinSet (depth 16, non-append jobs are barriers; already-resolved pendings complete inline via PendingEnqueue::try_ready — the eager/in-memory path keeps its old timing, which a 10k-publish replay test proved by regressing before the fix). (3) ClusterLog::submit_tiered: short-lock submit (offset = assigned+1, seq bump, writer send in offset order), lock-free quorum wait, IN-ORDER commit watermark (a success is only reported at/below the watermark), tail-fail with gap-free committed range and drain-reset offset reuse at higher seq (ADR 0042 T7 preserved). TESTS: pipelined_appends_overlap_and_commit_in_offset_order (reverse-order acks; nothing completes while offset 1 pends), a_failed_pipelined_append_fails_the_staged_tail_and_leaves_no_hole (offset 3's replication SUCCEEDS and still fails; retries reuse 2,3), an_earlier_pipelined_append_is_unaffected_by_a_later_failure; full mqtt-storage/mqtt-cluster/mqttd suites green. MEASURED (same machine, 48x8x48, release, 1 node): 4,119 -> 26,115 msg/s (6.3x), sat p50 85 -> 14.5 ms, ~113 ops/barrier on a ~230-barriers/s volume — beyond the ADR's 2x estimate because the conn-layer serializer (found when the first two fixes moved only +16%) multiplied it." |
| 0075-T2 | ✅ done | [#1189](https://github.com/mbilling/fss-mqtt-broker/issues/1189) | 2026-11-03 | "Built in #1190 (closed unmerged): MAX_BATCH_OPS / MAX_BATCH_BYTES in durable_plane.rs, drain_bounded for both drains of a batch, the linger skipped at the bound, two tests. Measured on h3, run h3-20261103T130326Z, with ADR 0092 Amendment 4's worker default and ADR 0090 T8's drain order, four shards (ADR 0093's delivery doc, 'The combined build measured on h3'): every round from 153,600 up stuck in write mode, the bounded writer clearing about 102,000 ops/s against the unbounded one's 120,000–165,000, the knee down from 327,100 to between 138,300 and 153,600. Withdrawn by ADR 0075 Amendment 1: a session's topic sees a delivery about every 1.7 ms at 300,000 msg/s, so no bound is short enough, and a shallower batch lengthens write mode. The writer stays unbounded; the hold A5 imposes is ADR 0089's to shorten." |
<!-- /status-table:0075 -->

## Changelog

- 2026-08-24 — ADR accepted on the issue #402 evidence (hardware: p50 45 ms vs
  4.7 ms appends at 25.5 ops/batch with the disk ~30% barrier-busy; local
  falsifier: 48×8 → 4,119 msg/s vs 384×1 → 7,917 at the same total
  in-flight). Implementation under way.
- 2026-08-24 — T1 done, and the estimate was beaten 3×: the first two fixes
  (lane + log) moved 48×8 only to 4,797 (+16%), which exposed the third and
  outermost serializer — conn.rs awaiting every QoS 1 durability round trip
  inline. With the per-connection ordered ack pipeline added, 48×8 measures
  26,115 msg/s at p50 14.5 ms (was 4,119 at 85 ms). One behavioral seam
  defended deterministically: resolved acks flush before any outbound write,
  so a message's own PUBACK still precedes the deliveries it caused (the
  acl.rs self-delivery test caught the race; the flush closed it).
- 2026-11-03 — Amendment 1: a bound on the writer's batch, proposed on ADR 0093's
  h3 runs (two rounds in write mode behind 550 ms commits), built in #1190 and
  measured the same day as a regression (run h3-20261103T130326Z: the knee from
  327,100 to between 138,300 and 153,600, every round above about 120,000 msg/s
  stuck in write mode). Withdrawn: the premise's 30 ms between two deliveries on
  a topic was a publisher's interval, the topic's is about 1.7 ms, and a
  shallower batch clears a backlog slower. The writer stays unbounded; T2 closed,
  #1189 closed, #1190 closed unmerged. The bistable path is ADR 0089's to close.
