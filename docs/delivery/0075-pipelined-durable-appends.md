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
---

# Delivery: ADR 0075 — Pipelined durable appends

[ADR 0075](../adr/0075-pipelined-durable-appends.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0075 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0075-T1 | ✅ done | 2026-08-24 | "Three serializations removed in one seam-preserving motion. (1) conn.rs: QoS1 acks park in a per-connection FIFO (drain branch writes front-only; refusal semantics verbatim in apply_publish_outcome; ACL denials ride the queue pre-decided; flush-before-any-outbound keeps a message's own PUBACK observably ahead of its fan-out deliveries; Receive-Maximum slot held until the ack writes; hard cap 256 with inline backpressure; QoS2 stays inline — ADR 0074 Decision 2 conservatism). (2) lanes: append jobs submit in order and spawn their durability waits into a worker-owned JoinSet (depth 16, non-append jobs are barriers; already-resolved pendings complete inline via PendingEnqueue::try_ready — the eager/in-memory path keeps its old timing, which a 10k-publish replay test proved by regressing before the fix). (3) ClusterLog::submit_tiered: short-lock submit (offset = assigned+1, seq bump, writer send in offset order), lock-free quorum wait, IN-ORDER commit watermark (a success is only reported at/below the watermark), tail-fail with gap-free committed range and drain-reset offset reuse at higher seq (ADR 0042 T7 preserved). TESTS: pipelined_appends_overlap_and_commit_in_offset_order (reverse-order acks; nothing completes while offset 1 pends), a_failed_pipelined_append_fails_the_staged_tail_and_leaves_no_hole (offset 3's replication SUCCEEDS and still fails; retries reuse 2,3), an_earlier_pipelined_append_is_unaffected_by_a_later_failure; full mqtt-storage/mqtt-cluster/mqttd suites green. MEASURED (same machine, 48x8x48, release, 1 node): 4,119 -> 26,115 msg/s (6.3x), sat p50 85 -> 14.5 ms, ~113 ops/barrier on a ~230-barriers/s volume — beyond the ADR's 2x estimate because the conn-layer serializer (found when the first two fixes moved only +16%) multiplied it." |
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
