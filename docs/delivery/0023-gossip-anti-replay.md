---
adr: "0023"
title: "Gossip anti-replay: persisted monotonic sequence + sliding window"
adr_status: Accepted
tasks:
  - id: 0023-P1
    title: Sliding replay window (RFC 6479 bitmap) — pure, accept/reject by sequence
    status: done
    date: 2026-06-22
    evidence: replay.rs ReplayWindow; an_exact_duplicate_is_rejected; out_of_order_within_the_window_is_accepted_once_then_rejected; a_sequence_below_the_window_is_rejected; a_large_forward_gap_slides_the_window_and_accepts
  - id: 0023-P2
    title: Persisted monotonic sequence allocator (block reservation + fsync; resumes above last block on restart)
    status: done
    date: 2026-06-22
    evidence: replay.rs SequenceAllocator/SeqStore; reserves_one_block_per_block_of_numbers; reopening_resumes_above_the_last_reserved_block_never_reusing
  - id: 0023-P3
    title: Wire format v3 in swim_auth (seq + signature; v1/v2 still understood; require/prefer/off)
    status: done
    date: 2026-06-22
    evidence: swim_auth.rs seal_sequenced/parse_v3/with_sequencing; Opened.seq; sequenced_seal_open_roundtrips_with_seq_and_identity; v3_body_framing_is_pinned; require_sequenced_rejects_v1_and_v2_but_accepts_v3; tampering_any_v3_byte_is_rejected_by_the_hmac
  - id: 0023-P4
    title: Driver integration — per-sender windows keyed by the authenticated CN; reject replays
    status: done
    date: 2026-06-22
    evidence: swim_driver per-sender ReplayWindows + SeqAlloc; sequenced sends + inbound replay drop; proven by a_replayed_v3_datagram_is_dropped
  - id: 0023-P5
    title: mqttd wiring — MQTTD_SWIM_REPLAY require/prefer/off, data-dir + signed require guards
    status: done
    date: 2026-06-22
    evidence: main.rs FileSeqStore (fsync'd <dir>/gossip-seq, fail-stop on persist error) + apply_anti_replay; MQTTD_SWIM_REPLAY require/prefer/off with signed + data-dir guards
  - id: 0023-P6
    title: Over-UDP integration test — a replayed datagram is rejected; live traffic flows; prefer accepts v2
    status: done
    date: 2026-06-22
    evidence: swim_cluster.rs sequenced_nodes_converge; a_replayed_v3_datagram_is_dropped (replay yields no second Ack over real UDP)
---

# Delivery — ADR 0023: Gossip anti-replay

Decision: [docs/adr/0023-gossip-anti-replay.md](../adr/0023-gossip-anti-replay.md).

Strict, clock-free, restart-safe replay rejection layered on ADR 0022 signing. Each phase
lands test-first; the two pure cores (window + allocator) are exhaustively unit-tested before
any wire/IO work builds on them.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0023-P1** Replay window | A pure sliding window: `check_and_set(seq)` accepts a new high (sliding the bitmap), accepts an in-window unseen seq, and rejects a duplicate or an at/below-low-edge seq. Exhaustive tests incl. large gaps and out-of-order arrival. |
| **0023-P2** Seq allocator | A persisted allocator hands out strictly increasing u64s; it reserves a block (one fsync) and, on reopen, resumes **above** the last reserved block — never reusing a number across restarts. Tested incl. the reopen-after-block case. |
| **0023-P3** Wire v3 | `swim_auth` seals v3 `[3][HMAC][seq][cert][sig][payload]` when sequencing is on; `open` returns the seq alongside the authenticated identity; v1/v2 still open; require rejects them; a KAT pins the v3 layout. |
| **0023-P4** Driver | The driver keeps a per-sender window keyed by the **authenticated** CN and drops a datagram whose seq is a replay; first datagram per sender seeds the window. |
| **0023-P5** Wiring | `MQTTD_SWIM_REPLAY` = require/prefer/off; `require` implies signed `require` and a writable data dir, else a startup error; transitional modes loudly logged. |
| **0023-P6** Integration | Over real UDP: a captured datagram replayed to a peer is dropped while live gossip converges; `prefer` still accepts a v2 (un-sequenced) peer mid-rollout. |

## Progress

<!-- status-table:0023 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0023-P1 | ✅ done | 2026-06-22 | replay.rs ReplayWindow; an_exact_duplicate_is_rejected; out_of_order_within_the_window_is_accepted_once_then_rejected; a_sequence_below_the_window_is_rejected; a_large_forward_gap_slides_the_window_and_accepts |
| 0023-P2 | ✅ done | 2026-06-22 | replay.rs SequenceAllocator/SeqStore; reserves_one_block_per_block_of_numbers; reopening_resumes_above_the_last_reserved_block_never_reusing |
| 0023-P3 | ✅ done | 2026-06-22 | swim_auth.rs seal_sequenced/parse_v3/with_sequencing; Opened.seq; sequenced_seal_open_roundtrips_with_seq_and_identity; v3_body_framing_is_pinned; require_sequenced_rejects_v1_and_v2_but_accepts_v3; tampering_any_v3_byte_is_rejected_by_the_hmac |
| 0023-P4 | ✅ done | 2026-06-22 | swim_driver per-sender ReplayWindows + SeqAlloc; sequenced sends + inbound replay drop; proven by a_replayed_v3_datagram_is_dropped |
| 0023-P5 | ✅ done | 2026-06-22 | main.rs FileSeqStore (fsync'd <dir>/gossip-seq, fail-stop on persist error) + apply_anti_replay; MQTTD_SWIM_REPLAY require/prefer/off with signed + data-dir guards |
| 0023-P6 | ✅ done | 2026-06-22 | swim_cluster.rs sequenced_nodes_converge; a_replayed_v3_datagram_is_dropped (replay yields no second Ack over real UDP) |
<!-- /status-table:0023 -->

## Changelog

- **2026-06-22** — P1–P6 landed, test-first: the pure replay window + persisted allocator;
  wire format v3 in `swim_auth`; the driver's per-sender windowing keyed by the authenticated
  CN; the `mqttd` wiring (`FileSeqStore` + `MQTTD_SWIM_REPLAY` require/prefer/off with the
  signed-and-data-dir guards); and the over-UDP proof that a replayed datagram is dropped
  while sequenced nodes converge. Clock-free and restart-safe by construction.
- **2026-06-22** — ADR accepted; phased plan recorded. Realizes `0003-T7` with a clock-free,
  restart-safe design (persisted monotonic sequence + sliding window, bound to ADR 0022's
  authenticated identity).
