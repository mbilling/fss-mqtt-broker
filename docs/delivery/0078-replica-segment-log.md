---
adr: "0078"
title: "The replica store becomes an append-only segment log — the disk is the limit, not the B-tree"
adr_status: Accepted
tasks:
  - id: 0078-T1
    title: "The segment log — format, one-flush batch commit, torn-tail recovery, segment roll"
    status: done
    issue: 659
    date: 2026-09-28
    evidence: "PR #663. crates/mqtt-cluster/src/segment_log.rs — preallocated segments of framed records (len, crc32c over kind+lsn+payload, kind, lsn, payload); a batch is one pwrite + one fdatasync; replay in LSN order stops at the first invalid record (CRC, LSN out of sequence, length out of bounds): a torn tail in the last segment, corruption (open fails) in any earlier one; recovery ZEROES the rest of the segment from the valid end so intact records of an unacked batch can never replay (the equal-size case an LSN check misses — mutation-checked: removing the zeroing fails two tests); a new segment is created, extended, flushed and its directory flushed before any record lands in it; an empty segment from a crashed roll is removed; drop_before deletes a prefix, never the active segment. CRC-32C in-house (check value 0xE3069283), no new dependency. 12 unit tests, incl. the zero-gap sector-reorder ghost (review finding). Standalone: T2 wires it into ReplicaState."
  - id: 0078-T2
    title: "ReplicaState on the segment log, behind MQTTD_REPLICA_STORE=redb|log"
    status: done
    issue: 660
    date: 2026-09-28
    evidence: "PR #665. One commit path (drive_plan) feeds a redb sink or a log sink, so the engines cannot drift; log records mirror the redb rows (Truncate carries up_to AND low_water). A differential test drives the same changes through every apply path on both backends (1 and 4 shards) and requires identical state live and after reopen — mutation-checked. One-way import from redb (staged, renamed into place; redb files kept as *.imported; an interrupted import is redone); redb refuses a log-format data dir. MQTTD_REPLICA_STORE wired in main.rs (default redb); store_watch counts the log dir; the restore guard treats it as store files. Default stays redb until T4. Review fix: the store counts as committed once shard 0 carries its format stamp, not once its directory exists (an unstamped shard 0 left by a crash is a fresh store; regression test, mutation-checked)."
  - id: 0078-T3
    title: "Space reclamation — segment drop and bounded compaction"
    status: in-progress
    issue: 661
    notes: "In review. LogShard tracks where each live entry's record sits and per-segment live counts (decoded from the records it just wrote — the replay's own decoder); drops a dead PREFIX of segments; restates format, fences, caught-up sets and low-waters (a new LowWater record that deletes nothing) at every segment head; above 2x live bytes copies the oldest segment's live entries forward a bounded step per batch (1/4 segment, max 1 MiB) under the state lock. The import writes through the same path, so it restates heads too. Review fixes: a failed compaction append puts back the entries it took (no permanent pin); an entry tracked as live but absent from the state is untracked as dead; one call examines at most 4,096 queued entries; compaction starts with one non-active segment, not two. Tests: bounded growth when consumers keep up, a slow consumer compacted forward (not pinning the log), metadata outliving its segment — each equal to the redb reference after reopen; the three review fixes, each mutation-checked."
  - id: 0078-T4
    title: "Evidence and the default flip — the calibration reads DISK-BOUND"
    status: planned
    issue: 662
    notes: "Paid calibration re-run with MQTTD_REPLICA_STORE=log; the flip needs every criterion in ADR 0078 §6."
---

# Delivery: ADR 0078 — The replica store becomes an append-only segment log

[ADR 0078](../adr/0078-replica-segment-log.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0078 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0078-T1 | ✅ done | [#659](https://github.com/mbilling/fss-mqtt-broker/issues/659) | 2026-09-28 | "PR #663. crates/mqtt-cluster/src/segment_log.rs — preallocated segments of framed records (len, crc32c over kind+lsn+payload, kind, lsn, payload); a batch is one pwrite + one fdatasync; replay in LSN order stops at the first invalid record (CRC, LSN out of sequence, length out of bounds): a torn tail in the last segment, corruption (open fails) in any earlier one; recovery ZEROES the rest of the segment from the valid end so intact records of an unacked batch can never replay (the equal-size case an LSN check misses — mutation-checked: removing the zeroing fails two tests); a new segment is created, extended, flushed and its directory flushed before any record lands in it; an empty segment from a crashed roll is removed; drop_before deletes a prefix, never the active segment. CRC-32C in-house (check value 0xE3069283), no new dependency. 12 unit tests, incl. the zero-gap sector-reorder ghost (review finding). Standalone: T2 wires it into ReplicaState." |
| 0078-T2 | ✅ done | [#660](https://github.com/mbilling/fss-mqtt-broker/issues/660) | 2026-09-28 | "PR #665. One commit path (drive_plan) feeds a redb sink or a log sink, so the engines cannot drift; log records mirror the redb rows (Truncate carries up_to AND low_water). A differential test drives the same changes through every apply path on both backends (1 and 4 shards) and requires identical state live and after reopen — mutation-checked. One-way import from redb (staged, renamed into place; redb files kept as *.imported; an interrupted import is redone); redb refuses a log-format data dir. MQTTD_REPLICA_STORE wired in main.rs (default redb); store_watch counts the log dir; the restore guard treats it as store files. Default stays redb until T4. Review fix: the store counts as committed once shard 0 carries its format stamp, not once its directory exists (an unstamped shard 0 left by a crash is a fresh store; regression test, mutation-checked)." |
| 0078-T3 | 🚧 in-progress | [#661](https://github.com/mbilling/fss-mqtt-broker/issues/661) | — | "In review. LogShard tracks where each live entry's record sits and per-segment live counts (decoded from the records it just wrote — the replay's own decoder); drops a dead PREFIX of segments; restates format, fences, caught-up sets and low-waters (a new LowWater record that deletes nothing) at every segment head; above 2x live bytes copies the oldest segment's live entries forward a bounded step per batch (1/4 segment, max 1 MiB) under the state lock. The import writes through the same path, so it restates heads too. Review fixes: a failed compaction append puts back the entries it took (no permanent pin); an entry tracked as live but absent from the state is untracked as dead; one call examines at most 4,096 queued entries; compaction starts with one non-active segment, not two. Tests: bounded growth when consumers keep up, a slow consumer compacted forward (not pinning the log), metadata outliving its segment — each equal to the redb reference after reopen; the three review fixes, each mutation-checked." |
| 0078-T4 | ⬜ planned | [#662](https://github.com/mbilling/fss-mqtt-broker/issues/662) | — | "Paid calibration re-run with MQTTD_REPLICA_STORE=log; the flip needs every criterion in ADR 0078 §6." |
<!-- /status-table:0078 -->

## Plan

1. **T1 — the log itself**, standalone, with crash tests. Nothing in the broker
   uses it yet, so it can land and be reviewed on its own.
2. **T2 — ReplicaState on the log**, behind `MQTTD_REPLICA_STORE`, default
   `redb`; the ReplicaState suite runs on both backends.
3. **T3 — space reclamation**, so a long run holds segments proportional to its
   live backlog.
4. **T4 — the hardware evidence** (ADR 0078 §6) and, only then, the default flip.

## Changelog

- 2026-09-28 — ADR accepted (PR #658 merged). Delivery tasks T1–T4 opened as
  issues #659–#662; T1 started.
- 2026-09-28 — T1 in review (PR #663): the segment log module, standalone,
  with torn-tail, ghost-record (bad-CRC and zero-gap), corruption,
  crashed-roll and prefix-drop tests.
- 2026-09-28 — T1 done: PR #663 merged, issue #659 closed.
- 2026-09-28 — T2 in review: ReplicaState on the segment log behind
  `MQTTD_REPLICA_STORE`, with the redb import.
- 2026-09-28 — T2 done: PR #665 merged, issue #660 closed.
