# Backup/restore measurement — export cost, window width, restore cost

**Dated 2026-08-15.** [ADR 0062](../adr/0062-online-backup-and-restore.md) ·
issue [#249](https://github.com/mbilling/fss-mqtt-broker/issues/249) · harness:
`crates/mqttd/tests/backup_bench.rs`

## Read this first: what these numbers are, and what they are not

Every number below was produced on **one developer machine** (8 cores, one APFS volume,
release build, in one process). In this repository's vocabulary that makes them
**development-grade** (`bench/README.md`, [ADR 0048](../adr/0048-comparative-benchmarking.md)
phase 2): they are published so the method and the shape are checkable, and so an operator
has a formula with real terms in it instead of an adjective. They are **not** a capacity
claim and must never be quoted as "mqttd restores N records/s".

Three specific things they do **not** bound:

1. **A cluster restore.** Both halves here run against the single-node persistent stores,
   which isolates the term that dominates (one fsync per durable write) and excludes the
   cluster's per-write quorum round-trip. **A cluster restore is slower than this, never
   faster.**
2. **Your disk — and not even reliably the same disk twice.** The restore rate is pinned by
   the volume's device cache flush rate.
   [DURABLE-PATH.md](DURABLE-PATH.md) measured **~215–240 durable writes/s** on its
   reference host for the same barrier. An earlier session on *this* host measured
   **~74–83/s**; the two runs recorded below, minutes apart on the same host and volume,
   measured **~162–173 records/s restoring and ~200–206 writes/s building the fixture**.
   So the spread is a factor of ~2.5 *between measurement sessions on one machine*, on top
   of the spread between machines. Do not inherit any of these numbers: the term belongs to
   the device and its current state, and the harness exists so you can measure your own.
3. **Export cost at scale.** The export materialises the readable session set in memory
   before writing (named as a residual in ADR 0062); at the fixture below that is tens of
   MB, and a node holding a million large queued messages is a different measurement.

## Fixture

| | |
|---|---|
| Sessions | 1,000, each with 1 subscription and 10 queued QoS-1 messages of 256 B |
| Retained | 10,000 topics × 256 B |
| Records | 21,000 (1,000 session records + 10,000 queued + 10,000 retained) |
| Stores | `sessions.redb` + `retained.redb`, single-node persistent, one process |
| Build | `cargo test --release -p mqttd --test backup_bench -- --ignored --nocapture` |

Both fixture sizes are the harness defaults and are overridable
(`MQTTD_BACKUP_BENCH_SESSIONS`, `_QUEUED`, `_RETAINED`).

## Results

Two consecutive runs, same host, same volume, minutes apart. Both are shown rather than one
averaged number, because the difference between them *is* one of the findings.

| Measurement | Run 1 | Run 2 | What it is |
|---|---|---|---|
| **Export wall clock** | **0.07 s** | **0.06 s** | The whole online export, from a live store |
| **Export size** | **11,616,121 B** (11.6 MB) | identical | ~553 B per record at a 256 B payload — NDJSON + base64 overhead |
| **Window width W** | **51 ms** | **51 ms** | `finished_unix_ms − started_unix_ms`: the width of the interval the consistency claim is stated over |
| **Restore wall clock** | **129.3 s** | **121.5 s** | 21,000 records through the ordinary durable path |
| **Restore rate** | **162 records/s** | **173 records/s** | One fsync per record; no batch-append API exists, by decision |
| **Fixture build rate** | **200 writes/s** | **206 writes/s** | The same fsync-bound path, measured independently in the same run — the ceiling both numbers sit on |

An earlier session on this host recorded 0.10 s / W = 59 ms / 74 records/s / 83 writes/s at
the same fixture. The export size moved for a knowable reason — format v2 added an
`(epoch, offset)` token and a `tombstone` bit to every retained record, and
11,616,121 − 11,306,119 = 310,002 B over 10,000 retained records is exactly that. The
**rates** moved for no reason visible from here, which is the honest state of a
device-bound measurement and the reason the preamble says what it says.

## What the numbers say

**The export is cheap and its window is narrow.** Under a tenth of a second for 21,000
records, and a 51 ms window — three orders of magnitude smaller than any sane `every_secs`.
So in the RPO formula

```
RPO ≤ every_secs + W
```

the `W` term is noise at this scale, and the RPO is effectively the schedule. `W` is
nevertheless recorded by **every** run — `finished_unix_ms − started_unix_ms` in the file's
trailer, and `backup.window_ms` on `/statusz` — because it is small *here*, and an operator
with 100× the state deserves to read their own value rather than trust this one. Note that
`mqttd_backup_duration_ms` is the **whole run's** wall clock (reads plus write, fsync and
rename), so it is an upper bound on `W`, not `W`: at this fixture, 60–70 ms against a 51 ms
window.

**The restore is fsync-bound and linear.** ~170 records/s means the RTO formula

```
RTO ≈ fresh-cluster start + records / durable-write rate
```

is dominated by its second term: at these numbers a 21,000-record restore is ~2 minutes and
a 100,000-record restore is ~10 — but at the earlier session's 74 records/s the same two
figures were ~5 and ~22 minutes. Linear and predictable *in shape* is what an RTO needs;
the constant is yours to measure. The fixture-build rate is reported beside the restore rate
for the same reason it always was: the two track each other (200 vs 162, 206 vs 173) because
they are the same barrier, which is the evidence that the restore is paying for durability
and not for the importer.

**Why it is not made faster.** A batched-durability import path would open a second
durability seam beside [ADR 0018](../adr/0018-on-disk-persistence.md)'s fsync-before-ack
contract, and a batch that crashes mid-way leaves a partially-imported store — the one
state ADR 0062 refuses to permit. The mitigations that cost no contract are in the ADR:
the restore is a cold-start operation, `/readyz` reports `restore-in-progress` and no
client port is bound while it runs, and an interrupted restore is never resumed.

**How to size your own RTO.** Multiply your record count (sessions + queued messages +
retained topics — the export's trailer counts all three) by the reciprocal of *your*
volume's durable-write rate, and add a quorum round-trip per write for a cluster. The
harness prints all four terms, so re-running it on the target hardware answers the question
directly.
