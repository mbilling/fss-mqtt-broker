# 0078. The replica store becomes an append-only segment log — the disk is the limit, not the B-tree

- **Status:** Accepted
- **Date:** 2026-09-28
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0078-replica-segment-log.md](../delivery/0078-replica-segment-log.md) — plan, progress, and changelog
- **Related:** [ADR 0018](0018-on-disk-persistence.md) (redb; the deferred segmented WAL),
  [ADR 0027](0027-replica-group-commit.md) / [0071](0071-owner-side-group-commit.md) (group commit),
  [ADR 0075](0075-pipelined-durable-appends.md) (pipelined appends),
  [ADR 0076](0076-self-measuring-sharded-store.md) (the barrier probe; K files rejected),
  issue [#568](https://github.com/mbilling/fss-mqtt-broker/issues/568)

## Context

Durable QoS 1/2 throughput should be bounded by the disk each node drew. It is
not. The first durable QoS 1 lane E calibration (2026-09-28, 3 × CCX23, v1.0.18,
persistent consumers, #568) and a profile of the writer's commit path found the
limit inside the commit, upstream of the device.

**What the calibration measured.** Knee between 2,000 and 6,000 msg/s per node:
GREEN at 6,000 msg/s for the cluster, not carried at 18,000 (11% of publishes
late). At the heaviest rung (36,000 offered, 30,159 delivered), per broker, over
the steady window:

| broker | barrier floor (fsync/s) | writer ops/s | ops per commit | commit time | time per op | one flush's share of a commit |
|---|---|---|---|---|---|---|
| broker2 | 1,492 | 30,498 | 573 | 18.50 ms | 0.032 ms | 4% |
| broker1 | 2,349 | 33,011 | 568 | 18.28 ms | 0.032 ms | 2% |
| broker0 | 2,783 | 30,531 | 333 | 10.74 ms | 0.032 ms | 3% |

- **Flush share** is `(1000 / F) / C`, where `F` is the broker's single-writer
  barrier floor (flushes per second, its own boot probe) and `C` the mean commit
  time in ms. At `F = 1,492` and `C = 18.50`: `(1000 / 1,492) / 18.50 = 0.67 /
  18.50 = 3.6%`. A disk-bound commit would be mostly flush; these are ~97% other
  work.
- **Time per op is identical on disks 1.9× apart** (`2,783 / 1,492`): the cost
  is not the disk's.
- The hub thread was at most 24% busy; every core kept 28–39% idle; iowait ~2%.
- `replicas.redb` on one broker reached 4.2 GB in a 27-minute run of 200-byte
  payloads, nearly all acknowledged and truncated.

**Where the commit time goes** (local profile of the real `apply_batch_sharded`
path, 2,304 keys, 260 B records, a 400k-entry backlog; tmpfs, so CPU and
syscalls only, fsync ~0):

| share of samples | where |
|---|---|
| ~60% | redb copy-on-write: each op dirties its own 4 KiB B-tree leaf, then copied, checksummed, tracked and freed at commit |
| ~14% | `pwrite` of those pages |
| ~20% | our bookkeeping — fence rows rewritten every commit, whole-backlog `retain` per truncate |
| ~0.1% | encoding |

The bookkeeping share was removed in PR #657 (−15% to −18% time per op, the same
reproducer). What remains is the engine's shape, not its tuning.

**Write and space amplification** (same reproducer, `/proc/self/io` `wchar`
per writer op): 6.8–15.6 KB written per op against ~320 B of logical data per
message — 21× to 49×; a live entry occupies 675–1,078 B of file.

**Why the engine does not fit the workload.** Per replica, one durable message is
two writer ops: an Append when it is published and a Truncate when it is
acknowledged. The store is a set of FIFO queues: append at the tail, drop from the
head, and never read on the hot path. `ReplicaState` already holds every live
entry, fence, truncation low-water and caught-up set **in memory**
(`ReplicaLogs`, `cluster_log.rs:142`); `load_all` reads the whole file once at
open, and every read after that is served from memory. The file is only a durable
record of changes. A copy-on-write B-tree turns that append-and-drop-prefix stream
into random page rewrites, 4 KiB at a time, and pays per-op CPU to maintain an
index nothing queries.

ADR 0018 chose redb and deferred exactly this option. A dedicated segmented WAL
"for the high-volume session message log is a possible later optimization", and
its Alternatives keep it "as a targeted later optimization for the one
high-volume store". ADR 0076 rejected
K parallel redb files because group commit makes throughput `D × F` (batch depth
× flush rate) and K files divide `D`; that holds while the flush dominates the
commit. On this hardware it does not — the commit is 97% per-op work — which is
why neither more files nor a deeper batch moves the knee.

## Decision

Replace **`replicas.redb`** — the replica state (`ReplicaState`) — with an
append-only **segment log**, per shard. `lease.redb` (the Raft log of the lease
group), `retained.redb` and the single-node `sessions.redb` are unchanged: they
are low-volume or out of scope for the clustered durable path.

### 1. Format

- A shard's log is a directory of **segments**, `seg-<first-lsn>.log`, each
  preallocated (`fallocate`) to a fixed size (proposed 64 MiB) so a flush never
  has to update file metadata.
- A segment is a sequence of **records**:
  `[len: u32][crc32c: u32][kind: u8][lsn: u64][payload]`. Kinds mirror the ops
  that exist today, plus the metadata the tables hold:
  `Append{key, offset, epoch, seq, record}`, `Truncate{key, up_to}`,
  `Remove{key}`, `Fence{group, epoch}`, `CaughtUp{group, members}`, and
  `Checkpoint` (below).
- The record **is** the durable message plus its framing (17 bytes), key and
  three 8-byte fields — about 50–80 bytes for the `q/{client}` keys in use — not
  a 4 KiB page. For a 200-byte payload that is ~1.3× written per logical byte,
  against 21–49× measured on redb (expected, not yet measured).

### 2. Commit — the flush is the only cost

- The single writer per shard (unchanged, `spawn_replica_writer`) serializes a
  batch's records into one buffer, appends it with one `write` at the segment's
  tail, and calls `fdatasync` once. Then it acks — **persist-before-ack is
  unchanged**, at batch granularity exactly as today.
- Ordering is the log's order, which is the writer's FIFO — the property the code
  already relies on ("one file, one writer, one FIFO", `durable_plane.rs:427`).
- All decisions stay where they are, in memory and under the lock: the fence
  check, the ADR 0042 T7 stale-attempt guard, the monotonic truncation low-water.
  The log records outcomes; it decides nothing.
- A `Fence` record is written only when a fence moves (as #657 does for the
  table), before the ops at that epoch in the same batch.

### 3. Recovery

- At open, replay every segment in LSN order into the in-memory state — what
  `load_all` does from the tables today. A record whose CRC fails, or a short
  record at the tail of the last segment, is a **torn tail**: the segment is
  truncated there, since nothing past it was ever acked (acks follow the flush).
- A torn or corrupt record **before** the tail is not recoverable locally and
  fails the open with the store's offset, exactly as a corrupt redb file does
  today. The key is then recovered the way a lost replica is today: the new
  owner's quorum merge of replica logs (`merge_replica_logs_tagged`).

### 4. Space reclamation

- Each segment keeps an in-memory count of its **live** Appends (not yet
  truncated or removed). A segment whose count reaches zero, and which is older
  than the last checkpoint, is deleted — the common case for a queue whose
  consumers keep up: whole segments die as acks pass them.
- When space amplification (log bytes / live bytes) passes a bound (proposed
  2×), the writer **compacts the oldest segment**: it re-appends that segment's
  still-live entries and a `Checkpoint` carrying current fences, low-waters and
  caught-up sets, then deletes the segment. Cost is proportional to the live
  data it moves, off the ack path but through the same writer, so order holds.
- Bounded by design: a slow consumer pins only its own live entries, which are
  in memory already; the log holds them once more on disk.

### 5. Rollout

- Behind a config switch, `MQTTD_REPLICA_STORE=redb|log`, default `redb`, so the
  two run side by side on the same harness.
- Migration: a node that finds `replicas.redb` and is set to `log` imports it once
  (load, write one checkpoint, flush) and keeps the redb file, renamed, until the
  operator removes it. The reverse is not supported; a node rolls back by
  restoring the kept file.
- The default flips to `log` only on the evidence in §6.

### 6. What counts as done

On the calibration shape (`bench/scale/qos1-campaign/durable-calibrate-n3.env`,
3 × CCX23, persistent consumers, iostat captured per PR #656):

1. **The disk is the limit.** At the most loaded durable rung,
   `summarize-curve.py`'s verdict is DISK-BOUND: one flush explains ≥ 50% of a
   commit, or the disk is ≥ 80% utilised.
2. **Throughput follows the disk.** Across the three brokers, durable appends per
   second at that rung track each broker's barrier floor.
3. **The knee moves.** The GREEN knee per node rises above the 2,000–6,000 msg/s
   bracket measured on redb.
4. **Nothing is lost or reordered.** The existing durability and failover suites
   pass with the log store selected, plus crash tests that cut a batch mid-write
   (torn tail) and kill the node between write and flush.
5. **Recovery is bounded.** Replay time at open is measured and stated per GB of
   log.

## Consequences

- The durable path's cost per message becomes `record bytes + one share of a
  flush`. The ceiling per node becomes the disk's: flushes per second × batch
  depth, or write bandwidth / record size, whichever binds first.
- mqttd owns a storage format: framing, checksums, torn-tail handling,
  compaction, and a migration. That is new code with durability consequences, and
  it is tested accordingly (§6.4), not assumed.
- Memory is unchanged: live entries are already held in memory by
  `ReplicaState`; the log does not add a cache.
- Space is reclaimed by deleting whole segments rather than freeing pages inside
  one file, so the 4.2 GB file of a 27-minute run becomes a handful of 64 MiB
  segments while consumers keep up.
- ADR 0076's sharding switch (`MQTTD_STORE_SHARDS`) keeps its meaning: K shards
  are K segment directories with K writers. Whether K > 1 pays off is measured
  again once the commit is mostly flush.

## Alternatives considered

- **Keep redb and tune it.** PR #657 removed our own overhead (−15% to −18%).
  Raising `set_cache_size` above 1 GiB helps only once the file outgrows the
  cache. Neither changes the page-per-op copy-on-write that is ~60% of the
  commit.
- **K redb files (ADR 0076).** Spreads per-op CPU over cores, but keeps the
  21–49× write amplification and the growing files. Rejected as a default on a
  flush-bound host; not the fix on a CPU-bound one.
- **RocksDB.** An LSM: sequential WAL plus memtable — the right shape. ADR 0018
  rejected it as a heavy C++ dependency, and its memtable would duplicate state
  `ReplicaState` already holds, while compaction would rewrite it again.
- **fjall** (pure-Rust LSM). The same shape and the same duplication, and a
  second storage engine to audit, for a store whose reads never touch disk.
- **LMDB, sled.** LMDB is a copy-on-write B-tree too (the same amplification);
  sled's format is beta and its maintenance stalled (ADR 0018).
- **Relax durability (`Durability::Eventual`).** Rejected by ADR 0027 and 0072:
  an acked QoS 1/2 message must survive node loss.
