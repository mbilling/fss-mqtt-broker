# ADR 0018 — On-disk persistence for durable state

- **Status:** Accepted; **phase 1 implemented** (2026-06-19); phases 2–5 pending
- **Date:** 2026-06-19
- **Deciders:** project maintainers
- **Related:** [ADR 0001](0001-session-durability.md) (session durability),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (consensus + durable store), [ADR 0014](0014-cross-node-retained.md) (retained replication)

## Context

Every durable store in the broker is **in-memory**. "Durable sessions" today means
*highly available across a single node failure* (quorum replication + takeover, ADR
0006/0016/0017) — **not** *persistent across restarts*. Three stores hold the state and
all lose it when their process exits:

1. **The lease consensus log** — `LeaseStore` (`crates/mqtt-cluster/src/lease_store.rs`)
   implements openraft's `RaftStorage` over an `Arc<Mutex<Inner>>`: the Raft log, the
   persisted **vote**, the applied state machine, and snapshots are all `BTreeMap`/`Vec`
   in memory. Its own module doc says "a node rebuilds its lease view from peers on
   restart" — which only works while a quorum stays up.
2. **The replicated session log** — session metadata + offline queues live in a
   `ReplicatedLog` (`crates/mqtt-storage/src/repl.rs`). The clustered backend keeps each
   node's committed copy in an in-memory replica state; the single-node backend is
   `InMemoryReplicatedLog` ("all state is lost when the process exits").
3. **Retained messages** — `MemoryRetainedStore` is a `Mutex<HashMap<String, Message>>`.

Consequences:

- **Full-cluster restart = total data loss.** Every node's log is freshly empty; there
  is nothing to recover from (peers are equally empty). Sessions, subscriptions, QoS-2
  dedup windows, offline queues, and retained messages are all gone.
- **Raft safety depends on a persisted vote.** openraft requires the vote and log to
  survive a crash so a node cannot vote twice in a term. The in-memory store passes
  openraft's conformance suite (which does not require disk) but **violates the real
  safety precondition on restart** — a crashed-and-restarted lease voter could
  participate in a way that risks split-brain on the lease group.
- A "durable" broker that cannot survive a rolling restart, a datacenter power event, or
  a simultaneous-quorum-loss is not durable in the sense operators expect.

The good news: all three stores sit behind clean trait seams (`ReplicatedLog`,
`RaftStorage` via `LeaseStore`, `RetainedStore`). Adding persistence is a matter of new
backends behind existing interfaces, not a rearchitecture.

## Decision

Adopt an **embedded, pure-Rust, ACID storage engine** and implement a disk-backed
backend behind each of the three seams. Persist with crash-consistent semantics tied to
the existing durability guarantee.

### 1. Storage engine: `redb` (pure-Rust, ACID, single-file)

Use [`redb`](https://crates.io/crates/redb) as the embedded engine for all three stores.
Rationale, in priority order for a security-first broker:

- **Pure Rust, no C/C++ toolchain.** Keeps the `cargo deny` supply-chain surface small
  and auditable — the dominant concern (ADR 0002/0003 ethos). RocksDB (`rust-rocksdb`)
  drags a large C++ build and transitive surface; `sled` is pure-Rust but its on-disk
  format is still beta and effectively unmaintained.
- **ACID with explicit durability.** `redb` is a copy-on-write B-tree with MVCC and a
  configurable `Durability` (incl. `Immediate` = fsync on commit) — exactly what a
  quorum-fsync-before-PUBACK guarantee needs.
- **Single file per store, actively maintained, MIT/Apache.**

One engine for all three stores means **one dependency to review** and one set of
crash-consistency tests. (A dedicated segmented-WAL for the high-volume *session message
log* is a possible later optimization if throughput demands it — see Consequences — but
is not needed to ship persistence.)

### 2. Durability semantics tied to the PUBACK guarantee

The contract in `ReplicatedLog::append` is *"return only once the record is epoch-fenced
and quorum-durable; until then the caller must not release a QoS≥1 PUBACK."* Persistence
makes "durable" mean **fsynced**:

- A node's local `append` **fsyncs to disk** (`Durability::Immediate`) before that node
  counts toward the quorum ack. So a committed (quorum) append is fsynced on a majority —
  it survives a simultaneous crash of the whole majority.
- The openraft lease store fsyncs the **vote** and **log entries** before acknowledging,
  per Raft's storage contract (this is what makes restart safe).
- Retained-store writes fsync before the PUBLISH is acknowledged at the configured QoS.

This trades latency for correctness (see Consequences). An explicit, documented relaxed
mode (group-commit / periodic fsync) MAY be offered later as an opt-in for throughput,
loudly logged like every other insecure/relaxed mode.

### 3. Restart recovery

- **Lease group:** on start, `LeaseStore` loads its persisted vote, log, and latest
  snapshot from disk; openraft replays to the last applied index. A restarted node
  rejoins the group with its real Raft identity — no double-voting, and the group can
  recover from a full restart as long as a quorum of *persisted* nodes returns.
- **Session log:** each node loads its persisted committed log on start; takeover
  recovery (ADR 0017) reads a quorum of *persisted* replicas, so a session survives full
  restart, not just single-node failure.
- **Retained:** loaded from disk on start; cross-node back-fill (ADR 0014) still
  reconciles divergence.

### 4. Data location & layout

A single configurable data directory (`MQTTD_DATA_DIR`, default e.g.
`/var/lib/mqttd`), with one `redb` file per store (`lease.redb`, `sessions.redb`,
`retained.redb`). Node identity (ADR 0004, the cert CN) is the implicit owner of the
directory; a startup check refuses to open a data dir stamped with a *different* node id
(prevents two nodes sharing a volume).

### Phasing

- **Phase 1 — single-node session persistence.** A `redb`-backed `ReplicatedLog`
  (`PersistentLog`) behind the existing trait. Smallest change; proves the engine,
  the fsync semantics, and crash-consistency testing. A single-node broker becomes
  truly durable. No cluster code touched.
- **Phase 2 — lease store on disk.** Replace `LeaseStore`'s `Inner` with a `redb`-backed
  implementation of the same `RaftStorage`. Restores Raft safety across restart; the
  lease group survives full restart. Validate against openraft's conformance suite **and**
  a crash-injection test.
- **Phase 3 — replicated session log on disk.** Persist each node's committed replica
  state so cluster session recovery survives full restart (not just single failure).
- **Phase 4 — retained store on disk.** `redb`-backed `RetainedStore`.
- **Phase 5 — operational:** compaction/snapshot policy, data-dir node-id guard, restart
  integration test (kill all nodes, restart, assert sessions + retained + leases recover).

## Implementation notes (for the workstream)

- New crate-internal modules: `mqtt-storage/src/persistent_log.rs` (`PersistentLog:
  ReplicatedLog`), `mqtt-storage/src/persistent_retained.rs`, and a redb-backed `Inner`
  for `mqtt-cluster/src/lease_store.rs`. A small shared `mqtt-storage/src/redb_util.rs`
  for table/codec helpers.
- Keys: session log is keyed `q/<client>` / `m/<client>` (already); map each to a redb
  table keyed by `(client, offset)` so `read`/`truncate`/`live_range` are range scans and
  the O(1) `live_range` watermark is a per-key min/max kept in a side table.
- `main.rs`: when `MQTTD_DATA_DIR` is set, build the persistent backends; otherwise keep
  the in-memory backends (single-node ephemeral stays the zero-config default, loudly
  logged as non-durable — consistent with the project's opt-in posture).
- Testing: a crash-injection harness (drop the process / the `redb` handle mid-write and
  reopen) asserting no torn reads and that a fsynced append survives. This is the
  correctness bar before any phase is "done" — same rigor as the SWIM/consensus work.

## Update — phase 1 implemented (2026-06-19)

`PersistentLog` (`crates/mqtt-storage/src/persistent_log.rs`) implements `ReplicatedLog`
over `redb` with `Durability::Immediate` (fsync) on every mutating commit. Synchronous
`redb` work runs on `spawn_blocking` so the fsync never stalls an async worker. The
on-disk layout is the two tables described in the implementation notes; the per-key
offset counter is persisted independently so it stays monotonic across `truncate` and
resets only on `remove` — matching `InMemoryReplicatedLog` exactly. Wired into `mqttd`
via `MQTTD_DATA_DIR`: a single-node broker now stores sessions in `<dir>/sessions.redb`
and survives a restart. `redb` (pure-Rust) resolves to a 1.75-MSRV-compatible version and
passes `cargo deny` (advisories/bans/licenses/sources). Tests cover the in-memory
backend's contract plus a **survives-reopen** durability proof (committed state and the
offset counter are recovered after the database is closed and reopened).

Still pending: phase 2 (disk-backed openraft lease store — the Raft-safety fix), phase 3
(disk-backed *replicated* session log for cluster restart survival), phase 4 (retained),
phase 5 (compaction, data-dir node-id guard, process-kill crash test).

## Consequences

- **Good:** real durability — sessions, subscriptions, QoS-2 exactly-once windows,
  offline queues, retained messages, and lease consensus survive process restart, rolling
  upgrades, and full-cluster restart. Raft safety is restored (persisted vote). The
  trait seams mean the change is additive and backend-selectable.
- **Cost:** fsync-on-commit adds write latency (single-digit-ms on SSD, much worse on
  spinning disks / networked block storage); QoS≥1 publish throughput is now disk-bound.
  Group-commit batching mitigates this and is the standard answer. A new (pure-Rust)
  dependency to vet. Disk capacity planning and compaction become operational concerns.
- **Risk:** storage code is correctness-critical and crash-consistency bugs are subtle —
  hence the explicit crash-injection testing and the choice of an ACID engine over a
  hand-rolled file format. fsync semantics on some filesystems/cloud volumes lie; document
  the supported storage classes.

## Alternatives considered

- **RocksDB.** The battle-tested choice for Raft logs, but a heavy C++ dependency that
  cuts against the pure-Rust, minimal-supply-chain posture. Reconsider only if redb's
  write throughput proves inadequate for the session message log.
- **`sled`.** Pure-Rust, but a beta on-disk format and stalled maintenance — unacceptable
  for the durability foundation.
- **Hand-rolled segmented WAL.** Best raw throughput for the append-only session log, but
  re-implementing crash-consistent storage is exactly the kind of correctness-critical
  wheel an ACID engine exists to avoid. Keep as a targeted later optimization for the one
  high-volume store, not the foundation.
- **Stay in-memory, document the limitation.** Honest, and fine for an HA-not-persistent
  posture — but it caps the product below what the consensus investment promises and what
  operators expect from "durable."
