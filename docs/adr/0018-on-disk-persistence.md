# ADR 0018 — On-disk persistence for durable state

- **Status:** Accepted
- **Date:** 2026-06-19
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0018-on-disk-persistence.md](../delivery/0018-on-disk-persistence.md) — phases, progress, and changelog
- **Related:** [ADR 0001](0001-session-durability.md) (session durability),
  [ADR 0006](0006-consensus-and-replication.md) / [ADR 0007](0007-durable-store-integration.md)
  (consensus + durable store), [ADR 0014](0014-cross-node-retained.md) (retained replication)

> This record states the decision only. The phased rollout and how far along it is live
> in the [delivery doc](../delivery/0018-on-disk-persistence.md).

## Context

Originally every durable store in the broker was **in-memory**. "Durable sessions" meant
*highly available across a single node failure* (quorum replication + takeover, ADR
0006/0016/0017) — **not** *persistent across restarts*. Three stores held the state and
all lost it when their process exited:

1. **The lease consensus log** — `LeaseStore` implements openraft's `RaftStorage` over an
   `Arc<Mutex<Inner>>`: the Raft log, the persisted **vote**, the applied state machine,
   and snapshots were all `BTreeMap`/`Vec` in memory ("a node rebuilds its lease view from
   peers on restart" — which only works while a quorum stays up).
2. **The replicated session log** — session metadata + offline queues live in a
   `ReplicatedLog`; the single-node backend is `InMemoryReplicatedLog` ("all state is lost
   when the process exits").
3. **Retained messages** — `MemoryRetainedStore` is a `Mutex<HashMap<String, Message>>`.

Consequences:

- **Full-cluster restart = total data loss.** Every node's log is freshly empty; there is
  nothing to recover from (peers are equally empty).
- **Raft safety depends on a persisted vote.** openraft requires the vote and log to
  survive a crash so a node cannot vote twice in a term. The in-memory store passes
  openraft's conformance suite (which does not require disk) but **violates the real
  safety precondition on restart** — a crashed-and-restarted lease voter could risk
  split-brain on the lease group.
- A "durable" broker that cannot survive a rolling restart, a datacenter power event, or a
  simultaneous-quorum-loss is not durable in the sense operators expect.

The good news: all three stores sit behind clean trait seams (`ReplicatedLog`,
`RaftStorage` via `LeaseStore`, `RetainedStore`). Adding persistence is new backends
behind existing interfaces, not a rearchitecture.

## Decision

Adopt an **embedded, pure-Rust, ACID storage engine** and implement a disk-backed backend
behind each of the three seams, with crash-consistent semantics tied to the existing
durability guarantee.

### 1. Storage engine: `redb` (pure-Rust, ACID, single-file)

Use [`redb`](https://crates.io/crates/redb) as the embedded engine for all three stores:

- **Pure Rust, no C/C++ toolchain.** Keeps the `cargo deny` supply-chain surface small and
  auditable (ADR 0002/0003 ethos). RocksDB drags a large C++ build; `sled` is pure-Rust but
  its on-disk format is beta and effectively unmaintained.
- **ACID with explicit durability.** A copy-on-write B-tree with MVCC and a configurable
  `Durability` (incl. `Immediate` = fsync on commit) — exactly what a
  quorum-fsync-before-PUBACK guarantee needs.
- **Single file per store, actively maintained, MIT/Apache.**

One engine for all three means **one dependency to review** and one set of
crash-consistency tests. (A dedicated segmented-WAL for the high-volume session message
log is a possible later optimization — see Consequences — but is not needed to ship.)

### 2. Durability semantics tied to the PUBACK guarantee

The `ReplicatedLog::append` contract is *"return only once the record is epoch-fenced and
quorum-durable; until then the caller must not release a QoS≥1 PUBACK."* Persistence makes
"durable" mean **fsynced**:

- A node's local `append` **fsyncs to disk** (`Durability::Immediate`) before it counts
  toward the quorum ack, so a committed append is fsynced on a majority — it survives a
  simultaneous crash of the whole majority.
- The openraft lease store fsyncs the **vote** and **log entries** before acknowledging,
  per Raft's storage contract (this is what makes restart safe).
- Retained-store writes fsync before the PUBLISH is acknowledged at the configured QoS.

This trades latency for correctness (see Consequences). An explicit, documented relaxed
mode (group-commit / periodic fsync) MAY be offered later as an opt-in, loudly logged like
every other relaxed mode.

### 3. Restart recovery

- **Lease group:** on start, `LeaseStore` loads its persisted vote, log, and latest
  snapshot; openraft replays to the last applied index. A restarted node rejoins with its
  real Raft identity — no double-voting — and the group recovers from a full restart as
  long as a quorum of *persisted* nodes returns.
- **Session log:** each node loads its persisted committed log on start; takeover recovery
  (ADR 0017) reads a quorum of *persisted* replicas, so a session survives full restart,
  not just single-node failure.
- **Retained:** loaded from disk on start; cross-node back-fill (ADR 0014) still reconciles
  divergence.

### 4. Data location & layout

A single configurable data directory (`MQTTD_DATA_DIR`, e.g. `/var/lib/mqttd`), one `redb`
file per store (`lease.redb`, `sessions.redb`, `retained.redb`, `replicas.redb`). Node
identity (ADR 0004, the cert CN) is the implicit owner of the directory; a startup check
refuses to open a data dir stamped with a *different* node id (prevents two nodes sharing a
volume).

## Consequences

- **Good:** real durability — sessions, subscriptions, QoS-2 exactly-once windows, offline
  queues, retained messages, and lease consensus survive process restart, rolling upgrades,
  and full-cluster restart. Raft safety is restored (persisted vote). The trait seams mean
  the change is additive and backend-selectable.
- **Cost:** fsync-on-commit adds write latency (single-digit-ms on SSD, worse on spinning
  disks / networked block storage); QoS≥1 publish throughput is now disk-bound. Group-commit
  batching mitigates this. A new (pure-Rust) dependency to vet. Disk capacity planning and
  compaction become operational concerns.
- **Risk:** storage code is correctness-critical and crash-consistency bugs are subtle —
  hence crash-injection testing and the choice of an ACID engine over a hand-rolled format.
  fsync semantics on some filesystems/cloud volumes lie; document the supported storage
  classes.

## Alternatives considered

- **RocksDB.** Battle-tested for Raft logs, but a heavy C++ dependency that cuts against the
  pure-Rust, minimal-supply-chain posture. Reconsider only if redb's write throughput proves
  inadequate for the session message log.
- **`sled`.** Pure-Rust, but a beta on-disk format and stalled maintenance — unacceptable for
  the durability foundation.
- **Hand-rolled segmented WAL.** Best raw throughput for the append-only session log, but
  re-implementing crash-consistent storage is exactly the wheel an ACID engine exists to
  avoid. Keep as a targeted later optimization for the one high-volume store.
- **Stay in-memory, document the limitation.** Honest, fine for an HA-not-persistent posture
  — but it caps the product below what the consensus investment promises.
