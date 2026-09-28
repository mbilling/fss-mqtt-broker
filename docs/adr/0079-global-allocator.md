# 0079. mqttd links mimalloc as its global allocator

- **Status:** Accepted
- **Date:** 2026-09-28
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0079-global-allocator.md](../delivery/0079-global-allocator.md) — plan, progress, and changelog
- **Related:** [ADR 0078](0078-replica-segment-log.md) (the replica segment log and its
  §6 evidence), [ADR 0071](0071-owner-side-group-commit.md) (the durable writer),
  issue [#568](https://github.com/mbilling/fss-mqtt-broker/issues/568)

## Context

The shipped `mqttd` is a static musl binary (`scripts/release/build-repro.sh`,
`x86_64-unknown-linux-musl` / `aarch64-unknown-linux-musl`, `+crt-static`), and it
uses the platform allocator: musl's `malloc`. Nothing in the workspace sets a
`#[global_allocator]`.

The ADR 0078 T4 calibration (2026-09-28, 3 × CCX23, persistent QoS 1 consumers,
`MQTTD_REPLICA_STORE=log`) measured the durable writer's commit at **~0.024 ms per
op** on every broker, flat across batch sizes from 37 to 2,268 ops, on disks 3.5×
apart in flush rate — so neither the disk nor the batch explained it. The same
commit function timed locally costs a tenth of that. The difference is the
allocator under concurrency.

**The measurement.** `ReplicaState::apply_batch_sharded` (the writer's whole
commit: decide, write, flush, apply, reclaim), 2,400 keys, 260-byte records,
batches of 2,000 ops, 400,000 ops, on tmpfs (so the flush is ~free and only CPU
and syscalls remain), the process pinned to 4 cores (`taskset -c 0-3`, the CCX23's
vCPU count). "Other threads" are 3 threads allocating and freeing small buffers in
a loop — a stand-in for the tokio workers that allocate on every publish,
delivery and frame while the writer commits.

| build | other threads allocating | log store, µs per op | redb, µs per op |
|---|---|---|---|
| glibc | 0 | 2.4 | 7.3 |
| glibc | 3 | 2.8 | — |
| musl | 0 | 4.1 | 10.4 |
| **musl** | **3** | **13.4** | — |
| musl + mimalloc | 0 | 3.6 | — |
| **musl + mimalloc** | **3** | **3.9** | 10.3 |

- musl's allocator costs `4.1 / 2.4 = 1.7×` glibc's when the writer is alone, and
  `13.4 / 2.8 = 4.8×` when three other threads allocate at the same time: it
  serializes allocation behind a lock that the other threads contend.
- mimalloc (thread-local free lists, no global lock on the fast path) takes the
  contended musl figure from 13.4 to 3.9 µs per op — `13.4 / 3.9 = 3.4×` — and
  leaves it within 8% of the uncontended one (`3.9 / 3.6`).
- The broker's other hot paths — codec, routing, per-message `Arc`/`Vec` churn on
  every tokio worker — allocate on the same allocator. The table measures only the
  writer; the effect on end-to-end throughput is what the delivery's evidence task
  measures, not what this ADR assumes.

## Decision

`mqttd` sets `mimalloc::MiMalloc` as its `#[global_allocator]`, unconditionally,
in the binary crate only (`crates/mqttd/src/main.rs`). Libraries in the workspace
do not set an allocator; a library user keeps their own.

- **Crate:** `mimalloc` (MIT), which builds the upstream C library (MIT) through
  `libmimalloc-sys` with the `cc` crate — the same C toolchain path `aws-lc-rs`
  already takes in the release build (`CC_<target>`), so the reproducible build
  recipe does not change.
- **Default features only.** No `secure` mode, no override of the C `malloc`
  symbol: Rust allocations go to mimalloc; C code (aws-lc) keeps musl's.
- **The FIPS variant** (`mqttd-fips`, ADR 0068) gets the same allocator — the
  validated module is aws-lc, not the allocator, and its allocations stay on the
  C `malloc` exactly as above.

## Consequences

- Durable-path CPU per op falls toward the glibc figure on the shipped binary;
  every benchmark after this decision says which allocator it ran (the binary's
  version pins it).
- One more C dependency is compiled into the binary, audited by `cargo deny`
  (MIT is already allowed) and pinned by `Cargo.lock` like every other crate.
- mimalloc holds freed memory in per-thread pages and returns it to the OS lazily,
  so RSS after a burst can sit above musl's. The memory brownout (ADR 0041 T8)
  reads RSS; its thresholds are the operator's, and the delivery measures idle
  and post-burst RSS so the difference is stated rather than discovered.
- Reversible: removing two lines restores musl's allocator; no on-disk format or
  wire behaviour depends on it.

## Alternatives considered

- **Keep musl's allocator.** Leaves the measured 3.4× on the durable writer's
  per-op cost, on the shipped binary only — local glibc builds would keep
  under-reporting it.
- **jemalloc (`tikv-jemallocator`).** Also removes the contention; a larger C
  build, slower to compile, and its maintenance is thinner than mimalloc's since
  upstream archived jemalloc in 2025.
- **Ship glibc (dynamic) binaries.** Changes the distribution model (static,
  reproducible, runs on any Linux) for one allocator; rejected.
- **Allocate less on the hot path.** Worth doing and not exclusive — but the
  contention is paid by every allocation in the process, and the codebase-wide
  churn is not one change away.
