---
adr: "0079"
title: "mqttd links mimalloc as its global allocator"
adr_status: Accepted
tasks:
  - id: 0079-T1
    title: "mimalloc as mqttd's #[global_allocator]; the release build and cargo deny still pass"
    status: done
    issue: 683
    date: 2026-09-28
    evidence: "PR #686. #[global_allocator] static GLOBAL: mimalloc::MiMalloc in crates/mqttd/src/main.rs (binary only; default features, so the C malloc aws-lc uses is not overridden); mimalloc as a workspace dependency (MIT). The reproducible musl build (build-repro.sh, zig cc) links it — MIMALLOC_VERBOSE=1 mqttd --version prints mimalloc v3.3.2 at process init; cargo test -p mqttd, clippy -D warnings, cargo deny (supply-chain audit) and the FIPS variant pass in CI."
  - id: 0079-T2
    title: "Evidence — per-op commit cost and RSS on the shipped binary, on the calibration shape"
    status: planned
    issue: 684
    notes: "Measured in the same paid run as ADR 0078 T4: the durable writer's ms/op against the musl-allocator run of 2026-09-28 (~0.024 ms/op), plus idle and post-burst RSS."
---

# Delivery: ADR 0079 — mqttd links mimalloc as its global allocator

[ADR 0079](../adr/0079-global-allocator.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0079 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0079-T1 | ✅ done | [#683](https://github.com/mbilling/fss-mqtt-broker/issues/683) | 2026-09-28 | "PR #686. #[global_allocator] static GLOBAL: mimalloc::MiMalloc in crates/mqttd/src/main.rs (binary only; default features, so the C malloc aws-lc uses is not overridden); mimalloc as a workspace dependency (MIT). The reproducible musl build (build-repro.sh, zig cc) links it — MIMALLOC_VERBOSE=1 mqttd --version prints mimalloc v3.3.2 at process init; cargo test -p mqttd, clippy -D warnings, cargo deny (supply-chain audit) and the FIPS variant pass in CI." |
| 0079-T2 | ⬜ planned | [#684](https://github.com/mbilling/fss-mqtt-broker/issues/684) | — | "Measured in the same paid run as ADR 0078 T4: the durable writer's ms/op against the musl-allocator run of 2026-09-28 (~0.024 ms/op), plus idle and post-burst RSS." |
<!-- /status-table:0079 -->

## Plan

1. **T1 — the allocator.** Two lines in `crates/mqttd/src/main.rs` and one
   dependency; the reproducible musl build (`scripts/release/build-repro.sh`)
   and `cargo deny check` pass unchanged.
2. **T2 — the evidence**, on the ADR 0078 calibration shape, in the same paid
   run as 0078-T4: the writer's time per op, and RSS at idle and after the
   heaviest rung.

## Changelog

- 2026-09-28 — ADR written from the local measurement (the table in the ADR);
  T1 and T2 opened.
- 2026-09-28 — T1 in review: `#[global_allocator]` in `crates/mqttd/src/main.rs`,
  `mimalloc` as a workspace dependency.
- 2026-09-28 — T1 done: PR #686 merged, issue #683 closed. T2 (the evidence)
  rides the next paid calibration.
