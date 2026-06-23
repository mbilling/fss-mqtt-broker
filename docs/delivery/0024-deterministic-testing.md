---
adr: "0024"
title: "Deterministic testing: inject time, synchronize causally, gate in CI"
adr_status: Accepted
tasks:
  - id: 0024-T1
    title: CI gate — fmt, clippy -D warnings, test --all, cargo-deny, cargo-audit, gen-status --check
    status: done
    date: 2026-06-22
    evidence: .github/workflows/ci.yml three jobs (build/test/lint with RUSTFLAGS=-D warnings; delivery dashboard runs gen-status.py --check so STATUS.md cannot drift; supply-chain runs cargo deny + cargo audit)
  - id: 0024-T2
    title: Self-describing Test step — no-fail-fast plus failing test/panic re-emitted as ::error:: annotations
    status: done
    date: 2026-06-22
    evidence: ci.yml Test step tees output and re-emits ^test ... FAILED and 'panicked at' lines as ::error:: workflow commands, readable via the check-run annotations API without repo-admin; identified retained_message_replicates_across_nodes from the API alone
  - id: 0024-T3
    title: Virtual time (tokio start_paused) for timer-driven unit tests — expiry sweep, metrics refresh, keepalive
    status: done
    date: 2026-06-23
    evidence: hub session_expiry_finite_retains_then_expires + gauge_refresh_snapshots_sessions_and_subscriptions (start_paused, drains commands before auto-advancing the sweep); conn idle_connection_is_closed_after_keepalive_grace over an in-memory duplex; all run in ~0s, no polling/deadline
  - id: 0024-T4
    title: Injectable Clock seam for wall-clock epoch seconds; deterministic message-expiry tests
    status: done
    date: 2026-06-23
    evidence: crates/mqttd/src/clock.rs Clock trait + SystemClock + system_clock(); Hub holds clock Arc<dyn Clock> with attach_clock; queued_message_expires_once_its_interval_elapses and the survives-while-fresh companion drive a TestClock (advance only), with a FIFO-flush barrier so the synchronous advance cannot outrun the async enqueue
  - id: 0024-T5
    title: Causal synchronization in real-TCP/UDP integration tests — barriers and bounded poll-retry, not sleeps
    status: done
    date: 2026-06-23
    evidence: publish_retained_acked (QoS1 + PUBACK) barrier removes the store-vs-subscribe race in cluster_chaos + v5_protocol; swim_cluster a_replayed_v3_datagram_is_dropped awaits the fresh Ping's Ack before replaying; bounded poll-retry confirmed prevailing (wait_until / wait_for_kind / durable retry loops); each verified under 8-core saturation
  - id: 0024-T6
    title: Generous CI margins for the inherently time-based integration waits (liveness backstops)
    status: done
    date: 2026-06-22
    evidence: common/mod.rs shared RECV_TIMEOUT 2s -> 10s (off-loop redb recovery under load); v5_protocol session-expiry e2e 2.5s -> 4s (sweep slips under load, cannot probe without cancelling expiry); conn connection-task joins 1s -> 10s
  - id: 0024-T7
    title: Deterministic simulation harness (madsim/turmoil-style) for seed-reproducible cluster ordering races
    status: deferred
    notes: the gold standard for distributed ordering races, but a large investment; per-test causal barriers (T5) and bounded poll-retry close the flakes seen today without it. Revisit if cluster-ordering flakes recur or a seed-reproducible failure is needed.
---

# Delivery — ADR 0024: Deterministic testing

Decision: [docs/adr/0024-deterministic-testing.md](../adr/0024-deterministic-testing.md).

Control the inputs that vary — time and event ordering — chosen per test layer, and gate the
result in CI with a diagnostic that works without repo-admin log access. This ADR was written
after the practice was applied; the tasks below record that work and the one deferred
end-state.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0024-T1** CI gate | Every push/PR runs `fmt --check`, `clippy --all-targets --all-features` under `-D warnings`, `test --all`, `cargo-deny`, `cargo-audit`, and `gen-status.py --check`, across the three workflow jobs. |
| **0024-T2** Self-describing failures | The Test step runs `--no-fail-fast` and re-emits each failing test and panic site as a `::error::` annotation, so a flake names itself via the check-run annotations API (no admin needed). |
| **0024-T3** Virtual time | The timer-driven unit tests (expiry sweep, metrics refresh, keepalive grace) run under `start_paused`, advancing virtual time deterministically instead of polling/sleeping real time. |
| **0024-T4** Clock seam | Wall-clock epoch-second reads go through an injectable `Clock` (`SystemClock` prod, `TestClock` test); message-expiry deadlines are tested with time advancing on command — not the `expiry=0`/`3600` shortcuts. |
| **0024-T5** Causal sync | Integration tests synchronize on observable events — a causal barrier (PUBACK/Ack) where a completion signal exists, bounded poll-retry for eventual consistency — never a fixed `sleep`-then-assert. |
| **0024-T6** Margined backstops | The inherently time-based integration waits (and the shared receive timeout) are sized generously above the worst case, as liveness backstops, so a loaded runner does not trip them. |
| **0024-T7** Simulation harness | A deterministic scheduler + simulated network + virtual clock, reproducible from a seed, for distributed ordering races. Deferred. |

## Progress

<!-- status-table:0024 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0024-T1 | ✅ done | 2026-06-22 | .github/workflows/ci.yml three jobs (build/test/lint with RUSTFLAGS=-D warnings; delivery dashboard runs gen-status.py --check so STATUS.md cannot drift; supply-chain runs cargo deny + cargo audit) |
| 0024-T2 | ✅ done | 2026-06-22 | ci.yml Test step tees output and re-emits ^test ... FAILED and 'panicked at' lines as ::error:: workflow commands, readable via the check-run annotations API without repo-admin; identified retained_message_replicates_across_nodes from the API alone |
| 0024-T3 | ✅ done | 2026-06-23 | hub session_expiry_finite_retains_then_expires + gauge_refresh_snapshots_sessions_and_subscriptions (start_paused, drains commands before auto-advancing the sweep); conn idle_connection_is_closed_after_keepalive_grace over an in-memory duplex; all run in ~0s, no polling/deadline |
| 0024-T4 | ✅ done | 2026-06-23 | crates/mqttd/src/clock.rs Clock trait + SystemClock + system_clock(); Hub holds clock Arc<dyn Clock> with attach_clock; queued_message_expires_once_its_interval_elapses and the survives-while-fresh companion drive a TestClock (advance only), with a FIFO-flush barrier so the synchronous advance cannot outrun the async enqueue |
| 0024-T5 | ✅ done | 2026-06-23 | publish_retained_acked (QoS1 + PUBACK) barrier removes the store-vs-subscribe race in cluster_chaos + v5_protocol; swim_cluster a_replayed_v3_datagram_is_dropped awaits the fresh Ping's Ack before replaying; bounded poll-retry confirmed prevailing (wait_until / wait_for_kind / durable retry loops); each verified under 8-core saturation |
| 0024-T6 | ✅ done | 2026-06-22 | common/mod.rs shared RECV_TIMEOUT 2s -> 10s (off-loop redb recovery under load); v5_protocol session-expiry e2e 2.5s -> 4s (sweep slips under load, cannot probe without cancelling expiry); conn connection-task joins 1s -> 10s |
| 0024-T7 | 💤 deferred | — | the gold standard for distributed ordering races, but a large investment; per-test causal barriers (T5) and bounded poll-retry close the flakes seen today without it. Revisit if cluster-ordering flakes recur or a seed-reproducible failure is needed. |
<!-- /status-table:0024 -->

## Changelog

- **2026-06-23** — ADR ratified after the practice was applied across the suite. T1/T2/T6
  (CI gate, self-describing failures, margined backstops) landed during the CI-stabilization
  pass; T3/T4/T5 (virtual time, the `Clock` seam, causal synchronization) landed in the
  determinism pass. T7 (full simulation harness) recorded as the deferred end-state.
