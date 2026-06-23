# ADR 0024 — Deterministic testing: inject time, synchronize causally, gate in CI

- **Status:** Accepted
- **Date:** 2026-06-23
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0024-deterministic-testing.md](../delivery/0024-deterministic-testing.md) — plan, progress, and changelog
- **Related:** [ADR 0009](0009-mqtt5-expiry.md) (the absolute message-expiry deadlines the
  wall-clock seam serves), [ADR 0020](0020-metrics-and-observability.md) (the sweep/gauge
  refresh now tested under virtual time), [ADR 0018](0018-on-disk-persistence.md) (the redb
  recovery whose latency drove one CI margin), [ADR 0016](0016-swim-membership-stability.md),
  [ADR 0022](0022-signed-gossip.md), [ADR 0023](0023-gossip-anti-replay.md) (the test-first
  security bar this generalizes)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0024-deterministic-testing.md).

## Context

The suite mixes pure unit tests — driven over in-memory channels and duplex streams — with
end-to-end integration tests over real TCP/UDP loopback, plus a multi-node cluster. Several
tests synchronized on **real wall-clock time**: `sleep(N)` then assert, or poll a real-time
deadline. That made them flaky on a loaded CI runner — the *same commit* could pass, then
fail with only the runner's scheduling changed. Three distinct failure shapes appeared:

1. **Ordering races dressed as timing.** Two events reach a single FIFO consumer (the hub
   command channel; a receiver's replay window) in an order the test assumed but did not
   enforce, with a fixed `sleep` between them as a guess. A QoS-0 retained publish "stored"
   via a sleep before a subscribe; a gossip datagram "processed" via a sleep before its
   replay.
2. **Real-work-under-load timeouts.** A test deadline (2s) shorter than the actual work —
   off-loop durable-session recovery (redb reopen + offline-queue replay, ADR 0017/0018) —
   when the runner is CPU-starved.
3. **Timer logic tested in real time.** Waiting real seconds for the 1s session-expiry sweep
   or the 1.5× keepalive grace to elapse: slow, and margin-sensitive.

tokio's paused-time test clock virtualizes timers (`sleep`, `interval`, `tokio::time::Instant`)
but **not** `SystemTime`, so the absolute message-expiry deadlines (ADR 0009 §3) had no test
seam at all — expiry could only be probed with degenerate intervals (`0` = instantly stale,
`3600` = always fresh), never the real "queued, interval elapses, now dropped" path.

## Decision

Make tests deterministic by **controlling the inputs that vary** — time and event ordering —
chosen per layer, and gate the result in CI with a diagnostic that survives not having
repo-admin access to the job logs.

### 1. Inject time; do not wait on it

- **Timer-driven logic** (the expiry sweep, keepalive grace, the metrics refresh) is
  unit-tested under `#[tokio::test(start_paused = true)]`: the runtime drains all ready work,
  then auto-advances the virtual clock to the next timer. `tokio::time::sleep(d)` becomes an
  instant, deterministic advance — no polling, no deadline. The formerly real-time
  gauge-refresh and keepalive tests now run in microseconds.
- **Wall-clock logic** (absolute epoch-second deadlines, ADR 0009 §3) reads through an
  injectable `Clock` (`mqttd::clock`): `SystemClock` in production, a settable `TestClock` in
  tests. The hub holds `clock: Arc<dyn Clock>` (default system, overridable via
  `attach_clock`); message expiry now exercises the real `now + interval` / `now ≥ deadline`
  arithmetic with time advancing on command.
- **Monotonic waits keep using tokio's clock** — no abstraction. They are already virtualized
  by paused time, so a second seam would be redundant.

### 2. Synchronize on events, not on the clock

In the real-TCP/UDP integration tests — where paused time and real I/O interplay badly — a
test must never `sleep` to let something "propagate" and then assert. It waits on the
**observable event** that the thing happened:

- **A causal barrier** where a positive completion signal exists: await the PUBACK that proves
  a retained publish was enqueued before subscribing; await a peer's Ack that proves a gossip
  datagram was processed (its sequence recorded) before replaying it. Ordering becomes
  guaranteed, not timed.
- **Bounded poll-retry** against the actual condition for eventually-consistent state
  (membership gossip, durable quorum commit, "is it listening yet"): retry until true or a
  deadline. This is already the prevailing pattern (`wait_until`, `wait_for_kind`, the durable
  retry loops) and is the default for cross-node convergence.

### 3. Real-time waits only where the semantics *are* time, with margin

A few integration assertions are inherently about time elapsing — a session expires after its
interval, an idle client is reaped after the keepalive grace — and cannot be probed without
disturbing the system (reconnecting to check expiry *cancels* it, ADR 0009). These keep a
fixed wait, sized generously above the worst case so a slow runner does not trip them; the
underlying logic is covered deterministically by the virtual-time unit tests (§1), so the
integration test only confirms the end-to-end wire path. The shared receive timeout is
likewise a generous **liveness backstop**, not a synchronization mechanism.

### 4. One gate, and make failures self-describing

CI runs the full gate on every push and PR: `cargo fmt --check`, `cargo clippy
--all-targets --all-features` under `RUSTFLAGS="-D warnings"`, `cargo test --all`,
`cargo-deny`, `cargo-audit`, and `gen-status.py --check` (the dashboard cannot drift). The
Test step runs `--no-fail-fast` and re-emits every failing test name and panic site as
`::error::` workflow annotations. GitHub's job-log download requires repo-admin even on a
public repo, but **annotations are readable from the check-run annotations API without it** —
so an intermittent failure names itself (which test, which `file:line`) instead of having to
be reproduced blind. That is how the second flaky test in this very effort was identified.

## Consequences

- **Good:** timer and expiry logic is tested instantly and deterministically; ordering races
  are fixed at the cause (a barrier) rather than papered over with a longer sleep; the
  remaining real-time waits are explicit and margined; and a CI flake is diagnosable from API
  data alone. The split is principled — virtual/injected time and causal sync at the unit
  layer; causal sync and bounded poll-retry at the integration layer; generous fixed waits
  only for genuine time semantics.
- **Cost:** a small `Clock` seam threaded through the hub; tests must reason about ordering (a
  synchronous `TestClock::advance` can still outrun an async enqueue — a FIFO-flush barrier is
  needed, the same discipline as the production fixes); `--no-fail-fast` runs the whole suite
  on failure.
- **Risk:** low and test-only — production reads time through `SystemClock`/tokio exactly as
  before. The real risk it *removes* is a flaky gate that erodes trust and hides regressions.

## Alternatives considered

- **Paused time everywhere, including the real-TCP tests.** Rejected: mixing virtual time with
  real socket I/O is fragile — real I/O readiness does not interplay cleanly with the
  auto-advance, risking hangs. Virtual time is for the channel/in-memory layer; integration
  synchronizes causally.
- **Just widen every sleep.** Rejected as the general answer: it slows the suite and only
  *reduces* — never removes — ordering-race flakiness, because the race is causal, not
  durational. Generous fixed waits are reserved for genuine time semantics (§3).
- **A full deterministic-simulation harness** (madsim/turmoil-style: deterministic scheduler +
  simulated network + virtual clock, reproducible from a seed). The gold standard for
  *distributed* ordering races, recorded as the ambitious end-state (`0024-T7`, deferred). It
  is a large investment; the per-test causal barriers and bounded poll-retry close the flakes
  seen today without it.
- **Retry the whole Test step on failure.** Rejected: it masks real failures and reruns the
  entire suite; the annotation diagnostic plus fixing the causes is the durable path.
