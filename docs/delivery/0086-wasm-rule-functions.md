---
adr: "0086"
title: "Rule functions in a WebAssembly sandbox: the real jq first, operator plugins on the same mechanism"
adr_status: Accepted
tasks:
  - id: 0086-T1
    title: "The sandbox crate and the jq module's build: loader fuzzing, the module built in CI from pinned sources and compared with the recorded hash, the EMQX oracle test in CI"
    status: in-progress
    issue: 905
    notes: "PR 904 carries all of it: the crate on wasmtime 49.0.2 with its limits, tests and failure mode; the `wasm sandbox` CI job, which builds jq.wasm with a compiler and sources pinned by sha256, compares it with jq.wasm.sha256, runs the 285-case EMQX oracle and the limit tests in release mode and checks the refusal under the shipped unit's hardening; and the `module_load` fuzz target in the nightly tier (crates/mqtt-wasm-sandbox/fuzz; 71,000 inputs on two hosts before the PR, no finding). The release build embedding the module is T2's, where the broker first links the crate."
  - id: 0086-T2
    title: "jq/2 and jq/3 in the rule engine behind the ABI: the instance pool, the worker hand-off, the per-message budget, grants, the off-by-default switch and its load errors, the module embedded by the release build, metrics and trace, --rule-test"
    status: planned
    issue: 906
    notes: "Follows T1."
  - id: 0086-T3
    title: "Operator plugins: [functions.<name>] in the rules file, pinned by sha256, load-time checks, live reload, the admin view, worked examples in Rust and C"
    status: planned
    issue: 907
    notes: "Follows T2, and may follow it at a distance: the ABI is the same either way."
  - id: 0086-T4
    title: "Documentation: RULES.md (the function, its limits, its cost), the departures from EMQX, threat model and hardening rows, the cookbook"
    status: planned
    issue: 908
    notes: "Follows T2."
---

# Delivery 0086 — rule functions in a WebAssembly sandbox

**ADR:** [docs/adr/0086-wasm-rule-functions.md](../adr/0086-wasm-rule-functions.md)

The plan, progress, and changelog for ADR 0086. Task status lives in the frontmatter
above; the table below is generated from it.

The ADR was accepted on 2026-10-11. Decided with it: the runtime is wasmtime, chosen for
speed; `$ENV` inside jq is empty unless the operator lists variables; the jq module is
built in the pipeline from pinned sources, not committed; libm's last-digit differences
are documented, not chased; `jq` is off unless the operator sets `rules.jq = true`, and the
shipped systemd unit keeps `MemoryDenyWriteExecute=true`, which that operator sets to
`false`.

What exists: the crate `crates/mqtt-wasm-sandbox` on wasmtime (nothing in the broker
depends on it), the jq module's build scripts, the comparison with EMQX 6.3.1, the
measurements in the ADR, a CI job that builds the module from pinned sources and runs
the comparison, and a fuzz target for the loader in the nightly tier. That is T1. T2 to
T4 are planned, each with its issue.

## Plan

| Task | Acceptance |
|---|---|
| **0086-T1** Sandbox and module build (D1, D2, D11, D13) | A fuzz target feeds the loader arbitrary bytes and arbitrary valid modules without a panic, a hang or memory growth past the limits; CI builds `jq.wasm` from the pinned sources with a compiler pinned by sha256 and fails on a hash other than `jq.wasm.sha256`; the oracle test (285 cases against EMQX 6.3.1) and the limit tests run in CI; a host that forbids executable memory gets an error, checked in CI under the shipped systemd unit's hardening. |
| **0086-T2** `jq` (D3–D8, D10, D13) | `jq/2` and `jq/3` evaluate in the broker and in `mqttd --rule-test` with the oracle's results; the release build embeds the module it built and the build-twice gate covers it; `rules.jq` is off by default and rules that call `jq` then fail to load naming the switch; with it on, they fail to load with the reason where compiled code cannot run, naming `MemoryDenyWriteExecute=false`; the systemd unit carries a comment next to that line; the workspace's declared Rust version follows the runtime's; `jq` is no longer refused at load; a ten-second jq on one connection does not delay another connection's publish; a call's output is charged to the message's build budget; `$ENV` holds what the operator listed and nothing else; the recorded differences from EMQX (libm's last digit, the limits) are in RULES.md, and `strptime %z` and `strftime %Z` are closed or recorded; per-function metrics are exported. |
| **0086-T3** Operator plugins (D9) | A rules file declares a module by path and sha256; a wrong hash, a missing export, another ABI version, a forbidden import, or a name that collides with a built-in or an EMQX function fails the load with the reason; a live edit swaps a module without dropping in-flight evaluations; the admin API lists modules, functions, limits and instance memory; a Rust and a C example build and run in CI. |
| **0086-T4** Documentation (D11, D12) | RULES.md documents `jq`, its limits and its cost, and the `[functions]` table; "Differences from EMQX" lists the limits, the environment default and the plugin mechanism; THREAT-MODEL.md has rows for a hostile payload driving jq, a hostile or broken plugin, and the module's supply chain; HARDENING.md says when to leave the feature out. |

## Progress

<!-- status-table:0086 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0086-T1 | 🚧 in-progress | [#905](https://github.com/mbilling/fss-mqtt-broker/issues/905) | — | "PR 904 carries all of it: the crate on wasmtime 49.0.2 with its limits, tests and failure mode; the `wasm sandbox` CI job, which builds jq.wasm with a compiler and sources pinned by sha256, compares it with jq.wasm.sha256, runs the 285-case EMQX oracle and the limit tests in release mode and checks the refusal under the shipped unit's hardening; and the `module_load` fuzz target in the nightly tier (crates/mqtt-wasm-sandbox/fuzz; 71,000 inputs on two hosts before the PR, no finding). The release build embedding the module is T2's, where the broker first links the crate." |
| 0086-T2 | ⬜ planned | [#906](https://github.com/mbilling/fss-mqtt-broker/issues/906) | — | "Follows T1." |
| 0086-T3 | ⬜ planned | [#907](https://github.com/mbilling/fss-mqtt-broker/issues/907) | — | "Follows T2, and may follow it at a distance: the ABI is the same either way." |
| 0086-T4 | ⬜ planned | [#908](https://github.com/mbilling/fss-mqtt-broker/issues/908) | — | "Follows T2." |
<!-- /status-table:0086 -->

## Changelog

- 2026-10-10 — Spike: jq 1.8.1 (EMQX's fork commit) builds to a 984,650-byte
  `wasm32-wasi` module with zig 0.16.0, byte-identical on three hosts; runs under wasmi
  2.0.0 with time, memory, output and stack limits; 278 of 285 cases identical to EMQX
  6.3.1 byte for byte (the six differences are libm's last digit and two time formats;
  one case takes the EMQX node down and is answered here); 15–26× slower than native jq.
  ADR proposed with the plugin-layer design.
- 2026-10-11 — Runtime decided: **wasmtime, for speed**. The crate moved from wasmi 2.0.0 to
  wasmtime 49.0.2 (Cranelift; features `runtime`, `cranelift`, `async`, `pulley`, `std`; the
  safe API only) and everything was measured again: a `jq` call on a 1 KB payload takes
  27–102 µs (wasmi 247–1,185 µs; native 14–45 µs); the module compiles in 0.56–0.81 s once
  per process, when rules first name `jq`; the 285 oracle cases and the 45 marshalling
  cases give the same results as under wasmi, also as static musl binaries on arm64 and
  amd64 and in wasmtime's interpreter. Deadlines by epoch interruption with a timer thread
  that sleeps until the next deadline; calls on a fiber of their own, so no caller's stack
  can be overrun; memory, output and stack limits as before. Compiled code is refused,
  with an error, where executable memory is forbidden, which includes the shipped systemd
  unit's `MemoryDenyWriteExecute=true`; `Execution::Interpreted` (Pulley, 25× slower) runs
  there. `deny.toml` allows `Apache-2.0 WITH LLVM-exception`; the lockfile grows by 54
  entries; a static release binary by 6.0 MB (aarch64) or 9.2 MB (x86_64). The release
  recipe builds it reproducibly (two clean builds, identical bytes).
- 2026-10-11 — T1's CI part: the `wasm sandbox` job in `ci.yml` builds `jq.wasm` from
  sources and a zig pinned by sha256 (`jq-module/fetch-zig.sh`), compares it with
  `jq.wasm.sha256`, runs the crate's tests with the module in release mode, and checks the
  failure mode under the shipped unit's hardening with `systemd-run`. Also decided:
  `$ENV` empty unless listed, the module built in the pipeline, libm documented. T1 stays
  open for a coverage-guided fuzz target; embedding the module in the release build moved
  to T2.
- 2026-10-11 — ADR accepted. Issues opened for the four tasks (#905 to #908). T1's last
  item: `module_load`, a cargo-fuzz target in `crates/mqtt-wasm-sandbox/fuzz`, run nightly
  with the other targets. An input is a module file, or the body of `mqttd_call` inside a
  module that is otherwise whole (the ABI's exports and seven of the host's imports), so
  the fuzzer reaches code that runs; compiled or interpreted, bounded by fuel or by the
  deadline alone. The target asserts that a refused module is never blamed on the host,
  that an instance is poisoned exactly when a call ended in anything but the function's
  own answer, and that memory and output stay under their caps. Run before the PR: 55,522
  inputs in five minutes on macOS arm64 and 15,909 in four on Linux arm64 (under
  AddressSanitizer; with LeakSanitizer on Linux), no finding. Its first seeds found one
  defect, fixed with it: with `Limits::max_fuel` set, making an instance was charged to
  that fuel, and the runtime bills 16,385 units for it compiled and 65,537 interpreted,
  so a smaller bound refused every module. Making an instance is now bounded by
  `init_timeout` alone (test `a_small_fuel_bound_still_loads_and_runs_a_module`).
