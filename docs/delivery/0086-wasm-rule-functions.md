---
adr: "0086"
title: "Rule functions in a WebAssembly sandbox: the real jq first, operator plugins on the same mechanism"
adr_status: Accepted
tasks:
  - id: 0086-T1
    title: "The sandbox crate and the jq module's build: loader fuzzing, the module built in CI from pinned sources and compared with the recorded hash, the EMQX oracle test in CI"
    status: done
    issue: 905
    date: 2026-10-11
    evidence: "PR 904: the crate on wasmtime 49.0.2 with its limits, tests and failure mode; the `wasm sandbox` CI job, which builds jq.wasm with a compiler and sources pinned by sha256, compares it with jq.wasm.sha256, runs the 285-case EMQX oracle and the limit tests in release mode and checks the refusal under the shipped unit's hardening; and the `module_load` fuzz target in the nightly tier (crates/mqtt-wasm-sandbox/fuzz; 71,000 inputs on two hosts before the PR, no finding). The release build embedding the module is T2's, where the broker first links the crate."
  - id: 0086-T2
    title: "jq/2 and jq/3 in the rule engine behind the ABI: the instance pool, the worker hand-off, the per-message budget, grants, the off-by-default switch and its load errors, the module embedded by the release build, metrics and trace, --rule-test"
    status: done
    issue: 906
    date: 2026-10-11
    evidence: "PR 911: `jq/2` and `jq/3` in `mqtt-rules` behind the ABI, with EMQX 6.3.1's results, tags and texts (79 probes of `emqx_rule_funcs:jq/2,3` and `emqx_rule_sqltester` on a local EMQX, all matched; the 285-case module oracle unchanged); `rules.jq` off by default with its load errors, `rules.jq_default_timeout_ms`, `rules.jq_env`; the instance pool, literal programs compiled at load; a call's output charged to the message's build budget; the hand-off (one worker thread, a ten-second jq on one connection: 6,974 publishes on another, median 152 µs, slowest 6.2 ms; without it the first is not answered in 2 s) and a Will's rules moved off the hub while rules call jq (ADR amendment); per-function and per-module metrics; `--rule-test`, `--check-rules`, `--check-config`; the module embedded by the build when present and required by the release recipe and CI; the systemd unit's comment; Rust 1.96. `strptime %z` and `strftime %Z` recorded, not closed."
  - id: 0086-T3
    title: "Operator plugins: [functions.<name>] in the rules file, pinned by sha256, load-time checks, live reload, the admin view, worked examples in Rust and C"
    status: in-progress
    issue: 907
    notes: "One PR carries it: `[functions.<name>]` in the rules file (`wasm`, `sha256`, `functions`, `timeout_ms`, `memory_mib`, `clock`, `env`, `fresh_instance`), loaded with the rules at boot, on a reload, through the admin API and in `--check-rules`; each load-time check a load error with its reason (hash, export, ABI version, import, a function the module does not describe, a name that is a built-in's or any EMQX function's); a module read only from under the rules file's directory, and its environment only from `rules.functions_env` (ADR amendment); limits and the output budget as for jq; a declaration that did not change keeps its module across a reload and a call in flight finishes on the old one (held in the rule engine's hand-off, and through the binary with a two-second call across an admin API write); `modules[]` in `GET /admin/v1/rules` and `mqttd --admin rules`; `--rule-test` with the configured file's modules; two examples, Rust and C, built in CI and called through the binary."
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

What exists: the crate `crates/mqtt-wasm-sandbox` on wasmtime, the jq module's build
scripts, the comparison with EMQX 6.3.1, the measurements in the ADR, a CI job that
builds the module from pinned sources and runs the comparison, and a fuzz target for the
loader in the nightly tier (T1); and `jq/2` and `jq/3` in the rule engine, off unless
`rules.jq = true`, with the release build embedding the module it built (T2). T3, the
modules a rules file declares, is in review; T4 is planned.

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
| 0086-T1 | ✅ done | [#905](https://github.com/mbilling/fss-mqtt-broker/issues/905) | 2026-10-11 | "PR 904: the crate on wasmtime 49.0.2 with its limits, tests and failure mode; the `wasm sandbox` CI job, which builds jq.wasm with a compiler and sources pinned by sha256, compares it with jq.wasm.sha256, runs the 285-case EMQX oracle and the limit tests in release mode and checks the refusal under the shipped unit's hardening; and the `module_load` fuzz target in the nightly tier (crates/mqtt-wasm-sandbox/fuzz; 71,000 inputs on two hosts before the PR, no finding). The release build embedding the module is T2's, where the broker first links the crate." |
| 0086-T2 | ✅ done | [#906](https://github.com/mbilling/fss-mqtt-broker/issues/906) | 2026-10-11 | "PR 911: `jq/2` and `jq/3` in `mqtt-rules` behind the ABI, with EMQX 6.3.1's results, tags and texts (79 probes of `emqx_rule_funcs:jq/2,3` and `emqx_rule_sqltester` on a local EMQX, all matched; the 285-case module oracle unchanged); `rules.jq` off by default with its load errors, `rules.jq_default_timeout_ms`, `rules.jq_env`; the instance pool, literal programs compiled at load; a call's output charged to the message's build budget; the hand-off (one worker thread, a ten-second jq on one connection: 6,974 publishes on another, median 152 µs, slowest 6.2 ms; without it the first is not answered in 2 s) and a Will's rules moved off the hub while rules call jq (ADR amendment); per-function and per-module metrics; `--rule-test`, `--check-rules`, `--check-config`; the module embedded by the build when present and required by the release recipe and CI; the systemd unit's comment; Rust 1.96. `strptime %z` and `strftime %Z` recorded, not closed." |
| 0086-T3 | 🚧 in-progress | [#907](https://github.com/mbilling/fss-mqtt-broker/issues/907) | — | "One PR carries it: `[functions.<name>]` in the rules file (`wasm`, `sha256`, `functions`, `timeout_ms`, `memory_mib`, `clock`, `env`, `fresh_instance`), loaded with the rules at boot, on a reload, through the admin API and in `--check-rules`; each load-time check a load error with its reason (hash, export, ABI version, import, a function the module does not describe, a name that is a built-in's or any EMQX function's); a module read only from under the rules file's directory, and its environment only from `rules.functions_env` (ADR amendment); limits and the output budget as for jq; a declaration that did not change keeps its module across a reload and a call in flight finishes on the old one (held in the rule engine's hand-off, and through the binary with a two-second call across an admin API write); `modules[]` in `GET /admin/v1/rules` and `mqttd --admin rules`; `--rule-test` with the configured file's modules; two examples, Rust and C, built in CI and called through the binary." |
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
- 2026-10-11 — T2: `jq/2` and `jq/3` in the rule engine, in the sandbox, off by default.
  - **The function.** `mqtt-rules` calls the module through `jq::Pool`; argument and
    result rules, error tags and texts, the timeout and its validation are
    `emqx_rule_funcs:jq/2,3`'s, probed on a local EMQX 6.3.1 (40 calls of the function,
    39 statements through `emqx_rule_sqltester`; every one matched). Found by probing and
    built as EMQX does it: an undefined input is the text `undefined`; the timeout is
    checked before the program; `'infinity'` is `bad_timeout_val` (a rule cannot write the
    atom); an integer of 2^64 or more is `{badmatch,{error,badarg}}`.
  - **The switch.** `rules.jq` (default `false`), `rules.jq_default_timeout_ms` (10000),
    `rules.jq_env` (empty); read at startup. Off: a rules file that calls `jq` fails to
    load naming the switch, and nothing of the runtime is made. On, on a host that forbids
    executable memory: it fails to load with the reason and `MemoryDenyWriteExecute=false`;
    CI runs the broker under the shipped unit's hardening and requires exactly that.
  - **The hand-off (D6), measured**, and one correction recorded as an ADR amendment: the
    hub evaluates a Will's rules (ADR 0083), so with rules that call `jq` loaded it hands
    them to a task of its own. One runtime worker thread, a ten-second jq on one
    connection: another connection completed 6,974 QoS 1 publishes meanwhile, median
    152 µs, slowest 6.2 ms; with the hand-off removed its first publish was unanswered
    after 2 s.
  - **Limits (D7).** A call's output is charged whole to the message's 1 MiB build budget;
    time, memory and stack as the sandbox has them; each a failed rule, counted by kind
    (`mqttd_rule_function_errors_total{function,kind}`) and reported with its text.
  - **Builds.** The sandbox crate's build script embeds `jq.wasm` when it finds it (after
    comparing its sha256 with the recorded one) and builds without it otherwise; `jq` then
    says it is not built in. `MQTTD_JQ_WASM_REQUIRED=1` (the release recipe, CI) makes its
    absence an error. `build-repro.sh` builds the module on every run under `target/`, so
    the build-twice gate covers it. Cargo feature `jq` (default) on `mqttd` and
    `mqtt-rules`; without it no runtime is linked.
  - **Cost.** Rust 1.96 for the workspace. A release `mqttd` (aarch64-apple-darwin):
    19.9 MB without the feature, 28.4 MB with it (stripped: 16.5 MB, 23.5 MB). Dev and test
    builds optimise the Cranelift crates, or every test process that loads jq rules would
    spend 6.5 s compiling the module instead of 0.6 s.
  - **Not closed:** `strptime %z` and `strftime %Z` (recorded in RULES.md with the libm
    digits). Concurrent long calls are bounded by the runtime's blocking threads, not by
    the worker count (ADR amendment): a cap is a decision for T3 or T4.
- 2026-10-11 — T3: functions an operator supplies as WebAssembly modules, declared in the
  rules file and pinned by hash.
  - **The table.** `[functions.<name>]` with `wasm`, `sha256` and `functions`, and
    optionally `timeout_ms` (100; at most 10000), `memory_mib` (16; at most 64), `clock`,
    `env` and `fresh_instance`. Declaring a module is the switch: no key in the broker's
    configuration turns modules on, `rules.jq` need not be set, and a broker whose rules
    declare none creates nothing of the runtime. An mqttd extension, in RULES.md's
    departures table.
  - **Load-time checks**, each a load error with its reason, the running rules kept: the
    path, the file's sha256 against the pin (on every load, so a file replaced on disk
    fails the next reload), a valid module importing only the sixteen provided functions,
    the ABI's exports and version, every named function described, and the names. A
    function may not have a built-in's name or that of any function of EMQX's rule
    engine: the 144 built-ins, plus the exports of `emqx_rule_funcs` in EMQX 6.3.1 that
    are not built in (read from the module's export table in the 6.3.1 image), the names
    it resolves without exporting, and two that earlier releases exported.
  - **Where a module may be read from** (ADR amendment). Only from under the directory of
    the configured rules file: a relative path of plain names ending in `.wasm`, still
    under that directory with every symlink resolved. The broker writes nothing there
    that is a module, so a rules writer of the admin API, or an operator checking text,
    can name only a file the operator placed, by a hash they must already know (the
    mismatch error does not state the file's). The environment a table may grant is
    bounded by a new key, `rules.functions_env` (`MQTTD_RULES_FUNCTIONS_ENV`), read at
    startup.
  - **The reload.** A rule set holds its modules; a declaration that did not change keeps
    its loaded module and warm instances, a changed one is loaded beside the old, and the
    old one goes with the last evaluation that started on it. Shown twice: in the rule
    engine, with a call suspended in the hand-off while the rules are replaced; and
    through the binary, where a publish whose call runs for two seconds is still
    running when `mqttd --admin rules-apply` pins another module, a second publisher's
    message is answered by the new module, and the first then returns the old module's
    answer and is acknowledged.
  - **The admin view.** `modules[]` in `GET /admin/v1/rules`: name, version, sha256,
    functions and arities, limits, grants, and instances with their memory; the path and
    the granted variables' names for an operator only; a second table in
    `mqttd --admin rules`. `mqttd --check-rules` lists what it loaded.
  - **Examples.** `examples/rule-plugins`: `geo` in Rust (`wasm32-unknown-unknown`, no
    dependencies; 70 kB) and `telemetry` in C (`zig cc -target wasm32-wasi`; 25 kB), with
    `build.sh`. The Rust one is outside the workspace because a guest needs `unsafe` to
    implement the ABI: three marked blocks and the exports, in guest code that runs in the
    sandbox. CI builds both (cached: a hit costs a cache restore) and the tests call
    them through the binary, grants included.
  - **Not changed:** the peer protocol (rules and modules are per-node files). **Not
    decided:** a cap on concurrent long calls, which T2 left to T3 or T4.
