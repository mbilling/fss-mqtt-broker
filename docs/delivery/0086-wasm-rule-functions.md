---
adr: "0086"
title: "Rule functions in a WebAssembly sandbox: the real jq first, operator plugins on the same mechanism"
adr_status: Proposed
tasks:
  - id: 0086-T1
    title: "The sandbox crate and the jq module's build: loader fuzzing, the module built in CI from pinned sources and compared with the recorded hash, the EMQX oracle test in CI"
    status: deferred
    notes: "Waits for the decision on ADR 0086 (Proposed). The spike's prototype is crates/mqtt-wasm-sandbox; an issue is opened when the ADR is accepted."
  - id: 0086-T2
    title: "jq/2 and jq/3 in the rule engine behind the ABI: the instance pool, the worker hand-off, the per-message budget, grants, metrics and trace, --rule-test"
    status: deferred
    notes: "Waits for the decision on ADR 0086 (Proposed), then for T1."
  - id: 0086-T3
    title: "Operator plugins: [functions.<name>] in the rules file, pinned by sha256, load-time checks, live reload, the admin view, worked examples in Rust and C"
    status: deferred
    notes: "Waits for the decision on ADR 0086 (Proposed), then for T2. May follow T2 at a distance: the ABI is the same either way."
  - id: 0086-T4
    title: "Documentation: RULES.md (the function, its limits, its cost), the departures from EMQX, threat model and hardening rows, the cookbook"
    status: deferred
    notes: "Waits for the decision on ADR 0086 (Proposed), then for T2."
---

# Delivery 0086 — rule functions in a WebAssembly sandbox

**ADR:** [docs/adr/0086-wasm-rule-functions.md](../adr/0086-wasm-rule-functions.md)

The plan, progress, and changelog for ADR 0086. Task status lives in the frontmatter
above; the table below is generated from it.

The ADR is Proposed. What exists is the spike: the prototype crate
`crates/mqtt-wasm-sandbox` (nothing in the broker depends on it), the jq module's build
script, the comparison with EMQX 6.3.1, and the measurements in the ADR. Every task is
deferred until the ADR is decided.

## Plan

| Task | Acceptance |
|---|---|
| **0086-T1** Sandbox and module build (D1, D2, D11) | A fuzz target feeds the loader arbitrary bytes and arbitrary valid modules without a panic, a hang or memory growth past the limits; CI builds `jq.wasm` from the pinned sources and fails on a hash other than `jq.wasm.sha256`; the oracle test (285 cases against EMQX 6.3.1) and the limit tests run in CI; the release build embeds the module it built and the build-twice gate covers it. |
| **0086-T2** `jq` (D3–D8, D10) | `jq/2` and `jq/3` evaluate in the broker and in `mqttd --rule-test` with the oracle's results; `jq` is no longer refused at load; a ten-second jq on one connection does not delay another connection's publish; a call's output is charged to the message's build budget; `$ENV` holds what the operator listed and nothing else; the recorded differences from EMQX (libm's last digit, the limits) are in RULES.md, and `strptime %z` and `strftime %Z` are closed or recorded; per-function metrics are exported. |
| **0086-T3** Operator plugins (D9) | A rules file declares a module by path and sha256; a wrong hash, a missing export, another ABI version, a forbidden import, or a name that collides with a built-in or an EMQX function fails the load with the reason; a live edit swaps a module without dropping in-flight evaluations; the admin API lists modules, functions, limits and instance memory; a Rust and a C example build and run in CI. |
| **0086-T4** Documentation (D11, D12) | RULES.md documents `jq`, its limits and its cost, and the `[functions]` table; "Differences from EMQX" lists the limits, the environment default and the plugin mechanism; THREAT-MODEL.md has rows for a hostile payload driving jq, a hostile or broken plugin, and the module's supply chain; HARDENING.md says when to leave the feature out. |

## Progress

<!-- status-table:0086 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0086-T1 | 💤 deferred | — | — | "Waits for the decision on ADR 0086 (Proposed). The spike's prototype is crates/mqtt-wasm-sandbox; an issue is opened when the ADR is accepted." |
| 0086-T2 | 💤 deferred | — | — | "Waits for the decision on ADR 0086 (Proposed), then for T1." |
| 0086-T3 | 💤 deferred | — | — | "Waits for the decision on ADR 0086 (Proposed), then for T2. May follow T2 at a distance: the ABI is the same either way." |
| 0086-T4 | 💤 deferred | — | — | "Waits for the decision on ADR 0086 (Proposed), then for T2." |
<!-- /status-table:0086 -->

## Changelog

- 2026-10-10 — Spike: jq 1.8.1 (EMQX's fork commit) builds to a 984,650-byte
  `wasm32-wasi` module with zig 0.16.0, byte-identical on three hosts; runs under wasmi
  2.0.0 with time, memory, output and stack limits; 278 of 285 cases identical to EMQX
  6.3.1 byte for byte (the six differences are libm's last digit and two time formats;
  one case takes the EMQX node down and is answered here); 15–26× slower than native jq.
  ADR proposed with the plugin-layer design.
