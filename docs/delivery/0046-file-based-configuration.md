---
adr: "0046"
title: "File-based configuration (layered over env, hot-reloadable, GitOps-friendly)"
adr_status: Proposed
tasks:
  - id: 0046-T1
    title: TOML config schema + parser — sections mirroring the MQTTD_* env groups; strict (unknown keys rejected, types/ranges checked); reuses the hardened ACL TOML parsing posture
    status: planned
  - id: 0046-T2
    title: Layering + precedence — defaults < config file < env vars < flags; --config path and MQTTD_CONFIG; effective config logged at startup with secrets redacted; a test asserts every MQTTD_* var maps to exactly one config key and vice versa
    status: planned
  - id: 0046-T3
    title: mqttd --check-config subcommand — validates a file and exits without binding ports; for GitOps CI and pre-rollout operator checks; clear located errors
    status: planned
  - id: 0046-T4
    title: Whole-config hot reload — SIGHUP/watch reloads the full config through the ADR 0032 validate-before-swap path; live-swappable settings change without restart, non-live ones logged as requires-restart; audited + metered
    status: planned
  - id: 0046-T5
    title: Secrets by reference (paths only, never inlined); docs (README config section + example file) and the container image documenting file + env paths
    status: planned
---

# Delivery: ADR 0046 — File-based configuration

[ADR 0046](../adr/0046-file-based-configuration.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0046 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0046-T1 | ⬜ planned | — |  |
| 0046-T2 | ⬜ planned | — |  |
| 0046-T3 | ⬜ planned | — |  |
| 0046-T4 | ⬜ planned | — |  |
| 0046-T5 | ⬜ planned | — |  |
<!-- /status-table:0046 -->

## Plan

| Task | Done means |
|---|---|
| **0046-T1** Schema + parser | A strict TOML schema covering every current `MQTTD_*` setting, parsed with unknown-key rejection and type/range validation. |
| **0046-T2** Precedence | defaults < file < env < flags, effective config logged (redacted); a CI test proves the env↔config key mapping is total and bijective. |
| **0046-T3** `--check-config` | A file can be validated to a clear pass/located-error without starting the broker. |
| **0046-T4** Hot reload | Editing the file + `SIGHUP` reloads the whole config validate-before-swap; a bad edit is rejected and the running config kept; reloads audited. |
| **0046-T5** Secrets + docs | Secret material is referenced by path (safe to commit/mount); README + an example config document both file and env paths. |

Order: T1 → T2 → T3/T4 (parallel) → T5.

## Changelog

- **2026-07-17** — ADR 0046 drafted. Adoption enabler: env-var-only config does not survive
  GitOps/Kubernetes. Reuses the ADR 0032/0033 validate-before-swap reload machinery.
  Priority **P1**.
