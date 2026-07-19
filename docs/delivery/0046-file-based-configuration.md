---
adr: "0046"
title: "File-based configuration (layered over env, hot-reloadable, GitOps-friendly)"
adr_status: Proposed
tasks:
  - id: 0046-T1
    title: TOML config schema + parser — sections mirroring the MQTTD_* env groups; strict (unknown keys rejected, types/ranges checked); reuses the hardened ACL TOML parsing posture
    status: done
    date: 2026-07-19
    evidence: "mqtt-config: a typed Config grouped by concern (node/listeners/tls/security/cluster/durable/limits/observability/runtime) mirroring the full MQTTD_* surface; every table #[serde(deny_unknown_fields)] so a typo fails the load; secure defaults (durable on, anonymous off, mTLS required, TLS-only) match the env defaults; Config::from_toml parses strict + validates ranges/relations (lease_voters≥1, crl needs its ca, swim.signed/replay ∈ {require,off}, queue_overflow enum). 8 unit tests: defaults secure, full-TOML round-trip, unknown key/table rejected, type mismatch rejected, out-of-range rejected, crl-without-ca rejected, bad enum rejected. Additive — not yet wired into main.rs (T2). clippy -D warnings + fmt clean; mqttd still builds."
  - id: 0046-T2
    title: Layering + precedence — defaults < config file < env vars < flags; --config path and MQTTD_CONFIG; effective config logged at startup with secrets redacted; a test asserts every MQTTD_* var maps to exactly one config key and vice versa
    status: in-progress
    notes: "Layering engine landed (Config::load(path) → defaults → TOML file → MQTTD_* overlay → validate; overlay_from<F>(get) is the single env↔typed mapping, injectable so it is unit-testable without the process env; per-var boolean conventions honoured — MQTTD_ALLOW_ANONYMOUS presence = on, MQTTD_DURABLE_SESSIONS 0/false/off/no = off; comma-lists and MQTTD_FAILURE_DOMAINS node=domain map parsed; numeric vars fail with a located ConfigError::Invalid). 13 mqtt-config tests green (adds env-overlay-wins-over-file, per-var-boolean-conventions, comma-lists-and-domain-map, bad-numeric-located-error, unset-env-leaves-intact). Also corrected two T1 doc bugs found here: MQTTD_QUEUE_OVERFLOW is drop-oldest|reject-newest (not drop-new), JWT env vars are MQTTD_JWT_HS256_SECRET / MQTTD_JWT_RS256_PEM. Remaining before done: rewire main.rs's ~57 env reads to consume the typed Config (chosen approach: typed rewire, not an env-bridge), effective-config startup log with secrets redacted, and the env↔config bijection test."
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
| 0046-T1 | ✅ done | 2026-07-19 | "mqtt-config: a typed Config grouped by concern (node/listeners/tls/security/cluster/durable/limits/observability/runtime) mirroring the full MQTTD_* surface; every table #[serde(deny_unknown_fields)] so a typo fails the load; secure defaults (durable on, anonymous off, mTLS required, TLS-only) match the env defaults; Config::from_toml parses strict + validates ranges/relations (lease_voters≥1, crl needs its ca, swim.signed/replay ∈ {require,off}, queue_overflow enum). 8 unit tests: defaults secure, full-TOML round-trip, unknown key/table rejected, type mismatch rejected, out-of-range rejected, crl-without-ca rejected, bad enum rejected. Additive — not yet wired into main.rs (T2). clippy -D warnings + fmt clean; mqttd still builds." |
| 0046-T2 | 🚧 in-progress | — | "Layering engine landed (Config::load(path) → defaults → TOML file → MQTTD_* overlay → validate; overlay_from<F>(get) is the single env↔typed mapping, injectable so it is unit-testable without the process env; per-var boolean conventions honoured — MQTTD_ALLOW_ANONYMOUS presence = on, MQTTD_DURABLE_SESSIONS 0/false/off/no = off; comma-lists and MQTTD_FAILURE_DOMAINS node=domain map parsed; numeric vars fail with a located ConfigError::Invalid). 13 mqtt-config tests green (adds env-overlay-wins-over-file, per-var-boolean-conventions, comma-lists-and-domain-map, bad-numeric-located-error, unset-env-leaves-intact). Also corrected two T1 doc bugs found here: MQTTD_QUEUE_OVERFLOW is drop-oldest|reject-newest (not drop-new), JWT env vars are MQTTD_JWT_HS256_SECRET / MQTTD_JWT_RS256_PEM. Remaining before done: rewire main.rs's ~57 env reads to consume the typed Config (chosen approach: typed rewire, not an env-bridge), effective-config startup log with secrets redacted, and the env↔config bijection test." |
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
- **2026-07-19** — **T2 layering engine landed** (rewire pending, so T2 stays in-progress).
  `Config::load(path)` applies the ADR 0046 precedence — defaults → TOML file → `MQTTD_*`
  overlay → validate; CLI flags remain the caller's top layer. The overlay is a single
  `overlay_from<F>(get)` mapping (injectable getter → unit-testable without touching the process
  env) that owns every `MQTTD_*` ↔ typed-field conversion, including the per-var boolean
  conventions a naive string flatten would get wrong (`MQTTD_ALLOW_ANONYMOUS` presence = on;
  `MQTTD_DURABLE_SESSIONS` `0/false/off/no` = off) and the comma-list / `node=domain`-map parses.
  `require_client_cert` has no env var by design (it is derived) — a documented exception the
  forthcoming bijection test will encode. Two T1 doc bugs found and fixed in passing:
  `MQTTD_QUEUE_OVERFLOW` values are `drop-oldest`/`reject-newest` (was `drop-new`), and the JWT
  vars are `MQTTD_JWT_HS256_SECRET` / `MQTTD_JWT_RS256_PEM`. Engine is additive — nothing
  consumes it yet; the `main.rs` typed rewire + redacted effective-config log + bijection test
  land next and flip T2 to done.
