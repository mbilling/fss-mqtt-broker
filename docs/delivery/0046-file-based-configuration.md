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
    status: done
    date: 2026-07-19
    evidence: "Two parts. (1) Layering engine in mqtt-config: Config::load(path) applies defaults → TOML file → MQTTD_* overlay → validate; overlay_from<F>(get) is the single, injectable env↔typed mapping (unit-testable without the process env), honouring the per-var boolean conventions a naive flatten gets wrong (MQTTD_ALLOW_ANONYMOUS presence = on; MQTTD_DURABLE_SESSIONS 0/false/off/no = off), comma-lists, and the MQTTD_FAILURE_DOMAINS node=domain map; numeric vars fail with a located ConfigError::Invalid. (2) Typed rewire of main.rs (the approach chosen over an env-bridge for its security footguns): every one of the ~57 MQTTD_* reads now sources from the loaded Config — node/listeners/tls/security/cluster/durable/limits/observability/runtime — via --config <path> / MQTTD_CONFIG (flag wins), fully backward compatible when neither is set. Effective config logged at startup with secrets redacted (swim.key, swim.key_accept, inline JWT HS256). Bijection: mqtt_config::ENV_VARS is the authoritative 57-var surface; tests assert it is deduplicated and that setting any one var alone moves the config off default (totality — no silently-dropped mapping), with documented exceptions (require_client_cert is derived → no var; MQTTD_CONFIG is the meta file path). require_client_cert has no env var by design. Also fixed two T1 doc bugs found here: MQTTD_QUEUE_OVERFLOW is drop-oldest|reject-newest (not drop-new); JWT vars are MQTTD_JWT_HS256_SECRET / MQTTD_JWT_RS256_PEM. mqtt-config 16 tests + mqttd lib 146 + main.rs bin 4 + durable_sessions 10 green; workspace all-targets builds; clippy -D warnings + fmt clean."
  - id: 0046-T3
    title: mqttd --check-config subcommand — validates a file and exits without binding ports; for GitOps CI and pre-rollout operator checks; clear located errors
    status: done
    date: 2026-07-19
    evidence: "main.rs: `mqttd --check-config` is dispatched at the very top of main() — before any port bind or hub start — and loads the exact config the broker would boot with (file from --config/MQTTD_CONFIG, layered under the MQTTD_* env) through Config::load, then exits. Exit 0 + `config OK: <path> + MQTTD_* env overlay validates` on success; exit 1 + a clear located error on an invalid config (TOML line/column for parse errors, e.g. an unknown key; the offending field for semantic ones, e.g. `cluster.swim.signed must be \"require\" or \"off\"`); exit 2 for a malformed invocation (--config with no value). Testable core (check_config_inner) separated from the exiting wrapper; CheckError classifies usage (exit 2) vs invalid-config (exit 1). New integration suite tests/check_config.rs drives the real binary (5 tests): no-file defaults+env OK, valid file OK + path reported, unknown key → exit 1 + located, bad env overlay (MQTTD_LEASE_VOTERS=0) → exit 1, --config without value → exit 2 — each asserts nothing is bound. clippy -D warnings + fmt clean."
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
| 0046-T2 | ✅ done | 2026-07-19 | "Two parts. (1) Layering engine in mqtt-config: Config::load(path) applies defaults → TOML file → MQTTD_* overlay → validate; overlay_from<F>(get) is the single, injectable env↔typed mapping (unit-testable without the process env), honouring the per-var boolean conventions a naive flatten gets wrong (MQTTD_ALLOW_ANONYMOUS presence = on; MQTTD_DURABLE_SESSIONS 0/false/off/no = off), comma-lists, and the MQTTD_FAILURE_DOMAINS node=domain map; numeric vars fail with a located ConfigError::Invalid. (2) Typed rewire of main.rs (the approach chosen over an env-bridge for its security footguns): every one of the ~57 MQTTD_* reads now sources from the loaded Config — node/listeners/tls/security/cluster/durable/limits/observability/runtime — via --config <path> / MQTTD_CONFIG (flag wins), fully backward compatible when neither is set. Effective config logged at startup with secrets redacted (swim.key, swim.key_accept, inline JWT HS256). Bijection: mqtt_config::ENV_VARS is the authoritative 57-var surface; tests assert it is deduplicated and that setting any one var alone moves the config off default (totality — no silently-dropped mapping), with documented exceptions (require_client_cert is derived → no var; MQTTD_CONFIG is the meta file path). require_client_cert has no env var by design. Also fixed two T1 doc bugs found here: MQTTD_QUEUE_OVERFLOW is drop-oldest|reject-newest (not drop-new); JWT vars are MQTTD_JWT_HS256_SECRET / MQTTD_JWT_RS256_PEM. mqtt-config 16 tests + mqttd lib 146 + main.rs bin 4 + durable_sessions 10 green; workspace all-targets builds; clippy -D warnings + fmt clean." |
| 0046-T3 | ✅ done | 2026-07-19 | "main.rs: `mqttd --check-config` is dispatched at the very top of main() — before any port bind or hub start — and loads the exact config the broker would boot with (file from --config/MQTTD_CONFIG, layered under the MQTTD_* env) through Config::load, then exits. Exit 0 + `config OK: <path> + MQTTD_* env overlay validates` on success; exit 1 + a clear located error on an invalid config (TOML line/column for parse errors, e.g. an unknown key; the offending field for semantic ones, e.g. `cluster.swim.signed must be \"require\" or \"off\"`); exit 2 for a malformed invocation (--config with no value). Testable core (check_config_inner) separated from the exiting wrapper; CheckError classifies usage (exit 2) vs invalid-config (exit 1). New integration suite tests/check_config.rs drives the real binary (5 tests): no-file defaults+env OK, valid file OK + path reported, unknown key → exit 1 + located, bad env overlay (MQTTD_LEASE_VOTERS=0) → exit 1, --config without value → exit 2 — each asserts nothing is bound. clippy -D warnings + fmt clean." |
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
- **2026-07-19** — **T2 done.** The typed rewire landed on the engine: `main.rs` now loads one
  `mqtt_config::Config` (from `--config <path>` / `MQTTD_CONFIG`, else defaults + env) and every
  `MQTTD_*` read across the startup path — listeners, TLS, auth, cluster/SWIM, durable, limits,
  observability, runtime — sources from it. `std::env` is no longer consulted for broker settings;
  the env surface is mapped exactly once, in `mqtt_config::overlay_from`. Startup logs the effective
  config with the inline secrets (`swim.key`, `swim.key_accept`, JWT HS256) redacted. The
  env↔config mapping is pinned by `mqtt_config::ENV_VARS` (the authoritative 57-var surface) plus a
  totality test — set any one var alone and the config must move off default, so a dropped mapping
  fails CI. Documented exceptions: `require_client_cert` is derived (no var); `MQTTD_CONFIG` names
  the file. Chosen over the env-bridge (`set_var`) approach, which round-trips secrets through the
  process environment. Backward compatible: an env-only deployment behaves exactly as before.
- **2026-07-19** — **T2 backward-compat fix.** `MQTTD_SHUTDOWN_GRACE=0` (drain immediately — the
  ADR 0019 fast-teardown value the `cluster_proc` harness spawns with) was a *valid* env value the
  old code accepted; the T1 `validate()` over-restricted `shutdown_grace_secs` to `≥ 1`, so the
  rewired binary refused to start under it and every spawned node failed readiness (caught by
  `cluster_proc` on the first CI run of the rewire). Fixed by allowing `0` (0 = immediate); the
  out-of-range test now asserts `0` is accepted. `cluster_proc` (3) green locally after the fix.
- **2026-07-19** — **T3 done.** `mqttd --check-config` validates the config the broker would boot
  with (file layered under env) and exits **without binding a port** — dispatched at the top of
  `main()`, before any resource is acquired. Exit `0` + `config OK` (naming the file), exit `1` +
  a located error (TOML line/column, or the offending field), exit `2` for a malformed invocation.
  New `tests/check_config.rs` drives the real binary across all four outcomes. This is the GitOps
  CI / pre-rollout gate: a bad ConfigMap fails the pipeline instead of crash-looping a pod.
