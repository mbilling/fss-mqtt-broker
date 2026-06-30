---
adr: "0033"
title: Filesystem-watch auto-reload of the security policy
adr_status: Accepted
tasks:
  - id: 0033-T1
    title: Expose the watched path set — the configured policy file paths (ACL, password, JWT PEM, TLS cert/key/CA) the binary built the reload closures from
    status: done
    date: 2026-06-30
    evidence: "main.rs watched_policy_paths() collects the set-and-file-backed policy env paths the reload closures read — MQTTD_ACL_FILE, MQTTD_PASSWORD_FILE, MQTTD_JWT_RS256_PEM, MQTTD_TLS_CERT/KEY/CLIENT_CA/CRL (the JWT HS256 secret is inline, not a file, so it is not watchable). A path is included only when its env var is set."
  - id: 0033-T2
    title: Stat-stamp poller task — tokio interval; stamp = (mtime, len, inode) per file; on any change call Reloader::reload(); record the last *applied* stamp so a rejected (partial/malformed) read is retried until it parses
    status: done
    date: 2026-06-30
    evidence: "config_watch.rs ConfigWatcher: stamp = (modified-time, len, inode) per file (inode via MetadataExt on unix, 0 elsewhere); tick() compares against the last *applied* stamps and calls the reload closure on any difference, advancing the applied stamps only when reload returns true — so a rejected reload is retried each poll until it parses. The async watch() loop runs it on a tokio::interval (MissedTickBehavior::Skip) until shutdown. Unit tests: no_change_does_not_reload, a_settled_edit_applies_exactly_once, a_rejected_reload_is_retried_until_it_parses, an_atomic_rename_is_detected."
  - id: 0033-T3
    title: Opt-in wiring — MQTTD_CONFIG_WATCH=<seconds> enables it (unset/0 = disabled, signal-only default); spawn the poller; on non-unix it is the only reload trigger
    status: done
    date: 2026-06-30
    evidence: "main.rs spawn_config_watcher: MQTTD_CONFIG_WATCH=<seconds> (unset/0 = disabled) spawns config_watch::watch; the Reloader is shared as Arc between the SIGHUP handler and the watcher (reload() is &self). A non-integer value is a startup error. Polling is portable, so on non-unix (no SIGHUP) the watcher is the only reload trigger."
  - id: 0033-T4
    title: Trigger attribution — security.reload audit + security_reloads_total carry trigger=signal|watch
    status: done
    date: 2026-06-30
    evidence: "Reloader::reload(trigger) threads the trigger into the security.reload audit detail and Metrics::security_reload(outcome, trigger); OutcomeLabel gains a trigger field so mqttd_security_reloads_total carries {outcome,trigger}. SIGHUP passes \"signal\", the watcher \"watch\". Test reload::reload_increments_the_metric_by_outcome asserts the {outcome,trigger=signal} labels."
  - id: 0033-T5
    title: Tests — a file edit auto-applies live (ACL tighten with no SIGHUP); a partial-then-whole write applies exactly once (retry-until-parse, never a torn apply); the watcher is inert when disabled
    status: done
    date: 2026-06-30
    evidence: "tests/config_watch.rs drives the real Reloader over a real ACL file through ConfigWatcher: a_file_edit_auto_applies_without_a_signal (tighten the file, one poll, the live authorizer now denies — no SIGHUP), a_partial_write_is_rejected_then_a_clean_write_applies (malformed write kept the running policy and is retried, the whole write applies exactly once). Plus the unit tests in T2, incl. no_change_does_not_reload for the inert case."
  - id: 0033-T6
    title: Operator docs + README — MQTTD_CONFIG_WATCH, opt-in/off-by-default, the Kubernetes ConfigMap use case, polling latency, and that it shares the ADR 0032 validate-before-swap fail-safe
    status: done
    date: 2026-06-30
    evidence: "README 'Filesystem auto-reload (opt-in, ADR 0033)' paragraph + the MQTTD_CONFIG_WATCH env-table row (the Kubernetes ConfigMap case, opt-in/off-by-default, the shared validate-before-swap + retry-until-parse fail-safe, the trigger label, and non-unix behaviour). main.rs env-list doc updated."
  - id: 0033-T7
    title: Follow-on — optional notify-backed (inotify/FSEvents/kqueue) event-driven backend behind the same seam, if sub-second reaction is ever needed
    status: deferred
    notes: polling covers the config-rollout use case with no new dependency; an event-driven backend is a latency optimisation that still needs the same retry-until-parse/debounce, so it is parked behind the watcher seam rather than bundled.
---

# Delivery — ADR 0033: Filesystem-watch auto-reload of the security policy

Decision: [docs/adr/0033-config-file-watch-reload.md](../adr/0033-config-file-watch-reload.md).

ADR 0032 made the security policy reload on `SIGHUP`; editing a file does nothing until that
signal arrives, so a forgotten signal (or a Kubernetes ConfigMap update, which lands on disk
with no signal) silently keeps the old policy. This adds an **opt-in** poller that detects a
changed policy file and calls the **same** ADR 0032 reload routine — inheriting its
validate-before-swap fail-safe — so declarative deployments converge without a sidecar
manufacturing a signal. Off by default; `SIGHUP` stays the default trigger.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0033-T1** Watched path set | The configured policy file paths are exposed to the watcher (the same paths the reload closures read). No behavioural change on its own. |
| **0033-T2** Stat-stamp poller | A `tokio::time::interval` task stamps each watched file as `(mtime, len, inode)` and, on any difference from the last *applied* stamp, calls `Reloader::reload()`. The applied stamp advances **only on a successful reload**, so a rejected read (partial/malformed) is retried on the next tick until it parses. |
| **0033-T3** Opt-in wiring | `MQTTD_CONFIG_WATCH=<seconds>` spawns the poller at that interval; unset or `0` leaves the broker signal-only (today's behaviour). On non-Unix the poller is the only reload path. |
| **0033-T4** Trigger attribution | The `security.reload` audit event and `security_reloads_total` metric distinguish `trigger=signal` from `trigger=watch`. |
| **0033-T5** Tests | A file edit auto-applies on a live connection with **no** `SIGHUP` (an ACL tighten denies an already-subscribed client's next op); a partial write is rejected and the subsequent whole write applies **exactly once** (retry-until-parse, never a torn apply); with the watcher disabled a file edit changes nothing until a signal. |
| **0033-T6** Docs | README + operator docs: the env var, opt-in/off-by-default, the k8s ConfigMap use case, polling-latency expectation, and the shared ADR 0032 fail-safe. |
| **0033-T7** Follow-on | *(deferred)* A `notify`-backed event-driven backend behind the same watcher seam, if sub-second reaction is ever required. |

## Progress

<!-- status-table:0033 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0033-T1 | ✅ done | 2026-06-30 | "main.rs watched_policy_paths() collects the set-and-file-backed policy env paths the reload closures read — MQTTD_ACL_FILE, MQTTD_PASSWORD_FILE, MQTTD_JWT_RS256_PEM, MQTTD_TLS_CERT/KEY/CLIENT_CA/CRL (the JWT HS256 secret is inline, not a file, so it is not watchable). A path is included only when its env var is set." |
| 0033-T2 | ✅ done | 2026-06-30 | "config_watch.rs ConfigWatcher: stamp = (modified-time, len, inode) per file (inode via MetadataExt on unix, 0 elsewhere); tick() compares against the last *applied* stamps and calls the reload closure on any difference, advancing the applied stamps only when reload returns true — so a rejected reload is retried each poll until it parses. The async watch() loop runs it on a tokio::interval (MissedTickBehavior::Skip) until shutdown. Unit tests: no_change_does_not_reload, a_settled_edit_applies_exactly_once, a_rejected_reload_is_retried_until_it_parses, an_atomic_rename_is_detected." |
| 0033-T3 | ✅ done | 2026-06-30 | "main.rs spawn_config_watcher: MQTTD_CONFIG_WATCH=<seconds> (unset/0 = disabled) spawns config_watch::watch; the Reloader is shared as Arc between the SIGHUP handler and the watcher (reload() is &self). A non-integer value is a startup error. Polling is portable, so on non-unix (no SIGHUP) the watcher is the only reload trigger." |
| 0033-T4 | ✅ done | 2026-06-30 | "Reloader::reload(trigger) threads the trigger into the security.reload audit detail and Metrics::security_reload(outcome, trigger); OutcomeLabel gains a trigger field so mqttd_security_reloads_total carries {outcome,trigger}. SIGHUP passes \"signal\", the watcher \"watch\". Test reload::reload_increments_the_metric_by_outcome asserts the {outcome,trigger=signal} labels." |
| 0033-T5 | ✅ done | 2026-06-30 | "tests/config_watch.rs drives the real Reloader over a real ACL file through ConfigWatcher: a_file_edit_auto_applies_without_a_signal (tighten the file, one poll, the live authorizer now denies — no SIGHUP), a_partial_write_is_rejected_then_a_clean_write_applies (malformed write kept the running policy and is retried, the whole write applies exactly once). Plus the unit tests in T2, incl. no_change_does_not_reload for the inert case." |
| 0033-T6 | ✅ done | 2026-06-30 | "README 'Filesystem auto-reload (opt-in, ADR 0033)' paragraph + the MQTTD_CONFIG_WATCH env-table row (the Kubernetes ConfigMap case, opt-in/off-by-default, the shared validate-before-swap + retry-until-parse fail-safe, the trigger label, and non-unix behaviour). main.rs env-list doc updated." |
| 0033-T7 | 💤 deferred | — | polling covers the config-rollout use case with no new dependency; an event-driven backend is a latency optimisation that still needs the same retry-until-parse/debounce, so it is parked behind the watcher seam rather than bundled. |
<!-- /status-table:0033 -->

## Changelog

- **2026-06-30** — T1–T6 delivered; ADR **Accepted**. A `config_watch::ConfigWatcher` polls the
  configured policy files on a `tokio::interval` and, on a `(mtime, len, inode)` stamp change,
  drives the existing `Reloader::reload` — inheriting ADR 0032's validate-before-swap fail-safe,
  with retry-until-parse (the applied stamp advances only on a successful swap). Opt-in via
  `MQTTD_CONFIG_WATCH=<seconds>` (off by default; the `Reloader` is shared `Arc` with the SIGHUP
  handler). The reload audit + `security_reloads_total` gain a `trigger` (`signal`/`watch`).
  Proven by unit tests (stamp/retry/atomic-rename) and `tests/config_watch.rs` end-to-end
  (an ACL tighten auto-applies with no SIGHUP; a partial write is rejected then a clean write
  applies once). T7 (a `notify` event-driven backend) stays deferred.
- **2026-06-26** — ADR proposed and delivery opened, as the file-watch follow-on ADR 0032
  explicitly anticipated. Mechanism: opt-in stat-stamp polling (no new dependency) that drives
  the existing validate-before-swap reload routine, with retry-until-parse so partial writes
  are never applied torn. Tasks `planned`; T7 (a `notify` event-driven backend) deferred.
