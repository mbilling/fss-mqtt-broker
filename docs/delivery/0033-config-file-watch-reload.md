---
adr: "0033"
title: Filesystem-watch auto-reload of the security policy
adr_status: Proposed
tasks:
  - id: 0033-T1
    title: Expose the watched path set — the configured policy file paths (ACL, password, JWT PEM, TLS cert/key/CA) the binary built the reload closures from
    status: planned
  - id: 0033-T2
    title: Stat-stamp poller task — tokio interval; stamp = (mtime, len, inode) per file; on any change call Reloader::reload(); record the last *applied* stamp so a rejected (partial/malformed) read is retried until it parses
    status: planned
  - id: 0033-T3
    title: Opt-in wiring — MQTTD_CONFIG_WATCH=<seconds> enables it (unset/0 = disabled, signal-only default); spawn the poller; on non-unix it is the only reload trigger
    status: planned
  - id: 0033-T4
    title: Trigger attribution — security.reload audit + security_reloads_total carry trigger=signal|watch
    status: planned
  - id: 0033-T5
    title: Tests — a file edit auto-applies live (ACL tighten with no SIGHUP); a partial-then-whole write applies exactly once (retry-until-parse, never a torn apply); the watcher is inert when disabled
    status: planned
  - id: 0033-T6
    title: Operator docs + README — MQTTD_CONFIG_WATCH, opt-in/off-by-default, the Kubernetes ConfigMap use case, polling latency, and that it shares the ADR 0032 validate-before-swap fail-safe
    status: planned
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
| 0033-T1 | ⬜ planned | — |  |
| 0033-T2 | ⬜ planned | — |  |
| 0033-T3 | ⬜ planned | — |  |
| 0033-T4 | ⬜ planned | — |  |
| 0033-T5 | ⬜ planned | — |  |
| 0033-T6 | ⬜ planned | — |  |
| 0033-T7 | 💤 deferred | — | polling covers the config-rollout use case with no new dependency; an event-driven backend is a latency optimisation that still needs the same retry-until-parse/debounce, so it is parked behind the watcher seam rather than bundled. |
<!-- /status-table:0033 -->

## Changelog

- **2026-06-26** — ADR proposed and delivery opened, as the file-watch follow-on ADR 0032
  explicitly anticipated. Mechanism: opt-in stat-stamp polling (no new dependency) that drives
  the existing validate-before-swap reload routine, with retry-until-parse so partial writes
  are never applied torn. Tasks `planned`; T7 (a `notify` event-driven backend) deferred.
