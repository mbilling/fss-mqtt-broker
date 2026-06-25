# ADR 0033 — Filesystem-watch auto-reload of the security policy

- **Status:** Proposed
- **Date:** 2026-06-26
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0033-config-file-watch-reload.md](../delivery/0033-config-file-watch-reload.md) — plan, progress, and changelog
- **Related:** [ADR 0032](0032-hot-reloadable-security-policy.md) (the SIGHUP reload routine
  this triggers — extends its explicitly-deferred "a watch could be layered on later, calling
  the same reload routine"), [ADR 0019](0019-graceful-shutdown.md) (the signal task that owns
  the other trigger), [ADR 0020](0020-metrics-and-observability.md) (the reload audit/metric),
  [ADR 0002](0002-transport-security.md) (minimal-supply-chain principle weighed below)

> This record states the decision only. How it is being built and how far along it is live in
> the [delivery doc](../delivery/0033-config-file-watch-reload.md).

## Context

ADR 0032 made the security policy hot-reloadable: on `SIGHUP` the broker re-reads the
configured files (`MQTTD_ACL_FILE`, `MQTTD_PASSWORD_FILE`, `MQTTD_JWT_*`, `MQTTD_TLS_*`) and
swaps them onto live connections, **validate-before-swap** (a bad file is rejected and the
running policy is kept). The trigger is deliberately operator-initiated — a signal, not a
file-watch — so the swap happens at an intentional moment.

That leaves one sharp edge: **editing the file does nothing until someone sends `SIGHUP`.**
The broker is passive between signals — it never inspects the file's mtime, never polls. So:

- An operator who updates `MQTTD_ACL_FILE` (tightening access, revoking a user) and **forgets
  the signal** keeps enforcing the *old* policy, silently, with no warning that on-disk config
  is newer than what is loaded.
- The common deployment that makes this acute is **Kubernetes**: a ConfigMap/Secret projected
  as a volume is updated **on disk** by the kubelet with no process signal. Today that update
  is inert until a sidecar or operator manufactures a `SIGHUP`. The natural cloud-native
  expectation — "update the ConfigMap, the policy follows" — does not hold.

A signal is the right *default* (an intentional, audited administrative act), but for
declarative/GitOps operation an **opt-in** "watch the files and reload when they change" is
the missing half. ADR 0032 anticipated exactly this and parked it: *"A watch could be layered
on later, calling the same reload routine."* This ADR is that layer.

## Decision

**Add an opt-in filesystem watcher that, when a configured policy file changes on disk, calls
the existing ADR 0032 reload routine — going through the identical validate-before-swap path.
It is off by default; signal-driven reload remains the default and is always available.**

### 1. Opt-in, off by default

A watcher auto-applies a security-policy change *without* an operator action — powerful, but a
behavioural change to a security control. It is therefore **opt-in** via a single env var,
`MQTTD_CONFIG_WATCH=<seconds>` (the poll interval; unset/`0` = disabled). With it unset the
broker behaves exactly as ADR 0032 today: reload only on `SIGHUP`. Both triggers can be on at
once and call the same routine.

### 2. Detection: stat-stamp polling (no new dependency)

Detect change by **polling**: on a `tokio::time::interval`, `stat` each watched file and
compare a **stamp** = `(modified-time, length, inode)` against the last-seen stamp. Any
difference (including atomic-rename, which swaps the inode) marks that file dirty. This needs
only `std::fs::metadata` + `tokio` — **no new crate**, consistent with the minimal-supply-chain
stance that kept `arc-swap` out (ADR 0002/0032). The `notify` crate (inotify/FSEvents/kqueue)
gives lower latency but pulls a dependency tree and still needs debounce/partial-write
handling; it is the considered alternative below, viable as a later backend behind the same
seam. Polling latency is bounded by the interval (seconds), which is fine for config rollout.

The set of files to watch is exactly the currently-configured policy paths; the binary already
knows them at startup (it built the reload closures from them), so this ADR exposes that path
list to the watcher.

### 3. Reuse the reload routine — including its fail-safe — verbatim

On a detected change the watcher calls **the same `Reloader::reload()`** as `SIGHUP`. It gains
ADR 0032's guarantees for free: all-or-nothing across ACL + authenticator + TLS, **never fail
open, never brick**, audited and metered. Critically this makes polling **robust to
partial writes**: a half-written file fails to parse, the reload is *rejected*, and the
running policy stays in force — no torn config is ever applied.

### 4. Retry-until-parse (the debounce that matters)

The watcher records the stamp it **last successfully applied**, not merely the last stamp
seen. A reload is attempted whenever the current stamp differs from the last *applied* one — so
a rejected reload (a partial or malformed write) does **not** advance the marker and is
**retried on the next poll**, until the file parses cleanly and swaps. This converges on
exactly one successful apply per settled edit, and tolerates an editor's truncate-then-write
without a fixed debounce delay.

### 5. Trigger attribution in the audit/metric

The reload audit event and `security_reloads_total` metric (ADR 0020/0032) gain a **trigger**
attribution — `signal` vs `watch` — so an operator can see *why* a reload fired. A
watch-driven reload is otherwise indistinguishable in the record from a manual one.

### 6. Cross-platform note

Polling is portable, so on non-Unix platforms (where `SIGHUP` is unavailable, ADR 0032) the
watcher becomes the **only** reload mechanism — a bonus, not the motivation.

## Consequences

- **Good:** declarative/GitOps operation — update the ConfigMap/Secret/file and the policy
  follows, no sidecar-manufactured signal, no forgotten-`SIGHUP` drift. Robust to partial
  writes (validate-before-swap + retry-until-parse). No new dependency. Works on non-Unix.
- **Cost:** a polling task per enabled broker; detection latency bounded by the interval; the
  watcher must hold and re-`stat` the configured path set.
- **Trade-off accepted:** auto-apply removes the "operator picks the exact moment" property of
  ADR 0032 — which is why it is **opt-in and off by default**. Operators who want changes
  gated to an explicit act simply leave it off and keep using `SIGHUP`.
- **Risk:** still security-critical (it drives a policy swap), but it adds **no new swap
  logic** — it only *triggers* the already-adversarially-tested ADR 0032 routine. The new
  surface is detection (stamping/retry), tested directly: an edit auto-applies; a partial then
  whole write applies exactly once; the watcher is inert when disabled.
- **Non-goal:** watching env vars or rotating *paths* (ADR 0032 already requires a restart for
  path changes — the watcher follows the same fixed path set), and cluster-wide config
  distribution/consistency (a node still watches only its own files — see the open
  config-distribution question; this ADR does not address drift *between* nodes).

## Alternatives considered

- **`notify` crate (inotify/FSEvents/kqueue), event-driven.** Lower latency than polling, but
  adds a non-trivial dependency tree, and event-driven watching still must debounce and handle
  partial writes / atomic-rename (the watched inode is replaced) — so it needs the same
  retry-until-parse logic anyway. Rejected as the *first* backend on minimal-supply-chain
  grounds; the watcher seam is drawn so a `notify` backend can be slotted in later if
  sub-second reaction is ever needed.
- **mtime-only comparison.** Simpler stamp, but misses same-second edits and is fooled by mtime
  being preserved across an atomic rename of a same-length file. Including length + inode in
  the stamp closes those holes for negligible cost.
- **Always-on (not opt-in).** Rejected: silently changing a security control's behaviour. Keep
  the explicit-signal model as the default; make auto-reload a deliberate opt-in.
- **Status quo — signal only (ADR 0032).** The motivating gap (silent drift, no ConfigMap
  follow-through). Not changing it leaves declarative deployments needing a sidecar to convert
  a file change into a signal. Rejected for the opt-in case; retained as the default.
- **An admin endpoint that pushes new config.** A new authenticated network surface (rejected
  already in ADR 0032 for the trigger) and a different model (push vs the file being the source
  of truth). Out of scope.
