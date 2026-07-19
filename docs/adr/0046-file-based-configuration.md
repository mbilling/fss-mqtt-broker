# ADR 0046 — File-based configuration (layered over env, hot-reloadable, GitOps-friendly)

- **Status:** Accepted
- **Date:** 2026-07-17 (accepted 2026-07-19)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0046-file-based-configuration.md](../delivery/0046-file-based-configuration.md) — plan, progress, and changelog
- **Related:** [ADR 0032](0032-hot-reloadable-security-policy.md) (the validate-before-swap
  hot-reload machinery this reuses for the config file), [ADR 0033](0033-config-file-watch-reload.md)
  (the filesystem-watch reload trigger this extends from the ACL file to the whole config),
  [ADR 0004](0004-identity-and-authentication.md) (the ACL/auth files this config references
  rather than absorbs), [ADR 0020](0020-metrics-and-observability.md) / [ADR 0041](0041-resource-governance.md)
  (the many knobs — binds, caps, quotas — that today live only as env vars)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0046-file-based-configuration.md).

## Context

Every operational knob is an environment variable — `MQTTD_*` for listeners, TLS material,
auth, caps, quotas, gossip, health, metrics. That is fine for a container's `-e` flags and
a quick local run, and it will stay supported. But it does not survive real deployments:

- A production broker has **dozens** of settings; a wall of `export`s is unreviewable and
  error-prone, and there is no single artifact to diff, review, or roll back.
- **GitOps and Kubernetes** want a file: a ConfigMap mounted at a path, versioned in git,
  changed by a reviewed commit. Env-var-only config forces awkward ConfigMap-to-env
  plumbing and loses the file as the unit of change.
- There is **no schema** an operator can validate ahead of a deploy — a typo in an env var
  is discovered at boot, or silently ignored.

The broker already has the hard part built: ADR 0032's **validate-before-swap** hot reload
and ADR 0033's **filesystem-watch** trigger reload the ACL policy on `SIGHUP`/file change
without dropping connections, failing closed on a bad file. That machinery generalizes to a
whole-broker config file; today it only covers the security policy.

## Decision

A single, optional, hot-reloadable **config file** becomes the primary way to configure the
broker, with env vars kept as an override layer. Five parts:

### 1. One TOML file, matching the ecosystem already in the repo

Config is **TOML** — the same format as the ACL policy files (ADR 0004), so operators learn
one syntax. A `--config <path>` flag (and `MQTTD_CONFIG` env) points at it; sections mirror
the existing env groups (`[listeners]`, `[tls]`, `[auth]`, `[cluster]`, `[limits]`,
`[observability]`, …). Every `MQTTD_*` var maps to exactly one documented key, so the two
surfaces never diverge.

### 2. Explicit, documented precedence

Layering is **defaults < config file < environment variables < command-line flags** — the
least-surprising order: a file sets the baseline, an env var or flag overrides one setting
without editing the file (the container-injection and debugging cases). The effective config
is logged at startup (secrets redacted) so what-is-actually-running is never a guess.

### 3. A validating schema, checkable before deploy

The config has a **strict schema** (unknown keys rejected, types and ranges checked), and a
`mqttd --check-config <path>` subcommand validates a file and exits — no broker started, no
ports bound. GitOps pipelines run it in CI; operators run it before a rollout. A malformed
file is a clear, located error, not a boot-time surprise.

### 4. Hot reload, riding the existing validate-before-swap path

Changing the file and sending `SIGHUP` (or via the ADR 0033 watch) **reloads the whole
config** the same way the ACL already reloads: parse and validate the new file entirely,
and swap only if it is valid — a bad edit is rejected and the running config kept intact
(never fail open, never brick). What can safely change live (ACLs, auth chain, caps,
quotas, TLS material — already reloadable) changes without a restart; settings that cannot
(a listener's bind address) are logged as "requires restart" rather than silently ignored.
Every reload is audited and metered, as security reloads already are.

### 5. Secrets stay out of the file by reference

The config file references secret material **by path** (TLS keys, password files, JWT keys,
the gossip key), never inlines it — so the file itself is safe to commit to git and mount
from a ConfigMap, while secrets come from a Secret mount or a secret manager. The gossip key
and other raw secrets may also stay env-only, keeping them out of any file at all.

## Consequences

- Real deployments get a reviewable, versionable, roll-back-able unit of configuration; the
  Kubernetes/GitOps path (ConfigMap → file, `--check-config` in CI) is first-class.
- Nothing breaks: env-var-only deployments keep working (env overrides the absent/partial
  file), and the container image documents both paths.
- The hot-reload story widens from "security policy" to "the whole broker," reusing proven
  validate-before-swap machinery rather than inventing a second reload path.
- One more surface to keep in sync: every new setting must land in the schema, the env
  mapping, and the docs together. The mapping is mechanical and testable (a test asserts
  every `MQTTD_*` var has a config key and vice versa), so drift is caught in CI.
- This ADR does **not** absorb the ACL/auth policy files (ADR 0004) into the main config —
  those stay separate files referenced by path, keeping the security policy independently
  reloadable and independently reviewable.

## Alternatives considered

- **YAML:** ubiquitous in the k8s world, but whitespace-fragile and a repeated source of
  ambiguity/security footguns (anchors, type coercion). TOML is already in the repo (ACLs),
  is unambiguous, and is a better fit for a security-first project. Rejected.
- **Keep env-var-only:** the status quo does not survive GitOps and offers no schema or
  reviewable artifact — the exact gap the analysis flagged. Rejected.
- **A bespoke config format:** more control, but a new syntax operators must learn and we
  must parse safely. Reusing TOML costs nothing and reuses the ACL parser's hardening
  (itself fuzzed under ADR 0044 P5). Rejected.
- **Absorbing ACL/auth policy into one mega-config:** simpler to explain, but couples the
  security policy's independent reload/review lifecycle to the broker's; keeping them
  separate files (referenced by path) is the safer factoring. Rejected.
