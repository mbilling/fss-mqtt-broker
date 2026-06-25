# ADR 0032 — Hot-reloadable security policy

- **Status:** Accepted
- **Date:** 2026-06-26
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0032-hot-reloadable-security-policy.md](../delivery/0032-hot-reloadable-security-policy.md) — plan, progress, and changelog
- **Related:** [ADR 0002](0002-transport-security.md) (the TLS material this reloads; cert
  revocation is a noted follow-on), [ADR 0004](0004-identity-and-authentication.md) (the
  authenticator chain + ACL this reloads), [ADR 0013](0013-enhanced-authentication.md)
  (enhanced auth, also a chain member), [ADR 0019](0019-graceful-shutdown.md) (the existing
  signal-handling task this extends), [ADR 0020](0020-metrics-and-observability.md) (the
  reload metric/audit)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0032-hot-reloadable-security-policy.md).

## Context

Every piece of security configuration is loaded **once at startup** and then immutable:

- the client-listener TLS material — `server_acceptor(cert, key, client_ca)` builds an
  `Arc<ServerConfig>` once ([`tls.rs`](../../crates/mqtt-net/src/tls.rs)), cloned into the
  accept loop;
- the **ACL** — `AclPolicy::from_toml_str` parsed once into `Arc<dyn Authorizer>`
  ([`acl.rs`](../../crates/mqtt-auth/src/acl.rs), built in
  [`main.rs`](../../crates/mqttd/src/main.rs));
- the **authenticator chain** — password file (a `HashMap` of PHC hashes), JWT HS256 secret /
  RS256 PEM, all read once into an `Arc<dyn Authenticator>`
  ([`password.rs`](../../crates/mqtt-auth/src/password.rs),
  [`token.rs`](../../crates/mqtt-auth/src/token.rs)).

These are bundled into an `Arc<ConnPolicy>` ([`conn.rs`](../../crates/mqttd/src/conn.rs)) that
is cloned into every connection task. There is **no reload path of any kind**: signal
handling is shutdown-only (SIGTERM/SIGINT, ADR 0019) — there is no SIGHUP, no file-watch, no
admin endpoint, and no `RwLock`/`ArcSwap`/`watch` indirection around the policy.

So **changing any security setting requires a full process restart** — which, on a clustered
node, drops every client connection and forces a lease/membership re-form. For a
security-first broker that is a real operational gap: rotating a leaked credential, revoking
a user's access by tightening the ACL, or renewing a TLS certificate before it expires should
not require downtime. Two deferred items live here — ADR 0002 (TLS contexts built once; no
reload; revocation absent) and ADR 0004 (ACL changes under live subscriptions) — both noting
they "unblock with hot-reloadable policy work".

## Decision

**Reload the security policy in place on `SIGHUP`, atomically and fail-safe, reaching live
connections — without dropping a single one.**

### 1. Trigger: SIGHUP, re-reading the configured files

Reload is **operator-initiated** via `SIGHUP` (the Unix idiom — nginx/haproxy/most daemons),
added to the existing signal task (ADR 0019) alongside the shutdown signals. It re-reads the
**same files** the process was started with (the `MQTTD_TLS_*`, `MQTTD_ACL_FILE`,
`MQTTD_PASSWORD_FILE`, `MQTTD_JWT_*` paths). No new network surface, no new authn to protect,
no auto-reaction to a half-written file. On a non-Unix platform SIGHUP is unavailable, so
reload is a no-op (logged); the feature is Unix-targeted, like graceful shutdown.

### 2. Swappable handles via `tokio::sync::watch` (no new dependency)

The hot-swappable parts of the policy — the `Authorizer`, the `Authenticator` chain, and the
client TLS `ServerConfig` — move behind `tokio::sync::watch` channels. `watch` is already
available (tokio is a core dep), gives a **cheap, lock-light read** (`borrow()`), and carries
change-notification for free — no `arc-swap` or other new crate.

- The connection's authz/auth checks read the **current** value per check
  (`rx.borrow().clone()` → an `Arc` clone), so a reload reaches **already-connected** clients:
  the next `authorize_publish`/`authorize_subscribe` after a reload is evaluated against the
  **new** ACL (an already-subscribed client losing access is denied its next operation — the
  ADR 0004 requirement). The added cost is one borrow + `Arc` clone per check.
- The TLS accept loop reads the current acceptor per `accept()`, so a renewed certificate is
  served on the **next** handshake; in-flight TLS sessions keep their negotiated parameters
  (correct — you do not tear down a live session to rotate a cert).

### 3. Validate-before-swap: atomic and fail-safe

A reload **never degrades the running policy**. On SIGHUP the broker reads and parses **every**
configured file into new values *first*; only if **all** succeed does it publish them (one
`watch` send per handle). If any file is missing, unreadable, or unparseable, the broker
**keeps the currently-running policy unchanged** and records the reload as **failed** with the
error. A deny-by-default broker must neither fail *open* (swap in an empty/permissive policy)
nor brick itself (refuse traffic) on an operator typo — the live, last-known-good policy
stays in force until a valid reload succeeds.

### 4. Audit + observability

Every reload — success or rejection — is an **audit** event (`security.reload`, with the
outcome and, on failure, the reason) and increments a reload counter metric (ADR 0020). The
log names which component(s) changed. A reload is a security-relevant administrative action
and is recorded as one.

### 5. Scope

This ADR delivers the **mechanism** plus reload for the **client-listener TLS material**, the
**ACL**, and the **authenticator chain** (password file, JWT keys). Two things are explicit
follow-ons enabled *by* this mechanism, tracked but not bundled:

- **Certificate revocation (CRL):** `WebPkiClientVerifier` accepts CRLs; a reloadable CRL file
  feeding the verifier is a natural extension once the verifier itself is rebuildable on
  reload (it is, as part of the TLS reload).
- **Peer-bus (cluster) TLS reload** (ADR 0003/0002): the same pattern applied to the peer
  acceptor/connector; deferred to avoid coupling a client-facing change to the consensus bus.

## Consequences

- **Good:** zero-downtime credential rotation, ACL changes, and cert renewal; a tightened ACL
  reaches live connections immediately; the reload is fail-safe (a bad file cannot open or
  brick the broker) and auditable. Unblocks the ADR 0002/0004 reload items and makes
  revocation tractable.
- **Cost:** the security state moves behind `watch` handles, so the authz hot path does a
  cheap `borrow().clone()` per check (an `Arc` bump); a reload routine + SIGHUP wiring; and
  the policy build is refactored to be callable both at startup and on reload.
- **Risk:** security-critical — a reload must be **atomic** and must **never fail open**. Built
  test-first with adversarial tests: a malformed file is rejected and the prior policy stays
  in force; a tightened ACL denies an already-subscribed client's next publish/subscribe; a
  rotated password file accepts the new hash and rejects the old; a renewed cert is served on
  the next handshake without disturbing live sessions. Same bar as ADR 0003/0004/0023/0025.

## Alternatives considered

- **File-watch (e.g. the `notify` crate): auto-reload on file change.** Convenient, but adds a
  dependency, races on partially-written files (must debounce / atomic-rename-detect), and
  reloads *without an operator action* — surprising for a security control. SIGHUP keeps
  reload an explicit, intentional act. (A watch could be layered on later, calling the same
  reload routine.)
- **An admin HTTP/control endpoint.** A new network surface that itself needs authentication
  and authorization to avoid becoming the weakest link — more attack surface than a signal.
  Rejected for the trigger; the health/metrics server stays read-only.
- **`arc-swap::ArcSwap` instead of `watch`.** Slightly leaner reads, but a new dependency for
  no capability `watch` lacks here. Rejected on the minimal-supply-chain principle (ADR 0002);
  `watch` is already in tree.
- **Restart-only (today).** Simple, but means downtime and a cluster re-form for any security
  change — the motivating problem. Rejected.
- **Swap but apply only to new connections.** Easier (no per-check read), but an ACL tightening
  would not reach an already-subscribed client — failing the ADR 0004 requirement that a
  revocation take effect on live sessions. Rejected.
