---
adr: "0032"
title: Hot-reloadable security policy
adr_status: Accepted
tasks:
  - id: 0032-T1
    title: Reloadable handles — Authorizer + Authenticator (+ TLS acceptor) behind tokio::sync::watch; connection reads the current value per check
    status: planned
  - id: 0032-T2
    title: SIGHUP wiring + reload routine (re-read the configured files; build new values), extending the ADR 0019 signal task
    status: planned
  - id: 0032-T3
    title: Validate-before-swap — a missing/unparseable file keeps the running policy unchanged (never fail open or brick); reload is all-or-nothing
    status: planned
  - id: 0032-T4
    title: ACL hot-reload end to end — a tightened ACL denies an already-subscribed client's next publish/subscribe (live enforcement)
    status: planned
  - id: 0032-T5
    title: Authenticator hot-reload end to end — a rotated password file / JWT key authenticates the new credential and rejects the old
    status: planned
  - id: 0032-T6
    title: TLS material reload — a renewed cert/key/client-CA is served on the next handshake; in-flight TLS sessions are undisturbed
    status: planned
  - id: 0032-T7
    title: Audit event (security.reload) + reload metric on every reload (success and rejection)
    status: planned
  - id: 0032-T8
    title: Operator docs + README — SIGHUP, what reloads, the fail-safe (validate-before-swap) semantics
    status: planned
  - id: 0032-T9
    title: Follow-ons via the same mechanism — cert revocation (reloadable CRL → WebPkiClientVerifier) and peer-bus TLS reload
    status: deferred
    notes: enabled by the T1/T6 reloadable verifier; tracked separately to avoid bundling a client-facing change with the consensus bus and the larger revocation surface (CRL parsing/distribution, OCSP).
---

# Delivery — ADR 0032: Hot-reloadable security policy

Decision: [docs/adr/0032-hot-reloadable-security-policy.md](../adr/0032-hot-reloadable-security-policy.md).

All security config is loaded once at startup and is immutable; changing any setting requires
a full restart (dropping every connection, re-forming the cluster). This adds a SIGHUP-driven,
**atomic, fail-safe** reload that reaches live connections — ACL, the authenticator chain, and
client TLS material — built test-first with the never-fail-open property as the central
adversarial test.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0032-T1** Reloadable handles | The `Authorizer`, `Authenticator` chain, and client TLS acceptor sit behind `tokio::sync::watch`; the connection authz/auth checks and the TLS accept loop read the **current** value per check/accept (so a reload reaches live connections). The policy build is refactored to run at startup *and* on reload. No new dependency. |
| **0032-T2** SIGHUP + reload | The ADR 0019 signal task also handles `SIGHUP`; a reload routine re-reads the configured files (the `MQTTD_*` paths) and builds new `Authorizer`/`Authenticator`/acceptor values. |
| **0032-T3** Validate-before-swap | Reload is all-or-nothing: parse every file first; swap only if all succeed. A missing/unparseable file leaves the running policy **unchanged** (never fail open, never brick) and is reported as a failed reload. |
| **0032-T4** ACL live | After a reload that tightens the ACL, an **already-subscribed** client is denied its next publish/subscribe; a loosened ACL likewise takes effect live. |
| **0032-T5** Auth live | After a reload, a rotated password file / JWT key authenticates the new credential and rejects the old, on new CONNECTs. |
| **0032-T6** TLS live | After a reload, a new handshake uses the renewed cert/key/client-CA; in-flight TLS sessions are undisturbed. |
| **0032-T7** Audit + metric | Every reload (success or rejection) emits a `security.reload` audit event (with outcome/reason) and increments a reload counter. |
| **0032-T8** Docs | Operator docs + README: SIGHUP triggers reload, what is reloadable, and the validate-before-swap fail-safe semantics. |
| **0032-T9** Follow-ons | *(deferred)* Cert revocation (a reloadable CRL fed to the client-cert verifier) and peer-bus TLS reload, both enabled by this mechanism. |

## Progress

<!-- status-table:0032 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0032-T1 | ⬜ planned | — |  |
| 0032-T2 | ⬜ planned | — |  |
| 0032-T3 | ⬜ planned | — |  |
| 0032-T4 | ⬜ planned | — |  |
| 0032-T5 | ⬜ planned | — |  |
| 0032-T6 | ⬜ planned | — |  |
| 0032-T7 | ⬜ planned | — |  |
| 0032-T8 | ⬜ planned | — |  |
| 0032-T9 | 💤 deferred | — | enabled by the T1/T6 reloadable verifier; tracked separately to avoid bundling a client-facing change with the consensus bus and the larger revocation surface (CRL parsing/distribution, OCSP). |
<!-- /status-table:0032 -->

## Changelog

- **2026-06-26** — ADR accepted and delivery opened. Chosen from the security-hardening
  backlog (the most security-central, unblocking the ADR 0002/0004 reload items). Mechanism:
  SIGHUP-triggered, `watch`-backed, validate-before-swap reload reaching live connections.
  Tasks `planned`; T9 (revocation + peer-bus TLS) deferred as a follow-on.
