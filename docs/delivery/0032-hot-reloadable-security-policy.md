---
adr: "0032"
title: Hot-reloadable security policy
adr_status: Accepted
tasks:
  - id: 0032-T1
    title: Reloadable handles — Authorizer + Authenticator (+ TLS acceptor) behind tokio::sync::watch; connection reads the current value per check
    status: done
    date: 2026-06-26
    evidence: "conn::ConnPolicy.auth/.authz are watch::Receiver re-read per check (authenticator()/authorizer() accessors); reload::Reloader/Handles own the senders; serve_tls_clients reads the acceptor from a watch::Receiver per accept via Reloader::attach_tls. No new dependency."
  - id: 0032-T2
    title: SIGHUP wiring + reload routine (re-read the configured files; build new values), extending the ADR 0019 signal task
    status: done
    date: 2026-06-26
    evidence: "main::spawn_reload_handler installs a SIGHUP signal stream (#[cfg(unix)]) driving Reloader::reload; client_policy_from_env supplies a build closure re-reading MQTTD_ACL_FILE + the authenticator chain; start_client_listeners attaches the TLS rebuild. Non-unix: logged no-op."
  - id: 0032-T3
    title: Validate-before-swap — a missing/unparseable file keeps the running policy unchanged (never fail open or brick); reload is all-or-nothing
    status: done
    date: 2026-06-26
    evidence: "Reloader::reload builds ACL+authenticator(+TLS) up front and publishes only if every build succeeds; any error rejects the whole reload and keeps the running policy. Tests: reload::a_failed_reload_keeps_the_running_policy, reload_acl::malformed_acl_reload_is_rejected_and_keeps_the_running_policy, reload_auth::malformed_password_file_reload_is_rejected_*, reload_tls::malformed_cert_reload_is_rejected_and_keeps_serving."
  - id: 0032-T4
    title: ACL hot-reload end to end — a tightened ACL denies an already-subscribed client's next publish/subscribe (live enforcement)
    status: done
    date: 2026-06-26
    evidence: "reload_acl::tightening_acl_reload_denies_a_live_publisher — an already-connected publisher's next publish is dropped after a tightening reload, no reconnect."
  - id: 0032-T5
    title: Authenticator hot-reload end to end — a rotated password file / JWT key authenticates the new credential and rejects the old
    status: done
    date: 2026-06-26
    evidence: "reload_auth::rotated_password_file_reload_authenticates_the_new_credential — PasswordAuthenticator rebuilt from a rotated file; new credential accepted, old one rejected on the next CONNECT (read live via policy.authenticator())."
  - id: 0032-T6
    title: TLS material reload — a renewed cert/key/client-CA is served on the next handshake; in-flight TLS sessions are undisturbed
    status: done
    date: 2026-06-26
    evidence: "Reloader::attach_tls + serve_tls_clients per-accept read. Test: reload_tls::renewed_cert_is_served_on_the_next_handshake — a renewed leaf (distinct CA) is served on the next handshake while the in-flight session (handshaked under the old cert) keeps carrying traffic."
  - id: 0032-T7
    title: Audit event (security.reload) + reload metric on every reload (success and rejection)
    status: done
    date: 2026-06-26
    evidence: "Reloader::reload records security.reload (ok / rejected: <reason>) and increments mqttd_security_reloads_total{outcome}. Test: reload::reload_increments_the_metric_by_outcome asserts both outcome labels."
  - id: 0032-T8
    title: Operator docs + README — SIGHUP, what reloads, the fail-safe (validate-before-swap) semantics
    status: done
    date: 2026-06-26
    evidence: "README Security bullet + 'Hot reload (SIGHUP)' Configuration subsection: kill -HUP, what reloads (ACL/auth/TLS), validate-before-swap fail-safe, audit + metric, non-unix caveat, path-rotation restart note."
  - id: 0032-T9
    title: Follow-ons via the same mechanism — cert revocation (reloadable CRL → WebPkiClientVerifier) and peer-bus TLS reload
    status: deferred
    notes: "Partly delivered. Cert revocation via a reloadable CRL → WebPkiClientVerifier is **done** (ADR 0002 T8: server_config_with_crl + MQTTD_TLS_CRL, applied through this ADR's reloadable acceptor; tests/tls.rs reloading_a_crl_revokes_a_client_in_place). Still deferred: peer-bus (cluster) TLS reload — the same pattern applied to the peer acceptor/connector, kept off the consensus bus for now to avoid coupling a client-facing change to membership/quorum."
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
| 0032-T1 | ✅ done | 2026-06-26 | "conn::ConnPolicy.auth/.authz are watch::Receiver re-read per check (authenticator()/authorizer() accessors); reload::Reloader/Handles own the senders; serve_tls_clients reads the acceptor from a watch::Receiver per accept via Reloader::attach_tls. No new dependency." |
| 0032-T2 | ✅ done | 2026-06-26 | "main::spawn_reload_handler installs a SIGHUP signal stream (#[cfg(unix)]) driving Reloader::reload; client_policy_from_env supplies a build closure re-reading MQTTD_ACL_FILE + the authenticator chain; start_client_listeners attaches the TLS rebuild. Non-unix: logged no-op." |
| 0032-T3 | ✅ done | 2026-06-26 | "Reloader::reload builds ACL+authenticator(+TLS) up front and publishes only if every build succeeds; any error rejects the whole reload and keeps the running policy. Tests: reload::a_failed_reload_keeps_the_running_policy, reload_acl::malformed_acl_reload_is_rejected_and_keeps_the_running_policy, reload_auth::malformed_password_file_reload_is_rejected_*, reload_tls::malformed_cert_reload_is_rejected_and_keeps_serving." |
| 0032-T4 | ✅ done | 2026-06-26 | "reload_acl::tightening_acl_reload_denies_a_live_publisher — an already-connected publisher's next publish is dropped after a tightening reload, no reconnect." |
| 0032-T5 | ✅ done | 2026-06-26 | "reload_auth::rotated_password_file_reload_authenticates_the_new_credential — PasswordAuthenticator rebuilt from a rotated file; new credential accepted, old one rejected on the next CONNECT (read live via policy.authenticator())." |
| 0032-T6 | ✅ done | 2026-06-26 | "Reloader::attach_tls + serve_tls_clients per-accept read. Test: reload_tls::renewed_cert_is_served_on_the_next_handshake — a renewed leaf (distinct CA) is served on the next handshake while the in-flight session (handshaked under the old cert) keeps carrying traffic." |
| 0032-T7 | ✅ done | 2026-06-26 | "Reloader::reload records security.reload (ok / rejected: <reason>) and increments mqttd_security_reloads_total{outcome}. Test: reload::reload_increments_the_metric_by_outcome asserts both outcome labels." |
| 0032-T8 | ✅ done | 2026-06-26 | "README Security bullet + 'Hot reload (SIGHUP)' Configuration subsection: kill -HUP, what reloads (ACL/auth/TLS), validate-before-swap fail-safe, audit + metric, non-unix caveat, path-rotation restart note." |
| 0032-T9 | 💤 deferred | — | "Partly delivered. Cert revocation via a reloadable CRL → WebPkiClientVerifier is **done** (ADR 0002 T8: server_config_with_crl + MQTTD_TLS_CRL, applied through this ADR's reloadable acceptor; tests/tls.rs reloading_a_crl_revokes_a_client_in_place). Still deferred: peer-bus (cluster) TLS reload — the same pattern applied to the peer acceptor/connector, kept off the consensus bus for now to avoid coupling a client-facing change to membership/quorum." |
<!-- /status-table:0032 -->

## Changelog

- **2026-06-26** — ADR accepted and delivery opened. Chosen from the security-hardening
  backlog (the most security-central, unblocking the ADR 0002/0004 reload items). Mechanism:
  SIGHUP-triggered, `watch`-backed, validate-before-swap reload reaching live connections.
  Tasks `planned`; T9 (revocation + peer-bus TLS) deferred as a follow-on.
- **2026-06-26** — T1–T8 delivered. `conn::ConnPolicy` reads the authorizer/authenticator
  from `watch` receivers per check; `reload::Reloader` owns the senders + the file-rereading
  `build` closure and swaps **validate-before-swap and all-or-nothing** (ACL + authenticator
  + TLS acceptor together — any bad file rejects the whole reload, keeping the running
  policy). `SIGHUP` drives it (`main::spawn_reload_handler`); `Reloader::attach_tls` makes the
  TLS listener read its acceptor per accept (renewed cert served next handshake, in-flight
  sessions undisturbed). Every reload is audited (`security.reload`) and metered
  (`security_reloads_total{outcome}`). Tests: `reload::` unit (swap / fail-safe / metric) plus
  `reload_acl`, `reload_auth`, `reload_tls` end-to-end over live connections. README documents
  SIGHUP, scope, and the fail-safe. T9 (CRL revocation + peer-bus TLS) remains deferred.
