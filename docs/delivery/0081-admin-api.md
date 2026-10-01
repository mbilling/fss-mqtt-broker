---
adr: "0081"
title: "An authenticated admin API: reads cluster-wide, actions audited, config stays in the file"
adr_status: Accepted
tasks:
  - id: 0081-T1
    title: "Admin listener: [admin] bind, mTLS required, viewer/operator roles from the cert subject, every request audited, /admin/v1/node"
    status: done
    issue: 710
    date: 2026-09-29
    evidence: "PR #729. crates/mqttd/src/admin/ (mod, http, roles, routes); mqtt_net::tls::admin_acceptor + ChainCheck; [admin] config + MQTTD_ADMIN_* vars, validated, restart-scoped except the role lists. tests/admin.rs over real mTLS: roles from subjects, unlisted subject 403, foreign CA refused at the handshake, hot-reloaded role lists, cluster-CA peer role and an impostor, 404/405 codes, one single-line admin.request audit record per request. THREAT-MODEL, HARDENING H-7.7/H-7.8/H-8.5, AUDIT-SCHEMA and OPERATIONS.md updated."
  - id: 0081-T2
    title: "mqttd --admin <verb>: CLI client for the admin API (table or --json)"
    status: done
    issue: 711
    date: 2026-09-29
    evidence: "PR #729. mqttd --admin <verb> (crates/mqttd/src/admin/cli.rs, client.rs): table-driven verbs validated before anything runs, flags or MQTTD_ADMIN_* client variables with config fallbacks, table or --json output, exit 0/1/2. Smoke-tested against the real binary (whoami, node, 403 for an unlisted subject)."
  - id: 0081-T3
    title: "Cluster view: /admin/v1/cluster and /placement answered by any node via admin-listener fan-out"
    status: done
    issue: 712
    date: 2026-09-29
    evidence: "PR #730. GET /admin/v1/cluster and /admin/v1/placement answered by any node: its members from /statusz, each peer's admin listener asked in parallel (3 s) under the cluster certificate (peer role), silent nodes listed with the reason; summary agreement on cluster id, version, config and membership. Node-to-node over the admin listeners per the ADR's 2026-09-29 amendment. tests/admin.rs three-node test with a refused peer; peer role cannot fan out."
  - id: 0081-T4
    title: "Clients and sessions: paged list and filters, session detail, sessions matching a topic, top-N backlog, retained by prefix"
    status: done
    issue: 713
    date: 2026-09-30
    evidence: "PR #731. /admin/v1/clients (filters, paging, total), /session (subscriptions, in flight, backlog, Will without payload, owner, offline queued count capped at 10 000), /subscribers (bounded page), /backlog, /retained (subtree via the match index, no payloads). HubCommand::Admin handled in hub/admin.rs; store I/O off the hub loop; Admission.source threaded from handle_stream. Three integration tests with real MQTT clients."
  - id: 0081-T5
    title: "Authorization dry run: /admin/v1/authz/check returns the verdict and the deciding rule"
    status: done
    issue: 714
    date: 2026-09-30
    evidence: "PR #732. GET /admin/v1/authz: verdict, deciding rule (index, effect, pattern as written and expanded) and reason from the live authorizer; invalid filters and publish wildcards refused before the policy. mqtt-auth Authorizer::explain, with AclPolicy sharing one evaluator between enforcement and the dry run (allocation-free Decider); explain_always_agrees_with_enforcement grid test plus mTLS integration test."
  - id: 0081-T6
    title: "Effective config (secrets fingerprinted, with a hash) and reload that returns its outcome"
    status: done
    issue: 715
    date: 2026-09-30
    evidence: "PR #733. GET /admin/v1/config serves the committed config (read under the reload lock) with secrets fingerprinted, the file checksum and generation; POST /admin/v1/reload (operator) runs the SIGHUP reload and returns applied, changed_sections and requires_restart, or 409 reload-rejected with the reason. Reloader::reload_with_outcome and a reload mutex; unit tests for the outcome and for never exposing a rejected candidate; mTLS integration test."
  - id: 0081-T7
    title: "Actions: kick (DISCONNECT 0x98) and purge a session, forwarded to the client's node"
    status: done
    issue: 716
    date: 2026-09-30
    evidence: "PR #734. POST /admin/v1/kick (MQTT 5 DISCONNECT 0x98, session kept) and /admin/v1/purge (disconnect, then discard_session: subscriptions, in-flight, expiry, stored queue); a non-owner node forwards to the owner's admin listener under the peer role with forwarded_for, never re-forwarded, audited on both nodes. 0x98 provoked on a real socket (reason-code gate); two-broker forwarding test."
  - id: 0081-T8
    title: "Cordon / uncordon: refuse new connections and report not-ready without draining"
    status: done
    issue: 717
    date: 2026-09-30
    evidence: "PR #735. POST /admin/v1/cordon and /uncordon (operator, this node): the admission gate refuses new connections (admission_rejected reason cordon), /readyz reports not-ready with reason cordoned-by-operator, /livez stays up, /statusz shows it; not persisted. Unit tests for the gate and health, integration round trip."
  - id: 0081-T9
    title: "Log filter override with a TTL (at most one hour), shown on /statusz"
    status: done
    issue: 718
    date: 2026-09-30
    evidence: "PR #736. The tracing filter sits behind a reload layer (mqttd::log_filter); GET/POST /admin/v1/log-level and /log-level/reset: an override in RUST_LOG syntax for at most 3600 s, restored by a timer (a newer override supersedes the older timer); every override keeps audit=info and filters naming the audit target are refused; shown on /statusz. Paused-clock unit tests and an integration test."
  - id: 0081-T10
    title: "OPERATIONS.md: day 0/1/2 runbook using the admin CLI; notes on ADRs 0032/0033/0051 and COMPARISON.md"
    status: done
    issue: 719
    date: 2026-09-30
    evidence: "PR #737. OPERATIONS.md day 0/1/2 table mapping operator questions to mqttd commands; dated notes on ADRs 0032, 0033, 0051 and 0055 pointing to ADR 0081; README, GUIDE and COMPARISON no longer claim there is no admin API."
  - id: 0081-T11
    title: "mqttd --print-config: the effective config with secrets fingerprinted, offline"
    status: done
    issue: 720
    date: 2026-09-29
    evidence: "PR #728. mqttd --print-config prints the effective config (defaults < file < env) with secrets fingerprinted via Config::redacted (mqtt-config) and mqttd::config_view; unit tests pin that no secret field survives redaction and URL separators are preserved."
  - id: 0081-T12
    title: "mqttd --check-tls: chain, key match, expiry and SANs for every configured listener"
    status: done
    issue: 721
    date: 2026-09-29
    evidence: "PR #728. mqttd --check-tls (crates/mqttd/src/tls_check.rs) checks chain, key match, expiry (fail/warn under 30 days) and SANs for [tls] and [cluster.peer_tls]; rcgen-minted ok/expired/key-mismatch tests."
  - id: 0081-T15
    title: "Helm chart and operator support: admin values, certs from Secrets, Service port, NetworkPolicy, CRD field"
    status: done
    issue: 770
    date: 2026-10-01
    evidence: "PR #779. Chart admin.* values and CR spec.admin serve the admin API on 9443 of every pod with its own cluster-bus leaf (headless Service only); render parity admin-on pass; kind smoke checks whoami=operator, anonymous refused, cluster view 3/3. NetworkPolicy split to #778."
  - id: 0081-T16
    title: "Admin e2e scenario suite (kill, partition, scale, decommission, durability, policy, drills), nightly in CI"
    status: done
    issue: 771
    date: 2026-10-01
    evidence: "PR #782. scripts/admin-e2e.sh scenarios: 16 scripted incidents, each on a fresh compose cluster and asserted through the admin API (123 checks; 2 consecutive local runs 16/16), nightly job admin-scenarios. Surfaced #783 and #784."
  - id: 0081-T17
    title: "Cluster-wide clients/session/subscribers (--all-nodes, scope=cluster)"
    status: done
    issue: 772
    date: 2026-10-01
    evidence: "PR #787. scope=cluster on clients/session/subscribers (CLI --all-nodes) fans out over the peer role with forwarded_for and merges: cross-cluster paging by client id, session found_on, nodes[] with replied; two-broker integration test; live e2e 72/72."
  - id: 0081-T18
    title: "Hot reload of the admin listener's certificate, key and CA"
    status: planned
    issue: 773
  - id: 0081-T19
    title: "CLI output polish: compact cluster table, wrapped help"
    status: planned
    issue: 774
  - id: 0081-T13
    title: "OIDC bearer tokens as a second admin authenticator on the same roles"
    status: deferred
    notes: "After T1–T9 are in use."
  - id: 0081-T14
    title: "Bulk retained-message deletion by prefix"
    status: deferred
    notes: "Needs a demonstrated need and a dry-run design (ADR §5)."
---

# Delivery: ADR 0081 — an authenticated admin API

[ADR 0081](../adr/0081-admin-api.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0081 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0081-T1 | ✅ done | [#710](https://github.com/mbilling/fss-mqtt-broker/issues/710) | 2026-09-29 | "PR #729. crates/mqttd/src/admin/ (mod, http, roles, routes); mqtt_net::tls::admin_acceptor + ChainCheck; [admin] config + MQTTD_ADMIN_* vars, validated, restart-scoped except the role lists. tests/admin.rs over real mTLS: roles from subjects, unlisted subject 403, foreign CA refused at the handshake, hot-reloaded role lists, cluster-CA peer role and an impostor, 404/405 codes, one single-line admin.request audit record per request. THREAT-MODEL, HARDENING H-7.7/H-7.8/H-8.5, AUDIT-SCHEMA and OPERATIONS.md updated." |
| 0081-T2 | ✅ done | [#711](https://github.com/mbilling/fss-mqtt-broker/issues/711) | 2026-09-29 | "PR #729. mqttd --admin <verb> (crates/mqttd/src/admin/cli.rs, client.rs): table-driven verbs validated before anything runs, flags or MQTTD_ADMIN_* client variables with config fallbacks, table or --json output, exit 0/1/2. Smoke-tested against the real binary (whoami, node, 403 for an unlisted subject)." |
| 0081-T3 | ✅ done | [#712](https://github.com/mbilling/fss-mqtt-broker/issues/712) | 2026-09-29 | "PR #730. GET /admin/v1/cluster and /admin/v1/placement answered by any node: its members from /statusz, each peer's admin listener asked in parallel (3 s) under the cluster certificate (peer role), silent nodes listed with the reason; summary agreement on cluster id, version, config and membership. Node-to-node over the admin listeners per the ADR's 2026-09-29 amendment. tests/admin.rs three-node test with a refused peer; peer role cannot fan out." |
| 0081-T4 | ✅ done | [#713](https://github.com/mbilling/fss-mqtt-broker/issues/713) | 2026-09-30 | "PR #731. /admin/v1/clients (filters, paging, total), /session (subscriptions, in flight, backlog, Will without payload, owner, offline queued count capped at 10 000), /subscribers (bounded page), /backlog, /retained (subtree via the match index, no payloads). HubCommand::Admin handled in hub/admin.rs; store I/O off the hub loop; Admission.source threaded from handle_stream. Three integration tests with real MQTT clients." |
| 0081-T5 | ✅ done | [#714](https://github.com/mbilling/fss-mqtt-broker/issues/714) | 2026-09-30 | "PR #732. GET /admin/v1/authz: verdict, deciding rule (index, effect, pattern as written and expanded) and reason from the live authorizer; invalid filters and publish wildcards refused before the policy. mqtt-auth Authorizer::explain, with AclPolicy sharing one evaluator between enforcement and the dry run (allocation-free Decider); explain_always_agrees_with_enforcement grid test plus mTLS integration test." |
| 0081-T6 | ✅ done | [#715](https://github.com/mbilling/fss-mqtt-broker/issues/715) | 2026-09-30 | "PR #733. GET /admin/v1/config serves the committed config (read under the reload lock) with secrets fingerprinted, the file checksum and generation; POST /admin/v1/reload (operator) runs the SIGHUP reload and returns applied, changed_sections and requires_restart, or 409 reload-rejected with the reason. Reloader::reload_with_outcome and a reload mutex; unit tests for the outcome and for never exposing a rejected candidate; mTLS integration test." |
| 0081-T7 | ✅ done | [#716](https://github.com/mbilling/fss-mqtt-broker/issues/716) | 2026-09-30 | "PR #734. POST /admin/v1/kick (MQTT 5 DISCONNECT 0x98, session kept) and /admin/v1/purge (disconnect, then discard_session: subscriptions, in-flight, expiry, stored queue); a non-owner node forwards to the owner's admin listener under the peer role with forwarded_for, never re-forwarded, audited on both nodes. 0x98 provoked on a real socket (reason-code gate); two-broker forwarding test." |
| 0081-T8 | ✅ done | [#717](https://github.com/mbilling/fss-mqtt-broker/issues/717) | 2026-09-30 | "PR #735. POST /admin/v1/cordon and /uncordon (operator, this node): the admission gate refuses new connections (admission_rejected reason cordon), /readyz reports not-ready with reason cordoned-by-operator, /livez stays up, /statusz shows it; not persisted. Unit tests for the gate and health, integration round trip." |
| 0081-T9 | ✅ done | [#718](https://github.com/mbilling/fss-mqtt-broker/issues/718) | 2026-09-30 | "PR #736. The tracing filter sits behind a reload layer (mqttd::log_filter); GET/POST /admin/v1/log-level and /log-level/reset: an override in RUST_LOG syntax for at most 3600 s, restored by a timer (a newer override supersedes the older timer); every override keeps audit=info and filters naming the audit target are refused; shown on /statusz. Paused-clock unit tests and an integration test." |
| 0081-T10 | ✅ done | [#719](https://github.com/mbilling/fss-mqtt-broker/issues/719) | 2026-09-30 | "PR #737. OPERATIONS.md day 0/1/2 table mapping operator questions to mqttd commands; dated notes on ADRs 0032, 0033, 0051 and 0055 pointing to ADR 0081; README, GUIDE and COMPARISON no longer claim there is no admin API." |
| 0081-T11 | ✅ done | [#720](https://github.com/mbilling/fss-mqtt-broker/issues/720) | 2026-09-29 | "PR #728. mqttd --print-config prints the effective config (defaults < file < env) with secrets fingerprinted via Config::redacted (mqtt-config) and mqttd::config_view; unit tests pin that no secret field survives redaction and URL separators are preserved." |
| 0081-T12 | ✅ done | [#721](https://github.com/mbilling/fss-mqtt-broker/issues/721) | 2026-09-29 | "PR #728. mqttd --check-tls (crates/mqttd/src/tls_check.rs) checks chain, key match, expiry (fail/warn under 30 days) and SANs for [tls] and [cluster.peer_tls]; rcgen-minted ok/expired/key-mismatch tests." |
| 0081-T15 | ✅ done | [#770](https://github.com/mbilling/fss-mqtt-broker/issues/770) | 2026-10-01 | "PR #779. Chart admin.* values and CR spec.admin serve the admin API on 9443 of every pod with its own cluster-bus leaf (headless Service only); render parity admin-on pass; kind smoke checks whoami=operator, anonymous refused, cluster view 3/3. NetworkPolicy split to #778." |
| 0081-T16 | ✅ done | [#771](https://github.com/mbilling/fss-mqtt-broker/issues/771) | 2026-10-01 | "PR #782. scripts/admin-e2e.sh scenarios: 16 scripted incidents, each on a fresh compose cluster and asserted through the admin API (123 checks; 2 consecutive local runs 16/16), nightly job admin-scenarios. Surfaced #783 and #784." |
| 0081-T17 | ✅ done | [#772](https://github.com/mbilling/fss-mqtt-broker/issues/772) | 2026-10-01 | "PR #787. scope=cluster on clients/session/subscribers (CLI --all-nodes) fans out over the peer role with forwarded_for and merges: cross-cluster paging by client id, session found_on, nodes[] with replied; two-broker integration test; live e2e 72/72." |
| 0081-T18 | ⬜ planned | [#773](https://github.com/mbilling/fss-mqtt-broker/issues/773) | — |  |
| 0081-T19 | ⬜ planned | [#774](https://github.com/mbilling/fss-mqtt-broker/issues/774) | — |  |
| 0081-T13 | 💤 deferred | — | — | "After T1–T9 are in use." |
| 0081-T14 | 💤 deferred | — | — | "Needs a demonstrated need and a dry-run design (ADR §5)." |
<!-- /status-table:0081 -->

## Plan

The order puts the reads operators ask for most first, and each action only after
the reads it depends on.

1. **T1, T2: the foundation.** The authenticated listener with roles and audit, and the
   CLI client, shipped with one endpoint (`/admin/v1/node`) so both are exercised end to
   end before anything identifying is exposed.
2. **T3: the cluster view.** The most requested read: one call to any node shows every
   node's version, readiness, membership, cluster identity and lag. Rolling upgrades and
   resizes depend on it.
3. **T4: clients and sessions.** The day-2 questions: who is connected, where, and why a
   queue is growing.
4. **T5: authorization dry run.** Answers "why is this client denied" without reading the
   policy by hand.
5. **T6: config and reload.** The first action, and the one that removes a gap in the
   current reload flow: the operator learns whether the change took.
6. **T7, T8: kick, purge, cordon.** The incident actions, after the reads that show their
   targets.
7. **T9: log filter override.** Diagnostic, lowest risk, least urgent.
8. **T10: documentation.** The runbook, and the notes on the earlier "no admin API"
   records.
9. **T11, T12: day-0 local commands.** Independent of the listener; listed last only
   because they fill smaller gaps.

## Changelog

- 2026-09-29: ADR proposed; tasks and issues filed.
- 2026-09-29: ADR accepted.
- 2026-09-29: T1, T2 delivered (PR #729 merged): the admin listener and the CLI client.
- 2026-09-29: T11, T12 delivered (PR #728 merged).
- 2026-09-29: T3 delivered (PR #730 merged): the cluster view; node-to-node over the admin listeners (ADR amendment).
- 2026-09-30: T4 (PR #731), T5 (PR #732), T6 (PR #733), T7 (PR #734), T8 (PR #735) and T9 (PR #736) delivered and merged.
- 2026-09-30: T10 delivered (PR #737 merged). Every planned task is done; T13 and T14 stay deferred.
- 2026-10-01: T15–T19 added after the live-cluster test and the docs pass (issues #770–#774); T15 and T16 first.
- 2026-10-01: T15 delivered (PR #779 merged): the chart and the operator enable the admin API; NetworkPolicy split out to #778.
- 2026-10-01: T16 delivered (PR #782 merged): the admin e2e scenario suite, nightly; it surfaced #783 and #784.
- 2026-10-01: T17 delivered (PR #787 merged): cluster-wide clients, session and subscribers.
