---
adr: "0081"
title: "An authenticated admin API: reads cluster-wide, actions audited, config stays in the file"
adr_status: Accepted
tasks:
  - id: 0081-T1
    title: "Admin listener: [admin] bind, mTLS required, viewer/operator roles from the cert subject, every request audited, /admin/v1/node"
    status: in-progress
    issue: 710
    notes: "PR #729."
  - id: 0081-T2
    title: "mqttd --admin <verb>: CLI client for the admin API (table or --json)"
    status: in-progress
    issue: 711
    notes: "PR #729."
  - id: 0081-T3
    title: "Cluster view: /admin/v1/cluster and /placement answered by any node via admin-listener fan-out"
    status: in-progress
    issue: 712
    notes: "Node to node over the admin listeners (ADR amendment 2026-09-29), not the peer bus; each row says whether that node replied, and why not."
  - id: 0081-T4
    title: "Clients and sessions: paged list and filters, session detail, sessions matching a topic, top-N backlog, retained by prefix"
    status: in-progress
    issue: 713
    notes: "Bounded responses only; reads of identifying data are audited."
  - id: 0081-T5
    title: "Authorization dry run: /admin/v1/authz/check returns the verdict and the deciding rule"
    status: in-progress
    issue: 714
    notes: "Evaluates the loaded policy; changes nothing."
  - id: 0081-T6
    title: "Effective config (secrets fingerprinted, with a hash) and reload that returns its outcome"
    status: in-progress
    issue: 715
    notes: "First action. Reload runs the ADR 0032 routine; the file stays the only input."
  - id: 0081-T7
    title: "Actions: kick (DISCONNECT 0x98) and purge a session, forwarded to the client's node"
    status: in-progress
    issue: 716
    notes: "Audited on the node receiving the call and on the node acting."
  - id: 0081-T8
    title: "Cordon / uncordon: refuse new connections and report not-ready without draining"
    status: planned
    issue: 717
    notes: "Not persisted; shown on /statusz."
  - id: 0081-T9
    title: "Log filter override with a TTL (at most one hour), shown on /statusz"
    status: planned
    issue: 718
  - id: 0081-T10
    title: "OPERATIONS.md: day 0/1/2 runbook using the admin CLI; notes on ADRs 0032/0033/0051 and COMPARISON.md"
    status: planned
    issue: 719
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
| 0081-T1 | 🚧 in-progress | [#710](https://github.com/mbilling/fss-mqtt-broker/issues/710) | — | "PR #729." |
| 0081-T2 | 🚧 in-progress | [#711](https://github.com/mbilling/fss-mqtt-broker/issues/711) | — | "PR #729." |
| 0081-T3 | 🚧 in-progress | [#712](https://github.com/mbilling/fss-mqtt-broker/issues/712) | — | "Node to node over the admin listeners (ADR amendment 2026-09-29), not the peer bus; each row says whether that node replied, and why not." |
| 0081-T4 | 🚧 in-progress | [#713](https://github.com/mbilling/fss-mqtt-broker/issues/713) | — | "Bounded responses only; reads of identifying data are audited." |
| 0081-T5 | 🚧 in-progress | [#714](https://github.com/mbilling/fss-mqtt-broker/issues/714) | — | "Evaluates the loaded policy; changes nothing." |
| 0081-T6 | 🚧 in-progress | [#715](https://github.com/mbilling/fss-mqtt-broker/issues/715) | — | "First action. Reload runs the ADR 0032 routine; the file stays the only input." |
| 0081-T7 | 🚧 in-progress | [#716](https://github.com/mbilling/fss-mqtt-broker/issues/716) | — | "Audited on the node receiving the call and on the node acting." |
| 0081-T8 | ⬜ planned | [#717](https://github.com/mbilling/fss-mqtt-broker/issues/717) | — | "Not persisted; shown on /statusz." |
| 0081-T9 | ⬜ planned | [#718](https://github.com/mbilling/fss-mqtt-broker/issues/718) | — |  |
| 0081-T10 | ⬜ planned | [#719](https://github.com/mbilling/fss-mqtt-broker/issues/719) | — |  |
| 0081-T11 | ✅ done | [#720](https://github.com/mbilling/fss-mqtt-broker/issues/720) | 2026-09-29 | "PR #728. mqttd --print-config prints the effective config (defaults < file < env) with secrets fingerprinted via Config::redacted (mqtt-config) and mqttd::config_view; unit tests pin that no secret field survives redaction and URL separators are preserved." |
| 0081-T12 | ✅ done | [#721](https://github.com/mbilling/fss-mqtt-broker/issues/721) | 2026-09-29 | "PR #728. mqttd --check-tls (crates/mqttd/src/tls_check.rs) checks chain, key match, expiry (fail/warn under 30 days) and SANs for [tls] and [cluster.peer_tls]; rcgen-minted ok/expired/key-mismatch tests." |
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
- 2026-09-29: T1, T2 in review (PR #729): the admin listener and the CLI client.
- 2026-09-29: T11, T12 delivered (PR #728 merged).
- 2026-09-29: T3 in review (PR #730): the cluster view; node-to-node over the admin listeners (ADR amendment).
