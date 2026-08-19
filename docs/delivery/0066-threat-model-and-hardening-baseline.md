---
adr: "0066"
title: "Threat model, hardening baseline, and SIEM-consumable audit"
adr_status: Proposed
tasks:
  - id: 0066-T1
    title: "docs/THREAT-MODEL.md — STRIDE over the five surfaces, every row naming its mechanism + ADR or its accepted risk; kept current by the frozen-surface checklist"
    status: planned
  - id: 0066-T2
    title: "docs/HARDENING.md — numbered, levelled baseline items, each with knob, default, and verification command"
    status: planned
  - id: 0066-T3
    title: "Audit-log SIEM export (RFC 5424 syslog and/or OTLP), documented schema, honest integrity story"
    status: planned
    notes: "GAP ANALYSIS (2026-08-19, recorded before the work): the instrumentation layer already exists — an AuditSink trait called at every auth/ACL/admin decision site, events carrying seq/kind/subject/detail plus the running chain head, and OTLP plumbing proven in metrics. Five gaps, in dependency order: (1) THE CHAIN WAS A PLACEHOLDER — crates/mqtt-observability/src/lib.rs used a self-described non-cryptographic mixing function while the docs said tamper-evident; shipping SIEM export of that chain would have been the overclaim this repo exists to prevent. FIXED FIRST (slice 1): SHA-256 chain via aws-lc-rs (ADR 0053 one-provider rule), length-prefixed presence-tagged fields, boot-scoped genesis (random boot id, genesis line announcing id+head) so a restart is a new announced chain, distinguishable from truncation; verification model is external anchoring — every record carries the head, so shipped heads contradict any rewrite; a keyed HMAC variant for deployments that cannot anchor externally ships with the export slice. (2) No SIEM-native transport — today the export is a tracing line on stdout; RFC 5424 syslog over TCP/TLS and/or OTLP logs needed. (3) No frozen schema — the kind vocabulary (auth.*/acl.*/admin.*) is unenumerated; a SIEM parser needs the list and an additive-only stability promise. (4) No delivery policy — record() must stay non-blocking, so the exporter needs the bridge-spool discipline: bounded queue, drop-with-counter, WARN on shed; a seq gap on the SIEM side is then detectable by design. (5) No verification procedure — an audit-verify subcommand or documented script that replays exported records and compares heads, so tamper-evident is checkable, not believed."
---

# Delivery — ADR 0066: Threat model, hardening baseline, SIEM-consumable audit

Decision: [docs/adr/0066-threat-model-and-hardening-baseline.md](../adr/0066-threat-model-and-hardening-baseline.md).

Consolidation, not invention: the threat reasoning distributed across sixty-four ADRs
becomes the one document a security architect asks for; the secure-configuration
narrative becomes a checkable baseline; the audit trail becomes ingestable by the
SIEMs that enterprise control sets actually run on.

## Progress

<!-- status-table:0066 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0066-T1 | ⬜ planned | — |  |
| 0066-T2 | ⬜ planned | — |  |
| 0066-T3 | ⬜ planned | — | "GAP ANALYSIS (2026-08-19, recorded before the work): the instrumentation layer already exists — an AuditSink trait called at every auth/ACL/admin decision site, events carrying seq/kind/subject/detail plus the running chain head, and OTLP plumbing proven in metrics. Five gaps, in dependency order: (1) THE CHAIN WAS A PLACEHOLDER — crates/mqtt-observability/src/lib.rs used a self-described non-cryptographic mixing function while the docs said tamper-evident; shipping SIEM export of that chain would have been the overclaim this repo exists to prevent. FIXED FIRST (slice 1): SHA-256 chain via aws-lc-rs (ADR 0053 one-provider rule), length-prefixed presence-tagged fields, boot-scoped genesis (random boot id, genesis line announcing id+head) so a restart is a new announced chain, distinguishable from truncation; verification model is external anchoring — every record carries the head, so shipped heads contradict any rewrite; a keyed HMAC variant for deployments that cannot anchor externally ships with the export slice. (2) No SIEM-native transport — today the export is a tracing line on stdout; RFC 5424 syslog over TCP/TLS and/or OTLP logs needed. (3) No frozen schema — the kind vocabulary (auth.*/acl.*/admin.*) is unenumerated; a SIEM parser needs the list and an additive-only stability promise. (4) No delivery policy — record() must stay non-blocking, so the exporter needs the bridge-spool discipline: bounded queue, drop-with-counter, WARN on shed; a seq gap on the SIEM side is then detectable by design. (5) No verification procedure — an audit-verify subcommand or documented script that replays exported records and compares heads, so tamper-evident is checkable, not believed." |
<!-- /status-table:0066 -->

## Changelog

- **2026-08-19** — T3 slice 1 landed: the placeholder chain found during the gap
  analysis (a non-cryptographic mix behind a "tamper-evident" claim) is replaced
  with a SHA-256 chain, boot-scoped genesis, and hex heads on every emitted
  record; the full five-gap analysis is recorded in T3's notes so the remaining
  export work has its dependency order written down.
- **2026-08-19** — ADR proposed and delivery opened, from the post-1.0-freeze
  enterprise-readiness review.
