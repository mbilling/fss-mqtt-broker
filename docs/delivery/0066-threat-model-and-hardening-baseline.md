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
    notes: "The one product change in the record; the export is a copy of the ADR 0004 tamper-evident stream, never its replacement."
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
| 0066-T3 | ⬜ planned | — | "The one product change in the record; the export is a copy of the ADR 0004 tamper-evident stream, never its replacement." |
<!-- /status-table:0066 -->

## Changelog

- **2026-08-19** — ADR proposed and delivery opened, from the post-1.0-freeze
  enterprise-readiness review.
