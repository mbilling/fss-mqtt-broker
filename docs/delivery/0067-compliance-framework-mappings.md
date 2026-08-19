---
adr: "0067"
title: "Compliance framework mappings: IEC 62443, EU CRA, SOC 2 / ISO 27001"
adr_status: Proposed
tasks:
  - id: 0067-T1
    title: "docs/compliance/iec-62443.md — 4-1 SDL mapping + 4-2 component-requirement mapping with achievable security levels and honest gaps"
    status: planned
  - id: 0067-T2
    title: "docs/compliance/eu-cra.md — Annex I essential-requirements checklist against shipped facts + the reporting-duty runbook"
    status: planned
    notes: "Time-sensitive: the CRA's actively-exploited-vulnerability reporting duty applies from September 2026; full obligations December 2027."
  - id: 0067-T3
    title: "docs/compliance/soc2-iso27001.md — feature → control → pullable-evidence map for customer assessments"
    status: planned
  - id: 0067-T4
    title: "The mappings join the release checklist: 'verified against' headers re-stamped per release, drift treated as a doc bug"
    status: planned
---

# Delivery — ADR 0067: Compliance framework mappings

Decision: [docs/adr/0067-compliance-framework-mappings.md](../adr/0067-compliance-framework-mappings.md).

Claims documents, held to the delivery-evidence discipline: every row cites the
mechanism and its proof, honest gaps included, and no document claims a
certification the org does not hold.

## Progress

<!-- status-table:0067 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0067-T1 | ⬜ planned | — |  |
| 0067-T2 | ⬜ planned | — | "Time-sensitive: the CRA's actively-exploited-vulnerability reporting duty applies from September 2026; full obligations December 2027." |
| 0067-T3 | ⬜ planned | — |  |
| 0067-T4 | ⬜ planned | — |  |
<!-- /status-table:0067 -->

## Changelog

- **2026-08-19** — ADR proposed and delivery opened, from the post-1.0-freeze
  enterprise-readiness review. Ordering rationale recorded: IEC 62443 first (MQTT's
  buyers are OT), CRA second (statutory deadlines), SOC 2/ISO map third (cheapest,
  leans on the other artifacts).
