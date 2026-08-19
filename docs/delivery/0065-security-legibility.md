---
adr: "0065"
title: "Security legibility: scorecard, VEX, dependency automation, lifecycle statements"
adr_status: Proposed
tasks:
  - id: 0065-T1
    title: "OpenSSF Scorecard action + badge; remediate what it flags (pinned digests, token permissions)"
    status: planned
  - id: 0065-T2
    title: "Dependency-update automation (Renovate or Dependabot) over crates, actions, and Python tooling, grouped and gated"
    status: planned
  - id: 0065-T3
    title: "OpenVEX per release, emitted by the pipeline beside the SBOMs, with an evidence rule for every 'not affected'"
    status: planned
  - id: 0065-T4
    title: "SUPPORT.md — the ADR 0039 lifecycle as a dated table — plus the export-control (ECCN) statement"
    status: planned
  - id: 0065-T5
    title: "OSS-Fuzz onboarding for the six fuzz targets; CodeQL beside clippy in CI"
    status: planned
  - id: 0065-T6
    title: "Funded third-party security audit, findings published in-repo with their fixes"
    status: planned
    notes: "Sequenced last deliberately: an audit of the 1.0 line after the freeze (ADR 0058) audits the surface enterprises will actually run."
---

# Delivery — ADR 0065: Security legibility

Decision: [docs/adr/0065-security-legibility.md](../adr/0065-security-legibility.md).

The legibility layer over the ADR 0044/0045 assurance substance: the artifacts an
enterprise vendor-risk process looks for by name — scorecard, VEX, lifecycle and
export statements, automated dependency inflow, external validation.

## Progress

<!-- status-table:0065 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0065-T1 | ⬜ planned | — |  |
| 0065-T2 | ⬜ planned | — |  |
| 0065-T3 | ⬜ planned | — |  |
| 0065-T4 | ⬜ planned | — |  |
| 0065-T5 | ⬜ planned | — |  |
| 0065-T6 | ⬜ planned | — | "Sequenced last deliberately: an audit of the 1.0 line after the freeze (ADR 0058) audits the surface enterprises will actually run." |
<!-- /status-table:0065 -->

## Changelog

- **2026-08-19** — ADR proposed and delivery opened, from the post-1.0-freeze
  enterprise-readiness review: the assurance program exists (SBOM, SLSA, cosign,
  deny/audit, fuzzing, coordinated disclosure) but is not legible to the intake
  tooling and vendor-risk processes that decide enterprise adoption.
