---
adr: "0067"
title: "Compliance framework mappings: IEC 62443, EU CRA, SOC 2 / ISO 27001"
adr_status: Accepted
tasks:
  - id: 0067-T1
    title: "docs/compliance/iec-62443.md — 4-1 SDL mapping + 4-2 component-requirement mapping with achievable security levels and honest gaps"
    status: done
    date: 2026-08-19
    evidence: "docs/compliance/iec-62443.md. Part 4-1: all eight practices (SM/SR/SD/SI/SVV/DM/SUM/SG) mapped to the actual SDL — the ADR discipline as SM/SR, deny-by-default and deliberate absences as SD, memory-safety + one-provider + deny/audit gates as SI, the red-first/mutation/fuzz/chaos battery as SVV, coordinated disclosure + VEX as DM, signed no-format-change patches on three lines as SUM, and HARDENING/tutorial/OPERATIONS/AUDIT-SCHEMA as SG. Honest 4-1 gaps stated: no per-feature requirements register (ours live in ADRs), no third-party verification yet (0065-T6), and role separation a solo project cannot have — compensated by the review-panel method, said plainly. Part 4-2: FR1-FR7 mapped with representative capability evidence and an SL-C read per FR (generally SL 2-3 capability, at-rest confidentiality SL 1 from the component with the platform completing it — stated, not hidden), the SL-C vs achieved-SL distinction explicit with HARDENING.md named as the bridge, and the not-applicable CR classes scoped out honestly."
  - id: 0067-T2
    title: "docs/compliance/eu-cra.md — Annex I essential-requirements checklist against shipped facts + the reporting-duty runbook"
    status: done
    date: 2026-08-19
    evidence: "docs/compliance/eu-cra.md. Scope stated first and honestly: no CE marking or conformity assessment implied; commercial-activity trigger and the OSS-steward regime explained; conservative default-category read with the downstream-integrator caveat. Annex I Part I mapped requirement-by-requirement to shipped, checkable facts (13 rows, each with its check: the no-known-exploitable-vulns row cites the cargo-audit gate + per-release VEX; secure-by-default cites HARDENING H-0; separable security updates cite ADR 0039's no-format-change patch rule; logging cites AUDIT-SCHEMA + the verifier). Annex I Part II vulnerability-handling mapped (SBOM per binary, disclosure policy, secure update distribution). THE RUNBOOK the task existed for: Article 14 reporting — 24h early warning / 72h notification / 14d-1mo final report via ENISA's platform — written as steps the maintainer executes, with the honest note that the disclosure machinery (GHSA, fix on supported lines, VEX) runs regardless of whether a steward duty legally attaches. Honest gaps: no calendar end-date per generation in SUPPORT.md (a commercial shipper must declare one), no auto-updater by deliberate posture."
  - id: 0067-T3
    title: "docs/compliance/soc2-iso27001.md — feature → control → pullable-evidence map for customer assessments"
    status: done
    date: 2026-08-19
    evidence: "docs/compliance/soc2-iso27001.md. Frame stated first: SOC 2/ISO attest organizations, no product is compliant, this file accelerates the CUSTOMER's evidence collection. Thirteen capability rows, each mapping to SOC 2 TSC (CC-series + A1) and ISO 27001:2022 Annex A controls with a PULLABLE artifact per row — the audit trail row points at the SIEM stream plus audit-verify.py exit 0; supply chain points at the actual release assets (sbom-*.cdx.json, vex-*.openvex.json, .sig) and RELEASING's verification one-liners; hardening points at a completed checklist with the deviation-record rule. Rows without a pullable artifact were not written. Closes with what the customer still owns (org controls, IR process, at-rest encryption per the threat model's stated boundary)."
  - id: 0067-T4
    title: "The mappings join the release checklist: 'verified against' headers re-stamped per release, drift treated as a doc bug"
    status: done
    date: 2026-08-19
    evidence: "RELEASING.md's cutting-a-release checklist gains step 4: re-verify and re-date the 'Verified against' headers of THREAT-MODEL, HARDENING, AUDIT-SCHEMA and docs/compliance/* before the tag — drift in a claims document is a doc bug fixed pre-release. Subsequent steps renumbered (and the one stale step-number cross-reference fixed). SUPPORT.md — the procurement-facing document — routes to docs/compliance/ with the no-certification-implied framing, so the mappings are findable from the door procurement actually enters through."
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
| 0067-T1 | ✅ done | 2026-08-19 | "docs/compliance/iec-62443.md. Part 4-1: all eight practices (SM/SR/SD/SI/SVV/DM/SUM/SG) mapped to the actual SDL — the ADR discipline as SM/SR, deny-by-default and deliberate absences as SD, memory-safety + one-provider + deny/audit gates as SI, the red-first/mutation/fuzz/chaos battery as SVV, coordinated disclosure + VEX as DM, signed no-format-change patches on three lines as SUM, and HARDENING/tutorial/OPERATIONS/AUDIT-SCHEMA as SG. Honest 4-1 gaps stated: no per-feature requirements register (ours live in ADRs), no third-party verification yet (0065-T6), and role separation a solo project cannot have — compensated by the review-panel method, said plainly. Part 4-2: FR1-FR7 mapped with representative capability evidence and an SL-C read per FR (generally SL 2-3 capability, at-rest confidentiality SL 1 from the component with the platform completing it — stated, not hidden), the SL-C vs achieved-SL distinction explicit with HARDENING.md named as the bridge, and the not-applicable CR classes scoped out honestly." |
| 0067-T2 | ✅ done | 2026-08-19 | "docs/compliance/eu-cra.md. Scope stated first and honestly: no CE marking or conformity assessment implied; commercial-activity trigger and the OSS-steward regime explained; conservative default-category read with the downstream-integrator caveat. Annex I Part I mapped requirement-by-requirement to shipped, checkable facts (13 rows, each with its check: the no-known-exploitable-vulns row cites the cargo-audit gate + per-release VEX; secure-by-default cites HARDENING H-0; separable security updates cite ADR 0039's no-format-change patch rule; logging cites AUDIT-SCHEMA + the verifier). Annex I Part II vulnerability-handling mapped (SBOM per binary, disclosure policy, secure update distribution). THE RUNBOOK the task existed for: Article 14 reporting — 24h early warning / 72h notification / 14d-1mo final report via ENISA's platform — written as steps the maintainer executes, with the honest note that the disclosure machinery (GHSA, fix on supported lines, VEX) runs regardless of whether a steward duty legally attaches. Honest gaps: no calendar end-date per generation in SUPPORT.md (a commercial shipper must declare one), no auto-updater by deliberate posture." |
| 0067-T3 | ✅ done | 2026-08-19 | "docs/compliance/soc2-iso27001.md. Frame stated first: SOC 2/ISO attest organizations, no product is compliant, this file accelerates the CUSTOMER's evidence collection. Thirteen capability rows, each mapping to SOC 2 TSC (CC-series + A1) and ISO 27001:2022 Annex A controls with a PULLABLE artifact per row — the audit trail row points at the SIEM stream plus audit-verify.py exit 0; supply chain points at the actual release assets (sbom-*.cdx.json, vex-*.openvex.json, .sig) and RELEASING's verification one-liners; hardening points at a completed checklist with the deviation-record rule. Rows without a pullable artifact were not written. Closes with what the customer still owns (org controls, IR process, at-rest encryption per the threat model's stated boundary)." |
| 0067-T4 | ✅ done | 2026-08-19 | "RELEASING.md's cutting-a-release checklist gains step 4: re-verify and re-date the 'Verified against' headers of THREAT-MODEL, HARDENING, AUDIT-SCHEMA and docs/compliance/* before the tag — drift in a claims document is a doc bug fixed pre-release. Subsequent steps renumbered (and the one stale step-number cross-reference fixed). SUPPORT.md — the procurement-facing document — routes to docs/compliance/ with the no-certification-implied framing, so the mappings are findable from the door procurement actually enters through." |
<!-- /status-table:0067 -->

## Changelog

- **2026-08-19** — All four tasks shipped in one pass (CRA first for its
  September reporting-duty clock): the three mappings under docs/compliance/
  plus the release-checklist re-stamp rule. ADR Accepted.

- **2026-08-19** — ADR proposed and delivery opened, from the post-1.0-freeze
  enterprise-readiness review. Ordering rationale recorded: IEC 62443 first (MQTT's
  buyers are OT), CRA second (statutory deadlines), SOC 2/ISO map third (cheapest,
  leans on the other artifacts).
