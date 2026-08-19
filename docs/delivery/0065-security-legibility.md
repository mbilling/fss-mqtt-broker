---
adr: "0065"
title: "Security legibility: scorecard, VEX, dependency automation, lifecycle statements"
adr_status: Proposed
tasks:
  - id: 0065-T1
    title: "OpenSSF Scorecard action + badge; remediate what it flags (pinned digests, token permissions)"
    status: done
    date: 2026-08-19
    evidence: "Scorecard workflow (.github/workflows/scorecard.yml): weekly cron + every main push, publish_results true (feeds the OpenSSF API and the new README badge), SARIF into code scanning; read-all default with job-scoped security-events/id-token writes; 20-min timeout. REMEDIATION DONE WITH IT, tree-wide: all 86 action references across the six workflows pinned to commit digests (resolved via git ls-remote against each upstream, tag kept as a trailing comment); dtolnay/rust-toolchain's ref-as-toolchain convention handled explicitly — every @stable site now pins the master digest AND selects the toolchain via with: (@master sites already did); ci.yml and nightly.yml gained top-level least-privilege permissions (contents: read) — release/stress/examples already had blocks. The badge renders after the first main run publishes results."
  - id: 0065-T2
    title: "Dependency-update automation (Renovate or Dependabot) over crates, actions, and Python tooling, grouped and gated"
    status: done
    date: 2026-08-19
    evidence: ".github/dependabot.yml: cargo (workspace root AND tools/mqttui's separate lockfile) + github-actions ecosystems, weekly, grouped (one PR per ecosystem per week — per-patch-bump noise trains reviewers to rubber-stamp), bounded open-PR limits; security updates bypass the schedule per Dependabot's built-in behaviour. Deliberate exclusions recorded in the file itself: deploy/ image pins advance with OUR release train (check-deploy-image-pin.sh gates them, RELEASING.md moves them), and the Python interop tooling has no manifest to watch (noted with the instruction to add a pip entry if scripts/ ever grows one)."
  - id: 0065-T3
    title: "OpenVEX per release, emitted by the pipeline beside the SBOMs, with an evidence rule for every 'not affected'"
    status: done
    date: 2026-08-19
    evidence: "security/vex/statements.json (OpenVEX 0.2.0) is the reviewed source of truth with @VERSION@/@TIMESTAMP@ placeholders; the release pipeline stamps it (json.loads refuses a malformed doc) into assets/vex-<version>.openvex.json, which the existing cosign loop signs and gh release uploads beside the SBOMs — no extra wiring, by construction. First real disposition shipped, with the evidence rule applied: RUSTSEC-2026-0190 (anyhow Error::downcast_mut unsoundness) is not_affected/vulnerable_code_not_in_execute_path — anyhow enters the tree ONLY through prost-derive (verified against Cargo.lock's dependent graph), a compile-time proc-macro shipping no code into the broker, and no code in the repository calls downcast_mut (tree-wide search, zero hits). cargo audit at authoring time: 0 vulnerabilities, this one warning. The evidence rule itself is codified in security/vex/README.md: a bare not_affected without justification + impact_statement + recorded analysis is a review rejection."
  - id: 0065-T4
    title: "SUPPORT.md — the ADR 0039 lifecycle as a dated table — plus the export-control (ECCN) statement"
    status: done
    date: 2026-08-19
    evidence: "SUPPORT.md: the ADR 0039 policy as a dated, procurement-quotable table — three supported minor lines with the rotation rule, the 1.0.x row seeded (v1.0.0, 2026-08-19), pre-1.0 releases explicitly unsupported with v0.9.1 named as the upgrade entry into the 1.0 line; what 'supported' means stated precisely (adjacent skew + nightly proof, migrate-in-place per ADR 0058, security fixes on every supported line as signed releases). Export control: publicly-available open-source with standard crypto is not subject to the EAR per SS734.7(a)(3) (86 FR 16482), with the conventional ECCN 5D002/TSU self-classification named for procurement paperwork and an explicit not-legal-advice scope."
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
| 0065-T1 | ✅ done | 2026-08-19 | "Scorecard workflow (.github/workflows/scorecard.yml): weekly cron + every main push, publish_results true (feeds the OpenSSF API and the new README badge), SARIF into code scanning; read-all default with job-scoped security-events/id-token writes; 20-min timeout. REMEDIATION DONE WITH IT, tree-wide: all 86 action references across the six workflows pinned to commit digests (resolved via git ls-remote against each upstream, tag kept as a trailing comment); dtolnay/rust-toolchain's ref-as-toolchain convention handled explicitly — every @stable site now pins the master digest AND selects the toolchain via with: (@master sites already did); ci.yml and nightly.yml gained top-level least-privilege permissions (contents: read) — release/stress/examples already had blocks. The badge renders after the first main run publishes results." |
| 0065-T2 | ✅ done | 2026-08-19 | ".github/dependabot.yml: cargo (workspace root AND tools/mqttui's separate lockfile) + github-actions ecosystems, weekly, grouped (one PR per ecosystem per week — per-patch-bump noise trains reviewers to rubber-stamp), bounded open-PR limits; security updates bypass the schedule per Dependabot's built-in behaviour. Deliberate exclusions recorded in the file itself: deploy/ image pins advance with OUR release train (check-deploy-image-pin.sh gates them, RELEASING.md moves them), and the Python interop tooling has no manifest to watch (noted with the instruction to add a pip entry if scripts/ ever grows one)." |
| 0065-T3 | ✅ done | 2026-08-19 | "security/vex/statements.json (OpenVEX 0.2.0) is the reviewed source of truth with @VERSION@/@TIMESTAMP@ placeholders; the release pipeline stamps it (json.loads refuses a malformed doc) into assets/vex-<version>.openvex.json, which the existing cosign loop signs and gh release uploads beside the SBOMs — no extra wiring, by construction. First real disposition shipped, with the evidence rule applied: RUSTSEC-2026-0190 (anyhow Error::downcast_mut unsoundness) is not_affected/vulnerable_code_not_in_execute_path — anyhow enters the tree ONLY through prost-derive (verified against Cargo.lock's dependent graph), a compile-time proc-macro shipping no code into the broker, and no code in the repository calls downcast_mut (tree-wide search, zero hits). cargo audit at authoring time: 0 vulnerabilities, this one warning. The evidence rule itself is codified in security/vex/README.md: a bare not_affected without justification + impact_statement + recorded analysis is a review rejection." |
| 0065-T4 | ✅ done | 2026-08-19 | "SUPPORT.md: the ADR 0039 policy as a dated, procurement-quotable table — three supported minor lines with the rotation rule, the 1.0.x row seeded (v1.0.0, 2026-08-19), pre-1.0 releases explicitly unsupported with v0.9.1 named as the upgrade entry into the 1.0 line; what 'supported' means stated precisely (adjacent skew + nightly proof, migrate-in-place per ADR 0058, security fixes on every supported line as signed releases). Export control: publicly-available open-source with standard crypto is not subject to the EAR per SS734.7(a)(3) (86 FR 16482), with the conventional ECCN 5D002/TSU self-classification named for procurement paperwork and an explicit not-legal-advice scope." |
| 0065-T5 | ⬜ planned | — |  |
| 0065-T6 | ⬜ planned | — | "Sequenced last deliberately: an audit of the 1.0 line after the freeze (ADR 0058) audits the surface enterprises will actually run." |
<!-- /status-table:0065 -->

## Changelog

- **2026-08-19** — ADR proposed and delivery opened, from the post-1.0-freeze
  enterprise-readiness review: the assurance program exists (SBOM, SLSA, cosign,
  deny/audit, fuzzing, coordinated disclosure) but is not legible to the intake
  tooling and vendor-risk processes that decide enterprise adoption.
