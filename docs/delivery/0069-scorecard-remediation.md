---
adr: "0069"
title: "Scorecard remediation: a solution per check, honestly bounded"
adr_status: Proposed
tasks:
  - id: 0069-T1
    title: "Token-Permissions: top-level read everywhere, write only at job level where the act needs it"
    status: done
    date: 2026-08-19
    evidence: "The rule enforced tree-wide: top-level contents: read in every workflow, writes granted only at job level where the act needs them. examples-bundle.yml (the flagged violator) moves contents:write + id-token:write from top level onto its one publishing job; ci/nightly gained top-level read blocks earlier (0065-T1); release/stress/scorecard already conformed. Every workflow now passes the checker's rule by construction."
  - id: 0069-T2
    title: "CodeQL on pull requests (the SAST check; the analysis half of 0065-T5)"
    status: done
    date: 2026-08-19
    evidence: ".github/workflows/codeql.yml: Rust analysis (build-mode none) on every PR, main push, and a weekly cron; findings land in code scanning beside the Scorecard SARIF; job-scoped security-events: write under a top-level contents: read; 60-min leash; actions digest-pinned (peeled SHAs). Also completes the analysis half of 0065-T5."
  - id: 0069-T3
    title: "Vulnerabilities: rkyv edge trimmed or dispositioned; jsonwebtoken false positive recorded in the VEX"
    status: done
    date: 2026-08-19
    evidence: "Both OSV hits dispositioned in the VEX with evidence, per the security/vex README rule: GHSA-h395-gr6q-cpjc (jsonwebtoken) is not_affected/component_not_present — the advisory's range ends at 10.3.0 and we ship 11.0.0; a scanner range-matching bug, now machine-readably answered. RUSTSEC-2026-0235 (rkyv) is not_affected/vulnerable_code_not_in_execute_path — the chain is openraft -> byte-unit -> rust_decimal -> rkyv (verified in Cargo.lock; NOT trimmable without forking upstream config, checked), and zero rkyv API call sites exist in the tree; no archive is ever constructed or read. anyhow was already dispositioned (0065-T3). All three scanner hits now have machine-readable answers shipping with every release."
  - id: 0069-T4
    title: "Pinned-Dependencies: digest-pin distroless bases in every Dockerfile; fix the operator-e2e.sh quote that breaks the checker's parser"
    status: done
    date: 2026-08-19
    evidence: "All three Dockerfiles pin the base by DIGEST (gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f... — the multi-arch index digest, confirmed against the registry from two independent paths), with the rationale comment: the tag names the intent, the digest IS the base. The operator-e2e.sh quote that made the checker's shell parser give up ('reached EOF without closing quote' at the escaped-dot jsonpath inside nested quotes) is rewritten as a go-template label read — bash -n parses, the vendored bundle re-vendored, and the checker can now finish reading the tree instead of reporting 'possibly incomplete results'."
  - id: 0069-T5
    title: "Security-Policy: SECURITY.md gains the advisory-form, advisories-page, and timeline links"
    status: done
    date: 2026-08-19
    evidence: "SECURITY.md gains 'The links, in one place': the private advisory form URL, the published advisories page, SUPPORT.md for versions/timelines, the per-release VEX directory, and docs/compliance/ — the linked content the checker (and a reporter at 2 a.m.) looks for."
  - id: 0069-T6
    title: "Maintainer: main-branch ruleset (require PR + status checks, no force-push/deletion), solo-compatible"
    status: planned
  - id: 0069-T7
    title: "Maintainer: bestpractices.dev registration and passing-level self-certification"
    status: planned
---

# Delivery — ADR 0069: Scorecard remediation

Decision: [docs/adr/0069-scorecard-remediation.md](../adr/0069-scorecard-remediation.md).

The 2026-08-19 published run (5.5/10) turned into per-check work with owners —
and an explicit no-work list (Maintained, Code-Review, Contributors are moved by
time and people, not commits). Target after T1–T7: ≈ 8.

## Progress

<!-- status-table:0069 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0069-T1 | ✅ done | 2026-08-19 | "The rule enforced tree-wide: top-level contents: read in every workflow, writes granted only at job level where the act needs them. examples-bundle.yml (the flagged violator) moves contents:write + id-token:write from top level onto its one publishing job; ci/nightly gained top-level read blocks earlier (0065-T1); release/stress/scorecard already conformed. Every workflow now passes the checker's rule by construction." |
| 0069-T2 | ✅ done | 2026-08-19 | ".github/workflows/codeql.yml: Rust analysis (build-mode none) on every PR, main push, and a weekly cron; findings land in code scanning beside the Scorecard SARIF; job-scoped security-events: write under a top-level contents: read; 60-min leash; actions digest-pinned (peeled SHAs). Also completes the analysis half of 0065-T5." |
| 0069-T3 | ✅ done | 2026-08-19 | "Both OSV hits dispositioned in the VEX with evidence, per the security/vex README rule: GHSA-h395-gr6q-cpjc (jsonwebtoken) is not_affected/component_not_present — the advisory's range ends at 10.3.0 and we ship 11.0.0; a scanner range-matching bug, now machine-readably answered. RUSTSEC-2026-0235 (rkyv) is not_affected/vulnerable_code_not_in_execute_path — the chain is openraft -> byte-unit -> rust_decimal -> rkyv (verified in Cargo.lock; NOT trimmable without forking upstream config, checked), and zero rkyv API call sites exist in the tree; no archive is ever constructed or read. anyhow was already dispositioned (0065-T3). All three scanner hits now have machine-readable answers shipping with every release." |
| 0069-T4 | ✅ done | 2026-08-19 | "All three Dockerfiles pin the base by DIGEST (gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f... — the multi-arch index digest, confirmed against the registry from two independent paths), with the rationale comment: the tag names the intent, the digest IS the base. The operator-e2e.sh quote that made the checker's shell parser give up ('reached EOF without closing quote' at the escaped-dot jsonpath inside nested quotes) is rewritten as a go-template label read — bash -n parses, the vendored bundle re-vendored, and the checker can now finish reading the tree instead of reporting 'possibly incomplete results'." |
| 0069-T5 | ✅ done | 2026-08-19 | "SECURITY.md gains 'The links, in one place': the private advisory form URL, the published advisories page, SUPPORT.md for versions/timelines, the per-release VEX directory, and docs/compliance/ — the linked content the checker (and a reporter at 2 a.m.) looks for." |
| 0069-T6 | ⬜ planned | — |  |
| 0069-T7 | ⬜ planned | — |  |
<!-- /status-table:0069 -->

## Changelog

- **2026-08-19** — T1-T5 (every repository task) shipped in one pass; T6/T7
  remain the maintainer's settings-and-registration acts.

- **2026-08-19** — ADR proposed and delivery opened from the first published
  scorecard run (5.5), same day the badge went live.
