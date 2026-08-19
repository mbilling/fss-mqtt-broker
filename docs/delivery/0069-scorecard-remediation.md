---
adr: "0069"
title: "Scorecard remediation: a solution per check, honestly bounded"
adr_status: Proposed
tasks:
  - id: 0069-T1
    title: "Token-Permissions: top-level read everywhere, write only at job level where the act needs it"
    status: planned
  - id: 0069-T2
    title: "CodeQL on pull requests (the SAST check; the analysis half of 0065-T5)"
    status: planned
  - id: 0069-T3
    title: "Vulnerabilities: rkyv edge trimmed or dispositioned; jsonwebtoken false positive recorded in the VEX"
    status: planned
    notes: "jsonwebtoken GHSA-h395-gr6q-cpjc: affected range ends 10.3.0, shipped 11.0.0 — a scanner false positive the VEX exists to silence. rkyv 0.7 arrives via byte-unit -> rust_decimal; no rkyv archive is ever deserialized."
  - id: 0069-T4
    title: "Pinned-Dependencies: digest-pin distroless bases in every Dockerfile; fix the operator-e2e.sh quote that breaks the checker's parser"
    status: planned
  - id: 0069-T5
    title: "Security-Policy: SECURITY.md gains the advisory-form, advisories-page, and timeline links"
    status: planned
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
| 0069-T1 | ⬜ planned | — |  |
| 0069-T2 | ⬜ planned | — |  |
| 0069-T3 | ⬜ planned | — | "jsonwebtoken GHSA-h395-gr6q-cpjc: affected range ends 10.3.0, shipped 11.0.0 — a scanner false positive the VEX exists to silence. rkyv 0.7 arrives via byte-unit -> rust_decimal; no rkyv archive is ever deserialized." |
| 0069-T4 | ⬜ planned | — |  |
| 0069-T5 | ⬜ planned | — |  |
| 0069-T6 | ⬜ planned | — |  |
| 0069-T7 | ⬜ planned | — |  |
<!-- /status-table:0069 -->

## Changelog

- **2026-08-19** — ADR proposed and delivery opened from the first published
  scorecard run (5.5), same day the badge went live.
