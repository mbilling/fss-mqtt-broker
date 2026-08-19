# ADR 0065 — Security legibility: scorecard, VEX, dependency automation, lifecycle statements

- **Status:** Proposed
- **Date:** 2026-08-19
- **Delivery:** [docs/delivery/0065-security-legibility.md](../delivery/0065-security-legibility.md)
- **Related:** [ADR 0044](0044-release-readiness-assurance.md) (the assurance program this
  makes legible), [ADR 0045](0045-release-engineering-and-distribution.md) (SBOM/provenance/signing),
  [ADR 0039](0039-versioning-and-upgrade-policy.md) (the support policy T4 restates)

> This record states the decision only. Progress lives in the delivery doc.

## Context

The assurance substance exists: per-artifact CycloneDX SBOMs, SLSA build-provenance
attestations, keyless cosign signatures, reproducible builds, cargo-deny/cargo-audit
gates on every PR and release, nightly fuzzing, and a SECURITY.md with private
coordinated disclosure. What does not exist is the **legible layer** — the artifacts an
enterprise vendor-risk process looks for *by name*, often via automated OSS-intake
tooling that never reads an ADR. A project that is more secure than its checklist
score looks exactly like one that is less: the checklist is the interface.

Two concrete gaps have teeth beyond optics. First, enterprises scan our SBOMs, find
CVEs in transitive dependencies, and have no machine-readable answer to "is mqttd
affected?" — every scanner hit becomes a support ticket or a silent disqualification.
Second, the dependency *gates* are reactive: nothing produces an inbound stream of
update PRs, so a vulnerable dependency waits for a human to notice the audit failure.

## Decision

Make the existing assurance program externally legible, in six pieces:

1. **OpenSSF Scorecard in CI, badge in the README.** The Scorecard action runs on a
   schedule and publishes results; what it flags (unpinned action digests, missing
   dependency automation) is remediated rather than argued with — its checklist is
   the one intake tooling runs.
2. **Dependency-update automation** (Renovate or Dependabot): an inbound PR stream
   for Rust crates, GitHub Actions, and the Python tooling, riding the existing
   cargo-deny/audit/test gates. Automation proposes; the gates and a human dispose.
3. **VEX statements per release** (OpenVEX), published beside the SBOMs. Each release
   carries a machine-readable disposition for scanner-visible findings in the
   dependency tree — "not affected, vulnerable code not present / not reachable" with
   justification, or "affected, fixed in X". The release pipeline emits the document;
   the analysis behind each claim is recorded in the delivery doc's evidence.
4. **SUPPORT.md**: the ADR 0039 policy (three most recent minor lines, adjacent skew,
   sequential majors) restated as a dated, procurement-quotable lifecycle table —
   which line is supported until when, where security fixes land.
5. **Export-control statement**: the ECCN classification note (5D002 with the
   publicly-available §740.13(e) treatment) enterprises' trade-compliance reviews ask
   for, stated once in the README/SUPPORT.md instead of re-answered per inquiry.
6. **External validation of the fuzzing and analysis**: OSS-Fuzz onboarding for the
   six fuzz targets (external compute, continuous, public trophies) and CodeQL as a
   second static-analysis opinion beside clippy. A funded third-party security audit
   (OSTIF-style, published in-repo unredacted) is the endgame — it is the strongest
   available substitute for the production track record the project honestly lacks.

## What this deliberately is not

No new security *capability* ships here — this ADR is the legibility layer over
ADR 0044/0045's substance. Anything that changes the broker's behaviour (FIPS,
SIEM export) lives in its own record (ADR 0066, 0068).

## Consequences

- Scorecard will initially flag real debt (action digest pinning, branch-protection
  visibility); fixing it is part of T1, not a separate negotiation.
- VEX claims are assertions with our name on them: a "not affected" requires the same
  evidence discipline as a delivery-doc claim, and a wrong one is worse than none.
- Renovate/Dependabot PRs are noise if unbounded — grouped updates, scheduled weekly,
  security updates immediate.

## Tasks

| id | title |
|----|-------|
| 0065-T1 | OpenSSF Scorecard action + badge; remediate what it flags (pinned digests, token permissions) |
| 0065-T2 | Dependency-update automation (Renovate or Dependabot) over crates, actions, and Python tooling, grouped and gated |
| 0065-T3 | OpenVEX per release, emitted by the pipeline beside the SBOMs, with an evidence rule for every "not affected" |
| 0065-T4 | SUPPORT.md — the ADR 0039 lifecycle as a dated table — plus the export-control (ECCN) statement |
| 0065-T5 | OSS-Fuzz onboarding for the six fuzz targets; CodeQL beside clippy in CI |
| 0065-T6 | Funded third-party security audit, findings published in-repo with their fixes |
