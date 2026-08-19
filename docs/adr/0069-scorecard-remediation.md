# ADR 0069 — Scorecard remediation: a solution per check, honestly bounded

- **Status:** Accepted
- **Date:** 2026-08-19
- **Delivery:** [docs/delivery/0069-scorecard-remediation.md](../delivery/0069-scorecard-remediation.md)
- **Related:** [ADR 0065](0065-security-legibility.md) (T1 put the scorecard in CI;
  this record works its findings; T3's VEX carries the dispositions; T5's CodeQL is
  the SAST answer), [ADR 0045](0045-release-engineering-and-distribution.md)

> This record states the decision only. Progress lives in the delivery doc.

## Context

The first published run scored **5.5/10** (viewer:
`scorecard.dev/viewer/?uri=github.com/mbilling/fss-mqtt-broker`). The value of the
number is that every point lost names a concrete practice with a concrete owner.
This record turns the run's findings into decisions, and — as important — states
which checks **cannot** be moved by work, so nobody burns effort chasing them or,
worse, games them.

Already at 10: Dependency-Update-Tool, Fuzzing, CI-Tests, Packaging, License,
Binary-Artifacts, Dangerous-Workflow. Signed-Releases sits at 8, Contributors at 6.

## Decision — repository work (each a task below)

1. **Token-Permissions (0 → target 10).** The rule the checker enforces is the
   right one: **top-level `contents: read` in every workflow, write grants only at
   job level, only where the job's act needs them.** `examples-bundle.yml` declares
   `contents: write` at top level — move it to the publishing job. Verify every
   workflow against the rule rather than the flagged one alone.
2. **SAST (0 → 10).** CodeQL on pull requests — the analysis half of 0065-T5,
   pulled forward because it is a whole scored check. clippy already runs; CodeQL
   is the independent second opinion the check counts.
3. **Vulnerabilities (7 → 10), the honest way.** Three OSV hits, three different
   truths: `RUSTSEC-2026-0190` (anyhow) is already dispositioned not_affected in
   the VEX (compile-time proc-macro path only); `GHSA-h395-gr6q-cpjc`
   (jsonwebtoken) is a **false positive** — the advisory's affected range ends at
   10.3.0 and we ship 11.0.0 — recorded in the VEX so scanners consuming it stop
   re-flagging; `RUSTSEC-2026-0235` (rkyv, via byte-unit → rust_decimal) needs a
   real disposition: prefer trimming the unused feature edge out of the tree,
   else a VEX not_affected with the vulnerable-path evidence (we deserialize no
   rkyv archives, ever).
4. **Pinned-Dependencies (7 → target 10).** Digest-pin the distroless base images
   in every Dockerfile (`gcr.io/distroless/static-debian12:nonroot@sha256:…`) —
   the same discipline the compose/operator pins already follow for our own
   images. Also fix the unbalanced quote in `scripts/k8s/operator-e2e.sh` that
   makes the checker's shell parser give up ("possibly incomplete results"): a
   gate that cannot finish reading us undercounts us.
5. **Security-Policy (4 → target 10).** SECURITY.md scores as text without links;
   add the URLs the checker (and a reporter) wants: the repository's private
   advisory form, the published advisories page, and SUPPORT.md for fix timelines.

## Decision — maintainer acts (settings and registrations, not commits)

6. **Branch-Protection (0 → realistic 3–6).** Enable a ruleset on `main`: require
   a pull request and passing status checks, forbid force-push and deletion. The
   ceiling is honest: requiring an approving review scores higher but a solo
   maintainer cannot approve their own PRs — set what solo development can
   actually operate, score what that is worth, no more.
7. **CII-Best-Practices (0 → 2–5).** Register at `bestpractices.dev` and
   self-certify: the passing-level criteria (published, tested, disclosure
   process, static analysis, signed releases) are already true here — the missing
   artifact is the registration itself.

## What cannot be moved, stated so nobody tries

- **Maintained (0):** scores repository age; the repo is younger than 90 days.
  Only the calendar fixes it.
- **Code-Review (0):** 0/21 changesets had a second approver — the bus-factor-1
  reality the review panel already named. A second regular reviewer is the only
  honest fix; self-approval theatre would be worse than the zero.
- **Contributors (6):** organization diversity; grows or doesn't with adoption.

Expected trajectory once tasks 1–5 land and acts 6–7 are done: **≈ 8**, with the
remainder held by the three time-and-people checks above — which is the correct
score for a four-week-old solo project, and the badge saying so is the badge
working.

## Tasks

| id | title |
|----|-------|
| 0069-T1 | Token-Permissions: top-level read everywhere, write only at job level where the act needs it |
| 0069-T2 | CodeQL on pull requests (the SAST check; the analysis half of 0065-T5) |
| 0069-T3 | Vulnerabilities: rkyv edge trimmed or dispositioned; jsonwebtoken false positive recorded in the VEX |
| 0069-T4 | Pinned-Dependencies: digest-pin distroless bases in every Dockerfile; fix the operator-e2e.sh quote that breaks the checker's parser |
| 0069-T5 | Security-Policy: SECURITY.md gains the advisory-form, advisories-page, and timeline links |
| 0069-T6 | Maintainer: main-branch ruleset (require PR + status checks, no force-push/deletion), solo-compatible |
| 0069-T7 | Maintainer: bestpractices.dev registration and passing-level self-certification |
