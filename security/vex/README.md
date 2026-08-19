# VEX statements (ADR 0065 T3)

Enterprises scan our SBOMs, find advisories in transitive dependencies, and need a
machine-readable answer to "is mqttd affected?". This directory is that answer's
source of truth: [`statements.json`](statements.json) is an
[OpenVEX](https://github.com/openvex/spec) document with `@VERSION@`/`@TIMESTAMP@`
placeholders; the release pipeline stamps it and publishes
`vex-<version>.openvex.json` beside the SBOM assets on every release.

**The evidence rule** (the ADR's condition for this file existing at all): every
`not_affected` statement carries a `justification` from the OpenVEX vocabulary AND
an `impact_statement` naming the concrete analysis — how the dependency enters the
tree, why the vulnerable code cannot execute, and where the verification is
recorded. A disposition nobody can check is worse than none; a PR adding a bare
`not_affected` should be rejected in review.

Maintenance: when `cargo audit` (CI-gated) reports a new advisory, the choice is
fix (update the dependency — the normal path, automated by Dependabot) or
disposition (add a statement here, with evidence). An advisory that is neither
fixed nor dispositioned keeps CI red — that is the design.
