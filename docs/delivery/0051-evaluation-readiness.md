---
adr: "0051"
title: "Evaluation readiness: an assessable, comparable, migratable first release"
adr_status: Proposed
tasks:
  - id: 0051-T1
    title: README truthing & restructure — differentiators and a TOC up top, an architecture sketch, a Bridge section + the missing mqtt-bridge crate-table row, stale facts fixed (44→50 ADRs) with a CI guard on derivable facts, and the MSRV / supported-platforms / pre-1.0-stability statements
    status: planned
  - id: 0051-T2
    title: Secured quickstart — generate certs → TLS + mTLS + ACL → foreign client connects, as a copy-paste block beside the plaintext one, wired into the quickstart-as-test CI job so the secure path cannot rot
    status: planned
  - id: 0051-T3
    title: Community surface — CONTRIBUTING.md (human-facing), CODE_OF_CONDUCT.md, issue/PR templates, changelog policy (GitHub Releases canonical, CHANGELOG.md pointer)
    status: planned
  - id: 0051-T4
    title: Cut v0.9.0 — flip ADR 0045 to Accepted, maintainer pushes the signed tag per RELEASING.md, verify the pipeline's artifacts end to end (first real signatures + SBOM complete 0045-T3/T5)
    status: planned
  - id: 0051-T5
    title: docs/COMPARISON.md + condensed README matrix — Mosquitto / EMQX / NanoMQ; every cell matched / exceeded / missing-by-design (with the deciding ADR) / missing-for-now (with the tracking task); versions pinned, claims dated, losses as prominent as wins
    status: planned
  - id: 0051-T6
    title: Migration from Mosquitto — scripts/migrate/from-mosquitto.py (mosquitto.conf → ADR 0046 TOML, acl_file → ACL TOML, bridge blocks → mqtt-bridge rules) + guide + fixture tests; loud unmapped report, secrets never transformed, output must pass --check-config
    status: planned
  - id: 0051-T7
    title: Migration from EMQX — scripts/migrate/from-emqx.py (listeners, TLS, authn/authz sources, bridges → common-subset TOML) + guide + fixture tests; same three converter rules
    status: planned
  - id: 0051-T8
    title: Migration from NanoMQ — scripts/migrate/from-nanomq.py (listeners, TLS, auth, bridge config → common-subset TOML) + guide + fixture tests; same three converter rules
    status: planned
  - id: 0051-T9
    title: NanoMQ joins the bench harness (amends ADR 0048's competitor set) and the first comparative results are published to docs/benchmarks/ under 0048-T4's honesty rules
    status: planned
  - id: 0051-T10
    title: The bridge made assessable — a demo/ second security zone (Mosquitto upstream + mqtt-bridge with directional rules), a walkthrough doc, and the Grafana screenshot into the README
    status: planned
  - id: 0051-T11
    title: The 1.0.0 freeze — after the bake window, run the ADR 0038 wire/schema review consciously, run the 0039-T3 skew smoke against two real tags, then the maintainer cuts the freeze tag
    status: blocked
    notes: "Needs v0.9.0 shipped (T4), a bake window survived, and a second tag (0.9.x/0.10) to make the 0039-T3 adjacent-skew smoke real — impossible before two releases exist by definition."
---

# Delivery: ADR 0051 — Evaluation readiness

[ADR 0051](../adr/0051-evaluation-readiness.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0051 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0051-T1 | ⬜ planned | — |  |
| 0051-T2 | ⬜ planned | — |  |
| 0051-T3 | ⬜ planned | — |  |
| 0051-T4 | ⬜ planned | — |  |
| 0051-T5 | ⬜ planned | — |  |
| 0051-T6 | ⬜ planned | — |  |
| 0051-T7 | ⬜ planned | — |  |
| 0051-T8 | ⬜ planned | — |  |
| 0051-T9 | ⬜ planned | — |  |
| 0051-T10 | ⬜ planned | — |  |
| 0051-T11 | ⛔ blocked | — | "Needs v0.9.0 shipped (T4), a bake window survived, and a second tag (0.9.x/0.10) to make the 0039-T3 adjacent-skew smoke real — impossible before two releases exist by definition." |
<!-- /status-table:0051 -->

## Plan

| Task | Done means |
|---|---|
| **0051-T1** README truthing | A reader discovers every shipped capability (bridge included), the top of the file argues the differentiators, no stated fact is stale, and MSRV/platforms/stability are explicit. The CI guard fails on a drifted derivable fact. |
| **0051-T2** Secured quickstart | Copy-paste from a clean checkout to a TLS+mTLS+ACL node with a foreign client connected — and CI runs those exact commands. |
| **0051-T3** Community surface | A first-time contributor and a first-time issue reporter each have a path; the changelog policy is written down. |
| **0051-T4** v0.9.0 | ADR 0045 Accepted; the signed tag exists; every published artifact verifies per RELEASING.md (cosign, provenance, SBOM, reproduce). |
| **0051-T5** Comparison | An expert from any of the three brokers can see in one read what they gain, what they lose, and which losses are on purpose. |
| **0051-T6..T8** Migration | A working competitor config converts to a validating mqttd TOML, with every unconverted directive explained — proven by fixture tests per product. |
| **0051-T9** NanoMQ benched | NanoMQ runs in the same harness under the same postures; the first honest numbers are in `docs/benchmarks/` linked from the README. |
| **0051-T10** Bridge assessable | Ten minutes with `demo/` shows a message crossing a security boundary under a deny-by-default rule — and the README shows the dashboard. |
| **0051-T11** 1.0.0 | The freeze is a reviewed act on evidence: bake survived, skew smoke green, ADR 0038 checklist walked. |

## Phased execution plan

| Phase | Tasks | Gate | Output |
|---|---|---|---|
| **A — truth the surface, ship** | T1, T2, T3 → T4 | fixes what actively misleads | `v0.9.0` exists; the repo stops under-selling and mis-stating |
| **B — the evaluation package** | T5–T10 (parallelizable; T9 rides ADR 0048's harness) | during the 0.9.x bake | an evaluator can assess, compare, and migrate without doing our work for us |
| **C — the conscious freeze** | T11 | bake + second tag + ADR 0038 review | `v1.0.0` |

Order inside A: T1/T2/T3 in any order, each its own PR; T4 last (the tag push is the
maintainer's act, gated on the ADR 0044 checklist which is already green).

## Changelog

- **2026-07-27** — ADR 0051 drafted from the release-readiness review: engineering ready
  (ADR 0044 green, ADR 0045 pipeline waiting on a tag), evaluation experience not — the
  finished bridge invisible in the README, no comparison or migration story, plaintext-only
  quickstarts, unpublished benchmarks, stale hand-written facts, missing community files.
  Decision: ship `v0.9.0` now (gated only on what misleads), deliver the evaluation package
  during the bake, keep `1.0.0` as the conscious ADR 0038 freeze. Maintainer widened the
  comparison set to include **NanoMQ** (benchmark + matrix + migration), triggering ADR
  0048's "widen if there is demand" clause — recorded there as an amendment.
