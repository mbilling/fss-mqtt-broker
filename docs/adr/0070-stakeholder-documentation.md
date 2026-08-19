# ADR 0070 — Usage documentation is owned by stakeholders, not by features

- **Status:** Proposed
- **Date:** 2026-08-19
- **Delivery:** [docs/delivery/0070-stakeholder-documentation.md](../delivery/0070-stakeholder-documentation.md)
- **Related:** [ADR 0051](0051-evaluation-readiness.md) (the evaluation-readiness
  push this extends), [ADR 0066](0066-threat-model-and-hardening-baseline.md) /
  [ADR 0067](0067-compliance-framework-mappings.md) (stakeholder docs this routes),
  the 2026-08-13 review panel (whose personas are the seed stakeholder set)

> This record states the decision only. Progress lives in the delivery doc.

## Context

The documentation grew the way documentation always grows: per feature, at the
moment of shipping. The result is ~40 documents totalling over half a megabyte that
are individually strong and collectively unowned — organized by what was built,
not by who is reading. The investigation behind this ADR found, concretely:

- **The README is eight documents in one** (122 KB, 1,605 lines) serving newcomer,
  evaluator, operator and contributor at once — and the review panel's newcomer
  already said it: "README orders jargon before the primer" (#255).
- **High-value docs are orphaned.** HARDENING.md has **zero** inbound links from
  any non-ADR document; THREAT-MODEL.md has one (from HARDENING). The two documents
  a security architect most needs are the two they cannot find from the front door.
- **The largest stakeholder has no document at all**: an application developer
  writing an MQTT client against mqttd finds no client-facing contract — session
  semantics, the reason codes mqttd actually emits (a catalogue that exists as a
  CI-gated script but no prose), refusal behaviour, no code examples in any
  language. Paho appears in this repository only as a test oracle.
- **The config surface is split across three places that disagree by count**:
  100 `MQTTD_*` variables in code, 83 named in the README, one annotated example
  TOML — three surfaces, no single reference, nothing generated.
- **The operator's 3 a.m. surface is prose**: 20+ alert rules live as a markdown
  table in OPERATIONS.md; no shipped `PrometheusRule`, dashboards only inside the
  experimental demo, no runbook keyed per alert.
- **Compliance content is trapped in ADR-space** (0067's mappings), which the
  project's own review method forbids external evaluators from reading.
- Freshness gates cover README/COMPARISON/benchmarks citations and four docs'
  policy phrases — every other document has no gate at all.

## Decision

**Every stakeholder gets one named primary document, and every document gets a
named primary stakeholder.** Ten stakeholders, from the panel's personas plus the
audiences later ADRs created:

| Stakeholder | Primary document | State |
|---|---|---|
| Evaluator / decision-maker | COMPARISON.md + a one-page EVALUATION entry | exists + **new** |
| Newcomer standing it up | README quickstart → SECURED-CLUSTER-TUTORIAL | exists |
| **Application developer** | **CLIENT-GUIDE.md** | **new — the largest gap** |
| Operator / SRE (day 2) | OPERATIONS.md + shipped alerts with per-alert runbooks | exists + **new artifact** |
| Kubernetes user | Helm/operator READMEs + values & CRD reference | **new** |
| Migrator | MIGRATION.md with per-source entry points | exists, restructure |
| Security architect | THREAT-MODEL.md | exists, unrouted |
| Auditor | HARDENING.md + AUDIT-SCHEMA.md | exists, unrouted |
| Compliance / procurement | docs/compliance/ (ADR 0067) + SUPPORT.md | 0067's tasks |
| Contributor | ARCHITECTURE.md + CONTRIBUTING/TEST-PLAN | **new** + exists |

Three structural rules make the model hold:

1. **Routing.** `docs/README.md` becomes the index — every document listed with
   its stakeholder and one line — and the README's "Start here" gains the persona
   fork: *evaluate it · run it · build against it · secure and audit it ·
   contribute*. The orphans get their inbound path from the front door.
2. **Generation over transcription.** The config reference is **generated** from
   the config code the way STATUS.md is generated from delivery front-matter, and
   CI-checked the same way — the 100-vs-83 drift class dies rather than gets
   fixed once. The reason-code catalogue in CLIENT-GUIDE.md is held to the
   CI-gated emission list by the same discipline.
3. **Gates follow the docs.** The index is mechanically checkable (every
   `docs/*.md` appears in `docs/README.md` — an unlisted doc fails CI, the
   orphan class dies); stakeholder docs carry the version-stamp convention
   THREAT-MODEL/HARDENING established.

## What this deliberately is not

Not a rewrite. The existing documents are good; the failure is ownership and
routing, so the work is: five new documents where a stakeholder has none, one
generated reference, one shipped artifact (alerts + runbooks), and wiring. The
ADR/delivery layer stays internal — it is the *source* the stakeholder docs
distill, never the thing an external reader is sent to.

## Consequences

- The README shrinks over time: sections whose stakeholder now has a primary
  document reduce to a routed summary. Its facts-gate anchors survive each move.
- New features owe a docs-routing line: "which stakeholder document carries
  this?" joins the review checklist alongside the ADR 0038 §D must-touch rule.
- The generated config reference makes undocumented knobs a CI failure — which
  is the point, and will sting exactly once per forgotten knob.

## Tasks

| id | title |
|----|-------|
| 0070-T1 | docs/README.md index (every doc, its stakeholder, one line) + the README persona fork; orphans routed |
| 0070-T2 | CLIENT-GUIDE.md: session/expiry semantics, the emitted reason-code catalogue (held to the CI-gated list), flow-control and refusal behaviour, worked client examples |
| 0070-T3 | Generated CONFIGURATION.md from the config code, CI-checked like STATUS.md; README's env list reduces to a routed summary |
| 0070-T4 | Shipped alerting: PrometheusRule in the chart + production dashboards + a runbook section per alert (OPERATIONS's table becomes the generated source or retires) |
| 0070-T5 | Kubernetes surface: helm chart READMEs, values reference, MqttdCluster CRD reference |
| 0070-T6 | ARCHITECTURE.md for contributors (the main.rs module-doc map and hub seams, promoted to prose) |
| 0070-T7 | EVALUATION.md one-pager; MIGRATION.md gains per-source entry points; CAPABILITY-PLAN refreshed or retired |
| 0070-T8 | The index gate: docs/README.md completeness check joins check-readme-facts.py; version stamps on every stakeholder doc |
