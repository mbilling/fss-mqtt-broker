---
adr: "0070"
title: "Usage documentation is owned by stakeholders, not by features"
adr_status: Proposed
tasks:
  - id: 0070-T1
    title: "docs/README.md index (every doc, its stakeholder, one line) + the README persona fork; orphans routed"
    status: planned
    notes: "Fixes the recorded orphans: HARDENING.md (zero inbound links outside ADRs), THREAT-MODEL.md (one), TEST-PLAN.md (one)."
  - id: 0070-T2
    title: "CLIENT-GUIDE.md: session/expiry semantics, the emitted reason-code catalogue (held to the CI-gated list), flow-control and refusal behaviour, worked client examples"
    status: planned
    notes: "The largest gap: the application developer has zero dedicated surface today; the reason-code catalogue exists only as scripts/check-reason-codes.py's gated list."
  - id: 0070-T3
    title: "Generated CONFIGURATION.md from the config code, CI-checked like STATUS.md; README's env list reduces to a routed summary"
    status: planned
    notes: "Kills the drift class measured at 100 MQTTD_* vars in code vs 83 in README."
  - id: 0070-T4
    title: "Shipped alerting: PrometheusRule in the chart + production dashboards + a runbook section per alert"
    status: planned
    notes: "OPERATIONS.md's 20+ alert rules are prose today; dashboards exist only inside the experimental demo."
  - id: 0070-T5
    title: "Kubernetes surface: helm chart READMEs, values reference, MqttdCluster CRD reference"
    status: planned
  - id: 0070-T6
    title: "ARCHITECTURE.md for contributors (the main.rs module-doc map and hub seams, promoted to prose)"
    status: planned
  - id: 0070-T7
    title: "EVALUATION.md one-pager; MIGRATION.md gains per-source entry points; CAPABILITY-PLAN refreshed or retired"
    status: planned
  - id: 0070-T8
    title: "The index gate: docs/README.md completeness check joins check-readme-facts.py; version stamps on every stakeholder doc"
    status: planned
---

# Delivery — ADR 0070: Stakeholder-owned usage documentation

Decision: [docs/adr/0070-stakeholder-documentation.md](../adr/0070-stakeholder-documentation.md).

Ten stakeholders, each with one named primary document; routing from the front
door; generation over transcription for the reference material; gates that make
an unlisted or drifting doc a CI failure. Sequenced by pain: the index and the
client guide first (a whole stakeholder with nothing), then the generated config
reference, then the operator's shipped alerts.

## Progress

<!-- status-table:0070 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0070-T1 | ⬜ planned | — | "Fixes the recorded orphans: HARDENING.md (zero inbound links outside ADRs), THREAT-MODEL.md (one), TEST-PLAN.md (one)." |
| 0070-T2 | ⬜ planned | — | "The largest gap: the application developer has zero dedicated surface today; the reason-code catalogue exists only as scripts/check-reason-codes.py's gated list." |
| 0070-T3 | ⬜ planned | — | "Kills the drift class measured at 100 MQTTD_* vars in code vs 83 in README." |
| 0070-T4 | ⬜ planned | — | "OPERATIONS.md's 20+ alert rules are prose today; dashboards exist only inside the experimental demo." |
| 0070-T5 | ⬜ planned | — |  |
| 0070-T6 | ⬜ planned | — |  |
| 0070-T7 | ⬜ planned | — |  |
| 0070-T8 | ⬜ planned | — |  |
<!-- /status-table:0070 -->

## Changelog

- **2026-08-19** — ADR proposed and delivery opened, from a full documentation
  inventory: ~40 documents, >500 KB, individually strong and collectively
  unowned; the review panel's recorded complaints (#255–#257) plus the orphan
  and drift measurements in the ADR's context section are the evidence base.
