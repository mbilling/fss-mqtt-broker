---
adr: "0070"
title: "Usage documentation is owned by stakeholders, not by features"
adr_status: Proposed
tasks:
  - id: 0070-T1
    title: "docs/README.md index (every doc, its stakeholder, one line) + the README persona fork; orphans routed"
    status: planned
    issue: 555
    notes: "Fixes the recorded orphans: HARDENING.md (zero inbound links outside ADRs), THREAT-MODEL.md (one), TEST-PLAN.md (one)."
  - id: 0070-T2
    title: "CLIENT-GUIDE.md: session/expiry semantics, the emitted reason-code catalogue (held to the CI-gated list), flow-control and refusal behaviour, worked client examples"
    status: planned
    issue: 556
    notes: "The largest gap: the application developer has zero dedicated surface today; the reason-code catalogue exists only as scripts/check-reason-codes.py's gated list."
  - id: 0070-T3
    title: "Generated CONFIGURATION.md from the config code, CI-checked like STATUS.md; README's env list reduces to a routed summary"
    status: planned
    issue: 557
    notes: "Kills the drift class measured at 100 MQTTD_* vars in code vs 83 in README."
  - id: 0070-T4
    title: "Shipped alerting: PrometheusRule in the chart + production dashboards + a runbook section per alert"
    status: planned
    issue: 558
    notes: "OPERATIONS.md's 20+ alert rules are prose today; dashboards exist only inside the experimental demo."
  - id: 0070-T5
    title: "Kubernetes surface: helm chart READMEs, values reference, MqttdCluster CRD reference"
    status: planned
    issue: 559
  - id: 0070-T6
    title: "ARCHITECTURE.md for contributors (the main.rs module-doc map and hub seams, promoted to prose)"
    status: planned
    issue: 560
  - id: 0070-T7
    title: "EVALUATION.md one-pager; MIGRATION.md gains per-source entry points; CAPABILITY-PLAN refreshed or retired"
    status: planned
    issue: 561
  - id: 0070-T8
    title: "The index gate: docs/README.md completeness check joins check-readme-facts.py; version stamps on every stakeholder doc"
    status: planned
    issue: 562
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
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0070-T1 | ⬜ planned | [#555](https://github.com/mbilling/fss-mqtt-broker/issues/555) | — | "Fixes the recorded orphans: HARDENING.md (zero inbound links outside ADRs), THREAT-MODEL.md (one), TEST-PLAN.md (one)." |
| 0070-T2 | ⬜ planned | [#556](https://github.com/mbilling/fss-mqtt-broker/issues/556) | — | "The largest gap: the application developer has zero dedicated surface today; the reason-code catalogue exists only as scripts/check-reason-codes.py's gated list." |
| 0070-T3 | ⬜ planned | [#557](https://github.com/mbilling/fss-mqtt-broker/issues/557) | — | "Kills the drift class measured at 100 MQTTD_* vars in code vs 83 in README." |
| 0070-T4 | ⬜ planned | [#558](https://github.com/mbilling/fss-mqtt-broker/issues/558) | — | "OPERATIONS.md's 20+ alert rules are prose today; dashboards exist only inside the experimental demo." |
| 0070-T5 | ⬜ planned | [#559](https://github.com/mbilling/fss-mqtt-broker/issues/559) | — |  |
| 0070-T6 | ⬜ planned | [#560](https://github.com/mbilling/fss-mqtt-broker/issues/560) | — |  |
| 0070-T7 | ⬜ planned | [#561](https://github.com/mbilling/fss-mqtt-broker/issues/561) | — |  |
| 0070-T8 | ⬜ planned | [#562](https://github.com/mbilling/fss-mqtt-broker/issues/562) | — |  |
<!-- /status-table:0070 -->

## Changelog

- **2026-08-19** — ADR proposed and delivery opened, from a full documentation
  inventory: ~40 documents, >500 KB, individually strong and collectively
  unowned; the review panel's recorded complaints (#255–#257) plus the orphan
  and drift measurements in the ADR's context section are the evidence base.
