---
adr: "0070"
title: "Usage documentation is owned by stakeholders, not by features"
adr_status: Proposed
tasks:
  - id: 0070-T1
    title: "docs/README.md index (every doc, its stakeholder, one line) + the README persona fork; orphans routed"
    status: done
    date: 2026-09-11
    issue: 555
    evidence: "docs/README.md lists every top-level docs/*.md with Document/Stakeholder/One line. README Start here forks evaluate/run/build/secure/contribute and routes HARDENING, THREAT-MODEL, TEST-PLAN from the front door. CI: check-readme-facts.py T8 index gate."
  - id: 0070-T2
    title: "CLIENT-GUIDE.md: session/expiry semantics, the emitted reason-code catalogue (held to the CI-gated list), flow-control and refusal behaviour, worked client examples"
    status: done
    date: 2026-09-11
    issue: 556
    evidence: "docs/CLIENT-GUIDE.md: ADR 0009 session/expiry, ADR 0012 flow-control, refusals, mosquitto+paho examples. Emitted >=0x80 table is between reason-codes markers and rewritten by scripts/check-reason-codes.py; CI runs that script with --check."
  - id: 0070-T3
    title: "Generated CONFIGURATION.md from the config code, CI-checked like STATUS.md; README's env list reduces to a routed summary"
    status: done
    date: 2026-09-11
    issue: 557
    evidence: "scripts/gen-configuration.py writes docs/CONFIGURATION.md from mqtt-config ENV_VARS+overlay+field rustdoc; CI gen-configuration.py --check. README Configuration tables replaced by a routed summary pointing at that file. Overlay vars that already worked were added to ENV_VARS (count 89->94) so the generator cannot omit them."
  - id: 0070-T4
    title: "Shipped alerting: PrometheusRule in the chart + production dashboards + a runbook section per alert"
    status: done
    date: 2026-09-11
    issue: 558
    evidence: "deploy/helm/mqttd/templates/prometheusrule.yaml (metrics.prometheusRule.enabled, default false). Production dashboards in deploy/observability/grafana/. OPERATIONS.md shipped-alerting index plus a ### Alert: heading per rule whose GitHub anchor matches the PrometheusRule runbook fragment."
  - id: 0070-T5
    title: "Kubernetes surface: helm chart READMEs, values reference, MqttdCluster CRD reference"
    status: done
    date: 2026-09-11
    issue: 559
    evidence: "docs/KUBERNETES.md routes to deploy/helm/mqttd/README.md (values) and deploy/helm/mqttd-operator/README.md (MqttdCluster CRD spec/status). README Kubernetes section points at KUBERNETES.md."
  - id: 0070-T6
    title: "ARCHITECTURE.md for contributors (the main.rs module-doc map and hub seams, promoted to prose)"
    status: done
    date: 2026-09-11
    issue: 560
    evidence: "docs/ARCHITECTURE.md: crate map, lib.rs modules, ADR 0064 hub seams, CONTRIBUTING.md pointer."
  - id: 0070-T7
    title: "EVALUATION.md one-pager; MIGRATION.md gains per-source entry points; CAPABILITY-PLAN refreshed or retired"
    status: done
    date: 2026-09-11
    issue: 561
    evidence: "docs/EVALUATION.md one-pager. MIGRATION.md per-source jump table (Mosquitto/EMQX/HiveMQ + dual-run). CAPABILITY-PLAN.md retired as a living plan; live status is delivery/STATUS.md, scaling gates on #537."
  - id: 0070-T8
    title: "The index gate: docs/README.md completeness check joins check-readme-facts.py; version stamps on every stakeholder doc"
    status: done
    date: 2026-09-11
    issue: 562
    evidence: "scripts/check-readme-facts.py: every docs/*.md except the index must appear as ](name) in docs/README.md, the Document/Stakeholder/One line table must exist, and each file header must carry Verified against / Dated / Generated against / GENERATED."
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
| 0070-T1 | ✅ done | [#555](https://github.com/mbilling/fss-mqtt-broker/issues/555) | 2026-09-11 | "docs/README.md lists every top-level docs/*.md with Document/Stakeholder/One line. README Start here forks evaluate/run/build/secure/contribute and routes HARDENING, THREAT-MODEL, TEST-PLAN from the front door. CI: check-readme-facts.py T8 index gate." |
| 0070-T2 | ✅ done | [#556](https://github.com/mbilling/fss-mqtt-broker/issues/556) | 2026-09-11 | "docs/CLIENT-GUIDE.md: ADR 0009 session/expiry, ADR 0012 flow-control, refusals, mosquitto+paho examples. Emitted >=0x80 table is between reason-codes markers and rewritten by scripts/check-reason-codes.py; CI runs that script with --check." |
| 0070-T3 | ✅ done | [#557](https://github.com/mbilling/fss-mqtt-broker/issues/557) | 2026-09-11 | "scripts/gen-configuration.py writes docs/CONFIGURATION.md from mqtt-config ENV_VARS+overlay+field rustdoc; CI gen-configuration.py --check. README Configuration tables replaced by a routed summary pointing at that file. Overlay vars that already worked were added to ENV_VARS (count 89->94) so the generator cannot omit them." |
| 0070-T4 | ✅ done | [#558](https://github.com/mbilling/fss-mqtt-broker/issues/558) | 2026-09-11 | "deploy/helm/mqttd/templates/prometheusrule.yaml (metrics.prometheusRule.enabled, default false). Production dashboards in deploy/observability/grafana/. OPERATIONS.md shipped-alerting index plus a ### Alert: heading per rule whose GitHub anchor matches the PrometheusRule runbook fragment." |
| 0070-T5 | ✅ done | [#559](https://github.com/mbilling/fss-mqtt-broker/issues/559) | 2026-09-11 | "docs/KUBERNETES.md routes to deploy/helm/mqttd/README.md (values) and deploy/helm/mqttd-operator/README.md (MqttdCluster CRD spec/status). README Kubernetes section points at KUBERNETES.md." |
| 0070-T6 | ✅ done | [#560](https://github.com/mbilling/fss-mqtt-broker/issues/560) | 2026-09-11 | "docs/ARCHITECTURE.md: crate map, lib.rs modules, ADR 0064 hub seams, CONTRIBUTING.md pointer." |
| 0070-T7 | ✅ done | [#561](https://github.com/mbilling/fss-mqtt-broker/issues/561) | 2026-09-11 | "docs/EVALUATION.md one-pager. MIGRATION.md per-source jump table (Mosquitto/EMQX/HiveMQ + dual-run). CAPABILITY-PLAN.md retired as a living plan; live status is delivery/STATUS.md, scaling gates on #537." |
| 0070-T8 | ✅ done | [#562](https://github.com/mbilling/fss-mqtt-broker/issues/562) | 2026-09-11 | "scripts/check-readme-facts.py: every docs/*.md except the index must appear as ](name) in docs/README.md, the Document/Stakeholder/One line table must exist, and each file header must carry Verified against / Dated / Generated against / GENERATED." |
<!-- /status-table:0070 -->

## Changelog

- **2026-08-19** — ADR proposed and delivery opened, from a full documentation
  inventory: ~40 documents, >500 KB, individually strong and collectively
  unowned; the review panel's recorded complaints (#255–#257) plus the orphan
  and drift measurements in the ADR's context section are the evidence base.
- **2026-09-11** — T1–T8 delivered: stakeholder index + README persona fork,
  CLIENT-GUIDE, generated CONFIGURATION.md, PrometheusRule + production
  dashboards + per-alert runbooks, Helm/CRD READMEs, ARCHITECTURE.md,
  EVALUATION.md / MIGRATION entry points / CAPABILITY-PLAN retired as a living
  plan, index gate + version stamps. Issues #555–#562.
