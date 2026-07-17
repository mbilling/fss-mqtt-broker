---
adr: "0048"
title: "Comparative performance benchmarking (published, reproducible, honest)"
adr_status: Proposed
tasks:
  - id: 0048-T1
    title: Containerized load harness — an established MQTT benchmark client + docker-compose that stands up each broker (ours, Mosquitto, EMQX) from its published image with documented reasonable config; same hardware, pinned versions, security posture held constant and disclosed
    status: planned
  - id: 0048-T2
    title: The selection metrics — sustained throughput (QoS 0/1/2), end-to-end latency p50/p99/p999, memory per idle connection at scale, connection-establishment rate (mTLS included); full distributions, never a single number
    status: planned
  - id: 0048-T3
    title: The scaling curve — the same workload against 1/3/5 nodes, throughput and p99 vs node count; tests capability claim 1 and the ADR 0015 shared-subscription mechanism end to end; a flat curve is a finding to fix
    status: planned
  - id: 0048-T4
    title: Honesty rules + publication — versions/hardware/config/date stated; losing dimensions reported as prominently as winning ones; results in docs/benchmarks/ linked from the README; self-benchmark runs nightly (ADR 0044 P4), cross-broker re-run per release
    status: planned
---

# Delivery: ADR 0048 — Comparative performance benchmarking

[ADR 0048](../adr/0048-comparative-benchmarking.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0048 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0048-T1 | ⬜ planned | — |  |
| 0048-T2 | ⬜ planned | — |  |
| 0048-T3 | ⬜ planned | — |  |
| 0048-T4 | ⬜ planned | — |  |
<!-- /status-table:0048 -->

## Plan

| Task | Done means |
|---|---|
| **0048-T1** Harness | `docker compose up` reproduces the comparison: each broker from its image, one load tool, one hardware profile, disclosed configs, constant security posture. |
| **0048-T2** Metrics | Throughput, latency (p50/p99/p999), memory/connection at scale, and mTLS connection rate — each with its distribution and the load that produced it. |
| **0048-T3** Scaling curve | Throughput + p99 vs 1/3/5 nodes, published; the linear-scaling claim earned or the gap surfaced. |
| **0048-T4** Honesty + publish | Results (with versions/hardware/date, wins and losses) in `docs/benchmarks/`; self-benchmark nightly, cross-broker per release. |

Order: T1 → T2 → T3 → T4.

## Changelog

- **2026-07-17** — ADR 0048 drafted. Differentiation/credibility: "Fast" and "linearly
  scalable" are in the product's own name but unproven; extends ADR 0044 P6's internal
  baselines to published, reproducible, self-critical cross-broker numbers. Priority **P2**.
