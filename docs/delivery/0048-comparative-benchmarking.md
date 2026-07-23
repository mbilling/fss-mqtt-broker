---
adr: "0048"
title: "Comparative performance benchmarking (published, reproducible, honest)"
adr_status: Accepted
tasks:
  - id: 0048-T1
    title: Containerized load harness — emqtt-bench + docker-compose that stands up each broker (ours, Mosquitto, EMQX) from its published image with documented reasonable config; same hardware, pinned versions, security posture held constant and disclosed
    status: done
    date: 2026-07-23
    evidence: "bench/: compose profiles run ONE broker at a time (mqttd built from source; Mosquitto 2.0.20; EMQX 5.8.6 — pinned), driven by emqtt-bench 0.6.3 (EMQX's own tool, ADR §3). run.sh executes identical scenarios per broker — connection-rate (timed window; emqtt_bench conn holds connections and never exits, learned in smoke), sustained pub/sub at QoS 0/1/2 (N pubs → N subs, 256 B), RSS snapshot — capturing raw logs + env.txt (versions/params/host, dev-grade label) per run; results/ is gitignored. Posture held constant and disclosed: plaintext/anonymous/in-memory on all three (mqttd explicitly opts out of durable-by-default; TLS posture is T2). Smoke-verified end-to-end on all three brokers: 100/100 connects, publishes complete at every QoS, exit 0."
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
| 0048-T1 | ✅ done | 2026-07-23 | "bench/: compose profiles run ONE broker at a time (mqttd built from source; Mosquitto 2.0.20; EMQX 5.8.6 — pinned), driven by emqtt-bench 0.6.3 (EMQX's own tool, ADR §3). run.sh executes identical scenarios per broker — connection-rate (timed window; emqtt_bench conn holds connections and never exits, learned in smoke), sustained pub/sub at QoS 0/1/2 (N pubs → N subs, 256 B), RSS snapshot — capturing raw logs + env.txt (versions/params/host, dev-grade label) per run; results/ is gitignored. Posture held constant and disclosed: plaintext/anonymous/in-memory on all three (mqttd explicitly opts out of durable-by-default; TLS posture is T2). Smoke-verified end-to-end on all three brokers: 100/100 connects, publishes complete at every QoS, exit 0." |
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

## Phased execution plan

Phased so **each step delivers value even if we stop there**, and so cost is deferred to the
last responsible moment — the harness and the numbers that *guide* us cost nothing; only the
numbers we *publish* cost money.

| Phase | Task | Cost | Output |
|---|---|---|---|
| **1. Harness** | T1 | none — start now | Containerized rig: fss / Mosquitto / EMQX from **pinned published images**, documented *reasonable* configs (theirs not crippled, ours not tuned), driven by **`emqtt-bench`** (EMQX's own load tool — a built-in honesty signal). **Two postures per broker: plaintext and TLS/mTLS**, disclosed. |
| **2. Dev-grade numbers** | T2 | none — local | Throughput QoS 0/1/2, latency p50/p99/p999, memory per 10k idle connections, mTLS connect rate — **full distributions**. Run on a workstation, labeled **development-grade**: they *guide* decisions, they are **not published and never quoted**. |
| **3. Publishable run** | T2 | small — one rented box for an afternoon (optionally two: driver + broker) | The same metrics, pinned everything, **raw output committed**. The **only** step with a cash cost, and the **only** numbers that go into `docs/benchmarks/`. |
| **4. Scaling curve** | T3 | small — 3–5 small cloud VMs for hours | 1/3/5-node throughput + p99 vs node count, **on separate hosts with independent disks**. A durable cluster is fsync-bound (ADR 0026/0027); a single-host curve would scale *negatively* and manufacture false evidence against us — so this runs on real separate hosts or it is not published. |
| **5. Publish** | T4 | none | `docs/benchmarks/` with versions/hardware/date, **losses printed as prominently as wins**, README Performance section links it. Nightly self-benchmark (ADR 0044 P4) guards regression; the cross-broker comparison is re-run per release. |

**The dev-grade / publishable line is the crux of the honesty story:** local numbers are
cheap and plentiful but run on shared, noisy, un-pinned hardware, so they steer the work
without ever becoming a quotable claim. A number only earns publication once it comes from
the pinned, dedicated, disclosed environment of phases 3–4.

## Changelog

- **2026-07-17** — ADR 0048 drafted. Differentiation/credibility: "Fast" and "linearly
  scalable" are in the product's own name but unproven; extends ADR 0044 P6's internal
  baselines to published, reproducible, self-critical cross-broker numbers. Priority **P2**.
- **2026-07-19** — Phased execution plan added (above), and two decision-level refinements
  folded into the ADR:
  - **`emqtt-bench` named as the load driver** — measuring ourselves with EMQX's own tool is
    an honesty signal; each broker measured in two disclosed postures (plaintext + TLS/mTLS).
  - **The scaling curve must run on separate hosts/disks.** A durable cluster is fsync-bound
    (ADR 0026/0027 — group-commit exists because per-message follower fsyncs were the
    bottleneck); a single-host N-node curve contends on one disk queue, scales negatively, and
    would publish false evidence *against* the broker. Curve runs on real separate hosts or not
    at all.
  Cost stays bounded: phases 1–2 (harness + dev-grade local numbers) are free and only guide;
  the sole cash outlay is the one publishable run (a rented box) plus a few VM-hours for the
  curve. Tasks remain **planned** — this is planning, not execution.
- **2026-07-19** — The single-host lesson is now backed by its primary source: the
  [7-node HA-bridge post-mortem](../postmortems/2026-07-14-ha-bridge-durable-refused.md) is
  filed under `docs/postmortems/` and cited from the ADR's scaling-curve decision. (The
  post-mortem also surfaces two real defects — learner-owner durable recovery, and a
  readiness blind spot — tracked separately, not by this ADR.)
