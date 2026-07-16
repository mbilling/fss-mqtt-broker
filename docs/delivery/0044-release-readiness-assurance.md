---
adr: "0044"
title: "Release readiness: out-of-process cluster harness and continuous assurance"
adr_status: Proposed
tasks:
  - id: 0044-P1
    title: Out-of-process harness skeleton — spawn real mqttd binaries (Cargo test-binary paths) with real data dirs, listeners, and gossip sockets; per-node unprivileged TCP relays on the peer links; port the schedule vocabulary and acked-facts oracle; first schedules run kill (SIGKILL) / restart / publish / retained / churn against a 3-node cluster
    status: planned
  - id: 0044-P2
    title: OS-real fault vocabulary — SIGKILL at any instant including mid-write (0018-T7 lands here), disk-full against a real filesystem bound, restart from surviving dirs, membership flap at SWIM-confusing rates (0007-T8 lands here), partitions/brownouts/half-open links via the relays
    status: planned
  - id: 0044-P3
    title: Two-binary rolling upgrade — build HEAD + a pinned baseline ref, roll a live cluster one node at a time in both directions under the oracle, reopen data dirs across versions (ADR 0038 gates fire for real); closes the ADR 0043 recorded gap and builds the machinery 0039-T3 rides at 1.0
    status: planned
  - id: 0044-P4
    title: Nightly tier + soak — scheduled CI workflow running the out-of-process schedules over a wide seed sweep, the upgrade paths, fuzz time, and an hours-long soak under sustained mixed load watching RSS / FDs / tail latency against declared drift watermarks (ADR 0041 caps, ADR 0020 gauges)
    status: planned
  - id: 0044-P5
    title: Continuous security program — fuzz targets for every attacker-reachable parser (MQTT packets exist; add peer frames, gossip datagram verify, bridge frames, WS/QUIC framing, auth/config parsers) with in-repo corpora, wired into the nightly tier; every find becomes a darksky regression; SECURITY.md response process (private reporting, triage bounds, advisory path)
    status: planned
  - id: 0044-P6
    title: Performance baselines + regression gates — criterion micro-benches (codec, hub fan-out, replica apply/group-commit) and a harness macro-bench (connection ramp, sustained msgs/sec, p99 durable QoS 1) with recorded baselines; nightly comparison flags regressions beyond stated tolerance
    status: planned
  - id: 0044-P7
    title: Conformance breadth + operator-experience smoke + closure — Paho as the second interop oracle (0034-T7 lands here) with richer assertions; a quickstart smoke test standing up the documented 3-node cluster from the README's own commands; the release-readiness checklist assembled and the ADR closed
    status: planned
---

# Delivery: ADR 0044 — Release readiness: out-of-process cluster harness and continuous assurance

[ADR 0044](../adr/0044-release-readiness-assurance.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0044 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0044-P1 | ⬜ planned | — |  |
| 0044-P2 | ⬜ planned | — |  |
| 0044-P3 | ⬜ planned | — |  |
| 0044-P4 | ⬜ planned | — |  |
| 0044-P5 | ⬜ planned | — |  |
| 0044-P6 | ⬜ planned | — |  |
| 0044-P7 | ⬜ planned | — |  |
<!-- /status-table:0044 -->

## Plan

| Task | Done means |
|---|---|
| **0044-P1** Harness skeleton | A seeded out-of-process schedule runs a real 3-node cluster (spawned binaries, real dirs/sockets, per-link relays), applies kill/restart/publish/retained/churn steps, and holds the ported acked-facts oracle; runs green on a stock CI runner with no privileges. |
| **0044-P2** OS-real faults | SIGKILL-mid-write, disk-full, restart-from-dirs, flap, and relay-injected partition/brownout/half-open steps compose into the seeded schedules under the unchanged oracle; 0018-T7 and 0007-T8 are un-deferred into dedicated tests here. |
| **0044-P3** Two-binary upgrade | A cluster of baseline-version nodes upgrades to HEAD one node at a time (and rolls back) under live acked load with zero oracle violations, data dirs reopened across versions; the ADR 0043 recorded gap closes. |
| **0044-P4** Nightly tier + soak | A scheduled workflow runs the wide seed sweep, upgrade paths, fuzz time, and an hours-long soak; drift watermarks (RSS, FDs, p99) are declared and enforced; a nightly failure produces an exhibit-ledger entry. |
| **0044-P5** Security program | Every attacker-reachable parser has a fuzz target with a persisted corpus running nightly; at least one full-corpus pass is clean; SECURITY.md ships the response process; any find lands as a darksky regression test. |
| **0044-P6** Perf gates | Baselines recorded in-repo for micro + macro benches; the nightly comparison demonstrably catches a seeded regression (validated non-vacuous); the README states the measured numbers honestly. |
| **0044-P7** Breadth + closure | Paho joins mosquitto behind the interop harness; the README quickstart executes verbatim as a smoke test; the release-readiness checklist holds end to end; ADR flips to Accepted. |

Order: P1 → P2 → P3 (each stands on the previous), P4 once P1–P3 give it content,
P5/P6 parallel after P1, P7 last.

## Exhibits / findings ledger

| # | Finding | Where | Status |
|---|---|---|---|
| — | 2026-07-15 inventory: assurance ceiling is structural — all multi-node testing shares one process and one binary; fuzzing exists as one target CI never runs; zero benchmarks; no soak; one interop oracle; quickstart untested prose | code/CI survey (see ADR context) | open — this ADR is the plan |

## Changelog

- **2026-07-15** — ADR 0044 drafted with delivery plan P1–P7, from the assurance
  inventory (in-process harness strong but single-process/single-binary; fuzzing
  dormant; no benchmarks, soak, or second interop oracle). Motivated by the release
  commitment: enterprise-grade support with "most secure, continuously" and "simplest
  to use" as standing, testable claims. Un-defers 0018-T7, 0007-T8, 0034-T7, and the
  ADR 0043 rolling-upgrade gap into P2/P3/P7.
