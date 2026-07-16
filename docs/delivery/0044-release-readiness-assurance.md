---
adr: "0044"
title: "Release readiness: out-of-process cluster harness and continuous assurance"
adr_status: Proposed
tasks:
  - id: 0044-P1
    title: Out-of-process harness skeleton — spawn real mqttd binaries (Cargo test-binary paths) with real data dirs, listeners, and gossip sockets; per-node unprivileged TCP relays on the peer links; port the schedule vocabulary and acked-facts oracle; first schedules run kill (SIGKILL) / restart / publish / retained / churn against a 3-node cluster
    status: done
    date: 2026-07-16
    evidence: "The out-of-process tier exists: crates/mqttd/tests/cluster_proc.rs spawns the COMPILED PRODUCTION BINARY (CARGO_BIN_EXE_mqttd) per node — real processes, real data dirs, real TCP/MQTT listeners, real UDP gossip sockets — configured purely through the documented MQTTD_* environment exactly as an operator would (node assembly is main.rs itself, not a test-side mirror of it). New env: MQTTD_PEER_ADVERTISE (default: the bind) lets gossip advertise a dialable peer address that differs from the bound one — NAT, container port mapping, or a fronting relay; the harness fronts every node's peer listener with an unprivileged in-test TCP relay and advertises the relay's address (the severable per-link seam the P2 fault vocabulary grows on — the relays carry ALL peer traffic here, proving the seam under no privileges). Bring-up, restart admission, and quiesce all read /readyz (members + lease_group_ready) — the operator's own convergence signal, never internal state; placement is deliberately invisible, so clients attach through ANY node and the ADR 0005 owner-relay routes them: the production client path, black-box. Kill is kernel SIGKILL (no in-process stand-in deciding what crash means); restart reopens the surviving redb dirs COLD over the same fixed ports (the relay keeps the advertised address valid across cycles), with re-seeding so a restarted founder REJOINS instead of re-bootstrapping — the tier's first live find: an all-seeded topology has no founder, never bootstraps the lease group, and never becomes ready (main.rs's no-seeds-is-the-founder rule, invisible to the in-process tier which bootstraps directly). Schedule vocabulary + acked-facts oracle ported from ADR 0042: publish (owed only from its PUBACK), retained (expected value from the last acked set onward), churn, a SIGKILL at a seeded position and a restart 2-3 steps later — EVERY seed exercises the full crash/recover cycle, under production SWIM timings (no test-tuned knobs; windows sized accordingly). Oracle judged entirely through MQTT + /readyz: every resume present=true (check_recovery_honesty), every acked payload replayed, retained converged across all nodes at-or-beyond the last acked set. Failures are self-diagnosing: log tails of every spawned node + the full schedule trace + REPRO_SEED printed on panic. CI profile runs one seed (~20-35s in the ordinary workspace test run); MQTTD_PROC_SEEDS widens for the nightly tier (3-seed sweep green, ~100s, every seed 1 sigkill + 1 restart). Workspace green, clippy zero warnings."
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
| 0044-P1 | ✅ done | 2026-07-16 | "The out-of-process tier exists: crates/mqttd/tests/cluster_proc.rs spawns the COMPILED PRODUCTION BINARY (CARGO_BIN_EXE_mqttd) per node — real processes, real data dirs, real TCP/MQTT listeners, real UDP gossip sockets — configured purely through the documented MQTTD_* environment exactly as an operator would (node assembly is main.rs itself, not a test-side mirror of it). New env: MQTTD_PEER_ADVERTISE (default: the bind) lets gossip advertise a dialable peer address that differs from the bound one — NAT, container port mapping, or a fronting relay; the harness fronts every node's peer listener with an unprivileged in-test TCP relay and advertises the relay's address (the severable per-link seam the P2 fault vocabulary grows on — the relays carry ALL peer traffic here, proving the seam under no privileges). Bring-up, restart admission, and quiesce all read /readyz (members + lease_group_ready) — the operator's own convergence signal, never internal state; placement is deliberately invisible, so clients attach through ANY node and the ADR 0005 owner-relay routes them: the production client path, black-box. Kill is kernel SIGKILL (no in-process stand-in deciding what crash means); restart reopens the surviving redb dirs COLD over the same fixed ports (the relay keeps the advertised address valid across cycles), with re-seeding so a restarted founder REJOINS instead of re-bootstrapping — the tier's first live find: an all-seeded topology has no founder, never bootstraps the lease group, and never becomes ready (main.rs's no-seeds-is-the-founder rule, invisible to the in-process tier which bootstraps directly). Schedule vocabulary + acked-facts oracle ported from ADR 0042: publish (owed only from its PUBACK), retained (expected value from the last acked set onward), churn, a SIGKILL at a seeded position and a restart 2-3 steps later — EVERY seed exercises the full crash/recover cycle, under production SWIM timings (no test-tuned knobs; windows sized accordingly). Oracle judged entirely through MQTT + /readyz: every resume present=true (check_recovery_honesty), every acked payload replayed, retained converged across all nodes at-or-beyond the last acked set. Failures are self-diagnosing: log tails of every spawned node + the full schedule trace + REPRO_SEED printed on panic. CI profile runs one seed (~20-35s in the ordinary workspace test run); MQTTD_PROC_SEEDS widens for the nightly tier (3-seed sweep green, ~100s, every seed 1 sigkill + 1 restart). Workspace green, clippy zero warnings." |
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

- **2026-07-16** — **0044-P1 done: the out-of-process harness skeleton.** A real
  3-node cluster of spawned production binaries (`CARGO_BIN_EXE_mqttd`), configured
  purely through the documented `MQTTD_*` environment, observed purely through
  `/readyz` and MQTT — kill is kernel `SIGKILL`, restart reopens the surviving data
  dirs cold, clients attach through any node via the ADR 0005 owner-relay, and the
  ADR 0042 acked-facts oracle holds black-box. Every node's peer listener sits
  behind an unprivileged in-test TCP relay advertised via the new
  `MQTTD_PEER_ADVERTISE` (also a real operator knob: NAT/container/fronting-proxy
  deployments), giving P2's fault vocabulary its severable per-link seam. First
  live find, before the first schedule even ran: an all-seeded topology has no
  founder and never becomes ready — the no-seeds-is-the-founder bootstrap rule is
  invisible to the in-process tier, exactly the class of gap this tier exists to
  surface. One seed in per-PR CI; `MQTTD_PROC_SEEDS` widens for the nightly tier.
- **2026-07-15** — ADR 0044 drafted with delivery plan P1–P7, from the assurance
  inventory (in-process harness strong but single-process/single-binary; fuzzing
  dormant; no benchmarks, soak, or second interop oracle). Motivated by the release
  commitment: enterprise-grade support with "most secure, continuously" and "simplest
  to use" as standing, testable claims. Un-defers 0018-T7, 0007-T8, 0034-T7, and the
  ADR 0043 rolling-upgrade gap into P2/P3/P7.
