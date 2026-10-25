---
adr: "0090"
title: "The benchmark rig measures msg/s and ack latency on one clock, at a fraction of the cost"
adr_status: Accepted
tasks:
  - id: 0090-T1
    title: "The load generator (D2): a Rust tool on the workspace codec; paced open-loop publishers with several messages in flight; a group's publishers and subscribers in one process; HDR histograms for ack and end-to-end latency; the identity ledger; generator health; a Prometheus endpoint"
    status: in-progress
    issue: 1109
    notes: "Size L. crates/mqtt-load (binary mqtt-load; README with the CLI and the JSON schema mqtt-load.summary.v1). MQTT 5 on mqtt-codec, QoS 0/1. The schedule is a pure function of time (phases at a total msg/s, one phase or --steps; publisher i of n at R msg/s sends at i/R + m*n/R). Each publisher has a writer, which sends what is due up to the free credit (min of --inflight and Receive Maximum), and a reader for the PUBACKs. Credit stall, socket backpressure, the backlog (offered - sent) and send delay are reported; the offer never drops. The payload carries publisher, sequence and the send time on the one process clock. HDR histograms for ack, end-to-end, scheduling lag and send delay. The identity ledger is a bitset per publisher (sent, acked) and per delivery stream (received): missing, unexpected, unacked, unexpected acks, duplicates. Generator health per window: process CPU, the busiest worker thread (Linux), scheduling lag p99. Thresholds 0.85 / 0.95 / 10 ms mark a window saturated; a saturated measured window makes the run void (exit 3). Prometheus /metrics during the run; the JSON summary has per-window, per-phase and total figures. Tests: unit tests (pacing, histograms, ledger, payload codec, CPU parsing) and crates/mqttd/tests/loadgen.rs against the production binary with a data dir: durable QoS 1 $share closes its ledger, clean QoS 0 fan-out closes its ledger, a cut consumer opens the ledger, and a 25 ms-each-way proxy adds one round trip to ack and end-to-end p50 (about 60 ms against a sub-ms or about 15 ms direct baseline). Measured on an M1 Pro against a local single-node mqttd: about 40k msg/s per generator core at QoS 1 (durable and clean), about 92k at QoS 0, never saturated; the broker on the same machine was the limit. Open: no reconnect, no QoS 2, no TLS; the emqtt-bench comparison is T2's."
  - id: 0090-T2
    title: "Calibration against emqtt-bench (D2): both tools against the same broker at a few rates, published beside every competitor comparison"
    status: planned
    issue: 1109
    notes: "Size S. Runs locally and on the 3-node tier."
  - id: 0090-T3
    title: "The msg/s ramp and the verdict (D1, D3, D4): carried msg/s from the brokers' counters, loss from the ledger, capacity at ack p99 <= 1 s, <= 250 ms and <= 100 ms; a saturated generator voids a step"
    status: planned
    issue: 1109
    notes: "Size M. Replaces the lane E ladder and its harness gates for runs on the new rig."
  - id: 0090-T4
    title: "Same-hosts A/B with repeats (D5): baseline and candidate interleaved on one provisioning, three repeats, the delta and its spread"
    status: planned
    issue: 1109
    notes: "Size M."
  - id: 0090-T5
    title: "The pre-built server image and the 3- and 10-node tiers (D6, D7)"
    status: planned
    issue: 1109
    notes: "Size M. Hetzner snapshot built by a script in the tree; the broker binary is still fetched per run and checked by sha256."
  - id: 0090-T6
    title: "The local tier (D7): A/B against main on one machine, extending durable_bench; relative figures only"
    status: planned
    issue: 1109
    notes: "Size S-M."
  - id: 0090-T7
    title: "Measure everything again on this rig (3 and 10 nodes, durable and clean QoS 1, QoS 0) and replace the published figures in the README, EVIDENCE and the release notes"
    status: planned
    issue: 1109
    notes: "Size S plus rig time (about EUR 3 a release confirmation). Only this rig's figures are reported; no comparison with earlier measurements."
---

# Delivery 0090 — the benchmark rig redesign

**ADR:** [docs/adr/0090-benchmark-rig-redesign.md](../adr/0090-benchmark-rig-redesign.md)

The plan, progress, and changelog for ADR 0090. Task status lives in the frontmatter
above; the table below is generated from it.

## Status

<!-- status-table:0090 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0090-T1 | 🚧 in-progress | [#1109](https://github.com/mbilling/fss-mqtt-broker/issues/1109) | — | "Size L. crates/mqtt-load (binary mqtt-load; README with the CLI and the JSON schema mqtt-load.summary.v1). MQTT 5 on mqtt-codec, QoS 0/1. The schedule is a pure function of time (phases at a total msg/s, one phase or --steps; publisher i of n at R msg/s sends at i/R + m*n/R). Each publisher has a writer, which sends what is due up to the free credit (min of --inflight and Receive Maximum), and a reader for the PUBACKs. Credit stall, socket backpressure, the backlog (offered - sent) and send delay are reported; the offer never drops. The payload carries publisher, sequence and the send time on the one process clock. HDR histograms for ack, end-to-end, scheduling lag and send delay. The identity ledger is a bitset per publisher (sent, acked) and per delivery stream (received): missing, unexpected, unacked, unexpected acks, duplicates. Generator health per window: process CPU, the busiest worker thread (Linux), scheduling lag p99. Thresholds 0.85 / 0.95 / 10 ms mark a window saturated; a saturated measured window makes the run void (exit 3). Prometheus /metrics during the run; the JSON summary has per-window, per-phase and total figures. Tests: unit tests (pacing, histograms, ledger, payload codec, CPU parsing) and crates/mqttd/tests/loadgen.rs against the production binary with a data dir: durable QoS 1 $share closes its ledger, clean QoS 0 fan-out closes its ledger, a cut consumer opens the ledger, and a 25 ms-each-way proxy adds one round trip to ack and end-to-end p50 (about 60 ms against a sub-ms or about 15 ms direct baseline). Measured on an M1 Pro against a local single-node mqttd: about 40k msg/s per generator core at QoS 1 (durable and clean), about 92k at QoS 0, never saturated; the broker on the same machine was the limit. Open: no reconnect, no QoS 2, no TLS; the emqtt-bench comparison is T2's." |
| 0090-T2 | ⬜ planned | [#1109](https://github.com/mbilling/fss-mqtt-broker/issues/1109) | — | "Size S. Runs locally and on the 3-node tier." |
| 0090-T3 | ⬜ planned | [#1109](https://github.com/mbilling/fss-mqtt-broker/issues/1109) | — | "Size M. Replaces the lane E ladder and its harness gates for runs on the new rig." |
| 0090-T4 | ⬜ planned | [#1109](https://github.com/mbilling/fss-mqtt-broker/issues/1109) | — | "Size M." |
| 0090-T5 | ⬜ planned | [#1109](https://github.com/mbilling/fss-mqtt-broker/issues/1109) | — | "Size M. Hetzner snapshot built by a script in the tree; the broker binary is still fetched per run and checked by sha256." |
| 0090-T6 | ⬜ planned | [#1109](https://github.com/mbilling/fss-mqtt-broker/issues/1109) | — | "Size S-M." |
| 0090-T7 | ⬜ planned | [#1109](https://github.com/mbilling/fss-mqtt-broker/issues/1109) | — | "Size S plus rig time (about EUR 3 a release confirmation). Only this rig's figures are reported; no comparison with earlier measurements." |
<!-- /status-table:0090 -->

## Changelog

- 2026-10-26: T1 in progress: `crates/mqtt-load`, the load generator (D2), with its
  tests against the production binary.
- 2026-10-25: ADR accepted; plan written (T1–T7). 1.3.0 ships on the existing rig,
  re-graded from the broker's side (ADR 0048 amendment of 2026-10-25).
