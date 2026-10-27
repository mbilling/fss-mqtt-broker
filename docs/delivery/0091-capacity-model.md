---
adr: "0091"
title: "Every bound is a function of a few measured numbers"
adr_status: Accepted
tasks:
  - id: 0091-T1
    title: "The harness never sets the knee (D3): mqtt-load deals every topic and group over every broker, refuses a skewed workload, reports the broker's measured spread; rig2 VOIDs a skewed step"
    status: in-progress
    issue: 1132
    notes: "Size S. PR #1133. Publisher i on broker (i % topics + i / topics) % brokers, member j of stream s on (s + j) % brokers; derived publishers round up to a multiple of topics x brokers; session_skew (expected busiest session / mean, under prefer-local) refused above --max-skew 1.25; spread (measured busiest consumer / mean) in the summary; verdict.py VOIDs a step with session_skew > 1.25 and notes a measured spread > 1.5. The old deal fails every_topic_has_publishers_on_every_broker with topic 0 spread [51, 0, ..., 0]."
  - id: 0091-T2
    title: "A hot-session workload (D3): one topic's whole rate into one durable $share member, to measure one session's ceiling before and after T4"
    status: planned
    issue: 1134
    notes: "Size S. mqtt-load --max-skew 0; a rig2 workload env; reported as msg/s per session, beside D2's D / L."
  - id: 0091-T3
    title: "$share selection skips a member past a quarter of its lane or outbound bound when another eligible member exists (D4)"
    status: planned
    issue: 1078
    notes: "Size M. Test first: a hub test that joins a cold member under load and a test with one member's lane held full; neither withholds acks while another member can take the publish. Locality still decides among members that keep up."
  - id: 0091-T4
    title: "One session's durable rate is the store's (D5): a lane submits its queued jobs as one batch with one durability wait"
    status: planned
    issue: 1134
    notes: "Size M-L. Order and per-message outcomes unchanged; measured with T2 before and after."
  - id: 0091-T5
    title: "Bounds stated as time with a byte ceiling (D6): the lane, outbound and backlog queues admit while their oldest entry is younger than the stall budget; today's counts stay as floors"
    status: planned
    issue: 1134
    notes: "Size M. Each bound documented with its formula beside its metric."
  - id: 0091-T6
    title: "The rig predicts before it measures (D7): per-session rate, disk and replication bytes per node and the expected capacity printed before provisioning; the report puts measured beside predicted and flags a gap over 25%"
    status: planned
    issue: 1134
    notes: "Size S-M."
  - id: 0091-T7
    title: "The formulas in the tuning docs, and one durable session's ceiling as a documented limit until T4 ships"
    status: planned
    issue: 1134
    notes: "Size S."
---

# Delivery 0091 — the capacity model

**ADR:** [docs/adr/0091-capacity-model.md](../adr/0091-capacity-model.md)

The plan, progress, and changelog for ADR 0091. Task status lives in the frontmatter
above; the table below is generated from it.

## Status

<!-- status-table:0091 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0091-T1 | 🚧 in-progress | [#1132](https://github.com/mbilling/fss-mqtt-broker/issues/1132) | — | "Size S. PR #1133. Publisher i on broker (i % topics + i / topics) % brokers, member j of stream s on (s + j) % brokers; derived publishers round up to a multiple of topics x brokers; session_skew (expected busiest session / mean, under prefer-local) refused above --max-skew 1.25; spread (measured busiest consumer / mean) in the summary; verdict.py VOIDs a step with session_skew > 1.25 and notes a measured spread > 1.5. The old deal fails every_topic_has_publishers_on_every_broker with topic 0 spread [51, 0, ..., 0]." |
| 0091-T2 | ⬜ planned | [#1134](https://github.com/mbilling/fss-mqtt-broker/issues/1134) | — | "Size S. mqtt-load --max-skew 0; a rig2 workload env; reported as msg/s per session, beside D2's D / L." |
| 0091-T3 | ⬜ planned | [#1078](https://github.com/mbilling/fss-mqtt-broker/issues/1078) | — | "Size M. Test first: a hub test that joins a cold member under load and a test with one member's lane held full; neither withholds acks while another member can take the publish. Locality still decides among members that keep up." |
| 0091-T4 | ⬜ planned | [#1134](https://github.com/mbilling/fss-mqtt-broker/issues/1134) | — | "Size M-L. Order and per-message outcomes unchanged; measured with T2 before and after." |
| 0091-T5 | ⬜ planned | [#1134](https://github.com/mbilling/fss-mqtt-broker/issues/1134) | — | "Size M. Each bound documented with its formula beside its metric." |
| 0091-T6 | ⬜ planned | [#1134](https://github.com/mbilling/fss-mqtt-broker/issues/1134) | — | "Size S-M." |
| 0091-T7 | ⬜ planned | [#1134](https://github.com/mbilling/fss-mqtt-broker/issues/1134) | — | "Size S." |
<!-- /status-table:0091 -->

## Changelog

- **2026-10-27:** ADR accepted after the 10-node rig2 run. T1 is in review (#1133).
