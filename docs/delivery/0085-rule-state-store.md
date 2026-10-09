---
adr: "0085"
title: "State for rules: a keyed, replicated store read and written from SQL"
adr_status: Proposed
tasks:
  - id: 0085-T1
    title: "The store and its SQL functions on local namespaces: the StateStore trait, kv_* functions incl. kv_held and kv_changed, namespaces in the rules file, bounds and memory accounting"
    status: planned
    issue: 880
  - id: 0085-T2
    title: "Typed merges as CRDTs (lww, first, max, min, sum, union, hll, tombstones) on a per-node hybrid logical clock"
    status: planned
    issue: 880
  - id: 0085-T3
    title: "eventual mode: CRDT deltas to the key's replica set over the peer bus, anti-entropy after partitions and restarts"
    status: planned
    issue: 880
  - id: 0085-T4
    title: "Expiry and $events/state/expired, fired once by the key's owner, at least once around membership changes"
    status: planned
    issue: 880
  - id: 0085-T5
    title: "kv_incr idempotency keys and the namespace dedupe window"
    status: planned
    issue: 880
  - id: 0085-T6
    title: "Observability and admin: state metrics, per-namespace $SYS statistics, mqttd --admin state get|list|del"
    status: planned
    issue: 880
  - id: 0085-T7
    title: "mqttd --rule-test --sequence: timed sequences replayed through a rule against an in-memory store"
    status: planned
    issue: 880
  - id: 0085-T8
    title: "Documentation: RULES.md (functions, merges, modes, the evaluation-order rule, the billing worked example), the departures from EMQX, threat model and hardening rows"
    status: planned
    issue: 880
  - id: 0085-T9
    title: "strong mode on the durable plane (phase 2), with partition and failover tests"
    status: planned
    issue: 880
  - id: 0085-T10
    title: "An optional snapshot to disk for local and eventual namespaces (phase 3)"
    status: planned
    issue: 880
---

# Delivery 0085 — state for rules

**ADR:** [docs/adr/0085-rule-state-store.md](../adr/0085-rule-state-store.md)

The plan, progress, and changelog for ADR 0085. Task status lives in the frontmatter
above; the table below is generated from it.

## Plan

| Task | Acceptance |
|---|---|
| **0085-T1** Store and functions (D1–D3, D9) | Every function in D3 works on a `local` namespace, from `mqttd --rule-test` and in the broker; `mqtt-rules` still holds no broker state; a full namespace refuses new keys (or evicts with `eviction = "lru"`), counted; the store's bytes reach the memory watermark. |
| **0085-T2** Merges (D4, D6) | Each merge type converges to one value from any delivery order of concurrent updates (property tests over random interleavings); a causally later `lww` write is never lost. |
| **0085-T3** `eventual` (D5) | Two nodes updating the same keys across a partition converge after it heals; a restarted node rebuilds its share from replicas; replication lag is a metric. |
| **0085-T4** Expiry events (D7) | A key's expiry fires `$events/state/expired` on one node in a stable cluster; "silent for X" works end to end in the live demo. |
| **0085-T5** Idempotency (D8) | A redelivered message with the same idempotency key does not count twice within the window. |
| **0085-T6** Observability and admin (D10) | Every metric in D10 is exported; `$SYS` never carries a key or value; the admin verbs are operator-only and audited. |
| **0085-T7** Sequence testing (D11) | A sequence file replays with timestamps and prints each step's outputs and state; the rule-authoring skill uses it. |
| **0085-T8** Documentation (D12) | RULES.md documents every function, merge and mode, the `SELECT`-before-`WHERE` rule and the billing example; the departures table lists the extension; the threat model has the state-exhaustion and payload-derived-value rows. |
| **0085-T9** `strong` (phase 2) | A quorum-acknowledged write survives the leader's loss; a minority partition's writes fail and are counted; a Will's evaluation never waits. |
| **0085-T10** Snapshot (phase 3) | A restarted node restores its `local` and `eventual` namespaces from the last snapshot. |

## Progress

<!-- status-table:0085 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0085-T1 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
| 0085-T2 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
| 0085-T3 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
| 0085-T4 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
| 0085-T5 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
| 0085-T6 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
| 0085-T7 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
| 0085-T8 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
| 0085-T9 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
| 0085-T10 | ⬜ planned | [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880) | — |  |
<!-- /status-table:0085 -->
