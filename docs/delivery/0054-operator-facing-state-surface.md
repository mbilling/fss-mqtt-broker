---
adr: "0054"
title: "Operator-facing state surface: /statusz + state gauges"
adr_status: Accepted
tasks:
  - id: 0054-T1
    title: /statusz route + state gauges — identity/members/lease/decommission/brownout/store/proto body on the health listener; brownout{axis}, store_max_bytes, decommission_state/pending, voters, replica_groups_current/tracked gauges wired on existing refresh paths
    status: in-progress
  - id: 0054-T2
    title: Cluster identity — minted at founding, persisted in the lease store, gossip-propagated, adopted by joiners; cluster-mismatch gossip guard (containment, not just detection); cluster_info/founder/foundings_total metrics; statusz cluster block; OPERATIONS.md split-brain detection rule
    status: planned
  - id: 0054-T3
    title: Rotation + convergence visibility — SWIM key count/fingerprints (never material), config checksum + reload generation, peer proto gauge; statusz keys/config blocks; OPERATIONS.md rotation verification
    status: planned
  - id: 0054-T4
    title: Monitoring docs + dashboard — Grafana rows for the new signals (brownout, store utilization, decommission, cluster identity, mismatch rate) and the OPERATIONS.md alert-rule catalogue the operator will encode
    status: planned
---

# 0054 — Operator-facing state surface: delivery

**Decision:** [ADR 0054](../adr/0054-operator-facing-state-surface.md). One-line
story: the operator program (ADR 0047 amendment, triggers engaged 2026-08-04) needs
state to act on; this lands the signals first — split-brain detectability, brownout
as a condition, drains visible to scrape — as one structured `/statusz` plus bounded
gauges, useful to humans and alert rules before any controller exists.

<!-- status-table:0054 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0054-T1 | 🚧 in-progress | — |  |
| 0054-T2 | ⬜ planned | — |  |
| 0054-T3 | ⬜ planned | — |  |
| 0054-T4 | ⬜ planned | — |  |
<!-- /status-table:0054 -->

## Notes

- 2026-08-05 — Inventory that motivated this (session record): no cluster identity
  anywhere (split-brain undetectable, `can_bootstrap` the sole guard); brownout state
  never exported (an idle browned-out broker is silent); decommission `active` flag
  computed but never surfaced; membership counts-only. The metrics/body split rule
  follows ADR 0020's cardinality discipline: node-naming detail is body-only.
