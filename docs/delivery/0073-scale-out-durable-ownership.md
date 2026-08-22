---
adr: "0073"
title: "Scale-out durable ownership: the voter set is a control plane, not the data path"
adr_status: Proposed
tasks:
  - id: 0073-T1
    title: Leader-forwarded lease operations — a learner claims/renews/bumps through the lease leader over the mesh, reads committed state locally; serving gate = the committed lease map; lease_forward_failures_total; capability-gated OFF
    status: planned
    notes: "The 0049 incident's actual defect, repaired: the learner write path. No hot-path change — appends/acks run under committed leases against the group replica set as today. Falsifier: a learner-owner in a test cluster serves attaches end to end with voters partitioned away from it only at renewal boundaries."
  - id: 0073-T2
    title: The ownership-domain flip is a committed lease-log record gated on a cluster-wide peer capability — mixed-version clusters keep voter-bounded ownership; the two-binary upgrade oracle proves no dual-ownership window
    status: planned
    notes: "One truth per lease epoch: an old+new mixed roll must never compute two HRW domains. cluster_upgrade.rs gains the roll with the capability half-present."
  - id: 0073-T3
    title: Placement over all admitted members (pre-0049 domain restored, settle discipline kept) + migration soak — grow 5→10, ownership spreads ~1/N, eager migration moves data, zero acked loss
    status: planned
    notes: "ADR 0043 P2 machinery unchanged; the soak is the falsifier."
  - id: 0073-T4
    title: "The measured slope: 7- and 10-node durable curve, capped vs uncapped ownership A/B on identical hardware; SCALE-CURVE.md + COMPARISON.md publication of the scale-out claim with the up-vs-out economics case"
    status: planned
    notes: "Needs the Hetzner quota raise (~100 dedicated vCPU). The PR #375 lease-voters rig variant becomes the A/B control arm."
---

# Delivery: ADR 0073 — Scale-out durable ownership

[ADR 0073](../adr/0073-scale-out-durable-ownership.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0073 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0073-T1 | ⬜ planned | — | "The 0049 incident's actual defect, repaired: the learner write path. No hot-path change — appends/acks run under committed leases against the group replica set as today. Falsifier: a learner-owner in a test cluster serves attaches end to end with voters partitioned away from it only at renewal boundaries." |
| 0073-T2 | ⬜ planned | — | "One truth per lease epoch: an old+new mixed roll must never compute two HRW domains. cluster_upgrade.rs gains the roll with the capability half-present." |
| 0073-T3 | ⬜ planned | — | "ADR 0043 P2 machinery unchanged; the soak is the falsifier." |
| 0073-T4 | ⬜ planned | — | "Needs the Hetzner quota raise (~100 dedicated vCPU). The PR #375 lease-voters rig variant becomes the A/B control arm." |
<!-- /status-table:0073 -->

## Changelog

- 2026-08-22 — ADR proposed. Grounded in the v1.0.2 curve (durable flat past
  the voter cap by design), the #368 diagnosis (multiplying membership planes
  argued against), and the #376 fix (per-owner throughput worth multiplying).
  No code in this change; T1 starts only if the ADR is accepted.
