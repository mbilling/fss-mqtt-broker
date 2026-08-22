---
adr: "0073"
title: "Scale-out durable ownership: the voter set is a control plane, not the data path"
adr_status: Accepted
tasks:
  - id: 0073-T1
    title: The capability-gated ownership domain — under cluster-wide peer proto >= 8 the durable driver pushes no voter restriction (HRW spreads ownership over every admitted member, the leader assigns leases to learners, learners read epochs locally) and lease_group_ready accepts a learner-owner; /statusz reports the in-force domain
    status: done
    date: 2026-08-22
    evidence: "Implementation found the proposal's leader-forwarded lease writes UNNECESSARY: assignment was already leader-push (LeaseAssigner::reconcile is the only production lease write) and owners read epochs from their own applied lease store — the ADR's Decision §1 records the refinement. What shipped: run_driver consults the shared ownership_domain_all flag and pushes an EMPTY voter restriction under it; DurablePlane::lease_group_ready waives the voter requirement under the flag (leader still required); statusz lease block gains \"ownership_domain\":\"members\"|\"voters\" (the in-force domain). Config: durable.ownership_domain = members (default) | voters (the loud escape hatch), MQTTD_OWNERSHIP_DOMAIN, ENV_VARS 86→87. FALSIFIER (durable_sessions): the_scale_out_domain_owns_sessions_on_learners_that_serve — 3 nodes at voter cap 1, ownership spreads to both learners, a learner reports lease_group_ready, and a LEARNER-OWNED session's durable enqueue COMMITS end to end and reads back — the exact serving path the 2026-07-14 post-mortem proved missing under ADR 0049. Plus a_learner_is_ready_under_the_scale_out_capability (plane gate edges) and the ADR 0049 control test (a_bounded_voter_cluster_owns_every_session_on_a_voter...) still green with the flag off."
  - id: 0073-T2
    title: Mixed versions cannot split ownership — peer proto 8 as the capability marker (degenerate additive bump, no new frames); each hub recomputes the verdict per sweep from every member's last-NEGOTIATED proto (link-flap-immune, forgotten only on confirmed death); unknown or old ⇒ the whole cluster holds the voter domain
    status: done
    date: 2026-08-22
    evidence: "PROTO_MAX 7→8 + PROTO_OWNERSHIP_DOMAIN with the ADR 0038 additive-bump rule documented at the constants (proto-7 peers negotiate 7 and keep every existing frame — asserted in the negotiation test alongside the new proto-8 ceiling). Hub: known_peer_protos records the negotiated proto at link attach, survives link flaps (peer_disconnected keeps it, mirroring the interest-kept design), is dropped only on confirmed death; refresh_ownership_domain runs each sweep, self trivially capable, and edge-logs expansion (info) / restriction (warn). The committed-log-record flip from the proposal was DROPPED as rollback-unsafe (an unknown record type in the previous release's state machine) and unnecessary — divergent group_owner computations are routing hints and assignment drift only; the lease leader is the single assigner and the committed lease map (plain node-id holders, readable by every version) is the only serving truth, so the conservative window costs at most one reassignment churn, never dual service. TESTS: the_ownership_domain_flag_needs_every_member_capable_and_the_operator_choice (escape hatch pins voters; unknown proto conservative; old peer holds; all-capable expands; rollback restores), ownership_domain_parses_both_values_and_refuses_others (config), proto negotiation edges. The nightly two-binary oracle (BASELINE_REF v1.0.2, proto max 7) rolls exactly the mixed window this design holds conservative."
  - id: 0073-T3
    title: "Grow/shrink migration soak at scale: 5→10 nodes, ownership spreads ~1/N via eager migration (ADR 0043 P2 machinery), zero acked loss under load"
    status: planned
    notes: "The migration machinery is ADR 0043's and unchanged; the soak proves it under the widened domain at fleet size. Natural to run beside T4 on the same paid hardware."
  - id: 0073-T4
    title: "The measured slope: 7- and 10-node durable curve, capped vs uncapped ownership A/B on identical hardware; SCALE-CURVE.md + COMPARISON.md publication of the scale-out claim with the up-vs-out economics case"
    status: planned
    notes: "Needs the Hetzner quota raise (~100 dedicated vCPU). The PR #375 lease-voters rig variant becomes the A/B control arm; the default arm now measures the ADR 0073 domain."
---

# Delivery: ADR 0073 — Scale-out durable ownership

[ADR 0073](../adr/0073-scale-out-durable-ownership.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0073 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0073-T1 | ✅ done | 2026-08-22 | "Implementation found the proposal's leader-forwarded lease writes UNNECESSARY: assignment was already leader-push (LeaseAssigner::reconcile is the only production lease write) and owners read epochs from their own applied lease store — the ADR's Decision §1 records the refinement. What shipped: run_driver consults the shared ownership_domain_all flag and pushes an EMPTY voter restriction under it; DurablePlane::lease_group_ready waives the voter requirement under the flag (leader still required); statusz lease block gains \"ownership_domain\":\"members\"|\"voters\" (the in-force domain). Config: durable.ownership_domain = members (default) | voters (the loud escape hatch), MQTTD_OWNERSHIP_DOMAIN, ENV_VARS 86→87. FALSIFIER (durable_sessions): the_scale_out_domain_owns_sessions_on_learners_that_serve — 3 nodes at voter cap 1, ownership spreads to both learners, a learner reports lease_group_ready, and a LEARNER-OWNED session's durable enqueue COMMITS end to end and reads back — the exact serving path the 2026-07-14 post-mortem proved missing under ADR 0049. Plus a_learner_is_ready_under_the_scale_out_capability (plane gate edges) and the ADR 0049 control test (a_bounded_voter_cluster_owns_every_session_on_a_voter...) still green with the flag off." |
| 0073-T2 | ✅ done | 2026-08-22 | "PROTO_MAX 7→8 + PROTO_OWNERSHIP_DOMAIN with the ADR 0038 additive-bump rule documented at the constants (proto-7 peers negotiate 7 and keep every existing frame — asserted in the negotiation test alongside the new proto-8 ceiling). Hub: known_peer_protos records the negotiated proto at link attach, survives link flaps (peer_disconnected keeps it, mirroring the interest-kept design), is dropped only on confirmed death; refresh_ownership_domain runs each sweep, self trivially capable, and edge-logs expansion (info) / restriction (warn). The committed-log-record flip from the proposal was DROPPED as rollback-unsafe (an unknown record type in the previous release's state machine) and unnecessary — divergent group_owner computations are routing hints and assignment drift only; the lease leader is the single assigner and the committed lease map (plain node-id holders, readable by every version) is the only serving truth, so the conservative window costs at most one reassignment churn, never dual service. TESTS: the_ownership_domain_flag_needs_every_member_capable_and_the_operator_choice (escape hatch pins voters; unknown proto conservative; old peer holds; all-capable expands; rollback restores), ownership_domain_parses_both_values_and_refuses_others (config), proto negotiation edges. The nightly two-binary oracle (BASELINE_REF v1.0.2, proto max 7) rolls exactly the mixed window this design holds conservative." |
| 0073-T3 | ⬜ planned | — | "The migration machinery is ADR 0043's and unchanged; the soak proves it under the widened domain at fleet size. Natural to run beside T4 on the same paid hardware." |
| 0073-T4 | ⬜ planned | — | "Needs the Hetzner quota raise (~100 dedicated vCPU). The PR #375 lease-voters rig variant becomes the A/B control arm; the default arm now measures the ADR 0073 domain." |
<!-- /status-table:0073 -->

## Changelog

- 2026-08-22 — ADR proposed. Grounded in the v1.0.2 curve (durable flat past
  the voter cap by design), the #368 diagnosis (multiplying membership planes
  argued against), and the #376 fix (per-owner throughput worth multiplying).
- 2026-08-22 — Accepted and T1+T2 shipped, with the mechanism refined during
  implementation: no lease-write forwarding exists because none was ever
  needed (leader-push assignment + local epoch reads were already the
  architecture); the capability flip is per-node conservative recomputation,
  not a committed log record (which would have been rollback-unsafe). The
  learner-serving falsifier passes end to end.
