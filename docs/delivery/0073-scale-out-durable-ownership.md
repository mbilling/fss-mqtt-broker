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
    title: "Grow/shrink migration soak: ownership spreads onto new (learner) members via eager migration, zero acked loss"
    status: done
    date: 2026-08-23
    evidence: "COMPLETE with the #390 fix: recovery falls back to a FULL ROSTER SWEEP when a quorum of the current replica set is all-hollow (the grow hand-off race) — read-only, so epoch fencing holds; invariants: the durable roster fully known (unknown members refuse), EVERY out-of-set member must answer (one silent member could hold the only copy), and the merged UNION must be gap-free (a swallowed tail after a full sweep is genuine loss — refuse, never truncate). ACCEPTANCE (durable_sessions::pre_grow_history_survives_migration_onto_data_less_newcomers — deterministically RED before the fix under full-suite load): 3->6 grow, 120 pre-grow durable sessions, every message reads back intact from its post-grow owner, learner newcomers included. Spread half: growing_the_scaled_out_cluster_migrates_ownership_with_zero_acked_loss. Full durable_sessions (13), mqtt-cluster (267+), cluster/chaos/stress/inflight suites green. The fleet-size soak still rides T4's paid run."
  - id: 0073-T4.0
    title: "The A/B arm is ONE provisioning, not two clusters: run-curve sequences both ownership domains on a single cluster (clean broker restart between arms with MQTTD_OWNERSHIP_DOMAIN=voters + MQTTD_LEASE_VOTERS, the PR #375 rig variant), env dumps record the in-force domain per arm, the summarizer keys and renders the A/B; SHAPE_ONLY=1 validates offline for nothing"
    status: planned
    notes: "Motivation: ADR 0077-T4 measured ~40% variance between freshly provisioned clusters running the SAME binary — wider than the slope T4 measures. Two paid runs would be two cluster draws; the only honest A/B this rig can pay for is both arms on one provisioning, plus a control rung common to both arms and repeated rungs within a provisioning."
  - id: 0073-T4.1
    title: "Simulated fleet-size migration soak in-tree: the ADR 0042 stress-harness schedules grow 3->6->9 and shrink under load with the zero-acked-loss invariant (T3's falsifiers at fleet scale) — the cheap place to find #390-class grow/shrink defects before any paid run"
    status: planned
    notes: "cargo land, €0, CI hours. The #390 grow hand-off race was found exactly this way (deterministically red under full-suite load); this stage exists so the next one like it costs CI minutes, not cluster money."
  - id: 0073-T4.2
    title: "Smoke run proves the arm plumbing end to end: 1 broker + 1 driver, shared vCPUs, ~€0.50, ~30 min — arm flip mid-provisioning works, env dumps record the domain per arm, summarizer renders the A/B"
    status: planned
    notes: "A rig bug found here costs cents. Same pipeline as the real runs, every knob shrunk."
  - id: 0073-T4.3
    title: "Small paid runs: a 3-node durable run with the voters arm at MQTTD_LEASE_VOTERS=2 (below N, so the ownership-domain difference is VISIBLE at cheap size — at default voters=5 the arms are identical at N=3) and a 1->3->5 grow/shrink soak under lane A load on real NVMe and real networking"
    status: planned
    notes: "€2–4, ~30–60 min each. T3's in-tree soak re-run at cloud scale; the last stage where defects are cheap."
  - id: 0073-T4.4
    title: "The measured slope: 7- and 10-node durable curve (lane A sat+lat, ONE driver — durable_bench serves from driver-1, so 10+1 = 11 servers / 44 vCPU, inside the current 15-server / 100-vCPU project limits with NO console raise), both ownership arms per size on one provisioning, repeated rungs + common control rung; SCALE-CURVE.md + COMPARISON.md publication of the scale-out claim with the up-vs-out economics case"
    status: planned
    notes: "€5–10 in one session. Corrects the original T4 note: the 'needs ~100 dedicated vCPU quota raise' applied to full-ladder shapes; the durable lane's shape fits the existing quota. Issues found here cost an hour and a few euros — the point of the ordering."
  - id: 0073-T4.5
    title: "The expensive tail, run ONLY on green T4.0–T4.4: an hours-scale soak on 10x CCX23 with repeated grow/shrink cycles, node kills and a partition arm (ADR 0077-T8's fault lane), and the paid rolling upgrade through the proto 7<->8 capability window (the nightly two-binary oracle already covers the logic in-tree)"
    status: planned
    notes: "~€30–40. By the time these run, steady-state behaviour — not correctness — should be all they can measure. Publication never cites gate runs: only T4.4's full-profile numbers go in the doc."
---

# Delivery: ADR 0073 — Scale-out durable ownership

[ADR 0073](../adr/0073-scale-out-durable-ownership.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0073 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0073-T1 | ✅ done | 2026-08-22 | "Implementation found the proposal's leader-forwarded lease writes UNNECESSARY: assignment was already leader-push (LeaseAssigner::reconcile is the only production lease write) and owners read epochs from their own applied lease store — the ADR's Decision §1 records the refinement. What shipped: run_driver consults the shared ownership_domain_all flag and pushes an EMPTY voter restriction under it; DurablePlane::lease_group_ready waives the voter requirement under the flag (leader still required); statusz lease block gains \"ownership_domain\":\"members\"|\"voters\" (the in-force domain). Config: durable.ownership_domain = members (default) | voters (the loud escape hatch), MQTTD_OWNERSHIP_DOMAIN, ENV_VARS 86→87. FALSIFIER (durable_sessions): the_scale_out_domain_owns_sessions_on_learners_that_serve — 3 nodes at voter cap 1, ownership spreads to both learners, a learner reports lease_group_ready, and a LEARNER-OWNED session's durable enqueue COMMITS end to end and reads back — the exact serving path the 2026-07-14 post-mortem proved missing under ADR 0049. Plus a_learner_is_ready_under_the_scale_out_capability (plane gate edges) and the ADR 0049 control test (a_bounded_voter_cluster_owns_every_session_on_a_voter...) still green with the flag off." |
| 0073-T2 | ✅ done | 2026-08-22 | "PROTO_MAX 7→8 + PROTO_OWNERSHIP_DOMAIN with the ADR 0038 additive-bump rule documented at the constants (proto-7 peers negotiate 7 and keep every existing frame — asserted in the negotiation test alongside the new proto-8 ceiling). Hub: known_peer_protos records the negotiated proto at link attach, survives link flaps (peer_disconnected keeps it, mirroring the interest-kept design), is dropped only on confirmed death; refresh_ownership_domain runs each sweep, self trivially capable, and edge-logs expansion (info) / restriction (warn). The committed-log-record flip from the proposal was DROPPED as rollback-unsafe (an unknown record type in the previous release's state machine) and unnecessary — divergent group_owner computations are routing hints and assignment drift only; the lease leader is the single assigner and the committed lease map (plain node-id holders, readable by every version) is the only serving truth, so the conservative window costs at most one reassignment churn, never dual service. TESTS: the_ownership_domain_flag_needs_every_member_capable_and_the_operator_choice (escape hatch pins voters; unknown proto conservative; old peer holds; all-capable expands; rollback restores), ownership_domain_parses_both_values_and_refuses_others (config), proto negotiation edges. The nightly two-binary oracle (BASELINE_REF v1.0.2, proto max 7) rolls exactly the mixed window this design holds conservative." |
| 0073-T3 | ✅ done | 2026-08-23 | "COMPLETE with the #390 fix: recovery falls back to a FULL ROSTER SWEEP when a quorum of the current replica set is all-hollow (the grow hand-off race) — read-only, so epoch fencing holds; invariants: the durable roster fully known (unknown members refuse), EVERY out-of-set member must answer (one silent member could hold the only copy), and the merged UNION must be gap-free (a swallowed tail after a full sweep is genuine loss — refuse, never truncate). ACCEPTANCE (durable_sessions::pre_grow_history_survives_migration_onto_data_less_newcomers — deterministically RED before the fix under full-suite load): 3->6 grow, 120 pre-grow durable sessions, every message reads back intact from its post-grow owner, learner newcomers included. Spread half: growing_the_scaled_out_cluster_migrates_ownership_with_zero_acked_loss. Full durable_sessions (13), mqtt-cluster (267+), cluster/chaos/stress/inflight suites green. The fleet-size soak still rides T4's paid run." |
| 0073-T4.0 | ⬜ planned | — | "Motivation: ADR 0077-T4 measured ~40% variance between freshly provisioned clusters running the SAME binary — wider than the slope T4 measures. Two paid runs would be two cluster draws; the only honest A/B this rig can pay for is both arms on one provisioning, plus a control rung common to both arms and repeated rungs within a provisioning." |
| 0073-T4.1 | ⬜ planned | — | "cargo land, €0, CI hours. The #390 grow hand-off race was found exactly this way (deterministically red under full-suite load); this stage exists so the next one like it costs CI minutes, not cluster money." |
| 0073-T4.2 | ⬜ planned | — | "A rig bug found here costs cents. Same pipeline as the real runs, every knob shrunk." |
| 0073-T4.3 | ⬜ planned | — | "€2–4, ~30–60 min each. T3's in-tree soak re-run at cloud scale; the last stage where defects are cheap." |
| 0073-T4.4 | ⬜ planned | — | "€5–10 in one session. Corrects the original T4 note: the 'needs ~100 dedicated vCPU quota raise' applied to full-ladder shapes; the durable lane's shape fits the existing quota. Issues found here cost an hour and a few euros — the point of the ordering." |
| 0073-T4.5 | ⬜ planned | — | "~€30–40. By the time these run, steady-state behaviour — not correctness — should be all they can measure. Publication never cites gate runs: only T4.4's full-profile numbers go in the doc." |
<!-- /status-table:0073 -->

## Changelog

- 2026-09-03 — T4 decomposed into staged sub-tasks (T4.0–T4.5), ordered by cost
  so correctness defects surface in CI or small paid runs before the expensive
  soaks. The A/B was redesigned to run both arms on ONE provisioning per size
  (ADR 0077-T4's provisioning-variance rule: two fresh clusters differ by ~40%,
  wider than the effect being measured). The quota note was corrected: lane A
  needs one driver, so 10 nodes + 1 driver = 11 servers / 44 vCPU fits the
  current project limits without a console raise. The voters arm at N=3 runs
  MQTTD_LEASE_VOTERS=2 so the ownership-domain difference is visible at cheap
  size. The hours-scale soak, node-kill/partition arms and the paid rolling
  upgrade are explicitly last (T4.5), conditioned on everything green before
  them.

- 2026-08-22 — ADR proposed. Grounded in the v1.0.2 curve (durable flat past
  the voter cap by design), the #368 diagnosis (multiplying membership planes
  argued against), and the #376 fix (per-owner throughput worth multiplying).
- 2026-08-23 — #390 FIXED (full-roster-sweep recovery fallback) and T3 completed:
  the acceptance test (pre-grow history surviving migration onto data-less
  learner newcomers) passes under the exact load that made the defect
  deterministic.
- 2026-08-23 — T3's grow soak ran and FOUND issue #390 (grow hand-off race:
  lease reassignment can outrun the old owner's eager data hand-off, stranding
  pre-grow history in permanent fail-closed NoQuorum on a data-less learner
  owner — newly reachable through this ADR's widened domain). The in-tree test
  keeps the proven spread half; the zero-loss half is #390's acceptance test.
- 2026-08-22 — Accepted and T1+T2 shipped, with the mechanism refined during
  implementation: no lease-write forwarding exists because none was ever
  needed (leader-push assignment + local epoch reads were already the
  architecture); the capability flip is per-node conservative recomputation,
  not a committed log record (which would have been rollback-unsafe). The
  learner-serving falsifier passes end to end.
