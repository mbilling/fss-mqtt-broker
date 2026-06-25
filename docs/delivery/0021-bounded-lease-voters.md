---
adr: "0021"
title: Bounded lease-consensus voter set
adr_status: Accepted
tasks:
  - id: 0021-T1
    title: MQTTD_LEASE_VOTERS config (default 5, odd; effective = min(N, live_eligible))
    status: done
    date: 2026-06-25
    evidence: "main.rs lease_voters() parses MQTTD_LEASE_VOTERS (default 5; 0 or unparseable is a startup error), threaded through build_durable_node into MembershipReconciler::new(.., voter_cap)."
  - id: 0021-T2
    title: durable_node.rs - replace desired=all-members with alive set + RaftView passed to reconciler
    status: done
    date: 2026-06-25
    evidence: "run_driver already computes the admitted (ADR 0028) alive set and passes the RaftView (now carrying voters AND nodes) to decide(); the cap reshapes that set into bounded voters."
  - id: 0021-T3
    title: Sticky vacancy-fill voter selection (promote lowest-id alive learner; never demote a live voter on join)
    status: done
    date: 2026-06-25
    evidence: "lease_membership::target_voters (deterministic fn of cap/eligible/current); tests a_live_voter_is_never_demoted_just_because_a_node_joins, a_dead_voter_is_replaced_by_the_lowest_id_learner."
  - id: 0021-T4
    title: All members added as learners so the committed lease log replicates to every node
    status: done
    date: 2026-06-25
    evidence: "decide adds every eligible member not yet in the group as a learner (add_as_learner = eligible - nodes); test a_high_id_join_becomes_a_learner_without_changing_voters."
  - id: 0021-T5
    title: Reconciler reshape - decide returns target (voters, learners); apply_action adds/promotes/demotes-to-learner/drops-departed
    status: done
    date: 2026-06-25
    evidence: "MembershipAction::Reconcile{target_voters, add_as_learner} (ReplaceAllVoters retain=true promotes fills, demotes removed voters to learners) + ::Drop (RemoveNodes departed); apply_action skips a no-op voter change."
  - id: 0021-T6
    title: Founder/bootstrap unaffected (sole-voter bootstrap then grows capped at N)
    status: done
    date: 2026-06-25
    evidence: "Initialize path unchanged; reconciler_bootstraps_then_grows_the_group (live) + a_bounded_voter_cluster_* (grows founder→3 voters capped at N)."
  - id: 0021-T7
    title: Pure policy tests (>N -> exactly N voters; dead voter replaced by lowest-id learner; high-id join no voter change; learner-owner reads lease; N>cluster all-voters; N=1 single voter)
    status: done
    date: 2026-06-25
    evidence: "lease_membership::tests — more_than_n_members_yield_exactly_n_voters, a_dead_voter_is_replaced_by_the_lowest_id_learner, a_high_id_join_becomes_a_learner_without_changing_voters, an_all_voters_cluster_shrinks_to_the_cap, n_larger_than_the_cluster_makes_every_member_a_voter, n_equals_one_keeps_a_single_voter, a_zero_cap_is_clamped_to_a_single_voter, a_departed_learner_is_dropped, a_steady_bounded_group_is_a_noop (16 policy tests)."
  - id: 0021-T8
    title: Integration - 5+-node durable cluster with bounded voter set; learner-owned session survives a non-voter and a voter failure
    status: done
    date: 2026-06-25
    evidence: "durable_sessions::a_bounded_voter_cluster_keeps_a_learner_owned_session_through_failures — 5 nodes cap 3 form exactly 3 voters; a learner-owned session is durable and survives a non-voter then a voter failure (learner promoted to voter live). Stable across 3 runs (~4.7s)."
  - id: 0021-T9
    title: Re-run openraft storage conformance (asserted unaffected)
    status: done
    date: 2026-06-25
    evidence: "passes_openraft_conformance_suite_in_memory + _persistent still green — the change is in the reconciler policy, not the LeaseStore."
---

# Delivery — ADR 0021: Bounded lease-consensus voter set

Decision: [docs/adr/0021-bounded-lease-voters.md](../adr/0021-bounded-lease-voters.md).

## Plan

The decision's numbered parts and implementation-notes workstream decompose into these
tasks. Each carries a stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0021-T1** Config | `MQTTD_LEASE_VOTERS` (default `5`, recommend odd) bounds the voter set; effective voters = `min(N, live_eligible_count)`; quorum is `⌊N/2⌋+1` regardless of cluster size. |
| **0021-T2** durable_node wiring | `durable_node.rs` stops computing `desired = all members`; it passes the alive member set and the current `RaftView` (voters) to the reconciler so the sticky policy and `N` cap can apply. |
| **0021-T3** Sticky vacancy-fill | A live voter stays a voter; when live voters < `N`, promote the lowest-id alive learner(s) until `N` (or all live members); a departed voter is removed; a deterministic function of *(committed voter config, alive members)* so reconcilers agree. |
| **0021-T4** All-learners | Every eligible member is added as a learner so the committed lease log replicates to all; a learner that HRW makes an owner reads its lease epoch from that log without voting. |
| **0021-T5** Reconciler reshape | `decide` returns a target *(voters, learners)*; `apply_action` adds learners (blocking catch-up), `change_membership` promotes fills and demotes removed voters to learners (retain), drops departed members; quorum-safe via incremental `change_membership`. |
| **0021-T6** Founder/bootstrap | The founder bootstraps as sole voter; vacancy-fill grows the voter set to `N` as members join — same growth path, capped at `N`. |
| **0021-T7** Policy tests | Pure-where-possible tests: `>N` members → exactly `N` voters; dead voter replaced by lowest-id learner (count restored); high-id join → learner, no voter change; learner-owner reads/serves its lease; `N > cluster` → all-voters; `N = 1` → sane single voter. |
| **0021-T8** Integration | A 5+-node durable cluster forms with a bounded voter set and a learner-owned session survives a non-voter and a voter failure. |
| **0021-T9** Conformance | openraft's storage conformance suite re-runs and is asserted unaffected. |

## Progress

<!-- status-table:0021 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0021-T1 | ✅ done | 2026-06-25 | "main.rs lease_voters() parses MQTTD_LEASE_VOTERS (default 5; 0 or unparseable is a startup error), threaded through build_durable_node into MembershipReconciler::new(.., voter_cap)." |
| 0021-T2 | ✅ done | 2026-06-25 | "run_driver already computes the admitted (ADR 0028) alive set and passes the RaftView (now carrying voters AND nodes) to decide(); the cap reshapes that set into bounded voters." |
| 0021-T3 | ✅ done | 2026-06-25 | "lease_membership::target_voters (deterministic fn of cap/eligible/current); tests a_live_voter_is_never_demoted_just_because_a_node_joins, a_dead_voter_is_replaced_by_the_lowest_id_learner." |
| 0021-T4 | ✅ done | 2026-06-25 | "decide adds every eligible member not yet in the group as a learner (add_as_learner = eligible - nodes); test a_high_id_join_becomes_a_learner_without_changing_voters." |
| 0021-T5 | ✅ done | 2026-06-25 | "MembershipAction::Reconcile{target_voters, add_as_learner} (ReplaceAllVoters retain=true promotes fills, demotes removed voters to learners) + ::Drop (RemoveNodes departed); apply_action skips a no-op voter change." |
| 0021-T6 | ✅ done | 2026-06-25 | "Initialize path unchanged; reconciler_bootstraps_then_grows_the_group (live) + a_bounded_voter_cluster_* (grows founder→3 voters capped at N)." |
| 0021-T7 | ✅ done | 2026-06-25 | "lease_membership::tests — more_than_n_members_yield_exactly_n_voters, a_dead_voter_is_replaced_by_the_lowest_id_learner, a_high_id_join_becomes_a_learner_without_changing_voters, an_all_voters_cluster_shrinks_to_the_cap, n_larger_than_the_cluster_makes_every_member_a_voter, n_equals_one_keeps_a_single_voter, a_zero_cap_is_clamped_to_a_single_voter, a_departed_learner_is_dropped, a_steady_bounded_group_is_a_noop (16 policy tests)." |
| 0021-T8 | ✅ done | 2026-06-25 | "durable_sessions::a_bounded_voter_cluster_keeps_a_learner_owned_session_through_failures — 5 nodes cap 3 form exactly 3 voters; a learner-owned session is durable and survives a non-voter then a voter failure (learner promoted to voter live). Stable across 3 runs (~4.7s)." |
| 0021-T9 | ✅ done | 2026-06-25 | "passes_openraft_conformance_suite_in_memory + _persistent still green — the change is in the reconciler policy, not the LeaseStore." |
<!-- /status-table:0021 -->

## Changelog

- **2026-06-25** — ADR ratified (Accepted) and shipped end to end. The reconciler now
  computes a bounded, sticky target voter set (`target_voters`) instead of "all members
  vote": at most `MQTTD_LEASE_VOTERS` (default 5) members vote, every other eligible
  member joins as a learner that still receives the lease log and can own/serve sessions.
  `MembershipAction` reshaped to `Reconcile { target_voters, add_as_learner }` (promote
  fills / demote-to-learner via `ReplaceAllVoters` with `retain = true`) + `Drop` (remove
  departed learners via `RemoveNodes`); `RaftView` gained the full `nodes` set. 16 pure
  policy tests + a 5-node integration test (learner-owned session survives a non-voter and
  a voter failure, with a live learner→voter promotion); openraft conformance re-asserted
  green. All ADR 0026/0028 churn protections (the `changing` joint-consensus gate, link-
  gated admission, the one-tick debounce) are preserved.
- **2026-06-19** — Delivery doc opened from the Proposed (design-only) ADR; all tasks
  `planned` pending ratification.
