---
adr: "0028"
title: Link-gated lease-group voter admission
adr_status: Accepted
tasks:
  - id: 0028-T1
    title: Gate voter admission on raft-link reachability in the lease driver (admission-only; never drop a current voter on a blip)
    status: done
    date: 2026-06-24
    evidence: "MeshRaftNetwork::is_connected exposes the peer registry; durable_node::admit_desired filters placement members to local OR current-voter OR connected, wired into run_driver. Unit tests admit_desired_admits_local_and_reachable_members_only, _keeps_a_current_voter_through_a_link_blip, _drops_a_member_evicted_from_placement. Full durable_sessions suite (7/7) still forms green."
  - id: 0028-T2
    title: Re-soak the durable demo and confirm formation no longer churns (term flat from bring-up)
    status: done
    date: 2026-06-24
    evidence: "Rebuilt durable demo under the loadgen: lease term settled at 8 within ~90s of bring-up and stayed flat for 20+ min (formation watch 8 min + sustained watch 15 min), zero restarts. Before the gate the same setup churned term 7 -> 71 over ~8 min (68 elections on one node) before settling. Formation churn eliminated."
  - id: 0028-T3
    title: Revisit the durable-default decision once formation is proven (broker + demo)
    status: done
    date: 2026-06-24
    evidence: "Formation proven clean (T2), so durable was made the default for the broker and demo — see ADR 0029 (docs/delivery/0029-durable-by-default.md)."
---

# Delivery — ADR 0028: Link-gated lease-group voter admission

Decision: [docs/adr/0028-link-gated-voter-admission.md](../adr/0028-link-gated-voter-admission.md).

A 35-minute durable soak (after ADR 0026 + 0027) showed steady state rock-stable but ~8 minutes
of formation churn: the founder admitted all voters as soon as SWIM listed them, before their
raft links were up, so it lost quorum and re-elected until the mesh converged. Gate admission on
link readiness.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0028-T1** Gate | The lease driver admits a member as a voter only once its raft link is connected; a current voter is never dropped on a transient blip (admission-only). Pure, unit-tested. |
| **0028-T2** Soak | A rebuilt durable demo forms quickly and quietly — the lease term does **not** climb for minutes during bring-up (the ~8-min churn is gone) and stays flat under load. |
| **0028-T3** Default | With formation proven, revisit making durable the default for the broker + demo (the user's goal), kept opt-in until then. |

## Progress

<!-- status-table:0028 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0028-T1 | ✅ done | 2026-06-24 | "MeshRaftNetwork::is_connected exposes the peer registry; durable_node::admit_desired filters placement members to local OR current-voter OR connected, wired into run_driver. Unit tests admit_desired_admits_local_and_reachable_members_only, _keeps_a_current_voter_through_a_link_blip, _drops_a_member_evicted_from_placement. Full durable_sessions suite (7/7) still forms green." |
| 0028-T2 | ✅ done | 2026-06-24 | "Rebuilt durable demo under the loadgen: lease term settled at 8 within ~90s of bring-up and stayed flat for 20+ min (formation watch 8 min + sustained watch 15 min), zero restarts. Before the gate the same setup churned term 7 -> 71 over ~8 min (68 elections on one node) before settling. Formation churn eliminated." |
| 0028-T3 | ✅ done | 2026-06-24 | "Formation proven clean (T2), so durable was made the default for the broker and demo — see ADR 0029 (docs/delivery/0029-durable-by-default.md)." |
<!-- /status-table:0028 -->

## Changelog

- **2026-06-24** — ADR accepted after a soak root-caused the formation churn to voters admitted
  before their links were ready. T1 (the admission gate) landed test-first; T2 (re-soak) and T3
  (revisit the default) follow.
