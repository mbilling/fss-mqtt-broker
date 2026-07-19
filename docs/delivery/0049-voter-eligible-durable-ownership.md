---
adr: "0049"
title: "Durable ownership must be lease-eligible, and a degraded durable plane must be visible"
adr_status: Proposed
tasks:
  - id: 0049-P1
    title: Voter-eligible ownership — plumb the lease voter set into Placement (pushed each reconcile tick by run_driver); group_owner/owner/owner_route hash over voters ∩ eligible with an empty-voters bootstrap fallback; the voter owner leads its group's replica set while replicas still span the full eligible set (ADR 0021 replication-independence preserved); real-cluster test enforces the invariant that no session id maps to a learner owner and every persistent attach succeeds
    status: planned
  - id: 0049-P2
    title: Durable-plane visibility — new counters durable_recovery_failures_total (at the attach refusal) and lease_rpc_timeouts_total (follower AppendEntries/replication timeouts); readiness augmented (not inverted) so a verbose /readyz probe reports durable-serviceability signals without flapping the k8s-facing ready gate
    status: planned
  - id: 0049-P3
    title: Docs + closure — demo sizing note (≥5 durable nodes on one host is fsync-bound), fix the stale hub.rs note_session_ownership "(ephemeral mode)" log line, cross-link the ADR 0021 §2 amendment, and record the leader /readyz-hang as a tracked open investigation
    status: planned
---

# Delivery: ADR 0049 — Voter-eligible durable ownership + durable-plane visibility

[ADR 0049](../adr/0049-voter-eligible-durable-ownership.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0049 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0049-P1 | ⬜ planned | — |  |
| 0049-P2 | ⬜ planned | — |  |
| 0049-P3 | ⬜ planned | — |  |
<!-- /status-table:0049 -->

## Plan

| Task | Done means |
|---|---|
| **0049-P1** Voter-eligible ownership | The lease voter set is plumbed into `Placement` and refreshed each tick; owner selection hashes over voters (∩ eligible, with an empty-voters fallback for bootstrap); the voter owner leads its replica set, which still spans all eligible nodes. A real-cluster test with `voter_cap < N` proves **every** group owner is a voter and a session whose id previously hashed to a learner now attaches. |
| **0049-P2** Visibility | `durable_recovery_failures_total` and `lease_rpc_timeouts_total` exist, increment on the real paths, and render in `/metrics`; a verbose readiness probe surfaces durable-serviceability without changing the plain `/readyz` ready/NotReady contract. A test asserts a recovery refusal moves the counter (which an append failure would not). |
| **0049-P3** Docs + closure | Demo docs state the single-host fsync limit; the misleading `(ephemeral mode)` log line is fixed; ADR 0021 §2 carries the amendment cross-reference; the leader `/readyz`-hang is recorded as an open investigation. ADR → Accepted. |

Order: P1 (the availability fix) → P2 (make the failure visible) → P3 (docs + closure).
P1 is the ship-blocker of the three; P2/P3 harden and close.

## Phased execution plan

- **P1 — the availability bug.** Root cause: `Placement` (`placement.rs`) hashes ownership
  over the full SWIM-eligible set with no voter awareness, so `LeaseAssigner` can hand a
  group lease to a learner that can never serve `claim_session` → CONNACK 0x88 forever for a
  deterministic slice of session ids. Fix: owner selection over the voter set (plumbed from
  `RaftView.voters` via `run_driver`), owner-leads-replica-set, replicas unchanged. This is
  the standalone-valuable core: it closes the data-availability hole.
- **P2 — make it impossible to hide again.** The incident was invisible: `/readyz` green for
  11 h, no metric moved. Add the two counters that would have screamed, and a verbose
  serviceability probe — deliberately *not* flipping the plain `/readyz` (which would flap
  healthy nodes under transient fsync load).
- **P3 — the honest tail.** Document the single-host fsync limit as a demo/operator note, fix
  the `(ephemeral mode)` log line that misleads during exactly this incident, amend ADR 0021
  §2, and file the one unexplained observation (leader `/readyz` hang) as open.

## Changelog

- **2026-07-19** — ADR 0049 drafted from the [7-node post-mortem](../postmortems/2026-07-14-ha-bridge-durable-refused.md).
  Owns both defects the post-mortem surfaced: the placement × voter-cap availability bug
  (durable ownership can land on a learner that never serves it — amends ADR 0021 §2) and the
  readiness/metrics blind spot (a dead durable plane reported healthy for 11 h). Phased
  P1 (fix) → P2 (visibility) → P3 (docs/closure). Tasks **planned** — build begins at P1.
