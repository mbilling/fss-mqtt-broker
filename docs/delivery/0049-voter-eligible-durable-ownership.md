---
adr: "0049"
title: "Durable ownership must be lease-eligible, and a degraded durable plane must be visible"
adr_status: Accepted
tasks:
  - id: 0049-P1
    title: Voter-eligible ownership — plumb the lease voter set into Placement (pushed each reconcile tick by run_driver); group_owner/owner/owner_route hash over voters ∩ eligible with an empty-voters bootstrap fallback; the voter owner leads its group's replica set while replicas still span the full eligible set (ADR 0021 replication-independence preserved); real-cluster test enforces the invariant that no session id maps to a learner owner and every persistent attach succeeds
    status: done
    date: 2026-07-19
    evidence: "placement.rs: voters field + set_voters (pushed each run_driver tick from membership_config.voter_ids(), the same source DurablePlane::voter_count trusts) + owner_over/owner_led_replica_set helpers; group_owner/group_replica_set/group_replica_set_without restricted to voters (owner leads), replicas still span eligible. Unit tests: owner is always a voter across all 256 groups, replicas still hit learners, empty-voters fallback == pre-0049. Rewrote the bounded-voter integration test (a_bounded_voter_cluster_owns_every_session_on_a_voter_and_survives_failures, amends ADR 0021 §2): all 5 nodes converge on a 3-voter set, no session owns on a learner, and a voter-owned session survives a replica loss + another failure. Full durable_sessions (10) + mqtt-cluster (245) green; clippy -D warnings + fmt clean."
  - id: 0049-P2
    title: Durable-plane visibility — durable_recovery_failures_total (the direct 0x88 fingerprint, at the attach refusal) + a lease_quorum_ack_ms gauge (the leading indicator, from openraft's millis_since_quorum_ack — the accurate instrument, since the incident's degradation is follower fsync slowness a network-level RPC-timeout counter cannot see); readiness augmented (not inverted) so the /readyz body reports durable-serviceability signals without flapping the k8s-facing ready gate
    status: done
    date: 2026-07-19
    evidence: "metrics.rs: durable_recovery_failures_total (Family<ReasonLabel>, dual OTLP+prometheus, fn durable_recovery_failed) incremented in the hub's SessionRecovery::Unavailable arm; lease_quorum_ack_ms gauge (fn set_lease_quorum_ack_ms) mirrored each refresh_gauges from DurablePlane::quorum_ack_age_ms() → openraft millis_since_quorum_ack (mqtt-cluster keeps no observability dep — plane exposes the value, hub mirrors it). health.rs: /readyz body gains voters + quorum_ack_age_ms (status contract unchanged). Test a_refused_durable_recovery_is_counted: a deadline-refused attach moves durable_recovery_failures_total{reason=deadline} and NOT durable_append_failures. mqttd lib (146) green; clippy -D warnings + fmt clean."
  - id: 0049-P3
    title: Docs + closure — demo sizing note (≥5 durable nodes on one host is fsync-bound), fix the stale hub.rs note_session_ownership "(ephemeral mode)" log line, cross-link the ADR 0021 §2 amendment, and record the leader /readyz-hang as a tracked open investigation
    status: done
    date: 2026-07-19
    evidence: "demo/README.md gains a single-host fsync-bound sizing caveat pointing at lease_quorum_ack_ms / durable_recovery_failures_total (ADR 0049) + the post-mortem. hub.rs note_session_ownership log line reworded (dropped the misleading '(ephemeral mode)' on a durable cluster → 'session relocation / cross-node affinity'). ADR 0021 §2 carries an inline amendment block pointing at ADR 0049. The leader /readyz-hang stays a tracked open item (ADR 0049 'out of scope' + post-mortem follow-up 4). ADR 0049 → Accepted."
---

# Delivery: ADR 0049 — Voter-eligible durable ownership + durable-plane visibility

[ADR 0049](../adr/0049-voter-eligible-durable-ownership.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0049 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0049-P1 | ✅ done | 2026-07-19 | "placement.rs: voters field + set_voters (pushed each run_driver tick from membership_config.voter_ids(), the same source DurablePlane::voter_count trusts) + owner_over/owner_led_replica_set helpers; group_owner/group_replica_set/group_replica_set_without restricted to voters (owner leads), replicas still span eligible. Unit tests: owner is always a voter across all 256 groups, replicas still hit learners, empty-voters fallback == pre-0049. Rewrote the bounded-voter integration test (a_bounded_voter_cluster_owns_every_session_on_a_voter_and_survives_failures, amends ADR 0021 §2): all 5 nodes converge on a 3-voter set, no session owns on a learner, and a voter-owned session survives a replica loss + another failure. Full durable_sessions (10) + mqtt-cluster (245) green; clippy -D warnings + fmt clean." |
| 0049-P2 | ✅ done | 2026-07-19 | "metrics.rs: durable_recovery_failures_total (Family<ReasonLabel>, dual OTLP+prometheus, fn durable_recovery_failed) incremented in the hub's SessionRecovery::Unavailable arm; lease_quorum_ack_ms gauge (fn set_lease_quorum_ack_ms) mirrored each refresh_gauges from DurablePlane::quorum_ack_age_ms() → openraft millis_since_quorum_ack (mqtt-cluster keeps no observability dep — plane exposes the value, hub mirrors it). health.rs: /readyz body gains voters + quorum_ack_age_ms (status contract unchanged). Test a_refused_durable_recovery_is_counted: a deadline-refused attach moves durable_recovery_failures_total{reason=deadline} and NOT durable_append_failures. mqttd lib (146) green; clippy -D warnings + fmt clean." |
| 0049-P3 | ✅ done | 2026-07-19 | "demo/README.md gains a single-host fsync-bound sizing caveat pointing at lease_quorum_ack_ms / durable_recovery_failures_total (ADR 0049) + the post-mortem. hub.rs note_session_ownership log line reworded (dropped the misleading '(ephemeral mode)' on a durable cluster → 'session relocation / cross-node affinity'). ADR 0021 §2 carries an inline amendment block pointing at ADR 0049. The leader /readyz-hang stays a tracked open item (ADR 0049 'out of scope' + post-mortem follow-up 4). ADR 0049 → Accepted." |
<!-- /status-table:0049 -->

## Plan

| Task | Done means |
|---|---|
| **0049-P1** Voter-eligible ownership | The lease voter set is plumbed into `Placement` and refreshed each tick; owner selection hashes over voters (∩ eligible, with an empty-voters fallback for bootstrap); the voter owner leads its replica set, which still spans all eligible nodes. A real-cluster test with `voter_cap < N` proves **every** group owner is a voter and a session whose id previously hashed to a learner now attaches. |
| **0049-P2** Visibility | `durable_recovery_failures_total` (the direct refusal fingerprint) and a `lease_quorum_ack_ms` gauge (the leading indicator) exist, update on the real paths, and render in `/metrics`; the `/readyz` body surfaces durable-serviceability without changing the ready/NotReady status contract. A test asserts a recovery refusal moves the recovery counter (which an append failure would not). |
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
  11 h, no metric moved. Add the refusal counter + the quorum-ack-age gauge that would have
  screamed, and enrich the `/readyz` body — deliberately *not* flipping the plain `/readyz`
  status (which would flap healthy nodes under transient fsync load).
- **P3 — the honest tail.** Document the single-host fsync limit as a demo/operator note, fix
  the `(ephemeral mode)` log line that misleads during exactly this incident, amend ADR 0021
  §2, and file the one unexplained observation (leader `/readyz` hang) as open.

## Changelog

- **2026-07-19** — ADR 0049 drafted from the [7-node post-mortem](../postmortems/2026-07-14-ha-bridge-durable-refused.md).
  Owns both defects the post-mortem surfaced: the placement × voter-cap availability bug
  (durable ownership can land on a learner that never serves it — amends ADR 0021 §2) and the
  readiness/metrics blind spot (a dead durable plane reported healthy for 11 h). Phased
  P1 (fix) → P2 (visibility) → P3 (docs/closure). Tasks **planned** — build begins at P1.
- **2026-07-19** — **P1 done.** Durable ownership is now voter-restricted: `Placement` carries
  the lease voter set (pushed each `run_driver` tick), and owner selection hashes over it
  (owner-led replica set, replicas still span eligible). A debug find along the way: the voter
  set must be read from `membership_config.voter_ids()` (what `voter_count` trusts), not
  `raft_view`'s `membership().voter_ids()`, which under a bounded set reported a base membership
  and left ownership split-brained. The bounded-voter integration test was rewritten to the
  amended invariant (no session owns on a learner; durable through failures). All green.
- **2026-07-19** — **P1 hardening.** The out-of-process disk-fault test (`cluster_proc`
  `a_disk_bound_crash_mid_write`) exposed a second issue the in-process tests missed: the
  founder bootstraps as *sole voter*, so restricting ownership immediately concentrated all
  256 groups on it and then thrashed them out via mass migration — which never converges on a
  disk-stressed founder (persistent `not the owning node`, zero acks). Fix: **settle-gate** the
  restriction — only restrict once the voter set has held steady for 3 ticks; while it grows,
  fall back to eligible. In a small all-voter cluster the set settles at `voters == eligible`,
  so the restriction is a no-op there; the bounded cluster still gets it. Full `cluster_proc`
  (3), `durable_sessions` (10), `mqtt-cluster` (245) green; clippy + fmt clean.
- **2026-07-19** — **P2 done.** Durable-plane visibility: `durable_recovery_failures_total`
  (the direct 0x88 fingerprint, at the attach refusal) + a `lease_quorum_ack_ms` gauge, plus
  durable-serviceability detail in the `/readyz` body. A design correction: the draft's
  `lease_rpc_timeouts_total` counter cannot see this failure mode — the incident's degradation
  is follower *fsync* slowness with a healthy network, where openraft's RPC timeout fires but
  our `MeshConn` send still gets a late reply. openraft's `millis_since_quorum_ack` measures
  the degradation directly and reads cleanly from raft metrics, so it's the instrument shipped.
  New test proves a recovery refusal moves the recovery counter and not the append counter.
- **2026-07-19** — **P3 done; ADR Accepted.** Docs/closure: a single-host fsync-bound sizing
  caveat on the demo, the misleading `(ephemeral mode)` log line corrected, ADR 0021 §2
  amended inline (learners replicate + hold data but do not *own* durable groups), and the
  leader `/readyz`-hang recorded as a tracked open investigation. All three phases (fix →
  visibility → docs) landed; both post-mortem defects are closed.
