---
adr: "0072"
title: "Per-message durability selection: the mqttd-durability user property"
adr_status: Accepted
tasks:
  - id: 0072-T1
    title: The tiers end to end — mqttd-durability parsed under the operator opt-in; relaxed releases the ack at local_done (nothing structurally skipped); local returns on the owner's durable copy with detached best-effort replication; every non-request falls back to quorum
    status: done
    date: 2026-08-21
    evidence: "DurabilityTier + DURABILITY_PROPERTY in mqtt_storage::repl; ReplicatedLog::append_tiered (defaulted to the full quorum append, so a backend without tiers can never be WEAKER than asked); GroupRoutedLog overrides it with the min-replicas write floor still gating EVERY tier (operator rails outrank publisher wishes); ClusterLog::append_tiered maps Local to required-acks=1 with the follower fan-out DETACHED (spawned to completion, never JoinSet-dropped) and the self-ack still durable-only (ADR 0042 T8 — single-copy, never zero-copy); ReplicatedSessionStore derives the tier from the stored record's own user properties (so a remote-owned append honors it on the owning node with NO peer-protocol change), gated by with_tier_selection; the hub marks relaxed pendings and try_complete_pending releases them at local_done (brownout refusals decided at the plan pass still refuse; appends/forwards/retained commit all still run); operator knob MQTTD_ALLOW_RELAXED_PUBLISH ([durable] allow_relaxed_publish), presence-=-on, default OFF — the property is then ignored and the publish gets the STRONGER strict path; v3.1.1 cannot carry the property and always gets quorum. TESTS: a_relaxed_publish_acks_while_its_append_is_still_parked (ack outruns a parked store, the write still lands after release), the_durability_property_is_inert_without_the_operator_opt_in (identical publish, no knob: ack WAITS on the parked append), a_local_tier_append_returns_on_the_owners_durability_alone (local returns <300ms against 400ms followers with ZERO deliveries resolved, detached fan-out then reaches both, and the same log's quorum append still waits >=350ms); full mqtt-cluster (261) + mqttd lib (334) suites green. Metric publish_tier{tier}."
  - id: 0072-T2
    title: Policy bounding — per-principal/per-topic limits on which tiers may be requested (ACL vocabulary)
    status: planned
    notes: "The ADR's alternatives note: an operator may want relaxed forbidden for some topics/principals even with the global opt-in on. Composes with T1; needs ACL surface design."
---

# Delivery: ADR 0072 — Per-message durability selection

[ADR 0072](../adr/0072-per-message-durability-selection.md) · tasks and status in the
frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0072 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0072-T1 | ✅ done | 2026-08-21 | "DurabilityTier + DURABILITY_PROPERTY in mqtt_storage::repl; ReplicatedLog::append_tiered (defaulted to the full quorum append, so a backend without tiers can never be WEAKER than asked); GroupRoutedLog overrides it with the min-replicas write floor still gating EVERY tier (operator rails outrank publisher wishes); ClusterLog::append_tiered maps Local to required-acks=1 with the follower fan-out DETACHED (spawned to completion, never JoinSet-dropped) and the self-ack still durable-only (ADR 0042 T8 — single-copy, never zero-copy); ReplicatedSessionStore derives the tier from the stored record's own user properties (so a remote-owned append honors it on the owning node with NO peer-protocol change), gated by with_tier_selection; the hub marks relaxed pendings and try_complete_pending releases them at local_done (brownout refusals decided at the plan pass still refuse; appends/forwards/retained commit all still run); operator knob MQTTD_ALLOW_RELAXED_PUBLISH ([durable] allow_relaxed_publish), presence-=-on, default OFF — the property is then ignored and the publish gets the STRONGER strict path; v3.1.1 cannot carry the property and always gets quorum. TESTS: a_relaxed_publish_acks_while_its_append_is_still_parked (ack outruns a parked store, the write still lands after release), the_durability_property_is_inert_without_the_operator_opt_in (identical publish, no knob: ack WAITS on the parked append), a_local_tier_append_returns_on_the_owners_durability_alone (local returns <300ms against 400ms followers with ZERO deliveries resolved, detached fan-out then reaches both, and the same log's quorum append still waits >=350ms); full mqtt-cluster (261) + mqttd lib (334) suites green. Metric publish_tier{tier}." |
| 0072-T2 | ⬜ planned | — | "The ADR's alternatives note: an operator may want relaxed forbidden for some topics/principals even with the global opt-in on. Composes with T1; needs ACL surface design." |
<!-- /status-table:0072 -->

## Changelog

- **2026-08-21** — T1 shipped: quorum | local | relaxed, double opt-in
  (publisher property + operator knob), no wire change anywhere.
- **2026-08-24** — Congestion valve (issue #399, ADR amendment): a gated
  submit that finds its append lane at ≥ half the lane cap marks the publish
  congested; a congested relaxed publish completes by the quorum rule, so the
  publisher's window throttles to the drain rate BEFORE the lane overflows
  (overflow fails the publish and closes the connection — the v1.0.5 curve's
  relaxed-arm reconnect storm). Below the threshold the tier is untouched.
  Test: a_congested_relaxed_publish_waits_for_its_append (lane parked at the
  threshold: the relaxed ack does NOT arrive in 300ms, then arrives after the
  store releases). Owner half: an uncongested relaxed FORWARD is answered
  Stored at submit-acceptance (the verdict outruns the parked append, never
  answered twice); a congested one answers only at append completion; a
  relaxed pending always awaits its forward verdicts — closing the cross-node
  refusal hole (remote lane rejection reached the publisher as Accepted; the
  3-node local repro measured 153k "acked" vs 1.4k delivered with 23,325
  silent drops before, 2,031 msg/s valid with zero drops after). Tests:
  an_uncongested_relaxed_forward_is_answered_at_submit,
  a_congested_relaxed_forward_is_answered_at_append_completion. A/B (1-node
  48×8 relaxed, release): before = publisher connections killed by lane
  overflow (BrokenPipe storm, run aborts); after = 41,911 msg/s, 0 stalls,
  p50 0.64ms.
