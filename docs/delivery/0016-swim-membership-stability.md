---
adr: "0016"
title: SWIM membership stability (dead-node fencing + false-positive resistance)
adr_status: Accepted
tasks:
  - id: 0016-P1
    title: Tombstoned terminal Dead (fixes resurrection)
    status: done
    date: 2026-06-18
    evidence: a_tombstoned_dead_node_stays_dead; a_tombstone_is_pruned_after_its_ttl_and_the_id_can_rejoin
  - id: 0016-P2
    title: Lifeguard local-health awareness multiplier
    status: done
    date: 2026-06-19
    evidence: awareness_rises_on_self_refutation_and_decays_on_a_clean_probe
  - id: 0016-P3
    title: Independent-suspicion confirmation before Dead
    status: done
    date: 2026-06-19
    evidence: independent_suspicions_shrink_the_window_to_the_floor; one_probers_suspicion_alone_holds_the_full_window
  - id: 0016-T4
    title: Failure-domain-aware voter selection (interaction with ADR 0021)
    status: planned
    notes: "Unblocked — ADR 0021 (bounded lease-consensus voter set) is now done (9/9), so the voter-selection seam it introduced exists. Next step: spread voters across failure domains (rack/zone) rather than selecting purely by id hash, so a single domain loss cannot take quorum. No domain-topology input or domain-aware selection logic in tree yet."
---

# Delivery — ADR 0016: SWIM membership stability

Decision: [docs/adr/0016-swim-membership-stability.md](../adr/0016-swim-membership-stability.md).

## Plan

The fix is additive on the existing incarnation/refutation core, sequenced so the
harder-to-mitigate *resurrection* half lands first, then the *false-positive* reductions.
All work is **test-first on the pure `Swim` state machine** (no network), per the ADR's
risk note.

| Task | Acceptance criterion |
|------|----------------------|
| **0016-P1** Tombstone Dead | A `Dead` member is tombstoned (`tombstone_deadline`); no third-party gossip revives it regardless of incarnation; `tick` prunes after `DEAD_TTL`, after which the id may rejoin. |
| **0016-P2** Lifeguard awareness | A per-node `awareness` score scales `ack`/`suspicion` timeouts by `(1 + awareness)`; rises on the unambiguous "we are slow" signal, decays on a clean round, saturates at a cap. |
| **0016-P3** Suspicion confirmation | The suspicion window interpolates from full (one suspecter) to a floor as distinct independent suspecters accumulate; duplicate suspicions from one node do not fast-track; a refutation resets the set. |
| **0016-T4** Failure-domain voters | Voter selection (with ADR 0021's bounded set) spreads across failure domains so one domain's loss never costs quorum. |

## Progress

<!-- status-table:0016 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0016-P1 | ✅ done | 2026-06-18 | a_tombstoned_dead_node_stays_dead; a_tombstone_is_pruned_after_its_ttl_and_the_id_can_rejoin |
| 0016-P2 | ✅ done | 2026-06-19 | awareness_rises_on_self_refutation_and_decays_on_a_clean_probe |
| 0016-P3 | ✅ done | 2026-06-19 | independent_suspicions_shrink_the_window_to_the_floor; one_probers_suspicion_alone_holds_the_full_window |
| 0016-T4 | ⬜ planned | — | "Unblocked — ADR 0021 (bounded lease-consensus voter set) is now done (9/9), so the voter-selection seam it introduced exists. Next step: spread voters across failure domains (rack/zone) rather than selecting purely by id hash, so a single domain loss cannot take quorum. No domain-topology input or domain-aware selection logic in tree yet." |
<!-- /status-table:0016 -->

**Deliberate deviation (P2):** awareness is bumped **only on self-refutation**, not on a
failed *outgoing* probe. Without NACKs (out of scope), a failed outgoing probe is ambiguous
— the target may simply be dead — and blaming local health there wrongly slows detection of
genuinely-dead peers (it broke `probe_failure_leads_to_suspect_then_dead`, which surfaced
the issue). Self-refutation is the unambiguous "others cannot reach us" signal.

## Changelog

- **2026-06-19** — P2 (awareness) + P3 (independent-suspicion confirmation) landed,
  test-first; existing SWIM convergence/detection integration tests still pass under the
  new timing. T4 (failure-domain-aware voters) noted as future work tied to ADR 0021.
- **2026-06-18** — P1 (tombstone `Dead`) landed. Closed the *membership* half of the
  durable-failover gap; the diagnosis then refined to a separate attach-path bug, fixed in
  [ADR 0017](../adr/0017-durable-attach-readiness.md).
