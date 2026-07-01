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
    status: done
    date: 2026-06-29
    evidence: "lease_membership::target_voters now fills vacancies (and the upgrade-path shrink) by least-represented failure domain with a lowest-id tie-break (pick_balanced/domain_load), so the bounded ADR 0021 voter set spreads across racks/zones and one domain's loss can't take quorum. Deterministic and backward-compatible: with no labels every node is its own singleton domain, reproducing the prior id-ordered selection exactly (decide delegates to decide_with_domains with an empty map; all 16 prior tests unchanged). Topology source: MQTTD_FAILURE_DOMAINS (node-id=domain pairs, cluster-uniform), parsed in main.rs and threaded build_durable_node -> run_driver -> decide_with_domains, re-keyed by raft id. No wire-protocol change (the authenticated gossip plane and peer handshake are untouched); gossip-based auto-propagation of a node's own label is a noted follow-on. Tests: vacancy_fill_spreads_across_failure_domains, within_a_domain_ties_break_by_lowest_id, the_upgrade_shrink_is_also_domain_balanced, a_live_voter_stays_even_if_its_domain_is_over_represented (stickiness wins), a_dead_voter_is_replaced_preferring_a_fresh_domain, no_domains_matches_the_lowest_id_fill."
  - id: 0016-T5
    title: Follow-on — gossip a node's own failure-domain label so the topology auto-propagates (no cluster-uniform static map)
    status: done
    date: 2026-07-01
    evidence: "Each node advertises its own MQTTD_FAILURE_DOMAIN over the authenticated SWIM gossip payload; the label rides Update.failure_domain + Message.from_domain (a flag-day wire change, safe pre-release), is learned non-erasingly like peer_addr, and surfaces via Action::StateChange/MembershipEvent.domain into Placement. Placement.domains() reports this node's own label plus every gossip-learned peer label; run_driver reads it live each tick and overlays it on the static MQTTD_FAILURE_DOMAINS seed (gossip wins) before decide_with_domains, so the topology self-assembles and tracks membership with no cluster-uniform static map. Tests: swim a_receiver_learns_a_peers_gossiped_failure_domain, first_contact_teaches_the_senders_domain, an_unlabelled_relay_does_not_erase_a_known_domain, a_node_advertises_its_own_domain_on_outgoing_gossip; placement domains_reports_local_and_gossip_learned_labels, a_dead_peer_drops_its_domain, an_unlabelled_observation_does_not_erase_a_known_domain, an_unlabelled_node_is_absent_from_the_domain_map; over-UDP swim_cluster a_nodes_failure_domain_propagates_over_gossip."
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
| **0016-T5** Gossip the domain | Each node advertises only its own `MQTTD_FAILURE_DOMAIN` over the authenticated SWIM gossip payload; peers learn it (non-erasingly), it flows through `Placement`, and the driver reads a live self-assembled map — no static cluster-uniform table required. |

## Progress

<!-- status-table:0016 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0016-P1 | ✅ done | 2026-06-18 | a_tombstoned_dead_node_stays_dead; a_tombstone_is_pruned_after_its_ttl_and_the_id_can_rejoin |
| 0016-P2 | ✅ done | 2026-06-19 | awareness_rises_on_self_refutation_and_decays_on_a_clean_probe |
| 0016-P3 | ✅ done | 2026-06-19 | independent_suspicions_shrink_the_window_to_the_floor; one_probers_suspicion_alone_holds_the_full_window |
| 0016-T4 | ✅ done | 2026-06-29 | "lease_membership::target_voters now fills vacancies (and the upgrade-path shrink) by least-represented failure domain with a lowest-id tie-break (pick_balanced/domain_load), so the bounded ADR 0021 voter set spreads across racks/zones and one domain's loss can't take quorum. Deterministic and backward-compatible: with no labels every node is its own singleton domain, reproducing the prior id-ordered selection exactly (decide delegates to decide_with_domains with an empty map; all 16 prior tests unchanged). Topology source: MQTTD_FAILURE_DOMAINS (node-id=domain pairs, cluster-uniform), parsed in main.rs and threaded build_durable_node -> run_driver -> decide_with_domains, re-keyed by raft id. No wire-protocol change (the authenticated gossip plane and peer handshake are untouched); gossip-based auto-propagation of a node's own label is a noted follow-on. Tests: vacancy_fill_spreads_across_failure_domains, within_a_domain_ties_break_by_lowest_id, the_upgrade_shrink_is_also_domain_balanced, a_live_voter_stays_even_if_its_domain_is_over_represented (stickiness wins), a_dead_voter_is_replaced_preferring_a_fresh_domain, no_domains_matches_the_lowest_id_fill." |
| 0016-T5 | ✅ done | 2026-07-01 | "Each node advertises its own MQTTD_FAILURE_DOMAIN over the authenticated SWIM gossip payload; the label rides Update.failure_domain + Message.from_domain (a flag-day wire change, safe pre-release), is learned non-erasingly like peer_addr, and surfaces via Action::StateChange/MembershipEvent.domain into Placement. Placement.domains() reports this node's own label plus every gossip-learned peer label; run_driver reads it live each tick and overlays it on the static MQTTD_FAILURE_DOMAINS seed (gossip wins) before decide_with_domains, so the topology self-assembles and tracks membership with no cluster-uniform static map. Tests: swim a_receiver_learns_a_peers_gossiped_failure_domain, first_contact_teaches_the_senders_domain, an_unlabelled_relay_does_not_erase_a_known_domain, a_node_advertises_its_own_domain_on_outgoing_gossip; placement domains_reports_local_and_gossip_learned_labels, a_dead_peer_drops_its_domain, an_unlabelled_observation_does_not_erase_a_known_domain, an_unlabelled_node_is_absent_from_the_domain_map; over-UDP swim_cluster a_nodes_failure_domain_propagates_over_gossip." |
<!-- /status-table:0016 -->

**Deliberate deviation (P2):** awareness is bumped **only on self-refutation**, not on a
failed *outgoing* probe. Without NACKs (out of scope), a failed outgoing probe is ambiguous
— the target may simply be dead — and blaming local health there wrongly slows detection of
genuinely-dead peers (it broke `probe_failure_leads_to_suspect_then_dead`, which surfaced
the issue). Self-refutation is the unambiguous "others cannot reach us" signal.

## Changelog

- **2026-07-01** — T5 (gossip-propagated failure domains) landed, unblocking it now that the
  gossip wire is free to take a flag-day change (the mainline was never released, so no
  mixed-version wire compatibility is owed). Each node advertises **only its own**
  `MQTTD_FAILURE_DOMAIN` over the authenticated SWIM gossip payload (`Update.failure_domain` +
  `Message.from_domain`, learned non-erasingly like `peer_addr`); the label flows through
  `Action::StateChange` → `MembershipEvent.domain` → `Placement`, and the lease-group driver
  reads `Placement::domains()` **live** each tick, overlaying it on the static
  `MQTTD_FAILURE_DOMAINS` seed (gossip wins) before `decide_with_domains`. The failure-domain
  topology now self-assembles and tracks membership — the cluster-uniform static map is a
  fallback, no longer required. Test-first at every layer (state machine, placement, and an
  over-UDP propagation test); the T4 selection algorithm is unchanged.
- **2026-06-29** — T4 (failure-domain-aware voters) landed, now that ADR 0021's bounded
  voter-selection seam exists. `target_voters` fills vacancies (and the upgrade shrink) by
  least-represented failure domain (lowest-id tie-break), so the bounded voter set spreads
  across racks/zones and one domain's loss can't take quorum. Backward-compatible — no labels
  reproduces the prior id-ordered selection exactly. Topology from a cluster-uniform
  `MQTTD_FAILURE_DOMAINS` map (no wire-protocol change); gossip-based auto-propagation of a
  node's own label is tracked as T5 (deferred).
- **2026-06-19** — P2 (awareness) + P3 (independent-suspicion confirmation) landed,
  test-first; existing SWIM convergence/detection integration tests still pass under the
  new timing. T4 (failure-domain-aware voters) noted as future work tied to ADR 0021.
- **2026-06-18** — P1 (tombstone `Dead`) landed. Closed the *membership* half of the
  durable-failover gap; the diagnosis then refined to a separate attach-path bug, fixed in
  [ADR 0017](../adr/0017-durable-attach-readiness.md).
