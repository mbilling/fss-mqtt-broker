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
  - id: 0016-T6
    title: Harden self-asserted failure-domain labels — attested (CA-embedded) labels, plus mismatch loudness
    status: deferred
    notes: "T5 labels are self-asserted: ADR 0022 signing authenticates WHICH node claimed a domain, not that the claim is TRUE, so a compromised-but-certified node can claim a unique fake rack to become the balancer's most attractive voter pick (impact bounded to placement skew/availability — consensus safety still needs a quorum; consistent with the plane's trust model, which already trusts a member for its own address). Candidate hardenings, by strength/cost: (1) CA-attested labels — embed the domain in the cluster-bus certificate (custom extension or SAN URI, SPIFFE-selector style); the ADR 0022 verifier already chain-verifies the cert, so it would extract the label from the cert and reject or ignore a gossiped label that disagrees; strongest fit for this codebase (reuses the existing verify path and 0002-T8 CRL revocation for relabels) at the cost of coupling PKI issuance to topology and making a relabel a reissue+revoke ceremony. (2) Authoritative controller-set labels (the Kubernetes NodeRestriction precedent: kubelets are forbidden from self-setting topology labels) — keep the static operator map authoritative and demote gossip to a drift-detection hint; zero new mechanism but reintroduces exactly the cluster-uniform-config friction T5 removed. (3) Platform attestation — validate claims against signed cloud instance-identity documents (AWS IID / GCP identity JWT carry the zone); strong but environment-specific, adds an external trust root, useless on bare metal. (4) Corroboration heuristics (RTT/coordinate sanity checks) — probabilistic, false-positive-prone, detect-only; poor fit for a security-first broker. (5) Damage limiting without verification — warn on gossip-vs-static mismatch (currently silent; cheap, should land regardless), audit label changes, and/or cooldown before a never-before-seen domain can win a voter seat; cheap but partial. Deferred: the exposure is availability-shaped and requires an already-certified insider; revisit alongside 0022-T7 (gossip cert revocation), since (1) shares its PKI surface."
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
| **0016-T6** Attested labels | *(deferred)* Labels are self-asserted (authenticated to the node, not verified true). Harden by embedding the domain in the cluster-bus cert so the ADR 0022 verifier vouches for it, plus a mismatch `warn!`; options and costs in the task notes. |

## Progress

<!-- status-table:0016 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0016-P1 | ✅ done | 2026-06-18 | a_tombstoned_dead_node_stays_dead; a_tombstone_is_pruned_after_its_ttl_and_the_id_can_rejoin |
| 0016-P2 | ✅ done | 2026-06-19 | awareness_rises_on_self_refutation_and_decays_on_a_clean_probe |
| 0016-P3 | ✅ done | 2026-06-19 | independent_suspicions_shrink_the_window_to_the_floor; one_probers_suspicion_alone_holds_the_full_window |
| 0016-T4 | ✅ done | 2026-06-29 | "lease_membership::target_voters now fills vacancies (and the upgrade-path shrink) by least-represented failure domain with a lowest-id tie-break (pick_balanced/domain_load), so the bounded ADR 0021 voter set spreads across racks/zones and one domain's loss can't take quorum. Deterministic and backward-compatible: with no labels every node is its own singleton domain, reproducing the prior id-ordered selection exactly (decide delegates to decide_with_domains with an empty map; all 16 prior tests unchanged). Topology source: MQTTD_FAILURE_DOMAINS (node-id=domain pairs, cluster-uniform), parsed in main.rs and threaded build_durable_node -> run_driver -> decide_with_domains, re-keyed by raft id. No wire-protocol change (the authenticated gossip plane and peer handshake are untouched); gossip-based auto-propagation of a node's own label is a noted follow-on. Tests: vacancy_fill_spreads_across_failure_domains, within_a_domain_ties_break_by_lowest_id, the_upgrade_shrink_is_also_domain_balanced, a_live_voter_stays_even_if_its_domain_is_over_represented (stickiness wins), a_dead_voter_is_replaced_preferring_a_fresh_domain, no_domains_matches_the_lowest_id_fill." |
| 0016-T5 | ✅ done | 2026-07-01 | "Each node advertises its own MQTTD_FAILURE_DOMAIN over the authenticated SWIM gossip payload; the label rides Update.failure_domain + Message.from_domain (a flag-day wire change, safe pre-release), is learned non-erasingly like peer_addr, and surfaces via Action::StateChange/MembershipEvent.domain into Placement. Placement.domains() reports this node's own label plus every gossip-learned peer label; run_driver reads it live each tick and overlays it on the static MQTTD_FAILURE_DOMAINS seed (gossip wins) before decide_with_domains, so the topology self-assembles and tracks membership with no cluster-uniform static map. Tests: swim a_receiver_learns_a_peers_gossiped_failure_domain, first_contact_teaches_the_senders_domain, an_unlabelled_relay_does_not_erase_a_known_domain, a_node_advertises_its_own_domain_on_outgoing_gossip; placement domains_reports_local_and_gossip_learned_labels, a_dead_peer_drops_its_domain, an_unlabelled_observation_does_not_erase_a_known_domain, an_unlabelled_node_is_absent_from_the_domain_map; over-UDP swim_cluster a_nodes_failure_domain_propagates_over_gossip." |
| 0016-T6 | 💤 deferred | — | "T5 labels are self-asserted: ADR 0022 signing authenticates WHICH node claimed a domain, not that the claim is TRUE, so a compromised-but-certified node can claim a unique fake rack to become the balancer's most attractive voter pick (impact bounded to placement skew/availability — consensus safety still needs a quorum; consistent with the plane's trust model, which already trusts a member for its own address). Candidate hardenings, by strength/cost: (1) CA-attested labels — embed the domain in the cluster-bus certificate (custom extension or SAN URI, SPIFFE-selector style); the ADR 0022 verifier already chain-verifies the cert, so it would extract the label from the cert and reject or ignore a gossiped label that disagrees; strongest fit for this codebase (reuses the existing verify path and 0002-T8 CRL revocation for relabels) at the cost of coupling PKI issuance to topology and making a relabel a reissue+revoke ceremony. (2) Authoritative controller-set labels (the Kubernetes NodeRestriction precedent: kubelets are forbidden from self-setting topology labels) — keep the static operator map authoritative and demote gossip to a drift-detection hint; zero new mechanism but reintroduces exactly the cluster-uniform-config friction T5 removed. (3) Platform attestation — validate claims against signed cloud instance-identity documents (AWS IID / GCP identity JWT carry the zone); strong but environment-specific, adds an external trust root, useless on bare metal. (4) Corroboration heuristics (RTT/coordinate sanity checks) — probabilistic, false-positive-prone, detect-only; poor fit for a security-first broker. (5) Damage limiting without verification — warn on gossip-vs-static mismatch (currently silent; cheap, should land regardless), audit label changes, and/or cooldown before a never-before-seen domain can win a voter seat; cheap but partial. Deferred: the exposure is availability-shaped and requires an already-certified insider; revisit alongside 0022-T7 (gossip cert revocation), since (1) shares its PKI surface." |
<!-- /status-table:0016 -->

**Deliberate deviation (P2):** awareness is bumped **only on self-refutation**, not on a
failed *outgoing* probe. Without NACKs (out of scope), a failed outgoing probe is ambiguous
— the target may simply be dead — and blaming local health there wrongly slows detection of
genuinely-dead peers (it broke `probe_failure_leads_to_suspect_then_dead`, which surfaced
the issue). Self-refutation is the unambiguous "others cannot reach us" signal.

## Changelog

- **2026-07-02** — Post-delivery review of the failure-domain strategy recorded in the ADR
  (new §5): the spread is **eventual, not an invariant** (stickiness preserves a legacy
  concentration until churn); **≥ 3 domains** are required for domain-loss tolerance (the
  majority arithmetic cannot be beaten at 2); T5 trades T4's identical-map-by-construction
  for **eventual consistency** across leaders (accepted: leader-only, debounced,
  vacancy-driven reconcile — worst case a transient extra proposal, never a safety issue);
  and labels are **self-asserted** (authenticated to the node, not verified true).
  Hardening options for the last point — CA-attested labels in the cluster-bus cert
  (preferred; shares PKI surface with 0022-T7), authoritative-controller labels (the
  Kubernetes NodeRestriction precedent), platform attestation, corroboration heuristics,
  and cheap damage-limiting (mismatch `warn!`, novel-domain cooldown) — are recorded with
  their costs as the new deferred **T6**.
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
