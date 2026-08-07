---
adr: "0054"
title: "Operator-facing state surface: /statusz + state gauges"
adr_status: Accepted
tasks:
  - id: 0054-T1
    title: /statusz route + state gauges — identity/members/lease/decommission/brownout/store/proto body on the health listener; brownout{axis}, store_max_bytes, decommission_state/pending, voters, replica_groups_current/tracked gauges wired on existing refresh paths
    status: done
    date: 2026-08-05
    evidence: "PR #79. /statusz always-200 on the health listener; unbounded detail body-only (member lists, voter ids), bounded gauges Prometheus+OTLP; statusz shape/brownout-transition/JSON-escape tests, store-watch snapshot test; mqttd lib 150 green, cardinality guard green" 
  - id: 0054-T2
    title: Cluster identity — minted at founding, persisted (cluster-id file in the data dir), gossip-propagated, adopted by joiners; cluster-mismatch gossip guard (containment, not just detection); cluster_info/founder/foundings_total metrics; statusz cluster block; OPERATIONS.md split-brain detection rule
    status: done
    date: 2026-08-05
    evidence: "PR #80. End-to-end over real sockets: joiner adopts the founder's id; a separately-founded cluster's gossip counted cluster-mismatch and its node never enters the membership view; identity unit tests (mint-once/reload-stable, adopt-once/persist, distinct foundings); swim wire field appended (pre-1.0 reshape, disclosed), fuzz seed regenerated" 
  - id: 0054-T3
    title: Rotation + convergence visibility — SWIM key count/fingerprints (never material), config checksum + reload generation, peer proto gauge; statusz keys/config blocks; OPERATIONS.md rotation verification
    status: done
    date: 2026-08-05
    evidence: "SwimAuth::key_fingerprints (sha256/8B hex per accepted key, primary first) + swim_keys_accepted gauge; reload::ConfigStamp (checksum of the config file bytes + applied-generation counter) recorded at startup and on every successful reload, mirrored to config_info{checksum} with previous-series zeroing; peer_proto_min/max gauges; statusz keys/config blocks with tests (fingerprint-not-material unit test; statusz shape); OPERATIONS.md rotation-verification steps" 
  - id: 0054-T4
    title: Monitoring docs + dashboard — Grafana rows for the new signals (brownout, store utilization, decommission, cluster identity, mismatch rate) and the OPERATIONS.md alert-rule catalogue the operator will encode
    status: done
    date: 2026-08-05
    evidence: "Demo dashboard gains an 'Operator signals (ADR 0054)' row: cluster-identity table (two values = split brain), foundings + cluster-mismatch rates, brownout state, store utilization vs watermark, decommission state/pending, replication lag + voters, rotation windows + config convergence, and the previously never-dashboarded ADR 0041 rejection families. OPERATIONS.md 'Monitoring for the operator' catalogues ten alert rules with actions; ADR 0047 amendment notes the operator path engaged with signals-first sequencing" 
  - id: 0054-T5
    title: Self-quarantine for the odd node of a split brain — a node alone AND hearing another cluster's gossip refuses readiness rather than serve an empty store; /statusz quarantine block, mqttd_refound_quarantine gauge, cluster.refound_guard escape hatch
    status: done
    date: 2026-08-06
    evidence: "Shipped in be96989 (#98), recorded 2026-08-07 during a delivery-record audit — the behaviour reached main with no task and no evidence, the mirror image of a title claiming work never done. T2 made a second, separately-founded cluster DETECTABLE and contained it at the gossip layer; that scope proved too narrow. The divergent node's own MQTT listener was untouched: a pod-0 whose volume is lost mints a new identity, its single-voter lease group elects itself, members=1 satisfies the founder's ready_min_members=1, so /readyz returns 200 and it joins the client Service endpoints — serving a share of connections from an EMPTY session and retained store beside the real cluster. OPERATIONS.md half-knew this ('the founder floor is 1 — act promptly'). A node now refuses readiness when, with the guard armed (cluster-configured, cluster.refound_guard default true), it has observed cluster-mismatch gossip AND its membership view holds only itself. Those two identify the divergent node and only it: it rejects every foreign datagram so it never learns a peer, while the healthy majority sees the same gossip but has each other. A genuine first bootstrap is alone too, but nothing is alive to send it foreign gossip, so the rule cannot fire there — load-bearing, because OrderedReady means pod-0 must come Ready alone or no other pod is created. NOTE the first design keyed on ClusterIdentity::minted and the kind e2e FALSIFIED it: minted is per-PROCESS while the divergence lives on disk, so the re-founder mints only in its first life and the operator's fence — by deleting the pod — is precisely what ends it; the guard was inert in the case it was built for. Membership survives a restart, provenance does not. Both conjuncts verified load-bearing by removing each in turn. Evaluated live, not latched, so a node that legitimately rejoins serves again unattended. Visible not just a 503: /statusz quarantine block + mqttd_refound_quarantine gauge + an OPERATIONS.md alert row, because an ordinary NotReady pod looks like a slow start and this one needs a human. MQTTD_REFOUND_GUARD=false (env, reachable mid-incident without a config roll) for a deliberate re-bootstrap. Verified on kind: 'the re-founded pod-0 self-quarantined (Ready=False), so it serves no clients'. CONTAINS, does not prevent — the node still re-founds and still needs the documented wipe-and-rejoin; prevention is 0055-T9."
---

# 0054 — Operator-facing state surface: delivery

**Decision:** [ADR 0054](../adr/0054-operator-facing-state-surface.md). One-line
story: the operator program (ADR 0047 amendment, triggers engaged 2026-08-04) needs
state to act on; this lands the signals first — split-brain detectability, brownout
as a condition, drains visible to scrape — as one structured `/statusz` plus bounded
gauges, useful to humans and alert rules before any controller exists.

<!-- status-table:0054 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0054-T1 | ✅ done | 2026-08-05 | "PR #79. /statusz always-200 on the health listener; unbounded detail body-only (member lists, voter ids), bounded gauges Prometheus+OTLP; statusz shape/brownout-transition/JSON-escape tests, store-watch snapshot test; mqttd lib 150 green, cardinality guard green" |
| 0054-T2 | ✅ done | 2026-08-05 | "PR #80. End-to-end over real sockets: joiner adopts the founder's id; a separately-founded cluster's gossip counted cluster-mismatch and its node never enters the membership view; identity unit tests (mint-once/reload-stable, adopt-once/persist, distinct foundings); swim wire field appended (pre-1.0 reshape, disclosed), fuzz seed regenerated" |
| 0054-T3 | ✅ done | 2026-08-05 | "SwimAuth::key_fingerprints (sha256/8B hex per accepted key, primary first) + swim_keys_accepted gauge; reload::ConfigStamp (checksum of the config file bytes + applied-generation counter) recorded at startup and on every successful reload, mirrored to config_info{checksum} with previous-series zeroing; peer_proto_min/max gauges; statusz keys/config blocks with tests (fingerprint-not-material unit test; statusz shape); OPERATIONS.md rotation-verification steps" |
| 0054-T4 | ✅ done | 2026-08-05 | "Demo dashboard gains an 'Operator signals (ADR 0054)' row: cluster-identity table (two values = split brain), foundings + cluster-mismatch rates, brownout state, store utilization vs watermark, decommission state/pending, replication lag + voters, rotation windows + config convergence, and the previously never-dashboarded ADR 0041 rejection families. OPERATIONS.md 'Monitoring for the operator' catalogues ten alert rules with actions; ADR 0047 amendment notes the operator path engaged with signals-first sequencing" |
| 0054-T5 | ✅ done | 2026-08-06 | "Shipped in be96989 (#98), recorded 2026-08-07 during a delivery-record audit — the behaviour reached main with no task and no evidence, the mirror image of a title claiming work never done. T2 made a second, separately-founded cluster DETECTABLE and contained it at the gossip layer; that scope proved too narrow. The divergent node's own MQTT listener was untouched: a pod-0 whose volume is lost mints a new identity, its single-voter lease group elects itself, members=1 satisfies the founder's ready_min_members=1, so /readyz returns 200 and it joins the client Service endpoints — serving a share of connections from an EMPTY session and retained store beside the real cluster. OPERATIONS.md half-knew this ('the founder floor is 1 — act promptly'). A node now refuses readiness when, with the guard armed (cluster-configured, cluster.refound_guard default true), it has observed cluster-mismatch gossip AND its membership view holds only itself. Those two identify the divergent node and only it: it rejects every foreign datagram so it never learns a peer, while the healthy majority sees the same gossip but has each other. A genuine first bootstrap is alone too, but nothing is alive to send it foreign gossip, so the rule cannot fire there — load-bearing, because OrderedReady means pod-0 must come Ready alone or no other pod is created. NOTE the first design keyed on ClusterIdentity::minted and the kind e2e FALSIFIED it: minted is per-PROCESS while the divergence lives on disk, so the re-founder mints only in its first life and the operator's fence — by deleting the pod — is precisely what ends it; the guard was inert in the case it was built for. Membership survives a restart, provenance does not. Both conjuncts verified load-bearing by removing each in turn. Evaluated live, not latched, so a node that legitimately rejoins serves again unattended. Visible not just a 503: /statusz quarantine block + mqttd_refound_quarantine gauge + an OPERATIONS.md alert row, because an ordinary NotReady pod looks like a slow start and this one needs a human. MQTTD_REFOUND_GUARD=false (env, reachable mid-incident without a config roll) for a deliberate re-bootstrap. Verified on kind: 'the re-founded pod-0 self-quarantined (Ready=False), so it serves no clients'. CONTAINS, does not prevent — the node still re-founds and still needs the documented wipe-and-rejoin; prevention is 0055-T9." |
<!-- /status-table:0054 -->

## Notes

- 2026-08-05 — Inventory that motivated this (session record): no cluster identity
  anywhere (split-brain undetectable, `can_bootstrap` the sole guard); brownout state
  never exported (an idle browned-out broker is silent); decommission `active` flag
  computed but never surfaced; membership counts-only. The metrics/body split rule
  follows ADR 0020's cardinality discipline: node-naming detail is body-only.
