---
adr: "0087"
title: "A QoS ≥ 1 publish is acknowledged only when every node that owns a subscriber has accepted it: no path around the acknowledged forward"
adr_status: Proposed
tasks:
  - id: 0087-T1
    title: "Characterise and design: what is acknowledged end to end today, what a false Dead verdict loses per subscriber kind, the failure-detection measurements, and this record"
    status: done
    issue: 979
    date: 2026-10-13
    evidence: "crates/mqttd/tests/false_dead_delivery.rs (six episodes over three brokers wired as main wires them, plain and durable, both link orientations, the evictor leading the lease group or not; every lossy row asserted exactly, so each fails when the behaviour changes in either direction), two unit tests in hub/forwarding.rs for the two faces of the gap at the hub, the false_dead_report measurement and two regression tests in swim_formation_sim.rs, two SWIM Config knobs that are off by default (confirm_before_dead, suspicion_scales_with_log_n) with their unit tests. No change to the delivery path."
  - id: 0087-T2
    title: "D1, D4, D6: a peer under a Dead verdict stays in the routing tables as unreachable — interest, $share membership (for a link-down peer too), the forward window; obligations in flight are kept; $share prefers a reachable member and holds when there is none"
    status: done
    issue: 980
    date: 2026-10-13
    evidence: "hub/mod.rs: peer_dead drops the link and erases nothing; the peer is held as unreachable (VerdictHold) with its interest, $share members, negotiated protocol, forward window and obligations, and peer_disconnected keeps $share membership. A publish that matches is owed to it and held; what it is owed goes out when its first interest snapshot arrives on a new link. Until T3 the hold is bounded at 8 sweep ticks with no link (REROUTE_GRACE_TICKS, what a verdict already cost a forward in flight), then erase_peer resolves as peer_dead did; a successor that announces the interest still discharges the obligation (reroute; T5 narrows it to durable interest). hub/delivery.rs: the last fallback of $share selection prefers a member on a linked node. hub/forwarding.rs: peer_proto answers with the last negotiated protocol for a peer with no link, so a $share delivery to it is an obligation. Gauge mqttd_publish_held_for_unreachable. Flips the two unit tests (now a_publish_for_a_peer_declared_dead_is_owed_to_it_as_it_is_to_a_link_down_peer and a_forward_unanswered_when_its_target_is_declared_dead_stays_owed_until_the_peer_answers), every $share row of the episodes, and adds a link-cut episode with no verdict; a forward retransmitted across a verdict is applied once at QoS 1 and 2 (forward_repeat.rs); the pending table's bounds under a hold are tested."
  - id: 0087-T2b
    title: "$share obligations follow the member, and order holds across a heal: a $share delivery is acknowledged only when a current member took it; a hold that runs out discharges nothing a kept member is owed; what was held for a peer goes out ahead of what is published after its link returns"
    status: done
    issue: 983
    date: 2026-10-13
    evidence: "The gaps an independent read-through of T2 left on #973 (items 1, 2, 3, 7 and the narrowing of 4). hub/forwarding.rs: register_forward sends nothing to a linked peer that has not announced on its link (interest for an ordinary forward, $share membership for a shared delivery), send_owed_to flushes one kind in publish order, the sweep sends a peer silent for a sweep interval its ordinary forwards oldest first (never a $share delivery), a publish first owed at the heal is re-routed at the announcement and flushed in its place, discharge_unreachable only with a durable plane, each obligation records whether it was ever written to a link. hub/mod.rs: peer_announced_interest / peer_announced_shared (the first $share snapshot on a link re-plans what is held), move_shared_from_unreachable for a lost link as for a verdict, erase_peer places a $share delivery with a member that is left. hub/delivery.rs: reselect_shared(Reselect::MemberGone) is a first placement; the receiver answers a $share delivery for a member that is not there — or whose connection has already ended — with Refused code 3 (brownout's code below proto 13). The snapshot commands carry the link they arrived on; one from a replaced link is dropped. No frame, no protocol bump. Tests: hub/tests/share_obligations.rs (19), forwarding.rs (the two discharge characterisations made durable and made to read the receiver, plus the plain-mode one), false_dead_delivery.rs (two numbered-stream episodes: order, and every acknowledgement under a link cut held until the heal)."
  - id: 0087-T3
    title: "D2: confirmation that a peer is gone, as a membership event (tombstone pruned unanswered, or a new generation), and the resolution of what it was still owed, per subscriber kind"
    status: planned
    issue: 973
    notes: "Size M (4-6 days). swim.rs (an action at the prune and at a generation change), swim_driver.rs, cluster.rs, a hub command; the resolution table of D2. Needs O1 and O4 settled. After T2: without it a held publish for a peer that really died is held until the pending bound."
  - id: 0087-T4
    title: "D5: the link manager keeps dialing a peer under a verdict until it is confirmed gone; a link is admitted by its handshake, not by membership, as a stated and tested rule"
    status: done
    issue: 981
    date: 2026-10-13
    evidence: "cluster.rs: the Dead arm no longer aborts the dialer; it tells it the peer is held Dead. peer.rs dial_member: under a verdict the redial delay doubles from 500 ms to 5 s and an attempt is bounded at each step, verdict or not (5 s to connect, and a connect under way when membership speaks is dropped for a fresh one; 10 s for TLS and 10 s for the Hello exchange, for every dialer), so a peer that is really gone costs at most 15 connection attempts over the minute it is dialed, then none; a link served under the verdict is served like any other; and the dialer stops dialing when the peer has been Dead with no link for held_dead_dial_window (twice dead_ttl: 60 s) — its task waits, and the next Alive sets it going again — or is replaced when Alive names a new routing address. Confirmation as a membership event is T3's and replaces that window. The link manager withdraws a verdict on the first Alive after a Dead whatever came between (SWIM can put a Suspect there), and for an Alive that carries no routing address too. A membership-driven dialer links only the node it was aimed at, and stays away for 60 s from an address where another node answered (each attempt there flaps that node's own link to this one). The hub remembers the verdict until the link manager withdraws it (HubCommand::PeerAlive), so the interest a link brought back under a verdict goes with that link — a node that is redialed while it drains and then exits leaves nothing behind. The two orientations of every episode in false_dead_delivery.rs now assert the same rows; what stays lost until T2 is the first publish for a $share group with no member elsewhere. peer_identity.rs: a node dials, accepts and serves a peer it holds Dead over mTLS, and still refuses a certificate that does not attest the claimed node id."
  - id: 0087-T5
    title: "D8: interest that says whether the lease plane owns it (peer protocol 15, additive, per-link), so a durable session's obligation is discharged by its committed owner's verdict and a clean session's only by its node"
    status: planned
    issue: 973
    notes: "Size L (1-2 weeks). mqtt-cluster/src/peer.rs (the frame, PROTO_MAX 15, the pinned-encoding tests), hub interest index, forwarding. Needs O2 and O5 settled. Without it T2+T3 make every durable failover a dead_ttl pause for publishers, so it ships with them or before them; cluster_upgrade.rs is its oracle."
  - id: 0087-T6
    title: "D3: bounds and the publisher's experience — the per-peer held count, the behaviour at the pending-table bound (O3), the stall of a connection whose head acknowledgement is held, documented where the guarantee is stated"
    status: planned
    issue: 973
    notes: "Size S-M (2-3 days). hub/forwarding.rs register_pending, conn.rs only if O3 chooses the v5 reason code; CLIENT-GUIDE, SIZING."
  - id: 0087-T7
    title: "D9: observability — held-for-unreachable gauge per peer, resolved-undeliverable and peer-unreachable counters, the log lines, the alert, the admin placement column; operator documentation"
    status: planned
    issue: 973
    notes: "Size M (3-4 days). mqtt-observability, hub, admin API and CLI, the chart's PrometheusRule (re-vendor the mqttui bundle), OPERATIONS, TROUBLESHOOTING, GUIDE. Also settles the no-subscriber reason that is documented and never emitted."
  - id: 0087-T8
    title: "D11: the tests — flip the characterisation rows, an episode with the peer really dead, false-Dead and partition injections with clean-session and $share subscribers in cluster_stress's ledger, a SIGSTOP nemesis in cluster_proc"
    status: planned
    issue: 973
    notes: "Size L (1-2 weeks), alongside T2-T5 rather than after them: each of those lands with its rows flipped. The stress and out-of-process injections are what makes the claim more than six scripted episodes."
  - id: 0087-T9
    title: "D10 and the rig: re-measure the hot path (clean QoS 1 at ten nodes, the durable rung, the $share QoS 1 lane, the dispatch benchmark) and run a membership fault under load with the ledger; then the README's claim"
    status: planned
    issue: 973
    notes: "Size M, plus rig time. Last: it certifies the rest. Until it has run, the README's 'none lost' carries the condition it was measured under (O8)."
  - id: 0087-T10
    title: "Failure detection: one last direct probe of a suspect before it is declared Dead, on by default"
    status: done
    issue: 999
    date: 2026-10-13
    evidence: "swim.rs: Config::confirm_before_dead defaults to true; not operator configuration (none of the detector's settings is). The formation simulation gains packet loss, one-way reachability, one link lost one way and a late member, and a second report (fault_shapes_report); the characterisation test is replaced by the bound the default keeps (a cut of up to 2 s ends in no Dead at 5, 14 and 30 nodes), with the old behaviour pinned as what the switch off gives. A driver test that only a datagram signed by the suspect answers the probe. Twelve real brokers with one stopped for 2 s (nightly tier). ADR 0016's amendment of 2026-10-13 carries the tables."
  - id: 0087-T10b
    title: "Failure detection: the same check before adopting a relayed Dead about a member this node can reach; decide whether the suspicion window scales with the cluster's size"
    status: planned
    issue: 975
    notes: "Size M. What T10 leaves of #975: the 3 s cells and the mixed-version rows (a Dead declared by another node, or by a v1.1.0 node, is adopted unchecked) and the packet-loss cells at 14 nodes and more (the window does not grow with the cluster). suspicion_scales_with_log_n exists, off. ADR 0016 gets the amendment."
---

# Delivery 0087 — acknowledged only when the owning node has accepted it

**ADR:** [docs/adr/0087-delivery-across-a-false-dead-verdict.md](../adr/0087-delivery-across-a-false-dead-verdict.md)

The plan, progress, and changelog for ADR 0087. Task status lives in the frontmatter
above; the table below is generated from it.

The direction was decided on 2026-10-13 (issue #973): a publisher's QoS ≥ 1
acknowledgement is given only after the node that owns the subscriber has accepted the
message, with no path that bypasses it, and correctness comes before a release date. The
ADR is still Proposed because its sub-decisions O1–O8 are open.

**The minimum complete fix is T2, T3, T4, T5, T6, T7 and T8**, certified by T9. That is
not what fits a date; it is what it takes for the rule to hold without an exception and
without making durable failover slower: T2 is the rule, T3 is what ends a hold when the
peer really is gone, T4 makes a false verdict heal in a second in either link
orientation, T5 keeps a real owner death at today's failover pause, T6 and T7 make the
hold bounded and visible, and T8 is the evidence. In working days for one person, by the
sizes above: about five to seven weeks, then the rig. T10 and T10b are separate,
smaller, and reduce how often any of it is exercised.

Tasks T2–T9 are tracked under #973 until the ADR is accepted; each gets its own issue
then, once its sub-decision is settled (T3: O1, O4; T5: O2, O5; T6: O3).

## Plan

| Task | Acceptance |
|---|---|
| **0087-T1** Characterise and design | The tests, measurements and ADR are merged; the issue carries the table. |
| **0087-T2** Unreachable, not erased (D1, D4, D6) | `a_publish_for_a_peer_declared_dead_owes_it_nothing_where_a_link_down_peer_is_owed` and `a_forward_unanswered_when_its_target_is_declared_dead_is_acked_when_the_grace_runs_out` are flipped: a publish for a peer under a verdict is an obligation, and one in flight stays one. In the episodes no row is `AckedAndLost` or `HeldThenLost` while the verdict stands, `$share` rows included; a QoS 2 forward retransmitted across a verdict is delivered once. |
| **0087-T2b** `$share` obligations follow the member, and order holds across a heal | A `$share` QoS ≥ 1 delivery is acknowledged to its publisher only when a current member took it, or its group has nobody: a peer answers one for a member that is not there with a refusal the origin re-selects on; a delivery held across an outage is not sent before the peer's membership snapshot and is placed again if the member is not in it; one made while a node is out of reach follows a consumer that reconnected through another node; a hold that runs out places it with a member on another kept node ("b expires, c returns"). A numbered sequence published across a link cut and across a false verdict reaches every subscriber — ordinary and `$share`, clean and persistent, QoS 1 and QoS 2, both directions — strictly increasing and complete, and every publish made with the link cut is acknowledged only after it is restored. Without a durable plane a newly matching interest discharges nothing an unreachable peer is owed. |
| **0087-T3** Confirmed gone (D2) | An episode with B really dead: a publish for its clean-session subscriber is acknowledged at confirmation and counted, not before and not silently; a durable session's is stored by the new owner within the failover pause; a new generation of B confirms the old one gone. A hold never outlives `dead_ttl` plus a sweep tick for non-durable interest. |
| **0087-T4** Keep dialing (D5) | The "A dials" and "B dials" columns of the episodes are the same. A node accepts and serves a link from a peer its membership holds `Dead`. The dialer for a confirmed-gone peer is stopped. |
| **0087-T5** Interest kinds (D8) | With every link at proto 15, an owner's death holds a publisher for the failover pause and no longer; a false verdict at the lease leader loses nothing for either kind. `cluster_upgrade.rs` holds in both directions of the roll; a proto-14 link is treated as O5 decides. |
| **0087-T6** Bounds (D3) | The pending table's behaviour at its bound under a hold is tested and documented; the per-connection stall is stated in the client guide; O3 is implemented as decided. |
| **0087-T7** Observability (D9) | An operator can see, from metrics alone, that publishes are held for a peer and how each hold ended; the alert fires in the chart's test; OPERATIONS has the runbook. |
| **0087-T8** Tests (D11) | `cluster_stress` holds acked durability for clean-session, persistent and `$share` subscribers across seeded false verdicts and partitions; `cluster_proc`'s `SIGSTOP` nemesis passes the acked-facts oracle. |
| **0087-T9** Rig | A ledgered run with a membership fault under load loses nothing; the re-measured rungs are published with the README. |
| **0087-T10** Failure detection: the last direct probe, on by default (#999) | `a_short_cut_still_ends_in_a_false_dead_on_the_default_window` is replaced by the bound the new default keeps; no formation regression. |
| **0087-T10b** Failure detection: a relayed `Dead`, and the window's scaling (#975) | A `Dead` relayed about a member the node can reach is checked before it is adopted, or the decision not to is recorded with its measurement; the 3 s cells and the mixed-version rows of ADR 0016's table say what changed. The window's scaling is decided, with the packet-loss cells at 14 and 30 nodes as its measure. |

## Progress

<!-- status-table:0087 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0087-T1 | ✅ done | [#979](https://github.com/mbilling/fss-mqtt-broker/issues/979) | 2026-10-13 | "crates/mqttd/tests/false_dead_delivery.rs (six episodes over three brokers wired as main wires them, plain and durable, both link orientations, the evictor leading the lease group or not; every lossy row asserted exactly, so each fails when the behaviour changes in either direction), two unit tests in hub/forwarding.rs for the two faces of the gap at the hub, the false_dead_report measurement and two regression tests in swim_formation_sim.rs, two SWIM Config knobs that are off by default (confirm_before_dead, suspicion_scales_with_log_n) with their unit tests. No change to the delivery path." |
| 0087-T2 | ✅ done | [#980](https://github.com/mbilling/fss-mqtt-broker/issues/980) | 2026-10-13 | "hub/mod.rs: peer_dead drops the link and erases nothing; the peer is held as unreachable (VerdictHold) with its interest, $share members, negotiated protocol, forward window and obligations, and peer_disconnected keeps $share membership. A publish that matches is owed to it and held; what it is owed goes out when its first interest snapshot arrives on a new link. Until T3 the hold is bounded at 8 sweep ticks with no link (REROUTE_GRACE_TICKS, what a verdict already cost a forward in flight), then erase_peer resolves as peer_dead did; a successor that announces the interest still discharges the obligation (reroute; T5 narrows it to durable interest). hub/delivery.rs: the last fallback of $share selection prefers a member on a linked node. hub/forwarding.rs: peer_proto answers with the last negotiated protocol for a peer with no link, so a $share delivery to it is an obligation. Gauge mqttd_publish_held_for_unreachable. Flips the two unit tests (now a_publish_for_a_peer_declared_dead_is_owed_to_it_as_it_is_to_a_link_down_peer and a_forward_unanswered_when_its_target_is_declared_dead_stays_owed_until_the_peer_answers), every $share row of the episodes, and adds a link-cut episode with no verdict; a forward retransmitted across a verdict is applied once at QoS 1 and 2 (forward_repeat.rs); the pending table's bounds under a hold are tested." |
| 0087-T2b | ✅ done | [#983](https://github.com/mbilling/fss-mqtt-broker/issues/983) | 2026-10-13 | "The gaps an independent read-through of T2 left on #973 (items 1, 2, 3, 7 and the narrowing of 4). hub/forwarding.rs: register_forward sends nothing to a linked peer that has not announced on its link (interest for an ordinary forward, $share membership for a shared delivery), send_owed_to flushes one kind in publish order, the sweep sends a peer silent for a sweep interval its ordinary forwards oldest first (never a $share delivery), a publish first owed at the heal is re-routed at the announcement and flushed in its place, discharge_unreachable only with a durable plane, each obligation records whether it was ever written to a link. hub/mod.rs: peer_announced_interest / peer_announced_shared (the first $share snapshot on a link re-plans what is held), move_shared_from_unreachable for a lost link as for a verdict, erase_peer places a $share delivery with a member that is left. hub/delivery.rs: reselect_shared(Reselect::MemberGone) is a first placement; the receiver answers a $share delivery for a member that is not there — or whose connection has already ended — with Refused code 3 (brownout's code below proto 13). The snapshot commands carry the link they arrived on; one from a replaced link is dropped. No frame, no protocol bump. Tests: hub/tests/share_obligations.rs (19), forwarding.rs (the two discharge characterisations made durable and made to read the receiver, plus the plain-mode one), false_dead_delivery.rs (two numbered-stream episodes: order, and every acknowledgement under a link cut held until the heal)." |
| 0087-T3 | ⬜ planned | [#973](https://github.com/mbilling/fss-mqtt-broker/issues/973) | — | "Size M (4-6 days). swim.rs (an action at the prune and at a generation change), swim_driver.rs, cluster.rs, a hub command; the resolution table of D2. Needs O1 and O4 settled. After T2: without it a held publish for a peer that really died is held until the pending bound." |
| 0087-T4 | ✅ done | [#981](https://github.com/mbilling/fss-mqtt-broker/issues/981) | 2026-10-13 | "cluster.rs: the Dead arm no longer aborts the dialer; it tells it the peer is held Dead. peer.rs dial_member: under a verdict the redial delay doubles from 500 ms to 5 s and an attempt is bounded at each step, verdict or not (5 s to connect, and a connect under way when membership speaks is dropped for a fresh one; 10 s for TLS and 10 s for the Hello exchange, for every dialer), so a peer that is really gone costs at most 15 connection attempts over the minute it is dialed, then none; a link served under the verdict is served like any other; and the dialer stops dialing when the peer has been Dead with no link for held_dead_dial_window (twice dead_ttl: 60 s) — its task waits, and the next Alive sets it going again — or is replaced when Alive names a new routing address. Confirmation as a membership event is T3's and replaces that window. The link manager withdraws a verdict on the first Alive after a Dead whatever came between (SWIM can put a Suspect there), and for an Alive that carries no routing address too. A membership-driven dialer links only the node it was aimed at, and stays away for 60 s from an address where another node answered (each attempt there flaps that node's own link to this one). The hub remembers the verdict until the link manager withdraws it (HubCommand::PeerAlive), so the interest a link brought back under a verdict goes with that link — a node that is redialed while it drains and then exits leaves nothing behind. The two orientations of every episode in false_dead_delivery.rs now assert the same rows; what stays lost until T2 is the first publish for a $share group with no member elsewhere. peer_identity.rs: a node dials, accepts and serves a peer it holds Dead over mTLS, and still refuses a certificate that does not attest the claimed node id." |
| 0087-T5 | ⬜ planned | [#973](https://github.com/mbilling/fss-mqtt-broker/issues/973) | — | "Size L (1-2 weeks). mqtt-cluster/src/peer.rs (the frame, PROTO_MAX 15, the pinned-encoding tests), hub interest index, forwarding. Needs O2 and O5 settled. Without it T2+T3 make every durable failover a dead_ttl pause for publishers, so it ships with them or before them; cluster_upgrade.rs is its oracle." |
| 0087-T6 | ⬜ planned | [#973](https://github.com/mbilling/fss-mqtt-broker/issues/973) | — | "Size S-M (2-3 days). hub/forwarding.rs register_pending, conn.rs only if O3 chooses the v5 reason code; CLIENT-GUIDE, SIZING." |
| 0087-T7 | ⬜ planned | [#973](https://github.com/mbilling/fss-mqtt-broker/issues/973) | — | "Size M (3-4 days). mqtt-observability, hub, admin API and CLI, the chart's PrometheusRule (re-vendor the mqttui bundle), OPERATIONS, TROUBLESHOOTING, GUIDE. Also settles the no-subscriber reason that is documented and never emitted." |
| 0087-T8 | ⬜ planned | [#973](https://github.com/mbilling/fss-mqtt-broker/issues/973) | — | "Size L (1-2 weeks), alongside T2-T5 rather than after them: each of those lands with its rows flipped. The stress and out-of-process injections are what makes the claim more than six scripted episodes." |
| 0087-T9 | ⬜ planned | [#973](https://github.com/mbilling/fss-mqtt-broker/issues/973) | — | "Size M, plus rig time. Last: it certifies the rest. Until it has run, the README's 'none lost' carries the condition it was measured under (O8)." |
| 0087-T10 | ✅ done | [#999](https://github.com/mbilling/fss-mqtt-broker/issues/999) | 2026-10-13 | "swim.rs: Config::confirm_before_dead defaults to true; not operator configuration (none of the detector's settings is). The formation simulation gains packet loss, one-way reachability, one link lost one way and a late member, and a second report (fault_shapes_report); the characterisation test is replaced by the bound the default keeps (a cut of up to 2 s ends in no Dead at 5, 14 and 30 nodes), with the old behaviour pinned as what the switch off gives. A driver test that only a datagram signed by the suspect answers the probe. Twelve real brokers with one stopped for 2 s (nightly tier). ADR 0016's amendment of 2026-10-13 carries the tables." |
| 0087-T10b | ⬜ planned | [#975](https://github.com/mbilling/fss-mqtt-broker/issues/975) | — | "Size M. What T10 leaves of #975: the 3 s cells and the mixed-version rows (a Dead declared by another node, or by a v1.1.0 node, is adopted unchecked) and the packet-loss cells at 14 nodes and more (the window does not grow with the cluster). suspicion_scales_with_log_n exists, off. ADR 0016 gets the amendment." |
<!-- /status-table:0087 -->

## Changelog

- 2026-10-13 — Issue #973 characterised. Three brokers wired as production, one told that
  a live peer is `Dead`: in the link orientation where the node that is wrong dials,
  every QoS 1 and QoS 2 publish for a subscriber only that peer has is acknowledged and
  lost — clean session, persistent session connected or offline, `$share` with no member
  elsewhere, a rule's republish — in plain and in durable mode; no drop counter moves. In
  the other orientation the link is back in a second and only a `$share` group without
  another member loses its first publish. A `$share` group on a peer that merely lost
  its link loses publishes too. A false verdict at the lease leader also moves the
  session's lease. The acknowledged-forward path was found sound: the gap is that
  `PeerDead` erases the interest the path is fed from, and discards obligations in
  flight.
- 2026-10-13 — Failure detection measured in the formation simulation: a member cut off
  for 1.5 s is declared dead in 16 of 32 seeds at 14 nodes and 25 of 32 at 30; with one
  last direct probe before `Dead`, in none at any size (#975). The probe is in the tree
  as `Config::confirm_before_dead`, off.
- 2026-10-13 — Direction decided by the maintainer: acknowledge only after the owning
  node has accepted, no bypass. The ADR was rewritten from options to that design; its
  sub-decisions O1–O8 are open.
- 2026-10-13 — T4 (D5) built. A node keeps dialing a peer it holds `Dead`, so a false
  verdict costs the data path one redial — the first publish acknowledged after 0.8 s
  and delivered — whichever of the pair owns the link; before, the node that was wrong
  stopped its own dialer and every publish for that peer's subscribers was acknowledged
  and lost for as long as the verdict stood. The episodes' two orientations now assert
  the same rows. Still lost until T2: the first publish for a `$share` group whose only
  member is on the peer, in both directions. Confirmation that a peer is gone is not a
  membership event yet (T3), so the dialer bounds itself: twice `dead_ttl` (60 s) with no
  link, at a delay that doubles to 5 s. No wire change.
- 2026-10-13 — T4, after an independent review of the first cut. Four defects fixed
  before merge: the verdict was withdrawn at the hub only when `Dead` was the event just
  before `Alive`, and SWIM can put a `Suspect` between them (the hub then kept the
  verdict for good and erased the member's routing state at every later link flap); a
  dialer that outlives a verdict could wait for ever on a peer that accepted the
  connection and went silent, since the handshake had no deadline (10 s now, for TLS and
  for the `Hello` exchange, for every dialer); a dialer that ended itself when its
  window was spent could miss the `Alive` that arrived in that instant (it now waits
  instead of ending); and an `Alive` without a routing address did not reach the dialer.
  Two more from the automated review: the connect already under way when a verdict
  landed had no bound and could not be interrupted (5 s now for every membership-driven
  attempt, and a change of standing starts a fresh one); and a dialer at a stale address
  flapped the answering node's own link to this node on every attempt (it now stays away
  for 60 s, or until the member's standing changes).
- 2026-10-13 — T2 (D1, D4, D6) built. A `Dead` verdict erases nothing: the peer is held as
  unreachable and a publish for its subscribers is owed to it and held, as for a peer
  whose link is down; `$share` membership survives a lost link and a verdict. With T4 no
  row of the episodes is acknowledged-and-lost any more: every flow's first publish is
  acknowledged 0.50 s after the verdict and delivered, `$share` included, and a link cut
  with no verdict holds and then delivers where it lost the `$share` flows. Until T3 the
  hold is bounded at 8 sweep ticks with no link, the time a verdict already cost a
  forward in flight, and then resolved as before — so a node that really died stalls
  nobody longer, and a partition that outlasts the hold is still acknowledged for,
  undelivered. Found on the way: a `$share` delivery to a member with no link was never
  an obligation (the frame was chosen as for the oldest protocol and dropped). No wire
  change.
- 2026-10-13 — T2b built, from an independent read-through of T2 (the gaps are listed on
  #973). **`$share`:** a delivery is acknowledged to its publisher only when a current
  member took it. A peer asked to deliver to a member that is not on it any more
  answers with a refusal instead of `Stored`; the origin does not send a held delivery
  before the peer's membership snapshot has arrived on the new link, and places it
  again if the member is not in it; a delivery recorded while a node was out of reach
  follows a consumer that reconnected through another node, after a lost link as after
  a verdict; and a hold that runs out no longer discharges a delivery whose group has
  a member on another node that is only out of reach ("b expires, c returns") — it is
  placed with that member and held. **Order:** a forward is no longer sent to a linked
  peer before the peer has announced on that link, and everything owed goes out oldest
  first when it does, so what was held across an outage is not overtaken by what is
  published in the instant after the link returns. Seven numbered streams — ordinary
  and `$share`, clean and persistent, QoS 1 and QoS 2, both directions — published
  across a link cut and across a false verdict arrive strictly increasing and
  complete; without the gate the link-cut episode showed `…, 10, 111, 11, 12, …` in two
  runs of three. **Plain mode:** a newly matching interest no longer discharges what
  an unreachable peer is owed (no session moves without a durable plane); the
  durable-mode gap stays with T5. **The drain:** a drain-gated `$share` delivery with
  nobody to hand it to is counted (`no-shared-member`), and one behind a lost link no
  longer holds the drain. No frame and no protocol bump: one refusal code (3) on the
  existing verdict, said only on links that negotiated proto 13 or more; a v1.1.0
  origin (proto 12) is told the refusal it already re-selects on.
- 2026-10-13 — T2b, after a second independent read of the pull request. Fixed before
  merge: a `$share` delivery for a clean-session member whose connection had ended but
  whose `Detach` was still queued was answered `Stored` (the hub still listed it as
  online; under backlog this is the likelier way for a consumer to be gone); a publish
  the origin first owed a peer AT the heal — its subscriber subscribed during the
  outage, or its interest had been erased with its hold — was re-routed by the next
  settle pass, up to a second behind newer publishes, and is now re-routed at the
  peer's announcement and flushed in its place; the sweep's send to a silent peer
  fired at the first sweep after the link formed, since everything held was already
  overdue, and now waits for the link to be a sweep interval old — and no longer
  sends `$share` deliveries at all before the membership snapshot; and a snapshot left
  over from a replaced link could be taken for the first on the new one (the snapshot
  commands now carry their link). The ordering episodes were made deterministic: the
  test relay holds what a node says after its `Hello` for 100 ms, and with the gate
  taken out both episodes then fail in 5 runs of 5.
- 2026-10-13 — T10 built: the last direct probe before `Dead` is the default (decided
  by the maintainer the same day). Re-measured on the final tree, 32 seeds per cell: a
  member cut off for up to 2 s is declared dead in no run at 3 to 30 nodes (without the
  probe: 16 of 32 at 14 nodes and 1.5 s, 28 of 32 at 30 nodes and 2 s); a real death is
  held `Dead` by every survivor about 0.3 s later in the median (5.6 s against 5.3 s at
  14 nodes), 0.6 s at worst. Formation is untouched. New in the simulation: packet loss
  (fewer false verdicts in every cell, not none: 10 of 32 runs at 30 nodes and 10 %
  loss, against 28), a member reachable one way only (declared dead as before — the
  probe needs a round trip), one link lost one way (no suspicion, as before), a member
  whose datagrams are late. Twelve real brokers, one stopped for 2 s 68 times:
  declared dead in none. No operator switch; no wire change. The task is narrowed to
  what was built and the rest of #975 — a relayed `Dead`, the window's scaling — is
  T10b.
