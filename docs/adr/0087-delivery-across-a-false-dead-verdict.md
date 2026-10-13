# ADR 0087 — A QoS ≥ 1 publish is acknowledged only when every node that owns a subscriber has accepted it: no path around the acknowledged forward

- **Status:** Proposed
- **Date:** 2026-10-13
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0087-delivery-across-a-false-dead-verdict.md](../delivery/0087-delivery-across-a-false-dead-verdict.md) — plan, progress, and changelog
- **Issues:** [#973](https://github.com/mbilling/fss-mqtt-broker/issues/973) (the loss),
  [#975](https://github.com/mbilling/fss-mqtt-broker/issues/975) (the false verdict a short
  outage still ends in), [#971](https://github.com/mbilling/fss-mqtt-broker/issues/971)
  (closed: formation, and re-admission after a false verdict)
- **Related:** [ADR 0016](0016-swim-membership-stability.md) (membership stability, and its
  2026-10-12 amendment), [ADR 0042](0042-durable-plane-stress-harness.md) (T9: "acked means
  durable, cluster-wide" — where the acknowledged forward comes from),
  [ADR 0005](0005-session-affinity.md), [ADR 0015](0015-cluster-shared-subscriptions.md),
  [ADR 0014](0014-cross-node-retained.md) / [ADR 0037](0037-durable-retained-messages.md),
  [ADR 0083](0083-rule-engine.md), [ADR 0082](0082-bounded-hub-ingress.md),
  [ADR 0075](0075-pipelined-durable-appends.md), [ADR 0057](0057-durable-outbound-inflight.md)

> **Direction decided 2026-10-13** (the maintainer, on #973): *the publisher's QoS ≥ 1
> acknowledgement is given only after the node that owns the subscriber has accepted the
> message, with no path that bypasses it — take the time and do it right.* The record stays
> **Proposed** because the sub-decisions in §6 are open; it is accepted when they are
> settled. Nothing in the delivery path is changed by the change that adds this record: it
> brings the tests that pin today's behaviour, the measurements, and this design.

## Context

Node A holds node B `Dead`. B is alive, its clients are connected, and the network between
the two works: the verdict is false. After #971's fix such a verdict ends within the
tombstone's 30 s and a formation no longer produces one; an outage of one member shorter
than the suspicion window still does, at ten nodes and above (§7, #975).

While the verdict stands, a QoS 1 publish that arrives at A for a subscriber on B is
acknowledged with a normal PUBACK and delivered to nobody (#973). The README says "733
million messages ledgered, none lost"; this is a way to lose one that no ledger of ours
looks at, because none of our faults is a wrong membership verdict.

Two cases are kept apart throughout:

- **B is unreachable from A** — a real partition. A cannot deliver whatever it does; the
  question is what it tells the publisher, and when.
- **A wrongly believes B dead while the path works** — the false verdict. A had a way to
  deliver and discarded the message.

## 1. What exists today

The peer protocol already has an end-to-end acknowledged forward (ADR 0042 T9 exhibit ⑤;
0041-T11/T12; #648; #738; #784). For a publish with an acknowledgement gate, the hub
registers a pending entry (`register_pending`), turns every peer whose announced **interest**
matches into a `ForwardObligation` (`forward_to_peers` → `register_forward`), sends it under
a sequence number, retransmits it under the same number every sweep tick while it is
overdue and the link is up (`sweep_pending_forwards`), and releases the publisher's answer
only when no obligation is outstanding (`try_complete_pending`). The receiver recognises a
repeat by `(origin node, seq)` plus a content fingerprint in a 16,384-entry window per
origin (`forward_is_repeat`), applies a forward once, and answers when its own durable
appends have landed (`finish_peer_verdict` → `answer_forward`). An obligation toward a peer
whose **link is merely down** is recorded and held, and sent when the link returns.

**Which forwards take that path, and which do not:**

| Message | Frame | Answered? | The publisher's acknowledgement waits? |
|---|---|---|---|
| Client publish, QoS 1 or 2, to ordinary subscribers on a peer — **clean sessions included** (the path is chosen by the publish, not by the session) | `PublishAckedTagged` (proto ≥ 12: origin id, re-route flag), `PublishAcked` below | `PublishVerdict { Stored \| Reached \| Refused \| Failed }` (proto ≥ 7; `Reached` proto ≥ 11), `PublishAck { ok }` at proto 6 | **Yes** |
| The same to a `$share` member on a peer | `SharedDeliverAcked` (proto ≥ 7), named member | the same verdict; a refusal or a death re-selects within the group (`reselect_shared`) | **Yes** |
| … to a `$share` member over a proto-6 link | `SharedDeliver` | no | no — fire-and-forget. (No supported release speaks 6: v1.1.0 is 12.) |
| What a rule derives from a QoS ≥ 1 publish, at QoS ≥ 1 (ADR 0083) | as a client publish: each derived message has its own gate | yes | **Yes** — the original's PUBACK waits for all of them |
| QoS 0, any origin | `Publish` / `SharedDeliver` | no | nothing to wait; shed and counted on a backlogged link (`peer-backlog`, `hub-ingress`) |
| A Will; what rules derive from a Will or a client/session event; `$SYS` statistics | `Publish`, whatever the QoS | no | no publisher exists; ungated by design (issue #238, ADR 0083) — **fire-and-forget at QoS ≥ 1** |
| Retained, non-durable mode, gated | acknowledged forward to *interested* peers; a plain `Publish` broadcast to the rest so they store it | interested peers only | for the interested peers; a missed broadcast is back-filled by the link-up digest (ADR 0014 §3) |
| Retained, durable mode | the commit through the topic's owner | yes (commit-gated) | **Yes** (`awaiting_retained`) |

**What happens to a forward already in flight when its target is declared `Dead`**
(`peer_dead`):

- An ordinary obligation toward the dead peer is **discarded**, and the entry gets a
  re-route grace of `REROUTE_GRACE_TICKS` (8 sweep ticks, one second each). During it the
  sweep and the settle pass forward the publish, flagged as a re-route, to any peer that
  now announces matching interest — the durable takeover path: the lease's new owner
  materialises the inherited sessions and announces their filters, and on a proto ≥ 12
  link the tagged frame lets it skip a session that already holds the message. If nobody
  announces it, the grace runs out and the publisher is answered **`Accepted`**: not
  failed, not left to time out, and with no answer from anyone. (If the link to the same
  peer comes back inside the grace, the peer counts as a re-route candidate and is sent
  the publish again.)
- A `$share` obligation is re-selected within its group at once; if no member is left, the
  acknowledgement is **withheld** (`shared_exhausted` → `drop_pending`).
- The receiver-side window for forwards *from* the dead peer is dropped
  (`forward_windows.remove`), so a forward that peer retransmits after returning is
  applied a second time.

**So the gap of #973 is this, and nothing in the acknowledged path itself:**

1. **The obligation is derived from the interest table, and `PeerDead` erases the peer's
   interest.** A publish that arrives afterwards finds nobody on B to owe, creates no
   obligation, and is acknowledged — at once, or at the end of the routing-settle window
   if nothing else matched (`settle::awaits_settle`). The acknowledged forward is never
   reached. `peer_disconnected`, for a link that is merely down, keeps the interest on
   purpose and says why; `peer_dead` is the same outcome by another door.
2. **Obligations already in flight are discarded** and answered `Accepted` after eight
   ticks if no successor appears.
3. **`$share` membership is dropped more readily than interest** — by `peer_dead` and
   also by `peer_disconnected` — and the settle pass re-routes ordinary interest only. A
   publish for a group whose members are all on a peer with no link finds no group; it is
   held while the mesh is not whole and acknowledged, unplaced, when the link returns.
   This needs no verdict at all, only a lost link.
4. Which node dials decides whether (1) lasts a second or the whole verdict: the `Dead`
   arm of the link manager aborts *this node's* dialer, so if the node that is wrong
   dials, nothing redials until its own `Alive` event.

Two unit tests in `hub/forwarding.rs` pin (1) and (2) at the hub
(`a_publish_for_a_peer_declared_dead_owes_it_nothing_where_a_link_down_peer_is_owed`,
`a_forward_unanswered_when_its_target_is_declared_dead_is_acked_when_the_grace_runs_out`).

## 2. What the broker promises today

| Where | What it says |
|---|---|
| `docs/delivery/0042-…`, T9 | "Acked means durable, cluster-wide … The cross-node QoS 1 forward is acked end-to-end (⑤)". |
| `cluster_stress.rs` (the oracle) | "Every ack is a **hard obligation** — acked means durable, cluster-wide (0042-T9)". "**Acked durability**: every acked `QoS` 1 payload reaches its subscriber — live, or replayed on resume after the takeover (duplicates are legal; loss is the violation)". "Under an active partition a gated ack HOLDS (the mesh-whole rule …): an alive-but-unreachable peer may hold interest this node cannot see, so "nobody is owed this" is only concluded on a whole mesh". Its subscribers are *persistent*; its faults are a kill, a restart, link flaps under SWIM's threshold, disk faults and brownout — **not a wrong verdict about a live node**. |
| ADR 0042 §1 | "**Acked durability:** a message acknowledged to a QoS ≥ 1 publisher … survives any single-fault schedule the plane is specified to tolerate — owner crash, takeover, follower loss within quorum, full-cluster restart." |
| ADR 0083 | "A **QoS ≥ 1 acknowledgement is released only for a message the broker owns** (ADR 0018, ADR 0042 T9)". |
| ADR 0005 (amendment) | "An unbounded honest refusal beats a bounded lie." |
| `hub/mod.rs`, `peer_disconnected` | "a link-down peer is not a dead peer, and its subscribers are still owed matching publishes" — and names the alternative as a defect: "found no interest anywhere and acked a trivially-empty fan-out". |
| `cluster.rs` (module doc) | "**`Suspect`** — no action; routing continues until failure is confirmed, so a transiently slow node loses nothing. **`Dead`** — stop the dialer and tell the hub to drop the peer's routing state." |
| ADR 0016 | "A node *falsely* declared `Dead` is tombstoned for `DEAD_TTL` too; §3 makes that rare but not impossible". Nothing about what it does to delivery. |
| ADR 0001 | "`QoS` > 0, clean session — [durable append:] no — nothing to resume into". |
| ADR 0029 | Non-durable mode is "RAM-only, so a correlated restart of a quorum loses acknowledged messages". |
| GUIDE, CLIENT-GUIDE | "Durable sessions: quorum-replicated **by default**; acked QoS 1/2 survives node loss". "An acked QoS 1/2 message survives the loss of the node that accepted it. A clean session is in-memory and cheaper." |
| README | "733,316,488 messages ledgered, none lost". "Resize: … zero acked loss, verified under SIGKILL + partition". "Partition: CP: minority serves committed state, retained writes queue until heal". |
| ADR 0015 | "Each node keeps the latest [shared-group] snapshot per peer; a dead/closed link drops it." |
| ADR 0014 §3, ADR 0037 | Retained: a best-effort broadcast back-filled by a digest at link-up; durable, a commit through the owner, and "retained writes **queue until heal**" on the quorum-less side. |
| RULES.md, ADR 0083 §4 | "Every derived message at QoS ≥ 1 gets its own acknowledgement gate, and the **PUBACK waits for all of them**: when it is released, each derived message is stored wherever it was owed … or has failed and been counted." |
| ADR 0082 §3 | Peer QoS 0 is shed past the credit and counted; "Remote QoS ≥ 1 is not charged. The origin's pending-publish table already bounds how many of these exist". |
| ADR 0057 (Proposed) | Exactly-once across a broker *crash*. Nothing about membership. |
| THREAT-MODEL, OPERATIONS, TROUBLESHOOTING | Silent on delivery under a wrong membership verdict. |

Per subscriber kind, for a publish that enters at A:

| Subscriber owed the publish | Promised while B is **unreachable from A** | Promised when B is **truly dead** |
|---|---|---|
| (a) clean session connected to B | Until SWIM's verdict the ack is held (mesh-whole rule; a lost link keeps B's interest). After the verdict: **silent.** | Nothing is owed (the session died with B). Stated for durability (ADR 0001), not for routing. |
| (b) persistent session on B, non-durable mode | As (a). | The session is lost with B; stated (ADR 0029). |
| (c) persistent session B's lease group owns, durable mode | "Acked means durable, cluster-wide"; "a gated ack HOLDS" under partition. A wrong verdict is neither in ADR 0042's list of tolerated faults nor excluded. | "Acked QoS 1/2 survives node loss": the successor serves the replicated log. |
| (d) `$share`, members on B only | Silent (ADR 0015: a dead or closed link drops B's members, so the group has none). | Silent; by the same reading nothing is owed. |
| (e) `$share`, members on B and elsewhere | Another member is selected (0041-T12). | The same. |
| (f) retained | Non-durable: best effort, back-filled at link-up. Durable: through the owner; a quorum-less side queues until heal. | In the surviving nodes' stores. |
| (g) what a rule republishes | As the subscriber kind it is owed to: "stored wherever it was owed … or has failed and been counted". | The same. |

## 3. What actually happens

`crates/mqttd/tests/false_dead_delivery.rs`: three brokers wired as `main` wires them (the
placement-aware hub, cluster-configured; real SWIM; the real link manager; metrics; in
durable mode the lease group and the replicated store). A is told on its own membership
event channel that B is `Dead` — the event its SWIM driver raises — and nothing else is
touched. Each flow has its own MQTT 5 publisher (a reason code would be seen) and
publishes twice: **first** at once, inside A's routing-settle window; **second** when the
first has been answered. Then A is told B is `Alive`.

| Flow (QoS 1 unless said) | Plain, **A dials** | Plain, **B dials** | Durable, A dials, A a follower | Durable, A dials, A the lease leader | Durable, B dials, A the leader |
|---|---|---|---|---|---|
| (a) clean session on B | **acked, lost** ×2 (first after 7.2 s, second at once) | delivered (first after 1.2 s) | **acked, lost** ×2 | **acked, lost** ×2 (first after 10.3 s) | delivered |
| (a) at QoS 2 | **PUBCOMP, lost** ×2 | delivered | first: connection closed, or lost; second **lost** | the same | first: connection closed (not delivered); second delivered |
| (b)/(c) persistent, connected | **acked, lost** ×2 | delivered | **acked, lost** ×2 | **a race**: lost (5 of 7 runs), or stored by the lease's new owner and delivered after the heal | delivered (a duplicate is possible: the client is moved) |
| (b)/(c) persistent, offline | **acked, lost** ×2 (never queued) | queued, delivered on resume | **acked, lost** ×2 | the same race | delivered on resume |
| (d) `$share`, member on B only | **acked, lost** ×2 | first **acked, lost**; second delivered | **acked, lost** ×2 | **acked, lost** ×2 | first **acked, lost**; second delivered |
| (e) `$share`, members on B and C | delivered (C's member) | delivered | delivered | delivered | delivered |
| (f) retained, published on A | acked; **absent on B until the heal**, then back-filled | on B at once | on B at once (through the owner) | on B at once | on B at once |
| (g) rule republish, wanted on B | **acked, lost** ×2; the action counted `ok` | delivered | **acked, lost** ×2 | **acked, lost** ×2 | delivered |
| C publishes for B's clean subscriber | delivered | delivered | delivered | delivered | delivered |
| **B** publishes for A's clean subscriber | **held**: no PUBACK in 15 s; delivered and acked at the heal | delivered | held, delivered | held, delivered | delivered |
| **B** publishes for a `$share` group on A only | **held, then acked at the heal and lost** ×2 | first **acked, lost**; second delivered | held, then **lost** ×2 | held, then **lost** ×2 | first **acked, lost**; second delivered |

Read with it:

- **Which side dials decides everything** (gap 4). When B dials, the link is back after
  `REDIAL_DELAY` (500 ms), the accept path asks membership nothing, both sides announce
  their interest again, and the settle pass re-routes what it was holding.
- **The settle window is the only protection there is, and it is about seven seconds.** A
  verdict withdrawn after 2 s loses nothing but the `$share` rows (first publish acked
  after 2.3 s, delivered); withdrawn just after the window closed, the first publish is
  lost. A false verdict normally stands for 30 s.
- **Nothing lost is ever re-delivered**, and **the publisher is told nothing**: every lost
  publish got reason code `0x00`.
- **The receiver side answers.** A holds B dead; when B's link is there (B dials), A
  applies B's acknowledged forwards and answers them — the accept path and the verdict
  path consult the link, not membership. When it is not, B holds.
- **The last row needs no verdict** (gap 3): B only lost a link.
- **Durable mode does not help unless the node that is wrong leads the lease group — and
  then it hurts as well.** A follower's false verdict moves no lease, so the durable plane
  never hears of the publish. The leader's false verdict *reassigns B's groups*: the
  session's committed owner becomes C while B, serving its connected client, still
  believes it owns it. If C materialises the inherited session before A's window closes,
  A's held publishes are re-routed to C and stored (delivered after the heal); otherwise
  they are lost. Either way, when ownership moves back, publishes toward that session go
  unacknowledged for 7–11 s (17–31 of them at four a second in these runs).
- In these tests only A was told. In a real cluster the `Dead` claim is gossiped and
  adopted by every node that has not seen B's refutation, so several nodes can be A.

**Can an operator see it?** Not as a loss. Between the verdict and the heal, on A:

| Signal | Moves? |
|---|---|
| `mqttd_publish_dropped_total{reason=…}` | **No**, for any reason. (`no-subscriber` is in the metric's help text and is emitted nowhere in the broker; `no-shared-member` is counted only for a group that *exists* with no reachable member, and here the group is gone.) |
| `mqttd_publish_forwarded_total` | Does not move for the lost publishes — an absence. |
| `mqttd_rule_actions_total{result="ok"}` | Moves: the lost republishes are counted as successes. |
| `mqttd_members{state="dead"}` | 1. The verdict is visible; that it is false is not. No shipped alert reads it. |
| `mqttd_routing_unsettled{reason="takeover"}`, `mqttd_pending_publishes_awaiting_settle` | Up for the settle window. |
| Log, at WARN | "membership: peer DEAD; dropping link and routing state", later "peer RECOVERED to alive". |

A rule on `$events/message/dropped` would be raised on A with `no_subscribers` for each
(`hub/events.rs::note_no_subscribers`); that is from reading the code, not from a test.

## 4. Violation, undocumented, or accepted

| Row | False verdict (the path works) | Real partition (B alive, unreachable from A) |
|---|---|---|
| (a) clean session on B | **Undocumented loss.** No document promises a clean session delivery across a membership change; none says an acknowledged publish may be discarded while its subscriber is connected, and `peer_disconnected`'s comment calls the same outcome a defect. | Nothing can be delivered. Acknowledging once SWIM has given its verdict is undocumented; holding until then is documented and kept. |
| (b) persistent, non-durable | **Undocumented loss.** ADR 0029 accepts losing a RAM-only session *with its node*; B did not die. | As (a). |
| (c) persistent, durable | **Violation.** "Acked means durable, cluster-wide"; "an acknowledgement is released only for a message the broker owns". The message was acknowledged and is in no log. | **Violation when the node cut off from B is a follower** (no lease moves, nothing is stored). When it leads, the lease moves and the successor stores — the documented path. |
| (d) `$share`, B only — and B's publishes for a group on A only | **Undocumented loss**, in both link orientations, and for the second form after a mere link flap. | Undocumented. |
| (e) `$share`, B and elsewhere | **Documented and correct**: re-selection. | The same. |
| (f) retained | **Documented and accepted**: late on B in non-durable mode, back-filled at link-up. | Documented (queue until heal). |
| (g) rule republish | **Violation** of RULES.md's sentence: the derived message was neither stored where owed nor counted as failed; it was counted `ok`. | As the subscriber kind it is owed to. |
| B's publishes toward A's ordinary subscribers | **Correct** (held, then delivered). | Correct. |
| The lease moving on a false verdict | Not a loss; **an undocumented availability cost** of a false verdict at the leader. | The documented takeover. |

## 5. Decision: the design

**One rule.** *An obligation toward a peer is created whenever a QoS ≥ 1 publish matches
anything that peer has announced, and it is discharged only by evidence from the node that
owns the subscriber: that node's verdict; or, for a subscriber the lease plane owns, the
verdict of the group's committed owner; or the peer's own statement that it no longer has
the subscriber; or confirmation that the peer is gone. This node's belief about the peer —
a lost link, a SWIM verdict — creates no obligation and discharges none.*

The acknowledged forward of §1 is kept exactly as it is. What changes is what feeds it and
what may end it.

### D1 — A peer under a `Dead` verdict stays in the routing tables, as unreachable

`peer_dead` stops deleting. It drops the link and marks the peer **unreachable (dead
verdict)**; the peer's `interest`, its `$share` membership, its negotiated protocol and the
forward window for it are kept. `peer_disconnected` does the same for `$share` membership,
which it drops today (gap 3). Consequences, all from code that already exists:

- A QoS ≥ 1 publish that matches the peer's interest is registered by `forward_to_peers`
  as an obligation, exactly as for a link-down peer today: recorded, not sent, the
  publisher's acknowledgement held; the sweep sends it when the link is back.
- Obligations in flight at the verdict are **kept**, not discarded. The re-route to a
  peer that newly announces the interest stays, as an addition (D2).
- `$share`: the selector prefers a member on a reachable node (so (e) keeps working
  without a pause); only when every member is on an unreachable node is the publish
  registered as a `Shared` obligation toward one of them and held.
- QoS 0 for an unreachable peer is not forwarded, as today, and is now counted (D9).

**As built (0087-T2).** `peer_dead` drops the link and erases nothing; the hub holds the
peer as *unreachable* (`VerdictHold`) with its interest, `$share` members, negotiated
protocol, forward window and every obligation toward it. `peer_disconnected` keeps
`$share` membership as it kept interest.

- *What ends a hold, until D2 exists.* Confirmation that a peer is gone is not built
  (T3), so the hold is bounded by what the verdict already cost: **8 sweep ticks** (8 s,
  `REROUTE_GRACE_TICKS`) with no link, counted from the verdict, or from the loss of a
  link the peer had under the verdict. Then the peer's state is erased and what it was
  still owed is resolved as `peer_dead` resolved it at the verdict before: the
  obligation discharged and its publisher acknowledged with no answer from anyone. (A
  `$share` delivery whose group has nobody left is acknowledged the same way, as D2's
  table has it. Erasing at the verdict withheld one that was in flight at that instant
  and acknowledged one made a moment later; held together, they are one case — and
  withholding every publish made to such a group during the hold would close each of
  those publishers' connections at once.) A forward
  in flight at the verdict is answered on the same sweep it was answered on before, and
  a publish that arrives during the hold with it — so a node that really died holds no
  publisher longer than it did. A link that returns ends the hold: from then on only
  the peer's answer discharges what it is owed. T3 replaces the countdown and that
  resolution with D2's; until it does, a peer that is alive and out of reach for longer
  than the hold — a real partition — is still acknowledged for, undelivered, after it.
  The 8 ticks are counted *without a link since the peer last had one*, not since the
  verdict: a peer held dead whose link keeps forming and dropping before it answers is
  alive, and is held for as long as it does that, with the pending table as the bound
  on its publishers. A verdict withdrawn while the peer still has no link ends the
  count too — the peer is then a member whose link is down, held until the link returns
  or a new verdict falls (today's rule for a lost link, now reachable from a verdict).
- *The successor.* A peer that newly announces matching interest is sent the publish as
  a re-route, and that forward stands in for what the unreachable peer was owed — today's
  discharge, kept so that a real owner death costs the failover pause and not the hold.
  The match is on the topic, not on the subscriber: the hub cannot tell the unreachable
  peer's session, moved to a new owner, from any other subscription that happens to
  match (D8). So a subscriber on a peer falsely held dead still loses a publish when any
  newly matching interest appears before the peer's link is back — on another node (a
  monitor on `#` reconnecting there) or on this one (a matching session attaching, which
  the settle pass delivers to and discharges on). The window is the half second the
  redial takes. T5.
- *`$share`.* A member behind a lost link or on a peer under a verdict stays in its
  group, marked not reachable: the selector takes any member that can receive now, then a
  local persistent member, then a remote member on a node with a link, and one of these
  only when the group has no other — and then the publish is owed to it and held. A
  delivery in flight at a verdict re-selects at once when the group has a member that
  can take it now — on this node, or on a node with a link — at any later tick of the
  hold at which one has appeared (the group's consumers reconnecting elsewhere), and
  stays owed until then (it was withheld from the publisher before). It does not move
  to a member on another node that cannot be reached either: the member it left is not
  tried again, so that move could strand it with a peer that never returns. Two things the sentence "all from code that already exists" above did not
  cover: a `$share` delivery to a member with no link was *not* an obligation — the
  frame is chosen by the link's negotiated protocol, which read as the oldest for a peer
  with no link, i.e. fire-and-forget, dropped, the publisher acknowledged — so the hub
  now answers with the protocol the peer's last link negotiated; and a delivery owed no
  answer (QoS 0, a Will, an event-derived message) to a group with no reachable member
  is counted `publish_dropped{reason="no-shared-member"}`.
- *When a held forward goes out.* The sweep retransmits only a forward that is overdue,
  so one recorded less than a tick before the link returned would wait a further tick.
  What a peer is owed is therefore sent when its first interest snapshot arrives on the
  new link, the first thing the peer says there. That is a trigger, not a gate: the
  sweep still retransmits an overdue forward on any link, announced on or not, so a peer
  that has just restarted can still be sent one before its view is settled and answer
  it with a failure, as before.
- *A draining node.* A publish the hub gated itself during the drain has nobody to retry
  it and a deadline: its `$share` delivery does not wait for an unreachable member and
  takes its fallback at the verdict, as before.
- *Measured* (`false_dead_delivery.rs`): with T4, every flow's first publish is
  acknowledged 0.50 s after the verdict and delivered — one redial delay and a
  handshake — the `$share` flows included, in both orientations, plain and durable. With
  the link alone cut and no verdict, every publish is held until the link is back (1.2 s
  in the test) and then delivered; before, the two `$share` flows were acknowledged at
  the link's return and lost.

### D2 — When a peer is confirmed gone, and how what was held is resolved

A held obligation ends in one of four ways.

1. **The peer answers.** Its link returns — the node that is wrong keeps dialing (D5) —
   and the sweep retransmits under the same seq; the verdict is the answer. A peer that
   has no subscriber any more says `Stored` with nothing matched, which discharges the
   obligation truthfully. This is the whole of the false-verdict case and of a healed
   partition.
2. **The lease plane's owner answers**, for interest the peer declared *durable* (D8). A
   durable session is owed to whoever the committed owner is; when a takeover moves it,
   the new owner announces the interest, the publish is re-routed to it (the existing
   path, with the proto-12 re-route flag so a session that already holds the message is
   skipped), and its `Reached` discharges the dead peer's obligation *for that durable
   interest*. This keeps a real owner death at today's failover pause.
3. **The peer says it no longer has the subscriber**: its first interest snapshot on a
   new link does not match the topic (a peer does not announce before its view is
   authoritative — 0043-P4), or it leaves gracefully (it announces its own `Dead`,
   ADR 0019, after handing its sessions on).
4. **The peer is confirmed gone**: the tombstone is pruned with the parting greeting of
   #971 unanswered and no link — `dead_ttl` (30 s) after the verdict — or a **new
   generation** of the same node id has joined (the old process is gone by definition).
   Membership raises this as its own event (SWIM is silent at a prune today).

At confirmation, what the dead peer still owed is resolved by kind:

| Kind | At confirmation |
|---|---|
| clean session on the dead node | The session is gone with its node: there is no subscriber. The publisher is acknowledged; the publish is counted as resolved-undeliverable (D9), not silently. |
| persistent session, non-durable mode | The session lived only on that node (ADR 0029). The same: acknowledged, counted. If the node returns with its disk, messages published in between are not in its queue — the documented limit of non-durable mode, stated in the client guide. *(Open: O4.)* |
| durable session | Already discharged by its committed owner's verdict (2). If no owner has answered — no quorum — the acknowledgement stays held: that is the durable plane's own rule (a write without a quorum is not acknowledged). |
| `$share`, another member reachable | Re-selected at the verdict, as today. |
| `$share`, no member left | Acknowledged, counted (today this is a withheld acknowledgement for an in-flight one and an immediate acknowledgement for a later one). |
| retained | Unchanged. |
| rule republish | As the kind it is owed to; `mqttd_rule_actions_total` counts `ok` only when the derived message's gate was answered by evidence, `failed` when it was resolved undeliverable. |

### D3 — Bounds, and what the publisher experiences

- **Where held publishes live:** the pending-publish table — 65,536 entries and 64 MiB
  per node, existing. No new queue. A per-peer count of held obligations is exported (D9).
- **At the bound:** the oldest entry is evicted and its acknowledgement withheld, as
  today (`pending-cap`); its publisher's connection is closed and the client retries with
  DUP. For a v5 publisher whose message is stored nowhere the broker could instead answer
  `0x97`, as it does for a brownout. *(Open: O3.)*
- **Per connection:** PUBACKs are written in publish order (ADR 0075), so one held
  publish holds every later acknowledgement on that connection, and the connection stops
  reading at 256 parked acknowledgements. A publisher that sends to an unreachable node's
  subscribers stalls as a whole; one that does not is untouched. ADR 0082's ingress credit
  is released at dispatch and is not consumed by a held publish.
- **How long, when the peer really is dead:** until D2 resolves it — for durable interest
  the failover pause (seconds, as today); for everything else `dead_ttl` (30 s) plus a
  sweep tick, where today it is the 8 s settle window. That is the price of the rule:
  after a real node death, publishers to that node's clean-session subscribers wait up
  to 30 s for an acknowledgement that today arrives after 8 s. At 40,000 msg/s with a
  fraction *f* owed to the dead node the table fills in 1.6 s / *f* — 10–33 s at the
  5–16 % cross-node share the README reports — so at the top of the clean-session range
  a node death can reach the bound before confirmation.
- **What MQTT shows:** a withheld PUBACK/PUBREC. The client keeps its keepalive (PINGREQ
  is answered), retransmits with DUP only after a reconnect (a duplicate, legal at QoS 1),
  and sees a reason code only at the bound, if O3 says so.

### D4 — QoS 2

The publisher's PUBREC is the gated answer; it is held like a PUBACK. Exactly-once across
the hold rests on the receiver recognising a retransmitted forward, so the forward window
for a peer under a verdict is **kept** until the peer is confirmed gone (today `peer_dead`
drops it, and a forward retransmitted across a false verdict is delivered twice with
DUP = 0). *(As built, 0087-T2: kept for as long as the peer's routing state is — the
hold of D1 — and dropped with it; T3 moves that to confirmation.)* A re-route to a durable session's new owner is covered by the origin id of
proto 12. Observed today and unchanged by this design: in durable mode the first QoS 2
publish after the verdict can be answered by closing the connection, because the
publisher's own dedup record has a replica on the node just written off.

### D5 — Both link orientations

The `Dead` arm of the link manager stops aborting this node's dialer: it keeps dialing a
peer under a verdict until the peer is confirmed gone. With D1 a false verdict then heals
on the data path within `REDIAL_DELAY` plus a handshake in either orientation, instead of
lasting the verdict when the wrong node happens to dial. Measured by making only this
change locally and running §3's tests: in all three "A dials" columns every lost row but
(d)'s first publish became delivered, the first publish acknowledged after 1.3 s instead
of 7–10 s. (The change is not part of this record's change.)

A link is admitted by its handshake, not by membership — today's behaviour, made a stated
rule and tested: a node accepts a peer its SWIM view holds `Dead`.

**As built (0087-T4).** `cluster.rs` tells the dialer the peer is held `Dead` instead of
aborting it, and `peer.rs` (`dial_member`) keeps dialing under the verdict:

- *Pace.* Each attempt that serves no link doubles the delay, from the ordinary 500 ms
  to 5 s. An attempt is bounded at each step, verdict or not: 5 s for the TCP connect
  (membership-driven dialers; a connect begun for a member in good standing is the one
  that sits in SYN retransmissions when a verdict lands), then 10 s for the TLS
  handshake and 10 s for the exchange of `Hello`s (every dialer — see below). A connect
  still under way when membership changes its mind about the peer is dropped for a
  fresh one. A peer that is really gone costs at most 15
  connection attempts in its first minute — one socket at a time, nothing queued — and
  then none; the window is looked at when an attempt ends, so the one under way when it
  closes is finished first. A link served under the verdict resets the delay.
- *A deadline on the handshake.* A dialer does one thing at a time, and it is the only
  side that may dial its pair. A peer whose kernel completed the connection and whose
  host then stopped without a reset — hung, or power-cycled — answers nothing, and a
  dialer waiting for its `Hello` without a deadline waits for good: the pair is not
  linked again until the dialing node restarts. While a verdict stopped the dialer, the
  `Alive` that followed started a fresh one and hid this; a dialer that survives the
  verdict is exposed to it, as a static peer's dialer always was. So the TLS handshake
  and the `Hello` exchange of every dialed link have a deadline. An established link
  needs none here: the node writes to it, and a host that has gone or come back
  answers a write with a reset.
- *End.* Confirmation that a peer is gone is not a membership event yet (D2, T3, O1), so
  the dialer bounds itself on what the link manager can see: it stops when the peer has
  been `Dead` with no link for **twice `dead_ttl`** (60 s) — the tombstone has been
  pruned, the parting greeting of #971 sent, and a live peer's answer would have arrived
  as `Alive` — counted from the verdict or from the end of the last link served under
  it; and it is replaced when an `Alive` names the peer at a different routing address,
  the only form in which a new generation reaches the link manager. A dialer whose
  window is spent dials no more but does not end: its task waits for its standing to
  change, and the next `Alive` sets it dialing again. (Ending it left an instant in
  which the link manager, seeing the task still running, handed it the `Alive` it was
  about to miss — a member alive with no dialer.) T3's event replaces the window.
- *What counts as a verdict standing.* The link manager keeps its own set of peers
  held dead, and withdraws the verdict — at the hub and at the dialer — on the first
  `Alive` for a peer in it, whatever came between. SWIM can raise `Suspect` between a
  `Dead` and that `Alive` (a relayed suspicion re-adding a pruned member, or superseding
  the tombstone of one that started a new life), and an `Alive` can arrive without a
  routing address; neither may leave the verdict in place.
- *What the verdict still decides.* Membership says `Dead` once. A peer that is linked
  again under the verdict announces its interest again and is routed to; when *that*
  link goes, no second verdict follows — a node that announced its own departure is
  redialed while it drains and then exits; a node falsely declared dead may die for
  real inside the tombstone's 30 s — and its interest would stay behind with nothing
  to resolve it, holding every matching publish for a node that is not coming back.
  So the hub remembers the verdict until the link manager withdraws it
  (`HubCommand::PeerAlive`, on the `Alive` that follows a `Dead`), and the loss of a
  link to a peer held `Dead` is handled as the verdict is: since 0087-T2, by the hold
  of D1, started again. (The second case exists without this task, in the orientation
  where the evicted node dials; it was found while building it.) T3 replaces the hold
  with a confirmation.
- *What the verdict was protecting.* On the accept side, nothing: `serve_listener` never
  consulted membership, which is why the other orientation already healed in a second.
  What a link requires is unchanged and is all in the handshake — the cluster CA's
  certificate on both ends, the revocation list, the node id the certificate attests, a
  common protocol version, the tie-break. (The peer link carries no cluster id; that is
  checked on gossip, which decides whom a node dials.) On the dial side the verdict did
  one thing: it stopped this node connecting to an address whose holder it believed
  gone. An address can change hands, so a membership-driven dialer now links only the
  node it was aimed at — whoever else answers there is hung up on — where before it
  would have linked whichever cluster member answered. The pin protects the dialing
  node only. The node that answered cannot know why it was dialed: it takes the
  connection for the dialer's link to *it*, registers it in place of the real one, and
  sees the hang-up afterwards, so each attempt at a stale address flaps the link of the
  pair that lives there. The dialer therefore stays away from such an address for 60 s,
  or until membership speaks of the member again. Refusing before the other node
  registers needs the expected node id in the `Hello` — a wire change, for D8's
  protocol step.
- *Measured* (`false_dead_delivery.rs`, three nodes, loopback): in all three "A dials"
  columns of §3 the first publish is acknowledged after 0.76 s and delivered, and the
  two orientations assert the same rows. Left for D1: `$share` with members on B only,
  first publish, in both directions.

### D6 — The receiver side

B holds A dead; A forwards to B. With a link, B applies the forward and answers it today
(observed, §3), because neither the accept path nor `answer_forward` consults
membership; that stays. Without one, A holds (D1). Two things change on the receiver:
the forward window is kept (D4), and a forward that matches nothing while the receiver's
own view is unsettled keeps being answered `Failed` (withheld at the origin) — the
existing receiver-side twin of the settle gate, which is conservative and correct.

### D7 — Mixed versions and the rolling upgrade from v1.1.0

D1–D6 need **no new frame**: every frame involved exists at proto 12, which v1.1.0
speaks. The behaviour is local to each node, so the guarantee arrives node by node: a
publish that enters at a v1.1.0 node is treated as today until that node is upgraded; a
v1.1.0 receiver drops its forward window on its own verdict (a duplicate, not a loss).
D8 is the one wire addition, additive at proto 15 with per-link gating; over an older link
the peer's interest is treated as not durable, i.e. held until the peer answers or is
confirmed gone. *(Open: O5 — or today's re-route discharge for such links, during the
upgrade only.)*

### D8 — Interest that says whether the lease plane owns it (proto 15, additive)

Interest gossip carries filters only, so the hub cannot tell a durable session's filter
(owed to the committed owner, wherever that is) from a clean session's (owed to that
node and nobody else). Without the distinction the rule forces a choice between two
wrongs: discharge a dead peer's obligation when a successor answers (today — and lose a
clean-session subscriber on a peer that was not dead), or hold it until the peer answers
or is confirmed gone (and turn every durable failover into a 30 s publisher pause). So
the interest snapshot says which filters belong to sessions the lease plane owns; D2's
discharge (2) applies to those and only those. A node sends the new form only on a link
that negotiated 15. *(Open: O2 — whether this is in the first cut. The view here: it is
part of the minimum complete fix, because without it either the loss or the failover
pause is wrong.)*

### D9 — Observability

- `mqttd_publish_held_for_unreachable{peer}` (gauge): obligations held for a peer with
  no link, and whether the peer is under a verdict. *(As built, 0087-T2: the node's
  total, without the label — publishes with a forward owed to a node that has no link.
  The split by peer and by verdict is T7's.)*
- `mqttd_publish_resolved_undeliverable_total{reason}`: publishes acknowledged at D2's
  confirmation because their subscriber's node was confirmed gone, by reason
  (`peer-gone`, `no-shared-member`).
- `mqttd_publish_dropped_total{reason="peer-unreachable"}`: QoS 0 not forwarded to a peer
  with matching interest and no link. The `no-subscriber` reason is either emitted or
  taken out of the help text.
- One WARN line when a hold starts for a peer and one when it ends, with the count and
  how it ended; a shipped alert on the gauge staying above zero; the held count beside
  each peer in `mqttd --admin placement`.

### D10 — The hot path

The steady state is unchanged by D1–D6: a QoS ≥ 1 forward already is the acknowledged
frame and its verdict, on every publish, and that is what the published 40,000 msg/s
(clean) and 17,000 msg/s (durable) per node were measured with. The new work is in the
event handlers (`peer_dead`, `peer_disconnected`, confirmation) and in `$share`
selection, which gains one reachability test per candidate. D8 changes the interest index
(a kind per filter, or two tries) and so is on the path of every forwarded publish. To be
re-measured before any number is republished: the hub publish-dispatch benchmark, the
clean-session QoS 1 rung at ten nodes, the durable rung, and the `$share` QoS 1 lane.

### D11 — Tests

- The six episodes of `false_dead_delivery.rs` and the two unit tests in
  `hub/forwarding.rs` are flipped: every row that says `AckedAndLost` or `HeldThenLost`
  becomes `Delivered`, `HeldUntilHeal` or `DeliveredOnHeal`, and the no-drop-counter
  assertion becomes an assertion on D9's series. A seventh episode holds B really dead
  and asserts D2's resolution and its time bound per kind.
- `cluster_stress.rs`: two injections join the vocabulary — a false `Dead` (the
  membership event, as here) and a full partition of one node (gossip and bus) that
  outlasts the suspicion window — and the ledger gains clean-session and `$share`
  subscribers; acked durability must hold for all of them after the heal.
- `cluster_proc.rs` (real binaries): a nemesis that `SIGSTOP`s a broker for longer than
  the suspicion window and `SIGCONT`s it — a real false verdict, with its clients'
  connections intact — under the acked-facts oracle.
- The rig: a membership fault under load (a stopped broker; gossip cut between two
  brokers with the bus left up), ledgered; and D10's re-measurement.

## 6. Open sub-decisions

- **O1 — What confirms a peer gone.** Proposed: the tombstone pruned with the parting
  greeting unanswered and no link (so `dead_ttl`, 30 s, is the bound), or a new generation
  of the id. Alternative: a separate, shorter bound for routing than for membership.
- **O2 — D8 in the first cut?** Proposed: yes (see D8).
- **O3 — At the pending-table bound:** withhold (today) or answer a v5 publisher `0x97`
  when the message is stored nowhere.
- **O4 — Persistent sessions in non-durable mode on a node confirmed gone:** acknowledge
  and count at confirmation (proposed), or hold longer for a node that may restart with
  its disk.
- **O5 — Links below proto 15 during an upgrade:** hold conservatively (proposed), or
  keep today's discharge-on-re-route for them.
- **O6 — A pair that cannot reach each other while both reach a third node.** SWIM does
  not call either dead (indirect probes succeed), so today and under this design the
  publish is held until the pair's link returns. Relaying through the third node is not
  proposed.
- **O7 — A v5 PUBACK `0x10` (no matching subscribers)** for a publish that matched
  nobody: a cheap signal to every v5 publisher; a separate decision, since it changes
  what an ordinary unmatched publish returns.
- **O8 — The README's "none lost"** until this ships. Since this record's change the
  sentence is followed by its condition (the runs inject no fault, and #973 is named),
  and the 1.2.0 release notes list the limit. Whether the headline figure at the top of
  the README should carry it too is open.

## 7. The false verdict itself

It still matters under this design: a false verdict no longer loses a message, but every
publish held for it costs its publisher latency, and at the leader it moves leases. #975
tracks it. Measured with #971's formation simulation (`swim_formation_sim.rs`,
`false_dead_report`): one member of a formed cluster cut off — nothing in, nothing out —
for a given time, 32 seeds per cell; a cell is the number of runs in which some node
turned a healthy member `Dead`.

| Detector | Nodes | 0.5 s | 1 s | 1.5 s | 2 s | 3 s | 5 s |
|---|---|---|---|---|---|---|---|
| **today**: 4 s, 1.5 s with three suspecters | 3 | 0 | 0 | 0 | 0 | 3 | 32 |
| | 5 | 0 | 0 | 3 | 7 | 18 | 32 |
| | 10 | 4 | 4 | 7 | 13 | 25 | 32 |
| | 14 | 6 | 10 | 16 | 21 | 25 | 32 |
| | 30 | 6 | 20 | 25 | 28 | 31 | 32 |
| window × `max(1, log10 N)` | 14 | 3 | 6 | 12 | 12 | 22 | 32 |
| | 30 | 2 | 5 | 5 | 12 | 23 | 32 |
| window doubled | 14 | 0 | 0 | 2 | 2 | 10 | 32 |
| | 30 | 0 | 1 | 1 | 6 | 11 | 31 |
| memberlist's defaults (floor `4·max(1, log10 N)` s, ceiling ×6) | 14 | 0 | 0 | 0 | 0 | 1 | 30 |
| | 30 | 0 | 0 | 0 | 0 | 0 | 23 |
| **one last direct probe before `Dead`** | 5 | 0 | 0 | 0 | 0 | 6 | 29 |
| | 10 | 0 | 0 | 0 | 0 | 3 | 31 |
| | 14 | 0 | 0 | 0 | 0 | 8 | 30 |
| | 30 | 0 | 0 | 0 | 0 | 0 | 28 |

Up to 3 s every one of today's verdicts was reached after the outage had ended; each stood
for the tombstone's 30 s. What is left with the probe at 3 s is a `Dead` declared *during*
the outage — three suspecters shorten the window to 1.5 s, and the member really was
silent when it was asked — and adopted from gossip by the others afterwards. What each costs when the member is really dead (ms until the
last survivor holds it `Dead`, median / worst): today 5313 / 8037 at 14 nodes and
6500 / 8347 at 30; the last direct probe 5613 / 8337 and 7005 / 8347; memberlist's
defaults 7775 / 20079 and 10869 / 23494. The full table and the mixed-version rows are in
#975.

The last direct probe is `Config::confirm_before_dead`, added with this record so that it
could be measured, **off by default**; so is `Config::suspicion_scales_with_log_n`.
Neither is operator configuration. The probe's rule is that a suspect which answers it is
never declared dead on that window, whatever its `Ack` carries: usually the `Ack` carries
the refutation; when it cannot (the suspect is already an incarnation ahead of a stale
claim and has nothing left to gossip about itself, or is an older build), the `Ack` alone
keeps it and it is asked for its own claim with a `Join`. That second half was added on
review; without it the 30-node row read 0, 0, 0, 2, 3, 30. The default is not changed here, although the probe is
never worse in any cell and adds no wire element: ADR 0016 asks that a change to failure
detection be a change of its own, a clean cut on a lossless network is one fault shape,
and in a cluster part-way through an upgrade the nodes still on v1.1.0 keep declaring —
and spreading — the verdicts the rule would have prevented (14 nodes, 1.5 s: 0 runs of 32
with every node on the rule, 5 with one node left on v1.1.0, 11 with half, 16 with all).

## Alternatives considered

- **Only keep dialing (D5 alone).** Measured: it removes the loss for a false verdict in
  about thirty lines, with no wire change, and it was this record's first recommendation
  for 1.2.0. It leaves a bypass — a publish in the first second still depends on the
  settle window, `$share` is still lost, and a real partition between a follower and a
  durable session's owner still acknowledges what it cannot deliver — so it is a part of
  the design, not the design. **Not chosen as the fix** (2026-10-13).
- **Only make the verdict rarer (§7).** Lowers a rate and guarantees nothing. Kept as
  #975, alongside.
- **Refuse instead of hold.** For a v3.1.1 publisher a refusal is a closed connection;
  the broker's stated preference is to hold (ADR 0005; issue #613 item 2.4). Kept only as
  O3, at the bound.
- **A new acknowledged-forward protocol.** Not needed: the one that exists is sound; it
  was being walked around.

## Consequences

- "Acked means durable, cluster-wide" becomes true through a false verdict and through a
  partition, for every subscriber kind, up to the pending table's bound.
- After a real node death, publishers to that node's non-durable subscribers wait for
  confirmation — up to 30 s where it is 8 s today. Durable failover is unchanged, given D8.
- A node's interest outlives a verdict about it by up to `dead_ttl`; the memory is the
  interest snapshot it already held.
- Operators get series and a log line for what is held and what was resolved
  undeliverable, where today there is nothing.

## Not verified

- The false verdict was injected at one node. What a gossiped `Dead` held by several
  nodes at once does follows from the same code but was not run.
- One cluster size (three nodes), loopback, no load. The race in the leader column was
  seen seven times; its odds under load are unknown.
- QoS 0, Wills and event-derived messages were not measured across a verdict; their rows
  in §1 are from the code.
- The proto-6 rows of §1 are from the code; no supported release speaks proto 6.
- D5 is built (0087-T4), and D1, D4 and D6 (0087-T2) with the bound D1 states in place
  of D2. D2, D3, D7–D9 are designed against the code and not built; D3's table-fill
  arithmetic is not a measurement.
- With D1 built, the hot path was checked by reading the diff — `forward_to_peers`,
  `register_forward` and the constant-time `$share` path are untouched — and by the
  publish-dispatch micro-benchmark (`benches/shared_plan.rs`), not on the rig.
- A peer that is unreachable by TCP while its host is up but silent (a dropped SYN, not a
  refused one) was not run: the 5 s bound on an attempt under a verdict is from the code.
  A host that accepts the connection and then says nothing is run, on loopback, against
  the handshake's deadline; a host that stops after the link is established is not —
  that such a link ends at the next write is from TCP, not from a test here.
- §7 is a simulation of one fault shape; #975 lists what it leaves out.
