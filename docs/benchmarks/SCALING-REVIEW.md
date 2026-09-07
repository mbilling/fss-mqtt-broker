# Broker scaling review

Scope: source at `a2e5f32`, including the v1.0.16 broker; static review, not a new benchmark or correctness proof. Focus: connections, subscriptions, publishing and cross-node scaling for QoS 0/1/2.

## Verdict

**Near-linear aggregate publishing capacity is a reasonable target for partitionable workloads, but is not established for all three QoS levels.** The ownership architecture supports it; the routing executor, subscription distribution and QoS 2 execution still impose constraints.

Define the target separately for each QoS: `Cq(N) ≈ N × Cq(1)`, where capacity means sustained accepted/delivered traffic within a fixed latency budget and delivery contract—not offered traffic. QoS 0, 1 and 2 need not have equal absolute throughput. For quorum-durable comparisons, use a baseline of at least three nodes with fixed replication factor R=3; a one-node quorum has different cost and resilience.

Linear scaling requires balanced clients and ownership, sufficient independent sessions/topics, fixed fan-out per publication, and load generators/network/storage scaling with demand. It cannot be unconditional: one hot subscriber remains a serial delivery endpoint, and broadcasting every publication to an increasing subscriber population increases work per publication.

Claude: "Verdict is sound and I could not falsify any of its code claims — every
file:line citation below resolves to what the review says it does, at `a2e5f32`.
One thing to add in the broker's favour: the QoS 2 hub-blocking gap is already
*measurable* without new instrumentation. `Hub::run` times every dispatch and
emits `observe_hub_dispatch(class, elapsed)` (`hub/mod.rs:2404-2412`) precisely as
a tripwire for awaits smuggled onto the loop. So option 1 can be opened with a
before/after number from the existing metric rather than a profiling project —
stall the QoS 2 store, watch the dispatch histogram for unrelated classes move."

**Review reply:** Agreed on reusing the metric, with a measurement correction: the
clock starts **after dequeue**, and the observation is emitted **after dispatch
returns**. It captures the blocked QoS 2 handler's elapsed time once released,
not unrelated commands' time waiting in the channel. Those commands can wait a
second and still report a microsecond dispatch. For the falsifier, stall the store,
submit an unrelated command, and assert its completion before releasing the stall;
use end-to-end latency or enqueue-to-dispatch timing for the collateral delay.

Claude: "You are right and I was wrong. Verified: `let started = Instant::now()`
sits AFTER `self.rx.recv()` returns (`hub/mod.rs:2404-2410`), so a command that
waited a second in the channel reports only its own dispatch time. My proposed
falsifier — 'watch the dispatch histogram for unrelated classes move' — would
have shown nothing and I would have read that as evidence of no problem.
What the existing metric does give free is the STALLED handler's own elapsed
time, which is an upper bound on how long the loop was unavailable. That is a
usable trigger, not a victim measurement. Measuring the victim needs an enqueue
timestamp, and `HubCommand` carries none today — so this is a small
instrumentation change, not the free measurement I claimed."

**Review reply — round 2:** Agreed for production queue-wait telemetry. The
regression test itself does not need new command timestamps: synchronize on the
store operation entering its stall, enqueue a command with an existing reply
channel, and assert that reply arrives while the store remains stalled. This
proves isolation without making scheduler timing the correctness oracle.

Claude: "Agreed — better than what I proposed. A reply-channel assertion while the
stall is held is a deterministic oracle; my enqueue-timestamp suggestion would have
made the test sensitive to scheduling. Keep the timestamps as production telemetry
only, where the question is 'how bad, in the field' rather than 'is it isolated'."

## ADR assessment

| Decisions | Assessment |
|---|---|
| **0005, 0006, 0021, 0073:** session affinity, epoch-fenced replication, bounded voters, ownership across members | Keep this design. Lease consensus is not on every publish; fixed-size replica sets and owners distributed across members permit capacity growth without enlarging the Raft quorum. Do not restore voter-only ownership or multiply lease consensuses merely to improve throughput. Placement remains limited to 256 groups. |
| **0010, 0015:** shared subscriptions, origin-selected recipient | Good: exactly one selected member per shared group avoids a cluster-wide cursor or extra coordinator hop. Current code adds locality preference and indexed matching; document those refinements alongside the older global-round-robin description. |
| **0043:** safe resize and session rehoming | Necessary for useful elasticity, not just steady-state benchmarks. Reconnects, catch-up and routing handoff consume capacity; some guarantees are described through amendments/follow-ups. Validate growth under load before claiming added nodes immediately add usable capacity. |
| **0061, 0064, 0071, 0074, 0075:** off-loop work, module seams, batching and pipelines | Strong QoS 1 improvements. Module extraction does **not** create parallel routing actors. QoS 2 was explicitly excluded from important latency improvements; that exclusion conflicts with treating QoS 2 scaling as a first-class requirement. |
| **0072:** selectable durability | Preserve the distinction between MQTT QoS and storage durability. A relaxed/local-tier improvement is not evidence that quorum-durable QoS 1/2 scales. Persistent recipients and delivered QoS determine which queue appends are owed. |
| **0076, 0077:** measured storage tuning and workload evidence | Keep the falsifications: more redb files and linger were measured slower, so neither should become a blanket scaling fix. Apply the replication, latency and harness-validity rules before publishing architectural conclusions. |

ADR maintenance: 0073's Delivery footer still says T3 is planned and T4 needs a quota raise, unlike its updated delivery plan. Reconcile proposal text, amendments and shipped behavior before using ADR prose as the implementation contract.

Claude: "Confirmed. `docs/adr/0073-scale-out-durable-ownership.md:200-202` still
reads 'T3 ... and T4 ... remain planned — T4 gated on the ...', which contradicts
the staged T4.0-T4.5 delivery plan added in 902c262. The 256-group cap the review
mentions is real and documented as such (`0073:149,152`): it caps useful owner
parallelism at 256 nodes, which is far beyond any target here — worth stating so
it is not mistaken for a near-term ceiling."

**Review reply:** Agreed: 256 is not a hard near-term node-count limit. However,
placement granularity and skew matter well before 256 nodes: ten owners average
25.6 groups each, and equal group counts need not imply equal traffic. Measure
per-owner work and hot-group distribution before deciding whether more groups
or workload-aware placement would help.

Claude: "Agreed, no correction. Worth connecting to your own evidence list: 'owner
balance' is already there as a required metric, and this is the argument for why
it must be reported as per-owner WORK, not per-owner group count. Group-count
balance is what hash placement guarantees and is not the quantity that matters."

**Review reply — round 2:** Agreed on work rather than counts. HRW gives statistical
balance, not guaranteed equal group counts. Report counts alongside bytes/s,
appends/s, pending work and service time per owner; their divergence exposes skew.

Claude: "Agreed, including reporting counts alongside rather than instead — the
divergence between them is the signal, which my 'not the quantity that matters'
phrasing would have thrown away."

## What the core code actually does

- **Connections:** transport tasks authenticate and apply ACLs before attaching. Persistent sessions can be proxied to their placement owner; nonpersistent sessions remain on their landing node (`mqttd/src/conn.rs:568–650`). This distributes connection I/O, but all local session/routing commands converge on **one Hub actor**. `Hub::run` awaits one dispatch at a time (`hub/mod.rs:2388–2449`). This is serialized execution, not a thread pinned permanently to one CPU.
  - Claude: "Confirmed. `Hub::run` awaits `self.dispatch(cmd).await` one at a time
    (`hub/mod.rs:2402-2415`). The review's careful phrasing — serialized execution,
    not a pinned thread — is the right one: the loop is a tokio task, so it yields;
    what it does not do is overlap two dispatches."
  - **Review reply:** Agreed. This establishes a serialization point, not proof
    that it caused the measured collapse. A task can migrate between worker CPUs;
    per-core averages alone neither identify nor exclude its saturation.

Claude: "Agreed, and the harness makes your point stronger than my comment did.
`run-curve.sh:126-131` documents a competing explanation in the rig itself: the
publish loop is synchronous per client, so a client can never exceed 1/RTT, and
the note ends 'That one mechanism is every \"wall\" this rig ever hit' — 3000
publishers x ~1/12ms. So hub serialization has never been shown to be the binding
constraint in any published run; a driver round-trip floor accounts for the
observed walls without it. My comment treated the serialization point as closer
to the collapse explanation than the evidence supports."

**Review reply — round 2:** The cited comment describes a QoS 1 PUBACK-limited
lane-B workload. Our SCADA runs used QoS 0, which has **no PUBACK round trip**.
That specific limit cannot explain their collapse. Also, a measured publish RTT
includes broker processing/queueing: an RTT ceiling and a broker bottleneck can
coexist. Neither the historical 'every wall' comment nor these runs establishes
that Hub serialization has never been limiting. Keep timer, driver, network and
broker limits as separate hypotheses and measure them on the actual QoS/profile.

Claude: "You are right and this was my error. Verified: the comment is attached to
`LANE_B_PUBS` (`run-curve.sh:139`), `LANE_B_QOS` defaults to 1 (`:225`), and the
text says PUBACK explicitly. Lane E defaults to QoS 0 (`:340`), where
`emqtt:publish` returns without an ack, so the round-trip floor cannot apply. I
took a lane-B/QoS-1 observation, and its 'every wall this rig ever hit' phrasing,
and applied it to QoS 0 SCADA runs. That was the strongest point I added last
round and it does not stand.
Completing the check in the other direction, so the correction is not one-sided:
the TIMER floor does apply at QoS 0, but lane E's default is 30000/1200 = 25/s per
publisher, a 40ms interval — far above the ~6ms where the drivers collapse. So at
lane E defaults NEITHER driver floor binds, and 'the driver did it' is equally
unsupported for those runs. Your instruction to keep timer, driver, network and
broker as separate hypotheses is the right one; I had collapsed them into whichever
single story was in front of me."

**Review reply — round 3:** Agreed on separating hypotheses. A 40 ms requested
interval avoids the cited short-timer regime, but does not establish aggregate
driver headroom: more sites add clients, containers and scheduling/network work.
The oversubscribed knee-hunt drivers still require validation. Neither a timer
setting nor a historical floor substitutes for achieved-offer measurements.

Claude: "Agreed — I inferred headroom from a configured interval, which is an
offered-rate setting and says nothing about what the drivers achieved. The 40ms
figure only rules out the one regime the comment documents; it does not speak to
aggregate CPU, container count or network per driver, and the oversubscribed
knee-hunt rungs remain unvalidated. Achieved offer per driver is the measurement,
and it is already in your evidence list as driver headroom."
- **Subscribers:** ordinary subscriptions already use a filter trie and recipient deduplication (`mqtt-core/src/subscriptions.rs:95–112`); a new trie is not the missing optimization. Delivery still enumerates every matched recipient. Shared selection has a constant-time local-online selection path, but the remote fallback constructs candidate lists (`hub/delivery.rs:843–978`). Six SCADA consumers spread over ten brokers cannot provide a local consumer on every publisher node.
  - Claude: "Confirmed, and the shared path is better than the review's summary
    implies. With locality on (default since #511) a group with any online local
    member is decided by one indexed lookup, explicitly independent of group size
    (`hub/delivery.rs:851-882`). The remote fallback still builds a candidate list
    per publish, but from a derived `remote_by_filter` index, not a peer scan
    (`delivery.rs:900-960`). The review's SCADA objection survives all of that:
    six consumers over ten brokers means most publisher nodes take the list-building
    fallback, which is where caching (option 3) actually pays."
  - **Review reply:** Agreed on the indexed fallback. One arithmetic correction:
    six consumers can cover six of ten nodes, leaving four without a local member
    (40%, not necessarily most). Concentration or offline consumers can increase
    that fraction. Measure the local/remote selection split; cache only the
    fallback's remaining candidate work, preserving membership/liveness updates.

Claude: "Conceded — six consumers cover six of ten nodes, so 40% take the
fallback, and 'most' was wrong.
One consequence worth adding, because it is a defect the throughput framing
hides: with locality preference on, those six covered nodes ALWAYS choose their
local member (`delivery.rs:851-860` — no peer can outrank a local online member).
The cursor then rotates only within each node's local members. So each consumer's
load is set by its host node's share of publishers, not by round-robin across the
group. Skewed publisher placement produces skewed consumer load with no
rebalancing mechanism. That is a fairness bug, not a throughput one, and it is
exactly what the original review's 'measurable fairness/backlog-aware spillover'
asks for. The local/remote split you want measured should therefore be reported
per member, not just as an aggregate ratio."

**Review reply — round 2:** Per-member delivery rate and backlog are useful,
preferably in benchmark output or bounded diagnostics rather than unbounded
production metric labels. Locality-induced skew is a documented policy trade-off,
not automatically a MQTT correctness bug: shared selection is implementation-
defined. It becomes a capacity/fairness problem when busy locals overload while
remote members have headroom. Specify the desired fairness objective before adding
spillover, and preserve one selected recipient and existing retry obligations.

Claude: "Conceded on 'bug' — shared-subscription selection is implementation-defined
and locality preference is a declared policy, so skew is a capacity/fairness
property, not a spec violation. Agreed too that per-member figures belong in
benchmark output or bounded diagnostics; unbounded per-member metric labels would
be a cardinality problem in exactly the deployments that need them most."
- **Cross-node routing:** ordinary QoS 0 forwarding finds interested nodes with a trie, then scans all connected peers and copies the payload for each destination (`hub/forwarding.rs:278–329`). Subscription changes clone full ordinary/shared snapshots to every peer (`hub/mod.rs:5868–5895`). Peer control/bulk priority and write batching already exist, but both queues are unbounded (`peer.rs:471–479, 878–936`). Fleet size and subscription churn therefore still increase per-node work.
  - Claude: "Confirmed on all three counts. Two sharpenings:
    (a) The 'scans all connected peers' cost is O(fleet) with fleet ≈ 10 — the real
    per-destination cost in that loop is `payload.to_vec()` (`forwarding.rs:322`),
    one full heap copy per destination. Switching to `Bytes` clones through internal
    forwarding is the bigger win than fixing the iteration order, and the review does
    list it in option 3 — I would promote it to the headline of that option.
    Note the full scan cannot simply be deleted: `retain_broadcasts` needs it.
    (b) `gossip_interest` (`hub/mod.rs:5869-5887`) clones the FULL ordinary filter
    list and shared snapshot to EVERY peer on EVERY local subscription change —
    O(peers x total_filters) per SUBSCRIBE. Under subscription churn this is a
    steeper curve than the forwarding scan, so I would rank the coalesce/version
    work in option 3 above the routing-iteration work.
    (c) Unbounded peer lanes: confirmed (`peer.rs:471,479`), and the code already
    knows — issue #504's comment says the invisible queue held a fifth of the
    stream on the lane E ladder while every drop counter read zero. The pump now
    publishes lane depth, so option 4 has a starting signal too."
  - **Review reply:** Agreed that existing depth gauges are a useful starting
    point. I would make the ranking workload-dependent: payload size and actual
    destination count determine copy cost; subscription churn and snapshot size
    determine gossip cost. A 200-byte steady-state SCADA run cannot establish
    which dominates a large-payload or reconnect workload. Measure allocations,
    peer bytes and snapshot frequency. Retain the all-peer retained exception;
    `Bytes` can remove internal copies but not the need to encode/transmit data.

Claude: "Conceded on both. My O(fleet)~10 dismissal silently assumed the SCADA
shape; at large payloads or high churn the ranking inverts, and workload-measured
prioritization is the correct discipline.
One detail that supports your `Bytes` caveat and sharpens it: the `to_vec` at
`forwarding.rs:322` is not the only copy — `write_batch` then encodes each frame
into the link's own buffer (`peer.rs:925-935`). So the payload is copied twice
before the syscall. Removing the `to_vec` removes one of two copies and neither
of the encode nor the transmit. That is a smaller win than my comment implied,
and it belongs in the same measured-tradeoff bucket you describe rather than as
the headline I made it."

**Review reply — round 2:** Agreed: the visible path includes an owned payload
copy and frame encoding. That is not an exhaustive end-to-end copy count—TLS and
kernel buffering may add work. Measure CPU/allocation savings and retained-buffer
memory before choosing the representation; cheap `Bytes` clones still retain
payload memory while queues grow.

Claude: "Agreed. And your last clause connects two items the document has been
treating separately: cheap `Bytes` clones retain the payload while queues grow, and
the peer lanes are unbounded (`peer.rs:471,479`). Making the copies cheaper without
option 4's bounds would let a backlog hold more payload memory alive, not less —
so the representation change should land behind the bounds, not ahead of them."

**Review reply — round 3:** Bounds remain important, but shared buffers do not
inherently retain more memory than owned copies. At equal queue contents,
fan-out clones can share one allocation instead of keeping one copy per target.
Retention can be worse when a small slice pins a much larger backing allocation,
or when changing throughput changes backlog growth. Measure those cases; neither
an inevitable memory regression nor a mandatory implementation order follows.

Claude: "Conceded — and my error was in the direction I did not check. Fan-out is
precisely where a shared buffer retains LESS: one allocation referenced by N peer
lanes instead of N owned copies. I asserted a regression from the general shape of
the change without working the case the change is actually for. The real risks are
the two you name — a small slice pinning a larger backing allocation, and altered
backlog growth from altered throughput — both measurable, neither implied. I
withdraw the ordering constraint; bounds and representation are independent."
- **QoS 0:** bypasses durability-completion gates but submits into the unbounded Hub channel (`conn.rs:2025–2053`). Slow-subscriber shedding exists; it does not itself bound ingress or peer queues. Overload can accumulate latency before shedding becomes effective.
- **QoS 1:** connection PUBACKs pipeline with a bounded FIFO; session append lanes pipeline up to 16 appends; the replicated log separates assignment from ordered commit (`conn.rs:2055–2103`, `hub/lanes.rs:295–356`, `mqtt-cluster/src/cluster_log.rs:1739+`). Durable subscriber delivery still costs per recipient, with the publisher waiting for its required local/remote obligations—not one global publish-log append regardless of fan-out.
- **QoS 2:** inbound processing awaits dedup persistence, publish completion and the acknowledged-dedup write before PUBREC (`conn.rs:2104–2227`). More importantly, outbound `pub_rec`/`pub_comp` await durable phase changes, cleanup and truncation **inside the shared Hub dispatcher** (`hub/mod.rs:4139–4222`). A slow QoS 2 store operation can delay unrelated sessions and QoS 0/1 traffic on that node. Subscription persistence and some lifecycle paths also still await storage there.
  - Claude: "This is the review's strongest finding and I confirm it. `pub_rec`
    awaits `store.advance_outbound` and `pub_comp` awaits `store.clear_outbound`
    plus `truncate_acked_now` -> `store.ack`, all inside `dispatch`
    (`hub/mod.rs:4159-4222`). Head-of-line blocking of unrelated sessions is
    structural, not incidental.
    But the inline await is deliberate, not an oversight: `truncate_acked_now`'s
    doc says ADR 0074 Decision 2 kept it because exactly-once rests on the durable
    outbound id-state (ADR 0057) and 'the pre-existing crash window between the
    outbound-id clear and the truncate must stay exactly as wide as it is'
    (`hub/mod.rs:4139-4143`). So option 1 is not 'move the await off the loop' —
    it is 'reproduce that crash window's exact shape inside a per-session lane'.
    The review's own caveat ('simply spawning detached writes is not safe') is the
    load-bearing sentence of the whole document; ADR 0057's ordering argument
    should be reread before any lane design is drawn."
  - **Review reply:** Agreed, and I reread ADR 0057. The design obligation is to
    preserve durable state transitions, wire ordering and recovery outcomes—not
    an exact elapsed crash-window duration, which scheduling cannot guarantee.
    Moving the writes off-loop exposes new interleavings: PUBREC/PUBCOMP retries,
    detach, takeover, packet-ID reuse and late completions. Freeze each operation's
    session generation, fence stale completions and test crashes between every
    durable/wire transition. Do not detach cleanup and assume QoS 1 replay
    tolerance makes it safe for QoS 2.

Claude: "Conceded on the framing — 'reproduce that crash window's exact shape' was
loose and your reading is the correct one. Correctness rests on reachable durable
states, wire ordering and recovery outcomes; wall-clock width is not schedulable
and is not a guarantee.
It is worth stating how far that goes, because it argues in your favour more than
the ADR comment's wording does: `pub_comp` documents the window's outcome as
already TOLERATED — an orphaned table entry, released by a later PUBCOMP, at the
cost of one spurious PUBREL that MQTT-4.3.3 covers (`hub/mod.rs:4205-4218`).
Widening the window in wall-clock therefore changes only the PROBABILITY of
landing in a case the design already handles, not the set of outcomes. So
duration is not a correctness objection to moving the write off-loop at all.
The real hazard is the one you list — new interleavings from retries, detach,
takeover, packet-ID reuse and late completions — and generation-fencing is the
right instrument. I over-weighted the ADR comment's 'exactly as wide as it is'."

**Review reply — round 2:** Careful: the cited orphan-record comment covers
`clear_outbound` failing and leaving a record that can elicit a spurious PUBREL.
It does not, by itself, prove safety of the distinct state where the ID record
has been cleared but the queued message has not yet been truncated. Exercise
that recovery boundary explicitly. The implementation must preserve acceptable
reachable outcomes, not just fence late completions; completion fencing cannot
undo a stale operation already committed to storage.

Claude: "You are right, and this is the correction that matters most. ADR 0074
Decision 2 states the window explicitly: 'the pre-existing crash window between
`clear_outbound` and the truncate stays exactly the width it is today — this ADR
widens no QoS 2 window' (`0074-detached-ack-truncate.md:54-58`). That is the
SUCCESSFUL-clear-then-crash-before-truncate state. The tolerance comment I quoted
(`hub/mod.rs:4205-4218`) is the FAILED-clear state. Two different windows; I
merged them and then reasoned from the wrong one.
Worse, the reasoning I built on it was the specific error you warned against one
round earlier. ADR 0074's removability argument rests on 'a duplicate at QoS 1 is
spec-legal' (`:31-35`) — which is exactly why Decision 2 carves QoS 2 out. I took
that QoS 1 replay tolerance and applied it to QoS 2, having been told in round 1:
'Do not detach cleanup and assume QoS 1 replay tolerance makes it safe for QoS 2.'
Concretely, in that window the outbound id record is gone while the message sits
above the ack watermark, so a resume replays it as a fresh delivery to a subscriber
that has already PUBCOMPed and has nothing left to dedup against — a duplicate at
QoS 2, not a tolerated outcome. So my conclusion that 'duration is not a
correctness objection to moving the write off-loop' is withdrawn. Widening this
window raises the probability of an exactly-once violation, and your point that
fencing cannot undo an already-committed stale operation is the reason a lane
design cannot recover it after the fact."

**Review reply — round 3:** This identifies the right suspected recovery gap,
not yet a reproduced failure. Add a deterministic test that commits ID clearance,
blocks truncation, restarts from durable state and checks replay/handshake behavior.
If a fresh delivery duplicates an already completed QoS 2 exchange, the gap exists
in today's inline path too: shortening the window is not a correctness fix.
Evaluate atomic durable completion or a recoverable completion marker covering
both ID retirement and delivery retirement. Respect noncontiguous completions;
blindly advancing a prefix could discard earlier unacknowledged messages.

Claude: "Agreed on all of it, and I want to be careful not to repeat this round's
mistake by overstating what I have. I traced the restore path and it is CONSISTENT
with the gap, but a code reading is not a reproduction: `outbound()` rebuilds the
id table keyed by offset and `pending()` replays the log (`hub/mod.rs:3616-3628`),
and the replay is suppressed only by `resumed_offsets` — the in-memory
still-in-flight set, which a restart empties. So an offset whose id record was
cleared but whose entry was never truncated has nothing left to suppress it and
would replay under a fresh packet id. That is a hypothesis your test would settle,
not a confirmed defect, and I am stating it that way deliberately.
Your consequence is the part I had not drawn: if it reproduces, it is a property
of TODAY's inline path, so it is not an argument against moving the write off-loop
— it is an argument that window width was never the safety mechanism. That
reframes my round-2 withdrawal: I was right to withdraw the claim as unsupported,
but the repair is atomic or recoverable completion covering both retirements, as
you say, not a shorter window.
Two supporting details from the code. `ack`'s own doc scopes its tolerance to QoS 1
verbatim — 'Redelivery of not-yet-truncated messages after a failover is spec-legal
for QoS 1' (`mqtt-storage/src/lib.rs:420-422`) — so the storage layer, ADR 0074
and the hub comment all confine that tolerance to QoS 1 in three separate places.
And on noncontiguous completions: `advance_ack` already takes the oldest still
outstanding offset minus one, falling back to `high_water` only when nothing is
owed (`hub/mod.rs:840-851`), so it advances a contiguous settled prefix and cannot
discard an earlier unacknowledged message. Your caution is satisfied by the
existing design — worth preserving explicitly as a test, since any completion
marker added here would be the natural place to break it."

## Prioritized options

1. **Make QoS 2 nonblocking to unrelated sessions — high priority, medium/high risk.** Move phase transitions into owned, bounded per-session lanes with completion events. Preserve write-before-PUBREL/PUBREC, packet-ID lifetime, reconnect fencing and ordering. Then consider bounded inbound QoS 2 pipelining. First tests: a deliberately stalled QoS 2 store must not stall unrelated QoS 0/1; crash/reconnect at every handshake boundary must preserve the protocol guarantees. Simply spawning detached writes is not safe.
2. **Reduce unnecessary cross-node traffic — high priority, medium effort.** Add owner-aware client routing/client-facing redirects where supported, retaining the authenticated proxy fallback. Offer bounded tenant affinity with multiple partitions for hot tenants, rather than pinning an entire large tenant to one node. Keep shared locality preference, but add measurable fairness/backlog-aware spillover. Benchmark both local and forced-remote delivery.
3. **Make routing work proportional to destinations — medium effort.** For ordinary non-retained QoS 0, iterate the interested-node set and look up live peers instead of scanning the mesh. Preserve special retained-broadcast behavior. Cache remote shared selection structures and use immutable payload buffers through internal forwarding where feasible. Coalesce/version subscription updates; retain full snapshots for recovery. Narrow global routing-settle dependencies only after adding authoritative handoff evidence—never by simply skipping existing ack gates.
4. **Bound overload across the whole path — high priority, medium effort.** Introduce message/byte credits for ingress and peer bulk queues, reserve control/completion capacity, and apply fair per-session admission. QoS 0 must backpressure or explicitly count shedding; QoS 1/2 must not acknowledge obligations that admission cannot satisfy. Measure queue age as well as depth. Raising fixed caps only postpones collapse.
5. **Partition the Hub within each node — strategic, high effort/risk.** Use stable session/placement-group shards, shard-local routing/inflight state and cross-shard delivery commands. Keep one logical owner per session; aggregate durable fan-out completions before releasing publisher acknowledgements. This can improve vertical scaling and per-node throughput, but does not eliminate hot keys or cross-node costs. Avoid both a global mutex around multiple workers and per-message consensus. Storage sharding is a separate decision; retain group commit by default.

Claude: "I agree with the ordering, with one amendment. Options 1 and 4 are the two
that change what the broker *guarantees* under load; 2 and 3 change constants.
Within option 3 I would reorder as noted above: immutable payload buffers and
coalesced interest gossip before the mesh-iteration change, since the mesh scan is
O(10) and the other two are O(destinations) and O(peers x filters).
Option 5 (Hub sharding) is correctly placed last. Worth adding explicitly: it should
not be attempted until option 1 lands, because QoS 2's inline store awaits would
otherwise be sharded *along with* the blocking, turning one predictable stall into
several harder-to-attribute ones."

**Review reply:** I support landing QoS 2 isolation before a broad sharding change
for reviewability, but it is not a technical prerequisite: the designs could be
integrated, and sharding alone would reduce a stall's blast radius. It would not
remove the stall. Also, option 1 should preserve existing guarantees while improving
isolation; option 4 makes overload handling bounded and explicit. Locality and
routing work are not only constant-factor changes: they can reduce the number of
cross-node deliveries and per-publish dependence on fleet size. Prioritize using
measured workload costs rather than a universal ordering.

Claude: "Conceded that reviewability, not technical dependency, is the real
argument for ordering — I overstated 'should not be attempted until'.
I do want to hold one technical point against 'sharding alone would reduce a
stall's blast radius'. The store is `Arc<dyn SessionStore>` (`hub/mod.rs:1824`):
ONE shared object behind every shard. If the stall originates in the store — a
redb write, a group commit — sharding the Hub does not reduce blast radius at
all, because every shard blocks on the same store. Sharding bounds blast radius
for loop-occupancy that is CPU-bound, which is a different failure than the one
option 1 addresses. So the two are not freely interchangeable in the way that
sentence suggests, though your integration point stands for the compute case."

**Review reply — round 2:** Agreed that Hub sharding cannot remove a shared-store
capacity bottleneck. But an `Arc<dyn SessionStore>` proves shared ownership, not
that every operation shares one blocking lock or that every dispatch uses it.
With one actor, one QoS 2 store await stalls unrelated nondurable QoS 0 work;
with independent actors, a shard doing that QoS 0 work can continue while another
awaits storage. Shards that all need the stalled writer remain affected. This is
isolation from an I/O stall as well as CPU work, not a cure for shared-resource
contention. Test both the mixed-QoS case and the all-durable case.

Claude: "Conceded — 'does not reduce blast radius at all' was too strong. `Arc<dyn
SessionStore>` establishes shared ownership and nothing about lock granularity or
which dispatches touch storage, and I read it as though it did. Your mixed-QoS case
is the one my claim gets wrong: a shard doing nondurable QoS 0 work has no reason
to wait behind another shard's QoS 2 store await, so the isolation is real for I/O
stalls and not only for CPU. What survives is narrower and I would keep it as a
test requirement rather than an argument: in the all-durable case, shards contending
for the same stalled store stay affected, so sharding must be measured on both
workloads before its isolation is stated as a property."

For large tenant-isolated fleets, independent regional/tenant cells remain a simpler option, with explicit cross-cell bridge/subscription limits rather than a claim of one indefinitely scalable mesh.

## Evidence needed before claiming linear scaling

The earlier SCADA runs measured **QoS 0 only**. They did not prove a hub or NIC root cause, QoS 1/2 scaling, or a linear node-count curve. The 10-node knee hunt's 8-site rung was loss-flagged, not a pass; its higher rungs oversubscribed 4-vCPU drivers. The 48-core comparison also changed plans and topology. Average idle CPU and softirq samples identify investigation leads, not causality; the claimed ramp-loss explanation was not verified.

Run a fixed-hardware 3/5/7/10-node matrix, separately for QoS 0/1/2, with fixed R=3 and durability tier, fixed per-publication fan-out, balanced growing client populations, and repeated sustained rungs. Include clean versus persistent sessions, shared versus ordinary subscriptions, hot-key skew, and local versus remote delivery. Lane E currently rejects QoS 2 (`run-curve.sh:652`); add a validated QoS 2 driver/oracle rather than changing that guard alone.

Claude: "`bench/scale/run-curve.sh:652` confirmed — `case \"$LANE_E_QOS\" in 0 | 1)`
rejects QoS 2 by validation, so the guard is the visible half of a missing driver,
exactly as the review says. Relaxing the case arm without a QoS 2 oracle would
produce numbers with nothing checking the handshake, which is worse than no numbers."

**Review reply:** Agreed about validation, but the guard proves only that **this
lane excludes QoS 2**; it does not prove the underlying load generator lacks QoS 2
support. First audit its handshake, inflight window and measurement semantics.
Reuse existing driver capability where sound; add message identities, completion
accounting and crash/reconnect checks rather than assuming a new driver is required.

Claude: "Conceded — I inferred a driver limitation from a harness guard, which the
code does not support. The harness passes `-q $LANE_E_QOS` straight through to
`emqx/emqtt-bench:0.6.3` (`run-curve.sh:1457,1471`); nothing there constrains the
value. The guard is a harness restriction only.
Doing the inflight/measurement audit you asked for, on the existing rig:
`run-curve.sh:126-131` records that emqtt-bench's publish is SYNCHRONOUS per
client — `emqtt:publish/4` returns on the PUBACK, and `-F` never fills because
the loop is one-at-a-time. At QoS 2 that return moves to the PUBCOMP, i.e. two
round trips instead of one, so the per-client ceiling roughly halves. Against the
~12ms round trip measured at 10 nodes, a QoS 2 client caps near ~40/s. Offering a
QoS 0 rung's rate at QoS 2 therefore needs roughly double the publishers, or the
rung measures the driver rather than the broker — the same failure mode the
review flags in the earlier runs.
So the conclusion is yours, with a number attached: reuse the driver, resize the
publisher fleet, and add the completion/identity accounting. No new driver."

**Review reply — round 2:** Reuse is the first option, but the cited harness
comment does not verify QoS 2 completion behavior in the pinned emqtt implementation.
Audit or test that before asserting PUBCOMP return timing. Even if completion takes
24 ms, a sequential client could theoretically sustain about 41.7/s; SCADA asks
for **25/s per publisher** (1200 × 25 = 30,000/site), so doubling publishers does
not follow from that estimate. Measure completion time, timer scheduling and
sustained offers under load. Do not transfer a historical QoS 1 RTT into a new
QoS 2 capacity claim; changing publisher count also changes the named profile.

Claude: "Conceded, and the arithmetic is yours. Verified the defaults:
`LANE_E_SITE_RATE=30000` (`:325`) over `LANE_E_PUBS_PER_SITE=1200` (`:332`) is
25/s per publisher. Against your 24ms completion estimate a sequential client
sustains ~41.7/s, comfortably above 25/s, so no doubling follows.
My ~40/s figure came from halving a 12ms PUBACK RTT measured on lane B at QoS 1
with 3000 publishers — a different lane, QoS, and population — and I then compared
it against nothing. Two unverified transfers stacked into a capacity claim. The
completion timing in the pinned emqtt build is unaudited, as you say, and your
point that changing the publisher count redefines the named profile is the reason
resizing would not have been a free fix even had the number held."

**Review reply — round 3:** To keep the record precise, 24 ms was a conditional
example using your estimate, not my measurement. Its reciprocal is only an upper
bound under an ideal sequential loop; a driver that sleeps 40 ms after completion
would achieve a different rate than one scheduling against a 40 ms deadline.
Audit that scheduling behavior and measure the pinned driver before claiming
25/s is comfortably sustainable. The agreed conclusion is that doubling the
publishers has not been justified.

Claude: "Correct on the record and I accept the attribution fix — 24ms was your
conditional use of my estimate, and I restated it as though it were an independent
measurement. That is the same pattern as the RTT transfer, one step removed: a
number acquiring authority by being repeated rather than measured.
Your fixed-delay versus fixed-rate distinction is one I should not guess at either.
`-I` is passed straight through (`run-curve.sh:1471`) and whether emqtt sleeps the
interval AFTER completion or schedules against a deadline changes the achievable
rate materially at QoS 2 — sleep-after-completion would make the per-publish
completion time additive to the interval rather than absorbed by it. Unaudited in
the pinned build. Agreed the only settled conclusion is that doubling the
publishers has not been justified."

Report offered, broker-received, protocol-completed and unique delivered rates separately, alongside p99, backlog growth, loss/duplicates, owner balance, peer bytes, storage batches and driver headroom. Use profiles of Hub dispatch/queue delay, connection writers, allocations, IRQ/RSS configuration and per-thread CPU to attribute bottlenecks. Restart or demonstrably drain between overload trials. Finally test grow/shrink, owner death and partitions: QoS 1 permits replay duplicates, while QoS 2 must preserve its per-session handshake guarantees.

Claude: "Agreed, and I would make one item non-optional rather than one of a list:
'restart or demonstrably drain between overload trials'. With both peer lanes
unbounded (`peer.rs:471,479`), a rung that overloads leaves work resident in a
queue that no counter reports, so the NEXT rung starts with an invisible debt.
Several of the earlier SCADA rungs the review questions are consistent with that,
which is a cheaper explanation than the ramp-loss story that was never verified."

**Review reply:** Agreed: a verified drained/reset state should be mandatory.
However, 'no counter reports' is too strong here: as your earlier comment notes,
the peer pump now exposes queue depth. That gauge is not a complete accounting of
bytes, age, in-flight socket data or other queues. Require sustained queue/broker
recovery plus a repeated low-load control rung, or restart between independent
trials. Carry-over remains a hypothesis until those observations support it; it
is not yet a replacement diagnosis for the earlier runs.

Claude: "Conceded — 'no counter reports' contradicted my own previous sentence
about the pump publishing lane depth, and it overstated the case. Precisely: the
pump exposes lane DEPTH; queue age, queued bytes and data resident in socket
buffers are not accounted. That is a gap, not an absence.
And yes — carry-over is a hypothesis I offered as if it were a finding. Your
repeated low-load control rung is the right falsifier and it is cheap; until it
runs, neither carry-over nor the ramp-loss story is established, and I should not
have ranked mine above the other on plausibility alone."

**Review reply — round 2:** Agreed. Make the reset/drain check and repeated control
rung acceptance conditions of the next campaign, with failures reported as
inconclusive trials rather than broker capacity findings. No additional causal
claim is warranted from the existing summaries.

Claude: "Agreed, with nothing to add. Making them acceptance conditions and
reporting failures as inconclusive trials is the right disposition — it is what
stops a rung that violated the preconditions from re-entering the record as a
capacity finding later."

**Recommendation:** first close the QoS 2 Hub-blocking gap and establish the per-QoS scaling matrix; then prioritize locality and bounded queues. Pursue Hub sharding when profiles show routing serialization is the remaining limiter.
