# ADR 0091 — Every bound is a function of a few measured numbers

- **Status:** Accepted
- **Date:** 2026-10-27 (accepted the same day)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0091-capacity-model.md](../delivery/0091-capacity-model.md) — plan, progress, and changelog
- **Issue:** [#1134](https://github.com/mbilling/fss-mqtt-broker/issues/1134)
- **Related:** [ADR 0090](0090-benchmark-rig-redesign.md) (the rig that measures these
  numbers), [ADR 0075](0075-pipelined-durable-appends.md) (the lane pipeline),
  [ADR 0061](0061-off-loop-durable-appends.md) (per-session append lanes),
  [ADR 0041](0041-resource-governance.md) (the outbound and backlog caps),
  [ADR 0015](0015-cluster-shared-subscriptions.md) (`$share` selection), #1078, #1132

> **Decided 2026-10-27** by the maintainer, after the 10-node rig2 run. Its three knees were
> per-session limits, not the cluster's. The generator had put every publisher of a topic
> on one broker (#1132). The broker had no defence against one hot `$share` member, which
> we knew from #1078. In their words: "we knew this issue already. why does it appear
> again. It's like we are not paying attention to the formulas of the broker design";
> "everything should be a function of some basic numbers, nodes, load, disk speed etc".

## Context

On 2026-10-27 the 10-node rig measured 432,500 msg/s at QoS 0, 379,800 msg/s at clean
QoS 1 and 101,300 msg/s at durable QoS 1. The durable rounds failed with an ack p99 under
3 ms and fsync p99 under 3.6 ms. Nothing in the cluster as a whole was busy.

What failed was one consumer session in ten. Each generator ran 20 topics and dealt
publisher `i` to broker `i % 10` and topic `i % 20`. Every publisher of topic `t`
therefore sat on broker `t % 10`. The broker prefers a local `$share` member, so it gave
the whole topic, about 2,500 msg/s, to the one member there. The other nine got nothing.

That session then hit three bounds that are fixed numbers:

| Bound | Value | What it allows |
|---|---|---|
| `LANE_PIPELINE_DEPTH` | 16 | one durable session: 16 ÷ append latency, about 2,700 msg/s at 6 ms and 480 msg/s at 33 ms |
| `LANE_QUEUE_CAP` | 256 jobs | a session's durable path may stall for 256 ÷ its rate: 1 s at 250 msg/s, 0.1 s at 2,500 msg/s |
| `MAX_OUTBOUND_QUEUE`, `DEFAULT_MAX_BACKLOG_MESSAGES` | 10,000 messages | a subscriber may stall for 10,000 ÷ its rate |

None of these numbers was derived. Each is right at one rate and wrong at another. The
rig's own settings were picked the same way: 20 topics, 50 msg/s per publisher, 128 in
flight, the start rates. Only the in-flight window was derived (rate × the ack band). #1078
had shown the same failure on 2026-10-21, filled lane and withheld acks included, and was
left as a known limit.

## Decision

### D1. The basic numbers

Every bound and every rig setting is stated as a formula over these:

| Symbol | What | Where it comes from |
|---|---|---|
| N | broker nodes | the deployment |
| c | cores per node | the host |
| f | fsync latency, p99 | the data-dir probe (ADR 0076), and `mqttd_durable_stage_seconds{stage="fsync"}` |
| r | peer round trip, p99 | `mqttd_durable_stage_seconds{stage="replicate_rtt"}` |
| k | copies of a durable session | the cluster config |
| R | offered load, msg/s | the workload |
| s | payload bytes | the workload |
| T, M | topics; members per `$share` group (or subscribers per topic) | the workload |
| S | stall budget: how long a bounded queue must ride out a stall before it refuses | policy, 1 s by default |

### D2. The formulas

| Quantity | Formula |
|---|---|
| durable append latency | L ≈ f + r (local write and quorum) |
| one session's rate, even spread | λ = R ÷ (T · M) |
| one durable session's ceiling, today | D ÷ L, where D is the lane's pipeline depth |
| a queue's needed depth | ≥ λ_max · S |
| a publisher's in-flight window | ≥ its rate × the ack band (ADR 0090) |
| disk write per node | R · s · k ÷ N bytes/s, against the measured disk bandwidth |
| replication traffic per node | R · s · (k − 1) ÷ N bytes/s |
| expected cluster capacity | the least of N · (per-node CPU limit), disk, network, and sessions × the per-session ceiling |

A limit that the formulas do not predict is either a defect or a missing term. Either way,
it is written down here before it is tuned.

### D3. The harness never sets the knee

The generator deals every topic over every broker and every group over every broker. It
computes the expected skew (busiest session ÷ mean) before it connects and refuses a
workload above 1.25. The rig VOIDs a step whose workload was skewed, and reports the
broker's measured spread on every step. A deliberate hot-session workload measures the
per-session ceiling on purpose. (Delivered with #1132.)

### D4. `$share` selection avoids a member that cannot keep up

A member whose append lane or outbound queue is past a quarter of its bound is skipped
when another eligible member exists. Locality still decides among the members that can
keep up. When every member is past the threshold, selection is unchanged and the bound
refuses as today. This is #1078's proposal; it makes one slow or cold member a local
problem instead of withheld acks for every publisher routed to it.

### D5. One session's durable rate is the store's, not the lane's

A lane submits every queued job in one batched submit, with one durability wait per batch,
instead of one wait per message under a fixed depth. A session's ceiling then follows the
store's group commit, as the node's does (ADR 0075). It is no longer D ÷ L. Order and the
per-message outcomes are unchanged.

### D6. Bounds are stated as time, with a byte ceiling

The lane, outbound and backlog bounds admit while their oldest entry has waited less than
the stall budget S, under a byte ceiling from the memory budget. The current counts stay as
floors, so no deployment refuses earlier than today. A queue bounded by time holds the
same stall at 250 msg/s and at 25,000 msg/s; a count does not. Each bound's formula is
documented beside its metric.

### D7. The report predicts before it measures

Before provisioning, the rig prints what D2 predicts for the tier: per-session rate, disk
and replication bytes per node, and the expected cluster capacity from the last smaller tier
and the measured f and r. The report then puts the measured capacity beside the prediction.
A gap of more than 25% is flagged as either a missing term or a defect, and it gets an
issue.

## Consequences

- The 10-node figures of 2026-10-27 are not reported anywhere. They are rerun after D3.
- **Until D5 ships:** one durable session's ceiling, about 2,500 msg/s at a 6 ms append, is
  a documented limit of the release that ships without it.
- D4 changes which member a `$share` publish reaches under load. Locality remains the first
  choice whenever the local member can keep up.
- D6 changes when a refusal happens: later than today at high rates, never earlier.
- Every new bound in the broker and every new rig setting comes with its formula. A review
  that finds a bare number asks for one.

## Alternatives considered

- **Raise the constants (16 → 64, 256 → 4096).** This moves each cliff instead of removing
  it. D ÷ L still sets a session's ceiling and moves with every change in disk or network
  latency. A larger count only delays the same refusal and uses more memory.
- **Fix the harness only.** With an even deal no session is hot, so the rig would pass.
  But production subscribers are not dealt evenly; #1078 happened with prefer-local off. A
  broker that a skewed load stalls at one session's limit is the defect, and the rig should
  measure that limit, not hide it.
- **Round-robin `$share` selection instead of prefer-local.** This spreads the load but
  sends most publishes across the peer bus, the limit #508 measured. D4 keeps locality and
  removes only the members that cannot keep up.
