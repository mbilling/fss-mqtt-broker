# QoS 0 scaling investigation plan (#613)

**Implementation follow-up:** #612 is now merged as `5c5cbae`. The first
[receipt-gated membership probe and two local pilots](MEMBERSHIP-PROBE.md) and
[profiles/TCP-client fixed-offer probes](MEMBERSHIP-TCP-PROBE.md) are implemented.
[Instruction accounting now supports a group-first index change](GROUPED-SHARED-INDEX.md).
Sustainable real-peer broker scale-out and the lane-E window repair remain outstanding.
The review snapshot and proposed acceptance contract below are retained as written.

## Scope and reviewed state

Investigation proposal for [#613](https://github.com/mbilling/fss-mqtt-broker/issues/613),
not an optimization claim or takeover of [#482](https://github.com/mbilling/fss-mqtt-broker/issues/482).
The deliverable is a sequence of falsifiable experiments, with broker scale-out first.
No new performance experiment was run for this review; historical raw results were
not independently reconstructed. No cloud provisioning or spending is authorized.

After fetching `origin/main`, both the checkout and remote main were
`4cf650317ce3f9c075b8902db5a9a0b684c829f6`.
[#612](https://github.com/mbilling/fss-mqtt-broker/pull/612) remained **open**, head
`c2c356bf9d015c33c44bb7b6a5903f8d3049816e`; its admission repair is not in this checkout.
Recheck both references before implementation. Coordinate any shared-delivery or
bridge changes with #482's owner. Do not wait for bridge work to investigate brokers.

## 1. Diagnosis boundaries

### Established by current code inspection

Paths and symbols below refer to the main SHA above, not to historical releases.

- `crates/mqtt-core/src/shared.rs::{matching_refs, GroupMembers::select_online}`
  uses indexed matching and an online prefix. `hub/delivery.rs::plan_shared`
  uses the local-online fast path and a remote filter index. Sorted remote
  membership is prepared on rebuild, not reconstructed on each publication.
  These fixes, and #526's consolidated gauge scan, already exist.
- After a local decision, `plan_shared` still walks matching remote locations,
  checks their group identity against `decided`, and skips their member lists.
  This is **not** the old scan of all remote groups or all members.
- `crates/mqttd/src/hub/forwarding.rs::forward_to_peers` still iterates connected
  peers for ordinary ungated QoS 0, even with empty matching ordinary interest.
  Shared-only, locally delivered traffic can therefore incur cluster work.
- `hub/mod.rs::Hub::run` times completed dispatch after dequeue. The measurement
  excludes queue wait, includes awaits/descheduling inside dispatch, and excludes
  detached tasks and socket writers. Sum all classes, including `cluster` and
  `sweep`; `publish` is not a count of all work per application publication.
- `hub/delivery.rs::send_to_client` bounds local QoS 0 outbound packets/bytes and
  counts `outbound-full` shedding. Online does not mean able to accept work.
  #612 reports deterministic pre-enqueue reselection regressions and channel
  receipts; those results are supporting evidence, not TCP/downstream capacity.
- `crates/mqttd/src/conn.rs` calls `out_meter.drained` **before** encoding and
  `writer.flush_queued().await`. Outbound depth can fall while data is still in
  the writer or kernel. Neither dequeue nor successful write proves receipt.
- `crates/mqtt-bridge/src/engine.rs` has unbounded inbound/per-side command
  channels; connected routing queues `Command::Publish`, while disconnected
  routing uses the spool. `forwarded` is recorded before that decision.
- `bench/scale/run-curve.sh::lane_e_rung` takes broker `before` before startup and
  `after` after drain, whereas subscriber baseline/final bracket steady state.
  Its fixed CPU duration starts before additional population settling.
  `bench/scale/cpu.sh::with_cpu_sampling` already provides a lifetime-based
  pattern for lane A, but lane E does not use it.

### Historical statements that must not drive new fixes

- [#482's original scan comment](https://github.com/mbilling/fss-mqtt-broker/issues/482#issuecomment-5475670129)
  predates #481. Its claim that local shared matching was already indexed was
  explicitly corrected by the next comment. Do not implement either old fix again.
- [The #524 comment](https://github.com/mbilling/fss-mqtt-broker/issues/482#issuecomment-5540399279)
  says the remote walk is skipped entirely. Current code skips remote **members**,
  not matching locations. The later code-inspection comment corrects this.
- [The v1.0.16 ladder interpretation](https://github.com/mbilling/fss-mqtt-broker/issues/482#issuecomment-5649904177)
  calls dispatch occupancy a fraction of one CPU core and excludes broker limits;
  [the second look](https://github.com/mbilling/fss-mqtt-broker/issues/482#issuecomment-5649945371)
  calls the evidence decisive. Neither conclusion follows from this timer,
  particularly with unaligned windows. Low aligned occupancy weakens sustained
  measured-dispatch saturation; it does not establish broker innocence.
- The issue body's single-hub ceiling is a historical interpretation, not a
  current diagnosis. Likewise the later association with shrinking driver fleets
  is a confound, not proof of driver saturation. Changing coverage and hardware,
  non-monotonic ladders, and unknown timing denominators preclude attribution.

**Missing:** aligned stage rates, actual per-node placement, independently verified
concurrent generator/consumer headroom, queue/writer time series, repeat uncertainty,
and a controlled scale-out comparison. No evidence yet establishes proportional
broker scaling, a current broker scaling defect, or a distributed capacity scheduler.

## 2. Three leading hypotheses, in investigation order

Ranking reflects discriminatory value, not a measured probability.

| Hypothesis | Relevant code | Predicted observation | Discriminating experiment | What would falsify it for the tested workload? |
|---|---|---|---|---|
| **H1: residual membership-dependent work raises broker service cost.** | `plan_shared` matching locations; `forward_to_peers` connected-peer loop | At unchanged hot-node receipts/offer and zero forwarding, CPU or dispatch cost per publication rises with matching locations or connected peers. | First experiment below: independent matching-interest and connected-peer arms, equal useful work; profile the hot process in the same window. | Cost slope is bounded below the predeclared meaningful effect, and local TCP capacity is unchanged within uncertainty. This does not exclude costs at larger N or during churn. |
| **H2: the rig cannot sustain concurrent offer/receipt work.** | `run-curve.sh::{lane_e_shape,lane_e_calibrate,lane_e_rung}`; calibration is not the full concurrent ladder | Actual send misses schedule, or consumer processing backlogs while broker stages have headroom; changing only endpoint resources moves the knee. | Independently exercise generators against a validated MQTT sink and consumers with a pre-encoded MQTT feed; then increase generator and consumer CPU separately at fixed broker load. | Both endpoint pools sustain 1.5× planned maximum concurrently with stable queues, and doubling each pool separately changes delivered rate by <5% without resolving failures. Low CPU alone is not a falsifier. |
| **H3: broker egress/admission limits hide behind cheap dispatch.** | `send_to_client`, `conn.rs` outbound batching/flush, shared peer send path | Offer and ingress remain on target, but receipts lag; outbound/peer/kernel queues, flush waits or drops rise, possibly unevenly across workers. | At fixed brokers/offer, vary capable workers 1/2/4; separately force a known remote fraction and constrain one worker. Correlate exact receipts with per-stage waits and profile CPU/off-CPU. | Full receipt and stable egress during the failure, with the deficit instead localized before dispatch. The known #612 local-full defect is not a new discovery. |

## 3. First experiment: membership cost at fixed useful work

### Smallest next milestone (M1)

Add a **local-only, receipt-gated membership fixture** beside
`crates/mqttd/benches/shared_plan.rs`, with machine-readable raw results. Do not
modify routing policy. Existing peer arms install `RemoteSharedInterest` but do
not establish `PeerConnected` links; they cannot price the connected-peer loop.
The recipients arm uses ordinary fan-out, waits for final dispatch rather than
receipts, and on this main does not decrement the outbound meter when draining.
Reuse/cooperate with #612's metered receipt helper rather than duplicating its fix.

M1 is deliberately smaller than an entire lane-E overhaul: isolate the two known
remaining loops without cloud, new production metrics, or bridge prerequisites.
Its result selects what to profile; it cannot establish network capacity.

### Exact fixture contract

- One hot Hub on a fixed current-thread runtime; fixed separate drain runtime
  with two workers. Pin these to disjoint CPU sets where available and record
  affinity, CPU model/governor, runtime threads, build/toolchain and revision.
  No per-arm change of resources. Record shared-host interference; if the host
  cannot provide the partition, report that limitation rather than overcommit.
- Publish MQTT 3.1.1 QoS 0, non-retained, no expiry or application properties,
  200-byte payloads containing run/burst/sequence IDs, to one fixed exact topic.
  Four online local subscribers belong to **one** shared group/filter, giving
  exactly one delivery obligation per publication. Prefer-local explicitly on.
- Peer counts `P = 0, 2, 4, 9`. Each nonzero arm has six advertised groups per
  peer, one online member per group. No churn during a measured interval.
  Change only these independent factors:

  | Arm | Remote shared interest | Connected peer channels |
  |---|---|---|
  | A | All six filters miss | Absent |
  | B | One filter/group matches the local group; other five miss | Absent |
  | C | All six filters miss | Present and drained |
  | D | Same as B | Present and drained |

  Keep ordinary remote interest empty. B−A estimates matching-location cost;
  C−A estimates connected-peer cost; D checks their interaction. Absent-link
  arms are synthetic Hub mechanism probes, not valid deployment topologies.
  Assert peer count and advertised group state after setup barriers. In connected
  arms, fail if **any data publication** reaches a peer (control traffic is separate).
- Publish bursts of 2,000; wait for **all 2,000 unique channel receipts** before
  the next burst. Drain with `OutboundMeter::drained` on each packet. Do not use
  dispatch completion as receipt. Record both dispatch-complete and receipt-complete
  times, packet count/bytes and per-member counts. Reject duplicates, missing IDs,
  counted drops, receiver errors, and changed placement. A 10-second burst deadline
  terminates and preserves a failed sample; it never silently retries it.
- Before comparison, replay the same packet workload directly through the drain
  fixture and exercise command construction/enqueue separately. Require each to
  sustain 1.5× the largest observed pilot rate with bounded backlog, then test
  them concurrently on the intended CPU sets. This is harness headroom, not TCP
  generator certification. Re-run representative A/D controls with extra drain
  resources: >5% movement makes broker attribution inconclusive.
- Pilot only to choose one fixed paced offered rate: 50% of the slowest healthy
  arm's receipt-gated burst rate. Save the pilot; freeze rate/config before the
  confirmatory comparison. If no arm is healthy, diagnose that failure first.
  Measure paced equal-work intervals as well as receipt-gated maximum service
  rates; do not normalize CPU using configured offer when actual work differs.
- Run five randomized complete A/B/C/D × P blocks, recording the random seed;
  bracket each block with P=0 controls. Each arm gets 10 seconds warm-up,
  60 seconds measured work, then at most 10 seconds drain. Retain startup,
  measurement and drain separately. No host/process restart inside an arm.

### Windows, instrumentation and artifacts

Use monotonic timestamps on this host for every phase, enqueue, receipt and
counter snapshot. Start CPU sampling before warm-up and stop after drain; require
coverage of the actual measured interval, not a guessed duration. Aggregate
completed Hub dispatch by **all** classes with its own actual elapsed denominator.
Record per-second process/thread CPU, RSS, meter depth/bytes, actual send and
receipt rates and drop deltas. A sampled enqueue timestamp in the fixture payload
can measure enqueue-to-receipt, **not** enqueue-to-dequeue; do not mislabel it.

For the largest paired A/D arm collect same-window whole-process CPU profiles;
collect off-CPU/scheduler evidence if wait dominates and permissions allow. Missing
profile permissions limit causal conclusions but do not invalidate exact receipt
counts. Add sampled Hub queue wait only as the next diagnostic if needed, using
bounded command-class labels and deterministic tests separating wait from dispatch.

Save a manifest, raw per-second samples, burst receipt summaries/ID exceptions,
configs, logs, profiles, sampler errors and exit statuses under a unique run path.
Include instrumented/uninstrumented controls: >5% measurement overhead requires
reducing sampling or reporting the perturbation before interpreting effect sizes.
Never overwrite failed runs or treat absent samples as zero.

### Predeclared result rules and next action

- **Valid comparison:** actual offer within 1% of target in paced arms, complete
  receipt reconciliation, correct topology, full samples, calibrated endpoints,
  and opening/closing control rates within 5%. Endpoint under-offer or wrong
  placement invalidates attribution, not the existence of the observed failure.
- **Failure:** a receipt deadline, loss/duplicate, or growing queue is a workload
  failure even if it prevents a clean cost comparison. Preserve it and localize
  the earliest diverging stage; do not discard it as an outlier.
- **H1 signal:** paired hot-node CPU/publication or service-time increase >5%,
  with a paired 95% confidence interval excluding zero, and profiles consistent
  with the corresponding loop. Report per-P values, not just an endpoint ratio.
- **H1 bounded:** upper confidence bound on the increase is <5% across this
  range. Proceed to broker TCP scale-out, not another speculative loop rewrite.
- **Inconclusive:** intervals overlap the meaningful-effect boundary or controls
  drift. Improve isolation/sample precision in a new declared run; no production
  optimization justified yet. Synthetic H1 evidence must reproduce over TCP
  before being called an end-to-end constraint.

## 4. Next experiments and measurable decision tree

### M2: TCP broker scale-out, followed by controlled forwarding

First repair lane E's window accounting (or use a small local runner with the same
contract): timestamp parallel broker **steady-before/steady-after** scrapes alongside
subscriber snapshots, preserving existing startup/drain files. Run CPU samplers
through actual lifetime. Summarization must reject missing coverage, counter resets
and excessive boundary skew (predeclare ≤100 ms for a 60-second interval), use
actual elapsed time, and not mix full-rung counts with steady latency. Add fixtures
for delayed settling, sampler early exit, failed scrapes and old artifact handling;
legacy artifacts remain readable but cannot acquire a new validity claim.

Use actual TCP brokers at N=1/2/4, where local hardware allows fixed CPU/RAM per
broker and a reserved constant endpoint pool. If it cannot, reduce the declared
N range; do not shrink endpoints or provision cloud. Use identical independent
tenants per broker: same topics, publishers, four local shared consumers per tenant,
message shape and protocol as above. Start with fixed per-node offer, then bracket
sustainable capacity. Freeze cluster/security/storage settings and voter/replication
configuration for comparisons; report standalone separately if its configuration
changes the work. Capture actual connections and forwarding, not intended placement.

Certify TCP endpoints concurrently at 1.5× maximum planned aggregate load, with
sequence receipt/latency and scheduling-deadline checks using a validated MQTT
sink/feed. Test endpoint CPU sensitivity separately. Socket-send counts are not
broker acceptance; record broker ingress and application receipts as distinct stages.
A missing independent endpoint calibration permits mechanism observations only.

Then hold N fixed (start at 2), workload and total subscribers fixed and test
remote-delivery fraction `f=0, 0.5, 1`. Move a balanced subset of tenant groups to
the next broker in a ring, keeping subscriber totals balanced. Publishers remain
fixed; measure the load-weighted actual f. Do not obtain f by silently reducing
consumer coverage as N grows. Remote delivery adds different source/destination
work; `1+f` is a dispatch-count approximation, not an exact capacity correction.

```
Actual send < target or scheduling deadlines missed?
  -> generator control at unchanged brokers; validity problem before capacity claim.
Send on target, broker ingress lags?
  -> inspect TCP send queues, connection read/decode and Hub enqueue/scheduling.
Ingress on target, Hub queue wait/depth rises?
  -> profile dispatch CPU vs waits, including cluster/sweep classes.
     CPU-bound measured stage -> smallest profiled optimization, not automatic sharding.
Dispatch completes, receipts lag?
  -> outbound meter + writer pending bytes/flush waits + kernel queues + peer pump.
     Meter empty is not proof of healthy egress (conn.rs drains before flush).
Only matching-location/connected-peer paired cost rises with P?
  -> optimize that measured traversal; verify TCP benefit and retain/ownership controls.
Only forced-remote arms degrade?
  -> measure peer serialization/pump/receiver cost and bandwidth before batching changes.
Only extra endpoint CPU/workers restore receipts?
  -> endpoint or shared-admission investigation, holding broker count fixed.
No stage accounts for deficit?
  -> add one bounded missing-stage diagnostic; do not assign the deficit to drivers.
```

### Separate worker axis, never a broker prerequisite

At fixed N and offer, test 1/2/4 capable shared consumers, then one deliberately
slow worker (local-full/healthy-remote included). Apply #612's known repair as a
separate pinned comparison, not an unlabelled revision change. If the actual bridge
is needed to reproduce an egress limit, use `ha="shared"`, unique client IDs and
one out-only group, then measure **downstream** IDs/latency. Independently cap
common downstream bandwidth to identify its plateau. Connected queues/RSS must be
observed separately from the disconnected spool. Do not assume remote capacity
from an open peer link or retry after ambiguous delivery.

**Production changes become justified only conditionally:** traversal optimization
for confirmed H1; generator/consumer harness repairs for H2 (no broker patch);
profiled encoding/writer/peer improvements for H3; bridge live bounds only for
observed bridge accumulation; admission/remote-credit design only for reproduced
unused eligible worker capacity beyond #612. Preserve retained broadcast behavior,
ownership fencing, disconnected-peer obligations and QoS 1/2 regression coverage.
Every patch needs the failing mechanism test plus the same end-to-end comparison.

## 5. Completion: an understandable scaling envelope

Before confirmatory capacity runs, declare the N range and these proposed gates:

- `C(N)` is the greatest **sustained unique subscriber-received rate** within a
  fixed p99 ≤1 second SLO, with no unexplained missing or duplicate IDs below
  capacity and stable queues/memory. Sequence-cohort reconciliation after drain
  diagnoses loss; late drain receipts do not increase steady throughput or erase
  SLO violations. Quantized histogram buckets must resolve the SLO boundary.
- Refine capacity brackets to ≤5% offered-rate spacing; repeat boundary points
  five times in randomized order and publish the bracket plus run-level 95%
  uncertainty. Do not call the highest isolated passing rung the knee when lower
  rungs fail. Explain non-monotonicity before publishing a capacity headline.
- Report `E(N)=C(N)/[(N/N0)*C(N0)]`; target a lower 95% confidence bound ≥0.90
  over the named range. Fix replica/workload semantics across N. A local shared
  host gives a diagnostic envelope, not independent-machine capacity proof.
- Report per-node ingress, receipts, forwarded work, CPU, drops and queue slopes;
  for balanced placement target max/min useful rate ≤1.10 and explain any skew.
- Confirm each claimed capacity with a 10-minute steady run. Bound queue and RSS
  growth: the upper confidence bound of their growth slope must project to <5%
  of the configured queue/memory budget over another 10 minutes, with no rising
  send-minus-receipt debt. Record budgets/high-water marks, not just average RSS.
- On the same live processes, run low load (50% C) for 60 s, overload (125% C)
  for 60 s, then return to low load for 120 s. Require queues/debt to settle in
  the first 60 s and the final 60 s to recover receipt rate within 5%, p99 within
  10% of baseline and still within SLO, without restart. Retained allocator RSS
  need not fall exactly, but must remain bounded and not grow across repetitions.
- At the eventual plateau name a measured limiting resource/stage, supported by
  a one-variable intervention that moves that plateau in the predicted direction.
  Idle average CPU, high enqueue counts, or a successful supporting fix is not
  completion evidence. If local resources plateau first, explicitly leave the
  broker capacity bound open; independent-machine validation needs approval.

Publish the raw artifacts, invalid/failing arms and uncertainty with the final
model: actual offer, broker local/remote service demand, usable consumer capacity
and downstream bandwidth. Broker and worker scaling efficiencies remain separate.
This completes an investigation only when the observed envelope and its limiting
stage are predictable—not merely when one benchmark gets faster.
