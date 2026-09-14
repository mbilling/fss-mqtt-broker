# QoS 0 shared-worker capacity — #482

## Acceptance target

For a fixed payload and one delivery obligation per source message, sustainable
**downstream-received** throughput should grow approximately proportionally with
usable shared-worker capacity until offered rate, the broker path, or downstream
capacity limits it. Broker count and bridge-replica count are separate scaling
axes. A successful arm must also meet a predeclared latency SLO, have no growing
queues, and recover to its low-load baseline after overload. QoS 0 does not promise
lossless delivery under failure/overload, but legal shedding does not excuse
avoidable loss while the intended worker pool has usable spare capacity.

This remains an **open acceptance target**, not a measured result.

## First reproducible defect and admission repair (2026-09-13)

Baseline: main `4cf650317ce3f9c075b8902db5a9a0b684c829f6`.
A shared member stays connected with a full outbound; another member has room.
The old selector considered liveness/locality, not that known queue pressure:

| Local deterministic fixture | Before | With admission repair |
|---|---|---|
| Full local + draining local; 12 control messages | 6 received, 6 shed | 12 received, no shed |
| Prefer-local; full local + two advertised remote members | 0 peer forwards, 12 shed | 12 peer-channel admissions, 6 per remote member |

The second row is **not remote socket receipt**. The fixtures pin the exact sequence
sets (including the held queue), not only an aggregate delivery count. No sleeps or
throughput thresholds select the outcome: attach and command barriers are bounded,
and pressure is established by filling the configured byte or packet budget.

The repair changes selection **before enqueue**, never retries a send. A full local
target can be replaced within its exact `(group, filter)` by another online member;
locals with room retain preference. The pressure scan borrows members and clones
only its choice. It is O(group membership), paid only on known pressure; the normal
local path stays O(1) in group size. Remote choices require a usable link, but their
consumer capacity remains unknown. No alternative preserves one counted shed.
Capacity is rechecked per group at commit, so overlapping groups cannot blindly
spend the same last local slot. Closed-target reaping and QoS 1/2 paths are unchanged.

Seven regressions live in `crates/mqttd/src/hub/tests/shared_capacity.rs` and its
`controls.rs`. Negative controls individually exercised and restored:

- Disable pre-enqueue reselection: four tests fail, including the 6/12 local loss.
- Use the reduced local next-cursor for fallback: remote split becomes 12/0, not 6/6.
- Omit exact remote group identity: a message escapes its group; expected shed disappears.
- Ignore remote link availability: an unreachable member consumes the message; expected shed disappears.

Other controls cover application-property bytes, all-full single shedding plus
recovery, overlapping group obligations, ordinary QoS 0 shedding and QoS 1 rotation.

## Receipt-gated local microbenchmark

```sh
cargo bench -p mqttd --bench shared_capacity -- \
  --warm-up-time 2 --measurement-time 3 --sample-size 30
```

One current-thread runtime runs a Hub, publishers and channel drainers; each burst
contains 2,000 messages of 200-byte payload. Every receiver decrements its outbound
meter, and a burst completes only after all 2,000 channel receipts. Missing receipts
fail after a bounded deadline rather than being reported as a faster result. Tasks
are aborted on fixture drop. The pressure arm keeps one additional connected member
full while four members drain. Deterministic tests, not benchmark counters alone,
provide the identity/no-duplicate oracle.

The older `shared_plan` benchmark's recipient drainers also now call
`OutboundMeter::drained`: previously they consumed packets without decrementing
the meter, eventually making empty queues look full and benchmarking shedding.
Its historical recipient timing rows are not used as the baseline here. That
benchmark still measures dispatch, whereas this new benchmark gates each burst
on actual channel receipts.

Local same-session comparison, rustc 1.97.0, release profile, Linux x86_64, CPU
affinity fixed to logical CPU 2 for both runs (shared workstation, not isolated
hardware). The control disables only the call to `replan_qos0_shared`; all other
code and the receipt-gated benchmark are identical. Mean wall time per burst,
Criterion 95% confidence interval in brackets:

| Draining members | Admission disabled | Admission enabled |
|---|---|---|
| 1, all ready | 1.8858 ms [1.8760, 1.8978] | 1.8575 ms [1.8490, 1.8693] |
| 4, all ready | 1.9663 ms [1.9529, 1.9808] | 1.9631 ms [1.9456, 1.9800] |
| 64, all ready | 2.2023 ms [2.1247, 2.3334] | 2.2456 ms [2.1516, 2.3770] |
| 4 draining + 1 full | loss exposed by the deterministic regression | 2.2306 ms [2.1999, 2.2561] |

The table records the final candidate. An earlier pinned admission-enabled pass
measured 1.8904 / 1.9917 / 2.3584 ms for 1/4/64 members and 2.1644 ms with one full
member (the pressure path subsequently gained an early return when a local
alternative exists, avoiding remote-member scans). The final and earlier runs
do not show a large common-path penalty at 1/4 members, but small speedup/overhead
claims are not robust across those repetitions. The 64-member pair has substantial
outliers and overlapping intervals: it does not establish a reliable effect size. An earlier unpinned trial was also noisy (4-member
mean 2.6260 ms, interval [1.9408, 3.7402]); pinning is not proof of an idle workstation.
These are **Hub plus channel-receipt costs**, not MQTT/bridge capacity numbers,
subscriber latency SLOs, or evidence that four workers give four times the throughput.

## Remaining work before a scaling claim

1. Bound/observe the bridge's connected-path queues and expose downstream pressure.
   `spool.max_bytes` bounds the disconnected spool, not the engine's live channels.
2. Define remote admission/credit semantics if remote spare capacity must be used
   predictably. Do not retry a QoS 0 forward after ambiguous acceptance. The local
   repair is not a distributed credit protocol or a globally load-aware scheduler.
3. Align lane E's broker/driver/subscriber steady-state windows and sampler lifetime;
   current broker before/after snapshots span startup and drain too.
4. Run actual TCP bridge replicas 1/2/4 with adequate fixed driver/downstream
   resources. Measure downstream sequence receipts, latency, queue/RSS trends and
   drops; separately slow one member and constrain shared downstream bandwidth.
5. Validate broker scale-out separately: fixed per-node workload and verified
   consumer placement/locality, repeated points and predeclared efficiency bounds.
   Local shared-host tests diagnose mechanisms, not cloud capacity. Paid runs
   require an explicitly approved experiment and budget.
