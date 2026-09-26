# The QoS 1 scaling curve — shared-subscription delivery vs node count

**Verified against `v1.0.17`, measured 2026-09-20 and 2026-09-21.** Two
qualified points on the ADR 0077 lane E curve at **QoS 1 both ways**: publishers
at QoS 1, subscribers granted QoS 1, clean sessions, one `$share` group per
tenant site. Measured by `bench/scale/run.sh`, certified by
`bench/scale/lane-e-evidence.py`, reported by
`bench/scale/qos1-campaign/report.py`.

**Corrected twice, 2026-09-22.** These numbers are what the *rig* carried. The
limit is one core of the broker host saturating on **network interrupt
handling** — mqttd's own hot path had ~2× headroom at the knee. See
[The limit is one core — and it is the kernel's, not mqttd's](#the-limit-is-one-core--and-it-is-the-kernels-not-mqttds).

## Read this first

- **What this is:** mqttd measured against itself at two cluster sizes, on the
  durability-gated QoS 1 path. Cross-broker comparison is a different document
  ([SINGLE-NODE-COMPARISON.md](SINGLE-NODE-COMPARISON.md)); the QoS 0 / durable
  curve is [SCALE-CURVE.md](SCALE-CURVE.md).
- **Latency is a bucket upper bound, not a point estimate.** Percentiles come
  from emqtt-bench histogram buckets merged across drivers, so "p99 ≤ 5 ms"
  means the 99th percentile fell in a bucket ending at 5 ms. Coarse, and
  incapable of flattering.
- **Every latency carries a clock bound.** Publishers and subscribers sit on
  different hosts, so a cross-host latency is only as good as the two clocks
  agree. The fleet disciplines to one reference over the private network and
  every capture is validated; the bound is stated per rung below.
- **A rung passes only if all of it passes.** The drivers offered the load, held
  its schedule, the broker delivered it, the population had fully connected, the
  cluster was reset, delivery reached steady state before the window opened,
  the backlog drained afterwards, and the ledger closed on exact message
  identities. Any one failing voids the rung.

## The curve

| nodes | qualified load | payload throughput | per node | repetitions | median received | late | p99 upper bound |
|---:|---:|---:|---:|---:|---:|---:|---|
| 3 | **120,000 msg/s** | **25.9 MB/s** | 40,000/s · 8.64 MB/s | 3 + control | 120,000.4/s | 0.001 % | ≤ 5 ms ± 5.2 ms clock |
| 5 | **180,000 msg/s** | **38.9 MB/s** | 36,000/s · 7.78 MB/s | 3 + control | 180,001.3/s | 0.001 % | ≤ 5 ms ± 6.5 ms clock |
| 7 | **270,000 msg/s** | **58.3 MB/s** | 38,571/s · 8.33 MB/s | 2 certified of 3 + control | 269,995/s (broker) | — | ≤ 500 ms |
| 10 | **390,000 msg/s** | **84.2 MB/s** | 39,000/s · 8.42 MB/s | 1 certified of 3 + control | 390,002/s (broker) | — | ≤ 500 ms |

The 7- and 10-node rows are a **different campaign** (2026-09-26, one provisioning;
see [7 and 10 nodes](#7-and-10-nodes--one-provisioning-2026-09-26)), with 20
consumers per site rather than 10, and they are qualified less strictly: the
repetitions not certified were INVALID on driver-side endpoint evidence while
every broker received the full offer. 7 nodes has a measured knee just above:
300,000/s failed on late publishers.

Throughput is application payload: 216 bytes per message (a 200-byte body plus
16 bytes of timestamp and sequence the audit driver appends so every identity can
be reconciled). On the wire each publish is ~237 bytes once the topic and MQTT 5
framing are counted, so ingress is 28.4 and 42.7 MB/s — 228 and 341 Mbit/s — and
roughly doubles again as each message is delivered to its shared-group member.

**The constraint is CPU per message, not bytes** — measured, not assumed. See
[Payload size is nearly free](#payload-size-is-nearly-free).

**Scaling 3 → 5 nodes is 1.5× the load on 1.67× the nodes — 90 % of linear.**
Between those two runs per-node capacity fell. The explanation offered at the
time: cross-node delivery means more packets per message, and they land on the
one core already saturating on interrupt handling. **The fall does not continue:**
7 and 10 nodes, with every broker holding a local member and 0.00% crossing,
carry ~39k/node, back between the 3- and 5-node points
([below](#7-and-10-nodes--one-provisioning-2026-09-26)).

Delivery matched offer to within 2 msg/s at every rung of both runs.
**53,607,265** and **90,825,125** unique message identities were reconciled,
with zero missing, zero unexpected, zero unacknowledged.

### Every rung, including the ones below the top

| nodes | sites | rep | offered | emitted/s | received/s | late | p99 | verdict |
|---:|---:|---:|---:|---:|---:|---:|---|---|
| 3 | 1 | 1 | 30,000 | 29,997.8 | 29,999.5 | 0.001 % | ≤ 1 ms | PASS |
| 3 | 1 | 2 (control) | 30,000 | 30,000.3 | 29,999.5 | 0.016 % | ≤ 1 ms | PASS |
| 3 | 2 | 1 | 60,000 | 59,997.4 | 60,000.2 | 0.002 % | ≤ 5 ms | PASS |
| 3 | 4 | 1 | 120,000 | 119,999.8 | 119,999.2 | 0.002 % | ≤ 5 ms | PASS |
| 3 | 4 | 2 | 120,000 | 119,998.8 | 120,000.4 | 0.001 % | ≤ 5 ms | PASS |
| 3 | 4 | 3 | 120,000 | 119,998.9 | 120,001.7 | 0.001 % | ≤ 5 ms | PASS |
| 5 | 1 | 1 | 30,000 | 29,998.8 | 30,000.1 | 0.001 % | ≤ 1 ms | PASS |
| 5 | 1 | 2 (control) | 30,000 | 30,000.6 | 30,000.9 | 0.001 % | ≤ 1 ms | PASS |
| 5 | 2 | 1 | 60,000 | 59,999.3 | 60,000.4 | 0.001 % | ≤ 5 ms | PASS |
| 5 | 4 | 1 | 120,000 | 119,999.9 | 119,998.6 | 0.000 % | ≤ 5 ms | PASS |
| 5 | 6 | 1 | 180,000 | 179,999.6 | 179,999.8 | 0.741 % | ≤ 100 ms | PASS |
| 5 | 6 | 2 | 180,000 | 179,999.9 | 180,001.3 | 0.001 % | ≤ 5 ms | PASS |
| 5 | 6 | 3 | 180,000 | 179,998.9 | 180,001.6 | 0.001 % | ≤ 5 ms | PASS |

The first 180,000 repetition ran 0.741 % late at p99 ≤ 100 ms, against 0.001 %
and ≤ 5 ms for the two after it. It passes the declared 5 % lateness budget, but
it is the warm-up rung and it is not as clean as its repeats.

## The caveat that matters most

**180,000 msg/s at 5 nodes passed three times on one provisioning and FAILED on
another** — same release, same workload, same container density, different
physical machines:

| provisioning | late | p99 | verdict |
|---|---:|---|---|
| 2026-09-20 14:57 UTC | 0.001 – 0.741 % | ≤ 5 ms | PASS ×3 |
| 2026-09-20 15:48 UTC | **6.572 %** | ≤ 500 ms | **FAIL** — publishers late |

The difference was the load generator, not the broker. On the failing fleet the
busiest driver core ran **78 % mean busy and spent 11.5 % of the window above
95 %**; on the passing fleet the same shape left every driver core below 56 %
mean and never above 95 %. Cloud hosts are not interchangeable, and a rig that is
merely adequate on one draw is not adequate on the next.

Read the curve accordingly: these are loads mqttd **has carried under audit**,
not loads it will carry on every machine you rent.

## Payload size is nearly free

Holding the message rate at 60,000/s and growing the message instead, on the same
5-node cluster (2026-09-22). Every rung passed, including the largest:

| payload | per message | payload throughput | per node, in + out | busiest broker core | driver | p99 |
|---:|---:|---:|---:|---:|---:|---|
| 200 B | 216 B | 13.0 MB/s | 41 Mbit/s | 37.3 % | 30.3 % | ≤ 1 ms |
| 1 KiB | 1,040 B | 62.4 MB/s | 200 Mbit/s | 43.3 % | 38.6 % | ≤ 1 ms |
| 2 KiB | 2,064 B | 123.8 MB/s | 396 Mbit/s | 51.6 % | 38.7 % | ≤ 1 ms |
| 4 KiB | 4,112 B | 246.7 MB/s | 790 Mbit/s | 53.3 % | 29.7 % | ≤ 1 ms |
| **8 KiB** | 8,208 B | **492.5 MB/s** | **1,576 Mbit/s** | **50.2 %** | 34.2 % | ≤ 5 ms |
| 200 B (control) | 216 B | 13.0 MB/s | 41 Mbit/s | 44.5 % | 31.9 % | ≤ 1 ms |

**A 38× increase in bytes cost 13 points of CPU**, and the hot core did not trend
upward at all past 2 KiB — 53.3 % at 4 KiB, 50.2 % at 8 KiB. Per-message cost
dominates so completely that payload size is close to free.

Two things this settles, and one it does not:

- **mqttd's QoS 1 ceiling is a message rate, not a bandwidth.** At 200 B, five
  nodes carry 180,000 msg/s = 38.9 MB/s and one core is saturated. At 8 KiB the
  same cluster carries **at least 492 MB/s** at a third of the message rate with
  that core half idle.
- **Small messages are the expensive case.** A fleet-telemetry workload of
  200-byte readings is far harder on this broker than a firmware-blob workload
  moving twelve times the bytes. Size a cluster on messages per second, not on
  megabytes per second.
- **492 MB/s is a floor, not a knee.** Nothing was saturated at 8 KiB — not the
  core, not the drivers, not the network, which carried 1,576 Mbit/s per node
  without complaint. Where the byte ceiling actually is remains unmeasured.

## The limit is one core — and it is the kernel's, not mqttd's

**Corrected twice.** The first revision said broker cores idled. The second said
one core was saturated and inferred mqttd's QoS 1 hot path was single-threaded.
Both were wrong, and this is what the evidence actually shows.

One core does saturate. On a 4-vCPU broker at 180,000 msg/s it runs 91–99 % busy
while the other three sit at 52–68 %. But the breakdown says what it is doing:

| | core 0 | core 1 | core 2 | core 3 |
|---|---:|---:|---:|---:|
| user | 26.0 % | 36.3 % | 22.6 % | 24.0 % |
| system | 38.3 % | 7.8 % | 35.5 % | 36.4 % |
| **softirq** | 0.5 % | **54.6 %** | 8.9 % | 0.3 % |
| idle | 35.3 % | **1.2 %** | 33.0 % | 39.3 % |

**That core is doing network packet processing**, not broker work. And mqttd's own
hub loop — the single-threaded routing task, measured by the broker's
`mqttd_hub_dispatch_seconds_sum` rather than by sampling CPU — is nowhere near
its limit:

| rung | hub loop occupancy |
|---|---|
| 3 nodes, 120,000/s | **0.42** of one core |
| 5 nodes, 180,000/s (passing fleet) | **0.46** |
| 5 nodes, 180,000/s (failing fleet) | **0.53** |
| 5 nodes, 204,000/s | **0.57** |

This agrees with [#611](https://github.com/mbilling/fss-mqtt-broker/pull/611),
which measured the hub loop at 0.50 of a core while a 5-node cluster carried
510,368 msg/s of QoS 0. The hub thread is not the per-node ceiling here either.

**The payload sweep proves the mechanism.** Holding 60,000 msg/s and growing the
message 38×, softirq does not move — 28.6 %, 24.4 %, 33.9 %, 32.3 %, 32.7 %
across 13 → 492 MB/s. Per-packet kernel work, not per-byte.

### Spreading that softirq does NOT raise capacity — already tested

The obvious prescription is to spread the interrupt work across the idle cores.
It has been tried, on the identical ladder with one variable changed
([#505](https://github.com/mbilling/fss-mqtt-broker/issues/505),
[#507](https://github.com/mbilling/fss-mqtt-broker/pull/507)), and the answer is
settled in both directions:

- **The diagnosis held.** RPS moved the hot core 92.9 % → 85.3 %, halved peak
  softirq, and doubled mqttd's user time on that core.
- **The prescription failed.** Peak throughput moved 360,828 → 367,780 msg/s —
  **+1.9 %, noise**. `broker_nic_spread` therefore ships default-off as a
  *latency* lever, explicitly documented as not a capacity one.

So the saturated core is a symptom, not the cause. Spreading the work
redistributes it; it does not reduce it.

**The cause is cross-node forwarding**, settled in
[#508](https://github.com/mbilling/fss-mqtt-broker/issues/508): on the same
binary, hardware and ladder, preferring a local shared-group member took 5 nodes
from 360,000 to **510,000 msg/s, +42 %** — which is why
[#511](https://github.com/mbilling/fss-mqtt-broker/pull/511) made
`shared_prefer_local` the default, and why these QoS 1 runs already had it on.

The QoS 1 numbers here are consistent with that reading. Per-node capacity falls
as the cluster grows — 40,000/node at 3 nodes against 36,000/node at 5 — because
a larger cluster forwards more, each forward costs packets, and those packets land
on the core already carrying the interrupt load. The softirq core is where the
cost *appears*; cross-node delivery is what *creates* it.

**Where mqttd's own QoS 1 ceiling is remains unmeasured**, and raising it is a
routing question rather than a kernel-tuning one.

## Where this stops

The 3- and 5-node figures are not knees. The 7-node campaign found one, between
38.6k and 42.9k msg/s per node (below). As of 2026-09-26, the three limits the
first campaign named:

1. **The pending-publish cap.** Each in-flight QoS 1 publish holds an entry in a
   per-broker table capped at 4,096 in `v1.0.17`. Since emqtt-bench holds one
   unacknowledged publish per client, a saturating rung drives ack latency toward
   the publish interval and every publisher holds a slot at once — so the table
   fills at the *publisher head count*, not at a rate. At 5 nodes that caps the
   offerable load at 20,480 publishers × 10 msg/s = **204,800 msg/s**. Measured
   2026-09-20 at 21,000 publishers: one broker evicted 104 publishes and
   withheld 104 acks, exactly as predicted. Issue
   [#633](https://github.com/mbilling/fss-mqtt-broker/issues/633) raises the
   bound; the curve cannot pass 204,800 msg/s at 5 nodes until it ships.
   **Shipped** (65,536 entries, bounded by bytes): the 10-node point carried
   39,000 publishers' worth of 390,000 msg/s with no eviction.
2. **Driver capacity** is no longer the binding constraint, but it is close
   enough to confuse a reading: driver cores ran 45–52 % mean while the broker's
   hot core ran 91–99 %. An earlier revision of this document had that backwards.
3. **Sizes 7 and 10 are measured** — below.

## 7 and 10 nodes — one provisioning (2026-09-26)

**Per-node QoS 1 capacity holds at 7 and 10 nodes**: ~39k msg/s per node, against
40k at 3 and 36k at 5. **7 nodes has a knee**, between 38.6k and 42.9k per node.

**Setup:**
- **Order:** arms 10 → 7 → 10 on 10 × CCX23 + 20 × CCX33, re-formed per size by
  `resize-cluster.sh`, through `482-per-node-knee.sh`
  (`bench/scale/qos1-campaign/curve-n7-n10.env`).
- **Binary:** `main` 0a08187, pinned by sha256. That is v1.0.18's broker minus
  the #647 retransmit-timing fix.
- **Driver:** the same audited driver as above (`67bb4194…`).
- **Site shape:** the same as above, **except 20 consumers per site in 2
  containers**. With 10, the containers would cover only 6 of 10 brokers (6 of 7)
  and ~40% of publishes would cross; with 20 every broker holds 2 local members.
  Each container still receives 15,000/s.
- **Drivers:** all 20 at both sizes.
- **Gates:** crossing certified 0.00% on every broker of every rung of all three
  arms (`GATE … PASS`, `cert=canary`).
- **Drift:** the closing 10-node arm matched the opening one at 12 sites within
  0.001% (360,002 vs 360,004/s), with busiest-broker idle 28% vs 29%.

| nodes | sites | per node | broker received/s | busiest broker idle | verdict |
|---|---|---|---|---|---|
| 10 | 10 | 30.0k | 299,996 | 38% | pass, p99 ≤ 5 ms |
| 10 | 12 | 36.0k | 360,004 | 29% | INVALID EVIDENCE |
| 10 | 13 | 39.0k | 390,020 | 25% | **pass**, p99 ≤ 500 ms |
| 10 | 13 rep 2–3 | 39.0k | 390,002 · 390,002 | 25% | INVALID EVIDENCE ×2 |
| 7 | 8 | 34.3k | 240,002 | 33% | pass, p99 ≤ 5 ms |
| 7 | 9 | 38.6k | 270,002 | 26% | **pass**, p99 ≤ 500 ms |
| 7 | 9 rep 2 | 38.6k | 269,955 | 26% | INVALID EVIDENCE |
| 7 | 9 rep 3 | 38.6k | 269,995 | 25% | **pass**, p99 ≤ 500 ms |
| 7 | 10 | 42.9k | 300,002 | 23% | **FAIL — publishers late (7%)** |

**The INVALID EVIDENCE rungs are the driver's measurement, not the broker's.**
- **What happened:** the audited driver's endpoint scrapes landed outside the
  window's 2% uncertainty bound, so the summarizer refuses to certify
  subscriber-side delivery from them.
- **What the brokers saw:** every broker's own counter shows the full offer
  received, with 0.00% crossing.
- **Not load:** the flag also appears at 6 sites on 7 nodes (low load), so it is
  intermittent rather than load-driven, and deserves its own fix.
- **So what this table can claim:** 7 nodes at 38.6k/node has 2 of 3 repetitions
  certified; 10 nodes at 39k/node has 1 of 3. Neither meets this document's
  "3 + control" bar, and both are reported as such.

**The 7-node knee is the broker's.**
- At QoS 1 a late publisher is a slow acknowledgement, not a busy driver: every
  driver was ≥ 66% idle on that rung.
- The broker's busiest core was 15% idle at its lowest second.
- 300,000/s at 7 nodes (42.9k/node) fails; 270,000/s (38.6k/node) passes.

**10 nodes has no knee here.** Its 42k/node probe was skipped by the ladder's
early stop, which counted the two INVALID EVIDENCE repetitions as failures. That
counting is now fixed: evidence-only failures neither count toward the stop nor
reset it.

## The workload

Identical at both sizes, and unchanged across every rung:

| | |
|---|---|
| Release | `mqttd v1.0.17`, signed |
| Brokers | CCX23 (4 dedicated vCPU, 16 GB), one per node |
| Drivers | CCX33 (8 dedicated vCPU) — 8 at 3 nodes, 12 at 5 nodes |
| Protocol | MQTT 5, QoS 1 publish, QoS 1 subscription grant, clean sessions |
| Site (the rung unit) | 3,000 publishers × 10 msg/s × 200 B = 30,000 msg/s |
| Per-publisher pacing | one publish every 100 ms, fixed at every rung |
| Consumers | one `$share` group of 10 per site, 3,000 msg/s each |
| Placement | containers spread across all drivers; every container spans all brokers |
| Window | 60 s, opened only after delivery held within 5 % of offer for 3 polls |
| Driver image | pinned emqtt-bench 0.6.3 + audit extension, `sha256:67bb4194…` |

Publishers and subscribers are deliberately **not** co-located per site, and the
cluster default is prefer-local routing — so a publish may be served by a local
shared member or forwarded to a peer, and the rung measures both paths.

## What the evidence had to survive

Every rung is certified rather than trusted, and each of these gates rejected a
real run during this campaign:

- **Steady state before the window.** Publishers run throughout the connect ramp,
  so a window opened on a backlog measures repayment, not capacity. Delivery must
  sit inside a ±5 % band around the offer for three consecutive polls first.
- **A forwarding positive control**, before every size: 100 QoS 1 messages over
  each directed broker pair, reconciled exactly, so a rung cannot certify a
  cluster whose cross-node delivery is broken.
- **Exact message identities.** Per-topic sequence bitmaps for sends, PUBACK
  completions and shared-group receipts; the ledger must close with zero missing,
  unexpected or unacknowledged. Duplicates are *reported*, since at-least-once
  permits them.
- **Cross-host clock validation** at every capture, gated on the fleet-relative
  error rather than absolute time, with the bound published beside each latency.
- **Driver core saturation**, per core rather than per host: a host mean hides
  one pinned scheduler, and a rung whose generator had no headroom is not a
  measurement of the broker.
- **Budget consistency** before provisioning: every phase budget as one tree,
  checked so a parent covers its children (`bench/scale/budgets.py`).

## Reproduction

```sh
# no cloud calls — validates the shape, the budgets and the ceilings
QOS1_PROFILE=bench/scale/qos1-campaign/curve-n3.env \
PREFLIGHT_ONLY=1 bench/scale/run.sh full 3

# the measured run, with automatic teardown
QOS1_PROFILE=bench/scale/qos1-campaign/curve-n3.env \
QOS1_DRIVER_ARCHIVE=/absolute/path/to/driver.tar.gz \
bench/scale/qos1-campaign/run-confirm.sh

python3 bench/scale/qos1-campaign/report.py RUN --output RUN/analysis
```

The 7- and 10-node points run as arms of one provisioning through the knee
campaign, with the same audited driver (build it with
`bench/scale/qos1-driver/build.sh <dir>`; the published points used the archive
with sha256 `67bb4194…`):

```sh
cd bench/scale
set -a && . qos1-campaign/curve-n7-n10.env && set +a
export QOS1_DRIVER_ARCHIVE=/absolute/path/to/driver.tar.gz
export MQTTD_VERSION=1.0.18      # or MQTTD_URL=... MQTTD_SHA256=... BENCH_GIT_REF=<commit>
PREFLIGHT_ONLY=1 ./482-per-node-knee.sh   # offline
./482-per-node-knee.sh                    # PAID: 10 -> 7 -> 10 on 30 servers
python3 extract-lane-e.py --crossing-gate 0.5 .runs/knee-<stamp>/<arm>/results
python3 summarize-curve.py .runs/knee-<stamp>/<arm>/results
```

The run behind the 7- and 10-node rows is `knee-20260926T151513Z`.

Raw captures, per-rung metrics, ledgers, clock reports and `evidence.sha256` are
retained per run outside the worktree. The run directories behind this document
are `curve-n3-20260921T061627Z` (3 nodes) and `curve-n5-d12-20260920T145744Z`
(5 nodes); the contrasting failed provisioning is
`curve-n5-210k-20260920T154805Z`.
