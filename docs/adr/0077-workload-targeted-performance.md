# 0077. Workload-targeted performance — a shape is not a result without its tail

Date: 2026-08-28
Status: Proposed

## Context

The scale rig measures five named workloads (ADR 0048 T3, `bench/scale/workloads/`)
and publishes a curve per release. The 2026-08-26..28 campaign showed the practice
is not yet fit for the purpose it is put to, in two distinct ways.

**The reported numbers were not the ones a reader needs.** The report led with
1,047,236 TPS on the broadcast fan-out shape. That rung's p99 is **≤7,500 ms**, and
the single-node top rung's is **≤25,000 ms**. Both figures had been printed by
`summarize-curve.py` since it was written; nothing but a manual assembly step stood
between the data and a page that no industrial or financial reader could act on.
The same data holds a far better claim that went unreported: at 2,400 offered, p99
is ≤7,500 ms at one node, ≤500 ms at three, and **≤100 ms at five** — the argument
for clustering is tail latency, not throughput.

**Four of the five workloads produced a wrong answer at least once, and every one
was the harness.** A metrics scrape that counted a working container as zero
(reported 75% complete against a true 100.6%); a REST port that could still be bound
on restart; a fixed attach timer that outran cluster convergence at 7 nodes and made
a harness bug look like broker message loss on two consecutive releases; a workload
file whose loader silently kept the first of two assignments and ran a 100-subscriber
shape while reporting a 420-subscriber one; and a bench driver built from a moving
`main`, worth a 24% swing on its own — enough to invert the sign of a version
comparison and nearly cause a published release to be pulled. Each produced a
*plausible* number rather than an obvious failure.

Two more gaps are structural rather than accidental. **Only lane A replicates.**
Everywhere else a rung is a single measurement, so most differences cannot be told
from noise — within one run the per-broker CPU spread reached 14.6 points, larger
than the 8.2-point effect it was being read to support. And **nothing is measured
under failure or over time**: no node is killed under load, no link is cut, nothing
runs longer than about a minute.

Meanwhile the questions arriving from the field are not "how many messages per
second". They are: *how many vehicles per node, and what does one asleep in a tunnel
cost me*; *what is the tail latency under load, and is a message ever lost*; *how many
sites can one cluster carry, and does that number scale with nodes*; *what does the
edge-to-central hop cost*. A benchmark that answers none of those persuades nobody,
however large its headline.

## Decision

**A workload is the unit of performance work, and a measurement of one is incomplete
without its tail latency, its replication count, and what it does not cover.**

Four rules, each a response to a specific failure above.

1. **No throughput figure is published without its p99 beside it.** The report is
   generated (`bench/scale/report.py`), not assembled, so this cannot be forgotten
   in the writing. A rung whose offered rate was not met keeps its flag rather than
   being dropped.

2. **A difference is not a finding until it is replicated.** Lane A runs three reps;
   nothing else does. Until a lane replicates, its differences are reported as
   suggestive and the report says so. Cross-run CPU comparison is not permitted at
   all — every size provisions fresh hosts.

3. **A cross-version comparison pins the harness.** `BENCH_GIT_REF` defaults to the
   release under test, and any comparison across releases pins one commit for every
   arm. This is what the 24% harness swing cost, and it is not a judgement call.

4. **Each workload states the number its industry buys**, in the units that industry
   uses — bytes per sleeping session, tail latency under load, sites per node, cost
   per hop — and the report states what the measurement does *not* cover in the same
   document.

The first two workloads to be taken to that standard are **SCADA scale-out** and
**bridge cost**, because together they answer one architecture question that the
existing five cannot: for a 3,000-site estate producing ~90M msg/s and 18 GB/s of
telemetry, is the answer one cluster per region, or one broker per site bridging
upward? Neither half is currently measurable — no lane scales tenants, and the
bridge (3,787 lines, ADRs 0025/0059/0060) has **never been measured by the rig at
all**.

## Consequences

The rig gains a lane that scales a *tenant unit* rather than a rate, and — for the
first time — measures a component that is not the broker. That is a real scope
extension and is stated here rather than slid in: `mqtt-bridge` is a separate
process with its own failure domain, and its spool behaviour under link loss is a
property of the boundary, not of the cluster.

Some workloads will be found unsuitable for what they were meant to prove, and that
outcome is recorded rather than retried until it passes. The `telematics` shape has
already produced two void runs; if the third shows that emqtt-bench's synchronous
QoS 1 publish cannot drive a fan-in shape honestly, the workload is cut and the
finding kept — the ADR 0076 precedent, where two of three tasks shipped as
falsifications.

Claims made from these numbers inherit the honesty rules with them. A figure quoted
outside the report without its tail latency, its replication count, or its coverage
gap is a misquotation of this decision, not a shorthand for it.
