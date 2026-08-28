---
adr: "0077"
title: "Workload-targeted performance — a shape is not a result without its tail"
adr_status: Proposed
tasks:
  - id: 0077-T1
    title: "The report is generated, and every throughput figure carries its p99"
    status: done
    date: 2026-08-28
    evidence: "bench/scale/report.py + report_html.py (PR #471). Takes run directories, keys them by the mqttd version each recorded, emits one self-contained HTML page; adding a size, a workload or a release is pointing it at another run. Imports summarize-curve.py rather than reimplementing it — above all the lane B latency histogram, which must be differenced against a post-ramp baseline and merged across drivers, since an end-of-rung scrape reports the container's whole lifetime and puts a rung's connect ramp in its published tail. Every rate now prints its p99; a rung whose offer was not met keeps its flag; lane A is labelled as the only lane that replicates; the footer states what is NOT measured (no soak, no node killed under load, no partition, fresh hosts per size so CPU is not comparable across runs, one measurement per rung outside lane A, no comparison against another broker). This exists because the hand-assembled version published 1,047,236 TPS without the ≤7,500 ms p99 beside it."
  - id: 0077-T2
    title: "The harness stops producing plausible wrong numbers: four defects, each fixed where it could not recur"
    status: done
    date: 2026-08-28
    evidence: "PR #458 — a subscriber whose REST port failed to bind was counted as ZERO, so nodes=5 reported 75% complete against a true 100.6%; attach and resume now use disjoint port ranges and the drained total is read from the containers' own logs, which survive a dead endpoint. PR #466 — lane D declared sessions attached after a fixed 16s sleep; at 7 nodes only 1,553 of 1,680 had registered and publishers spent 40s aiming at sessions that did not exist, which looked like broker message loss on BOTH v1.0.9 and v1.0.10; it now polls mqttd_sessions until the population exists. PR #467 — run-workload.sh loads first-wins, so a corrected LANE_B_SUBS_OVERRIDE=420 appended below a stale =100 was silently discarded and a whole comparison ran the wrong shape; the loader now refuses any file assigning a key twice. PR #470 — BENCH_GIT_REF defaulted to `main`, worth a 24% swing on the same broker (20,394 vs 25,292 msg/s, non-overlapping across three reps each) and enough to invert a version comparison's sign; it now defaults to the release tag. PR #468 — dashboards on by default, since every one of these was found by forensics on static snapshots when a live graph would have shown it."
  - id: 0077-T3
    title: "Lane E — scale-out by tenant: the rung is a SITE, not a rate"
    status: planned
    notes: "Lane B's rungs vary the offered rate at a fixed population; a site scales publishers, rate and consumers together, which its shape checks and knee detection cannot express. One site = 1,000 publishers x 30 msg/s x 200B QoS 0 into a $share group of 2 on site/<n>/#, which is the measured SCADA shape rather than an invented one. Ladder doubles (1,2,4,8,16,32) because the knee is the only point of interest; 32 is the ceiling with 5 drivers at ~200k msg/s each, so the bound is the harness and must be printed as such. A rung passes only if its p99 stays under a stated bound. Whole ladder runs inside ONE cluster provisioning, so the cost is per cluster size, not per rung."
  - id: 0077-T4
    title: "Sites per node — is the tenant capacity of a cluster linear in its nodes?"
    status: planned
    notes: "Run T3's ladder at 1, 3 and 5 nodes. The deliverable is a single purchasable number — sites per node at acceptable p99 — and whether it holds as nodes are added. If 3 nodes carry fewer than 3x what 1 node carries, the gap IS the cross-node forwarding cost, and it decides per-site edge brokers versus regional clusters for a 3,000-site estate. Depends on T3. Roughly 3 provisionings, EUR 5-8."
  - id: 0077-T5
    title: "The burst arm — a step function, which no lane currently measures"
    status: planned
    notes: "Every lane measures steady state. The SCADA workload's sizing constraint is a once-daily burst (90,000 per turbine; 90M messages and 18 GB per site) against a default MQTTD_MAX_QUEUED_MESSAGES of 100,000 — 900x over, so the broker will not buffer it, it will drop by policy. Measure what is held, what is refused and with which reason, and how long recovery takes. Whether the burst is SYNCHRONISED across sites is a 1,500x swing in required capacity (4,500M msg/s over 60s versus 3M staggered across 24h), so the arm sweeps the stagger, not just the size. Depends on T3."
  - id: 0077-T6
    title: "What one bridge hop costs — the rig measures something that is not the broker"
    status: planned
    notes: "mqtt-bridge is 3,787 lines across ADRs 0025/0059/0060 and the rig has NEVER measured it, though a 3,000-site topology puts it in the path of every message. It is an MQTT client to both sides, not a plugin, so a bridged message is delivered as a normal subscriber and then re-published: expect roughly double the message work plus a serialize/parse round trip and a network leg. Four numbers: per-message overhead bridged vs direct at equal offered rate; added p99 (irrelevant for datalake telemetry, not for the 1% QoS 1 alarm traffic); the throughput ceiling of ONE bridge, which is the one expected to bite — a single client connection against a 30,000 msg/s site, where our measured per-subscriber QoS 0 rate of 10,806 msg/s would mean three bridges per site rather than one; and spool behaviour under link loss, which is the failure sites will actually experience."
  - id: 0077-T7
    title: "Is the ~1,000 msg/s per-subscriber ceiling the broker or the bench?"
    status: planned
    notes: "Cheapest task here and the highest leverage: it swings the datalake consumer count by 30x (3,000 versus 90,000 writers across the estate). The figure comes from QoS 1 $share runs. Every QoS 0 measurement we hold is BROADCAST fan-out, where a subscriber sustained 10,806 msg/s and the 240-subscriber runs never plateaued (1,201 -> 2,403 -> 4,123 as offered load rose). We have no QoS 0 $share measurement at all, and shared subscriptions add per-group selection work the fan-out path does not do, so the extrapolation crosses a mechanism boundary — exactly the step that produced wrong answers throughout the 2026-08 campaign. Sweep the group size at 30,000 msg/s per site, QoS 0."
  - id: 0077-T8
    title: "Soak and node-kill — the two gaps that block every high-availability claim"
    status: planned
    notes: "Nothing runs longer than about a minute, so there is no evidence about sustained behaviour, memory growth or a 24-hour soak; and no node is killed and no network partitioned while load is running, so the HA claim rests on unit tests rather than on the rig. For the industries this ADR targets, behaviour on node loss is the FIRST question asked and currently has no measured answer. Expensive (hours, not minutes) and therefore sequenced last, but the report footer must keep disclosing the gap until this lands."
---

# Delivery: ADR 0077 — Workload-targeted performance

[ADR 0077](../adr/0077-workload-targeted-performance.md) · tasks and status in
the frontmatter above · this file is the plan, progress log, and changelog.

<!-- status-table:0077 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0077-T1 | ✅ done | 2026-08-28 | "bench/scale/report.py + report_html.py (PR #471). Takes run directories, keys them by the mqttd version each recorded, emits one self-contained HTML page; adding a size, a workload or a release is pointing it at another run. Imports summarize-curve.py rather than reimplementing it — above all the lane B latency histogram, which must be differenced against a post-ramp baseline and merged across drivers, since an end-of-rung scrape reports the container's whole lifetime and puts a rung's connect ramp in its published tail. Every rate now prints its p99; a rung whose offer was not met keeps its flag; lane A is labelled as the only lane that replicates; the footer states what is NOT measured (no soak, no node killed under load, no partition, fresh hosts per size so CPU is not comparable across runs, one measurement per rung outside lane A, no comparison against another broker). This exists because the hand-assembled version published 1,047,236 TPS without the ≤7,500 ms p99 beside it." |
| 0077-T2 | ✅ done | 2026-08-28 | "PR #458 — a subscriber whose REST port failed to bind was counted as ZERO, so nodes=5 reported 75% complete against a true 100.6%; attach and resume now use disjoint port ranges and the drained total is read from the containers' own logs, which survive a dead endpoint. PR #466 — lane D declared sessions attached after a fixed 16s sleep; at 7 nodes only 1,553 of 1,680 had registered and publishers spent 40s aiming at sessions that did not exist, which looked like broker message loss on BOTH v1.0.9 and v1.0.10; it now polls mqttd_sessions until the population exists. PR #467 — run-workload.sh loads first-wins, so a corrected LANE_B_SUBS_OVERRIDE=420 appended below a stale =100 was silently discarded and a whole comparison ran the wrong shape; the loader now refuses any file assigning a key twice. PR #470 — BENCH_GIT_REF defaulted to `main`, worth a 24% swing on the same broker (20,394 vs 25,292 msg/s, non-overlapping across three reps each) and enough to invert a version comparison's sign; it now defaults to the release tag. PR #468 — dashboards on by default, since every one of these was found by forensics on static snapshots when a live graph would have shown it." |
| 0077-T3 | ⬜ planned | — | "Lane B's rungs vary the offered rate at a fixed population; a site scales publishers, rate and consumers together, which its shape checks and knee detection cannot express. One site = 1,000 publishers x 30 msg/s x 200B QoS 0 into a $share group of 2 on site/<n>/#, which is the measured SCADA shape rather than an invented one. Ladder doubles (1,2,4,8,16,32) because the knee is the only point of interest; 32 is the ceiling with 5 drivers at ~200k msg/s each, so the bound is the harness and must be printed as such. A rung passes only if its p99 stays under a stated bound. Whole ladder runs inside ONE cluster provisioning, so the cost is per cluster size, not per rung." |
| 0077-T4 | ⬜ planned | — | "Run T3's ladder at 1, 3 and 5 nodes. The deliverable is a single purchasable number — sites per node at acceptable p99 — and whether it holds as nodes are added. If 3 nodes carry fewer than 3x what 1 node carries, the gap IS the cross-node forwarding cost, and it decides per-site edge brokers versus regional clusters for a 3,000-site estate. Depends on T3. Roughly 3 provisionings, EUR 5-8." |
| 0077-T5 | ⬜ planned | — | "Every lane measures steady state. The SCADA workload's sizing constraint is a once-daily burst (90,000 per turbine; 90M messages and 18 GB per site) against a default MQTTD_MAX_QUEUED_MESSAGES of 100,000 — 900x over, so the broker will not buffer it, it will drop by policy. Measure what is held, what is refused and with which reason, and how long recovery takes. Whether the burst is SYNCHRONISED across sites is a 1,500x swing in required capacity (4,500M msg/s over 60s versus 3M staggered across 24h), so the arm sweeps the stagger, not just the size. Depends on T3." |
| 0077-T6 | ⬜ planned | — | "mqtt-bridge is 3,787 lines across ADRs 0025/0059/0060 and the rig has NEVER measured it, though a 3,000-site topology puts it in the path of every message. It is an MQTT client to both sides, not a plugin, so a bridged message is delivered as a normal subscriber and then re-published: expect roughly double the message work plus a serialize/parse round trip and a network leg. Four numbers: per-message overhead bridged vs direct at equal offered rate; added p99 (irrelevant for datalake telemetry, not for the 1% QoS 1 alarm traffic); the throughput ceiling of ONE bridge, which is the one expected to bite — a single client connection against a 30,000 msg/s site, where our measured per-subscriber QoS 0 rate of 10,806 msg/s would mean three bridges per site rather than one; and spool behaviour under link loss, which is the failure sites will actually experience." |
| 0077-T7 | ⬜ planned | — | "Cheapest task here and the highest leverage: it swings the datalake consumer count by 30x (3,000 versus 90,000 writers across the estate). The figure comes from QoS 1 $share runs. Every QoS 0 measurement we hold is BROADCAST fan-out, where a subscriber sustained 10,806 msg/s and the 240-subscriber runs never plateaued (1,201 -> 2,403 -> 4,123 as offered load rose). We have no QoS 0 $share measurement at all, and shared subscriptions add per-group selection work the fan-out path does not do, so the extrapolation crosses a mechanism boundary — exactly the step that produced wrong answers throughout the 2026-08 campaign. Sweep the group size at 30,000 msg/s per site, QoS 0." |
| 0077-T8 | ⬜ planned | — | "Nothing runs longer than about a minute, so there is no evidence about sustained behaviour, memory growth or a 24-hour soak; and no node is killed and no network partitioned while load is running, so the HA claim rests on unit tests rather than on the rig. For the industries this ADR targets, behaviour on node loss is the FIRST question asked and currently has no measured answer. Expensive (hours, not minutes) and therefore sequenced last, but the report footer must keep disclosing the gap until this lands." |
<!-- /status-table:0077 -->

## Why these tasks, in this order

T1 and T2 are done and are the reason the rest is worth running: without a generated
report that carries tail latency, and without the four harness fixes, another campaign
would produce more plausible wrong numbers rather than more evidence.

T7 comes next despite being last-numbered in the SCADA story, because it is the
cheapest measurement here and the only one whose answer changes an architecture
before it is built — 3,000 datalake writers or 90,000 is not a tuning decision.

T3 → T4 → T5 is the SCADA sequence proper: build the lane, get sites-per-node, then
find out whether the daily burst is survivable. T6 answers the other half of the same
architecture question, and can run in parallel with T4 once T3 exists.

T8 is sequenced last because it is the most expensive, not because it is the least
important. Until it lands, every high-availability statement about this broker is
unmeasured, and the report says so on its face.

## The standard each task is held to

A task is `done` when its number is published in the generated report **with its tail
latency, its replication count, and its coverage gap** — not when the measurement
runs. A task that measures something and finds the shape unsuitable is `cut` with the
finding kept, following ADR 0076, where two of three tasks shipped as falsifications.
