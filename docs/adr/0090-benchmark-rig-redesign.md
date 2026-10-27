# ADR 0090 — The benchmark rig measures msg/s and ack latency on one clock, at a fraction of the cost

- **Status:** Accepted
- **Date:** 2026-10-25 (accepted the same day)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0090-benchmark-rig-redesign.md](../delivery/0090-benchmark-rig-redesign.md) — plan, progress, and changelog
- **Issue:** [#1109](https://github.com/mbilling/fss-mqtt-broker/issues/1109)
- **Revisits:** [ADR 0048](0048-comparative-benchmarking.md) (how a rung is run and judged)
- **Related:** [ADR 0076](0076-self-measuring-sharded-store.md) (the data-dir volume probe), the lane E rig under `bench/scale/`

> **Decided 2026-10-25** by the maintainer, after the 1.3.0 release-candidate run. On 10
> nodes the brokers took the whole 270,000 msg/s offer with no message lost and p99 ≤ 500 ms,
> and the rig still certified nothing, because 5.7% of publishes were "late". In their words:
> "we need a solution where we judge the broker only and not the harness"; "I want the
> cheapest and most stable, repeatable setup, which we can run multiple times at lower
> cost"; "I'm interested in ack latency and msg/s"; "why don't we measure latency on the test
> host — it knows exactly how much time is spent"; "drop the site concept, just talk
> messages/s".

## Context

The lane E rig judges a rung through harness-side gates as well as the broker's:

- **Late publishers.** emqtt-bench counts a publish as late (`pub_overrun`) when it starts
  after its slot on an absolute schedule. Our QoS 1 publishers send synchronously, one
  message in flight, at 10 msg/s each. So any PUBACK slower than 100 ms makes publishes late,
  and more than 5% late voids the rung. Nobody chose that 100 ms; it falls out of "600
  publishers per 6,000 msg/s site". It is ten times stricter than the 1 s p99 the rig grades
  on.
- **Cross-host clocks.** A site's publishers and subscribers run on different driver hosts,
  so end-to-end latency needs chrony within a 5 ms error bound. A breach makes the rung
  INVALID EVIDENCE.
- **Steady-state and scrape-window checks** on the drivers' own counters.

And it is expensive:

- **The harness costs more than the cluster.** A 10-node run uses 20 driver hosts (CCX33,
  €0.277/h) beside 10 brokers (CCX23, €0.172/h): €5.55 of €7.27 an hour. Synchronous
  emqtt-bench carries about 16,000 msg/s per driver.
- **Setup is slow.** About 25 minutes per provisioning.
- **Rungs are long.** 10-minute rungs on fixed 6,000 msg/s steps.
- **Host variance.** Runs on different provisionings differ by about one rung (roughly
  15%), so a single run cannot tell a 3% regression from noise.

The 1.3.0 confirmation cost about €20 for three hours and certified one rung per cluster
size.

## Decision

### D1. Three measurements, all of the broker

- **Throughput (msg/s):** the brokers' own received and delivered counters over the
  measured window.
- **Ack latency:** PUBLISH to PUBACK, timed by the publisher on its own monotonic clock.
- **End-to-end latency:** PUBLISH to delivery. The send time is stamped into the payload and
  read by a subscriber in the same process, on the same monotonic clock (D2).
- **Loss:** the identity ledger (every message has an identity; missing, unexpected and
  unacknowledged must be zero) and the brokers' drop counters. Any loss fails a run.

No latency figure crosses hosts, so there is no clock synchronisation, error bound or clock
gate.

### D2. Our own load generator, in Rust

- **Built on the workspace's MQTT codec.**
- **Topology on one clock.** A group's publishers and subscribers live in one process, so
  ack and end-to-end latency are both read from one clock.
- **Paced, open-loop publishers with several messages in flight.** The offered rate does not
  depend on ack time; a slow broker shows up as higher ack latency and as the brokers
  receiving less than the offer, not as "late publishers".
- **Measurement.** HDR histograms for ack and end-to-end latency; the identity ledger; a
  Prometheus endpoint.
- **Generator health as its own check.** Its CPU, network and scheduling headroom are
  reported every run. A run where the generator is saturated is VOID (rerun with more
  generator capacity) and never counted against the broker.
- **Calibration.** For comparisons with other brokers, it is calibrated against emqtt-bench
  (EMQX's tool) at a few rates, and both figures are published. That keeps "our own tool"
  checkable.

### D3. Load is stated in msg/s

There is no "site". A workload fixes the payload size, the QoS, the sessions (durable or
clean), and the subscription shape (a `$share` group of N persistent consumers per topic, or
plain fan-out). The generator derives the publisher count and the rate per publisher from
the target msg/s. Results, verdicts and charts speak of msg/s offered, msg/s carried, ack p99
and end-to-end p99.

### D4. A msg/s ramp, not a ladder

- **Ramp.** Offered msg/s climbs in short steps (60 s measured windows after a warm-up).
- **Each step records** msg/s carried, loss, and ack and end-to-end p50, p99 and p999.
- **Capacity** is the highest step where:
  - the brokers took the full offer (≥ 99% received),
  - nothing was lost,
  - and **ack p99 ≤ 1 s** (the headline).

  The same curve gives capacity at ack p99 ≤ 100 ms and ≤ 250 ms.
- **The step size adapts near the knee**, so the knee is not bound to a step boundary.

### D5. Same-hosts A/B by default, with repeats

- **Comparisons interleave.** A comparison runs the baseline build and the candidate on the
  same servers, alternating (A-B-A-B), three repeats each.
- **What is reported.** The delta and its spread. A difference smaller than the spread is
  reported as no difference.
- **Absolute figures** carry the provisioning they were measured on.

### D6. A pre-built server image

The broker binary is still downloaded and checked by sha256 per run. The generator, the OS
settings and the monitoring are baked into a snapshot, so bring-up takes minutes. No chrony
setup is needed (D1).

### D7. Three tiers

| Tier | Shape | Cost | Use |
|---|---|---|---|
| Local | A/B against main on one machine (extends `durable_bench`) | free | every PR; relative only |
| Cloud, 3 nodes | 3 brokers (CCX23) + 1 generator host (CCX33) | about €0.80/h, 30–60 min | nightly or weekly; absolute figures and A/B |
| Cloud, 10 nodes | 10 brokers + 4 generator hosts | about €2.83/h, about 1 h | release confirmation |

A release confirmation goes from about €20 to about €3.

## Consequences

- **Certification judges only the broker.** Harness trouble can void a run, never fail or
  pass it.
- **The 100 ms artefact and the clock gate disappear.** End-to-end latency becomes exact,
  not an upper bound.
- **Everything is measured again on this rig, and those figures replace the published ones.**
  The generator, the workload shape and the method all change, so the README, EVIDENCE and
  the release notes report only this rig's figures, with no side-by-side against earlier
  measurements (the maintainer, 2026-10-25).
- **A tool of our own** needs its calibration published (D2), or competitor comparisons lose
  credibility.
- **1.3.0 ships with figures from the lane E rig**, re-graded from the broker's side ([ADR 0048](0048-comparative-benchmarking.md),
  amendment of 2026-10-25). Its PUBACK latency per rung is reported from the driver's
  existing histogram.

## Alternatives considered

- **Keep emqtt-bench, change its settings** (more messages in flight, slower publishers).
  Cheaper to start. But the driver cost (20 hosts per 10 brokers) stays, and cross-host
  placement still forces the clock machinery. Rejected as the end state; useful as the
  calibration reference (D2).
- **Keep the ladder, drop the late gate only.** Fixes the voided rungs but none of the cost,
  speed or variance. Taken for 1.3.0 only (the ADR 0048 amendment).
- **Bigger brokers instead of more nodes.** Out of scope here; the redesign makes it cheap
  to test later.
