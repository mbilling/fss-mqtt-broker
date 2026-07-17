# ADR 0048 — Comparative performance benchmarking (published, reproducible, honest)

- **Status:** Proposed
- **Date:** 2026-07-17
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0048-comparative-benchmarking.md](../delivery/0048-comparative-benchmarking.md) — plan, progress, and changelog
- **Related:** [ADR 0044](0044-release-readiness-assurance.md) (P6's internal micro/hot-path
  baselines and regression gate this extends to end-to-end, cross-broker numbers),
  [ADR 0001](0001-session-durability.md) (the linear-scaling thesis the scaling curve tests),
  [ADR 0015](0015-cluster-shared-subscriptions.md) (cluster-wide shared subscriptions — the
  mechanism that should make throughput scale with nodes), [ADR 0024](0024-deterministic-testing.md)
  (the reproducibility discipline a credible benchmark demands)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0048-comparative-benchmarking.md).

## Context

"Fast, Secure and Scalable" is in the project's own description and "linear horizontal
scalability" is capability claim #1 — yet there is **not one published performance number**.
ADR 0044 P6 added internal micro-benchmarks (codec, replica apply) with a regression gate,
which proves the broker's CPU work does not silently regress, but says nothing about
end-to-end throughput, latency under load, memory per connection, or whether adding a node
actually adds throughput. An external review made the point directly: "'Fast' is in the
name but currently unproven," and named head-to-head numbers vs the incumbents (EMQX,
Mosquitto) as a concrete differentiation play.

Benchmarks are also a credibility trap: a self-run benchmark that flatters the author is
worse than none. The decision here is as much about **method and honesty** as about running
the load.

## Decision

A **reproducible, adversarially-honest benchmark suite** with **published results** ships,
comparing the broker to Mosquitto and EMQX on the dimensions that matter. Five parts:

### 1. The metrics that decide broker selection

Measure what operators actually choose on: **sustained throughput** (msg/s at QoS 0/1/2),
**end-to-end latency distribution** (p50/p99/p999, not just mean), **memory per connection**
at a large idle-connection count (the fan-out/IoT case), and **connection establishment
rate** (mTLS included, since that is our posture). Each is reported with its full
distribution and the load that produced it — never a single headline number.

### 2. The scaling curve — the claim that must be earned

The load driver runs the same workload against a **1-, 3-, and 5-node cluster** and plots
throughput and p99 against node count. "Adding a node adds throughput" (ADR 0001) is only
true if the curve shows it — and cluster-wide shared subscriptions (ADR 0015) are the
mechanism that should make it so. This curve is the single most important, most falsifiable
result; publishing a *flat* curve honestly would be a finding to fix, not a number to bury.

### 3. Reproducible, containerized, and fair

The whole harness is **containerized and scripted** — every broker (ours, Mosquitto, EMQX)
run from its published image with documented, *reasonable* configuration (not ours tuned and
theirs default), the same load tool (an established one, e.g. an MQTT benchmark client),
the same hardware, pinned versions. Anyone can `docker compose up` the harness and reproduce
the table. The methodology, configs, and raw output are published alongside the summary.

### 4. Honesty rules, stated up front

Published results state **broker versions, hardware, config, and date**; report the
**dimensions we lose on** as prominently as the ones we win (a security-first broker that
does mTLS on every connection will pay a connection-setup cost — say so); and never compare
our clustered throughput to a competitor's single node without labeling it. The security
posture is held **constant and disclosed** (e.g. TLS on where the comparison is like-for-like)
so "fast" is never bought by quietly turning security off.

### 5. Published, versioned, and re-run

Results live in `docs/benchmarks/` with the date and versions, linked from the README's
Performance section. The harness runs in the nightly tier (ADR 0044 P4) against our own
broker to catch end-to-end regression between releases; the cross-broker comparison is
re-run and re-published per release (competitor versions move too).

## Consequences

- "Fast" and "linearly scalable" become evidence, not slogans — or the benchmark tells us
  they aren't yet, which is itself the most valuable outcome (a regression/scaling bug found
  before a user finds it).
- The scaling curve directly tests capability claim #1 and the ADR 0015 shared-subscription
  mechanism end to end, complementing the acked-facts *correctness* oracle with a
  *performance* one.
- Publishing numbers we lose on is a cost (and a discipline) — but selective benchmarking is
  transparent and corrosive to a trust-first brand; the honesty rules are the point.
- Maintenance cost is real: competitor images and versions drift, so the comparison is
  re-run per release, not continuously. The *self* benchmark (our broker over time) runs
  nightly and is cheap.

## Alternatives considered

- **Publish only the internal micro-benchmarks (ADR 0044 P6):** honest and reproducible, but
  answers "did our codec regress?" not "is this broker fast, and does it scale?" — the
  questions an adopter asks. Insufficient alone. Kept, and extended here.
- **A one-off marketing benchmark:** easy to make flattering, impossible to trust, and
  exactly the credibility trap a security-first project must avoid. Rejected in favor of a
  reproducible, versioned, self-critical harness.
- **No comparative benchmark (let users measure):** cedes the "fast/scalable" claims to
  doubt and hands the differentiation-vs-incumbents opening to no one. The claims are in the
  product's own name; they must be earned in public. Rejected.
- **Benchmark against every broker (HiveMQ, NanoMQ, VerneMQ, …):** more coverage, more
  maintenance, diminishing returns. Start with the two the market actually compares us to
  (Mosquitto = ubiquity, EMQX = the clustered incumbent); widen if there is demand.
