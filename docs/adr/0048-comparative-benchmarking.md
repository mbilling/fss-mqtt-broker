# ADR 0048 — Comparative performance benchmarking (published, reproducible, honest)

- **Status:** Accepted
- **Date:** 2026-07-17 (accepted 2026-07-23)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0048-comparative-benchmarking.md](../delivery/0048-comparative-benchmarking.md) — plan, progress, and changelog
- **Related:** [ADR 0044](0044-release-readiness-assurance.md) (P6's internal micro/hot-path
  baselines and regression gate this extends to end-to-end, cross-broker numbers),
  [ADR 0001](0001-session-durability.md) (the linear-scaling thesis the scaling curve tests),
  [ADR 0015](0015-cluster-shared-subscriptions.md) (cluster-wide shared subscriptions — the
  mechanism that should make throughput scale with nodes), [ADR 0024](0024-deterministic-testing.md)
  (the reproducibility discipline a credible benchmark demands),
  [ADR 0026](0026-lease-timing-durable-storage.md) + [ADR 0027](0027-replica-group-commit.md)
  (the fsync-bound durable-commit reality that forces the scaling curve onto separate hosts —
  see decision §2), the [7-node single-host post-mortem](../postmortems/2026-07-14-ha-bridge-durable-refused.md)
  (the incident that proved it)

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

**The scaling curve must run on separate hosts with independent disks — never many nodes on
one machine.** This is not a precaution, it is a lesson learned: a
[7-node single-host demo](../postmortems/2026-07-14-ha-bridge-durable-refused.md) drove the
follower `AppendEntries` commit path past its deadline and refused **100% of durable sessions
for 11 hours**. A consensus-backed cluster is **fsync-bound** — the follower replica persists
before answering (ADR 0026/0027 — group-commit was added precisely because per-message
follower fsyncs were the bottleneck) — so co-locating N nodes on one host makes them contend
for the *same* disk queue, and the "cluster" scales *negatively*: adding a node subtracts
throughput. That is a laptop artifact, not a property of the system, and publishing it would
manufacture false evidence *against* the broker. The curve therefore requires one small host
(and one disk) per node — a handful of cloud VMs for a few hours — or it is not published at all.

### 3. Reproducible, containerized, and fair

The whole harness is **containerized and scripted** — every broker (ours, Mosquitto, EMQX)
run from its **pinned published image** with documented, *reasonable* configuration (not ours
tuned and theirs default), the same hardware, pinned versions. The load driver is
**`emqtt-bench`** — deliberately **EMQX's own load tool**: measuring ourselves with a
competitor's instrument is itself an honesty signal (no home-field driver quietly tuned to
flatter us). Each broker is measured in **two disclosed postures — plaintext and TLS/mTLS —**
so the security cost is shown, never hidden or silently avoided. Anyone can `docker compose up`
the harness and reproduce the table; the methodology, configs, and raw output are published
alongside the summary.

### 4. Honesty rules, stated up front

Published results state **broker versions, hardware, config, and date**; report the
**dimensions we lose on** as prominently as the ones we win (a security-first broker that
does mTLS on every connection will pay a connection-setup cost — say so); and never compare
our clustered throughput to a competitor's single node without labeling it. The security
posture is held **constant and disclosed** (e.g. TLS on where the comparison is like-for-like)
so "fast" is never bought by quietly turning security off.

Our **known architectural weak spots are named in the results up front**, not left for a
reader to discover: durable-session ownership is bounded by the lease voter set
(`MQTTD_LEASE_VOTERS`, ADR 0021/0049) — durable-session capacity scales with the voter cap,
not the node count — and the memory footprint is that of a Rust broker with an embedded
observability stack, not a few-MB single-threaded C daemon (Mosquitto wins footprint;
the table says so).

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
- **Cost is bounded and mostly zero.** The work is phased so each step stands alone (harness
  and dev-grade local numbers cost nothing and guide but are never quoted); the only cash
  outlay is renting a dedicated box for an afternoon for the one *publishable* run, plus a
  handful of small VMs for a few hours for the scaling curve. Delivery plan and the
  dev-grade/publishable line are in the [delivery doc](../delivery/0048-comparative-benchmarking.md).

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

### Amendment (2026-07-27): NanoMQ joins the comparison set (ADR 0051)

The "widen if there is demand" clause above has been exercised: the maintainer named
**NanoMQ** as a comparison target during the release-readiness review
([ADR 0051](0051-evaluation-readiness.md)). NanoMQ covers the quadrant the original pair
does not — the lightweight, edge-native broker (and EMQX's own edge sibling), the natural
alternative wherever footprint drives the choice. It joins the containerized harness under
the same rules: pinned published image, reasonable disclosed config, both postures
(plaintext and TLS/mTLS), driven by `emqtt-bench`. The set is now Mosquitto, EMQX, NanoMQ;
it widens further only on further demand. Delivery of the NanoMQ lane is tracked as
0051-T9, feeding the same publication gate as 0048-T4.

### Amendment (2026-08-03): VerneMQ joins the comparison set (ADR 0051)

The demand clause fires a second time, and this one earns its place architecturally:
**VerneMQ** is the closest structural neighbor this broker has — the only other
open-source broker with masterless clustering — which makes it the most informative
head-to-head for the clustering and durability claims specifically. A 2026-07-29
analysis of its design (recorded in ADR 0051's delivery changelog) sets the fairness
terms the honesty rules (§4) require:

- **Durability postures must be disclosed, because the defaults are not comparable.**
  VerneMQ's session queues are node-local and unreplicated (documented: offline messages
  on a dead node are lost); ours are quorum-replicated by default. A throughput table
  that hides this would flatter us. Like-for-like runs pair our
  `MQTTD_DURABLE_SESSIONS=0` posture against their default, and our durable-default
  posture is labeled as carrying a guarantee VerneMQ does not offer.
- **Partition behavior differs by design** (their default fails closed cluster-wide on a
  detected netsplit; we keep serving under CP rules) — benchmark scenarios that touch
  faults must state which regime was active.
- **Their official images are EULA-licensed but free for testing** — benchmark use is
  testing; the image is pinned and the license posture disclosed like every version.

The set is now Mosquitto, EMQX, NanoMQ, VerneMQ; delivery rides the same 0051-T9 lane
and 0048-T4 publication gate.

### Amendment as delivered (2026-08-14): the durable-path lane, and what "never published" means (0048-T5)

Issue [#244](https://github.com/mbilling/fss-mqtt-broker/issues/244) — the 2026-08-13
review panel's strongest independent-agreement finding — exposed a gap this ADR's own
structure had left: **§2 forbids the single-host scaling curve, §5 requires publication,
and the dev-grade rule (delivery phase 2) says laptop numbers are "not published and
never quoted"** — with the result that the durable path, the thing this product's
guarantee actually costs, had **no end-to-end number anywhere**, and the one document
claiming verifiability cited an untracked path for the numbers it did not have.

Two clarifications, both narrowing rather than widening what may be claimed.

**1. The dev-grade rule is about *comparison* and *capacity*, not about our own path.**
Its purpose is that no unpinned, shared, noisy machine produces a "mqttd does N msg/s"
or "mqttd beats X" claim. It was never a reason to have no measurement of our own
durable path at all — and burying one in an untracked directory is what produced the
dangling citation. So: **a measurement of mqttd alone, on a fully disclosed host, may be
published in `docs/benchmarks/` provided it is labelled dev-grade at every point of use,
states its limits in the same breath as its numbers, and is reproducible from a command
in the document.** It supports no comparison and no capacity claim.
[`docs/benchmarks/DURABLE-PATH.md`](../benchmarks/DURABLE-PATH.md) is the first such
artifact: acked QoS 1/2 throughput and p50/p95/p99/p99.9 latency against a real 3-node
quorum with the durable plane on, from `crates/mqttd/tests/durable_bench.rs`.

**2. §2 stands unchanged, and is now enforced by construction.** A fixed-N point is not
a curve. The published artifact prints **one** 3-node configuration and **no**
throughput-versus-node-count series; the driver is parameterised for the multi-host lane
(`MQTTD_BENCH_BROKERS` / `_HEALTH` / `_NODE_IDS` — nothing is spawned when they are set)
and that invocation is documented in full, exercised as code by a preflight test, and
**not run**. T3 stays *planned*: the curve needs one small host per node, as §2 says.

What the lane bought beyond the numbers, and the reason to prefer measurement to
argument: it found that the durable append path pays a device barrier per append with no
group commit (so per-node durable throughput is `1 / commit_time` regardless of
concurrency — ADR 0027's batching exists on the replica writer, not here), that inbound
QoS 2 hangs silently when the publisher's own placement group is owned elsewhere, and
that an idle healthy cluster produces occasional 5 s/10 s publish stalls at the
replication RPC bound. None of those was visible from micro-benchmarks or from `bench/`,
which measures a single non-durable node.

Also as delivered: `scripts/check-readme-facts.py`'s tracked-citation guard widened from
`docs/COMPARISON.md` alone to the README and every `docs/benchmarks/*.md`, so a published
number whose method or harness is not reachable in the repository fails the build.
