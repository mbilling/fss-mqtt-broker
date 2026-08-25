# The scaling curve — throughput and p99 vs node count

**STATUS: NO PUBLISHED RUN YET.** This document is the record's template: the
method, validity rules and disclosure obligations are fixed here *before* the
first paid run, so the numbers cannot bend the rules they are judged by. The
tables below are empty until a full 1/3/5 run on real hardware fills them, at
which point this header gains the "Verified against" stamp the other benchmark
records carry and delivery tasks 0048-T3/T4 flip to done.

Mandated by [ADR 0048 §2](../adr/0048-comparative-benchmarking.md): the same
workload against 1-, 3- and 5-node clusters, one small cloud host and one disk
per node, throughput and p99 vs node count. **A flat curve is a finding to fix,
not a number to bury.**

## Read this first

- **What this is:** mqttd measured against itself at three cluster sizes on
  dedicated-vCPU Hetzner Cloud hosts (one broker per host, local NVMe, spread
  placement group), driven from separate load-generator hosts by
  `bench/scale/run.sh`. No competitor appears here — cross-broker comparison is
  `docs/COMPARISON.md`'s job under ADR 0048 §3/§4.
- **Two curves, deliberately** ([ADR 0049](../adr/0049-voter-eligible-durable-ownership.md),
  and `docs/benchmarks/DURABLE-PATH.md`'s scope note): the durable QoS 1 curve is
  fsync- and ownership-bound; the non-durable `$share` fan-out curve is
  routing- and CPU-bound. Neither substitutes for the other, and the idle
  connection point is a third, memory-bound axis.
- **Latency honesty differs by lane.** Lane A percentiles are exact, computed
  from per-message ack RTTs in `crates/mqttd/tests/durable_bench.rs`. Lane B
  percentiles are emqtt-bench histogram **bucket upper bounds** ("p99 ≤ X ms"),
  merged across drivers by summing bucket counts — coarse, but incapable of
  flattering.
- **Postures are disclosed per lane:** plaintext on the private network, and
  mTLS at a reference rung. Lanes B/C run the broker with the durable plane off
  (parity with `bench/`'s comparative posture); lane A runs it on.

## The workload, and what "the same workload" means

The same **total** client population, topics, payload and offered-load ladder at
every node count, with clients spread uniformly across all brokers. Not
per-node scaling — scaling the offer with N would assume the conclusion.

| Lane | Population (identical at N=1/3/5) | Measures |
|---|---|---|
| A durable QoS 1 | 48 closed-loop publishers × window 8 + 48 durable subscribers, 256 B, sessions round-robin across all owners (`MQTTD_BENCH_SPREAD=1`) | acked durable msg/s, exact p99 |
| B `$share` fan-out | 4 800 publishers → `bench/%i`, one shared group `$share/g1/bench/#` of 600 subscribers, QoS 1, 256 B; ladder 20/50/100/200/400/600/800k msg/s (rungs are exact divisors: emqtt-bench paces per client in whole ms, so rate scales by adding publishers, never sub-ms intervals) | sustained knee, bucket-bound p99 |
| C idle connections | 50 000 connections, ramp 2 500/s, hold 120 s | establishment, KiB per connection |

Throughput vs node count cannot come from one fixed offered rate (every size
that keeps up reads identically), so the plotted point is the **knee**: the
highest rung where the drivers achieved ≥ 97 % of the offer and subscribers
received ≥ 99 % of what was sent. p99 is compared at the highest rung all sizes
sustain, and at each size's own knee.

## A run judges itself

Mechanical, enforced by `bench/scale/run-curve.sh` and
`bench/scale/summarize-curve.py` — not by the author's discipline:

- Every broker host runs the fsync **barrier probe** before any lane
  (`DURABLE-PATH.md`'s prerequisite); a size with no probe output renders as
  UNINTERPRETABLE and its Curve 1 row is refused.
- Lane A carries `durable_bench`'s own verdicts (violations/caveats) verbatim,
  including the multi-host caveat that the in-driver driver-bound check cannot
  run; every host's CPU is sampled via `mpstat` during every rung instead.
- Broker-side counter deltas cross-check driver-reported totals (±2 %).
- Driver-limited rungs are printed struck from knee detection, never presented
  as a broker limit.
- Preflight captures each node's `/readyz` and `/statusz` — the known 5-node
  replica-convergence plateau is quoted beside the 5-node point, not tolerated
  silently.

## Host, build, configuration

*(filled by the run: Hetzner instance types, location, kernel, per-broker disk
and its measured barrier floor, mqttd release version and checksum, the full
rendered broker environment — from `bench/scale/templates/mqttd.env.tmpl` —
emqtt-bench 0.6.3, driver specs, chrony offsets, date.)*

## Per-host durability barrier floors

*(table from `bench/scale/summarize-curve.py` — barriers/s per broker host, the
number Curve 1 is bounded by.)*

## Curve 1 — durable QoS 1, closed loop

*(table + chart. The N=1 point is labeled **single-copy acks**: a majority of a
1-element replica set is 1, so its acks carry a different guarantee than the
3- and 5-node points'.)*

> **Known blocker, tracked:** spread ownership currently stalls durable acks on
> groups owned by non-founder nodes
> ([#358](https://github.com/mbilling/fss-mqtt-broker/issues/358)) — found by
> this harness before the first paid run. Until it is resolved, the 3- and
> 5-node durable points would measure that defect, which is the publishable
> outcome ADR 0048 §2 anticipates ("a finding to fix").

## Curve 2 — non-durable `$share` fan-out

*(ladder table per size, knee per size, chart, and the shared-group delivery
balance across subscribers — the ADR 0015 mechanism observed, not assumed.)*

## Connections at 50 000

*(establishment rate, RSS growth, KiB/connection vs node count.)*

## Losing dimensions, stated first

Per ADR 0048 §4, the unflattering facts lead:

- **Durable-session ownership scales with the lease-voter cap, not node count**
  (`MQTTD_LEASE_VOTERS`, default 5 — ADR 0021/0049). 1/3/5 nodes is the
  *entire uncapped regime*: the durable curve cannot be extended past five
  nodes by adding nodes.
- The 5-node cluster's replica groups plateau below `current == tracked`
  (75–85 % observed); the 5-node point runs under that printed disclosure.
- Lane B p99s are bucket bounds, and cross-driver clock skew bounds the finest
  readable bucket; the broker-side `deliver_latency_seconds` histogram is the
  skew-immune cross-check.
- Memory per connection is expected to lose to a slim C daemon (ADR 0048 §4
  names it); the number is printed either way.

## Reproducing everything above

```sh
cd bench/scale
export HCLOUD_TOKEN=...   # Read & Write token, dedicated Hetzner project
./run.sh smoke            # ~20 min, <€0.50 — proves the rig end to end
./run.sh full             # fresh 1-, 3- and 5-node clusters, ~€2
python3 summarize-curve.py .runs/<stamp>/results
```

The rig is `bench/scale/run.sh` (orchestration), `bench/scale/terraform/`
(hosts), `bench/scale/bootstrap-cluster.sh` (secrets + founder-first bring-up),
`bench/scale/run-curve.sh` (lanes) — see `bench/scale/README.md`.

## Related

- `docs/benchmarks/DURABLE-PATH.md` — the single-host durable floor this curve
  extends to real hosts.
- `docs/adr/0048-comparative-benchmarking.md` §2 — the mandate and its honesty
  rules; `docs/delivery/0048-comparative-benchmarking.md` T3/T4 track this work.
