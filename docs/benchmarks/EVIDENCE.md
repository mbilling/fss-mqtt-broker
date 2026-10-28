# Evidence: what mqttd carries, and how it was measured

**Dated 2026-10-28.** The throughput and latency mqttd publishes. They were measured with
the benchmark rig of [ADR 0090](../adr/0090-benchmark-rig-redesign.md) on the 1.3.0
candidate (main `edfdb49`).

- **The full card:** [RIG2-RESULTS.md](RIG2-RESULTS.md) has the method, the limits, every
  step of every ramp and each ramp's chart.
- **The README's charts** are drawn from [`data/headline.csv`](data/headline.csv) by
  `bench/scale/chart-readme.py`.

Jump to: [the figures](#the-figures) · [per vCPU](#per-vcpu-like-for-like) ·
[published cluster benchmarks](#against-published-cluster-benchmarks) ·
[where competitors win](#where-competitors-win) · [earlier measurements](#earlier-measurements)

## The figures

The setup:
- **Hosts:** Hetzner CCX23 brokers, 4 vCPU each.
- **Load:** 200 B payloads, `$share` groups with one member per broker.
- **Steps:** 3 rounds a step.

For each workload, the table gives the **maximum lossless** step (every round of it lost
nothing), then the highest step at p99 ≤ 1 s:

| workload | 3 nodes | 10 nodes |
|---|---|---|
| durable QoS 1 (ack after fsync and a quorum copy) | 117,800 msg/s · 114,200 at ack p99 ≤ 1 s | 292,200 msg/s · 228,000 at ack p99 ≤ 1 s |
| QoS 1, clean sessions | 270,800 msg/s · 152,000 at ack p99 ≤ 1 s | 875,900 msg/s · 303,800 at ack p99 ≤ 1 s |
| QoS 0 | 340,300 msg/s · 253,200 at end-to-end p99 ≤ 1 s | 973,200 msg/s · 506,300 at end-to-end p99 ≤ 1 s |

Per node, at the maximum lossless step:

| workload | 3 nodes | 10 nodes |
|---|---|---|
| durable QoS 1 | 39.3k | 29.2k |
| QoS 1, clean sessions | 90.3k | 87.6k |
| QoS 0 | 113k | 97.3k |

**How a figure is judged:**
- **Carried:** a step counts only when every one of its rounds lost nothing. That's
  checked against the generators' identity ledgers and the brokers' drop counters, and
  the brokers must take at least 99% of the offer.
- **Throughput** comes from the brokers' own counters.
- **Latency** is measured on the load generator's one clock: ack latency for QoS 1,
  end-to-end latency for QoS 0. It is the mean of the rounds' p99.
- **Generators are calibrated:** a load generator that can't keep up voids its round,
  and a host that keeps voiding is replaced. So no figure here is the harness's.

**Scaling from 3 to 10 nodes:**
- **QoS 1 with clean sessions and QoS 0** hold close to their per-node rate.
- **Durable QoS 1** falls from 39.3k to 29.2k per node. At 10 nodes, more of each
  message's replication and forwarding crosses between nodes. It's being profiled.

## Per vCPU, like for like

![Durable QoS 1 per vCPU: mqttd against HiveMQ 4.18's published figure](img/readme-durable-per-vcpu.svg)

A vCPU is not a fixed unit, so every figure here says what one is:
- **An SMT thread** on our Hetzner CCX23s and on HiveMQ's AWS `m6a`. Both are AMD EPYC
  Milan, with two vCPUs per physical core. Per vCPU, these compare directly.
- **A whole core** on EMQX 5.0's AWS `c6g` (Graviton2, no SMT). A core does more work
  than one thread, so per vCPU it is flattered against an SMT host. Compare it per
  physical core instead.

Figures are only compared inside one workload class. A durable, replicated QoS 1 message
costs several times a QoS 0 one, and connection count dominates the large vendor runs.

| class | mqttd (3 nodes, 12 vCPU, at p99 ≤ 1 s) | best comparable | ratio |
|---|---|---|---|
| cluster, durable QoS 1, 2 copies | 9,517 /vCPU (114,200 msg/s, fsync + replication) | HiveMQ 4.18 2,812 /vCPU (published) | **3.4×** |
| cluster, QoS 1, not durable | 12,667 /vCPU (152,000 msg/s, clean sessions) | EMQX 5.0 679 /core (published; compare per core) | — |

**Why HiveMQ 4.18 is the closest comparison:**
- **Durable, at the same copy count.** HiveMQ publishes its benchmarks on its default
  configuration, which persists sessions and keeps **2 copies** of all persistent data
  ([`replica-count` 2](https://docs.hivemq.com/hivemq/latest/user-guide/cluster.html)).
  So its 270,000 QoS 1 msg/s on 96 vCPU is durable, like mqttd's figure at its default
  of 2 replicas.
- **The same kind of vCPU:** an EPYC Milan SMT thread on both.
- **One gap:** HiveMQ's payload is not published.

Data, with every source: [per-vcpu.csv](data/per-vcpu.csv).

## Against published cluster benchmarks

HiveMQ and EMQX publish large-cluster results around 1M msg/s. **We have not reproduced
these.** The table puts their published figures next to ours, with every difference that
matters shown alongside:

| | **mqttd** (ours, 2026-10-28) | HiveMQ 4.11 (2023) | EMQX 5.0 (2022) | EMQX 4.3 (2021) | HiveMQ 4.18 (2023) |
|---|---|---|---|---|---|
| throughput | **973,200** QoS 0, maximum lossless | 1M QoS 1 PUBLISH/s peak | > 1M QoS 1 in and out | 505k QoS 0, 1:1 | 270k QoS 1, durable (default: 2 copies) |
| cluster | **10 × 4 vCPU = 40 vCPU** | 40 nodes (AWS; instance type not published) | 23 × `c6g.metal` = 1,472 cores | 5 × 32 cores = 160 cores | 3 × `m6a.8xlarge` = 96 vCPU |
| per vCPU | **~24,300 msg/s** | — | ~680 in | ~3,200 | ~2,800 |
| one vCPU is | SMT thread (EPYC Milan) | not published | whole core (Graviton2) | not published (per core) | SMT thread (EPYC Milan) |
| per physical core | **~48,700 msg/s** | — | ~680 in | ~3,200 | ~5,600 |
| connections | 11,200 | 200M | 100M | 10M | 20k |
| payload · pattern | 200 B · `$share`, one member per broker | 16 B · 20M pubs → 180M subs | 256 B · 1:1 wildcard | 50 B · 1:1 | not published |

**What the differences mean.**
- **Connection count.** The two ~1M headline runs carried thousands of times more
  connections than ours, and connection count dominates their resource use: HiveMQ
  reports about 13 KB of heap per connection, and EMQX uses 90% of RAM at 100M
  connections.
- **QoS.** HiveMQ's runs and EMQX 5.0's are QoS 1; ours here is QoS 0.
- **So the per-vCPU rows are not a like-for-like ratio.** They show the hardware each
  system needs to carry a message, at the connection counts shown.
- **The closest comparison is HiveMQ 4.18,** durable at 2 copies on the same CPU:
  [per vCPU ↑](#per-vcpu-like-for-like).

What the table does not show, and where the others lead:
- **connection scale:** 100–200M connections is a regime we have not measured
- **track record:** mqttd has no production users yet

What it does show:
- **an open cluster:** mqttd's clustering is free and Apache-2.0. HiveMQ clustering is
  commercial, and EMQX 5.x/6.x clustering is BSL.
- **durability by default:** sessions are quorum-replicated without any extra
  configuration.

Sources: [HiveMQ 200M connections](https://www.hivemq.com/whitepaper/achieving-200-mil-concurrent-connections-with-hivemq/) ·
[HiveMQ 4.18 throughput](https://www.hivemq.com/blog/hivemq-4-18-delivers-higher-mqtt-throughput/) ·
[EMQX 5.0 100M connections](https://www.emqx.com/en/blog/reaching-100m-mqtt-connections-with-emqx-5-0) ·
[EMQX 4.3 10M connections](https://www.emqx.com/en/resources/emqx-v-4-3-0-ten-million-connections-performance-test-report).

## Where competitors win

| dimension | winner | detail |
|---|---|---|
| Footprint | NanoMQ, Mosquitto | ~4.6 MB image / few-MB daemon vs ~14 MB |
| Track record | all of them | mqttd: **no production users** |
| Hard memory cap | Mosquitto | mqttd: watermark + brownout only |
| Feature surface | EMQX | dashboard, rule-engine data sinks, MQTT-SN/CoAP |
| Connection scale | HiveMQ, EMQX | published 200M / 100M connections; mqttd's runs used 7,200 to 18,400 |
| Durable scale-out per node | not yet mqttd | durable QoS 1 falls from 39.3k to 29.2k msg/s per node from 3 to 10 nodes ([↑](#the-figures)); plan [#537](https://github.com/mbilling/fss-mqtt-broker/issues/537) |
| Not covered | — | no other broker has been measured on this rig yet; vendor cluster figures are published, not reproduced |

The full list of mqttd's limitations is in [GUIDE.md § Limitations](../../GUIDE.md#limitations).

## Earlier measurements

These were taken on an earlier rig, with a different load tool, method and verdicts.
They are kept as the record of what was measured at the time. Their figures are not
comparable with the ones above:
- **Single-node comparison** with Mosquitto, EMQX and HiveMQ CE, 2026-09-17:
  [SINGLE-NODE-COMPARISON.md](SINGLE-NODE-COMPARISON.md).
- **Scaling curves:** [SCALE-CURVE.md](SCALE-CURVE.md) and
  [QOS1-SCALE-CURVE.md](QOS1-SCALE-CURVE.md).
- **The durable path on one host,** 2026-08-14: [DURABLE-PATH.md](DURABLE-PATH.md).
- **Dated run cards:** `bench/scale/*.md`.
