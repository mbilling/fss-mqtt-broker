# mqttd — a security-first, cluster-native MQTT broker

[![CI](https://img.shields.io/github/actions/workflow/status/mbilling/fss-mqtt-broker/ci.yml?branch=main&label=CI)](https://github.com/mbilling/fss-mqtt-broker/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/mbilling/fss-mqtt-broker)](https://github.com/mbilling/fss-mqtt-broker/releases)
[![License](https://img.shields.io/github/license/mbilling/fss-mqtt-broker)](LICENSE)
[![Container image](https://img.shields.io/badge/ghcr.io-fss--mqtt--broker-blue?logo=docker)](https://github.com/mbilling/fss-mqtt-broker/pkgs/container/fss-mqtt-broker)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/mbilling/fss-mqtt-broker/badge)](https://scorecard.dev/viewer/?uri=github.com/mbilling/fss-mqtt-broker)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/14161/badge)](https://www.bestpractices.dev/projects/14161)

> **Open == Enterprise: every feature, no paid tier.**
>
> MQTT 3.1.1 + 5.0 · Rust · durable sessions quorum-replicated **by default** · Apache-2.0, everything.
>
> **75,000 msg/s** on one 4-vCPU node at p99 ≤ 1 s — **1.7× Mosquitto and EMQX, 2.5× HiveMQ CE**, same host, EMQX's own load tool. [Numbers ↓](#by-the-numbers)
>
> **Linear scale-out, 3 → 10 nodes:**
> - **QoS 0:** ~114,000 msg/s **per 4-vCPU node** — **1.14M msg/s on 10 nodes, 40 vCPU in total**; 1.2M at p99 ≤ 2 s, no loss.
> - **QoS 1, clean sessions:** ~39,000–42,000 msg/s per node — **390,000–420,000 msg/s on 10 nodes**.
> - **Durable QoS 1** (ack after fsync + replication): **90,000 msg/s on 3 nodes, 12 vCPU** ([↓](#durable-and-qos-1-scale-out)).
>
> All three use shared subscriptions. [Scale-out ↓](#cluster-scale-out-qos-0-shared-subscriptions)

---

## Why mqttd

| | mqttd | the others |
|---|---|---|
| **Single-node throughput** | 75k msg/s at p99 ≤ 1 s, 30% CPU idle | 45k / 45k / 30k (Mosquitto / EMQX / HiveMQ CE) |
| **Cluster scale-out** | flat per node from 3 → 10 nodes: ~114k msg/s QoS 0 and ~39k QoS 1 (clean sessions) per 4-vCPU node; ~1M msg/s QoS 0 on **40 vCPU** | vendor-published ~1M msg/s runs: HiveMQ on 40 nodes, EMQX on 1,472 cores ([different workloads ↓](#against-published-cluster-benchmarks)) |
| **Per vCPU** (same kind of vCPU) | 7,500 msg/s durable QoS 1, 2 copies | HiveMQ 4.18: 2,812 durable QoS 1, 2 copies ([like for like ↓](#per-vcpu-like-for-like)) |
| **Durable sessions** | quorum-replicated, **default**; acked QoS 1/2 survives node loss, even in flight | Mosquitto/NanoMQ single-node · VerneMQ loses queues on node death · EMQX opt-in |
| **Revocation** | policy reload **evicts live sessions** | not documented by any compared broker |
| **Secure by default** | TLS 1.3, mTLS/OIDC, deny-by-default ACL, hash-chained audit; insecure = opt-in + `INSECURE:` log | varies; NanoMQ and Mosquitto < 2.0 allow anonymous by default |
| **Clustering** | free, Apache-2.0, signed reproducible builds | EMQX 💰 BSL · VerneMQ 💰 EULA binaries · HiveMQ CE single-node |
| **Checkable claims** | every capability → task → evidence ([dashboard](docs/delivery/STATUS.md)); losing cells printed | — |

---

## By the numbers

**How a rung is judged** ([ADR 0048](docs/adr/0048-comparative-benchmarking.md#amendment-2026-09-27-a-rung-fails-only-on-loss-p99-is-graded-green--yellow--red)): a rung **fails only if it loses messages**. A lossless rung is graded by p99 — **GREEN ≤ 1 s (certified)**, **YELLOW ≤ 5 s**, **RED** above. Every knee below is the highest GREEN rung; the YELLOW figure beside it is the latency around the knee, for workloads that can take it.

Sources: [SINGLE-NODE-COMPARISON.md](docs/benchmarks/SINGLE-NODE-COMPARISON.md) (2026-09-16, one Hetzner CCX23, 4 vCPU / 16 GB, brokers in sequence, untuned) · [knee-3-5-7-10.md](bench/scale/knee-3-5-7-10.md) (2026-09-26, QoS 0 scale-out, 3 → 10 nodes on one provisioning) · [SCALE-CURVE.md](docs/benchmarks/SCALE-CURVE.md) (one host + NVMe per broker) · [0080-replicas-ab-n3.md](bench/scale/0080-replicas-ab-n3.md) (2026-09-30, durable QoS 1 on 3 nodes) · [per-vcpu.csv](docs/benchmarks/data/per-vcpu.csv) (every per-vCPU figure and its source).

**Single-node knee** — highest GREEN rate, p99 ≤ 1 s (1:1 QoS 0, 200 B). Qualified at ≥ 99% delivered before the zero-loss rule; the raw runs are not retained, so zero loss is not re-checked:

```text
mqttd      1.0.17   ██████████████████████████████████████████████████  75,000 msg/s
Mosquitto  2.0.20   ██████████████████████████████                      45,000
EMQX       5.8.6    ██████████████████████████████                      45,000
HiveMQ CE  2024.3   ████████████████████                                30,000
```

| broker | knee | p99 at knee | CPU idle | RSS | what limits it |
|---|---|---|---|---|---|
| **mqttd** | **75,000** | ≤ 500 ms | **30%** | 133 MiB | tail latency |
| Mosquitto | 45,000 | ≤ 100 ms | 70% | **18 MiB** | single thread (1 of 4 cores) |
| EMQX | 45,000 | ≤ 500 ms | 2% | 434 MiB | CPU |
| HiveMQ CE | 30,000 | ≤ 100 ms | 5% | 927 MiB | JVM heap → OOM crash |

**Latency distribution at 45,000 msg/s** (Mosquitto's and EMQX's knee) — share delivered within:

| | 1 ms | 10 ms | 25 ms | 100 ms | 1 s |
|---|---|---|---|---|---|
| **mqttd** | **39.9%** | **97.8%** | **100%** | 100% | 100% |
| Mosquitto | 0.1% | 30.6% | 57.8% | 99.7% | 100% |
| EMQX | 0.1% | 14.9% | 40.6% | 82.8% | 100% |
| HiveMQ CE | 0.0% | 0.4% | 2.3% | 25.4% | 61.1% |

![Latency distribution at 45,000 msg/s](docs/benchmarks/img/latency-45000.svg)

**Overload at 150,000 msg/s:**

| | mqttd | Mosquitto | EMQX | HiveMQ CE |
|---|---|---|---|---|
| behaviour | queues, drains at **239k msg/s** (1.6× offer) | sheds, 38% of offer | queues | JVM dies |
| peak memory | 3.8 GiB | 991 MiB | 3.6 GiB | 12 GiB |
| memory returned | **95%** | **98%** | 24% | — |

![mqttd at 150,000 msg/s](docs/benchmarks/img/timeline-mqttd-150k.svg)

### Cluster scale-out, QoS 0 shared subscriptions

The highest rate each cluster size carries GREEN — p99 ≤ 1 s, zero loss —
and the YELLOW rung above it (1:1 via `$share`, 200 B, 10 consumers per site). All four sizes ran on
**one provisioning**, the same hosts re-formed 10 → 7 → 5 → 3 → 10. Each size had
2 load generators per broker and ladders matched in per-node offer, so a driver
carries the same load at every size:

```text
3 nodes  ██████████·······················  ≥ 360k msg/s  ≥ 120k/node   every rung GREEN — knee above
5 nodes  ████████████████·················    570k         114k/node     YELLOW 600k   (p99 ≤ 2 s)
7 nodes  ███████████████████████··········    810k         116k/node     YELLOW 840k   (p99 ≤ 2 s)
10 nodes █████████████████████████████████  1,140k         114k/node     YELLOW 1,200k (p99 ≤ 2 s)   highest GREEN rung (see below)
```

![QoS 0 scale-out: msg/s at the knee vs nodes](docs/benchmarks/img/scale-out-qos0.svg)

**Per-node capacity is flat from 3 to 10 nodes.** 5, 7 and 10 nodes are all GREEN
at ~114k msg/s per node and YELLOW at 120k (p99 ≤ 2 s, nothing lost). 3 nodes is GREEN at 120k, which is one ladder
step (6–10k/node) above the others and inside the run's declared ±0.1 resolution.
Crossing was 0.00% on every broker of every rung, and ingress stayed balanced
(busiest broker ≤ 1.01× the mean). A closing 10-node arm on the same hosts matched
the opening one within 0.002%.

Read strictly, by the rules fixed before the run:
- **3 nodes' figure is a floor.** Its whole ladder passed.
- **10 nodes is uncertified.** Near saturation, one busy broker stopped answering
  membership probes and was declared dead by its peers for ~30 s, at 108k/node on
  10 nodes and at 120k/node on 7. The run's own mesh gate therefore fails the
  10-node arm. No message was lost (0 dropped, 0 pending), but this is a real
  membership flap under load and is tracked as a limitation.
- The 10-node ladder also had one NOT STEADY rung at 96k/node, which received the
  full offer but never held the per-site band. Its strictly certified figure is
  900k.

Method, the rules fixed before the run, and the commands to reproduce it:
[knee-3-5-7-10.md](bench/scale/knee-3-5-7-10.md#reproduce).

The earlier "5 → 7 nodes plateaus" reading came from a benchmark gap: one of 7
brokers had no local subscriber, so 14% of publishes crossed the cluster. It
does not reproduce under a controlled harness.

### Durable and QoS 1 scale-out

**Durable QoS 1 on 3 nodes** (every ack waits for fsync + replication; persistent
`$share` consumers; 3 × CCX23, 12 vCPU, log store): **90,000 msg/s GREEN**, p99
≤ 100 ms, no loss, **7,500 msg/s per vCPU**. The same at 2 and 3 replicas; 2
replicas carried 122,594 msg/s at most against 104,716 for 3. Measured 2026-09-30
on an unreleased build of main ([0080-replicas-ab-n3.md](bench/scale/0080-replicas-ab-n3.md)).

The earlier curve below is a different harness: closed loop, 48 publishers with 8
messages in flight each, so its rate is set by latency at that concurrency, not by
capacity (v1.0.5, [SCALE-CURVE.md](docs/benchmarks/SCALE-CURVE.md)):

```text
1 node  ████████████████████░░░░░░░░░░░░░   8.5k msg/s   p99 90 ms
3 nodes ████████████████████░░░░░░░░░░░░░   8.6k         p99 82 ms   quorum tax absorbed
5 nodes █████████████████████████████████  13.9k         p99 56 ms   1.63× one node
```

**Cluster scale-out, QoS 1 shared subscriptions** (clean sessions, at-least-once
both ways — no durable session, so this is the routing path, not the fsync one):

```text
3 nodes  ██████████░░░░░░░░░░░░░░░░░░░░░░░  120k msg/s   25.9 MB/s   p99 ≤ 5 ms     40.0k/node
5 nodes  ███████████████░░░░░░░░░░░░░░░░░░  180k         38.9 MB/s   p99 ≤ 5 ms     36.0k/node
7 nodes  ███████████████████████░░░░░░░░░░  270k         58.3 MB/s   p99 ≤ 500 ms   38.6k/node   knee: 300k not carried
10 nodes █████████████████████████████████  390k         84.2 MB/s   p99 ≤ 500 ms   39.0k/node   knee: 420k–450k
```

**Per-node QoS 1 capacity holds out to 10 nodes.**
- **3 and 5 nodes:** separate provisionings.
- **7 and 10 nodes:** one provisioning on 2026-09-26, with 20 consumers per site so
  every broker holds a local member. Crossing was 0.00% throughout, and a closing
  10-node arm matched the opening one within 0.001%.
- **Certification:** 2 of 3 repetitions are certified at 7 nodes and 1 of 3 at 10.
  The rest were invalid on the load driver's own endpoint scrapes, while every
  broker received the full offer.

![QoS 1 scale-out: msg/s vs nodes](docs/benchmarks/img/scale-out-qos1.svg)

Latency is not what limits QoS 1: every rung here is GREEN, p99 ≤ 500 ms, and
nothing was lost. The knee is backpressure. 7 nodes carries 270k and does not carry
300k (42.9k/node) because publishers ran late. At QoS 1 that means slow acks from
the brokers; every load generator was still ≥ 66% idle. 10 nodes showed nothing
wrong at 420k (42k/node), and publishers ran late at 450k (45k/node), so its knee is
at or just above 7 nodes'. The 420k rungs are not certified: the load driver's metrics endpoint renders too slowly
under that load for the 60 s window's evidence check. Details and every
rung: [QOS1-SCALE-CURVE.md](docs/benchmarks/QOS1-SCALE-CURVE.md#7-and-10-nodes--one-provisioning-2026-09-26);
reproduce: [QOS1-SCALE-CURVE.md § Reproduction](docs/benchmarks/QOS1-SCALE-CURVE.md#reproduction).


MB/s is application payload (216 B/message). **The constraint is packets, not
bytes** — measured: holding 60,000 msg/s and growing the payload to 8 KiB reached
**492 MB/s** (1,576 Mbit/s per node) while softirq stayed flat at 24–34% across
the whole 38× increase. So **small messages are the expensive case** — size a
cluster on messages per second, not megabytes. 492 MB/s is a floor; nothing was
saturated there.

Three repetitions plus a passing control at each size, delivery matching offer to
within 2 msg/s, 144M message identities reconciled with zero lost. **These are
what the rig carried, and the rig ran out first**: one core of each broker host
saturates on *network interrupt handling* (54.6% softirq, 1.2% idle, while its
three siblings sit 33–39% idle), and mqttd's own hub loop was at **0.46 of a
core** — roughly 2× headroom — measured by the broker's own dispatch metric
rather than by sampling CPU. Spreading that softirq is a settled dead end for capacity
(#505/#507: +1.9%, noise); the cause is **cross-node forwarding**, which is why
`shared_prefer_local` is the default (#508/#511, +42% at N=5). Full method, the
per-core breakdown, and two earlier revisions of this claim that were wrong:
[QOS1-SCALE-CURVE.md](docs/benchmarks/QOS1-SCALE-CURVE.md).

### Per vCPU, like for like

![Throughput per vCPU, like for like](docs/benchmarks/img/per-vcpu.svg)

A vCPU is not a fixed unit, so every figure here says what one is:
- **An SMT thread** on our Hetzner CCX23s and on HiveMQ's AWS `m6a`: both AMD EPYC
  Milan, two vCPUs per physical core. Per vCPU, these compare directly.
- **A whole core** on EMQX 5.0's AWS `c6g` (Graviton2, no SMT). A core does more work
  than one thread, so per vCPU it is flattered against an SMT host; compare it per
  physical core instead (the chart shows both).

Figures are only compared inside one workload class. A durable, replicated QoS 1
message costs several times a QoS 0 one, and connection count dominates the large
vendor runs.

| class | mqttd | best comparable | ratio |
|---|---|---|---|
| single node, QoS 0, **same host, measured by us** | 18,750 /vCPU | EMQX 5.8.6 11,250 · HiveMQ CE 7,500 | 1.7× · 2.5× |
| cluster, QoS 1, not durable | 9,750 /vCPU (clean sessions) | EMQX 5.0 679 /core (published; compare per core) | — |
| cluster, durable QoS 1, 2 copies | 7,500 /vCPU (fsync + replication) | HiveMQ 4.18 2,812 /vCPU (published) | **2.7×** |

Mosquitto (11,250 /vCPU on the same host) is single-threaded: it uses one of the
four vCPUs, so per vCPU it undersells what one thread does. HiveMQ publishes its
benchmarks on its default configuration, which persists sessions and keeps **2
copies** of all persistent data ([`replica-count` 2](https://docs.hivemq.com/hivemq/latest/user-guide/cluster.html)).
So HiveMQ 4.18's 270,000 QoS 1 msg/s on 96 vCPU is durable, like mqttd's figure at
its default of 2 replicas: the same copy count, on the same kind of thread (EPYC
Milan SMT). Its payload is not published. Data, with every source:
[per-vcpu.csv](docs/benchmarks/data/per-vcpu.csv); chart:
`bench/scale/chart-per-vcpu.py`.

### Against published cluster benchmarks

HiveMQ and EMQX publish large-cluster results around 1M msg/s. **We have not
reproduced these.** The table puts their published figures next to ours, with
every difference that matters shown alongside:

| | **mqttd** (ours, 2026-09-26) | HiveMQ 4.11 (2023) | EMQX 5.0 (2022) | EMQX 4.3 (2021) | HiveMQ 4.18 (2023) |
|---|---|---|---|---|---|
| throughput | **1.14M** QoS 0 (810k certified at 7 nodes) | 1M QoS 1 PUBLISH/s peak | > 1M QoS 1 in and out | 505k QoS 0, 1:1 | 270k QoS 1, durable (default: 2 copies) |
| cluster | **10 × 4 vCPU = 40 vCPU** | 40 nodes (AWS; instance type not published) | 23 × `c6g.metal` = 1,472 cores | 5 × 32 cores = 160 cores | 3 × `m6a.8xlarge` = 96 vCPU |
| per vCPU | **~28,500 msg/s** | — | ~680 in | ~3,200 | ~2,800 |
| one vCPU is | SMT thread (EPYC Milan) | not published | whole core (Graviton2) | not published (per core) | SMT thread (EPYC Milan) |
| per physical core | **~57,000 msg/s** | — | ~680 in | ~3,200 | ~5,600 |
| connections | ~45k | 200M | 100M | 10M | 20k |
| payload · pattern | 200 B · 1:1 `$share` | 16 B · 20M pubs → 180M subs | 256 B · 1:1 wildcard | 50 B · 1:1 | not published |
| CPU at that rate | busiest broker 11–19% idle | 75–80% | 97% | 56–68% | not published |

**What the differences mean.** The two ~1M headline runs carried **2,000–4,000×
more connections** than ours (EMQX 4.3: ~220×), and connection count dominates
their resource use: HiveMQ reports about 13 KB of heap per connection, and EMQX
uses 90% of RAM at 100M connections. HiveMQ's runs and EMQX 5.0's are QoS 1;
ours is QoS 0. So the per-vCPU rows are **not a like-for-like ratio**. It is a statement about the hardware each system needs to
carry a message at the connection counts shown.

The closest comparison is HiveMQ 4.18: 20k clients, QoS 1 on HiveMQ's default
configuration (persistent, 2 copies), and the same CPU (EPYC Milan, a vCPU is an
SMT thread on both). mqttd's durable QoS 1 at 2 copies is 90,000 msg/s on 3 × 4
vCPU: **7,500 msg/s per vCPU against HiveMQ's ~2,800**, 2.7× ([per vCPU ↑](#per-vcpu-like-for-like)).
HiveMQ's payload is not published.

What the table does not show, and where the others lead:
- **connection scale:** 100–200M connections is a regime we have not measured
- **track record:** mqttd has no production users yet

What it does show:
- **an open cluster:** mqttd's clustering is free and Apache-2.0. HiveMQ
  clustering is commercial, and EMQX 5.x/6.x clustering is BSL.
- **durability by default:** sessions are quorum-replicated without any extra
  configuration.

Sources: [HiveMQ 200M connections](https://www.hivemq.com/whitepaper/achieving-200-mil-concurrent-connections-with-hivemq/) ·
[HiveMQ 4.18 throughput](https://www.hivemq.com/blog/hivemq-4-18-delivers-higher-mqtt-throughput/) ·
[EMQX 5.0 100M connections](https://www.emqx.com/en/blog/reaching-100m-mqtt-connections-with-emqx-5-0) ·
[EMQX 4.3 10M connections](https://www.emqx.com/en/resources/emqx-v-4-3-0-ten-million-connections-performance-test-report).

| more published points | |
|---|---|
| `$share` fan-out floor, 1 → 3 → 5 nodes (v1.0.5, driver-limited; superseded by the scale-out curve above) | ~18.6k → ~53.9k → ~81.4k msg/s |
| 50,000 idle connections | 19.3–19.7 KiB each, flat across cluster sizes |
| durable QoS 1 vs clean session, same publish | ~28 ms vs ~0.03 ms p50 (dev host) |
| codec, 256 B PUBLISH | encode ~270 ns · decode ~190 ns · per-PR regression gate |

**Where mqttd loses:**
- Mosquitto: **7× less memory** at its knee; tighter tail (≤ 100 ms) at its own knee, as does HiveMQ
- EMQX: quietest at 15k msg/s (≤ 1 ms p99; mqttd matches)
- 3 nodes ≈ 1 node on the durable path
- near saturation a busy broker can be declared dead by its peers for ~30 s (membership flap, seen at 108–120k msg/s per node; no loss at 0% crossing)
- **No production users yet**
- full list: [GUIDE.md § Limitations](GUIDE.md#limitations)

---

## Quick Start (60 seconds)

```sh
docker run -d --name mqttd -p 1883:1883 \
  -e MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 -e MQTTD_ALLOW_ANONYMOUS=1 \
  -e MQTTD_DATA_DIR=/var/lib/mqttd -v mqttd-data:/var/lib/mqttd \
  ghcr.io/mbilling/fss-mqtt-broker:latest

mosquitto_sub -h 127.0.0.1 -p 1883 -t 'sensors/+/temp' &
mosquitto_pub -h 127.0.0.1 -p 1883 -t 'sensors/kitchen/temp' -m '21.5C'
```

Plaintext + anonymous: a first look, never a deployment. Secured version (TLS 1.3 + mTLS + ACL, CI-tested): [GUIDE](GUIDE.md#single-node-secured-tls-13--mtls--acl). Windows: [GUIDE](GUIDE.md#try-it-in-two-minutes).

---

## Features

| Protocol | |
|---|---|
| Versions | MQTT 3.1.1 + 5.0, full semantics; CI-conformant against Mosquitto CLI + Paho |
| QoS | 0 / 1 / 2; QoS 2 handshake resumes under the same packet id across a crash |
| Retained | durable, single-owner, consensus-ordered — no wall-clock in correctness |
| Shared subscriptions | `$share/<group>/<filter>`, **cluster-wide** |
| MQTT 5 | session/message expiry, topic aliases, flow control, user properties, subscription ids, request/response, enhanced `AUTH`, reason codes |
| Transports | TCP · TLS 1.3 · WebSocket (ws/wss) · **QUIC** (multi-stream) |

| Security | |
|---|---|
| TLS | 1.3 default (rustls / aws-lc-rs); hardened 1.2 opt-in; fleet-sized resumption cache |
| Identity | mTLS (CN/SAN, CRL) · Argon2id passwords · JWT · **OIDC** with JWKS rotation · fail-closed HTTP auth hook |
| Authorization | deny-by-default TOML ACL, `%i` / `%c` substitution, `0x87` on denied v5 publish |
| Session binding | a session cannot be resumed by a different principal |
| Hot reload | `SIGHUP` / file watch; validate-before-swap; **sweeps live sessions, grants, peer links** |
| Cluster | mTLS bus, one cert per node; signed, replay-protected gossip |
| Audit | hash-chained, SIEM schema, offline verifier |
| Code | Rust, `#![forbid(unsafe_code)]`, every parser fuzzed |

| Scale & HA | |
|---|---|
| Mesh | masterless; SWIM membership; auto mTLS peer links; interest-based routing; HRW placement |
| Durability | openraft lease group + epoch-fenced quorum replication + on-disk redb — **default** |
| Resize | grow / shrink (`SIGUSR1` drain) / replace with zero acked loss, verified under SIGKILL + partition |
| Partition | CP: minority serves committed state, retained writes queue until heal |
| Upgrades | adjacent-release skew (N ↔ N+1), nightly-tested both directions |
| Backup | online per-node export/restore, window measured |

| Operations | |
|---|---|
| Metrics | Prometheus `GET /metrics` + OTLP push; bounded labels |
| Health | `/livez` · `/readyz` · `/statusz` · `mqttd --probe` |
| Governance | connection caps (global, per-IP), auth penalty box, quotas, rate limit by backpressure, retained cap, disk/memory watermarks → brownout (refuse, never silent-drop) |
| Admin | signals + files + `--check-config`; **no HTTP API or dashboard, by design** |
| Packaging | Helm chart · Kubernetes operator (`MqttdCluster` CRD) · Compose · hardened systemd |

| Integrations | |
|---|---|
| Bridge | `mqtt-bridge`: separate signed binary/image, deny-by-default directional rules, loop prevention, bounded spool, HA pairs |
| Rule engine | EMQX-compatible rule SQL: filter, transform and re-route at QoS 0/1/2, evaluated once per message on the node it arrived at ([RULES.md](docs/RULES.md)) |
| Kafka / webhook / DB | `$share` consumer group on durable sessions ([INTEGRATION.md](docs/INTEGRATION.md)); the rule engine has no sinks, by design |
| Migration | converters for Mosquitto, EMQX, HiveMQ configs + ACLs → reviewed draft ([MIGRATION.md](docs/MIGRATION.md)) |
| Plugins | HTTP auth hook; `Authenticator` / `Authorizer` traits; no dynamic loader |

---

## Open == Enterprise

- **One build.** Clustering, quorum durability, mTLS/OIDC, live eviction, audit chain, metrics, Helm, operator, bridge, FIPS variant — all in it.
- **License:** [Apache-2.0](LICENSE), binaries and images included. Commercial use, modification, embedding, redistribution: yes.
- **Paid, ever:** support, SLAs, certified builds. **Never features.**
- **Procurement:** [SUPPORT.md](SUPPORT.md) · [compliance/](docs/compliance/) (EU CRA, IEC 62443, SOC 2 / ISO 27001) · SBOM + [VEX](security/vex/) + SLSA per release.

---

## Benchmarks

Rules ([ADR 0048](docs/adr/0048-comparative-benchmarking.md)): pinned versions · disclosed hardware and config · dated · losing cells printed · control arm or the run is void.

### Single-node comparison — method

| | |
|---|---|
| Record | [SINGLE-NODE-COMPARISON.md](docs/benchmarks/SINGLE-NODE-COMPARISON.md), dated 2026-09-17 |
| Host | Hetzner CCX23 (4 dedicated vCPU, EPYC-Milan, 16 GB); 3 × CCX43 (16 vCPU) drivers |
| Versions | mqttd 1.0.17 (published image) · Mosquitto 2.0.20 · EMQX 5.8.6 · HiveMQ CE 2024.3 — digests + config SHA-256 per arm |
| Tool | emqtt-bench 0.6.3 (EMQX's); driver-side measurement only |
| Workload | 1:1 QoS 0, 200 B, 25 msg/s per publisher; ladder 15k → 150k msg/s; 60 s per rung |
| Rung verdict | **FAILED** only on loss · carried = ≥ 95% of offer, settled, drained · p99 **GREEN ≤ 1 s** (certified) / **YELLOW ≤ 5 s** / **RED** · this run predates the zero-loss rule and passed rungs at ≥ 99% delivered |
| Config | documented minimum per broker, [committed verbatim](bench/scale/compare/); **nobody tuned**; HiveMQ heap = ½ host RAM |
| Posture | plaintext, anonymous, in-memory; **mqttd durable OFF** (`MQTTD_DURABLE_SESSIONS=0`), disclosed |
| Control | mqttd run first and last → same knee, 0.1% drift; > 5% would void the run |
| Second run | 2026-09-17, fresh fleet: mqttd 75k knee and HiveMQ heap death reproduced |
| Latency | histogram bucket upper bounds ("p99 ≤ X") — coarse, cannot flatter |
| Cost | ~€5, ~2.5 h |

```sh
cd bench/scale && export HCLOUD_TOKEN=…
COMPARE_BROKERS="mqttd mosquitto emqx hivemq" \
COMPARE_RATES="15000 30000 45000 60000 75000 90000 120000 150000" \
BROKER_TYPE=ccx23 DRIVER_TYPE=ccx43 DRIVER_COUNT=3 ./run.sh compare
python3 summarize-compare.py .runs/<stamp>/results
```

More charts: [latency 30k](docs/benchmarks/img/latency-30000.svg) · [latency 75k](docs/benchmarks/img/latency-75000.svg) · overload [Mosquitto](docs/benchmarks/img/timeline-mosquitto-150k.svg) · [EMQX](docs/benchmarks/img/timeline-emqx-150k.svg) · [HiveMQ](docs/benchmarks/img/timeline-hivemq-150k.svg)

### Scaling curve — method

| | |
|---|---|
| Record | [SCALE-CURVE.md](docs/benchmarks/SCALE-CURVE.md), verified against `v1.0.5` (2026-08-24) |
| Hosts | one CCX23 + local NVMe per broker; CCX33 drivers; fresh cluster per size |
| Build | released, cosign-signed, byte-reproducible binary; shipped systemd unit |
| Lanes | durable QoS 1 / QoS 2 (`durable_bench`, exact ack RTTs) · `$share` fan-out (emqtt-bench) · 50k idle connections |
| Workload | 256 B; 48 publishers × window 8; 60 s windows; median of 3 |
| Self-check | per-host disk barrier floor gates the durable curve; driver-limited rungs excluded; counter mismatches flagged |
| Cost | `./run.sh smoke` < €0.50 / 20 min; full 1+3+5 a few euros |

| durable QoS 1 | 1 node | 3 nodes | 5 nodes |
|---|---|---|---|
| acked msg/s | 8,503 | 8,647 | **13,893** |
| p99 saturating | 90 ms | 82 ms | **56 ms** |
| p99 uncontended | 1.01 ms | 1.81 ms | 1.69 ms |
| × disk barrier rate | 3.9× | 4.4× | 7.2× |
| QoS 2 msg/s | 2,970 | 2,124 | 3,009 |
| clean sessions msg/s | 32.1k | 89.4k | 109.6k |

**QoS 0 scale-out (3 → 10 nodes), method:**
[knee-3-5-7-10.md](bench/scale/knee-3-5-7-10.md)
- **Hardware:** one provisioning (10 × CCX23 brokers, 20 × CCX33 drivers), re-formed per
  size by `resize-cluster.sh`; drivers are 2 per broker.
- **Safeguards per size:** a mesh-settle wait, a forwarding positive control, a driver
  health gate with in-place swap of a bad host, and a closing drift control.
- **Ladders:** matched per node and stopped two failing rungs past the knee.
- **Candidate:** the unreleased `main` 0a08187, pinned by sha256.

Other published measurements (dev-grade, single host, never capacity): [DURABLE-PATH.md](docs/benchmarks/DURABLE-PATH.md) · [BASELINE.md](docs/benchmarks/BASELINE.md) · [BACKUP-RESTORE.md](docs/benchmarks/BACKUP-RESTORE.md).

### Where competitors win

| dimension | winner | detail |
|---|---|---|
| Memory at knee | Mosquitto | 18 MiB vs 133 MiB (7×); 4 MiB at 15k |
| Tail at own knee | Mosquitto, HiveMQ CE | ≤ 100 ms vs mqttd ≤ 500 ms |
| Quiet at low load | EMQX (mqttd matches) | ≤ 1 ms p99 at 15k |
| Footprint | NanoMQ, Mosquitto | ~4.6 MB image / few-MB daemon vs ~14 MB |
| Track record | all of them | mqttd: **no production users** |
| Hard memory cap | Mosquitto | mqttd: watermark + brownout only |
| Feature surface | EMQX | dashboard, SQL rules, MQTT-SN/CoAP |
| Linear scale-out, durable path | nobody yet | QoS 0 is flat 3 → 10 nodes; durable QoS 1 still 3 ≈ 1 node; plan [#537](https://github.com/mbilling/fss-mqtt-broker/issues/537) |
| Connection scale | HiveMQ, EMQX | published 200M / 100M connections; mqttd measured to 50k |
| Not covered | — | single-node comparison: one instance type, QoS 0, plaintext, no TLS, no cluster; newer Mosquitto 2.1 / EMQX 6.x unmeasured; vendor cluster figures published, not reproduced |

---

## Feature Comparison Matrix

✅ open build · 💰 **paid edition only** · ⚠️ partial · ✖ absent · n/v not verified. Sources and notes: [COMPARISON.md](docs/COMPARISON.md) (dated 2026-08-19).

| Feature | mqttd | Mosquitto 2.x | EMQX 6.x | HiveMQ CE | VerneMQ 2.1 | NanoMQ 0.25 |
|---|---|---|---|---|---|---|
| **Protocol** | | | | | | |
| MQTT 3.1.1 + 5.0 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| QoS 0/1/2, retained, LWT | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Shared subscriptions | ✅ cluster-wide | ✅ node | ✅ | ✅ node | ⚠️ | ⚠️ |
| WebSocket | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| QUIC | ✅ | ✖ | ✅ | ✖ | ✖ | ⚠️ |
| MQTT-SN gateway | ✖ | ✖ | ✅ | ✖ | ✖ | ✖ |
| CoAP gateway | ✖ | ✖ | ✅ | ✖ | ✖ | ✖ |
| **Security** | | | | | | |
| TLS 1.3 by default | ✅ | ⚠️ | ⚠️ | n/v | ⚠️ | ⚠️ |
| mTLS | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| JWT client auth | ✅ | ✖ | ✅ | 💰 | ✖ | ✖ |
| OIDC (JWKS discovery) | ✅ | ✖ | ⚠️ JWKS URL | 💰 | ✖ | ✖ |
| Hot reload: policy + certs | ✅ | ⚠️ certs from 2.1 | ✅ | ⚠️ certs only | ✅ | ⚠️ no TLS |
| Revocation reaches live sessions | ✅ | ⚠️ dynsec only | n/v | ✖ | ✖ | n/v |
| Tamper-evident audit | ✅ | ✖ | n/v | n/v | ✖ | ✖ |
| **Clustering & durability** | | | | | | |
| Clustering | ✅ | ✖ | 💰 BSL | 💰 | ✅ (💰 binaries) | ✖ |
| Replicated sessions | ✅ default | n/a | ⚠️ opt-in | 💰 | ✖ | ✖ |
| Acked msg survives node loss | ✅ proven | n/a | n/v | 💰 | ✖ | ✖ |
| Data-safe resize | ✅ | n/a | n/v | 💰 | ⚠️ | n/a |
| Online backup/restore | ✅ | ⚠️ | n/v | 💰 | n/v | n/v |
| **Operations** | | | | | | |
| Prometheus metrics | ✅ | ✖ `$SYS` only | ✅ | ✅ free extension | ✅ | ✅ |
| OpenTelemetry export | ✅ | ✖ | ✅ | 💰 | ✖ | ✖ |
| Helm chart | ✅ | ✖ | ✅ | 💰 | ✅ | ✖ |
| Kubernetes operator | ✅ | ✖ | ✅ | 💰 | ⚠️ unmaintained | ✖ |
| Admin API | ✅ mTLS, roles, audited | ⚠️ 2.1: experimental, no auth | ✅ | 💰 | ✅ | ✅ |
| Admin CLI | ✅ `mqttd --admin` | ⚠️ `mosquitto_ctrl`: security config | ✅ `emqx ctl` | ✖ | ✅ `vmq-admin` | ⚠️ start/reload |
| Admin dashboard | ✖ by design | ⚠️ 2.1: experimental | ✅ | 💰 | ⚠️ status page | ✖ |
| **Integration** | | | | | | |
| Bridge | ✅ | ✅ | ✅ | ✖ | ✅ | ✅ |
| Rule engine | ✅ EMQX rule SQL, no sinks | ✖ | ✅ | ✖ | ✖ | ✅ |
| **Build & licence** | | | | | | |
| Signed reproducible builds + SBOM | ✅ | ✖ | n/v | n/v | ✖ | ✖ |
| FIPS variant | ✅ | ✖ | n/v | 💰 | ✖ | ✖ |
| Language | Rust | C | Erlang | Java | Erlang | C |
| **License** | **Apache-2.0** | EPL/EDL | BSL 1.1 | Apache-2.0 | Apache src / EULA bin | MIT |

---

## Installation

Current release **v1.0.17** · static musl `linux/amd64` + `linux/arm64` · cosign-signed · SLSA · CycloneDX SBOM · verify: [RELEASING.md](RELEASING.md).

**Docker**
```sh
docker run -d --name mqttd --read-only --cap-drop ALL --security-opt no-new-privileges \
  -v mqttd-data:/var/lib/mqttd -v "$PWD/pki":/etc/mqttd/tls:ro -v "$PWD/acl.toml":/etc/mqttd/acl.toml:ro \
  -p 8883:8883 -p 8080:8080 \
  -e MQTTD_TLS_BIND=0.0.0.0:8883 -e MQTTD_TLS_CERT=/etc/mqttd/tls/server.crt -e MQTTD_TLS_KEY=/etc/mqttd/tls/server.key \
  -e MQTTD_ACL_FILE=/etc/mqttd/acl.toml -e MQTTD_DATA_DIR=/var/lib/mqttd -e MQTTD_HEALTH_BIND=0.0.0.0:8080 \
  ghcr.io/mbilling/fss-mqtt-broker:1.0.17
cosign verify ghcr.io/mbilling/fss-mqtt-broker:1.0.17 \
  --certificate-identity-regexp 'https://github.com/mbilling/fss-mqtt-broker/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

**Kubernetes**
```sh
NS=mqttd REPLICAS=3 ./deploy/helm/mqttd/bootstrap.sh
helm install mqttd deploy/helm/mqttd -n mqttd --set replicaCount=3 \
  --set secrets.tls.secretName=mqttd-tls --set secrets.peerTls.secretName=mqttd-peer-tls \
  --set secrets.gossipKey.secretName=mqttd-gossip
```
Operator: `deploy/helm/mqttd-operator` · guide: [KUBERNETES.md](docs/KUBERNETES.md)

**Compose / systemd**
```sh
cd deploy/compose && ./bootstrap.sh && docker compose up -d    # 3 TLS nodes, one host
```
Bare metal: [`deploy/systemd/`](deploy/systemd/) · tutorial: [SECURED-CLUSTER-TUTORIAL.md](docs/SECURED-CLUSTER-TUTORIAL.md)

**Binaries:** [GitHub Releases](https://github.com/mbilling/fss-mqtt-broker/releases) (`mqttd-*`, `mqttd-fips-*`, `mqtt-bridge-*`)

**Source** (Rust ≥ 1.88)
```sh
git clone https://github.com/mbilling/fss-mqtt-broker && cd fss-mqtt-broker
cargo build --release --bin mqttd
cargo install --locked --path tools/mqttui    # runnable map of every demo/test/migration script
```

---

## Configuration

Layering: **defaults < TOML < `MQTTD_*` env < CLI** · strict schema · secrets by path · hot reload.

```toml
[node]
id = "node-1"
data_dir = "/var/lib/mqttd"

[listeners]
tls_bind    = "0.0.0.0:8883"
health_bind = "0.0.0.0:8080"      # /livez /readyz /metrics

[tls]
cert      = "/etc/mqttd/tls/server.crt"
key       = "/etc/mqttd/tls/server.key"
client_ca = "/etc/mqttd/tls/client-ca.crt"   # mTLS

[security]
allow_anonymous = false
acl_file = "/etc/mqttd/acl.toml"             # deny by default
```

```sh
mqttd --check-config --config mqttd.toml    # validate, binds nothing
mqttd --print-config --config mqttd.toml    # effective config, secrets fingerprinted
mqttd --check-tls --config mqttd.toml       # certs, keys, CAs, CRLs: match, order, expiry
kill -HUP "$(pidof mqttd)"                  # reload policy, TLS, quotas
```

Template: [`docs/mqttd.example.toml`](docs/mqttd.example.toml) · every key: [CONFIGURATION.md](docs/CONFIGURATION.md)

---

## Production Deployment

**Sizing** — [SIZING.md](docs/SIZING.md), preset [`bounded-node.toml`](docs/examples/bounded-node.toml). Nine knobs bound a node:

| knob | ships as |
|---|---|
| `MQTTD_DATA_DIR` | required (durable on) |
| `MQTTD_STORE_MAX_BYTES` | unbounded |
| `MQTTD_MAX_CONNECTIONS` (+`_PER_IP`) | uncapped |
| `MQTTD_MAX_PACKET_SIZE` | 1 MiB |
| `MQTTD_MAX_SESSIONS` | uncapped |
| `MQTTD_MAX_QUEUED_MESSAGES` | 100,000 / drop-oldest |
| `MQTTD_MAX_BACKLOG_*`, `MQTTD_MAX_INFLIGHT_MESSAGES` | 10,000 / off / 65,535 |
| `MQTTD_MAX_RETAINED_MESSAGES` | uncapped |
| `MQTTD_AUTH_PENALTY_THRESHOLD` | unlimited attempts |

Memory watermark at 75–85% of the container limit; the container limit is the hard bound. ~19 KiB per idle connection.

**Clustering**
- **≥ 3 nodes, never 2** (2-node write quorum is 2-of-2: worse than 1)
- one founder boots with an empty seed list; others seed off any member
- one cluster-bus certificate **per node**
- grow: start a node · shrink: `SIGUSR1` · replace: grow then shrink · upgrade: one node at a time
- day 2: [OPERATIONS.md](docs/OPERATIONS.md)

**Hardening** — 34-item baseline with auditor checks: [HARDENING.md](docs/HARDENING.md)
- [ ] no `INSECURE:` lines in the startup log
- [ ] `MQTTD_DATA_DIR` on a volume
- [ ] no plaintext listener; TLS 1.3; client certs carry `clientAuth` EKU
- [ ] anonymous off; Argon2id passwords via `mqttd --hash-password`; file mode ≤ 640
- [ ] ACL file, `default = "deny"`
- [ ] caps and watermarks set
- [ ] per-node bus certs; gossip key from file
- [ ] CRL configured, reload tested
- [ ] `--read-only --cap-drop ALL --security-opt no-new-privileges`, or the shipped systemd unit

---

## Monitoring & Observability

| | |
|---|---|
| Metrics | Prometheus `GET /metrics` · OTLP push (`MQTTD_OTLP_ENDPOINT`) · bounded labels |
| Probes | `/livez` · `/readyz` (members, lease, decommission) · `/statusz` · `mqttd --probe` |
| Audit | hash-chained JSON; schema + verifier: [AUDIT-SCHEMA.md](docs/AUDIT-SCHEMA.md) |
| Dashboards | Grafana for broker + bridge: [`deploy/observability/`](deploy/observability/); alert runbooks: [OPERATIONS.md](docs/OPERATIONS.md) |
| Demo | `cd demo && docker compose up --build` → cluster + Grafana + Prometheus + Alloy at `localhost:3000` |

## Admin API & CLI

An authenticated admin API — its own mTLS listener, `viewer` and `operator` roles, every
request audited — answers what metrics cannot, and `mqttd --admin` drives it from the same
binary (so it works in the distroless image). Off until `admin.bind` is set; it never writes
configuration.

```sh
mqttd --admin cluster                        # every node, from any node: ready, version, lag, agreement
mqttd --admin clients --prefix sensor-       # who is connected, from where
mqttd --admin session sensor-7               # subscriptions, in flight, backlog, owner
mqttd --admin authz device-7 publish plant/7/temp   # allowed? which ACL rule decided?
mqttd --admin reload                         # re-read the config file; says what changed or why not
mqttd --admin kick sensor-7                  # operator: disconnect (MQTT 5 reason 0x98), from any node
mqttd --admin cordon                         # operator: stop new connections without draining
```

Try it on a local three-node cluster: `scripts/admin-e2e.sh up`. Reference:
[ADMIN-CLI.md](docs/ADMIN-CLI.md) (every command) · [ADMIN-API.md](docs/ADMIN-API.md) (every
endpoint, roles, errors).

---

## Architecture

```text
        MQTT clients  (TCP · TLS 1.3 · WebSocket · QUIC)
              │  identity: mTLS-CN / password / JWT / OIDC
              ▼        ↓ deny-by-default topic ACL
       ┌──────────────────────────────────────────┐
       │  node                                    │
       │   listeners → per-connection tasks       │
       │                    ▼                     │
       │            hub (routing actor)           │   one hub per node;
       │        subscriptions · retained · queues │   no lock on the hot path
       └────────────────────┼─────────────────────┘
   ┌────────────────────────┼────────────────────────┐
   │ SWIM gossip            │ peer links (mTLS)      │  one trust domain =
   │ membership, interest   │ interest-based forward │  one logical broker
   └────────────────────────┼────────────────────────┘
                            ▼
              durable plane — openraft lease group
              epoch-fenced quorum replication
```

- shared-nothing nodes; one routing actor per node; cross-node traffic is just another command
- durable write **before** the wire send → "acked" means "replicated"; clean sessions and QoS 0 skip it
- consensus for control (epochs, ownership), small replica sets for data
- refuse at the edge: reason code or backpressure, never a silent drop
- bridge is a separate process and failure domain
- decisions: [`docs/adr/`](docs/adr/) (83 ADRs, per-task status) · tour: [ARCHITECTURE.md](docs/ARCHITECTURE.md) · [THREAT-MODEL.md](docs/THREAT-MODEL.md)

**Workspace layout**

| crate | owns |
|---|---|
| `mqtt-codec` | MQTT 3.1.1 + 5.0 wire codec; fuzzed |
| `mqtt-core` | sessions, subscription tables, topic matching, ACL relations |
| `mqtt-net` | listeners (TCP/TLS/WebSocket/QUIC), the one audited TLS module |
| `mqtt-auth` | `Authenticator` / `Authorizer` traits; mTLS-CN, Argon2id, JWT, OIDC, ACL |
| `mqtt-storage` | `SessionStore` / `RetainedStore`, replicated log, redb |
| `mqtt-cluster` | SWIM, gossip auth, HRW placement, peer wire, durable plane |
| `mqtt-observability` | Prometheus/OTLP metrics, hash-chained audit |
| `mqtt-config` | typed config, secure defaults |
| `mqtt-rules` | rule engine: EMQX-compatible rule SQL, republish/console actions |
| `mqtt-bridge` | zone-crossing bridge: spool, QoS 1 replay |
| `mqttd` | the broker binary: hub, connections, peer mesh |
| `mqttd-operator` | Kubernetes operator for `MqttdCluster` |
| `history-check` | independent checker of recorded client-visible histories |

---

## Documentation

Index: [docs/README.md](docs/README.md)

| goal | doc |
|---|---|
| Everything, long form | [GUIDE.md](GUIDE.md) — walkthroughs, Limitations, resizing, upgrades, hot reload |
| Evaluate | [EVALUATION.md](docs/EVALUATION.md) · [COMPARISON.md](docs/COMPARISON.md) |
| Deploy | [SECURED-CLUSTER-TUTORIAL.md](docs/SECURED-CLUSTER-TUTORIAL.md) · [KUBERNETES.md](docs/KUBERNETES.md) |
| Build clients | [CLIENT-GUIDE.md](docs/CLIENT-GUIDE.md) |
| Operate | [OPERATIONS.md](docs/OPERATIONS.md) · [ADMIN-CLI.md](docs/ADMIN-CLI.md) · [ADMIN-API.md](docs/ADMIN-API.md) · [SIZING.md](docs/SIZING.md) · [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) |
| Audit | [THREAT-MODEL.md](docs/THREAT-MODEL.md) · [HARDENING.md](docs/HARDENING.md) · [compliance/](docs/compliance/) |
| Migrate | [MIGRATION.md](docs/MIGRATION.md) |
| Decisions | [adr/](docs/adr/) · [delivery dashboard](docs/delivery/STATUS.md) |
| Terms | [GLOSSARY.md](docs/GLOSSARY.md) |

---

## Roadmap

Tracked on the [delivery dashboard](docs/delivery/STATUS.md).

- **Benchmarks:** multi-host cluster comparison ([#244](https://github.com/mbilling/fss-mqtt-broker/issues/244)), larger hosts, TLS posture, per-release re-run ([#545](https://github.com/mbilling/fss-mqtt-broker/issues/545))
- **Scale-out:** measure then optimise ([#537](https://github.com/mbilling/fss-mqtt-broker/issues/537)); durable ownership beyond the voter set (ADR 0073); workload arms incl. burst (ADR 0077); QoS 0 shared-worker capacity ([#482](https://github.com/mbilling/fss-mqtt-broker/issues/482))
- **Auth:** SCRAM · OCSP · PSK suites · server-initiated re-auth
- **Migration:** NanoMQ converter ([#546](https://github.com/mbilling/fss-mqtt-broker/issues/546)); bridge demo ([#547](https://github.com/mbilling/fss-mqtt-broker/issues/547))
- **Security:** OSS-Fuzz ([#553](https://github.com/mbilling/fss-mqtt-broker/issues/553)); funded third-party audit ([#554](https://github.com/mbilling/fss-mqtt-broker/issues/554))
- **Routing:** bloom subscription digests; MQTT 5 Server-Reference redirect
- **Operator:** CRD promotion from `v1alpha1`
- **Not planned, by decision:** dashboard, writing config over the network, rule-engine data sinks (Kafka/HTTP/DB), MQTT-SN/CoAP

---

## Security Policy

- **Report privately:** <https://github.com/mbilling/fss-mqtt-broker/security/advisories/new> — never a public issue
- Policy, scope, timelines: [SECURITY.md](SECURITY.md)
- Per-release CVE dispositions: [security/vex/](security/vex/)
- Supported lines: [SUPPORT.md](SUPPORT.md) (three most recent minors)

---

## Support & Community

- **Issues:** [GitHub Issues](https://github.com/mbilling/fss-mqtt-broker/issues) — the only channel today; no chat or forum yet
- **Releases:** [GitHub Releases](https://github.com/mbilling/fss-mqtt-broker/releases)
- **Lifecycle:** three minor lines patched; adjacent-release upgrades ([SUPPORT.md](SUPPORT.md))
- **Commercial:** model = support, SLAs, certified builds; nothing published yet — open an issue
- **Status:** `v1.0.17` released and signed; **no production users yet**

---

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) · [Code of Conduct](CODE_OF_CONDUCT.md)

- decisions live in ADRs, progress in delivery docs
- a task title is a claim, and the claim must be true
- prose must not actively mislead

```sh
cargo build && cargo test && cargo clippy --all-targets && cargo deny check
./scripts/interop/run.sh     # foreign-client conformance
mqttui --list                # There are 118 runnable scripts here: demos, smokes, migrations, benches
```

---

## License

[Apache-2.0](LICENSE). Every crate, binary and image. No paid tier, no feature gates.
