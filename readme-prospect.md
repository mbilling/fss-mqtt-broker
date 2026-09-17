# mqttd — a security-first, cluster-native MQTT broker

[![CI](https://img.shields.io/github/actions/workflow/status/mbilling/fss-mqtt-broker/ci.yml?branch=main&label=CI)](https://github.com/mbilling/fss-mqtt-broker/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/mbilling/fss-mqtt-broker)](https://github.com/mbilling/fss-mqtt-broker/releases)
[![License](https://img.shields.io/github/license/mbilling/fss-mqtt-broker)](LICENSE)
[![Container image](https://img.shields.io/badge/ghcr.io-fss--mqtt--broker-blue?logo=docker)](https://github.com/mbilling/fss-mqtt-broker/pkgs/container/fss-mqtt-broker)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/mbilling/fss-mqtt-broker/badge)](https://scorecard.dev/viewer/?uri=github.com/mbilling/fss-mqtt-broker)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/14161/badge)](https://www.bestpractices.dev/projects/14161)

> **Open == Enterprise: every feature, no paid tier.**
>
> An MQTT 3.1.1 + 5.0 broker written in Rust, built to be the most cyber-secure
> broker available, with quorum-replicated durable sessions **on by default**,
> and a 100% open, Apache-2.0 feature set — clustering, security and
> observability included.
>
> **Measured, not asserted:** on one 4-vCPU cloud host, a single mqttd node holds
> **75,000 msg/s** at p99 ≤ 1 s — **1.7× Mosquitto and EMQX, 2.5× HiveMQ CE** —
> with **97.8% of messages delivered within 10 ms** at the rate where the others
> hit their knee. Same host, same load tool (EMQX's own), control arm reproduced
> to 0.1%. [The numbers](#by-the-numbers) · [the method](#benchmarks).

*(GHCR does not publish a pull-count badge; the image badge above links to the package page.)*

---

## Why mqttd

- **The fastest single node in the published comparison.** 75,000 msg/s at p99 ≤ 1 s against 45,000 for Mosquitto and EMQX and 30,000 for HiveMQ CE, on the same host, with 30% CPU still idle at the knee — and under 2× overload it queues, then drains at 1.6× the offered rate and hands 95% of the memory back.
- **Durable by default.** Every persistent session is quorum-replicated. An acked QoS 1/2 message survives the loss of the node that accepted it — queued *or in flight* — and a group too thin to keep that promise **refuses** the write rather than acking on one copy.
- **Revocation reaches live state.** Reload the policy and a revoked certificate, removed user, or tightened grant **evicts the already-connected client** — not at its next reconnect, now. No compared broker documents this.
- **Secure by default, loudly.** TLS 1.3, mTLS/OIDC identity, deny-by-default ACLs, tamper-evident audit. Every insecure mode is opt-in and logs `INSECURE:` on every start.
- **Clustering is not a paid feature.** One Apache-2.0 codebase, signed reproducible binaries, SBOM and SLSA provenance. EMQX gates production clustering behind BSL; VerneMQ's production binaries are EULA-paid; HiveMQ CE is single-node.
- **Claims you can check.** Every capability maps to a task with evidence on the [delivery dashboard](docs/delivery/STATUS.md); benchmarks print their losing cells; what is missing is listed in [Limitations](README.md#limitations), not left to be discovered.

---

## By the numbers

Every figure below is published in-tree with its method, hardware, versions, and the cells
mqttd loses. Cross-broker numbers: [SINGLE-NODE-COMPARISON.md](docs/benchmarks/SINGLE-NODE-COMPARISON.md)
(run 2026-09-16, Hetzner CCX23, 4 dedicated vCPU / 16 GB, one host, brokers in sequence).
Cluster numbers: [SCALE-CURVE.md](docs/benchmarks/SCALE-CURVE.md) (one dedicated host and NVMe disk per broker).

**Single-node knee** — the highest offered rate that still met p99 ≤ 1 s with ≥ 99% delivered (1:1 QoS 0, 200 B, untuned documented minimum config for every broker):

| broker | knee | p99 at knee | CPU idle at knee | RSS at knee |
|---|---|---|---|---|
| **mqttd 1.0.17** | **75,000 msg/s** | ≤ 500 ms | **30%** | 133 MiB |
| Mosquitto 2.0.20 | 45,000 msg/s | ≤ 100 ms | 70% (single-threaded: one core of four) | **18 MiB** |
| EMQX 5.8.6 | 45,000 msg/s | ≤ 500 ms | 2% | 434 MiB |
| HiveMQ CE 2024.3 | 30,000 msg/s | ≤ 100 ms | 5% | 927 MiB |

**Latency is a distribution.** At 45,000 msg/s, the rate where Mosquitto and EMQX reach their knee, the share of messages delivered within:

| | 1 ms | 10 ms | 25 ms | 100 ms | 1 s |
|---|---|---|---|---|---|
| **mqttd** | **39.9%** | **97.8%** | **100%** | 100% | 100% |
| Mosquitto | 0.1% | 30.6% | 57.8% | 99.7% | 100% |
| EMQX | 0.1% | 14.9% | 40.6% | 82.8% | 100% |
| HiveMQ CE | 0.0% | 0.4% | 2.3% | 25.4% | 61.1% |

![Latency distribution at 45,000 msg/s offered — mqttd's dashed control arm lies on its solid first arm](docs/benchmarks/img/latency-45000.svg)

**Overload at 150,000 msg/s** (2× past mqttd's knee, 3–5× past the others'):

| | mqttd | Mosquitto | EMQX | HiveMQ CE |
|---|---|---|---|---|
| behaviour | queues, then **drains at 239,000 msg/s** (1.6× offered) | accepts 38% of offer, flat | queues | **JVM dies** (`OutOfMemory`) |
| peak memory | 3.8 GiB | 991 MiB | 3.6 GiB | 12 GiB |
| memory returned after drain | **95%** | **98%** | 24% | — |
| delivered ledger | whole | partial | whole | 0 clients connected |

**Cluster scale-out, durable QoS 1** (acked only after fsync + quorum replication; 48 publishers × window 8, 256 B):

| nodes | acked msg/s | p99 saturating | p99 uncontended |
|---|---|---|---|
| 1 | 8,503 | 90 ms | 1.01 ms |
| 3 | 8,647 | 82 ms | 1.81 ms |
| 5 | **13,893** (1.63× one node) | **56 ms** | 1.69 ms |

**More published points:** non-durable `$share` fan-out floor **~18.6k → ~53.9k → ~81.4k msg/s** at 1 → 3 → 5 nodes (driver-limited, so floors); **50,000 idle connections at a flat 19.3–19.7 KiB each** at every cluster size; codec encodes a 256 B PUBLISH in **~270 ns** and decodes it in **~190 ns**, with a per-PR regression floor.

**Where mqttd loses, on the same pages:** Mosquitto holds its knee in **7× less memory**; Mosquitto and HiveMQ keep a **tighter tail at their own knees** (≤ 100 ms vs mqttd's ≤ 500 ms); EMQX is the quietest at 15,000 msg/s (≤ 1 ms p99, matched by mqttd); the durable path costs **~28 ms p50** per message on dev hardware against ~0.03 ms to a clean session; 3 nodes ≈ 1 node on the durable curve (the quorum tax); and there are **no production users yet**.

---

## Quick Start (60 seconds)

You need Docker and the Mosquitto clients (`brew install mosquitto` / `apt install mosquitto-clients`).

```sh
docker run -d --name mqttd -p 1883:1883 \
  -e MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 -e MQTTD_ALLOW_ANONYMOUS=1 \
  -e MQTTD_DATA_DIR=/var/lib/mqttd -v mqttd-data:/var/lib/mqttd \
  ghcr.io/mbilling/fss-mqtt-broker:latest

mosquitto_sub -h 127.0.0.1 -p 1883 -t 'sensors/+/temp' &
mosquitto_pub -h 127.0.0.1 -p 1883 -t 'sensors/kitchen/temp' -m '21.5C'
```

`21.5C` arrives in the subscriber. That is **plaintext with anonymous clients** — a
first look, never a deployment; the broker says so in its own logs. The
[secured quickstart](README.md#single-node-secured-tls-13--mtls--acl) stands up
TLS 1.3 + mutual TLS + a deny-by-default ACL in about the same number of commands,
and CI runs those exact commands on every push. Windows/PowerShell variant:
[README](README.md#try-it-in-two-minutes).

---

## Features

### Protocol
- **MQTT 3.1.1 and 5.0**, full semantics not just the codec — validated in CI against two independent foreign clients (Mosquitto CLI + Eclipse Paho).
- **QoS 0/1/2** end to end, with the outbound QoS 2 packet id and phase persisted with the session so the handshake resumes under the same id across a broker crash.
- **Retained messages**, durable and single-owner across the cluster: conflicts are prevented by consensus, never resolved by wall-clock.
- **Shared subscriptions** (`$share/<group>/<filter>`), **cluster-wide**: each message reaches exactly one member across the mesh.
- MQTT 5: session/message expiry, topic aliases, flow control (Receive Maximum), User Properties forwarded end to end, subscription identifiers, request/response, enhanced `AUTH`, reason codes on every refusal.
- **Transports:** TCP, TLS 1.3, **WebSocket** (`ws://`/`wss://`), and **QUIC** (multi-stream, no head-of-line blocking).
- Last Will and Testament, keepalive enforcement, persistent sessions with offline queueing.

### Security
- **TLS 1.3 by default** (rustls on aws-lc-rs, one crypto provider for the whole build); a hardened TLS 1.2 opt-in for legacy fleets; fleet-sized session resumption.
- **mTLS** client certificates (identity from CN/SAN, CRL revocation, hot-reloadable), **Argon2id** password file, **JWT**, **OIDC** with live JWKS rotation, and a fail-closed **HTTP auth hook** for LDAP/OAuth2/bespoke user tables.
- **Deny-by-default topic ACLs** (TOML) with `%i` identity and `%c` client-id substitution; a denied MQTT 5 publish is answered `0x87 Not authorized`.
- **Session-identity binding:** a persistent session cannot be resumed or taken over by a different principal.
- **Hot-reloadable policy** (`SIGHUP` or file watch): validate-before-swap, all-or-nothing, and the reload **sweeps live sessions, grants and peer links** against the new policy.
- **Mutually authenticated cluster bus** (one certificate per node) and **signed, replay-protected gossip**.
- **Tamper-evident audit log** (hash-chained; SIEM schema in [AUDIT-SCHEMA.md](docs/AUDIT-SCHEMA.md)); an offline verifier reproves a captured stream with no secret.
- Memory-safe: Rust, `#![forbid(unsafe_code)]`; every attacker-reachable parser continuously fuzzed.

### Scale and HA
- **Masterless mesh:** authenticated SWIM membership, automatic mTLS peer links, interest-based routing, HRW (rendezvous) session placement.
- **Quorum-replicated durable sessions** (openraft lease group, epoch-fenced replication, on-disk redb stores) — the **default**, not an add-on.
- **Elastic, data-safe resize:** grow, shrink (`SIGUSR1` decommission drain) and replace on a running cluster with zero acked loss, verified under SIGKILL and partition harnesses.
- **CP under partition, explicitly:** the minority keeps serving committed state; its retained writes queue until heal.
- **Rolling upgrades:** adjacent-release version skew (N ↔ N+1) is the supported and nightly-tested mixed state.
- **Online backup/restore** per node with a measured window ([BACKUP-RESTORE.md](docs/benchmarks/BACKUP-RESTORE.md)).

### Operations
- **Prometheus** `GET /metrics` (bounded label sets) and **OTLP push** to an OpenTelemetry Collector, from one registry.
- **Health:** `GET /livez`, `GET /readyz` (membership, lease readiness, decommission progress) and `GET /statusz` state surface — Kubernetes-probe shaped.
- **Structured logging** (tracing), effective config logged at boot with secrets redacted.
- **Resource governance:** connection caps (global and per-IP, enforced before TLS work), auth-failure penalty box, per-client subscription/session quotas, publish-rate limiting by TCP backpressure, retained-topic cap, disk and memory watermarks with **brownout** (refuse the publisher, never silently drop).
- **Admin surface is deliberate:** signals and files, a read-only health listener, `mqttd --check-config` as a pre-rollout gate. There is **no HTTP admin API or dashboard by design** (see [Feature Comparison](#feature-comparison-matrix)).
- **Helm chart** (StatefulSet, per-pod PV, decommission-draining scale-down, PDB) and a **Kubernetes operator** (`MqttdCluster` CRD, split-brain fencing); Compose and hardened systemd packagings ship alongside.

### Integrations
- **`mqtt-bridge`:** a standalone, signed, separately containerised bridge to brokers in other security zones — deny-by-default directional rules, hop-count loop prevention, bounded store-and-forward spool, HA pairs via shared subscriptions, its own Prometheus metrics and Grafana dashboard.
- **Kafka / webhooks / databases:** no in-broker rule engine, by design. The documented, CI-tested replacement is an ordinary `$share` consumer group on durable sessions ([INTEGRATION.md](docs/INTEGRATION.md)) — at-least-once into your sink, deduplicated there.
- **Migration converters** for Mosquitto, EMQX and HiveMQ configs and ACLs, producing a reviewed draft with every undecidable construct marked `TODO(migrate)` ([MIGRATION.md](docs/MIGRATION.md)).
- Plugins: authentication is pluggable through the HTTP hook and the `Authenticator`/`Authorizer` traits in `mqtt-auth`; there is no dynamic plugin loader.

---

## Open == Enterprise

**Everything in this repository is in the open version, because there is only one version.**
Clustering, quorum durability, mTLS/OIDC, hot-reloadable policy with live eviction,
the tamper-evident audit chain, Prometheus/OTLP metrics, the Helm chart, the
Kubernetes operator, the bridge, the FIPS build variant — all Apache-2.0, all in the
signed release artifacts.

- **License:** [Apache-2.0](LICENSE), including the released binaries and container images. Use it commercially, modify it, embed it, redistribute it; the only obligations are attribution and notice preservation.
- **No feature gates, ever.** The project's stated model reserves paid offerings for support, SLAs, and certified builds — never for functionality ([principles](README.md#principles)).
- **What "enterprise" buys you elsewhere, you get here:** see the [comparison matrix](#feature-comparison-matrix) for the cells rivals reserve for their paid editions.
- **Procurement surface:** [SUPPORT.md](SUPPORT.md) (support lifecycle, export-control statement), [docs/compliance/](docs/compliance/) (EU CRA, IEC 62443, SOC 2 / ISO 27001 mappings, crypto policy), per-release SBOM, [OpenVEX](security/vex/) and SLSA provenance.

---

## Benchmarks

**Honesty rules first** ([ADR 0048](docs/adr/0048-comparative-benchmarking.md)): versions
pinned, hardware and config disclosed, results dated, losing dimensions printed as prominently
as winning ones, a control arm that voids the run if it drifts, and nothing single-host is ever
published as a cluster number.

### Cross-broker: the single-node knee

**Dated 2026-09-17, run 2026-09-16** — [SINGLE-NODE-COMPARISON.md](docs/benchmarks/SINGLE-NODE-COMPARISON.md).
One Hetzner CCX23 (4 dedicated vCPU, AMD EPYC-Milan, 16 GB), all four brokers on the same host
one after another with a reboot between arms, driven by three oversized CCX43 (16 vCPU) hosts
running EMQX's own [emqtt-bench 0.6.3](https://github.com/emqx/emqtt-bench).

| broker | version (image digest pinned) | **knee** | p99 at knee | CPU idle at knee (mean / min) | RSS at knee | first failing rung |
|---|---|---|---|---|---|---|
| **mqttd** | 1.0.17 (the published image, not a special build) | **75,000 msg/s** | ≤ 500 ms | 30% / 21% | 133 MiB | 90,000 — p99 ≤ 7.5 s |
| Mosquitto | 2.0.20 | 45,000 msg/s | ≤ 100 ms | 70% / 57% | **18 MiB** | 60,000 — p99 ≤ 10 s |
| EMQX | 5.8.6 (last Apache-2.0 line) | 45,000 msg/s | ≤ 500 ms | 2% / 0% | 434 MiB | 60,000 — p99 ≤ 25 s |
| HiveMQ CE | 2024.3 | 30,000 msg/s | ≤ 100 ms | 5% / 3% | 927 MiB | 45,000 — p99 ≤ 5 s |

The ladder steps in 15,000 msg/s, so every knee is bracketed by its next rung (mqttd's true
knee is between 75,000 and 90,000; a finer ladder would move every row). **The control
passed:** mqttd ran first and last, and arm 5 reproduced arm 1's knee with 0.1% delivery
drift after four reboots and three other brokers. A second, shorter run on a separately
provisioned fleet on 2026-09-17 reproduced mqttd's 75,000 knee and HiveMQ's heap death.

**What limited each broker:** Mosquitto is core-bound (single-threaded, one to one and a half
of four cores ever used); EMQX is CPU-bound (2% idle at its knee); HiveMQ CE is memory-bound and
does not degrade, it dies (52.6% of messages undelivered one rung past its knee, then
`OutOfMemory: Java heap space` with the vendor-shipped `-XX:+CrashOnOutOfMemoryError`);
**mqttd is the only one not CPU-limited at its knee** — what fails first is the tail.

```text
single-node knee, msg/s at p99 ≤ 1 s (same host, same driver, untuned)
mqttd      1.0.17   ██████████████████████████████████████████████████  75,000
Mosquitto  2.0.20   ██████████████████████████████                      45,000
EMQX       5.8.6    ██████████████████████████████                      45,000
HiveMQ CE  2024.3   ████████████████████                                30,000
```

**Latency distribution, all four brokers at 45,000 msg/s** (chart above in
[By the numbers](#by-the-numbers); the 30,000 and 75,000 charts are
[here](docs/benchmarks/img/latency-30000.svg) and [here](docs/benchmarks/img/latency-75000.svg)).
At 30,000 msg/s, where every broker passes, they are far closer: mqttd 96.7% within 1 ms,
EMQX 75.9%, Mosquitto 59.5%, HiveMQ 12.2%. The curves separate as load approaches each broker's
knee, which is what a single p99 hides.

**Overload has a shape.** Driven at 150,000 msg/s until the backlog cleared:

| | mqttd | Mosquitto | EMQX | HiveMQ CE |
|---|---|---|---|---|
| memory at rung start | 9 MiB | 1 MiB | 666 MiB | 5,788 MiB |
| peak | 3,781 MiB | 991 MiB | 3,568 MiB | 12,012 MiB |
| after the drain | 190 MiB | 17 MiB | 2,714 MiB | process gone |
| **memory returned** | **95%** | **98%** | 24% | — |
| burst when publishers stop | **yes, 239,000 msg/s (1.6× offered)** | none (never accepted the load: ~40,000 msg/s flat) | none | — |
| chart | [timeline](docs/benchmarks/img/timeline-mqttd-150k.svg) | [timeline](docs/benchmarks/img/timeline-mosquitto-150k.svg) | [timeline](docs/benchmarks/img/timeline-emqx-150k.svg) | [timeline](docs/benchmarks/img/timeline-hivemq-150k.svg) |

![mqttd at 150,000 msg/s: delivered rate against broker memory, then the drain burst](docs/benchmarks/img/timeline-mqttd-150k.svg)

mqttd banks the excess and flushes it; Mosquitto sheds at the door and stays under 1 GiB. Both
are defensible answers to overload, and a buyer should pick the one their system wants.

### mqttd against itself: the cluster scaling curve

[SCALE-CURVE.md](docs/benchmarks/SCALE-CURVE.md), verified against `v1.0.5` (2026-08-24): the
same workload against fresh 1-, 3- and 5-node clusters, one Hetzner CCX23 and one local NVMe
disk per broker, released cosign-signed binary, shipped systemd unit.

**Curve 1 — durable QoS 1, acked only after fsync + quorum replication** (48 closed-loop
publishers × window 8, 48 durable subscribers, 256 B, 60 s windows, median of 3):

| nodes | acked msg/s (saturating) | p99 (saturating) | p99 (uncontended) | × slowest disk's barrier rate |
|---|---|---|---|---|
| 1 | 8,503 | 90 ms | 1.01 ms | 3.9× |
| 3 | 8,647 | 82 ms | 1.81 ms | 4.4× |
| 5 | **13,893** | **56 ms** | 1.69 ms | 7.2× |

```text
durable QoS 1, acked msg/s
1 node  ████████████████████░░░░░░░░░░░░░   8.5k
3 nodes ████████████████████░░░░░░░░░░░░░   8.6k   (quorum tax fully absorbed)
5 nodes █████████████████████████████████  13.9k   (1.63× one node; p99 improves 90 → 56 ms)
```

Durable throughput runs at 3.9–7.2× the disk's own barrier rate, so it is decoupled from the
fsync floor. Same shape for QoS 2 (2,970 → 2,124 → 3,009 msg/s) and clean sessions
(32.1k → 89.4k → 109.6k msg/s, nothing durable to write). Weakening the ack to single-copy
(`local` tier) buys nothing on datacenter NVMe: 8,734 vs 8,503 at one node.

**Curve 2 — non-durable `$share` fan-out** (600 publishers → 300 subscribers in one shared
group, QoS 1): delivered plateau **~18.6k → ~53.9k → ~81.4k msg/s** at 1 → 3 → 5 nodes.
Every rung above 50k offered was **driver-limited**, so these are floors, not capacities.

**Connections:** 50,000 idle connections cost a flat **19.3–19.7 KiB each** at every cluster size.

**Two more published measurements**, both labelled dev-grade single-host and never to be quoted
as capacity: the [durable path](docs/benchmarks/DURABLE-PATH.md) (a durable QoS 1 publish costs
~28 ms p50 against ~0.03 ms to a clean session — the price of the guarantee, pinned by that
host's per-volume flush rate of ~215–240/s) and the
[hot-path micro-baselines](docs/benchmarks/BASELINE.md) (256 B PUBLISH encodes in ~270 ns,
decodes in ~190 ns; a per-PR regression floor fails the build on a gross slowdown).

### Methodology

| | Single-node comparison | Cluster scaling curve |
|---|---|---|
| Record | [SINGLE-NODE-COMPARISON.md](docs/benchmarks/SINGLE-NODE-COMPARISON.md) | [SCALE-CURVE.md](docs/benchmarks/SCALE-CURVE.md) |
| Scripts | [`bench/scale/compare-brokers.sh`](bench/scale/compare-brokers.sh), [`summarize-compare.py`](bench/scale/summarize-compare.py), [`chart-compare.py`](bench/scale/chart-compare.py) | [`bench/scale/run.sh`](bench/scale/run.sh), [`summarize-curve.py`](bench/scale/summarize-curve.py) |
| Hardware | one Hetzner CCX23 broker host (4 dedicated vCPU, 16 GB), three CCX43 (16 vCPU) drivers | one CCX23 + local NVMe per broker, CCX33 drivers, `fsn1` |
| Versions | mqttd 1.0.17, Mosquitto 2.0.20, EMQX 5.8.6, HiveMQ CE 2024.3 — image digests and config SHA-256 recorded per arm | released, cosign-signed, byte-reproducible `mqttd` `v1.0.5` |
| Config | each broker's documented reasonable minimum, committed verbatim in [`bench/scale/compare/`](bench/scale/compare/); **nobody tuned**, including mqttd; HiveMQ heap set to half the host's RAM | shipped systemd unit + disclosed env template; cluster PKI from `deploy/systemd/gen-certs.sh` |
| Load tool | emqtt-bench 0.6.3 (EMQX's own); measurement driver-side only, no broker counters | emqtt-bench 0.6.3 for fan-out; `durable_bench` harness with exact per-message ack RTTs for the durable lane |
| Workload | 1:1 QoS 0 pub/sub per topic, 200 B, 25 msg/s per publisher; ladder 15k → 150k msg/s; 60 s window per rung; rung passes only at ≥ 99% delivered, ≥ 95% of offer, p99 ≤ 1 s, settled and drained | 256 B; durable QoS 1 and QoS 2; QoS 1 `$share`; 50k idle connections; 3 reps |
| Posture | plaintext, anonymous, in-memory sessions — **mqttd's durable-by-default switched OFF** (`MQTTD_DURABLE_SESSIONS=0`) for like-for-like, disclosed | durable plane on for the durable lane, off for fan-out; per-host disk barrier floor measured before every lane |
| Self-check | mqttd run first and last as a control; > 5% drift or a moved knee voids the run | barrier probes gate the durable curve; driver-limited rungs excluded from knee detection; counter mismatches flagged |
| Cost to reproduce | ~€5 of cloud time, ~2.5 h | `./run.sh smoke` ~20 min under €0.50; full 1+3+5 a few euros |

```sh
cd bench/scale && export HCLOUD_TOKEN=…
COMPARE_BROKERS="mqttd mosquitto emqx hivemq" \
COMPARE_RATES="15000 30000 45000 60000 75000 90000 120000 150000" \
BROKER_TYPE=ccx23 DRIVER_TYPE=ccx43 DRIVER_COUNT=3 ./run.sh compare
python3 summarize-compare.py .runs/<stamp>/results
```

Latency is reported as emqtt-bench histogram **bucket upper bounds** ("p99 ≤ X ms"): coarse,
but incapable of flattering, and the same instrument for every broker. If a configuration
misrepresents a broker, open an issue and the lane is re-run with a dated changelog line.

### Where competitors win

- **Memory:** Mosquitto holds its knee in **18 MiB, 7× less than mqttd's 133 MiB**, and 4 MiB at 15,000 msg/s. If memory is the budget, it wins outright.
- **Tail at their own knee:** Mosquitto and HiveMQ CE stay ≤ 100 ms at their knees; mqttd's knee is higher, not quieter (≤ 500 ms).
- **Quiet at low load:** EMQX is ≤ 1 ms p99 at 15,000 msg/s, matched by mqttd; Mosquitto is ≤ 5 ms.
- **Footprint:** NanoMQ (sub-MB binary claims, ~4.6 MB image) and Mosquitto (a few-MB C daemon) beat everyone; mqttd's distroless image is ~14 MB.
- **Maturity and track record:** Mosquitto since ~2010, EMQX at enormous fleet scale, VerneMQ a decade in production. **mqttd has signed releases but no production users yet.**
- **Hard memory cap:** Mosquitto has one; mqttd has a sampled watermark and brownout, and the container limit is the real bound.
- **Feature surface:** EMQX's dashboard, SQL rule engine and MQTT-SN/CoAP gateways have no equivalent here.
- **Scale-out is not yet linear, and is not claimed to be:** 3 nodes ≈ 1 node on the durable path (the quorum tax), and durable ownership capacity scales with the voter set (default 5), not the node count. The plan is [#537](https://github.com/mbilling/fss-mqtt-broker/issues/537).
- **What the comparison does not cover:** one instance type, one full ladder, QoS 0 only, plaintext, no persistence, no cluster, no TLS. A broker fast here may be slow where a guarantee is real; the clustered and TLS postures are separate lanes. Newer Mosquitto (2.0.22 / 2.1.2) and EMQX (6.x, BSL) lines exist and were not measured.

---

## Feature Comparison Matrix

Legend: ✅ in the open build · 💰 **paid edition only** · ⚠️ partial (see [COMPARISON.md](docs/COMPARISON.md) for the note) · ✖ absent · n/v not verified.
Competitor cells come from vendor documentation researched 2026-07-29 → 2026-08-03 and are
re-verified each release; corrections welcome.

| Feature | mqttd | Mosquitto 2.x | EMQX 6.x | HiveMQ CE 2024.3 | VerneMQ 2.1 | NanoMQ 0.25 |
|---|---|---|---|---|---|---|
| MQTT 3.1.1 + 5.0 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| QoS 0/1/2, retained, LWT | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Shared subscriptions | ✅ cluster-wide | ✅ single node | ✅ | ✅ single node | ⚠️ | ⚠️ |
| WebSocket | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| QUIC listener | ✅ | ✖ | ✅ | ✖ | ✖ | ⚠️ bridge-client only |
| TLS 1.3 default, hardened 1.2 opt-in | ✅ | ⚠️ OpenSSL | ⚠️ | n/v | ⚠️ | ⚠️ |
| mTLS client certs | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| OIDC / JWT built in | ✅ live JWKS | ✖ plugin | ✅ | 💰 | ✖ plugin | ✖ |
| Policy reload evicts **live** sessions | ✅ | ⚠️ not documented | n/v | n/v | ⚠️ not documented | ⚠️ |
| Tamper-evident audit log | ✅ | ✖ | n/v | n/v | ✖ | ✖ |
| **Clustering** | ✅ masterless | ✖ | 💰 production clustering (BSL 1.1) | 💰 enterprise only | ✅ (binaries EULA-paid for production) | ✖ |
| **Sessions replicated across nodes** | ✅ default, quorum | n/a | ⚠️ opt-in | 💰 | ✖ lost on node death | ✖ |
| Acked QoS 1/2 survives node loss | ✅ proven under SIGKILL/partition | n/a | n/v | 💰 | ✖ | ✖ |
| Data-safe grow/shrink/replace | ✅ | n/a | n/v | 💰 | ⚠️ | n/a |
| Prometheus metrics | ✅ + OTLP push | `$SYS` only | ✅ | 💰 | ✅ | ✅ |
| Kubernetes Helm chart + operator | ✅ both | ✖ | ✅ | 💰 | ⚠️ | ⚠️ |
| Online backup/restore | ✅ | ⚠️ persistence file | n/v | 💰 | n/v | n/v |
| Bridge to other zones | ✅ standalone, deny-by-default | ✅ | ✅ | ✖ | ✅ basic | ✅ |
| Rule engine / SQL | ✖ by design | ✖ | ✅ | ✖ | ✖ | ✅ |
| Dashboard / HTTP admin API | ✖ by design | ✖ | ✅ | 💰 control center | ✅ CLI/API | ✅ |
| MQTT-SN / CoAP gateways | ✖ | ✖ | ✅ | ✖ | ✖ | ✖ |
| Signed, reproducible builds + SBOM + SLSA | ✅ | ✖ | n/v | n/v | ✖ | ✖ |
| FIPS build variant | ✅ | ✖ | n/v | 💰 | ✖ | ✖ |
| Memory-safe implementation | ✅ Rust, no `unsafe` | C | Erlang | Java | Erlang | C |
| **License** | **Apache-2.0, everything** | EPL/EDL | BSL 1.1 | Apache-2.0 (CE) | Apache-2.0 source, EULA binaries | MIT |

The rows that matter to a budget are the 💰 ones: in mqttd, clustering, replicated
sessions, metrics, Kubernetes tooling, backup and FIPS are in the same free build as
everything else. HiveMQ cells are limited to the vendor's published edition split; the
full per-cell notes, sources and every losing cell are in [docs/COMPARISON.md](docs/COMPARISON.md).

---

## Installation

Releases are cut from signed tags: every artifact is **reproducible**, **cosign-signed**
(keyless), carries **SLSA provenance**, and ships with a **CycloneDX SBOM**. Binaries are
static musl builds for `linux/amd64` and `linux/arm64`. Current release: **v1.0.17**.

**Docker**

```sh
docker run -d --name mqttd \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -v mqttd-data:/var/lib/mqttd -p 8883:8883 -p 8080:8080 \
  -e MQTTD_TLS_BIND=0.0.0.0:8883 -e MQTTD_TLS_CERT=/etc/mqttd/tls/server.crt \
  -e MQTTD_TLS_KEY=/etc/mqttd/tls/server.key -e MQTTD_ACL_FILE=/etc/mqttd/acl.toml \
  -e MQTTD_DATA_DIR=/var/lib/mqttd -e MQTTD_HEALTH_BIND=0.0.0.0:8080 \
  -v "$PWD/pki":/etc/mqttd/tls:ro -v "$PWD/acl.toml":/etc/mqttd/acl.toml:ro \
  ghcr.io/mbilling/fss-mqtt-broker:1.0.17

# verify the image before trusting it
cosign verify ghcr.io/mbilling/fss-mqtt-broker:1.0.17 \
  --certificate-identity-regexp 'https://github.com/mbilling/fss-mqtt-broker/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The image is distroless, non-root (uid 65532), and `/var/lib/mqttd` is the only writable path.

**Kubernetes (Helm)**

```sh
NS=mqttd REPLICAS=3 ./deploy/helm/mqttd/bootstrap.sh    # mints gossip key, TLS, one bus cert per node
helm install mqttd deploy/helm/mqttd -n mqttd \
  --set replicaCount=3 \
  --set secrets.tls.secretName=mqttd-tls \
  --set secrets.peerTls.secretName=mqttd-peer-tls \
  --set secrets.gossipKey.secretName=mqttd-gossip
```

Operator (`MqttdCluster` CRD, `v1alpha1`): `deploy/helm/mqttd-operator`. Full guide: [KUBERNETES.md](docs/KUBERNETES.md).

**Compose / systemd**

```sh
cd deploy/compose && ./bootstrap.sh && docker compose up -d     # three TLS nodes on one host
```

Bare metal: [`deploy/systemd/`](deploy/systemd/) ships a hardened unit and a cert minter; see the [secured three-node tutorial](docs/SECURED-CLUSTER-TUTORIAL.md).

**Binaries**

Download `mqttd-<version>-<arch>-unknown-linux-musl` (and `mqttd-fips-…`) from
[GitHub Releases](https://github.com/mbilling/fss-mqtt-broker/releases); verification and
reproduction one-liners are in [RELEASING.md](RELEASING.md).

**From source** (Rust ≥ 1.88)

```sh
git clone https://github.com/mbilling/fss-mqtt-broker && cd fss-mqtt-broker
cargo build --release --bin mqttd
cargo install --locked --path tools/mqttui   # optional: the runnable map of every demo/test/migration script
```

---

## Configuration

Layered **defaults < TOML file < `MQTTD_*` env < CLI flags**, strict schema (unknown keys
fail with a located error), secrets referenced by path so the file is safe to commit or
mount from a ConfigMap. Minimal secured single node:

```toml
[node]
id = "node-1"
data_dir = "/var/lib/mqttd"

[listeners]
tls_bind    = "0.0.0.0:8883"
health_bind = "0.0.0.0:8080"      # /livez, /readyz, /metrics

[tls]
cert      = "/etc/mqttd/tls/server.crt"
key       = "/etc/mqttd/tls/server.key"
client_ca = "/etc/mqttd/tls/client-ca.crt"   # require client certificates

[security]
allow_anonymous = false
acl_file = "/etc/mqttd/acl.toml"             # deny by default
```

```sh
mqttd --check-config --config /etc/mqttd/mqttd.toml   # validates without binding a port
kill -HUP "$(pidof mqttd)"                             # hot-reload policy, TLS material, quotas
```

Fully commented template: [`docs/mqttd.example.toml`](docs/mqttd.example.toml).
Generated, CI-checked reference of every key: [**CONFIGURATION.md**](docs/CONFIGURATION.md).

---

## Production Deployment

**Sizing.** Defaults are safe against attackers, not against success: connections, sessions,
retained topics and disk are uncapped until you cap them. Nine numbers bound a node —
data dir, disk watermark, max connections, packet size, sessions, offline queue depth,
per-subscriber backlog, retained topics, auth-penalty threshold. The arithmetic and a
ready-made preset ([`docs/examples/bounded-node.toml`](docs/examples/bounded-node.toml)) are in
[**SIZING.md**](docs/SIZING.md). Rule of thumb: keep the memory watermark at 75–85% of the
container limit; the container limit stays the hard bound.

**Clustering.**
- Run **three or more nodes, never two**: a two-node durable cluster has *worse* write availability than one (write quorum 2-of-2). Go 1 → 3 in one motion.
- Exactly one node boots with an empty seed list (the founder); every other node seeds off any member. On Kubernetes the chart does this; on Compose/systemd it is yours.
- One cluster-bus certificate **per node** — identity is bound to the certificate CN, and a shared certificate drops every peer link.
- Grow by starting a node; shrink with `SIGUSR1` (drain, then leave); replace by grow-then-decommission. Rolling upgrades ride the same one-node-at-a-time motion.
- Details: [Resizing](README.md#resizing-the-cluster), [OPERATIONS.md](docs/OPERATIONS.md).

**Hardening checklist** (the full 34-item L1/L2 baseline with auditor-runnable checks is [**HARDENING.md**](docs/HARDENING.md)):
- [ ] Startup log has **no `INSECURE:` lines** (`grep INSECURE:` — every hit names the fix).
- [ ] `MQTTD_DATA_DIR` set and on a volume (durable-on without it refuses to start).
- [ ] No plaintext listener; TLS 1.3 only; client certs carry the `clientAuth` EKU.
- [ ] Anonymous off; passwords are Argon2id via `mqttd --hash-password`; file mode ≤ 640.
- [ ] `MQTTD_ACL_FILE` set, `default = "deny"`.
- [ ] Connection, session, retained and queue caps set; disk and memory watermarks set.
- [ ] Cluster bus mTLS with per-node certs; gossip key from a file, not inline.
- [ ] CRL path configured and reload tested; policy reload audited.
- [ ] Container: `--read-only --cap-drop ALL --security-opt no-new-privileges`, or the shipped systemd unit.

---

## Monitoring & Observability

- **Metrics:** Prometheus text on `GET /metrics` (health port or a separate `MQTTD_METRICS_BIND`), and/or OTLP push (`MQTTD_OTLP_ENDPOINT`). Connections, publish/deliver, sessions, retained convergence, cluster membership, lease role/epoch, durable-append latency, hub dispatch time, quota rejections, security reloads — bounded label sets, no per-client or per-topic cardinality.
- **Probes:** `/livez` (hub draining), `/readyz` (member floor + lease readiness + decommission progress), `/statusz` (operator state surface). `mqttd --probe /readyz` for distroless health commands.
- **Audit:** hash-chained JSON records of every auth/authz decision, reload and eviction; schema and verifier in [AUDIT-SCHEMA.md](docs/AUDIT-SCHEMA.md).
- **Dashboards and alerts:** provisioned Grafana dashboards for the broker and the bridge under [`deploy/observability/`](deploy/observability/), with per-alert runbooks in [OPERATIONS.md](docs/OPERATIONS.md).
- **See it live:** `cd demo && docker compose up --build` brings up a durable cluster with Grafana + Prometheus + Alloy and a load generator at `localhost:3000`.

---

## Architecture

```text
        MQTT clients  (TCP · TLS 1.3 · WebSocket · QUIC)
              │  identity: mTLS-CN / password / JWT / OIDC
              ▼        ↓ deny-by-default topic ACL
       ┌──────────────────────────────────────────┐
       │  node                                    │
       │   listeners → per-connection tasks       │
       │                    │                     │
       │                    ▼                     │
       │            hub (routing actor)           │   one hub per node owns
       │        subscriptions · retained · queues │   routing; no lock on the
       │                    │                     │   publish hot path
       └────────────────────┼─────────────────────┘
                            │
   ┌────────────────────────┼────────────────────────┐
   │ SWIM gossip            │ peer links (mTLS)      │  one trust domain =
   │ membership, interest   │ interest-based forward │  one logical broker
   └────────────────────────┼────────────────────────┘
                            ▼
              durable plane — openraft lease group
              epoch-fenced quorum replication of
              sessions, queues and retained state
```

**Design choices, in one line each**
- **Shared-nothing nodes, one routing actor per node**: connection tasks never share mutable session state; cross-node traffic is just another command source into the same hub.
- **Durability is a write before the wire send**, not after: that is what makes "acked means replicated" true, and it is why clean sessions and QoS 0 skip it entirely.
- **Consensus for control, not for the data path**: the lease group mints epochs and ownership; per-session logs replicate to a small replica set, epoch-fenced against stale owners.
- **Refuse at the edge**: every quota answers with a reason code or backpressure; nothing acked is silently dropped.
- **Security decisions are recorded, then enforced**: 77 ADRs in [`docs/adr/`](docs/adr/), each with a delivery record and evidence.
- **The bridge is a separate process** with its own identity and failure domain, so a compromised far side never lands inside the broker.

Contributor tour: [ARCHITECTURE.md](docs/ARCHITECTURE.md). Threat model: [THREAT-MODEL.md](docs/THREAT-MODEL.md).

---

## Documentation

The full index, one line per document and stakeholder, is [**docs/README.md**](docs/README.md).

| You want to… | Read |
|---|---|
| Decide whether to run it | [EVALUATION.md](docs/EVALUATION.md), [COMPARISON.md](docs/COMPARISON.md) |
| Stand up a secured cluster | [SECURED-CLUSTER-TUTORIAL.md](docs/SECURED-CLUSTER-TUTORIAL.md), [KUBERNETES.md](docs/KUBERNETES.md) |
| Write a client against it | [CLIENT-GUIDE.md](docs/CLIENT-GUIDE.md) |
| Operate it day 2 | [OPERATIONS.md](docs/OPERATIONS.md), [SIZING.md](docs/SIZING.md), [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) |
| Audit it | [THREAT-MODEL.md](docs/THREAT-MODEL.md), [HARDENING.md](docs/HARDENING.md), [compliance/](docs/compliance/) |
| Migrate to it | [MIGRATION.md](docs/MIGRATION.md) |
| Understand a decision | [docs/adr/](docs/adr/), [delivery dashboard](docs/delivery/STATUS.md) |
| Look up a term | [GLOSSARY.md](docs/GLOSSARY.md) |

---

## Roadmap

Open work is tracked as tasks under their ADRs on the [delivery dashboard](docs/delivery/STATUS.md);
the items below are the ones an evaluator is most likely to ask about.

- **Cross-broker benchmarks beyond one node:** the single-node knee is published; still owed are the multi-host cluster comparison ([#244](https://github.com/mbilling/fss-mqtt-broker/issues/244)), larger instance types, the TLS/auth posture, and the per-release re-run ([#545](https://github.com/mbilling/fss-mqtt-broker/issues/545)).
- **Horizontal scale, measured then optimised** — the ordered plan is [#537](https://github.com/mbilling/fss-mqtt-broker/issues/537): scale-out durable ownership beyond the voter set (ADR 0073), workload-shaped arms including burst and per-tenant capacity (ADR 0077), and QoS 0 shared-worker capacity ([#482](https://github.com/mbilling/fss-mqtt-broker/issues/482)).
- **Auth fast-follows:** SCRAM, OCSP, PSK cipher suites for constrained devices, server-initiated re-authentication.
- **Migration from NanoMQ** ([#546](https://github.com/mbilling/fss-mqtt-broker/issues/546)); an assessable bridge demo with a second security zone ([#547](https://github.com/mbilling/fss-mqtt-broker/issues/547)).
- **Security legibility:** OSS-Fuzz onboarding ([#553](https://github.com/mbilling/fss-mqtt-broker/issues/553)) and a funded third-party audit of the 1.0 line ([#554](https://github.com/mbilling/fss-mqtt-broker/issues/554)).
- **Routing:** bloom subscription digests for sub-linear fan-out; MQTT 5 Server-Reference redirect for clients that follow it.
- **Operator:** `MqttdCluster` CRD promotion from `v1alpha1`.

Deliberately **not** on the roadmap: a dashboard, an HTTP admin API, a SQL rule engine, MQTT-SN/CoAP gateways. Those are recorded decisions ([ADR 0020](docs/adr/0020-metrics-and-observability.md), [ADR 0063](docs/adr/0063-external-consumer-integration.md)), not gaps waiting for time.

---

## Security Policy

**Do not open a public issue for a suspected vulnerability.** Report privately through
GitHub's coordinated-disclosure channel:
<https://github.com/mbilling/fss-mqtt-broker/security/advisories/new>.

What to expect, what is in scope (gossip plane, client codec, peer bus, config/auth
parsers, the durability contract itself), how fixes ship on every supported line, and the
continuous fuzzing that backs it: [**SECURITY.md**](SECURITY.md). Per-release
"is mqttd affected by CVE-X?" answers: [security/vex/](security/vex/). Supported release
lines and fix timelines: [SUPPORT.md](SUPPORT.md).

---

## Support & Community

- **Bug reports and questions:** [GitHub Issues](https://github.com/mbilling/fss-mqtt-broker/issues). Issues are the project's channel today; there is no chat room or discussion forum yet.
- **Release notes:** [GitHub Releases](https://github.com/mbilling/fss-mqtt-broker/releases) (canonical; [CHANGELOG.md](CHANGELOG.md) explains why).
- **Support lifecycle:** the three most recent minor lines receive security and correctness patches; adjacent-release skew is the supported upgrade path ([SUPPORT.md](SUPPORT.md)).
- **Commercial support:** the project's model reserves paid offerings for support, SLAs and certified builds — never for features. No commercial offering is published in this repository yet; open an issue to start the conversation.
- **Status:** `v1.0.17` is released, signed and verifiable, and is the exact image measured in the [single-node comparison](docs/benchmarks/SINGLE-NODE-COMPARISON.md). There are **no production users yet**; that is stated here rather than discovered.

---

## Contributing

Bug reports, questions and patches are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md):
the build and test gates, the two house conventions (decisions live in ADRs and progress in
delivery docs; a task title is a claim that must be true), and the review bar for prose —
nothing user-facing may actively mislead. Participation is governed by the
[Code of Conduct](CODE_OF_CONDUCT.md).

```sh
cargo build && cargo test && cargo clippy --all-targets && cargo deny check
./scripts/interop/run.sh          # foreign-client conformance (needs mosquitto-clients)
mqttui --list                     # every runnable demo, smoke and migration script
```

---

## License

[Apache-2.0](LICENSE). Every crate, every release binary, every container image.
No paid tier, no feature gates.
