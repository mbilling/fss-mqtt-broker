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

*(GHCR does not publish a pull-count badge; the image badge above links to the package page.)*

---

## Why mqttd

- **Durable by default.** Every persistent session is quorum-replicated. An acked QoS 1/2 message survives the loss of the node that accepted it — queued *or in flight* — and a group too thin to keep that promise **refuses** the write rather than acking on one copy.
- **Revocation reaches live state.** Reload the policy and a revoked certificate, removed user, or tightened grant **evicts the already-connected client** — not at its next reconnect, now. No compared broker documents this.
- **Secure by default, loudly.** TLS 1.3, mTLS/OIDC identity, deny-by-default ACLs, tamper-evident audit. Every insecure mode is opt-in and logs `INSECURE:` on every start.
- **Clustering is not a paid feature.** One Apache-2.0 codebase, signed reproducible binaries, SBOM and SLSA provenance. EMQX gates production clustering behind BSL; VerneMQ's production binaries are EULA-paid; HiveMQ CE is single-node.
- **Claims you can check.** Every capability maps to a task with evidence on the [delivery dashboard](docs/delivery/STATUS.md); benchmarks print their losing cells; what is missing is listed in [Limitations](README.md#limitations), not left to be discovered.

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

**Honesty rules first** ([ADR 0048](docs/adr/0048-comparative-benchmarking.md)): versions pinned,
hardware and config disclosed, results dated, losing dimensions printed as prominently
as winning ones, and nothing single-host is ever published as a cluster number.

### Cross-broker results: not yet published

The [harness](bench/README.md) runs **mqttd, Mosquitto 2.0.20, EMQX 5.8.6, VerneMQ 2.1.1
and NanoMQ 0.25.5** under identical, disclosed postures, driven by
[emqtt-bench 0.6.3](https://github.com/emqx/emqtt-bench) — deliberately EMQX's own load
tool, so no home-field driver flatters us. **No cross-broker numbers are printed here**
because the publishable multi-host run has not happened yet
(tracked: [#545](https://github.com/mbilling/fss-mqtt-broker/issues/545)). The table
below is the shape of the result that will land, with the harness scenario each row comes from.

| Scenario (`bench/run.sh`) | mqttd | Mosquitto 2.0.20 | EMQX 5.8.6 | VerneMQ 2.1.1 | NanoMQ 0.25.5 |
|---|---|---|---|---|---|
| `conn` — connect rate, RSS per idle connection | *pending* | *pending* | *pending* | *pending* | *pending* |
| `pubsub-qos0` — msg/s, p50/p99/p999 | *pending* | *pending* | *pending* | *pending* | *pending* |
| `pubsub-qos1` | *pending* | *pending* | *pending* | *pending* | *pending* |
| `pubsub-qos2` | *pending* | *pending* | *pending* | *pending* | *pending* |
| `tls-conn` — connect rate under mTLS | *pending* | *pending* | *pending* | *pending* | *pending* |
| `tls-pubsub-qos1` — the security cost, shown | *pending* | *pending* | *pending* | *pending* | *pending* |

HiveMQ CE is not in the set: ADR 0048 compares self-hostable, like-for-like brokers and
HiveMQ CE has no clustering to compare against.

### What is published: mqttd against itself, on real hardware

The **scaling curve** ([SCALE-CURVE.md](docs/benchmarks/SCALE-CURVE.md), verified against
`v1.0.5`, 2026-08-24): the same workload against fresh 1-, 3- and 5-node clusters, one
dedicated host and one local NVMe disk per broker.

**Curve 1 — durable QoS 1, acked only after fsync + quorum replication** (48 closed-loop
publishers × window 8, 48 durable subscribers, 256 B, 60 s windows, median of 3):

| nodes | acked msg/s (saturating) | p99 (saturating) | p99 (uncontended) |
|---|---|---|---|
| 1 | 8,503 | 90 ms | 1.01 ms |
| 3 | 8,647 | 82 ms | 1.81 ms |
| 5 | **13,893** | 56 ms | 1.69 ms |

```text
durable QoS 1, acked msg/s
1 node  ████████████████████░░░░░░░░░░░░░   8.5k
3 nodes ████████████████████░░░░░░░░░░░░░   8.6k   (quorum tax fully absorbed)
5 nodes █████████████████████████████████  13.9k   (1.63× one node)
```

**Curve 2 — non-durable `$share` fan-out** (600 publishers → 300 subscribers in one shared
group): delivered plateau **~18.6k → ~53.9k → ~81.4k msg/s** at 1 → 3 → 5 nodes; every rung
above 50k offered was **driver-limited**, so these are floors, not capacities.

**Connections:** 50,000 idle connections cost a flat **19.3–19.7 KiB each** at every cluster size.

Two more published measurements, both labelled dev-grade single-host and never to be quoted
as capacity: the [durable path](docs/benchmarks/DURABLE-PATH.md) (a durable QoS 1 publish
costs ~28 ms p50 against ~0.03 ms to a clean session — the price of the guarantee) and the
[hot-path micro-baselines](docs/benchmarks/BASELINE.md) (256 B PUBLISH encodes in ~270 ns,
decodes in ~190 ns; a per-PR regression floor fails the build on a gross slowdown).

### Methodology

| | Cross-broker harness | Scaling curve |
|---|---|---|
| Script | [`bench/run.sh`](bench/run.sh), [`bench/summarize.py`](bench/summarize.py) | [`bench/scale/run.sh`](bench/scale/run.sh), [`summarize-curve.py`](bench/scale/summarize-curve.py) |
| Hardware | dedicated host, driver separated from broker (required for publication) | Hetzner CCX23 per broker (4 dedicated vCPU, 16 GB, local NVMe), CCX33 drivers, `fsn1` |
| Broker build | pinned images per broker, configs in [`bench/configs/`](bench/configs/) | released, cosign-signed, byte-reproducible `mqttd` binary, shipped systemd unit |
| Load tool | emqtt-bench 0.6.3 | emqtt-bench 0.6.3 (fan-out), `durable_bench` harness (durable lane, exact per-message RTTs) |
| Payload / QoS | 256 B; QoS 0, 1, 2; 5k connections; 60 s per scenario | 256 B; QoS 1 durable and QoS 2; QoS 1 `$share`; 50k idle connections |
| Postures | plaintext+anonymous (competitors' out-of-the-box) **and** mTLS with client certs required, on every broker | durable plane on for the durable lane, off for fan-out; per-host disk barrier floors measured before every lane |
| Fairness | mqttd runs with `MQTTD_DURABLE_SESSIONS=0` for like-for-like against brokers whose sessions are not quorum-replicated — disclosed, not hidden | a run judges itself: barrier probes gate the durable curve, driver-limited rungs are excluded from knee detection, counter mismatches are flagged |

Reproduce: `cd bench && ./run.sh smoke` (minutes, one machine) or
`cd bench/scale && ./run.sh smoke` (about 20 minutes, under €0.50 on Hetzner; the full 1+3+5
curve is a few euros). Every step is in the two READMEs.

### Where competitors win

- **Footprint:** NanoMQ (sub-MB binary claims, ~4.6 MB image) and Mosquitto (a few-MB C daemon) beat everyone; mqttd's distroless image is ~14 MB.
- **Maturity and track record:** Mosquitto since ~2010, EMQX at enormous fleet scale, VerneMQ a decade in production. **mqttd has signed releases but no production users yet.**
- **Hard memory cap:** Mosquitto has one; mqttd has a sampled watermark and brownout, and the container limit is the real bound.
- **Feature surface:** EMQX's dashboard, SQL rule engine and MQTT-SN/CoAP gateways have no equivalent here.
- **Scale-out is not yet linear, and is not claimed to be:** 3 nodes ≈ 1 node on the durable path (the quorum tax), and durable ownership capacity scales with the voter set (default 5), not the node count. The measurement-then-optimisation plan is [#537](https://github.com/mbilling/fss-mqtt-broker/issues/537).
- **Provisioning variance:** two identical cloud clusters running the same binary differed by ~40% in one campaign. Cross-version numbers carry that confound; read ratios within a run, not absolute cells across runs.

---

## Feature Comparison Matrix

Legend: ✅ in the open build · 💰 **paid edition only** · ⚠️ partial (see [COMPARISON.md](docs/COMPARISON.md) for the note) · ✖ absent · n/v not verified.
Competitor cells come from vendor documentation researched 2026-07-29 → 2026-08-03 and are
re-verified each release; corrections welcome.

| Feature | mqttd | Mosquitto 2.x | EMQX 6.x | HiveMQ CE | VerneMQ 2.1 | NanoMQ 0.25 |
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

- **Published cross-broker benchmarks** on dedicated hardware, re-run per release ([#545](https://github.com/mbilling/fss-mqtt-broker/issues/545)).
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
- **Status:** `v1.0.17` is released, signed and verifiable. There are **no production users yet**; that is stated here rather than discovered.

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
