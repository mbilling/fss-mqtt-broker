# mqttd demo: 7-node cluster + Grafana / Prometheus / Alloy

> **Experiment branch (`experiment/7-node-demo`).** Scaled up from the baseline 3-node demo
> to exercise a larger durable cluster. The node count is adjustable — see
> [Scaling the cluster](#scaling-the-cluster) below.

A one-command demo that brings up a **7-node durable `mqttd` cluster** wired to a
**Grafana + Prometheus + Grafana Alloy** observability stack, with a small load
generator driving cross-node traffic and a dashboard that showcases every metric the
broker exports (ADR 0020).

```sh
cd demo
docker compose up --build
```

First build compiles the broker (a few minutes); subsequent starts are fast. Then open:

| URL | What |
|-----|------|
| <http://localhost:8088> | **MQTT playground** → a browser page to spin up MQTT sessions, publish, and watch subscribers (see below) |
| <http://localhost:3000> | **Grafana** → dashboard **"mqttd — broker overview"** (anonymous admin) |
| <http://localhost:9090> | Prometheus |
| <http://localhost:12345> | Alloy UI (pipeline graph, OTLP receiver, scrape targets) |
| `localhost:1883` / `1884` / `1885` / `1887` / `1888` / `1889` / `1891` | the seven brokers' MQTT ports (mqttd-1..7) |
| <http://localhost:8080/metrics> | node-1's raw Prometheus exposition |

Tear down (and wipe the durable volumes):

```sh
docker compose down -v
```

## Scaling the cluster

Docker Compose can't loop to create N distinct stateful brokers (each node needs its own
node id, peer/SWIM bind, data volume, and host ports), so the per-node topology is **generated**.
`demo/scale-cluster.py` is the single source of truth — it rewrites the marked regions of
`docker-compose.yml`, `alloy/config.alloy`, `loadgen.sh`'s node list, and the QUIC server-cert
SAN, and sets `MQTTD_READY_MIN_MEMBERS` to the majority quorum:

```sh
python3 demo/scale-cluster.py 5     # scale to 5 nodes (default is 7)
cd demo && docker compose down -v && docker compose up --build
```

`mqttd-1` (the founder / playground / QUIC entry point) is hand-written; only the homogeneous
followers `mqttd-2..N` are generated. Host ports are assigned deterministically and skip ports
already taken by other services, so there are no collisions as N changes. Valid range: 1–20 nodes.

## What's running

```
 mqttd-1 (founder) ─┐
 mqttd-2 ───────────┤
   …                ┤  SWIM gossip mesh + durable lease group (ADR 0006/0007/0018)
 mqttd-7 ───────────┘  (7 nodes; adjust with demo/scale-cluster.py)
     │  │
     │  └── Prometheus /metrics ──────── scraped by ─┐
     └───── OTLP/HTTP push ── received by ───────────┤
                                                     ▼
                                          Grafana Alloy ── remote_write ──▶ Prometheus ──▶ Grafana
```

Both metric paths are live:

- **Prometheus pull** — Alloy scrapes each broker's `/metrics` every 5 s. The dashboard
  queries these `mqttd_*` series (stable, prefixed, one `instance` label per node).
- **OTLP push** — each broker also pushes OTLP/HTTP to Alloy (`MQTTD_OTLP_ENDPOINT`),
  which converts and remote-writes it too. Watch it arrive in the Alloy UI's pipeline
  graph, or `docker compose logs alloy`. This exercises the broker's in-process OTLP
  exporter end to end.

The cluster runs **durable, consensus-backed sessions** — the default (ADR 0029). Each broker
persists its lease group and replicated session log to its own `/data` volume. Expect
`cluster_members = 7`, `peer_links = 6` per node, and `members{alive} = 7` once the mesh forms.

> The `lease_*` and `durable_append_*` panels populate once the lease group elects a leader
> (~90 s). **Bounded lease voters** (ADR 0021/0028, both Accepted) cap the voter set, so the
> group stays stable at 7 nodes instead of the all-voters churn earlier revisions hit — which
> is what makes scaling this demo up viable. Set `MQTTD_DURABLE_SESSIONS=0` to fall back to the
> bounded in-memory store (then those panels stay empty).

The **loadgen** keeps a persistent QoS-1 subscriber on node-2 and publishes QoS-1 +
retained messages on node-1, so publishes route **across nodes** — populating
publish/deliver, sessions, subscriptions, retained, and inflight panels.

> Security note: this demo runs **plaintext** MQTT, **plaintext** peer links, and
> **unauthenticated** gossip — all loudly logged as insecure by the broker. It exists to
> showcase metrics, not as a deployment template. See the top-level README for the
> secure-by-default production knobs (TLS, mTLS cluster bus, gossip keys, ACLs).

## The dashboard

`grafana/dashboards/mqttd.json` (provisioned automatically) has a **Node** variable to
filter by broker and panels grouped roughly as:

- **Overview** — cluster size, active connections, sessions, retained.
- **Throughput** — PUBLISH received/delivered per second by QoS; deliver-latency
  percentiles (p50/p95/p99) from the histogram; drops by reason.
- **Connections** — active by node, accepts by protocol/listener, connection errors by
  reason (auth / acl / keepalive / tls / accept).
- **State** — sessions / subscriptions / inflight per node.
- **Cluster** — members & peer links, members by SWIM state, lease leader/epoch, durable
  append p95 + failures by reason (no-quorum / not-owner / …).
- **Security** — gossip datagrams rejected by reason; build info.

## MQTT playground (browser)

Open <http://localhost:8088> (or `http://<host>:8088` over Tailscale/LAN). A small page to
exercise the cluster from a browser — or a phone:

- **+ Subscriber / + Publisher / + Session** — each spins up a **real MQTT client with a
  unique client-id**. Publishers send to a topic; subscribers subscribe to a filter
  (`room/1`, `sensors/#`, …).
- **Message log** — every message any subscriber receives, with its session, topic, and QoS.
- **+ live cluster feed** — subscribe a session to the loadgen's real `demo/#` traffic.

Open it in several tabs or on several devices; they all share the cluster — watch the
`publish_received` / `publish_delivered` panels in Grafana light up as you send.

How it works: the page (served by an `nginx` container on `:8088`) connects over
**native MQTT-over-WebSocket** ([ADR 0035](../docs/adr/0035-websocket-transport.md)) straight
to mqttd-1's WS listener (`MQTTD_WS_BIND`, host port `:8089`). Each browser tab is therefore a
**real mqttd session** — its own client-id, placement, durability, ACL, audit, metrics — no
mosquitto gateway, no bridge. (In production use `wss://` via `MQTTD_WSS_BIND`; this demo uses
plaintext `ws://` over the local network.)

## MQTT-over-QUIC showcase (ADR 0036)

Browsers can't speak MQTT-over-QUIC (no browser API exists), so a native QUIC client shows it
off. Every node runs a **native MQTT-over-QUIC listener** (`MQTTD_QUIC_BIND`, UDP) — QUIC
mandates TLS 1.3, so a one-shot `quic-certs` service mints a throwaway demo PKI (CA + server +
client certs) first. The **`quic-demo`** service then connects to the cluster over QUIC
(TLS 1.3 + mTLS, ALPN `mqtt`) and publishes a steady stream **across several QUIC data
streams** — multi-stream in action (ADR 0036).

See it from the browser playground: click **+ QUIC demo feed** on any session to subscribe to
`quic/demo/#` and watch the QUIC-originated messages arrive **through the cluster** (orange
lines), proving MQTT-over-QUIC interoperates end-to-end with WebSocket/TCP. In Grafana, the
**accepts-by-listener** panel shows the `quic` connection alongside `tls`/`plaintext`.

**Connection migration (ADR 0036 §3b).** The `quic-demo` client rebinds its UDP socket every 10s
(`QUIC_MIGRATE_MS`), simulating a network path change — a Wi-Fi↔cellular handover or NAT rebind.
QUIC keeps the **same** connection alive across it: no reconnect, no new TLS handshake, no new
CONNECT. The broker logs each migration and the **QUIC path migrations** counter
(`mqttd_quic_path_migrations_total`) ticks up — all while the `quic/demo/*` feed keeps flowing
uninterrupted:

```sh
# the broker logs the path change (same session, new client address):
docker compose logs -f mqttd-1 | grep "migrated to a new client path"
```

```sh
# the quic-demo client's log shows it connecting + publishing over QUIC:
docker compose logs -f quic-demo
```

mqttd-1's QUIC port is published on `localhost:8094/udp` for an external QUIC client (the demo
server cert's SAN covers the node names + localhost/127.0.0.1). Everything here is **demo-only**
(throwaway certs, plaintext for the other listeners); MQTT-over-QUIC is a non-standard,
EMQX-style extension, so interop is limited to clients that speak it.

## Boundary bridge (ADR 0025)

The demo also runs a **boundary bridge** between the cluster and a separate, isolated
`partner-broker` (a standalone `mqttd` that is **not** part of the mesh — a stand-in for a
broker in another security/administrative zone). The bridge is a standalone MQTT client to
*both* sides — not an in-process plugin — so the crossing is a small, isolated, auditable
unit whose failure domain is its own: a bridge fault cannot touch the cluster's consensus
or membership.

Its config — [`bridge/bridge.toml`](bridge/bridge.toml) — declares two mappings:

- **`telemetry/#` — unidirectional (`out`):** cluster telemetry flows to the partner only,
  remapped under `from-cluster/telemetry/…`. The one-wayness is enforced *in code* — for an
  `out` rule the bridge never subscribes to the topic on the partner — not merely as a
  setting, so the partner can never push back on this channel (a data-diode direction).
- **`shared/#` — bidirectional (`both`):** a command/response channel that crosses both
  ways; the hop-count user property (`fss-bridge-hop-count`, bounded by `hop_count_limit`)
  guarantees any forwarding loop self-terminates.

Try it (with `mosquitto` clients):

```sh
# one-way: a telemetry publish on the cluster reaches the partner, remapped...
mosquitto_sub -h localhost -p 1886 -t 'from-cluster/telemetry/#' -v &   # partner
mosquitto_pub -h localhost -p 1883 -t 'telemetry/room/temp' -m '21C'    # cluster
# ...but a publish on the partner under telemetry/# never crosses back (one-way enforced).

# both-way: shared/# crosses in either direction.
mosquitto_sub -h localhost -p 1883 -t 'shared/#' -v &                   # cluster
mosquitto_pub -h localhost -p 1886 -t 'shared/cmd' -m 'ping'            # partner -> cluster
```

The bridge exposes Prometheus metrics on `:8090` (`fss_bridge_forwarded_total`,
`…_dropped_total`, `…_reconnects_total`, scraped by Alloy) and writes an **audit** log line
(`bridge::audit`) for every message that crosses, recording the upstream, direction, and
source/destination topics.

**Production note (least privilege, ADR 0025 §8):** the demo uses plaintext and anonymous
access for convenience. A real boundary uses a **distinct mTLS identity per upstream** and a
**least-privilege account on each broker** — publish-only or subscribe-only on exactly the
bridged topics (an ACL on the bridge's account, ADR 0004) — so even a bridge code fault
cannot open a path the credentials forbid. Run ≥2 instances with a shared `share_group` for
HA (the cluster load-balances and deduplicates across them).

## Files

| Path | Purpose |
|------|---------|
| `docker-compose.yml` | the whole topology |
| `Dockerfile` | builds the `mqttd` binary on a slim runtime |
| `Dockerfile.bridge` | builds the `mqtt-bridge` binary (ADR 0025) |
| `bridge/bridge.toml` | the boundary-bridge config (one-way + bidirectional mappings) |
| `alloy/config.alloy` | Alloy: scrape `/metrics` (brokers + bridge) + receive OTLP → remote_write to Prometheus |
| `prometheus/prometheus.yml` | Prometheus (remote-write receiver) |
| `grafana/provisioning/*` | datasource + dashboard auto-provisioning |
| `grafana/dashboards/mqttd.json` | the dashboard |
| `loadgen/loadgen.sh` | cross-node MQTT traffic generator (mosquitto clients) |
