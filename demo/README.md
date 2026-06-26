# mqttd demo: 3-node cluster + Grafana / Prometheus / Alloy

A one-command demo that brings up a **3-node durable `mqttd` cluster** wired to a
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
| `localhost:1883` / `1884` / `1885` | the three brokers' MQTT ports |
| <http://localhost:8080/metrics> | node-1's raw Prometheus exposition |

Tear down (and wipe the durable volumes):

```sh
docker compose down -v
```

## What's running

```
 mqttd-1 (founder) ─┐
 mqttd-2 ───────────┤  SWIM gossip mesh + durable lease group (ADR 0006/0007/0018)
 mqttd-3 ───────────┘
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

The cluster runs **ephemeral (in-memory) sessions** — SWIM-clustered but without the durable
lease group. Expect `cluster_members = 3`, `peer_links = 2` per node, and `members{alive} = 3`.

> The `lease_*` and `durable_append_*` panels are intentionally empty here: they only exist
> in **durable** mode (`MQTTD_DURABLE_SESSIONS=1`), which is disabled because the all-voters
> lease group currently churns at 3 nodes (re-electing ~1×/s). Bounded voters — the fix —
> are ADR 0021, still *Proposed*. Enable durable mode (and a per-node `MQTTD_DATA_DIR`) to
> exercise those panels once that lands.

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
