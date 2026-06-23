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

## Files

| Path | Purpose |
|------|---------|
| `docker-compose.yml` | the whole topology |
| `Dockerfile` | builds the `mqttd` binary on a slim runtime |
| `alloy/config.alloy` | Alloy: scrape `/metrics` + receive OTLP → remote_write to Prometheus |
| `prometheus/prometheus.yml` | Prometheus (remote-write receiver) |
| `grafana/provisioning/*` | datasource + dashboard auto-provisioning |
| `grafana/dashboards/mqttd.json` | the dashboard |
| `loadgen/loadgen.sh` | cross-node MQTT traffic generator (mosquitto clients) |
