# Bridge deployment topologies

How to run the boundary bridge ([ADR 0025](adr/0025-boundary-bridge.md)) as a
single instance or as an HA pair, and what each one does and does not buy you.

The bridge is an ordinary **MQTT client of both sides** — not a broker plugin and
not a cluster member. It subscribes on the local cluster, applies a
deny-by-default forwarding policy, and republishes to the upstream. That is why
it is a separate process with its own identity, credentials and failure domain:
it is the thing that crosses a trust boundary, so a compromise of the far side
lands in the bridge, not inside the broker.

---

## Standalone — one instance

The simplest shape, and the right one until you need the bridge to survive its
own restart without a gap.

```
              LOCAL / TRUSTED ZONE                    ┊        PARTNER ZONE
                                                      ┊
   ┌──────────────────────────────────────┐           ┊
   │  mqttd cluster                       │           ┊
   │   ┌────────┐  ┌────────┐  ┌────────┐ │           ┊
   │   │ node-0 │  │ node-1 │  │ node-2 │ │           ┊
   │   └────────┘  └────────┘  └────────┘ │           ┊
   └───────────────────┬──────────────────┘           ┊
                       │                              ┊
        subscribe  "telemetry/#"   (plain filter)     ┊
                       │  every message → this one    ┊
                       ▼          subscriber          ┊
             ┌───────────────────────┐                ┊
             │      mqtt-bridge      │                ┊
             │  client_id = <unique> │                ┊
             │                       │                ┊
             │   ┌───────────────┐   │                ┊
             │   │     spool     │   │  buffers while ┊
             │   │  (bounded)    │   │  the upstream  ┊
             │   └───────────────┘   │  is down       ┊
             └───────────┬───────────┘                ┊
                         │                            ┊
                publish  │  direction = "out" ONLY    ┊
                         │  (never subscribes there)  ┊
                         └────────────────────────────┊──►  ┌──────────────────┐
                                                      ┊     │ partner broker   │
                                                      ┊     │ (any vendor)     │
                                                    TRUST    └──────────────────┘
                                                   BOUNDARY
```

```toml
share_group = ""              # no group: this instance takes the whole stream
[local]
url = "mqttd:1883"
client_id = "bridge-a"        # still give it one; see below
```

**What you get:** the crossing works, and an upstream outage is buffered in the
spool and replayed on reconnect.

**What you don't:** while the bridge itself is restarting, nothing is forwarded.
Messages published during that window are not queued anywhere for it — the local
side's persistent session holds its subscription, so a *brief* restart resumes and
catches up, but a long outage or a lost pod is a gap.

---

## HA — two or more instances

```
   ┌──────────────────────────────────────┐
   │  mqttd cluster                       │   The group is CLUSTER-WIDE
   │   ┌────────┐  ┌────────┐  ┌────────┐ │   (ADR 0015): members may be
   │   │ node-0 │  │ node-1 │  │ node-2 │ │   attached to different nodes
   │   └────────┘  └────────┘  └────────┘ │   and it still load-balances.
   └────────┬────────────────────┬────────┘
            │                    │
            │  subscribe  "$share/edge-bridges/telemetry/#"
            │                    │
     ONE copy of each message  →  exactly ONE member
            │                    │
            ▼                    ▼
  ┌────────────────────┐  ┌────────────────────┐
  │  mqttd-bridge-0    │  │  mqttd-bridge-1    │
  │                    │  │                    │
  │  client_id =       │  │  client_id =       │
  │  bridge-mqttd-     │  │  bridge-mqttd-     │
  │  bridge-0          │  │  bridge-1          │
  │        ▲           │  │        ▲           │
  │        └── DISTINCT ids ⇒ separate sessions │
  │                    │  │                    │
  │  ┌──────────────┐  │  │  ┌──────────────┐  │
  │  │   spool 0    │  │  │  │   spool 1    │  │  per-replica PVC —
  │  │  (its PVC)   │  │  │  │  (its PVC)   │  │  NOT shared
  │  └──────────────┘  │  │  └──────────────┘  │
  └─────────┬──────────┘  └──────────┬─────────┘
            │                        │
            └───────────┬────────────┘
                        ▼
              ┌────────────────────┐
              │  partner broker    │   each message crosses ONCE
              └────────────────────┘
```

```toml
share_group = "edge-bridges"          # the group both replicas join
[local]
url = "mqttd:1883"
client_id = "bridge-__POD_NAME__"     # the chart substitutes the pod name
```

```sh
helm upgrade --install mqttd deploy/helm/mqttd \
  --set bridge.enabled=true --set bridge.replicaCount=2
```

### The two "shared" things are different

This is the part that trips people up, so it is worth stating plainly:

| | shared between replicas? | keyed by |
|---|---|---|
| **Subscription group** (`$share/<group>/...`) | **yes** — that is the point | the group name |
| **Session** | **no** — each replica has its own | the **client id** |

Joining a shared subscription does **not** mean sharing a session. Each member is
a separate client, with its own session, its own in-flight state, and its own
spool.

### Why a shared client id breaks it

MQTT identifies a session by client id, and requires the broker to disconnect the
existing client when a new one connects with the same id. mqttd implements that
(`session_takeover_publishes_will`), as it must.

The bridge's built-in default id is a **constant** — `fss-bridge-local` — so
replicas that don't set one collide:

```
   bridge-0 connects as fss-bridge-local   →  accepted
   bridge-1 connects as fss-bridge-local   →  broker evicts bridge-0   (spec-mandated)
   bridge-0 reconnects                     →  broker evicts bridge-1
   bridge-1 reconnects                     →  broker evicts bridge-0
                                              … indefinitely
```

Neither replica stays connected long enough to be a useful group member, and every
takeover fires the evicted client's Will. Two healthy processes, configured for
redundancy, produce an outage.

The local side also uses a **persistent** session (`clean_session=false`) so a brief
restart resumes its subscription — which means colliding replicas would be
inheriting and clobbering each other's stored queue, not merely stealing a socket.

Put `__POD_NAME__` in every `client_id` and the chart's init container substitutes
the pod's name. CI fails the build if any `client_id` loses it
(`scripts/k8s/check-bridge-chart.sh`).

---

## What HA does and does not cover

**Covered.** If a replica dies, the broker stops delivering its share and the
survivors take the whole stream. Forwarding continues with no duplicates, because
the group delivers each message once regardless of how many members are up.

**Not covered — and worth knowing before you rely on it.** A spool is replayed
only by the replica that owns it, on *its* reconnect. So messages already buffered
by a replica that is down stay buffered until **that** replica returns:

```
   bridge-1 spooled 5 000 messages while the partner was unreachable
   bridge-1's pod is deleted and does not come back
   → bridge-0 keeps forwarding the LIVE stream normally
   → those 5 000 remain on bridge-1's PVC, unsent, until a pod re-attaches to it
```

A StatefulSet re-attaches the same PVC to the same ordinal, so a restarted or
rescheduled replica drains its own backlog. **Scaling down does not** — the
retired ordinal's volume keeps its spool with nobody to replay it. Drain before
scaling down, or accept the backlog is stranded.

Watch it with:

```promql
fss_bridge_spool_depth                       # per side, per replica
fss_bridge_connected == 0                    # a side that is down
rate(fss_bridge_dropped_total{reason="spool-full"}[5m])   # actual message loss
```

The Grafana dashboard at
[`../demo/grafana/dashboards/mqttd-bridge.json`](../demo/grafana/dashboards/mqttd-bridge.json)
has panels for all three.

---

## Choosing

| | standalone | HA (2+) |
|---|---|---|
| Duplicate forwarding | n/a | prevented by the share group |
| Survives a bridge restart without a gap | ✖ | ✔ |
| Survives losing a bridge pod | ✖ | ✔ (live stream) |
| Buffered backlog survives losing that pod | ✔ (same PVC on restart) | ✔ only when the ordinal returns |
| Distinct `client_id` required | good practice | **mandatory** |
| Upstream credentials | one set per upstream | the same set, used by every replica |

Start standalone. Move to HA when a forwarding gap during a restart is a problem
— and set `share_group` and per-replica `client_id` together, since either one
without the other is broken in a different way.
