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
# client_id is optional: unset generates a per-instance, 23-byte-safe id
```

**What you get:** the crossing works, and an upstream outage is buffered in the
spool and replayed on reconnect.

**What you don't:** while the bridge itself is restarting, nothing is forwarded.
Messages published during that window are not queued anywhere for it — the local
side's persistent session holds its subscription, so a *brief* restart resumes and
catches up, but a long outage or a lost pod is a gap.

---

## HA — two or more instances

There are two HA topologies, selected by `ha` (ADR 0059). The default, **`partitioned`**, is
the one to use.

### Partitioned (the default)

Each instance owns a disjoint hash-slice of the topic space (`hash(topic) mod total ==
instance`). Every instance subscribes the **full** filter on **both** sides with a *plain*
subscription, and forwards only the topics it owns. So each message crosses **exactly once**,
and because one topic has exactly one owner, **per-topic order is preserved** — in **every**
direction, including `in`/`both` rules. There is no `$share`, no coordinator, and no shared
state; correctness depends only on each instance knowing its `instance` index and the `total`.

```toml
ha = "partitioned"   # the default — may be omitted
[local]
url = "mqttd:1883"
client_id = "bridge-__POD_NAME__"     # still distinct per replica (separate sessions)
```

```sh
helm upgrade --install mqttd deploy/helm/mqttd \
  --set bridge.enabled=true --set bridge.replicaCount=2
```

The chart sets **`MQTTD_BRIDGE_TOTAL`** from `replicaCount`, and the bridge derives its
**instance index from its pod-name ordinal** (`mqttd-bridge-0` → 0), so the same config ships
to every replica. Getting `total` wrong is the one footgun: too low **duplicates** inbound,
too high **strands** a slice — so wire it from the deployment (the chart does). A scale change
re-derives ownership across the survivors; a dead instance's slice waits for the operator
rebalance (or a returning ordinal), covered by each side's persistent session meanwhile.

### Shared (`$share`) — opt-in, `out`-only

`ha = "shared"` is the pre-ADR-0059 model: a cluster-side `$share` subscription load-balances
the local (`out`) stream. It is correct **only for `out` rules** — an `in`/`both` rule at ≥2
instances **double-delivers** inbound (the foreign broker has no `$share` for the bridge to
share on), and `$share` load-balances per message so **per-topic order is not preserved**.
Choose it only for `out`-only, ordering-insensitive deployments. It also **cannot cross
retained state** (a `$share` subscription receives no retained messages, MQTT-3.8.4).

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
ha = "shared"                         # opt in to the $share model
share_group = "edge-bridges"          # the group both replicas join
[local]
url = "mqttd:1883"
client_id = "bridge-__POD_NAME__"     # the chart substitutes the pod name
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

**The default is now safe.** An unset `client_id` is generated per instance, in
MQTT's *guaranteed-support* shape — at most 23 bytes of `[0-9a-zA-Z]`, which every
broker must accept:

```
  fssb   lo    ttdbridge0   cuv2nyb        →  fssblottdbridge0cuv2nyb
  ────   ──    ──────────   ───────
   4      2        10          7
  ours   side   host tail     hash
```

- `fssb` marks it as ours in someone else's broker logs.
- `lo` / `u0`…`uz` is the side (local, or upstream *n*).
- The host tail keeps the recognisable part of the pod name — the **first DNS
  label**, so an FQDN yields `bridge0`, not `usterlocal` from `…svc.cluster.local`.
- The hash is taken over the **full** host name and side, so truncation can never
  merge two hosts.

Kubernetes sets the hostname to the pod name, so replicas are distinct with no
configuration, and each id is stable across restarts (the local side keeps a
persistent session, so an id that changed on each start would orphan it). The
mapping is logged once at startup, so the opaque suffix is still traceable:

```
generated MQTT client id … client_id=fssblottdbridge0cuv2nyb host=mqttd-bridge-0 side=Local
```

**Why the strict shape on every side, not just upstreams?** A broker need only
support ids of 1–23 alphanumeric bytes; ours accepts anything. But the `local`
endpoint is just a URL a user configures — nothing guarantees it points at this
broker. "The near side is permissive" is an assumption the code cannot check, so
it does not make one.

Two replicas with no `client_id` at all now run correctly.

It used to be the **constant** `fss-bridge-local`, and setting the same id
explicitly on two replicas still reproduces the failure exactly:

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

Measured, two replicas over 30 seconds:

| | reconnects per replica | forwarding |
|---|---|---|
| **shared** client id | **73** | unusable |
| distinct (the default now) | **1** | 6 messages in → 6 out, split 3/3 |

The local side also uses a **persistent** session (`clean_session=false`) so a brief
restart resumes its subscription — which means colliding replicas would be
inheriting and clobbering each other's stored queue, not merely stealing a socket.

The chart is explicit anyway: put `__POD_NAME__` in every `client_id` and its init
container substitutes the pod's name, so the id is visible in the config rather
than implied by the environment. CI fails the build if any `client_id` loses it
(`scripts/k8s/check-bridge-chart.sh`).

If the host name cannot be determined at all (no `HOSTNAME`, no `/etc/hostname`)
the bridge falls back to the old constants **and logs a warning saying so** — a
second instance on that fallback would still collide.

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

## Encrypt the spool at rest — a deployment requirement

The store-and-forward spool holds **cross-zone message payloads in the clear** while a
side is unreachable (a plain length-prefixed store, ADR 0025 §7). On a boundary host —
by design a more-exposed box than a broker inside a trusted zone — that is a plaintext
copy of everything crossing the boundary. Anyone who reads that disk (a stolen drive, a
backup, a compromised host) reads the traffic.

The bridge does **not** encrypt the spool itself, and deliberately so: that is not how the
field handles data-at-rest. Mosquitto (`mosquitto.db`), EMQX and HiveMQ (RocksDB/LMDB)
all persist their message stores in the clear and rely on the **volume** being encrypted.
Encrypting inside the app would also be *less* complete — it leaves logs, swap, temp files
and core dumps (which can carry the same payloads) exposed. So the requirement is:

> **Put the spool directory on a volume that is encrypted at rest, on every boundary host.**

- **Kubernetes:** set the bridge `persistence.storageClassName` to a StorageClass whose
  provisioner encrypts the volume — e.g. an AWS EBS class with `encrypted: "true"`, a GCP
  CMEK-backed class, or an Azure disk with encryption-at-host. Do not leave it unset unless
  the cluster's default class is itself encrypted. The chart's
  [`values.yaml`](../deploy/helm/mqttd/values.yaml) documents this at the `persistence` block.
- **Docker / bare metal:** put Docker's storage (or the spool bind-mount) on an encrypted
  filesystem — full-disk encryption (LUKS/dm-crypt), an encrypted cloud volume, or a
  bind mount onto an encrypted path. The demo
  [`docker-compose.yml`](../demo/docker-compose.yml) documents this on the `bridge-spool`
  volume.

If you need the payload to be opaque to the bridge **itself** (not just to the disk under
it), that is **end-to-end payload encryption** — the publishers encrypt the payload before
it ever reaches the bridge, so the spool holds ciphertext for free. That is an application
choice, orthogonal to this requirement, and the standard way to keep a boundary crossing
from ever seeing plaintext.

---

## Choosing

| | standalone | HA (2+) |
|---|---|---|
| Duplicate forwarding | n/a | prevented by the share group |
| Survives a bridge restart without a gap | ✖ | ✔ |
| Survives losing a bridge pod | ✖ | ✔ (live stream) |
| Buffered backlog survives losing that pod | ✔ (same PVC on restart) | ✔ only when the ordinal returns |
| Distinct `client_id` | automatic (host-derived) | automatic; the chart sets it explicitly too |
| Upstream credentials | one set per upstream | the same set, used by every replica |

Start standalone. Move to HA when a forwarding gap during a restart is a problem
— and set `share_group` and per-replica `client_id` together, since either one
without the other is broken in a different way.
