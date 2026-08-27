# Workload patterns — five industries, five different scale curves

"How many messages per second" is not a property of a broker. The same cluster,
on the same hardware, measured on 2026-08-26 (mqttd 1.0.9, 10 subscribers, 1M
offered): **188k TPS** on one shape and **20k** on another. So a number is only
meaningful with the workload named — and these are the five named shapes.

Each is a **configuration of a lane the rig already has**, not a new mechanism,
so a run is reproducible from its `.env` file and comparable across releases.

| Workload | Shape | Lane | Stresses |
|---|---|---|---|
| `market-data` | broadcast fan-out, QoS 0, few pubs → many subs | B | egress path, delivery p99 |
| `telematics` | massive fan-in, 100k connections, QoS 1 @ 1 msg/s | B (`$share`) | routing/hub, session memory |
| `industrial` | durable QoS 1/2, low volume, latency-critical | A | fsync barrier, durable p99 |
| `smart-home` | enormous idle connection count, reconnect storms | C | memory/conn, establishment |
| `logistics` | store-and-forward, disconnect/resume | D | offline queues, session resume |

A second set, added 2026-08-27, chosen for the dimensions the first five never
touch — payload size, topic cardinality, delivery amplification, connection
ceiling, and establishment rate:

| Workload | Shape | Lane | Stresses |
|---|---|---|---|
| `video-surveillance` | 64 KB frames, 60 cameras → 120 viewers | B | **bytes**/s not msgs/s: write path, backlog byte bound |
| `energy-metering` | 100k meters, one reading each per 10 s | B (`$share`) | topic **cardinality** at rest, not throughput |
| `emergency-alerting` | 30 pubs → 3 000 subs, full fan-out | B | delivery **amplification** (300×) |
| `wearables` | 150 000 idle connections | C | the connection **ceiling** past smart-home's 50k |
| `gaming-presence` | 50k connections at 10 000/s | C | **establishment rate**, not capacity |

`video-surveillance` is a **1/3/5** curve: 60 publishers cannot spread over 7
brokers, and the smallest population that could (420) is a different workload at
64 KB. The other four run 1/3/5/7.

Still uncovered by any workload, and honest to say so: retained-message load,
QoS 2 at scale (lane B refuses it; only lane A's arm exercises it), MQTT 5
user properties on the hot path, and request/response correlation.

## Running

```sh
export MQTTD_VERSION=1.0.9              # required: the binary under test is a disclosure item
cd bench/scale/workloads
./run-workload.sh market-data           # the default curve: sizes 1, 3, 5
./run-workload.sh market-data 5         # one size
```

The **default curve axis is node count** (1/3/5 in one `run.sh full`
invocation, ~€1.5–2 with `DRIVER_COUNT=2`). Each workload also has **its own
axis**, swept by overriding a knob per run — the caller's environment always
wins over the file:

```sh
LANE_B_SUBS_OVERRIDE=480 ./run-workload.sh market-data 5   # fan-out width
LANE_B_PUBS_OVERRIDE=200000 LANE_B_RUNGS_OVERRIDE=200000 \
  ./run-workload.sh telematics 5                           # connection count
LANE_C_CONNS=200000 LANE_C_RAMP=5000 ./run-workload.sh smart-home 5
```

**A note on the mTLS reference rung.** `run.sh full` runs one extra rung per size
in the mTLS posture (ADR 0048 §3: disclose both postures without paying for the
ladder twice). It is a rung like any other, so it must divide `LANE_B_PUBS*1000`
into an integer `-I` — and it must *suit the shape*: the rig default (50 000) is a
sane publish rate for fan-in but 50 000 x subscribers under fan-out. Workloads set
`LANE_B_REF_RUNG` accordingly. **Validate a new workload under the profile you will
run it with** (`full`, not `STANDARD=1`) or the ref rung is not checked.

## Reading the result

**`logistics` is the exception to everything in this section** — it does not
report a rate. Lane D measures a *cycle*: 1920 persistent sessions attach,
detach, 160 000 messages are published to them while they are offline, and the
same sessions resume. Its `laneD/summary.txt` answers three questions —

| number | meaning |
|---|---|
| accepted while offline | what the cluster took in with nobody listening (broker counter, not the driver's) |
| drained after resume | what the resumed sessions actually received, as a % of accepted |
| drain time | how long the backlog took to clear, and at what rate |

A shortfall is a **defect only if `dropped` does not explain it**: an over-cap
queue is a disclosed bound (ADR 0001 §6), a silent loss is not. The lane refuses
QoS 0 outright — an offline session queues nothing at QoS 0, so the measurement
would be a guaranteed zero.

For the other four workloads, use **TPS = received + sent**, from the brokers' own counters
(`mqttd_publish_received_total` + `mqttd_publish_delivered_total`) — it counts
total broker work in both directions and does not depend on driver-side rates,
which oscillate near saturation.

Two traps this rig has already fallen into, both recorded so they are not
repeated:

- **A QoS 1 number can measure the harness, not the broker.** emqtt-bench's QoS 1
  publish is synchronous — one PUBACK round trip per client at a time — so a high
  per-client rate stalls the load generator while the brokers idle (measured: 44%
  broker CPU with 97% of publishes late). `telematics` therefore fixes the
  per-client rate at 1 msg/s. **If broker CPU is low and the offered rate is not
  met, suspect the load model before the broker.**
- **Broadcast fan-out cannot scale linearly with nodes.** Total delivery work is
  `publishes × subscribers`; adding nodes spreads the same deliveries and *adds*
  cross-node forwarding. Measured on `market-data`'s shape: cluster TPS 188k →
  254k → 268k for 1 → 3 → 5 nodes while per-node TPS fell 188k → 85k → 54k. Near
  linear scaling belongs to workloads with **one logical consumer per message**
  (`$share` groups, durable-session ownership), not to broadcast.

## Optimization pass — the levers per workload

Find the curve first, then tune against it; otherwise you are tuning against an
unknown baseline. Every lever is an existing `MQTTD_*` setting, passed in the
environment (it wins over the workload file).

| Workload | Levers |
|---|---|
| `market-data` | `MQTTD_MAX_QUEUED_MESSAGES`, `MQTTD_QUEUE_OVERFLOW` (drop-oldest is *correct* for stale ticks), `MQTTD_TOPIC_ALIAS_MAX` |
| `telematics` | `MQTTD_RECEIVE_MAXIMUM`, `MQTTD_MAX_CONNECTIONS`, `MQTTD_DURABLE_SESSIONS`, `MQTTD_MAX_INFLIGHT_MESSAGES` |
| `industrial` | `MQTTD_BENCH_TIER` (quorum\|local\|relaxed, ADR 0072), `MQTTD_STORE_SHARDS` and `MQTTD_STORE_LINGER` (ADR 0076 — built, measured, rejected as *defaults*; this is the workload where they might pay), `MQTTD_LEASE_VOTERS` |
| `smart-home` | `MQTTD_MAX_CONNECTIONS(_PER_IP)`, `MQTTD_MEMORY_MAX_BYTES`, `MQTTD_TLS_SESSION_CACHE` |
| `logistics` | `MQTTD_MAX_QUEUED_MESSAGES`, `MQTTD_QUEUE_OVERFLOW`, message expiry, `MQTTD_LEASE_VOTERS` |

Raw results are untracked scratch under `.runs/`; the published record is
`docs/benchmarks/SCALE-CURVE.md`.
