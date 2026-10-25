# rig2: the msg/s ramp, judged on the broker

The benchmark rig of [ADR 0090](../../../docs/adr/0090-benchmark-rig-redesign.md): tasks
0090-T3 (the ramp and the verdict) and 0090-T5 (the 3- and 10-node tiers). It offers load
in msg/s with [`mqtt-load`](../../../crates/mqtt-load/README.md), reads throughput from
the brokers' own counters, reads ack and end-to-end latency on the generator's one clock,
and judges every step on the broker only.

It reuses the lane E rig around it: provisioning (`../terraform`), host bring-up
(`await_fleet` in `../lib.sh`), the cluster bootstrap (`../bootstrap-cluster.sh durable`:
PKI from `deploy/systemd/gen-certs.sh`, founder first, the private network), the
Prometheus parsing (`../extract-lane-e.py`), failure evidence (`../collect.sh`) and
teardown with its leak audit (`../teardown.sh`). There is no clock setup: no latency
figure crosses hosts.

## Tiers

| `TIER` | shape | price | use |
|---|---|---|---|
| `local` | 3 local mqttd processes (durable, own data dirs) + 1 local mqtt-load | free | proves the pipeline; relative figures only |
| `n3` | 3 brokers CCX23 + 1 generator host CCX33 | EUR 0.794/h | nightly or weekly; absolute figures |
| `n10` | 10 brokers CCX23 + 3 generator hosts CCX33 | EUR 2.555/h | release confirmation |

`BROKERS`, `GENERATORS`, `BROKER_TYPE` and `GEN_TYPE` override the shape. Prices are in
`verdict.py` (`PRICES_EUR_H`, Hetzner, excluding VAT).

## Running

```sh
# Free: a 3-node local cluster and a short ramp (3 steps of 5 s warm-up + 10 s measured).
TIER=local bench/scale/rig2/run.sh
TIER=local WORKLOADS="durable-qos1 clean-qos1 qos0" MAX_STEPS=6 bench/scale/rig2/run.sh

# The quote, with no cloud call:
PRICE_ONLY=1 TIER=n3 WORKLOADS="durable-qos1 clean-qos1 qos0" bench/scale/rig2/run.sh

# PAID. A release:
export HCLOUD_TOKEN=...            # the dedicated bench project
MQTTD_VERSION=1.3.0 TIER=n3 WORKLOADS="durable-qos1 clean-qos1 qos0" bench/scale/rig2/run.sh

# PAID. A candidate (stamped UNRELEASED in the run dir):
MQTTD_URL=https://.../mqttd-x86_64-unknown-linux-musl MQTTD_SHA256=<sha256> \
  TIER=n10 WORKLOADS=durable-qos1 bench/scale/rig2/run.sh
```

Before provisioning, the runner prints the tier's hourly price and the expected
duration, then asks for confirmation (or needs `RIG2_YES=1` when stdin is not a
terminal). One provisioning carries every workload in `WORKLOADS`, one ladder after the
other on the same cluster. `SSH_KEY=~/.ssh/<key>` selects a non-default key, as for the
old rig. `KEEP_INFRA=1` skips the teardown and says so; destroy later with
`bench/scale/teardown.sh`. The runner refuses to apply while `../terraform`'s state still
tracks servers (another run, or a leak).

Expected duration and price (estimate: about 9 steps per ladder at about 125 s each,
10 to 14 minutes of bring-up, 3 of teardown; the real step count depends on where the
knee is):

| tier | one workload | three workloads |
|---|---|---|
| n3 | ~32 min, EUR 0.42 prorated (EUR 0.79 per started hour) | ~69 min, EUR 0.92 (EUR 1.59) |
| n10 | ~36 min, EUR 1.52 prorated (EUR 2.55 per started hour) | ~73 min, EUR 3.12 (EUR 5.11) |

### The generator binary

The generator hosts get a static x86_64 Linux build of mqtt-load, shipped over SSH (no
Docker and no emqtt-bench on them: `driver_emqtt_bench=false`).
`build-mqtt-load.sh` builds it in Docker as `linux/amd64` with
`scripts/release/build-repro.sh`'s flags (musl-gcc, a static CRT, stripped,
`--locked`):

```sh
bench/scale/rig2/build-mqtt-load.sh        # prints target/rig2-amd64/mqtt-load
```

The runner calls it when `MQTT_LOAD_BIN` is not set. A warm build takes about a minute
on Apple silicon (emulated).

## Workloads

Each is a small file in `workloads/`, expressed in msg/s only. Values in the file are
defaults; the environment wins.

| workload | QoS | consumers | sessions | payload | latency gate |
|---|---|---|---|---|---|
| `durable-qos1` | 1 | `$share`, one member per broker per topic | persistent, session expiry 30 s | 200 B | ack p99 |
| `clean-qos1` | 1 | `$share`, one member per broker per topic | clean | 200 B | ack p99 |
| `qos0` | 0 | `$share`, one member per broker per topic | clean | 200 B | end-to-end p99 |

Each generator process publishes under its own topic prefix
(`rig2/<step>/g<generator>/<t>`), so a topic's publishers and consumers always share one
process and one clock. Publishers send 50 msg/s each (QoS 0: 100) with 128 messages in
flight, which holds the offer through 2.56 s of ack latency, past the 1 s band.

## A step

1. **Idle check.** Every broker is scraped once. The step starts when the cluster holds no
   connections, no sessions, no pending publish, and no placement group under-replicated
   (`mqttd_replication_min_actual < mqttd_replication_desired`), within `RESET_BUDGET`
   (120 s). Otherwise the step is VOID (unreset). mqtt-load ends its durable sessions with
   session expiry 0, so a clean step leaves nothing behind.
2. **Broker scrapes.** `scrape-loop.py` starts on every broker host and scrapes
   `/metrics` every `SCRAPE_INTERVAL` (2 s) on the broker's own clock for the whole step.
3. **Load.** One mqtt-load process per generator host, started together, each offering
   `rate / GENERATORS` msg/s: `WARMUP` (20 s) of warm-up, then `MEASURE` (60 s) measured in
   `WINDOW` (10 s) windows, then the drain (at most `DRAIN_TIMEOUT`, 30 s). Each step has its
   own process, so each has its own ledger.
4. **Judgment** (`verdict.py judge`). The brokers' window is the overlap of every
   generator's measured span (on the wall clock, from the summary's
   `schedule_start_unix_ms`). For each broker, the first and last scrape inside it (less
   `MARGIN_S` at each edge) give its received and delivered msg/s. Drops are read over the
   whole step.

Raw evidence per step: `<run>/<arm>/<workload>/steps/<NN>-<rate>/` holds `step.json`,
`gen-<g>.json` (the mqtt-load summary, schema `mqtt-load.summary.v2`), `.stderr`,
`.exit`, `pre/` (the idle scrapes), `scrapes/broker-<i>/<unix_ms>.prom`, and
`verdict.json`.

## The verdict

Judged on the broker only (ADR 0090 D1). In order:

| verdict | when | ramp |
|---|---|---|
| **FAILED** | a broker dropped a message (`mqttd_publish_dropped_total`, any reason), a broker restarted, or a generator's ledger did not close (missing, unexpected, unacked) | stops the ladder (`BISECT_ON_FAIL=1` keeps bisecting below it) |
| **VOID** | a generator saturated in a measured window, a generator left no summary, the generators' windows overlap less than 90%, the scrapes do not cover the window, or the cluster was not idle | rerun (`VOID_RETRIES`, 1); never counted against the broker |
| **DISCONNECTED** | connections were lost during the step. mqtt-load does not reconnect, so its ledger cannot be read after that | not carried; the ramp treats it as above the knee |
| **OFFER NOT TAKEN** | the brokers received less than 99% of the offered msg/s (`OFFER_TAKEN` from `../summarize-curve.py`) | above the knee |
| **CARRIED** | none of the above | at or below the knee if the gated p99 is at most 1 s |

A broker drop fails a step even when the generator's ledger closes. A VOID ledger loss
(the generator saturated) is not counted.

Latency is the exact merge of every generator's HDR histogram: mqtt-load exports its
buckets, and `verdict.py` adds the counts of equal buckets and reads the quantile as
hdrhistogram does. It is never an average of percentiles.

**Capacity** is the highest CARRIED step with **ack p99 ≤ 1 s** (the headline), and the
same at ≤ 250 ms and ≤ 100 ms. For `qos0` the bands apply to the end-to-end p99.

## The ramp

The first step is the workload's `START_RATE_<tier>`. Coarse steps multiply by `FACTOR`
(1.5) until the first step that is not carried within 1 s. Then the ramp bisects between
the last good and the first bad step until their gap is at most `GAP_PCT` (3%) of the good
one. Rates are rounded to `GRANULARITY` (100 msg/s). A step that is not carried at the
first rate steps down by the factor instead. `MAX_STEPS` (14; local 3) and `MAX_RATE`
bound a ladder.

## Output

`<run>/report.md` and `<run>/report.json` (`mqtt-rig2.report.v1`). Per step: msg/s
offered, brokers received, delivered, the received share, loss, ack p50/p99/p999,
end-to-end p50/p99/p999, generator health (worst CPU fraction and scheduling-lag p99,
saturation) and the verdict with its reasons. Per ladder: the three capacity lines and
why the ladder ended. `verdict.py report <run>` re-renders them from the raw evidence.

## Not yet

- **Same-hosts A/B** (ADR 0090 D5) is 0090-T4. The run layout already has an arm level
  (`<run>/<arm>/<workload>`, `ARM=A`), and `../swap-binary.sh` plus
  `../bootstrap-cluster.sh` swap a binary on the same hosts. `MQTTD_URL_B` is refused
  until T4 adds the interleaving.
- **A pre-built server image** (ADR 0090 D6). Bring-up is the old rig's cloud-init without
  the durable_bench build or Docker on the generator hosts.
- **Calibration against emqtt-bench** is 0090-T2.

Tests: `python3 bench/scale/rig2/test-verdict.py` (offline; CI runs it).
