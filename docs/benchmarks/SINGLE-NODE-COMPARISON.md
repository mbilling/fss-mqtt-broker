# Single-node knee — mqttd · Mosquitto · EMQX · HiveMQ CE

**Dated 2026-09-17. Run 2026-09-16, one Hetzner CCX23 (4 dedicated vCPU,
16 GB), all four brokers on the same host, one after another.** First
cross-broker throughput record published in-tree under
[ADR 0048](../adr/0048-comparative-benchmarking.md) §3/§4 (T4). The prose
comparison — features, licences, operations — is
[`docs/COMPARISON.md`](../COMPARISON.md); this file is only the number.

The question it answers: **at what offered rate does a single node stop
meeting a p99 ≤ 1 s service level**, for each broker, on hardware nobody's
laptop can flatter.

## The result

| broker | version | **knee** | p99 at knee | broker CPU idle at knee | container RSS at knee | first failing rung |
|---|---|---|---|---|---|---|
| **mqttd** | 1.0.17 | **75 000 msg/s** | ≤500 ms | 30% mean / 21% min | 133 MiB | 90 000 — p99 ≤7.5 s |
| **Mosquitto** | 2.0.20 | **45 000 msg/s** | ≤100 ms | 70% / 57% | **18 MiB** | 60 000 — p99 ≤10 s |
| **EMQX** | 5.8.6 | **45 000 msg/s** | ≤500 ms | 2% / 0% | 434 MiB | 60 000 — p99 ≤25 s |
| **HiveMQ CE** | 2024.3 | **30 000 msg/s** | ≤100 ms | 5% / 3% | 927 MiB | 45 000 — p99 ≤5 s |

**The knee is the highest rung that passed every gate**, and the ladder
steps in 15 000 msg/s, so each figure is bracketed by its neighbour: mqttd's
true knee is between 75 000 and 90 000, Mosquitto's and EMQX's between
45 000 and 60 000, HiveMQ's between 30 000 and 45 000. A finer ladder would
move every row, not just ours — an earlier 30 000-step run of this same lane
put mqttd at 60 000 and Mosquitto at 30 000.

**Where mqttd loses, stated as plainly as where it wins:**

- **Mosquitto is 7× leaner at its knee** — 18 MiB against mqttd's 133 MiB,
  and 4 MiB at 15 000 msg/s. If memory is the budget, it wins outright.
- **Mosquitto and HiveMQ both hold a tighter tail at their own knees**
  (≤100 ms) than mqttd does at its (≤500 ms). mqttd's knee is higher; it is
  not quieter.
- **EMQX is the quietest at low rates** — ≤1 ms p99 at 15 000 msg/s, where
  mqttd is also ≤1 ms and Mosquitto ≤5 ms.
- The only claim the table supports for mqttd is **the highest rate on this
  host at this service level**, at 1.7× Mosquitto and EMQX and 2.5× HiveMQ CE.

## What actually limited each broker

The CPU column is the interesting one, because all four failed the same
gate for different reasons.

- **Mosquitto is core-bound, not CPU-bound.** Its *mean* idle never fell
  below 61% at any rung up to 150 000 msg/s, and its busiest single second
  was 45% idle — on 4 vCPU that is between one and one and a half cores. It
  is single-threaded by design, so most of the host is unreachable to it.
  Within that limit it is excellent, and its knee would be expected to move
  with clock speed rather than with core count.
- **EMQX is CPU-bound.** Idle hits 2% at its knee and 1% above it: it uses
  the whole host and still reaches the same 45 000 msg/s that Mosquitto
  reaches on one core. Nothing is architecturally out of reach; each message
  simply costs more.
- **HiveMQ CE is memory-bound, and it does not degrade — it dies.** At
  60 000 msg/s (one rung past its knee) it failed to deliver **52.6%** of
  what was published, and the JVM then terminated with
  `OutOfMemory: Java heap space`; every later rung measured a dead container
  (0 connections, 100% host idle). This was the JVM's own heap limit, not the
  kernel — `dmesg` recorded no OOM kill. See the heap caveat below.
- **mqttd is the only one not CPU-limited at its knee** (30% idle). What
  fails first is the tail. Past the knee it queues rather than sheds — RSS
  133 MiB → 2.3 GiB at 150 000 msg/s — and the delivered ledger stays whole.

## The control: is the sequence readable at all?

Four brokers ran in sequence on one host with a reboot between arms, so the
obvious objection is that the host drifted and arm 4 was measured on a
different machine than arm 1. **mqttd was therefore run twice** — first and
last, as arms 1 and 5, with three other brokers and four reboots in between.

> **PASS** — arm 5 reproduces arm 1: both knee at **75 000 msg/s**, delivered
> 74 951/s against 74 994/s (**0.1% drift**).

Had the control's knee moved, or its delivery drifted more than 5%, the
harness would have declared the whole sequence void and these numbers would
not be published. That check is not advisory: it is
`summarize-compare.py`'s verdict, and it is the reason a single run is
worth reading.

## The fairness choices, and how to attack them

Every one of these is a decision that could be argued differently. They are
listed so they can be.

1. **One host, one after another — not four hosts in parallel.** Cloud
   provisioning varies by tens of percent between identical instances
   ([ADR 0077](../adr/0077-workload-targeted-performance.md) T4), which is
   larger than the differences being measured. Sequential arms on one host
   remove that variance; the control arm proves they did.
2. **The load generator is EMQX's own tool.** `emqtt-bench` 0.6.3 drives
   every arm. Using our own driver against competitors would be the first
   thing to distrust, and rightly.
3. **Measurement is driver-side only.** No broker's internal counters are
   used. The four export different things under different names, so a table
   built from them would compare their instrumentation, not their delivery.
4. **mqttd runs with its durable-by-default switched OFF**
   (`MQTTD_DURABLE_SESSIONS=0`), because the other three keep sessions in
   memory and that is the like-for-like posture. ADR 0048 §4 forbids buying
   "fast" by quietly disabling a guarantee; this is that disclosure. mqttd's
   durable path is measured separately in
   [`docs/benchmarks/DURABLE-PATH.md`](DURABLE-PATH.md), where it is roughly
   900× slower per message.
5. **Drivers are deliberately oversized** — three 16-vCPU CCX43 hosts
   driving one 4-vCPU broker — so a driver limit cannot be mistaken for a
   broker limit. Rungs where the drivers themselves saturated are flagged in
   the raw report and excluded from knee detection.
6. **The same image digest for every run**, pinned in
   [`bench/scale/compare-brokers.sh`](../../bench/scale/compare-brokers.sh),
   with the digest and the config's SHA-256 recorded per arm.
7. **Latency is a histogram bucket upper bound**, differenced against a
   baseline scraped after the rung settled. It reads "p99 ≤ X ms" because it
   is coarse — but it cannot flatter, and it is the same instrument for all.
8. **A rung must drain before its ledger is believed.** After the publishers
   stop, the rung waits for arrivals to fall below 0.1% of its offered rate.
   A rung still moving when the budget expires is failed, not reported — an
   undrained rung cannot tell late traffic from lost traffic.

**The strongest argument against these numbers** is choice 9, which we did
not make: **none of the four brokers is tuned.** Each runs a documented
reasonable minimum, printed in full below. A vendor tuning their own broker
on this hardware would very likely beat its figure here, and that applies to
mqttd exactly as much as to the other three.

## The exact configuration of each broker

Committed under [`bench/scale/compare/`](../../bench/scale/compare/), applied
verbatim, hash recorded per arm.

**mqttd** — [`bench/scale/compare/mqttd.env`](../../bench/scale/compare/mqttd.env),
sha256 `91b0263c4663…`, image
`ghcr.io/mbilling/fss-mqtt-broker@sha256:db226efa1c18…` (the published 1.0.17
image, not a special build):

```sh
MQTTD_PLAINTEXT_BIND=0.0.0.0:1883
MQTTD_ALLOW_ANONYMOUS=1
MQTTD_DURABLE_SESSIONS=0
MQTTD_HEALTH_BIND=0.0.0.0:8080
```

**Mosquitto** — [`bench/scale/compare/mosquitto.conf`](../../bench/scale/compare/mosquitto.conf),
sha256 `76459b982717…`, image `eclipse-mosquitto@sha256:21421af7b32b…`:

```conf
allow_anonymous true
persistence false
log_type error
log_type warning
listener 1883
max_connections -1
```

`max_connections -1` lifts the stock 1024, which would refuse this lane's
client population outright. `log_type` is narrowed because Mosquitto logs a
line per connect and per disconnect by default, and 12 000 connecting clients
would otherwise measure its log path as much as its delivery path.

**EMQX** — [`bench/scale/compare/emqx.env`](../../bench/scale/compare/emqx.env),
sha256 `942106c40c2e…`, image `emqx/emqx@sha256:a1e3d10fa1dc…`:

```sh
EMQX_NODE__NAME=emqx@127.0.0.1
EMQX_NODE__COOKIE=compare
EMQX_LISTENERS__TCP__DEFAULT__BIND=0.0.0.0:1883
EMQX_LISTENERS__SSL__DEFAULT__ENABLE=false
EMQX_LISTENERS__WS__DEFAULT__ENABLE=false
EMQX_LISTENERS__WSS__DEFAULT__ENABLE=false
EMQX_AUTHENTICATION=[]
EMQX_ALLOW_ANONYMOUS=true
EMQX_LOG__CONSOLE__LEVEL=warning
EMQX_LOG__CONSOLE__ENABLE=true
EMQX_LOG__FILE__ENABLE=false
EMQX_DASHBOARD__LISTENERS__HTTP__BIND=0
```

Listeners nobody connects to are disabled so the process is not charged for
them; everything else is EMQX's own default.

**HiveMQ CE** — [`bench/scale/compare/hivemq.env`](../../bench/scale/compare/hivemq.env),
sha256 `5126c65e5d6c…`, image `hivemq/hivemq-ce@sha256:5f440cd2e286…`:

```sh
HIVEMQ_ALLOW_ALL_CLIENTS=true
```

plus `HIVEMQ_HEAPSIZE`, set per arm to **half the host's RAM** (7 805 MB on
this host) by the harness. **This is the caveat most likely to change
HiveMQ's row.** The JVM's default heap is a fraction of host memory, so
leaving it implicit would hand HiveMQ a different share of a 4-vCPU host than
of a 32-vCPU one and make the vertical-scaling arm measure the JVM default
rather than the broker. Half the host is a defensible choice, not the only
one: a larger heap would plausibly postpone the OOM, and anyone re-running
this lane with a different `HIVEMQ_HEAPSIZE` should say so beside the number.

## Method

- **Workload**: 1:1 QoS 0 — one publisher and one subscriber per topic,
  `bench/<i>`, 200-byte payload, 25 msg/s per publisher, 600 publishers per
  container. A rung's client population scales with its rate: 600 publishers
  and 600 subscribers at 15 000 msg/s, 6 000 and 6 000 at 150 000.
- **Ladder**: 15 000 · 30 000 · 45 000 · 60 000 · 75 000 · 90 000 · 120 000 ·
  150 000 msg/s, every arm, in the same order.
- **Per rung**: subscribers connect first; the window opens only once every
  client has connected; 60-second measurement window; publishers are then
  removed and the rung drains before its ledger is read.
- **A rung passes** only if it delivered ≥99% of what was published, met
  ≥95% of its offered rate, kept p99 ≤ 1 s, settled before the window, and
  drained after it. Any one failure fails the rung.
- **Arms**: mqttd, Mosquitto, EMQX, HiveMQ CE, then mqttd again as the
  control, with a host reboot between arms.
- **Harness**: `bench/scale/compare-brokers.sh`, rendered by
  [`bench/scale/summarize-compare.py`](../../bench/scale/summarize-compare.py),
  run as `bench/scale/run.sh compare`.

## Limits of this record

- **One run.** No repeats, so these are single measurements with a control,
  not a distribution. The control's 0.1% drift bounds host drift within the
  run; it says nothing about run-to-run variance.
- **One instance type.** CCX23 only. How each broker uses a larger host is a
  separate question this lane was built to answer and has not yet run.
- **Versions are pinned to what was tested**, and newer lines exist:
  Mosquitto 2.0.22/2.1.2 and EMQX 6.x are current at the time of writing.
  EMQX 5.8.6 is the last Apache-2.0 line, which is why it is the one measured.
- **QoS 0, plaintext, anonymous, no persistence, no cluster.** A broker fast
  here may be slow where a guarantee is real. TLS and authentication cost is
  a different posture (ADR 0048 §3) and is not free for anyone.
- **HiveMQ's rungs above its knee measured a dead process**, not a degraded
  one. Its 45 000 row is a genuine p99 failure; its 60 000 row is a genuine
  52.6% delivery failure; the rows above that are only evidence that it did
  not restart.
- Raw per-rung tables, CPU streams and container logs live in the run
  directory produced by the harness. That output is untracked scratch by
  design (see `bench/README.md`); the tracked artifacts a reader can check
  are the harness, the configs above, and this record.

## Reproducing it

```sh
export HCLOUD_TOKEN=…
export COMPARE_BROKERS="mqttd mosquitto emqx hivemq"
export COMPARE_RATES="15000 30000 45000 60000 75000 90000 120000 150000"
export BROKER_TYPE=ccx23 DRIVER_TYPE=ccx43 DRIVER_COUNT=3
bench/scale/run.sh compare
python3 bench/scale/summarize-compare.py .runs/<stamp>/results
```

The run costs roughly €5 of cloud time and about 2.5 hours. Corrections are
welcome: if a configuration here misrepresents a broker, open an issue and
the lane will be re-run with the correction and a dated changelog line.
