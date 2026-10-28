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
| `n10` | 10 brokers CCX23 + 4 generator hosts CCX33 | EUR 2.832/h | release confirmation |

`BROKERS`, `GENERATORS`, `BROKER_TYPE` and `GEN_TYPE` override the shape. Prices are in
`verdict.py` (`PRICES_EUR_H`, Hetzner, excluding VAT).

**Generator hosts are checked before they measure anything.**

- **RPS at bring-up.** Every generator host gets RPS/RFS over all its CPUs, as the brokers
  do. With one NIC queue and no RPS, all received packets are processed on one core. That
  core runs out of budget (softnet squeezes) long before the generator's own CPU looks busy.
- **Calibration.** With 2+ generators in the cloud, every generator runs the same QoS 0
  share load before the first ladder: `CALIB_RATE_PER_GEN`, 30,000 msg/s, for
  `CALIB_MEASURE`, 30 s. `verdict.py calibrate` names a host that saturated, or whose
  end-to-end p99 is above 5 × the other generators' median + 50 ms.
- **Replacement.** That host is replaced (`tofu apply -replace`, same name and private
  address) and calibration runs again, up to `CALIB_REPLACE_MAX` (2) times. Then the run
  stops. `CALIBRATE=0` skips calibration.

During the ladders, a generator that voids 2 attempts of one step gets a new host before
the next attempt, and those VOIDs don't use up the round's reruns (`verdict.py
replace-due`, at most `GEN_REPLACE_MAX`, 2, a run). The live Grafana tunnel is
supervised: it is reopened when it stops answering (`observe-tunnel.log` in the run
directory).

The brokers serve every generator alike, so a generator far slower than the rest is
measuring its own host. On 2026-10-27 one host's deliveries arrived after 291 ms, while
the other generators' arrived after 0.8 and 1.6 ms.

## Running

```sh
# Free: a 3-node local cluster and a short ramp (3 steps of 5 s warm-up + 10 s measured,
# one round a step; ROUNDS=2 measures each step twice).
TIER=local bench/scale/rig2/run.sh
TIER=local ROUNDS=2 bench/scale/rig2/run.sh
TIER=local WORKLOADS="durable-qos1 clean-qos1 qos0" MAX_STEPS=6 bench/scale/rig2/run.sh

# The quote, with no cloud call:
PRICE_ONLY=1 TIER=n3 WORKLOADS="durable-qos1 clean-qos1 qos0" bench/scale/rig2/run.sh

# PAID. A release:
export HCLOUD_TOKEN=...            # the dedicated bench project
MQTTD_VERSION=1.2.0 TIER=n3 WORKLOADS="durable-qos1 clean-qos1 qos0" bench/scale/rig2/run.sh

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

Ctrl-C or a TERM to `run.sh` tears down at once, whatever the run is doing: every long
child (tofu, the bootstrap, each ladder) runs as a background job under `wait`, which a
signal interrupts, while bash holds a trap back until a foreground child returns (on the
first 10-node run a TERM waited behind the ladder until the fleet was torn down by hand).
The trap stops the run's children (TERM, `CHILD_GRACE` s, default 60, then KILL; a
TERMed tofu writes its state first), then destroys; a second Ctrl-C does not interrupt
the teardown. `kill -9` still leaves the fleet up.

Expected duration and price, as the runner quotes it (`verdict.py cost`): about 9 steps
per ladder (`EST_STEPS`), each `ROUNDS` (3) rounds of about 135 s (20 s warm-up, 60 s
measured, ~20 s of connect, start, drain and scrape fetch, the 10-node runs of
2026-10-27 took 97 to 111 s a round, plus the short settle), and per ladder ~350 s for
the 90 s prime and four long settles (the first, and `EST_LONG_SETTLES`, 3, after bad
steps or before transient retries); then 10 to 14 minutes of bring-up and 3 of
teardown. The real step count depends on where the knee is; a step that is not carried
stops at its first bad round, so its other rounds are not paid for.

| tier | one workload | three workloads |
|---|---|---|
| n3 | ~80 min, EUR 1.05 prorated (EUR 1.59 per started hour) | ~213 min, EUR 2.82 (EUR 3.18) |
| n10 | ~84 min, EUR 3.56 prorated (EUR 5.11 per started hour) | ~217 min, EUR 9.23 (EUR 10.22) |

With `ROUNDS=1` (one measurement a step, as before 2026-10-27) the three-workload n10 run
is ~95 min, EUR 4.06 (EUR 5.11).

### Live Grafana (OBSERVE, on by default)

Every run attaches the old rig's live observation stack (`../observe.sh`); `OBSERVE=0`
skips it. Nothing new runs in the cloud and nothing needs Docker there:

```
operator's machine: docker compose in bench/scale/observe/
   Grafana :3000 (every interface, anonymous admin) + Prometheus :9090 + Alloy + Pushgateway
      │  ssh -N -L 8096:localhost:8096 root@<generator host 0>   (opened by observe.sh)
      ▼
generator host 0: curl bridge (systemd-run units: a curl loop + python3 -m http.server 8096)
   every 2 s, over the private network: each broker's /metrics, and each generator's
   mqtt-load --metrics (<private_ip>:9464, LOAD_METRICS_PORT)
```

Why this shape and not Alloy on a generator host: the stack has run on the operator's
machine since 2026-09 (it outlives the cluster, so a teardown loses no history), and the
bridge needs only curl and python3, which the stock image has, so `driver_emqtt_bench=false`
hosts carry it as they are. Bring-up adds one `scp` and one `ssh` after the cluster
bootstrap; there is no extra provisioning and no Docker install.

At bring-up the runner prints the URLs and the tunnel:

```
observe: live at http://localhost:3000 — dashboards: mqttd (bench) http://localhost:3000/d/mqttd-overview-bench, mqtt-load (bench) http://localhost:3000/d/mqtt-load-bench
observe: the tunnel is this run's ssh -N -L 8096:localhost:8096 root@<ip> (pid N); ...
```

Two dashboards: the broker overview (`deploy/observability/grafana/mqttd.json`, derived
with the run-phase strip) and **mqtt-load (bench)**: offered, sent, acked and received
msg/s, the brokers' received rate beside the generators' sent rate, ack and end-to-end
p50/p99/p99.9, generator CPU, saturation, scheduling lag, in-flight and backlog, error
and ledger-anomaly rates, and broker drops by reason. Its quantiles come from mqtt-load's
16 Prometheus buckets and are coarse; the report's are the exact HDR merge. Each step is
its own mqtt-load process, so the generator panels go blank between steps (the bridge
deletes a generator's file when nothing answers). The run-phase strip names the ladder,
step and msg/s. Both dashboards are written by `../observe/build-bench-dashboards.py`.

`TIER=local` uses the same stack without a bridge or tunnel: Alloy scrapes the local
brokers and mqtt-load (127.0.0.1:9464) directly (`observe.sh attach-local`). Without
Docker the attach warns and the run goes on unobserved.

Teardown is unchanged: the bridge lives and dies with generator host 0, the tunnel is a
local ssh process that the exit trap closes (`observe.sh detach`), and no cloud resource
is added, so `../teardown.sh`'s destroy and its `purpose=mqttd-bench-scale` audit cover
everything. The local stack keeps running after the run, as for the old rig;
`bench/scale/observe.sh stop` stops it.

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

## A ladder

Before its first step, a ladder **primes** the cluster: `PRIME_SECS` (90 s on n3 and n10,
0 locally) of load at the start rate in its own mqtt-load process, kept in
`<ladder>/prime/` and never judged, then the full settle. Run 2 (2026-10-26) showed the
first steps' latency excursions 70 to 80 s into the step (durable 10,000: ack p99 14 ms
against 2.2 ms on the next step, all of it in the last measured window; clean 30,000:
148 ms), so a longer warm-up inside the step would have measured them, and a separate
run keeps them out.

## A step: its rounds

A step is measured `ROUNDS` times at its msg/s (3 on n3 and n10, 1 locally; the
maintainer's request of 2026-10-27: "3 rounds per step to avoid disturbance — report the
variance and plot mean"). Each round is a separate mqtt-load run per generator host at
the same rate, with everything a step had before: its own settle (the full one before a
ladder's first round and after any round that was not carried, the short one between
the rounds of a carried step), its own warm-up, the shared start, its own ledger, its own
broker scrapes, and its own verdict (below). `verdict.py round-next` names the next
round to run and its settle; `verdict.py step` then judges the step from its rounds:

- **FAILED** if any round FAILED (loss is loss; the verdict names the round), **VOID** if
  any round stayed VOID through its reruns, then **DISCONNECTED**, **NOT DRAINED**,
  **OFFER NOT TAKEN** (the precedence of the round verdicts below); **CARRIED** only when
  every round carried.
- A **VOID round is rerun alone** (`VOID_RETRIES`, 1, per round), its other rounds
  standing; a round VOID through its reruns makes the step VOID and stops the ladder.
- **The transient retry is per step, on one round**: the first bad round of a step, when
  the ladder's transient-retry rule allows it (see the ramp), is rerun once after the full
  settle. If that retry carries, the round counts as carried and the step says "round K
  retried: transient"; if it is bad too, it counts. One retry a step, `RETRY_MAX` a
  ladder.
- A step stops at its first round that settles it as not carried: more rounds cannot
  carry it.

**Statistics across the rounds** that count (each round's last attempt; a retried
round's first attempt and VOID attempts do not count), in `verdict.json` `stats`: mean,
sample standard deviation (n − 1; none for one round), min and max of the brokers'
received and delivered msg/s, ack p50/p99/p999 and end-to-end p50/p99/p999, and the
coefficient of variation (sd / mean) of each, reported for the received msg/s and the
gated p99. **A step's headline latency is the MEAN of its rounds' p99** (and p50, p999):
`ack_latency`, `e2e_latency` and `gate_p99_us` are the means. Beside them,
`ack_latency_merged` and `gate_p99_merged_us` are the exact merge of every round's
histograms (the p99 of all the step's samples). `spread` flags rounds that disagree by
more than 10% in received msg/s ((max − min) / min) or more than 2× in p99 (max / min).

### One round

1. **The settle.** Every broker is scraped every `SETTLE_POLL` (5) s (`verdict.py idle`),
   all of them together: in the cloud one ssh to generator host 0 curls every broker's
   `/metrics` over the private network, and a broker that pass missed is asked again over
   its own ssh (`SSH_PAR`, 5, at a time). A pass that still misses a broker is unknown: it
   neither confirms nor restarts the quiet period, and the next complete pass is compared
   with the last complete one. Budgets and the quiet period are wall-clock seconds. (On the
   first 10-node run, ten scrapes in a row over ssh took 10-15 s a pass, a single failed
   one restarted the quiet period, and the cluster, idle throughout, never settled.) A
   broker that never answered within the budget is named in the VOID's reasons. The round starts
   when the cluster holds no connections (`IDLE_FLOOR`), its session count is back to the
   run's baseline (scraped on the fresh cluster before the first ladder; mqtt-load ends its
   durable sessions with expiry 0, so a clean step leaves none), and nothing is in the
   brokers' pipes: `mqttd_pending_publishes`, `_pending_publishes_awaiting_settle`,
   `_publish_held_for_unreachable`, `_append_lane_jobs`, `_inflight_messages`,
   `_backlog_bytes` and `_peer_forwards_in_flight` all 0, `mqttd_hub_queue_depth` at most
   8; no placement group under-replicated (`mqttd_replication_min_actual < _desired`), no
   routing view unsettled (`mqttd_routing_unsettled`, any term but `scan`), no session
   scan still running on the same broker two passes in a row (`scan`: every broker of a
   durable cluster runs one every ~30 s for under a second, so on ten brokers a pass sees
   one about 30% of the time; counted as unsettled, it held the 30 s quiet period off
   for whole 300 s budgets on the 10-node run of 2026-10-27, which VOIDed every step
   after the first one that was not carried), and the replication lag
   (`mqttd_replica_groups_tracked - _current`) not moving (a lag that stands still between
   two scrapes is recorded, not waited for: a local cluster held 72 groups behind at idle
   for minutes). Then it must stay so for a quiet period with the traffic and durable-writer
   counters (`mqttd_publish_received_total`, `_delivered_total`, `_forwarded_total`,
   `mqttd_durable_writer_ops_total`) standing still, or moving between two passes at no
   more than a background rate: 0.1% of the coming step's msg/s, at least 20/s
   (`verdict.py` `QUIET_FRACTION`, `QUIET_FLOOR`; an idle 10-node cluster's writer did 0
   to 110 ops in five minutes, a drain runs at hundreds to thousands a second, and work
   still in the pipes is held by the gauges above whatever the counters do):
   `SETTLE_QUIET` (10 s) within
   `RESET_BUDGET` (120 s) after a carried round (between a step's rounds, or the first
   round after a carried step); `SETTLE_QUIET_LONG` (30 s) within
   `SETTLE_BUDGET_LONG` (300 s) before a ladder's first round and after any round that was
   not carried. Otherwise the round runs but is VOID (unreset), whatever it measured, loss
   included (its findings stay listed: residue of the step before can cause them), and
   `step.json`'s `idle_before.state.why_not` and the verdict say what had not settled:
   the last pass's reasons, or, when that pass was idle, the reasons of the last pass
   that restarted the quiet period (`idle_before.last_blocked`). Every pass is in
   `pre/passes.jsonl`. Before a prime, a settle that runs out only says so: the prime is
   not judged, and the first step settles again. These are the
   lane E rig's UNRESET gauges (connections, sessions) and the residue its `run-curve.sh`
   compared against a baseline, plus the pipes. In run 2 every step started with all of
   them idle: the excursions of clean-qos1's first steps were not visible on any broker
   gauge.
2. **Broker scrapes.** `scrape-loop.py` starts on every broker host and scrapes
   `/metrics` every `SCRAPE_INTERVAL` (2 s) on the broker's own clock for the whole step.
   After the step each broker's scrapes come back as one tar stream over one ssh (one scp
   per file took ~15 s a broker on the 10-node run), unpacked aside and renamed into
   place only when the stream arrived whole.
3. **Load.** One mqtt-load process per generator host, each offering `rate / GENERATORS`
   msg/s, all on one start: each writes a ready file once its clients are connected
   (`--ready-file`) and waits for a start file (`--start-file`); when every generator is
   ready, or after `START_SYNC_WAIT` (30 s), the rig writes one start time,
   `START_LEAD_MS` (3000; local 500) ahead on generator host 0's clock, into every start
   file. On the 10-node run of 2026-10-27 one generator took 13.8 s to connect against
   the others' 5.2 s, and the windows of clean-qos1 303,800 overlapped 51.6 s of 60. A
   generator that misses the start begins on its own (`--start-wait`), the summary's
   `start.by` says so, the verdict notes it, and the overlap check decides.
   Then: `WARMUP` (20 s) of warm-up, then `MEASURE` (60 s) measured in
   `WINDOW` (10 s) windows, then the drain. Each step has its own process, so each has its
   own ledger.
4. **The drain** ends when every expected delivery and ack is in (`complete`), after a
   quiet window with nothing arriving (`quiet`), or at `DRAIN_TIMEOUT` (120 s, `timeout`).
   The quiet window is `DRAIN_QUIET` (10 s) or `DRAIN_QUIET_FACTOR` (2) times the slowest
   measured window's ack or end-to-end p99, whichever is longer (mqtt-load
   `--drain-quiet-factor`): run 2's clean-qos1 152,000 step, its deliveries 17.9 s behind,
   was cut off after 5 s of silence. The summary's `drain` says how it ended, the quiet
   window it applied, and what was still owed (`outstanding_deliveries`,
   `outstanding_acks`).
5. **Judgment** (`verdict.py judge`). The brokers' window is the overlap of every
   generator's measured span (on the wall clock, from the summary's
   `schedule_start_unix_ms`). For each broker, the first and last scrape inside it (less
   `MARGIN_S` at each edge) give its received and delivered msg/s. Drops are read over the
   whole step. The cluster's rates exist only when EVERY broker of the run (`step.json`
   `brokers`) has a usable window; a broker without scrapes VOIDs the step by name. On
   the aborted 10-node run (2026-10-27) brokers 5-9's scrapes could not be fetched during
   an outage on the operator's side, and the sum over the other five read as OFFER NOT
   TAKEN at half the offer.

Raw evidence per step: `<run>/<arm>/<workload>/steps/<NN>-<rate>/` holds the step's
`step.json` (rate, rounds, why the ramp chose it) and `verdict.json` (the step's verdict,
its rounds and their statistics), and one directory per round attempt, `round-<K>/`, a
rerun `round-<K>-a2/` beside it. A round directory holds what a step held before:
`step.json`, `gen-<g>.json` (the mqtt-load summary, schema `mqtt-load.summary.v2`),
`.stderr`, `.exit`, `pre/` (the last settle scrapes, their state, and every pass in
`passes.jsonl`), `scrapes/broker-<i>/<unix_ms>.prom`, and the round's `verdict.json`.

## The verdict

Judged on the broker only (ADR 0090 D1), for every round. In order:

| verdict | when | ramp |
|---|---|---|
| **FAILED** | a generator's ledger did not close (missing, unexpected, unacked), a broker dropped a message by a reason that is loss (`mqttd_publish_dropped_total`, see below), or a broker restarted | stops the ladder (`BISECT_ON_FAIL=1` keeps bisecting below it) |
| **VOID** | a generator saturated in 2 or more of its measured windows (CPU, a worker thread, scheduling lag, or more than 50 softnet squeezes/s; a single such window is a pause and only a note), a generator's gated p99 above 5 × the other generators' median + 50 ms, a generator left no summary, the generators' windows overlap less than 90%, a broker of the run has no scrapes of the round or its scrapes do not cover the window, or the cluster was not idle (unreset: VOID before any other verdict, FAILED included) | the round is rerun (`VOID_RETRIES`, 1); never counted against the broker |
| **DISCONNECTED** | connections were lost during the step. mqtt-load does not reconnect, so its ledger cannot be read after that | not carried; the ramp treats it as above the knee |
| **NOT DRAINED** | the ledger misses deliveries, no broker dropped any as loss, the drain ended before it was complete, and the brokers' own gauges at their first scrape after the drain (`mqttd_inflight_messages` + `mqttd_pending_publishes`) hold at least what the ledger misses: the messages were still queued, not lost | not carried, not loss; above the knee |
| **OFFER NOT TAKEN** | the brokers received less than 99% of the offered msg/s (`OFFER_TAKEN` from `../summarize-curve.py`) | above the knee |
| **CARRIED** | none of the above | at or below the knee, whatever its latency |

A broker drop that is loss fails a step even when the generator's ledger closes. A VOID
ledger loss (the generator saturated) is not counted. When a generator saturated or
diverged, and the brokers' only loss was for slow consumers (`outbound-full`,
`backlog-overflow`, `queue-overflow`), the step is VOID too, with the finding listed in
its notes. A broker that drops for a client that stopped reading is doing its job, and
that client was the harness's. Drops on the publisher side, and broker restarts, still
fail the step.

**The missing count, reconciled.** When a drain ended before it was complete, the verdict
(`drain` in `verdict.json`, a note in the report) splits the ledger's missing count:
broker loss drops + still queued on the brokers at the cut-off (their unacked deliveries
and pending publishes, at most the rest) + unexplained (no drop counter and no broker
queue accounts for it). Run 2's clean-qos1 152,000: missing 7,511,964 = loss drops
5,933,730 (`backlog-overflow`) + 1,578,234 still queued (the brokers held 3,080,145 unacked
deliveries and 230 MB of backlog after the drain, and still did when mqtt-load exited
200 s later: those consumers' deliveries had stopped). FAILED either way; the loss column
shows both.

**Which drops are loss.** `verdict.py`'s `DROP_IS_LOSS` classifies every reason the broker
documents or increments (`crates/mqtt-observability/src/metrics.rs`, the call sites in
`crates/mqttd/src`), in one table: loss is a message the broker took and still owed to a
subscriber that exists (`outbound-full`, `append-backlog-full`, `queue-overflow`,
`backlog-overflow`, `no-shared-member`, `peer-unreachable`, `brownout`, ...). Not loss,
reported as a note: `pending-cap-replay`, `settle-replay` and `settle-replay-refused`
(an already-acknowledged entry leaving the settle-replay window; only a session created
during that window misses it), `pending-cap` (the ack is withheld, the publisher still
owns the message), `outbound-id-write-failed` (a deferral), and the retained-replay and
`$SYS` guards. A reason not in the table is loss. A test fails when the broker gains a
reason the table does not classify. The identity ledger stays the primary judge.

A round's latency is the exact merge of every generator's HDR histogram: mqtt-load
exports its buckets, and `verdict.py` adds the counts of equal buckets and reads the
quantile as hdrhistogram does. It is never an average of percentiles. A step's headline
across its rounds is the mean of the rounds' percentiles (the request of 2026-10-27: the
mean, with the variance), and the exact merge across the rounds is reported beside it.

**Maximum lossless** (the headline) is the highest CARRIED step (every round carried),
whatever its latency, with its mean ack p99 and end-to-end p99: a step above 1 s that
lost nothing still counts. Below it, the highest CARRIED step with **mean ack p99 ≤ 1 s**,
≤ 250 ms and ≤ 100 ms. For `qos0` the bands apply to the end-to-end p99.

## The ramp

The first step is the workload's `START_RATE_<tier>`. Coarse steps multiply by `FACTOR`
(1.5) on every CARRIED step, whatever its latency, until the first FAILED, OFFER NOT
TAKEN, DISCONNECTED or NOT DRAINED step. Then the ramp bisects between the last carried and that
step until their gap is at most `GAP_PCT` (3%) of the carried one. A VOID step is rerun. Rates are rounded to `GRANULARITY` (100 msg/s). A step that is not carried at the
first rate steps down by the factor instead. `MAX_STEPS` (14; local 3) and `MAX_RATE`
bound a ladder. The ramp reads only step verdicts, so a step's rounds never make the
ladder rerun it.

**Transient retries.** One bad round must not cap a ladder when nothing below it pointed at
the knee. A FAILED, OFFER NOT TAKEN, DISCONNECTED or NOT DRAINED round of a step whose
nearest carried step below it had its (mean) gated p99 (ack; QoS 0: end-to-end) under
`RETRY_P99_MS` (50 ms) is rerun once at the same rate, after the full settle. Only a
second bad attempt counts; a retry that carries lets the step carry, reported "round K
retried: transient". One retry a step and never a retry of a retry; a ladder retries at
most `RETRY_MAX` (2) times, so a real cliff cannot loop. (Ladders judged before rounds
retried the whole step, `<NN>-<rate>-a2`, reported "retried: transient" and drawn as a
dashed grey ring; their reports still read that way.) Why 50 ms: on
these hosts a flat, healthy step's p99 is 1 to 4 ms (durable and clean QoS 1, QoS 0
end-to-end below 75,000 msg/s); every carried step that preceded a real knee in runs 1
and 2 had already risen past it (durable 76,100: 66 ms, clean 101,300: 638 ms) or close to
it (QoS 0 112,500: 30 to 41 ms, retried and decided by the retry); 50 ms is half the
lowest reported band. Run 2's durable-qos1 had 32,400 and 33,100 carried at ack p99
1.7 to 1.8 ms, and 33,800 FAILED (290 `append-backlog-full`, 41 publisher disconnects) above
22,500 at 1.5 ms, where run 1 carried up to 90,500: under this rule 33,800 is retried.

## Output

`<run>/report.md` and `<run>/report.json` (`mqtt-rig2.report.v1`). Per step: the rounds
that count of those planned, msg/s offered, brokers received, delivered, the received
share, loss, ack p50/p99/p999, "ack p99 mean ± sd" (± the sample standard deviation, then
the merged rounds' p99), "CV" (of the received msg/s and of the p99; ⚑ when the rounds
disagree), end-to-end p50/p99/p999, generator health (worst CPU fraction and
scheduling-lag p99, saturation) and the verdict with the round that decided it and the
reasons; every figure of a step is the mean of its rounds. Per ladder: the maximum
lossless msg/s, the three latency-band lines (`capacity` in the JSON: `lossless`,
`p99_le_1000ms`, `p99_le_250ms`, `p99_le_100ms`, on the mean p99, with the rounds and the
merged p99), why the ladder ended, and a variance section: the largest CV of the received
msg/s and of the p99, and the steps whose rounds disagree by more than 10% in received
msg/s or 2× in p99. `verdict.py report <run>` re-renders them from the steps'
`verdict.json`.

**Charts.** One SVG per ladder, `<run>/report-<workload>.svg`, linked from report.md
(`chart-ramp.py`, called by the report; `chart-ramp.py <run>` redraws them from
report.json). x is the offered msg/s of every step; y the MEAN ack p99 of its rounds
(QoS 0: end-to-end) on a log scale with the mean p50 lighter, over the GREEN (≤ 1 s),
YELLOW (≤ 5 s) and RED bands; each mean carries an error bar from the rounds' min to
their max (the legend and subtitle say so). Carried steps are filled points, FAILED /
OFFER NOT TAKEN / DISCONNECTED hollow red rings, VOID dashed grey rings; the maximum
lossless step is labelled, and the title states it.

## Not yet

- **Same-hosts A/B** (ADR 0090 D5) is 0090-T4. The run layout already has an arm level
  (`<run>/<arm>/<workload>`, `ARM=A`), and `../swap-binary.sh` plus
  `../bootstrap-cluster.sh` swap a binary on the same hosts. `MQTTD_URL_B` is refused
  until T4 adds the interleaving.
- **A pre-built server image** (ADR 0090 D6). Bring-up is the old rig's cloud-init without
  the durable_bench build or Docker on the generator hosts.
- **Calibration against emqtt-bench** is 0090-T2.

Tests: `python3 bench/scale/rig2/test-verdict.py` (offline; CI runs it; it includes
`chart-ramp.py --self-test`) and `python3 bench/scale/rig2/test-run-sh.py` (offline; CI
runs it): the real `run.sh` at `TIER=local` against fake brokers, for a flaky scrape, the
wall-clock budget, a broker that never answers, a background counter tick (no hold), a
drain (held, and named), a settle that runs out on an idle pass (the reason still named),
a broker whose scrapes cannot be fetched (that round rerun alone), a `ROUNDS=2` step
against a generator stub that writes clean summaries, and a TERM mid-step.
