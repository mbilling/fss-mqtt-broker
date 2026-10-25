# mqtt-load: the benchmark rig's load generator

The load generator from [ADR 0090](../../docs/adr/0090-benchmark-rig-redesign.md)
(D2, task 0090-T1, issue #1109). It offers a stated rate in msg/s and measures the broker
in three ways:

- **ack latency**: PUBLISH written to PUBACK read, timed by the publisher.
- **end-to-end latency**: PUBLISH written to delivery read. The consumer runs in the same
  process and reads the same monotonic clock.
- **loss**: every message carries an identity. The ledger reconciles what was sent, acked
  and received.

It also reports its own health, so that a run the generator could not drive is VOID
rather than counted against the broker.

It is a client of the broker, built on the workspace's own codec (`mqtt-codec`), and is
never linked into `mqttd`.

## How it works

- **One process, one clock.** A group's publishers and consumers run in one process. Every
  timestamp is nanoseconds of the monotonic clock since one process-wide epoch. No latency
  figure crosses hosts, so no clock synchronisation is needed.
- **Open loop.** The schedule is a pure function of time. With `n` publishers at a total of
  `R` msg/s, publisher `i` sends message `m` at `phase_start + i/R + m·n/R`. The sends of
  different publishers interleave evenly, and nothing a broker does moves the offer.
  "Offered" in every report comes from this schedule, not from a count kept by the
  publishers.
- **In-flight window.** Each publisher may have up to `--inflight` QoS 1 messages
  unacknowledged, capped by the Receive Maximum in the broker's CONNACK. When the window
  is full, due messages wait in the generator. That wait shows up as:
  - `credit_stall_fraction`;
  - a backlog (offered minus sent);
  - `send_delay`;
  - and, after the window opens again, as a burst.

  A slow broker therefore shows up as ack latency and as the broker receiving less than
  the offer. It never shows up as a lower offer.
- **Two tasks per publisher.** A writer follows the schedule and writes every message
  that is due, up to the free credit, in one `write`. A reader reads PUBACKs. The send
  path encodes PUBLISH straight into the write buffer, so there is no allocation per
  message.
- **Consumers** use `$share/<group>/<topic>` groups (`--shape share`) or plain fan-out
  subscribers (`--shape fanout`). Their sessions are durable (`clean_start=false` plus a
  session expiry) or clean. Durable sessions are ended with a DISCONNECT carrying session
  expiry 0, unless `--keep-sessions` is given.
- **Endpoints.** Connections are spread round-robin over `--broker` endpoints.

### Payload

Each payload starts with a 32-byte big-endian header, followed by filler up to
`--payload` bytes:

| offset | size | field |
|---|---|---|
| 0 | 4 | magic `MQLG` |
| 4 | 4 | run id |
| 8 | 4 | publisher id |
| 12 | 4 | reserved (0) |
| 16 | 8 | sequence number within the publisher |
| 24 | 8 | send time, ns since the process epoch |

A delivery that carries another run's id (left over in a reused durable session) is
counted as `other_run` and ignored. A delivery with no header is counted as `foreign`.

### The identity ledger

The ledger keeps one bit per identity (publisher, sequence) in three sets:

- **sent** and **acked**, per publisher;
- **received**, per delivery stream. A stream is one `$share` group on one topic, or one
  fan-out subscriber.

The schedule fixes every publisher's message count in advance, so the bitsets are sized
once and loss is exact. After the drain:

- `missing`: sent, and not received on a stream that should have had it.
- `unexpected`: received, and never sent to that stream.
- `unacked`: sent at QoS 1 and not positively acknowledged. A PUBACK with a reason code of
  0x80 or more is also counted in `negative_acks`.
- `unexpected_acks`: a PUBACK for a packet id that had nothing in flight.
- `duplicates`: a stream received an identity it already had. These are counted, and they
  do not open the ledger, because QoS 1 may redeliver.

The ledger is `closed` when something was sent and `missing`, `unexpected`, `unacked` and
`unexpected_acks` are all 0. These are the same checks as the lane E reconcile in
`bench/scale/lane-e-evidence.py`.

### Generator health and VOID

Every window records:

- **process CPU**, as cores and as a fraction of the runtime's worker threads;
- **the busiest runtime worker thread** (Linux only, from `/proc/self/task`);
- **scheduling lag**: the delay from a message's due time to its write, counted only for
  sends that nothing outside the generator held back (no credit wait and no full socket
  since the message fell due). This is the generator's own lateness: timers, scheduling
  and CPU;
- **socket backpressure**: writes that found the socket buffer full, and the time spent
  waiting for it to drain;
- **credit stall**: time with messages due and no send credit. This is the broker's
  backpressure, not a generator problem, and it never saturates a window.

A window is **saturated** when any of these thresholds is crossed:

| threshold | default | flag |
|---|---|---|
| process CPU above this fraction of the worker threads | 0.85 | `--sat-cpu` |
| busiest worker above this fraction of a core (Linux) | 0.95 | `--sat-thread` |
| scheduling lag p99 above this | 10 ms | `--sat-lag-ms` |

A run with a saturated **measured** window is `void`. Rerun it with more generator
capacity; it says nothing about the broker. Tokio's timers have a resolution of 1 ms, so
the scheduling lag of an idle generator is about 1 ms (p50) and 3 to 4 ms (p99). The
10 ms threshold sits well above that floor.

## Usage

```sh
cargo build --release -p mqtt-load
target/release/mqtt-load --broker 10.0.0.1:1883,10.0.0.2:1883 \
  --rate 20000 --qos 1 --payload 128 \
  --topics 10 --shape share --consumers 2 --sessions durable \
  --warmup 10 --duration 60 --window 10 \
  --metrics 0.0.0.0:9464 --out summary.json
```

A step schedule for a ramp:

```sh
mqtt-load --steps 5000,10000,20000,40000 --warmup 10 --duration 60 ...
```

Each step gets its own warm-up and measured span. The publisher count is fixed for the
run (derived from the highest step), so the rate per publisher steps with the total.

| flag | default | meaning |
|---|---|---|
| `--broker` | `127.0.0.1:1883` | endpoints, comma-separated; connections are spread round-robin |
| `--rate` / `--steps` | 1000 | total msg/s; `--steps` gives one phase per rate |
| `--warmup`, `--duration`, `--window` | 10, 60, 10 s | per phase: warm-up, measured span, and measured-window length |
| `--publishers` | ceil(peak / `--rate-per-publisher`) | publisher connections (at least one per topic) |
| `--rate-per-publisher` | 10 | used to derive `--publishers` |
| `--inflight` | 32 | QoS 1 window per publisher, capped by Receive Maximum; it holds the offer up to `inflight / rate-per-publisher` seconds of ack latency |
| `--qos` | 1 | 0 or 1 |
| `--payload` | 128 | bytes, at least 32 |
| `--topics`, `--topic-prefix` | 10, `load` | publisher `i` publishes to `<prefix>/<i % topics>` |
| `--shape` | `share` | `share`, `fanout` or `none` |
| `--consumers`, `--groups`, `--group-prefix` | 2, 1, `g` | consumers per topic (per group for `share`); groups per topic |
| `--sessions`, `--session-expiry`, `--keep-sessions` | `durable`, 300 s | consumer sessions |
| `--client-prefix` | `mqload-<run id>` | client id prefix |
| `--drain-timeout`, `--drain-quiet` | 30 s, 5 s | the drain ends when nothing is in flight and every expected delivery has arrived, when nothing moves for the quiet period, or at the timeout |
| `--metrics` | off | Prometheus endpoint during the run |
| `--threads` | available parallelism | tokio worker threads (the denominator of the CPU fraction) |
| `--out` | stdout | JSON summary path |
| `--quiet` | off | no per-window lines on stderr |

Exit codes:

| code | meaning |
|---|---|
| 0 | ok |
| 1 | could not start (invalid workload, or a connection failed before the schedule started) |
| 2 | loss: the ledger did not close |
| 3 | void: the generator saturated |
| 4 | connections were lost or failed during the run |

When more than one applies, 4 takes precedence over 3, and 3 over 2.

## Prometheus metrics

All series are prefixed `mqload_`:

- counters `offered_total`, `sent_total`, `acked_total`, `received_total`,
  `duplicates_total`, `unexpected_total`, `negative_acks_total`, `unexpected_acks_total`,
  and `errors_total{kind}`;
- gauges `inflight`, `backlog`, `phase`, `target_rate`, `cpu_cores` and `saturated`;
- histograms `ack_latency_seconds`, `e2e_latency_seconds` and `sched_lag_seconds`, with
  buckets from 250 µs to 30 s.

The text is re-rendered once a second by the collector, so a scrape never touches the
measurement path.

## JSON summary: schema `mqtt-load.summary.v2`

Fields are only added within a version. A removal or a change of meaning bumps the version.
v2 adds `schedule_start_unix_ms` and the histogram bucket exports (`ack_histogram`,
`e2e_histogram`) on phases and totals; every v1 field is unchanged.

```text
schema            "mqtt-load.summary.v2"
tool              {name, version}
run_id            hex string; also in every payload and in the default client ids
started_unix_ms   wall clock, only for correlating with broker metrics
schedule_start_unix_ms
                  wall clock at the schedule's start (offset 0 of every start_s/end_s),
                  only for lining windows up with broker scrapes
workload          {phases: [{rate, warmup_s, measure_s}], window_s, publishers, inflight,
                   qos, payload_bytes, topics, topic_prefix, shape, consumers, groups,
                   sessions, session_expiry_s, deliveries_per_message, brokers}
connections       {publishers, consumers, receive_maximum_min, inflight_window_min, connect_s}
windows[]         one per warm-up, measured window and drain, in time order:
                  {index, phase, kind: warmup|measure|drain, start_s, end_s, target_rate,
                   <aggregate>, health}
phases[]          per phase over its measured windows: {index, target_rate, <aggregate>, saturated}
totals            <aggregate> over every measured window
drain             {duration_s, ended_by: complete|quiet|timeout}
ledger            {sent, acked, received, unique_received, expected, missing, unexpected,
                   unacked, unexpected_acks, negative_acks, duplicates, other_run, foreign, closed}
generator         {saturated, saturated_windows[], thresholds{cpu_fraction, thread_fraction,
                   sched_lag_p99_us}, worker_threads, per_thread_cpu, cpu_cores_max,
                   sent_per_cpu_second}
errors            {connect, disconnects, server_disconnects, protocol, write}
outcome           ok | loss | void | error

<aggregate> = {duration_s, offered, sent, acked, received,
               offered_rate, sent_rate, acked_rate, received_rate,
               ack_latency, e2e_latency, sched_lag, send_delay,
               ack_histogram, e2e_histogram}   (the two histograms: phases and totals only)
latency     = {count, mean_us, p50_us, p90_us, p99_us, p999_us, max_us}
histogram   = {unit: "us", significant_figures: 3, highest_trackable_us,
               buckets: [[value_us, count], ...]}   ascending, recorded buckets only
health      = {cpu_cores, cpu_fraction, busiest_worker (null off Linux),
               credit_stall_fraction, socket_blocked, socket_blocked_fraction,
               backlog, saturated, reasons[]: cpu|worker_thread|sched_lag}
```

Notes on the fields:

- `received` counts deliveries over all streams. Divide by
  `workload.deliveries_per_message` (the number of streams per topic) to compare it with
  the publish rate.
- Counts are attributed to the window in which they happened: a send to the window it
  was written in, an ack or a delivery to the window it was read in. Collection runs on a
  1 s tick, so a window's edges are accurate to a few milliseconds.
- `ledger` covers the whole run, warm-up and drain included.
- **Merging histograms.** Every histogram has the same bounds and precision, and
  `value_us` is the highest value equivalent to its bucket. Histograms from several
  generator processes therefore merge exactly by adding the counts of equal `value_us`.
  A quantile `q` of the merge is the `value_us` of the first bucket at which the
  cumulative count reaches `max(1, ceil(q * total))`, which is what `hdrhistogram`
  reports for `*_latency.p99_us` and the like. `bench/scale/rig2/verdict.py` does this.

## Measured on a developer machine

These figures come from one Apple M1 Pro (8 cores), against a release build of a
single-node mqttd on the same machine, with its data dir on. They are 10 s runs after a
3 s warm-up, at 100 msg/s per publisher unless noted, with 10 topics. The generator never
saturated in any of these runs; the broker sharing the machine was the limit every time.
A dedicated generator host will carry more. T2 calibrates properly.

| workload | offered msg/s | carried | ack p99 | generator CPU | msg/s per generator core |
|---|---|---|---|---|---|
| QoS 1, durable `$share`, 2 consumers/topic | 20,000 | all, ledger closed | 16 ms | 0.47 cores | 42,500 |
| the same, 10 msg/s per publisher (2,000 publishers) | 20,000 | all, ledger closed | 227 ms | 0.49 cores | 41,000 |
| QoS 1, durable `$share`, 4 consumers/topic | 30,000 | all, ledger closed | 17 ms | 0.78 cores | 38,300 |
| QoS 1, clean `$share` | 80,000 | all, ledger closed | 7 ms | 1.98 cores | 40,400 |
| QoS 1, clean `$share`, 4 consumers/topic, 1,000 msg/s per publisher | 160,000 | 111,500 (the broker's limit) | 419 ms | 2.0 cores | 56,700 |
| QoS 0, clean `$share`, 4 consumers/topic, 1,000 msg/s per publisher | 200,000 | all | — | 2.2 cores | 92,200 |

"msg/s per core" is messages written per CPU-second of the whole generator process. At
QoS 1 that one message costs four packets:

- a PUBLISH written;
- a PUBACK read;
- a delivery read;
- a PUBACK written.

emqtt-bench was not run beside these figures: its image is amd64-only, and on this arm64
machine it would run emulated inside Docker's VM, which is not a fair comparison. T2 does
the calibration.

## Known limits

- MQTT 5 only, and QoS 0 or 1. QoS 2 is not implemented.
- No reconnect. A connection the broker closes stays closed, and the run reports it
  (`errors`, outcome `error`). Under overload the broker's durable plane closes
  publishers on purpose: a full append lane fails the publish closed.
- No TLS.
- Per-thread CPU is read on Linux only. Elsewhere `ps` gives the process total.
- The payload carries the send time of the write. For messages that waited for credit
  or for the socket, the time they spent queued in the generator is reported separately
  (`send_delay`) and is not part of ack or end-to-end latency.
