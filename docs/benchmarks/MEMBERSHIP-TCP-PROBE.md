# Membership investigation: profiles, TCP receipts and fixed offer (#613)

Follow-up: [instruction accounting and the group-first shared index](GROUPED-SHARED-INDEX.md).
The timing failures below remain failures; the follow-up makes a narrower work-count claim.

Follow-up to [the channel probe](MEMBERSHIP-PROBE.md), on production main
`5c5cbae` plus the benchmark changes described here. **No production optimization
or broker scale-out claim.** The new measurements expose substantial fixture/host
variability; they do not establish a stable TCP effect size for the matching loop.

## 1. Profile the candidate, not the historical explanation

Recorded A/miss versus B/match at nine advertised peers, four local shared workers,
using the original channel fixture and its exact receipt oracle. Commands used
`perf record -e cycles:u -F 199 --call-graph dwarf,8192`, with Criterion
`--profile-time 15 --exact qos0_membership_receipts/<arm>/9`, Hub CPU 2 and two
drain workers on CPUs 3/4. Profiles cover setup plus the profile loop, not an aligned
capacity window; they are **userspace CPU samples**, not off-CPU or kernel profiles.

The matching run samples the remote callback inside `Hub::plan_shared`. Source
annotation resolves it to `remote_by_filter.get(filter)`, the location loop,
remote node/group lookup and the `decided` membership check. Hashing symbols are
prominent too. This supports investigating the remaining location-resolution work;
it does **not** resurrect the already-fixed all-filter scan or establish that every
sample in a hashing helper belongs to this callback. Inclusive Rust call chains
were incomplete, so no claimed inclusive percentage or cycles/publication is derived.

Both recordings report zero lost samples. Artifacts:
`.scratch/membership-613/profile-20260914T105322Z/`, including both perf.data files,
logs, leaf-symbol reports, source annotation and the exact profiled binary retained
from perf's build-ID cache (`e769f760463f13652f99bc24d199b2ac1086c37d`).

`perf` was not installed. Its signed Arch package and required shared libraries were
verified with the system keyring and extracted into `.scratch/membership-613/tools/`.
No system package installation, governor change, privilege escalation or cloud
provisioning was performed. The recordings used perf 7.2-1.

## 2. What the TCP fixture adds

`crates/mqttd/benches/shared_membership/tcp.rs` runs real localhost MQTT sockets:

- One publisher and four shared subscribers use the real connection decoder,
  Hub, outbound queues and socket writer. Broker tasks share one current-thread
  runtime; client tasks use the separate endpoint runtime. This constrained
  runtime placement is a mechanism fixture, **not the production daemon's default
  multithreaded runtime configuration**.
- The A/B/C/D remote-interest matrix is unchanged. Peer links are still **synthetic
  channels**, not a mesh of TCP brokers. This test does not measure peer encoding,
  peer network bandwidth, forwarding capacity or N-node scale-out.
- Each subscriber also has a unique ordinary fence topic. After sending 2,000
  sequence-labelled QoS 0 publications, the publisher sends PINGREQ. PINGRESP
  proves preceding packets were decoded/handed to the Hub, **not delivered**.
- A subsequent Hub barrier establishes completion of those dispatches. Four
  ordinary fence publications are then enqueued, one per subscriber. Each fence
  travels behind that subscriber's data through the **same outbound FIFO and TCP
  writer**. Consumers read to the fence and validate all data IDs. This catches a
  missing or extra final message without a silence timeout or count-only success.
- Exactly 500 data receipts per member and 2,000 unique receipts overall are
  required. The four fence messages are control overhead, excluded from reported
  data counts. Burst duration includes publisher PING, Hub barrier, fences and
  validation; it is not a bare message-latency histogram.
- Client and accepted sockets have TCP_NODELAY enabled, MQTT 3.1.1, clean sessions,
  no TLS/auth policy costs (the permissive test handler), keepalive disabled and
  200-byte data payloads. This is not a claim for secured production workloads.
- Setup and each burst retain the ten-second failure deadline. Connections,
  listeners and client tasks are owned/aborted on fixture drop. Peer validation
  stays outside timing, rejecting data forwards even if local delivery succeeded.

Use the existing benchmark with `MEMBERSHIP_TRANSPORT=tcp`. TCP has its own
`qos0_tcp_membership_receipts` result namespace and deliberately has **no channel
bypass calibration**. `MEMBERSHIP_DRAIN_THREADS` defaults to two; CPU placement
variables remain explicit and failed pinning remains fatal.

```sh
# Choose CPU IDs appropriate to the machine. Use a NEW artifact directory per run.
out=$(mktemp -d /tmp/membership-tcp.XXXXXX)
CRITERION_HOME="$out/criterion" MEMBERSHIP_TRANSPORT=tcp MEMBERSHIP_SEED=615 \
  MEMBERSHIP_HUB_CPUS=2 MEMBERSHIP_DRAIN_CPUS=3,4 \
  cargo bench -p mqttd --bench shared_membership -- \
  --warm-up-time 1 --measurement-time 2 --sample-size 20 --noplot \
  'qos0_tcp_membership_receipts/(opening|closing|[CD]-.*/9)$'
```

As with the channel probe, archive the command, log, exit status, source and raw
samples. A zero benchmark exit means receipt correctness passed; it does not mean
the independent performance-validity gates passed.

## 3. TCP Criterion trials: keep the invalid controls

Same shared i7-9750H workstation, rustc 1.97.0, benchmark profile, production source
`5c5cbae`. No build/tests ran concurrently with these trials. Fixed four subscribers,
2,000 data messages per burst and nine synthetic peers in C/D. Seeds randomize C/D
ordering; standalone controls bracket the pair. Every arm passed the receipt and
zero-forwarding oracle. Below are **within-run mean** changes from raw Criterion
estimates, not its console regression slope and not cross-run confidence bounds.

| Seed | Warm-up | Endpoint threads / CPU set | Closing vs opening mean burst time | D vs C mean burst time | Attribution validity |
|---|---|---|---|---|---|
| 615 | 1 s | 2 / 3,4 | -65.99% | +33.18% | invalid control |
| 616 | 1 s | 2 / 3,4 | -75.40% | +29.94% | invalid control |
| 617 | 1 s | 2 / 3,4 | -20.61% | +25.03% | invalid control |
| 618 | 1 s | 2 / 3,4 | +3.27% | +27.76% | control passes; not full calibration |
| 619 | 1 s | 2 / 3,4 | -16.44% | +27.55% | invalid control |
| 625 | 10 s | 2 / 3,4 | -3.09% | +24.62% | control passes; not full calibration |
| 626 | 10 s | 2 / 3,4 | -38.32% | +99.87% | invalid control |
| 635 | 10 s | 4 / 0,1,3,4 | -61.01% | +42.28% | invalid control |
| 636 | 10 s | 4 / 0,1,3,4 | +50.73% | +18.91% | invalid control |

The original **5% control gate is unchanged**. Failed one-second-warm-up controls
motivated a separately declared ten-second warm-up (as proposed in the investigation
plan), not a changed deadline or acceptance rule. Longer warm-up did not reliably
stabilize controls. The endpoint-resource intervention is consequently inconclusive:
it does not certify headroom, identify the endpoints as the limit, or clear them.

Each run is preserved under `.scratch/membership-613/<UTC>-tcp-seed<seed>/`, with
manifest, fixture sources, logs, raw Criterion samples/estimates, CPU samples and
exit status. Starting at seed 625, `events.jsonl` timestamps console phase markers.

## 4. Fixed offer and aligned CPU windows

To stop conflating adaptive burst throughput with equal useful work,
`crates/mqttd/benches/shared_membership/paced.rs` adds a separate paced mode:

```sh
MEMBERSHIP_PACED=1 MEMBERSHIP_TRANSPORT=tcp MEMBERSHIP_SEED=645 \
  MEMBERSHIP_RATE=100000 MEMBERSHIP_SECONDS=60 MEMBERSHIP_WARMUP=10 \
  MEMBERSHIP_HUB_CPUS=2 MEMBERSHIP_DRAIN_CPUS=3,4 \
  cargo bench -p mqttd --bench shared_membership
```

This mode uses environment settings, not Criterion timing/filter arguments, and
runs opening/P=0, C/P=9, D/P=9, closing/P=0 (C/D reverse for even seeds). Warm-up
uses the same paced workload but is excluded from measured results. Each burst
contains 2,000 data messages; the schedule is intentionally bursty, not an
independently paced publication per MQTT client. A burst must finish with receipts
before the next can begin. If it falls behind, **no scheduled work is skipped**;
actual elapsed time and schedule lateness expose under-offer.

Each `window_end` JSON includes scheduled/received data count, actual elapsed time,
measurement UTC boundaries, every burst duration/start lateness, and CPU tick deltas
from the same window. `/proc/thread-self/stat` measures the hot current thread
(Hub **plus** server connection tasks and orchestration/validation), while
`/proc/self/stat` also includes endpoint threads. Neither is Hub dispatch occupancy.
The clock tick frequency is recorded at `window_prepare`. CPU reads bracket the
monotonic wall timer with two small /proc reads; this is not an atomic kernel
snapshot. UTC boundaries are for sampler alignment, not the elapsed denominator.

The CPU parser explicitly handles spaces/parentheses in process names and excludes
child-process CPU. Raw integer ticks are retained. p99 below is the nearest-rank
**burst completion** time, an upper-bound diagnostic including fences, not measured
per-message latency. Exact fences establish zero outstanding data obligation at
burst boundaries, not continuous queue-depth or RSS stability inside a burst.

### Seed 645: 100,000/s, 60 seconds per arm

| Arm | Data receipts | Actual elapsed | Received/s | Hot-thread CPU | Mean / p99 burst | p99 start lateness |
|---|---|---|---|---|---|---|
| Opening | 6,000,000 | 60.005 s | 99,991 | 53.96 s | 19.40 / 30.70 ms | 563 ms |
| C/miss | 6,000,000 | 61.880 s | 96,962 | 55.84 s | 20.10 / 33.21 ms | 1,778 ms |
| D/match | 6,000,000 | 60.002 s | 99,997 | 54.49 s | 19.20 / 31.51 ms | 602 ms |
| Closing | 6,000,000 | 60.224 s | 99,629 | 52.60 s | 19.02 / 31.94 ms | 474 ms |

Receipt and control checks pass (mean burst drift -1.97%, CPU drift -2.52%), but
**C under-offers by 3.04%, failing the declared 1% actual-offer gate**. Its runtime
uses about 90% of one CPU; pace debt grows. This is not an equal-offer comparison
and does not establish a matching-path capacity effect. A per-message SLO timed
only after actual send would conceal this schedule delay, which is why both are
retained separately. The next diagnostic rate was declared as 50,000/s, not a
reinterpretation of this arm as a pass.

### Seed 646: 50,000/s, 60 seconds per arm, reversed C/D order

| Arm | Data receipts | Received/s | Hot-thread CPU | Mean / p99 burst | p99 start lateness |
|---|---|---|---|---|---|
| Opening | 3,000,000 | 49,999 | 42.68 s | 31.09 / 48.44 ms | 23.43 ms |
| D/match | 3,000,000 | 49,997 | 48.66 s | 34.79 / 62.26 ms | 96.87 ms |
| C/miss | 3,000,000 | 49,999 | 38.37 s | 27.54 / 47.28 ms | 17.23 ms |
| Closing | 3,000,000 | 49,999 | 34.58 s | 24.86 / 38.01 ms | 1.78 ms |

All four meet the offer/receipt gate, but **control drift fails**: -20.05% mean
burst time and -18.98% hot-thread CPU. Do not use D/C here as a production effect
size or accept the apparent improvement caused by lower offer. The host's CPU 2
uses the powersave governor; frequency sampling was added for this run. That is
another uncontrolled variable to investigate, **not proof that DVFS caused the
entire instability**. No governor setting was changed.

Complete aligned logs and summaries are preserved in:

- `.scratch/membership-613/20260914T112212Z-paced-tcp-seed645/`
- `.scratch/membership-613/20260914T112955Z-paced-tcp-seed646/`

These local artifacts are not a hosted evidence bundle. There has been no cloud run,
real peer-mesh measurement, independent endpoint certification or N=1/2/4 scale-out
acceptance. All production routing, ownership fencing and QoS 1/2 behavior are unchanged.

## 5. What this changes about the next action

The channel profile names remaining matching-location work, and TCP probes keep it
plausible. But repeated invalid controls mean a production speedup claim would be
premature. **Do not select an optimization using only the largest D/C ratio.**

Next, establish repeatable controls on a fixed-power/resource-isolated local rig,
or use per-stage CPU/cycle accounting to distinguish connection scheduling and
frequency effects from the matching pass. Preserve the existing failures, retain
the 1% offer and 5% control gates, and independently certify concurrent endpoints.
Then compare one measured routing change against the same paced TCP workload and
proceed to broker scale-out with real peers. Fixing lane E's corresponding window
accounting remains necessary before using its ladders as acceptance evidence.

Validation adds TCP matrix coverage and CPU-parser tests. A deliberately omitted
last TCP publication was exercised: the fence oracle immediately failed with 499
receipts instead of 500, without waiting for the ten-second failure deadline. The
mutation was restored; the failure log is retained under the validation artifacts.

Final validation: `cargo test -p mqttd` exited 0 with **713 passed, 0 failed and
9 existing ignored**. All five membership-fixture tests passed. All-target mqttd
clippy with `-D warnings`, workspace formatting, test hygiene/inventory (1,605 tests
in 74 binaries), README facts, generated dashboard and diff whitespace checks passed.
