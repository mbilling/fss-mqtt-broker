# Durable-path benchmark — end-to-end acked throughput and latency

**Dated 2026-08-14.** [ADR 0048](../adr/0048-comparative-benchmarking.md) T5 ·
issue [#244](https://github.com/mbilling/fss-mqtt-broker/issues/244) · harness:
`crates/mqttd/tests/durable_bench.rs`

## Read this first: what these numbers are, and what they are not

This is the first end-to-end **durable-path** measurement in the repository —
acked QoS 1 and QoS 2 publish throughput and latency percentiles against a real
quorum with the durable plane on, measured through the production binary's own
client port.

It is **not** the multi-host result issue #244 asks for. Every number below was
produced on **one developer machine**, with three or five broker processes and the
load driver sharing **8 cores and one APFS volume**, over **loopback**. In this
repository's vocabulary that makes them **development-grade** (`bench/README.md`,
ADR 0048 phase 2): they are published so the method, the harness and the shape are
checkable, and so nobody has to take "it scales" on trust — but they are not a
capacity claim, they are not a comparison, and they must never be quoted as
"mqttd does N msg/s".

Four specific things they do **not** bound:

1. **Per-node capacity on real hardware.** Section [The durability barrier is the
   ceiling](#the-durability-barrier-is-the-ceiling) shows that the durable numbers
   here are pinned by this machine's **per-volume** device cache flush rate —
   ~215–240/s, shared by every process on the disk. A server with one device per node
   (or a device whose barrier is faster) is a different measurement, not a scaled
   one.
2. **The scaling curve.** Throughput/p99 versus node count is **still unmeasured
   and still unpublished**, deliberately: ADR 0048 §2 forbids running it
   single-host, because N nodes contending for one disk queue scale *negatively*
   and would manufacture false evidence against the broker (the
   [7-node single-host post-mortem](../postmortems/2026-07-14-ha-bridge-durable-refused.md)
   is what taught that). There is no node-count curve in this document. There is a
   single fixed 3-node point. *(2026-08-20: the multi-host rig and method for that
   curve now exist — `bench/scale/run.sh` and `docs/benchmarks/SCALE-CURVE.md` —
   with no published run yet.)*
3. **Anything about competitors.** No cross-broker number appears here; that is
   `bench/`'s job and ADR 0048 T4's.
4. **Connection scale.** Every number here was measured at **at most ~32 concurrent
   connections** (16 publishers + 16 subscribers). Issue #244 also asks about the
   5,000-connection envelope, and this document does not touch it: connection capacity is
   a memory-and-file-descriptor question, measured by `bench/run.sh`'s `conn` scenario
   (RSS delta per idle connection), not by this durable-path harness. The two are
   deliberately separate measurements, and neither substitutes for the other.

The multi-host lane is real code, not a promise — the same driver runs against
operator-provisioned brokers with three environment variables — and
[its invocation is written out below](#the-multi-host-lane-documented-and-unrun).
It has not been run, because it needs hosts this lane did not have.

## Host, build, and configuration

Everything below was measured on:

| | |
|---|---|
| Host | Darwin 23.5.0 arm64 (macOS 14.5), 8 cores, 16 GiB RAM |
| Disk | APFS on internal SSD (the temp volume the nodes' data dirs live on) |
| Build | `--release` (opt-level 3), workspace at `origin/main` + this lane |
| Broker processes | 3 (5 for the isolation experiment), each `TOKIO_WORKER_THREADS=2` |
| Driver | in the same `cargo test` process, tokio `worker_threads = 2` |
| Broker log level | `RUST_LOG=warn` |
| Durable plane | **ON** — nothing sets `MQTTD_DURABLE_SESSIONS=0` |
| Replication | R=3, quorum 2, write floor `majority` (derived), 256 placement groups |
| Auth / transport | `MQTTD_ALLOW_ANONYMOUS=1`, plaintext (so **no TLS cost is included**) |
| Peer bus | loopback, fronted by the harness's relay with `TCP_NODELAY` set |

Thread arithmetic, stated because it matters: 3 brokers × 2 worker threads + 2
driver threads = 8 threads on 8 cores. The isolation experiment runs 5 brokers
(12 threads on 8 cores) under a much lighter load, and reports a **ratio**, which
is why it survives that oversubscription.

The host was **not** quiet — it is a working developer machine. That shows up as
run-to-run spread, and the spread is printed rather than smoothed: every
configuration ran **3 times, twice over** — the durable-path tables are
`median [min..max]` across 6 reps of two separate invocations of the same command — and
the isolation experiment ran 4 times.

## Method

Closed-loop and saturating. `MQTTD_BENCH_PUBLISHERS` publisher connections each hold
a sliding window of `MQTTD_BENCH_WINDOW` publishes in flight — one packet id per
window slot, re-issued the instant its ack lands — so the offered rate *is* the
achieved rate, and the measured latency is a real per-message ack round trip:

- **QoS 1** — PUBLISH → PUBACK.
- **QoS 2** — PUBLISH → PUBREC → PUBREL → PUBCOMP (latency to PUBCOMP).

Because the broker acks **after** durable (issue #124 / ADR 0057), that round trip
*is* the end-to-end durable-commit latency as a publisher experiences it. That is the
whole reason this measurement is possible from the outside.

Subscribers are real, online and draining: `MQTTD_BENCH_SUBS` sessions, one topic
each, acking every delivery. Their client ids are chosen with the broker's **own**
placement hash so every session is HRW-owned by node 0 — which is what makes
"publish via the owner" and "publish via a non-owner" separable arms rather than an
uninterpretable average. Subscriptions are granted at QoS 1 in every arm, so the
QoS 2 arm measures the *inbound* exactly-once path (dedup record, PUBREC/PUBREL/
PUBCOMP, outbound-id record) with the delivery side held constant.

Warm-up (5 s) is discarded **by timestamp** — a sample counts only if its ack landed
inside the measurement window.

### A run judges itself

The harness prints a verdict beside every number and distinguishes **INVALID** (do
not quote this) from a **caveat** (quote it with the caveat):

| Signal | Verdict |
|---|---|
| `debug_assertions` (unoptimized profile) | INVALID |
| any publisher connection error / 30 s silence with publishes outstanding | INVALID |
| `mqttd_durable_append_failures_total` moved | INVALID |
| `mqttd_publish_dropped_total{reason="append-backlog-full"}` moved | INVALID (lane saturation) |
| `mqttd_replication_min_actual < mqttd_replication_write_floor` on any node | INVALID (measured under a refusal regime) |
| no completion inside the window | INVALID |
| fewer than 500 samples | caveat: "thin sample — p99 rests on ~N points" |
| driver burned more CPU than the busiest broker | caveat: "driver-bound — read the rate as a floor" |
| broker CPU unavailable (the **external multi-host lane**) | caveat: the driver-bound check **did not run**, and says so — it needs the brokers' CPU, which a remote cluster does not expose. It prints the driver's own CPU-seconds so a reader can judge |

Broker-side counters are **differenced across each arm** and printed next to the
client-side tail, so a reader can see whether a run was store-bound or loop-bound:
`mqttd_durable_append_latency_seconds` (the store's share of the ack RTT),
`mqttd_hub_dispatch_seconds{command="publish"}` (the single-threaded hub loop's own
dispatch tail), `mqttd_append_lane_jobs` (ADR 0061 lane depth). Per-process CPU is
`ps -o cputime=` differenced across the arm — exact CPU seconds consumed, which
Darwin's decaying `%cpu` average cannot give.

## Result 1 — latency at low load (one publisher, one in flight)

The per-message cost of a durable publish with **no queueing anywhere**: one
publisher, window 1, one subscriber session, 40 s measurement window. The command was
run **twice**, 3 reps each, and the cells below are `median [min..max]` over all
**6 reps** — including the one that came out INVALID, which stays in the range.

```sh
MQTTD_BENCH_PUBLISHERS=1 MQTTD_BENCH_WINDOW=1 MQTTD_BENCH_SUBS=1 \
MQTTD_BENCH_REPS=3 MQTTD_BENCH_SECS=40 MQTTD_BENCH_WARMUP_SECS=5 \
cargo test --release -p mqttd --test durable_bench durable_path_floor \
  -- --ignored --nocapture
```

| Arm | acked msg/s | p50 | p95 | p99 | p99.9 | max | stalls ≥1 s per rep | broker append mean | verdict |
|---|---|---|---|---|---|---|---|---|---|
| `qos1-durable-owner` | 24 [4..36] | 27.6 [26.4..29.8] ms | 43.8 [34.6..52.6] ms | 58.1 [37.8..80.1] ms | 100 [63..5029] ms | 5033 [71..10030] ms | 0,1,1,1,1,3 | 11.9 [11.2..26.2] ms | **1 of 6 INVALID** — a publisher error with `durable_append_failures_total` moving (see finding 2); that rep's 4 msg/s is the range's floor |
| `qos1-durable-relay` | 28 [23..31] | 26.7 [25.5..28.6] ms | 44.8 [35.8..58.8] ms | 66.5 [57.9..80.7] ms | 2611 [76..5094] ms | 5200 [5057..10155] ms | 1,1,1,2,2,2 | 10.8 [9.7..11.4] ms | valid |
| `qos1-clean-owner` | 30000 [29137..30351] | 0.03 ms | 0.05 ms | 0.06 ms | 0.11 ms | 6 [1..39] ms | 0,0,0,0,0,0 | — (nothing durable is written) | valid |
| `qos2-durable-owner` | 9 [8..10] | 106 [96..111] ms | 138 [124..154] ms | 167 [135..211] ms | 217 [152..5097] ms | 217 [152..5097] ms | 0,0,0,0,0,1 | 10.3 [9.3..11.1] ms | valid — thin sample (~350): p99 rests on ~3 points |

One thing to read past first: the owner arm's **median rate** (24) is *below* the relay
arm's (28) only because of that one INVALID rep, whose 4 msg/s drags the median of six.
The latency columns — which are what this configuration exists to measure — are
indistinguishable between the two arms. This is exactly why the invalid rep is left in
view rather than dropped.

Four things this says, in order of how much they matter:

1. **The durable path costs ~900× a non-durable publish per message here.**
   27.6 ms versus 0.03 ms at p50. That is not a scandal, it is the guarantee: the
   PUBACK is withheld until the record is quorum-durable. It *is* the number an
   operator needs, and `docs/SIZING.md`'s advice — use clean sessions where
   redelivery across a restart is not needed — is worth exactly this ratio.
2. **The owner-relay hop is free at this load.** Publishing through a node that does
   not own the subscriber's session (ADR 0005 relay) costs nothing measurable
   (26.7 ms vs 27.6 ms at p50, well inside the rep-to-rep spread). The cross-node hop is
   microseconds on loopback; the durable append dominates so completely that the hop
   does not show. On real hosts the hop is a real network RTT — this arm exists so
   that comparison can be made there, and it is one of the things the multi-host run
   should re-measure.
3. **QoS 2 costs ~4× QoS 1 per message** (106 ms vs 27.6 ms), because the inbound
   exactly-once flow adds durable records of its own around the same barrier.
4. **There is a repeatable multi-second tail that p99 does not show.** Every rep but
   one contains 1–3 completions at **5.0 s or 10.0 s** — exactly 1× and 2× the 5 s
   replication RPC bound (`crates/mqtt-cluster/src/repl_net.rs`) — on an idle, healthy,
   3-node loopback cluster with no fault injected. p99 is 58 ms; max is 10 s. This is
   reported as an observation, not explained: see
   [Findings this benchmark surfaced](#findings-this-benchmark-surfaced).

## Result 2 — throughput under saturation

Same cluster, 16 publishers × window 8 = 128 publishes in flight across 16 durable
sessions, 30 s measurement window. Run **twice**, 3 reps each; cells are
`median [min..max]` over all **6 reps**.

```sh
MQTTD_BENCH_PUBLISHERS=16 MQTTD_BENCH_WINDOW=8 MQTTD_BENCH_SUBS=16 \
MQTTD_BENCH_REPS=3 MQTTD_BENCH_SECS=30 MQTTD_BENCH_WARMUP_SECS=5 \
cargo test --release -p mqttd --test durable_bench durable_path_floor \
  -- --ignored --nocapture
```

| Arm | acked msg/s | p50 | p95 | p99 | p99.9 | max | stalls ≥1 s per rep | broker append mean | verdict |
|---|---|---|---|---|---|---|---|---|---|
| `qos1-durable-owner` | 27 [27..30] | 3219 [2828..3395] ms | 13172 [3638..13765] ms | 13284 [3703..13858] ms | 13339 [3703..13902] ms | 13354 [3713..13908] ms | 798–903 | 29.5 [27.0..40.1] ms | valid |
| `qos1-durable-relay` | 17 [12..28] | 5201 [3334..6853] ms | 15602 [6270..18524] ms | 16586 [6842..20052] ms | 16639 [7428..20076] ms | 16708 [7429..20076] ms | 353–830 | 63.3 [51.0..70.5] ms | valid — thin sample in 1 rep |
| `qos1-clean-owner` | 81319 [37285..117577] | 1.4 [1.1..2.1] ms | 2.0 [1.6..10.5] ms | 2.3 [1.9..23.3] ms | 3 [2..46] ms | 12 [6..107] ms | 0 | — (nothing durable is written) | valid |
| `qos2-durable-owner` | 11 [8..13] | 8274 [6863..19001] ms | 18582 [7888..21098] ms | 18850 [8132..21235] ms | 19466 [8233..21278] ms | 19466 [8233..21278] ms | 248–384 | 72.6 [59.4..90.3] ms | **2 of 6 reps INVALID** — publisher errors, `durable_append_failures_total` moving |

This table's job is to be read **against the previous one**, and the comparison is the
most useful thing on this page:

- **128× the concurrency bought no more throughput and ~117× the latency.**
  `qos1-durable-owner` goes from 24 msg/s at 1 publish in flight to 27 msg/s at 128
  (the same to within the rep spread of both), while p50 goes from 27.6 ms to 3219 ms. That is the textbook signature of a hard
  capacity ceiling with a queue in front of it: the extra load became queueing delay,
  nothing else. The next section says what the ceiling is.
- **Under saturation the owner-relay hop costs throughput** (median 17 vs 27 msg/s,
  broker append mean 63 ms vs 29 ms — though its 12–28 msg/s spread overlaps) — even though at low load it was
  free. Publishing through a non-owner adds work that only shows when the durable path
  is already the constraint. On real hosts, where the hop is also a network RTT, expect
  this gap to be larger, and measure it there.
- **The non-durable arm is limited by something else entirely**: ~81 000 msg/s with
  node 0 burning 43–62 CPU-seconds in a 30 s window (i.e. ~1.5–2 cores) and the driver
  burning 19–32. That arm is CPU-bound on a shared 8-core box with the driver as a
  co-tenant, and its **37 000–118 000 msg/s** spread across 6 reps is the honest width
  of that measurement, not a headline. It is here as the **durable-vs-non-durable delta**, not as a throughput
  claim.
- **QoS 2 at 128 in flight failed 2 of 6 reps, and those reps stay in the table.**
  Pipelined QoS 2 publishers wedged (PUBRECs arriving, PUBCOMPs stopping) and durable
  appends failed; see finding 1 below. On this build QoS 2 is only reliably measurable at
  low concurrency, which is itself a result.
- **`mqttd_hub_dispatch_seconds{command="publish"}` p99 stayed at ≤ 0.2 ms in every
  arm of every rep.** That series is the load-bearing one: it is a histogram over every
  dispatch, so a stall anywhere in the window would show in its tail. The hub loop is not
  the bottleneck — ADR 0061's property held throughout, which Result 3 measures directly.
  The `mqttd_append_lane_jobs` figures (7–29 of the 256-job cap) are reported for
  completeness but prove **nothing** about queue depth under load, and the earlier draft
  leaned on them wrongly: that gauge is *current* depth, and the harness scrapes it once,
  after every publisher has already stopped. What it shows is that lanes had drained, not
  how deep they got. Sampling it during the load would need a scrape loop the harness does
  not have.

## The durability barrier is the ceiling

The durable rows above are not CPU-bound and not network-bound: the brokers burned
~2 s of CPU across 40 s (about 5 % of one core) while acking 28 msg/s. Two
in-tree micro-benchmarks say what they *are* bound by, and this is the single most
important context for every durable number on this page.

```sh
cargo test --release -p mqttd --test durable_bench device_barrier_floor -- --ignored --nocapture
cargo test --release -p mqttd --test durable_bench store_append_floor   -- --ignored --nocapture
```

**The device barrier** (`std::fs::File::sync_data()` — the exact call one durable
commit waits on):

| concurrent writers, separate files | aggregate barriers/s (2 runs) | p50 | p99 |
|---|---|---|---|
| 1 | 214 / 234 | 4.86 / 3.98 ms | 8.96 / 7.94 ms |
| 3 | 217 / 238 | 11.61 / 11.24 ms | 24.74 / 20.57 ms |
| 8 | 361 / 292 | 14.61 / 17.39 ms | 39.00 / 43.61 ms |

The flat column is the finding: this is a **per-volume** device cache flush, so
concurrency buys almost nothing — 8 writers get ~1.3× the aggregate of 1, and pay it
back entirely in latency — and every store on the machine (every node's session log,
replica store, lease store) draws from the same **~215–240/s** budget.

Why `sync_data` and not `fsync`: `mqtt-storage`'s log commits at
`Durability::Immediate` (`crates/mqtt-storage/src/persistent_log.rs`); redb's macOS
backend implements non-eventual durability as `File::sync_data()` (its cheaper
`F_BARRIERFSYNC` path is the *eventual* one), and Rust's `sync_data` on macOS is
`fcntl(F_FULLFSYNC)` — a **true device flush**. Measured on this volume with a
90-second probe (`python3` + `fcntl`, so the three syscalls can be compared
directly):

| operation | median | ~rate |
|---|---|---|
| `fsync(2)` | 0.05 ms | ~20 000/s |
| `fcntl(F_BARRIERFSYNC)` | 0.26 ms | ~3 800/s |
| `fcntl(F_FULLFSYNC)` | 4.2 ms | ~215/s |

<details>
<summary>the probe, so this row is reproducible too</summary>

```sh
python3 - <<'PY'
import fcntl, os, time, statistics
codes = {"fsync": None, "F_BARRIERFSYNC": 85, "F_FULLFSYNC": 51}
def bench(name, n=300):
    fd = os.open(".fsync-probe.tmp", os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
    buf, ts = b"x" * 4096, []
    try:
        for _ in range(n):
            os.write(fd, buf)
            t0 = time.perf_counter()
            os.fsync(fd) if codes[name] is None else fcntl.fcntl(fd, codes[name])
            ts.append((time.perf_counter() - t0) * 1e3)
    finally:
        os.close(fd); os.unlink(".fsync-probe.tmp")
    ts.sort()
    return statistics.median(ts), ts[int(0.99 * len(ts))]
for name in codes:
    med, p99 = bench(name)
    print(f"{name:16s} median {med:.3f} ms  p99 {p99:.3f} ms  ~{1000/med:.0f}/s")
PY
```

</details>

**The store append** (one `PersistentLog::append` = one redb write transaction at
`Durability::Immediate`), which is what the barrier cost buys:

| measurement | median [min..max] over 2 runs × 3 reps |
|---|---|
| serial append p50 | 5.0 [4.0..5.0] ms |
| serial append rate | 179 / 203 [165..208] appends/s |
| 32-way concurrent append rate, one store | 178 / 197 [176..200] appends/s |
| 3 separate stores on one device, serial each | 227 / 202 [196..288] appends/s |

Two conclusions, both mechanical:

- **No group commit on this path.** 32-way concurrency does not beat serial: redb
  admits one write transaction at a time and each one pays a barrier. (ADR 0027's
  group commit is on the *replica writer*, which batches inbound `Replicate` frames;
  the owner-side session-log append is not batched.)
- **The ceiling is per volume, not per store.** Three stores aggregate to ~200–230/s,
  not ~540–600/s — the same number the raw barrier probe gives. On this host all three
  nodes share it.

**The arithmetic bounds it from above, and does not close.** One acked QoS 1 durable
publish needs at least two barrier-bearing appends on the critical path (the owner's own
local store plus one follower, for quorum 2 of 3). Dividing the measured volume budget by
that floor gives a **ceiling of ~110–120 acked msg/s for the whole cluster**
(~215–240 ÷ 2) — and the measurement is **24–28 msg/s** (the `qos1-durable-owner` and
`qos1-durable-relay` medians in Result 1), a factor of four *below* its own ceiling. So the
barrier budget is necessary to explain the number but not sufficient, and this document
does not claim otherwise.

What the two measurements jointly imply is the per-message barrier cost: 215–240 barriers/s
sustaining 24–28 acked msg/s is **roughly 8–10 barriers per acked message**, not 2. The
two-per-message floor counts only the critical path; the same volume is simultaneously
serving the other two nodes' replica-store appends, the lease store and the retained store,
and every one of those pays its own barrier. That is an *inference from two numbers*, not a decomposition:
nobody instrumented barriers per store here, and the honest way to settle it is
`fs_usage`/`dtrace` per store on a host that permits it, or the multi-host lane where each
node has its own budget. Recorded as the open question it is, rather than dressed up as a
closed derivation. The brokers are idle while they wait.

**Which is exactly why the multi-host lane is not a formality.** On separate hosts
each node has its own barrier budget, the barrier itself is a different cost, and the
quorum RPC becomes a real network round trip instead of a microsecond. The number
below the barrier changes, the number above it changes, and nothing about this
document predicts the result.

## Result 3 — head-of-line isolation under a degraded placement group

[ADR 0061](../adr/0061-off-loop-durable-appends.md) moved durable appends off the hub
loop after issue #242, and `docs/SIZING.md` summarises the resulting claim as: with one
placement group's followers degraded, "publishes to other groups' sessions, connects, and
subscribes are unaffected". This measures that instead of asserting it.

```sh
MQTTD_BENCH_PHASE_SECS=40 MQTTD_BENCH_ISO_PUBS=2 MQTTD_BENCH_ISO_WINDOW=1 \
MQTTD_BENCH_SLOW_MS=1500 \
cargo test --release -p mqttd --test durable_bench \
  degraded_group_does_not_delay_other_groups -- --ignored --nocapture
```

**Construction.** 5 nodes (the minimum that admits both classes under R=3: over 4 nodes
every group excludes exactly one node, so none can exclude *both* slowed nodes; over 3,
none can). Two nodes' inbound peer bus is delayed 1500 ms per chunk — degraded, not dead,
since SWIM is UDP and untouched, so nothing confirms a death and appends simply crawl
toward the 5 s RPC bound. Then, computed from the broker's **own** placement hash before
anything is slowed:

- **victim** ids sit in groups whose replica set is `{owner, slowed, slowed}` — quorum
  of 2 needs a slowed follower;
- **control** ids sit in groups whose replica set excludes both slowed nodes;
- both classes are owned by the **same** node, so they share one hub loop — the thing
  the claim is about;
- **every** role is class-pinned: the subscriber sessions, the *publishers' own* client
  ids, and the *CONNECT probers' own* client ids. This matters more than it sounds: an
  earlier version pinned only the sessions, and its control arm collapsed — because the
  control publishers' own attach was slow, their own ids having hashed into degraded
  groups. That measures "a client in a degraded group is slow", which nobody disputes.
- publishers are connected **before** the phase clock starts, so a slow CONNECT is never
  charged to throughput.

Three phases — baseline, degraded, healed — each 40 s, and the whole experiment was run
**4 times**. Per class: 2 subscriber sessions, 2 publishers at 1 publish in flight,
~180 CONNECT probes. The load is deliberately far below the host's durable ceiling: this
result is a *ratio*, and a saturated baseline has a multi-second tail of its own that
swamps the effect.

Two disclosures about the 5-node runs specifically. First, the readiness gate
**proceeded without full replica-group currency**: on a 5-node cluster formed
incrementally, `/statusz`'s `replica_groups` plateaus at ~75–85 % `current` within 15 s
and then does not move for minutes, because a node keeps *tracking* groups it held under
an earlier membership and no longer holds. Every node was ready with full membership, a
ready lease group and held groups, and each arm's own functional gates (a granted
durability-gated SUBACK, then an acked warm-up publish observed to arrive) passed — the
harness prints the shortfall rather than hiding it. Second, five brokers plus the driver
on 8 cores occasionally starve a node's health endpoint past its 2 s HTTP timeout for a
single poll; the gate tolerates one such poll rather than failing a run over it, and that
starvation is itself a fact about this host, not about the broker.

| measurement | baseline → degraded, across 4 runs | ratio |
|---|---|---|
| **control** publish p50 | 132→41, 136→53, 130→61, 111→52 ms | **0.31–0.47× (faster)** |
| **control** publish p95 | 261→92, 237→94, 237→117, 205→92 ms | **0.35–0.50× (faster)** |
| **control** publish throughput | 8.7→6.7, 7.2→4.1, 9.3→5.3, 10.8→4.4 msg/s | 0.41–0.77× |
| **victim** publish throughput | 8.6→0.0, 6.7→0.1, 9.2→0.1, 10.2→0.1 msg/s | **0.00–0.01×** |
| **victim** CONNECT p99 (own group degraded) | 24 s, 25 s, 29 s, 24 s under the fault | — |
| `hub_dispatch_seconds{command="publish"}` p99 | ≤ 0.2 ms in **all 12** phase measurements | **1.0×** |
| recovery when healed | **did not reproduce as written** — see the note below | — |

**What this establishes.** While one placement group's appends are stalled hard enough
to stop that group's publishes completely, publishes for sessions in healthy groups on
the same node keep flowing, and the hub loop's own publish dispatch never moves off
~0.2 ms. That is ADR 0061's mechanism, measured: the append is off the loop, so the loop
does not carry the stall. A pre-#242 build would show both arms degrading together.

Read the control class's *faster* p50/p95 for what it is: its **throughput fell to
0.41–0.77×** in the same runs, so there was less durable work competing for the disk
barrier and each surviving message went quicker. The property demonstrated is isolation of
**failure** — a degraded group cannot stop or stall a healthy one — not isolation of
**capacity**, which five processes sharing one volume could not show even if it held.

**The recovery row did not reproduce, and the row now says so.** An earlier draft of this
table asserted "control and victim both return to baseline in all 4 runs" with no numbers
behind it — the only such row in the document. Re-run at `PHASE_SECS=30`, the healed phase
gave control 7.9 msg/s against a 12.6 baseline (0.63×) and victim 8.1 against 13.3
(0.61×), with healed-control p99 at 5113 ms against a 299 ms baseline. Whether that is
genuine post-fault hysteresis (the relay heals, but redb, the replica catch-up and the
lease traffic all have work queued behind them) or simply this host's variance across a
90 s window, this benchmark cannot say — and a claim of "returns to baseline" is not
available from the data either way. Measuring recovery honestly needs a longer healed
phase than the harness currently runs.

**What this does *not* establish — and this is the honest half.** The control class's
**tail beyond p95 cannot be attributed on this host.** Every phase — baseline and healed
included — contains publish completions at 10–20 s, the same background stall as
[finding 2](#findings-this-benchmark-surfaced), and in 3 of the 4 runs at least one
*control* publisher hit a ≥18 s stall or a 30 s wedge during the degraded phase. With
baseline maxima already at 10–20 s, no fault attribution is possible above p95, and the
control-throughput dip (0.41–0.77×) is mostly those stalls eating a large share of a
40 s window rather than a systematic slowdown. So the part of SIZING's claim about
**connects and the tail** is **untested here**; the median/p95/mechanism part is
measured and holds. Re-run this on a host without the background stall — which is
another reason the multi-host lane matters.

**One thing the experiment did settle plainly:** a client whose **own** placement group
is degraded pays the RPC bound on CONNECT — 24–29 s at p99 in every run, versus
~0.3–0.4 s at baseline. That is the client's own group being slow, not head-of-line
blocking, and it is the right behaviour to expect; but "connects are unaffected" is only
true of clients whose own groups are healthy, and ADR 0061 §5's own text is the accurate
one (residual store awaits stay in the attach/control classes).

## Findings this benchmark surfaced

A macro-benchmark's first job is to find things, and this one found four. None is
fixed here (this lane owns the harness and the numbers, not the broker); each is
recorded with the evidence needed to reproduce it.

1. **Inbound QoS 2 hangs silently when the publisher's group is owned elsewhere.**
   A QoS 2 PUBLISH writes a dedup record in the **publisher's own** placement group.
   A publisher attached to a node that does not own that group logs
   `QoS2 dedup store write failed; withholding PUBREC (fail closed) …
   not the owning node for this group`, and then **nothing** reaches the client: no
   PUBREC, no DISCONNECT, no error — the connection just sits there. Failing closed is
   right; failing *silently* leaves a client hung forever. The harness now pins
   publisher client ids to the node they attach to so the QoS 2 arm is measurable at
   all, which is a workaround inside a benchmark and would be an outage outside one.
2. **A repeatable 5 s / 10 s durable-publish stall on a healthy idle cluster.**
   0–2 completions per 40 s rep land at exactly 1× or 2× the 5 s replication RPC
   bound, with no fault injected, no node down, `min_actual ≥ write_floor` throughout,
   and hub dispatch p99 ≤ 0.2 ms. In one earlier rep the same stall ended in a
   withheld ack: `durable_append_failures_total` moved by 1 and the publisher waited
   forever with no signal (the ADR 0061 §4 "withhold, do not refuse" path — correct,
   and invisible to the client). Worth an investigation issue: something periodic is
   letting a replication RPC reach its timeout.
3. **The durable append path has no group commit.** Measured above: 32-way
   concurrency does not beat serial appends. Every durable append pays its own device
   barrier, so per-node durable throughput is `1 / commit_time` regardless of session,
   lane or publisher concurrency. On a host where the barrier is expensive that is the
   whole capacity story. ADR 0027 already establishes the pattern on the replica
   writer; the owner-side append is the obvious place to apply it, and this is the
   first measurement that makes the case with a number.
4. **`/statusz`'s `replica_groups` never reaches `current == tracked` on a 5-node
   cluster.** Every node reports ready, full membership and a ready lease group within
   seconds, and `current` climbs to ~75–85 % of `tracked` within 15 s — then stops, and
   does not move for minutes. The shape says a node keeps *tracking* groups it held
   under an earlier membership and no longer holds, and nothing untracks them; the
   3-node case reaches `current == tracked` in ~5 s. If that reading is right the
   consequence is not a stalled cluster but a **permanently non-green operator signal**
   on any cluster that grew — the ADR 0043 P1 catch-up signal an operator is told to
   wait for. The harness had to demote it from a gate to a printed disclosure to run the
   5-node experiment at all, which is exactly the kind of accommodation worth an issue.

## The multi-host lane: documented and unrun

**There are no multi-host numbers in this repository.** This section is the
invocation, not a result. The driver is the same code that produced everything
above — with the three variables below set, nothing is spawned locally.

### 1. On each broker host

One node per host, one disk per node (that is the entire point). Substitute real
addresses; the peer/gossip addresses must be reachable **between hosts**, not
`127.0.0.1`:

```sh
# host 1 (the founder: no seeds)
MQTTD_NODE_ID=n0 \
MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 \
MQTTD_PEER_BIND=0.0.0.0:7000 MQTTD_PEER_ADVERTISE=10.0.0.1:7000 \
MQTTD_SWIM_BIND=0.0.0.0:7001 \
MQTTD_SWIM_KEY=<64 hex chars, the same on every host> \
MQTTD_DATA_DIR=/var/lib/mqttd \
MQTTD_HEALTH_BIND=0.0.0.0:9090 \
MQTTD_ALLOW_ANONYMOUS=1 RUST_LOG=warn \
  mqttd

# hosts 2..N: identical, plus the seed list
MQTTD_NODE_ID=n1 … MQTTD_SWIM_SEEDS=10.0.0.1:7001,10.0.0.3:7001 mqttd
```

Disclose, in the results, whatever differs from the single-host posture above —
especially TLS (these numbers include none) and `TOKIO_WORKER_THREADS` (leave it
unset to give each broker its host's cores, and say so).

### 2. Verify the cluster is measurable before measuring

The harness does this itself and refuses to start otherwise, but check it by hand
first — a run against a cluster that is still converging measures convergence:

```sh
curl -s 10.0.0.1:9090/readyz    # want: ready true, members = N, lease_group_ready true
curl -s 10.0.0.1:9090/statusz   # want: replica_groups current == tracked > 0
```

### 3. From a SEPARATE driver host

The driver must not share cores with a broker; that is one of the confounders this
lane exists to remove.

```sh
git clone <this repo> && cd fss-mqtt-broker
MQTTD_BENCH_BROKERS=10.0.0.1:1883,10.0.0.2:1883,10.0.0.3:1883 \
MQTTD_BENCH_HEALTH=10.0.0.1:9090,10.0.0.2:9090,10.0.0.3:9090 \
MQTTD_BENCH_NODE_IDS=n0,n1,n2 \
MQTTD_BENCH_PUBLISHERS=16 MQTTD_BENCH_WINDOW=8 MQTTD_BENCH_SUBS=16 \
MQTTD_BENCH_SECS=60 MQTTD_BENCH_WARMUP_SECS=10 MQTTD_BENCH_REPS=3 \
cargo test --release -p mqttd --test durable_bench durable_path_floor \
  -- --ignored --nocapture
```

The three lists are **parallel and in the same node order**; the node ids must be the
brokers' real `MQTTD_NODE_ID` values, because the harness computes placement from
them to pin sessions to node 0 and to separate the owner and relay arms. A mismatch
is a wrong measurement, not an error, so the harness prints the ids it used.

`multi_host_preflight` exercises the parsing, reachability and readiness gate on its
own, and prints this command when the variables are absent:

```sh
cargo test --release -p mqttd --test durable_bench multi_host_preflight -- --ignored --nocapture
```

### 4. What to watch, and what makes a run valid

Same rules as [above](#a-run-judges-itself) — the harness applies them and prints the
verdict. On real hosts, additionally:

- **the driver must not be the bottleneck**: the harness prints per-process CPU for
  *local* nodes only, so on a multi-host run watch the driver host's own CPU and
  scale `MQTTD_BENCH_PUBLISHERS` until throughput stops rising;
- **watch `mqttd_append_lane_jobs`** on every node: sustained growth means the
  followers cannot keep up and the run is measuring backlog, not throughput;
- **watch `mqttd_hub_dispatch_seconds{command="publish"}`**: a p99 above ~100 ms
  means something is back on the hub loop (ADR 0061's regression signal);
- **re-run the barrier probe on each broker host.** The single most useful number in
  this whole document is that host's `File::sync_data()` rate, and it is one command
  (`device_barrier_floor`). Publish it beside the throughput or the throughput cannot
  be interpreted.

### 5. The scaling curve, still deliberately absent

Even on multiple hosts, a 1/3/5-node curve is a separate exercise with its own
honesty rules (ADR 0048 §2 / 0048-T3): one small host **per node**, independent
disks, and a flat curve published as a finding rather than buried. This harness can
drive it (`MQTTD_BENCH_BROKERS` with 1, 3, then 5 endpoints), but the curve is not
this document's claim and is not made anywhere in this repository.

*Update 2026-08-20:* that separate exercise now has its rig and its record
template — `bench/scale/run.sh` (Hetzner, one host and one disk per node, per-host
barrier probes) and `docs/benchmarks/SCALE-CURVE.md`, which stays explicitly
unfilled until a real run. The harness grew the curve's workload shape as
`MQTTD_BENCH_SPREAD=1` (sessions spread across all owners instead of pinned to
node 0), and exercising it immediately surfaced
[#358](https://github.com/mbilling/fss-mqtt-broker/issues/358) — durable acks
stalling on groups owned by non-founder nodes — before any money was spent.

## Reproducing everything above

Every number on this page comes from one of these, on the host described above:

```sh
# Result 1 — latency at low load
MQTTD_BENCH_PUBLISHERS=1 MQTTD_BENCH_WINDOW=1 MQTTD_BENCH_SUBS=1 \
MQTTD_BENCH_REPS=3 MQTTD_BENCH_SECS=40 MQTTD_BENCH_WARMUP_SECS=5 \
cargo test --release -p mqttd --test durable_bench durable_path_floor -- --ignored --nocapture

# Result 2 — throughput under saturation
MQTTD_BENCH_PUBLISHERS=16 MQTTD_BENCH_WINDOW=8 MQTTD_BENCH_SUBS=16 \
MQTTD_BENCH_REPS=3 MQTTD_BENCH_SECS=30 MQTTD_BENCH_WARMUP_SECS=5 \
cargo test --release -p mqttd --test durable_bench durable_path_floor -- --ignored --nocapture

# Result 3 — head-of-line isolation (ADR 0061), run 4 times
MQTTD_BENCH_PHASE_SECS=40 MQTTD_BENCH_ISO_PUBS=2 MQTTD_BENCH_ISO_WINDOW=1 \
MQTTD_BENCH_SLOW_MS=1500 \
cargo test --release -p mqttd --test durable_bench \
  degraded_group_does_not_delay_other_groups -- --ignored --nocapture

# The ceiling underneath all of it
cargo test --release -p mqttd --test durable_bench device_barrier_floor -- --ignored --nocapture
cargo test --release -p mqttd --test durable_bench store_append_floor   -- --ignored --nocapture
```

All of these are `#[ignore]`d: they take minutes, and the per-PR test profile is
unoptimized, where the harness refuses to treat its own output as evidence.

## Related

- [`docs/benchmarks/BASELINE.md`](BASELINE.md) — the **micro**-benchmarks: per-operation
  CPU costs of the codec, durable plane and bridge, plus the per-PR regression floor.
  Those numbers are about the broker's CPU work; these are about the whole path.
- [`bench/README.md`](../../bench/README.md) — the **comparative** harness (Mosquitto,
  EMQX, NanoMQ, VerneMQ via `emqtt-bench`), which measures a single **non-durable**
  node. Its raw output lands in `bench/results/`, which is untracked scratch; published
  results live here.
- [`docs/SIZING.md`](../SIZING.md) — what the write rate means for a node's budget, and
  the clean-session escape hatch this document prices.
- [ADR 0048](../adr/0048-comparative-benchmarking.md) (benchmarking + publication rules),
  [ADR 0057](../adr/0057-durable-outbound-inflight.md) (ack-after-durable — why the
  publisher's RTT is the durable-commit latency),
  [ADR 0061](../adr/0061-off-loop-durable-appends.md) (the head-of-line property Result 3
  measures), [ADR 0027](../adr/0027-replica-group-commit.md) (group commit, and where it
  is not).
