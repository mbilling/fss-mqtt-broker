# The scaling curve — throughput and p99 vs node count

**Verified against `v1.0.5` (2026-08-24).** Fifth published run of the ADR 0048
§2 curve: the same workload against fresh 1-, 3- and 5-node clusters of the
signed `v1.0.5` release, one dedicated-vCPU cloud host and one local NVMe disk
per broker, measured by `bench/scale/run.sh` and rendered by
`bench/scale/summarize-curve.py`. Two questions this run answers: **does the
scale-out shape survive a second release and a second disk draw** (yes —
5 nodes beats 1 node by 1.6× on the durable path, again), and **does the
issue #396 SWIM fix make 5-node formation routine** (yes — first-attempt
formation, against v1.0.4's seven attempts). One new finding, disclosed per
the standing rule that **a flat curve is a finding to fix, not a number to
bury**: the first honest measurement of the `relaxed` tier found it broken
(issue #399 — acked-then-dropped under saturation; its rows are absent below).

## Read this first

- **What this is:** mqttd measured against itself at three cluster sizes. No
  competitor appears here — cross-broker comparison is `docs/COMPARISON.md`'s
  job under ADR 0048 §3/§4.
- **Two curves, deliberately** ([ADR 0049](../adr/0049-voter-eligible-durable-ownership.md)):
  the durable QoS 1 curve is fsync- and ownership-bound; the non-durable
  `$share` fan-out curve is routing/CPU-bound. Neither substitutes for the
  other, and the idle-connection point is a third, memory-bound axis.
- **Latency honesty differs by lane.** Lane A percentiles are exact, computed
  from per-message ack RTTs in `crates/mqttd/tests/durable_bench.rs`. Lane B
  percentiles are emqtt-bench histogram **bucket upper bounds** ("p99 ≤ X ms"),
  merged across drivers — coarse, but incapable of flattering.
- **Driver fleet per size:** sizes 1 and 3 ran with **three** drivers, size 5
  with two — the account's ~40-dedicated-vCPU ceiling refuses 5×CCX23 +
  3×CCX33. A quota raise to ~100 vCPU is requested; until then every lane B
  driver-limited flag is reproduced below rather than hidden.
- **Absolute durable numbers move with the disk draw** (multi-tenant NVMe):
  this run drew slower volumes than v1.0.4's (floors 1,941–2,395 vs
  2,051–2,521 barriers/s, and ~2× the append latency), so its absolute rates
  sit below v1.0.4's while the *shape* and the barrier-rate decoupling hold.
  Read the ratios, not just the cells.
- **Provenance:** size 1 is run `20260823T190100Z`; sizes 3 and 5 are the
  same-night continuation `20260823T205824Z` after a rig fix (the run died
  between sizes when a cloud-init retry poisoned a systemd start limit —
  mechanism and fix in PR #400). Same signed build, host class, region, rig.

## Host, build, configuration

- **Brokers:** Hetzner Cloud CCX23 (4 dedicated vCPU, AMD EPYC-Milan, 16 GB,
  local NVMe), one broker per host, spread placement group, `fsn1`, Ubuntu
  24.04, kernel 6.8.0-137. Data dir on the host's local NVMe — never network
  storage.
- **Broker build:** the released, cosign-signed, byte-reproducible
  `mqttd-1.0.5-x86_64-unknown-linux-musl`, checksum-verified at install.
  Config: the shipped `deploy/systemd/mqttd.service` + the disclosed drop-in
  and env template in `bench/scale/` (health on 0.0.0.0 for private-net
  scraping, `MQTTD_MAX_CONNECTIONS=60000`, plaintext listener on the private
  IP, durable plane ON for lane A / OFF for lanes B–C,
  `TOKIO_WORKER_THREADS` unset, `MQTTD_ALLOW_RELAXED_PUBLISH=1` for the ADR
  0072 tier lanes — disclosed in the template; default lanes publish v3.1.1
  and are unaffected). Cluster PKI from `deploy/systemd/gen-certs.sh`; SWIM
  signed, bound to the routable private IP.
- **Drivers:** CCX33 (8 dedicated vCPU) on the same private network;
  emqtt-bench 0.6.3 (docker, host network) for lanes B–C; `durable_bench`
  built from the release commit for lane A.
- **Topology per size:** fresh cluster (apply → measure → destroy). Founder-
  first bring-up, founder armed to the majority floor before any load; the rig
  proves the private-net full mesh before the first broker starts and gates
  lane A on full membership. Date: 2026-08-23→24.

## Per-host durability barrier floors

Measured on every broker host before any lane (`device_barrier_floor`, scratch
on the data-dir filesystem):

| nodes | per-broker floor (single-writer barriers/s) |
|---|---|
| 1 | 2,162 |
| 3 | 2,177 / 1,954 / 2,096 |
| 5 | 2,395 / 2,031 / 1,941 / 2,261 / 2,109 |

A uniformly slower draw than v1.0.4's, with ~2× its append latency (p99
12.8 ms vs 6.4 ms at size 1) — the honest denominator for every durable row.

## Curve 1 — durable QoS 1, closed loop (spread ownership)

48 closed-loop publishers × window 8, 48 durable subscribers, 256 B payloads,
sessions spread across every node's HRW-owned groups, 60 s windows, median
[min..max] over 3 reps. Acks are given only after the message is fsync'd and
quorum-replicated (ADR 0057).

| nodes | acked msg/s (saturating) | exact p99 (saturating) | exact p99 (uncontended: window 1, one publisher per node) | verdict |
|---|---|---|---|---|
| 1 | 8,503 [8,134..8,636] | 90 ms [88..93] | 1.01 ms [1.00..1.02] | valid |
| 3 | 8,647 [8,352..8,725] | 82 ms [81..84] | 1.81 ms [1.78..1.86] | valid |
| 5 | **13,893 [13,799..14,214]** | 56 ms [55..77] | 1.69 ms [1.68..1.76] | valid — **first-attempt formation** |

QoS 2, same shape: 2,970 [2,965..3,023] msg/s at 1 node, 2,124 [2,111..2,248]
at 3, 3,009 [3,006..3,075] at 5. Clean sessions (nothing durable to write):
32.1k [27.4k..38.7k] at 1 node, 89.4k [79.6k..93.9k] at 3, 109.6k
[109.1k..110.0k] at 5.

**The v1.0.4 findings hold on a second release and a second draw.** The rows
sit at **3.9× / 4.4× / 7.2×** of the slowest member's barrier rate (the
pre-0074 pinning was 0.8–1.35× across four draws) — durable throughput stays
decoupled from the disk. The shape repeats: 3 nodes ≈ 1 node (the quorum tax
fully absorbed — this draw actually reads +1.7%), and **5 nodes = 1.63× a
single node** (v1.0.4: 1.38×) — ownership spread across five owners beats the
quorum tax with room to spare, twice in a row now. Saturating p99 *improves*
with size (90 → 82 → 56 ms): more owners means shallower per-owner queues at
the same offered load.

**v1.0.4 → v1.0.5, honestly:** the absolute cells read lower (8.5k vs 11.8k
at 1 node) on a draw whose volumes are ~10% slower on barrier rate and ~2× on
append latency; the barrier-rate multiples above are the like-for-like
comparison, and they match. No durable-path change shipped in v1.0.5 (its
changes are SWIM addressing and bench hygiene), so the draw is the whole
difference — the multi-tenant-NVMe caveat this doc has carried since v1.0.3.

**Formation, the v1.0.5 proof:** v1.0.4's 5-node point took seven formations
across four paid launches to land once, by luck (issues #393/#396). This
run's 5-node cluster formed **on the first attempt** with the #396 fix aboard
(unroutable addresses can neither poison nor survive in gossip) — the
membership gate passed without a re-form, and the same held at size 3.

## Durability tiers (ADR 0072) — same workload, publisher-selected ack meaning

| nodes | tier | acked msg/s (sat) | exact p99 (sat) | exact p99 (uncontended) |
|---|---|---|---|---|
| 1 | `quorum` | 8,503 [8,134..8,636] | 90 ms | 1.01 ms [1.00..1.02] |
| 1 | `local` | 8,734 [8,716..8,773] | 82 ms | 1.03 ms [1.02..1.06] |
| 3 | `quorum` | 8,647 [8,352..8,725] | 82 ms | 1.81 ms [1.78..1.86] |
| 3 | `local` | 8,230 [8,208..8,325] | 83 ms | 1.83 ms [1.82..1.86] |
| 5 | `quorum` | 13,893 [13,799..14,214] | 56 ms | 1.69 ms [1.68..1.76] |
| 5 | `local` | 13,584 [13,055..13,681] | 77 ms | 1.72 ms [1.68..1.74] |

`quorum` and `local` converge for the fourth run in a row — on datacenter
NVMe, weakening the ack to single-copy buys nothing.

**The `relaxed` rows are absent because the tier is broken, and this run is
the first to measure it (issue #399).** With the #394 bench defect fixed, the
relaxed lane ran honestly for the first time since ADR 0074 shipped: a
relaxed pending is acked `Accepted` on submit even when the append lane
refused the job, and with the ack released early the publisher has no flow
control at all — it free-runs into the bounded lanes and the completions
collapse (measured 0 / 215 / 3,762 msg/s across reps at 1 node; 0 / 0 / 0 at
3 and 5 nodes; deliveries flowing throughout). The hole has existed since ADR
0072 but was masked first by the pre-0074 hub-loop throttle (v1.0.3 measured
2,489 msg/s clean because *everything* was slow), then by #394 hiding the
lanes in v1.0.4. Fix directions are in the issue; the rows return when it
closes.

## Curve 2 — non-durable `$share` fan-out (the ADR 0015 mechanism)

600 publishers → `bench/%i` (QoS 1, 256 B, window 100), 300 subscribers in one
shared group `$share/g1/bench/#`, populations spread across all drivers and
brokers; the same offered ladder (20k…300k msg/s) at every size, 60 s per
rung. Broker restarted clean per size, durable plane off.

Subscriber-side delivered rates (the summarizer's honest lane,
emqtt-bench-counted):

| offered | 1 node | 3 nodes | 5 nodes (2 drivers) |
|---|---|---|---|
| 20k | 18.1k | 18.7k, p99 ≤ 5ms | 19.1k, p99 ≤ 5ms |
| 50k | 18.2k ← plateau | 46.1k, p99 ≤ 25ms | 47.0k, p99 ≤ 5ms |
| 100k | 18.1k | 53.1k | 79.3k |
| 200–300k | ~18.6k | **~53.9k ← plateau** | **~81.4k ← plateau** |

Every rung above 50k is driver-limited (flagged per cell), so these are
floors. Two observations worth carrying anyway:

- **The 5-node floor jumped ~50% against v1.0.4's identical two-driver
  configuration** (~81k vs ~54k delivered). No fan-out code changed in
  v1.0.5; the suspect is the #396 fix — v1.0.4's 5-node cluster ran with
  relay-poisoned SWIM records even when formation succeeded, and v1.0.5's
  gossip is clean. A floor-vs-floor comparison proves nothing alone; the
  three-driver rerun after the quota raise is the test.
- **Broker-received exceeds driver-sent at every rung** — closed-loop QoS 1
  retransmission under deliberate overload; the subscriber-delivered number
  is the one tabled.

## Connections at 50,000

| nodes | connected | broker RSS growth | KiB per idle connection |
|---|---|---|---|
| 1 | 49,998 | 945 MiB | 19.4 |
| 3 | 49,998 | 960 MiB | 19.7 |
| 5 | 50,000 | 944 MiB | 19.3 |

Flat per-connection memory in cluster size, fifth run in a row (~15 KiB/conn
claimed in `docs/SIZING.md`; 19.3–19.7 measured with observability running).

## Losing dimensions, stated first

- **The `relaxed` tier is broken and unmeasured** — issue #399, first honest
  measurement, full mechanism in the tier section. The other two tiers are
  unaffected (their flow control is the ack-wait itself).
- **The run died once between sizes** — a cloud-init clean+reboot retry left
  the enabled mqttd unit crash-looping against a not-yet-pushed config until
  systemd's start limit poisoned it, and bootstrap's own restart was then
  refused. Rig-fixed the same night (PR #400: config-less boots are inert
  via `ConditionPathExists`; bootstrap runs `reset-failed` before its
  restart), and sizes 3/5 completed on the relaunch.
- **Curve 2's 5-node column is a two-driver floor** (quota), and both its
  cells and the v1.0.4 comparison above are floor-vs-floor.
- Lane B p99s are bucket bounds; cross-driver clock skew bounds the finest
  readable bucket.
- Multi-tenant NVMe variance is real and measured per run — this run's
  uniformly slower draw moved every absolute durable cell down ~25–30% while
  the barrier-rate multiples held; the doc's rule stands: read durable rows
  against their printed floors.

## A run judges itself

Enforced mechanically: per-host barrier probes gate Curve 1 (a size without
probes renders UNINTERPRETABLE); `durable_bench`'s verdicts are carried
verbatim; broker counter deltas cross-check driver totals with every mismatch
flagged; driver-limited rungs are excluded from knee detection; preflight
captures every node's `/readyz` + `/statusz`; the rig proves the private-net
full mesh before the first broker starts, gates lane A on full membership,
and collects every host's journal on failure — which is how both of this
campaign's rig defects (#400's start-limit poison, and v1.0.4's #396) went
from mystery to mechanism within hours.

## Reproducing everything above

```sh
cd bench/scale
export HCLOUD_TOKEN=...   # Read & Write token, dedicated Hetzner project
./run.sh smoke            # ~20 min, <€0.50 — proves the rig end to end
MQTTD_VERSION=1.0.5 OBSERVE=1 DRIVER_COUNT=3 ./run.sh full 1 3
MQTTD_VERSION=1.0.5 OBSERVE=1 DRIVER_COUNT=2 ./run.sh full 5
# a single lane can be rerun in isolation, e.g. the durable lane only:
LANES=A MQTTD_VERSION=1.0.5 DRIVER_COUNT=2 ./run.sh full 5
python3 summarize-curve.py .runs/<stamp>/results
```

The rig: `bench/scale/run.sh` (orchestration, including the private-net truth
gate), `bench/scale/terraform/` (hosts), `bench/scale/bootstrap-cluster.sh`
(secrets + founder-first bring-up + the pre-start full-mesh gate),
`bench/scale/run-curve.sh` (lanes, including per-tier saturating + uncontended
variants, the `LANES` filter and the lane A membership gate),
`bench/scale/observe.sh` (live Grafana) — see `bench/scale/README.md`.

## Related

- `docs/benchmarks/DURABLE-PATH.md` — the single-host durable floor and method.
- `docs/adr/0071-owner-side-group-commit.md`, `docs/adr/0072-per-message-durability-selection.md`
  (its `relaxed` tier is the subject of issue #399),
  `docs/adr/0073-scale-out-durable-ownership.md` (awaiting its 7/10-node
  measurement behind the quota raise), `docs/adr/0074-detached-ack-truncate.md`
  (hardware-verified two releases running: durable rows at 3.9–7.2× the
  slowest disk's barrier rate vs the 0.8–1.35× pinning it removed).
- `docs/adr/0048-comparative-benchmarking.md` §2 — the mandate and honesty
  rules; `docs/delivery/0048-comparative-benchmarking.md` T3/T4 track this work.
- Issues: #383 (fixed v1.0.4), #390 / #393 / #394 / #396 (fixed v1.0.5 —
  this run's first-attempt 5-node formation is #396's proof), #399 (open —
  the relaxed tier, found by this run), #400 (rig, fixed mid-campaign).
