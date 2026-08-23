# The scaling curve — throughput and p99 vs node count

**Verified against `v1.0.3` (2026-08-23).** Third published run of the ADR 0048
§2 curve: the same workload against fresh 1-, 3- and 5-node clusters of the
signed `v1.0.3` release, one dedicated-vCPU cloud host and one local NVMe disk
per broker, measured by `bench/scale/run.sh` and rendered by
`bench/scale/summarize-curve.py`. This run measures the issue #376 fan-out fix
(shared selection stopped cloning the group to pick one member) and carries
two disclosed findings per the standing rule that **a flat curve is a finding
to fix, not a number to bury**: the durable rows' disk-draw sensitivity (whose
mechanism is now fully diagnosed — ADR 0074 removes it next release), and the
fifth 5-node formation split, this time diagnosed live (issue #383).

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
- **Driver fleet per size:** sizes 1 and 3 ran with **three** drivers (the
  issue #376 rerun's configuration, kept), size 5 with two — the account's
  ~40-dedicated-vCPU ceiling refuses 5×CCX23 + 3×CCX33. Every lane B rung's
  offered-shortfall flag is reproduced below rather than hidden.

## Host, build, configuration

- **Brokers:** Hetzner Cloud CCX23 (4 dedicated vCPU, AMD EPYC-Milan, 16 GB,
  local NVMe), one broker per host, spread placement group, `fsn1`, Ubuntu
  24.04, kernel 6.8.0-137. Data dir on the host's local NVMe — never network
  storage.
- **Broker build:** the released, cosign-signed, byte-reproducible
  `mqttd-1.0.3-x86_64-unknown-linux-musl`, checksum-verified at install.
  Config: the shipped `deploy/systemd/mqttd.service` + the disclosed drop-in
  and env template in `bench/scale/` (health on 0.0.0.0 for private-net
  scraping, `MQTTD_MAX_CONNECTIONS=60000`, plaintext listener on the private
  IP, durable plane ON for lane A / OFF for lanes B–C,
  `TOKIO_WORKER_THREADS` unset, `MQTTD_ALLOW_RELAXED_PUBLISH=1` for the ADR
  0072 tier lanes — disclosed in the template; default lanes publish v3.1.1
  and are unaffected). Cluster PKI from `deploy/systemd/gen-certs.sh`; SWIM
  signed. New this release and active during measurement: the rig's
  **private-net truth gate** (issue #368) verified full pairwise reachability
  before every size.
- **Drivers:** CCX33 (8 dedicated vCPU) on the same private network;
  emqtt-bench 0.6.3 (docker, host network) for lanes B–C; `durable_bench`
  built from the release commit for lane A.
- **Topology per size:** fresh cluster (apply → measure → destroy) — a grown
  cluster is a known-degraded configuration. Founder-first bring-up, founder
  armed to the majority floor before any load. Date: 2026-08-22→23.

## Per-host durability barrier floors

Measured on every broker host before any lane (`device_barrier_floor`, scratch
on the data-dir filesystem) — the per-volume ceiling the durable rows must be
read against, because **this run drew badly**:

| nodes | per-broker floor (single-writer barriers/s) |
|---|---|
| 1 | 2,257 |
| 3 | 2,575 / 1,467 / **706** |
| 5 | 2,179 / **695** / 2,224 / **701** / 2,254 |

The 3-node and 5-node draws each include ~700-barriers/s volumes (the
multi-tenant NVMe outlier class; the v1.0.2 3-node draw was 2,168–2,383 on all
three). A quorum write waits on the slowest member, which is why the floors
are printed beside every durable number rather than averaged away.

## Curve 1 — durable QoS 1, closed loop (spread ownership)

48 closed-loop publishers × window 8, 48 durable subscribers, 256 B payloads,
sessions spread across every node's HRW-owned groups, 60 s windows, median
[min..max] over 3 reps. Acks are given only after the message is fsync'd and
quorum-replicated (ADR 0057).

| nodes | acked msg/s (saturating) | exact p99 (saturating) | exact p99 (uncontended: window 1, one publisher per node) | verdict |
|---|---|---|---|---|
| 1 | 3,048 [3,042..3,406] | 140 ms [126..145] | 1.13 ms [1.13..1.15] | valid |
| 3 | 867 [866..934] | 469 ms [436..470] | 6.83 ms [6.81..6.93] | valid — **read against the 706/s disk below** |
| 5 | — | — | — | **not measurable — issue #383** (below) |

QoS 2, same shape: 1,772 [1,766..1,819] msg/s at 1 node, 897 [870..897] at 3.
Clean sessions (nothing durable to write): 42k [31k..61k] at 1 node,
**106k [102k..106k] at 3** — the durable price tag on this hardware's draw is
~14–120×, stated plainly.

**Reading the rows against the disks — the diagnosis this run completed:**
the 1-node row (2,257-barriers/s disk) runs at **1.35× its disk's barrier
rate**; the 3-node row (slowest disk 706/s) at **1.23×**. The v1.0.2 rows sat
at 1.35× and 0.81× of *their* draws' barriers. Durable throughput tracks the
slowest disk's barrier **rate**, not its capacity — while the ADR 0071
group-commit writer measured only 2.29 ops per fsync batch at ~3,700
batches/s, capacity idling. That constant ratio across four draws is the
signature of one serialized barrier-wait per message on the hub loop: the
subscriber-ack **truncate** (ADR 0061's residual). [ADR 0074](../adr/0074-detached-ack-truncate.md)
(accepted, next release) detaches it — measured locally at **22.8×** durable
throughput on barrier-expensive storage — so the next curve's durable rows
are predicted to decouple from the disk draw entirely. If they do not, ADR
0074 is wrong and says so.

**v1.0.2 → v1.0.3 comparison, honestly:** 1-node durable +9% (2,791 → 3,048
on a comparable disk). The 3-node drop (1,753 → 867) is **the disk draw, not
a regression** — no durable-path change shipped in v1.0.3, and the
barrier-ratio analysis above accounts for the row to within the draw.

## Durability tiers (ADR 0072) — same workload, publisher-selected ack meaning

Saturating plus an uncontended (window 1) variant per tier — at closed-loop
saturation the lanes flow-control every tier to the pipeline's rate, so the
tier's real face is the uncontended ack RTT. This run completes the table the
v1.0.2 curve started (the 1-node uncontended cells ran for the first time):

| nodes | tier | acked msg/s (sat) | exact p99 (sat) | exact p99 (uncontended) |
|---|---|---|---|---|
| 1 | `quorum` | 3,048 [3,042..3,406] | 140 ms | 1.13 ms [1.13..1.15] |
| 1 | `local` | 2,842 [2,721..2,890] | 156 ms | 1.17 ms [1.17..1.20] |
| 1 | `relaxed` | 2,489 [2,489..2,492] | 168 ms | 1.17 ms [1.15..1.18] |
| 3 | `quorum` | 867 [866..934] | 469 ms | 6.83 ms [6.81..6.93] |
| 3 | `local` | 863 [863..864] | 485 ms | 6.78 ms [6.73..6.94] |
| 3 | `relaxed` | 812 [809..818] | 525 ms | 9.70 ms [9.69..9.78] |

The v1.0.2 finding **holds on a second run and a worse disk draw: the tiers
converge on datacenter NVMe.** Weakening the ack's meaning buys no headline
number here — quorum is already the cheapest honest thing this hardware can
say. The tiers' value remains confined to barrier-expensive/high-RTT
deployments (the macOS F_FULLFSYNC contrast: 300×), and ADR 0074 will narrow
even that.

## Curve 2 — non-durable `$share` fan-out (the ADR 0015 mechanism)

600 publishers → `bench/%i` (QoS 1, 256 B, window 100), 300 subscribers in one
shared group `$share/g1/bench/#`, populations spread across all drivers and
brokers; the same offered ladder (20k…300k msg/s) at every size, 60 s per
rung. Broker restarted clean per size, durable plane off.

**This is the #376 fix's release, and the lane transformed.** Subscriber-side
delivered rates (the summarizer's honest lane, emqtt-bench-counted):

| offered | 1 node | 3 nodes | 5 nodes (2 drivers) |
|---|---|---|---|
| 20k | 19.7k | 18.9k, p99 ≤ 5ms | 15.0k |
| 50k | 22.8k ← plateau | 47.1k, p99 ≤ 10ms | 37.1k |
| 100k | 22.6k | 67.9k | 61.0k |
| 200–300k | ~22.4k | **~69k ← plateau** | **~70k ← plateau** |

Against v1.0.2's published plateaus this is **3.3× at 1 node (6.9k → 22.8k),
3.5× at 3 nodes (19.6k → 69k), 2.2× at 5 nodes (31.9k → 70k)** — the measured
consequence of shared selection no longer materializing every group member per
publish (~64% of hub publish dispatch, now gone).

Two disclosures keep the table honest:

- **Broker-received vs subscriber-delivered diverge under deliberate
  overload.** By the authoritative broker counter the 3-node cluster
  *accepted* ~100k msg/s at the 100k+ rungs while delivering ~69k — the
  bounded per-subscriber backlog dropping oldest under overload (ADR 0012 /
  issue #241), which is the designed behavior when offered exceeds delivery
  capacity. The delivered number is the one tabled.
- **The 5-node column ran with two drivers** (quota ceiling) and its rungs
  are flagged driver-limited; 5n ≈ 3n at the plateau is therefore a floor,
  not a scaling verdict. The three-driver 5-node point — and the real 5-node
  knee — wait on the quota raise.

## Connections at 50,000

| nodes | connected | broker RSS growth | KiB per idle connection |
|---|---|---|---|
| 1 | 49,998 | 945 MiB | 19.4 |
| 3 | 49,998 | 960 MiB | 19.7 |
| 5 | 50,000 | 944 MiB | 19.3 |

Flat per-connection memory in cluster size, third run in a row (~15 KiB/conn
claimed in `docs/SIZING.md`; 19.3–19.7 measured with observability running).

## Losing dimensions, stated first

- **The durable rows are disk-draw-hostage this run** (706-barriers/s outlier
  volumes at both multi-node sizes) — mechanism diagnosed, fix accepted (ADR
  0074), next curve is the falsifier.
- **The 5-node durable point is still not measurable — fifth reproduction,
  first live diagnosis** (issue #383, succeeding #368): the network was clean
  the whole time (the new mesh gate verified pairwise reachability; sized
  pings showed 0% loss mid-incident); the v1.0.3 WARN instrumentation showed
  cluster-wide SUSPECT/RECOVERED flapping during the synchronized load
  window, an eviction that then STUCK at idle, and `cert-miss` gossip-auth
  drops as the stickiness residue — a pruned member's fingerprint-sealed
  datagrams orphaned because nothing re-sends its certificate. A logical
  one-way partition over a perfect network; proposed fix (sender-side
  re-prime) tracked in #383. Non-durable lanes at 5 nodes measured normally.
- **Curve 2's 5-node column is a two-driver floor** (quota), and its
  delivered numbers sit under deliberate-overload backlog shedding, disclosed
  above.
- Lane B p99s are bucket bounds; cross-driver clock skew bounds the finest
  readable bucket.
- Multi-tenant NVMe variance (3.7× across nominally identical hosts this run)
  is real and measured per run; two runs of this curve may legitimately
  differ — this run IS the cautionary example.

## A run judges itself

Enforced mechanically: per-host barrier probes gate Curve 1 (a size without
probes renders UNINTERPRETABLE); `durable_bench`'s verdicts are carried
verbatim; broker counter deltas cross-check driver totals with every mismatch
flagged; driver-limited rungs are excluded from knee detection; preflight
captures every node's `/readyz` + `/statusz` — which, with the v1.0.3
isolation instrumentation, is how issue #383 was diagnosed live instead of
averaged over.

## Reproducing everything above

```sh
cd bench/scale
export HCLOUD_TOKEN=...   # Read & Write token, dedicated Hetzner project
./run.sh smoke            # ~20 min, <€0.50 — proves the rig end to end
MQTTD_VERSION=1.0.3 OBSERVE=1 DRIVER_COUNT=3 ./run.sh full 1 3
MQTTD_VERSION=1.0.3 OBSERVE=1 DRIVER_COUNT=2 ./run.sh full 5
python3 summarize-curve.py .runs/<stamp>/results
```

The rig: `bench/scale/run.sh` (orchestration, including the private-net truth
gate), `bench/scale/terraform/` (hosts), `bench/scale/bootstrap-cluster.sh`
(secrets + founder-first bring-up), `bench/scale/run-curve.sh` (lanes,
including per-tier saturating + uncontended variants and the `LANES` filter),
`bench/scale/observe.sh` (live Grafana) — see `bench/scale/README.md`.

## Related

- `docs/benchmarks/DURABLE-PATH.md` — the single-host durable floor and method.
- `docs/adr/0071-owner-side-group-commit.md`, `docs/adr/0072-per-message-durability-selection.md`,
  `docs/adr/0073-scale-out-durable-ownership.md` (awaiting its 7/10-node
  measurement behind the quota raise), `docs/adr/0074-detached-ack-truncate.md`
  (the next durable unlock, measured 22.8× locally).
- `docs/adr/0048-comparative-benchmarking.md` §2 — the mandate and honesty
  rules; `docs/delivery/0048-comparative-benchmarking.md` T3/T4 track this work.
- Issues: #358 (fixed v1.0.1), #368 (observability fixed v1.0.3), #376 (fixed
  v1.0.3 — this run measured it), #383 (open — the 5-node split, now
  diagnosed).
