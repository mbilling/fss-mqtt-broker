# The scaling curve — throughput and p99 vs node count

**Verified against `v1.0.4` (2026-08-23).** Fourth published run of the ADR 0048
§2 curve: the same workload against fresh 1-, 3- and 5-node clusters of the
signed `v1.0.4` release, one dedicated-vCPU cloud host and one local NVMe disk
per broker, measured by `bench/scale/run.sh` and rendered by
`bench/scale/summarize-curve.py`. This run existed to answer one question —
**was ADR 0074 right that the durable rows were pinned to the disk's barrier
rate by a single detached-able wait?** It was: the durable curve moved 3.9× at
1 node, 12× at 3 nodes, and produced the **first successful 5-node durable
measurement in the project's history** — 16,378 msg/s, the first size where
scale-OUT beats one node on the durable path. Getting that 5-node number took
seven cluster formations and a live packet capture, which found a SWIM
address-dissemination defect (issue #396) that had been manufacturing every
"5-node split" since v1.0.2 — disclosed in full below, per the standing rule
that **a flat curve is a finding to fix, not a number to bury**.

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
  3×CCX33. Every lane B rung's driver-limited flag is reproduced below rather
  than hidden.
- **Provenance:** sizes 1/3 and the 5-node lanes B–C are run
  `20260823T083536Z`; the 5-node lane A is the same-day `LANES=A` rerun
  (`20260823T143528Z`) after the issue #396 diagnosis — same signed build,
  host class, region, and rig, fresh hardware.

## Host, build, configuration

- **Brokers:** Hetzner Cloud CCX23 (4 dedicated vCPU, AMD EPYC-Milan, 16 GB,
  local NVMe), one broker per host, spread placement group, `fsn1`, Ubuntu
  24.04, kernel 6.8.0-137. Data dir on the host's local NVMe — never network
  storage.
- **Broker build:** the released, cosign-signed, byte-reproducible
  `mqttd-1.0.4-x86_64-unknown-linux-musl`, checksum-verified at install.
  Config: the shipped `deploy/systemd/mqttd.service` + the disclosed drop-in
  and env template in `bench/scale/` (health on 0.0.0.0 for private-net
  scraping, `MQTTD_MAX_CONNECTIONS=60000`, plaintext listener on the private
  IP, durable plane ON for lane A / OFF for lanes B–C,
  `TOKIO_WORKER_THREADS` unset, `MQTTD_ALLOW_RELAXED_PUBLISH=1` for the ADR
  0072 tier lanes — disclosed in the template; default lanes publish v3.1.1
  and are unaffected). Cluster PKI from `deploy/systemd/gen-certs.sh`; SWIM
  signed. Changed mid-campaign and disclosed: `MQTTD_SWIM_BIND` now binds the
  **private IP, not 0.0.0.0** — the bind is what SWIM gossips to third
  parties, and the unroutable default was the mechanism behind every prior
  5-node formation split (issue #396).
- **Drivers:** CCX33 (8 dedicated vCPU) on the same private network;
  emqtt-bench 0.6.3 (docker, host network) for lanes B–C; `durable_bench`
  built from the release commit for lane A.
- **Topology per size:** fresh cluster (apply → measure → destroy) — a grown
  cluster is a known-degraded configuration. Founder-first bring-up, founder
  armed to the majority floor before any load; since this campaign the rig
  also proves the private-net full mesh before the first broker starts and
  gates lane A on full membership (both in `bench/scale/`, PR #395).
  Date: 2026-08-23.

## Per-host durability barrier floors

Measured on every broker host before any lane (`device_barrier_floor`, scratch
on the data-dir filesystem) — the per-volume ceiling durable rows used to be
read against:

| nodes | per-broker floor (single-writer barriers/s) |
|---|---|
| 1 | 2,382 |
| 3 | 2,416 / 2,373 / 2,195 |
| 5 | 2,521 / 2,143 / 2,263 / 2,222 / 2,051 |

A clean draw this time (no ~700-barriers/s outlier volumes; v1.0.3 drew two).
The floors matter differently now — see the falsifier verdict below.

## Curve 1 — durable QoS 1, closed loop (spread ownership)

48 closed-loop publishers × window 8, 48 durable subscribers, 256 B payloads,
sessions spread across every node's HRW-owned groups, 60 s windows, median
[min..max] over 3 reps. Acks are given only after the message is fsync'd and
quorum-replicated (ADR 0057).

| nodes | acked msg/s (saturating) | exact p99 (saturating) | exact p99 (uncontended: window 1, one publisher per node) | verdict |
|---|---|---|---|---|
| 1 | 11,833 [10,957..12,005] | 65 ms [64..95] | 0.93 ms [0.92..0.95] | valid |
| 3 | 10,393 [10,325..10,398] | 84 ms [82..86] | 1.79 ms [1.76..1.79] | valid |
| 5 | **16,378 [16,333..16,679]** | 53 ms [43..64] | 1.59 ms [1.58..1.63] | valid — **first 5-node durable measurement ever** |

QoS 2, same shape: 4,685 [4,681..4,716] msg/s at 1 node, 2,425 [2,415..2,628]
at 3, 3,309 [3,309..3,448] at 5. Clean sessions (nothing durable to write):
43.6k [35.0k..57.4k] at 1 node, 99.1k [91.1k..102.1k] at 3, 102.5k
[102.4k..103.1k] at 5 — the durable price tag is now ~4–9×, down from
~14–120× last run.

**The ADR 0074 falsifier — verdict: PASSED.** The v1.0.2/v1.0.3 rows sat at a
near-constant 0.8–1.35× of their slowest disk's barrier *rate* across four
independent draws — the signature of one serialized barrier-wait per message
(the subscriber-ack truncate) on the hub loop. ADR 0074 predicted that
detaching it would decouple durable throughput from the disk draw entirely,
and staked itself on this run. The rows now sit at **5.0×** (1 node), **4.7×**
(3 nodes) and **8.0×** (5 nodes) of the slowest member's barrier rate — the
pinning is gone, and the group-commit writer (ADR 0071) finally runs at real
batch depth instead of the 2.29 ops/fsync measured last release.

**The curve's new shape — 11.8k → 10.4k → 16.4k — is the honest geometry of
quorum + spread ownership.** Three nodes is the worst durable size: every
write pays majority replication but ownership spreading only buys three
owners' disks (quorum tax vs 1 node: **12%**, down from 72% in v1.0.3). Five
nodes pays the same tax and buys five owners — **1.38× a single node**, the
first time the durable path has scaled OUT past one machine. The uncontended
ack stays ~1.6 ms at 5 nodes vs 0.93 ms at 1 — quorum's latency cost when the
pipeline is empty.

**v1.0.3 → v1.0.4, honestly:** 1-node 3,048 → 11,833 (**3.9×**, comparable
disks); 3-node 867 → 10,393 (nominally 12×, but v1.0.3's row was hostage to a
706-barriers/s volume — against v1.0.2's 1,753 on a healthy draw it is
**5.9×**); 5-node — no prior number has ever existed to compare against.
QoS 2: 2.6× / 2.7× at sizes 1 / 3.

## Durability tiers (ADR 0072) — same workload, publisher-selected ack meaning

Saturating plus an uncontended (window 1) variant per tier — at closed-loop
saturation the lanes flow-control every tier to the pipeline's rate, so the
tier's real face is the uncontended ack RTT:

| nodes | tier | acked msg/s (sat) | exact p99 (sat) | exact p99 (uncontended) |
|---|---|---|---|---|
| 1 | `quorum` | 11,833 [10,957..12,005] | 65 ms | 0.93 ms [0.92..0.95] |
| 1 | `local` | 11,683 [11,664..11,888] | 65 ms | 0.93 ms [0.92..0.94] |
| 3 | `quorum` | 10,393 [10,325..10,398] | 84 ms | 1.79 ms [1.76..1.79] |
| 3 | `local` | 9,941 [9,667..10,252] | 85 ms | 1.78 ms [1.78..1.81] |
| 5 | `quorum` | 16,378 [16,333..16,679] | 53 ms | 1.59 ms [1.58..1.63] |
| 5 | `local` | 16,303 [16,142..16,601] | 54 ms | 1.55 ms [1.55..1.57] |

The two-run finding **holds for a third run and a third disk draw: the tiers
converge on datacenter NVMe** — weakening the ack's meaning buys nothing here;
quorum is already the cheapest honest thing this hardware can say. **The
`relaxed` rows are absent this run — a bench defect, not a broker one**
(issue #394): the tier arms reuse durable client-ids across invocations
against the same long-lived cluster, and the relaxed arm (which runs last)
inherits the local arm's still-queued sessions; the broker's spec-correct
post-CONNACK redelivery then trips the harness's rigid frame-order
assertion. The defect was filed with the fix direction (per-invocation id
salt); the rows return next run.

## Curve 2 — non-durable `$share` fan-out (the ADR 0015 mechanism)

600 publishers → `bench/%i` (QoS 1, 256 B, window 100), 300 subscribers in one
shared group `$share/g1/bench/#`, populations spread across all drivers and
brokers; the same offered ladder (20k…300k msg/s) at every size, 60 s per
rung. Broker restarted clean per size, durable plane off.

Subscriber-side delivered rates (the summarizer's honest lane,
emqtt-bench-counted):

| offered | 1 node | 3 nodes | 5 nodes (2 drivers) |
|---|---|---|---|
| 20k | 19.6k | 18.8k, p99 ≤ 5ms | 14.9k |
| 50k | 22.5k ← plateau | 47.1k, p99 ≤ 25ms | 36.3k |
| 100k | 22.5k | 67.1k | 59.6k |
| 200–300k | ~22.9k | **~68.5k ← plateau** | ~54k |

Statistically unchanged from v1.0.3 (22.8k / 69k / 70k) at sizes 1 and 3 —
expected: v1.0.4's changes are durable-path, and this lane runs with the
durable plane off. Two disclosures keep the table honest:

- **Broker-received exceeds driver-sent at every rung** (flagged per cell by
  the summarizer): under deliberate overload the closed-loop QoS 1 window
  retransmits, and the broker counts each accepted redelivery. The
  subscriber-delivered number is the one tabled.
- **The 5-node column is a two-driver floor** (quota ceiling), and this
  floor read *lower* than v1.0.3's two-driver floor (~54k vs ~70k at the top
  rungs) — driver-side variance between runs of an explicitly driver-limited
  configuration, not a broker verdict in either direction. The real 5-node
  knee still waits on the quota raise.

## Connections at 50,000

| nodes | connected | broker RSS growth | KiB per idle connection |
|---|---|---|---|
| 1 | 49,998 | 945 MiB | 19.4 |
| 3 | 49,998 | 960 MiB | 19.7 |
| 5 | 50,000 | 943 MiB | 19.3 |

Flat per-connection memory in cluster size, fourth run in a row (~15 KiB/conn
claimed in `docs/SIZING.md`; 19.3–19.7 measured with observability running).

## Losing dimensions, stated first

- **The 5-node durable point took seven formations to measure, and the reason
  is a real product defect (issue #396).** SWIM gossips a member's
  self-claimed bind address to third parties; bound to the documented default
  `0.0.0.0:7946`, every member learned by relay (rather than from a seed's
  first-hand UDP source) is recorded at an unroutable address and dialed at
  loopback. On this rig's seed graph the three non-seed brokers had no
  working SWIM links among themselves, and the ring-middle member was
  probe-failed by both neighbors and evicted — the same node, every
  formation, on provably healthy fabric (live tcpdump: the victim's probes of
  gossip-learned peers landing on its own loopback; ping, TCP and sized-UDP
  sweeps clean throughout). Recovery is then impossible: the evicted member's
  probes still get *answered* by nodes that no longer list it, so the #383
  re-greet never fires, and nothing re-admits a fully removed member (issue
  #393) — restart included, because the poisoned records live on the other
  nodes. Whether a formation survives is a race between indirect-probe
  address repair and the suspicion timeout: six formations lost it, the
  seventh won, and the rig now binds SWIM to the routable private IP so the
  class is closed for the bench. This also retroactively explains the
  v1.0.2/v1.0.3 "5-node splits" attributed to #383 — whose shipped fixes,
  for the record, were observed behaving exactly as designed in the wild
  (first-hand tombstone pierces landing mid-incident).
- **The `relaxed` tier is unmeasured this run** — bench defect #394,
  disclosed in the tier section.
- **Curve 2's 5-node column is a two-driver floor** (quota), with the
  floor-vs-floor variance against v1.0.3 disclosed above.
- Lane B p99s are bucket bounds; cross-driver clock skew bounds the finest
  readable bucket.
- Multi-tenant NVMe variance is real (3.7× across nominally identical hosts
  in v1.0.3); this run's draw was clean (2,051–2,521), and with ADR 0074
  shipped the durable rows no longer inherit the draw either way.

## A run judges itself

Enforced mechanically: per-host barrier probes gate Curve 1 (a size without
probes renders UNINTERPRETABLE); `durable_bench`'s verdicts are carried
verbatim; broker counter deltas cross-check driver totals with every mismatch
flagged; driver-limited rungs are excluded from knee detection; preflight
captures every node's `/readyz` + `/statusz`; and since this campaign the rig
proves the private-net full mesh before the first broker starts, gates lane A
on full membership with one wipe-and-re-form retry, and collects every host's
journal on failure — which is how issue #396 went from "fifth mystery split"
to a packet-capture-proven root cause inside one afternoon.

## Reproducing everything above

```sh
cd bench/scale
export HCLOUD_TOKEN=...   # Read & Write token, dedicated Hetzner project
./run.sh smoke            # ~20 min, <€0.50 — proves the rig end to end
MQTTD_VERSION=1.0.4 OBSERVE=1 DRIVER_COUNT=3 ./run.sh full 1 3
MQTTD_VERSION=1.0.4 OBSERVE=1 DRIVER_COUNT=2 ./run.sh full 5
# a single lane can be rerun in isolation, e.g. the durable lane only:
LANES=A MQTTD_VERSION=1.0.4 DRIVER_COUNT=2 ./run.sh full 5
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
- `docs/adr/0071-owner-side-group-commit.md`, `docs/adr/0072-per-message-durability-selection.md`,
  `docs/adr/0073-scale-out-durable-ownership.md` (awaiting its 7/10-node
  measurement behind the quota raise), `docs/adr/0074-detached-ack-truncate.md`
  — **its hardware falsifier is this run, and it passed** (3.9× / 12× /
  first-ever 5-node point; disk-draw pinning gone).
- `docs/adr/0048-comparative-benchmarking.md` §2 — the mandate and honesty
  rules; `docs/delivery/0048-comparative-benchmarking.md` T3/T4 track this work.
- Issues: #358 (fixed v1.0.1), #368 (observability fixed v1.0.3), #376 (fixed
  v1.0.3), #383 (fixed v1.0.4 — fixes observed working in the wild this run),
  #390 (fixed, ships next release), #393 / #396 (open — the removal trap and
  the address-dissemination defect this campaign diagnosed), #394 (open —
  the relaxed-tier bench defect).
