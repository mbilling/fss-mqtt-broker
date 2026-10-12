# Durable QoS 1 at 3 and 10 nodes on the 1.2.0 candidate, default store — one provisioning

Campaign card. Env: [`qos1-campaign/durable-knees-7d4eced.env`](qos1-campaign/durable-knees-7d4eced.env)
(shape: [`durable-3-5-7-10.env`](qos1-campaign/durable-3-5-7-10.env)). Driver:
[`482-per-node-knee.sh`](482-per-node-knee.sh). The clean-session arm of the same day is
[`qos1-knees-2026-10-12.md`](qos1-knees-2026-10-12.md).

**The binary is an unreleased candidate, not the signed release.** It is the
`release: v1.2.0` commit `7d4eced8`, cross-built by hand (zig cc, x86_64 musl, not
stripped, built without the jq module), published as the pre-release
`bench-candidate-7d4eced`, sha256 `9111c1eb…c513`. v1.2.0 was not tagged when this ran, and
this is not the release pipeline's build. The run is stamped `UNRELEASED-BINARY.txt`: its
numbers belong to that binary and are **not published-curve points**
([README](README.md), ADR 0048).

**The store is the default, redb.** `MQTTD_REPLICA_STORE` and `MQTTD_REPLICAS` were unset:
redb, 2 copies. The earlier 3-node durable figures in
[`0080-replicas-ab-n3.md`](0080-replicas-ab-n3.md) (GREEN through 15 sites, 30k msg/s per
node) used `MQTTD_REPLICA_STORE=log`. **The two are not comparable**, and nothing here
says the log-store figures changed.

## The question

What does durable QoS 1 carry per node at 10 nodes, where no durable knee was recorded,
and is it the same per node at 3?

**No v1.1.0 arm ran on the same hosts**, so the run does not measure a change since
v1.1.0, in either direction.

## The run

- Run `.runs/knee-20261012T122155Z` (untracked), 2026-10-12, 12:21–13:49 UTC, Hetzner
  `fsn1`. 10 × CCX23 brokers (4 dedicated vCPU, local NVMe) + 20 × CCX33 drivers, one
  provisioning, torn down and leak-audited by the rig.
- Arms on the same hosts: 10 nodes (sites 1 · 25 · 35 · 40; 50 skipped by the stop rule),
  3 nodes with 8 drivers (1 · 12 · 15; 18 skipped), 10 nodes again (1 · 25) as the drift
  control. Each arm closes with a repeated 1-site control.
- A site is 600 QoS 1 publishers at 10 msg/s (6,000 msg/s, 200 B) into a `$share` group of
  20 **persistent** QoS 1 consumers (`LANE_E_SESSION_EXPIRY=30`): every delivery waits for
  fsync and replication. 60 s windows.
- **The 3-node ladder started above this store's ceiling.** It was set from the log-store
  result (12 sites = 24k/node), which redb does not carry. There is therefore no carried
  3-node rung above 1 site. That is a mistake in choosing the ladder, not a finding.
- Audit driver: emqtt-bench 0.6.3 + the `qos1-driver/` audit extension, rebuilt for this run
  (archive sha256 `2cded28d…`, image `sha256:1194c6b2…`).
- `OBSERVE=1`: live Grafana was attached, so every host also ran the metrics shipper. The
  rig's own note says a published number may want `OBSERVE=0`.

## Result

`delivered/s (drivers)` is `summarize-curve.py`'s column, the rig's verdict tool.
`delivered/s (brokers)` is `extract-lane-e.py`'s, from each broker's own counters over
the window. Latencies are histogram bucket upper bounds.

### Graded GREEN by the rig

| nodes | sites | offered msg/s | delivered/s (drivers) | delivered/s (brokers) | per node | p50 · p95 · p99 | verdict |
|---|---|---|---|---|---|---|---|
| 10 | 25 (drift control) | 150,000 | 149,976 | 149,979 | 15.0k | ≤25 · ≤50 · ≤500 ms | GREEN |

Every 1-site rung (6,000 msg/s; six of them) is GREEN at p50 · p95 · p99 ≤ 5 ms.

**The arm this rung belongs to fails the crossing gate** (next section), so the rung is
graded but the arm is not a gated scale-curve point.

### Measured, not certified

| nodes | sites | offered msg/s | sent/s (publishers) | delivered/s (drivers) | delivered/s (brokers) | per node, drivers · brokers | publishes late | p50 · p95 · p99 | verdict |
|---|---|---|---|---|---|---|---|---|---|
| 10 | 25 | 150,000 | 150,259 | 149,071 | 150,023 | 14.9k · 15.0k | 4% | ≤25 · ≤100 · ≤500 ms | NOT CARRIED |
| 10 | 35 | 210,000 | 199,910 (95%) | 199,072 | 192,217 | 19.9k · 19.2k | 41% | ≤50 · ≤500 · ≤500 ms | NOT CARRIED |
| 10 | 40 | 240,000 | 219,082 (91%) | 219,004 | 210,547 | 21.9k · 21.1k | 53% | ≤100 · ≤500 · ≤500 ms | NOT CARRIED |
| 3 | 12 | 72,000 | 62,933 (87%) | 62,806 | 62,243 | 20.9k · 20.7k | 57% | ≤100 · ≤500 · ≤500 ms | NOT CARRIED |
| 3 | 15 | 90,000 | 63,369 (70%) | 63,429 | 62,611 | 21.1k · 20.9k | 88% | ≤100 · ≤500 · ≤1000 ms | NOT CARRIED |

The verdicts, verbatim, in the order of the table:

```text
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%)
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%); NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (95% of offer); PUBLISHERS LATE (41% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%); NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (91% of offer); PUBLISHERS LATE (53% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%); NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (87% of offer); PUBLISHERS LATE (57% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%); NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (70% of offer); PUBLISHERS LATE (88% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
```

What each reason means here:

- **Scrape-window uncertainty (all five rows).** The drivers' endpoint scrapes at the
  window edges were too far apart for the audited rates, so the summarizer fell back to
  the drivers' own logs. On the first 25-site rung that is the only objection: the offer
  was sent and delivered, and the same rung 57 minutes later, on the same hosts, is GREEN.
- **Offer not met, publishers late (four rows).** Past the ceiling the brokers acknowledge
  more slowly, and a QoS 1 publisher that waits for its ack falls behind its schedule. The
  messages that were not sent were never offered to the broker. Nothing was dropped.
- **Not the load generators.** No rung carries a DRIVER CORE SATURATED flag, and the
  least-idle driver host averaged 68–78% idle on every rung above 1 site.
- **Not steady.** These four rungs never held their per-site band, so their delivered rate
  is a 60 s average of an overloaded cluster, not a steady state.
- Both ladders stopped after two consecutive NOT CARRIED rungs.

### The crossing gate fails at 10 nodes

The rig says to gate each arm before reading any number. For both 10-node arms
(abridged: the full line names every broker above 0.5% on every rung):

```text
GATE nodes=10 FAIL sites-1 broker0 crossing 100.00% > 0.5%; … sites-25 broker2 crossing 32.00% > 0.5%; … sites-40 broker9 crossing 20.90% > 0.5%
```

| arm | sites | publishes forwarded to another broker | highest single broker |
|---|---|---|---|
| 10 nodes | 1 · 1 (control) | 19.97% · 10.00% | 100.00% |
| 10 nodes | 25 · 35 · 40 | 13.60% · 16.84% · 15.48% | 32.00% · 37.87% · 31.40% |
| 10 nodes (drift) | 1 · 1 (control) · 25 | 20.00% · 10.00% · 13.60% | 100.00% · 100.00% · 32.00% |
| 3 nodes | 1 · 1 (control) · 12 · 15 | 0.00% on every rung | 0.00% |

`GATE nodes=3 PASS 4 rungs, cert=canary, max broker crossing 0.00% <= 0.5%`.

The shape predicts about 0% crossing (20 consumers per site, covering every broker), and
the clean-session campaign on the same shape of consumers measured 0.00% at 10 nodes the
same day. With persistent consumers at 10 nodes, 14–17% of publishes were forwarded. This
card does not explain it. It matters for the reading below: **the 10-node rungs carried
forwarding work the 3-node rungs did not**, so the two sizes did not run the same path.

### Where the time goes

The summarizer's own verdict at the most loaded rung of each arm:

```text
10 nodes, 40 sites: WRITER-BOUND: the writer spends up to 100% of each second on per-op work, and one flush explains at most 17% of a commit, disk util up to 54%; per-op commit cost within 30% across disks 3.8x apart in flush rate; the limit is the hot path (commit preparation), not the disk
3 nodes, 12 sites:  WRITER-BOUND: the writer spends up to 107% of each second on per-op work, and one flush explains at most 11% of a commit, disk util up to 49%; per-op commit cost within 8% across disks 3.0x apart in flush rate; the limit is the hot path (commit preparation), not the disk
10 nodes, 25 sites (drift control, GREEN): WRITER-BOUND: the writer spends up to 89% of each second on per-op work, and one flush explains at most 48% of a commit, disk util up to 63%; per-op commit cost within 66% across disks 3.4x apart in flush rate; the limit is the hot path (commit preparation), not the disk
```

| nodes | sites | writer ops/s per node | ops per commit | commit ms | barrier floor/s | broker idle (mean / least-idle host / least-idle second) |
|---|---|---|---|---|---|---|
| 10 | 25 | 24,376–40,402 | 18.1–349.5 | 0.80–9.27 | 703–2,644 | 32/28/22% |
| 10 | 35 | 30,314–51,611 | 40.5–685.9 | 1.43–15.27 | 703–2,644 | 28/23/14% |
| 10 | 40 | 40,128–51,920 | 87.1–877.3 | 2.22–18.14 | 703–2,644 | 27/21/14% |
| 3 | 12 | 46,848–49,160 | 155.6–729.6 | 3.59–16.62 | 866–2,611 | 31/26/18% |
| 3 | 15 | 44,753–51,122 | 127.6–1165.0 | 2.92–26.21 | 866–2,611 | 34/32/23% |
| 10 | 25 (drift control) | 22,610–39,615 | 18.4–263.5 | 0.81–6.95 | 760–2,577 | 31/28/18% |

At both sizes the per-node writers top out near 50,000 ops/s, on disks whose flush rates
are 3–4× apart.

### Ledger

`qos1-campaign/ledger-report.py`, per rung: every message identity sent, acknowledged and
received, reconciled across the whole `$share` group.

| arm | rung | sent | acked | received | unique | missing | unacked | duplicates |
|---|---|---|---|---|---|---|---|---|
| 10 nodes | 1 site | 677,928 | 677,928 | 677,928 | 677,928 | 0 | 0 | 0 |
| 10 nodes | 1 site (control) | 700,935 | 700,935 | 700,935 | 700,935 | 0 | 0 | 0 |
| 10 nodes | 25 sites | 18,458,400 | 18,458,400 | 18,458,400 | 18,458,400 | 0 | 0 | 0 |
| 10 nodes | 35 sites | 56,354,906 | 56,354,906 | 56,354,906 | 56,354,906 | 0 | 0 | 0 |
| 10 nodes | 40 sites | 61,242,562 | 61,242,562 | 61,242,562 | 61,242,562 | 0 | 0 | 0 |
| 3 nodes | 1 site | 655,614 | 655,614 | 655,614 | 655,614 | 0 | 0 | 0 |
| 3 nodes | 1 site (control) | 661,179 | 661,179 | 661,179 | 661,179 | 0 | 0 | 0 |
| 3 nodes | 12 sites | 17,620,469 | 17,620,469 | 17,620,469 | 17,620,469 | 0 | 0 | 0 |
| 3 nodes | 15 sites | 17,405,715 | 17,405,715 | 17,405,715 | 17,405,715 | 0 | 0 | 0 |
| 10 nodes (drift) | 1 site | 710,731 | 710,731 | 710,731 | 710,731 | 0 | 0 | 0 |
| 10 nodes (drift) | 1 site (control) | 689,599 | 689,599 | 689,599 | 689,599 | 0 | 0 | 0 |
| 10 nodes (drift) | 25 sites | 18,188,092 | 18,188,092 | 18,188,092 | 18,188,092 | 0 | 0 | 0 |

193,366,130 identities over the campaign: none missing, none unacknowledged, none
duplicated, none unexpected. Every broker reports 0 drops in every window. **Past the
ceiling the cluster slows its publishers; it does not lose what it accepted.**

## What this does and does not show

- **Graded GREEN:** 150,000 msg/s (15.0k per node) at 10 nodes, p99 ≤ 500 ms, once, in an
  arm whose crossing gate failed. The same rung an hour earlier delivered the same rate
  and was rejected only on scrape-window evidence.
- **The clean rate at 10 nodes is at least 15k msg/s per node.** 25 sites was the lowest
  rung above 1 site, and the next one (21k/node offered) was not carried, so the knee lies
  somewhere between 15k and about 20k per node. It was not located.
- **The ceiling is about 21k msg/s per node on the default store**: 19.2–21.9k at 10
  nodes (two rungs, two readings each), 20.7–21.1k at 3 nodes. It is read from NOT CARRIED,
  not-steady rungs, so it is a saturation plateau, not a certified knee.
- **The ceiling is about the same per node at 3 and at 10 nodes.** That is the most this
  run supports. It is not a certified flat curve: no 3-node rung was carried, the 10-node
  arm failed the crossing gate, and 3 and 10 are the only sizes.
- **redb only.** These figures are not comparable with the log-store results, and say
  nothing about them.
- **Not a comparison with v1.1.0.** No A/B ran on this provisioning.

## Estimate: brokers needed to meet the offers that were not met

**This is arithmetic on the figures above, not a measurement.** It assumes per-node
capacity stays flat beyond 10 nodes. It was measured about flat only between 3 and 10,
with the caveats above.

Two per-node rates:
- **Clean: 15,000 msg/s per node**, the GREEN rung (149,976 / 10). The rate to size on.
- **Ceiling: 21,000 msg/s per node**, where publishers already run late (10 nodes:
  219,004 / 10 = 21,900 and 199,072 / 10 = 19,907; 3 nodes: 63,429 / 3 = 21,143 and
  62,806 / 3 = 20,935). The lowest reading is the brokers' 192,217 / 10 = 19,222.

| offer not met | at the ceiling: offer / 21,000 | at the lowest ceiling reading: offer / 19,222 | at the clean rate: offer / 15,000 |
|---|---|---|---|
| 210,000 msg/s (35 sites) | 10.0 → 10 nodes | 10.9 → 11 nodes | 14.0 → **14 nodes** |
| 240,000 msg/s (40 sites) | 11.4 → 12 nodes | 12.5 → 13 nodes | 16.0 → **16 nodes** |
| 72,000 msg/s (12 sites) | 3.4 → 4 nodes | 3.7 → 4 nodes | 4.8 → **5 nodes** |
| 90,000 msg/s (15 sites) | 4.3 → 5 nodes | 4.7 → 5 nodes | 6.0 → **6 nodes** |

A cluster sized at the ceiling delivers the rate with publishers running late. 10 nodes at
210,000 offered is exactly the rung that was not carried.

What fits the rig's quota (30 servers, 200 vCPU; CCX23 = 4 vCPU, CCX33 = 8 vCPU; a durable
site needs 3 driver containers, 1 publisher + 2 consumer, at one vCPU each):

| target | brokers | drivers | servers · vCPU | driver containers needed · available | fits |
|---|---|---|---|---|---|
| 210,000 at the clean rate | 14 × CCX23 | 16 × CCX33 | 30 · 56 + 128 = 184 | 35 × 3 = 105 · 128 | yes |
| 240,000 at the clean rate | 16 × CCX23 | 14 × CCX33 | 30 · 64 + 112 = 176 | 40 × 3 = 120 · 112 | **no**, 8 containers short; 15 drivers would be 31 servers |
| 240,000 at the ceiling | 13 × CCX23 | 17 × CCX33 | 30 · 52 + 136 = 188 | 120 · 136 | yes |
| 72,000 · 90,000 at the clean rate | 5 · 6 × CCX23 | 8 × CCX33 | 13 · 14 servers | 36 · 45 of 64 | yes |

So one provisioning inside the quota can test the estimate at 14 nodes (210,000 msg/s,
35 sites). 16 nodes at 240,000 needs a larger driver type or one more server.

## Reproduce

```sh
cd bench/scale
export HCLOUD_TOKEN=...          # a dedicated Hetzner project, 30 servers / 200 vCPU
set -a && . qos1-campaign/durable-knees-7d4eced.env && set +a
export QOS1_DRIVER_ARCHIVE=/absolute/path/to/driver.tar.gz   # qos1-driver/build.sh <dir>
PREFLIGHT_ONLY=1 ./482-per-node-knee.sh   # offline
./482-per-node-knee.sh                    # PAID: 10 -> 3 -> 10 on 30 servers, ~1.5 h

# gate each arm, then read it
python3 extract-lane-e.py --crossing-gate 0.5 .runs/knee-<stamp>/<arm>/results
python3 summarize-curve.py .runs/knee-<stamp>/<arm>/results
python3 qos1-campaign/ledger-report.py .runs/knee-<stamp>/<arm>
python3 chart-scale-out.py qos1-candidate   # redraw the chart from the recorded points
```

A 3-node ladder for this store should start below the ceiling: 6 · 8 · 9 · 10 sites is
12k · 16k · 18k · 20k per node.

Chart: [`scale-out-qos1-candidate.svg`](../../docs/benchmarks/img/scale-out-qos1-candidate.svg),
drawn by [`chart-scale-out.py`](chart-scale-out.py) from the points in this card.
