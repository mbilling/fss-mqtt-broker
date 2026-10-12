# Durable QoS 1 scale-out at 10, 12 and 14 nodes — one provisioning

Campaign card. Env:
[`qos1-campaign/durable-scaleout-10-12-14.env`](qos1-campaign/durable-scaleout-10-12-14.env).
Driver: [`482-per-node-knee.sh`](482-per-node-knee.sh). Method and harness safeguards:
[`482-per-node-knee.md`](482-per-node-knee.md).

**This is a diagnostic scale-out, read by a simple rule. It is not a certified curve.**
Its figures are for saying "the cluster carried this much at this size" in plain words.
They are not published-curve points (ADR 0048), and 12 and 14 nodes say nothing about
what the cluster survives (see [Placement](#placement-two-groups-above-10-brokers)).

**Ran on 2026-10-12. Three of its four arms completed; the closing drift control did
not** ([why](#the-closing-arm-did-not-run)).

**The binary is an unreleased candidate, not the signed release.** It is the
`release: v1.2.0` commit `7d4eced8`, cross-built by hand (zig cc, x86_64 musl), published
as the pre-release `bench-candidate-7d4eced`, sha256 `9111c1eb…c513`. It is not the
release pipeline's build. The run is stamped `UNRELEASED-BINARY.txt`.

## Result

**The total grew with every size, at about the same rate per node, and no message was
lost.** Rates are the brokers' own counters over one 60 s window per rung.

| nodes | offered 15k msg/s per node: delivered | offered 18k msg/s per node: delivered | p99 | messages ledgered · lost |
|---|---|---|---|---|
| 10 | 150,419 msg/s of 150,000 (15.0k per node) | 173,100 of 180,000 (17.3k per node) | ≤ 500 ms | 91,243,694 · 0 |
| 12 | 179,039 of 180,000 (14.9k per node) | 205,750 of 216,000 (17.1k per node) | ≤ 500 ms | 83,430,832 · 0 |
| 14 | 208,739 of 210,000 (14.9k per node) | 240,497 of 252,000 (17.2k per node) | ≤ 500 ms | 131,558,906 · 0 |

- **At 15k msg/s per node offered, each size delivered its offer to within 0.6%**:
  150,419, 179,039 and 208,739 msg/s at 10, 12 and 14 nodes.
- **At 18k per node offered, each size delivered 95–96% of it**: 173,100, 205,750 and
  240,497 msg/s. The publishers were slowed; nothing was dropped.
- **Every rung holds by the reading rule** below: its ledger closes, no broker dropped a
  message, p99 ≤ 500 ms.
- **The rig certified none of the six.** Its verdict on each is NOT CARRIED, because
  publishers ran behind schedule ([verbatim](#the-rigs-verdicts)). Only the 1-site
  control rungs are GREEN.
- **No closing drift control ran**, so nothing bounds drift across the two hours by a
  repeat. See [What this does and does not show](#what-this-does-and-does-not-show).

![Durable QoS 1 total msg/s at 10, 12 and 14 nodes on the 1.2.0 candidate](../../docs/benchmarks/img/scale-out-durable-candidate.svg)

## The question

Does durable QoS 1 keep growing when brokers are added past 10? Concretely: do 12 and
14 nodes carry the same per-node rate that 10 nodes carry, on the same hosts?

## The reading rule

A rung **holds** if:

1. **no message is lost**: the ledger shows `missing = 0` for the rung
   (`qos1-campaign/ledger-report.py`: every message identity sent, acknowledged and
   received, reconciled across the whole `$share` group), and no broker dropped
   anything, and
2. **p99 stays below 1 s**: the rung's end-to-end p99 is in the ≤ 1000 ms bucket or
   lower (`summarize-curve.py`).

The rate a rung holds at is the brokers' own: each broker's delivered counter over its
window. `summarize-curve.py` computes the rule and prints it per rung, with its inputs,
under "Reading from the brokers' counters", beside the rig's verdict and apart from it.

That is the maintainer's rule for this campaign: not 100% scientifically correct, but
easy to communicate. What it leaves out, on purpose:

- **The rig's own verdict is stricter.** `summarize-curve.py` also grades a rung NOT
  CARRIED when publishers could not keep their timers (late publishes, a sent rate below
  the offer). Report the delivered rate, not the offered one, when the two differ.
- **The crossing gate is reported, not applied** ([Cross-node traffic](#cross-node-traffic)).
- **One 60 s window per rung, no repetitions.** A rung that holds once is a reading, not
  a median.

## The run

- Run `.runs/knee-20261012T170816Z` (untracked), 2026-10-12, 17:08–19:13 UTC, Hetzner
  `fsn1`. 14 × CCX23 brokers (4 dedicated vCPU, local NVMe) + 16 × CCX33 drivers (8
  vCPU), one provisioning, torn down and leak-audited by the rig.
- **Ran:** 14 nodes (sites 1 · 35 · 42), then 12 nodes (1 · 30 · 36), then 10 nodes
  (1 · 25 · 30), each a fresh cluster on a prefix of the same hosts, all 16 drivers in
  every arm, each closing with a repeated 1-site control.
- **Did not run:** the fourth arm, 14 nodes again (1 · 35), the drift control. It stopped
  before its first rung ([below](#the-closing-arm-did-not-run)).
- A site is 600 QoS 1 publishers at 10 msg/s (6,000 msg/s, 200 B) into a `$share` group
  of **28 persistent** QoS 1 consumers (`LANE_E_SESSION_EXPIRY=30`): every delivery waits
  for fsync and replication.
- **Store:** the default (redb), 2 copies. `MQTTD_REPLICA_STORE` and `MQTTD_REPLICAS`
  were unset. Not comparable with the log-store figures in
  [`0080-replicas-ab-n3.md`](0080-replicas-ab-n3.md).
- `OBSERVE=1`: live Grafana was attached, so every host also ran the metrics shipper.
- The rig scripts that ran were at commit `73dc9bee`. The figures here were derived
  afterwards with the tools of the tree this card is in.

## Every rung

`brokers received/s` and `brokers delivered/s` are each broker's own counters over its
own window (`extract-lane-e.py`). Every broker answered both window scrapes within 23 ms,
and the band that leaves on the rate (`deliv_band`) is ±0.02% on every loaded rung.
`drivers delivered/s` is the driver-side figure the rig graded; it agrees with the
brokers' to within 18 msg/s on every rung, and `summarize-curve.py` prints no
`READINGS DISAGREE`. Latencies are histogram bucket upper bounds, from the consumers.

| nodes | sites | offered msg/s | brokers received/s | brokers delivered/s | of the offer | per node | drivers delivered/s | publishes late | p50 · p95 · p99 | reading | the rig's verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 14 | 35 | 210,000 | 208,721 | 208,739 | 99.4% | 14.9k | 208,745 | 14% | ≤50 · ≤500 · ≤500 ms | holds | NOT CARRIED (1) |
| 14 | 42 | 252,000 | 240,484 | 240,497 | 95.4% | 17.2k | 240,507 | 28% | ≤100 · ≤500 · ≤500 ms | holds | NOT CARRIED (2) |
| 12 | 30 | 180,000 | 179,029 | 179,039 | 99.5% | 14.9k | 179,033 | 7% | ≤50 · ≤100 · ≤500 ms | holds | NOT CARRIED (3) |
| 12 | 36 | 216,000 | 205,746 | 205,750 | 95.3% | 17.1k | 205,744 | 26% | ≤50 · ≤500 · ≤500 ms | holds | NOT CARRIED (4) |
| 10 | 25 | 150,000 | 150,441 | 150,419 | 100.3% | 15.0k | 150,401 | 8% | ≤25 · ≤100 · ≤500 ms | holds | NOT CARRIED (5) |
| 10 | 30 | 180,000 | 173,106 | 173,100 | 96.2% | 17.3k | 173,099 | 25% | ≤50 · ≤500 · ≤500 ms | holds | NOT CARRIED (6) |

Every 1-site rung (6,000 msg/s; six of them, one before and one after each ladder) is
GREEN and holds, at 6,000 msg/s delivered and p99 ≤ 10 ms. No rung was skipped: each
ladder had two rungs above 1 site and the stop rule needs two consecutive failures.

### The rig's verdicts

Verbatim, in table order:

```text
(1) NOT CARRIED: NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); PUBLISHERS LATE (14% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
(2) NOT CARRIED: NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (95% of offer); PUBLISHERS LATE (28% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
(3) NOT CARRIED: PUBLISHERS LATE (7% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
(4) NOT CARRIED: NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (95% of offer); PUBLISHERS LATE (26% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
(5) NOT CARRIED: PUBLISHERS LATE (8% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
(6) NOT CARRIED: NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (96% of offer); PUBLISHERS LATE (25% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
```

- **At 15k per node the only objection at 10 and 12 nodes is late publishes** (8% and
  7%). At 14 nodes it is 14%, and the rung was also not steady.
- **At 18k per node the offer was not met** (95–96%), a quarter of the publishes were
  late, and no rung went steady.
- **No driver-side evidence was refused** on any rung, unlike the morning's campaign.

### What limited the 18k rungs: the brokers, not the load generators

The risk named before the run was the drivers: at 42 sites, 126 one-vCPU containers ran
on the 128 vCPUs the 16 drivers have. The evidence does not point there.

| nodes | sites | busiest driver core: mean busy · share of seconds ≥ 95% | driver hosts idle (mean / least-idle host / least-idle second) | busiest broker core: mean busy · share of seconds ≥ 95% | broker hosts idle (mean / least-idle host / least-idle second) | writer ops/s per node | commit ms |
|---|---|---|---|---|---|---|---|
| 14 | 35 | 66% · 0% | 73/60/53% | 95% · 64% | 27/15/10% | 19,576–44,037 | 1.88–19.36 |
| 14 | 42 | 76% · 2% | 68/59/54% | 95% · 75% | 25/14/9% | 23,002–45,460 | 4.28–23.17 |
| 12 | 30 | 53% · 0% | 76/69/64% | 90% · 0% | 29/21/16% | 22,621–42,385 | 3.88–12.91 |
| 12 | 36 | 65% · 0% | 73/65/56% | 94% · 17% | 26/17/14% | 28,169–44,775 | 3.75–22.71 |
| 10 | 25 | 45% · 0% | 82/76/69% | 89% · 0% | 29/22/18% | 24,230–38,125 | 3.76–8.14 |
| 10 | 30 | 51% · 0% | 77/67/61% | 90% · 0% | 26/19/13% | 29,658–42,247 | 4.30–17.03 |

- **The drivers had headroom.** No rung carries the summarizer's DRIVER CORE SATURATED
  flag. On the fully packed 42-site rung the busiest driver core averaged 76% busy and
  was at or above 95% in 2% of the window's seconds; the least-idle driver host averaged
  59% idle.
- **The brokers did not.** On the same rung the least-idle broker host averaged 14% idle,
  and its busiest core was at or above 95% in 75% of the seconds. The summarizer's verdict
  at the most loaded rung of each arm is WRITER-BOUND:

  ```text
  14 nodes, 42 sites: WRITER-BOUND: the writer spends up to 101% of each second on per-op work, and one flush explains at most 24% of a commit, disk util up to 62%; per-op commit cost within 116% across disks 1.3x apart in flush rate; the limit is the hot path (commit preparation), not the disk
  12 nodes, 36 sites: WRITER-BOUND: the writer spends up to 102% of each second on per-op work, and one flush explains at most 27% of a commit, disk util up to 63%; per-op commit cost within 47% across disks 1.7x apart in flush rate; the limit is the hot path (commit preparation), not the disk
  10 nodes, 30 sites: WRITER-BOUND: the writer spends up to 99% of each second on per-op work, and one flush explains at most 24% of a commit, disk util up to 72%; the limit is the hot path (commit preparation), not the disk
  ```
- **So the 4–5% not taken at 18k per node was the brokers acknowledging more slowly**, and
  QoS 1 publishers that wait for their acks falling behind. It is the same picture as the
  morning's 10-node arm past 15k per node.
- **This is not a located ceiling.** Each size has one rung above 15k per node, and it
  delivered 95–96% of its offer. The morning's 10-node arm, offered more, delivered up to
  21.1k per node. Where the cluster stops taking more was not measured here.
- **Not checked:** which broker cores were the busy ones and what ran on them. The
  highest reading at 14 nodes is on a broker of the second placement group
  (index 12, `mqttd-13`).

### Ledger

`qos1-campaign/ledger-report.py`, per rung: every message identity sent, acknowledged and
received, reconciled across the whole `$share` group.

| arm | rung | sent | acked | received | unique | missing | unacked | duplicates |
|---|---|---|---|---|---|---|---|---|
| 14 nodes | 1 site | 750,990 | 750,990 | 750,990 | 750,990 | 0 | 0 | 0 |
| 14 nodes | 1 site (control) | 699,483 | 699,483 | 699,483 | 699,483 | 0 | 0 | 0 |
| 14 nodes | 35 sites | 59,840,136 | 59,840,136 | 59,840,136 | 59,840,136 | 0 | 0 | 0 |
| 14 nodes | 42 sites | 70,268,297 | 70,268,297 | 70,268,297 | 70,268,297 | 0 | 0 | 0 |
| 12 nodes | 1 site | 695,711 | 695,711 | 695,711 | 695,711 | 0 | 0 | 0 |
| 12 nodes | 1 site (control) | 740,787 | 740,787 | 740,787 | 740,787 | 0 | 0 | 0 |
| 12 nodes | 30 sites | 21,832,180 | 21,832,180 | 21,832,180 | 21,832,180 | 0 | 0 | 0 |
| 12 nodes | 36 sites | 60,162,154 | 60,162,154 | 60,162,154 | 60,162,154 | 0 | 0 | 0 |
| 10 nodes | 1 site | 705,586 | 705,586 | 705,586 | 705,586 | 0 | 0 | 0 |
| 10 nodes | 1 site (control) | 693,967 | 693,967 | 693,967 | 693,967 | 0 | 0 | 0 |
| 10 nodes | 25 sites | 40,130,839 | 40,130,839 | 40,130,839 | 40,130,839 | 0 | 0 | 0 |
| 10 nodes | 30 sites | 49,713,302 | 49,713,302 | 49,713,302 | 49,713,302 | 0 | 0 | 0 |

306,233,432 identities over the campaign (131,558,906 at 14 nodes, 83,430,832 at 12,
91,243,694 at 10): none missing, none unacknowledged, none duplicated, none unexpected.
Every broker reports 0 drops on every rung, and on every rung the brokers' received total
equals the ledger's sent count.

With the two campaigns of the same morning
([`qos1-knees-2026-10-12.md`](qos1-knees-2026-10-12.md), 233,716,926;
[`durable-knees-2026-10-12.md`](durable-knees-2026-10-12.md), 193,366,130) that is
733,316,488 identities ledgered on this candidate in one day, none lost.

## Cross-node traffic

`extract-lane-e.py --crossing-gate 0.5` prints, per arm, the share of publishes each
broker forwarded to another broker. The shape check predicts about 0%: every broker is
dialled by a member of every site's `$share` group. **That does not hold for persistent
consumers**: a persistent session lives on the broker that owns it, not on the one its
client dialled, so some brokers hold no member of a site's group and forward that site's
publishes ([`durable-knees-2026-10-12.md`](durable-knees-2026-10-12.md#cross-node-traffic)).
That card predicted, for 28 hashed consumers per site, (1 − 1/N)^28: 12.6% at 14 nodes,
8.7% at 12 and 5.2% at 10.

Measured, with the prediction and the same sum weighted by the groups each broker reports
in `mqttd_replica_groups_current`:

| nodes | predicted, (1 − 1/N)^28 | predicted, weighted by groups | measured at 15k per node | measured at 18k per node | highest single broker (15k · 18k) |
|---|---|---|---|---|---|
| 10 | 5.2% | 5.98% | 4.78% | 8.67% | 11.99% · 20.56% |
| 12 | 8.7% | 9.84% | 13.41% | 12.15% | 33.33% · 35.33% |
| 14 | 12.6% | 14.29% | 15.79% | 13.36% | 49.27% · 39.18% |

The 1-site rungs forward 10.00–35.64%: one site has only 28 consumers, and each broker
without one of them forwards everything it receives (1 of 10, 2–3 of 12 and 2–5 of 14
brokers).

- **The prediction has the right direction and about the right size at 10 and 14 nodes,
  and is low at 12.** Forwarding grows with the cluster, as predicted.
- **The measured share is again a count of empty site × broker pairs.** At 10 nodes and
  25 sites the per-broker shares are whole 25ths, 1 · 2 · 3 · 0 · 0 · 0 · 1 · 0 · 2 · 3
  sites on brokers 0–9: 12 of 250 pairs, 4.80%, measured 4.78%. At 12 nodes and 30 sites
  they are about 10 · 7 · 9 · 0 · 2 · 3 · 3 · 1 · 3 · 6 · 0 · 4, 48 of 360 pairs. At 14
  nodes and 35 sites, about 77 of 490.
- **Consumers are not spread evenly, which is why some brokers forward much more than
  the mean.** `mqttd_subscriptions` per broker at the 15k rungs:
  - 10 nodes: 58 · 67 · 40 · 98 · 81 · 80 · 67 · 88 · 62 · 59 (700);
  - 12 nodes: 37 · 49 · 36 · 105 · 82 · 70 · 64 · 99 · 63 · 66 · 105 · 64 (840);
  - 14 nodes: 44 · 65 · 27 · 113 · 84 · 68 · 44 · 91 · 58 · 53 · 107 · 65 · 103 · 58 (980).

  Broker 2 holds the fewest at every size (27 of 980 at 14 nodes, where an even share is
  70) and forwards the most there, 49.27%. Broker 3 holds the most and forwarded 0.00% on
  every loaded rung. `mqttd_replica_groups_current` reads 49 · 51 · 35 · 69 · 50 · 60 ·
  46 · 53 · 56 · 43 at 10 nodes, 35 · 39 · 30 · 57 · 40 · 45 · 40 · 39 · 46 · 32 · 58 ·
  51 at 12, and 28 · 31 · 22 · 52 · 34 · 34 · 30 · 31 · 38 · 29 · 50 · 46 · 52 · 35 at 14
  (512 in all at each size): broker 2 has the fewest groups too.
- **Every forwarded publish carries `reason="shared-remote"`.**
- **Not checked:** the owner of each consumer was not recomputed from its client id; why
  12 nodes has more empty pairs than the group-weighted sum predicts (48 against about
  35); and why the share moves between the two rungs of one arm (other sites, and at 18k
  the sites did not deliver at equal rates).

The rig's crossing gate (every broker ≤ 0.5%) fails on all three arms, as expected. It is
reported, not applied.

**The consequence for comparing sizes:** the larger clusters did more forwarding. 10
nodes forwarded 4.78% of its publishes at 15k per node, 12 nodes 13.41% and 14 nodes
15.79%. They delivered about the same rate per node anyway.

## What this does and does not show

- **Adding nodes added throughput, at both offers.** 10 → 12 → 14 nodes delivered
  150,419 → 179,039 → 208,739 msg/s at 15k per node offered, and 173,100 → 205,750 →
  240,497 at 18k.
- **The rate per node did not fall with size**: 15.0k · 14.9k · 14.9k, and
  17.3k · 17.1k · 17.2k.
- **Nothing was lost and p99 stayed at or below 500 ms on every rung.**
- **None of it is certified.** The rig graded every loaded rung NOT CARRIED. These are
  readings by the rule above.
- **Drift across the two hours is not bounded by a repeat.** The closing 14-node arm did
  not run, so no rung was measured twice. What the run does have: a 1-site control before
  and after each ladder, all six GREEN at 6,000 msg/s; and an order (14, then 12, then
  10) in which the largest cluster ran first, on the freshest hosts. A host that slowed
  or sped up between 17:40 and 19:00 would move the comparison by an amount this run
  cannot state.
- **Brokers 11–14 are in a second placement group**, so the 12- and 14-node arms may have
  had two brokers on one physical machine. The result is a throughput figure and makes no
  claim about failure independence. The 10-node arm ran entirely on the first group.
- **One 60 s window per rung.** No repetition, no median.
- **Not the morning's 10-node shape.** That arm used 20 consumers per site and 20
  drivers; this one 28 and 16. At 25 sites both delivered the offer (150,023 there,
  150,419 here), forwarding 13.60% there and 4.78% here. The morning's arm was offered
  more and delivered 192,217 and 210,547 msg/s at 35 and 40 sites, so its higher totals
  are a higher offer, not a faster cluster.
- **redb only, and not a comparison with v1.1.0.** No A/B ran on this provisioning.
- **Unreleased candidate.** The figures belong to that binary.

## The closing arm did not run

Arm 4 re-formed the 14-node cluster on the same hosts after the 10-node arm, to repeat
arm 1's 35-site rung. The rig stopped it before the first rung, at 19:11:46Z:

```text
lane E: the mesh did not settle within 180s at N=14
```

- **What was wrong:** one pair of brokers, in one direction. `mqttd-11` held 13 members
  and 12 peer links and did not have `mqttd-12` in its view; `mqttd-12` held 14 members
  and 12 links. The other 12 brokers held 14 and 13. The state did not change on any of
  the 12 polls from t=8 s to t=192 s.
- **How it got there:** `mqttd-11` declared `mqttd-12` DEAD at 19:07:15, 68 s after
  starting, and never re-admitted it. Both processes were idle and healthy.
- **It is the broker's membership, not the rig.** Every formation in this campaign shows
  a burst of false SUSPECT and DEAD declarations between healthy brokers for one to three
  minutes, more at larger sizes. Most clear after about 30 s. Some do not: in arm 1 one
  broker held another DEAD for 191 s and a second for 128 s, and that formation passed the
  same gate at t=153 s of 180. The same two hosts, at the same addresses, were in the
  clusters of arms 1 and 2. The mesh settled in 153 s at 14 nodes, 66 s at 12 and 31 s
  at 10.
- **Tracked as [#971](https://github.com/mbilling/fss-mqtt-broker/issues/971)**, with the
  timestamps, the counters, the reading of the code on that path, and what it does not
  prove. The rig was not changed: its gate refused to measure a cluster with a missing
  link, which is what it is for.
- **For a rerun:** `LANE_E_MESH_SETTLE_BUDGET` (default 180 s) can be raised to wait out
  a slow heal. That hides the delay; it does not remove it.

The provisioning was destroyed and audited at 19:13:26Z.

## Shape

- **One provisioning:** 14 × CCX23 brokers (4 dedicated vCPU, local NVMe) + 16 × CCX33
  drivers (8 vCPU), Hetzner `fsn1`.
- **Quota:** 14 × 4 + 16 × 8 = 56 + 128 = **184 vCPU on 30 servers**. The project allows
  200 vCPU and 30 servers, so the server count is exactly at its limit.
- **Arms**, largest first, each re-formed as a fresh cluster on a prefix of the same
  hosts (`resize-cluster.sh`), all 16 drivers in every arm:

  | arm | brokers | left out (stopped, still billed) | sites | offered msg/s | per node |
  |---|---|---|---|---|---|
  | 1 | 14 | none | 1 · 35 · 42 | 6k · 210k · 252k | 15k · 18k |
  | 2 | 12 | mqttd-13, mqttd-14 | 1 · 30 · 36 | 6k · 180k · 216k | 15k · 18k |
  | 3 | 10 | mqttd-11 … mqttd-14 | 1 · 25 · 30 | 6k · 150k · 180k | 15k · 18k |
  | 4 (did not run) | 14 | none | 1 · 35 | 6k · 210k | 15k (drift control) |

  Each arm closes with a repeated 1-site control. Arm 4 was to repeat arm 1's 35-site
  rung: if the two differ, the hosts drifted and the sizes between them are not
  comparable.
- **A site** is 600 QoS 1 publishers at 10 msg/s (6,000 msg/s, 200 B) into a `$share`
  group of **28 persistent** QoS 1 consumers (`LANE_E_SESSION_EXPIRY=30`): every
  delivery waits for fsync and replication. One publisher container and two consumer
  containers per site. 60 s windows.
- **Store:** the default (redb), 2 copies. `MQTTD_REPLICA_STORE` and `MQTTD_REPLICAS`
  are unset.
- **Binary:** pinned in the shell, not in the env (`MQTTD_VERSION`, or `MQTTD_URL` +
  `MQTTD_SHA256` + `BENCH_GIT_REF`), as is the audit driver (`QOS1_DRIVER_ARCHIVE`).

### 28 consumers per site, not 20

The morning's campaign used 20 consumers per site in two containers. Each container's
10 clients dial 10 consecutive brokers, and the second container starts one broker
later, so the two reach 11 brokers. That was every broker at 10 nodes. At 14 it leaves
three brokers without a local member, and the shape check refuses it: "20 consumers in 2
containers cover only 11 of 14 brokers … ~21% of publishes would forward". 28 puts 14
clients in each container, so each one reaches every broker at every size.

Consequences: the 10-node arm here is not the morning's 10-node shape (28 consumers per
site, not 20), and at 12 and 10 nodes a container's 14 clients land two on some brokers
and one on others.

## Risks named before the run, and what happened

- **Drivers fully packed at 42 sites** (126 one-vCPU containers on 128 vCPUs). They had
  headroom: see
  [What limited the 18k rungs](#what-limited-the-18k-rungs-the-brokers-not-the-load-generators).
- **No server to spare** (30 of 30). No host was swapped.
- **First clusters above 10 nodes on this rig.** Formation worked at 12 and 14 on fresh
  hosts, slowly at 14 (153 s to a full mesh), and the re-formed 14-node cluster did not
  reach a full mesh in 192 s ([above](#the-closing-arm-did-not-run)).
- **The forwarding canary over 182 directed pairs at 14 nodes** (132 at 12, 90 at 10)
  passed in every arm that reached it, in about 30 s at 14 nodes and 19 s at 12 and 10,
  against a 180 s deadline.
- **Left-out brokers are the highest-numbered**, the ones in the second placement group.
  The 10-node arm therefore ran entirely on the first group and the 12- and 14-node arms
  added hosts of the second.

## Placement: two groups above 10 brokers

Hetzner admits at most 10 servers to a spread placement group. Brokers 1–10 are in the
original group and brokers 11–14 in a second one (`terraform/layout.tf`). Each group
keeps its own members on distinct physical hosts. Nothing keeps a broker of one group off
a host that carries a broker of the other. So at 12 and 14 nodes two brokers may share a
physical machine: the result is a throughput figure, and makes no claim about failure
independence.

## Reproduce

```sh
cd bench/scale
export HCLOUD_TOKEN=...          # a dedicated Hetzner project, 30 servers / 200 vCPU
set -a && . qos1-campaign/durable-scaleout-10-12-14.env && set +a
export MQTTD_VERSION=...         # or MQTTD_URL=... MQTTD_SHA256=... BENCH_GIT_REF=<commit>
export QOS1_DRIVER_ARCHIVE=/absolute/path/to/driver.tar.gz   # qos1-driver/build.sh <dir>

PREFLIGHT_ONLY=1 ./482-per-node-knee.sh   # offline: every arm's shape, budgets, self-tests
./482-per-node-knee.sh                    # PAID: 14 -> 12 -> 10 -> 14 on 30 servers

# per arm: the two things the reading rule needs, then the crossing
python3 qos1-campaign/ledger-report.py .runs/knee-<stamp>/<arm>      # missing must be 0
python3 summarize-curve.py .runs/knee-<stamp>/<arm>/results          # p99, delivered/s, both readings
python3 extract-lane-e.py --crossing-gate 0.5 .runs/knee-<stamp>/<arm>/results   # reported, not gated
python3 chart-scale-out.py durable-scaleout-candidate   # redraw the chart from the recorded points
```

Arm directories are `1-n14`, `2-n12`, `3-n10`, `4-n14`. `RESIZED.txt` in each resized arm
names the brokers kept and left out.

Chart: [`scale-out-durable-candidate.svg`](../../docs/benchmarks/img/scale-out-durable-candidate.svg),
drawn by [`chart-scale-out.py`](chart-scale-out.py) from the points in this card.
