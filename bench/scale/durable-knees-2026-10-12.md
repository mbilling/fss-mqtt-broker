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
  result (12 sites = 24k/node), which redb does not carry. There is therefore no 3-node
  rung above 1 site that took its whole offer. That is a mistake in choosing the ladder,
  not a finding.
- Audit driver: emqtt-bench 0.6.3 + the `qos1-driver/` audit extension, rebuilt for this run
  (archive sha256 `2cded28d…`, image `sha256:1194c6b2…`).
- `OBSERVE=1`: live Grafana was attached, so every host also ran the metrics shipper. The
  rig's own note says a published number may want `OBSERVE=0`.

## Result

Every rung is read twice, and the two readings are kept apart.

- **The reading rule: a rung holds if no message was lost and p99 is at or below 1 s,
  asserted from the brokers' counters.** No loss means the rung's identity ledger closes
  and no broker dropped anything. The rate is the brokers' own: each broker's
  `mqttd_publish_delivered_total` over its own window. A rung offered more than the
  cluster takes holds at the rate the brokers carried, which is below its offer. This is a
  reading, easy to state and to check. It is not a certification.
- **The rig's verdict** is the certification. GREEN also needs the offer met on schedule,
  a steady window, a load generator with headroom and valid driver-side evidence.

`summarize-curve.py` prints both: the verdict in its first table, the reading in
"Reading from the brokers' counters". In the table below, `delivered/s (brokers)` is the
figure the reading asserts. Every broker answered both window scrapes within 29 ms, so
that rate is known to ±0.03% on every rung. `delivered/s (drivers)` is the driver-side
figure the rig graded. Latencies are histogram bucket upper bounds, from the consumers.

| nodes | sites | offered msg/s | delivered/s (brokers) | of the offer | per node | delivered/s (drivers) | sent/s (publishers) | publishes late | p50 · p95 · p99 | reading | the rig's verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 25 | 150,000 | 150,023 | 100.0% | 15.0k | 149,071 | 150,259 | 4% | ≤25 · ≤100 · ≤500 ms | holds | NOT CARRIED |
| 10 | 35 | 210,000 | 192,217 | 91.5% | 19.2k | 199,072 | 199,910 (95%) | 41% | ≤50 · ≤500 · ≤500 ms | holds | NOT CARRIED |
| 10 | 40 | 240,000 | 210,547 | 87.7% | 21.1k | 219,004 | 219,082 (91%) | 53% | ≤100 · ≤500 · ≤500 ms | holds | NOT CARRIED |
| 10 | 25 (drift control) | 150,000 | 149,979 | 100.0% | 15.0k | 149,976 | 149,975 | 3% | ≤25 · ≤50 · ≤500 ms | holds | GREEN |
| 3 | 12 | 72,000 | 62,243 | 86.4% | 20.7k | 62,806 | 62,933 (87%) | 57% | ≤100 · ≤500 · ≤500 ms | holds | NOT CARRIED |
| 3 | 15 | 90,000 | 62,611 | 69.6% | 20.9k | 63,429 | 63,369 (70%) | 88% | ≤100 · ≤500 · ≤1000 ms | holds | NOT CARRIED |

Every 1-site rung (6,000 msg/s; six of them) is GREEN and holds, at p50 · p95 · p99 ≤ 5 ms.

### What held, and where the cluster stops taking more

- **10 nodes takes 150,000 msg/s in full: 15.0k msg/s per node**, p99 ≤ 500 ms, twice,
  57 minutes apart. The second pass is the rig's one GREEN rung above 1 site.
- **Past that it stops taking more at about 21k msg/s per node, at both sizes.** Offered
  210,000 and 240,000 msg/s, 10 nodes delivered 192,217 and 210,547 (19.2k and 21.1k per
  node). Offered 72,000 and 90,000, 3 nodes delivered 62,243 and 62,611 (20.7k and 20.9k
  per node): 18,000 more msg/s offered, 368 more delivered.
- **Nothing was lost on any of them, and p99 stayed at or below 1 s** (≤ 500 ms on every
  rung but the last, ≤ 1000 ms at 3 nodes and 15 sites). Past the ceiling the brokers
  acknowledge more slowly and a QoS 1 publisher that waits for its ack falls behind its
  schedule. The messages it did not send were never offered to the broker.
- **The 3-node ladder started above this store's ceiling**, so 3 nodes has no rung that
  took its whole offer above 1 site. See [The run](#the-run).

### Why the rig did not certify five of the six

The verdicts, verbatim, for the five NOT CARRIED rows in table order:

```text
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%)
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%); NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (95% of offer); PUBLISHERS LATE (41% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%); NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (91% of offer); PUBLISHERS LATE (53% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%); NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (87% of offer); PUBLISHERS LATE (57% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%); NOT STEADY (last aggregate sample below offer; per-site delivery did not remain within the offer band; publisher pacing, scrape timing and broker service are not isolated); OFFER NOT MET (70% of offer); PUBLISHERS LATE (88% of publishes behind schedule — end-to-end publisher timing; scheduling, network and broker service are not isolated)
```

- **Scrape-window uncertainty (all five).** The drivers' endpoint scrapes at the window
  edges were too far apart for the audited rates, so the summarizer fell back to the
  subscribers' logs. That is a limit of the driver-side evidence: the brokers' scrapes
  took at most 29 ms. On the first 25-site rung it is the only objection.
- **Offer not met, publishers late, not steady (four).** These are the rungs past the
  ceiling. The rig does not certify a rate the cluster was not offered on schedule.
- **Not the load generators.** No rung carries a DRIVER CORE SATURATED flag, and the
  least-idle driver host averaged 68–78% idle on every rung above 1 site.
- **Two readings disagree.** At 35 and 40 sites the fallback driver-side rate is 3.6% and
  4.0% **above** the brokers', past the tool's 2% tolerance, and `summarize-curve.py`
  prints `READINGS DISAGREE`. The rungs were not steady and the two readings do not cover
  the same seconds: the fallback takes the last 60 s of each subscriber's log, the
  brokers' windows were 66.0 and 64.8 s between their own scrapes. This card asserts the
  brokers'.
- Both ladders stopped after two consecutive NOT CARRIED rungs.

### Cross-node traffic

The share of publishes a broker forwarded to another broker
(`extract-lane-e.py`: Δ `mqttd_publish_forwarded_total` / Δ `mqttd_publish_received_total`
over the window), for the cluster and for the highest single broker, on every rung of both
campaigns of the day:

| campaign | arm | sites | publishes forwarded, cluster | highest single broker |
|---|---|---|---|---|
| durable | 10 nodes | 1 | 19.97% | 100.00% |
| durable | 10 nodes | 1 (control) | 10.00% | 100.00% |
| durable | 10 nodes | 25 | 13.60% | 32.00% |
| durable | 10 nodes | 35 | 16.84% | 37.87% |
| durable | 10 nodes | 40 | 15.48% | 31.40% |
| durable | 3 nodes | 1 · 1 (control) · 12 · 15 | 0.00% on each | 0.00% |
| durable | 10 nodes (drift) | 1 | 20.00% | 100.00% |
| durable | 10 nodes (drift) | 1 (control) | 10.00% | 100.00% |
| durable | 10 nodes (drift) | 25 | 13.60% | 32.00% |
| clean sessions | 10 nodes | 1 · 1 (control) · 13 · 14 | 0.00% on each | 0.00% |
| clean sessions | 3 nodes | 1 · 1 (control) · 4 · 4 (rep 2) · 5 | 0.00% on each | 0.00% |
| clean sessions | 10 nodes (drift) | 1 · 1 (control) · 13 | 0.00% on each | 0.00% |

The rig's crossing gate (every broker ≤ 0.5%) therefore passes at 3 nodes and on all three
clean-session arms, and fails on both durable 10-node arms:

```text
GATE nodes=3 PASS 4 rungs, cert=canary, max broker crossing 0.00% <= 0.5%, ladder stopped at its knee (1 skipped)
GATE nodes=10 FAIL sites-1 broker0 crossing 100.00% > 0.5%; … sites-25 broker2 crossing 32.00% > 0.5%; … sites-40 broker9 crossing 20.90% > 0.5%
```

Two things follow.

**(a) Some cross-node traffic did not stop it scaling.** On the loaded 10-node rungs
13–17% of publishes crossed between brokers, up to 31–38% on one broker. With that extra
work the cluster stopped taking more at 19.2–21.1k msg/s per node. 3 nodes, forwarding
nothing, stopped at 20.7–20.9k. The 10-node rungs did not run the same path as the 3-node
ones, and still reached about the same ceiling per node.

**(b) Why durable consumers forward at 10 nodes and clean-session consumers do not.**
This is an explanation from this run's evidence and the broker's documented design. It
was not tested by changing anything.

- **A persistent session lives on the broker that owns it, not on the broker its client
  dialled.** The owner is chosen from the client id by the placement ring; the broker the
  client dialled only relays the connection
  ([ADR 0005](../../docs/adr/0005-session-affinity.md); `relocation_target` in
  `crates/mqttd/src/conn.rs`). A clean session stays where it connected.
- **A `$share` publish is delivered by the broker that received it when that broker
  holds a member of the group, and forwarded when it holds none** (the default,
  `shared_prefer_local`). Every forwarded publish in this run carries
  `reason="shared-remote"`.
- **So the shape's coverage is by dialling, and persistent consumers are not where they
  dialled.** Each site's 20 consumers dial the brokers in rotation, 2 per broker at 10
  nodes. With clean sessions `mqttd_subscriptions` reads exactly that on every broker:
  2 at 1 site, 26 at 13, 28 at 14. With persistent sessions the same 20 consumers read
  0 · 2 · 1 · 3 · 3 · 0 · 4 · 2 · 2 · 3 across brokers 0–9 at 1 site. Brokers 0 and 5 hold
  no member, and they are the two brokers that forwarded 100% of their publishes: 2 of
  10 brokers, 19.97%. The control rung uses other client ids and reads
  1 · 2 · 1 · 4 · 4 · 3 · 0 · 1 · 3 · 1. Broker 6 holds none and forwarded 100%: 10.00%.
- **At 25 sites the forwarded share is a count of empty site × broker pairs.** The
  per-broker shares are whole 25ths: 2 · 7 · 8 · 3 · 1 · 2 · 3 · 1 · 3 · 4 sites on
  brokers 0–9, 34 of the 250 pairs, 13.60%. Broker 2 has 8 of 25, 32.00%.
- **It follows the client ids, not the hosts.** The drift arm is a cluster formed again
  from scratch after the 3-node arm. The same rung names give the same client ids, and
  the result is the same to the digit: subscriptions 40 · 43 · 20 · 72 · 61 · 59 · 50 ·
  68 · 45 · 42 per broker at 25 sites in both arms, 13.60% forwarded, 32.00% on broker 2.
- **The size of it is what hashing predicts.** If a site's 20 consumers land on N brokers
  independently, a broker holds none of them with probability (1 − 1/N)^20: 12.2% at 10
  nodes and 0.03% at 3. Weighted by the groups each broker reports in
  `mqttd_replica_groups_current` (49 · 51 · 35 · 69 · 50 · 60 · 46 · 53 · 56 · 43, 512 in
  all, read here as the groups it owns) it is 13.0% at 10 nodes. Measured: 13.60% at 10
  nodes, 0.00% at 3. Broker 2 reports the fewest groups (35), hosted the fewest consumers
  (20 of 500) and forwarded the most.

What was not checked:

- **The owner of each consumer was not recomputed from its client id.** The evidence holds
  no group-to-owner map, so the explanation rests on the per-broker counters above. That
  `mqttd_replica_groups_current` counts owned groups is an inference from its sum (512
  at both sizes), not from its description.
- **The 35- and 40-site shares (16.84%, 15.48%) are not whole counts of pairs.** Those
  rungs were not steady, so sites did not deliver at equal rates. Their per-site placement
  was not reconstructed.
- **The relay itself is not in this figure.** A persistent consumer whose owner is not
  the broker it dialled is served through that broker over the peer link, at 3 nodes as
  well as at 10. No counter in this run isolates that traffic.

Consequences for the rig:

- **The shape check's "predicted crossing ≈ 0%" is wrong for persistent consumers.** It
  counts the brokers the consumers dial. At 10 nodes about 51 persistent consumers per
  site would be needed before a broker without a member is rarer than the 0.5% gate
  ((1 − 1/10)^51 = 0.46%).
- **A prediction, not a result:** the 14 · 12 · 10 campaign
  ([`durable-scaleout-10-12-14.md`](durable-scaleout-10-12-14.md)) uses 28 persistent
  consumers per site. (1 − 1/N)^28 is 12.6% at 14 nodes, 8.7% at 12 and 5.2% at 10.

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
duplicated, none unexpected. Every broker reports 0 drops on every rung, and on every rung
the brokers' received total equals the ledger's sent count. **Past the
ceiling the cluster slows its publishers; it does not lose what it accepted.**

## What this does and does not show

- **Holds, by the reading rule:** every rung, with no message lost and p99 ≤ 1 s.
- **10 nodes takes 15.0k msg/s per node in full**, twice. The rig graded one of the two
  passes GREEN, in an arm whose crossing gate failed; it rejected the other only on
  driver-side scrape evidence.
- **The cluster stops taking more at about 21k msg/s per node on the default store**:
  19.2–21.1k at 10 nodes, 20.7–20.9k at 3 nodes, by the brokers' counters. It is read from
  rungs the rig graded NOT CARRIED and not steady, so it is a saturation plateau, not a
  certified knee. Where between 15k and about 20k per node the offer stops being taken in
  full was not located.
- **About the same ceiling per node at 3 and at 10 nodes**, and 10 nodes reached it while
  forwarding 13–17% of publishes between brokers. It is not a certified flat curve: no
  3-node rung took its whole offer, 3 and 10 are the only sizes, and each rung is one
  60 s window.
- **redb only.** These figures are not comparable with the log-store results, and say
  nothing about them.
- **Not a comparison with v1.1.0.** No A/B ran on this provisioning.

## Estimate: brokers needed to meet the offers that were not met

**This is arithmetic on the figures above, not a measurement.** It assumes per-node
capacity stays flat beyond 10 nodes. It was measured about flat only between 3 and 10,
with the caveats above.

Two per-node rates, from the brokers' counters:
- **Clean: 15,000 msg/s per node**, the GREEN rung (149,979 / 10). The rate to size on.
- **Ceiling: 21,000 msg/s per node**, where publishers already run late (10 nodes:
  210,547 / 10 = 21,055 and 192,217 / 10 = 19,222; 3 nodes: 62,611 / 3 = 20,870 and
  62,243 / 3 = 20,748). The lowest of the four is 19,222.

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
python3 summarize-curve.py .runs/knee-<stamp>/<arm>/results   # the rig's verdicts, then the broker-counter reading
python3 qos1-campaign/ledger-report.py .runs/knee-<stamp>/<arm>
python3 chart-scale-out.py qos1-candidate   # redraw the chart from the recorded points
```

A 3-node ladder for this store should start below the ceiling: 6 · 8 · 9 · 10 sites is
12k · 16k · 18k · 20k per node.

Chart: [`scale-out-qos1-candidate.svg`](../../docs/benchmarks/img/scale-out-qos1-candidate.svg),
drawn by [`chart-scale-out.py`](chart-scale-out.py) from the points in this card.
