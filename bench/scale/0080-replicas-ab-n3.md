# ADR 0080 T6 — R=3 against R=2 on ONE provisioning

Campaign card. Env: [`qos1-campaign/durable-replicas-ab-n3.env`](qos1-campaign/durable-replicas-ab-n3.env)
(shape: [`durable-calibrate-n3.env`](qos1-campaign/durable-calibrate-n3.env)). Driver:
[`482-per-node-knee.sh`](482-per-node-knee.sh). Issue:
[#705](https://github.com/mbilling/fss-mqtt-broker/issues/705). Diagnostic only: an
unreleased binary, so do not copy its numbers into `docs/benchmarks/SCALE-CURVE.md`.

## The question

ADR 0080 predicted that at N = 3, R = 2 carries about 3 / 2 = 1.5× the durable QoS 1 rate
of R = 3, because each node's writer handles R / N of every message (3 / 3 at R = 3, 2 / 3
at R = 2). Does it?

## The run

- Run `.runs/knee-20260930T084513Z` (untracked), 2026-09-30, Hetzner `nbg1` (`fsn1` had no
  CCX capacity). 3 × CCX23 brokers, 8 × CCX33 drivers, one provisioning.
- Candidate `bench-candidate-039232d` (main at 039232d, ADR 0080 T1–T5), sha256
  `23962a42…f449`. Log store (`MQTTD_REPLICA_STORE=log`).
- Arm 1: `MQTTD_REPLICAS=3`, sites 1 · 12 · 15 · 18, then the 1-site control.
  Arm 2: `MQTTD_REPLICAS=2`, sites 1 · 12 · 15 · 18 · 21, then the 1-site control.
  Each arm is a fresh cluster on empty stores, founded at its factor.
- A site is 600 `QoS` 1 publishers at 10 msg/s (6,000 msg/s) into a `$share` group of 20
  durable `QoS` 1 consumers. 21 sites (126,000 msg/s) is the fleet's ceiling.
- An earlier attempt the same morning (`.runs/knee-20260930T072703Z`, other hosts) ran the
  full R = 3 arm (1 · 6 · 9 · 12 · 15 · 18 · 21) and died in the R = 2 arm when the
  operator's laptop changed network route. Its R = 3 arm agrees with this one: GREEN
  through 15 sites, 18 never steady (105,583/s), 21 delivered 108,832/s.

## Result

| sites | offered msg/s | R = 3 delivered/s | R = 3 p50 · p95 · p99 | R = 3 verdict | R = 2 delivered/s | R = 2 p50 · p95 · p99 | R = 2 verdict |
|---|---|---|---|---|---|---|---|
| 1 | 6,000 | 6,000 | ≤5 · ≤5 · ≤10 ms | GREEN | 6,000 | ≤5 · ≤5 · ≤5 ms | GREEN |
| 12 | 72,000 | 72,009 | ≤10 · ≤25 · ≤50 ms | GREEN | 71,999 | ≤5 · ≤25 · ≤25 ms | GREEN |
| 15 | 90,000 | 89,997 | ≤25 · ≤100 · ≤100 ms | GREEN | 90,012 | ≤25 · ≤50 · ≤100 ms | GREEN |
| 18 | 108,000 | 104,716, never steady | ≤100 · ≤500 · ≤500 ms | not carried | 107,355, steady in 16 s | ≤25 · ≤100 · ≤500 ms | not carried (scrape window, 5% publishes late) |
| 21 | 126,000 | — | — | — | 122,594, never steady | ≤100 · ≤500 · ≤500 ms | not carried |

No rung lost, duplicated or dropped a message in either arm; both 1-site controls
repeated their opening rung.

- **Certified knee: equal.** Both factors are GREEN through 15 sites (90,000 msg/s). R = 2
  held 18 sites steady, where R = 3 never settled, but the rung is not certified: the
  endpoint scrape window's uncertainty exceeded 2% and 5% of publishes ran behind schedule.
- **Ceiling: R = 2 about 17% higher.** Most delivered: R = 3 104,716/s (108,832/s on the
  earlier hosts), R = 2 122,594/s, against the same 126,000 offer ceiling.
- **Writer work: a third less, as predicted.** Writer ops/s per node at the same delivered
  rate:

  | sites | delivered/s | R = 3 writer ops/s per node | R = 2 writer ops/s per node | R = 3 disk MB/s | R = 2 disk MB/s |
  |---|---|---|---|---|---|
  | 12 | 72,000 | 80,951–80,968 | 54,560–54,938 | 45.7–48.0 | 32.8–37.1 |
  | 15 | 90,000 | 99,025–99,037 | 62,335–69,599 | 56.1–57.2 | 39.3–41.6 |

## Why not 1.5×

The prediction assumed the writer, one fsync'd store per node, sets the ceiling. It does
not on these hosts. At 15 sites and above the brokers are **CPU-bound** at both factors,
and nothing else is near a limit:

| sites | R = 3 broker CPU busy | R = 2 broker CPU busy | busiest driver | broker disk util |
|---|---|---|---|---|
| 15 | 81 · 88 · 93% | 79 · 80 · 92% | 31% | < 27% |
| 18 | 89 · 89 · 95% | 83 · 82 · 94% | 35% | < 27% |
| 21 | — | 88 · 88 · 94% | 39% | < 27% |

R = 2 removes a third of the replication work, and that is what buys the 17%: the rest of
each message's CPU (receiving the publish, routing it to the owner, `$share` delivery and
its acknowledgement, the TLS on every hop) does not depend on R. The durable ceiling at
N = 3 on CCX23 is a CPU ceiling, and lowering it is the next lever, not the copy count.

## Cost

About €8.80–11.00 net in all, at €2.19/h for the 11 servers: this run (1 h 14 min, billed
as 2 h, €4.38), the attempt it replaced (1 h 15 min, €4.38), and a first provisioning in
`fsn1` that failed on capacity within a minute (at most one billed hour, €2.19).
