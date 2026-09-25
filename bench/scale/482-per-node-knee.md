# #482 / #613 per-node knee — N=7 vs N=5 on ONE provisioning

Campaign card. Env: [`482-per-node-knee.env`](482-per-node-knee.env). Driver:
[`482-per-node-knee.sh`](482-per-node-knee.sh). Smoke:
[`482-knee-smoke.sh`](482-knee-smoke.sh). Issues:
[#482](https://github.com/mbilling/fss-mqtt-broker/issues/482),
[#613](https://github.com/mbilling/fss-mqtt-broker/issues/613). Diagnostic
only: do not copy its numbers into `docs/benchmarks/SCALE-CURVE.md`.

## The question

Is per-node QoS 0 `$share` capacity at N=7 close to per-node capacity at N=5?

## Why the 2026-09-15 Option B pair does not answer it

That pair ([`482-constant-driver-N5-N7-optionB.md`](482-constant-driver-N5-N7-optionB.md))
already had full consumer coverage — 7 consumers in one container cover
C + K − 1 = 1 + 7 − 1 = 7 brokers, so crossing was certified 0.00% on every
broker of every rung. The coverage gap `5b763e1` fixed was in a different run (10
consumers in 2 containers). What that pair lacked:

1. **A knee.** Both arms ran the same total-offer ladder to 300 000/s. N=7 passed
   everything (a lower bound); N=5 reached p99 ≤ 500 ms with 8% late publishers
   at 60 000/s per node — consistent with a limit, not a measured one.
2. **One provisioning.** Each size was its own provisioning. Two provisionings of
   nominally identical hardware have carried the same binary and shape 40% apart
   (ADR 0077 T4); within one, repeated rungs land within ~1%. A 10–20%
   efficiency question cannot be read through a 40% spread.
3. **The code under test.** `5761b1e` predates #640, which removed the O(peers)
   per-message term (~4.3% for 5→7, `docs/benchmarks/MEMBERSHIP-GATED-AB.md`).

## The measure

`E = (C₇ / 7) / (C₅ / 5)`, where `C_N` is the highest total offered rate (msg/s)
at which the N-node arm passes that rung **and every rung below it**. A rung
passes when it is settled, drained, p99 ≤ 1000 ms (`LANE_E_P99_BUDGET_MS`), not
PUBLISHERS LATE, no driver pinned, and the crossing gate certifies it (every
broker ≤ 0.5%).

Example: N=5 knees at 10 sites and N=7 at 14 sites →
`E = (420 000 / 7) / (300 000 / 5) = 60 000 / 60 000 = 1.00`.

The ladders step per-node offer by ~6 000/s (~10%), so `E` is resolved to roughly
±0.1. That separates linear from clearly sub-linear and no finer.

## Decision rules — declared before the run

| outcome | reading | next |
|---|---|---|
| `E ≥ 0.90` | 5→7 per-node capacity holds on this workload | post on #613/#482; close the membership-cost claim for QoS 0 lane E |
| `E ≤ 0.80`, crossing ≈ 0%, `eff_nodes` ≈ 7 | a genuine broker finding | profile what the busiest N=7 broker shows (hub busy by command, hottest core, `hub_queue_depth`) — not the hub by default |
| `0.80 < E < 0.90` | inside the ladder's resolution | a finer ladder around both knees, same provisioning discipline |
| no knee on either arm | lower bounds only | stop; report floors, claim nothing |
| closing N=7 differs from opening by > ~1% | the hardware drifted | the whole comparison is void |
| any arm's crossing gate fails | not Option B | stop reading that arm |

## Shape

Option B's per-site shape unchanged — 7 consumers in 1 container, 2 publisher
containers of 600 each (1 200 per site), 30 000/s per site, QoS 0, 200 B,
prefer-local on (the default; never `MQTTD_SHARED_PREFER_LOCAL=0`).

- **Brokers:** 7 × CCX23, provisioned once.
- **Drivers:** 18 × CCX33, the same machines for every arm. 18 is one driver per
  site at the top rung, so every rung of both arms carries at most 3 containers
  on its busiest driver (the offline shape check confirms it). At 10 drivers, N=7's
  60k/node rung put two sites on four drivers while N=5's put one on each — the
  matched rungs would have differed in driver load, at the density where the
  2026-09-15 pair flagged late publishers.
- **Quota:** 7 × 4 + 18 × 8 = 172 vCPU on 25 servers (project limit 200 / 30).

Ladders matched in per-node offer (per node = 30 000 × sites / N):

| per node/s | ~30k | 42k | 47–48k | 54–56k | 60k | 64–66k | 72–73k | 77–78k |
|---|---|---|---|---|---|---|---|---|
| N=5 sites | 5 | 7 | 8 | 9 | 10 ×2 | 11 | 12 | 13 |
| N=7 sites | 7 | 10 | 11 | 13 | 14 ×2 | 15 | 17 | 18 |

Each ladder opens with a 1-site rung that `LANE_E_CONTROL=1` repeats at its end.
The 60k/node rung runs twice on each arm. Lane E climbs every rung whatever the
one below did, so the ladders are the cost.

## Order — A/B/A on one provisioning

1. **Arm `1-n7`.** `run.sh full 7` provisions, bootstraps and runs the 7-node
   ladder, with `KEEP_INFRA=1` so the cluster outlives it.
2. **Arm `2-n5`.** `resize-cluster.sh` stops mqttd on all 7 brokers and clears
   every store; `bootstrap-cluster.sh` starts a fresh 5-node cluster on the first
   five (new PKI, founder-first, armed at majority 3). Brokers 6–7 stay stopped.
3. **Arm `3-n7-close`.** The same, back to 7: the `1` and `14` rungs, to compare
   with arm 1.

Every arm runs its own forwarding canary and calibration, exactly as a `run.sh`
size does. `482-per-node-knee.sh` traps every exit: it collects evidence from a
failed arm, then runs `teardown.sh`.

## Before paying

1. Offline, free:
   ```sh
   cd bench/scale
   python3 test-resize.py
   set -a && . ./482-per-node-knee.env && set +a
   PREFLIGHT_ONLY=1 ./482-per-node-knee.sh      # all three arms' shapes + self-tests
   python3 forward-canary.py local-proof --mqttd <candidate> --nodes 7
   python3 forward-canary.py local-proof --mqttd <candidate> --nodes 5
   ```
2. **A candidate binary containing #640** — `MQTTD_URL`, `MQTTD_SHA256`,
   `MQTTD_VERSION`, `BENCH_GIT_REF` pinned to one commit for every arm. v1.0.17
   does not contain #640.
3. **The smoke (paid, small):** `bash ./482-knee-smoke.sh` — 3 → 1 → 3 brokers
   on shared-vCPU hosts, one driver, a 1 000/s site. It is the only proof that
   bootstrap works on a host that has already run a cluster, and that the trapped
   teardown fires from this driver. Confirm the project is empty afterwards.
4. **The campaign:** `./482-per-node-knee.sh`, then gate every arm:
   `python3 extract-lane-e.py --crossing-gate 0.5 .runs/knee-<stamp>/<arm>/results`.

## Cost

`budgets.py` puts a lane E rung at ≤ 13.2 min worst case, and the three arms run 25 rungs; a normal
rung takes a fraction of its worst-case budget. Expect about 3 h of fleet time for
the three arms plus provisioning, 6 h worst case — 75–150 server-hours on 25
servers. Check current prices before starting.

## Known risk

`budgets.py` reports a MEASUREMENT RISK on every lane E rung: one worst-case edge
scrape (a 10 s timeout) exceeds the window-bracket tolerance (2% × 2 × 60 s =
2.4 s). That is a timeout budget, not a prediction — the 2026-09-15 pair lost no
rung to it. Read `bracket_ms` on arm 1's first rung; if it is voided on the
bracket, stop the campaign rather than paying for the rest.

## Read on every arm

Beyond the gate: `hub_dispatch` mean µs **and** count by command, the hottest
core, `rx_skew` / `eff_nodes`, peer in-flight and drops. At 0% crossing the
2026-09-15 pair's mean `cluster` dispatch still rose with load and was higher at
N=7 (63.9 vs 52.5 µs at 300 000/s) — find out which traffic that is.
