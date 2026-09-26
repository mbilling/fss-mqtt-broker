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

## Bad hosts — swapped, not paid for

A fresh provisioning can draw a host that is broken for this workload. On
2026-09-25 one driver of eighteen (`bench-driver-9`) held a core at 98–100%
softirq under the one-site load its peers carried at < 20% on their hottest
core; its scrape took ~7.5 s (voiding every rung it loaded on the window
bracket) and its site under-offered by ~29%. The harness now handles this
without destroying the provisioning:

- **Before the ladder**, `lane_e_driver_gate` bursts every driver at a rung's
  per-container rate, all at once, and samples every host with `mpstat -P ALL`.
  A driver whose hottest core averages ≥ `LANE_E_DRIVER_SOFTIRQ_MAX` (80) %soft,
  or that offers < 97% of the rate, is swapped and gated again once.
- **After every rung**, the rung's own CPU samples get the same test. A rung that
  loaded a pinned driver is moved to `laneE/voided-sites-…` (outside the
  extractor's `sites-*` pattern, with `VOIDED.txt`), the driver is swapped, and
  the rung runs again once.
- **Swapping** is `replace-node.sh`: `tofu apply -replace` for that one server
  with the exact variables `run.sh` recorded (`tf-apply-args-<N>.sh`). Private
  IPs are fixed per index, so only the public IP and host key change; a driver
  gets the arm's client certificates and is ready for the next rung. Every swap
  is logged in the arm's `REPLACED.txt`.
- **A broker** is judged against its peers in the same burst (pinned while the
  median broker's hottest core is under half the threshold — softirq rising on
  every broker is the broker working). It is never swapped under a running arm:
  that would change the hardware the arm measures. The gate names it in
  `bad-brokers.txt` and stops the arm; `482-per-node-knee.sh` swaps it and
  re-forms that arm once (`<arm>-r2`) before any rung.

The window-edge bracket risk `budgets.py` reports is otherwise real only on a
bad host: healthy brokers scraped in 13–19 ms and healthy drivers in ≤ 1.5 s at
every rung measured.

## 2026-09-25 attempts — no knee yet

1. **Canary failed, correctly.** The founder's re-arm restart set off a
   membership storm; two brokers held mqttd-4 DEAD from 19:07:22 to 19:07:53 and
   the forwarding control scraped in between. Forwarding itself was exact (600
   shared-remote forwards per broker). Fixed: lane E waits for a full, stable
   mesh first (`await_full_mesh`); the next attempt needed ~50 s of it.
2. **Stopped on the bracket rule.** Mesh and canary passed; 1 and 7 sites were
   clean (30 000 and 210 000 /s received, crossing 0.00%, `rx_skew` 1.00,
   brackets 45 / 66 ms). From 10 sites every rung was voided by the one pinned
   driver above. Evidence: `.runs/knee-20260925T203528Z/1-n7/`.

## 2026-09-26 result — per-node capacity holds from 5 to 7; no knee inside either ladder

Run `.runs/knee-20260925T235258Z` (untracked). Candidate `bench-candidate-0a08187`,
sha256 `66dd3e62…f32f`; 7 × CCX23 + 18 × CCX33, one provisioning, arms 7 → 5 → 7.
No host was swapped (driver gate passed on every arm; no rung pinned a driver).

**Gates.** `extract-lane-e.py --crossing-gate 0.5`: `GATE nodes=7 PASS 11 rungs`,
`GATE nodes=5 PASS 11 rungs`, closing `GATE nodes=7 PASS 3 rungs`, all
`cert=canary`, max broker crossing 0.00%. Every rung settled and drained; zero
drops; zero peer in-flight; `rx_skew` 1.00–1.01 on every rung; busiest driver
≥ 68% idle; brackets 71–129 ms.

**Drift.** Closing 14-site rung 419 977/s vs opening 419 996 and 420 160/s
(≤ 0.04%); hub publish 7.8 µs vs 7.9 / 7.8 µs; busiest broker idle 25% vs
23 / 24%. The 1-site controls deliver 30 000/s everywhere; their publish µs
(8.5–9.6) is low-load noise. The comparison stands.

| per node | N=5 rung | p99 | N=7 rung | p99 |
|---|---|---|---|---|
| 42k | 7 sites, 210 000/s | ≤ 5 ms | 10 sites, 300 000/s (42.9k) | ≤ 5 ms |
| 48k / 47k | 8, 240 000 | ≤ 5 ms | 11, 330 000 | ≤ 5 ms |
| 54k / 56k | 9, 270 000 | ≤ 25 ms | 13, 390 000 | ≤ 500 ms |
| 60k | 10 ×2, 300 000 | ≤ 500 ms | 14 ×2, 420 000 | ≤ 500 ms |
| 66k / 64k | 11, 330 000 | ≤ 500 ms | 15, 450 000 | ≤ 500 ms |
| 72k / 73k | 12, 360 000 | ≤ 1000 ms | 17, 510 000 | ≤ 1000 ms |
| 78k / 77k | 13, 390 000 | ≤ 1000 ms | 18, 540 000 | ≤ 1000 ms |

Every rung of both ladders passes. By the declared rules there is **no knee**, so
`E` is not computed: the floors are `C₅ ≥ 390 000/s` (78 000 per node) and
`C₇ ≥ 540 000/s` (77 142 per node). What the matched rungs do show is the
per-node cost: the same p99 bucket at every matched rate from 60k/node up, and
mean hub publish busy per broker **lower** at N=7 (0.475 / 0.557 / 0.586 of a
core at 60 / 73 / 77k per node) than at N=5 (0.556 / 0.590 / 0.676 at 60 / 72 /
78k). Two more brokers cost nothing measurable per node on this workload.

**The mean `cluster` dispatch rise is not load.** At 0% crossing `cluster`
dispatches run at < 1 per second per broker; their mean µs is a handful of
gossip calls, and their busy fraction is 0.000.

**Where the per-node limit is forming — same hosts, both sizes.** Ingress is equal
on every broker (60.0k each at 60k/node), yet brokers 0 and 3 spend ~14 µs per
publish against ~5 µs on the others, holding ~0.85 of a core in the hub; broker 2
joins them at 77–78k/node. Those are exactly the brokers whose CPU 1 — the core
taking the NIC softirq (58–71%) — sits at 0–3% idle, while the other brokers'
CPU 1 keeps 20–41%. The hub's dispatch timer counts wall time, so preemption by
interrupt work reads as dispatch cost. This is the host-interrupt ceiling already
documented for QoS 1 (`docs/benchmarks/QOS1-SCALE-CURVE.md`), appearing per host,
independent of N; spreading the softirq is a settled dead end (#505/#507/#508).

**Next, if a knee is wanted:** extend both ladders past 78k/node on one
provisioning (N=7 needs 19+ sites and a driver per site); expect the first failing
rung on the brokers whose interrupt core saturates, at the same per-node rate at
both sizes.

## Read on every arm

Beyond the gate: `hub_dispatch` mean µs **and** count by command, the hottest
core, `rx_skew` / `eff_nodes`, peer in-flight and drops. At 0% crossing the
2026-09-15 pair's mean `cluster` dispatch still rose with load and was higher at
N=7 (63.9 vs 52.5 µs at 300 000/s) — find out which traffic that is.
