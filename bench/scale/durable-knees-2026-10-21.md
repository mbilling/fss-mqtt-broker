# Durable QoS 1 at 3 and 10 nodes on the 1.2.0 release candidate, both stores — one provisioning

Campaign card. Env: [`qos1-campaign/durable-knees-ec40721.env`](qos1-campaign/durable-knees-ec40721.env).
Driver: [`482-per-node-knee.sh`](482-per-node-knee.sh). The previous durable confirmation is
[`durable-knees-2026-10-12.md`](durable-knees-2026-10-12.md); the clean-session arm of this
release candidate is [`qos1-knees-2026-10-21.md`](qos1-knees-2026-10-21.md).

**The binary is an unreleased candidate.** `main` at `ec407218` (everything in 1.2.0), built
with the release recipe, published as the pre-release `bench-candidate-ec40721`, sha256
`e3c3dcbf…e036`. Arm 3 runs the 2026-10-12 build (`7d4eced`, sha256 `9111c1eb…c513`) on the
same hosts. Not the signed release pipeline's build; stamped `UNRELEASED-BINARY.txt`.

## The questions

1. Does the release candidate still carry durable QoS 1 as the 2026-10-12 build does, after
   #1052–#1060 (including #1053's takeover hold and #1057's resume fix)? Arm 3 is the same
   hosts on the earlier build.
2. What does the segment log store (ADR 0078, `MQTTD_REPLICA_STORE=log`, experimental) carry
   against the default redb store, on the same hardware?

## The run

- Run `.runs/knee-20261021T073222Z` (untracked), 2026-10-21 07:32–10:43 UTC, Hetzner `fsn1`.
  10 × CCX23 brokers (4 dedicated vCPU, local NVMe) + 20 × CCX33 drivers, one provisioning,
  torn down and leak-audited by the rig ("nothing leaked"). A first attempt that morning
  (`knee-20261021T065854Z`) failed before any measurement, on a host that never got its
  private network address.
- A site is 600 QoS 1 publishers at 10 msg/s (6,000 msg/s, 200 B) into a `$share` group of
  20 **persistent** QoS 1 consumers (`LANE_E_SESSION_EXPIRY=30`): every delivery waits for
  fsync and quorum replication. Default 2 copies. 60 s windows.
- Arms, in order: 1 redb, 10 nodes (1 · 25 · 35; 40 skipped by the stop rule); 2 redb, 3
  nodes with 8 drivers (1 · 8 · 10; 12 skipped); 3 the 2026-10-12 build, redb, 10 nodes (1 ·
  25 · 35); 4 the log store, 10 nodes (1 · 35 · 45 · 50); 5 the log store, 3 nodes (1 · 12 ·
  15 · 18); 6 redb again, 10 nodes (1 · 25 · 35), the drift control. Every arm starts from
  empty stores and closes with a repeated 1-site control.
- Audit driver: emqtt-bench 0.6.3 + `qos1-driver/` (archive sha256 `d765e908…`, image
  `sha256:ab924082…`). `OBSERVE=1`.

## Result

The reading rule: a rung holds if no message was lost and p99 is at or below 1 s, from the
brokers' counters; the rate is each broker's `mqttd_publish_delivered_total` over its own
window. The rig's verdict is the stricter certification beside it.

| arm | build · store | nodes | sites | offered msg/s | delivered/s (brokers) | per node | of offer | lost | p99 | the rig's verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | ec407218 · redb | 10 | 25 | 150,000 | 148,717 | 14.9k | 99.1% | 0 | ≤500 ms | NOT CARRIED |
| 1 | ec407218 · redb | 10 | 35 | 210,000 | 185,376 | 18.5k | 88.3% | 0 | ≤500 ms | NOT CARRIED |
| 2 | ec407218 · redb | 3 | 8 | 48,000 | 48,017 | 16.0k | 100.0% | 0 | ≤500 ms | NOT CARRIED |
| 2 | ec407218 · redb | 3 | 10 | 60,000 | 57,973 | 19.3k | 96.6% | 0 | ≤500 ms | NOT CARRIED |
| 3 | 7d4eced · redb | 10 | 25 | 150,000 | 149,613 | 15.0k | 99.7% | 0 | ≤500 ms | NOT CARRIED |
| 3 | 7d4eced · redb | 10 | 35 | 210,000 | 185,025 | 18.5k | 88.1% | 0 | ≤500 ms | NOT CARRIED |
| 4 | ec407218 · log | 10 | 35 | 210,000 | 209,989 | 21.0k | 100.0% | 0 | ≤50 ms | GREEN |
| 4 | ec407218 · log | 10 | 45 | 270,000 | 270,034 | 27.0k | 100.0% | 0 | ≤500 ms | GREEN |
| 4 | ec407218 · log | 10 | 50 | 300,000 | 298,530 | 29.9k | 99.5% | 0 | ≤500 ms | NOT CARRIED |
| 5 | ec407218 · log | 3 | 12 | 72,000 | 72,003 | 24.0k | 100.0% | 0 | ≤50 ms | GREEN |
| 5 | ec407218 · log | 3 | 15 | 90,000 | 90,003 | 30.0k | 100.0% | 0 | ≤500 ms | GREEN |
| 5 | ec407218 · log | 3 | 18 | 108,000 | 104,264 | 34.8k | 96.5% | 0 | ≤500 ms | NOT CARRIED |
| 6 | ec407218 · redb | 10 | 25 (drift control) | 150,000 | 149,405 | 14.9k | 99.6% | 0 | ≤500 ms | GREEN |
| 6 | ec407218 · redb | 10 | 35 (drift control) | 210,000 | 184,293 | 18.4k | 87.8% | 0 | ≤500 ms | NOT CARRIED |

Every 1-site rung (6,000 msg/s; twelve of them) is GREEN and holds. **444,664,692 messages
ledgered, none missing, unacknowledged or duplicated, and no broker dropped one.** A rung
below 100% of the offer held what it delivered: the brokers acknowledged more slowly, the
publishers fell behind their schedule, and nothing was lost.

### What it shows

- **No regression since 2026-10-12.** On the same hosts, the 2026-10-12 build and the
  release candidate deliver the same on redb: 149,613 and 148,717 msg/s at 25 sites, 185,025
  and 185,376 at 35, where both stop taking more. The drift control (149,405 and 184,293)
  matches arm 1, so the hosts did not drift.
- **redb stops at about 18.5k msg/s per node at 10 nodes and 19.3k at 3.** The 2026-10-12
  provisioning measured 19.2–21.1k and 20.7–20.9k; the difference is these hosts, as arm 3
  shows, not the build.
- **The log store carries at least 46% more on the same hardware.** At 10 nodes it holds
  270,000 msg/s (27k per node) GREEN, against redb's ceiling of about 185,000, and reaches
  298,530 at the top of its ladder. At 3 nodes it holds 90,000 msg/s (30k per node) GREEN,
  against redb's 57,973. These are ADR 0078 criterion 3 ("the knee moves") on one
  provisioning, certified; the store stays experimental until criteria 1 and 5 are met.

### Cross-node traffic

Durable QoS 1 forwards between brokers by design: a persistent session lives on the broker
that owns it, not on the one its client dialled. At 10 nodes 13–17% of publishes were
forwarded on the loaded rungs, up to 40% on the busiest broker (`broker2` on every arm); at 3
nodes none were. The rig's crossing gate, built for clean sessions, therefore fails every
10-node arm here, as on 2026-10-12.

## Commands

```sh
cd bench/scale
set -a && . qos1-campaign/durable-knees-ec40721.env && set +a
export QOS1_DRIVER_ARCHIVE=<the audit driver archive above>
./482-per-node-knee.sh
for arm in 1-n10 2-n3 3-n10 4-n10 5-n3 6-n10; do
  python3 summarize-curve.py .runs/knee-20261021T073222Z/$arm/results
done
```
