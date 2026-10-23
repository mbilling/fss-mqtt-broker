# Write elision on main at 3 nodes: the head rule narrowed, against writing first, on both stores — one provisioning

Campaign card for #1083 (ADR 0089 T5–T8 evidence). Env:
[`qos1-campaign/elision-ab-n3-main.env`](qos1-campaign/elision-ab-n3-main.env). Driver:
[`482-per-node-knee.sh`](482-per-node-knee.sh). The sweep before Amendment 1 A2/A5 is
[`elision-window-n3-2026-10-21.md`](elision-window-n3-2026-10-21.md).

**The binary is an unreleased candidate.** `main` at `ff3607da` (ADR 0089 T1–T8: the head rule
narrowed to an append in flight and per topic, the slow-subscriber backoff), built with the
release recipe and published as the pre-release `bench-candidate-ff3607d`, sha256
`58d26b30…6722`. It is not the signed release pipeline's build; the run is stamped
`UNRELEASED-BINARY.txt`.

## The questions

1. **Short windows.** Do A2 and A5 rescue a short window? Before them, 5 ms elided 1.5% at 10
   sites.
2. **The knee.** Do they lift the knee with a 50 ms window? Before them, the 50 ms arm elided
   38% at 15 sites and carried 71,268 msg/s.
3. **The defaults.** What do 50 ms and the default prefer-local delivery carry on redb, and how
   does the segment log store compare on the same hosts?

## The run

- **Run:** `.runs/knee-20261022T152322Z` (untracked), 2026-10-22 15:23–17:28 UTC, Hetzner
  `fsn1`.
- **Hosts:** 3 × CCX23 brokers and 8 × CCX33 drivers, one provisioning. The rig tore them
  down and audited for leaks: 0 servers, empty state.
- **A site:** 600 QoS 1 publishers at 10 msg/s (6,000 msg/s, 200 B) into a `$share` group of
  20 persistent consumers, 2 copies, 60 s windows. Every arm starts from empty stores and
  closes with a 1-site control.
- **Ladders:** `LANE_E_STOP_AFTER_FAILS=3`, so the start-up overflow of #1078 cannot end a
  ladder by itself.
- **Arms run:**

  | arm | window | delivery | store |
  |---|---|---|---|
  | 1 | 0 (writing first) | crossing forced (`MQTTD_SHARED_PREFER_LOCAL=0`) | redb |
  | 2 | 5 ms | crossing forced | redb |
  | 3 | 50 ms | crossing forced | redb |
  | 4 | 50 ms | prefer-local (the default) | redb |
  | 5 | 0 | prefer-local | segment log |

- **Not run:** arm 6 (log store + 50 ms + prefer-local, the 1.3.0 defaults) and arm 7 (the
  drift control). Arm 6's top rung of 24 sites needed 9 containers on the busiest driver,
  against a budget of 8. The rig refused the arm's shape before running it, and the campaign
  ended there. The combined 1.3.0 defaults are measured in the confirmation run (#1083).
- **Not covered:** a lagging subscriber. The audit driver acknowledges at once, so criterion 2
  rests on the hub cost-model tests and DURABLE-PATH Result 4.

## Result

The rate is each broker's `mqttd_publish_delivered_total` over its own window. Elision figures
are the brokers' ADR 0089 counters over the same window. The ledger columns are sent · missing
· unacknowledged · duplicates.

| arm | window | delivery · store | sites | delivered/s | of offer | elided | write mean | p99 | ledger | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0 | crossing · redb | 10 | 57,584 | 96.0% | — | 36.4 ms | ≤500 ms | 19.3M · 280 · 280 · 0 | FAILED ¹ |
| 1 | 0 | crossing · redb | 15 | 59,723 | 66.4% | — | 66.2 ms | ≤1000 ms | 18.1M · 0 · 0 · 0 | NOT CARRIED |
| 2 | 5 ms | crossing · redb | 10 | **59,997** | 100.0% | **49.9%** | 12.1 ms | ≤50 ms | 6.9M · 0 · 0 · 0 | **GREEN** |
| 2 | 5 ms | crossing · redb | 15 | 78,695 | 87.4% | 36.2% | 45.2 ms | ≤500 ms | 22.2M · 0 · 0 · 0 | NOT CARRIED |
| 3 | 50 ms | crossing · redb | 10 | 60,001 | 100.0% | 99.0% | 8.9 ms | ≤500 ms | 6.8M · 0 · 0 · 0 | NOT CARRIED |
| 3 | 50 ms | crossing · redb | 15 | **86,277** | 95.9% | **94.3%** | 49.9 ms | ≤500 ms | 23.6M · 0 · 0 · 0 | NOT CARRIED |
| 3 | 50 ms | crossing · redb | 20 | 85,610 | 71.3% | 92.1% | 77.1 ms | ≤500 ms | 24.3M · 171 · 171 · 117 | FAILED ² |
| 4 | 50 ms | prefer-local · redb | 10 | 60,000 | 100.0% | 99.6% | 4.7 ms | **≤10 ms** | 6.8M · 0 · 0 · 0 | **GREEN** |
| 4 | 50 ms | prefer-local · redb | 15 | **89,992** | **100.0%** | 91.0% | 16.2 ms | ≤500 ms | 10.4M · 0 · 0 · 0 | **GREEN** |
| 4 | 50 ms | prefer-local · redb | 20 | **110,426** | 92.0% | 85.2% | 77.6 ms | ≤1000 ms | 30.2M · 0 · 0 · 0 | NOT CARRIED |
| 5 | 0 | prefer-local · log | 15 | 90,020 | 100.0% | — | 18.9 ms | ≤500 ms | 10.3M · 0 · 0 · 0 | NOT CARRIED |
| 5 | 0 | prefer-local · log | 20 | 95,256 | 79.4% | — | 36.0 ms | ≤1000 ms | 26.7M · 0 · 0 · 0 | NOT CARRIED |

Every 1-site control is GREEN at exactly 6,000 msg/s: ten of them, an opening and a closing
one per arm.

**No acknowledged message was lost.** On both FAILED rungs the missing count equals the
unacknowledged count: publishes the brokers refused (`append-backlog-full`) and never
acknowledged, which their publishers were owed a resend of. The 117 duplicates at 20 sites are
QoS 1 redeliveries.

¹ The start-up overflow of #1078: one site-0 consumer's append lane filled within seconds of
the arm's first loaded rung, with elision off.

² Past the knee, with acknowledgements withheld on several sites.

### What it shows

- **A2 and A5 rescue a short window.** At 10 sites, 5 ms elided 49.9% against 1.5% before
  them. Writes fell from 31 ms to 12 ms, and the rung carried the full offer GREEN at p99
  ≤ 50 ms. About half the deliveries still time out (the mean hold is 1.4 ms, but the tail is
  longer), but a timeout no longer keeps a session off the fast path.
- **They lift the knee.** At 15 sites with crossing forced, the 50 ms arm carried 86,277 msg/s
  with 94% elided (71,268 and 38% before them). That is **+44%** over writing first on the
  same hosts (59,723). The 5 ms arm carried 78,695 (+32%).
- **50 ms with the default delivery, on redb:** 15 sites (90,000 msg/s, 30k per node) GREEN at
  the full offer, and 110,426 at 20 sites. redb alone stopped at about 19.3k per node at 3
  nodes on 2026-10-21, so this is about +55% per node on the same store. It matches what the
  log store certified without elision that day (30k per node).
- **The log store without elision** carried the full offer at 15 sites, but 95,256 at 20 (79%),
  against redb + 50 ms's 110,426 (92%) on the same hosts.

## Commands

```sh
cd bench/scale
set -a && . qos1-campaign/elision-ab-n3-main.env && set +a
export QOS1_DRIVER_ARCHIVE=<the audit driver archive>
./482-per-node-knee.sh
for arm in 1 2 3 4 5; do
  python3 summarize-curve.py .runs/knee-20261022T152322Z/$arm-n3/results
done
```
