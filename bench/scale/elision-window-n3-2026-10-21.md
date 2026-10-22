# Durable QoS 1 write elision at 3 nodes: the window from 0 to 50 ms, crossing nodes — one provisioning

Campaign card for ADR 0089 T5, first sweep. Env:
[`qos1-campaign/elision-window-n3-fb90e03.env`](qos1-campaign/elision-window-n3-fb90e03.env).
Driver: [`482-per-node-knee.sh`](482-per-node-knee.sh). The elision-off reference at this shape
is [`durable-knees-2026-10-21.md`](durable-knees-2026-10-21.md) (prefer-local on).

**The binary is an unreleased candidate.** Branch `adr-0089-t4` at `fb90e03a`, which is
ADR 0089 T1 + T2 + T4 on 1.2.0. It was built with the release recipe and published as the
pre-release `bench-candidate-fb90e03`, sha256 `6232dadc…f58b`. It is not the signed release
pipeline's build, and the run is stamped `UNRELEASED-BINARY.txt`.

## The question

With `durable.write_elision_ms` set, how much of durable QoS 1 skips the write, and what does
that do to the knee, when the subscriber's PUBACK must cross nodes? Which window does it take?

## The run

- **Run:** `.runs/knee-20261021T173147Z` (untracked), 2026-10-21 17:31–20:31 UTC, Hetzner
  `fsn1`.
- **Hosts:** 3 × CCX23 brokers and 8 × CCX33 drivers, one provisioning. The rig tore them
  down and audited for leaks ("nothing leaked").
- **Earlier attempts:** two that day died before measuring anything elision-related, from
  rig defects fixed in #1075 (the clock gate) and #1076 (withheld acks ended the run).
- **A site:** 600 QoS 1 publishers at 10 msg/s (6,000 msg/s, 200 B) into a `$share` group of
  20 **persistent** QoS 1 consumers, `LANE_E_SESSION_EXPIRY=30`. Store redb (the default), 2
  copies, 60 s windows.
- **Crossing:** every arm except 6 sets `MQTTD_SHARED_PREFER_LOCAL=0`, so a delivery goes to
  any member. Two thirds of publishes were forwarded to another node before the subscriber's
  PUBACK could discharge them.
- **Arms, in order:**

  | arm | window | crossing | ladder (sites) |
  |---|---|---|---|
  | 1 | 0 (off) | forced | 1 · 8 · 10 |
  | 2 | 5 ms | forced | 1 · 10 · 15 |
  | 3 | 10 ms | forced | 1 · 10 · 15 |
  | 4 | 20 ms | forced | 1 · 10 · 15 · 20 |
  | 5 | 50 ms | forced | 1 · 10 · 15 · 20 |
  | 6 | 20 ms | prefer-local on | 1 · 15 · 20 |
  | 7 | 0 (off) | forced | 1 · 10 (the drift control) |

  Ladders end two failing rungs in. Each arm starts from empty stores and closes with a
  repeated 1-site control.
- **Audit driver:** emqtt-bench 0.6.3 + `qos1-driver/` (archive sha256 `d765e908…`). `OBSERVE=1`.

## Result

The rate is each broker's `mqttd_publish_delivered_total` over its own window. The elision
columns are the brokers' ADR 0089 counters over the same window:
- **elided:** `mqttd_durable_elided_total` against durable appends;
- **fallbacks:** `mqttd_durable_elision_fallback_total`, all `timeout` here;
- **hold:** p99 of `mqttd_durable_elision_hold_seconds`, censored at the window;
- **write:** mean `mqttd_durable_append_latency_seconds`.

| arm | window | sites | offered | delivered/s | of offer | elided | fallbacks | hold p99 | write | p99 | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0 | 8 | 48,000 | 46,569 | 97.0% | — | — | — | 15.0 ms | ≤50 ms | FAILED ¹ |
| 1 | 0 | 10 | 60,000 | 60,005 | 100.0% | — | — | — | 35.8 ms | ≤500 ms | GREEN |
| 2 | 5 | 10 | 60,000 | 57,148 | 95.2% | 1.5% | 713 | ≤5.8 ms | 31.2 ms | ≤100 ms | FAILED ¹ |
| 2 | 5 | 15 | 90,000 | 63,780 | 70.9% | 4.7% | 7,212 | ≤5.8 ms | 56.5 ms | ≤2000 ms | FAILED ² |
| 3 | 10 | 10 | 60,000 | 57,069 | 95.1% | 1.6% | 419 | ≤8.8 ms | 33.0 ms | ≤100 ms | FAILED ¹ |
| 3 | 10 | 15 | 90,000 | 63,913 | 71.0% | 6.8% | 9,232 | ≤8.8 ms | 56.3 ms | ≤2000 ms | FAILED ² |
| 4 | 20 | 10 | 60,000 | 59,982 | 100.0% | 6.1% | 850 | ≤19.7 ms | 34.7 ms | ≤100 ms | GREEN |
| 4 | 20 | 15 | 90,000 | 66,789 | 74.2% | 25.9% | 21,318 | ≤19.7 ms | 61.0 ms | ≤2000 ms | FAILED ² |
| 4 | 20 | 20 | 120,000 | 60,459 | 50.4% | 21.8% | 22,419 | ≤19.7 ms | 92.6 ms | ≤2000 ms | FAILED ² |
| 5 | 50 | 10 | 60,000 | **59,997** | 100.0% | **98.1%** | 59 | ≤19.7 ms | **6.8 ms** | **≤25 ms** | GREEN |
| 5 | 50 | 15 | 90,000 | **71,268** | 79.2% | 38.3% | 120 | ≤29.6 ms | 59.6 ms | ≤1000 ms | FAILED ² |
| 5 | 50 | 20 | 120,000 | 62,183 | 51.8% | 28.4% | 87 | — | 92.2 ms | ≤2000 ms | FAILED ² |
| 6 | 20, local | 15 | 90,000 | 67,187 | 74.7% | 15.7% | 32,040 | ≤19.7 ms | 58.4 ms | ≤1000 ms | NOT CARRIED |
| 6 | 20, local | 20 | 120,000 | 61,999 | 51.7% | 20.7% | 69,210 | ≤19.7 ms | 93.8 ms | ≤2000 ms | NOT CARRIED |
| 7 | 0 | 10 | 60,000 | 57,099 | 95.2% | — | — | — | 33.0 ms | ≤100 ms | FAILED ¹ |

Every 1-site rung is GREEN at exactly 6,000 msg/s: fourteen of them, an opening and a closing
control in each arm. At 1 site every window elided 99.4–99.7%, with the PUBACK held 0.5 ms
on average.

**No acknowledged message was lost.** On every FAILED rung the identity ledger's missing count
equals its unacknowledged count and the brokers' `append-backlog-full` drops (143 to 1,025):
publishes the broker refused and never acknowledged, so their publishers were owed a resend.
The 20-site rungs of arms 4 and 5 carry 2 and 127 duplicates, which QoS 1 allows.

¹ **A start-up overflow, not the window.** On the first loaded rung after a cluster start, in
5 of the 6 forced-crossing arms (not arm 4), one site-0 consumer session on broker 0 filled its
256-job append lane within about 4 s. Its acks were withheld (`append lane full`, issue #242's
bound) and the load tool does not reconnect dropped publishers. It happens with elision off
(arms 1 and 7), and never with prefer-local on (arm 6). See "Open" below.

² Past the knee: many sites' acks withheld, publishers behind schedule, delivery below offer.

### What it shows

- **The window has to cover the PUBACK tail under load, not its mean.**
  - At 10 sites, windows of 5 to 20 ms elided 1.5–6.1%, and 50 ms elided 98.1%.
  - The p99 hold at 10–15 sites is about 20–30 ms (p99.9 30–44 ms in the 50 ms arm), against a
    mean under 3 ms.
- **The path is bistable under ADR 0089's original D5 head rule.**
  - A few timeouts put a session on the durable path, and D5 keeps it there while anything
    written for it is unacknowledged.
  - Writes then slow to 30–90 ms, and a session receiving 300 msg/s almost never sees the gap it
    needs to return.
  - At 50 ms almost nothing times out (59 fallbacks), writes fall to 6.8 ms, and the arm stays
    on the fast path.
- **What elision buys at the knee.**
  - At 10 sites every clean arm carries the full offer, and the gain is latency: p99 ≤500 ms
    off, ≤100 ms at 20 ms, ≤25 ms at 50 ms.
  - At 15 sites the 50 ms arm carries 71,268 msg/s, against 63,780–66,789 for 5–20 ms (+7 to
    +12%).
  - The knee is between 10 and 15 sites for every window: 20–24k msg/s per node with crossing
    forced.
- **What still binds at 50 ms.** At 15–20 sites the share elided falls to 28–38% with only
  87–120 timeouts. Deliveries are ineligible because their session has a written delivery still
  unacknowledged, which is D5's second clause. Amendment 1 A2 narrows it.
- **Crossing is not what limits elision.** Arm 6 (20 ms, prefer-local on, 0% forwarded) elided
  less than arm 4 at the same window, with more timeouts. It withheld no acks, though.

Decided on this evidence (ADR 0089 Amendment 1): a 30–50 ms window, documented at 50 ms; the
head rule narrowed (A2); slow subscribers stop being held (A3); and the window shrinks only for
memory (A4). Separately, the maintainer set 1.3.0's durable defaults to elision 50 ms, prefer-local
on (already the default) and the segment log store. That rests on the work and the combined
rig run the ADR and ADR 0078 still require.

## Open

- **The start-up overflow (¹).** The leading hypothesis is that a `$share` member joins cold
  (its first packet-id block not yet reserved, its placement group just formed) and
  round-robin still sends it 1/20 of the site at once. The broker 0 journal for arm 2's
  overflow is kept with the run (`2-n3/broker0-journal-rung10-overflow.log`).
- **The store.** No arm ran the segment log store. The combined 1.3.0 shape (log store, 50 ms,
  prefer-local) is a separate run.
- **The drift control.** It reads 57,099 msg/s at 10 sites against arm 1's 60,005, but arm 7's
  rung had the start-up overflow and arm 1's did not, so the two rates do not compare. The
  fourteen 1-site controls all held exactly 6,000.

## Commands

```sh
cd bench/scale
set -a && . qos1-campaign/elision-window-n3-fb90e03.env && set +a
export QOS1_DRIVER_ARCHIVE=<the audit driver archive above>
./482-per-node-knee.sh
for arm in 1 2 3 4 5 6 7; do
  python3 summarize-curve.py .runs/knee-20261021T173147Z/$arm-n3/results
done
```
