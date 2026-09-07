# Load-driver audit — what `emqtt-bench` actually measures

Issue #534, task 1. The scale rig's numbers are only as good as the driver's
semantics, and those semantics had never been read — they had been *assumed*,
twice, in opposite directions. This records what the pinned driver does, with
citations, so the next capacity claim argues from the source rather than from a
comment.

Every `.erl` line reference below is to UPSTREAM source, not to this
repository — `emqtt_bench.erl` is emqx/emqtt-bench and `emqtt.erl` is emqx/emqtt,
at the versions in the table. Nothing here is a tracked artifact of this repo,
which is exactly why the versions are pinned by digest and tag.

## What is pinned

| Thing | Version | How it is pinned |
|---|---|---|
| Driver image | `emqx/emqtt-bench:0.6.3` | `BENCH_IMG` in `run-curve.sh`, now by digest |
| Image digest | `sha256:ae7f2d56cd49b14824c835140c808b093c5e3f2defb3a29b34b17560feb456cd` | OCI index; `linux/amd64` is `sha256:2e459112715ed2272c5…` |
| Driver source | `emqtt-bench` tag `0.6.3` | [`emqx/emqtt-bench@0.6.3:src/emqtt_bench.erl`](https://github.com/emqx/emqtt-bench/blob/0.6.3/src/emqtt_bench.erl) |
| MQTT client | `emqtt` `1.15.1` | [`emqx/emqtt@1.15.1:src/emqtt.erl`](https://github.com/emqx/emqtt/blob/1.15.1/src/emqtt.erl), pinned in emqtt-bench's `rebar.config` |

A tag is mutable; a digest is not. The rig had pinned the tag, so two campaigns
"on 0.6.3" were not provably the same binary.

## 1. The publish timer is DEADLINE-BASED, not fixed-delay

`emqtt_bench.erl:824-835`:

```erlang
PubAttempted = bump_publish_attempt_counter(),
BeginTime    = get_publish_begin_time(),
NextTime     = BeginTime + PubAttempted * Interval,
Remain       = NextTime - erlang:monotonic_time(millisecond),
Interval > 0 andalso Remain < 0 andalso inc_counter(Prometheus, pub_overrun),
case Remain > 0 of
    true  -> erlang:send_after(Remain, self(), Trigger);
    false -> self() ! Trigger        %% already late: fire at once
end
```

The next publish is scheduled against an **absolute deadline** — start time plus
attempt count times interval — not by sleeping `Interval` after the previous one
completed. So publish latency is *absorbed* by the interval rather than added to
it, and `-I 40` offers 25/s per client as long as a publish completes inside
40 ms. Past that the client can never catch up: `Remain` stays negative and the
loop runs flat out.

**This settles the question the scaling review left open** ("a driver that sleeps
40 ms after completion would achieve a different rate than one scheduling against
a 40 ms deadline"). It schedules against a deadline.

## 2. `pub_overrun` is a first-class under-offer signal — used by lane B, missing from lane E

That same branch increments **`pub_overrun`** on every missed deadline
(`COUNTER_NAMES`, `emqtt_bench.erl:215-233`), exported to Prometheus when
`--prometheus` is passed — which every lane already passes.

An earlier draft of this audit said "the rig does not read it". **That was wrong
and is corrected here:** `lane_b_rung` has read it since it was written, as
`late_rate`, and flags `PUBLISHERS LATE` past `LATE_OK = 0.05`.

The accurate finding is narrower and more useful: **`lane_e_rung` did not**, and
lane E is the SCADA tenancy ladder — the lane whose runs the scaling review took
apart. It judged a rung on an offered-rate average (`DRIVER_OK = 0.97`) and
nothing else, so a rung could **meet its offer on average while a third of its
publishes ran late**, and still be reported as a passing site count. The average
cannot see that; only the counter can.

Fixed in this change: lane E now reads `pub_overrun`, flags `PUBLISHERS LATE`,
and — because a flag nobody acts on is decoration — includes it in the rung's
`pass` verdict alongside offer, delivery and latency budget.

## 3. At QoS 2 the driver returns on PUBREC — NOT on PUBCOMP

`emqtt.erl:552-573` — `publish_via/3` is synchronous: it issues an async publish
with a callback and blocks until the callback fires. When the callback fires
depends on the QoS:

| QoS | Callback fires at | Source |
|---|---|---|
| 0 | immediately | — |
| 1 | **PUBACK** | `emqtt.erl:1819-1835`, `ack_inflight(?PUBACK_PACKET…)` calls `eval_callback_handler` |
| 2 | **PUBREC** | `emqtt.erl:1838-1863`, `ack_inflight(?PUBREC_PACKET…)` calls `eval_callback_handler`, then re-inserts an `INFLIGHT_PUBREL` entry |
| 2 | *not* PUBCOMP | `emqtt.erl:1865-1875` deletes the `INFLIGHT_PUBREL` entry and evaluates **no callback** |

So a QoS 2 publish returns after **one** round trip, exactly like QoS 1. The
PUBREL/PUBCOMP half completes asynchronously afterwards.

### Two consequences, both correcting earlier claims

**(a) The per-client rate ceiling at QoS 2 is ~1/RTT, not 1/2RTT.** The scaling
review thread reasoned that "at QoS 2 the return moves to the PUBCOMP, i.e. two
round trips instead of one, so the per-client ceiling roughly halves", and
derived a publisher-count change from it. That was wrong, and it is withdrawn:
the driver returns at PUBREC and the ceiling is unchanged from QoS 1. The
reviewer's objection — audit the pinned implementation before asserting
completion timing — was correct, and this is that audit.

**(b) The driver cannot report QoS 2 protocol completion at all.** Its `pub`
counter increments when `publish` returns, which at QoS 2 is PUBREC. A message
counted `pub` has *not* finished its exactly-once handshake. Any accounting that
treats driver `pub` as "protocol-completed" **overcounts at QoS 2**, which is
precisely the confusion #534 exists to prevent ("distinguish client completion
from durable acceptance and application delivery"). Completion must come from the
broker's counters, or from a driver change — not from `pub`.

Note also that the `INFLIGHT_PUBREL` entry keeps occupying an inflight slot until
PUBCOMP, so `-F` (max inflight) governs the *whole* handshake, not just its first
half, and `emqtt_bench.erl:711` warns the publish call "hangs if emqtt inflight
is full".

## 3b. Retransmission is OFF by default, and a stalled publish never returns

`emqtt-bench`'s `--retry-interval` defaults to **0**, documented as "no resend"
(`emqtt_bench.erl:191-195`), and it is passed straight through to `emqtt`.
`emqtt` starts its retry timer only `when Interval > 0`
(`do_ensure_retry_timer`), so at the rig's defaults **nothing is ever
retransmitted**. `publish_via` also passes `infinity` as the inflight expiry, so
the entry never times out and the callback never fires with an error.

Consequence for measurement: at QoS 1/2, a publish whose ack is lost **blocks
that publisher client forever**. It is not retried, not failed, not counted —
the client simply stops offering. `pub_fail` stays zero. The only counter that
moves is `pub_overrun`, which is finding 2's argument restated from the other
end: under loss the offered rate becomes fiction, and the overrun counter is the
only thing that says so.

When a run *does* set `--retry-interval`, retransmits carry `dup = true`
correctly (`retry_send` sets `Msg#mqtt_msg{dup = true}`), and a QoS 2 retransmit
in the release phase re-sends PUBREL rather than the PUBLISH.

## 3c. The SUBSCRIBER side at QoS 2 *is* a sound unique-delivery oracle

The asymmetry matters, because the two ends are not equally trustworthy:

| Side | Counter | Fires at | Sound oracle for? |
|---|---|---|---|
| publisher, QoS 1 | `pub` | PUBACK | completion — yes |
| publisher, QoS 2 | `pub` | **PUBREC** | completion — **no**, handshake unfinished |
| subscriber, QoS 1 | `recv` | on delivery | delivery, but duplicates are spec-legal |
| subscriber, QoS 2 | `recv` | **PUBREL**, deduplicated | **unique delivery — yes** |

For inbound QoS 2, `emqtt` stores the PUBLISH in an `awaiting_rel` map and
answers PUBREC *without delivering*; delivery happens on PUBREL, via
`maps:take`, which removes the entry and so cannot deliver twice
(`publish_qos2/3`, `process_pubrel/3`). `emqtt_bench` counts `recv` on that
delivery.

So a both-ends QoS 2 lane can count unique deliveries honestly from the
subscriber, while completion must come from the broker. That is the shape any
QoS 2 accounting should take.

## 4. Stable message identities already exist — no new driver needed

`emqtt_bench.erl:886-905` substitutes into a payload template:

- `%UNIQUE%` → `erlang:unique_integer()`
- `%TIMESTAMP%`, `%TIMESTAMPMS%`, `%TIMESTAMPUS%`, `%TIMESTAMPNS%`
- `%RANDOM%` → n random bytes

`#534` asks for "stable application message identities" so delivered messages can
be counted uniquely and duplicates detected. `%UNIQUE%` supplies exactly that
through the existing pinned driver, which is what the review meant by "reuse
sound existing driver functionality; replace it only if a demonstrated limitation
requires that". The remaining work is on the *subscriber* side — recording the
identities seen — not on the publisher.

## 5. The rung had no drain, so pending traffic was booked as loss

Not a driver fact — a rig one, found while writing the accounting the sections
above call for, and fixed here (`run-curve.sh`, `lane_e_rung`).

Lane E used to tear down publishers and consumers in the **same batch**:

```bash
driver_batch "$di" "${stop[di]}docker rm -f${names[di]} ..."
```

So every message the broker still held, or that was on the wire, at that instant
was never received — and `summarize-curve.py` then reported the shortfall as

```
LOSS (delivered 98.7% of what was published)
```

Three different situations wear that one label: the broker **dropped** it, the
broker still **holds** it, or it was **in flight**. Only the first is a finding
about the broker, and the rung's own teardown manufactured the other two.

What replaces it, per #534's seventh acceptance criterion:

1. The **publishers** stop. The consumers stay connected.
2. `recv` is polled across every consumer container every `LANE_E_DRAIN_POLL`
   seconds, bounded by `LANE_E_DRAIN_SECS` (15/30/60 by profile). The drain ends
   after `LANE_E_FLAT_POLLS` (3) consecutive **non-increasing** totals — not one,
   because a single flat poll can land inside a scrape gap and end the
   measurement early. Those polls are then discounted from the reported
   `drain_secs`, since the backlog was already gone when the first was taken.
   The poll is lane D's `recv_total` — the same REST scrape the rung already
   uses for its histograms, and the same loop lane D has run against real
   hardware since it was written. A second read path would be a second thing to
   get wrong.
3. The deadline is **explicit and bounded** deliberately. Waiting for quiet
   instead would hang a paid rung on a broker that is never going to reach it,
   which is #504's non-recovery.
4. `rung.txt` records `drained=yes|no`, `drain_secs` and `drain_deadline_s`, and
   a broker-side `snapshot_metrics … drain` is taken **at** the deadline with the
   consumers still connected — what the broker holds there is the difference
   between pending and dropped, and no driver counter can see it.

The summarizer then classifies rather than assumes:

| `drained` | shortfall reads as |
|---|---|
| `yes` | **LOSS** — the consumers stopped receiving with the broker still owing it |
| `no` | **UNRESOLVED** — pending or dropped, and this rung cannot tell which |
| absent | **LOSS**, disclosing that a pre-drain run directory cannot separate them |

A drain that exhausts its budget with the backlog still moving also `warn`s at
the time, so the operator sees it during the run and not only in the summary.

An `UNRESOLVED` rung is not a pass and **not a broker finding**; the ladder says
so in its own verdict rather than letting "we did not measure this" render as
"the cluster could not do it".

Two consequences worth stating plainly:

- The post-drain total (`sub-*.drain`) is what the **delivery** check reads; the
  steady-window total (`sub-*.log`) is what the **rate** reads. `driver_rate`
  averages over the last 60 seconds of a series, so letting the consumers' log
  run into a draining tail would understate every rung's rate.
- **Every lane E `LOSS` flag recorded before this** was measured with this
  teardown. Those runs keep reporting LOSS, and now say in the flag itself that
  they could not tell pending from dropped.

## 6. What the rig now records, and what it still cannot

`#534`'s remaining criteria are accounting rules, and most of them turned on a
question the rig had never asked: *of the eight things that can happen to a
message, which end can actually see each one?*

| count | read from | notes |
|---|---|---|
| offered | `rung.txt` rate x the publishers' own run span | not the measurement window — see below |
| sent | driver `pub`/`pub_succ`, double-count corrected | at QoS 2 this is PUBREC, **not** completion |
| broker-received | `mqttd_publish_received_total`, by QoS | |
| protocol-completed | QoS 1 only | **not measurable at QoS 2** — see below |
| uniquely delivered | consumer `recv`, post-drain | exact at QoS 2 (`awaiting_rel` + `maps:take`) |
| duplicate | broker delivered − consumer unique | the only place it is visible |
| dropped | `mqttd_publish_dropped_total`, by reason | |
| still-pending | sent − delivered at the drain deadline | only when the drain did not converge |

### QoS 2 protocol completion cannot be measured by anything

Section 3 established that `emqtt` fires the publish callback at PUBREC and
evaluates no callback on PUBCOMP, so the DRIVER stops one round trip short. The
broker's side was then checked, and it is no better: the entire relevant metric
surface is

```
mqttd_publish_received_total{qos}   mqttd_publish_delivered_total{qos}
mqttd_publish_dropped_total{reason} mqttd_sessions   mqttd_connections_active
```

There is **no PUBCOMP counter**. Neither end can certify that an exactly-once
handshake finished, so the rig reports completion as `n/a` at QoS 2 and flags the
rung `QOS2 COMPLETION UNVERIFIABLE`, which prevents it passing. Reporting `sent`
there would overcount exactly where #534 exists to prevent overcounting.

**This is the thing that blocks QoS 2 in lane E**, and it is a one-counter fix in
the broker rather than anything about the driver.

### What the QoS labels DO buy

`mqttd_publish_delivered_total` is labelled by QoS, so the QoS a delivery
actually went out at is a measured fact rather than the one the rung asked for. A
subscription granted a lower QoS than it requested is now detected
(`QOS DOWNGRADE`) instead of silently measuring a different protocol.

### Queue depth, bytes and age are NOT available

`#534` asks for them "where available". They are not: there is no
`mqttd_backlog_bytes` metric — the name appears only in comments — and nothing
counts what is queued inside a session. Sessions and connections ARE recorded, at
the drain deadline. The table says so rather than putting a number where a gap is.

## 7. Measured — the first run of any of this (2026-09-07)

One `LANES=E` smoke on Hetzner, 1 broker (cpx32) + 1 driver (cpx42), 1.0.16,
ladder 1 and 2 sites at 5,000 msg/s per site, QoS 0. Not a capacity measurement
and not offered as one; it exists to make the machinery above something that has
run rather than something that type-checks.

- **Provenance** resolved: harness `18c768e` clean, broker binary
  `34db6a24…`, and the driver image digest the driver actually pulled matched the
  pinned one exactly.
- **Calibration**: asked 5,000/s of one container, achieved 5,000/s, **0 late
  publishes**. The ceiling guard is now anchored to a measurement on the metal
  that was billed, not to another campaign's comment.
- **Settle**: 202/202 and 404/404 clients connected before the window opened.
- **Reset**: 0 connections carried between rungs.
- **Drain converged in 7-8 s against a 45 s deadline** — the prediction this
  audit recorded before the run, and the reason `drained=yes` rather than
  `UNRESOLVED` is the expected reading of a healthy rung.
- **Control**: the 1-site rung repeated after the ladder delivered 4,983/s
  against 5,000/s, a 0.3% drift — inside the 5% bound, so the trial is conclusive.
- Accounting closed: broker-received equalled uniquely-delivered **exactly** at
  every rung, with 0 dropped and 0 pending.

Two findings from that run, both about the rig rather than the broker:

1. **`offered` was being computed over the measurement window while every other
   count is a lifetime total**, which rendered a healthy rung as having sent 2.4x
   what was asked. Fixed to the publishers' own run span; the numbers then close
   to 99.2%.
2. **The driver's lifetime total undercounts the broker's by ~4%.**
   `emqtt-bench` writes a progress line once a second and the last one predates
   container teardown, so up to a second of publishes is never logged. Broker
   counters are the better denominator for anything that has to balance.

## 8. Measured — QoS 2, both ends (2026-09-07)

The same shape, `LANE_E_QOS=2 LANE_E_SUB_QOS=2`. This is the both-ends arm #534
asks for and #405 says the old durable_bench rows never measured. Every validity
rule in this document fired, and three of them fired on behaviour nobody
arranged.

| rung | delivered/s | p99 | verdict |
|---|---|---|---|
| 1 site | 5,003 (100.1% of offer) | ≤50 ms | **9% of publishes late** |
| 2 sites | 6,067 (61% of offer) | ≤100 ms | offer not met, **100% late** |
| 1 site (control) | 4,975 | ≤50 ms | **41% late** |

### The "meets its offer while running late" case is real

The 1-site rung delivered **100.1% of its offered rate** and was still **9%
behind its own schedule**. An offered-rate average cannot see that, and until
#534 lane E had nothing else — this is the exact rung that would have been
published as a passing site count. It is now flagged, and the flag is load-
bearing: it is why the rung does not pass.

The mechanism is section 1's deadline-based timer. At QoS 2 a publish returns on
PUBREC, so a client's ceiling is ~1/RTT; at `-I 40` with 200 clients per
container on a shared-vCPU driver, the deadline is missed and the scheduler fires
immediately to catch up. Average rate preserved, schedule not. **Calibration saw
it before the ladder started** — 5,112/s achieved with 48,866 late publishes —
and warned.

### The control rung caught something on its first outing

The 1-site rung was 9% late at the start of the ladder and **41% late when
repeated at the end**. Same rung, same cluster, four times worse. So the trial is
reported `INCONCLUSIVE — no capacity is claimed`, which is what #534 asks for
("report failed controls as inconclusive trials, not capacity findings"). Without
the control this run would have published a 1-site pass and a 2-site failure and
called that a capacity ladder.

### QoS 2 accounting held

Broker-received equalled uniquely-delivered **exactly** on all three rungs
(188,082 / 189,146 / 228,638), with 0 dropped and 0 pending — consistent with
section 3's claim that a QoS 2 subscriber's `recv` is a true unique-delivery
oracle, now observed rather than only read from source. `completed` is `n/a` on
every row, for the reason section 6 gives.

Latency is the other visible cost: ≤5 ms at QoS 0 against ≤50 ms at QoS 2 for the
same offered rate.

### What this does NOT say

It does not say the broker is slow at QoS 2, and nothing here measures the
broker at all. Every rung is driver-limited on a shared-vCPU cpx42 — that is what
"PUBLISHERS LATE" means — so the honest reading is that **this hardware cannot
offer QoS 2 at this shape**, and a QoS 2 capacity question needs dedicated cores
and fewer clients per container. Which is the point: the rig now says so instead
of publishing the number.

## What this audit does NOT establish


- **This is not a capacity measurement, and none of these numbers sizes
  anything.** One node, one driver, shared-vCPU machines, 5,000 msg/s per site,
  a 15-second window. It establishes that the validity machinery runs and agrees
  with itself; it establishes nothing about how many SCADA sites a cluster holds.
  Those come from `full`, on dedicated cores, after #536.
- **QoS 2 stays disabled in lane E** (`LANE_E_QOS=0`), and the blocker is now
  named precisely: nothing on either side can certify a QoS 2 handshake finished.
  Enabling it needs a broker-side completion counter, not more driver work.
- **The asymmetric arm (publisher QoS 2 / subscriber QoS 1) has a knob and no
  run.** `LANE_E_SUB_QOS` exists and is shape-checked, and the both-ends arm has
  now run (section 8); #405's inbound-vs-outbound split is answerable, not
  answered.
- **The ceilings are calibrated per run, not per machine type.** The probe proves
  one container can offer a rung's rate on the driver in front of it. It does not
  build a table of what a cpx42 or a CCX33 can do, and the 20,000/container and
  10,000/consumer defaults still come from 0077-T7's campaign.
- **The drain's convergence rule is an argument, not a measurement.** Three
  consecutive non-increasing polls ended every drain well inside the deadline in
  both runs — but nothing has yet exercised the case it exists for, a broker that
  does not drain. `UNRESOLVED` has fired only against fixtures.
- **The control rung has now caught a real degradation** (section 8) and has also
  passed cleanly at 0.3% drift (section 7), so it is neither inert nor a
  false-positive generator. What it has not done is distinguish a DEGRADED BROKER
  from a degraded driver: the QoS 2 run's control failed on publisher lateness,
  which is a driver fact. The residual-overload case it was built for is still
  unobserved.
- **Duplicate delivery and QoS downgrade have fixtures, not sightings.** Both are
  detectable from the broker's QoS-labelled counters; neither has been observed,
  because nothing in these runs misbehaved. The QoS 2 run does confirm the
  downgrade check does not FALSE-positive: deliveries were labelled `qos="2"`
  throughout and no flag was raised.
- **Sections 1-4 remain source facts, not behaviour.** The PUBREC/PUBCOMP claim
  in section 3 is read from `emqtt` 1.15.1's source; it has not been confirmed by
  watching packets on the wire.
