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

## 2. `pub_overrun` is a first-class under-offer signal, and the rig ignores it

That same branch increments **`pub_overrun`** on every missed deadline
(`COUNTER_NAMES`, `emqtt_bench.erl:215-233`), exported to Prometheus when
`--prometheus` is passed — which every lane already passes.

The rig does not read it. `summarize-curve.py` INFERS under-offer by comparing a
rate parsed out of container log lines against the requested rate
(`DRIVER_OK = 0.97`), and `snapshot_metrics` scrapes the **brokers'** `/metrics`
only, never the drivers'. So the rig reconstructs, with a threshold, a fact the
driver already reports exactly.

Wiring `pub_overrun` in is the cheapest validity win available: a non-zero value
on a rung means the driver could not keep up, whatever the log-derived rate says.

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

## What this audit does NOT establish

- Nothing here was **run**. These are source facts about the pinned versions,
  which is what task 1 asked for; the behavioural validation (both ends at QoS 2,
  duplicate detection, drain deadlines) is still open.
- The subscriber-side accounting, per-driver achievable offer, and the
  reset/control-rung requirements remain unaddressed — see #534.
- `pub_overrun` is described from source; it has not been observed on a real run,
  and the harness does not yet scrape it.
