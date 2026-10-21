# ADR 0089 — Write elision on the durable QoS 1 path: a message the subscriber has acknowledged need never be written

- **Status:** Accepted
- **Date:** 2026-10-21 (accepted the same day)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0089-durable-write-elision.md](../delivery/0089-durable-write-elision.md) — plan, progress, and changelog
- **Issue:** [#1068](https://github.com/mbilling/fss-mqtt-broker/issues/1068)
- **Revisits:** [ADR 0061](0061-off-loop-durable-appends.md)'s ACK-AFTER-DURABLE invariant
  as stated for #124 ("the durable record exists before the wire send")
- **Related:** [ADR 0057](0057-durable-outbound-inflight.md) (durable outbound QoS 2 state),
  [ADR 0071](0071-owner-side-group-commit.md) (group commit),
  [ADR 0072](0072-per-message-durability-selection.md) (the opt-in weaker tiers),
  [ADR 0075](0075-pipelined-durable-appends.md) (pipelined appends),
  [ADR 0078](0078-replica-segment-log.md) (the segment log store),
  [ADR 0087](0087-delivery-across-a-false-dead-verdict.md) (holds across a false verdict)

> **Proposed 2026-10-21** by the maintainer: on the durable QoS 1 hot path, hold a message
> for a short time (the example given was 20 ms) to see whether it can be delivered to its
> subscribers and acknowledged before the fsync write; write only if it cannot. Durable QoS
> 1 is the path the project most wants to be great at. This record states the idea so it
> keeps the durability promise, and what would have to be shown before it is on by default.
>
> **Accepted 2026-10-21** by the maintainer, with the rule restated in their words: the
> message is sent to the subscriber within `write_elision_ms`; if it is not delivered and
> acknowledged in that time, it is written to disk, and the publisher is acknowledged once
> the write is durable. The default stays 0 until the evidence below is in.
>
> **Amended 2026-10-21** by the maintainer after the first rig sweep (T5): the window is
> 30–50 ms, documented at 50 ms; the head rule of D5 is narrowed to what order needs; a
> subscriber that keeps missing the window stops being held; the window shrinks only for
> memory. See [Amendment 1](#amendment-1--the-window-the-head-rule-and-slow-subscribers).

## Context

A QoS 1 publish matching a persistent session is today acknowledged to its publisher only
once the message is durable: written on the session's owner and on a quorum of its replica
set (ADR 0042 T8), through the group-commit writer (ADR 0071) and the pipelined append
(ADR 0075). The delivery to an online subscriber is sent only after that write
(`hub/lanes.rs`, "the post-durable live send"). That order is #124's fix: before it, the
publisher was acknowledged on a bare socket send, and a node that died before the
subscriber's PUBACK lost an acknowledged message.

What the durable path costs, measured on one provisioning on 2026-10-21 (10 × CCX23
brokers, persistent `$share` consumers that are online and keeping up, 6,000 msg/s per
site, `bench/scale/durable-knees-2026-10-21.md`):

| store | 10 nodes | 3 nodes |
|---|---|---|
| redb (default) | stops at about 18.5k msg/s per node | about 19.3k per node |
| segment log (ADR 0078, experimental) | 27k per node GREEN, 29.9k at 99.5% of offer | 30k per node GREEN |

Clean-session QoS 1 on the same kind of hosts carries 39–45k per node. The difference is the
durable write and its replication round, paid for every message, including the large
majority that an online subscriber acknowledges within a few milliseconds. For such a
message the write protects nothing that the subscriber's acknowledgement does not already
settle: once the subscriber has acknowledged it, no session is owed it, and the durable copy
is truncated soon after (ADR 0074).

## Decision

### D1. The promise, restated

**The publisher's acknowledgement waits until every persistent session the message is owed
to has either acknowledged it or holds a durable copy of it.** Today only the second
discharges an obligation. This record adds the first.

The invariant this replaces is "durable before the wire". The invariant it keeps is the one
#124 was about: *an acknowledged publish is never lost*. A node that dies while a message is
held in its window has acknowledged nobody, so the publisher resends it (QoS 1 is at least
once) and nothing acknowledged is lost. The subscriber may receive that message twice, which
QoS 1 allows.

### D2. Which deliveries may skip the write

All of these must hold for a delivery to a persistent session to be *eligible*:

- the message and the subscription are QoS 1 (QoS 2 keeps its durable phase records,
  ADR 0057, and is out of scope);
- the session is attached on its owner node at fan-out time;
- the session's **head is clear**: nothing earlier for that session is in its durable queue
  unacknowledged, and no earlier delivery for it is still in a window (D5);
- the node is not in brownout, not draining, and not in a takeover window for that
  session's group (`takeover_open`, #1053);
- the elision memory bound (D7) has room.

An ineligible delivery takes today's path, unchanged.

### D3. The window

An eligible delivery is sent to the subscriber at once and **held**: its message stays in
memory, and the publish's acknowledgement waits. It ends in exactly one of two ways:

- **discharged**: the subscriber's PUBACK arrives within `durable.write_elision_ms`. No
  durable write, no replication. The obligation is settled;
- **written**: anything else first, namely the timer, the subscriber's connection ending, an
  ownership change for the group, brownout, drain, or the memory bound. The message is then
  appended through today's path, and the obligation is settled when the append is durable,
  as now. A PUBACK that arrives after the write started settles the in-flight entry as
  today's PUBACK does.

`durable.write_elision_ms` = 0 is today's behaviour exactly. The first release ships with 0.

### D4. Fan-out

A message with several owed persistent sessions is discharged only when every one of them
is, each by its own acknowledgement or its own durable copy. Some may be written while others
are elided. For a `$share` group, the obligation is the one member the delivery was placed
with (ADR 0087). Clean sessions and QoS 0 subscribers are not obligations and change
nothing.

### D5. Order

A session's durable queue order is its replay order (ADR 0061). An elided delivery has no
offset, so a later message that is written while an earlier one is still held would replay
ahead of it after a reconnect. Hence the head rule in D2, applied per session:

- while any delivery for a session is held or has fallen back to a write that is not yet
  acknowledged, every later delivery for that session takes the durable path;
- elision resumes for the session once its queue is empty again.

A subscriber that keeps up stays on the fast path; one that falls behind leaves it on its
first slow acknowledgement and returns when it has caught up.

### D6. In a cluster

The session's owner decides, as it decides the append today. A publish forwarded to the
owner is answered (`Reached`/`Stored`) only when its obligation there is discharged, so the
origin's acknowledgement waits the same way. The held-forward rule of #1053 and the holds of
ADR 0087 compose: a forward held for a takeover is not eligible, and an owner that dies in a
window has answered nobody.

### D7. Bounds

Held deliveries are bounded per node in entries and bytes (defaults to be measured). At the
bound, an eligible delivery is written instead. A held message costs its payload in memory
for at most the window, so the bound is roughly the window times the elided rate.

### D8. What is not changed

Offline persistent sessions, retained messages, Wills, QoS 2, and the opt-in tiers of
ADR 0072 (`local`, `relaxed`) keep their behaviour. The `quorum` tier keeps its meaning:
*durable on a quorum, or acknowledged by every subscriber owed the message*.

### D9. Observability

- `mqttd_durable_elided_total`: deliveries discharged without a write;
- `mqttd_durable_elision_fallback_total{reason}`: `timeout`, `disconnect`, `ownership`,
  `brownout`, `drain`, `bound`, `head`;
- a histogram of the time a discharged delivery was held (the subscriber's acknowledgement
  latency);
- the elided share on `/statusz`.

## What counts as done before the default changes

On the rig, on one provisioning, with the same-hosts A/B shape used for the 2026-10-21
durable confirmation:

1. **The gain is real where it should be.** With consumers that keep up, elision on (a
   window of 20 ms) carries more durable QoS 1 per node than off, at p99 ≤ 1 s, on both
   stores.
2. **It costs nothing where it cannot help.** With offline consumers, and with consumers
   that acknowledge more slowly than the window, elision on is within the rig's run-to-run
   spread of off.
3. **Nothing is lost.** The identity ledger closes on every rung. The fault suites
   (`cluster_proc`, `cluster_stress`) pass with elision on, including a new schedule that
   kills a session's owner while deliveries are held, and history-check's promises hold
   (acknowledged durability, order, redelivery marked).
4. **Order holds** under a subscriber that alternates between keeping up and falling
   behind (a characterisation test of D5).

## Consequences

- **Good:** for subscribers that keep up, the durable path stops paying a write and a
  replication round per message, which is the cost the durable ceiling is made of. The
  publisher's acknowledgement latency becomes about one subscriber round trip.
- **Bad:** a subscriber that is online but slow pays the window on top of the write, for its
  first slow message. Duplicates after a crash in the window reach the subscriber as new
  messages (`DUP` 0), the same shape as #1058. The obligation ledger gains a second way to be
  discharged, which every path that settles a publish must respect.
- **Benchmarks:** the durable rig workload has consumers that keep up, the best case for
  this change. A headline figure with elision on must sit beside the offline and lagging
  shapes of criterion 2.

## Alternatives considered

- **Acknowledge the publisher at the wire send, write behind.** Rejected: that is #124's
  defect. An acknowledgement must not precede both the durable copy and the subscriber's
  acknowledgement.
- **The `relaxed` tier of ADR 0072.** It weakens the promise per message, by the
  publisher's choice. This keeps the promise and changes only how it is met.
- **More batching or a faster store.** Done (ADR 0071, 0075) and in progress (ADR 0078); they
  compose with this. A faster write raises the ceiling; elision removes the write where it
  is not needed.

## Amendment 1 — the window, the head rule and slow subscribers

Decided 2026-10-21 by the maintainer, on the first rig sweep: 3 × CCX23 brokers, 2 copies,
the default redb store, persistent `$share` consumers that keep up, and
`MQTTD_SHARED_PREFER_LOCAL=0`, so two thirds of publishes cross nodes before the subscriber's
PUBACK can discharge them. One provisioning, run `knee-20261021T173147Z`; the card is
[`bench/scale/elision-window-n3-2026-10-21.md`](../../bench/scale/elision-window-n3-2026-10-21.md).

**What it showed.** At 10 sites (60,000 msg/s offered, 20k per node):

| window | elided | fallbacks | write latency | PUBACK hold p99 / p99.9 |
|---|---|---|---|---|
| 0 (off) | — | — | 35.8 ms | — |
| 5 ms | 1.5% | 713 | 31.2 ms | capped at the window |
| 10 ms | 1.6% | 419 | 33.0 ms | capped at the window |
| 20 ms | 6.1% | 850 | 34.7 ms | capped at the window |
| 50 ms | **98.1%** | 59 | **6.8 ms** | 19.7 / 29.6 ms |

At 1 site every window elided 99.6%. Under load the path is **bistable**: a few timeouts put
a session on the durable path, D5 keeps it there while anything it was written is
unacknowledged, writes slow to 30–90 ms, and a busy session (300 msg/s here) almost never sees
the gap it needs to return. A window that covers the loaded PUBACK tail never tips over.

### A1. The window

`durable.write_elision_ms` is documented at **50 ms**; 30–50 ms is the supported range. It
must cover the subscriber's acknowledgement tail under load (p99.9 30–44 ms at 10–15 sites
here), not its mean (under 3 ms). For a subscriber that keeps up a long window costs nothing:
a held delivery is discharged at its PUBACK, not at the window's end.

**The default becomes 50 ms in 1.3.0** (maintainer, 2026-10-21, together with the segment log
store as the default replica store, ADR 0078). Until then it stays 0. The flip rests on the
criteria above with A2 and A3 in, the cluster cases (T3), and one rig confirmation of the
combined 1.3.0 defaults.

The window does **not** follow load. Raising it after timeouts appear comes too late, since
the sessions are already on the durable path; and a short window under light load saves
nothing.

### A2. The head rule, narrowed

D5's first clause stays: while any delivery for a session is **held**, every later one takes
the durable path, and a session's held deliveries fall back together and in order. Its second
clause ("or has fallen back to a write that is not yet acknowledged") is replaced by: **while
any append for the session is in flight**. A delivery that is already written and on the
wire, but not yet acknowledged, no longer blocks the next one from being held.

Order holds. After a crash the written delivery replays from the store; the later held one
was either acknowledged (and is never resent) or was never acknowledged to its publisher,
which resends it after. MQTT requires a resend to keep the original order of what is resent,
and nothing acknowledged is resent.

### A3. A subscriber that keeps missing the window

A session whose recent deliveries keep falling back on `timeout` stops being eligible for a
while, so its publishers pay the write alone and not the window on top (criterion 2). It
becomes eligible again after a quiet interval. The threshold and the interval are measured in
T7, not fixed here.

### A4. Memory

The window shortens, or new holds stop, only as the node nears the D7 bound. That is the one
cost of a long window that grows with load.

## Open questions

- Whether a publish whose subscriber is on another node (a relocated session's frontend)
  is eligible, given the extra hop in the acknowledgement path. (The T5 sweep forced the
  extra hop and held 98% at 50 ms; the relocated-frontend case is still untested.)
- Whether a resend after a crash in the window can carry `DUP` to the subscriber (#1058).
