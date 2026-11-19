# ADR 0060 — Bridge durability and acknowledgement contract

- **Status:** Proposed
- **Date:** 2026-08-11
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0060-bridge-durability-and-ack-contract.md](../delivery/0060-bridge-durability-and-ack-contract.md) — plan, progress, and changelog
- **Amends:** [ADR 0025 §7](0025-boundary-bridge.md) (store-and-forward), which promised at-least-once but left the ack/durability ordering unspecified.
- **Amended:** [Amendment 1](#amendment-1-2026-11-19-the-source-session-is-the-buffer)
  (2026-11-19, issue [#1103](https://github.com/mbilling/fss-mqtt-broker/issues/1103)):
  traffic from the local cluster is forwarded through the bridge's persistent session there,
  acknowledged after the upstream's acknowledgement, with no spool; one link per upstream
- **Related:** [ADR 0057](0057-durable-outbound-inflight.md) (the broker's own outbound-in-flight durability — this is its bridge analogue), [ADR 0041 §T7](0041-resource-governance.md) (the spool byte-bound), [ADR 0025 §8](0025-boundary-bridge.md) (the audit record the drop path must feed).

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0060-bridge-durability-and-ack-contract.md).

## Context

ADR 0025 §7 promises **at-least-once** delivery for QoS≥1 rules across a transient outage,
backed by a bounded disk-backed spool. A code audit found the ack/durability ordering that
"at-least-once" depends on is unspecified and, as built, **broken**:

1. **Ack before durable.** A QoS-1 inbound PUBLISH is PUBACK'd to the **source** broker
   immediately in the read loop, and only *then* handed to a separate task that routes and
   (if the destination is down) spools it. If the bridge crashes in that window — acked, not
   yet committed — the message is **lost while the source broker considers it delivered**. This
   is the classic bridge data-loss bug, and it is *at-most-once*, not the promised at-least-once.

2. **Durability is conditional and silent.** The disk spool relies on redb's default commit
   durability with no explicit fsync in the crate; and if no spool directory is configured, or
   the disk spool **fails to open**, the bridge **silently falls back to an in-memory spool** —
   acked messages then live only in RAM, lost on any restart, with no signal.

3. **Drop-oldest is counted but not audited.** Under spool pressure the bridge drops the oldest
   message (bounded by count; the byte-bound is ADR 0041 T7, unbuilt). The drop increments a
   Prometheus counter but writes **no audit record** — yet the dropped message was already
   acked (§1) and this is a security-auditable crossing (ADR 0025 §8).

These are the three places ADR 0025 §7's "must not lose beyond the QoS contract" is currently
not held. This is the bridge analogue of ADR 0057, which established the same ack-on-durable
discipline for the broker's own outbound in-flight.

## Decision

Gate every source acknowledgement on durability, and make the spool's durability guarantee and
its overflow behaviour explicit and honest.

### 1. Ack-on-durable (the core rule)

The bridge **must not PUBACK the source** for a QoS≥1 message until that message is durably
accepted for forwarding — meaning **either** it has been committed to the disk spool (fsync'd)
**or** the destination has acknowledged the forward (QoS≥1 downstream). The read loop moves to a
**pending-ack model**: the message is handed to the router first, and the source PUBACK is
emitted by a completion callback when durability is reached. QoS-0 rules are unchanged (no ack,
no promise).

This preserves at-least-once precisely: a crash before durability means the source **never saw
an ack** and will redeliver; a crash after means the message is on disk and replays. Duplicates
across the crash are the accepted at-least-once cost (ADR 0025 §7 already disclaims
exactly-once).

### 2. The spool durability guarantee is explicit

A disk spool commit is **fsync-on-commit** (redb `Immediate` durability, asserted in code and
tested, not left to a default). The spool's contract — "a message the bridge has ack'd to the
source is on stable storage or already forwarded-and-acked" — is stated in the ADR 0025 §7
amendment.

### 3. No silent non-durable operation for QoS≥1

If a QoS≥1 rule is configured but no durable spool is available (no `spool.dir`, or the disk
spool fails to open), the bridge **refuses to start** for that rule, or — under an explicit
`allow_ephemeral_spool = true` opt-in — runs with an in-memory spool while **logging loudly**
at startup and on every reconnect that QoS≥1 durability is **not** in effect. The silent
in-memory fallback is removed. QoS-0-only bridges may run without a spool as today.

### 4. The drop path is audited

Dropping a spooled (already-acked) message is a loss event on an auditable crossing, so it emits
an **audit record** (topic, direction, upstream, reason) into the same ADR 0025 §8 audit stream,
in addition to the metric. The count-based cap stays until the byte-bound (ADR 0041 T7) lands;
both are documented as the pressure behaviour. Whether the default under sustained pressure
should be **drop-oldest** or **refuse-and-backpressure** (stop acking the source, letting its
own queue absorb) is decided per-rule via `overflow = "drop-oldest" | "refuse"`, defaulting to
**`refuse`** for QoS≥1 rules (a stalled crossing is safer than silent loss on a boundary) and
`drop-oldest` for QoS-0.

### 5. How the pending-ack model stays fast (the hot-path design)

Decision 1 sits on the bridge's hot path, so *how* it is built decides whether the bridge stays
usable. The naive reading — "fsync every QoS≥1 message before acking" — would be a serious
regression: today the fast path touches **no disk at all** (the spool is used only while a side
is *down*), so that reading adds an fsync per message and caps throughput at disk-commit rate.
The rule is deliberately "spool-commit **or** a downstream ack"; the "or" is where the
performance comes from. The implementation therefore:

1. **Satisfies the rule with the network on the fast path.** When the destination is connected
   the bridge does **not** spool: it forwards and waits for the destination's PUBACK, which is
   itself proof the message survived beyond this process. The source ack fires on that, with
   **zero added disk I/O** — the fast path costs what it costs today; only the *timing* of our
   ack changes.
2. **Never blocks the read loop.** The ack becomes an event, not a return value: the read loop
   hands the message off and immediately reads the next packet, while a pending-ack table
   (`source pkid → outstanding obligations`) releases the PUBACK on completion. This preserves
   **pipelining** — many messages in flight rather than one round trip at a time. The broker
   already uses this shape for its own gated publishes (`pending_publishes`/`awaiting`, ADR 0042
   T9), so it is a proven pattern here, not a new invention.
3. **Group-commits when it does spool.** While a side is down, messages must reach disk; instead
   of fsync-per-message they accumulate into **one write transaction** over a small window (~1 ms
   or N messages), committed once, releasing that batch's acks together. One fsync amortised over
   many messages — the standard group-commit trade — so the degraded path stays fast too.
4. **Bounds the in-flight window with the protocol's own flow control.** A pending-ack table
   needs a ceiling or it is unbounded memory. The bridge advertises a **Receive Maximum** on
   CONNECT so the *source broker* limits outstanding unacked messages; when the window is full
   the bridge stops reading, applying natural TCP backpressure. No bespoke queue.
5. **Handles fan-out and failure by the same counter.** A message matching several rules carries
   several obligations and is acked when the last completes. If a link drops mid-flight the
   message rolls into the spool and the ack follows that commit; if the spool write **fails**,
   no ack is sent at all and the source retries — fail closed, which is the point of the ADR.

`QoS` 0 is untouched: no ack, no promise, no added cost.

**The trade, stated plainly:** this costs **latency per message**, not throughput, and only for
`QoS`≥1 — the traffic that asked for a durability guarantee. Fast path: no extra disk I/O, added
latency ≈ one round trip to the destination, hidden by pipelining. Degraded path: one fsync per
batch. Memory: bounded by the receive-maximum window.

**Measured, not asserted.** The change lands with a bridge throughput/latency benchmark and a
regression floor in the `bench/` harness (ADR 0048), captured before and after, because a
performance claim about a hot path is exactly the kind of claim this project requires evidence
for.

## Consequences

- **Good:** at-least-once actually holds across a bridge crash; durability is explicit and
  tested; QoS≥1 can never silently run non-durable; a dropped auditable message leaves an audit
  trail; overflow policy is a stated choice, safe by default.
- **Cost:** the source ack now waits for a spool fsync (or downstream ack) — added latency on
  the ingress path, bounded by the spool write; `refuse` overflow applies backpressure to the
  source rather than shedding, which can stall a rule under sustained pressure (the intended,
  visible behaviour).
- **Risk:** built **test-first** to ADR 0025's adversarial bar. Defining tests: a crash injected
  between ack and spool-commit loses **nothing** (red today: the acked message is lost); a
  QoS≥1 rule with no durable spool **refuses to start** (or warns under the opt-in); a forced
  drop writes an audit record.

## Alternatives considered

- **Leave ack-first, document at-most-once.** Contradicts ADR 0025 §7's at-least-once promise on
  a security-crossing component; a silent downgrade of the delivery contract. Rejected.
- **Always in-memory spool (no disk).** Simple, but "durable across a transient outage" (§7) is
  then false, and a bridge host restart loses acked messages. Rejected as the default; retained
  only as the explicit `allow_ephemeral_spool` opt-in for QoS-0 or best-effort deployments.
- **Ack only after the destination acks (no spool ack path).** Correct but couples source ack
  latency to destination availability — a slow/down destination stalls all ingress even when the
  spool could absorb it. The spool-commit-or-downstream-ack rule is strictly better.

## Amendment 1 (2026-11-19): the source session is the buffer

> **Decided 2026-11-19** by the maintainer, who designed the flow: the bridge holds a
> persistent session on the source, forwards each message to the destination, acknowledges
> it at the source only after the destination has, and disconnects from the source while the
> destination is unreachable, so the source keeps the messages. Issue
> [#1103](https://github.com/mbilling/fss-mqtt-broker/issues/1103).

### Why

§5 was built in part. T1–T7 moved the source acknowledgement off arrival, but on the fast
path the bridge still acknowledged a message when it handed it to a connected destination,
not when the destination acknowledged it, and T8, which was to close that, was deferred.
Probes against in-process brokers on 2026-11-19 measured what that left:

- **A dropped destination link lost what was on the wire.** Five QoS 1 forwards written to
  the upstream, the link dropped before the PUBACK, the bridge reconnected: 0 of 5 arrived.
  The send loop ignored PUBACKs and kept no list of what was in flight.
- **A bridge restart lost what was published meanwhile.** The local side connected with
  MQTT 5 and `clean_start = false` but no Session Expiry Interval, which MQTT 5 reads as 0,
  so the session ended at every disconnect. Of 5 messages published during a restart, 0
  crossed. A plain MQTT 5 client with a 3,600-second expiry kept all 5.
- **A half-open destination stayed "connected".** PINGRESPs were never checked, so a broker
  that vanished without a reset was noticed only when TCP gave up, about 15 minutes with
  Linux defaults. Meanwhile the router kept acknowledging the source into an unbounded
  queue, and the spool was never used because the side still counted as up.
- **A replay deleted before it delivered.** `drain` removed spooled messages from disk
  before sending them, so a crash or a second drop during the replay lost them.

### Decision

**1. For traffic from the local cluster, the local session is the buffer.** The bridge keeps
no copy of an outbound message on disk. Per upstream:

1. Connect to the local cluster with `clean_start = false` and a Session Expiry Interval
   (`[local] session_expiry_secs`, default 4,294,967,295, which MQTT 5 defines as never). If
   the session is new, subscribe without retained messages. Disconnect.
2. Connect to the upstream, retrying with backoff.
3. Connect to the local cluster. If the session is gone (`session_present = 0`), count and
   audit the gap, and subscribe again.
4. Forward each message as it arrives.
5. Acknowledge a message at the local cluster when the upstream has acknowledged every
   forward it produced, in the order the messages arrived [MQTT-4.6.0-2].
6. When the upstream link fails (an error, a close, or no packet within the keepalive),
   disconnect from the local cluster without acknowledging what is pending. The local
   cluster resends it, with DUP set, on the next connect. Go to 2.
7. When the local link fails, discard the pending acknowledgements, so none is ever sent on
   another connection, and go to 3.

A QoS ≥ 1 `out` or `both` rule therefore needs a local session that outlives a disconnect:
`session_expiry_secs = 0` is refused for it. It no longer needs `[spool].dir`.

**2. One link per upstream.** Each upstream gets its own local session, with its own client id
and subscriptions, so an unreachable upstream stops only its own traffic. With more than one
upstream, a configured local client id gets `.<upstream name>` appended, a generated one
encodes the upstream, and a `$share` group gets `.<upstream name>` appended so each link is
its own group. A message the bridge brings in from upstream A is then an ordinary local
message: if upstream B has an `out` rule that matches it, the bridge forwards it to B, and the
hop count bounds any loop. Before, one local connection with No Local kept a single bridge
from doing that, although a second bridge process would. The bridge warns at startup for each
pair of rules where it can happen.

**3. A window bounded in bytes, not messages.** The bridge does not lower the local session's
Receive Maximum below the protocol's 65,535. It bounds what it holds by bytes instead: past
`max_inflight_bytes` per direction of a link (default 64 MiB) it stops reading the source,
and it reads again below half of that. Toward the destination it keeps to the Receive
Maximum the destination's CONNACK grants.

**4. Keepalive.** A connection that has received nothing from its broker for one keepalive
interval, or whose write has blocked that long, is closed. A connect attempt times out after
10 seconds. The keepalive is the endpoint's `keep_alive_secs` (default 30), or the broker's
Server Keep Alive when it sends one.

**5. Refusals.** A PUBACK whose reason code says not authorized (0x87), topic name invalid
(0x90) or payload format invalid (0x99) is permanent: the message is acknowledged at the
source, audited, and counted as `fss_bridge_dropped_total{reason="refused"}`. Any other error
code is temporary: the message is published again as a new message, after 1 second at first
and at most 30 seconds, and its source acknowledgement waits.

**6. Retained messages.** On a resumed session the bridge subscribes with retain handling 1,
so a reconnect does not resend every retained message. When the session is new or was lost
it uses retain handling 0, so the current retained state crosses. No Local is never set on a
`$share` subscription, where MQTT 5 makes it a protocol error [MQTT-3.8.3-4].

**7. Traffic from an upstream keeps the spool.** An upstream's session stays clean by default,
because a partner broker may not allow a persistent one, and leaving a clean session loses
what is published meanwhile. So the bridge stays connected to the upstream and:

- while the local link is up, forwards each message and acknowledges it at the upstream after
  the local cluster's PUBACK, in order;
- while the local link is down, spools each message (fsync) and acknowledges it after the
  commit;
- when the local link fails, spools what it was forwarding, then acknowledges it;
- replays the spool oldest first when the local link is back, deleting each message when the
  local cluster acknowledges it, and only then forwards newer messages.

If the spool refuses a message, the bridge holds it in memory unacknowledged, behind the
byte bound of §3. Each upstream has its own spool file. Setting `session_expiry_secs` on an
upstream makes that session persistent, so messages the upstream has sent but the bridge has
not acknowledged survive a bridge restart.

**8. Upgrades.** Spools an older bridge left behind are drained, not dropped. An upstream's
old spool (`spool-upstream-<i>.redb`) is sent to that upstream before the link's local
session connects, deleting each message as the upstream acknowledges it. The old shared
`spool-local.redb` is moved into the first link's spool at startup.

### Consequences

- **Good:** for traffic from the local cluster, at-least-once holds across a bridge crash or
  restart, a dropped upstream link and a half-open one, with nothing on the bridge's disk. The
  bridge holds at most `max_inflight_bytes` per direction of a link, and an unreachable
  upstream no longer stalls the others.
- **Cost:** a source acknowledgement waits for one round trip to the destination, which the
  window hides. Every failure can duplicate messages, as at-least-once allows. The local
  cluster carries one session per upstream.
- **The broker's limits are the bridge's limits.** While an upstream is unreachable, the local
  session's queue is the only buffer, so the broker's session limits decide how long an outage
  it rides out. Issue [#1235](https://github.com/mbilling/fss-mqtt-broker/issues/1235) replaces
  their message counts with memory and disk limits.
- **Changed behaviour:** relaying between upstreams through the local cluster (§2); local client
  ids and `$share` groups that name the upstream when there are several; per-link local sides in
  the metrics when there are several upstreams.
- **A retired bridge's session outlives it.** With the default expiry, a decommissioned bridge's
  local session keeps queueing until it is deleted. Set a finite `session_expiry_secs` to have
  it expire.

### Alternatives considered

- **Spool every outbound message before acknowledging the source.** §5 already rejected a disk
  commit per message.
- **Stay connected to the source and stop acknowledging** while the destination is down. The
  broker then treats the bridge as a slow subscriber, not an offline one. Beyond the window and
  its in-memory backlog it drops the oldest (issue #1235), and under `$share` it keeps dealing
  messages to the stalled member.
- **One local session for every upstream.** One unreachable upstream would stall the others
  behind its unacknowledged messages, which is why §5 kept the spool.
- **Receive Maximum 1**, taking one message at a time. One message per round trip: about
  50 msg/s at 20 ms.

### Left open

- **Disconnecting from a persistent upstream** while the local link is down, so traffic from
  that upstream uses the flow of §1 instead of the spool.
- **Group commit** (§5.3) for the spool.

### Delivery

Task 0060-T9 in the [delivery doc](../delivery/0060-bridge-durability-and-ack-contract.md),
issue [#1103](https://github.com/mbilling/fss-mqtt-broker/issues/1103). It supersedes T8.
