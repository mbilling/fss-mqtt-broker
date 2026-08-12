# ADR 0060 — Bridge durability and acknowledgement contract

- **Status:** Proposed
- **Date:** 2026-08-11
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0060-bridge-durability-and-ack-contract.md](../delivery/0060-bridge-durability-and-ack-contract.md) — plan, progress, and changelog
- **Amends:** [ADR 0025 §7](0025-boundary-bridge.md) (store-and-forward), which promised at-least-once but left the ack/durability ordering unspecified.
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
