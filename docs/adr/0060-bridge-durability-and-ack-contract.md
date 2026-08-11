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
