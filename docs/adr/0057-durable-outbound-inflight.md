# ADR 0057 — Durable outbound in-flight state: exactly-once across a broker crash

**Status:** Proposed
**Date:** 2026-08-10
**Relates to:** ADR 0005 (sessions), ADR 0018 (durable replication), #124 (in-flight
durability), #130 (this decision's tracking issue)

## Context

The #124 fix made the *message* durable before it reaches the wire: a QoS 1/2 delivery to
a persistent subscriber survives a broker crash and is replayed on resume. What is not
durable is the **packet id it was in flight under and its acknowledgement phase**.
`Hub::inflight` holds `PendingOut { message, state, offset }` in memory; after a restart
the message is replayed under a **fresh** id.

For QoS 1 that is spec-legal and harmless: the redelivery carries `DUP` and at-least-once
allows a duplicate.

For QoS 2 it breaks exactly-once in precisely the window it is bought for. A subscriber
that has already PUBRECed under the old id cannot match the redelivery — its dedup window
keys on the packet id — so the application sees the message twice. Exactly-once currently
holds across a *client* reconnect but not across a *broker* crash. The README's
Limitations section says so.

The inbound direction already has the durable shape this needs: `record_received` /
`clear_received` / `received` on `SessionStore` maintain the incoming QoS 2 dedup window,
and that state replicates with the session. The outbound equivalent does not exist.

## Decision

### 1. Persist outbound in-flight state for QoS 2 only

`SessionStore` gains the outbound mirror of the inbound window:

- `record_outbound(client, packet_id, offset, phase)` — written when the id is allocated
  (phase `AwaitingPubRec`) for a QoS 2 delivery
- `advance_outbound(client, packet_id, phase)` — on PUBREC (phase → `AwaitingPubComp`),
  i.e. "PUBREL territory": the subscriber has the message; only the release handshake
  remains
- `clear_outbound(client, packet_id)` — on PUBCOMP
- `outbound(client) -> Vec<(packet_id, offset, phase)>` — read at session restore

QoS 1 deliveries are **deliberately excluded**: a fresh-id `DUP` redelivery is what
at-least-once means, and persisting ids for it would buy nothing while doubling the write
surface. This keeps the added cost proportional to actual QoS 2 traffic, which is the
traffic that asked for exactly-once by name.

### 2. On by default — correctness is not an opt-in here

The alternative — a `strict_qos2` flag, off by default — was considered and rejected.
This broker's headline is that an acknowledged fact survives; QoS 2 is the protocol's
spelling of "I will pay for exactly-once". A default where exactly-once quietly means
"unless the broker restarts" is the kind of asterisk this project exists to remove, and
the cost lands only on QoS 2 flows, which are rare and already the slow path (a
four-packet handshake). If measurement (below) shows the cost is unacceptable, the
fallback position is a flag to *disable* it — loudly — not one to enable it.

### 3. Restore reconstructs the handshake mid-phrase

On session resume after a restart:

- entries in `AwaitingPubRec`: resend the PUBLISH with `DUP` under the **original** id
  (the subscriber may or may not have seen it; its dedup window matches the id either way)
- entries in `AwaitingPubComp`: send **PUBREL** under the original id — never a second
  PUBLISH, because the subscriber holds the message and is waiting for release
- the packet-id allocator seeds itself past every restored id, so a new delivery cannot
  collide with a restored one

### 4. Measured before merge, in the existing harness

The per-delivery cost is one additional durable write at allocation, one at PUBREC, one
delete at PUBCOMP, on QoS 2 flows only. The delta is recorded in the delivery
document (T5). **Measured 2026-08-11**: the QoS 2 persistent-durable lane roughly doubles
in wall time (dev-grade, single machine), the QoS 1 control lane is unchanged, and the
verdict is **defensible — Decision 2 stands**. One correction to the original plan here:
the comparative bench lane runs durable-off with clean sessions, where this code never
executes, so measuring there would have reported a zero delta without running the code. A
dedicated harness exercising exactly the changed path was used instead.

### 5. Schema

Pre-1.0: the session-store schema changes freely (ADR 0039 applies from 1.0.0), so the
outbound table is added without a version bump, matching the standing decision that all
planes reset to schema 1 at first release.

## Acceptance

`crates/mqttd/tests/inflight_durability.rs` gains the QoS 2 shape from #130: subscriber
PUBRECs, broker SIGKILLed, and on resume the subscriber receives **PUBREL for the id it
already knows** — not a second PUBLISH under a new one. A companion case covers the
`AwaitingPubRec` phase (crash before PUBREC arrives → PUBLISH redelivered with `DUP`
under the original id).

## Tasks

| id | title |
|----|-------|
| 0057-T1 | `SessionStore` outbound in-flight table: record / advance / clear / read, replicated with the session |
| 0057-T2 | Hub wiring: write at allocation and PUBREC, clear at PUBCOMP, fail closed with the ack withheld when the write fails |
| 0057-T3 | Restore: rebuild `pending` from the table, resume at PUBLISH+DUP or PUBREL under the original id, seed the allocator past restored ids |
| 0057-T4 | The SIGKILL acceptance test, both phases |
| 0057-T5 | Measure the QoS 2 delta in the bench lane; record it; revisit Decision 2 if indefensible |
| 0057-T6 | Remove the Limitations entry the fix retires — the README claim and the code must change together |
