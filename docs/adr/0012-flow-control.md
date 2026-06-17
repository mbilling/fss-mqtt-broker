# ADR 0012 — MQTT 5.0 flow control (Receive Maximum)

- **Status:** Accepted (design); implementation phased (workstream G)
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Related:** [ADR 0001](0001-session-durability.md) (offline queue / in-flight
  state), [ADR 0008](0008-mqtt-5-codec.md) (the v5 wire),
  [ADR 0009](0009-mqtt5-expiry.md) (queued-message expiry, which interacts with the
  backlog), [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md) workstream G

## Context

MQTT 5.0 **Receive Maximum** (`0x21`, in CONNECT and CONNACK) caps how many QoS 1
and QoS 2 PUBLISH packets may be **in flight** — sent but not yet fully
acknowledged — in one direction at a time. It is the protocol's back-pressure
mechanism: a slow consumer advertises a small Receive Maximum and the sender must
stop after that many unacked publishes until acknowledgements free the quota.
There is no flow control for QoS 0.

The two directions are independent and each side advertises the limit it imposes on
*the other*:

- the **client's** CONNECT Receive Maximum bounds how many unacked QoS > 0
  PUBLISHes the **server** may have outstanding to that client (server → client);
- the **server's** CONNACK Receive Maximum bounds the reverse (client → server).

A value of `0` is a protocol error; an absent value means 65535.

Today the hub's per-session in-flight table (`Inflight.pending`) grows without
bound: `send_to_client` allocates a packet id and writes immediately, however far
behind the client is. A slow or stalled QoS > 0 consumer therefore forces unbounded
memory and unbounded packet-id pressure. Receive Maximum is exactly the lever to
fix this, and a v5 client that advertises one is entitled to have it honoured.

## Decision

### 1. Enforce the **server → client** direction in the hub (the bounding one)

The hub already owns per-session in-flight state, so it is where outbound quota
lives. Each session records the client's advertised Receive Maximum (default 65535;
v3.1.1 has no such property, so it is effectively unlimited). The number of
QoS > 0 PUBLISHes in flight is exactly `Inflight.pending.len()` — every entry is one
unacked publish, held from PUBLISH until PUBACK (QoS 1) or PUBCOMP (QoS 2).

When `send_to_client` would deliver a QoS > 0 message and `pending.len()` already
equals the Receive Maximum, the message is **not** put on the wire. Instead it is
appended to a per-session **backlog** (an in-memory FIFO). When a PUBACK or PUBCOMP
later frees a slot, the hub drains the backlog — assigning a packet id and sending —
up to the quota again. QoS 0 is never throttled (no acknowledgement, no quota).

### 2. The backlog is not lost across a disconnect

For a persistent session, backlog entries (never sent, so never assigned a packet
id) are flushed to the durable offline queue on detach, exactly as a message that
arrived while the client was offline would be. They then replay on reconnect. The
already-sent-but-unacked `pending` entries keep their existing behaviour:
redelivered with DUP on resume. A clean or expired session drops its backlog with
the rest of its state.

### 3. Advertise — but do not yet strictly enforce — the **client → server** direction

The CONNACK advertises a server Receive Maximum (`SERVER_RECEIVE_MAXIMUM`) so a
conforming client paces what it sends us. The broker acknowledges inbound QoS > 0
publishes promptly (PUBACK/PUBREC are sent as the packet is handled), so its own
inbound in-flight count stays at or below what it advertises in normal operation
and there is nothing to buffer. Detecting a *misbehaving* client that overruns the
limit and answering with DISCONNECT reason `0x93` (Receive Maximum exceeded) is
folded into the later "act on v5 reason codes" work, together with the other
protocol-violation disconnects.

### 4. Quota counts publishes, not packet ids

The in-flight count is the number of `pending` entries, which is the count of
unacked QoS > 0 PUBLISHes — the precise quantity Receive Maximum bounds. A QoS 2
publish occupies one slot for its whole PUBLISH → PUBREC → PUBREL → PUBCOMP life,
released only at PUBCOMP, matching the spec's "until PUBCOMP" rule.

## Consequences

- **Good:** a slow QoS > 0 consumer can no longer force unbounded in-flight growth;
  honours a client's advertised Receive Maximum; persistent backlog survives
  reconnect via the existing offline queue; reuses the in-flight table and ack paths
  with no new wire state.
- **Cost / limits:** the backlog is in memory while the client is online (bounded in
  practice by consumer lag; an explicit cap with an overload policy mirrors the
  offline queue and can follow); a held backlog entry forwards its Message Expiry
  Interval as captured at enqueue, so a long hold slightly over-states the remaining
  lifetime (ADR 0009); the inbound direction is advertised but not strictly enforced
  (§3), pending the reason-code work.

## Alternatives considered

- **Drop QoS > 0 messages over quota.** Rejected: silently losing deliverable
  messages to a session that is merely slow violates the QoS contract; back-pressure
  via a backlog is the point of the feature.
- **Block the hub until the client acks.** Rejected: the hub is a single actor
  serving every session; blocking on one slow consumer would stall all of them. The
  backlog keeps the hub non-blocking.
- **Reuse the offline queue for the online backlog.** Rejected as the primary
  mechanism: its offset-based replay/ack model is built for offline accumulate then
  bulk replay, not incremental per-message online draining. It is used only as the
  durable spill on detach (§2), where its semantics fit.
