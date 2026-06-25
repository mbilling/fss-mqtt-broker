# ADR 0030 — Forward MQTT 5 User Properties through delivery

- **Status:** Accepted
- **Date:** 2026-06-25
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0030-user-property-forwarding.md](../delivery/0030-user-property-forwarding.md) — plan, progress, and changelog
- **Related:** [ADR 0008](0008-mqtt-5-codec.md) (the property codec this forwards),
  [ADR 0014](0014-cross-node-delivery.md) / [ADR 0015](0015-cluster-shared-subscriptions.md)
  (the cross-node + shared-subscription delivery paths the properties must traverse),
  [ADR 0018](0018-on-disk-persistence.md) (the durable queue codec extended here),
  [ADR 0025](0025-boundary-bridge.md) (the boundary bridge whose hop-count loop-prevention
  depends on this — the change that surfaced the gap)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0030-user-property-forwarding.md).

## Context

MQTT 5 §3.3.2.3.7 is unambiguous: **"The Server MUST send all User Properties unaltered in
a PUBLISH packet when forwarding the Application Message to a Client [MQTT-3.3.2-17]."** Our
broker does not. The internal [`Message`](../../crates/mqtt-core/src/lib.rs) type carries
only `topic`/`payload`/`qos`/`retain`, and the delivery constructor
[`publish_packet`](../../crates/mqttd/src/hub.rs) rebuilds a fresh `Properties` block
holding only the message-expiry interval. So a publisher's User Properties are **silently
dropped** the moment a PUBLISH is ingested — they never reach any subscriber, local or
cross-node.

This was surfaced while building the boundary bridge (ADR 0025): its loop-prevention
mechanism increments an `fss-bridge-hop-count` **User Property** on each forward and drops a
message that reaches the hop limit, which bounds any multi-bridge cycle. That only works if
User Properties survive a hop *through a broker* — and through *our* broker they do not. The
gap is therefore both a standalone conformance defect and a hard blocker for ADR 0025-T5.

## Decision

**Carry the publisher's User Properties on the internal `Message` and re-emit them on every
delivery path** — single-node, cross-node (ADR 0014), shared-subscription (ADR 0015), and
on replay from the durable/offline queue (ADR 0018) — so a subscriber receives them
unaltered, as the spec requires.

### 1. Representation

`Message` gains `user_properties: Vec<(String, String)>` — the ordered key/value pairs from
the inbound PUBLISH's User Properties, in wire order (order is significant and preserved per
ADR 0008). Plain `(String, String)` tuples, not the codec's `Property` enum, because the
value must serialize over the cluster peer wire (bincode) and persist in the durable queue
codec; `Property` is a codec type without those derives. Empty vec = none (the common case,
zero overhead).

**User Properties** ship first (T1–T4) — the explicit `MUST` and the bridge's actual need —
then the other message-level application properties the spec also forwards (Payload Format
Indicator, Content Type, Response Topic, Correlation Data) follow in T5, bundled with User
Properties into one `AppProperties` value carried on `Message`. Topic Alias and Subscription
Identifier are deliberately **not** forwarded: they are hop-by-hop / per-subscription, not
part of the application message.

### 2. Ingestion and delivery

On ingest, the connection/hub copies the inbound PUBLISH's User Properties into the
`Message`. On delivery, `publish_packet` emits them alongside the existing message-expiry
property. A Will message's User Properties are captured the same way, so a published Will
also forwards them.

### 3. Across the cluster and the durable queue

`PeerMessage::Publish` and `PeerMessage::SharedDeliver` gain a `user_properties` field
(serde, like the existing `message_expiry`), so a cross-node or shared delivery re-emits the
originating publisher's properties. The durable queued-message codec
([`logged.rs`](../../crates/mqtt-storage/src/logged.rs)) is extended **backward-compatibly**
— the property pairs are appended after the existing fields, and a record written by an
older build (no trailing bytes) decodes to an empty set — the same length-prefix-then-EOF
discipline already used for the session-expiry field.

### 4. Bounds

User Properties are bounded by the broker's inbound maximum packet size (ADR 0008 / the
connection wire limits): a PUBLISH that carries them was already size-capped on the wire, so
forwarding and persisting them adds no *unbounded* vector — per-message storage grows only
within the existing packet-size cap. No separate cap is introduced; if a deployment needs a
tighter bound it lowers the maximum packet size.

## Consequences

- **Good:** conformance with MQTT-3.3.2-17; User Properties (request/response metadata,
  tracing tags, and the bridge's hop counter) now reach subscribers and survive cross-node,
  shared, and offline-queue delivery. ADR 0025-T5 becomes implementable end to end.
- **Cost:** a new field on `Message` and on two peer-wire variants, an extended (still
  backward-compatible) durable codec, and the ingestion/delivery plumbing — a cross-cutting
  but mechanical change, built test-first.
- **Risk:** low. The codec extension is append-only and EOF-defaulted (proven pattern); the
  peer-wire additions are serde fields; no behaviour changes for messages without User
  Properties. The remaining application properties (T6) are out of scope and explicitly
  tracked rather than silently omitted.

## Alternatives considered

- **Forward all application properties at once** (incl. content type, response topic,
  correlation data, payload format). More complete, but larger; User Properties are the
  explicit `MUST` and the only one the bridge needs, so they ship first and the rest are a
  tracked follow-up rather than a bundled, slower change.
- **A side channel for the bridge hop count** (topic prefix / payload envelope). Avoids the
  broker change but is non-conformant, leaks bridge mechanics into the topic/payload space,
  and does not interoperate with external brokers that *do* forward User Properties.
  Rejected: fixing the actual conformance gap is the cleaner, reusable solution.
- **Leave it; scope the bridge hop-count to external brokers only.** Honest but accepts a
  permanent in-cluster loop-prevention gap and a standing spec violation. Rejected in favour
  of fixing the broker first.
