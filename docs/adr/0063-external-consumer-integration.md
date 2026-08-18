# ADR 0063 — External integration without a rule engine: the consumer-group pattern

- **Status:** Accepted
- **Date:** 2026-08-18
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0063-external-consumer-integration.md](../delivery/0063-external-consumer-integration.md) — plan, progress, and changelog
- **Related:** [ADR 0010](0010-shared-subscriptions.md) / [ADR 0015](0015-cluster-shared-subscriptions.md)
  (the shared-subscription selection and cluster-wide single delivery the pattern rides on),
  [ADR 0029](0029-durable-by-default.md) (the durable session queue that is the pattern's
  buffer), [ADR 0041](0041-resource-governance.md) (the queue cap and its overflow
  policies — the pattern's stated loss boundary), [ADR 0012](0012-flow-control.md)
  (backpressure), [ADR 0030](0030-user-property-forwarding.md) (the message-id carrier for
  sink-side dedupe), [ADR 0025](0025-boundary-bridge.md) / [ADR 0059](0059-bridge-ha-topology-and-ordering.md)
  / [ADR 0060](0060-bridge-durability-and-ack-contract.md) (the bridge the pattern composes
  with at a trust boundary, and whose ordering/ack lessons it reuses),
  [ADR 0058](0058-one-dot-zero-stability-contract.md) (why growing a sink surface now is the
  wrong trade), issue #251 (the review finding this answers).

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0063-external-consumer-integration.md).

## Context

mqttd has no rule engine and no non-MQTT sinks: the bridge is MQTT-to-MQTT only, and
`grep -ri kafka crates/` returns nothing. `docs/COMPARISON.md` said "✖ not planned —
boundary bridge + standard integrations instead", but no document said what those
"standard integrations" *are*. The 2026-08-13 review panel called that the single
largest functional gap (issue #251): for an EMQX migrator every SQL rule becomes a
service they must design unaided, and "documented honestly" does not help when there is
nothing documented to design against. An unsubstantiated claim is a defect; this is the
substantiation.

## Decision

1. **No built-in rule engine, and no built-in Kafka/webhook sinks — reaffirmed.**
   mqttd is a broker, not an integration platform (the posture ADR 0020 set for
   dashboards and ADR 0025 set for connectors). Everything the broker ships is held to
   its durability, refusal and audit contracts; a sink half-owned by the broker would
   either dilute those contracts or grow a second product inside this one — during the
   pre-1.0 surface freeze (ADR 0058), the wrong trade in both directions.

2. **The supported integration path is the external-consumer pattern, and it is a
   committed, tested deliverable — not a shrug.** A sink (Kafka, webhook, DB, anything)
   is an ordinary MQTT consumer group: `$share/<group>/<filter>` at QoS 1 with durable
   persistent sessions and a least-privilege subscribe-only identity, acking each
   message only after the sink accepted it, deduping on a publisher-assigned id.
   The broker is the buffer — queue-while-down, quorum-replicated, bounded and
   monitored — so the adopter builds a stateless forwarder, not a store.
   The reference architecture, its guarantees **and its refusals** (the queue cap's
   overflow behaviour, `$share` ordering, the retained-replay exclusion) live in
   [docs/INTEGRATION.md](../INTEGRATION.md); COMPARISON and the migration guide point
   there instead of at a blank.

3. **The blueprint is held true by CI.** Its load-bearing claims — group single
   delivery (including a 3.1.1 member), retained state skipping shared subscriptions,
   and in-order queue-while-down replay — run against the real binary in
   `scripts/integration-consumer-smoke.sh`, same contract as the quickstart and
   cutover smokes: if the document stops being true, the build fails.

## Revisit trigger

First-class sinks *on the bridge* (a Kafka producer / HTTP POST destination driven by
the bridge's existing deny-by-default rules and durable spool) remain the scoped
follow-up — issue #251's "acceptable exit 1" — to be taken up when an adopter asks,
not speculatively. Nothing in this decision forecloses it: the pattern documented here
is also the semantic spec such a sink would have to meet (at-least-once,
ack-after-durable, idempotent write).

## Consequences

- **Good:** the rule-engine replacement story is a designed, executable blueprint; the
  COMPARISON claim is substantiated; every mechanism used is already load-bearing and
  tested elsewhere in the product, so the pattern inherits maintained guarantees
  instead of adding new surface.
- **Cost, stated plainly:** the adopter still writes and operates the consumer
  (transform-as-code, sink credentials, deploys); there is no SQL DSL and no per-rule
  broker-side metrics. A team whose dominant cost is operating even small services is
  better served by a rule-engine broker, and COMPARISON continues to say so.
