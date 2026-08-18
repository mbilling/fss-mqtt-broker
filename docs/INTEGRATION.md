# External consumers: the integration blueprint

How to get messages out of mqttd and into the rest of your stack — Kafka, a
webhook, a database, anything — without a rule engine, and what the broker
does and does not promise while you do it ([ADR 0063](adr/0063-external-consumer-integration.md)).

mqttd has no SQL rule engine and no built-in Kafka/HTTP sinks, **by design**:
it is a broker, not an integration platform, and everything it ships is held
to the same durability and refusal contracts. What replaces the rule engine
is a *pattern*, not a plugin — an ordinary MQTT consumer group, built on
three broker features that are already load-bearing elsewhere:

- **cluster-wide shared subscriptions** ([ADR 0010](adr/0010-shared-subscriptions.md),
  [ADR 0015](adr/0015-cluster-shared-subscriptions.md)) — a `$share` group is
  delivered each message **once, cluster-wide**, round-robin across members;
- **durable persistent sessions** ([ADR 0029](adr/0029-durable-by-default.md)) —
  a QoS 1 message for a disconnected persistent member is **queued,
  quorum-replicated**, and replayed on reconnect;
- **deny-by-default ACLs** ([ADR 0004](adr/0004-identity-and-authentication.md)) —
  the sink gets a least-privilege, subscribe-only identity, exactly like the
  [bridge](BRIDGE.md) gets one.

The pattern's load-bearing claims are executed in CI against the real binary
by [`scripts/integration-consumer-smoke.sh`](../scripts/integration-consumer-smoke.sh),
so this document cannot silently rot.

---

## The shape

```
   ┌──────────────────────────────────────┐
   │  mqttd cluster                       │
   │   ┌────────┐  ┌────────┐  ┌────────┐ │      each message delivered ONCE,
   │   │ node-0 │  │ node-1 │  │ node-2 │ │      cluster-wide (ADR 0015),
   │   └────────┘  └────────┘  └────────┘ │      round-robin across the group
   └────────┬────────────────────┬────────┘
            │                    │
            │   subscribe  "$share/sink/telemetry/#"   qos 1
            │                    │        clean_start = false
            ▼                    ▼        session-expiry: long
  ┌────────────────────┐  ┌────────────────────┐
  │  consumer-0        │  │  consumer-1        │   ordinary MQTT clients —
  │  (your code, or a  │  │                    │   scale out by adding
  │  stock connector)  │  │                    │   members
  └─────────┬──────────┘  └──────────┬─────────┘
            │  produce / POST        │
            ▼                        ▼
  ┌─────────────────────────────────────────┐
  │  Kafka topic / webhook / DB / …         │   idempotent write keyed by a
  │  (the sink dedupes; see §exactly-once)  │   producer-assigned message id
  └─────────────────────────────────────────┘
```

While the **whole group is disconnected** — a deploy, a sink outage you chose
to stop consuming through, a crash — matching QoS 1 messages are queued to a
persistent member's durable session and replayed when it returns. The broker
is the buffer; you do not build one.

## The contract — five settings, each load-bearing

| Setting | Value | Why it is load-bearing |
|---|---|---|
| Topic filter | `$share/<group>/<filter>`, with an **explicit** filter per stream | The group gets single delivery; a narrow filter is what the sink's ACL grant will mirror |
| QoS | **1** (subscribe *and* the publishers) | QoS 0 promises nothing — a disconnected group is then a silent gap. Delivery is `min(publish QoS, granted QoS)`, so a QoS 0 *publish* is still QoS 0 end to end |
| `clean_start` | `false` | A clean-start session evaporates on disconnect, and everything queued with it |
| Session expiry | Longer than your worst credible group outage | Queueing-while-down lasts exactly as long as the session does |
| Client id | Distinct **per member**, stable across restarts | MQTT keys the session by client id; two members sharing one id evict each other in a takeover loop (measured on the bridge — [BRIDGE.md](BRIDGE.md), ADR 0025 T14) |

With `mosquitto_sub` for a first try:

```sh
mosquitto_sub -V mqttv5 -q 1 -c -x 604800 -i sink-0 -t '$share/sink/telemetry/#' -v
```

`$share` is an MQTT 5 feature; mqttd also honours it for 3.1.1 clients (as
Mosquitto and EMQX do), which matters because several stock connectors still
speak 3.1.1 — a 3.1.1 member joins the same group, and `CleanSession=0` gives
it a session that does not expire.

## What the broker guarantees — and what it refuses to pretend

**At-least-once, cluster-wide-once selection.** Each matching QoS 1 message is
delivered to **one** group member: an online member if any, else it is
**queued to a persistent offline member** for replay on reconnect, else — no
member with a live session at all — it is dropped for that group
(ADR 0010 §2). At-least-once means *duplicates are possible* (a redelivery
after an unacked crash, a replayed queue); the sink dedupes (see
[exactly-once-ish](#exactly-once-ish-the-recipe)).

**The queue is durable but bounded.** A session's queue is quorum-replicated
(survives node loss) and capped — `MQTTD_MAX_QUEUED_MESSAGES`, default
100 000 per session ([ADR 0041](adr/0041-resource-governance.md)). At the cap
the default `drop-oldest` policy **acks and drops** the oldest already-queued
entries (counted `mqttd_publish_dropped_total{reason="queue-overflow"}`);
`reject-newest` sheds the newest instead. Size the cap against
`outage duration × publish rate`, and alert on that counter — it is the
pattern's actual data-loss signal.

**Ordering.** Within one member's session, per-topic order holds. But
`$share` load-balances **per message**, so two members interleave a topic's
stream — same as every broker's shared subscriptions, and same reason the
bridge's HA default is not `$share` ([ADR 0059](adr/0059-bridge-ha-topology-and-ordering.md)).
If per-topic order matters end to end:

- **one member per group** (order preserved; scale by splitting filters into
  more groups), or
- **partition by topic**: N members, each subscribing a *plain* (non-shared)
  filter set it owns — e.g. `hash(topic) mod N` agreed in your deployment, or
  a static split (`telemetry/eu/#` vs `telemetry/us/#`). One topic, one
  owner, order preserved — the bridge's `partitioned` topology, reused. In
  Kafka, finish the job by keying the produce on the MQTT topic, so one MQTT
  topic lands in one partition.

**Retained messages do not reach a shared subscription** ([MQTT-3.8.4] —
ADR 0010 §3; also true of the bridge's `shared` mode). A sink that needs the
current retained state at startup does a one-shot **plain** subscribe first
(retained values replay to it), then switches to the group for the live
stream.

**Backpressure is flow control, not disconnection.** A slow consumer paces
the broker with MQTT 5 Receive Maximum ([ADR 0012](adr/0012-flow-control.md));
unacked in-flight messages beyond the window wait. A consumer that would
rather queue than throttle can simply disconnect — that is what the durable
queue is for.

**QoS 2 is not the exactly-once answer here.** mqttd supports it, but the
exactly-once window closes at the *MQTT client* — the hop into Kafka or the
webhook is outside it, and a crash between MQTT ack and sink write still
duplicates or loses. The honest recipe is QoS 1 plus an idempotent sink,
which is also what QoS 2 pipelines end up needing anyway.

## Exactly-once-ish: the recipe

At-least-once transport + idempotent sink = effectively-once. Three rules:

1. **The publisher assigns the identity.** Put a unique message id where it
   survives the pipeline — an MQTT 5 **user property** (forwarded intact,
   [ADR 0030](adr/0030-user-property-forwarding.md)) or a payload field.
   Packet ids cannot serve: they are per-connection and recycled.
2. **Ack only after the sink accepted it.** The consumer sends its PUBACK
   *after* the Kafka produce is acknowledged / the webhook returned 2xx —
   never before. (Client libraries call this manual ack; in paho-python,
   `manual_ack=True`.) A crash before the ack means redelivery, which rule 3
   absorbs. This is the same ack-after-durable contract the bridge adopted
   ([ADR 0060](adr/0060-bridge-durability-and-ack-contract.md)).
3. **The sink write is idempotent on that id.** Kafka: enable the idempotent
   producer (dedupes *retries*, not *redeliveries*) **and** key the record on
   the message id — compaction or a consumer-side seen-set gives you the
   effectively-once read. Webhook: send the id as an `Idempotency-Key`
   header and have the receiver treat a repeat as success. Database: upsert
   on the id.

## The Kafka lane

Two ways in, same contract either way:

**A stock connector.** Any Kafka Connect MQTT source connector (or an
MQTT-source feature of your streaming platform) works against mqttd if it can
be configured to: subscribe `$share/<group>/<filter>` (to a connector this is
just a topic string), QoS 1, persistent session, distinct client id per
connector task. mqttd deliberately has no connector-side surface to
integrate with — to the connector it is just a spec-conforming broker
(foreign-client conformance is CI-gated,
[ADR 0034](adr/0034-foreign-client-interop-conformance.md)). Check the task
count against the ordering section above: tasks sharing one group interleave
topics.

**A forwarder you own.** ~40 lines with an MQTT client library and a Kafka
producer, and you get rule-engine "transform" for free as ordinary code. A
sketch (the *contract* is the numbered rules above and the settings table —
this shows where each lands; it is not a tested program):

```python
# paho-mqtt + confluent-kafka, sketched
producer = Producer({"bootstrap.servers": "kafka:9092",
                     "enable.idempotence": True})     # dedupes producer RETRIES

def on_message(client, _, msg):                # called with manual acks enabled
    event = transform(msg)                     # your "rule": reshape, filter, enrich
    if event is None:
        client.ack(msg)                        # filtered out on purpose: ack, move on
        return
    producer.produce("telemetry",
                     key=msg.topic,            # key = MQTT topic → per-topic order
                     value=event,
                     on_delivery=lambda err, _rec:
                         err is None and client.ack(msg))   # ack ONLY after Kafka has it

# connect: MQTT 5, clean_start=False, a long session-expiry property,
# client_id "kafka-sink-0" (distinct per copy); subscribe
# "$share/kafka/telemetry/#" at QoS 1; then loop forever.
```

Run N copies for throughput (distinct client ids), or keep one per group and
scale by splitting filters if you need strict per-topic order without keying.

## The webhook lane

Same consumer shape; the sink write is an HTTP POST:

- POST with the message id as `Idempotency-Key`; treat only 2xx as delivered,
  then ack.
- On failure, **do not ack** — back off and retry; the message (and the
  in-flight window behind it) waits, and if the consumer gives up and
  disconnects, the durable queue holds the stream.
- For a poison message (a permanent 4xx), republish it to a dead-letter MQTT
  topic (`dlq/<original topic>`) **at QoS 1, wait for that PUBACK, then ack
  the original** — the DLQ inherits the broker's durability instead of a side
  file, and a DLQ consumer is just this pattern again, one level down.

There is precedent for "one HTTP hook instead of N integrations" in mqttd
itself: authentication does exactly this (`MQTTD_HTTP_AUTH_URL`, ADR 0004).

## Crossing a trust boundary: compose with the bridge

Everything above assumes the consumer may attach to the cluster. When the
data platform lives in **another security zone**, do not stretch a consumer's
credentials across the boundary — put the [boundary bridge](BRIDGE.md)
(ADR 0025) at the edge, exactly as designed: a one-way `out` rule for the
streams that may leave, its fsync-durable spool riding out far-side outages
(ADR 0060), and run the consumer group against the far-side broker. The
guarantees compose: publisher → broker (QoS 1 replicated), broker → bridge →
far broker (at-least-once, ack-after-durable, with the one residual ADR 0060
states plainly), far broker → consumer group (this document). Duplicates
remain possible at each hop; the sink's idempotent write at the end absorbs
all of them at once.

## What a rule-engine migrator maps where

For an EMQX/NanoMQ migration, each SQL rule decomposes onto the pattern —
and the [EMQX converter](MIGRATION.md#emqx--mqttd) names every
connector/action it could not carry as a `TODO(migrate)` so no pipeline
vanishes silently:

| Rule-engine construct | In this pattern |
|---|---|
| `FROM "telemetry/#"` (route) | the group's topic filter |
| `WHERE payload.x > 3` / `SELECT` reshaping (transform) | ordinary code in the consumer (`transform()` above) |
| Kafka / webhook / DB action (sink) | the consumer's producer / POST / upsert |
| Rule-engine buffering & retry | the durable session queue + the consumer's ack-after-write |
| Fan-out to several sinks | one group **per sink** — each group gets its own copy of the stream |
| Republish action | the consumer publishes back at QoS 1 (as in the DLQ recipe) |

What you own that the rule engine used to own: the consumer processes
themselves (deploy, restart, monitor — they are stateless; all state is the
broker's session), and the transform as code instead of SQL. What you gain:
the transform is testable, versioned, and its failure mode is a stalled,
alarmed queue instead of a silently-misfiring rule.

## Least privilege and monitoring

Give the sink its own identity with a **subscribe-only** grant on exactly its
filters (deny-by-default ACLs; the same posture the bridge's account gets,
ADR 0025 T8). A compromised sink can then read the streams it already reads —
not publish, and not read anything else.

Watch, on the broker ([ADR 0020](adr/0020-metrics-and-observability.md)):

```promql
rate(mqttd_publish_dropped_total{reason="queue-overflow"}[5m])  # actual pattern data loss
mqttd_sessions                                                  # the group's sessions exist
mqttd_inflight_messages                                         # a stuck consumer pins its window
mqttd_backlog_bytes                                             # memory pressure from backlogs
```

and on the sink side: its own lag/queue metric, and the consumer's connect
state. There is no per-session queue-depth gauge today; the overflow counter
above is the loss signal, and sizing the cap is the prevention.

## What this pattern does not give you

Stated so nobody discovers it in production: no SQL DSL (transforms are
code), no broker-side schema validation, no per-rule metrics out of the box
(instrument the consumer), and the broker will not transform payloads in
flight — a message crosses byte-for-byte, user properties intact
(ADR 0030). If operating even a small forwarder fleet is the cost that
matters most to you, a rule-engine broker is the honest recommendation —
[COMPARISON.md](COMPARISON.md) says exactly that.
