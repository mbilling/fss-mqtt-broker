# Client guide

**Verified against `v1.0.16` (2026-09-11).** The application-developer contract:
what a client can rely on when it talks to mqttd. Protocol semantics are MQTT
3.1.1 / 5.0; this page states *this broker's* session, expiry, flow-control and
refusal behaviour, and the reason codes it actually emits.

The emitted-code table is held to `scripts/check-reason-codes.py`. A code the
broker can place on the wire that is missing here is a CI failure (ADR 0070 T2).

## Connect and sessions

MQTT 3.1.1 has `clean_session`. MQTT 5.0 splits that into **Clean Start** (resume
or start fresh) and **Session Expiry Interval** (how long the broker keeps the
session after disconnect). mqttd normalizes both versions at the connection edge
([ADR 0009](adr/0009-mqtt5-expiry.md)):

| Client says | What the broker keeps after disconnect |
|---|---|
| v3.1.1 `clean_session=1` | Nothing. Equivalent to Clean Start + expiry `0`. |
| v3.1.1 `clean_session=0` | Subscriptions and the offline queue, until the session expires or is replaced. Equivalent to Clean Start `0` + expiry never (`0xFFFFFFFF`). |
| v5 Clean Start `1`, expiry `0` | Nothing. |
| v5 Clean Start `0`, expiry `N` | The previous session, then drop it `N` seconds after this disconnect. `0xFFFFFFFF` = never. |

Persistent sessions are **durable by default** (quorum-replicated,
[ADR 0029](adr/0029-durable-by-default.md)). An acked QoS 1/2 message survives the
loss of the node that accepted it. A clean session is in-memory and cheaper.

**Session takeover:** a second CONNECT with the same client id disconnects the
first and publishes the will. After a placement roll, a v5 session on a node
that no longer owns it is sent `0x9C Use another server` so it reconnects onto
the owner (issue #284) — the code comments record `0x8E Session taken over` as
the alternative that was *not* chosen for that path.

**Last Will:** registered at CONNECT, published on any ungraceful end — including
takeover and rehome closes. A clean DISCONNECT discards it ([MQTT-3.14.4-3]).

## Message expiry

A v5 PUBLISH may carry Message Expiry Interval. Queued messages store an
**absolute** deadline; on delivery the remaining seconds are forwarded
([ADR 0009](adr/0009-mqtt5-expiry.md) §3). Expired entries are dropped, not
delivered. Cross-node shared delivery carries the deadline on `SharedDeliver`
(0015-T7).

A residual to know: if a session is rehomed from a node that does **not** hold
the group's lease, the expiry deadline may fail to persist
(`mqttd_session_expiry_unpersisted_total{reason="not-owner"}`). It self-heals
when the client reconnects (CONNECT carries the interval). Stated in ADR 0009's
as-delivered note.

## Flow control

v5 **Receive Maximum** is honoured in both directions ([ADR 0012](adr/0012-flow-control.md),
as delivered):

- **Server → client:** the hub will not put more unacked QoS > 0 publishes on
  the wire than the client's Receive Maximum (or `MQTTD_MAX_INFLIGHT_MESSAGES`
  if set lower). Surplus waits in a bounded in-memory backlog.
- **Client → server:** CONNACK advertises `MQTTD_RECEIVE_MAXIMUM` (default 256).
  A client that exceeds it is disconnected with **`0x93`**.

QoS 0 is not flow-controlled by Receive Maximum. A slow subscriber's QoS 0 is
shed from the outbound socket channel (`mqttd_publish_dropped_total{reason="outbound-full"}`)
when `MQTTD_MAX_OUTBOUND_BYTES` / the 10 000-packet cap fires.

The online backlog is **drop-oldest** at `MQTTD_MAX_BACKLOG_MESSAGES` (default
10 000) and optional `MQTTD_MAX_BACKLOG_BYTES`. Shedding already-acked messages
does not tell the publisher (`reason="backlog-overflow"`). There is no unbounded
setting: `0` is refused at config validation.

Inbound publish *rate* (`MQTTD_MAX_PUBLISH_RATE`) pauses the socket read — TCP
backpressure — it does not drop or disconnect.

## Refusal behaviour

A refused operation is an **answered refusal**, not a silent drop (except where
MQTT 3.1.1 has no packet to carry a reason):

| Situation | MQTT 5 | MQTT 3.1.1 |
|---|---|---|
| Bad credentials / not authorized | CONNACK/PUBACK/DISCONNECT `0x87` | CONNACK return code 4/5; unauthorized publish is dropped (check the audit log) |
| Quota / brownout / session cap | `0x97` | No ack + close for QoS ≥ 1 publishes; CONNACK server-unavailable for a new session |
| Receive Maximum exceeded | DISCONNECT `0x93` | v3.1.1 has no Receive Maximum; inbound is not disconnected on this path |
| Packet too large | `0x95` | Connection close |
| Shared filter malformed | SUBACK `0x80` | SUBACK `0x80` |
| Subscription identifiers | CONNACK advertises `Subscription Identifier Available = 1` (`0x29`); identifiers are delivered (issue #266) | n/a |

Brownout (disk or memory watermark) refuses *growth* writes: QoS ≥ 1 publishes
that need a durable append, new sessions, new retained topics. Subscriber acks,
reads, deletes and expiry continue. Re-sending is the application's decision; a
v5 reason ≥ `0x80` completes the packet-id lifecycle.

Over-cap connections (`MQTTD_MAX_CONNECTIONS` / `_PER_IP`) are closed **at
accept**, before TLS — no CONNACK.

## Shared subscriptions

`$share/<group>/<filter>` — each matching publish is delivered to **exactly one**
member cluster-wide ([ADR 0010](adr/0010-shared-subscriptions.md),
[ADR 0015](adr/0015-cluster-shared-subscriptions.md)). Retained messages are
**not** replayed onto a new shared subscription.

**Selection, as shipped:** with `MQTTD_SHARED_PREFER_LOCAL` (default **on** since
#511) a group that has any online member on the publishing node is answered by
that member; the cursor rotates among online locals only. Remote members are
reached through the `remote_by_filter` index when this node hosts none.
Consequence for a worker pool: a consumer's share follows its *host node's*
share of publishers, not even round-robin across the group. MQTT leaves
selection implementation-defined; fairness/spillover is a separate option
(#537 Phase 3), not a spec defect.

**QoS 0 exception under pressure (#482):** a selected local member whose outbound
cannot fit the message is bypassed before enqueue if another online member of the
same group can be selected. Locals with room remain preferred; remote capacity is
unknown. No alternative means a counted QoS 0 drop, not unlimited buffering or a
retry after an ambiguous send. This does not yet make the group an end-to-end
capacity-aware bridge pool; see [the measured scope](benchmarks/SHARED-CAPACITY.md).

## Emitted reason codes (failure, `>= 0x80`)

This table is the CI-gated catalogue. Success codes (`< 0x80`) and codes the
broker never sends are omitted on purpose — the gate compares tests against
production emissions, not the full MQTT 5 catalogue in `mqtt-codec`.

<!-- reason-codes:begin -->

| Code | Name | When mqttd emits it | Tests |
|------|------|---------------------|-------|
| `0x80` | `UNSPECIFIED_ERROR` | SUBACK failure slot, or a codec value the broker maps as unspecified error. | provoked in integration tests |
| `0x81` | `MALFORMED_PACKET` | Malformed packet at decode (CONNACK/DISCONNECT depending on when it is caught). | provoked in integration tests |
| `0x82` | `PROTOCOL_ERROR` | Protocol violation (illegal packet for the current state). | provoked in integration tests |
| `0x84` | `UNSUPPORTED_PROTOCOL_VERSION` | Mapped in `conn.rs::codec_reason` for totality; unreachable on the wire (CONNECT with an unsupported protocol level closes silently per [MQTT-3.14.0-1]). | exempt (see `scripts/check-reason-codes.py`) |
| `0x87` | `NOT_AUTHORIZED` | Authentication or ACL denial (CONNACK, PUBACK/PUBREC, DISCONNECT on revocation sweep). | provoked in integration tests |
| `0x8b` | `SERVER_SHUTTING_DOWN` | Graceful drain of live v5 sessions (ADR 0019 / `SIGTERM`). | exempt (see `scripts/check-reason-codes.py`) |
| `0x8c` | `BAD_AUTHENTICATION_METHOD` | Enhanced-authentication method the broker does not accept. | provoked in integration tests |
| `0x8f` | `TOPIC_FILTER_INVALID` | SUBSCRIBE/UNSUBSCRIBE filter the broker rejects (including a malformed `$share/...`). | provoked in integration tests |
| `0x93` | `RECEIVE_MAXIMUM_EXCEEDED` | Client exceeded the server's advertised Receive Maximum (inbound QoS > 0 in flight). | provoked in integration tests |
| `0x94` | `TOPIC_ALIAS_INVALID` | Topic alias out of range or used before it was bound (ADR 0011). | provoked in integration tests |
| `0x95` | `PACKET_TOO_LARGE` | Inbound packet larger than the advertised Maximum Packet Size. | provoked in integration tests |
| `0x97` | `QUOTA_EXCEEDED` | A quota or brownout refusal (sessions, subscriptions, retained growth, durable-append floor). | provoked in integration tests |
| `0x9c` | `USE_ANOTHER_SERVER` | This node no longer owns the persistent session; reconnect and land on the owner (issue #284). | exempt (see `scripts/check-reason-codes.py`) |

<!-- reason-codes:end -->

v3.1.1 CONNACK *return codes* (`0x00–0x05`) are a different space and are not
in this table.

## Worked examples

These are the smallest clients that exercise the contract above. They assume the
README two-minute broker (plaintext, anonymous, data dir set). Production uses
TLS 1.3; see [SECURED-CLUSTER-TUTORIAL.md](SECURED-CLUSTER-TUTORIAL.md).

### mosquitto_pub / mosquitto_sub (what CI already runs)

```sh
# Persistent session: queue while the subscriber is down, replay on reconnect.
mosquitto_sub -h 127.0.0.1 -p 1883 -i worker-1 -c -q 1 -t 'orders/#' &
# ... kill the subscriber, publish, restart it with the same -i -c ...
mosquitto_pub -h 127.0.0.1 -p 1883 -t orders/eu -q 1 -m 'ack-me'

# Shared group: each message to exactly one member.
mosquitto_sub -h 127.0.0.1 -p 1883 -t '$share/workers/orders/#' -q 1
```

### Python (paho-mqtt) — persistent session + v5 expiry

Paho is the in-repo foreign-client oracle (`crates/mqttd/tests/` interop). A
minimal v5 persistent subscriber:

```python
import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

props = Properties(PacketTypes.CONNECT)
props.SessionExpiryInterval = 3600

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="worker-1",
                protocol=mqtt.MQTTv5)
c.connect("127.0.0.1", 1883, clean_start=False, properties=props)
c.subscribe("orders/#", qos=1)
c.loop_forever()
```

A publisher that sets message expiry:

```python
pub_props = Properties(PacketTypes.PUBLISH)
pub_props.MessageExpiryInterval = 30
c.publish("orders/eu", b"ack-me", qos=1, properties=pub_props)
```

Handle a refusal: paho surfaces a v5 reason on `on_connect` / `on_disconnect`.
`0x87` is not authorized; `0x97` is quota/brownout — retry with backoff after
the operator clears pressure; `0x9C` means reconnect (possibly to another
bootstrap address).

### Any v5 client — Receive Maximum

CONNACK carries the server's Receive Maximum (`MQTTD_RECEIVE_MAXIMUM`, default
256). A client that pipelines more unacked QoS > 0 publishes than that value
is disconnected with `0x93`. Pace on the CONNACK property, not on an unbounded
pipeline. The same rule is what `crates/mqttd/tests/quotas.rs` provokes.

## See also

- Config knobs: [CONFIGURATION.md](CONFIGURATION.md)
- Operator refusals and watermarks: [OPERATIONS.md](OPERATIONS.md), [SIZING.md](SIZING.md)
- External sinks as a `$share` group: [INTEGRATION.md](INTEGRATION.md)
