# ADR 0001 — Session durability in a horizontally-scalable cluster

- **Status:** Accepted
- **Date:** 2026-06-02
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0001-session-durability.md](../delivery/0001-session-durability.md) — plan, progress, and changelog
- **Related:** [Capability Plan](../CAPABILITY-PLAN.md) §4 (scalability), `mqtt-storage::SessionStore`

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0001-session-durability.md).

## Context

A **persistent session** (MQTT 3.1.1 `clean_session=false`, or MQTT 5.0 with a
non-zero Session Expiry Interval) requires the broker to retain, across client
disconnects:

1. **Subscriptions** — small, low churn.
2. **Offline message queue** — QoS 1/2 messages that arrived while the client was
   offline. Potentially large, high churn. *This is the state whose loss users
   notice most.*
3. **In-flight outbound state** — PUBLISHes sent but not yet PUBACK/PUBCOMP'd.
4. **In-flight inbound QoS-2 state** — received PUBLISH awaiting PUBREL.
5. **Next packet-id counter** and the QoS-2 received-packet-id set (dedup).

We have committed to a **shared-nothing, linearly-scalable** cluster (Capability
Plan §4): a client may connect to any node, and there is no coordinator on the
publish hot path. This pulls directly against durability, which inherently
requires replicating session state somewhere.

### The core tension

Durability requires replication; linear scalability forbids global coordination.
You cannot have zero-cost durability. The goal is to make replication cost
**bounded and sharded**, not global.

Two observations make this tractable:

- **Shard, don't centralize.** Partition sessions by client-id across the cluster
  so no node holds all of them. Adding nodes adds aggregate session capacity →
  linear.
- **Asymmetric durability.** *Enqueue* must be durable — losing it loses a
  message. *Dequeue/ack* may be lazy: MQTT QoS 1 is explicitly "at least once,"
  so a failover that redelivers a few in-flight messages is **spec-legal**, not
  data loss. This keeps consensus off the per-ack path.

## Decision

### 1. Session ownership by hashing, with a bounded replica set

`owner(client_id)` is selected by **rendezvous (HRW) hashing** over the gossip
(SWIM) membership. Each session has a **replica set of R nodes** (default R=3),
*not* the whole cluster. Sessions are thus sharded across the cluster.

### 2. The offline queue is a replicated log per session

Enqueue = append to the session's log, **quorum-replicated** across its replica
set before the producing client's QoS≥1 PUBLISH is PUBACK'd. That ack is the
durability contract: *a PUBACK means the message is durably enqueued for every
matching persistent session.*

### 3. Acks/dequeue are local-first and lazily truncated

Delivery acknowledgements truncate the log lazily; they do **not** require a
synchronous cross-node round-trip. On failover a replica replays the log from the
last durably-truncated offset, which may redeliver in-flight QoS-1 messages
(legal). QoS-2 exactly-once is preserved because the received-packet-id dedup set
is part of the replicated session state.

```
client (any node)                          session "owner" + replica set
   │  PUBLISH qos1 to topic T                ┌──────── shard for client-id "X" ────────┐
   ▼                                         │  node A (primary)   node B   node C       │
[ingress node] ──interest map──▶ owner(X) ──▶│  append to X's queue-log, quorum-replicate│
   ▲                                         └────────────────────────────────────────────┘
   │  PUBACK to producer ONLY after the enqueue is quorum-durable
```

### 4. Consensus is scoped to ownership + the enqueue log only

A network partition must never let two nodes both believe they own session X
(split-brain → divergent queues). Therefore **session ownership and the enqueue
log go through quorum/consensus**, while message fan-out / routing stays
coordinator-free. This is the "consensus only where truly needed" slice from the
Capability Plan.

### 5. Takeover / handoff

On reconnect the client may land on any node (load balancer). The landing node
consults the ownership ring:
- owner alive → proxy/redirect to it;
- owner dead → a replica is promoted and replays the log to rebuild the queue.

This is the MQTT session-takeover flow; an existing connection for the same
client-id is disconnected first.

### 6. Bounded queues (anti-OOM)

A dead-but-persistent client must not grow a queue without limit:
- per-session queue caps with an overflow policy (drop-oldest / reject-with-reason);
- MQTT 5 **Session Expiry Interval** → garbage-collect sessions;
- MQTT 5 **Message Expiry Interval** → drop stale queued messages;
- **shared subscriptions** (`$share/...`) spread a topic's load across consumers
  so a single session queue is not the only delivery path (also the main lever
  for *consumer* linear scale).

### 7. Storage interface shape

The `mqtt-storage::SessionStore` trait is the seam. It is **incremental and
async** (`enqueue` / `pending` / `ack`) rather than load-whole / save-whole,
because the clustered backend performs network-replicated, per-message writes and
rewriting an entire queue per change does not scale. The single-node in-memory
implementation is built against this same interface so no second refactor is
needed when the clustered backend lands.

**Refinement ([ADR 0006](0006-consensus-and-replication.md)).** The clustered
backend layers in two tiers: a generic `ReplicatedLog` append-log (the
*replication mechanism* — keyed, offset-addressed byte records) with
`SessionStore` *semantics* expressed over it. `mqtt-storage::logged::ReplicatedSessionStore`
already realizes `SessionStore` over that seam against the single-node
`InMemoryReplicatedLog` (queue in a `q/{client}` log, metadata in `m/{client}`),
holding no durable state of its own — so substituting the consensus-backed log
makes sessions durable with no change to the session-semantics layer. The QoS-2
dedup set and next-packet-id counter (§5 above) are not on the `SessionStore`
trait surface today; they join the replicated state with the durable backend.

## Consequences

**Positive**
- No loss of *durably-enqueued* messages on a single-node failure (R=3, quorum=2).
- Split-brain-safe session ownership.
- Cost is bounded by R (not N) and sharded in parallel → throughput scales ~linearly.
- The broker core is unchanged between single-node and clustered backends.

**Negative / accepted trade-offs**
- **R× write amplification** and one cross-node quorum hop on enqueue. There is no
  free durability.
- Possible **duplicate delivery** of in-flight QoS-1 messages after failover
  (spec-legal; clients must tolerate it, which compliant clients already do).
- Gating PUBACK on quorum-durable enqueue adds **publish latency** for QoS≥1 to
  persistent subscribers. (QoS-0 and non-persistent paths are unaffected.)
- Ownership/consensus adds operational complexity (membership, leader election per
  shard).

## Alternatives considered

- **Externalized store (Redis / Kafka / FoundationDB / Cassandra).** Broker nodes
  become near-stateless caches over a replicated store. Operationally simple, but
  moves the scaling/cost bottleneck into the store and adds per-enqueue latency.
  Rejected as the *default* (it contradicts shared-nothing), but remains a valid
  `SessionStore` backend choice for operators who already run such a store.
- **One global Raft group for all sessions.** Strongly consistent and simple to
  reason about, but the single leader is a throughput ceiling — the opposite of
  linear scale. Rejected.
- **No replication (best-effort, queue lives only on the owner).** Maximum
  performance, zero durability: a node crash loses every offline queue it held.
  Rejected for the default; may be offered as an explicit "ephemeral sessions"
  mode for workloads that don't need durability.

## Amendment (2026-08-10): the durable append covers ONLINE subscribers too (issue #124)

§2 states the contract without qualification: *a PUBACK means the message is durably
enqueued for every matching persistent session.* The implementation was narrower than
the ADR. `Hub::deliver_to_client` took the online branch first — send to the connected
subscriber's channel, return `true`, publisher acked — and the unacknowledged message
existed only in `Hub::inflight`, an in-memory map nothing wrote to the store. A crash
between the publisher's PUBACK and the subscriber's lost a message that no operator
could find afterwards, because there was nothing anywhere to find.

An external reviewer inferred this from the *absence* of a counter-test rather than from
reading the bug, which is a stronger signal than the finding itself: the claim was
resting on an unstated precondition, and nothing in the test suite would have noticed if
the precondition were false.

Nothing *could* have. The acked-facts oracle — the harness this ADR's guarantee is
verified by — recorded a `QoS` 1 payload as received the moment the subscriber's socket
saw it, before the PUBACK was written. In this defect the subscriber does see the live
PUBLISH; the broker then dies still owing the redelivery. The payload was already
counted, so every fault schedule passed. Both oracles now record a `QoS` 1 payload only
after its acknowledgement is written (`docs/delivery/0042-…`, `0044-…`). The lesson
generalises past this bug: **a durability oracle must define "delivered" as the client
defines it**, or it measures reachability rather than durability.

**The append now happens before the wire send**, for a `QoS` > 0 delivery to a
persistent subscriber, online or not. The wording of §2 is unchanged because it was
already right; what changed is that it is now true.

The mechanism reuses the log the offline path already writes, rather than adding a
parallel one:

- `deliver_to_client` appends first and carries the returned offset into the live send.
- The offset travels with the message through the in-flight table and the flow-control
  backlog (ADR 0012).
- The log is truncated only through the **contiguous** acknowledged prefix — an
  out-of-order PUBACK for a later message cannot discard an earlier one still in flight
  behind it. `Inflight::advance_ack` is that arithmetic, unit-tested in isolation because
  it is the part that would lose data silently if it were wrong.
- Replay on resume skips offsets the in-flight table still holds, since the DUP resume
  has already re-sent those. After a *broker* restart that table is empty, which is
  exactly when the replay must carry them.

**Durability follows the session, not the connection.** This is the rule that decides who
pays:

| | durable append | why |
|---|---|---|
| `QoS` > 0, persistent session | **yes** | redelivery is owed on resume |
| `QoS` > 0, clean session | no | nothing to resume into |
| `QoS` 0, any session | no | at-most-once owes no redelivery |
| retained replay | no | the value is durable in the retained store; a crash loses the delivery, not the fact |
| under disk/memory brownout (ADR 0041 T5/T11) | no — and the **publisher's ack is refused with it** (v5 `0x97`, v3.1.1 no ack + close; cross-node, the refusal travels as a peer-bus verdict — an older link mid-rolling-upgrade withholds the ack and closes instead), counted as `quota_rejections_total{reason="brownout-publish"}` | the growth write is refused, so nothing is acked-and-lost; re-sending is the application's decision (issue #238) |
| offline queue at its cap (§6, `MQTTD_QUEUE_OVERFLOW`) | **yes — but the cap sheds acked data**: the default `drop-oldest` truncates the oldest *already-acked* entries out of the durable queue; the opt-in `reject-newest` acks and drops the new message | a bounded queue must shed at its bound; the shed is counted (`publish_dropped{reason="queue-overflow"}`) and stated here, where the durability claim is made |

So the §2 latency cost listed under *Consequences* — "gating PUBACK on quorum-durable
enqueue adds publish latency for `QoS` ≥ 1 to persistent subscribers" — now applies on
the online path as well, where it previously did not. That is a real throughput cost and
it is the price of the claim. The escape hatch is the session type, not a flag: a
subscriber that does not need redelivery across a broker restart connects with
`clean_session` (or a zero Session Expiry) and never pays.

**What this does not fix.** The message is durable; the *packet id* it was in flight
under is not, nor is its acknowledgement phase. A subscriber resuming after a broker
crash is redelivered under a fresh id, so a `QoS` 2 message it had already PUBRECed
cannot be matched and is seen twice — exactly-once degrades to at-least-once across that
window. A duplicate is a better failure than a loss, and this is the trade taken
knowingly; issue #130 tracks persisting the outbound in-flight table, which the inbound
direction already has an equivalent of (`record_received` / `clear_received`).
