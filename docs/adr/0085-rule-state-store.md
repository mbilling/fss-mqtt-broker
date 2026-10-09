# ADR 0085 — State for rules: a keyed, replicated store read and written from SQL

- **Status:** Proposed
- **Date:** 2026-10-09
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0085-rule-state-store.md](../delivery/0085-rule-state-store.md) — plan, progress, and changelog
- **Revisits:** [ADR 0083](0083-rule-engine.md) §1 (the `mqtt-rules` crate "holds no
  broker state"): the crate stays free of broker state and I/O, and reaches the store
  only through a trait the broker supplies (D2).
- **Related:** [ADR 0084](0084-watching-and-editing-rules-live.md) (statistics, trace and
  the admin rules endpoints this extends), [ADR 0080](0080-replication-factor.md) and
  [ADR 0078](0078-replica-segment-log.md) (the durable plane `strong` mode rides on),
  [ADR 0041](0041-resource-governance.md) (the memory accounting the store joins),
  [ADR 0082](0082-bounded-hub-ingress.md) (ingress credit), issue
  [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880).

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0085-rule-state-store.md).

## Context

A rule sees one message or event at a time and remembers nothing between them (ADR 0083
follows EMQX here). Most of what people ask a rule to do next needs memory:

- **Duration:** "temperature above 30 for 5 minutes", not on the first hot reading.
- **Change:** "only when the value differs from the last one", "when the state flips".
- **Counting in a window:** "more than 5 failed logins in a minute".
- **Absence:** "no message from this device for 10 minutes".
- **Distinct counts:** "how many distinct devices reported from site A this hour".

None of these can be expressed today. Three ways to add memory were weighed:

1. **Retained messages as state.** A rule republishes `state/<device>/hot_since` with
   `retain = true` and a new function reads it back. Republish already supports `retain`,
   so only the read is missing. But a derived message is routed asynchronously (ADR 0083
   §3), so the next reading from the same device can be evaluated before the previous
   state write lands; under a burst the rule reads stale state and decides wrongly. Every
   state change also becomes a retained write, which is durable on a durable node: a
   second durable write per reading on the hot path. And retained messages cannot express
   absence: nothing fires when a topic goes quiet.
2. **An external Redis.** A network round trip per evaluation on the publish path adds
   latency to every publish the rule sees, and a Redis outage becomes a rule outage.
   Writing to an external store is a different feature (EMQX's data integration); reading
   from one per message is the part to avoid.
3. **A store inside the broker, read and written during evaluation.** Updates happen
   inside the evaluation, so the next evaluation of the same key sees them; there is no
   network on the default path; and expiry can raise an event, which gives absence.

Replication is required, not optional: a device's state must survive its node failing,
and some keys (counters that gate actions, quotas) must be exact across the cluster while
others (a last-seen time) only need to converge. Operators therefore choose the guarantee
per use, not once for the broker.

## Decision

### D1. A keyed state store inside the broker, reached only from rules

The broker keeps a keyed store of small values, organised in **namespaces**. Rules read
and write it from SQL (D3); clients cannot reach it, and nothing else writes it. Each
namespace is declared in the rules file with its consistency mode (D5) and bounds (D9):

```toml
[state.alerts]                 # a namespace; by default each rule also has its own
consistency = "eventual"       # local | eventual | strong
max_keys = 100000
default_ttl = "1h"
```

A rule that names no namespace uses its own, keyed by its id, with the file's
`[state.default]` settings (`local` unless set). Two rules share state only by naming the
same declared namespace, so one rule cannot clobber another's keys by accident.

### D2. The rule engine stays pure: state through a trait

`mqtt-rules` keeps ADR 0083 §1's property of holding no broker state and doing no I/O.
Evaluation reaches the store through a `StateStore` trait carried by `EvalCtx`, which the
broker implements; `mqttd --rule-test` implements it in memory (D11). Every state function
fails cleanly (the rule's `failed` result, counted and traced) when no store is wired, so a
rules file that uses state still loads and tests anywhere.

### D3. SQL functions, modelled on Redis

| Function | Like | Returns |
|---|---|---|
| `kv_get(key)` | `GET` | the value, or `undefined` |
| `kv_set(key, value[, ttl])` | `SET … EX` | the value written |
| `kv_set_once(key, value[, ttl])` | `SET … NX` | the value now held: the new one, or the earlier one |
| `kv_incr(key[, by[, ttl[, idempotency_key]]])` | `INCRBY` | the count after |
| `kv_del(key)` | `DEL` | whether a value was there |
| `kv_hll_add(key, value[, ttl])` | `PFADD` | the distinct-count estimate after |
| `kv_hll_count(key)` | `PFCOUNT` | the estimate |
| `kv_ttl(key)` | `TTL` | seconds left, or `undefined` |
| `kv_held(key, condition[, ttl])` | — | how many seconds `condition` has held without a break: it records when it first became true and clears the key when it is false, returning `undefined` then |
| `kv_changed(key, value[, ttl])` | — | whether `value` differs from the one stored, storing it: `true` the first time |

A key is a string up to 256 bytes, built from the message (`'hot:' + clientid`) and scoped
to the rule's namespace, or to another with `ns:key` syntax for a declared shared one.
TTLs are seconds or a duration string, capped by the namespace. These functions are an
mqttd extension and are listed among the departures from EMQX.

**When a state call runs.** A rule decides its `WHERE` first, on only the `SELECT` fields
the `WHERE` reads (#882, which amends ADR 0083 §1). So a state call in a `SELECT` field
the `WHERE` does not read runs only for messages that pass, as a reader expects; one in a
field the `WHERE` reads (directly or through an alias) runs for every message the `FROM`
selects, because the `WHERE` needs its value. `kv_held` and `kv_changed` are built for that
second case: they encode the two most common patterns and their own reset, so they are
right to run on every message. `--rule-test --sequence` (D11) shows the store after each
step, so either case is visible before a rule runs.

"Above 30 for 5 minutes":

```sql
SELECT clientid, payload.temp AS temp,
       kv_held('hot:' + clientid, payload.temp > 30, 3600) AS hot_for
FROM "sensors/+/data"
WHERE hot_for >= 300
```

"Only when the state flips":

```sql
SELECT clientid, payload.state AS state
FROM "machines/+/status"
WHERE kv_changed('state:' + clientid, payload.state)
```

### D4. Each value has a merge type, and every merge is a CRDT

Replicas of a key must converge whatever order updates arrive in, without asking anyone to
resolve a conflict mid-evaluation. So a key's operations define how concurrent updates
merge; each is a CRDT:

| Merge | Written by | Concurrent updates resolve to |
|---|---|---|
| `lww` | `kv_set`, `kv_changed` | the later write by hybrid logical clock (D6); ties by node id |
| `first` | `kv_set_once`, `kv_held` | the earlier write by hybrid logical clock |
| `max`, `min` | `kv_set` on a namespace or key declared `max`/`min` | the larger or smaller value |
| `sum` | `kv_incr` | the sum of each node's own increments (a per-node count vector) |
| `union` | `kv_add` (a bounded set) | the union of the elements |
| `hll` | `kv_hll_add` | the register-wise maximum of two HyperLogLog sketches |
| delete | `kv_del` | a tombstone, kept for the namespace's tombstone window, so a late replica cannot undo it |

Full vector clocks with sibling values (a multi-value register) are not offered: they
detect concurrency but leave it unresolved, and a rule has no one to resolve it. The hybrid
logical clock keeps causally ordered writes in order, so `lww` drops only one of two
genuinely concurrent writes, which is the right outcome for a "latest value". Where losing
either is wrong, the key uses a merge that keeps both (`sum`, `union`, `max`, `first`).
`hll` uses 2^14 registers (about 0.8% standard error, 12 KiB dense, sparse below that).

### D5. Consistency is chosen per namespace

| Mode | Write | Read | During a partition | Cost on the publish path |
|---|---|---|---|---|
| `local` | this node | local | unaffected; no replicas | in-memory map |
| `eventual` | applied locally, shipped to the key's replicas in the background | local, possibly briefly stale | both sides work; values converge on heal | in-memory map; replication off the path |
| `strong` | through the key's leader, acknowledged by a quorum before the rule continues | from the leader | the minority side's writes fail (counted, traced) | one replication round trip per write |

- **`eventual`** ships each update's CRDT delta to the key's replica set over the peer bus,
  with periodic anti-entropy between replicas after a partition or restart. Replicas are
  chosen by key hash over the current members, with the cluster's replication factor (ADR
  0080).
- **`strong`** places each key in a placement group of the durable plane and writes
  through its leader's replicated log (ADR 0078/0080): write-through by construction.
  Evaluation of a rule that touches a `strong` namespace waits at each state call, so the
  publishing connection's next message waits too, as a durable QoS 1 publish does; this is
  documented as the cost of exactness and is for low-rate keys. A rule runs on the hub only
  for a Will (ADR 0083 §2), which must not wait: there, a `strong` write is submitted
  without waiting and a read is the local replica's, and the trace says so.
- **Persistence:** `strong` state is durable through the replicated log. `local` and
  `eventual` state is in memory, rebuilt for `eventual` from replicas by anti-entropy
  after a restart; an optional snapshot to disk is a later phase (Phasing).

### D6. Time: one hybrid logical clock per node

Every write is stamped with the node's hybrid logical clock (physical milliseconds plus a
logical counter, merged on receipt), which orders `lww` and `first` and survives modest
clock skew. `now_timestamp()` keeps its EMQX meaning (the wall clock), read once per
evaluation so a rule sees one instant. Durations computed across nodes are only as good as
the nodes' clock agreement; the documentation says so and the rig's clock gate is the
reference practice.

### D7. Expiry raises an event, once

A key whose TTL passes is removed and raises `$events/state/expired` with the namespace,
key and last value, which rules can select like any event. Refreshing a key with a TTL on
every message therefore detects silence: "no message for 10 minutes" is
`kv_set('seen:' + clientid, now_timestamp(), 600)` in one rule and `FROM
"$events/state/expired"` in another. Exactly one node fires the event: the key's leader in
`strong` mode, otherwise the first live member of the key's replica set. Around a
membership change it can fire twice, so the event is **at least once**, and stated so.

### D8. Idempotency for counters

A counter cannot tell a redelivered message from a new one: QoS 1 redelivery, a takeover
replay or a client retry would count twice. `kv_incr` takes an optional idempotency key
(typically the message's id or a payload field); the namespace remembers seen keys for its
`dedupe_window` and ignores a repeat. Without one, the documentation states that counts are
at least once.

### D9. Bounds, accounting and eviction

Keys can contain client-chosen values (`clientid`, topic levels), so every dimension is
bounded and the totals join the memory watermark (ADR 0041):

- per file: at most 64 namespaces; per namespace: `max_keys` (default 100 000, capped
  1 000 000), values up to 4 KiB, `union` sets up to 1 024 elements, a TTL cap;
- when a namespace is full, a write of a new key fails (counted, traced) by default;
  `eviction = "lru"` evicts the least recently used key instead, for caches;
- the store's bytes count toward the memory watermark; in a brownout new keys are refused
  and existing keys still update.

### D10. Observability and admin

- **Metrics:** `mqttd_rule_state_keys{namespace}`, `mqttd_rule_state_bytes{namespace}`,
  `mqttd_rule_state_ops_total{namespace,op,result}`,
  `mqttd_rule_state_replication_lag_seconds{namespace}` and
  `mqttd_rule_state_expired_total{namespace}`.
- **`$SYS`:** per-namespace key counts and rates beside the rule statistics; never keys or
  values, which can carry payload data.
- **Admin API and CLI (operator):** `mqttd --admin state get|list|del <ns> [key]`, audited
  like every admin request; values are shown to operators only, as the rules' SQL is.

### D11. Testing: sequences, not single messages

`mqttd --rule-test` gains `--sequence <file>`: a timed list of messages and events
replayed through one rule (or the whole file) against an in-memory store, printing each
step's outputs and the store's state. This is how a rule with state is written and
reviewed, and it is the loop the rule-authoring skill drives: the user's examples become a
sequence with expected outputs, and a draft passes only when every step matches.

### D12. Not a system of record

The store makes live decisions; it is not a ledger. Billing is the worked example in the
documentation: use a `strong` namespace with idempotency keys for quota and tier
decisions, and publish each billable event as a durable QoS 1 message that the billing
system consumes and deduplicates by message id. `eventual` counters can be read before
every node's increments arrive, so a threshold can fire late or twice; a node lost before
it replicates loses its `eventual` increments.

## Phasing

1. **`local` and `eventual`, every merge type, expiry events, bounds, metrics, the admin
   read verbs and `--sequence` testing.** The stateful patterns ("above Y for X", change,
   counting windows, silence, distinct counts) all work in this phase.
2. **`strong`,** on the durable plane, with partition and failover tests in the style of the
   durable plane's own before it is called supported.
3. **An optional snapshot to disk** for `local` and `eventual` namespaces.

## Consequences

- Rules can express duration, change, counting, absence and distinct counts, which
  removes the most common reason to put a separate stream processor beside the broker.
- A rule that uses state costs an in-memory map operation per state call in `local` and
  `eventual` mode; replication traffic grows with the rate of state writes, not of
  messages.
- `strong` mode puts a replication round trip on the publish path of the connections whose
  messages reach those rules. It is opt-in per namespace and documented as such.
- The store is new memory the broker holds on behalf of client-shaped keys; D9 bounds it,
  and the threat model gains rows for state exhaustion and for values derived from payloads.
- `$events/state/expired` is at least once, and `eventual` reads can be briefly stale;
  both are stated where the functions are documented.
- These functions are not EMQX's: rules that use them do not port back to EMQX. The
  departures table lists them.

## Alternatives considered

- **Retained topics as state.** Rejected as the mechanism (stale reads under bursts, a
  durable write per state change, no absence), kept as an optional mirror: a namespace may
  later publish its changes as retained messages for visibility.
- **An external Redis, read per evaluation.** Rejected: latency on every publish a rule
  sees and a new failure dependency. Writing state out to external systems is a separate
  feature.
- **Vector clocks with sibling values.** Rejected for the first phases: they surface
  conflicts a rule cannot resolve. Typed merges (D4) resolve them deterministically.
- **One global consistency setting.** Rejected: a last-seen time and a quota counter need
  different guarantees in the same cluster.
- **`strong` only.** Rejected: a quorum round trip per state write is the wrong default for
  per-device telemetry state, which is most of the demand.
