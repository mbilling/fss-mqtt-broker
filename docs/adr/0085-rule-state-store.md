# ADR 0085 — State for rules: a keyed, replicated store read and written from SQL

- **Status:** Accepted
- **Date:** 2026-10-09 (reviewed against the code and accepted: 2026-10-11)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0085-rule-state-store.md](../delivery/0085-rule-state-store.md) — plan, progress, and changelog
- **Revisits:** [ADR 0083](0083-rule-engine.md) §1 (the `mqtt-rules` crate "holds no
  broker state"): the crate stays free of broker state and I/O, and reaches the store
  only through a trait the broker supplies (D2). Its 2026-10-10 amendment "functions and
  literals are EMQX's too" left `kv_store_*` and `proc_dict_*` refused "until ADR 0085
  decides their mapping": D13 decides it.
- **Related:** [ADR 0084](0084-watching-and-editing-rules-live.md) (statistics, trace, dry
  runs and the admin rules endpoints this extends),
  [ADR 0086](0086-wasm-rule-functions.md) (the evaluation model a state call has to fit:
  the `Functions` load-time API, the per-message budget, the hand-off for calls that
  wait), [ADR 0082](0082-bounded-hub-ingress.md) (peer data is shed, never paused; what a
  message can make the broker hold is bounded), [ADR 0041](0041-resource-governance.md)
  (the memory watermark), [ADR 0038](0038-prerelease-compatibility-freeze.md) and
  [ADR 0039](0039-versioning-and-upgrade-policy.md) (the peer protocol is extended
  additively), [ADR 0080](0080-replication-factor.md) and
  [ADR 0078](0078-replica-segment-log.md) (the durable plane `strong` mode would ride on),
  issue [#880](https://github.com/mbilling/fss-mqtt-broker/issues/880).

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0085-rule-state-store.md).
>
> **Accepted on 2026-10-11**, after a review against what had landed since it was written:
> the EMQX evaluation order (ADR 0083, amended 2026-10-10), republish re-entry, the message
> events, peer protocol 13 and ADR 0086. What the review changed, and why, is in
> [Amendments](#amendments). Five choices in it are the maintainer's and were **decided by
> default** so that the record is not left open; each is marked where it applies and they
> are listed together in [Decided by default](#decided-by-default-2026-10-11), with what
> the other answer would cost. Two of them were **confirmed by the user** the same day
> (state is on without a switch; the names stay `kv_*`), and the operator's tools on the
> store (view, purge, change expiry) were added: see the amendment "admin features and
> confirmed defaults".

## Context

A rule sees one message or event at a time and remembers nothing between them. Most of
what people ask a rule to do next needs memory:

- **Duration:** "temperature above 30 for 5 minutes", not on the first hot reading.
- **Change:** "only when the value differs from the last one", "when the state flips".
- **Counting in a window:** "more than 5 failed logins in a minute".
- **Absence:** "no message from this device for 10 minutes".
- **Distinct counts:** "how many distinct devices reported from site A this hour".

EMQX has two small answers of its own, both exported by `emqx_rule_funcs` and neither in
its function reference: `kv_store_put/get/del`, one unbounded in-memory table per node,
and `proc_dict_put/get/del`, the dictionary of whichever process is evaluating. Neither is
replicated, bounded or expiring, and neither can express absence. mqttd refuses both at
load today (ADR 0083).

Three ways to add memory were weighed:

1. **Retained messages as state.** A rule republishes `state/<device>/hot_since` with
   `retain = true` and a new function reads it back. A republished message now re-enters
   the rules on the same task (ADR 0083, amended 2026-10-10), but it is *stored* by the
   hub, after the publish's batch is dispatched, so the next reading from the same device
   can be evaluated before the previous state write lands; under a burst the rule reads
   stale state and decides wrongly. Every state change also becomes a retained write,
   which is durable on a durable node: a second durable write per reading on the hot
   path. And retained messages cannot express absence: nothing fires when a topic goes
   quiet.
2. **An external Redis.** A network round trip per evaluation on the publish path adds
   latency to every publish the rule sees, and a Redis outage becomes a rule outage.
   Writing to an external store is a different feature (EMQX's data integration); reading
   from one per message is the part to avoid.
3. **A store inside the broker, read and written during evaluation.** Updates happen
   inside the evaluation, so the next evaluation of the same key sees them; there is no
   network on the default path; and expiry can raise an event, which gives absence.

A device's state should survive its node failing and follow the device to another node.
Some keys (counters that gate actions, quotas) should be exact across the cluster while
others (a last-seen time) only need to converge. Operators therefore choose the guarantee
per namespace, not once for the broker.

## Decision

### D1. A keyed state store inside the broker, reached only from rules

The broker keeps a keyed store of small values, organised in **namespaces**. Rules read
and write it from SQL (D3, D13); clients cannot reach it, and nothing else writes it but
an operator's delete (D10) and, since the amendment of 2026-10-11, an operator's purge
and expiry change. A namespace is declared in the rules file with its
consistency mode (D5) and bounds (D9), and a rule names the one it uses in its own table:

```toml
[state.alerts]                 # a letter, then up to 63 lower-case letters, digits or `_`
consistency = "eventual"       # local (the default) | eventual; strong is phase 2 (D5)
max_keys = 100000
max_bytes = "64MiB"
default_ttl = "1h"
eviction = "fail"              # or "lru" (local namespaces only)

[rules.hot_sensor]
state = "alerts"               # optional; without it the rule has a namespace of its own
sql = '''...'''
```

- **A rule uses exactly one namespace.** Without `state`, it is the rule's own: keyed by
  the rule's id, with the file's `[state.default]` settings (`local` unless set). Two
  rules share state only by naming the same declared namespace, so one rule cannot
  clobber another's keys by accident.
- **The namespace is never read out of a key.** A key is the whole string the rule
  builds, and keys are built from client ids, topic levels and payload fields: a key that
  could name its namespace would let a publisher choose it. (The first draft's `ns:key`
  syntax did exactly that, and collided with its own example key `'hot:' + clientid`.)
- **Reserved names:** `default` (the settings a rule's own namespace gets) and `kv_store`
  (EMQX's table, D13).
- **`[state.*]` and `state =` are mqttd's own**, listed under "Differences from EMQX". The
  rules digest covers them. Unknown keys are load errors, as everywhere in the file, so a
  rules file that declares state does not load on a release without this feature.
- **Using it is the switch.** A rules file that declares no namespace and calls no state
  function creates no store: no memory, no task, no frame on a peer link. The store's
  total size has a ceiling in the broker's configuration, `rules.state_max_bytes`
  (`MQTTD_RULES_STATE_MAX_BYTES`, default 256 MiB, read at startup), which a rules file
  cannot raise; `0` switches state off, and a rules file that uses it is then refused at
  load, naming the key. *(Decided by default, 2026-10-11: on, with that ceiling.
  Confirmed by the user, 2026-10-11.)*
- **Across a reload** (ADR 0084): a namespace whose declaration did not change keeps its
  keys; changed bounds apply at once (lowering `max_keys` evicts nothing, it only refuses
  new keys until the count is below it); a changed `consistency` is refused, with the
  advice to declare a new namespace; a namespace whose declaration is gone, and the
  namespace of a rule that is gone or renamed, is dropped with its keys once the new set
  is running. A rule edited under the same id keeps its keys.

### D2. The rule engine stays pure: state through a trait

`mqtt-rules` keeps ADR 0083 §1's property of holding no broker state and doing no I/O. It
defines a `StateStore` trait; the broker implements it, and hands it over where ADR 0086
already hands over `jq` and the modules: in `Functions` (`RuleSet::parse_with`,
`load_with`). A state call is resolved to its namespace when the file is loaded, as a
`jq` call is resolved to its program, so an evaluation does no lookup by name.
`mqttd --rule-test`, `mqttd --check-rules` and the admin API's dry runs supply an
in-memory implementation (D11). Loaded with no store (`RuleSet::parse`, a build that
left it out, `rules.state_max_bytes = 0`), a file that calls a state function is refused
at load with the reason, as a file that calls `jq` is where `jq` is off.

### D3. SQL functions, modelled on Redis

| Function | Like | Returns |
|---|---|---|
| `kv_get(key)` | `GET` | the value, or `undefined`. For a counter the count, for a set its elements as an array (in term order), for a sketch the estimate |
| `kv_set(key, value[, ttl])` | `SET … EX` | the value written |
| `kv_set_once(key, value[, ttl])` | `SET … NX` | the value now held: the new one, or the earlier one |
| `kv_incr(key[, by[, ttl[, idempotency_key]]])` | `INCRBY` | the count after. `by` is an integer, default 1, and may be negative |
| `kv_max(key, value[, ttl])`, `kv_min(key, value[, ttl])` | — | the larger (smaller) of `value` and the number held, which is stored |
| `kv_add(key, element[, ttl])` | `SADD` | the number of elements after |
| `kv_del(key)` | `DEL` | whether a value was there, on this node |
| `kv_hll_add(key, value[, ttl])` | `PFADD` | the distinct-count estimate after |
| `kv_hll_count(key)` | `PFCOUNT` | the estimate, or 0 |
| `kv_ttl(key)` | `TTL` | whole seconds left, or `undefined` for no key or no expiry |
| `kv_held(key, condition[, ttl])` | — | how many whole seconds `condition` has been `true` without a break. It records when it first became true; on anything but `true` it deletes the key and returns `undefined` |
| `kv_changed(key, value[, ttl])` | — | whether `value` differs (exactly, as `=:=`) from the one stored, storing it: `true` the first time |

- **A key** is a string of 1 to 256 bytes; anything else fails the rule. It is scoped to
  the rule's namespace (D1).
- **A value** is any rule value but `undefined` (storing `undefined` fails the rule; use
  `kv_del`), at most 4 KiB as the rule engine writes it (ADR 0086 D3's form: a binary as
  its bytes, anything else as JSON text, so an integer of any size stays an integer and a
  float a float). That form is what replication carries.
- **A TTL** is a positive integer of seconds or a duration string (`'10m'`), at least one
  second and at most the namespace's `max_ttl`; anything else fails the rule. A write
  without one gives a new key the namespace's `default_ttl` and leaves an existing key's
  expiry as it is; a write with one sets it.
- **A key has the type of the operation that created it** (D4) until it is deleted or
  expires. Another type's operation on it fails the rule (`wrong_type`, as Redis answers
  `WRONGTYPE`); `kv_get`, `kv_ttl` and `kv_del` take any.
- **A failed call fails the rule**, counted and traced like any evaluation error (ADR
  0083 §8): a full namespace, a wrong type, a key or value past its bound, the
  per-message budget (D9). It is never "no result": a `WHERE` must not pass or fail
  silently because the store refused.
- **These names are mqttd's.** EMQX has no function called `kv_get` or any other in the
  table, and the functions it does have keep their own names and their exact behaviour
  (D13). A module (ADR 0086 D9) can no longer take one of these names: they are built in.
  *(Decided by default, 2026-10-11: the names stay `kv_*`, beside EMQX's `kv_store_*`.
  Confirmed by the user, 2026-10-11.)*

**When a state call runs: when evaluation reaches it, in EMQX's order, and it is never
undone.** mqttd evaluates a statement exactly as `emqx_rule_runtime` does (ADR 0083,
amended 2026-10-10): every `SELECT` field left to right, then the `WHERE` over the input
with the selection merged over it; in a `FOREACH`, the collection expressions, the
`WHERE`, then per element the `INCASE` and the `DO`. A state call is an effect of the
expression it stands in, applied to the store at that moment, exactly as EMQX's own
`kv_store_put` is an `ets:insert` at that moment (D13). It follows that:

- **A call in a `SELECT` field runs for every message the `FROM` selects, whether or not
  the `WHERE` then passes.** `SELECT kv_incr('n') AS n … WHERE payload.ok = true` counts
  every message, not the ones that pass.
- **A call in the `WHERE` runs after every `SELECT` field, and only if evaluation reaches
  it**: `AND` and `OR` stop at the first operand that decides (EMQX's `andalso` and
  `orelse`), and a `CASE` evaluates only the branch it takes. To write only for messages
  that match, put the condition first: `WHERE payload.temp > 30 AND kv_incr('hot') > 5`,
  or `CASE WHEN payload.temp > 30 THEN kv_incr('hot') END AS hot`.
- **In a `FOREACH`**, a call in the collection expressions runs once per message before
  the `WHERE`; one in the `INCASE` once per element (up to 10,000); one in the `DO` once
  per element that passed.
- **Nothing is rolled back.** A write stays when a later field fails the rule, when the
  `WHERE` is false, when an action fails, and when the hub then refuses the publish. A
  QoS 1 publish that is sent again is evaluated again and writes again; D8 is the answer
  for counters. (An inbound QoS 2 publish is deduplicated before evaluation, and a
  publish the ACL refuses is never evaluated.)
- **A later call sees an earlier one**, in the same statement, in the next rule the
  message matches, and in the rules a republished message re-enters (which run on the
  same task, in order). A rule runs again on its own republished output (EMQX's guard
  stops only its `republish` actions), so a rule that counts and republishes into its
  own `FROM` counts twice.

`kv_held` and `kv_changed` exist for this order: they are meant to run on every message
the `FROM` selects, they carry their own reset, and both of the examples below would be
wrong under any rule that skipped the write for a message the `WHERE` rejects. That is
also why writes are not held back until the rule matches: `kv_held` would never record
the first hot reading (see Alternatives).

The cost-saving candidate ADR 0083 left open (evaluating the `WHERE` first when no field
can fail and none is hidden by a later `*`) gains a third condition: no `SELECT` field
calls a state function.

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

### D4. Each key has a type, and every type merges as a CRDT

Replicas of a key must converge whatever order updates arrive in, without asking anyone to
resolve a conflict mid-evaluation. The operation that creates a key gives it its type,
and each type's merge is a CRDT:

| Type | Written by | Concurrent updates resolve to |
|---|---|---|
| `lww` | `kv_set`, `kv_changed` | the later write by hybrid logical clock (D6); ties by node id |
| `first` | `kv_set_once`, `kv_held` | the earliest write that no delete covers |
| `max`, `min` | `kv_max`, `kv_min` | the larger or smaller number |
| `sum` | `kv_incr` | the sum of each node's own increments and decrements (two per-node count vectors) |
| `union` | `kv_add` | the union of the elements, at most 1,024 |
| `hll` | `kv_hll_add` | the register-wise maximum of two HyperLogLog sketches |
| delete | `kv_del`, `kv_held` on a false condition | a tombstone stamped by the clock: it covers every write stamped before it, and is kept for the namespace's tombstone window so a late replica cannot undo it |

Full vector clocks with sibling values (a multi-value register) are not offered: they
detect concurrency but leave it unresolved, and a rule has no one to resolve it. The hybrid
logical clock keeps causally ordered writes in order, so `lww` drops only one of two
genuinely concurrent writes, which is the right outcome for a "latest value". Where losing
either is wrong, the key uses a type that keeps both (`sum`, `union`, `max`, `first`).
`hll` uses 2^14 registers (about 0.8% standard error, 12 KiB dense, sparse below that).
An expiry merges with its write for `lww` and `first`, and as the later of the two for
the other types. A key is replicated as its whole state, which is small (D9) and merges
idempotently, so a resent or reordered update is harmless.

*(Amended 2026-10-11, T2: a delete covers exactly the writes stamped before it for `lww`;
for the other types it also covers the writes of nodes that had not heard of it. `first`'s
expiry is the later of the two. See [Amendments](#amendments).)*

*(Amended 2026-10-11, T3: a counter keeps a count for each run of each node, not for
each node, so that a restart loses no counts; a delete of a key this node holds nothing
of does nothing. See [Amendments](#amendments).)*

### D5. Consistency is chosen per namespace

| Mode | Write | Read | During a partition | Cost on the publish path |
|---|---|---|---|---|
| `local` | this node | this node | unaffected; no replicas | an in-memory map operation |
| `eventual` | applied on this node, then shipped to every other member in the background | this node, possibly briefly stale | both sides work; values converge on heal | an in-memory map operation; replication is off the path |
| `strong` (phase 2) | through the key's leader, acknowledged by a quorum before the rule continues | from the leader | the minority side's calls fail (counted, traced) | a round trip to the leader per call, read or write |

- **`eventual` replicates a namespace to every member of the cluster**, not to a replica
  set chosen by key hash. A rule is evaluated on the node its message arrived at and reads
  that node's copy; a device reconnects wherever the load balancer sends it. Only a copy
  on every node makes "read this node" give the device's state there. The first draft
  placed each key on `replication_factor` nodes (ADR 0080) and still read locally, which
  returns nothing on any node outside that set. *(Decided by default, 2026-10-11: every
  member.)* The price is stated in Consequences: each node holds the whole namespace, and
  a write is sent to every other node.
- **How it travels** is D15: off the publish path and off the hub, bounded by the number
  of keys, repaired by anti-entropy after a partition or a restart.
- **`strong` is phase 2 and is not designed here.** A `strong` call waits for a quorum,
  and evaluation is synchronous on the publisher's connection task (ADR 0083, ADR 0086
  D6). The only mechanism the broker has for a call that waits is ADR 0086's hand-off
  (`block_in_place`), built for the rare long `jq`; a `strong` namespace would take it on
  every call, each waiting connection holding a thread, and the cap on concurrent waits
  is the question ADR 0086 left open. T9 therefore starts with an amendment to this
  record that settles the wait (its bound, its timeout, what a Will's rules do: with the
  rule set marked as one that may wait, the hub already hands a Will's rules to a task),
  and nothing of `strong` is built before it. Until then `consistency = "strong"` is
  refused at load. *(Decided by default, 2026-10-11: phase 1 ships without it.)*
- **Persistence:** `local` and `eventual` state is in memory. An `eventual` namespace is
  rebuilt from the other members after a restart; a `local` one starts empty, and so does
  every namespace after the whole cluster stops. Backups (ADR 0062) do not carry it. An
  optional snapshot to disk is phase 3.

### D6. Time: one hybrid logical clock per node

Every write is stamped with the node's hybrid logical clock (physical milliseconds plus a
logical counter, merged on receipt), which orders `lww`, `first` and deletes and survives
modest clock skew. `kv_held` measures from that clock's physical part; expiries are
absolute times on it. `now_timestamp()` is unchanged: EMQX's function, the wall clock read
at each call. Durations computed across nodes (a `kv_held` whose device moved, a TTL
judged by another node) are only as good as the nodes' clock agreement; the documentation
says so and the rig's clock gate is the reference practice.

*(Amended 2026-10-11, T2: a stamp more than 60 seconds ahead of the receiving node's wall
clock is refused, not followed; a `local` namespace is not stamped. See
[Amendments](#amendments).)*

### D7. Expiry raises an event

A key whose expiry has passed reads as absent from that moment. It is removed by a sweep,
and when a rule on the node selects it the sweep raises `$events/state/expired`, with
`event`, `namespace`, `key`, `value` (what `kv_get` returned last), `expired_at`,
`timestamp`, `node` and `metadata.rule_id`. Refreshing a key with a TTL on every message
therefore detects silence: "no message for 10 minutes" is
`kv_set('seen:' + clientid, now_timestamp(), 600)` in one rule and
`FROM "$events/state/expired"` in another.

- **An mqttd event, selected only by its own name.** EMQX has no such event. A wildcard
  such as `"$events/#"` keeps selecting exactly the events EMQX's would (ADR 0083, amended
  2026-10-10), so a rule ported from EMQX never starts receiving this one. Listed under
  "Differences from EMQX". The `kv_store` namespace (D13) has no expiry and raises none.
- **Which node.** In a `local` namespace, this node. In an `eventual` one, the member that
  a hash of the key picks among those alive, so normally one node; around a membership
  change two nodes can each pick themselves. The event is therefore **at least once**,
  and stated so. Each node evaluates it against its own rules file.
- **Where its rules run:** on a task the store owns, never the hub loop and never a
  connection. What they derive is routed ungated and charged to no connection's credit,
  as for every event (ADR 0083 §4, §8), within the per-message bounds.
- **Bounded, late rather than lost.** The sweep hands over at most
  `rules.state_expiry_per_sec` events a second (default 1,000). A key is removed only as
  its event is handed over, so when a whole site falls silent at once the events arrive
  late, in expiry order, and none is shed. A node that stops before it has raised them
  does not raise them: the next owner does, from its own copy.
- **A rule on this event may write state**, including the key that expired. The shortest
  TTL is one second, which bounds any loop built that way.

### D8. Idempotency for counters

A counter cannot tell a redelivered message from a new one: a QoS 1 resend, a takeover
replay or a client retry is evaluated again (D3) and would count twice. `kv_incr` takes an
optional idempotency key (a string of at most 256 bytes, typically the message's id or a
payload field); the increment is applied only if that key is new to the namespace, and the
key is remembered for the namespace's `dedupe_window` (default 5 minutes, at most 1 hour).
Remembered keys are entries of the namespace like any other (`first` type, with the window
as their TTL): they count toward its bounds and they replicate. On one node the check is
exact. Across nodes it holds once the entry has arrived: a resend that reaches another
node inside the replication lag counts twice. Without an idempotency key, the
documentation states that counts are at least once.

### D9. Bounds, accounting and eviction

Keys carry client-chosen values (`clientid`, topic levels, payload fields), so everything
a message can make the store hold or do is bounded (ADR 0082's principle, and the threat
model's rule for payload-driven work):

- **Per file:** at most 64 declared namespaces, and one namespace of its own per rule
  that uses state without naming one.
- **Per namespace:** `max_keys` (default 100,000, at most 1,000,000) and `max_bytes`
  (default 64 MiB, at most the store's ceiling), whichever is reached first; `default_ttl`
  (default 24 hours) and `max_ttl` (default and at most 30 days); a tombstone window
  (default 1 hour) and the dedupe window of D8. Tombstones and remembered idempotency
  keys count as keys until they expire.
- **Per key:** 256 bytes of key; a value of 4 KiB; a set of 1,024 elements, each at most
  256 bytes; a sketch of 12 KiB; a counter of one pair of counts per node id that ever
  incremented it.
- **Per message:** at most 1,024 state calls, read or write, counted in the message's
  `Budget`, so the limit covers every rule the message matches and every message it
  leads to (as the limit on effects does); past it the call fails the rule. What a read
  returns is charged whole to the message's 1 MiB build budget, as a `jq` call's output
  is (ADR 0086 D7), so a `FOREACH` cannot multiply stored values into a large message.
- **The store:** `rules.state_max_bytes` across all namespaces (D1). The namespaces'
  bounds may add up to more; whichever bound is reached first refuses.
- **When a bound is reached,** a write that would create a key fails the rule (counted by
  reason: `namespace_full`, `store_full`); writes to existing keys still work, within the
  per-key bounds. `eviction = "lru"` evicts the least recently used key instead, for
  caches, in `local` namespaces only (an eviction on one node of an `eventual` namespace
  would be undone by the next repair).
- **Memory watermark** (ADR 0041): the store is process memory and counts toward it. In a
  brownout new keys are refused (`brownout`) and existing keys still update, expire and
  delete.
- **Replication** adds no queue a message can grow: D15.

### D10. Observability and admin

- **Metrics:** `mqttd_rule_state_keys{namespace}`, `mqttd_rule_state_bytes{namespace}`,
  `mqttd_rule_state_ops_total{namespace,op,result}`,
  `mqttd_rule_state_expired_total{namespace}`,
  `mqttd_rule_state_expiry_backlog{namespace}`, and for `eventual` namespaces
  `mqttd_rule_state_replication_lag_seconds{namespace}`,
  `mqttd_rule_state_replication_pending{namespace}` and
  `mqttd_rule_state_replication_refused_total{namespace,reason}`. The `namespace` label
  comes from the operator's file (a declared name, or `rule:<id>`), never from a client:
  the same bounded exception to ADR 0020 as the `rule` label (ADR 0083 §7). A rule's
  evaluation time (`mqttd_rule_eval_seconds_total`) includes its state calls.
- **`$SYS`:** with `sys_interval_secs` (ADR 0084 D4), one message per namespace on
  `$SYS/brokers/<node>/rule_state/<namespace>`: counts, bytes, bounds, rates. A subtree of
  its own, so a grant on the rule statistics does not cover it. Never a key or a value,
  which can carry payload data.
- **The trace** (ADR 0084 D5) records a failed state call as its rule's error, with the
  reason. It records no key and no value beyond what the rule's outputs already carry.
- **Admin API** (ADR 0081, in the shape of ADR 0084 D6: per node, parameters in the query
  string): `GET /admin/v1/rules/state` (viewer: each namespace's mode, bounds, counts and
  bytes), `GET /admin/v1/rules/state/keys?ns=&prefix=&cursor=` (operator: at most 1,000
  keys a page), `GET /admin/v1/rules/state/key?ns=&key=` (operator: the value, type and
  expiry), `DELETE /admin/v1/rules/state/key?ns=&key=` (operator and a listed rules
  writer; audited as `state.delete`; in an `eventual` namespace it is a replicated
  delete). Values are shown to operators only, as the rules' SQL is.
- **CLI** (ADR 0084 D7's style): `mqttd --admin state`, `state-keys <ns>`,
  `state-get <ns> <key>`, `state-del <ns> <key>`.

*(Amended 2026-10-11: the listing gains each key's type, size and expiry and its
filters; a bulk purge and a change of expiry are added, with their verbs. T6 builds this
section as written; T12 to T15 build the additions. No last-write time is kept. See
[Amendments](#amendments).)*

### D11. Testing: sequences, not single messages

`mqttd --rule-test` gains `--sequence <file>`: a timed list of messages and events
replayed through one statement, or through the configured rules file, against an
in-memory store with a clock the sequence drives, printing each step's outputs and what
the step changed in the store. Expiry events fire in the replay when the sequence's
clock passes them. This is how a rule with state is written and reviewed: the examples
become a sequence with expected outputs, and RULES.md's worked examples are such
sequences, run by CI.

*(Amended 2026-10-11, T7: the file's format, the replay's clock and the report as built;
the expiry events in a replay wait for T4. See [Amendments](#amendments).)*

**No test touches the running store.** `--rule-test`, `--check-rules` and the admin API's
`POST /admin/v1/rules/test` (ADR 0084 D6: "a dry run touches no counter, no last error,
no trace") load and evaluate against an empty in-memory store that is dropped afterwards. EMQX's
SQL tester calls the real functions, so a test of `kv_store_put` there writes the node's
table; here it does not, and that is listed as a difference.

### D12. Not a system of record

The store makes live decisions; it is not a ledger. `eventual` counters can be read
before every node's increments arrive, so a threshold can fire late or twice; a node lost
before it replicates loses its last increments; a whole-cluster restart loses everything.
Billing is the worked example in the documentation: publish each billable event as a
durable QoS 1 message that the billing system consumes and deduplicates by message id,
and use the store only to decide what to do next. Exact cluster-wide counters wait for
`strong` (phase 2).

### D13. EMQX's own state functions, with EMQX's behaviour

mqttd's rule engine is EMQX's without exception (ADR 0083, amended 2026-10-10), and these
six functions are EMQX's. They are provided under their names with what
`emqx_rule_funcs.erl` does, read from the source (emqx/emqx master: `kv_store_put/2` is
`ets:insert(?KV_TAB, …)`, `kv_store_get/1,2` an `ets:lookup` with a default,
`kv_store_del/1` an `ets:delete`, on a `set` table created by `emqx_rule_engine_sup`;
`proc_dict_*` are `erlang:put/get/erase` under a `{'@rule_engine', Key}` key). Each value
below is to be probed on EMQX 6.3.1 by the task that builds it, as every other function
was.

| Function | Returns | Where the value lives |
|---|---|---|
| `kv_store_put(key, value)` | `true` | one table per node, shared by every rule on it |
| `kv_store_get(key)`, `kv_store_get(key, default)` | the value, or `undefined` / `default` | the same |
| `kv_store_del(key)` | `true`, whether or not the key existed | the same |
| `proc_dict_put(key, value)` | the value held before, or `undefined` | the evaluating process |
| `proc_dict_get(key)` | the value, or `undefined` | the same |
| `proc_dict_del(key)` | the value held before, or `undefined` | the same |

- **`kv_store_*` is the reserved namespace `kv_store`:** always `local` (an `ets` table is
  per node), shared by every rule whatever its `state` key says, no expiry, a key and a
  value of any type compared exactly (an `ets` `set` matches keys with `=:=`: `1` and
  `1.0` are two keys), `undefined` storable. They take effect when evaluated, a `SELECT`
  field before the `WHERE` (D3): the same rule as for `kv_*`, because it is EMQX's.
  `[state.kv_store]` may set its bounds and `eviction`; any `consistency` but `local`, and
  any TTL, is refused. EMQX's current source scopes the table by the rule's EMQX
  namespace; mqttd has no such namespaces (ADR 0083), so it is EMQX's one global key
  space.
- **`proc_dict_*` is a dictionary per evaluating task.** In EMQX the process is the
  client's connection process for its publishes, its events, the deliveries to it and
  its Will, so the values are seen by nothing else and live as long as that process: as
  long as the connection, and for a session that outlives its connection until the
  session is resumed (amended 2026-10-11, T11). mqttd evaluates those on the client's
  connection task (ADR 0083), which gets the dictionary: empty at connect, gone at
  disconnect, not carried by a takeover or a resumed session (EMQX starts a new process
  there too). A republished message is evaluated on the same task and sees it, as in
  EMQX. *(Decided by default, 2026-10-11: provided. The other answer keeps them
  refused. Delivered: T11, #933.)*
- **What differs from EMQX,** each listed in RULES.md:
  - *Bounds.* EMQX's table and dictionaries grow until the node runs out of memory.
    Here `kv_store` has the bounds of any namespace (D9), with a key of at most 256
    bytes as written and a value of 4 KiB, and a dictionary holds at most 64 keys and
    64 KiB per connection, counted against the store's ceiling. Past a bound the call
    fails the rule, where EMQX would have stored the value.
  - *A Will, and events about a client that has no connection.* EMQX evaluates a Will in
    the dying channel process, which still has the connection's dictionary. mqttd
    evaluates it on the hub or on a task of its own (ADR 0086), with an empty dictionary
    that is discarded afterwards; so is the dictionary of an event evaluated by the
    notes task or the store's expiry task. The notes task's events are those EMQX
    still evaluates in a process that has the dictionary: a `delivery_dropped` for a
    session without a connection, and `message_dropped` for a message a rule
    republished or for a Will (amended 2026-10-11, T11).
  - *The test.* A dry run uses a scratch table (D11), and a scratch dictionary: for
    the dictionary that is what EMQX's SQL test over its API does too, each request
    being a process of its own.
- **The two families do not share keys.** `kv_get('x')` does not read what
  `kv_store_put('x', …)` wrote. `mqttd --check-rules` warns about a file that calls both.
- These names leave the list of EMQX functions mqttd refuses (`EMQX_ONLY`), and the
  converter stops commenting out rules that call them.

### D14. Where a state call runs, what it may cost, and the sandbox

- **On the task that evaluates the rule,** synchronously: the publisher's connection task
  for a publish and for what it republishes; the subscriber's for `delivered` and `acked`;
  the hub for a Will (or the Will's own task while the rules call a sandboxed function);
  the notes task; the store's expiry task. There is no second place and no queue between
  the call and its effect.
- **A `local` or `eventual` call never waits.** It is one operation on a sharded
  in-memory map, under a lock held for that operation only: never across an `await`,
  never while a sandboxed call runs (arguments are evaluated before the call), never
  while anything is sent. It therefore needs no hand-off, does not make a rule set one
  that "calls the sandbox" (`RuleSet::calls_sandbox`), and leaves a Will's rules on the
  hub, where a map operation costs what it costs anywhere.
- **What it costs is measured by T1, not asserted here.** Two requirements: a rule set
  that calls no state function evaluates exactly as it does today (the per-publish
  benchmark does not move outside its spread: no store, no lookup, no field read on the
  path); and the cost of each call, on one core and under contention from every worker,
  is recorded in RULES.md beside the other costs.
- **With `jq` and modules.** A state call and a sandboxed call compose as expressions do:
  `kv_set(k, jq('.a', payload))` runs the jq call first, handed off if it is long, and
  then writes. **A module cannot call state**, and neither can a jq program: ABI 1 has no
  host functions (ADR 0086 D9: "a plugin cannot read broker state"). A rule passes state
  to a module as an argument and stores what it returns. Giving modules a state import
  would be a new decision, with a new trust question (a module's memory could then
  outlive its instance).
- **`strong` is the mode that waits,** and for that reason is not built in phase 1 (D5).

### D15. The cluster: off the hub, additive on the wire, per-node files

- **Sending.** A write to an `eventual` namespace marks its key as changed for each peer.
  A task per peer link sends the changed keys' states in batches and clears the marks, so
  what waits to be sent is bounded by the number of keys, not by the number of writes: a
  key written a thousand times while a link is slow is sent once. When the link is
  congested the task waits; nothing is dropped and no connection is paused.
- **Receiving.** The peer link's reader applies each state to the store directly, a map
  operation. State frames are not hub commands and wait in none of the hub's lanes (ADR
  0082), so they can neither delay the control lane nor be shed with the data lane.
- **Repair.** At link-up and periodically, two members compare per-namespace digests and
  exchange what differs. That is how a restarted node is filled and a healed partition
  converges.
- **The wire:** new frames appended at **peer protocol 14**, additive (`PROTO_MIN` stays
  6; ADR 0038, ADR 0039). A node sends them only on a link that negotiated 14, and only
  for namespaces it has `eventual` state in, so a cluster that uses no such namespace
  puts nothing new on the wire. In a rolling upgrade a node still on 13 holds no state
  and receives none; rules that use state do not load there anyway (D1).
- **Rules files are per node** (ADR 0083 §6), so two nodes can declare a namespace
  differently. A state frame names its namespace and a fingerprint of its declaration
  (mode and type-relevant settings). A node that does not declare the namespace, or
  declares it otherwise, refuses the frame and counts it
  (`mqttd_rule_state_replication_refused_total{reason}`); the cluster view's `same_rules`
  check (ADR 0084) already shows the drift.
- **A node at a bound** applies a replicated new key only while its copy has room;
  otherwise it refuses and counts it, and repair retries later. A namespace run at its
  bound is therefore not guaranteed to hold the same keys on every node.

*(Amended 2026-10-11, T3: how keys are sent without a queue, the digest and the exchange
that repairs, the three frames, the fingerprint, and what a merge refuses. See
[Amendments](#amendments).)*

## Decided by default, 2026-10-11

These are choices with a real cost either way that neither EMQX's behaviour, the code nor
an obvious default settles. The record is written on the recommended answer; each is
cheap to revisit before the task named builds it. **Standing** says where each is now:
the user was asked on 2026-10-11 and confirmed two; the other three stand by default,
one of them delivered. Rows 6 to 8 were added the same day by the amendment "admin
features and confirmed defaults"; the user withdrew what row 6 decided about (the
amendment "no last-write time"), and rows 7 and 8 are open to the user in the same way.

| # | Choice | Decided | Standing | It costs | The other answer |
|---|---|---|---|---|---|
| 1 | Where an `eventual` namespace is replicated (D5) | To every member | By default | Every node holds the whole namespace; a write is sent to N−1 nodes | A replica set per key (`replication_factor`): less memory, but a rule on a node outside the set reads nothing unless reads go over the network on the publish path. Revisit before T3 |
| 2 | `strong` mode (D5) | Phase 2, behind a design amendment; refused at load until then | By default | No exact cluster-wide counter in phase 1: an `eventual` quota can over-admit by what was in flight | Build it now on the hand-off: every call a thread hand-over, and concurrent waits uncapped (ADR 0086's open question). Revisit at T9 |
| 3 | EMQX's `proc_dict_*` (D13) | Provided, per connection task | By default; **delivered** (T11, #933) | One task (T11), up to 64 KiB per connection that a rule makes use it, the documented differences (a Will; events without a connection; `message_dropped` for a republished message or a Will) | Keep refusing them: a rule that uses them stays unportable. No longer cheap: rules may use them now |
| 4 | Is state on without a switch (D1) | Yes: using it is the switch, under `rules.state_max_bytes` = 256 MiB; `0` turns it off | **Confirmed by the user, 2026-10-11** | A listed rules writer (ADR 0084) can make a node hold up to that much | Off unless switched on, as `jq` is: then a ported EMQX rule that calls `kv_store_put` does not load until the operator acts |
| 5 | The names (D3) | `kv_*`, beside EMQX's `kv_store_*` | **Confirmed by the user, 2026-10-11** (asked, no rename requested) | Two families one word apart with different tables (a warning when a file uses both); a clash if EMQX ever adds a `kv_get` | `state_*` for mqttd's. T1 has shipped them: a rename now breaks rules |
| 6 | ~~Where a key's last-write time lives in an `eventual` namespace (amendment, T12)~~ | ~~On each node, in memory, 8 bytes a key; not in the replicated form~~ | **Withdrawn: the user dropped the last-write time, 2026-10-11** ("drop the stale key idea completely. drop the extra bytes") | Nothing: no time is stored, on a node or on the wire | None is open: there is no last-write time to place |
| 7 | Moving an expiry sooner on a key that is not `lww`, in an `eventual` namespace (amendment, T14) | Refused, and counted in a selection as `not_shortened`; the operator purges instead | By default | No way to make such a key expire early *with its expiry event* on every node | Delete and write back under a new delete stamp: the shorter expiry then replicates, and writes made on nodes that had not heard of it are lost (a counter loses increments). Revisit before T14 |
| 8 | Clearing an expiry outside `kv_store` (amendment, T14) | Offered: the one admin act that leaves `max_ttl` | By default (the user approved "set, extend or clear … within `max_ttl`"; the two meet only here) | A key no rule could have made: it never expires and holds its room until a rule or an operator deletes it. The listing's "no expiry" filter finds them | Refuse `clear` wherever the namespace has a `max_ttl`, i.e. everywhere but `kv_store`, where no expiry may be set at all: then `clear` does nothing anywhere and is dropped. Revisit before T14 |
| 9 | Whose counts a counter keeps in an `eventual` namespace (amendment, T3) | Each run of each node counts under a name of its own | By default | A counter gains a row for each node restart that then increments it, and refuses a new run's increment past 256 rows until it is deleted | Count under the node id alone: no growth, and a restarted node silently loses what it counts before it has been refilled. Revisit before T8 documents it |

## Phasing

1. **`local` and `eventual`, every type, EMQX's own functions, expiry events, bounds,
   metrics, the admin verbs and `--sequence` testing** (T1 to T8, T11). The stateful
   patterns ("above Y for X", change, counting windows, silence, distinct counts) all
   work in this phase, and `kv_store_*` rules port from EMQX. The operator's view of
   the contents, the purge and the change of expiry (T12 to T15, amended 2026-10-11)
   follow T6 and carry their own documentation, so they do not hold T8.
2. **`strong`** (T9), after its design amendment (D5), with partition and failover tests
   in the style of the durable plane's own before it is called supported.
3. **An optional snapshot to disk** for `local` and `eventual` namespaces (T10).

## Consequences

- Rules can express duration, change, counting, absence and distinct counts, which
  removes the most common reason to put a separate stream processor beside the broker.
- EMQX rules that call `kv_store_*` or `proc_dict_*` load and behave as they do there,
  within the bounds of D13.
- **A state call in `SELECT` runs for every message the `FROM` selects.** That is EMQX's
  order and it is what makes `kv_held` and `kv_changed` work, and it surprises anyone who
  reads SQL as "filter, then compute". RULES.md says it where the functions are, with the
  `CASE` and `AND` forms that write only on a match, and `--sequence` shows it.
- A rule that uses state costs an in-memory map operation per call in `local` and
  `eventual` mode. A rule set that uses none pays nothing.
- An `eventual` namespace costs its size in memory on every node, and replication traffic
  in proportion to the number of keys that change, per peer.
- The store is new memory the broker holds on behalf of client-shaped keys. D9 bounds it
  per key, per namespace, per message and in total, and the threat model gains rows for
  state exhaustion (a publisher filling a namespace so that others' keys are refused, or
  under `lru` evicted), for values derived from payloads shown to operators, and for a
  rules writer's reach (the ceiling, and dropping a namespace by editing the file).
- `$events/state/expired` is at least once and can be late under a mass expiry;
  `eventual` reads can be briefly stale; all state is lost when every node stops. Each is
  stated where the functions are documented.
- The `kv_*` functions, `[state.*]`, the rule key `state` and the expiry event are not
  EMQX's: rules that use them do not port back. The departures table lists them, with the
  bounds on EMQX's own functions.
- The peer protocol goes to 14.

## Alternatives considered

- **Holding writes back until the rule matches** (apply a statement's writes only if its
  `WHERE` passes, or only when the hub accepts the publish). It reads naturally and it is
  not EMQX's behaviour for the state functions EMQX has. It also breaks the functions
  this record exists for: `kv_held` records the first hot reading on a message whose
  `WHERE hot_for >= 300` is false, and that write would be discarded every time. And a
  later call in the same message would have to read writes that have not happened.
  Rejected.
- **Evaluating the `WHERE` first** so that a `SELECT` write runs only on a match (#882).
  Reverted in #884 for changing errors and counters against EMQX; the same reason holds.
- **Retained topics as state.** Rejected as the mechanism (stale reads under bursts, a
  durable write per state change, no absence), kept as an optional mirror: a namespace may
  later publish its changes as retained messages for visibility.
- **An external Redis, read per evaluation.** Rejected: latency on every publish a rule
  sees and a new failure dependency. Writing state out to external systems is a separate
  feature.
- **Mapping `kv_store_*` onto a replicated namespace.** Rejected: EMQX's table is per
  node, and a rule ported from EMQX must see what it saw there. An operator who wants
  replication uses `kv_*`.
- **Vector clocks with sibling values.** Rejected: they surface conflicts a rule cannot
  resolve. Typed merges (D4) resolve them deterministically.
- **One global consistency setting.** Rejected: a last-seen time and a quota counter need
  different guarantees in the same cluster.
- **`strong` only.** Rejected: a quorum round trip per state call is the wrong default for
  per-device telemetry state, which is most of the demand.
- **A namespace named inside the key** (`ns:key`). Rejected: keys are built from values a
  publisher chooses (D1).

## Amendments

### 2026-10-11 — review before acceptance

The record was written on 2026-10-09 and last revised with #882. Before accepting it, it
was read against the code on `main` and against EMQX's source. This lists what was no
longer true, what was inconsistent, and what was missing.

**No longer true**

- **"A rule decides its `WHERE` first" (D3).** #882 was reverted by #884 on 2026-10-10:
  evaluation is EMQX's, `SELECT` before `WHERE`. The paragraph promised that a state call
  in a field the `WHERE` does not read "runs only for messages that pass". It runs for
  every message. D3 now states EMQX's order and its consequences, clause by clause, with
  EMQX's own `kv_store_put` as the evidence: `emqx_rule_runtime:evaluate_select/3` calls
  `select_and_transform/2` and only then `match_conditions/2`, and the function is an
  `ets:insert` with nothing to undo it.
- **"A derived message is routed asynchronously … so the next reading can be evaluated
  before the state write lands" (Context).** Republished messages now re-enter the rules
  on the same task. The argument still holds for a different reason, now given: the
  retained *store* is written by the hub, later.
- **"A rule runs on the hub only for a Will, which must not wait: there, a `strong` write
  is submitted without waiting" (D5).** Since ADR 0086 the hub hands a Will's rules to a
  task while the rules may run long, so the special case is unnecessary; and events for
  clients without a connection are evaluated by a third task, which the text did not
  know. D14 lists every place.
- **"`now_timestamp()` … read once per evaluation" (D6).** It reads the clock at each
  call, as EMQX's does, and stays so.
- **"State through a trait carried by `EvalCtx`" and "every state function fails cleanly
  when no store is wired, so a rules file still loads anywhere" (D2).** ADR 0086 gave
  the rule engine a load-time hand-over (`Functions`), and a function the host cannot
  provide is a load error there. State follows it.
- **"These functions are an mqttd extension" as the whole of the relation to EMQX.** EMQX
  has state functions, mqttd refuses them, and ADR 0083 left the mapping to this record.
  D13 provides them with EMQX's behaviour.
- **"The loop the rule-authoring skill drives" (D11).** There is no such skill in the
  tree. The sentence and the acceptance criterion that leaned on it are gone; RULES.md's
  worked sequences, run by CI, take their place.
- **"Peer bus" without a version (D5).** The protocol is at 13 (the message events). The
  state frames are 14, additive (D15).

**Inconsistent in itself**

- **`eventual` placed keys on a replica set and read locally (D5).** A node outside the
  set would read nothing. Now every member holds the namespace (decided by default, 1).
- **`ns:key` addressing (D3)** collided with the record's own example key and let a
  publisher's value choose the namespace. A rule now names its namespace in its table
  (D1).
- **`strong` "one replication round trip per write", with reads "from the leader".** A
  read on any other node is a round trip too. The table says so.
- **D4 named `kv_add`, and a `max`/`min` "declared" per key; D3 had neither** a function
  nor a way to declare. `kv_add`, `kv_max` and `kv_min` are in D3; a key's type is its
  creating operation's, and a mismatch is `wrong_type`.
- **Default bounds that did not bound memory.** 100,000 keys of 256 bytes with 4 KiB
  values is over 400 MiB per namespace, times 64 namespaces. D9 adds `max_bytes` per
  namespace and a ceiling for the store in the broker's configuration.

**Missing**

- **How a state call fits ADR 0086's evaluation model** (D14): where it runs, that it
  never waits in phase 1, what the hand-off would mean for `strong` and why that defers
  it, that a module cannot call state, the per-message call budget, reads charged to the
  build budget.
- **The dry runs** of ADR 0084 and `--rule-test` must not write the running store (D11).
- **Reload** (ADR 0084): what happens to keys when a declaration changes or goes (D1).
- **Per-node rules files** against a replicated store: the fingerprint and the refusal
  (D15).
- **The expiry event against the event rules of ADR 0083**: selected only by name, where
  its rules run, its rate bound, its fields (D7).
- **Idempotency keys across nodes** (D8): they replicate as entries of the namespace, and
  the window in which a resend to another node still counts twice is stated.
- **LRU eviction in a replicated namespace** would be undone by repair; it is `local`
  only (D9).
- **Re-entry:** a rule runs again on its own republished output, so its state calls do
  too (D3).

**Tasks.** T1 gains EMQX's `kv_store_*`, the order tests and the ceilings; T3 the
every-member replication and protocol 14; T9 is gated on an amendment; T11 (`proc_dict_*`)
is new. The delivery doc's changelog has the rest.

### 2026-10-11 — T1: what EMQX 6.3.1 answered, and what building it settled

D13 was written from EMQX's source. T1 probed it on EMQX 6.3.1 (`emqx/emqx:latest`):
141 probes, `emqx_rule_funcs` called directly and statements run through
`emqx_rule_sqltester:test/1`, each effect read back from the node's table. They are
pinned as tests of the broker's store (`crates/mqttd/src/rule_state/tests.rs`), which
name each probe.

**Nothing in D3 or D13 was contradicted.** `kv_store_put` is `true`, `kv_store_get` the
value or `undefined` or the default, `kv_store_del` `true` either way; a write in a
`SELECT` field is in the table after a false `WHERE`; `AND`, `OR` and `CASE` evaluate
what they need and no more; a write stays when a later field or the `WHERE` fails the
rule, and a field that fails first stops the write after it; in a `FOREACH` the
collection's calls run before the `WHERE`, the `INCASE` per element, the `DO` per element
that passed; three messages through `kv_store_get('n', 0) + 1` count 1, 2, 3; the SQL
test writes the real table.

**What the probes added**

- **Exactness goes further than `1` and `1.0`.** `0.0` and `-0.0` are two keys (OTP's
  `=:=` since 27); `true` and `null` are not the strings `'true'` and `'null'`;
  `undefined` is a key; a map is the same key whatever order it was written in, and one
  with `1.0` where another has `1` is not; an integer past 64 bits is a key and not the
  float of the same value.
- **A stored `undefined` is a value:** `kv_store_get(Key, Default)` returns it, not the
  default. The default is for a missing key only, and is evaluated whether or not it is
  used, like any argument.
- **A wrong argument count** is a failure on every message in EMQX
  (`sql_function_not_supported`); in mqttd it is a load error, as for every function
  (ADR 0083). Listed under "Differences from EMQX".
- **An `IN` list is evaluated whole** (`lists:member(eval(Var), [eval(V) || V <- Vals])`):
  `1 IN (1, kv_store_put('k', 1))` writes, and `1 IN (1, abs('x'))` fails the rule.
  mqttd's evaluator stopped at the first match, which no test could see before a function
  had an effect. It now evaluates the list whole: the write happens, and the rule fails
  where EMQX's does (ADR 0083, amended the same day).

**Seen while probing, and left as it is** (neither is about state; both are the
maintainer's to decide): in a `WHERE`, EMQX fails the rule (`function_clause`) on
`x NOT IN (…)` and on an `AND`/`OR` operand that is a bare value rather than a comparison
or a call (`payload.flag AND …`); mqttd evaluates `NOT IN`, and reads a bare operand as
"not `true`" unless it is `true`. Where the left operand of an `OR` is such a value, mqttd
therefore goes on to the right operand, state calls included, where EMQX has already
failed the rule. *(Decided later on 2026-10-11: both are kept as documented differences
(ADR 0083, 2026-10-11). No rule that works in EMQX holds either form; RULES.md lists them
under "Conditions EMQX fails and mqttd evaluates".)*

**What the record left open and T1 settled**

- **A non-`local` declaration in this build:** `consistency = "eventual"` is refused at
  load ("not available yet"), as `"strong"` is. A rules file written for T3 does not load
  on a T1 build, and says why.
- **Where the settings live.** The store holds namespaces weakly and each loaded rule set
  holds the ones it uses, with its own settings. That is the whole of D1's reload rule:
  a namespace two sets hold is one namespace (it keeps its keys), the new set's bounds
  apply to the new set's calls, and a namespace no set holds any more goes with the last
  set that held it. A load that fails, or a candidate the admin API only checks, leaves
  nothing behind.
- **A dry run is told its store.** `RuleSet::evaluate_one` takes the scratch store and
  resolves each namespace in it by name; given none, a state call fails the rule. The
  running store cannot be reached from a dry run by forgetting an argument.
- **`kv_ttl` rounds up,** so a key that is still held never reads `0`.
- **`kv_incr` counts in 64 bits;** `by` is a 64-bit integer, and a count that would pass
  64 bits fails the rule and keeps the count (T2's per-node vectors may widen it).
- **A set's elements** are one when they are exactly equal (`1` and `1.0` are two), and
  read back in term order, exact identity deciding between values that order calls equal.
- **`kv_held` on a key `kv_set_once` wrote** (both type `first`) fails as `wrong_type`
  when the value is not a time `kv_held` recorded.
- **Accounting.** A key is accounted as its bytes, its value's, and 128 bytes for the
  entry; an element of a set as its bytes and 32. Without that a namespace of small
  values would hold several times its `max_bytes`.
- **Expiry before D7's sweep.** A key past its expiry reads as absent at once and is
  removed by the call that finds it; when a namespace needs room, expired keys are removed
  first (each shard keeps its keys in order of expiry, to the second, which is the order
  the sweep will walk).
- **`lru`.** A read is a use. Eviction is of the namespace's least recently used key,
  also when the store's ceiling, not the namespace's bound, is what is in the way; in a
  brownout nothing is evicted and a new key is refused.
- **The brownout** is the memory watermark's (ADR 0041 T8). The disk watermark does not
  refuse state.
- **A `kv_store_*` key's size** is measured as the rule engine writes the key (a string as
  its bytes, anything else as JSON text): at most 256 bytes.

**The concurrency structure** (D14 asked for a sharded map): 64 shards per namespace, each
a map behind its own mutex, a key's hash choosing the shard; counts and bytes in atomics
that a new key reserves with a compare-and-swap that never passes a bound. A call takes
one shard's lock for one map operation, trying it a few times before it waits, because
the lock is held for about a hundred nanoseconds. Measured on eight cores, each thread on
keys of its own: 0.12 µs for a call alone, 0.20 µs with four threads calling, 0.33 µs
with eight. All of them on one key: 0.19 µs with two, 0.63 µs with four, 1.9 µs with
eight. RULES.md carries the figures.

**Still open after T1.** The admin API's per-rule edit (`PUT /admin/v1/rule`) keeps a
rule's `state` key but cannot set one, since ADR 0084's rule body has no such field: a
writer declares namespaces and names them by writing the whole file. T6 or T8 decides
whether the body gains the field.

### 2026-10-11 — T11: what EMQX 6.3.1 answered about `proc_dict_*`

D13 said what the three functions return from EMQX's source, and which process the
dictionary belongs to from reading the channel. T11 probed both on EMQX 6.3.1
(`emqx/emqx:latest`): 99 probes. 30 call `emqx_rule_funcs` directly; 29 run statements
through `emqx_rule_sqltester:test/1` in one process and read its dictionary back; 3 run
the SQL test over the REST API; 37 use clients on the running node, with rules that put
a value on one trigger and read it on another, and a scan of every process's dictionary
for `{'@rule_engine', _}` keys. They are pinned as tests: the answers in
`crates/mqtt-rules/src/tests_proc_dict.rs`, the process each trigger works on in
`crates/mqttd/tests/rules_proc_dict.rs`.

**What D13's table says holds.** `proc_dict_put` and `proc_dict_del` answer the value
held before or `undefined`, `proc_dict_get` the value or `undefined`. A key is any value
matched exactly, as T1 found for the table (`1`, `1.0` and `'1'` are three keys, `0.0`
and `-0.0` two, a map is one key whatever its order, `undefined` and `null` are keys).
The dictionary and the `kv_store` table share nothing. A call runs when evaluation
reaches it, in the order T1 pinned, and nothing undoes it.

**Whose dictionary, per trigger.** The process is `emqx_socket_connection`, the client's
connection, in every case:

| Trigger | The process in EMQX | The task in mqttd | Same |
|---|---|---|---|
| A client's publish; a later rule on it | the publisher's connection | the publisher's connection task | yes |
| A message a rule republished, re-entering the rules | the same process | the same task | yes |
| `client_connected`, `client_connack`, `check_authn_complete`, `check_authz_complete`, `session_subscribed`, `session_unsubscribed` | the client's connection | its connection task | yes |
| `client_disconnected` | the client's connection, as it ends: it still sees the dictionary | its connection task, before it ends | yes |
| `message_delivered`, `message_acked` | the subscriber's connection | the subscriber's connection task | yes |
| `message_dropped` for a client's publish | the publisher's connection | the publisher's connection task (the notes task once it has gone) | yes, while it is connected |
| `message_dropped` for a message a rule republished, or for a Will | the process that published it: the client's | the notes task | **no: empty** |
| `delivery_dropped` | the subscriber's connection process, also when its session has no connection | the subscriber's connection task; the notes task for a session without one | **no for a session without a connection: empty** |
| A Will | the connection's process as it ends: it sees the dictionary | the hub, or a task of the Will's own | **no: empty** |
| The SQL test | the caller's: a process per request over the REST API | a scratch dictionary per dry run | yes |

Two publishes of one connection see each other's values; another client does not; the
same client after a DISCONNECT and a new CONNECT does not; a connection that takes the
client id over does not; a resumed session does not.

**Where the probes corrected the record**

- **"The values live as long as the connection."** They live as long as the process.
  For a session that outlives its connection (`clean_start = false`, a session expiry
  above 0) EMQX keeps the process after the socket closes, and with it the dictionary:
  a `delivery_dropped` raised for the offline session read what the closed connection
  had put. The process, and the dictionary, end when the session is resumed (the new
  connection is a new process and starts empty), taken over or expired. D13 now says
  so. mqttd's dictionary still ends with the connection: nothing can read it
  afterwards but those events, which are evaluated by the notes task on an empty one.
  That is the second listed difference, now stated as what it is.
- **`message_dropped` is evaluated by whoever published the message**, not by a session.
  For a message a rule republished, or a Will, that is still the client's process in
  EMQX; mqttd hands that note to the notes task. D13's second difference named "events
  about a client that has no connection"; these are events about a message with no
  client of its own, with the same answer (an empty dictionary), and RULES.md lists
  them.
- **The dry run is not a difference for the dictionary** where EMQX's SQL test is called
  over its API: each request is a process, and nothing carries from one to the next.
  It is one only against a test run from a shell that stays.

**What the record left open and T11 settled**

- **`undefined` as a value.** EMQX stores it (the key is in the dictionary), and no
  function a rule can call tells that key from one that is not there. mqttd answers
  the same and holds nothing for it, so putting `undefined` frees a key rather than
  using one of the 64.
- **The bounds, exactly.** 64 keys; 64 KiB for the keys and values together, each as
  the rule engine writes it (a string as its bytes, anything else as JSON text). A key
  has no bound of its own. A put past either fails the rule (`dictionary_full`) and
  leaves what was held; reading or deleting a key too large to be held answers
  `undefined`, as EMQX would.
- **The ceiling.** A dictionary's bytes count in `rules.state_max_bytes` with the
  namespaces', in no namespace: a put the store has no room for fails the rule
  (`store_full`), and a dictionary does not grow in a memory brownout (`brownout`). The
  store counts those refusals; T6 exports them. A rules file that calls `proc_dict_*`
  therefore loads only where there is a store, like any state function.
- **The per-message bounds apply**: a call is one of the message's 1,024 state calls,
  and what it reads is charged to the build budget, so a `FOREACH` cannot multiply a
  64 KiB value into a large message.
- **Where it lives.** In the connection's own view of the rules, beside the rule set it
  evaluates against and behind the lock that connection's evaluations already take,
  which nothing else contends for: no lock was added, and no other task can reach it.
  Empty, it is a few words and no allocation. A Will's evaluation makes one and drops
  it; the notes task clears its own after every note; a dry run makes one per request.
- **A wrong argument count** is a load error, as for every function, where EMQX fails
  the rule on each message.

### 2026-10-11 — T2: the merges as built, and what a state of bounded size cannot do

T2 built D4 and D6 as data types, a merge and a clock
(`crates/mqtt-rules/src/crdt.rs`), proven on one machine; nothing is sent between nodes
yet (T3). The merge is commutative, associative and idempotent over every state the
decoder accepts, not only over states a node can reach, because a peer can send any
bytes. Property tests check it over every type, every pair of concurrent operations and
300 random interleavings on three nodes whose wall clocks disagree; a fuzz target
(`state_merge`) checks it over arbitrary bytes and arbitrary scripts.

**Where D4 could not be built as written**

- **A delete "covers every write stamped before it" exactly only for `lww`.** For
  `first`, D4 asks for "the earliest write that no delete covers". A node that holds two
  writes of a key cannot know which of them a delete it has not heard of yet will leave,
  so it would have to keep both, and every write after them: no state of bounded size
  computes it, and a merge that guesses is not associative (a write at 1, a write at 3
  and a delete at 2 give different answers by the order they meet in). The same holds
  for `max` and `min` (the largest of the writes a later delete leaves) and for a
  counter's increments. What is built: a key's state is **the latest delete it has seen
  and what was written by nodes that had seen that delete**. Of two states, the one with
  the later delete replaces the other's content; two with the same delete join by type.
  So for `first`, `max`, `min`, `sum`, `union` and `hll` a delete covers every write
  stamped before it (a node that had seen the delete stamps after it) **and also the
  writes of nodes that had not heard of it yet**, even when the clock put those after
  it; a write made after hearing of the delete is never covered. `lww` is one stamped
  write and needs no such rule: it survives every delete stamped before it, whoever had
  seen what, which is D4 to the letter.
- **`first`'s expiry is the later of the two,** as for the accumulating types, not "with
  its write": the write happens once, and `kv_held` and `kv_set_once` set the expiry at
  every call, so an expiry that travelled with the write would never be refreshed on
  another node. `lww`'s travels with its write.
- **An expiry found by a write is a delete.** A write that finds its key expired stamps
  a delete first and then writes, so the old content cannot come back from a node that
  still holds it (a counter does not resume at its old count).
- **For every type but `lww`, a shortened TTL does not travel.** On one node a call
  with a TTL sets the expiry, sooner or later than it was (D3). Between nodes the later
  expiry wins, so an expiry moved sooner on one node is moved back by a copy that still
  has the later one. Refreshing a key, which is what the windows and `kv_held` do, only
  ever moves it later.

**What the record left open and T2 settled**

- **Two types on one key.** D3's `wrong_type` is one node's answer. Two nodes can create
  a key at once with different functions: a surviving `lww` write wins, and between the
  other types the one later in D4's table (`first`, `max`, `min`, `sum`, `union`, `hll`).
  Every node then refuses the other type's calls alike.
- **The clock (D6).** 48 bits of milliseconds and a 16-bit counter in one word, taken
  with one compare-and-swap; the counter carries into the milliseconds after 65,536
  stamps of one millisecond. When the wall clock stands still or steps back the counter
  carries on, and what expiries are judged by (the clock's physical part) never goes
  back. A stamp adds a 64-bit tag of the node (a fixed hash of its id), which only breaks
  ties; two ids with one tag would make a tie fall to the values, still the same way on
  every node. **A stamp more than 60 seconds ahead of the receiving node's wall clock is
  refused:** the state that carries it is not merged and does not move the receiver's
  clock, and it is accepted once the wall clock has caught up (T3: counted in
  `mqttd_rule_state_replication_refused_total{reason="clock_skew"}`, retried by repair).
  Without a bound one node with its clock a year ahead would stamp every other node's
  writes a year ahead; with it, such a node's writes are not seen elsewhere until its
  clock is fixed and it is restarted (the clock is in memory). 60 seconds is far above
  what NTP leaves and far below what a wrong time zone or a dead clock battery gives.
- **A `local` namespace is not stamped.** Its keys are never merged with another node's
  and it can never become `eventual` (D1), so its writes take no clock and the publish
  path pays nothing for one: D6's "expiries are absolute times on the clock" applies to
  replicated namespaces, and a `local` one keeps the wall clock, as T1 built it.
- **A counter** keeps, per node, how much it added and how much it took away, each in
  128 bits. A node's increment is still refused where the count would pass 64 bits (T1),
  so one node behaves as before; the merged count of several nodes can pass 64 bits and
  is then read exactly, as an integer of that size. A counter keeps counts of at most
  256 node ids (D9 said one pair per node id that ever incremented it, which nothing
  bounded), each id at most 255 bytes; past that a merge keeps the first in order of id
  and a new node's increment is refused.
- **A set** merged past 1,024 elements keeps the first 1,024 in its order. That order had
  to become total: Erlang's term order leaves two maps of one size undecided, and T1
  kept such elements in the order they arrived in, which two nodes would not share. They
  are now ordered by their identity. This is the one change to what T1 answered on a
  single node, and only for a set that holds maps of equal size.
- **`max` and `min`** of `1` and `1.0` (one number, two values). On one node the number
  held stays when an equal one is offered, as T1 pinned it: a write replaces only what
  it is strictly beyond. When two nodes' copies merge, both keep the integer. So which of
  two equal numbers a node holds can change at a merge, and never changes back.
- **The sketch.** A value is hashed by its exact identity (as a set tells elements
  apart) with SipHash-2-4 under a fixed key, written by hand and pinned by the reference
  vector and by fixed outputs: no node, architecture or release may hash differently,
  since sketches merge register by register. The upper 14 bits choose the register.
  Registers are listed (four bytes each in memory) while at most 3,072 are set and
  become a 12 KiB array after, so a sketch never takes more than 12 KiB. The estimate is
  Ertl's improved estimator (2017), which needs no correction tables; it uses only
  additions, halvings and square roots of doubles, so every node computes the same
  number. Measured error over eight sizes from 1,000 to 300,000: at most 0.6%.
  `kv_get` reads a sketch as its estimate.
- **The replicated form.** A version byte, the delete's stamp, the expiry, a type tag,
  then the type's content; values as ADR 0086 D3 has them. At most 265,246 bytes, which
  is a full set of the largest elements; a counter of 256 nodes is at most 75 KiB, a
  sketch 12 KiB, a value 4 KiB. The decoder accepts exactly what the encoder writes (one
  encoding per state) and nothing longer, and allocates no more than its input holds.
- **What D3's form cannot carry.** It has no `-0.0` (written `0.0`), no bytes that are
  not UTF-8 inside an array or a map (written with U+FFFD; a binary on its own keeps its
  bytes), and no `undefined` inside one (written as the text). A replicated key
  therefore holds a value as it reads back from that form, settled on the node that
  writes, so every node holds the same value. T3 applies it in `eventual` namespaces
  only; `local` ones keep values as they are.
- **Forgetting.** A key that holds nothing (deleted, or expired) is forgotten once its
  delete is older than the namespace's tombstone window. Tombstones are therefore
  bounded by the deletes of one window, and by `max_keys`, which they count toward (D9).
- **Where it lives.** In `mqtt-rules`, which still holds no state and does no I/O: these
  are types and functions. The broker's store keeps a key in them from now on.

**Left to T3, with what T2 measured for it.** Stamping writes in an `eventual` namespace
(a stamp is 4 ns alone and 0.2 to 1 µs with eight threads taking them at once,
all on one word: a clock per shard would give the same order per key without the shared
word, if that shows on the rig); storing values as they read back from the replicated
form; whether `kv_del` of a key the node has never seen leaves a tombstone (the merge
needs one to cover a concurrent write elsewhere; the bounds may refuse it); the refusal
count for clock skew.

### 2026-10-11 — admin features and confirmed defaults

Two things the user decided on 2026-10-11, after acceptance.

**Confirmed defaults.** Of the five choices decided by default, the user confirmed two:
**state is on without a switch** (using it is the opt-in, under `rules.state_max_bytes`
= 256 MiB; `0` turns it off), and **the names stay `kv_*`** (asked, the user requested no
rename). The other three stand as they were: `eventual` replicates to every member,
`strong` is phase 2, and `proc_dict_*` are provided, which T11 has since delivered
(#933). The [table](#decided-by-default-2026-10-11) carries each one's standing.

**Admin features: decided.** The user asked for three things D10 does not give: to view
the contents of the store, to expire stale entries by hand, and to change the expiry of
stored keys. *(The same day the user dropped selecting keys by staleness and the
last-write time it needed: see the amendment "no last-write time" below. The text here
is edited to what is built; that amendment records what was taken out.)* D10 stays as written and T6 builds it (the namespaces, the keys by prefix,
one key's value, one key's delete, the verbs, the metrics, `$SYS`). Four tasks add the
rest: T12 (view), T13 (purge), T14 (change expiry), T15 (their verbs and documentation).
What follows is decided unless it says "by default".

**The six points the user settled**

1. **A manual purge raises no `$events/state/expired`** unless the call asks for it
   (`events=true`). An operator clearing out a namespace does not want every "silent for
   10 minutes" rule to fire for keys that did not fall silent.
2. ~~"Stale" means not written for a given time, judged by a last-write time kept per
   key.~~ **Withdrawn by the user the same day:** no last-write time is kept and no
   selector asks for one. Entries are expired by hand by prefix, by "already expired"
   and by "expires within" (T13).
3. **EMQX's `kv_store` table can be viewed and purged, and its keys cannot be given an
   expiry.** What a rule sees of that table stays EMQX's: a key is there until a rule or
   an operator deletes it.
4. **The cluster:** in an `eventual` namespace a purge or a change of expiry replicates
   as any write does; in a `local` namespace, and in `kv_store`, it acts on the node that
   was asked (the CLI's `--all-nodes` asks each).
5. **Roles:** a purge and a change of expiry need the operator role **and** a listed
   rules writer, as D10's delete does. Viewing, a dry run included, needs the operator
   role, as D10's listing does.
6. **Bounds:** a call changes at most 100,000 keys and answers with a cursor to go on
   from.

**No last-write time per key.** The plan approved here first added one to every entry (8
bytes a key on each node, not replicated) to answer "not written for". The user
withdrew it; the store's entry and `ENTRY_OVERHEAD` (128 bytes) stay as T1 and T2 built
them, nothing is added to a write call, and no answer carries a write time.

**T12. View the contents.** `GET /admin/v1/rules/state/keys` (D10) answers, per key,
`type` (D4's name), `bytes` (what the key is accounted as, D9) and `expires_at` (absent
for none). `values=true` adds each value as D10's read of one key shows it; a page then
ends at 1,000 keys or 1 MiB of values, whichever comes first, and always holds at least
one key. Two filters, which combine with each other and with `prefix`:
`expires_within=<duration>` ("expires before") and `no_expiry=true`; durations as the
rules file writes them. Everything shown and selected is in the entry already. The
reserved `kv_store` namespace is listed like any other (its keys are any value: they are
shown as D10 shows them, and it is where `no_expiry` finds every key). Tombstones are
not listed; they are in the namespace's counts. A filter costs a walk of the namespace
in batches, a shard's lock for at most 64 keys at a time as the store removes expired
keys today, on a blocking task, never on a connection's. Operator role; values are shown
to operators only, as before.

**T13. Expire entries manually.** `POST /admin/v1/rules/state/purge?ns=…` removes the
keys of one namespace that a selection names: the listing's `prefix`,
`expires_within` and `no_expiry`, and `expired=true` (keys past their expiry that the
sweep has not removed yet), combined when more than one is given. A call that names none
is refused. There is no selector for keys nothing has written for a time; a key written
with a TTL that nothing refreshes expires by itself, and `expires_within` reaches it
sooner.

- **`dry_run=true`** changes nothing and answers the count (up to the cap) and a sample
  of 20 keys as the listing shows them, without values.
- **Bounded and off the publish path.** At most 100,000 keys a call, then a `cursor`;
  one purge at a time per node (a second is refused as busy); it runs on a task of the
  store's, taking a shard's lock for at most 64 keys at a time, so a rule's call waits
  for one batch at the most.
- **Audited** as `state.purge`: the subject, the namespace, the selection as given, the
  count removed, whether events were asked for. A dry run is not a change and is not
  audited.
- **What a purge is in each mode.** In a `local` namespace and in `kv_store`, the keys
  are removed on this node. In an `eventual` namespace each is a replicated delete, with
  what D4 and the T2 amendment say of a delete: it leaves a tombstone, which counts
  toward `max_keys` for the namespace's tombstone window (so a purge frees bytes at once
  and key room an hour later, by default), and for every type but `lww` it also covers
  writes made on nodes that had not heard of it yet.
- **Events.** None by default (point 1): purging `expired=true` keys is also how an
  operator drops a backlog of expiry events on purpose. With `events=true` each purged
  key raises `$events/state/expired` as if it had expired at the purge: the key reads as
  absent at once and its event is handed over by D7's sweep at D7's rate, late rather
  than lost, on the node that ran the purge (the other members see a delete and raise
  nothing). `kv_store` raises none (D7), so the flag is refused there. This half needs
  T4.

**T14. Change expiry on stored keys.** `POST /admin/v1/rules/state/expiry?ns=…` with
`key=` for one key, or T13's selection for many, and one of `ttl=<duration>` (expires
that long from now), `extend=<duration>` (that much later than it would have) or
`clear=true` (never). `dry_run`, the cap, the cursor, the task and the roles are T13's;
audited as `state.expire` with the selection, the change and the counts.

- **Within `max_ttl`:** a `ttl` or an `extend` that would put the expiry further than
  the namespace's `max_ttl` from now is refused for one key and skipped and counted
  (`past_max_ttl`) in a selection. `clear` is the exception, and is the only way a key
  outside `kv_store` comes to have no expiry *(decided by default, 8)*.
- **`kv_store`:** refused (point 3).
- **It lasts until the next write that names a TTL.** D3: a write with a TTL sets the
  expiry. A key a rule refreshes on every message (the windows, `kv_held`, "silent
  for X") has the operator's expiry until the next message.
- **A key that then expires, expires as any key:** D7's sweep removes it and raises its
  event. Where moving an expiry sooner is possible, that is how an operator ends keys
  early *with* their events, without the purge's flag.

What an `eventual` namespace can carry of it follows from how an expiry merges (D4, the
T2 amendment: with its write for `lww`, as the later of the two for every other type,
"never" being the latest):

| | `local` (this node) | `eventual`, `lww` | `eventual`, any other type |
|---|---|---|---|
| Later (`extend`, a later `ttl`) | exact | replicates | replicates: the later expiry wins |
| `clear` | exact | replicates | replicates: "never" is the latest |
| Sooner (an earlier `ttl`) | exact | replicates | **does not replicate: refused** |

- **`lww`:** the change is made as a write of the value the node holds, under a new
  stamp, with the new expiry. It wins over every earlier write, so it replicates in
  both directions. It is a write in every other respect too: it also wins over a write
  another node made just before that this node had not received, whose value is then
  lost as it would be to any `kv_set`.
- **Every other type, sooner:** every other copy still has the later expiry, and the
  merge takes the later, on the node that was asked as well, at the next repair. The
  change would hold on one node until then and nowhere after. So it is **refused**
  (`not_shortened`: an error for one key, skipped and counted in a selection) *(decided
  by default, 7)*, and the answer names what does work: purge the keys (T13).
- **Why not a delete at the new time.** It is not an expiry. A delete raises no
  `$events/state/expired`, which is the point of ending a "silent for X" key; it cannot
  be taken back by the rule's next refresh, where an expiry is moved by it; it would
  cover writes made on nodes that had not heard of it; and something would have to hold
  the pending delete, on one node, until its time.
- **The other answer: delete and write back.** A node can stamp a delete and write its
  copy of the content back after it with the shorter expiry: the state with the later
  delete replaces the others' content, expiry included, so the shorter expiry does
  replicate. The price is the T2 amendment's: that delete covers writes made on nodes
  that had not heard of it, so a counter loses increments in flight, a set elements, and
  `first` gets a new write time. It is exact only for a key nothing is writing. Not
  built unless the user asks for it; if they do, it is a flag on T14's call, off by
  default.

**T15. CLI and documentation.** In ADR 0084 D7's style, beside D10's verbs:
`state-keys <ns>` gains `--values`, `--expires-within` and `--no-expiry`; `mqttd --admin state-purge <ns>` and `mqttd --admin state-expire <ns>
[<key>]` (named as Redis names `EXPIRE`: it sets when a key expires, it does not expire
it now) take the selection and `--dry-run`, and for a selection past the cap go on from
the cursor only with `--all`. Control characters are printed escaped, as everywhere.
ADMIN-API.md documents the endpoints; RULES.md gains a section on operating the store
(what each selector means, what a purge does and does not raise, the table above);
THREAT-MODEL.md's row for a rules writer's reach gains the purge. **T8 is not changed
and does not wait for these:** it closes phase 1's documentation, and T15 carries the
documentation of T12 to T14, so the feature's tracker (#880) closes with T8 as planned.

**Order.** T12 follows T6 (the listing, its cursor, the roles). T13 and T14 follow T12
(the selection); their halves for `eventual` namespaces follow
T3, and the purge's `events=true` follows T4. T15 follows T13 and T14.

### 2026-10-11 — no last-write time: selecting stale keys is dropped

The amendment above planned a last-write time per key, so that the listing, the purge and
the change of expiry could select keys "not written for" a given time. The user, the same
day, before any of T12 to T15 was built:

> ok, drop the stale key idea completely. drop the extra bytes

So it is withdrawn, and the amendment above and the plan are edited to match. Nothing had
been built on it.

**Taken out**

- **The last-write time itself.** No field is added to an entry, in any namespace: no 8
  bytes a key, no store on a write call, nothing set when a merge changes a key, and an
  operator's change of expiry counts as nothing beyond the change. `ENTRY_OVERHEAD` stays
  128 bytes and T12 no longer measures it.
- **`written_at`** in the listing, and **`written_known_since`** in every answer.
- **The selector** `not_written_for` ("not written since") in the listing (T12), the
  purge (T13) and the change of expiry (T14), and `--not-written-for` in the CLI (T15).
- **Decision 6** of the [table](#decided-by-default-2026-10-11) (the time is each node's
  own and is not replicated). Its two consequences go with it: that a node refilled after
  a restart knows no write time before its refill, and that a write on another node which
  changed neither value nor expiry is not seen.
- **Point 2** of the six the user settled ("stale" means not written for a given time).

**What stays**

- **T12:** per key `type`, `bytes` and `expires_at`, values with `values=true`; the
  filters `prefix`, `expires_within` and `no_expiry`; `kv_store` listed like any other.
  All of it is in the entry today, so T12 changes nothing on the publish path.
- **T13:** a purge by `prefix`, `expires_within`, `no_expiry` and `expired=true`,
  combined when more than one is given; the dry run, the cap and cursor, the audit
  record, the roles, `events=true`, what a purge is in each mode.
- **T14:** unchanged but for its selection, which is T13's. Decisions 7 and 8 stand by
  default as they were.
- **T15:** the verbs without `--not-written-for`; RULES.md says what each selector means.

**"Expire stale entries by hand", as it is now served.** The user's request stands; what
goes is one way of naming the entries. An operator removes the keys of devices that are
gone, or of a rule's earlier key scheme, by `prefix`; drops keys that have expired and
not been swept yet by `expired=true`; and removes keys about to expire by
`expires_within`. A key a rule writes with a TTL and then stops writing expires by
itself, which is the store's own answer to staleness. What cannot be asked any more is
"the keys nothing has written for a month" among keys that never expire (`kv_store`, or
a key whose expiry was cleared): those are found by `no_expiry` and chosen by prefix.

### 2026-10-11 — T7: `--sequence` as built

D11 decided that `mqttd --rule-test` replays "a timed list of messages and events …
against an in-memory store with a clock the sequence drives", and left the file's format,
the clock and the report open. T7 built it (`crates/mqttd/src/rule_replay.rs`); this is
what it settled.

**The file.** One JSON object per line; blank lines and lines that start with `#` are
skipped; `--sequence -` reads standard input.

| Key | |
|---|---|
| `at` | the step's time from the sequence's start: a number of seconds (fractions allowed), or text with a unit (`"250ms"`, `"90s"`, `"5m"`, `"1h"`, `"2d"`). Never before the step before it: a sequence's clock does not go back |
| `after` | instead of `at`: how long after the step before it. With neither, the step happens when the one before it did (the first one at the start) |
| `topic`, `payload`, `qos`, `retain`, `clientid`, `username` | the publish. A `payload` that is a JSON string is those bytes; any other JSON value is its text exactly as the line has it. What a line leaves out is what `--rule-test`'s options of the same names say, or their defaults |
| `event` | instead of a publish, a sample of that event, as `--rule-test --event` builds it |

A line with no `topic`, no `payload` and no `event` sends nothing and only moves the clock
(a step in which keys expire). An unknown key is an error, as everywhere. Bounds: 10,000
steps, 1 MiB a line, a hundred years from the start. An error names the line, and a
sequence that cannot be read is a usage error (exit 2): nothing is replayed.

**The clock.** A sequence starts at 2025-01-01T00:00:00Z, and a step happens at that time
plus its `at`. Everything that reads time reads the step's: the store (TTLs, `kv_held`,
`kv_ttl`), `now_timestamp()`, `now_rfc3339()`, a `format_date` without a time,
`timestamp` and `publish_received_at`; a message's `id` counts from 1. Nothing reads the
wall clock or draws a random number, so a replay prints the same thing every time. D6's
"`now_timestamp()` is the wall clock read at each call" stands for a running broker: the
engine reads a replay's clock only from an input that carries one (`Input::clock_ms`),
and no input of a running broker does. The per-publish benchmark did not move (below).

**What a replay is.** One store, empty at the start and dropped at the end, under the
configured ceiling; never the store the rules of a running broker hold (D11), whichever
store the functions it is given carry. One dictionary per client id in the sequence,
counted against that store as a connection's is: a sequence has no connections, so the
steps of one client id are taken as one connection of it. The rules run as the broker
runs them on a connection's task: every enabled rule whose `FROM` selects the trigger, in
id order; a republished message re-enters on the same dictionary and within the same
per-message budget, depth first; state calls in EMQX's order. With `--sql` it is the
statement only, as `--rule-test` has always been (no actions); without, the configured
rules file with its actions and its `[state.*]` tables. One node: a namespace that is not
`local` has nobody to replicate to and behaves as `local`, which the report notes.

**The report.** Per step: its number, its time, what was sent; then, for a statement, its
outputs as `--rule-test` prints them, or `(no output)`, `(not selected: <why>)` or
`FAILED: <why>`; for a rules file, each rule that ran with `passed`, `no_result` or
`FAILED: <why>` and under it what each action rendered (`republish <topic> qos <n>
[retain] <payload>`, `console <output>`, `action FAILED: <why>`), then each re-entering
message under a `>` line. Then what the step changed in the store, a key a line, by
namespace and key: `+` new, `~` a changed value or expiry, `-` gone, with the value as
`kv_get` reads it, the key's type and its expiry. `--show-state` adds the store after the
last step. A failing step does not stop the replay (what it wrote stays, as in the
broker). Exit 1 when a step had a rule or an action fail, or when nothing in the sequence
was selected at all; 0 otherwise.

**Expiry, and what waits for T4.** A key past its expiry reads as absent at once (T1),
and the first step after it reports it as `- <key> (expired <when>)`, in order of expiry.
D7's event does not exist yet, so a replay raises none: "expiry events fire in the
replay" is T4's to finish, where the replay takes the expired keys out of what it shows
(`rule_replay::expire`): for each, at its expiry time and before the step's own message,
evaluate the rules that select `$events/state/expired` and report what they produced.

**The same replay is a library call** (`mqttd::rule_replay`: `parse_sequence`, `Replay`,
`Report`), so tests assert sequences without the binary. The worked examples are pinned
that way (`crates/mqttd/tests/rules_sequence.rs`: "hot for N seconds", a quota in a
window, change detection, an EMQX `kv_store_*` counter), and RULES.md's two transcripts
are run by `rules_docs.rs` against the binary.

**Also settled.** `--rule-test` accepts `--config`, as `--check-rules` does: it has always
read the effective configuration (for `rules.jq` and the modules of `rules.file`), and a
replay of the rules file needs to be told which. A `kv_store_*` key is shown as the value
it is (`1`, `1.0` and `"1"` are three keys), read back from the store's exact form.

**Measured** (`cargo bench -p mqtt-rules`, Apple M1 Pro, `main`'s binary and this change's
run in turn, six rounds, the fastest median of each case; the machine was shared):
`no_match` 113.3 to 114.0 ns, `filter_reject` 987.8 to 984.6 ns, `filter_republish`
1.8335 to 1.8328 µs, `select_star_republish` 4.597 to 4.594 µs, `foreach_10` 8.462 to
8.467 µs. Between −0.3% and +0.6%, inside the spread from one run to the next.

### 2026-10-11 — T3: `eventual` as built, and what D15 left to decide

T3 built D5's `eventual` mode and D15: every member holds the namespace, what changed is
sent per peer link, and copies are compared and repaired
(`crates/mqttd/src/rule_state/replica.rs` and `link.rs`, three frames at peer protocol 14
in `crates/mqtt-cluster/src/peer.rs`). Nothing in D5 or D15 was contradicted. D15 named
its mechanisms without fixing them, and building it met four things the record did not
foresee. They come first.

**What the record did not foresee**

- **A node that restarts forgets what it had counted, and the others do not.** A counter
  keeps, per node, how much that node added, and two copies merge by keeping the larger
  of each node's numbers (D4). The store is in memory (D5), so a restarted node starts
  its own number again at zero under the same node id, and whatever it counts before it
  has been refilled is smaller than the number the others still hold for it: the merge
  keeps theirs and the new increments are lost, silently. No rule on the restarted node
  can avoid it (it would have to wait for a refill it cannot see), and no merge of
  bounded size can tell the two apart afterwards. So **a node counts under a name that
  is its id and when this run of it began** (`<node id>#<milliseconds, in hex>`): each
  run has numbers of its own, and what an earlier run counted stays counted. The price:
  a counter gains a row for each run of each node that increments it, and T2's bound of
  256 rows is then reached by restarts, not only by nodes. A counter with a TTL goes
  with its key long before that. One that is refreshed for ever and lives through 256
  node restarts refuses the next run's increment (the rule fails, counted) until the key
  is deleted. Rows of runs that are over could be folded together only by every member
  at once, which is a coordinated step this record does not have; it is left open.
  *(Decided by default, 9: counts are kept exact across restarts, and the bound is the
  price. The other answer counts under the node id alone: no growth, and a
  restarted node loses what it counted before its refill arrived.)*
- **A delete of a key this node holds nothing of does nothing** (the question T2 left).
  `kv_del` answers `false` and leaves no tombstone, and so does `kv_held` on a false
  condition, which is such a delete for every reading of every device that is not hot:
  a tombstone each would fill the namespace with keys nobody wrote and send one to
  every member per message. What it gives up: a write of the same key made elsewhere
  and not yet arrived is not covered by that delete, and survives. A key this node has
  seen deleted is not deleted again either, so a delete is sent once.
- **A write that leaves the key as it was is not sent.** A value that is not the largest,
  an element the set holds, a value the sketch has seen, a second `kv_set_once`: on a
  busy namespace most calls of `kv_max`, `kv_add` and `kv_hll_add` are these, and each
  would otherwise send its key's whole state (12 KiB for a sketch) to every member.
  `KeyState::apply` reports whether the state changed; only then does the key take a
  new place in the order the links read.
- **A node that joins has a link to every member, and each would send it everything.**
  Repair is between two nodes (D15), so a restarted node in a cluster of N would be sent
  the namespace N−1 times. A node therefore repairs a namespace over one link at a
  time; a comparison another link's peer asks for meanwhile is kept and started when
  the first is over, and by then finds little or nothing to send.

**Sending (D15's "a task per peer link")**

- **No queue, and so no bound to hit.** A shard numbers its keys' local changes; a key
  has one place in that order, its latest change. A link keeps a position per shard and
  reads on from it, at most 64 KiB of keys and states a frame (one key's state can be
  larger, up to 265 KiB, and then travels alone). The store keeps nothing per link, so
  what waits for a link is its distance from the end of the order: at most the keys
  there are, whatever was written and however long the peer has been slow or away. The
  task asked what happens when a link's backlog bound is hit: there is no backlog that
  can pass a bound, and nothing is ever dropped for a slow peer. A link that is gone
  costs nothing either; when it comes back it starts at the end of the order and
  repair finds what it missed.
- **A key that no link has read past keeps its place** when it is written again, so the
  hot path of a key written faster than the links read is the write alone.
- **The link's task is the peer link's own** (`peer.rs`'s pump), with state as its
  lowest lane: it writes a state frame when nothing else is waiting, and one after each
  batch of the bulk lane when something always is. While it waits on a full socket
  nothing accumulates behind it.
- **Waking it costs a write nothing most of the time.** The first write after a link
  went to sleep wakes the links; the writes after it read one flag. A link that has sent
  everything waits 5 ms before it looks again, so a burst leaves as one frame.
- **A state merged in from a peer is not sent on.** Every member has a link to the node
  that wrote it. Where two members have none (a partial partition), repair through a
  member they both reach carries it, within the repair interval.
- **Receiving** is as D15 says: the link's reader applies each key to the store. A state
  frame is never a hub command (ADR 0082), which a test pins by counting what the hub is
  sent while 10,000 keys cross.

**Repair (D15's "compare per-namespace digests"), as chosen**

- **The digest.** A namespace is summarised in 1,024 parts. A key's part is the top ten
  bits of the fixed hash of the key (SipHash-2-4 under T2's fixed key), so every node
  puts a key in the same part whatever its own shards are. A part's number is the
  exclusive-or of its keys' hashes, a key's hash being that of its key and its encoded
  state (one encoding per state, T2); the root is a hash over the parts. Keys that
  could be forgotten are not in it, whether or not they have been removed yet, so two
  nodes that forget at slightly different moments still agree.
- **The exchange.** When a link comes up and every 30 seconds, each side sends its root
  (`StateDigest` with no parts). Of the two, the node whose id sorts first answers a
  root that is not its own with its parts (8 KiB); the other compares, asks for the
  parts that differ (`StateWant`, a bitmap) and sends its own keys of those parts, and
  the first then sends its own. Each side lists the keys it will send when it starts,
  so it does not send back what the other is sending it, and says it has sent them by
  sending its root once more; a side that hears nothing for 5 seconds takes the repair
  to be over. Two copies that agree exchange a few bytes; two that differ in a few keys
  exchange the digest and the keys of the parts those are in, about a thousandth of the
  namespace each.
- **What it does not bound well.** The digest is made by looking at every key once, a
  shard at a time, on the link's task, when something changed since the last one
  (its cost is inside the catch-up measured below, and was not measured alone). A namespace run at its bound is offered, every interval, the keys it
  refused (D15 accepts that such a namespace differs between nodes; the cost is the
  parts those keys are in, every 30 seconds).

**The wire**

- Three frames, appended (`PeerMessage` tags 31 to 33, bytes pinned by a test), sent
  only on a link that negotiated 14 and only for namespaces the sender's rules declare
  `eventual`: `StateKeys { namespace, declaration, entries: [(key, state)] }`,
  `StateDigest { namespace, declaration, root, parts }` and
  `StateWant { namespace, declaration, parts }`. A state is T2's replicated form.
- **Mixed versions.** A link to a node on protocol 13 has no state lane at all: that
  node is sent nothing and holds nothing, and its rules file cannot declare the
  namespace (a release with T1 and T2 refuses `eventual` at load; one before T1 refuses
  `[state.*]`). The members on 14 replicate among themselves. Upgrade every node, then
  deploy the rules. The two-binary rolling upgrade (`cluster_upgrade.rs`) declares no
  such namespace and its links carry what they carried before, byte for byte; a test of
  the link itself pins that a peer announcing 13 is sent no state frame in ten minutes
  of link time while one announcing 14 is sent the digest at once.
- **The declaration's fingerprint** covers the mode, the form a state travels in and the
  tombstone window (two nodes that forget deletes at different times would bring each
  other's deleted keys back). The bounds are not in it: D15 lets a node hold fewer keys.
  A frame whose fingerprint is not this node's is refused whole
  (`reason="declaration"`). One for a namespace this node does not declare `eventual`
  at all has no namespace of the operator's to be counted under: it is counted for the
  store, exported as `{namespace="(undeclared)", reason="undeclared"}`.
- **`tombstone_window`** is a key of `[state.<name>]` (D9 named the window and its
  default, an hour; one second to 30 days), allowed in an `eventual` namespace only.

**Bounds and refusals (D9, D15's "a node at a bound")**

- **A merge keeps to this node's own declaration**, the one its rules loaded last. A
  key that is new here needs room under `max_keys`, `max_bytes` and the store's ceiling
  (keys that can be forgotten make room first) and is refused in a brownout. **A key
  that would grow needs the bytes too**: a rule's write to a key that exists is bounded
  per key and never refused for room (T1), but a peer is not a rule, and without this a
  peer could raise every key this node holds to its largest. What is refused is counted
  (`namespace_full`, `store_full`, `brownout`) and **what this node held is kept as it
  was**; the peers offer it again at the next repair.
- **Malformed** (`malformed`): a key that is empty or longer than 256 bytes, a state
  T2's decoder refuses, a digest that has neither no parts nor 1,024, a request that is
  not 16 words. Each key of a frame is judged on its own, and the good ones are taken.
  A frame is bounded by the peer bus (16 MiB), a state by T2 (265 KiB), and decoding
  allocates no more than the frame holds. A test feeds a bounded namespace 3,000 frames
  of random and of damaged states and checks its bounds after every one; the peer bus's
  fuzz target (`peer_decode`, nightly) has seeds for the three frames.
- **Clock skew** (`clock_skew`): T2's 60 seconds, applied to the states that carry a
  stamp: an `lww` or `first` value, and anything that has seen a delete. A counter, a
  set, a sketch or an extreme that was never deleted carries no stamp, so there is no
  clock in it to follow and it is merged; its expiry is still a time by its writer's
  clock, as D6 says of every duration between nodes.
- **`eviction = "lru"` with `consistency = "eventual"`** is a load error (D9 said why).
  **A namespace's mode cannot change** while rules hold it: the load, and so the reload,
  is refused (D1), at the store.
- **Accounting.** A key of an `eventual` namespace is accounted 96 bytes more than a
  `local` one (its delete's stamp, its hashes, its places in the orders of change and
  of forgetting), a tombstone as a key without a value, a counter 32 bytes and the
  node's name for each run beyond the first.

**What a rule sees**

- **Reads are local, and an `eventual` namespace answers a rule as a `local` one does**
  on one node: a test runs the same statements through both. Two things differ and
  neither changes an answer: a value is held as it reads back from the replicated form
  (T2), and a deleted key still counts toward `max_keys` until its window has passed.
- **`kv_changed` with a TTL on a value that did not change is a write** (a new stamp):
  an `lww` expiry travels with its write, so there is no other way to move it.
- **Expiry** is by the clock's physical part, the same moment on every node; the key
  reads as absent from then on. It is removed when a namespace needs room and whenever
  a digest is made, as are tombstones past their window. The sweep and the event are T4.

**Observability** (the three series D10 names for `eventual` namespaces; the rest is T6)

- `mqttd_rule_state_replication_pending{namespace}`: the keys the link furthest behind
  has yet to read. Counted exactly while a shard has fewer than 64 unread changes,
  taken as the shard's changed keys past that.
- `mqttd_rule_state_replication_lag_seconds{namespace}`: how long that link has had
  something to send, that is, the time since it last had sent everything.
- `mqttd_rule_state_replication_refused_total{namespace,reason}`, with the reasons
  above.

**Measured** (`cargo bench -p mqttd --bench rule_state`; the delivery doc has the run)

Apple M1 Pro, eight cores, the best of three rounds on a machine other work was sharing.

- **On the publish path** a write to an `eventual` namespace costs what one to a `local`
  namespace does, within the spread: a publish evaluated with one `kv_incr` 1.80 µs
  `local` and 1.84 µs `eventual`, with `kv_held` 1.91 and 1.94 µs, with `kv_hll_add`
  2.06 and 2.10 µs (1.42 µs with no state call).
- **In the store, one thread:** `kv_set` 0.15 µs `local`; 0.16 µs `eventual` on a node
  no link reads from, 0.30 µs with a link reading all the while (the key goes to the end
  of the order of change each time it has been sent). `kv_incr` 0.13, 0.16 and 0.27 µs.
  A read 0.15 µs `local` and 0.12 µs `eventual`. A `kv_hll_add` of a value the sketch
  has seen, 0.24 µs, sends nothing.
- **Every thread writing, a link reading,** each on keys of its own: 0.19, 0.38, 0.69 and
  0.92 µs a call on one, two, four and eight threads (`local`: 0.11, 0.14, 0.19 and
  0.34 µs). The difference is the order of change, which a write joins under its
  shard's lock and the link reads under the same lock. With every write stamped
  (`kv_set`): 0.19, 0.41, 0.86 and 1.65 µs, so **the one clock word does show** at
  eight threads writing nothing but stamped values; T2's clock per shard is the way
  out and was not taken, since a publish is two microseconds of other work. All
  threads on one key: 0.15, 0.35, 0.97 and 2.7 µs (`local`: 0.11, 0.18, 0.67 and
  1.9 µs).
- **On the wire, per key** (a 17-byte key, in a frame of 100): a counter one node counts
  65 bytes, a 44-byte value 105, a `kv_held` time 74, a deleted key 38, a set of ten
  client ids 175, a sketch of 1,000 values 2,970. One frame per link for whatever
  changed in the 5 ms before it.
- **Catching up:** a node that comes up empty beside one that holds 100,000 keys (a
  44-byte value each, 10.5 MB on the wire) holds them all 0.14 s after its link is
  dialled: two processes' worth of work in one process, over loopback TCP, each end the
  link task the broker runs, digests included.
- **A write on one node is read on the other** 0.4 to 0.8 ms later, the link having been
  idle.
- **Not measured:** the same against `main`'s binary run in turn, as T2 did (the machine
  ran out of disk for a second build; the `local` figures above are T2's to within its
  own spread), a cluster on separate machines, and a namespace of a million keys.

**Left open**

- **Folding the rows of a counter's past runs** (above), and whether 256 is the right
  bound once restarts count toward it.
- **The digest's cost at the largest namespace** (a million keys): it is a pass over
  every key on the link's task. Keeping it up to date as keys change would remove the
  pass; it was not needed at the sizes measured.
- **T6:** an `eventual` namespace's keys are kept apart from a `local` one's (they hold
  the state that merges, not a value), so the admin listing and lookup need their own
  read of them, and an operator's delete there is `kv_del`'s.
- **T4:** which member raises the expiry event, and the sweep that removes expired keys
  and tombstones on time instead of when room is needed or a digest is made.
- **A node without a store** (`rules.state_max_bytes = 0`) that is sent state frames
  drops them without a count: it has no store to count in.
