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
> the other answer would cost.

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
an operator's delete (D10). A namespace is declared in the rules file with its
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
  load, naming the key. *(Decided by default, 2026-10-11: on, with that ceiling.)*
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
  *(Decided by default, 2026-10-11: the names stay `kv_*`, beside EMQX's `kv_store_*`.)*

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

### D11. Testing: sequences, not single messages

`mqttd --rule-test` gains `--sequence <file>`: a timed list of messages and events
replayed through one statement, or through the configured rules file, against an
in-memory store with a clock the sequence drives, printing each step's outputs and what
the step changed in the store. Expiry events fire in the replay when the sequence's
clock passes them. This is how a rule with state is written and reviewed: the examples
become a sequence with expected outputs, and RULES.md's worked examples are such
sequences, run by CI.

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
  client's channel for its publishes, its events and the deliveries to it, so the values
  live as long as the connection and are seen by nothing else. mqttd evaluates those on
  the client's connection task (ADR 0083), which gets the dictionary: empty at connect,
  gone at disconnect, not carried by a takeover (EMQX starts a new process there too).
  A republished message is evaluated on the same task and sees it, as in EMQX.
  *(Decided by default, 2026-10-11: provided. The other answer keeps them refused.)*
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
    notes task or the store's expiry task.
  - *The test.* A dry run uses a scratch table (D11).
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

## Decided by default, 2026-10-11

These are choices with a real cost either way that neither EMQX's behaviour, the code nor
an obvious default settles. The record is written on the recommended answer; each is
cheap to revisit before the task named builds it.

| # | Choice | Decided by default | It costs | The other answer |
|---|---|---|---|---|
| 1 | Where an `eventual` namespace is replicated (D5) | To every member | Every node holds the whole namespace; a write is sent to N−1 nodes | A replica set per key (`replication_factor`): less memory, but a rule on a node outside the set reads nothing unless reads go over the network on the publish path. Revisit before T3 |
| 2 | `strong` mode (D5) | Phase 2, behind a design amendment; refused at load until then | No exact cluster-wide counter in phase 1: an `eventual` quota can over-admit by what was in flight | Build it now on the hand-off: every call a thread hand-over, and concurrent waits uncapped (ADR 0086's open question). Revisit at T9 |
| 3 | EMQX's `proc_dict_*` (D13) | Provided, per connection task | One task (T11), up to 64 KiB per connection that a rule makes use it, two documented differences (a Will; events without a connection) | Keep refusing them: a rule that uses them stays unportable. Drop T11 |
| 4 | Is state on without a switch (D1) | Yes: using it is the switch, under `rules.state_max_bytes` = 256 MiB | A listed rules writer (ADR 0084) can make a node hold up to that much | Off unless switched on, as `jq` is: then a ported EMQX rule that calls `kv_store_put` does not load until the operator acts. Revisit before T1 |
| 5 | The names (D3) | `kv_*`, beside EMQX's `kv_store_*` | Two families one word apart with different tables (a warning when a file uses both); a clash if EMQX ever adds a `kv_get` | `state_*` for mqttd's. Revisit before T1; after it, a rename breaks rules |

## Phasing

1. **`local` and `eventual`, every type, EMQX's own functions, expiry events, bounds,
   metrics, the admin verbs and `--sequence` testing** (T1 to T8, T11). The stateful
   patterns ("above Y for X", change, counting windows, silence, distinct counts) all
   work in this phase, and `kv_store_*` rules port from EMQX.
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
