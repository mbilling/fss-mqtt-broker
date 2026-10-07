# Rule engine

**Dated 2026-10-07.** Unreleased: lands after `v1.0.18` ([ADR 0083](adr/0083-rule-engine.md)). Rules that filter,
transform and re-route messages as they pass through the broker, written in **EMQX's
rule SQL** with EMQX's `republish` and `console` actions. A rule written for EMQX's
rule engine runs here unchanged unless it uses one of the constructs listed under
[Differences from EMQX](#differences-from-emqx).

```toml
# /etc/mqttd/rules.toml  (point [rules] file / MQTTD_RULES_FILE at it)
[rules.high_temp]
description = "Alert on hot sensors"
sql = '''
SELECT payload.temp AS temp, clientid, qos
FROM "sensors/+/data"
WHERE payload.temp > 30
'''
actions = [
  { function = "republish", args = { topic = "alerts/${clientid}", payload = "${.}" } },
]
```

A rule never changes or suppresses the message that triggered it. The original is
delivered exactly as it would have been without the rule; what the rule *produces* is
an additional message.

---

## Contents

- [Where rules run, and why it scales](#where-rules-run-and-why-it-scales)
- [Delivery guarantees: QoS 0, 1 and 2](#delivery-guarantees-qos-0-1-and-2)
- [The rules file](#the-rules-file)
- [SQL](#sql)
- [Fields](#fields)
- [Functions](#functions)
- [Actions](#actions)
- [Operating rules](#operating-rules)
- [Performance](#performance)
- [Differences from EMQX](#differences-from-emqx)
- [Migrating rules from EMQX](#migrating-rules-from-emqx)

---

## Where rules run, and why it scales

- **On the connection task that read the publish**, after the ACL check and before the
  message reaches the hub. Connection tasks run in parallel on every core, so rule
  evaluation never touches the hub's single-threaded loop for client publishes.
- **Once per message, on the node it arrived at.** A message forwarded to another node
  is never evaluated again there, even though that node runs the same rules. Rule work
  is therefore spread across nodes exactly the way client connections are. It scales
  with the cluster as ingress does.
- **What a rule republishes is an ordinary publish.** It is routed, forwarded to other
  nodes, shared-subscription balanced, retained, queued for offline persistent sessions
  and quorum-replicated like a message a client sent. A subscriber on any node receives
  it.
- **A republished message never re-enters the rule engine**, so no rule can loop. EMQX
  calls this `direct_dispatch`; in mqttd it is always on.
- **Last Wills run rules too**, as in EMQX. The hub publishes a Will, so the hub
  evaluates it. Wills are rare, so this is the one place rule SQL runs on the hub.

## Delivery guarantees: QoS 0, 1 and 2

| Inbound publish | What its rules' messages get |
|---|---|
| `QoS` 0 | Published at the `qos` each action sets. Nothing waits for them, because a QoS 0 publish has no acknowledgement. |
| `QoS` 1 | Every derived message at QoS ≥ 1 gets its own acknowledgement gate. The publisher's **PUBACK is released only after the original and all of them are accepted**, durably where durability applies. |
| `QoS` 2 | As for QoS 1, for the PUBREC. The broker's inbound exactly-once window covers the rules too: a DUP resend of an acknowledged packet id is answered without being forwarded again, so **its rules fire once**. |

If the original and its derived messages disagree, for example the original is stored
but a derived message is refused under a disk brownout, the acknowledgement is
**withheld**: the connection closes without a PUBACK and the publisher retries. Telling
the publisher "refused" would claim nothing was stored, which is false; acknowledging it
would claim everything was, which is also false. A retry can duplicate the original,
which QoS 1 allows. `tests/rules.rs` proves this over a real socket.

A derived message is published **as no client**: MQTT 5 *No Local* does not suppress it
for the original publisher (in EMQX the rule is the sender, too), and a retained-quota
overflow delivers it live without retaining it rather than refusing the original.

Client/session events (below) gate nothing, because there is no acknowledgement to
hold. Their derived messages are published like a Will.

**Authorization.** The ACL decides whether the *original* publish is accepted; rules
run only on accepted publishes. What a rule republishes is not checked against the
publisher's ACL. The rules file is operator configuration with the same trust as the
ACL file, so a rule can deliberately publish into topics the publisher cannot.

## The rules file

`[rules] file` in the config (or `MQTTD_RULES_FILE`) names a TOML file. Unset means no
rules. Each rule is a table named by its id. EMQX writes the same structure as
`rule_engine.rules.<id>` in HOCON:

```toml
[rules.<id>]          # a letter or `_`, then up to 63 letters, digits, `_` or `-`
sql = '...'           # required: one SELECT or FOREACH statement
actions = [ ... ]     # zero or more; see Actions
enable = true         # default true; a disabled rule is loaded and listed, never run
description = ""      # optional
```

Loading is **all-or-nothing**: one rule that does not parse rejects the file. At
startup the broker refuses to boot, and on a reload the running rules stay in force.
Unknown keys are errors. The limits are 1,024 rules per file, 16 actions per rule and
64 KiB of SQL per rule. One message can produce at most 256 outputs from one `FOREACH`
and 1,024 effects across all its rules.

Rules evaluate in **id order**, and their derived messages are published in that order
after the original.

## SQL

The grammar is EMQX's (`rulesql`), with the same operator precedence. Keywords are
case-insensitive.

```sql
SELECT <field> [AS <alias>], ... FROM "<topic filter or event>", ... [WHERE <condition>]

FOREACH <array expression> [AS <name>]
  [DO <field> [AS <alias>], ...]
  [INCASE <condition>]
  FROM "<topic filter or event>", ...
  [WHERE <condition>]
```

**Quoting follows EMQX's grammar.** `'single quotes'` are string literals. `"double
quotes"` are *identifiers*: a topic filter in `FROM`, or a field name such as
`"my-field"` elsewhere. `WHERE name = "sensor_1"` compares `name` to a *field* called
`sensor_1`, as EMQX does. The loader warns when it sees that pattern; write
`'sensor_1'`. Inside a string literal, `''` is a single quote.

| Construct | Example | Notes |
|---|---|---|
| Field path | `payload.a.b`, `pub_props.'User-Property'.foo` | A path into `payload` decodes the payload as JSON once per message. If the payload is not JSON, a rule that reads into it **fails**; one that only reads `payload` as a whole does not. |
| Index | `payload.list[1]`, `payload.list[-1]` | 1-based; negative counts from the end; out of range is `undefined`. |
| Range | `payload.list[2..3]`, `[1..5]` | A slice, or the integers from one end to the other. |
| Array literal | `['a', 1 + 1]` | |
| Arithmetic | `+ - * / div mod` | `/` always gives a float; `div` and `mod` take integers. `+` concatenates when either side is a string. Overflow and division by zero are errors. |
| Comparison | `= != <> < <= > >=` | `undefined` compares false with any value and equal to `undefined`. A number against a string converts the string; a non-numeric one is an error. |
| Topic match | `topic =~ 'sensors/+/data'` | The MQTT filter match. |
| Logic | `AND OR NOT`, `x IN (...)`, `x NOT IN (...)` | A condition passes only on boolean `true`. `IN` matches exactly, so `1` is not in `(1.0)`. |
| `CASE` | `CASE WHEN x > 7 THEN 7 ELSE x END`, `CASE x WHEN 'a' THEN 1 END` | No match and no `ELSE` gives `undefined`. |
| Alias | `payload.x AS y`, `payload.x y`, `1 AS a.b` | A dotted alias builds a nested map. An unaliased path stores under its own path, so `payload.x` gives `{"payload":{"x":…}}`. |
| Comment | `-- to end of line` | |

`WHERE` sees what `SELECT` selected: `SELECT payload.x AS y FROM "t" WHERE y = 1`. A
later `SELECT` field can read an earlier alias.

**`FOREACH`** produces one output per element of the array its last expression
evaluates to. Each output runs the rule's actions once. The element is `item` unless the
expression has an alias. Without `DO`, an output is every field plus the element, as in
EMQX. Leading expressions can name intermediate values:
`FOREACH payload.data AS d, d.sensors AS s DO s.name AS name FROM "t/#"`.

Within the same meaning, mqttd accepts a little more than EMQX: `AND`/`OR`/`NOT` in
`SELECT`, keywords as path segments (`payload.from`), field access on a computed value
(`json_decode(payload).a`), and `1e5` float literals.

## Fields

### Messages: `FROM "<topic filter>"`

`FROM` takes MQTT topic filters. `#` and `+` follow MQTT rules, including that a
leading wildcard does not match a `$`-topic.

| Field | Value |
|---|---|
| `id` | A unique message id (32 upper-case hex digits) |
| `clientid` | The publisher's client id |
| `username` | Its CONNECT username, if it sent one |
| `payload` | The payload: text if it is UTF-8, bytes otherwise |
| `peerhost` / `peername` | The publisher's IP / `ip:port` (absent for a session relocated from another node, whose socket is the relaying node) |
| `topic` | The topic, with aliases resolved |
| `qos` | 0, 1 or 2 |
| `flags` | `{"dup": …, "retain": …}` |
| `pub_props` | MQTT 5 properties under their spec names: `User-Property` (a map; a repeated key keeps its last value), `User-Property-Pairs` (every pair, in order), `Content-Type`, `Response-Topic`, `Correlation-Data`, `Payload-Format-Indicator`, `Message-Expiry-Interval` |
| `publish_received_at` / `timestamp` | Milliseconds since the epoch |
| `node` | This node's id |
| `event` | `message.publish` |
| `metadata` | `{"rule_id": …}` |
| `client_attrs` | `{}`: mqttd has no client attributes |

### Events: `FROM "$events/…"`

Both EMQX spellings work, for example `$events/client/connected` and
`$events/client_connected`.

| Event | Fields beyond `clientid`, `username`, `timestamp`, `node`, `event` |
|---|---|
| `$events/client/connected` | `peername`, `proto_name`, `proto_ver`, `keepalive`, `clean_start`, `expiry_interval`, `is_bridge` (always false), `connected_at`, `conn_props` |
| `$events/client/disconnected` | `peername`, `reason`, `connected_at`, `disconnected_at`, `disconn_props`. `reason` is `normal` (a client DISCONNECT), `keepalive_timeout`, `tcp_closed` (the socket closed or failed), `server_closed` (the broker ended it: a takeover, an eviction, a protocol violation) or `shutdown` (graceful drain). |
| `$events/session/subscribed` | `peerhost`, `topic`, `qos`, `sub_props`. One event per filter the SUBACK granted. |
| `$events/session/unsubscribed` | `peerhost`, `topic`, `unsub_props`. One event per filter actually removed. |

The message events (`message/delivered`, `message/acked`, `message/dropped`,
`message/delivery_dropped`), `client/connack`, the authentication and authorization
events, `client/ping` and the alarm events are not raised. A rule that selects one is
refused at load.

## Functions

Every function below is named, typed and behaves as in EMQX's built-in function
reference; that reference's examples are this engine's tests
(`crates/mqtt-rules/src/tests.rs`). A function given the wrong type fails the rule. An
unknown function or a wrong argument count fails the **load**, not the first message.

| Group | Functions |
|---|---|
| Math | `abs`, `acos`, `acosh`, `asin`, `asinh`, `atan`, `atanh`, `ceil`, `cos`, `cosh`, `exp`, `floor`, `fmod`, `log`, `log10`, `log2`, `round`, `power`, `random`, `sin`, `sinh`, `sqrt`, `tan`, `tanh` |
| Type checks | `is_array`, `is_bool`, `is_float`, `is_int`, `is_map`, `is_null`, `is_not_null`, `is_null_var`, `is_not_null_var`, `is_num`, `is_str`, `is_empty` |
| Conversion | `bool`, `float`, `float2str`, `int`, `str`, `str_utf8`, `str_utf16_le`, `map` |
| Strings | `ascii`, `concat`, `find`, `join_to_string`, `lower`, `ltrim`, `pad`, `regex_match`, `regex_replace`, `regex_extract`, `replace`, `reverse`, `rm_prefix`, `rtrim`, `split`, `sprintf`, `strlen`, `substr`, `tokens`, `trim`, `unescape`, `upper` |
| Maps | `map_new`, `map_get`, `map_put`, `mget`, `mput`, `map_keys`, `map_values`, `map_size`, `map_to_entries` |
| Arrays | `contains`, `first`, `last`, `length`, `nth`, `sublist` |
| Hashing | `md5`, `sha`, `sha256`, `hash_to_range`, `map_to_range` |
| Bits | `bitand`, `bitor`, `bitxor`, `bitnot`, `bitsl`, `bitsr` |
| Encoding | `base64_encode`, `base64_decode` (both with `'urlsafe'` / `'no_padding'`), `json_decode`, `json_encode`, `bin2hexstr`, `hexstr2bin`, `sqlserver_bin2hexstr` |
| Time | `now_timestamp`, `now_rfc3339`, `unix_ts_to_rfc3339`, `rfc3339_to_unix_ts`, `timezone_to_offset_seconds`, `format_date`, `date_to_unix_ts` |
| UUID | `uuid_v4`, `uuid_v4_no_hyphen` |
| Conditional | `coalesce`, `coalesce_ne` |
| Legacy accessors | `topic` (`topic(n)` is the nth level), `clientid`, `username`, `qos`, `msgid`, `flags`, `flag`, `peerhost`, `clientip`, `payload` (`payload('a.b')` is a path) |

**Not implemented:** `jq`; compression (`gzip`, `gunzip`, `zip`, `unzip`,
`zip_compress`, `zip_uncompress`, `lz4_compress`, `lz4_uncompress`); bit sequences
(`subbits`, `bitsize`, `bytesize`); schema registry and Sparkplug B
(`schema_encode`, `schema_decode`, `schema_check`, `sparkplug_encode`,
`sparkplug_decode`); `maptab_lookup`; the MongoDB date helpers; `map_to_redis_hset_args`
and `join_to_sql_values_string`, which only exist for EMQX's sinks; `contains_topic`;
and `getenv`, because a rule must not be able to read the broker's environment.

## Actions

### `republish`

```toml
{ function = "republish", args = { topic = "alerts/${clientid}", qos = 1, payload = "${.}" } }
```

| Arg | Default (EMQX's) | |
|---|---|---|
| `topic` | — (required) | A template. A rendered topic that is empty, has a wildcard, NUL or `$share/`, or is over 65,535 bytes fails the action, not the rule. |
| `qos` | `"${qos}"` | 0, 1, 2 or one placeholder. **The placeholder reads the rule's output, not the input message**, as in EMQX: a rule that does not select `qos` (or `*`) republishes at **QoS 0**. |
| `retain` | `"${retain}"` | A boolean or one placeholder. A publish has no `retain` field (it is `flags.retain`), so this defaults to false unless the SQL selects `flags.retain AS retain`. |
| `payload` | `"${payload}"` | A template. An empty string is the whole output as JSON (`${.}`). `${payload}` keeps a binary payload's exact bytes. |
| `user_properties` | `"${user_properties}"` | One placeholder naming a map (or EMQX's `[{key, value}]` list) in the output. `"${pub_props.'User-Property'}"` carries the publisher's properties in wire order, duplicates included. If absent, none are sent. |
| `mqtt_properties` | none | `Payload-Format-Indicator`, `Message-Expiry-Interval`, `Content-Type`, `Response-Topic`, `Correlation-Data`; each value is a template. A value that renders badly is dropped, as in EMQX. |
| `direct_dispatch` | — | Accepted for compatibility. mqttd always dispatches directly, so `false` gets a load warning and changes nothing. |

**Templates.** `${path}` reads the rule's **output** (what `SELECT` produced), with the
path syntax of the SQL: `${payload.a.b}`, `${pub_props.'User-Property'.k}`,
`${list[1]}`. `${.}` is the whole output as JSON. A missing value renders as
`undefined`, as in EMQX, so a mistake shows up in the message instead of silently
disappearing. JSON-encoding a binary (non-UTF-8) value is an error, never a lossy
conversion: select `base64_encode(payload)` instead.

### `console`

```toml
{ function = "console" }
```

Logs the output at INFO as `rule console action rule=<id> output=<json>`. Use it for
debugging, not as a data path.

### No sinks

There are no Kafka, HTTP, database or other data-integration actions (ADR 0083 §4). An
action that names one, an EMQX bridge id like `"kafka:my_sink"`, is refused at load.
Republish to a topic and consume it with a `$share` group
([INTEGRATION.md](INTEGRATION.md)). For an MQTT sink, use
[mqtt-bridge](BRIDGE.md).

## Operating rules

| Task | How |
|---|---|
| Validate a rules file | `mqttd --check-rules <file>` (exit 0 OK, 1 invalid, 2 usage). With no file it checks the configured `rules.file`. `mqttd --check-config --preflight` loads it too. |
| Try a statement | `mqttd --rule-test --sql '<statement>' [--topic t] [--payload p] [--clientid c] [--username u] [--qos n]` prints each output as JSON. This is EMQX's "SQL test", offline. |
| Change rules | Edit the file, then `SIGHUP`, or `POST /admin/v1/reload`; with `MQTTD_CONFIG_WATCH` set, the file watcher picks the edit up on its own. The next publish runs the new rules. A file that does not load is rejected with the reload, keeping the running rules. |
| Confirm every node runs the same rules | `mqttd_rules_info{checksum}`: the file's SHA-256, one series at 1 per node. Rules are per-node configuration like the ACL file, so a node with a different file evaluates its own clients' publishes with different rules. |
| Watch rules work | `mqttd_rule_evaluations_total{rule,result}` (`passed`, `no_result`, `failed`) and `mqttd_rule_actions_total{rule,result}` (`ok`, `failed`) are EMQX's per-rule counters. `mqttd_rules_loaded` is the number of enabled rules. A rule failing on every message logs one WARN per 10 s with the error; the rest are at DEBUG. |

Alert on a failing rule:

```promql
sum by (rule) (rate(mqttd_rule_evaluations_total{result="failed"}[5m])) > 0
```

## Performance

Measured on one core of the development VM (`cargo bench -p mqtt-rules`, criterion,
one 60-byte JSON payload per message). Each figure is the cost added to one publish,
paid on the connection task:

| Case | Time per publish |
|---|---|
| Rules loaded, none selects the topic | 0.17 µs |
| `WHERE` rejects (JSON decode + compare) | 1.4 µs |
| `WHERE` passes, one republish | 1.8 µs |
| `SELECT *` with a `${.}` JSON republish | 6.3 µs |
| `FOREACH` over 10 elements, 10 republishes | 15.8 µs |

The single-node knee is 75,000 msg/s, measured across 4 vCPUs. At 1.8 µs per matching
publish, rule work on every one of those messages would take about 0.14 core, spread
across the connection tasks. These are microbenchmarks, not a cluster benchmark. A
derived message costs what any publish costs to route, so a rule that doubles your
message count doubles the routing load.

## Differences from EMQX

Verified against EMQX's rule engine source and documentation (emqx/emqx and
emqx/emqx-docs, release 6.2). Each row is a behaviour a rule written for EMQX could
notice.

| | EMQX | mqttd |
|---|---|---|
| Republished messages | Re-enter the rule engine unless `direct_dispatch = true` | Never re-enter it. A rule cannot loop, and a rule chain that relied on re-triggering needs a second rule on the original topic. |
| Actions | `republish`, `console`, data-integration sinks | `republish`, `console`. A sink reference is refused at load. |
| Events | 14 event topics | `client/connected`, `client/disconnected`, `session/subscribed`, `session/unsubscribed` |
| Data-bridge sources (`$bridges/…`) | Yes | No |
| Functions | 124 in the built-in reference, plus `jq` | 107 of those 124, plus the 13 legacy accessors EMQX keeps undocumented (120 in all). The rest are refused at load. |
| Regular expressions | PCRE | Rust's `regex`: linear-time, no backreferences or look-around. In `regex_replace`, `\N` and `&` are translated to the same meaning. |
| `''` inside a string literal | Kept as two quotes | One quote (standard SQL) |
| `sprintf` | Erlang `io_lib:format` | `~s`, `~p`, `~w`, `~n` and `~~` |
| `strlen`, `substr`, `pad` | Grapheme clusters | Unicode scalar values (the same for text without combining marks) |
| `id` | EMQX GUID | 32 hex digits: clock, per-process salt and counter |
| `client_attrs`, `mountpoint` | Client attributes, mountpoints | Always empty / absent |
| Namespaces (6.x) | Rules can be confined to a namespace | No namespaces |
| Unaliased computed field | Stored under a generated `_v_…` key | Stored under the expression's source text |
| Ack semantics | The rule engine runs after the publish is accepted; a republish does not hold the publisher's ack | A QoS ≥ 1 republish holds the publisher's PUBACK/PUBREC until it is accepted ([above](#delivery-guarantees-qos-0-1-and-2)) |
| Where it runs | Every node | Every node, once per message at the node it arrived at, never on a forwarded copy |

## Migrating rules from EMQX

The EMQX converter translates the rule engine:

```sh
scripts/migrate/from-emqx.py emqx.conf --out-config mqttd.toml --out-rules rules.toml
mqttd --check-rules rules.toml
```

Each rule's SQL is carried verbatim with its `republish` and `console` actions. A sink
action becomes a `TODO(migrate)` line. A rule that selects a data-bridge source or an
event mqttd does not raise, or calls a function it does not implement, is written
**commented out** with the reason, so the file still loads. The CI fixture is EMQX's own
documented rule examples ([MIGRATION.md](MIGRATION.md#emqx--mqttd)).
