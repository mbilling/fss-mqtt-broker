# Rule engine

**Dated 2026-10-07.** Unreleased: no release has the rule engine yet
([ADR 0083](adr/0083-rule-engine.md)). Rules filter, transform and re-route messages as
they pass through the broker. They are written in **EMQX's rule SQL**, with EMQX's
`republish` and `console` actions. A rule written for EMQX's rule engine runs here
unchanged unless it uses one of the constructs listed under
[Differences from EMQX](#differences-from-emqx).

A rule never changes or suppresses the message that triggered it. The original is
delivered exactly as it would have been without the rule; what the rule *produces* is
an additional message.

**New to rules?** [Try it in two minutes](#try-it-in-two-minutes), then take a recipe
from the **[rule cookbook](RULES-COOKBOOK.md)**: sixteen tested rules (alerts, unit
conversion, routing, device presence, CSV and binary decoding, MQTT 5 properties), each a
file in [`docs/examples/rules/`](examples/rules/) with the messages to publish and the
exact output to expect. [Testing and debugging rules](#testing-and-debugging-rules) and
[Gotchas](#gotchas) cover what goes wrong. The rest of this page is the reference.

---

## Try it in two minutes

### 1. Get a build that has the rule engine

No release has the rule engine yet. `v1.1.0` and every earlier release do not: the
binaries, the `ghcr.io/mbilling/fss-mqtt-broker` images and the Helm chart's default
image. Until the next release, build from source (Rust ≥ 1.88; the first build takes a few
minutes):

```sh
git clone https://github.com/mbilling/fss-mqtt-broker && cd fss-mqtt-broker
cargo build --release -p mqttd
export PATH="$PWD/target/release:$PATH"
mqttd --help | grep -e --rule-test
```

The last command prints a `mqttd --rule-test --sql <statement>` line only in a build
that has the rule engine. Do not go by `mqttd --version`: a build of `main` reports the
last release's version (`mqttd 1.1.0` today), exactly as that release's binary does,
until the next release bumps it.

A release binary or image does not know rules, and does not always say so:
`mqttd --check-rules` is an `unrecognised argument`, a `[rules]` table in `mqttd.toml` is
an `unknown config key`, but `MQTTD_RULES_FILE` is ignored without a word. A broker that
runs your rules logs a `rule engine:` line at startup (step 3); no line, no rules.

**For Docker**, build an image from the same checkout. This needs Linux and the musl C
toolchain (`apt install musl-tools` on Debian/Ubuntu); it is what
`scripts/image-smoke.sh` does:

```sh
TARGET="$(uname -m)-unknown-linux-musl"
rustup target add "$TARGET"
cargo build --release -p mqttd --target "$TARGET"
mkdir -p dist && cp "target/$TARGET/release/mqttd" dist/mqttd
docker build -t mqttd:rules .
```

### 2. Write a rules file

Rules live in a TOML file of their own, never in `mqttd.toml`:

```sh
mkdir rules-demo && cd rules-demo
cat > rules.toml <<'EOF'
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
EOF
```

For every message on `sensors/+/data` whose JSON payload has a `temp` above 30, the rule
builds an output from `temp`, the publisher's `clientid` and the message's `qos`, and
publishes that output as JSON (`${.}`) to `alerts/<clientid>`.

Check it the way the broker will load it:

```sh
mqttd --check-rules rules.toml
```

```text
rules OK: rules.toml: 1 rule(s), 1 enabled, sha256 2f027340235af0b20eafed114c6c36869b05aac8568546125274382b66e892ad
  high_temp (enabled): FROM "sensors/+/data", 1 action(s)
```

### 3. Start mqttd with it

```sh
MQTTD_PLAINTEXT_BIND=127.0.0.1:1883 MQTTD_ALLOW_ANONYMOUS=1 MQTTD_DURABLE_SESSIONS=0 \
  MQTTD_HEALTH_BIND=127.0.0.1:8080 MQTTD_RULES_FILE=rules.toml mqttd
```

That is a plaintext listener on localhost, anonymous clients, sessions kept in memory, the
health and metrics endpoint on port 8080, and the rules file: for trying things out, never
for production (the broker's INSECURE warnings say so). Among the startup lines (log
lines on this page are shown without their timestamps and colours; the broker colours its
log even into a file or a pipe, so add `NO_COLOR=1` to a broker whose saved log you will
`grep` for a value such as `rules=1`):

```text
INFO mqttd: rule engine: rules loaded (ADR 0083) rules=1 enabled=1 digest=2f027340235af0b20eafed114c6c36869b05aac8568546125274382b66e892ad
```

The digest is the file's SHA-256, the one `--check-rules` printed.

**With Docker**, run the image from step 1 in the directory that holds `rules.toml`:

```sh
docker run -d --name mqttd -p 127.0.0.1:1883:1883 -p 127.0.0.1:8080:8080 \
  -v "$PWD":/etc/mqttd/rules:ro \
  -e MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 -e MQTTD_ALLOW_ANONYMOUS=1 -e MQTTD_DURABLE_SESSIONS=0 \
  -e MQTTD_HEALTH_BIND=0.0.0.0:8080 -e MQTTD_RULES_FILE=/etc/mqttd/rules/rules.toml \
  mqttd:rules
docker logs mqttd 2>&1 | grep 'rule engine'
```

Mount the directory, not the file: an editor that saves by writing a new file leaves a
single-file mount showing the old one.

### 4. Publish and watch

You need the Mosquitto clients (`apt install mosquitto-clients`, `brew install mosquitto`).
In a second terminal, watch the alerts:

```sh
mosquitto_sub -h 127.0.0.1 -t 'alerts/#' -v
```

In a third, publish a hot reading and a normal one, as client `kitchen`:

```sh
mosquitto_pub -h 127.0.0.1 -i kitchen -q 1 -t sensors/kitchen/data -m '{"temp": 35}'
mosquitto_pub -h 127.0.0.1 -i kitchen -q 1 -t sensors/kitchen/data -m '{"temp": 20}'
```

The watcher prints exactly one line, for the hot reading:

```text
alerts/kitchen {"temp":35,"clientid":"kitchen","qos":1}
```

Both readings still reach anyone subscribed to `sensors/#`, unchanged; the rule only
adds the alert. The alert goes out at QoS 1 because the rule selects `qos`: a rule that
does not, republishes at QoS 0 ([Gotchas](#gotchas)).

### 5. See that the rule fired

The counters are on `GET /metrics`, which the broker serves only when
`MQTTD_HEALTH_BIND` (or a separate `MQTTD_METRICS_BIND`) is set:

```sh
curl -s http://127.0.0.1:8080/metrics | grep '^mqttd_rule' | sort
```

```text
mqttd_rule_actions_total{rule="high_temp",result="ok"} 1
mqttd_rule_evaluations_total{rule="high_temp",result="no_result"} 1
mqttd_rule_evaluations_total{rule="high_temp",result="passed"} 1
mqttd_rules_info{checksum="2f027340235af0b20eafed114c6c36869b05aac8568546125274382b66e892ad"} 1
mqttd_rules_loaded 1
```

`passed` is the 35 (the statement produced an output), `no_result` the 20 (`WHERE` was
false), and `ok` the alert, which the broker accepted and routed. To see each output in the
log instead, add a [`console` action](#console).

### 6. Change it while it runs

Edit the threshold in `rules.toml` (say, `> 25`), then tell the broker to reload:

```sh
kill -HUP "$(pgrep -n -x mqttd)"    # the newest mqttd; Docker: docker kill --signal=HUP mqttd
```

```text
INFO mqttd::reload: rules reloaded (ADR 0083) rules=1 enabled=1 digest=<the new file's SHA-256>
```

The next publish runs the new rule, on connections that were already open too. A broker
started with `MQTTD_CONFIG_WATCH=1` as well checks the file every second and reloads it
without a signal. A file that does not load is rejected, and the running rules stay.
Delete the number after `>` in the `WHERE` and reload again:

```text
WARN mqttd::reload: security reload REJECTED — keeping the running policy trigger="signal" error=rules: rule `high_temp`: expected an expression (line 4, column 1, near `end of statement`)
```

Lines and columns count within the rule's `sql` string, not the file: line 4, column 1
is the end of the statement, just after the newline that closes its `WHERE` line.

Stop the broker with Ctrl-C (or `docker rm -f mqttd`). Every recipe in the
[cookbook](RULES-COOKBOOK.md) runs the same way.

---

## Contents

- [Try it in two minutes](#try-it-in-two-minutes)
- [Testing and debugging rules](#testing-and-debugging-rules)
- [Gotchas](#gotchas)
- [Security: values the publisher chooses](#security-values-the-publisher-chooses)
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

Recipes: [RULES-COOKBOOK.md](RULES-COOKBOOK.md) and [`docs/examples/rules/`](examples/rules/).

---

## Testing and debugging rules

### Check a file: `mqttd --check-rules`

`mqttd --check-rules rules.toml` loads the file exactly as the broker does. It prints a
summary line (the rule count, the enabled count and the file's SHA-256, the value
`mqttd_rules_info{checksum}` will report) and then one line per rule, in the order the
rules run: its id, `enabled` or `disabled`, its `FROM` and its action count. It exits `0`
for a valid file, `1` for an invalid one and `2` for a usage error. An invalid file names
the rule and where its statement broke, counting lines within the `sql` string. The
quickstart's file with the number after `>` deleted:

```text
rules INVALID (rules.toml): rule `high_temp`: expected an expression (line 4, column 1, near `end of statement`)
```

A warning — a [double-quoted string](#gotchas) compared with `=`, or
`direct_dispatch = false` — goes to stderr and does not fail the check.

With no file argument, `mqttd --check-rules` checks the `rules.file` of the effective
configuration (`--config` and `MQTTD_*`). It loads that configuration first, as the broker
would at boot, so a configuration error is reported instead: with only `MQTTD_RULES_FILE`
set, that is `EPHEMERAL durability REFUSED` until `MQTTD_DATA_DIR` or
`MQTTD_DURABLE_SESSIONS=0` is set too. To check a file on its own, name it.
`mqttd --check-config` does not load the rules file; `mqttd --check-config --preflight`
does.

### Try a statement: `mqttd --rule-test`

`mqttd --rule-test` is EMQX's "SQL test", offline: it runs one statement against a
simulated message and prints each output as one line of JSON.

| Option | Default |
|---|---|
| `--sql '<statement>'` | required |
| `--topic <t>` | `t/1` |
| `--payload <p>` | `{}` |
| `--clientid <c>` | `test-client` |
| `--username <u>` | none, so `username` is `undefined` |
| `--qos <n>` | `0` |
| `--event <e>` | for a `$events` statement, see below |

A statement whose `WHERE` passes:

```console
$ mqttd --rule-test --sql 'SELECT payload.temp AS temp, clientid, qos FROM "sensors/+/data" WHERE payload.temp > 30' --topic sensors/kitchen/data --payload '{"temp": 35}' --clientid kitchen --qos 1
{"temp":35,"clientid":"kitchen","qos":1}
```

One whose `WHERE` does not:

```console
$ mqttd --rule-test --sql 'SELECT payload.temp AS temp, clientid, qos FROM "sensors/+/data" WHERE payload.temp > 30' --topic sensors/kitchen/data --payload '{"temp": 20}'
(no output: the statement's WHERE / INCASE did not match this message)
```

A `FOREACH` prints one line per output:

```console
$ mqttd --rule-test --sql 'FOREACH payload.sensors AS s DO s.id AS id, s.v AS v FROM "t/#"' --payload '{"sensors": [{"id": "a", "v": 1}, {"id": "b", "v": 2}]}'
{"id":"a","v":1}
{"id":"b","v":2}
```

It exits `0` for both kinds of result. It exits `1` with `rule test FAILED: <why>` on
stderr when the statement fails (`payload is not JSON, so payload.<field> is unreadable`,
`cannot compare a number with the string 'hot'`, …) and when the message is one the rule
would never see: `topic "other/kitchen" matches none of the FROM filters
(sensors/+/data)`. It exits `2` on a usage error, such as `--qos 3`.

**Events.** A statement that selects `$events/…` runs against a sample of the event, built
from `--clientid`, `--username`, and for `session/subscribed` and `session/unsubscribed`,
`--topic` and `--qos`. The rest is fixed: `peername` `127.0.0.1:52345`, `node`
`rule-test`; a connect is MQTT 5 with keepalive 60, clean start and expiry 0; a
disconnect's `reason` is `normal`. Like a real event, a sample has no `sockname`
([Events](#events-from-events)). It names the sample on stderr, as
`(a sample client.connected event)`, and prints the outputs on stdout:

```console
$ mqttd --rule-test --sql 'SELECT clientid, event, proto_ver, keepalive FROM "$events/client/connected"' --clientid sensor-7
{"clientid":"sensor-7","event":"client.connected","proto_ver":5,"keepalive":60}
```

When the statement names more than one event, it runs against the first one named;
`--event` picks another (`client.connected`, `client.disconnected`, `session.subscribed`
or `session.unsubscribed`, or their topic forms, such as `client/disconnected`):

```console
$ mqttd --rule-test --sql 'SELECT clientid, event, reason FROM "$events/client/connected", "$events/client/disconnected"' --event client.disconnected --clientid sensor-7
{"clientid":"sensor-7","event":"client.disconnected","reason":"normal"}
```

A statement that selects topics and events runs against a publish unless `--event` is
given. An `--event` the statement's `FROM` does not name exits `1`.

**What `--rule-test` does not do.** It runs the statement only, not the actions: the
rendered topic, `qos`, `retain`, payload template and properties of a republish are not
shown. A simulated publish has no MQTT 5 properties, no retain flag and no peer address,
and there are no options to set them. Every option needs a value that is not empty and
does not start with `-`, so an empty payload, or one such as `-5`, cannot be simulated
(either is a usage error, exit `2`). It does not print the loader's warnings (the
double-quote one in particular): run `--check-rules` on the file for those, and a
[`console` action](#console) on a test broker to see real messages.

### Watch a running rule

- **The `console` action** logs each output of its rule at INFO. Add
  `{ function = "console" }` to a rule's `actions` (or make it a rule of its own with
  `SELECT *` to see every field a rule can read):

  ```text
  INFO mqttd::rules: rule console action rule=debug output={"clientid":"dev1","temp":21.5}
  ```

- **The log.** A rule whose statement fails (a payload that is not JSON, a type error, a
  limit) or whose action fails (a rendered topic with a wildcard, `${.}` of binary data, a
  per-message limit, a refusal by the broker) logs one WARN per rule per 10 s, with the
  error. That rule's further failures in the window are logged at DEBUG
  (`RUST_LOG=info,mqttd::rules=debug` shows each one); another rule's failure gets its own
  WARN:

  ```text
  WARN mqttd::rules: rule SQL failed (counted in mqttd_rule_evaluations_total{result="failed"}; this rule's further failures within 10s are logged at debug) rule=sqlfail error=payload is not JSON, so payload.<field> is unreadable (invalid JSON: expected ident at line 1 column 2)
  WARN mqttd::rules: rule action failed (counted in mqttd_rule_actions_total{result="failed"}; this rule's further failures within 10s are logged at debug) rule=star error=cannot JSON-encode binary (non-UTF-8) data; select base64_encode(...) or bin2hexstr(...) of it instead
  ```

  A reload logs `rules reloaded (ADR 0083) rules=<n> enabled=<n> digest=<sha256>`, or
  `security reload REJECTED — keeping the running policy … error=rules: …`.

  The log goes to stdout, with colour codes around each field name even in a file, a
  pipe, `docker logs` or the journal. Searching for message text (`grep 'rules loaded'`)
  works either way; searching for a field (`grep 'rules=1'`) finds nothing unless the
  broker runs with `NO_COLOR=1`.

- **Metrics**, on `GET /metrics` at `MQTTD_HEALTH_BIND` (or `MQTTD_METRICS_BIND`):

  | Series | Meaning |
  |---|---|
  | `mqttd_rule_evaluations_total{rule,result}` | `passed`: the statement produced output and the actions ran. `no_result`: `FROM` matched, but `WHERE` was false or a `FOREACH` produced nothing. `failed`: the statement raised an error. A rule with no series has never matched a message. |
  | `mqttd_rule_actions_total{rule,result}` | `ok`: a console line logged, or a republish the broker accepted and routed. `failed`: see [Operating rules](#operating-rules). |
  | `mqttd_rules_loaded` | Enabled rules loaded on this node. |
  | `mqttd_rules_info{checksum}` | The loaded file's SHA-256, at 1. A checksum replaced by a reload stays exported at 0. |

- **Alerts.** One for statements that fail, one for actions that fail; a rule whose every
  republish fails passes every evaluation, so the first alone never fires for it:

  ```promql
  sum by (rule) (rate(mqttd_rule_evaluations_total{result="failed"}[5m])) > 0
  sum by (rule) (rate(mqttd_rule_actions_total{result="failed"}[5m])) > 0
  ```

### My rule does not fire

Work down the list; each step rules out one cause.

1. **Is the file loaded?** The startup log says `rule engine: rules loaded … rules=<n>`
   (no such line: the binary has no rule engine, step 1, or `MQTTD_RULES_FILE` is unset),
   `mqttd_rules_loaded` counts the enabled rules, and `mqttd_rules_info{checksum}` equals
   the `sha256` that `mqttd --check-rules <file>` prints. After an edit: was there a
   `SIGHUP` (or `MQTTD_CONFIG_WATCH`), and did the log say `rules reloaded`, or
   `security reload REJECTED`? To `grep` a saved log for `rules=` or `digest=`, run the
   broker with `NO_COLOR=1` ([the log](#watch-a-running-rule)).
2. **Is the rule enabled?** `mqttd --check-rules` lists it as `(enabled)`.
3. **Does `FROM` match the topic?** `mqttd --rule-test --sql '…' --topic <the topic>` says
   `matches none of the FROM filters` if it does not. A filter that starts with `#` or
   `+` does not match a topic that starts with `$`. The filter is in double quotes:
   `FROM "sensors/#"`.
4. **Was the publish accepted?** Rules run only on publishes the ACL accepts. An MQTT 5
   publisher is told `0x87`; an MQTT 3.1.1 one is not, and the denial is in the audit log.
5. **What does `mqttd_rule_evaluations_total{rule="<id>"}` say?**
   - No series at all: no message reached the rule (steps 1 to 4).
   - `no_result`: `WHERE` was false. Look for a [double-quoted string](#gotchas)
     (`WHERE name = "sensor_1"` compares two fields) and for a comparison across types.
     Try the statement with a real payload in `--rule-test`.
   - `failed`: the WARN line names the error, typically a payload that is not JSON.
6. **The rule passes, but nothing arrives.**
   - `mqttd_rule_actions_total{result="failed"}` with a WARN: the topic rendered with a
     wildcard or empty, `${.}` met binary data, or the broker refused the message.
   - `ok`: the message was routed. Subscribe to `#` to see the topic it really went to: a
     placeholder for a field the statement did not select renders as `undefined`
     (`alerts/undefined`). Check the subscriber's filter and ACL, and that a QoS 0
     republish was not meant for an offline session (QoS 0 is not queued).
7. **It fires on a republished message?** It never does: a message a rule publishes
   never runs rules. Write the second rule against the original topic.

---

## Gotchas

- **Rules go in their own file, not in `mqttd.toml`.** The configuration's `[rules]`
  table takes only `file`. A `[rules.<id>]` table in `mqttd.toml` refuses the boot as an
  unknown key, with a hint saying where rules go. Do not set
  `config_unknown_keys = "warn"` to get past it: the broker would then boot and ignore the
  rule.
- **Write the SQL in a TOML multi-line literal string, `'''…'''`.** SQL writes a quote
  inside a string literal as `''`, and in a single-quoted TOML string (`sql = '…'`) that
  ends the TOML string, so the file does not parse. Inside `'''…'''` it is safe:

  ```toml
  sql = '''
  SELECT payload FROM "t" WHERE payload = 'go' OR payload = 'it''s'
  '''
  ```

- **Double-quoted strings are field names.** `WHERE payload.name = "sensor_1"` compares
  `payload.name` with a field called `sensor_1`, which does not exist, as in EMQX. A
  message that has no `name` therefore matches (`undefined` equals `undefined`). Write
  `'sensor_1'`. `--check-rules` warns about this; `--rule-test` does not:

  ```console
  $ mqttd --rule-test --sql 'SELECT payload.name AS name FROM "t/#" WHERE payload.name = "sensor_1"' --payload '{"other": 1}'
  {"name":"undefined"}
  ```

- **Not selecting `qos` republishes at QoS 0.** A republish's `qos` defaults to `${qos}`,
  and every placeholder reads the rule's **output**, not the message. A rule that does
  not select `qos` (or `*`) therefore republishes a QoS 2 message at QoS 0. Select `qos`,
  or set `qos = 1` on the action. `retain` works the same way: select
  `flags.retain AS retain` to keep a retained message retained.
- **Placeholders read the output, and a missing value renders as `undefined`.** `${topic}`
  in a rule that did not select `topic` renders `undefined`, in a topic, a payload template
  or a property. Inside `${.}` a missing value becomes the string `"undefined"`. There is
  no `null` literal: `SELECT null AS x` gives `{"x":"undefined"}`; `coalesce(payload.x, 0)`
  gives a default.
- **Every alias is in the output.** `SELECT split(payload, ',') AS f, nth(1, f) AS first`
  puts `f` in `${.}` beside `first`. Nest the call instead
  (`nth(1, split(payload, ',')) AS first`), or write the payload template yourself.
- **An event rule's republish needs a `payload`.** The default payload is `${payload}`,
  and events have no payload, so the body is the text `undefined`. Use
  `payload = "${.}"` (or your own template).
- **`payload = ""` is not an empty payload.** As in EMQX, an empty template means the
  whole output as JSON, like `${.}`. To publish an empty payload, which is how a retained
  message is deleted, select an empty string and use it as the template:

  ```toml
  [rules.clear_state]
  sql = '''SELECT topic(2) AS device, '' AS empty FROM "devices/+/decommission"'''
  actions = [
    { function = "republish", args = { topic = "state/${device}", qos = 1, retain = true, payload = "${empty}" } },
  ]
  ```

  The cookbook's [recipe 12](RULES-COOKBOOK.md#12-keep-the-last-known-state-and-clear-it)
  keeps the state this clears.
- **`SELECT *` with a `${.}` payload fails on binary data.** `SELECT *` includes the raw
  `payload` and `pub_props`, with any `Correlation-Data`. A publish whose payload or
  Correlation-Data is not UTF-8 cannot be encoded as JSON, so that republish fails (it is
  counted and logged). Select the fields you need, or `base64_encode(payload)`.
- **Comparisons across types follow EMQX.** A missing field (`undefined`) compares false
  with anything, except that two `undefined`s are equal. A number against a string
  converts the string (`'40' > 30` is true; a non-numeric string fails the rule). A
  boolean or `null` against a string compares their text (`payload.on = 'true'` is true
  for `{"on": true}`). Anything else uses Erlang's term order: number < `false` < `null`
  < `true` < object < array < string. So `payload.temp > 30` is **true** for
  `{"temp": null}`, `{"temp": true}` and `{"temp": [1]}`. Guard with `is_num`:

  ```console
  $ mqttd --rule-test --sql 'SELECT payload.temp AS temp FROM "t/#" WHERE payload.temp > 30' --payload '{"temp": null}'
  {"temp":null}
  ```

  `WHERE is_num(payload.temp) AND payload.temp > 30` gives no output for it.
- **JSON `null` is a value, not `undefined`.** For `{"x": null}`, `is_null(payload.x)` is
  false and `is_not_null(payload.x)` is true; they test for a missing field.
  `is_null_var` and `is_not_null_var` treat `null` as missing too.
- **Integers beyond 64 bits become floats.** A JSON integer outside the signed 64-bit range
  is decoded as a float: `{"id": 12345678901234567890}` gives `1.2345678901234567e+19` in
  `${.}` and `12345678901234567168.0` in `${id}`. Send such ids as strings, or forward
  `${payload}`, which keeps the original bytes.
- **`topic(n)` counts from 1:** `topic(2)` of `home/kitchen/temp` is `kitchen`, and
  `topic(0)` fails the rule. Indexes (`payload.list[1]`) and `nth` count from 1 too, but
  `substr` counts from 0. More function traps are under [Functions](#functions).
- **Rules never chain.** A message a rule publishes never runs rules, its own or another's.

---

## Security: values the publisher chooses

What a rule republishes is not checked against the publisher's ACL
([Authorization](#delivery-guarantees-qos-0-1-and-2)). A topic template filled from the
message therefore lets the publisher choose where the derived message goes:

- `topic = "devices/${device}"` with `device` selected from the payload: a payload with
  `"device": "x/../admin/cmd"` publishes on `devices/x/../admin/cmd`, and one with no
  `device` on `devices/undefined`. A template that is all placeholders, such as
  `${target}`, can reach `$SYS/…`.
- The client id and the username are chosen by the client too, unless mTLS or the ACL's
  `connect` rules pin them. `alerts/${clientid}`, as in the example above, publishes on
  `alerts/a/../admin` for a client that connects as `a/../admin`.

A rendered topic with a wildcard, NUL or `$share/` fails the action, but `/` and
`$`-prefixed levels are valid topic names. Let only a plain topic level through, in
`WHERE`:

```sql
SELECT payload.device AS device, payload
FROM "ingest"
WHERE is_str(payload.device) AND regex_match(payload.device, '^[A-Za-z0-9_-]{1,64}$')
```

`is_str` keeps a missing or non-string value from failing the rule. A message that does not
pass produces nothing. The cookbook's
[routing recipe](RULES-COOKBOOK.md#7-route-by-a-payload-field-safely) is the complete rule,
and its MQTT 5 recipe guards a user property the same way.

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
  evaluates it; this is the one place rule SQL runs on the hub. Evaluating a Will at
  CONNECT instead, on the connection task, would give it the wrong timestamps and miss
  any reload before the client went away. A rule that selects no Will topic costs a
  Will about 0.17 µs; one that matches costs what it costs any publish (see
  [Performance](#performance)), on top of the Will's own routing. A mass disconnect
  (a partition, a load-balancer restart) brings the Wills at once, so rules that match
  Will topics add to that burst.
- **A publish and its derived messages reach the hub as one command.** The hub routes
  the original first, then its derived messages, and only if it accepted the original:
  a refused publish produces nothing, so a resend cannot duplicate what its rules
  derived. The batch keeps the connection's place in the hub's FIFO data lane; it is
  bounded by the per-message limits below, so it holds the hub loop for at most the
  routing of 1,025 messages.
- **Derived messages are charged to the publisher's ingress credit** (ADR 0082), like
  the publish itself, before the batch is queued, so a rule that multiplies a publish
  cannot multiply what one connection may hold in the hub's queue. If the credit is
  not there, the connection gives back its original's credit and waits for the whole
  charge at once, holding none of it, so no connection holds credit while it waits for
  more. It waits the way any publish waiting for credit does: it stops reading its
  socket but keeps delivering to its client, and its keepalive is not enforced. (A QoS
  2 publish, whose PUBREC already waits in place for the hub's answer, waits for this
  credit in place too. While it waits the connection does nothing else: it delivers
  nothing to its client and notices neither a shutdown drain nor a hangup until the wait
  ends, which the hub's progress bounds, as it bounds the PUBREC's own wait. Its
  keepalive restarts when the wait ends.) The charge is
  clamped to the per-connection cap, so a batch larger than the cap still proceeds, as
  the largest single message does. Under `MQTTD_INGRESS_OVERLOAD=shed-qos0` a QoS 0
  publish never waits: if the credit for its derived messages is not there, they are
  dropped and counted as failed actions. What client/session events and Wills derive
  has no publish to charge ([below](#delivery-guarantees-qos-0-1-and-2)).

## Delivery guarantees: QoS 0, 1 and 2

| Inbound publish | What its rules' messages get |
|---|---|
| `QoS` 0 | Published at the `qos` each action sets. Nothing waits for them, because a QoS 0 publish has no acknowledgement. |
| `QoS` 1 | Every derived message at QoS ≥ 1 gets its own acknowledgement gate, and the **PUBACK waits for all of them**: when it is released, each derived message is stored wherever it was owed (durably where durability applies) or has failed and been counted. |
| `QoS` 2 | As for QoS 1, for the PUBREC. The broker's inbound exactly-once window covers the rules too: a DUP resend of an acknowledged packet id is answered without being forwarded again, so **its rules fire once**. |

**The publisher is told exactly what it would be told without rules — the original's
own answer.** Its derived messages never change it:

- **The hub refuses the original** (a brownout, a retained quota) as it routes it: it
  routes none of the derived messages, so nothing was stored for them anywhere and a
  resend duplicates nothing. The publisher gets the refusal.
- **A derived message fails** — the broker refuses it (a brownout refuses it because it
  needs storage), or cannot vouch for it (a durable write failed, or a peer refused a
  copy after a local one was stored): it is counted as a failed action
  (`mqttd_rule_actions_total{result="failed"}`) and the original's answer stands. The
  original was delivered; withholding its acknowledgement to retry a derived message
  would have the publisher resend it, and the broker re-deliver it, for as long as the
  condition lasts. A brownout refuses new growth writes, and a derived message is one.
- **The original's own fate is unknown**: the acknowledgement is withheld, the
  connection closes and the publisher retries, as for any publish.

One residual: a **peer node** can refuse the original (its verdict on a copy it was
forwarded) after this node's hub has already routed the derived messages. The
publisher is told the refusal, the derived messages stay delivered, and a resend
derives them again.

`tests/rules.rs` proves the first two over a real socket, under a brownout; the
answer table is a unit test (`rules::tests`).

A derived message is published **as no client**: MQTT 5 *No Local* does not suppress it
for the original publisher (in EMQX the rule is the sender, too), and a retained-quota
overflow delivers it live without retaining it rather than refusing the original.

**Client/session events and Wills** (below) hold back no acknowledgement, because there
is no publisher to answer. The messages their rules derive are routed ungated, as a
Will is: each action is counted `ok` once the broker has routed its message, and they
take no room in the table of publishes awaiting acknowledgement, so a burst of events —
a load balancer restarting, a partition healing — cannot crowd a client's publish out of
it. A durable copy one of them loses to a brownout is counted as a drop, as a Will's is,
in `mqttd_publish_dropped_total{reason="brownout"}`. A graceful
shutdown waits for them: the `client/disconnected` events (reason `shutdown`) raised as
the broker drains its connections are routed, and stored where they are owed (a
persistent session's offline queue, durably where durability applies), before the broker
exits. While it drains, what they derive for a subscriber or session on another node is
forwarded acked, and the drain waits for that node's answer too. A node that is itself
in a brownout refuses such a forward when it owes a durable copy there; the draining
node then sends it again unacked, and that node delivers it live. A shared group that
runs out of members to try (each refusing it, or its node dying) gets it unacked at the
first member tried that is still in the group. That wait is part of
the drain, bounded by `shutdown_grace_secs`; a second signal ends it. The drain does not
wait for:

- **forwards from a node draining in a brownout.** A brownout refuses an acknowledged
  publish that owes a durable copy outright, live copies and all, so a node in a brownout
  does not gate what it derives: it goes out live everywhere, as at any other time, with a
  refused durable copy counted as a drop.
- **more than the table of publishes awaiting acknowledgement holds** (65,536 messages or
  64 MiB, client publishes included). Past that the oldest are evicted and no longer
  waited for, and the drain logs how many of these messages it lost that way with a
  WARN (an evicted client publish is not lost: its publisher retries it). A node draining tens of thousands
  of connections under a presence rule, or fewer under a `FOREACH`, can reach it.
- **a peer link that is down for the whole drain.** A draining node does not redial, so
  what it owes there is lost at the grace deadline, with a WARN.

These messages are not charged to any connection's ingress credit,
because no publish carries them: each event, and each Will, is bounded by the per-message
limits instead (at most 1,024 derived messages, carrying at most 4 MiB together; a Will's
budget also grows with four times its payload).

**Authorization.** The ACL decides whether the *original* publish is accepted; rules
run only on accepted publishes. What a rule republishes is not checked against the
publisher's ACL. The rules file is operator configuration with the same trust as the
ACL file, so a rule can deliberately publish into topics the publisher cannot. A topic
built from the publisher's values needs a guard
([Security](#security-values-the-publisher-chooses)).

## The rules file

`[rules] file` in the config (or `MQTTD_RULES_FILE`) names a TOML file. Unset means no
rules. The configuration's `[rules]` table takes only `file`: rules cannot be written in
`mqttd.toml` itself. Each rule is a table named by its id. EMQX writes the same structure
as `rule_engine.rules.<id>` in HOCON:

```toml
[rules.<id>]          # a letter or `_`, then up to 63 letters, digits, `_` or `-`
sql = '''...'''       # required: one SELECT or FOREACH statement
actions = [ ... ]     # zero or more; see Actions
enable = true         # default true; a disabled rule is loaded and listed by --check-rules, never run
description = ""      # optional
```

Loading is **all-or-nothing**: one rule that does not parse rejects the file. At
startup the broker refuses to boot, and on a reload the running rules stay in force.
Unknown keys are errors. The limits are 1,024 rules per file, 16 actions per rule and
64 KiB of SQL per rule. An expression may be at most 256 levels deep, a chain of 256
operands such as `1 + 1 + …` (past it: `expression is more than 256 levels deep`), and
may nest at most 64 levels of parentheses, signs, `NOT`, function calls or array
literals inside each other (past it: `expression nests more than 64 levels deep`).
Evaluating an expression recurses once per level, so a deeper one is refused at load
rather than overflowing a stack when a message arrives. A
republish `qos` or `retain` written as a literal that can never be valid (`qos = 3`,
`retain = "yes"`) is refused at load too.

Per message, the limits protect the broker from what a publisher sends, because rules
often pass payload fields to functions. They apply to the message as a whole — every
rule, every `FOREACH` output — not to each call:

- a `FOREACH` iterates at most 10,000 elements and produces at most 256 outputs;
- all of a message's rules produce at most 1,024 effects, which together carry at most
  4 MiB plus four times the message's payload (topics, payloads and properties of the
  derived messages, and console lines); past it, further actions fail;
- its functions build at most 1 MiB beyond their inputs, together (`pad`'s length, a
  `replace` or `regex_replace` that substitutes a longer string at every match, a
  `join_to_string` separator);
- `map_put` / `mput` take a key path of at most 64 segments, and a timestamp must lie
  within the date range every time zone can render.

Past a limit the function or action fails, so the rule fails (or the action does) and
is counted; the message itself is still routed. And whatever a message derives is
charged to its connection's ingress credit like any publish (above), so a rule cannot
multiply what a publisher may queue for the hub.

Rules evaluate in **id order**, and their derived messages are routed in that order
after the original. The order is byte-wise: `Zeta` runs before `alpha`, and `rule10`
before `rule9`. A subscriber sees that order among messages delivered at `QoS` 0, and
among those delivered at `QoS` 1 or 2; a `QoS` 0 message can reach it before a `QoS` 1
or 2 message routed just ahead of it.

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
`'sensor_1'`. Inside a string literal, `''` is a single quote, which is why the SQL
belongs in a TOML `'''…'''` string ([Gotchas](#gotchas)).

| Construct | Example | Notes |
|---|---|---|
| Field path | `payload.a.b`, `pub_props.'User-Property'.foo` | A path into `payload` decodes the payload as JSON once per message. If the payload is not JSON, a rule that reads into it **fails**; one that only reads `payload` as a whole does not. A JSON integer outside the signed 64-bit range decodes as a float. |
| Index | `payload.list[1]`, `payload.list[-1]` | 1-based; negative counts from the end; out of range is `undefined`. |
| Range | `payload.list[2..3]`, `[1..5]` | A slice, or the integers from one end to the other. |
| Array literal | `['a', 1 + 1]` | |
| Arithmetic | `+ - * / div mod` | `/` always gives a float; `div` and `mod` take integers. `+` concatenates when either side is a string. Overflow and division by zero are errors. |
| Comparison | `= != <> < <= > >=` | `undefined` compares false with any value and equal to `undefined`. A number against a string converts the string; a non-numeric one is an error. A boolean or `null` against a string compares their text. Any other pair of types uses Erlang's term order (number < `false` < `null` < `true` < object < array < string), so `null > 30` is true ([Gotchas](#gotchas)). |
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
| `username` | Its CONNECT username, if it sent one (`undefined` otherwise) |
| `payload` | The payload: text if it is UTF-8, bytes otherwise |
| `peerhost` / `peername` | The publisher's IP / `ip:port` (absent for a session relocated from another node, whose socket is the relaying node) |
| `topic` | The topic, with aliases resolved |
| `qos` | 0, 1 or 2 |
| `flags` | `{"dup": …, "retain": …}` |
| `pub_props` | MQTT 5 properties under their spec names: `User-Property` (a map; a repeated key keeps its last value), `User-Property-Pairs` (every pair, in order), `Content-Type`, `Response-Topic`, `Correlation-Data`, `Payload-Format-Indicator`, `Message-Expiry-Interval`. A publish that carried none (every MQTT 3.1.1 publish) has only an empty `User-Property` map, so `${pub_props.'Content-Type'}` renders `undefined`. |
| `publish_received_at` / `timestamp` | Milliseconds since the epoch: when the broker received the message / when the rules looked at it. Each is read once per message, so every reference in every rule sees the same value, and `timestamp` is never earlier than `publish_received_at` |
| `node` | This node's id |
| `event` | `message.publish` |
| `metadata` | `{"rule_id": …}` |
| `client_attrs` | `{}`: mqttd has no client attributes |

### Events: `FROM "$events/…"`

Both EMQX spellings work, for example `$events/client/connected` and
`$events/client_connected`.

| Event | Fields beyond `clientid`, `username`, `timestamp`, `node`, `event` |
|---|---|
| `$events/client/connected` | `peername`, `proto_name`, `proto_ver`, `keepalive`, `clean_start`, `expiry_interval`, `is_bridge` (always false), `connected_at`, `conn_props`. No `sockname`. |
| `$events/client/disconnected` | `peername`, `reason`, `connected_at`, `disconnected_at`, `disconn_props`; no `sockname`. `reason` is `normal` (a client DISCONNECT with reason `0x00`), EMQX's name for any other reason code a v5 client's DISCONNECT carries (`disconnect_with_will_message` for `0x04`, `unspecified_error`, `protocol_error`, …), `keepalive_timeout`, `tcp_closed` (the socket closed or failed), `server_closed` (the broker ended it: a takeover, an eviction, a protocol violation) or `shutdown` (graceful drain). |
| `$events/session/subscribed` | `peerhost`, `topic`, `qos`, `sub_props`. One event per filter the SUBACK granted. |
| `$events/session/unsubscribed` | `peerhost`, `topic`, `unsub_props`. One event per filter actually removed. |

`conn_props`, `disconn_props`, `sub_props` and `unsub_props` are always `{}`: the
properties the CONNECT, DISCONNECT, SUBSCRIBE or UNSUBSCRIBE carried are not passed to
rules. The client events carry no `sockname` (the listener address)
([Differences from EMQX](#differences-from-emqx)). An event has
no `payload`, so a republish from an event rule needs one of its own:

```toml
[rules.presence]
sql = '''
SELECT clientid, event, timestamp
FROM "$events/client/connected", "$events/client/disconnected"
'''
actions = [
  { function = "republish", args = { topic = "presence/events", qos = 1, payload = "${.}" } },
]
```

Each connect and disconnect then publishes, for example,
`{"clientid":"sensor-7","event":"client.connected","timestamp":1791393256611}`. (Selecting
`reason` too would put `"reason":"undefined"` in every connect's output: a connect has no
reason.)

The cookbook's [presence recipe](RULES-COOKBOOK.md#11-device-presence-from-connect-and-disconnect-events)
keeps one retained status per device.

The message events (`message/delivered`, `message/acked`, `message/dropped`,
`message/delivery_dropped`), `client/connack`, the authentication and authorization
events, `client/ping` and the alarm events are not raised. A rule that selects one is
refused at load.

## Functions

Every function below is named, typed and behaves as in EMQX's built-in function
reference. That reference's examples run as this engine's unit tests
(`crates/mqtt-rules/src/tests_emqx_examples.rs`); where a value differs from the
reference's text, the test says why: a typo in the reference, a last-digit difference in a
transcendental function (Erlang's math library and Rust's round the last bit differently),
or `is_empty` of a missing value (below). A function given the wrong type fails the rule. An unknown function or a wrong argument count fails the
**load**, not the first message.

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
| Legacy accessors | `topic` (`topic(n)` is the nth level, counting from 1: `topic(1)` of `a/b/c` is `a`), `clientid`, `username`, `qos`, `msgid`, `flags`, `flag`, `peerhost`, `clientip`, `payload` (`payload('a.b')` is a path) |

Traps that EMQX's reference states only in passing, each checked against this engine:

- `split('a,,b', ',')` drops the empty field (`["a","b"]`); `split('a,,b', ',', 'notrim')`
  keeps it (`["a","","b"]`), so CSV columns stay in place.
- `substr('hello', 1, 3)` is `ell`: `substr` counts from 0, while `nth` and indexes count
  from 1.
- `regex_extract` returns an array of the groups (`["42"]`), empty when nothing matches,
  and `nth(1, [])` fails the rule.
- `round` takes one argument: `round(2.567, 2)` fails the load.
- `unix_ts_to_rfc3339` and `now_rfc3339` write the broker host's local time zone, as EMQX
  does (`+00:00` on a host or container set to UTC). `format_date` with an explicit offset
  (`'+01:00'`) gives the same text on every host.
- `is_empty` of a missing value fails the rule
  (`is_empty(): expected an array or a map, got a undefined`); EMQX documents it as
  `false`. Guard it: `is_not_null(payload.list) AND is_empty(payload.list)`.

**Not implemented:** `jq`; compression (`gzip`, `gunzip`, `zip`, `unzip`,
`zip_compress`, `zip_uncompress`, `lz4_compress`, `lz4_uncompress`); bit sequences
(`subbits`, `bitsize`, `bytesize`); schema registry and Sparkplug B
(`schema_encode`, `schema_decode`, `schema_check`, `sparkplug_encode`,
`sparkplug_decode`); `maptab_lookup`; the MongoDB date helpers; `map_to_redis_hset_args`
and `join_to_sql_values_string`, which only exist for EMQX's sinks; `contains_topic`;
and `getenv`, because a rule must not be able to read the broker's environment. The
cookbook decodes binary payloads without `subbits`
([recipe 15](RULES-COOKBOOK.md#15-decode-base64-hex-and-binary-payloads)).

## Actions

### `republish`

```toml
{ function = "republish", args = { topic = "alerts/${clientid}", qos = 1, payload = "${.}" } }
```

| Arg | Default (EMQX's) | |
|---|---|---|
| `topic` | — (required) | A template. A rendered topic that is empty, has a wildcard, NUL or `$share/`, or is over 65,535 bytes fails the action, not the rule. A topic built from the publisher's values needs a [guard](#security-values-the-publisher-chooses). |
| `qos` | `"${qos}"` | 0, 1, 2 or one placeholder. **The placeholder reads the rule's output, not the input message**, as in EMQX: a rule that does not select `qos` (or `*`) republishes at **QoS 0**. A literal outside 0 to 2 fails the load; a placeholder that renders one fails the action. |
| `retain` | `"${retain}"` | A boolean or one placeholder. A publish has no `retain` field (it is `flags.retain`), so this defaults to false unless the SQL selects `flags.retain AS retain`. |
| `payload` | `"${payload}"` | A template. An empty string is the whole output as JSON (`${.}`). `${payload}` keeps a binary payload's exact bytes. An event has no payload: give an event rule's republish one. |
| `user_properties` | `"${user_properties}"` | One placeholder naming a map (or EMQX's `[{key, value}]` list) in the output. `"${pub_props.'User-Property'}"` carries the publisher's properties in wire order, duplicates included. If absent, none are sent. |
| `mqtt_properties` | none | `Payload-Format-Indicator`, `Message-Expiry-Interval`, `Content-Type`, `Response-Topic`, `Correlation-Data`; each value is a template. A `Payload-Format-Indicator` or `Message-Expiry-Interval` that does not render as a valid number, or a `Response-Topic` that is not a valid topic name, is dropped, as in EMQX, and the message still goes out. A placeholder for a value the publisher did not send renders as `undefined` like any other, so `"Content-Type" = "${pub_props.'Content-Type'}"` sends `Content-Type: undefined` for a publish without one: give it a default with `coalesce()` ([recipe 16](RULES-COOKBOOK.md#16-mqtt-5-user-properties-in-and-out)). |
| `direct_dispatch` | — | Accepted for compatibility. mqttd always dispatches directly, so `false` gets a load warning and changes nothing. |

**Templates.** `${path}` reads the rule's **output** (what `SELECT` produced), with the
path syntax of the SQL: `${payload.a.b}`, `${pub_props.'User-Property'.k}`,
`${list[1]}`. `${.}` is the whole output as JSON. A missing value renders as
`undefined`, as in EMQX, so a mistake shows up in the message instead of silently
disappearing. JSON-encoding a binary (non-UTF-8) value is an error, never a lossy
conversion: select `base64_encode(payload)` instead.

**Size.** A derived message is not bounded by `limits.max_packet_size`, which limits what
clients send. A subscriber whose MQTT 5 Maximum Packet Size it exceeds does not get its
copy (`mqttd_publish_dropped_total{reason="too-large"}`), as for any message.

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
[mqtt-bridge](BRIDGE.md). What else rules cannot do, and what to use instead, is in the
cookbook's [What rules cannot do](RULES-COOKBOOK.md#what-rules-cannot-do).

## Operating rules

| Task | How |
|---|---|
| Validate a rules file | `mqttd --check-rules <file>` (exit 0 OK, 1 invalid, 2 usage) lists every rule it loaded. With no file it loads the effective configuration and checks its `rules.file`. `mqttd --check-config --preflight` loads it too. See [Check a file](#check-a-file-mqttd---check-rules). |
| Try a statement | `mqttd --rule-test --sql '<statement>' [--topic t] [--payload p] [--clientid c] [--username u] [--qos n] [--event e]` prints each output as JSON. This is EMQX's "SQL test", offline; a `$events` statement runs against a sample event. See [Try a statement](#try-a-statement-mqttd---rule-test). |
| Change rules | Edit the file, then `SIGHUP` (`kill -HUP <pid>`; under the shipped systemd unit, `systemctl reload mqttd`; in a container, `docker kill --signal=HUP <container>`). With `MQTTD_CONFIG_WATCH` set, the file watcher picks the edit up on its own; the image has no shell to send a signal from, so the Helm chart turns the watcher on. With the admin API (`admin.bind` and its mTLS certificates), `mqttd --admin reload` or `POST /admin/v1/reload` does the same ([ADMIN-CLI.md](ADMIN-CLI.md#config-reload)). The next publish runs the new rules. A file that does not load is rejected with the reload, keeping the running rules: the **whole** reload, so a broken rules file also holds back an ACL or credential change made at the same time. |
| Confirm every node runs the same rules | `mqttd_rules_info{checksum}`: the file's SHA-256, one series at 1 per node (a checksum replaced by a reload stays exported at 0). Rules are per-node configuration like the ACL file, so a node with a different file evaluates its own clients' publishes with different rules. |
| Watch rules work | On `GET /metrics`, served at `MQTTD_HEALTH_BIND` or `MQTTD_METRICS_BIND`: `mqttd_rule_evaluations_total{rule,result}` (`passed`, `no_result`, `failed`) and `mqttd_rule_actions_total{rule,result}` (`ok`, `failed`) are EMQX's per-rule counters. An action is counted once its outcome is known: a republish is `ok` when the broker routed it (accepted it, for a QoS ≥ 1 message behind a QoS ≥ 1 publish and for every message an event or a Will derives; routed it, for anything else) and `failed` when it could not render, when the broker refused it, when the hub refused its original (and so routed none of its derived messages), or when its fate is unknown. `ok` does not follow each copy to its subscribers: a subscriber's own limits can drop one later (a full offline queue), counted in `mqttd_publish_dropped_total` as for any message. `mqttd_rules_loaded` is the number of enabled rules. A rule whose statement or action fails logs one WARN per rule per 10 s with the error; the rest are at DEBUG. |

Alert on a failing rule, both its statement and its actions:

```promql
sum by (rule) (rate(mqttd_rule_evaluations_total{result="failed"}[5m])) > 0
sum by (rule) (rate(mqttd_rule_actions_total{result="failed"}[5m])) > 0
```

### Shipping the rules file

None of the shipped packagings sets `MQTTD_RULES_FILE` yet; add it beside the file. Until
a release has the rule engine, each one also needs a binary or image built from source
([step 1](#1-get-a-build-that-has-the-rule-engine)): a release ignores the variable.

- **systemd** (`deploy/systemd`). The unit reads `/etc/mqttd/mqttd.env` only when it
  starts, so setting the variable needs one restart; later edits to the file need only
  `systemctl reload mqttd`:

  ```sh
  sudo install -m 0640 -o root -g mqttd rules.toml /etc/mqttd/rules.toml
  echo 'MQTTD_RULES_FILE=/etc/mqttd/rules.toml' | sudo tee -a /etc/mqttd/mqttd.env
  sudo systemctl restart mqttd
  ```

- **Docker Compose** (`deploy/compose`). Put `rules.toml` in a `rules/` directory next to
  `compose.yaml`, and the variable and the mount in an overlay of your own, named
  explicitly on the command line as `compose.plaintext.yaml` is:

  ```yaml
  # compose.rules.yaml: docker compose -f compose.yaml -f compose.rules.yaml up -d
  services:
    mqttd-1: &rules
      environment:
        MQTTD_RULES_FILE: /etc/mqttd/rules/rules.toml
      volumes:
        - ./rules:/etc/mqttd/rules:ro
    mqttd-2: *rules
    mqttd-3: *rules
  ```

  Run it with `MQTTD_IMAGE` set to an image that has the rule engine. The brokers run as
  uid 65532, so the file must be readable by others (mode `0644`). Reload after an edit
  with `docker compose kill -s HUP mqttd-1 mqttd-2 mqttd-3`.
- **Kubernetes** (the Helm chart): a ConfigMap mounted through the chart's extra volume
  values ([KUBERNETES.md](KUBERNETES.md#rules)).

## Performance

Measured on one core of the development VM (`cargo bench -p mqtt-rules`, criterion,
one 60-byte JSON payload per message). Each figure is the time the engine's evaluation
adds to one publish, paid on the connection task:

| Case | Time per publish |
|---|---|
| Rules loaded, none selects the topic | 0.17 µs |
| `WHERE` rejects (JSON decode + compare) | 1.4 µs |
| `WHERE` passes, one republish | 1.8 µs |
| `SELECT *` with a `${.}` JSON republish | 6.3 µs |
| `FOREACH` over 10 elements, 10 republishes | 15.8 µs |

The single-node knee is 75,000 msg/s, measured across 4 vCPUs. At 1.8 µs per matching
publish, rule work on every one of those messages would take about 0.14 core, spread
across the connection tasks. These are microbenchmarks of the engine, not a cluster
benchmark. A derived message costs what any publish costs to route, so a rule that
doubles your message count doubles the routing load.

## Differences from EMQX

Verified against EMQX's rule engine source and documentation (emqx/emqx and
emqx/emqx-docs, release 6.2). Each row is a behaviour a rule written for EMQX could
notice.

| | EMQX | mqttd |
|---|---|---|
| Republished messages | Re-enter the rule engine unless `direct_dispatch = true` | Never re-enter it. A rule cannot loop, and a rule chain that relied on re-triggering needs a second rule on the original topic. |
| Actions | `republish`, `console`, data-integration sinks | `republish`, `console`. A sink reference is refused at load. |
| Events | 14 event topics | `client/connected`, `client/disconnected`, `session/subscribed`, `session/unsubscribed` |
| Event property maps (`conn_props`, `disconn_props`, `sub_props`, `unsub_props`) | The properties of the CONNECT, DISCONNECT, SUBSCRIBE or UNSUBSCRIBE | Always `{}` |
| `sockname` in `client/connected` and `client/disconnected` | The listener's address | Absent, so `undefined` |
| Data-bridge sources (`$bridges/…`) | Yes | No |
| Functions | 124 in the built-in reference, plus `jq` | 107 of those 124, plus the 13 legacy accessors EMQX keeps undocumented (120 in all). The rest are refused at load. |
| `is_empty` of a missing value | Documented as `false` | Fails the rule: `is_empty(): expected an array or a map, got a undefined` |
| JSON integers | Any size (Erlang integers) | Signed 64-bit. One outside that range decodes as a float, so `12345678901234567890` becomes `1.2345678901234567e+19`. `${payload}` keeps the original bytes. |
| Regular expressions | PCRE | Rust's `regex`: linear-time, no backreferences or look-around. In `regex_replace`, `\N` and `&` are translated to the same meaning. |
| `''` inside a string literal | Kept as two quotes | One quote (standard SQL) |
| `sprintf` | Erlang `io_lib:format` | `~s`, `~p`, `~w`, `~n` and `~~` |
| `strlen`, `substr`, `pad` | Grapheme clusters | Unicode scalar values (the same for text without combining marks) |
| `id` | EMQX GUID | 32 hex digits: clock, per-process salt and counter |
| `client_attrs`, `mountpoint` | Client attributes, mountpoints | Always empty / absent |
| Namespaces (6.x) | Rules can be confined to a namespace | No namespaces |
| Unaliased computed field | Stored under a generated `_v_…` key | Stored under the expression's source text |
| Ack semantics | The rule engine runs after the publish is accepted; a republish does not hold the publisher's ack | A QoS ≥ 1 republish holds the publisher's PUBACK/PUBREC until its fate is known; the answer is still the original's own, and a failed republish is a failed action ([above](#delivery-guarantees-qos-0-1-and-2)) |
| A refused publish | Rules ran on it before it was refused downstream | The hub routes none of its derived messages (unless the refusal is a peer's, arriving later) |
| Limits on payload-driven work | None beyond the Erlang process's memory | A `FOREACH` iterates at most 10,000 elements; a function may build at most 1 MiB beyond its inputs; `map_put`/`mput` paths have at most 64 segments; timestamps must be renderable in every time zone; expressions at most 256 levels deep and nested at most 64 levels ([The rules file](#the-rules-file)) |
| Where it runs | Every node | Every node, once per message at the node it arrived at, never on a forwarded copy |

## Migrating rules from EMQX

The EMQX converter translates the rule engine:

```sh
scripts/migrate/from-emqx.py emqx.conf --out-config mqttd.toml --out-rules rules.toml
mqttd --check-rules rules.toml
```

Each rule's SQL is carried verbatim with its `republish` and `console` actions. A sink
action is dropped with a `TODO(migrate)` line, and its rule stays live with its other
actions: a rule left with none runs and produces nothing, so give it a `republish` (and a
consumer, [INTEGRATION.md](INTEGRATION.md)) or delete it. A rule that selects a
data-bridge source or an event mqttd does not raise, or calls a function it does not
implement, is written **commented out** with the reason, so the file still loads. The CI
fixture is part EMQX's own documented rule examples and part rules written for the
converter's gaps, each marked as such in the file
([MIGRATION.md](MIGRATION.md#emqx--mqttd)).
