# Rule cookbook

**Dated 2026-10-07.** For the rule engine, which no release has yet: see
[RULES.md](RULES.md) for how to get a build that has it.

Sixteen working recipes for mqttd's [rule engine](RULES.md). Each one is a rules file in
[`docs/examples/rules/`](examples/rules/) that you can copy as it is, with the messages to
publish and the exact output to expect.

Every recipe on this page is run against the real `mqttd` binary by
[`crates/mqttd/tests/rules_cookbook.rs`](../crates/mqttd/tests/rules_cookbook.rs). The same
test checks this page: each rules file is quoted here verbatim, and every code block on the
page, every command and every line of output, is generated from what the test publishes and
receives, so none is missing, stale or extra. The gotchas are tested too: the test runs the
guarded recipes without their guards and checks that each trap is real. If a recipe stopped
doing what this page says, the build would fail.

For the SQL, the fields, the functions and the actions themselves, see
[RULES.md](RULES.md). For the recipes put together on realistic data (ten simulated
minutes of power plants, homes and cars, with 21 rules and the alerts, KPIs and
privacy-safe feeds they derive), see the [rule-engine demo](../demo/rules/README.md).

---

## Contents

- [Run any recipe in under a minute](#run-any-recipe-in-under-a-minute)
- [1. Debug: see what a rule sees](#1-debug-see-what-a-rule-sees)
- [2. A threshold alert](#2-a-threshold-alert)
- [3. Convert units](#3-convert-units)
- [4. Reshape a vendor's payload](#4-reshape-a-vendors-payload)
- [5. Flatten nested JSON](#5-flatten-nested-json)
- [6. Split an array into one message per element](#6-split-an-array-into-one-message-per-element)
- [7. Route by a payload field, safely](#7-route-by-a-payload-field-safely)
- [8. Add who, where and when](#8-add-who-where-and-when)
- [9. Move to a new topic namespace](#9-move-to-a-new-topic-namespace)
- [10. Thin out a stream without state](#10-thin-out-a-stream-without-state)
- [11. Device presence from connect and disconnect events](#11-device-presence-from-connect-and-disconnect-events)
- [12. Keep the last known state, and clear it](#12-keep-the-last-known-state-and-clear-it)
- [13. Choose the QoS of what a rule publishes](#13-choose-the-qos-of-what-a-rule-publishes)
- [14. Parse CSV and key=value text](#14-parse-csv-and-keyvalue-text)
- [15. Decode base64, hex and binary payloads](#15-decode-base64-hex-and-binary-payloads)
- [16. MQTT 5 user properties in and out](#16-mqtt-5-user-properties-in-and-out)
- [What rules cannot do](#what-rules-cannot-do)

---

## Run any recipe in under a minute

You need a build of `mqttd` that has the rule engine (no release has it yet) and the
Mosquitto command-line clients, `mosquitto_sub` and `mosquitto_pub`. Run the commands from
the repository root, so the recipe paths resolve. From a source checkout, build the broker
and put it on your `PATH`, in each terminal where you run `mqttd` ([RULES.md](RULES.md)
also shows how to build a Docker image):

```sh
cargo build --release -p mqttd
export PATH="$PWD/target/release:$PATH"
```

**1. Check the recipe.** `--check-rules` loads the file exactly as the broker will and lists
its rules in the order they run:

```sh
mqttd --check-rules docs/examples/rules/02-threshold-alert.toml
```

```text
rules OK: docs/examples/rules/02-threshold-alert.toml: 1 rule(s), 1 enabled, sha256 fc7b72adc4d1ebb7b5e5c1b68cde6243697ec86d11e034007a9012eaeeb4b7ab
  overheat_alert (enabled): FROM "machines/+/telemetry", 1 action(s)
```

**2. Start a broker that runs it.** This one listens on `127.0.0.1:1883`, lets anyone
connect without a password and keeps everything in memory. It is for trying things out,
never for production:

```sh
MQTTD_PLAINTEXT_BIND=127.0.0.1:1883 MQTTD_ALLOW_ANONYMOUS=1 MQTTD_DURABLE_SESSIONS=0 \
  MQTTD_RULES_FILE=docs/examples/rules/02-threshold-alert.toml mqttd
```

Its log confirms the file is loaded. The digest is the file's SHA-256, the same one
`--check-rules` prints:

```text
2026-10-07T16:46:48.642001Z  INFO mqttd: rule engine: rules loaded (ADR 0083) rules=1 enabled=1 digest=fc7b72adc4d1ebb7b5e5c1b68cde6243697ec86d11e034007a9012eaeeb4b7ab
```

**3. Watch everything.** In a second terminal:

```sh
mosquitto_sub -q 2 -t '#' -F '%t  qos=%q retain=%r  %p'
```

`-q 2` subscribes at QoS 2, so every message arrives at the QoS it was published with. The
format prints each message on one line: its topic, its QoS, its retain flag and its payload.

**4. Publish.** In a third terminal:

```sh
mosquitto_pub -i m-7 -t machines/m-7/telemetry -m '{"temp": 95}'
```

The watcher prints the original message, then the alert the rule derived from it:

```text
machines/m-7/telemetry  qos=0 retain=0  {"temp": 95}
alerts/critical/m-7  qos=1 retain=0  {"machine":"m-7","temp":95,"alarm":"overheat","severity":"critical"}
```

That is the whole loop. For any other recipe, stop the broker (Ctrl-C), start it again with
that recipe's file in `MQTTD_RULES_FILE`, restart the watcher, and run the recipe's
commands. With no data directory, the broker keeps everything in memory, retained
messages included, so every start is a clean slate.

Things that hold for every recipe:

- **A rule never changes or stops the original.** The watcher receives the original
  unchanged, as well as what the rules derived from it. To keep a message from subscribers,
  use the ACL, not a rule.
- **Rules run in id order**, and one rule's actions in the order they are listed. The
  broker routes the original first and then what the rules derive, in that order, and the
  output on this page is listed in that order.
- **The order on your screen can differ in one way.** The broker sends a QoS 0 message the
  moment it is routed, and a QoS 1 or 2 message through the subscriber's queue, so a QoS 0
  line can print before a QoS 1 or 2 line that was routed just ahead of it, even before the
  original. And `mosquitto_sub` prints a QoS 2 message once its QoS 2 exchange has
  completed, which can be after later messages. Within QoS 0, and within QoS 1 and 2, the
  order is exactly as shown, and the test checks it.
- **Edit a recipe without restarting:** change the file and send the broker `SIGHUP`
  (`kill -HUP <pid>`). A file that does not load is rejected and the running rules stay.
- **A rule that fails** (for example, it reads `payload.x` from a payload that is not JSON)
  publishes nothing. The original is still delivered, the failure is counted in
  `mqttd_rule_evaluations_total{result="failed"}`, and the log says why, at most once every
  10 seconds per rule.

---

## 1. Debug: see what a rule sees

**Problem:** you want to see the fields a rule receives before you write one.

The `console` action logs each output of its rule. `SELECT *` makes that output every field
of the message.

```toml
# Recipe 01: see exactly what a rule sees (the console action)
#
# Logs every message published under factory/ to the broker's log at INFO, with every
# field a rule can read: clientid, payload, topic, qos, flags, timestamps and the MQTT 5
# properties. Use it to learn the field names before you write a real rule, then delete
# it: it is a debugging aid, not a data path. It publishes nothing, and the original
# message is delivered as usual.
#
# The log line is JSON, and JSON holds text, not raw bytes: in a payload (or MQTT 5
# Correlation-Data) that is not UTF-8 text, each byte sequence that is not text is
# logged as U+FFFD. To see such payloads, log them as hex: docs/RULES-COOKBOOK.md shows
# the rule.
#
# In:  factory/line1/temp  qos=1 retain=0  {"t": 20.5}
# The broker logs one line for it:
#   INFO mqttd::rules: rule console action rule=debug_factory output={"id":"...","clientid":"plc-1",...}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.debug_factory]
description = "Log every message under factory/ (remove when done)"
sql = 'SELECT * FROM "factory/#"'
actions = [{ function = "console" }]
```

Try it:

```sh
mosquitto_pub -i plc-1 -q 1 -t factory/line1/temp -m '{"t": 20.5}'
```

The watcher prints only the original, because the rule publishes nothing:

```text
factory/line1/temp  qos=1 retain=0  {"t": 20.5}
```

The broker's log has one line for it. The id, the port and the times differ on every run:

```text
2026-10-07T16:35:42.937873Z  INFO mqttd::rules: rule console action rule=debug_factory output={"id":"00065D42B4CEC1F9F092834900000000","clientid":"plc-1","username":"undefined","payload":"{\"t\": 20.5}","peerhost":"127.0.0.1","peername":"127.0.0.1:54475","topic":"factory/line1/temp","qos":1,"flags":{"dup":false,"retain":false},"pub_props":{"User-Property":{}},"publish_received_at":1791390942937,"client_attrs":{},"event":"message.publish","timestamp":1791390942937,"node":"node-local","metadata":{"rule_id":"debug_factory"}}
```

**Try a statement without a broker.** `mqttd --rule-test` runs one statement against a
message you describe (`--topic`, `--payload`, and optionally `--clientid`, `--username`,
`--qos`) and prints each output as JSON:

```sh
mqttd --rule-test --sql 'SELECT topic(2) AS machine, payload.temp AS temp FROM "machines/+/telemetry" WHERE payload.temp >= 75' --topic machines/m-7/telemetry --payload '{"temp": 80.5}'
```

```text
{"machine":"m-7","temp":80.5}
```

When the `WHERE` does not match:

```sh
mqttd --rule-test --sql 'SELECT topic(2) AS machine, payload.temp AS temp FROM "machines/+/telemetry" WHERE payload.temp >= 75' --topic machines/m-7/telemetry --payload '{"temp": 70}'
```

```text
(no output: the statement's WHERE / INCASE did not match this message)
```

When the rule cannot run on that message, it says why and exits with status 1:

```sh
mqttd --rule-test --sql 'SELECT topic(2) AS machine, payload.temp AS temp FROM "machines/+/telemetry" WHERE payload.temp >= 75' --topic machines/m-7/telemetry --payload hot
```

```text
rule test FAILED: payload is not JSON, so payload.<field> is unreadable (invalid JSON: expected value at line 1 column 1)
```

**Gotcha: binary payloads.** The console action logs its output as JSON, and JSON holds
text, not raw bytes. In a payload, or MQTT 5 Correlation-Data, that is not UTF-8 text,
each byte sequence that is not text is logged as U+FFFD (`�`), the replacement
character, as EMQX's JSON encoder writes it. The message itself is delivered byte for
byte:

```sh
printf '\001\377\304' | mosquitto_pub -i plc-2 -t factory/line1/raw -s
```

```text
2026-10-10T17:41:07.512204Z  INFO mqttd::rules: rule console action rule=debug_factory output={"id":"00065D7FA2C1A3B8F092834900000001","clientid":"plc-2","username":"undefined","payload":"\u0001��","peerhost":"127.0.0.1","peername":"127.0.0.1:54519","topic":"factory/line1/raw","qos":0,"flags":{"dup":false,"retain":false},"pub_props":{"User-Property":{}},"publish_received_at":1791654067512,"client_attrs":{},"event":"message.publish","timestamp":1791654067512,"node":"node-local","metadata":{"rule_id":"debug_factory"}}
```

To see the bytes a device sends, use this rule in place of the recipe's. It logs the
payload as hex, so it works for text and bytes alike:

```toml
[rules.debug_factory]
description = "Log every message under factory/, its payload as hex"
sql = 'SELECT clientid, topic, qos, bin2hexstr(payload) AS payload_hex FROM "factory/#"'
actions = [{ function = "console" }]
```

For the same message, it logs:

```text
2026-10-07T18:04:33.031296Z  INFO mqttd::rules: rule console action rule=debug_factory output={"clientid":"plc-2","topic":"factory/line1/raw","qos":0,"payload_hex":"01FFC4"}
```

**Gotchas.** `--rule-test` runs the SQL only: it does not run the actions, so check topic
and payload templates on a broker, as above. And remove the console rule when you are done:
it logs a line at INFO for every message it matches.

---

## 2. A threshold alert

**Problem:** alert when a machine runs hot, with a severity that alerting tools can
subscribe to separately.

```toml
# Recipe 02: a threshold alert with a severity level
#
# When a machine reports 75 degrees or more on machines/<machine>/telemetry, publish an
# alert on alerts/<severity>/<machine> at QoS 1: "critical" from 90, "warning" below
# that. A reading under 75 produces nothing. The machine id is a level of the topic the
# device published on (topic(2)), so it is always exactly one topic level.
#
# In:  machines/m-7/telemetry  qos=0 retain=0  {"temp": 80.5}
# Out: alerts/warning/m-7  qos=1 retain=0  {"machine":"m-7","temp":80.5,"alarm":"overheat","severity":"warning"}
# In:  machines/m-7/telemetry  qos=0 retain=0  {"temp": 95}
# Out: alerts/critical/m-7  qos=1 retain=0  {"machine":"m-7","temp":95,"alarm":"overheat","severity":"critical"}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.overheat_alert]
description = "Alert when a machine runs hot"
sql = '''
SELECT
  topic(2)     AS machine,
  payload.temp AS temp,
  'overheat'   AS alarm,
  CASE WHEN payload.temp >= 90 THEN 'critical' ELSE 'warning' END AS severity
FROM "machines/+/telemetry"
WHERE payload.temp >= 75
'''
actions = [
  { function = "republish", args = { topic = "alerts/${severity}/${machine}", qos = 1, payload = "${.}" } },
]
```

Try it:

```sh
mosquitto_pub -i m-7 -t machines/m-7/telemetry -m '{"temp": 70}'
mosquitto_pub -i m-7 -t machines/m-7/telemetry -m '{"temp": 80.5}'
mosquitto_pub -i m-7 -t machines/m-7/telemetry -m '{"temp": 95}'
```

The watcher prints:

```text
machines/m-7/telemetry  qos=0 retain=0  {"temp": 70}
machines/m-7/telemetry  qos=0 retain=0  {"temp": 80.5}
alerts/warning/m-7  qos=1 retain=0  {"machine":"m-7","temp":80.5,"alarm":"overheat","severity":"warning"}
machines/m-7/telemetry  qos=0 retain=0  {"temp": 95}
alerts/critical/m-7  qos=1 retain=0  {"machine":"m-7","temp":95,"alarm":"overheat","severity":"critical"}
```

**Gotcha:** the reading of 70 is still delivered. A rule only adds messages; it never
filters the original.

---

## 3. Convert units

**Problem:** a dashboard wants Fahrenheit, hectopascals, miles per hour and volts, and the
devices send Celsius, pascals, metres per second and millivolts.

```toml
# Recipe 03: convert units and round them
#
# Converts a weather station's metric readings for a US dashboard: Celsius to
# Fahrenheit with one decimal, pascals to hectopascals, metres per second to whole miles
# per hour, and millivolts to volts. The result goes to weather/<station>/imperial.
# `/` always gives a float, and round() takes one argument and gives an integer, so one
# decimal place is round(x * 10) / 10.
#
# In:  weather/oslo/raw  qos=0 retain=0  {"temp_c": 21.37, "pressure_pa": 101325, "wind_ms": 5.2, "battery_mv": 3610}
# Out: weather/oslo/imperial  qos=0 retain=0  {"station":"oslo","temp_f":70.5,"pressure_hpa":1013.25,"wind_mph":12,"battery_v":3.61}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.weather_imperial]
description = "Convert metric weather readings for a US dashboard"
sql = '''
SELECT
  topic(2)                                        AS station,
  round((payload.temp_c * 9 / 5 + 32) * 10) / 10  AS temp_f,       -- one decimal place
  payload.pressure_pa / 100                       AS pressure_hpa,
  round(payload.wind_ms * 2.23694)                AS wind_mph,     -- a whole number
  payload.battery_mv / 1000                       AS battery_v
FROM "weather/+/raw"
'''
actions = [
  { function = "republish", args = { topic = "weather/${station}/imperial", payload = "${.}" } },
]
```

Try it:

```sh
mosquitto_pub -i ws-1 -t weather/oslo/raw -m '{"temp_c": 21.37, "pressure_pa": 101325, "wind_ms": 5.2, "battery_mv": 3610}'
```

The watcher prints:

```text
weather/oslo/raw  qos=0 retain=0  {"temp_c": 21.37, "pressure_pa": 101325, "wind_ms": 5.2, "battery_mv": 3610}
weather/oslo/imperial  qos=0 retain=0  {"station":"oslo","temp_f":70.5,"pressure_hpa":1013.25,"wind_mph":12,"battery_v":3.61}
```

**Gotchas:** `/` always gives a float, even for whole numbers (`3000 / 1000` is `3.0`).
`round()` takes one argument and gives an integer, so round to one decimal place with
`round(x * 10) / 10`. And this rule does not select `qos`, so it publishes at QoS 0 (see
[recipe 13](#13-choose-the-qos-of-what-a-rule-publishes)).

---

## 4. Reshape a vendor's payload

**Problem:** a vendor's devices send `devId`, `vals.t` and a millisecond `ts`, and your
systems expect your own nested schema with an ISO 8601 time.

```toml
# Recipe 04: rename and reshape a vendor's payload into your own schema
#
# Maps a vendor's uplink format (devId, vals.t, vals.h, ts in milliseconds) onto a
# canonical nested schema with an ISO 8601 time, published on canonical/<device id> at
# QoS 1. A dotted alias (device.id) builds a nested object. The device id comes from the
# payload and becomes a topic level, so the WHERE lets only a plain id through (see
# recipe 07): anything else produces nothing.
#
# In:  vendor/acme/uplink  qos=1 retain=0  {"devId": "th-42", "ts": 1791386000000, "vals": {"t": 21.5, "h": 40}}
# Out: canonical/th-42  qos=1 retain=0  {"device":{"id":"th-42","vendor":"acme"},"measurements":{"temperature":21.5,"humidity":40},"time":"2026-10-07T15:13:20.000+00:00"}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.vendor_to_canonical]
description = "Map vendor uplinks onto the canonical telemetry schema"
sql = '''
SELECT
  payload.devId  AS device.id,
  topic(2)       AS device.vendor,
  payload.vals.t AS measurements.temperature,
  payload.vals.h AS measurements.humidity,
  unix_ts_to_rfc3339(payload.ts, 'millisecond') AS time
FROM "vendor/+/uplink"
WHERE is_str(payload.devId) AND regex_match(payload.devId, '^[A-Za-z0-9_-]{1,64}$')
'''
actions = [
  { function = "republish", args = { topic = "canonical/${device.id}", qos = 1, payload = "${.}" } },
]
```

Try it. The second message has a device id that is not a single topic level:

```sh
mosquitto_pub -i acme-gw -q 1 -t vendor/acme/uplink -m '{"devId": "th-42", "ts": 1791386000000, "vals": {"t": 21.5, "h": 40}}'
mosquitto_pub -i acme-gw -q 1 -t vendor/acme/uplink -m '{"devId": "th-42/cmd", "ts": 1791386000000, "vals": {"t": 21.5, "h": 40}}'
```

The watcher prints:

```text
vendor/acme/uplink  qos=1 retain=0  {"devId": "th-42", "ts": 1791386000000, "vals": {"t": 21.5, "h": 40}}
canonical/th-42  qos=1 retain=0  {"device":{"id":"th-42","vendor":"acme"},"measurements":{"temperature":21.5,"humidity":40},"time":"2026-10-07T15:13:20.000+00:00"}
vendor/acme/uplink  qos=1 retain=0  {"devId": "th-42/cmd", "ts": 1791386000000, "vals": {"t": 21.5, "h": 40}}
```

**Gotchas:** `unix_ts_to_rfc3339` writes the time in the broker host's local time zone, as
EMQX does: `+00:00` on a host or container set to UTC, as here, and `+01:00` on one set to
Danish winter time. For the same text on every host, use `format_date` with an explicit
offset (recipe 8). It writes UTC as `+00:00`, not `Z`; both are valid RFC 3339, but a
consumer that compares strings must expect the first.

---

## 5. Flatten nested JSON

**Problem:** a gateway sends deeply nested JSON. One consumer wants it flat, and Telegraf
wants InfluxDB line protocol.

```toml
# Recipe 05: flatten nested JSON, and write it as InfluxDB line protocol too
#
# Flattens a gateway's nested state into one level of JSON on flat/<gateway>, and
# publishes the same reading as InfluxDB line protocol text on influx/pumps for a
# Telegraf MQTT consumer. One rule, two actions: they publish in the order they are
# listed.
#
# In:  gw/edge-1/state  qos=0 retain=0  {"device": {"id": "pump-3", "fw": {"version": "2.1.0"}}, "readings": {"env": {"temp": 40.2, "hum": 31}, "power": {"volts": 229.8, "amps": 3.1}}}
# Out: flat/edge-1  qos=0 retain=0  {"gateway":"edge-1","device_id":"pump-3","fw_version":"2.1.0","env_temp":40.2,"env_hum":31,"power_w":712}
# Out: influx/pumps  qos=0 retain=0  pump,gateway=edge-1,device=pump-3,fw=2.1.0 temp=40.2,hum=31,power_w=712
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.flatten_gateway_state]
description = "Flatten nested gateway state into one level"
sql = '''
SELECT
  topic(2)                  AS gateway,
  payload.device.id         AS device_id,
  payload.device.fw.version AS fw_version,
  payload.readings.env.temp AS env_temp,
  payload.readings.env.hum  AS env_hum,
  round(payload.readings.power.volts * payload.readings.power.amps) AS power_w
FROM "gw/+/state"
'''
actions = [
  { function = "republish", args = { topic = "flat/${gateway}", payload = "${.}" } },
  { function = "republish", args = { topic = "influx/pumps", payload = "pump,gateway=${gateway},device=${device_id},fw=${fw_version} temp=${env_temp},hum=${env_hum},power_w=${power_w}" } },
]
```

Try it:

```sh
mosquitto_pub -i edge-1 -t gw/edge-1/state -m '{"device": {"id": "pump-3", "fw": {"version": "2.1.0"}}, "readings": {"env": {"temp": 40.2, "hum": 31}, "power": {"volts": 229.8, "amps": 3.1}}}'
```

The watcher prints:

```text
gw/edge-1/state  qos=0 retain=0  {"device": {"id": "pump-3", "fw": {"version": "2.1.0"}}, "readings": {"env": {"temp": 40.2, "hum": 31}, "power": {"volts": 229.8, "amps": 3.1}}}
flat/edge-1  qos=0 retain=0  {"gateway":"edge-1","device_id":"pump-3","fw_version":"2.1.0","env_temp":40.2,"env_hum":31,"power_w":712}
influx/pumps  qos=0 retain=0  pump,gateway=edge-1,device=pump-3,fw=2.1.0 temp=40.2,hum=31,power_w=712
```

**Gotcha:** a text template inserts values as they are. InfluxDB needs spaces, commas and
`=` in tag values escaped, so only use this for values you know are plain.

---

## 6. Split an array into one message per element

**Problem:** a gateway batches several readings into one message, and downstream wants one
message per sensor, plus an alert for each hot one.

```toml
# Recipe 06: split a batch (a JSON array) into one message per element
#
# A gateway sends several readings in one message to save bandwidth. split_batch
# publishes one message per reading on sensors/<sensor>/temp; split_batch_hot publishes
# an alert for only the readings above 50 (INCASE filters the elements). Rules run in id
# order, so every per-reading message comes before the alerts. The sensor ids come from
# the payload and become topic levels, so INCASE lets only plain ids through (see
# recipe 07).
#
# In:  gw/gw-9/batch  qos=1 retain=0  {"gateway": "gw-9", "readings": [{"sensor": "s1", "temp": 21.0}, {"sensor": "s2", "temp": 85.5}, {"sensor": "s3", "temp": 22.4}]}
# Out: sensors/s1/temp  qos=1 retain=0  {"sensor":"s1","temp":21.0,"gateway":"gw-9"}
# Out: sensors/s2/temp  qos=1 retain=0  {"sensor":"s2","temp":85.5,"gateway":"gw-9"}
# Out: sensors/s3/temp  qos=1 retain=0  {"sensor":"s3","temp":22.4,"gateway":"gw-9"}
# Out: alerts/hot/s2  qos=1 retain=0  {"sensor":"s2","temp":85.5}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.split_batch]
description = "One message per reading in a gateway batch"
sql = '''
FOREACH payload.readings AS r
DO
  r.sensor        AS sensor,
  r.temp          AS temp,
  payload.gateway AS gateway
INCASE is_str(r.sensor) AND regex_match(r.sensor, '^[A-Za-z0-9_-]{1,64}$')
FROM "gw/+/batch"
'''
actions = [
  { function = "republish", args = { topic = "sensors/${sensor}/temp", qos = 1, payload = "${.}" } },
]

[rules.split_batch_hot]
description = "An alert for each reading above 50"
sql = '''
FOREACH payload.readings AS r
DO
  r.sensor AS sensor,
  r.temp   AS temp
INCASE r.temp > 50 AND is_str(r.sensor) AND regex_match(r.sensor, '^[A-Za-z0-9_-]{1,64}$')
FROM "gw/+/batch"
'''
actions = [
  { function = "republish", args = { topic = "alerts/hot/${sensor}", qos = 1, payload = "${.}" } },
]
```

Try it. The second batch has a sensor id that is not a single topic level:

```sh
mosquitto_pub -i gw-9 -q 1 -t gw/gw-9/batch -m '{"gateway": "gw-9", "readings": [{"sensor": "s1", "temp": 21.0}, {"sensor": "s2", "temp": 85.5}, {"sensor": "s3", "temp": 22.4}]}'
mosquitto_pub -i gw-9 -q 1 -t gw/gw-9/batch -m '{"gateway": "gw-9", "readings": [{"sensor": "s4/../cmd", "temp": 99.0}]}'
```

The watcher prints:

```text
gw/gw-9/batch  qos=1 retain=0  {"gateway": "gw-9", "readings": [{"sensor": "s1", "temp": 21.0}, {"sensor": "s2", "temp": 85.5}, {"sensor": "s3", "temp": 22.4}]}
sensors/s1/temp  qos=1 retain=0  {"sensor":"s1","temp":21.0,"gateway":"gw-9"}
sensors/s2/temp  qos=1 retain=0  {"sensor":"s2","temp":85.5,"gateway":"gw-9"}
sensors/s3/temp  qos=1 retain=0  {"sensor":"s3","temp":22.4,"gateway":"gw-9"}
alerts/hot/s2  qos=1 retain=0  {"sensor":"s2","temp":85.5}
gw/gw-9/batch  qos=1 retain=0  {"gateway": "gw-9", "readings": [{"sensor": "s4/../cmd", "temp": 99.0}]}
```

**Gotchas:** each element runs the rule's actions once, and one message can produce at
most 256 outputs this way. Without `qos = 1`, the per-sensor copies of a QoS 1 batch would
go out at QoS 0.

---

## 7. Route by a payload field, safely

**Problem:** a gateway posts every device's message to one topic, and subscribers want
them per device type and device.

```toml
# Recipe 07: route one shared topic by a payload field, safely
#
# A gateway posts every device's message to the single topic ingest. This rule forwards
# each one, byte for byte, to devices/<kind>/<device>, where kind comes from the
# payload's type (door, meter, anything else is "other") and device from its device
# field.
#
# The guard matters. The device id comes from the PUBLISHER, and what a rule publishes
# is not checked against the publisher's ACL. Without the WHERE, a payload with
# "device": "x/../admin/cmd" would publish on devices/door/x/../admin/cmd, and one with
# no device field on devices/door/undefined. The WHERE lets through only an id that is
# one plain topic level: letters, digits, '_' and '-', at most 64 of them.
#
# In:  ingest  qos=1 retain=0  {"type": "door", "device": "d-17", "open": true}
# Out: devices/door/d-17  qos=1 retain=0  {"type": "door", "device": "d-17", "open": true}
# In:  ingest  qos=1 retain=0  {"type": "door", "device": "x/../admin/cmd", "open": true}
# (nothing is published for it)
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.route_by_type]
description = "Fan the shared ingest topic out to devices/<kind>/<device>"
sql = '''
SELECT
  CASE payload.type WHEN 'door' THEN 'door' WHEN 'meter' THEN 'meter' ELSE 'other' END AS kind,
  payload.device AS device,
  payload
FROM "ingest"
WHERE is_str(payload.device) AND regex_match(payload.device, '^[A-Za-z0-9_-]{1,64}$')
'''
actions = [
  { function = "republish", args = { topic = "devices/${kind}/${device}", qos = 1, payload = "${payload}" } },
]
```

Try it. The last two messages are the ones the guard is for:

```sh
mosquitto_pub -i cell-gw -q 1 -t ingest -m '{"type": "door", "device": "d-17", "open": true}'
mosquitto_pub -i cell-gw -q 1 -t ingest -m '{"type": "meter", "device": "m-3", "kwh": 12.5}'
mosquitto_pub -i cell-gw -q 1 -t ingest -m '{"type": "valve", "device": "v-1", "pos": 40}'
mosquitto_pub -i cell-gw -q 1 -t ingest -m '{"type": "door", "device": "x/../admin/cmd", "open": true}'
mosquitto_pub -i cell-gw -q 1 -t ingest -m '{"type": "door", "open": true}'
```

The watcher prints:

```text
ingest  qos=1 retain=0  {"type": "door", "device": "d-17", "open": true}
devices/door/d-17  qos=1 retain=0  {"type": "door", "device": "d-17", "open": true}
ingest  qos=1 retain=0  {"type": "meter", "device": "m-3", "kwh": 12.5}
devices/meter/m-3  qos=1 retain=0  {"type": "meter", "device": "m-3", "kwh": 12.5}
ingest  qos=1 retain=0  {"type": "valve", "device": "v-1", "pos": 40}
devices/other/v-1  qos=1 retain=0  {"type": "valve", "device": "v-1", "pos": 40}
ingest  qos=1 retain=0  {"type": "door", "device": "x/../admin/cmd", "open": true}
ingest  qos=1 retain=0  {"type": "door", "open": true}
```

**Gotcha:** whenever a value from the payload, a user property or the client id becomes a
level of the topic a rule publishes on, guard it like this. What a rule publishes is not
checked against the publisher's ACL. Without the guard, the fourth message would be
published on `devices/door/x/../admin/cmd`, a topic the device chose, and the fifth on
`devices/door/undefined`. A value taken from the topic the device published on
(`topic(2)`) is always one plain level, so the other recipes use that where they can.

---

## 8. Add who, where and when

**Problem:** stamp each reading with the client, its address, the broker node that took it,
a message id, and the time it arrived.

```toml
# Recipe 08: enrich a reading with who sent it, from where, and when
#
# Stamps each counter reading with the publisher's client id, username, IP address, the
# broker node that received it, a unique message id, and the time it arrived: in
# milliseconds, as RFC 3339, and as the plant's local time. The local time uses a
# fixed offset (+02:00); there are no time-zone rules, so daylight saving is not applied.
# A client that sent no username has none: coalesce() gives it a value, where the JSON
# would otherwise carry the text "undefined".
#
# The id and the times differ on every message.
#
# In:  plant/line-a/counter  qos=0 retain=0  {"count": 1042}
# Out: enriched/line-a/counter  qos=0 retain=0  {"line":"line-a","count":1042,"meta":{"clientid":"plc-12","username":"anonymous","ip":"127.0.0.1","broker":"node-local","msg_id":"00065D42D9C3E9B22670C4A000000000","received_ms":1791391562983,"received_at":"2026-10-07T16:46:02.983+00:00","plant_time":"2026-10-07 18:46:02"}}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.enrich_counter]
description = "Attach client, network and time metadata"
sql = '''
SELECT
  topic(2)      AS line,
  payload.count AS count,
  clientid      AS meta.clientid,
  coalesce(username, 'anonymous') AS meta.username,
  peerhost      AS meta.ip,
  node          AS meta.broker,
  id            AS meta.msg_id,
  timestamp     AS meta.received_ms,
  unix_ts_to_rfc3339(timestamp, 'millisecond') AS meta.received_at,
  format_date('millisecond', '+02:00', '%Y-%m-%d %H:%M:%S', timestamp) AS meta.plant_time
FROM "plant/+/counter"
'''
actions = [
  { function = "republish", args = { topic = "enriched/${line}/counter", payload = "${.}" } },
]
```

Try it:

```sh
mosquitto_pub -i plc-12 -t plant/line-a/counter -m '{"count": 1042}'
```

The watcher prints the following. The id and the three times differ on every run, and the
three times always agree with each other:

```text
plant/line-a/counter  qos=0 retain=0  {"count": 1042}
enriched/line-a/counter  qos=0 retain=0  {"line":"line-a","count":1042,"meta":{"clientid":"plc-12","username":"anonymous","ip":"127.0.0.1","broker":"node-local","msg_id":"00065D42D9C3E9B22670C4A000000000","received_ms":1791391562983,"received_at":"2026-10-07T16:46:02.983+00:00","plant_time":"2026-10-07 18:46:02"}}
```

**Gotchas:** a field that has no value, like `username` for a client that sent none, is
`undefined`, and in JSON that becomes the text `"undefined"`; `coalesce()` replaces it.
`format_date` takes a fixed offset (`+02:00`), not a time zone, so it does not follow
daylight saving time.

---

## 9. Move to a new topic namespace

**Problem:** firmware is moving from `v1/<site>/<device>/<metric>` to
`sites/<site>/devices/<device>/<metric>`, and subscribers need the new tree before every
device has moved.

```toml
# Recipe 09: migrate a topic namespace, keeping QoS and the retain flag
#
# Firmware is moving from v1/<site>/<device>/<metric> to
# sites/<site>/devices/<device>/<metric>. Until every subscriber has moved, this rule
# mirrors the old tree into the new one for two pilot sites, keeping the payload bytes,
# the QoS and the retain flag. It must SELECT qos and flags.retain: the republish
# defaults read the rule's output, so without them the copy would be QoS 0 and never
# retained (see recipe 13).
#
# In:   v1/munich/th-9/state  qos=2 retain=1  ON
# Out:  sites/munich/devices/th-9/state  qos=2 retain=0  ON
# In:   v1/paris/th-4/temp  qos=1 retain=0  19.0
# (nothing: paris is not a pilot site)
#
# A subscriber that was already connected sees retain=0 on the copy, as MQTT delivers
# any live message. One that subscribes later gets the retained copy:
#
# Late: sites/munich/devices/th-9/state  qos=2 retain=1  ON
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.v1_to_v2_topics]
description = "Mirror legacy v1 topics into the v2 tree for berlin and munich"
sql = '''
SELECT
  topic(2) AS site, topic(3) AS device, topic(4) AS metric,
  payload, qos, flags.retain AS retain
FROM "v1/+/+/+"
WHERE topic(2) IN ('berlin', 'munich')
'''
actions = [
  { function = "republish", args = { topic = "sites/${site}/devices/${device}/${metric}", qos = "${qos}", retain = "${retain}", payload = "${payload}" } },
]
```

Try it. The second message is published at QoS 2 with the retain flag:

```sh
mosquitto_pub -i fw-1 -q 1 -t v1/berlin/th-1/temp -m 21.5
mosquitto_pub -i fw-1 -q 2 -r -t v1/munich/th-9/state -m ON
mosquitto_pub -i fw-1 -q 1 -t v1/paris/th-4/temp -m 19.0
```

The watcher prints:

```text
v1/berlin/th-1/temp  qos=1 retain=0  21.5
sites/berlin/devices/th-1/temp  qos=1 retain=0  21.5
v1/munich/th-9/state  qos=2 retain=0  ON
sites/munich/devices/th-9/state  qos=2 retain=0  ON
v1/paris/th-4/temp  qos=1 retain=0  19.0
```

A subscriber that connects afterwards gets the retained copy, and the retained original
too. Retained messages arrive in no particular order:

```sh
mosquitto_sub -q 2 -t '#' -F '%t  qos=%q retain=%r  %p'
```

```text
sites/munich/devices/th-9/state  qos=2 retain=1  ON
v1/munich/th-9/state  qos=2 retain=1  ON
```

**Gotcha:** the watcher shows `retain=0` on both munich messages. MQTT clears the retain
flag on a message delivered live to an existing subscription (unless an MQTT 5
subscription asks for Retain As Published), and sets it on a retained message sent to a new
subscription.

---

## 10. Thin out a stream without state

**Problem:** a vibration sensor reports several times a second, and a dashboard needs far
less.

```toml
# Recipe 10: thin out a high-rate stream without state
#
# A rule sees one message at a time and remembers nothing, so it can only thin a stream
# by something in the message itself:
#
# - decimate_by_seq keeps every 10th reading, using the device's own sequence counter,
#   on vib/<device>/1in10.
# - fleet_sample keeps every reading from a stable ~25% of the devices (the same ones
#   every time: the hash of the device id decides), on debug/sample/<device>.
#
# What needs state is not possible in a rule: an average or a min/max over a window,
# counting, deduplication, "only when the value changed" (a deadband), a per-device rate
# limit, or exactly one message per interval. docs/RULES-COOKBOOK.md says what to use.
#
# In:  vib/pump-1/raw  qos=0 retain=0  {"seq": 10, "v": 0.42}
# Out: vib/pump-1/1in10  qos=0 retain=0  {"device":"pump-1","seq":10,"v":0.42}
# In:  vib/pump-2/raw  qos=0 retain=0  {"seq": 21, "v": 0.37}
# Out: debug/sample/pump-2  qos=0 retain=0  {"device":"pump-2","bucket":21,"seq":21,"v":0.37}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.decimate_by_seq]
description = "Every 10th reading"
sql = '''
SELECT topic(2) AS device, payload.seq AS seq, payload.v AS v
FROM "vib/+/raw"
WHERE payload.seq mod 10 = 0
'''
actions = [
  { function = "republish", args = { topic = "vib/${device}/1in10", payload = "${.}" } },
]

[rules.fleet_sample]
description = "Every reading from a stable quarter of the fleet"
sql = '''
SELECT
  topic(2) AS device,
  hash_to_range(topic(2), 1, 100) AS bucket,
  payload.seq AS seq,
  payload.v AS v
FROM "vib/+/raw"
WHERE bucket <= 25
'''
actions = [
  { function = "republish", args = { topic = "debug/sample/${device}", payload = "${.}" } },
]
```

Try it. `pump-1` hashes to bucket 59, outside the sample; `pump-2` hashes to 21:

```sh
mosquitto_pub -i pump-1 -t vib/pump-1/raw -m '{"seq": 9, "v": 0.38}'
mosquitto_pub -i pump-1 -t vib/pump-1/raw -m '{"seq": 10, "v": 0.42}'
mosquitto_pub -i pump-1 -t vib/pump-1/raw -m '{"seq": 11, "v": 0.4}'
mosquitto_pub -i pump-2 -t vib/pump-2/raw -m '{"seq": 20, "v": 0.35}'
mosquitto_pub -i pump-2 -t vib/pump-2/raw -m '{"seq": 21, "v": 0.37}'
```

The watcher prints:

```text
vib/pump-1/raw  qos=0 retain=0  {"seq": 9, "v": 0.38}
vib/pump-1/raw  qos=0 retain=0  {"seq": 10, "v": 0.42}
vib/pump-1/1in10  qos=0 retain=0  {"device":"pump-1","seq":10,"v":0.42}
vib/pump-1/raw  qos=0 retain=0  {"seq": 11, "v": 0.4}
vib/pump-2/raw  qos=0 retain=0  {"seq": 20, "v": 0.35}
vib/pump-2/1in10  qos=0 retain=0  {"device":"pump-2","seq":20,"v":0.35}
debug/sample/pump-2  qos=0 retain=0  {"device":"pump-2","bucket":21,"seq":20,"v":0.35}
vib/pump-2/raw  qos=0 retain=0  {"seq": 21, "v": 0.37}
debug/sample/pump-2  qos=0 retain=0  {"device":"pump-2","bucket":21,"seq":21,"v":0.37}
```

**Gotcha:** a rule sees one message at a time and keeps nothing between messages. Averages,
minimum or maximum over a window, counts, deduplication, "only when the value changed", a
rate limit per device, or exactly one message per interval all need state. See
[What rules cannot do](#what-rules-cannot-do).

---

## 11. Device presence from connect and disconnect events

**Problem:** a dashboard needs every device's current status, online or offline and why,
including when it connects after the devices did.

```toml
# Recipe 11: device presence from connect and disconnect events, as retained status
#
# presence/<client id> always holds a device's current status, retained, so a dashboard
# that connects later gets every device's status at once. presence_online publishes
# "online" when a device connects; presence_offline publishes "offline" with the reason
# when it goes away: normal (it sent DISCONNECT), tcp_closed (the connection dropped),
# keepalive_timeout (it went silent) or shutdown (the broker is stopping).
#
# Both WHERE clauses track devices only (client ids starting "sensor-", not your
# dashboards or services), and keep the client id, which becomes a topic level, to one
# plain level.
#
# presence_offline skips takenover and discarded. When a device reconnects while the
# broker still holds its old connection (a takeover: say its network changed), the OLD
# connection ends with takenover (discarded when the new one asks for a clean start),
# and that event can arrive after the new connection's "online": publishing it would
# mark a connected device offline. Every other end is published with its reason, as
# EMQX names it: kicked (an operator disconnected it), not_authorized (its credentials
# were revoked), protocol_error and the like (it broke the protocol).
#
# "since" is the time of the event in milliseconds, so it differs on every run.
#
# Out:  presence/sensor-a  qos=1 retain=0  {"clientid":"sensor-a","status":"online","since":1791391609588}
# Out:  presence/sensor-a  qos=1 retain=0  {"clientid":"sensor-a","status":"offline","since":1791391609589,"reason":"normal"}
# Late: presence/sensor-b  qos=1 retain=1  {"clientid":"sensor-b","status":"offline","since":1791391610989,"reason":"tcp_closed"}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.presence_online]
description = "A device connected"
sql = '''
SELECT clientid, 'online' AS status, connected_at AS since
FROM "$events/client/connected"
WHERE regex_match(clientid, '^sensor-[A-Za-z0-9_-]{1,57}$')
'''
actions = [
  { function = "republish", args = { topic = "presence/${clientid}", qos = 1, retain = true, payload = "${.}" } },
]

[rules.presence_offline]
description = "A device went away"
sql = '''
SELECT clientid, 'offline' AS status, disconnected_at AS since, reason
FROM "$events/client/disconnected"
WHERE regex_match(clientid, '^sensor-[A-Za-z0-9_-]{1,57}$') AND reason <> 'takenover' AND reason <> 'discarded'
'''
actions = [
  { function = "republish", args = { topic = "presence/${clientid}", qos = 1, retain = true, payload = "${.}" } },
]
```

Watch the presence topics instead of everything:

```sh
mosquitto_sub -q 2 -t 'presence/#' -F '%t  qos=%q retain=%r  %p'
```

Then, in one shell (so that `%1` is the first device):

```sh
mosquitto_sub -i sensor-b -t 'cmd/sensor-b' &
mosquitto_sub -i sensor-c -t 'cmd/sensor-c' &
mosquitto_pub -i sensor-a -t telemetry/sensor-a -m 21.5
kill -9 %1
```

`sensor-b` and `sensor-c` stay connected. `sensor-a` connects, publishes and disconnects
cleanly. Then `sensor-b` dies without a DISCONNECT. The watcher prints the following; the
times in `since` differ on every run:

```text
presence/sensor-b  qos=1 retain=0  {"clientid":"sensor-b","status":"online","since":1791391608985}
presence/sensor-c  qos=1 retain=0  {"clientid":"sensor-c","status":"online","since":1791391609287}
presence/sensor-a  qos=1 retain=0  {"clientid":"sensor-a","status":"online","since":1791391609588}
presence/sensor-a  qos=1 retain=0  {"clientid":"sensor-a","status":"offline","since":1791391609589,"reason":"normal"}
presence/sensor-b  qos=1 retain=0  {"clientid":"sensor-b","status":"offline","since":1791391610989,"reason":"tcp_closed"}
```

A dashboard that connects afterwards gets every device's status at once, in no particular
order:

```sh
mosquitto_sub -q 2 -t 'presence/#' -F '%t  qos=%q retain=%r  %p'
```

```text
presence/sensor-a  qos=1 retain=1  {"clientid":"sensor-a","status":"offline","since":1791391609589,"reason":"normal"}
presence/sensor-b  qos=1 retain=1  {"clientid":"sensor-b","status":"offline","since":1791391610989,"reason":"tcp_closed"}
presence/sensor-c  qos=1 retain=1  {"clientid":"sensor-c","status":"online","since":1791391609287}
```

**Gotcha: takeovers.** When a device connects again while the broker still holds its old
connection (its network changed, say), the old connection ends with the reason
`takenover` (`discarded` when the new connection asks for a clean start), and that event
can arrive after the new connection's "online". A rule that published it would mark a
connected device offline. This recipe skips both, as an EMQX presence rule does, and the
test proves that a takeover leaves the device online. Every other end is published with
EMQX's reason: `kicked` when an operator disconnects it, `not_authorized` when its
credentials are revoked, `protocol_error` and the like when it breaks the protocol. A
device that goes silent is marked offline with `keepalive_timeout` once one and a
half keepalive intervals pass without a packet from it.

---

## 12. Keep the last known state, and clear it

**Problem:** an app screen should show each device's latest reading the moment it opens,
and a decommissioned device should disappear.

```toml
# Recipe 12: a retained "last known state" per device, and clearing it
#
# last_known_state copies each device's telemetry to state/<device>, retained, so an
# app that subscribes later gets every device's latest reading at once. clear_state
# deletes it when a device is decommissioned: anything published to
# devices/<device>/decommission publishes an EMPTY retained message to state/<device>,
# which is how MQTT deletes a retained message.
#
# The empty payload needs a trick: payload = "" does NOT mean empty (an empty template
# is the whole output as JSON), so the rule selects an empty string and publishes that.
#
# A subscriber that is connected sees each copy with retain=0, as MQTT delivers any
# live message; one that subscribes later gets the retained copies.
#
# In:   devices/d1/telemetry  qos=1 retain=0  {"temp": 23}
# Out:  state/d1  qos=1 retain=0  {"temp": 23}
# In:   devices/d3/decommission  qos=1 retain=0
# Out:  state/d3  qos=1 retain=0
# Late: state/d1  qos=1 retain=1  {"temp": 23}
# (and nothing for d3: its retained state was deleted)
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.last_known_state]
description = "Keep each device's latest telemetry, retained"
sql = 'SELECT topic(2) AS device, payload FROM "devices/+/telemetry"'
actions = [
  { function = "republish", args = { topic = "state/${device}", qos = 1, retain = true, payload = "${payload}" } },
]

[rules.clear_state]
description = "Delete a decommissioned device's retained state"
sql = '''SELECT topic(2) AS device, '' AS empty FROM "devices/+/decommission"'''
actions = [
  { function = "republish", args = { topic = "state/${device}", qos = 1, retain = true, payload = "${empty}" } },
]
```

Try it. The last command publishes an empty message (`-n`) to decommission `d3`:

```sh
mosquitto_pub -i d1 -q 1 -t devices/d1/telemetry -m '{"temp": 20}'
mosquitto_pub -i d1 -q 1 -t devices/d1/telemetry -m '{"temp": 23}'
mosquitto_pub -i d2 -q 1 -t devices/d2/telemetry -m '{"temp": 30}'
mosquitto_pub -i d3 -q 1 -t devices/d3/telemetry -m '{"temp": 31}'
mosquitto_pub -i ops -q 1 -t devices/d3/decommission -n
```

The watcher prints the following. The last two lines have empty payloads:

```text
devices/d1/telemetry  qos=1 retain=0  {"temp": 20}
state/d1  qos=1 retain=0  {"temp": 20}
devices/d1/telemetry  qos=1 retain=0  {"temp": 23}
state/d1  qos=1 retain=0  {"temp": 23}
devices/d2/telemetry  qos=1 retain=0  {"temp": 30}
state/d2  qos=1 retain=0  {"temp": 30}
devices/d3/telemetry  qos=1 retain=0  {"temp": 31}
state/d3  qos=1 retain=0  {"temp": 31}
devices/d3/decommission  qos=1 retain=0
state/d3  qos=1 retain=0
```

A subscriber that connects afterwards gets `d1`'s latest reading and `d2`'s, in no
particular order, and nothing for `d3`:

```sh
mosquitto_sub -q 2 -t 'state/#' -F '%t  qos=%q retain=%r  %p'
```

```text
state/d1  qos=1 retain=1  {"temp": 23}
state/d2  qos=1 retain=1  {"temp": 30}
```

**Gotcha:** `payload = ""` does not publish an empty payload. An empty template means the
whole output as JSON, so `clear_state` selects an empty string and publishes `${empty}`.

---

## 13. Choose the QoS of what a rule publishes

**Problem:** an alarm inside cheap QoS 0 telemetry must not be lost, a dashboard copy can
be fire-and-forget, and a mirror should keep the device's QoS.

```toml
# Recipe 13: choose the QoS of what a rule publishes (and the default trap)
#
# A republish's qos defaults to "${qos}", and that placeholder reads the rule's OUTPUT,
# not the incoming message. So a rule that does not SELECT qos publishes at QoS 0,
# whatever QoS the device used. Set it on purpose:
#
# - alarm_upgrade: telemetry arrives at QoS 0 because it is cheap, but an alarm inside
#   it must not be lost, so the alarm goes out at QoS 2.
# - dashboard_copy: a live dashboard copy can be fire-and-forget, so it is QoS 0 even
#   when the reading arrived at QoS 2.
# - mirror_keep_qos: the copy keeps the device's QoS, because the rule SELECTs qos.
# - mirror_forgot_qos: the same rule without qos in its SELECT. This is the trap: a QoS
#   2 reading is copied at QoS 0.
#
# In:  tele/t-1/data  qos=2 retain=0  {"alarm": false, "v": 7}
# Out: dash/t-1  qos=0 retain=0  {"alarm": false, "v": 7}
# Out: mirror_gotcha/t-1  qos=0 retain=0  {"alarm": false, "v": 7}
# Out: mirror/t-1  qos=2 retain=0  {"alarm": false, "v": 7}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.alarm_upgrade]
description = "An alarm found in QoS 0 telemetry goes out at QoS 2"
sql = '''
SELECT topic(2) AS device, payload.code AS code
FROM "tele/+/data"
WHERE payload.alarm = true
'''
actions = [
  { function = "republish", args = { topic = "alarms/${device}", qos = 2, payload = "${.}" } },
]

[rules.dashboard_copy]
description = "A fire-and-forget copy for a live dashboard"
sql = 'SELECT topic(2) AS device, payload FROM "tele/+/data"'
actions = [
  { function = "republish", args = { topic = "dash/${device}", qos = 0, payload = "${payload}" } },
]

[rules.mirror_keep_qos]
description = "A copy at the device's own QoS: qos is selected"
sql = 'SELECT topic(2) AS device, payload, qos FROM "tele/+/data"'
actions = [
  { function = "republish", args = { topic = "mirror/${device}", payload = "${payload}" } },
]

[rules.mirror_forgot_qos]
description = "THE TRAP: the same copy without qos in the SELECT is always QoS 0"
sql = 'SELECT topic(2) AS device, payload FROM "tele/+/data"'
actions = [
  { function = "republish", args = { topic = "mirror_gotcha/${device}", payload = "${payload}" } },
]
```

Try it. The first message is QoS 0, the second QoS 2:

```sh
mosquitto_pub -i t-1 -t tele/t-1/data -m '{"alarm": true, "code": "E42"}'
mosquitto_pub -i t-1 -q 2 -t tele/t-1/data -m '{"alarm": false, "v": 7}'
```

The broker sends these, in this order within QoS 0 and within QoS 1 and 2 (see
[the note on order](#run-any-recipe-in-under-a-minute); your screen may show the QoS 0
lines first):

```text
tele/t-1/data  qos=0 retain=0  {"alarm": true, "code": "E42"}
alarms/t-1  qos=2 retain=0  {"device":"t-1","code":"E42"}
dash/t-1  qos=0 retain=0  {"alarm": true, "code": "E42"}
mirror_gotcha/t-1  qos=0 retain=0  {"alarm": true, "code": "E42"}
mirror/t-1  qos=0 retain=0  {"alarm": true, "code": "E42"}
tele/t-1/data  qos=2 retain=0  {"alarm": false, "v": 7}
dash/t-1  qos=0 retain=0  {"alarm": false, "v": 7}
mirror_gotcha/t-1  qos=0 retain=0  {"alarm": false, "v": 7}
mirror/t-1  qos=2 retain=0  {"alarm": false, "v": 7}
```

**Gotcha: the default is QoS 0.** A republish's `qos` defaults to `${qos}`, and a
placeholder reads the rule's **output**, not the incoming message. A rule that does not
select `qos` has no `qos` in its output, so `mirror_forgot_qos` copies the QoS 2 reading at
QoS 0. Either select `qos` (`mirror_keep_qos`) or set a number (`alarm_upgrade`,
`dashboard_copy`). The retain flag works the same way: see
[recipe 9](#9-move-to-a-new-topic-namespace).

---

## 14. Parse CSV and key=value text

**Problem:** older devices send `time,device,temp,running` lines or `temp=21.5;state=ON`
strings, and everything downstream wants JSON with numbers and nulls.

```toml
# Recipe 14: parse CSV and key=value payloads into JSON
#
# Older devices send text, not JSON. Both rules build the JSON they publish under one
# alias prefix (out.time, out.temp, ...) and publish just that part with "${out}", so
# the intermediate values (f, kv) stay out of the message.
#
# csv_to_json reads "time,device,temp,running". split(..., 'notrim') keeps empty
# fields, so a missing value never shifts the columns; an empty temp becomes a real
# JSON null (there is no null literal: json_decode('null') makes one).
#
# kv_to_json reads "temp=21.5;state=ON;bat=3.61": it rewrites the text into a JSON
# object of strings, decodes it once, then converts each value. A key that is missing
# gives null (or a default), never a failed rule. A value must not contain '"' or '\'.
#
# In:  legacy/logger-1/csv  qos=0 retain=0  2026-10-07T12:00:05Z,pump-8,,0
# Out: parsed/logger-1/csv  qos=0 retain=0  {"time":"2026-10-07T12:00:05Z","device":"pump-8","temp":null,"running":false}
# In:  legacy/logger-2/kv  qos=0 retain=0  temp=21.5;state=ON;bat=3.61
# Out: parsed/logger-2/kv  qos=0 retain=0  {"temp":21.5,"battery_v":3.61,"state":"ON"}
# In:  legacy/logger-2/kv  qos=0 retain=0  state=ON
# Out: parsed/logger-2/kv  qos=0 retain=0  {"temp":null,"battery_v":null,"state":"ON"}
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.csv_to_json]
description = "CSV lines to JSON"
sql = '''
SELECT
  split(payload, ',', 'notrim') AS f,
  topic(2) AS logger,
  nth(1, f) AS out.time,
  nth(2, f) AS out.device,
  CASE WHEN nth(3, f) = '' THEN json_decode('null') ELSE float(nth(3, f)) END AS out.temp,
  nth(4, f) = '1' AS out.running
FROM "legacy/+/csv"
'''
actions = [
  { function = "republish", args = { topic = "parsed/${logger}/csv", payload = "${out}" } },
]

[rules.kv_to_json]
description = "key=value pairs to JSON"
sql = '''
SELECT
  topic(2) AS logger,
  json_decode(concat('{', regex_replace(regex_replace(payload, '([^;=]+)=([^;]*)', '"\1":"\2"'), ';', ','), '}')) AS kv,
  CASE WHEN is_null(kv.temp) THEN json_decode('null') ELSE float(kv.temp) END AS out.temp,
  CASE WHEN is_null(kv.bat) THEN json_decode('null') ELSE float(kv.bat) END AS out.battery_v,
  coalesce(kv.state, 'unknown') AS out.state
FROM "legacy/+/kv"
'''
actions = [
  { function = "republish", args = { topic = "parsed/${logger}/kv", payload = "${out}" } },
]
```

Try it. The third line is missing two columns, the fifth message has no `state`, and the
last one has only a `state`:

```sh
mosquitto_pub -i logger-1 -t legacy/logger-1/csv -m '2026-10-07T12:00:00Z,pump-7,48.2,1'
mosquitto_pub -i logger-1 -t legacy/logger-1/csv -m '2026-10-07T12:00:05Z,pump-8,,0'
mosquitto_pub -i logger-1 -t legacy/logger-1/csv -m '2026-10-07T12:00:10Z,pump-9'
mosquitto_pub -i logger-2 -t legacy/logger-2/kv -m 'temp=21.5;state=ON;bat=3.61'
mosquitto_pub -i logger-2 -t legacy/logger-2/kv -m 'temp=-3.0;bat=3.20'
mosquitto_pub -i logger-2 -t legacy/logger-2/kv -m state=ON
```

The watcher prints:

```text
legacy/logger-1/csv  qos=0 retain=0  2026-10-07T12:00:00Z,pump-7,48.2,1
parsed/logger-1/csv  qos=0 retain=0  {"time":"2026-10-07T12:00:00Z","device":"pump-7","temp":48.2,"running":true}
legacy/logger-1/csv  qos=0 retain=0  2026-10-07T12:00:05Z,pump-8,,0
parsed/logger-1/csv  qos=0 retain=0  {"time":"2026-10-07T12:00:05Z","device":"pump-8","temp":null,"running":false}
legacy/logger-1/csv  qos=0 retain=0  2026-10-07T12:00:10Z,pump-9
legacy/logger-2/kv  qos=0 retain=0  temp=21.5;state=ON;bat=3.61
parsed/logger-2/kv  qos=0 retain=0  {"temp":21.5,"battery_v":3.61,"state":"ON"}
legacy/logger-2/kv  qos=0 retain=0  temp=-3.0;bat=3.20
parsed/logger-2/kv  qos=0 retain=0  {"temp":-3.0,"battery_v":3.2,"state":"unknown"}
legacy/logger-2/kv  qos=0 retain=0  state=ON
parsed/logger-2/kv  qos=0 retain=0  {"temp":null,"battery_v":null,"state":"ON"}
```

**Gotchas:** `nth()` counts from 1 and fails past the end of the array, so a CSV line with
a missing column fails `csv_to_json`: it publishes nothing for that line, and the failure is
counted and logged. Without `'notrim'`, `split()` drops empty fields and every later column
would shift. There is no `null` literal: `null` in SQL is read as a field name, so
`json_decode('null')` makes a JSON null.

---

## 15. Decode base64, hex and binary payloads

**Problem:** one integration sends JSON wrapped in base64, a LoRaWAN network server sends
the device's bytes as base64, and a constrained device sends raw bytes.

```toml
# Recipe 15: decode base64, hex and raw binary payloads
#
# unwrap_base64_json: a cloud service pushes JSON wrapped in base64 inside a JSON
#   envelope; publish the inner JSON on unwrapped/<device>. The device id comes from
#   the payload and becomes a topic level, so the WHERE lets only a plain id through.
# lorawan_uplink: a LoRaWAN network server's uplink carries the device's bytes as base64
#   frm_payload; publish them as hex, with the port and the signal strength.
# decode_binary_frame: a constrained device sends 3 raw bytes, [type: u8][temperature in
#   hundredths of a degree: i16, big-endian]. There is no function that reads bits, so
#   the rule reads the hex digits: 16 - strlen(find('0123456789ABCDEF', d)) is the value
#   of hex digit d. It publishes the decoded reading, and archives the raw bytes
#   unchanged.
#
# Payloads that are not text are shown as hex (01 09 c4 is "0109c4").
#
# In:  bin/nb-1/up  qos=0 retain=0  02ff9c
# Out: decoded/nb-1  qos=0 retain=0  {"type":2,"temp_c":-1.0,"raw":"Av+c"}
# Out: archive/nb-1  qos=0 retain=0  02ff9c
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.unwrap_base64_json]
description = "JSON wrapped in base64"
sql = '''
SELECT payload.deviceId AS device, json_decode(base64_decode(payload.data)) AS data
FROM "cloud/push"
WHERE is_str(payload.deviceId) AND regex_match(payload.deviceId, '^[A-Za-z0-9_-]{1,64}$')
'''
actions = [
  { function = "republish", args = { topic = "unwrapped/${device}", payload = "${data}" } },
]

[rules.lorawan_uplink]
description = "A LoRaWAN uplink's base64 frm_payload as hex"
sql = '''
SELECT
  topic(5)                                       AS device,
  payload.uplink_message.f_port                  AS port,
  bin2hexstr(base64_decode(payload.uplink_message.frm_payload)) AS hex,
  payload.uplink_message.rx_metadata[1].rssi     AS rssi
FROM "lora/v3/+/devices/+/up"
'''
actions = [
  { function = "republish", args = { topic = "lora/hex/${device}", payload = "${.}" } },
]

[rules.decode_binary_frame]
description = "A 3-byte binary frame: type, then a signed big-endian temperature"
sql = '''
SELECT
  topic(2) AS device,
  payload,                         -- selected, so ${payload} below is the raw bytes
  base64_encode(payload) AS raw_b64,
  bin2hexstr(payload) AS hex,      -- 02 FF 9C -> '02FF9C'
  (16 - strlen(find('0123456789ABCDEF', substr(hex, 0, 1)))) * 16
    + (16 - strlen(find('0123456789ABCDEF', substr(hex, 1, 1)))) AS frame_type,
  (16 - strlen(find('0123456789ABCDEF', substr(hex, 2, 1)))) * 4096
    + (16 - strlen(find('0123456789ABCDEF', substr(hex, 3, 1)))) * 256
    + (16 - strlen(find('0123456789ABCDEF', substr(hex, 4, 1)))) * 16
    + (16 - strlen(find('0123456789ABCDEF', substr(hex, 5, 1)))) AS raw_temp,
  CASE WHEN raw_temp >= 32768 THEN raw_temp - 65536 ELSE raw_temp END / 100 AS temp_c
FROM "bin/+/up"
WHERE strlen(hex) = 6
'''
actions = [
  { function = "republish", args = { topic = "decoded/${device}", payload = '{"type":${frame_type},"temp_c":${temp_c},"raw":"${raw_b64}"}' } },
  { function = "republish", args = { topic = "archive/${device}", payload = "${payload}" } },
]
```

Try it. `printf` writes the raw bytes `01 09 c4` and `02 ff 9c`, and `-s` sends standard
input as the payload:

```sh
mosquitto_pub -i push-svc -t cloud/push -m '{"deviceId": "door-5", "data": "eyJ0ZW1wIjogMjIuNSwgImRvb3IiOiAiY2xvc2VkIn0="}'
mosquitto_pub -i lns -t lora/v3/myapp/devices/lht-1/up -m '{"end_device_ids": {"device_id": "lht-1"}, "uplink_message": {"f_port": 2, "frm_payload": "AQnE", "rx_metadata": [{"gateway_ids": {"gateway_id": "gw-a"}, "rssi": -97}]}}'
printf '\001\011\304' | mosquitto_pub -i nb-1 -t bin/nb-1/up -s
printf '\002\377\234' | mosquitto_pub -i nb-1 -t bin/nb-1/up -s
```

The watcher prints the following. The payloads that are not text are shown here in hex, as
`%x` in the `-F` format prints them:

```text
cloud/push  qos=0 retain=0  {"deviceId": "door-5", "data": "eyJ0ZW1wIjogMjIuNSwgImRvb3IiOiAiY2xvc2VkIn0="}
unwrapped/door-5  qos=0 retain=0  {"temp":22.5,"door":"closed"}
lora/v3/myapp/devices/lht-1/up  qos=0 retain=0  {"end_device_ids": {"device_id": "lht-1"}, "uplink_message": {"f_port": 2, "frm_payload": "AQnE", "rx_metadata": [{"gateway_ids": {"gateway_id": "gw-a"}, "rssi": -97}]}}
lora/hex/lht-1  qos=0 retain=0  {"device":"lht-1","port":2,"hex":"0109C4","rssi":-97}
bin/nb-1/up  qos=0 retain=0  0109c4
decoded/nb-1  qos=0 retain=0  {"type":1,"temp_c":25.0,"raw":"AQnE"}
archive/nb-1  qos=0 retain=0  0109c4
bin/nb-1/up  qos=0 retain=0  02ff9c
decoded/nb-1  qos=0 retain=0  {"type":2,"temp_c":-1.0,"raw":"Av+c"}
archive/nb-1  qos=0 retain=0  02ff9c
```

**Gotchas:** a `${...}` placeholder reads the rule's output, so `decode_binary_frame`
selects `payload` for the archive copy; without it, the archive would contain the text
`undefined`. `bin2hexstr` gives upper-case hex. JSON holds text, not raw bytes, so `${.}`
on an output with a binary value writes U+FFFD for each byte sequence that is not UTF-8,
as EMQX does: encode the bytes first, as `raw_b64` does.

---

## 16. MQTT 5 user properties in and out

**Problem:** route orders by a `tenant` user property, add a property of your own, set
Content-Type and an expiry, and keep an audit copy with every original property.

```toml
# Recipe 16: MQTT 5 user properties and message properties, in and out
#
# order_audit_copy keeps an audit copy of every order on audit/orders, carrying every
#   user property of the original unchanged (in order, repeated keys included) and its
#   Content-Type. A publisher that sent no Content-Type (an MQTT 3.1.1 client, say)
#   would otherwise get the literal text "undefined", so coalesce() gives a default.
# order_router routes an order by its "tenant" user property to tenants/<tenant>/orders,
#   adds a "processed-by" property, and sets Content-Type and a one-hour expiry. The
#   tenant comes from the publisher and becomes a topic level, so the WHERE lets only a
#   plain id through. pub_props.'User-Property' is a map, so it keeps only the LAST
#   value of a repeated key: here the two "tag" properties become one.
#
# In:  orders/new  qos=1 retain=0  {"order": 1001, "sku": "A-7"}
#      (MQTT 5, user properties tenant=acme, trace-id=abc123, tag=red, tag=fragile,
#       Content-Type application/vnd.shop+json)
# Out: audit/orders  qos=1 retain=0  {"order": 1001, "sku": "A-7"}
#      (user properties tenant=acme, trace-id=abc123, tag=red, tag=fragile,
#       Content-Type application/vnd.shop+json)
# Out: tenants/acme/orders  qos=1 retain=0  {"order": 1001, "sku": "A-7"}
#      (user properties tenant=acme, trace-id=abc123, tag=fragile, processed-by=order_router,
#       Content-Type application/json, Message-Expiry-Interval 3600)
#
# Tested by crates/mqttd/tests/rules_cookbook.rs; explained in docs/RULES-COOKBOOK.md.

[rules.order_audit_copy]
description = "Audit copy with every original property"
sql = '''
SELECT
  payload,
  pub_props,
  coalesce(pub_props.'Content-Type', 'application/octet-stream') AS content_type
FROM "orders/+"
'''
actions = [
  { function = "republish", args = {
      topic = "audit/orders", qos = 1, payload = "${payload}",
      user_properties = "${pub_props.'User-Property'}",
      mqtt_properties = { "Content-Type" = "${content_type}" } } },
]

[rules.order_router]
description = "Route orders by their tenant user property"
sql = '''
SELECT
  pub_props.'User-Property'.tenant AS tenant,
  payload,
  map_put('processed-by', 'order_router', pub_props.'User-Property') AS user_properties
FROM "orders/+"
WHERE is_str(tenant) AND regex_match(tenant, '^[A-Za-z0-9_-]{1,64}$')
'''
actions = [
  { function = "republish", args = {
      topic = "tenants/${tenant}/orders", qos = 1, payload = "${payload}",
      user_properties = "${user_properties}",
      mqtt_properties = { "Content-Type" = "application/json", "Message-Expiry-Interval" = "3600" } } },
]
```

Try it. The first order comes from an MQTT 5 client with five properties, the second from
an MQTT 3.1.1 client, which cannot send any:

```sh
mosquitto_pub -V 5 -i shop-1 -q 1 -D publish content-type application/vnd.shop+json -D publish user-property tenant acme -D publish user-property trace-id abc123 -D publish user-property tag red -D publish user-property tag fragile -t orders/new -m '{"order": 1001, "sku": "A-7"}'
mosquitto_pub -i legacy-311 -q 1 -t orders/new -m '{"order": 1002}'
```

The watcher prints:

```text
orders/new  qos=1 retain=0  {"order": 1001, "sku": "A-7"}
audit/orders  qos=1 retain=0  {"order": 1001, "sku": "A-7"}
tenants/acme/orders  qos=1 retain=0  {"order": 1001, "sku": "A-7"}
orders/new  qos=1 retain=0  {"order": 1002}
audit/orders  qos=1 retain=0  {"order": 1002}
```

An MQTT 5 subscriber also receives these properties with each message (with
`mosquitto_sub`, add `-V 5`, and `%P`, `%C` and `%E` to the format to print them):

| Message | User properties, in order | Content-Type | Message expiry |
|---|---|---|---|
| `orders/new` | `tenant=acme` `trace-id=abc123` `tag=red` `tag=fragile` | `application/vnd.shop+json` | none |
| `audit/orders` | `tenant=acme` `trace-id=abc123` `tag=red` `tag=fragile` | `application/vnd.shop+json` | none |
| `tenants/acme/orders` | `tenant=acme` `trace-id=abc123` `tag=fragile` `processed-by=order_router` | `application/json` | 3600 s |
| `orders/new` | none | none | none |
| `audit/orders` | none | `application/octet-stream` | none |

**Gotchas:** `pub_props.'User-Property'` is a map, so a key sent twice keeps only its last
value (`tag=fragile`). To forward every property as sent, use
`user_properties = "${pub_props.'User-Property'}"`, as the audit copy does. And a
placeholder for a property the publisher did not send renders as the text `undefined`, so
give it a default with `coalesce()`.

---

## What rules cannot do

A rule sees one message, or one client event, at a time. It keeps nothing between messages,
publishes only back into the broker, and calls nothing outside it. These need something
else:

| You need | Why a rule cannot | Use instead |
|---|---|---|
| An average, sum, count, minimum or maximum over time | No memory between messages | A consumer in a `$share` group ([INTEGRATION.md](INTEGRATION.md)) that aggregates and publishes the result back, or a time-series database fed by Telegraf |
| Windows: every N seconds, one message per interval, a rate limit per device | No timers and no state | The same consumer, or have the device report at the rate you need |
| Deduplication, or "only when the value changed" | Needs the previous value | The same consumer, or report on change at the device |
| Joining two topics, or looking a value up in a table | A rule sees one message; `maptab_lookup` is not implemented | A consumer with a cache, or put the static data in the topic or the payload at the source |
| Sending to Kafka, HTTP, a database or another system | There are no sink actions ([ADR 0083](adr/0083-rule-engine.md)) | Republish to a topic and consume it with a `$share` group ([INTEGRATION.md](INTEGRATION.md)); to another MQTT broker, [mqtt-bridge](BRIDGE.md) |
| Calling out: an HTTP request, a script, a file or an environment variable | Rules do no I/O; `getenv` reads only `EMQXVAR_…` variables, fixed while the broker runs | A consumer service |
| Dropping or changing the original message | A rule only adds messages | Have devices publish to a raw topic that subscribers cannot read (the ACL), and let a rule republish the cleaned message to the topic they do read |
| Protobuf, Sparkplug B or a schema registry | Those functions are not implemented ([RULES.md](RULES.md)) | A consumer that decodes and republishes |
