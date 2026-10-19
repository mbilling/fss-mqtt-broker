# The rule engine, live: watch the rules run, and change them while they do

[`demo/rules`](../rules/README.md) plays ten simulated minutes of a Danish utility and its
fleet into mqttd and stops. This stack keeps them playing, and opens the broker up so you
can watch what its [rule engine](../../docs/RULES.md) does with each message, and change
the rules while it runs ([ADR 0084](../../docs/adr/0084-watching-and-editing-rules-live.md)):

- **mqttd**, built from this checkout, running the demo's 24 rules. Every 2 s it publishes
  each rule's statistics on `$SYS/brokers/<node>/rules/<id>`, and with the rule trace on it
  publishes what each rule ran on and what it rendered on
  `$SYS/brokers/<node>/trace/rules/<id>`.
- **Three simulators**, power plants, homes and cars
  ([`live.py`](../rules/live.py)), playing the demo's ten minutes over and over on the wall
  clock: wind turbines, SunSpec inverters, DSMR smart meters, heat pumps, EV chargers,
  telematics and OBD-II frames, about four messages a second; and every minute each
  turbine's 50 Hz fast log, a [Parquet file](#turbine-fast-log-parquet) of 0.53 MB.
- **Will scenarios** ([`feeds/wills.py`](feeds/wills.py)): sixteen ways for an MQTT 3.1.1
  or 5 connection to end, each [Will checked against the spec](#wills-checked-against-mqtt),
  a round every two minutes.
- **Every payload format** an MQTT explorer such as
  [fss-mqtt](https://github.com/mbilling/fss-mqtt) decodes, [one retained topic
  each](#every-payload-format-one-topic-each) under `formats/`: Parquet, SQLite, Avro, Arrow,
  MessagePack, CBOR, Protobuf and the rest.
- **[Sparkplug B](#sparkplug-b)** edge nodes on the wind farm and the solar park, with a
  primary host application, playing the whole lifecycle on `spBv1.0/`.
- **A rule editor** at <http://localhost:8070>: the rules with their live counts, each
  rule's trace, the live messages, and an editor that checks, tests and applies a rule
  through the broker's admin API, then shows the new rules arriving on `$SYS`.
- **Your own MQTT client** on `localhost:1883`: every topic the page shows, you can
  subscribe to.

## Run it

You need Docker with Compose v2 (`docker compose`). The first run builds mqttd from source
in release mode, which takes 5-15 minutes; later runs reuse Docker's build cache and start
in seconds.

```sh
demo/rules-live/up.sh
```

With `mqttui`, run it as `mqttui --run demo-rules-live`. `up.sh` checks that the host ports
are free, runs `docker compose up --build -d` (always building, so the image matches the
checkout: an older image would ignore the statistics settings without a word), waits until
the broker is healthy, and prints the addresses and the topics below.

| What | Address | Moved with |
|---|---|---|
| The rule editor | <http://localhost:8070> | `UI_PORT` |
| MQTT, plaintext, anonymous | `127.0.0.1:1883` | `MQTT_PORT`, `MQTT_BIND` |
| Health and Prometheus metrics | <http://localhost:8080/metrics> | `HEALTH_PORT` |
| The admin API, mTLS | `https://localhost:9443` | `ADMIN_PORT` |

The broker's node id is `node-local`, so its statistics are on `$SYS/brokers/node-local/…`.

## Watch it with your own MQTT client

A `#` filter never matches a topic that starts with `$` (MQTT 5 §4.7.2), so
`mosquitto_sub -t '#'` shows the devices and what the rules derive, never the statistics.
Subscribe to those by name:

| Topics | What | Try |
|---|---|---|
| `$SYS/brokers/+/rules/#` | A summary every 2 s (the running rules' digest, how many are enabled, the last reload, the trace setting), then one message per rule: its counts since the broker started (`matched`, `passed`, `no_result`, `failed`, `actions_ok`, `actions_failed`), the time spent evaluating it (`eval_ns`, and `eval_us_avg` per message), rates per second, when it last matched and its last error | `mosquitto_sub -v -t '$SYS/brokers/+/rules/#'` |
| `$SYS/brokers/+/trace/rules/+` | The rule trace: for each rule, at most 5 records a second, and 5 more for messages its WHERE turned away (`no_result`), of the message it ran on (topic, client id, the first 1 KiB of the payload), the result, and every message it rendered. Not under `rules/#`: a trace subscription must be asked for on purpose | `mosquitto_sub -v -t '$SYS/brokers/+/trace/rules/+'` |
| `alerts/#`, `kpi/#`, `normalized/#`, `analytics/#`, `state/#`, `events/#` | What the rules derive: the six roots of [the demo's rules](../rules/README.md#where-the-results-go) | `mosquitto_sub -v -t 'alerts/#' -t 'kpi/#' -t 'state/#'` |
| `plant/#`, `home/#`, `vehicle/#` | What the devices publish | `mosquitto_sub -v -t 'vehicle/#'` |
| `wills/#` | The Will scenarios' Wills and their verdicts | `mosquitto_sub -v -t 'wills/+/result'` |
| `formats/#` | One retained message per payload format | `mosquitto_sub -v -t 'formats/json' -t 'formats/csv'` |
| `spBv1.0/#` | Sparkplug B births, data, commands, deaths and the host's STATE | `mosquitto_sub -v -t 'spBv1.0/STATE/#' -t 'spBv1.0/+/NDEATH/#'` |

The turbines' fast logs are binary (next section), so `-v` on `plant/#` prints half a
megabyte of Parquet a minute per turbine. Leave them out with `-T 'plant/+/+/fastlog'`.

One rule's statistics and trace only, here the grid-frequency alert:

```sh
mosquitto_sub -v -t '$SYS/brokers/+/rules/power_grid_frequency' \
  -t '$SYS/brokers/+/trace/rules/power_grid_frequency'
```

## Silent for two minutes

Two of the rules keep state between messages
([`car_last_heard` and `car_silent`](../rules/README.md#cars)). The first sets a key per
vehicle on every telemetry record and OBD frame, expiring two minutes later; the second
selects [`$events/state/expired`](../../docs/RULES.md#when-a-key-expires-eventsstateexpired),
the event the broker raises when such a key expires, and publishes an alert:

```sh
mosquitto_sub -v -t 'alerts/cars/+/silent'
```

```json
{"vehicle":"XDKCE5K38RH287145","alert":"no data for 2 minutes","severity":"warning","last_heard":"2026-10-11T16:02:16Z","at":"2026-10-11T16:04:16Z","last_stream":"telemetry","body":"car","powertrain":"battery-electric","action":"see events/cars/<VIN>/presence: ignition off, or a lost connection"}
```

In every ten-minute window (they start on whole ten minutes of the wall clock) the car
`XDKCE5K38RH287145` loses its connection 2 min 21 s in and is back at 5 min 05 s, so its
alert comes about 4 min 20 s into the window, two minutes after its last record; a van parks for a job at about 5 min and
drives on at 8 min, and the vehicles whose ignition goes off stay silent until the next
window brings them back. The alert's times are the broker's: up to a second after the
expiry, and no more than `rules.state_expiry_per_sec` events a second (1,000 by default).
The rule's own trace and statistics are under `car_silent` on `$SYS`, and in the editor.
The state is in memory: restarting the broker forgets the keys, and a vehicle that was
already silent then raises nothing until it has spoken again.

## Wills, checked against MQTT

A client's Will is the message the broker publishes for it when its connection ends
without a clean DISCONNECT. `sim-wills` ([`feeds/wills.py`](feeds/wills.py)) ends
connections in sixteen ways, a round every two minutes with the scenarios a few seconds
apart, and checks each Will:

- it arrives when the spec says: at once, after its Will Delay, or when its session ends,
  whichever is first;
- it carries what the client set (QoS, retain, and in MQTT 5 its properties, user
  properties in order);
- it never arrives after a clean DISCONNECT, or when the session was resumed or taken over
  in time.

```sh
mosquitto_sub -v -t 'wills/#'
```

Each Will goes to `wills/<scenario>/will` and names its scenario and run. The verdicts are
retained, so a client that subscribes later sees the last round:

| Topic | What |
|---|---|
| `wills/<scenario>/result` | One scenario's verdict: `pass` or `fail`, what the spec expects, what happened, the problems, and SHOULD-level warnings |
| `wills/summary` | The round: how many scenarios passed, and which failed |

```json
{"scenario":"v5-delay","round":3,"result":"pass","what":"MQTT 5: Will Delay 4 s, session expiry 60 s","spec":"§3.1.3.2.2","expected":"a Will 3.5-6.5 s after the trigger","observed":"a Will 4.53 s after the trigger","problems":[],"warnings":[],"at":"2026-10-19T12:40:16.231+00:00"}
```

| Scenario | The connection | Expected |
|---|---|---|
| `v311-drop` | 3.1.1: the socket closes, no DISCONNECT | the Will at once |
| `v311-disconnect` | 3.1.1: a DISCONNECT | no Will |
| `v311-keepalive` | 3.1.1: silent past 1.5 times its 2 s keep-alive | the Will after about 3 s |
| `v311-protocol-error` | 3.1.1: a PUBLISH with QoS 3, which the broker must close it for | the Will at once |
| `v311-takeover` | 3.1.1: a second connection with the same client id | the first one's Will at once |
| `v311-retained` | 3.1.1: Will Retain set, the socket closes | the Will at once, and retained for later subscribers |
| `v5-drop-props` | 5: the socket closes; the Will has a content type, payload format, message expiry and three user properties | the Will at once, with them all, user properties in order |
| `v5-disconnect` | 5: DISCONNECT 0x00 | no Will |
| `v5-disconnect-with-will` | 5: DISCONNECT 0x04, Disconnect with Will Message | the Will at once |
| `v5-delay` | 5: Will Delay 4 s, session expiry 60 s, the socket closes | the Will after 4 s |
| `v5-session-ends-first` | 5: Will Delay 30 s, session expiry 3 s | the Will after 3 s, when the session ends |
| `v5-no-session` | 5: Will Delay 30 s, session expiry 0 | the Will at once: the session ends with the connection |
| `v5-resumed` | 5: Will Delay 6 s; the client is back after 2 s with clean start 0 | no Will, and Session Present 1 |
| `v5-takeover` | 5: taken over; Will Delay 0 | the Will at once |
| `v5-takeover-resumed` | 5: taken over with clean start 0, inside Will Delay 8 s | no Will |
| `v5-takeover-clean` | 5: taken over with clean start 1, which ends the session | the Will at once |

Three scenarios fail against mqttd today, and their results say so (`known_issue`):
`v5-drop-props` and `v5-takeover-resumed`
([#1033](https://github.com/mbilling/fss-mqtt-broker/issues/1033): the Will's Message Expiry
Interval is not sent, and a takeover inside the Will Delay publishes the Will), and
`v5-resumed` ([#1034](https://github.com/mbilling/fss-mqtt-broker/issues/1034): with
`MQTTD_DURABLE_SESSIONS=0`, as in this stack, a session without subscriptions is not kept).
mqttd also closes a taken-over MQTT 5 connection without the DISCONNECT 0x8E that §3.1.4
describes, which the takeover results report as a warning.

The scenarios run against any broker, each once, with the exit status as the verdict:

```sh
python3 demo/rules-live/feeds/wills.py --once --port 1883     # --list, --only v5-delay,...
```

CI runs them on every change, against the freshly built broker with the durable session
store and with the in-memory one ([`scripts/wills-smoke.sh`](../../scripts/wills-smoke.sh)).
The known failures are expected there, and a run fails as soon as one of them passes, so
the list is kept up to date.

## Turbine fast log (Parquet)

A turbine controller records its fast signals many times a second and uploads them in
files, for the engineers who look into a trip or tune the controller. Here, every minute
each of wf-falster's six turbines uploads the minute just ended:

| | |
|---|---|
| Topic | `plant/wf-falster/<turbine>/fastlog`, `wtg01` to `wtg06`, from client `wf-falster-<turbine>` |
| When | every minute: the minute just ended, `[start_ms, end_ms)` on whole minutes of the wall clock, uploaded 2.0 s (wtg01) to 9.5 s (wtg06) into the next |
| Payload | an [Apache Parquet](https://parquet.apache.org/docs/file-format/) file: 3,000 rows (60 s at 50 Hz), 22 columns, one row group, PLAIN encoding, uncompressed: 531,398 bytes (0.53 MB, 0.51 MiB), under mqttd's 1 MiB packet limit |
| MQTT | QoS 1, not retained, MQTT 5: content type `application/vnd.apache.parquet`, payload format indicator 0 (binary), user properties `schema=fastlog/v1`, `site`, `turbine`, `rows=3000`, `rate_hz=50`, `start_ms`, `end_ms` |
| File metadata | the same pairs, plus `units` (JSON, column to unit); `created_by` names the simulator |

Only this stack sends them (`SIM_FASTLOG=1` on `sim-power`, [`live.py
--fastlog`](../rules/live.py)); the [ten-minute demo](../rules/README.md) has none. The
values follow the turbine's 1-s simulation, interpolated to 50 Hz, with what a 1-s model
leaves out: turbulence within the second, the drivetrain's torsional mode in the generator
speed, the tower's sway in the nacelle accelerations, the blade-passing (3P) ripple, and
sensor noise. A turbine that stops (wtg06, 6 min 12 s into every ten-minute window) shows
it: power to -9 kW, pitch to 88°, the rotor idling. [`sim/power.py`](../rules/sim/power.py)
writes the files with a small Parquet writer of its own ([`sim/parquet.py`](../rules/sim/parquet.py),
standard library only).

| Column | Parquet type | Unit | What |
|---|---|---|---|
| `ts` | INT64, TIMESTAMP(MILLIS, UTC) | ms | sample time, every 20 ms |
| `rotor_speed_rpm` | DOUBLE | rpm | rotor speed, low-speed shaft |
| `generator_speed_rpm` | DOUBLE | rpm | generator speed, high-speed shaft (gear ratio 1:119) |
| `pitch_angle_a_deg`, `pitch_angle_b_deg`, `pitch_angle_c_deg` | DOUBLE | ° | blade pitch angles |
| `active_power_kw` | DOUBLE | kW | active power at the generator terminals |
| `reactive_power_kvar` | DOUBLE | kvar | reactive power |
| `generator_torque_knm` | DOUBLE | kNm | generator torque (0 when not generating) |
| `wind_speed_ms` | DOUBLE | m/s | nacelle anemometer |
| `wind_direction_deg` | DOUBLE | ° | wind direction, from north |
| `yaw_angle_deg` | DOUBLE | ° | nacelle position, from north |
| `tower_acc_fa_ms2`, `tower_acc_ss_ms2` | DOUBLE | m/s² | nacelle acceleration, fore-aft and side-side |
| `gearbox_vibration_mms` | DOUBLE | mm/s | gearbox vibration velocity, RMS 10-1000 Hz |
| `voltage_l1_v`, `voltage_l2_v`, `voltage_l3_v` | DOUBLE | V | phase voltages, line to neutral, 690 V system |
| `current_l1_a`, `current_l2_a`, `current_l3_a` | DOUBLE | A | phase currents |
| `grid_frequency_hz` | DOUBLE | Hz | grid frequency |

The rule `power_wtg_fastlog` indexes each file without decoding it (SQL cannot read
Parquet): from the payload's size, its first four bytes (`PAR1`) and the MQTT 5
properties, it publishes a small record on `analytics/power/wf-falster/<turbine>/fastlog`:

```json
{"site":"wf-falster","turbine":"wtg03","bytes":531398,"content_type":"application/vnd.apache.parquet","schema":"fastlog/v1","rows":3000,"rate_hz":50,"start_ms":1791633540000,"end_ms":1791633600000,"from_utc":"2026-10-10T11:59:00Z","to_utc":"2026-10-10T12:00:00Z","received_at":1791633605067,"upload_lag_s":5.1,"key":"wf-falster/wtg03/2026/10/10/1159.parquet"}
```

The editor shows a fast log as its size, content type and first bytes (`50 41 52 31 …
PAR1`), never the file. To save one and read it (`-N`: no newline after the payload,
`-C 1`: one message, then exit):

```sh
mosquitto_sub -h 127.0.0.1 -t plant/wf-falster/wtg01/fastlog -C 1 -N > wtg01.parquet
mosquitto_sub -h 127.0.0.1 -V mqttv5 -t plant/wf-falster/wtg01/fastlog -C 1 -F '%P' -N   # its user properties
duckdb -c "DESCRIBE 'wtg01.parquet'; SELECT count(*), min(ts), max(ts), avg(active_power_kw) FROM 'wtg01.parquet'"
python3 -c "import pyarrow.parquet as pq; t = pq.read_table('wtg01.parquet'); print(t.schema, t.num_rows)"
```

(DuckDB and pyarrow are not part of the stack: `pip install duckdb pyarrow` in a virtual
environment of your own.)

## Every payload format, one topic each

`sim-formats` ([`feeds/formats.py`](feeds/formats.py)) publishes, every minute, one message
in each format an MQTT explorer meets in the field, about the same plants and homes. Each
is retained under `formats/`, so a client that subscribes to `formats/#` gets one of each at
once. Each carries an MQTT 5 content type and payload format indicator, at QoS 1. The
formats are written with the standard library ([`feeds/binfmt.py`](feeds/binfmt.py),
[`feeds/arrow.py`](feeds/arrow.py) and the simulators'
[`sim/parquet.py`](../rules/sim/parquet.py)).

| Topic | Content type | What |
|---|---|---|
| `formats/json` | `application/json` | wf-falster's status: power, wind, grid, every turbine |
| `formats/jsonl` | `application/jsonl` | ten grid-frequency readings, a JSON document a line |
| `formats/xml` | `application/xml` | a production schedule for the next four hours, quarter-hourly |
| `formats/csv` | `text/csv` | yesterday's production per turbine |
| `formats/csv-semicolon` | `text/csv; charset=utf-8` | a Danish meter export: semicolons, decimal commas, `Forbrug (kWh)` |
| `formats/tsv` | `text/tab-separated-values` | pv-lolland's inverter strings |
| `formats/psv` | `text/plain` | a SCADA alarm list, pipe-delimited, for the explorer to recognise |
| `formats/text` | `text/plain` | one status line |
| `formats/parquet` | `application/vnd.apache.parquet` | 24 hours of hourly energy per turbine: 144 rows, two row groups, GZIP, nullable columns, strings, booleans |
| `formats/parquet-zstd` | `application/vnd.apache.parquet` | a day of quarter-hour meter readings for four homes, ZSTD |
| `formats/avro` | `application/avro` | the turbines' event log, an Avro container file, deflate |
| `formats/avro-zstd` | `application/avro` | EV charging sessions, zstandard |
| `formats/avro-registry` | `application/vnd.confluent.avro` | one event in schema-registry framing (schema id 42): the schema is not in the payload |
| `formats/avro-single-object` | `application/avro` | one event in Avro's single-object encoding, with the schema's fingerprint |
| `formats/arrow` | `application/vnd.apache.arrow.file` | 2 s of 50 Hz vibration on three drivetrain sensors, an Arrow IPC file: a dictionary-encoded column, ZSTD buffers |
| `formats/arrow-stream` | `application/vnd.apache.arrow.stream` | pk-koge's one-second samples for a minute, an Arrow IPC stream |
| `formats/sqlite` | `application/vnd.sqlite3` | the wind farm's configuration as a SQLite database file: three tables, an index, a view, a trigger |
| `formats/sqlite-changeset` | `application/x-sqlite-changeset` | the setpoint changes of the last ten minutes, as a SQLite session changeset |
| `formats/sqlite-patchset` | `application/x-sqlite-patchset` | the same changes as a patchset |
| `formats/msgpack` | `application/msgpack` | a turbine's state |
| `formats/cbor` | `application/cbor` | a vibration summary, with a tagged epoch time and a byte string |
| `formats/protobuf` | `application/x-protobuf` | a telemetry reading, without its schema (the schema is in `formats.py`) |
| `formats/grpc` | `application/grpc` | the same reading in a gRPC frame |
| `formats/gzip` | `application/gzip` | the three plants' summary, JSON inside |
| `formats/zstd` | `application/zstd` | the solar park's 5-minute data, a Parquet file inside (LZ4_RAW pages) |
| `formats/binary` | `application/octet-stream` | a meter's Modbus holding registers, as read |
| `formats/empty` | `application/octet-stream` | nothing: an empty payload (not retained, since an empty retained message deletes one) |

The turbines' 0.53 MB fast logs ([above](#turbine-fast-log-parquet)) are the other Parquet
files in the stack. To see one of a format with no client at hand:

```sh
python3 demo/rules-live/feeds/formats.py --list          # each topic, content type and size
python3 demo/rules-live/feeds/formats.py --once --port 1883   # publish them all once
```

## Sparkplug B

`sim-sparkplug` ([`feeds/sparkplug.py`](feeds/sparkplug.py)) plays [Eclipse
Sparkplug](https://sparkplug.eclipse.org/) 3.0 on two edge nodes and a primary host
application:

| Who | Topics | |
|---|---|---|
| wf-falster's park power controller | `spBv1.0/wf-falster/{NBIRTH,NDATA,NDEATH,NCMD}/ppc`, and `{DBIRTH,DDATA,DDEATH,DCMD}/ppc/wtg01`..`wtg06` | a device per turbine |
| pv-lolland's SCADA | `spBv1.0/pv-lolland/.../scada`, devices `inv01`, `inv02` | a device per inverter |
| the host application `scada-host` | `spBv1.0/STATE/scada-host` | JSON `{"online":true,...}`, retained; its Will says `false` |

Each edge node connects with its NDEATH as its MQTT Will, carrying the bdSeq of the NBIRTH
that follows. The NBIRTH and a DBIRTH per device name every metric with its alias, data type
and properties (engineering units, ranges). NDATA and DDATA then carry aliases only, by
exception, every five seconds, with seq 0 to 255 throughout. The births carry every
Sparkplug 3.0 data type: the signed and unsigned integers, Float, Double, Boolean, String,
Text, UUID, DateTime, Bytes, File, every array type, a DataSet (the park's alarm table), a
Template definition (`TurbineModel`) with an instance per turbine, and PropertySets.

On a schedule:

| Every | What happens |
|---|---|
| 3 min | the host writes a turbine's power limit (DCMD `Setpoint/Max Power`); the turbine reports it in its next DDATA |
| 6 min | the alarm table goes out in an NDATA compressed the way Eclipse Tahu does it (uuid `SPBV1.0_COMPRESSED`, GZIP) |
| 10 min | the host asks the park controller for a rebirth (NCMD `Node Control/Rebirth`): a new NBIRTH and DBIRTHs, seq from 0 |
| 12 min | a turbine goes into service: DDEATH, and its DBIRTH a minute later |
| 15 min | the solar SCADA loses power without a DISCONNECT, so the broker publishes its NDEATH from the Will; it is back 20 s later with the next bdSeq |
| 20 min | the park controller restarts cleanly: it publishes its NDEATH itself, disconnects, and is back 10 s later |

Data and command messages name their metrics by alias only. A client that subscribes after a
birth sees aliases until the next one, which the rebirth brings every ten minutes. fss-mqtt
learns the names and types from the births it sees.

## Change a rule in the editor

Open <http://localhost:8070>. The header shows the digest of the rules that run and of the
file on disk, how many rules are enabled, the last reload and whether the trace is on; the
table below it counts every rule's messages as they arrive, with rates over the last 10 s.

1. Press **New rule**. The editor fills with a small example, `demo_rule_1`. It takes the
   wind farm's grid meter (`plant/+/poc/grid`, a reading every 2 s) and republishes a
   reading to `kpi/demo/<site>/frequency` when the frequency is more than 20 mHz off 50 Hz:
   `payload.Hz < 49.98 OR payload.Hz > 50.02`.
2. **Check** splices the rule into the rules file and validates the whole file, writing
   nothing. An error names the line and column, and selects it in the editor.
3. **Test** runs the rule, as if enabled, on the latest grid reading the page has seen, and
   shows what it would publish, publishing nothing. Most readings are within 20 mHz, so
   the answer is often `no_result`: FROM matched, but WHERE was false.
4. **Apply** writes the rules file (atomically; the previous file is kept as
   `rules.toml.prev` beside it) and reloads it. The answer shows the new digest; a few
   seconds later the summary on `$SYS` reports it, the header changes with it, and
   `demo_rule_1` is in the table. It passes a reading every 7 s or so, in bursts: watch
   its **Passed** and **Passed/s**, its trace (each `passed` record with the message it
   published), and **Live messages** with the filter `kpi/demo/#`.
5. Make it fire less often: in the SQL, change `49.98` to `49.97` and `50.02` to `50.03`
   (and the description to 30 mHz), and **Apply** again. It now passes about a third as
   many readings, one every 20 s or so. **Passed** still grows, since the counts add up from
   the broker's start (an edited rule keeps its counts), but more slowly; **Passed/s**
   drops, and in the trace (tick **Hide no_result**) the passes come further apart.
   **Avg µs** is what the rule costs per message over the last 10 s: its SQL plus
   rendering its actions (not routing what it republishes). Compare rules side by side,
   or watch it while you edit one: a heavier `WHERE` or a `FOREACH` shows up there first.
6. **Delete** removes `demo_rule_1` again.

The simulated grid also has an excursion: 6 min 45 s into every ten-minute window (at
hh:06:45, hh:16:45 and so on, UTC), the frequency drops to about 49.75 Hz and stays low
until the window ends. Meanwhile `demo_rule_1` passes every reading, whatever its band, and
its **Passed** grows faster. The shipped **power_grid_frequency** alerts only outside
49.9-50.1 Hz, so it fires only in the first three minutes or so of the excursion. To see
what it publishes at another time, choose it, choose **this message** under "Test it
against", and **Test** it with the topic `plant/wf-falster/poc/grid` and this payload:

```json
{"ts":1791469801000,"Hz":49.8,"ROCOF":-0.03,"U_kV":{"L12":51.4,"L23":51.3,"L31":51.4}}
```

It passes, and renders the alert it would publish on `alerts/power/grid/frequency`: band
"FCR-D upward", 25 % FCR-D activation.

Rules do not chain: a rule never runs on a message a rule published, its own or another's.
A rule with `FROM "kpi/demo/#"` would never see `demo_rule_1`'s output, only what clients
publish there.

**Delete** removes the chosen rule, and **The whole rules file** at the bottom edits the
file as text, with **Reset to the shipped rules** to put
[`demo/rules/rules.toml`](../rules/rules.toml) back. Every write names the version of the
file its text came from. If the file changed since (in another tab, say) in a way the
write would undo, the write is refused instead of overwriting that change, and a second
Apply writes over it on purpose. (Reset to the shipped rules is the exception: it replaces
whatever is there.)

The edited rules live in the stack's `rules` volume, never in the checkout, and survive
`docker compose down`.

## The admin API from the host

The rules endpoints are ordinary admin API calls ([ADMIN-API.md](../../docs/ADMIN-API.md),
[ADMIN-CLI.md](../../docs/ADMIN-CLI.md)), so anything the page does, you can do from a
terminal. The `admin` service runs the broker's own CLI with the page's certificate,
`CN=rules-ui`, an operator and a rules writer:

```sh
cd demo/rules-live
docker compose run --rm admin rules           # every rule, its counts and last error
docker compose run --rm admin rules-source    # the rules file as it is on disk
docker compose run --rm admin help            # every verb
```

To edit the file in your own editor, take a copy (outside the checkout) and the digest of
the file you copied:

```sh
work="$(mktemp -d)"
docker compose run --rm -T admin rules-source > "$work/rules.toml"
docker compose run --rm -T admin rules-source --json | jq -r .digest
```

Edit `$work/rules.toml`, then write it back. The `admin` container sees its certificate,
not your files, so the file goes in on standard input. `--if_match` names the digest you
took: if the file changed since, the write is refused (`412 digest-mismatch`) instead of
overwriting that change. `--if_match '*'` replaces whatever is there.

```sh
docker compose run --rm -T admin rules-apply /dev/stdin --if_match <digest> < "$work/rules.toml"
```

The answer shows the new file's `digest`, which the next write names. To remove one rule:

```sh
docker compose run --rm admin rule-delete <id> --if_match <digest>
```

With curl, copy the certificate out of the stack first, somewhere outside the checkout:

```sh
pki="$(mktemp -d)"
docker compose cp ui:/pki/. "$pki"
curl --cacert "$pki/ca.crt" --cert "$pki/rules-ui.crt" --key "$pki/rules-ui.key" \
  https://localhost:9443/admin/v1/rules
```

## Ports

Every port is published on 127.0.0.1. When one is taken (a local mosquitto on 1883, the
[cluster demo](../README.md) on 1883 and 8080), `up.sh` says which, and how to move it:

```sh
MQTT_PORT=1884 UI_PORT=8071 HEALTH_PORT=8081 ADMIN_PORT=9444 demo/rules-live/up.sh
```

Give the same variables to any later `docker compose up` (export them in your shell), or
Compose recreates the broker on the default ports. `docker compose run --rm admin`, `logs`,
`cp` and `down` do not need them.

`MQTT_BIND=0.0.0.0` publishes MQTT on every interface, to reach the stack from a phone or
another machine. That exposes an anonymous broker, with the rule trace, to your network:
only on one you trust.

## Stop and reset

```sh
cd demo/rules-live
docker compose down       # stop; the edited rules and the PKI stay in their volumes
docker compose down -v    # stop and forget them: the next up.sh starts from the shipped rules
```

## What the clock changes

The simulators' devices tell the time by the wall clock, so what they report, and what the
rules make of it, depends on when you watch:

- the solar park produces only in daylight, so its inverter ground fault (which needs 5 kW
  of DC) never fires at night;
- the EV peak-tariff advice fires only for a charging session that starts between 17:00 and
  21:00 Danish time;
- the heat-pump and heating faults happen only in the heating season.

Each ten-minute window also starts the devices afresh: odometers, meter registers and
charge levels jump back, and a faulted turbine is healthy again. The rules see one message
at a time, so this raises no false alert.

To see every alert whatever the hour, give the devices the README's clock: every window
then replays [the demo's ten minutes](../rules/README.md), 54 alerts included.

```sh
SIM_CLOCK=fixture demo/rules-live/up.sh
```

## Security: a demo on your own machine

- **The broker is anonymous, with no ACL file.** Whoever reaches its MQTT port may publish
  and subscribe anywhere, the rule trace included, and the trace copies payloads, client ids
  and user names onto `$SYS`.
- **The editor is an admin-API operator without a login.** It holds the `CN=rules-ui`
  certificate, which may write the rules file, so whoever reaches the page can rewrite the
  rules, and through them read and republish any message. It refuses what another web site
  could send it (it serves only `Host: localhost:<port>` or `127.0.0.1:<port>`, takes a
  write only from its own page, with a JSON body and its own header, and sends a strict
  Content-Security-Policy), and it shows every message as text, never as HTML. It still
  has no password.
- **So every port is on 127.0.0.1, and there is no WebSocket listener**: the broker's
  WebSocket listener has no Origin check, so any web page you visit could reach it.
- The PKI is throwaway, minted on the first run into Docker volumes, never into the
  checkout; the CA key is in a volume only the one-shot `pki` service mounts.

For a real deployment, leave the trace off and `[rules] admin_writers` empty, and grant
`$SYS/brokers/+/rules/#` only to who needs the statistics
([HARDENING.md](../../docs/HARDENING.md)).

## When something is off

- **"This broker predates ADR 0084"** in the editor: the broker has no rules endpoints, or
  publishes no statistics. The image is older than the checkout; `up.sh` rebuilds it.
- **No statistics with your own client:** subscribe to `$SYS/brokers/+/rules/#` by name;
  `#` does not cover it.
- **What each service says:** `docker compose logs -f mqttd` (or `ui`, `sim-cars`, …),
  from `demo/rules-live`.

## Files

| File | What |
|---|---|
| [`compose.yaml`](compose.yaml) | The stack: `pki` and `rules-seed` (one-shot), `mqttd`, `sim-power`, `sim-formats`, `sim-sparkplug`, `sim-homes`, `sim-cars`, `sim-wills`, `ui`, and `admin` (run on demand) |
| [`up.sh`](up.sh) | Preflight the ports, build, start, wait until healthy, print where to look |
| [`pki/gen.sh`](pki/gen.sh) | The throwaway CA, the admin listener's certificate and the `CN=rules-ui` client certificate |
| [`ui/server.py`](ui/server.py) | The editor's server, standard library only: the page, the admin API over mTLS, and MQTT as server-sent events |
| [`ui/index.html`](ui/index.html), [`ui/app.js`](ui/app.js), [`ui/style.css`](ui/style.css) | The page, with no third-party code |
| [`feeds/wills.py`](feeds/wills.py) | The Will scenarios, standard library only; its MQTT client is the simulators' [`sim/mqtt.py`](../rules/sim/mqtt.py) |
| [`feeds/formats.py`](feeds/formats.py) | Every payload format, one retained topic each |
| [`feeds/sparkplug.py`](feeds/sparkplug.py) | Sparkplug B edge nodes, devices and a host application |
| [`feeds/binfmt.py`](feeds/binfmt.py), [`feeds/arrow.py`](feeds/arrow.py) | MessagePack, CBOR, Protobuf, Avro, SQLite changeset and Arrow IPC writers, standard library only |

The simulators and the rules are [`demo/rules`](../rules/README.md)'s, mounted read-only.
