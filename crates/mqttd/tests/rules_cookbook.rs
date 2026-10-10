//! The rule cookbook, kept true: `docs/RULES-COOKBOOK.md` and `docs/examples/rules/`.
//!
//! Each recipe is a rules file a reader copies. This suite runs every file in the REAL
//! `mqttd` binary, started the way the cookbook's quick start starts it, publishes the
//! input the cookbook shows (connecting, publishing and disconnecting, as one
//! `mosquitto_pub` does), and asserts the exact messages a `QoS` 2 subscriber to `#`
//! receives: topic, payload, `QoS` and retain flag for an MQTT 3.1.1 subscriber, which is
//! what the cookbook's `mosquitto_sub` is, and the properties too for an MQTT 5 subscriber
//! wherever a recipe has properties. Each original arrives unchanged, along with what the
//! rules derived from it, in routing order within `QoS` 0 and within `QoS` 1 and 2 (a
//! `QoS` 0 message can overtake a `QoS` 1 or 2 one routed before it), and nothing else
//! arrives.
//!
//! It also keeps the page and the files from drifting apart. Every file in
//! `docs/examples/rules/` has a case here, runs on the broker and passes
//! `mqttd --check-rules` with no warning. Every fenced block on the page, section by
//! section and in order, is one this suite renders from the table it runs: the files
//! verbatim, every command and every output line, with none missing and none extra. And
//! every `In:`, `Out:` and `Late:` example in a file's header is, from that table, one
//! publish with everything it derives, a message derived from a client event, or a
//! retained message. Change a recipe's behaviour, its file or its page alone and a test
//! here fails, printing what should be there.

mod common;
mod listen_wait;
mod proc_common;

use std::fmt::Write as _;
use std::io::Read as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::Client;
use mqtt_codec::packet::Publish;
use mqtt_codec::{Packet, Properties, Property, QoS};

// ---------------------------------------------------------------------------------------
// The table: every recipe's input and the exact messages it produces.
// ---------------------------------------------------------------------------------------

/// A payload, as the cookbook shows it.
#[derive(Clone, Copy, Debug)]
enum Body {
    /// UTF-8 text, shown as is. Empty is `mosquitto_pub -n`.
    Text(&'static str),
    /// Bytes that are not text, shown as `mosquitto_sub`'s `%x` shows them: lower-case hex.
    /// Written as byte strings (`b"\x02"`): `scripts/check-reason-codes.py` reads a hex
    /// literal from 0x80 up in an integration test as a reason code the test provokes.
    Bytes(&'static [u8]),
    /// Text with values that differ on every run: a [`match_pattern`] pattern.
    Varies(&'static str),
}

/// An MQTT 5 property of a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Prop {
    Expiry(u32),
    ContentType(&'static str),
    Correlation(&'static [u8]),
    User(&'static str, &'static str),
}

/// A message a subscriber receives, as the cookbook shows it.
#[derive(Clone, Copy, Debug)]
struct Msg {
    topic: &'static str,
    qos: u8,
    retain: bool,
    body: Body,
    /// MQTT 5 properties, in the order the broker writes them: expiry, content type,
    /// correlation data, then user properties.
    props: &'static [Prop],
}

/// One `mosquitto_pub`: connect as `client`, publish once, disconnect.
#[derive(Clone, Copy, Debug)]
struct Pub {
    client: &'static str,
    v5: bool,
    topic: &'static str,
    qos: u8,
    retain: bool,
    body: Body,
    /// Properties to publish with, in the broker's order (see [`Msg::props`]).
    props: &'static [Prop],
    /// What the recipe's rules derive from it, in the order the broker routes it, after
    /// the original.
    derived: &'static [Msg],
}

/// How the broker test drives a recipe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Run {
    /// Its publishes, one `mosquitto_pub` each, then a late subscriber if it has one.
    Publishes,
    /// Recipe 01: its publishes, then the console line in the broker's log.
    ConsoleLog,
    /// Recipe 11: devices that connect, stay, disconnect and die, then a late dashboard.
    Presence,
}

/// One recipe file and what it does.
#[derive(Debug)]
struct Recipe {
    file: &'static str,
    /// What `mqttd --check-rules` lists after its summary: one line per rule, in id order.
    listing: &'static [&'static str],
    run: Run,
    /// What running it on the broker proves; a failure prints it.
    proves: &'static str,
    pubs: &'static [Pub],
    /// Messages the recipe derives from client events rather than from [`Recipe::pubs`].
    events: &'static [Msg],
    /// The filter of the subscriber the cookbook starts after the publishes, if it does.
    late_filter: Option<&'static str>,
    /// Every retained message in the broker after the publishes, by topic.
    retained: &'static [Msg],
}

const fn out(topic: &'static str, qos: u8, payload: &'static str) -> Msg {
    Msg {
        topic,
        qos,
        retain: false,
        body: Body::Text(payload),
        props: &[],
    }
}

const fn varies(topic: &'static str, qos: u8, retain: bool, pattern: &'static str) -> Msg {
    Msg {
        topic,
        qos,
        retain,
        body: Body::Varies(pattern),
        props: &[],
    }
}

const fn kept(topic: &'static str, qos: u8, payload: &'static str) -> Msg {
    Msg {
        topic,
        qos,
        retain: true,
        body: Body::Text(payload),
        props: &[],
    }
}

const fn publish(
    client: &'static str,
    topic: &'static str,
    qos: u8,
    payload: &'static str,
    derived: &'static [Msg],
) -> Pub {
    Pub {
        client,
        v5: false,
        topic,
        qos,
        retain: false,
        body: Body::Text(payload),
        props: &[],
        derived,
    }
}

const R01: Recipe = Recipe {
    file: "01-debug-console.toml",
    listing: &[r#"  debug_factory (enabled): FROM "factory/#", 1 action(s)"#],
    run: Run::ConsoleLog,
    proves: "the console action logs every field of the message at INFO, exactly the line \
             the cookbook shows; it publishes nothing and the original is delivered",
    pubs: &[publish(
        "plc-1",
        "factory/line1/temp",
        1,
        r#"{"t": 20.5}"#,
        &[],
    )],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R02: Recipe = Recipe {
    file: "02-threshold-alert.toml",
    listing: &[r#"  overheat_alert (enabled): FROM "machines/+/telemetry", 1 action(s)"#],
    run: Run::Publishes,
    proves: "a reading of 75 or more alerts at QoS 1 on alerts/<severity>/<machine>, \
             \"critical\" from 90; a reading under 75 derives nothing",
    pubs: &[
        publish("m-7", "machines/m-7/telemetry", 0, r#"{"temp": 70}"#, &[]),
        publish(
            "m-7",
            "machines/m-7/telemetry",
            0,
            r#"{"temp": 80.5}"#,
            &[out(
                "alerts/warning/m-7",
                1,
                r#"{"machine":"m-7","temp":80.5,"alarm":"overheat","severity":"warning"}"#,
            )],
        ),
        publish(
            "m-7",
            "machines/m-7/telemetry",
            0,
            r#"{"temp": 95}"#,
            &[out(
                "alerts/critical/m-7",
                1,
                r#"{"machine":"m-7","temp":95,"alarm":"overheat","severity":"critical"}"#,
            )],
        ),
    ],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R03: Recipe = Recipe {
    file: "03-unit-conversion.toml",
    listing: &[r#"  weather_imperial (enabled): FROM "weather/+/raw", 1 action(s)"#],
    run: Run::Publishes,
    proves: "unit conversion: `/` gives a float and round() a whole number, so the output \
             has exactly the decimals the cookbook shows",
    pubs: &[publish(
        "ws-1",
        "weather/oslo/raw",
        0,
        r#"{"temp_c": 21.37, "pressure_pa": 101325, "wind_ms": 5.2, "battery_mv": 3610}"#,
        &[out(
            "weather/oslo/imperial",
            0,
            r#"{"station":"oslo","temp_f":70.5,"pressure_hpa":1013.25,"wind_mph":12,"battery_v":3.61}"#,
        )],
    )],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R04: Recipe = Recipe {
    file: "04-reshape-vendor-payload.toml",
    listing: &[r#"  vendor_to_canonical (enabled): FROM "vendor/+/uplink", 1 action(s)"#],
    run: Run::Publishes,
    proves: "dotted aliases build the nested canonical schema and the time renders as RFC \
             3339; a device id that is not one plain topic level derives nothing",
    pubs: &[
        publish(
            "acme-gw",
            "vendor/acme/uplink",
            1,
            r#"{"devId": "th-42", "ts": 1791386000000, "vals": {"t": 21.5, "h": 40}}"#,
            &[out(
                "canonical/th-42",
                1,
                r#"{"device":{"id":"th-42","vendor":"acme"},"measurements":{"temperature":21.5,"humidity":40},"time":"2026-10-07T15:13:20.000+00:00"}"#,
            )],
        ),
        // A device id that is not one plain topic level derives nothing.
        publish(
            "acme-gw",
            "vendor/acme/uplink",
            1,
            r#"{"devId": "th-42/cmd", "ts": 1791386000000, "vals": {"t": 21.5, "h": 40}}"#,
            &[],
        ),
    ],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R05: Recipe = Recipe {
    file: "05-flatten-nested-json.toml",
    listing: &[r#"  flatten_gateway_state (enabled): FROM "gw/+/state", 2 action(s)"#],
    run: Run::Publishes,
    proves: "one rule, two actions, published in the order listed: flat JSON, then \
             InfluxDB line protocol rendered from a text template",
    pubs: &[publish(
        "edge-1",
        "gw/edge-1/state",
        0,
        r#"{"device": {"id": "pump-3", "fw": {"version": "2.1.0"}}, "readings": {"env": {"temp": 40.2, "hum": 31}, "power": {"volts": 229.8, "amps": 3.1}}}"#,
        &[
            out(
                "flat/edge-1",
                0,
                r#"{"gateway":"edge-1","device_id":"pump-3","fw_version":"2.1.0","env_temp":40.2,"env_hum":31,"power_w":712}"#,
            ),
            out(
                "influx/pumps",
                0,
                "pump,gateway=edge-1,device=pump-3,fw=2.1.0 temp=40.2,hum=31,power_w=712",
            ),
        ],
    )],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R06: Recipe = Recipe {
    file: "06-split-array-foreach.toml",
    listing: &[
        r#"  split_batch (enabled): FROM "gw/+/batch", 1 action(s)"#,
        r#"  split_batch_hot (enabled): FROM "gw/+/batch", 1 action(s)"#,
    ],
    run: Run::Publishes,
    proves: "FOREACH publishes one message per element, INCASE keeps only the hot ones, \
             rules run in id order, and an element whose id is not one plain topic level is \
             skipped by both",
    pubs: &[
        publish(
            "gw-9",
            "gw/gw-9/batch",
            1,
            r#"{"gateway": "gw-9", "readings": [{"sensor": "s1", "temp": 21.0}, {"sensor": "s2", "temp": 85.5}, {"sensor": "s3", "temp": 22.4}]}"#,
            &[
                out(
                    "sensors/s1/temp",
                    1,
                    r#"{"sensor":"s1","temp":21.0,"gateway":"gw-9"}"#,
                ),
                out(
                    "sensors/s2/temp",
                    1,
                    r#"{"sensor":"s2","temp":85.5,"gateway":"gw-9"}"#,
                ),
                out(
                    "sensors/s3/temp",
                    1,
                    r#"{"sensor":"s3","temp":22.4,"gateway":"gw-9"}"#,
                ),
                out("alerts/hot/s2", 1, r#"{"sensor":"s2","temp":85.5}"#),
            ],
        ),
        // A sensor id that is not one plain topic level is skipped by both rules.
        publish(
            "gw-9",
            "gw/gw-9/batch",
            1,
            r#"{"gateway": "gw-9", "readings": [{"sensor": "s4/../cmd", "temp": 99.0}]}"#,
            &[],
        ),
    ],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R07: Recipe = Recipe {
    file: "07-route-by-payload-field.toml",
    listing: &[r#"  route_by_type (enabled): FROM "ingest", 1 action(s)"#],
    run: Run::Publishes,
    proves: "each message on ingest is forwarded byte for byte to devices/<kind>/<device>, \
             and the guard stops a device id with `/` in it, and a missing one, from \
             choosing the topic",
    pubs: &[
        publish(
            "cell-gw",
            "ingest",
            1,
            r#"{"type": "door", "device": "d-17", "open": true}"#,
            &[out(
                "devices/door/d-17",
                1,
                r#"{"type": "door", "device": "d-17", "open": true}"#,
            )],
        ),
        publish(
            "cell-gw",
            "ingest",
            1,
            r#"{"type": "meter", "device": "m-3", "kwh": 12.5}"#,
            &[out(
                "devices/meter/m-3",
                1,
                r#"{"type": "meter", "device": "m-3", "kwh": 12.5}"#,
            )],
        ),
        publish(
            "cell-gw",
            "ingest",
            1,
            r#"{"type": "valve", "device": "v-1", "pos": 40}"#,
            &[out(
                "devices/other/v-1",
                1,
                r#"{"type": "valve", "device": "v-1", "pos": 40}"#,
            )],
        ),
        publish(
            "cell-gw",
            "ingest",
            1,
            r#"{"type": "door", "device": "x/../admin/cmd", "open": true}"#,
            &[],
        ),
        publish(
            "cell-gw",
            "ingest",
            1,
            r#"{"type": "door", "open": true}"#,
            &[],
        ),
    ],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R08: Recipe = Recipe {
    file: "08-enrich-metadata.toml",
    listing: &[r#"  enrich_counter (enabled): FROM "plant/+/counter", 1 action(s)"#],
    run: Run::Publishes,
    proves: "the reading is stamped with the publisher's client id, \"anonymous\" for its \
             missing username, its IP, the node, a message id, and the arrival time three \
             ways that agree with each other and with this run's clock",
    pubs: &[publish(
        "plc-12",
        "plant/line-a/counter",
        0,
        r#"{"count": 1042}"#,
        &[varies(
            "enriched/line-a/counter",
            0,
            false,
            r#"{"line":"line-a","count":1042,"meta":{"clientid":"plc-12","username":"anonymous","ip":"127.0.0.1","broker":"node-local","msg_id":"<id>","received_ms":<ms>,"received_at":"<utc>","plant_time":"<plant>"}}"#,
        )],
    )],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R09: Recipe = Recipe {
    file: "09-migrate-topic-namespace.toml",
    listing: &[r#"  v1_to_v2_topics (enabled): FROM "v1/+/+/+", 1 action(s)"#],
    run: Run::Publishes,
    proves: "the mirror keeps payload, QoS and the retain flag of the original, for the \
             pilot sites only; a late subscriber gets the retained copy (and the retained \
             original) with the retain flag set",
    pubs: &[
        publish(
            "fw-1",
            "v1/berlin/th-1/temp",
            1,
            "21.5",
            &[out("sites/berlin/devices/th-1/temp", 1, "21.5")],
        ),
        Pub {
            client: "fw-1",
            v5: false,
            topic: "v1/munich/th-9/state",
            qos: 2,
            retain: true,
            body: Body::Text("ON"),
            props: &[],
            derived: &[out("sites/munich/devices/th-9/state", 2, "ON")],
        },
        publish("fw-1", "v1/paris/th-4/temp", 1, "19.0", &[]),
    ],
    events: &[],
    late_filter: Some("#"),
    retained: &[
        kept("sites/munich/devices/th-9/state", 2, "ON"),
        kept("v1/munich/th-9/state", 2, "ON"),
    ],
};

const R10: Recipe = Recipe {
    file: "10-stateless-thinning.toml",
    listing: &[
        r#"  decimate_by_seq (enabled): FROM "vib/+/raw", 1 action(s)"#,
        r#"  fleet_sample (enabled): FROM "vib/+/raw", 1 action(s)"#,
    ],
    run: Run::Publishes,
    proves: "every 10th reading by sequence number, and every reading of the devices whose \
             id hashes into the first quarter; nothing else",
    pubs: &[
        // pump-1 hashes to bucket 59, so it is never sampled; pump-2 hashes to 21.
        publish(
            "pump-1",
            "vib/pump-1/raw",
            0,
            r#"{"seq": 9, "v": 0.38}"#,
            &[],
        ),
        publish(
            "pump-1",
            "vib/pump-1/raw",
            0,
            r#"{"seq": 10, "v": 0.42}"#,
            &[out(
                "vib/pump-1/1in10",
                0,
                r#"{"device":"pump-1","seq":10,"v":0.42}"#,
            )],
        ),
        publish(
            "pump-1",
            "vib/pump-1/raw",
            0,
            r#"{"seq": 11, "v": 0.4}"#,
            &[],
        ),
        publish(
            "pump-2",
            "vib/pump-2/raw",
            0,
            r#"{"seq": 20, "v": 0.35}"#,
            &[
                out(
                    "vib/pump-2/1in10",
                    0,
                    r#"{"device":"pump-2","seq":20,"v":0.35}"#,
                ),
                out(
                    "debug/sample/pump-2",
                    0,
                    r#"{"device":"pump-2","bucket":21,"seq":20,"v":0.35}"#,
                ),
            ],
        ),
        publish(
            "pump-2",
            "vib/pump-2/raw",
            0,
            r#"{"seq": 21, "v": 0.37}"#,
            &[out(
                "debug/sample/pump-2",
                0,
                r#"{"device":"pump-2","bucket":21,"seq":21,"v":0.37}"#,
            )],
        ),
    ],
    events: &[],
    late_filter: None,
    retained: &[],
};

/// Recipe 11's events, in the order the cookbook's steps raise them.
const ONLINE_B: Msg = varies(
    "presence/sensor-b",
    1,
    false,
    r#"{"clientid":"sensor-b","status":"online","since":<ms>}"#,
);
const ONLINE_C: Msg = varies(
    "presence/sensor-c",
    1,
    false,
    r#"{"clientid":"sensor-c","status":"online","since":<ms>}"#,
);
const ONLINE_A: Msg = varies(
    "presence/sensor-a",
    1,
    false,
    r#"{"clientid":"sensor-a","status":"online","since":<ms>}"#,
);
const OFFLINE_A: Msg = varies(
    "presence/sensor-a",
    1,
    false,
    r#"{"clientid":"sensor-a","status":"offline","since":<ms>,"reason":"normal"}"#,
);
const OFFLINE_B: Msg = varies(
    "presence/sensor-b",
    1,
    false,
    r#"{"clientid":"sensor-b","status":"offline","since":<ms>,"reason":"tcp_closed"}"#,
);

/// Recipe 11's first two steps: the devices that connect and stay connected (one
/// `mosquitto_sub -i <id> &` each). The first one's process is then killed.
const STAYS: [&str; 2] = ["sensor-b", "sensor-c"];

/// Recipe 11's third step: a device that connects, publishes and disconnects.
const SENSOR_A: Pub = publish("sensor-a", "telemetry/sensor-a", 0, "21.5", &[]);

/// Recipe 11's last step: the first background job, `STAYS[0]`, dies without a DISCONNECT.
const KILL_FIRST: &str = "kill -9 %1";

/// What recipe 11's watcher and dashboard subscribe to.
const PRESENCE: &str = "presence/#";

const R11: Recipe = Recipe {
    file: "11-device-presence.toml",
    listing: &[
        r#"  presence_offline (enabled): FROM "$events/client/disconnected", 1 action(s)"#,
        r#"  presence_online (enabled): FROM "$events/client/connected", 1 action(s)"#,
    ],
    run: Run::Presence,
    proves: "connecting, a clean DISCONNECT and a dropped connection each publish the \
             device's retained status, a dashboard that connects later gets every device's \
             current status, and a takeover (the same client id connecting again) leaves \
             the device online rather than publishing the old connection's discarded as \
             \"offline\"",
    pubs: &[SENSOR_A],
    events: &[ONLINE_B, ONLINE_C, ONLINE_A, OFFLINE_A, OFFLINE_B],
    late_filter: Some(PRESENCE),
    retained: &[
        varies(
            "presence/sensor-a",
            1,
            true,
            r#"{"clientid":"sensor-a","status":"offline","since":<ms>,"reason":"normal"}"#,
        ),
        varies(
            "presence/sensor-b",
            1,
            true,
            r#"{"clientid":"sensor-b","status":"offline","since":<ms>,"reason":"tcp_closed"}"#,
        ),
        varies(
            "presence/sensor-c",
            1,
            true,
            r#"{"clientid":"sensor-c","status":"online","since":<ms>}"#,
        ),
    ],
};

const R12: Recipe = Recipe {
    file: "12-last-known-state.toml",
    listing: &[
        r#"  clear_state (enabled): FROM "devices/+/decommission", 1 action(s)"#,
        r#"  last_known_state (enabled): FROM "devices/+/telemetry", 1 action(s)"#,
    ],
    run: Run::Publishes,
    proves: "each reading is copied to state/<device>, retained; the decommission publishes \
             an empty retained message that deletes state/d3, so a late subscriber gets d1's \
             latest and d2's, and nothing for d3",
    pubs: &[
        publish(
            "d1",
            "devices/d1/telemetry",
            1,
            r#"{"temp": 20}"#,
            &[out("state/d1", 1, r#"{"temp": 20}"#)],
        ),
        publish(
            "d1",
            "devices/d1/telemetry",
            1,
            r#"{"temp": 23}"#,
            &[out("state/d1", 1, r#"{"temp": 23}"#)],
        ),
        publish(
            "d2",
            "devices/d2/telemetry",
            1,
            r#"{"temp": 30}"#,
            &[out("state/d2", 1, r#"{"temp": 30}"#)],
        ),
        publish(
            "d3",
            "devices/d3/telemetry",
            1,
            r#"{"temp": 31}"#,
            &[out("state/d3", 1, r#"{"temp": 31}"#)],
        ),
        publish(
            "ops",
            "devices/d3/decommission",
            1,
            "",
            &[out("state/d3", 1, "")],
        ),
    ],
    events: &[],
    late_filter: Some("state/#"),
    retained: &[
        kept("state/d1", 1, r#"{"temp": 23}"#),
        kept("state/d2", 1, r#"{"temp": 30}"#),
    ],
};

const R13: Recipe = Recipe {
    file: "13-choose-qos.toml",
    listing: &[
        r#"  alarm_upgrade (enabled): FROM "tele/+/data", 1 action(s)"#,
        r#"  dashboard_copy (enabled): FROM "tele/+/data", 1 action(s)"#,
        r#"  mirror_forgot_qos (enabled): FROM "tele/+/data", 1 action(s)"#,
        r#"  mirror_keep_qos (enabled): FROM "tele/+/data", 1 action(s)"#,
    ],
    run: Run::Publishes,
    proves: "the alarm goes out at QoS 2 from a QoS 0 reading, the dashboard copy is always \
             QoS 0, the mirror that selects qos keeps the device's, and the one that does not \
             (the trap) copies a QoS 2 reading at QoS 0",
    pubs: &[
        publish(
            "t-1",
            "tele/t-1/data",
            0,
            r#"{"alarm": true, "code": "E42"}"#,
            &[
                out("alarms/t-1", 2, r#"{"device":"t-1","code":"E42"}"#),
                out("dash/t-1", 0, r#"{"alarm": true, "code": "E42"}"#),
                out("mirror_gotcha/t-1", 0, r#"{"alarm": true, "code": "E42"}"#),
                out("mirror/t-1", 0, r#"{"alarm": true, "code": "E42"}"#),
            ],
        ),
        publish(
            "t-1",
            "tele/t-1/data",
            2,
            r#"{"alarm": false, "v": 7}"#,
            &[
                out("dash/t-1", 0, r#"{"alarm": false, "v": 7}"#),
                out("mirror_gotcha/t-1", 0, r#"{"alarm": false, "v": 7}"#),
                out("mirror/t-1", 2, r#"{"alarm": false, "v": 7}"#),
            ],
        ),
    ],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R14: Recipe = Recipe {
    file: "14-parse-csv-and-key-value.toml",
    listing: &[
        r#"  csv_to_json (enabled): FROM "legacy/+/csv", 1 action(s)"#,
        r#"  kv_to_json (enabled): FROM "legacy/+/kv", 1 action(s)"#,
    ],
    run: Run::Publishes,
    proves: "CSV and key=value text become typed JSON; an empty CSV field and a missing key \
             become null or the default rather than failing the rule, and a CSV line missing a \
             column derives nothing",
    pubs: &[
        publish(
            "logger-1",
            "legacy/logger-1/csv",
            0,
            "2026-10-07T12:00:00Z,pump-7,48.2,1",
            &[out(
                "parsed/logger-1/csv",
                0,
                r#"{"time":"2026-10-07T12:00:00Z","device":"pump-7","temp":48.2,"running":true}"#,
            )],
        ),
        publish(
            "logger-1",
            "legacy/logger-1/csv",
            0,
            "2026-10-07T12:00:05Z,pump-8,,0",
            &[out(
                "parsed/logger-1/csv",
                0,
                r#"{"time":"2026-10-07T12:00:05Z","device":"pump-8","temp":null,"running":false}"#,
            )],
        ),
        // A line with a missing column fails the rule (nth(3) is past the end): nothing
        // is published, and the original is still delivered.
        publish(
            "logger-1",
            "legacy/logger-1/csv",
            0,
            "2026-10-07T12:00:10Z,pump-9",
            &[],
        ),
        publish(
            "logger-2",
            "legacy/logger-2/kv",
            0,
            "temp=21.5;state=ON;bat=3.61",
            &[out(
                "parsed/logger-2/kv",
                0,
                r#"{"temp":21.5,"battery_v":3.61,"state":"ON"}"#,
            )],
        ),
        // No state: the default.
        publish(
            "logger-2",
            "legacy/logger-2/kv",
            0,
            "temp=-3.0;bat=3.20",
            &[out(
                "parsed/logger-2/kv",
                0,
                r#"{"temp":-3.0,"battery_v":3.2,"state":"unknown"}"#,
            )],
        ),
        // No temp and no bat: nulls.
        publish(
            "logger-2",
            "legacy/logger-2/kv",
            0,
            "state=ON",
            &[out(
                "parsed/logger-2/kv",
                0,
                r#"{"temp":null,"battery_v":null,"state":"ON"}"#,
            )],
        ),
    ],
    events: &[],
    late_filter: None,
    retained: &[],
};

const R15: Recipe = Recipe {
    file: "15-decode-binary-payloads.toml",
    listing: &[
        r#"  decode_binary_frame (enabled): FROM "bin/+/up", 2 action(s)"#,
        r#"  lorawan_uplink (enabled): FROM "lora/v3/+/devices/+/up", 1 action(s)"#,
        r#"  unwrap_base64_json (enabled): FROM "cloud/push", 1 action(s)"#,
    ],
    run: Run::Publishes,
    proves: "base64-wrapped JSON is unwrapped, a LoRaWAN frm_payload becomes hex, a raw \
             binary frame is decoded (its temperature as a signed 16-bit number), and the raw \
             bytes are archived unchanged",
    pubs: &[
        publish(
            "push-svc",
            "cloud/push",
            0,
            r#"{"deviceId": "door-5", "data": "eyJ0ZW1wIjogMjIuNSwgImRvb3IiOiAiY2xvc2VkIn0="}"#,
            &[out(
                "unwrapped/door-5",
                0,
                r#"{"temp":22.5,"door":"closed"}"#,
            )],
        ),
        publish(
            "lns",
            "lora/v3/myapp/devices/lht-1/up",
            0,
            r#"{"end_device_ids": {"device_id": "lht-1"}, "uplink_message": {"f_port": 2, "frm_payload": "AQnE", "rx_metadata": [{"gateway_ids": {"gateway_id": "gw-a"}, "rssi": -97}]}}"#,
            &[out(
                "lora/hex/lht-1",
                0,
                r#"{"device":"lht-1","port":2,"hex":"0109C4","rssi":-97}"#,
            )],
        ),
        Pub {
            client: "nb-1",
            v5: false,
            topic: "bin/nb-1/up",
            qos: 0,
            retain: false,
            body: Body::Bytes(b"\x01\x09\xc4"),
            props: &[],
            derived: &[
                out(
                    "decoded/nb-1",
                    0,
                    r#"{"type":1,"temp_c":25.0,"raw":"AQnE"}"#,
                ),
                Msg {
                    topic: "archive/nb-1",
                    qos: 0,
                    retain: false,
                    body: Body::Bytes(b"\x01\x09\xc4"),
                    props: &[],
                },
            ],
        },
        Pub {
            client: "nb-1",
            v5: false,
            topic: "bin/nb-1/up",
            qos: 0,
            retain: false,
            body: Body::Bytes(b"\x02\xff\x9c"),
            props: &[],
            derived: &[
                out(
                    "decoded/nb-1",
                    0,
                    r#"{"type":2,"temp_c":-1.0,"raw":"Av+c"}"#,
                ),
                Msg {
                    topic: "archive/nb-1",
                    qos: 0,
                    retain: false,
                    body: Body::Bytes(b"\x02\xff\x9c"),
                    props: &[],
                },
            ],
        },
    ],
    events: &[],
    late_filter: None,
    retained: &[],
};

/// Recipe 16's user properties as the MQTT 5 client sends them: wire order, a repeated key.
const ORDER_PROPS: &[Prop] = &[
    Prop::ContentType("application/vnd.shop+json"),
    Prop::User("tenant", "acme"),
    Prop::User("trace-id", "abc123"),
    Prop::User("tag", "red"),
    Prop::User("tag", "fragile"),
];

const R16: Recipe = Recipe {
    file: "16-mqtt5-properties.toml",
    listing: &[
        r#"  order_audit_copy (enabled): FROM "orders/+", 1 action(s)"#,
        r#"  order_router (enabled): FROM "orders/+", 1 action(s)"#,
    ],
    run: Run::Publishes,
    proves: "the audit copy carries every user property in wire order with repeats and the \
             original Content-Type (or the default for a 3.1.1 publisher); the router routes by \
             the tenant property, keeps the last value of a repeated key, adds processed-by, \
             and sets Content-Type and a one-hour expiry",
    pubs: &[
        Pub {
            client: "shop-1",
            v5: true,
            topic: "orders/new",
            qos: 1,
            retain: false,
            body: Body::Text(r#"{"order": 1001, "sku": "A-7"}"#),
            props: ORDER_PROPS,
            derived: &[
                Msg {
                    topic: "audit/orders",
                    qos: 1,
                    retain: false,
                    body: Body::Text(r#"{"order": 1001, "sku": "A-7"}"#),
                    props: ORDER_PROPS,
                },
                Msg {
                    topic: "tenants/acme/orders",
                    qos: 1,
                    retain: false,
                    body: Body::Text(r#"{"order": 1001, "sku": "A-7"}"#),
                    props: &[
                        Prop::Expiry(3600),
                        Prop::ContentType("application/json"),
                        Prop::User("tenant", "acme"),
                        Prop::User("trace-id", "abc123"),
                        Prop::User("tag", "fragile"),
                        Prop::User("processed-by", "order_router"),
                    ],
                },
            ],
        },
        // An MQTT 3.1.1 client can send no properties: no tenant, so no route, and the
        // audit copy's Content-Type is the rule's default.
        publish(
            "legacy-311",
            "orders/new",
            1,
            r#"{"order": 1002}"#,
            &[Msg {
                topic: "audit/orders",
                qos: 1,
                retain: false,
                body: Body::Text(r#"{"order": 1002}"#),
                props: &[Prop::ContentType("application/octet-stream")],
            }],
        ),
    ],
    events: &[],
    late_filter: None,
    retained: &[],
};

/// Every recipe, in file order.
const RECIPES: [&Recipe; 16] = [
    &R01, &R02, &R03, &R04, &R05, &R06, &R07, &R08, &R09, &R10, &R11, &R12, &R13, &R14, &R15, &R16,
];

/// Recipe 01's offline half: `mqttd --rule-test` runs a statement against a sample
/// message. (statement, topic, payload, exit code, what the terminal shows).
const RULE_TESTS: [(&str, &str, &str, i32, &str); 3] = [
    (
        r#"SELECT topic(2) AS machine, payload.temp AS temp FROM "machines/+/telemetry" WHERE payload.temp >= 75"#,
        "machines/m-7/telemetry",
        r#"{"temp": 80.5}"#,
        0,
        r#"{"machine":"m-7","temp":80.5}"#,
    ),
    (
        r#"SELECT topic(2) AS machine, payload.temp AS temp FROM "machines/+/telemetry" WHERE payload.temp >= 75"#,
        "machines/m-7/telemetry",
        r#"{"temp": 70}"#,
        0,
        "(no output: the statement's WHERE / INCASE did not match this message)",
    ),
    (
        r#"SELECT topic(2) AS machine, payload.temp AS temp FROM "machines/+/telemetry" WHERE payload.temp >= 75"#,
        "machines/m-7/telemetry",
        "hot",
        1,
        "rule test FAILED: payload is not JSON, so payload.<field> is unreadable (invalid JSON: expected value at line 1 column 1)",
    ),
];

/// The broker's log line for recipe 01's console action.
const CONSOLE_LINE: &str = r#"<logtime>  INFO mqttd::rules: rule console action rule=debug_factory output={"id":"<id>","clientid":"plc-1","payload":"{\"t\": 20.5}","peerhost":"127.0.0.1","peername":"127.0.0.1:<port>","topic":"factory/line1/temp","qos":1,"flags":{"dup":false,"retain":false},"pub_props":{"User-Property":{}},"publish_received_at":<ms>,"client_attrs":{},"event":"message.publish","timestamp":<ms>,"node":"node-local","metadata":{"rule_id":"debug_factory"}}"#;

/// Recipe 01's gotcha: a device that sends bytes that are not text.
const BINARY_PUB: Pub = Pub {
    client: "plc-2",
    v5: false,
    topic: "factory/line1/raw",
    qos: 0,
    retain: false,
    body: Body::Bytes(b"\x01\xff\xc4"),
    props: &[],
    derived: &[],
};

/// A text payload with MQTT 5 Correlation-Data that is not text: the same gotcha, which
/// the cookbook states in prose.
const CORRELATION_PUB: Pub = Pub {
    client: "plc-3",
    v5: true,
    topic: "factory/line1/temp",
    qos: 0,
    retain: false,
    body: Body::Text(r#"{"t": 20.5}"#),
    props: &[Prop::Correlation(b"\x01\xff\xc4")],
    derived: &[],
};

/// What recipe 01 logs, in place of its console line, for a message it cannot encode.
const BINARY_WARN: &str = r#"<logtime>  WARN mqttd::rules: rule action failed (counted in mqttd_rule_actions_total{result="failed"}; this rule's further failures within 10s are logged at debug) rule=debug_factory error=cannot JSON-encode binary (non-UTF-8) data; select base64_encode(...) or bin2hexstr(...) of it instead"#;

/// The rule the cookbook gives in recipe 01's place for a device that sends bytes, as the
/// page quotes it.
const HEX_DEBUG: &str = r#"[rules.debug_factory]
description = "Log every message under factory/, its payload as hex"
sql = 'SELECT clientid, topic, qos, bin2hexstr(payload) AS payload_hex FROM "factory/#"'
actions = [{ function = "console" }]
"#;

/// What [`HEX_DEBUG`] logs for recipe 01's own (text) publish: it works for text too.
const HEX_TEXT_LINE: &str = r#"<logtime>  INFO mqttd::rules: rule console action rule=debug_factory output={"clientid":"plc-1","topic":"factory/line1/temp","qos":1,"payload_hex":"7B2274223A2032302E357D"}"#;

/// What [`HEX_DEBUG`] logs for [`BINARY_PUB`].
const HEX_CONSOLE_LINE: &str = r#"<logtime>  INFO mqttd::rules: rule console action rule=debug_factory output={"clientid":"plc-2","topic":"factory/line1/raw","qos":0,"payload_hex":"01FFC4"}"#;

// ---------------------------------------------------------------------------------------
// How the cookbook shows things.
// ---------------------------------------------------------------------------------------

/// The `-F` format of the subscriber the cookbook runs.
const SUB_FORMAT: &str = "%t  qos=%q retain=%r  %p";
/// The settings the quick start starts the broker with, beside the bind address and the
/// rules file. The suite starts every broker with exactly these.
const QUICK_START_ENV: [(&str, &str); 2] = [
    ("MQTTD_ALLOW_ANONYMOUS", "1"),
    ("MQTTD_DURABLE_SESSIONS", "0"),
];
/// The quick start's heading.
const QUICK_START: &str = "Run any recipe in under a minute";
/// The package the quick start builds, and the binary it puts on `PATH`.
const PACKAGE: &str = "mqttd";
/// A message no recipe matches: once it arrives, everything published before it has.
const SENTINEL_TOPIC: &str = "cookbook/end";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A recipe file's path as the cookbook writes it, relative to the repository root.
fn recipe_rel(file: &str) -> String {
    format!("docs/examples/rules/{file}")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn cookbook() -> String {
    read("docs/RULES-COOKBOOK.md")
}

/// A recipe's number: 2 for `02-....toml`.
fn number(r: &Recipe) -> u8 {
    r.file[..2]
        .parse()
        .expect("a recipe file starts with its number")
}

/// `bytes` in lower-case hex, as `mosquitto_sub`'s `%x` prints a payload.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn body_text(body: &Body) -> String {
    match body {
        Body::Text(s) | Body::Varies(s) => (*s).to_string(),
        Body::Bytes(b) => hex(b),
    }
}

/// A message as the cookbook's subscriber prints it ([`SUB_FORMAT`]), trailing blanks
/// trimmed (an empty payload ends the line).
fn line(m: &Msg) -> String {
    format!(
        "{}  qos={} retain={}  {}",
        m.topic,
        m.qos,
        u8::from(m.retain),
        body_text(&m.body)
    )
    .trim_end()
    .to_string()
}

/// `p` as its publisher sends it, as the cookbook shows it in a recipe's header.
fn published(p: &Pub) -> Msg {
    Msg {
        retain: p.retain,
        ..original(p)
    }
}

/// The original of `p` as a subscriber receives it: unchanged, and live, so not retained.
fn original(p: &Pub) -> Msg {
    Msg {
        topic: p.topic,
        qos: p.qos,
        retain: false,
        body: p.body,
        props: p.props,
    }
}

/// A `mosquitto_pub` argument as a shell reads it.
fn quoted(s: &str) -> String {
    assert!(
        !s.contains('\''),
        "the cookbook quotes arguments in '': {s}"
    );
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || "/_-.+=".contains(c))
    {
        s.to_string()
    } else {
        format!("'{s}'")
    }
}

/// The `mosquitto_pub` command the cookbook shows for `p`.
fn pub_cmd(p: &Pub) -> String {
    let mut args = vec!["mosquitto_pub".to_string()];
    if p.v5 {
        args.push("-V 5".into());
    }
    args.push(format!("-i {}", p.client));
    if p.qos > 0 {
        args.push(format!("-q {}", p.qos));
    }
    if p.retain {
        args.push("-r".into());
    }
    for prop in p.props {
        args.push(match prop {
            Prop::User(k, v) => format!("-D publish user-property {k} {v}"),
            Prop::ContentType(c) => format!("-D publish content-type {c}"),
            Prop::Expiry(s) => format!("-D publish message-expiry-interval {s}"),
            Prop::Correlation(_) => panic!("the cookbook shows no publish with Correlation-Data"),
        });
    }
    args.push(format!("-t {}", quoted(p.topic)));
    args.push(match p.body {
        Body::Text("") => "-n".into(),
        Body::Text(s) => format!("-m {}", quoted(s)),
        Body::Bytes(_) => "-s".into(),
        Body::Varies(_) => panic!("a publish has a fixed payload"),
    });
    let cmd = args.join(" ");
    match p.body {
        // printf takes octal escapes everywhere; \x is not POSIX.
        Body::Bytes(b) => {
            let octal = b.iter().fold(String::new(), |mut s, x| {
                let _ = write!(s, "\\{x:03o}");
                s
            });
            format!("printf '{octal}' | {cmd}")
        }
        _ => cmd,
    }
}

/// The subscriber command the cookbook shows for `filter`.
fn sub_cmd(filter: &str) -> String {
    format!("mosquitto_sub -q 2 -t '{filter}' -F '{SUB_FORMAT}'")
}

/// The command that starts recipe 11's device `id`, which stays connected.
fn stay_cmd(id: &str) -> String {
    format!("mosquitto_sub -i {id} -t 'cmd/{id}' &")
}

/// The commands that build the broker and put it on `PATH`.
fn build_cmds() -> [String; 2] {
    [
        format!("cargo build --release -p {PACKAGE}"),
        r#"export PATH="$PWD/target/release:$PATH""#.to_string(),
    ]
}

/// The command that starts the quick start's broker with `file`.
fn start_cmd(file: &str) -> String {
    let env: Vec<String> = QUICK_START_ENV
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    format!(
        "MQTTD_PLAINTEXT_BIND=127.0.0.1:1883 {} \\\n  MQTTD_RULES_FILE={} {PACKAGE}",
        env.join(" "),
        recipe_rel(file)
    )
}

/// The `mqttd --rule-test` command the cookbook shows.
fn rule_test_cmd(sql: &str, topic: &str, payload: &str) -> String {
    format!(
        "mqttd --rule-test --sql {} --topic {} --payload {}",
        quoted(sql),
        quoted(topic),
        quoted(payload)
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes).as_ref())
}

/// What `mqttd --check-rules docs/examples/rules/<file>` prints for a recipe.
fn check_rules_stdout(r: &Recipe) -> String {
    let n = r.listing.len();
    let mut s = format!(
        "rules OK: {}: {n} rule(s), {n} enabled, sha256 {}\n",
        recipe_rel(r.file),
        sha256_hex(read(&recipe_rel(r.file)).as_bytes())
    );
    for l in r.listing {
        s.push_str(l);
        s.push('\n');
    }
    s
}

/// The line the broker logs once it has loaded a recipe.
fn loaded_line(r: &Recipe) -> String {
    loaded_line_for(&read(&recipe_rel(r.file)), r.listing.len())
}

/// The line the broker logs once it has loaded `rules` enabled rules from `text`.
fn loaded_line_for(text: &str, rules: usize) -> String {
    format!(
        "<logtime>  INFO mqttd: rule engine: rules loaded (ADR 0083) rules={rules} enabled={rules} \
         digest={}",
        sha256_hex(text.as_bytes())
    )
}

/// Recipe 16's properties table row for one message.
fn props_row(m: &Msg) -> String {
    let users: Vec<String> = m
        .props
        .iter()
        .filter_map(|p| match p {
            Prop::User(k, v) => Some(format!("`{k}={v}`")),
            _ => None,
        })
        .collect();
    let content_type = m.props.iter().find_map(|p| match p {
        Prop::ContentType(c) => Some(format!("`{c}`")),
        _ => None,
    });
    let expiry = m.props.iter().find_map(|p| match p {
        Prop::Expiry(s) => Some(format!("{s} s")),
        _ => None,
    });
    format!(
        "| `{}` | {} | {} | {} |",
        m.topic,
        if users.is_empty() {
            "none".to_string()
        } else {
            users.join(" ")
        },
        content_type.unwrap_or_else(|| "none".into()),
        expiry.unwrap_or_else(|| "none".into())
    )
}

/// The properties of `m` as a recipe file's header lists them, in an indented `(...)`
/// note under its `In:` or `Out:` line; `MQTT 5, ` first for a message an MQTT 5 client
/// publishes. None for a message without properties.
fn props_note(m: &Msg, sent_by_v5: bool) -> Option<String> {
    if m.props.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    if sent_by_v5 {
        parts.push("MQTT 5".to_string());
    }
    let users: Vec<String> = m
        .props
        .iter()
        .filter_map(|p| match p {
            Prop::User(k, v) => Some(format!("{k}={v}")),
            _ => None,
        })
        .collect();
    if !users.is_empty() {
        parts.push(format!("user properties {}", users.join(", ")));
    }
    parts.extend(m.props.iter().find_map(|p| match p {
        Prop::ContentType(c) => Some(format!("Content-Type {c}")),
        _ => None,
    }));
    parts.extend(m.props.iter().find_map(|p| match p {
        Prop::Expiry(s) => Some(format!("Message-Expiry-Interval {s}")),
        _ => None,
    }));
    parts.extend(m.props.iter().find_map(|p| match p {
        Prop::Correlation(b) => Some(format!("Correlation-Data {}", hex(b))),
        _ => None,
    }));
    Some(format!("({})", parts.join(", ")))
}

/// The messages a `#` subscriber receives from a recipe's publishes, in routing order.
fn stream(r: &Recipe) -> Vec<Msg> {
    r.pubs
        .iter()
        .flat_map(|p| std::iter::once(original(p)).chain(p.derived.iter().copied()))
        .collect()
}

// ---------------------------------------------------------------------------------------
// Values that differ on every run.
// ---------------------------------------------------------------------------------------

/// Days since 1970-01-01 to (year, month, day), in the proleptic Gregorian calendar.
fn civil(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

/// `secs` since the epoch as (date, time): `YYYY-MM-DD`, `HH:MM:SS`.
fn date_time(secs: u64) -> (String, String) {
    let (y, mo, d) = civil(secs / 86_400);
    let s = secs % 86_400;
    (
        format!("{y:04}-{mo:02}-{d:02}"),
        format!("{:02}:{:02}:{:02}", s / 3600, s % 3600 / 60, s % 60),
    )
}

/// What `unix_ts_to_rfc3339(ms, 'millisecond')` renders.
fn utc(ms: u64) -> String {
    let (date, time) = date_time(ms / 1000);
    format!("{date}T{time}.{:03}+00:00", ms % 1000)
}

/// What recipe 08's `format_date('millisecond', '+02:00', '%Y-%m-%d %H:%M:%S', ms)` renders.
fn plant(ms: u64) -> String {
    let (date, time) = date_time(ms / 1000 + 2 * 3600);
    format!("{date} {time}")
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// Match `text` against `pattern`, whose holes stand for values that differ on every run:
/// `<ms>` milliseconds since the epoch (13 digits), `<id>` a message id (32 upper-case hex
/// digits), `<port>` a port number, `<logtime>` a log line's timestamp, and `<utc>` and
/// `<plant>` the preceding `<ms>` as recipe 08 renders it. Returns every `<ms>`.
fn match_pattern(pattern: &str, text: &str) -> Result<Vec<u64>, String> {
    let mut ms = Vec::new();
    let (mut pat, mut rest) = (pattern, text);
    while !pat.is_empty() {
        let lit = pat.find('<').unwrap_or(pat.len());
        let (literal, tail) = pat.split_at(lit);
        rest = rest
            .strip_prefix(literal)
            .ok_or_else(|| format!("expected {literal:?} at {rest:?}"))?;
        if tail.is_empty() {
            break;
        }
        let close = tail.find('>').ok_or("unclosed hole")?;
        let hole = &tail[1..close];
        pat = &tail[close + 1..];
        let run = |ok: fn(char) -> bool| rest.find(|c: char| !ok(c)).unwrap_or(rest.len());
        let take = match hole {
            "ms" => {
                let n = run(|c| c.is_ascii_digit());
                if n != 13 {
                    return Err(format!("expected 13 digits of milliseconds at {rest:?}"));
                }
                ms.push(rest[..n].parse().map_err(|e| format!("{e}"))?);
                n
            }
            "id" => {
                let n = run(|c| c.is_ascii_digit() || ('A'..='F').contains(&c));
                if n != 32 {
                    return Err(format!("expected a 32-digit message id at {rest:?}"));
                }
                n
            }
            "port" => {
                let n = run(|c| c.is_ascii_digit());
                if n > 5 {
                    return Err(format!("expected a port number at {rest:?}"));
                }
                n
            }
            "logtime" => run(|c| c.is_ascii_digit() || "-T:.Z".contains(c)),
            "utc" | "plant" => {
                let last = *ms.last().ok_or("no <ms> before a rendered time")?;
                let want = if hole == "utc" {
                    utc(last)
                } else {
                    plant(last)
                };
                if !rest.starts_with(&want) {
                    return Err(format!("expected {want:?} (from {last}) at {rest:?}"));
                }
                want.len()
            }
            other => return Err(format!("unknown hole <{other}>")),
        };
        if take == 0 {
            return Err(format!("expected <{hole}> at {rest:?}"));
        }
        rest = &rest[take..];
    }
    if rest.is_empty() {
        Ok(ms)
    } else {
        Err(format!("unexpected trailing {rest:?}"))
    }
}

/// Whether a line the cookbook shows (or a header claims) is `m`'s line.
fn shows(m: &Msg, shown: &str) -> bool {
    let want = line(m);
    match m.body {
        Body::Varies(_) => match_pattern(&want, shown.trim_end()).is_ok(),
        _ => want == shown.trim_end(),
    }
}

// ---------------------------------------------------------------------------------------
// The page: sections and fenced blocks.
// ---------------------------------------------------------------------------------------

/// The page's `## ` sections, in order: (heading, its text up to the next `## ` heading).
/// The text before the first heading is the section with an empty heading.
fn sections(doc: &str) -> Vec<(String, String)> {
    let mut found = vec![(String::new(), String::new())];
    let mut in_block = false;
    for l in doc.lines() {
        if l.starts_with("```") {
            in_block = !in_block;
        }
        if let Some(heading) = l.strip_prefix("## ").filter(|_| !in_block) {
            found.push((heading.to_string(), String::new()));
        }
        let text = &mut found.last_mut().expect("a section").1;
        text.push_str(l);
        text.push('\n');
    }
    found
}

/// A recipe's section: `## 2. ...` for `02-....toml`.
fn recipe_section(doc: &str, r: &Recipe) -> String {
    let prefix = format!("{}. ", number(r));
    let mut found = sections(doc)
        .into_iter()
        .filter(|(h, _)| h.starts_with(&prefix));
    let (_, text) = found
        .next()
        .unwrap_or_else(|| panic!("docs/RULES-COOKBOOK.md has no `## {prefix}...` section"));
    assert!(
        found.next().is_none(),
        "docs/RULES-COOKBOOK.md has two `## {prefix}...` sections"
    );
    text
}

/// Every fenced block of `text`: its language and its lines, trailing blanks trimmed.
fn blocks(text: &str) -> Vec<(String, Vec<String>)> {
    let mut found = Vec::new();
    let mut open: Option<(String, Vec<String>)> = None;
    for l in text.lines() {
        match open.take() {
            None => {
                if let Some(lang) = l.strip_prefix("```") {
                    open = Some((lang.trim().to_string(), Vec::new()));
                }
            }
            Some(block) if l.trim_end() == "```" => found.push(block),
            Some((lang, mut lines)) => {
                lines.push(l.trim_end().to_string());
                open = Some((lang, lines));
            }
        }
    }
    assert!(open.is_none(), "an unclosed fenced block: {open:?}");
    found
}

/// A line the page must show: exactly, or matching a [`match_pattern`] pattern.
enum Want {
    Exact(String),
    Pattern(String),
}

impl Want {
    fn of(m: &Msg) -> Self {
        match m.body {
            Body::Varies(_) => Self::Pattern(line(m)),
            _ => Self::Exact(line(m)),
        }
    }

    fn matches(&self, shown: &str) -> bool {
        match self {
            Self::Exact(s) => s == shown,
            Self::Pattern(p) => match_pattern(p, shown).is_ok(),
        }
    }

    fn text(&self) -> &str {
        match self {
            Self::Exact(s) | Self::Pattern(s) => s,
        }
    }
}

/// A fenced block the page must show. In a `sh` block, comment and blank lines are not
/// compared.
struct WantBlock {
    lang: &'static str,
    lines: Vec<Want>,
}

impl WantBlock {
    fn exact(lang: &'static str, lines: impl IntoIterator<Item = String>) -> Self {
        Self {
            lang,
            lines: lines.into_iter().map(Want::Exact).collect(),
        }
    }

    fn messages<'a>(msgs: impl IntoIterator<Item = &'a Msg>) -> Self {
        Self {
            lang: "text",
            lines: msgs.into_iter().map(Want::of).collect(),
        }
    }

    fn pattern(pattern: &str) -> Self {
        Self {
            lang: "text",
            lines: vec![Want::Pattern(pattern.to_string())],
        }
    }

    fn matches(&self, (lang, lines): &(String, Vec<String>)) -> bool {
        let lines: Vec<&String> = lines
            .iter()
            .filter(|s| self.lang != "sh" || !(s.is_empty() || s.starts_with('#')))
            .collect();
        lang == self.lang
            && lines.len() == self.lines.len()
            && lines.iter().zip(&self.lines).all(|(s, w)| w.matches(s))
    }

    fn render(&self) -> String {
        let lines: Vec<&str> = self.lines.iter().map(Want::text).collect();
        format!("```{}\n{}\n```", self.lang, lines.join("\n"))
    }
}

/// Assert `heading`'s section shows exactly `want`: the same blocks, in the same order,
/// none missing and none extra.
fn assert_blocks(heading: &str, text: &str, want: &[WantBlock]) {
    let found = blocks(text);
    let bad = (0..found.len().max(want.len())).find(|&i| match (found.get(i), want.get(i)) {
        (Some(f), Some(w)) => !w.matches(f),
        _ => true,
    });
    let Some(i) = bad else { return };
    let wanted = want.get(i).map_or_else(
        || "nothing: the page has a block this suite does not run".to_string(),
        WantBlock::render,
    );
    let shown = found.get(i).map_or_else(
        || "nothing".to_string(),
        |(lang, lines)| format!("```{lang}\n{}\n```", lines.join("\n")),
    );
    panic!(
        "docs/RULES-COOKBOOK.md, section `## {heading}`: block {} of the {} this suite runs \
         should be (holes like <ms> stand for a value of that shape)\n{wanted}\nbut the page \
         shows\n{shown}",
        i + 1,
        want.len()
    );
}

/// The quick start's blocks: build, check recipe 02, start the broker with it, the log
/// line that proves it loaded, the watcher, one publish and what the watcher prints.
fn quick_start_blocks() -> Vec<WantBlock> {
    let hot = &R02.pubs[2];
    vec![
        WantBlock::exact("sh", build_cmds()),
        WantBlock::exact(
            "sh",
            [format!("mqttd --check-rules {}", recipe_rel(R02.file))],
        ),
        WantBlock::exact("text", check_rules_stdout(&R02).lines().map(String::from)),
        WantBlock::exact("sh", start_cmd(R02.file).lines().map(String::from)),
        WantBlock::pattern(&loaded_line(&R02)),
        WantBlock::exact("sh", [sub_cmd("#")]),
        WantBlock::exact("sh", [pub_cmd(hot)]),
        WantBlock::messages(
            &std::iter::once(original(hot))
                .chain(hot.derived.iter().copied())
                .collect::<Vec<_>>(),
        ),
    ]
}

/// A recipe section's blocks: the file verbatim, then what its run shows.
fn recipe_blocks(r: &Recipe) -> Vec<WantBlock> {
    let file = WantBlock::exact("toml", read(&recipe_rel(r.file)).lines().map(String::from));
    let late = |filter: &str| {
        [
            WantBlock::exact("sh", [sub_cmd(filter)]),
            WantBlock::messages(
                r.retained
                    .iter()
                    .filter(|m| filter_matches(filter, m.topic)),
            ),
        ]
    };
    let mut want = vec![file];
    if r.run == Run::Presence {
        let mut steps: Vec<String> = STAYS.iter().map(|id| stay_cmd(id)).collect();
        steps.extend(r.pubs.iter().map(pub_cmd));
        steps.push(KILL_FIRST.to_string());
        want.push(WantBlock::exact("sh", [sub_cmd(PRESENCE)]));
        want.push(WantBlock::exact("sh", steps));
        want.push(WantBlock::messages(r.events));
        want.extend(late(PRESENCE));
        return want;
    }
    want.push(WantBlock::exact("sh", r.pubs.iter().map(pub_cmd)));
    want.push(WantBlock::messages(&stream(r)));
    if r.run == Run::ConsoleLog {
        want.push(WantBlock::pattern(CONSOLE_LINE));
        for (sql, topic, payload, _, shown) in RULE_TESTS {
            want.push(WantBlock::exact("sh", [rule_test_cmd(sql, topic, payload)]));
            want.push(WantBlock::exact("text", [shown.to_string()]));
        }
        want.push(WantBlock::exact("sh", [pub_cmd(&BINARY_PUB)]));
        want.push(WantBlock::pattern(BINARY_WARN));
        want.push(WantBlock::exact(
            "toml",
            HEX_DEBUG.lines().map(String::from),
        ));
        want.push(WantBlock::pattern(HEX_CONSOLE_LINE));
    }
    if let Some(filter) = r.late_filter {
        want.extend(late(filter));
    }
    want
}

/// The blocks the section under `heading` must show: the quick start's, a recipe's, or
/// none.
fn section_blocks(heading: &str) -> Vec<WantBlock> {
    if heading == QUICK_START {
        return quick_start_blocks();
    }
    let Some(n) = heading
        .split_once(". ")
        .and_then(|(n, _)| n.parse::<u8>().ok())
    else {
        return Vec::new();
    };
    let r = RECIPES.iter().find(|r| number(r) == n).unwrap_or_else(|| {
        panic!("docs/RULES-COOKBOOK.md has a section for recipe {n}, which has no file")
    });
    recipe_blocks(r)
}

/// Whether `filter` matches `topic` (MQTT rules; no `$` topics here).
fn filter_matches(filter: &str, topic: &str) -> bool {
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) | (None, None) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            _ => return false,
        }
    }
}

// ---------------------------------------------------------------------------------------
// A recipe file's header.
// ---------------------------------------------------------------------------------------

/// The keys of a header's example lines.
const CLAIM_KEYS: [&str; 3] = ["In", "Out", "Late"];

/// One example line of a recipe file's header (`# In: <message>`, `# Out: ...` or
/// `# Late: ...`) and the `(...)` property note indented under it, if any.
#[derive(Debug)]
struct Claim {
    key: &'static str,
    shown: String,
    note: Option<String>,
}

/// The example lines of `file`'s header, in order. A line that starts like one but is
/// not written `# In: `, `# Out: ` or `# Late: ` is an error, so a typo cannot hide a
/// line from the check; so is an indented `(...)` note under no `In:` or `Out:` line.
fn header_claims(file: &str, header: &str) -> Result<Vec<Claim>, String> {
    let mut claims: Vec<Claim> = Vec::new();
    // Whether the line before was an In:/Out: line, and whether a note under one is
    // still open (it ends with `)`).
    let (mut after_claim, mut note_open) = (false, false);
    for l in header.lines() {
        let body = l.strip_prefix('#').unwrap_or("");
        let text = body.trim();
        if note_open {
            if let Some(note) = claims.last_mut().and_then(|c| c.note.as_mut()) {
                note.push(' ');
                note.push_str(text);
            }
            note_open = !text.ends_with(')');
            continue;
        }
        if body.starts_with("  ") && text.starts_with('(') {
            let Some(last) = claims.last_mut().filter(|c| after_claim && c.key != "Late") else {
                return Err(format!(
                    "{file}: the note `{l}` is indented under no In: or Out: line"
                ));
            };
            last.note = Some(text.to_string());
            note_open = !text.ends_with(')');
            after_claim = false;
            continue;
        }
        let key = CLAIM_KEYS.into_iter().find(|k| {
            text.split_once(':')
                .is_some_and(|(word, _)| word.trim().eq_ignore_ascii_case(k))
        });
        let Some(key) = key else {
            after_claim = false;
            continue;
        };
        let Some(shown) = l.strip_prefix(&format!("# {key}: ")) else {
            return Err(format!(
                "{file}: write the header line `{l}` as `# {key}: <message>`"
            ));
        };
        claims.push(Claim {
            key,
            shown: shown.trim().to_string(),
            note: None,
        });
        after_claim = key != "Late";
    }
    if note_open {
        return Err(format!(
            "{file}: a `(...)` note in the header has no closing `)`"
        ));
    }
    Ok(claims)
}

/// Whether `c` shows `m`: its line, and its properties' note.
fn claim_is(c: &Claim, m: &Msg, sent_by_v5: bool) -> bool {
    shows(m, &c.shown) && c.note == props_note(m, sent_by_v5)
}

/// A recipe file's header: its comment lines before the first `[rules.<id>]` table.
fn header_of(r: &Recipe) -> String {
    let content = read(&recipe_rel(r.file));
    let end = content.find("\n[rules.").expect("a [rules.<id>] table");
    content[..end].to_string()
}

/// Check every example in `header` is what `r` does. Each `In:` line and the `Out:` lines
/// right under it are one publish of the table and, in order, everything it derives (none
/// when no `Out:` line follows). An `Out:` line under no `In:` line is a message derived
/// from a client event, and `Late:` lines are retained messages, each in the table's
/// order.
fn check_header(r: &Recipe, header: &str) -> Result<(), String> {
    let claims = header_claims(r.file, header)?;
    if !claims.iter().any(|c| c.key != "Late") {
        return Err(format!("{}: the header shows no In:/Out: example", r.file));
    }
    let mut events = r.events.iter();
    let mut retained = r.retained.iter();
    let mut i = 0;
    while i < claims.len() {
        let c = &claims[i];
        let ok = match c.key {
            "In" => {
                let outs: Vec<&Claim> = claims[i + 1..]
                    .iter()
                    .take_while(|c| c.key == "Out")
                    .collect();
                i += outs.len();
                r.pubs.iter().any(|p| {
                    claim_is(c, &published(p), p.v5)
                        && outs.len() == p.derived.len()
                        && outs
                            .iter()
                            .zip(p.derived)
                            .all(|(o, m)| claim_is(o, m, false))
                })
            }
            "Out" => events.any(|m| claim_is(c, m, false)),
            _ => retained.any(|m| claim_is(c, m, false)),
        };
        if !ok {
            let what = match c.key {
                "In" => {
                    "one publish of the table, with exactly what it derives on the Out: \
                         lines under it, in order"
                }
                "Out" => "(in order) a message the recipe derives from a client event",
                _ => "(in order) a retained message",
            };
            return Err(format!(
                "{}: the header's `{}: {}` {:?} is not {what}",
                r.file, c.key, c.shown, c.note
            ));
        }
        i += 1;
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// The real binary.
// ---------------------------------------------------------------------------------------

/// Kills the spawned broker when the test ends (including on panic).
struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// `mqttd` with no `MQTTD_*` from the environment the suite runs in.
fn mqttd() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            cmd.env_remove(k);
        }
    }
    // `unix_ts_to_rfc3339` writes the host's local time zone; the cookbook shows UTC.
    cmd.env("TZ", "UTC");
    cmd
}

/// Run an offline `mqttd` command from the repository root (so a path prints as the
/// cookbook writes it) and return its exit code, stdout and stderr. A regression that
/// starts a broker instead of exiting is killed by the bounded wait.
fn run_cli(args: &[&str]) -> (Option<i32>, String, String) {
    let child = mqttd()
        .current_dir(repo_root())
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mqttd");
    let mut guard = ChildGuard(child);
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = guard.0.try_wait().expect("try_wait") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "`mqttd {args:?}` did not exit within 30 s"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut stdout = String::new();
    let mut stderr = String::new();
    guard
        .0
        .stdout
        .take()
        .expect("stdout piped")
        .read_to_string(&mut stdout)
        .expect("read stdout");
    guard
        .0
        .stderr
        .take()
        .expect("stderr piped")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    (status.code(), stdout, stderr)
}

/// A broker running one rules file, started as the quick start starts it.
struct Broker {
    addr: SocketAddr,
    log: listen_wait::Log,
    _child: ChildGuard,
}

async fn start(r: &Recipe) -> Broker {
    start_file(&repo_root().join(recipe_rel(r.file)), r.listing.len()).await
}

/// A broker running the `rules` rules in `file`, started as the quick start starts one.
async fn start_file(file: &Path, rules: usize) -> Broker {
    let (child, log, addr) = listen_wait::spawn_listening_logged(|| {
        let addr: SocketAddr = format!("127.0.0.1:{}", proc_common::free_tcp_port())
            .parse()
            .unwrap();
        let mut cmd = mqttd();
        cmd.env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .envs(QUICK_START_ENV)
            .env("MQTTD_RULES_FILE", file)
            .env("RUST_LOG", "mqttd=info")
            .env("NO_COLOR", "1")
            .stderr(Stdio::null());
        (cmd, vec![addr], addr)
    })
    .await;
    let broker = Broker {
        addr,
        log,
        _child: ChildGuard(child),
    };
    // The rules line precedes the listener's: the broker is serving THIS file.
    let text = std::fs::read_to_string(file).expect("read the rules file");
    let loaded = loaded_line_for(&text, rules);
    assert!(
        broker
            .log
            .text()
            .lines()
            .any(|l| match_pattern(&loaded, l).is_ok()),
        "the broker never logged loading {}: want {loaded}\n{}",
        file.display(),
        broker.log.tail(30)
    );
    broker
}

/// The first `n` lines of `broker`'s log that contain `needle`, once they are there (at
/// most 10 s).
async fn logged(broker: &Broker, needle: &str, n: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let text = broker.log.text();
        let found: Vec<String> = text
            .lines()
            .filter(|l| l.contains(needle))
            .map(String::from)
            .collect();
        if found.len() >= n {
            return found[..n].to_vec();
        }
        assert!(
            Instant::now() < deadline,
            "{} of {n} `{needle}` line(s) in the broker's log in 10 s:\n{}",
            found.len(),
            broker.log.tail(20)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Assert `logged` is `pattern`'s line, with times from this run (`since` .. now).
fn assert_log_line(pattern: &str, logged: &str, since: u64) {
    let times = match_pattern(pattern, logged)
        .unwrap_or_else(|e| panic!("{e}\nwant {pattern}\n got {logged}"));
    let now = now_ms();
    assert!(
        times.iter().all(|t| (since..=now).contains(t)),
        "times {times:?} are not from this run ({since}..={now}): {logged}"
    );
}

fn qos(n: u8) -> QoS {
    match n {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        other => panic!("no QoS {other}"),
    }
}

fn qos_num(q: QoS) -> u8 {
    match q {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn wire_props(props: &[Prop]) -> Vec<Property> {
    props
        .iter()
        .map(|p| match *p {
            Prop::Expiry(s) => Property::MessageExpiryInterval(s),
            Prop::ContentType(c) => Property::ContentType(c.into()),
            Prop::Correlation(b) => Property::CorrelationData(bytes::Bytes::from_static(b)),
            Prop::User(k, v) => Property::UserProperty(k.into(), v.into()),
        })
        .collect()
}

/// Do what one `mosquitto_pub` does: connect, publish, finish the `QoS` exchange,
/// disconnect cleanly.
async fn mosquitto_pub(addr: SocketAddr, p: &Pub) {
    let mut c = if p.v5 {
        Client::connect_v5_ok(addr, p.client).await
    } else {
        Client::connect(addr, p.client).await
    };
    let payload = match p.body {
        Body::Text(s) => bytes::Bytes::from_static(s.as_bytes()),
        Body::Bytes(b) => bytes::Bytes::from_static(b),
        Body::Varies(_) => panic!("a publish has a fixed payload"),
    };
    c.send(&Packet::Publish(Publish {
        dup: false,
        qos: qos(p.qos),
        retain: p.retain,
        topic: p.topic.into(),
        pkid: (p.qos > 0).then_some(1),
        properties: Properties(wire_props(p.props)),
        payload,
    }))
    .await;
    match p.qos {
        0 => {}
        1 => assert_eq!(
            c.recv().await,
            Packet::PubAck(1.into()),
            "{} PUBACK",
            p.client
        ),
        _ => {
            assert_eq!(
                c.recv().await,
                Packet::PubRec(1.into()),
                "{} PUBREC",
                p.client
            );
            c.pubrel(1).await;
            assert_eq!(
                c.recv().await,
                Packet::PubComp(1.into()),
                "{} PUBCOMP",
                p.client
            );
        }
    }
    c.disconnect().await;
}

/// A `QoS` 2 subscriber to a filter. MQTT 3.1.1, as the cookbook's `mosquitto_sub -q 2`
/// is, unless `v5` (`mosquitto_sub -V 5`), which also receives the properties.
struct Watcher {
    c: Client,
    v5: bool,
}

impl Watcher {
    async fn new(addr: SocketAddr, id: &str, filter: &str, v5: bool) -> Self {
        let mut c = if v5 {
            Client::connect_v5_ok(addr, id).await
        } else {
            Client::connect(addr, id).await
        };
        let ack = c.subscribe(1, filter, QoS::ExactlyOnce).await;
        assert_eq!(
            ack.return_codes,
            vec![2],
            "{id} is granted QoS 2 on {filter}"
        );
        Self { c, v5 }
    }

    /// What this watcher receives of `m`: an MQTT 3.1.1 one gets no properties.
    fn sees(&self, m: &Msg) -> Msg {
        if self.v5 {
            *m
        } else {
            Msg { props: &[], ..*m }
        }
    }

    async fn expect(&mut self, want: &Msg, since: u64) {
        let want = self.sees(want);
        let got = next_publish(&mut self.c).await;
        assert_message(&got, &want, since);
    }

    async fn expect_batch(&mut self, wants: &[Msg], since: u64) {
        let wants: Vec<Msg> = wants.iter().map(|m| self.sees(m)).collect();
        expect_batch(&mut self.c, &wants, since).await;
    }
}

/// The next PUBLISH on `c`, acknowledged as a `QoS` 2 subscriber must: PUBACK for `QoS` 1,
/// PUBREC for `QoS` 2, and PUBCOMP for each PUBREL that arrives on the way.
async fn next_publish(c: &mut Client) -> Publish {
    loop {
        match c.recv().await {
            Packet::Publish(p) => {
                match (p.qos, p.pkid) {
                    (QoS::AtLeastOnce, Some(id)) => c.puback(id).await,
                    (QoS::ExactlyOnce, Some(id)) => c.pubrec(id).await,
                    _ => {}
                }
                return p;
            }
            Packet::PubRel(a) => c.pubcomp(a.pkid).await,
            other => panic!("expected a PUBLISH, got {other:?}"),
        }
    }
}

/// A received PUBLISH as the cookbook's subscriber prints it, and its properties.
fn received_line(p: &Publish) -> String {
    let body =
        std::str::from_utf8(&p.payload).map_or_else(|_| hex(&p.payload), ToString::to_string);
    format!(
        "{}  qos={} retain={}  {body}   {:?}",
        p.topic,
        qos_num(p.qos),
        u8::from(p.retain),
        p.properties.0
    )
}

/// Assert `got` is exactly `want`. A value that differs per run must be a time from this
/// run (`since` .. now).
fn assert_message(got: &Publish, want: &Msg, since: u64) {
    let shown = format!("{}   {:?}", line(want), wire_props(want.props));
    let ok_body = match want.body {
        Body::Text(s) => got.payload[..] == *s.as_bytes(),
        Body::Bytes(b) => got.payload[..] == *b,
        Body::Varies(pattern) => {
            let text = std::str::from_utf8(&got.payload).unwrap_or("");
            match match_pattern(pattern, text) {
                Ok(times) => {
                    let now = now_ms();
                    assert!(
                        times.iter().all(|t| (since..=now).contains(t)),
                        "times {times:?} in {} are not from this run ({since}..={now})",
                        received_line(got)
                    );
                    true
                }
                Err(_) => false,
            }
        }
    };
    assert!(
        got.topic == want.topic
            && qos_num(got.qos) == want.qos
            && got.retain == want.retain
            && ok_body
            && got.properties.0 == wire_props(want.props),
        "want {shown}\n got {}",
        received_line(got)
    );
}

/// Receive what one publish produces: `wants`, the original and then what its rules
/// derived, in the order the hub routed them. The broker sends a `QoS` 0 message the
/// moment it is routed and a `QoS` 1 or 2 message through the session's ordered queue,
/// so each of the two is in routing order but a `QoS` 0 message can overtake a `QoS` 1 or
/// 2 one routed before it. Every message must arrive, exactly as listed.
async fn expect_batch(c: &mut Client, wants: &[Msg], since: u64) {
    let mut got = Vec::new();
    for _ in wants {
        got.push(next_publish(c).await);
    }
    for class in [true, false] {
        let g: Vec<&Publish> = got
            .iter()
            .filter(|p| (p.qos == QoS::AtMostOnce) == class)
            .collect();
        let w: Vec<&Msg> = wants.iter().filter(|m| (m.qos == 0) == class).collect();
        assert_eq!(
            g.len(),
            w.len(),
            "want {:#?}\n got {:#?}",
            wants.iter().map(line).collect::<Vec<_>>(),
            got.iter().map(received_line).collect::<Vec<_>>()
        );
        for (g, w) in g.into_iter().zip(w) {
            assert_message(g, w, since);
        }
    }
}

/// Publish a message no recipe matches, at `QoS` 1 so it queues behind every `QoS` 1 or 2
/// delivery routed before it, and wait for its acknowledgement.
async fn send_sentinel(addr: SocketAddr) {
    let mut c = Client::connect(addr, "cookbook-end").await;
    c.publish(SENTINEL_TOPIC, b"end", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(
        c.recv().await,
        Packet::PubAck(1.into()),
        "the sentinel's PUBACK"
    );
    c.disconnect().await;
}

/// Assert the next thing `w` receives is the sentinel.
async fn expect_sentinel(w: &mut Watcher, what: &str) {
    let got = next_publish(&mut w.c).await;
    assert!(
        got.topic == SENTINEL_TOPIC && got.payload[..] == *b"end",
        "{what}: {}",
        received_line(&got)
    );
}

/// Send the sentinel and assert it is the next thing each watcher receives: every message
/// routed before it has been delivered, so anything the recipe derived that its table
/// does not list would have arrived first.
async fn assert_nothing_more(addr: SocketAddr, watchers: &mut [Watcher]) {
    send_sentinel(addr).await;
    for w in watchers {
        expect_sentinel(w, "a message the recipe does not list arrived").await;
    }
}

/// A subscriber to `#` that connects now, as the cookbook's late `mosquitto_sub` does,
/// receives exactly `retained` (by topic), each with the retain flag set, and then
/// nothing more.
async fn assert_retained(addr: SocketAddr, retained: &[Msg], since: u64) {
    let mut late = Watcher::new(addr, "late", "#", false).await;
    send_sentinel(addr).await;
    let mut got = Vec::new();
    for _ in retained {
        got.push(next_publish(&mut late.c).await);
    }
    got.sort_by(|a, b| a.topic.cmp(&b.topic));
    for (g, want) in got.iter().zip(retained) {
        assert_message(g, &late.sees(want), since);
    }
    expect_sentinel(&mut late, "a retained message the recipe does not list").await;
}

/// Run `r` on its own broker, as its [`Run`] says.
async fn run(r: &'static Recipe) {
    match r.run {
        Run::Publishes => run_recipe(r).await,
        Run::ConsoleLog => run_console(r).await,
        Run::Presence => run_presence(r).await,
    }
}

/// Run a recipe whose input is its publishes: each publish's original, then exactly what
/// its rules derived, then nothing else; then the retained messages, if it has any.
async fn run_recipe(r: &Recipe) {
    let broker = start(r).await;
    let since = now_ms();
    run_pubs(broker.addr, r.pubs, since).await;
    if r.late_filter.is_some() {
        assert_retained(broker.addr, r.retained, since).await;
    }
}

/// A `#` watcher receives each of `pubs`, unchanged, and exactly what its rules derived
/// from it, and then nothing else. An MQTT 3.1.1 watcher always, as the cookbook runs
/// one; an MQTT 5 one too, for its properties, when any message has properties.
async fn run_pubs(addr: SocketAddr, pubs: &[Pub], since: u64) {
    let has_props = pubs
        .iter()
        .any(|p| !p.props.is_empty() || p.derived.iter().any(|m| !m.props.is_empty()));
    let mut watchers = vec![Watcher::new(addr, "watcher", "#", false).await];
    if has_props {
        watchers.push(Watcher::new(addr, "watcher-v5", "#", true).await);
    }
    for p in pubs {
        mosquitto_pub(addr, p).await;
        let wants: Vec<Msg> = std::iter::once(original(p))
            .chain(p.derived.iter().copied())
            .collect();
        for w in &mut watchers {
            w.expect_batch(&wants, since).await;
        }
    }
    assert_nothing_more(addr, &mut watchers).await;
}

/// Recipe 01: its publishes, and its console line in the broker's log.
async fn run_console(r: &Recipe) {
    let broker = start(r).await;
    let since = now_ms();
    run_pubs(broker.addr, r.pubs, since).await;
    let lines = logged(&broker, "rule console action", 1).await;
    assert_log_line(CONSOLE_LINE, &lines[0], since);
}

/// Recipe 11: the cookbook's steps, then a dashboard that connects afterwards, then a
/// takeover.
async fn run_presence(r: &Recipe) {
    let broker = start(r).await;
    let addr = broker.addr;
    let since = now_ms();
    let mut watcher = Watcher::new(addr, "watcher", "#", false).await;
    let mut events = r.events.iter();
    let mut next_event = || *events.next().expect("an event the table lists");

    // `mosquitto_sub -i <id> ... &` for each of STAYS: devices that connect and stay.
    let mut stays = Vec::new();
    for id in STAYS {
        let mut c = Client::connect(addr, id).await;
        c.subscribe(1, &format!("cmd/{id}"), QoS::AtMostOnce).await;
        stays.push(c);
        watcher.expect(&next_event(), since).await;
    }

    // SENSOR_A: connect, publish, DISCONNECT.
    for p in r.pubs {
        mosquitto_pub(addr, p).await;
        let (online, offline) = (next_event(), next_event());
        watcher
            .expect_batch(&[online, original(p), offline], since)
            .await;
    }

    // KILL_FIRST: the first device's process dies, so its socket closes with no
    // DISCONNECT.
    drop(stays.remove(0));
    watcher.expect(&next_event(), since).await;
    assert!(events.next().is_none(), "an event the steps never raised");
    assert_nothing_more(addr, std::slice::from_mut(&mut watcher)).await;

    // A dashboard that connects now gets every device's status. (Its sentinel reaches
    // the watcher too, and nothing else does.)
    assert_retained(addr, r.retained, since).await;
    expect_sentinel(&mut watcher, "the dashboard's sentinel").await;

    // The takeover the recipe guards against: the device still connected connects again
    // while its first connection is open. The new connection is online; the old one ends
    // with discarded (the new one asks for a clean start), which the recipe does not
    // publish.
    let (id, mut first) = (STAYS[1], stays.remove(0));
    let takeover = now_ms();
    let _again = Client::connect(addr, id).await;
    first.expect_closed().await;
    watcher.expect(&ONLINE_C, takeover).await;
    assert_nothing_more(addr, std::slice::from_mut(&mut watcher)).await;
    let mut dashboard = Watcher::new(addr, "dashboard", &format!("presence/{id}"), false).await;
    dashboard
        .expect(
            &varies(
                "presence/sensor-c",
                1,
                true,
                r#"{"clientid":"sensor-c","status":"online","since":<ms>}"#,
            ),
            takeover,
        )
        .await;
}

/// The text of a panic's payload.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_else(|| "(a panic without a message)".to_string())
}

// ---------------------------------------------------------------------------------------
// The files and the page.
// ---------------------------------------------------------------------------------------

/// Every recipe file has a case here and every case a file, so a recipe added to
/// `docs/examples/rules/` without one, or renamed, is never silently untested. (Every
/// case runs on a broker: see [`every_recipe_does_on_the_real_broker_what_the_cookbook_shows`].)
#[test]
fn every_recipe_file_has_a_case_and_every_case_a_file() {
    let dir = repo_root().join("docs/examples/rules");
    let mut files: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "toml"))
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    files.sort();
    let cases: Vec<String> = RECIPES.iter().map(|r| r.file.to_string()).collect();
    assert_eq!(
        files, cases,
        "docs/examples/rules/*.toml and this suite's RECIPES"
    );
}

/// Each recipe file passes `mqttd --check-rules` exactly as the cookbook says a reader
/// should check one: exit 0, no warning, and the listing of its rules in the order they
/// run. A recipe that stops loading, starts warning (a `"double-quoted"` string compared
/// as a field, say) or changes its rules fails here.
#[test]
fn every_recipe_passes_check_rules_with_no_warning() {
    for r in RECIPES {
        let (code, stdout, stderr) = run_cli(&["--check-rules", &recipe_rel(r.file)]);
        assert_eq!(code, Some(0), "{}: {stdout}{stderr}", r.file);
        assert_eq!(stderr, "", "{} warns", r.file);
        assert_eq!(stdout, check_rules_stdout(r), "{}", r.file);
    }
}

/// Every `In:`, `Out:` and `Late:` example a recipe file's header shows is what the
/// recipe does, from this suite's table, which the broker test proves: an `In:` line and
/// the `Out:` lines under it are one publish and everything it derives, in order (so
/// swapping two outputs, or dropping one, fails), with the MQTT 5 properties an indented
/// `(...)` note lists for each; an `Out:` line under no `In:` line is a message derived
/// from a client event; a `Late:` line is a retained message. A malformed example line
/// (`# Out :`) fails rather than going unchecked.
#[test]
fn every_recipe_header_shows_what_the_recipe_does() {
    for r in RECIPES {
        check_header(r, &header_of(r)).unwrap_or_else(|e| panic!("{e}"));
    }
}

/// The page shows exactly the blocks this suite renders from what it runs, section by
/// section and in order, with none missing and none extra, so no command or output on
/// it can be wrong or stale: the quick start's build, check, start and log line,
/// subscriber, publish and output; each recipe's file (verbatim, byte for byte), its
/// `mosquitto_pub` commands, what the subscriber prints, and what a late subscriber
/// prints; recipe 01's console line, `--rule-test` commands and their output, and its
/// binary-payload gotcha; recipe 11's steps. No other section has a block. Every recipe
/// has its section, recipe 16's properties table is the properties the broker test
/// receives, and the quick start builds the package and binary this suite runs.
#[test]
fn the_page_shows_exactly_the_blocks_this_suite_runs() {
    let doc = cookbook();
    for r in RECIPES {
        let sec = recipe_section(&doc, r);
        let content = read(&recipe_rel(r.file));
        assert!(
            sec.contains(&format!("```toml\n{content}```\n")),
            "docs/RULES-COOKBOOK.md must quote {} verbatim in a ```toml block",
            r.file
        );
    }
    assert!(
        recipe_section(&doc, &R01).contains(&format!("```toml\n{HEX_DEBUG}```\n")),
        "docs/RULES-COOKBOOK.md must quote recipe 01's hex variant verbatim:\n{HEX_DEBUG}"
    );
    let all = sections(&doc);
    assert!(
        all.iter().any(|(h, _)| h == QUICK_START),
        "docs/RULES-COOKBOOK.md has no `## {QUICK_START}` section"
    );
    for (heading, text) in &all {
        assert_blocks(heading, text, &section_blocks(heading));
    }

    // Recipe 16's properties, which the subscriber line does not show.
    let mut table = vec![
        "| Message | User properties, in order | Content-Type | Message expiry |".to_string(),
        "|---|---|---|---|".to_string(),
    ];
    table.extend(stream(&R16).iter().map(props_row));
    let table = table.join("\n");
    assert!(
        recipe_section(&doc, &R16).contains(&format!("\n\n{table}\n\n")),
        "docs/RULES-COOKBOOK.md must show recipe 16's properties as this table, and only \
         it:\n{table}"
    );

    // `cargo build -p <PACKAGE>` builds this binary, which `PATH` then finds by that name.
    assert_eq!(env!("CARGO_PKG_NAME"), PACKAGE);
    assert_eq!(
        Path::new(env!("CARGO_BIN_EXE_mqttd"))
            .file_stem()
            .and_then(|s| s.to_str()),
        Some(PACKAGE)
    );
}

/// Recipe 01's offline half: each `mqttd --rule-test` command the cookbook shows prints
/// exactly what the cookbook says, with the exit code it implies (1 for the failure).
#[test]
fn recipe_01_rule_test_runs_a_statement_offline() {
    for (sql, topic, payload, code, shown) in RULE_TESTS {
        let (got_code, stdout, stderr) = run_cli(&[
            "--rule-test",
            "--sql",
            sql,
            "--topic",
            topic,
            "--payload",
            payload,
        ]);
        assert_eq!(got_code, Some(code), "{payload}: {stdout}{stderr}");
        assert_eq!(
            format!("{stdout}{stderr}"),
            format!("{shown}\n"),
            "{payload}"
        );
    }
}

/// The gotchas the cookbook states in prose, each run through `mqttd --rule-test`: `/`
/// gives a float even for whole numbers (recipe 03); a field with no value, like an
/// anonymous client's `username`, is the text `"undefined"` in JSON (recipe 08); `null`
/// is read as a field name, not a null (recipe 14); `split()` drops empty fields unless
/// given `'notrim'` (recipe 14); `nth()` past the end fails the rule (recipe 14); and
/// `bin2hexstr` gives upper-case hex (recipe 15). If one stops holding, the page's advice
/// is wrong.
#[test]
fn the_gotchas_the_cookbook_states_hold() {
    for (sql, payload, code, shown) in [
        (
            r#"SELECT 3000 / 1000 AS v FROM "t/#""#,
            "{}",
            0,
            r#"{"v":3.0}"#,
        ),
        (
            r#"SELECT username AS v FROM "t/#""#,
            "{}",
            0,
            r#"{"v":"undefined"}"#,
        ),
        (
            r#"SELECT null AS v FROM "t/#""#,
            "{}",
            0,
            r#"{"v":"undefined"}"#,
        ),
        (
            r#"SELECT split(payload, ',') AS v FROM "t/#""#,
            "a,,c",
            0,
            r#"{"v":["a","c"]}"#,
        ),
        (
            r#"SELECT nth(3, split(payload, ',', 'notrim')) AS v FROM "t/#""#,
            "a,b",
            1,
            "rule test FAILED: nth(): nth(3) is past the end of a 2-element array",
        ),
        (
            r#"SELECT bin2hexstr(payload) AS v FROM "t/#""#,
            "Az",
            0,
            r#"{"v":"417A"}"#,
        ),
    ] {
        let (got_code, stdout, stderr) = run_cli(&[
            "--rule-test",
            "--sql",
            sql,
            "--topic",
            "t/1",
            "--payload",
            payload,
        ]);
        assert_eq!(got_code, Some(code), "{sql}: {stdout}{stderr}");
        assert_eq!(format!("{stdout}{stderr}"), format!("{shown}\n"), "{sql}");
    }
}

/// The page's pattern matcher accepts the values a run produces and rejects a time that
/// does not match its milliseconds, so recipe 08's assertions are not vacuous.
#[test]
fn the_pattern_matcher_checks_rendered_times() {
    let p = r#"{"ms":<ms>,"at":"<utc>","local":"<plant>","id":"<id>"}"#;
    let ok = r#"{"ms":1791391562983,"at":"2026-10-07T16:46:02.983+00:00","local":"2026-10-07 18:46:02","id":"00065D42D9C3E9B22670C4A000000000"}"#;
    assert_eq!(match_pattern(p, ok), Ok(vec![1_791_391_562_983]));
    let wrong_hour = ok.replace("18:46:02", "17:46:02");
    assert!(match_pattern(p, &wrong_hour).is_err());
    let wrong_ms = ok.replace(".983+", ".984+");
    assert!(match_pattern(p, &wrong_ms).is_err());
    assert_eq!(utc(0), "1970-01-01T00:00:00.000+00:00");
    assert_eq!(utc(951_782_400_000), "2000-02-29T00:00:00.000+00:00");
}

/// The header check is not vacuous: real headers changed in the ways a header drifts are
/// rejected. Two outputs swapped, an output dropped, a property note missing a property,
/// a malformed example line, events out of order, a retained message shown live, an
/// output under an `In:` that derives nothing, and a note under a `Late:` line.
#[test]
fn the_header_check_rejects_a_header_that_does_not_match() {
    let swap = |text: &str, a: &str, b: &str| {
        assert!(text.contains(a) && text.contains(b), "{a} / {b}");
        text.replace(a, "\u{0}").replace(b, a).replace('\u{0}', b)
    };
    let r02 = header_of(&R02);
    let r02_outs: Vec<&str> = r02.lines().filter(|l| l.starts_with("# Out: ")).collect();
    let r06 = header_of(&R06);
    let r06_last_out = r06
        .lines()
        .rfind(|l| l.starts_with("# Out: "))
        .expect("an Out: line");
    let r11 = header_of(&R11);
    let r11_outs: Vec<&str> = r11.lines().filter(|l| l.starts_with("# Out:")).collect();
    let r07 = header_of(&R07);
    let cases: [(&Recipe, String); 8] = [
        (&R02, swap(&r02, r02_outs[0], r02_outs[1])),
        (&R06, r06.replace(&format!("{r06_last_out}\n"), "")),
        (&R16, header_of(&R16).replace("tag=red, ", "")),
        (&R02, r02.replacen("# Out: ", "# Out : ", 1)),
        (&R11, swap(&r11, r11_outs[0], r11_outs[1])),
        (
            &R09,
            header_of(&R09).replace(
                "# Late: sites/munich/devices/th-9/state  qos=2 retain=1",
                "# Late: sites/munich/devices/th-9/state  qos=2 retain=0",
            ),
        ),
        (
            &R07,
            r07.replace(
                "# (nothing is published for it)",
                "# Out: devices/door/x  qos=1 retain=0  {}",
            ),
        ),
        (
            &R12,
            header_of(&R12).replace(
                "# (and nothing for d3",
                "#       (user properties a=b)\n# (and nothing for d3",
            ),
        ),
    ];
    for (r, changed) in cases {
        assert_ne!(
            changed,
            header_of(r),
            "{}: the change changed nothing",
            r.file
        );
        assert!(
            check_header(r, &changed).is_err(),
            "{}: the header check accepted\n{changed}",
            r.file
        );
    }
}

// ---------------------------------------------------------------------------------------
// The recipes, end to end in the real binary.
// ---------------------------------------------------------------------------------------

/// Every recipe in the table, each in its own real broker and all at once, does exactly
/// what the table says and the page and its file's header show: for each publish, the
/// original arrives unchanged with exactly the derived messages listed (topic, payload,
/// `QoS`, retain flag, and the properties for an MQTT 5 subscriber), in routing order
/// within `QoS` 0 and within `QoS` 1 and 2, then nothing else; and a late subscriber gets
/// exactly the retained messages listed. Recipe 01 also logs its console line, and recipe
/// 11 runs its devices' steps and a takeover. The loop is over the table, so no recipe
/// can be on the page without running on a broker. A failure names each recipe that
/// failed and what its run proves (its `proves`).
#[tokio::test]
async fn every_recipe_does_on_the_real_broker_what_the_cookbook_shows() {
    let runs: Vec<_> = RECIPES.iter().map(|&r| (r, tokio::spawn(run(r)))).collect();
    let mut failed = Vec::new();
    for (r, handle) in runs {
        if let Err(e) = handle.await {
            let why = e
                .try_into_panic()
                .map_or_else(|e| e.to_string(), |p| panic_text(&*p));
            failed.push(format!("{}: {}\n{why}", r.file, r.proves));
        }
    }
    assert!(
        failed.is_empty(),
        "{} recipe(s) do not do what the cookbook shows:\n\n{}",
        failed.len(),
        failed.join("\n\n")
    );
}

/// Recipe 01's gotcha, as the cookbook shows it: the console action logs JSON, which
/// cannot hold raw bytes, so for a payload that is not UTF-8 text the broker logs the
/// WARN the page shows instead of a console line, and still delivers the message; binary
/// MQTT 5 Correlation-Data does the same, as the page says; and the hex rule the page
/// gives in the recipe's place logs text and bytes alike, with the console line the page
/// shows for the bytes. If the console action learned to log bytes, or the hex rule
/// stopped working, the page's advice would be wrong.
#[tokio::test]
async fn recipe_01_binary_payloads_warn_and_the_hex_variant_logs_them() {
    for p in [&BINARY_PUB, &CORRELATION_PUB] {
        // A broker each: the WARN is logged once per rule in 10 s.
        let broker = start(&R01).await;
        let since = now_ms();
        run_pubs(broker.addr, std::slice::from_ref(p), since).await;
        let warned = logged(&broker, "WARN mqttd::rules:", 1).await;
        assert_log_line(BINARY_WARN, &warned[0], since);
        assert!(
            !broker.log.text().contains("rule console action"),
            "{}: a console line as well as the WARN:\n{}",
            p.client,
            broker.log.tail(5)
        );
    }

    let file = tempfile::NamedTempFile::new().expect("a temp rules file");
    std::fs::write(file.path(), HEX_DEBUG).expect("write the rules");
    let broker = start_file(file.path(), 1).await;
    let since = now_ms();
    run_pubs(broker.addr, &[R01.pubs[0], BINARY_PUB], since).await;
    let lines = logged(&broker, "rule console action", 2).await;
    assert_log_line(HEX_TEXT_LINE, &lines[0], since);
    assert_log_line(HEX_CONSOLE_LINE, &lines[1], since);
}

/// Four recipes guard against a trap, and the cookbook says what the trap does. These are
/// those recipes' rules WITHOUT the guard, so a reader can trust the warning.
const UNGUARDED: &str = r#"# Recipe 07 without its WHERE
[rules.a_route]
sql = 'SELECT payload.type AS kind, payload.device AS device, payload FROM "ingest"'
actions = [{ function = "republish", args = { topic = "devices/${kind}/${device}", payload = "${payload}" } }]

# Recipe 12 with payload = "" to "publish an empty payload"
[rules.b_clear]
sql = 'SELECT topic(2) AS device FROM "devices/+/decommission"'
actions = [{ function = "republish", args = { topic = "state/${device}", payload = "" } }]

# Recipe 15 without payload in its SELECT
[rules.c_archive]
sql = 'SELECT topic(2) AS device FROM "bin/+/up"'
actions = [{ function = "republish", args = { topic = "archive/${device}", payload = "${payload}" } }]

# Recipe 16 without coalesce()
[rules.d_audit]
sql = 'SELECT payload, pub_props FROM "orders/+"'
actions = [{ function = "republish", args = { topic = "audit/orders", payload = "${payload}", mqtt_properties = { "Content-Type" = "${pub_props.'Content-Type'}" } } }]
"#;

const UNGUARDED_DOOR: &str = r#"{"type": "door", "device": "x/../admin/cmd", "open": true}"#;
const UNGUARDED_NO_ID: &str = r#"{"type": "door", "open": true}"#;

/// What [`UNGUARDED`] is given, and what it derives.
const UNGUARDED_PUBS: [Pub; 5] = [
    publish(
        "cell-gw",
        "ingest",
        0,
        UNGUARDED_DOOR,
        &[out("devices/door/x/../admin/cmd", 0, UNGUARDED_DOOR)],
    ),
    publish(
        "cell-gw",
        "ingest",
        0,
        UNGUARDED_NO_ID,
        &[out("devices/door/undefined", 0, UNGUARDED_NO_ID)],
    ),
    publish(
        "ops",
        "devices/d3/decommission",
        0,
        "",
        &[out("state/d3", 0, r#"{"device":"d3"}"#)],
    ),
    Pub {
        client: "nb-1",
        v5: false,
        topic: "bin/nb-1/up",
        qos: 0,
        retain: false,
        body: Body::Bytes(b"\x01\x09\xc4"),
        props: &[],
        derived: &[out("archive/nb-1", 0, "undefined")],
    },
    publish(
        "legacy-311",
        "orders/new",
        0,
        r#"{"order": 1002}"#,
        &[Msg {
            topic: "audit/orders",
            qos: 0,
            retain: false,
            body: Body::Text(r#"{"order": 1002}"#),
            props: &[Prop::ContentType("undefined")],
        }],
    ),
];

/// Without their guards, the recipes do what the cookbook warns: recipe 07's device id
/// steers the topic (`devices/door/x/../admin/cmd`, and `devices/door/undefined` for a
/// missing id); recipe 12's `payload = ""` publishes the output as JSON, not an empty
/// payload, so it would never delete the retained state; recipe 15's `${payload}` is the
/// text `undefined` when the SELECT leaves `payload` out; and recipe 16's Content-Type
/// placeholder sends the text `undefined` for a publisher that sent none. If one of these
/// stopped happening, the guard (and the cookbook's warning) would be stale.
#[tokio::test]
async fn without_their_guards_the_recipes_do_what_the_cookbook_warns() {
    let file = tempfile::NamedTempFile::new().expect("a temp rules file");
    std::fs::write(file.path(), UNGUARDED).expect("write the rules");
    let broker = start_file(file.path(), 4).await;
    let since = now_ms();
    run_pubs(broker.addr, &UNGUARDED_PUBS, since).await;
}
