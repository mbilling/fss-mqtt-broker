//! The rule-engine demo (`demo/rules/`), kept true.
//!
//! The demo plays simulated power-plant, home and car telemetry into mqttd and prints what
//! its rules derive. This suite takes the demo's seeded fixture (the ten simulated minutes
//! `simulate.py --dry-run` writes for the README's seed and start time), replays it through
//! the REAL `mqttd` binary loading `demo/rules/rules.toml`, one connection per simulated
//! client (a device or its gateway) as the simulator's player makes them, and asserts that
//! the derived messages a `QoS` 1 subscriber receives are exactly `rules_demo.expected`
//! beside this file: topic, `QoS`, retain flag and payload, with nothing missing and
//! nothing extra. It also checks that the simulator is deterministic, that the rules file
//! passes `--check-rules` with no warning, and that every derived message the README quotes
//! is one the fixture produces.
//!
//! The live simulator (`live.py`, which the demo stack in `demo/rules-live/` runs) plays the
//! same ten minutes endlessly, in windows aligned to the wall clock. Its tests here check
//! where its schedule places a time, that its scheduler keeps to the schedule (joining
//! mid-window, dropping what is late, rejoining after a long stall), that in fixture mode
//! every window is the fixture again, byte for byte, that in now mode device clocks run on
//! from one window into the next, that its player and MQTT client bound every wait and keep
//! devices offline where the script has them offline, and, against the real binary, that it
//! rides out a broker restart and disconnects every device cleanly on SIGTERM.
//!
//! The demo stack's rule editor (`demo/rules-live/ui/`) is checked without the stack: its
//! page renders text, never markup, and its server refuses what another web site could make
//! a browser send it, and sends a strict Content-Security-Policy.
//!
//! The fixture is generated, not stored: it is about a megabyte, and everything under
//! `demo/` is also copied into the `mqttui` bundle. After changing the simulator or the
//! rules on purpose, regenerate the expected output and review its diff like any other
//! change:
//!
//! ```text
//! MQTTD_DEMO_BLESS=1 cargo test -p mqttd --test rules_demo the_demo_derives
//! ```

mod common;
mod listen_wait;
mod proc_common;

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Client, Recv, V4};
use mqtt_codec::packet::{Connect, LastWill, Publish};
use mqtt_codec::{Packet, Properties, QoS};

/// The simulator arguments that write the fixture (README.md, "Reproduce it").
const FIXTURE_ARGS: [&str; 7] = [
    "--dry-run",
    "--seed",
    "7",
    "--start",
    "2026-03-24T15:55:00Z",
    "--duration",
    "600",
];
const SIMULATOR: &str = "demo/rules/simulate.py";
/// The live simulator, which plays the fixture's ten minutes endlessly.
const LIVE: &str = "demo/rules/live.py";
/// The simulation's domains; the demo stack runs one live simulator for each.
const DOMAINS: [&str; 3] = ["power", "homes", "cars"];
/// The length of one live window, in seconds: the fixture's duration.
const WINDOW_S: i64 = 600;
const EXPECTED: &str = "crates/mqttd/tests/rules_demo.expected";
const RULES: &str = "demo/rules/rules.toml";
const README: &str = "demo/rules/README.md";
/// Where the demo's rules publish (`simulate.py`'s `DERIVED`).
const DERIVED: [&str; 6] = [
    "alerts/#",
    "kpi/#",
    "normalized/#",
    "analytics/#",
    "state/#",
    "events/#",
];
/// Published after the replay; once it arrives, everything routed before it has.
const SENTINEL: &str = "demo-test/end";
/// The simulator's player connects every device with this keepalive.
const KEEPALIVE: u16 = 60;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(repo_root().join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// The fixture: what the simulator prints for [`FIXTURE_ARGS`].
fn fixture() -> String {
    let mut args = vec![SIMULATOR];
    args.extend(FIXTURE_ARGS);
    python_out(&args)
}

fn blessing() -> bool {
    std::env::var_os("MQTTD_DEMO_BLESS").is_some()
}

/// `python3` run from the repository root, with none of the live simulator's `SIM_*`
/// settings from the caller's environment and no bytecode written into the checkout.
fn python3() -> Command {
    let mut cmd = Command::new("python3");
    for (k, _) in std::env::vars() {
        if k.starts_with("SIM_") {
            cmd.env_remove(k);
        }
    }
    cmd.current_dir(repo_root())
        .env("PYTHONDONTWRITEBYTECODE", "1");
    cmd
}

/// What `python3 <args>` prints; it must succeed.
fn python_out(args: &[&str]) -> String {
    python_out_env(args, &[])
}

/// What `python3 <args>` prints with `env` set; it must succeed.
fn python_out_env(args: &[&str], env: &[(&str, &str)]) -> String {
    let out = python3()
        .envs(env.iter().copied())
        .args(args)
        .output()
        .expect("python3 runs");
    assert!(
        out.status.success(),
        "python3 {} failed:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("UTF-8")
}

struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn mqttd() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            cmd.env_remove(k);
        }
    }
    // `unix_ts_to_rfc3339` writes the host's local time zone; the expected output is UTC.
    cmd.env("TZ", "UTC");
    cmd
}

// ---------------------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------------------

/// One line of the fixture: what one simulated device does next.
#[derive(Debug)]
enum Event {
    Connect {
        client: String,
        will: Option<(String, Vec<u8>, u8, bool)>,
    },
    Publish {
        client: String,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
    },
    Disconnect {
        client: String,
    },
    Drop {
        client: String,
    },
}

fn base64_decode(text: &str) -> Vec<u8> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in text.bytes().filter(|&c| c != b'=') {
        let v = u32::try_from(ALPHABET.iter().position(|&a| a == c).expect("base64 digit"))
            .expect("fits");
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(u8::try_from((acc >> bits) & 0xff).expect("a byte"));
        }
    }
    out
}

fn field<'a>(line: &'a serde_json::Value, key: &str) -> &'a serde_json::Value {
    line.get(key)
        .unwrap_or_else(|| panic!("fixture line without {key}: {line}"))
}

fn text(v: &serde_json::Value) -> String {
    v.as_str().expect("a string").to_string()
}

fn small(v: &serde_json::Value) -> u8 {
    u8::try_from(v.as_u64().expect("a number")).expect("a QoS")
}

fn parse_fixture(fixture: &str) -> Vec<Event> {
    fixture
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let line: serde_json::Value = serde_json::from_str(l).expect("a JSON fixture line");
            let client = text(field(&line, "client"));
            match field(&line, "kind").as_str().expect("a kind") {
                "connect" => Event::Connect {
                    client,
                    will: line.get("will").map(|w| {
                        (
                            text(field(w, "topic")),
                            text(field(w, "payload")).into_bytes(),
                            small(field(w, "qos")),
                            field(w, "retain").as_bool().expect("a bool"),
                        )
                    }),
                },
                "publish" => Event::Publish {
                    client,
                    topic: text(field(&line, "topic")),
                    payload: match (line.get("payload"), line.get("payload_b64")) {
                        (Some(p), _) => text(p).into_bytes(),
                        (None, Some(b)) => base64_decode(&text(b)),
                        (None, None) => Vec::new(),
                    },
                    qos: small(field(&line, "qos")),
                    retain: field(&line, "retain").as_bool().expect("a bool"),
                },
                "disconnect" => Event::Disconnect { client },
                "drop" => Event::Drop { client },
                other => panic!("unknown fixture event kind {other:?}"),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------------------
// The broker and the replay
// ---------------------------------------------------------------------------------------

struct Broker {
    addr: SocketAddr,
    log: listen_wait::Log,
    _child: ChildGuard,
}

async fn start_broker() -> Broker {
    start_broker_with(&repo_root().join(RULES), None).await
}

/// mqttd loading `rules`, on `addr` (a restart where the broker was) or on a fresh port.
async fn start_broker_with(rules: &Path, addr: Option<SocketAddr>) -> Broker {
    let (child, log, addr) = listen_wait::spawn_listening_logged(|| {
        let addr: SocketAddr = addr.unwrap_or_else(|| {
            format!("127.0.0.1:{}", proc_common::free_tcp_port())
                .parse()
                .unwrap()
        });
        let mut cmd = mqttd();
        cmd.env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_DURABLE_SESSIONS", "0")
            .env("MQTTD_RULES_FILE", rules)
            .env("RUST_LOG", "mqttd=info")
            .env("NO_COLOR", "1")
            .stderr(Stdio::null());
        (cmd, vec![addr], addr)
    })
    .await;
    assert!(
        log.text().contains("rules loaded"),
        "the broker did not load {}:\n{}",
        rules.display(),
        log.tail(30)
    );
    Broker {
        addr,
        log,
        _child: ChildGuard(child),
    }
}

fn qos(n: u8) -> QoS {
    match n {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

/// A device's connection, as the simulator's player opens it: v3.1.1, clean session,
/// keepalive 60, and the Will the fixture gives it, if any.
async fn connect(
    addr: SocketAddr,
    client: &str,
    will: Option<&(String, Vec<u8>, u8, bool)>,
) -> Client {
    let mut c = Client::open(addr, V4).await;
    c.send(&Packet::Connect(Connect {
        properties: Properties::new(),
        protocol: V4,
        clean_session: true,
        keep_alive: KEEPALIVE,
        client_id: client.to_string(),
        last_will: will.map(|(topic, payload, q, retain)| LastWill {
            topic: topic.clone(),
            payload: bytes::Bytes::copy_from_slice(payload),
            qos: qos(*q),
            retain: *retain,
            properties: Properties::new(),
        }),
        username: None,
        password: None,
    }))
    .await;
    match c.recv().await {
        Packet::ConnAck(a) => assert_eq!(a.code, 0, "{client}: CONNECT refused"),
        other => panic!("{client}: expected CONNACK, got {other:?}"),
    }
    c
}

struct Device {
    c: Client,
    next_pkid: u16,
}

/// Play the fixture in order, as fast as the broker answers, and then disconnect every
/// device still connected, as the player does at the end of a run.
async fn replay(addr: SocketAddr, events: &[Event]) {
    let mut devices: HashMap<String, Device> = HashMap::new();
    for ev in events {
        match ev {
            Event::Connect { client, will } => {
                if let Some(mut old) = devices.remove(client) {
                    old.c.disconnect().await;
                }
                let c = connect(addr, client, will.as_ref()).await;
                devices.insert(client.clone(), Device { c, next_pkid: 1 });
            }
            Event::Publish {
                client,
                topic,
                payload,
                qos: q,
                retain,
            } => {
                if !devices.contains_key(client) {
                    let c = connect(addr, client, None).await;
                    devices.insert(client.clone(), Device { c, next_pkid: 1 });
                }
                let d = devices.get_mut(client).expect("connected above");
                let pkid = (*q > 0).then(|| {
                    let id = d.next_pkid;
                    d.next_pkid = id % 65_535 + 1;
                    id
                });
                d.c.publish_full(topic, payload, qos(*q), *retain, pkid)
                    .await;
                if let Some(id) = pkid {
                    match d.c.recv().await {
                        Packet::PubAck(a) => assert_eq!(a.pkid, id, "{client}: PUBACK id"),
                        other => panic!("{client}: expected PUBACK for {topic}, got {other:?}"),
                    }
                }
            }
            Event::Disconnect { client } => {
                if let Some(mut d) = devices.remove(client) {
                    d.c.disconnect().await;
                }
            }
            Event::Drop { client } => {
                // Closing the socket without a DISCONNECT, as a device losing power does.
                drop(devices.remove(client));
            }
        }
    }
    let mut left: Vec<_> = devices.into_iter().collect();
    left.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, mut d) in left {
        d.c.disconnect().await;
    }
}

/// One derived message as `rules_demo.expected` writes it: topic, `QoS`, retain flag,
/// payload (text as is; bytes that are not UTF-8 as `0x` and lower-case hex).
fn line(p: &Publish) -> String {
    let body = match std::str::from_utf8(&p.payload) {
        Ok(t) => t.to_string(),
        Err(_) => p.payload.iter().fold(String::from("0x"), |mut hex, b| {
            let _ = write!(hex, "{b:02x}");
            hex
        }),
    };
    let q = match p.qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    };
    format!("{} q{q} r{} {body}", p.topic, u8::from(p.retain))
}

/// Everything the watcher receives, until the sentinel has arrived and `want` messages
/// have (when blessing, until the sentinel and a quiet second). A device whose connection
/// was dropped is noticed by the broker on its own schedule, so what its disconnect derives
/// may arrive after the sentinel.
async fn collect(broker: &Broker, watcher: &mut Client, want: Option<usize>) -> Vec<String> {
    let mut sender = Client::connect(broker.addr, "demo-test-end").await;
    sender
        .publish(SENTINEL, b"end", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(
        sender.recv().await,
        Packet::PubAck(1.into()),
        "the sentinel's PUBACK"
    );
    sender.disconnect().await;

    let mut got = Vec::new();
    let mut sentinel = false;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let done = match want {
            Some(n) => sentinel && got.len() >= n,
            None => false,
        };
        if done {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "derived messages stopped at {} (want {want:?}); sentinel seen: {sentinel}\n{}",
            got.len(),
            broker.log.tail(20)
        );
        match watcher.recv_bounded(Duration::from_secs(1)).await {
            Recv::Packet(Packet::Publish(p)) => {
                if let Some(id) = p.pkid {
                    watcher.puback(id).await;
                }
                if p.topic == SENTINEL {
                    sentinel = true;
                } else {
                    got.push(line(&p));
                }
            }
            Recv::Quiet if want.is_none() && sentinel => break,
            Recv::Packet(_) | Recv::Quiet => {}
            Recv::Closed => panic!("the watcher's connection closed\n{}", broker.log.tail(20)),
        }
    }
    // Nothing more: a second sentinel arrives next, with nothing before it.
    let mut sender = Client::connect(broker.addr, "demo-test-end").await;
    sender
        .publish(SENTINEL, b"end", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(
        sender.recv().await,
        Packet::PubAck(1.into()),
        "the sentinel's PUBACK"
    );
    sender.disconnect().await;
    loop {
        match watcher.recv().await {
            Packet::Publish(p) if p.topic == SENTINEL => break,
            Packet::Publish(p) => panic!("an unexpected derived message: {}", line(&p)),
            _ => {}
        }
    }
    got.sort();
    got
}

// ---------------------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------------------

/// The fixture replayed through the real broker derives exactly `rules_demo.expected`, and
/// no rule fails along the way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_demo_derives_exactly_what_it_shows() {
    let events = parse_fixture(&fixture());
    let broker = start_broker().await;
    let mut watcher = Client::connect(broker.addr, "demo-test-watch").await;
    for (i, filter) in DERIVED.iter().chain([&SENTINEL]).enumerate() {
        let pkid = u16::try_from(i + 1).expect("a packet id");
        watcher.subscribe(pkid, filter, QoS::AtLeastOnce).await;
    }

    replay(broker.addr, &events).await;

    let expected: Vec<String> = if blessing() {
        Vec::new()
    } else {
        let mut lines: Vec<String> = read(EXPECTED).lines().map(str::to_string).collect();
        lines.sort();
        lines
    };
    let got = collect(
        &broker,
        &mut watcher,
        (!blessing()).then_some(expected.len()),
    )
    .await;

    let log = broker.log.text();
    let failures: Vec<&str> = log
        .lines()
        .filter(|l| l.contains("rule failed") || l.contains("rule action failed"))
        .collect();
    assert!(
        failures.is_empty(),
        "rules failed during the demo:\n{}",
        failures.join("\n")
    );

    assert!(
        got.len() > 1_000,
        "the demo's ten minutes derived only {} messages",
        got.len()
    );
    // Blessing writes what the broker derived as the new expected output, then checks it
    // like any other run.
    let expected: Vec<String> = if blessing() {
        let mut text = got.join("\n");
        text.push('\n');
        std::fs::write(repo_root().join(EXPECTED), text).expect("write the expected output");
        read(EXPECTED).lines().map(str::to_string).collect()
    } else {
        expected
    };
    if got != expected {
        let missing: Vec<_> = expected.iter().filter(|l| !got.contains(l)).collect();
        let extra: Vec<_> = got.iter().filter(|l| !expected.contains(l)).collect();
        panic!(
            "the demo derived something other than {EXPECTED} ({} vs {} lines)\n\
             missing ({}):\n  {}\nextra ({}):\n  {}\n\
             If the change is intended: MQTTD_DEMO_BLESS=1 cargo test -p mqttd --test rules_demo",
            got.len(),
            expected.len(),
            missing.len(),
            missing
                .iter()
                .take(20)
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("\n  "),
            extra.len(),
            extra
                .iter()
                .take(20)
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("\n  "),
        );
    }
}

/// The simulator is seeded: the same arguments print the same fixture, byte for byte, so
/// the README's numbers and the expected output hold on every machine.
#[test]
fn the_simulator_is_deterministic() {
    let (a, b) = (fixture(), fixture());
    assert!(
        a.lines().count() > 1_000,
        "the fixture is the demo's ten minutes"
    );
    assert!(
        a == b,
        "two runs of simulate.py {} differ",
        FIXTURE_ARGS.join(" ")
    );
}

/// `mqttd --check-rules` loads the demo's file with no warning.
#[test]
fn the_demo_rules_file_checks_clean() {
    let out = mqttd()
        .arg("--check-rules")
        .arg(repo_root().join(RULES))
        .output()
        .expect("run mqttd --check-rules");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "--check-rules failed:\n{}{stderr}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        !stderr.contains("WARN") && !stderr.to_lowercase().contains("warning"),
        "--check-rules warned:\n{stderr}"
    );
}

/// Every derived message the README quotes (a line `⇒ <topic>  <payload>` in a fenced
/// block) is one the fixture produces, so the walkthrough cannot drift from the rules.
#[test]
fn the_readme_quotes_only_messages_the_fixture_derives() {
    let expected = read(EXPECTED);
    let derived: Vec<(&str, &str)> = expected
        .lines()
        .filter_map(|l| {
            let (topic, rest) = l.split_once(' ')?;
            let (_, rest) = rest.split_once(' ')?;
            let (_, payload) = rest.split_once(' ')?;
            Some((topic, payload))
        })
        .collect();
    let readme = read(README);
    let quoted: Vec<&str> = readme
        .lines()
        .filter_map(|l| l.trim_start().strip_prefix("⇒ "))
        .collect();
    assert!(
        quoted.len() >= 10,
        "the README should quote the derived messages it explains (found {})",
        quoted.len()
    );
    for q in quoted {
        let (topic, payload) = q
            .split_once("  ")
            .unwrap_or_else(|| panic!("a quoted line is `⇒ <topic>  <payload>`: {q}"));
        assert!(
            derived.contains(&(topic, payload)),
            "the README quotes a message the fixture does not derive:\n  {q}"
        );
    }
}

// ---------------------------------------------------------------------------------------
// The live simulator
// ---------------------------------------------------------------------------------------

/// The first line where `got` and `want` differ, for a failure message.
fn first_difference(got: &str, want: &str) -> String {
    let (g, w): (Vec<&str>, Vec<&str>) = (got.lines().collect(), want.lines().collect());
    match g.iter().zip(&w).position(|(a, b)| a != b) {
        Some(i) => format!("line {}:\n  got  {}\n  want {}", i + 1, g[i], w[i]),
        None => format!("{} lines, want {}", g.len(), w.len()),
    }
}

/// `anchor()` floors a wall-clock time to the 10-minute grid every live simulator shares,
/// and `plan(now, s0)` says which window of the schedule anchored at `s0` holds `now` (window
/// k starts at `s0 + 600 k`) and how many seconds into it `now` is. A clock set back past the
/// anchor lands in window -1, not in a negative offset.
#[test]
fn the_live_schedule_places_a_time_in_its_window() {
    // 2026-10-08T09:20:00Z, on the grid.
    const S0: f64 = 1_791_451_200.0;
    // (now - S0, anchor(now) - S0)
    let anchors = [
        (0.0, 0.0),
        (0.5, 0.0),
        (599.5, 0.0),
        (600.0, 600.0),
        (1_234.5, 1_200.0),
    ];
    // (now - S0, k, offset)
    let plans = [
        (0.0, 0, 0.0),
        (0.5, 0, 0.5),
        (599.5, 0, 599.5),
        (600.0, 1, 0.0),
        (3_725.0, 6, 125.0),
        (-5.0, -1, 595.0),
    ];
    let script = r#"
import json, sys
sys.path.insert(0, "demo/rules")
import live
s0, anchors, plans = json.loads(sys.argv[1])
print(json.dumps([[live.anchor(s0 + d) - s0 for d in anchors],
                  [live.plan(s0 + d, s0) for d in plans]]))
"#;
    let input = serde_json::json!([S0, anchors.map(|(d, _)| d), plans.map(|(d, _, _)| d)]);
    let out: serde_json::Value =
        serde_json::from_str(&python_out(&["-c", script, &input.to_string()])).expect("JSON");
    let close =
        |v: &serde_json::Value, want: f64| (v.as_f64().expect("a number") - want).abs() < 1e-6;
    for (i, (d, want)) in anchors.into_iter().enumerate() {
        let got = &out[0][i];
        assert!(
            close(got, want),
            "anchor(S0 + {d}) is S0 + {got}, want S0 + {want}"
        );
    }
    for (i, (d, k, offset)) in plans.into_iter().enumerate() {
        let got = &out[1][i];
        assert!(
            got[0].as_i64() == Some(k) && close(&got[1], offset),
            "plan(S0 + {d}, S0) is {got}, want [{k}, {offset}]"
        );
    }
}

/// Drives `live.Live` with a fake clock and a fake player, for
/// [`the_live_scheduler_joins_drops_late_events_and_rejoins`]: one device publishing at 0,
/// 100, ..., 500 s into every window of a schedule anchored at 0; the clock starts at
/// `start`, advances as the player idles, and jumps by `jumps[n]` seconds while the n-th
/// event is sent (a send that blocks, a machine that sleeps); the loop stops at `end`.
/// Prints the player's log, the late and re-plan counts and what the loop said, as JSON.
const SCHEDULER_HARNESS: &str = r#"
import json, sys
sys.path.insert(0, "demo/rules")
import live
from sim.core import Event

start, jumps, end = json.loads(sys.argv[1])
jumps = {int(n): s for n, s in jumps.items()}
now = [float(start)]
window = [Event(at=float(at), client="d", topic="t") for at in range(0, 600, 100)]


class Schedule(live.Schedule):
    def events(self, k):
        return window

    def ready(self, k):
        return True


class Player:
    host, port, sent, unsent, reconnects, seen = "fake", 0, 0, 0, 0, set()

    def __init__(self):
        self.log = []

    def tend(self):
        pass

    def idle(self, seconds):
        now[0] += seconds

    def send(self, ev):
        self.log.append(["send", now[0], ev.at])
        self.sent += 1
        now[0] += jumps.get(self.sent, 0.0)

    def skip(self, ev):
        self.log.append(["skip", now[0], ev.at])

    def begin(self, events):
        self.log.append(["begin", now[0], events[0].at if events else None])

    def connected(self):
        return 0

    def close(self):
        self.log.append(["close", now[0], None])


player, said = Player(), []
run = live.Live(Schedule(["power"], 7, "now", 0.0), player, 1000.0, lambda: now[0] >= end,
                said.append)
run.run(lambda: now[0])
print(json.dumps({"log": player.log, "late": run.late, "replans": run.replans, "said": said}))
"#;

/// The live scheduler keeps to the wall-clock schedule (driven here by a fake clock, so no
/// real time passes). Started 150 s into a window it skips what came before and sends the
/// rest on time. An event 5 s late is still sent; one 35 s late is dropped and counted, not
/// sent. At the window's end the next one begins. And more than a window behind (the clock
/// jumped 2,000 s, as when a laptop sleeps) it does not send what it missed: it rejoins the
/// schedule where it is now, window 4 at 300 s, as it would have started there.
#[test]
fn the_live_scheduler_joins_drops_late_events_and_rejoins() {
    let input = serde_json::json!([150, {"1": 105, "2": 130, "5": 2000}, 2850]);
    let out: serde_json::Value =
        serde_json::from_str(&python_out(&["-c", SCHEDULER_HARNESS, &input.to_string()]))
            .expect("JSON");
    // [what, the clock then, the event's offset into its window]
    let want = serde_json::json!([
        // Joins window 0 at 150 s.
        ["skip", 150.0, 0.0],
        ["skip", 150.0, 100.0],
        ["begin", 150.0, 200.0],
        ["send", 200.0, 200.0],
        // That send took 105 s: the event at 300 s is 5 s late, and still sent.
        ["send", 305.0, 300.0],
        // That one took 130 s: the event at 400 s is 35 s late, and dropped.
        ["skip", 435.0, 400.0],
        ["send", 500.0, 500.0],
        // Window 1.
        ["begin", 500.0, 0.0],
        ["send", 600.0, 0.0],
        ["send", 700.0, 100.0],
        // 2,000 s lost: window 4 is under way, 300 s in.
        ["skip", 2700.0, 0.0],
        ["skip", 2700.0, 100.0],
        ["skip", 2700.0, 200.0],
        ["begin", 2700.0, 300.0],
        ["send", 2700.0, 300.0],
        ["send", 2800.0, 400.0],
        ["close", 2850.0, null],
    ]);
    assert_eq!(
        out["log"], want,
        "the scheduler's moves (left: what it did)"
    );
    assert_eq!(out["late"], 1, "one event was dropped for being late");
    assert_eq!(out["replans"], 1, "one jump of more than a window");
    let said: Vec<&str> = out["said"]
        .as_array()
        .expect("lines")
        .iter()
        .map(|l| l.as_str().expect("a line"))
        .collect();
    assert_eq!(
        said.len(),
        4,
        "a start line, one heartbeat, the jump, the stop: {said:#?}"
    );
    assert!(
        said[0].contains(
            "window 0 began 1970-01-01T00:00:00Z, joining it 150 s in; \
             next window at 1970-01-01T00:10:00Z"
        ),
        "the start line: {}",
        said[0]
    );
    assert_eq!(
        said[1], "window 1 +2100 s: sent 5, late 1, unsent 0, reconnects 0, connected 0 of 0",
        "the heartbeat, due at 1,150 s and printed when the clock came back"
    );
    assert_eq!(
        said[2],
        "1900 s behind in window 1: rejoining window 4 at 300 s"
    );
    assert_eq!(
        said[3],
        "stopped: window 4 +450 s: sent 7, late 1, unsent 0, reconnects 0, connected 0 of 0"
    );
}

/// In fixture mode every window of the live simulator is the README's ten minutes again:
/// `live.py --dry-run --windows 2 --clock fixture` prints exactly what `simulate.py
/// --dry-run` prints for the fixture, twice, device timestamps included. Checked for each
/// domain on its own, as the demo stack runs them (each domain has its own seeded
/// generator), set through the environment as the stack sets it; and for all three
/// together, as `live.py` runs by default, set by flags, which win over the environment.
#[test]
fn the_live_simulator_replays_the_fixture_in_every_window() {
    let flags = [
        "--clock",
        "fixture",
        "--seed",
        "7",
        "--domains",
        "power,homes,cars",
    ];
    let overruled = [
        ("SIM_CLOCK", "now"),
        ("SIM_SEED", "8"),
        ("SIM_DOMAINS", "cars"),
    ];
    for domains in DOMAINS.into_iter().chain(["power,homes,cars"]) {
        let mut args = vec![SIMULATOR];
        args.extend(FIXTURE_ARGS);
        args.extend(["--domains", domains]);
        let once = python_out(&args);
        assert!(
            once.lines().count() > 700,
            "the fixture's {domains} is ten minutes of messages"
        );
        let dry_run = [LIVE, "--dry-run", "--windows", "2"];
        let live = if domains.contains(',') {
            python_out_env(&[&dry_run[..], &flags].concat(), &overruled)
        } else {
            let env = [
                ("SIM_CLOCK", "fixture"),
                ("SIM_SEED", "7"),
                ("SIM_DOMAINS", domains),
            ];
            python_out_env(&dry_run, &env)
        };
        let twice = once.repeat(2);
        assert!(
            live == twice,
            "live.py --clock fixture --domains {domains}: two windows are not the fixture twice; \
             {}",
            first_difference(&live, &twice)
        );
    }
}

/// Each device's own timestamps in a live dry run, in Unix milliseconds, from the payloads
/// that carry one this can read: a JSON object's `ts`, the RTU's CSV line (Unix seconds
/// first) and the OBD dongle's binary header (4-byte Unix seconds). The P1 telegrams (local
/// time) and the OCPP chargers (ISO 8601) are not read.
fn device_clocks(dry_run: &str) -> BTreeMap<String, Vec<i64>> {
    let mut clocks: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for ev in parse_fixture(dry_run) {
        let Event::Publish {
            client,
            topic,
            payload,
            ..
        } = ev
        else {
            continue;
        };
        let ms = if let Ok(serde_json::Value::Object(o)) = serde_json::from_slice(&payload) {
            o.get("ts").and_then(serde_json::Value::as_i64)
        } else if topic.ends_with("/obd") || topic.ends_with("/dtc") {
            payload
                .first_chunk::<4>()
                .map(|h| i64::from(u32::from_be_bytes(*h)) * 1000)
        } else {
            std::str::from_utf8(&payload)
                .ok()
                .and_then(|t| t.split_once(','))
                .and_then(|(first, _)| first.parse::<i64>().ok())
                .map(|s| s * 1000)
        };
        if let Some(ms) = ms {
            clocks.entry(client).or_default().push(ms);
        }
    }
    clocks
}

/// In now mode a window's device clocks are its own ten minutes of the wall clock: every
/// timestamp a device sends in window k lies in [S0 + 600 k, S0 + 600 (k + 1)), where S0 is
/// the start time floored to ten minutes. So no device's clock goes back from one window to
/// the next, although each window replays the same script. Window 0 is also the same
/// whether or not a second window follows. Anchored at the README's hour and at night,
/// when the sun is down and some faults cannot happen.
#[test]
fn the_live_simulators_device_clocks_run_on_from_window_to_window() {
    // (the time the dry run pretends it is, S0)
    for (now, s0) in [
        ("2026-10-08T14:55:00Z", 1_791_471_000_i64),
        ("2026-10-08T00:23:00Z", 1_791_418_800),
    ] {
        let windows = |n: &str| {
            python_out(&[
                LIVE,
                "--dry-run",
                "--clock",
                "now",
                "--now",
                now,
                "--windows",
                n,
            ])
        };
        let (one, two) = (windows("1"), windows("2"));
        let second = two.strip_prefix(one.as_str()).unwrap_or_else(|| {
            panic!(
                "anchored at {now}, window 0 changes when a second window follows; {}",
                first_difference(&two, &one)
            )
        });
        let (first, next) = (device_clocks(&one), device_clocks(second));
        for (k, clocks) in [(0_i64, &first), (1, &next)] {
            let (lo, hi) = ((s0 + k * WINDOW_S) * 1000, (s0 + (k + 1) * WINDOW_S) * 1000);
            for (client, stamps) in clocks {
                for &ms in stamps {
                    assert!(
                        (lo..hi).contains(&ms),
                        "anchored at {now}: {client} says {ms} in window {k}, outside \
                         [{lo}, {hi})"
                    );
                }
            }
        }
        let mut across = 0;
        for (client, before) in &first {
            let (Some(last), Some(after)) = (
                before.iter().max(),
                next.get(client).and_then(|a| a.iter().min()),
            ) else {
                continue;
            };
            assert!(
                last < after,
                "anchored at {now}: {client}'s clock goes back from {last} in window 0 to \
                 {after} in window 1"
            );
            across += 1;
        }
        assert!(
            across >= 35,
            "anchored at {now}: only {across} devices' clocks were read in both windows"
        );
    }
}

/// Exercises `sim/mqtt.py`'s `Client` against fake brokers on a local socket, for
/// [`the_simulators_mqtt_client_bounds_its_waits_and_reads_while_idle`]. Each step runs in
/// a thread joined with a limit, so a wait that never ends is reported as `hung`, not
/// waited for. Prints each step's result (its error text, or what it returned) and its
/// duration, as JSON.
const MQTT_CLIENT_HARNESS: &str = r#"
import json, socket, sys, threading, time
sys.path.insert(0, "demo/rules")
from sim.mqtt import Client, MqttError

srv = socket.create_server(("127.0.0.1", 0))
port = srv.getsockname()[1]
out = {}


def step(name, fn, limit=8.0):
    box = {}

    def run():
        try:
            box["result"] = fn()
        except (MqttError, OSError) as e:
            box["result"] = str(e)

    t = threading.Thread(target=run, daemon=True)
    began = time.monotonic()
    t.start()
    t.join(limit)
    out[name] = "hung" if t.is_alive() else box["result"]
    out[name + "_s"] = time.monotonic() - began


def until(client, seen):
    deadline = time.monotonic() + 5
    while not seen():
        if time.monotonic() > deadline:
            return "not seen"
        client.drain()
        time.sleep(0.01)
    return "seen"


# A broker that takes the connection and never answers.
mute = Client("127.0.0.1", port, "mute", timeout=0.5)
step("mute", lambda: mute.connect() or "connected")
out["mute_socket_closed"] = mute.sock is None
srv.accept()[0].close()

# A broker that answers, sends a PINGRESP and a PUBLISH when told to, and then hangs up.
go, bye = threading.Event(), threading.Event()


def broker():
    conn, _ = srv.accept()
    conn.recv(1024)
    conn.sendall(bytes([0x20, 2, 0, 0]))
    go.wait(5)
    conn.sendall(bytes([0xD0, 0, 0x30, 4, 0, 1]) + b"tx")
    bye.wait(5)
    conn.close()


threading.Thread(target=broker, daemon=True).start()
quiet = Client("127.0.0.1", port, "quiet", timeout=5)
quiet.connect()
step("idle", lambda: quiet.drain() or "nothing")
go.set()
step("publish", lambda: until(quiet, lambda: quiet.inbox))
out["inbox"] = [[m.topic, m.payload.decode()] for m in quiet.inbox]
bye.set()
step("closed", lambda: until(quiet, lambda: False))
print(json.dumps(out))
"#;

/// The simulators' MQTT client bounds every wait and reads while idle (the live
/// simulator's player relies on both). A broker that accepts the connection and never sends
/// CONNACK fails the connect after the client's timeout, with the socket closed, instead of
/// hanging it. `drain()` returns at once when nothing has arrived, handles what has (a
/// PINGRESP is read and dropped, a PUBLISH lands in the inbox), and reports a connection
/// the broker closed.
#[test]
fn the_simulators_mqtt_client_bounds_its_waits_and_reads_while_idle() {
    let out: serde_json::Value =
        serde_json::from_str(&python_out(&["-c", MQTT_CLIENT_HARNESS])).expect("JSON");
    let secs = |k: &str| out[k].as_f64().expect("seconds");
    assert_eq!(out["mute"], "mute: no CONNACK within 0.5 s", "{out}");
    assert!(
        (0.4..3.0).contains(&secs("mute_s")),
        "the CONNACK wait took {} s, not about the 0.5 s timeout",
        secs("mute_s")
    );
    assert_eq!(
        out["mute_socket_closed"], true,
        "a failed connect closes its socket"
    );
    assert_eq!(out["idle"], "nothing", "{out}");
    assert!(
        secs("idle_s") < 1.0,
        "drain() waited {} s with nothing to read",
        secs("idle_s")
    );
    assert_eq!(out["publish"], "seen", "{out}");
    assert_eq!(out["inbox"], serde_json::json!([["t", "x"]]), "{out}");
    assert_eq!(
        out["closed"], "quiet: connection closed by the broker",
        "{out}"
    );
}

/// Exercises `sim/live.py` without a broker, for
/// [`the_live_player_backs_off_and_keeps_devices_offline_where_the_script_does`]: its
/// backoff's delays over twelve failures in a row, and what `begin()` and `skip()` do to
/// four connected devices (fakes that record how their connection ends). Prints JSON.
const LIVE_PLAYER_HARNESS: &str = r#"
import json, random, sys
sys.path.insert(0, "demo/rules")
from sim.core import Event
from sim.live import Backoff, LivePlayer

b = Backoff(rng=random.Random(1))
delays = [b.failed(100.0) for _ in range(12)]
waiting = [b.ready(100.0 + delays[-1] - 0.01), b.ready(100.0 + delays[-1])]
b.reset()
waiting.append(b.ready(0.0))

ended = []


class Fake:
    def __init__(self, name):
        self.name = name

    def disconnect(self):
        ended.append([self.name, "disconnect"])

    def drop(self):
        ended.append([self.name, "drop"])


player = LivePlayer("fake", 0)
player.clients = {name: Fake(name) for name in ("ev", "van", "car", "meter")}
player.begin([
    Event(at=0.0, client="van"),
    Event(at=10.0, client="car", kind="disconnect"),
    Event(at=40.0, client="ev", kind="connect"),
    Event(at=41.0, client="ev"),
])
began = sorted(player.clients)
for ev in (Event(at=50.0, client="car", kind="disconnect"),
           Event(at=60.0, client="meter", kind="drop"),
           Event(at=70.0, client="van"),
           Event(at=80.0, client="nobody", kind="connect")):
    player.skip(ev)
print(json.dumps({"delays": delays, "waiting": waiting, "ended": ended, "began": began,
                  "left": sorted(player.clients)}))
"#;

/// The live player's backoff and its bookkeeping of connections, without a broker. The
/// n-th failure in a row waits between half and all of 0.5 s * 2^n, capped at 30 s, and
/// `reset()` clears it. Where play begins (a new window, or a jump into one), a device
/// whose next event is an explicit connect is offline until then, as at the start of the
/// README's run, so its old connection is closed cleanly; the others stay. A skipped
/// disconnect or drop still ends the connection (without a DISCONNECT for a drop); a
/// skipped publish or connect sends nothing.
#[test]
fn the_live_player_backs_off_and_keeps_devices_offline_where_the_script_does() {
    let out: serde_json::Value =
        serde_json::from_str(&python_out(&["-c", LIVE_PLAYER_HARNESS])).expect("JSON");
    let delays = out["delays"].as_array().expect("delays");
    assert_eq!(delays.len(), 12);
    let mut step = 0.5_f64;
    for (n, d) in delays.iter().enumerate() {
        let d = d.as_f64().expect("seconds");
        let cap = step.min(30.0);
        assert!(
            (cap / 2.0..=cap).contains(&d),
            "failure {n} waits {d} s, not within [{}, {cap}]",
            cap / 2.0
        );
        step *= 2.0;
    }
    assert_eq!(
        out["waiting"],
        serde_json::json!([false, true, true]),
        "not ready until the delay has passed; ready again after reset()"
    );
    assert_eq!(
        out["began"],
        serde_json::json!(["car", "meter", "van"]),
        "begin() closes only the device whose first event is a connect"
    );
    assert_eq!(
        out["ended"],
        serde_json::json!([
            ["ev", "disconnect"],
            ["car", "disconnect"],
            ["meter", "drop"]
        ]),
        "how each connection ended"
    );
    assert_eq!(out["left"], serde_json::json!(["van"]));
}

/// The rule editor's script and page (`demo/rules-live/ui/`).
const UI_SCRIPT: &str = "demo/rules-live/ui/app.js";
const UI_PAGE: &str = "demo/rules-live/ui/index.html";

/// Where each tag of an HTML page starts and ends (past its `>`). A `>` inside a quoted
/// attribute value does not end the tag.
fn html_tags(html: &str) -> Vec<(usize, usize)> {
    let mut tags = Vec::new();
    let mut from = 0;
    while let Some(at) = html[from..].find('<') {
        let start = from + at;
        let mut quote = None;
        let mut end = None;
        for (i, c) in html[start..].char_indices() {
            match quote {
                Some(q) if c == q => quote = None,
                None if c == '"' || c == '\'' => quote = Some(c),
                None if c == '>' => {
                    end = Some(start + i + 1);
                    break;
                }
                _ => {}
            }
        }
        let end = end.unwrap_or_else(|| panic!("an unclosed tag: {}", &html[start..]));
        tags.push((start, end));
        from = end;
    }
    tags
}

/// A tag, lower-cased, with its quoted attribute values blanked: what is left is the tag
/// name and the attribute names.
fn tag_names(tag: &str) -> String {
    let mut quote = None;
    tag.chars()
        .map(|c| match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
                ' '
            }
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                ' '
            }
            None => c.to_ascii_lowercase(),
        })
        .collect()
}

/// The name of an `on…=` event handler attribute in a tag, if it has one.
fn handler_attribute(tag: &str) -> Option<String> {
    let bare = tag_names(tag);
    let b = bare.as_bytes();
    (1..b.len()).find_map(|i| {
        if !b[i - 1].is_ascii_whitespace() || !b[i..].starts_with(b"on") {
            return None;
        }
        let name = 2 + b[i + 2..]
            .iter()
            .take_while(|c| c.is_ascii_alphanumeric() || **c == b'_')
            .count();
        let after = b[i + name..].iter().find(|c| !c.is_ascii_whitespace());
        (name > 2 && after == Some(&b'=')).then(|| bare[i..i + name].to_string())
    })
}

/// The rule editor shows what anyone on the broker can publish, and holds a certificate
/// that may rewrite the rules, so text from MQTT or the admin API must never become markup
/// or code on its page. `app.js` uses none of the DOM's HTML parsers and runs no string as
/// code: it builds the page from text (`textContent`, or text nodes). `index.html` has no
/// inline script, no style element and no event handler attribute, so server.py's
/// Content-Security-Policy can forbid all three (see
/// [`the_rule_editors_server_refuses_foreign_requests_and_sends_strict_headers`]).
#[test]
fn the_rules_editor_renders_text_never_markup() {
    let script = read(UI_SCRIPT);
    let page = read(UI_PAGE);
    for (file, text) in [(UI_SCRIPT, &script), (UI_PAGE, &page)] {
        for sink in [
            "innerHTML",
            "outerHTML",
            "insertAdjacentHTML",
            "document.write",
            "createContextualFragment",
            "DOMParser",
            "eval(",
            "new Function",
        ] {
            assert!(
                !text.contains(sink),
                "{file} uses {sink}: text from MQTT or the admin API could become markup or \
                 code there"
            );
        }
    }
    assert!(
        script.len() > 1024 && script.contains(".textContent ="),
        "{UI_SCRIPT} is not the page's script any more: {} bytes, setting textContent: {}",
        script.len(),
        script.contains(".textContent =")
    );

    let tags = html_tags(&page);
    assert!(tags.len() > 50, "{UI_PAGE}: only {} tags found", tags.len());
    let mut scripts = Vec::new();
    for &(start, end) in &tags {
        let tag = &page[start..end];
        let bare = tag_names(tag);
        let name: String = bare[1..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '!')
            .collect();
        assert_ne!(name, "style", "{UI_PAGE} has a style element: {tag}");
        if name == "script" {
            assert!(
                bare.contains(" src=") && page[end..].starts_with("</script>"),
                "{UI_PAGE} has an inline script: {tag}"
            );
            scripts.push(tag);
        }
        assert_eq!(
            handler_attribute(tag),
            None,
            "{UI_PAGE} has an event handler attribute: {tag}"
        );
    }
    assert_eq!(
        scripts,
        [r#"<script src="app.js" defer>"#],
        "{UI_PAGE}'s scripts"
    );
    assert_eq!(
        handler_attribute(r#"<button type="button" onClick = "go()">"#).as_deref(),
        Some("onclick"),
        "the event handler check finds a handler"
    );
    assert_eq!(
        handler_attribute(r#"<input title="on=" data-x="1">"#),
        None,
        "the event handler check reads attribute names, not values"
    );
}

/// Drives `demo/rules-live/ui/server.py` without a broker, for
/// [`the_rule_editors_server_refuses_foreign_requests_and_sends_strict_headers`]: its pure
/// `refusal()` on a table of requests, then its real handler on a free loopback port over
/// raw HTTP. The admin API is unreachable (no certificates, and a port nothing listens
/// on), and every call to `admin()` is recorded. Prints JSON.
const UI_SERVER_HARNESS: &str = r#"
import json, os, socket, sys, tempfile, threading
from email.message import Message

closed = socket.socket()
closed.bind(("127.0.0.1", 0))  # held, never listening: a dial is refused
tmp = tempfile.TemporaryDirectory()
os.environ["UI_ADMIN"] = "127.0.0.1:%d" % closed.getsockname()[1]
os.environ["UI_PKI"] = os.path.join(tmp.name, "no-pki")
sys.path.insert(0, "demo/rules-live/ui")
import server
import sim.mqtt

out = {"client": [m for m in ("poll", "drop") if callable(getattr(sim.mqtt.Client, m, None))]}

P = 8070
L, N = "localhost:%d" % P, "127.0.0.1:%d" % P
JSON = ("Content-Type", "application/json")
UI = ("X-Rules-UI", "1")


def refusal(method, *headers):
    h = Message()
    for k, v in headers:
        h[k] = v  # a second Host is a second header, as on the wire
    r = server.refusal(method, h, P)
    return None if r is None else [r[0], r[1]]


out["refusal"] = {
    "GET without Host": refusal("GET"),
    "GET with two Hosts": refusal("GET", ("Host", L), ("Host", L)),
    "GET for another name": refusal("GET", ("Host", "evil.example:%d" % P)),
    "GET for another port": refusal("GET", ("Host", "localhost:%d" % (P + 1))),
    "GET without a port": refusal("GET", ("Host", "localhost")),
    "GET for [::1]": refusal("GET", ("Host", "[::1]:%d" % P)),
    "GET for localhost": refusal("GET", ("Host", L)),
    "GET for 127.0.0.1": refusal("GET", ("Host", N)),
    "PUT without Origin": refusal("PUT", ("Host", L), JSON, UI),
    "PUT from another site": refusal("PUT", ("Host", L), ("Origin", "http://evil.example"), JSON, UI),
    "PUT from the other loopback": refusal("PUT", ("Host", L), ("Origin", "http://" + N), JSON, UI),
    "PUT from https": refusal("PUT", ("Host", L), ("Origin", "https://" + L), JSON, UI),
    "PUT from an opaque origin": refusal("PUT", ("Host", L), ("Origin", "null"), JSON, UI),
    "PUT for another name, from it": refusal(
        "PUT", ("Host", "evil.example:%d" % P), ("Origin", "http://evil.example:%d" % P), JSON, UI),
    "PUT as a form": refusal(
        "PUT", ("Host", L), ("Origin", "http://" + L),
        ("Content-Type", "application/x-www-form-urlencoded"), UI),
    "PUT as text": refusal("PUT", ("Host", L), ("Origin", "http://" + L), ("Content-Type", "text/plain"), UI),
    "PUT without Content-Type": refusal("PUT", ("Host", L), ("Origin", "http://" + L), UI),
    "PUT without X-Rules-UI": refusal("PUT", ("Host", L), ("Origin", "http://" + L), JSON),
    "PUT with X-Rules-UI: 0": refusal("PUT", ("Host", L), ("Origin", "http://" + L), JSON, ("X-Rules-UI", "0")),
    "PUT from this page": refusal(
        "PUT", ("Host", L), ("Origin", "http://" + L), ("Content-Type", "application/json; charset=utf-8"), UI),
    "POST from this page on 127.0.0.1": refusal("POST", ("Host", N), ("Origin", "http://" + N), JSON, UI),
    "DELETE without Origin": refusal("DELETE", ("Host", L), JSON, UI),
    "DELETE from this page": refusal("DELETE", ("Host", L), ("Origin", "http://" + L), JSON, UI),
}

calls = []
dial = server.admin


def admin(method, path, body):
    calls.append([method, path])
    return dial(method, path, body)


server.admin = admin
server.Handler.hub = server.Hub()  # no MQTT connection: no device message seen
httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
port = httpd.server_address[1]
server.UI_PORT = port
threading.Thread(target=httpd.serve_forever, daemon=True).start()
HOST = "127.0.0.1:%d" % port


def http(method, path, *headers, host=HOST):
    lines = ["%s %s HTTP/1.1" % (method, path), "Host: " + host]
    lines += ["%s: %s" % h for h in headers] + ["Connection: close", "", ""]
    with socket.create_connection(("127.0.0.1", port), timeout=10) as s:
        s.sendall("\r\n".join(lines).encode())
        data = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    status, *fields = head.decode("latin-1").split("\r\n")
    got = {"status": int(status.split()[1]), "headers": {}, "code": None}
    for f in fields:
        k, _, v = f.partition(":")
        got["headers"][k.strip().lower()] = v.strip()
    if got["headers"].get("content-type") == "application/json":
        got["code"] = json.loads(body)["error"]["code"]
    return got


out["page"] = http("GET", "/")
out["api"] = http("GET", "/api/latest?topic_filter=plant/%2B/poc/grid")
out["rebound"] = http("GET", "/", host="evil.example:%d" % port)
out["foreign_reset"] = http("PUT", "/api/reset", ("Origin", "http://evil.example"), JSON, UI)
out["calls_after_foreign"] = list(calls)
out["own_reset"] = http("PUT", "/api/reset", ("Origin", "http://" + HOST), JSON, UI)
out["calls"] = calls
httpd.shutdown()
httpd.server_close()
closed.close()
tmp.cleanup()
print(json.dumps(out))
"#;

/// What `refusal()` answers each request of [`UI_SERVER_HARNESS`]'s table: refused, with
/// a status and a code, or served (`None`).
const UI_REFUSALS: [(&str, Option<(u16, &str)>); 23] = [
    ("GET without Host", Some((403, "bad-host"))),
    ("GET with two Hosts", Some((403, "bad-host"))),
    ("GET for another name", Some((403, "bad-host"))),
    ("GET for another port", Some((403, "bad-host"))),
    ("GET without a port", Some((403, "bad-host"))),
    ("GET for [::1]", Some((403, "bad-host"))),
    ("GET for localhost", None),
    ("GET for 127.0.0.1", None),
    ("PUT without Origin", Some((403, "bad-origin"))),
    ("PUT from another site", Some((403, "bad-origin"))),
    ("PUT from the other loopback", Some((403, "bad-origin"))),
    ("PUT from https", Some((403, "bad-origin"))),
    ("PUT from an opaque origin", Some((403, "bad-origin"))),
    ("PUT for another name, from it", Some((403, "bad-host"))),
    ("PUT as a form", Some((415, "bad-content-type"))),
    ("PUT as text", Some((415, "bad-content-type"))),
    ("PUT without Content-Type", Some((415, "bad-content-type"))),
    ("PUT without X-Rules-UI", Some((403, "missing-header"))),
    ("PUT with X-Rules-UI: 0", Some((403, "missing-header"))),
    ("PUT from this page", None),
    ("POST from this page on 127.0.0.1", None),
    ("DELETE without Origin", Some((403, "bad-origin"))),
    ("DELETE from this page", None),
];

/// The rule editor's server holds a certificate that may rewrite the rules and asks
/// nobody for a password, so it refuses what another web site could make a browser send
/// it. It serves only `Host: localhost:<port>` or `127.0.0.1:<port>`, one Host, with its
/// port (a name rebound to 127.0.0.1 is refused). A GET needs nothing more; a request that
/// changes something needs this page's exact Origin, a JSON body and `X-Rules-UI: 1`.
/// Over HTTP, every answer carries a Content-Security-Policy that allows only the server's
/// own script and style, never inline ones, with `nosniff` and `no-referrer`; an /api/
/// answer is not cached; and "Reset to the shipped rules" from a foreign Origin is refused
/// before the admin API is called, while the same request from the page calls it. The
/// server's MQTT client is the simulators' `Client`, which must keep the `poll` and `drop`
/// the server relies on.
#[test]
fn the_rule_editors_server_refuses_foreign_requests_and_sends_strict_headers() {
    let out: serde_json::Value = serde_json::from_str(&python_out_env(
        &["-c", UI_SERVER_HARNESS],
        &[("UI_RULES", "demo/rules")],
    ))
    .expect("JSON");
    assert_eq!(
        out["client"],
        serde_json::json!(["poll", "drop"]),
        "sim.mqtt.Client's methods server.py uses"
    );

    let refusals = out["refusal"].as_object().expect("the refusal table");
    for (case, want) in UI_REFUSALS {
        let want = want.map_or(serde_json::Value::Null, |(status, code)| {
            serde_json::json!([status, code])
        });
        assert_eq!(refusals.get(case), Some(&want), "refusal(): {case}");
    }
    assert_eq!(refusals.len(), UI_REFUSALS.len(), "{out}");

    let header_of = |answer: &str, name: &str| {
        out[answer]["headers"][name]
            .as_str()
            .unwrap_or_default()
            .to_string()
    };
    assert_eq!(out["page"]["status"], 200, "{out}");
    assert!(
        header_of("page", "content-type").starts_with("text/html"),
        "{out}"
    );
    for answer in ["page", "rebound", "foreign_reset"] {
        let csp = header_of(answer, "content-security-policy");
        for directive in [
            "default-src 'self'",
            "script-src 'self'",
            "style-src 'self'",
            "frame-ancestors 'none'",
        ] {
            assert!(
                csp.contains(directive),
                "{answer}: the CSP {csp:?} lacks {directive}"
            );
        }
        assert!(
            !csp.contains("unsafe"),
            "{answer}: the CSP {csp:?} allows inline or eval'd code"
        );
        assert_eq!(
            header_of(answer, "x-content-type-options"),
            "nosniff",
            "{answer}"
        );
        assert_eq!(
            header_of(answer, "referrer-policy"),
            "no-referrer",
            "{answer}"
        );
    }
    assert_eq!(out["api"]["status"], 404, "{out}");
    assert_eq!(out["api"]["code"], "no-input", "{out}");
    assert_eq!(header_of("api", "cache-control"), "no-store", "{out}");
    assert_eq!(out["rebound"]["status"], 403, "{out}");
    assert_eq!(out["rebound"]["code"], "bad-host", "{out}");

    assert_eq!(out["foreign_reset"]["status"], 403, "{out}");
    assert_eq!(out["foreign_reset"]["code"], "bad-origin", "{out}");
    assert_eq!(
        out["calls_after_foreign"],
        serde_json::json!([]),
        "a refused reset called the admin API"
    );
    assert_eq!(out["own_reset"]["status"], 502, "{out}");
    assert_eq!(out["own_reset"]["code"], "admin-unreachable", "{out}");
    assert_eq!(
        out["calls"],
        serde_json::json!([["PUT", "/admin/v1/rules?if_match=%2A"]]),
        "the page's own reset calls the admin API once, to replace whatever is there"
    );
}

/// The rules file for [`the_live_simulator_rides_out_a_broker_restart_and_stops_cleanly`]:
/// each client's connection state, `connected` or the broker's disconnect reason, retained,
/// so a watcher that subscribes after a device connected still sees it.
#[cfg(unix)]
const PRESENCE_RULES: &str = r#"
[rules.presence]
sql = '''
SELECT clientid,
  CASE WHEN event = 'client.connected' THEN 'connected' ELSE reason END AS state
FROM "$events/client/connected", "$events/client/disconnected"
'''
actions = [
  { function = "republish", args = { topic = "test/presence/${clientid}", qos = 1, retain = true, payload = "${state}" } },
]
"#;

/// A running `live.py`, and every line it has written to stdout or stderr so far.
#[cfg(unix)]
struct LiveSim {
    child: ChildGuard,
    lines: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

#[cfg(unix)]
impl LiveSim {
    fn spawn(args: &[&str], env: &[(&str, &str)]) -> Self {
        use std::io::BufRead as _;
        let mut child = python3()
            .envs(env.iter().copied())
            .arg(LIVE)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("python3 runs live.py");
        let lines = std::sync::Arc::<std::sync::Mutex<Vec<String>>>::default();
        let out: [Box<dyn std::io::Read + Send>; 2] = [
            Box::new(child.stdout.take().expect("piped")),
            Box::new(child.stderr.take().expect("piped")),
        ];
        for stream in out {
            let lines = lines.clone();
            std::thread::spawn(move || {
                for line in std::io::BufReader::new(stream)
                    .lines()
                    .map_while(Result::ok)
                {
                    lines.lock().unwrap().push(line);
                }
            });
        }
        LiveSim {
            child: ChildGuard(child),
            lines,
        }
    }

    fn said(&self) -> String {
        self.lines.lock().unwrap().join("\n")
    }

    /// Send SIGTERM and wait, at most 15 s, for it to exit.
    async fn terminate(&mut self) -> std::process::ExitStatus {
        let pid = self.child.0.id().to_string();
        let sent = Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("run kill");
        assert!(sent.success(), "kill -TERM {pid}");
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.child.0.try_wait().expect("wait for live.py") {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "live.py did not exit on SIGTERM:\n{}",
                self.said()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The reconnects its latest heartbeat counts.
    fn reconnects(&self) -> u64 {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .filter_map(|l| {
                l.split_once("reconnects ")?
                    .1
                    .split(',')
                    .next()?
                    .parse()
                    .ok()
            })
            .max()
            .unwrap_or(0)
    }
}

/// The client id of the restart test's watcher, whose own presence is not a device's.
#[cfg(unix)]
const LIVE_WATCHER: &str = "demo-test-live-watch";

/// What a watcher has seen of the live simulator: how many device messages, and each
/// device's connection state as the presence rule last published it.
#[cfg(unix)]
#[derive(Default)]
struct Seen {
    device: usize,
    state: BTreeMap<String, String>,
}

#[cfg(unix)]
impl Seen {
    fn connected(&self) -> Vec<String> {
        self.state
            .iter()
            .filter(|(_, s)| *s == "connected")
            .map(|(c, _)| c.clone())
            .collect()
    }
}

#[cfg(unix)]
async fn watch_live(addr: SocketAddr) -> Client {
    let mut w = Client::connect(addr, LIVE_WATCHER).await;
    w.subscribe(1, "plant/#", QoS::AtMostOnce).await;
    w.subscribe(2, "test/presence/#", QoS::AtLeastOnce).await;
    w
}

/// Read what `w` receives into `seen` until `done(seen)` holds; fail after `within`.
#[cfg(unix)]
async fn watch_until(
    w: &mut Client,
    seen: &mut Seen,
    within: Duration,
    what: &str,
    sim: &LiveSim,
    done: impl Fn(&Seen) -> bool,
) {
    let deadline = Instant::now() + within;
    while !done(seen) {
        assert!(
            Instant::now() < deadline,
            "{what}: not within {within:?}; {} device messages, states {:?}\nlive.py said:\n{}",
            seen.device,
            seen.state,
            sim.said()
        );
        match w.recv_bounded(Duration::from_millis(500)).await {
            Recv::Packet(Packet::Publish(p)) => {
                if let Some(id) = p.pkid {
                    w.puback(id).await;
                }
                if let Some(client) = p.topic.strip_prefix("test/presence/") {
                    if client != LIVE_WATCHER {
                        let state = String::from_utf8_lossy(&p.payload).into_owned();
                        seen.state.insert(client.to_string(), state);
                    }
                } else {
                    seen.device += 1;
                }
            }
            Recv::Packet(_) | Recv::Quiet => {}
            Recv::Closed => panic!("{what}: the watcher's connection closed"),
        }
    }
}

/// The live simulator outlives its broker and stops cleanly, against the real binary. It
/// plays the power domain into a broker (found through `SIM_HOST` and `SIM_PORT`, quiet by
/// `SIM_QUIET`, as the demo stack configures it), which is killed; a new broker starts on
/// the same port, and the devices reconnect by themselves: their data flows again and the
/// heartbeat counts the reconnects. On SIGTERM it exits 0, and every device it had
/// connected sends a DISCONNECT first: the broker reports `normal`, not `tcp_closed`.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_live_simulator_rides_out_a_broker_restart_and_stops_cleanly() {
    let dir = common::TempDir::new();
    let rules = dir.path().join("rules.toml");
    std::fs::write(&rules, PRESENCE_RULES).expect("write the rules file");
    let first = start_broker_with(&rules, None).await;
    let addr = first.addr;
    let port = addr.port().to_string();
    let mut sim = LiveSim::spawn(
        &[
            "--domains",
            "power",
            "--clock",
            "fixture",
            "--heartbeat",
            "1",
        ],
        &[
            ("SIM_HOST", "127.0.0.1"),
            ("SIM_PORT", &port),
            ("SIM_QUIET", "1"),
        ],
    );

    let mut w = watch_live(addr).await;
    let mut seen = Seen::default();
    watch_until(
        &mut w,
        &mut seen,
        Duration::from_secs(30),
        "device data in the first broker",
        &sim,
        |s| s.device >= 5,
    )
    .await;

    // Killed: every device's connection ends without a word.
    drop(w);
    drop(first);
    let _second = start_broker_with(&rules, Some(addr)).await;
    let mut w = watch_live(addr).await;
    let mut seen = Seen::default();
    watch_until(
        &mut w,
        &mut seen,
        Duration::from_secs(30),
        "the devices back on the restarted broker",
        &sim,
        |s| s.device >= 5 && s.connected().len() >= 3,
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while sim.reconnects() == 0 {
        assert!(
            Instant::now() < deadline,
            "no heartbeat counts a reconnect:\n{}",
            sim.said()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let online = seen.connected();
    let status = sim.terminate().await;
    assert!(
        status.success(),
        "live.py exited {status} on SIGTERM:\n{}",
        sim.said()
    );
    watch_until(
        &mut w,
        &mut seen,
        Duration::from_secs(10),
        "every device connected at SIGTERM disconnected",
        &sim,
        |s| online.iter().all(|c| s.state[c] != "connected"),
    )
    .await;
    for client in &online {
        assert_eq!(
            seen.state[client], "normal",
            "{client} went away without a DISCONNECT"
        );
    }
    let said = sim.said();
    assert!(
        said.contains(&format!(
            "live: power to 127.0.0.1:{port}, seed 7, clock fixture"
        )),
        "the start line:\n{said}"
    );
    assert!(
        said.lines().any(|l| l.starts_with("live: stopped: ")),
        "the stop line:\n{said}"
    );
    assert!(
        !said.contains(" → "),
        "SIM_QUIET=1, yet it printed device messages:\n{said}"
    );
}
