//! The rule-engine demo (`demo/rules/`), kept true.
//!
//! The demo plays simulated power-plant, home and car telemetry into mqttd and prints what
//! its rules derive. This suite takes the demo's seeded fixture (the ten simulated minutes
//! `simulate.py --dry-run` writes for the README's seed and start time), replays it through
//! the REAL `mqttd` binary loading `demo/rules/rules.toml`, one connection per simulated
//! device as the simulator's player makes them, and asserts that the derived messages a
//! `QoS` 1 subscriber receives are exactly `rules_demo.expected` beside this file: topic,
//! `QoS`, retain flag and payload, with nothing missing and nothing extra. It also checks
//! that the simulator is deterministic, that the rules file passes `--check-rules` with no
//! warning, and that every derived message the README quotes is one the fixture produces.
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

use std::collections::HashMap;
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
    let out = Command::new("python3")
        .arg(repo_root().join(SIMULATOR))
        .args(FIXTURE_ARGS)
        .output()
        .expect("python3 runs the simulator");
    assert!(
        out.status.success(),
        "simulate.py failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("UTF-8")
}

fn blessing() -> bool {
    std::env::var_os("MQTTD_DEMO_BLESS").is_some()
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
    let rules = repo_root().join(RULES);
    let (child, log, addr) = listen_wait::spawn_listening_logged(|| {
        let addr: SocketAddr = format!("127.0.0.1:{}", proc_common::free_tcp_port())
            .parse()
            .unwrap();
        let mut cmd = mqttd();
        cmd.env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_DURABLE_SESSIONS", "0")
            .env("MQTTD_RULES_FILE", &rules)
            .env("RUST_LOG", "mqttd=info")
            .env("NO_COLOR", "1")
            .stderr(Stdio::null());
        (cmd, vec![addr], addr)
    })
    .await;
    assert!(
        log.text().contains("rules loaded"),
        "the broker did not load {RULES}:\n{}",
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

/// The fixture replayed through the real broker derives exactly `expected.txt`, and no
/// rule fails along the way.
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
        "the demo's ten minutes derive more than {} messages",
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
