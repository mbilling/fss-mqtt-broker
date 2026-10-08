//! The rule engine through the real `mqttd` binary (ADR 0083, docs/RULES.md).
//!
//! `tests/rules.rs` drives an in-process miniature of the broker: no ingress credit, a
//! memory session store, an allow-all ACL, and rules handed over a channel instead of
//! loaded from a file. Nothing there proves what an operator gets from the shipped
//! binary. Every test here runs `CARGO_BIN_EXE_mqttd` configured the way an operator
//! configures it — a `--config` TOML file whose `[rules] file` names a rules file (or
//! `MQTTD_RULES_FILE`), `MQTTD_*` variables for the listeners, an ACL file, a data
//! directory — and then talks MQTT 3.1.1 and 5 to it over TCP, reads `/metrics` from
//! `MQTTD_HEALTH_BIND`, signals it, restarts it and reads its log. The offline commands
//! (`--check-rules`, `--rule-test`) are run as a user runs them, and their output is
//! compared byte for byte; the other command-line modes are run with stdout closed.
//!
//! Each test's doc comment names the documented claim it pins and how it would fail if
//! the claim broke.

mod common;
mod listen_wait;
mod proc_common;

use std::fmt::Write as _;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use common::{Client, Recv};
use mqtt_codec::packet::{ConnAck, Connect, Subscribe, SubscribeFilter, Unsubscribe};
use mqtt_codec::{
    Disconnect, Packet, Properties, Property, ProtocolVersion, QoS, SubscriptionOptions,
};

const V4: ProtocolVersion = ProtocolVersion::V311;
const V5: ProtocolVersion = ProtocolVersion::V5;

// ---------------------------------------------------------------------------------------
// Running the binary
// ---------------------------------------------------------------------------------------

/// Kills the broker when the test ends, a panicking test included.
struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The broker binary with none of the runner's `MQTTD_*` environment, so a test is
/// configured by exactly what it sets, and with plain (uncoloured) log lines.
fn mqttd() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (key, _) in std::env::vars() {
        if key.starts_with("MQTTD_") {
            cmd.env_remove(key);
        }
    }
    cmd.env("NO_COLOR", "1").stdin(Stdio::null());
    cmd
}

/// A fresh loopback address on the shared test port band (`proc_common`).
fn band_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], proc_common::free_tcp_port()))
}

/// What a command printed, and how it exited.
#[derive(Debug)]
struct Ran {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

/// Read a child's pipe to its end on a thread, so a command that prints more than a pipe
/// holds never blocks on it.
fn read_all(mut pipe: impl std::io::Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut text = String::new();
        pipe.read_to_string(&mut text)
            .expect("read the command's output");
        text
    })
}

/// Run `cmd` to completion and capture its output. Bounded: a command that hangs, or
/// boots a broker instead of exiting, fails the test instead of wedging the suite.
fn run(mut cmd: Command) -> Ran {
    let what = format!("{:?}", cmd.get_args().collect::<Vec<_>>());
    let mut child = ChildGuard(
        cmd.stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn mqttd"),
    );
    let stdout = read_all(child.0.stdout.take().expect("stdout piped"));
    let stderr = read_all(child.0.stderr.take().expect("stderr piped"));
    let status = wait_bounded(&mut child, &what);
    Ran {
        code: status.code(),
        stdout: stdout.join().expect("stdout reader"),
        stderr: stderr.join().expect("stderr reader"),
    }
}

/// Wait for `child` to exit, at most 60 s: a command that hangs, or boots a broker
/// instead of exiting, fails the test instead of wedging the suite.
fn wait_bounded(child: &mut ChildGuard, what: &str) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.0.try_wait().expect("try_wait") {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "mqttd {what} did not exit within 60 s"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Run an offline `mqttd` command.
fn cli(args: &[&str]) -> Ran {
    let mut cmd = mqttd();
    cmd.args(args);
    run(cmd)
}

/// SHA-256 of `bytes` as lower-case hex, computed here rather than by the rules crate, so
/// a broker that reported the wrong digest could not agree with itself.
fn sha256_hex(bytes: &[u8]) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
    let mut hex = String::with_capacity(64);
    for b in digest.as_ref() {
        write!(hex, "{b:02x}").expect("write to a String");
    }
    hex
}

/// Write `text` as `dir/rules.toml`, the file every broker here is configured with.
fn write_rules(dir: &Path, text: &str) {
    std::fs::write(dir.join("rules.toml"), text).expect("write the rules file");
}

/// Replace `dir/rules.toml` the way a Kubernetes `ConfigMap` update does: write a new
/// file and rename it over the old one.
fn replace_rules(dir: &Path, text: &str) {
    let next = dir.join("rules.toml.next");
    std::fs::write(&next, text).expect("write the new rules file");
    std::fs::rename(&next, dir.join("rules.toml")).expect("swap the rules file in");
}

/// How a test configures its broker beyond the base config [`start`] writes.
#[derive(Default)]
struct Setup {
    /// Whole TOML sections appended to the config file (`[runtime]`, `[cluster]`).
    toml: String,
    /// Extra environment, as an operator would export it.
    env: Vec<(&'static str, String)>,
    /// Name the rules file with `MQTTD_RULES_FILE` instead of `[rules] file`.
    rules_by_env: bool,
    /// Join a static-peer cluster: open the inter-node listener (`MQTTD_PEER_BIND`) on a
    /// band port chosen with the others, and dial these peers (`MQTTD_PEERS`).
    peers: Option<Vec<SocketAddr>>,
}

/// A running broker: the real binary, killed when this is dropped.
struct Broker {
    child: ChildGuard,
    log: listen_wait::Log,
    addr: SocketAddr,
    health: SocketAddr,
    /// The inter-node listener, when [`Setup::peers`] asked for one.
    peer: Option<SocketAddr>,
}

/// Start the broker over `dir` as node `node`. `dir/mqttd.toml` is written here: a data
/// directory, anonymous clients, durability off — a persistent single node (ADR 0018) —
/// and `[rules] file = dir/rules.toml`, which the test writes first. The MQTT listener
/// and the health endpoint (`/metrics`) bind fresh band ports through
/// `MQTTD_PLAINTEXT_BIND` and `MQTTD_HEALTH_BIND` (and the peer listener through
/// `MQTTD_PEER_BIND`, when asked). Returns once the broker has logged binding every
/// address; a broker that lost a port to another process is retried on fresh ones,
/// every port chosen again.
async fn start(dir: &Path, node: &str, setup: &Setup) -> Broker {
    let rules = dir.join("rules.toml");
    let config = dir.join("mqttd.toml");
    std::fs::create_dir_all(dir.join("data")).expect("create the data directory");
    let mut text = format!(
        "[node]\nid = \"{node}\"\ndata_dir = \"{}\"\n\n[security]\nallow_anonymous = true\n\n\
         [durable]\nenabled = false\n",
        dir.join("data").display()
    );
    if !setup.rules_by_env {
        writeln!(text, "\n[rules]\nfile = \"{}\"", rules.display()).expect("write to a String");
    }
    text.push_str(&setup.toml);
    std::fs::write(&config, text).expect("write the config file");
    let stderr = std::fs::File::create(dir.join("stderr.log")).expect("stderr log");
    let (child, log, (addr, health, peer)) = listen_wait::spawn_listening_logged(|| {
        let (addr, health) = (band_addr(), band_addr());
        let peer = setup.peers.as_ref().map(|_| band_addr());
        let mut cmd = mqttd();
        cmd.arg("--config")
            .arg(&config)
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_HEALTH_BIND", health.to_string())
            .env("RUST_LOG", "mqttd=info")
            .stderr(stderr.try_clone().expect("stderr log handle"));
        if setup.rules_by_env {
            cmd.env("MQTTD_RULES_FILE", &rules);
        }
        for (key, value) in &setup.env {
            cmd.env(key, value);
        }
        let mut bound = vec![addr, health];
        if let (Some(peer), Some(peers)) = (peer, &setup.peers) {
            cmd.env("MQTTD_PEER_BIND", peer.to_string());
            if !peers.is_empty() {
                let list: Vec<String> = peers.iter().map(SocketAddr::to_string).collect();
                cmd.env("MQTTD_PEERS", list.join(","));
            }
            bound.push(peer);
        }
        (cmd, bound, (addr, health, peer))
    })
    .await;
    Broker {
        child: ChildGuard(child),
        log,
        addr,
        health,
        peer,
    }
}

/// `mqttd_rule_evaluations_total{rule,result}`, as rendered.
fn evaluations(rule: &str, result: &str) -> String {
    format!(r#"mqttd_rule_evaluations_total{{rule="{rule}",result="{result}"}}"#)
}

/// `mqttd_rule_actions_total{rule,result}`, as rendered.
fn actions(rule: &str, result: &str) -> String {
    format!(r#"mqttd_rule_actions_total{{rule="{rule}",result="{result}"}}"#)
}

/// `mqttd_rules_info{checksum}`, as rendered.
fn rules_info(checksum: &str) -> String {
    format!(r#"mqttd_rules_info{{checksum="{checksum}"}}"#)
}

/// One sample from a `/metrics` body by its full series name; `None` when the series is
/// absent (the registry renders only label sets that were touched).
fn sample(metrics: &str, series: &str) -> Option<u64> {
    metrics
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| l.rsplit_once(' '))
        .find(|(name, _)| *name == series)
        .map(|(_, v)| {
            v.trim()
                .parse()
                .unwrap_or_else(|_| panic!("{series}: not an integer: {v}"))
        })
}

/// The `/metrics` lines whose series name starts with `prefix`.
fn series_lines<'a>(metrics: &'a str, prefix: &str) -> Vec<&'a str> {
    metrics.lines().filter(|l| l.starts_with(prefix)).collect()
}

impl Broker {
    /// Send `kill -<sig>` to the broker, as an operator (or an init system) does.
    fn signal(&self, sig: &str) {
        let sent = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(self.child.0.id().to_string())
            .status()
            .expect("run kill");
        assert!(sent.success(), "kill -{sig} failed");
    }

    /// This broker's `/metrics` body, from its health endpoint.
    async fn metrics(&self) -> String {
        proc_common::http_get(self.health, "/metrics")
            .await
            .expect("the health endpoint serves /metrics")
    }

    /// One sample of this broker's metrics (see [`sample`]).
    async fn sample(&self, series: &str) -> Option<u64> {
        sample(&self.metrics().await, series)
    }

    /// Wait until `series` reads exactly `want`. Bounded: fails, naming what it saw.
    async fn wait_metric(&self, series: &str, want: u64) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let have = self.sample(series).await;
            if have == Some(want) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{series}: wanted {want}, still {have:?} after 15 s; metrics:\n{}",
                self.metrics().await
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The log lines (so far) that `keep` selects.
    fn log_lines(&self, keep: impl Fn(&str) -> bool) -> Vec<String> {
        self.log
            .text()
            .lines()
            .filter(|l| keep(l))
            .map(str::to_string)
            .collect()
    }

    /// Wait until a log line ends with `suffix` (the part after the timestamp) and return
    /// it. Bounded: fails with the log's tail.
    async fn wait_log(&self, suffix: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(line) = self.log_lines(|l| l.ends_with(suffix)).pop() {
                return line;
            }
            assert!(
                Instant::now() < deadline,
                "no log line ending {suffix:?} after 15 s; log tail:\n{}",
                self.log.tail(30)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Crash the broker: `SIGKILL`, so nothing is flushed or drained, and reap it.
    fn crash(&mut self) {
        self.child.0.kill().expect("SIGKILL the broker");
        self.child.0.wait().expect("reap the broker");
    }

    /// Stop the broker the operator's way, `SIGTERM` (a graceful drain), and wait for it to
    /// exit. Bounded: fails with the log's tail.
    async fn terminate(&mut self) -> ExitStatus {
        self.signal("TERM");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.0.try_wait().expect("try_wait") {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "mqttd did not exit within 30 s of SIGTERM; log tail:\n{}",
                self.log.tail(30)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

// ---------------------------------------------------------------------------------------
// MQTT helpers over the test `Client`
// ---------------------------------------------------------------------------------------

/// A received PUBLISH: topic, payload, `QoS`, RETAIN.
type Got = (String, String, QoS, bool);

/// The [`Got`] a test expects.
fn got(topic: &str, payload: &str, qos: QoS, retain: bool) -> Got {
    (topic.to_string(), payload.to_string(), qos, retain)
}

/// The next packet on `c`, which must be a PUBLISH; acknowledged when `QoS` 1.
async fn next(c: &mut Client) -> Got {
    let p = c.expect_publish().await;
    if p.qos == QoS::AtLeastOnce {
        c.puback(p.pkid.expect("a QoS 1 PUBLISH has a packet id"))
            .await;
    }
    let payload = String::from_utf8(p.payload.to_vec()).expect("a UTF-8 payload");
    (p.topic, payload, p.qos, p.retain)
}

/// Up to `n` PUBLISH packets, each `QoS` 1 one acknowledged, waiting at most 10 s for each.
/// Stops early when nothing comes, so the caller's exact assertion reports what did
/// arrive rather than a bare timeout. Any other packet fails the test.
async fn collect(c: &mut Client, n: usize) -> Vec<Got> {
    let mut out = Vec::new();
    while out.len() < n {
        match c.recv_bounded(Duration::from_secs(10)).await {
            Recv::Packet(Packet::Publish(p)) => {
                if p.qos == QoS::AtLeastOnce {
                    c.puback(p.pkid.expect("a QoS 1 PUBLISH has a packet id"))
                        .await;
                }
                let payload = String::from_utf8(p.payload.to_vec()).expect("a UTF-8 payload");
                out.push((p.topic, payload, p.qos, p.retain));
            }
            Recv::Quiet | Recv::Closed => break,
            Recv::Packet(other) => panic!("expected only PUBLISH packets, got {other:?}"),
        }
    }
    out
}

/// CONNECT with explicit keepalive, clean flag and properties; the CONNACK.
async fn connect_as(
    addr: SocketAddr,
    version: ProtocolVersion,
    client_id: &str,
    clean: bool,
    keep_alive: u16,
    properties: Vec<Property>,
) -> (Client, ConnAck) {
    let mut c = Client::open(addr, version).await;
    c.send(&Packet::Connect(Connect {
        protocol: version,
        clean_session: clean,
        keep_alive,
        client_id: client_id.to_string(),
        last_will: None,
        username: None,
        password: None,
        properties: Properties(properties),
    }))
    .await;
    match c.recv().await {
        Packet::ConnAck(ack) => (c, ack),
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

/// A persistent v5 session (Session Expiry 3600 s): connect, asserting success and
/// whether the broker had the session.
async fn connect_persistent_v5(addr: SocketAddr, client_id: &str, clean: bool) -> (Client, bool) {
    let (c, ack) = Client::connect_v5(
        addr,
        client_id,
        clean,
        vec![Property::SessionExpiryInterval(3600)],
    )
    .await;
    assert_eq!(ack.code, 0, "{client_id}: CONNACK");
    (c, ack.session_present)
}

/// One SUBSCRIBE carrying `filters` (each with its `QoS` and options); the SUBACK codes.
async fn subscribe_all(
    c: &mut Client,
    pkid: u16,
    filters: &[(&str, QoS, SubscriptionOptions)],
) -> Vec<u8> {
    c.send(&Packet::Subscribe(Subscribe {
        properties: Properties::new(),
        pkid,
        filters: filters
            .iter()
            .map(|(path, qos, options)| SubscribeFilter {
                options: *options,
                path: (*path).to_string(),
                qos: *qos,
            })
            .collect(),
    }))
    .await;
    match c.recv().await {
        Packet::SubAck(a) => {
            assert_eq!(a.pkid, pkid, "SUBACK packet id");
            a.return_codes
        }
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

/// Subscribe to one filter with default options; the SUBACK codes.
async fn subscribe(c: &mut Client, pkid: u16, filter: &str, qos: QoS) -> Vec<u8> {
    c.subscribe(pkid, filter, qos).await.return_codes
}

/// One v5 UNSUBSCRIBE carrying `filters`; the UNSUBACK reason codes.
async fn unsubscribe(c: &mut Client, pkid: u16, filters: &[&str]) -> Vec<u8> {
    c.send(&Packet::Unsubscribe(Unsubscribe {
        pkid,
        filters: filters.iter().map(|f| (*f).to_string()).collect(),
        properties: Properties::new(),
    }))
    .await;
    match c.recv().await {
        Packet::UnsubAck(ack) => {
            assert_eq!(ack.pkid, pkid, "UNSUBACK packet id");
            ack.reason_codes
        }
        other => panic!("expected UNSUBACK, got {other:?}"),
    }
}

/// Publish at `QoS` 1 and wait for the PUBACK; its reason code (always 0 on v3.1.1). The
/// publisher must not be subscribed to anything that could arrive first.
async fn publish_acked(c: &mut Client, topic: &str, payload: &[u8], pkid: u16) -> u8 {
    c.publish(topic, payload, QoS::AtLeastOnce, Some(pkid), vec![])
        .await;
    match c.recv().await {
        Packet::PubAck(ack) => {
            assert_eq!(ack.pkid, pkid, "PUBACK packet id");
            ack.reason
        }
        other => panic!("expected PUBACK {pkid}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------------------

/// RULES.md "The rules file": loading is all-or-nothing and "at startup the broker
/// refuses to boot" on a file that does not load. On the binary: exit code 1, and stderr
/// carries the loader's own multi-line diagnostic — the line and column (2, 49), the
/// offending line and a caret under it — not a one-line Debug string with literal `\n`
/// and `\"`.
/// The file is the trap RULES.md's quoting tip leads to (`''` inside a TOML literal
/// string). A broker that booted without its rules, exited 0, or printed the error
/// through `{:?}` fails the exit-code or the exact-stderr assertion.
#[test]
fn the_broker_refuses_to_boot_on_a_rules_file_that_does_not_load() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        "[rules.go]\nsql = 'SELECT payload FROM \"t\" WHERE payload = ''go'''\n",
    );
    let config = dir.path().join("mqttd.toml");
    std::fs::write(
        &config,
        format!(
            "[security]\nallow_anonymous = true\n\n[durable]\nenabled = false\n\n[rules]\n\
             file = \"{}\"\n",
            dir.path().join("rules.toml").display()
        ),
    )
    .unwrap();
    // The listener binds before the rules load; a band port another process took first
    // would be a bind error, not this test's subject, so take a fresh one then.
    let mut ran = None;
    for _ in 0..3 {
        let mut cmd = mqttd();
        cmd.arg("--config")
            .arg(&config)
            .env("MQTTD_PLAINTEXT_BIND", band_addr().to_string())
            .env("RUST_LOG", "mqttd=info");
        let this = run(cmd);
        if !this.stderr.contains("Address already in use") {
            ran = Some(this);
            break;
        }
    }
    let ran = ran.expect("three band ports in a row were taken");
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert_eq!(
        ran.stderr,
        "Error: rules: rules file: TOML parse error at line 2, column 49\n  \
         |\n2 | sql = 'SELECT payload FROM \"t\" WHERE payload = ''go'''\n  \
         |                                                 ^\n\
         unexpected key or value, expected newline, `#`\n\n"
    );
    assert!(
        !ran.stderr.contains("\\n") && !ran.stderr.contains("\\\""),
        "an escaped Debug string: {}",
        ran.stderr
    );
}

/// RULES.md "The rules file": rules live in their own file, named by `[rules] file`. The
/// natural mistake — writing `[rules.<id>]` straight into the main config, beside
/// `[rules]` — is refused at boot (exit 1) with the unknown key named and the hint that
/// rules go in a file of their own. A broker that silently ignored the table (a rule the
/// operator believes is running, and is not), or refused it without the hint, fails here.
#[test]
fn a_rule_written_into_the_main_config_is_refused_with_where_rules_go() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("mqttd.toml");
    std::fs::write(
        &config,
        "[rules.high_temp]\nsql = 'SELECT * FROM \"sensors/+/data\"'\n",
    )
    .unwrap();
    let mut cmd = mqttd();
    cmd.arg("--config")
        .arg(&config)
        .env("MQTTD_PLAINTEXT_BIND", band_addr().to_string())
        .env("RUST_LOG", "mqttd=info");
    let ran = run(cmd);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert_eq!(
        ran.stderr,
        "Error: config parse error: unknown config key(s): rules.high_temp — a typo, or a \
         config written for a NEWER broker version; set runtime.config_unknown_keys = \
         \"warn\" (or MQTTD_CONFIG_UNKNOWN_KEYS=warn) to boot anyway during a rollback or \
         mixed-version window, ignored keys logged (ADR 0058 T4). Rules are not written in \
         this file: put the [rules.<id>] tables in a rules file of their own and point \
         [rules] file (MQTTD_RULES_FILE) at it (docs/RULES.md)\n"
    );
}

// ---------------------------------------------------------------------------------------
// --check-rules
// ---------------------------------------------------------------------------------------

/// RULES.md's opening example rule, an events rule naming two events, and a disabled rule.
const GOOD_RULES: &str = r#"[rules.high_temp]
description = "Alert on hot sensors"
sql = '''
SELECT payload.temp AS temp, clientid, qos
FROM "sensors/+/data"
WHERE payload.temp > 30
'''
actions = [
  { function = "republish", args = { topic = "alerts/${clientid}", payload = "${.}" } },
]

[rules.presence]
sql = 'SELECT clientid, reason FROM "$events/client/disconnected", "$events/client/connected"'
actions = [{ function = "console" }]

[rules.off]
enable = false
sql = 'SELECT * FROM "a/#"'
"#;

/// What `--check-rules` prints for [`GOOD_RULES`] at `path`.
fn good_rules_listing(path: &str) -> String {
    format!(
        "rules OK: {path}: 3 rule(s), 2 enabled, sha256 {}\n  \
         high_temp (enabled): FROM \"sensors/+/data\", 1 action(s)\n  \
         off (disabled): FROM \"a/#\", 0 action(s)\n  \
         presence (enabled): FROM \"$events/client/disconnected\", \
         \"$events/client/connected\", 1 action(s)\n",
        sha256_hex(GOOD_RULES.as_bytes())
    )
}

/// RULES.md "Operating rules": `mqttd --check-rules <file>` exits 0 for a valid file and
/// prints the summary line — rules, enabled rules and the file's SHA-256 (the value
/// `mqttd_rules_info` reports) — then one line per rule in id order with its state, its
/// FROM and its action count. A wrong digest (checked against an independent SHA-256), a
/// disabled rule counted as enabled, a missing rule line or a non-zero exit fails here.
#[test]
fn check_rules_accepts_a_valid_file_and_lists_every_rule() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), GOOD_RULES);
    let path = dir.path().join("rules.toml").display().to_string();
    let ran = cli(&["--check-rules", &path]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(ran.stdout, good_rules_listing(&path));
    assert_eq!(ran.stderr, "");
}

/// Run `cmd` with its stdout closed before it starts and `stdin` written to it; return its
/// exit code and its stderr.
fn run_with_stdout_closed(mut cmd: Command, stdin: &str, what: &str) -> (Option<i32>, String) {
    let mut child = ChildGuard(
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn mqttd"),
    );
    // The reader is gone before the command has started, let alone written.
    drop(child.0.stdout.take());
    let mut input = child.0.stdin.take().expect("stdin piped");
    input.write_all(stdin.as_bytes()).expect("write stdin");
    drop(input);
    let stderr = read_all(child.0.stderr.take().expect("stderr piped"));
    let status = wait_bounded(&mut child, what);
    (status.code(), stderr.join().expect("stderr reader"))
}

/// A reader that goes away first (`mqttd --check-rules rules.toml | head -1`) neither
/// panics a command-line mode (exit status 101) nor changes its exit status: every mode
/// writes stdout through one helper that treats a closed pipe as the end of the output, and
/// the command finishes as it would have. A failed `--check-tls` still exits 1, and
/// `--decommission` still waits for its target to exit. `--probe` and `--backup`, which need
/// a running broker, are in the next test.
#[test]
fn a_closed_stdout_neither_panics_a_command_nor_changes_its_exit_status() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), GOOD_RULES);
    let path = dir.path().join("rules.toml").display().to_string();
    // TLS material that does not exist: `--check-tls` writes its findings, then fails.
    let broken_tls = dir.path().join("broken-tls.toml");
    std::fs::write(
        &broken_tls,
        format!(
            "[durable]\nenabled = false\n\n[tls]\ncert = \"{0}/missing-cert.pem\"\n\
             key = \"{0}/missing-key.pem\"\n",
            dir.path().display()
        ),
    )
    .unwrap();
    let broken_tls = broken_tls.display().to_string();
    let config = dir.path().join("mqttd.toml");
    std::fs::write(&config, "[durable]\nenabled = false\n").unwrap();
    let config = config.display().to_string();
    // What `--decommission` signals: a process that exits on SIGUSR1, as the broker does
    // once its drain is done.
    let target = ChildGuard(
        Command::new("sh")
            .arg("-c")
            .arg("trap 'exit 0' USR1; for i in $(seq 1 600); do sleep 0.1; done")
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn the decommission target"),
    );
    let target_pid = target.0.id().to_string();
    let rule_test: &[&str] = &[
        "--rule-test",
        "--sql",
        "SELECT 1 AS x FROM \"t\"",
        "--topic",
        "t",
        "--payload",
        "{}",
    ];
    // (arguments, stdin, the exit status the command has with its output read)
    let commands: [(&[&str], &str, i32); 10] = [
        (&["--check-rules", &path], "", 0),
        (rule_test, "", 0),
        (&["--help"], "", 0),
        (&["--version"], "", 0),
        (&["--check-config", "--config", &config], "", 0),
        (&["--print-config", "--config", &config], "", 0),
        (&["--hash-password", "alice"], "correct horse", 0),
        (&["--check-tls", "--config", &broken_tls], "", 1),
        (&["--admin", "help"], "", 0),
        (
            &["--decommission", "--pid", &target_pid, "--timeout", "30"],
            "",
            0,
        ),
    ];
    for (args, stdin, code) in commands {
        let mut cmd = mqttd();
        cmd.args(args);
        let (got, stderr) = run_with_stdout_closed(cmd, stdin, &format!("{args:?}"));
        assert_eq!(got, Some(code), "mqttd {args:?}, stdout closed: {stderr}");
        assert!(
            !stderr.contains("panicked"),
            "mqttd {args:?} panicked: {stderr}"
        );
    }
    drop(target);
}

/// The same for the modes that talk to a running broker: `--probe` answers from its health
/// endpoint, and `--backup` signals it and waits for the export, each with stdout closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_stdout_leaves_probe_and_backup_their_exit_status() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), GOOD_RULES);
    let backups = dir.path().join("backups");
    std::fs::create_dir_all(&backups).unwrap();
    let setup = Setup {
        env: vec![("MQTTD_BACKUP_DIR", backups.display().to_string())],
        ..Setup::default()
    };
    let broker = start(dir.path(), "closed-stdout", &setup).await;
    let config = dir.path().join("mqttd.toml").display().to_string();
    let (health, pid) = (broker.health.to_string(), broker.child.0.id().to_string());
    let commands: [&[&str]; 2] = [
        &["--probe", "/livez", "--url", &health],
        &[
            "--backup",
            "--pid",
            &pid,
            "--config",
            &config,
            "--timeout",
            "30",
        ],
    ];
    for args in commands {
        let mut cmd = mqttd();
        cmd.args(args).env("MQTTD_BACKUP_DIR", &backups);
        let (got, stderr) = run_with_stdout_closed(cmd, "", &format!("{args:?}"));
        assert_eq!(got, Some(0), "mqttd {args:?}, stdout closed: {stderr}");
        assert!(
            !stderr.contains("panicked"),
            "mqttd {args:?} panicked: {stderr}"
        );
    }
}

/// RULES.md "Operating rules": "With no file it checks the configured `rules.file`" —
/// from `[rules] file` in a `--config` file and from `MQTTD_RULES_FILE`. If the bare
/// command ignored the configuration (a usage error, or checking nothing) this fails.
#[test]
fn check_rules_with_no_file_checks_the_configured_rules_file() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), GOOD_RULES);
    let path = dir.path().join("rules.toml").display().to_string();
    let config = dir.path().join("mqttd.toml");
    std::fs::write(
        &config,
        format!("[durable]\nenabled = false\n\n[rules]\nfile = \"{path}\"\n"),
    )
    .unwrap();
    let ran = cli(&["--check-rules", "--config", &config.display().to_string()]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(ran.stdout, good_rules_listing(&path));
    assert_eq!(ran.stderr, "");

    let mut cmd = mqttd();
    cmd.arg("--check-rules")
        .env("MQTTD_RULES_FILE", &path)
        .env("MQTTD_DURABLE_SESSIONS", "0");
    let ran = run(cmd);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(ran.stdout, good_rules_listing(&path));
    assert_eq!(ran.stderr, "");
}

/// Run `--check-rules` on a file holding `rules` and assert it is refused: exit 1, nothing
/// on stdout, and exactly `rules INVALID (<file>): <message>` on stderr.
fn assert_check_rules_refuses(rules: &str, message: &str) {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), rules);
    let path = dir.path().join("rules.toml").display().to_string();
    let ran = cli(&["--check-rules", &path]);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert_eq!(ran.stdout, "");
    assert_eq!(ran.stderr, format!("rules INVALID ({path}): {message}\n"));
}

/// RULES.md "The rules file": "one rule that does not parse rejects the file", and
/// `--check-rules` exits 1 for it. The message names the rule and the line, column and
/// text where the statement broke. A file accepted with a broken statement, or an error
/// without its position, fails here.
#[test]
fn check_rules_refuses_a_sql_syntax_error_with_its_position() {
    assert_check_rules_refuses(
        "[rules.broken]\nsql = 'SELECT payload.x FROM \"t\" WHERE'\n",
        "rule `broken`: expected an expression (line 1, column 32, near `end of statement`)",
    );
}

/// RULES.md "Functions": "An unknown function or a wrong argument count fails the
/// **load**, not the first message." A file calling a function mqttd lacks would
/// otherwise load and fail on every message.
#[test]
fn check_rules_refuses_an_unknown_function() {
    assert_check_rules_refuses(
        "[rules.fn]\nsql = 'SELECT nosuch(payload) AS x FROM \"t\"'\n",
        "rule `fn`: unknown function nosuch() — see docs/RULES.md for the supported \
         functions (line 1, column 8, near `nosuch(payload) AS x FRO`)",
    );
}

/// RULES.md "Events": message, connack, auth, ping and alarm events "are not raised. A
/// rule that selects one is refused at load." A rule on `message/delivered` that loaded
/// would never fire, silently.
#[test]
fn check_rules_refuses_an_event_mqttd_does_not_raise() {
    assert_check_rules_refuses(
        "[rules.ev]\nsql = 'SELECT * FROM \"$events/message/delivered\"'\n",
        "rule `ev`: \"$events/message/delivered\" is not a supported event (supported: \
         $events/client/connected, $events/client/disconnected, $events/session/subscribed, \
         $events/session/unsubscribed)",
    );
}

/// RULES.md "No sinks": an action that names a data-integration sink, an EMQX bridge id
/// like `"kafka:my_sink"`, "is refused at load". Loaded, it would drop every output.
#[test]
fn check_rules_refuses_a_sink_action() {
    assert_check_rules_refuses(
        "[rules.sink]\nsql = 'SELECT * FROM \"t/#\"'\nactions = [\"kafka:my_sink\"]\n",
        "rule `sink`: action \"kafka:my_sink\" references a data bridge / sink; mqttd has no \
         external sinks — republish to a topic and consume it with a $share group \
         (docs/INTEGRATION.md)",
    );
}

/// RULES.md "Actions": a republish `qos` is "0, 1, 2 or one placeholder". A literal 3 can
/// never be valid, so it fails the load; before the audit fix it passed `--check-rules`
/// and then failed the action on every message.
#[test]
fn check_rules_refuses_a_literal_qos_of_3() {
    assert_check_rules_refuses(
        "[rules.q]\nsql = 'SELECT * FROM \"t/#\"'\n\
         actions = [{ function = \"republish\", args = { topic = \"out\", qos = 3 } }]\n",
        "rule `q`: qos must be 0, 1 or 2, got 3",
    );
}

/// `n` rules, each selecting its own topic, named so that id order is numeric order.
fn n_rules(n: usize) -> String {
    let mut text = String::new();
    for i in 0..n {
        writeln!(text, "[rules.r{i:04}]\nsql = 'SELECT * FROM \"t/{i}\"'")
            .expect("write to a String");
    }
    text
}

/// RULES.md "The rules file": "The limits are 1,024 rules per file". The limit itself
/// loads — every one of the 1,024 rules listed — and one more is refused with the limit
/// named. A limit off by one in either direction fails one of the two halves.
#[test]
fn check_rules_loads_1024_rules_and_refuses_1025() {
    let dir = tempfile::tempdir().unwrap();
    let text = n_rules(1024);
    write_rules(dir.path(), &text);
    let path = dir.path().join("rules.toml").display().to_string();
    let ran = cli(&["--check-rules", &path]);
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    let mut want = format!(
        "rules OK: {path}: 1024 rule(s), 1024 enabled, sha256 {}\n",
        sha256_hex(text.as_bytes())
    );
    for i in 0..1024 {
        writeln!(want, "  r{i:04} (enabled): FROM \"t/{i}\", 0 action(s)")
            .expect("write to a String");
    }
    assert_eq!(ran.stdout, want);
    assert_eq!(ran.stderr, "");

    assert_check_rules_refuses(
        &n_rules(1025),
        "rules file: 1025 rules is more than the 1024 a file may define",
    );
}

/// One rule running `n` console actions.
fn n_actions(n: usize) -> String {
    let list = vec![r#"{ function = "console" }"#; n].join(", ");
    format!("[rules.many]\nsql = 'SELECT * FROM \"t\"'\nactions = [{list}]\n")
}

/// RULES.md "The rules file": "16 actions per rule". Sixteen load; seventeen are refused
/// with the limit named.
#[test]
fn check_rules_loads_16_actions_and_refuses_17() {
    let dir = tempfile::tempdir().unwrap();
    let text = n_actions(16);
    write_rules(dir.path(), &text);
    let path = dir.path().join("rules.toml").display().to_string();
    let ran = cli(&["--check-rules", &path]);
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(
        ran.stdout,
        format!(
            "rules OK: {path}: 1 rule(s), 1 enabled, sha256 {}\n  \
             many (enabled): FROM \"t\", 16 action(s)\n",
            sha256_hex(text.as_bytes())
        )
    );
    assert_eq!(ran.stderr, "");

    assert_check_rules_refuses(
        &n_actions(17),
        "rule `many`: 17 actions is more than the 16 a rule may run",
    );
}

/// A rule whose SQL is exactly `bytes` long: a statement padded with a `--` comment.
fn sql_of_length(bytes: usize) -> String {
    let head = "SELECT * FROM \"t\" WHERE 1 = 1\n--";
    let sql = format!("{head}{}", "x".repeat(bytes - head.len()));
    assert_eq!(sql.len(), bytes);
    format!("[rules.big]\nsql = '''{sql}'''\n")
}

/// RULES.md "The rules file": "64 KiB of SQL per rule". A 65,536-byte statement loads;
/// 65,537 bytes are refused with the limit named.
#[test]
fn check_rules_loads_64_kib_of_sql_and_refuses_more() {
    let dir = tempfile::tempdir().unwrap();
    let text = sql_of_length(64 * 1024);
    write_rules(dir.path(), &text);
    let path = dir.path().join("rules.toml").display().to_string();
    let ran = cli(&["--check-rules", &path]);
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(
        ran.stdout,
        format!(
            "rules OK: {path}: 1 rule(s), 1 enabled, sha256 {}\n  \
             big (enabled): FROM \"t\", 0 action(s)\n",
            sha256_hex(text.as_bytes())
        )
    );
    assert_eq!(ran.stderr, "");

    assert_check_rules_refuses(
        &sql_of_length(64 * 1024 + 1),
        "rule `big`: sql is longer than 65536 bytes",
    );
}

/// RULES.md "Operating rules": `--check-rules` exits "2 usage". Two files is a usage
/// error, reported before any file is read; a checker that exited 1 (invalid) or 0 here
/// would make a CI gate read a typo as a verdict on the rules.
#[test]
fn check_rules_exits_2_on_a_usage_error() {
    let ran = cli(&["--check-rules", "a.toml", "b.toml"]);
    assert_eq!(ran.code, Some(2), "{ran:?}");
    assert_eq!(ran.stdout, "");
    assert_eq!(
        ran.stderr,
        "mqttd: unexpected positional argument: \"b.toml\"\n\
         Try 'mqttd --help' for the list of flags.\n"
    );
}

// ---------------------------------------------------------------------------------------
// --rule-test
// ---------------------------------------------------------------------------------------

/// RULES.md "Operating rules": `mqttd --rule-test --sql '<statement>' [--topic t]
/// [--payload p] [--clientid c] [--qos n]` "prints each output as JSON" — EMQX's SQL
/// test, offline. RULES.md's own example rule gives one JSON object with its selected
/// fields in order; a `FOREACH` gives one line per output; a `WHERE` that does not match
/// says so and still exits 0. Any other output fails here.
#[test]
fn rule_test_prints_each_output_of_a_publish_statement_as_json() {
    let ran = cli(&[
        "--rule-test",
        "--sql",
        "SELECT payload.temp AS temp, clientid, qos FROM \"sensors/+/data\" \
         WHERE payload.temp > 30",
        "--topic",
        "sensors/dev7/data",
        "--payload",
        r#"{"temp":35}"#,
        "--clientid",
        "dev7",
        "--qos",
        "1",
    ]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(
        ran.stdout,
        "{\"temp\":35,\"clientid\":\"dev7\",\"qos\":1}\n"
    );
    assert_eq!(ran.stderr, "");

    let ran = cli(&[
        "--rule-test",
        "--sql",
        "FOREACH payload.list AS e DO e AS v FROM \"t/#\"",
        "--payload",
        r#"{"list":[1,2]}"#,
    ]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(ran.stdout, "{\"v\":1}\n{\"v\":2}\n");
    assert_eq!(ran.stderr, "");

    let ran = cli(&[
        "--rule-test",
        "--sql",
        "SELECT payload.temp AS temp FROM \"sensors/+/data\" WHERE payload.temp > 30",
        "--topic",
        "sensors/dev7/data",
        "--payload",
        r#"{"temp":3}"#,
    ]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(
        ran.stdout,
        "(no output: the statement's WHERE / INCASE did not match this message)\n"
    );
    assert_eq!(ran.stderr, "");
}

/// `--rule-test` on a `$events` statement runs it against a sample of that event, not a
/// fake publish: `event` is `client.connected`, the sample is an MQTT 5 client
/// (`proto_ver` 5) with keepalive 60, and `--clientid` is its client id. Before the audit
/// fix this printed `event: "message.publish"` and every event field as `"undefined"`
/// with exit 0 — plausible and wrong — which fails the exact stdout here.
#[test]
fn rule_test_runs_an_events_statement_against_a_sample_event() {
    let ran = cli(&[
        "--rule-test",
        "--sql",
        "SELECT clientid, event, proto_name, proto_ver, keepalive, clean_start \
         FROM \"$events/client/connected\"",
        "--clientid",
        "dev7",
    ]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(
        ran.stdout,
        "{\"clientid\":\"dev7\",\"event\":\"client.connected\",\"proto_name\":\"MQTT\",\
         \"proto_ver\":5,\"keepalive\":60,\"clean_start\":true}\n"
    );
    assert_eq!(ran.stderr, "(a sample client.connected event)\n");
}

/// RULES.md "Try a statement": "When the statement names more than one event, it runs
/// against the first one named; `--event` picks another (`client.connected`, …, or their
/// topic forms, such as `client/disconnected`)". The statement names
/// `client/disconnected` first, so with no `--event` the sample is a disconnect; each
/// spelling of `--event` — EMQX's event name, the topic form RULES.md documents, and the
/// full `$events/` topic — picks the second event, `client.connected`, which is never
/// the default. A spelling that was ignored, or refused, fails its run.
#[test]
fn rule_test_event_picks_among_several_events() {
    let sql = "SELECT clientid, event FROM \"$events/client/disconnected\", \
               \"$events/client/connected\"";
    for (event, want) in [
        (None, "client.disconnected"),
        (Some("client.connected"), "client.connected"),
        (Some("client/connected"), "client.connected"),
        (Some("$events/client/connected"), "client.connected"),
    ] {
        let mut args = vec!["--rule-test", "--sql", sql, "--clientid", "dev7"];
        if let Some(event) = event {
            args.extend(["--event", event]);
        }
        let ran = cli(&args);
        assert_eq!(ran.code, Some(0), "{event:?}: {ran:?}");
        assert_eq!(
            ran.stdout,
            format!("{{\"clientid\":\"dev7\",\"event\":\"{want}\"}}\n"),
            "{event:?}"
        );
        assert_eq!(
            ran.stderr,
            format!("(a sample {want} event)\n"),
            "{event:?}"
        );
    }
}

/// RULES.md "Try a statement": "A statement that selects topics and events runs against a
/// publish unless `--event` is given." One statement selecting `t/#` and
/// `$events/client/connected`: with `--topic t/1` it is tested against that publish
/// (`event` is `message.publish`, the topic is the one given, nothing names a sample on
/// stderr); with `--event client.connected` against a sample connect, which has no
/// topic (RULES.md "Gotchas": a missing value is the string `"undefined"`). A tool that
/// preferred the event, or ignored `--event`, fails one of the two exact outputs.
#[test]
fn rule_test_runs_a_topics_and_events_statement_against_a_publish_unless_event_is_given() {
    let sql = "SELECT clientid, event, topic FROM \"t/#\", \"$events/client/connected\"";
    let ran = cli(&[
        "--rule-test",
        "--sql",
        sql,
        "--topic",
        "t/1",
        "--clientid",
        "dev7",
    ]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(
        ran.stdout,
        "{\"clientid\":\"dev7\",\"event\":\"message.publish\",\"topic\":\"t/1\"}\n"
    );
    assert_eq!(ran.stderr, "");

    let ran = cli(&[
        "--rule-test",
        "--sql",
        sql,
        "--topic",
        "t/1",
        "--clientid",
        "dev7",
        "--event",
        "client.connected",
    ]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(
        ran.stdout,
        "{\"clientid\":\"dev7\",\"event\":\"client.connected\",\"topic\":\"undefined\"}\n"
    );
    assert_eq!(ran.stderr, "(a sample client.connected event)\n");
}

/// `--rule-test` refuses, with exit 1, an input the rule would never see: a publish to a
/// topic outside the statement's FROM, and an event the statement does not select.
/// Printing an output for either would tell the user a rule fires when it never would.
#[test]
fn rule_test_refuses_an_input_the_rule_would_never_see() {
    let ran = cli(&[
        "--rule-test",
        "--sql",
        "SELECT payload.temp AS t FROM \"s/+/data\"",
        "--topic",
        "other/topic",
        "--payload",
        r#"{"temp":35}"#,
    ]);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert_eq!(ran.stdout, "");
    assert_eq!(
        ran.stderr,
        "rule test FAILED: topic \"other/topic\" matches none of the FROM filters (s/+/data)\n"
    );

    let ran = cli(&[
        "--rule-test",
        "--sql",
        "SELECT clientid FROM \"$events/client/connected\"",
        "--event",
        "session.subscribed",
    ]);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert_eq!(ran.stdout, "");
    assert_eq!(
        ran.stderr,
        "(a sample session.subscribed event)\n\
         rule test FAILED: the statement's FROM does not select the session.subscribed event\n"
    );
}

// ---------------------------------------------------------------------------------------
// Reload
// ---------------------------------------------------------------------------------------

/// One rule: a publish to `rl/in` republishes `A` to `rl/out`.
const SWAP_A: &str = r#"[rules.swap]
sql = 'SELECT payload FROM "rl/in"'
actions = [{ function = "republish", args = { topic = "rl/out", payload = "A" } }]
"#;

/// The same rule republishing `B`, and a second enabled rule.
const SWAP_B: &str = r#"[rules.swap]
sql = 'SELECT payload FROM "rl/in"'
actions = [{ function = "republish", args = { topic = "rl/out", payload = "B" } }]

[rules.tap]
sql = 'SELECT payload FROM "rl/other"'
actions = [{ function = "console" }]
"#;

/// [`SWAP_A`]'s rule with a statement that does not parse.
const SWAP_BROKEN: &str = r#"[rules.swap]
sql = 'SELECT payload FROM "rl/in" WHERE'
actions = [{ function = "republish", args = { topic = "rl/out", payload = "C" } }]
"#;

/// A subscriber on `rl/out` and a publisher that stays connected for the whole test.
async fn swap_clients(broker: &Broker) -> (Client, Client) {
    let mut sub = Client::connect(broker.addr, "swap-sub").await;
    assert_eq!(
        subscribe(&mut sub, 1, "rl/out", QoS::AtMostOnce).await,
        vec![0]
    );
    let publ = Client::connect(broker.addr, "swap-pub").await;
    (sub, publ)
}

/// Publish to `rl/in` and return what the rule republished to `rl/out`.
async fn swap_output(publ: &mut Client, sub: &mut Client) -> Got {
    publ.publish("rl/in", b"x", QoS::AtMostOnce, None, vec![])
        .await;
    next(sub).await
}

/// RULES.md "Operating rules": "Edit the file, then `SIGHUP` … The next publish runs the
/// new rules", and `mqttd_rules_loaded` / `mqttd_rules_info{checksum}` (the file's
/// SHA-256, one series at 1) move with them. The publisher stays connected across the
/// reload, so a broker that only applied new rules to new connections fails the second
/// output; one that kept the old checksum at 1, or reported a digest other than the
/// file's independently computed SHA-256, fails the metrics.
#[tokio::test]
async fn sighup_swaps_the_rules_for_a_client_that_stays_connected() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), SWAP_A);
    let broker = start(dir.path(), "reload-ok", &Setup::default()).await;
    let (mut sub, mut publ) = swap_clients(&broker).await;
    let (sha_a, sha_b) = (sha256_hex(SWAP_A.as_bytes()), sha256_hex(SWAP_B.as_bytes()));
    assert_eq!(broker.sample("mqttd_rules_loaded").await, Some(1));
    assert_eq!(broker.sample(&rules_info(&sha_a)).await, Some(1));
    assert_eq!(
        swap_output(&mut publ, &mut sub).await,
        got("rl/out", "A", QoS::AtMostOnce, false)
    );

    write_rules(dir.path(), SWAP_B);
    broker.signal("HUP");
    broker
        .wait_log(&format!(
            " INFO mqttd::reload: rules reloaded (ADR 0083) rules=2 enabled=2 digest={sha_b}"
        ))
        .await;
    // Logged once the new rules are sent to the connections.
    broker
        .wait_log(
            " INFO mqttd::reload: security policy reloaded: ACL + authenticator (+ TLS, \
             gossip CRL) swapped trigger=\"signal\"",
        )
        .await;
    let metrics = broker.metrics().await;
    assert_eq!(sample(&metrics, "mqttd_rules_loaded"), Some(2), "{metrics}");
    assert_eq!(sample(&metrics, &rules_info(&sha_b)), Some(1), "{metrics}");
    assert_eq!(sample(&metrics, &rules_info(&sha_a)), Some(0), "{metrics}");
    assert_eq!(
        swap_output(&mut publ, &mut sub).await,
        got("rl/out", "B", QoS::AtMostOnce, false)
    );
    sub.expect_silence().await;
}

/// RULES.md "Operating rules": "A file that does not load is rejected with the reload,
/// keeping the running rules." On `SIGHUP` with a broken file the log says the reload was
/// rejected and why; the connected publisher's next publish still runs the old rule; and
/// the metrics still report the old file. A broker that swapped in an empty rule set (no
/// output), the broken one, or dropped the old checksum fails here.
#[tokio::test]
async fn a_rules_file_that_does_not_load_on_sighup_is_rejected_and_the_old_rules_keep_working() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), SWAP_A);
    let broker = start(dir.path(), "reload-bad", &Setup::default()).await;
    let (mut sub, mut publ) = swap_clients(&broker).await;
    let sha_a = sha256_hex(SWAP_A.as_bytes());
    assert_eq!(
        swap_output(&mut publ, &mut sub).await,
        got("rl/out", "A", QoS::AtMostOnce, false)
    );

    write_rules(dir.path(), SWAP_BROKEN);
    broker.signal("HUP");
    broker
        .wait_log(
            " WARN mqttd::reload: security reload REJECTED — keeping the running policy \
             trigger=\"signal\" error=rules: rule `swap`: expected an expression (line 1, \
             column 34, near `end of statement`)",
        )
        .await;
    assert_eq!(
        swap_output(&mut publ, &mut sub).await,
        got("rl/out", "A", QoS::AtMostOnce, false)
    );
    let metrics = broker.metrics().await;
    assert_eq!(sample(&metrics, "mqttd_rules_loaded"), Some(1), "{metrics}");
    assert_eq!(
        series_lines(&metrics, "mqttd_rules_info{"),
        vec![format!("{} 1", rules_info(&sha_a))],
    );
    assert_eq!(
        broker.log_lines(|l| l.contains("rules reloaded")),
        Vec::<String>::new()
    );
    sub.expect_silence().await;
}

/// RULES.md "Operating rules": "with `MQTTD_CONFIG_WATCH` set, the file watcher picks the
/// edit up on its own." The rules file is replaced the way a `ConfigMap` update replaces
/// it and no signal is sent: the metrics move to the new file, the log names the watcher
/// as the trigger, and the connected publisher's next publish runs the new rule. A rules
/// file left out of the watch scope never moves `mqttd_rules_info` and fails the wait.
#[tokio::test]
async fn with_config_watch_an_edited_rules_file_is_picked_up_without_a_signal() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), SWAP_A);
    let setup = Setup {
        env: vec![("MQTTD_CONFIG_WATCH", "1".to_string())],
        ..Setup::default()
    };
    let broker = start(dir.path(), "reload-watch", &setup).await;
    // The watcher starts after the listeners bind and takes its baseline of the files on
    // its first run, so the edit waits for it to be enabled (two files: the config and
    // the rules file it names) and for the round trip below.
    broker
        .wait_log(
            " INFO mqttd: config-file watcher enabled (ADR 0033): auto-reload on change \
             interval_secs=1 files=2",
        )
        .await;
    let (mut sub, mut publ) = swap_clients(&broker).await;
    let (sha_a, sha_b) = (sha256_hex(SWAP_A.as_bytes()), sha256_hex(SWAP_B.as_bytes()));
    assert_eq!(
        swap_output(&mut publ, &mut sub).await,
        got("rl/out", "A", QoS::AtMostOnce, false)
    );

    replace_rules(dir.path(), SWAP_B);
    broker.wait_metric(&rules_info(&sha_b), 1).await;
    broker
        .wait_log(
            " INFO mqttd::reload: security policy reloaded: ACL + authenticator (+ TLS, \
             gossip CRL) swapped trigger=\"watch\"",
        )
        .await;
    assert_eq!(broker.sample("mqttd_rules_loaded").await, Some(2));
    assert_eq!(broker.sample(&rules_info(&sha_a)).await, Some(0));
    assert_eq!(
        swap_output(&mut publ, &mut sub).await,
        got("rl/out", "B", QoS::AtMostOnce, false)
    );
    assert_eq!(
        broker.log_lines(|l| l.contains("SIGHUP received")),
        Vec::<String>::new()
    );
}

// ---------------------------------------------------------------------------------------
// A rule's output is an ordinary publish
// ---------------------------------------------------------------------------------------

/// RULES.md "Where rules run": what a rule republishes is "retained … like a message a
/// client sent", and `retain` is "a boolean or one placeholder". A literal `retain = true`
/// republish of a non-retained `QoS` 1 publish is stored as the topic's retained message:
/// a v5 and a v3.1.1 subscriber that arrive after the PUBACK both receive it with RETAIN
/// set, and the original (not retained) is not. A derived message routed live but not
/// stored reaches neither late subscriber.
#[tokio::test]
async fn a_retained_derived_message_reaches_a_late_subscriber() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.keep]
sql = 'SELECT payload FROM "ret/in"'
actions = [{ function = "republish", args = { topic = "ret/out", payload = "${payload}", qos = 1, retain = true } }]
"#,
    );
    let broker = start(dir.path(), "retain", &Setup::default()).await;
    let mut publ = Client::connect(broker.addr, "ret-pub").await;
    assert_eq!(publish_acked(&mut publ, "ret/in", b"v1", 1).await, 0);

    let mut late5 = Client::connect_v5_ok(broker.addr, "late-v5").await;
    assert_eq!(
        subscribe(&mut late5, 1, "ret/out", QoS::AtLeastOnce).await,
        vec![1]
    );
    assert_eq!(
        next(&mut late5).await,
        got("ret/out", "v1", QoS::AtLeastOnce, true)
    );
    let mut late4 = Client::connect(broker.addr, "late-v311").await;
    assert_eq!(
        subscribe(&mut late4, 1, "ret/#", QoS::AtLeastOnce).await,
        vec![1]
    );
    assert_eq!(
        next(&mut late4).await,
        got("ret/out", "v1", QoS::AtLeastOnce, true)
    );
    late4.expect_silence().await;
}

/// RULES.md "Where rules run": a republished message is "queued for offline persistent
/// sessions … like a message a client sent". A v5 (Session Expiry 3600) and a v3.1.1
/// (clean session off) subscriber go offline; a `QoS` 1 publish is acknowledged; each
/// reconnects to its stored session and receives the `QoS` 1 derived message, exactly
/// once. A derived message routed only to connected subscribers never arrives.
#[tokio::test]
async fn a_qos1_derived_message_is_queued_for_an_offline_persistent_session() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.fwd]
sql = 'SELECT payload FROM "pers/in"'
actions = [{ function = "republish", args = { topic = "pers/out", payload = "derived:${payload}", qos = 1 } }]
"#,
    );
    let broker = start(dir.path(), "offline", &Setup::default()).await;
    let (mut sleeper5, _) = connect_persistent_v5(broker.addr, "sleeper-v5", true).await;
    assert_eq!(
        subscribe(&mut sleeper5, 1, "pers/out", QoS::AtLeastOnce).await,
        vec![1]
    );
    sleeper5.disconnect().await;
    let (mut sleeper4, _) = Client::connect_v311(broker.addr, "sleeper-v311", false).await;
    assert_eq!(
        subscribe(&mut sleeper4, 1, "pers/out", QoS::AtLeastOnce).await,
        vec![1]
    );
    sleeper4.disconnect().await;

    let mut publ = Client::connect(broker.addr, "pers-pub").await;
    assert_eq!(publish_acked(&mut publ, "pers/in", b"p1", 1).await, 0);

    let want = got("pers/out", "derived:p1", QoS::AtLeastOnce, false);
    let (mut sleeper5, present) = connect_persistent_v5(broker.addr, "sleeper-v5", false).await;
    assert!(present, "the v5 session was stored");
    assert_eq!(next(&mut sleeper5).await, want);
    sleeper5.expect_silence().await;
    let (mut sleeper4, present) = Client::connect_v311(broker.addr, "sleeper-v311", false).await;
    assert!(present, "the v3.1.1 session was stored");
    assert_eq!(next(&mut sleeper4).await, want);
    sleeper4.expect_silence().await;
}

/// RULES.md "Delivery guarantees": for a `QoS` 1 publish "the PUBACK waits for all of
/// them: when it is released, each derived message is stored wherever it was owed
/// (durably where durability applies)". On a persistent single node an offline session's
/// queue is on disk, so the broker is `SIGKILL`ed the moment the last of twenty pipelined
/// `QoS` 1 publishes is acknowledged — nothing flushed, nothing drained — and restarted
/// on the same data directory: the offline subscriber's session holds all twenty derived
/// messages, in order. A PUBACK released before its derived message's store commit loses
/// that message whenever the kill lands first, most likely for the last publishes, and
/// fails the exact list. (A race can only be caught when it is lost, so this pins the
/// ordering as strongly as a black-box test can.)
#[tokio::test]
async fn a_qos1_puback_is_released_only_once_each_derived_message_is_stored() {
    const N: u16 = 20;
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.fwd]
sql = 'SELECT payload FROM "crash/in"'
actions = [{ function = "republish", args = { topic = "crash/out", payload = "derived:${payload}", qos = 1 } }]
"#,
    );
    let mut broker = start(dir.path(), "crash", &Setup::default()).await;
    let (mut sleeper, _) = connect_persistent_v5(broker.addr, "sleeper", true).await;
    assert_eq!(
        subscribe(&mut sleeper, 1, "crash/out", QoS::AtLeastOnce).await,
        vec![1]
    );
    sleeper.disconnect().await;

    let mut publ = Client::connect(broker.addr, "crash-pub").await;
    for i in 1..=N {
        publ.publish(
            "crash/in",
            i.to_string().as_bytes(),
            QoS::AtLeastOnce,
            Some(i),
            vec![],
        )
        .await;
    }
    for i in 1..=N {
        match publ.recv().await {
            Packet::PubAck(ack) => assert_eq!((ack.pkid, ack.reason), (i, 0), "PUBACK"),
            other => panic!("expected PUBACK {i}, got {other:?}"),
        }
    }
    broker.crash();
    drop(publ);

    let broker = start(dir.path(), "crash", &Setup::default()).await;
    let (mut sleeper, present) = connect_persistent_v5(broker.addr, "sleeper", false).await;
    assert!(present, "the session survived the crash");
    let want: Vec<Got> = (1..=N)
        .map(|i| {
            got(
                "crash/out",
                &format!("derived:{i}"),
                QoS::AtLeastOnce,
                false,
            )
        })
        .collect();
    assert_eq!(collect(&mut sleeper, usize::from(N)).await, want);
    sleeper.expect_silence().await;
}

/// RULES.md "Where rules run": a republished message is "shared-subscription balanced …
/// like a message a client sent". A `$share` group with a v5 and a v3.1.1 member receives
/// 20 derived `QoS` 1 messages: each exactly once across the group, ten per member (the
/// group's round-robin, ADR 0015). A derived message that bypassed the group (each
/// member getting all 20) or skipped balancing fails the split.
#[tokio::test]
async fn a_shared_group_receives_derived_messages_balanced_across_members() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.fan]
sql = 'SELECT payload, qos FROM "shr/in"'
actions = [{ function = "republish", args = { topic = "shr/out" } }]
"#,
    );
    let broker = start(dir.path(), "share", &Setup::default()).await;
    let mut m5 = Client::connect_v5_ok(broker.addr, "member-v5").await;
    assert_eq!(
        subscribe(&mut m5, 1, "$share/g/shr/out", QoS::AtLeastOnce).await,
        vec![1]
    );
    let mut m4 = Client::connect(broker.addr, "member-v311").await;
    assert_eq!(
        subscribe(&mut m4, 1, "$share/g/shr/out", QoS::AtLeastOnce).await,
        vec![1]
    );
    let mut publ = Client::connect(broker.addr, "shr-pub").await;
    for i in 0..20u16 {
        assert_eq!(
            publish_acked(&mut publ, "shr/in", i.to_string().as_bytes(), i + 1).await,
            0
        );
    }
    // Every derived message was routed before its PUBACK; ten each is the claim, and
    // silence after them shows no member got more.
    let (from5, from4) = (collect(&mut m5, 10).await, collect(&mut m4, 10).await);
    m5.expect_silence().await;
    m4.expect_silence().await;
    let mut seen = Vec::new();
    for (topic, payload, qos, retain) in from5.iter().chain(&from4) {
        assert_eq!(
            (topic.as_str(), *qos, *retain),
            ("shr/out", QoS::AtLeastOnce, false)
        );
        seen.push(payload.parse::<u16>().expect("a numeric payload"));
    }
    seen.sort_unstable();
    assert_eq!(seen, (0..20).collect::<Vec<u16>>(), "each exactly once");
    assert_eq!(
        (from5.len(), from4.len()),
        (10, 10),
        "balanced: {from5:?} / {from4:?}"
    );
}

/// RULES.md "Delivery guarantees": "A derived message is published as no client: MQTT 5
/// No Local does not suppress it for the original publisher." The publisher, subscribed
/// with No Local, receives the derived message and not its own original — which a plain
/// subscriber receives first, so the original was routed. A derived message attributed to
/// the publisher would be suppressed too, and the publisher's first packet would never
/// come.
#[tokio::test]
async fn no_local_does_not_suppress_a_derived_message_for_its_publisher() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.echo]
sql = 'SELECT payload FROM "nl/in"'
actions = [{ function = "republish", args = { topic = "nl/out", payload = "derived:${payload}" } }]
"#,
    );
    let broker = start(dir.path(), "nolocal", &Setup::default()).await;
    let mut plain = Client::connect(broker.addr, "nl-plain").await;
    assert_eq!(
        subscribe(&mut plain, 1, "nl/#", QoS::AtMostOnce).await,
        vec![0]
    );
    let mut publ = Client::connect_v5_ok(broker.addr, "nl-pub").await;
    let no_local = SubscriptionOptions {
        no_local: true,
        ..SubscriptionOptions::default()
    };
    assert_eq!(
        subscribe_all(&mut publ, 1, &[("nl/#", QoS::AtMostOnce, no_local)]).await,
        vec![0]
    );

    publ.publish("nl/in", b"hello", QoS::AtMostOnce, None, vec![])
        .await;
    assert_eq!(
        next(&mut plain).await,
        got("nl/in", "hello", QoS::AtMostOnce, false)
    );
    assert_eq!(
        next(&mut plain).await,
        got("nl/out", "derived:hello", QoS::AtMostOnce, false)
    );
    assert_eq!(
        next(&mut publ).await,
        got("nl/out", "derived:hello", QoS::AtMostOnce, false)
    );
    publ.expect_silence().await;
}

// ---------------------------------------------------------------------------------------
// ACL
// ---------------------------------------------------------------------------------------

/// Everyone may publish to `sensors/#` only, and subscribe to `alerts/#`, `sensors/#` and
/// `watch/#`.
const ACL: &str = r#"[[rules]]
actions = ["publish"]
topics = ["sensors/#"]

[[rules]]
actions = ["subscribe"]
topics = ["alerts/#", "sensors/#", "watch/#"]
"#;

/// `r_alert` republishes every `sensors/#` publish to `alerts/<clientid>`; `r_blocked`
/// would republish a `blocked/#` publish to `watch/blocked`.
const ACL_RULES: &str = r#"[rules.r_alert]
sql = 'SELECT payload, clientid, qos FROM "sensors/#"'
actions = [{ function = "republish", args = { topic = "alerts/${clientid}" } }]

[rules.r_blocked]
sql = 'SELECT payload, qos FROM "blocked/#"'
actions = [{ function = "republish", args = { topic = "watch/blocked" } }]
"#;

/// A broker running [`ACL_RULES`] under [`ACL`] (`MQTTD_ACL_FILE`).
async fn start_with_acl(dir: &Path, node: &str) -> Broker {
    write_rules(dir, ACL_RULES);
    std::fs::write(dir.join("acl.toml"), ACL).unwrap();
    let setup = Setup {
        env: vec![("MQTTD_ACL_FILE", dir.join("acl.toml").display().to_string())],
        ..Setup::default()
    };
    start(dir, node, &setup).await
}

/// RULES.md "Delivery guarantees" (Authorization): "The ACL decides whether the original
/// publish is accepted; rules run only on accepted publishes." A publish the ACL denies
/// (v5 PUBACK 0x87) runs no rule: its rule's evaluation counter never appears, and the
/// message it would have derived never reaches a subscriber allowed to read it. If rules
/// ran before the ACL, `watch/blocked` would arrive ahead of the allowed publish that
/// follows on the same connection, and `r_blocked` would be counted.
#[tokio::test]
async fn an_acl_denied_publish_runs_no_rules() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_with_acl(dir.path(), "acl-denied").await;
    let mut sub = Client::connect(broker.addr, "acl-sub").await;
    assert_eq!(
        subscribe(&mut sub, 1, "watch/#", QoS::AtLeastOnce).await,
        vec![1]
    );
    assert_eq!(
        subscribe(&mut sub, 2, "sensors/#", QoS::AtLeastOnce).await,
        vec![1]
    );
    let mut publ = Client::connect_v5_ok(broker.addr, "devA").await;
    assert_eq!(
        publish_acked(&mut publ, "blocked/x", b"nope", 1).await,
        0x87
    );
    assert_eq!(
        publish_acked(&mut publ, "sensors/devA", b"reading", 2).await,
        0
    );

    assert_eq!(
        next(&mut sub).await,
        got("sensors/devA", "reading", QoS::AtLeastOnce, false)
    );
    broker
        .wait_metric(&evaluations("r_alert", "passed"), 1)
        .await;
    let metrics = broker.metrics().await;
    assert_eq!(
        metrics
            .lines()
            .filter(|l| l.contains("rule=\"r_blocked\""))
            .collect::<Vec<_>>(),
        Vec::<&str>::new(),
        "a denied publish was evaluated"
    );
    sub.expect_silence().await;
}

/// RULES.md "Delivery guarantees" (Authorization): "What a rule republishes is not checked
/// against the publisher's ACL … a rule can deliberately publish into topics the
/// publisher cannot." The publisher's own publish to `alerts/devA` is denied (0x87), yet
/// its allowed `sensors/devA` publish is republished there and delivered. A republish
/// checked against the publisher's rights would be dropped and fail the delivery.
#[tokio::test]
async fn a_republish_is_not_checked_against_the_publishers_acl() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_with_acl(dir.path(), "acl-republish").await;
    let mut sub = Client::connect(broker.addr, "acl-sub").await;
    assert_eq!(
        subscribe(&mut sub, 1, "alerts/#", QoS::AtLeastOnce).await,
        vec![1]
    );
    let mut publ = Client::connect_v5_ok(broker.addr, "devA").await;
    assert_eq!(
        publish_acked(&mut publ, "alerts/devA", b"forged", 1).await,
        0x87
    );
    assert_eq!(
        publish_acked(&mut publ, "sensors/devA", b"reading", 2).await,
        0
    );

    assert_eq!(
        next(&mut sub).await,
        got("alerts/devA", "reading", QoS::AtLeastOnce, false)
    );
    sub.expect_silence().await;
    broker.wait_metric(&actions("r_alert", "ok"), 1).await;
}

// ---------------------------------------------------------------------------------------
// Failing rules and actions
// ---------------------------------------------------------------------------------------

/// Two rules that fail on every message they see — a payload that is not JSON read with
/// `payload.x`, and a division by zero — each with a console action.
const BROKEN_RULES: &str = r#"[rules.first_broken]
sql = 'SELECT payload.x AS x FROM "wl/a"'
actions = [{ function = "console" }]

[rules.second_broken]
sql = 'SELECT 1 div 0 AS y FROM "wl/b"'
actions = [{ function = "console" }]

[rules.zz_marker]
sql = 'SELECT payload FROM "wl/marker"'
actions = [{ function = "console" }]
"#;

/// The WARN `first_broken`'s failure logs, after the timestamp.
const FIRST_BROKEN_WARN: &str = " WARN mqttd::rules: rule SQL failed (counted in \
    mqttd_rule_evaluations_total{result=\"failed\"}; this rule's further failures within 10s \
    are logged at debug) rule=first_broken error=payload is not JSON, so payload.<field> is \
    unreadable (invalid JSON: expected ident at line 1 column 2)";

/// The WARN `second_broken`'s failure logs, after the timestamp.
const SECOND_BROKEN_WARN: &str = " WARN mqttd::rules: rule SQL failed (counted in \
    mqttd_rule_evaluations_total{result=\"failed\"}; this rule's further failures within 10s \
    are logged at debug) rule=second_broken error=division by zero";

/// Publish to `wl/marker` and wait for `zz_marker`'s console line: everything the same
/// connection published before it has been evaluated, and logged, by then.
async fn mark_the_log(broker: &Broker, publ: &mut Client, pkid: u16) {
    assert_eq!(publish_acked(publ, "wl/marker", b"end", pkid).await, 0);
    broker
        .wait_log(
            " INFO mqttd::rules: rule console action rule=zz_marker output={\"payload\":\"end\"}",
        )
        .await;
}

/// RULES.md: "A rule never changes or suppresses the message that triggered it", and past
/// an error "the rule fails … and is counted; the message itself is still routed". Ten
/// publishes to each of two failing rules: every one is acknowledged with reason 0, every
/// original reaches the subscriber in order, and
/// `mqttd_rule_evaluations_total{result="failed"}` — the series behind RULES.md's alert —
/// counts each failure. A failing rule that dropped, reordered or refused its message, or
/// went uncounted, fails here.
#[tokio::test]
async fn a_failing_rule_never_affects_the_original_and_is_counted_failed() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), BROKEN_RULES);
    let broker = start(dir.path(), "failing", &Setup::default()).await;
    let mut sub = Client::connect_v5_ok(broker.addr, "wl-sub").await;
    assert_eq!(
        subscribe(&mut sub, 1, "wl/+", QoS::AtLeastOnce).await,
        vec![1]
    );
    let mut publ = Client::connect_v5_ok(broker.addr, "wl-pub").await;
    let mut want = Vec::new();
    for i in 0..10u16 {
        assert_eq!(
            publish_acked(&mut publ, "wl/a", b"not json", 2 * i + 1).await,
            0
        );
        assert_eq!(publish_acked(&mut publ, "wl/b", b"{}", 2 * i + 2).await, 0);
        want.push(got("wl/a", "not json", QoS::AtLeastOnce, false));
        want.push(got("wl/b", "{}", QoS::AtLeastOnce, false));
    }
    let mut delivered = Vec::new();
    for _ in 0..20 {
        delivered.push(next(&mut sub).await);
    }
    assert_eq!(delivered, want);
    sub.expect_silence().await;
    broker
        .wait_metric(&evaluations("first_broken", "failed"), 10)
        .await;
    broker
        .wait_metric(&evaluations("second_broken", "failed"), 10)
        .await;
    // Nothing else was counted: no `passed`, no `no_result`. (The registry renders a
    // family's series in no fixed order.)
    let metrics = broker.metrics().await;
    let mut counted = series_lines(&metrics, "mqttd_rule_evaluations_total{");
    counted.sort_unstable();
    assert_eq!(
        counted,
        vec![
            format!("{} 10", evaluations("first_broken", "failed")),
            format!("{} 10", evaluations("second_broken", "failed")),
        ]
    );
}

/// RULES.md "Operating rules": "A rule failing on every message logs one WARN per 10 s
/// with the error". The limit is per rule: two different rules failing on every message
/// — twenty times each, well inside 10 s — log exactly one WARN each, each naming its
/// rule and its error. The process-wide limiter the audit found logged only the first
/// rule; a missing limiter logs forty lines. Either fails the exact list.
#[tokio::test]
async fn two_rules_failing_on_every_message_each_log_one_warn_per_ten_seconds() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), BROKEN_RULES);
    let broker = start(dir.path(), "warnings", &Setup::default()).await;
    let mut publ = Client::connect(broker.addr, "wl-pub").await;
    let started = Instant::now();
    for i in 0..20u16 {
        assert_eq!(
            publish_acked(&mut publ, "wl/a", b"not json", 2 * i + 1).await,
            0
        );
        assert_eq!(publish_acked(&mut publ, "wl/b", b"{}", 2 * i + 2).await, 0);
    }
    mark_the_log(&broker, &mut publ, 100).await;
    assert!(
        started.elapsed() < Duration::from_secs(9),
        "the failures took {:?}, so they no longer sit inside one 10 s window",
        started.elapsed()
    );
    let warns: Vec<String> = broker
        .log_lines(|l| l.contains(" WARN mqttd::rules:"))
        .into_iter()
        .map(|l| {
            let at = l.find(" WARN").expect("a WARN line");
            l[at..].to_string()
        })
        .collect();
    assert_eq!(warns, vec![FIRST_BROKEN_WARN, SECOND_BROKEN_WARN]);
    assert_eq!(
        broker.sample(&evaluations("first_broken", "failed")).await,
        Some(20)
    );
    assert_eq!(
        broker.sample(&evaluations("second_broken", "failed")).await,
        Some(20)
    );
}

/// RULES.md "Actions": a rendered topic with a wildcard "fails the action, not the rule";
/// "Operating rules": an action is counted `failed` "when it could not render", and a
/// failing rule logs one WARN per 10 s. Five publishes whose rendered topic is
/// `wl/out/+`: the SQL passes five times, the action fails five times
/// (`mqttd_rule_actions_total{result="failed"}`), nothing is counted `ok`, every original
/// is delivered, and exactly one WARN names the rule and the bad topic. Before the audit
/// fix action failures were logged only at DEBUG, invisible to an operator.
#[tokio::test]
async fn a_failing_action_is_counted_failed_and_warned() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.bad_topic]
sql = 'SELECT payload.room AS room FROM "wl/c"'
actions = [{ function = "republish", args = { topic = "wl/out/${room}" } }]

[rules.zz_marker]
sql = 'SELECT payload FROM "wl/marker"'
actions = [{ function = "console" }]
"#,
    );
    let broker = start(dir.path(), "bad-action", &Setup::default()).await;
    let mut sub = Client::connect(broker.addr, "wl-sub").await;
    assert_eq!(
        subscribe(&mut sub, 1, "wl/#", QoS::AtLeastOnce).await,
        vec![1]
    );
    let mut publ = Client::connect(broker.addr, "wl-pub").await;
    for i in 0..5u16 {
        assert_eq!(
            publish_acked(&mut publ, "wl/c", br#"{"room":"+"}"#, i + 1).await,
            0
        );
    }
    mark_the_log(&broker, &mut publ, 100).await;
    let mut delivered = Vec::new();
    for _ in 0..6 {
        delivered.push(next(&mut sub).await);
    }
    let mut want = vec![got("wl/c", r#"{"room":"+"}"#, QoS::AtLeastOnce, false); 5];
    want.push(got("wl/marker", "end", QoS::AtLeastOnce, false));
    assert_eq!(delivered, want);
    sub.expect_silence().await;

    let metrics = broker.metrics().await;
    assert_eq!(
        series_lines(&metrics, "mqttd_rule_actions_total{rule=\"bad_topic\""),
        vec![format!("{} 5", actions("bad_topic", "failed"))]
    );
    assert_eq!(
        series_lines(&metrics, "mqttd_rule_evaluations_total{rule=\"bad_topic\""),
        vec![format!("{} 5", evaluations("bad_topic", "passed"))]
    );
    let warns: Vec<String> = broker
        .log_lines(|l| l.contains(" WARN mqttd::rules:"))
        .into_iter()
        .map(|l| {
            let at = l.find(" WARN").expect("a WARN line");
            l[at..].to_string()
        })
        .collect();
    assert_eq!(
        warns,
        vec![
            " WARN mqttd::rules: rule action failed (counted in \
             mqttd_rule_actions_total{result=\"failed\"}; this rule's further failures \
             within 10s are logged at debug) rule=bad_topic error=rendered topic \
             \"wl/out/+\" is not a valid topic name (empty, too long, or containing + # \
             or NUL)"
        ]
    );
}

// ---------------------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------------------

/// RULES.md "Events": `$events/client/connected` carries `proto_name`, `proto_ver`,
/// `keepalive`, `clean_start` and `expiry_interval` from the CONNECT. A v5 client
/// (keepalive 45, clean start, Session Expiry 120) and two v3.1.1 clients (keepalive 30
/// with clean session off — a session that never expires, `u32::MAX` — and keepalive 20
/// with it on) each produce one event whose fields are exactly the CONNECT's. A field
/// read from the wrong place, or defaulted, fails the exact payload.
#[tokio::test]
async fn client_connected_carries_the_connects_fields_for_v5_and_v311() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.conn]
sql = '''
SELECT clientid, proto_name, proto_ver, keepalive, clean_start, expiry_interval
FROM "$events/client/connected"
WHERE clientid <> 'watcher'
'''
actions = [{ function = "republish", args = { topic = "ev/conn/${clientid}", payload = "${.}" } }]
"#,
    );
    let setup = Setup {
        rules_by_env: true,
        ..Setup::default()
    };
    let broker = start(dir.path(), "connected", &setup).await;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    assert_eq!(
        subscribe(&mut watcher, 1, "ev/#", QoS::AtMostOnce).await,
        vec![0]
    );

    let mut held = Vec::new();
    for (version, id, clean, keepalive, props, want) in [
        (
            V5,
            "ev5",
            true,
            45,
            vec![Property::SessionExpiryInterval(120)],
            r#"{"clientid":"ev5","proto_name":"MQTT","proto_ver":5,"keepalive":45,"clean_start":true,"expiry_interval":120}"#,
        ),
        (
            V4,
            "ev4-persistent",
            false,
            30,
            vec![],
            r#"{"clientid":"ev4-persistent","proto_name":"MQTT","proto_ver":4,"keepalive":30,"clean_start":false,"expiry_interval":4294967295}"#,
        ),
        (
            V4,
            "ev4-clean",
            true,
            20,
            vec![],
            r#"{"clientid":"ev4-clean","proto_name":"MQTT","proto_ver":4,"keepalive":20,"clean_start":true,"expiry_interval":0}"#,
        ),
    ] {
        let (client, ack) = connect_as(broker.addr, version, id, clean, keepalive, props).await;
        assert_eq!(ack.code, 0, "{id}: CONNACK");
        held.push(client);
        assert_eq!(
            next(&mut watcher).await,
            got(&format!("ev/conn/{id}"), want, QoS::AtMostOnce, false)
        );
    }
    watcher.expect_silence().await;
}

/// RULES.md "Events": `$events/session/subscribed` fires "one event per filter the SUBACK
/// granted". One v5 SUBSCRIBE carries three filters, the ACL denies the middle one: the
/// SUBACK grants two, and two events arrive, for the granted filters with their granted
/// `QoS`. Then one more SUBSCRIBE (`data/marker`) from the same client: one connection's
/// events reach the hub in order, so the next event is the marker's — no third event for
/// the first packet came in between. An event for the refused filter, or one event per
/// packet, fails here.
#[tokio::test]
async fn session_subscribed_fires_once_per_granted_filter() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.subs]
sql = '''
SELECT clientid, topic, qos FROM "$events/session/subscribed" WHERE clientid = 'subber'
'''
actions = [{ function = "republish", args = { topic = "evs/sub/${clientid}", payload = "${topic} ${qos}" } }]
"#,
    );
    std::fs::write(
        dir.path().join("acl.toml"),
        "[[rules]]\nactions = [\"subscribe\"]\ntopics = [\"data/#\", \"evs/#\"]\n",
    )
    .unwrap();
    let setup = Setup {
        env: vec![(
            "MQTTD_ACL_FILE",
            dir.path().join("acl.toml").display().to_string(),
        )],
        ..Setup::default()
    };
    let broker = start(dir.path(), "subscribed", &setup).await;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    assert_eq!(
        subscribe(&mut watcher, 1, "evs/#", QoS::AtMostOnce).await,
        vec![0]
    );
    let mut subber = Client::connect_v5_ok(broker.addr, "subber").await;
    let plain = SubscriptionOptions::default();
    assert_eq!(
        subscribe_all(
            &mut subber,
            1,
            &[
                ("data/a", QoS::AtLeastOnce, plain),
                ("secret/x", QoS::AtMostOnce, plain),
                ("data/b", QoS::AtMostOnce, plain),
            ],
        )
        .await,
        vec![1, 0x80, 0],
        "the ACL refuses secret/x (a refused filter's slot is 0x80)"
    );
    assert_eq!(
        collect(&mut watcher, 2).await,
        vec![
            got("evs/sub/subber", "data/a 1", QoS::AtMostOnce, false),
            got("evs/sub/subber", "data/b 0", QoS::AtMostOnce, false),
        ]
    );
    assert_eq!(
        subscribe(&mut subber, 2, "data/marker", QoS::AtMostOnce).await,
        vec![0]
    );
    assert_eq!(
        next(&mut watcher).await,
        got("evs/sub/subber", "data/marker 0", QoS::AtMostOnce, false)
    );
    watcher.expect_silence().await;
}

/// RULES.md "Events": `$events/session/unsubscribed` fires "one event per filter actually
/// removed". An UNSUBSCRIBE of a held filter and a never-subscribed one is answered
/// 0x00 / 0x11, and one event arrives, for the held filter. Then the same client
/// unsubscribes from `data/marker`, which it holds: one connection's events reach the
/// hub in order, so the next event is the marker's — no event for the never-subscribed
/// filter came in between. An event per requested filter fails here.
#[tokio::test]
async fn session_unsubscribed_fires_once_per_filter_actually_removed() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.unsubs]
sql = '''
SELECT clientid, topic FROM "$events/session/unsubscribed" WHERE clientid = 'subber'
'''
actions = [{ function = "republish", args = { topic = "evs/unsub/${clientid}", payload = "${topic}" } }]
"#,
    );
    let broker = start(dir.path(), "unsubscribed", &Setup::default()).await;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    assert_eq!(
        subscribe(&mut watcher, 1, "evs/#", QoS::AtMostOnce).await,
        vec![0]
    );
    let mut subber = Client::connect_v5_ok(broker.addr, "subber").await;
    let plain = SubscriptionOptions::default();
    assert_eq!(
        subscribe_all(
            &mut subber,
            1,
            &[
                ("data/a", QoS::AtMostOnce, plain),
                ("data/marker", QoS::AtMostOnce, plain),
            ],
        )
        .await,
        vec![0, 0]
    );
    assert_eq!(
        unsubscribe(&mut subber, 2, &["data/a", "never/subscribed"]).await,
        vec![0x00, 0x11]
    );
    assert_eq!(
        collect(&mut watcher, 1).await,
        vec![got("evs/unsub/subber", "data/a", QoS::AtMostOnce, false)]
    );
    assert_eq!(
        unsubscribe(&mut subber, 3, &["data/marker"]).await,
        vec![0x00]
    );
    assert_eq!(
        next(&mut watcher).await,
        got("evs/unsub/subber", "data/marker", QoS::AtMostOnce, false)
    );
    watcher.expect_silence().await;
}

/// RULES.md "Events": `$events/client/disconnected`'s `reason` is `normal` for a
/// DISCONNECT with reason 0x00, `tcp_closed` when the socket closes, and EMQX's name for
/// any other reason code a v5 client's DISCONNECT carries (`unspecified_error` for 0x80,
/// `disconnect_with_will_message` for 0x04). A reason mapped wrongly — every v5 code read
/// as `normal`, or a dropped socket reported as a DISCONNECT — fails the exact payloads.
/// RULES.md "Delivery guarantees": each action an event derives "is counted `ok` once
/// the broker has routed its message" — four disconnects, four `ok` actions and nothing
/// else. An event action counted `failed`,
/// twice, or not at all fails the exact series.
#[tokio::test]
async fn client_disconnected_reports_normal_tcp_closed_and_a_v5_reason_by_its_emqx_name() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.gone]
sql = 'SELECT clientid, reason FROM "$events/client/disconnected"'
actions = [{ function = "republish", args = { topic = "ev/gone/${clientid}", payload = "${reason}" } }]
"#,
    );
    let broker = start(dir.path(), "disconnected", &Setup::default()).await;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    assert_eq!(
        subscribe(&mut watcher, 1, "ev/gone/#", QoS::AtMostOnce).await,
        vec![0]
    );

    let mut bye = Client::connect(broker.addr, "bye").await;
    bye.disconnect().await;
    assert_eq!(
        next(&mut watcher).await,
        got("ev/gone/bye", "normal", QoS::AtMostOnce, false)
    );

    drop(Client::connect(broker.addr, "dropped").await);
    assert_eq!(
        next(&mut watcher).await,
        got("ev/gone/dropped", "tcp_closed", QoS::AtMostOnce, false)
    );

    for (id, code, name) in [
        ("failing", 0x80, "unspecified_error"),
        ("leaving", 0x04, "disconnect_with_will_message"),
    ] {
        let mut c = Client::connect_v5_ok(broker.addr, id).await;
        c.send(&Packet::Disconnect(Disconnect {
            reason: code,
            properties: Properties::new(),
        }))
        .await;
        c.expect_closed().await;
        assert_eq!(
            next(&mut watcher).await,
            got(&format!("ev/gone/{id}"), name, QoS::AtMostOnce, false)
        );
    }
    watcher.expect_silence().await;
    broker.wait_metric(&actions("gone", "ok"), 4).await;
    let metrics = broker.metrics().await;
    assert_eq!(
        series_lines(&metrics, "mqttd_rule_actions_total{rule=\"gone\""),
        vec![format!("{} 4", actions("gone", "ok"))]
    );
}

/// A rule on Will topics, `v1` of it.
const WILL_V1: &str = r#"[rules.lastwill]
sql = 'SELECT payload, clientid FROM "will/#"'
actions = [{ function = "republish", args = { topic = "wout/${clientid}", payload = "v1:${payload}", qos = 1 } }]
"#;

/// [`WILL_V1`] after an edit: the same rule, a different payload.
const WILL_V2: &str = r#"[rules.lastwill]
sql = 'SELECT payload, clientid FROM "will/#"'
actions = [{ function = "republish", args = { topic = "wout/${clientid}", payload = "v2:${payload}", qos = 1 } }]
"#;

/// RULES.md "Where rules run": "Last Wills run rules too, as in EMQX. The hub publishes a
/// Will, so the hub evaluates it"; evaluating it at CONNECT instead "would … miss any
/// reload before the client went away". And "Delivery guarantees": each action a Will's
/// rules derive "is counted `ok` once the broker has routed its message". A v3.1.1 client
/// connects with a `QoS` 1 Will; the rules file is edited and reloaded (`SIGHUP`) while
/// it stays connected; then its socket drops. A watcher receives the Will, then the
/// message the reloaded rule derives from it (`v2:`), and the rule counts one `passed`
/// evaluation and one `ok` action. A Will that skipped the rules never produces the
/// second message; one evaluated at CONNECT produces `v1:`; an action counted `failed`,
/// twice, or not at all fails the exact series.
#[tokio::test]
async fn a_will_runs_the_rules_in_force_when_the_broker_publishes_it() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), WILL_V1);
    let broker = start(dir.path(), "will", &Setup::default()).await;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    assert_eq!(
        subscribe(&mut watcher, 1, "will/#", QoS::AtLeastOnce).await,
        vec![1]
    );
    assert_eq!(
        subscribe(&mut watcher, 2, "wout/#", QoS::AtLeastOnce).await,
        vec![1]
    );
    let mut device = Client::open(broker.addr, V4).await;
    device
        .connect_with_will("dev-w", "will/dev-w", b"gone")
        .await;

    write_rules(dir.path(), WILL_V2);
    broker.signal("HUP");
    broker
        .wait_log(&format!(
            " INFO mqttd::reload: rules reloaded (ADR 0083) rules=1 enabled=1 digest={}",
            sha256_hex(WILL_V2.as_bytes())
        ))
        .await;
    broker
        .wait_log(
            " INFO mqttd::reload: security policy reloaded: ACL + authenticator (+ TLS, \
             gossip CRL) swapped trigger=\"signal\"",
        )
        .await;
    drop(device);

    assert_eq!(
        next(&mut watcher).await,
        got("will/dev-w", "gone", QoS::AtLeastOnce, false)
    );
    assert_eq!(
        next(&mut watcher).await,
        got("wout/dev-w", "v2:gone", QoS::AtLeastOnce, false)
    );
    watcher.expect_silence().await;
    broker.wait_metric(&actions("lastwill", "ok"), 1).await;
    let metrics = broker.metrics().await;
    assert_eq!(
        series_lines(&metrics, "mqttd_rule_evaluations_total{rule=\"lastwill\""),
        vec![format!("{} 1", evaluations("lastwill", "passed"))]
    );
    assert_eq!(
        series_lines(&metrics, "mqttd_rule_actions_total{rule=\"lastwill\""),
        vec![format!("{} 1", actions("lastwill", "ok"))]
    );
}

// ---------------------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------------------

/// RULES.md "Events" lists the `shutdown` disconnect reason (graceful drain), and an
/// event's derived messages are "published like a Will" — so routed and queued for
/// offline persistent sessions like any publish. An offline persistent watcher subscribed
/// to `presence/#`, twelve clients connected (v5 and v3.1.1), `SIGTERM`, a restart on the
/// same data directory: the watcher's stored session holds all twelve `shutdown`
/// messages. Before the audit fix these were sent fire-and-forget and the broker exited
/// first: run against that binary, this test's watcher received none of the twelve in
/// each of seven runs, which fails the exact list.
#[tokio::test]
async fn a_graceful_shutdown_keeps_every_shutdown_event_for_an_offline_watcher() {
    const N: usize = 12;
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.presence]
sql = '''
SELECT clientid, reason FROM "$events/client/disconnected"
WHERE reason = 'shutdown'
'''
actions = [{ function = "republish", args = { topic = "presence/${clientid}", payload = "${reason}", qos = 1 } }]
"#,
    );
    let setup = Setup {
        toml: "\n[runtime]\nshutdown_grace_secs = 10\n".to_string(),
        ..Setup::default()
    };
    let mut broker = start(dir.path(), "drain", &setup).await;
    let (mut watcher, _) = connect_persistent_v5(broker.addr, "watcher", true).await;
    assert_eq!(
        subscribe(&mut watcher, 1, "presence/#", QoS::AtLeastOnce).await,
        vec![1]
    );
    watcher.disconnect().await;
    let mut devices = Vec::new();
    for i in 0..N {
        let id = format!("dev-{i:02}");
        devices.push(if i % 2 == 0 {
            Client::connect_v5_ok(broker.addr, &id).await
        } else {
            Client::connect(broker.addr, &id).await
        });
    }

    let status = broker.terminate().await;
    assert!(status.success(), "a graceful stop exits 0, got {status:?}");
    drop(devices);

    let broker = start(dir.path(), "drain", &setup).await;
    let (mut watcher, present) = connect_persistent_v5(broker.addr, "watcher", false).await;
    assert!(present, "the watcher's session survived the restart");
    let mut delivered = collect(&mut watcher, N).await;
    delivered.sort();
    let want: Vec<Got> = (0..N)
        .map(|i| {
            got(
                &format!("presence/dev-{i:02}"),
                "shutdown",
                QoS::AtLeastOnce,
                false,
            )
        })
        .collect();
    assert_eq!(delivered, want);
    watcher.expect_silence().await;
}

// ---------------------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------------------

/// RULES.md "Delivery guarantees": a graceful shutdown keeps what its disconnects derive
/// for a subscriber on ANOTHER node too — while the broker drains, those messages are
/// forwarded acked, and the drain waits for the peer's answer before the process exits
/// (review of PR #871). Two real processes, A's link to B through a relay; the watcher is
/// live on node B and twelve clients are connected to node A. The link turns slow — the
/// relay holds each chunk for 1.5 s each way — and A is stopped with `SIGTERM`. The
/// watcher receives all twelve `shutdown` presence messages, exactly once each.
///
/// What this pins is the end-to-end behaviour on a real, slow link. The mechanism — the
/// forwards are acked while draining, and the drain barrier waits for B's answers — is
/// pinned by `hub::tests::while_draining_a_rule_derived_forward_holds_the_barrier_until_
/// the_peer_answers`, which fails without it. This test does not: on a local link a
/// frame handed to the kernel is still delivered after A's graceful close, so the loss
/// needs frames queued inside A at exit (a backed-up link), which a test cannot stage
/// reliably. (A link DOWN for the whole drain is not covered either way: a draining node
/// does not redial, so what it owes there is lost at the grace deadline, logged.)
#[tokio::test]
async fn a_graceful_shutdown_forwards_every_shutdown_event_to_a_watcher_on_another_node() {
    const N: usize = 12;
    const PRESENCE: &str = r#"[rules.presence]
sql = '''
SELECT clientid, reason FROM "$events/client/disconnected"
WHERE reason = 'shutdown'
'''
actions = [{ function = "republish", args = { topic = "presence/${clientid}", payload = "${reason}", qos = 1 } }]
"#;
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_rules(dir_a.path(), PRESENCE);
    write_rules(dir_b.path(), PRESENCE);
    let node_b = start(
        dir_b.path(),
        "node-b",
        &Setup {
            peers: Some(vec![]),
            rules_by_env: true,
            ..Setup::default()
        },
    )
    .await;
    let (relay_addr, relay, _relay_task) =
        proc_common::spawn_relay(node_b.peer.expect("node B's peer listener")).await;
    let mut node_a = start(
        dir_a.path(),
        "node-a",
        &Setup {
            peers: Some(vec![relay_addr.parse().unwrap()]),
            toml: "\n[runtime]\nshutdown_grace_secs = 20\n".to_string(),
            ..Setup::default()
        },
    )
    .await;

    let mut watcher = Client::connect(node_b.addr, "watcher").await;
    assert_eq!(
        subscribe(&mut watcher, 1, "presence/#", QoS::AtLeastOnce).await,
        vec![1]
    );
    // Until the link is up and B's interest has reached A, a publish on A reaches nobody
    // on B: publish numbered warm-ups until the latest one sent arrives (the link is
    // FIFO). The warm-up client then leaves with a normal DISCONNECT, which the rule's
    // WHERE does not select.
    let mut warm = Client::connect(node_a.addr, "warm").await;
    let mut sent = 0u16;
    'warm: loop {
        sent += 1;
        assert!(sent <= 100, "node A's publishes never reached node B");
        let n = sent.to_string();
        assert_eq!(
            publish_acked(&mut warm, "presence/warm-up", n.as_bytes(), sent).await,
            0
        );
        loop {
            match watcher.recv_bounded(Duration::from_millis(300)).await {
                Recv::Packet(Packet::Publish(p)) => {
                    if let Some(id) = p.pkid {
                        watcher.puback(id).await;
                    }
                    assert_eq!(p.topic, "presence/warm-up");
                    if p.payload == n.as_bytes() {
                        break 'warm;
                    }
                }
                Recv::Quiet => continue 'warm,
                other => panic!("expected a warm-up PUBLISH, got {other:?}"),
            }
        }
    }
    warm.disconnect().await;

    let mut devices = Vec::new();
    for i in 0..N {
        devices.push(Client::connect(node_a.addr, &format!("dev-{i:02}")).await);
    }
    // The link stays up but turns slow: the relay holds every chunk for 1.5 s each way.
    relay.slow(1500);
    let status = node_a.terminate().await;
    assert!(status.success(), "a graceful stop exits 0, got {status:?}");
    drop(devices);

    let mut delivered = collect(&mut watcher, N).await;
    delivered.sort();
    let want: Vec<Got> = (0..N)
        .map(|i| {
            got(
                &format!("presence/dev-{i:02}"),
                "shutdown",
                QoS::AtLeastOnce,
                false,
            )
        })
        .collect();
    assert_eq!(delivered, want);
    watcher.expect_silence().await;
}

/// RULES.md "Where rules run": "Once per message, on the node it arrived at. A message
/// forwarded to another node is never evaluated again there", and what a rule republishes
/// reaches "a subscriber on any node". Two real `mqttd` processes in a static-peer mesh,
/// both running the same rules file (node B names it with `MQTTD_RULES_FILE`): a
/// publish on A is evaluated once on A, its original and derived message both reach a
/// subscriber on B, and B's evaluation counter never appears although B has the rule
/// loaded. A forwarded copy re-evaluated on B would deliver a second derived message and
/// count on B.
///
/// The link is owned by the lower node id (`node-a`), which dials; `node-b` only listens,
/// so it starts first and A is given B's bound peer address. Every port, peer ports
/// included, is chosen inside `start`'s retry, so a port lost to another process is
/// replaced rather than reused.
#[tokio::test]
async fn in_a_two_process_cluster_a_publish_is_evaluated_once_on_its_landing_node() {
    const ALERT: &str = r#"[rules.alert]
sql = 'SELECT payload.temp AS temp, clientid, qos FROM "sensors/+/data" WHERE payload.temp > 30'
actions = [{ function = "republish", args = { topic = "alerts/${clientid}", payload = "${.}" } }]
"#;
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_rules(dir_a.path(), ALERT);
    write_rules(dir_b.path(), ALERT);
    let node_b = start(
        dir_b.path(),
        "node-b",
        &Setup {
            peers: Some(vec![]),
            rules_by_env: true,
            ..Setup::default()
        },
    )
    .await;
    let node_a = start(
        dir_a.path(),
        "node-a",
        &Setup {
            peers: Some(vec![node_b.peer.expect("node B's peer listener")]),
            ..Setup::default()
        },
    )
    .await;

    let mut sub = Client::connect(node_b.addr, "sub-b").await;
    assert_eq!(subscribe(&mut sub, 1, "#", QoS::AtLeastOnce).await, vec![1]);
    let mut publ = Client::connect(node_a.addr, "devA").await;
    // Until the link is up and B's interest has reached A, a publish on A reaches nobody
    // on B. Publish numbered warm-ups (no rule selects them) until the latest one sent
    // arrives: the link is FIFO, so no earlier one can arrive after it.
    let mut sent = 0u16;
    'warm: loop {
        sent += 1;
        assert!(sent <= 100, "node A's publishes never reached node B");
        let n = sent.to_string();
        assert_eq!(
            publish_acked(&mut publ, "warm/up", n.as_bytes(), sent).await,
            0
        );
        loop {
            match sub.recv_bounded(Duration::from_millis(300)).await {
                Recv::Packet(Packet::Publish(p)) => {
                    if let Some(id) = p.pkid {
                        sub.puback(id).await;
                    }
                    assert_eq!(p.topic, "warm/up");
                    if p.payload == n.as_bytes() {
                        break 'warm;
                    }
                }
                Recv::Quiet => continue 'warm,
                other => panic!("expected a warm-up PUBLISH, got {other:?}"),
            }
        }
    }

    assert_eq!(
        publish_acked(&mut publ, "sensors/devA/data", br#"{"temp":35}"#, 1000).await,
        0
    );
    assert_eq!(
        next(&mut sub).await,
        got(
            "sensors/devA/data",
            r#"{"temp":35}"#,
            QoS::AtLeastOnce,
            false
        )
    );
    assert_eq!(
        next(&mut sub).await,
        got(
            "alerts/devA",
            r#"{"temp":35,"clientid":"devA","qos":1}"#,
            QoS::AtLeastOnce,
            false
        )
    );
    sub.expect_silence().await;

    node_a.wait_metric(&evaluations("alert", "passed"), 1).await;
    let metrics_a = node_a.metrics().await;
    assert_eq!(
        series_lines(&metrics_a, "mqttd_rule_evaluations_total{"),
        vec![format!("{} 1", evaluations("alert", "passed"))]
    );
    assert_eq!(sample(&metrics_a, &actions("alert", "ok")), Some(1));
    let metrics_b = node_b.metrics().await;
    assert_eq!(sample(&metrics_b, "mqttd_rules_loaded"), Some(1));
    assert_eq!(
        series_lines(&metrics_b, "mqttd_rule_evaluations_total{"),
        Vec::<&str>::new(),
        "node B evaluated a forwarded message"
    );
}
