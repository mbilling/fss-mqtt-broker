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
    let deadline = Instant::now() + Duration::from_mins(1);
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
    // What `--decommission` signals: a process that exits a second after SIGUSR1, as the
    // broker does once its drain is done. The second is what `--decommission` must wait out
    // with its stdout gone: a command that ended at its first write would return first.
    let mut target = ChildGuard(
        Command::new("sh")
            .arg("-c")
            .arg("trap 'sleep 1; exit 0' USR1; for i in $(seq 1 600); do sleep 0.1; done")
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
    assert!(
        target.0.try_wait().expect("try_wait").is_some(),
        "mqttd --decommission returned with stdout closed before its target had exited"
    );
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

/// RULES.md "Events": the alarm, schema validation and message transformation events
/// "are not raised. A rule that selects one is refused at load." A rule on
/// `sys/alarm_activated` that loaded would never fire, silently. The message events are
/// raised, and listed.
#[test]
fn check_rules_refuses_an_event_mqttd_does_not_raise() {
    assert_check_rules_refuses(
        "[rules.ev]\nsql = 'SELECT * FROM \"$events/sys/alarm_activated\"'\n",
        "rule `ev`: \"$events/sys/alarm_activated\" is not a supported event (supported: \
         $events/client/connected, $events/client/disconnected, $events/client/connack, \
         $events/client/ping, $events/auth/check_authn_complete, \
         $events/auth/check_authz_complete, $events/session/subscribed, \
         $events/session/unsubscribed, $events/message/delivered, $events/message/acked, \
         $events/message/dropped, $events/message/delivery_dropped)",
    );
}

/// RULES.md "Events": EMQX matches a `FROM "$events/…"` filter against its event topics
/// (`emqx_rule_events:match_event_names/1`), so a wildcard selects every event it
/// matches; one matching only events mqttd does not raise is refused, and `$sources/`,
/// EMQX's data-integration sources, is refused like `$bridges/`.
#[test]
fn check_rules_refuses_wildcards_matching_no_raised_event_and_sources() {
    assert_check_rules_refuses(
        "[rules.ev]\nsql = 'SELECT * FROM \"$events/sys/+\"'\n",
        "rule `ev`: \"$events/sys/+\" matches no event mqttd raises (supported: \
         $events/client/connected, $events/client/disconnected, $events/client/connack, \
         $events/client/ping, $events/auth/check_authn_complete, \
         $events/auth/check_authz_complete, $events/session/subscribed, \
         $events/session/unsubscribed, $events/message/delivered, $events/message/acked, \
         $events/message/dropped, $events/message/delivery_dropped)",
    );
    assert_check_rules_refuses(
        "[rules.src]\nsql = 'SELECT * FROM \"$sources/mqtt:in\"'\n",
        "rule `src`: \"$sources/mqtt:in\": mqttd has no data integration sources to select \
         from (ADR 0083)",
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

/// `--rule-test` prints each output as the rule renders it, as EMQX 6.3.1 encodes the
/// same values (`emqx_utils_json:encode/1`): every digit of an integer past 64 bits, the
/// float `1.0e20`, the keys in the statement's order, and U+FFFD for the byte
/// `sprintf('~c', 210)` gives, which is not UTF-8.
#[test]
fn rule_test_prints_an_output_exactly_as_the_rule_renders_it() {
    let ran = cli(&[
        "--rule-test",
        "--sql",
        "SELECT payload.z AS z, payload.z + 1 AS next, payload.f AS f, payload.m AS m, \
         sprintf('~c', 210) AS c FROM \"t/#\"",
        "--payload",
        r#"{"z":123456789012345678901234567890,"f":1e20,"m":{"b":1,"a":2}}"#,
    ]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(
        ran.stdout,
        "{\"z\":123456789012345678901234567890,\"next\":123456789012345678901234567891,\
         \"f\":1.0e20,\"m\":{\"b\":1,\"a\":2},\"c\":\"\u{FFFD}\"}\n"
    );
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

// ---------------------------------------------------------------------------------------
// Watching the running rules on $SYS (ADR 0084)
// ---------------------------------------------------------------------------------------

/// ADR 0084: the rule statistics and the trace, configured from the environment the way
/// the live demo stack configures them, reach a real subscriber of the real binary — the
/// summary, the rule's message with its counted evaluation, and the trace record of the
/// publish that fired it. `#` does not cover `$SYS`, so the subscriber names the trees.
/// A client publishing into `$SYS` is refused (v5 `0x87`), so it cannot forge them. The
/// log says the trace is on, and — with no ACL file — that any client may read it.
#[tokio::test]
async fn env_configured_rule_statistics_and_trace_reach_a_subscriber() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.watched]
sql = 'SELECT payload.v AS v FROM "w/#"'
actions = [{ function = "republish", args = { topic = "w-out/${v}" } }]
"#,
    );
    let setup = Setup {
        env: vec![
            ("MQTTD_RULES_SYS_INTERVAL", "1".to_string()),
            ("MQTTD_RULES_TRACE", "1".to_string()),
            ("MQTTD_RULES_TRACE_RATE", "5".to_string()),
        ],
        ..Setup::default()
    };
    let broker = start(dir.path(), "sys-node", &setup).await;
    let mut sub = Client::connect_v5_ok(broker.addr, "watcher").await;
    // The trace first: nothing has run a rule yet, so nothing arrives on it. Subscribed
    // second, its SUBACK could be preceded by a statistics tick from the first
    // subscription (every second), which the helper would read in the SUBACK's place.
    assert_eq!(
        subscribe(&mut sub, 1, "$SYS/brokers/+/trace/rules/+", QoS::AtMostOnce).await,
        [0]
    );
    assert_eq!(
        subscribe(&mut sub, 2, "$SYS/brokers/+/rules/#", QoS::AtMostOnce).await,
        [0]
    );
    let mut publ = Client::connect_v5_ok(broker.addr, "publisher").await;
    assert_eq!(
        publish_acked(&mut publ, "$SYS/brokers/sys-node/rules", b"{}", 1).await,
        0x87,
        "a client cannot publish into $SYS"
    );
    assert_eq!(publish_acked(&mut publ, "w/1", br#"{"v":"x"}"#, 2).await, 0);

    let (mut summary, mut counted, mut traced) = (false, false, false);
    let deadline = Instant::now() + Duration::from_secs(15);
    while !(summary && counted && traced) {
        assert!(
            Instant::now() < deadline,
            "summary {summary}, counted {counted}, traced {traced} after 15 s; log tail:\n{}",
            broker.log.tail(30)
        );
        let Recv::Packet(Packet::Publish(p)) = sub.recv_bounded(Duration::from_secs(5)).await
        else {
            continue;
        };
        assert_eq!((p.qos, p.retain), (QoS::AtMostOnce, false));
        let doc: serde_json::Value = serde_json::from_slice(&p.payload).unwrap();
        match p.topic.as_str() {
            "$SYS/brokers/sys-node/rules" => {
                assert_eq!(doc["rules"], 1);
                assert_eq!(doc["interval_secs"], 1);
                assert_eq!(
                    (doc["trace"].as_bool(), doc["trace_rate"].as_u64()),
                    (Some(true), Some(5))
                );
                summary = true;
            }
            "$SYS/brokers/sys-node/rules/watched" => {
                counted |= doc["counts"]["passed"] == 1;
            }
            "$SYS/brokers/sys-node/trace/rules/watched" => {
                assert_eq!(doc["trigger"]["topic"], "w/1");
                assert_eq!(doc["trigger"]["clientid"], "publisher");
                assert_eq!(doc["outputs"][0]["topic"], "w-out/x");
                traced = true;
            }
            other => panic!("nothing else is published on $SYS: {other}"),
        }
    }
    broker
        .wait_log(" WARN mqttd::rules: INSECURE: the rule trace is on and no MQTTD_ACL_FILE is configured: any client can subscribe to $SYS/brokers/+/trace/rules/+ and read what the rules see (ADR 0084)")
        .await;
    broker.wait_log(&trace_is_on(5)).await;
}

/// The WARN the broker logs as the trace turns on at `rate` records a second per rule.
fn trace_is_on(rate: u32) -> String {
    format!(
        " WARN mqttd::rules: rule trace is ON: every rule's evaluations — trigger topic, \
         client id, username, up to 1 KiB of payload, rendered outputs — are copied onto \
         $SYS/brokers/<node>/trace/rules/<id> (ADR 0084); a subscriber there reads every \
         topic that rule's FROM matches rate={rate}"
    )
}

/// The next `$SYS` message `c` receives on `topic` that `want` accepts, read as JSON; the
/// rest are passed over. Bounded: fails after 15 s, naming the last one on `topic`.
async fn sys_where(
    c: &mut Client,
    topic: &str,
    want: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = serde_json::Value::Null;
    loop {
        assert!(
            Instant::now() < deadline,
            "nothing as wanted on {topic} after 15 s; the last: {last}"
        );
        match c.recv_bounded(Duration::from_secs(5)).await {
            Recv::Packet(Packet::Publish(p)) if p.topic == topic => {
                let doc = serde_json::from_slice(&p.payload).expect("JSON on $SYS");
                if want(&doc) {
                    return doc;
                }
                last = doc;
            }
            Recv::Packet(Packet::Publish(_)) | Recv::Quiet => {}
            other => panic!("expected only $SYS publishes, got {other:?}"),
        }
    }
}

/// RULES.md "Watch and edit rules live": the `[rules]` watch keys are live — an applied
/// reload changes them, and a rejected one leaves them as they were. With the keys in the
/// config file and `SIGHUP`: a reload that turns the trace on brings the next publish's
/// record; one with `trace_rate = 0` is rejected — the summary says so — and the trace
/// stays on; one that turns the trace off and the interval to 2 s shows both in the
/// summary, and a publish then has no record across two ticks. A broker that took the
/// keys at boot only fails the first wait; one that applied a rejected candidate, or kept
/// the trace on, fails the later ones.
#[tokio::test]
async fn a_reload_turns_rule_watching_on_and_off_and_a_rejected_one_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(
        dir.path(),
        r#"[rules.watched]
sql = 'SELECT payload.v AS v FROM "w/#"'
actions = []
"#,
    );
    let watch = |keys: &str| format!("\n[rules]\n{keys}");
    let setup = Setup {
        toml: watch("sys_interval_secs = 1\ntrace = false\n"),
        rules_by_env: true,
        ..Setup::default()
    };
    let broker = start(dir.path(), "watch-node", &setup).await;
    let config = dir.path().join("mqttd.toml");
    let base = std::fs::read_to_string(&config)
        .unwrap()
        .replace(&setup.toml, "");
    let reload = |keys: &str| {
        std::fs::write(&config, format!("{base}{}", watch(keys))).unwrap();
        broker.signal("HUP");
    };
    let (summary, trace) = (
        "$SYS/brokers/watch-node/rules",
        "$SYS/brokers/watch-node/trace/rules/watched",
    );
    let mut sub = Client::connect_v5_ok(broker.addr, "watcher").await;
    // The trace first: it is off, so nothing arrives on it. Subscribed second, its SUBACK
    // could be preceded by a statistics tick from the first subscription (every second).
    assert_eq!(subscribe(&mut sub, 1, trace, QoS::AtMostOnce).await, [0]);
    assert_eq!(subscribe(&mut sub, 2, summary, QoS::AtMostOnce).await, [0]);
    let mut publ = Client::connect_v5_ok(broker.addr, "publisher").await;
    let first = sys_where(&mut sub, summary, |_| true).await;
    assert_eq!(first["trace"], false);
    assert_eq!(first["interval_secs"], 1);
    assert_eq!(first["reload"], serde_json::Value::Null);

    reload("sys_interval_secs = 1\ntrace = true\n");
    let on = sys_where(&mut sub, summary, |s| s["trace"] == true).await;
    assert_eq!(on["reload"]["trigger"], "signal");
    assert_eq!(on["reload"]["applied"], true);
    broker.wait_log(&trace_is_on(20)).await;
    assert_eq!(publish_acked(&mut publ, "w/1", br#"{"v":1}"#, 1).await, 0);
    let record = sys_where(&mut sub, trace, |_| true).await;
    assert_eq!(record["trigger"]["payload"], r#"{"v":1}"#);

    reload("sys_interval_secs = 1\ntrace = true\ntrace_rate = 0\n");
    let rejected = sys_where(&mut sub, summary, |s| s["reload"]["applied"] == false).await;
    assert_eq!(rejected["reload"]["error_kind"], "config");
    assert_eq!(rejected["trace"], true, "the trace stays on");
    assert_eq!(
        rejected["trace_rate"], 20,
        "the rejected rate is not applied"
    );
    assert_eq!(publish_acked(&mut publ, "w/1", br#"{"v":2}"#, 2).await, 0);
    let record = sys_where(&mut sub, trace, |_| true).await;
    assert_eq!(record["trigger"]["payload"], r#"{"v":2}"#, "still tracing");

    reload("sys_interval_secs = 2\ntrace = false\n");
    let off = sys_where(&mut sub, summary, |s| s["trace"] == false).await;
    assert_eq!(off["reload"]["applied"], true);
    assert_eq!(off["interval_secs"], 2);
    broker
        .wait_log(" INFO mqttd::rules: rule trace is off (ADR 0084)")
        .await;
    assert_eq!(publish_acked(&mut publ, "w/1", br#"{"v":3}"#, 3).await, 0);
    // Two ticks after the publish, and nothing but the summaries.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut ticks = 0;
    while ticks < 2 {
        assert!(
            Instant::now() < deadline,
            "{ticks} of two ticks after 15 s at a 2 s interval"
        );
        match sub.recv_bounded(Duration::from_secs(5)).await {
            Recv::Packet(Packet::Publish(p)) => {
                assert_eq!(p.topic, summary, "a trace record with the trace off");
                let doc: serde_json::Value = serde_json::from_slice(&p.payload).unwrap();
                assert_eq!(doc["trace"], false);
                assert_eq!(doc["interval_secs"], 2);
                ticks += 1;
            }
            Recv::Quiet => {}
            other => panic!("expected only $SYS publishes, got {other:?}"),
        }
    }
    let watched = broker.sample(&evaluations("watched", "passed")).await;
    assert_eq!(watched, Some(3), "the untraced publish still ran the rule");
}

// ---------------------------------------------------------------------------------------
// Rules edited through the admin API (ADR 0084)
// ---------------------------------------------------------------------------------------

/// The admin listener's certificates: a CA, the broker's for 127.0.0.1, and the
/// `CN=rules-ui` client the rules are written as.
struct AdminPki {
    ca: std::path::PathBuf,
    server: (std::path::PathBuf, std::path::PathBuf),
    client: (std::path::PathBuf, std::path::PathBuf),
}

fn admin_pki(dir: &Path) -> AdminPki {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "rules admin CA");
    let ca = rcgen::CertifiedIssuer::self_signed(params, key).unwrap();
    std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
    let leaf = |name: &str| {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &ca).unwrap();
        let (cert_path, key_path) = (
            dir.join(format!("{name}.pem")),
            dir.join(format!("{name}.key")),
        );
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        (cert_path, key_path)
    };
    AdminPki {
        ca: dir.join("ca.pem"),
        server: leaf("mqttd"),
        client: leaf("rules-ui"),
    }
}

/// `mqttd --admin <args>` against the listener on `admin`, as `CN=rules-ui`.
fn admin_cli(pki: &AdminPki, admin: SocketAddr, args: &[&str]) -> Ran {
    let url = format!("https://{admin}");
    let mut all = vec!["--admin"];
    all.extend_from_slice(args);
    let (ca, cert, key) = (
        pki.ca.display().to_string(),
        pki.client.0.display().to_string(),
        pki.client.1.display().to_string(),
    );
    all.extend_from_slice(&["--url", &url, "--ca", &ca, "--cert", &cert, "--key", &key]);
    cli(&all)
}

/// The environment that opens the admin listener on `admin` with `pki`, `CN=rules-ui` an
/// operator and its only rules writer.
fn admin_env(pki: &AdminPki, admin: SocketAddr) -> Vec<(&'static str, String)> {
    vec![
        ("MQTTD_ADMIN_BIND", admin.to_string()),
        ("MQTTD_ADMIN_CERT", pki.server.0.display().to_string()),
        ("MQTTD_ADMIN_KEY", pki.server.1.display().to_string()),
        ("MQTTD_ADMIN_CLIENT_CA", pki.ca.display().to_string()),
        ("MQTTD_ADMIN_OPERATORS", "CN=rules-ui".to_string()),
        ("MQTTD_RULES_ADMIN_WRITERS", "CN=rules-ui".to_string()),
    ]
}

const EDIT_A: &str = r#"# The rules the admin API edits.
[rules.watched]
sql = 'SELECT payload.v AS v FROM "w/#"'
actions = [{ function = "republish", args = { topic = "out/w/${v}", payload = "${v}" } }]
"#;

const EDIT_B: &str = r#"# The rules the admin API edits.
[rules.watched]
sql = 'SELECT payload.v AS v FROM "w/#"'
actions = [{ function = "republish", args = { topic = "out/w/${v}", payload = "${v}" } }]

[rules.added]
sql = 'SELECT payload.v AS v FROM "n/#"'
actions = [{ function = "republish", args = { topic = "out/new/${v}", payload = "${v}" } }]
"#;

/// ADR 0084: a rules writer edits the running rules through the admin API, from the
/// operator's own commands — `mqttd --admin rules-source`, `rules-apply`, `rules` and
/// `rule-delete` — and the next message runs the edited rules, for a client that stayed
/// connected throughout. `rules-source` prints the file byte for byte; `rules-apply`
/// without `--if_match` is refused (428) and writes nothing; with it the file is replaced
/// and reloaded; a delete leaves the rest. A broker that wrote the file but did not
/// reload, or reloaded only for new connections, fails the first derived message; one
/// whose delete did not take fails the order of the last two.
#[tokio::test]
async fn an_admin_rules_edit_applies_to_the_next_message() {
    let dir = tempfile::tempdir().unwrap();
    let pki_dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), EDIT_A);
    let pki = admin_pki(pki_dir.path());
    let admin = band_addr();
    let setup = Setup {
        env: admin_env(&pki, admin),
        ..Setup::default()
    };
    let broker = start(dir.path(), "edit-node", &setup).await;
    broker
        .wait_log(&format!(
            "serving the admin API (mTLS; ADR 0081) bind={admin} viewers=0 operators=1 \
             rules_writers=1 peers=false"
        ))
        .await;
    let mut sub = Client::connect(broker.addr, "edit-sub").await;
    assert_eq!(subscribe(&mut sub, 1, "out/#", QoS::AtMostOnce).await, [0]);
    let mut publ = Client::connect(broker.addr, "edit-pub").await;

    let source = admin_cli(&pki, admin, &["rules-source"]);
    assert_eq!(
        (source.code, source.stdout.as_str()),
        (Some(0), EDIT_A),
        "{source:?}"
    );

    let next_file = pki_dir.path().join("next.toml");
    std::fs::write(&next_file, EDIT_B).unwrap();
    let next_path = next_file.display().to_string();
    let refused = admin_cli(&pki, admin, &["rules-apply", &next_path]);
    assert_eq!(refused.code, Some(1), "{refused:?}");
    assert!(
        refused.stderr.contains("428 precondition-required"),
        "{refused:?}"
    );
    let digest = sha256_hex(EDIT_A.as_bytes());
    let applied = admin_cli(
        &pki,
        admin,
        &["rules-apply", &next_path, "--if_match", &digest],
    );
    assert_eq!(applied.code, Some(0), "{applied:?}");
    assert!(
        applied
            .stdout
            .lines()
            .any(|l| l.starts_with("applied ") && l.ends_with(" true")),
        "{applied:?}"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("rules.toml")).unwrap(),
        EDIT_B
    );

    // The connection that was open all along runs the new rule on its next message.
    publ.publish("n/1", br#"{"v":"x"}"#, QoS::AtMostOnce, None, vec![])
        .await;
    assert_eq!(
        next(&mut sub).await,
        got("out/new/x", "x", QoS::AtMostOnce, false)
    );
    let table = admin_cli(&pki, admin, &["rules"]);
    assert_eq!(table.code, Some(0), "{table:?}");
    let added: Vec<&str> = table
        .stdout
        .lines()
        .find(|l| l.starts_with("added "))
        .unwrap_or_else(|| panic!("no row for the new rule: {table:?}"))
        .split_whitespace()
        .collect();
    assert_eq!(added[..5], ["added", "yes", "n/#", "1", "1"], "{table:?}");

    // Deleted, it no longer runs: the message after this one is the first to arrive.
    let deleted = admin_cli(&pki, admin, &["rule-delete", "added"]);
    assert_eq!(deleted.code, Some(0), "{deleted:?}");
    publ.publish("n/2", br#"{"v":"z"}"#, QoS::AtMostOnce, None, vec![])
        .await;
    publ.publish("w/1", br#"{"v":"y"}"#, QoS::AtMostOnce, None, vec![])
        .await;
    assert_eq!(
        next(&mut sub).await,
        got("out/w/y", "y", QoS::AtMostOnce, false)
    );
    let on_disk = std::fs::read_to_string(dir.path().join("rules.toml")).unwrap();
    assert!(
        on_disk.starts_with(EDIT_A) && !on_disk.contains("[rules.added]"),
        "{on_disk}"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("rules.toml.prev")).unwrap(),
        EDIT_B,
        "the replaced file is kept"
    );
}

/// ADR 0084: rules writers configured for a rules file whose directory the broker may not
/// write in — the documented read-only mounts — are a configuration that refuses every
/// write, and the broker says so at boot and again on each reload, before anyone tries.
/// A broker that stayed quiet until the first write fails the first wait; one that warned
/// only at boot fails the second count.
#[cfg(unix)]
#[tokio::test]
async fn writers_for_a_rules_directory_the_broker_cannot_write_in_are_warned_about() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let pki_dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), EDIT_A);
    let read_only = dir.path().join("ro");
    std::fs::create_dir(&read_only).unwrap();
    std::fs::write(read_only.join("rules.toml"), EDIT_A).unwrap();
    std::fs::set_permissions(&read_only, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::write(read_only.join("probe"), "").is_ok() {
        std::fs::set_permissions(&read_only, std::fs::Permissions::from_mode(0o755)).unwrap();
        crate::skip_locally_or_fail_in_ci!(
            "this user can create files in a mode-555 directory (running as root?), so a \
             rules directory the broker may not write in cannot be made here; run the suite \
             as an unprivileged user"
        );
    }
    let pki = admin_pki(pki_dir.path());
    let admin = band_addr();
    let mut env = admin_env(&pki, admin);
    env.push((
        "MQTTD_RULES_FILE",
        read_only.join("rules.toml").display().to_string(),
    ));
    let broker = start(
        dir.path(),
        "ro-node",
        &Setup {
            env,
            ..Setup::default()
        },
    )
    .await;
    let warning = format!(
        " WARN mqttd: rules.admin_writers is set but the rules file's directory is not \
         writable by the broker ({}: Permission denied (os error 13)): every admin rules \
         write will be refused with rules-file-unwritable (ADR 0084)",
        read_only.display()
    );
    broker.wait_log(&warning).await;
    assert_eq!(broker.log_lines(|l| l.ends_with(&warning)).len(), 1);
    broker.signal("HUP");
    // Once at boot, once on the reload.
    let deadline = Instant::now() + Duration::from_secs(15);
    while broker.log_lines(|l| l.ends_with(&warning)).len() < 2 {
        assert!(
            Instant::now() < deadline,
            "the reload did not warn again within 15 s; log tail:\n{}",
            broker.log.tail(20)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    std::fs::set_permissions(&read_only, std::fs::Permissions::from_mode(0o755)).unwrap();
}

// ---------------------------------------------------------------------------------------
// The jq function (ADR 0086)
// ---------------------------------------------------------------------------------------

/// What the load error says while `jq` is switched off (ADR 0086 D13).
const JQ_OFF: &str = "jq is switched off: set `rules.jq = true` to use it (see RULES.md for \
                      what it needs from the host)";

/// One rule that calls `jq`.
const JQ_ONE: &str = r#"[rules.shape]
sql = '''SELECT jq('.a[]', payload) AS r FROM "jq/in"'''
actions = [{ function = "republish", args = { topic = "jq/out", payload = "${r}" } }]
"#;

/// The tests below that run jq need a broker built with the jq module, which is built
/// from pinned sources and not checked in. CI builds it; a laptop without zig does not.
const JQ_MODULE_HOW: &str =
    "run crates/mqtt-wasm-sandbox/jq-module/build.sh (zig 0.16.0) and build again";

/// `mqttd_rule_function_errors_total{function="jq",kind}`, as rendered.
fn jq_errors(kind: &str) -> String {
    format!(r#"mqttd_rule_function_errors_total{{function="jq",kind="{kind}"}}"#)
}

const JQ_CALLS: &str = r#"mqttd_rule_function_calls_total{function="jq"}"#;

/// ADR 0086 D13: `jq` is off unless the operator switches it on, and a rules file that
/// calls it then does not load — in `--check-rules`, in `--rule-test`, at boot — with a
/// message that names the switch. Nothing here needs the jq module: this is what every
/// build does by default. A broker that loaded the file and failed the rule on each
/// message, or refused it as an unknown function, fails the exact-stderr assertions.
#[test]
fn jq_is_off_by_default_and_a_rules_file_that_calls_it_does_not_load() {
    assert_check_rules_refuses(
        JQ_ONE,
        &format!("rule `shape`: {JQ_OFF} (line 1, column 8, near `jq('.a[]', payload) AS r`)"),
    );
    // Said off as well as left off.
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), JQ_ONE);
    let path = dir.path().join("rules.toml").display().to_string();
    let mut cmd = mqttd();
    cmd.args(["--check-rules", &path])
        .env("MQTTD_RULES_JQ", "0");
    let ran = run(cmd);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert!(ran.stderr.contains(JQ_OFF), "{ran:?}");

    let ran = cli(&[
        "--rule-test",
        "--sql",
        "SELECT jq('.', payload) AS r FROM \"t/#\"",
    ]);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert_eq!(
        ran.stderr,
        format!("rule test FAILED: {JQ_OFF} (line 1, column 8, near `jq('.', payload) AS r FR`)\n")
    );

    // The broker does not boot on it.
    let config = dir.path().join("mqttd.toml");
    std::fs::write(
        &config,
        format!(
            "[security]\nallow_anonymous = true\n\n[durable]\nenabled = false\n\n[rules]\n\
             file = \"{path}\"\n"
        ),
    )
    .unwrap();
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
        format!(
            "Error: rules: rule `shape`: {JQ_OFF} (line 1, column 8, near `jq('.a[]', payload) \
             AS r`)\n"
        )
    );
}

/// `mqttd --rule-test` with `rules.jq` on, as `MQTTD_RULES_JQ=1`.
fn jq_rule_test(sql: &str, payload: &str, env: &[(&str, &str)]) -> Ran {
    let mut cmd = mqttd();
    cmd.args(["--rule-test", "--sql", sql, "--payload", payload])
        .env("MQTTD_RULES_JQ", "1")
        // Nothing of the runner's environment reaches jq unless a test lists it.
        .env("SITE", "plant-7");
    for (key, value) in env {
        cmd.env(key, value);
    }
    run(cmd)
}

/// ADR 0086 D4: `jq/2` and `jq/3` in `mqttd --rule-test`, with what
/// `emqx_rule_sqltester:test/1` answered on EMQX 6.3.1 for the same statement and
/// payload (probed 2026-10-11): the outputs as a list, a string input read as JSON text
/// and any other value encoded first, integers of any size and floats kept apart, an
/// undefined input as the text `undefined`, and each way a call fails, tag and text.
/// A result of another shape, or an error EMQX words differently, fails the exact
/// comparison.
#[test]
#[allow(clippy::too_many_lines)] // one row per probed case
fn rule_test_runs_jq_with_the_results_emqx_gives() {
    if let Err(why) = mqtt_rules::Jq::built_in() {
        skip_locally_or_fail_in_ci!("{why}: {JQ_MODULE_HOW}");
    }
    let select = |call: &str| format!("SELECT {call} AS r FROM \"t/#\"");
    for (call, payload, emqx) in [
        (
            "jq('.', payload)",
            r#"{"a":[1,2]}"#,
            r#"{"r":[{"a":[1,2]}]}"#,
        ),
        ("jq('.a[]', payload)", r#"{"a":[1,2]}"#, r#"{"r":[1,2]}"#),
        ("jq('.', payload, 100)", "1", r#"{"r":[1]}"#),
        ("jq('.', payload, 0)", "1", r#"{"r":[1]}"#),
        ("jq('.', payload, 4294967296)", "1", r#"{"r":[1]}"#),
        ("jq('empty', payload)", "1", r#"{"r":[]}"#),
        (
            "jq('.', payload.a)",
            r#"{"a":{"b":1.0,"c":12345678901234567890123}}"#,
            r#"{"r":[{"b":1.0,"c":12345678901234567890123}]}"#,
        ),
        ("jq('.', nosuch)", "1", r#"{"r":["undefined"]}"#),
        ("jq('.', true)", "1", r#"{"r":[true]}"#),
        ("jq('.', 1.5)", "1", r#"{"r":[1.5]}"#),
        (
            "jq('.a,.b', payload)",
            r#"{"a":1,"b":[1.5,true,null]}"#,
            r#"{"r":[1,[1.5,true,null]]}"#,
        ),
    ] {
        let ran = jq_rule_test(&select(call), payload, &[]);
        assert_eq!(
            (ran.code, ran.stdout.as_str()),
            (Some(0), format!("{emqx}\n").as_str()),
            "{call} on {payload}: {ran:?}"
        );
    }
    // FOREACH over jq's outputs, and jq in a WHERE.
    let ran = jq_rule_test(
        "FOREACH jq('.[]', payload) DO item AS x FROM \"t/#\"",
        "[1,2]",
        &[],
    );
    assert_eq!(ran.stdout, "{\"x\":1}\n{\"x\":2}\n", "{ran:?}");
    let ran = jq_rule_test(
        "SELECT 1 AS x FROM \"t/#\" WHERE jq('.a', payload) = [1]",
        r#"{"a":1}"#,
        &[],
    );
    assert_eq!(ran.stdout, "{\"x\":1}\n", "{ran:?}");

    // The failures: EMQX's `{jq_exception, {Tag, Message}}`, as `tag: message`.
    let needs = "bad_timeout_val: Needs positive integer or infinity but got";
    for (call, payload, emqx) in [
        ("jq('.x', payload)", "[1]", "jq_err_process: jq error: Cannot index array with string \"x\"".to_string()),
        ("jq('1, error(\"boom\")', payload)", "1", "jq_err_process: jq error: boom".to_string()),
        ("jq('.', payload)", "abc", "jq_err_parse: Invalid numeric literal at EOF at line 1, column 3 (while parsing 'abc')".to_string()),
        ("jq('.', '')", "1", "jq_err_parse: Expected JSON value (while parsing '')".to_string()),
        (
            "jq('', payload)",
            "1",
            "jq_err_compile: jq: error: Top-level program not given (try \".\")jq: 1 compile error".to_string(),
        ),
        (
            "jq('repeat(.)', payload, 30)",
            "1",
            "timeout: jq program canceled as it took too long time to execute (timeout set to 30 ms)".to_string(),
        ),
        (
            "jq('repeat(.)', payload, 0)",
            "1",
            "timeout: jq program canceled as it took too long time to execute (timeout set to 0 ms)".to_string(),
        ),
        // The timeout is looked at before the program and the input are.
        ("jq('%%', payload, -1)", "1", format!("{needs} -1")),
        ("jq('.', payload, 100.0)", "1", format!("{needs} 100.0")),
        ("jq('.', payload, 1.5)", "1", format!("{needs} 1.5")),
        ("jq('.', payload, '100')", "1", format!("{needs} <<\"100\">>")),
        ("jq('.', payload, 'infinity')", "1", format!("{needs} <<\"infinity\">>")),
        ("jq('.', payload, nosuch)", "1", format!("{needs} undefined")),
        ("jq('.', payload, true)", "1", format!("{needs} true")),
        ("jq('.', payload, payload.l)", r#"{"l":[1,2]}"#, format!("{needs} [1,2]")),
        ("jq('.', payload, -99999999999999999999)", "1", format!("{needs} -99999999999999999999")),
        // Erlang's timer refuses a time of 2^64 ms or more, and the NIF's match on its
        // answer fails: EMQX's `{badmatch,{error,badarg}}`.
        ("jq('.', payload, 99999999999999999999)", "1", "{badmatch,{error,badarg}}".to_string()),
    ] {
        let ran = jq_rule_test(&select(call), payload, &[]);
        assert_eq!(
            (ran.code, ran.stdout.as_str(), ran.stderr.as_str()),
            (Some(1), "", format!("rule test FAILED: jq(): {emqx}\n").as_str()),
            "{call} on {payload}"
        );
    }
    // Where EMQX has no clause for the arguments, or its decoder refuses an output,
    // the rule fails here too; the text is this engine's.
    for (call, payload, ours) in [
        (
            "jq(1, payload)",
            "1",
            "jq(): the program must be a string, got a integer",
        ),
        (
            "jq(nosuch, payload)",
            "1",
            "jq(): the program must be a string, got a undefined",
        ),
        (
            "jq('.', payload)",
            "1e1000",
            "jq(): an output of the program is not JSON the rule engine reads: invalid JSON: \
             number out of range",
        ),
    ] {
        let ran = jq_rule_test(&select(call), payload, &[]);
        assert_eq!(ran.code, Some(1), "{call}: {ran:?}");
        assert!(ran.stderr.contains(ours), "{call}: {ran:?}");
    }
}

/// ADR 0086 D8: `$ENV` and `env` inside jq hold the variables `rules.jq_env` lists, when
/// they are set, and nothing else — where EMQX shows jq the broker's whole environment.
/// A broker that passed its environment through would show `PATH` (and any secret
/// beside it) to a program taken from a payload, and fail the first assertion.
#[test]
fn jq_sees_only_the_environment_variables_the_operator_listed() {
    if let Err(why) = mqtt_rules::Jq::built_in() {
        skip_locally_or_fail_in_ci!("{why}: {JQ_MODULE_HOW}");
    }
    let sql = "SELECT jq('[$ENV.SITE, env.SITE, $ENV.PATH, $ENV.MQTTD_RULES_JQ, ($ENV | keys)]', \
               payload) AS r FROM \"t/#\"";
    let ran = jq_rule_test(sql, "1", &[]);
    assert_eq!(
        ran.stdout, "{\"r\":[[null,null,null,null,[]]]}\n",
        "{ran:?}"
    );
    // Listed and set, listed and unset.
    let ran = jq_rule_test(sql, "1", &[("MQTTD_RULES_JQ_ENV", "SITE,REGION")]);
    assert_eq!(
        ran.stdout, "{\"r\":[[\"plant-7\",\"plant-7\",null,null,[\"SITE\"]]]}\n",
        "{ran:?}"
    );
}

/// The rules of [`jq_runs_in_the_broker_with_emqx_results_limits_and_metrics`].
const JQ_RULES: &str = r#"[rules.whole]
sql = '''SELECT jq('.', payload) AS r FROM "jq/whole"'''
actions = [{ function = "republish", args = { topic = "jq/out/whole", payload = "${r}" } }]

[rules.each]
sql = '''FOREACH jq('.a[]', payload) DO item AS x FROM "jq/each"'''
actions = [{ function = "republish", args = { topic = "jq/out/each", payload = "${x}" } }]

[rules.field]
sql = '''SELECT jq('.', payload.a, 100) AS r FROM "jq/field"'''
actions = [{ function = "republish", args = { topic = "jq/out/field", payload = "${r}" } }]

[rules.where]
sql = '''SELECT jq(payload.p, payload.i) AS r FROM "jq/where" WHERE jq('.a', payload) = [1]'''
actions = [{ function = "republish", args = { topic = "jq/out/where", payload = "${r}" } }]

[rules.env]
sql = '''SELECT jq('[$ENV.SITE, $ENV.PATH, ($ENV | keys)]', payload) AS r FROM "jq/env"'''
actions = [{ function = "republish", args = { topic = "jq/out/env", payload = "${r}" } }]

[rules.slow]
sql = '''SELECT jq('def f: f; f', payload, 40) AS r FROM "jq/slow"'''
actions = [{ function = "republish", args = { topic = "jq/out/slow", payload = "${r}" } }]

[rules.index]
sql = '''SELECT jq('.x', payload) AS r FROM "jq/index"'''
actions = [{ function = "republish", args = { topic = "jq/out/index", payload = "${r}" } }]

[rules.big]
sql = '''SELECT jq('[range(1e9)]', payload) AS r FROM "jq/big"'''
actions = [{ function = "republish", args = { topic = "jq/out/big", payload = "${r}" } }]

[rules.twice]
sql = '''SELECT jq('"x" * 600000 | length', payload) AS n, jq('"x" * 600000', payload) AS a, jq('"x" * 600000', payload) AS b FROM "jq/twice"'''
actions = [{ function = "republish", args = { topic = "jq/out/twice", payload = "${n}" } }]

[rules.once]
sql = '''SELECT jq('"x" * 600000 | length', payload) AS n, jq('"x" * 600000', payload) AS a FROM "jq/once"'''
actions = [{ function = "republish", args = { topic = "jq/out/once", payload = "${n}" } }]
"#;

/// The last line the broker logged at WARN for a failed evaluation of `rule`.
fn rule_failure(broker: &Broker, rule: &str) -> String {
    broker
        .log_lines(|l| l.contains("rule SQL failed") && l.contains(&format!("rule={rule} ")))
        .pop()
        .unwrap_or_else(|| panic!("no failure of `{rule}` logged:\n{}", broker.log.tail(40)))
}

/// ADR 0086 D4, D7, D8, D10, through the shipped binary with `[rules] jq = true`: the
/// results EMQX 6.3.1 gave for the same statements and payloads; a program taken from
/// the payload; the listed environment; the timeout, the memory cap and the output
/// budget each failing its rule, with its kind counted and its text logged, while the
/// message is still delivered and the next call is served; and the per-function and
/// per-module metrics.
///
/// The output budget is the message's: `once` returns 600 KB and passes, `twice`
/// returns it a second time in the same statement and is refused, because the first
/// call's output was charged (the budget is 1 MiB per message). A call given the whole
/// megabyte each time would pass `twice`.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one broker, one scenario per rule
async fn jq_runs_in_the_broker_with_emqx_results_limits_and_metrics() {
    if let Err(why) = mqtt_rules::Jq::built_in() {
        skip_locally_or_fail_in_ci!("{why}: {JQ_MODULE_HOW}");
    }
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), JQ_RULES);
    let setup = Setup {
        toml: "\n[rules]\njq = true\njq_env = [\"SITE\", \"REGION\"]\n".to_string(),
        env: vec![("SITE", "plant-7".to_string())],
        rules_by_env: true,
        ..Setup::default()
    };
    let broker = start(dir.path(), "jq-node", &setup).await;
    // Said once at boot: the switch is on; and the module was compiled when the rules
    // were loaded, before any message.
    assert_eq!(
        broker
            .log_lines(|l| l.contains("rules.jq is on: rules may call jq"))
            .len(),
        1,
        "{}",
        broker.log.tail(40)
    );
    assert_eq!(
        broker
            .log_lines(|l| l.contains("the rules call jq; its WebAssembly module is compiled"))
            .len(),
        1
    );
    assert_eq!(
        broker.sample(JQ_CALLS).await,
        None,
        "no call before a message"
    );

    let mut sub = Client::connect(broker.addr, "jq-sub").await;
    assert_eq!(subscribe(&mut sub, 1, "jq/#", QoS::AtMostOnce).await, [0]);
    let mut publ = Client::connect(broker.addr, "jq-pub").await;
    let mut pkid = 0u16;
    let mut publish = async |publ: &mut Client, topic: &str, payload: &str| {
        pkid += 1;
        assert_eq!(
            publish_acked(publ, topic, payload.as_bytes(), pkid).await,
            0
        );
    };

    // EMQX 6.3.1's answers (emqx_rule_sqltester, 2026-10-11).
    for (topic, payload, derived) in [
        (
            "jq/whole",
            r#"{"a":[1,2]}"#,
            vec![("jq/out/whole", r#"[{"a":[1,2]}]"#)],
        ),
        (
            "jq/each",
            r#"{"a":[1,2]}"#,
            vec![("jq/out/each", "1"), ("jq/out/each", "2")],
        ),
        (
            "jq/field",
            r#"{"a":{"b":1.0,"c":12345678901234567890123}}"#,
            vec![("jq/out/field", r#"[{"b":1.0,"c":12345678901234567890123}]"#)],
        ),
        // The program and its input from the payload, and jq deciding the WHERE.
        (
            "jq/where",
            r#"{"a":1,"p":".[] | select(. > 1)","i":[1,2,3]}"#,
            vec![("jq/out/where", "[2,3]")],
        ),
        (
            "jq/env",
            "1",
            vec![("jq/out/env", r#"[["plant-7",null,["SITE"]]]"#)],
        ),
    ] {
        publish(&mut publ, topic, payload).await;
        assert_eq!(
            next(&mut sub).await,
            got(topic, payload, QoS::AtMostOnce, false)
        );
        for (out, text) in derived {
            assert_eq!(next(&mut sub).await, got(out, text, QoS::AtMostOnce, false));
        }
    }
    // The WHERE that does not hold: both calls ran (the SELECT is evaluated first, as in
    // EMQX) and nothing is derived.
    publish(&mut publ, "jq/where", r#"{"a":2,"p":".","i":1}"#).await;
    assert_eq!(
        next(&mut sub).await,
        got(
            "jq/where",
            r#"{"a":2,"p":".","i":1}"#,
            QoS::AtMostOnce,
            false
        )
    );
    assert_eq!(
        broker.sample(&evaluations("where", "no_result")).await,
        Some(1)
    );
    assert_eq!(broker.sample(JQ_CALLS).await, Some(8));

    // Each limit fails its rule and nothing else: the message arrives, nothing is
    // derived from it, and the failure is counted by kind and logged with its text.
    for (topic, rule, kind, text) in [
        (
            "jq/slow",
            "slow",
            "timeout",
            "jq(): timeout: jq program canceled as it took too long time to execute (timeout \
             set to 40 ms)",
        ),
        (
            "jq/index",
            "index",
            "process",
            "jq(): jq_err_process: jq error: Cannot index number with string \"x\"",
        ),
        (
            "jq/big",
            "big",
            "memory",
            "jq(): sandbox_limit (memory): jq stopped: the module reached its memory limit",
        ),
        (
            "jq/twice",
            "twice",
            "output",
            "jq(): sandbox_limit (output): jq output is larger than the limit",
        ),
    ] {
        publish(&mut publ, topic, "1").await;
        assert_eq!(
            next(&mut sub).await,
            got(topic, "1", QoS::AtMostOnce, false)
        );
        assert_eq!(
            broker.sample(&evaluations(rule, "failed")).await,
            Some(1),
            "{rule}"
        );
        assert_eq!(broker.sample(&jq_errors(kind)).await, Some(1), "{rule}");
        let logged = rule_failure(&broker, rule);
        assert!(
            logged.contains(&format!("error={text}")),
            "{rule}: {logged}"
        );
    }
    // 600 KB once fits the message's megabyte; the instance that hit the memory cap was
    // replaced, and the calls after it were served.
    publish(&mut publ, "jq/once", "1").await;
    assert_eq!(
        next(&mut sub).await,
        got("jq/once", "1", QoS::AtMostOnce, false)
    );
    assert_eq!(
        next(&mut sub).await,
        got("jq/out/once", "[600000]", QoS::AtMostOnce, false)
    );
    sub.expect_silence().await;

    let metrics = broker.metrics().await;
    // 8 before, one each for slow, index and big, three for twice, two for once.
    assert_eq!(sample(&metrics, JQ_CALLS), Some(16), "{metrics}");
    assert_eq!(
        series_lines(&metrics, "mqttd_rule_function_errors_total{").len(),
        4,
        "{metrics}"
    );
    let gauge = |name: &str| {
        sample(&metrics, &format!("{name}{{module=\"jq\"}}"))
            .unwrap_or_else(|| panic!("no {name}:\n{metrics}"))
    };
    assert!(gauge("mqttd_rule_module_instances") >= 1, "{metrics}");
    assert!(
        gauge("mqttd_rule_module_memory_bytes") >= 2 << 20,
        "{metrics}"
    );
    assert!(
        gauge("mqttd_rule_module_instances_replaced_total") >= 1,
        "the instance stopped at the memory cap was not replaced:\n{metrics}"
    );
    assert!(
        series_lines(
            &metrics,
            "mqttd_rule_function_seconds_total{function=\"jq\"}"
        )
        .len()
            == 1,
        "{metrics}"
    );
}

/// One rule whose jq never ends by itself (and prints nothing, so it is the clock that
/// stops it, not the output budget) and runs under `jq/2`'s default timeout, EMQX's
/// ten seconds; and nothing on any other topic.
const JQ_TEN_SECONDS: &str = r#"[rules.endless]
sql = '''SELECT jq('def f: f; f', payload) AS r FROM "slow/#"'''
actions = [{ function = "republish", args = { topic = "never", payload = "${r}" } }]
"#;

/// One `QoS` 1 round trip on `c`: when it was sent and how long the PUBACK took.
async fn timed_publish(c: &mut Client, topic: &str, pkid: u16) -> (Instant, Duration) {
    let sent = Instant::now();
    assert_eq!(publish_acked(c, topic, b"1", pkid).await, 0);
    (sent, sent.elapsed())
}

/// ADR 0086 D6: a ten-second jq on one connection does not delay another connection's
/// publish. The broker runs on ONE runtime worker thread (`TOKIO_WORKER_THREADS=1`), so
/// a call that kept the worker would hold every connection for ten seconds; after its
/// first millisecond the call goes on under `block_in_place`, which gives the worker's
/// other tasks to another thread.
///
/// One client publishes into the endless rule at `QoS` 1; its PUBACK waits for the rule,
/// that is for the timeout. A second client publishes at `QoS` 1 the whole time. When the
/// first PUBACK arrives, the jq is known to have run for the ten seconds before it: of
/// the second client's round trips inside that window there must be many, and none may
/// have taken a second. Without the hand-off there is at most one, ten seconds long.
#[tokio::test]
async fn a_ten_second_jq_on_one_connection_does_not_delay_another_connections_publish() {
    if let Err(why) = mqtt_rules::Jq::built_in() {
        skip_locally_or_fail_in_ci!("{why}: {JQ_MODULE_HOW}");
    }
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), JQ_TEN_SECONDS);
    let setup = Setup {
        env: vec![
            ("MQTTD_RULES_JQ", "1".to_string()),
            ("TOKIO_WORKER_THREADS", "1".to_string()),
        ],
        ..Setup::default()
    };
    let broker = start(dir.path(), "jq-slow", &setup).await;
    let mut slow = Client::connect(broker.addr, "slow-pub").await;
    let mut fast = Client::connect(broker.addr, "fast-pub").await;
    let mut sub = Client::connect(broker.addr, "fast-sub").await;
    assert_eq!(subscribe(&mut sub, 1, "fast/#", QoS::AtMostOnce).await, [0]);

    let timeout = Duration::from_secs(10);
    let started = Instant::now();
    slow.publish("slow/1", b"1", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    // The other connection's publishes, each delivered to its subscriber, until the
    // slow one is answered.
    let mut trips = Vec::new();
    let mut pkid = 0u16;
    let answered = loop {
        pkid = pkid.wrapping_add(1).max(1);
        trips.push(timed_publish(&mut fast, "fast/1", pkid).await);
        assert_eq!(
            next(&mut sub).await,
            got("fast/1", "1", QoS::AtMostOnce, false)
        );
        match slow.recv_bounded(Duration::ZERO).await {
            Recv::Quiet => {}
            Recv::Packet(Packet::PubAck(ack)) => {
                assert_eq!((ack.pkid, ack.reason), (1, 0));
                break Instant::now();
            }
            other => panic!("expected the slow publish's PUBACK, got {other:?}"),
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the slow publish was never answered; log tail:\n{}",
            broker.log.tail(30)
        );
    };
    let held = answered - started;
    assert!(
        held >= timeout && held < timeout + Duration::from_secs(5),
        "the jq rule held its own publisher for {held:?}, not its ten-second timeout"
    );
    assert_eq!(broker.sample(&jq_errors("timeout")).await, Some(1));
    // The jq was running from (answered - 10 s) on.
    let during: Vec<Duration> = trips
        .iter()
        .filter(|(sent, _)| *sent + timeout >= answered)
        .map(|(_, took)| *took)
        .collect();
    let slowest = during.iter().max().copied().unwrap_or_default();
    assert!(
        during.len() >= 100 && slowest < Duration::from_secs(1),
        "while one connection's jq ran for ten seconds, another connection completed {} \
         publishes, the slowest in {slowest:?}",
        during.len()
    );
    let mut sorted = during.clone();
    sorted.sort_unstable();
    eprintln!(
        "D6 hand-off, one worker thread: {} QoS 1 round trips on another connection during a \
         ten-second jq; median {:?}, p99 {:?}, slowest {slowest:?}",
        sorted.len(),
        sorted[sorted.len() / 2],
        sorted[sorted.len() * 99 / 100],
    );
}

/// A Will into the endless rule, with a timeout the test can wait out.
const JQ_WILL: &str = r#"[rules.lastwords]
sql = '''SELECT jq('def f: f; f', payload, 3000) AS r FROM "will/#"'''
actions = [{ function = "republish", args = { topic = "never", payload = "${r}" } }]
"#;

/// ADR 0086 D6, amended 2026-10-11: the hub publishes a Will and, since ADR 0083, runs
/// its rules itself. Rules that call `jq` may run for seconds, and everything waits for
/// the hub: with such rules loaded a Will's rules run on a task of their own. Here a
/// client's Will meets a rule whose jq runs for three seconds; every publish of another
/// client goes through the hub and must be acknowledged promptly throughout. A hub
/// that evaluated the Will itself would hold one of them for the three seconds.
#[tokio::test]
async fn a_will_whose_rules_call_jq_does_not_hold_the_hub() {
    if let Err(why) = mqtt_rules::Jq::built_in() {
        skip_locally_or_fail_in_ci!("{why}: {JQ_MODULE_HOW}");
    }
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), JQ_WILL);
    let setup = Setup {
        env: vec![("MQTTD_RULES_JQ", "1".to_string())],
        ..Setup::default()
    };
    let broker = start(dir.path(), "jq-will", &setup).await;
    let mut other = Client::connect(broker.addr, "bystander").await;
    let mut watcher = Client::connect(broker.addr, "will-watcher").await;
    assert_eq!(
        subscribe(&mut watcher, 1, "will/#", QoS::AtMostOnce).await,
        [0]
    );
    let mut doomed = Client::open(broker.addr, V4).await;
    doomed
        .connect_with_will("doomed", "will/doomed", b"1")
        .await;
    // The connection is dropped without a DISCONNECT: the hub publishes the Will.
    drop(doomed);
    assert_eq!(
        next(&mut watcher).await,
        got("will/doomed", "1", QoS::AtMostOnce, false)
    );
    let started = Instant::now();
    let mut slowest = Duration::ZERO;
    let mut pkid = 0u16;
    // Publish until the Will's jq has timed out, which the broker counts when it has
    // (the metrics are read ten times a second, between publishes).
    let mut read_at = started;
    loop {
        pkid = pkid.wrapping_add(1).max(1);
        slowest = slowest.max(timed_publish(&mut other, "plain/1", pkid).await.1);
        if read_at.elapsed() < Duration::from_millis(100) {
            continue;
        }
        read_at = Instant::now();
        if broker.sample(&jq_errors("timeout")).await == Some(1) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the Will's rule never ran:\n{}",
            broker.log.tail(30)
        );
    }
    assert!(
        started.elapsed() >= Duration::from_millis(2500),
        "the Will's jq ended after {:?}, not its three seconds",
        started.elapsed()
    );
    assert!(
        slowest < Duration::from_secs(1),
        "a publish waited {slowest:?} for the hub while a Will's jq ran"
    );
    assert_eq!(
        broker.sample(&evaluations("lastwords", "failed")).await,
        Some(1)
    );
}

/// ADR 0086 D5, D13 with ADR 0084: rules that call `jq` are applied through the admin
/// API to a running broker. Before the edit the broker has `jq` switched on and no rule
/// that calls it: nothing of the runtime exists (no module compiled, no jq series). The
/// applied file is validated and loaded on the admin's own thread, which is where the
/// module is compiled; the connection that was open all along runs jq on its next
/// message. A broker with `jq` switched off refuses the same file with the reason — 422,
/// naming the switch — writes nothing and keeps its rules, through the admin API and on
/// SIGHUP alike.
#[tokio::test]
async fn jq_rules_are_applied_live_through_the_admin_api_and_refused_where_jq_is_off() {
    if let Err(why) = mqtt_rules::Jq::built_in() {
        skip_locally_or_fail_in_ci!("{why}: {JQ_MODULE_HOW}");
    }
    let compiled = "the rules call jq; its WebAssembly module is compiled";
    for jq_on in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let pki_dir = tempfile::tempdir().unwrap();
        write_rules(dir.path(), EDIT_A);
        let pki = admin_pki(pki_dir.path());
        let admin = band_addr();
        let mut env = admin_env(&pki, admin);
        if jq_on {
            env.push(("MQTTD_RULES_JQ", "1".to_string()));
        }
        let setup = Setup {
            env,
            ..Setup::default()
        };
        let broker = start(dir.path(), "jq-edit", &setup).await;
        let mut sub = Client::connect(broker.addr, "jq-edit-sub").await;
        assert_eq!(subscribe(&mut sub, 1, "out/#", QoS::AtMostOnce).await, [0]);
        assert_eq!(subscribe(&mut sub, 2, "jq/out", QoS::AtMostOnce).await, [0]);
        let mut publ = Client::connect(broker.addr, "jq-edit-pub").await;
        assert_eq!(
            broker.log_lines(|l| l.contains(compiled)),
            Vec::<String>::new()
        );

        let both = format!("{EDIT_A}\n{JQ_ONE}");
        let next_file = pki_dir.path().join("next.toml");
        std::fs::write(&next_file, &both).unwrap();
        let digest = sha256_hex(EDIT_A.as_bytes());
        let applied = admin_cli(
            &pki,
            admin,
            &[
                "rules-apply",
                &next_file.display().to_string(),
                "--if_match",
                &digest,
            ],
        );
        let on_disk = std::fs::read_to_string(dir.path().join("rules.toml")).unwrap();
        if jq_on {
            assert_eq!(applied.code, Some(0), "{applied:?}");
            assert_eq!(on_disk, both);
            assert_eq!(broker.log_lines(|l| l.contains(compiled)).len(), 1);
            publ.publish(
                "jq/in",
                br#"{"a":[1,"two"]}"#,
                QoS::AtMostOnce,
                None,
                vec![],
            )
            .await;
            assert_eq!(
                next(&mut sub).await,
                got("jq/out", r#"[1,"two"]"#, QoS::AtMostOnce, false)
            );
            broker.wait_metric(JQ_CALLS, 1).await;
        } else {
            assert_eq!(applied.code, Some(1), "{applied:?}");
            assert!(
                applied.stderr.contains("422 rules-invalid") && applied.stderr.contains(JQ_OFF),
                "{applied:?}"
            );
            assert_eq!(on_disk, EDIT_A, "a refused write wrote the file");
            // The same file put there by hand and reloaded is rejected the same way.
            write_rules(dir.path(), &both);
            broker.signal("HUP");
            let deadline = Instant::now() + Duration::from_secs(15);
            while broker
                .log_lines(|l| l.contains("reload REJECTED") && l.contains(JQ_OFF))
                .is_empty()
            {
                assert!(
                    Instant::now() < deadline,
                    "the reload was not rejected for jq being off; log tail:\n{}",
                    broker.log.tail(30)
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(
                broker.log_lines(|l| l.contains(compiled)),
                Vec::<String>::new()
            );
            assert_eq!(broker.sample(JQ_CALLS).await, None);
        }
        // The rule that was running keeps running either way.
        publ.publish("w/1", br#"{"v":"y"}"#, QoS::AtMostOnce, None, vec![])
            .await;
        assert_eq!(
            next(&mut sub).await,
            got("out/w/y", "y", QoS::AtMostOnce, false)
        );
    }
}

/// ADR 0086 D13: `mqttd --check-config` says, while `rules.jq` is on, whether this host
/// lets compiled code run — and says nothing about jq while it is off. The config is
/// valid either way (exit 0): the check often runs where the broker will not.
#[test]
fn check_config_reports_whether_this_host_can_run_jq_while_it_is_switched_on() {
    let check = |jq: Option<&str>| {
        let mut cmd = mqttd();
        cmd.arg("--check-config").env("MQTTD_DURABLE_SESSIONS", "0");
        if let Some(jq) = jq {
            cmd.env("MQTTD_RULES_JQ", jq);
        }
        run(cmd)
    };
    let ok = "config OK: defaults + MQTTD_* env overlay validates (no config file set)\n";
    for off in [None, Some("0")] {
        let ran = check(off);
        assert_eq!((ran.code, ran.stdout.as_str()), (Some(0), ok), "{ran:?}");
    }
    let ran = check(Some("1"));
    assert_eq!(ran.code, Some(0), "{ran:?}");
    let report = ran
        .stdout
        .strip_prefix(ok)
        .unwrap_or_else(|| panic!("{ran:?}"));
    match mqtt_rules::Jq::built_in() {
        // On a host that forbids executable memory this is a warning naming
        // MemoryDenyWriteExecute; the hosts the tests run on allow it.
        Ok(()) => assert!(
            report.starts_with("rules.jq is on: this host allows compiled code (jq module sha256 ")
                && report.trim_end().ends_with(')'),
            "{ran:?}"
        ),
        Err(why) => assert_eq!(
            report,
            format!(
                "warning: rules.jq is on, but {why}: a rules file that calls jq will not load\n"
            ),
            "{ran:?}"
        ),
    }
}

// ---------------------------------------------------------------------------------------
// Operator modules: [functions.<name>] in the rules file (ADR 0086 D9, task 0086-T3)
// ---------------------------------------------------------------------------------------

/// Writes `wasm` as `dir/<file>` and returns its sha256: what a rules file pins it by.
#[cfg(feature = "jq")]
fn place_module(dir: &Path, file: &str, wasm: &[u8]) -> String {
    let path = dir.join(file);
    std::fs::create_dir_all(path.parent().expect("a file in a directory"))
        .expect("create the modules directory");
    std::fs::write(path, wasm).expect("write the module");
    sha256_hex(wasm)
}

/// A `[functions.<name>]` table.
#[cfg(feature = "jq")]
fn module_table(name: &str, file: &str, sha256: &str, functions: &[&str], more: &str) -> String {
    let functions: Vec<String> = functions.iter().map(|f| format!("\"{f}\"")).collect();
    format!(
        "[functions.{name}]\nwasm = \"{file}\"\nsha256 = \"{sha256}\"\nfunctions = [{}]\n{more}\n",
        functions.join(", ")
    )
}

/// A rule that republishes `fields` of its statement's output on `to`.
#[cfg(feature = "jq")]
fn module_rule(id: &str, sql: &str, to: &str, payload: &str) -> String {
    format!(
        "[rules.{id}]\nsql = '{sql}'\nactions = [{{ function = \"republish\", args = {{ topic = \
         \"{to}\", payload = \"{payload}\" }} }}]\n"
    )
}

#[cfg(feature = "jq")]
fn function_calls(function: &str) -> String {
    format!(r#"mqttd_rule_function_calls_total{{function="{function}"}}"#)
}

#[cfg(feature = "jq")]
fn function_errors(function: &str, kind: &str) -> String {
    format!(r#"mqttd_rule_function_errors_total{{function="{function}",kind="{kind}"}}"#)
}

/// ADR 0086 D9, D7, D10 through the shipped binary, with `rules.jq` left off: a rules
/// file declares a module by path and sha256, and that declaration is all it takes. Its
/// functions are called like any other; a binary crosses as its bytes and any other
/// value as JSON; a call that runs past the table's `timeout_ms` fails its rule, counted
/// by kind and logged with its text, while the message is still acknowledged and the
/// next call is served; the module's own refusal fails the rule with its message; and
/// the per-function and per-module series are exported.
///
/// Then the pin: the module file is replaced on disk under the unchanged rules file and
/// the broker is told to reload. The reload is rejected with the reason, and the module
/// that was loaded keeps running. A broker that took whatever the path now holds fails
/// the last derived message.
#[cfg(feature = "jq")]
#[tokio::test]
#[allow(clippy::too_many_lines)] // one broker, one scenario per rule
async fn a_declared_module_runs_in_the_broker_under_its_limits_and_stays_pinned() {
    use mqtt_wasm_sandbox::test_module;
    let dir = tempfile::tempdir().unwrap();
    let sha = place_module(
        dir.path(),
        "modules/site.wasm",
        &test_module::plugin("1.0", b"j42"),
    );
    let rules = module_table(
        "site",
        "modules/site.wasm",
        &sha,
        &["which", "ident", "loop_forever", "err"],
        "timeout_ms = 50",
    ) + &module_rule(
        "value",
        "SELECT which() AS n, ident(payload.tag) AS tag, ident(payload.list) AS list FROM \
         \"m/value\"",
        "m/out/value",
        "${n} ${tag} ${list}",
    ) + &module_rule(
        "hang",
        "SELECT loop_forever() AS n FROM \"m/hang\"",
        "m/out/hang",
        "${n}",
    ) + &module_rule(
        "own",
        "SELECT err() AS n FROM \"m/own\"",
        "m/out/own",
        "${n}",
    );
    write_rules(dir.path(), &rules);
    let broker = start(dir.path(), "module-node", &Setup::default()).await;

    let mut sub = Client::connect(broker.addr, "module-sub").await;
    assert_eq!(
        subscribe(&mut sub, 1, "m/out/#", QoS::AtMostOnce).await,
        [0]
    );
    let mut publ = Client::connect(broker.addr, "module-pub").await;
    assert_eq!(
        publish_acked(
            &mut publ,
            "m/value",
            br#"{"tag":"pump-7","list":[1,2.5,{"k":123456789012345678901234567890}]}"#,
            1
        )
        .await,
        0
    );
    assert_eq!(
        next(&mut sub).await,
        got(
            "m/out/value",
            r#"42 pump-7 [1,2.5,{"k":123456789012345678901234567890}]"#,
            QoS::AtMostOnce,
            false
        )
    );

    // The timeout fails the rule, not the message: the publisher is answered.
    assert_eq!(publish_acked(&mut publ, "m/hang", b"1", 2).await, 0);
    assert_eq!(publish_acked(&mut publ, "m/own", b"1", 3).await, 0);
    broker
        .wait_metric(&function_errors("loop_forever", "timeout"), 1)
        .await;
    broker
        .wait_metric(&function_errors("err", "module"), 1)
        .await;
    let metrics = broker.metrics().await;
    for (series, want) in [
        (function_calls("which"), 1),
        (function_calls("ident"), 2),
        (function_calls("loop_forever"), 1),
        (function_calls("err"), 1),
        (
            r#"mqttd_rule_evaluations_total{rule="hang",result="failed"}"#.to_string(),
            1,
        ),
        (
            r#"mqttd_rule_evaluations_total{rule="own",result="failed"}"#.to_string(),
            1,
        ),
        (
            r#"mqttd_rule_module_instances_replaced_total{module="site"}"#.to_string(),
            1,
        ),
        (
            r#"mqttd_rule_module_instances{module="site"}"#.to_string(),
            1,
        ),
    ] {
        assert_eq!(sample(&metrics, &series), Some(want), "{series}\n{metrics}");
    }
    assert_eq!(
        series_lines(
            &metrics,
            r#"mqttd_rule_function_errors_total{function="which""#
        ),
        Vec::<&str>::new()
    );
    for text in [
        "loop_forever(): sandbox_limit (timeout): the call ran past its 50 ms (timeout_ms of \
         [functions.site])",
        "err(): status 7: nope",
    ] {
        assert_eq!(
            broker.log_lines(|l| l.contains(text)).len(),
            1,
            "{text}\n{}",
            broker.log.tail(40)
        );
    }
    sub.expect_silence().await;

    // Another file under the pinned name: the reload is refused, what runs stays.
    std::fs::write(
        dir.path().join("modules/site.wasm"),
        test_module::plugin("2.0", b"j99"),
    )
    .unwrap();
    broker.signal("HUP");
    broker
        .wait_log(&format!(
            " WARN mqttd::reload: security reload REJECTED — keeping the running policy \
             trigger=\"signal\" error=rules: functions `site`: wasm = \"modules/site.wasm\": \
             the file does not have the sha256 the table pins ({sha}); `sha256sum` prints the \
             file's"
        ))
        .await;
    assert_eq!(
        publish_acked(&mut publ, "m/value", br#"{"tag":"t","list":[]}"#, 4).await,
        0
    );
    assert_eq!(
        next(&mut sub).await,
        got("m/out/value", "42 t []", QoS::AtMostOnce, false)
    );
}

/// The acceptance row of 0086-T3, through `mqttd --check-rules`: "a wrong hash, a missing
/// export, another ABI version, a forbidden import, or a name that collides with a
/// built-in or an EMQX function fails the load with the reason" — and so does a path
/// that leaves the rules file's directory. Each is exit 1 with the one line an operator
/// reads. A file whose module passes is listed with what it loaded.
#[cfg(feature = "jq")]
#[test]
#[allow(clippy::too_many_lines)] // one case per reason
fn check_rules_refuses_a_module_that_fails_a_check_and_says_why() {
    use mqtt_wasm_sandbox::test_module::{self, Shape};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rules.toml").display().to_string();
    let described = |shape: Shape| {
        test_module::module(&Shape {
            description: test_module::description("1"),
            ..shape
        })
    };
    let good = test_module::plugin("1.4", b"j1");
    let sha = place_module(dir.path(), "good.wasm", &good);
    let outside = tempfile::tempdir().unwrap();
    place_module(outside.path(), "elsewhere.wasm", &good);
    let elsewhere = outside.path().join("elsewhere.wasm").display().to_string();
    let wrong = "f".repeat(64);
    let declare = |file: &str, wasm: Vec<u8>| {
        let sha = place_module(dir.path(), file, &wasm);
        module_table("m", file, &sha, &["which"], "")
    };
    for (rules, reason) in [
        (
            module_table("m", "good.wasm", &wrong, &["which"], ""),
            format!(
                "wasm = \"good.wasm\": the file does not have the sha256 the table pins \
                 ({wrong}); `sha256sum` prints the file's"
            ),
        ),
        (
            declare(
                "abi.wasm",
                described(Shape {
                    version: 2,
                    ..Shape::default()
                }),
            ),
            "the module speaks ABI version 2, this host 1".to_string(),
        ),
        (
            declare(
                "import.wasm",
                described(Shape {
                    import: Some(("env", "system")),
                    ..Shape::default()
                }),
            ),
            "the module imports env.system, which is not provided".to_string(),
        ),
        (
            module_table("m", "good.wasm", &sha, &["upper"], ""),
            "a module cannot provide `upper`: a built-in function has that name, and a module \
             must not change what a rule written for EMQX means"
                .to_string(),
        ),
        (
            module_table("m", "good.wasm", &sha, &["proc_dict_put"], ""),
            "a module cannot provide `proc_dict_put`: EMQX has a function of that name, and a \
             module must not change what a rule written for EMQX means"
                .to_string(),
        ),
        (
            module_table("m", &elsewhere, &sha, &["which"], ""),
            format!(
                "wasm = {elsewhere:?}: a module is named by a relative path under the rules \
                 file's directory, without `..`"
            ),
        ),
    ] {
        write_rules(dir.path(), &rules);
        let ran = cli(&["--check-rules", &path]);
        assert_eq!(
            (ran.code, ran.stdout.as_str(), ran.stderr.as_str()),
            (
                Some(1),
                "",
                format!("rules INVALID ({path}): functions `m`: {reason}\n").as_str()
            ),
            "{rules}"
        );
    }
    // A missing export: the runtime's own words follow ours.
    write_rules(
        dir.path(),
        &declare(
            "nocall.wasm",
            described(Shape {
                without_call: true,
                ..Shape::default()
            }),
        ),
    );
    let ran = cli(&["--check-rules", &path]);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert!(
        ran.stderr.starts_with(&format!(
            "rules INVALID ({path}): functions `m`: the module does not export the function \
             ABI: "
        )) && ran.stderr.contains("mqttd_call"),
        "{ran:?}"
    );

    // And one that passes: what was loaded, then the rules.
    let rules = module_table("site", "good.wasm", &sha, &["which", "ident"], "")
        + "[rules.r]\nsql = 'SELECT which() AS n FROM \"t/#\"'\n";
    write_rules(dir.path(), &rules);
    let ran = cli(&["--check-rules", &path]);
    assert_eq!(
        (ran.code, ran.stdout.as_str(), ran.stderr.as_str()),
        (
            Some(0),
            format!(
                "rules OK: {path}: 1 rule(s), 1 enabled, sha256 {}\n  module site (test 1.4, \
                 sha256 {sha}): ident, which\n  r (enabled): FROM \"t/#\", 0 action(s)\n",
                sha256_hex(rules.as_bytes())
            )
            .as_str(),
            ""
        ),
        "{ran:?}"
    );
    // `--rule-test` runs a statement with the functions the configured rules file
    // declares; without the file the name is unknown, as for any other.
    let sql = "SELECT which() + 1 AS n FROM \"t/#\"";
    let mut with_file = mqttd();
    with_file
        .args(["--rule-test", "--sql", sql])
        .env("MQTTD_RULES_FILE", &path);
    let ran = run(with_file);
    assert_eq!(
        (ran.code, ran.stdout.as_str()),
        (Some(0), "{\"n\":2}\n"),
        "{ran:?}"
    );
    let ran = cli(&["--rule-test", "--sql", sql]);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert!(ran.stderr.contains("unknown function which()"), "{ran:?}");
}

/// How far the test module's `spin` must count to run for at least `want` on this
/// machine: a short call is timed three times, and the count scaled from the fastest,
/// so that a machine busy with other tests can only make the real call longer. The
/// count is what makes a call long enough to be caught in flight and short enough to
/// end well inside its timeout, whatever the machine.
#[cfg(feature = "jq")]
fn spin_count_for(want: Duration) -> u32 {
    use mqtt_wasm_sandbox::{test_module, Grants, Limits, ModulePool, Sandbox};
    const PROBE: u32 = 2_000_000;
    let wasm = test_module::plugin_spinning("probe", b"j0", PROBE);
    let sandbox =
        Sandbox::load(&wasm, Limits::default(), Grants::default()).expect("the test module loads");
    let pool = ModulePool::new(sandbox, 1, false);
    // Once to make the instance, once to time the call alone.
    for _ in 0..2 {
        pool.call("which", &[], Duration::from_mins(1), 64)
            .expect("the test module answers");
    }
    let took = (0..3)
        .map(|_| {
            let started = Instant::now();
            pool.call("spin", &[], Duration::from_mins(1), 64)
                .expect("the probe spins to its end");
            started.elapsed().as_secs_f64()
        })
        .fold(f64::INFINITY, f64::min)
        .max(1e-6);
    let count = f64::from(PROBE) * want.as_secs_f64() / took;
    assert!(
        count < f64::from(u32::MAX),
        "this machine counts {PROBE} in {took:.6} s: too fast for a spin of {want:?}"
    );
    // Truncation is the intent, and the value was just checked to fit.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let count = count as u32;
    count.max(PROBE)
}

/// The `modules` of `mqttd --admin rules --json`.
#[cfg(feature = "jq")]
fn admin_modules(pki: &AdminPki, admin: SocketAddr) -> Vec<serde_json::Value> {
    let ran = admin_cli(pki, admin, &["rules", "--json"]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    let answer: serde_json::Value =
        serde_json::from_str(&ran.stdout).unwrap_or_else(|e| panic!("{e}: {ran:?}"));
    answer["modules"]
        .as_array()
        .unwrap_or_else(|| panic!("no modules in {answer}"))
        .clone()
}

/// The acceptance row of 0086-T3: "a live edit swaps a module without dropping in-flight
/// evaluations" and "the admin API lists modules, functions, limits and instance
/// memory", through the shipped binary and the operator's own commands.
///
/// One publisher's message is in a call of the old module that runs for two seconds
/// or more. While it runs, the admin API shows the module with one instance in a call;
/// a rules writer applies a rules file that pins another module; a second publisher's
/// message is answered by the new module at once; and only then does the first call
/// return — with the OLD module's answer, its message republished and its publisher
/// acknowledged. A broker that tore the old module down at the swap loses the first
/// derived message; one that made the swap wait for the call delivers the two in the
/// other order; one that kept answering from the old module fails the second value.
#[cfg(feature = "jq")]
#[tokio::test]
#[allow(clippy::too_many_lines)] // one scenario, in the order it happens
async fn a_live_edit_swaps_a_module_while_a_publish_in_flight_finishes_on_the_old_one() {
    use mqtt_wasm_sandbox::test_module;
    let dir = tempfile::tempdir().unwrap();
    let pki_dir = tempfile::tempdir().unwrap();
    let spin = spin_count_for(Duration::from_secs(2));
    let old_sha = place_module(
        dir.path(),
        "modules/v1.wasm",
        &test_module::plugin_spinning("1.0", b"j1", spin),
    );
    let new_sha = place_module(
        dir.path(),
        "modules/v2.wasm",
        &test_module::plugin("2.0", b"j2"),
    );
    let rules = |file: &str, sha: &str| {
        module_table(
            "site",
            file,
            sha,
            &["spin", "which"],
            "timeout_ms = 10000\nmemory_mib = 4",
        ) + &module_rule(
            "slow",
            "SELECT spin() AS v FROM \"m/slow\"",
            "m/out/slow",
            "${v}",
        ) + &module_rule(
            "quick",
            "SELECT which() AS v FROM \"m/quick\"",
            "m/out/quick",
            "${v}",
        )
    };
    let (v1, v2) = (
        rules("modules/v1.wasm", &old_sha),
        rules("modules/v2.wasm", &new_sha),
    );
    write_rules(dir.path(), &v1);
    let pki = admin_pki(pki_dir.path());
    let admin = band_addr();
    let setup = Setup {
        env: admin_env(&pki, admin),
        ..Setup::default()
    };
    let broker = start(dir.path(), "swap-node", &setup).await;
    broker
        .wait_log(&format!(
            "serving the admin API (mTLS; ADR 0081) bind={admin} viewers=0 operators=1 \
             rules_writers=1 peers=false"
        ))
        .await;

    // What the admin API lists before any call: the module, its functions, its limits
    // and grants, and no instance yet.
    let listed = admin_modules(&pki, admin);
    assert_eq!(
        listed,
        vec![serde_json::json!({
            "name": "site",
            "module": "test",
            "version": "1.0",
            "sha256": old_sha,
            "wasm": "modules/v1.wasm",
            "functions": [
                {"name": "spin", "min_args": 0, "max_args": 8},
                {"name": "which", "min_args": 0, "max_args": 8},
            ],
            "limits": {"timeout_ms": 10000, "memory_bytes": 4 << 20},
            "grants": {"clock": false, "env_count": 0, "env": []},
            "fresh_instance": false,
            "instances": {"count": 0, "in_call": 0, "memory_bytes": 0, "replaced": 0},
        })]
    );

    let mut sub = Client::connect(broker.addr, "swap-sub").await;
    assert_eq!(
        subscribe(&mut sub, 1, "m/out/#", QoS::AtMostOnce).await,
        [0]
    );
    let mut slow = Client::connect(broker.addr, "swap-slow").await;
    let mut quick = Client::connect(broker.addr, "swap-quick").await;

    // The first publish goes into the old module and stays there.
    slow.publish("m/slow", b"1", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let modules = admin_modules(&pki, admin);
        if modules[0]["instances"]["in_call"] == 1 {
            // An instance and its memory, while it is in the call.
            assert_eq!(modules[0]["instances"]["count"], 1, "{modules:?}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the slow call never showed as in flight: {modules:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The live edit, by a rules writer, while that call runs.
    let next_file = pki_dir.path().join("v2.toml");
    std::fs::write(&next_file, &v2).unwrap();
    let applied = admin_cli(
        &pki,
        admin,
        &[
            "rules-apply",
            &next_file.display().to_string(),
            "--if_match",
            &sha256_hex(v1.as_bytes()),
        ],
    );
    assert_eq!(applied.code, Some(0), "{applied:?}");
    assert!(
        applied
            .stdout
            .lines()
            .any(|l| l.starts_with("applied ") && l.ends_with(" true")),
        "{applied:?}"
    );

    // From here on a message is answered by the new module, at once; the first call is
    // still running.
    assert_eq!(publish_acked(&mut quick, "m/quick", b"1", 1).await, 0);
    assert_eq!(
        next(&mut sub).await,
        got("m/out/quick", "2", QoS::AtMostOnce, false)
    );
    let listed = admin_modules(&pki, admin);
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(
        (
            &listed[0]["version"],
            &listed[0]["sha256"],
            &listed[0]["wasm"]
        ),
        (
            &serde_json::json!("2.0"),
            &serde_json::json!(new_sha),
            &serde_json::json!("modules/v2.wasm")
        ),
    );
    assert_eq!(
        (
            &listed[0]["instances"]["count"],
            &listed[0]["instances"]["in_call"],
            &listed[0]["instances"]["memory_bytes"]
        ),
        (
            &serde_json::json!(1),
            &serde_json::json!(0),
            &serde_json::json!(65_536)
        ),
        "the new module's one warm instance and its page of memory: {listed:?}"
    );
    // The table view of the same answer.
    let table = admin_cli(&pki, admin, &["rules"]);
    assert_eq!(table.code, Some(0), "{table:?}");
    let row: Vec<&str> = table
        .stdout
        .lines()
        .find(|l| l.starts_with("site "))
        .unwrap_or_else(|| panic!("no row for the module: {table:?}"))
        .split_whitespace()
        .collect();
    assert_eq!(
        row,
        [
            "site",
            "2.0",
            &new_sha[..12],
            "spin,",
            "which",
            "10000",
            "4194304",
            "1",
            "0",
            "65536"
        ],
        "{table:?}"
    );

    // The call that was in flight ends on the module it began on: its answer is the
    // old one's, its message is republished, its publisher acknowledged.
    assert_eq!(
        next(&mut sub).await,
        got("m/out/slow", "1", QoS::AtMostOnce, false)
    );
    match slow.recv().await {
        Packet::PubAck(ack) => assert_eq!((ack.pkid, ack.reason), (1, 0)),
        other => panic!("expected the slow publish's PUBACK, got {other:?}"),
    }
    let metrics = broker.metrics().await;
    assert_eq!(
        series_lines(&metrics, "mqttd_rule_function_errors_total"),
        Vec::<&str>::new(),
        "no call failed"
    );
    assert_eq!(sample(&metrics, &function_calls("spin")), Some(1));
    assert_eq!(sample(&metrics, &function_calls("which")), Some(1));
    // The same connection's next message runs the new rules.
    assert_eq!(publish_acked(&mut slow, "m/quick", b"1", 2).await, 0);
    assert_eq!(
        next(&mut sub).await,
        got("m/out/quick", "2", QoS::AtMostOnce, false)
    );
}

/// Where `examples/rule-plugins/build.sh` leaves the two worked examples.
#[cfg(feature = "jq")]
fn example_modules() -> Option<(Vec<u8>, Vec<u8>)> {
    let out = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/rule-plugins/out");
    Some((
        std::fs::read(out.join("geo.wasm")).ok()?,
        std::fs::read(out.join("telemetry.wasm")).ok()?,
    ))
}

#[cfg(feature = "jq")]
const EXAMPLES_HOW: &str = "the example modules are not built: run examples/rule-plugins/build.sh \
     (zig 0.16.0 and `rustup target add wasm32-unknown-unknown`)";

/// The acceptance row of 0086-T3: "a Rust and a C example build and run in CI". The two
/// modules of `examples/rule-plugins` (built by its `build.sh`, which CI runs first) are
/// declared in a rules file and called by rules in the shipped binary: the Rust one's
/// distances, the C one's checksum, and the C one's two grants — the wall clock, and one
/// environment variable that the table names and the broker's `rules.functions_env`
/// lists. `PATH` is in the broker's environment and not in the list, so a table naming
/// it does not load; and without the grants the same module reads no clock and no
/// environment, which `--rule-test` shows.
#[cfg(feature = "jq")]
#[tokio::test]
#[allow(clippy::too_many_lines)] // two modules, each function once
async fn the_worked_example_modules_run_in_the_broker_with_their_grants() {
    let Some((geo, telemetry)) = example_modules() else {
        skip_locally_or_fail_in_ci!("{EXAMPLES_HOW}");
    };
    let dir = tempfile::tempdir().unwrap();
    let geo_sha = place_module(dir.path(), "plugins/geo.wasm", &geo);
    let telemetry_sha = place_module(dir.path(), "plugins/telemetry.wasm", &telemetry);
    let tables = |grants: &str| {
        module_table(
            "geo",
            "plugins/geo.wasm",
            &geo_sha,
            &["geo_distance", "geo_within"],
            "",
        ) + &module_table(
            "telemetry",
            "plugins/telemetry.wasm",
            &telemetry_sha,
            &["crc16_modbus", "site_label", "clock_ms"],
            grants,
        )
    };
    // Copenhagen's Rådhuspladsen, and how far a reported position is from it.
    let rules = tables("clock = true\nenv = [\"SITE\"]")
        + &module_rule(
            "near",
            "SELECT geo_distance(payload.lat, payload.lon, 55.6761, 12.5683) AS metres, \
             geo_within(payload.lat, payload.lon, 55.6761, 12.5683, 5000) AS near FROM \
             \"ex/position\"",
            "ex/out/position",
            "${metres} ${near}",
        )
        + &module_rule(
            "frame",
            "SELECT crc16_modbus(payload) AS crc, site_label(clientid) AS label, clock_ms() AS \
             at FROM \"ex/frame\"",
            "ex/out/frame",
            "${crc} ${label} ${at}",
        );
    write_rules(dir.path(), &rules);
    let setup = Setup {
        toml: "\n[rules]\nfunctions_env = [\"SITE\"]\n".to_string(),
        env: vec![("SITE", "plant-7".to_string())],
        rules_by_env: true,
        ..Setup::default()
    };
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let broker = start(dir.path(), "example-node", &setup).await;
    let mut sub = Client::connect(broker.addr, "example-sub").await;
    assert_eq!(
        subscribe(&mut sub, 1, "ex/out/#", QoS::AtMostOnce).await,
        [0]
    );
    let mut publ = Client::connect(broker.addr, "example-pub").await;

    // Rust: Aarhus is about 156 km from Copenhagen, and not within 5 km of it; the
    // same point is 0 m from itself.
    assert_eq!(
        publish_acked(
            &mut publ,
            "ex/position",
            br#"{"lat":56.1629,"lon":10.2039}"#,
            1
        )
        .await,
        0
    );
    let (topic, payload, ..) = next(&mut sub).await;
    assert_eq!(topic, "ex/out/position");
    let (metres, near) = payload.split_once(' ').unwrap();
    let metres: f64 = metres.parse().unwrap_or_else(|_| panic!("{payload}"));
    assert!((156_500.0..157_500.0).contains(&metres), "{payload}");
    assert_eq!(near, "false");
    assert_eq!(
        publish_acked(
            &mut publ,
            "ex/position",
            br#"{"lat":55.6761,"lon":12.5683}"#,
            2
        )
        .await,
        0
    );
    assert_eq!(
        next(&mut sub).await,
        got("ex/out/position", "0.0 true", QoS::AtMostOnce, false)
    );
    // Its own refusal fails the rule, with its message.
    assert_eq!(
        publish_acked(&mut publ, "ex/position", br#"{"lat":91,"lon":0}"#, 3).await,
        0
    );
    broker
        .wait_metric(&function_errors("geo_distance", "module"), 1)
        .await;
    assert_eq!(
        broker
            .log_lines(
                |l| l.contains("geo_distance(): status 1: a latitude is from -90 to 90, got 91")
            )
            .len(),
        1,
        "{}",
        broker.log.tail(40)
    );

    // C: the CRC-16/MODBUS check value of "123456789" is 0x4B37; the label carries the
    // granted variable; the clock is the wall clock.
    assert_eq!(
        publish_acked(&mut publ, "ex/frame", b"123456789", 4).await,
        0
    );
    let (topic, payload, ..) = next(&mut sub).await;
    assert_eq!(topic, "ex/out/frame");
    let fields: Vec<&str> = payload.split(' ').collect();
    assert_eq!(fields[..2], ["19255", "plant-7/example-pub"], "{payload}");
    let at: u128 = fields[2].parse().unwrap_or_else(|_| panic!("{payload}"));
    assert!(at >= before && at < before + 600_000, "{payload}");

    // A variable the broker's configuration does not list cannot be granted by a rules
    // file, whatever the broker's environment holds.
    let path = dir.path().join("rules.toml").display().to_string();
    write_rules(dir.path(), &tables("env = [\"SITE\", \"PATH\"]"));
    let mut check = mqttd();
    check
        .args(["--check-rules", &path])
        .env("MQTTD_RULES_FUNCTIONS_ENV", "SITE");
    let ran = run(check);
    assert_eq!(
        (ran.code, ran.stderr.as_str()),
        (
            Some(1),
            format!(
                "rules INVALID ({path}): functions `telemetry`: env names \"PATH\", which the \
                 broker's rules.functions_env (MQTTD_RULES_FUNCTIONS_ENV) does not list: a \
                 module sees only variables the broker's own configuration allows\n"
            )
            .as_str()
        ),
        "{ran:?}"
    );
    // Without the grants the module reads no environment and no clock.
    write_rules(dir.path(), &tables(""));
    let mut test = mqttd();
    test.args([
        "--rule-test",
        "--sql",
        "SELECT site_label('pump') AS label, clock_ms() AS at, crc16_modbus(payload) AS crc \
         FROM \"t/#\"",
        "--payload",
        "123456789",
    ])
    .env("MQTTD_RULES_FILE", &path)
    .env("MQTTD_RULES_FUNCTIONS_ENV", "SITE")
    .env("SITE", "plant-7");
    let ran = run(test);
    assert_eq!(
        (ran.code, ran.stdout.as_str()),
        (Some(0), "{\"label\":\"/pump\",\"at\":0,\"crc\":19255}\n"),
        "{ran:?}"
    );
}

/// A rules file that keeps state (ADR 0085): a declared namespace two rules share, and
/// EMQX's `kv_store_*`.
const STATE_RULES: &str = r#"[state.fleet]
max_keys = 1000
default_ttl = "1h"

[rules.count]
state = "fleet"
sql = '''SELECT clientid AS c, kv_incr('n:' + clientid) AS n, kv_store_put('last', clientid) AS p FROM "in/#" WHERE payload.pass = true'''
actions = [{ function = "republish", args = { topic = "out/${c}", payload = "${.}" } }]

[rules.read]
state = "fleet"
sql = '''SELECT kv_get('n:' + clientid) AS n, kv_store_get('last') AS last FROM "ask"'''
actions = [{ function = "republish", args = { topic = "out/ask", payload = "${.}" } }]
"#;

/// ADR 0085 through the binary: a broker started on a rules file that keeps state loads
/// it without any switch, and its rules remember from one publish to the next: a call
/// in a `SELECT` field counts a message the `WHERE` turns away (EMQX's order), another
/// rule reads the same namespace, and EMQX's `kv_store_*` table is written and read. A
/// broker that forgot between messages, or wrote only for messages that pass, answers
/// another count.
#[tokio::test]
async fn a_broker_keeps_rule_state_between_publishes() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), STATE_RULES);
    let broker = start(dir.path(), "state", &Setup::default()).await;
    let mut sub = Client::connect(broker.addr, "state-sub").await;
    assert_eq!(
        subscribe(&mut sub, 1, "out/#", QoS::AtMostOnce).await,
        vec![0]
    );
    let mut dev = Client::connect(broker.addr, "dev-7").await;
    for payload in [r#"{"pass":false}"#, r#"{"pass":false}"#, r#"{"pass":true}"#] {
        dev.publish("in/a", payload.as_bytes(), QoS::AtMostOnce, None, vec![])
            .await;
    }
    assert_eq!(
        next(&mut sub).await,
        got(
            "out/dev-7",
            r#"{"c":"dev-7","n":3,"p":true}"#,
            QoS::AtMostOnce,
            false
        )
    );
    dev.publish("ask", b"{}", QoS::AtMostOnce, None, vec![])
        .await;
    assert_eq!(
        next(&mut sub).await,
        got(
            "out/ask",
            r#"{"n":3,"last":"dev-7"}"#,
            QoS::AtMostOnce,
            false
        )
    );
    broker
        .wait_log(
            "rules: the rules keep state between messages, in memory on this node (ADR 0085): \
             it starts empty and is bounded by rules.state_max_bytes namespaces=2 \
             max_bytes=268435456",
        )
        .await;
}

/// `mqttd --check-rules` loads a file that keeps state as the broker would, warns about
/// one that calls both families of functions, and refuses it where rule state is
/// switched off, naming the key (`rules.state_max_bytes`), as the broker refuses to
/// boot on it. Neither `kv_store_*` nor `[state.<name>]` is an unknown function or key
/// any more.
#[test]
fn check_rules_loads_rule_state_and_refuses_it_where_it_is_switched_off() {
    let dir = tempfile::tempdir().unwrap();
    write_rules(dir.path(), STATE_RULES);
    let path = dir.path().join("rules.toml").display().to_string();
    let ran = cli(&["--check-rules", &path]);
    assert_eq!(ran.code, Some(0), "{ran:?}");
    assert_eq!(
        ran.stderr,
        "warning: the file calls both EMQX's kv_store_* functions and mqttd's kv_* functions: \
         they do not share keys (kv_get does not read what kv_store_put wrote)\n"
    );
    assert!(
        ran.stdout
            .starts_with(&format!("rules OK: {path}: 2 rule(s), 2 enabled, sha256 ")),
        "{ran:?}"
    );

    let mut off = mqttd();
    off.args(["--check-rules", &path])
        .env("MQTTD_RULES_STATE_MAX_BYTES", "0");
    let ran = run(off);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert_eq!(ran.stdout, "");
    assert_eq!(
        ran.stderr,
        format!(
            "rules INVALID ({path}): rules file: [state.fleet]: rule state is switched off: \
             `rules.state_max_bytes` is 0 (set it above 0 for rules to keep state; see \
             RULES.md)\n"
        )
    );
    // A file that only calls a state function, with nothing declared: the rule is named.
    write_rules(
        dir.path(),
        "[rules.put]\nsql = 'SELECT kv_store_put(clientid, payload) AS p FROM \"t/#\"'\n",
    );
    assert_eq!(cli(&["--check-rules", &path]).code, Some(0));
    let mut off = mqttd();
    off.args(["--check-rules", &path])
        .env("MQTTD_RULES_STATE_MAX_BYTES", "0");
    let ran = run(off);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert_eq!(
        ran.stderr,
        format!(
            "rules INVALID ({path}): rule `put`: rule state is switched off: \
             `rules.state_max_bytes` is 0 (set it above 0 for rules to keep state; see \
             RULES.md) (line 1, column 8, near `kv_store_put(clientid, p`)\n"
        )
    );
    // What this release does with a mode it does not have yet.
    assert_check_rules_refuses(
        "[state.shared]\nconsistency = \"eventual\"\n",
        "rules file: [state.shared]: consistency = \"eventual\" is not available yet: this \
         release has \"local\" namespaces only (ADR 0085)",
    );
    assert_check_rules_refuses(
        "[state.shared]\nconsistency = \"strong\"\n",
        "rules file: [state.shared]: consistency = \"strong\" is not available: it is a later \
         phase (ADR 0085 D5); use \"local\"",
    );
}

/// `mqttd --rule-test` runs every state function, on a store of its own: within one
/// statement a later call sees an earlier one, and nothing is left for the next run
/// (ADR 0085 D11: a test never touches a running store, and there is none to touch
/// here). The `kv_store_*` values are EMQX 6.3.1's for the same calls.
#[test]
fn rule_test_runs_the_state_functions_on_a_store_of_its_own() {
    let sql = "SELECT kv_store_put('a', 1) AS p, kv_store_get('a') AS g, \
               kv_store_get('nope', 'dflt') AS d, kv_store_del('a') AS x, \
               kv_set('k', payload.v, '10m') AS s, kv_get('k') AS k, kv_ttl('k') AS t, \
               kv_set_once('o', 1) AS o1, kv_set_once('o', 2) AS o2, \
               kv_incr('n') AS n1, kv_incr('n', 5) AS n2, \
               kv_max('hi', 3) AS hi, kv_min('lo', 3) AS lo, \
               kv_add('s', 'b') AS a1, kv_add('s', 'a') AS a2, kv_get('s') AS set, \
               kv_held('h', payload.v.on) AS held, kv_changed('c', 1) AS c1, \
               kv_changed('c', 1) AS c2, kv_del('k') AS del, kv_get('k') AS gone \
               FROM \"t/#\"";
    let want = "{\"p\":true,\"g\":1,\"d\":\"dflt\",\"x\":true,\"s\":{\"on\":true},\
                \"k\":{\"on\":true},\"t\":600,\"o1\":1,\"o2\":1,\"n1\":1,\"n2\":6,\"hi\":3,\
                \"lo\":3,\"a1\":1,\"a2\":2,\"set\":[\"a\",\"b\"],\"held\":0,\"c1\":true,\
                \"c2\":false,\"del\":true,\"gone\":\"undefined\"}\n";
    for _ in 0..2 {
        let ran = cli(&[
            "--rule-test",
            "--sql",
            sql,
            "--payload",
            r#"{"v":{"on":true}}"#,
        ]);
        assert_eq!((ran.code, ran.stdout.as_str()), (Some(0), want), "{ran:?}");
        assert_eq!(ran.stderr, "");
    }
    // A refused call fails the statement with the reason, and where rule state is
    // switched off the statement does not load.
    let ran = cli(&[
        "--rule-test",
        "--sql",
        "SELECT kv_set('k', 1) AS a, kv_incr('k') AS b FROM \"t/#\"",
    ]);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert_eq!(
        ran.stderr,
        "rule test FAILED: kv_incr(): wrong_type: the key holds a value of type `lww`, and \
         this is an operation on type `sum` (kv_del deletes a key of any type)\n"
    );
    let mut off = mqttd();
    off.args([
        "--rule-test",
        "--sql",
        "SELECT kv_get('k') AS a FROM \"t/#\"",
    ])
    .env("MQTTD_RULES_STATE_MAX_BYTES", "0");
    let ran = run(off);
    assert_eq!(ran.code, Some(1), "{ran:?}");
    assert!(
        ran.stderr.starts_with(
            "rule test FAILED: rule state is switched off: `rules.state_max_bytes` is 0"
        ),
        "{ran:?}"
    );
}
