//! `mqttd --hash-password` (ADR 0004 step 6, ADR 0051 onboarding): the hash it prints
//! must be one the **running broker** accepts.
//!
//! Password authentication was documented long before it was reachable. The broker
//! verifies Argon2id PHC hashes and `MQTTD_PASSWORD_FILE` wants `username:hash` lines,
//! but nothing shipped could produce one — and `mosquitto_passwd` output is a different
//! format, so the migration path dead-ended at the same place. An operator following
//! the documentation had to write an Argon2id hasher before they could turn on auth.
//!
//! These tests drive the real binary end to end — hash with the CLI, write the file,
//! boot the broker against it, CONNECT — because the only claim worth testing is that
//! the whole path works. Asserting the CLI merely *prints something Argon2id-shaped*
//! would pass while the broker rejected every login.

mod common;

use std::io::Write as _;
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use mqtt_codec::{packet::Connect, Packet, ProtocolVersion};
use tokio::net::TcpStream;

mod proc_common;
use proc_common::free_tcp_port;

const V4: ProtocolVersion = ProtocolVersion::V311;

/// MQTT 3.1.1 CONNACK return codes (table 3.1).
const ACCEPTED: u8 = 0x00;
const BAD_CREDENTIALS: u8 = 0x04;

fn mqttd() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    // Hermetic: strip any MQTTD_* the runner carries, so each case controls its own overlay.
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            c.env_remove(k);
        }
    }
    c
}

/// Run `mqttd --hash-password [username]` with `password` on stdin; return stdout, trimmed.
fn hash_password(password: &str, username: Option<&str>) -> String {
    let mut cmd = mqttd();
    cmd.arg("--hash-password");
    if let Some(u) = username {
        cmd.arg(u);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mqttd --hash-password");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(password.as_bytes())
        .expect("write the password to stdin");
    let out = child.wait_with_output().expect("wait");
    assert!(
        out.status.success(),
        "--hash-password exited {:?}; stderr={}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

struct Broker(Child);

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Boot the real binary with `password_file` as its only credential source, and wait
/// until it is listening.
async fn start_broker(password_file: &std::path::Path) -> (Broker, SocketAddr) {
    let addr: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let child = mqttd()
        .env("MQTTD_NODE_ID", "pwcli")
        .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
        .env("MQTTD_PASSWORD_FILE", password_file)
        // Anonymous stays OFF: the password file is the whole gate under test.
        .env("RUST_LOG", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn mqttd");
    let broker = Broker(child);
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_ok() {
            return (broker, addr);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("mqttd never started listening on {addr}");
}

/// CONNECT with `username`/`password` and return the CONNACK return code.
async fn connack_code(addr: SocketAddr, username: &str, password: &str) -> u8 {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (rh, wh) = stream.into_split();
    let mut reader = mqtt_net::FrameReader::new(rh, V4);
    let mut writer = mqtt_net::FrameWriter::new(wh, V4);
    writer
        .send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 30,
            client_id: "pw-client".to_string(),
            last_will: None,
            username: Some(username.to_string()),
            password: Some(bytes::Bytes::from(password.as_bytes().to_vec())),
        }))
        .await
        .expect("send CONNECT");
    match tokio::time::timeout(Duration::from_secs(10), reader.next_packet()).await {
        Ok(Ok(Some(Packet::ConnAck(ack)))) => ack.code,
        other => panic!("expected a CONNACK, got {other:?}"),
    }
}

/// The claim: a line printed by `--hash-password` is a line the broker authenticates.
#[tokio::test]
async fn a_cli_generated_line_authenticates_against_the_running_broker() {
    let password = "correct horse battery staple";
    let line = hash_password(password, Some("alice"));
    assert!(
        line.starts_with("alice:$argon2id$"),
        "expected a `username:argon2id-hash` line, got: {line}"
    );

    let dir = tempfile::tempdir().expect("temp dir");
    let pw_file = dir.path().join("passwd");
    std::fs::write(&pw_file, format!("{line}\n")).expect("write the password file");

    let (_broker, addr) = start_broker(&pw_file).await;

    assert_eq!(
        connack_code(addr, "alice", password).await,
        ACCEPTED,
        "the password that was hashed must be accepted by the broker"
    );
    // Without this the test above would pass against a broker that accepted anything.
    assert_eq!(
        connack_code(addr, "alice", "wrong password").await,
        BAD_CREDENTIALS,
        "a wrong password must still be refused"
    );
    assert_eq!(
        connack_code(addr, "mallory", password).await,
        BAD_CREDENTIALS,
        "an unknown username must be refused"
    );
}

/// A password with spaces and a trailing-newline-free pipe (`printf %s`) must hash to
/// something that verifies: the CLI strips at most the shell's own trailing newline, and
/// nothing else. Over-trimming would produce a hash that silently fails to log in.
#[tokio::test]
async fn spaces_and_punctuation_survive_the_round_trip() {
    let password = "  s p a c e s  and #punctuation$ ";
    let hash = hash_password(password, None);
    assert!(
        hash.starts_with("$argon2id$"),
        "with no username, the bare hash is printed; got: {hash}"
    );

    let dir = tempfile::tempdir().expect("temp dir");
    let pw_file = dir.path().join("passwd");
    std::fs::write(&pw_file, format!("bob:{hash}\n")).expect("write the password file");

    let (_broker, addr) = start_broker(&pw_file).await;
    assert_eq!(
        connack_code(addr, "bob", password).await,
        ACCEPTED,
        "leading/trailing spaces inside the password must be preserved by the CLI"
    );
    assert_eq!(
        connack_code(addr, "bob", password.trim()).await,
        BAD_CREDENTIALS,
        "the trimmed password is a DIFFERENT password and must not authenticate"
    );
}

/// An empty password is a usage error, not a hash of "".
#[test]
fn an_empty_password_is_refused_with_exit_2() {
    let mut child = mqttd()
        .arg("--hash-password")
        .arg("alice")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    drop(child.stdin.take()); // EOF with nothing written
    let out = child.wait_with_output().expect("wait");
    assert_eq!(out.status.code(), Some(2), "empty input is a usage error");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("empty password"),
        "the error must say what went wrong; stderr was: {stderr}"
    );
}
