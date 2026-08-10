//! The memory watermark, against the real binary (ADR 0041 T8).
//!
//! The unit tests in `memory_watch` prove the transition logic with an injected sampler,
//! and the hub tests prove brownout refuses growth. Neither proves the two are *wired
//! together* — that a real broker, whose real RSS is over a real `MQTTD_MEMORY_MAX_BYTES`,
//! actually refuses a new session and says so in its metrics. That wiring is the whole
//! feature, and it is exactly the kind of thing that can be half-connected while every
//! unit test stays green.
//!
//! **Linux only**, deliberately and visibly. RSS comes from `/proc/self/status`; released
//! binaries are Linux-only (see the README's supported platforms). On other systems the
//! watcher logs loudly that the watermark is NOT being enforced and exits, so there is
//! nothing to assert — and a test that quietly passed there would be claiming coverage
//! this platform does not have.

#![cfg(target_os = "linux")]

mod common;
mod proc_common;

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use mqtt_codec::{packet::Connect, Packet, ProtocolVersion};
use proc_common::free_tcp_port;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

const V5: ProtocolVersion = ProtocolVersion::V5;
/// MQTT 5.0 reason code 0x97 — the honest answer to "your session would exceed a quota".
const QUOTA_EXCEEDED: u8 = 0x97;

struct Broker(Child);

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Boot the real binary with `memory_max` as its RSS watermark (`None` = unset).
async fn start_broker(
    memory_max: Option<u64>,
) -> (Broker, SocketAddr, SocketAddr, tempfile::TempDir) {
    let client: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let health: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let dir = tempfile::tempdir().expect("temp data dir");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            cmd.env_remove(k);
        }
    }
    cmd.env("MQTTD_NODE_ID", "mem-node")
        .env("MQTTD_PLAINTEXT_BIND", client.to_string())
        .env("MQTTD_HEALTH_BIND", health.to_string())
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("MQTTD_DATA_DIR", dir.path())
        .env("RUST_LOG", "off");
    if let Some(max) = memory_max {
        cmd.env("MQTTD_MEMORY_MAX_BYTES", max.to_string());
    }
    let child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn mqttd");
    let broker = Broker(child);

    for _ in 0..300 {
        if TcpStream::connect(client).await.is_ok() {
            return (broker, client, health, dir);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("mqttd never listened on {client}");
}

/// Scrape `/metrics` and return the body.
async fn metrics(health: SocketAddr) -> String {
    let mut stream = TcpStream::connect(health).await.expect("connect to health");
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .await
        .expect("send request");
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.expect("read response");
    String::from_utf8_lossy(&body).into_owned()
}

/// Wait until `pred` holds over the metrics body, or give up and return the last body so
/// the failure shows what was actually exported.
async fn metrics_until(health: SocketAddr, pred: impl Fn(&str) -> bool) -> String {
    let mut last = String::new();
    for _ in 0..300 {
        last = metrics(health).await;
        if pred(&last) {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    last
}

/// CONNECT as a v5 client with a fresh session; return the CONNACK reason code.
async fn connack_code(addr: SocketAddr, client_id: &str) -> u8 {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (rh, wh) = stream.into_split();
    let mut reader = mqtt_net::FrameReader::new(rh, V5);
    let mut writer = mqtt_net::FrameWriter::new(wh, V5);
    writer
        .send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V5,
            clean_session: true,
            keep_alive: 30,
            client_id: client_id.to_string(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await
        .expect("send CONNECT");
    match tokio::time::timeout(Duration::from_secs(10), reader.next_packet()).await {
        Ok(Ok(Some(Packet::ConnAck(ack)))) => ack.code,
        other => panic!("expected a CONNACK, got {other:?}"),
    }
}

/// A watermark of 1 KiB is under any real process's RSS, so the broker is browned out
/// from its first poll. That must show in the metrics AND refuse a new session — the two
/// halves of "the watermark is wired up".
#[tokio::test]
async fn an_rss_over_the_watermark_browns_out_and_refuses_new_sessions() {
    let (_broker, client, health, _dir) = start_broker(Some(1024)).await;

    let body = metrics_until(health, |b| b.contains("brownout{axis=\"memory\"} 1")).await;
    assert!(
        body.contains("brownout{axis=\"memory\"} 1"),
        "the memory axis must report brownout when RSS exceeds the watermark; metrics were:\n{}",
        memory_lines(&body)
    );

    // RSS is exported, and is a plausible number rather than a zero standing in for
    // "could not read" — a zero would sit under every watermark and never fire.
    let rss = gauge(&body, "process_resident_bytes").expect("process_resident_bytes exported");
    assert!(
        rss > 1024 * 1024,
        "a running broker should hold more than 1 MiB; got {rss}"
    );
    assert_eq!(
        gauge(&body, "memory_max_bytes"),
        Some(1024),
        "the configured watermark must be exported so headroom is computable"
    );

    assert_eq!(
        connack_code(client, "newcomer").await,
        QUOTA_EXCEEDED,
        "under memory brownout a NEW session must be refused (ADR 0041)"
    );
}

/// The control: with no watermark configured, the same broker exports its RSS and is not
/// browned out. Without this the test above would pass against a broker that refused
/// every connection for some entirely unrelated reason.
#[tokio::test]
async fn without_a_watermark_the_same_broker_accepts_sessions() {
    let (_broker, client, health, _dir) = start_broker(None).await;

    let body = metrics_until(health, |b| b.contains("process_resident_bytes")).await;
    assert!(
        !body.contains("brownout{axis=\"memory\"} 1"),
        "no watermark configured must mean no memory brownout; metrics were:\n{}",
        memory_lines(&body)
    );
    assert_eq!(
        gauge(&body, "memory_max_bytes").unwrap_or(0),
        0,
        "an unset watermark exports 0, so PromQL can tell 'off' from 'very small'"
    );

    assert_eq!(
        connack_code(client, "newcomer").await,
        0x00,
        "with no watermark the same client connects normally"
    );
}

/// Read a bare (unlabelled) gauge value out of a Prometheus exposition body.
///
/// Integer, not float: every gauge asserted here is a byte count exported from an `i64`,
/// so a decimal point would mean the metric changed shape — and comparing floats for
/// equality is the wrong tool for a byte count regardless.
fn gauge(body: &str, name: &str) -> Option<i64> {
    body.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(name)?.trim().parse::<i64>().ok())
}

/// Just the memory/brownout lines, for a readable assertion message.
fn memory_lines(body: &str) -> String {
    body.lines()
        .filter(|l| {
            !l.starts_with('#')
                && (l.contains("brownout") || l.contains("resident") || l.contains("memory_max"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}
