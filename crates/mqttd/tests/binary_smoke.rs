//! Process-level smoke test: launch the real `mqttd` binary (not the in-process
//! harness) and drive a pub/sub round-trip against it. This is the only test that
//! exercises `main.rs` — env-var config parsing, the plaintext listener wiring, and
//! the accept loop — end to end. See `docs/TEST-PLAN.md`.

mod common;

use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::time::Duration;

use common::Client;
use mqtt_codec::QoS;

/// Kills the spawned broker process when the test ends (including on panic).
struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Reserve an ephemeral port, then release it for the broker to bind. A small race
/// window, acceptable on loopback for a short-lived test.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Wait for the broker to accept TCP connections, or panic after ~5s.
async fn wait_until_listening(addr: SocketAddr) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the mqttd binary never started listening on {addr}");
}

#[tokio::test]
async fn binary_serves_a_plaintext_pubsub_roundtrip() {
    let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();

    // Launch the actual binary as a child process with a plaintext listener.
    let child = Command::new(env!("CARGO_BIN_EXE_mqttd"))
        .env("MQTTD_NODE_ID", "smoke")
        .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("RUST_LOG", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn the mqttd binary");
    let _guard = ChildGuard(child);

    wait_until_listening(addr).await;

    // A full pub/sub round-trip through the real server process.
    let mut sub = Client::connect(addr, "smoke-sub").await;
    sub.subscribe(1, "smoke/+", QoS::AtMostOnce).await;

    let mut pubr = Client::connect(addr, "smoke-pub").await;
    pubr.publish("smoke/test", b"alive", QoS::AtMostOnce, None, vec![])
        .await;

    let p = sub.expect_publish().await;
    assert_eq!(p.topic, "smoke/test");
    assert_eq!(&p.payload[..], b"alive");
}
