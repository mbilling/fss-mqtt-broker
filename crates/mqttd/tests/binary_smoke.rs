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

/// An over-cap connection is refused at accept: the socket is closed with no
/// CONNACK (no TLS/MQTT work is spent on it).
async fn assert_refused_at_accept(addr: SocketAddr) {
    assert!(
        Client::connect_v311_within(addr, "over-cap", true, Duration::from_secs(2))
            .await
            .is_none(),
        "an over-cap connection must be closed at accept, never CONNACKed"
    );
}

/// Poll until a fresh connect succeeds (a freed slot takes a moment to recycle:
/// the broker must observe the disconnect and drop the permit), or panic.
async fn connect_when_slot_frees(addr: SocketAddr, id: &str) -> Client {
    for _ in 0..50 {
        if let Some((c, _)) =
            Client::connect_v311_within(addr, id, true, Duration::from_millis(300)).await
        {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("a freed admission slot was never reusable");
}

/// ADR 0041 T1 — the global connection cap, through the real binary: with
/// `MQTTD_MAX_CONNECTIONS=2`, two clients connect and work, the third is closed
/// at accept (no CONNACK), and a slot freed by a disconnect is reusable.
#[tokio::test]
async fn max_connections_cap_refuses_at_accept_and_recovers() {
    let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_mqttd"))
        .env("MQTTD_NODE_ID", "cap")
        .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("MQTTD_MAX_CONNECTIONS", "2")
        .env("RUST_LOG", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn the mqttd binary");
    let _guard = ChildGuard(child);
    wait_until_listening(addr).await;

    // The readiness probes above consumed slots transiently; connect the two
    // holders with the tolerant variant.
    let mut first = connect_when_slot_frees(addr, "cap-1").await;
    let _second = connect_when_slot_frees(addr, "cap-2").await;

    // Third concurrent connection: refused at accept.
    assert_refused_at_accept(addr).await;

    // The capped broker keeps serving its admitted clients.
    first.subscribe(1, "cap/t", QoS::AtMostOnce).await;
    first
        .publish("cap/t", b"still-served", QoS::AtMostOnce, None, vec![])
        .await;
    assert_eq!(&first.expect_publish().await.payload[..], b"still-served");

    // A freed slot is reusable.
    drop(first);
    let _third = connect_when_slot_frees(addr, "cap-3").await;
}

/// ADR 0041 T1 — the per-source-IP cap, through the real binary: with
/// `MQTTD_MAX_CONNECTIONS_PER_IP=1`, a second connection from the same address
/// is refused at accept while the first stays served, and the slot recycles.
#[tokio::test]
async fn per_ip_cap_refuses_a_second_connection_from_the_same_address() {
    let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_mqttd"))
        .env("MQTTD_NODE_ID", "ipcap")
        .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("MQTTD_MAX_CONNECTIONS_PER_IP", "1")
        .env("RUST_LOG", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn the mqttd binary");
    let _guard = ChildGuard(child);
    wait_until_listening(addr).await;

    let mut only = connect_when_slot_frees(addr, "ip-1").await;
    // Everything in this test comes from 127.0.0.1: the second is refused.
    assert_refused_at_accept(addr).await;

    // The admitted client is untouched by the refusal.
    only.subscribe(1, "ip/t", QoS::AtMostOnce).await;
    only.publish("ip/t", b"mine", QoS::AtMostOnce, None, vec![])
        .await;
    assert_eq!(&only.expect_publish().await.payload[..], b"mine");

    drop(only);
    let _next = connect_when_slot_frees(addr, "ip-2").await;
}
