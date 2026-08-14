//! Process-level smoke test: launch the real `mqttd` binary (not the in-process
//! harness) and drive a pub/sub round-trip against it. This is the only test that
//! exercises `main.rs` — env-var config parsing, the plaintext listener wiring, and
//! the accept loop — end to end. See `docs/TEST-PLAN.md`.

mod common;

use std::io::Read as _;
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

/// Issue #240: durable-on (the default) with no `MQTTD_DATA_DIR` is a hard startup
/// error — quorum-of-RAM loses acked messages on a correlated restart, and a warning
/// log is not a substitute for refusing the configuration. The refusal must name
/// both ways out.
#[test]
fn durable_on_with_no_data_dir_refuses_to_start() {
    let addr = format!("127.0.0.1:{}", free_port());
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    // Hermetic: strip any MQTTD_* the runner carries (a data dir or the opt-in in the
    // ambient env would make this pass for the wrong reason).
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            cmd.env_remove(k);
        }
    }
    let child = cmd
        .env("MQTTD_NODE_ID", "refuse-ephemeral")
        .env("MQTTD_PLAINTEXT_BIND", &addr)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn the mqttd binary");
    let mut guard = ChildGuard(child);
    // Poll instead of .output(): on a regression the broker BOOTS and stays up, and an
    // unbounded wait would wedge the suite instead of failing it.
    let mut status = None;
    for _ in 0..100 {
        if let Some(s) = guard.0.try_wait().expect("try_wait") {
            status = Some(s);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let Some(status) = status else {
        panic!("mqttd stayed up: durable-on with no data dir must refuse to start (#240)");
    };
    assert!(
        !status.success(),
        "expected a non-zero exit for the refused configuration, got {status:?}"
    );
    let mut stderr = String::new();
    guard
        .0
        .stderr
        .take()
        .expect("stderr piped")
        .read_to_string(&mut stderr)
        .unwrap();
    for remedy in ["MQTTD_DATA_DIR", "MQTTD_ALLOW_EPHEMERAL_DURABILITY"] {
        assert!(
            stderr.contains(remedy),
            "the refusal must name {remedy}; stderr was: {stderr}"
        );
    }
}

/// Issue #240, the other half: the explicit opt-in boots — and the EPHEMERAL warning
/// still fires. The flag permits the mode; it must never silence the loud register.
#[tokio::test]
async fn the_ephemeral_opt_in_boots_and_still_warns() {
    let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            cmd.env_remove(k);
        }
    }
    let child = cmd
        .env("MQTTD_NODE_ID", "opted-ephemeral")
        .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
        .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
        .env("RUST_LOG", "warn")
        // The tracing subscriber writes to STDOUT; that is where the warning lands.
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn the mqttd binary");
    let mut guard = ChildGuard(child);
    wait_until_listening(addr).await;
    // The broker is up (the opt-in worked); kill it and read the captured log.
    let _ = guard.0.kill();
    let mut logs = String::new();
    guard
        .0
        .stdout
        .take()
        .expect("stdout piped")
        .read_to_string(&mut logs)
        .unwrap();
    assert!(
        logs.contains("EPHEMERAL durability"),
        "the opt-in must not silence the EPHEMERAL warning; logs were: {logs}"
    );
}

#[tokio::test]
async fn binary_serves_a_plaintext_pubsub_roundtrip() {
    let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();

    // Launch the actual binary as a child process with a plaintext listener.
    let child = Command::new(env!("CARGO_BIN_EXE_mqttd"))
        .env("MQTTD_NODE_ID", "smoke")
        .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        // Ephemeral durability needs the explicit opt-in (#240); tests are its use case.
        .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
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
        .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
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
        .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
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

/// CONNECT from a specific loopback source address with username/password.
/// `Some(code)` = the broker answered CONNACK `code`; `None` = the connection was
/// closed with no CONNACK (refused at accept).
async fn connect_from(
    source: &str,
    addr: SocketAddr,
    user: &str,
    pass: &str,
) -> Option<(u8, mqtt_net::FrameReader<tokio::net::tcp::OwnedReadHalf>)> {
    use mqtt_codec::{packet::Connect, Packet, ProtocolVersion};
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind(format!("{source}:0").parse().unwrap()).unwrap();
    let stream = socket.connect(addr).await.ok()?;
    let (rh, wh) = stream.into_split();
    let mut reader = mqtt_net::FrameReader::new(rh, ProtocolVersion::V311);
    let mut writer = mqtt_net::FrameWriter::new(wh, ProtocolVersion::V311);
    writer
        .send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: ProtocolVersion::V311,
            clean_session: true,
            keep_alive: 30,
            client_id: format!("pen-{user}"),
            last_will: None,
            username: Some(user.to_string()),
            password: Some(pass.as_bytes().to_vec().into()),
        }))
        .await
        .ok()?;
    match tokio::time::timeout(Duration::from_secs(2), reader.next_packet()).await {
        Ok(Ok(Some(Packet::ConnAck(a)))) => Some((a.code, reader)),
        _ => None, // closed with no CONNACK, or timed out
    }
}

/// ADR 0041 T2 — the auth-failure penalty box, through the real binary: two bad
/// passwords from one address penalize it (its next connection is closed at
/// accept, even with GOOD credentials), a different address authenticates
/// normally throughout, and the penalty decays back to admission.
#[tokio::test]
async fn repeated_auth_failures_penalize_the_source_address_then_decay() {
    use argon2::password_hash::{PasswordHasher, SaltString};
    use argon2::Argon2;
    // Per-SOURCE-ADDRESS isolation is the point, and that needs a second loopback
    // address. Linux has all of 127/8 on lo by default; stock macOS has only
    // 127.0.0.1 (issue #217) — probe, and SKIP with a note rather than fail on an
    // environmental impossibility. CI is Linux, so coverage there is unconditional;
    // `sudo ifconfig lo0 alias 127.0.0.2 up` runs it on a Mac.
    if std::net::TcpListener::bind(("127.0.0.2", 0)).is_err() {
        eprintln!(
            "SKIPPED repeated_auth_failures_penalize_the_source_address_then_decay: \
             127.0.0.2 is not bindable on this host (stock macOS loopback carries \
             only 127.0.0.1 — issue #217; `sudo ifconfig lo0 alias 127.0.0.2 up` \
             enables the test locally)"
        );
        return;
    }
    let salt = SaltString::encode_b64(b"penalty-salt-b").unwrap();
    let phc = Argon2::default()
        .hash_password(b"right-pw", &salt)
        .unwrap()
        .to_string();
    let pw_path = std::env::temp_dir().join(format!("mqttd-pen-{}.pw", std::process::id()));
    std::fs::write(&pw_path, format!("alice:{phc}\n")).unwrap();

    let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_mqttd"))
        .env("MQTTD_NODE_ID", "penalty")
        .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
        .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
        .env("MQTTD_PASSWORD_FILE", &pw_path)
        .env("MQTTD_AUTH_PENALTY_THRESHOLD", "2")
        .env("MQTTD_AUTH_PENALTY_DECAY_SECS", "1")
        .env("RUST_LOG", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn the mqttd binary");
    let _guard = ChildGuard(child);
    wait_until_listening(addr).await;
    let _cleanup = scopeguard(pw_path.clone());

    // Two failed authentications from 127.0.0.2: each gets its CONNACK 0x04.
    for _ in 0..2 {
        let (code, _r) = connect_from("127.0.0.2", addr, "alice", "wrong-pw")
            .await
            .expect("pre-penalty failures still get a CONNACK");
        assert_eq!(code, 0x04, "bad credentials CONNACK");
    }

    // The address is now penalized: even CORRECT credentials are closed at
    // accept, with no CONNACK — no Argon2 work is spent on it.
    assert!(
        connect_from("127.0.0.2", addr, "alice", "right-pw")
            .await
            .is_none(),
        "a penalized address must be closed at accept"
    );

    // A different address is unaffected throughout.
    let (code, _keep) = connect_from("127.0.0.1", addr, "alice", "right-pw")
        .await
        .expect("a different address must authenticate normally");
    assert_eq!(code, 0);

    // The penalty decays (threshold 2, one strike per second): poll until the
    // penalized address is admitted again.
    for i in 0..60 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Some((code, _r)) = connect_from("127.0.0.2", addr, "alice", "right-pw").await {
            assert_eq!(code, 0, "the recovered address must authenticate");
            return;
        }
        assert!(i < 59, "the penalty never decayed");
    }
}

/// Remove `path` when dropped (test cleanup that survives panics).
fn scopeguard(path: std::path::PathBuf) -> impl Drop {
    struct G(std::path::PathBuf);
    impl Drop for G {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    G(path)
}
