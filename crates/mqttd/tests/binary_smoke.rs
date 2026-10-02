//! Process-level smoke test: launch the real `mqttd` binary (not the in-process
//! harness) and drive a pub/sub round-trip against it. This is the only test that
//! exercises `main.rs` — env-var config parsing, the plaintext listener wiring, and
//! the accept loop — end to end. See `docs/TEST-PLAN.md`.

mod common;
mod listen_wait;
mod proc_common;

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

/// A port for the broker to bind, from the shared test band (`proc_common`), not the
/// kernel's ephemeral range. A port released from the ephemeral range is handed straight
/// back out as the source port of an outgoing connect, and these tests make hundreds of
/// connects: under parallel load the broker then lost its port before binding it.
fn free_port() -> u16 {
    proc_common::free_tcp_port()
}

/// Spawn the broker `make` builds for `addr`, on a fresh band port, until it reports
/// binding that address; a broker that exits first lost its port to another process and
/// is retried (#827). The returned [`listen_wait::Log`] keeps collecting its stdout.
async fn spawn_smoke(
    mut make: impl FnMut(SocketAddr) -> Command,
) -> (ChildGuard, listen_wait::Log, SocketAddr) {
    let (child, log, addr) = listen_wait::spawn_listening_logged(|| {
        let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        (make(addr), vec![addr], addr)
    })
    .await;
    (ChildGuard(child), log, addr)
}

/// As [`spawn_smoke`], for a broker whose stdout goes to a log FILE (a test that reads
/// the log while the broker runs). `make` gets `n` fresh band addresses and the log file
/// to send stdout to; `RUST_LOG` must pass `mqttd`'s info lines. Returns the broker, the
/// log file and the addresses.
async fn spawn_smoke_to_file(
    n: usize,
    mut make: impl FnMut(&[SocketAddr], std::fs::File) -> Command,
) -> (ChildGuard, tempfile::NamedTempFile, Vec<SocketAddr>) {
    let mut failures = Vec::new();
    for attempt in 1..=3 {
        let addrs: Vec<SocketAddr> = (0..n)
            .map(|_| format!("127.0.0.1:{}", free_port()).parse().unwrap())
            .collect();
        let log = tempfile::NamedTempFile::new().expect("broker log file");
        let sink = log.reopen().expect("broker log handle");
        let child = make(&addrs, sink)
            .spawn()
            .expect("failed to spawn the mqttd binary");
        let mut guard = ChildGuard(child);
        match listen_wait::wait_logged_file(
            &mut guard.0,
            log.path(),
            &addrs,
            listen_wait::BIND_TIMEOUT,
        )
        .await
        {
            Ok(()) => return (guard, log, addrs),
            Err(why) => {
                eprintln!("attempt {attempt}: {why}");
                failures.push(why);
            }
        }
    }
    panic!(
        "mqttd failed to bind in 3 attempts on fresh ports:\n{}",
        failures.join("\n---\n")
    );
}

/// Run a CLI invocation with unusable config and an unopened data path. A
/// regression that boots instead of exiting is killed by the bounded wait.
fn cli_exit_before_startup(args: &[&str]) -> (std::process::ExitStatus, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("must-not-be-created");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (key, _) in std::env::vars() {
        if key.starts_with("MQTTD_") {
            cmd.env_remove(key);
        }
    }
    let child = cmd
        .args(args)
        .env("MQTTD_CONFIG", dir.path().join("missing.toml"))
        .env("MQTTD_DATA_DIR", &data)
        .env(
            "MQTTD_PLAINTEXT_BIND",
            listener.local_addr().unwrap().to_string(),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut guard = ChildGuard(child);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = guard.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "CLI invocation started a broker or hung: {args:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut stdout = String::new();
    let mut stderr = String::new();
    guard
        .0
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    guard
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(!data.exists(), "CLI invocation touched storage: {args:?}");
    (status, stdout, stderr)
}

#[test]
fn malformed_cli_exits_two_before_config_or_storage_startup() {
    for args in [
        vec!["start"],
        vec!["--check-confg"],
        vec!["--check-config", "stray"],
        vec!["--pid", "123"],
        vec!["--url", "127.0.0.1:1"],
        vec!["--config", "--version"],
        vec!["--help", "--unknown"],
        vec!["--version", "--unknown"],
        vec!["--check-config", "--hash-password"],
        vec!["--probe", "readyz"],
        vec!["--hash-password", "alice", "stray"],
    ] {
        let (status, stdout, stderr) = cli_exit_before_startup(&args);
        assert_eq!(status.code(), Some(2), "{args:?}: {stdout}\n{stderr}");
        assert!(
            stderr.contains("mqttd:") && stderr.contains("mqttd --help"),
            "{args:?}: {stderr}"
        );
        assert!(
            !stderr.contains("missing.toml"),
            "argument errors must precede config loading: {stderr}"
        );
    }
}

#[test]
fn help_and_version_still_exit_successfully_without_loading_config() {
    for arg in ["--help", "-h", "--version", "-V"] {
        let (status, stdout, stderr) = cli_exit_before_startup(&[arg]);
        assert!(status.success(), "{arg}: {stderr}");
        assert!(
            stdout.contains(concat!("mqttd ", env!("CARGO_PKG_VERSION"))),
            "{stdout}"
        );
        if matches!(arg, "--help" | "-h") {
            for option in ["--config", "--url", "--pid", "--timeout"] {
                assert!(stdout.contains(option), "help omitted {option}: {stdout}");
            }
        }
    }
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
    let (mut guard, log, _addr) = spawn_smoke(|addr| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
        for (k, _) in std::env::vars() {
            if k.starts_with("MQTTD_") {
                cmd.env_remove(k);
            }
        }
        cmd.env("MQTTD_NODE_ID", "opted-ephemeral")
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("RUST_LOG", "mqttd=info")
            // The tracing subscriber writes to STDOUT; that is where the warning lands.
            .stderr(Stdio::null());
        cmd
    })
    .await;
    // The broker is up (the opt-in worked); kill it and read the captured log.
    let _ = guard.0.kill();
    let _ = guard.0.wait();
    let logs = log.complete(Duration::from_secs(5)).await;
    assert!(
        logs.contains("EPHEMERAL durability"),
        "the opt-in must not silence the EPHEMERAL warning; logs were: {logs}"
    );
}

#[tokio::test]
async fn binary_serves_a_plaintext_pubsub_roundtrip() {
    // Launch the actual binary as a child process with a plaintext listener.
    let (_guard, _log, addr) = spawn_smoke(|addr| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
        cmd.env("MQTTD_NODE_ID", "smoke")
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            // Ephemeral durability needs the explicit opt-in (#240); tests are its use case.
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("RUST_LOG", "mqttd=info")
            .stderr(Stdio::null());
        cmd
    })
    .await;

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
    let (_guard, _log, addr) = spawn_smoke(|addr| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
        cmd.env("MQTTD_NODE_ID", "cap")
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_MAX_CONNECTIONS", "2")
            .env("RUST_LOG", "mqttd=info")
            .stderr(Stdio::null());
        cmd
    })
    .await;

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
    let (_guard, _log, addr) = spawn_smoke(|addr| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
        cmd.env("MQTTD_NODE_ID", "ipcap")
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_MAX_CONNECTIONS_PER_IP", "1")
            .env("RUST_LOG", "mqttd=info")
            .stderr(Stdio::null());
        cmd
    })
    .await;

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
    use argon2::password_hash::phc::Salt;
    use argon2::password_hash::PasswordHasher;
    use argon2::Argon2;
    // Per-SOURCE-ADDRESS isolation is the point, and that needs a second loopback
    // address. Linux has all of 127/8 on lo by default; stock macOS has only
    // 127.0.0.1 (issue #217) — probe, and SKIP with a note rather than fail on an
    // environmental impossibility. CI is Linux, so coverage there is unconditional;
    // `sudo ifconfig lo0 alias 127.0.0.2 up` runs it on a Mac.
    if std::net::TcpListener::bind(("127.0.0.2", 0)).is_err() {
        crate::skip_locally_or_fail_in_ci!(
            "127.0.0.2 is not bindable on this host, so per-source-address isolation cannot \
             be exercised (stock macOS loopback carries only 127.0.0.1 — issue #217; \
             `sudo ifconfig lo0 alias 127.0.0.2 up` enables the test locally). CI runs on \
             Linux, which has all of 127/8 on lo, so this must never be taken there."
        );
    }
    let salt = Salt::new(b"penalty-salt-b").unwrap();
    let phc = Argon2::default()
        .hash_password_with_salt(b"right-pw", &salt)
        .unwrap()
        .to_string();
    let pw_path = std::env::temp_dir().join(format!("mqttd-pen-{}.pw", std::process::id()));
    std::fs::write(&pw_path, format!("alice:{phc}\n")).unwrap();

    let (_guard, _log, addr) = spawn_smoke(|addr| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
        cmd.env("MQTTD_NODE_ID", "penalty")
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_PASSWORD_FILE", &pw_path)
            .env("MQTTD_AUTH_PENALTY_THRESHOLD", "2")
            .env("MQTTD_AUTH_PENALTY_DECAY_SECS", "1")
            .env("RUST_LOG", "mqttd=info")
            .stderr(Stdio::null());
        cmd
    })
    .await;
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

/// ADR 0066 T3: a graceful stop CLOSES the audit chain — the last audit record is
/// `audit.shutdown`, carrying the closing head. This is what lets a SIEM enforce
/// "every chain ends with a shutdown record, and every genesis follows one": a
/// chain that just stops is a crash or a suppression, either worth an alert. The
/// genesis line at boot is asserted too, so the pair the invariant needs — open
/// announcement, closing record — is pinned end to end on the real binary.
#[tokio::test]
async fn a_graceful_stop_closes_the_audit_chain() {
    let (mut guard, log, _addr) = spawn_smoke(|addr| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
        for (k, _) in std::env::vars() {
            if k.starts_with("MQTTD_") {
                cmd.env_remove(k);
            }
        }
        cmd.env("MQTTD_NODE_ID", "audit-close")
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_DURABLE_SESSIONS", "0")
            .env("MQTTD_SHUTDOWN_GRACE", "5")
            // The tracing subscriber writes to STDOUT; capture that, not stderr.
            .env("RUST_LOG", "info")
            .stderr(Stdio::null());
        cmd
    })
    .await;

    let pid = guard.0.id();
    let sent = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("spawn kill");
    assert!(sent.success(), "kill -TERM failed");

    // Bounded wait for the graceful exit (no connections, so the drain is instant).
    let mut status = None;
    for _ in 0..100 {
        if let Some(s) = guard.0.try_wait().expect("try_wait") {
            status = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let status = status.expect("mqttd never exited after SIGTERM");
    assert!(
        status.success(),
        "graceful stop must exit 0, got {status:?}"
    );

    let stdout = log.complete(Duration::from_secs(5)).await;
    assert!(
        stdout.contains("audit chain genesis"),
        "boot must announce the chain genesis; stdout was:\n{stdout}"
    );
    let closing = stdout
        .lines()
        .rfind(|l| l.contains("audit.shutdown"))
        .unwrap_or_else(|| panic!("no audit.shutdown record; stdout was:\n{stdout}"));
    assert!(
        closing.contains("drained") && closing.contains("head"),
        "the closing record must carry the drain outcome and the chain head; got: {closing}"
    );
    // The invariant itself: no audit-target record follows the closing one.
    let after = stdout.split("audit.shutdown").last().unwrap_or("");
    assert!(
        !after.contains("audit:") || !after.contains("seq"),
        "an audit record followed the closing record; tail was: {after}"
    );
}

/// ADR 0066 T3, the export end to end: with `MQTTD_AUDIT_SYSLOG` set, the real
/// binary ships its audit chain to a TCP listener as RFC 5424 frames whose MSG
/// is one JSON object per record — genesis, an auth record from a real client
/// connect, and the closing `audit.shutdown` — and the exported stream VERIFIES:
/// replaying the records through [`mqtt_observability::AuditChain`] reproduces
/// every emitted head. This is the external-anchoring model proven end to end:
/// what the SIEM holds is enough to detect any rewrite.
#[tokio::test]
async fn the_audit_export_ships_a_verifiable_chain() {
    let syslog = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let syslog_addr = syslog.local_addr().unwrap().to_string();
    let collector = std::thread::spawn(move || {
        let (mut s, _) = syslog.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut buf = Vec::new();
        loop {
            if String::from_utf8_lossy(&buf).contains("audit.shutdown") {
                break;
            }
            let mut chunk = [0u8; 4096];
            match std::io::Read::read(&mut s, &mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    });

    let (mut guard, _log, addr) = spawn_smoke(|addr| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
        for (k, _) in std::env::vars() {
            if k.starts_with("MQTTD_") {
                cmd.env_remove(k);
            }
        }
        cmd.env("MQTTD_NODE_ID", "audit-export")
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_DURABLE_SESSIONS", "0")
            .env("MQTTD_SHUTDOWN_GRACE", "5")
            .env("MQTTD_AUDIT_SYSLOG", &syslog_addr)
            .env("RUST_LOG", "mqttd=info")
            .stderr(Stdio::null());
        cmd
    })
    .await;

    // One real client connect produces an auth.success record between genesis
    // and shutdown, so the verified chain is not vacuously genesis-only.
    let c = Client::connect(addr, "audit-client").await;
    drop(c);

    let pid = guard.0.id();
    assert!(Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("spawn kill")
        .success());
    for _ in 0..100 {
        if guard.0.try_wait().expect("try_wait").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let stream = collector.join().unwrap();
    // Parse the JSON objects out of the octet-counted frames.
    let mut records = Vec::new();
    for part in stream.split('{').skip(1) {
        let json: serde_json::Value = serde_json::from_str(&format!(
            "{{{}",
            part.split('}').next().unwrap_or("").to_owned() + "}"
        ))
        .unwrap_or(serde_json::Value::Null);
        if json.is_object() {
            records.push(json);
        }
    }
    assert!(
        records.len() >= 3,
        "expected genesis + at least one record + shutdown; stream was:\n{stream}"
    );
    assert_eq!(
        records[0]["kind"], "audit.genesis",
        "first record is genesis"
    );
    let boot = records[0]["boot"].as_str().expect("genesis boot id");

    // Replay: the genesis head must equal the boot-derived genesis, and every
    // subsequent head must reproduce from (kind, subject, detail) alone.
    let mut chain = mqtt_observability::AuditChain::with_boot(boot);
    assert_eq!(
        records[0]["head"].as_str().unwrap(),
        chain.head_hex(),
        "genesis head must derive from the announced boot id"
    );
    let mut last_kind = String::new();
    for rec in &records[1..] {
        let kind = rec["kind"].as_str().unwrap();
        let subject = rec["subject"].as_str().map(ToString::to_string);
        let detail = rec["detail"].as_str().unwrap_or("");
        chain.append(kind, subject, detail);
        assert_eq!(
            rec["head"].as_str().unwrap(),
            chain.head_hex(),
            "head mismatch at seq {:?} — the exported stream does not verify",
            rec["seq"]
        );
        last_kind = kind.to_string();
    }
    assert_eq!(
        last_kind, "audit.shutdown",
        "the chain must close with the shutdown record"
    );
}

/// Issue #504 — the accept loop must SURVIVE an accept error.
///
/// Runs the real binary under a low `RLIMIT_NOFILE`, exhausts it with more
/// sockets than the process has file descriptors, releases them, and then
/// requires a fresh client to complete a full MQTT CONNECT.
///
/// This is the falsifier for the reported outage, not a unit of the fix: before
/// the fix `serve_tcp_clients` returned on any `accept()` error, so the first
/// `EMFILE` killed that listener for the life of the process — the broker
/// stayed up, kept answering, and never accepted another client. Observed on
/// v1.0.13 (ADR 0077 lane E) as `connections_total` frozen across two rungs and
/// three minutes while `connections_active` drained 4,099 → 1,171 → 0.
///
/// The log assertion is what stops this passing vacuously: if the squeeze never
/// actually produced an accept error, the reconnect proves nothing, so the test
/// requires the broker to have logged one.
///
/// `ulimit -n` lowers the soft limit and needs no privilege. The TCP connects
/// themselves always succeed — the kernel completes them into the listen
/// backlog whether or not the application ever calls `accept()` — which is
/// exactly why the assertion is an MQTT handshake and not a socket connect.
#[tokio::test]
async fn the_listener_survives_fd_exhaustion_and_accepts_again() {
    const FD_LIMIT: usize = 128;
    const SOCKETS: usize = 400;

    let (mut guard, logs, addrs) = spawn_smoke_to_file(1, |addrs, sink| {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c")
            .arg(format!("ulimit -n {FD_LIMIT}; exec \"$0\""))
            .arg(env!("CARGO_BIN_EXE_mqttd"));
        for (k, _) in std::env::vars() {
            if k.starts_with("MQTTD_") {
                cmd.env_remove(k);
            }
        }
        cmd.env("MQTTD_NODE_ID", "fd-squeeze")
            .env("MQTTD_PLAINTEXT_BIND", addrs[0].to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("RUST_LOG", "warn,mqttd=info")
            // STDOUT to a FILE, not a pipe: the accept-failure warning is the observable
            // this test waits on, and a pipe cannot be read until the child ends.
            .stdout(Stdio::from(sink))
            .stderr(Stdio::null());
        cmd
    })
    .await;
    let (addr, log_path) = (addrs[0], logs.path().to_path_buf());

    // Squeeze: hold far more sockets open than the broker has descriptors, so its
    // accept() runs out. Held in a Vec — dropping one would hand the fd back.
    //
    // Keep connecting until the WALL ITSELF is observed, not for one burst: the squeeze
    // is only on once the broker has actually failed an accept, and if it never does
    // then nothing below proves anything about surviving one. A failed or slow connect
    // does not end it — under parallel load one burst gave up after a handful of
    // sockets, far short of the broker's 128 descriptors.
    let mut held = Vec::with_capacity(SOCKETS);
    let squeeze_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let squeezed = loop {
        if std::fs::read_to_string(&log_path)
            .unwrap_or_default()
            .contains("listener accept failed")
        {
            break true;
        }
        if std::time::Instant::now() >= squeeze_deadline {
            break false;
        }
        if held.len() < SOCKETS {
            if let Some(s) = try_connect(addr).await {
                held.push(s);
                continue;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        squeezed,
        "holding {} sockets, the squeeze never forced an accept error in 20s, so this test proves nothing \
         about surviving one — raise SOCKETS or lower FD_LIMIT. Log: {}",
        held.len(),
        std::fs::read_to_string(&log_path).unwrap_or_default()
    );

    // Release. The broker's own descriptors come back as its connection tasks end.
    drop(held);

    // The assertion: a full MQTT handshake, because a bare TCP connect would
    // succeed into the backlog even with the listener dead.
    let mut recovered = false;
    for _ in 0..100 {
        if Client::connect_v311_within(addr, "after-squeeze", true, Duration::from_millis(300))
            .await
            .is_some()
        {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _ = guard.0.kill();
    let logs = std::fs::read_to_string(&log_path).unwrap_or_default();

    assert!(
        recovered,
        "the listener never accepted again after an accept error (#504): the broker \
         process was still alive and the descriptors had been released. Logs were: {logs}"
    );
}

/// The broker log at `path` with ANSI escapes removed: tracing writes them between a
/// field's name and its value even to a file.
fn plain_log(path: &std::path::Path) -> String {
    let raw = std::fs::read_to_string(path).unwrap_or_default();
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Whether the log shows `listener` failing an accept (the shared #504 warning).
fn logged_accept_failure(log: &str, listener: &str) -> bool {
    log.lines().any(|l| {
        l.contains("listener accept failed") && l.contains(&format!("listener=\"{listener}\""))
    })
}

async fn try_connect(addr: SocketAddr) -> Option<tokio::net::TcpStream> {
    tokio::time::timeout(
        Duration::from_millis(200),
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .ok()
    .and_then(Result::ok)
}

/// Whether `/livez` answers HTTP 200 within ~10 s — a real answer, not a TCP connect
/// (which the kernel completes into the backlog with the listener dead).
async fn livez_answers(health: SocketAddr) -> bool {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    for _ in 0..100 {
        if let Some(mut s) = try_connect(health).await {
            let asked = s
                .write_all(b"GET /livez HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
                .await
                .is_ok();
            let mut body = Vec::new();
            let read =
                tokio::time::timeout(Duration::from_millis(500), s.read_to_end(&mut body)).await;
            if asked && read.is_ok() && body.starts_with(b"HTTP/1.1 200") {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Whether the peer listener accepts and handles a connection within ~10 s: it reads our
/// EOF and closes its side. A dead listener never reads the backlogged connection.
async fn peer_listener_accepts(peer: SocketAddr) -> bool {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    for _ in 0..100 {
        if let Some(mut s) = try_connect(peer).await {
            let _ = s.shutdown().await;
            let mut buf = [0u8; 64];
            if let Ok(Ok(0) | Err(_)) =
                tokio::time::timeout(Duration::from_millis(500), s.read(&mut buf)).await
            {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Issue #504, the rest of the class: the HEALTH and CLUSTER-BUS listeners must survive an
/// accept error too.
///
/// Found by a local overload of the real binary on main after the client-listener fix:
/// the MQTT listener recovered from the fd squeeze, but `/metrics`, `/readyz` and `/livez`
/// never answered again, because the health loop still `return`ed on any accept error —
/// as did the peer listener (a node that can never accept another peer link) and the
/// admin listener. On Kubernetes the dead liveness probe then restarts the pod, which is
/// the "restart away the symptom" #504 forbids.
///
/// Same squeeze as above, aimed at all three listeners. Each must have LOGGED an accept
/// failure (else the recovery below proves nothing), then answer once fds are released.
#[tokio::test]
async fn the_health_and_peer_listeners_survive_fd_exhaustion() {
    const FD_LIMIT: usize = 128;
    const CLIENT_SOCKETS: usize = 300;
    const SIDE_SOCKETS: usize = 40;

    let (mut guard, logs, addrs) = spawn_smoke_to_file(3, |addrs, sink| {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c")
            .arg(format!("ulimit -n {FD_LIMIT}; exec \"$0\""))
            .arg(env!("CARGO_BIN_EXE_mqttd"));
        for (k, _) in std::env::vars() {
            if k.starts_with("MQTTD_") {
                cmd.env_remove(k);
            }
        }
        cmd.env("MQTTD_NODE_ID", "fd-squeeze-side")
            .env("MQTTD_PLAINTEXT_BIND", addrs[0].to_string())
            .env("MQTTD_HEALTH_BIND", addrs[1].to_string())
            .env("MQTTD_PEER_BIND", addrs[2].to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")
            .env("RUST_LOG", "warn,mqttd=info")
            .stdout(Stdio::from(sink))
            .stderr(Stdio::null());
        cmd
    })
    .await;
    let (mqtt, health, peer) = (addrs[0], addrs[1], addrs[2]);
    let log_path = logs.path().to_path_buf();

    // Exhaust the descriptors through the client listener, then knock on the other two so
    // their accept() runs into the full table.
    //
    // Until BOTH have logged the failure, not one burst: a knock sent before the client
    // squeeze has filled the table is accepted normally, and under parallel load one
    // burst of knocks often all landed before the table was full, so neither listener
    // ever hit the wall. Each knock is retried until its listener has.
    let mut held = Vec::new();
    let (mut clients, mut side) = (0, [0usize; 2]);
    let squeeze_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let squeezed = loop {
        let log = plain_log(&log_path);
        let failed = [
            logged_accept_failure(&log, "health"),
            logged_accept_failure(&log, "peer"),
        ];
        if failed == [true, true] {
            break true;
        }
        if std::time::Instant::now() >= squeeze_deadline {
            break false;
        }
        if clients < CLIENT_SOCKETS {
            if let Some(s) = try_connect(mqtt).await {
                held.push(s);
                clients += 1;
                continue;
            }
        }
        for (i, addr) in [health, peer].into_iter().enumerate() {
            if !failed[i] && side[i] < SIDE_SOCKETS {
                if let Some(s) = try_connect(addr).await {
                    held.push(s);
                    side[i] += 1;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        squeezed,
        "holding {} sockets (health failed: {}, peer failed: {}), the squeeze never forced an \
         accept error on BOTH the health and the peer \
         listener in 20s, so this test proves nothing about surviving one. Log: {}",
        held.len(),
        logged_accept_failure(&plain_log(&log_path), "health"),
        logged_accept_failure(&plain_log(&log_path), "peer"),
        plain_log(&log_path)
    );
    drop(held);

    let livez = livez_answers(health).await;
    let peer_alive = peer_listener_accepts(peer).await;
    let _ = guard.0.kill();
    let log = plain_log(&log_path);
    assert!(
        livez,
        "the health listener never answered /livez again after an accept error (#504) — \
         /metrics, /readyz and /livez stay dead while the broker runs. Log: {log}"
    );
    assert!(
        peer_alive,
        "the cluster-bus listener never accepted again after an accept error (#504) — \
         this node could take no new peer link. Log: {log}"
    );
}

/// #827: the readiness wait matches the broker's own post-bind line for the exact address
/// (a port that is a prefix of another must not match), and nothing else.
#[test]
fn a_bound_line_names_the_exact_address() {
    let line = "INFO mqttd: accepting MQTT 3.1.1 clients addr=127.0.0.1:20001";
    assert!(listen_wait::reports_bound(line, "127.0.0.1:20001"));
    assert!(
        !listen_wait::reports_bound(line, "127.0.0.1:2000"),
        "a prefix of the port"
    );
    assert!(!listen_wait::reports_bound(
        "INFO mqttd: starting addr=127.0.0.1:20001",
        "127.0.0.1:20001"
    ));
    assert!(listen_wait::reports_bound(
        "INFO mqttd: serving health endpoints bind=127.0.0.1:20002 min_members=1",
        "127.0.0.1:20002"
    ));
}

/// A stop that lands right after the client listener binds drains, not kills. The SIGTERM
/// handler used to be registered only when startup finished and the drain began waiting,
/// so a stop in the tail of startup (the admin listener, the reload handlers) took the
/// signal's default action: the process died by signal 15, a node already serving clients
/// went with no drain and no closing audit record. The stop signals are now registered
/// before the client listeners bind. The signal goes from the log reader the moment the
/// bind line appears, the earliest an orchestrator could see the node serving.
#[cfg(unix)]
#[test]
fn a_stop_right_after_the_client_bind_drains_instead_of_killing() {
    use rustix::process::{kill_process, Pid, Signal};
    use std::io::{BufRead, BufReader};
    let (mut round, mut lost_port) = (0, 0);
    while round < 10 {
        let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mqttd"));
        for (k, _) in std::env::vars() {
            if k.starts_with("MQTTD_") {
                cmd.env_remove(k);
            }
        }
        let child = cmd
            .env("MQTTD_NODE_ID", "early-stop")
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_DURABLE_SESSIONS", "0")
            .env("MQTTD_SHUTDOWN_GRACE", "5")
            .env("RUST_LOG", "mqttd=info")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn the mqttd binary");
        let mut guard = ChildGuard(child);
        let pid = Pid::from_raw(i32::try_from(guard.0.id()).unwrap()).unwrap();
        let mut log = Vec::new();
        let mut signalled = false;
        for line in BufReader::new(guard.0.stdout.take().unwrap()).lines() {
            let Ok(line) = line else { break };
            if !signalled && listen_wait::reports_bound(&line, &addr.to_string()) {
                kill_process(pid, Signal::TERM).expect("send SIGTERM");
                signalled = true;
            }
            log.push(line);
        }
        // EOF: the process has exited (or closed stdout on its way out).
        let status = guard.0.wait().expect("wait");
        if !signalled {
            // The broker exited before binding: another process took the port between
            // its release and the bind. Not this test's subject; take a fresh port.
            lost_port += 1;
            assert!(
                lost_port <= 5,
                "round {round}: the broker exited before binding {lost_port} times; log:\n{}",
                log.join("\n")
            );
            continue;
        }
        assert!(
            status.success(),
            "round {round}: a SIGTERM right after the bind must drain and exit 0, got \
             {status:?}; log:\n{}",
            log.join("\n")
        );
        round += 1;
    }
}
