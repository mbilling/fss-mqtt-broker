//! Does an acknowledged `QoS 1` message survive a crash while it is **in flight to
//! an online subscriber**? (issue #124)
//!
//! The README's headline guarantee is that no acknowledged fact is lost. That was
//! well proven for the *offline-enqueue* path — a message for a disconnected
//! persistent subscriber is durably appended, quorum-replicated, and the
//! publisher's PUBACK gated on it — and, until this test, unproven for the online
//! one.
//!
//! It was also untrue. `Hub::deliver_to_client` sent straight to a connected
//! subscriber's channel and returned `true`, satisfying the publisher's ack gate,
//! while the in-flight table it registered the packet in (`Hub::inflight`) was an
//! in-memory `HashMap` that nothing ever wrote to the store. The fix appends to the
//! session log **before** the wire send and truncates it only when the subscriber
//! acknowledges; this test is what keeps that true.
//!
//! It asks the question directly, against the real binary:
//!
//!   1. a PERSISTENT subscriber is online and subscribed at `QoS 1`;
//!   2. a publisher sends `QoS 1` and **receives its `PUBACK`** — the broker has now
//!      promised to deliver;
//!   3. the subscriber receives the PUBLISH and deliberately does **not** PUBACK,
//!      so the message is genuinely in flight, exactly as it would be for a device
//!      on a slow link;
//!   4. the broker is `SIGKILL`ed — no flush, no goodbye;
//!   5. it restarts on the same data directory and the subscriber resumes its
//!      session.
//!
//! MQTT requires an unacknowledged `QoS 1` message to be redelivered on session
//! resume. If it is not, an acknowledged fact was lost and the guarantee is
//! narrower than the README states.

mod common;
mod proc_common;

use std::net::SocketAddr;
use std::process::{Child, Command};
use std::time::Duration;

use common::{Client, Recv};
use mqtt_codec::{Packet, QoS};
use proc_common::free_tcp_port;
use tokio::net::TcpStream;

/// Where a spawned broker's own log goes. Kept next to the data dir rather than
/// `/dev/null` so a failure to start reports *why* instead of only that it did not —
/// the same lesson as issue #106.
fn log_path(data_dir: &std::path::Path, which: &str) -> std::path::PathBuf {
    data_dir.join(format!("mqttd-{which}.log"))
}

/// A spawned broker that is killed when it goes out of scope — **including when the
/// test panics**.
///
/// Without this, every failing run left a live `mqttd` behind: the panic skipped the
/// explicit `kill`. They accumulate, and a machine carrying several stray brokers makes
/// the *next* run slower and flakier, so one real failure turns into a stream of
/// unrelated ones that look like the same bug. Observed directly while developing this
/// test — three strays, one over eight minutes old.
struct Broker {
    child: Child,
    data_dir: std::path::PathBuf,
    which: &'static str,
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Broker {
    /// Spawn the real binary on `addr`, persisting to `data_dir`.
    fn spawn(addr: SocketAddr, data_dir: &std::path::Path, which: &'static str) -> Self {
        let log = std::fs::File::create(log_path(data_dir, which)).expect("create the broker log");
        let child = Command::new(env!("CARGO_BIN_EXE_mqttd"))
            .env("MQTTD_NODE_ID", "inflight")
            .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            // Durable sessions are on by default; a data dir makes them persistent.
            .env("MQTTD_DATA_DIR", data_dir.to_string_lossy().to_string())
            .env("MQTTD_SHUTDOWN_GRACE", "0")
            .env("RUST_LOG", "info")
            // Both streams to the same file: the log goes to stdout, and a config or
            // bind error goes to stderr — a diagnostic that captures only one of them
            // is empty in exactly the cases it was added for.
            .stdout(log.try_clone().expect("clone the log handle"))
            .stderr(log)
            .spawn()
            .expect("failed to spawn the mqttd binary");
        Self {
            child,
            data_dir: data_dir.to_path_buf(),
            which,
        }
    }

    /// `SIGKILL`, and wait for the process to be reaped — so the port it held is
    /// definitely released before the restart tries to bind it.
    fn kill(&mut self) {
        self.child.kill().expect("kill the broker");
        let _ = self.child.wait();
    }

    /// The broker's own log, or why it could not be read — never a silent empty string,
    /// which would read as "the broker said nothing" when it means "we looked in the
    /// wrong place".
    fn log(&self) -> String {
        let path = log_path(&self.data_dir, self.which);
        match std::fs::read_to_string(&path) {
            Ok(s) if s.is_empty() => format!("(empty: {} exists but has no bytes)", path.display()),
            Ok(s) => tail(&s),
            Err(e) => format!("(unreadable: {} — {e})", path.display()),
        }
    }

    async fn wait_until_listening(&mut self, addr: SocketAddr) {
        for _ in 0..200 {
            if TcpStream::connect(addr).await.is_ok() {
                return;
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!(
                    "the {} mqttd exited before listening on {addr} ({status}).\nIts log:\n{}",
                    self.which,
                    self.log()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "the {} mqttd never started listening on {addr} after 20s and is still running \
             (pid {}).\nIts log:\n{}",
            self.which,
            self.child.id(),
            self.log()
        );
    }
}

/// The last few lines of a log — enough to name the cause without burying it.
fn tail(log: &str) -> String {
    let lines: Vec<&str> = log.lines().collect();
    lines[lines.len().saturating_sub(15)..].join("\n")
}

/// The regression test for issue #124.
///
/// It failed on 2026-08-09 against the real binary — the publisher was acknowledged,
/// the persistent session survived the crash, and the message was never redelivered.
/// The fix makes the durable append happen **before** the wire send for a `QoS` > 0
/// delivery to a persistent subscriber, and truncates the session log only when that
/// subscriber acknowledges. This test is what holds that in place.
#[tokio::test]
async fn an_acked_qos1_in_flight_to_an_online_subscriber_survives_a_crash() {
    // The band allocator (`proc_common`), not `bind(":0")`: a port released from the
    // ephemeral range can be handed straight back out as an outbound source port before
    // the broker binds it. That is the same race `proc_common` was written to close, and
    // this test predates it.
    let addr: SocketAddr = format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap();
    let data_dir = tempfile::tempdir().expect("temp data dir");

    let mut broker = Broker::spawn(addr, data_dir.path(), "first");
    broker.wait_until_listening(addr).await;

    // 1. A PERSISTENT subscriber, online and subscribed at QoS 1.
    let (mut sub, _present) = Client::connect_v311(addr, "inflight-sub", false).await;
    sub.subscribe(1, "inflight/test", QoS::AtLeastOnce).await;

    // 2. The publisher gets its PUBACK: the broker has taken responsibility.
    let mut pubr = Client::connect(addr, "inflight-pub").await;
    pubr.publish(
        "inflight/test",
        b"acked-then-crashed",
        QoS::AtLeastOnce,
        Some(1),
        vec![],
    )
    .await;
    assert_eq!(
        pubr.recv().await,
        Packet::PubAck(1.into()),
        "the publisher must be acked before this test means anything"
    );

    // 3. The subscriber receives it and deliberately does NOT acknowledge, so the
    //    message is genuinely in flight — the state the broker holds only in memory.
    //    Bounded receive, so a failure here is reported as itself rather than as a
    //    bare timeout that could be mistaken for the redelivery step failing.
    let delivered = match sub.recv_bounded(Duration::from_secs(10)).await {
        Recv::Packet(Packet::Publish(p)) => p,
        Recv::Packet(other) => panic!("step 3: expected the live PUBLISH, got {other:?}"),
        Recv::Quiet => panic!("step 3: the subscriber never got the live delivery (quiet)"),
        Recv::Closed => panic!("step 3: the subscriber's connection closed before delivery"),
    };
    assert_eq!(&delivered.payload[..], b"acked-then-crashed");
    assert_eq!(
        delivered.qos,
        QoS::AtLeastOnce,
        "must be delivered at QoS 1 for redelivery to be owed"
    );

    // 4. Kill the broker outright: no flush, no goodbye.
    broker.kill();
    drop(sub);
    drop(pubr);

    // 5. Restart on the same data directory and resume the session.
    let mut restarted = Broker::spawn(addr, data_dir.path(), "restarted");
    restarted.wait_until_listening(addr).await;
    // Give the durable plane a moment to recover its state before resuming.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (mut resumed, session_present) = Client::connect_v311(addr, "inflight-sub", false).await;
    assert!(
        session_present,
        "the persistent session did not survive the restart at all — \
         nothing else in this test can be interpreted"
    );

    // MQTT: an unacknowledged QoS 1 message is redelivered on resume.
    match resumed.recv_bounded(Duration::from_secs(15)).await {
        Recv::Packet(Packet::Publish(p)) if p.payload[..] == b"acked-then-crashed"[..] => {}
        Recv::Packet(other) => panic!("expected the redelivered PUBLISH, got {other:?}"),
        Recv::Closed => panic!("the resumed connection closed before any redelivery"),
        Recv::Quiet => panic!(
            "NOT redelivered: the publisher was acknowledged for a message that no \
             longer exists anywhere. This is the #124 regression — the durable append \
             must happen BEFORE the live send for a QoS > 0 delivery to a persistent \
             subscriber, and the session log must be truncated only when that \
             subscriber acknowledges."
        ),
    }
    // `restarted` is killed by its Drop, on this path and on every panic above.
}
