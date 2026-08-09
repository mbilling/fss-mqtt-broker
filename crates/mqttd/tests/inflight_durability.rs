//! Does an acknowledged QoS 1 message survive a crash while it is **in flight to
//! an online subscriber**? (issue #124)
//!
//! The README's headline guarantee is that no acknowledged fact is lost. That is
//! well proven for the *offline-enqueue* path: a message for a disconnected
//! persistent subscriber is durably appended, quorum-replicated, and the
//! publisher's PUBACK is gated on it.
//!
//! The online path looks different in the code. `Hub::deliver_to_client` sends
//! straight to a connected subscriber's channel and returns `true` — satisfying
//! the publisher's ack gate — and the in-flight table it registers the packet in
//! (`Hub::inflight`) is an in-memory `HashMap` that nothing ever writes to the
//! store.
//!
//! So this test asks the question directly, against the real binary:
//!
//!   1. a PERSISTENT subscriber is online and subscribed at QoS 1;
//!   2. a publisher sends QoS 1 and **receives its PUBACK** — the broker has now
//!      promised to deliver;
//!   3. the subscriber receives the PUBLISH and deliberately does **not** PUBACK,
//!      so the message is genuinely in flight, exactly as it would be for a device
//!      on a slow link;
//!   4. the broker is SIGKILLed — no flush, no goodbye;
//!   5. it restarts on the same data directory and the subscriber resumes its
//!      session.
//!
//! MQTT requires an unacknowledged QoS 1 message to be redelivered on session
//! resume. If it is not, an acknowledged fact was lost and the guarantee is
//! narrower than the README states.
//!
//! Written to be honest either way: if it passes, the question is settled and this
//! test stops it being re-litigated. If it fails, it names precisely what has to be
//! fixed or reworded.

mod common;

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::{Client, Recv};
use mqtt_codec::{Packet, QoS};
use tokio::net::TcpStream;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Spawn the real binary on `addr`, persisting to `data_dir`.
fn spawn_broker(addr: SocketAddr, data_dir: &std::path::Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_mqttd"))
        .env("MQTTD_NODE_ID", "inflight")
        .env("MQTTD_PLAINTEXT_BIND", addr.to_string())
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        // Durable sessions are on by default; a data dir makes them persistent.
        .env("MQTTD_DATA_DIR", data_dir.to_string_lossy().to_string())
        .env("MQTTD_SHUTDOWN_GRACE", "0")
        .env("RUST_LOG", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn the mqttd binary")
}

async fn wait_until_listening(addr: SocketAddr) {
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the mqttd binary never started listening on {addr}");
}

/// CURRENTLY FAILS — this is the reproduction for issue #124, kept as the
/// regression test for the fix rather than deleted.
///
/// Confirmed 2026-08-09 against the real binary: the publisher is acknowledged,
/// the subscriber's persistent session survives the crash (`session_present` is
/// true), and the message is **never redelivered**. An acknowledged fact is lost.
///
/// Ignored so CI stays honest about what passes rather than red about a known,
/// documented gap. Remove the `#[ignore]` with the fix — the README and
/// COMPARISON have been narrowed to match reality in the meantime.
#[tokio::test]
#[ignore = "reproduces issue #124: in-flight QoS 1 to an ONLINE subscriber is not durable"]
async fn an_acked_qos1_in_flight_to_an_online_subscriber_survives_a_crash() {
    let addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let data_dir = tempfile::tempdir().expect("temp data dir");

    let mut child = spawn_broker(addr, data_dir.path());
    wait_until_listening(addr).await;

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
    child.kill().expect("kill the broker");
    let _ = child.wait();
    drop(sub);
    drop(pubr);

    // 5. Restart on the same data directory and resume the session.
    let mut child2 = spawn_broker(addr, data_dir.path());
    wait_until_listening(addr).await;
    // Give the durable plane a moment to recover its state before resuming.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (mut resumed, session_present) = Client::connect_v311(addr, "inflight-sub", false).await;
    assert!(
        session_present,
        "the persistent session did not survive the restart at all — \
         nothing else in this test can be interpreted"
    );

    // MQTT: an unacknowledged QoS 1 message is redelivered on resume.
    let outcome = match resumed.recv_bounded(Duration::from_secs(15)).await {
        Recv::Packet(Packet::Publish(p)) if p.payload[..] == b"acked-then-crashed"[..] => Ok(()),
        Recv::Packet(other) => Err(format!("expected the redelivered PUBLISH, got {other:?}")),
        Recv::Closed => Err("the resumed connection closed before any redelivery".to_string()),
        Recv::Quiet => Err(
            "NOT redelivered: the publisher was acknowledged for a message that no \
             longer exists anywhere. The in-flight state for an ONLINE subscriber is \
             held only in Hub::inflight (in memory) and is never written to the store, \
             so the crash lost an acknowledged fact. The README's 'no acknowledged \
             fact is lost' must either cover this path or say that it does not."
                .to_string(),
        ),
    };

    let _ = child2.kill();
    let _ = child2.wait();

    if let Err(why) = outcome {
        panic!("{why}");
    }
}
