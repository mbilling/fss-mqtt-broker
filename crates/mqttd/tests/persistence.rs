//! On-disk persistence restart test (ADR 0018 phase 1, the phase-5 node-level proof).
//!
//! The in-memory broker (`start_broker`) proves a *retained* session is replayed while
//! the process stays up. This proves the stronger, on-disk claim: a persistent session
//! and a message queued for it while it was offline both survive a **full node restart**
//! from the same data directory — observed end to end over real TCP by a client.
//!
//! It is also the proof that graceful shutdown (ADR 0019) releases the redb file lock:
//! the restarting node reopens `sessions.redb` from the same directory, which fails
//! ("Database already open") if the previous node leaked a database handle.

mod common;

use common::{start_persistent_node, Client, TempDir};
use mqtt_codec::QoS;

#[tokio::test]
async fn persistent_session_and_offline_queue_survive_a_node_restart() {
    let dir = TempDir::new();

    // --- node lifetime #1: establish a persistent session and queue a message for it ---
    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;

    // A persistent subscriber registers its session + subscription, then detaches.
    let (mut sub, present) = Client::connect_v311(addr, "durable", false).await;
    assert!(
        !present,
        "a brand-new persistent session has no prior state"
    );
    sub.subscribe(1, "offline/topic", QoS::AtMostOnce).await;
    sub.disconnect().await; // session retained on disk, subscriber now offline

    // Publish QoS 1 while the subscriber is away; the PUBACK proves the message is
    // durably enqueued (fsync'd) before we restart.
    let mut pubr = Client::connect(addr, "pub-offline").await;
    pubr.publish(
        "offline/topic",
        b"queued-while-away",
        QoS::AtLeastOnce,
        Some(1),
        vec![],
    )
    .await;
    assert_eq!(pubr.recv().await, mqtt_codec::Packet::PubAck(1.into()));
    pubr.disconnect().await;

    // Restart: a clean shutdown must release the redb lock so the directory reopens.
    node.shutdown().await;

    // --- node lifetime #2: a fresh node over the SAME directory recovers the state ---
    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;

    // Same client id, clean_session=false: the session is present (recovered from disk)
    // and the message queued before the restart is replayed.
    let (mut sub, present) = Client::connect_v311(addr, "durable", false).await;
    assert!(
        present,
        "the persistent session must survive a node restart"
    );
    let p = sub.expect_publish().await;
    assert_eq!(p.topic, "offline/topic");
    assert_eq!(&p.payload[..], b"queued-while-away");

    sub.disconnect().await;
    node.shutdown().await;
}

#[tokio::test]
async fn a_clean_session_does_not_survive_a_node_restart() {
    let dir = TempDir::new();

    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;

    // A clean (clean_session=true) session must not be persisted.
    let (mut c, present) = Client::connect_v311(addr, "ephemeral", true).await;
    assert!(!present);
    c.subscribe(1, "x", QoS::AtMostOnce).await;
    c.disconnect().await;
    node.shutdown().await;

    // Reopen: reconnecting clean shows no prior session, and a fresh persistent connect
    // under the same id is likewise absent (nothing was ever written).
    let node = start_persistent_node(dir.path()).await;
    let addr = node.client_addr;
    let (mut c, present) = Client::connect_v311(addr, "ephemeral", false).await;
    assert!(!present, "a clean session must not survive a restart");

    c.disconnect().await;
    node.shutdown().await;
}
