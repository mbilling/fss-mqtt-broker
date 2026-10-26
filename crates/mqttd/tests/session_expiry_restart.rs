//! A persistent session whose connection ended in a broker crash still expires on its
//! Session Expiry Interval (MQTT 5 §3.1.2.11.2) once the broker is back.
//!
//! Found while chasing durable sessions that outlived a 30 s expiry by minutes on a
//! local 3-node cluster. The persisted expiry is an ABSOLUTE deadline (ADR 0009 §3),
//! written only at detach: `finish_attach` clears it when a client connects
//! (`set_session_expiry(None)`), and `persist_detach_deadline` writes it again when the
//! connection ends. Any end that skips that write leaves a record with subscriptions
//! and NO deadline — a crash or kill while the client is connected, or a detach on a
//! node whose deadline write the store refuses ("not the owning node for this group",
//! the cluster case). The boot / inherited-session scan (`inherit_sessions`) then
//! materialises such a record with the placeholder interval `u32::MAX` and no entry in
//! `expiring`, so the sweep never discards it: the session, its subscriptions and its
//! offline queue live until the client comes back, which may be never.
//!
//! The test: a v5 client with Session Expiry Interval 2 s subscribes; the node dies
//! with the client still connected; a fresh node over the same data directory must
//! report the session gone a few seconds later.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Client, TempDir};
use mqtt_codec::{Property, QoS};
use mqtt_observability::metrics::Metrics;
use mqttd::hub::Hub;
use tokio::net::TcpListener;

const EXPIRY_S: u32 = 2;

struct Node {
    addr: std::net::SocketAddr,
    metrics: Arc<Metrics>,
    accept: tokio::task::JoinHandle<()>,
    hub: tokio::task::JoinHandle<()>,
}

/// An on-disk node (as `common::start_persistent_node`) with metrics attached.
async fn start_node(data_dir: &std::path::Path) -> Node {
    use mqtt_cluster::NodeId;
    use mqtt_storage::logged::ReplicatedSessionStore;
    use mqtt_storage::persistent_log::PersistentLog;
    use mqtt_storage::{QueueLimits, SessionStore};

    let path = data_dir.join("sessions.redb");
    // Bounded: the previous node's redb file lock can take a moment to release.
    let deadline = Instant::now() + Duration::from_secs(5);
    let log = loop {
        match PersistentLog::open(&path) {
            Ok(log) => break log,
            Err(e) => {
                assert!(Instant::now() < deadline, "open {}: {e}", path.display());
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    };
    let store: Arc<dyn SessionStore> = Arc::new(ReplicatedSessionStore::with_limits(
        log,
        QueueLimits::default(),
    ));
    let metrics = Arc::new(Metrics::new("expiry-restart"));
    let (mut hub, hub_tx) = Hub::with_config(NodeId("node-expiry".into()), store);
    hub.attach_metrics(metrics.clone());
    let hub = tokio::spawn(hub.run());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, hub_tx.clone()));
        }
    });
    Node {
        addr,
        metrics,
        accept,
        hub,
    }
}

impl Node {
    /// Stop abruptly, as a crash would: the hub never sees the connections end.
    async fn kill(self) {
        self.accept.abort();
        let _ = self.accept.await;
        self.hub.abort();
        let _ = self.hub.await;
        tokio::task::yield_now().await;
    }

    fn sessions(&self) -> Option<u64> {
        self.metrics
            .render()
            .lines()
            .find(|l| l.starts_with("mqttd_sessions "))
            .and_then(|l| l.rsplit(' ').next()?.parse().ok())
    }
}

/// Lifetime #1 ends with the node dying; `detach_first` decides whether the client had
/// already disconnected (its deadline written) or was still connected. Lifetime #2 must
/// recover the session and then expire it on its interval.
async fn session_expires_after_restart(detach_first: bool) {
    let dir = TempDir::new();

    let node = start_node(dir.path()).await;
    let (mut sub, ack) = Client::connect_v5(
        node.addr,
        "crash-held",
        false,
        vec![Property::SessionExpiryInterval(EXPIRY_S)],
    )
    .await;
    assert_eq!(ack.code, 0, "CONNACK: {ack:?}");
    sub.subscribe(1, "crash/t", QoS::AtLeastOnce).await;
    if detach_first {
        // Waits for the broker to close the socket, so the Detach is queued; a SUBACK
        // for a later client then proves the hub has handled it (one FIFO command
        // queue), including its awaited deadline write.
        sub.disconnect().await;
        let mut probe = Client::connect(node.addr, "probe").await;
        probe.subscribe(1, "probe/t", QoS::AtMostOnce).await;
        probe.disconnect().await;
    }
    node.kill().await;
    drop(sub);

    // The restarted node recovers the session from disk ...
    let node = start_node(dir.path()).await;
    let restarted = Instant::now();
    let seen_by = restarted + Duration::from_secs(10);
    while node.sessions() != Some(1) {
        assert!(
            Instant::now() < seen_by,
            "the restarted node never reported the recovered session: {:?}",
            node.sessions()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ... and, with no connection, must expire it within its interval (plus the sweep's
    // one-second granularity and generous slack).
    let expired_by = restarted + Duration::from_secs(u64::from(EXPIRY_S) + 10);
    while node.sessions() != Some(0) {
        assert!(
            Instant::now() < expired_by,
            "a session with Session Expiry Interval {EXPIRY_S} s, whose connection ended \
             {}, is still held {:?} after the restart (mqttd_sessions = {:?}): the record \
             carried no deadline, and the boot scan materialised it as never-expiring",
            if detach_first {
                "before the broker died"
            } else {
                "when the broker died"
            },
            restarted.elapsed(),
            node.sessions()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    node.kill().await;
}

/// The control: a detach persisted the deadline, and the restarted node honours it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_that_disconnected_before_a_crash_expires_after_restart() {
    session_expires_after_restart(true).await;
}

/// The defect: the crash took the detach, and with it the only deadline write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_whose_connection_ended_in_a_crash_expires_after_restart() {
    session_expires_after_restart(false).await;
}
