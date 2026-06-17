//! Durable-session cluster integration test (ADR 0006/0007, workstream E step 4f).
//!
//! Three broker nodes, each with the durable consensus-backed session store, form a
//! cluster purely via SWIM gossip (no static peer list). The lease group bootstraps
//! on the founder and grows to all three; leases are assigned to placement owners.
//!
//! The durability claim is then observable from the **owner alone**: a durable
//! `enqueue` returns `Ok` only once the message is *quorum-durable*, and on a
//! three-node group quorum = the owner plus at least one follower — so a committed
//! enqueue has, by definition, replicated to a peer and would survive the owner's
//! loss. (Serving the session *after* the owner dies is takeover, workstream F.)

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use mqtt_cluster::durable_node::build_durable_node;
use mqtt_cluster::placement::{Placement, DEFAULT_REPLICAS};
use mqtt_cluster::swim::{Config as SwimConfig, Swim};
use mqtt_cluster::swim_auth::{SwimAuth, KEY_LEN};
use mqtt_cluster::{swim_driver, NodeId};
use mqtt_core::{ClientId, Message, QoS};
use mqtt_storage::SessionStore;
use mqttd::Hub;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

/// Tight SWIM timings so discovery converges quickly.
fn swim_cfg() -> SwimConfig {
    SwimConfig {
        protocol_period_ms: 150,
        ack_timeout_ms: 60,
        suspicion_timeout_ms: 500,
        indirect_probes: 2,
        gossip_fanout: 8,
        gossip_multiplier: 4,
    }
}

struct DurableNode {
    node_id: NodeId,
    store: Arc<dyn SessionStore>,
    placement: Arc<RwLock<Placement>>,
    swim_addr: String,
    /// A clone of this node's durable plane, kept only to observe lease-group
    /// readiness (`voter_count`) from the test.
    plane: mqtt_cluster::durable_plane::DurablePlane,
    /// Abort handles for every task this node spawned — aborting them all crashes
    /// the node (it stops gossiping, serving, and replicating), so peers detect it
    /// dead and re-elect. Used by the takeover test (workstream F).
    aborts: Vec<AbortHandle>,
}

impl DurableNode {
    /// Crash this node: abort all its tasks so peers see it die.
    fn kill(&self) {
        for a in &self.aborts {
            a.abort();
        }
    }
}

/// Start one durable broker node: the durable store + lease-group endpoint (the
/// node assembly), the hub with the plane attached, a plaintext peer listener, and
/// SWIM membership driving the mesh. A node with no seeds is the founder.
async fn start_durable_node(id: &str, swim_seeds: Vec<String>) -> DurableNode {
    let node_id = NodeId(id.to_string());
    let can_bootstrap = swim_seeds.is_empty();
    let placement = Arc::new(RwLock::new(Placement::new(
        node_id.clone(),
        DEFAULT_REPLICAS,
    )));

    let (store, plane) =
        build_durable_node(node_id.clone(), placement.clone(), can_bootstrap).await;
    let plane_observer = plane.clone();
    let (mut hub, hub_tx) =
        Hub::with_config_and_placement(node_id.clone(), store.clone(), Some(placement.clone()));
    hub.attach_durable_plane(plane);
    let mut aborts = vec![tokio::spawn(hub.run()).abort_handle()];

    // Peer-link listener (plaintext mesh for the test); SWIM gossips its address.
    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer_listener.local_addr().unwrap().to_string();
    aborts.push(
        tokio::spawn(mqttd::peer::serve_listener(
            peer_listener,
            node_id.clone(),
            hub_tx.clone(),
            None,
            None,
        ))
        .abort_handle(),
    );

    // SWIM membership driving the peer mesh.
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let swim_addr = socket.local_addr().unwrap().to_string();
    let swim = Swim::new(
        node_id.clone(),
        swim_addr.clone(),
        peer_addr,
        swim_cfg(),
        swim_seeds,
    );
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let auth = SwimAuth::new(&[0x5A; KEY_LEN]);
    aborts.push(
        tokio::spawn(swim_driver::run(
            socket,
            swim,
            Duration::from_millis(20),
            event_tx,
            Some(auth),
        ))
        .abort_handle(),
    );
    aborts.push(
        tokio::spawn(mqttd::cluster::maintain_peer_links(
            event_rx,
            node_id.clone(),
            hub_tx,
            None,
            Some(placement.clone()),
        ))
        .abort_handle(),
    );

    DurableNode {
        node_id,
        store,
        placement,
        swim_addr,
        plane: plane_observer,
        aborts,
    }
}

async fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(
            Instant::now() < deadline,
            "cluster did not converge in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn enqueue_is_durable_across_a_three_node_cluster() {
    let a = start_durable_node("dur-a", vec![]).await; // founder
    let b = start_durable_node("dur-b", vec![a.swim_addr.clone()]).await;
    let c = start_durable_node("dur-c", vec![a.swim_addr.clone()]).await;
    let nodes = [&a, &b, &c];

    // SWIM converges: every node sees all three members.
    wait_until(Duration::from_secs(20), || {
        nodes
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 3)
    })
    .await;

    // A persistent client; its group owner is consistent across nodes (HRW).
    let client = ClientId("durable-session-1".to_string());
    let owner = a.placement.read().unwrap().owner(&client.0);
    let owner_node = nodes.iter().find(|n| n.node_id == owner).unwrap();

    let msg = Message {
        topic: "t".to_string(),
        payload: bytes::Bytes::from_static(b"survives"),
        qos: QoS::AtLeastOnce,
        retain: false,
    };

    // Enqueue on the owner, polling until it COMMITS. A committed enqueue on a
    // three-node group required quorum (owner + ≥1 follower), so the message has
    // replicated to a peer. This waits out the lease group forming, the client's
    // group lease being assigned to the owner, and the peer mesh coming up.
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if owner_node.store.enqueue(&client, &msg).await.is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "durable enqueue never committed across the cluster"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The owner replays the committed (quorum-durable) message.
    let pending = owner_node.store.pending(&client, 0, 100).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(&pending[0].message.payload[..], b"survives");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_replica_serves_the_session_after_the_owner_dies() {
    let a = start_durable_node("dur-a", vec![]).await; // founder
    let b = start_durable_node("dur-b", vec![a.swim_addr.clone()]).await;
    let c = start_durable_node("dur-c", vec![a.swim_addr.clone()]).await;
    let nodes = [&a, &b, &c];

    // SWIM converges: every node sees all three members.
    wait_until(Duration::from_secs(20), || {
        nodes
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 3)
    })
    .await;

    // A persistent client; durably enqueue a message on its owner. A committed
    // enqueue on a three-node group is quorum-durable (owner + ≥1 follower), so the
    // surviving replicas hold it even once the owner is gone.
    let client = ClientId("takeover-session-1".to_string());
    let owner = a.placement.read().unwrap().owner(&client.0);
    let owner_node = nodes.iter().find(|n| n.node_id == owner).unwrap();
    let msg = Message {
        topic: "t".to_string(),
        payload: bytes::Bytes::from_static(b"survives-takeover"),
        qos: QoS::AtLeastOnce,
        retain: false,
    };
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if owner_node.store.enqueue(&client, &msg).await.is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "durable enqueue never committed across the cluster"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Wait until the lease group has grown to all three voters. Session-log
    // replication (our quorum-append) is independent of raft membership, so an
    // enqueue can commit while b/c are still learners; killing a voter before the
    // group has a surviving quorum would wedge it. A real failover is only safe
    // once losing one node still leaves a raft quorum.
    wait_until(Duration::from_secs(30), || {
        nodes.iter().all(|n| n.plane.voter_count() == 3)
    })
    .await;

    // Kill the owner. The two survivors detect it dead (SWIM), drop it from
    // placement, re-elect the lease leader if needed, and reassign the client's
    // group to a surviving replica at a **new epoch** (fencing the dead owner).
    owner_node.kill();
    let survivors: Vec<&&DurableNode> = nodes.iter().filter(|n| n.node_id != owner).collect();
    wait_until(Duration::from_secs(20), || {
        survivors
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 2)
    })
    .await;

    // The new owner is a survivor (HRW over the surviving members). It was a
    // replica, so on first touch it rebuilds the committed log from a quorum of the
    // surviving replicas (workstream F) and serves the session — the enqueued
    // message replays with no loss.
    let new_owner = survivors[0].placement.read().unwrap().owner(&client.0);
    assert_ne!(new_owner, owner, "a survivor must take over the group");
    let new_owner_node = survivors.iter().find(|n| n.node_id == new_owner).unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    let pending = loop {
        if let Ok(p) = new_owner_node.store.pending(&client, 0, 100).await {
            if !p.is_empty() {
                break p;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the surviving replica never recovered the session after takeover"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(pending.len(), 1);
    assert_eq!(&pending[0].message.payload[..], b"survives-takeover");
}
