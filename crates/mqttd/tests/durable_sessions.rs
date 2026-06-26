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
//!
//! These also check that a durable node serves ordinary MQTT clients through its hub
//! (`a_durable_node_serves_a_client_pubsub`).
//!
//! **Client-observable durable failover** (`a_persistent_client_resumes_its_session_on_the_new_owner_after_takeover`)
//! — a *persistent* client whose owner is killed reconnects to the **new owner** and
//! resumes its session. This took two fixes, now both landed:
//!
//! 1. **Membership** ([ADR 0016](../../docs/adr/0016-swim-membership-stability.md)
//!    phase 1, tombstone `Dead`): the new owner's `placement.members()` no longer flaps
//!    to a wrong set (killed node resurrected, live survivor dropped), so
//!    `group_replica_set` has a live quorum and recovery does not read the dead node.
//! 2. **Attach path** ([ADR 0017](../../docs/adr/0017-durable-attach-readiness.md)):
//!    during the ~1s before the group's lease reassigns to the new owner the durable
//!    reads return a transient `Unavailable`; the attach now **waits** for an
//!    authoritative answer off the hub loop and resumes the session, or rejects with
//!    Server-unavailable so the client retries — it never silently downgrades a
//!    recoverable session to a fresh one.

mod common;

use std::net::SocketAddr;
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
        suspicion_min_timeout_ms: 200,
        suspicion_confirmations: 3,
        dead_ttl_ms: 5000,
        indirect_probes: 2,
        gossip_fanout: 8,
        gossip_multiplier: 4,
        awareness_max: 8,
    }
}

struct DurableNode {
    node_id: NodeId,
    store: Arc<dyn SessionStore>,
    placement: Arc<RwLock<Placement>>,
    swim_addr: String,
    /// This node's MQTT client listener address, for the client-observable failover
    /// test (a reconnecting client served by the new owner).
    client_addr: SocketAddr,
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
    // Cap 5: every node in these small (≤5) clusters votes, preserving the original
    // all-voters behaviour these tests assert (ADR 0021).
    start_durable_node_capped(id, swim_seeds, 5, None).await
}

/// As [`start_durable_node`], but injects a per-commit latency into the lease store —
/// simulating a slow-fsync durable backend so the lease-group timing can be exercised
/// against it deterministically (ADR 0026).
async fn start_durable_node_cfg(
    id: &str,
    swim_seeds: Vec<String>,
    commit_delay: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
) -> DurableNode {
    start_durable_node_capped(id, swim_seeds, 5, commit_delay).await
}

/// As [`start_durable_node_cfg`], but with an explicit bounded voter cap `N` (ADR 0021)
/// so a larger cluster can be brought up with only `N` voters and the rest as learners.
async fn start_durable_node_capped(
    id: &str,
    swim_seeds: Vec<String>,
    voter_cap: usize,
    commit_delay: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
) -> DurableNode {
    let node_id = NodeId(id.to_string());
    let can_bootstrap = swim_seeds.is_empty();
    let placement = Arc::new(RwLock::new(Placement::new(
        node_id.clone(),
        DEFAULT_REPLICAS,
    )));

    let (store, plane, driver) = build_durable_node(
        node_id.clone(),
        placement.clone(),
        can_bootstrap,
        voter_cap,
        None,
        commit_delay,
    )
    .await;
    let plane_observer = plane.clone();
    let (mut hub, hub_tx) =
        Hub::with_config_and_placement(node_id.clone(), store.clone(), Some(placement.clone()));
    hub.attach_durable_plane(plane);
    // Killing a node aborts its hub, accept loop, and lease-group driver together.
    let mut aborts = vec![
        tokio::spawn(hub.run()).abort_handle(),
        driver.abort_handle(),
    ];

    // MQTT client listener (permissive, served by this node's hub). Clients connect
    // here directly; for the failover test a client reconnects to the new owner.
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();
    {
        let tx = hub_tx.clone();
        aborts.push(
            tokio::spawn(async move {
                loop {
                    let (stream, _) = client_listener.accept().await.unwrap();
                    tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
                }
            })
            .abort_handle(),
        );
    }

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
            None, // no anti-replay sequencing in this test
            None, // no reject sink in this test
            std::future::pending(),
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
            None,
        ))
        .abort_handle(),
    );

    DurableNode {
        node_id,
        store,
        placement,
        swim_addr,
        client_addr,
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

/// ADR 0026 persistent-path coverage: a durable lease group must **form and hold a stable
/// leader** even when the store is slow to commit. A 200ms per-commit latency (simulating a
/// slow-fsync disk — the condition that made the real demo churn) is injected from the start,
/// so the write-heavy bring-up (initialize + add each voter, every write an fsync) runs under
/// it. The relaxed timing (heartbeat 500ms / election 1500–3000ms) tolerates it: the group
/// converges to one leader and the term settles.
///
/// Scope honesty: this is *coverage of the slow-commit write path*, not a deterministic guard
/// for the churn itself. The demo churn is driven by heartbeat/lease maintenance over a real
/// network with latency; this harness's in-process router delivers every RPC instantly, and
/// `commit_delay` only delays the persist path (`save_vote`/`append_to_log`) — empty
/// steady-state heartbeats never persist, so the latency does not reach the lease-maintenance
/// path. A true regression guard needs network-latency injection into the raft RPCs (a
/// madsim/turmoil-style harness — ADR 0024 T7, deferred); the timing fix itself was validated
/// live in the demo (ADR 0026). This test does prove the persistent write path forms and
/// serves under injected fsync latency, which the in-memory tests never exercise.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lease_group_forms_and_is_stable_under_slow_durable_commits() {
    use mqtt_cluster::node_registry::raft_id;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    // Deliberately make the founder the *maximum* raft id — NOT the min. This doubles as the
    // ADR 0026 T7 regression guard: the founder must form the group regardless of its id rank
    // (the old min-id bootstrap tiebreak hung a non-min founder at term 0). `can_bootstrap` —
    // not id rank — decides who initializes.
    let mut names = ["lease-stab-1", "lease-stab-2", "lease-stab-3"];
    names.sort_by_key(|n| std::cmp::Reverse(raft_id(&NodeId((*n).to_string()))));

    // 200ms per-commit latency, on from the start so bring-up runs under slow commits.
    let knob = Arc::new(AtomicU64::new(200));
    let a = start_durable_node_cfg(names[0], vec![], Some(knob.clone())).await; // founder = max id
    let b = start_durable_node_cfg(names[1], vec![a.swim_addr.clone()], Some(knob.clone())).await;
    let c = start_durable_node_cfg(names[2], vec![a.swim_addr.clone()], Some(knob.clone())).await;
    let nodes = [&a, &b, &c];

    // Under slow commits, the group must converge to exactly one leader.
    wait_until(Duration::from_secs(40), || {
        nodes.iter().filter(|n| n.plane.lease_role().0).count() == 1
    })
    .await;

    // And the leader holds: the term does not keep climbing over the window.
    let term_before = nodes.iter().map(|n| n.plane.lease_role().1).max().unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let term_after = nodes.iter().map(|n| n.plane.lease_role().1).max().unwrap();
    let leaders = nodes.iter().filter(|n| n.plane.lease_role().0).count();

    assert_eq!(
        leaders, 1,
        "exactly one leader must remain under slow commits"
    );
    assert!(
        term_after <= term_before + 1,
        "lease term climbed under slow commits ({term_before} -> {term_after}): \
         the lease group is re-electing — its raft timing is too tight for the store latency"
    );
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

    let msg = Message::new(
        "t".to_string(),
        bytes::Bytes::from_static(b"survives"),
        QoS::AtLeastOnce,
        false,
    );

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

/// A durable node must still serve ordinary MQTT clients through its hub: connect,
/// subscribe, publish, deliver — proving the durable store's session operations in
/// the attach/serve path complete (not just direct store reads).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_durable_node_serves_a_client_pubsub() {
    let a = start_durable_node("solo-a", vec![]).await; // founder, bootstraps alone

    // Clean-session client: a fresh subscribe + publish round-trip on the owner node.
    let mut sub = common::Client::connect(a.client_addr, "dur-sub").await;
    sub.subscribe(1, "t", QoS::AtMostOnce).await;
    let mut pubr = common::Client::connect(a.client_addr, "dur-pub").await;
    pubr.publish("t", b"served", QoS::AtMostOnce, None, vec![])
        .await;

    let p = sub.expect_publish().await;
    assert_eq!(&p.payload[..], b"served");

    // A persistent (clean_session=false) connect must also complete its CONNACK —
    // the attach path's durable ensure_session/subscriptions reads have to resolve.
    let (mut persistent, _present) =
        common::Client::connect_v311(a.client_addr, "dur-persistent", false).await;
    persistent.subscribe(2, "p", QoS::AtMostOnce).await;
}

/// A **clean-session** connect to the durable owner of a *cold* group must not stall
/// (ADR 0017): the clean-start discard does a durable `remove` that can trigger a
/// first-touch group recovery on the owner; doing it inline would freeze the hub and
/// delay the CONNACK. With the discard off the hub loop, a fresh clean client connects
/// and round-trips promptly on a three-node cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_clean_session_client_connects_promptly_on_the_group_owner() {
    let a = start_durable_node("dur-a", vec![]).await; // founder
    let b = start_durable_node("dur-b", vec![a.swim_addr.clone()]).await;
    let c = start_durable_node("dur-c", vec![a.swim_addr.clone()]).await;
    let nodes = [&a, &b, &c];

    wait_until(Duration::from_secs(20), || {
        nodes
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 3)
    })
    .await;
    wait_until(Duration::from_secs(30), || {
        nodes.iter().all(|n| n.plane.voter_count() == 3)
    })
    .await;

    // Connect a clean subscriber to the **owner** of its group, so its clean-start
    // discard hits the cold-group durable `remove` path that used to stall inline.
    let sub_id = "clean-sub";
    let sub_owner = a.placement.read().unwrap().owner(sub_id);
    let node = nodes.iter().find(|n| n.node_id == sub_owner).unwrap();

    let (mut sub, present) =
        common::Client::connect_v311_within(node.client_addr, sub_id, true, Duration::from_secs(8))
            .await
            .expect("a clean CONNACK must not stall on the cold group owner");
    assert!(!present, "a clean session reports no prior state");
    sub.subscribe(1, "ct", QoS::AtMostOnce).await;

    // A clean publisher on the same node; the QoS-0 message routes locally to the sub.
    let (mut pubr, _) = common::Client::connect_v311_within(
        node.client_addr,
        "clean-pub",
        true,
        Duration::from_secs(8),
    )
    .await
    .expect("a clean CONNACK must not stall");
    pubr.publish("ct", b"hello", QoS::AtMostOnce, None, vec![])
        .await;

    let p = sub.expect_publish().await;
    assert_eq!(&p.payload[..], b"hello");
}

/// Client-observable durable failover (ADR 0016 phase 1 + ADR 0017): a **persistent**
/// client whose owner is killed reconnects to the **new owner** and resumes its session
/// (`session_present=true`). Phase 1 keeps the new owner's replica set correct (no
/// resurrected corpse); ADR 0017 makes the attach **wait** for the group's lease to
/// reassign rather than reporting the recoverable session as absent. The CONNACK is
/// either a resumed session or a Server-unavailable retry — never a silent fresh session.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_persistent_client_resumes_its_session_on_the_new_owner_after_takeover() {
    let a = start_durable_node("dur-a", vec![]).await; // founder
    let b = start_durable_node("dur-b", vec![a.swim_addr.clone()]).await;
    let c = start_durable_node("dur-c", vec![a.swim_addr.clone()]).await;
    let nodes = [&a, &b, &c];

    wait_until(Duration::from_secs(20), || {
        nodes
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 3)
    })
    .await;
    wait_until(Duration::from_secs(30), || {
        nodes.iter().all(|n| n.plane.voter_count() == 3)
    })
    .await;

    // A persistent client establishes a session on its owner: clean_session=false + a
    // subscription writes the session meta + subscription durably (quorum-replicated).
    // The attach waits for the lease (ADR 0017), so the first cold connect may take a
    // moment — allow more than the harness's 2s default for the CONNACK.
    let client_id = "failover-resume-1";
    let owner = a.placement.read().unwrap().owner(client_id);
    let owner_node = nodes.iter().find(|n| n.node_id == owner).unwrap();
    {
        let (mut persistent, present) = loop {
            if let Some(ok) = common::Client::connect_v311_within(
                owner_node.client_addr,
                client_id,
                false,
                Duration::from_secs(8),
            )
            .await
            {
                break ok;
            }
        };
        assert!(
            !present,
            "a brand-new persistent session has no prior state"
        );
        persistent.subscribe(1, "t", QoS::AtLeastOnce).await;
        // Drop the connection (client goes offline); the durable session remains.
    }

    // Kill the owner; the survivors drop it and reassign the group to a survivor.
    owner_node.kill();
    let survivors: Vec<&&DurableNode> = nodes.iter().filter(|n| n.node_id != owner).collect();
    wait_until(Duration::from_secs(20), || {
        survivors
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 2)
    })
    .await;
    let new_owner = survivors[0].placement.read().unwrap().owner(client_id);
    assert_ne!(new_owner, owner, "a survivor must take over the group");
    let new_owner_node = survivors.iter().find(|n| n.node_id == new_owner).unwrap();

    // Reconnect to the new owner. Its attach waits for the lease to reassign and then
    // recovers the session from a quorum of the surviving replicas — resuming it
    // (session_present=true), never silently resetting it. We retry the connect to ride
    // out the brief lease handoff (a refused attempt comes back as `None`).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some((_client, true)) = common::Client::connect_v311_within(
            new_owner_node.client_addr,
            client_id,
            false,
            Duration::from_secs(8),
        )
        .await
        {
            break; // the new owner recovered and resumed the durable session.
        }
        assert!(
            Instant::now() < deadline,
            "the new owner never resumed the persistent session after takeover"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Client-observable relocation **with a message in flight**: a persistent client
/// subscribes then goes offline; a message published while it is away is durably
/// queued; the owner is killed; the client reconnects to the new owner and the queued
/// message is **replayed** to it. Proves a quorum-durable offline message survives a
/// cross-node takeover and reaches the client end to end (ADR 0001 §5, ADR 0017).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_queued_message_is_replayed_to_the_client_after_takeover() {
    let a = start_durable_node("dur-a", vec![]).await; // founder
    let b = start_durable_node("dur-b", vec![a.swim_addr.clone()]).await;
    let c = start_durable_node("dur-c", vec![a.swim_addr.clone()]).await;
    let nodes = [&a, &b, &c];

    wait_until(Duration::from_secs(20), || {
        nodes
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 3)
    })
    .await;
    wait_until(Duration::from_secs(30), || {
        nodes.iter().all(|n| n.plane.voter_count() == 3)
    })
    .await;

    let client_id = "failover-queue-1";
    let owner = a.placement.read().unwrap().owner(client_id);
    let owner_node = nodes.iter().find(|n| n.node_id == owner).unwrap();

    // The persistent subscriber establishes its session (meta + subscription durable),
    // then goes offline.
    {
        let (mut sub, present) = loop {
            if let Some(ok) = common::Client::connect_v311_within(
                owner_node.client_addr,
                client_id,
                false,
                Duration::from_secs(8),
            )
            .await
            {
                break ok;
            }
        };
        assert!(
            !present,
            "a brand-new persistent session has no prior state"
        );
        sub.subscribe(1, "t", QoS::AtLeastOnce).await;
        // Drop the connection: the subscriber is now offline but its session persists.
    }

    // A message queued for the offline subscriber. The durability-critical enqueue
    // commits only across a quorum (owner + ≥1 follower), so once it returns Ok the
    // message is guaranteed to survive the owner's loss. (The hub's publish→offline-
    // queue path is covered by unit tests; here we drive the durable queue directly so
    // the failover assertion is deterministic.)
    let cid = ClientId(client_id.to_string());
    let msg = Message::new(
        "t".to_string(),
        bytes::Bytes::from_static(b"in-flight"),
        QoS::AtLeastOnce,
        false,
    );
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if owner_node.store.enqueue(&cid, &msg).await.is_ok() {
            break; // committed across a quorum.
        }
        assert!(
            Instant::now() < deadline,
            "the offline message never durably committed on the owner"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Kill the owner; the survivors reassign the group to a survivor.
    owner_node.kill();
    let survivors: Vec<&&DurableNode> = nodes.iter().filter(|n| n.node_id != owner).collect();
    wait_until(Duration::from_secs(20), || {
        survivors
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 2)
    })
    .await;
    let new_owner = survivors[0].placement.read().unwrap().owner(client_id);
    assert_ne!(new_owner, owner, "a survivor must take over the group");
    let new_owner_node = survivors.iter().find(|n| n.node_id == new_owner).unwrap();

    // Reconnect to the new owner; on resume the queued message replays to the client.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut sub = loop {
        if let Some((client, true)) = common::Client::connect_v311_within(
            new_owner_node.client_addr,
            client_id,
            false,
            Duration::from_secs(8),
        )
        .await
        {
            break client; // resumed the session; the replay follows the CONNACK.
        }
        assert!(
            Instant::now() < deadline,
            "the new owner never resumed the persistent session after takeover"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let replayed = sub.expect_publish().await;
    assert_eq!(
        &replayed.payload[..],
        b"in-flight",
        "the queued message must replay to the client through the new owner"
    );
}

/// QoS-2 **inbound exactly-once across an owner failover** (ADR 0001 §5, ADR 0006 §4): the
/// received-packet-id dedup set is replicated session state, so a redelivered PUBLISH (same
/// packet id) after a takeover is de-duplicated by the **new** owner — it is not processed a
/// second time. This is the headline exactly-once guarantee, exercised over a *real* cluster
/// failover: the owner records the receipt (quorum-durable), is killed, and a survivor — which
/// must rebuild the dedup set from a quorum of replicas — still sees the id as a duplicate.
///
/// Like the queue-replay takeover test, this drives the durable store directly so the
/// failover assertion is deterministic (the hub's PUBLISH→`record_received` dedup path is
/// covered by `qos2.rs`); here we prove the *replicated state* survives the owner change.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn qos2_inbound_dedup_survives_owner_takeover() {
    let a = start_durable_node("dur-a", vec![]).await; // founder
    let b = start_durable_node("dur-b", vec![a.swim_addr.clone()]).await;
    let c = start_durable_node("dur-c", vec![a.swim_addr.clone()]).await;
    let nodes = [&a, &b, &c];

    wait_until(Duration::from_secs(20), || {
        nodes
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 3)
    })
    .await;
    wait_until(Duration::from_secs(30), || {
        nodes.iter().all(|n| n.plane.voter_count() == 3)
    })
    .await;

    let client_id = "failover-qos2-1";
    let cid = ClientId(client_id.to_string());
    let owner = a.placement.read().unwrap().owner(client_id);
    let owner_node = nodes.iter().find(|n| n.node_id == owner).unwrap();

    // The owner records the inbound QoS-2 PUBLISH (packet id 5). The receipt commits only
    // across a quorum (owner + ≥1 follower), so once it returns Ok it is guaranteed to
    // survive the owner's loss. First receipt → `true`.
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if let Ok(newly) = owner_node.store.record_received(&cid, 5).await {
            assert!(newly, "the first receipt of packet id 5 is new");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the QoS-2 receipt never durably committed on the owner"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Kill the owner; a survivor takes over the group.
    owner_node.kill();
    let survivors: Vec<&&DurableNode> = nodes.iter().filter(|n| n.node_id != owner).collect();
    wait_until(Duration::from_secs(20), || {
        survivors
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 2)
    })
    .await;
    let new_owner = survivors[0].placement.read().unwrap().owner(client_id);
    assert_ne!(new_owner, owner, "a survivor must take over the group");
    let new_owner_node = survivors.iter().find(|n| n.node_id == new_owner).unwrap();

    // On the new owner, the redelivered PUBLISH (same packet id 5) must be seen as a
    // DUPLICATE — the dedup set was rebuilt from a quorum of replicas during takeover, so
    // `record_received` returns `false` and the message is not delivered a second time.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(newly) = new_owner_node.store.record_received(&cid, 5).await {
            assert!(
                !newly,
                "after takeover, the redelivered packet id 5 must be a duplicate \
                 (the dedup set survived the owner change) — exactly-once preserved"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the new owner never recovered the QoS-2 dedup set after takeover"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // And the recovered dedup window still lists id 5 (authoritative resume state).
    assert_eq!(
        new_owner_node.store.received(&cid).await.unwrap(),
        vec![5],
        "the recovered session must report packet id 5 as still received-not-completed"
    );
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
    let msg = Message::new(
        "t".to_string(),
        bytes::Bytes::from_static(b"survives-takeover"),
        QoS::AtLeastOnce,
        false,
    );
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

    // Generous: post-death takeover means a Raft reconfiguration (drop the dead voter)
    // plus a first-touch quorum log rebuild, which can run long on a contended CI runner.
    // A deterministic simulation harness (ADR 0024-T7) would remove the wall-clock margin.
    let deadline = Instant::now() + Duration::from_secs(120);
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

/// ADR 0021 (bounded lease voters) — integration. A **five-node** durable cluster with a
/// voter cap of `3` forms a *bounded* voter set (exactly 3 voters, 2 learners) instead of
/// a 5-voter group, and a session owned by a **learner** (a non-voting member) is durable
/// and survives both a non-voter and a voter failure. This exercises the whole wiring:
/// ownership (HRW) and session-data replication (R = 3) are independent of the lease voter
/// set, so a learner can own and serve sessions, and the sticky vacancy-fill promotes a
/// learner to voter live when a voter is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)] // a multi-phase 5-node integration scenario
async fn a_bounded_voter_cluster_keeps_a_learner_owned_session_through_failures() {
    use mqtt_cluster::lease_raft::RaftNodeId;
    use mqtt_cluster::node_registry::raft_id;
    use std::collections::BTreeSet;

    let a = start_durable_node_capped("bv-a", vec![], 3, None).await; // founder
    let b = start_durable_node_capped("bv-b", vec![a.swim_addr.clone()], 3, None).await;
    let c = start_durable_node_capped("bv-c", vec![a.swim_addr.clone()], 3, None).await;
    let nd = start_durable_node_capped("bv-d", vec![a.swim_addr.clone()], 3, None).await;
    let ne = start_durable_node_capped("bv-e", vec![a.swim_addr.clone()], 3, None).await;
    let nodes = [&a, &b, &c, &nd, &ne];

    // SWIM converges: every node sees all five members.
    wait_until(Duration::from_secs(30), || {
        nodes
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 5)
    })
    .await;

    // The lease group BOUNDS its voter set: exactly 3 voters even with 5 members.
    wait_until(Duration::from_secs(45), || {
        nodes.iter().all(|n| n.plane.voter_count() == 3)
    })
    .await;

    // Voter identity from the settled membership (all nodes agree once bounded; read it
    // from the founder). Snapshot it now, before any kills.
    let voter_rids: BTreeSet<RaftNodeId> = a
        .plane
        .raft()
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    let is_voter = |n: &DurableNode| voter_rids.contains(&raft_id(&n.node_id));
    let learners: Vec<&&DurableNode> = nodes.iter().filter(|n| !is_voter(n)).collect();
    assert_eq!(
        nodes.iter().filter(|n| is_voter(n)).count(),
        3,
        "the lease group is bounded to 3 voters"
    );
    assert_eq!(learners.len(), 2, "the other two members are learners");

    // A client whose placement owner is a **learner** (ownership is independent of voting).
    let client = (0..4000)
        .map(|i| ClientId(format!("bv-sess-{i}")))
        .find(|c| {
            let owner = a.placement.read().unwrap().owner(&c.0);
            learners.iter().any(|n| n.node_id == owner)
        })
        .expect("some client hashes to a learner owner");
    let owner_id = a.placement.read().unwrap().owner(&client.0);
    let owner_node = *nodes.iter().find(|n| n.node_id == owner_id).unwrap();
    assert!(
        !is_voter(owner_node),
        "the chosen session owner is a non-voting learner"
    );

    // Durably enqueue on the learner owner — quorum-replicated over its R=3 replica set.
    let msg = Message::new(
        "t".to_string(),
        bytes::Bytes::from_static(b"learner-owned"),
        QoS::AtLeastOnce,
        false,
    );
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if owner_node.store.enqueue(&client, &msg).await.is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "durable enqueue on a learner owner never committed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // The learner owner serves it back — a non-voter reads/serves its lease (ADR 0021 §2).
    let pending = owner_node.store.pending(&client, 0, 100).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(&pending[0].message.payload[..], b"learner-owned");

    // Pick failure victims that keep the session's data quorum: the replica set R has 3 of
    // the 5 nodes (incl. the owner). Kill the OTHER learner (a non-voter), then a voter
    // OUTSIDE R — so at most one of R's three replicas is lost (≥2/3 survive) and the
    // learner owner itself survives, keeping the session readable throughout.
    let replica_set: BTreeSet<NodeId> = a
        .placement
        .read()
        .unwrap()
        .replica_set(&client.0)
        .into_iter()
        .collect();
    let victim_nonvoter = learners
        .iter()
        .find(|n| n.node_id != owner_id)
        .map(|n| n.node_id.clone())
        .expect("a second learner exists");
    let victim_voter = nodes
        .iter()
        .find(|n| is_voter(n) && !replica_set.contains(&n.node_id))
        .map(|n| n.node_id.clone())
        .expect("a voter outside the replica set exists");

    // --- survive a NON-VOTER (learner) failure ---
    nodes
        .iter()
        .find(|n| n.node_id == victim_nonvoter)
        .unwrap()
        .kill();
    wait_until(Duration::from_secs(25), || {
        nodes
            .iter()
            .filter(|n| n.node_id != victim_nonvoter)
            .all(|n| n.placement.read().unwrap().member_count() == 4)
    })
    .await;
    let pending = owner_node.store.pending(&client, 0, 100).await.unwrap();
    assert_eq!(
        pending.len(),
        1,
        "the learner-owned session survives a non-voter failure"
    );

    // --- survive a VOTER failure (sticky vacancy-fill promotes a learner live) ---
    nodes
        .iter()
        .find(|n| n.node_id == victim_voter)
        .unwrap()
        .kill();
    let survivors: Vec<&&DurableNode> = nodes
        .iter()
        .filter(|n| n.node_id != victim_nonvoter && n.node_id != victim_voter)
        .collect();
    wait_until(Duration::from_secs(30), || {
        survivors
            .iter()
            .all(|n| n.placement.read().unwrap().member_count() == 3)
    })
    .await;
    // The three survivors (now ≤ cap) all become voters — a learner is promoted to refill
    // the voter set after the loss (vacancy-fill, live).
    wait_until(Duration::from_secs(45), || {
        survivors.iter().all(|n| n.plane.voter_count() == 3)
    })
    .await;
    // The session — still owned by the surviving learner-turned-voter — replays intact.
    let deadline = Instant::now() + Duration::from_secs(60);
    let pending = loop {
        if let Ok(p) = owner_node.store.pending(&client, 0, 100).await {
            if !p.is_empty() {
                break p;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the session did not survive the voter failure"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(pending.len(), 1);
    assert_eq!(&pending[0].message.payload[..], b"learner-owned");
}
