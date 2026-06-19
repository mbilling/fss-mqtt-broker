//! Multi-node SWIM test over real UDP loopback: nodes discover each other
//! (convergence), detect a stopped node as dead (failure detection), and —
//! with gossip authentication — ignore nodes that lack the cluster key.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mqtt_cluster::swim::{Config, MemberState, Swim};
use mqtt_cluster::swim_auth::{SwimAuth, KEY_LEN};
use mqtt_cluster::swim_driver::{run, MembershipEvent};
use mqtt_cluster::NodeId;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Tight timings so the test converges and detects failure in ~2s.
fn cfg() -> Config {
    Config {
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

/// A running node: its driver task and the membership view it has observed.
struct Node {
    handle: JoinHandle<()>,
    view: Arc<Mutex<HashMap<String, MemberState>>>,
}

async fn spawn_node(id: &str, seeds: Vec<String>, auth: Option<SwimAuth>) -> (String, Node) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap().to_string();
    // No peer-link transport in this test; the routing address is a placeholder.
    let peer_addr = format!("{addr}-peer");
    let swim = Swim::new(
        NodeId(id.to_string()),
        addr.clone(),
        peer_addr,
        cfg(),
        seeds,
    );

    let (tx, mut rx) = mpsc::unbounded_channel::<MembershipEvent>();
    let view: Arc<Mutex<HashMap<String, MemberState>>> = Arc::new(Mutex::new(HashMap::new()));
    let view2 = view.clone();
    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            view2.lock().unwrap().insert(ev.id.0, ev.state);
        }
    });

    // Tick well under the ack timeout so deadlines are observed promptly.
    let handle = tokio::spawn(run(socket, swim, Duration::from_millis(20), tx, auth));
    (addr, Node { handle, view })
}

/// Poll `cond` every 25ms until it holds or the deadline passes.
async fn wait_for(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cond()
}

fn sees(node: &Node, id: &str, state: MemberState) -> bool {
    node.view.lock().unwrap().get(id) == Some(&state)
}

/// Whether `node` has `id` in its membership view at all, in **any** state.
/// Negative membership assertions must use this, not `sees`: an intruder that
/// was admitted and then drifted to `Suspect`/`Dead` would slip past an
/// `Alive`-only check.
fn knows(node: &Node, id: &str) -> bool {
    node.view.lock().unwrap().contains_key(id)
}

#[tokio::test]
async fn three_nodes_converge_then_detect_failure() {
    // n1 is the seed; n2 and n3 join through it.
    let (addr1, n1) = spawn_node("n1", vec![], None).await;
    let (_addr2, n2) = spawn_node("n2", vec![addr1.clone()], None).await;
    let (_addr3, n3) = spawn_node("n3", vec![addr1.clone()], None).await;

    // Convergence: every node sees the other two as Alive.
    let converged = wait_for(Duration::from_secs(6), || {
        sees(&n1, "n2", MemberState::Alive)
            && sees(&n1, "n3", MemberState::Alive)
            && sees(&n2, "n1", MemberState::Alive)
            && sees(&n2, "n3", MemberState::Alive)
            && sees(&n3, "n1", MemberState::Alive)
            && sees(&n3, "n2", MemberState::Alive)
    })
    .await;
    assert!(converged, "cluster failed to converge");

    // Failure detection: stop n3 and verify the others mark it Dead.
    n3.handle.abort();
    let detected = wait_for(Duration::from_secs(6), || {
        sees(&n1, "n3", MemberState::Dead) && sees(&n2, "n3", MemberState::Dead)
    })
    .await;
    assert!(detected, "surviving nodes did not detect n3 as dead");

    // n1 and n2 still consider each other alive.
    assert!(sees(&n1, "n2", MemberState::Alive));
    assert!(sees(&n2, "n1", MemberState::Alive));

    n1.handle.abort();
    n2.handle.abort();
}

#[tokio::test]
async fn two_nodes_discover_each_other() {
    let (addr1, n1) = spawn_node("a1", vec![], None).await;
    let (_addr2, n2) = spawn_node("a2", vec![addr1], None).await;

    let ok = wait_for(Duration::from_secs(5), || {
        sees(&n1, "a2", MemberState::Alive) && sees(&n2, "a1", MemberState::Alive)
    })
    .await;
    assert!(ok, "two nodes failed to discover each other");

    n1.handle.abort();
    n2.handle.abort();
}

/// ADR 0003: nodes sharing the gossip key converge; a node with the wrong key
/// (or none) is invisible to the cluster and sees nothing of it.
#[tokio::test]
async fn keyed_cluster_ignores_nodes_without_the_key() {
    let key = |b: u8| Some(SwimAuth::new(&[b; KEY_LEN]));

    let (addr1, n1) = spawn_node("s1", vec![], key(9)).await;
    let (_addr2, n2) = spawn_node("s2", vec![addr1.clone()], key(9)).await;
    let converged = wait_for(Duration::from_secs(5), || {
        sees(&n1, "s2", MemberState::Alive) && sees(&n2, "s1", MemberState::Alive)
    })
    .await;
    assert!(converged, "keyed nodes failed to converge");

    // Wrong key and no key at all: both try to join through the seed.
    let (_a3, wrong) = spawn_node("wrong-key", vec![addr1.clone()], key(1)).await;
    let (_a4, unkeyed) = spawn_node("unkeyed", vec![addr1], None).await;

    // Give them ample time to be (wrongly) admitted before asserting. The
    // property is absence from the membership view in ANY state — an intruder
    // admitted and then suspected would still be a breach.
    let intruder_known = wait_for(Duration::from_secs(2), || {
        knows(&n1, "wrong-key")
            || knows(&n2, "wrong-key")
            || knows(&n1, "unkeyed")
            || knows(&n2, "unkeyed")
    })
    .await;
    assert!(
        !intruder_known,
        "a node without the cluster key entered the membership view"
    );
    assert!(
        !knows(&wrong, "s1") && !knows(&wrong, "s2"),
        "the wrong-key node must learn nothing about the cluster"
    );
    assert!(
        !knows(&unkeyed, "s1") && !knows(&unkeyed, "s2"),
        "the unkeyed node must learn nothing about the cluster"
    );

    for n in [n1, n2, wrong, unkeyed] {
        n.handle.abort();
    }
}
