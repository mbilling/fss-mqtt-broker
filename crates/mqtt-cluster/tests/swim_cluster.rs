//! Multi-node SWIM test over real UDP loopback: nodes discover each other
//! (convergence) and detect a stopped node as dead (failure detection).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mqtt_cluster::swim::{Config, MemberState, Swim};
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
        indirect_probes: 2,
        gossip_fanout: 8,
        gossip_multiplier: 4,
    }
}

/// A running node: its driver task and the membership view it has observed.
struct Node {
    handle: JoinHandle<()>,
    view: Arc<Mutex<HashMap<String, MemberState>>>,
}

async fn spawn_node(id: &str, seeds: Vec<String>) -> (String, Node) {
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
    let handle = tokio::spawn(run(socket, swim, Duration::from_millis(20), tx));
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

#[tokio::test]
async fn three_nodes_converge_then_detect_failure() {
    // n1 is the seed; n2 and n3 join through it.
    let (addr1, n1) = spawn_node("n1", vec![]).await;
    let (_addr2, n2) = spawn_node("n2", vec![addr1.clone()]).await;
    let (_addr3, n3) = spawn_node("n3", vec![addr1.clone()]).await;

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
    let (addr1, n1) = spawn_node("a1", vec![]).await;
    let (_addr2, n2) = spawn_node("a2", vec![addr1]).await;

    let ok = wait_for(Duration::from_secs(5), || {
        sees(&n1, "a2", MemberState::Alive) && sees(&n2, "a1", MemberState::Alive)
    })
    .await;
    assert!(ok, "two nodes failed to discover each other");

    n1.handle.abort();
    n2.handle.abort();
}
