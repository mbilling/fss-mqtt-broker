//! Multi-node SWIM test over real UDP loopback: nodes discover each other
//! (convergence), detect a stopped node as dead (failure detection), and —
//! with gossip authentication — ignore nodes that lack the cluster key.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mqtt_cluster::swim::{Config, Kind, MemberState, Message, Swim};
use mqtt_cluster::swim_auth::{GossipSign, GossipVerify, SwimAuth, KEY_LEN};
use mqtt_cluster::swim_driver::{run, MembershipEvent};
use mqtt_cluster::NodeId;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A deterministic stand-in for the real PKI signer (ADR 0022): its certificate encodes its
/// CN as `cert:<cn>` and its signature as `sig:<cn>:<payload>`, so the verifier can recover
/// the signer's identity. The real crypto is exercised in `mqtt-auth`; here we test the
/// driver's identity binding (a datagram's authenticated CN must equal its SWIM `from`).
struct StubSigner {
    cn: String,
    cert: Vec<u8>,
}
impl StubSigner {
    fn new(cn: &str) -> Self {
        Self {
            cn: cn.to_string(),
            cert: format!("cert:{cn}").into_bytes(),
        }
    }
}
impl GossipSign for StubSigner {
    fn cert_der(&self) -> &[u8] {
        &self.cert
    }
    fn sign(&self, payload: &[u8]) -> Vec<u8> {
        let mut s = format!("sig:{}:", self.cn).into_bytes();
        s.extend_from_slice(payload);
        s
    }
}

struct StubVerifier;
impl GossipVerify for StubVerifier {
    fn verify(&self, cert_der: &[u8], payload: &[u8], sig: &[u8]) -> Option<String> {
        let cn = std::str::from_utf8(cert_der).ok()?.strip_prefix("cert:")?;
        let expected: Vec<u8> = [format!("sig:{cn}:").as_bytes(), payload].concat();
        (sig == expected).then(|| cn.to_string())
    }
}

/// A signing `SwimAuth` (require mode) for node `cn`, on the shared key `key`.
fn signed_auth(key: u8, cn: &str) -> SwimAuth {
    SwimAuth::new(&[key; KEY_LEN]).with_signing(
        Arc::new(StubSigner::new(cn)),
        Arc::new(StubVerifier),
        true,
    )
}

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

/// Spawn a node whose driver performs a **graceful SWIM leave** when the returned
/// trigger is fired (ADR 0019 §2), instead of running until aborted.
async fn spawn_leavable_node(
    id: &str,
    seeds: Vec<String>,
) -> (String, Node, tokio::sync::oneshot::Sender<()>) {
    let (leave_tx, leave_rx) = tokio::sync::oneshot::channel::<()>();
    let (addr, node) = spawn_node_inner(id, seeds, None, async move {
        let _ = leave_rx.await;
    })
    .await;
    (addr, node, leave_tx)
}

async fn spawn_node(id: &str, seeds: Vec<String>, auth: Option<SwimAuth>) -> (String, Node) {
    spawn_node_inner(id, seeds, auth, std::future::pending()).await
}

/// Spawn a node that signs its gossip as itself (ADR 0022, require mode).
async fn spawn_signed_node(id: &str, seeds: Vec<String>, key: u8) -> (String, Node) {
    spawn_node_inner(
        id,
        seeds,
        Some(signed_auth(key, id)),
        std::future::pending(),
    )
    .await
}

async fn spawn_node_inner(
    id: &str,
    seeds: Vec<String>,
    auth: Option<SwimAuth>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> (String, Node) {
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
    let handle = tokio::spawn(run(
        socket,
        swim,
        Duration::from_millis(20),
        tx,
        auth,
        shutdown,
    ));
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

/// ADR 0019 §2: a node that leaves **gracefully** is seen `Dead` by its peer almost
/// immediately — well before failure detection (one probe period + ack timeout +
/// suspicion window) could even begin to conclude it dead. This is the latency a
/// routine restart/upgrade saves on every node.
#[tokio::test]
async fn a_graceful_leave_is_seen_dead_faster_than_failure_detection() {
    let (addr1, n1) = spawn_node("g1", vec![], None).await;
    let (_addr2, n2, leave2) = spawn_leavable_node("g2", vec![addr1.clone()]).await;

    let converged = wait_for(Duration::from_secs(5), || {
        sees(&n1, "g2", MemberState::Alive) && sees(&n2, "g1", MemberState::Alive)
    })
    .await;
    assert!(converged, "the two nodes failed to converge");

    // g2 announces a graceful leave. The suspicion window alone is 500ms (and failure
    // detection cannot even *start* concluding Dead until a probe has timed out), so a
    // 400ms bound proves n1 learned it via the direct departure announcement, not
    // failure detection.
    leave2.send(()).unwrap();
    let left = wait_for(Duration::from_millis(400), || {
        sees(&n1, "g2", MemberState::Dead)
    })
    .await;
    assert!(
        left,
        "n1 did not see the graceful leave as Dead within the no-failure-detection window"
    );

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

/// ADR 0022: two nodes that sign as themselves (require mode) converge — each signature
/// verifies and its authenticated certificate CN matches the sender's id.
#[tokio::test]
async fn signed_gossip_converges() {
    let key = 0x33;
    let (addr1, n1) = spawn_signed_node("sg1", vec![], key).await;
    let (_addr2, n2) = spawn_signed_node("sg2", vec![addr1.clone()], key).await;

    let ok = wait_for(Duration::from_secs(5), || {
        sees(&n1, "sg2", MemberState::Alive) && sees(&n2, "sg1", MemberState::Alive)
    })
    .await;
    assert!(ok, "signed nodes failed to converge");

    n1.handle.abort();
    n2.handle.abort();
}

/// ADR 0022: a datagram whose authenticated identity (certificate CN) does not match its
/// claimed SWIM `from` is dropped — a node holding the shared key cannot impersonate
/// another. We forge a Join "from ghost" that is validly signed by "evil" (a key holder),
/// and confirm the victim never learns "ghost"; a correctly-attributed Join from "evil"
/// IS learned, proving it is specifically the identity mismatch that is rejected.
#[tokio::test]
async fn a_forged_sender_identity_is_rejected() {
    let key = 0x44;
    let (victim_addr, victim) = spawn_signed_node("victim", vec![], key).await;
    let evil = signed_auth(key, "evil");
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let seal = |m: &Message| evil.seal(&bincode::serialize(m).unwrap());

    // Control: a Join honestly attributed to "evil" is accepted (the victim learns it).
    let honest = Message {
        from: "evil".into(),
        from_addr: sock.local_addr().unwrap().to_string(),
        from_peer_addr: String::new(),
        kind: Kind::Join,
        gossip: vec![],
    };
    sock.send_to(&seal(&honest), &victim_addr).await.unwrap();
    assert!(
        wait_for(Duration::from_secs(3), || knows(&victim, "evil")).await,
        "an honestly-signed Join should be learned"
    );

    // Forge: "evil" signs a Join claiming to be "ghost". CN(evil) != from(ghost) → dropped.
    let forged = Message {
        from: "ghost".into(),
        from_addr: "10.0.0.9:1".into(),
        from_peer_addr: String::new(),
        kind: Kind::Join,
        gossip: vec![],
    };
    sock.send_to(&seal(&forged), &victim_addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !knows(&victim, "ghost"),
        "a forged sender identity must be rejected"
    );

    victim.handle.abort();
}
