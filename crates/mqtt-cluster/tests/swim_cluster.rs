//! Multi-node SWIM test over real UDP loopback: nodes discover each other
//! (convergence), detect a stopped node as dead (failure detection), and —
//! with gossip authentication — ignore nodes that lack the cluster key.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mqtt_cluster::replay::{SeqStore, SequenceAllocator};
use mqtt_cluster::swim::{Config, Kind, MemberState, Message, Swim};
use mqtt_cluster::swim_auth::{
    GossipSign, GossipVerify, OpenReject, SwimAuth, VerifiedIdentity, KEY_LEN,
};
use mqtt_cluster::swim_driver::{run, MembershipEvent, SeqAlloc};
use mqtt_cluster::NodeId;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A deterministic stand-in for the real PKI signer (ADR 0022): its certificate encodes its
/// CN as `cert:<cn>` (or `cert:<cn>@<domain>` when it carries a CA-attested failure-domain
/// label, ADR 0016 T6) and its signature as `sig:<cn>:<payload>`, so the verifier can recover
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

    /// A signer whose stub certificate also attests failure domain `domain`.
    fn in_domain(cn: &str, domain: &str) -> Self {
        Self {
            cn: cn.to_string(),
            cert: format!("cert:{cn}@{domain}").into_bytes(),
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
    fn verify(
        &self,
        cert_der: &[u8],
        payload: &[u8],
        sig: &[u8],
    ) -> Result<VerifiedIdentity, OpenReject> {
        let subject = std::str::from_utf8(cert_der)
            .ok()
            .and_then(|s| s.strip_prefix("cert:"))
            .ok_or(OpenReject::Auth)?;
        let (cn, domain) = match subject.split_once('@') {
            Some((cn, d)) => (cn, Some(d.to_string())),
            None => (subject, None),
        };
        let expected: Vec<u8> = [format!("sig:{cn}:").as_bytes(), payload].concat();
        if sig == expected {
            Ok(VerifiedIdentity {
                cn: cn.to_string(),
                failure_domain: domain,
            })
        } else {
            Err(OpenReject::Auth)
        }
    }
}

/// A signing `SwimAuth` (the strict signed posture) for node `cn`, on the shared key `key`.
fn signed_auth(key: u8, cn: &str) -> SwimAuth {
    SwimAuth::new(&[key; KEY_LEN])
        .with_signing(Arc::new(StubSigner::new(cn)), Arc::new(StubVerifier))
}

/// A signing `SwimAuth` whose stub certificate attests failure domain `domain` (ADR 0016 T6).
fn attested_auth(key: u8, cn: &str, domain: &str) -> SwimAuth {
    SwimAuth::new(&[key; KEY_LEN]).with_signing(
        Arc::new(StubSigner::in_domain(cn, domain)),
        Arc::new(StubVerifier),
    )
}

/// An in-memory `SeqStore` for tests (anti-replay needs no real persistence here).
#[derive(Default)]
struct MemSeqStore {
    reserved: std::sync::atomic::AtomicU64,
}
impl SeqStore for MemSeqStore {
    fn reserved(&self) -> u64 {
        self.reserved.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn persist(&mut self, reserved_until: u64) {
        self.reserved
            .store(reserved_until, std::sync::atomic::Ordering::Relaxed);
    }
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

/// A running node: its driver task, the membership view it has observed, and the
/// per-reason count of gossip datagrams its driver dropped (ADR 0003-T6).
struct Node {
    handle: JoinHandle<()>,
    view: Arc<Mutex<HashMap<String, MemberState>>>,
    rejects: Arc<Mutex<HashMap<String, u64>>>,
    /// The failure-domain label this node has learned for each peer via gossip
    /// (ADR 0016 T5).
    domains: Arc<Mutex<HashMap<String, String>>>,
}

impl Node {
    /// How many inbound datagrams the driver dropped for `reason`.
    fn reject_count(&self, reason: &str) -> u64 {
        self.rejects
            .lock()
            .unwrap()
            .get(reason)
            .copied()
            .unwrap_or(0)
    }

    /// The failure-domain label this node has learned for peer `id` (ADR 0016 T5).
    fn domain_of(&self, id: &str) -> Option<String> {
        self.domains.lock().unwrap().get(id).cloned()
    }
}

/// Spawn a node whose driver performs a **graceful SWIM leave** when the returned
/// trigger is fired (ADR 0019 §2), instead of running until aborted.
async fn spawn_leavable_node(
    id: &str,
    seeds: Vec<String>,
) -> (String, Node, tokio::sync::oneshot::Sender<()>) {
    let (leave_tx, leave_rx) = tokio::sync::oneshot::channel::<()>();
    let (addr, node) = spawn_node_inner(id, seeds, None, None, None, None, async move {
        let _ = leave_rx.await;
    })
    .await;
    (addr, node, leave_tx)
}

async fn spawn_node(id: &str, seeds: Vec<String>, auth: Option<SwimAuth>) -> (String, Node) {
    spawn_node_inner(id, seeds, auth, None, None, None, std::future::pending()).await
}

/// Spawn a node that binds a real socket but ADVERTISES `advertise` as its own SWIM
/// datagram address — used to reproduce the k8s `0.0.0.0`-advertise isolation.
async fn spawn_node_advertising(
    id: &str,
    seeds: Vec<String>,
    advertise: &str,
) -> (String, Node) {
    spawn_node_inner(
        id,
        seeds,
        None,
        None,
        None,
        Some(advertise.to_string()),
        std::future::pending(),
    )
    .await
}

/// Spawn a node that advertises its own failure-domain label over gossip (ADR 0016 T5).
async fn spawn_node_in_domain(id: &str, seeds: Vec<String>, domain: &str) -> (String, Node) {
    spawn_node_inner(id, seeds, None, None, Some(domain), None, std::future::pending()).await
}

/// Spawn a node that signs its gossip as itself (ADR 0022, require mode).
async fn spawn_signed_node(id: &str, seeds: Vec<String>, key: u8) -> (String, Node) {
    spawn_node_inner(
        id,
        seeds,
        Some(signed_auth(key, id)),
        None,
        None,
        None,
        std::future::pending(),
    )
    .await
}

/// Spawn a node that signs **and sequences** its gossip (ADR 0023, require mode), with an
/// in-memory sequence allocator — so the driver windows inbound sequenced datagrams.
async fn spawn_sequenced_node(id: &str, seeds: Vec<String>, key: u8) -> (String, Node) {
    let auth = signed_auth(key, id).with_sequencing();
    let alloc = SequenceAllocator::open(Box::new(MemSeqStore::default()) as Box<dyn SeqStore>, 64);
    spawn_node_inner(
        id,
        seeds,
        Some(auth),
        Some(alloc),
        None,
        None,
        std::future::pending(),
    )
    .await
}

async fn spawn_node_inner(
    id: &str,
    seeds: Vec<String>,
    auth: Option<SwimAuth>,
    seq_alloc: Option<SeqAlloc>,
    domain: Option<&str>,
    // The address this node ADVERTISES as its own SWIM datagram address (`from_addr`).
    // `None` advertises its real bound address (the honest case). A test passes an
    // unroutable value (e.g. a `0.0.0.0` bind) to prove peers still reach it via the
    // datagram source (2026-07-20 post-mortem). The real bound address is always
    // returned for seeding.
    advertise: Option<String>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> (String, Node) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap().to_string();
    let advertised = advertise.unwrap_or_else(|| addr.clone());
    // No peer-link transport in this test; the routing address is a placeholder.
    let peer_addr = format!("{advertised}-peer");
    let swim = Swim::new(
        NodeId(id.to_string()),
        advertised,
        peer_addr,
        domain.map(str::to_string),
        cfg(),
        seeds,
    );

    let (tx, mut rx) = mpsc::unbounded_channel::<MembershipEvent>();
    let view: Arc<Mutex<HashMap<String, MemberState>>> = Arc::new(Mutex::new(HashMap::new()));
    let domains: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let view2 = view.clone();
    let domains2 = domains.clone();
    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            if let Some(d) = ev.domain {
                domains2.lock().unwrap().insert(ev.id.0.clone(), d);
            }
            view2.lock().unwrap().insert(ev.id.0, ev.state);
        }
    });

    // A reject counter that records each drop by reason, exposed on the Node for tests.
    let rejects: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(HashMap::new()));
    let rejects2 = rejects.clone();
    let reject: mqtt_cluster::swim_driver::RejectCounter = Arc::new(move |reason: &'static str| {
        *rejects2
            .lock()
            .unwrap()
            .entry(reason.to_string())
            .or_default() += 1;
    });

    // Tick well under the ack timeout so deadlines are observed promptly.
    let handle = tokio::spawn(run(
        socket,
        swim,
        Duration::from_millis(20),
        tx,
        auth,
        seq_alloc,
        Some(reject),
        shutdown,
    ));
    (
        addr,
        Node {
            handle,
            view,
            rejects,
            domains,
        },
    )
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

/// Regression for the 2026-07-20 kube-smoke isolation: a node that advertises an
/// **unroutable** SWIM datagram address (the kubernetes `0.0.0.0:<port>` bind default)
/// must still converge, because peers learn its real address from the datagram source
/// rather than trusting its self-claim.
///
/// Before the driver learned the source address, the founder learned joiners from
/// their greets (sent to its routable seed address) but every gossip *reply* went to
/// `0.0.0.0` and was black-holed — so joiners stayed isolated (`members=[self]`), their
/// placement HRW ring believed they owned every group, and durable ownership split from
/// the committed lease (permanent retained `NotOwner`). This is why the existing
/// convergence test above did not catch it: it advertises each node's *real* bound
/// address, the one case the bug does not hit.
#[tokio::test]
async fn nodes_that_advertise_an_unroutable_gossip_address_still_converge() {
    // Advertise 0.0.0.0:7946 (as under a k8s 0.0.0.0 bind) while listening on a real
    // ephemeral socket. Joiners are seeded with the founder's REAL address — that is
    // where the initial greet goes (the routable seed, ADR 0016) — but from then on
    // reachability depends entirely on peers learning the source address.
    let bogus = "0.0.0.0:7946";
    let (seed_addr, n1) = spawn_node_advertising("n1", vec![], bogus).await;
    let (_a2, n2) = spawn_node_advertising("n2", vec![seed_addr.clone()], bogus).await;
    let (_a3, n3) = spawn_node_advertising("n3", vec![seed_addr.clone()], bogus).await;

    // FULL convergence — including the joiners seeing each other and the founder. The
    // joiner-side assertions (n2/n3 see their peers) are the ones that regress without
    // the source-address fix; the founder-side ones passed even with the bug.
    let converged = wait_for(Duration::from_secs(8), || {
        sees(&n1, "n2", MemberState::Alive)
            && sees(&n1, "n3", MemberState::Alive)
            && sees(&n2, "n1", MemberState::Alive)
            && sees(&n2, "n3", MemberState::Alive)
            && sees(&n3, "n1", MemberState::Alive)
            && sees(&n3, "n2", MemberState::Alive)
    })
    .await;
    assert!(
        converged,
        "nodes advertising an unroutable gossip address failed to converge — a joiner \
         isolated to [self] (2026-07-20 post-mortem)"
    );

    n1.handle.abort();
    n2.handle.abort();
    n3.handle.abort();
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

/// ADR 0016 T5: each node advertises its own failure-domain label over the gossip
/// plane, and its peer learns it — so the failure-domain topology self-assembles over
/// the real wire, with no static cluster-uniform map.
#[tokio::test]
async fn a_nodes_failure_domain_propagates_over_gossip() {
    let (addr1, n1) = spawn_node_in_domain("d1", vec![], "rack-a").await;
    let (_addr2, n2) = spawn_node_in_domain("d2", vec![addr1], "rack-b").await;

    let converged = wait_for(Duration::from_secs(6), || {
        n1.domain_of("d2").as_deref() == Some("rack-b")
            && n2.domain_of("d1").as_deref() == Some("rack-a")
    })
    .await;
    assert!(
        converged,
        "failure-domain labels did not propagate: d1 sees d2={:?}, d2 sees d1={:?}",
        n1.domain_of("d2"),
        n2.domain_of("d1"),
    );

    n1.handle.abort();
    n2.handle.abort();
}

/// ADR 0016 T6: a CA-attested failure-domain label (carried by the node's certificate)
/// propagates with **no** self-configured label at all — relabeling a node needs only a
/// reissued certificate.
#[tokio::test]
async fn a_cert_attested_domain_propagates_without_any_self_claim() {
    let key = 3;
    let (addr1, n1) = spawn_node_inner(
        "c1",
        vec![],
        Some(attested_auth(key, "c1", "rack-a")),
        None,
        None, // no MQTTD_FAILURE_DOMAIN-style self claim — the cert alone labels the node
        None,
        std::future::pending(),
    )
    .await;
    let (_addr2, n2) = spawn_node_inner(
        "c2",
        vec![addr1],
        Some(attested_auth(key, "c2", "rack-b")),
        None,
        None,
        None,
        std::future::pending(),
    )
    .await;

    let converged = wait_for(Duration::from_secs(6), || {
        n1.domain_of("c2").as_deref() == Some("rack-b")
            && n2.domain_of("c1").as_deref() == Some("rack-a")
    })
    .await;
    assert!(
        converged,
        "attested labels did not propagate: c1 sees c2={:?}, c2 sees c1={:?}",
        n1.domain_of("c2"),
        n2.domain_of("c1"),
    );

    n1.handle.abort();
    n2.handle.abort();
}

/// ADR 0016 T6: a node whose self-claimed failure domain contradicts its certificate is a
/// liar — its datagrams are dropped (reject reason `domain`) and the false label never
/// enters a peer's view.
#[tokio::test]
async fn a_domain_claim_contradicting_the_certificate_is_rejected() {
    let key = 4;
    let (addr1, n1) = spawn_node_inner(
        "h1",
        vec![],
        Some(attested_auth(key, "h1", "rack-a")),
        None,
        None,
        None,
        std::future::pending(),
    )
    .await;
    // The liar's cert attests rack-a, but it self-advertises rack-z.
    let (_addr2, liar) = spawn_node_inner(
        "liar",
        vec![addr1],
        Some(attested_auth(key, "liar", "rack-a")),
        None,
        Some("rack-z"),
        None,
        std::future::pending(),
    )
    .await;

    // The honest node drops the liar's datagrams under the bounded `domain` reason.
    let rejected = wait_for(Duration::from_secs(6), || n1.reject_count("domain") > 0).await;
    assert!(rejected, "no datagram was rejected for a domain mismatch");
    // The forged label never appears in the honest node's view.
    assert_ne!(n1.domain_of("liar").as_deref(), Some("rack-z"));

    n1.handle.abort();
    liar.handle.abort();
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
    let seal = |m: &Message| evil.seal(&bincode::serialize(m).unwrap(), true);

    // Control: a Join honestly attributed to "evil" is accepted (the victim learns it).
    let honest = Message {
        from: "evil".into(),
        from_addr: sock.local_addr().unwrap().to_string(),
        from_peer_addr: String::new(),
        from_domain: None,
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
        from_domain: None,
        kind: Kind::Join,
        gossip: vec![],
    };
    sock.send_to(&seal(&forged), &victim_addr).await.unwrap();
    // Causally wait for the driver to process and reject it — the reject is counted under
    // reason `identity` (ADR 0003-T6) — then confirm "ghost" was never learned.
    assert!(
        wait_for(Duration::from_secs(3), || victim.reject_count("identity")
            >= 1)
        .await,
        "a forged sender identity must be rejected and counted"
    );
    assert!(
        !knows(&victim, "ghost"),
        "a forged sender identity must not be learned"
    );

    victim.handle.abort();
}

/// ADR 0003 (T8) zero-downtime key rotation: mid-rotation, one node still seals with the
/// old key A (and accepts B) while another already seals with the new key B (and accepts
/// A). Both keys are in every node's ring, so the cluster converges across the rotation
/// rather than partitioning — the property that makes a no-downtime key change possible.
#[tokio::test]
async fn a_dual_key_window_lets_nodes_on_different_primaries_converge() {
    let a = [0x11; KEY_LEN];
    let b = [0x22; KEY_LEN];
    let auth_old = SwimAuth::new(&a).accept_also(&b); // seals A, accepts A+B
    let auth_new = SwimAuth::new(&b).accept_also(&a); // seals B, accepts A+B

    let (addr1, n1) = spawn_node("rot-old", vec![], Some(auth_old)).await;
    let (_addr2, n2) = spawn_node("rot-new", vec![addr1.clone()], Some(auth_new)).await;

    let ok = wait_for(Duration::from_secs(5), || {
        sees(&n1, "rot-new", MemberState::Alive) && sees(&n2, "rot-old", MemberState::Alive)
    })
    .await;
    assert!(
        ok,
        "nodes on different primary keys (rotation window) failed to converge"
    );

    n1.handle.abort();
    n2.handle.abort();
}

/// ADR 0023: two nodes that sign **and sequence** their gossip converge — the per-sender
/// replay window must accept the live monotonic stream without false-dropping it.
#[tokio::test]
async fn sequenced_nodes_converge() {
    let key = 0x55;
    let (addr1, n1) = spawn_sequenced_node("sq1", vec![], key).await;
    let (_addr2, n2) = spawn_sequenced_node("sq2", vec![addr1.clone()], key).await;

    let ok = wait_for(Duration::from_secs(5), || {
        sees(&n1, "sq2", MemberState::Alive) && sees(&n2, "sq1", MemberState::Alive)
    })
    .await;
    assert!(
        ok,
        "sequenced nodes failed to converge (replay window false-dropping live traffic?)"
    );

    n1.handle.abort();
    n2.handle.abort();
}

/// ADR 0023: a captured v3 datagram replayed to a peer is dropped by the sender's replay
/// window. We deliver the same sequenced Ping twice; the victim Acks the fresh one and
/// drops the replay, so exactly one Ack comes back (the victim's own probes are Pings, not
/// Acks, so they do not inflate the count).
#[tokio::test]
async fn a_replayed_v3_datagram_is_dropped() {
    let key = 0x66;
    let (victim_addr, victim) = spawn_sequenced_node("victim", vec![], key).await;
    let sender = signed_auth(key, "sender").with_sequencing();
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let ping = Message {
        from: "sender".into(),
        from_addr: sock.local_addr().unwrap().to_string(),
        from_peer_addr: String::new(),
        from_domain: None,
        kind: Kind::Ping { seq: 1 },
        gossip: vec![],
    };
    // One datagram at anti-replay sequence 1.
    let datagram = sender.seal_sequenced(&bincode::serialize(&ping).unwrap(), 1, true);
    // A sequenced (v3) opener: under strict postures only a v3 node opens the victim's v3
    // replies (its own CN is irrelevant — the stub verifier recovers the signer's CN).
    let opener = signed_auth(key, "x").with_sequencing();
    let mut buf = vec![0u8; 64 * 1024];

    // Deliver the fresh Ping and wait for the victim's Ack. The Ack is the causal
    // proof that the victim processed the datagram and recorded sequence 1 in its
    // replay window — so the replay below cannot race ahead of that bookkeeping (no
    // fixed inter-send sleep to guess at).
    sock.send_to(&datagram, &victim_addr).await.unwrap();
    assert!(
        recv_ack(&sock, &opener, &mut buf, Duration::from_secs(2)).await,
        "the fresh Ping must be Acked"
    );

    // Replay the exact same datagram; the window must drop it, so no further Ack
    // arrives in a bounded window (an erroneous accept would Ack within milliseconds).
    sock.send_to(&datagram, &victim_addr).await.unwrap();
    assert!(
        !recv_ack(&sock, &opener, &mut buf, Duration::from_millis(500)).await,
        "the replayed Ping must be dropped (no second Ack)"
    );

    // ADR 0003-T6: the drop is also counted on the reject metric under reason `replay`.
    assert!(
        victim.reject_count("replay") >= 1,
        "the dropped replay must be counted on the reject sink"
    );

    victim.handle.abort();
}

/// Wait up to `within` for the victim to send back an Ack (its `v3` reply, opened
/// with `opener`), returning whether one arrived. The victim's own probe Pings are
/// ignored — only Acks count.
async fn recv_ack(sock: &UdpSocket, opener: &SwimAuth, buf: &mut [u8], within: Duration) -> bool {
    let deadline = Instant::now() + within;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        let Ok(Ok((n, _))) = tokio::time::timeout(remaining, sock.recv_from(buf)).await else {
            return false; // window elapsed with no datagram
        };
        if let Ok(o) = opener.open(&buf[..n]) {
            if let Ok(m) = bincode::deserialize::<Message>(o.payload) {
                if matches!(m.kind, Kind::Ack { .. }) {
                    return true;
                }
            }
        }
    }
}
