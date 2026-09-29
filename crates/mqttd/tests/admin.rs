//! The admin API listener (ADR 0081 T1/T2) over real mTLS: roles from certificate
//! subjects, refusals with stable codes, the cluster CA's `peer` role, and an audit record
//! for every request.
//!
//! Every certificate is minted per test with `rcgen`; the client is the same
//! `mqttd::admin::client` the CLI uses.

use mqtt_cluster::placement::{Placement, DEFAULT_REPLICAS};
use mqtt_cluster::swim::MemberState;
mod common;

use common::Client;
use mqtt_cluster::NodeId;
use mqtt_codec::QoS;
use mqtt_storage::MemorySessionStore;
use mqttd::admin::client::{self, Target};
use mqttd::admin::cluster::PeerAccess;
use mqttd::admin::AdminState;
use mqttd::health::HealthState;
use mqttd::Hub;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;

/// An audit sink that keeps what it was given.
#[derive(Debug, Default)]
struct Recorded(Mutex<Vec<(String, Option<String>, String)>>);

impl mqtt_observability::AuditSink for Recorded {
    fn record(&self, kind: &str, subject: Option<&str>, detail: &str) {
        self.0
            .lock()
            .unwrap()
            .push((kind.into(), subject.map(String::from), detail.into()));
    }
}

/// A CA and the directory its files go in.
struct Ca {
    dir: PathBuf,
    issuer: rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
    pem: PathBuf,
}

fn temp_dir(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static UNIQUE: AtomicU64 = AtomicU64::new(0);
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("mqttd-admin-{}-{tag}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn mint_ca(tag: &str) -> Ca {
    let dir = temp_dir(tag);
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, format!("{tag} CA"));
    let issuer = rcgen::CertifiedIssuer::self_signed(params, key).unwrap();
    let pem = dir.join("ca.pem");
    std::fs::write(&pem, issuer.pem()).unwrap();
    Ca { dir, issuer, pem }
}

/// A leaf for `127.0.0.1` with the given Common Name (and optional organization);
/// returns `(cert.pem, key.pem)`.
fn mint_leaf(ca: &Ca, name: &str, org: Option<&str>) -> (PathBuf, PathBuf) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    if let Some(org) = org {
        params
            .distinguished_name
            .push(rcgen::DnType::OrganizationName, org);
    }
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = params.signed_by(&key, &ca.issuer).unwrap();
    let cert_path = ca.dir.join(format!("{name}.pem"));
    let key_path = ca.dir.join(format!("{name}.key"));
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

struct Harness {
    addr: String,
    admin_ca: Arc<Ca>,
    audit: Arc<Recorded>,
    config: Arc<RwLock<mqtt_config::Config>>,
}

/// A running admin listener: `viewers`/`operators` are the role lists; `cluster_ca`, when
/// given, is trusted for the `peer` role.
async fn start(viewers: &[&str], operators: &[&str], cluster_ca: Option<&Ca>) -> Harness {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    start_node(Node {
        id: "admin-node",
        listener,
        admin_ca: Arc::new(mint_ca("admin")),
        server_ca: None,
        cluster_ca,
        viewers,
        operators,
        placement: None,
        peers: None,
        hub: None,
        authz: None,
        reload: None,
    })
}

/// Everything one test node is made of.
struct Node<'a> {
    id: &'a str,
    listener: TcpListener,
    admin_ca: Arc<Ca>,
    /// Issues the listener's server certificate; the admin CA when `None`.
    server_ca: Option<&'a Ca>,
    cluster_ca: Option<&'a Ca>,
    viewers: &'a [&'a str],
    operators: &'a [&'a str],
    placement: Option<Arc<RwLock<Placement>>>,
    peers: Option<PeerAccess>,
    /// A running hub (and its store) to serve the session endpoints from; `None` spawns
    /// a bare hub with no session endpoints.
    hub: Option<(
        tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>,
        Arc<MemorySessionStore>,
    )>,
    /// The live authorizer for the dry run.
    authz: Option<mqttd::admin::authz::LiveAuthorizer>,
    /// The reloader and config stamp for the config and reload endpoints, and the live
    /// config cell they share with the admin state (as in the broker).
    reload: Option<(
        mqttd::admin::config::ReloadAccess,
        Arc<RwLock<mqtt_config::Config>>,
    )>,
}

fn start_node(node: Node<'_>) -> Harness {
    let admin_ca = node.admin_ca;
    let (server_cert, server_key) = mint_leaf(
        node.server_ca.unwrap_or(&admin_ca),
        &format!("{}-server", node.id),
        None,
    );
    let mut cas = vec![admin_ca.pem.as_path()];
    cas.extend(node.cluster_ca.map(|c| c.pem.as_path()));
    let acceptor = mqtt_net::tls::admin_acceptor(&server_cert, &server_key, &cas).unwrap();

    let (hub_tx, sessions) = if let Some((tx, store)) = node.hub {
        let access = mqttd::admin::sessions::SessionAccess {
            hub: tx.clone(),
            store,
            placement: node.placement.clone(),
        };
        (tx, Some(access))
    } else {
        let (hub, tx) =
            Hub::with_config(NodeId(node.id.into()), Arc::new(MemorySessionStore::new()));
        tokio::spawn(hub.run());
        (tx, None)
    };
    let health = HealthState::new(hub_tx, node.placement, None, 1).with_status(
        node.id.into(),
        Arc::new(
            mqtt_cluster::cluster_identity::ClusterIdentity::load_or_mint(true, None).unwrap(),
        ),
        Arc::new(mqttd::health::BrownoutStatus::default()),
        Arc::new(mqttd::reload::ConfigStamp::default()),
        Arc::new(std::sync::OnceLock::new()),
        None,
    );
    let (reload, config) = match node.reload {
        Some((access, live)) => (Some(access), live),
        None => (None, Arc::new(RwLock::new(mqtt_config::Config::default()))),
    };
    {
        let mut c = config.write().unwrap();
        c.admin.viewers = node.viewers.iter().map(|s| (*s).to_string()).collect();
        c.admin.operators = node.operators.iter().map(|s| (*s).to_string()).collect();
    }
    let audit = Arc::new(Recorded::default());
    let mut state = AdminState::new(node.id.into(), health, config.clone(), audit.clone());
    if let Some(ca) = node.cluster_ca {
        state = state.with_cluster_ca(mqtt_net::tls::ChainCheck::new(&ca.pem).unwrap());
    }
    if let Some(peers) = node.peers {
        state = state.with_peers(peers);
    }
    if let Some(access) = sessions {
        state = state.with_sessions(access);
    }
    if let Some(live) = node.authz {
        state = state.with_authorizer(live);
    }
    if let Some(access) = reload {
        state = state.with_reload(access);
    }
    let addr = node.listener.local_addr().unwrap().to_string();
    tokio::spawn(mqttd::admin::serve(node.listener, acceptor, state));
    Harness {
        addr,
        admin_ca,
        audit,
        config,
    }
}

impl Harness {
    fn target(&self, cert: &Path, key: &Path) -> Target {
        Target {
            addr: self.addr.clone(),
            server_name: "127.0.0.1".into(),
            connector: mqtt_net::tls::client_connector(&self.admin_ca.pem, cert, key).unwrap(),
            timeout: Duration::from_secs(10),
        }
    }

    /// `GET path` as the holder of `(cert, key)`; returns status and parsed body.
    async fn get(&self, who: &(PathBuf, PathBuf), path: &str) -> (u16, Value) {
        let (status, body) = client::call(&self.target(&who.0, &who.1), "GET", path, None)
            .await
            .unwrap();
        (status, serde_json::from_str(&body).unwrap())
    }
}

fn code(body: &Value) -> &str {
    body.pointer("/error/code")
        .and_then(Value::as_str)
        .unwrap_or("")
}

#[tokio::test]
async fn listed_subjects_get_their_role_and_everyone_else_is_refused() {
    let h = start(&["CN=alice"], &["CN=root, O=ops"], None).await;
    let alice = mint_leaf(&h.admin_ca, "alice", Some("anywhere"));
    let root = mint_leaf(&h.admin_ca, "root", Some("ops"));
    let mallory = mint_leaf(&h.admin_ca, "mallory", None);

    let (status, body) = h.get(&alice, "/admin/v1/whoami").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["role"], "viewer");
    assert_eq!(body["cn"], "alice");
    assert_eq!(body["node_id"], "admin-node");

    let (status, body) = h.get(&root, "/admin/v1/whoami").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["role"], "operator");

    // A certificate from the right CA whose subject is in no list: refused, and told why.
    let (status, body) = h.get(&mallory, "/admin/v1/whoami").await;
    assert_eq!(status, 403);
    assert_eq!(code(&body), "forbidden");

    // The role lists are read per request: a reload that lists mallory applies at once.
    h.config
        .write()
        .unwrap()
        .admin
        .viewers
        .push("CN=mallory".into());
    let (status, body) = h.get(&mallory, "/admin/v1/whoami").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["role"], "viewer");
}

#[tokio::test]
async fn a_certificate_from_another_ca_fails_the_handshake() {
    let h = start(&["CN=alice"], &[], None).await;
    let other = mint_ca("other");
    let (cert, key) = mint_leaf(&other, "alice", None);
    let err = client::call(&h.target(&cert, &key), "GET", "/admin/v1/whoami", None)
        .await
        .expect_err("an untrusted client certificate must not get an answer");
    assert!(
        err.contains("TLS") || err.contains("read failed") || err.contains("malformed"),
        "{err}"
    );
}

#[tokio::test]
async fn node_reports_the_statusz_body_and_unknown_routes_have_stable_codes() {
    let h = start(&["CN=alice"], &[], None).await;
    let alice = mint_leaf(&h.admin_ca, "alice", None);

    let (status, body) = h.get(&alice, "/admin/v1/node").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["node_id"], "admin-node");
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    assert!(body.get("ready").is_some(), "{body}");

    let (status, body) = h.get(&alice, "/admin/v1/nope").await;
    assert_eq!((status, code(&body)), (404, "not-found"));

    let (status, body) = client::call(
        &h.target(&alice.0, &alice.1),
        "POST",
        "/admin/v1/node",
        Some("{}"),
    )
    .await
    .unwrap();
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!((status, code(&body)), (405, "method-not-allowed"));
}

#[tokio::test]
async fn every_request_is_audited_with_subject_role_and_outcome() {
    let h = start(&["CN=alice"], &[], None).await;
    let alice = mint_leaf(&h.admin_ca, "alice", None);
    let mallory = mint_leaf(&h.admin_ca, "mallory", None);
    h.get(&alice, "/admin/v1/node?x=a%20b").await;
    h.get(&mallory, "/admin/v1/node").await;
    // A newline smuggled into the path must not split the one-line audit record.
    let (status, _) = h.get(&alice, "/admin/v1/node%0Aforged%20line").await;
    assert_eq!(status, 404);

    let records = h.audit.0.lock().unwrap().clone();
    assert_eq!(records.len(), 3, "{records:?}");
    assert_eq!(
        records[2].2,
        "role=viewer GET /admin/v1/node%0Aforged%20line -> 404"
    );
    assert!(records.iter().all(|(_, _, d)| !d.contains('\n')));
    assert!(records.iter().all(|(kind, _, _)| kind == "admin.request"));
    assert_eq!(records[0].1.as_deref(), Some("CN=alice"));
    assert_eq!(
        records[0].2,
        "role=viewer GET /admin/v1/node?x=a%20b -> 200"
    );
    assert_eq!(records[1].1.as_deref(), Some("CN=mallory"));
    assert_eq!(records[1].2, "role=none GET /admin/v1/node -> 403");
}

#[tokio::test]
async fn cluster_certificates_get_the_peer_role() {
    let cluster = mint_ca("cluster");
    let h = start(&["CN=alice"], &[], Some(&cluster)).await;
    let node2 = mint_leaf(&cluster, "node-2", None);
    // The client trusts the admin CA for the server; it presents a cluster certificate.
    let target = Target {
        connector: mqtt_net::tls::client_connector(&h.admin_ca.pem, &node2.0, &node2.1).unwrap(),
        ..h.target(&node2.0, &node2.1)
    };
    let (status, body) = client::call(&target, "GET", "/admin/v1/whoami", None)
        .await
        .unwrap();
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["role"], "peer");

    // A self-signed "node-2" that the cluster CA did not issue is not a peer: the
    // handshake refuses it outright.
    let impostor_ca = mint_ca("impostor");
    let impostor = mint_leaf(&impostor_ca, "node-2", None);
    let target = Target {
        connector: mqtt_net::tls::client_connector(&h.admin_ca.pem, &impostor.0, &impostor.1)
            .unwrap(),
        ..h.target(&impostor.0, &impostor.1)
    };
    assert!(client::call(&target, "GET", "/admin/v1/whoami", None)
        .await
        .is_err());
}

/// Three admin listeners whose membership views name each other; `n3` has no listener
/// (a closed port stands in for a partitioned node). Each node presents its cluster
/// certificate to its peers, and — the recommended setup — serves its admin listener
/// with a cluster-CA certificate too.
struct ThreeNodes {
    harnesses: Vec<Harness>,
    admin_ca: Arc<Ca>,
    cluster: Ca,
}

impl ThreeNodes {
    /// A target on node `i` for the holder of `(cert, key)`, trusting both CAs.
    fn target(&self, i: usize, who: &(PathBuf, PathBuf)) -> Target {
        Target {
            connector: mqtt_net::tls::client_connector_multi(
                &[&self.admin_ca.pem, &self.cluster.pem],
                &who.0,
                &who.1,
            )
            .unwrap(),
            ..self.harnesses[i].target(&who.0, &who.1)
        }
    }
}

async fn three_nodes() -> ThreeNodes {
    let admin_ca = Arc::new(mint_ca("admin"));
    let cluster = mint_ca("cluster");
    let ids = ["n1", "n2", "n3"];
    let listeners = [
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    // n3's port: bound and dropped, so connecting to it is refused.
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap().to_string();
    drop(dead);
    let mut admin_addrs: HashMap<String, String> = HashMap::new();
    admin_addrs.insert("n1".into(), listeners[0].local_addr().unwrap().to_string());
    admin_addrs.insert("n2".into(), listeners[1].local_addr().unwrap().to_string());
    admin_addrs.insert("n3".into(), dead_addr);
    let admin_addrs = Arc::new(admin_addrs);

    let mut harnesses = Vec::new();
    for (id, listener) in ids.iter().zip(listeners) {
        let mut placement = Placement::new(NodeId((*id).into()), DEFAULT_REPLICAS);
        for peer in ids.iter().filter(|p| *p != id) {
            placement.observe(
                &NodeId((*peer).into()),
                MemberState::Alive,
                &format!("{peer}.cluster:7000"),
                None,
            );
        }
        let (node_cert, node_key) = mint_leaf(&cluster, id, None);
        let connector = mqtt_net::tls::client_connector_multi(
            &[&admin_ca.pem, &cluster.pem],
            &node_cert,
            &node_key,
        )
        .unwrap();
        let map = admin_addrs.clone();
        let peers = PeerAccess::new(
            connector,
            Arc::new(move |node: &str, _peer_addr: &str| map.get(node).cloned()),
        );
        harnesses.push(start_node(Node {
            id,
            listener,
            admin_ca: admin_ca.clone(),
            // The recommended setup: the node's cluster certificate serves the admin
            // listener too, so it names the node and chains to the cluster CA.
            server_ca: Some(&cluster),
            cluster_ca: Some(&cluster),
            viewers: &["CN=alice"],
            operators: &[],
            placement: Some(Arc::new(RwLock::new(placement))),
            peers: Some(peers),
            hub: None,
            authz: None,
            reload: None,
        }));
    }

    ThreeNodes {
        harnesses,
        admin_ca,
        cluster,
    }
}

#[tokio::test]
async fn any_node_answers_for_the_cluster_and_a_silent_node_is_listed_as_silent() {
    let c = three_nodes().await;
    let alice = mint_leaf(&c.admin_ca, "alice", None);
    for i in 0..c.harnesses.len() {
        let (status, body) = client::call(&c.target(i, &alice), "GET", "/admin/v1/cluster", None)
            .await
            .unwrap();
        let body: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(status, 200, "{body}");
        let nodes = body["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 3, "{body}");
        let by_id = |id: &str| nodes.iter().find(|n| n["node_id"] == id).unwrap().clone();
        assert_eq!(by_id("n1")["replied"], true, "{body}");
        assert_eq!(by_id("n2")["replied"], true, "{body}");
        assert_eq!(by_id("n1")["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(by_id("n2")["members"], 3);
        let n3 = by_id("n3");
        assert_eq!(n3["replied"], false, "{body}");
        assert!(n3["error"].as_str().unwrap().contains("connect"), "{n3}");
        assert_eq!(body["summary"]["nodes"], 3);
        assert_eq!(body["summary"]["replied"], 2);
        assert_eq!(body["summary"]["same_membership"], true);
        assert_eq!(body["summary"]["same_version"], true);
        // Each test node minted its own cluster identity: the split-brain check fires.
        assert_eq!(body["summary"]["same_cluster_id"], false);
    }

    let (status, body) = client::call(&c.target(1, &alice), "GET", "/admin/v1/placement", None)
        .await
        .unwrap();
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["answered_by"], "n2");
    assert_eq!(body["members"].as_array().unwrap().len(), 3);
    let views = body["views"].as_array().unwrap();
    assert_eq!(views.len(), 2, "{body}");
    assert!(views
        .iter()
        .any(|v| v["node_id"] == "n1" && v["same_membership"] == true));
    assert!(views
        .iter()
        .any(|v| v["node_id"] == "n3" && v["replied"] == false));

    // A peer may read a node's own state, but not ask it to fan out.
    let node = mint_leaf(&c.cluster, "n1-again", None);
    let as_peer = c.target(1, &node);
    let (status, body) = client::call(&as_peer, "GET", "/admin/v1/cluster", None)
        .await
        .unwrap();
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!((status, code(&body)), (403, "forbidden"), "{body}");
}

/// A broker (hub + plaintext MQTT listener) with an admin listener serving its sessions.
struct Broker {
    mqtt: std::net::SocketAddr,
    admin: Harness,
}

async fn start_broker_with_admin() -> Broker {
    let store = Arc::new(MemorySessionStore::new());
    let (hub, hub_tx) = Hub::with_config(NodeId("b1".into()), store.clone());
    tokio::spawn(hub.run());
    let mqtt_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mqtt = mqtt_listener.local_addr().unwrap();
    let tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = mqtt_listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
        }
    });
    let admin = start_node(Node {
        id: "b1",
        listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
        admin_ca: Arc::new(mint_ca("admin")),
        server_ca: None,
        cluster_ca: None,
        viewers: &["CN=alice"],
        operators: &[],
        placement: None,
        peers: None,
        hub: Some((hub_tx, store)),
        authz: None,
        reload: None,
    });
    Broker { mqtt, admin }
}

/// `GET path` as alice (a viewer); status and parsed body.
async fn view(b: &Broker, path: &str) -> (u16, Value) {
    let alice = mint_leaf(&b.admin.admin_ca, "alice", None);
    b.admin.get(&alice, path).await
}

/// Poll `path` until `ready(body)` holds, failing after 10 s.
async fn view_until(b: &Broker, path: &str, ready: impl Fn(&Value) -> bool) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = view(b, path).await;
        assert_eq!(status, 200, "{body}");
        if ready(&body) {
            return body;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{path} never reached the expected state: {body}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn clients_are_listed_filtered_and_paged_with_their_connection_facts() {
    let b = start_broker_with_admin().await;
    let _s1 = Client::connect(b.mqtt, "sensor-1").await;
    let _s2 = Client::connect_v5_ok(b.mqtt, "sensor-2").await;
    let _app = Client::connect(b.mqtt, "app-1").await;

    let body = view_until(&b, "/admin/v1/clients?prefix=sensor-", |v| {
        v["matched"] == 2
    })
    .await;
    let rows = body["sessions"].as_array().unwrap();
    assert_eq!(rows[0]["client_id"], "sensor-1");
    assert_eq!(rows[0]["connected"], true);
    assert_eq!(rows[0]["protocol"], "3.1.1");
    assert_eq!(rows[1]["protocol"], "5");
    assert!(
        rows[0]["source"]
            .as_str()
            .unwrap()
            .starts_with("127.0.0.1:"),
        "{body}"
    );
    assert_eq!(body["next_cursor"], Value::Null);

    // Paging: one row per page, the cursor carries on, the total is stable.
    let (_, page1) = view(&b, "/admin/v1/clients?limit=2").await;
    assert_eq!(page1["matched"], 3);
    let ids: Vec<_> = page1["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["client_id"].clone())
        .collect();
    assert_eq!(ids, ["app-1", "sensor-1"]);
    let cursor = page1["next_cursor"].as_str().unwrap().to_string();
    let (_, page2) = view(&b, &format!("/admin/v1/clients?limit=2&cursor={cursor}")).await;
    assert_eq!(page2["sessions"][0]["client_id"], "sensor-2");
    assert_eq!(page2["next_cursor"], Value::Null);

    let (_, none) = view(&b, "/admin/v1/clients?source=10.").await;
    assert_eq!(none["matched"], 0);
    let (_, all) = view(&b, "/admin/v1/clients?source=127.0.0.1").await;
    assert_eq!(all["matched"], 3);
    let (status, bad) = view(&b, "/admin/v1/clients?limit=0").await;
    assert_eq!((status, code(&bad)), (400, "bad-request"));
}

#[tokio::test]
async fn a_session_shows_its_subscriptions_and_an_offline_one_its_queue() {
    let b = start_broker_with_admin().await;
    let mut sub = Client::connect(b.mqtt, "sub-1").await;
    sub.subscribe(1, "a/+/temp", QoS::AtLeastOnce).await;
    let (mut keeper, _) = Client::connect_v311(b.mqtt, "keeper", false).await;
    keeper.subscribe(1, "q/#", QoS::AtLeastOnce).await;
    keeper.disconnect().await;

    let body = view_until(&b, "/admin/v1/session?client=sub-1", |v| {
        v["subscriptions"] == 1
    })
    .await;
    assert_eq!(body["subscription_list"][0]["filter"], "a/+/temp");
    assert_eq!(body["subscription_list"][0]["qos"], 1);
    assert_eq!(body["connected"], true);
    assert_eq!(body["node"], "b1");

    // A message for the disconnected persistent session waits in the store.
    let mut publisher = Client::connect(b.mqtt, "pub-1").await;
    publisher
        .publish("q/1", b"x", QoS::AtLeastOnce, Some(7), vec![])
        .await;
    let body = view_until(&b, "/admin/v1/session?client=keeper", |v| v["queued"] == 1).await;
    assert_eq!(body["connected"], false);
    assert_eq!(body["persistent"], true);
    assert_eq!(body["queued_capped"], false);

    let (status, missing) = view(&b, "/admin/v1/session?client=nobody").await;
    assert_eq!((status, code(&missing)), (404, "not-found"));
}

#[tokio::test]
async fn subscribers_backlog_and_retained_answer_the_day_two_questions() {
    let b = start_broker_with_admin().await;
    let mut sub = Client::connect(b.mqtt, "slow").await;
    sub.subscribe(1, "a/+/temp", QoS::AtLeastOnce).await;
    let mut member = Client::connect(b.mqtt, "worker").await;
    member.subscribe(1, "$share/g/a/#", QoS::AtMostOnce).await;

    let body = view_until(&b, "/admin/v1/subscribers?topic=a/b/temp", |v| {
        v["subscribers"].as_array().is_some_and(|a| a.len() == 2)
    })
    .await;
    let rows = body["subscribers"].as_array().unwrap();
    assert_eq!(rows[0]["client_id"], "slow");
    assert_eq!(rows[0]["filter"], "a/+/temp");
    assert_eq!(rows[1]["client_id"], "worker");
    assert_eq!(rows[1]["shared_group"], "g");
    let (_, one) = view(&b, "/admin/v1/subscribers?topic=a/b/temp&limit=1").await;
    assert_eq!(one["subscribers"].as_array().unwrap().len(), 1);
    assert_eq!(one["subscribers"][0]["client_id"], "slow");
    assert_eq!(one["truncated"], true);
    let (status, bad) = view(&b, "/admin/v1/subscribers?topic=a/%2B/temp").await;
    assert_eq!((status, code(&bad)), (400, "bad-request"));
    // An unencoded `+` is a `+`, not a space: still refused as a wildcard.
    let (status, bad) = view(&b, "/admin/v1/subscribers?topic=a/+/temp").await;
    assert_eq!((status, code(&bad)), (400, "bad-request"));

    // `slow` never acknowledges: its QoS 1 deliveries stay in flight.
    let mut publisher = Client::connect(b.mqtt, "pub").await;
    for pkid in 1..=3 {
        publisher
            .publish("a/x/temp", b"t", QoS::AtLeastOnce, Some(pkid), vec![])
            .await;
    }
    let body = view_until(&b, "/admin/v1/backlog?top=5", |v| {
        v["sessions"][0]["inflight"] == 3
    })
    .await;
    assert_eq!(body["sessions"][0]["client_id"], "slow");

    // A fresh client: `publisher` still has three unread PUBACKs queued.
    let mut retainer = Client::connect(b.mqtt, "retainer").await;
    for topic in ["r/2", "r/1", "x/1"] {
        retainer.publish_retained_acked(topic, b"hello", 9).await;
    }
    let body = view_until(&b, "/admin/v1/retained?prefix=r/", |v| v["count"] == 2).await;
    assert_eq!(body["payload_bytes"], 10);
    assert_eq!(body["retained"][0]["topic"], "r/1");
    let (_, page) = view(&b, "/admin/v1/retained?prefix=r/&limit=1").await;
    assert_eq!(page["retained"].as_array().unwrap().len(), 1);
    assert_eq!(page["next_cursor"], "r/1");
}

#[tokio::test]
async fn the_authorization_dry_run_names_the_deciding_rule_of_the_live_policy() {
    let policy = mqtt_auth::acl::AclPolicy::from_toml_str(
        r#"
        [[rules]]
        identities = ["device-*"]
        actions = ["publish"]
        topics = ["devices/%i/#"]

        [[rules]]
        groups = ["ops"]
        actions = ["subscribe"]
        effect = "deny"
        topics = ["secret/#"]
        "#,
    )
    .unwrap();
    let (tx, live) =
        tokio::sync::watch::channel(Arc::new(policy) as Arc<dyn mqtt_auth::Authorizer>);
    let h = start_node(Node {
        id: "authz-node",
        listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
        admin_ca: Arc::new(mint_ca("admin")),
        server_ca: None,
        cluster_ca: None,
        viewers: &["CN=alice"],
        operators: &[],
        placement: None,
        peers: None,
        hub: None,
        authz: Some(live),
        reload: None,
    });
    let alice = mint_leaf(&h.admin_ca, "alice", None);

    let (status, body) = h
        .get(
            &alice,
            "/admin/v1/authz?user=device-7&action=publish&target=devices/device-7/t",
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["allowed"], true);
    assert_eq!(body["rule"]["index"], 0);
    assert_eq!(body["rule"]["expanded"], "devices/device-7/#");

    let (_, body) = h
        .get(
            &alice,
            "/admin/v1/authz?user=device-7&action=publish&target=devices/device-8/t",
        )
        .await;
    assert_eq!(body["allowed"], false);
    assert_eq!(body["rule"], Value::Null);

    let (_, body) = h
        .get(
            &alice,
            "/admin/v1/authz?user=bob&groups=ops,dev&action=subscribe&target=secret/%2B",
        )
        .await;
    assert_eq!(body["allowed"], false, "{body}");
    assert_eq!(body["rule"]["effect"], "deny");
    assert_eq!(body["groups"], serde_json::json!(["ops", "dev"]));

    // The dry run reads the LIVE policy: a reload that swaps it applies to the next call.
    tx.send(Arc::new(mqtt_auth::AllowAll)).unwrap();
    let (_, body) = h
        .get(
            &alice,
            "/admin/v1/authz?user=device-7&action=publish&target=devices/device-8/t",
        )
        .await;
    assert_eq!(body["allowed"], true);
    assert!(
        body["reason"].as_str().unwrap().contains("no ACL policy"),
        "{body}"
    );

    let (status, body) = h
        .get(&alice, "/admin/v1/authz?user=x&action=publish&target=a/%23")
        .await;
    assert_eq!((status, code(&body)), (400, "bad-request"));
    let (status, body) = h
        .get(&alice, "/admin/v1/authz?user=x&action=delete&target=a")
        .await;
    assert_eq!((status, code(&body)), (400, "bad-request"));
    // A structurally invalid filter is refused before the policy is asked.
    let (status, body) = h
        .get(
            &alice,
            "/admin/v1/authz?user=x&action=subscribe&target=a/%23/b",
        )
        .await;
    assert_eq!((status, code(&body)), (400, "bad-request"));
}

/// A reloader over `path` into `live`, with an allow-all policy build — the config swap
/// is what the reload tests are about.
fn allow_all_reloader(
    live: &Arc<RwLock<mqtt_config::Config>>,
    path: &Path,
    stamp: &Arc<mqttd::reload::ConfigStamp>,
) -> Arc<mqttd::reload::Reloader> {
    let policy = || {
        (
            Arc::new(mqtt_auth::AllowAll) as Arc<dyn mqtt_auth::Authorizer>,
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }) as Arc<dyn mqtt_auth::Authenticator>,
        )
    };
    let (mut reloader, _handles) = mqttd::reload::Reloader::new(
        policy(),
        Arc::new(Recorded::default()),
        move || Ok(policy()),
    );
    reloader.attach_config_stamp(stamp.clone());
    reloader.attach_config_source(mqttd::reload::ConfigSource {
        live: live.clone(),
        path: Some(path.to_path_buf()),
        precheck: Box::new(|_| Ok(())),
        apply: Box::new(|_, _| Vec::new()),
    });
    Arc::new(reloader)
}

#[tokio::test]
async fn config_is_served_redacted_and_reload_is_an_operator_action_with_an_outcome() {
    let dir = temp_dir("reload");
    let path = dir.join("mqttd.toml");
    let file = |extra: &str| {
        format!(
            "[durable]\nallow_ephemeral = true\n[cluster.swim]\nkey = \"s3cret-gossip-key\"\n\
             [admin]\nviewers = [\"CN=alice\"]\noperators = [\"CN=root\"]\n{extra}"
        )
    };
    std::fs::write(&path, file("")).unwrap();
    let loaded = mqtt_config::Config::load(Some(&path)).unwrap();
    let live = Arc::new(RwLock::new(loaded));
    let stamp = Arc::new(mqttd::reload::ConfigStamp::default());
    stamp.record(&std::fs::read(&path).unwrap());
    let reloader = allow_all_reloader(&live, &path, &stamp);

    let admin_ca = Arc::new(mint_ca("admin"));
    let h = start_node(Node {
        id: "cfg-node",
        listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
        admin_ca: admin_ca.clone(),
        server_ca: None,
        cluster_ca: None,
        viewers: &["CN=alice"],
        operators: &["CN=root"],
        placement: None,
        peers: None,
        hub: None,
        authz: None,
        reload: Some((
            mqttd::admin::config::ReloadAccess { reloader, stamp },
            live.clone(),
        )),
    });
    let alice = mint_leaf(&admin_ca, "alice", None);
    let root = mint_leaf(&admin_ca, "root", None);

    let (status, body) = h.get(&alice, "/admin/v1/config").await;
    assert_eq!(status, 200, "{body}");
    let text = body.to_string();
    assert!(
        !text.contains("s3cret-gossip-key"),
        "the gossip key leaked: {text}"
    );
    assert!(body["config"]["cluster"]["swim"]["key"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    assert_eq!(body["generation"], 1);
    assert_eq!(body["file_checksum"].as_str().unwrap().len(), 64);

    // Reload: an operator action. A viewer is refused before anything runs.
    let post = |who: &(PathBuf, PathBuf)| {
        let target = h.target(&who.0, &who.1);
        async move {
            let (status, body) = client::call(&target, "POST", "/admin/v1/reload", Some(""))
                .await
                .unwrap();
            (status, serde_json::from_str::<Value>(&body).unwrap())
        }
    };
    let (status, body) = post(&alice).await;
    assert_eq!((status, code(&body)), (403, "forbidden"));

    std::fs::write(&path, file("[limits]\nmax_sessions = 5\n")).unwrap();
    let (status, body) = post(&root).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["applied"], true);
    assert_eq!(body["trigger"], "admin");
    assert_eq!(body["changed_sections"], serde_json::json!(["limits"]));

    std::fs::write(&path, "[limits\n").unwrap();
    let (status, body) = post(&root).await;
    assert_eq!((status, code(&body)), (409, "reload-rejected"), "{body}");
    assert_eq!(body["outcome"]["applied"], false);
    assert_eq!(
        live.read().unwrap().limits.max_sessions,
        Some(5),
        "the running config is kept"
    );

    let records = h.audit.0.lock().unwrap().clone();
    assert!(records
        .iter()
        .any(|(_, who, d)| who.as_deref() == Some("CN=root")
            && d == "role=operator POST /admin/v1/reload -> 200"));
    std::fs::remove_dir_all(&dir).ok();
}
