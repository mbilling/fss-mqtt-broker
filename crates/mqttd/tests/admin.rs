//! The admin API listener (ADR 0081 T1/T2) over real mTLS: roles from certificate
//! subjects, refusals with stable codes, the cluster CA's `peer` role, and an audit record
//! for every request.
//!
//! Every certificate is minted per test with `rcgen`; the client is the same
//! `mqttd::admin::client` the CLI uses.

use mqtt_cluster::NodeId;
use mqtt_storage::MemorySessionStore;
use mqttd::admin::client::{self, Target};
use mqttd::admin::AdminState;
use mqttd::health::HealthState;
use mqttd::Hub;
use serde_json::Value;
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
    admin_ca: Ca,
    audit: Arc<Recorded>,
    config: Arc<RwLock<mqtt_config::Config>>,
}

/// A running admin listener: `viewers`/`operators` are the role lists; `cluster_ca`, when
/// given, is trusted for the `peer` role.
async fn start(viewers: &[&str], operators: &[&str], cluster_ca: Option<&Ca>) -> Harness {
    let admin_ca = mint_ca("admin");
    let (server_cert, server_key) = mint_leaf(&admin_ca, "admin-server", None);
    let mut cas = vec![admin_ca.pem.as_path()];
    cas.extend(cluster_ca.map(|c| c.pem.as_path()));
    let acceptor = mqtt_net::tls::admin_acceptor(&server_cert, &server_key, &cas).unwrap();

    let (hub, hub_tx) = Hub::with_config(
        NodeId("admin-node".into()),
        Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());
    let health = HealthState::new(hub_tx, None, None, 1).with_status(
        "admin-node".into(),
        Arc::new(
            mqtt_cluster::cluster_identity::ClusterIdentity::load_or_mint(true, None).unwrap(),
        ),
        Arc::new(mqttd::health::BrownoutStatus::default()),
        Arc::new(mqttd::reload::ConfigStamp::default()),
        Arc::new(std::sync::OnceLock::new()),
        None,
    );
    let mut config = mqtt_config::Config::default();
    config.admin.viewers = viewers.iter().map(|s| (*s).to_string()).collect();
    config.admin.operators = operators.iter().map(|s| (*s).to_string()).collect();
    let config = Arc::new(RwLock::new(config));
    let audit = Arc::new(Recorded::default());
    let mut state = AdminState::new("admin-node".into(), health, config.clone(), audit.clone());
    if let Some(ca) = cluster_ca {
        state = state.with_cluster_ca(mqtt_net::tls::ChainCheck::new(&ca.pem).unwrap());
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(mqttd::admin::serve(listener, acceptor, state));
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

    let records = h.audit.0.lock().unwrap().clone();
    assert_eq!(records.len(), 2, "{records:?}");
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
