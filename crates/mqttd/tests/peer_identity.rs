//! Peer node-id ↔ certificate binding (ADR 0004 step 5; resolves a deferred
//! item from ADR 0002).
//!
//! On the inter-node cluster bus a link runs mutual TLS against the cluster CA
//! and then exchanges a `Hello { node_id }`. Possession of a cluster-CA cert
//! must not let a node claim an *arbitrary* node id: the Hello's `node_id` MUST
//! equal the peer certificate's Subject Common Name, or the link is dropped.
//!
//! Each node here is minted a leaf certificate whose CN equals its node id, so
//! an honest node's Hello matches its cert. The impersonation test deliberately
//! runs a node whose cert CN ("node-evil") disagrees with its `NodeId`
//! ("node-a"), and asserts the peer rejects the link.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqtt_storage::MemorySessionStore;
use mqttd::peer::PeerTls;
use mqttd::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;

// --- throwaway PKI: a CA, plus per-node leafs whose CN == the node id --------

/// One cluster CA plus the per-node leaf certs minted under it. Adapted from
/// tests/tls.rs `mint_pki`, but each leaf's Common Name is a chosen node id
/// (tls.rs only mints a single "127.0.0.1" CN), which is exactly what this
/// task binds against.
struct ClusterPki {
    dir: PathBuf,
    ca_path: PathBuf,
    // rcgen 0.14: issuer + its certificate travel as one value.
    ca: rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
}

impl ClusterPki {
    fn new(tag: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static UNIQUE: AtomicU64 = AtomicU64::new(0);
        let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("mqttd-peerid-{}-{tag}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        // CrlSign so the CA can also issue the revocation list (ADR 0040 T4 test);
        // rcgen refuses to sign a CRL without it.
        ca_params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let ca = rcgen::CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

        let ca_path = dir.join("ca.pem");
        std::fs::write(&ca_path, ca.pem()).unwrap();
        ClusterPki { dir, ca_path, ca }
    }

    /// Mint a leaf signed by this CA whose Subject CN is `cn`. The leaf also
    /// carries SAN "127.0.0.1" (the dialer verifies the server name) and both
    /// server+client extended key usages (a cluster node is both). Returns the
    /// cert and key PEM paths.
    fn mint_leaf(&self, cn: &str) -> (PathBuf, PathBuf) {
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let leaf = params.signed_by(&leaf_key, &self.ca).unwrap();

        let cert_path = self.dir.join(format!("{cn}-cert.pem"));
        let key_path = self.dir.join(format!("{cn}-key.pem"));
        std::fs::write(&cert_path, leaf.pem()).unwrap();
        std::fs::write(&key_path, leaf_key.serialize_pem()).unwrap();
        (cert_path, key_path)
    }

    /// Like [`mint_leaf`](Self::mint_leaf), with an explicit certificate serial —
    /// the fact a CRL names (ADR 0040 T4).
    fn mint_leaf_with_serial(&self, cn: &str, serial: u64) -> (PathBuf, PathBuf) {
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.serial_number = Some(rcgen::SerialNumber::from(serial));
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let leaf = params.signed_by(&leaf_key, &self.ca).unwrap();
        let cert_path = self.dir.join(format!("{cn}-cert.pem"));
        let key_path = self.dir.join(format!("{cn}-key.pem"));
        std::fs::write(&cert_path, leaf.pem()).unwrap();
        std::fs::write(&key_path, leaf_key.serialize_pem()).unwrap();
        (cert_path, key_path)
    }

    /// A CA-signed CRL revoking `serial`, written as PEM (ADR 0040 T4).
    fn mint_crl(&self, serial: u64) -> PathBuf {
        let crl_params = rcgen::CertificateRevocationListParams {
            this_update: rcgen::date_time_ymd(2020, 1, 1),
            next_update: rcgen::date_time_ymd(2099, 1, 1),
            crl_number: rcgen::SerialNumber::from(1u64),
            issuing_distribution_point: None,
            revoked_certs: vec![rcgen::RevokedCertParams {
                serial_number: rcgen::SerialNumber::from(serial),
                revocation_time: rcgen::date_time_ymd(2020, 1, 2),
                reason_code: Some(rcgen::RevocationReason::KeyCompromise),
                invalidity_date: None,
            }],
            key_identifier_method: rcgen::KeyIdMethod::Sha256,
        };
        let crl = crl_params.signed_by(&self.ca).unwrap();
        let crl_path = self.dir.join("crl.pem");
        std::fs::write(&crl_path, crl.pem().unwrap()).unwrap();
        crl_path
    }

    /// A `PeerTls` whose presented cert has Common Name `cn` and serial `serial`.
    fn peer_tls_with_serial(&self, cn: &str, serial: u64) -> PeerTls {
        let (cert, key) = self.mint_leaf_with_serial(cn, serial);
        PeerTls {
            acceptor: mqttd::peer::fixed_acceptor(
                mqtt_net::tls::server_acceptor(&cert, &key, Some(&self.ca_path)).unwrap(),
            ),
            connector: mqttd::peer::fixed_connector(
                mqtt_net::tls::client_connector(&self.ca_path, &cert, &key).unwrap(),
            ),
            ca_der: mqtt_net::tls::first_cert_der(&self.ca_path).unwrap(),
            cert_der: mqtt_net::tls::first_cert_der(&cert).unwrap(),
            key_der: mqtt_net::tls::private_key_der(&key).unwrap(),
            gossip_crl: std::sync::Arc::new(std::sync::RwLock::new(None)),
            crl_path: None,
        }
    }

    /// A `PeerTls` whose presented cert has Common Name `cn`.
    fn peer_tls(&self, cn: &str) -> PeerTls {
        let (cert, key) = self.mint_leaf(cn);
        PeerTls {
            acceptor: mqttd::peer::fixed_acceptor(
                mqtt_net::tls::server_acceptor(&cert, &key, Some(&self.ca_path)).unwrap(),
            ),
            connector: mqttd::peer::fixed_connector(
                mqtt_net::tls::client_connector(&self.ca_path, &cert, &key).unwrap(),
            ),
            ca_der: mqtt_net::tls::first_cert_der(&self.ca_path).unwrap(),
            cert_der: mqtt_net::tls::first_cert_der(&cert).unwrap(),
            key_der: mqtt_net::tls::private_key_der(&key).unwrap(),
            gossip_crl: std::sync::Arc::new(std::sync::RwLock::new(None)),
            crl_path: None,
        }
    }
}

// --- broker harness ----------------------------------------------------------

/// A node's listeners and identity, wired so callers control the *cert CN*
/// independently of the `NodeId` — the whole point of the binding test. The
/// peer listener is held until `serve` consumes it, so all addresses are known
/// before any dialing begins.
struct NodeHandles {
    client_addr: SocketAddr,
    peer_addr: SocketAddr,
    peer_listener: Option<TcpListener>,
    hub_tx: tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>,
    node_id: NodeId,
}

/// Bind a node's peer + client listeners and start its hub. Does NOT start the
/// peer transport tasks yet (the caller dials/serves once both addrs are known).
async fn bind_node(node_id: NodeId) -> NodeHandles {
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cli = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer.local_addr().unwrap();
    let client_addr = cli.local_addr().unwrap();

    let (hub, hub_tx) = Hub::with_config(
        node_id.clone(),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());

    let tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = cli.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
        }
    });

    NodeHandles {
        client_addr,
        peer_addr,
        peer_listener: Some(peer),
        hub_tx,
        node_id,
    }
}

impl NodeHandles {
    /// Start this node's peer listener with the given (optional) TLS context.
    fn serve(&mut self, tls: Option<PeerTls>) {
        let listener = self
            .peer_listener
            .take()
            .expect("peer listener already taken");
        tokio::spawn(mqttd::peer::serve_listener(
            listener,
            self.node_id.clone(),
            self.hub_tx.clone(),
            tls,
            None,
            None,
        ));
    }

    /// Dial `peer_addr` from this node with the given (optional) TLS context.
    fn dial(&self, peer_addr: SocketAddr, tls: Option<PeerTls>) {
        tokio::spawn(mqttd::peer::dial_forever(
            peer_addr.to_string(),
            self.node_id.clone(),
            self.hub_tx.clone(),
            tls,
            None,
        ));
    }
}

// --- minimal MQTT client ------------------------------------------------------

struct Client {
    reader: mqtt_net::FrameReader<tokio::net::tcp::OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl Client {
    async fn connect(addr: SocketAddr, id: &str) -> Self {
        let (rh, wh) = TcpStream::connect(addr).await.unwrap().into_split();
        let mut c = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        c.writer
            .send(&Packet::Connect(Connect {
                properties: mqtt_codec::Properties::new(),
                protocol: V4,
                clean_session: true,
                keep_alive: 30,
                client_id: id.to_string(),
                last_will: None,
                username: None,
                password: None,
            }))
            .await
            .unwrap();
        assert!(matches!(c.recv().await, Some(Packet::ConnAck(_))));
        c
    }

    async fn subscribe(&mut self, filter: &str) {
        self.writer
            .send(&Packet::Subscribe(Subscribe {
                properties: mqtt_codec::Properties::new(),
                pkid: 1,
                filters: vec![SubscribeFilter {
                    options: mqtt_codec::SubscriptionOptions::default(),
                    path: filter.to_string(),
                    qos: QoS::AtMostOnce,
                }],
            }))
            .await
            .unwrap();
        assert!(matches!(self.recv().await, Some(Packet::SubAck(_))));
    }

    async fn publish(&mut self, topic: &str, payload: &'static [u8]) {
        self.writer
            .send(&Packet::Publish(Publish {
                properties: mqtt_codec::Properties::new(),
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: topic.to_string(),
                pkid: None,
                payload: bytes::Bytes::from_static(payload),
            }))
            .await
            .unwrap();
    }

    async fn recv(&mut self) -> Option<Packet> {
        timeout(Duration::from_millis(300), self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

/// Poll: publish on `pubr` and try to receive on `sub`, up to ~50 attempts
/// (each ~300ms). Returns true if the payload crossed the cluster.
async fn crosses(
    sub: &mut Client,
    pubr: &mut Client,
    topic: &'static str,
    payload: &'static [u8],
) -> bool {
    for _ in 0..50 {
        pubr.publish(topic, payload).await;
        if let Some(Packet::Publish(p)) = sub.recv().await {
            assert_eq!(p.topic, topic);
            assert_eq!(&p.payload[..], payload);
            return true;
        }
    }
    false
}

/// Negative check: confirm the payload does NOT cross within a bounded number
/// of attempts. Fewer attempts than `crosses` keeps the test snappy while still
/// giving any (erroneously established) link ample time to route.
async fn never_crosses(sub: &mut Client, pubr: &mut Client, topic: &'static str) -> bool {
    for _ in 0..15 {
        pubr.publish(topic, b"should-not-cross").await;
        if sub.recv().await.is_some() {
            return false;
        }
    }
    true
}

// --- tests -------------------------------------------------------------------

/// Honest cluster: node-a (cert CN "node-a") and node-b (cert CN "node-b")
/// link over mTLS and a publish on one reaches a subscriber on the other —
/// the Hello node id matches the cert CN on both sides.
#[tokio::test]
async fn honest_nodes_with_matching_cert_cn_link_and_route() {
    let pki = ClusterPki::new("honest");
    let mut a = bind_node(NodeId("node-a".into())).await;
    let mut b = bind_node(NodeId("node-b".into())).await;

    a.serve(Some(pki.peer_tls("node-a")));
    b.serve(Some(pki.peer_tls("node-b")));
    a.dial(b.peer_addr, Some(pki.peer_tls("node-a")));
    b.dial(a.peer_addr, Some(pki.peer_tls("node-b")));

    let mut sub = Client::connect(a.client_addr, "sub").await;
    sub.subscribe("ok/+/data").await;
    let mut pubr = Client::connect(b.client_addr, "pub").await;

    assert!(
        crosses(&mut sub, &mut pubr, "ok/zone/data", b"honest").await,
        "honest matching-CN nodes must link and route across the cluster"
    );
}

/// Impersonation rejected: a node whose certificate CN is "node-evil" but whose
/// `NodeId` (hence Hello) claims "node-a" must NOT establish a usable link. The
/// honest accepting node (real node-b) sees a cert CN that disagrees with the
/// Hello and drops the link, so no message crosses.
#[tokio::test]
async fn cert_cn_mismatch_with_hello_node_id_is_rejected() {
    let pki = ClusterPki::new("impersonate");
    // The impersonator: NodeId "node-a" (its Hello says "node-a"), but it dials
    // and serves with a cert whose CN is "node-evil".
    let mut evil = bind_node(NodeId("node-a".into())).await;
    let mut honest = bind_node(NodeId("node-b".into())).await;

    evil.serve(Some(pki.peer_tls("node-evil")));
    honest.serve(Some(pki.peer_tls("node-b")));
    evil.dial(honest.peer_addr, Some(pki.peer_tls("node-evil")));
    honest.dial(evil.peer_addr, Some(pki.peer_tls("node-b")));

    let mut sub = Client::connect(honest.client_addr, "sub").await;
    sub.subscribe("evil/+/data").await;
    let mut pubr = Client::connect(evil.client_addr, "pub").await;

    assert!(
        never_crosses(&mut sub, &mut pubr, "evil/zone/data").await,
        "a cert CN ({:?}) that disagrees with the Hello node id ({:?}) must not route",
        "node-evil",
        "node-a",
    );
}

/// Plaintext mesh (tls = None) keeps working with no binding: two nodes link
/// and route. Guards backward-compat for the unauthenticated mesh.
#[tokio::test]
async fn plaintext_mesh_without_binding_still_routes() {
    let mut a = bind_node(NodeId("plain-a".into())).await;
    let mut b = bind_node(NodeId("plain-b".into())).await;

    a.serve(None);
    b.serve(None);
    a.dial(b.peer_addr, None);
    b.dial(a.peer_addr, None);

    let mut sub = Client::connect(a.client_addr, "sub").await;
    sub.subscribe("plain/+/data").await;
    let mut pubr = Client::connect(b.client_addr, "pub").await;

    assert!(
        crosses(&mut sub, &mut pubr, "plain/zone/data", b"cleartext").await,
        "plaintext mesh must still link and route with no CN binding"
    );
}

/// ADR 0040 T4 — peer-bus revocation reaches the **established** link: with a
/// healthy two-node mTLS mesh routing traffic, publishing a cluster CRL that
/// revokes node-b's certificate and reloading on node-a tears the live link down
/// (no waiting for it to drop on its own), node-b cannot re-handshake in either
/// direction (both sides gate on the same live CRL slot), and node-a's local
/// clients keep working undisturbed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_crl_reload_tears_down_the_revoked_nodes_link() {
    use mqtt_auth::signed_gossip::RevocationList;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let pki = ClusterPki::new("crl-evict");
    let revoked_serial = 0xB0B;
    let mut a = bind_node(NodeId("node-a".into())).await;
    let mut b = bind_node(NodeId("node-b".into())).await;

    // Node A's TLS context shares its CRL slot with a reloader, exactly as the
    // binary wires MQTTD_PEER_TLS_CRL (initially: no CRL).
    let tls_a = pki.peer_tls_with_serial("node-a", 0xA11CE);
    let tls_b = pki.peer_tls_with_serial("node-b", revoked_serial);
    let crl_path = pki.mint_crl(revoked_serial);
    let crl_active = Arc::new(AtomicBool::new(false));

    let audit = Arc::new(mqtt_observability::AuditLog::new());
    let ok_build = || -> mqttd::reload::BuildResult {
        Ok((
            Arc::new(mqtt_auth::AllowAll) as Arc<dyn mqtt_auth::Authorizer>,
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }) as Arc<dyn mqtt_auth::Authenticator>,
        ))
    };
    let (mut reloader, _handles) =
        mqttd::reload::Reloader::new(ok_build().unwrap(), audit, ok_build);
    {
        let crl_active = crl_active.clone();
        reloader.attach_swim_crl(tls_a.gossip_crl.clone(), move || {
            if !crl_active.load(Ordering::SeqCst) {
                return Ok(RevocationList::default());
            }
            let bytes = std::fs::read(&crl_path).map_err(|e| format!("read crl: {e}"))?;
            RevocationList::from_bytes_unverified(&bytes).map_err(|e| format!("parse crl: {e}"))
        });
    }
    reloader.attach_identity_sweep(a.hub_tx.clone(), None);

    a.serve(Some(tls_a.clone()));
    b.serve(Some(tls_b.clone()));
    a.dial(b.peer_addr, Some(tls_a.clone()));
    b.dial(a.peer_addr, Some(tls_b));

    // The mesh is up: traffic crosses from B to a subscriber on A.
    let mut sub_a = Client::connect(a.client_addr, "sub-a").await;
    sub_a.subscribe("evict/+").await;
    let mut pub_b = Client::connect(b.client_addr, "pub-b").await;
    assert!(
        crosses(&mut sub_a, &mut pub_b, "evict/x", b"pre-revocation").await,
        "the mTLS mesh must route before the revocation"
    );

    // Publish the CRL and reload on node A: the sweep must tear the LIVE link down.
    crl_active.store(true, Ordering::SeqCst);
    assert!(reloader.reload("signal"), "the CRL reload should apply");

    // SETTLE(crl-redial-attempt-window): half of this is a negative (the link must STAY torn
    // down), and half wants evidence that a re-handshake was attempted and refused. The second
    // half has no observable today: a refused peer handshake is logged, not counted, so there is
    // no series to poll for "at least one revoked dialer was turned away". 2 s covers several
    // `dial_forever` backoff cycles, so the dialers do retry inside it. A slow machine gives the
    // dialers MORE attempts within the window, which strengthens the check rather than weakening
    // it. Turning this into a bounded poll needs a
    // `mqttd_peer_handshake_rejected_total{reason="revoked"}` counter; issue #260 records the ask.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        never_crosses(&mut sub_a, &mut pub_b, "evict/y").await,
        "a revoked node's link must stay torn down"
    );

    // Node A's local clients are undisturbed.
    let mut local_pub = Client::connect(a.client_addr, "pub-a").await;
    local_pub.publish("evict/z", b"local").await;
    match sub_a.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(&p.payload[..], b"local"),
        other => panic!("node A's local delivery must keep working, got {other:?}"),
    }
}

// --- ADR 0087 D5: a link is admitted by its handshake, not by membership -------

/// A sender that counts the links registered through it on their way to `hub`.
fn counting_links(
    hub: tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>,
) -> (
    tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    let (tap_tx, mut tap_rx) = tokio::sync::mpsc::unbounded_channel();
    let links = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = links.clone();
    tokio::spawn(async move {
        while let Some(command) = tap_rx.recv().await {
            if matches!(command, mqttd::HubCommand::PeerConnected { .. }) {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            if hub.send(command).is_err() {
                break;
            }
        }
    });
    (tap_tx, links)
}

/// The membership event a SWIM driver raises about `node`.
fn membership(
    node: &NodeHandles,
    state: mqtt_cluster::swim::MemberState,
) -> mqtt_cluster::swim_driver::MembershipEvent {
    mqtt_cluster::swim_driver::MembershipEvent {
        id: node.node_id.clone(),
        addr: String::new(),
        peer_addr: node.peer_addr.to_string(),
        state,
        domain: None,
    }
}

/// Wait until `links` links have been registered with the hub behind the counter.
async fn links_reach(counter: &std::sync::atomic::AtomicUsize, links: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while counter.load(std::sync::atomic::Ordering::SeqCst) < links {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the hub never registered link number {links}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// ADR 0087 D5, both orientations, over mutual TLS: node `holder` is told by its own
/// membership that the live node `peer` is `Dead`. Its hub drops the link — and a
/// second link is registered while the verdict stands, and serves: a publish on the
/// peer reaches a subscriber on the holder. Whoever dials (the smaller node id), the
/// holder's membership is asked nothing on the way: everything a link needs — the
/// cluster CA's certificate on both ends, the node id it attests — it still needs,
/// and the last test of this file shows it is still refused without.
async fn a_link_returns_under_a_dead_verdict(holder_id: &str, peer_id: &str) {
    use mqtt_cluster::swim::MemberState;

    let pki = ClusterPki::new("held-dead");
    let mut holder = bind_node(NodeId(holder_id.into())).await;
    let mut peer = bind_node(NodeId(peer_id.into())).await;
    let (holder_hub, links) = counting_links(holder.hub_tx.clone());
    holder.hub_tx = holder_hub;
    holder.serve(Some(pki.peer_tls(holder_id)));
    peer.serve(Some(pki.peer_tls(peer_id)));

    // Each node's links are its membership's to manage, as in production.
    let (holder_events, events) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(mqttd::cluster::maintain_peer_links(
        events,
        holder.node_id.clone(),
        holder.hub_tx.clone(),
        Some(pki.peer_tls(holder_id)),
        None,
        None,
        None,
    ));
    let (peer_events, events) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(mqttd::cluster::maintain_peer_links(
        events,
        peer.node_id.clone(),
        peer.hub_tx.clone(),
        Some(pki.peer_tls(peer_id)),
        None,
        None,
        None,
    ));
    holder_events
        .send(membership(&peer, MemberState::Alive))
        .unwrap();
    peer_events
        .send(membership(&holder, MemberState::Alive))
        .unwrap();

    let mut sub = Client::connect(holder.client_addr, "sub").await;
    sub.subscribe("held/+").await;
    let mut pubr = Client::connect(peer.client_addr, "pub").await;
    assert!(
        crosses(&mut sub, &mut pubr, "held/before", b"before").await,
        "the mesh must route before the verdict"
    );
    links_reach(&links, 1).await;

    // The false verdict, at the holder only. The peer is told nothing.
    holder_events
        .send(membership(&peer, MemberState::Dead))
        .unwrap();

    // The link the verdict dropped is replaced while the verdict stands...
    links_reach(&links, 2).await;
    // ...and is served: nothing between the two nodes asks membership.
    assert!(
        crosses(&mut sub, &mut pubr, "held/during", b"during").await,
        "a link from a peer held dead must be accepted and served"
    );
}

#[tokio::test]
async fn the_node_holding_a_peer_dead_keeps_dialing_it_and_serves_the_link() {
    // "node-a" < "node-b": the node that holds the verdict owns the link.
    a_link_returns_under_a_dead_verdict("node-a", "node-b").await;
}

#[tokio::test]
async fn a_node_accepts_and_serves_a_link_from_a_peer_it_holds_dead() {
    // "node-b" > "node-a": the peer held dead dials, and the holder accepts.
    a_link_returns_under_a_dead_verdict("node-b", "node-a").await;
}

/// What a `Dead` verdict does NOT do is open a door. A node that holds `node-a`
/// `Dead` is dialed by something that claims to be `node-a` with a certificate that
/// attests another name: refused, exactly as it is for a member in good standing
/// (`cert_cn_mismatch_with_hello_node_id_is_rejected`) — the accept path's checks
/// are the handshake's, and membership never stood in for any of them.
#[tokio::test]
async fn a_dead_verdict_admits_nobody_the_handshake_would_refuse() {
    use mqtt_cluster::swim::MemberState;

    let pki = ClusterPki::new("held-dead-impostor");
    // The impostor: its Hello says "node-a", its certificate says "node-evil".
    let evil = bind_node(NodeId("node-a".into())).await;
    let mut honest = bind_node(NodeId("node-b".into())).await;
    let (honest_hub, links) = counting_links(honest.hub_tx.clone());
    honest.hub_tx = honest_hub;
    honest.serve(Some(pki.peer_tls("node-b")));

    let (events_tx, events) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(mqttd::cluster::maintain_peer_links(
        events,
        honest.node_id.clone(),
        honest.hub_tx.clone(),
        Some(pki.peer_tls("node-b")),
        None,
        None,
        None,
    ));
    events_tx
        .send(membership(&evil, MemberState::Dead))
        .unwrap();
    evil.dial(honest.peer_addr, Some(pki.peer_tls("node-evil")));

    let mut sub = Client::connect(honest.client_addr, "sub").await;
    sub.subscribe("evil/+/data").await;
    let mut pubr = Client::connect(evil.client_addr, "pub").await;
    assert!(
        never_crosses(&mut sub, &mut pubr, "evil/zone/data").await,
        "a node held dead is not a node anyone may claim to be"
    );
    assert_eq!(
        links.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no link was ever registered for the impostor"
    );
}
