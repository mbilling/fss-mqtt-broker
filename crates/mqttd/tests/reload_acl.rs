//! ACL hot-reload over **live** connections (ADR 0032 — T4, and T3 at the broker level).
//!
//! The broker's authorizer is supplied through a [`reload::Reloader`] whose `build`
//! closure re-reads an ACL file from disk — the same mechanism the `mqttd` binary wires
//! to SIGHUP. The connection reads the *current* authorizer per check, so these tests
//! prove the two properties that matter:
//!
//! * **Live enforcement (T4):** a tightening reload denies an *already-connected*
//!   publisher's *next* publish — no reconnect required.
//! * **Validate-before-swap / never fail open (T3):** a malformed ACL file is rejected
//!   and the running (permissive) policy is kept intact.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use mqtt_auth::acl::AclPolicy;
use mqtt_auth::basic::BasicAuthenticator;
use mqtt_auth::{Authenticator, Authorizer, Identity};
use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqtt_storage::MemorySessionStore;
use mqttd::{reload, Hub};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;
const RECV_TIMEOUT: Duration = Duration::from_millis(300);

/// A permissive ACL: anyone may publish and subscribe across `room/#`.
const PERMISSIVE: &str = r#"
[[rules]]
actions = ["publish", "subscribe"]
topics = ["room/#"]
"#;

/// A tightened ACL: subscribe still allowed, but publishing to `room/#` is denied.
const TIGHTENED: &str = r#"
[[rules]]
actions = ["subscribe"]
topics = ["room/#"]
"#;

/// Like [`PERMISSIVE`], plus a connect rule (ADR 0031, opt-in) reserving connects
/// for the `keeper` identity — once any connect rule exists, a connect (and, per
/// ADR 0040 T2, a live session) needs a matching allow.
const CONNECT_LOCKED: &str = r#"
[[rules]]
actions = ["publish", "subscribe"]
topics = ["room/#"]

[[rules]]
identities = ["keeper"]
actions = ["connect"]
clients = ["*"]
"#;

/// A subscribe-tightened ACL: publishing to `room/#` stays allowed, but the
/// subscribe grant is gone — existing subscriptions lose their grant (ADR 0040 T3).
const PUBLISH_ONLY: &str = r#"
[[rules]]
actions = ["publish"]
topics = ["room/#"]
"#;

/// A self-removing temp file holding the ACL the broker reloads from.
struct AclFile {
    path: PathBuf,
}

impl AclFile {
    fn new(initial: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("mqttd-reload-{}-{n}.toml", std::process::id()));
        std::fs::write(&path, initial).unwrap();
        AclFile { path }
    }

    fn write(&self, contents: &str) {
        std::fs::write(&self.path, contents).unwrap();
    }
}

impl Drop for AclFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Start a broker whose authorizer is reloaded from `acl_path`. Connections are assigned
/// the next identity pushed into the returned queue (one shared hub, per-connection
/// identities — as under mTLS). The returned [`reload::Reloader`] re-reads the file on
/// `reload()`, exactly as SIGHUP does in the binary.
async fn start_reloadable_node(
    acl_path: PathBuf,
) -> (
    SocketAddr,
    mpsc::UnboundedSender<Identity>,
    reload::Reloader,
) {
    let build = move || -> reload::BuildResult {
        let toml = std::fs::read_to_string(&acl_path).map_err(|e| format!("read acl: {e}"))?;
        let acl = AclPolicy::from_toml_str(&toml).map_err(|e| format!("parse acl: {e}"))?;
        Ok((
            Arc::new(acl) as Arc<dyn Authorizer>,
            Arc::new(BasicAuthenticator {
                allow_anonymous: false,
            }) as Arc<dyn Authenticator>,
        ))
    };
    let initial = build().expect("the initial ACL builds");
    let audit = Arc::new(mqtt_observability::AuditLog::new());
    let (mut reloader, handles) = reload::Reloader::new(initial, audit, build);

    let (hub, hub_tx) = Hub::with_config(
        NodeId("reload-node".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());
    // Revocation reaches live state (ADR 0040 T2): a successful reload sweeps the
    // online table against the new policy.
    reloader.attach_identity_sweep(hub_tx.clone(), None);
    // ...and resumed sessions re-check their restored grants (ADR 0040 T3).
    hub_tx
        .send(mqttd::hub::HubCommand::AttachAuthorizer(
            mqttd::hub::AuthzWatch(handles.authz.clone()),
        ))
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (id_tx, mut id_rx) = mpsc::unbounded_channel::<Identity>();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let identity = id_rx.recv().await;
            // Each connection clones the live `watch` receivers — a reload reaches them.
            let conn_policy = Arc::new(mqttd::conn::ConnPolicy {
                auth: handles.auth.clone(),
                authz: handles.authz.clone(),
                audit: Arc::new(mqtt_observability::AuditLog::new()),
                proxy: None,
                store: None,
                connect_timeout: Duration::from_secs(10),
                shutdown: None,
                metrics: None,
                enhanced: None,
            });
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                mqttd::conn::handle_stream(
                    stream,
                    Some(peer),
                    identity.map(|identity| mqttd::conn::CertAdmission {
                        identity,
                        serial: None,
                    }),
                    conn_policy,
                    hub,
                )
                .await;
            });
        }
    });
    (addr, id_tx, reloader)
}

fn identity(subject: &str) -> Identity {
    Identity {
        subject: subject.into(),
        groups: vec![],
    }
}

struct Client {
    reader: mqtt_net::FrameReader<tokio::net::tcp::OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl Client {
    async fn connect(addr: SocketAddr, id: &str) -> Self {
        Self::connect_with(addr, id, true).await
    }

    async fn connect_with(addr: SocketAddr, id: &str, clean_session: bool) -> Self {
        let (rh, wh) = TcpStream::connect(addr).await.unwrap().into_split();
        let mut c = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        c.send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session,
            keep_alive: 30,
            client_id: id.to_string(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await;
        match c.recv().await {
            Some(Packet::ConnAck(ack)) if ack.code == 0 => c,
            other => panic!("expected CONNACK 0x00, got {other:?}"),
        }
    }

    async fn send(&mut self, packet: &Packet) {
        self.writer.send(packet).await.unwrap();
    }

    async fn subscribe(&mut self, filter: &str, qos: QoS) -> Vec<u8> {
        self.send(&Packet::Subscribe(Subscribe {
            properties: mqtt_codec::Properties::new(),
            pkid: 1,
            filters: vec![SubscribeFilter {
                options: mqtt_codec::SubscriptionOptions::default(),
                path: filter.to_string(),
                qos,
            }],
        }))
        .await;
        match self.recv().await {
            Some(Packet::SubAck(ack)) => ack.return_codes,
            other => panic!("expected SUBACK, got {other:?}"),
        }
    }

    /// Publish at `QoS` 1; the broker acks whether or not the ACL forwards it.
    async fn publish_qos1(&mut self, topic: &str, pkid: u16, payload: &'static [u8]) {
        self.send(&Packet::Publish(Publish {
            properties: mqtt_codec::Properties::new(),
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: topic.to_string(),
            pkid: Some(pkid),
            payload: bytes::Bytes::from_static(payload),
        }))
        .await;
        assert_eq!(
            self.recv().await,
            Some(Packet::PubAck(pkid.into())),
            "publishes are acked whether or not the ACL forwards them"
        );
    }

    async fn recv(&mut self) -> Option<Packet> {
        timeout(RECV_TIMEOUT, self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

/// T4 — live enforcement: after a tightening reload, an **already-connected** publisher's
/// next publish is dropped, with no reconnect on either side.
#[tokio::test]
async fn tightening_acl_reload_denies_a_live_publisher() {
    let acl = AclFile::new(PERMISSIVE);
    let (addr, ids, reloader) = start_reloadable_node(acl.path.clone()).await;

    ids.send(identity("sub")).unwrap();
    let mut sub = Client::connect(addr, "sub").await;
    assert_eq!(sub.subscribe("room/#", QoS::AtMostOnce).await, vec![0x00]);

    ids.send(identity("pub")).unwrap();
    let mut publ = Client::connect(addr, "pub").await;

    // Before the reload the permissive ACL forwards the publish.
    publ.publish_qos1("room/a", 1, b"before").await;
    match sub.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(&p.payload[..], b"before"),
        other => panic!("expected delivery before the reload, got {other:?}"),
    }

    // Tighten the file on disk and reload — the swap must reach the live connections.
    acl.write(TIGHTENED);
    assert!(
        reloader.reload("signal"),
        "a valid tightened ACL must apply"
    );

    // The same already-connected publisher is now denied; nothing is delivered.
    publ.publish_qos1("room/a", 2, b"after").await;
    assert_eq!(
        sub.recv().await,
        None,
        "the tightened ACL must drop the live publisher's next publish"
    );
}

/// T3 — never fail open: a malformed ACL file is rejected; the running (permissive)
/// policy is kept, so traffic keeps flowing exactly as before the failed reload.
#[tokio::test]
async fn malformed_acl_reload_is_rejected_and_keeps_the_running_policy() {
    let acl = AclFile::new(PERMISSIVE);
    let (addr, ids, reloader) = start_reloadable_node(acl.path.clone()).await;

    ids.send(identity("sub")).unwrap();
    let mut sub = Client::connect(addr, "sub").await;
    assert_eq!(sub.subscribe("room/#", QoS::AtMostOnce).await, vec![0x00]);

    ids.send(identity("pub")).unwrap();
    let mut publ = Client::connect(addr, "pub").await;
    publ.publish_qos1("room/a", 1, b"before").await;
    match sub.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(&p.payload[..], b"before"),
        other => panic!("expected delivery before the failed reload, got {other:?}"),
    }

    // Corrupt the file and reload — the build fails and the swap must be aborted.
    acl.write("this is not valid TOML [[[");
    assert!(
        !reloader.reload("signal"),
        "a malformed ACL must be rejected, not applied"
    );

    // The running policy is unchanged: the publish still flows.
    publ.publish_qos1("room/a", 2, b"after").await;
    match sub.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(
            &p.payload[..],
            b"after",
            "the kept policy must still forward the publish"
        ),
        other => panic!("expected delivery after the rejected reload, got {other:?}"),
    }
}

/// ADR 0040 T2 — the identity sweep: a reload that denies a principal's *connect*
/// evicts its **live** session with no client action, while an untouched session
/// keeps flowing. (Contrast with the T4 test above, which pins next-operation
/// semantics for permission changes.)
#[tokio::test]
async fn a_connect_acl_tightening_evicts_the_live_session() {
    let acl = AclFile::new(PERMISSIVE);
    let (addr, ids, reloader) = start_reloadable_node(acl.path.clone()).await;

    ids.send(identity("victim")).unwrap();
    let mut victim = Client::connect(addr, "c-victim").await;
    assert_eq!(victim.subscribe("room/1", QoS::AtMostOnce).await, vec![0]);

    ids.send(identity("keeper")).unwrap();
    let mut keeper = Client::connect(addr, "c-keeper").await;
    assert_eq!(keeper.subscribe("room/1", QoS::AtMostOnce).await, vec![0]);

    // Tighten: connects become keeper-only; the reload sweeps live sessions.
    acl.write(CONNECT_LOCKED);
    assert!(
        reloader.reload("signal"),
        "the tightened ACL should reload cleanly"
    );

    // The victim is evicted without sending anything.
    assert!(
        victim.recv().await.is_none(),
        "the connect-denied session must be closed by the sweep"
    );

    // The keeper's live session is untouched: it still publishes and receives.
    keeper.publish_qos1("room/1", 7, b"still-here").await;
    match keeper.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(&p.payload[..], b"still-here"),
        other => panic!("the keeper must keep flowing, got {other:?}"),
    }
}

/// ADR 0040 T3 — the grant sweep, online: a reload that removes the subscribe grant
/// stops delivery to an existing subscription immediately. The subscriber is NOT
/// disconnected (permission changed, not identity) — its connection still answers —
/// and its next SUBSCRIBE re-attempt is denied like any new operation.
#[tokio::test]
async fn a_subscribe_acl_tightening_stops_delivery_to_a_live_subscription() {
    let acl = AclFile::new(PERMISSIVE);
    let (addr, ids, reloader) = start_reloadable_node(acl.path.clone()).await;

    ids.send(identity("reader")).unwrap();
    let mut reader = Client::connect(addr, "c-reader").await;
    assert_eq!(reader.subscribe("room/1", QoS::AtMostOnce).await, vec![0]);

    ids.send(identity("writer")).unwrap();
    let mut writer = Client::connect(addr, "c-writer").await;
    writer.publish_qos1("room/1", 1, b"before").await;
    match reader.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(&p.payload[..], b"before"),
        other => panic!("expected the pre-reload publish, got {other:?}"),
    }

    // Tighten: the subscribe grant disappears; the reload sweeps live grants.
    acl.write(PUBLISH_ONLY);
    assert!(reloader.reload("signal"), "the tightened ACL should reload");

    // Delivery stops — but the connection survives (identity untouched).
    writer.publish_qos1("room/1", 2, b"after").await;
    assert!(
        reader.recv().await.is_none(),
        "a revoked grant must stop delivering after the sweep"
    );
    reader.send(&Packet::PingReq).await;
    assert!(
        matches!(reader.recv().await, Some(Packet::PingResp)),
        "a permission-only change must not disconnect the client"
    );

    // The next SUBSCRIBE re-attempt is denied at the admission-path check.
    assert_eq!(
        reader.subscribe("room/1", QoS::AtMostOnce).await,
        vec![0x80],
        "re-subscribing to the revoked filter must be denied"
    );
}

/// ADR 0040 T3 — the grant sweep, offline: a persistent session that slept through
/// a tightening reload loses the revoked grant at resume — the queued message only
/// that grant admits is NOT replayed, and re-subscribing is denied.
#[tokio::test]
async fn an_offline_sessions_revoked_grant_does_not_replay_its_queue_on_resume() {
    let acl = AclFile::new(PERMISSIVE);
    let (addr, ids, reloader) = start_reloadable_node(acl.path.clone()).await;

    // A persistent subscriber at QoS 1 (so missed messages queue), then it sleeps.
    ids.send(identity("sleeper")).unwrap();
    let mut sleeper = Client::connect_with(addr, "c-sleeper", false).await;
    assert_eq!(sleeper.subscribe("room/1", QoS::AtLeastOnce).await, vec![1]);
    drop(sleeper);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A message queues for the sleeping session; then the grant is revoked.
    ids.send(identity("writer")).unwrap();
    let mut writer = Client::connect(addr, "c-writer").await;
    writer.publish_qos1("room/1", 1, b"missed").await;
    acl.write(PUBLISH_ONLY);
    assert!(reloader.reload("signal"), "the tightened ACL should reload");

    // On resume: no replay of the revoked grant's queue, and no re-subscribe.
    ids.send(identity("sleeper")).unwrap();
    let mut resumed = Client::connect_with(addr, "c-sleeper", false).await;
    assert!(
        resumed.recv().await.is_none(),
        "the revoked grant's queued message must not replay on resume"
    );
    assert_eq!(
        resumed.subscribe("room/1", QoS::AtLeastOnce).await,
        vec![0x80],
        "re-subscribing to the revoked filter must be denied"
    );
}
