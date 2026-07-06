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
    let (reloader, handles) = reload::Reloader::new(initial, audit, build);

    let (hub, hub_tx) = Hub::with_config(
        NodeId("reload-node".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());

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
        let (rh, wh) = TcpStream::connect(addr).await.unwrap().into_split();
        let mut c = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        c.send(&Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
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
