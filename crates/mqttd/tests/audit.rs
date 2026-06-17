//! Audit-trail integration (ADR 0004 step 4): the connection layer records auth
//! and authorization decisions into the configured [`AuditSink`]. A
//! `RecordingAuditSink` lets these tests assert exactly what production would
//! have written to the tamper-evident chain.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mqtt_auth::acl::AclPolicy;
use mqtt_auth::basic::BasicAuthenticator;
use mqtt_auth::{AllowAll, Identity};
use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Connect, LastWill, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqtt_observability::RecordingAuditSink;
use mqtt_storage::MemorySessionStore;
use mqttd::conn::ConnPolicy;
use mqttd::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;

/// Start a node whose connections use `policy_builder` to assemble the auth /
/// authz / audit policy. The shared recording sink is returned for assertions.
async fn start_node(
    allow_anonymous: bool,
    acl_toml: Option<&str>,
    next_identity: Option<Identity>,
) -> (SocketAddr, Arc<RecordingAuditSink>) {
    let audit = Arc::new(RecordingAuditSink::new());
    let authz: Arc<dyn mqtt_auth::Authorizer> = match acl_toml {
        Some(toml) => Arc::new(AclPolicy::from_toml_str(toml).expect("policy parses")),
        None => Arc::new(AllowAll),
    };
    let policy = Arc::new(ConnPolicy {
        auth: Arc::new(BasicAuthenticator { allow_anonymous }),
        authz,
        audit: audit.clone(),
        proxy: None,
        store: None,
        enhanced: None,
    });

    let (hub, hub_tx) = Hub::with_config(
        NodeId("audit-node".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let policy = policy.clone();
            let identity = next_identity.clone();
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                mqttd::conn::handle_stream(stream, Some(peer), identity, policy, hub).await;
            });
        }
    });
    (addr, audit)
}

struct Client {
    reader: mqtt_net::FrameReader<tokio::net::tcp::OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl Client {
    async fn connect(addr: SocketAddr, id: &str, will: Option<LastWill>) -> Self {
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
            last_will: will,
            username: None,
            password: None,
        }))
        .await;
        c
    }

    async fn send(&mut self, packet: &Packet) {
        self.writer.send(packet).await.unwrap();
    }

    async fn recv(&mut self) -> Option<Packet> {
        timeout(Duration::from_millis(300), self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

/// Audit kinds settle asynchronously (the hub round-trips through a channel);
/// poll the sink until it contains `kind` or the window elapses.
async fn wait_for_kind(audit: &RecordingAuditSink, kind: &str) -> bool {
    for _ in 0..40 {
        if audit.kinds().iter().any(|k| k == kind) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

/// A successful CONNECT records `auth.success` with the principal subject.
#[tokio::test]
async fn successful_connect_is_audited() {
    let (addr, audit) = start_node(true, None, None).await;
    let mut c = Client::connect(addr, "alice", None).await;
    assert!(matches!(c.recv().await, Some(Packet::ConnAck(_))));

    assert!(wait_for_kind(&audit, "auth.success").await);
    let ev = audit
        .events()
        .into_iter()
        .find(|e| e.kind == "auth.success")
        .unwrap();
    // Anonymous principal's subject is the configured anonymous identity.
    assert_eq!(ev.subject.as_deref(), Some("anonymous"));
}

/// A rejected CONNECT records `auth.failure` keyed by client id — never a
/// credential.
#[tokio::test]
async fn rejected_connect_is_audited() {
    let (addr, audit) = start_node(false, None, None).await; // deny anonymous
    let mut c = Client::connect(addr, "mallory", None).await;
    match c.recv().await {
        Some(Packet::ConnAck(ack)) => assert_eq!(ack.code, 0x05),
        other => panic!("expected CONNACK 0x05, got {other:?}"),
    }

    assert!(wait_for_kind(&audit, "auth.failure").await);
    let ev = audit
        .events()
        .into_iter()
        .find(|e| e.kind == "auth.failure")
        .unwrap();
    assert_eq!(ev.subject.as_deref(), Some("mallory"));
}

/// A denied publish and a denied subscription are each audited with the
/// offending topic/filter.
#[tokio::test]
async fn denied_publish_and_subscribe_are_audited() {
    let acl = r#"
        [[rules]]
        actions = ["publish", "subscribe"]
        topics = ["allowed/#"]
    "#;
    let (addr, audit) = start_node(false, Some(acl), Some(identity("dev"))).await;
    let mut c = Client::connect(addr, "dev", None).await;
    assert!(matches!(c.recv().await, Some(Packet::ConnAck(_))));

    // Denied subscribe → 0x80 + audited.
    c.send(&Packet::Subscribe(Subscribe {
        properties: mqtt_codec::Properties::new(),
        pkid: 1,
        filters: vec![SubscribeFilter {
            options: mqtt_codec::SubscriptionOptions::default(),
            path: "forbidden/#".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await;
    assert!(matches!(c.recv().await, Some(Packet::SubAck(_))));

    // Denied publish → dropped+acked + audited.
    c.send(&Packet::Publish(Publish {
        properties: mqtt_codec::Properties::new(),
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "forbidden/x".into(),
        pkid: Some(7),
        payload: bytes::Bytes::from_static(b"x"),
    }))
    .await;
    assert_eq!(c.recv().await, Some(Packet::PubAck(7.into())));

    assert!(wait_for_kind(&audit, "acl.deny.subscribe").await);
    assert!(wait_for_kind(&audit, "acl.deny.publish").await);
    let kinds = audit.kinds();
    assert!(kinds.contains(&"acl.deny.subscribe".to_string()));
    assert!(kinds.contains(&"acl.deny.publish".to_string()));
    let sub_ev = audit
        .events()
        .into_iter()
        .find(|e| e.kind == "acl.deny.subscribe")
        .unwrap();
    assert_eq!(sub_ev.detail, "forbidden/#");
}

/// An unauthorized will topic is audited at CONNECT (the will is a deferred
/// publish refused before the session forms).
#[tokio::test]
async fn unauthorized_will_is_audited() {
    let acl = r#"
        [[rules]]
        actions = ["publish"]
        topics = ["status/%i"]
    "#;
    let (addr, audit) = start_node(false, Some(acl), Some(identity("dev-1"))).await;
    let will = LastWill {
        properties: mqtt_codec::Properties::new(),
        topic: "status/other".into(),
        payload: bytes::Bytes::from_static(b"gone"),
        qos: QoS::AtMostOnce,
        retain: false,
    };
    let mut c = Client::connect(addr, "dev-1", Some(will)).await;
    match c.recv().await {
        Some(Packet::ConnAck(ack)) => assert_eq!(ack.code, 0x05),
        other => panic!("expected CONNACK 0x05, got {other:?}"),
    }

    assert!(wait_for_kind(&audit, "acl.deny.will").await);
}

fn identity(subject: &str) -> Identity {
    Identity {
        subject: subject.into(),
        groups: vec![],
    }
}
