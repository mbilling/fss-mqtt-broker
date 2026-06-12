//! ACL enforcement integration tests (ADR 0004 step 3): deny-by-default topic
//! authorization at SUBSCRIBE (0x80 per filter), PUBLISH (dropped but acked),
//! and the will topic at CONNECT (0x05).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mqtt_auth::acl::AclPolicy;
use mqtt_auth::basic::BasicAuthenticator;
use mqtt_auth::Identity;
use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Connect, LastWill, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqtt_storage::MemorySessionStore;
use mqttd::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;
const RECV_TIMEOUT: Duration = Duration::from_millis(300);

/// Start a broker node enforcing `policy_toml`. Connections are assigned the
/// next identity pushed into the returned queue (simulating per-connection
/// mTLS identities against one shared hub).
async fn start_acl_node(policy_toml: &str) -> (SocketAddr, mpsc::UnboundedSender<Identity>) {
    let policy = Arc::new(AclPolicy::from_toml_str(policy_toml).expect("test policy parses"));
    let (hub, hub_tx) = Hub::with_config(
        NodeId("acl-node".into()),
        Box::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (id_tx, mut id_rx) = mpsc::unbounded_channel::<Identity>();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let identity = id_rx.recv().await;
            let auth = Arc::new(BasicAuthenticator {
                allow_anonymous: false,
            });
            let policy = policy.clone();
            let hub = hub_tx.clone();
            tokio::spawn(async move {
                mqttd::conn::handle_stream(stream, Some(peer), identity, auth, policy, hub).await;
            });
        }
    });
    (addr, id_tx)
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
    /// Connect as the next queued identity; asserts CONNACK 0x00.
    async fn connect(addr: SocketAddr, id: &str) -> Self {
        let mut c = Self::connect_raw(addr, id, None).await;
        match c.recv().await {
            Some(Packet::ConnAck(ack)) if ack.code == 0 => c,
            other => panic!("expected CONNACK 0x00, got {other:?}"),
        }
    }

    /// Connect (optionally with a will) without asserting the CONNACK.
    async fn connect_raw(addr: SocketAddr, id: &str, will: Option<LastWill>) -> Self {
        let (rh, wh) = TcpStream::connect(addr).await.unwrap().into_split();
        let mut c = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        c.send(&Packet::Connect(Connect {
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

    /// Subscribe to `filters`, returning the SUBACK return codes.
    async fn subscribe(&mut self, filters: &[(&str, QoS)]) -> Vec<u8> {
        self.send(&Packet::Subscribe(Subscribe {
            pkid: 1,
            filters: filters
                .iter()
                .map(|(path, qos)| SubscribeFilter {
                    path: (*path).to_string(),
                    qos: *qos,
                })
                .collect(),
        }))
        .await;
        match self.recv().await {
            Some(Packet::SubAck(ack)) => ack.return_codes,
            other => panic!("expected SUBACK, got {other:?}"),
        }
    }

    /// Publish at `QoS` 1 and assert the broker acks it (denied publishes are
    /// dropped but still acknowledged — 3.1.1 has no negative PUBACK).
    async fn publish_qos1(&mut self, topic: &str, pkid: u16, payload: &'static [u8]) {
        self.send(&Packet::Publish(Publish {
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
            Some(Packet::PubAck(pkid)),
            "publishes must be acked whether or not the ACL forwards them"
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

/// A denied SUBSCRIBE filter gets return code 0x80 and is never registered:
/// granted filters in the same packet still work.
#[tokio::test]
async fn denied_subscription_gets_0x80_and_no_delivery() {
    let (addr, ids) = start_acl_node(
        r##"
        [[rules]]
        actions = ["subscribe"]
        topics = ["ok/#"]

        [[rules]]
        actions = ["publish"]
        topics = ["#"]
        "##,
    )
    .await;

    ids.send(identity("sub")).unwrap();
    let mut sub = Client::connect(addr, "sub").await;
    let codes = sub
        .subscribe(&[("ok/a", QoS::AtMostOnce), ("secret/x", QoS::AtMostOnce)])
        .await;
    assert_eq!(codes, vec![0x00, 0x80]);

    ids.send(identity("pub")).unwrap();
    let mut publ = Client::connect(addr, "pub").await;
    publ.publish_qos1("secret/x", 1, b"hidden").await;
    publ.publish_qos1("ok/a", 2, b"visible").await;

    match sub.recv().await {
        Some(Packet::Publish(p)) => {
            assert_eq!(p.topic, "ok/a", "only the granted filter may deliver");
            assert_eq!(&p.payload[..], b"visible");
        }
        other => panic!("expected the granted delivery, got {other:?}"),
    }
    assert_eq!(sub.recv().await, None, "denied filter must deliver nothing");
}

/// A denied PUBLISH is dropped (no delivery) but still acknowledged, so
/// spec-conforming `QoS` 1 publishers are not left retrying forever.
#[tokio::test]
async fn denied_publish_is_dropped_but_acked() {
    let (addr, ids) = start_acl_node(
        r##"
        [[rules]]
        actions = ["subscribe"]
        topics = ["#"]

        [[rules]]
        identities = ["writer"]
        actions = ["publish"]
        topics = ["mine/#"]
        "##,
    )
    .await;

    ids.send(identity("watcher")).unwrap();
    let mut sub = Client::connect(addr, "watcher").await;
    assert_eq!(
        sub.subscribe(&[("other/#", QoS::AtMostOnce), ("mine/#", QoS::AtMostOnce)])
            .await,
        vec![0x00, 0x00]
    );

    ids.send(identity("writer")).unwrap();
    let mut publ = Client::connect(addr, "writer").await;
    publ.publish_qos1("other/x", 1, b"forbidden").await; // acked, dropped
    publ.publish_qos1("mine/y", 2, b"permitted").await;

    match sub.recv().await {
        Some(Packet::Publish(p)) => {
            assert_eq!(p.topic, "mine/y", "the denied publish must not deliver");
        }
        other => panic!("expected the permitted delivery, got {other:?}"),
    }
    assert_eq!(sub.recv().await, None);
}

/// A will is a deferred publish: an unauthorized will topic is refused at
/// CONNECT with 0x05; an authorized one connects normally.
#[tokio::test]
async fn unauthorized_will_topic_is_refused_at_connect() {
    let (addr, ids) = start_acl_node(
        r#"
        [[rules]]
        actions = ["publish"]
        topics = ["status/%i"]
        "#,
    )
    .await;
    let will = |topic: &str| LastWill {
        topic: topic.into(),
        payload: bytes::Bytes::from_static(b"gone"),
        qos: QoS::AtMostOnce,
        retain: false,
    };

    ids.send(identity("dev-1")).unwrap();
    let mut refused = Client::connect_raw(addr, "dev-1", Some(will("status/other"))).await;
    match refused.recv().await {
        Some(Packet::ConnAck(ack)) => assert_eq!(ack.code, 0x05, "unauthorized will topic"),
        other => panic!("expected CONNACK 0x05, got {other:?}"),
    }
    assert_eq!(refused.recv().await, None, "connection must close");

    ids.send(identity("dev-1")).unwrap();
    let mut accepted = Client::connect_raw(addr, "dev-1", Some(will("status/dev-1"))).await;
    match accepted.recv().await {
        Some(Packet::ConnAck(ack)) => assert_eq!(ack.code, 0x00),
        other => panic!("expected CONNACK 0x00, got {other:?}"),
    }
}

/// `%i` substitution scopes rules to the connecting identity: alpha can use
/// its own namespace and nobody else's.
#[tokio::test]
async fn identity_substitution_scopes_topics() {
    let (addr, ids) = start_acl_node(
        r#"
        [[rules]]
        actions = ["publish", "subscribe"]
        topics = ["dev/%i/#"]
        "#,
    )
    .await;

    ids.send(identity("alpha")).unwrap();
    let mut alpha = Client::connect(addr, "alpha").await;
    assert_eq!(
        alpha
            .subscribe(&[
                ("dev/alpha/#", QoS::AtMostOnce),
                ("dev/beta/#", QoS::AtMostOnce)
            ])
            .await,
        vec![0x00, 0x80],
        "an identity may subscribe to its own namespace only"
    );
    alpha.publish_qos1("dev/alpha/state", 1, b"mine").await;
    match alpha.recv().await {
        Some(Packet::Publish(p)) => assert_eq!(p.topic, "dev/alpha/state"),
        other => panic!("expected own-namespace delivery, got {other:?}"),
    }
    alpha.publish_qos1("dev/beta/state", 2, b"theirs").await; // acked, dropped
    assert_eq!(sub_silence(&mut alpha).await, None);
}

/// Deny rules use overlap semantics: denying `secret/#` blocks a broad `#`
/// subscription outright, so wide filters cannot tunnel past denials.
#[tokio::test]
async fn deny_overlap_blocks_broad_subscription() {
    let (addr, ids) = start_acl_node(
        r##"
        [[rules]]
        actions = ["subscribe"]
        topics = ["#"]

        [[rules]]
        effect = "deny"
        actions = ["subscribe"]
        topics = ["secret/#"]
        "##,
    )
    .await;

    ids.send(identity("snoop")).unwrap();
    let mut snoop = Client::connect(addr, "snoop").await;
    assert_eq!(
        snoop
            .subscribe(&[("#", QoS::AtMostOnce), ("public/x", QoS::AtMostOnce)])
            .await,
        vec![0x80, 0x00],
        "a subscription overlapping a denied pattern is refused entirely"
    );
}

/// Allow rules use coverage semantics: granting `devices/+/state` does not
/// admit the broader `devices/#`.
#[tokio::test]
async fn narrow_allow_does_not_cover_broad_subscription() {
    let (addr, ids) = start_acl_node(
        r#"
        [[rules]]
        actions = ["subscribe"]
        topics = ["devices/+/state"]
        "#,
    )
    .await;

    ids.send(identity("dash")).unwrap();
    let mut dash = Client::connect(addr, "dash").await;
    assert_eq!(
        dash.subscribe(&[
            ("devices/#", QoS::AtMostOnce),
            ("devices/+/state", QoS::AtMostOnce),
        ])
        .await,
        vec![0x80, 0x00]
    );
}

async fn sub_silence(c: &mut Client) -> Option<Packet> {
    c.recv().await
}
