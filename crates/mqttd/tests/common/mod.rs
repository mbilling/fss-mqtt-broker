//! Shared end-to-end test harness (see `docs/TEST-PLAN.md`).
//!
//! Starts an in-process broker over real TCP loopback and provides a small MQTT
//! client — v3.1.1 and v5 — built on the project codec. Used by the integration
//! suites so each one does not re-implement `start_broker`/`Client`.
//!
//! The self-codec client is intentional: it gives precise control over the wire,
//! including the malformed/adversarial packets the darksky tests need.

#![allow(dead_code)] // each test crate uses only part of the harness

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mqtt_codec::{
    packet::{Auth, ConnAck, Connect, Publish, SubAck, Subscribe, SubscribeFilter},
    Packet, Properties, Property, ProtocolVersion, QoS,
};
use mqttd::conn::ConnPolicy;
use mqttd::hub::Hub;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};
use tokio::time::timeout;

pub const V4: ProtocolVersion = ProtocolVersion::V311;
pub const V5: ProtocolVersion = ProtocolVersion::V5;

/// How long a `recv`/`expect_*` waits before declaring the broker unresponsive.
const RECV_TIMEOUT: Duration = Duration::from_secs(2);

/// Spawn a permissive in-process broker (anonymous allowed, open ACL) on an
/// ephemeral port and return its address. The common path for protocol tests.
pub async fn start_broker() -> SocketAddr {
    let (hub, hub_tx) = Hub::new();
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, hub_tx.clone()));
        }
    });
    addr
}

/// Spawn a permissive broker whose per-session offline queue uses the given limits,
/// for exercising the bounded-queue overflow policy (ADR 0001 §6) end to end.
pub async fn start_broker_with_queue_limits(limits: mqtt_storage::QueueLimits) -> SocketAddr {
    use mqtt_cluster::NodeId;
    use mqtt_storage::MemorySessionStore;

    let store = Arc::new(MemorySessionStore::with_limits(limits));
    let (hub, hub_tx) = Hub::with_config(NodeId("node-local".into()), store);
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    spawn_client_loop(listener, hub_tx);
    addr
}

/// Bring up a two-node cluster (full peer mesh) on ephemeral ports and return each
/// node's client address. Cross-node routing is eventually consistent (interest is
/// gossiped on subscribe), so cluster tests retry until interest has propagated.
pub async fn start_two_node_cluster() -> (SocketAddr, SocketAddr) {
    use mqtt_cluster::NodeId;
    use mqtt_storage::MemorySessionStore;

    let peer_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let paddr_a = peer_a.local_addr().unwrap();
    let paddr_b = peer_b.local_addr().unwrap();
    let cli_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cli_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let caddr_a = cli_a.local_addr().unwrap();
    let caddr_b = cli_b.local_addr().unwrap();

    let id_a = NodeId("node-a".into());
    let id_b = NodeId("node-b".into());
    let (hub_a, tx_a) = Hub::with_config(id_a.clone(), Arc::new(MemorySessionStore::new()));
    let (hub_b, tx_b) = Hub::with_config(id_b.clone(), Arc::new(MemorySessionStore::new()));
    tokio::spawn(hub_a.run());
    tokio::spawn(hub_b.run());

    spawn_client_loop(cli_a, tx_a.clone());
    spawn_client_loop(cli_b, tx_b.clone());

    tokio::spawn(mqttd::peer::serve_listener(
        peer_a,
        id_a.clone(),
        tx_a.clone(),
        None,
        None,
    ));
    tokio::spawn(mqttd::peer::serve_listener(
        peer_b,
        id_b.clone(),
        tx_b.clone(),
        None,
        None,
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_b.to_string(),
        id_a,
        tx_a,
        None,
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_a.to_string(),
        id_b,
        tx_b,
        None,
    ));

    (caddr_a, caddr_b)
}

fn spawn_client_loop(
    listener: TcpListener,
    tx: tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>,
) {
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
        }
    });
}

/// Spawn an in-process broker driven by a caller-supplied [`ConnPolicy`] — for
/// tests that need a specific authenticator, ACL, or enhanced-auth mechanism.
pub async fn start_broker_with_policy(policy: Arc<ConnPolicy>) -> SocketAddr {
    let (hub, hub_tx) = Hub::new();
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle_stream(
                stream,
                Some(peer),
                None,
                policy.clone(),
                hub_tx.clone(),
            ));
        }
    });
    addr
}

/// A minimal MQTT client over the project framing + codec.
pub struct Client {
    reader: mqtt_net::FrameReader<OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<OwnedWriteHalf>,
}

impl Client {
    /// Open a TCP connection framed at `version` (no CONNECT sent yet).
    pub async fn open(addr: SocketAddr, version: ProtocolVersion) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (rh, wh) = stream.into_split();
        Client {
            reader: mqtt_net::FrameReader::new(rh, version),
            writer: mqtt_net::FrameWriter::new(wh, version),
        }
    }

    /// Connect as a clean v3.1.1 session, asserting a successful CONNACK.
    pub async fn connect(addr: SocketAddr, client_id: &str) -> Self {
        Self::connect_v311(addr, client_id, true).await.0
    }

    /// Connect as v3.1.1 with an explicit clean-session flag; returns the client and
    /// the CONNACK `session_present` flag.
    pub async fn connect_v311(addr: SocketAddr, client_id: &str, clean: bool) -> (Self, bool) {
        let mut c = Self::open(addr, V4).await;
        c.send(&Packet::Connect(Connect {
            properties: Properties::new(),
            protocol: V4,
            clean_session: clean,
            keep_alive: 30,
            client_id: client_id.to_string(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await;
        let present = match c.recv().await {
            Packet::ConnAck(a) => {
                assert_eq!(a.code, 0, "v3.1.1 CONNACK should be success");
                a.session_present
            }
            other => panic!("expected CONNACK, got {other:?}"),
        };
        (c, present)
    }

    /// Connect as v5 with the given CONNECT properties; returns the client and the
    /// full CONNACK (so the caller can assert negotiated properties or a reason code).
    pub async fn connect_v5(
        addr: SocketAddr,
        client_id: &str,
        clean_start: bool,
        properties: Vec<Property>,
    ) -> (Self, ConnAck) {
        let mut c = Self::open(addr, V5).await;
        c.send(&Packet::Connect(Connect {
            properties: Properties(properties),
            protocol: V5,
            clean_session: clean_start,
            keep_alive: 30,
            client_id: client_id.to_string(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await;
        match c.recv().await {
            Packet::ConnAck(a) => (c, a),
            other => panic!("expected v5 CONNACK, got {other:?}"),
        }
    }

    /// Connect as a clean v5 session, asserting success.
    pub async fn connect_v5_ok(addr: SocketAddr, client_id: &str) -> Self {
        let (c, ack) = Self::connect_v5(addr, client_id, true, vec![]).await;
        assert_eq!(ack.code, 0, "v5 CONNACK should be success");
        c
    }

    pub async fn send(&mut self, packet: &Packet) {
        self.writer.send(packet).await.unwrap();
    }

    /// The next packet, or panic on timeout/close.
    pub async fn recv(&mut self) -> Packet {
        timeout(RECV_TIMEOUT, self.reader.next_packet())
            .await
            .expect("timed out waiting for a packet")
            .expect("transport error")
            .expect("connection closed unexpectedly")
    }

    /// The next packet within the window, or `None` if none arrived (still open).
    pub async fn try_recv(&mut self) -> Option<Packet> {
        match timeout(Duration::from_millis(300), self.reader.next_packet()).await {
            Ok(r) => r.expect("transport error"),
            Err(_) => None,
        }
    }

    /// Assert that no packet arrives in the quiet window (the socket stays open).
    pub async fn expect_silence(&mut self) {
        if let Some(p) = self.try_recv().await {
            panic!("expected silence, got {p:?}");
        }
    }

    /// Assert the broker closed the connection (clean EOF).
    pub async fn expect_closed(&mut self) {
        let pkt = timeout(RECV_TIMEOUT, self.reader.next_packet())
            .await
            .expect("timed out waiting for close")
            .expect("transport error");
        assert!(pkt.is_none(), "expected connection close, got {pkt:?}");
    }

    /// Subscribe to one filter and return the SUBACK.
    pub async fn subscribe(&mut self, pkid: u16, filter: &str, qos: QoS) -> SubAck {
        self.send(&Packet::Subscribe(Subscribe {
            properties: Properties::new(),
            pkid,
            filters: vec![SubscribeFilter {
                options: mqtt_codec::SubscriptionOptions::default(),
                path: filter.into(),
                qos,
            }],
        }))
        .await;
        match self.recv().await {
            Packet::SubAck(a) => a,
            other => panic!("expected SUBACK, got {other:?}"),
        }
    }

    /// Publish with the given `QoS`, packet id, and (v5) properties. For `QoS` > 0 the
    /// caller supplies the packet id so it can drive the ack handshake.
    pub async fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        pkid: Option<u16>,
        properties: Vec<Property>,
    ) {
        self.send(&Packet::Publish(Publish {
            properties: Properties(properties),
            dup: false,
            qos,
            retain: false,
            topic: topic.into(),
            pkid,
            payload: bytes::Bytes::copy_from_slice(payload),
        }))
        .await;
    }

    /// Publish a retained message (`QoS` 0).
    pub async fn publish_retained(&mut self, topic: &str, payload: &[u8]) {
        self.send(&Packet::Publish(Publish {
            properties: Properties::new(),
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: topic.into(),
            pkid: None,
            payload: bytes::Bytes::copy_from_slice(payload),
        }))
        .await;
    }

    pub async fn puback(&mut self, pkid: u16) {
        self.send(&Packet::PubAck(pkid.into())).await;
    }

    /// Send a clean DISCONNECT and wait for the broker to close the socket. Waiting
    /// for the close guarantees the Detach is processed before the test proceeds.
    pub async fn disconnect(&mut self) {
        self.send(&Packet::Disconnect(
            mqtt_codec::packet::Disconnect::default(),
        ))
        .await;
        self.expect_closed().await;
    }

    /// The next packet expected to be a PUBLISH.
    pub async fn expect_publish(&mut self) -> Publish {
        match self.recv().await {
            Packet::Publish(p) => p,
            other => panic!("expected PUBLISH, got {other:?}"),
        }
    }

    /// The next packet expected to be an AUTH.
    pub async fn expect_auth(&mut self) -> Auth {
        match self.recv().await {
            Packet::Auth(a) => a,
            other => panic!("expected AUTH, got {other:?}"),
        }
    }
}

/// Helpers for the HMAC-SHA256 enhanced-authentication mechanism (ADR 0013): a
/// broker policy configured with one subject ("alice"), the proof the client
/// returns, and an AUTH-packet builder. Shared by the sunshine and darksky suites.
pub mod enhanced {
    use super::{Arc, ConnPolicy, Packet, Properties, Property};

    pub const METHOD: &str = "HMAC-SHA256";
    pub const SUBJECT: &str = "alice";
    const SECRET: &[u8] = b"alice-secret";

    /// A broker policy whose enhanced authenticator knows `SUBJECT`'s secret.
    #[must_use]
    pub fn policy() -> Arc<ConnPolicy> {
        let mut secrets = std::collections::HashMap::new();
        secrets.insert(SUBJECT.to_string(), SECRET.to_vec());
        Arc::new(ConnPolicy {
            auth: Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }),
            enhanced: Some(Arc::new(mqtt_auth::HmacChallengeAuthenticator::new(
                secrets,
            ))),
            authz: Arc::new(mqtt_auth::AllowAll),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            store: None,
        })
    }

    /// The correct HMAC-SHA256 proof over `nonce` for `SUBJECT`.
    #[must_use]
    pub fn proof(nonce: &[u8]) -> Vec<u8> {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, SECRET);
        ring::hmac::sign(&key, nonce).as_ref().to_vec()
    }

    /// An AUTH packet for `METHOD` with the given reason and data.
    #[must_use]
    pub fn auth(reason: u8, data: &[u8]) -> Packet {
        Packet::Auth(mqtt_codec::packet::Auth {
            reason,
            properties: Properties(vec![
                Property::AuthenticationMethod(METHOD.into()),
                Property::AuthenticationData(bytes::Bytes::copy_from_slice(data)),
            ]),
        })
    }

    /// Extract a challenge nonce (Authentication Data) from an AUTH's properties.
    #[must_use]
    pub fn nonce_of(props: &Properties) -> Vec<u8> {
        props
            .0
            .iter()
            .find_map(|p| match p {
                Property::AuthenticationData(b) => Some(b.to_vec()),
                _ => None,
            })
            .expect("an AUTH challenge nonce")
    }
}
