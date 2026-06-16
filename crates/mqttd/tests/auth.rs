//! CONNECT authentication-gate integration tests (ADR 0004): the broker must
//! verify credentials BEFORE attaching a session to the hub, answer failures
//! with CONNACK 0x04 (bad user name or password) or 0x05 (not authorized), and
//! close the connection without serving a single packet.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mqtt_auth::basic::BasicAuthenticator;
use mqtt_auth::{Authenticator, Identity};
use mqtt_codec::{
    packet::{ConnAck, Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqttd::hub::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;

/// CONNACK return codes under test (MQTT 3.1.1, table 3.1).
const BAD_CREDENTIALS: u8 = 0x04;
const NOT_AUTHORIZED: u8 = 0x05;

/// Spawn a broker whose accept loop injects the given transport identity
/// (simulating an mTLS-verified client certificate) and authenticator into
/// every connection; returns its address.
async fn start_broker(identity: Option<Identity>, auth: Arc<dyn Authenticator>) -> SocketAddr {
    let (hub, hub_tx) = Hub::new();
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let policy = std::sync::Arc::new(mqttd::conn::ConnPolicy {
                auth: auth.clone(),
                authz: std::sync::Arc::new(mqtt_auth::AllowAll),
                audit: std::sync::Arc::new(mqtt_observability::AuditLog::new()),
                proxy: None,
                store: None,
            });
            tokio::spawn(mqttd::conn::handle_stream(
                stream,
                None,
                identity.clone(),
                policy,
                hub_tx.clone(),
            ));
        }
    });
    addr
}

/// Spawn a broker on the legacy `conn::handle` path (the test-only shim the
/// existing integration suites use); returns its address.
async fn start_legacy_broker() -> SocketAddr {
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

/// A minimal MQTT client built on the project's framing + codec.
struct Client {
    reader: mqtt_net::FrameReader<tokio::net::tcp::OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl Client {
    /// Open a TCP connection, send CONNECT with the given credentials, and
    /// return the client together with the broker's CONNACK.
    async fn connect(
        addr: SocketAddr,
        client_id: &str,
        username: Option<&str>,
        password: Option<&'static [u8]>,
    ) -> (Self, ConnAck) {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (rh, wh) = stream.into_split();
        let mut client = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        client
            .send(&Packet::Connect(Connect {
                protocol: V4,
                clean_session: true,
                keep_alive: 30,
                client_id: client_id.to_string(),
                last_will: None,
                username: username.map(str::to_string),
                password: password.map(bytes::Bytes::from_static),
            }))
            .await;
        match client.recv().await {
            Some(Packet::ConnAck(ack)) => (client, ack),
            other => panic!("expected CONNACK, got {other:?}"),
        }
    }

    async fn send(&mut self, packet: &Packet) {
        self.writer.send(packet).await.unwrap();
    }

    /// Send without unwrapping: rejected connections may already be closed.
    async fn send_ignoring_errors(&mut self, packet: &Packet) {
        let _ = self.writer.send(packet).await;
    }

    /// Next packet within the test window; EOF and transport errors map to
    /// `None` (the assertions only care whether an MQTT packet arrived).
    async fn recv(&mut self) -> Option<Packet> {
        timeout(Duration::from_millis(300), self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

fn subscribe_packet(filter: &str) -> Packet {
    Packet::Subscribe(Subscribe {
        pkid: 1,
        filters: vec![SubscribeFilter {
            path: filter.to_string(),
            qos: QoS::AtMostOnce,
        }],
    })
}

fn publish_packet(topic: &str, payload: &'static [u8]) -> Packet {
    Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: topic.to_string(),
        pkid: None,
        payload: bytes::Bytes::from_static(payload),
    })
}

/// Subscribe + publish on one connection and assert the message comes back —
/// proof the session was attached to the hub and is fully functional.
async fn assert_working_session(client: &mut Client, topic: &'static str) {
    client.send(&subscribe_packet(topic)).await;
    assert!(
        matches!(client.recv().await, Some(Packet::SubAck(_))),
        "expected SUBACK"
    );
    client.send(&publish_packet(topic, b"ping")).await;
    match client.recv().await {
        Some(Packet::Publish(p)) => {
            assert_eq!(p.topic, topic);
            assert_eq!(&p.payload[..], b"ping");
        }
        other => panic!("expected the published message back, got {other:?}"),
    }
}

#[tokio::test]
async fn default_policy_rejects_anonymous_with_not_authorized() {
    let auth = Arc::new(BasicAuthenticator {
        allow_anonymous: false,
    });
    let addr = start_broker(None, auth).await;

    let (mut client, ack) = Client::connect(addr, "anon", None, None).await;
    assert_eq!(ack.code, NOT_AUTHORIZED, "anonymous must get CONNACK 0x05");
    assert!(!ack.session_present);

    // The connection must be closed without a session: a SUBSCRIBE gets no
    // SUBACK (the write itself may already fail on the closed socket).
    client
        .send_ignoring_errors(&subscribe_packet("forbidden/#"))
        .await;
    assert_eq!(
        client.recv().await,
        None,
        "a rejected client must never receive a SUBACK"
    );
}

#[tokio::test]
async fn allow_anonymous_permits_a_working_session() {
    let auth = Arc::new(BasicAuthenticator {
        allow_anonymous: true,
    });
    let addr = start_broker(None, auth).await;

    let (mut client, ack) = Client::connect(addr, "anon-ok", None, None).await;
    assert_eq!(ack.code, 0x00, "anonymous must be accepted when allowed");
    assert_working_session(&mut client, "open/topic").await;
}

#[tokio::test]
async fn mtls_identity_is_accepted_even_without_anonymous() {
    // Simulate the TLS layer having verified a client certificate: the accept
    // loop injects the extracted identity; anonymous stays forbidden.
    let identity = Identity {
        subject: "device-7".into(),
        groups: vec![],
    };
    let auth = Arc::new(BasicAuthenticator {
        allow_anonymous: false,
    });
    let addr = start_broker(Some(identity), auth).await;

    let (mut client, ack) = Client::connect(addr, "dev7", None, None).await;
    assert_eq!(ack.code, 0x00, "a verified cert identity must be accepted");
    assert_working_session(&mut client, "devices/7/state").await;
}

#[tokio::test]
async fn password_credentials_are_rejected_with_bad_credentials() {
    // BasicAuthenticator has no password verifier — even with anonymous
    // allowed, presented credentials must fail closed with CONNACK 0x04.
    let auth = Arc::new(BasicAuthenticator {
        allow_anonymous: true,
    });
    let addr = start_broker(None, auth).await;

    let (mut client, ack) =
        Client::connect(addr, "alice-conn", Some("alice"), Some(b"secret")).await;
    assert_eq!(ack.code, BAD_CREDENTIALS, "password must get CONNACK 0x04");
    assert!(!ack.session_present);
    assert_eq!(
        client.recv().await,
        None,
        "the connection must close after the rejection"
    );
}

#[tokio::test]
async fn legacy_handle_path_still_accepts_anonymous_clients() {
    // Guards the existing integration suites, which all go through
    // `conn::handle` (anonymous allowed, no transport identity).
    let addr = start_legacy_broker().await;

    let (mut client, ack) = Client::connect(addr, "legacy", None, None).await;
    assert_eq!(ack.code, 0x00, "the legacy path must stay permissive");
    client.send(&Packet::PingReq).await;
    assert_eq!(client.recv().await, Some(Packet::PingResp));
}
