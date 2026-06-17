//! End-to-end tests for real `QoS` 1 downstream delivery (MQTT 3.1.1).
//!
//! These tests pin the spec behavior for `QoS` 1 subscriptions: SUBACK grants
//! the requested `QoS` [MQTT-3.8.4-5/6], downstream delivery happens at
//! `min(publish QoS, granted QoS)` with broker-assigned packet ids
//! [MQTT-3.8.4-6, MQTT-4.3.2], unacknowledged `QoS` 1 messages are redelivered
//! with DUP after a reconnect [MQTT-4.4.0-1], and offline-queued messages for
//! persistent sessions replay at `QoS` 1 until acknowledged.

use std::time::Duration;

use mqtt_codec::{
    packet::{Connect, Publish, SubAck, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqttd::hub::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;

/// How long we wait for an expected packet before declaring it missing.
const RECV_TIMEOUT: Duration = Duration::from_millis(300);

/// How long we allow a write (or an expected close) to take.
const IO_TIMEOUT: Duration = Duration::from_secs(2);

/// Spawn the broker on an ephemeral port and return its address.
async fn start_broker() -> std::net::SocketAddr {
    let (hub, hub_tx) = Hub::new();
    tokio::spawn(hub.run());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let tx = hub_tx.clone();
            tokio::spawn(mqttd::conn::handle(stream, tx));
        }
    });
    addr
}

/// A minimal MQTT client built on the project's framing + codec, with `QoS` 1
/// helpers: subscribe asserting the granted `QoS`, publish with qos+pkid,
/// receiving a full PUBLISH, and sending PUBACK.
struct Client {
    reader: mqtt_net::FrameReader<tokio::net::tcp::OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr, client_id: &str) -> Self {
        Client::connect_opts(addr, client_id, true).await.0
    }

    /// Connect with an explicit clean-session flag; returns the client and the
    /// CONNACK `session_present` flag.
    async fn connect_opts(
        addr: std::net::SocketAddr,
        client_id: &str,
        clean_session: bool,
    ) -> (Self, bool) {
        let stream = timeout(IO_TIMEOUT, TcpStream::connect(addr))
            .await
            .expect("timed out connecting")
            .unwrap();
        let (rh, wh) = stream.into_split();
        let mut client = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        client
            .send(&Packet::Connect(Connect {
                properties: mqtt_codec::Properties::new(),
                protocol: V4,
                clean_session,
                keep_alive: 30,
                client_id: client_id.to_string(),
                last_will: None,
                username: None,
                password: None,
            }))
            .await;
        let session_present = match client.recv().await {
            Packet::ConnAck(a) => {
                assert_eq!(a.code, 0, "CONNACK should be success");
                a.session_present
            }
            other => panic!("expected CONNACK, got {other:?}"),
        };
        (client, session_present)
    }

    async fn send(&mut self, packet: &Packet) {
        timeout(IO_TIMEOUT, self.writer.send(packet))
            .await
            .expect("timed out writing a packet")
            .unwrap();
    }

    async fn recv(&mut self) -> Packet {
        timeout(RECV_TIMEOUT, self.reader.next_packet())
            .await
            .expect("timed out waiting for a packet")
            .expect("transport error")
            .expect("connection closed unexpectedly")
    }

    /// Subscribe to `filter` at `qos` and assert the SUBACK grants exactly the
    /// requested `QoS` [MQTT-3.8.4-5/6].
    async fn subscribe(&mut self, pkid: u16, filter: &str, qos: QoS) {
        self.send(&Packet::Subscribe(Subscribe {
            properties: mqtt_codec::Properties::new(),
            pkid,
            filters: vec![SubscribeFilter {
                options: mqtt_codec::SubscriptionOptions::default(),
                path: filter.into(),
                qos,
            }],
        }))
        .await;
        match self.recv().await {
            Packet::SubAck(SubAck {
                pkid: ack_pkid,
                return_codes,
                ..
            }) => {
                assert_eq!(ack_pkid, pkid, "SUBACK pkid must echo the SUBSCRIBE");
                assert_eq!(
                    return_codes,
                    vec![qos as u8],
                    "SUBACK must grant the requested QoS [MQTT-3.8.4-5/6]"
                );
            }
            other => panic!("expected SUBACK, got {other:?}"),
        }
    }

    /// Publish `payload` to `topic` at `qos`; `pkid` must be `Some` iff `qos > 0`.
    async fn publish(&mut self, topic: &str, qos: QoS, pkid: Option<u16>, payload: &'static [u8]) {
        self.send(&Packet::Publish(Publish {
            properties: mqtt_codec::Properties::new(),
            dup: false,
            qos,
            retain: false,
            topic: topic.into(),
            pkid,
            payload: bytes::Bytes::from_static(payload),
        }))
        .await;
    }

    /// Receive the next packet and require it to be a PUBLISH; returns the full
    /// struct so callers can assert dup/qos/pkid precisely.
    async fn recv_publish(&mut self) -> Publish {
        match self.recv().await {
            Packet::Publish(p) => p,
            other => panic!("expected PUBLISH, got {other:?}"),
        }
    }

    /// Acknowledge a `QoS` 1 PUBLISH received from the broker.
    async fn puback(&mut self, pkid: u16) {
        self.send(&Packet::PubAck(pkid.into())).await;
    }

    /// Assert that nothing arrives within the receive window (the connection
    /// stays open and silent).
    async fn expect_silence(&mut self) {
        match timeout(RECV_TIMEOUT, self.reader.next_packet()).await {
            Err(_) => {} // timed out: nothing was delivered, as expected
            Ok(pkt) => {
                let pkt = pkt.expect("transport error");
                panic!("expected no delivery, got {pkt:?}");
            }
        }
    }

    /// Expect the broker to have closed the connection (clean EOF).
    async fn expect_closed(&mut self) {
        let pkt = timeout(IO_TIMEOUT, self.reader.next_packet())
            .await
            .expect("timed out waiting for EOF")
            .expect("transport error");
        assert!(pkt.is_none(), "expected connection close, got {pkt:?}");
    }
}

/// [MQTT-3.8.4-5/6] The SUBACK return code is the *granted* `QoS`: subscribing
/// with requested `QoS` 1 must be answered with return code 0x01, not 0x00.
#[tokio::test]
async fn suback_grants_requested_qos1() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "grant-sub").await;
    // The helper asserts return_codes == [0x01].
    sub.subscribe(1, "grant/topic", QoS::AtLeastOnce).await;
}

/// [MQTT-4.3.2] `QoS` 1 delivery downstream: a publisher's `QoS` 1 PUBLISH is
/// acknowledged by the broker, and a `QoS` 1 subscriber receives the message as a
/// `QoS` 1 PUBLISH carrying a packet identifier, which it acknowledges.
#[tokio::test]
async fn qos1_publish_is_delivered_downstream_at_qos1() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "q1-sub").await;
    sub.subscribe(1, "q1/topic", QoS::AtLeastOnce).await;

    let mut pubr = Client::connect(addr, "q1-pub").await;
    pubr.publish("q1/topic", QoS::AtLeastOnce, Some(42), b"at-least-once")
        .await;
    assert_eq!(
        pubr.recv().await,
        Packet::PubAck(42.into()),
        "broker must PUBACK the inbound QoS 1 publish"
    );

    let p = sub.recv_publish().await;
    assert_eq!(p.topic, "q1/topic");
    assert_eq!(&p.payload[..], b"at-least-once");
    assert_eq!(
        p.qos,
        QoS::AtLeastOnce,
        "delivery to a QoS 1 subscription of a QoS 1 publish must be QoS 1 [MQTT-4.3.2]"
    );
    let pkid = p
        .pkid
        .expect("a QoS 1 PUBLISH must carry a packet identifier");
    assert_ne!(pkid, 0, "packet identifiers must be non-zero");
    assert!(!p.dup, "first delivery must not have DUP set");

    // Complete the QoS 1 exchange from the receiver side.
    sub.puback(pkid).await;
}

/// [MQTT-3.8.4-6] Delivery `QoS` is `min(publish QoS, granted QoS)`: a `QoS` 0
/// subscriber gets a `QoS` 1 publish downgraded to `QoS` 0 (no packet id), and
/// a `QoS` 1 subscriber gets a `QoS` 0 publish at `QoS` 0.
#[tokio::test]
async fn delivery_qos_is_min_of_publish_and_granted_qos() {
    let addr = start_broker().await;

    let mut sub0 = Client::connect(addr, "min-sub0").await;
    sub0.subscribe(1, "min/down", QoS::AtMostOnce).await;
    let mut sub1 = Client::connect(addr, "min-sub1").await;
    sub1.subscribe(1, "min/up", QoS::AtLeastOnce).await;

    let mut pubr = Client::connect(addr, "min-pub").await;

    // QoS 1 publish -> QoS 0 subscription: downgraded to QoS 0, no packet id.
    pubr.publish("min/down", QoS::AtLeastOnce, Some(7), b"downgraded")
        .await;
    assert_eq!(pubr.recv().await, Packet::PubAck(7.into()));
    let p = sub0.recv_publish().await;
    assert_eq!(p.topic, "min/down");
    assert_eq!(&p.payload[..], b"downgraded");
    assert_eq!(
        p.qos,
        QoS::AtMostOnce,
        "QoS 1 publish to a QoS 0 subscription must be delivered at QoS 0 [MQTT-3.8.4-6]"
    );
    assert_eq!(p.pkid, None, "a QoS 0 PUBLISH must not carry a packet id");

    // QoS 0 publish -> QoS 1 subscription: stays QoS 0 (no upgrade).
    pubr.publish("min/up", QoS::AtMostOnce, None, b"not-upgraded")
        .await;
    let p = sub1.recv_publish().await;
    assert_eq!(p.topic, "min/up");
    assert_eq!(&p.payload[..], b"not-upgraded");
    assert_eq!(
        p.qos,
        QoS::AtMostOnce,
        "a QoS 0 publish must never be upgraded by a QoS 1 subscription [MQTT-3.8.4-6]"
    );
    assert_eq!(p.pkid, None, "a QoS 0 PUBLISH must not carry a packet id");
}

/// Two un-acknowledged in-flight `QoS` 1 messages to the same subscriber must
/// arrive with distinct, non-zero packet identifiers (a packet id may only be
/// reused after its PUBACK).
#[tokio::test]
async fn inflight_qos1_messages_have_distinct_packet_ids() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "dist-sub").await;
    sub.subscribe(1, "dist/topic", QoS::AtLeastOnce).await;

    let mut pubr = Client::connect(addr, "dist-pub").await;
    pubr.publish("dist/topic", QoS::AtLeastOnce, Some(10), b"first")
        .await;
    assert_eq!(pubr.recv().await, Packet::PubAck(10.into()));
    pubr.publish("dist/topic", QoS::AtLeastOnce, Some(11), b"second")
        .await;
    assert_eq!(pubr.recv().await, Packet::PubAck(11.into()));

    // Receive both without acking either, so both stay in flight.
    let first = sub.recv_publish().await;
    let second = sub.recv_publish().await;
    assert_eq!(&first.payload[..], b"first");
    assert_eq!(&second.payload[..], b"second");
    assert_eq!(first.qos, QoS::AtLeastOnce);
    assert_eq!(second.qos, QoS::AtLeastOnce);

    let id1 = first.pkid.expect("first QoS 1 PUBLISH must carry a pkid");
    let id2 = second.pkid.expect("second QoS 1 PUBLISH must carry a pkid");
    assert_ne!(id1, 0, "packet identifiers must be non-zero");
    assert_ne!(id2, 0, "packet identifiers must be non-zero");
    assert_ne!(
        id1, id2,
        "concurrently in-flight QoS 1 messages must have distinct packet ids"
    );

    sub.puback(id1).await;
    sub.puback(id2).await;
}

/// [MQTT-4.4.0-1] An un-acknowledged `QoS` 1 message must be redelivered with
/// DUP=1 when the (persistent-session) client reconnects; once acknowledged it
/// must not be delivered again.
#[tokio::test]
async fn unacked_qos1_message_is_redelivered_with_dup_on_reconnect() {
    let addr = start_broker().await;

    let (mut sub, present) = Client::connect_opts(addr, "redeliver", false).await;
    assert!(!present, "no session should exist yet");
    sub.subscribe(1, "redo/topic", QoS::AtLeastOnce).await;

    let mut pubr = Client::connect(addr, "redeliver-pub").await;
    pubr.publish("redo/topic", QoS::AtLeastOnce, Some(5), b"needs-ack")
        .await;
    assert_eq!(pubr.recv().await, Packet::PubAck(5.into()));

    // First delivery arrives at QoS 1 — but the subscriber never PUBACKs it and
    // its connection drops (no DISCONNECT, simulating a network failure).
    let p = sub.recv_publish().await;
    assert_eq!(&p.payload[..], b"needs-ack");
    assert_eq!(p.qos, QoS::AtLeastOnce);
    assert!(p.pkid.is_some(), "QoS 1 PUBLISH must carry a packet id");
    drop(sub);

    // Reconnect: the un-acked message must come again, flagged as a duplicate.
    // (We are lenient about whether the broker reuses the original pkid.)
    let (mut sub, present) = Client::connect_opts(addr, "redeliver", false).await;
    assert!(present, "persistent session must survive the drop");
    let p = sub.recv_publish().await;
    assert_eq!(p.topic, "redo/topic");
    assert_eq!(&p.payload[..], b"needs-ack");
    assert_eq!(
        p.qos,
        QoS::AtLeastOnce,
        "redelivery must stay at QoS 1 [MQTT-4.4.0-1]"
    );
    assert!(
        p.dup,
        "redelivery of an unacknowledged QoS 1 message must set DUP [MQTT-4.4.0-1]"
    );
    let pkid = p.pkid.expect("redelivered QoS 1 PUBLISH must carry a pkid");
    assert_ne!(pkid, 0, "packet identifiers must be non-zero");

    // Ack it this time; a further reconnect must deliver nothing.
    sub.puback(pkid).await;
    sub.send(&Packet::Disconnect(mqtt_codec::Disconnect::default()))
        .await;
    sub.expect_closed().await;

    let (mut sub, present) = Client::connect_opts(addr, "redeliver", false).await;
    assert!(present, "session must still be present");
    sub.expect_silence().await;
}

/// `QoS` 1 publishes that arrive while a persistent `QoS` 1 subscriber is
/// offline must be queued and replayed at `QoS` 1 (with a packet id) on
/// reconnect; once acknowledged they must not replay again.
#[tokio::test]
async fn offline_qos1_message_replays_at_qos1_until_acked() {
    let addr = start_broker().await;

    let (mut sub, present) = Client::connect_opts(addr, "offline-q1", false).await;
    assert!(!present, "no session should exist yet");
    sub.subscribe(1, "off/topic", QoS::AtLeastOnce).await;
    // Disconnect cleanly; waiting for the close guarantees the Detach is
    // enqueued at the hub before we publish.
    sub.send(&Packet::Disconnect(mqtt_codec::Disconnect::default()))
        .await;
    sub.expect_closed().await;

    // Publish QoS 1 while the subscriber is away; the PUBACK guarantees the
    // broker received (and thus queued) the message before we reconnect.
    let mut pubr = Client::connect(addr, "offline-q1-pub").await;
    pubr.publish("off/topic", QoS::AtLeastOnce, Some(3), b"stored")
        .await;
    assert_eq!(pubr.recv().await, Packet::PubAck(3.into()));

    // Reconnect: the queued message must replay as a QoS 1 PUBLISH.
    let (mut sub, present) = Client::connect_opts(addr, "offline-q1", false).await;
    assert!(present, "session should now be present");
    let p = sub.recv_publish().await;
    assert_eq!(p.topic, "off/topic");
    assert_eq!(&p.payload[..], b"stored");
    assert_eq!(
        p.qos,
        QoS::AtLeastOnce,
        "a message queued for a QoS 1 subscription must replay at QoS 1"
    );
    let pkid = p.pkid.expect("replayed QoS 1 PUBLISH must carry a pkid");
    assert_ne!(pkid, 0, "packet identifiers must be non-zero");

    // Ack it; after a further reconnect nothing may be redelivered.
    sub.puback(pkid).await;
    sub.send(&Packet::Disconnect(mqtt_codec::Disconnect::default()))
        .await;
    sub.expect_closed().await;

    let (mut sub, present) = Client::connect_opts(addr, "offline-q1", false).await;
    assert!(present, "session must still be present");
    sub.expect_silence().await;
}
