//! Integration tests for **retained messages** (MQTT 3.1.1, section 3.3.1).
//!
//! These tests are written test-first: they pin the spec-mandated behavior of
//! retained messages before the broker implements it, so most of them are
//! expected to fail until the hub stores retained publishes and replays them
//! to new subscriptions.
//!
//! Spec rules covered:
//! - [MQTT-3.3.1-5]  a new retained message replaces the previous one;
//! - [MQTT-3.3.1-6]  retained messages matching a new subscription are sent;
//! - [MQTT-3.3.1-7]  ...with the RETAIN flag set on that delivery;
//! - [MQTT-3.3.1-9]  live delivery to an existing subscription has RETAIN 0;
//! - [MQTT-3.3.1-10/11] a zero-byte retained payload clears the retained
//!   message and is not itself stored;
//! - [MQTT-3.8.4] every new SUBSCRIBE re-sends matching retained messages.

use std::time::Duration;

use bytes::Bytes;
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqttd::hub::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;

/// How long a negative assertion waits before concluding nothing will arrive.
const QUIET: Duration = Duration::from_millis(300);

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

/// A minimal MQTT client built on the project's framing + codec.
struct Client {
    reader: mqtt_net::FrameReader<tokio::net::tcp::OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<tokio::net::tcp::OwnedWriteHalf>,
    /// Counter for `QoS` 1 publish packet identifiers.
    next_pkid: u16,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr, client_id: &str) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (rh, wh) = stream.into_split();
        let mut client = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
            next_pkid: 0,
        };
        client
            .send(&Packet::Connect(Connect {
                protocol: V4,
                clean_session: true,
                keep_alive: 30,
                client_id: client_id.to_string(),
                last_will: None,
                username: None,
                password: None,
            }))
            .await;
        match client.recv().await {
            Packet::ConnAck(a) => assert_eq!(a.code, 0, "CONNACK should be success"),
            other => panic!("expected CONNACK, got {other:?}"),
        }
        client
    }

    async fn send(&mut self, packet: &Packet) {
        self.writer.send(packet).await.unwrap();
    }

    async fn recv(&mut self) -> Packet {
        timeout(Duration::from_secs(2), self.reader.next_packet())
            .await
            .expect("timed out waiting for a packet")
            .expect("transport error")
            .expect("connection closed unexpectedly")
    }

    /// Receive with a short timeout; `None` means nothing arrived (the negative
    /// assertion helper, mirroring the hub tests' 300ms `recv_packet` pattern).
    async fn recv_opt(&mut self) -> Option<Packet> {
        timeout(QUIET, self.reader.next_packet())
            .await
            .ok()?
            .expect("transport error")
    }

    /// SUBSCRIBE to a single filter at `QoS` 0 and wait for the SUBACK.
    async fn subscribe(&mut self, pkid: u16, filter: &str) {
        self.send(&Packet::Subscribe(Subscribe {
            pkid,
            filters: vec![SubscribeFilter {
                path: filter.into(),
                qos: QoS::AtMostOnce,
            }],
        }))
        .await;
        match self.recv().await {
            Packet::SubAck(a) => assert_eq!(a.pkid, pkid),
            other => panic!("expected SUBACK, got {other:?}"),
        }
    }

    /// PUBLISH at `QoS` 1 with the given retain flag and wait for the PUBACK.
    ///
    /// Using `QoS` 1 makes the publish synchronous: once the PUBACK is back,
    /// the broker has processed the message, so a subsequent SUBSCRIBE from
    /// another client is ordered after it.
    async fn publish(&mut self, topic: &str, payload: &'static [u8], retain: bool) {
        self.next_pkid += 1;
        let pkid = self.next_pkid;
        self.send(&Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain,
            topic: topic.into(),
            pkid: Some(pkid),
            payload: Bytes::from_static(payload),
        }))
        .await;
        assert_eq!(
            self.recv().await,
            Packet::PubAck(pkid),
            "publisher should get a PUBACK for its QoS 1 publish"
        );
    }

    /// Expect the next packet to be a PUBLISH with exactly these fields.
    async fn expect_publish(&mut self, topic: &str, payload: &[u8], retain: bool) {
        match self.recv().await {
            Packet::Publish(p) => {
                assert_eq!(p.topic, topic);
                assert_eq!(&p.payload[..], payload);
                assert_eq!(
                    p.retain, retain,
                    "RETAIN must be set iff this delivery replays a retained message"
                );
            }
            other => panic!("expected PUBLISH, got {other:?}"),
        }
    }
}

/// [MQTT-3.3.1-6] and [MQTT-3.3.1-7]: a PUBLISH with RETAIN=1 is stored by the
/// server; when a client subscribes to a matching filter *later*, the retained
/// message is sent to it, and that delivery carries RETAIN=1 to mark it as a
/// retained message being replayed for a new subscription.
#[tokio::test]
async fn late_subscriber_receives_retained_message_with_retain_set() {
    let addr = start_broker().await;

    let mut pubr = Client::connect(addr, "ret-pub").await;
    pubr.publish("ret/late", b"stored", true).await;

    // This client subscribes only after the retained publish completed.
    let mut sub = Client::connect(addr, "ret-late-sub").await;
    sub.subscribe(1, "ret/late").await;
    sub.expect_publish("ret/late", b"stored", true).await;
}

/// [MQTT-3.3.1-9]: when a retained publish is routed to a subscription that
/// already exists, it is ordinary live delivery — the server must send it with
/// RETAIN=0 (RETAIN=1 is reserved for replays to new subscriptions).
#[tokio::test]
async fn live_delivery_to_existing_subscriber_has_retain_clear() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "ret-live-sub").await;
    sub.subscribe(1, "ret/live").await;

    let mut pubr = Client::connect(addr, "ret-live-pub").await;
    pubr.publish("ret/live", b"now", true).await;

    sub.expect_publish("ret/live", b"now", false).await;
}

/// [MQTT-3.3.1-5]: a new retained message on a topic replaces any previously
/// retained message there. A later subscriber receives only the latest
/// payload, exactly once.
#[tokio::test]
async fn new_retained_publish_replaces_previous_one() {
    let addr = start_broker().await;

    let mut pubr = Client::connect(addr, "ret-replace-pub").await;
    pubr.publish("ret/replace", b"old", true).await;
    pubr.publish("ret/replace", b"new", true).await;

    let mut sub = Client::connect(addr, "ret-replace-sub").await;
    sub.subscribe(1, "ret/replace").await;
    sub.expect_publish("ret/replace", b"new", true).await;
    assert!(
        sub.recv_opt().await.is_none(),
        "only the latest retained message may be delivered, exactly once"
    );
}

/// [MQTT-3.3.1-10] and [MQTT-3.3.1-11]: a retained PUBLISH with a zero-length
/// payload removes the retained message for that topic and must not be stored
/// itself, so a later subscriber receives nothing.
#[tokio::test]
async fn zero_length_retained_publish_clears_retained_message() {
    let addr = start_broker().await;

    let mut pubr = Client::connect(addr, "ret-clear-pub").await;
    pubr.publish("ret/clear", b"to-be-cleared", true).await;
    pubr.publish("ret/clear", b"", true).await;

    let mut sub = Client::connect(addr, "ret-clear-sub").await;
    sub.subscribe(1, "ret/clear").await;
    assert!(
        sub.recv_opt().await.is_none(),
        "a zero-byte retained publish must clear the topic and not be stored"
    );
}

/// [MQTT-3.8.4] (with [MQTT-3.3.1-6]): a new subscription with a wildcard
/// filter receives *every* retained message whose topic matches, each with
/// RETAIN=1 — and none from non-matching topics.
#[tokio::test]
async fn wildcard_subscription_receives_all_matching_retained_messages() {
    let addr = start_broker().await;

    let mut pubr = Client::connect(addr, "ret-wild-pub").await;
    pubr.publish("ret/a/1", b"A", true).await;
    pubr.publish("ret/b/1", b"B", true).await;
    pubr.publish("other/x", b"X", true).await;

    let mut sub = Client::connect(addr, "ret-wild-sub").await;
    sub.subscribe(1, "ret/+/1").await;

    // Both matching retained messages arrive (in unspecified order), retained.
    let mut got = Vec::new();
    for _ in 0..2 {
        match sub.recv().await {
            Packet::Publish(p) => {
                assert!(p.retain, "retained replay must carry RETAIN=1");
                got.push((p.topic.clone(), p.payload.to_vec()));
            }
            other => panic!("expected a retained PUBLISH, got {other:?}"),
        }
    }
    got.sort();
    assert_eq!(
        got,
        vec![
            ("ret/a/1".to_string(), b"A".to_vec()),
            ("ret/b/1".to_string(), b"B".to_vec()),
        ]
    );
    assert!(
        sub.recv_opt().await.is_none(),
        "a retained message on a non-matching topic must not be delivered"
    );
}

/// MQTT 3.1.1 semantics ([MQTT-3.3.1-6] applies to *every* new SUBSCRIBE):
/// re-subscribing to the same filter delivers the retained message again.
#[tokio::test]
async fn resubscribing_delivers_retained_message_again() {
    let addr = start_broker().await;

    let mut pubr = Client::connect(addr, "ret-again-pub").await;
    pubr.publish("ret/again", b"sticky", true).await;

    let mut sub = Client::connect(addr, "ret-again-sub").await;
    sub.subscribe(1, "ret/again").await;
    sub.expect_publish("ret/again", b"sticky", true).await;

    // A second SUBSCRIBE for the same filter triggers a fresh replay.
    sub.subscribe(2, "ret/again").await;
    sub.expect_publish("ret/again", b"sticky", true).await;
}
