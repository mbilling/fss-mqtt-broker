//! `QoS` 2 (exactly-once) integration tests: publishers and subscribers talk to
//! a running broker over real TCP sockets, using the project's own codec as
//! the client, and exercise the full four-way PUBLISH/PUBREC/PUBREL/PUBCOMP
//! handshake in both directions.
//!
//! These tests pin the MQTT 3.1.1 exactly-once rules:
//! - [MQTT-3.8.4-5]/[MQTT-3.8.4-6]: SUBACK carries one return code per filter
//!   and may grant the requested `QoS`, up to `QoS` 2.
//! - [MQTT-4.3.3]: inbound `QoS` 2 flow is PUBLISH -> PUBREC, PUBREL -> PUBCOMP.
//! - [MQTT-4.3.3-2]: a PUBLISH re-sent before PUBREL must not be re-delivered
//!   to subscribers (inbound deduplication by packet identifier).
//! - Downstream, a `QoS` 2 subscriber gets a `QoS` 2 PUBLISH with a packet id and
//!   must complete PUBREC/PUBREL/PUBCOMP; an unacknowledged delivery is
//!   re-sent with DUP on the next session resume, and never after PUBCOMP.

use std::time::Duration;

use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqttd::hub::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;

/// Window for any single network await: generous for a loopback socket, short
/// enough that a missing packet fails the test quickly instead of hanging.
const RECV_TIMEOUT: Duration = Duration::from_millis(300);

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

/// A minimal MQTT client built on the project's framing + codec, extended with
/// `QoS` 2 helpers (subscribe-with-grant, `QoS` 2 publish, PUBREC/PUBREL/PUBCOMP).
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
        let stream = timeout(RECV_TIMEOUT, TcpStream::connect(addr))
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
        timeout(RECV_TIMEOUT, self.writer.send(packet))
            .await
            .expect("timed out sending a packet")
            .unwrap();
    }

    async fn recv(&mut self) -> Packet {
        timeout(RECV_TIMEOUT, self.reader.next_packet())
            .await
            .expect("timed out waiting for a packet")
            .expect("transport error")
            .expect("connection closed unexpectedly")
    }

    /// Expect the next packet to be a PUBLISH and return it whole.
    async fn recv_publish(&mut self) -> Publish {
        match self.recv().await {
            Packet::Publish(p) => p,
            other => panic!("expected PUBLISH, got {other:?}"),
        }
    }

    /// Assert that nothing arrives within the receive window (e.g. no
    /// duplicate PUBLISH after a completed handshake).
    async fn expect_silence(&mut self) {
        match timeout(RECV_TIMEOUT, self.reader.next_packet()).await {
            Err(_elapsed) => {} // timed out: nothing arrived, as expected
            Ok(Ok(Some(pkt))) => panic!("expected no packet, got {pkt:?}"),
            Ok(Ok(None)) => panic!("connection closed unexpectedly"),
            Ok(Err(e)) => panic!("transport error: {e}"),
        }
    }

    /// Expect the broker to have closed the connection (clean EOF).
    async fn expect_closed(&mut self) {
        let pkt = timeout(RECV_TIMEOUT, self.reader.next_packet())
            .await
            .expect("timed out waiting for EOF")
            .expect("transport error");
        assert!(pkt.is_none(), "expected connection close, got {pkt:?}");
    }

    /// Subscribe to a single `filter` at `qos`; asserts the SUBACK matches the
    /// SUBSCRIBE and carries a granted-`QoS` code (not failure 0x80), and
    /// returns that return code for the caller to pin further.
    async fn subscribe(&mut self, pkid: u16, filter: &str, qos: QoS) -> u8 {
        self.send(&Packet::Subscribe(Subscribe {
            pkid,
            filters: vec![SubscribeFilter {
                path: filter.into(),
                qos,
            }],
        }))
        .await;
        match self.recv().await {
            Packet::SubAck(a) => {
                assert_eq!(a.pkid, pkid, "SUBACK pkid must match the SUBSCRIBE");
                assert_eq!(
                    a.return_codes.len(),
                    1,
                    "[MQTT-3.8.4-5] one return code per filter"
                );
                let code = a.return_codes[0];
                assert!(
                    code <= 0x02,
                    "SUBACK must grant a QoS (0x00..=0x02), got {code:#04x}"
                );
                code
            }
            other => panic!("expected SUBACK, got {other:?}"),
        }
    }

    /// Send a `QoS` 2 PUBLISH carrying `pkid` (the first half of the inbound
    /// exactly-once handshake; the broker must answer with PUBREC).
    async fn publish_qos2(&mut self, topic: &str, pkid: u16, payload: &'static [u8], dup: bool) {
        self.send(&Packet::Publish(Publish {
            dup,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: topic.into(),
            pkid: Some(pkid),
            payload: bytes::Bytes::from_static(payload),
        }))
        .await;
    }

    /// Run the complete publisher-side `QoS` 2 handshake for one message:
    /// PUBLISH -> PUBREC, PUBREL -> PUBCOMP [MQTT-4.3.3].
    async fn publish_qos2_complete(&mut self, topic: &str, pkid: u16, payload: &'static [u8]) {
        self.publish_qos2(topic, pkid, payload, false).await;
        assert_eq!(
            self.recv().await,
            Packet::PubRec(pkid),
            "QoS 2 PUBLISH must be answered with PUBREC"
        );
        self.pubrel(pkid).await;
        assert_eq!(
            self.recv().await,
            Packet::PubComp(pkid),
            "PUBREL must be answered with PUBCOMP"
        );
    }

    async fn puback(&mut self, pkid: u16) {
        self.send(&Packet::PubAck(pkid)).await;
    }

    async fn pubrec(&mut self, pkid: u16) {
        self.send(&Packet::PubRec(pkid)).await;
    }

    async fn pubrel(&mut self, pkid: u16) {
        self.send(&Packet::PubRel(pkid)).await;
    }

    async fn pubcomp(&mut self, pkid: u16) {
        self.send(&Packet::PubComp(pkid)).await;
    }
}

/// [MQTT-3.8.4-5]/[MQTT-3.8.4-6]: the SUBACK carries one return code per
/// filter, and a filter requesting `QoS` 2 is granted `QoS` 2 (return code 0x02).
#[tokio::test]
async fn suback_grants_requested_qos_up_to_2() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "grant-sub").await;
    sub.send(&Packet::Subscribe(Subscribe {
        pkid: 3,
        filters: vec![
            SubscribeFilter {
                path: "grant/q0".into(),
                qos: QoS::AtMostOnce,
            },
            SubscribeFilter {
                path: "grant/q1".into(),
                qos: QoS::AtLeastOnce,
            },
            SubscribeFilter {
                path: "grant/q2".into(),
                qos: QoS::ExactlyOnce,
            },
        ],
    }))
    .await;
    match sub.recv().await {
        Packet::SubAck(a) => {
            assert_eq!(a.pkid, 3);
            assert_eq!(
                a.return_codes,
                vec![0x00, 0x01, 0x02],
                "each filter must be granted its requested QoS, including 0x02"
            );
        }
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

/// [MQTT-4.3.3]: the full inbound `QoS` 2 handshake — PUBLISH(pkid) is answered
/// with PUBREC(pkid), PUBREL(pkid) with PUBCOMP(pkid) — and the message
/// reaches a subscriber exactly once.
#[tokio::test]
async fn inbound_qos2_handshake_delivers_exactly_once() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "inbound-sub").await;
    let granted = sub.subscribe(1, "exactly/once", QoS::AtMostOnce).await;
    assert_eq!(granted, 0x00);

    let mut pubr = Client::connect(addr, "inbound-pub").await;
    pubr.publish_qos2_complete("exactly/once", 10, b"only-one")
        .await;

    // Exactly one copy arrives, downgraded to the subscriber's QoS 0.
    let p = sub.recv_publish().await;
    assert_eq!(p.topic, "exactly/once");
    assert_eq!(&p.payload[..], b"only-one");
    assert_eq!(p.qos, QoS::AtMostOnce);
    assert_eq!(p.pkid, None);
    sub.expect_silence().await;
}

/// [MQTT-4.3.3-2]: a `QoS` 2 PUBLISH re-sent (DUP=1, same pkid) before the
/// publisher issues PUBREL must not be dispatched to subscribers a second
/// time, and the broker still answers the re-send with PUBREC and completes
/// the handshake normally.
#[tokio::test]
async fn resent_qos2_publish_before_pubrel_is_deduplicated() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "dedup-sub").await;
    sub.subscribe(1, "dedup/topic", QoS::ExactlyOnce).await;

    let mut pubr = Client::connect(addr, "dedup-pub").await;
    pubr.publish_qos2("dedup/topic", 11, b"no-dups", false)
        .await;
    assert_eq!(pubr.recv().await, Packet::PubRec(11));

    // Pretend the PUBREC was lost: re-send the same PUBLISH with DUP set.
    pubr.publish_qos2("dedup/topic", 11, b"no-dups", true).await;
    assert_eq!(
        pubr.recv().await,
        Packet::PubRec(11),
        "[MQTT-4.3.3] a re-sent PUBLISH must be acknowledged with PUBREC again"
    );

    pubr.pubrel(11).await;
    assert_eq!(pubr.recv().await, Packet::PubComp(11));

    // Exactly one copy reaches the subscriber. Be lenient about the delivery
    // QoS here (test 4 pins it); if it arrives at QoS 2, complete the
    // downstream handshake so nothing is owed.
    let p = sub.recv_publish().await;
    assert_eq!(p.topic, "dedup/topic");
    assert_eq!(&p.payload[..], b"no-dups");
    if p.qos == QoS::ExactlyOnce {
        let id = p
            .pkid
            .expect("QoS 2 PUBLISH must carry a packet identifier");
        sub.pubrec(id).await;
        assert_eq!(sub.recv().await, Packet::PubRel(id));
        sub.pubcomp(id).await;
    }
    sub.expect_silence().await;
}

/// Downstream four-way handshake: a `QoS` 2 subscriber receives the message as
/// a `QoS` 2 PUBLISH with a packet identifier; its PUBREC is answered with
/// PUBREL, it completes with PUBCOMP, and no duplicate PUBLISH follows.
#[tokio::test]
async fn downstream_qos2_uses_four_way_handshake() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "down-q2-sub").await;
    sub.subscribe(1, "down/q2", QoS::ExactlyOnce).await;

    let mut pubr = Client::connect(addr, "down-q2-pub").await;
    pubr.publish_qos2_complete("down/q2", 31, b"four-way").await;

    let p = sub.recv_publish().await;
    assert_eq!(p.topic, "down/q2");
    assert_eq!(&p.payload[..], b"four-way");
    assert_eq!(
        p.qos,
        QoS::ExactlyOnce,
        "a QoS 2 subscriber must receive a QoS 2 publish at QoS 2"
    );
    assert!(!p.dup, "first transmission must not have DUP set");
    let id = p
        .pkid
        .expect("QoS 2 PUBLISH must carry a packet identifier");

    sub.pubrec(id).await;
    assert_eq!(
        sub.recv().await,
        Packet::PubRel(id),
        "the broker must answer the subscriber's PUBREC with PUBREL"
    );
    sub.pubcomp(id).await;

    // The handshake is complete: nothing further may be (re)delivered.
    sub.expect_silence().await;
}

/// [MQTT-3.8.4-6]: delivery `QoS` is the minimum of the publish `QoS` and the
/// granted subscription `QoS`. A `QoS` 1 subscriber gets a `QoS` 2 publish at `QoS` 1
/// (with a packet id, needing only PUBACK); a `QoS` 0 subscriber gets `QoS` 0
/// with no packet id.
#[tokio::test]
async fn qos2_publish_is_downgraded_to_subscription_qos() {
    let addr = start_broker().await;

    let mut sub1 = Client::connect(addr, "down-q1-sub").await;
    sub1.subscribe(1, "down/grade", QoS::AtLeastOnce).await;
    let mut sub0 = Client::connect(addr, "down-q0-sub").await;
    sub0.subscribe(1, "down/grade", QoS::AtMostOnce).await;

    let mut pubr = Client::connect(addr, "downgrade-pub").await;
    pubr.publish_qos2_complete("down/grade", 41, b"downgraded")
        .await;

    let p1 = sub1.recv_publish().await;
    assert_eq!(p1.topic, "down/grade");
    assert_eq!(&p1.payload[..], b"downgraded");
    assert_eq!(
        p1.qos,
        QoS::AtLeastOnce,
        "a QoS 1 subscriber must receive a QoS 2 publish at QoS 1"
    );
    assert!(!p1.dup);
    let id = p1
        .pkid
        .expect("QoS 1 PUBLISH must carry a packet identifier");
    sub1.puback(id).await;
    sub1.expect_silence().await;

    let p0 = sub0.recv_publish().await;
    assert_eq!(p0.topic, "down/grade");
    assert_eq!(&p0.payload[..], b"downgraded");
    assert_eq!(
        p0.qos,
        QoS::AtMostOnce,
        "a QoS 0 subscriber must receive a QoS 2 publish at QoS 0"
    );
    assert_eq!(p0.pkid, None, "QoS 0 PUBLISH must not carry a packet id");
}

/// Downstream redelivery: a persistent `QoS` 2 subscriber that never sends
/// PUBREC gets the PUBLISH again with DUP=1 on session resume; once the full
/// PUBREC/PUBREL/PUBCOMP handshake completes, the message is never re-sent.
/// (Packet identifiers may differ across reconnects; dup/qos/topic/payload
/// are pinned exactly.)
#[tokio::test]
async fn unacked_downstream_qos2_is_resent_with_dup_until_pubcomp() {
    let addr = start_broker().await;

    // 1. Fresh persistent session subscribing at QoS 2.
    let (mut sub, present) = Client::connect_opts(addr, "durable-q2", false).await;
    assert!(!present, "no session should exist yet");
    sub.subscribe(1, "redelivery/topic", QoS::ExactlyOnce).await;

    // 2. Publish QoS 2; the completed inbound handshake guarantees the broker
    //    owns the message before we misbehave on the subscriber side.
    let mut pubr = Client::connect(addr, "redelivery-pub").await;
    pubr.publish_qos2_complete("redelivery/topic", 21, b"once-only")
        .await;

    // 3. First delivery arrives at QoS 2; withhold PUBREC and drop the socket.
    let first = sub.recv_publish().await;
    assert_eq!(first.topic, "redelivery/topic");
    assert_eq!(&first.payload[..], b"once-only");
    assert_eq!(first.qos, QoS::ExactlyOnce);
    assert!(!first.dup, "first transmission must not have DUP set");
    assert!(first.pkid.is_some());
    drop(sub);

    // 4. Resume the session: the unacknowledged PUBLISH must be re-sent with
    //    DUP=1 (pkid may differ from the first attempt).
    let (mut sub, present) = Client::connect_opts(addr, "durable-q2", false).await;
    assert!(present, "session should now be present");
    let second = sub.recv_publish().await;
    assert_eq!(second.topic, "redelivery/topic");
    assert_eq!(&second.payload[..], b"once-only");
    assert_eq!(second.qos, QoS::ExactlyOnce);
    assert!(
        second.dup,
        "redelivery of an unacknowledged PUBLISH must set DUP"
    );
    let id = second
        .pkid
        .expect("QoS 2 PUBLISH must carry a packet identifier");

    // 5. Complete the downstream handshake this time.
    sub.pubrec(id).await;
    assert_eq!(sub.recv().await, Packet::PubRel(id));
    sub.pubcomp(id).await;

    // 6. Disconnect cleanly and resume once more: the handshake completed, so
    //    nothing may be redelivered.
    sub.send(&Packet::Disconnect).await;
    sub.expect_closed().await;
    let (mut sub, present) = Client::connect_opts(addr, "durable-q2", false).await;
    assert!(present, "session should still be present");
    sub.expect_silence().await;
}
