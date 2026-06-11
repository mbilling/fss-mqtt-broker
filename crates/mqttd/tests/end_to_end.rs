//! End-to-end test: a publisher and a subscriber talk to a running broker over
//! real TCP sockets, using the project's own codec as the client.
//!
//! This exercises the full path: accept loop -> CONNECT/CONNACK -> SUBSCRIBE/
//! SUBACK -> PUBLISH routing -> delivery to the matching subscriber.

use std::time::Duration;

use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqttd::hub::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;

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
        let stream = TcpStream::connect(addr).await.unwrap();
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
        self.writer.send(packet).await.unwrap();
    }

    /// Expect the broker to have closed the connection (clean EOF).
    async fn expect_closed(&mut self) {
        let pkt = timeout(Duration::from_secs(2), self.reader.next_packet())
            .await
            .expect("timed out waiting for EOF")
            .expect("transport error");
        assert!(pkt.is_none(), "expected connection close, got {pkt:?}");
    }

    async fn recv(&mut self) -> Packet {
        timeout(Duration::from_secs(2), self.reader.next_packet())
            .await
            .expect("timed out waiting for a packet")
            .expect("transport error")
            .expect("connection closed unexpectedly")
    }
}

#[tokio::test]
async fn publish_reaches_matching_subscriber() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "subscriber").await;
    sub.send(&Packet::Subscribe(Subscribe {
        pkid: 1,
        filters: vec![SubscribeFilter {
            path: "sensors/+/temp".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await;
    match sub.recv().await {
        Packet::SubAck(a) => {
            assert_eq!(a.pkid, 1);
            assert_eq!(a.return_codes, vec![0x00]);
        }
        other => panic!("expected SUBACK, got {other:?}"),
    }

    // Publish from a second client to a topic that matches the subscription.
    let mut pubr = Client::connect(addr, "publisher").await;
    pubr.send(&Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "sensors/kitchen/temp".into(),
        pkid: None,
        payload: bytes::Bytes::from_static(b"21.5C"),
    }))
    .await;

    // The subscriber should receive the forwarded PUBLISH.
    match sub.recv().await {
        Packet::Publish(p) => {
            assert_eq!(p.topic, "sensors/kitchen/temp");
            assert_eq!(&p.payload[..], b"21.5C");
            assert_eq!(p.qos, QoS::AtMostOnce);
        }
        other => panic!("expected PUBLISH, got {other:?}"),
    }
}

#[tokio::test]
async fn non_matching_topic_is_not_delivered() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "sub2").await;
    sub.send(&Packet::Subscribe(Subscribe {
        pkid: 7,
        filters: vec![SubscribeFilter {
            path: "a/b".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await;
    assert!(matches!(sub.recv().await, Packet::SubAck(_)));

    let mut pubr = Client::connect(addr, "pub2").await;
    pubr.send(&Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a/c".into(), // does not match "a/b"
        pkid: None,
        payload: bytes::Bytes::from_static(b"x"),
    }))
    .await;

    // A PINGREQ/PINGRESP round-trip proves the subscriber's socket is live, and
    // that no stray PUBLISH arrived before the PINGRESP.
    sub.send(&Packet::PingReq).await;
    assert_eq!(sub.recv().await, Packet::PingResp);
}

#[tokio::test]
async fn qos1_publish_is_acked_and_downgraded() {
    let addr = start_broker().await;

    let mut sub = Client::connect(addr, "sub3").await;
    sub.send(&Packet::Subscribe(Subscribe {
        pkid: 1,
        filters: vec![SubscribeFilter {
            path: "q".into(),
            qos: QoS::ExactlyOnce,
        }],
    }))
    .await;
    assert!(matches!(sub.recv().await, Packet::SubAck(_)));

    let mut pubr = Client::connect(addr, "pub3").await;
    pubr.send(&Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "q".into(),
        pkid: Some(99),
        payload: bytes::Bytes::from_static(b"hi"),
    }))
    .await;
    // Publisher gets PUBACK for its QoS 1 message.
    assert_eq!(pubr.recv().await, Packet::PubAck(99));

    // Subscriber receives it downgraded to QoS 0.
    match sub.recv().await {
        Packet::Publish(p) => {
            assert_eq!(p.qos, QoS::AtMostOnce);
            assert_eq!(p.pkid, None);
            assert_eq!(&p.payload[..], b"hi");
        }
        other => panic!("expected PUBLISH, got {other:?}"),
    }
}

#[tokio::test]
async fn persistent_session_queues_offline_and_replays_on_reconnect() {
    let addr = start_broker().await;

    // 1. Persistent subscriber connects (fresh) and subscribes.
    let (mut sub, present) = Client::connect_opts(addr, "durable", false).await;
    assert!(!present, "no session should exist yet");
    sub.send(&Packet::Subscribe(Subscribe {
        pkid: 1,
        filters: vec![SubscribeFilter {
            path: "offline/topic".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await;
    assert!(matches!(sub.recv().await, Packet::SubAck(_)));

    // 2. Subscriber disconnects cleanly. Waiting for the broker to close the
    //    socket guarantees the Detach is enqueued before we publish.
    sub.send(&Packet::Disconnect).await;
    sub.expect_closed().await;

    // 3. Publish while the subscriber is offline. QoS 1 + waiting for PUBACK
    //    guarantees the broker has received (and thus enqueued) the message
    //    before we reconnect.
    let mut pubr = Client::connect(addr, "pub-offline").await;
    pubr.send(&Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "offline/topic".into(),
        pkid: Some(1),
        payload: bytes::Bytes::from_static(b"queued-while-away"),
    }))
    .await;
    assert_eq!(pubr.recv().await, Packet::PubAck(1));

    // 4. Reconnect with the same id and clean_session=false: the session must be
    //    present and the queued message replayed.
    let (mut sub, present) = Client::connect_opts(addr, "durable", false).await;
    assert!(present, "session should now be present");
    match sub.recv().await {
        Packet::Publish(p) => {
            assert_eq!(p.topic, "offline/topic");
            assert_eq!(&p.payload[..], b"queued-while-away");
        }
        other => panic!("expected replayed PUBLISH, got {other:?}"),
    }
}

#[tokio::test]
async fn clean_session_does_not_persist() {
    let addr = start_broker().await;

    // Connect clean, subscribe, disconnect.
    let (mut sub, present) = Client::connect_opts(addr, "ephemeral", true).await;
    assert!(!present);
    sub.send(&Packet::Subscribe(Subscribe {
        pkid: 1,
        filters: vec![SubscribeFilter {
            path: "x".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await;
    assert!(matches!(sub.recv().await, Packet::SubAck(_)));
    sub.send(&Packet::Disconnect).await;
    sub.expect_closed().await;

    // Publish while gone — must NOT be queued for a clean session.
    let mut pubr = Client::connect(addr, "pub-clean").await;
    pubr.send(&Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "x".into(),
        pkid: Some(1),
        payload: bytes::Bytes::from_static(b"dropped"),
    }))
    .await;
    assert_eq!(pubr.recv().await, Packet::PubAck(1));

    // Reconnect clean: no session, and a PINGREQ round-trip shows nothing queued.
    let (mut sub, present) = Client::connect_opts(addr, "ephemeral", true).await;
    assert!(!present, "clean session must not persist");
    sub.send(&Packet::PingReq).await;
    assert_eq!(sub.recv().await, Packet::PingResp);
}
