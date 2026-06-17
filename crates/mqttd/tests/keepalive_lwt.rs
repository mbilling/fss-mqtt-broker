//! Integration tests for keepalive enforcement and Last Will and Testament
//! (LWT), written test-first: they define the MQTT 3.1.1 behavior the broker
//! must implement.
//!
//! Keepalive [MQTT-3.1.2-24]: if the server does not receive any packet from a
//! client within 1.5x the negotiated keepalive interval, it MUST close the
//! connection. A keepalive of 0 disables the mechanism entirely.
//!
//! LWT [MQTT-3.1.2-8]: the will message carried in CONNECT MUST be published
//! when the connection ends for any reason other than the client sending
//! DISCONNECT first [MQTT-3.14.4-3] — abrupt socket loss, keepalive timeout,
//! and server-side disconnection (e.g. session takeover) all trigger it.

use std::time::Duration;

use mqtt_codec::{
    packet::{Connect, LastWill, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqttd::hub::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout, Instant};

const V4: ProtocolVersion = ProtocolVersion::V311;

/// Default window for receiving a packet the broker should send promptly.
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

/// A minimal MQTT client built on the project's framing + codec.
struct Client {
    reader: mqtt_net::FrameReader<tokio::net::tcp::OwnedReadHalf>,
    writer: mqtt_net::FrameWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl Client {
    /// Connect with an explicit keepalive (seconds) and optional will, clean
    /// session, and expect a successful CONNACK.
    async fn connect_with(
        addr: std::net::SocketAddr,
        client_id: &str,
        keep_alive: u16,
        last_will: Option<LastWill>,
    ) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (rh, wh) = stream.into_split();
        let mut client = Client {
            reader: mqtt_net::FrameReader::new(rh, V4),
            writer: mqtt_net::FrameWriter::new(wh, V4),
        };
        client
            .send(&Packet::Connect(Connect {
                properties: mqtt_codec::Properties::new(),
                protocol: V4,
                clean_session: true,
                keep_alive,
                client_id: client_id.to_string(),
                last_will,
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

    /// Subscribe to one filter at `QoS` 0 and wait for the SUBACK.
    async fn subscribe(&mut self, filter: &str) {
        self.send(&Packet::Subscribe(Subscribe {
            properties: mqtt_codec::Properties::new(),
            pkid: 1,
            filters: vec![SubscribeFilter {
                options: mqtt_codec::SubscriptionOptions::default(),
                path: filter.into(),
                qos: QoS::AtMostOnce,
            }],
        }))
        .await;
        match self.recv().await {
            Packet::SubAck(_) => {}
            other => panic!("expected SUBACK, got {other:?}"),
        }
    }

    /// Next packet within the default prompt-response window.
    async fn recv(&mut self) -> Packet {
        self.recv_within(RECV_TIMEOUT).await
    }

    /// Next packet within an explicit window (for keepalive-scale waits).
    async fn recv_within(&mut self, dur: Duration) -> Packet {
        timeout(dur, self.reader.next_packet())
            .await
            .expect("timed out waiting for a packet")
            .expect("transport error")
            .expect("connection closed unexpectedly")
    }

    /// Expect the broker to close the connection (EOF or reset) within `dur`.
    /// Times out — and fails — instead of hanging if the broker keeps the
    /// connection open forever.
    async fn expect_closed_within(&mut self, dur: Duration) {
        let Ok(received) = timeout(dur, self.reader.next_packet()).await else {
            panic!("connection still open after {dur:?}; broker never closed it");
        };
        match received {
            // Clean EOF or a transport-level reset both prove the server
            // dropped the connection.
            Ok(None) | Err(_) => {}
            Ok(Some(pkt)) => panic!("expected connection close, got {pkt:?}"),
        }
    }
}

/// [MQTT-3.1.2-24] A client that negotiates `keep_alive=1` and then sends
/// nothing must be disconnected by the server once 1.5x the interval (1.5s)
/// elapses. We allow up to 4s (1.5s grace + generous scheduling slack), but
/// the connection must NOT still be open at that point.
#[tokio::test]
async fn idle_client_is_disconnected_after_keepalive_grace() {
    let addr = start_broker().await;
    let mut client = Client::connect_with(addr, "ka-idle", 1, None).await;

    // Send nothing. The server owes us a close at ~1.5s; 4s is the hard limit.
    client.expect_closed_within(Duration::from_secs(4)).await;
}

/// [MQTT-3.1.2-24] only fires when the server receives *nothing*. A client
/// with `keep_alive=1` that sends PINGREQ every ~500ms (well inside the 1.5s
/// grace window) must stay connected past 3s and get a PINGRESP each time.
#[tokio::test]
async fn pinging_client_stays_connected_past_keepalive() {
    let addr = start_broker().await;
    let mut client = Client::connect_with(addr, "ka-ping", 1, None).await;

    // 7 pings x 500ms spacing = 3.5s of activity, each ping resetting the
    // server's 1.5s idle deadline.
    let start = Instant::now();
    for _ in 0..7 {
        client.send(&Packet::PingReq).await;
        assert_eq!(client.recv().await, Packet::PingResp);
        sleep(Duration::from_millis(500)).await;
    }
    assert!(
        start.elapsed() >= Duration::from_secs(3),
        "test must cover more than 3s of connection lifetime"
    );

    // Still alive after outliving the keepalive interval several times over.
    client.send(&Packet::PingReq).await;
    assert_eq!(client.recv().await, Packet::PingResp);
}

/// A keepalive of 0 turns the mechanism off (MQTT 3.1.2.10): the server must
/// never disconnect such a client for idleness.
#[tokio::test]
async fn zero_keepalive_is_never_idle_disconnected() {
    let addr = start_broker().await;
    let mut client = Client::connect_with(addr, "ka-zero", 0, None).await;

    // Idle well past what a small keepalive's 1.5x grace would allow, then
    // prove the connection is still alive with a ping round-trip.
    sleep(Duration::from_millis(2500)).await;
    client.send(&Packet::PingReq).await;
    assert_eq!(client.recv().await, Packet::PingResp);
}

/// The will for the LWT tests: topic "will/t", payload "gone", `QoS` 0.
fn will(topic: &str) -> LastWill {
    LastWill {
        properties: mqtt_codec::Properties::new(),
        topic: topic.into(),
        payload: bytes::Bytes::from_static(b"gone"),
        qos: QoS::AtMostOnce,
        retain: false,
    }
}

/// [MQTT-3.1.2-8] An abrupt connection loss (socket dropped without a
/// DISCONNECT packet) must cause the server to publish the client's will to
/// matching subscribers.
#[tokio::test]
async fn abrupt_drop_publishes_will() {
    let addr = start_broker().await;

    let mut sub = Client::connect_with(addr, "lwt-sub-drop", 30, None).await;
    sub.subscribe("will/t").await;

    let client_a = Client::connect_with(addr, "lwt-a-drop", 30, Some(will("will/t"))).await;
    // Drop the socket without sending DISCONNECT: the server sees EOF with no
    // graceful goodbye, which must trigger the will.
    drop(client_a);

    // The will is published as soon as the server notices the dead socket;
    // 2s is generous for a localhost FIN.
    match sub.recv_within(Duration::from_secs(2)).await {
        Packet::Publish(p) => {
            assert_eq!(p.topic, "will/t");
            assert_eq!(&p.payload[..], b"gone");
        }
        other => panic!("expected will PUBLISH, got {other:?}"),
    }
}

/// [MQTT-3.14.4-3] A graceful DISCONNECT discards the will: the subscriber
/// must receive nothing.
#[tokio::test]
async fn graceful_disconnect_discards_will() {
    let addr = start_broker().await;

    let mut sub = Client::connect_with(addr, "lwt-sub-graceful", 30, None).await;
    sub.subscribe("will/t").await;

    let mut client_a = Client::connect_with(addr, "lwt-a-graceful", 30, Some(will("will/t"))).await;
    client_a
        .send(&Packet::Disconnect(mqtt_codec::Disconnect::default()))
        .await;
    // Wait for the broker to actually tear down A's connection so any
    // (erroneous) will publication would have happened before we check.
    client_a.expect_closed_within(Duration::from_secs(2)).await;
    drop(client_a);
    sleep(Duration::from_millis(500)).await;

    // A ping round-trip flushes the subscriber's ordered stream: if a will had
    // been (wrongly) published, it would arrive before the PINGRESP.
    sub.send(&Packet::PingReq).await;
    assert_eq!(
        sub.recv().await,
        Packet::PingResp,
        "no will may be delivered after a graceful DISCONNECT"
    );
}

/// [MQTT-3.1.2-8] + [MQTT-3.1.2-24] A keepalive timeout is an ungraceful end:
/// when the server times out a silent client, it must publish that client's
/// will.
#[tokio::test]
async fn keepalive_timeout_publishes_will() {
    let addr = start_broker().await;

    let mut sub = Client::connect_with(addr, "lwt-sub-timeout", 30, None).await;
    sub.subscribe("will/timeout").await;

    // A negotiates keep_alive=1 with a will and then goes silent. The server
    // must time it out at ~1.5s and publish the will; 4s allows ample slack.
    let _client_a =
        Client::connect_with(addr, "lwt-a-timeout", 1, Some(will("will/timeout"))).await;

    match sub.recv_within(Duration::from_secs(4)).await {
        Packet::Publish(p) => {
            assert_eq!(p.topic, "will/timeout");
            assert_eq!(&p.payload[..], b"gone");
        }
        other => panic!("expected will PUBLISH on keepalive timeout, got {other:?}"),
    }
}

/// [MQTT-3.1.2-8] Session takeover: when a second connection with the same
/// client id displaces the first, the server is the one ending the old
/// connection — the old client never sent DISCONNECT — so the old connection's
/// will must be published.
#[tokio::test]
async fn session_takeover_publishes_will() {
    let addr = start_broker().await;

    let mut sub = Client::connect_with(addr, "lwt-sub-takeover", 30, None).await;
    sub.subscribe("will/takeover").await;

    let _victim = Client::connect_with(addr, "lwt-takeover", 30, Some(will("will/takeover"))).await;
    // Same client id: the broker disconnects the first connection.
    let _usurper = Client::connect_with(addr, "lwt-takeover", 30, None).await;

    match sub.recv_within(Duration::from_secs(2)).await {
        Packet::Publish(p) => {
            assert_eq!(p.topic, "will/takeover");
            assert_eq!(&p.payload[..], b"gone");
        }
        other => panic!("expected will PUBLISH on takeover, got {other:?}"),
    }
}
