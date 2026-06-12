//! SWIM-driven cluster test: two nodes are given **no static peer list** — they
//! discover each other via SWIM gossip, the membership layer establishes the
//! peer link, and a publish on one node reaches a subscriber on the other.

use std::net::SocketAddr;
use std::time::Duration;

use mqtt_cluster::swim::{Config as SwimConfig, Swim};
use mqtt_cluster::swim_auth::{SwimAuth, KEY_LEN};
use mqtt_cluster::{swim_driver, NodeId};
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, ProtocolVersion, QoS,
};
use mqtt_storage::MemorySessionStore;
use mqttd::Hub;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;

/// Tight SWIM timings so discovery converges in well under a second.
fn swim_cfg() -> SwimConfig {
    SwimConfig {
        protocol_period_ms: 150,
        ack_timeout_ms: 60,
        suspicion_timeout_ms: 500,
        indirect_probes: 2,
        gossip_fanout: 8,
        gossip_multiplier: 4,
    }
}

/// Start one broker node with SWIM membership; returns its client and SWIM addrs.
async fn start_node(id: &str, swim_seeds: Vec<String>) -> (SocketAddr, String) {
    let node_id = NodeId(id.to_string());
    let (hub, hub_tx) = Hub::with_config(node_id.clone(), Box::new(MemorySessionStore::new()));
    tokio::spawn(hub.run());

    // Client listener.
    let cli = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cli_addr = cli.local_addr().unwrap();
    let conn_tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = cli.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, conn_tx.clone()));
        }
    });

    // Peer-link listener; its address is what SWIM gossips as our routing address.
    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer_listener.local_addr().unwrap().to_string();
    tokio::spawn(mqttd::peer::serve_listener(
        peer_listener,
        node_id.clone(),
        hub_tx.clone(),
        None,
    ));

    // SWIM membership driving the peer mesh. No static dialing anywhere.
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let swim_addr = socket.local_addr().unwrap().to_string();
    let swim = Swim::new(
        node_id.clone(),
        swim_addr.clone(),
        peer_addr,
        swim_cfg(),
        swim_seeds,
    );
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    // The cluster-shared gossip key (ADR 0003): this test exercises the
    // authenticated path end-to-end.
    let auth = SwimAuth::new(&[0x5A; KEY_LEN]);
    tokio::spawn(swim_driver::run(
        socket,
        swim,
        Duration::from_millis(20),
        event_tx,
        Some(auth),
    ));
    tokio::spawn(mqttd::cluster::maintain_peer_links(
        event_rx, node_id, hub_tx, None,
    ));

    (cli_addr, swim_addr)
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
        c.writer
            .send(&Packet::Connect(Connect {
                protocol: V4,
                clean_session: true,
                keep_alive: 30,
                client_id: id.to_string(),
                last_will: None,
                username: None,
                password: None,
            }))
            .await
            .unwrap();
        assert!(matches!(c.recv().await, Some(Packet::ConnAck(_))));
        c
    }

    async fn subscribe(&mut self, filter: &str) {
        self.writer
            .send(&Packet::Subscribe(Subscribe {
                pkid: 1,
                filters: vec![SubscribeFilter {
                    path: filter.to_string(),
                    qos: QoS::AtMostOnce,
                }],
            }))
            .await
            .unwrap();
        assert!(matches!(self.recv().await, Some(Packet::SubAck(_))));
    }

    async fn publish(&mut self, topic: &str, payload: &'static [u8]) {
        self.writer
            .send(&Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: topic.to_string(),
                pkid: None,
                payload: bytes::Bytes::from_static(payload),
            }))
            .await
            .unwrap();
    }

    /// Receive the next packet within a short window, or `None` on timeout/close.
    async fn recv(&mut self) -> Option<Packet> {
        timeout(Duration::from_millis(300), self.reader.next_packet())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
    }
}

/// Publish on `from` until `sub` receives `topic`, retrying while SWIM discovery
/// and interest propagation converge in the background.
async fn assert_routes(sub: &mut Client, from: &mut Client, topic: &str, payload: &'static [u8]) {
    for attempt in 0..50 {
        from.publish(topic, payload).await;
        if let Some(Packet::Publish(p)) = sub.recv().await {
            assert_eq!(p.topic, topic);
            assert_eq!(&p.payload[..], payload);
            return;
        }
        assert!(attempt < 49, "message never arrived across the cluster");
    }
}

#[tokio::test]
async fn swim_discovery_establishes_routing_both_ways() {
    // Node A is the seed; node B joins through it. Ids are ordered so A owns
    // the link (A dials), exercising the dial side and the accept side.
    let (cli_a, swim_a) = start_node("swim-node-a", vec![]).await;
    let (cli_b, _swim_b) = start_node("swim-node-b", vec![swim_a]).await;

    // A subscriber on A receives a publish originating on B...
    let mut sub_a = Client::connect(cli_a, "sub-a").await;
    sub_a.subscribe("swim/+/data").await;
    let mut pub_b = Client::connect(cli_b, "pub-b").await;
    assert_routes(&mut sub_a, &mut pub_b, "swim/zone1/data", b"b-to-a").await;

    // ...and the same link carries interest and messages the other way.
    let mut sub_b = Client::connect(cli_b, "sub-b").await;
    sub_b.subscribe("swim/back/#").await;
    let mut pub_a = Client::connect(cli_a, "pub-a").await;
    assert_routes(&mut sub_b, &mut pub_a, "swim/back/data", b"a-to-b").await;
}
