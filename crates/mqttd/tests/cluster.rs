//! Two-node cluster test: a message published to one node is delivered to a
//! subscriber connected to the *other* node, over a real peer link.

use std::net::SocketAddr;
use std::time::Duration;

use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, Property, ProtocolVersion, QoS,
};
use mqtt_storage::MemorySessionStore;
use mqttd::Hub;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const V4: ProtocolVersion = ProtocolVersion::V311;

/// Bring up a two-node cluster on ephemeral ports; return each node's client addr.
async fn start_two_node_cluster() -> (SocketAddr, SocketAddr) {
    // Bind peer + client listeners first so addresses are known before dialing.
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
    let (hub_a, tx_a) =
        Hub::with_config(id_a.clone(), std::sync::Arc::new(MemorySessionStore::new()));
    let (hub_b, tx_b) =
        Hub::with_config(id_b.clone(), std::sync::Arc::new(MemorySessionStore::new()));
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

    // Full mesh: each node dials the other.
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
                properties: mqtt_codec::Properties::new(),
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
                properties: mqtt_codec::Properties::new(),
                pkid: 1,
                filters: vec![SubscribeFilter {
                    options: mqtt_codec::SubscriptionOptions::default(),
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
                properties: mqtt_codec::Properties::new(),
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

    /// Connect as an MQTT 5 client (so User Properties traverse the wire).
    async fn connect_v5(addr: SocketAddr, id: &str) -> Self {
        let (rh, wh) = TcpStream::connect(addr).await.unwrap().into_split();
        let mut c = Client {
            reader: mqtt_net::FrameReader::new(rh, ProtocolVersion::V5),
            writer: mqtt_net::FrameWriter::new(wh, ProtocolVersion::V5),
        };
        c.writer
            .send(&Packet::Connect(Connect {
                properties: mqtt_codec::Properties::new(),
                protocol: ProtocolVersion::V5,
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

    /// Publish (`QoS` 0) with MQTT 5 User Properties.
    async fn publish_with_props(
        &mut self,
        topic: &str,
        payload: &'static [u8],
        props: &[(&str, &str)],
    ) {
        let properties = mqtt_codec::Properties(
            props
                .iter()
                .map(|(k, v)| Property::UserProperty((*k).to_string(), (*v).to_string()))
                .collect(),
        );
        self.writer
            .send(&Packet::Publish(Publish {
                properties,
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
}

#[tokio::test]
async fn publish_on_one_node_reaches_subscriber_on_another() {
    let (addr_a, addr_b) = start_two_node_cluster().await;

    let mut sub = Client::connect(addr_a, "sub").await;
    sub.subscribe("cluster/+/data").await;

    let mut pubr = Client::connect(addr_b, "pub").await;

    // Retry until A's interest has propagated to B and the message routes back.
    // Each attempt waits up to 300ms; ~50 attempts gives a generous ceiling.
    for attempt in 0..50 {
        pubr.publish("cluster/zone1/data", b"cross-node").await;
        if let Some(Packet::Publish(p)) = sub.recv().await {
            assert_eq!(p.topic, "cluster/zone1/data");
            assert_eq!(&p.payload[..], b"cross-node");
            return;
        }
        assert!(attempt < 49, "message never arrived across the cluster");
    }
}

/// ADR 0030-T3: a publisher's User Properties are forwarded unaltered to a subscriber on
/// **another** node (MQTT-3.3.2-17), so they survive the peer-link hop too.
#[tokio::test]
async fn user_properties_survive_cross_node_delivery() {
    let (addr_a, addr_b) = start_two_node_cluster().await;

    let mut sub = Client::connect_v5(addr_a, "sub-v5").await;
    sub.subscribe("cluster/+/data").await;

    let mut pubr = Client::connect_v5(addr_b, "pub-v5").await;

    for attempt in 0..50 {
        pubr.publish_with_props(
            "cluster/zone1/data",
            b"cross-node",
            &[("fss-bridge-hop-count", "2"), ("trace", "abc")],
        )
        .await;
        if let Some(Packet::Publish(p)) = sub.recv().await {
            assert_eq!(&p.payload[..], b"cross-node");
            let props: Vec<(String, String)> = p
                .properties
                .0
                .iter()
                .filter_map(|prop| match prop {
                    Property::UserProperty(k, v) => Some((k.clone(), v.clone())),
                    _ => None,
                })
                .collect();
            assert_eq!(
                props,
                vec![
                    ("fss-bridge-hop-count".to_string(), "2".to_string()),
                    ("trace".to_string(), "abc".to_string()),
                ],
                "user properties must cross the peer link unaltered and in order"
            );
            return;
        }
        assert!(attempt < 49, "message never arrived across the cluster");
    }
}

#[tokio::test]
async fn non_matching_topic_is_not_forwarded_across_nodes() {
    let (addr_a, addr_b) = start_two_node_cluster().await;

    let mut sub = Client::connect(addr_a, "sub2").await;
    sub.subscribe("only/this").await;

    // Give interest time to propagate, then publish a non-matching topic on B.
    let mut pubr = Client::connect(addr_b, "pub2").await;
    for _ in 0..5 {
        pubr.publish("something/else", b"nope").await;
    }
    // The subscriber must receive nothing for the non-matching topic.
    assert!(
        sub.recv().await.is_none(),
        "non-matching topic should not cross nodes"
    );
}
