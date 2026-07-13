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
        None,
    ));
    tokio::spawn(mqttd::peer::serve_listener(
        peer_b,
        id_b.clone(),
        tx_b.clone(),
        None,
        None,
        None,
    ));

    // Full mesh: each node dials the other.
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_b.to_string(),
        id_a,
        tx_a,
        None,
        None,
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_a.to_string(),
        id_b,
        tx_b,
        None,
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

    /// Publish a retained message (`QoS` 0).
    async fn publish_retained(&mut self, topic: &str, payload: &[u8]) {
        self.writer
            .send(&Packet::Publish(Publish {
                properties: mqtt_codec::Properties::new(),
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic: topic.to_string(),
                pkid: None,
                payload: bytes::Bytes::copy_from_slice(payload),
            }))
            .await
            .unwrap();
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

    /// Publish a retained message (`QoS` 0) carrying the given MQTT 5 properties.
    async fn publish_retained_v5(
        &mut self,
        topic: &str,
        payload: &[u8],
        properties: mqtt_codec::Properties,
    ) {
        self.writer
            .send(&Packet::Publish(Publish {
                properties,
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic: topic.to_string(),
                pkid: None,
                payload: bytes::Bytes::copy_from_slice(payload),
            }))
            .await
            .unwrap();
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

/// One of two durable-retained nodes (`name` + `other`) sharing the same
/// two-member ring view.
async fn build_durable_retained_node(
    name: &str,
    other: &str,
) -> (
    NodeId,
    SocketAddr, // client addr
    SocketAddr, // peer addr
    tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>,
    std::sync::Arc<std::sync::RwLock<mqtt_cluster::placement::Placement>>,
) {
    use mqtt_cluster::placement::{Placement, DEFAULT_REPLICAS};
    use mqtt_cluster::swim::MemberState;
    use mqtt_storage::repl::InMemoryReplicatedLog;
    use mqtt_storage::retained_log::ReplicatedRetained;
    use std::sync::{Arc, RwLock};

    let id = NodeId(name.to_string());
    let mut p = Placement::new(id.clone(), DEFAULT_REPLICAS);
    p.observe(&NodeId(other.to_string()), MemberState::Alive, "x:1", None);
    let placement = Arc::new(RwLock::new(p));
    let (mut hub, tx) = Hub::with_config_and_placement(
        id.clone(),
        Arc::new(MemorySessionStore::new()),
        Some(placement.clone()),
    );
    hub.attach_durable_retained(Arc::new(ReplicatedRetained::new(
        InMemoryReplicatedLog::new(),
    )));
    tokio::spawn(hub.run());

    let cli = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = cli.local_addr().unwrap();
    spawn_client_loop(cli, tx.clone());
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer.local_addr().unwrap();
    tokio::spawn(mqttd::peer::serve_listener(
        peer,
        id.clone(),
        tx.clone(),
        None,
        None,
        None,
    ));
    (id, client_addr, peer_addr, tx, placement)
}

/// The retained value a fresh subscriber replays for `topic` on the node at `addr`,
/// or `None` if that node's cache holds nothing. `id` must be unique per call.
async fn retained_on(addr: SocketAddr, id: &str, topic: &str) -> Option<Vec<u8>> {
    let mut c = Client::connect(addr, id).await;
    c.subscribe(topic).await;
    match c.recv().await {
        Some(Packet::Publish(p)) => Some(p.payload.to_vec()),
        _ => None,
    }
}

/// The retained (payload, properties) a fresh **MQTT 5** subscriber replays for
/// `topic` on the node at `addr`, or `None` if that node's cache holds nothing.
/// `id` must be unique per call.
async fn retained_v5_on(
    addr: SocketAddr,
    id: &str,
    topic: &str,
) -> Option<(Vec<u8>, Vec<Property>)> {
    let mut c = Client::connect_v5(addr, id).await;
    c.subscribe(topic).await;
    match c.recv().await {
        Some(Packet::Publish(p)) => Some((p.payload.to_vec(), p.properties.0)),
        _ => None,
    }
}

/// ADR 0038 T3 acceptance: **a retained publish with MQTT 5 properties replays with
/// those properties from any node's cache** (MQTT-3.3.2-17 across the cluster).
///
/// One scenario pins all three property-carrying paths end to end over real peer
/// links:
///  1. a v5 publish with the full forwardable set (payload-format indicator,
///     Content Type, Response Topic, Correlation Data, User Properties) lands on
///     the NON-owner node — so the properties ride the owner-routed submit, the
///     committed record, and the commit fan-out before any subscriber sees them;
///  2. fresh subscribers on BOTH nodes replay payload AND properties;
///  3. the mesh is severed, the owner commits an update with different properties,
///     and on heal the non-owner converges via queue-heal + token back-fill — a
///     fresh subscriber there replays the NEW properties.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_mqtt5_properties_replay_from_any_nodes_cache() {
    let (id_a, cli_a, peer_a, tx_a, placement_a) =
        build_durable_retained_node("prop-a", "prop-b").await;
    let (id_b, cli_b, peer_b, tx_b, _) = build_durable_retained_node("prop-b", "prop-a").await;

    let link = || {
        vec![
            tokio::spawn(mqttd::peer::dial_forever(
                peer_b.to_string(),
                id_a.clone(),
                tx_a.clone(),
                None,
                None,
            )),
            tokio::spawn(mqttd::peer::dial_forever(
                peer_a.to_string(),
                id_b.clone(),
                tx_b.clone(),
                None,
                None,
            )),
        ]
    };

    // A topic whose group node A owns (HRW — identical from both views), so a
    // publish on B must route to A, commit there, and fan back out.
    let topic = (0..100_000)
        .map(|i| format!("dev/{i}/state"))
        .find(|t| placement_a.read().unwrap().owner(t) == id_a)
        .expect("some topic is owned by A");

    // The full forwardable property set, in the order the broker replays them.
    let props_v1 = || {
        mqtt_codec::Properties(vec![
            Property::PayloadFormatIndicator(1),
            Property::ContentType("application/json".into()),
            Property::ResponseTopic("replies/dev".into()),
            Property::CorrelationData(bytes::Bytes::from_static(b"corr-1")),
            Property::UserProperty("trace".into(), "abc".into()),
            Property::UserProperty("origin".into(), "sensor-7".into()),
        ])
    };

    // 1+2: publish on the non-owner; a fresh v5 subscriber on EACH node must
    // replay the payload with every property intact.
    let dials = link();
    let mut publisher = Client::connect_v5(cli_b, "prop-pub").await;
    let expect_v1 = Some((b"v1".to_vec(), props_v1().0));
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut tick = 0;
    loop {
        tick += 1;
        publisher
            .publish_retained_v5(&topic, b"v1", props_v1())
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        if retained_v5_on(cli_a, &format!("pv-a{tick}"), &topic).await == expect_v1
            && retained_v5_on(cli_b, &format!("pv-b{tick}"), &topic).await == expect_v1
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "properties never replayed from both caches"
        );
    }

    // 3: sever, commit an update with DIFFERENT properties on the owner, heal —
    // the non-owner must converge to the new payload AND the new properties.
    for d in dials {
        d.abort();
    }
    tokio::time::sleep(Duration::from_millis(400)).await;

    let props_v2 = mqtt_codec::Properties(vec![
        Property::ContentType("text/plain".into()),
        Property::UserProperty("trace".into(), "xyz".into()),
    ]);
    let mut owner_pub = Client::connect_v5(cli_a, "prop-pub-a").await;
    owner_pub
        .publish_retained_v5(&topic, b"v2", props_v2.clone())
        .await;
    let expect_v2 = Some((b"v2".to_vec(), props_v2.0));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut tick = 0;
    loop {
        tick += 1;
        if retained_v5_on(cli_a, &format!("ov-a{tick}"), &topic).await == expect_v2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the owner never committed the updated properties"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _dials = link();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut tick = 0;
    loop {
        tick += 1;
        let on_b = retained_v5_on(cli_b, &format!("bf-b{tick}"), &topic).await;
        if on_b == expect_v2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "back-fill never delivered the updated properties to the non-owner: {on_b:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The 0014-T7 scenario, closed (ADR 0037 §5, P6): **divergent retained writes across
/// a partition converge after heal on every node.**
///
/// Node A owns the topic's group. During the partition A's own retained write commits
/// (the majority side keeps working), while B's write cannot reach the owner and
/// **queues** — bounded queue-until-heal, never a divergent local commit. B stays on
/// the last committed value meanwhile: retained **staleness** on the minority side,
/// never divergence (the CP trade, ADR 0037 §5). On heal, B's queue submits to the
/// owner, which commits it *after* A's write — and the commit fan-out plus the
/// token-aware back-fill converge every node to that one committed value.
///
/// The durable authority here is the always-owner in-memory log; the real plane's
/// quorum, fencing, and lease behaviour are proven in the mqtt-cluster and
/// `durable_sessions` suites. What this test pins is partition-time queueing and
/// heal-time convergence over real, severable TCP peer links.
// Prime, partition, diverge, heal, converge — one scenario, deliberately linear.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn divergent_retained_writes_across_a_partition_converge_after_heal() {
    let (id_a, cli_a, peer_a, tx_a, placement_a) =
        build_durable_retained_node("part-a", "part-b").await;
    let (id_b, cli_b, peer_b, tx_b, _) = build_durable_retained_node("part-b", "part-a").await;

    // A severable full-mesh link (mirrors common::link/sever).
    let link = |tx_a: &tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>,
                tx_b: &tokio::sync::mpsc::UnboundedSender<mqttd::HubCommand>| {
        vec![
            tokio::spawn(mqttd::peer::dial_forever(
                peer_b.to_string(),
                id_a.clone(),
                tx_a.clone(),
                None,
                None,
            )),
            tokio::spawn(mqttd::peer::dial_forever(
                peer_a.to_string(),
                id_b.clone(),
                tx_b.clone(),
                None,
                None,
            )),
        ]
    };

    // A topic whose group node A owns (HRW — identical from both views).
    let topic = (0..100_000)
        .map(|i| format!("dev/{i}/state"))
        .find(|t| placement_a.read().unwrap().owner(t) == id_a)
        .expect("some topic is owned by A");

    // Link up and prime through the full path: B's publish routes to owner A,
    // commits, and fans back out — a fresh subscriber on EACH node replays it.
    let dials = link(&tx_a, &tx_b);
    let mut prime = Client::connect(cli_b, "prime").await;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut tick = 0;
    loop {
        tick += 1;
        prime.publish_retained(&topic, b"prime").await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        if retained_on(cli_a, &format!("up-a{tick}"), &topic).await == Some(b"prime".to_vec())
            && retained_on(cli_b, &format!("up-b{tick}"), &topic).await == Some(b"prime".to_vec())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the owner-commit + fan-out pipeline never came up"
        );
    }

    // PARTITION: sever the mesh; give both sides a moment to observe the EOFs.
    for d in dials {
        d.abort();
    }
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Divergent writes: A's commits on the majority side; B's cannot reach the
    // owner and queues (ADR 0037 §5) — B keeps serving the last committed value.
    let mut pub_a = Client::connect(cli_a, "pub-a").await;
    pub_a.publish_retained(&topic, b"from-a").await;
    let mut pub_b = Client::connect(cli_b, "pub-b").await;
    pub_b.publish_retained(&topic, b"from-b").await;

    // A converges to its own committed write...
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut tick = 0;
    loop {
        tick += 1;
        if retained_on(cli_a, &format!("pa{tick}"), &topic).await == Some(b"from-a".to_vec()) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "A never committed its majority-side write"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // ...while B is STALE, not divergent: it still serves the committed prime value
    // (its own write is queued, not applied — the CP trade in action).
    assert_eq!(
        retained_on(cli_b, "stale-b", &topic).await,
        Some(b"prime".to_vec()),
        "the minority side must serve the last committed value, not its queued write"
    );

    // HEAL: relink. B's queue submits to the owner (committing AFTER A's write),
    // and fan-out + token back-fill converge every node to that value.
    let _dials = link(&tx_a, &tx_b);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut tick = 0;
    loop {
        tick += 1;
        let on_a = retained_on(cli_a, &format!("ca{tick}"), &topic).await;
        let on_b = retained_on(cli_b, &format!("cb{tick}"), &topic).await;
        if on_a == Some(b"from-b".to_vec()) && on_b == Some(b"from-b".to_vec()) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "nodes never converged to the heal-committed write: A={on_a:?} B={on_b:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// ADR 0038: the peer handshake negotiates a protocol range. A node announcing a
/// disjoint range is rejected at `Hello` — closed before it half-joins the mesh,
/// with no Hello reply — while a compatible node completes the handshake.
#[tokio::test]
async fn an_incompatible_peer_protocol_range_is_rejected_at_hello() {
    use mqtt_cluster::peer::{encode, PeerMessage, PROTO_MAX, PROTO_MIN};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (hub, tx) = Hub::with_config(
        NodeId("proto-a".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = peer.local_addr().unwrap();
    tokio::spawn(mqttd::peer::serve_listener(
        peer,
        NodeId("proto-a".into()),
        tx,
        None,
        None,
        None,
    ));

    // A "future" build speaking only versions far beyond ours: the listener must
    // close the link without announcing itself.
    let mut s = TcpStream::connect(addr).await.unwrap();
    let mut frame = Vec::new();
    encode(
        &PeerMessage::Hello {
            node_id: "future".into(),
            proto_min: 99,
            proto_max: 99,
        },
        &mut frame,
    )
    .unwrap();
    s.write_all(&frame).await.unwrap();
    let mut buf = [0u8; 64];
    let n = timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .expect("the listener must close an incompatible link promptly")
        .unwrap_or(0);
    assert_eq!(
        n, 0,
        "an incompatible peer must get no Hello reply, just a close"
    );

    // Control: the same handshake at our version completes (Hello comes back).
    let mut s = TcpStream::connect(addr).await.unwrap();
    let mut frame = Vec::new();
    encode(
        &PeerMessage::Hello {
            node_id: "peer-ok".into(),
            proto_min: PROTO_MIN,
            proto_max: PROTO_MAX,
        },
        &mut frame,
    )
    .unwrap();
    s.write_all(&frame).await.unwrap();
    let n = timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .expect("a compatible peer must receive the Hello reply")
        .unwrap();
    assert!(n > 0, "a compatible peer completes the handshake");
}
