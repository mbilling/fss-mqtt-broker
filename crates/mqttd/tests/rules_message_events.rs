//! The rule engine's four message events end to end (ADR 0083): real sockets, the real
//! codec, the real hub — and, for the cross-node half, a real peer link.
//!
//! `$events/message/delivered`, `acked`, `dropped` and `delivery_dropped` as EMQX raises
//! them (`emqx_rule_events:eventmsg_delivered/2` and its siblings): which packets fire
//! them, on which node, once per what, and with which fields. The expected values are
//! EMQX 6.3.1's for the same exchange (`mosquitto` and `paho` clients against
//! `emqx/emqx:6.3.1`, a `SELECT *` rule per event); where mqttd differs the test says so.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::Client;
use mqtt_cluster::NodeId;
use mqtt_codec::packet::{Connect, Subscribe, SubscribeFilter};
use mqtt_codec::{Packet, Properties, Property as P, ProtocolVersion, QoS, SubscriptionOptions};
use mqtt_storage::{MemorySessionStore, OverflowPolicy, QueueLimits};
use mqttd::hub::{Hub, HubCommand};
use serde_json::{json, Value};
use tokio::net::TcpListener;

/// Every message event about a message in `t/`, republished as JSON to `ev/<event>` —
/// EMQX's own example statements (`… FROM "$events/message/delivered" WHERE topic =~
/// 't/#'`) — and the publish itself, so a test can compare ids.
const WATCH: &str = r#"
[rules.delivered]
sql = '''SELECT * FROM "$events/message/delivered" WHERE topic =~ 't/#' '''
actions = [{ function = "republish", args = { topic = "ev/${event}", payload = "${.}", qos = 0 } }]

[rules.acked]
sql = '''SELECT * FROM "$events/message/acked" WHERE topic =~ 't/#' '''
actions = [{ function = "republish", args = { topic = "ev/${event}", payload = "${.}", qos = 0 } }]

[rules.dropped]
sql = '''SELECT * FROM "$events/message/dropped" WHERE topic =~ 't/#' '''
actions = [{ function = "republish", args = { topic = "ev/${event}", payload = "${.}", qos = 0 } }]

[rules.delivery_dropped]
sql = '''SELECT * FROM "$events/message/delivery_dropped" WHERE topic =~ 't/#' '''
actions = [{ function = "republish", args = { topic = "ev/${event}", payload = "${.}", qos = 0 } }]

[rules.published]
sql = '''SELECT id, clientid, publish_received_at FROM "t/#" '''
actions = [{ function = "republish", args = { topic = "ev/message.publish", payload = "${.}", qos = 0 } }]
"#;

/// A node with no rules at all.
const NONE: &str = "";

struct Broker {
    addr: SocketAddr,
    hub_tx: tokio::sync::mpsc::UnboundedSender<HubCommand>,
}

/// Admits any username (with any password) and no credentials.
struct AnyUser;

#[async_trait::async_trait]
impl mqtt_auth::Authenticator for AnyUser {
    async fn authenticate(
        &self,
        _client: &mqtt_core::ClientId,
        creds: &mqtt_auth::Credentials<'_>,
    ) -> Result<mqtt_auth::Identity, mqtt_auth::AuthError> {
        let subject = match creds {
            mqtt_auth::Credentials::Password { username, .. } => (*username).to_string(),
            _ => "anonymous".into(),
        };
        Ok(mqtt_auth::Identity {
            subject,
            groups: vec![],
        })
    }
}

/// The production wiring in miniature, as `tests/rules.rs` builds it: the rule engine on
/// every connection and in the hub, one store shared by both.
async fn start_node(name: &str, rules: &str, limits: QueueLimits) -> (Broker, TcpListener, NodeId) {
    start_node_at(name, rules, limits, None).await
}

/// A wall clock the test moves: message expiry is judged in whole epoch seconds, and a
/// test that waited for them to pass would be waiting on nothing it can observe.
#[derive(Debug)]
struct TestClock(std::sync::atomic::AtomicU64);

impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self(std::sync::atomic::AtomicU64::new(1_800_000_000)))
    }

    fn advance(&self, secs: u64) {
        self.0.fetch_add(secs, std::sync::atomic::Ordering::Relaxed);
    }
}

impl mqttd::clock::Clock for TestClock {
    fn now_epoch_secs(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// [`start_node`], with the hub's wall clock in the test's hands when one is given.
async fn start_node_at(
    name: &str,
    rules: &str,
    limits: QueueLimits,
    clock: Option<Arc<TestClock>>,
) -> (Broker, TcpListener, NodeId) {
    let store = Arc::new(MemorySessionStore::with_limits(limits));
    let id = NodeId(name.into());
    let (mut hub, hub_tx) = Hub::with_config(id.clone(), store.clone());
    if let Some(clock) = clock {
        hub.attach_clock(clock);
    }
    tokio::spawn(hub.run());
    let set = Arc::new(
        mqtt_rules::RuleSet::parse(rules)
            .unwrap_or_else(|e| panic!("test rules must load: {e}"))
            .rules,
    );
    let (rules_tx, rx) = tokio::sync::watch::channel(set);
    // The rules never reload in these tests; the sender only has to outlive them.
    std::mem::forget(rules_tx);
    let engine = mqttd::rules::Rules::new(rx, Arc::from(name), None);
    hub_tx
        .send(HubCommand::AttachRules(engine.clone()))
        .unwrap();
    let policy = Arc::new(mqttd::conn::ConnPolicy {
        anonymous: None,
        auth: mqttd::conn::auth_handle(Arc::new(AnyUser)),
        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
        identity_source: mqtt_auth::mtls::IdentitySource::default(),
        audit: Arc::new(mqtt_observability::AuditLog::new()),
        proxy: None,
        node: None,
        store: Some(store as Arc<dyn mqtt_storage::SessionStore>),
        connect_timeout: Duration::from_secs(10),
        enhanced: None,
        shutdown: None,
        metrics: None,
        ingress: None,
        rules: Some(engine),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let arrival = mqttd::conn::Arrival {
                sockname: stream.local_addr().ok(),
                transport: mqtt_net::Transport::PlainTcp,
            };
            tokio::spawn(mqttd::conn::handle_stream_watched(
                stream,
                Some(peer),
                arrival,
                None,
                policy.clone(),
                accept_tx.clone(),
                None,
            ));
        }
    });
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    (Broker { addr, hub_tx }, peer, id)
}

async fn start_broker(rules: &str) -> Broker {
    start_node("node-1", rules, QueueLimits::default()).await.0
}

/// Two nodes, each with its own rules, linked into a full peer mesh.
async fn start_cluster(rules_a: &str, rules_b: &str) -> (Broker, Broker) {
    let (a, peer_a, id_a) = start_node("node-a", rules_a, QueueLimits::default()).await;
    let (b, peer_b, id_b) = start_node("node-b", rules_b, QueueLimits::default()).await;
    let (paddr_a, paddr_b) = (peer_a.local_addr().unwrap(), peer_b.local_addr().unwrap());
    for (listener, id, hub) in [
        (peer_a, id_a.clone(), a.hub_tx.clone()),
        (peer_b, id_b.clone(), b.hub_tx.clone()),
    ] {
        tokio::spawn(mqttd::peer::serve_listener(
            listener, id, hub, None, None, None,
        ));
    }
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_b.to_string(),
        id_a,
        a.hub_tx.clone(),
        None,
        None,
    ));
    tokio::spawn(mqttd::peer::dial_forever(
        paddr_a.to_string(),
        id_b,
        b.hub_tx.clone(),
        None,
        None,
    ));
    (a, b)
}

/// Connect a v5 client with a username (and so an address, a username and a client id
/// for the events to report), a clean start unless `resume`, and the given properties.
async fn connect(
    addr: SocketAddr,
    id: &str,
    username: Option<&str>,
    resume: bool,
    properties: Vec<P>,
) -> Client {
    let mut c = Client::open(addr, ProtocolVersion::V5).await;
    c.send(&Packet::Connect(Connect {
        protocol: ProtocolVersion::V5,
        clean_session: !resume,
        keep_alive: 0,
        client_id: id.into(),
        last_will: None,
        username: username.map(Into::into),
        password: username.map(|_| bytes::Bytes::from_static(b"x")),
        properties: Properties(properties),
    }))
    .await;
    assert!(
        matches!(c.recv().await, Packet::ConnAck(a) if a.code == 0),
        "{id} connects"
    );
    c
}

/// The watcher of the events the rules republish to `ev/#`.
async fn watcher(addr: SocketAddr) -> Client {
    let mut w = Client::connect(addr, "watcher").await;
    w.subscribe(1, "ev/#", QoS::AtMostOnce).await;
    w
}

/// The next event the watcher hears whose `event` is `event`, as JSON; the publish
/// rule's own output (`ev/message.publish`) is skipped unless asked for.
async fn next(w: &mut Client, event: &str) -> Value {
    loop {
        let p = w.expect_publish().await;
        let v: Value = serde_json::from_slice(&p.payload)
            .unwrap_or_else(|e| panic!("{}: {e}", String::from_utf8_lossy(&p.payload)));
        if p.topic == "ev/message.publish" && event != "message.publish" {
            continue;
        }
        assert_eq!(p.topic, format!("ev/{event}"), "{v}");
        return v;
    }
}

/// No further event of the four reaches the watcher (publish-rule output aside).
async fn no_more(w: &mut Client) {
    while let Some(p) = w.try_recv().await {
        match p {
            Packet::Publish(p) if p.topic == "ev/message.publish" => {}
            other => panic!("an event nobody should have raised: {other:?}"),
        }
    }
}

fn field_names(v: &Value) -> Vec<&str> {
    let mut names: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    names.sort_unstable();
    names
}

/// EMQX's `message.delivered` columns, with the `metadata` a `SELECT *` adds.
const DELIVERED: [&str; 17] = [
    "clientid",
    "event",
    "flags",
    "from_clientid",
    "from_username",
    "id",
    "metadata",
    "node",
    "payload",
    "peerhost",
    "peername",
    "pub_props",
    "publish_received_at",
    "qos",
    "timestamp",
    "topic",
    "username",
];

fn with<'a>(base: &[&'a str], extra: &'a str) -> Vec<&'a str> {
    let mut all = base.to_vec();
    all.push(extra);
    all.sort_unstable();
    all
}

/// EMQX's scenario 1, field for field: `pub1`/`pubuser` publishes `QoS` 1 with
/// properties to `sub1`/`subuser`, subscribed `QoS` 1 with Subscription Identifier 7.
/// The delivery raises `message.delivered` and the subscriber's PUBACK `message.acked`,
/// each once, naming both parties, with the PUBLISH's properties as sent (the
/// subscription's identifier among them), and the id the publish's own rules saw.
// One exchange, each packet and its event after the other.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn a_qos1_delivery_and_its_acknowledgement_read_as_emqx_s() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut sub = connect(broker.addr, "sub1", Some("subuser"), false, vec![]).await;
    sub.subscribe_with_id(1, "t/a", QoS::AtLeastOnce, 7).await;
    let mut publisher = connect(broker.addr, "pub1", Some("pubuser"), false, vec![]).await;
    publisher
        .publish(
            "t/a",
            br#"{"x":1}"#,
            QoS::AtLeastOnce,
            Some(1),
            vec![
                P::UserProperty("k".into(), "v".into()),
                P::MessageExpiryInterval(60),
                P::ContentType("application/json".into()),
            ],
        )
        .await;
    assert_eq!(publisher.recv().await, Packet::PubAck(1.into()));
    let delivery = sub.expect_publish().await;
    assert_eq!(
        (delivery.qos, &delivery.payload[..]),
        (QoS::AtLeastOnce, &br#"{"x":1}"#[..])
    );

    let at_publish = next(&mut w, "message.publish").await;
    let delivered = next(&mut w, "message.delivered").await;
    assert_eq!(field_names(&delivered), DELIVERED);
    assert_eq!(
        (
            &delivered["from_clientid"],
            &delivered["from_username"],
            &delivered["clientid"],
            &delivered["username"],
            &delivered["topic"],
            &delivered["qos"],
            &delivered["payload"],
            &delivered["event"],
            &delivered["node"],
            &delivered["flags"],
        ),
        (
            &json!("pub1"),
            &json!("pubuser"),
            &json!("sub1"),
            &json!("subuser"),
            &json!("t/a"),
            &json!(1),
            &json!(r#"{"x":1}"#),
            &json!("message.delivered"),
            &json!("node-1"),
            &json!({"dup": false, "retain": false}),
        )
    );
    assert_eq!(
        delivered["pub_props"],
        json!({
            "Subscription-Identifier": 7,
            "Message-Expiry-Interval": 60,
            "User-Property": {"k": "v"},
            "User-Property-Pairs": [{"key": "k", "value": "v"}],
            "Content-Type": "application/json",
        })
    );
    // The receiver's address, as the listener saw it.
    assert_eq!(delivered["peerhost"], "127.0.0.1");
    assert!(
        delivered["peername"]
            .as_str()
            .unwrap()
            .starts_with("127.0.0.1:"),
        "{delivered}"
    );
    // One message, one id: the publish's rules and the delivery's agree on it, and on
    // when it was received.
    assert_eq!(delivered["id"], at_publish["id"]);
    assert_eq!(delivered["id"].as_str().unwrap().len(), 32);
    assert_eq!(
        delivered["publish_received_at"],
        at_publish["publish_received_at"]
    );
    assert!(delivered["timestamp"].as_i64() >= delivered["publish_received_at"].as_i64());
    no_more(&mut w).await;

    // The acknowledgement: the same message, plus the PUBACK's properties.
    sub.send(&Packet::PubAck(mqtt_codec::packet::Ack {
        pkid: delivery.pkid.unwrap(),
        reason: 0,
        properties: Properties(vec![
            P::ReasonString("fine".into()),
            P::UserProperty("a".into(), "b".into()),
        ]),
    }))
    .await;
    let acked = next(&mut w, "message.acked").await;
    assert_eq!(field_names(&acked), with(&DELIVERED, "puback_props"));
    assert_eq!(
        acked["puback_props"],
        json!({
            "Reason-String": "fine",
            "User-Property": {"a": "b"},
            "User-Property-Pairs": [{"key": "a", "value": "b"}],
        })
    );
    for field in [
        "id",
        "from_clientid",
        "from_username",
        "clientid",
        "username",
        "qos",
        "payload",
        "pub_props",
        "peername",
    ] {
        assert_eq!(acked[field], delivered[field], "{field}");
    }
    assert_eq!(acked["event"], "message.acked");
    // A second PUBACK for the same id acknowledges nothing.
    sub.puback(delivery.pkid.unwrap()).await;
    no_more(&mut w).await;
}

/// `qos` is the DELIVERY's: a `QoS` 1 publish to a `QoS` 0 subscriber is delivered —
/// and reported — at 0, and nothing acknowledges it. EMQX's scenario 4.
#[tokio::test]
async fn a_downgraded_delivery_is_reported_at_its_own_qos_and_never_acked() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut sub = Client::connect(broker.addr, "sub3").await;
    sub.subscribe(1, "t/d", QoS::AtMostOnce).await;
    let mut publisher = connect(broker.addr, "pub1", Some("pubuser"), false, vec![]).await;
    publisher
        .publish("t/d", b"down", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(publisher.recv().await, Packet::PubAck(1.into()));
    assert_eq!(sub.expect_publish().await.qos, QoS::AtMostOnce);
    let delivered = next(&mut w, "message.delivered").await;
    assert_eq!(
        (&delivered["qos"], &delivered["clientid"]),
        (&json!(0), &json!("sub3"))
    );
    // A v3.1.1 subscriber that sent no username: EMQX prints `undefined`, and a
    // PUBLISH without properties prints the empty user-property map.
    assert_eq!(delivered["username"], "undefined");
    assert_eq!(delivered["pub_props"], json!({"User-Property": {}}));
    no_more(&mut w).await;
}

/// A `QoS` 2 delivery is acked at the subscriber's PUBREC — once, and not again at its
/// PUBCOMP — as in EMQX (`emqx_channel:process_pubrec/2`). Scenario 3.
#[tokio::test]
async fn a_qos2_delivery_is_acked_at_its_pubrec_and_only_there() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut sub = connect(broker.addr, "sub2", None, false, vec![]).await;
    sub.subscribe(1, "t/q2", QoS::ExactlyOnce).await;
    let mut publisher = connect(broker.addr, "pub1", Some("pubuser"), false, vec![]).await;
    publisher
        .publish("t/q2", b"two", QoS::ExactlyOnce, Some(1), vec![])
        .await;
    assert!(matches!(publisher.recv().await, Packet::PubRec(a) if a.pkid == 1));
    publisher.pubrel(1).await;
    assert!(matches!(publisher.recv().await, Packet::PubComp(a) if a.pkid == 1));

    let delivery = sub.expect_publish().await;
    assert_eq!(delivery.qos, QoS::ExactlyOnce);
    let delivered = next(&mut w, "message.delivered").await;
    assert_eq!(
        (&delivered["qos"], &delivered["payload"]),
        (&json!(2), &json!("two"))
    );
    no_more(&mut w).await;

    let pkid = delivery.pkid.unwrap();
    // A PUBACK is not this delivery's acknowledgement.
    sub.puback(pkid).await;
    no_more(&mut w).await;
    sub.pubrec(pkid).await;
    assert!(matches!(sub.recv().await, Packet::PubRel(a) if a.pkid == pkid));
    let acked = next(&mut w, "message.acked").await;
    assert_eq!(
        (&acked["qos"], &acked["id"], &acked["puback_props"]),
        (&json!(2), &delivered["id"], &json!({"User-Property": {}}))
    );
    sub.pubcomp(pkid).await;
    no_more(&mut w).await;
}

/// A shared subscription's one chosen member is the delivery's receiver. Scenario 5.
#[tokio::test]
async fn a_shared_subscription_s_chosen_member_is_the_receiver() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut member = connect(broker.addr, "sh1", None, false, vec![]).await;
    member.subscribe(1, "$share/g/t/s", QoS::AtLeastOnce).await;
    let mut publisher = connect(broker.addr, "pub1", Some("pubuser"), false, vec![]).await;
    publisher
        .publish("t/s", b"shared", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(publisher.recv().await, Packet::PubAck(1.into()));
    let delivery = member.expect_publish().await;
    let delivered = next(&mut w, "message.delivered").await;
    assert_eq!(
        (
            &delivered["clientid"],
            &delivered["topic"],
            &delivered["from_clientid"]
        ),
        (&json!("sh1"), &json!("t/s"), &json!("pub1"))
    );
    member.puback(delivery.pkid.unwrap()).await;
    assert_eq!(next(&mut w, "message.acked").await["clientid"], "sh1");
    // Placed with a shared group: the publish was not dropped.
    no_more(&mut w).await;
}

/// A publish nobody subscribes to raises `message.dropped`, reason `no_subscribers`,
/// with the PUBLISHER's client id, username and address — at `QoS` 0 as well, where
/// nothing else tells the publisher — and its retain flag. One a subscriber takes
/// raises none. Scenarios 2 and 2b.
#[tokio::test]
async fn a_publish_nobody_subscribes_to_is_reported_dropped() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut publisher = connect(broker.addr, "pub1", Some("pubuser"), false, vec![]).await;
    publisher
        .publish("t/nobody", b"none", QoS::AtMostOnce, None, vec![])
        .await;
    let at_publish = next(&mut w, "message.publish").await;
    let dropped = next(&mut w, "message.dropped").await;
    assert_eq!(
        field_names(&dropped),
        [
            "clientid",
            "event",
            "flags",
            "id",
            "metadata",
            "node",
            "payload",
            "peerhost",
            "peername",
            "pub_props",
            "publish_received_at",
            "qos",
            "reason",
            "timestamp",
            "topic",
            "username",
        ]
    );
    assert_eq!(
        (
            &dropped["reason"],
            &dropped["clientid"],
            &dropped["username"],
            &dropped["peerhost"],
            &dropped["topic"],
            &dropped["qos"],
            &dropped["payload"],
            &dropped["flags"],
            &dropped["pub_props"],
        ),
        (
            &json!("no_subscribers"),
            &json!("pub1"),
            &json!("pubuser"),
            &json!("127.0.0.1"),
            &json!("t/nobody"),
            &json!(0),
            &json!("none"),
            &json!({"dup": false, "retain": false}),
            &json!({"User-Property": {}}),
        )
    );
    assert_eq!(dropped["id"], at_publish["id"]);

    // Retained, `QoS` 1, from a client without a username: still dropped (retaining
    // it is not delivering it), `username` printed as EMQX prints an absent one.
    let mut plain = Client::connect(broker.addr, "pub2").await;
    plain
        .publish_retained_acked("t/nobody2", b"none-r", 1)
        .await;
    let dropped = next(&mut w, "message.dropped").await;
    assert_eq!(
        (
            &dropped["clientid"],
            &dropped["username"],
            &dropped["qos"],
            &dropped["flags"]
        ),
        (
            &json!("pub2"),
            &json!("undefined"),
            &json!(1),
            &json!({"dup": false, "retain": true})
        )
    );

    // With a subscriber there is no drop: the delivery is the only event.
    let mut sub = Client::connect(broker.addr, "sub").await;
    sub.subscribe(1, "t/somebody", QoS::AtMostOnce).await;
    publisher
        .publish("t/somebody", b"here", QoS::AtMostOnce, None, vec![])
        .await;
    sub.expect_publish().await;
    assert_eq!(next(&mut w, "message.delivered").await["clientid"], "sub");
    no_more(&mut w).await;
}

/// A `QoS` 2 publish under a packet id that still awaits its PUBREL is not delivered a
/// second time. Resent with the DUP flag it is the client's retransmission and raises
/// nothing; WITHOUT it, the client reused an id in use, and the rule engine is told of a
/// dropped message with EMQX's reason `packet_identifier_inuse`
/// (`emqx_session_mem:publish/3`, `emqx_session:on_dropped_qos2_msg/3`) — both as EMQX
/// 6.3.1 does for the same bytes. Its publish rules do not run on either.
#[tokio::test]
async fn a_qos2_packet_id_reused_before_its_release_is_dropped_as_emqx_drops_it() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut sub = Client::connect(broker.addr, "sub").await;
    sub.subscribe(1, "t/q2", QoS::AtMostOnce).await;
    let mut publisher = connect(broker.addr, "pub1", Some("pubuser"), false, vec![]).await;
    let publish = |dup| {
        Packet::Publish(mqtt_codec::packet::Publish {
            properties: Properties::new(),
            dup,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: "t/q2".into(),
            pkid: Some(7),
            payload: bytes::Bytes::from_static(b"once"),
        })
    };
    publisher.send(&publish(false)).await;
    assert!(matches!(publisher.recv().await, Packet::PubRec(a) if a.pkid == 7));
    sub.expect_publish().await;
    assert_eq!(next(&mut w, "message.publish").await["clientid"], "pub1");
    assert_eq!(next(&mut w, "message.delivered").await["clientid"], "sub");

    // The retransmission: acknowledged again, delivered to nobody, no event.
    publisher.send(&publish(true)).await;
    assert!(matches!(publisher.recv().await, Packet::PubRec(a) if a.pkid == 7));
    no_more(&mut w).await;
    sub.expect_silence().await;
    // The same id on a publish that is not a retransmission.
    publisher.send(&publish(false)).await;
    assert!(matches!(publisher.recv().await, Packet::PubRec(a) if a.pkid == 7));
    let dropped = next_any(&mut w).await;
    assert_eq!(
        (
            &dropped["event"],
            &dropped["reason"],
            &dropped["clientid"],
            &dropped["username"],
            &dropped["qos"],
            &dropped["payload"],
            &dropped["flags"],
        ),
        (
            &json!("message.dropped"),
            &json!("packet_identifier_inuse"),
            &json!("pub1"),
            &json!("pubuser"),
            &json!(2),
            &json!("once"),
            &json!({"dup": false, "retain": false}),
        )
    );
    sub.expect_silence().await;
    // Released, the id is free again: the next publish under it is a new message.
    publisher.pubrel(7).await;
    assert!(matches!(publisher.recv().await, Packet::PubComp(a) if a.pkid == 7));
    publisher.send(&publish(false)).await;
    assert!(matches!(publisher.recv().await, Packet::PubRec(a) if a.pkid == 7));
    sub.expect_publish().await;
    assert_eq!(next(&mut w, "message.delivered").await["clientid"], "sub");
    no_more(&mut w).await;
}

/// A message a rule republishes is a message from that rule (EMQX publishes it as the
/// rule's id, with no username or address): unrouted, it is dropped as one.
#[tokio::test]
async fn a_rule_s_unrouted_republish_is_dropped_as_the_rule_s_message() {
    let rules = r#"
[rules.copy]
sql = '''SELECT payload FROM "in/#" '''
actions = [{ function = "republish", args = { topic = "t/copy", payload = "${payload}" } }]

[rules.dropped]
sql = '''SELECT * FROM "$events/message/dropped" WHERE topic =~ 't/#' '''
actions = [{ function = "republish", args = { topic = "ev/${event}", payload = "${.}", qos = 0 } }]
"#;
    let broker = start_broker(rules).await;
    let mut w = watcher(broker.addr).await;
    let mut listener = Client::connect(broker.addr, "listener").await;
    listener.subscribe(1, "in/#", QoS::AtMostOnce).await;
    let mut publisher = connect(broker.addr, "pub1", Some("pubuser"), false, vec![]).await;
    publisher
        .publish("in/x", b"hello", QoS::AtMostOnce, None, vec![])
        .await;
    listener.expect_publish().await;
    let dropped = next(&mut w, "message.dropped").await;
    assert_eq!(
        (
            &dropped["clientid"],
            &dropped["username"],
            &dropped["peerhost"],
            &dropped["peername"],
            &dropped["topic"],
            &dropped["payload"],
        ),
        (
            &json!("copy"),
            &json!("undefined"),
            &json!("undefined"),
            &json!("undefined"),
            &json!("t/copy"),
            &json!("hello"),
        )
    );
    no_more(&mut w).await;
}

/// A client's own publish matching its No Local subscription is not delivered back, and
/// raises `delivery.dropped` with reason `no_local` — the client is both parties — and
/// NOT `message.dropped`: its session was reached, as in EMQX.
#[tokio::test]
async fn a_no_local_subscriber_s_own_publish_is_a_dropped_delivery() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut c = connect(broker.addr, "nl1", Some("nluser"), false, vec![]).await;
    c.send(&Packet::Subscribe(Subscribe {
        properties: Properties::new(),
        pkid: 1,
        filters: vec![SubscribeFilter {
            options: SubscriptionOptions {
                no_local: true,
                ..SubscriptionOptions::default()
            },
            path: "t/nl".into(),
            qos: QoS::AtLeastOnce,
        }],
    }))
    .await;
    assert!(matches!(c.recv().await, Packet::SubAck(_)));
    c.publish("t/nl", b"self", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(c.recv().await, Packet::PubAck(1.into()));
    let lost = next(&mut w, "delivery.dropped").await;
    assert_eq!(field_names(&lost), with(&DELIVERED, "reason"));
    assert_eq!(
        (
            &lost["reason"],
            &lost["from_clientid"],
            &lost["from_username"],
            &lost["clientid"],
            &lost["username"],
            &lost["peerhost"],
            &lost["qos"],
            &lost["payload"],
            &lost["event"],
        ),
        (
            &json!("no_local"),
            &json!("nl1"),
            &json!("nluser"),
            &json!("nl1"),
            &json!("nluser"),
            &json!("127.0.0.1"),
            &json!(1),
            &json!("self"),
            &json!("delivery.dropped"),
        )
    );
    no_more(&mut w).await;
    c.expect_silence().await;

    // Another client's publish to the same topic is simply delivered.
    let mut other = connect(broker.addr, "other", None, false, vec![]).await;
    other
        .publish("t/nl", b"theirs", QoS::AtMostOnce, None, vec![])
        .await;
    c.expect_publish().await;
    assert_eq!(
        next(&mut w, "message.delivered").await["from_clientid"],
        "other"
    );
    no_more(&mut w).await;
}

/// A queued message whose Message Expiry Interval runs out before its offline session
/// returns is dropped at the resume, with reason `expired`; the resumed connection is
/// the receiver. The message that had not expired is delivered — and reported — then.
#[tokio::test]
async fn a_queued_message_that_expired_is_a_dropped_delivery_at_the_resume() {
    let clock = TestClock::new();
    let broker = start_node_at("node-1", WATCH, QueueLimits::default(), Some(clock.clone()))
        .await
        .0;
    let mut w = watcher(broker.addr).await;
    let mut sub = connect(
        broker.addr,
        "exp1",
        Some("expuser"),
        false,
        vec![P::SessionExpiryInterval(300)],
    )
    .await;
    sub.subscribe(1, "t/exp/#", QoS::AtLeastOnce).await;
    sub.disconnect().await;

    let mut publisher = connect(broker.addr, "pubx", Some("pubuser"), false, vec![]).await;
    publisher
        .publish(
            "t/exp/stale",
            b"soon-stale",
            QoS::AtLeastOnce,
            Some(1),
            vec![P::MessageExpiryInterval(1)],
        )
        .await;
    assert_eq!(publisher.recv().await, Packet::PubAck(1.into()));
    publisher
        .publish(
            "t/exp/fresh",
            b"keeps",
            QoS::AtLeastOnce,
            Some(2),
            vec![P::MessageExpiryInterval(600)],
        )
        .await;
    assert_eq!(publisher.recv().await, Packet::PubAck(2.into()));
    // Queued for the offline session: reached, so neither is dropped as unrouted.
    no_more(&mut w).await;
    clock.advance(2);

    let mut sub = connect(
        broker.addr,
        "exp1",
        Some("expuser"),
        true,
        vec![P::SessionExpiryInterval(300)],
    )
    .await;
    let fresh = sub.expect_publish().await;
    assert_eq!(&fresh.payload[..], b"keeps");
    let mut seen = [next_any(&mut w).await, next_any(&mut w).await];
    seen.sort_by_key(|v| v["event"].as_str().unwrap().to_string());
    let (lost, delivered) = (&seen[0], &seen[1]);
    assert_eq!(
        (
            &lost["event"],
            &lost["reason"],
            &lost["clientid"],
            &lost["username"],
            &lost["topic"],
            &lost["payload"]
        ),
        (
            &json!("delivery.dropped"),
            &json!("expired"),
            &json!("exp1"),
            &json!("expuser"),
            &json!("t/exp/stale"),
            &json!("soon-stale"),
        )
    );
    assert_eq!(
        (
            &delivered["event"],
            &delivered["topic"],
            &delivered["clientid"]
        ),
        (
            &json!("message.delivered"),
            &json!("t/exp/fresh"),
            &json!("exp1")
        )
    );
    // The queue kept who published them (the in-memory store holds the message whole).
    assert_eq!(lost["from_clientid"], "pubx");
    assert_eq!(delivered["from_username"], "pubuser");
    // What is left of the fresh one's interval is what was sent, and reported.
    let left = delivered["pub_props"]["Message-Expiry-Interval"]
        .as_i64()
        .unwrap();
    assert!((590..=598).contains(&left), "{left}");
    no_more(&mut w).await;
}

/// Any of the four events, whichever comes.
async fn next_any(w: &mut Client) -> Value {
    loop {
        let p = w.expect_publish().await;
        if p.topic != "ev/message.publish" {
            return serde_json::from_slice(&p.payload).unwrap();
        }
    }
}

/// An offline session's queue that is full and rejects the newest message
/// (`reject-newest`) drops it with reason `queue_full`. The session has no connection,
/// so the event knows its subscriber's client id only: `username`, `peerhost` and
/// `peername` are `undefined` (EMQX's session remembers them).
#[tokio::test]
async fn a_full_offline_queue_rejecting_the_newest_is_a_dropped_delivery() {
    let limits = QueueLimits {
        max_messages: 2,
        overflow: OverflowPolicy::RejectNewest,
    };
    let broker = start_node("node-1", WATCH, limits).await.0;
    let mut w = watcher(broker.addr).await;
    let mut sub = connect(
        broker.addr,
        "qf1",
        Some("qfuser"),
        false,
        vec![P::SessionExpiryInterval(300)],
    )
    .await;
    sub.subscribe(1, "t/qf", QoS::AtLeastOnce).await;
    sub.disconnect().await;

    let mut publisher = connect(broker.addr, "pubx", Some("pubuser"), false, vec![]).await;
    for i in 0..4u16 {
        publisher
            .publish(
                "t/qf",
                format!("m{i}").as_bytes(),
                QoS::AtLeastOnce,
                Some(i + 1),
                vec![],
            )
            .await;
        assert_eq!(publisher.recv().await, Packet::PubAck((i + 1).into()));
    }
    for want in ["m2", "m3"] {
        let lost = next(&mut w, "delivery.dropped").await;
        assert_eq!(
            (
                &lost["reason"],
                &lost["payload"],
                &lost["clientid"],
                &lost["from_clientid"],
                &lost["from_username"],
                &lost["qos"],
            ),
            (
                &json!("queue_full"),
                &json!(want),
                &json!("qf1"),
                &json!("pubx"),
                &json!("pubuser"),
                &json!(1),
            )
        );
        assert_eq!(
            (&lost["username"], &lost["peerhost"], &lost["peername"]),
            (
                &json!("undefined"),
                &json!("undefined"),
                &json!("undefined")
            ),
            "an offline session has no connection to ask"
        );
    }
    no_more(&mut w).await;

    // The two that fit are delivered at the resume, in order, and reported then.
    let mut sub = connect(
        broker.addr,
        "qf1",
        Some("qfuser"),
        true,
        vec![P::SessionExpiryInterval(300)],
    )
    .await;
    for want in ["m0", "m1"] {
        assert_eq!(&sub.expect_publish().await.payload[..], want.as_bytes());
        let delivered = next(&mut w, "message.delivered").await;
        assert_eq!(
            (&delivered["payload"], &delivered["username"]),
            (&json!(want), &json!("qfuser"))
        );
    }
    no_more(&mut w).await;
}

/// The same cap with the subscriber ONLINE drops nothing: a message its full queue
/// rejects has no durable copy but is still sent live, so it is a delivery — reported
/// as one, and not as a dropped one.
#[tokio::test]
async fn a_message_an_online_subscriber_s_full_queue_rejects_is_still_a_delivery() {
    let limits = QueueLimits {
        max_messages: 2,
        overflow: OverflowPolicy::RejectNewest,
    };
    let broker = start_node("node-1", WATCH, limits).await.0;
    let mut w = watcher(broker.addr).await;
    let mut sub = connect(
        broker.addr,
        "qf2",
        None,
        false,
        vec![P::SessionExpiryInterval(300)],
    )
    .await;
    sub.subscribe(1, "t/qf", QoS::AtLeastOnce).await;
    let mut publisher = connect(broker.addr, "pubx", None, false, vec![]).await;
    // Unacknowledged, each stays in the session's queue: the third and fourth find it full.
    for i in 0..4u16 {
        publisher
            .publish(
                "t/qf",
                format!("m{i}").as_bytes(),
                QoS::AtLeastOnce,
                Some(i + 1),
                vec![],
            )
            .await;
        assert_eq!(publisher.recv().await, Packet::PubAck((i + 1).into()));
        assert_eq!(
            &sub.expect_publish().await.payload[..],
            format!("m{i}").as_bytes()
        );
        let delivered = next(&mut w, "message.delivered").await;
        assert_eq!(delivered["payload"], format!("m{i}"));
    }
    no_more(&mut w).await;
}

/// A message larger than the subscriber's Maximum Packet Size is dropped for that
/// subscriber alone. EMQX counts it (`delivery.dropped.too_large`) and raises no event;
/// mqttd raises `delivery.dropped` with its own reason, `too_large`, and no
/// `message.delivered` for a packet it did not send.
#[tokio::test]
async fn a_message_too_large_for_its_subscriber_is_a_dropped_delivery() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut small = connect(
        broker.addr,
        "small",
        Some("s"),
        false,
        vec![P::MaximumPacketSize(64)],
    )
    .await;
    small.subscribe(1, "t/big", QoS::AtMostOnce).await;
    let mut publisher = connect(broker.addr, "pub1", Some("pubuser"), false, vec![]).await;
    let big = vec![b'x'; 200];
    publisher
        .publish("t/big", &big, QoS::AtMostOnce, None, vec![])
        .await;
    let lost = next(&mut w, "delivery.dropped").await;
    assert_eq!(
        (
            &lost["reason"],
            &lost["clientid"],
            &lost["username"],
            &lost["from_clientid"],
            &lost["topic"]
        ),
        (
            &json!("too_large"),
            &json!("small"),
            &json!("s"),
            &json!("pub1"),
            &json!("t/big")
        )
    );
    assert_eq!(lost["payload"].as_str().unwrap().len(), 200);
    no_more(&mut w).await;
    small.expect_silence().await;
    // One that fits is delivered and reported as usual.
    publisher
        .publish("t/big", b"ok", QoS::AtMostOnce, None, vec![])
        .await;
    small.expect_publish().await;
    assert_eq!(next(&mut w, "message.delivered").await["payload"], "ok");
}

/// A retained message sent at subscribe is a delivery: `flags.retain` is set and the
/// message is still its publisher's. Scenario 6.
#[tokio::test]
async fn a_retained_message_sent_at_subscribe_is_a_delivery() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut publisher = Client::connect(broker.addr, "pub2").await;
    publisher
        .publish_retained_acked("t/kept", b"none-r", 1)
        .await;
    let dropped = next(&mut w, "message.dropped").await;
    let mut sub = connect(broker.addr, "sub4", None, false, vec![]).await;
    sub.subscribe(1, "t/kept", QoS::AtLeastOnce).await;
    let delivery = sub.expect_publish().await;
    assert!(delivery.retain);
    let delivered = next(&mut w, "message.delivered").await;
    assert_eq!(
        (
            &delivered["flags"],
            &delivered["from_clientid"],
            &delivered["from_username"],
            &delivered["clientid"],
            &delivered["qos"],
            &delivered["id"],
        ),
        (
            &json!({"dup": false, "retain": true}),
            &json!("pub2"),
            &json!("undefined"),
            &json!("sub4"),
            &json!(1),
            &dropped["id"],
        ),
        "the retained copy is the message that was published"
    );
}

/// A `QoS` 1 delivery resent when its session resumes is a delivery again, with
/// `flags.dup` set — EMQX's hook runs for every PUBLISH it sends, a resend included —
/// and the acknowledgement of the resend is the one reported.
#[tokio::test]
async fn a_resent_delivery_is_reported_again_with_dup() {
    let broker = start_broker(WATCH).await;
    let mut w = watcher(broker.addr).await;
    let mut sub = connect(
        broker.addr,
        "re1",
        Some("u"),
        false,
        vec![P::SessionExpiryInterval(300)],
    )
    .await;
    sub.subscribe(1, "t/re", QoS::AtLeastOnce).await;
    let mut publisher = connect(broker.addr, "pub1", Some("pubuser"), false, vec![]).await;
    publisher
        .publish("t/re", b"again", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(publisher.recv().await, Packet::PubAck(1.into()));
    let first = sub.expect_publish().await;
    assert!(!first.dup);
    let delivered = next(&mut w, "message.delivered").await;
    assert_eq!(delivered["flags"]["dup"], false);
    // Gone without acknowledging it.
    sub.disconnect().await;
    let mut sub = connect(
        broker.addr,
        "re1",
        Some("u"),
        true,
        vec![P::SessionExpiryInterval(300)],
    )
    .await;
    let resent = sub.expect_publish().await;
    assert!(resent.dup);
    let again = next(&mut w, "message.delivered").await;
    assert_eq!(
        (
            &again["flags"]["dup"],
            &again["id"],
            &again["from_clientid"]
        ),
        (&json!(true), &delivered["id"], &json!("pub1"))
    );
    sub.puback(resent.pkid.unwrap()).await;
    let acked = next(&mut w, "message.acked").await;
    assert_eq!(
        (&acked["id"], &acked["flags"]["dup"]),
        (&delivered["id"], &json!(true))
    );
    no_more(&mut w).await;
}

/// The legacy, underscore spellings select the same events, and `"$events/message/+"`
/// all four.
#[tokio::test]
async fn the_legacy_spellings_and_the_wildcard_select_the_events() {
    let rules = r#"
[rules.legacy]
sql = '''SELECT event FROM "$events/message_delivered", "$events/message_acked", "$events/message_dropped", "$events/delivery_dropped" WHERE topic =~ 't/#' '''
actions = [{ function = "republish", args = { topic = "ev/legacy", payload = "${event}", qos = 0 } }]

[rules.wild]
sql = '''SELECT event FROM "$events/message/+" WHERE topic =~ 't/#' '''
actions = [{ function = "republish", args = { topic = "ev/wild", payload = "${event}", qos = 0 } }]
"#;
    let broker = start_broker(rules).await;
    let mut w = watcher(broker.addr).await;
    let mut c = connect(broker.addr, "c", None, false, vec![]).await;
    c.send(&Packet::Subscribe(Subscribe {
        properties: Properties::new(),
        pkid: 1,
        filters: vec![
            SubscribeFilter {
                options: SubscriptionOptions::default(),
                path: "t/in".into(),
                qos: QoS::AtLeastOnce,
            },
            SubscribeFilter {
                options: SubscriptionOptions {
                    no_local: true,
                    ..SubscriptionOptions::default()
                },
                path: "t/mine".into(),
                qos: QoS::AtLeastOnce,
            },
        ],
    }))
    .await;
    assert!(matches!(c.recv().await, Packet::SubAck(_)));
    let mut heard = Vec::new();
    let hear = async |w: &mut Client, n: usize, heard: &mut Vec<(String, String)>| {
        for _ in 0..n {
            let p = w.expect_publish().await;
            heard.push((p.topic, String::from_utf8(p.payload.to_vec()).unwrap()));
        }
    };
    let mut other = connect(broker.addr, "other", None, false, vec![]).await;
    other
        .publish("t/in", b"1", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(other.recv().await, Packet::PubAck(1.into()));
    let delivery = c.expect_publish().await;
    hear(&mut w, 2, &mut heard).await;
    c.puback(delivery.pkid.unwrap()).await;
    hear(&mut w, 2, &mut heard).await;
    other
        .publish("t/none", b"2", QoS::AtMostOnce, None, vec![])
        .await;
    hear(&mut w, 2, &mut heard).await;
    c.publish("t/mine", b"3", QoS::AtMostOnce, None, vec![])
        .await;
    hear(&mut w, 2, &mut heard).await;
    heard.sort();
    let want: Vec<(String, String)> = ["ev/legacy", "ev/wild"]
        .into_iter()
        .flat_map(|topic| {
            [
                "delivery.dropped",
                "message.acked",
                "message.delivered",
                "message.dropped",
            ]
            .into_iter()
            .map(move |event| (topic.to_string(), event.to_string()))
        })
        .collect();
    assert_eq!(heard, want);
    no_more(&mut w).await;
}

/// Without a rule selecting a message event, nothing is raised — and a rule selecting
/// only ONE of them sees only that one.
#[tokio::test]
async fn only_the_selected_event_is_raised() {
    let rules = r#"
[rules.acked]
sql = '''SELECT clientid, from_clientid FROM "$events/message/acked" '''
actions = [{ function = "republish", args = { topic = "ev/acked", payload = "${clientid}<${from_clientid}", qos = 0 } }]
"#;
    let broker = start_broker(rules).await;
    let mut w = watcher(broker.addr).await;
    let mut sub = connect(broker.addr, "sub", None, false, vec![]).await;
    sub.subscribe(1, "t/#", QoS::AtLeastOnce).await;
    let mut publisher = connect(broker.addr, "pub", None, false, vec![]).await;
    publisher
        .publish("t/a", b"1", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert_eq!(publisher.recv().await, Packet::PubAck(1.into()));
    publisher
        .publish("nobody/home", b"2", QoS::AtMostOnce, None, vec![])
        .await;
    let delivery = sub.expect_publish().await;
    w.expect_silence().await;
    sub.puback(delivery.pkid.unwrap()).await;
    let p = w.expect_publish().await;
    assert_eq!(
        (p.topic.as_str(), &p.payload[..]),
        ("ev/acked", &b"sub<pub"[..])
    );
    w.expect_silence().await;
}

/// A rule that republishes from `message.delivered` into a topic its own watcher
/// subscribes to does not feed on its own output: the delivery of what it republished
/// is an event about ITS message, which it does not republish from again (EMQX's
/// `republish_by` guard). One publish, one audit message — not a loop.
#[tokio::test]
async fn a_rule_republishing_deliveries_to_its_own_watcher_does_not_loop() {
    let rules = r#"
[rules.audit]
sql = '''SELECT clientid, topic FROM "$events/message/delivered" '''
actions = [{ function = "republish", args = { topic = "audit/${clientid}", payload = "${topic}", qos = 0 } }]
"#;
    let broker = start_broker(rules).await;
    let mut auditor = Client::connect(broker.addr, "auditor").await;
    auditor.subscribe(1, "audit/#", QoS::AtMostOnce).await;
    let mut sub = Client::connect(broker.addr, "sub").await;
    sub.subscribe(1, "x/#", QoS::AtMostOnce).await;
    let mut publisher = Client::connect(broker.addr, "pub").await;
    publisher
        .publish("x/1", b"m", QoS::AtMostOnce, None, vec![])
        .await;
    sub.expect_publish().await;
    let audit = auditor.expect_publish().await;
    assert_eq!(
        (audit.topic.as_str(), &audit.payload[..]),
        ("audit/sub", &b"x/1"[..])
    );
    // The audit message was delivered to the auditor — an event the rule sees and does
    // not republish from.
    auditor.expect_silence().await;
}

/// Across nodes: the publisher is on node A, the subscriber and the only rules on node
/// B. B's rules still name the publisher — A carries each publish's origin to B because
/// B asked for it — and report the delivery and its acknowledgement on B, where they
/// happen. A, which has no rules, raises nothing.
#[tokio::test]
async fn a_delivery_on_another_node_names_its_publisher() {
    let (a, b) = start_cluster(NONE, WATCH).await;
    let mut w = watcher(b.addr).await;
    let mut sub = connect(b.addr, "sub-b", Some("subuser"), false, vec![]).await;
    sub.subscribe_with_id(1, "t/x", QoS::AtLeastOnce, 9).await;
    // B's interest and its request for origins reach A over the link.
    let mut publisher = connect(a.addr, "pub-a", Some("pubuser"), false, vec![]).await;
    let mut tries = 0;
    let delivery = loop {
        publisher
            .publish(
                "t/x",
                b"across",
                QoS::AtLeastOnce,
                Some(1),
                vec![P::UserProperty("k".into(), "v".into())],
            )
            .await;
        assert_eq!(publisher.recv().await, Packet::PubAck(1.into()));
        if let Some(Packet::Publish(p)) = sub.try_recv().await {
            break p;
        }
        tries += 1;
        assert!(tries < 50, "B's interest never reached A");
    };
    // Publishes made before A had heard B's request carry no origin; wait for one that
    // does (the request is one frame behind B's interest).
    let mut delivery = delivery;
    let mut unnamed = 0;
    let delivered = loop {
        let delivered = next(&mut w, "message.delivered").await;
        if delivered["from_clientid"] == "pub-a" {
            break delivered;
        }
        assert_eq!(delivered["from_clientid"], "undefined", "{delivered}");
        unnamed += 1;
        assert!(unnamed < 20, "A never carried an origin to B: {delivered}");
        sub.puback(delivery.pkid.unwrap()).await;
        next(&mut w, "message.acked").await;
        publisher
            .publish(
                "t/x",
                b"across",
                QoS::AtLeastOnce,
                Some(1),
                vec![P::UserProperty("k".into(), "v".into())],
            )
            .await;
        assert_eq!(publisher.recv().await, Packet::PubAck(1.into()));
        delivery = sub.expect_publish().await;
    };
    assert_eq!(
        (
            &delivered["from_clientid"],
            &delivered["from_username"],
            &delivered["clientid"],
            &delivered["username"],
            &delivered["node"],
            &delivered["qos"],
            &delivered["payload"],
            &delivered["peerhost"],
        ),
        (
            &json!("pub-a"),
            &json!("pubuser"),
            &json!("sub-b"),
            &json!("subuser"),
            &json!("node-b"),
            &json!(1),
            &json!("across"),
            &json!("127.0.0.1"),
        )
    );
    assert_eq!(
        delivered["pub_props"],
        json!({
            "Subscription-Identifier": 9,
            "User-Property": {"k": "v"},
            "User-Property-Pairs": [{"key": "k", "value": "v"}],
        })
    );
    assert_eq!(delivered["id"].as_str().unwrap().len(), 32);
    assert!(delivered["publish_received_at"].as_i64() <= delivered["timestamp"].as_i64());
    sub.puback(delivery.pkid.unwrap()).await;
    let acked = next(&mut w, "message.acked").await;
    assert_eq!(
        (&acked["id"], &acked["from_username"], &acked["node"]),
        (&delivered["id"], &json!("pubuser"), &json!("node-b"))
    );
    no_more(&mut w).await;
}

/// `message.dropped` is raised on the node the publish ARRIVED at, and only when no
/// node has a subscriber: a publish on A for B's subscriber is not dropped, one for
/// nobody is — on A, by A's rules.
#[tokio::test]
async fn an_unrouted_publish_is_dropped_on_the_node_it_arrived_at() {
    let (a, b) = start_cluster(WATCH, WATCH).await;
    let mut w_a = watcher(a.addr).await;
    let mut w_b = Client::connect(b.addr, "watcher-b").await;
    w_b.subscribe(1, "ev/#", QoS::AtMostOnce).await;
    let mut sub = connect(b.addr, "sub-b", None, false, vec![]).await;
    sub.subscribe(1, "t/far", QoS::AtMostOnce).await;
    let mut publisher = connect(a.addr, "pub-a", Some("pubuser"), false, vec![]).await;
    // Until A has heard B's interest the publish reaches nobody; then it reaches B.
    let mut tries = 0;
    loop {
        publisher
            .publish("t/far", b"over", QoS::AtMostOnce, None, vec![])
            .await;
        if matches!(sub.try_recv().await, Some(Packet::Publish(_))) {
            break;
        }
        tries += 1;
        assert!(tries < 50, "B's interest never reached A");
    }
    // Drain what the warm-up raised, on both nodes (the events are forwarded like any
    // message, so each watcher hears both nodes' events).
    while w_a.try_recv().await.is_some() {}
    while w_b.try_recv().await.is_some() {}

    publisher
        .publish("t/far", b"routed", QoS::AtMostOnce, None, vec![])
        .await;
    assert_eq!(&sub.expect_publish().await.payload[..], b"routed");
    let delivered = next(&mut w_a, "message.delivered").await;
    assert_eq!(
        (
            &delivered["node"],
            &delivered["clientid"],
            &delivered["from_clientid"],
            &delivered["payload"]
        ),
        (
            &json!("node-b"),
            &json!("sub-b"),
            &json!("pub-a"),
            &json!("routed")
        )
    );
    no_more(&mut w_a).await;

    publisher
        .publish("t/nowhere", b"lost", QoS::AtMostOnce, None, vec![])
        .await;
    let dropped = next(&mut w_a, "message.dropped").await;
    assert_eq!(
        (
            &dropped["node"],
            &dropped["clientid"],
            &dropped["username"],
            &dropped["reason"],
            &dropped["payload"]
        ),
        (
            &json!("node-a"),
            &json!("pub-a"),
            &json!("pubuser"),
            &json!("no_subscribers"),
            &json!("lost")
        )
    );
    no_more(&mut w_a).await;
}
