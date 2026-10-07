//! The rule engine end to end (ADR 0083): real sockets, the real codec, the real hub.
//!
//! The `mqtt-rules` unit tests prove the SQL against EMQX's documented behaviour; these
//! prove the broker around it — that a rule's output is delivered at every `QoS`, that a
//! `QoS` 1/2 publisher's acknowledgement waits for what its rules produced and answers
//! with the original's own fate, that a refused original routes nothing it derived, that a
//! `QoS` 2 publish fires its rules once across a DUP resend, that a republished message can never
//! re-trigger a rule, that Wills and client events run rules, and that in a cluster each
//! message is evaluated exactly once — on the node it arrived at — while its derived
//! messages reach subscribers anywhere.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::Client;
use mqtt_cluster::NodeId;
use mqtt_codec::{Packet, QoS};
use mqtt_storage::MemorySessionStore;
use mqttd::hub::{BrownoutAxis, Hub, HubCommand};
use tokio::net::TcpListener;

/// A broker whose connections run `rules`.
struct Broker {
    addr: SocketAddr,
    hub_tx: tokio::sync::mpsc::UnboundedSender<HubCommand>,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
    /// What a reload sends the new rules through.
    rules_tx: tokio::sync::watch::Sender<Arc<mqtt_rules::RuleSet>>,
}

fn rule_set(text: &str) -> Arc<mqtt_rules::RuleSet> {
    Arc::new(
        mqtt_rules::RuleSet::parse(text)
            .unwrap_or_else(|e| panic!("test rules must load: {e}"))
            .rules,
    )
}

/// The production wiring in miniature: one store shared by the hub and the connections
/// (so `QoS` 2 dedup is the durable window), the rule engine on every connection and in
/// the hub (for Wills), metrics recorded.
async fn start_node(name: &str, rules: &str) -> (Broker, TcpListener, NodeId) {
    let store = Arc::new(MemorySessionStore::new());
    let id = NodeId(name.into());
    let (mut hub, hub_tx) = Hub::with_config(id.clone(), store.clone());
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("test"));
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());
    let (rules_tx, rx) = tokio::sync::watch::channel(rule_set(rules));
    let engine = mqttd::rules::Rules::new(rx, Arc::from(name), Some(metrics.clone()));
    hub_tx
        .send(HubCommand::AttachRules(engine.clone()))
        .unwrap();
    let policy = Arc::new(mqttd::conn::ConnPolicy {
        anonymous: None,
        auth: mqttd::conn::auth_handle(Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        })),
        authz: mqttd::conn::authz_handle(Arc::new(mqtt_auth::AllowAll)),
        identity_source: mqtt_auth::mtls::IdentitySource::default(),
        audit: Arc::new(mqtt_observability::AuditLog::new()),
        proxy: None,
        node: None,
        store: Some(store as Arc<dyn mqtt_storage::SessionStore>),
        connect_timeout: Duration::from_secs(10),
        enhanced: None,
        shutdown: None,
        metrics: Some(metrics.clone()),
        ingress: None,
        rules: Some(engine),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle_stream(
                stream,
                Some(peer),
                None,
                policy.clone(),
                accept_tx.clone(),
            ));
        }
    });
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    (
        Broker {
            addr,
            hub_tx,
            metrics,
            rules_tx,
        },
        peer,
        id,
    )
}

async fn start_broker(rules: &str) -> Broker {
    start_node("rules-test", rules).await.0
}

/// Two nodes running `rules`, linked into a full peer mesh.
async fn start_cluster(rules: &str) -> (Broker, Broker) {
    let (a, peer_a, id_a) = start_node("node-a", rules).await;
    let (b, peer_b, id_b) = start_node("node-b", rules).await;
    let (paddr_a, paddr_b) = (peer_a.local_addr().unwrap(), peer_b.local_addr().unwrap());
    tokio::spawn(mqttd::peer::serve_listener(
        peer_a,
        id_a.clone(),
        a.hub_tx.clone(),
        None,
        None,
        None,
    ));
    tokio::spawn(mqttd::peer::serve_listener(
        peer_b,
        id_b.clone(),
        b.hub_tx.clone(),
        None,
        None,
        None,
    ));
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

/// The next PUBLISH on `client`, skipping nothing: any other packet is a failure.
async fn next_publish(client: &mut Client) -> (String, Vec<u8>, QoS) {
    let p = client.expect_publish().await;
    (p.topic, p.payload.to_vec(), p.qos)
}

const ALERT: &str = r#"
[rules.alert]
sql = '''
SELECT payload.temp AS temp, clientid
FROM "sensors/+/data"
WHERE payload.temp > 30
'''
actions = [{ function = "republish", args = { topic = "alerts/${clientid}", payload = "${.}" } }]
"#;

/// The same rule selecting `qos`, so the republish inherits the publisher's: EMQX's
/// default `qos = "${qos}"` reads the rule's OUTPUT, not the input message — a rule that
/// does not select `qos` (as [`ALERT`] does not) republishes at `QoS` 0, in EMQX and here.
const ALERT_QOS: &str = r#"
[rules.alert]
sql = '''
SELECT payload.temp AS temp, clientid, qos
FROM "sensors/+/data"
WHERE payload.temp > 30
'''
actions = [{ function = "republish", args = { topic = "alerts/${clientid}", payload = "${temp}" } }]
"#;

/// A `QoS` 0 publish that passes the WHERE is transformed and republished; the original
/// is still delivered (a rule is a tap, never a filter); one that fails the WHERE
/// produces nothing. The per-rule metrics move.
#[tokio::test]
async fn a_matching_publish_is_transformed_and_republished_beside_the_original() {
    let broker = start_broker(ALERT).await;
    let mut sub = Client::connect(broker.addr, "sub").await;
    sub.subscribe(1, "alerts/#", QoS::AtMostOnce).await;
    sub.subscribe(2, "sensors/#", QoS::AtMostOnce).await;
    let mut publ = Client::connect(broker.addr, "dev7").await;

    publ.publish(
        "sensors/dev7/data",
        br#"{"temp":35}"#,
        QoS::AtMostOnce,
        None,
        vec![],
    )
    .await;
    // Order is the original, then what its rules produced: both enter the hub's FIFO
    // data lane from the same connection, original first.
    assert_eq!(
        next_publish(&mut sub).await,
        (
            "sensors/dev7/data".into(),
            br#"{"temp":35}"#.to_vec(),
            QoS::AtMostOnce
        )
    );
    assert_eq!(
        next_publish(&mut sub).await,
        (
            "alerts/dev7".into(),
            br#"{"temp":35,"clientid":"dev7"}"#.to_vec(),
            QoS::AtMostOnce
        )
    );

    publ.publish(
        "sensors/dev7/data",
        br#"{"temp":3}"#,
        QoS::AtMostOnce,
        None,
        vec![],
    )
    .await;
    assert_eq!(next_publish(&mut sub).await.0, "sensors/dev7/data");
    sub.expect_silence().await;

    let text = broker.metrics.render();
    assert!(
        text.contains(r#"mqttd_rule_evaluations_total{rule="alert",result="passed"} 1"#),
        "{text}"
    );
    assert!(
        text.contains(r#"mqttd_rule_evaluations_total{rule="alert",result="no_result"} 1"#),
        "{text}"
    );
    assert!(
        text.contains(r#"mqttd_rule_actions_total{rule="alert",result="ok"} 1"#),
        "{text}"
    );
}

/// `QoS` 1: a rule that selects `qos` republishes at the publisher's `QoS` (EMQX's
/// `${qos}` default), delivered at `QoS` 1 to a `QoS` 1 subscriber; the publisher is acked.
#[tokio::test]
async fn a_qos1_publish_acks_and_its_derived_message_is_qos1() {
    let broker = start_broker(ALERT_QOS).await;
    let mut sub = Client::connect(broker.addr, "sub1").await;
    sub.subscribe(1, "alerts/#", QoS::AtLeastOnce).await;
    let mut publ = Client::connect(broker.addr, "dev1").await;
    publ.publish(
        "sensors/dev1/data",
        br#"{"temp":40}"#,
        QoS::AtLeastOnce,
        Some(5),
        vec![],
    )
    .await;
    assert_eq!(publ.recv().await, Packet::PubAck(5.into()));
    let p = sub.expect_publish().await;
    assert_eq!(p.topic, "alerts/dev1");
    assert_eq!(p.qos, QoS::AtLeastOnce);
    sub.puback(p.pkid.unwrap()).await;
}

/// The `QoS` default trap, as in EMQX: `qos = "${qos}"` reads the rule's OUTPUT, so a
/// rule that does not select `qos` republishes at `QoS` 0 even from a `QoS` 1 publish —
/// a `QoS` 1 subscriber receives it at `QoS` 0. (The publisher is still acked.)
#[tokio::test]
async fn a_rule_that_does_not_select_qos_republishes_a_qos1_publish_at_qos0() {
    let broker = start_broker(ALERT).await;
    let mut sub = Client::connect(broker.addr, "sub0").await;
    sub.subscribe(1, "alerts/#", QoS::AtLeastOnce).await;
    let mut publ = Client::connect(broker.addr, "dev2").await;
    publ.publish(
        "sensors/dev2/data",
        br#"{"temp":40}"#,
        QoS::AtLeastOnce,
        Some(6),
        vec![],
    )
    .await;
    assert_eq!(publ.recv().await, Packet::PubAck(6.into()));
    let p = sub.expect_publish().await;
    assert_eq!(p.topic, "alerts/dev2");
    assert_eq!(p.qos, QoS::AtMostOnce, "not selected, so not inherited");
}

/// `QoS` 2 exactly-once inbound reaches what the rules produce: a DUP resend of a
/// PUBREC'd packet id is answered from the dedup window and NOT re-forwarded, so its
/// rules do not fire a second time.
#[tokio::test]
async fn a_qos2_publish_fires_its_rules_once_across_a_dup_resend() {
    let broker = start_broker(ALERT_QOS).await;
    let mut sub = Client::connect(broker.addr, "sub2").await;
    sub.subscribe(1, "alerts/#", QoS::ExactlyOnce).await;
    let mut publ = Client::connect(broker.addr, "dev2").await;
    for _ in 0..2 {
        publ.publish(
            "sensors/dev2/data",
            br#"{"temp":50}"#,
            QoS::ExactlyOnce,
            Some(9),
            vec![],
        )
        .await;
        assert_eq!(publ.recv().await, Packet::PubRec(9.into()));
    }
    publ.pubrel(9).await;
    assert_eq!(publ.recv().await, Packet::PubComp(9.into()));

    let p = sub.expect_publish().await;
    assert_eq!((p.topic.as_str(), p.qos), ("alerts/dev2", QoS::ExactlyOnce));
    let id = p.pkid.unwrap();
    sub.pubrec(id).await;
    assert_eq!(sub.recv().await, Packet::PubRel(id.into()));
    sub.pubcomp(id).await;
    sub.expect_silence().await;
}

fn action_count(broker: &Broker, rule: &str, result: &str) -> u64 {
    let text = broker.metrics.render();
    let key = format!(r#"mqttd_rule_actions_total{{rule="{rule}",result="{result}"}} "#);
    text.lines()
        .find_map(|l| l.strip_prefix(key.as_str()))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

/// A persistent `QoS` 1 subscriber on `filter`, then offline: its durable queue is the
/// only place a message for it can go, so a brownout refuses every such message.
async fn park_a_sleeper(broker: &Broker, id: &str, filter: &str) {
    let (mut sleeper, ack) = Client::connect_v5(
        broker.addr,
        id,
        false,
        vec![mqtt_codec::Property::SessionExpiryInterval(u32::MAX)],
    )
    .await;
    assert_eq!(ack.code, 0);
    assert_eq!(
        sleeper
            .subscribe(1, filter, QoS::AtLeastOnce)
            .await
            .return_codes,
        vec![1]
    );
    sleeper.disconnect().await;
}

fn brownout(broker: &Broker) {
    broker
        .hub_tx
        .send(HubCommand::SetBrownout {
            axis: BrownoutAxis::Disk,
            on: true,
        })
        .unwrap();
}

/// What an event derives is counted by its fate, not when it is sent: a presence
/// message the broker stores is `ok`, one it refuses (a brownout, and it needs storage
/// for an offline subscriber) is `failed`. Nobody else would ever learn of it — an
/// event answers no publisher.
#[tokio::test]
async fn an_event_derived_message_is_counted_by_its_fate() {
    const PRESENCE: &str = r#"
[rules.presence]
sql = 'SELECT clientid FROM "$events/client/disconnected"'
actions = [{ function = "republish", args = { topic = "presence/${clientid}", payload = "offline", qos = 1 } }]
"#;
    let broker = start_broker(PRESENCE).await;
    let counted = |result: &'static str, n: u64| {
        let broker = &broker;
        async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while action_count(broker, "presence", result) != n {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "presence {result}: expected {n}, have {}",
                    action_count(broker, "presence", result)
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };
    // The watcher's own disconnect raises one, stored for its now-offline session.
    park_a_sleeper(&broker, "watcher", "presence/#").await;
    counted("ok", 1).await;
    let mut a = Client::connect(broker.addr, "dev-a").await;
    a.disconnect().await;
    counted("ok", 2).await;

    let mut b = Client::connect(broker.addr, "dev-b").await;
    brownout(&broker);
    b.disconnect().await;
    counted("failed", 1).await;
    assert_eq!(
        action_count(&broker, "presence", "ok"),
        2,
        "refused, so not ok"
    );
}

/// A derived message the broker refuses (brownout: it needs storage) fails its action;
/// the original — which needed none, and was delivered — is still acknowledged, every
/// time. Withholding it instead would have the publisher re-send, and re-deliver, the
/// original for as long as the brownout lasts.
#[tokio::test]
async fn a_refused_derived_message_fails_its_action_and_the_original_is_still_acked() {
    let broker = start_broker(ALERT_QOS).await;
    park_a_sleeper(&broker, "sleeper", "alerts/#").await;
    let mut publ = Client::connect_v5_ok(broker.addr, "dev3").await;
    let publish = |pkid: u16, temp: u8| (pkid, format!(r#"{{"temp":{temp}}}"#));

    // Control: before the brownout the derived message is stored and counted `ok`.
    let (pkid, payload) = publish(1, 31);
    publ.publish(
        "sensors/dev3/data",
        payload.as_bytes(),
        QoS::AtLeastOnce,
        Some(pkid),
        vec![],
    )
    .await;
    match publ.recv().await {
        Packet::PubAck(a) => assert_eq!((a.pkid, a.reason), (1, 0)),
        other => panic!("expected PUBACK, got {other:?}"),
    }
    assert_eq!(action_count(&broker, "alert", "ok"), 1);

    brownout(&broker);
    for (pkid, temp) in [(2, 32), (3, 33)] {
        let (pkid, payload) = publish(pkid, temp);
        publ.publish(
            "sensors/dev3/data",
            payload.as_bytes(),
            QoS::AtLeastOnce,
            Some(pkid),
            vec![],
        )
        .await;
        match publ.recv().await {
            Packet::PubAck(a) => assert_eq!(
                (a.pkid, a.reason),
                (pkid, 0),
                "the original stored nothing and was accepted: acked"
            ),
            other => panic!("expected PUBACK, got {other:?}"),
        }
    }
    assert_eq!(
        action_count(&broker, "alert", "failed"),
        2,
        "each refused derived message is a failed action"
    );
    assert_eq!(action_count(&broker, "alert", "ok"), 1);
}

/// A refused original routes none of its derived messages: the hub decides the original
/// first and drops what its rules produced, so a resend cannot duplicate them. Here the
/// original needs storage (a parked subscriber) and the derived message does not (a live
/// `QoS` 0 subscriber), so only this ordering keeps the alert from going out.
#[tokio::test]
async fn a_refused_original_routes_none_of_its_derived_messages() {
    let broker = start_broker(ALERT_QOS).await;
    park_a_sleeper(&broker, "keeper", "sensors/#").await;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    watcher.subscribe(1, "alerts/#", QoS::AtMostOnce).await;
    let mut publ = Client::connect_v5_ok(broker.addr, "dev4").await;

    brownout(&broker);
    publ.publish(
        "sensors/dev4/data",
        br#"{"temp":34}"#,
        QoS::AtLeastOnce,
        Some(1),
        vec![],
    )
    .await;
    match publ.recv().await {
        Packet::PubAck(a) => assert_eq!(
            (a.pkid, a.reason),
            (1, 0x97),
            "the original is refused, and v5 is told so"
        ),
        other => panic!("expected PUBACK, got {other:?}"),
    }
    watcher.expect_silence().await;
    assert_eq!(action_count(&broker, "alert", "failed"), 1);
    assert_eq!(action_count(&broker, "alert", "ok"), 0);
}

/// A connection reads the rules through its own cached view, so a reload has to reach
/// connections that were already open: the same publisher, before and after.
#[tokio::test]
async fn a_reload_reaches_connections_that_were_already_open() {
    let broker = start_broker("").await;
    let mut sub = Client::connect(broker.addr, "sub-reload").await;
    sub.subscribe(1, "alerts/#", QoS::AtMostOnce).await;
    let mut publ = Client::connect(broker.addr, "dev9").await;
    publ.publish(
        "sensors/dev9/data",
        br#"{"temp":40}"#,
        QoS::AtMostOnce,
        None,
        vec![],
    )
    .await;
    sub.expect_silence().await;

    broker.rules_tx.send(rule_set(ALERT)).unwrap();
    publ.publish(
        "sensors/dev9/data",
        br#"{"temp":41}"#,
        QoS::AtMostOnce,
        None,
        vec![],
    )
    .await;
    assert_eq!(next_publish(&mut sub).await.0, "alerts/dev9");

    broker.rules_tx.send(rule_set("")).unwrap();
    publ.publish(
        "sensors/dev9/data",
        br#"{"temp":42}"#,
        QoS::AtMostOnce,
        None,
        vec![],
    )
    .await;
    sub.expect_silence().await;
}

/// An idle connection lets go of a superseded rule set when it pings, instead of
/// holding it until it next publishes: a series of reloads must not keep a series of
/// old rule sets alive in idle connections.
#[tokio::test]
async fn an_idle_connection_releases_a_superseded_rule_set_when_it_pings() {
    async fn ping(c: &mut Client) {
        c.send(&Packet::PingReq).await;
        match c.recv().await {
            Packet::PingResp => {}
            other => panic!("expected PINGRESP, got {other:?}"),
        }
    }
    let broker = start_broker(ALERT).await;
    let old = broker.rules_tx.borrow().clone();
    let mut idle = Client::connect(broker.addr, "idle").await;
    ping(&mut idle).await;
    broker.rules_tx.send(rule_set("")).unwrap();
    ping(&mut idle).await;
    // The PINGRESP follows the refresh, so by now only this test holds the old set.
    assert_eq!(
        Arc::strong_count(&old),
        1,
        "the idle connection still holds the superseded rule set"
    );
}

/// A republished message never re-enters the rule engine (EMQX's `direct_dispatch`,
/// always on): a rule that republishes into its own FROM produces exactly one message,
/// not a loop.
#[tokio::test]
async fn a_rule_republishing_into_its_own_from_cannot_loop() {
    let broker = start_broker(
        r#"
        [rules.echo]
        sql = 'SELECT topic FROM "loop/#"'
        actions = [{ function = "republish", args = { topic = "loop/${topic}" } }]
        "#,
    )
    .await;
    let mut sub = Client::connect(broker.addr, "sub-loop").await;
    sub.subscribe(1, "loop/#", QoS::AtMostOnce).await;
    let mut publ = Client::connect(broker.addr, "pub-loop").await;
    publ.publish("loop/a", b"x", QoS::AtMostOnce, None, vec![])
        .await;
    assert_eq!(next_publish(&mut sub).await.0, "loop/a");
    assert_eq!(next_publish(&mut sub).await.0, "loop/loop/a");
    sub.expect_silence().await;
}

/// FOREACH fans one publish out into one message per array element.
#[tokio::test]
async fn foreach_fans_one_publish_out() {
    let broker = start_broker(
        r#"
        [rules.split]
        sql = 'FOREACH payload.readings AS r DO r.id AS id, r.v AS v FROM "batch/+"'
        actions = [{ function = "republish", args = { topic = "readings/${id}", payload = "${v}" } }]
        "#,
    )
    .await;
    let mut sub = Client::connect(broker.addr, "sub-fe").await;
    sub.subscribe(1, "readings/#", QoS::AtMostOnce).await;
    let mut publ = Client::connect(broker.addr, "pub-fe").await;
    publ.publish(
        "batch/1",
        br#"{"readings":[{"id":"a","v":1},{"id":"b","v":2.5},{"id":"c","v":"x"}]}"#,
        QoS::AtMostOnce,
        None,
        vec![],
    )
    .await;
    assert_eq!(
        next_publish(&mut sub).await,
        ("readings/a".into(), b"1".to_vec(), QoS::AtMostOnce)
    );
    assert_eq!(
        next_publish(&mut sub).await,
        ("readings/b".into(), b"2.5".to_vec(), QoS::AtMostOnce)
    );
    assert_eq!(
        next_publish(&mut sub).await,
        ("readings/c".into(), b"x".to_vec(), QoS::AtMostOnce)
    );
    sub.expect_silence().await;
}

const PRESENCE: &str = r#"
[rules.presence]
sql = '''SELECT clientid, event, reason, topic FROM "$events/client/connected", "$events/client_disconnected", "$events/session/subscribed"'''
actions = [{ function = "republish", args = { topic = "presence/${clientid}", payload = "${.}", qos = 1 } }]
"#;

/// Client events run rules: connected, subscribed and disconnected (with EMQX's
/// `normal` reason for a clean DISCONNECT), in that order.
#[tokio::test]
async fn client_events_run_rules() {
    let broker = start_broker(PRESENCE).await;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    // The watcher's own subscribe fires `session/subscribed` for itself — after its
    // SUBACK, so it may or may not arrive before the subscription is in place.
    watcher
        .subscribe(1, "presence/dev9", QoS::AtLeastOnce)
        .await;
    let mut dev = Client::connect(broker.addr, "dev9").await;
    let mut events = Vec::new();
    let p = watcher.expect_publish().await;
    events.push(String::from_utf8(p.payload.to_vec()).unwrap());
    watcher.puback(p.pkid.unwrap()).await;
    dev.subscribe(1, "cmd/dev9", QoS::AtMostOnce).await;
    let p = watcher.expect_publish().await;
    events.push(String::from_utf8(p.payload.to_vec()).unwrap());
    watcher.puback(p.pkid.unwrap()).await;
    dev.disconnect().await;
    let p = watcher.expect_publish().await;
    events.push(String::from_utf8(p.payload.to_vec()).unwrap());
    assert_eq!(
        events,
        [
            r#"{"clientid":"dev9","event":"client.connected","reason":"undefined","topic":"undefined"}"#,
            r#"{"clientid":"dev9","event":"session.subscribed","reason":"undefined","topic":"cmd/dev9"}"#,
            r#"{"clientid":"dev9","event":"client.disconnected","reason":"normal","topic":"undefined"}"#,
        ]
    );
}

/// A v5 client that ends with a non-zero DISCONNECT reason is reported with EMQX's name
/// for that reason code — here `0x04 Disconnect with Will Message` — not `normal`, which
/// EMQX keeps for `0x00` (`emqx_channel:disconnect_reason/1`).
#[tokio::test]
async fn a_disconnect_reason_code_is_reported_by_its_emqx_name() {
    let broker = start_broker(PRESENCE).await;
    let mut watcher = Client::connect(broker.addr, "watcher-rc").await;
    watcher
        .subscribe(1, "presence/dev10", QoS::AtLeastOnce)
        .await;
    let mut dev = Client::connect_v5_ok(broker.addr, "dev10").await;
    let p = watcher.expect_publish().await;
    watcher.puback(p.pkid.unwrap()).await;
    dev.send(&Packet::Disconnect(mqtt_codec::packet::Disconnect {
        reason: 0x04,
        properties: mqtt_codec::Properties::default(),
    }))
    .await;
    dev.expect_closed().await;
    let p = watcher.expect_publish().await;
    assert_eq!(
        String::from_utf8(p.payload.to_vec()).unwrap(),
        r#"{"clientid":"dev10","event":"client.disconnected","reason":"disconnect_with_will_message","topic":"undefined"}"#
    );
}

/// A Will is a publish to the rule engine (as in EMQX): the hub publishes it on an
/// ungraceful end, and evaluates it.
#[tokio::test]
async fn a_will_runs_rules_when_the_hub_publishes_it() {
    let broker = start_broker(
        r#"
        [rules.offline]
        sql = 'SELECT clientid, payload FROM "devices/+/status"'
        actions = [{ function = "republish", args = { topic = "fleet/${clientid}", payload = "${payload}" } }]
        "#,
    )
    .await;
    let mut sub = Client::connect(broker.addr, "fleet").await;
    sub.subscribe(1, "fleet/#", QoS::AtMostOnce).await;
    let mut dev = Client::open(broker.addr, mqtt_codec::ProtocolVersion::V311).await;
    dev.connect_with_will("dev-w", "devices/dev-w/status", b"offline")
        .await;
    drop(dev); // ungraceful: the Will is owed
    assert_eq!(
        next_publish(&mut sub).await,
        ("fleet/dev-w".into(), b"offline".to_vec(), QoS::AtMostOnce)
    );
}

/// Cluster: rules run on the node a message ARRIVED at, exactly once. A subscriber on
/// the other node receives the original (forwarded) and ONE derived copy — the
/// forwarded original is not re-evaluated there, though that node runs the same rules.
#[tokio::test]
async fn in_a_cluster_each_message_is_evaluated_once_on_its_landing_node() {
    let (a, b) = start_cluster(ALERT).await;
    let mut sub = Client::connect(b.addr, "sub-b").await;
    sub.subscribe(1, "alerts/#", QoS::AtMostOnce).await;
    sub.subscribe(2, "sensors/#", QoS::AtMostOnce).await;
    let mut publ = Client::connect(a.addr, "dev-a").await;
    // Interest reaches node A by gossip. Warm up until an ORIGINAL lands on node B (its
    // `sensors/#` interest was subscribed after `alerts/#`, and interest travels as a
    // full snapshot, so both are known to A by then), with a bound.
    let mut warm = false;
    for _ in 0..50 {
        publ.publish(
            "sensors/dev-a/data",
            br#"{"temp":1}"#,
            QoS::AtMostOnce,
            None,
            vec![],
        )
        .await;
        if let Some(Packet::Publish(p)) = sub.try_recv().await {
            if p.topic == "sensors/dev-a/data" {
                warm = true;
                break;
            }
        }
    }
    assert!(warm, "node B's interest never reached node A");
    // A cold reading produces no derived message; drain whatever is still in flight.
    while sub.try_recv().await.is_some() {}

    // Five hot readings: exactly five originals and five derived copies. A node B that
    // re-evaluated the forwarded originals would deliver ten derived copies.
    for _ in 0..5 {
        publ.publish(
            "sensors/dev-a/data",
            br#"{"temp":99}"#,
            QoS::AtMostOnce,
            None,
            vec![],
        )
        .await;
    }
    let (mut originals, mut derived) = (0, 0);
    while let Some(p) = sub.try_recv().await {
        let Packet::Publish(p) = p else {
            panic!("unexpected {p:?}")
        };
        match p.topic.as_str() {
            "sensors/dev-a/data" => originals += 1,
            "alerts/dev-a" => {
                assert_eq!(&p.payload[..], br#"{"temp":99,"clientid":"dev-a"}"#);
                derived += 1;
            }
            other => panic!("unexpected topic {other}"),
        }
    }
    assert_eq!((originals, derived), (5, 5));
    let b_metrics = b.metrics.render();
    assert!(
        !b_metrics.contains(r#"mqttd_rule_evaluations_total{rule="alert""#),
        "node B never evaluated a forwarded copy: {b_metrics}"
    );
}
