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
    start_node_with(
        name,
        rules,
        Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        }),
        Arc::new(mqtt_auth::AllowAll),
    )
    .await
}

/// [`start_node`] with the given authenticator and authorizer, the listener's address
/// passed on as production's listeners pass it (ADR 0083's `sockname`).
async fn start_node_with(
    name: &str,
    rules: &str,
    auth: Arc<dyn mqtt_auth::Authenticator>,
    authz: Arc<dyn mqtt_auth::Authorizer>,
) -> (Broker, TcpListener, NodeId) {
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
        auth: mqttd::conn::auth_handle(auth),
        authz: mqttd::conn::authz_handle(authz),
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

/// What an event derives is routed ungated, as a Will is, so it holds no entry in the
/// table of publishes awaiting acknowledgement (a burst of events cannot crowd a
/// client's publish out of it), and its action is counted `ok` once the hub has routed
/// it. A durable copy a brownout refuses is a drop like a Will's: counted in
/// `mqttd_publish_dropped_total{reason="brownout"}`, never hidden.
#[tokio::test]
async fn an_event_derived_message_is_counted_when_routed_and_a_refused_copy_as_a_drop() {
    const PRESENCE: &str = r#"
[rules.presence]
sql = 'SELECT clientid FROM "$events/client/disconnected"'
actions = [{ function = "republish", args = { topic = "presence/${clientid}", payload = "offline", qos = 1 } }]
"#;
    let broker = start_broker(PRESENCE).await;
    let brownout_drops = |broker: &Broker| {
        broker
            .metrics
            .render()
            .lines()
            .find_map(|l| l.strip_prefix(r#"mqttd_publish_dropped_total{reason="brownout"} "#))
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    let counted = |n: u64, drops: u64| {
        let broker = &broker;
        async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while (
                action_count(broker, "presence", "ok"),
                brownout_drops(broker),
            ) != (n, drops)
            {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "expected {n} ok and {drops} brownout drops, have {} and {}",
                    action_count(broker, "presence", "ok"),
                    brownout_drops(broker)
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };
    // The watcher's own disconnect raises one, stored for its now-offline session.
    park_a_sleeper(&broker, "watcher", "presence/#").await;
    counted(1, 0).await;
    let mut a = Client::connect(broker.addr, "dev-a").await;
    a.disconnect().await;
    counted(2, 0).await;

    let mut b = Client::connect(broker.addr, "dev-b").await;
    brownout(&broker);
    b.disconnect().await;
    counted(3, 1).await;
    assert_eq!(action_count(&broker, "presence", "failed"), 0);
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

/// Admits a username with the password `pw`, or no credentials; refuses the rest.
struct Passwords;

#[async_trait::async_trait]
impl mqtt_auth::Authenticator for Passwords {
    async fn authenticate(
        &self,
        _client: &mqtt_core::ClientId,
        creds: &mqtt_auth::Credentials<'_>,
    ) -> Result<mqtt_auth::Identity, mqtt_auth::AuthError> {
        let subject = match creds {
            mqtt_auth::Credentials::Password { username, password } if *password == b"pw" => {
                (*username).to_string()
            }
            mqtt_auth::Credentials::Anonymous => "anonymous".into(),
            _ => return Err(mqtt_auth::AuthError::Rejected),
        };
        Ok(mqtt_auth::Identity {
            subject,
            groups: vec![],
        })
    }
}

/// Every event a client named `dev-…` raises, republished to `ev/<event>` as JSON.
/// (`$events/#` also matches events mqttd does not raise, which the load warns about.)
const EVERY_EVENT: &str = r#"
[rules.events]
sql = '''SELECT * FROM "$events/#" WHERE regex_match(clientid, '^dev-')'''
actions = [{ function = "republish", args = { topic = "ev/${event}", payload = "${.}" } }]
"#;

/// The next event the watcher hears, as JSON, checked against its topic.
async fn next_event(watcher: &mut Client) -> serde_json::Value {
    let p = watcher.expect_publish().await;
    let v: serde_json::Value = serde_json::from_slice(&p.payload).unwrap();
    assert_eq!(
        p.topic,
        format!("ev/{}", v["event"].as_str().unwrap()),
        "{v}"
    );
    v
}

/// An event's field names, sorted. (`SELECT *` adds `metadata`, as EMQX's does.)
fn field_names(v: &serde_json::Value) -> Vec<&str> {
    let mut names: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    names.sort_unstable();
    names
}

fn props(p: Vec<mqtt_codec::Property>) -> mqtt_codec::Properties {
    mqtt_codec::Properties(p)
}

/// A connection's whole life, as EMQX's rule events show it (`emqx_rule_events`
/// `eventmsg_*`): authentication, `client.connected` then `client.connack` (EMQX's
/// order), the subscription's authorization and `session.subscribed`, a PINGREQ, an
/// unsubscribe and a DISCONNECT — each with the properties its packet carried, printed
/// as `printable_props/1` prints them, the listener's address as `sockname`, and the
/// connection's own Receive Maximum and Session Expiry Interval (seconds on
/// `client.connected`, milliseconds on `client.connack` and `client.ping`).
// One connection's whole life, one packet and its events after another.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn a_connection_s_events_carry_emqx_s_fields_end_to_end() {
    use mqtt_codec::Property as P;
    let broker = start_node_with(
        "rules-test",
        EVERY_EVENT,
        Arc::new(Passwords),
        Arc::new(mqtt_auth::AllowAll),
    )
    .await
    .0;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    watcher.subscribe(1, "ev/#", QoS::AtMostOnce).await;

    let mut dev = Client::open(broker.addr, mqtt_codec::ProtocolVersion::V5).await;
    dev.send(&Packet::Connect(mqtt_codec::packet::Connect {
        protocol: mqtt_codec::ProtocolVersion::V5,
        clean_session: true,
        keep_alive: 30,
        client_id: "dev-1".into(),
        last_will: None,
        username: Some("u".into()),
        password: Some(bytes::Bytes::from_static(b"pw")),
        properties: props(vec![
            P::SessionExpiryInterval(7200),
            P::ReceiveMaximum(10),
            P::UserProperty("k".into(), "v".into()),
        ]),
    }))
    .await;
    assert!(matches!(dev.recv().await, Packet::ConnAck(a) if a.code == 0));
    let sockname = serde_json::json!(broker.addr.to_string());
    let conn_props = serde_json::json!({
        "Session-Expiry-Interval": 7200,
        "Receive-Maximum": 10,
        "User-Property": {"k": "v"},
        "User-Property-Pairs": [{"key": "k", "value": "v"}],
    });

    let authentication = next_event(&mut watcher).await;
    assert_eq!(authentication["event"], "client.check_authn_complete");
    assert_eq!(
        (
            &authentication["reason_code"],
            &authentication["is_anonymous"],
            &authentication["is_superuser"]
        ),
        (&"success".into(), &false.into(), &false.into())
    );
    assert_eq!(authentication["username"], "u");

    let connected = next_event(&mut watcher).await;
    assert_eq!(connected["event"], "client.connected");
    assert_eq!(
        field_names(&connected),
        [
            "clean_start",
            "client_attrs",
            "clientid",
            "conn_props",
            "connected_at",
            "event",
            "expiry_interval",
            "is_bridge",
            "keepalive",
            "metadata",
            "node",
            "peername",
            "proto_name",
            "proto_ver",
            "receive_maximum",
            "sockname",
            "timestamp",
            "username"
        ]
    );
    assert_eq!(connected["sockname"], sockname);
    assert_eq!(connected["conn_props"], conn_props);
    assert_eq!(connected["receive_maximum"], 10);
    assert_eq!(connected["expiry_interval"], 7200);
    assert_eq!(connected["keepalive"], 30);
    assert_eq!(
        (&connected["proto_name"], &connected["proto_ver"]),
        (&"MQTT".into(), &5.into())
    );
    assert_eq!(connected["is_bridge"], false);

    let connack = next_event(&mut watcher).await;
    assert_eq!(connack["event"], "client.connack");
    assert_eq!(connack["reason_code"], "success");
    assert_eq!(connack["expiry_interval"], 7_200_000, "EMQX's milliseconds");
    assert_eq!(connack["connected_at"], connected["connected_at"]);
    assert_eq!(connack["conn_props"], conn_props);
    assert_eq!(connack["sockname"], sockname);

    dev.send(&Packet::Subscribe(mqtt_codec::packet::Subscribe {
        pkid: 1,
        filters: vec![mqtt_codec::packet::SubscribeFilter {
            path: "cmd/dev-1".into(),
            qos: QoS::AtLeastOnce,
            options: mqtt_codec::SubscriptionOptions::default(),
        }],
        properties: props(vec![
            P::SubscriptionIdentifier(5),
            P::UserProperty("s".into(), "1".into()),
        ]),
    }))
    .await;
    assert!(matches!(dev.recv().await, Packet::SubAck(_)));
    let authz = next_event(&mut watcher).await;
    assert_eq!(authz["event"], "client.check_authz_complete");
    assert_eq!(
        (
            &authz["topic"],
            &authz["action"],
            &authz["result"],
            &authz["authz_source"]
        ),
        (
            &"cmd/dev-1".into(),
            &"subscribe".into(),
            &"allow".into(),
            &"default".into()
        ),
        "no ACL file: authorization.no_match decided"
    );
    let subscribed = next_event(&mut watcher).await;
    assert_eq!(subscribed["event"], "session.subscribed");
    assert_eq!(subscribed["qos"], 1);
    assert_eq!(
        subscribed["sub_props"],
        serde_json::json!({
            "Subscription-Identifier": 5,
            "User-Property": {"s": "1"},
            "User-Property-Pairs": [{"key": "s", "value": "1"}],
        })
    );
    assert_eq!(subscribed["peerhost"], "127.0.0.1");

    dev.send(&Packet::PingReq).await;
    assert_eq!(dev.recv().await, Packet::PingResp);
    let ping = next_event(&mut watcher).await;
    assert_eq!(ping["event"], "client.ping");
    assert_eq!(
        field_names(&ping),
        [
            "clean_start",
            "clientid",
            "conn_props",
            "event",
            "expiry_interval",
            "keepalive",
            "metadata",
            "node",
            "peername",
            "proto_name",
            "proto_ver",
            "sockname",
            "timestamp",
            "username"
        ]
    );
    assert_eq!(ping["expiry_interval"], 7_200_000);

    dev.send(&Packet::Unsubscribe(mqtt_codec::packet::Unsubscribe {
        pkid: 2,
        filters: vec!["cmd/dev-1".into()],
        properties: props(vec![P::UserProperty("u".into(), "2".into())]),
    }))
    .await;
    assert!(matches!(dev.recv().await, Packet::UnsubAck(_)));
    let unsubscribed = next_event(&mut watcher).await;
    assert_eq!(unsubscribed["event"], "session.unsubscribed");
    assert_eq!(unsubscribed["qos"], 1, "the QoS the subscription had");
    assert_eq!(
        unsubscribed["unsub_props"],
        serde_json::json!({
            "User-Property": {"u": "2"},
            "User-Property-Pairs": [{"key": "u", "value": "2"}],
        })
    );

    dev.disconnect_with(vec![
        P::ReasonString("bye".into()),
        P::UserProperty("d".into(), "3".into()),
    ])
    .await;
    let disconnected = next_event(&mut watcher).await;
    assert_eq!(disconnected["event"], "client.disconnected");
    assert_eq!(disconnected["reason"], "normal");
    assert_eq!(
        disconnected["disconn_props"],
        serde_json::json!({
            "Reason-String": "bye",
            "User-Property": {"d": "3"},
            "User-Property-Pairs": [{"key": "d", "value": "3"}],
        })
    );
    assert_eq!(
        (
            &disconnected["proto_name"],
            &disconnected["proto_ver"],
            &disconnected["sockname"]
        ),
        (&"MQTT".into(), &5.into(), &sockname)
    );
    watcher.expect_silence().await;
}

/// A refused CONNECT raises `client.connack` with EMQX's name for the reason
/// (`emqx_reason_codes:name/1`, the MQTT 5 name for an MQTT 3.1.1 client too), and a
/// refused authentication `client.check_authn_complete` before it; a client id refused
/// before authentication raises no authentication event, as in EMQX's
/// `process_connect/2` pipeline.
#[tokio::test]
async fn a_refused_connect_raises_its_connack_and_authentication_events() {
    let broker = start_node_with(
        "rules-test",
        r#"
        [rules.refused]
        sql = '''SELECT event, clientid, reason_code, is_anonymous FROM "$events/client/connack", "$events/auth/check_authn_complete" WHERE reason_code <> 'success' '''
        actions = [{ function = "republish", args = { topic = "ev/${event}", payload = "${.}" } }]
        "#,
        Arc::new(Passwords),
        Arc::new(mqtt_auth::AllowAll),
    )
    .await
    .0;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    watcher.subscribe(1, "ev/#", QoS::AtMostOnce).await;

    let mut dev = Client::open(broker.addr, mqtt_codec::ProtocolVersion::V311).await;
    dev.send(&Packet::Connect(mqtt_codec::packet::Connect {
        protocol: mqtt_codec::ProtocolVersion::V311,
        clean_session: true,
        keep_alive: 0,
        client_id: "dev-2".into(),
        last_will: None,
        username: Some("u".into()),
        password: Some(bytes::Bytes::from_static(b"wrong")),
        properties: mqtt_codec::Properties::new(),
    }))
    .await;
    assert!(matches!(dev.recv().await, Packet::ConnAck(a) if a.code == 0x04));
    assert_eq!(
        next_event(&mut watcher).await,
        serde_json::json!({"event": "client.check_authn_complete", "clientid": "dev-2",
            "reason_code": "bad_username_or_password", "is_anonymous": false})
    );
    assert_eq!(
        next_event(&mut watcher).await,
        serde_json::json!({"event": "client.connack", "clientid": "dev-2",
            "reason_code": "bad_username_or_password", "is_anonymous": "undefined"})
    );

    let (_, ack) = Client::connect_v5(broker.addr, "", false, vec![]).await;
    assert_eq!(ack.code, 0x85);
    assert_eq!(
        next_event(&mut watcher).await,
        serde_json::json!({"event": "client.connack", "clientid": "",
            "reason_code": "client_identifier_not_valid", "is_anonymous": "undefined"})
    );
    watcher.expect_silence().await;
}

/// The next `gone/<client id>` and its reason.
async fn gone(watcher: &mut Client) -> (String, String) {
    let p = watcher.expect_publish().await;
    (p.topic, String::from_utf8(p.payload.to_vec()).unwrap())
}

/// How a connection ended, in EMQX's words (`emqx_channel`): `takenover` when the same
/// client id connects again without clean start, `discarded` when it does with one,
/// `kicked` for an operator's kick, the reason code's name for a protocol violation
/// (`topic_alias_invalid`), `tcp_closed` for a socket the client closed.
#[tokio::test]
async fn a_closed_connection_reports_emqx_s_reason() {
    let broker = start_broker(
        r#"
        [rules.gone]
        sql = 'SELECT clientid, reason FROM "$events/client/disconnected"'
        actions = [{ function = "republish", args = { topic = "gone/${clientid}", payload = "${reason}" } }]
        "#,
    )
    .await;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    watcher.subscribe(1, "gone/#", QoS::AtMostOnce).await;

    let (mut first, _) = Client::connect_v311(broker.addr, "dev-t", false).await;
    let (mut second, _) = Client::connect_v311(broker.addr, "dev-t", false).await;
    first.expect_closed().await;
    assert_eq!(
        gone(&mut watcher).await,
        ("gone/dev-t".into(), "takenover".into())
    );
    let _third = Client::connect(broker.addr, "dev-t").await;
    second.expect_closed().await;
    assert_eq!(
        gone(&mut watcher).await,
        ("gone/dev-t".into(), "discarded".into())
    );

    let mut kicked = Client::connect(broker.addr, "dev-k").await;
    let (reply, outcome) = tokio::sync::oneshot::channel();
    broker
        .hub_tx
        .send(HubCommand::Admin(mqttd::hub::admin::AdminRequest::Kick {
            client: "dev-k".into(),
            reply,
        }))
        .unwrap();
    assert!(outcome.await.unwrap().disconnected);
    kicked.expect_closed().await;
    assert_eq!(
        gone(&mut watcher).await,
        ("gone/dev-k".into(), "kicked".into())
    );

    let mut bad = Client::connect_v5_ok(broker.addr, "dev-p").await;
    bad.publish(
        "",
        b"x",
        QoS::AtMostOnce,
        None,
        vec![mqtt_codec::Property::TopicAlias(9999)],
    )
    .await;
    bad.expect_disconnect(0x94).await;
    assert_eq!(
        gone(&mut watcher).await,
        ("gone/dev-p".into(), "topic_alias_invalid".into())
    );

    let dropped = Client::connect(broker.addr, "dev-e").await;
    drop(dropped);
    assert_eq!(
        gone(&mut watcher).await,
        ("gone/dev-e".into(), "tcp_closed".into())
    );
}

/// EMQX evaluates a publish's rules on `emqx_message:clean_dup(Msg)`, so a rule reads
/// `flags.dup` as `false` even for a PUBLISH that set it.
#[tokio::test]
async fn a_rule_reads_the_dup_flag_as_false() {
    let broker = start_broker(
        r#"
        [rules.flags]
        sql = 'SELECT flags FROM "d/#"'
        actions = [{ function = "republish", args = { topic = "flags", payload = "${.}" } }]
        "#,
    )
    .await;
    let mut sub = Client::connect(broker.addr, "sub-dup").await;
    sub.subscribe(1, "flags", QoS::AtMostOnce).await;
    let mut publ = Client::connect(broker.addr, "pub-dup").await;
    publ.send(&Packet::Publish(mqtt_codec::packet::Publish {
        dup: true,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "d/1".into(),
        pkid: Some(7),
        payload: bytes::Bytes::from_static(b"x"),
        properties: mqtt_codec::Properties::new(),
    }))
    .await;
    assert_eq!(publ.recv().await, Packet::PubAck(7.into()));
    assert_eq!(
        next_publish(&mut sub).await,
        (
            "flags".into(),
            br#"{"flags":{"dup":false,"retain":false}}"#.to_vec(),
            QoS::AtMostOnce
        )
    );
}

/// `client.check_authz_complete` for every publish (the Will's, at CONNECT, included) and
/// subscribe: `authz_source` is `file` when a rule of the ACL file decided and `default`
/// when none matched (EMQX's `authorization.no_match`); a `$SYS` publish, which the broker
/// refuses whatever the ACL says, is a `default` denial.
#[tokio::test]
async fn authorization_events_name_the_acl_file_or_the_default() {
    let acl = mqtt_auth::acl::AclPolicy::from_toml_str(
        r#"
        [[rules]]
        actions = ["publish", "subscribe"]
        topics = ["ok/#", "authz/#"]
        "#,
    )
    .unwrap();
    let broker = start_node_with(
        "rules-test",
        r#"
        [rules.authz]
        sql = '''SELECT topic, action, authz_source, result FROM "$events/auth/check_authz_complete" WHERE clientid = 'dev-z' '''
        actions = [{ function = "republish", args = { topic = "authz/x", payload = "${.}" } }]
        "#,
        Arc::new(mqtt_auth::basic::BasicAuthenticator {
            allow_anonymous: true,
        }),
        Arc::new(acl),
    )
    .await
    .0;
    let mut watcher = Client::connect(broker.addr, "watcher").await;
    watcher.subscribe(1, "authz/#", QoS::AtMostOnce).await;
    let mut dev = Client::open(broker.addr, mqtt_codec::ProtocolVersion::V311).await;
    dev.connect_with_will("dev-z", "ok/will", b"bye").await;
    dev.publish("ok/1", b"x", QoS::AtMostOnce, None, vec![])
        .await;
    dev.publish("no/1", b"x", QoS::AtMostOnce, None, vec![])
        .await;
    dev.publish("$SYS/x", b"x", QoS::AtMostOnce, None, vec![])
        .await;
    dev.subscribe(1, "no/#", QoS::AtMostOnce).await;
    let mut seen = Vec::new();
    for _ in 0..5 {
        let p = watcher.expect_publish().await;
        seen.push(String::from_utf8(p.payload.to_vec()).unwrap());
    }
    assert_eq!(
        seen,
        [
            r#"{"topic":"ok/will","action":"publish","authz_source":"file","result":"allow"}"#,
            r#"{"topic":"ok/1","action":"publish","authz_source":"file","result":"allow"}"#,
            r#"{"topic":"no/1","action":"publish","authz_source":"default","result":"deny"}"#,
            r#"{"topic":"$SYS/x","action":"publish","authz_source":"default","result":"deny"}"#,
            r#"{"topic":"no/#","action":"subscribe","authz_source":"default","result":"deny"}"#,
        ]
    );
}
