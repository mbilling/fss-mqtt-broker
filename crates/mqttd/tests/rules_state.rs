//! State for rules end to end (ADR 0085): real sockets, the real codec, the real hub,
//! and the broker's store.
//!
//! The store's own tests (`mqttd::rule_state`) prove what each function does and pin
//! EMQX's `kv_store_*` to what EMQX 6.3.1 answered; these prove the broker around it:
//! that a rule on a connection keeps state from one publish to the next, that a state
//! call in a `SELECT` field counts a message its `WHERE` turns away, that a later rule
//! and a rule a republished message re-enters see a write, that a Will's rules (on the
//! hub) work on the same store, that a reload keeps or drops a namespace's keys, that
//! a memory brownout refuses new keys, and that a key's expiry raises
//! `$events/state/expired` for the rules that name it, and only for those.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::Client;
use mqtt_cluster::NodeId;
use mqtt_codec::{Packet, QoS};
use mqtt_storage::MemorySessionStore;
use mqttd::hub::{BrownoutAxis, Hub, HubCommand};
use mqttd::rule_state::Store;
use tokio::net::TcpListener;

/// A broker whose connections, and whose hub, run rules that keep state in `state`.
struct Broker {
    addr: SocketAddr,
    hub_tx: tokio::sync::mpsc::UnboundedSender<HubCommand>,
    state: Arc<Store>,
    functions: mqtt_rules::Functions,
    /// What a reload sends the new rules through.
    rules_tx: tokio::sync::watch::Sender<Arc<mqtt_rules::RuleSet>>,
}

impl Broker {
    /// A rules file loaded as this broker's reload loads it: with its store.
    fn load(&self, text: &str) -> Arc<mqtt_rules::RuleSet> {
        Arc::new(
            mqtt_rules::RuleSet::parse_with(text, &self.functions)
                .unwrap_or_else(|e| panic!("test rules must load: {e}"))
                .rules,
        )
    }
}

/// The production wiring in miniature: one rule state store for the life of the
/// broker, handed to every load of the rules; the rule engine on every connection and
/// in the hub.
async fn start_broker(rules: &str) -> Broker {
    let (functions, state) =
        mqttd::rules::functions_and_state_from_config(&mqtt_config::Rules::default());
    let state = state.expect("rule state is on by default");
    let sessions = Arc::new(MemorySessionStore::new());
    let (hub, hub_tx) = Hub::with_config(NodeId("state-test".into()), sessions.clone());
    tokio::spawn(hub.run());
    let set = mqtt_rules::RuleSet::parse_with(rules, &functions)
        .unwrap_or_else(|e| panic!("test rules must load: {e}"))
        .rules;
    let (rules_tx, rx) = tokio::sync::watch::channel(Arc::new(set));
    let engine = mqttd::rules::Rules::new(rx, Arc::from("state-test"), None)
        .with_functions(functions.clone())
        .with_state(Some(state.clone()));
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
        store: Some(sessions as Arc<dyn mqtt_storage::SessionStore>),
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
    Broker {
        addr,
        hub_tx,
        state,
        functions,
        rules_tx,
    }
}

/// The next message `c` receives, as (topic, payload).
async fn next(c: &mut Client) -> (String, String) {
    match c.recv().await {
        Packet::Publish(p) => (p.topic, String::from_utf8_lossy(&p.payload).into_owned()),
        other => panic!("expected a publish, got {other:?}"),
    }
}

async fn publish(c: &mut Client, topic: &str, payload: &str) {
    c.publish(topic, payload.as_bytes(), QoS::AtMostOnce, None, vec![])
        .await;
}

fn out(topic: &str, payload: &str) -> (String, String) {
    (topic.to_string(), payload.to_string())
}

/// A `republish` action to `topic` carrying the whole output.
fn republish(topic: &str) -> String {
    format!(
        "actions = [{{ function = \"republish\", args = {{ topic = \"{topic}\", payload = \"${{.}}\" }} }}]\n"
    )
}

/// D3: a state call in a `SELECT` field runs for every message the `FROM` selects,
/// whether or not the `WHERE` then passes; the state is the connection task's to call,
/// and is there for the next publish.
#[tokio::test]
async fn a_rule_keeps_state_between_publishes_and_counts_what_its_where_rejects() {
    let broker = start_broker(&format!(
        "[rules.count]\nsql = '''SELECT clientid AS c, kv_incr('n:' + clientid) AS n, \
         kv_changed('v:' + clientid, payload.v) AS changed FROM \"in/#\" \
         WHERE payload.pass = true'''\n{}",
        republish("out/${c}")
    ))
    .await;
    let mut sub = Client::connect(broker.addr, "watch").await;
    sub.subscribe(1, "out/#", QoS::AtMostOnce).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    publish(&mut dev, "in/a", r#"{"pass":false,"v":1}"#).await;
    publish(&mut dev, "in/a", r#"{"pass":false,"v":1}"#).await;
    publish(&mut dev, "in/a", r#"{"pass":true,"v":1}"#).await;
    publish(&mut dev, "in/a", r#"{"pass":true,"v":2}"#).await;
    // The two rejected messages were counted, and stored the value the third compared.
    assert_eq!(
        next(&mut sub).await,
        out("out/dev-1", r#"{"c":"dev-1","n":3,"changed":false}"#)
    );
    assert_eq!(
        next(&mut sub).await,
        out("out/dev-1", r#"{"c":"dev-1","n":4,"changed":true}"#)
    );
    // Another connection has keys of its own, because the rule built them so; the
    // namespace is the rule's.
    let mut other = Client::connect(broker.addr, "dev-2").await;
    publish(&mut other, "in/a", r#"{"pass":true,"v":1}"#).await;
    assert_eq!(
        next(&mut sub).await,
        out("out/dev-2", r#"{"c":"dev-2","n":1,"changed":true}"#)
    );
    assert_eq!(broker.state.namespaces(), ["rule:count"]);
    assert_eq!(broker.state.stats("rule:count").unwrap().keys, 4);
}

/// D13: EMQX's `kv_store_put` and `kv_store_get` across two messages (what EMQX 6.3.1
/// answers for the same statement: 1, then 2), in a table every rule shares.
#[tokio::test]
async fn kv_store_counts_across_messages_and_is_shared_by_every_rule() {
    let broker = start_broker(&format!(
        "[rules.a_count]\nsql = '''SELECT kv_store_get('n', 0) + 1 AS n, kv_store_put('n', n) AS p \
         FROM \"in/#\"'''\n{}\n\
         [rules.b_read]\nsql = '''SELECT kv_store_get('n') AS seen FROM \"in/#\"'''\n{}",
        republish("out/count"),
        republish("out/read")
    ))
    .await;
    let mut sub = Client::connect(broker.addr, "watch").await;
    sub.subscribe(1, "out/#", QoS::AtMostOnce).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    publish(&mut dev, "in/a", "{}").await;
    assert_eq!(
        next(&mut sub).await,
        out("out/count", r#"{"n":1,"p":true}"#)
    );
    assert_eq!(next(&mut sub).await, out("out/read", r#"{"seen":1}"#));
    publish(&mut dev, "in/a", "{}").await;
    assert_eq!(
        next(&mut sub).await,
        out("out/count", r#"{"n":2,"p":true}"#)
    );
    assert_eq!(next(&mut sub).await, out("out/read", r#"{"seen":2}"#));
    assert_eq!(broker.state.namespaces(), ["kv_store"]);
}

/// D3: a later call sees an earlier one in the next rule the message matches and in
/// the rules a republished message re-enters, which run on the same task, in order. A
/// rule runs again on its own republished output (EMQX's guard stops only its
/// `republish` actions), so a rule that counts and republishes into its own `FROM`
/// counts twice.
#[tokio::test]
async fn a_later_rule_and_a_re_entered_rule_see_a_write() {
    let rule = |id: &str, sql: &str, to: &str| {
        format!(
            "[rules.{id}]\nstate = \"s\"\nsql = '''{sql}'''\n{}\n",
            republish(to)
        )
    };
    let broker = start_broker(&format!(
        "[state.s]\n{}{}{}{}",
        rule(
            "a_write",
            "SELECT kv_incr('seen') AS n FROM \"in/#\"",
            "mid/x"
        ),
        rule(
            "b_later",
            "SELECT kv_get('seen') AS later FROM \"in/#\"",
            "out/later"
        ),
        rule(
            "c_reentered",
            "SELECT kv_get('seen') AS seen, kv_incr('reentered') AS r FROM \"mid/#\"",
            "out/reentered"
        ),
        rule(
            "d_loop",
            "SELECT kv_incr('self') AS n FROM \"loop/#\"",
            "loop/again"
        ),
    ))
    .await;
    let mut sub = Client::connect(broker.addr, "watch").await;
    sub.subscribe(1, "out/#", QoS::AtMostOnce).await;
    sub.subscribe(2, "loop/again", QoS::AtMostOnce).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    publish(&mut dev, "in/a", "{}").await;
    let mut got = vec![next(&mut sub).await, next(&mut sub).await];
    got.sort();
    assert_eq!(
        got,
        [
            out("out/later", r#"{"later":1}"#),
            out("out/reentered", r#"{"seen":1,"r":1}"#),
        ]
    );
    // The rule ran on its own output: one publish, two counts, one republish.
    publish(&mut dev, "loop/start", "{}").await;
    assert_eq!(next(&mut sub).await, out("loop/again", r#"{"n":1}"#));
    publish(&mut dev, "loop/start", "{}").await;
    assert_eq!(next(&mut sub).await, out("loop/again", r#"{"n":3}"#));
}

/// D14: a Will's rules run on the hub, on the same store, and what they write is
/// there for a connection's next publish.
#[tokio::test]
async fn a_wills_rules_call_the_same_store() {
    let broker = start_broker(&format!(
        "[state.fleet]\n\
         [rules.offline]\nstate = \"fleet\"\nsql = '''SELECT kv_incr('offline') AS offline \
         FROM \"devices/+/status\"'''\n{}\n\
         [rules.ask]\nstate = \"fleet\"\nsql = '''SELECT kv_get('offline') AS offline FROM \"ask\"'''\n{}",
        republish("out/will"),
        republish("out/ask")
    ))
    .await;
    let mut sub = Client::connect(broker.addr, "watch").await;
    sub.subscribe(1, "out/#", QoS::AtMostOnce).await;
    let mut dev = Client::open(broker.addr, mqtt_codec::ProtocolVersion::V311).await;
    dev.connect_with_will("dev-w", "devices/dev-w/status", b"offline")
        .await;
    drop(dev); // ungraceful: the Will is owed
    assert_eq!(next(&mut sub).await, out("out/will", r#"{"offline":1}"#));
    let mut asker = Client::connect(broker.addr, "asker").await;
    publish(&mut asker, "ask", "{}").await;
    assert_eq!(next(&mut sub).await, out("out/ask", r#"{"offline":1}"#));
}

/// D1: across a reload a namespace whose declaration is still there keeps its keys,
/// and one whose declaration (or whose rule) is gone is dropped with them once the new
/// set is the one running.
#[tokio::test]
async fn a_reload_keeps_a_declared_namespace_and_drops_one_that_is_gone() {
    let count = |id: &str, state: &str| {
        format!(
            "[rules.{id}]\n{state}sql = '''SELECT kv_incr('n') AS n FROM \"{id}\"'''\n{}\n",
            republish(&format!("out/{id}"))
        )
    };
    let v1 = format!(
        "[state.kept]\n[state.gone]\n{}{}{}",
        count("kept", "state = \"kept\"\n"),
        count("gone", "state = \"gone\"\n"),
        count("own", "")
    );
    let broker = start_broker(&v1).await;
    let mut sub = Client::connect(broker.addr, "watch").await;
    sub.subscribe(1, "out/#", QoS::AtMostOnce).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    for topic in ["kept", "gone", "own"] {
        publish(&mut dev, topic, "{}").await;
        assert_eq!(
            next(&mut sub).await,
            out(&format!("out/{topic}"), r#"{"n":1}"#)
        );
    }
    assert_eq!(broker.state.namespaces(), ["gone", "kept", "rule:own"]);
    // The reload: `gone` is no longer declared and its rule is gone; `own` is renamed.
    let v2 = format!(
        "[state.kept]\nmax_keys = 5\n{}{}",
        count("kept", "state = \"kept\"\n"),
        count("own2", "")
    );
    let old = broker.rules_tx.send_replace(broker.load(&v2));
    drop(old);
    // The connection picks the new set up for its next publish, which also shows that
    // nothing of the old one is being evaluated any more.
    publish(&mut dev, "kept", "{}").await;
    assert_eq!(next(&mut sub).await, out("out/kept", r#"{"n":2}"#));
    publish(&mut dev, "own2", "{}").await;
    assert_eq!(next(&mut sub).await, out("out/own2", r#"{"n":1}"#));
    assert_eq!(broker.state.namespaces(), ["kept", "rule:own2"]);
    // Declared again later, it starts empty.
    let old = broker.rules_tx.send_replace(broker.load(&v1));
    drop(old);
    publish(&mut dev, "gone", "{}").await;
    assert_eq!(next(&mut sub).await, out("out/gone", r#"{"n":1}"#));
    publish(&mut dev, "kept", "{}").await;
    assert_eq!(next(&mut sub).await, out("out/kept", r#"{"n":3}"#));
}

/// D9, ADR 0041: over the memory watermark the store takes no new key, counted as
/// `brownout`; existing keys still update. The hub tells the store as it learns it.
#[tokio::test]
async fn a_memory_brownout_refuses_new_keys_and_existing_ones_still_update() {
    let broker = start_broker(&format!(
        "[rules.count]\nsql = '''SELECT payload.k AS k, kv_incr('k:' + payload.k) AS n FROM \"in/#\"'''\n{}",
        republish("out/count")
    ))
    .await;
    let mut sub = Client::connect(broker.addr, "watch").await;
    sub.subscribe(1, "out/#", QoS::AtMostOnce).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    publish(&mut dev, "in/a", r#"{"k":"a"}"#).await;
    assert_eq!(next(&mut sub).await, out("out/count", r#"{"k":"a","n":1}"#));
    let set_brownout = |axis, on| {
        broker
            .hub_tx
            .send(HubCommand::SetBrownout { axis, on })
            .unwrap();
    };
    // The disk's watermark is not the store's concern.
    set_brownout(BrownoutAxis::Disk, true);
    set_brownout(BrownoutAxis::Memory, true);
    // A subscribe is answered by the hub after the commands before it.
    sub.subscribe(2, "sync/1", QoS::AtMostOnce).await;
    publish(&mut dev, "in/a", r#"{"k":"b"}"#).await; // a new key: the rule fails
    publish(&mut dev, "in/a", r#"{"k":"a"}"#).await; // an existing one: it counts
    assert_eq!(next(&mut sub).await, out("out/count", r#"{"k":"a","n":2}"#));
    let stats = broker.state.stats("rule:count").unwrap();
    assert_eq!((stats.keys, stats.refused_brownout), (1, 1));
    set_brownout(BrownoutAxis::Memory, false);
    sub.subscribe(3, "sync/2", QoS::AtMostOnce).await;
    publish(&mut dev, "in/a", r#"{"k":"b"}"#).await;
    assert_eq!(next(&mut sub).await, out("out/count", r#"{"k":"b","n":1}"#));
}

/// "Silent for one second": a key refreshed with a TTL by every message of a device,
/// and a rule on its expiry.
const SILENT: &str = "[state.seen]\n\
     [rules.a_seen]\nstate = \"seen\"\n\
     sql = '''SELECT kv_set(clientid, payload.n, 1) AS n FROM \"in/#\"'''\n\
     [rules.b_silent]\n\
     sql = '''SELECT namespace, key, value, type, node, expired_at <= timestamp AS after, \
     clientid, kv_get(key) AS own FROM \"$events/state/expired\"'''\n\
     actions = [{ function = \"republish\", args = { topic = \"silent/${key}\", payload = \"${.}\" } }]\n";

/// D7: a key past its expiry raises `$events/state/expired` on this node, once, with
/// the key, what it held, its type and its namespace; the event is about no client;
/// its rules call the store like any rule's (here a namespace of their own, where the
/// key is not); and a rule on `"$events/#"` is not sent it. The key is gone from the
/// store when the event has been raised, and raises it again after it is written
/// again.
#[tokio::test]
async fn a_key_that_expires_raises_the_expiry_event_for_the_rule_that_names_it() {
    let broker = start_broker(&format!(
        "{SILENT}[rules.c_every_event]\nsql = '''SELECT event FROM \"$events/#\"'''\n\
         actions = [{{ function = \"republish\", args = {{ topic = \"every/${{event}}\" }} }}]\n"
    ))
    .await;
    let mut sub = Client::connect(broker.addr, "watch").await;
    sub.subscribe(1, "silent/#", QoS::AtMostOnce).await;
    sub.subscribe(2, "every/#", QoS::AtMostOnce).await;
    assert!(
        broker.state.expiry_events(),
        "a loaded rule names the event"
    );
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    for n in [7, 8] {
        publish(&mut dev, "in/a", &format!(r#"{{"n":{n}}}"#)).await;
        // Whatever the wildcard rule is sent meanwhile, it is never this event.
        let silent = loop {
            let (topic, payload) = next(&mut sub).await;
            assert_ne!(topic, "every/state.expired");
            if topic.starts_with("silent/") {
                break (topic, payload);
            }
        };
        assert_eq!(
            silent,
            out(
                "silent/dev-1",
                &format!(
                    r#"{{"namespace":"seen","key":"dev-1","value":{n},"type":"lww","node":"state-test","after":true,"clientid":"undefined","own":"undefined"}}"#
                )
            )
        );
        assert_eq!(
            broker.state.stats("seen").unwrap().keys,
            0,
            "removed as its event was handed over"
        );
    }
}

/// D7: the event's machinery is off until a loaded rule names the event, and a
/// reload that adds such a rule switches it on: keys that expire afterwards raise
/// it. A reload that removes the rule switches it off again, and an expired key is
/// then only removed.
#[tokio::test]
async fn a_reload_that_adds_a_rule_on_the_expiry_event_starts_raising_it() {
    let seen_only = "[state.seen]\n[rules.a_seen]\nstate = \"seen\"\n\
                     sql = '''SELECT kv_set(clientid, payload.n, 1) AS n FROM \"in/#\"'''\n";
    let broker = start_broker(seen_only).await;
    let mut sub = Client::connect(broker.addr, "watch").await;
    sub.subscribe(1, "silent/#", QoS::AtMostOnce).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    assert!(!broker.state.expiry_events(), "no rule names the event");
    drop(broker.rules_tx.send_replace(broker.load(SILENT)));
    publish(&mut dev, "in/a", r#"{"n":1}"#).await;
    let (topic, payload) = next(&mut sub).await;
    assert_eq!(topic, "silent/dev-1");
    assert!(payload.contains(r#""value":1"#), "{payload}");
    assert!(broker.state.expiry_events());
    // Without the rule again, a key that expires is only removed by the sweep.
    drop(broker.rules_tx.send_replace(broker.load(seen_only)));
    // At QoS 1: the key is stored by the time the publish is acknowledged.
    dev.publish("in/a", br#"{"n":2}"#, QoS::AtLeastOnce, Some(1), vec![])
        .await;
    assert!(matches!(dev.recv().await, Packet::PubAck(_)));
    assert_eq!(broker.state.stats("seen").unwrap().keys, 1);
    let mut removed = false;
    for _ in 0..500 {
        if !broker.state.expiry_events() && broker.state.stats("seen").unwrap().keys == 0 {
            removed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        removed,
        "the sweep removes an expired key no event is owed for"
    );
    drop(broker.rules_tx.send_replace(broker.load(SILENT)));
    publish(&mut dev, "in/a", r#"{"n":3}"#).await;
    let (topic, payload) = next(&mut sub).await;
    assert_eq!(topic, "silent/dev-1");
    assert!(
        payload.contains(r#""value":3"#),
        "no event for 2: {payload}"
    );
}
