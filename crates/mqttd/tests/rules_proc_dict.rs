//! EMQX's `proc_dict_*` end to end (ADR 0085 D13): real sockets, the real codec, the
//! real hub, and whose dictionary each evaluation works on.
//!
//! In EMQX the dictionary is the evaluating process's. Which process that is was
//! probed on EMQX 6.3.1 (`emqx/emqx:latest`, 2026-10-11) with clients on a running
//! node: rules that put a value on one trigger and read it on another, and a scan of
//! every process's dictionary for `{'@rule_engine', _}` keys. It is the client's
//! connection process (`emqx_socket_connection`) for everything about the client:
//!
//! | Probe | What EMQX showed |
//! |---|---|
//! | `L03` `L04` | a second publish on one connection gets the first one's value |
//! | `L05` `L06` | another client has a dictionary of its own |
//! | `L03` | a later rule on the same message, and the rules its republished message re-enters, see the put |
//! | `L07` `L08` | after DISCONNECT and a new CONNECT the dictionary is empty |
//! | `L19` `L20` | a second connection that takes the client id over starts empty |
//! | `M07`..`M12` | a session that is resumed starts empty: the process is a new one |
//! | `L02` `L09`..`L12` | `client_connected`, `connack`, the two `check_*_complete`, `session_subscribed`, `session_unsubscribed`, `message_delivered` and `message_acked` are evaluated in that client's process |
//! | `M13` `L20` | so is `client_disconnected`, which still sees the dictionary |
//! | `L03` `L13` | `message_dropped` (`no_subscribers`) is evaluated in the publisher's process |
//! | `M03` `M05` | **a Will is evaluated in the dying process and sees its dictionary** |
//! | `M08` `M09` | **a session that outlives its connection keeps the process, and the dictionary: a `delivery_dropped` for it sees what the closed connection put** |
//! | `M01` | **`message_dropped` for a message a rule republished is evaluated in the process that republished it** |
//!
//! mqttd evaluates the same things on the client's connection task, which owns the
//! dictionary, and these tests pin each row to it. The three rows in bold are where
//! mqttd has no connection task to evaluate on (the hub or a task of its own for a
//! Will, the notes task for the other two): there the rules get an empty dictionary
//! that is dropped afterwards, and the tests pin that instead. They are listed under
//! "Differences from EMQX" in `docs/RULES.md`, with the bounds, which EMQX does not
//! have.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::Client;
use mqtt_cluster::NodeId;
use mqtt_codec::{Packet, QoS};
use mqtt_storage::MemorySessionStore;
use mqttd::hub::{BrownoutAxis, Hub, HubCommand};
use mqttd::rule_state::Store;
use tokio::net::TcpListener;

struct Broker {
    addr: SocketAddr,
    hub_tx: tokio::sync::mpsc::UnboundedSender<HubCommand>,
    /// The store whose ceiling the connections' dictionaries count against.
    state: Arc<Store>,
    /// The rules are never reloaded here; the watch lives as long as the broker.
    _rules_tx: tokio::sync::watch::Sender<Arc<mqtt_rules::RuleSet>>,
}

/// The production wiring in miniature, as `rules_state.rs` has it: one rule state
/// store, the rule engine on every connection and in the hub (which starts the notes
/// task). `state_max_bytes` is `rules.state_max_bytes`.
async fn start_broker_under(rules: &str, state_max_bytes: u64) -> Broker {
    let config = mqtt_config::Rules {
        state_max_bytes,
        ..mqtt_config::Rules::default()
    };
    let (functions, state) = mqttd::rules::functions_and_state_from_config(&config);
    let state = state.expect("rule state is on");
    let sessions = Arc::new(MemorySessionStore::new());
    let (hub, hub_tx) = Hub::with_config(NodeId("dict-test".into()), sessions.clone());
    tokio::spawn(hub.run());
    let set = mqtt_rules::RuleSet::parse_with(rules, &functions)
        .unwrap_or_else(|e| panic!("test rules must load: {e}"))
        .rules;
    let (rules_tx, rx) = tokio::sync::watch::channel(Arc::new(set));
    let engine = mqttd::rules::Rules::new(rx, Arc::from("dict-test"), None)
        .with_functions(functions)
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
        _rules_tx: rules_tx,
    }
}

async fn start_broker(rules: &str) -> Broker {
    start_broker_under(rules, mqtt_config::Rules::default().state_max_bytes).await
}

/// The next message `c` receives, as (topic, payload).
async fn next(c: &mut Client) -> (String, String) {
    match c.recv().await {
        Packet::Publish(p) => (p.topic, String::from_utf8_lossy(&p.payload).into_owned()),
        other => panic!("expected a publish, got {other:?}"),
    }
}

/// The next `n` messages, sorted: what one publish's rules produced, whose order
/// among each other is not what a test here is about.
async fn next_sorted(c: &mut Client, n: usize) -> Vec<(String, String)> {
    let mut got = Vec::with_capacity(n);
    for _ in 0..n {
        got.push(next(c).await);
    }
    got.sort();
    got
}

async fn publish(c: &mut Client, topic: &str, payload: &str) {
    c.publish(topic, payload.as_bytes(), QoS::AtMostOnce, None, vec![])
        .await;
}

fn out(topic: &str, payload: &str) -> (String, String) {
    (topic.to_string(), payload.to_string())
}

/// A rule `id` whose outputs are republished whole to `to` (no action without one).
fn rule(id: &str, sql: &str, to: Option<&str>) -> String {
    let actions = to.map_or_else(String::new, |topic| {
        format!(
            "actions = [{{ function = \"republish\", args = {{ topic = \"{topic}\", payload = \"${{.}}\" }} }}]\n"
        )
    });
    format!("[rules.{id}]\nsql = '''{sql}'''\n{actions}\n")
}

/// A client that sees everything published under `out/`.
async fn watcher(broker: &Broker) -> Client {
    let mut watch = Client::connect(broker.addr, "watch").await;
    watch.subscribe(1, "out/#", QoS::AtMostOnce).await;
    watch
}

/// Wait for something another task does on its own time (a connection task ending).
async fn until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "{what}: not in time");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// `SELECT … FROM "in/#"`: put the payload under `n`, and say what was there.
const PUT: &str = "SELECT clientid AS c, proc_dict_put('n', payload) AS prev, \
                   proc_dict_get('n') AS now FROM \"in/#\"";

/// L03..L06, M01: the dictionary is the connection's. A second publish on it gets the
/// first one's value; a later rule on the same message and the rule a republished
/// message re-enters see the put (they run on the same task); another client has a
/// dictionary of its own. d27: it is not EMQX's `kv_store` table. What the
/// dictionaries hold is counted in the store, in no namespace.
#[tokio::test]
async fn a_connections_dictionary_lasts_from_one_publish_to_the_next_and_is_its_own() {
    let broker = start_broker(&format!(
        "{}{}{}{}",
        rule("a_put", PUT, Some("out/put/${c}")),
        rule(
            "b_later",
            "SELECT proc_dict_get('n') AS later FROM \"in/#\"",
            Some("out/later")
        ),
        rule(
            "c_reentered",
            "SELECT clientid AS by, proc_dict_get('n') AS seen, proc_dict_put('re', 1) AS prev \
             FROM \"out/put/#\"",
            Some("out/reentered")
        ),
        rule(
            "d_tables",
            "SELECT kv_store_put('both', 'kv') AS k, proc_dict_get('both') AS g, \
             proc_dict_put('both', 'pd') AS p, kv_store_get('both') AS s FROM \"both\"",
            Some("out/both")
        ),
    ))
    .await;
    let mut watch = watcher(&broker).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    let round = |c: &str, prev: &str, now: &str, re: &str| {
        vec![
            out("out/later", &format!(r#"{{"later":"{now}"}}"#)),
            out(
                &format!("out/put/{c}"),
                &format!(r#"{{"c":"{c}","prev":{prev},"now":"{now}"}}"#),
            ),
            out(
                "out/reentered",
                &format!(r#"{{"by":"a_put","seen":"{now}","prev":{re}}}"#),
            ),
        ]
    };
    publish(&mut dev, "in/a", "one").await;
    assert_eq!(
        next_sorted(&mut watch, 3).await,
        round("dev-1", r#""undefined""#, "one", r#""undefined""#)
    );
    publish(&mut dev, "in/a", "two").await;
    assert_eq!(
        next_sorted(&mut watch, 3).await,
        round("dev-1", r#""one""#, "two", "1")
    );
    let mut other = Client::connect(broker.addr, "dev-2").await;
    publish(&mut other, "in/a", "bee").await;
    assert_eq!(
        next_sorted(&mut watch, 3).await,
        round("dev-2", r#""undefined""#, "bee", r#""undefined""#)
    );
    publish(&mut dev, "in/a", "three").await;
    assert_eq!(
        next_sorted(&mut watch, 3).await,
        round("dev-1", r#""two""#, "three", "1")
    );
    // Two dictionaries: `n` and `re` in each, as written (1 + 5 and 2 + 1 bytes for
    // dev-1, 1 + 3 and 2 + 1 for dev-2), in no namespace: the one the file holds is
    // EMQX's table, for the last rule, and it is empty.
    assert_eq!(broker.state.bytes(), 9 + 7);
    assert_eq!(broker.state.namespaces(), ["kv_store"]);
    assert_eq!(broker.state.stats("kv_store").unwrap().bytes, 0);
    // d27: {true, undefined, undefined, kv}.
    publish(&mut dev, "both", "{}").await;
    assert_eq!(
        next(&mut watch).await,
        out(
            "out/both",
            r#"{"k":true,"g":"undefined","p":"undefined","s":"kv"}"#
        )
    );
}

/// L07, L08, M13: `client_disconnected` is evaluated while the dictionary is still
/// there; the same client's next connection starts with an empty one, and the old
/// one's bytes went back to the store with its task.
#[tokio::test]
async fn a_reconnect_starts_with_an_empty_dictionary() {
    let broker = start_broker(&format!(
        "{}{}",
        rule("put", PUT, Some("out/put")),
        rule(
            "bye",
            "SELECT clientid AS c, proc_dict_get('n') AS n FROM \"$events/client_disconnected\" \
             WHERE clientid = 'dev-1'",
            Some("out/bye")
        ),
    ))
    .await;
    let mut watch = watcher(&broker).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    publish(&mut dev, "in/a", "one").await;
    assert_eq!(
        next(&mut watch).await,
        out("out/put", r#"{"c":"dev-1","prev":"undefined","now":"one"}"#)
    );
    assert_eq!(broker.state.bytes(), 4);
    dev.disconnect().await;
    assert_eq!(
        next(&mut watch).await,
        out("out/bye", r#"{"c":"dev-1","n":"one"}"#)
    );
    until("the ended connection's dictionary is given back", || {
        broker.state.bytes() == 0
    })
    .await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    publish(&mut dev, "in/a", "two").await;
    assert_eq!(
        next(&mut watch).await,
        out("out/put", r#"{"c":"dev-1","prev":"undefined","now":"two"}"#)
    );
}

/// L19, L20, M05: a second connection with the same client id takes the client over
/// and starts with an empty dictionary (EMQX starts a new process there too); the
/// connection it replaced still had its own when its `client_disconnected` ran.
#[tokio::test]
async fn a_takeover_does_not_carry_the_dictionary() {
    let broker = start_broker(&format!(
        "{}{}",
        rule("put", PUT, Some("out/put")),
        rule(
            "bye",
            "SELECT proc_dict_get('n') AS n FROM \"$events/client_disconnected\" \
             WHERE clientid = 'dev-1'",
            Some("out/bye")
        ),
    ))
    .await;
    let mut watch = watcher(&broker).await;
    let mut first = Client::connect(broker.addr, "dev-1").await;
    publish(&mut first, "in/a", "one").await;
    assert_eq!(
        next(&mut watch).await,
        out("out/put", r#"{"c":"dev-1","prev":"undefined","now":"one"}"#)
    );
    let mut second = Client::connect(broker.addr, "dev-1").await;
    first.expect_closed().await;
    assert_eq!(next(&mut watch).await, out("out/bye", r#"{"n":"one"}"#));
    publish(&mut second, "in/a", "two").await;
    assert_eq!(
        next(&mut watch).await,
        out("out/put", r#"{"c":"dev-1","prev":"undefined","now":"two"}"#)
    );
}

/// M07..M12: a session that is resumed (`clean_session = false`) keeps its
/// subscriptions and not its dictionary: the connection is a new one.
#[tokio::test]
async fn a_resumed_session_starts_with_an_empty_dictionary() {
    let broker = start_broker(&rule("put", PUT, Some("out/put"))).await;
    let mut watch = watcher(&broker).await;
    let (mut dev, present) = Client::connect_v311(broker.addr, "dev-1", false).await;
    assert!(!present);
    publish(&mut dev, "in/a", "one").await;
    assert_eq!(
        next(&mut watch).await,
        out("out/put", r#"{"c":"dev-1","prev":"undefined","now":"one"}"#)
    );
    dev.disconnect().await;
    let (mut dev, present) = Client::connect_v311(broker.addr, "dev-1", false).await;
    assert!(present);
    publish(&mut dev, "in/a", "two").await;
    assert_eq!(
        next(&mut watch).await,
        out("out/put", r#"{"c":"dev-1","prev":"undefined","now":"two"}"#)
    );
}

/// L02, L09..L12: the client's own events, and the deliveries to it, are evaluated on
/// its connection: what their rules put is in the dictionary its publishes read, and
/// in nobody else's.
#[tokio::test]
async fn a_clients_events_and_deliveries_work_on_its_dictionary() {
    let put = |id: &str, event: &str, value: &str| {
        rule(
            id,
            &format!("SELECT proc_dict_put('{id}', {value}) AS prev FROM \"{event}\""),
            None,
        )
    };
    let broker = start_broker(&format!(
        "{}{}{}{}{}{}{}",
        put("connected", "$events/client_connected", "clientid"),
        put("connack", "$events/client_connack", "clientid"),
        put("subscribed", "$events/session_subscribed", "topic"),
        put("unsubscribed", "$events/session_unsubscribed", "topic"),
        put("delivered", "$events/message_delivered", "topic"),
        put("acked", "$events/message_acked", "topic"),
        rule(
            "ask",
            "SELECT proc_dict_get('connected') AS connected, proc_dict_get('connack') AS connack, \
             proc_dict_get('subscribed') AS subscribed, \
             proc_dict_get('unsubscribed') AS unsubscribed, \
             proc_dict_get('delivered') AS delivered, proc_dict_get('acked') AS acked \
             FROM \"ask\"",
            Some("out/ask")
        ),
    ))
    .await;
    let mut watch = watcher(&broker).await;
    let mut sub = Client::connect(broker.addr, "sub").await;
    sub.subscribe(1, "d/#", QoS::AtLeastOnce).await;
    let mut publisher = Client::connect(broker.addr, "pub").await;
    publisher
        .publish("d/1", b"hello", QoS::AtLeastOnce, Some(7), vec![])
        .await;
    let delivered = sub.expect_publish().await;
    assert_eq!(delivered.topic, "d/1");
    sub.puback(delivered.pkid.expect("a QoS 1 delivery")).await;
    // The PUBACK is read, and its event evaluated, before the publish behind it.
    publish(&mut sub, "ask", "{}").await;
    assert_eq!(
        next(&mut watch).await,
        out(
            "out/ask",
            r#"{"connected":"sub","connack":"sub","subscribed":"d/#","unsubscribed":"undefined","delivered":"d/1","acked":"d/1"}"#
        )
    );
    sub.send(&Packet::Unsubscribe(mqtt_codec::packet::Unsubscribe {
        pkid: 2,
        filters: vec!["d/#".into()],
        properties: mqtt_codec::Properties::new(),
    }))
    .await;
    assert!(matches!(sub.recv().await, Packet::UnsubAck(_)));
    publish(&mut sub, "ask", "{}").await;
    assert_eq!(
        next(&mut watch).await,
        out(
            "out/ask",
            r#"{"connected":"sub","connack":"sub","subscribed":"d/#","unsubscribed":"d/#","delivered":"d/1","acked":"d/1"}"#
        )
    );
    // The publisher was delivered nothing and subscribed to nothing.
    publish(&mut publisher, "ask", "{}").await;
    assert_eq!(
        next(&mut watch).await,
        out(
            "out/ask",
            r#"{"connected":"pub","connack":"pub","subscribed":"undefined","unsubscribed":"undefined","delivered":"undefined","acked":"undefined"}"#
        )
    );
}

/// L03, L13: `message_dropped` for a client's publish nobody subscribes to is
/// evaluated on the publisher's connection, with its dictionary.
///
/// M01, a listed difference: for a message a *rule* republished, EMQX evaluates the
/// event in the process that republished it. mqttd has no connection to hand that
/// note to: the notes task evaluates it, on an empty dictionary that is dropped
/// afterwards, like every event about a client that has no connection.
#[tokio::test]
async fn a_dropped_publish_works_on_its_publishers_dictionary_and_a_rules_on_an_empty_one() {
    let broker = start_broker(&format!(
        "{}{}{}",
        rule(
            "a_put",
            "SELECT proc_dict_put('n', payload) AS prev FROM \"in/#\"",
            Some("nowhere/x")
        ),
        rule(
            "dropped",
            "SELECT topic, reason, proc_dict_get('n') AS n, proc_dict_put('note', 1) AS prev \
             FROM \"$events/message_dropped\" WHERE topic =~ 'in/#' OR topic =~ 'nowhere/#'",
            Some("out/dropped")
        ),
        rule(
            "ask",
            "SELECT proc_dict_get('note') AS note FROM \"ask\"",
            Some("out/ask")
        ),
    ))
    .await;
    let mut watch = watcher(&broker).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    for (payload, note) in [("one", r#""undefined""#), ("two", "1")] {
        publish(&mut dev, "in/a", payload).await;
        assert_eq!(
            next_sorted(&mut watch, 2).await,
            [
                // The client's own publish: its connection's dictionary, which keeps
                // what the event's rule put.
                out(
                    "out/dropped",
                    &format!(
                        r#"{{"topic":"in/a","reason":"no_subscribers","n":"{payload}","prev":{note}}}"#
                    )
                ),
                // The rule's republish: an empty dictionary, every time.
                out(
                    "out/dropped",
                    r#"{"topic":"nowhere/x","reason":"no_subscribers","n":"undefined","prev":"undefined"}"#
                ),
            ]
        );
    }
    publish(&mut dev, "ask", "{}").await;
    assert_eq!(next(&mut watch).await, out("out/ask", r#"{"note":1}"#));
}

/// M03, M05, a listed difference: EMQX evaluates a Will in the dying connection's
/// process, which still has the dictionary (it answered `"w1"` there). mqttd's
/// connection task has ended: the Will's rules get an empty dictionary that is
/// dropped afterwards, so they read nothing and what they put is seen by nothing.
#[tokio::test]
async fn a_wills_rules_get_an_empty_dictionary() {
    let broker = start_broker(&format!(
        "{}{}",
        rule("put", PUT, Some("out/put")),
        rule(
            "will",
            "SELECT clientid AS c, proc_dict_get('n') AS n, proc_dict_put('will', 1) AS prev \
             FROM \"will/#\"",
            Some("out/will")
        ),
    ))
    .await;
    let mut watch = watcher(&broker).await;
    for id in ["dev-w1", "dev-w2"] {
        let mut dev = Client::open(broker.addr, mqtt_codec::ProtocolVersion::V311).await;
        dev.connect_with_will(id, "will/x", b"gone").await;
        publish(&mut dev, "in/a", "w1").await;
        assert_eq!(
            next(&mut watch).await,
            out(
                "out/put",
                &format!(r#"{{"c":"{id}","prev":"undefined","now":"w1"}}"#)
            )
        );
        drop(dev); // ungraceful: the Will is owed
        assert_eq!(
            next(&mut watch).await,
            out(
                "out/will",
                &format!(r#"{{"c":"{id}","n":"undefined","prev":"undefined"}}"#)
            )
        );
    }
    until("nothing a Will put is kept", || broker.state.bytes() == 0).await;
}

/// D13, a listed difference: a connection's dictionary holds 64 keys. The 65th put
/// fails its rule; the connection keeps the 64, and another connection has 64 of its
/// own.
#[tokio::test]
async fn a_connections_dictionary_holds_64_keys() {
    let broker = start_broker(&format!(
        "{}{}",
        rule(
            "fill",
            "FOREACH payload.a AS k DO proc_dict_put(k, k) AS p FROM \"fill\"",
            None
        ),
        rule(
            "ask",
            "SELECT proc_dict_get(63) AS held, proc_dict_get(64) AS refused FROM \"ask\"",
            Some("out/ask")
        ),
    ))
    .await;
    let mut watch = watcher(&broker).await;
    let keys = |n: usize| {
        let list: Vec<String> = (0..n).map(|i| i.to_string()).collect();
        format!("{{\"a\":[{}]}}", list.join(","))
    };
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    publish(&mut dev, "fill", &keys(65)).await;
    publish(&mut dev, "ask", "{}").await;
    assert_eq!(
        next(&mut watch).await,
        out("out/ask", r#"{"held":63,"refused":"undefined"}"#)
    );
    let mut other = Client::connect(broker.addr, "dev-2").await;
    publish(&mut other, "fill", &keys(64)).await;
    publish(&mut other, "ask", "{}").await;
    assert_eq!(
        next(&mut watch).await,
        out("out/ask", r#"{"held":63,"refused":"undefined"}"#)
    );
    // Each holds its 64 keys and values as written: 10 of one digit, 54 of two.
    assert_eq!(broker.state.bytes(), 2 * 2 * (10 + 54 * 2));
    assert_eq!(broker.state.dictionary_refused(), 0);
}

/// D13: what the dictionaries hold counts against `rules.state_max_bytes`. A put the
/// store has no room for fails its rule (`store_full`, counted) and holds nothing;
/// when the connection that held the room ends, the room is back.
#[tokio::test]
async fn the_dictionaries_count_against_the_stores_ceiling() {
    let broker = start_broker_under(&rule("put", PUT, Some("out/put")), 100).await;
    let mut watch = watcher(&broker).await;
    let big = "x".repeat(60);
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    publish(&mut dev, "in/a", &big).await;
    assert_eq!(
        next(&mut watch).await,
        out(
            "out/put",
            &format!(r#"{{"c":"dev-1","prev":"undefined","now":"{big}"}}"#)
        )
    );
    assert_eq!(broker.state.bytes(), 61);
    // 61 more would pass 100: the rule fails, and the next put finds nothing held.
    let mut other = Client::connect(broker.addr, "dev-2").await;
    publish(&mut other, "in/a", &big).await;
    publish(&mut other, "in/a", "small").await;
    assert_eq!(
        next(&mut watch).await,
        out(
            "out/put",
            r#"{"c":"dev-2","prev":"undefined","now":"small"}"#
        )
    );
    assert_eq!(broker.state.dictionary_refused(), 1);
    assert_eq!(broker.state.bytes(), 61 + 6);
    dev.disconnect().await;
    until("the ended connection's dictionary is given back", || {
        broker.state.bytes() == 6
    })
    .await;
    publish(&mut other, "in/a", &big).await;
    assert_eq!(
        next(&mut watch).await,
        out(
            "out/put",
            &format!(r#"{{"c":"dev-2","prev":"small","now":"{big}"}}"#)
        )
    );
    assert_eq!(broker.state.dictionary_refused(), 1);
}

/// D9, ADR 0041: over the memory watermark a dictionary does not grow (`brownout`);
/// it still reads, shrinks and deletes.
#[tokio::test]
async fn a_memory_brownout_refuses_a_dictionary_room_to_grow() {
    let broker = start_broker(&format!(
        "{}{}",
        rule("put", PUT, Some("out/put")),
        rule(
            "del",
            "SELECT proc_dict_del('n') AS was FROM \"del\"",
            Some("out/del")
        ),
    ))
    .await;
    let mut watch = watcher(&broker).await;
    let mut dev = Client::connect(broker.addr, "dev-1").await;
    publish(&mut dev, "in/a", "four").await;
    assert_eq!(
        next(&mut watch).await,
        out(
            "out/put",
            r#"{"c":"dev-1","prev":"undefined","now":"four"}"#
        )
    );
    let set_brownout = |on| {
        broker
            .hub_tx
            .send(HubCommand::SetBrownout {
                axis: BrownoutAxis::Memory,
                on,
            })
            .unwrap();
    };
    set_brownout(true);
    // A subscribe is answered by the hub after the commands before it.
    watch.subscribe(2, "sync/1", QoS::AtMostOnce).await;
    publish(&mut dev, "in/a", "longer").await; // would grow: the rule fails
    publish(&mut dev, "in/a", "two").await; // smaller: it is put
    assert_eq!(
        next(&mut watch).await,
        out("out/put", r#"{"c":"dev-1","prev":"four","now":"two"}"#)
    );
    assert_eq!(broker.state.dictionary_refused(), 1);
    publish(&mut dev, "del", "{}").await;
    assert_eq!(next(&mut watch).await, out("out/del", r#"{"was":"two"}"#));
    assert_eq!(broker.state.bytes(), 0);
    set_brownout(false);
    watch.subscribe(3, "sync/2", QoS::AtMostOnce).await;
    publish(&mut dev, "in/a", "longer").await;
    assert_eq!(
        next(&mut watch).await,
        out(
            "out/put",
            r#"{"c":"dev-1","prev":"undefined","now":"longer"}"#
        )
    );
}
