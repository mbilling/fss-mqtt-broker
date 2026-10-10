//! The four message events — `$events/message/delivered`, `acked`, `dropped` and
//! `delivery_dropped` — against EMQX's builders (`eventmsg_delivered/2`,
//! `eventmsg_acked/2`, `eventmsg_dropped/2`, `eventmsg_delivery_dropped/3` in
//! `emqx_rule_events.erl`) and what EMQX 6.3.1 printed for them: every expected field
//! set and value below is copied from a `SELECT *` rule's output on a real broker.

use std::sync::Arc;

use bytes::Bytes;
use mqtt_core::Origin;

use crate::{
    printable_props, test_sql, ClientInfo, Effect, EventInput, EventKind, EventMessage, Input, Map,
    Outcome, Recursion, RuleSet, Value,
};

/// The subscriber of EMQX's captured `message.delivered`: `sub1` / `subuser`.
fn receiver() -> ClientInfo<'static> {
    ClientInfo {
        clientid: "sub1",
        username: Some("subuser"),
        peer: Some("172.17.0.1:57612".parse().unwrap()),
        sockname: Some("172.17.0.3:1883".parse().unwrap()),
        node: "emqx@172.17.0.3",
    }
}

/// Its publisher: `pub1` / `pubuser`.
fn origin() -> Origin {
    Origin {
        id: 0x0006_5D7F_9D39_05CE_4259_0000_16A4_0002,
        clientid: "pub1".into(),
        username: Some("pubuser".into()),
        peer: Some("172.17.0.1:57648".parse().unwrap()),
        received_at_ms: 1_791_652_540_253,
        republished: false,
        republish_depth: 0,
    }
}

fn message<'a>(origin: Option<&'a Origin>, payload: &'a Bytes) -> EventMessage<'a> {
    EventMessage {
        origin,
        topic: "t/a",
        payload,
        qos: 1,
        retain: false,
        dup: false,
        user_properties: &[],
    }
}

/// The PUBLISH's properties as EMQX printed them for the delivery: the publisher's,
/// plus the subscription's identifier.
fn delivered_props() -> Map {
    printable_props(
        &[("k", "v")],
        [
            ("Subscription-Identifier", Value::Int(7)),
            ("Message-Expiry-Interval", Value::Int(60)),
            ("Content-Type", Value::from("application/json")),
        ],
    )
}

fn no_props() -> Map {
    printable_props::<&str, &str>(&[], [])
}

fn names(ev: &EventInput) -> Vec<String> {
    let mut names: Vec<String> = ev.all_fields().iter().map(|(k, _)| k.to_string()).collect();
    names.sort();
    names
}

fn sorted(names: &[&str]) -> Vec<String> {
    let mut names: Vec<String> = names.iter().map(ToString::to_string).collect();
    names.sort();
    names
}

/// What EMQX's `message.delivered` output has, less the `metadata` its runtime adds.
const DELIVERED: [&str; 16] = [
    "from_username",
    "from_clientid",
    "publish_received_at",
    "pub_props",
    "qos",
    "clientid",
    "topic",
    "peerhost",
    "username",
    "payload",
    "event",
    "peername",
    "timestamp",
    "node",
    "id",
    "flags",
];

/// `message.dropped`: the publisher's own columns, and a `reason`.
const DROPPED: [&str; 15] = [
    "publish_received_at",
    "pub_props",
    "qos",
    "clientid",
    "topic",
    "peerhost",
    "username",
    "payload",
    "event",
    "peername",
    "timestamp",
    "reason",
    "node",
    "id",
    "flags",
];

/// A field's value, as JSON (a `Value` has no equality of its own).
fn json(ev: &EventInput, field: &str) -> String {
    ev.field(field).to_json().unwrap()
}

fn one(sql: &str, ev: &EventInput) -> String {
    let out = test_sql(sql, ev).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(out.len(), 1, "{sql}: {out:?}");
    out.into_iter().next().unwrap()
}

/// Each event has exactly the columns EMQX's builder sets — no `client_attrs`, no
/// `sockname` — and names itself as EMQX's hook does.
#[test]
fn each_message_event_has_exactly_emqx_s_columns() {
    let payload = Bytes::from_static(br#"{"x":1}"#);
    let o = origin();
    let msg = message(Some(&o), &payload);
    let delivered = EventInput::message_delivered(&receiver(), &msg, delivered_props());
    assert_eq!(names(&delivered), sorted(&DELIVERED));
    assert_eq!(delivered.kind(), EventKind::MessageDelivered);

    let acked = EventInput::message_acked(&receiver(), &msg, delivered_props(), no_props());
    let mut with_ack: Vec<&str> = DELIVERED.to_vec();
    with_ack.push("puback_props");
    assert_eq!(names(&acked), sorted(&with_ack));

    let dropped =
        EventInput::message_dropped("emqx@172.17.0.3", &msg, no_props(), "no_subscribers");
    assert_eq!(names(&dropped), sorted(&DROPPED));

    let lost = EventInput::delivery_dropped(&receiver(), &msg, no_props(), "queue_full");
    let mut with_reason: Vec<&str> = DELIVERED.to_vec();
    with_reason.push("reason");
    assert_eq!(names(&lost), sorted(&with_reason));

    for (ev, event) in [
        (&delivered, "message.delivered"),
        (&acked, "message.acked"),
        (&dropped, "message.dropped"),
        (&lost, "delivery.dropped"),
    ] {
        assert_eq!(json(ev, "event"), format!("\"{event}\""));
        assert_eq!(ev.kind().event_name(), event);
    }
}

/// EMQX 6.3.1's `message.delivered` for a `QoS` 1 publish from `pub1` to `sub1`, value
/// for value (its `timestamp` is the event's own time).
#[test]
fn a_delivery_reads_as_emqx_printed_it() {
    let payload = Bytes::from_static(br#"{"x":1}"#);
    let o = origin();
    let ev =
        EventInput::message_delivered(&receiver(), &message(Some(&o), &payload), delivered_props());
    assert_eq!(
        one(
            "SELECT from_username, from_clientid, publish_received_at, pub_props, qos, clientid, \
             topic, peerhost, username, payload, event, peername, node, id, flags \
             FROM \"$events/message/delivered\"",
            &ev
        ),
        concat!(
            r#"{"from_username":"pubuser","from_clientid":"pub1","#,
            r#""publish_received_at":1791652540253,"#,
            r#""pub_props":{"User-Property":{"k":"v"},"#,
            r#""User-Property-Pairs":[{"key":"k","value":"v"}],"#,
            r#""Subscription-Identifier":7,"Message-Expiry-Interval":60,"#,
            r#""Content-Type":"application/json"},"#,
            r#""qos":1,"clientid":"sub1","topic":"t/a","peerhost":"172.17.0.1","#,
            r#""username":"subuser","payload":"{\"x\":1}","event":"message.delivered","#,
            r#""peername":"172.17.0.1:57612","node":"emqx@172.17.0.3","#,
            r#""id":"00065D7F9D3905CE4259000016A40002","#,
            r#""flags":{"dup":false,"retain":false}}"#
        )
    );
    let Value::Int(at) = ev.field("timestamp") else {
        panic!("timestamp is an integer")
    };
    assert!(at >= 1_791_652_540_253 || at > 0);
    // The payload is the message's, readable as JSON and as it is.
    assert_eq!(
        one(
            "SELECT payload.x AS x, topic FROM \"$events/message/delivered\" WHERE topic =~ 't/#'",
            &ev
        ),
        r#"{"x":1,"topic":"t/a"}"#
    );
}

/// `message.acked` is the delivery plus the acknowledgement's properties; an
/// acknowledgement without any prints as EMQX's `{"User-Property": {}}`.
#[test]
fn an_acknowledgement_adds_its_properties() {
    let payload = Bytes::from_static(b"two");
    let o = origin();
    let msg = EventMessage {
        qos: 2,
        ..message(Some(&o), &payload)
    };
    let ev = EventInput::message_acked(&receiver(), &msg, no_props(), no_props());
    assert_eq!(
        one(
            "SELECT puback_props, qos, clientid, from_clientid FROM \"$events/message/acked\"",
            &ev
        ),
        r#"{"puback_props":{"User-Property":{}},"qos":2,"clientid":"sub1","from_clientid":"pub1"}"#
    );
    let reasoned = printable_props(&[("why", "ok")], [("Reason-String", Value::from("fine"))]);
    let ev = EventInput::message_acked(&receiver(), &msg, no_props(), reasoned);
    assert_eq!(
        one(
            "SELECT puback_props.'Reason-String' AS r, puback_props.'User-Property'.why AS w \
             FROM \"$events/message_acked\"",
            &ev
        ),
        r#"{"r":"fine","w":"ok"}"#
    );
}

/// `message.dropped`'s client columns are the PUBLISHER's — EMQX's captured one, for a
/// v3.1.1 client that sent no username, prints it as `undefined`.
#[test]
fn a_dropped_publish_names_its_publisher() {
    let payload = Bytes::from_static(b"none-r");
    let o = Origin {
        clientid: "pub2".into(),
        username: None,
        peer: Some("172.17.0.1:55388".parse().unwrap()),
        ..origin()
    };
    let msg = EventMessage {
        topic: "t/nobody2",
        retain: true,
        ..message(Some(&o), &payload)
    };
    let ev = EventInput::message_dropped("emqx@172.17.0.3", &msg, no_props(), "no_subscribers");
    assert_eq!(
        one(
            "SELECT pub_props, qos, clientid, topic, peerhost, username, payload, event, \
             peername, reason, flags FROM \"$events/message/dropped\"",
            &ev
        ),
        concat!(
            r#"{"pub_props":{"User-Property":{}},"qos":1,"clientid":"pub2","#,
            r#""topic":"t/nobody2","peerhost":"172.17.0.1","username":"undefined","#,
            r#""payload":"none-r","event":"message.dropped","#,
            r#""peername":"172.17.0.1:55388","reason":"no_subscribers","#,
            r#""flags":{"dup":false,"retain":true}}"#
        )
    );
    // A dropped publish has no receiver: none of the delivery's sender columns.
    assert!(ev.field("from_clientid").is_undefined());
    assert!(!names(&ev).contains(&"from_clientid".to_string()));
}

/// `delivery.dropped` carries EMQX's reason and both parties; for `no_local` they are
/// the same client.
#[test]
fn a_dropped_delivery_carries_its_reason_and_both_parties() {
    let payload = Bytes::from_static(b"self");
    let o = Origin {
        clientid: "nl1".into(),
        username: Some("nluser".into()),
        ..origin()
    };
    let me = ClientInfo {
        clientid: "nl1",
        username: Some("nluser"),
        ..receiver()
    };
    let msg = EventMessage {
        topic: "t/nl",
        ..message(Some(&o), &payload)
    };
    for reason in ["no_local", "expired", "queue_full", "qos0_msg", "too_large"] {
        let ev = EventInput::delivery_dropped(&me, &msg, no_props(), reason);
        assert_eq!(
            one(
                "SELECT from_username, from_clientid, clientid, username, reason, event \
                 FROM \"$events/message/delivery_dropped\"",
                &ev
            ),
            format!(
                r#"{{"from_username":"nluser","from_clientid":"nl1","clientid":"nl1","username":"nluser","reason":"{reason}","event":"delivery.dropped"}}"#
            )
        );
    }
}

/// A message that no longer carries its origin — read back from disk, or forwarded by a
/// peer that does not send one — still raises its events: the publisher's columns are
/// `undefined` (and present, as EMQX's builders always set them), the `id` is a fresh
/// one, and `publish_received_at` is the event's own time.
#[test]
fn a_message_without_its_origin_shows_its_publisher_as_undefined() {
    let payload = Bytes::from_static(b"p");
    let msg = message(None, &payload);
    let ev = EventInput::message_delivered(&receiver(), &msg, no_props());
    assert_eq!(names(&ev), sorted(&DELIVERED), "the columns stay");
    assert_eq!(
        one(
            "SELECT from_clientid, from_username, clientid FROM \"$events/message/delivered\"",
            &ev
        ),
        r#"{"from_clientid":"undefined","from_username":"undefined","clientid":"sub1"}"#
    );
    assert_eq!(json(&ev, "publish_received_at"), json(&ev, "timestamp"));
    let Value::Str(id) = ev.field("id") else {
        panic!("id is a string")
    };
    assert_eq!(id.len(), 32);
    assert!(id.bytes().all(|b| b.is_ascii_hexdigit()), "{id}");
    let again = EventInput::message_delivered(&receiver(), &msg, no_props());
    assert_ne!(json(&again, "id"), json(&ev, "id"), "a fresh id each time");

    let dropped = EventInput::message_dropped("n", &msg, no_props(), "no_subscribers");
    assert_eq!(names(&dropped), sorted(&DROPPED));
    assert_eq!(
        one(
            "SELECT clientid, username, peerhost, peername FROM \"$events/message/dropped\"",
            &dropped
        ),
        r#"{"clientid":"undefined","username":"undefined","peerhost":"undefined","peername":"undefined"}"#
    );
    // A receiver the broker knows only by its client id (an offline session).
    let offline = ClientInfo {
        clientid: "gone",
        username: None,
        peer: None,
        sockname: None,
        node: "n",
    };
    let lost = EventInput::delivery_dropped(&offline, &msg, no_props(), "queue_full");
    assert_eq!(
        one(
            "SELECT clientid, username, peerhost, peername FROM \"$events/delivery_dropped\"",
            &lost
        ),
        r#"{"clientid":"gone","username":"undefined","peerhost":"undefined","peername":"undefined"}"#
    );
}

/// The id a message's events report is its origin's, in EMQX's 32 upper-case hex
/// digits — the same on every event about that message.
#[test]
fn every_event_about_a_message_reports_the_same_id() {
    let payload = Bytes::from_static(b"p");
    let o = origin();
    let msg = message(Some(&o), &payload);
    let id = r#""00065D7F9D3905CE4259000016A40002""#;
    for ev in [
        EventInput::message_delivered(&receiver(), &msg, no_props()),
        EventInput::message_acked(&receiver(), &msg, no_props(), no_props()),
        EventInput::message_dropped("n", &msg, no_props(), "no_subscribers"),
        EventInput::delivery_dropped(&receiver(), &msg, no_props(), "expired"),
    ] {
        assert_eq!(json(&ev, "id"), id, "{:?}", ev.kind());
        assert_eq!(json(&ev, "publish_received_at"), "1791652540253");
    }
}

/// `flags` are the delivery's: `dup` on a resend, `retain` for a retained message sent
/// at subscribe (EMQX's captured one has `"retain":true`).
#[test]
fn the_flags_are_the_delivery_s() {
    let payload = Bytes::from_static(b"p");
    let o = origin();
    let msg = EventMessage {
        dup: true,
        retain: true,
        ..message(Some(&o), &payload)
    };
    let ev = EventInput::message_delivered(&receiver(), &msg, no_props());
    assert_eq!(
        one(
            "SELECT flags.dup AS dup, flags.retain AS retain FROM \"$events/message/delivered\"",
            &ev
        ),
        r#"{"dup":true,"retain":true}"#
    );
}

/// Both spellings of each event load, select the same event, and a rule on one does
/// not run on another.
#[test]
fn both_spellings_select_the_same_event() {
    for (namespaced, legacy, kind) in [
        (
            "$events/message/delivered",
            "$events/message_delivered",
            EventKind::MessageDelivered,
        ),
        (
            "$events/message/acked",
            "$events/message_acked",
            EventKind::MessageAcked,
        ),
        (
            "$events/message/dropped",
            "$events/message_dropped",
            EventKind::MessageDropped,
        ),
        (
            "$events/message/delivery_dropped",
            "$events/delivery_dropped",
            EventKind::DeliveryDropped,
        ),
    ] {
        assert_eq!(kind.topic(), namespaced);
        for topic in [namespaced, legacy] {
            assert_eq!(EventKind::from_topic(topic), Some(kind), "{topic}");
            assert_eq!(EventKind::parse(topic), Some(kind), "{topic}");
            let set = RuleSet::parse(&format!(
                "[rules.r]\nsql = 'SELECT topic FROM \"{topic}\"'\nactions = [{{ function = \"console\" }}]\n"
            ))
            .unwrap_or_else(|e| panic!("{topic}: {e}"));
            assert!(set.warnings.is_empty(), "{topic}: {:?}", set.warnings);
            for k in EventKind::ALL {
                assert_eq!(set.rules.wants_event(k), k == kind, "{topic} / {k:?}");
            }
            assert!(set.rules.wants_message_events());
        }
        assert_eq!(EventKind::parse(kind.event_name()), Some(kind));
    }
    let clients = RuleSet::parse(
        "[rules.r]\nsql = 'SELECT clientid FROM \"$events/client/connected\"'\nactions = [{ function = \"console\" }]\n",
    )
    .unwrap();
    assert!(
        !clients.rules.wants_message_events(),
        "a client event carries no message origin"
    );
}

fn republishing(from: &str) -> RuleSet {
    RuleSet::parse(&format!(
        "[rules.audit]\nsql = 'SELECT topic, from_clientid FROM \"{from}\"'\n\
         actions = [{{ function = \"republish\", args = {{ topic = \"audit/${{topic}}\", payload = \"${{from_clientid}}\" }} }}]\n"
    ))
    .unwrap()
    .rules
}

fn run(set: &RuleSet, ev: &EventInput) -> (Vec<(Arc<str>, Effect)>, Vec<Recursion>) {
    let mut out = Vec::new();
    let mut guards = Vec::new();
    set.on_event(
        ev,
        &mut |_, o| {
            if let Outcome::Recursive(g) = o {
                guards.push(g);
            }
        },
        &mut out,
    );
    (out, guards)
}

/// A rule republishing from a message event does not republish again from the event
/// about its own republished message — EMQX's `republish_by` guard, which its message
/// events carry in their `headers` — so a subscriber of what the rule publishes cannot
/// drive it in a loop. Another rule's message is not held back by it.
#[test]
fn a_rule_does_not_republish_from_an_event_about_its_own_message() {
    let set = republishing("$events/message/delivered");
    let payload = Bytes::from_static(b"p");
    let o = origin();
    let (out, guards) = run(
        &set,
        &EventInput::message_delivered(&receiver(), &message(Some(&o), &payload), no_props()),
    );
    assert!(guards.is_empty());
    assert!(
        matches!(&out[..], [(rule, Effect::Republish(r))] if &**rule == "audit" && r.topic == "audit/t/a" && &r.payload[..] == b"pub1"),
        "{out:?}"
    );

    let own = Origin {
        clientid: "audit".into(),
        username: None,
        peer: None,
        republished: true,
        republish_depth: 1,
        ..origin()
    };
    let (out, guards) = run(
        &set,
        &EventInput::message_delivered(&receiver(), &message(Some(&own), &payload), no_props()),
    );
    assert!(out.is_empty(), "{out:?}");
    assert_eq!(guards, [Recursion::SameRule]);

    // A client that merely shares the rule's id is not the rule.
    let namesake = Origin {
        clientid: "audit".into(),
        ..origin()
    };
    let (out, guards) = run(
        &set,
        &EventInput::message_delivered(
            &receiver(),
            &message(Some(&namesake), &payload),
            no_props(),
        ),
    );
    assert_eq!(out.len(), 1);
    assert!(guards.is_empty());

    let other = Origin {
        clientid: "another-rule".into(),
        republished: true,
        republish_depth: 1,
        ..origin()
    };
    let (out, guards) = run(
        &set,
        &EventInput::message_delivered(&receiver(), &message(Some(&other), &payload), no_props()),
    );
    assert_eq!(out.len(), 1, "another rule's message is republished from");
    assert!(guards.is_empty());
}

/// Rules that republish into each other through the message events stop
/// [`crate::MAX_REPUBLISH_DEPTH`] republishes deep, as rules republishing into each
/// other's `FROM` do: the depth travels in the message's origin.
#[test]
fn a_chain_through_the_message_events_stops_at_the_depth_guard() {
    let set = republishing("$events/message/dropped");
    let payload = Bytes::from_static(b"p");
    let deep = |depth| Origin {
        clientid: "another-rule".into(),
        republished: true,
        republish_depth: depth,
        ..origin()
    };
    let at = |depth| {
        let o = deep(depth);
        run(
            &set,
            &EventInput::message_dropped(
                "n",
                &message(Some(&o), &payload),
                no_props(),
                "no_subscribers",
            ),
        )
    };
    let (out, guards) = at(crate::MAX_REPUBLISH_DEPTH - 1);
    assert_eq!(out.len(), 1);
    assert!(guards.is_empty());
    let (out, guards) = at(crate::MAX_REPUBLISH_DEPTH);
    assert!(out.is_empty(), "{out:?}");
    assert_eq!(guards, [Recursion::Depth]);
}

/// A republish from a message event that asks for the publisher's user properties
/// (`${pub_props.'User-Property'}`) gets the message's, in wire order, as one from a
/// publish does; and its `flags` have `dup`.
#[test]
fn a_republish_from_a_message_event_copies_the_message_s_user_properties() {
    let set = RuleSet::parse(
        "[rules.audit]\nsql = 'SELECT topic FROM \"$events/message/acked\"'\n\
         actions = [{ function = \"republish\", args = { topic = \"audit/${topic}\", \
         user_properties = \"${pub_props.'User-Property'}\" } }]\n",
    )
    .unwrap()
    .rules;
    let payload = Bytes::from_static(b"p");
    let o = origin();
    let user = [("k".to_string(), "v".to_string())];
    let msg = EventMessage {
        user_properties: &user,
        ..message(Some(&o), &payload)
    };
    let ev = EventInput::message_acked(&receiver(), &msg, no_props(), no_props());
    assert_eq!(ev.user_properties(), &user);
    assert!(ev.has_dup_flag());
    assert_eq!(ev.payload(), Some(&payload));
    let (out, _) = run(&set, &ev);
    let [(_, Effect::Republish(r))] = &out[..] else {
        panic!("{out:?}")
    };
    assert_eq!(r.app.user_properties, user);
    assert!(r.dup_flag);
}

/// `mqttd --rule-test --event message.delivered` and the admin API's dry run evaluate a
/// sample: the given client as both sender and receiver, the given topic, `QoS` and
/// payload, and EMQX's SQL-test reasons for the two drop events.
#[test]
fn the_sql_test_has_a_sample_of_each_message_event() {
    let c = receiver();
    let payload = Bytes::from_static(br#"{"msg": "hello"}"#);
    for (kind, sql, want) in [
        (
            EventKind::MessageDelivered,
            "SELECT event, from_clientid, clientid, topic, qos, payload.msg AS msg FROM \"$events/message/delivered\"",
            r#"{"event":"message.delivered","from_clientid":"sub1","clientid":"sub1","topic":"t/9","qos":2,"msg":"hello"}"#,
        ),
        (
            EventKind::MessageAcked,
            "SELECT event, puback_props FROM \"$events/message/acked\"",
            r#"{"event":"message.acked","puback_props":{"User-Property":{}}}"#,
        ),
        (
            EventKind::MessageDropped,
            "SELECT event, reason, clientid, username, peerhost FROM \"$events/message/dropped\"",
            r#"{"event":"message.dropped","reason":"no_subscribers","clientid":"sub1","username":"subuser","peerhost":"172.17.0.1"}"#,
        ),
        (
            EventKind::DeliveryDropped,
            "SELECT event, reason, from_username FROM \"$events/message/delivery_dropped\"",
            r#"{"event":"delivery.dropped","reason":"queue_full","from_username":"subuser"}"#,
        ),
    ] {
        let ev = EventInput::sample_message(kind, &c, "t/9", 2, &payload);
        assert_eq!(ev.kind(), kind);
        assert_eq!(one(sql, &ev), want);
        // `sample` is the same with EMQX's example payload.
        let ev = EventInput::sample(kind, &c, "t/9", 2);
        assert_eq!(json(&ev, "payload"), r#""{\"msg\": \"hello\"}""#);
    }
    // A sample of another event is refused, as for every event statement.
    let e = test_sql(
        "SELECT * FROM \"$events/message/delivered\"",
        &EventInput::sample(EventKind::MessageAcked, &c, "t", 0),
    )
    .unwrap_err();
    assert!(e.contains("does not select the message.acked event"), "{e}");
}
