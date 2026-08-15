//! End-to-end sunshine tests for the MQTT 5.0 feature surface (ADRs 0008–0013):
//! session/message expiry, shared subscriptions, topic aliases, flow control, and
//! enhanced authentication — exercised over real TCP with the project codec.
//!
//! See `docs/TEST-PLAN.md`. These were the largest coverage gap: every v5 feature
//! had only unit/`conn`-module tests before this suite.

mod common;

use std::time::Duration;

use common::{enhanced, start_broker, start_broker_with_policy, Client};
use mqtt_codec::{
    packet::{Connect, LastWill},
    Packet, Properties, Property, ProtocolVersion, QoS,
};

fn find<F, T>(props: &Properties, f: F) -> Option<T>
where
    F: Fn(&Property) -> Option<T>,
{
    props.0.iter().find_map(f)
}

fn topic_alias(props: &Properties) -> Option<u16> {
    find(props, |p| match p {
        Property::TopicAlias(v) => Some(*v),
        _ => None,
    })
}

fn message_expiry(props: &Properties) -> Option<u32> {
    find(props, |p| match p {
        Property::MessageExpiryInterval(v) => Some(*v),
        _ => None,
    })
}

// --- core round-trip ---------------------------------------------------------

#[tokio::test]
async fn v5_connect_and_pubsub_roundtrip() {
    let addr = start_broker().await;
    let mut sub = Client::connect_v5_ok(addr, "v5-sub").await;
    sub.subscribe(1, "sensors/+/temp", QoS::AtMostOnce).await;

    let mut pubr = Client::connect_v5_ok(addr, "v5-pub").await;
    pubr.publish(
        "sensors/kitchen/temp",
        b"21.5C",
        QoS::AtMostOnce,
        None,
        vec![],
    )
    .await;

    let p = sub.expect_publish().await;
    assert_eq!(p.topic, "sensors/kitchen/temp");
    assert_eq!(&p.payload[..], b"21.5C");
}

/// ADR 0030-T1: the broker forwards a publisher's User Properties unaltered to a
/// subscriber (MQTT-3.3.2-17), in order.
#[tokio::test]
async fn v5_user_properties_are_forwarded_to_subscribers() {
    let addr = start_broker().await;
    let mut sub = Client::connect_v5_ok(addr, "up-sub").await;
    sub.subscribe(1, "up/+", QoS::AtMostOnce).await;

    let mut pubr = Client::connect_v5_ok(addr, "up-pub").await;
    pubr.publish(
        "up/x",
        b"body",
        QoS::AtMostOnce,
        None,
        vec![
            Property::UserProperty("k1".into(), "v1".into()),
            Property::UserProperty("k2".into(), "v2".into()),
        ],
    )
    .await;

    let p = sub.expect_publish().await;
    let got: Vec<(String, String)> = p
        .properties
        .0
        .iter()
        .filter_map(|prop| match prop {
            Property::UserProperty(k, v) => Some((k.clone(), v.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string())
        ]
    );
}

/// ADR 0030-T5: the broker forwards the other message-level application properties —
/// Content Type, Response Topic, Correlation Data, Payload Format — unaltered, alongside
/// User Properties.
#[tokio::test]
async fn v5_application_properties_are_forwarded_to_subscribers() {
    let addr = start_broker().await;
    let mut sub = Client::connect_v5_ok(addr, "ap-sub").await;
    sub.subscribe(1, "ap/+", QoS::AtMostOnce).await;

    let mut pubr = Client::connect_v5_ok(addr, "ap-pub").await;
    pubr.publish(
        "ap/x",
        b"{}",
        QoS::AtMostOnce,
        None,
        vec![
            Property::PayloadFormatIndicator(1),
            Property::ContentType("application/json".into()),
            Property::ResponseTopic("ap/reply".into()),
            Property::CorrelationData(bytes::Bytes::from_static(b"\x00\x01id")),
            Property::UserProperty("trace".into(), "abc".into()),
        ],
    )
    .await;

    let p = sub.expect_publish().await;
    let pf = find(&p.properties, |prop| match prop {
        Property::PayloadFormatIndicator(v) => Some(*v),
        _ => None,
    });
    let ct = find(&p.properties, |prop| match prop {
        Property::ContentType(s) => Some(s.clone()),
        _ => None,
    });
    let rt = find(&p.properties, |prop| match prop {
        Property::ResponseTopic(s) => Some(s.clone()),
        _ => None,
    });
    let cd = find(&p.properties, |prop| match prop {
        Property::CorrelationData(b) => Some(b.clone()),
        _ => None,
    });
    let up = find(&p.properties, |prop| match prop {
        Property::UserProperty(k, v) if k == "trace" => Some(v.clone()),
        _ => None,
    });
    assert_eq!(pf, Some(1));
    assert_eq!(ct.as_deref(), Some("application/json"));
    assert_eq!(rt.as_deref(), Some("ap/reply"));
    assert_eq!(cd.as_deref(), Some(&b"\x00\x01id"[..]));
    assert_eq!(up.as_deref(), Some("abc"));
}

/// ADR 0030-T4: a Will message's User Properties are forwarded when the will fires.
#[tokio::test]
async fn v5_will_user_properties_are_forwarded() {
    let addr = start_broker().await;
    let mut sub = Client::connect_v5_ok(addr, "will-up-sub").await;
    sub.subscribe(1, "will/up", QoS::AtLeastOnce).await;

    // A publisher whose Will carries a User Property, then an abrupt drop (no DISCONNECT).
    let mut pubr = Client::open(addr, ProtocolVersion::V5).await;
    pubr.send(&Packet::Connect(Connect {
        properties: Properties::new(),
        protocol: ProtocolVersion::V5,
        clean_session: true,
        keep_alive: 30,
        client_id: "will-up-pub".to_string(),
        last_will: Some(LastWill {
            topic: "will/up".to_string(),
            payload: bytes::Bytes::from_static(b"gone"),
            qos: QoS::AtLeastOnce,
            retain: false,
            properties: Properties(vec![Property::UserProperty(
                "reason".into(),
                "crash".into(),
            )]),
        }),
        username: None,
        password: None,
    }))
    .await;
    match pubr.recv().await {
        Packet::ConnAck(a) => assert_eq!(a.code, 0),
        other => panic!("expected CONNACK, got {other:?}"),
    }
    drop(pubr); // abrupt close fires the will

    let p = sub.expect_publish().await;
    assert_eq!(p.topic, "will/up");
    let reason = p.properties.0.iter().find_map(|prop| match prop {
        Property::UserProperty(k, v) if k == "reason" => Some(v.clone()),
        _ => None,
    });
    assert_eq!(
        reason.as_deref(),
        Some("crash"),
        "the will's user property must forward"
    );
}

// --- will delay (§3.1.3.2.2, issue #299) ------------------------------------

/// Open a v5 connection whose Will carries a Will Delay Interval, and whose
/// session lives long enough for the delay to matter.
async fn connect_with_delayed_will(
    addr: std::net::SocketAddr,
    client_id: &str,
    topic: &str,
    delay_secs: u32,
    session_expiry: u32,
) -> Client {
    let mut c = Client::open(addr, ProtocolVersion::V5).await;
    c.send(&Packet::Connect(Connect {
        properties: Properties(vec![Property::SessionExpiryInterval(session_expiry)]),
        protocol: ProtocolVersion::V5,
        clean_session: false,
        keep_alive: 30,
        client_id: client_id.to_string(),
        last_will: Some(LastWill {
            topic: topic.to_string(),
            payload: bytes::Bytes::from_static(b"gone"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties(vec![Property::WillDelayInterval(delay_secs)]),
        }),
        username: None,
        password: None,
    }))
    .await;
    match c.recv().await {
        Packet::ConnAck(a) => assert_eq!(a.code, 0),
        other => panic!("expected CONNACK, got {other:?}"),
    }
    c
}

/// §3.1.3.2.2: the Will is held for its Will Delay Interval, not published the
/// instant the connection drops.
///
/// This is what the property is FOR: a brief network blip must not announce a
/// death that did not happen. Found by the Eclipse `paho.mqtt.testing` oracle
/// (`test_will_delay`, issue #299), which measured the Will arriving at 0.1 s
/// where 4 s had been asked for.
#[tokio::test]
async fn v5_a_will_is_held_for_its_delay_then_published() {
    let addr = start_broker().await;
    let mut watcher = Client::connect_v5_ok(addr, "delay-watch").await;
    watcher.subscribe(1, "wills/delayed", QoS::AtMostOnce).await;

    let dying = connect_with_delayed_will(addr, "delay-dies", "wills/delayed", 2, 300).await;
    drop(dying); // abrupt close — ungraceful, so the Will is owed

    // Not immediately: the whole point of the delay.
    assert!(
        matches!(
            watcher.recv_bounded(Duration::from_millis(700)).await,
            common::Recv::Quiet
        ),
        "the will must be HELD for its delay, not published on the drop"
    );

    // ...and then it arrives, once the delay has elapsed (2s delay + the 1s sweep
    // cadence, with margin for a loaded runner).
    let p = watcher.expect_publish().await;
    assert_eq!(p.topic, "wills/delayed");
    assert_eq!(&p.payload[..], b"gone");
}

/// The half that makes the delay worth having: a client that comes back inside
/// the window cancels its own Will. Without this the feature is only a slower
/// announcement of a death that did not happen.
#[tokio::test]
async fn v5_a_will_is_cancelled_by_a_reconnect_inside_the_delay() {
    let addr = start_broker().await;
    let mut watcher = Client::connect_v5_ok(addr, "cancel-watch").await;
    watcher
        .subscribe(1, "wills/cancelled", QoS::AtMostOnce)
        .await;

    let dying = connect_with_delayed_will(addr, "cancel-dies", "wills/cancelled", 3, 300).await;
    drop(dying);

    // Back well inside the 3s window.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let _back = connect_with_delayed_will(addr, "cancel-dies", "wills/cancelled", 3, 300).await;

    // Past when the will would have fired had the reconnect not cancelled it.
    assert!(
        matches!(
            watcher.recv_bounded(Duration::from_secs(5)).await,
            common::Recv::Quiet
        ),
        "a client that returned inside its will delay must not have its will published"
    );
}

/// §3.1.3.2.2 publishes on "delay elapsed" OR "session ended", whichever is
/// FIRST — so a delay longer than the session's own lifetime is bounded by it.
/// A Will must never outlive the session it describes.
#[tokio::test]
async fn v5_a_will_delay_is_bounded_by_the_session_expiry() {
    let addr = start_broker().await;
    let mut watcher = Client::connect_v5_ok(addr, "bound-watch").await;
    watcher.subscribe(1, "wills/bounded", QoS::AtMostOnce).await;

    // Expiry 0: the session ends the moment the connection does, so the will is
    // due at once however long a delay was requested.
    let dying = connect_with_delayed_will(addr, "bound-dies", "wills/bounded", 3600, 0).await;
    drop(dying);

    let p = watcher.expect_publish().await;
    assert_eq!(
        p.topic, "wills/bounded",
        "a session that ends at once publishes its will at once, delay or not"
    );
}

// --- session expiry (ADR 0009 phase 1) --------------------------------------

#[tokio::test]
async fn v5_persistent_session_resumes_within_expiry_window() {
    let addr = start_broker().await;
    let (mut sub, present) = Client::connect_v5(
        addr,
        "durable-v5",
        false,
        vec![Property::SessionExpiryInterval(300)],
    )
    .await;
    assert!(!present.session_present, "no session yet");
    sub.subscribe(1, "offline/t", QoS::AtMostOnce).await;
    sub.disconnect().await;

    // Publish while offline, then reconnect within the expiry window.
    let mut pubr = Client::connect_v5_ok(addr, "pub-x").await;
    pubr.publish("offline/t", b"queued", QoS::AtMostOnce, None, vec![])
        .await;

    let (mut sub, ack) = Client::connect_v5(
        addr,
        "durable-v5",
        false,
        vec![Property::SessionExpiryInterval(300)],
    )
    .await;
    assert!(
        ack.session_present,
        "session resumes within the expiry window"
    );
    let p = sub.expect_publish().await;
    assert_eq!(&p.payload[..], b"queued");
}

#[tokio::test]
async fn v5_session_expires_after_interval() {
    let addr = start_broker().await;
    let (mut sub, _) = Client::connect_v5(
        addr,
        "shortlived",
        false,
        vec![Property::SessionExpiryInterval(1)],
    )
    .await;
    sub.subscribe(1, "t", QoS::AtMostOnce).await;
    sub.disconnect().await;

    // SETTLE(v5-session-expiry-wire): the observable would DESTROY the subject. The only way
    // to ask "has this session expired?" over the wire is to reconnect with the same client id,
    // and reconnecting CANCELS the pending expiry — so a poll cannot exist here, by
    // construction. 4 s covers the 1 s interval plus the 1 s sweep cadence (ADR 0009) with
    // margin for a loaded runner whose sweep slips; the expiry *logic* is covered
    // deterministically by the paused-clock unit tests, and this only pins the end-to-end wire
    // path. On a slow machine the sweep runs later in wall-clock terms but the wait is longer
    // too, and the failure mode is a false FAILURE (session still present), never a false pass.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let (_sub, ack) = Client::connect_v5(
        addr,
        "shortlived",
        false,
        vec![Property::SessionExpiryInterval(1)],
    )
    .await;
    assert!(!ack.session_present, "the session expired and was swept");
}

/// §3.14.2.2.2: a Session Expiry Interval on the DISCONNECT **overrides** the one
/// agreed at CONNECT.
///
/// This is the documented way for a client to say "I connected expecting to be
/// brief, but hold my session — I will be back". Ignoring it fails silently: the
/// client gets a clean DISCONNECT and only discovers the loss on reconnect, with
/// its subscriptions and queued messages gone. Found by the Eclipse
/// `paho.mqtt.testing` oracle (`test_session_expiry`, issue #298).
#[tokio::test]
async fn v5_disconnect_session_expiry_overrides_the_connect_value() {
    let addr = start_broker().await;
    let (mut sub, _) = Client::connect_v5(
        addr,
        "extends-on-exit",
        false,
        vec![Property::SessionExpiryInterval(1)],
    )
    .await;
    sub.subscribe(1, "t", QoS::AtMostOnce).await;
    // Connected for 1s, leaving for 300 — the session must survive on the 300.
    sub.disconnect_with(vec![Property::SessionExpiryInterval(300)])
        .await;

    // Past the CONNECT's 1s and the 1s sweep, with margin for a loaded runner. The
    // same generous fixed wait as `v5_session_expires_after_interval`, and for the
    // same reason: reconnecting earlier would cancel the pending expiry outright,
    // so the wait has to straddle the window rather than probe it.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let (_sub, ack) = Client::connect_v5(
        addr,
        "extends-on-exit",
        false,
        vec![Property::SessionExpiryInterval(300)],
    )
    .await;
    assert!(
        ack.session_present,
        "the DISCONNECT raised the expiry to 300s, so the session must outlive \
         the 1s agreed at CONNECT [MQTT-3.14.2.2.2]"
    );
}

/// The other half of §3.14.2.2.2, and the half that is easy to skip: if the
/// CONNECT's interval was **0**, a non-zero one on the DISCONNECT is a Protocol
/// Error. A session that was never meant to outlive its connection cannot be
/// resurrected on the way out.
#[tokio::test]
async fn v5_disconnect_cannot_raise_an_expiry_the_connect_set_to_zero() {
    let addr = start_broker().await;
    let (mut sub, _) = Client::connect_v5(
        addr,
        "zero-then-nonzero",
        false,
        vec![Property::SessionExpiryInterval(0)],
    )
    .await;
    sub.subscribe(1, "t", QoS::AtMostOnce).await;
    sub.send(&mqtt_codec::Packet::Disconnect(
        mqtt_codec::packet::Disconnect {
            reason: 0,
            properties: mqtt_codec::Properties(vec![Property::SessionExpiryInterval(300)]),
        },
    ))
    .await;
    // Announced, not merely closed: this is post-CONNACK, so [MQTT-4.13.2] wants
    // the reason on the wire before the close.
    match sub.recv().await {
        mqtt_codec::Packet::Disconnect(d) => assert_eq!(
            d.reason,
            mqtt_codec::reason::PROTOCOL_ERROR,
            "raising a zero expiry on DISCONNECT is a Protocol Error [MQTT-3.14.2.2.2]"
        ),
        other => panic!("expected a server DISCONNECT(0x82), got {other:?}"),
    }

    // And the refusal must not have applied the override: the session is still
    // expiry-0, so it is gone.
    let (_sub, ack) = Client::connect_v5(
        addr,
        "zero-then-nonzero",
        false,
        vec![Property::SessionExpiryInterval(300)],
    )
    .await;
    assert!(
        !ack.session_present,
        "the refused override must not have been applied"
    );
}

// --- message expiry (ADR 0009 phase 2) --------------------------------------

#[tokio::test]
async fn v5_expired_queued_message_dropped_remaining_interval_forwarded() {
    let addr = start_broker().await;
    let (mut sub, _) = Client::connect_v5(
        addr,
        "exp-sub",
        false,
        vec![Property::SessionExpiryInterval(300)],
    )
    .await;
    sub.subscribe(1, "m", QoS::AtLeastOnce).await;
    sub.disconnect().await;

    let mut pubr = Client::connect_v5_ok(addr, "exp-pub").await;
    // A 0-second interval is stale the instant it is received, so it is always
    // dropped by the time the session reconnects; the fresh one survives.
    pubr.publish(
        "m",
        b"stale",
        QoS::AtLeastOnce,
        Some(1),
        vec![Property::MessageExpiryInterval(0)],
    )
    .await;
    assert_eq!(pubr.recv().await, Packet::PubAck(1.into()));
    pubr.publish(
        "m",
        b"fresh",
        QoS::AtLeastOnce,
        Some(2),
        vec![Property::MessageExpiryInterval(3600)],
    )
    .await;
    assert_eq!(pubr.recv().await, Packet::PubAck(2.into()));

    let (mut sub, _) = Client::connect_v5(
        addr,
        "exp-sub",
        false,
        vec![Property::SessionExpiryInterval(300)],
    )
    .await;
    let p = sub.expect_publish().await;
    assert_eq!(&p.payload[..], b"fresh", "the expired message is skipped");
    let remaining = message_expiry(&p.properties).expect("a forwarded expiry interval");
    assert!(
        remaining > 0 && remaining <= 3600,
        "remaining interval bounded: {remaining}"
    );
}

// --- shared subscriptions (ADR 0010) ----------------------------------------

#[tokio::test]
async fn v5_shared_subscription_round_robins_one_member_each() {
    let addr = start_broker().await;
    let mut a = Client::connect_v5_ok(addr, "share-a").await;
    a.subscribe(1, "$share/grp/t/+", QoS::AtMostOnce).await;
    let mut b = Client::connect_v5_ok(addr, "share-b").await;
    b.subscribe(1, "$share/grp/t/+", QoS::AtMostOnce).await;

    let mut pubr = Client::connect_v5_ok(addr, "share-pub").await;
    pubr.publish("t/1", b"m1", QoS::AtMostOnce, None, vec![])
        .await;
    pubr.publish("t/2", b"m2", QoS::AtMostOnce, None, vec![])
        .await;

    // One message each, round-robin in subscribe order; no duplicates.
    assert_eq!(&a.expect_publish().await.payload[..], b"m1");
    assert_eq!(&b.expect_publish().await.payload[..], b"m2");
    a.expect_silence().await;
    b.expect_silence().await;
}

#[tokio::test]
async fn v5_shared_subscription_skips_retained_but_ordinary_gets_it() {
    let addr = start_broker().await;
    let mut pubr = Client::connect_v5_ok(addr, "ret-pub").await;
    // Acked so the retained message is stored before the ordinary subscriber below
    // subscribes, which must observe it via retained-replay (retain=1).
    pubr.publish_retained_acked("t", b"r", 1).await;

    let mut shared = Client::connect_v5_ok(addr, "ret-shared").await;
    shared.subscribe(1, "$share/g/t", QoS::AtMostOnce).await;
    shared.expect_silence().await; // no retained for shared subs [MQTT-3.8.4]

    let mut ordinary = Client::connect_v5_ok(addr, "ret-ord").await;
    ordinary.subscribe(1, "t", QoS::AtMostOnce).await;
    let p = ordinary.expect_publish().await;
    assert_eq!(&p.payload[..], b"r");
    assert!(p.retain, "ordinary subscriber gets the retained flag set");
}

// --- topic aliases (ADR 0011) -----------------------------------------------

#[tokio::test]
async fn v5_inbound_topic_alias_resolves_to_full_topic() {
    let addr = start_broker().await;
    let mut sub = Client::connect_v5_ok(addr, "ta-sub").await;
    sub.subscribe(1, "room/+", QoS::AtMostOnce).await;

    let mut pubr = Client::connect_v5_ok(addr, "ta-pub").await;
    // Establish alias 2 -> "room/x", then reference it with an empty topic.
    pubr.publish(
        "room/x",
        b"first",
        QoS::AtMostOnce,
        None,
        vec![Property::TopicAlias(2)],
    )
    .await;
    pubr.publish(
        "",
        b"second",
        QoS::AtMostOnce,
        None,
        vec![Property::TopicAlias(2)],
    )
    .await;

    let p1 = sub.expect_publish().await;
    assert_eq!(p1.topic, "room/x");
    assert_eq!(&p1.payload[..], b"first");
    let p2 = sub.expect_publish().await;
    assert_eq!(
        p2.topic, "room/x",
        "the reference resolves to the full topic"
    );
    assert_eq!(&p2.payload[..], b"second");
}

#[tokio::test]
async fn v5_outbound_topic_alias_assigned_then_referenced() {
    let addr = start_broker().await;
    // The subscriber invites the server to alias outbound by advertising a maximum.
    let (mut sub, _) =
        Client::connect_v5(addr, "ota-sub", true, vec![Property::TopicAliasMaximum(5)]).await;
    sub.subscribe(1, "room/+", QoS::AtMostOnce).await;

    let mut pubr = Client::connect_v5_ok(addr, "ota-pub").await;
    pubr.publish("room/a", b"1", QoS::AtMostOnce, None, vec![])
        .await;
    pubr.publish("room/a", b"2", QoS::AtMostOnce, None, vec![])
        .await;

    let p1 = sub.expect_publish().await;
    assert_eq!(p1.topic, "room/a", "first send keeps the full topic");
    assert_eq!(topic_alias(&p1.properties), Some(1));
    let p2 = sub.expect_publish().await;
    assert_eq!(p2.topic, "", "second send references the alias");
    assert_eq!(topic_alias(&p2.properties), Some(1));
}

// --- flow control (ADR 0012) ------------------------------------------------

#[tokio::test]
async fn v5_receive_maximum_limits_inflight_until_acked() {
    let addr = start_broker().await;
    let (mut sub, _) =
        Client::connect_v5(addr, "fc-sub", true, vec![Property::ReceiveMaximum(1)]).await;
    sub.subscribe(1, "t", QoS::AtLeastOnce).await;

    let mut pubr = Client::connect_v5_ok(addr, "fc-pub").await;
    pubr.publish("t", b"m1", QoS::AtLeastOnce, Some(1), vec![])
        .await;
    pubr.publish("t", b"m2", QoS::AtLeastOnce, Some(2), vec![])
        .await;

    // Quota of 1: only the first is in flight; the second waits for the PUBACK.
    let p1 = sub.expect_publish().await;
    assert_eq!(&p1.payload[..], b"m1");
    sub.expect_silence().await;
    sub.puback(p1.pkid.expect("QoS1 publish has a packet id"))
        .await;
    let p2 = sub.expect_publish().await;
    assert_eq!(&p2.payload[..], b"m2", "the backlog drains on PUBACK");
}

// --- enhanced authentication + re-auth (ADR 0013) ---------------------------

#[tokio::test]
async fn v5_enhanced_auth_then_reauthentication() {
    let addr = start_broker_with_policy(enhanced::policy()).await;
    let mut c = Client::open(addr, ProtocolVersion::V5).await;

    // CONNECT names the method and seeds the exchange with the subject.
    c.send(&Packet::Connect(Connect {
        properties: Properties(vec![
            Property::AuthenticationMethod(enhanced::METHOD.into()),
            Property::AuthenticationData(bytes::Bytes::copy_from_slice(
                enhanced::SUBJECT.as_bytes(),
            )),
        ]),
        protocol: ProtocolVersion::V5,
        clean_session: true,
        keep_alive: 30,
        client_id: "auth-client".into(),
        last_will: None,
        username: None,
        password: None,
    }))
    .await;

    // Challenge -> proof -> CONNACK success.
    let challenge = c.expect_auth().await;
    assert_eq!(challenge.reason, 0x18);
    let nonce = enhanced::nonce_of(&challenge.properties);
    c.send(&enhanced::auth(0x18, &enhanced::proof(&nonce)))
        .await;
    match c.recv().await {
        Packet::ConnAck(a) => assert_eq!(a.code, 0, "enhanced auth accepted"),
        other => panic!("expected CONNACK, got {other:?}"),
    }

    // Re-authenticate mid-session: AUTH 0x19 -> challenge -> proof -> AUTH 0x00.
    c.send(&enhanced::auth(0x19, enhanced::SUBJECT.as_bytes()))
        .await;
    let challenge = c.expect_auth().await;
    assert_eq!(challenge.reason, 0x18, "re-auth challenge");
    let nonce = enhanced::nonce_of(&challenge.properties);
    c.send(&enhanced::auth(0x18, &enhanced::proof(&nonce)))
        .await;
    assert_eq!(c.expect_auth().await.reason, 0x00, "re-auth succeeded");
}

// --- server-assigned client identifier ---------------------------------------

fn assigned_client_id(props: &Properties) -> Option<String> {
    find(props, |p| match p {
        Property::AssignedClientIdentifier(v) => Some(v.clone()),
        _ => None,
    })
}

/// MQTT 5.0 §3.2.2.3.7: a client that connects with a zero-length id MUST be told
/// which id the server picked.
///
/// The broker assigned one and never sent it back, so a v5 client could not learn
/// its own identity — the one that shows up in our audit log, in `%c` ACL
/// substitution, and in every message it publishes.
#[tokio::test]
async fn a_zero_length_client_id_is_assigned_and_returned() {
    let addr = start_broker().await;
    let (_c, ack) = Client::connect_v5(addr, "", true, vec![]).await;
    assert_eq!(ack.code, 0, "a zero-length id with clean start is legal");

    let assigned = assigned_client_id(&ack.properties)
        .expect("CONNACK must carry the Assigned Client Identifier (MQTT 5.0 §3.2.2.3.7)");
    assert!(!assigned.is_empty(), "assigned id must not itself be empty");

    // Two such clients must not be handed the same identity.
    let (_c2, ack2) = Client::connect_v5(addr, "", true, vec![]).await;
    let assigned2 = assigned_client_id(&ack2.properties).expect("second assignment");
    assert_ne!(
        assigned, assigned2,
        "two zero-id clients were given the same identity"
    );
}

/// A client that supplies its own id must NOT be told a different one — the
/// property is only for ids the server chose.
#[tokio::test]
async fn a_client_supplied_id_is_not_reassigned() {
    let addr = start_broker().await;
    let (_c, ack) = Client::connect_v5(addr, "i-chose-this", true, vec![]).await;
    assert_eq!(ack.code, 0);
    assert_eq!(
        assigned_client_id(&ack.properties),
        None,
        "the server must not claim to have assigned an id the client supplied"
    );
}

/// A zero-length id with `clean_start = false` has no session to resume, so it is
/// refused rather than silently given a fresh one.
#[tokio::test]
async fn a_zero_length_id_without_clean_start_is_refused() {
    let addr = start_broker().await;
    let (_c, ack) = Client::connect_v5(addr, "", false, vec![]).await;
    assert_ne!(
        ack.code, 0,
        "zero-length id + persistent session must be refused"
    );
}
