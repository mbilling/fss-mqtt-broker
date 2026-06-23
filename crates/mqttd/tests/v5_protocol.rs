//! End-to-end sunshine tests for the MQTT 5.0 feature surface (ADRs 0008–0013):
//! session/message expiry, shared subscriptions, topic aliases, flow control, and
//! enhanced authentication — exercised over real TCP with the project codec.
//!
//! See `docs/TEST-PLAN.md`. These were the largest coverage gap: every v5 feature
//! had only unit/`conn`-module tests before this suite.

mod common;

use std::time::Duration;

use common::{enhanced, start_broker, start_broker_with_policy, Client};
use mqtt_codec::{packet::Connect, Packet, Properties, Property, ProtocolVersion, QoS};

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

    // Wait past the 1s interval plus the 1s sweep cadence (ADR 0009), with margin for
    // a loaded CI runner where the sweep interval can slip. Probing earlier is not an
    // option: reconnecting to this client id would cancel the pending expiry. The
    // expiry *logic* is covered deterministically by the paused-time unit tests; this
    // only confirms the end-to-end wire path, so a generous fixed wait is fine.
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
