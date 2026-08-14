//! End-to-end darksky tests: protocol violations and authentication failures must
//! close the connection (or send the right reason code) without corrupting broker
//! state. These use the self-codec client to send packets a conformant library
//! would never emit. See `docs/TEST-PLAN.md`.

mod common;

use std::time::Duration;

use common::{enhanced, permissive_policy, start_broker, start_broker_with_policy, Client};
use mqtt_codec::{
    packet::{Auth, Connect, Publish, Subscribe, SubscribeFilter},
    Packet, Properties, Property, ProtocolVersion, QoS, SubscriptionOptions,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// --- protocol violations close the connection -------------------------------

#[tokio::test]
async fn publish_with_wildcard_topic_closes_connection() {
    let addr = start_broker().await;
    let mut c = Client::connect_v5_ok(addr, "wild").await;
    // [MQTT-3.3.2-2]: a PUBLISH topic must not contain wildcards.
    c.publish("a/+/b", b"x", QoS::AtMostOnce, None, vec![])
        .await;
    c.expect_closed().await;
}

#[tokio::test]
async fn first_packet_not_connect_closes_connection() {
    let addr = start_broker().await;
    let mut c = Client::open(addr, ProtocolVersion::V5).await;
    // A PUBLISH before any CONNECT: the broker must refuse the connection.
    c.send(&Packet::Publish(Publish {
        properties: Properties::new(),
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "t".into(),
        pkid: None,
        payload: bytes::Bytes::from_static(b"x"),
    }))
    .await;
    c.expect_closed().await;
}

// --- half-open / slow-loris: the connect deadline ---------------------------

#[tokio::test]
async fn connection_idle_before_connect_is_closed_after_deadline() {
    let addr = start_broker_with_policy(permissive_policy(Duration::from_millis(300))).await;
    // Open the socket and send nothing. The keepalive timer only starts after
    // CONNECT, so the connect deadline is what must reap this half-open connection.
    let mut c = Client::open(addr, ProtocolVersion::V5).await;
    c.expect_closed().await;
}

#[tokio::test]
async fn partial_connect_then_stall_is_closed_after_deadline() {
    let addr = start_broker_with_policy(permissive_policy(Duration::from_millis(300))).await;
    // A slow-loris: announce a CONNECT fixed header (remaining length 16) but send
    // only part of the body, then stall. The frame never completes, so the connect
    // deadline must close the connection.
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[0x10, 0x10, 0x00, 0x04, b'M', b'Q'])
        .await
        .unwrap();

    // A read returns 0 (EOF) once the broker closes the connection.
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .expect("broker should close the stalled connection")
        .expect("read");
    assert_eq!(n, 0, "the broker closed the half-sent CONNECT");
}

// --- topic-alias violations (ADR 0011) --------------------------------------

#[tokio::test]
async fn topic_alias_zero_closes_connection() {
    let addr = start_broker().await;
    let mut c = Client::connect_v5_ok(addr, "ta-zero").await;
    c.publish(
        "t",
        b"x",
        QoS::AtMostOnce,
        None,
        vec![Property::TopicAlias(0)],
    )
    .await;
    c.expect_disconnect(0x94).await;
}

#[tokio::test]
async fn topic_alias_above_maximum_closes_connection() {
    let addr = start_broker().await;
    let mut c = Client::connect_v5_ok(addr, "ta-big").await;
    // The server advertises a Topic Alias Maximum of 16; 99 is out of range.
    c.publish(
        "t",
        b"x",
        QoS::AtMostOnce,
        None,
        vec![Property::TopicAlias(99)],
    )
    .await;
    c.expect_disconnect(0x94).await;
}

#[tokio::test]
async fn unmapped_topic_alias_reference_closes_connection() {
    let addr = start_broker().await;
    let mut c = Client::connect_v5_ok(addr, "ta-unmapped").await;
    // Empty topic + an alias that was never established.
    c.publish(
        "",
        b"x",
        QoS::AtMostOnce,
        None,
        vec![Property::TopicAlias(5)],
    )
    .await;
    c.expect_disconnect(0x94).await;
}

// --- subscription-identifier violations (issue #245, MQTT 5.0 §3.2.2.3.12) --
//
// This server does not deliver Subscription Identifiers, and §3.2.2.3.12 makes an absent
// CONNACK property 0x29 mean "supported" — so it advertises 0 and refuses any use of them.

/// A v5 SUBSCRIBE carrying a Subscription Identifier is refused with DISCONNECT 0xA1
/// (Subscription Identifiers not supported), per §3.2.2.3.12's own prescription plus
/// `[MQTT-4.13.1-1]` (closing is the MUST). Then, honouring this file's contract — "without
/// corrupting broker state" — a normal subscriber and publisher still work end to end,
/// proving the refusal aborted before any subscription state was recorded.
#[tokio::test]
async fn v5_subscribe_with_subscription_identifier_disconnects_0xa1() {
    let addr = start_broker().await;
    let mut c = Client::connect_v5_ok(addr, "subid-refused").await;
    c.send(&Packet::Subscribe(Subscribe {
        properties: Properties(vec![Property::SubscriptionIdentifier(5)]),
        pkid: 1,
        filters: vec![SubscribeFilter {
            options: SubscriptionOptions::default(),
            path: "subid/t".into(),
            qos: QoS::AtLeastOnce,
        }],
    }))
    .await;
    c.expect_disconnect(0xA1).await;

    // The broker is healthy and holds no state from the refused SUBSCRIBE.
    let mut sub = Client::connect_v5_ok(addr, "subid-ok").await;
    assert_eq!(
        sub.subscribe(1, "subid/t", QoS::AtMostOnce)
            .await
            .return_codes,
        vec![0],
        "an identifier-free subscribe to the same filter is granted"
    );
    let mut pubr = Client::connect_v5_ok(addr, "subid-pub").await;
    pubr.publish("subid/t", b"hello", QoS::AtMostOnce, None, vec![])
        .await;
    assert_eq!(
        sub.expect_publish().await.payload,
        bytes::Bytes::from_static(b"hello"),
        "delivery still works after the refusal"
    );
}

/// A v5 SUBSCRIBE carrying `SubscriptionIdentifier(0)` is rejected at the CODEC boundary
/// (§3.8.2.1.2: "It is a Protocol Error if the Subscription Identifier has a value of 0"),
/// i.e. before the 0xA1 guard ever sees the packet.
///
/// The residual this test used to record — that the codec path closed WITHOUT the
/// SHOULD-level DISCONNECT of `[MQTT-4.13.2]` — is now closed: decode failures after
/// CONNACK are announced with a reason code (`conn.rs::codec_reason`). A Protocol Error
/// answers `0x82`, distinct from `0x81` for input that is not parseable as MQTT at all.
#[tokio::test]
async fn v5_subscribe_with_subscription_identifier_zero_disconnects_with_protocol_error() {
    let addr = start_broker().await;
    let mut c = Client::connect_v5_ok(addr, "subid-zero").await;
    c.send(&Packet::Subscribe(Subscribe {
        properties: Properties(vec![Property::SubscriptionIdentifier(0)]),
        pkid: 1,
        filters: vec![SubscribeFilter {
            options: SubscriptionOptions::default(),
            path: "subid/zero".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await;
    c.expect_disconnect(0x82).await;
    c.expect_closed().await;
}

/// `[MQTT-3.3.4-6]`, verbatim: "A PUBLISH packet sent from a Client to a Server MUST NOT
/// contain a Subscription Identifier." DISCONNECT 0x82 (Protocol Error) and no PUBACK —
/// §4.13.1 reserves the specific 0xA1 for "I do not support the feature", which is not
/// what this client got wrong. Sent at `QoS` 1 so the missing PUBACK is observable.
#[tokio::test]
async fn v5_client_publish_carrying_a_subscription_identifier_is_protocol_error() {
    let addr = start_broker().await;
    let mut c = Client::connect_v5_ok(addr, "pub-subid").await;
    c.publish(
        "subid/pub",
        b"x",
        QoS::AtLeastOnce,
        Some(1),
        vec![Property::SubscriptionIdentifier(1)],
    )
    .await;
    c.expect_disconnect(0x82).await;
}

/// Guard (passes today, must keep passing): neither refusal touches v3.1.1. A v4 client
/// subscribes and completes a `QoS` 1 round-trip on the same broker build. Not red-first
/// evidence.
#[tokio::test]
async fn v311_subscribe_and_publish_are_unaffected_by_the_identifier_refusal() {
    let addr = start_broker().await;
    let mut c = Client::connect(addr, "v4-unaffected").await;
    assert_eq!(
        c.subscribe(1, "v4/t", QoS::AtLeastOnce).await.return_codes,
        vec![1],
        "v3.1.1 SUBACK grants the requested QoS"
    );
    c.publish("v4/t", b"y", QoS::AtLeastOnce, Some(2), vec![])
        .await;
    // The broker's PUBACK for our publish, and the delivery back to ourselves, in
    // whichever order they arrive.
    let mut saw_puback = false;
    let mut saw_publish = false;
    for _ in 0..2 {
        match c.recv().await {
            Packet::PubAck(a) => {
                assert_eq!(a.pkid, 2);
                saw_puback = true;
            }
            Packet::Publish(p) => {
                assert_eq!(p.topic, "v4/t");
                saw_publish = true;
            }
            other => panic!("expected PUBACK or PUBLISH, got {other:?}"),
        }
    }
    assert!(saw_puback && saw_publish, "v3.1.1 QoS 1 round-trip intact");
}

// --- AUTH / re-auth violations (ADR 0013) -----------------------------------

#[tokio::test]
async fn auth_without_prior_enhanced_auth_is_protocol_error() {
    let addr = start_broker().await;
    let mut c = Client::connect_v5_ok(addr, "no-enh").await;
    // An AUTH on a session that never used enhanced auth is a protocol error.
    c.send(&enhanced::auth(0x19, b"alice")).await;
    match c.recv().await {
        Packet::Disconnect(d) => assert_eq!(d.reason, 0x82, "protocol error"),
        other => panic!("expected DISCONNECT, got {other:?}"),
    }
    c.expect_closed().await;
}

#[tokio::test]
async fn reauth_method_change_is_protocol_error() {
    let addr = start_broker_with_policy(enhanced::policy()).await;
    let mut c = connect_enhanced(addr, "reauth-bad").await;
    // Re-authenticate with a different method than the one used at connect.
    c.send(&Packet::Auth(Auth {
        reason: 0x19,
        properties: Properties(vec![Property::AuthenticationMethod("SCRAM-SHA-1".into())]),
    }))
    .await;
    match c.recv().await {
        Packet::Disconnect(d) => assert_eq!(d.reason, 0x82, "method must not change"),
        other => panic!("expected DISCONNECT, got {other:?}"),
    }
}

#[tokio::test]
async fn enhanced_auth_wrong_proof_is_rejected() {
    let addr = start_broker_with_policy(enhanced::policy()).await;
    let mut c = Client::open(addr, ProtocolVersion::V5).await;
    c.send(&connect_with_method(
        "wrong-proof",
        enhanced::SUBJECT.as_bytes(),
    ))
    .await;
    assert_eq!(c.expect_auth().await.reason, 0x18, "challenge");

    // A proof under the wrong key.
    let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, b"not-the-secret");
    let bad = aws_lc_rs::hmac::sign(&key, b"nonce");
    c.send(&enhanced::auth(0x18, bad.as_ref())).await;
    match c.recv().await {
        Packet::ConnAck(a) => assert_eq!(a.code, 0x87, "not authorized"),
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

#[tokio::test]
async fn enhanced_auth_unknown_method_is_rejected() {
    let addr = start_broker_with_policy(enhanced::policy()).await;
    let mut c = Client::open(addr, ProtocolVersion::V5).await;
    c.send(&Packet::Connect(Connect {
        properties: Properties(vec![Property::AuthenticationMethod("SCRAM-SHA-1".into())]),
        protocol: ProtocolVersion::V5,
        clean_session: true,
        keep_alive: 30,
        client_id: "unknown-method".into(),
        last_will: None,
        username: None,
        password: None,
    }))
    .await;
    match c.recv().await {
        Packet::ConnAck(a) => assert_eq!(a.code, 0x8C, "bad authentication method"),
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

// --- helpers ----------------------------------------------------------------

/// A v5 CONNECT that requests the HMAC method with the given initial data.
fn connect_with_method(client_id: &str, initial: &[u8]) -> Packet {
    Packet::Connect(Connect {
        properties: Properties(vec![
            Property::AuthenticationMethod(enhanced::METHOD.into()),
            Property::AuthenticationData(bytes::Bytes::copy_from_slice(initial)),
        ]),
        protocol: ProtocolVersion::V5,
        clean_session: true,
        keep_alive: 30,
        client_id: client_id.to_string(),
        last_will: None,
        username: None,
        password: None,
    })
}

/// Drive a successful HMAC enhanced-auth connect and return the live client.
async fn connect_enhanced(addr: std::net::SocketAddr, client_id: &str) -> Client {
    let mut c = Client::open(addr, ProtocolVersion::V5).await;
    c.send(&connect_with_method(
        client_id,
        enhanced::SUBJECT.as_bytes(),
    ))
    .await;
    let challenge = c.expect_auth().await;
    let nonce = enhanced::nonce_of(&challenge.properties);
    c.send(&enhanced::auth(0x18, &enhanced::proof(&nonce)))
        .await;
    match c.recv().await {
        Packet::ConnAck(a) => assert_eq!(a.code, 0, "connect auth succeeds"),
        other => panic!("expected CONNACK, got {other:?}"),
    }
    c
}
