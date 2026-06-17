//! End-to-end darksky tests: protocol violations and authentication failures must
//! close the connection (or send the right reason code) without corrupting broker
//! state. These use the self-codec client to send packets a conformant library
//! would never emit. See `docs/TEST-PLAN.md`.

mod common;

use common::{enhanced, start_broker, start_broker_with_policy, Client};
use mqtt_codec::{
    packet::{Auth, Connect, Publish},
    Packet, Properties, Property, ProtocolVersion, QoS,
};

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
    c.expect_closed().await;
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
    c.expect_closed().await;
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
    c.expect_closed().await;
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
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"not-the-secret");
    let bad = ring::hmac::sign(&key, b"nonce");
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
