//! T1 — the bridge's MQTT client connects to a broker and round-trips a message.
//!
//! Spins up an in-process `mqttd` broker (the same `Hub` + connection handler the broker
//! binary uses) and drives [`mqtt_bridge::client::MqttClient`] through a real
//! CONNECT/SUBSCRIBE/PUBLISH/deliver cycle — the proof that the client side is wired to the
//! codec/framing correctly before any forwarding logic is built on top.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use mqtt_bridge::client::{ConnectOptions, Event, MqttClient, Transport};
use mqtt_codec::properties::{Properties, Property};
use mqtt_codec::{ProtocolVersion, QoS};
use mqttd::Hub;
use tokio::net::TcpListener;

/// Start a permissive in-process broker on an ephemeral port; return its address.
async fn start_broker() -> SocketAddr {
    let (hub, hub_tx) = Hub::new();
    tokio::spawn(hub.run());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, hub_tx.clone()));
        }
    });
    addr
}

fn opts(addr: SocketAddr, client_id: &str) -> ConnectOptions {
    ConnectOptions {
        addr: addr.to_string(),
        transport: Transport::Plain,
        version: ProtocolVersion::V5,
        client_id: client_id.to_string(),
        username: None,
        password: None,
        keep_alive: 30,
        clean_start: true,
    }
}

#[tokio::test]
async fn the_client_connects_subscribes_and_round_trips_a_publish() {
    let addr = start_broker().await;

    // A subscriber and a publisher — proving the client both receives and sends.
    let mut sub = MqttClient::connect(&opts(addr, "bridge-sub"))
        .await
        .unwrap();
    let mut pubr = MqttClient::connect(&opts(addr, "bridge-pub"))
        .await
        .unwrap();

    sub.subscribe(1, "bridge/t/#", QoS::AtLeastOnce)
        .await
        .unwrap();
    match sub.next_event().await.unwrap() {
        Event::SubAck { pkid, return_codes } => {
            assert_eq!(pkid, 1);
            assert_eq!(return_codes, vec![1]); // QoS 1 granted
        }
        other => panic!("expected SubAck, got {other:?}"),
    }

    // Publish QoS 1 with a user property; the publisher must see its PUBACK.
    let props = Properties(vec![Property::UserProperty(
        "fss-bridge-hop-count".into(),
        "1".into(),
    )]);
    pubr.publish(
        "bridge/t/a",
        Bytes::from_static(b"hello"),
        QoS::AtLeastOnce,
        Some(7),
        props,
    )
    .await
    .unwrap();
    match pubr.next_event().await.unwrap() {
        Event::PubAck(pkid) => assert_eq!(pkid, 7),
        other => panic!("expected PubAck, got {other:?}"),
    }

    // The subscriber receives the delivered message, with the user property intact.
    let delivered = tokio::time::timeout(Duration::from_secs(5), sub.next_event())
        .await
        .expect("a message should arrive")
        .unwrap();
    match delivered {
        Event::Publish(p) => {
            assert_eq!(p.topic, "bridge/t/a");
            assert_eq!(&p.payload[..], b"hello");
            assert_eq!(p.qos, QoS::AtLeastOnce);
            // The broker forwards the publisher's User Properties unaltered (MQTT-3.3.2-17,
            // ADR 0030) — so the bridge's hop-count property survives a broker hop, which is
            // what makes the loop-prevention (0025-T5) work end to end.
            let hop = p.properties.0.iter().find_map(|prop| match prop {
                Property::UserProperty(k, v) if k == "fss-bridge-hop-count" => Some(v.clone()),
                _ => None,
            });
            assert_eq!(
                hop.as_deref(),
                Some("1"),
                "the hop-count property must survive"
            );
            // Acknowledge it (QoS 1 delivery).
            sub.puback(p.pkid.unwrap()).await.unwrap();
        }
        other => panic!("expected Publish, got {other:?}"),
    }

    sub.disconnect().await;
    pubr.disconnect().await;
}

#[tokio::test]
async fn connect_refused_surfaces_as_an_error() {
    // Nothing listening on this port → a connect error, not a panic.
    match MqttClient::connect(&opts("127.0.0.1:1".parse().unwrap(), "x")).await {
        Err(mqtt_bridge::client::ClientError::Connect { .. }) => {}
        Err(other) => panic!("expected a Connect error, got {other:?}"),
        Ok(_) => panic!("connect to a dead port should fail"),
    }
}
