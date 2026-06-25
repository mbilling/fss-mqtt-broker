//! Engine integration (0025-T3/T4/T5, and the central 0025-T10 security property): run the
//! bridge between two **real** in-process brokers (a "local" and an "upstream" `mqttd`) and
//! verify that a one-way `out` rule forwards local→upstream with a remap and a stamped hop
//! count — and **never** leaks the reverse direction.
#![allow(clippy::similar_names)] // pub/sub-style test client names are intentionally paired

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use mqtt_bridge::client::{ConnectOptions, Event, MqttClient, Transport};
use mqtt_bridge::config::BridgeConfig;
use mqtt_bridge::engine::Bridge;
use mqtt_codec::properties::{Properties, Property};
use mqtt_codec::{ProtocolVersion, QoS};
use mqttd::Hub;
use tokio::net::TcpListener;

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

async fn client(addr: SocketAddr, id: &str) -> MqttClient {
    MqttClient::connect(&ConnectOptions {
        addr: addr.to_string(),
        transport: Transport::Plain,
        version: ProtocolVersion::V5,
        client_id: id.to_string(),
        username: None,
        password: None,
        keep_alive: 30,
        clean_start: true,
    })
    .await
    .unwrap()
}

/// Wait for a subscriber's SUBACK.
async fn subscribe(c: &mut MqttClient, filter: &str) {
    c.subscribe(1, filter, QoS::AtMostOnce).await.unwrap();
    match c.next_event().await.unwrap() {
        Event::SubAck { .. } => {}
        other => panic!("expected SubAck, got {other:?}"),
    }
}

fn hop_count(p: &mqtt_codec::packet::Publish) -> Option<String> {
    p.properties.0.iter().find_map(|prop| match prop {
        Property::UserProperty(k, v) if k == "fss-bridge-hop-count" => Some(v.clone()),
        _ => None,
    })
}

#[tokio::test]
async fn a_one_way_out_rule_forwards_to_the_upstream_and_never_leaks_back() {
    let local = start_broker().await;
    let upstream = start_broker().await;

    let cfg = BridgeConfig::parse_toml(&format!(
        r#"
        [local]
        url = "{local}"

        [[upstreams]]
        name = "partner"
        url = "{upstream}"

        [[upstreams.rules]]
        direction = "out"
        filter = "telemetry/#"
        remap = {{ strip_prefix = "telemetry/", prefix = "org/telemetry/" }}
        "#,
    ))
    .unwrap();
    let bridge = Bridge::start(cfg);

    // A subscriber on the UPSTREAM for the remapped topic.
    let mut up_sub = client(upstream, "up-sub").await;
    subscribe(&mut up_sub, "org/telemetry/#").await;

    // A subscriber on the LOCAL side that must NEVER receive an upstream-origin message for
    // this one-way `out` rule (the reverse path is closed).
    let mut local_sub = client(local, "local-sub").await;
    subscribe(&mut local_sub, "telemetry/#").await;

    // Publish on LOCAL; the bridge subscribes to telemetry/# locally and forwards to the
    // upstream as org/telemetry/.... Retry until the bridge's local subscription is live.
    let mut local_pub = client(local, "local-pub").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let forwarded = loop {
        local_pub
            .publish(
                "telemetry/room/temp",
                Bytes::from_static(b"21C"),
                QoS::AtMostOnce,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_millis(300), up_sub.next_event()).await {
            Ok(Ok(Event::Publish(p))) => break p,
            _ => assert!(
                tokio::time::Instant::now() < deadline,
                "the upstream never received the forwarded message"
            ),
        }
    };
    assert_eq!(forwarded.topic, "org/telemetry/room/temp", "remap applied");
    assert_eq!(&forwarded.payload[..], b"21C");
    assert_eq!(
        hop_count(&forwarded).as_deref(),
        Some("1"),
        "the first bridge hop stamps hop-count=1"
    );

    // Reverse direction: publish an upstream-origin message that MATCHES the out rule's
    // filter; the local subscriber must NEVER receive *that* message back (a one-way `out`
    // rule never opens the reverse path — the bridge never subscribed on the upstream for
    // it). We tag the probe with a unique payload so the legitimate local-origin "21C"
    // deliveries that `local_sub` also sees (normal same-broker delivery, not a bridge hop)
    // are not mistaken for a leak.
    let mut up_pub = client(upstream, "up-pub").await;
    for _ in 0..6 {
        up_pub
            .publish(
                "telemetry/leak/probe",
                Bytes::from_static(b"LEAK-PROBE"),
                QoS::AtMostOnce,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        // Drain whatever the local subscriber sees; only a probe-tagged message is a leak.
        while let Ok(Ok(Event::Publish(p))) =
            tokio::time::timeout(Duration::from_millis(150), local_sub.next_event()).await
        {
            assert_ne!(
                &p.payload[..],
                b"LEAK-PROBE",
                "one-way out rule leaked an upstream message back to local"
            );
        }
    }

    // Observability (T9): the out forward was counted; nothing was forwarded inbound.
    let m = bridge.metrics();
    assert!(m.forwarded_out_count() >= 1, "the out forward was counted");
    assert_eq!(m.forwarded_in_count(), 0, "nothing was forwarded inbound");

    bridge.shutdown();
}

/// ADR 0025-T6: two bridge instances sharing a cluster-side group must **not** duplicate
/// forwarding — the shared subscription load-balances, so each local message is forwarded
/// to the upstream at most once.
#[tokio::test]
async fn two_bridge_instances_do_not_duplicate_forwarding() {
    let local = start_broker().await;
    let upstream = start_broker().await;

    let cfg = |client_id: &str| {
        BridgeConfig::parse_toml(&format!(
            r#"
            share_group = "ha"
            [local]
            url = "{local}"
            client_id = "{client_id}"
            [[upstreams]]
            name = "partner"
            url = "{upstream}"
            [[upstreams.rules]]
            direction = "out"
            filter = "telemetry/#"
            "#,
        ))
        .unwrap()
    };
    let b1 = Bridge::start(cfg("bridge-a"));
    let b2 = Bridge::start(cfg("bridge-b"));

    // A subscriber on the upstream collects forwarded messages.
    let mut up_sub = client(upstream, "ha-up-sub").await;
    subscribe(&mut up_sub, "telemetry/#").await;

    // Let both instances connect and register their shared subscription.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Publish a batch of uniquely-payloaded messages on the local side.
    let mut local_pub = client(local, "ha-local-pub").await;
    for n in 0..20u32 {
        local_pub
            .publish(
                "telemetry/x",
                Bytes::from(format!("m{n}")),
                QoS::AtMostOnce,
                None,
                Properties::new(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Collect for a window; every payload must appear at most once (no duplicate forward).
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    while let Ok(Ok(Event::Publish(p))) =
        tokio::time::timeout(Duration::from_millis(400), up_sub.next_event()).await
    {
        assert!(
            seen.insert(p.payload.to_vec()),
            "duplicate forward of {:?}: two instances both forwarded it",
            String::from_utf8_lossy(&p.payload)
        );
    }
    assert!(
        !seen.is_empty(),
        "the HA pair forwarded nothing — the shared subscription never delivered"
    );

    b1.shutdown();
    b2.shutdown();
}
