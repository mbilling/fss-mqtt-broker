//! Per-client quota tests (ADR 0041 T3): the subscription quota answers in
//! SUBACK slots, the publish-rate throttle pauses reads instead of dropping or
//! disconnecting, and the Receive Maximum counts `QoS` 1 and `QoS` 2 together.
//!
//! This file sets the process-wide [`mqttd::conn::WireLimits`] (rate 5/s,
//! Receive Maximum 1), so these tests live in their own integration binary.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::Client;
use mqtt_codec::{
    packet::{Subscribe, SubscribeFilter},
    Packet, QoS,
};
use mqtt_storage::MemorySessionStore;
use mqttd::hub::{HubCommand, Quotas};
use mqttd::Hub;
use tokio::net::TcpListener;

/// The one process-wide limit set: publish rate 5/s, Receive Maximum 1.
fn set_limits() {
    mqttd::conn::set_wire_limits(mqttd::conn::WireLimits {
        receive_maximum: 1,
        publish_rate: Some(5),
        ..Default::default()
    });
}

/// A permissive broker whose hub has the given subscription quota.
async fn start_broker_with_quotas(quotas: Quotas) -> SocketAddr {
    set_limits();
    let (hub, hub_tx) = Hub::with_config(
        mqtt_cluster::NodeId("quota-node".into()),
        Arc::new(MemorySessionStore::new()),
    );
    tokio::spawn(hub.run());
    hub_tx.send(HubCommand::SetQuotas(quotas)).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tx = hub_tx.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
        }
    });
    addr
}

/// One SUBSCRIBE packet with several filters; returns the SUBACK return codes.
async fn subscribe_many(c: &mut Client, pkid: u16, filters: &[&str]) -> Vec<u8> {
    c.send(&Packet::Subscribe(Subscribe {
        properties: mqtt_codec::Properties::new(),
        pkid,
        filters: filters
            .iter()
            .map(|f| SubscribeFilter {
                options: mqtt_codec::SubscriptionOptions::default(),
                path: (*f).to_string(),
                qos: QoS::AtMostOnce,
            })
            .collect(),
    }))
    .await;
    match c.recv().await {
        Packet::SubAck(a) => a.return_codes,
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

/// ADR 0041 T3 — the subscription quota answers per SUBACK slot: with a cap of
/// two, the third filter in one packet is denied `0x97` (v5) while the in-cap
/// filters are granted; re-subscribing a held filter never consumes quota; and a
/// v3.1.1 client gets `0x80` in the denied slot.
#[tokio::test]
async fn the_subscription_quota_denies_excess_filters_per_suback_slot() {
    let addr = start_broker_with_quotas(Quotas {
        max_subscriptions_per_client: Some(2),
    })
    .await;

    // v5: two granted, the third denied 0x97 — in one packet.
    let (mut v5, _ack) = Client::connect_v5(addr, "q-v5", true, vec![]).await;
    assert_eq!(
        subscribe_many(&mut v5, 1, &["a", "b", "c"]).await,
        vec![0, 0, 0x97],
        "the over-quota filter must be denied 0x97 in its slot"
    );
    // Re-subscribing a held filter replaces — granted even at the cap...
    assert_eq!(subscribe_many(&mut v5, 2, &["a"]).await, vec![0]);
    // ...while a new filter is still over quota.
    assert_eq!(subscribe_many(&mut v5, 3, &["d"]).await, vec![0x97]);
    // The session itself is untouched: granted subscriptions deliver.
    let mut pubr = Client::connect(addr, "q-pub").await;
    pubr.publish("a", b"still-works", QoS::AtMostOnce, None, vec![])
        .await;
    let p = v5.expect_publish().await;
    assert_eq!(&p.payload[..], b"still-works");

    // v3.1.1 has no 0x97: the denied slot carries 0x80.
    let (mut v3, _) = Client::connect_v311(addr, "q-v3", true).await;
    assert_eq!(
        subscribe_many(&mut v3, 1, &["x", "y", "z"]).await,
        vec![0, 0, 0x80]
    );
}

/// ADR 0041 T3 — the publish-rate throttle: an over-rate `QoS` 1 publisher is
/// slowed to the configured rate by read-pause. Nothing is dropped, nothing is
/// disconnected — every message is acked and delivered — while a second client
/// publishes through the same broker unimpeded mid-throttle.
#[tokio::test]
async fn an_over_rate_publisher_is_throttled_without_loss_or_disconnect() {
    let addr = start_broker_with_quotas(Quotas::default()).await;

    let (mut sub, _) = Client::connect_v311(addr, "rate-sub", true).await;
    sub.subscribe(1, "r/#", QoS::AtMostOnce).await;

    // A publisher pumps 12 `QoS` 1 messages as fast as acks allow (rate cap: 5/s).
    let pump = tokio::spawn(async move {
        let mut a = Client::connect(addr, "rate-a").await;
        let started = Instant::now();
        for i in 1..=12u16 {
            a.publish("r/a", b"tick", QoS::AtLeastOnce, Some(i), vec![])
                .await;
            assert_eq!(a.recv().await, Packet::PubAck(i.into()), "no drops");
        }
        (a, started.elapsed())
    });

    // Mid-throttle, a second client is unimpeded.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mut b = Client::connect(addr, "rate-b").await;
    b.publish("r/b", b"free", QoS::AtMostOnce, None, vec![])
        .await;

    // The subscriber sees ALL 13 messages: 12 throttled ticks plus the free one
    // (which must arrive interleaved, not after the whole throttled stream).
    let mut ticks = 0;
    let mut free_at = None;
    for n in 0..13 {
        let p = sub.expect_publish().await;
        match p.topic.as_str() {
            "r/a" => ticks += 1,
            "r/b" => free_at = Some(n),
            other => panic!("unexpected topic {other}"),
        }
    }
    assert_eq!(ticks, 12, "the throttle must not drop messages");
    let free_at = free_at.expect("the unthrottled client's message arrived");
    assert!(
        free_at < 12,
        "the second client must not wait for the throttled stream to finish"
    );

    let (mut a, elapsed) = pump.await.unwrap();
    // 12 messages at 5/s with a 5-token burst: ~7 waits of 200ms. Generous lower
    // bound to stay timing-robust.
    assert!(
        elapsed >= Duration::from_millis(900),
        "the publisher must have been slowed (elapsed {elapsed:?})"
    );
    // Throttled, not punished: the connection still answers.
    a.send(&Packet::PingReq).await;
    assert_eq!(a.recv().await, Packet::PingResp);
}

/// ADR 0041 T3 (closing the ADR 0012 §3 deferral) — Receive Maximum counts `QoS` 1
/// and `QoS` 2 publications TOGETHER: with the advertised maximum of 1 already
/// consumed by an open `QoS` 2 window, a `QoS` 1 publish is a flow-control breach —
/// `DISCONNECT 0x93`.
#[tokio::test]
async fn a_qos1_publish_beyond_the_shared_receive_maximum_gets_0x93() {
    let addr = start_broker_with_quotas(Quotas::default()).await;

    let (mut c, _ack) = Client::connect_v5(addr, "rm-v5", true, vec![]).await;
    // Open a `QoS` 2 window (PUBLISH → PUBREC) and leave it unreleased.
    c.publish("rm/t", b"two", QoS::ExactlyOnce, Some(1), vec![])
        .await;
    assert_eq!(c.recv().await, Packet::PubRec(1.into()));

    // The window (1) is full: a `QoS` 1 publish is one more concurrent
    // publication — the server disconnects with 0x93.
    c.publish("rm/t", b"one", QoS::AtLeastOnce, Some(2), vec![])
        .await;
    c.expect_disconnect(0x93).await;
}
