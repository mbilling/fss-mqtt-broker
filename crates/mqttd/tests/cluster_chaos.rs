//! Cluster routing tests over a two-node peer mesh: cross-node `QoS` > 0 delivery,
//! and the two documented carried limitations — shared subscriptions deliver
//! per-node (ADR 0010 §5), and retained state is not replicated across nodes
//! (peer.rs / ADR 0001 phase 3). See `docs/TEST-PLAN.md`.
//!
//! Cross-node routing is eventually consistent (interest is gossiped on subscribe),
//! so these retry the publish until interest has propagated, mirroring `cluster.rs`.

mod common;

use common::{start_two_node_cluster, Client};
use mqtt_codec::{Packet, QoS};

/// Retry a QoS-tagged publish from `pubr` until `sub` receives it, returning the
/// delivered PUBLISH. Tolerates interest-propagation lag across the peer link.
async fn route(
    pubr: &mut Client,
    sub: &mut Client,
    topic: &str,
    payload: &[u8],
    qos: QoS,
) -> mqtt_codec::packet::Publish {
    for attempt in 0..50u16 {
        pubr.publish(topic, payload, qos, Some(attempt + 1), vec![])
            .await;
        if let Some(Packet::Publish(p)) = sub.try_recv().await {
            return p;
        }
        assert!(attempt < 49, "message never routed across the cluster");
    }
    unreachable!()
}

#[tokio::test]
async fn qos1_is_delivered_and_acked_across_nodes() {
    let (a, b) = start_two_node_cluster().await;
    let mut sub = Client::connect_v5_ok(a, "sub").await;
    sub.subscribe(1, "cluster/+/data", QoS::AtLeastOnce).await;
    let mut pubr = Client::connect_v5_ok(b, "pub").await;

    let p = route(
        &mut pubr,
        &mut sub,
        "cluster/z/data",
        b"q1",
        QoS::AtLeastOnce,
    )
    .await;
    assert_eq!(p.qos, QoS::AtLeastOnce, "QoS is preserved across the hop");
    assert_eq!(&p.payload[..], b"q1");
    // The subscriber acks to its own node; the delivery completes cleanly.
    sub.puback(p.pkid.expect("a cross-node QoS1 delivery has a packet id"))
        .await;
    sub.expect_silence().await;
}

#[tokio::test]
async fn shared_subscription_delivers_once_per_node() {
    let (a, b) = start_two_node_cluster().await;
    // One member of the same shared group on each node.
    let mut sub_a = Client::connect_v5_ok(a, "share-a").await;
    sub_a.subscribe(1, "$share/g/t", QoS::AtMostOnce).await;
    let mut sub_b = Client::connect_v5_ok(b, "share-b").await;
    sub_b.subscribe(1, "$share/g/t", QoS::AtMostOnce).await;

    let mut pubr = Client::connect_v5_ok(a, "share-pub").await;

    // Publish repeatedly until both members have received at least once. The point
    // is that the message reaches a member on *each* node (one-per-node), not
    // exactly one cluster-wide (ADR 0010 §5).
    let (mut got_a, mut got_b) = (false, false);
    for attempt in 0..50 {
        pubr.publish("t", b"x", QoS::AtMostOnce, None, vec![]).await;
        if let Some(Packet::Publish(p)) = sub_a.try_recv().await {
            assert_eq!(&p.payload[..], b"x");
            got_a = true;
        }
        if let Some(Packet::Publish(p)) = sub_b.try_recv().await {
            assert_eq!(&p.payload[..], b"x");
            got_b = true;
        }
        if got_a && got_b {
            break;
        }
        assert!(
            attempt < 49,
            "shared members did not both receive (got_a={got_a}, got_b={got_b})"
        );
    }
    assert!(
        got_a && got_b,
        "a shared publish reaches one member on every node"
    );
}

#[tokio::test]
async fn retained_message_is_not_replicated_across_nodes() {
    let (a, b) = start_two_node_cluster().await;

    // Retain a message on node B.
    let mut pubr = Client::connect_v5_ok(b, "ret-pub").await;
    pubr.publish_retained("only/here", b"r").await;

    // A subscriber on the same node (B) receives it: same-node retained works.
    let mut same_node = Client::connect_v5_ok(b, "same-node").await;
    same_node.subscribe(1, "only/here", QoS::AtMostOnce).await;
    let p = same_node.expect_publish().await;
    assert_eq!(&p.payload[..], b"r");
    assert!(p.retain, "same-node retained delivery sets the retain flag");

    // A subscriber on the other node (A) receives nothing: retained state is not
    // replicated across nodes (carried limitation, ADR 0001 phase 3).
    let mut other_node = Client::connect_v5_ok(a, "other-node").await;
    other_node.subscribe(1, "only/here", QoS::AtMostOnce).await;
    other_node.expect_silence().await;
}
