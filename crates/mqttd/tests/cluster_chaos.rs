//! Cluster routing + chaos tests over a peer mesh: cross-node `QoS` 1/2 delivery,
//! retained replication and back-fill on join (ADR 0014), cluster-wide shared
//! subscriptions (ADR 0015), and a partition/heal reconvergence. See
//! `docs/TEST-PLAN.md`.
//!
//! Cross-node routing is eventually consistent (interest is gossiped on subscribe),
//! so these retry until propagation completes, mirroring `cluster.rs`.

mod common;

use std::time::Duration;

use common::{link, start_node, start_two_node_cluster, Client};
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
        // QoS 0 carries no packet id; QoS > 0 needs a distinct one per attempt.
        let pkid = (qos != QoS::AtMostOnce).then_some(attempt + 1);
        pubr.publish(topic, payload, qos, pkid, vec![]).await;
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
async fn qos2_is_delivered_across_nodes_exactly_once() {
    let (a, b) = start_two_node_cluster().await;
    let mut sub = Client::connect_v5_ok(a, "q2-sub").await;
    sub.subscribe(1, "warmup", QoS::AtMostOnce).await;
    sub.subscribe(2, "t", QoS::ExactlyOnce).await;
    let mut pubr = Client::connect_v5_ok(b, "q2-pub").await;

    // Warm up the B -> A route; this also confirms A's "t" interest reached B (a
    // single interest snapshot carries both filters).
    let _ = route(&mut pubr, &mut sub, "warmup", b"up", QoS::AtMostOnce).await;

    // Publish QoS 2 on B and complete the publisher-side handshake there.
    pubr.publish("t", b"q2", QoS::ExactlyOnce, Some(7), vec![])
        .await;
    match pubr.recv().await {
        Packet::PubRec(r) => assert_eq!(r.pkid, 7),
        other => panic!("expected PUBREC, got {other:?}"),
    }
    pubr.pubrel(7).await;
    match pubr.recv().await {
        Packet::PubComp(r) => assert_eq!(r.pkid, 7),
        other => panic!("expected PUBCOMP, got {other:?}"),
    }

    // The subscriber on A receives it at QoS 2 and completes the downstream handshake.
    let p = sub.expect_publish().await;
    assert_eq!(p.qos, QoS::ExactlyOnce, "QoS 2 preserved across the hop");
    assert_eq!(&p.payload[..], b"q2");
    let pkid = p.pkid.expect("a QoS2 delivery has a packet id");
    sub.pubrec(pkid).await;
    match sub.recv().await {
        Packet::PubRel(r) => assert_eq!(r.pkid, pkid),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    sub.pubcomp(pkid).await;
    sub.expect_silence().await;
}

#[tokio::test]
async fn partition_severs_delivery_and_heal_reconverges() {
    let a = start_node("a").await;
    let b = start_node("b").await;
    let live = link(&a, &b);

    let mut sub = Client::connect_v5_ok(a.client_addr, "p-sub").await;
    sub.subscribe(1, "t", QoS::AtMostOnce).await;
    let mut pubr = Client::connect_v5_ok(b.client_addr, "p-pub").await;

    // Route is up: a publish on B reaches the subscriber on A.
    let _ = route(&mut pubr, &mut sub, "t", b"before", QoS::AtMostOnce).await;

    // Partition: sever the link and let the teardown propagate.
    live.sever();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // During the partition, publishes on B do not reach A.
    for _ in 0..5 {
        pubr.publish("t", b"during", QoS::AtMostOnce, None, vec![])
            .await;
    }
    sub.expect_silence().await;

    // Heal: re-link the nodes; routing reconverges and delivery resumes.
    let _healed = link(&a, &b);
    let p = route(&mut pubr, &mut sub, "t", b"after", QoS::AtMostOnce).await;
    assert_eq!(&p.payload[..], b"after", "delivery resumes after the heal");
}

#[tokio::test]
async fn shared_subscription_delivers_once_cluster_wide() {
    let (a, b) = start_two_node_cluster().await;
    // One member of the same shared group on each node. The publisher is on A.
    let mut sub_a = Client::connect_v5_ok(a, "share-a").await;
    sub_a.subscribe(1, "$share/g/t", QoS::AtMostOnce).await;
    let mut sub_b = Client::connect_v5_ok(b, "share-b").await;
    sub_b.subscribe(1, "$share/g/t", QoS::AtMostOnce).await;
    let mut pubr = Client::connect_v5_ok(a, "share-pub").await;

    // Membership is gossiped to A eventually; retry until A has B's member in its
    // global view, proven by B receiving a publish. Each publish must reach exactly
    // ONE member cluster-wide (ADR 0015) — never both nodes for the same message.
    let mut reached_b = false;
    for attempt in 0..50 {
        pubr.publish("t", b"x", QoS::AtMostOnce, None, vec![]).await;
        let got_a = sub_a.try_recv().await.is_some();
        let got_b = sub_b.try_recv().await.is_some();
        assert!(
            !(got_a && got_b),
            "a single shared publish must not reach members on both nodes"
        );
        if got_b {
            reached_b = true;
            break;
        }
        assert!(
            attempt < 49,
            "B's shared member never entered A's global view"
        );
    }
    assert!(
        reached_b,
        "the global round-robin selected the remote member"
    );

    // And once more, confirm A's local member can also be the sole recipient: keep
    // publishing until A receives, again never both at once.
    for attempt in 0..50 {
        pubr.publish("t", b"y", QoS::AtMostOnce, None, vec![]).await;
        let got_a = sub_a.try_recv().await.is_some();
        let got_b = sub_b.try_recv().await.is_some();
        assert!(!(got_a && got_b), "still at most one recipient per publish");
        if got_a {
            return;
        }
        assert!(attempt < 49, "A's local member was never selected");
    }
}

#[tokio::test]
async fn retained_message_replicates_across_nodes() {
    let (a, b) = start_two_node_cluster().await;

    // Warm up the B -> A peer link: a retained publish replicates to the peers that
    // are members at publish time (ADR 0014), so confirm the link is up first.
    let mut warmup = Client::connect_v5_ok(a, "warmup-sub").await;
    warmup.subscribe(1, "warmup", QoS::AtMostOnce).await;
    let mut pubr = Client::connect_v5_ok(b, "ret-pub").await;
    let _ = route(&mut pubr, &mut warmup, "warmup", b"up", QoS::AtMostOnce).await;

    // Retain a message on node B. QoS 1 + PUBACK: the retain-store command is enqueued
    // on B's hub before we subscribe below, so the subscriber observes it via
    // retained-replay (retain=1) rather than racing the store and getting a live
    // (retain=0) delivery — the source of an intermittent CI failure on this assert.
    pubr.publish_retained_acked("only/here", b"r", 1).await;

    // A subscriber on the same node (B) receives it immediately.
    let mut same_node = Client::connect_v5_ok(b, "same-node").await;
    same_node.subscribe(1, "only/here", QoS::AtMostOnce).await;
    let p = same_node.expect_publish().await;
    assert_eq!(&p.payload[..], b"r");
    assert!(p.retain, "same-node retained delivery sets the retain flag");

    // A subscriber on the *other* node (A) also receives it: retained state is now
    // replicated across nodes (ADR 0014). Cross-node propagation is eventually
    // consistent, so re-subscribe until the replicated retained message arrives.
    let mut cross = Client::connect_v5_ok(a, "cross-node").await;
    for attempt in 0..50 {
        cross.subscribe(2, "only/here", QoS::AtMostOnce).await;
        if let Some(Packet::Publish(p)) = cross.try_recv().await {
            assert_eq!(&p.payload[..], b"r");
            assert!(
                p.retain,
                "cross-node retained delivery sets the retain flag"
            );
            return;
        }
        assert!(attempt < 49, "retained never replicated to the other node");
    }
}

#[tokio::test]
async fn retained_back_fills_a_node_that_joins_after_the_publish() {
    // Node A is up alone; retain a message before any peer exists.
    let a = start_node("a").await;
    let mut pubr = Client::connect_v5_ok(a.client_addr, "ret-pub").await;
    // Acked so A has the retained message stored before node B links and pulls the
    // retained snapshot (ADR 0014 §3) — otherwise the snapshot could race the store.
    pubr.publish_retained_acked("history/t", b"r", 1).await;

    // Node B joins the cluster *after* the publish and links to A. On link-up A
    // sends B its retained snapshot, so B back-fills (ADR 0014 §3) — a subscriber
    // on B then sees the message it was never live for.
    let b = start_node("b").await;
    link(&a, &b);

    let mut late = Client::connect_v5_ok(b.client_addr, "late-sub").await;
    for attempt in 0..50 {
        late.subscribe(1, "history/t", QoS::AtMostOnce).await;
        if let Some(Packet::Publish(p)) = late.try_recv().await {
            assert_eq!(&p.payload[..], b"r");
            assert!(
                p.retain,
                "back-filled retained delivery sets the retain flag"
            );
            return;
        }
        assert!(attempt < 49, "the late-joining node was never back-filled");
    }
}
