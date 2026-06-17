//! End-to-end resource-limit (darksky) tests: the broker must bound its per-session
//! state under load rather than grow without limit. See `docs/TEST-PLAN.md`.

mod common;

use common::{start_broker_with_queue_limits, Client};
use mqtt_codec::{Packet, QoS};
use mqtt_storage::{OverflowPolicy, QueueLimits};

/// With a drop-oldest offline queue of size 2, a persistent session that misses
/// three messages while offline replays only the newest two — the oldest is
/// evicted, not retained unboundedly (ADR 0001 §6).
#[tokio::test]
async fn offline_queue_drops_oldest_past_the_cap() {
    let addr = start_broker_with_queue_limits(QueueLimits {
        max_messages: 2,
        overflow: OverflowPolicy::DropOldest,
    })
    .await;

    // A persistent subscriber registers interest, then goes offline.
    let (mut sub, _) = Client::connect_v311(addr, "bounded", false).await;
    sub.subscribe(1, "q", QoS::AtLeastOnce).await;
    sub.disconnect().await;

    // Three QoS1 messages arrive while offline; the queue caps at two.
    let mut pubr = Client::connect_v311(addr, "pub", true).await.0;
    for (i, payload) in [b"m1", b"m2", b"m3"].iter().enumerate() {
        let pkid = u16::try_from(i + 1).unwrap();
        pubr.publish("q", *payload, QoS::AtLeastOnce, Some(pkid), vec![])
            .await;
        assert_eq!(pubr.recv().await, Packet::PubAck(pkid.into()));
    }

    // On reconnect only the newest two replay, in order; the oldest was dropped.
    let (mut sub, present) = Client::connect_v311(addr, "bounded", false).await;
    assert!(present, "the persistent session survived");

    let first = sub.expect_publish().await;
    assert_eq!(&first.payload[..], b"m2", "the oldest (m1) was evicted");
    sub.puback(first.pkid.unwrap()).await;
    let second = sub.expect_publish().await;
    assert_eq!(&second.payload[..], b"m3");
    sub.puback(second.pkid.unwrap()).await;

    sub.expect_silence().await;
}
