//! Boundaries of `QoS` 0 pre-enqueue reselection (#482).
use super::*;

#[tokio::test]
async fn qos0_shared_capacity_includes_application_properties() {
    let rig = Rig::new(true, Some(MESSAGE_BYTES * 2));
    let (mut slow, sm) = rig.member("slow").await;
    rig.publish(0);
    ping(&rig.tx).await;
    let (mut fast, fm) = rig.member("fast").await;
    let app = AppProperties {
        user_properties: vec![("key".into(), "x".repeat(100))],
        ..AppProperties::default()
    };
    for seq in 1_u64..=6 {
        rig.tx
            .send(HubCommand::Publish {
                topic: "t".into(),
                payload: Bytes::copy_from_slice(&seq.to_be_bytes()),
                qos: QoS::AtMostOnce,
                retain: false,
                message_expiry: None,
                app: app.clone(),
                done: None,
                publisher: None,
                v5: false,
            })
            .unwrap();
        ping(&rig.tx).await;
        assert_eq!(drain(&mut fast, &fm), vec![seq]);
    }
    assert_eq!(drain(&mut slow, &sm), vec![0]);
    assert_eq!(dropped_for(&rig.metrics, "outbound-full"), 0);
}

#[tokio::test]
async fn qos0_shared_rechecks_capacity_between_matching_groups() {
    let rig = Rig::new(true, Some(MESSAGE_BYTES));
    let (mut a, am) = rig.member("a").await;
    let (mut b, bm) = rig.member("b").await;
    // Both plans initially choose a. Their COMMIT must not spend its one slot twice.
    subscribe(&rig.tx, "a", "$share/other/t");
    subscribe(&rig.tx, "b", "$share/other/t");
    ping(&rig.tx).await;
    rig.publish(7);
    ping(&rig.tx).await;
    assert_eq!(drain(&mut a, &am), vec![7]);
    assert_eq!(drain(&mut b, &bm), vec![7]); // one delivery owed to EACH group
    assert_eq!(dropped_for(&rig.metrics, "outbound-full"), 0);
}

#[tokio::test]
async fn qos0_shared_does_not_escape_into_another_group_or_a_down_link() {
    let rig = Rig::new(true, Some(MESSAGE_BYTES));
    let (mut slow, sm) = rig.member("slow").await;
    rig.publish(0);
    ping(&rig.tx).await;
    let (mut unrelated, um) = rig.member("unrelated").await;
    rig.tx
        .send(HubCommand::Unsubscribe {
            client: ClientId("unrelated".into()),
            filters: vec!["$share/g/t".into()],
            reply: None,
        })
        .unwrap();
    subscribe(&rig.tx, "unrelated", "$share/other/t");
    let mut peer = connect_peer(&rig.tx, "peer", 1);
    remote_shared_interest(&rig.tx, "peer", "other", "t", &["remote-other"]);
    // A live-looking member with NO connected link is not an admission destination.
    remote_shared_interest(&rig.tx, "down", "g", "t", &["unreachable"]);
    ping(&rig.tx).await;
    rig.publish(1);
    ping(&rig.tx).await;
    assert_eq!(drain(&mut slow, &sm), vec![0]);
    assert_eq!(drain(&mut unrelated, &um), vec![1]);
    assert_eq!(dropped_for(&rig.metrics, "outbound-full"), 1);
    while let Ok(frame) = peer.try_recv() {
        assert!(
            !matches!(frame, PeerMessage::SharedDeliver { .. }),
            "no extra copy in the other group"
        );
    }
}

#[tokio::test]
async fn qos1_shared_rotation_and_qos0_ordinary_shedding_are_unchanged() {
    let rig = Rig::new(true, Some(MESSAGE_BYTES));
    let (mut a, am) = rig.member("a").await;
    rig.publish(0);
    ping(&rig.tx).await;
    let (mut b, bm) = rig.member("b").await;
    subscribe_qos(&rig.tx, "a", "$share/g/t", QoS::AtLeastOnce);
    subscribe_qos(&rig.tx, "b", "$share/g/t", QoS::AtLeastOnce);
    ping(&rig.tx).await;
    publish_qos1(&rig.tx, "t", b"qos1-a");
    publish_qos1(&rig.tx, "t", b"qos1-b");
    ping(&rig.tx).await;
    let fill = a.try_recv().unwrap();
    am.drained(&fill);
    let qa = timeout(Duration::from_secs(5), a.recv())
        .await
        .unwrap()
        .unwrap();
    am.drained(&qa);
    let qb = timeout(Duration::from_secs(5), b.recv())
        .await
        .unwrap()
        .unwrap();
    bm.drained(&qb);
    assert!(
        matches!(*qa, Packet::Publish(ref p) if p.qos == QoS::AtLeastOnce && p.payload == b"qos1-a"[..])
    );
    assert!(
        matches!(*qb, Packet::Publish(ref p) if p.qos == QoS::AtLeastOnce && p.payload == b"qos1-b"[..])
    );
    assert!(a.try_recv().is_err() && b.try_recv().is_err());
    assert_eq!(dropped_for(&rig.metrics, "outbound-full"), 0);

    let ordinary = Rig::new(true, Some(ENTRY_OVERHEAD + "ordinary".len() + 3));
    let (mut a, am) = ordinary.member("a").await;
    subscribe(&ordinary.tx, "a", "ordinary");
    ping(&ordinary.tx).await;
    publish(&ordinary.tx, "ordinary", b"one");
    publish(&ordinary.tx, "ordinary", b"two");
    ping(&ordinary.tx).await;
    let packet = a.try_recv().unwrap();
    am.drained(&packet);
    assert_eq!(payload_of(&packet), b"one");
    assert!(a.try_recv().is_err());
    assert_eq!(dropped_for(&ordinary.metrics, "outbound-full"), 1);
}
