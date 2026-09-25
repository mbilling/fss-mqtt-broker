//! Exact identity, stable cursor order and membership invalidation for #613's
//! group-first derived index. No timing thresholds or production policy changes.
use super::*;
use crate::hub::{delivery::shared_cursor_hash, SharedKey};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;

fn group(name: &str, filter: &str, clients: &[&str], qos: QoS) -> RemoteSharedGroup {
    RemoteSharedGroup {
        group: name.into(),
        filter: filter.into(),
        members: clients
            .iter()
            .map(|c| (ClientId((*c).into()), qos, true))
            .collect(),
    }
}

async fn interest(hub: &mut Hub, node: &str, groups: Vec<RemoteSharedGroup>) {
    hub.dispatch(HubCommand::RemoteSharedInterest {
        node: NodeId(node.into()),
        groups,
    })
    .await;
}

#[tokio::test]
async fn remote_group_index_skips_only_the_exact_local_group_and_filter() {
    for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
        let (mut hub, _) = Hub::new();
        hub.set_shared_prefer_local(true);
        // Pure planner fixture: no socket delivery is claimed by this test.
        hub.shared
            .subscribe(ClientId("local".into()), "pool", "site/#", qos, true);
        interest(
            &mut hub,
            "z",
            vec![
                group("pool", "site/#", &["shadow-z"], qos),
                group("other", "site/#", &["remote-other"], qos),
            ],
        )
        .await;
        interest(
            &mut hub,
            "a",
            vec![
                group("pool", "site/#", &["shadow-a"], qos),
                group("pool", "site/+", &["remote-narrow"], qos),
            ],
        )
        .await;
        let plans = hub.plan_shared("site/x");
        assert_eq!(plans.len(), 3, "one obligation per exact group/filter");
        let choices: BTreeMap<_, _> = plans
            .into_iter()
            .map(|p| {
                let chosen = p.chosen.unwrap();
                assert_eq!(chosen.qos, qos);
                (p.key_if_new.unwrap(), chosen.client.0.to_string())
            })
            .collect();
        assert_eq!(
            choices,
            BTreeMap::from([
                (("pool".into(), "site/#".into()), "local".into()),
                (("pool".into(), "site/+".into()), "remote-narrow".into()),
                (("other".into(), "site/#".into()), "remote-other".into()),
            ])
        );
        // Two peers in this group, but exactly one group entry to check/skip.
        let indexed = &hub.remote_by_filter["site/#"];
        assert_eq!(indexed.len(), 2);
        assert_eq!(
            indexed["pool"],
            vec![(NodeId("a".into()), 0), (NodeId("z".into()), 0)]
        );
    }
}

#[tokio::test]
async fn remote_group_index_keeps_hot_and_cold_candidate_order_and_qos() {
    for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
        let (mut hub, _) = Hub::new();
        hub.set_shared_prefer_local(false);
        // Reverse arrival order, interspersed groups, and two source entries for
        // the same group on one node. Preserve every original cursor position.
        interest(
            &mut hub,
            "z",
            vec![
                group("other", "t", &["other-z"], qos),
                group("pool", "t", &["z0"], qos),
            ],
        )
        .await;
        interest(
            &mut hub,
            "a",
            vec![
                group("pool", "t", &["a0", "a1"], qos),
                group("other", "t", &["other-a"], qos),
                group("pool", "t", &["a2"], qos),
            ],
        )
        .await;
        let key: SharedKey = ("pool".into(), "t".into());
        let cold = hub
            .shared_candidates("t")
            .into_iter()
            .find(|(k, _)| k == &key)
            .unwrap()
            .1;
        assert_eq!(
            cold.iter().map(|c| c.client.0.as_ref()).collect::<Vec<_>>(),
            vec!["a0", "a1", "a2", "z0"]
        );
        assert_eq!(
            hub.remote_by_filter["t"]["pool"],
            vec![
                (NodeId("a".into()), 0),
                (NodeId("a".into()), 2),
                (NodeId("z".into()), 1)
            ]
        );
        for cursor in 0..12 {
            hub.shared_cursor
                .insert(shared_cursor_hash(&key.0, &key.1), (key.clone(), cursor));
            let plan = hub
                .plan_shared("t")
                .into_iter()
                .find(|p| p.key_hash == shared_cursor_hash(&key.0, &key.1))
                .unwrap();
            let hot = plan.chosen.unwrap();
            let expected = &cold[cursor % cold.len()];
            assert_eq!(hot.client, expected.client);
            assert_eq!(hot.node, expected.node);
            assert_eq!(hot.qos, qos);
            assert_eq!(plan.next_cursor, (cursor + 1) % cold.len());
        }
        // Liveness updates replace the index too, without collapsing cursor
        // positions. An offline first member must not hide the next live one.
        let mut changed = group("pool", "t", &["a0", "a1"], qos);
        changed.members[0].2 = false;
        interest(&mut hub, "a", vec![changed]).await;
        hub.shared_cursor.clear();
        let selected = hub
            .plan_shared("t")
            .into_iter()
            .find(|p| p.key_if_new.as_ref() == Some(&key))
            .unwrap();
        assert_eq!(selected.chosen.unwrap().client.0.as_ref(), "a1");
    }
}

#[tokio::test]
async fn remote_group_index_rebuilds_on_replacement_disconnect_and_death() {
    let (mut hub, _) = Hub::new();
    let (tx, _rx) = mpsc::unbounded_channel();
    hub.dispatch(HubCommand::PeerConnected {
        node: NodeId("z".into()),
        conn_id: 7,
        ctl: tx.clone(),
        tx,
        cert_serial: None,
        proto: mqtt_cluster::peer::PROTO_MAX,
        depth: Arc::new(AtomicUsize::new(0)),
    })
    .await;
    interest(
        &mut hub,
        "z",
        vec![group("old", "old/#", &["old"], QoS::AtMostOnce)],
    )
    .await;
    interest(
        &mut hub,
        "a",
        vec![group("keep", "keep/+", &["keep"], QoS::AtMostOnce)],
    )
    .await;
    interest(
        &mut hub,
        "z",
        vec![
            group("new", "new/+", &["new0"], QoS::AtMostOnce),
            group("new", "new/#", &["new1"], QoS::AtLeastOnce),
        ],
    )
    .await;
    assert!(!hub.remote_by_filter.contains_key("old/#"));
    assert!(hub.plan_shared("old/x").is_empty());
    assert_eq!(hub.plan_shared("new/x").len(), 2);
    hub.dispatch(HubCommand::PeerDisconnected {
        node: NodeId("z".into()),
        conn_id: 6,
    })
    .await;
    assert_eq!(
        hub.plan_shared("new/x").len(),
        2,
        "stale link loss must not remove current interest"
    );
    hub.dispatch(HubCommand::PeerDisconnected {
        node: NodeId("z".into()),
        conn_id: 7,
    })
    .await;
    assert!(hub.plan_shared("new/x").is_empty());
    assert_eq!(hub.plan_shared("keep/x").len(), 1);
    hub.dispatch(HubCommand::PeerDead {
        node: NodeId("a".into()),
    })
    .await;
    assert!(hub.remote_by_filter.is_empty());
    assert!(hub.plan_shared("keep/x").is_empty());
}
