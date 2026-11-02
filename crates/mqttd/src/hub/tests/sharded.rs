//! ADR 0092 T5: two data shards on one node, behind one [`HubTx`]. A publish entering
//! on one shard reaches the subscribers the other owns through a hand-off, and the
//! publisher's acknowledgement waits for the other shard's answer — through the durable
//! write and through the elision window alike — while the routing mirror keeps the
//! match current when a subscription goes.

use super::*;
use std::collections::BTreeSet;

/// Two shards of one node over one store, with `elision_ms` as the write-elision window.
fn start_shards(elision_ms: u32) -> HubTx {
    start_shards_with(elision_ms, false, None, None, None)
}

/// As [`start_shards`], with ADR 0093 D4's remote wake (the mailboxes and the mailman)
/// when `remote_wake`, the node's ingress credit attached, the metrics attached, and
/// the store given (an in-memory one otherwise).
fn start_shards_with(
    elision_ms: u32,
    remote_wake: bool,
    ingress: Option<&Arc<crate::ingress::IngressCredit>>,
    metrics: Option<&Arc<mqtt_observability::metrics::Metrics>>,
    store: Option<Arc<dyn SessionStore>>,
) -> HubTx {
    let store: Arc<dyn SessionStore> = store.unwrap_or_else(|| Arc::new(MemorySessionStore::new()));
    let mut hubs: Vec<Hub> = (0..2)
        .map(|_| Hub::with_config(NodeId("hub-test".into()), store.clone()).0)
        .collect();
    let txs: Vec<_> = hubs.iter().map(Hub::sender).collect();
    let mailboxes = remote_wake.then(|| {
        let boxes: Arc<[crate::hub::ShardMailbox]> = (0..2)
            .map(|_| crate::hub::ShardMailbox::default())
            .collect();
        let mailman = crate::hub::spawn_mailman(boxes.clone());
        (boxes, mailman)
    });
    for (k, hub) in hubs.iter_mut().enumerate() {
        hub.set_shards(u8::try_from(k).unwrap(), txs.clone());
        hub.set_write_elision_ms(elision_ms);
        // As the node runs it (`cluster.shared_prefer_local`, on by default): the
        // `$share` fast path, where a shard deals among its own members (ADR 0093
        // Amendment 1).
        hub.set_shared_prefer_local(true);
        if let Some((boxes, mailman)) = &mailboxes {
            hub.set_shard_mailboxes(boxes.clone(), mailman.clone());
        }
        if let Some(ingress) = ingress {
            hub.attach_ingress(ingress.clone());
        }
        if let Some(metrics) = metrics {
            hub.attach_metrics(metrics.clone());
        }
    }
    for hub in hubs {
        tokio::spawn(hub.run());
    }
    HubTx::sharded(txs)
}

/// The `shard`-class dispatches of both shards so far: one per hand-off batch and one
/// per batch of answers (ADR 0093 D3), never one per message.
fn shard_dispatches(metrics: &mqtt_observability::metrics::Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .filter(|l| {
            l.starts_with("mqttd_hub_dispatch_seconds_count{") && l.contains("command=\"shard\"")
        })
        .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

/// The `n`th client id (of the `prefix` family) that shard `shard` of two owns.
fn client_on(shard: usize, prefix: &str, n: usize) -> String {
    (0..100_000)
        .map(|i| format!("{prefix}{i}"))
        .filter(|id| crate::hub::shard_of(id, 2) == shard)
        .nth(n)
        .expect("the hash spreads ids over both shards")
}

/// A gated `QoS` 1 publish from `publisher`, routed to its shard as a connection's is.
fn publish_from(
    tx: &HubTx,
    publisher: &str,
    topic: &str,
    payload: &[u8],
) -> oneshot::Receiver<PublishOutcome> {
    let (done_tx, done_rx) = oneshot::channel();
    tx.send(HubCommand::Publish {
        topic: topic.into(),
        payload: Bytes::copy_from_slice(payload),
        qos: QoS::AtLeastOnce,
        retain: false,
        message_expiry: None,
        app: mqtt_core::AppProperties::default(),
        done: Some(done_tx),
        v5: true,
        publisher: Some(ClientId(publisher.into())),
        credit: None,
    })
    .unwrap();
    done_rx
}

/// Both shards have dispatched everything sent before, and everything the first barrier
/// made them send each other (a subscribe's routing mirror): two joined flushes.
async fn settle(tx: &HubTx) {
    ping(tx).await;
    ping(tx).await;
}

async fn next_publish(rx: &mut crate::hub::OutboundRx) -> mqtt_codec::packet::Publish {
    loop {
        let out = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a delivery within 5 s")
            .expect("the outbound stays open");
        if let Some(Packet::Publish(p)) = out.into_packet() {
            return p;
        }
    }
}

/// A subscriber on the other shard receives the publish; the publisher's ack waits for
/// that shard's durable write (elision off) and is released by its answer.
#[tokio::test]
async fn a_publish_reaches_a_subscriber_the_other_shard_owns_and_is_acked_after_its_write() {
    let tx = start_shards(0);
    assert_eq!(tx.shards(), 2);
    let a = client_on(0, "pub-", 0);
    let b = client_on(1, "sub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    let (mut brx, _) = attach_full(&tx, &b, 2, false, 30, 256).await;
    subscribe_qos(&tx, &b, "t", QoS::AtLeastOnce);
    settle(&tx).await;

    let done = publish_from(&tx, &a, "t", b"across");
    let p = next_publish(&mut brx).await;
    assert_eq!(&p.payload[..], b"across");
    assert_eq!(p.qos, QoS::AtLeastOnce);
    let pkid = p.pkid.expect("a QoS 1 delivery");
    assert_eq!(
        timeout(Duration::from_secs(5), done)
            .await
            .expect("acked within 5 s")
            .unwrap(),
        PublishOutcome::Accepted
    );
    tx.send(HubCommand::PubAck {
        client: ClientId(b.into()),
        pkid,
        received: tokio::time::Instant::now(),
    })
    .unwrap();
    settle(&tx).await;
}

/// ADR 0093 D3: a hand-off carries the publish's ingress credit, which returns to the
/// pool when the other shard answers — not when the origin's dispatch ends — so a shard
/// that answers late keeps the pool charged and parks the publishers that feed it. The
/// other shard's write is parked by the store until the test releases it.
#[tokio::test]
async fn a_hand_off_holds_the_publishers_credit_until_the_other_shard_answers() {
    let node = Arc::new(crate::ingress::IngressCredit::new(
        1 << 20,
        1 << 16,
        crate::ingress::OverloadMode::Pause,
    ));
    let store = super::ParkingStore::new();
    let as_store: Arc<dyn SessionStore> = store.clone();
    let tx = start_shards_with(0, false, Some(&node), None, Some(as_store));
    let a = client_on(0, "pub-", 0);
    let b = client_on(1, "sub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    let (mut brx, _) = attach_full(&tx, &b, 2, false, 30, 256).await;
    subscribe_qos(&tx, &b, "t", QoS::AtLeastOnce);
    settle(&tx).await;
    // The subscriber's write on the other shard parks: that shard's answer, and the
    // publisher's acknowledgement, wait on it.
    let release = store.park(&b);

    let permit = node
        .try_acquire_pool(node.cost(1, 6))
        .expect("an empty pool has credit");
    let charged = node.in_use();
    assert!(charged > 0);
    let (done_tx, mut done_rx) = oneshot::channel();
    tx.send(HubCommand::Publish {
        topic: "t".into(),
        payload: Bytes::from_static(b"credit"),
        qos: QoS::AtLeastOnce,
        retain: false,
        message_expiry: None,
        app: mqtt_core::AppProperties::default(),
        done: Some(done_tx),
        v5: true,
        publisher: Some(ClientId(a.as_str().into())),
        credit: Some(permit),
    })
    .unwrap();
    settle(&tx).await;
    assert!(
        matches!(done_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "the publisher is not answered while the other shard's write is parked"
    );
    assert_eq!(
        node.in_use(),
        charged,
        "handed off and unanswered: the credit is held, not released by the origin's dispatch"
    );
    release.send(true).expect("the parked write is waiting");
    let p = next_publish(&mut brx).await;
    assert_eq!(&p.payload[..], b"credit");
    assert_eq!(
        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("acked within 5 s")
            .unwrap(),
        PublishOutcome::Accepted
    );
    settle(&tx).await;
    assert_eq!(
        node.in_use(),
        0,
        "the last shard answered: the credit is back in the pool"
    );
}

/// ADR 0093 D3: a burst of publishes to the other shard goes over as batches, not as
/// one command per message — the `shard` class dispatches far fewer times than there are
/// messages — and every publish is delivered and answered.
#[tokio::test]
async fn a_burst_of_hand_offs_goes_over_in_batches_and_every_publish_is_answered() {
    const MESSAGES: usize = 40;
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("batched"));
    let tx = start_shards_with(0, false, None, Some(&metrics), None);
    let a = client_on(0, "pub-", 0);
    let b = client_on(1, "sub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    let (mut brx, _) = attach_full(&tx, &b, 2, false, 30, 256).await;
    subscribe_qos(&tx, &b, "t", QoS::AtLeastOnce);
    settle(&tx).await;
    let before = shard_dispatches(&metrics);

    let dones: Vec<_> = (0..MESSAGES)
        .map(|i| publish_from(&tx, &a, "t", format!("m{i}").as_bytes()))
        .collect();
    let mut seen = BTreeSet::new();
    for _ in 0..MESSAGES {
        seen.insert(next_publish(&mut brx).await.payload.to_vec());
    }
    assert_eq!(seen.len(), MESSAGES, "every publish delivered once");
    for done in dones {
        assert_eq!(
            timeout(Duration::from_secs(5), done)
                .await
                .expect("acked within 5 s")
                .unwrap(),
            PublishOutcome::Accepted
        );
    }
    settle(&tx).await;
    let dispatches = shard_dispatches(&metrics) - before;
    assert!(
        dispatches < MESSAGES as u64,
        "{MESSAGES} messages went over in {dispatches} shard dispatches: batched, not one each"
    );
}

/// ADR 0093 D3, the per-pair credit: a target shard at its hand-off cap parks the
/// origin's publishers — the data lane waits, the control lane, where the hand-offs and
/// their answers travel, does not — and the target's answer reopens it.
#[tokio::test]
async fn a_target_at_its_hand_off_cap_parks_the_origins_data_lane_until_it_answers() {
    let store: Arc<dyn SessionStore> = Arc::new(MemorySessionStore::new());
    let mut a = Hub::with_config(NodeId("hub-test".into()), store.clone()).0;
    let b = Hub::with_config(NodeId("hub-test".into()), store).0;
    a.set_shards(0, vec![a.sender(), b.sender()]);
    a.set_handoff_inflight_cap(2);
    // A pending publish handed to shard 1, and the pair at its cap.
    let (done, _rx) = oneshot::channel();
    let id = a.register_pending(
        done,
        "t",
        &Bytes::from_static(b"owed"),
        QoS::AtLeastOnce,
        false,
        None,
        &mqtt_core::AppProperties::default(),
    );
    a.pending_publishes.get_mut(id).unwrap().shard_outstanding = 1 << 1;
    a.handoff_inflight[1] = 2;
    a.enqueue(HubCommand::ShardRoute {
        client: ClientId("elsewhere".into()),
        filters: vec![],
        online: true,
    });
    a.enqueue(HubCommand::Draining);
    assert!(
        matches!(a.next_command(), Some(HubCommand::Draining)),
        "the control lane moves whatever the cap"
    );
    assert!(
        a.next_command().is_none(),
        "the data lane waits while shard 1 is at its cap"
    );
    a.shard_done(1, id, crate::hub::DurableOutcome::Ok, true);
    assert_eq!(a.handoff_inflight[1], 1, "one answer fewer owed");
    assert!(
        matches!(a.next_command(), Some(HubCommand::ShardRoute { .. })),
        "answered: the data lane moves again"
    );
    assert!(
        matches!(
            HubCommand::ShardDeliver(Box::new(crate::hub::ShardBatch {
                origin: 1,
                messages: Vec::new()
            }))
            .lane(),
            crate::hub::Lane::Control
        ),
        "a batch of hand-offs travels on the control lane, never behind a parked data lane"
    );
}

/// ADR 0093 D4's second arm: with the hand-off's wake issued by the mailman from
/// outside the runtime, the batch still reaches the other shard and the publisher is
/// still answered by that shard's write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hand_off_through_the_mailbox_and_the_mailman_is_delivered_and_answered() {
    let tx = start_shards_with(0, true, None, None, None);
    let a = client_on(0, "pub-", 0);
    let b = client_on(1, "sub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    let (mut brx, _) = attach_full(&tx, &b, 2, false, 30, 256).await;
    subscribe_qos(&tx, &b, "t", QoS::AtLeastOnce);
    settle(&tx).await;

    let done = publish_from(&tx, &a, "t", b"by mail");
    let p = next_publish(&mut brx).await;
    assert_eq!(&p.payload[..], b"by mail");
    assert_eq!(
        timeout(Duration::from_secs(5), done)
            .await
            .expect("acked within 5 s")
            .unwrap(),
        PublishOutcome::Accepted
    );
    settle(&tx).await;
}

/// ADR 0093 Amendment 1: a `$share` group with a member on each shard deals a
/// publish to the member on the publisher's own shard — no hand-off — and reaches
/// across to the other shard's member only once the publisher's shard has no member
/// online.
#[tokio::test]
async fn a_shared_group_deals_on_the_publishers_shard_and_crosses_only_when_it_has_no_member_online(
) {
    const MESSAGES: usize = 20;
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("shard-local"));
    let tx = start_shards_with(0, false, None, Some(&metrics), None);
    let p = client_on(0, "pub-", 0);
    let near = client_on(0, "con-", 0);
    let far = client_on(1, "con-", 0);
    let _ = attach_full(&tx, &p, 1, true, 0, 256).await;
    let (mut near_rx, _) = attach_full(&tx, &near, 2, true, 0, 256).await;
    let (mut far_rx, _) = attach_full(&tx, &far, 3, true, 0, 256).await;
    subscribe_qos(&tx, &near, "$share/g/t", QoS::AtLeastOnce);
    subscribe_qos(&tx, &far, "$share/g/t", QoS::AtLeastOnce);
    settle(&tx).await;
    let handed = || {
        metrics
            .render()
            .lines()
            .find(|l| l.starts_with("mqttd_publish_forwarded_total{reason=\"shard\"}"))
            .and_then(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
            .unwrap_or(0)
    };

    let dones: Vec<_> = (0..MESSAGES)
        .map(|i| publish_from(&tx, &p, "t", format!("m{i}").as_bytes()))
        .collect();
    for _ in 0..MESSAGES {
        // Acknowledged as it arrives, so the member's in-flight window keeps moving.
        let p = next_publish(&mut near_rx).await;
        tx.send(HubCommand::PubAck {
            client: ClientId(near.as_str().into()),
            pkid: p.pkid.expect("a QoS 1 delivery"),
            received: tokio::time::Instant::now(),
        })
        .unwrap();
    }
    for done in dones {
        assert_eq!(
            timeout(Duration::from_secs(5), done)
                .await
                .expect("acked within 5 s")
                .unwrap(),
            PublishOutcome::Accepted
        );
    }
    settle(&tx).await;
    assert!(
        far_rx.try_recv().is_err(),
        "the far member got nothing while the near one was online"
    );
    assert_eq!(handed(), 0, "no publish was handed to the other shard");

    // The near member leaves: the deal reaches across.
    detach(&tx, &near, 2);
    settle(&tx).await;
    let done = publish_from(&tx, &p, "t", b"across");
    assert_eq!(&next_publish(&mut far_rx).await.payload[..], b"across");
    assert_eq!(
        timeout(Duration::from_secs(5), done)
            .await
            .expect("acked within 5 s")
            .unwrap(),
        PublishOutcome::Accepted
    );
    assert_eq!(
        handed(),
        1,
        "one publish crossed, once the near member was gone"
    );
}

/// A `$share` group whose members all sit on the other shard: every publish is handed
/// across, every one is acknowledged, and under the elision window the acks are
/// released by the members' PUBACKs through the hand-off's answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_group_on_the_other_shard_takes_every_publish_under_the_window() {
    const MESSAGES: usize = 60;
    let tx = start_shards(50);
    let p = client_on(0, "pub-", 0);
    let members = [client_on(1, "con-", 0), client_on(1, "con-", 1)];
    let _ = attach_full(&tx, &p, 1, true, 0, 256).await;
    // Each member reports a delivery here before it acknowledges it: the publisher's
    // ack is released by the write or the PUBACK, never by the member having read it.
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<(usize, Vec<u8>)>();
    for (i, m) in members.iter().enumerate() {
        let (mut rx, _) = attach_full(&tx, m, 10 + i as u64, false, 30, 256).await;
        subscribe_qos(&tx, m, "$share/g/t", QoS::AtLeastOnce);
        let (tx, seen, m) = (tx.clone(), seen_tx.clone(), ClientId(m.as_str().into()));
        tokio::spawn(async move {
            while let Some(out) = rx.recv().await {
                let Some(Packet::Publish(p)) = out.into_packet() else {
                    continue;
                };
                let _ = seen.send((i, p.payload.to_vec()));
                let _ = tx.send(HubCommand::PubAck {
                    client: m.clone(),
                    pkid: p.pkid.expect("a QoS 1 delivery"),
                    received: tokio::time::Instant::now(),
                });
            }
        });
    }
    settle(&tx).await;

    let dones: Vec<_> = (0..MESSAGES)
        .map(|n| publish_from(&tx, &p, "t", format!("m{n}").as_bytes()))
        .collect();
    for done in dones {
        assert_eq!(
            timeout(Duration::from_secs(10), done)
                .await
                .expect("acked within 10 s")
                .unwrap(),
            PublishOutcome::Accepted
        );
    }
    let mut got = [BTreeSet::new(), BTreeSet::new()];
    for _ in 0..MESSAGES {
        let (i, payload) = timeout(Duration::from_secs(10), seen_rx.recv())
            .await
            .expect("every publish reaches a member within 10 s")
            .expect("the members stay attached");
        got[i].insert(payload);
    }
    let all: BTreeSet<Vec<u8>> = got[0].union(&got[1]).cloned().collect();
    assert_eq!(
        all.len(),
        MESSAGES,
        "every publish reached one member, none twice"
    );
    assert!(
        !got[0].is_empty() && !got[1].is_empty(),
        "the dealing took turns between the far shard's members: {} and {}",
        got[0].len(),
        got[1].len()
    );
}

/// An unsubscribe on one shard stops the other's matches: after it, a publish to the
/// filter is acknowledged (nobody is owed it) and the subscriber's next delivery is the
/// one for the filter it still holds.
#[tokio::test]
async fn an_unsubscribe_is_mirrored_to_the_other_shard() {
    let tx = start_shards(0);
    let a = client_on(0, "pub-", 0);
    let b = client_on(1, "sub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    let (mut brx, _) = attach_full(&tx, &b, 2, false, 30, 256).await;
    subscribe_qos(&tx, &b, "t", QoS::AtLeastOnce);
    subscribe_qos(&tx, &b, "u", QoS::AtLeastOnce);
    settle(&tx).await;
    tx.send(HubCommand::Unsubscribe {
        client: ClientId(b.as_str().into()),
        filters: vec!["t".into()],
        reply: None,
    })
    .unwrap();
    settle(&tx).await;

    let gone = publish_from(&tx, &a, "t", b"not for b");
    let kept = publish_from(&tx, &a, "u", b"still for b");
    assert_eq!(
        timeout(Duration::from_secs(5), gone)
            .await
            .unwrap()
            .unwrap(),
        PublishOutcome::Accepted
    );
    assert_eq!(
        timeout(Duration::from_secs(5), kept)
            .await
            .unwrap()
            .unwrap(),
        PublishOutcome::Accepted
    );
    let p = next_publish(&mut brx).await;
    assert_eq!(p.topic, "u");
    assert_eq!(&p.payload[..], b"still for b");
}

/// A `QoS` 0 publish is handed off ungated: the other shard's subscriber gets it and
/// nobody waits for an answer.
#[tokio::test]
async fn a_qos0_publish_is_handed_off_ungated() {
    let tx = start_shards(0);
    let a = client_on(0, "pub-", 0);
    let b = client_on(1, "sub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    let (mut brx, _) = attach_full(&tx, &b, 2, true, 0, 256).await;
    subscribe_qos(&tx, &b, "z", QoS::AtMostOnce);
    settle(&tx).await;
    tx.send(HubCommand::Publish {
        topic: "z".into(),
        payload: Bytes::from_static(b"fire and forget"),
        qos: QoS::AtMostOnce,
        retain: false,
        message_expiry: None,
        app: mqtt_core::AppProperties::default(),
        done: None,
        v5: true,
        publisher: Some(ClientId(a.into())),
        credit: None,
    })
    .unwrap();
    let p = next_publish(&mut brx).await;
    assert_eq!(&p.payload[..], b"fire and forget");
    assert_eq!(p.qos, QoS::AtMostOnce);
}
