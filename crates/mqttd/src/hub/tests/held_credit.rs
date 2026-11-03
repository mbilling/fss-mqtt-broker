//! ADR 0093 Amendment 2: the bounds that push back come before the ones that drop.
//!
//! A publish whose publisher waits for an answer keeps its ingress credit until that
//! answer — through a durable write, an elision hold and a settle window alike — so a
//! durable path that falls behind parks the publishers at their sockets instead of
//! filling the pending table until its stall budget evicts with the acks withheld (the
//! four-shard h3 runs of 2026-11-02). The periodic reconcile scan is not a takeover
//! window for write elision (ADR 0089 Amendment 2), and each shard runs it at its own
//! phase of the cycle.

use super::*;

fn ingress() -> Arc<crate::ingress::IngressCredit> {
    Arc::new(crate::ingress::IngressCredit::new(
        1 << 20,
        1 << 16,
        crate::ingress::OverloadMode::Pause,
    ))
}

/// One hub over `store`, with `credit` attached and `elision_ms` as the window.
fn start(
    store: Arc<dyn SessionStore>,
    credit: &Arc<crate::ingress::IngressCredit>,
    elision_ms: u32,
) -> HubTx {
    let (mut hub, tx) = Hub::with_config(NodeId("hub-test".into()), store);
    hub.set_write_elision_ms(elision_ms);
    hub.attach_ingress(credit.clone());
    tokio::spawn(hub.run());
    tx
}

/// A publish carrying `permit`, as a connection hands one to the hub; `wait` asks for an
/// answer (`QoS` 1), else it is a `QoS` 0 publish nobody waits on.
fn publish_with(
    tx: &HubTx,
    topic: &str,
    payload: &'static [u8],
    permit: crate::ingress::IngressPermit,
    wait: bool,
) -> Option<oneshot::Receiver<PublishOutcome>> {
    let (done_tx, done_rx) = oneshot::channel();
    tx.send(HubCommand::Publish {
        topic: topic.into(),
        payload: Bytes::from_static(payload),
        qos: if wait {
            QoS::AtLeastOnce
        } else {
            QoS::AtMostOnce
        },
        retain: false,
        message_expiry: None,
        app: mqtt_core::AppProperties::default(),
        done: wait.then_some(done_tx),
        v5: true,
        publisher: None,
        credit: Some(permit),
    })
    .unwrap();
    wait.then_some(done_rx)
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

/// The publisher's credit stays charged while its ack waits on the subscriber's durable
/// write, and returns when the write lands and the publisher is answered — not when the
/// hub has dispatched the publish, which is what it did before Amendment 2.
#[tokio::test]
async fn a_publish_keeps_its_credit_until_its_durable_write_lands() {
    let credit = ingress();
    let store = super::ParkingStore::new();
    let as_store: Arc<dyn SessionStore> = store.clone();
    let tx = start(as_store, &credit, 0);
    let (mut rx, _) = attach_full(&tx, "sub", 1, false, 30, 256).await;
    subscribe_qos(&tx, "sub", "t", QoS::AtLeastOnce);
    ping(&tx).await;
    let release = store.park("sub");

    let permit = credit
        .try_acquire_pool(credit.cost(1, 6))
        .expect("an empty pool has credit");
    let charged = credit.in_use();
    assert!(charged > 0);
    let mut done = publish_with(&tx, "t", b"parked", permit, true).unwrap();
    ping(&tx).await;
    assert!(
        matches!(done.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "the publisher is not answered while the subscriber's write is parked"
    );
    assert_eq!(
        credit.in_use(),
        charged,
        "dispatched and unanswered: the credit is held on the pending entry"
    );

    release.send(true).expect("the parked write is waiting");
    assert_eq!(&next_publish(&mut rx).await.payload[..], b"parked");
    assert_eq!(
        timeout(Duration::from_secs(5), done)
            .await
            .expect("acked within 5 s")
            .unwrap(),
        PublishOutcome::Accepted
    );
    ping(&tx).await;
    assert_eq!(
        credit.in_use(),
        0,
        "answered: the credit is back in the pool"
    );
}

/// Under write elision the credit stays charged through the hold and returns when the
/// subscriber's PUBACK discharges it: the publish is answered then, with no write.
#[tokio::test]
async fn a_held_delivery_keeps_the_credit_until_the_subscribers_puback() {
    let credit = ingress();
    // A window far longer than the test: only the PUBACK can end the hold.
    let tx = start(Arc::new(MemorySessionStore::new()), &credit, 600_000);
    let (mut rx, _) = attach_full(&tx, "sub", 1, false, 30, 256).await;
    subscribe_qos(&tx, "sub", "t", QoS::AtLeastOnce);
    ping(&tx).await;
    // The session's first delivery reserves its packet-id block on the written path;
    // elision starts from the next one (ADR 0089).
    let first = credit.try_acquire_pool(credit.cost(1, 5)).unwrap();
    let done = publish_with(&tx, "t", b"first", first, true).unwrap();
    let p = next_publish(&mut rx).await;
    tx.send(HubCommand::PubAck {
        client: ClientId("sub".into()),
        pkid: p.pkid.unwrap(),
        received: tokio::time::Instant::now(),
    })
    .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), done)
            .await
            .unwrap()
            .unwrap(),
        PublishOutcome::Accepted
    );
    ping(&tx).await;
    assert_eq!(credit.in_use(), 0);

    let permit = credit.try_acquire_pool(credit.cost(1, 4)).unwrap();
    let charged = credit.in_use();
    let mut done = publish_with(&tx, "t", b"held", permit, true).unwrap();
    let p = next_publish(&mut rx).await;
    assert_eq!(&p.payload[..], b"held");
    ping(&tx).await;
    assert!(
        matches!(done.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "held in the window: the publisher waits for the subscriber's PUBACK"
    );
    assert_eq!(
        credit.in_use(),
        charged,
        "held: the credit is still charged"
    );
    tx.send(HubCommand::PubAck {
        client: ClientId("sub".into()),
        pkid: p.pkid.unwrap(),
        received: tokio::time::Instant::now(),
    })
    .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), done)
            .await
            .unwrap()
            .unwrap(),
        PublishOutcome::Accepted
    );
    ping(&tx).await;
    assert_eq!(
        credit.in_use(),
        0,
        "discharged: the credit is back in the pool"
    );
}

/// A publish nobody waits for (`QoS` 0) gives its credit back once the hub has
/// dispatched it, as before.
#[tokio::test]
async fn a_publish_nobody_waits_for_returns_its_credit_at_dispatch() {
    let credit = ingress();
    let tx = start(Arc::new(MemorySessionStore::new()), &credit, 0);
    let (mut rx, _) = attach_full(&tx, "sub", 1, true, 0, 256).await;
    subscribe_qos(&tx, "sub", "t", QoS::AtMostOnce);
    ping(&tx).await;
    let permit = credit.try_acquire_pool(credit.cost(1, 3)).unwrap();
    assert!(publish_with(&tx, "t", b"qos", permit, false).is_none());
    assert_eq!(&next_publish(&mut rx).await.payload[..], b"qos");
    ping(&tx).await;
    assert_eq!(credit.in_use(), 0);
}

/// An entry the settle window keeps after its publisher was answered (issue #613 item
/// 2.1) gives its credit back at the answer, not when the window lets it go.
#[test]
fn an_answered_entry_the_settle_window_keeps_returns_its_credit() {
    let credit = ingress();
    let (mut hub, _tx) = Hub::with_config(
        NodeId("held-credit".into()),
        Arc::new(MemorySessionStore::new()),
    );
    hub.set_cluster_configured();
    assert!(
        hub.routing_unsettled(),
        "the rig must be in a settle window, or this passes vacuously"
    );
    let (done, mut answer) = oneshot::channel();
    let id = hub.register_pending(
        done,
        "t",
        &Bytes::from_static(b"settle"),
        QoS::AtLeastOnce,
        false,
        None,
        &mqtt_core::AppProperties::default(),
    );
    let permit = credit.try_acquire_pool(credit.cost(1, 6)).unwrap();
    hub.pending_publishes.hold_credit(id, permit);
    assert!(credit.in_use() > 0);

    // A fan-out that reached somebody releases the ack hold; the local pass completing
    // answers the publisher while the window still owes the publish its replay.
    hub.pending_fan_out_reached(id);
    hub.pending_local_done(id);
    assert_eq!(answer.try_recv().unwrap(), PublishOutcome::Accepted);
    assert!(
        hub.pending_publishes.get(id).is_some(),
        "the settle window keeps the answered entry for its replay"
    );
    assert_eq!(
        credit.in_use(),
        0,
        "answered: the credit is back in the pool"
    );
}

/// ADR 0089 Amendment 2: the periodic reconcile scan does not open write elision's
/// takeover window, while the boot scan, a scan the takeover window starts and the
/// window itself do — and the forward rule of #1053 still waits on any scan.
#[tokio::test]
async fn only_a_takeover_closes_write_elision_and_the_periodic_scan_does_not() {
    let (mut hub, _tx) =
        Hub::with_config(NodeId("window".into()), Arc::new(MemorySessionStore::new()));
    hub.probe
        .sessions_move
        .store(true, std::sync::atomic::Ordering::Relaxed);
    hub.set_cluster_configured();
    // A fresh hub opens with the boot's eager window (8 sweep ticks).
    assert!(hub.takeover_window_open() && hub.takeover_open());
    hub.takeover_reconcile_ticks = 0;
    assert!(!hub.takeover_window_open() && !hub.takeover_open());

    hub.spawn_inherited_session_scan(false);
    assert!(
        hub.takeover_open(),
        "a forward still waits for the periodic scan (#1053)"
    );
    assert!(
        !hub.takeover_window_open(),
        "the periodic scan leaves write elision on"
    );
    // A takeover asking for its scan while the periodic one is in flight takes that
    // scan over: one scan at a time, and the window lasts until it lands.
    hub.spawn_inherited_session_scan(true);
    assert!(hub.takeover_window_open());
    hub.inherit_sessions(Vec::new(), true);
    assert!(!hub.takeover_window_open() && !hub.takeover_open());

    hub.spawn_inherited_session_scan(true);
    assert!(
        hub.takeover_window_open(),
        "the boot or takeover scan holds the window"
    );
    hub.inherit_sessions(Vec::new(), true);
    assert!(!hub.takeover_window_open());

    hub.takeover_reconcile_ticks = 2;
    assert!(
        hub.takeover_window_open(),
        "the eager window holds it with no scan in flight"
    );
    hub.takeover_reconcile_ticks = 0;
    hub.spawn_inherited_session_scan(false);
    assert!(!hub.takeover_window_open());
}

/// The same rule at the eligibility check itself: a delivery that may be held stays
/// eligible while the periodic scan is in flight — the h3 runs of 2026-11-02 wrote every
/// delivery for that scan, every 30 s — and is written while a takeover's scan is.
#[tokio::test]
async fn a_delivery_stays_eligible_for_elision_through_the_periodic_scan() {
    let (mut hub, _tx) = Hub::with_config(
        NodeId("eligible".into()),
        Arc::new(MemorySessionStore::new()),
    );
    hub.set_write_elision_ms(50);
    hub.probe
        .sessions_move
        .store(true, std::sync::atomic::Ordering::Relaxed);
    hub.set_cluster_configured();
    hub.takeover_reconcile_ticks = 0;
    let client = ClientId("sub".into());
    // Online, with a packet id and a send slot free: everything else eligible.
    hub.inflight.insert(
        client.clone(),
        super::super::Inflight {
            block_remaining: 16,
            ..Default::default()
        },
    );
    let message = Message {
        topic: "t".into(),
        payload: Bytes::from_static(b"m"),
        qos: QoS::AtLeastOnce,
        retain: false,
        expires_at: None,
        app: mqtt_core::AppProperties::default(),
    };
    let gate = super::super::AppendGate::Pending(1);
    assert!(
        matches!(
            hub.elidable(&client, &message, &gate),
            super::super::elision::Elidable::Now
        ),
        "eligible with no scan"
    );

    hub.spawn_inherited_session_scan(false);
    assert!(
        matches!(
            hub.elidable(&client, &message, &gate),
            super::super::elision::Elidable::Now
        ),
        "the periodic scan leaves the delivery eligible"
    );
    hub.inherit_sessions(Vec::new(), true);

    hub.spawn_inherited_session_scan(true);
    assert!(
        !matches!(
            hub.elidable(&client, &message, &gate),
            super::super::elision::Elidable::Now
        ),
        "a takeover's scan writes the delivery"
    );
    hub.inherit_sessions(Vec::new(), true);
    assert!(matches!(
        hub.elidable(&client, &message, &gate),
        super::super::elision::Elidable::Now
    ));
}

/// ADR 0093 Amendment 2: shard `k` of `K` runs its periodic scan at `k × EVERY / K` of
/// the reconcile cycle, so a node's shards never enumerate the store at once.
#[test]
fn each_shard_runs_its_periodic_scan_at_its_own_phase() {
    let store: Arc<dyn SessionStore> = Arc::new(MemorySessionStore::new());
    let one = Hub::with_config(NodeId("one".into()), store.clone()).0;
    assert_eq!(one.periodic_scan_phase(), 0, "one shard: as before");
    for shards in [2u8, 4] {
        let mut hubs: Vec<Hub> = (0..shards)
            .map(|_| Hub::with_config(NodeId("k".into()), store.clone()).0)
            .collect();
        let txs: Vec<_> = hubs.iter().map(Hub::sender).collect();
        for (k, hub) in hubs.iter_mut().enumerate() {
            hub.set_shards(u8::try_from(k).unwrap(), txs.clone());
        }
        let phases: Vec<u32> = hubs.iter().map(Hub::periodic_scan_phase).collect();
        let expected: Vec<u32> = (0..u32::from(shards))
            .map(|k| super::super::EXPIRY_RECONCILE_EVERY * k / u32::from(shards))
            .collect();
        assert_eq!(phases, expected);
        let distinct: std::collections::BTreeSet<u32> = phases.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            usize::from(shards),
            "no two shards share a phase"
        );
        assert!(phases
            .iter()
            .all(|&p| p < super::super::EXPIRY_RECONCILE_EVERY));
    }
}
