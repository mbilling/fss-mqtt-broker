//! ADR 0093 Amendment 5: a node with peers runs its shards. Shard 0 hears from the peer
//! links and tells the other shards what changed in its view of the peers, so every
//! shard forwards its own publishes; a forward's answer finds the shard that sent it; a
//! peer's forward to another shard's subscriber is answered once that shard has stored
//! it; and the node's interest, every shard's, is gossiped from shard 0.

use super::sharded::{client_on, publish_from, settle, start_shards, start_shards_with};
use super::*;
use mqtt_cluster::peer::ForwardVerdict;

fn peer() -> NodeId {
    NodeId("peer-a".into())
}

/// Link `peer()` to the node through `tx` (shard 0 hears of it), on connection
/// `conn_id`, and say what it subscribes to.
fn link_peer(tx: &HubTx, conn_id: u64, filters: &[&str]) -> mpsc::UnboundedReceiver<PeerMessage> {
    let (peer_tx, to_peer): (PeerOutbound, _) = mpsc::unbounded_channel();
    tx.send(HubCommand::PeerConnected {
        node: peer(),
        conn_id,
        ctl: peer_tx.clone(),
        tx: peer_tx,
        cert_serial: None,
        cert_ends: crate::hub::LinkCertEnds::default(),
        proto: mqtt_cluster::peer::PROTO_MAX,
        depth: Arc::default(),
    })
    .unwrap();
    tx.send(HubCommand::RemoteInterest {
        node: peer(),
        conn_id: Some(conn_id),
        filters: filters.iter().map(|f| (*f).to_string()).collect(),
    })
    .unwrap();
    to_peer
}

/// What is queued on the link now.
fn drain(to_peer: &mut mpsc::UnboundedReceiver<PeerMessage>) -> Vec<PeerMessage> {
    let mut out = Vec::new();
    while let Ok(m) = to_peer.try_recv() {
        out.push(m);
    }
    out
}

/// The next acknowledged forward on the link: its seq.
async fn next_forward(to_peer: &mut mpsc::UnboundedReceiver<PeerMessage>) -> u64 {
    loop {
        let m = timeout(Duration::from_secs(5), to_peer.recv())
            .await
            .expect("a forward within 5 s")
            .expect("the link stays open");
        match m {
            PeerMessage::PublishAcked { seq, .. } | PeerMessage::PublishAckedTagged { seq, .. } => {
                return seq;
            }
            _ => {}
        }
    }
}

/// The shard tag a forward seq carries (`set_shards`).
fn issuer(seq: u64) -> u64 {
    seq >> 60
}

/// A publish on the shard that does not hear from the links is forwarded on shard 0's
/// link, and the peer's verdict reaches the shard that sent it: the publisher is
/// answered.
#[tokio::test]
async fn a_publish_on_another_shard_is_forwarded_and_its_answer_finds_that_shard() {
    let tx = start_shards(0);
    let mut to_peer = link_peer(&tx, 1, &["t/#"]);
    let a = client_on(1, "pub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    settle(&tx).await;
    drain(&mut to_peer);

    let mut done = publish_from(&tx, &a, "t/x", b"over");
    let seq = next_forward(&mut to_peer).await;
    assert_eq!(
        issuer(seq),
        1,
        "sent by the publisher's shard, under its seqs"
    );
    settle(&tx).await;
    assert!(
        matches!(done.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "the publisher waits for the peer"
    );
    tx.send(HubCommand::RemotePublishVerdict {
        node: peer(),
        seq,
        verdict: ForwardVerdict::Reached,
    })
    .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), done)
            .await
            .expect("answered within 5 s")
            .unwrap(),
        PublishOutcome::Accepted
    );
}

/// A link lost on shard 0 is lost on every shard: a publish on the other shard is held
/// for the peer, not sent on the old link, and goes out on the new one once the peer
/// says what it subscribes to there.
#[tokio::test]
async fn a_lost_link_is_lost_on_every_shard_and_what_was_owed_goes_out_on_the_next() {
    let tx = start_shards(0);
    let mut first = link_peer(&tx, 1, &["t/#"]);
    let a = client_on(1, "pub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    settle(&tx).await;
    drain(&mut first);
    tx.send(HubCommand::PeerDisconnected {
        node: peer(),
        conn_id: 1,
    })
    .unwrap();
    settle(&tx).await;

    let mut done = publish_from(&tx, &a, "t/x", b"held");
    settle(&tx).await;
    assert!(
        drain(&mut first).iter().all(|m| !matches!(
            m,
            PeerMessage::PublishAcked { .. } | PeerMessage::PublishAckedTagged { .. }
        )),
        "nothing is sent on a link shard 0 let go of"
    );
    assert!(
        matches!(done.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "the publish is owed to the peer, and held"
    );

    let mut second = link_peer(&tx, 2, &["t/#"]);
    let seq = next_forward(&mut second).await;
    assert_eq!(issuer(seq), 1);
    tx.send(HubCommand::RemotePublishVerdict {
        node: peer(),
        seq,
        verdict: ForwardVerdict::Reached,
    })
    .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), done)
            .await
            .expect("answered within 5 s")
            .unwrap(),
        PublishOutcome::Accepted
    );
}

/// A peer's acknowledged forward handled on one shard, for a subscriber the other shard
/// owns, is answered only once that shard has stored its copy: the hand-off is gated as
/// a client's publish is.
#[tokio::test]
async fn a_peers_forward_is_answered_once_the_other_shard_has_stored_it() {
    let store = super::ParkingStore::new();
    let as_store: Arc<dyn SessionStore> = store.clone();
    let tx = start_shards_with(0, false, None, None, Some(as_store));
    let b = client_on(1, "sub-", 0);
    let (mut brx, _) = attach_full(&tx, &b, 2, false, 30, 256).await;
    subscribe_qos(&tx, &b, "t", QoS::AtLeastOnce);
    let mut to_peer = link_peer(&tx, 1, &[]);
    settle(&tx).await;
    drain(&mut to_peer);
    // A seq whose forward is handled on shard 0, which does not own the subscriber.
    let lane = (0..8u64)
        .find(|lane| crate::hub::peer_shard(&peer(), *lane, 2) == 0)
        .expect("one of eight lanes lands on shard 0");
    let seq = (lane << 60) | 7;
    let release = store.park(&b);
    tx.send(HubCommand::RemotePublishAcked {
        node: peer(),
        seq,
        topic: "t".into(),
        payload: Bytes::from_static(b"gated"),
        qos: QoS::AtLeastOnce,
        retain: false,
        message_expiry: None,
        app: mqtt_core::AppProperties::default(),
        origin: Some(42),
        replay: false,
    })
    .unwrap();
    settle(&tx).await;
    assert!(
        drain(&mut to_peer)
            .iter()
            .all(|m| !matches!(m, PeerMessage::PublishVerdict { .. })),
        "no verdict while the other shard's write is parked"
    );
    release.send(true).expect("the parked write is waiting");
    let p = super::sharded::next_publish(&mut brx).await;
    assert_eq!(&p.payload[..], b"gated");
    let verdict = loop {
        let m = timeout(Duration::from_secs(5), to_peer.recv())
            .await
            .expect("a verdict within 5 s")
            .expect("the link stays open");
        if let PeerMessage::PublishVerdict { seq: s, verdict } = m {
            assert_eq!(s, seq);
            break verdict;
        }
    };
    assert_eq!(verdict, ForwardVerdict::Reached);
}

/// The node's interest is gossiped by shard 0 and holds every shard's filters: the peer
/// replaces its view of the node wholesale, so a snapshot of one shard's would hide the
/// others' subscribers.
#[tokio::test]
async fn the_nodes_interest_is_gossiped_from_shard_0_with_every_shards_filters() {
    let tx = start_shards(0);
    let mut to_peer = link_peer(&tx, 1, &[]);
    let a = client_on(0, "s0-", 0);
    let b = client_on(1, "s1-", 0);
    let _ = attach_full(&tx, &a, 1, false, 30, 256).await;
    let _ = attach_full(&tx, &b, 2, false, 30, 256).await;
    settle(&tx).await;
    drain(&mut to_peer);
    subscribe_qos(&tx, &b, "s1/#", QoS::AtLeastOnce);
    settle(&tx).await;
    subscribe_qos(&tx, &a, "s0/#", QoS::AtLeastOnce);
    // Until both are in a snapshot. (A snapshot from before the shards' boot scans landed
    // may come first: gossip opens once every shard's has.)
    let both = vec!["s0/#".to_string(), "s1/#".to_string()];
    let mut snapshots: Vec<Vec<String>> = Vec::new();
    while snapshots.last() != Some(&both) {
        let m = timeout(Duration::from_secs(5), to_peer.recv())
            .await
            .expect("the node's interest within 5 s")
            .expect("the link stays open");
        if let PeerMessage::Interest { mut filters } = m {
            filters.sort();
            snapshots.push(filters);
        }
    }
    let s1 = "s1/#".to_string();
    let first = snapshots
        .iter()
        .position(|f| f.contains(&s1))
        .expect("the last has it");
    assert!(
        snapshots[first..].iter().all(|f| f.contains(&s1)),
        "once shard 1's filter is gossiped, no snapshot leaves it out: {snapshots:?}"
    );
}

/// A durable retained keyspace whose commits wait for the test, counting how many are
/// in flight at once.
#[derive(Debug)]
struct GatedRetained {
    inner:
        mqtt_storage::retained_log::ReplicatedRetained<mqtt_storage::repl::InMemoryReplicatedLog>,
    open: tokio::sync::watch::Receiver<bool>,
    in_flight: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl mqtt_storage::retained_log::DurableRetained for GatedRetained {
    async fn set(
        &self,
        topic: &str,
        payload: &[u8],
        qos: u8,
        props: &AppProps,
        expires_at: Option<u64>,
    ) -> Result<(u64, u64), mqtt_storage::StorageError> {
        use std::sync::atomic::Ordering::SeqCst;
        let now = self.in_flight.fetch_add(1, SeqCst) + 1;
        self.most.fetch_max(now, SeqCst);
        let mut open = self.open.clone();
        let _ = open.wait_for(|o| *o).await;
        let out = self.inner.set(topic, payload, qos, props, expires_at).await;
        self.in_flight.fetch_sub(1, SeqCst);
        out
    }

    async fn clear(&self, topic: &str) -> Result<(u64, u64), mqtt_storage::StorageError> {
        self.inner.clear(topic).await
    }

    async fn get(
        &self,
        topic: &str,
    ) -> Result<Option<mqtt_storage::retained_log::RetainedEntry>, mqtt_storage::StorageError> {
        self.inner.get(topic).await
    }

    async fn topics(&self) -> Result<Vec<String>, mqtt_storage::StorageError> {
        self.inner.topics().await
    }

    async fn reap(&self, topic: &str) -> Result<(), mqtt_storage::StorageError> {
        self.inner.reap(topic).await
    }
}

/// Two shards with durable retained on, one keyspace and one retained cache for both, as
/// the node gives them.
fn start_durable_shards(durable: &Arc<GatedRetained>) -> HubTx {
    let store: Arc<dyn SessionStore> = Arc::new(MemorySessionStore::new());
    let retained: Arc<dyn mqtt_storage::RetainedStore> =
        Arc::new(mqtt_storage::MemoryRetainedStore::new());
    let mut hubs: Vec<Hub> = (0..2)
        .map(|_| Hub::with_config(NodeId("hub-test".into()), store.clone()).0)
        .collect();
    let txs: Vec<_> = hubs.iter().map(Hub::sender).collect();
    let board = crate::hub::RoutingBoard::new(2);
    for (k, hub) in hubs.iter_mut().enumerate() {
        hub.set_shards(u8::try_from(k).unwrap(), txs.clone(), &board);
        hub.attach_durable_retained(durable.clone());
        hub.attach_retained_store(retained.clone());
    }
    for hub in hubs {
        tokio::spawn(hub.run());
    }
    HubTx::sharded(txs)
}

/// A retained `QoS` 1 publish from `publisher`, on its shard.
fn publish_retained_from(
    tx: &HubTx,
    publisher: &str,
    topic: &str,
    payload: &'static [u8],
) -> oneshot::Receiver<PublishOutcome> {
    let (done_tx, done_rx) = oneshot::channel();
    tx.send(HubCommand::Publish {
        topic: topic.into(),
        payload: Bytes::from_static(payload),
        qos: QoS::AtLeastOnce,
        retain: true,
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

/// Shard 0 commits every retained mutation of the node, one at a time (ADR 0037 §5,
/// ADR 0093 Amendment 5): a retained publish on the other shard waits behind one on
/// shard 0, each publisher is answered once its own commit lands, and a subscriber on
/// either shard replays both from the node's one cache.
#[tokio::test]
async fn shard_0_commits_every_retained_mutation_one_at_a_time() {
    let (release, open) = tokio::sync::watch::channel(false);
    let durable = Arc::new(GatedRetained {
        inner: mqtt_storage::retained_log::ReplicatedRetained::new(
            mqtt_storage::repl::InMemoryReplicatedLog::new(),
        ),
        open,
        in_flight: std::sync::atomic::AtomicUsize::new(0),
        most: std::sync::atomic::AtomicUsize::new(0),
    });
    let tx = start_durable_shards(&durable);
    let a = client_on(0, "pub-", 0);
    let b = client_on(1, "pub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    let _ = attach_full(&tx, &b, 2, true, 0, 256).await;
    settle(&tx).await;

    let mut done_a = publish_retained_from(&tx, &a, "r/a", b"from 0");
    let mut done_b = publish_retained_from(&tx, &b, "r/b", b"from 1");
    settle(&tx).await;
    settle(&tx).await;
    assert_eq!(
        durable.most.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "one commit in flight for the node"
    );
    assert!(matches!(
        done_a.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        done_b.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release.send(true).unwrap();
    for done in [done_a, done_b] {
        assert_eq!(
            timeout(Duration::from_secs(5), done)
                .await
                .expect("answered within 5 s")
                .unwrap(),
            PublishOutcome::Accepted
        );
    }
    assert_eq!(durable.most.load(std::sync::atomic::Ordering::SeqCst), 1);

    for (shard, conn) in [(0, 3), (1, 4)] {
        let s = client_on(shard, "sub-", 0);
        let (mut rx, _) = attach_full(&tx, &s, conn, true, 0, 256).await;
        subscribe_qos(&tx, &s, "r/#", QoS::AtLeastOnce);
        let mut got = Vec::new();
        for _ in 0..2 {
            let p = super::sharded::next_publish(&mut rx).await;
            assert!(p.retain, "a replay carries RETAIN");
            got.push(p.topic.clone());
        }
        got.sort();
        assert_eq!(got, ["r/a", "r/b"], "shard {shard} replays both");
    }
}

/// Without a durable plane a retained value set through one shard is in the node's one
/// store: a subscriber on the other shard replays it.
#[tokio::test]
async fn a_value_retained_through_one_shard_is_replayed_on_the_other() {
    let tx = start_shards(0);
    let a = client_on(0, "pub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    tx.send(HubCommand::Publish {
        topic: "m/t".into(),
        payload: Bytes::from_static(b"kept"),
        qos: QoS::AtMostOnce,
        retain: true,
        message_expiry: None,
        app: mqtt_core::AppProperties::default(),
        done: None,
        v5: true,
        publisher: Some(ClientId(a.as_str().into())),
        credit: None,
    })
    .unwrap();
    settle(&tx).await;
    let b = client_on(1, "sub-", 0);
    let (mut rx, _) = attach_full(&tx, &b, 2, true, 0, 256).await;
    subscribe_qos(&tx, &b, "m/#", QoS::AtMostOnce);
    let p = super::sharded::next_publish(&mut rx).await;
    assert_eq!(&p.payload[..], b"kept");
    assert!(p.retain);
}

/// The node's gauges are its shards' together (ADR 0093 Amendment 5): a session on each
/// shard reads as two, not as whichever shard wrote last.
#[tokio::test]
async fn the_sessions_gauge_counts_every_shard() {
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("shards"));
    let tx = start_shards_with(0, false, None, Some(&metrics), None);
    let a = client_on(0, "s-", 0);
    let b = client_on(1, "s-", 0);
    let _on_a = attach_full(&tx, &a, 1, false, 30, 256).await;
    let _on_b = attach_full(&tx, &b, 2, false, 30, 256).await;
    let read = || {
        metrics
            .render()
            .lines()
            .find(|l| l.starts_with("mqttd_sessions "))
            .map(str::to_string)
    };
    timeout(Duration::from_secs(5), async {
        while read().as_deref() != Some("mqttd_sessions 2") {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("both shards' sessions within 5 s");
    // And it stays there: each shard's refresh writes the node's figure.
    for _ in 0..3 {
        // SETTLE(shard-gauges-hold): what is under test is an absence — that neither
        // shard's next refresh (one a second each) writes its own count back over the
        // node's — so there is no event to wait on; 1.2 s spans both shards' next ticks.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(read().as_deref(), Some("mqttd_sessions 2"));
    }
}

/// Two shards of one node over one store, driven without their loops: what one sends
/// the other is read from the hub's own batches.
fn two_unrun_shards() -> (Hub, Hub) {
    let store: Arc<dyn SessionStore> = Arc::new(MemorySessionStore::new());
    let mut a = Hub::with_config(NodeId("hub-test".into()), store.clone()).0;
    let mut b = Hub::with_config(NodeId("hub-test".into()), store).0;
    let txs = vec![a.sender(), b.sender()];
    let board = crate::hub::RoutingBoard::new(2);
    a.set_shards(0, txs.clone(), &board);
    b.set_shards(1, txs, &board);
    (a, b)
}

/// The shard a `$share` delivery was handed to says, in its answer, that the member was
/// gone (ADR 0093 Amendment 5): the outcome claims nothing stored and nothing failed.
#[tokio::test]
async fn a_hand_off_to_a_member_that_is_gone_names_it_in_the_answer() {
    let (_a, mut b) = two_unrun_shards();
    let gone = ClientId(client_on(1, "m-", 0).into());
    let key_hash = crate::hub::delivery::shared_cursor_hash("g", "t");
    b.dispatch_shard_deliver(crate::hub::ShardDeliver {
        origin: 0,
        gate: Some(7),
        from: None,
        replay: false,
        live_only: false,
        topic: "t".into(),
        payload: Bytes::from_static(b"x"),
        qos: QoS::AtLeastOnce,
        retain: false,
        message_expiry: None,
        app: mqtt_core::AppProperties::default(),
        targets: vec![crate::hub::ShardTarget::Shared {
            client: gone.clone(),
            qos: QoS::AtLeastOnce,
            key_hash,
            tried: Vec::new(),
        }],
        credit: None,
    });
    let answers = std::mem::take(&mut b.shard_done_batches[0]);
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0].id, 7);
    assert!(matches!(answers[0].outcome, crate::hub::DurableOutcome::Ok));
    assert_eq!(
        answers[0].gone,
        vec![crate::hub::GoneMember {
            key_hash,
            client: gone,
            qos: QoS::AtLeastOnce,
            tried: Vec::new(),
        }]
    );
}

/// The publisher's shard places a delivery whose member was gone with another member of
/// the group, as a peer's member-gone answer is placed (ADR 0093 Amendment 5): here the
/// other shard's online member, handed to it under the same pending publish, which stays
/// unanswered until that shard answers.
#[tokio::test]
async fn a_member_gone_on_another_shard_is_dealt_again_there() {
    let (mut a, mut b) = two_unrun_shards();
    let gone = ClientId(client_on(1, "m-", 0).into());
    let there = ClientId(client_on(1, "m-", 1).into());
    b.shared
        .subscribe(there.clone(), "g", "t", QoS::AtLeastOnce, true);
    b.routing_changed();
    b.publish_routing(true);
    a.refresh_foreign_routing();
    let key: (String, String) = ("g".into(), "t".into());
    let key_hash = crate::hub::delivery::shared_cursor_hash("g", "t");
    a.shared_cursor.insert(key_hash, (key, 0));
    let (done, mut answered) = oneshot::channel();
    let id = a.register_pending(
        done,
        "t",
        &Bytes::from_static(b"moved"),
        QoS::AtLeastOnce,
        false,
        None,
        &mqtt_core::AppProperties::default(),
    );
    if let Some(p) = a.pending_publishes.get_mut(id) {
        p.local_done = true;
        p.shard_outstanding = 0b10;
    }
    a.shard_done(
        1,
        crate::hub::ShardAnswer {
            id,
            outcome: crate::hub::DurableOutcome::Ok,
            reached: false,
            gone: vec![crate::hub::GoneMember {
                key_hash,
                client: gone.clone(),
                qos: QoS::AtLeastOnce,
                tried: Vec::new(),
            }],
        },
    );
    assert!(
        matches!(
            answered.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "not answered: the delivery was placed again, and waits for its shard"
    );
    assert_eq!(
        a.pending_publishes.get(id).map(|p| p.shard_outstanding),
        Some(0b10),
        "owed by the shard of the member it was placed with"
    );
    let batch = &a.handoff_batches[1];
    assert_eq!(batch.len(), 1);
    let targets: Vec<&ClientId> = batch[0]
        .targets
        .iter()
        .map(|t| match t {
            crate::hub::ShardTarget::Shared { client, .. } => client,
            other => panic!("a $share delivery: {other:?}"),
        })
        .collect();
    assert_eq!(
        targets,
        [&there],
        "the member that is there, not the one that left"
    );
}

/// A verdict on a peer reaches every shard (ADR 0093 Amendment 5): a publish on the shard
/// that does not hear from membership, owed to a peer then declared dead, is held there;
/// once membership confirms the peer gone, what it was owed is resolved on that shard and
/// its publisher answered.
#[tokio::test]
async fn a_publish_owed_to_a_peer_declared_dead_then_gone_is_held_then_resolved_on_its_shard() {
    let tx = start_shards(0);
    let mut to_peer = link_peer(&tx, 1, &["t/#"]);
    let a = client_on(1, "pub-", 0);
    let _ = attach_full(&tx, &a, 1, true, 0, 256).await;
    settle(&tx).await;
    drain(&mut to_peer);
    let mut done = publish_from(&tx, &a, "t/x", b"held");
    let seq = next_forward(&mut to_peer).await;
    assert_eq!(issuer(seq), 1);
    tx.send(HubCommand::PeerDead { node: peer() }).unwrap();
    settle(&tx).await;
    assert!(
        matches!(done.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "held for a peer declared dead, which may yet answer"
    );
    tx.send(HubCommand::PeerGone { node: peer() }).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), done)
            .await
            .expect("resolved within 5 s")
            .unwrap(),
        PublishOutcome::Accepted,
        "the peer is gone: its subscribers with it"
    );
}

/// Two `$share` members that left their shard in one routing gap are each tried once
/// (ADR 0093 Amendment 5): the second's answer carries who was tried, so the re-placement
/// does not hand the delivery back to the first, and with nobody left that can take it
/// the publish is withheld for its publisher to retry, not dealt round again.
#[tokio::test]
async fn two_members_gone_in_one_gap_are_each_tried_once() {
    let (mut a, mut b) = two_unrun_shards();
    let first = ClientId(client_on(1, "m-", 0).into());
    let second = ClientId(client_on(1, "m-", 1).into());
    // Shard 1's routing as last published: both online. Both have left since.
    b.shared
        .subscribe(first.clone(), "g", "t", QoS::AtLeastOnce, true);
    b.shared
        .subscribe(second.clone(), "g", "t", QoS::AtLeastOnce, true);
    b.routing_changed();
    b.publish_routing(true);
    a.refresh_foreign_routing();
    let key_hash = crate::hub::delivery::shared_cursor_hash("g", "t");
    a.shared_cursor
        .insert(key_hash, (("g".into(), "t".into()), 0));
    let (done, mut answered) = oneshot::channel();
    let id = a.register_pending(
        done,
        "t",
        &Bytes::from_static(b"twice"),
        QoS::AtLeastOnce,
        false,
        None,
        &mqtt_core::AppProperties::default(),
    );
    if let Some(p) = a.pending_publishes.get_mut(id) {
        p.local_done = true;
        p.shard_outstanding = 0b10;
    }
    let gone =
        |client: &ClientId, tried: Vec<(Option<NodeId>, ClientId)>| crate::hub::ShardAnswer {
            id,
            outcome: crate::hub::DurableOutcome::Ok,
            reached: false,
            gone: vec![crate::hub::GoneMember {
                key_hash,
                client: client.clone(),
                qos: QoS::AtLeastOnce,
                tried,
            }],
        };
    // The deal took `first`; it was gone.
    a.shard_done(1, gone(&first, Vec::new()));
    let placed = std::mem::take(&mut a.handoff_batches[1]);
    let (to, tried) = match &placed[0].targets[0] {
        crate::hub::ShardTarget::Shared { client, tried, .. } => (client.clone(), tried.clone()),
        other => panic!("a $share delivery: {other:?}"),
    };
    assert_eq!(to, second, "placed with the other member");
    assert!(
        tried.iter().any(|(_, c)| *c == first) && tried.iter().any(|(_, c)| *c == second),
        "and the hand-off says who was tried: {tried:?}"
    );
    // `second` was gone too: its answer carries that list back.
    a.shard_done(1, gone(&second, tried));
    assert!(
        a.handoff_batches[1].is_empty(),
        "not handed back to `first`: nobody left was tried twice"
    );
    assert!(
        matches!(
            answered.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ),
        "withheld: the group's routing still lists members, and its publisher retries"
    );
}
