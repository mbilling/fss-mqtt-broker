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
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(read().as_deref(), Some("mqttd_sessions 2"));
    }
}
