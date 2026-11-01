//! ADR 0092 T5: two data shards on one node, behind one [`HubTx`]. A publish entering
//! on one shard reaches the subscribers the other owns through a hand-off, and the
//! publisher's acknowledgement waits for the other shard's answer — through the durable
//! write and through the elision window alike — while the routing mirror keeps the
//! match current when a subscription goes.

use super::*;
use std::collections::BTreeSet;

/// Two shards of one node over one store, with `elision_ms` as the write-elision window.
fn start_shards(elision_ms: u32) -> HubTx {
    let store: Arc<dyn SessionStore> = Arc::new(MemorySessionStore::new());
    let mut hubs: Vec<Hub> = (0..2)
        .map(|_| Hub::with_config(NodeId("hub-test".into()), store.clone()).0)
        .collect();
    let txs: Vec<_> = hubs.iter().map(Hub::sender).collect();
    for (k, hub) in hubs.iter_mut().enumerate() {
        hub.set_shards(u8::try_from(k).unwrap(), txs.clone());
        hub.set_write_elision_ms(elision_ms);
    }
    for hub in hubs {
        tokio::spawn(hub.run());
    }
    HubTx::sharded(txs)
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

/// A `$share` group with a member on each shard: the dealing reaches both, every
/// publish is acknowledged, and under the elision window the acks are released by the
/// members' PUBACKs through the hand-off's answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_group_split_across_shards_takes_every_publish_under_the_window() {
    const MESSAGES: usize = 60;
    let tx = start_shards(50);
    let p = client_on(0, "pub-", 0);
    let members = [client_on(0, "con-", 0), client_on(1, "con-", 0)];
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
        "the dealing reached both shards: {} and {}",
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
