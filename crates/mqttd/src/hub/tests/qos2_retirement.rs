//! #577: deterministic completion/recovery boundaries. These fixtures drive the
//! real Hub handlers without a running actor, so dropping the Hub really loses
//! its memory and cannot leave a background actor mutating the recovered store.
mod quorum;
use super::*;
use crate::hub::{OutState, PendingOut};
use mqtt_storage::{Offset, SessionStore, StorageError};

async fn delivery(hub: &mut Hub, client: &ClientId, pkid: u16, qos: QoS) -> Offset {
    hub.store.ensure_session(client).await.unwrap();
    let message = Message {
        topic: "retirement/t".into(),
        payload: Bytes::from(format!("message-{pkid}")),
        qos,
        retain: false,
        expires_at: None,
        app: mqtt_core::AppProperties::default(),
    };
    let offset = match hub.store.enqueue(client, &message).await.unwrap() {
        mqtt_storage::Enqueued::Stored { offset, .. } => offset,
        other @ mqtt_storage::Enqueued::Rejected => panic!("fixture must append: {other:?}"),
    };
    let state = if qos == QoS::ExactlyOnce {
        hub.store
            .record_outbound(client, pkid, offset)
            .await
            .unwrap();
        hub.store.advance_outbound(client, pkid).await.unwrap();
        OutState::AwaitingPubComp
    } else {
        OutState::AwaitingPubAck
    };
    let inf = hub.inflight.entry(client.clone()).or_default();
    inf.track(offset);
    inf.pending.insert(
        pkid,
        PendingOut {
            message,
            state,
            offset: Some(offset),
        },
    );
    offset
}

async fn assert_released_restore(
    store: std::sync::Arc<ParkingStore>,
    client: &ClientId,
    pkid: u16,
) {
    let tx = start_hub_with_arc(store.clone());
    let (mut rx, present) = attach(&tx, client.as_str(), 2, false).await;
    assert!(present, "the durable session must survive");
    let resumed = recv_packet(&mut rx)
        .await
        .expect("released ID resumes with PUBREL");
    assert!(
        matches!(&resumed, Packet::PubRel(k) if k.pkid == pkid),
        "never a fresh PUBLISH: {resumed:?}"
    );
    pub_comp(&tx, client.as_str(), pkid);
    let (reply, done) = oneshot::channel();
    tx.send(HubCommand::Ping { reply }).unwrap();
    timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store.outbound(client).await.unwrap().is_empty(),
        "the original ID retires after recovery"
    );
    assert!(
        store.pending(client, 0, 10).await.unwrap().is_empty(),
        "its queued delivery retires too"
    );
    assert!(
        rx.try_recv().is_err(),
        "recovery must not also publish under a fresh ID"
    );
}

fn fixture() -> (Hub, std::sync::Arc<ParkingStore>, ClientId) {
    let store = ParkingStore::new();
    let (hub, _) = Hub::with_config(NodeId("retirement-owner".into()), store.clone());
    (hub, store, ClientId("retirement-client".into()))
}

#[tokio::test]
async fn a_qos1_ack_retires_later_completed_qos2_ids_only_after_the_prefix_is_safe() {
    let (mut hub, store, client) = fixture();
    delivery(&mut hub, &client, 1, QoS::AtLeastOnce).await;
    hub.pub_ack(&client, 1).await; // QoS 1 submission hint advances, no flusher here.
    delivery(&mut hub, &client, 2, QoS::AtLeastOnce).await;
    delivery(&mut hub, &client, 3, QoS::ExactlyOnce).await;
    hub.pub_comp(&client, 3).await;
    assert_eq!(store.outbound(&client).await.unwrap()[0].packet_id, 3);
    assert_eq!(store.pending(&client, 0, 10).await.unwrap().len(), 3);
    hub.pub_ack(&client, 2).await;
    assert!(store.outbound(&client).await.unwrap().is_empty());
    assert!(store.pending(&client, 0, 10).await.unwrap().is_empty());
    assert!(!hub.qos2_cleanup.contains(&client));
}

#[tokio::test]
async fn a_failed_clear_reserves_the_id_and_retries_without_another_client_packet() {
    let (mut hub, store, client) = fixture();
    delivery(&mut hub, &client, 2, QoS::ExactlyOnce).await;
    store
        .clear_failures
        .lock()
        .unwrap()
        .push_back(StorageError::NoQuorum);
    hub.pub_comp(&client, 2).await;
    assert!(store.pending(&client, 0, 10).await.unwrap().is_empty());
    assert_eq!(store.outbound(&client).await.unwrap()[0].packet_id, 2);
    let inf = hub.inflight.get_mut(&client).unwrap();
    inf.receive_maximum = 1;
    assert!(
        inf.quota_full(),
        "deferred cleanup remains bounded by the session quota"
    );
    inf.next_pkid = 1;
    inf.block_remaining = 10;
    assert_eq!(
        hub.alloc_pkid(&client),
        Some(3),
        "do not reuse the still-durable ID"
    );
    hub.retry_qos2_cleanup().await; // the sweep's retry, not another PUBCOMP
    assert!(store.outbound(&client).await.unwrap().is_empty());
    assert!(!hub.inflight[&client].quota_full());
    assert!(!hub.qos2_cleanup.contains(&client));
}

#[tokio::test]
async fn a_failed_orphan_clear_retries_without_another_pubcomp() {
    let (mut hub, store, client) = fixture();
    let offset = delivery(&mut hub, &client, 2, QoS::ExactlyOnce).await;
    store.ack_durable(&client, offset).await.unwrap();
    // The restored form after a successful truncate but failed ID clearance.
    let inf = hub.inflight.get_mut(&client).unwrap();
    inf.pending.clear();
    inf.outstanding.clear();
    inf.orphaned_qos2.insert(2, offset);
    store
        .clear_failures
        .lock()
        .unwrap()
        .push_back(StorageError::NoQuorum);
    hub.pub_comp(&client, 2).await;
    assert_eq!(store.outbound(&client).await.unwrap().len(), 1);
    assert!(hub.inflight[&client].orphaned_qos2.contains_key(&2));
    hub.retry_qos2_cleanup().await;
    assert!(
        store.outbound(&client).await.unwrap().is_empty(),
        "the subscriber owes no further packet after PUBCOMP"
    );
    assert!(hub.inflight[&client].orphaned_qos2.is_empty());
    assert!(!hub.qos2_cleanup.contains(&client));
}

#[tokio::test]
async fn restored_qos2_offsets_pin_the_prefix_until_each_handshake_completes() {
    let (mut hub, store, client) = fixture();
    delivery(&mut hub, &client, 1, QoS::ExactlyOnce).await;
    delivery(&mut hub, &client, 2, QoS::ExactlyOnce).await;
    drop(hub);
    let tx = start_hub_with_arc(store.clone());
    let (mut rx, _) = attach(&tx, client.as_str(), 2, false).await;
    for id in [1, 2] {
        let packet = recv_packet(&mut rx).await.unwrap();
        assert!(
            matches!(&packet, Packet::PubRel(k) if k.pkid == id),
            "{packet:?}"
        );
    }
    pub_comp(&tx, client.as_str(), 2);
    let (reply, done) = oneshot::channel();
    tx.send(HubCommand::Ping { reply }).unwrap();
    timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.pending(&client, 0, 10).await.unwrap().len(),
        2,
        "restore must track the earlier offset rather than treating replay as acknowledgement"
    );
    assert_eq!(store.outbound(&client).await.unwrap().len(), 2);
    pub_comp(&tx, client.as_str(), 1);
    let (reply, done) = oneshot::channel();
    tx.send(HubCommand::Ping { reply }).unwrap();
    timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap();
    assert!(store.outbound(&client).await.unwrap().is_empty());
    assert!(store.pending(&client, 0, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn an_id_outside_the_replay_window_pins_the_prefix() {
    let (mut hub, store, client) = fixture();
    let offset = delivery(&mut hub, &client, 1, QoS::ExactlyOnce).await;
    store.record_outbound(&client, 1, offset).await.unwrap(); // still owes PUBLISH
    let inf = hub.inflight.get_mut(&client).unwrap();
    inf.pending.clear();
    inf.outstanding.clear();
    inf.orphaned_qos2.insert(1, offset); // unmatched in the bounded replay
    delivery(&mut hub, &client, 2, QoS::ExactlyOnce).await;
    hub.pub_comp(&client, 2).await;
    assert_eq!(
        store.pending(&client, 0, 10).await.unwrap().len(),
        2,
        "an unmatched ID may still own a queued message, not an orphan"
    );
    assert_eq!(store.outbound(&client).await.unwrap().len(), 2);
    let inf = hub.inflight.get_mut(&client).unwrap();
    inf.next_pkid = 0;
    inf.block_remaining = 10;
    assert_eq!(hub.alloc_pkid(&client), Some(3), "reserve both identities");
}

#[tokio::test]
async fn a_failed_outbound_snapshot_refuses_attach_instead_of_republishing() {
    let (mut hub, store, client) = fixture();
    delivery(&mut hub, &client, 7, QoS::ExactlyOnce).await;
    drop(hub);
    store
        .outbound_failures
        .lock()
        .unwrap()
        .push_back(StorageError::NoQuorum);
    let tx = start_hub_with_arc(store.clone());
    let outcome = timeout(
        Duration::from_secs(5),
        attach_outcome(&tx, client.as_str(), 2),
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome, AttachOutcome::Unavailable),
        "an unreadable ID table is not an empty ID table: {outcome:?}"
    );
    assert_eq!(store.outbound(&client).await.unwrap()[0].packet_id, 7);
    assert_eq!(store.pending(&client, 0, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn an_unreleased_orphan_is_cleared_without_republishing() {
    let (mut hub, store, client) = fixture();
    let offset = delivery(&mut hub, &client, 4, QoS::ExactlyOnce).await;
    store.record_outbound(&client, 4, offset).await.unwrap(); // unreleased phase
    store.ack_durable(&client, offset).await.unwrap();
    drop(hub);
    let tx = start_hub_with_arc(store.clone());
    let (mut rx, present) = attach(&tx, client.as_str(), 2, false).await;
    assert!(present);
    let (reply, done) = oneshot::channel();
    tx.send(HubCommand::Ping { reply }).unwrap();
    timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap();
    assert!(store.outbound(&client).await.unwrap().is_empty());
    assert!(
        rx.try_recv().is_err(),
        "an unreleased orphan has no message to publish"
    );
}

#[tokio::test]
async fn a_local_disk_restart_keeps_the_original_qos2_release_identity() {
    use mqtt_storage::{logged::ReplicatedSessionStore, persistent_log::PersistentLog};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sessions.redb");
    let client = ClientId("disk-retirement".into());
    {
        let store = ParkingStore::with_store(std::sync::Arc::new(ReplicatedSessionStore::new(
            PersistentLog::open(&path).unwrap(),
        )));
        let (mut hub, _) = Hub::with_config(NodeId("disk-owner".into()), store.clone());
        delivery(&mut hub, &client, 7, QoS::ExactlyOnce).await;
        store
            .clear_failures
            .lock()
            .unwrap()
            .push_back(StorageError::Unavailable("injected".into()));
        hub.pub_comp(&client, 7).await;
        assert!(store.pending(&client, 0, 10).await.unwrap().is_empty());
        assert_eq!(store.outbound(&client).await.unwrap()[0].packet_id, 7);
    } // close the database and lose all Hub/store state before reopening
    let reopened = ParkingStore::with_store(std::sync::Arc::new(ReplicatedSessionStore::new(
        PersistentLog::open(&path).unwrap(),
    )));
    assert_released_restore(reopened, &client, 7).await;
}

#[tokio::test]
async fn a_later_qos2_completion_keeps_its_id_behind_an_outstanding_prefix() {
    let (mut hub, store, client) = fixture();
    let earlier = delivery(&mut hub, &client, 1, QoS::AtLeastOnce).await;
    let later = delivery(&mut hub, &client, 2, QoS::ExactlyOnce).await;
    hub.pub_comp(&client, 2).await;
    assert_eq!(
        store
            .pending(&client, 0, 10)
            .await
            .unwrap()
            .iter()
            .map(|q| q.offset)
            .collect::<Vec<_>>(),
        vec![earlier, later]
    );
    assert_eq!(
        store.outbound(&client).await.unwrap().len(),
        1,
        "a pinned prefix is not durable retirement of the later completed delivery"
    );
    drop(hub);
    let tx = start_hub_with_arc(store);
    let (mut rx, present) = attach(&tx, client.as_str(), 2, false).await;
    assert!(present);
    let packets = [
        recv_packet(&mut rx).await.unwrap(),
        recv_packet(&mut rx).await.unwrap(),
    ];
    assert!(
        packets
            .iter()
            .any(|p| matches!(p, Packet::Publish(p) if p.payload.as_ref() == b"message-1")),
        "earlier delivery survives: {packets:?}"
    );
    assert!(
        packets
            .iter()
            .any(|p| matches!(p, Packet::PubRel(k) if k.pkid == 2)),
        "later delivery resumes release, never a fresh PUBLISH: {packets:?}"
    );
}

#[tokio::test]
async fn a_repeated_pubcomp_cannot_clear_an_id_after_failed_truncation() {
    let (mut hub, store, client) = fixture();
    let offset = delivery(&mut hub, &client, 2, QoS::ExactlyOnce).await;
    store
        .ack_failures
        .lock()
        .unwrap()
        .extend([StorageError::NotOwner, StorageError::NoQuorum]);
    hub.pub_comp(&client, 2).await;
    assert_eq!(store.outbound(&client).await.unwrap().len(), 1);
    hub.pub_comp(&client, 2).await;
    assert_eq!(
        store.pending(&client, 0, 10).await.unwrap()[0].offset,
        offset
    );
    assert_eq!(
        store.outbound(&client).await.unwrap().len(),
        1,
        "an absent in-memory pending entry is not proof of a durable orphan"
    );
}

#[tokio::test]
async fn a_failed_inline_truncate_retries_the_same_watermark() {
    let (mut hub, store, client) = fixture();
    let offset = delivery(&mut hub, &client, 2, QoS::ExactlyOnce).await;
    hub.inflight.get_mut(&client).unwrap().release(offset);
    store
        .ack_failures
        .lock()
        .unwrap()
        .push_back(StorageError::Unavailable("injected".into()));
    let _ = hub.truncate_acked_now(&client).await;
    assert_eq!(store.pending(&client, 0, 10).await.unwrap().len(), 1);
    let _ = hub.truncate_acked_now(&client).await;
    assert!(
        store.pending(&client, 0, 10).await.unwrap().is_empty(),
        "a failed write must not make the watermark look durably acknowledged"
    );
}
