//! Packet-id blocks reserved ahead of need (ADR 0089 Amendment 4): a durable session
//! reserves its first block when it attaches, and its next one when the current block is
//! half spent, so a held delivery never meets a spent block. Reserved only once spent, a
//! session's first delivery and every one after it until the reservation landed were
//! written instead of held, which at a load step was every durable session at once.
//!
//! Every test here holds the store's appends (`hold_enqueue`): a delivery that took the
//! written path would never reach the wire, so arriving at all is the proof of a hold.

use super::*;

/// The store has answered `n` packet-id block reservations, and the hub has dispatched
/// every answer. The store counts a reservation and the task sends its answer in the same
/// poll, so once the count is seen the answer is ahead of the flush.
async fn reserved(store: &FlakyStore, tx: &HubTx, n: usize) {
    timeout(Duration::from_secs(2), async {
        while store.reserves.load(std::sync::atomic::Ordering::SeqCst) < n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the block reservation ran");
    flush(tx).await;
}

/// A durable session `c` on an eliding hub, subscribed to `t`, with its attach's block
/// reservation answered and every durable append held from here on.
async fn a_durable_session_with_appends_held(
) -> (std::sync::Arc<FlakyStore>, HubTx, crate::hub::OutboundRx) {
    let store = FlakyStore::new(0);
    let tx = start_hub_eliding(store.clone(), 60_000);
    let (rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
    subscribe_qos(&tx, "c", "t", QoS::AtLeastOnce);
    reserved(&store, &tx, 1).await;
    store
        .hold_enqueue
        .store(true, std::sync::atomic::Ordering::SeqCst);
    (store, tx, rx)
}

/// One `QoS` 1 publish on `t`, its delivery acknowledged, its publisher answered.
async fn deliver_and_ack(tx: &HubTx, rx: &mut crate::hub::OutboundRx, payload: &'static [u8]) {
    let done = publish_gated(tx, "t", payload, QoS::AtLeastOnce, false);
    let p = recv_packet(rx)
        .await
        .expect("held and sent at once: a written delivery would wait for the held append");
    assert_eq!(payload_of(&p), payload);
    pub_ack(tx, "c", pkid_of(&p));
    assert!(matches!(
        timeout(Duration::from_secs(2), done).await,
        Ok(Ok(PublishOutcome::Accepted))
    ));
}

/// The block the attach reserved is there for the session's first delivery: held, sent
/// at once, discharged by the PUBACK, and never written.
#[tokio::test]
async fn a_durable_sessions_first_delivery_is_held_not_written() {
    let (store, tx, mut rx) = a_durable_session_with_appends_held().await;
    deliver_and_ack(&tx, &mut rx, b"first").await;
    assert_eq!(
        store.appends.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "nothing was written"
    );
    assert!(
        store
            .pending(&ClientId("c".into()), 0, 16)
            .await
            .unwrap()
            .is_empty(),
        "no durable copy"
    );
    store
        .hold_enqueue
        .store(false, std::sync::atomic::Ordering::SeqCst);
}

/// A stream longer than one block: the next block is reserved once the current one is
/// half spent and is banked before it runs out, so no delivery past the first block's
/// last id waits for a reservation or is written.
#[tokio::test]
async fn the_next_block_is_banked_before_the_current_one_runs_out() {
    let (store, tx, mut rx) = a_durable_session_with_appends_held().await;
    for _ in 0..usize::from(crate::hub::PKID_BLOCK) + 8 {
        deliver_and_ack(&tx, &mut rx, b"m").await;
    }
    assert!(
        store.reserves.load(std::sync::atomic::Ordering::SeqCst) >= 2,
        "the second block was reserved ahead"
    );
    assert_eq!(
        store.appends.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no delivery was written"
    );
    store
        .hold_enqueue
        .store(false, std::sync::atomic::Ordering::SeqCst);
}

/// A clean session reserves nothing at its attach: it has no durable state for a block
/// to advance, and its first send reserves on the path it always took.
#[tokio::test]
async fn a_clean_session_reserves_nothing_at_its_attach() {
    let store = FlakyStore::new(0);
    let tx = start_hub_eliding(store.clone(), 60_000);
    let (mut rx, _) = attach(&tx, "clean", 1, true).await;
    subscribe_qos(&tx, "clean", "t", QoS::AtLeastOnce);
    flush(&tx).await;
    assert_eq!(
        store.reserves.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no reservation at a clean attach"
    );
    let done = publish_gated(&tx, "t", b"q1", QoS::AtLeastOnce, false);
    let p = recv_packet(&mut rx).await.expect("delivered");
    assert_eq!(payload_of(&p), b"q1");
    pub_ack(&tx, "clean", pkid_of(&p));
    assert!(matches!(
        timeout(Duration::from_secs(2), done).await,
        Ok(Ok(PublishOutcome::Accepted))
    ));
    assert!(
        store.reserves.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "its first send reserved the block"
    );
}
