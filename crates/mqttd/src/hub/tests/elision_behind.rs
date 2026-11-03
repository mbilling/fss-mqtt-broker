//! Write elision behind a write in flight (ADR 0089 Amendment 3): a `QoS` 1 delivery
//! whose session has a write in flight on the same topic waits for that write, unsent
//! and unwritten, and goes on the wire after it as a hold, instead of being written too.
//!
//! The write in flight is made the same way in every test: a `QoS` 2 delivery `one`
//! (always written) lands and goes out with its outbound record in flight, so `two`,
//! which waited behind it, is written at that landing (reason `behind`) and is in
//! flight for the store's write delay; the delivery under test then arrives on the
//! same topic, eligible but for `two`'s write.

use super::*;

/// A local `QoS` 1 publish whose outcome the caller awaits.
fn publish_qos1(
    tx: &HubTx,
    topic: &str,
    payload: &'static [u8],
) -> oneshot::Receiver<PublishOutcome> {
    let (done_tx, done_rx) = oneshot::channel();
    tx.send(HubCommand::Publish {
        topic: topic.into(),
        payload: Bytes::from_static(payload),
        qos: QoS::AtLeastOnce,
        retain: false,
        message_expiry: None,
        app: AppProperties::default(),
        done: Some(done_tx),
        v5: true,
        publisher: None,
        credit: None,
    })
    .unwrap();
    done_rx
}

/// The rendered counters starting with `name`, summed (a family's every label).
fn counter(metrics: &mqtt_observability::metrics::Metrics, name: &str) -> u64 {
    metrics
        .render()
        .lines()
        .filter(|l| l.starts_with(name))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

async fn accepted(on: oneshot::Receiver<PublishOutcome>) -> bool {
    matches!(
        timeout(Duration::from_secs(2), on).await,
        Ok(Ok(PublishOutcome::Accepted))
    )
}

async fn queued_payloads(store: &FlakyStore, client: &str) -> Vec<Vec<u8>> {
    store
        .pending(&ClientId(client.into()), 0, 16)
        .await
        .unwrap()
        .into_iter()
        .map(|q| q.message.payload.to_vec())
        .collect()
}

/// Every `QoS` 1 delivery on the wire, in order, each acknowledged as it arrives.
async fn drain_wire(tx: &HubTx, rx: &mut crate::hub::OutboundRx) -> Vec<Vec<u8>> {
    let mut wire = Vec::new();
    while let Some(p) = recv_packet(rx).await {
        if let Packet::Publish(p) = &p {
            if p.qos == QoS::AtLeastOnce {
                pub_ack(tx, "c", p.pkid.expect("a QoS 1 delivery"));
            }
            wire.push(p.payload.to_vec());
        }
    }
    wire
}

/// A hub eliding with a long window and metrics attached; a persistent subscriber `c`
/// on connection 1, subscribed to `t` at `QoS` 2 with its packet-id block warmed; a
/// store whose every write takes 150 ms; `one` (`QoS` 2) landed and on the wire,
/// `two` written at that landing (reason `behind`) and in flight. Returns `two`'s
/// publisher outcome with the rest.
async fn a_write_in_flight_on_t() -> (
    std::sync::Arc<FlakyStore>,
    Arc<mqtt_observability::metrics::Metrics>,
    HubTx,
    crate::hub::OutboundRx,
    oneshot::Receiver<PublishOutcome>,
) {
    let store = FlakyStore::new(0);
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("elision-behind"));
    let (mut hub, tx) = Hub::with_config(NodeId("hub-test".into()), store.clone());
    hub.set_write_elision_ms(60_000);
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());
    let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
    subscribe_qos(&tx, "c", "t", QoS::ExactlyOnce);
    let _ = attach(&tx, "barrier", 99, true).await;
    warm_packet_ids(&tx, &mut rx, "c").await;

    store
        .write_delay_ms
        .store(150, std::sync::atomic::Ordering::SeqCst);
    publish_qos2(&tx, "t", b"one");
    let on_two = publish_qos1(&tx, "t", b"two");
    let first = recv_packet(&mut rx)
        .await
        .expect("one's write lands and its delivery goes out");
    assert_eq!(payload_of(&first), b"one");
    assert_eq!(
        counter(&metrics, "mqttd_durable_elision_behind_total"),
        1,
        "two waited behind one's write"
    );
    assert_eq!(
        counter(
            &metrics,
            "mqttd_durable_elision_fallback_total{reason=\"behind\"}"
        ),
        1,
        "and was written at its landing, one's record being in flight"
    );
    // SETTLE(elision-behind-record): one's outbound record completes off-loop in
    // microseconds, while two's write takes the store's 150 ms; what is published
    // after this pause finds the record done and two's write in flight.
    tokio::time::sleep(Duration::from_millis(30)).await;
    (store, metrics, tx, rx, on_two)
}

/// `three` waits behind `two`'s write, unsent, and goes on the wire after `two` as a
/// hold the subscriber's PUBACK discharges: never written.
#[tokio::test]
async fn a_delivery_behind_a_write_in_flight_on_its_topic_waits_for_it_and_is_then_held() {
    let (store, metrics, tx, mut rx, on_two) = a_write_in_flight_on_t().await;
    let on_three = publish_qos1(&tx, "t", b"three");
    let _ = attach(&tx, "barrier2", 98, true).await;
    assert_eq!(counter(&metrics, "mqttd_durable_elision_behind_total"), 2);

    assert_eq!(
        drain_wire(&tx, &mut rx).await,
        vec![b"two".to_vec(), b"three".to_vec()],
        "two goes out at its landing, three after it"
    );
    assert!(accepted(on_two).await);
    assert!(accepted(on_three).await);
    assert_eq!(counter(&metrics, "mqttd_durable_elided_total"), 1);
    assert_eq!(
        counter(&metrics, "mqttd_durable_elision_fallback_total"),
        1,
        "only two was written after waiting"
    );
    assert_eq!(
        queued_payloads(&store, "c").await,
        vec![b"one".to_vec(), b"two".to_vec()],
        "three was never written (two's record waits for the truncation)"
    );
}

/// A later delivery on the topic that takes the durable path (here `QoS` 2) has what
/// waits written first, in order, so the store keeps the wire's order (reason `head`).
#[tokio::test]
async fn what_waits_behind_a_write_is_written_before_a_later_durable_delivery_on_its_topic() {
    let (store, metrics, tx, mut rx, on_two) = a_write_in_flight_on_t().await;
    let on_three = publish_qos1(&tx, "t", b"three");
    publish_qos2(&tx, "t", b"four");
    let _ = attach(&tx, "barrier2", 98, true).await;
    assert_eq!(counter(&metrics, "mqttd_durable_elision_behind_total"), 2);
    assert_eq!(
        counter(
            &metrics,
            "mqttd_durable_elision_fallback_total{reason=\"head\"}"
        ),
        1,
        "three was written ahead of four"
    );

    let mut wire = Vec::new();
    while let Some(p) = recv_packet(&mut rx).await {
        wire.push(payload_of(&p).to_vec());
    }
    assert_eq!(
        wire,
        vec![b"two".to_vec(), b"three".to_vec(), b"four".to_vec()],
        "the wire keeps the order"
    );
    assert!(accepted(on_two).await);
    assert!(accepted(on_three).await);
    assert_eq!(
        queued_payloads(&store, "c").await,
        vec![
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
            b"four".to_vec()
        ],
        "the store keeps the order"
    );
}

/// At the write's landing the session cannot take a hold (a `QoS` 2 delivery's outbound
/// record is in flight): the first waiting delivery is written instead (reason `behind`),
/// and the one after it waits again, behind that write, then goes out as a hold.
#[tokio::test]
async fn what_waits_behind_a_write_is_written_at_the_landing_when_its_session_cannot_take_it() {
    let store = FlakyStore::new(0);
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new(
        "elision-behind-landing",
    ));
    let (mut hub, tx) = Hub::with_config(NodeId("hub-test".into()), store.clone());
    hub.set_write_elision_ms(60_000);
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());
    let (mut rx, _) = attach_full(&tx, "c", 1, false, u32::MAX, 8).await;
    subscribe_qos(&tx, "c", "t", QoS::ExactlyOnce);
    let _ = attach(&tx, "barrier", 99, true).await;
    warm_packet_ids(&tx, &mut rx, "c").await;
    store
        .hold_enqueue
        .store(true, std::sync::atomic::Ordering::SeqCst);
    publish_qos2(&tx, "t", b"one");
    let on_two = publish_qos1(&tx, "t", b"two");
    let on_three = publish_qos1(&tx, "t", b"three");
    assert!(recv_packet(&mut rx).await.is_none());
    assert_eq!(counter(&metrics, "mqttd_durable_elision_behind_total"), 2);

    store
        .hold_enqueue
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        drain_wire(&tx, &mut rx).await,
        vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
        "the wire keeps the order"
    );
    assert!(accepted(on_two).await);
    assert!(accepted(on_three).await);
    assert_eq!(
        counter(
            &metrics,
            "mqttd_durable_elision_fallback_total{reason=\"behind\"}"
        ),
        1,
        "the first waiting delivery was written at the landing"
    );
    assert_eq!(
        counter(&metrics, "mqttd_durable_elided_total"),
        1,
        "the second waited again and was held"
    );
}

/// The session detaches while a delivery waits: it is written as an offline delivery
/// (reason `disconnect`) and the resume replays the topic in order.
#[tokio::test]
async fn what_waits_behind_a_write_is_written_for_the_replay_when_its_session_detaches() {
    let (_store, metrics, tx, _rx, on_two) = a_write_in_flight_on_t().await;
    let on_three = publish_qos1(&tx, "t", b"three");
    let _ = attach(&tx, "barrier2", 98, true).await;
    assert_eq!(counter(&metrics, "mqttd_durable_elision_behind_total"), 2);
    detach(&tx, "c", 1);
    assert!(accepted(on_two).await);
    assert!(accepted(on_three).await);
    assert_eq!(
        counter(
            &metrics,
            "mqttd_durable_elision_fallback_total{reason=\"disconnect\"}"
        ),
        1
    );

    let (mut rx, present) = attach_full(&tx, "c", 2, false, u32::MAX, 8).await;
    assert!(present);
    let mut replay = Vec::new();
    while let Some(p) = recv_packet(&mut rx).await {
        if let Packet::Publish(p) = p {
            if p.qos == QoS::AtLeastOnce {
                replay.push(p.payload.to_vec());
            }
        }
    }
    assert_eq!(
        replay,
        vec![b"two".to_vec(), b"three".to_vec()],
        "the replay keeps the order"
    );
}
