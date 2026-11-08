//! A `$share` member whose append lane is full passes its turn on (issue #1219).
//!
//! A durable session's appends run in its own lane, at most
//! [`LANE_QUEUE_CAP`](crate::hub::LANE_QUEUE_CAP) jobs deep. While a node joins, the
//! sessions whose groups move to it stall in their appends — no quorum, or no longer
//! the owner — until they are re-homed, and their lanes fill. Every further delivery
//! the deal gave such a member was rejected at submit, counted
//! `publish_dropped{reason="append-backlog-full"}`, and failed its publish closed,
//! though its group had members with room: 5,918 in one burst on four brokers in the
//! 8 → 12 join of run `h3-20261106T194402Z`. The deal now checks the member's lane
//! before anything is enqueued, as it checked a `QoS` 0 member's outbound queue
//! (#482), and only a group with no member that can take the message fails it.
//!
//! A parked store call stands in for the stalled append: the member's lane worker
//! holds one job in it and the lane fills behind it, with nothing timed.

use super::*;
use crate::hub::LANE_QUEUE_CAP;

const GROUP_FILTER: &str = "$share/g/t";

/// A hub over `store`, writing every durable delivery (no elision window, so each one
/// takes a lane job), with `$share` locality as given and its metrics kept.
fn start(
    store: &Arc<ParkingStore>,
    prefer_local: bool,
) -> (HubTx, Arc<mqtt_observability::metrics::Metrics>) {
    let (mut hub, tx) = Hub::with_config(NodeId("lanes".into()), store.clone());
    hub.set_write_elision_ms(0);
    hub.set_shared_prefer_local(prefer_local);
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("lanes"));
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());
    (tx, metrics)
}

/// A persistent, connected member of the test's group at `QoS` 1.
async fn member(tx: &HubTx, name: &str) -> crate::hub::OutboundRx {
    let (rx, _) = attach(tx, name, 1, false).await;
    subscribe_qos(tx, name, GROUP_FILTER, QoS::AtLeastOnce);
    rx
}

fn payload(seq: usize) -> String {
    format!("m{seq:05}")
}

/// Publish `seq` to the group's topic at `qos`; a `QoS` ≥ 1 publish is gated and its
/// outcome returned.
fn publish(tx: &HubTx, seq: usize, qos: QoS) -> Option<oneshot::Receiver<PublishOutcome>> {
    let (done, outcome) = if qos == QoS::AtMostOnce {
        (None, None)
    } else {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    };
    tx.send(HubCommand::Publish {
        topic: "t".into(),
        payload: Bytes::from(payload(seq).into_bytes()),
        qos,
        retain: false,
        message_expiry: None,
        app: AppProperties::default(),
        done,
        v5: true,
        publisher: None,
        credit: None,
    })
    .unwrap();
    outcome
}

/// Publish `range` at `QoS` 1 and return each publish's outcome, in order.
fn publish_qos1(
    tx: &HubTx,
    range: std::ops::Range<usize>,
) -> Vec<oneshot::Receiver<PublishOutcome>> {
    range
        .map(|seq| publish(tx, seq, QoS::AtLeastOnce).expect("gated"))
        .collect()
}

/// Every outcome, awaited: `Some` with the answer, `None` for a publish whose ack was
/// withheld (its sender dropped unanswered).
async fn outcomes(dones: Vec<oneshot::Receiver<PublishOutcome>>) -> Vec<Option<PublishOutcome>> {
    let mut out = Vec::with_capacity(dones.len());
    for done in dones {
        out.push(
            timeout(Duration::from_secs(5), done)
                .await
                .expect("every publish is answered or withheld")
                .ok(),
        );
    }
    out
}

/// The payloads `rx` has been handed so far, in order.
fn received(rx: &mut crate::hub::OutboundRx) -> Vec<String> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|out| match out.into_packet() {
            Some(Packet::Publish(p)) => Some(String::from_utf8(p.payload.to_vec()).unwrap()),
            _ => None,
        })
        .collect()
}

/// The issue's shape: one member's appends stall, its lane fills, and the publishes
/// keep coming. Its turns pass to the member with room once its lane is full, so no
/// delivery is rejected and every publish is acknowledged — the stalled member's own
/// once its appends land, in order.
#[tokio::test]
async fn a_share_member_whose_append_lane_is_full_passes_its_turn_to_one_with_room() {
    let store = ParkingStore::new();
    let release = store.park("stuck");
    let (tx, metrics) = start(&store, true);
    let mut stuck = member(&tx, "stuck").await;
    let mut free = member(&tx, "free").await;

    // Half the turns are the stalled member's: more than its lane holds.
    let total = 2 * LANE_QUEUE_CAP + 100;
    let dones = publish_qos1(&tx, 0..total);
    ping(&tx).await;
    assert_eq!(
        dropped_for(&metrics, "append-backlog-full"),
        0,
        "no delivery was dealt to the full lane"
    );

    release.send(true).unwrap();
    for (seq, outcome) in outcomes(dones).await.into_iter().enumerate() {
        assert!(
            matches!(outcome, Some(PublishOutcome::Accepted)),
            "publish {seq} is acknowledged, not failed closed: {outcome:?}"
        );
    }
    ping(&tx).await;
    let to_stuck = received(&mut stuck);
    let to_free = received(&mut free);
    // Its lane's worker holds one job in the parked store call: the lane took its
    // cap of jobs behind it, or the cap alone if the worker had not run yet.
    assert!(
        [LANE_QUEUE_CAP, LANE_QUEUE_CAP + 1].contains(&to_stuck.len()),
        "the stalled member took its turns until its lane was full: {}",
        to_stuck.len()
    );
    let mut ordered = to_stuck.clone();
    ordered.sort();
    assert_eq!(to_stuck, ordered, "its lane kept publish order");
    let mut all: Vec<String> = to_stuck.into_iter().chain(to_free).collect();
    all.sort();
    assert_eq!(
        all,
        (0..total).map(payload).collect::<Vec<_>>(),
        "every message reached exactly one member"
    );
    assert_eq!(dropped_for(&metrics, "append-backlog-full"), 0);
}

/// With no member that can take it, a publish still fails closed, as before: the full
/// lane rejects the delivery, the rejection is counted, and the publisher's ack is
/// withheld so it sends again. Never acknowledged, and nothing else lost.
#[tokio::test]
async fn a_share_group_whose_every_member_lane_is_full_still_fails_the_publish_closed() {
    let store = ParkingStore::new();
    let release_a = store.park("a");
    let release_b = store.park("b");
    let (tx, metrics) = start(&store, true);
    let mut a = member(&tx, "a").await;
    let mut b = member(&tx, "b").await;

    let total = 2 * (LANE_QUEUE_CAP + 1) + 40;
    let dones = publish_qos1(&tx, 0..total);
    ping(&tx).await;
    let rejected = dropped_for(&metrics, "append-backlog-full");
    assert!(
        rejected > 0,
        "both lanes full: the overflow is rejected, and counted"
    );

    release_a.send(true).unwrap();
    release_b.send(true).unwrap();
    let outcomes = outcomes(dones).await;
    let accepted = outcomes
        .iter()
        .filter(|o| matches!(o, Some(PublishOutcome::Accepted)))
        .count();
    let withheld = outcomes.iter().filter(|o| o.is_none()).count();
    assert_eq!(accepted + withheld, total, "nothing refused: {outcomes:?}");
    assert_eq!(
        u64::try_from(withheld).unwrap(),
        rejected,
        "each rejected delivery withheld its publish's ack"
    );
    ping(&tx).await;
    assert_eq!(
        received(&mut a).len() + received(&mut b).len(),
        accepted,
        "exactly the acknowledged publishes were delivered"
    );
}

/// A `QoS` 0 delivery to a member with appends in flight rides its lane, to keep the
/// member's order, and a full lane sheds it. Its turn passes on as a durable one does.
#[tokio::test]
async fn a_qos0_share_delivery_passes_over_a_member_whose_append_lane_is_full() {
    let store = ParkingStore::new();
    let release = store.park("stuck");
    let (tx, metrics) = start(&store, true);
    let mut stuck = member(&tx, "stuck").await;
    let mut free = member(&tx, "free").await;

    // Fill the stalled member's lane with its durable turns.
    let durable = 2 * LANE_QUEUE_CAP + 20;
    let dones = publish_qos1(&tx, 0..durable);
    ping(&tx).await;
    let at_most_once = durable..durable + 40;
    for seq in at_most_once.clone() {
        assert!(publish(&tx, seq, QoS::AtMostOnce).is_none());
    }
    ping(&tx).await;
    assert_eq!(dropped_for(&metrics, "append-backlog-full"), 0);
    let qos0: Vec<String> = at_most_once.map(payload).collect();
    let to_free = received(&mut free);
    assert_eq!(
        to_free
            .iter()
            .filter(|p| qos0.contains(p))
            .cloned()
            .collect::<Vec<_>>(),
        qos0,
        "every QoS 0 message went to the member with room, in order"
    );

    release.send(true).unwrap();
    assert!(outcomes(dones)
        .await
        .iter()
        .all(|o| matches!(o, Some(PublishOutcome::Accepted))));
    ping(&tx).await;
    assert!(
        received(&mut stuck).iter().all(|p| !qos0.contains(p)),
        "nothing at QoS 0 was queued behind the stalled appends"
    );
}

/// A `$share` delivery a peer refuses is placed again with another member (0041-T12).
/// The re-selection passes over a member here whose lane is full, as a first placement
/// does, instead of handing it a delivery the lane rejects — which withheld a publish
/// the group's other member could take.
#[tokio::test]
async fn a_share_delivery_a_peer_refused_is_not_moved_onto_a_member_whose_lane_is_full() {
    let store = ParkingStore::new();
    let release = store.park("stuck");
    // Global rotation, so the deal reaches the peer's member as well as this node's.
    let (tx, metrics) = start(&store, false);
    let mut stuck = member(&tx, "stuck").await;
    let mut free = member(&tx, "free").await;

    // Fill the stalled member's lane before the peer is linked.
    let fill = 2 * LANE_QUEUE_CAP + 20;
    let fill_dones = publish_qos1(&tx, 0..fill);
    ping(&tx).await;

    let mut peer = connect_peer(&tx, "p", 1);
    remote_shared_interest_qos(&tx, "p", "g", "t", &[("r", QoS::AtLeastOnce)]);
    ping(&tx).await;
    let mut refused = 0;
    let mut dones = Vec::new();
    for seq in fill..fill + 30 {
        dones.push(publish(&tx, seq, QoS::AtLeastOnce).expect("gated"));
        ping(&tx).await;
        while let Ok(frame) = peer.try_recv() {
            if let PeerMessage::SharedDeliverAcked { seq, .. } = frame {
                tx.send(HubCommand::RemotePublishVerdict {
                    node: NodeId("p".into()),
                    seq,
                    verdict: ForwardVerdict::Refused {
                        code: PublishRefusal::Brownout.wire_code(),
                    },
                })
                .unwrap();
                refused += 1;
            }
        }
        ping(&tx).await;
    }
    assert!(
        refused >= 4,
        "fixture: the rotation reached the peer's member ({refused} times)"
    );
    for (i, outcome) in outcomes(dones).await.into_iter().enumerate() {
        assert!(
            matches!(outcome, Some(PublishOutcome::Accepted)),
            "publish {} placed again with the member that has room: {outcome:?}",
            fill + i
        );
    }
    assert_eq!(dropped_for(&metrics, "append-backlog-full"), 0);

    release.send(true).unwrap();
    assert!(outcomes(fill_dones)
        .await
        .iter()
        .all(|o| matches!(o, Some(PublishOutcome::Accepted))));
    ping(&tx).await;
    let late: Vec<String> = (fill..fill + 30).map(payload).collect();
    assert!(
        received(&mut stuck).iter().all(|p| !late.contains(p)),
        "nothing published after its lane filled was given to the stalled member"
    );
    assert_eq!(
        received(&mut free)
            .iter()
            .filter(|p| late.contains(p))
            .count(),
        late.len(),
        "the member with room took every later publish, refused ones included"
    );
}
