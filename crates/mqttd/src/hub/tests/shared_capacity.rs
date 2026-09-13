//! `QoS` 0 shared admission under observable queue pressure (#482).
//! These are channel-boundary regressions, NOT downstream/cloud capacity evidence.
mod controls;

use super::*;
use crate::backpressure::ENTRY_OVERHEAD;
use crate::hub::OutboundMeter;

struct Rig {
    tx: HubTx,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Rig {
    fn new(prefer_local: bool, bytes: Option<usize>) -> Self {
        let (mut hub, tx) = Hub::with_config(
            NodeId("capacity".into()),
            Arc::new(MemorySessionStore::new()),
        );
        hub.set_shared_prefer_local(prefer_local);
        hub.subscriber_limits.max_outbound_bytes = bytes;
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("capacity"));
        hub.attach_metrics(metrics.clone());
        Self {
            tx,
            metrics,
            task: tokio::spawn(hub.run()),
        }
    }

    async fn member(&self, name: &str) -> (mpsc::UnboundedReceiver<Box<Packet>>, OutboundMeter) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (outbound, meter) = Outbound::new(tx);
        let (reply, wait) = oneshot::channel();
        self.tx
            .send(HubCommand::Attach {
                client: ClientId(name.into()),
                admission: admission(name),
                conn_id: 1,
                clean_start: true,
                session_expiry: 0,
                receive_maximum: u16::MAX,
                will: None,
                outbound,
                reply,
            })
            .unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap(),
            AttachOutcome::Present(false)
        ));
        subscribe(&self.tx, name, "$share/g/t");
        ping(&self.tx).await;
        (rx, meter)
    }

    fn publish(&self, sequence: u64) {
        self.tx
            .send(HubCommand::Publish {
                topic: "t".into(),
                payload: Bytes::copy_from_slice(&sequence.to_be_bytes()),
                qos: QoS::AtMostOnce,
                retain: false,
                message_expiry: None,
                app: AppProperties::default(),
                done: None,
                publisher: None,
                v5: false,
            })
            .unwrap();
    }
}

const MESSAGE_BYTES: usize = ENTRY_OVERHEAD + 1 + 8;

fn drain(rx: &mut mpsc::UnboundedReceiver<Box<Packet>>, meter: &OutboundMeter) -> Vec<u64> {
    let mut seen = Vec::new();
    while let Ok(packet) = rx.try_recv() {
        meter.drained(&packet);
        let Packet::Publish(p) = *packet else {
            panic!("unexpected packet")
        };
        assert_eq!(p.qos, QoS::AtMostOnce);
        seen.push(u64::from_be_bytes(p.payload[..].try_into().unwrap()));
    }
    seen
}

#[tokio::test]
async fn qos0_shared_uses_spare_local_capacity_at_both_outbound_bounds() {
    for prefer_local in [false, true] {
        for (bytes, fill) in [(None, MAX_OUTBOUND_QUEUE), (Some(MESSAGE_BYTES), 1)] {
            let rig = Rig::new(prefer_local, bytes);
            let (mut slow, slow_meter) = rig.member("slow").await;
            for seq in 0..fill {
                rig.publish(seq as u64);
            }
            ping(&rig.tx).await;
            let (mut fast, fast_meter) = rig.member("fast").await;
            let mut received = Vec::new();
            for seq in 20_000..20_012 {
                rig.publish(seq);
                ping(&rig.tx).await;
                received.extend(drain(&mut fast, &fast_meter));
            }
            assert_eq!(received, (20_000..20_012).collect::<Vec<_>>(),
                "a full but connected member must not waste spare local capacity; prefer_local={prefer_local}, bytes={bytes:?}");
            assert_eq!(
                drain(&mut slow, &slow_meter),
                (0..fill as u64).collect::<Vec<_>>()
            );
            assert_eq!(dropped_for(&rig.metrics, "outbound-full"), 0);
        }
    }
}

#[tokio::test]
async fn qos0_shared_pressure_can_escape_locality_and_rotates_remote_members() {
    let rig = Rig::new(true, Some(MESSAGE_BYTES));
    let (mut slow, slow_meter) = rig.member("slow").await;
    rig.publish(0);
    ping(&rig.tx).await;
    let mut peer = connect_peer(&rig.tx, "peer", 1);
    remote_shared_interest(&rig.tx, "peer", "g", "t", &["remote-a", "remote-b"]);
    ping(&rig.tx).await;
    for seq in 1..=12 {
        rig.publish(seq);
    }
    ping(&rig.tx).await;
    let mut sequences = Vec::new();
    let mut destinations = std::collections::BTreeMap::new();
    while let Ok(frame) = peer.try_recv() {
        if let PeerMessage::SharedDeliver {
            client, payload, ..
        } = frame
        {
            sequences.push(u64::from_be_bytes(payload[..].try_into().unwrap()));
            *destinations.entry(client).or_insert(0) += 1;
        }
    }
    assert_eq!(
        sequences,
        (1..=12).collect::<Vec<_>>(),
        "locality must not force a known-full local queue"
    );
    assert_eq!(
        destinations.values().copied().collect::<Vec<_>>(),
        vec![6, 6]
    );
    assert_eq!(drain(&mut slow, &slow_meter), vec![0]);
    assert_eq!(dropped_for(&rig.metrics, "outbound-full"), 0);
    // This asserts admission to the peer channel, not a remote socket/consumer receipt.
}

#[tokio::test]
async fn qos0_shared_all_full_drops_once_and_a_drained_member_rejoins() {
    let rig = Rig::new(true, Some(MESSAGE_BYTES));
    let (mut a, am) = rig.member("a").await;
    let (mut b, bm) = rig.member("b").await;
    for seq in 0..3 {
        rig.publish(seq);
    }
    ping(&rig.tx).await;
    assert_eq!(drain(&mut a, &am), vec![0]);
    assert_eq!(drain(&mut b, &bm), vec![1]);
    assert_eq!(dropped_for(&rig.metrics, "outbound-full"), 1);
    rig.publish(3);
    rig.publish(4);
    ping(&rig.tx).await;
    let mut recovered = drain(&mut a, &am);
    recovered.extend(drain(&mut b, &bm));
    recovered.sort_unstable();
    assert_eq!(recovered, vec![3, 4]);
    assert_eq!(dropped_for(&rig.metrics, "outbound-full"), 1);
}
