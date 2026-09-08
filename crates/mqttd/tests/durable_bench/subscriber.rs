//! Lane A's subscriber protocol/accounting, not just its publisher's `QoS` (#594).
use mqtt_codec::{Packet, QoS};
use std::collections::BTreeSet;

#[derive(Default)]
pub(super) struct DrainState {
    awaiting_release: BTreeSet<u16>,
    pub(super) delivered: usize,
}

impl DrainState {
    /// `QoS` 2 counts at release, once per handshake. A duplicate PUBREL still gets
    /// PUBCOMP; completing the handshake frees its ID for a later publication.
    pub(super) fn receive(&mut self, packet: &Packet) -> Option<Packet> {
        match packet {
            Packet::Publish(p) if p.qos == QoS::ExactlyOnce => {
                let pkid = p.pkid.expect("QoS 2 PUBLISH requires an ID");
                self.awaiting_release.insert(pkid);
                Some(Packet::PubRec(pkid.into()))
            }
            Packet::Publish(p) => {
                self.delivered += 1;
                p.pkid.map(|pkid| Packet::PubAck(pkid.into()))
            }
            Packet::PubRel(release) => {
                if self.awaiting_release.remove(&release.pkid) {
                    self.delivered += 1;
                }
                Some(Packet::PubComp(release.pkid.into()))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;
    use mqtt_codec::packet::Publish;
    use mqtt_storage::{MemorySessionStore, SessionStore};
    use std::{sync::Arc, time::Duration};

    fn publish(qos: QoS, dup: bool) -> Packet {
        Packet::Publish(Publish {
            dup,
            qos,
            retain: false,
            topic: "bench/protocol".into(),
            pkid: (qos != QoS::AtMostOnce).then_some(7),
            properties: vec![].into(),
            payload: b"unique".as_slice().into(),
        })
    }

    #[test]
    fn qos2_duplicates_do_not_count_twice_and_completed_ids_can_be_reused() {
        let mut state = DrainState::default();
        for dup in [false, true] {
            assert!(
                matches!(state.receive(&publish(QoS::ExactlyOnce, dup)), Some(Packet::PubRec(k)) if k.pkid == 7)
            );
            assert_eq!(state.delivered, 0);
        }
        for _ in 0..2 {
            assert!(
                matches!(state.receive(&Packet::PubRel(7.into())), Some(Packet::PubComp(k)) if k.pkid == 7)
            );
            assert_eq!(state.delivered, 1);
        }
        assert!(state.receive(&publish(QoS::ExactlyOnce, false)).is_some());
        assert!(state.receive(&Packet::PubRel(7.into())).is_some());
        assert_eq!(state.delivered, 2);
        assert!(state.awaiting_release.is_empty());
    }

    #[test]
    fn qos0_and_qos1_keep_their_own_acknowledgement_rules() {
        let mut state = DrainState::default();
        assert!(state.receive(&publish(QoS::AtMostOnce, false)).is_none());
        assert!(
            matches!(state.receive(&publish(QoS::AtLeastOnce, false)), Some(Packet::PubAck(k)) if k.pkid == 7)
        );
        assert_eq!(state.delivered, 2);
    }

    /// Real TCP/Hub/store control: subscription negotiation and both `QoS` 2 halves
    /// must run, and the original outbound ID must retire. No paid cluster needed.
    async fn real_broker_control(qos: QoS) {
        let store = Arc::new(MemorySessionStore::new());
        let (hub, tx) =
            mqttd::hub::Hub::with_config(mqtt_cluster::NodeId("bench-test".into()), store.clone());
        let hub = tokio::spawn(hub.run());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(mqttd::conn::handle(stream, tx.clone()));
            }
        });
        let (mut sub, _) = common::Client::connect_v311(addr, "bench-sub", false).await;
        let arm = crate::Arm {
            label: "test",
            qos,
            durable_subs: true,
            via_owner: true,
        };
        let ack = arm.subscribe(&mut sub, "bench/protocol").await;
        assert_eq!(ack.return_codes, vec![qos as u8]);
        let mut publisher = common::Client::connect(addr, "bench-pub").await;
        publisher
            .publish(
                "bench/protocol",
                b"unique",
                arm.qos,
                (qos != QoS::AtMostOnce).then_some(7),
                vec![],
            )
            .await;
        match qos {
            QoS::ExactlyOnce => {
                assert!(matches!(publisher.recv().await, Packet::PubRec(k) if k.pkid == 7));
                publisher.pubrel(7).await;
                assert!(matches!(publisher.recv().await, Packet::PubComp(k) if k.pkid == 7));
            }
            QoS::AtLeastOnce => {
                assert!(matches!(publisher.recv().await, Packet::PubAck(k) if k.pkid == 7));
            }
            QoS::AtMostOnce => {}
        }
        let client = mqtt_core::ClientId("bench-sub".into());
        if qos == QoS::ExactlyOnce {
            tokio::time::timeout(Duration::from_secs(5), async {
                while store.outbound(&client).await.unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("outbound ID must be stored before the subscriber acknowledges");
        }
        // Exercise the actual timed benchmark drainer, not a second implementation.
        let delivered =
            crate::drainer(sub, std::time::Instant::now() + Duration::from_secs(2), qos).await;
        assert_eq!(delivered, 1);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let ids = store.outbound(&client).await.unwrap();
                let queued = store.pending(&client, 0, 10).await.unwrap();
                if ids.is_empty() && queued.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscriber completion must retire the stored ID and queue prefix");
        assert!(store.pending(&client, 0, 10).await.unwrap().is_empty());
        publisher.disconnect().await;
        accept.abort();
        hub.abort();
    }

    #[tokio::test]
    async fn real_broker_qos0_control() {
        real_broker_control(QoS::AtMostOnce).await;
    }

    #[tokio::test]
    async fn real_broker_qos1_control() {
        real_broker_control(QoS::AtLeastOnce).await;
    }

    #[tokio::test]
    async fn real_broker_qos2_subscriber_completes_and_retires_the_outbound_id() {
        real_broker_control(QoS::ExactlyOnce).await;
    }
}
