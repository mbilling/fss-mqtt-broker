//! `QoS` 0 shared dispatch AND metered channel receipts (#482).
//!
//! Unlike an enqueue-only benchmark, every burst must actually reach the drainers.
//! One arm leaves a connected member's queue full. This is a local Hub microbench,
//! NOT bridge/socket throughput or a claim of proportional cloud scaling.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mqtt_codec::{Packet, ProtocolVersion, QoS};
use mqtt_core::{AppProperties, ClientId};
use mqttd::hub::{Admission, AttachOutcome, AuthMethod, Hub, Outbound, MAX_OUTBOUND_QUEUE};
use mqttd::HubCommand;
use tokio::sync::{mpsc, oneshot, Notify};

const BURST: usize = 2_000;

struct Rig {
    tx: mpsc::UnboundedSender<HubCommand>,
    received: Arc<AtomicUsize>,
    changed: Arc<Notify>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    held: Vec<mpsc::UnboundedReceiver<Box<Packet>>>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Rig {
    async fn new(workers: usize, stalled: bool) -> Self {
        let (mut hub, tx) = Hub::new();
        hub.set_shared_prefer_local(true);
        let mut rig = Self {
            tx,
            received: Arc::new(AtomicUsize::new(0)),
            changed: Arc::new(Notify::new()),
            tasks: vec![tokio::spawn(hub.run())],
            held: Vec::new(),
        };
        if stalled {
            rig.attach("stalled", false).await;
            rig.publish(MAX_OUTBOUND_QUEUE);
            rig.barrier().await;
            assert_eq!(rig.held[0].len(), MAX_OUTBOUND_QUEUE);
        }
        for i in 0..workers {
            rig.attach(&format!("worker-{i}"), true).await;
        }
        rig
    }

    async fn attach(&mut self, name: &str, draining: bool) {
        let (tx, mut rx) = mpsc::unbounded_channel::<Box<Packet>>();
        let (outbound, meter) = Outbound::new(tx);
        if draining {
            let received = self.received.clone();
            let changed = self.changed.clone();
            self.tasks.push(tokio::spawn(async move {
                while let Some(packet) = rx.recv().await {
                    meter.drained(&packet);
                    assert!(matches!(*packet, Packet::Publish(ref p) if p.qos == QoS::AtMostOnce));
                    received.fetch_add(1, Ordering::Relaxed);
                    changed.notify_one();
                }
            }));
        } else {
            self.held.push(rx);
        }
        let (reply, wait) = oneshot::channel();
        self.tx
            .send(HubCommand::Attach {
                client: ClientId(name.into()),
                admission: Admission {
                    identity: mqtt_auth::Identity {
                        subject: name.into(),
                        groups: vec![],
                    },
                    method: AuthMethod::Password,
                    cert_serial: None,
                    protocol: ProtocolVersion::V311,
                },
                conn_id: 1,
                clean_start: true,
                session_expiry: 0,
                receive_maximum: u16::MAX,
                will: None,
                outbound,
                reply,
            })
            .unwrap();
        assert!(matches!(wait.await.unwrap(), AttachOutcome::Present(false)));
        let (reply, wait) = oneshot::channel();
        self.tx
            .send(HubCommand::Subscribe {
                client: ClientId(name.into()),
                filters: vec![("$share/capacity/t".into(), QoS::AtMostOnce)],
                no_local_filters: vec![],
                sub_id: None,
                rap_filters: vec![],
                retain_handling: vec![0],
                reply: Some(reply),
            })
            .unwrap();
        wait.await.unwrap();
    }

    fn publish(&self, count: usize) {
        let payload = Bytes::from_static(&[0; 200]);
        for _ in 0..count {
            self.tx
                .send(HubCommand::Publish {
                    topic: "t".into(),
                    payload: payload.clone(),
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

    async fn barrier(&self) {
        let (reply, wait) = oneshot::channel();
        self.tx.send(HubCommand::Ping { reply }).unwrap();
        tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .unwrap()
            .unwrap();
    }

    async fn burst(&self) {
        let target = self.received.load(Ordering::Relaxed) + BURST;
        self.publish(BURST);
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.received.load(Ordering::Relaxed) < target {
                self.changed.notified().await;
            }
        })
        .await
        .expect("burst was shed or never drained; not a throughput result");
        self.barrier().await;
        assert_eq!(self.received.load(Ordering::Relaxed), target);
    }
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("qos0_shared_receipts");
    group.throughput(Throughput::Elements(BURST as u64));
    for (workers, stalled) in [(1, false), (4, false), (64, false), (4, true)] {
        let rig = rt.block_on(Rig::new(workers, stalled));
        group.bench_with_input(
            BenchmarkId::new(if stalled { "one-full" } else { "all-ready" }, workers),
            &rig,
            |b, rig| b.to_async(&rt).iter(|| rig.burst()),
        );
    }
    group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
