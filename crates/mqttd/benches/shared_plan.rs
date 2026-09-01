//! What a cluster costs the publish path when nothing crosses the network.
//!
//! ADR 0077 T4 measured tenant capacity growing as `sites = N + 2` rather than
//! proportionally, and the obvious explanation — `$share` round-robin sending
//! (N-1)/N of publishes across the cluster bus — was tested on the rig and
//! **refused**: pinning every site to one broker, so no message for it ever left
//! that node, moved the knee from 5 sites to at most 6 rather than to the
//! predicted 9. At 9 sites pinned each broker carried exactly the load a
//! STANDALONE node serves at p99 <=1000ms, and collapsed to >30s and 84%
//! delivery. Something about cluster membership costs per-node throughput with
//! no forwarding involved at all.
//!
//! Reading `Hub::plan_shared` names two candidates, both paid per publish and
//! both absent at N=1:
//!
//! 1. `self.remote_shared.iter().collect::<BTreeMap<_, _>>()` — an allocation
//!    and tree build on EVERY publish, purely to iterate nodes in a stable
//!    order.
//! 2. remote groups are matched by a linear scan calling `topic_matches` per
//!    group, where the local side uses an index (`shared.matching_refs`).
//!
//! `deliver_shared` runs unconditionally for every locally-originated publish
//! (`hub/mod.rs`), with no guard on whether any shared subscription exists — so
//! both costs are paid by a node that has peers even when it has no shared
//! subscribers of its own and delivers nothing remotely. That is what this
//! benchmark isolates: identical publishes, identical local state, varying only
//! the number of peers whose shared interest this node happens to know about.
//!
//! Static analysis said these were the right SHAPE; it cannot say the magnitude,
//! and on this workload the obvious mechanism has already been wrong twice. Hence
//! a measurement.

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mqtt_cluster::NodeId;
use mqtt_codec::QoS;
use mqtt_core::{AppProperties, ClientId};
use mqtt_storage::MemorySessionStore;
use mqtt_codec::{Packet, ProtocolVersion};
use mqttd::hub::{Admission, AuthMethod, Hub, Outbound, RemoteSharedGroup};
use mqttd::HubCommand;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// One peer's shared interest. The filters deliberately do NOT match the topic
/// published below: this measures the cost of a peer being KNOWN, not the cost of
/// delivering to it, which is the case the pinned rig run isolated.
fn peer_groups(node: usize, groups_per_node: usize) -> Vec<RemoteSharedGroup> {
    (0..groups_per_node)
        .map(|g| RemoteSharedGroup {
            group: format!("g{node}-{g}"),
            filter: format!("other/{node}/{g}/#"),
            members: vec![(
                ClientId(Arc::from(format!("peer{node}-member{g}").as_str())),
                QoS::AtMostOnce,
                true,
            )],
        })
        .collect()
}

fn hub_with_peers(
    rt: &tokio::runtime::Runtime,
    peers: usize,
    groups_per_node: usize,
) -> mpsc::UnboundedSender<HubCommand> {
    let (hub, tx) = Hub::with_config(NodeId("bench".into()), Arc::new(MemorySessionStore::new()));
    rt.spawn(hub.run());
    for n in 0..peers {
        let _ = tx.send(HubCommand::RemoteSharedInterest {
            node: NodeId(format!("peer{n}")),
            groups: peer_groups(n, groups_per_node),
        });
    }
    tx
}

/// The topic every arm publishes to. Subscribers below match it EXACTLY, so each
/// one is a real recipient: the hub must plan it, enqueue it, and wake its conn.
const TOPIC: &str = "site/0/turbine/7/rpm";

fn admission(subject: &str) -> Admission {
    Admission {
        identity: mqtt_auth::Identity {
            subject: subject.to_string(),
            groups: vec![],
        },
        method: AuthMethod::Password,
        cert_serial: None,
        protocol: ProtocolVersion::V311,
    }
}

/// A hub with `subs` attached, online, QoS 0 subscribers of [`TOPIC`] — the
/// dimension the peer arms above hold at ZERO. Their filters miss on purpose, so
/// those arms price a publish that reaches nobody. Lane E's shape is 6 consumers
/// per site; the cost of actually handing a message to R recipients — the plan,
/// the per-recipient enqueue, the per-recipient wake — was never on the bench.
///
/// Each subscriber's outbound queue is drained on `drain`, a SEPARATE multi-thread
/// runtime, so the measured current-thread runtime runs only the hub and the
/// publisher loop, exactly as the peer arms do. QoS 0 keeps inflight/pkid/durable
/// machinery out: this isolates dispatch + hand-off, nothing else.
fn hub_with_subscribers(
    rt: &tokio::runtime::Runtime,
    drain: &tokio::runtime::Runtime,
    subs: usize,
) -> mpsc::UnboundedSender<HubCommand> {
    let (hub, tx) = Hub::with_config(NodeId("bench".into()), Arc::new(MemorySessionStore::new()));
    rt.spawn(hub.run());
    rt.block_on(async {
        for i in 0..subs {
            let client = ClientId(Arc::from(format!("sub{i}").as_str()));
            let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Packet>();
            drain.spawn(async move { while out_rx.recv().await.is_some() {} });
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(HubCommand::Attach {
                client: client.clone(),
                admission: admission(&format!("sub{i}")),
                conn_id: i as u64 + 1,
                clean_start: true,
                session_expiry: 0,
                receive_maximum: u16::MAX,
                will: None,
                outbound: Outbound::new(out_tx).0,
                reply: reply_tx,
            })
            .expect("hub alive");
            let _ = reply_rx.await;
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(HubCommand::Subscribe {
                client,
                filters: vec![(TOPIC.to_string(), QoS::AtMostOnce)],
                no_local_filters: vec![],
                sub_id: None,
                rap_filters: vec![],
                retain_handling: vec![0],
                reply: Some(reply_tx),
            })
            .expect("hub alive");
            let _ = reply_rx.await;
        }
    });
    tx
}

/// Publish `count` messages and wait only for the LAST to complete. The hub
/// processes its queue in order, so the final completion means every earlier one
/// was dispatched too — this measures dispatch throughput rather than per-message
/// round-trip latency.
async fn publish_burst(tx: &mpsc::UnboundedSender<HubCommand>, count: usize) {
    let payload = Bytes::from_static(&[0u8; 200]);
    for i in 0..count {
        let last = i + 1 == count;
        let (done_tx, done_rx) = oneshot::channel();
        let _ = tx.send(HubCommand::Publish {
            topic: TOPIC.to_string(),
            payload: payload.clone(),
            qos: QoS::AtMostOnce,
            retain: false,
            message_expiry: None,
            app: AppProperties::default(),
            done: Some(done_tx),
            publisher: None,
            v5: false,
        });
        if last {
            let _ = done_rx.await;
        }
    }
}

const BURST: usize = 2_000;
/// lane E: one `$share` group of 6 consumers per site.
const GROUPS_PER_NODE: usize = 6;

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let mut g = c.benchmark_group("publish_dispatch_vs_known_peers");
    g.throughput(Throughput::Elements(BURST as u64));
    // 0 peers is the standalone baseline the rig compared against; 2 and 4 are the
    // peer counts of the 3- and 5-node clusters lane E measured.
    for peers in [0usize, 2, 4, 9] {
        let tx = hub_with_peers(&rt, peers, GROUPS_PER_NODE);
        g.bench_with_input(BenchmarkId::from_parameter(peers), &peers, |b, _| {
            b.to_async(&rt).iter(|| publish_burst(&tx, BURST));
        });
    }
    g.finish();

    // Which of the two candidates dominates? Hold the peer count fixed and vary
    // how many shared groups each peer announces:
    //
    //   the per-publish `collect::<BTreeMap>()` is O(peers)          -> FLAT here
    //   the linear `topic_matches` scan is O(peers x groups_per_peer) -> RISES here
    //
    // One dimension separates a one-line type change from indexing the remote
    // side, so it is worth its own group.
    let mut g = c.benchmark_group("publish_dispatch_vs_groups_per_peer");
    g.throughput(Throughput::Elements(BURST as u64));
    for groups in [1usize, 6, 24] {
        let tx = hub_with_peers(&rt, 4, groups);
        g.bench_with_input(BenchmarkId::from_parameter(groups), &groups, |b, _| {
            b.to_async(&rt).iter(|| publish_burst(&tx, BURST));
        });
    }
    g.finish();

    // The dimension every arm above holds at zero. 0 is the same publish-to-nobody
    // baseline; 1 prices the first real hand-off; 6 is lane E's consumers per site;
    // 60 is the fan-out at which per-recipient cost, not matching, owns the hub.
    let drain = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("drain runtime");
    let mut g = c.benchmark_group("publish_dispatch_vs_local_recipients");
    g.throughput(Throughput::Elements(BURST as u64));
    for subs in [0usize, 1, 6, 60] {
        let tx = hub_with_subscribers(&rt, &drain, subs);
        g.bench_with_input(BenchmarkId::from_parameter(subs), &subs, |b, _| {
            b.to_async(&rt).iter(|| publish_burst(&tx, BURST));
        });
    }
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
