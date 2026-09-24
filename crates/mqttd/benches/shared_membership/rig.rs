//! Receipt-gated, channel-only membership probe. No socket/capacity claims.
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_cluster::placement::{Placement, DEFAULT_REPLICAS};
use mqtt_cluster::swim::MemberState;
use mqtt_cluster::{peer::PeerMessage, NodeId};
use mqtt_codec::{packet::Publish, Packet, ProtocolVersion, QoS};
use mqtt_core::{AppProperties, ClientId};
use mqtt_storage::MemorySessionStore;
use mqttd::hub::{
    Admission, AttachOutcome, AuthMethod, Hub, Outbound, PublishOutcome, RemoteSharedGroup,
};
use mqttd::HubCommand;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub const BURST: usize = 2_000;
pub const WORKERS: usize = 4;
pub const TOPIC: &str = "site/0/turbine/7/rpm";
pub const GROUP: &str = "membership";
/// The gated arms' ZERO-MATCH topic: no local subscriber, no shared group, no
/// peer interest. `matched == 0` is what keeps `routing_unsettled()` on the
/// publish hot path under BOTH shapes of the settle gate — `register_pending`'s
/// `awaiting_settle`/`ack_awaits_settle` initializers, and item 2.1's ack
/// decision taken after the fan-out from `matched`. A gated burst on the shared
/// TOPIC could stop exercising that predicate if the gate ever moves wholly
/// behind the fan-out evidence, so this topic, not TOPIC, is the durable home
/// for items 1.1/1.2.
#[allow(dead_code)]
pub const UNROUTED_TOPIC: &str = "site/0/turbine/7/unrouted";
const DEADLINE: Duration = Duration::from_secs(10);
/// A clustered hub starts with `takeover_reconcile_ticks = 8` (hub/mod.rs:2411)
/// and burns one per `SESSION_SWEEP_INTERVAL` (1s), re-arming on a sweep that
/// sees the member set change. Settling is therefore ~9-10 wall seconds, well
/// past `DEADLINE`. This bound is a failure bound on the SETUP, never a latency
/// result.
#[allow(dead_code)]
const SETTLE_DEADLINE: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug)]
pub struct Shape {
    pub peers: usize,
    pub matching: bool,
    pub connected: bool,
}

impl Shape {
    pub fn arm(self) -> &'static str {
        match (self.matching, self.connected) {
            (false, false) => "A-miss-unlinked",
            (true, false) => "B-hit-unlinked",
            (false, true) => "C-miss-linked",
            (true, true) => "D-hit-linked",
        }
    }
}

// A checkpoint is requested ONLY after the Hub barrier. Everything it has sent
// is then already in these channels. Each drainer consumes any remaining backlog
// before returning its records. This is receipt, not just dispatch completion.
// No per-message cross-thread notification or shared receipt lock is needed.
struct Collector<T> {
    checkpoint: mpsc::UnboundedSender<oneshot::Sender<Vec<T>>>,
}

impl<T: Send + 'static> Collector<T> {
    fn spawn<M: Send + 'static>(
        drain: &Runtime,
        mut rx: mpsc::UnboundedReceiver<M>,
        mut record: impl FnMut(M) -> Option<T> + Send + 'static,
    ) -> (Self, JoinHandle<()>) {
        let (checkpoint, mut requests) = mpsc::unbounded_channel::<oneshot::Sender<Vec<T>>>();
        let task = drain.spawn(async move {
            let mut records = Vec::new();
            loop {
                tokio::select! {
                    message = rx.recv() => match message {
                        Some(message) => records.extend(record(message)),
                        None => break,
                    },
                    reply = requests.recv() => match reply {
                        Some(reply) => {
                            while let Ok(message) = rx.try_recv() {
                                records.extend(record(message));
                            }
                            let _ = reply.send(std::mem::take(&mut records));
                        }
                        None => break,
                    },
                }
            }
        });
        (Self { checkpoint }, task)
    }

    fn request(&self) -> oneshot::Receiver<Vec<T>> {
        let (reply, wait) = oneshot::channel();
        self.checkpoint.send(reply).expect("drainer ended");
        wait
    }
}

pub struct Rig {
    tx: mpsc::UnboundedSender<HubCommand>,
    workers: Vec<(Outbound, Collector<(u64, u64)>)>,
    peers: Vec<Collector<PeerMessage>>,
    tasks: Vec<JoinHandle<()>>,
    burst_id: u64,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Rig {
    pub async fn new(drain: &Runtime, shape: Shape) -> Self {
        tokio::time::timeout(DEADLINE, Self::setup(drain, shape, true))
            .await
            .expect("membership setup deadline")
    }

    /// Same remote-interest fixture, but real TCP connections attach the locals.
    pub async fn peer_only(drain: &Runtime, shape: Shape) -> Self {
        tokio::time::timeout(DEADLINE, Self::setup(drain, shape, false))
            .await
            .expect("peer setup deadline")
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<HubCommand> {
        self.tx.clone()
    }

    async fn setup(drain: &Runtime, shape: Shape, channel_workers: bool) -> Self {
        Self::setup_with(drain, shape, channel_workers, false).await
    }

    /// `clustered` installs a real [`Placement`] and marks the hub
    /// cluster-configured. Both are required before a gated publish can cost
    /// anything items 1.1/1.2 remove: `routing_unsettled()` (hub/mod.rs:5831)
    /// returns `false` before touching `peers_all` when `clustered()` is false,
    /// and `peers_all` (hub/mod.rs:5857) returns `true` before reading the lock
    /// when `placement` is `None`. Today's rig satisfies BOTH short circuits, so
    /// even a gated publish on it would execute neither.
    ///
    /// Membership follows the LINK, not `shape.peers`: a membership-alive peer
    /// with no live link leaves `mesh_settled()` false forever (hub/mod.rs:5846),
    /// which would hold every gated ack and turn the arm into a measurement of
    /// the hold path. So the A/B (unlinked) gated arms carry a one-member ring —
    /// the fixed `members()` cost — and the C/D (linked) arms carry the N-member
    /// slope. That is the contrast the gated matrix is FOR; it is not the same
    /// contrast the `QoS` 0 matrix draws.
    ///
    /// `pub` so the Zone TESTS correctness gate in
    /// `crates/mqttd/tests/shared_membership.rs` can build a clustered rig
    /// directly. `mod rig` is a private module there, so this widens nothing
    /// outside the fixture; a private `fn` would NOT have been reachable from
    /// that crate root, contrary to the spec's note.
    pub async fn setup_with(
        drain: &Runtime,
        shape: Shape,
        channel_workers: bool,
        clustered: bool,
    ) -> Self {
        let local = NodeId("membership-hot".into());
        let placement = clustered.then(|| {
            let mut p = Placement::new(local.clone(), DEFAULT_REPLICAS);
            if shape.connected {
                for n in 0..shape.peers {
                    p.observe(
                        &NodeId(format!("peer-{n}")),
                        MemberState::Alive,
                        &format!("peer-{n}:7000"),
                        None,
                    );
                }
            }
            Arc::new(std::sync::RwLock::new(p))
        });
        let (mut hub, tx) =
            Hub::with_config_and_placement(local, Arc::new(MemorySessionStore::new()), placement);
        if clustered {
            // A one-member ring is indistinguishable from standalone by
            // membership alone, so the A/B gated arms need this to be clustered
            // at all — and it keeps every gated arm on the same predicate.
            hub.set_cluster_configured();
        }
        hub.set_shared_prefer_local(true);
        let mut rig = Self {
            tx,
            workers: Vec::new(),
            peers: Vec::new(),
            tasks: vec![tokio::spawn(hub.run())],
            burst_id: 0,
        };
        if channel_workers {
            for i in 0..WORKERS {
                rig.attach(drain, i).await;
            }
        }
        for n in 0..shape.peers {
            let node = NodeId(format!("peer-{n}"));
            if shape.connected {
                let (tx, rx) = mpsc::unbounded_channel();
                // Control traffic is retained too, so setup can prove each link
                // was registered. Peer checkpoints stay OUTSIDE timed bursts:
                // per-peer diagnostic round trips would manufacture an O(N) tax.
                let (collector, task) = Collector::spawn(drain, rx, Some);
                rig.peers.push(collector);
                rig.tasks.push(task);
                rig.tx
                    .send(HubCommand::PeerConnected {
                        node: node.clone(),
                        conn_id: 1,
                        ctl: tx.clone(),
                        tx,
                        cert_serial: None,
                        proto: mqtt_cluster::peer::PROTO_MAX,
                        depth: Arc::new(AtomicUsize::new(0)),
                    })
                    .unwrap();
                rig.tx
                    .send(HubCommand::RemoteInterest {
                        node: node.clone(),
                        filters: vec![],
                    })
                    .unwrap();
            }
            rig.tx
                .send(HubCommand::RemoteSharedInterest {
                    node,
                    groups: (0..6)
                        .map(|g| RemoteSharedGroup {
                            group: if shape.matching && g == 0 {
                                GROUP.into()
                            } else {
                                format!("other-{n}-{g}")
                            },
                            filter: if shape.matching && g == 0 {
                                TOPIC.into()
                            } else {
                                format!("other/{n}/{g}/#")
                            },
                            members: vec![(
                                ClientId(format!("remote-{n}-{g}").into()),
                                QoS::AtMostOnce,
                                true,
                            )],
                        })
                        .collect(),
                })
                .unwrap();
        }
        rig.barrier().await;
        // The INTEREST snapshot is what proves the link registered — but only
        // once the hub is willing to gossip one. `peer_connected` sends it under
        // `if self.interest_authoritative`, and a CLUSTERED hub holds that false
        // until a complete scan lands over a WHOLE mesh (0043-P4 exhibit 2).
        // With a placement full of alive peers that cannot happen until every
        // `PeerConnected` has been dispatched AND a later sweep-tick scan lands,
        // which is seconds away. So on the clustered path this proof belongs
        // after the settle loop, and `gated` runs it there; asserting it here
        // demanded a frame the hub was still correctly suppressing.
        //
        // The QoS 0 path has `placement: None`, so `mesh_whole()` is trivially
        // true, the boot scan settles it in milliseconds, and the proof is
        // meaningful right here — which is why the QoS 0 arms never caught this.
        if !clustered {
            rig.verify_peer_registration().await;
        }
        rig
    }

    /// Every connected peer received this node's INTEREST snapshot, and nothing
    /// but control traffic. A shape whose `PeerConnected` silently failed to
    /// register would otherwise measure an arm it is not labelled as.
    #[allow(dead_code)]
    pub async fn verify_peer_registration(&self) {
        for peer in &self.peers {
            let frames = peer.request().await.unwrap();
            assert!(
                frames
                    .iter()
                    .any(|f| matches!(f, PeerMessage::Interest { .. })),
                "a connected peer never received the interest snapshot, so this                  arm's links are not proven registered"
            );
            assert_control_only(&frames);
        }
    }

    async fn attach(&mut self, drain: &Runtime, i: usize) {
        let name = format!("local-{i}");
        let (tx, rx) = mpsc::unbounded_channel::<Box<Packet>>();
        let (outbound, meter) = Outbound::new(tx);
        let (collector, task) = Collector::spawn(drain, rx, move |packet| {
            meter.drained(&packet);
            let Packet::Publish(p) = *packet else {
                panic!("unexpected local packet")
            };
            assert_eq!(p.qos, QoS::AtMostOnce);
            assert_eq!(p.topic, TOPIC);
            assert!(!p.retain && !p.dup);
            assert_eq!(p.payload.len(), 200);
            Some((
                u64::from_be_bytes(p.payload[..8].try_into().unwrap()),
                u64::from_be_bytes(p.payload[8..16].try_into().unwrap()),
            ))
        });
        self.tasks.push(task);
        self.workers.push((outbound.clone(), collector));
        let (reply, wait) = oneshot::channel();
        self.tx
            .send(HubCommand::Attach {
                client: ClientId(name.clone().into()),
                admission: Admission {
                    identity: mqtt_auth::Identity {
                        subject: name.clone(),
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
                filters: vec![(format!("$share/{GROUP}/{TOPIC}"), QoS::AtMostOnce)],
                no_local_filters: vec![],
                sub_id: None,
                rap_filters: vec![],
                retain_handling: vec![0],
                reply: Some(reply),
            })
            .unwrap();
        wait.await.unwrap();
    }

    pub async fn barrier(&self) {
        let (reply, wait) = oneshot::channel();
        self.tx.send(HubCommand::Ping { reply }).unwrap();
        wait.await.unwrap();
    }

    /// The gated twin of [`new`](Self::new): a CLUSTERED hub whose routing view
    /// has settled, so a gated publish runs the whole `routing_unsettled()`
    /// predicate instead of short-circuiting on a cheap term. The expensive term
    /// (`mesh_settled()`, and through it `peers_all` and `Placement::members()`)
    /// is the LAST `||` operand at hub/mod.rs:5831-5838 — it is reached only once
    /// `takeover_reconcile_ticks`, `inherited_scan_inflight`,
    /// `interest_authoritative` and `last_scan_complete` have all settled. An
    /// unsettled arm measures the short circuit and reports nothing about 1.1/1.2.
    #[allow(dead_code)]
    pub async fn gated(drain: &Runtime, shape: Shape) -> Self {
        let rig = tokio::time::timeout(SETTLE_DEADLINE, Self::setup_with(drain, shape, true, true))
            .await
            .expect("gated setup deadline");
        tokio::time::timeout(SETTLE_DEADLINE, rig.await_settled())
            .await
            .expect("routing view never settled; a gated burst would time the HOLD path");
        // Settled means the hub has gossiped, so the registration proof that
        // `setup_with` deliberately skipped on this path is now available.
        tokio::time::timeout(SETTLE_DEADLINE, async {
            rig.barrier().await;
            rig.verify_peer_registration().await;
        })
        .await
        .expect("peer registration deadline");
        rig
    }

    /// Drive the settle gate through the only observable it has. A gated publish
    /// that matches NOTHING is held exactly while `routing_unsettled()` is true.
    /// The first probe that is ANSWERED proves the view is settled; nothing in
    /// this fixture re-arms the window afterwards (no membership change, no
    /// ownership-epoch move, no peer death). A probe that times out is dropped,
    /// which the hub tolerates — the held entry is retired when the window
    /// closes and its answer goes to a receiver nobody is holding.
    ///
    /// `pub` for the same reason [`setup_with`](Self::setup_with) is: the Zone
    /// TESTS correctness gate drives it from the test crate root.
    #[allow(dead_code)]
    pub async fn await_settled(&self) {
        loop {
            let (done, wait) = oneshot::channel();
            self.publish_gated(UNROUTED_TOPIC, 0, 0, Some(done));
            if let Ok(Ok(PublishOutcome::Accepted)) =
                tokio::time::timeout(Duration::from_millis(1_500), wait).await
            {
                return;
            }
        }
    }

    /// One `QoS` 1 publish carrying its own ack gate. The publish `QoS` is what
    /// arms the gate; the local members' GRANT stays `QoS` 0, so
    /// `min_qos(publish, granted)` (delivery.rs:626) puts a byte-identical `QoS` 0
    /// packet on the wire. The receipt oracle, the `OutboundMeter` arithmetic and
    /// `validate_receipts` therefore apply to a gated burst UNCHANGED, and the
    /// only delta against a `QoS` 0 arm is the publisher-side gate itself.
    #[allow(dead_code)]
    fn publish_gated(
        &self,
        topic: &str,
        burst_id: u64,
        seq: usize,
        done: Option<oneshot::Sender<PublishOutcome>>,
    ) {
        let mut payload = [0; 200];
        payload[..8].copy_from_slice(&burst_id.to_be_bytes());
        payload[8..16].copy_from_slice(&u64::try_from(seq).unwrap().to_be_bytes());
        self.tx
            .send(HubCommand::Publish {
                topic: topic.into(),
                payload: Bytes::copy_from_slice(&payload),
                qos: QoS::AtLeastOnce,
                retain: false,
                message_expiry: None,
                app: AppProperties::default(),
                done,
                publisher: None,
                v5: false,
            })
            .unwrap();
    }

    /// One gated burst. THE FENCE IS THE ACK, NOT THE RECEIPT. `Ping` proves
    /// DISPATCH, and a gated publish's completion is a different event that can
    /// fall on either side of it: the ack is released from
    /// `try_complete_pending`, which a held `awaiting_settle`/`ack_awaits_settle`
    /// can delay past the barrier, while `deliver_to_client` returns `Ok` for a
    /// message merely queued. So the burst is over when all `BURST` `done`
    /// oneshots have answered `Accepted` — and only then are the receipt
    /// checkpoints requested, which is still sound because the barrier follows.
    ///
    /// `routed` publishes on the shared TOPIC (receipts apply unchanged);
    /// otherwise on [`UNROUTED_TOPIC`], where `matched == 0` keeps the settle
    /// decision on the hot path whichever side of item 2.1 the hub is on and the
    /// ack oracle is the ONLY fence.
    #[allow(dead_code)]
    pub async fn burst_gated(&mut self, routed: bool) {
        tokio::time::timeout(DEADLINE, self.burst_gated_inner(routed))
            .await
            .expect("gated burst failed to release every ack; not a throughput result");
    }

    async fn burst_gated_inner(&mut self, routed: bool) {
        self.burst_id = self.burst_id.checked_add(1).unwrap();
        let topic = if routed { TOPIC } else { UNROUTED_TOPIC };
        let mut acks = Vec::with_capacity(BURST);
        for seq in 0..BURST {
            let (done, wait) = oneshot::channel();
            self.publish_gated(topic, self.burst_id, seq, Some(done));
            acks.push(wait);
        }
        // In order: the hub completes them in order, and a dropped sender (the
        // WITHHOLD answer) must fail the arm rather than be read as a result.
        for wait in acks {
            assert!(
                matches!(wait.await, Ok(PublishOutcome::Accepted)),
                "a gated publish was refused or withheld; not a throughput result"
            );
        }
        self.barrier().await;
        let waits: Vec<_> = self.workers.iter().map(|(_, c)| c.request()).collect();
        let mut receipts = Vec::with_capacity(BURST);
        for wait in waits {
            let member = wait.await.expect("drainer panicked");
            if routed {
                assert_eq!(member.len(), BURST / WORKERS, "unbalanced shared selection");
            } else {
                assert!(member.is_empty(), "an unrouted topic reached a subscriber");
            }
            receipts.extend(member);
        }
        if routed {
            validate_receipts(self.burst_id, &receipts);
        }
        for (outbound, _) in &self.workers {
            assert_eq!(outbound.depth(), 0, "meter was not drained");
            assert_eq!(outbound.bytes(), 0, "byte meter was not drained");
        }
    }

    /// Includes construction, dispatch, metered receipts and the identity oracle.
    /// Direct bypass prices construction/draining/oracle overhead, not a broker.
    pub async fn burst(&mut self, direct: bool) {
        tokio::time::timeout(DEADLINE, self.burst_inner(direct))
            .await
            .expect("burst failed to reach channel consumers; not a throughput result");
    }

    async fn burst_inner(&mut self, direct: bool) {
        self.burst_id = self.burst_id.checked_add(1).unwrap();
        for seq in 0..BURST {
            let mut payload = [0; 200];
            payload[..8].copy_from_slice(&self.burst_id.to_be_bytes());
            payload[8..16].copy_from_slice(&u64::try_from(seq).unwrap().to_be_bytes());
            let payload = Bytes::copy_from_slice(&payload);
            if direct {
                assert!(self.workers[seq % WORKERS].0.send(Packet::Publish(Publish {
                    topic: TOPIC.into(),
                    payload,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    dup: false,
                    pkid: None,
                    properties: mqtt_codec::Properties::default(),
                })));
            } else {
                self.tx
                    .send(HubCommand::Publish {
                        topic: TOPIC.into(),
                        payload,
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
        self.barrier().await;
        let waits: Vec<_> = self.workers.iter().map(|(_, c)| c.request()).collect();
        let mut receipts = Vec::with_capacity(BURST);
        for wait in waits {
            let member = wait.await.expect("drainer panicked");
            assert_eq!(member.len(), BURST / WORKERS, "unbalanced shared selection");
            receipts.extend(member);
        }
        validate_receipts(self.burst_id, &receipts);
        for (outbound, _) in &self.workers {
            assert_eq!(outbound.depth(), 0, "meter was not drained");
            assert_eq!(outbound.bytes(), 0, "byte meter was not drained");
        }
    }

    /// Call outside the measured interval; a locally received message could also
    /// have been incorrectly forwarded, so local receipts alone are insufficient.
    pub async fn verify_peers(&self) {
        tokio::time::timeout(DEADLINE, async {
            self.barrier().await;
            for peer in &self.peers {
                assert_control_only(&peer.request().await.unwrap());
            }
        })
        .await
        .expect("peer verification deadline");
    }
}

pub fn validate_receipts(burst: u64, receipts: &[(u64, u64)]) {
    assert_eq!(receipts.len(), BURST, "missing or extra receipts");
    let mut seen = [false; BURST];
    for &(id, seq) in receipts {
        assert_eq!(id, burst, "receipt from another burst");
        let seq = usize::try_from(seq).unwrap();
        assert!(seq < BURST, "sequence out of range");
        assert!(
            !std::mem::replace(&mut seen[seq], true),
            "duplicate receipt"
        );
    }
}

pub fn assert_control_only(frames: &[PeerMessage]) {
    for frame in frames {
        // Fail closed if an unexpected new wire variant appears in this fixture.
        assert!(
            matches!(
                frame,
                PeerMessage::Interest { .. }
                    | PeerMessage::SharedInterest { .. }
                    | PeerMessage::RetainedDigest { .. }
            ),
            "unexpected peer frame in zero-forwarding arm: {frame:?}"
        );
    }
}
