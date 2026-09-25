//! Wave B's per-publish work-elimination proofs, issue #613 items 1.1–1.5.
//!
//! Every item in this wave is of the shape "work X stops happening once per
//! message". None of it has a behavioural observable — the same clients receive
//! the same bytes either way — so the only honest test is a COUNT of how many
//! times X ran. These read the `#[cfg(test)]` probe counters
//! ([`crate::hub::probe::HubProbe`] and `forwarding::PEER_FANOUT_VISITS`), never
//! a clock. Issue #613 CORRECTION 5 is why they are call counters and not
//! allocation counters: the workspace sets `unsafe_code = "forbid"`, so there can
//! be no counting `GlobalAlloc`.
//!
//! `mqtt_cluster`'s own `members_probe` is deliberately NOT among them: it is
//! `#[cfg(test)]` inside THAT crate, so this test target links the ordinary
//! probe-free build and cannot see it. See
//! [`membership_enumeration_borrows_the_member_set_under_the_read_lock`] for what
//! this side can prove about item 1.2 instead.
//!
//! **Why this file is in-crate and not under `crates/mqttd/tests/`.** The probe
//! counters, `Hub::peers_all`, `Hub::routing_unsettled`, `Hub::mesh_settled` and
//! the four `routing_unsettled` term fields are all crate-private or
//! `#[cfg(test)]`. An integration test links the library built WITHOUT
//! `cfg(test)` and can see none of them. The precedent copied here is
//! `hub::tests::remote_group_index`: a `Hub` built in the test body and driven
//! with `hub.dispatch(..).await` inline, with no `tokio::spawn` and no `run()`
//! loop — which is also what keeps the thread-local counters exact under
//! `cargo test`'s parallel test threads.
//!
//! **THE TRAP THIS FILE EXISTS TO AVOID.** `routing_unsettled()` is
//!
//! ```text
//! clustered() && (takeover_reconcile_ticks > 0 || inherited_scan_inflight
//!                 || !interest_authoritative || !last_scan_complete || !mesh_settled())
//! ```
//!
//! — a short-circuiting `||` with `mesh_settled()` LAST. On a freshly built hub
//! `takeover_reconcile_ticks` is 8, so the chain answers `true` at term 1 and
//! `peers_all` is never reached at all. A "zero enumerations" assertion on such a
//! hub passes on the pre-fix code and certifies nothing. [`Mesh`] therefore drives
//! the routing view to genuinely settled and [`Mesh::assert_settled`] asserts each
//! of the four earlier terms INDIVIDUALLY, before and after every measurement
//! window, so the file can never silently degrade back into measuring the short
//! circuit.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Peers in the default fixture. Large enough that a per-message enumeration is
/// unmistakable, small enough to stay instant.
const PEERS: usize = 8;
/// Messages per measurement window. The claim is that the counted work is
/// CONSTANT in this number, so any per-message cost shows up multiplied by it.
const MESSAGES: usize = 64;

/// A snapshot of one hub's work counters, so assertions read DELTAS over a
/// measurement window rather than absolutes. Absolutes drift the moment another
/// wave adds a counted call site; a delta over a bounded window does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Counts {
    /// Times [`Hub::peers_all`] walked the member set (items 1.1 and 1.2).
    peers_all: usize,
    /// Times shared planning took a scratch collection for its `decided` set
    /// (item 1.5).
    shared_scratch: usize,
    /// Peers the fan-out loop examined, on this thread (item 1.3).
    peer_loop: usize,
}

impl Counts {
    fn now(probe: &crate::hub::probe::HubProbe) -> Self {
        Self {
            peers_all: probe.peers_all_evals.load(Ordering::Relaxed),
            shared_scratch: probe.shared_plan_scratch_uses.load(Ordering::Relaxed),
            peer_loop: crate::hub::forwarding::PEER_FANOUT_VISITS.with(std::cell::Cell::get),
        }
    }

    fn since(self, before: Self) -> Self {
        Self {
            peers_all: self.peers_all - before.peers_all,
            shared_scratch: self.shared_scratch - before.shared_scratch,
            peer_loop: self.peer_loop - before.peer_loop,
        }
    }
}

/// A clustered hub whose routing view is GENUINELY settled, with `n` peer links
/// up and interest-synced, every one of them advertising exactly `filters`.
struct Mesh {
    hub: Hub,
    placement: Arc<RwLock<Placement>>,
    /// Kept alive on purpose: dropping a peer's receiver closes its channel, and
    /// `peer.tx.send` then fails silently — which would hide the control
    /// assertions that check a frame actually arrived.
    peers: Vec<(NodeId, mpsc::UnboundedReceiver<PeerMessage>)>,
    _ctl: Vec<mpsc::UnboundedReceiver<PeerMessage>>,
    probe: std::sync::Arc<crate::hub::probe::HubProbe>,
    /// Kept so the hub's own command channel never reports "closed".
    _tx: mpsc::UnboundedSender<HubCommand>,
}

impl Mesh {
    async fn settled(n: usize, filters: &[&str]) -> Self {
        let local = NodeId("mesh-local".into());
        let mut p = Placement::new(local.clone(), DEFAULT_REPLICAS);
        for i in 0..n {
            p.observe(
                &NodeId(format!("peer-{i}")),
                MemberState::Alive,
                &format!("peer-{i}:7000"),
                None,
            );
        }
        let placement = Arc::new(RwLock::new(p));
        let (mut hub, tx) = Hub::with_config_and_placement(
            local,
            Arc::new(MemorySessionStore::new()),
            Some(placement.clone()),
        );
        let probe = hub.probe();
        // `clustered()` is the outer `&&` of `routing_unsettled`: without it the
        // whole predicate is false for a reason that has nothing to do with the
        // mesh, and every assertion below would be vacuous.
        hub.set_cluster_configured();

        let mut peers = Vec::new();
        let mut ctls = Vec::new();
        for i in 0..n {
            let node = NodeId(format!("peer-{i}"));
            let (ptx, prx) = mpsc::unbounded_channel();
            let (ctl, ctl_rx) = mpsc::unbounded_channel();
            // Mesh transition 1 of 5 — a real link event, not a field poke.
            hub.dispatch(HubCommand::PeerConnected {
                node: node.clone(),
                conn_id: i as u64,
                tx: ptx,
                ctl,
                cert_serial: None,
                proto: crate::hub::PROTO_FORWARD_VERDICT,
                depth: Arc::new(AtomicUsize::new(0)),
            })
            .await;
            // Mesh transition 4 of 5 — the only place `interest_synced` flips,
            // and therefore the only way `mesh_settled` can become true.
            hub.dispatch(HubCommand::RemoteInterest {
                node: node.clone(),
                filters: filters.iter().map(|f| (*f).to_string()).collect(),
            })
            .await;
            peers.push((node, prx));
            ctls.push(ctl_rx);
        }

        // The takeover window. `sweep_expired_sessions` decrements this once per
        // tick, but each of those ticks also SPAWNS an inherited-session scan,
        // which would put the work being measured on another task. Set to the
        // value eight sweeps reach, which is the steady state every long-lived
        // hub is in.
        hub.takeover_reconcile_ticks = 0;
        // The real transition for the other two terms: a COMPLETE scan landing
        // clears `inherited_scan_inflight`, sets `last_scan_complete`, and —
        // because the mesh is whole — flips `interest_authoritative`. No field
        // is poked; this is the code path a live hub takes.
        hub.dispatch(HubCommand::InheritedSessions {
            sessions: Vec::new(),
            complete: true,
        })
        .await;

        let mesh = Self {
            hub,
            placement,
            peers,
            _ctl: ctls,
            probe,
            _tx: tx,
        };
        mesh.assert_settled("at fixture build");
        mesh
    }

    /// THE HARD PRECONDITION. Each of `routing_unsettled`'s five terms is
    /// asserted separately, so the file fails loudly rather than degrading into
    /// a measurement of the `||` short circuit (see the module docs).
    fn assert_settled(&self, when: &str) {
        assert!(
            self.hub.clustered(),
            "{when}: the hub must be CLUSTERED, or routing_unsettled() is false \
             for a reason that has nothing to do with the mesh and every \
             assertion in this file is vacuous"
        );
        assert_eq!(
            self.hub.takeover_reconcile_ticks, 0,
            "{when}: term 1 of routing_unsettled() must be false, or the `||` \
             short-circuits before mesh_settled() is ever reached — the exact \
             way this test could certify a non-fix"
        );
        assert!(
            !self.hub.inherited_scan_inflight,
            "{when}: term 2 must be false (short circuit)"
        );
        assert!(
            self.hub.interest_authoritative,
            "{when}: term 3 must be false (short circuit)"
        );
        assert!(
            self.hub.last_scan_complete,
            "{when}: term 4 must be false (short circuit)"
        );
        assert!(
            self.hub.mesh_settled(),
            "{when}: term 5 — the ONE this file measures — must be reached AND \
             answer settled"
        );
        assert!(
            !self.hub.routing_unsettled(),
            "{when}: the whole predicate must be false"
        );
    }

    fn counts(&self) -> Counts {
        Counts::now(&self.probe)
    }

    /// A `QoS` 1 publish whose ack is gated on the fan-out — the shape
    /// `register_pending` evaluates `routing_unsettled()` for.
    async fn gated_publish(&mut self, topic: &str) -> oneshot::Receiver<PublishOutcome> {
        let (done, rx) = oneshot::channel();
        self.hub
            .dispatch(HubCommand::Publish {
                topic: topic.into(),
                payload: Bytes::from_static(b"x"),
                qos: QoS::AtLeastOnce,
                retain: false,
                message_expiry: None,
                app: AppProperties::default(),
                done: Some(done),
                v5: true,
                publisher: None,
            })
            .await;
        rx
    }

    /// A `QoS` 0 publish with no publisher waiting — the ungated fan-out item
    /// 1.3 is about.
    async fn plain_publish(&mut self, topic: &str) {
        self.hub
            .dispatch(HubCommand::Publish {
                topic: topic.into(),
                payload: Bytes::from_static(b"x"),
                qos: QoS::AtMostOnce,
                retain: false,
                message_expiry: None,
                app: AppProperties::default(),
                done: None,
                v5: true,
                publisher: None,
            })
            .await;
    }

    /// An inbound acked forward from `peer-0` — the receiver-side twin of the
    /// settle gate at `hub/mod.rs`'s `RemotePublishAcked` arm.
    async fn peer_forward(&mut self, topic: &str, seq: u64) {
        self.hub
            .dispatch(HubCommand::RemotePublishAcked {
                node: NodeId("peer-0".into()),
                seq,
                topic: topic.into(),
                payload: Bytes::from_static(b"x"),
                qos: QoS::AtLeastOnce,
                retain: false,
                message_expiry: None,
                app: AppProperties::default(),
            })
            .await;
    }
}

// ---------------------------------------------------------------------------
// Item 1.1 — the cached mesh predicates
// ---------------------------------------------------------------------------

/// ITEM 1.1, the sender side. On a settled routing view the answer to "is my
/// routing view still settling?" is a CONSTANT, but `register_pending` asks it
/// on every gated publish and the answer used to be recomputed by walking the
/// whole member set each time.
///
/// Zero enumerations for `MESSAGES` publishes, and the control proves the cache
/// is a cache and not a deletion: a membership transition must still recompute.
#[tokio::test]
async fn a_settled_gated_publish_does_not_re_enumerate_cluster_members() {
    let mut mesh = Mesh::settled(PEERS, &[]).await;

    let before = mesh.counts();
    let mut acks = Vec::new();
    for i in 0..MESSAGES {
        acks.push(mesh.gated_publish(&format!("site/{i}/rpm")).await);
    }
    let d = mesh.counts().since(before);
    mesh.assert_settled("after the measurement window");

    assert_eq!(
        d.peers_all,
        0,
        "a settled routing view is a CONSTANT: {MESSAGES} gated publishes must \
         not re-enumerate the member set once each \
         (register_pending -> routing_unsettled -> mesh_settled -> peers_all), \
         got {} enumerations over {} members",
        d.peers_all,
        PEERS + 1
    );

    // The publishes really were served and really were gated: a settled view
    // acks a zero-match fan-out rather than holding it, which is what proves
    // `routing_unsettled()` was consulted and answered false.
    for (i, mut rx) in acks.into_iter().enumerate() {
        assert!(
            matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
            "publish {i} was gated on a settled view and must have been acked"
        );
    }

    // THE CONTROL. A cache nothing refreshes is a stale cache. `peer_connected`
    // is one of the five recompute transitions and must still pay for one.
    mesh.placement.write().unwrap().observe(
        &NodeId("peer-late".into()),
        MemberState::Alive,
        "l:7000",
        None,
    );
    let (ptx, _prx) = mpsc::unbounded_channel();
    let (ctl, _ctl_rx) = mpsc::unbounded_channel();
    let before = mesh.counts();
    mesh.hub
        .dispatch(HubCommand::PeerConnected {
            node: NodeId("peer-late".into()),
            conn_id: 999,
            tx: ptx,
            ctl,
            cert_serial: None,
            proto: crate::hub::PROTO_FORWARD_VERDICT,
            depth: Arc::new(AtomicUsize::new(0)),
        })
        .await;
    let d = mesh.counts().since(before);
    assert!(
        d.peers_all >= 1,
        "peer_connected is mesh transition 1 of 5; a cache that no transition \
         recomputes is a cache that can only be stale"
    );
}

/// ITEM 1.1, the RECEIVER side — the second per-message caller of the same
/// predicate, on the path a publish takes on every node that is not its origin.
/// `RemotePublishAcked`'s arm evaluates `routing_unsettled()` for every forward
/// that matched nothing locally, which in a cluster is most of them.
///
/// The sender-side test above would not notice if this one kept enumerating.
#[tokio::test]
async fn a_settled_peer_forward_does_not_re_enumerate_cluster_members() {
    let mut mesh = Mesh::settled(PEERS, &[]).await;
    // Drain the interest snapshots peer-0 was sent at link-up so the verdict
    // control below reads only what this window produced.
    while mesh.peers[0].1.try_recv().is_ok() {}

    let before = mesh.counts();
    for seq in 1..=MESSAGES as u64 {
        mesh.peer_forward("site/inbound/rpm", seq).await;
    }
    let d = mesh.counts().since(before);
    mesh.assert_settled("after the measurement window");

    assert_eq!(
        d.peers_all, 0,
        "the receiver-side twin evaluates routing_unsettled() on every \
         zero-match forward; with a settled view that must be a cached read, \
         got {} enumerations for {MESSAGES} forwards",
        d.peers_all
    );

    // The control: the branch was REACHED and answered, not skipped. A settled
    // view stands behind "nobody here is owed this" and answers the peer.
    let mut answered = 0usize;
    while let Ok(frame) = mesh.peers[0].1.try_recv() {
        match frame {
            PeerMessage::PublishVerdict { .. } | PeerMessage::PublishAck { .. } => answered += 1,
            _ => {}
        }
    }
    assert_eq!(
        answered, MESSAGES,
        "every forward must have been answered, or the measured branch was \
         never reached"
    );
}

// ---------------------------------------------------------------------------
// Item 1.2 — the member set is BORROWED, not copied out
// ---------------------------------------------------------------------------

/// ITEM 1.2. `Placement::members()` returns `Vec<NodeId>` and `NodeId` is
/// `pub String`, so the old `peers_all` copied the whole member set out from
/// under the read lock — one heap `Vec` plus one `String` clone per member —
/// to run a predicate that only ever READS them.
///
/// `mqtt-cluster`'s `members_probe` is `#[cfg(test)]` inside THAT crate, so a
/// `mqttd` test cannot count `members()` calls (that half is proved by
/// `placement::tests::visiting_the_member_set_never_calls_the_allocating_one`).
/// What this test can prove, from here, is the structural fact that makes the
/// copy unnecessary and that no behavioural test can fake: the predicate now
/// runs while the READ GUARD IS STILL HELD. A `peers_all` written as
/// `let members = { placement.read().members() };` — the shape being replaced —
/// has dropped the guard by the time the predicate runs, so a `try_write` from
/// inside it SUCCEEDS. Borrowing, it cannot.
#[tokio::test]
async fn membership_enumeration_borrows_the_member_set_under_the_read_lock() {
    let mesh = Mesh::settled(PEERS, &[]).await;
    let placement = mesh.placement.clone();

    let visits = std::cell::Cell::new(0usize);
    let guard_held = std::cell::Cell::new(true);
    let before = mesh.counts();
    let all = mesh.hub.peers_all(|_peer| {
        visits.set(visits.get() + 1);
        // A reader is held ⇒ try_write must fail. If `peers_all` had copied the
        // members out first, the guard would be gone and this would succeed.
        if placement.try_write().is_ok() {
            guard_held.set(false);
        }
        true
    });
    let d = mesh.counts().since(before);

    assert!(all, "every peer link in the fixture is up");
    assert_eq!(d.peers_all, 1, "exactly the one enumeration being measured");
    assert!(
        guard_held.get(),
        "peers_all must BORROW the member set under the placement read guard \
         (Placement::all_members), not copy it out into a Vec<NodeId> of \
         {} String clones and then walk the copy (issue #613 item 1.2)",
        PEERS + 1
    );
    assert_eq!(
        visits.get(),
        PEERS,
        "cheaper, not NARROWER: the borrowing walk must still visit every \
         eligible member other than this node (the local id short-circuits) — \
         peers_all is the single definition of which members count"
    );
}

// ---------------------------------------------------------------------------
// Item 1.3 — the peer-map walk that produced nothing
// ---------------------------------------------------------------------------

/// ITEM 1.3, THE SHARPEST ASSERTION IN THE SET. With zero ordinary remote
/// interest and `P` connected peers, a `QoS` 0 publish must touch the peer map
/// EXACTLY ZERO times — not `P` times to discover there is nothing to do.
///
/// Driven through the whole `HubCommand::Publish` dispatch, not through
/// `forward_to_peers` directly, so it also pins that nothing else on the publish
/// path reintroduces the walk. The slope is asserted directly: the count is zero
/// at 4 peers and still zero at 16, so it is O(1) in peer count and not merely
/// small.
#[tokio::test]
async fn a_qos0_publish_with_no_remote_interest_never_touches_the_peer_map() {
    for peers in [4usize, 16] {
        let mut mesh = Mesh::settled(peers, &[]).await;
        let before = mesh.counts();
        for i in 0..MESSAGES {
            mesh.plain_publish(&format!("site/{i}/rpm")).await;
        }
        let d = mesh.counts().since(before);
        mesh.assert_settled("after the measurement window");
        assert_eq!(
            d.peer_loop,
            0,
            "no peer advertises interest and retain is false, so the fan-out has \
             nothing for anybody: the peer map must be touched ZERO times, not \
             {} ({MESSAGES} publishes x {peers} peers, every iteration a \
             `continue`) — issue #613 item 1.3",
            MESSAGES * peers
        );
    }

    // THE CONTROL, on a fixture whose peers DO advertise. The early return must
    // mean "nothing to do", never "do nothing": the loop must still run once per
    // peer and must still put the frame in every peer's channel.
    let mut hit = Mesh::settled(PEERS, &["site/#"]).await;
    for (_, rx) in &mut hit.peers {
        while rx.try_recv().is_ok() {}
    }
    let before = hit.counts();
    hit.plain_publish("site/0/rpm").await;
    let d = hit.counts().since(before);
    assert_eq!(
        d.peer_loop, PEERS,
        "with interest the loop IS the delivery path and must still run"
    );
    for (node, rx) in &mut hit.peers {
        let frame = rx
            .try_recv()
            .unwrap_or_else(|e| panic!("{} was owed the publish but got nothing ({e})", node.0));
        assert!(
            matches!(frame, PeerMessage::Publish { ref topic, .. } if topic == "site/0/rpm"),
            "{} got {frame:?}",
            node.0
        );
    }
}

// ---------------------------------------------------------------------------
// Item 1.4 — one reused interest buffer instead of a fresh set per publish
// ---------------------------------------------------------------------------

/// ITEM 1.4, through the full publish dispatch. The fan-out used to build a
/// fresh `HashSet<NodeId>` per message — one heap set plus one `String` per
/// interested node, minted only to be compared and dropped. It now resolves into
/// a hub-owned scratch `Vec<ClientId>` of the subscription table's own interned
/// ids.
///
/// The observable is buffer IDENTITY: after the first publish the scratch's
/// allocation must never move again, however many publishes follow. That is the
/// allocation claim stated as something deterministic, and it is measured across
/// all three of `forward_to_peers`'s exits.
#[tokio::test]
async fn the_interest_buffer_is_allocated_once_for_every_publish_that_follows() {
    let mut mesh = Mesh::settled(PEERS, &["site/#"]).await;

    // Warm it: the first publish is allowed to allocate.
    mesh.plain_publish("site/0/rpm").await;
    let ptr = mesh.hub.interest_scratch.as_ptr();
    let cap = mesh.hub.interest_scratch.capacity();
    assert!(
        cap >= PEERS,
        "the scratch must actually be holding the {PEERS} interested nodes, or \
         this test is measuring an empty Vec's dangling pointer"
    );

    for i in 0..MESSAGES {
        // A matching QoS 0 fan-out (the fall-through exit)...
        mesh.plain_publish(&format!("site/{i}/rpm")).await;
        // ...a gated QoS 1 fan-out (the early `return` after the acked arm)...
        drop(mesh.gated_publish(&format!("site/{i}/rpm")).await);
        // ...and a topic nobody wants (item 1.3's emptiness guard exit).
        mesh.plain_publish(&format!("nobody/{i}")).await;
        assert!(
            std::ptr::eq(mesh.hub.interest_scratch.as_ptr(), ptr),
            "publish {i} moved the interest buffer: the fan-out is allocating a \
             fresh interested-node collection per message again (issue #613 \
             item 1.4), or an exit failed to restore the scratch"
        );
    }
    assert_eq!(
        mesh.hub.interest_scratch.capacity(),
        cap,
        "the buffer must be REUSED, not regrown, across {MESSAGES} publishes"
    );
    mesh.assert_settled("after the measurement window");
}

// ---------------------------------------------------------------------------
// Item 1.5 — the shared planner's scratch set
// ---------------------------------------------------------------------------

/// ITEM 1.5, as the TWO-ARM shape the critics demanded instead of an absolute
/// count on a path waves A, C and D all touch.
///
/// `plan_shared`'s `decided` scratch exists only so the PEER pass does not
/// resurrect a group the constant-time local path already answered. When no peer
/// has announced a shared group at all — every single-node deployment, and every
/// cluster whose shared groups are local — that pass returns before it ever reads
/// `decided`, so filling it was one collection per publish to answer a question
/// nobody asks.
///
/// Arm A (no remote shared group) must drop the work to zero. Arm B (a remote
/// shared group exists) must NOT regress: it still takes exactly one scratch per
/// publish, and — the slope — that stays one however many groups match.
#[tokio::test]
async fn shared_planning_takes_no_scratch_when_no_peer_announced_a_group() {
    for groups in [1usize, 4] {
        // ---- arm A: nothing remote ----------------------------------------
        let (mut hub, _tx) = Hub::new();
        let probe = hub.probe();
        hub.set_shared_prefer_local(true);
        for g in 0..groups {
            hub.shared.subscribe(
                ClientId(format!("local-{g}").into()),
                &format!("pool-{g}"),
                "site/#",
                QoS::AtLeastOnce,
                true,
            );
        }
        let before = Counts::now(&probe);
        for i in 0..MESSAGES {
            let plans = hub.plan_shared(&format!("site/{i}/rpm"));
            assert_eq!(plans.len(), groups, "the fixture must reach the planner");
        }
        let d = Counts::now(&probe).since(before);
        assert_eq!(
            d.shared_scratch, 0,
            "no peer announced a shared group, so the peer pass cannot resurrect \
             anything and `decided` is never read: {MESSAGES} publishes over \
             {groups} matching group(s) must take ZERO scratch collections, got \
             {} (issue #613 item 1.5)",
            d.shared_scratch
        );

        // ---- arm B: a peer announced one -----------------------------------
        let (mut hub, _tx) = Hub::new();
        let probe = hub.probe();
        hub.set_shared_prefer_local(true);
        for g in 0..groups {
            hub.shared.subscribe(
                ClientId(format!("local-{g}").into()),
                &format!("pool-{g}"),
                "site/#",
                QoS::AtLeastOnce,
                true,
            );
        }
        hub.dispatch(HubCommand::RemoteSharedInterest {
            node: NodeId("peer-0".into()),
            groups: vec![RemoteSharedGroup {
                group: "pool-remote".into(),
                filter: "site/#".into(),
                members: vec![(ClientId("remote-0".into()), QoS::AtLeastOnce, true)],
            }],
        })
        .await;
        let before = Counts::now(&probe);
        for i in 0..MESSAGES {
            let plans = hub.plan_shared(&format!("site/{i}/rpm"));
            assert_eq!(
                plans.len(),
                groups + 1,
                "the local groups plus the announced remote one"
            );
        }
        let d = Counts::now(&probe).since(before);
        assert_eq!(
            d.shared_scratch, MESSAGES,
            "the with-remote arm must NOT regress: exactly one scratch per \
             publish, and — the slope — still one with {groups} matching local \
             group(s), never one per group"
        );
    }
}

// ---------------------------------------------------------------------------
// Item 1.6 — the O(peers) term is observable in PRODUCTION, not just in tests.
// ---------------------------------------------------------------------------

/// `hub_fanout_peer_visits_total / hub_fanout_seconds_count` is the average
/// number of peer links one publish walks — the quantity items 1.3 and 1.4
/// reduce, and the only reading that can show it on a real cluster at N=5 versus
/// N=7. That ratio is only meaningful if the CHEAP path reports too: a fan-out
/// that early-returned having walked no links must still observe, with zero
/// visits. Otherwise the denominator counts only expensive fan-outs and the
/// average is pinned near the old value however cheap the common case gets.
///
/// Fails before the metric was wired: `observe_hub_fanout` was registered with
/// no caller anywhere in the repo, so both series rendered as permanently
/// absent — an operator reading them would conclude the peer loop is never
/// walked, which is false.
#[tokio::test]
async fn a_fan_out_that_walks_no_peers_still_reports_itself() {
    let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("t"));
    let (mut hub, tx) = Hub::with_config(
        NodeId("fanout-metric".into()),
        std::sync::Arc::new(MemorySessionStore::new()),
    );
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());

    // A local subscriber and NO peers: every publish takes item 1.3's early
    // return, which is exactly the path that must still report.
    let (mut out_rx, _guard) = attach_full(&tx, "sub", 1, true, 0, u16::MAX).await;
    subscribe(&tx, "sub", "fanout/t");
    publish(&tx, "fanout/t", b"hi");

    // The delivery is the barrier: once the subscriber has the packet, the
    // publish dispatch (and its fan-out) has completed on the loop.
    let pkt = timeout(Duration::from_millis(500), out_rx.recv())
        .await
        .expect("delivery")
        .expect("a packet");
    assert!(matches!(*pkt, Packet::Publish(_)));

    let out = metrics.render();
    assert!(
        out.contains("mqttd_hub_fanout_seconds_count 1"),
        "the fan-out did not observe itself on the early-return path, so \
         hub_fanout_seconds_count counts only EXPENSIVE fan-outs and the \
         per-publish average links walked is unreadable (#613 item 1.6):\n{out}"
    );
    assert!(
        out.contains("mqttd_hub_fanout_peer_visits_total 0"),
        "zero peer visits must be RECORDED as zero, not left absent — absent \
         and zero render the same and mean the opposite (#613 item 1.6):\n{out}"
    );
}
