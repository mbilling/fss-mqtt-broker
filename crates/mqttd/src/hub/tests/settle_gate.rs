//! Behavioural proofs for issue #613 wave A: the settle gate (item 2.1, AS
//! REDESIGNED under CORRECTION 1), the durable-plane gate on the periodic
//! inherited-session scan (item 2.2), and the interest-authoritative liveness
//! backstop's own constant (item 2.6).
//!
//! **Why these are in-crate and not under `crates/mqttd/tests/`.** Item 2.1's
//! whole claim is that the ACK and the settle work set stopped being one fact:
//! a publish may be answered and STILL owe the settle pass a re-delivery and a
//! re-route. The only honest observables for that are `PendingPublish`'s two
//! flags (`awaiting_settle`, `ack_awaits_settle`) and `ack_released()`, all
//! `pub(super)`, plus `Hub::routing_unsettled()`, which is private. An
//! integration test links the library built WITHOUT `cfg(test)` and can see
//! none of them; it could only assert "the ack arrived", which is exactly the
//! half that the original (unsound) spec also produced — while silently
//! deleting the replay. A test that cannot tell those two apart is the
//! non-fix-certifying test this wave was warned about.
//!
//! **No clock anywhere in this file.** The hub is driven WITHOUT its actor (the
//! `tests/qos2_retirement.rs` precedent): commands go through `Hub::dispatch`
//! directly, the hub's own self-queue is pumped to quiescence by [`quiesce`],
//! and the sweep is called as the plain `async fn` it is. Nothing sleeps,
//! nothing polls a deadline, and no assertion is a duration — every one is a
//! state read or a count taken at a point where no work is left to do.

use super::*;
use crate::hub::{EXPIRY_RECONCILE_EVERY, INTEREST_AUTHORITATIVE_BACKSTOP_TICKS};
use mqtt_core::Subscription;

/// Item 2.6's other half, enforced at COMPILE time rather than in a test body:
/// the backstop is a degraded-node DEADLINE, and the whole point of giving it
/// its own constant was that it stopped being the reconcile CADENCE. Setting it
/// back to 30 would make the item a rename, and this refuses to build.
const _: () = assert!(
    INTEREST_AUTHORITATIVE_BACKSTOP_TICKS < EXPIRY_RECONCILE_EVERY,
    "INTEREST_AUTHORITATIVE_BACKSTOP_TICKS must stay strictly shorter than the \
     reconcile cadence it used to borrow (issue #613 item 2.6)"
);

/// The hub under test plus the store it was built on, so a test can read what
/// actually landed in an offline session's durable queue.
struct Fix {
    hub: Hub,
    store: Arc<MemorySessionStore>,
    /// Outbound meters for the attached clients. Held for the fixture's life:
    /// dropping a meter is how a connection reports itself gone, and
    /// `reap_closed_outbounds` would then take the subscriber out from under
    /// the very fan-out being measured.
    meters: Vec<crate::hub::OutboundMeter>,
}

/// A CLUSTERED hub with NO placement.
///
/// That combination is deliberate and is what makes every assertion below a
/// statement about the settle gate rather than about the mesh cache:
///   * `clustered()` is true via `cluster_configured`, so `routing_unsettled()`
///     is not short-circuited to `false` (hub/mod.rs, `clustered`);
///   * `mesh_fingerprint()` is `None`, so `mesh_whole()`/`mesh_settled()` are
///     unconditionally true — the mesh terms of `routing_unsettled()` are held
///     OUT of these tests, and the only live terms are the ones item 2.1 is
///     about (`takeover_reconcile_ticks`, `interest_authoritative`,
///     `last_scan_complete`);
///   * `owns_session` answers `true` for every client, so an inherited session
///     really is materialised by `inherit_sessions` rather than skipped as
///     another node's.
fn clustered_fixture() -> Fix {
    let store = Arc::new(MemorySessionStore::new());
    let (mut hub, _tx) = Hub::with_config(
        NodeId("settle-gate".into()),
        store.clone() as Arc<dyn SessionStore>,
    );
    hub.set_cluster_configured();
    Fix {
        hub,
        store,
        meters: Vec::new(),
    }
}

/// A clustered hub whose mesh can never be WHOLE: one membership-alive peer
/// that has no link and never will.
///
/// Used only by the item 2.6 test, and for one reason: `inherit_sessions` flips
/// `interest_authoritative` on the first COMPLETE scan over a WHOLE mesh, and
/// `MemorySessionStore::all_sessions` always answers complete. Holding the mesh
/// broken is the clock-free way to keep that door shut, so the backstop is the
/// only thing left that can open it — which is what the test is measuring.
fn partitioned_fixture() -> Fix {
    let store = Arc::new(MemorySessionStore::new());
    let mut placement = Placement::new(NodeId("settle-gate".into()), DEFAULT_REPLICAS);
    placement.observe(
        &NodeId("unreachable".into()),
        MemberState::Alive,
        "127.0.0.1:7100",
        None,
    );
    let (hub, _tx) = Hub::with_config_and_placement(
        NodeId("settle-gate".into()),
        store.clone() as Arc<dyn SessionStore>,
        Some(Arc::new(RwLock::new(placement))),
    );
    Fix {
        hub,
        store,
        meters: Vec::new(),
    }
}

/// Drive the hub to a QUIESCENT point without its actor.
///
/// `Hub::dispatch` is the only thing `run()`'s command arm does, but several
/// handlers finish off-loop and report back through the hub's own `self_tx`:
/// session recovery (`SessionRecovered`), the append lanes (`AppendDone`) and
/// the inherited-session scan (`InheritedSessions`). Pumping `hub.rx` here is
/// what `run()` would have done, minus the sweep timer — so the tests keep
/// direct access to hub state while still exercising the real handlers.
///
/// Bounded and yield-driven, never timed: each round drains everything queued
/// and then yields so the spawned tasks can produce more. It returns once
/// `IDLE_ROUNDS` consecutive rounds have produced nothing — more than one,
/// because an off-loop completion takes several polls to travel (the lane
/// worker is woken, its store future resolves, then it sends), and returning on
/// the first quiet round would be a race dressed up as a quiescence check.
async fn quiesce(hub: &mut Hub) {
    const IDLE_ROUNDS: u32 = 8;
    let mut idle = 0;
    for _ in 0..256 {
        let mut progressed = false;
        while let Ok(cmd) = hub.rx.try_recv() {
            hub.dispatch(cmd).await;
            progressed = true;
        }
        tokio::task::yield_now().await;
        idle = if progressed || !hub.rx.is_empty() {
            0
        } else {
            idle + 1
        };
        if idle >= IDLE_ROUNDS {
            return;
        }
    }
    panic!("hub did not quiesce: its self-queue kept producing work for 256 rounds");
}

impl Fix {
    async fn dispatch(&mut self, cmd: HubCommand) {
        self.hub.dispatch(cmd).await;
        quiesce(&mut self.hub).await;
    }

    /// Attach an ONLINE, CLEAN-session subscriber and subscribe it to `filter`.
    ///
    /// Clean-session on purpose: `is_persistent` is then false, so
    /// `deliver_to_client` takes the direct-send branch instead of the off-loop
    /// append lane, `appends_outstanding` stays 0, and the only thing that can
    /// still be holding this publish's ack is the settle gate. That is what
    /// makes "the ack resolved" a statement about item 2.1 and nothing else.
    async fn subscriber(&mut self, client: &str, filter: &str, conn_id: u64) {
        let (otx, _orx) = mpsc::unbounded_channel::<Box<Packet>>();
        let (outbound, meter) = Outbound::new(otx);
        self.meters.push(meter);
        let (reply, wait) = oneshot::channel();
        self.dispatch(HubCommand::Attach {
            client: ClientId(client.into()),
            admission: admission(client),
            conn_id,
            clean_start: true,
            session_expiry: 0,
            receive_maximum: u16::MAX,
            will: None,
            outbound,
            reply,
        })
        .await;
        assert!(
            matches!(wait.await, Ok(AttachOutcome::Present(false))),
            "the fixture's subscriber must attach cleanly"
        );
        let (reply, wait) = oneshot::channel();
        self.dispatch(HubCommand::Subscribe {
            client: ClientId(client.into()),
            filters: vec![(filter.into(), QoS::AtLeastOnce)],
            no_local_filters: Vec::new(),
            sub_id: None,
            rap_filters: Vec::new(),
            retain_handling: vec![0],
            reply: Some(reply),
        })
        .await;
        assert_eq!(wait.await.unwrap(), vec![true], "the subscribe must grant");
    }

    /// A GATED `QoS` 1 publish: `done` is `Some`, so `register_pending` runs and
    /// the returned receiver IS the publisher's acknowledgement.
    async fn publish_gated(&mut self, topic: &str) -> oneshot::Receiver<PublishOutcome> {
        let (done, wait) = oneshot::channel();
        self.dispatch(HubCommand::Publish {
            topic: topic.into(),
            payload: Bytes::from_static(b"settle-gate"),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: None,
            app: AppProperties::default(),
            done: Some(done),
            v5: true,
            publisher: None,
        })
        .await;
        wait
    }

    /// The one pending publish's id. Every test here registers exactly one.
    fn only_pending(&self) -> u64 {
        let ids: Vec<u64> = self.hub.pending_publishes.keys().copied().collect();
        assert!(
            !ids.is_empty(),
            "the pending-publish ledger is EMPTY: this publish's entry was \
             retired together with its acknowledgement, so \
             `settle_pending_publishes` can never re-deliver or re-route it. \
             Every delivery to a session the takeover scan materialises during \
             this window is then silently lost — no compile error, no other \
             failing test (#613 CORRECTION 1)"
        );
        assert_eq!(ids.len(), 1, "fixture invariant: one pending publish");
        ids[0]
    }

    /// Put a durable session into the STORE only — never through the hub — so
    /// the hub learns about it exclusively from an inherited-session scan. This
    /// is the session that exists in exhibit ⑥: handed over by a takeover, with
    /// subscriptions this node has not yet materialised.
    async fn store_only_session(&self, client: &str, filter: &str) {
        let c = ClientId(client.into());
        self.store.ensure_session(&c).await.unwrap();
        self.store
            .set_subscriptions(
                &c,
                &[Subscription {
                    filter: filter.into(),
                    max_qos: QoS::AtLeastOnce,
                    no_local: false,
                    sub_id: None,
                }],
            )
            .await
            .unwrap();
    }

    /// Deliver the result of an inherited-session scan, exactly as the real scan
    /// task does (`spawn_inherited_session_scan` -> `HubCommand::InheritedSessions`).
    /// `inherit_sessions` is `settle_pending_publishes`'s only caller in the tree,
    /// and this command is the only way to reach it — which is why every replay
    /// and re-route assertion below is taken after one of these and never after a
    /// `RemoteInterest` or a sweep.
    async fn scan_lands(&mut self, clients: &[&str], complete: bool) {
        let mut sessions = Vec::new();
        for c in clients {
            let id = ClientId((*c).into());
            let subs = self.store.subscriptions(&id).await.unwrap();
            sessions.push((id, subs, None));
        }
        self.dispatch(HubCommand::InheritedSessions { sessions, complete })
            .await;
    }

    async fn queued(&self, client: &str) -> Vec<mqtt_storage::QueuedMessage> {
        self.store
            .pending(&ClientId(client.into()), 0, 10)
            .await
            .unwrap()
    }
}

/// `Err(Empty)` is the only honest reading of "the ack is still held": the
/// sender is alive inside `pending_publishes`, so the answer is neither given
/// nor withheld. `Err(Closed)` means the entry was DROPPED, which is a withhold
/// — a different outcome that must never be allowed to pass as a hold.
fn held(rx: &mut oneshot::Receiver<PublishOutcome>) -> bool {
    matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty))
}

// ---------------------------------------------------------------------------
// Item 2.1 — the settle gate, as redesigned under CORRECTION 1.
// ---------------------------------------------------------------------------

/// **(a) The item's own claim.** A gated `QoS` 1 publish that REACHED a local
/// subscriber is acknowledged even though this node's routing view is
/// admittedly unsettled. Evidence releases the ack; the window does not have to
/// close first.
///
/// Fails before item 2.1: `register_pending` sets `awaiting_settle` from
/// `routing_unsettled()` alone, before the fan-out has run, and the completion
/// test contained `!p.awaiting_settle` — so this ack was held until the window
/// closed, whatever the fan-out found.
#[tokio::test]
async fn an_unsettled_view_releases_the_ack_of_a_publish_that_reached_a_subscriber() {
    let mut fix = clustered_fixture();
    fix.subscriber("live", "settle/matched", 1).await;
    assert!(
        fix.hub.routing_unsettled(),
        "fixture invariant: the routing view must be unsettled, or this test \
         asserts nothing"
    );

    let mut ack = fix.publish_gated("settle/matched").await;

    assert_eq!(
        ack.try_recv(),
        Ok(PublishOutcome::Accepted),
        "a gated QoS 1 publish that matched a local subscriber is still being \
         held by the settle gate: the ack is being decided at registration, \
         before the fan-out, instead of against what the fan-out found (#613 \
         item 2.1)"
    );
}

/// **(b) The guard that the relaxation was not implemented by deleting the
/// gate.** A fan-out that reached NOBODY, on a view this node admits is
/// incomplete, is still held — and released, as `Accepted`, only once the
/// window closes.
///
/// This one passes both before and after by design, and it is not offered as
/// the proof of 2.1. It is the discriminator against the wrong fix: any
/// implementation that clears `ack_awaits_settle` unconditionally rather than
/// against evidence trips its first assertion, and any implementation that
/// forgets to clear it at `window_over` trips its last.
#[tokio::test]
async fn a_zero_match_publish_still_holds_its_ack_on_an_unsettled_view() {
    let mut fix = clustered_fixture();
    assert!(fix.hub.routing_unsettled());

    let mut ack = fix.publish_gated("settle/nobody").await;
    let id = fix.only_pending();

    assert!(
        held(&mut ack),
        "a ZERO-match gated publish was acked while the routing view was \
         unsettled: an Accepted released against no recorded evidence (ADR \
         0042 T9 exhibit ⑥)"
    );
    let p = &fix.hub.pending_publishes[&id];
    assert!(
        p.ack_awaits_settle,
        "the ack hold must be the thing holding it"
    );
    assert!(p.awaiting_settle, "and it is still in the settle work set");
    assert!(!p.ack_released());

    // Close the window: a complete scan over a whole mesh makes interest
    // authoritative, and nothing else is left unsettled once the boot window's
    // ticks are spent.
    fix.hub.takeover_reconcile_ticks = 0;
    fix.scan_lands(&[], true).await;
    assert!(
        !fix.hub.routing_unsettled(),
        "fixture invariant: the window must actually be closed"
    );

    assert_eq!(
        ack.try_recv(),
        Ok(PublishOutcome::Accepted),
        "the window closed and the held ack was never released: `window_over` \
         must clear BOTH holds, or a zero-evidence publish is never answered \
         at all"
    );
    assert!(
        !fix.hub.pending_publishes.contains_key(&id),
        "with both holds clear and no obligation outstanding the entry retires"
    );
}

/// **THE CORRECTION-1 REGRESSION TEST.** The ack leaves early; the REPLAY
/// OBLIGATION does not.
///
/// One already-attached local subscriber makes the fan-out evidence-bearing, so
/// the publisher is answered inside the same dispatch. A second session exists
/// only in the store, subscribed to the same topic, and is materialised on this
/// node exclusively by the inherited-session scan — the exhibit ⑥ session. The
/// test asserts that after the early ack the entry is STILL in
/// `settle_pending_publishes`'s work set, and that when the scan lands the
/// message really is enqueued for that session.
///
/// Fails against the ORIGINAL (cancelled) 2.1 spec, which set
/// `awaiting_settle: false` at registration and re-armed it only for a
/// zero-match fan-out: `pending_local_done -> try_complete_pending` would then
/// remove the entry on the same dispatch, `settle_pending_publishes` would
/// never see it, and the inherited session's queue would be empty here — with
/// no compile error and no other failing test to say so. Fails against today's
/// pre-#613 code on its first assertion, where the ack is held for the whole
/// window.
#[tokio::test]
async fn an_early_acked_publish_still_replays_to_a_session_the_scan_materialises() {
    let mut fix = clustered_fixture();
    fix.subscriber("live", "settle/x", 1).await;
    fix.store_only_session("inherited", "settle/x").await;
    assert!(
        fix.hub.routing_unsettled(),
        "fixture invariant: the takeover window must be open"
    );
    assert!(
        !fix.hub.has_materialized_subs(&ClientId("inherited".into())),
        "fixture invariant: the inherited session must be unknown to the hub \
         until the scan lands — otherwise the original fan-out reaches it and \
         the replay proves nothing"
    );

    let mut ack = fix.publish_gated("settle/x").await;
    let id = fix.only_pending();

    // Half one: item 2.1's win.
    assert_eq!(
        ack.try_recv(),
        Ok(PublishOutcome::Accepted),
        "the matched fan-out is evidence; the ack must not wait for the window"
    );
    assert!(
        fix.queued("inherited").await.is_empty(),
        "fixture invariant: the ORIGINAL fan-out cannot have reached a session \
         this node does not yet route"
    );

    // Half two: CORRECTION 1. The entry survives its own acknowledgement,
    // because the settle pass still owes this publish a re-delivery.
    let p = fix.hub.pending_publishes.get(&id).expect(
        "the ledger entry must OUTLIVE its ack: releasing the ack must \
                 not remove the publish from settle_pending_publishes's work \
                 set, or every delivery to a session materialised during this \
                 window is silently lost (#613 CORRECTION 1)",
    );
    assert!(
        p.awaiting_settle,
        "the replay obligation must be untouched by the ack release"
    );
    assert!(
        !p.ack_awaits_settle,
        "only the ACK hold may be cleared by fan-out evidence"
    );
    assert!(p.ack_released(), "and it was cleared by answering, once");

    // The scan lands and materialises the session. `complete: false` keeps the
    // window open, so this asserts the REPLAY alone, with the entry's retirement
    // still pending.
    fix.scan_lands(&["inherited"], false).await;

    let queued = fix.queued("inherited").await;
    assert_eq!(
        queued.len(),
        1,
        "the settle pass did not re-deliver to the session it had just \
         materialised: the acknowledged publish was dropped from the settle \
         work set (#613 CORRECTION 1). Queue: {queued:?}"
    );
    assert_eq!(queued[0].message.topic, "settle/x");
    assert_eq!(&queued[0].message.payload[..], b"settle-gate");

    // And the publisher is answered exactly once: the replay is a broker-side
    // obligation with nobody left waiting on it.
    assert_eq!(
        ack.try_recv(),
        Err(oneshot::error::TryRecvError::Closed),
        "nothing further may be sent to a publisher already answered"
    );

    // Finally, the window closes and the entry retires.
    fix.hub.takeover_reconcile_ticks = 0;
    fix.scan_lands(&["inherited"], true).await;
    assert!(
        !fix.hub.pending_publishes.contains_key(&id),
        "at window close an answered entry with no outstanding obligation retires"
    );
}

/// The RE-ROUTE half of the same work set. A publish acked early against a
/// local match still re-routes to a peer that advertises matching interest
/// AFTER its fan-out — the takeover successor re-gossiping the filters it just
/// inherited (exhibit ⑤).
///
/// Fails against the original 2.1 spec for a structural reason worth stating:
/// the entry would already be gone, `reroute_candidates(id)` returns
/// `Vec::new()` for an absent id, and `register_forward` bails at its
/// `get_mut(&id)` guard — so the forward is silently never sent, with no error
/// anywhere. Note that `HubCommand::RemoteInterest` itself re-routes nothing:
/// `settle_pending_publishes` is the only path that does, which is what makes
/// this a statement about the work set.
#[tokio::test]
async fn an_early_acked_publish_still_re_routes_to_a_peer_that_advertises_interest_later() {
    let mut fix = clustered_fixture();
    fix.subscriber("live", "settle/r", 1).await;

    let (peer_tx, mut peer_rx): (PeerOutbound, _) = mpsc::unbounded_channel();
    fix.dispatch(HubCommand::PeerConnected {
        node: NodeId("successor".into()),
        conn_id: 1,
        ctl: peer_tx.clone(),
        tx: peer_tx,
        cert_serial: None,
        proto: mqtt_cluster::peer::PROTO_MAX,
        depth: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    })
    .await;

    let mut ack = fix.publish_gated("settle/r").await;
    let id = fix.only_pending();
    assert_eq!(ack.try_recv(), Ok(PublishOutcome::Accepted));
    assert!(
        fix.hub.pending_publishes[&id].awaiting.is_empty(),
        "fixture invariant: no peer advertised interest at fan-out time"
    );
    while peer_rx.try_recv().is_ok() {} // drop the link-up gossip

    // The successor materialises the inherited subscriber and re-advertises.
    fix.dispatch(HubCommand::RemoteInterest {
        node: NodeId("successor".into()),
        filters: vec!["settle/r".into()],
    })
    .await;
    assert!(
        fix.hub.pending_publishes[&id].awaiting.is_empty(),
        "fixture invariant: RemoteInterest alone must not re-route — only the \
         settle pass does, which is what this test is about"
    );

    fix.scan_lands(&[], false).await;

    let entry = fix
        .hub
        .pending_publishes
        .get(&id)
        .expect("the entry must still be here to be re-routed at all (#613 CORRECTION 1)");
    assert_eq!(
        entry.awaiting.len(),
        1,
        "the settle pass did not re-route an already-acked publish to the peer \
         that advertised interest after its fan-out"
    );
    assert_eq!(
        entry.awaiting.values().next().unwrap().node,
        NodeId("successor".into())
    );
    let mut acked_forwards = 0;
    while let Ok(frame) = peer_rx.try_recv() {
        if matches!(frame, PeerMessage::PublishAcked { .. }) {
            acked_forwards += 1;
        }
    }
    assert_eq!(
        acked_forwards, 1,
        "the obligation was recorded but no frame reached the peer"
    );
}

// ---------------------------------------------------------------------------
// Item 2.2 — the periodic inherited-session scan is gated on a durable plane.
// ---------------------------------------------------------------------------

/// Drive one sweep and let the scan it may have spawned complete, so the next
/// sweep is not suppressed by `inherited_scan_inflight`. `complete` decides
/// whether the landing scan is allowed to settle `interest_authoritative`.
async fn sweep_once(fix: &mut Fix, complete: bool) {
    fix.hub.sweep_expired_sessions().await;
    quiesce(&mut fix.hub).await;
    if fix.hub.inherited_scan_inflight {
        // The scan task's result reaches the hub through `self_tx`; the fixture
        // stands in for it so that "a scan ran" is counted once per tick rather
        // than once per test.
        fix.dispatch(HubCommand::InheritedSessions {
            sessions: Vec::new(),
            complete,
        })
        .await;
    }
}

fn scans(fix: &Fix) -> usize {
    fix.hub
        .probe
        .scans_started
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// With the durable plane OFF, the inherited-session scan runs for the takeover
/// window and then NEVER AGAIN — in particular not on the slow
/// `EXPIRY_RECONCILE_EVERY` cadence. Without a durable plane the store is local,
/// so its contents cannot have been inherited from anywhere and the scan can
/// only ever re-enumerate what this process already holds.
///
/// Fails before item 2.2: the periodic arm had no `durable_plane.is_some()`
/// gate, so sweep 30 spawned a full `all_sessions()` enumeration and the count
/// at 40 ticks is one higher than the count at 12.
#[tokio::test]
async fn no_periodic_inherited_scan_runs_without_a_durable_plane() {
    let mut fix = clustered_fixture();
    assert!(
        fix.hub.durable_plane.is_none(),
        "fixture invariant: no durable plane"
    );

    // The boot takeover window. The arm that spends it is deliberately NOT
    // gated — arming it at all already means something moved — so it must still
    // scan on each of its ticks. Read from the hub rather than written as a
    // literal, so retuning the window is not a spurious failure here.
    let window = usize::from(fix.hub.takeover_reconcile_ticks);
    assert!(window > 0, "fixture invariant: a boot window to spend");
    for _ in 0..(window + 4) {
        sweep_once(&mut fix, true).await;
    }
    let after_window = scans(&fix);
    assert_eq!(
        after_window, window,
        "the takeover window must still scan on each of its ticks, and exactly once each"
    );

    // Past sweep 30, where `expiry_reconcile_tick % EXPIRY_RECONCILE_EVERY == 0`.
    for _ in (window + 4)..(EXPIRY_RECONCILE_EVERY as usize + 10) {
        sweep_once(&mut fix, true).await;
    }
    assert_eq!(
        scans(&fix),
        after_window,
        "a periodic inherited-session scan ran with the durable plane OFF: the \
         reconcile-cadence arm is not gated the way rehome_misplaced_sessions \
         already is (#613 item 2.2)"
    );
}

// ---------------------------------------------------------------------------
// Item 2.6 — the interest-authoritative backstop has its own constant.
// ---------------------------------------------------------------------------

/// A node whose session scans never complete must not hide its live clients
/// from the cluster forever. The backstop that forces its interest gossip
/// authoritative fires at `INTEREST_AUTHORITATIVE_BACKSTOP_TICKS` — its own
/// deadline, not the reconcile CADENCE it used to borrow.
///
/// Fails before item 2.6: the comparison read `EXPIRY_RECONCILE_EVERY` (30), so
/// at tick `INTEREST_AUTHORITATIVE_BACKSTOP_TICKS` (10) nothing had fired and
/// the second assertion trips. The `<` assertion is the guard against the
/// constant being quietly set back to the cadence's value, which would make the
/// item a rename.
#[tokio::test]
async fn the_interest_authoritative_backstop_fires_on_its_own_constant() {
    let mut fix = partitioned_fixture();
    assert!(
        !fix.hub.mesh_whole(),
        "fixture invariant: the mesh must stay broken, or a completing scan \
         settles the flag and the backstop is never the thing under test"
    );
    for tick in 1..INTEREST_AUTHORITATIVE_BACKSTOP_TICKS {
        sweep_once(&mut fix, false).await;
        assert!(
            !fix.hub.interest_authoritative,
            "the backstop fired early, on tick {tick} of \
             {INTEREST_AUTHORITATIVE_BACKSTOP_TICKS}"
        );
    }

    sweep_once(&mut fix, false).await;
    assert!(
        fix.hub.interest_authoritative,
        "the interest-authoritative backstop did not fire at \
         INTEREST_AUTHORITATIVE_BACKSTOP_TICKS ({INTEREST_AUTHORITATIVE_BACKSTOP_TICKS}): \
         it is still counting against the reconcile cadence (#613 item 2.6)"
    );
}

// ---------------------------------------------------------------------------
// Item 2.2 follow-up — the two liveness holes gating the periodic scan opened.
//
// `settle_pending_publishes` had exactly one production caller (the tail of
// `inherit_sessions`), and `last_scan_complete` has exactly one writer (the
// same function). Both therefore depended on a scan LANDING. Item 2.2 gated the
// only periodic arm that spawns one on `durable_plane.is_some()`, which on a
// clustered NON-DURABLE node (scale-rig lanes B/C; any cluster on clean
// sessions) leaves a held PUBACK with nothing to release it — for the life of
// the process, not for 30 seconds.
// ---------------------------------------------------------------------------

/// A held ack must retire on the SWEEP once the window closes, even though no
/// scan ever lands again.
///
/// The publish is registered while the view is unsettled, so it carries
/// `ack_awaits_settle`. The window is then closed by hand — every term of
/// `routing_unsettled()` cleared — without a scan, which is reachable in
/// production: the item 2.6 backstop forces `interest_authoritative` true on
/// its own deadline and neither spawns a scan nor arms the takeover window.
///
/// Fails before this fix: `sweep_expired_sessions` never called
/// `settle_pending_publishes`, so with no durable plane and the takeover window
/// spent there was no caller left at all and `try_recv` still reads `Empty`.
#[tokio::test]
async fn a_held_ack_retires_on_the_sweep_when_the_window_closes_without_a_scan() {
    let mut fix = clustered_fixture();
    assert!(
        fix.hub.durable_plane.is_none(),
        "fixture invariant: no durable plane, so the periodic scan arm is gated"
    );
    assert!(fix.hub.routing_unsettled());

    let mut ack = fix.publish_gated("settle/nobody").await;
    let id = fix.only_pending();
    assert!(
        held(&mut ack) && fix.hub.pending_publishes[&id].ack_awaits_settle,
        "fixture invariant: a zero-match publish must start out held"
    );

    // Close every term of `routing_unsettled()` WITHOUT landing a scan.
    fix.hub.takeover_reconcile_ticks = 0;
    fix.hub.interest_authoritative = true;
    fix.hub.last_scan_complete = true;
    assert!(
        !fix.hub.routing_unsettled(),
        "fixture invariant: the window must actually be closed"
    );

    let before = scans(&fix);
    sweep_once(&mut fix, true).await;
    assert_eq!(
        scans(&fix),
        before,
        "fixture invariant: this test is about the sweep releasing the hold, so \
         no scan may run — if one did, it would release the ack for the wrong \
         reason and the assertion below would not discriminate"
    );

    assert_eq!(
        ack.try_recv(),
        Ok(PublishOutcome::Accepted),
        "the routing view settled and a full sweep tick ran, but the held ack \
         was never released: `settle_pending_publishes` has no driver other \
         than a landing scan, and item 2.2 gated the only periodic one off on a \
         node with no durable plane (#613 item 2.2 follow-up)"
    );
    assert!(
        !fix.hub.pending_publishes.contains_key(&id),
        "both holds clear and no obligation outstanding: the entry retires"
    );
}

/// An INCOMPLETE scan must still be retried on a node with no durable plane.
///
/// `last_scan_complete` is seeded `false`, is a term of `routing_unsettled()`,
/// and is written only when a scan lands. So a boot scan that lands incomplete
/// (a store enumeration that errored, a quorum read that timed out) can only be
/// repaired by another scan. Item 2.2 removed the retry; the liveness half of
/// its gate puts it back without paying for it on a healthy node.
///
/// Fails before this fix: past the takeover window the periodic arm reads
/// `self.durable_plane.is_some()` alone, so the count never moves again and the
/// node routes with a permanently unsettled view.
#[tokio::test]
async fn an_incomplete_scan_is_retried_on_a_node_with_no_durable_plane() {
    let mut fix = clustered_fixture();
    assert!(fix.hub.durable_plane.is_none());

    // Spend the takeover window.
    let window = usize::from(fix.hub.takeover_reconcile_ticks);
    assert!(window > 0, "fixture invariant: a boot window to spend");
    for _ in 0..window {
        sweep_once(&mut fix, true).await;
    }
    let after_window = scans(&fix);

    // Now put the hub in the state a scan that landed INCOMPLETE leaves behind.
    // Set directly rather than driven: `inherit_sessions` writes exactly this
    // one field from its `complete` argument (hub/mod.rs, `last_scan_complete`),
    // and this fixture's `MemorySessionStore::all_sessions` can never answer
    // anything but complete — so driving it would mean substituting a failing
    // store, which tests the store, not the gate.
    fix.hub.last_scan_complete = false;
    assert!(
        fix.hub.routing_unsettled(),
        "fixture invariant: an incomplete scan leaves the view unsettled with \
         no other term able to clear it"
    );

    // Past the reconcile cadence, with the window spent.
    for _ in window..(EXPIRY_RECONCILE_EVERY as usize + 10) {
        sweep_once(&mut fix, false).await;
    }
    assert!(
        scans(&fix) > after_window,
        "a clustered node whose scan landed INCOMPLETE never retried it: \
         `last_scan_complete` is written only by a landing scan and is a term \
         of `routing_unsettled()`, so every zero-match gated publish is held \
         for the life of the process (#613 item 2.2 follow-up)"
    );

    // And the retry is what repairs the view.
    sweep_once(&mut fix, true).await;
    assert!(
        fix.hub.last_scan_complete && !fix.hub.routing_unsettled(),
        "a scan that lands COMPLETE must settle the view again"
    );
}

/// The healthy case item 2.2 was actually about is unchanged: a settled,
/// non-durable node still never pays for `all_sessions()`.
///
/// This is the control for the test above — without it, the liveness condition
/// could be widened to "always" and nothing would notice.
#[tokio::test]
async fn a_settled_node_with_no_durable_plane_still_skips_the_periodic_scan() {
    let mut fix = clustered_fixture();
    fix.hub.takeover_reconcile_ticks = 0;
    fix.hub.interest_authoritative = true;
    fix.hub.last_scan_complete = true;
    assert!(!fix.hub.routing_unsettled());

    let before = scans(&fix);
    for _ in 0..(EXPIRY_RECONCILE_EVERY as usize + 10) {
        sweep_once(&mut fix, true).await;
    }
    assert_eq!(
        scans(&fix),
        before,
        "item 2.2's saving was given back: a SETTLED node with no durable plane \
         paid for a periodic `all_sessions()` enumeration that cannot find \
         anything it does not already hold"
    );
}
