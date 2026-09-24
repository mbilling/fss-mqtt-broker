//! The pending-publish ADMISSION policy, proved END TO END (issue #613 wave A,
//! items 2.4 and 2.5).
//!
//! WHY THIS FILE EXISTS ALONGSIDE `zone_fwd_proofs`. Zone FWD's tests in
//! `hub/forwarding.rs` call `register_pending` directly, which is the right way
//! to pin the *policy* (which entry is the victim, who is answered what). They
//! structurally cannot prove the thing a publisher actually cares about: that a
//! refusal taken at the cap is EFFECT-FREE — the message was not delivered to a
//! local subscriber, not handed to a peer, and not retained. A test that never
//! calls `publish()` cannot observe side effects `publish()` would have had.
//! These tests drive the real `Hub` loop over `HubCommand`, so the early
//! `return` in the `HubCommand::Publish` arm (hub/mod.rs, "The early `return` is
//! load-bearing") is inside the thing under test rather than assumed.
//!
//! WHY IN-CRATE AND NOT `crates/mqttd/tests/`. `PENDING_PUBLISH_CAP` and
//! `PENDING_PUBLISH_MAX_AGE` are private `const`s in `hub/mod.rs`; the seam did
//! not export them. An integration test would have to hard-code 4096 and 120s,
//! which is a second source of truth for the very bound under test — a cap read
//! in one place and enforced in another would pass. A descendant module of `hub`
//! sees them for free and can never drift from the policy it tests.
//!
//! DETERMINISM. No wall clock anywhere. Ordering comes from two facts the hub
//! already guarantees: `HubCommand` rides one FIFO `mpsc::UnboundedSender`, and
//! `HubCommand::Ping { reply }` is answered from inside `dispatch`, so when
//! [`ping`] resolves every earlier command has been fully processed. Ack state
//! is then read with `oneshot::Receiver::try_recv()` — a non-blocking three-way
//! read (`Ok` / `Empty` / `Closed`), never a race against a timer. The one test
//! that needs age uses `tokio::time::advance` on a hub that is NOT spawned, so
//! no sweep can run across the advance at all.

use super::*;
use crate::hub::{
    Refusal311, PENDING_PUBLISH_CAP, PENDING_PUBLISH_MAX_AGE, SESSION_SWEEP_INTERVAL,
};

/// The topic the local subscriber and the retained store are observed on.
const PROBE: &str = "cap/probe";
/// The peer's interest filter — covers `PROBE` and every fill topic, so one
/// peer holds every entry in the ledger open.
const PEER_FILTER: &str = "cap/#";

/// What the publisher's connection would see, read WITHOUT awaiting — so "still
/// pending" is an observable state rather than a hang.
#[derive(Debug, PartialEq, Eq)]
enum Ack {
    /// Neither acked nor answered: what a correctly admitted gated publish looks
    /// like while its obligations are open.
    Pending,
    /// A terminal answer the publisher is TOLD.
    Outcome(PublishOutcome),
    /// The sender was dropped. `conn.rs` turns this into a close with no PUBACK,
    /// and the publisher retries. This is what the OLD cap did to the oldest
    /// entry on every overrun.
    Withheld,
}

fn ack(w: &mut oneshot::Receiver<PublishOutcome>) -> Ack {
    match w.try_recv() {
        Ok(outcome) => Ack::Outcome(outcome),
        Err(oneshot::error::TryRecvError::Empty) => Ack::Pending,
        Err(oneshot::error::TryRecvError::Closed) => Ack::Withheld,
    }
}

/// A spawned hub with metrics attached and one peer that never answers a
/// forward.
///
/// That peer is the whole fixture, and it needs no timer. A gated `QoS` 1
/// publish on a topic a peer announced interest in records an acked-forward
/// obligation (`forward_to_peers` -> `send_acked_forward` -> `register_forward`),
/// and `try_complete_pending` will not retire the entry while `p.awaiting` is
/// non-empty. `sweep_pending_forwards` only RETRANSMITS an outstanding forward;
/// its completion paths are gated on `reroute_grace`, which stays `None` unless
/// a peer DIES. So every publish parks in `pending_publishes` until the test
/// ends, and the ledger can be driven to exactly `PENDING_PUBLISH_CAP`.
struct Rig {
    tx: HubTx,
    peer_rx: mpsc::UnboundedReceiver<PeerMessage>,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Rig {
    async fn new() -> Self {
        let (mut hub, tx) = Hub::with_config(
            NodeId("admission-cap".into()),
            Arc::new(MemorySessionStore::new()),
        );
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("admission"));
        hub.attach_metrics(Arc::clone(&metrics));
        let task = tokio::spawn(hub.run());
        let peer_rx = connect_peer(&tx, "sink", 1);
        remote_interest(&tx, "sink", &[PEER_FILTER]);
        let rig = Self {
            tx,
            peer_rx,
            metrics,
            task,
        };
        ping(&rig.tx).await;
        rig
    }

    /// Submit one GATED v5 `QoS` 1 publish. The returned receiver IS the
    /// publisher's ack channel — the same `oneshot` `conn.rs` waits on to choose
    /// between a PUBACK, a `0x97` and a close.
    fn publish_gated(
        &self,
        topic: &str,
        payload: &str,
        retain: bool,
    ) -> oneshot::Receiver<PublishOutcome> {
        let (done, wait) = oneshot::channel();
        self.tx
            .send(HubCommand::Publish {
                topic: topic.into(),
                payload: Bytes::from(payload.to_owned()),
                qos: QoS::AtLeastOnce,
                retain,
                message_expiry: None,
                app: AppProperties::default(),
                done: Some(done),
                v5: true,
                publisher: None,
            })
            .unwrap();
        wait
    }

    /// Fill the ledger with `n` gated publishes on distinct fill topics, which
    /// no local subscriber matches — the ledger depth is the only thing they are
    /// for.
    fn fill(&self, n: usize) -> Vec<oneshot::Receiver<PublishOutcome>> {
        (0..n)
            .map(|i| self.publish_gated(&format!("cap/fill/{i}"), "fill", false))
            .collect()
    }

    fn render(&self) -> String {
        self.metrics.render()
    }

    /// Every payload the hub has handed to the peer so far. Call only AFTER a
    /// ping. De-duplicated: the sweep may retransmit an outstanding obligation,
    /// and a retransmit is not a new forward.
    fn forwarded_payloads(&mut self) -> HashSet<String> {
        let mut seen = HashSet::new();
        while let Ok(frame) = self.peer_rx.try_recv() {
            let payload = match &frame {
                PeerMessage::PublishAcked { payload, .. }
                | PeerMessage::Publish { payload, .. } => payload.clone(),
                _ => continue, // Interest / digests: control only.
            };
            seen.insert(String::from_utf8_lossy(&payload).into_owned());
        }
        seen
    }
}

/// Every publish payload this subscriber has been handed.
fn delivered_payloads(rx: &mut mpsc::UnboundedReceiver<Box<Packet>>) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(pkt) = rx.try_recv() {
        if let Packet::Publish(p) = *pkt {
            out.push(String::from_utf8_lossy(&p.payload).into_owned());
        }
    }
    out
}

/// Exactly one series line, by full name, as prometheus rendered it. `0` when the
/// family carries no such label at all — which is the answer the "this counter
/// did not move" assertions want.
fn counter(metrics: &mqtt_observability::metrics::Metrics, series: &str) -> u64 {
    let prefix = format!("{series} ");
    metrics
        .render()
        .lines()
        .find_map(|l| l.strip_prefix(&prefix)?.trim().parse().ok())
        .unwrap_or(0)
}

const DROPPED_CAP: &str = "mqttd_publish_dropped_total{reason=\"pending-cap\"}";
const DROPPED_ADMISSION: &str = "mqttd_publish_dropped_total{reason=\"pending-cap-admission\"}";

// ---------------------------------------------------------------------------
// ITEM 2.4 — the headline, both halves, through the real hub loop.
// ---------------------------------------------------------------------------

/// At `PENDING_PUBLISH_CAP` the NEW publish is refused and the OLDEST entry is
/// STILL PENDING.
///
/// BOTH halves are asserted on purpose. "The new one was refused" alone is
/// satisfied by an implementation that evicts the oldest AND refuses the new one
/// — strictly worse than today's behaviour. "The oldest survived, unanswered" is
/// the half that proves the policy actually flipped, and it is the half a
/// careless implementation gets wrong.
///
/// Note the distinction `Ack::Withheld` vs `Ack::Pending` carries: a withhold is
/// a DROPPED sender, which `conn.rs` turns into a close with no PUBACK. That is
/// precisely what the old `pop_first()` did to whoever had published FIRST.
#[tokio::test]
async fn at_the_cap_the_new_publish_is_refused_and_the_oldest_is_still_pending() {
    let rig = Rig::new().await;
    let mut held = rig.fill(PENDING_PUBLISH_CAP);
    ping(&rig.tx).await;

    // Vacuity guard: the fixture really did reach the cap with everything still
    // waiting. Without this, every assertion below could pass on an empty ledger.
    for (i, w) in held.iter_mut().enumerate() {
        assert_eq!(
            ack(w),
            Ack::Pending,
            "entry {i} did not stay pending, so the ledger never reached the cap \
             and this test would be vacuous"
        );
    }
    assert_eq!(
        counter(&rig.metrics, DROPPED_CAP),
        0,
        "filling TO the cap must evict nothing"
    );

    // One more. This is the whole experiment.
    let mut over = rig.publish_gated("cap/fill/over", "over", false);
    ping(&rig.tx).await;

    // HALF ONE — the oldest survived, and so did every other admitted entry: a
    // cap that evicts *some* other entry is the same defect wearing a different
    // index.
    assert_eq!(
        ack(&mut held[0]),
        Ack::Pending,
        "the OLDEST unacknowledged publish was answered by an arrival it had \
         nothing to do with; it may already be stored, so its publisher now \
         retries and duplicates"
    );
    for (i, w) in held.iter_mut().enumerate() {
        assert_eq!(ack(w), Ack::Pending, "admitted entry {i} lost its ack");
    }

    // HALF TWO — the NEW publish was refused, with a reason the publisher can
    // act on rather than a silent withhold.
    assert_eq!(
        ack(&mut over),
        Ack::Outcome(PublishOutcome::Refused(PublishRefusal::PendingCap)),
        "the publish over the cap was not refused"
    );
    // The two protocol answers, at the hub's own vocabulary: v5 gets 0x97, and
    // v3.1.1 — which has no reason byte — must see a close, because a plain
    // PUBACK would claim a message the broker did not take.
    assert_eq!(
        PublishRefusal::PendingCap.v5_reason(),
        mqtt_codec::reason::QUOTA_EXCEEDED
    );
    assert_eq!(PublishRefusal::PendingCap.v311(), Refusal311::CloseNoAck);

    // ITEM 2.5 — the refusal is counted, and counted APART from the eviction
    // backstop, so an operator can tell "we are shedding new work" from "we are
    // dropping work we already took".
    assert_eq!(
        counter(&rig.metrics, DROPPED_ADMISSION),
        1,
        "{}",
        rig.render()
    );
    assert_eq!(
        counter(&rig.metrics, DROPPED_CAP),
        0,
        "nothing was evicted, so the eviction counter must not have moved: {}",
        rig.render()
    );
}

// ---------------------------------------------------------------------------
// ITEM 2.4 — the refusal is EFFECT-FREE, end to end.
// ---------------------------------------------------------------------------

/// `refuse_pending`'s contract, and `Refused`'s positive claim, is "nothing of
/// this publish was stored ANYWHERE, so retry". This test asserts that claim
/// against the three places a publish can leave a mark: a local subscriber, a
/// peer link, and the retained store.
///
/// It is written so that a future change which stores before admitting FAILS
/// here rather than lying to a publisher:
///
///   * register-then-refuse: `refuse_pending`'s `p.stored || appends_outstanding
///     > 0` guard downgrades the answer to a WITHHOLD, and the `Ack::Outcome`
///     assertion fails with `Ack::Withheld`;
///   * register, fan out, then refuse with nothing durably stored: the refusal
///     is contract-honest but a subscriber, a peer and the retained store have
///     all already seen it — the three `!contains` assertions fail.
///
/// The vacuity problem is handled by running the SAME publish twice, differing
/// in exactly one variable: whether the ledger is at the cap. The first copy
/// must be seen everywhere; the second must be seen nowhere.
#[tokio::test]
async fn a_publish_refused_at_the_cap_is_not_delivered_forwarded_or_retained() {
    let mut rig = Rig::new().await;
    let (mut sub_rx, _) = attach(&rig.tx, "watcher", 10, true).await;
    subscribe(&rig.tx, "watcher", PROBE);
    ping(&rig.tx).await;

    // CONTROL. The identical publish, below the cap. Everything it touches, it
    // must touch — otherwise the assertions after the cap prove nothing.
    let mut control = rig.publish_gated(PROBE, "accepted", true);
    ping(&rig.tx).await;
    assert_eq!(
        delivered_payloads(&mut sub_rx),
        vec!["accepted".to_string()],
        "the control publish never reached the local subscriber; the \
         not-delivered assertion below would be vacuous"
    );
    assert!(
        rig.forwarded_payloads().contains("accepted"),
        "the control publish never reached the peer; the not-forwarded \
         assertion below would be vacuous"
    );
    assert_eq!(
        ack(&mut control),
        Ack::Pending,
        "the control publish must stay pending, so it holds a ledger slot"
    );

    // Fill the rest of the ledger. The control publish already holds one slot.
    let mut held = rig.fill(PENDING_PUBLISH_CAP - 1);
    ping(&rig.tx).await;
    for (i, w) in held.iter_mut().enumerate() {
        assert_eq!(ack(w), Ack::Pending, "fill entry {i} did not hold a slot");
    }

    // THE EXPERIMENT. Same topic, same retain flag, same subscriber, same peer.
    let mut refused = rig.publish_gated(PROBE, "refused", true);
    ping(&rig.tx).await;

    assert_eq!(
        ack(&mut refused),
        Ack::Outcome(PublishOutcome::Refused(PublishRefusal::PendingCap)),
        "a withhold here would mean something MIGHT have been stored; the \
         publisher must be told, with 0x97"
    );

    // (1) NOT DELIVERED.
    assert_eq!(
        delivered_payloads(&mut sub_rx),
        Vec::<String>::new(),
        "a refused publish was delivered to a local subscriber, so \"nothing was \
         stored, retry\" is false and the retry duplicates on that subscriber"
    );
    // (2) NOT FORWARDED. A peer holding the message would hold a durability
    // obligation for a publish the publisher was told to retry.
    assert!(
        !rig.forwarded_payloads().contains("refused"),
        "a refused publish crossed the node boundary"
    );
    // (3) NOT RETAINED. Observed the only way a retained value can be observed
    // from outside the hub: a fresh subscriber's retained delivery. The control
    // publish's value must still be the one on file.
    let (mut late_rx, _) = attach(&rig.tx, "late", 11, true).await;
    subscribe(&rig.tx, "late", PROBE);
    ping(&rig.tx).await;
    assert_eq!(
        delivered_payloads(&mut late_rx),
        vec!["accepted".to_string()],
        "the refused publish mutated the retained store; a later subscriber is \
         being handed a message whose publisher was told it was not taken"
    );

    assert_eq!(counter(&rig.metrics, DROPPED_ADMISSION), 1);
    assert_eq!(counter(&rig.metrics, DROPPED_CAP), 0);
}

// ---------------------------------------------------------------------------
// ITEM 2.4 — the two cap conditions, separated.
// ---------------------------------------------------------------------------

/// The cap has TWO conditions and they must never be one number: a full ledger
/// of YOUNG entries refuses the ARRIVAL (`pending-cap-admission`, publisher
/// TOLD, oldest untouched), while past `PENDING_PUBLISH_MAX_AGE` the age-bounded
/// liveness backstop still evicts the oldest under the OLD `pending-cap` reason
/// (ack WITHHELD, never refused — a stuck entry may already be stored).
///
/// One ledger, two phases, differing in exactly one variable: the age of the
/// oldest entry. A single fused counter, or a policy that dropped either
/// condition, fails here.
///
/// The hub is NOT spawned for this one. `register_pending` is synchronous, so
/// calling it directly is the honest way to move virtual time across a full
/// ledger: no sweep task exists to retransmit 4096 obligations once per virtual
/// second for the 120 seconds the age bound needs. `PendingPublish::created_at`
/// is a `tokio::time::Instant`, so `advance` IS the clock it is read against.
#[tokio::test(start_paused = true)]
async fn the_admission_refusal_and_the_age_backstop_are_counted_apart() {
    let (mut hub, _tx) = Hub::with_config(
        NodeId("admission-ages".into()),
        Arc::new(MemorySessionStore::new()),
    );
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("ages"));
    hub.attach_metrics(Arc::clone(&metrics));

    let register = |hub: &mut Hub, topic: &str| {
        let (done, wait) = oneshot::channel();
        let id = hub.register_pending(
            done,
            topic,
            &Bytes::from_static(b"x"),
            QoS::AtLeastOnce,
            false,
            None,
            &AppProperties::default(),
        );
        (id, wait)
    };

    let mut held: Vec<oneshot::Receiver<PublishOutcome>> = (0..PENDING_PUBLISH_CAP)
        .map(|i| {
            let (id, wait) = register(&mut hub, &format!("fill/{i}"));
            assert!(id.is_some(), "the fill loop must be admitted");
            wait
        })
        .collect();
    assert_eq!(hub.pending_publishes.len(), PENDING_PUBLISH_CAP);
    let oldest = *hub.pending_publishes.keys().next().expect("full ledger");

    // PHASE 1 — every entry YOUNG. The arrival pays, nothing is evicted.
    let (id, mut young_arrival) = register(&mut hub, "arriving/young");
    assert!(id.is_none(), "a young full ledger must refuse the arrival");
    assert_eq!(
        ack(&mut young_arrival),
        Ack::Outcome(PublishOutcome::Refused(PublishRefusal::PendingCap))
    );
    assert!(
        hub.pending_publishes.contains_key(&oldest),
        "the oldest entry was evicted although it was young"
    );
    assert_eq!(ack(&mut held[0]), Ack::Pending, "the oldest lost its ack");
    assert_eq!(
        counter(&metrics, DROPPED_ADMISSION),
        1,
        "{}",
        metrics.render()
    );
    assert_eq!(
        counter(&metrics, DROPPED_CAP),
        0,
        "a refusal is not an eviction; the two conditions were summed: {}",
        metrics.render()
    );

    // PHASE 2 — the same ledger, now AGED past the bound. Exactly one variable
    // changed, and the verdict flips to the backstop.
    tokio::time::advance(PENDING_PUBLISH_MAX_AGE + SESSION_SWEEP_INTERVAL).await;
    let (id, mut aged_arrival) = register(&mut hub, "arriving/aged");
    assert!(
        id.is_some(),
        "the liveness backstop must admit the arrival; without it a ledger of \
         permanently stuck entries wedges the node shut forever"
    );
    assert_eq!(
        ack(&mut aged_arrival),
        Ack::Pending,
        "once the backstop fires the arrival must NOT be refused"
    );
    assert!(
        !hub.pending_publishes.contains_key(&oldest),
        "the abandoned oldest entry was not evicted"
    );
    assert_eq!(
        ack(&mut held[0]),
        Ack::Withheld,
        "the evicted entry's ack is WITHHELD, never Refused — it may already be \
         stored, so nothing may be claimed about it"
    );

    // The two reasons moved independently, exactly once each.
    assert_eq!(counter(&metrics, DROPPED_CAP), 1, "{}", metrics.render());
    assert_eq!(
        counter(&metrics, DROPPED_ADMISSION),
        1,
        "the backstop eviction was counted as an admission refusal: {}",
        metrics.render()
    );
    assert_eq!(hub.pending_publishes.len(), PENDING_PUBLISH_CAP);
}

// ---------------------------------------------------------------------------
// ITEM 2.5 — the ledger depth is observable.
// ---------------------------------------------------------------------------

/// Item 2.5: the pending table is on the dashboard, so the cap's behaviour under
/// load can be READ rather than inferred from refusal counts.
///
/// The `awaiting_settle` half is asserted at ZERO, which is what a standalone
/// node must always report: `routing_unsettled()` opens with `clustered()`, and
/// this rig never configures a cluster. Its non-zero case belongs to the
/// settle-gate tests that own item 2.1; this file must not fork that fixture.
#[tokio::test(start_paused = true)]
async fn the_pending_publish_gauges_report_the_ledger_depth() {
    let rig = Rig::new().await;
    let mut held = rig.fill(3);
    ping(&rig.tx).await;
    for (i, w) in held.iter_mut().enumerate() {
        assert_eq!(
            ack(w),
            Ack::Pending,
            "entry {i} completed, so the gauge would legitimately read 0"
        );
    }

    // One sweep tick refreshes the gauges off the maps. Under a paused clock the
    // sweep's one-interval deadline is strictly earlier than this sleeper's
    // two-interval one, so it has run by the time this resolves.
    tokio::time::sleep(SESSION_SWEEP_INTERVAL * 2).await;

    let out = rig.render();
    assert!(out.contains("mqttd_pending_publishes 3"), "{out}");
    assert!(
        out.contains("mqttd_pending_publishes_awaiting_settle 0"),
        "{out}"
    );
}
