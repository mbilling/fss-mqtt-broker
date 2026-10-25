//! Issue #1049: one durable session this node owns and cannot read must not hold
//! the PUBACK of every `QoS` 1 publish on the node.
//!
//! **The field shape.** After churn, one session's group could not be recovered:
//! every read of `m/{client}` and `q/{client}` returned `NoQuorum`, for over 90
//! minutes and across restarts. Each session scan therefore landed
//! `complete = false` (`ReplicatedSessionStore::all_sessions` counts any per-key
//! error but `NotOwner` as incomplete), `last_scan_complete` stayed false, and that
//! one term of `routing_unsettled()` held every gated publish that matched no
//! session the node knew — a node-wide `QoS` 1 outage that `/readyz` did not see.
//!
//! **The store is real, the fault is in the log.** The hub runs over a
//! [`ReplicatedSessionStore`] — the production session layer — on a log that
//! answers `NoQuorum` for the stuck client's two keys and nothing else, while
//! still LISTING them in `keys()`, exactly as a node that replicates a group it
//! cannot recover does. So the scan's `complete = false` and its `unread` list come
//! from the production code path, not from a hand-built `InheritedSessions`.
//!
//! **No clock.** As in `settle_gate.rs`, the hub is driven without its actor: the
//! sweep is called tick by tick and the self-queue pumped to quiescence, so a
//! "bound" is a count of sweep ticks (one tick is `SESSION_SWEEP_INTERVAL`, 1 s,
//! in production), never a sleep.

use super::settle_gate::quiesce;
use super::*;
use crate::hub::EXPIRY_RECONCILE_EVERY;
use mqtt_core::Subscription;
use mqtt_storage::logged::ReplicatedSessionStore;
use mqtt_storage::repl::{LogEntry, ReplError, ReplicatedLog};
use mqtt_storage::Offset;
use std::sync::Mutex;

/// The session whose group cannot be recovered.
const STUCK: &str = "stuck";

/// How many sweep ticks a node may take to stop one unreadable session from holding
/// its acks: ten reconcile cadences, so ten incomplete scans of the same key, about
/// five minutes in production.
///
/// Far longer than a real recovery: the reporter's transient `scan-incomplete`
/// after churn cleared in about 20 s, and a fix may want several failed scans of
/// the same key before it decides that key is stuck. Far shorter than the defect:
/// the field node held its acks for over 13 minutes before anyone looked, and the
/// key had failed for 90.
const BOUND_TICKS: u32 = 10 * EXPIRY_RECONCILE_EVERY;

/// An in-memory log on which one session's keys can never be read or written:
/// every call on them answers `NoQuorum`, the error recovery of the field key kept
/// returning. The keys stay in [`keys`](ReplicatedLog::keys), because the node still
/// holds a copy of the group it cannot recover.
#[derive(Debug)]
struct StuckLog {
    inner: Arc<InMemoryReplicatedLog>,
    stuck: Arc<Mutex<HashSet<String>>>,
}

impl StuckLog {
    fn check(&self, key: &str) -> Result<(), ReplError> {
        if self
            .stuck
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(key)
        {
            Err(ReplError::NoQuorum)
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl ReplicatedLog for StuckLog {
    type Key = String;

    async fn append(&self, key: &String, record: Vec<u8>) -> Result<Offset, ReplError> {
        self.check(key)?;
        self.inner.append(key, record).await
    }

    async fn read(
        &self,
        key: &String,
        after: Offset,
        limit: usize,
    ) -> Result<Vec<LogEntry>, ReplError> {
        self.check(key)?;
        self.inner.read(key, after, limit).await
    }

    async fn live_range(&self, key: &String) -> Result<Option<(Offset, Offset)>, ReplError> {
        self.check(key)?;
        self.inner.live_range(key).await
    }

    async fn truncate(&self, key: &String, up_to: Offset) -> Result<(), ReplError> {
        self.check(key)?;
        self.inner.truncate(key, up_to).await
    }

    async fn truncate_durable(&self, key: &String, up_to: Offset) -> Result<(), ReplError> {
        self.check(key)?;
        self.inner.truncate_durable(key, up_to).await
    }

    async fn remove(&self, key: &String) -> Result<(), ReplError> {
        self.check(key)?;
        self.inner.remove(key).await
    }

    async fn keys(&self) -> Result<Vec<String>, ReplError> {
        self.inner.keys().await
    }
}

/// The lines logged at WARN and above while the guard lives, without timestamps.
///
/// A thread-local default subscriber is enough: `#[tokio::test]` runs one thread,
/// so the scan task the hub spawns logs here too.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Warnings {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Warnings {
    fn capture(&self) -> tracing::subscriber::DefaultGuard {
        let writer = self.clone();
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(move || writer.clone())
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .without_time()
                .finish(),
        )
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(
            &self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .into_owned()
    }
}

/// A CLUSTERED hub with no placement (so it owns every session, and the mesh terms
/// of `routing_unsettled()` are out of the picture — see `settle_gate.rs`'s
/// `clustered_fixture`), over the real replicated session store, holding one
/// persistent session [`STUCK`] subscribed to `stuck/#` whose keys then become
/// unreadable.
///
/// The session is written through a second store over the same log before the
/// fault is armed, so it is a well-formed stored session, not a dangling key.
/// Returns the hub and the set of keys the log refuses, so a test can let the key
/// recover.
async fn stuck_fixture() -> (Hub, Arc<Mutex<HashSet<String>>>) {
    let log = Arc::new(InMemoryReplicatedLog::new());
    let seed = ReplicatedSessionStore::new(log.clone());
    let stuck = ClientId(STUCK.into());
    seed.ensure_session(&stuck).await.unwrap();
    seed.set_subscriptions(
        &stuck,
        &[Subscription {
            filter: "stuck/#".into(),
            max_qos: QoS::AtLeastOnce,
            no_local: false,
            sub_id: None,
        }],
    )
    .await
    .unwrap();
    let keys = log.keys().await.unwrap();
    assert!(
        keys.contains(&format!("m/{STUCK}")),
        "fixture invariant: the stuck session is stored ({keys:?})"
    );
    let stuck_keys = Arc::new(Mutex::new(HashSet::from([
        format!("m/{STUCK}"),
        format!("q/{STUCK}"),
    ])));
    let store = ReplicatedSessionStore::new(StuckLog {
        inner: log,
        stuck: stuck_keys.clone(),
    });
    let (mut hub, _tx) = Hub::with_config(NodeId("stuck-node".into()), Arc::new(store));
    hub.set_cluster_configured();
    (hub, stuck_keys)
}

/// One sweep tick, and the scan it spawned (if any) landed — the real scan, read
/// through the store, not a stand-in.
async fn tick(hub: &mut Hub) {
    hub.sweep_expired_sessions().await;
    quiesce(hub).await;
    assert!(
        !hub.inherited_scan_inflight,
        "fixture invariant: the scan over an in-memory log lands within one quiesce"
    );
}

/// Sweep until the first session scan has landed, and check it is the field state:
/// incomplete, because of the stuck session alone.
async fn boot_until_the_first_scan_lands(hub: &mut Hub, warnings: &Warnings) {
    for _ in 0..EXPIRY_RECONCILE_EVERY {
        tick(hub).await;
        if hub.incomplete_scans > 0 || hub.last_scan_complete {
            break;
        }
    }
    assert!(
        !hub.last_scan_complete && hub.incomplete_scans > 0,
        "fixture invariant: a scan must have landed INCOMPLETE on the stuck session, \
         or this test reproduces nothing"
    );
    assert!(
        hub.routing_unsettled(),
        "fixture invariant: the incomplete scan leaves the view unsettled"
    );
    // The operator signal #1051 added: the scan names the session and the error.
    let text = warnings.text();
    assert!(
        text.contains("session scan incomplete") && text.contains("stuck: no replication quorum"),
        "the incomplete scan must name the unreadable session and its error at WARN \
         (#1051); logged:\n{text}"
    );
}

/// **The control.** A gated `QoS` 1 publish that reaches a live subscriber is
/// acknowledged at once on the stuck node, as on any node: the fan-out is the
/// evidence (#613 item 2.1), and the stuck session takes no part in it.
///
/// Passes on main. It is here so the defect test below cannot fail for a reason
/// the fixture introduced: on this same hub, with the same stuck key and the same
/// incomplete scan, an ack that is not gated on the scan is released.
#[tokio::test]
async fn a_stuck_session_does_not_hold_the_ack_of_a_publish_that_reached_a_live_subscriber() {
    let warnings = Warnings::default();
    let _log = warnings.capture();
    let (mut hub, _stuck) = stuck_fixture().await;
    boot_until_the_first_scan_lands(&mut hub, &warnings).await;

    let (mut live, _meter, _) =
        super::settle_gate::attach_watched(&mut hub, "live", true, 0, 1, "live/topic").await;
    let mut ack = publish_gated(&mut hub, "live/topic").await;

    assert_eq!(
        ack.try_recv(),
        Ok(PublishOutcome::Accepted),
        "a publish that reached a live subscriber is acked whatever the scan says"
    );
    assert_eq!(
        super::settle_gate::published_topics(&mut live),
        vec!["live/topic".to_string()]
    );
}

/// **The other control: the hold is the scan's, and recovery is one way out.**
/// The same unmatched publish, held while the key is unreadable, is released as
/// `Accepted` once the key reads again and the next scan lands complete — the
/// maintainer's first fix shape ("the stuck key recovers"), and the proof that
/// what holds the ack in the defect test below is `last_scan_complete` and
/// nothing else in the fixture.
///
/// Passes on main.
#[tokio::test]
async fn a_held_ack_is_released_once_the_stuck_session_reads_again() {
    let warnings = Warnings::default();
    let _log = warnings.capture();
    let (mut hub, stuck) = stuck_fixture().await;
    boot_until_the_first_scan_lands(&mut hub, &warnings).await;

    let mut ack = publish_gated(&mut hub, "nobody/listens").await;
    tick(&mut hub).await;
    assert!(
        held(&mut ack),
        "held while the stuck session cannot be read"
    );

    stuck
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    let (outcome, ticks) = answer_within_the_bound(&mut hub, &mut ack).await;
    assert_eq!(
        outcome,
        Ok(PublishOutcome::Accepted),
        "the key reads again, yet the held ack was not released within {ticks} ticks \
         (last_scan_complete = {})",
        hub.last_scan_complete
    );
    assert!(hub.last_scan_complete && !hub.routing_unsettled());
}

/// **The defect.** On a node that owns one durable session it cannot read, a gated
/// `QoS` 1 publish that matches no session the node knows must be acknowledged
/// within [`BOUND_TICKS`] sweep ticks — not held for as long as that one key stays
/// unreadable.
///
/// **What "correct" can be, given the maintainer's rule (#1049).** No blind
/// backstop that releases held acks on a timer: that is the path that loses
/// messages owed to a session that is merely slow to load. The direction stated
/// is that the stuck key either recovers or is identified and quarantined loudly,
/// and that one such key stops holding every `QoS` 1 ack on its node. This log
/// never lets the key recover, so the only correct ending left is the second: the
/// node decides, after repeated failed reads of the same key, that the key is
/// stuck; says so, naming it; and stops counting it against scan completeness
/// (complete-with-quarantine). Routing then settles and the held ack is released
/// as `Accepted` through the ordinary settle pass.
///
/// Why `Accepted` and not a refusal: a refusal of every unmatched publish for as
/// long as the key is stuck is the same node-wide outage, only louder. And
/// quarantine costs the stuck session nothing it is not already losing: its queue
/// takes no append (the same `NoQuorum`), so a publish it was owed could not be
/// stored for it anyway; and the hub already acks a publish that reached any live
/// subscriber without knowing what the unreadable session subscribes to (the
/// control above).
///
/// The test does not pin HOW the key is quarantined, the bound inside
/// [`BOUND_TICKS`], or a new surface's name; only that the ack comes, and that the
/// node has named the session (the #1051 warning, asserted on the way in). Whether
/// quarantine should also show on `/statusz` is the fix's call — `/readyz` is out
/// of scope by decision (an incomplete scan is not made not-ready).
///
/// Fails on main: every scan lands `complete = false` on the stuck key,
/// `last_scan_complete` never turns true, and the ack is still held after
/// [`BOUND_TICKS`] ticks.
#[tokio::test]
#[ignore = "known defect #1049: one unreadable owned session holds every unmatched QoS 1 ack"]
async fn a_stuck_session_does_not_hold_the_ack_of_a_publish_no_session_matches() {
    let warnings = Warnings::default();
    let _log = warnings.capture();
    let (mut hub, _stuck) = stuck_fixture().await;
    boot_until_the_first_scan_lands(&mut hub, &warnings).await;

    let mut ack = publish_gated(&mut hub, "nobody/listens").await;
    let (outcome, ticks) = answer_within_the_bound(&mut hub, &mut ack).await;
    assert_eq!(
        outcome,
        Ok(PublishOutcome::Accepted),
        "after {ticks} sweep ticks ({} incomplete scans in a row, last_scan_complete = {}, \
         interest_authoritative = {}, takeover ticks = {}, routing_unsettled = {}), a QoS 1 publish that matches no known session is still \
         not acked (Err(Empty) = held, Err(Closed) = withheld): one unreadable session \
         holds every unmatched QoS 1 ack on the node (#1049)",
        hub.incomplete_scans,
        hub.last_scan_complete,
        hub.interest_authoritative,
        hub.takeover_reconcile_ticks,
        hub.routing_unsettled(),
    );
}

/// A GATED `QoS` 1 publish: `done` is `Some`, so the returned receiver is the
/// publisher's acknowledgement.
async fn publish_gated(hub: &mut Hub, topic: &str) -> oneshot::Receiver<PublishOutcome> {
    let (done, wait) = oneshot::channel();
    hub.dispatch(HubCommand::Publish {
        topic: topic.into(),
        payload: Bytes::from_static(b"stuck-node"),
        qos: QoS::AtLeastOnce,
        retain: false,
        message_expiry: None,
        app: AppProperties::default(),
        done: Some(done),
        v5: true,
        publisher: None,
        credit: None,
    })
    .await;
    quiesce(hub).await;
    wait
}

/// Sweep until the publisher's acknowledgement is answered or [`BOUND_TICKS`] ticks
/// have passed: the answer (`Err(Empty)` = still held, `Err(Closed)` = withheld, the
/// sender dropped unsent) and the ticks it took. Each read consumes an answer, so
/// the loop keeps the one it saw rather than reading again.
async fn answer_within_the_bound(
    hub: &mut Hub,
    ack: &mut oneshot::Receiver<PublishOutcome>,
) -> (Result<PublishOutcome, oneshot::error::TryRecvError>, u32) {
    let mut ticks = 0;
    loop {
        let answer = ack.try_recv();
        if ticks >= BOUND_TICKS || answer != Err(oneshot::error::TryRecvError::Empty) {
            return (answer, ticks);
        }
        tick(hub).await;
        ticks += 1;
    }
}

/// The ack is still held: the sender is alive in the pending ledger. Consumes the
/// answer if there is one.
fn held(rx: &mut oneshot::Receiver<PublishOutcome>) -> bool {
    matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty))
}
