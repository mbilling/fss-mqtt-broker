//! A held delivery's obligation, settled off the hub loop (ADR 0092 T4, D3).
//!
//! ADR 0089 sends a `QoS` 1 delivery to an online persistent session at once and holds
//! the publisher's obligation for a window instead of writing first: the subscriber's
//! PUBACK inside the window discharges it with no write. Until T4 that discharge was a
//! hub command per PUBACK — the hold queue and the backoff looked up cold, a histogram,
//! the publisher's wake, all on the loop — for a decision the subscriber's connection
//! task can take itself. Now the hub hands the connection task a [`Hold`] with the held
//! packet; the task settles it when the PUBACK comes and fires the publisher's answer
//! through the [`PublishGate`] the publish's pending entry shares with every hold of its
//! fan-out, once the hub has armed the gate with nothing else owed. The hub keeps its
//! hold queue for the window's fallback writes and drops a settled hold when the ack
//! batch reaches it.
//!
//! Who settles a hold is decided once, by a compare-and-swap on its state: the
//! connection task's PUBACK, or the hub's fallback write at the window's end, never
//! both. A hold of a peer's forward, or of a delivery nobody waits on, has no gate and
//! is settled on the hub as before: the forward's verdict is the hub's to send.
//!
//! Measured once the other way (run `h3-20261101T105009Z`): a gate that told the hub
//! of every firing through a channel, and holds pruned at their deadlines, moved the
//! ack's work out of the ack dispatch and into the loop's own passes — the knee fell.
//! So the hub retires an armed entry at the arming, and the hold leaves the queue with
//! the ack batch, on the loop's existing per-ack work.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use mqtt_observability::metrics::Metrics;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::PublishOutcome;

/// Nothing taken from the entry yet: its own `done` answers the publisher.
const UNARMED: u8 = 0;
/// The hub has nothing else to wait for and handed the gate the answer; the last hold
/// settled fires it, wherever that happens.
const ARMED: u8 = 1;
/// The answer was sent, or dropped (a withhold).
const FIRED: u8 = 2;

/// A pending publish's held deliveries: how many are still owed, and — once the hub has
/// armed it — the publisher's answer, fired by the last one settled.
#[derive(Debug)]
pub struct PublishGate {
    /// The pending publish.
    id: u64,
    /// Holds not yet discharged, and fallback writes not yet landed.
    remaining: AtomicUsize,
    /// Holds a PUBACK discharged: a delivered, acknowledged copy exists, so a refusal
    /// may only withhold (issue #238).
    discharged: AtomicUsize,
    state: AtomicU8,
    done: Mutex<Option<oneshot::Sender<PublishOutcome>>>,
    /// The hub kept the entry (a settle window has a claim on it): the gate tells the
    /// hub when it fired, so the entry is retired. Otherwise the entry went at the
    /// arming and nothing is sent.
    keep: AtomicBool,
    created_at: Instant,
    retire: mpsc::UnboundedSender<u64>,
    /// The node's gates armed and not yet fired: a drain barrier waits for them.
    armed: Arc<AtomicUsize>,
    metrics: Option<Arc<Metrics>>,
}

impl PublishGate {
    pub(super) fn new(
        id: u64,
        created_at: Instant,
        retire: mpsc::UnboundedSender<u64>,
        armed: Arc<AtomicUsize>,
        metrics: Option<Arc<Metrics>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            remaining: AtomicUsize::new(0),
            discharged: AtomicUsize::new(0),
            state: AtomicU8::new(UNARMED),
            done: Mutex::new(None),
            keep: AtomicBool::new(false),
            created_at,
            retire,
            armed,
            metrics,
        })
    }

    /// One more held delivery owed.
    pub(super) fn register(&self) {
        self.remaining.fetch_add(1, Ordering::SeqCst);
    }

    /// Whether a hold or a fallback write is still owed.
    pub(super) fn pending(&self) -> bool {
        self.remaining.load(Ordering::SeqCst) > 0
    }

    /// How many holds a PUBACK discharged.
    pub(super) fn discharged(&self) -> usize {
        self.discharged.load(Ordering::SeqCst)
    }

    /// Whether the gate holds the publisher's answer and has not fired it.
    pub(super) fn armed(&self) -> bool {
        self.state.load(Ordering::SeqCst) == ARMED
    }

    /// The hub has nothing else to wait for: the gate takes the answer, and fires it at
    /// once if nothing is owed any more. `keep` says the hub kept the entry and wants
    /// to hear of the firing.
    pub(super) fn arm(&self, done: oneshot::Sender<PublishOutcome>, keep: bool) {
        {
            let mut slot = self.done.lock().expect("gate lock");
            *slot = Some(done);
            self.keep.store(keep, Ordering::SeqCst);
            self.armed.fetch_add(1, Ordering::SeqCst);
            self.state.store(ARMED, Ordering::SeqCst);
        }
        // Checked AFTER the answer is in place: a discharge that reached zero before it
        // found nothing to fire, and this is where it fires instead.
        if !self.pending() {
            self.try_fire();
        }
    }

    /// The hub answers through the gate (a refusal decided while armed). `false` when
    /// the gate holds no answer: not armed, or fired already.
    pub(super) fn answer(&self, outcome: PublishOutcome) -> bool {
        match self.take() {
            Some(tx) => {
                let _ = tx.send(outcome);
                true
            }
            None => false,
        }
    }

    /// Withhold: the answer, if the gate holds it, is dropped unsent (the sender side of
    /// fail-closed), and no later discharge can fire it.
    pub(super) fn cancel(&self) {
        drop(self.take());
    }

    /// A hold's fallback write landed: that copy is owed no more.
    pub(super) fn write_landed(&self) {
        self.settle_one();
    }

    /// A hold's PUBACK discharged it: a delivered, acknowledged copy.
    pub(crate) fn discharge(&self) {
        self.discharged.fetch_add(1, Ordering::SeqCst);
        self.settle_one();
    }

    fn settle_one(&self) {
        let before = self
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
        if before == Ok(1) {
            self.try_fire();
        }
    }

    /// Take the answer out of an armed gate: the gate has fired, whatever is done with
    /// it; the node's armed count and the kept entry's retirement follow.
    fn take(&self) -> Option<oneshot::Sender<PublishOutcome>> {
        let taken = self.done.lock().expect("gate lock").take();
        if taken.is_some() {
            self.state.store(FIRED, Ordering::SeqCst);
            self.armed.fetch_sub(1, Ordering::SeqCst);
            if self.keep.load(Ordering::SeqCst) {
                let _ = self.retire.send(self.id);
            }
        }
        taken
    }

    fn try_fire(&self) {
        let Some(tx) = self.take() else {
            return;
        };
        let _ = tx.send(PublishOutcome::Accepted);
        if let Some(m) = &self.metrics {
            m.observe_publish_ack(self.created_at.elapsed().as_secs_f64());
        }
    }
}

/// On the wire, obligation held.
const HELD: u8 = 0;
/// The subscriber acknowledged it inside the window: discharged, never written.
const ACKED: u8 = 1;
/// The window ran out (or the session detached): the hub wrote it.
const WRITTEN: u8 = 2;

/// One held delivery, shared between the hub's hold queue and the subscriber's
/// connection task.
#[derive(Debug)]
pub struct Hold {
    state: AtomicU8,
    /// The publish waiting on it; `None` settles on the hub (a peer's forward).
    gate: Option<Arc<PublishGate>>,
    /// When it went on the wire: the hold time a discharge reports.
    sent: Instant,
}

impl Hold {
    pub(super) fn new(gate: Option<Arc<PublishGate>>) -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU8::new(HELD),
            gate,
            sent: Instant::now(),
        })
    }

    /// The subscriber's PUBACK, on its connection task: a hold with a gate is
    /// discharged here and the gate told. One with no gate is the hub's; one the
    /// window ran out on was written, and the ack is a plain ack.
    pub(crate) fn acked(&self, metrics: Option<&Metrics>) {
        let Some(gate) = &self.gate else {
            return;
        };
        if !self.settle(ACKED) {
            return;
        }
        if let Some(m) = metrics {
            m.durable_elided(self.sent.elapsed().as_secs_f64());
        }
        gate.discharge();
    }

    /// The hub's own discharge: a hold with no gate, or an ack that reached the loop
    /// first. The caller settles the obligation.
    pub(super) fn acked_on_hub(&self) -> bool {
        self.settle(ACKED)
    }

    /// The window ran out: the hub writes it, unless the PUBACK came first.
    pub(super) fn take_for_write(&self) -> bool {
        self.settle(WRITTEN)
    }

    pub(super) fn sent(&self) -> Instant {
        self.sent
    }

    pub(super) fn gate(&self) -> Option<&Arc<PublishGate>> {
        self.gate.as_ref()
    }

    fn settle(&self, to: u8) -> bool {
        self.state
            .compare_exchange(HELD, to, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

/// A gate and a hold as the hub would hand them to a connection task, for the
/// connection's own tests: the gate armed with `done`, one hold registered on it. The
/// counter is the node's armed gates: one until the hold settles.
#[cfg(test)]
pub(crate) fn armed_hold_for_test(
    done: Option<oneshot::Sender<PublishOutcome>>,
) -> (Arc<Hold>, Arc<AtomicUsize>) {
    let (retire_tx, _retire_rx) = mpsc::unbounded_channel();
    let armed = Arc::new(AtomicUsize::new(0));
    let gate = done.map(|done| {
        let g = PublishGate::new(1, Instant::now(), retire_tx, armed.clone(), None);
        g.register();
        g.arm(done, false);
        g
    });
    (Hold::new(gate), armed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate() -> (
        Arc<PublishGate>,
        mpsc::UnboundedReceiver<u64>,
        Arc<AtomicUsize>,
    ) {
        let (retire_tx, retire_rx) = mpsc::unbounded_channel();
        let armed = Arc::new(AtomicUsize::new(0));
        (
            PublishGate::new(7, Instant::now(), retire_tx, armed.clone(), None),
            retire_rx,
            armed,
        )
    }

    #[test]
    fn an_armed_gate_fires_on_the_last_discharge_and_only_then() {
        let (g, mut retired, armed) = gate();
        g.register();
        g.register();
        let (tx, mut rx) = oneshot::channel();
        g.arm(tx, false);
        assert!(g.armed());
        assert_eq!(armed.load(Ordering::SeqCst), 1);
        g.discharge();
        assert!(rx.try_recv().is_err(), "one of two holds is still owed");
        g.discharge();
        assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));
        assert!(!g.armed());
        assert_eq!(armed.load(Ordering::SeqCst), 0);
        assert!(
            retired.try_recv().is_err(),
            "the entry went at the arming: nothing to retire"
        );
        assert!(!g.answer(PublishOutcome::Accepted), "fired once");
    }

    #[test]
    fn a_kept_entry_is_told_of_the_firing() {
        let (g, mut retired, _armed) = gate();
        g.register();
        let (tx, mut rx) = oneshot::channel();
        g.arm(tx, true);
        g.discharge();
        assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));
        assert_eq!(retired.try_recv().ok(), Some(7));
    }

    #[test]
    fn a_gate_discharged_before_it_is_armed_fires_at_the_arming() {
        let (g, _retired, armed) = gate();
        g.register();
        g.discharge();
        assert!(!g.pending());
        assert_eq!(g.discharged(), 1);
        let (tx, mut rx) = oneshot::channel();
        g.arm(tx, false);
        assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));
        assert_eq!(armed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_fallback_write_landing_settles_like_a_discharge_but_counts_none() {
        let (g, _retired, _armed) = gate();
        g.register();
        let (tx, mut rx) = oneshot::channel();
        g.arm(tx, false);
        g.write_landed();
        assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)));
        assert_eq!(g.discharged(), 0);
    }

    #[test]
    fn a_cancelled_gate_withholds_and_a_late_discharge_fires_nothing() {
        let (g, _retired, armed) = gate();
        g.register();
        let (tx, mut rx) = oneshot::channel();
        g.arm(tx, false);
        g.cancel();
        assert!(!g.armed());
        assert_eq!(armed.load(Ordering::SeqCst), 0);
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
        g.discharge();
        assert!(!g.pending());
    }

    #[test]
    fn the_hub_answers_through_an_armed_gate() {
        let (g, _retired, _armed) = gate();
        g.register();
        let (tx, mut rx) = oneshot::channel();
        assert!(
            !g.answer(PublishOutcome::Accepted),
            "not armed: nothing to answer with"
        );
        g.arm(tx, false);
        assert!(g.answer(PublishOutcome::Refused(
            super::super::PublishRefusal::Brownout
        )));
        assert!(matches!(rx.try_recv(), Ok(PublishOutcome::Refused(_))));
        g.discharge();
        assert!(!g.armed());
    }

    #[test]
    fn settling_below_zero_is_not_a_fire() {
        let (g, _retired, _armed) = gate();
        let (tx, mut rx) = oneshot::channel();
        g.arm(tx, false);
        assert!(
            matches!(rx.try_recv(), Ok(PublishOutcome::Accepted)),
            "nothing owed: fires"
        );
        g.write_landed();
        assert!(!g.pending());
    }

    #[test]
    fn a_hold_is_settled_once_by_whoever_comes_first() {
        let (g, _retired, _armed) = gate();
        g.register();
        let h = Hold::new(Some(g.clone()));
        h.acked(None);
        assert!(!h.take_for_write(), "acknowledged: nothing to write");
        h.acked(None);
        assert_eq!(
            g.discharged(),
            1,
            "a second ack of the same id is a plain ack"
        );
        assert!(!g.pending());
    }

    #[test]
    fn a_hold_taken_for_its_write_is_not_discharged_by_a_late_ack() {
        let (g, _retired, _armed) = gate();
        g.register();
        let h = Hold::new(Some(g.clone()));
        assert!(h.take_for_write());
        h.acked(None);
        assert!(!h.acked_on_hub());
        assert_eq!(g.discharged(), 0);
        assert!(g.pending(), "the write still owes the copy");
    }

    #[test]
    fn a_hold_without_a_gate_is_the_hubs_to_settle() {
        let h = Hold::new(None);
        h.acked(None);
        assert!(h.acked_on_hub(), "the connection task left it to the hub");
        assert!(!h.acked_on_hub());
    }
}
