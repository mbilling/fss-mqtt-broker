//! The pending-publish ledger's gauges, read off a running hub (issue #613
//! wave A, item 2.5).
//!
//! This file used to be `admission_cap.rs` and mostly proved item 2.4's
//! refuse-the-arrival policy end to end. That policy was withdrawn when the
//! branch merged the byte-bounded pending table (3571682) — see
//! `Hub::register_pending` — so its tests went with it. Main's `pending_bounds`
//! module proves the oldest-first eviction and both bounds; `zone_fwd_proofs`
//! proves the one thing 2.4 still contributes, the `pending-cap-replay` count.
//! What remains here is the dashboard half, which needs a real, spawned hub.
//!
//! DETERMINISM. No wall clock. `HubCommand` rides one FIFO channel and
//! `HubCommand::Ping` is answered from inside `dispatch`, so when [`ping`]
//! resolves every earlier command has been processed; ack state is read with a
//! non-blocking `try_recv`; the sweep tick is driven under a paused clock.

use super::*;
use crate::hub::SESSION_SWEEP_INTERVAL;

/// The peer's interest filter. Every fill topic falls under it, so one silent
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
    /// Held, never read: dropping it would close the link the peer interest
    /// rides on, and the forward obligations that keep entries open with it.
    _peer_rx: mpsc::UnboundedReceiver<PeerMessage>,
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
            NodeId("pending-gauges".into()),
            Arc::new(MemorySessionStore::new()),
        );
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("pending-gauges"));
        hub.attach_metrics(Arc::clone(&metrics));
        let task = tokio::spawn(hub.run());
        let peer_rx = connect_peer(&tx, "sink", 1);
        remote_interest(&tx, "sink", &[PEER_FILTER]);
        let rig = Self {
            tx,
            _peer_rx: peer_rx,
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
