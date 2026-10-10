//! Issue #648: a RETRANSMITTED acked forward is answered again, never applied
//! twice.
//!
//! The sender's sweep re-sends an unanswered forward under the same seq once it
//! is overdue — legitimately, when the receiver's answer is late (a durable
//! append slower than one sweep interval under load), crosses the retransmission
//! in flight, or is lost with the link. Before the fix the receiver applied
//! every copy: the message went into the subscriber's session log once per copy,
//! and replay delivered each as a first delivery with `DUP = 0` [MQTT-4.4.0-1].
//!
//! The retransmission here is GENUINE: a real sender hub records the obligation,
//! its own sweep re-sends the overdue frame, and both copies are handed to a real
//! receiver hub exactly as the peer link would. Driven without either hub's actor
//! (the `settle_gate` precedent): commands go through `Hub::dispatch`, the
//! receiver's self-queue is pumped by [`quiesce`], and no assertion reads a clock.

use super::settle_gate::quiesce;
use super::*;

const TOPIC: &str = "fr/t";
const SUB: &str = "fr-sub";

/// One sender, one receiver, and what crosses the link between them.
struct Rig {
    sender: Hub,
    /// Frames the sender put on its link to the receiver.
    to_receiver: mpsc::UnboundedReceiver<PeerMessage>,
    receiver: Hub,
    store: Arc<MemorySessionStore>,
    /// Frames the receiver put on its link back to the sender.
    to_sender: mpsc::UnboundedReceiver<PeerMessage>,
}

fn origin() -> NodeId {
    NodeId("fr-origin".into())
}

fn target() -> NodeId {
    NodeId("fr-receiver".into())
}

impl Rig {
    /// The receiver holds one OFFLINE persistent session subscribed to [`TOPIC`], so
    /// every applied copy is a durable enqueue that would replay on resume.
    async fn new() -> Self {
        let (mut sender, _) = Hub::with_config(origin(), Arc::new(MemorySessionStore::new()));
        let (tx, to_receiver) = mpsc::unbounded_channel();
        let (ctl, _) = mpsc::unbounded_channel();
        sender.peer_connected(
            target(),
            1,
            tx,
            ctl,
            None,
            mqtt_cluster::peer::PROTO_MAX,
            Arc::default(),
        );

        let store = Arc::new(MemorySessionStore::new());
        let (receiver, _) = Hub::with_config(target(), store.clone() as Arc<dyn SessionStore>);
        let (peer_tx, to_sender): (PeerOutbound, _) = mpsc::unbounded_channel();
        let mut rig = Self {
            sender,
            to_receiver,
            receiver,
            store,
            to_sender,
        };
        rig.dispatch(HubCommand::PeerConnected {
            node: origin(),
            conn_id: 1,
            ctl: peer_tx.clone(),
            tx: peer_tx,
            cert_serial: None,
            proto: mqtt_cluster::peer::PROTO_MAX,
            depth: Arc::default(),
        })
        .await;
        rig.offline_persistent_subscriber().await;
        while rig.to_sender.try_recv().is_ok() {} // link-up gossip
        rig
    }

    async fn dispatch(&mut self, cmd: HubCommand) {
        self.receiver.dispatch(cmd).await;
        quiesce(&mut self.receiver).await;
    }

    async fn offline_persistent_subscriber(&mut self) {
        let (otx, _orx) = mpsc::unbounded_channel::<Box<crate::hub::Outgoing>>();
        let (reply, wait) = oneshot::channel();
        self.dispatch(HubCommand::Attach {
            client: ClientId(SUB.into()),
            admission: Box::new(admission(SUB)),
            conn_id: 7,
            clean_start: false,
            session_expiry: u32::MAX,
            receive_maximum: u16::MAX,
            will: None,
            outbound: Outbound::new(otx).0,
            reply,
        })
        .await;
        assert!(matches!(wait.await, Ok(AttachOutcome::Present(false))));
        let (reply, wait) = oneshot::channel();
        self.dispatch(HubCommand::Subscribe {
            client: ClientId(SUB.into()),
            filters: vec![(TOPIC.into(), QoS::AtLeastOnce)],
            no_local_filters: Vec::new(),
            sub_id: None,
            rap_filters: Vec::new(),
            retain_handling: vec![0],
            reply: Some(reply),
        })
        .await;
        assert_eq!(wait.await.unwrap(), vec![true]);
        self.dispatch(HubCommand::Detach {
            client: ClientId(SUB.into()),
            conn_id: 7,
            graceful: true,
            session_expiry_override: None,
        })
        .await;
    }

    /// A gated publish on the sender forwarded to the receiver, then left
    /// unanswered for a full sweep interval: the sweep's retransmission is
    /// returned alongside the original. Both are the SAME frame.
    fn forward_and_retransmit(
        &mut self,
    ) -> (
        u64,
        oneshot::Receiver<PublishOutcome>,
        PeerMessage,
        PeerMessage,
    ) {
        let (done, ack) = oneshot::channel();
        let id = self.sender.register_pending(
            done,
            TOPIC,
            &Bytes::from_static(b"once"),
            QoS::AtLeastOnce,
            false,
            None,
            &AppProperties::default(),
        );
        self.sender.send_acked_forward(id, &target(), false);
        // The local half is done; only the peer's answer is outstanding.
        self.sender.pending_local_done(id);
        let first = self.next_forward();

        // The answer has not come back within one sweep interval: overdue.
        let aged = tokio::time::Instant::now()
            .checked_sub(crate::hub::SESSION_SWEEP_INTERVAL)
            .expect("the clock is past one sweep interval");
        self.sender
            .pending_publishes
            .get_mut(id)
            .unwrap()
            .created_at = aged;
        self.sender.sweep_pending_forwards();
        let again = self.next_forward();
        assert_eq!(
            first, again,
            "fixture invariant: the sweep must retransmit the SAME frame under the SAME seq"
        );
        (id, ack, first, again)
    }

    fn next_forward(&mut self) -> PeerMessage {
        loop {
            let frame = self
                .to_receiver
                .try_recv()
                .expect("the sender put an acked forward on the link");
            if matches!(
                frame,
                PeerMessage::PublishAcked { .. } | PeerMessage::PublishAckedTagged { .. }
            ) {
                return frame;
            }
        }
    }

    /// What the receiver's peer-link pump would dispatch for `frame`.
    fn arrive(frame: PeerMessage) -> HubCommand {
        // A proto-12 link carries the tagged frame (#784); both arrive as one command.
        let (seq, topic, payload, qos, retain, message_expiry, app, tag, replay) = match frame {
            PeerMessage::PublishAcked {
                seq,
                topic,
                payload,
                qos,
                retain,
                message_expiry,
                app,
            } => (
                seq,
                topic,
                payload,
                qos,
                retain,
                message_expiry,
                app,
                None,
                false,
            ),
            PeerMessage::PublishAckedTagged {
                seq,
                origin: tag,
                replay,
                topic,
                payload,
                qos,
                retain,
                message_expiry,
                app,
            } => (
                seq,
                topic,
                payload,
                qos,
                retain,
                message_expiry,
                app,
                Some(tag),
                replay,
            ),
            other => panic!("not an acked forward: {other:?}"),
        };
        HubCommand::RemotePublishAcked {
            node: origin(),
            seq,
            topic,
            payload: payload.into(),
            qos: QoS::from_u8(qos).unwrap(),
            retain,
            message_expiry,
            app: crate::hub::app_from_wire(app),
            origin: tag,
            replay,
        }
    }

    /// Every answer the receiver has sent back since the last call.
    fn answers(&mut self) -> Vec<(u64, ForwardVerdict)> {
        let mut out = Vec::new();
        while let Ok(frame) = self.to_sender.try_recv() {
            if let PeerMessage::PublishVerdict { seq, verdict } = frame {
                out.push((seq, verdict));
            }
        }
        out
    }

    async fn queued(&self) -> usize {
        self.store
            .pending(&ClientId(SUB.into()), 0, 16)
            .await
            .unwrap()
            .len()
    }
}

fn seq_of(frame: &PeerMessage) -> u64 {
    match frame {
        PeerMessage::PublishAcked { seq, .. } | PeerMessage::PublishAckedTagged { seq, .. } => *seq,
        other => panic!("not an acked forward: {other:?}"),
    }
}

/// **The late answer.** The receiver applied the forward and answered, but the
/// answer did not reach the sender before its sweep re-sent the frame (crossed in
/// flight, or lost with the link). The retransmission must be answered again —
/// the sender needs it to retire the obligation — and must NOT be stored again.
///
/// Fails before the fix: the session's queue holds the message twice.
#[tokio::test]
async fn a_retransmission_after_the_answer_is_re_answered_not_stored_twice() {
    let mut rig = Rig::new().await;
    let (id, mut ack, first, again) = rig.forward_and_retransmit();
    let seq = seq_of(&first);

    rig.dispatch(Rig::arrive(first)).await;
    assert_eq!(
        rig.queued().await,
        1,
        "fixture invariant: the first copy is stored"
    );
    assert_eq!(
        rig.answers(),
        vec![(seq, ForwardVerdict::Reached)],
        "the first copy is answered once its append landed"
    );
    // That answer is lost: it never reaches the sender.

    rig.dispatch(Rig::arrive(again)).await;
    assert_eq!(
        rig.queued().await,
        1,
        "the sender's retransmission of a forward this node already stored was \
         stored AGAIN: the subscriber replays it twice, both with DUP = 0 \
         [MQTT-4.4.0-1] (issue #648)"
    );
    let answers = rig.answers();
    assert_eq!(
        answers,
        vec![(seq, ForwardVerdict::Reached)],
        "the retransmission must still be ANSWERED — the sender retires the \
         obligation only on an answer, and the first one was lost"
    );

    // And that answer is what releases the publisher.
    rig.sender
        .forward_answered(&target(), seq, ForwardVerdict::Stored);
    assert_eq!(ack.try_recv(), Ok(PublishOutcome::Accepted));
    assert!(!rig.sender.pending_publishes.contains_key(id));
}

/// **The slow append.** The retransmission arrives while the first copy's
/// durable append is still running. It must not submit a second append, and the
/// sender gets exactly one answer — the first copy's, when its append lands.
///
/// Fails before the fix: the second copy is folded into the same verdict
/// aggregate but still appended, so the queue holds the message twice.
#[tokio::test]
async fn a_retransmission_during_the_append_is_dropped_and_answered_once() {
    let mut rig = Rig::new().await;
    let (_id, _ack, first, again) = rig.forward_and_retransmit();
    let seq = seq_of(&first);

    // Both copies dispatched before the first copy's lane append can land.
    rig.receiver.dispatch(Rig::arrive(first)).await;
    assert!(
        rig.receiver
            .remote_append_pending
            .contains_key(&(origin(), seq)),
        "fixture invariant: the first copy's append must still be in flight"
    );
    rig.receiver.dispatch(Rig::arrive(again)).await;
    quiesce(&mut rig.receiver).await;

    assert_eq!(
        rig.queued().await,
        1,
        "a retransmission that arrived while the original's append was still \
         running was appended too (issue #648)"
    );
    assert_eq!(
        rig.answers(),
        vec![(seq, ForwardVerdict::Reached)],
        "exactly one answer, sent when the one append landed"
    );
}

/// A SHARED delivery's retransmission is recognised the same way: same
/// `(origin, seq)`, same named member, same content.
#[tokio::test]
async fn a_repeated_shared_delivery_is_re_answered_not_delivered_twice() {
    let mut rig = Rig::new().await;
    let shared = |seq| HubCommand::RemoteSharedDeliverAcked {
        node: origin(),
        seq,
        client: ClientId(SUB.into()),
        topic: TOPIC.into(),
        payload: Bytes::from_static(b"shared"),
        qos: QoS::AtLeastOnce,
        message_expiry: None,
        app: AppProperties::default(),
    };
    rig.dispatch(shared(41)).await;
    rig.dispatch(shared(41)).await;
    assert_eq!(
        rig.queued().await,
        1,
        "a repeated shared delivery was stored twice"
    );
    assert_eq!(
        rig.answers(),
        vec![(41, ForwardVerdict::Stored), (41, ForwardVerdict::Stored)],
        "each copy is answered"
    );
}

/// The key cannot swallow a DIFFERENT message: an origin that restarted (an
/// older build starts its seqs at 1 again) re-uses a remembered seq for new
/// content, and that is applied. And an origin's window dies with it.
#[tokio::test]
async fn a_re_used_seq_with_new_content_is_applied_and_a_dead_origin_is_forgotten() {
    let mut rig = Rig::new().await;
    let forward = |payload: &'static [u8]| HubCommand::RemotePublishAcked {
        node: origin(),
        seq: 1,
        topic: TOPIC.into(),
        payload: Bytes::from_static(payload),
        qos: QoS::AtLeastOnce,
        retain: false,
        message_expiry: None,
        app: AppProperties::default(),
        origin: None,
        replay: false,
    };
    rig.dispatch(forward(b"first life")).await;
    rig.dispatch(forward(b"second life")).await;
    assert_eq!(
        rig.queued().await,
        2,
        "the same seq carrying DIFFERENT content is a new message from a restarted \
         origin; dropping it would lose an acknowledged publish"
    );

    rig.receiver.peer_dead(&origin());
    assert!(
        !rig.receiver.forward_windows.contains_key(&origin()),
        "a dead origin's window is dropped with the rest of its routing state"
    );
}

/// The window is bounded: past [`crate::hub::forwarding::FORWARD_WINDOW`] entries the oldest
/// is forgotten, whatever an origin sends.
#[test]
fn the_forward_window_is_bounded() {
    let mut w = crate::hub::forwarding::ForwardWindow::default();
    let cap = crate::hub::forwarding::FORWARD_WINDOW as u64;
    for seq in 0..=cap {
        w.record(seq, seq);
    }
    assert_eq!(w.len(), crate::hub::forwarding::FORWARD_WINDOW);
    assert!(w.seen(0).is_none(), "the oldest seq is forgotten first");
    assert!(w.seen(cap).is_some(), "the newest is remembered");
}
