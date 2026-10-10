//! The rule engine's message events, as far as the hub decides them (ADR 0083).
//!
//! **The invariant this module owns:** the hub never runs rule SQL for a message
//! event. It only *notices* — a publish that reached nobody, a message dropped on its
//! way to one subscriber — and hands what it noticed, as a
//! [`MessageNote`](crate::rules::MessageNote), to a task that evaluates: the
//! connection task of the client the note is about, or, when that client has no
//! connection (an offline session, a forward from a peer), the one task that exists
//! for such notes ([`Rules::spawn_notes`](crate::rules::Rules::spawn_notes)).
//! `$events/message/delivered` and `acked` never pass through here at all: the
//! subscriber's connection raises them as it writes and reads.
//!
//! **What it costs.** Every site below is behind [`State::wants`], a plain
//! field: with no rule selecting a message event nothing is allocated, cloned or
//! queued, and a delivery's `origin` stays `None`. The field is refreshed from the
//! rules' watch at the top of each dispatch — one load of the watch's version.
//!
//! **Across nodes.** What the events say about a message's publisher is its
//! [`Origin`], which travels with the message. A node whose rules select a message
//! event tells its peers so ([`PeerMessage::MessageEvents`]); a node stamps origins on
//! its publishes while its own rules or any linked peer's want them, and carries them
//! only on the links whose far end asked ([`origin_frame`]).

#[allow(clippy::wildcard_imports)] // an intra-hub module split (#258): the
// siblings share one type/state vocabulary by design.
use super::*;
use crate::rules::{MessageNote, NoteEvent, Rules};
use mqtt_cluster::peer::{WireOrigin, PROTO_MESSAGE_ORIGIN};

/// How often the log says that notes were shed because the queue of the task raising
/// them was full.
const NOTES_SHED_WARN_EVERY: Duration = Duration::from_secs(10);

/// What the hub keeps for the message events.
#[derive(Debug, Default)]
pub(super) struct State {
    /// Which message events the loaded rules select: all `false` — and never
    /// refreshed — without rules. Read at every site that could notice a drop or
    /// attach an origin; refreshed from `watch` at the top of each dispatch
    /// ([`Hub::refresh_message_wants`]).
    pub wants: crate::rules::MessageWants,
    watch: Option<crate::rules::MessageWatch>,
    /// The queue of the task that raises notes about clients with no connection;
    /// started with the rules.
    notes: Option<mpsc::Sender<Box<MessageNote>>>,
    /// When a note was last dropped because that queue was full: the log says so once
    /// per interval.
    notes_shed_at: Option<Instant>,
    /// What this node last told its peers about its message-event rules
    /// ([`PeerMessage::MessageEvents`]).
    told: bool,
}

/// A message as the publish path holds it, for a note about it.
pub(super) struct Published<'a> {
    pub topic: &'a str,
    pub payload: &'a Bytes,
    pub qos: QoS,
    pub retain: bool,
    pub message_expiry: Option<u32>,
    pub app: &'a AppProperties,
}

impl Published<'_> {
    fn note(&self, event: NoteEvent) -> Box<MessageNote> {
        Box::new(MessageNote {
            event,
            topic: self.topic.to_string(),
            payload: self.payload.clone(),
            qos: self.qos,
            retain: self.retain,
            message_expiry: self.message_expiry,
            app: self.app.clone(),
        })
    }
}

/// An [`Origin`] in its peer-wire form.
pub(super) fn origin_to_wire(o: &Origin) -> WireOrigin {
    WireOrigin {
        id: o.id,
        clientid: o.clientid.clone(),
        username: o.username.clone(),
        peer: o.peer,
        received_at_ms: o.received_at_ms,
        republished: o.republished,
        republish_depth: o.republish_depth,
    }
}

/// An [`Origin`] a peer sent with a forwarded publish.
pub(crate) fn origin_from_wire(w: WireOrigin) -> Arc<Origin> {
    Arc::new(Origin {
        id: w.id,
        clientid: w.clientid,
        username: w.username,
        peer: w.peer,
        received_at_ms: w.received_at_ms,
        republished: w.republished,
        republish_depth: w.republish_depth,
    })
}

/// `frame` as it goes to `peer`: carrying the message's origin
/// ([`PeerMessage::OriginPublish`]) when the message has one and the peer asked for
/// origins, else unchanged — so a peer that did not ask, or that speaks a proto below
/// 13, is sent exactly the frame it always was.
pub(super) fn origin_frame(peer: &Peer, frame: PeerMessage, app: &AppProperties) -> PeerMessage {
    match &app.origin {
        Some(origin) if peer.wants_origin => frame.with_origin(origin_to_wire(origin)),
        _ => frame,
    }
}

impl Hub {
    /// Take the rule engine: the Will's rules, the watch the message-event flags are
    /// read from, and the task that raises notes for clients without a connection.
    pub(super) fn attach_rules(&mut self, rules: Rules) {
        let mut watch = rules.message_watch();
        self.events.wants = watch.wants();
        self.events.watch = Some(watch);
        self.events.notes = Some(rules.spawn_notes(self.self_tx.clone()));
        self.rules = Some(rules);
        self.recompute_peers_want_origin();
        self.tell_message_events(None);
    }

    /// Pick up a reloaded rule set's message-event flags, and tell the peers when
    /// whether this node wants origins changed. One load of the watch's version when
    /// nothing was reloaded; nothing at all without rules.
    pub(super) fn refresh_message_wants(&mut self) {
        let Some(watch) = &mut self.events.watch else {
            return;
        };
        let wants = watch.wants();
        if wants != self.events.wants {
            self.events.wants = wants;
            self.tell_message_events(None);
        }
    }

    /// The origin a delivery of a message with `app` carries to its connection: the
    /// message's own, while a rule here selects an event the connection raises.
    pub(super) fn delivery_origin(&self, app: &AppProperties) -> Option<Arc<Origin>> {
        if self.events.wants.on_delivery() {
            app.origin.clone()
        } else {
            None
        }
    }

    /// Tell `only` — or every peer — whether this node's rules select a message event,
    /// when that differs from what the peer was last told (a new link starts at "no").
    /// Never sent to a link below proto 13.
    pub(super) fn tell_message_events(&mut self, only: Option<&NodeId>) {
        let wanted = self.events.wants.any();
        // A new link: it has heard nothing, and "no" is its default.
        if let Some(node) = only {
            if wanted {
                if let Some(peer) = self.peers.get(node) {
                    if peer.proto >= PROTO_MESSAGE_ORIGIN {
                        let _ = peer.tx.send(PeerMessage::MessageEvents { wanted });
                    }
                }
            }
            return;
        }
        if wanted == self.events.told {
            return;
        }
        self.events.told = wanted;
        for peer in self.peers.values() {
            if peer.proto >= PROTO_MESSAGE_ORIGIN {
                let _ = peer.tx.send(PeerMessage::MessageEvents { wanted });
            }
        }
    }

    /// A peer said whether it wants origins.
    pub(super) fn remote_message_events(&mut self, node: &NodeId, wanted: bool) {
        let Some(peer) = self.peers.get_mut(node) else {
            return;
        };
        debug!(peer = %node.0, wanted, "peer's rules select a message event: {wanted}");
        peer.wants_origin = wanted && peer.proto >= PROTO_MESSAGE_ORIGIN;
        self.recompute_peers_want_origin();
    }

    /// Tell the rule engine whether any linked peer wants origins, so every publish on
    /// this node is — or is no longer — stamped with one for them.
    pub(super) fn recompute_peers_want_origin(&mut self) {
        if let Some(rules) = &self.rules {
            rules.set_peers_want_origin(self.peers.values().any(|p| p.wants_origin));
        }
    }

    /// `$events/message/delivery_dropped`, reason `no_local`: the publisher's own No
    /// Local subscription matched its publish. Raised on the publisher's connection —
    /// it is the subscriber.
    pub(super) fn note_no_local(&mut self, publisher: &ClientId, message: &Published<'_>) {
        if !self.events.wants.delivery_dropped || mqtt_core::is_reserved_topic(message.topic) {
            return;
        }
        let note = message.note(NoteEvent::DeliveryDropped {
            receiver: publisher.clone(),
            reason: "no_local",
        });
        self.post_note(Some(publisher), note);
    }

    /// `$events/message/dropped`, reason `no_subscribers`: the publish reached nobody.
    /// Raised on the publisher's connection; for a message nobody is connected for (a
    /// Will, a rule's republish), by the notes task.
    pub(super) fn note_no_subscribers(
        &mut self,
        publisher: Option<&ClientId>,
        message: &Published<'_>,
    ) {
        if !self.events.wants.dropped || mqtt_core::is_reserved_topic(message.topic) {
            return;
        }
        let note = message.note(NoteEvent::Dropped {
            reason: "no_subscribers",
        });
        self.post_note(publisher, note);
    }

    /// `$events/message/dropped` on the node a publish was forwarded to, when nothing
    /// there wanted it after all (the subscriber left while the forward was on its
    /// way): EMQX raises it on that node too. Not for a retained message, which
    /// without durable retained is forwarded to every node whether it has a subscriber
    /// or not.
    pub(super) fn note_unrouted_forward(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        message_expiry: Option<u32>,
        app: &AppProperties,
    ) {
        if retain {
            return;
        }
        self.note_no_subscribers(
            None,
            &Published {
                topic,
                payload,
                qos,
                retain,
                message_expiry,
                app,
            },
        );
    }

    /// `$events/message/delivery_dropped`: `message` was dropped on its way to
    /// `client`, for EMQX's `reason`. Raised on that client's connection, or by the
    /// notes task when it has none.
    pub(super) fn note_delivery_dropped(
        &mut self,
        client: &ClientId,
        reason: &'static str,
        message: &Message,
        retain: bool,
        message_expiry: Option<u32>,
    ) {
        if !self.events.wants.delivery_dropped || mqtt_core::is_reserved_topic(&message.topic) {
            return;
        }
        let note = Box::new(MessageNote {
            event: NoteEvent::DeliveryDropped {
                receiver: client.clone(),
                reason,
            },
            topic: message.topic.clone(),
            payload: message.payload.clone(),
            qos: message.qos,
            retain,
            message_expiry,
            app: message.app.clone(),
        });
        self.post_note(Some(client), note);
    }

    /// A session queue's cap rejected the newest message (`reject-newest`,
    /// [`LaneOutcome::Dropped`]) and it is not going out live either — the session is
    /// offline: EMQX's `queue_full`. A rejected message that IS `sent_live`, to the
    /// connection it was planned for, is delivered, not dropped; and any other outcome
    /// is no drop.
    pub(super) fn note_queue_rejected(
        &mut self,
        job: &AppendJob,
        outcome: LaneOutcome,
        sent_live: bool,
    ) {
        if matches!(outcome, LaneOutcome::Dropped) && !sent_live {
            self.note_delivery_dropped(
                &job.client,
                "queue_full",
                &job.message,
                job.retain,
                job.message_expiry,
            );
        }
    }

    /// Hand `note` to the connection task of `about`, or to the notes task when that
    /// client is not connected (or there is none). A note that fits in neither queue is
    /// dropped — the event is not raised — and the log says so once per interval.
    fn post_note(&mut self, about: Option<&ClientId>, note: Box<MessageNote>) {
        let note = match about.and_then(|c| self.online.get(c)) {
            Some(online) => match online.tx.note(note) {
                Ok(()) => return,
                // The connection is gone, or its queue is full of what it is not
                // reading: the notes task raises it instead.
                Err(note) => note,
            },
            None => note,
        };
        let Some(notes) = &self.events.notes else {
            return;
        };
        if notes.try_send(note).is_err() {
            let now = Instant::now();
            if self
                .events
                .notes_shed_at
                .is_none_or(|at| now.duration_since(at) >= NOTES_SHED_WARN_EVERY)
            {
                self.events.notes_shed_at = Some(now);
                warn!(
                    queue = crate::rules::NOTE_QUEUE,
                    "message events are being dropped unraised: the rules selecting \
                     $events/message/dropped or delivery_dropped are not keeping up with \
                     the drops (further drops within {}s are not logged)",
                    NOTES_SHED_WARN_EVERY.as_secs()
                );
            }
        }
    }
}
