//! The hub's half of the rule engine's message events (ADR 0083): what it queues for a
//! connection, what it puts on a peer link, and what it never does without a rule that
//! asks.
//!
//! The events themselves are proven end to end in `tests/rules_message_events.rs`. These
//! pin the parts only the hub can show: that a delivery carries its origin only while a
//! rule here selects an event raised at the delivery; that a link is sent the two
//! proto-13 frames only when it negotiated 13 — and the origin only when its far end
//! asked — so a mixed-version cluster keeps exchanging exactly the frames it always did;
//! and that a drop the hub decides is handed to a task as a note, never evaluated here.

use super::*;
use crate::hub::Outgoing;
use crate::rules::{MessageNote, NoteEvent, Rules};
use mqtt_cluster::peer::{PROTO_MESSAGE_ORIGIN, PROTO_PUBLISH_ORIGIN};
use mqtt_core::Origin;

/// Rules selecting every message event, each republishing what it saw to `ev/<event>`.
const WATCH: &str = r#"
[rules.all]
sql = '''SELECT event, reason, clientid, username, payload FROM "$events/message/+" WHERE topic =~ 't/#' '''
actions = [{ function = "republish", args = { topic = "ev/${event}", payload = "${reason}|${clientid}|${username}|${payload}", qos = 0 } }]
"#;

/// Rules selecting only the two events the hub decides.
const DROPS: &str = r#"
[rules.drops]
sql = '''SELECT event FROM "$events/message/dropped", "$events/message/delivery_dropped" '''
actions = [{ function = "console" }]
"#;

fn rule_set(text: &str) -> Arc<mqtt_rules::RuleSet> {
    Arc::new(mqtt_rules::RuleSet::parse(text).unwrap().rules)
}

/// A hub with `rules` attached; the sender reloads them.
fn hub_with_rules(
    text: &str,
) -> (
    HubTx,
    Rules,
    tokio::sync::watch::Sender<Arc<mqtt_rules::RuleSet>>,
) {
    let tx = start_hub();
    let (rules_tx, rx) = tokio::sync::watch::channel(rule_set(text));
    let rules = Rules::new(rx, Arc::from("hub-test"), None);
    tx.send(HubCommand::AttachRules(rules.clone())).unwrap();
    (tx, rules, rules_tx)
}

fn origin() -> Arc<Origin> {
    Arc::new(Origin {
        id: 42,
        clientid: "pub1".into(),
        username: Some("pubuser".into()),
        peer: Some("10.0.0.9:50000".parse().unwrap()),
        received_at_ms: 1_700_000_000_000,
        republished: false,
        republish_depth: 0,
    })
}

/// A client's publish, carrying `origin` as the connection would have stamped it.
fn publish_from(
    tx: &HubTx,
    topic: &str,
    payload: &'static [u8],
    qos: QoS,
    origin: Option<Arc<Origin>>,
    done: Option<oneshot::Sender<PublishOutcome>>,
) {
    tx.send(HubCommand::Publish {
        topic: topic.into(),
        payload: Bytes::from_static(payload),
        qos,
        retain: false,
        message_expiry: None,
        app: AppProperties {
            origin,
            ..AppProperties::default()
        },
        done,
        v5: true,
        publisher: Some(ClientId("pub1".into())),
        credit: None,
    })
    .unwrap();
}

async fn next_out(rx: &mut crate::hub::OutboundRx) -> Outgoing {
    *timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("something queued for the client")
        .expect("the channel is open")
}

/// The next frame on a peer link that carries a publish or says what the peer's rules
/// want; the interest and retained gossip beside them is skipped.
async fn next_frame(rx: &mut mpsc::UnboundedReceiver<PeerMessage>) -> PeerMessage {
    loop {
        let msg = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("a peer frame within the deadline")
            .expect("the link is open");
        if !matches!(
            msg,
            PeerMessage::Interest { .. }
                | PeerMessage::SharedInterest { .. }
                | PeerMessage::RetainedDigest { .. }
        ) {
            return msg;
        }
    }
}

/// Nothing but gossip is on the link.
async fn only_gossip(rx: &mut mpsc::UnboundedReceiver<PeerMessage>) {
    while let Ok(Some(msg)) = timeout(Duration::from_millis(100), rx.recv()).await {
        assert!(
            matches!(
                msg,
                PeerMessage::Interest { .. }
                    | PeerMessage::SharedInterest { .. }
                    | PeerMessage::RetainedDigest { .. }
            ),
            "an unexpected frame: {msg:?}"
        );
    }
}

fn wants_origin(tx: &HubTx, node: &str, wanted: bool) {
    tx.send(HubCommand::RemoteMessageEvents {
        node: NodeId(node.into()),
        wanted,
    })
    .unwrap();
}

/// Without a rule selecting a message event the hub is exactly what it was: a delivery
/// of a message that carries an origin is queued WITHOUT it, nothing is queued for a
/// drop, a peer that did not ask is sent the plain frame, and peers are told nothing.
#[tokio::test]
async fn without_a_message_event_rule_nothing_is_carried_or_noted() {
    for rules in [None, Some("")] {
        let tx = match rules {
            None => start_hub(),
            Some(text) => hub_with_rules(text).0,
        };
        let mut peer = connect_peer(&tx, "p", 1);
        remote_interest(&tx, "p", &["t/#"]);
        let (mut sub, _) = attach(&tx, "sub", 1, true).await;
        subscribe(&tx, "sub", "t/a");
        let (mut publisher, _) = attach(&tx, "pub1", 2, true).await;
        ping(&tx).await;

        publish_from(&tx, "t/a", b"m", QoS::AtMostOnce, Some(origin()), None);
        match next_out(&mut sub).await {
            Outgoing::Packet {
                packet: Packet::Publish(p),
                origin,
            } => {
                assert_eq!(&p.payload[..], b"m");
                assert!(origin.is_none(), "no rule raises an event at the delivery");
            }
            other => panic!("expected the PUBLISH, got {other:?}"),
        }
        assert!(
            matches!(next_frame(&mut peer).await, PeerMessage::Publish { .. }),
            "the peer did not ask for origins"
        );
        // Reaches nobody: no note to the publisher, no frame to the peer.
        publish_from(&tx, "u/nobody", b"m", QoS::AtMostOnce, Some(origin()), None);
        ping(&tx).await;
        assert!(
            publisher.try_recv().is_err(),
            "nothing is queued for a drop"
        );
        only_gossip(&mut peer).await;
    }
}

/// A delivery carries its message's origin to the connection exactly while a rule
/// selects an event the connection raises (`delivered`, `acked`, a `too_large`
/// `delivery_dropped`) — not for a rule on `message.dropped` alone, and again not once a
/// reload has removed the rule.
#[tokio::test]
async fn a_delivery_carries_its_origin_only_while_a_delivery_event_is_selected() {
    let only_dropped = r#"
[rules.d]
sql = 'SELECT event FROM "$events/message/dropped"'
actions = [{ function = "console" }]
"#;
    let (tx, _rules, reload) = hub_with_rules(only_dropped);
    let (mut sub, _) = attach(&tx, "sub", 1, true).await;
    subscribe_qos(&tx, "sub", "t/a", QoS::AtLeastOnce);
    ping(&tx).await;
    let carried = async |tx: &HubTx, sub: &mut crate::hub::OutboundRx, qos| {
        let o = origin();
        publish_from(tx, "t/a", b"m", qos, Some(o.clone()), None);
        match next_out(sub).await {
            Outgoing::Packet {
                packet: Packet::Publish(_),
                origin,
            } => origin.map(|got| Arc::ptr_eq(&got, &o)),
            other => panic!("expected the PUBLISH, got {other:?}"),
        }
    };
    assert_eq!(carried(&tx, &mut sub, QoS::AtMostOnce).await, None);
    for selecting in [
        "$events/message/delivered",
        "$events/message/acked",
        "$events/message/delivery_dropped",
    ] {
        reload
            .send(rule_set(&format!(
                "[rules.r]\nsql = 'SELECT event FROM \"{selecting}\"'\nactions = [{{ function = \"console\" }}]\n"
            )))
            .unwrap();
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce] {
            assert_eq!(
                carried(&tx, &mut sub, qos).await,
                Some(true),
                "{selecting}: the publish's own origin, shared, not a copy"
            );
        }
    }
    reload.send(rule_set("")).unwrap();
    assert_eq!(carried(&tx, &mut sub, QoS::AtLeastOnce).await, None);
}

/// A rolling upgrade's mixed mesh. With a rule here selecting a message event, a link
/// that negotiated proto 13 is told so and a proto-12 link is not; a publish with an
/// origin goes out as `OriginPublish` only on a proto-13 link whose far end ASKED, and
/// as the plain frame — byte for byte what a proto-12 build sends — everywhere else.
/// Both `QoS` 0 (`Publish`) and a gated `QoS` 1 (`PublishAckedTagged` / `PublishAcked`).
#[tokio::test]
async fn only_a_proto_13_link_that_asked_is_sent_origins() {
    let (tx, _rules, reload) = hub_with_rules(WATCH);
    ping(&tx).await;
    let mut old = connect_peer_at_proto(&tx, "old", 1, PROTO_PUBLISH_ORIGIN);
    let mut asking = connect_peer_at_proto(&tx, "asking", 2, PROTO_MESSAGE_ORIGIN);
    let mut quiet = connect_peer_at_proto(&tx, "quiet", 3, PROTO_MESSAGE_ORIGIN);
    for node in ["old", "asking", "quiet"] {
        remote_interest(&tx, node, &["t/#"]);
    }
    // This node's rules select message events: the two proto-13 links hear so.
    for link in [&mut asking, &mut quiet] {
        assert_eq!(
            next_frame(link).await,
            PeerMessage::MessageEvents { wanted: true }
        );
    }
    only_gossip(&mut old).await;

    wants_origin(&tx, "asking", true);
    // A proto-12 peer cannot have sent this; if it somehow did, it is still not sent a
    // frame it cannot decode.
    wants_origin(&tx, "old", true);
    ping(&tx).await;

    let o = origin();
    publish_from(&tx, "t/a", b"m", QoS::AtMostOnce, Some(o.clone()), None);
    let plain = PeerMessage::Publish {
        topic: "t/a".into(),
        payload: b"m".to_vec(),
        qos: 0,
        retain: false,
        message_expiry: None,
        app: mqtt_cluster::peer::WireAppProps::default(),
    };
    assert_eq!(next_frame(&mut old).await, plain);
    assert_eq!(next_frame(&mut quiet).await, plain);
    let (unwrapped, carried) = next_frame(&mut asking).await.into_plain();
    assert_eq!(unwrapped, plain);
    let carried = carried.expect("the peer asked for origins");
    assert_eq!(
        (
            carried.id,
            carried.clientid.as_str(),
            carried.username.as_deref(),
            carried.peer,
            carried.received_at_ms
        ),
        (o.id, "pub1", Some("pubuser"), o.peer, o.received_at_ms)
    );

    // Gated: an acked forward is the same frame it always was, origin or not.
    let (done, _wait) = oneshot::channel();
    publish_from(
        &tx,
        "t/a",
        b"g",
        QoS::AtLeastOnce,
        Some(o.clone()),
        Some(done),
    );
    assert!(matches!(
        next_frame(&mut old).await,
        PeerMessage::PublishAckedTagged { .. }
    ));
    assert!(matches!(
        next_frame(&mut quiet).await,
        PeerMessage::PublishAckedTagged { .. }
    ));
    let (unwrapped, carried) = next_frame(&mut asking).await.into_plain();
    assert!(
        matches!(&unwrapped, PeerMessage::PublishAckedTagged { payload, .. } if payload == b"g"),
        "{unwrapped:?}"
    );
    assert_eq!(carried.map(|c| c.id), Some(o.id));

    // A publish without an origin is plain for everyone.
    publish_from(&tx, "t/a", b"n", QoS::AtMostOnce, None, None);
    for link in [&mut old, &mut asking, &mut quiet] {
        assert!(matches!(
            next_frame(link).await,
            PeerMessage::Publish { .. }
        ));
    }

    // The rule goes: the proto-13 links are told, the proto-12 link still hears nothing.
    reload.send(rule_set("")).unwrap();
    ping(&tx).await;
    for link in [&mut asking, &mut quiet] {
        assert_eq!(
            next_frame(link).await,
            PeerMessage::MessageEvents { wanted: false }
        );
    }
    only_gossip(&mut old).await;
}

/// A targeted shared delivery carries the origin to a peer that asked, too.
#[tokio::test]
async fn a_shared_delivery_to_a_peer_that_asked_carries_the_origin() {
    let tx = start_hub();
    let mut asking = connect_peer_at_proto(&tx, "asking", 1, PROTO_MESSAGE_ORIGIN);
    remote_shared_interest(&tx, "asking", "g", "t/s", &["m1"]);
    wants_origin(&tx, "asking", true);
    ping(&tx).await;
    let o = origin();
    publish_from(&tx, "t/s", b"m", QoS::AtMostOnce, Some(o.clone()), None);
    let (unwrapped, carried) = next_frame(&mut asking).await.into_plain();
    assert!(
        matches!(&unwrapped, PeerMessage::SharedDeliver { client, .. } if client == "m1"),
        "{unwrapped:?}"
    );
    assert_eq!(carried.map(|c| c.clientid), Some("pub1".to_string()));
}

/// Every publish on this node is stamped with an origin while a linked peer wants
/// origins — whether or not this node has such a rule itself — and no longer once that
/// peer says otherwise, or its link is gone.
#[tokio::test]
async fn publishes_are_stamped_while_a_linked_peer_asks() {
    let (tx, rules, _reload) = hub_with_rules("");
    ping(&tx).await;
    assert!(!rules.origin_wanted());
    let _old = connect_peer_at_proto(&tx, "old", 1, PROTO_PUBLISH_ORIGIN);
    let _new = connect_peer_at_proto(&tx, "new", 2, PROTO_MESSAGE_ORIGIN);
    wants_origin(&tx, "old", true);
    ping(&tx).await;
    assert!(
        !rules.origin_wanted(),
        "a link below proto 13 cannot be sent an origin, so none is stamped for it"
    );
    wants_origin(&tx, "new", true);
    ping(&tx).await;
    assert!(rules.origin_wanted());
    wants_origin(&tx, "new", false);
    ping(&tx).await;
    assert!(!rules.origin_wanted());
    wants_origin(&tx, "new", true);
    ping(&tx).await;
    assert!(rules.origin_wanted());
    tx.send(HubCommand::PeerDisconnected {
        node: NodeId("new".into()),
        conn_id: 2,
    })
    .unwrap();
    ping(&tx).await;
    assert!(!rules.origin_wanted(), "the link that asked is gone");
    // A new link starts at "no".
    let _again = connect_peer_at_proto(&tx, "new", 3, PROTO_MESSAGE_ORIGIN);
    ping(&tx).await;
    assert!(!rules.origin_wanted());
}

/// The flow-control backlog evicting its oldest message is EMQX's `queue_full`: the hub
/// queues a NOTE about the evicted message for the subscriber's connection — it
/// evaluates nothing itself — and the note carries the message whole, origin included.
#[tokio::test]
async fn a_backlog_eviction_is_noted_for_the_subscriber_s_connection() {
    let (mut hub, tx) = Hub::with_config(
        NodeId("hub-test".into()),
        Arc::new(MemorySessionStore::new()),
    );
    hub.set_subscriber_limits(SubscriberLimits {
        max_backlog_messages: 1,
        ..SubscriberLimits::default()
    });
    tokio::spawn(hub.run());
    let (_rules_tx, rx) = tokio::sync::watch::channel(rule_set(DROPS));
    tx.send(HubCommand::AttachRules(Rules::new(
        rx,
        Arc::from("hub-test"),
        None,
    )))
    .unwrap();
    // Receive Maximum 1: with the first `QoS` 1 message unacknowledged, the second
    // waits in the backlog and the third evicts it.
    let (mut sub, _) = attach_full(&tx, "sub", 1, true, 0, 1).await;
    subscribe_qos(&tx, "sub", "t/a", QoS::AtLeastOnce);
    ping(&tx).await;
    let o = origin();
    let send = |payload: &'static [u8]| {
        tx.send(HubCommand::Publish {
            topic: "t/a".into(),
            payload: Bytes::from_static(payload),
            qos: QoS::AtLeastOnce,
            retain: false,
            message_expiry: Some(60),
            app: AppProperties {
                origin: Some(o.clone()),
                ..AppProperties::default()
            },
            done: None,
            v5: true,
            publisher: None,
            credit: None,
        })
        .unwrap();
    };
    send(b"m0");
    let first = next_out(&mut sub).await;
    assert!(
        matches!(
            &first,
            Outgoing::Packet {
                packet: Packet::Publish(p),
                ..
            } if &p.payload[..] == b"m0"
        ),
        "{first:?}"
    );
    send(b"m1");
    send(b"m2");
    let Outgoing::Note(note) = next_out(&mut sub).await else {
        panic!("expected the note about the evicted message");
    };
    let MessageNote {
        event,
        topic,
        payload,
        qos,
        message_expiry,
        app,
        ..
    } = *note;
    assert_eq!(
        event,
        NoteEvent::DeliveryDropped {
            receiver: ClientId("sub".into()),
            reason: "queue_full",
        }
    );
    assert_eq!(
        (topic.as_str(), &payload[..], qos, message_expiry),
        ("t/a", &b"m1"[..], QoS::AtLeastOnce, Some(60))
    );
    assert!(Arc::ptr_eq(app.origin.as_ref().unwrap(), &o));
}

/// A note counts toward the client's queue like a packet and is refused, handed back,
/// once the queue is at its cap: a client that reads nothing cannot grow it with notes.
#[test]
fn a_note_is_refused_by_a_full_queue() {
    let (out_tx, mut rx) = mpsc::unbounded_channel();
    let (out, meter) = Outbound::new(out_tx);
    let note = || {
        Box::new(MessageNote {
            event: NoteEvent::Dropped {
                reason: "no_subscribers",
            },
            topic: "t".into(),
            payload: Bytes::from_static(b"p"),
            qos: QoS::AtMostOnce,
            retain: false,
            message_expiry: None,
            app: AppProperties::default(),
        })
    };
    assert!(out.note(note()).is_ok());
    assert_eq!((out.depth(), out.bytes()), (1, 0), "a note has no bytes");
    let queued = rx.try_recv().unwrap();
    assert!(matches!(*queued, Outgoing::Note(_)));
    meter.drained(&queued);
    assert_eq!(out.depth(), 0);

    for _ in 0..MAX_OUTBOUND_QUEUE {
        assert!(out.send(Packet::PingResp));
    }
    let back = out.note(note()).expect_err("the queue is full");
    assert_eq!(back.topic, "t");
    assert_eq!(
        out.depth(),
        MAX_OUTBOUND_QUEUE,
        "a refused note is not counted"
    );
    drop(rx);
    let (out_tx, rx) = mpsc::unbounded_channel();
    let (out, _meter) = Outbound::new(out_tx);
    drop(rx);
    assert!(out.note(note()).is_err(), "the connection is gone");
    assert_eq!(out.depth(), 0);
}

/// What the notes task raises, as its rule republished it: `reason|clientid|username|payload`.
async fn raised(watcher: &mut crate::hub::OutboundRx) -> (String, String) {
    match next_out(watcher).await {
        Outgoing::Packet {
            packet: Packet::Publish(p),
            ..
        } => (p.topic, String::from_utf8(p.payload.to_vec()).unwrap()),
        other => panic!("expected a republished event, got {other:?}"),
    }
}

/// A `QoS` 0 message shed for a subscriber that has stopped reading is EMQX's
/// `queue_full`. Its queue has no room for a note either, so the notes task raises the
/// event — off the hub loop, knowing the subscriber by its client id only.
#[tokio::test]
async fn a_qos0_shed_for_a_stalled_subscriber_is_raised_by_the_notes_task() {
    let (tx, _rules, _reload) = hub_with_rules(WATCH);
    let (mut watcher, _) = attach(&tx, "watcher", 1, true).await;
    subscribe(&tx, "watcher", "ev/#");
    // Held and never read from.
    let (_stalled, _) = attach(&tx, "stalled", 2, true).await;
    subscribe(&tx, "stalled", "t/flood");
    ping(&tx).await;
    for _ in 0..MAX_OUTBOUND_QUEUE {
        publish(&tx, "t/flood", b"fill");
    }
    ping(&tx).await;
    publish_from(
        &tx,
        "t/flood",
        b"shed",
        QoS::AtMostOnce,
        Some(origin()),
        None,
    );
    // The fill's deliveries raise nothing here (nothing reads them); the shed does.
    assert_eq!(
        raised(&mut watcher).await,
        (
            "ev/delivery.dropped".to_string(),
            "queue_full|stalled|undefined|shed".to_string()
        )
    );
}

/// A publish forwarded here that finds no subscriber after all — the route its origin
/// node forwarded on was stale — is dropped HERE, as EMQX drops it on the node its
/// dispatch found nobody on: the notes task raises `message.dropped` naming the
/// publisher the forward carried. A retained forward, which (without durable retained)
/// goes to every node whether it has a subscriber or not, raises nothing.
#[tokio::test]
async fn a_forward_that_reaches_nobody_is_dropped_on_the_receiving_node() {
    let (tx, _rules, _reload) = hub_with_rules(WATCH);
    let (mut watcher, _) = attach(&tx, "watcher", 1, true).await;
    subscribe(&tx, "watcher", "ev/#");
    ping(&tx).await;
    let forward = |payload: &'static [u8], retain| HubCommand::RemotePublish {
        topic: "t/stale".into(),
        payload: Bytes::from_static(payload),
        qos: QoS::AtMostOnce,
        retain,
        message_expiry: None,
        app: AppProperties {
            origin: Some(origin()),
            ..AppProperties::default()
        },
        credit: None,
    };
    tx.send(forward(b"kept", true)).unwrap();
    tx.send(forward(b"lost", false)).unwrap();
    assert_eq!(
        raised(&mut watcher).await,
        (
            "ev/message.dropped".to_string(),
            "no_subscribers|pub1|pubuser|lost".to_string()
        )
    );
    ping(&tx).await;
    assert!(
        watcher.try_recv().is_err(),
        "the retained forward raised nothing"
    );
}
