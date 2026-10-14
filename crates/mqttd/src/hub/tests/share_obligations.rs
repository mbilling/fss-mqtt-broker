//! ADR 0087, task 0087-T2b: a `$share` obligation follows the member, and publish
//! order holds across a heal. The gaps an independent read-through of 0087-T2 left
//! on issue #973, each pinned here at the hub:
//!
//! 1. a held `$share` delivery was answered `Stored` by a peer that no longer had
//!    the member, and one pinned to a member behind a lost link never moved to the
//!    same consumer reconnected through another node;
//! 2. a `$share` delivery was discharged when its node's hold ran out while the
//!    group still had a member on another node that was only out of reach;
//! 3. a publish made between a link's return and the peer's first announcement on
//!    it overtook everything held for that peer;
//! 5. (the drain) a drain-gated `$share` delivery with nobody to hand it to was
//!    dropped without a count, and one behind a link-down member waited.
//!
//! Driven without the hub's actor (the `settle_gate` precedent): the handlers are
//! called directly, the sweep is the plain function it is, and no assertion depends
//! on how long the test took — the one test about what the sweep does as time
//! passes runs on a paused clock and moves it itself. A peer is a pair of channels;
//! what the hub put on its link is read back in order.

use super::settle_gate::quiesce;
use super::*;
use crate::hub::forwarding::{ForwardKind, PendingPublish};
use crate::hub::{DurableOutcome, PROTO_FORWARD_VERDICT, REROUTE_GRACE_TICKS};
use mqtt_cluster::peer::{PROTO_MAX, REFUSAL_SHARED_MEMBER_GONE};

/// The protocol v1.1.0 speaks: the newest release that neither sends nor reads
/// [`REFUSAL_SHARED_MEMBER_GONE`], and that answers `Stored` for a `$share`
/// delivery whose member is not there.
const PROTO_V1_1_0: u32 = 12;

const GROUP: &str = "g";
const FILTER: &str = "s/#";
const TOPIC: &str = "s/t";

fn hub() -> Hub {
    Hub::with_config(
        NodeId("so-origin".into()),
        Arc::new(MemorySessionStore::new()),
    )
    .0
}

/// One peer link: what the hub wrote to it, in order.
struct Link {
    node: NodeId,
    rx: mpsc::UnboundedReceiver<PeerMessage>,
    _ctl: mpsc::UnboundedReceiver<PeerMessage>,
}

/// A link that has just formed: the peer has said nothing on it yet.
fn link(hub: &mut Hub, name: &str, proto: u32) -> Link {
    link_as(hub, name, proto, 0)
}

/// [`link`], as connection `conn_id`.
fn link_as(hub: &mut Hub, name: &str, proto: u32, conn_id: u64) -> Link {
    let node = NodeId(name.into());
    let (tx, rx) = mpsc::unbounded_channel();
    let (ctl, ctl_rx) = mpsc::unbounded_channel();
    hub.peer_connected(node.clone(), conn_id, tx, ctl, None, proto, Arc::default());
    Link {
        node,
        rx,
        _ctl: ctl_rx,
    }
}

/// The membership snapshot of a node whose members of the test's group are `members`.
fn members(members: &[&str]) -> Vec<RemoteSharedGroup> {
    if members.is_empty() {
        return Vec::new();
    }
    vec![RemoteSharedGroup {
        group: GROUP.into(),
        filter: FILTER.into(),
        members: members
            .iter()
            .map(|m| (ClientId((*m).into()), QoS::AtLeastOnce, true))
            .collect(),
    }]
}

/// The peer says what it routes, as it does first thing on every link: its
/// interest, then its `$share` members.
fn announce(hub: &mut Hub, link: &Link, interest: &[&str], group: &[&str]) {
    hub.peer_announced_interest(
        &link.node,
        interest.iter().map(|f| (*f).to_string()).collect(),
    );
    hub.peer_announced_shared(&link.node, members(group));
}

/// A linked peer that has announced `group` as its members of the test's group.
fn peer(hub: &mut Hub, name: &str, proto: u32, group: &[&str]) -> Link {
    let l = link(hub, name, proto);
    announce(hub, &l, &[], group);
    l
}

fn register(hub: &mut Hub, topic: &str, qos: QoS) -> (u64, oneshot::Receiver<PublishOutcome>) {
    let (tx, rx) = oneshot::channel();
    let id = hub.register_pending(
        tx,
        topic,
        &Bytes::from_static(b"x"),
        qos,
        false,
        None,
        &AppProperties::default(),
    );
    (id, rx)
}

/// A gated publish to the test's `$share` group: placed with one member.
fn publish_shared(hub: &mut Hub) -> (u64, oneshot::Receiver<PublishOutcome>) {
    let (id, rx) = register(hub, TOPIC, QoS::AtLeastOnce);
    let (durable, placed) = hub.deliver_shared(
        TOPIC,
        &Bytes::from_static(b"x"),
        QoS::AtLeastOnce,
        None,
        &AppProperties::default(),
        Some(id),
        false,
    );
    assert!(
        matches!(durable, DurableOutcome::Ok) && placed,
        "fixture: the group placed it"
    );
    hub.pending_local_done(id);
    (id, rx)
}

/// A gated publish forwarded to every peer whose interest matches `topic`.
fn publish_ordinary(
    hub: &mut Hub,
    topic: &str,
    qos: QoS,
) -> (u64, oneshot::Receiver<PublishOutcome>) {
    let (id, rx) = register(hub, topic, qos);
    hub.forward_to_peers(
        topic,
        &Bytes::from_static(b"x"),
        qos,
        false,
        None,
        &AppProperties::default(),
        Some(id),
    );
    hub.pending_local_done(id);
    (id, rx)
}

/// The acked forwards `link` was sent since it was last read, in order: each
/// frame's seq, and the named member for a `$share` delivery.
fn sent(link: &mut Link) -> Vec<(u64, Option<String>)> {
    std::iter::from_fn(|| link.rx.try_recv().ok())
        .filter_map(|frame| match frame {
            PeerMessage::PublishAcked { seq, .. } | PeerMessage::PublishAckedTagged { seq, .. } => {
                Some((seq, None))
            }
            PeerMessage::SharedDeliverAcked { seq, client, .. } => Some((seq, Some(client))),
            _ => None,
        })
        .collect()
}

/// Whom pending publish `id` is owed to: each outstanding obligation's seq, node,
/// and named member.
fn owed(hub: &Hub, id: u64) -> Vec<(u64, String, Option<String>)> {
    let mut out: Vec<_> = hub
        .pending_publishes
        .get(id)
        .map(|p: &PendingPublish| {
            p.awaiting
                .iter()
                .map(|(seq, o)| {
                    let member = match &o.kind {
                        ForwardKind::Shared { client, .. } => Some(client.0.to_string()),
                        ForwardKind::Ordinary { .. } | ForwardKind::Question { .. } => None,
                    };
                    (*seq, o.node.0.clone(), member)
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// One sweep tick as the hub runs it: the holds first, then the pending table.
fn tick(hub: &mut Hub) {
    hub.sweep_verdict_holds();
    hub.sweep_pending_forwards();
}

/// Restart the overdue clock of pending publish `id`, as a registration on it does.
fn just_forwarded(hub: &mut Hub, id: u64) {
    let p = hub.pending_publishes.get_mut(id).expect("still pending");
    p.forwarded_after_ms = u32::try_from(p.created_at.elapsed().as_millis()).unwrap();
}

fn accepted(rx: &mut oneshot::Receiver<PublishOutcome>) -> bool {
    matches!(rx.try_recv(), Ok(PublishOutcome::Accepted))
}

fn unanswered(rx: &mut oneshot::Receiver<PublishOutcome>) -> bool {
    matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty))
}

fn withheld(rx: &mut oneshot::Receiver<PublishOutcome>) -> bool {
    matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed))
}

// ---- item 3: publish order across a heal -------------------------------------

/// A subscriber on a peer sees one publisher's messages in publish order across an
/// outage. Two publishes are held for the peer while it is out of reach; its link
/// returns; a third is published BEFORE the peer has announced on the new link.
/// It is recorded behind the two, and the three go out oldest first when the peer
/// announces. Until this, the third was sent the moment it was registered — the
/// link was there — and reached the peer's subscriber ahead of the two held ones.
///
/// For a peer that lost its link and for one held dead, at `QoS` 1 and `QoS` 2.
#[test]
fn a_publish_made_before_the_peer_announces_on_a_new_link_goes_out_behind_what_was_held() {
    for (dead, qos) in [
        (false, QoS::AtLeastOnce),
        (true, QoS::AtLeastOnce),
        (false, QoS::ExactlyOnce),
        (true, QoS::ExactlyOnce),
    ] {
        let case = format!("dead={dead} qos={qos:?}");
        let mut h = hub();
        let mut first = link(&mut h, "p", PROTO_MAX);
        announce(&mut h, &first, &["t/#"], &[]);
        // In flight when the peer goes: sent, and not answered.
        let (in_flight, _before) = publish_ordinary(&mut h, "t/a", qos);
        assert_eq!(
            sent(&mut first).len(),
            1,
            "{case}: sent at once in the steady state"
        );

        if dead {
            h.peer_dead(&first.node);
        } else {
            h.peer_disconnected(&first.node, 0);
        }
        let (held_1, _a) = publish_ordinary(&mut h, "t/a", qos);
        let (held_2, _b) = publish_ordinary(&mut h, "t/a", qos);

        // The link is back; the peer has not said anything on it yet.
        let mut back = link(&mut h, "p", PROTO_MAX);
        let (after_link_up, _c) = publish_ordinary(&mut h, "t/a", qos);
        assert!(
            sent(&mut back).is_empty(),
            "{case}: nothing is sent on a link the peer has not announced on — a \
             publish sent now would overtake the two held for it"
        );

        h.peer_announced_interest(&back.node, vec!["t/#".to_string()]);
        let order: Vec<u64> = sent(&mut back).into_iter().map(|(seq, _)| seq).collect();
        let expected: Vec<u64> = [in_flight, held_1, held_2, after_link_up]
            .into_iter()
            .map(|id| owed(&h, id)[0].0)
            .collect();
        assert_eq!(
            order, expected,
            "{case}: everything owed goes out in the order it was published — the \
             forward in flight when the link went (a repeat, if the peer had it), the \
             two held, then the one made after the link came back"
        );

        // From here on the peer has announced, and a forward is sent at once.
        let (_, _d) = publish_ordinary(&mut h, "t/a", qos);
        assert_eq!(sent(&mut back).len(), 1, "{case}");
    }
}

/// The same for a `$share` delivery to a named member, which waits for the peer's
/// MEMBERSHIP snapshot — the one after its interest snapshot — since that is what
/// says whether the member is still there.
#[test]
fn a_share_delivery_made_before_the_peers_membership_arrives_goes_out_behind_what_was_held() {
    for dead in [false, true] {
        let mut h = hub();
        let first = peer(&mut h, "p", PROTO_MAX, &["c"]);
        if dead {
            h.peer_dead(&first.node);
        } else {
            h.peer_disconnected(&first.node, 0);
        }
        let (held_1, _a) = publish_shared(&mut h);
        let (held_2, _b) = publish_shared(&mut h);

        let mut back = link(&mut h, "p", PROTO_MAX);
        let (after_link_up, _c) = publish_shared(&mut h);
        h.peer_announced_interest(&back.node, Vec::new());
        let (after_interest, _d) = publish_shared(&mut h);
        assert!(
            sent(&mut back).is_empty(),
            "dead={dead}: not before the peer has said who its members are"
        );

        h.peer_announced_shared(&back.node, members(&["c"]));
        let order: Vec<u64> = sent(&mut back).into_iter().map(|(seq, _)| seq).collect();
        let expected: Vec<u64> = [held_1, held_2, after_link_up, after_interest]
            .into_iter()
            .map(|id| owed(&h, id)[0].0)
            .collect();
        assert_eq!(order, expected, "dead={dead}: oldest publish first");
    }
}

/// The sweep still sends an overdue forward to a linked peer that has not
/// announced — a peer that restarted and cannot yet say what it routes answers it
/// with a failure, and the publisher retries instead of waiting on it — but:
///
/// - only once the LINK has been there for a sweep interval. What was held across
///   an outage is overdue the moment the link forms, and the first sweep after it
///   came within a second: the backlog went out before the peer had said anything;
/// - EVERYTHING the peer is owed, oldest first, not the overdue one alone ahead of
///   an older one beside it — once: the link is then open;
/// - never a `$share` delivery. Only the membership snapshot says whether the named
///   member is still there, and a peer that has not sent it cannot yet say who is
///   on it: it could only answer `Failed`, withholding an acknowledgement the
///   snapshot would have let this node place again.
///
/// On a paused clock, moved by the test: nothing here depends on how long the test
/// itself takes.
#[tokio::test(start_paused = true)]
async fn a_peer_silent_for_a_sweep_interval_is_sent_its_ordinary_forwards_oldest_first() {
    let interval = crate::hub::SESSION_SWEEP_INTERVAL;
    for proto in [PROTO_MAX, PROTO_V1_1_0] {
        let mut h = hub();
        let first = peer(&mut h, "p", proto, &["c"]);
        h.interest
            .replace(first.node.clone(), vec!["t/#".to_string()]);
        h.peer_disconnected(&first.node, 0);
        let (older, _a) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
        let (newer, _b) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
        let (shared, _c) = publish_shared(&mut h);
        // The outage lasts: everything held is long overdue when the link returns.
        tokio::time::advance(interval * 5).await;

        let mut back = link(&mut h, "p", proto);
        tick(&mut h);
        assert!(
            sent(&mut back).is_empty(),
            "proto {proto}: nothing at the first sweep after the link formed — the \
             peer has had no time to say anything"
        );

        // A sweep interval of silence. Only the NEWER forward is overdue now: the
        // older one's clock was restarted (as a re-route on its publish does).
        tokio::time::advance(interval).await;
        just_forwarded(&mut h, older);
        tick(&mut h);
        assert_eq!(
            sent(&mut back),
            vec![(owed(&h, older)[0].0, None), (owed(&h, newer)[0].0, None)],
            "proto {proto}: both ordinary forwards, the older first, and no `$share` \
             delivery"
        );

        // That was the one flush: the link is open for ordinary forwards now, so a
        // new one is sent as it is registered, and a sweep with nothing overdue
        // sends nothing again.
        let (_, _d) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
        assert_eq!(sent(&mut back).len(), 1, "proto {proto}: sent at once");
        tick(&mut h);
        assert!(
            sent(&mut back).is_empty(),
            "proto {proto}: the whole backlog is not sent again every tick"
        );

        // However long the peer stays silent, the `$share` delivery waits.
        tokio::time::advance(interval * 3).await;
        tick(&mut h);
        assert!(
            sent(&mut back).iter().all(|(_, member)| member.is_none()),
            "proto {proto}: only ordinary retransmissions"
        );

        // The peer announces. Its interest: nothing is sent a second time. Its
        // members: the member is still there, and the delivery goes out.
        h.peer_announced_interest(&back.node, vec!["t/#".to_string()]);
        assert!(sent(&mut back).is_empty(), "proto {proto}");
        h.peer_announced_shared(&back.node, members(&["c"]));
        assert_eq!(
            sent(&mut back),
            vec![(owed(&h, shared)[0].0, Some("c".to_string()))],
            "proto {proto}"
        );
    }
}

/// The order holds for what this node learns it owes only AT the heal. A publish
/// that matched nobody when it was made is kept for the settle pass while the view
/// is unsettled, with no forward recorded; when the peer's snapshot on the new link
/// announces a matching filter — its subscriber subscribed during the outage, or
/// its interest had been erased with its hold — the publish is re-routed to it.
/// That happened at the next settle pass, up to a tick after the snapshot, and
/// everything published in between had already been sent: the subscriber saw up to
/// a second of newer messages, then the older ones. The re-route now happens at
/// the announcement, before the link opens, and goes out in its place.
#[test]
fn what_is_first_owed_at_the_heal_goes_out_in_publish_order_too() {
    // (a) the subscriber subscribed on the peer while the link was down.
    let mut h = hub();
    h.set_cluster_configured();
    assert!(
        h.routing_unsettled(),
        "fixture: an unmatched publish is kept"
    );
    let first = link(&mut h, "p", PROTO_MAX);
    announce(&mut h, &first, &["other/#"], &[]);
    h.peer_disconnected(&first.node, 0);
    let (cut_1, mut ack_1) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
    let (cut_2, _b) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
    assert!(
        owed(&h, cut_1).is_empty() && unanswered(&mut ack_1),
        "nothing is owed to anybody yet, and the acknowledgement is held"
    );
    let mut back = link(&mut h, "p", PROTO_MAX);
    let (linked, _c) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
    h.peer_announced_interest(&back.node, vec!["t/#".to_string()]);
    let order: Vec<u64> = sent(&mut back).into_iter().map(|(seq, _)| seq).collect();
    let expected: Vec<u64> = [cut_1, cut_2, linked]
        .into_iter()
        .map(|id| owed(&h, id)[0].0)
        .collect();
    assert_eq!(
        order, expected,
        "(a) re-routed at the announcement, oldest first"
    );
    let (_, _d) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
    assert_eq!(
        sent(&mut back).len(),
        1,
        "(a) and what follows goes behind them"
    );

    // (b) the outage outlasts the hold: the peer is held dead, it is confirmed
    // gone and its interest is erased; it returns (a new life) and announces the
    // same filter.
    let mut h = hub();
    h.set_cluster_configured();
    let first = link(&mut h, "p", PROTO_MAX);
    announce(&mut h, &first, &["t/#"], &[]);
    h.peer_dead(&first.node);
    let (during_the_hold, _a) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
    assert_eq!(
        owed(&h, during_the_hold).len(),
        1,
        "owed to the peer held dead"
    );
    h.peer_gone(&first.node);
    tick(&mut h);
    assert!(!h.interest.has_node(&first.node), "the hold ended");
    assert!(
        owed(&h, during_the_hold).is_empty(),
        "and discharged what was owed"
    );
    let (after_1, _b) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
    let (after_2, _c) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
    let mut back = link(&mut h, "p", PROTO_MAX);
    let (linked, _d) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
    h.peer_announced_interest(&back.node, vec!["t/#".to_string()]);
    let order: Vec<u64> = sent(&mut back).into_iter().map(|(seq, _)| seq).collect();
    let expected: Vec<u64> = [during_the_hold, after_1, after_2, linked]
        .into_iter()
        .map(|id| owed(&h, id)[0].0)
        .collect();
    assert_eq!(order, expected, "(b) everything still held, oldest first");
}

/// A snapshot left over from a link the peer has since REPLACED opens nothing. The
/// snapshot commands carry the link they arrived on; one that names another link
/// than the node's current one is dropped — it is not the first thing the peer
/// said on THIS link, and a stale membership could name a consumer that has left.
#[tokio::test]
async fn a_snapshot_from_a_replaced_link_does_not_open_the_new_one() {
    let mut h = hub();
    let old = link_as(&mut h, "p", PROTO_MAX, 1);
    announce(&mut h, &old, &["t/#"], &["c"]);
    h.peer_disconnected(&old.node, 1);
    let (_, _a) = publish_ordinary(&mut h, "t/a", QoS::AtLeastOnce);
    let (_, _b) = publish_shared(&mut h);
    let mut new = link_as(&mut h, "p", PROTO_MAX, 2);

    let node = new.node.clone();
    let interest = |conn_id| HubCommand::RemoteInterest {
        node: node.clone(),
        conn_id: Some(conn_id),
        filters: vec!["t/#".to_string()],
        kinds: None,
    };
    let membership = |conn_id| HubCommand::RemoteSharedInterest {
        node: node.clone(),
        conn_id: Some(conn_id),
        groups: members(&["c"]),
    };
    h.dispatch(interest(1)).await;
    h.dispatch(membership(1)).await;
    assert!(
        sent(&mut new).is_empty(),
        "left over from the replaced link"
    );
    h.dispatch(interest(2)).await;
    assert_eq!(sent(&mut new).len(), 1, "the ordinary forward");
    h.dispatch(membership(2)).await;
    assert_eq!(sent(&mut new).len(), 1, "the `$share` delivery");
}

// ---- item 1: a `$share` delivery is acknowledged only when a member took it ----

/// A receiver with one peer link to the origin at `proto`; what it answers is read
/// from the link.
fn receiver(proto: u32, clustered: bool) -> (Hub, Link) {
    let (mut hub, _) = Hub::with_config(
        NodeId("so-receiver".into()),
        Arc::new(MemorySessionStore::new()),
    );
    if clustered {
        hub.set_cluster_configured();
    }
    let origin = link(&mut hub, "so-origin", proto);
    (hub, origin)
}

fn shared_delivery(seq: u64, member: &str) -> HubCommand {
    HubCommand::RemoteSharedDeliverAcked {
        node: NodeId("so-origin".into()),
        seq,
        client: ClientId(member.into()),
        topic: TOPIC.into(),
        payload: Bytes::from_static(b"x"),
        qos: QoS::AtLeastOnce,
        message_expiry: None,
        app: AppProperties::default(),
    }
}

fn verdicts(link: &mut Link) -> Vec<(u64, ForwardVerdict)> {
    std::iter::from_fn(|| link.rx.try_recv().ok())
        .filter_map(|frame| match frame {
            PeerMessage::PublishVerdict { seq, verdict } => Some((seq, verdict)),
            _ => None,
        })
        .collect()
}

/// The receiver's half. A `$share` delivery names one member; if nobody of that
/// name is on this node — not connected, and no session to queue for — nothing was
/// stored, and the origin is told so in a form it re-selects on. This arm used to
/// fall through to `Stored`, and the origin acknowledged its publisher for a
/// message nobody took.
///
/// MIXED VERSIONS. An origin that negotiated proto 13 or more reads the new code.
/// One below it — v1.1.0 speaks 12 — would read an unknown code as an answer it
/// cannot interpret and withhold, closing its publisher's connection; it is told
/// the refusal it already re-selects on instead (brownout's code: "nothing was
/// stored here"). A repeat of the delivery is answered the same way again.
#[tokio::test]
async fn a_shared_delivery_for_a_member_that_is_not_here_is_refused_not_answered_stored() {
    for (proto, code) in [
        (PROTO_MAX, REFUSAL_SHARED_MEMBER_GONE),
        (
            mqtt_cluster::peer::PROTO_SHARED_MEMBER_GONE,
            REFUSAL_SHARED_MEMBER_GONE,
        ),
        (PROTO_V1_1_0, PublishRefusal::Brownout.wire_code()),
        (PROTO_FORWARD_VERDICT, PublishRefusal::Brownout.wire_code()),
    ] {
        let (mut h, mut origin) = receiver(proto, false);
        assert!(
            !h.routing_unsettled(),
            "fixture: this node knows who is here"
        );
        for _ in 0..2 {
            h.dispatch(shared_delivery(7, "gone")).await;
            quiesce(&mut h).await;
        }
        assert_eq!(
            verdicts(&mut origin),
            vec![(7, ForwardVerdict::Refused { code }); 2],
            "proto {proto}: refused — and refused again for the repeat — never `Stored`"
        );
    }
}

/// A node that cannot yet say who is on it — mid-boot, or in a takeover window — is
/// not in a position to call a member gone: a session it is about to materialise
/// looks the same. It answers what an ordinary forward that matched nobody gets
/// then: `Failed`, which claims nothing, and the origin withholds.
#[tokio::test]
async fn a_node_whose_view_is_unsettled_does_not_call_a_share_member_gone() {
    let (mut h, mut origin) = receiver(PROTO_MAX, true);
    assert!(h.routing_unsettled(), "fixture: the view is unsettled");
    h.dispatch(shared_delivery(7, "not-yet")).await;
    quiesce(&mut h).await;
    assert_eq!(verdicts(&mut origin), vec![(7, ForwardVerdict::Failed)]);

    // "Unsettled" for a reason that says nothing about who is on THIS node — a
    // window opened by a link event, a peer out of reach — is not that. This node
    // has loaded its own sessions and, with no durable plane, none can arrive from
    // anywhere else: the member is gone, and it says so. (With the broader test,
    // one peer down made every gone member an acknowledgement withheld.)
    let (mut h, mut origin) = receiver(PROTO_MAX, true);
    h.interest_authoritative = true;
    h.last_scan_complete = true;
    h.takeover_reconcile_ticks = 3;
    assert!(
        h.routing_unsettled(),
        "fixture: the routing view is unsettled"
    );
    h.dispatch(shared_delivery(8, "gone")).await;
    quiesce(&mut h).await;
    assert_eq!(
        verdicts(&mut origin),
        vec![(
            8,
            ForwardVerdict::Refused {
                code: REFUSAL_SHARED_MEMBER_GONE
            }
        )]
    );
}

/// The likelier way for a consumer to be gone when the hub is busy: its connection
/// has ended, and its `Detach` is still queued behind the commands ahead of it — so
/// the hub still lists it as online when the delivery arrives. The send finds the
/// channel closed and reaps the session; with a clean session nobody took the
/// message, and the answer is the refusal, not `Stored` (which this path gave: the
/// first delivery after a consumer left was acknowledged to its publisher, and
/// only the later ones were refused). At `QoS` 1 and `QoS` 2.
///
/// A PERSISTENT session in the same state did take it: the message is in its
/// queue, and the answer is `Stored`.
#[tokio::test]
async fn a_shared_delivery_for_a_member_whose_connection_has_ended_is_refused() {
    for (persistent, qos) in [
        (false, QoS::AtLeastOnce),
        (false, QoS::ExactlyOnce),
        (true, QoS::AtLeastOnce),
    ] {
        let case = format!("persistent={persistent} qos={qos:?}");
        let store = Arc::new(MemorySessionStore::new());
        let (mut h, _) = Hub::with_config(NodeId("so-receiver".into()), store.clone());
        let mut origin = link(&mut h, "so-origin", PROTO_MAX);
        let member = ClientId("left".into());
        let (otx, orx) = mpsc::unbounded_channel::<Box<crate::hub::Outgoing>>();
        let (outbound, _meter) = Outbound::new(otx);
        let (reply, wait) = oneshot::channel();
        h.dispatch(HubCommand::Attach {
            client: member.clone(),
            admission: Box::new(admission("left")),
            conn_id: 7,
            clean_start: !persistent,
            session_expiry: if persistent { u32::MAX } else { 0 },
            receive_maximum: u16::MAX,
            will: None,
            outbound,
            reply,
        })
        .await;
        quiesce(&mut h).await;
        assert!(matches!(wait.await, Ok(AttachOutcome::Present(false))));
        // The connection task ends. Its `Detach` has not been dispatched.
        drop(orx);
        assert!(
            h.online.contains_key(&member),
            "{case}: still listed online"
        );

        h.dispatch(HubCommand::RemoteSharedDeliverAcked {
            node: NodeId("so-origin".into()),
            seq: 7,
            client: member.clone(),
            topic: TOPIC.into(),
            payload: Bytes::from_static(b"x"),
            qos,
            message_expiry: None,
            app: AppProperties::default(),
        })
        .await;
        quiesce(&mut h).await;
        let queued = store.pending(&member, 0, 16).await.unwrap_or_default();
        if persistent {
            assert_eq!(verdicts(&mut origin), vec![(7, ForwardVerdict::Stored)]);
            assert_eq!(queued.len(), 1, "{case}: in the session's queue");
        } else {
            assert_eq!(
                verdicts(&mut origin),
                vec![(
                    7,
                    ForwardVerdict::Refused {
                        code: REFUSAL_SHARED_MEMBER_GONE
                    }
                )],
                "{case}: nobody took it"
            );
            assert!(queued.is_empty(), "{case}");
        }
    }
}

/// CHARACTERISATION of what the receiver's test does not cover: it asks whether the
/// named client has a SESSION here, not whether it is still subscribed. A member
/// that unsubscribed from the group in the instant the delivery was in flight, and
/// is still connected, is handed the message and the origin hears `Stored` — the
/// selection was made while it was a member, and a broker may complete a delivery
/// it had started when an UNSUBSCRIBE arrives [MQTT-3.10.4-3]. (Across an outage
/// the origin's own check against the peer's membership snapshot covers it:
/// `a_share_delivery_held_for_a_member_that_left_during_the_outage_is_placed_again`.)
#[tokio::test]
async fn a_shared_delivery_for_a_connected_client_that_is_no_longer_subscribed_is_delivered() {
    let (mut h, mut origin) = receiver(PROTO_MAX, false);
    let (otx, mut orx) = mpsc::unbounded_channel::<Box<crate::hub::Outgoing>>();
    let (outbound, _meter) = Outbound::new(otx);
    let (reply, wait) = oneshot::channel();
    h.dispatch(HubCommand::Attach {
        client: ClientId("still-here".into()),
        admission: Box::new(admission("still-here")),
        conn_id: 7,
        clean_start: true,
        session_expiry: 0,
        receive_maximum: u16::MAX,
        will: None,
        outbound,
        reply,
    })
    .await;
    quiesce(&mut h).await;
    assert!(matches!(wait.await, Ok(AttachOutcome::Present(false))));
    while orx.try_recv().is_ok() {}

    h.dispatch(shared_delivery(7, "still-here")).await;
    quiesce(&mut h).await;
    assert_eq!(verdicts(&mut origin), vec![(7, ForwardVerdict::Stored)]);
    assert!(
        orx.try_recv().is_ok(),
        "and the client was handed the message"
    );
}

/// The origin's half of the same answer. The member is gone, nothing was stored:
/// the delivery is placed with another member of the group, and that member's
/// answer acknowledges the publisher.
#[test]
fn a_share_delivery_refused_because_its_member_is_gone_is_placed_with_another_member() {
    let mut h = hub();
    let mut p = peer(&mut h, "p", PROTO_MAX, &["c"]);
    let mut q = peer(&mut h, "q", PROTO_MAX, &["d"]);
    let (id, mut rx) = publish_shared(&mut h);
    let first = sent(&mut p);
    assert_eq!(first.len(), 1, "the rotation starts at p's member");

    h.forward_answered(
        &p.node,
        first[0].0,
        ForwardVerdict::Refused {
            code: REFUSAL_SHARED_MEMBER_GONE,
        },
    );
    assert!(unanswered(&mut rx), "neither acknowledged nor refused");
    let moved = sent(&mut q);
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].1.as_deref(), Some("d"), "placed with q's member");
    assert_eq!(owed(&h, id).len(), 1, "and owed to it alone");

    h.forward_answered(&q.node, moved[0].0, ForwardVerdict::Stored);
    assert!(accepted(&mut rx), "acknowledged when a member took it");
}

/// When the member that is gone was the group's last, the publish has no
/// subscriber in the group any more: it is acknowledged, as one published a moment
/// later — when the peer's new membership has arrived and there is no group — is.
/// The publisher's connection is not closed on it.
#[test]
fn a_share_delivery_whose_group_has_nobody_left_is_acknowledged() {
    let mut h = hub();
    let mut p = peer(&mut h, "p", PROTO_MAX, &["c"]);
    let (_, mut rx) = publish_shared(&mut h);
    let first = sent(&mut p);
    h.forward_answered(
        &p.node,
        first[0].0,
        ForwardVerdict::Refused {
            code: REFUSAL_SHARED_MEMBER_GONE,
        },
    );
    assert!(accepted(&mut rx));
    assert!(h.pending_publishes.is_empty() && h.forward_index.is_empty());
}

/// When the only other member is on a node that cannot be reached, the delivery is
/// placed with it — as a first placement would be — and held: the publisher is
/// acknowledged when that node is back and its member has taken the message.
#[test]
fn a_share_delivery_whose_member_is_gone_is_held_for_a_member_out_of_reach() {
    let mut h = hub();
    let mut p = peer(&mut h, "p", PROTO_MAX, &["c"]);
    let away = peer(&mut h, "r", PROTO_MAX, &["d"]);
    h.peer_disconnected(&away.node, 0);
    let (id, mut rx) = publish_shared(&mut h);
    let first = sent(&mut p);
    assert_eq!(first.len(), 1, "placed with the member that can be reached");

    h.forward_answered(
        &p.node,
        first[0].0,
        ForwardVerdict::Refused {
            code: REFUSAL_SHARED_MEMBER_GONE,
        },
    );
    assert!(unanswered(&mut rx), "held, not acknowledged");
    let now = owed(&h, id);
    assert_eq!(
        (now[0].1.as_str(), now[0].2.as_deref()),
        ("r", Some("d")),
        "owed to the member that is left"
    );

    let mut back = link(&mut h, "r", PROTO_MAX);
    announce(&mut h, &back, &[], &["d"]);
    assert_eq!(sent(&mut back), vec![(now[0].0, Some("d".to_string()))]);
    h.forward_answered(&back.node, now[0].0, ForwardVerdict::Stored);
    assert!(accepted(&mut rx));
}

/// A group that still has a member which was offered this publish earlier and
/// refused it is not a group with nobody: nothing can be claimed either way, and
/// the acknowledgement is withheld — the publisher retries, and the retry is
/// placed afresh.
#[test]
fn a_share_delivery_is_withheld_when_the_only_members_left_already_refused_it() {
    let mut h = hub();
    let mut p = peer(&mut h, "p", PROTO_MAX, &["c"]);
    let mut q = peer(&mut h, "q", PROTO_MAX, &["d"]);
    let (_, mut rx) = publish_shared(&mut h);
    let first = sent(&mut p);
    h.forward_answered(
        &p.node,
        first[0].0,
        ForwardVerdict::Refused {
            code: PublishRefusal::Brownout.wire_code(),
        },
    );
    let moved = sent(&mut q);
    assert_eq!(moved[0].1.as_deref(), Some("d"));
    h.forward_answered(
        &q.node,
        moved[0].0,
        ForwardVerdict::Refused {
            code: REFUSAL_SHARED_MEMBER_GONE,
        },
    );
    assert!(withheld(&mut rx), "neither acknowledged nor refused");
    assert!(h.pending_publishes.is_empty() && h.forward_index.is_empty());
}

/// The origin's own check, which is what covers a peer that would answer `Stored`
/// for anyone (v1.1.0): a `$share` delivery held for a member is not sent on a new
/// link until the peer's membership snapshot has arrived on it, and if the
/// snapshot no longer lists the member — the consumer disconnected or unsubscribed
/// during the outage, or the peer restarted — the delivery is placed again:
/// with another member there, or, with nobody left, acknowledged.
#[test]
fn a_share_delivery_held_for_a_member_that_left_during_the_outage_is_placed_again() {
    for proto in [PROTO_MAX, PROTO_V1_1_0] {
        for (now_there, goes_to) in [(&["d"][..], Some("d")), (&[][..], None)] {
            let case = format!("proto {proto}, members after the outage {now_there:?}");
            let mut h = hub();
            let first = peer(&mut h, "p", proto, &["c"]);
            h.peer_disconnected(&first.node, 0);
            let (id, mut rx) = publish_shared(&mut h);
            assert_eq!(
                owed(&h, id)[0].2.as_deref(),
                Some("c"),
                "{case}: held for c"
            );

            let mut back = link(&mut h, "p", proto);
            h.peer_announced_interest(&back.node, Vec::new());
            assert!(
                sent(&mut back).is_empty(),
                "{case}: not before the snapshot"
            );
            h.peer_announced_shared(&back.node, members(now_there));
            let frames = sent(&mut back);
            if let Some(member) = goes_to {
                assert_eq!(frames.len(), 1, "{case}");
                assert_eq!(
                    frames[0].1.as_deref(),
                    Some(member),
                    "{case}: sent to a member that is there, never to the one that left"
                );
                assert!(unanswered(&mut rx), "{case}");
                h.forward_answered(&back.node, frames[0].0, ForwardVerdict::Stored);
                assert!(accepted(&mut rx), "{case}");
            } else {
                assert!(frames.is_empty(), "{case}: nothing is sent to nobody");
                assert!(
                    accepted(&mut rx),
                    "{case}: the group has nobody — acknowledged as a publish \
                     nobody subscribes to"
                );
            }
        }
    }
}

/// A delivery the peer was SENT — in flight when the link went — may have been taken
/// by its member before the member left. When the first snapshot on the new link no longer lists
/// that member, such a delivery is left for the peer to answer, if the peer can:
/// it answers a repeat from its window, or says the member is gone, and only then
/// is the delivery placed with another member. Placing it again at the snapshot
/// could hand a second member a message the first one got.
///
/// A v1.1.0 peer (proto 12) cannot say "gone" — it would answer `Stored` for a
/// member that never got the message — so for it the delivery is placed again at
/// the snapshot, sent or not: a possible duplicate, over a certain loss.
#[test]
fn a_share_delivery_the_peer_was_sent_is_left_for_it_to_answer_when_it_can() {
    for proto in [PROTO_MAX, PROTO_V1_1_0] {
        let mut h = hub();
        let mut first = peer(&mut h, "p", proto, &["c"]);
        let mut q = peer(&mut h, "q", proto, &["d"]);
        let (_, mut rx) = publish_shared(&mut h);
        let in_flight = sent(&mut first);
        assert_eq!(in_flight.len(), 1, "proto {proto}: in flight to p's member");
        let seq = in_flight[0].0;
        h.peer_disconnected(&first.node, 0);

        // The link is back, and p no longer lists the member.
        let mut back = link(&mut h, "p", proto);
        announce(&mut h, &back, &[], &[]);
        if proto == PROTO_MAX {
            assert_eq!(
                sent(&mut back),
                vec![(seq, Some("c".to_string()))],
                "proto {proto}: sent again under its seq, for p to answer"
            );
            assert!(
                sent(&mut q).is_empty(),
                "proto {proto}: not placed again yet"
            );
            assert!(unanswered(&mut rx));
            // p never had it, and says so.
            h.forward_answered(
                &back.node,
                seq,
                ForwardVerdict::Refused {
                    code: REFUSAL_SHARED_MEMBER_GONE,
                },
            );
        } else {
            assert!(
                sent(&mut back).is_empty(),
                "proto {proto}: never sent to a member that is not listed"
            );
        }
        let moved = sent(&mut q);
        assert_eq!(moved.len(), 1, "proto {proto}");
        assert_eq!(moved[0].1.as_deref(), Some("d"), "proto {proto}");
        h.forward_answered(&q.node, moved[0].0, ForwardVerdict::Stored);
        assert!(accepted(&mut rx), "proto {proto}");
    }
}

/// A link cut with no verdict: the consumer a delivery is held for reconnects
/// through another node. The delivery follows it at the next tick. Until this the
/// per-tick re-selection ran only for a node held dead, so with a merely lost link
/// the delivery stayed pinned to the node the consumer had left.
///
/// What moves is a delivery that was never written to a link — nobody can have
/// received it. One that was IN FLIGHT when the link went stays with its node,
/// which answers for it when its link returns (a repeat from its window, or "that
/// member is gone"); it moves only if the node is declared dead, as it always has.
#[test]
fn a_share_delivery_follows_a_consumer_that_reconnected_through_another_node() {
    let mut h = hub();
    let mut p = peer(&mut h, "p", PROTO_MAX, &["c"]);
    let mut q = peer(&mut h, "q", PROTO_MAX, &[]);
    let (in_flight, mut rx_in_flight) = publish_shared(&mut h);
    assert_eq!(sent(&mut p).len(), 1, "sent to p's member before the cut");

    h.peer_disconnected(&p.node, 0);
    let (under_the_cut, mut rx_under_the_cut) = publish_shared(&mut h);
    tick(&mut h);
    assert_eq!(owed(&h, under_the_cut)[0].1, "p", "nowhere to move to yet");

    // The same consumer, connected to q now.
    h.peer_announced_shared(&q.node, members(&["c"]));
    tick(&mut h);
    let moved = sent(&mut q);
    assert_eq!(
        moved,
        vec![(owed(&h, under_the_cut)[0].0, Some("c".to_string()))],
        "the delivery made under the cut follows the consumer"
    );
    assert_eq!(
        owed(&h, in_flight)[0].1,
        "p",
        "the one p may already have delivered stays p's to answer"
    );
    h.forward_answered(&q.node, moved[0].0, ForwardVerdict::Stored);
    assert!(accepted(&mut rx_under_the_cut));
    assert!(unanswered(&mut rx_in_flight));

    // A verdict changes that, as it always has: every delivery owed to the node
    // moves, at least once.
    h.peer_dead(&p.node);
    let moved = sent(&mut q);
    assert_eq!(moved.len(), 1, "moved at the verdict");
    h.forward_answered(&q.node, moved[0].0, ForwardVerdict::Stored);
    assert!(accepted(&mut rx_in_flight));
}

// ---- item 2: a hold that runs out discharges nothing a kept member is owed ------

/// "b expires, c returns" — the mirror of
/// `a_share_delivery_does_not_move_from_one_unreachable_member_to_another`. The
/// group has a member on b and one on c, and neither node can be reached. The
/// delivery is owed to b's member; b is confirmed gone (ADR 0087 D2) and erased.
/// c is only out of reach — held dead and not confirmed gone, or merely without a
/// link — so the delivery is placed with c's member and held, and c's member takes
/// it when c is back. Until this, erasing b discharged the delivery and
/// acknowledged the publisher, and c's member was never offered the message.
///
/// And when c is confirmed gone too, that ends it: the group has nobody, and the
/// publisher is acknowledged — and the publish counted `no-shared-member` (D9).
#[test]
fn a_share_delivery_moves_to_a_member_on_a_kept_node_when_its_own_nodes_hold_runs_out() {
    for (c_is_dead, c_returns) in [(false, true), (true, true), (true, false)] {
        let case = format!("c_is_dead={c_is_dead} c_returns={c_returns}");
        let mut h = hub();
        let metrics = std::sync::Arc::new(mqtt_observability::metrics::Metrics::new("so"));
        h.attach_metrics(metrics.clone());
        let mut b = peer(&mut h, "b", PROTO_MAX, &["m-b"]);
        let c = peer(&mut h, "c", PROTO_MAX, &["m-c"]);
        let (id, mut rx) = publish_shared(&mut h);
        assert_eq!(sent(&mut b).len(), 1, "{case}: in flight to b's member");

        // Cut off from both.
        if c_is_dead {
            h.peer_dead(&c.node);
        } else {
            h.peer_disconnected(&c.node, 0);
        }
        h.peer_dead(&b.node);
        for _ in 0..4 {
            tick(&mut h);
        }
        assert_eq!(owed(&h, id)[0].1, "b", "{case}: c cannot be reached either");
        if c_is_dead {
            // c's link forms and goes again without c saying anything: it is
            // there, and its hold starts over.
            let _ = link(&mut h, "c", PROTO_MAX);
            h.peer_disconnected(&c.node, 0);
        }
        for _ in 4..REROUTE_GRACE_TICKS * 2 {
            assert!(
                unanswered(&mut rx),
                "{case}: held until b is confirmed gone"
            );
            tick(&mut h);
        }
        h.peer_gone(&b.node);
        tick(&mut h);
        assert!(
            !h.remote_shared.contains_key(&b.node),
            "{case}: b is confirmed gone and erased"
        );
        assert!(
            unanswered(&mut rx),
            "{case}: erasing b acknowledges nothing while the group has c's member"
        );
        let now = owed(&h, id);
        assert_eq!(
            (now[0].1.as_str(), now[0].2.as_deref()),
            ("c", Some("m-c")),
            "{case}: placed with the member that is left"
        );

        if c_returns {
            let mut back = link(&mut h, "c", PROTO_MAX);
            announce(&mut h, &back, &[], &["m-c"]);
            assert_eq!(
                sent(&mut back),
                vec![(now[0].0, Some("m-c".to_string()))],
                "{case}: c's member is offered it when c is back"
            );
            h.forward_answered(&back.node, now[0].0, ForwardVerdict::Stored);
            assert!(accepted(&mut rx), "{case}");
        } else {
            h.peer_gone(&c.node);
            tick(&mut h);
            assert!(
                accepted(&mut rx),
                "{case}: c is confirmed gone too — the group has nobody left"
            );
            assert!(h.pending_publishes.is_empty() && h.forward_index.is_empty());
            assert!(
                metrics.render().contains(
                    "mqttd_publish_resolved_undeliverable_total{reason=\"no-shared-member\"} 1"
                ),
                "{case}: and counted: {}",
                metrics.render()
            );
        }
    }
}

// ---- item 5: the drain ---------------------------------------------------------

/// A draining hub with metrics, one peer with a member of the drain test's group,
/// and a `$share` delivery the DRAIN gated (a rule's message about a client this
/// node is disconnecting — nobody will retry it, and the drain waits for it).
async fn drain_gated_delivery(
    unreachable_first: bool,
) -> (Hub, Arc<mqtt_observability::metrics::Metrics>, Link) {
    let mut h = hub();
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("so"));
    h.attach_metrics(metrics.clone());
    let l = link(&mut h, "p", PROTO_MAX);
    h.peer_announced_interest(&l.node, Vec::new());
    h.peer_announced_shared(
        &l.node,
        vec![RemoteSharedGroup {
            group: GROUP.into(),
            filter: "presence/#".into(),
            members: vec![(ClientId("m".into()), QoS::AtLeastOnce, true)],
        }],
    );
    if unreachable_first {
        h.peer_disconnected(&l.node, 0);
    }
    h.dispatch(HubCommand::Draining).await;
    h.dispatch(presence_message("presence/e")).await;
    quiesce(&mut h).await;
    assert_eq!(h.drain_gated.len(), 1, "fixture: the drain gated it");
    (h, metrics, l)
}

fn dropped_no_shared_member(metrics: &mqtt_observability::metrics::Metrics) -> bool {
    metrics
        .render()
        .lines()
        .any(|l| l.starts_with("mqttd_publish_dropped_total{reason=\"no-shared-member\"} 1"))
}

/// Issue #973, item 5 of the read-through of 0087-T2, first half. A drain-gated
/// `$share` delivery in flight to the group's only member when that member's node
/// is declared dead has no fallback — there is no link to hand it over on — and
/// ends there. It was dropped without a trace; it is counted now
/// (`no-shared-member`: the group exists and none of its members can be reached),
/// and the drain is not held for it.
#[tokio::test]
async fn a_drain_gated_share_delivery_with_nobody_to_hand_it_to_is_dropped_and_counted() {
    let (mut h, metrics, mut l) = drain_gated_delivery(false).await;
    assert_eq!(sent(&mut l).len(), 1, "in flight to the member");
    h.peer_dead(&l.node);
    assert!(
        h.pending_publishes.iter().all(|(_, p)| p.ack_released()),
        "nothing holds the drain barrier"
    );
    assert!(dropped_no_shared_member(&metrics), "{}", metrics.render());
}

/// The second half: a drain-gated `$share` delivery first placed with a member
/// behind a LOST LINK (no verdict). It waited there — nothing re-selected a
/// delivery owed to a node that was not held dead — and the drain waited with it
/// to its deadline. It now takes its fallback at the next tick, as one owed to a
/// node held dead does at the verdict; with no member that can be handed it, that
/// is the counted drop above.
#[tokio::test]
async fn a_drain_gated_share_delivery_behind_a_lost_link_does_not_hold_the_drain() {
    let (mut h, metrics, _l) = drain_gated_delivery(true).await;
    assert!(
        h.pending_publishes.iter().any(|(_, p)| !p.ack_released()),
        "owed to the member out of reach until the tick"
    );
    tick(&mut h);
    assert!(
        h.pending_publishes.iter().all(|(_, p)| p.ack_released()),
        "nothing holds the drain barrier"
    );
    assert!(dropped_no_shared_member(&metrics), "{}", metrics.render());
}
