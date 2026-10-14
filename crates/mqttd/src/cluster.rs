//! SWIM-driven peer-link management: the glue between gossip membership and the
//! routing layer.
//!
//! [`maintain_peer_links`] consumes [`MembershipEvent`]s from the SWIM driver and
//! keeps the inter-node mesh in sync with the membership view:
//!
//! - **`Alive`** — if this node owns the link (same smaller-node-id tie-break as
//!   the peer handshake in [`crate::peer`]), start a dialer for the member's
//!   gossiped routing address. The other side just accepts.
//! - **`Suspect`** — no action; routing continues until failure is confirmed, so
//!   a transiently slow node loses nothing.
//! - **`Dead`** — tell the hub, which drops the link (dropping its outbound sender
//!   closes the link's pump on whichever side holds the socket) and holds the peer
//!   as unreachable: its routing state is kept, and what its subscribers are owed
//!   waits for it (ADR 0087 D1). The dialer is **not** stopped (D5): it is told
//!   the peer is held `Dead` and keeps dialing, backing off, until the peer is
//!   confirmed gone — see below.
//! - **confirmed gone** ([`MembershipEvent::gone`], ADR 0087 D2) — while a verdict
//!   stands, tell the hub ([`HubCommand::PeerGone`]), which resolves what it was
//!   holding for the peer, and stop the dialer dialing.
//!
//! A member that refutes its suspicion comes back as another `Alive` event, which
//! returns its dialer to the ordinary pace, or starts one; redial-on-drop is the
//! dialer's own business ([`crate::peer::dial_member`]). An `Alive` that follows a
//! `Dead` also tells the hub the verdict is withdrawn ([`HubCommand::PeerAlive`]).
//!
//! # A link is admitted by its handshake, not by membership
//!
//! A `Dead` verdict is this node's belief, and it can be false: the peer is alive
//! and the network between the two works (issues #971, #973, #975). Membership
//! therefore decides only **whom this node dials**, never whom it accepts: the
//! accept path ([`crate::peer::serve_listener`]) asks membership nothing, and a peer
//! this node holds `Dead` that completes the handshake — the cluster CA's mutual
//! TLS, the revocation list, the node id its certificate attests, a common protocol
//! version — is linked and served like any other. That was already so for a link
//! the *other* node dials; since ADR 0087 D5 it is so for a link this node dials
//! too, so a false verdict costs the data path one redial (about a second) whichever
//! of the pair owns the link, instead of the whole verdict when the node that is
//! wrong happens to be the one that dials.
//!
//! The verdict still stands while such a link is up, and it still means something:
//! membership says `Dead` once, so the hub remembers it, and when the link of a
//! peer held `Dead` goes, what the link brought back — the peer's interest, the
//! forwards in flight to it — is held for as long as it was at the verdict and no
//! longer. A node that announces its own departure is the plain case: it is
//! redialed while it drains, serves that link, and then exits without another word
//! from membership.
//!
//! # When a dialer does stop
//!
//! What ends a dialer's dialing is the peer being **confirmed gone** (ADR 0087 D2,
//! O1 as decided on 2026-10-14), which membership raises as an event of its own: the
//! tombstone pruned with the parting greeting (issue #971) unanswered — `dead_ttl`
//! after the verdict — or a new generation of the node id joining. The dialer then
//! dials no more; its task stays, waiting, and the next `Alive` (the peer's next
//! life, or a pruned member that was alive after all) sets it dialing again. An
//! `Alive` that names the peer at a **different routing address** replaces the
//! dialer, as before.
//!
//! The confirmation is told to the hub only while a verdict stands here. A new
//! generation of a member this node never held `Dead` (a restart nobody noticed) is
//! not the end of anything the hub is holding: the member's link and its interest
//! are those of a member, and the new life's link carries a fresh snapshot.

use crate::hub::HubCommand;
use crate::peer::{self, DialStanding, DialTarget};
use mqtt_cluster::placement::Placement;
use mqtt_cluster::swim::MemberState;
use mqtt_cluster::swim_driver::MembershipEvent;
use mqtt_cluster::NodeId;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// React to membership events until the event channel closes.
///
/// `tls` is the cluster-bus mTLS context handed to every dialer; `None` means
/// the (loudly logged, testing-only) plaintext mesh. `placement`, when present,
/// is kept in sync with membership (ADR 0005) so the hub can identify session
/// owners.
pub async fn maintain_peer_links(
    events: mpsc::UnboundedReceiver<MembershipEvent>,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<peer::PeerTls>,
    placement: Option<Arc<RwLock<Placement>>>,
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
) {
    maintain_peer_links_with_ingress(
        events, local, hub, tls, placement, metrics, plane, None, None,
    )
    .await;
}

/// One dialer of the link manager's book.
struct Dialer {
    /// The address it was spawned with: a peer that returns at a NEW routing address
    /// is `Alive -> Alive`, so without this the live dialer would keep redialing the
    /// address it started with for the rest of the process's life (issue #92).
    addr: String,
    task: JoinHandle<()>,
    /// Membership's standing of the peer, as the dialer reads it (ADR 0087 D5).
    standing: watch::Sender<DialStanding>,
}

/// [`maintain_peer_links`], with every dialed link charging the node's ingress credit
/// for inbound peer `QoS` 0 publishes (ADR 0082 T4). `None` charges nothing.
#[allow(clippy::too_many_arguments)] // the wiring seam above, plus the credit
pub async fn maintain_peer_links_with_ingress(
    events: mpsc::UnboundedReceiver<MembershipEvent>,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<peer::PeerTls>,
    placement: Option<Arc<RwLock<Placement>>>,
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
    ingress: Option<Arc<crate::ingress::IngressCredit>>,
    state: Option<Arc<crate::rule_state::Store>>,
) {
    maintain(
        events, local, hub, tls, placement, metrics, plane, ingress, state,
    )
    .await;
}

/// The link manager.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)] // one event loop; its three arms read together
async fn maintain(
    mut events: mpsc::UnboundedReceiver<MembershipEvent>,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<peer::PeerTls>,
    placement: Option<Arc<RwLock<Placement>>>,
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
    ingress: Option<Arc<crate::ingress::IngressCredit>>,
    state: Option<Arc<crate::rule_state::Store>>,
) {
    // Active dialer per peer we own the link to. The book aborts every dialer
    // when it drops — the dial tasks are children of THIS task, and must not
    // outlive it: a dialer that survives its node's shutdown (or a test
    // harness's in-process "crash") keeps redialing forever and pins every
    // resource its links capture (observed: a killed stress node's lease store
    // held open by a zombie dialer's durable-plane handle, ADR 0042 T4).
    struct DialerBook(HashMap<NodeId, Dialer>);
    impl Drop for DialerBook {
        fn drop(&mut self) {
            for (_, dialer) in self.0.drain() {
                dialer.task.abort();
            }
        }
    }
    let mut dialers = DialerBook(HashMap::new());
    // Last-seen SWIM state per peer, for the members-by-state gauge (ADR 0020-T6). A
    // node reuses its stable id across restarts, so a rejoin overwrites its `Dead` entry.
    let mut member_states: HashMap<NodeId, MemberState> = HashMap::new();
    // The peers a `Dead` verdict stands for: declared dead, and not since alive.
    // What the hub is told, and when, follows this and not `member_states`, which
    // only remembers the last event.
    let mut held_dead: HashSet<NodeId> = HashSet::new();
    // The peers confirmed gone while a verdict stood, and not since alive. The hub
    // may still hold one of them — confirmed while linked, it is resolved when that
    // link ends — so the `Alive` that follows is told to the hub too.
    let mut confirmed_gone: HashSet<NodeId> = HashSet::new();

    while let Some(ev) = events.recv().await {
        // Confirmation that a peer is gone (ADR 0087 D2) is not a change of state:
        // the member already left the view as `Dead`. It ends the verdict that
        // stands here, if one does: the hub resolves what it was holding for the
        // peer, and the dialer dials no more until the next `Alive`.
        if ev.gone {
            if held_dead.remove(&ev.id) {
                confirmed_gone.insert(ev.id.clone());
                warn!(peer = %ev.id.0, "membership: peer CONFIRMED GONE (tombstone pruned unanswered, or a new generation joined); resolving what was held for it");
                if let Some(dialer) = dialers.0.get(&ev.id) {
                    dialer.standing.send_replace(DialStanding::Gone);
                }
                let _ = hub.send(HubCommand::PeerGone { node: ev.id });
            }
            continue;
        }
        // Keep the placement ring in step with membership before routing reacts.
        if let Some(placement) = &placement {
            if let Ok(mut p) = placement.write() {
                p.observe(&ev.id, ev.state, &ev.peer_addr, ev.domain.as_deref());
            }
        }
        // Membership transitions log at WARN when they carry operational weight
        // (issue #368): a 40-minute asymmetric split was invisible at the default
        // RUST_LOG=warn because eviction and recovery logged at info. Transitions
        // are edge-triggered and bounded by cluster size — no flooding lever.
        let previous = member_states.insert(ev.id.clone(), ev.state);
        if let Some(m) = &metrics {
            publish_member_gauges(m, &member_states);
        }
        match ev.state {
            MemberState::Alive => {
                if matches!(previous, Some(MemberState::Suspect | MemberState::Dead)) {
                    warn!(peer = %ev.id.0, was = ?previous,
                        "membership: peer RECOVERED to alive");
                }
                // The verdict is withdrawn, and the hub is told — whichever node
                // owns the link: a peer held `Dead` may have had a link all along
                // (ADR 0087 D5), and from here on losing it is a member losing a
                // link, not the end of a node already written off.
                //
                // Decided by whether a verdict STANDS, not by what the last event
                // was: SWIM can raise `Suspect` between a `Dead` and the `Alive`
                // that ends it (a relayed suspicion about a member it has pruned, or
                // about a new generation of it, re-adds the member as suspect). The
                // hub would then keep the verdict for the rest of the member's life,
                // and treat every later loss of its link as the loss of a node
                // already written off.
                // (`|`, not `||`: both sets forget the peer.)
                if held_dead.remove(&ev.id) | confirmed_gone.remove(&ev.id) {
                    let _ = hub.send(HubCommand::PeerAlive {
                        node: ev.id.clone(),
                    });
                }
                // And its dialer, if it has one, is told — before anything below
                // can find a reason not to start one: an `Alive` that carries no
                // routing address yet withdraws the verdict all the same, and must
                // not leave the dialer counting down to the end of its window.
                if let Some(dialer) = dialers.0.get(&ev.id) {
                    dialer.standing.send_if_modified(|standing| {
                        let withdrawn = *standing != DialStanding::Member;
                        *standing = DialStanding::Member;
                        withdrawn
                    });
                }
                // One link per pair: only the smaller-id node dials (the same
                // tie-break the handshake enforces, applied early to avoid churn).
                if local.0 >= ev.id.0 {
                    continue;
                }
                if ev.peer_addr.is_empty() {
                    // Not fatal and not final: a peer first seen through relayed
                    // gossip may not carry its routing address yet. The address
                    // arriving later now raises its own event (issue #92), so this
                    // peer is picked up then rather than dropped for good.
                    warn!(peer = %ev.id.0, "peer is alive but gossiped no routing address yet; waiting for one");
                    continue;
                }
                if let Some(dialer) = dialers.0.get(&ev.id) {
                    if !dialer.task.is_finished() {
                        if dialer.addr == ev.peer_addr {
                            // Already dialing, or linked, at this address — or
                            // waiting, its window spent, for the standing it was
                            // just given above.
                            continue;
                        }
                        // The peer moved: the running dialer is aiming at an address
                        // that no longer exists, so replace it.
                        info!(
                            peer = %ev.id.0, from = %dialer.addr, to = %ev.peer_addr,
                            "membership: peer routing address changed; redialing"
                        );
                        dialer.task.abort();
                    }
                }
                info!(peer = %ev.id.0, addr = %ev.peer_addr, "membership: peer alive; establishing link");
                let (standing, standing_rx) = watch::channel(DialStanding::Member);
                let task = tokio::spawn(peer::dial_member(
                    ev.peer_addr.clone(),
                    local.clone(),
                    hub.clone(),
                    tls.clone(),
                    plane.clone(),
                    ingress.clone(),
                    state.clone(),
                    DialTarget {
                        node: ev.id.clone(),
                        standing: standing_rx,
                    },
                ));
                dialers.0.insert(
                    ev.id.clone(),
                    Dialer {
                        addr: ev.peer_addr.clone(),
                        task,
                        standing,
                    },
                );
            }
            MemberState::Suspect => {
                // No routing action (a transiently slow node loses nothing), but
                // say it: suspicion is the first observable step of every eviction.
                warn!(peer = %ev.id.0, "membership: peer SUSPECT (probes unanswered; routing continues pending refutation or timeout)");
            }
            MemberState::Dead => {
                // The dialer is NOT stopped (ADR 0087 D5): the verdict can be false,
                // and a link is how this node finds that out on the data path. It is
                // told, so that it backs off; it stops when the peer is confirmed
                // gone. A repeated `Dead` changes nothing.
                if let Some(dialer) = dialers.0.get(&ev.id) {
                    dialer.standing.send_if_modified(|standing| {
                        let newly = *standing == DialStanding::Member;
                        if newly {
                            *standing = DialStanding::HeldDead;
                        }
                        newly
                    });
                }
                held_dead.insert(ev.id.clone());
                confirmed_gone.remove(&ev.id);
                warn!(peer = %ev.id.0, "membership: peer DEAD; dropping its link and holding it as unreachable (what its subscribers are owed waits for it, until it answers or is confirmed gone)");
                let _ = hub.send(HubCommand::PeerDead { node: ev.id });
            }
        }
    }
}

/// Recompute the members-by-state gauge from the per-peer state map (ADR 0020-T6).
/// Counts the peers SWIM has reported on; this node itself is always alive and is
/// added to the `alive` bucket so the total matches the cluster size.
fn publish_member_gauges(
    metrics: &mqtt_observability::metrics::Metrics,
    member_states: &HashMap<NodeId, MemberState>,
) {
    let mut alive = 1usize; // this node
    let mut suspect = 0usize;
    let mut dead = 0usize;
    for state in member_states.values() {
        match state {
            MemberState::Alive => alive += 1,
            MemberState::Suspect => suspect += 1,
            MemberState::Dead => dead += 1,
        }
    }
    metrics.set_members_in_state("alive", alive);
    metrics.set_members_in_state("suspect", suspect);
    metrics.set_members_in_state("dead", dead);
}

#[cfg(test)]
mod tests {
    use super::maintain_peer_links;
    use crate::hub::HubCommand;
    use mqtt_cluster::swim::MemberState;
    use mqtt_cluster::swim_driver::MembershipEvent;
    use mqtt_cluster::NodeId;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    fn ev(id: &str, peer_addr: &str, state: MemberState) -> MembershipEvent {
        MembershipEvent {
            id: NodeId(id.into()),
            addr: format!("{id}-swim"),
            peer_addr: peer_addr.into(),
            state,
            domain: None,
            gone: false,
        }
    }

    /// The event membership raises when `id` is confirmed gone (ADR 0087 D2).
    fn gone(id: &str, peer_addr: &str) -> MembershipEvent {
        MembershipEvent {
            gone: true,
            ..ev(id, peer_addr, MemberState::Dead)
        }
    }

    /// Spawn the link manager for `local`, returning the event feed and the
    /// stream of hub commands it produces.
    fn start(
        local: &str,
    ) -> (
        mpsc::UnboundedSender<MembershipEvent>,
        mpsc::UnboundedReceiver<HubCommand>,
    ) {
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        tokio::spawn(maintain_peer_links(
            ev_rx,
            NodeId(local.into()),
            hub_tx,
            None,
            None,
            None,
            None,
        ));
        (ev_tx, hub_rx)
    }

    /// ADR 0020-T6: the members-by-state gauge tracks the SWIM event stream — this node
    /// counts as one `alive`, and a peer's state changes move it between buckets.
    #[tokio::test]
    async fn member_states_drive_the_gauge() {
        use std::sync::Arc;
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("t"));
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let (hub_tx, _hub_rx) = mpsc::unbounded_channel();
        tokio::spawn(maintain_peer_links(
            ev_rx,
            NodeId("a".into()),
            hub_tx,
            None,
            None,
            Some(metrics.clone()),
            None,
        ));

        // Two alive peers + self = 3 alive; then one goes suspect, then dead.
        ev_tx.send(ev("b", "b:7000", MemberState::Alive)).unwrap();
        ev_tx.send(ev("c", "c:7000", MemberState::Alive)).unwrap();
        wait_for(&metrics, "mqttd_members{state=\"alive\"} 3").await;

        ev_tx.send(ev("c", "c:7000", MemberState::Suspect)).unwrap();
        wait_for(&metrics, "mqttd_members{state=\"suspect\"} 1").await;
        wait_for(&metrics, "mqttd_members{state=\"alive\"} 2").await;

        ev_tx.send(ev("c", "c:7000", MemberState::Dead)).unwrap();
        wait_for(&metrics, "mqttd_members{state=\"dead\"} 1").await;
        wait_for(&metrics, "mqttd_members{state=\"suspect\"} 0").await;
    }

    /// Poll the rendered exposition until it contains `needle`, or panic after ~2s.
    /// (The link manager applies events on its own task; this awaits the effect.)
    async fn wait_for(metrics: &mqtt_observability::metrics::Metrics, needle: &str) {
        for _ in 0..200 {
            if metrics.render().contains(needle) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("gauge never reached `{needle}`:\n{}", metrics.render());
    }

    /// ADR 0087 D5. An `Alive` member is dialed; a later `Dead` tells the hub to
    /// drop the link — and the dialer is NOT stopped: when the link has gone it
    /// dials the peer again, with membership still holding the peer `Dead`. (Until
    /// this was so, a false verdict held by the node that owns the link lasted on
    /// the data path for as long as membership held it.)
    #[tokio::test]
    async fn alive_dials_and_dead_tells_the_hub_and_keeps_dialing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (ev_tx, mut hub_rx) = start("a"); // "a" < "b": we own the link

        ev_tx.send(ev("b", &addr, MemberState::Alive)).unwrap();
        let (mut sock, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("alive member was never dialed")
            .unwrap();
        // The dialer speaks first: its Hello arrives.
        let mut buf = [0u8; 256];
        let n = timeout(Duration::from_secs(2), sock.read(&mut buf))
            .await
            .expect("no Hello from dialer")
            .unwrap();
        assert!(n > 0);

        // Suspect changes nothing; Dead is told to the hub, which drops the link.
        ev_tx.send(ev("b", &addr, MemberState::Suspect)).unwrap();
        ev_tx.send(ev("b", &addr, MemberState::Dead)).unwrap();
        match timeout(Duration::from_secs(2), hub_rx.recv())
            .await
            .unwrap()
        {
            Some(HubCommand::PeerDead { node }) => assert_eq!(node.0, "b"),
            other => panic!("expected PeerDead, got {other:?}"),
        }
        // The link goes (here: the peer's end of it), and the peer is dialed again.
        drop(sock);
        let (mut again, _) = timeout(Duration::from_secs(20), listener.accept())
            .await
            .expect("a peer held dead must still be dialed")
            .unwrap();
        let n = timeout(Duration::from_secs(2), again.read(&mut buf))
            .await
            .expect("no Hello on the redial")
            .unwrap();
        assert!(n > 0);
    }

    /// ADR 0087 D2: the dialer of a peer held `Dead` dials until the peer is
    /// confirmed gone, and then no more — the hub is told — and the `Alive` that
    /// follows (the peer's next life) sets it dialing again. The peer here hangs up
    /// on every attempt, so each attempt is seen and none serves a link.
    #[tokio::test]
    async fn a_peer_confirmed_gone_is_dialed_no_more_until_alive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (ev_tx, mut hub_rx) = start("a");

        ev_tx.send(ev("b", &addr, MemberState::Alive)).unwrap();
        let first = timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("first dial")
            .unwrap();

        ev_tx.send(ev("b", &addr, MemberState::Dead)).unwrap();
        assert!(matches!(
            next_command(&mut hub_rx).await,
            HubCommand::PeerDead { .. }
        ));
        drop(first);
        // Under the verdict it keeps coming.
        for _ in 0..2 {
            drop(
                timeout(Duration::from_secs(20), listener.accept())
                    .await
                    .expect("a peer held dead is still dialed")
                    .unwrap(),
            );
        }

        ev_tx.send(gone("b", &addr)).unwrap();
        match next_command(&mut hub_rx).await {
            HubCommand::PeerGone { node } => assert_eq!(node.0, "b"),
            other => panic!("expected PeerGone, got {other:?}"),
        }
        // At most the attempt under way when the confirmation arrived; then six
        // seconds of silence, longer than any backoff.
        let mut after = 0;
        while let Ok(accepted) = timeout(Duration::from_secs(6), listener.accept()).await {
            drop(accepted.unwrap());
            after += 1;
            assert!(after <= 1, "a peer confirmed gone is still dialed");
        }

        ev_tx.send(ev("b", &addr, MemberState::Alive)).unwrap();
        let again = timeout(Duration::from_secs(2), listener.accept()).await;
        assert!(again.is_ok(), "the member's next life was not dialed");
        // The hub is told the member is alive: it may still hold a peer confirmed
        // gone while linked (`VerdictHold::GoneLinked`).
        assert!(matches!(
            next_command(&mut hub_rx).await,
            HubCommand::PeerAlive { .. }
        ));
    }

    /// The confirmation reaches the hub only while a verdict stands, and once: a new
    /// generation of a member never held `Dead` (a restart nobody noticed) is not
    /// the end of anything the hub holds. The `Alive` after a confirmation is told
    /// to the hub, once.
    #[tokio::test]
    async fn gone_is_told_to_the_hub_once_and_only_while_a_verdict_stands() {
        let (ev_tx, mut hub_rx) = start("z");
        ev_tx.send(ev("b", "b:7000", MemberState::Alive)).unwrap();
        ev_tx.send(gone("b", "b:7000")).unwrap();
        ev_tx.send(ev("b", "b:7000", MemberState::Dead)).unwrap();
        ev_tx.send(gone("b", "b:7000")).unwrap();
        ev_tx.send(gone("b", "b:7000")).unwrap();
        // The next `Alive` is told (the hub may hold the peer confirmed gone while
        // linked), once; then a verdict of its own, so the absences are observable.
        ev_tx.send(ev("b", "b:7000", MemberState::Alive)).unwrap();
        ev_tx.send(ev("b", "b:7000", MemberState::Alive)).unwrap();
        ev_tx.send(ev("b", "b:7000", MemberState::Dead)).unwrap();
        let mut told = Vec::new();
        for _ in 0..4 {
            told.push(next_command(&mut hub_rx).await);
        }
        assert!(
            matches!(
                &told[..],
                [
                    HubCommand::PeerDead { node: dead },
                    HubCommand::PeerGone { node: gone },
                    HubCommand::PeerAlive { node: alive },
                    HubCommand::PeerDead { node: again },
                ] if dead.0 == "b" && gone.0 == "b" && alive.0 == "b" && again.0 == "b"
            ),
            "{told:?}"
        );
    }

    /// `Alive` withdraws a `Dead` verdict at the hub whichever node owns the link —
    /// here the peer does ("z" > "b"), so this node has no dialer for it at all —
    /// and an `Alive` that withdraws nothing tells the hub nothing.
    #[tokio::test]
    async fn alive_after_dead_tells_the_hub_the_verdict_is_withdrawn() {
        let (ev_tx, mut hub_rx) = start("z");
        ev_tx.send(ev("b", "b:7000", MemberState::Alive)).unwrap();
        ev_tx.send(ev("b", "b:7000", MemberState::Suspect)).unwrap();
        ev_tx.send(ev("b", "b:7000", MemberState::Alive)).unwrap();
        ev_tx.send(ev("b", "b:7000", MemberState::Dead)).unwrap();
        ev_tx.send(ev("b", "b:7000", MemberState::Alive)).unwrap();
        let mut told = Vec::new();
        for _ in 0..2 {
            told.push(
                timeout(Duration::from_secs(2), hub_rx.recv())
                    .await
                    .expect("the hub is told of the verdict and of its withdrawal")
                    .unwrap(),
            );
        }
        assert!(
            matches!(
                &told[..],
                [HubCommand::PeerDead { node: dead }, HubCommand::PeerAlive { node: alive }]
                    if dead.0 == "b" && alive.0 == "b"
            ),
            "{told:?}"
        );
    }

    /// A member that returns at a new routing address is dialed there and no longer
    /// at the old one — the one form in which a new generation of a node id reaches
    /// the link manager — whether or not the old address was held `Dead` meanwhile.
    #[tokio::test]
    async fn alive_at_a_new_address_replaces_the_dialer_of_a_peer_held_dead() {
        let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let new = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (ev_tx, _hub_rx) = start("a");

        let old_addr = old.local_addr().unwrap().to_string();
        ev_tx.send(ev("b", &old_addr, MemberState::Alive)).unwrap();
        let first = timeout(Duration::from_secs(2), old.accept())
            .await
            .expect("first dial")
            .unwrap();
        ev_tx.send(ev("b", &old_addr, MemberState::Dead)).unwrap();
        drop(first);

        let new_addr = new.local_addr().unwrap().to_string();
        ev_tx.send(ev("b", &new_addr, MemberState::Alive)).unwrap();
        assert!(
            timeout(Duration::from_secs(2), new.accept()).await.is_ok(),
            "the member was not dialed at its new address"
        );
        // Whatever the old dialer had under way has landed by now or never will:
        // drain it, then the old address stays quiet for longer than a redial takes.
        while timeout(Duration::from_millis(100), old.accept())
            .await
            .is_ok()
        {}
        assert!(
            timeout(Duration::from_millis(1500), old.accept())
                .await
                .is_err(),
            "the old address is still being dialed"
        );
    }

    /// The next hub command, which must come.
    async fn next_command(hub_rx: &mut mpsc::UnboundedReceiver<HubCommand>) -> HubCommand {
        timeout(Duration::from_secs(20), hub_rx.recv())
            .await
            .expect("the link manager told the hub nothing")
            .expect("the link manager is running")
    }

    /// SWIM can raise `Suspect` between a `Dead` and the `Alive` that ends it: once
    /// the tombstone is pruned, a relayed suspicion about the member re-adds it as
    /// suspect; and a relayed suspicion about a NEW generation of it supersedes the
    /// tombstone at once (that one usually returns at a new routing address). The
    /// verdict is withdrawn at the hub all the same — what decides is that a verdict
    /// stands, not what the last event was. (When it was the last event that
    /// decided, the hub kept the verdict for the rest of the member's life, and
    /// every later loss of its link was handled as the loss of a node already
    /// written off.) For the node that dials the pair and for the one that does not.
    #[tokio::test]
    async fn a_suspect_between_dead_and_alive_still_withdraws_the_verdict_at_the_hub() {
        for local in ["a", "z"] {
            for returns_at in ["b:7000", "b-reborn:7000"] {
                let (ev_tx, mut hub_rx) = start(local);
                ev_tx.send(ev("b", "b:7000", MemberState::Alive)).unwrap();
                ev_tx.send(ev("b", "b:7000", MemberState::Dead)).unwrap();
                ev_tx
                    .send(ev("b", returns_at, MemberState::Suspect))
                    .unwrap();
                ev_tx.send(ev("b", returns_at, MemberState::Alive)).unwrap();
                // A second `Alive` withdraws nothing, and says nothing.
                ev_tx.send(ev("b", returns_at, MemberState::Alive)).unwrap();
                // Then a verdict of its own, so the absence above is observable.
                ev_tx.send(ev("b", returns_at, MemberState::Dead)).unwrap();
                let mut told = Vec::new();
                for _ in 0..3 {
                    told.push(next_command(&mut hub_rx).await);
                }
                assert!(
                    matches!(
                        &told[..],
                        [
                            HubCommand::PeerDead { node: dead },
                            HubCommand::PeerAlive { node: alive },
                            HubCommand::PeerDead { node: again },
                        ] if dead.0 == "b" && alive.0 == "b" && again.0 == "b"
                    ),
                    "local={local} returns_at={returns_at}: {told:?}"
                );
            }
        }
    }

    /// An `Alive` that carries no routing address yet withdraws the verdict all the
    /// same — for the dialer too. A dialer left believing the peer `Dead` would back
    /// off — 0.5 s, 1 s, 2 s, 4 s, 5 s between attempts, 12.5 s for five; told the
    /// truth, it goes on at the ordinary pace, 0.5 s.
    #[tokio::test]
    async fn alive_without_a_routing_address_still_withdraws_the_verdict_from_the_dialer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (ev_tx, mut hub_rx) = start("a");

        ev_tx.send(ev("b", &addr, MemberState::Alive)).unwrap();
        let first = timeout(Duration::from_secs(20), listener.accept())
            .await
            .expect("first dial")
            .unwrap();
        ev_tx.send(ev("b", &addr, MemberState::Dead)).unwrap();
        ev_tx.send(ev("b", "", MemberState::Alive)).unwrap();
        assert!(matches!(
            next_command(&mut hub_rx).await,
            HubCommand::PeerDead { .. }
        ));
        assert!(matches!(
            next_command(&mut hub_rx).await,
            HubCommand::PeerAlive { .. }
        ));
        drop(first);

        // Every attempt is hung up on, and they keep coming at the ordinary pace.
        let hung_up = std::time::Instant::now();
        for attempt in 1..=5 {
            let accepted = timeout(Duration::from_secs(20), listener.accept())
                .await
                .unwrap_or_else(|_| panic!("the dialer stopped after {} attempt(s)", attempt - 1));
            drop(accepted.unwrap());
        }
        assert!(
            hung_up.elapsed() < Duration::from_secs(8),
            "five attempts at the ordinary pace, not the held-dead backoff: {:?}",
            hung_up.elapsed()
        );
    }

    /// One link per pair: the larger node id never dials (the peer owns it).
    #[tokio::test]
    async fn larger_node_id_does_not_dial() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (ev_tx, _hub_rx) = start("z"); // "z" > "b": the peer dials us

        ev_tx.send(ev("b", &addr, MemberState::Alive)).unwrap();
        let dialed = timeout(Duration::from_millis(400), listener.accept()).await;
        assert!(dialed.is_err(), "the larger-id side must not dial");
    }

    /// An `Alive` member that gossiped no routing address cannot be dialed and
    /// must be skipped without wedging the manager.
    #[tokio::test]
    async fn alive_without_routing_address_is_skipped() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (ev_tx, mut hub_rx) = start("a");

        ev_tx.send(ev("b", "", MemberState::Alive)).unwrap();
        // The manager is still serving events: a dialable member works...
        ev_tx.send(ev("c", &addr, MemberState::Alive)).unwrap();
        assert!(
            timeout(Duration::from_secs(2), listener.accept())
                .await
                .is_ok(),
            "manager wedged after the undialable member"
        );
        // ...and the undialable member's death is still told to the hub.
        ev_tx.send(ev("b", "", MemberState::Dead)).unwrap();
        match timeout(Duration::from_secs(2), hub_rx.recv())
            .await
            .unwrap()
        {
            Some(HubCommand::PeerDead { node }) => assert_eq!(node.0, "b"),
            other => panic!("expected PeerDead for b, got {other:?}"),
        }
    }
}
