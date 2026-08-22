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
//! - **`Dead`** — stop the dialer and tell the hub to drop the peer's routing
//!   state. Dropping the hub's outbound sender also closes an accepted-side link,
//!   so both directions converge without coordination.
//!
//! A member that refutes its suspicion comes back as another `Alive` event, which
//! restarts the dialer; redial-on-drop within a live membership is handled by
//! [`crate::peer::dial_forever`] itself.

use crate::hub::HubCommand;
use crate::peer;
use mqtt_cluster::placement::Placement;
use mqtt_cluster::swim::MemberState;
use mqtt_cluster::swim_driver::MembershipEvent;
use mqtt_cluster::NodeId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// React to membership events until the event channel closes.
///
/// `tls` is the cluster-bus mTLS context handed to every dialer; `None` means
/// the (loudly logged, testing-only) plaintext mesh. `placement`, when present,
/// is kept in sync with membership (ADR 0005) so the hub can identify session
/// owners.
pub async fn maintain_peer_links(
    mut events: mpsc::UnboundedReceiver<MembershipEvent>,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<peer::PeerTls>,
    placement: Option<Arc<RwLock<Placement>>>,
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
) {
    // Active dialer per peer we own the link to. The book aborts every dialer
    // when it drops — the dial tasks are children of THIS task, and must not
    // outlive it: a dialer that survives its node's shutdown (or a test
    // harness's in-process "crash") keeps redialing forever and pins every
    // resource its links capture (observed: a killed stress node's lease store
    // held open by a zombie dialer's durable-plane handle, ADR 0042 T4).
    // The address a dialer was spawned with is remembered alongside its handle: a
    // peer that returns at a NEW routing address is `Alive -> Alive`, so without
    // this the live dialer would keep redialing the address it started with for
    // the rest of the process's life (issue #92).
    struct DialerBook(HashMap<NodeId, (String, JoinHandle<()>)>);
    impl Drop for DialerBook {
        fn drop(&mut self) {
            for (_, (_, h)) in self.0.drain() {
                h.abort();
            }
        }
    }
    let mut dialers = DialerBook(HashMap::new());
    // Last-seen SWIM state per peer, for the members-by-state gauge (ADR 0020-T6). A
    // node reuses its stable id across restarts, so a rejoin overwrites its `Dead` entry.
    let mut member_states: HashMap<NodeId, MemberState> = HashMap::new();

    while let Some(ev) = events.recv().await {
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
                if let Some((addr, h)) = dialers.0.get(&ev.id) {
                    if !h.is_finished() {
                        if addr == &ev.peer_addr {
                            continue; // already dialing / linked at this address
                        }
                        // The peer moved: the running dialer is aiming at an address
                        // that no longer exists, so replace it.
                        info!(
                            peer = %ev.id.0, from = %addr, to = %ev.peer_addr,
                            "membership: peer routing address changed; redialing"
                        );
                        h.abort();
                    }
                }
                info!(peer = %ev.id.0, addr = %ev.peer_addr, "membership: peer alive; establishing link");
                let handle = tokio::spawn(peer::dial_forever(
                    ev.peer_addr.clone(),
                    local.clone(),
                    hub.clone(),
                    tls.clone(),
                    plane.clone(),
                ));
                dialers
                    .0
                    .insert(ev.id.clone(), (ev.peer_addr.clone(), handle));
            }
            MemberState::Suspect => {
                // No routing action (a transiently slow node loses nothing), but
                // say it: suspicion is the first observable step of every eviction.
                warn!(peer = %ev.id.0, "membership: peer SUSPECT (probes unanswered; routing continues pending refutation or timeout)");
            }
            MemberState::Dead => {
                if let Some((_, h)) = dialers.0.remove(&ev.id) {
                    h.abort();
                }
                warn!(peer = %ev.id.0, "membership: peer DEAD; dropping link and routing state");
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

    /// An `Alive` member is dialed; a later `Dead` aborts the dialer (closing
    /// the half-open link) and tells the hub to drop routing state.
    #[tokio::test]
    async fn alive_dials_and_dead_aborts_link_and_notifies_hub() {
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

        // Suspect changes nothing; Dead tears the link down.
        ev_tx.send(ev("b", &addr, MemberState::Suspect)).unwrap();
        ev_tx.send(ev("b", &addr, MemberState::Dead)).unwrap();
        match timeout(Duration::from_secs(2), hub_rx.recv())
            .await
            .unwrap()
        {
            Some(HubCommand::PeerDead { node }) => assert_eq!(node.0, "b"),
            other => panic!("expected PeerDead, got {other:?}"),
        }
        let n = timeout(Duration::from_secs(2), sock.read(&mut buf))
            .await
            .expect("aborted dialer should close its socket")
            .unwrap_or(0);
        assert_eq!(n, 0, "the dead peer's link must be closed");
    }

    /// A refuted suspicion (`Dead` then `Alive` again) restarts the dialer.
    #[tokio::test]
    async fn alive_after_dead_restarts_the_dialer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (ev_tx, mut hub_rx) = start("a");

        ev_tx.send(ev("b", &addr, MemberState::Alive)).unwrap();
        let _first = timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("first dial")
            .unwrap();

        ev_tx.send(ev("b", &addr, MemberState::Dead)).unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(2), hub_rx.recv())
                .await
                .unwrap(),
            Some(HubCommand::PeerDead { .. })
        ));

        ev_tx.send(ev("b", &addr, MemberState::Alive)).unwrap();
        let second = timeout(Duration::from_secs(2), listener.accept()).await;
        assert!(second.is_ok(), "rejoined member was not redialed");
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
        // ...and the undialable member's death still clears routing state.
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
