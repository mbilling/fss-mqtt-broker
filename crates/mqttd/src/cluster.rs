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
use mqtt_cluster::swim::MemberState;
use mqtt_cluster::swim_driver::MembershipEvent;
use mqtt_cluster::NodeId;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// React to membership events until the event channel closes.
///
/// `tls` is the cluster-bus mTLS context handed to every dialer; `None` means
/// the (loudly logged, testing-only) plaintext mesh.
pub async fn maintain_peer_links(
    mut events: mpsc::UnboundedReceiver<MembershipEvent>,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<peer::PeerTls>,
) {
    // Active dialer per peer we own the link to.
    let mut dialers: HashMap<NodeId, JoinHandle<()>> = HashMap::new();

    while let Some(ev) = events.recv().await {
        match ev.state {
            MemberState::Alive => {
                // One link per pair: only the smaller-id node dials (the same
                // tie-break the handshake enforces, applied early to avoid churn).
                if local.0 >= ev.id.0 {
                    continue;
                }
                if ev.peer_addr.is_empty() {
                    warn!(peer = %ev.id.0, "peer is alive but gossiped no routing address; cannot dial");
                    continue;
                }
                if let Some(h) = dialers.get(&ev.id) {
                    if !h.is_finished() {
                        continue; // already dialing / linked
                    }
                }
                info!(peer = %ev.id.0, addr = %ev.peer_addr, "membership: peer alive; establishing link");
                let handle = tokio::spawn(peer::dial_forever(
                    ev.peer_addr.clone(),
                    local.clone(),
                    hub.clone(),
                    tls.clone(),
                ));
                dialers.insert(ev.id.clone(), handle);
            }
            MemberState::Suspect => {}
            MemberState::Dead => {
                if let Some(h) = dialers.remove(&ev.id) {
                    h.abort();
                }
                info!(peer = %ev.id.0, "membership: peer dead; dropping link");
                let _ = hub.send(HubCommand::PeerDead { node: ev.id });
            }
        }
    }
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
        ));
        (ev_tx, hub_rx)
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
