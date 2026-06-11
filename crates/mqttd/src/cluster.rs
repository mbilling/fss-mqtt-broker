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
