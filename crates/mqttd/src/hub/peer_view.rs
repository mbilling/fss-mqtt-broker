//! ADR 0093 Amendment 5: the node's view of its peers, on every shard.
//!
//! Shard 0 is the node's face to its peers. The link tasks and membership tell it
//! (`HubCommand::route` sends every peer lifecycle command there), and its sweeps are the
//! ones that end a link: a revoked or expired certificate, a hold's countdown. After each
//! change to its view it sends the change to every other shard as a [`PeerView`], in the
//! order it made them, and each shard applies it to its own copy and does what the change
//! means for its own pending publishes: re-routes, flushes, holds, `$share` moves, the
//! takeover window. So every shard forwards its own publishes, deals to the peers' `$share`
//! members and settles its own pending publishes against the same view. What happens once
//! per node — the durable plane's links, the snapshots a new link is sent, the retained
//! digest, the rules' origin flag, the capability flags — happens on shard 0 alone.
//!
//! A shard other than 0 holds a link's lanes weakly ([`LinkLane::Shared`]): a link closes
//! when shard 0 and the durable plane let go of it, as it did with one loop.
//!
//! With one shard nothing here runs: there is nobody to tell, and every lane is the
//! link's own.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use mqtt_cluster::peer::PeerMessage;
use mqtt_cluster::NodeId;
use tokio::sync::mpsc;

use super::{Hub, HubCommand, PeerOutbound, RemoteSharedGroup};

/// One lane of a peer link, as a shard holds it (ADR 0093 Amendment 5).
#[derive(Debug, Clone)]
pub enum LinkLane {
    /// The link's own sender, held by shard 0: the link stays open while it is held.
    Owner(PeerOutbound),
    /// A weak copy, held by every other shard: a frame sent through it goes out while the
    /// link is open, and holding it keeps nothing open.
    Shared(mpsc::WeakUnboundedSender<PeerMessage>),
}

impl LinkLane {
    /// Queue `msg` on the link. Fails, handing `msg` back, once the link has closed: for
    /// a shared lane, also once shard 0 has let go of it.
    #[allow(clippy::result_large_err)] // the channel's own error, as `PeerOutbound::send`'s
    pub(super) fn send(&self, msg: PeerMessage) -> Result<(), mpsc::error::SendError<PeerMessage>> {
        match self {
            Self::Owner(tx) => tx.send(msg),
            Self::Shared(weak) => match weak.upgrade() {
                Some(tx) => tx.send(msg),
                None => Err(mpsc::error::SendError(msg)),
            },
        }
    }

    /// Whether the link is gone: its pump has stopped, or nobody keeps it open.
    pub(super) fn is_closed(&self) -> bool {
        match self {
            Self::Owner(tx) => tx.is_closed(),
            Self::Shared(weak) => weak.upgrade().is_none_or(|tx| tx.is_closed()),
        }
    }

    /// The copy another shard holds.
    pub(super) fn shared(&self) -> Self {
        match self {
            Self::Owner(tx) => Self::Shared(tx.downgrade()),
            Self::Shared(weak) => Self::Shared(weak.clone()),
        }
    }
}

/// A change shard 0 made to the node's view of its peers, which every other shard repeats
/// (ADR 0093 Amendment 5).
#[derive(Debug)]
pub enum PeerView {
    /// A link to `node` formed (`HubCommand::PeerConnected`).
    Linked {
        /// The peer.
        node: NodeId,
        /// The link's connection id.
        conn_id: u64,
        /// Its bulk lane.
        tx: LinkLane,
        /// Its control lane.
        ctl: LinkLane,
        /// The frames queued on its lanes, published by its pump.
        depth: Arc<AtomicUsize>,
        /// The peer-bus protocol version it negotiated.
        proto: u32,
    },
    /// Link `conn_id` to `node` ended: the link task said so, or shard 0 ended it (a
    /// revoked or expired certificate).
    LinkLost {
        /// The peer.
        node: NodeId,
        /// The link that ended.
        conn_id: u64,
    },
    /// Membership declared `node` dead.
    Dead {
        /// The peer.
        node: NodeId,
    },
    /// Membership holds `node` alive again.
    Alive {
        /// The peer.
        node: NodeId,
    },
    /// Membership confirmed `node` gone.
    Gone {
        /// The peer.
        node: NodeId,
    },
    /// The hold of `node`, held dead with no link, ran out.
    HoldExpired {
        /// The peer.
        node: NodeId,
    },
    /// The link of `node`, confirmed gone, outlived its countdown and was dropped.
    GoneLinkDropped {
        /// The peer.
        node: NodeId,
    },
    /// `node`'s interest snapshot, accepted on its current link.
    Interest {
        /// The peer.
        node: NodeId,
        /// The filters its sessions hold.
        filters: Arc<Vec<String>>,
    },
    /// `node`'s `$share` membership snapshot, accepted on its current link.
    Shared {
        /// The peer.
        node: NodeId,
        /// Its groups and their members.
        groups: Arc<Vec<RemoteSharedGroup>>,
    },
    /// Whether `node`'s rules select a message event.
    MessageEvents {
        /// The peer.
        node: NodeId,
        /// What it said.
        wanted: bool,
    },
}

impl Hub {
    /// Whether this shard is the node's face to its peers: shard 0, which hears from the
    /// links and from membership and does what happens once per node (ADR 0093
    /// Amendment 5). With one shard, always.
    pub(super) fn peer_face(&self) -> bool {
        self.shard == 0
    }

    /// Whether this shard tells others what changed in its peer view: shard 0 of more
    /// than one.
    pub(super) fn tells_shards(&self) -> bool {
        self.shard == 0 && self.shard_count > 1
    }

    /// Send `view()` to every other shard, in the order of the calls.
    pub(super) fn tell_shards(&self, view: impl Fn() -> PeerView) {
        if !self.tells_shards() {
            return;
        }
        for tx in self.shard_txs.iter().skip(1) {
            let _ = tx.send(HubCommand::PeerView(view()));
        }
    }

    /// Repeat a change shard 0 made to its peer view.
    pub(super) fn apply_peer_view(&mut self, view: PeerView) {
        match view {
            PeerView::Linked {
                node,
                conn_id,
                tx,
                ctl,
                depth,
                proto,
            } => {
                // The link this one replaces is shard 0's to keep open for its last
                // frames; here it is only let go.
                let _ = self.link_up(node, conn_id, tx, ctl, depth, proto, None);
            }
            PeerView::LinkLost { node, conn_id } => {
                if self.peers.get(&node).map(|p| p.conn_id) == Some(conn_id) {
                    self.peers.remove(&node);
                    self.mark_shared_unreachable(&node);
                    self.recompute_mesh();
                    self.hold_after_link_lost(&node);
                }
            }
            PeerView::Dead { node } => {
                let _ = self.dead(&node);
            }
            PeerView::Alive { node } => {
                self.held_dead.remove(&node);
            }
            PeerView::Gone { node } => self.gone(&node),
            PeerView::HoldExpired { node } => self.hold_expired(&node),
            PeerView::GoneLinkDropped { node } => {
                if self.peers.remove(&node).is_some() {
                    self.mark_shared_unreachable(&node);
                    self.recompute_mesh();
                }
                self.gone_link_dropped(&node);
            }
            PeerView::Interest { node, filters } => self.interest_announced(&node, &filters),
            PeerView::Shared { node, groups } => {
                self.shared_announced(&node, Arc::unwrap_or_clone(groups));
            }
            PeerView::MessageEvents { node, wanted } => {
                let _ = self.note_wants_origin(&node, wanted);
            }
        }
    }
}
