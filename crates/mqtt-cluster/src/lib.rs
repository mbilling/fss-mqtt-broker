//! Shared-nothing clustering layer.
//!
//! Design goal: **linear scalability**. There is no coordinator on the publish
//! hot path. Each node owns its local subscribers and gossips a compact
//! *subscription digest* so peers route a PUBLISH only to nodes that might have
//! matching subscribers.
//!
//! This crate provides [`hrw`] placement, the [`peer`] wire protocol, and SWIM
//! gossip membership ([`swim`] state machine + [`swim_driver`] UDP shell), plus
//! the cluster *interface* traits. SWIM gossips each member's routing address so
//! the broker can establish peer links dynamically; a statically-configured peer
//! set remains available as a fallback.

pub mod hrw;
pub mod peer;
pub mod swim;
pub mod swim_auth;
pub mod swim_driver;

use mqtt_core::Message;

/// Stable identifier for a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// The health of a peer as tracked by the failure detector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeHealth {
    /// Responding to gossip; eligible for routing.
    Alive,
    /// Missed probes; suspected down (still gossiped, not yet removed).
    Suspect,
    /// Confirmed failed; its sessions are eligible for takeover.
    Dead,
}

/// Cluster membership view, maintained by a SWIM-style gossip protocol.
pub trait Membership: Send + Sync {
    /// This node's identity.
    fn local(&self) -> &NodeId;
    /// Currently known peers and their health.
    fn peers(&self) -> Vec<(NodeId, NodeHealth)>;
}

/// Routes a published message to peer nodes that may have matching subscribers.
///
/// Implementations consult gossiped subscription digests to avoid fanning a
/// message out to every node — the key to keeping fan-out cost sub-linear.
pub trait Router: Send + Sync {
    /// Return the set of peers that should receive `message`, excluding the local
    /// node (local delivery is handled by the core directly).
    fn route(&self, message: &Message) -> Vec<NodeId>;
}

/// Errors from cluster transport.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// The target peer is not reachable.
    #[error("peer unreachable: {0:?}")]
    Unreachable(NodeId),
    /// A transport-level failure.
    #[error("cluster transport error: {0}")]
    Transport(String),
}
