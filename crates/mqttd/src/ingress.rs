//! Ingress credit for client publishes (ADR 0082 T3).
//!
//! The hub's data lane was unbounded: every publish a connection read went straight in,
//! however far behind the hub was, and the #504 cloud run measured where that ends —
//! 2.5M queued commands, 10 GB, and a node frozen at its cgroup `MemoryHigh`. Credit
//! bounds it at the producer.
//!
//! - A node-wide **pool** of bytes, and a **per-connection cap** so one hot publisher
//!   cannot hold the whole pool.
//! - A client publish acquires [`cost`] bytes from both before it is handed to the hub.
//!   The [`IngressPermit`] travels **inside** `HubCommand::Publish` and is released when
//!   the hub drops the command after dispatching it. The hub never acquires credit, so
//!   no wait here can depend on the hub's own progress: there is no credit cycle.
//! - A connection without credit **stops reading its socket** (TCP backpressure) — for
//!   `QoS` 0 too under [`OverloadMode::Pause`]; under [`OverloadMode::ShedQos0`] a `QoS`
//!   0 publish is dropped and counted instead. `QoS` 1 and 2 always wait: a publish the
//!   broker will acknowledge is never shed to satisfy a memory bound.
//!
//! Peer links (ADR 0082 T4, §3) draw on the same pool, with no per-link cap. A peer's
//! `QoS` 0 publish without credit is **shed**, never paused: a peer link carries
//! consensus and replication frames, and pausing its reads would stall the cluster
//! behind best-effort traffic. Peer `QoS` 1 and 2, retained publishes and every
//! control frame are uncharged.
//!
//! Only client publishes and peer `QoS` 0 publishes are charged. Acks, subscriptions,
//! pings and every control command are not, and neither is anything the hub sends
//! itself.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// What one queued publish costs beyond its topic, payload and properties (ADR 0082
/// T1), measured by `tests/ingress_cost.rs` under the release allocator.
///
/// Under overload the hub holds its backlog in a lane, a `VecDeque<HubCommand>` whose
/// capacity doubles. Just past a doubling, half its slots are empty, so a queued
/// command can cost **two** slots. The rest covers the topic's own allocation, the
/// frame's share of the connection's read buffer (the payload is a zero-copy slice of
/// it) and allocator rounding. Derived from the slot size, so the charge follows the
/// command if it grows.
///
/// Measured 2026-10-02 with the 408-byte slot: 455-610 B beyond topic and payload while
/// queued on the channel, 731-1,076 B in a lane at its worst point. With the slot cut
/// to 248 B (#835): 256-412 B on the channel, up to 552 B in a lane.
pub const COMMAND_OVERHEAD: usize = 2 * std::mem::size_of::<crate::hub::HubCommand>() + 384;

/// The pool when neither it nor a memory watermark is configured (ADR 0082 §5).
pub const DEFAULT_POOL_BYTES: usize = 256 * 1024 * 1024;

/// The per-connection cap when unconfigured (ADR 0082 §5).
pub const DEFAULT_CONN_BYTES: usize = 1024 * 1024;

/// What a connection does with a `QoS` 0 publish that has no credit (ADR 0082 §2a).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverloadMode {
    /// Stop reading until credit frees. Lossless; under sustained overload a client
    /// with a short PINGREQ timeout reconnects.
    Pause,
    /// Keep reading and drop it, counted as `publish_dropped{reason="hub-ingress"}`.
    ShedQos0,
}

impl OverloadMode {
    /// Parse the config value (`pause` | `shed-qos0`); `None` = the default, `pause`.
    #[must_use]
    pub fn from_config(v: Option<&str>) -> Self {
        match v {
            Some("shed-qos0") => Self::ShedQos0,
            _ => Self::Pause,
        }
    }
}

/// The node's ingress credit: the pool, the per-connection cap and the overload mode.
#[derive(Debug)]
pub struct IngressCredit {
    pool: Arc<Semaphore>,
    pool_bytes: usize,
    conn_bytes: usize,
    mode: OverloadMode,
    /// Peer `QoS` 0 publishes shed since the hub last took the count (T4).
    peer_shed: AtomicU64,
}

impl IngressCredit {
    /// A pool of `pool_bytes` with a per-connection cap of `conn_bytes` (clamped to the
    /// pool).
    #[must_use]
    pub fn new(pool_bytes: usize, conn_bytes: usize, mode: OverloadMode) -> Self {
        let pool_bytes = pool_bytes.clamp(1, Semaphore::MAX_PERMITS);
        Self {
            pool: Arc::new(Semaphore::new(pool_bytes)),
            pool_bytes,
            conn_bytes: conn_bytes.clamp(1, pool_bytes),
            mode,
            peer_shed: AtomicU64::new(0),
        }
    }

    /// The pool size from config: explicit, else 1/8 of the memory watermark, else
    /// [`DEFAULT_POOL_BYTES`].
    #[must_use]
    pub fn pool_from_config(explicit: Option<u64>, memory_max: Option<u64>) -> usize {
        let bytes = explicit
            .or_else(|| memory_max.map(|m| m / 8))
            .map_or(DEFAULT_POOL_BYTES as u64, |b| b.max(4096));
        usize::try_from(bytes).unwrap_or(usize::MAX)
    }

    /// The overload mode.
    #[must_use]
    pub fn mode(&self) -> OverloadMode {
        self.mode
    }

    /// The pool size, bytes.
    #[must_use]
    pub fn pool_bytes(&self) -> usize {
        self.pool_bytes
    }

    /// Bytes currently held by queued publishes.
    #[must_use]
    pub fn in_use(&self) -> usize {
        self.pool_bytes
            .saturating_sub(self.pool.available_permits())
    }

    /// A connection's view: its own cap over the shared pool.
    #[must_use]
    pub fn connection(self: &Arc<Self>) -> ConnCredit {
        ConnCredit {
            node: self.clone(),
            conn: Arc::new(Semaphore::new(self.conn_bytes)),
        }
    }

    /// Take `cost` bytes from the pool alone, now, or nothing (ADR 0082 T4). Peer links
    /// have no per-link cap: one link carries many publishers' traffic.
    #[must_use]
    pub fn try_acquire_pool(&self, cost: u32) -> Option<IngressPermit> {
        let pool = self.pool.clone().try_acquire_many_owned(cost).ok()?;
        Some(IngressPermit {
            _conn: None,
            _pool: pool,
        })
    }

    /// Count one peer `QoS` 0 publish shed for want of credit.
    pub fn note_peer_shed(&self) {
        self.peer_shed.fetch_add(1, Ordering::Relaxed);
    }

    /// The peer publishes shed since the last call, resetting the count: the hub's
    /// sweep moves them into `publish_dropped{reason="hub-ingress"}`.
    #[must_use]
    pub fn take_peer_shed(&self) -> u64 {
        self.peer_shed.swap(0, Ordering::Relaxed)
    }

    /// What a publish of `topic_len` and `payload_len` bytes costs, clamped to the
    /// per-connection cap so even the largest message can eventually proceed. The
    /// payload length includes the publish's properties (their `accounted_bytes`):
    /// they are publisher-controlled and held as long as the payload.
    #[must_use]
    pub fn cost(&self, topic_len: usize, payload_len: usize) -> u32 {
        let c = topic_len
            .saturating_add(payload_len)
            .saturating_add(COMMAND_OVERHEAD)
            .min(self.conn_bytes);
        u32::try_from(c).unwrap_or(u32::MAX)
    }
}

/// One connection's credit: its cap, and the node pool behind it.
#[derive(Debug, Clone)]
pub struct ConnCredit {
    node: Arc<IngressCredit>,
    conn: Arc<Semaphore>,
}

impl ConnCredit {
    /// The node's credit (mode, cost).
    #[must_use]
    pub fn node(&self) -> &IngressCredit {
        &self.node
    }

    /// Take `cost` bytes now, from the connection's cap and then the pool, or nothing.
    #[must_use]
    pub fn try_acquire(&self, cost: u32) -> Option<IngressPermit> {
        let conn = self.conn.clone().try_acquire_many_owned(cost).ok()?;
        let pool = self.node.pool.clone().try_acquire_many_owned(cost).ok()?;
        Some(IngressPermit {
            _conn: Some(conn),
            _pool: pool,
        })
    }

    /// Wait for `cost` bytes, from the connection's cap and then the pool. Both
    /// semaphores are FIFO, so waiting connections are served in arrival order. Never
    /// fails: neither semaphore is ever closed.
    ///
    /// # Panics
    ///
    /// Only if a semaphore were closed, which nothing does.
    pub async fn acquire(self, cost: u32) -> IngressPermit {
        let conn = self
            .conn
            .acquire_many_owned(cost)
            .await
            .expect("the per-connection credit is never closed");
        let pool = self
            .node
            .pool
            .clone()
            .acquire_many_owned(cost)
            .await
            .expect("the ingress pool is never closed");
        IngressPermit {
            _conn: Some(conn),
            _pool: pool,
        }
    }
}

/// Credit held by one queued client publish, or by a peer `QoS` 0 publish (pool only,
/// no connection cap). Dropping it — when the hub has dispatched
/// the command, or on any path that discards it — returns the bytes to the connection
/// and the pool.
#[derive(Debug)]
pub struct IngressPermit {
    _conn: Option<OwnedSemaphorePermit>,
    _pool: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pool_follows_the_config_then_the_watermark_then_the_default() {
        assert_eq!(
            IngressCredit::pool_from_config(Some(65_536), Some(8 << 30)),
            65_536
        );
        assert_eq!(
            IngressCredit::pool_from_config(None, Some(1_500_000_000)),
            187_500_000
        );
        assert_eq!(
            IngressCredit::pool_from_config(None, None),
            DEFAULT_POOL_BYTES
        );
    }

    #[test]
    fn credit_is_returned_when_the_permit_drops_and_the_cap_bounds_one_connection() {
        let unit = 200 + COMMAND_OVERHEAD; // one 4-byte-topic, 196-byte-payload publish
        let node = Arc::new(IngressCredit::new(10 * unit, 4 * unit, OverloadMode::Pause));
        let a = node.connection();
        let b = node.connection();
        let cost = node.cost(4, 196);
        assert_eq!(cost as usize, unit);
        let held: Vec<_> = (0..4).map(|_| a.try_acquire(cost).unwrap()).collect();
        assert!(a.try_acquire(cost).is_none(), "a's cap is four publishes");
        assert_eq!(node.in_use(), 4 * unit);
        let b_held: Vec<_> = (0..4).map(|_| b.try_acquire(cost).unwrap()).collect();
        assert_eq!(node.in_use(), 8 * unit);
        drop(held);
        assert_eq!(
            node.in_use(),
            4 * unit,
            "dropping the permits returns the bytes"
        );
        assert!(a.try_acquire(cost).is_some());
        drop(b_held);
        // A message larger than the cap is clamped to it, so it can still proceed.
        assert_eq!(node.cost(10, 1 << 20) as usize, 4 * unit);
    }

    #[test]
    fn peer_credit_draws_on_the_pool_alone_and_counts_what_it_sheds() {
        let node = Arc::new(IngressCredit::new(3_000, 1_000, OverloadMode::Pause));
        // No per-link cap: three permits at the connection cap fill the whole pool.
        let held: Vec<_> = (0..3)
            .map(|_| node.try_acquire_pool(1_000).unwrap())
            .collect();
        assert_eq!(node.in_use(), 3_000);
        assert!(node.try_acquire_pool(1).is_none(), "the pool is full");
        node.note_peer_shed();
        node.note_peer_shed();
        assert_eq!(node.take_peer_shed(), 2);
        assert_eq!(node.take_peer_shed(), 0, "taking the count resets it");
        drop(held);
        assert_eq!(node.in_use(), 0);
    }
}
