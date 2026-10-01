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
//! Only client publishes are charged. Acks, subscriptions, pings and every control
//! command are not, and neither is anything the hub sends itself.

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// What one queued publish costs beyond its topic and payload (ADR 0082 T1): the
/// command, its channel slot and the allocations around it. The #504 cloud re-run
/// measured about 1,019 bytes per queued command at 200-byte payloads (2,553,413
/// commands at 2.60 GB RSS), so about 800 beyond the payload.
pub const COMMAND_OVERHEAD: usize = 800;

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

    /// What a publish of `topic_len` and `payload_len` bytes costs, clamped to the
    /// per-connection cap so even the largest message can eventually proceed.
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
            _conn: conn,
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
            _conn: conn,
            _pool: pool,
        }
    }
}

/// Credit held by one queued client publish. Dropping it — when the hub has dispatched
/// the command, or on any path that discards it — returns the bytes to the connection
/// and the pool.
#[derive(Debug)]
pub struct IngressPermit {
    _conn: OwnedSemaphorePermit,
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
        let node = Arc::new(IngressCredit::new(10_000, 4_000, OverloadMode::Pause));
        let a = node.connection();
        let b = node.connection();
        let cost = node.cost(4, 196); // 1,000 bytes
        assert_eq!(cost, 1_000);
        let held: Vec<_> = (0..4).map(|_| a.try_acquire(cost).unwrap()).collect();
        assert!(a.try_acquire(cost).is_none(), "a's cap is 4,000");
        assert_eq!(node.in_use(), 4_000);
        let b_held: Vec<_> = (0..4).map(|_| b.try_acquire(cost).unwrap()).collect();
        assert_eq!(node.in_use(), 8_000);
        drop(held);
        assert_eq!(
            node.in_use(),
            4_000,
            "dropping the permits returns the bytes"
        );
        assert!(a.try_acquire(cost).is_some());
        drop(b_held);
        // A message larger than the cap is clamped to it, so it can still proceed.
        assert_eq!(node.cost(10, 1 << 20), 4_000);
    }
}
