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
//!   The [`IngressPermit`] travels **inside** `HubCommand::Publish`. A publish whose
//!   publisher waits for an answer keeps it on its pending entry until that answer
//!   (ADR 0093 Amendment 2), so a durable path that falls behind parks the publishers
//!   here; any other is released when the hub drops the command after dispatching it.
//!   The hub never acquires credit, and nothing an answer waits for (an append, a
//!   subscriber's PUBACK, a shard's or a peer's answer) does either, so no wait here
//!   can depend on its own release: there is no credit cycle.
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

/// The pool when neither it nor a memory watermark is configured (ADR 0082 §5), before
/// [`MAX_SHARD_POOL_BYTES`] applies.
pub const DEFAULT_POOL_BYTES: usize = 256 * 1024 * 1024;

/// The most one hub shard's pool gets when the pool is not configured (ADR 0093
/// Amendment 2): as many publishes as the shard's pending table holds before it is past
/// its count floor, at the least a publish can cost. A publish keeps its credit until
/// its publisher is answered, so a full pool parks the shard's publishers at their
/// sockets before that table holds more than its floor of them, and the stall budget,
/// which only ever evicts over the floor, never drops a client publish's ack. About
/// 56 MiB: 65,536 × [`COMMAND_OVERHEAD`].
pub const MAX_SHARD_POOL_BYTES: usize = crate::hub::PENDING_PUBLISH_CAP * COMMAND_OVERHEAD;

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
///
/// ADR 0093 D3: with `K` hub shards the pool is `K` pools of a `K`-th each, and a client
/// connection draws on its own shard's (`connection_for`). One shared pool goes to
/// whichever shard falls behind: its queued publishes hold the credit, the other shard's
/// publishers find none, and the other shard starves — measured on 2026-11-02, a lagging
/// shard's queue holding the whole 256 MiB while its partner idled and the pair took 72,000
/// of 105,500 msg/s. Per shard, a shard behind parks only its own publishers.
#[derive(Debug)]
pub struct IngressCredit {
    pools: Vec<Arc<Semaphore>>,
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
        Self::sharded(pool_bytes, conn_bytes, mode, 1)
    }

    /// As [`new`](Self::new), split into `shards` pools of `pool_bytes / shards` each
    /// (ADR 0093 D3); a client connection draws on its own shard's. The per-connection
    /// cap is clamped to one shard's pool.
    #[must_use]
    pub fn sharded(
        pool_bytes: usize,
        conn_bytes: usize,
        mode: OverloadMode,
        shards: usize,
    ) -> Self {
        let shards = shards.max(1);
        let pool_bytes = pool_bytes.clamp(shards, Semaphore::MAX_PERMITS);
        let each = (pool_bytes / shards).max(1);
        Self {
            pools: (0..shards)
                .map(|_| Arc::new(Semaphore::new(each)))
                .collect(),
            pool_bytes: each * shards,
            conn_bytes: conn_bytes.clamp(1, each),
            mode,
            peer_shed: AtomicU64::new(0),
        }
    }

    /// How many shard pools the credit is split into.
    #[must_use]
    pub fn shards(&self) -> usize {
        self.pools.len()
    }

    /// The pool size from config for `shards` hub shards: explicit, as given; else 1/8 of
    /// the memory watermark, else [`DEFAULT_POOL_BYTES`], either at most
    /// [`MAX_SHARD_POOL_BYTES`] a shard (ADR 0093 Amendment 2).
    #[must_use]
    pub fn pool_from_config(
        explicit: Option<u64>,
        memory_max: Option<u64>,
        shards: usize,
    ) -> usize {
        let bytes = explicit.unwrap_or_else(|| {
            let derived = memory_max.map_or(DEFAULT_POOL_BYTES as u64, |m| m / 8);
            let cap = (MAX_SHARD_POOL_BYTES as u64)
                .saturating_mul(u64::try_from(shards.max(1)).unwrap_or(u64::MAX));
            derived.min(cap)
        });
        usize::try_from(bytes.max(4096)).unwrap_or(usize::MAX)
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

    /// Bytes currently held by queued publishes, every shard's pool together.
    #[must_use]
    pub fn in_use(&self) -> usize {
        let available: usize = self.pools.iter().map(|p| p.available_permits()).sum();
        self.pool_bytes.saturating_sub(available)
    }

    /// A connection's view: its own cap over the first shard's pool (the one pool with
    /// one shard).
    #[must_use]
    pub fn connection(self: &Arc<Self>) -> ConnCredit {
        self.connection_on(0)
    }

    /// A client connection's view (ADR 0093 D3): its own cap over the pool of the shard
    /// that owns `client`.
    #[must_use]
    pub fn connection_for(self: &Arc<Self>, client: &str) -> ConnCredit {
        self.connection_on(crate::hub::shard_of(client, self.pools.len()))
    }

    fn connection_on(self: &Arc<Self>, shard: usize) -> ConnCredit {
        ConnCredit {
            node: self.clone(),
            pool: self.pools[shard.min(self.pools.len() - 1)].clone(),
            conn: Arc::new(Semaphore::new(self.conn_bytes)),
        }
    }

    /// Take `cost` bytes from the pool alone, now, or nothing (ADR 0082 T4). Peer links
    /// have no per-link cap: one link carries many publishers' traffic. With shards, the
    /// first shard's pool: peer traffic enters there (ADR 0093 D5).
    #[must_use]
    pub fn try_acquire_pool(&self, cost: u32) -> Option<IngressPermit> {
        let pool = self.pools[0].clone().try_acquire_many_owned(cost).ok()?;
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

    /// What a publish's derived messages (ADR 0083) cost on top of its own `charged`
    /// bytes: each costs what a publish of its size would, and together they are
    /// clamped to what the per-connection cap leaves beside the original — so the
    /// batch's whole charge fits under the cap and, waited for in one acquire with
    /// nothing else held, can always eventually be had, as the largest single message
    /// can. `derived` yields each message's topic length and payload-plus-properties
    /// length.
    #[must_use]
    pub fn derived_cost(&self, charged: u32, derived: impl Iterator<Item = (usize, usize)>) -> u32 {
        let room = self.conn_bytes.saturating_sub(charged as usize);
        let total = derived
            .map(|(t, p)| self.cost(t, p) as usize)
            .fold(0usize, usize::saturating_add)
            .min(room);
        u32::try_from(total).unwrap_or(u32::MAX)
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

/// One connection's credit: its cap, and the pool behind it (its shard's).
#[derive(Debug, Clone)]
pub struct ConnCredit {
    node: Arc<IngressCredit>,
    pool: Arc<Semaphore>,
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
        let pool = self.pool.clone().try_acquire_many_owned(cost).ok()?;
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
            IngressCredit::pool_from_config(Some(65_536), Some(8 << 30), 1),
            65_536
        );
        assert_eq!(
            IngressCredit::pool_from_config(None, Some(200_000_000), 4),
            25_000_000
        );
        assert_eq!(
            IngressCredit::pool_from_config(None, Some(1_500_000_000), 4),
            187_500_000
        );
        assert_eq!(
            IngressCredit::pool_from_config(None, None, 8),
            DEFAULT_POOL_BYTES
        );
    }

    /// ADR 0093 Amendment 2: a pool nobody sized is at most `MAX_SHARD_POOL_BYTES` a
    /// shard, so a full pool of the cheapest publishes is the pending table's count
    /// floor and no more; a configured pool is used as given.
    #[test]
    fn an_unconfigured_pool_is_at_most_the_pending_floor_a_shard() {
        let floor = crate::hub::PENDING_PUBLISH_CAP;
        assert_eq!(MAX_SHARD_POOL_BYTES / COMMAND_OVERHEAD, floor);
        for shards in [1, 2, 4] {
            let pool = IngressCredit::pool_from_config(None, None, shards);
            assert_eq!(pool, MAX_SHARD_POOL_BYTES * shards, "{shards} shard(s)");
            assert!(pool <= DEFAULT_POOL_BYTES);
            let watermark = IngressCredit::pool_from_config(None, Some(64 << 30), shards);
            assert_eq!(
                watermark,
                MAX_SHARD_POOL_BYTES * shards,
                "{shards} shard(s)"
            );
            let credit =
                IngressCredit::sharded(pool, DEFAULT_CONN_BYTES, OverloadMode::Pause, shards);
            // The cheapest publish costs the overhead and a one-byte topic: one shard's
            // pool holds fewer of them than the floor.
            let cheapest = credit.cost(1, 0) as usize;
            assert!(
                credit.pool_bytes() / shards / cheapest < floor,
                "{shards} shard(s)"
            );
        }
        assert_eq!(
            IngressCredit::pool_from_config(Some(1 << 30), None, 1),
            1 << 30,
            "a configured pool is the operator's"
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

    /// ADR 0093 D3: with two shards, each has its own half of the pool, so one shard's
    /// publishers holding all of theirs leave the other shard's untouched.
    #[test]
    fn a_shard_that_holds_its_whole_pool_leaves_the_other_shards_pool_untouched() {
        let node = Arc::new(IngressCredit::sharded(4_000, 1_000, OverloadMode::Pause, 2));
        assert_eq!(node.shards(), 2);
        assert_eq!(node.pool_bytes(), 4_000);
        let on = |shard: usize, n: usize| {
            (0..100_000)
                .map(|i| format!("c{i}"))
                .filter(|id| crate::hub::shard_of(id, 2) == shard)
                .nth(n)
                .expect("the hash spreads ids over both shards")
        };
        // Shard 0's two connections take its whole 2,000.
        let a = node.connection_for(&on(0, 0));
        let b = node.connection_for(&on(0, 1));
        let held = [a.try_acquire(1_000).unwrap(), b.try_acquire(1_000).unwrap()];
        assert_eq!(node.in_use(), 2_000);
        assert!(
            node.connection_for(&on(0, 2)).try_acquire(1).is_none(),
            "shard 0's pool is full"
        );
        let c = node.connection_for(&on(1, 0));
        assert!(
            c.try_acquire(1_000).is_some(),
            "shard 1's publishers draw on their own pool"
        );
        drop(held);
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
