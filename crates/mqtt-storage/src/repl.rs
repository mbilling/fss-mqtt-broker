//! The `ReplicatedLog` seam: a generic, keyed, offset-addressed append-log.
//!
//! This is the abstraction [ADR 0006](../../../docs/adr/0006-consensus-and-replication.md)
//! decides on — the boundary that insulates the broker's MQTT session/queue
//! semantics from *how* the log is replicated. A [`SessionStore`](crate::SessionStore)
//! backend (workstream E) is built **on top of** a `ReplicatedLog`; it never sees
//! leader election, epochs, or quorum.
//!
//! Three backends are planned behind this trait:
//! - [`InMemoryReplicatedLog`] — single-node, always-owner; ships **now** for
//!   development, tests, and non-clustered deployments.
//! - the consensus-backed cluster log — workstream E's production target: an
//!   ownership lease plus epoch-fenced quorum-append over the replica set.
//! - an external-store adapter — the operator option for shops already running a
//!   suitable store (ADR 0001).
//!
//! ## The contract a clustered backend must honor
//!
//! These are the guarantees ADR 0006 §4 specifies; the in-memory backend trivially
//! satisfies the single-node projection of each, and the cluster backend must
//! uphold the full distributed form:
//!
//! - [`append`](ReplicatedLog::append) returns only once the record is durable. In
//!   the cluster backend that means *epoch-fenced and quorum-durable* across the
//!   replica set — this is what gates a producer's QoS≥1 PUBACK. A non-owner, or a
//!   lease-holder fenced at a superseded epoch, returns [`ReplError::NotOwner`] /
//!   [`ReplError::NoQuorum`] rather than diverging the log.
//! - [`read`](ReplicatedLog::read) returns entries with offset strictly greater
//!   than `after`, in offset order — the reconnect / takeover replay path.
//! - [`truncate`](ReplicatedLog::truncate) is local-first and lazy; it needs no
//!   synchronous cross-node round-trip (an over-eager truncate only costs a
//!   spec-legal QoS-1 redelivery, never a lost quorum-durable message).
//! - [`remove`](ReplicatedLog::remove) drops a key's log entirely (clean start /
//!   session expiry).
//!
//! Records are opaque `Vec<u8>`: the log replicates bytes and assigns offsets;
//! encoding queued messages, the QoS-2 dedup set, and the packet-id counter into
//! those bytes is the `SessionStore` backend's job, not the log's.

use crate::Offset;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

/// Errors from a [`ReplicatedLog`].
///
/// `NotOwner` and `NoQuorum` are the two distributed-failure shapes the cluster
/// backend surfaces; the in-memory backend never produces them (it is always the
/// sole owner and trivially "replicates" to itself), but callers must handle them
/// so the same call sites work against either backend.
#[derive(Debug, thiserror::Error)]
pub enum ReplError {
    /// This node does not hold the ownership lease for the key (it was never the
    /// owner, or its lease was superseded by a newer epoch after a partition).
    /// The caller must not treat the append as durable.
    #[error("not the lease owner for this key")]
    NotOwner,
    /// The append could not be made durable across a quorum of the replica set
    /// (replicas unreachable, or the writer was fenced at a stale epoch). The
    /// producer's QoS≥1 PUBACK must **not** be released.
    #[error("replication quorum not reached")]
    NoQuorum,
    /// A backend-specific failure (I/O, engine error, serialization, ...).
    #[error("replicated-log backend error: {0}")]
    Backend(String),
}

/// One record in a key's log, together with the offset the log assigned it on
/// [`append`](ReplicatedLog::append).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The monotonically increasing position assigned within this key's log.
    pub offset: Offset,
    /// The opaque record bytes.
    pub record: Vec<u8>,
}

/// A generic async append-log, replicated and keyed.
///
/// `Key` is the partition the log shards on — for the session backend it is the
/// client id. Each key has an independent offset space starting at 1 (so `0` is a
/// valid "nothing yet" sentinel for `after` / `up_to`).
#[async_trait]
pub trait ReplicatedLog: Send + Sync + std::fmt::Debug {
    /// The key each independent log is addressed by.
    type Key: Send + Sync;

    /// Append `record` to `key`'s log and return the assigned offset.
    ///
    /// This is the **durability-critical** write. A clustered backend returns only
    /// once the record is epoch-fenced and quorum-durable; until then the caller
    /// must not release a QoS≥1 PUBACK. A non-owner returns [`ReplError::NotOwner`];
    /// a writer that cannot reach quorum returns [`ReplError::NoQuorum`].
    async fn append(&self, key: &Self::Key, record: Vec<u8>) -> Result<Offset, ReplError>;

    /// Read entries with offset strictly greater than `after`, up to `limit`, in
    /// offset order. `after = 0` starts from the beginning of the retained log.
    async fn read(
        &self,
        key: &Self::Key,
        after: Offset,
        limit: usize,
    ) -> Result<Vec<LogEntry>, ReplError>;

    /// Truncate `key`'s log up to and including `up_to`. Local-first and lazy;
    /// idempotent and tolerant of stale / already-truncated offsets.
    async fn truncate(&self, key: &Self::Key, up_to: Offset) -> Result<(), ReplError>;

    /// Remove `key`'s log entirely.
    async fn remove(&self, key: &Self::Key) -> Result<(), ReplError>;
}

// ---------------------------------------------------------------------------
// In-memory, single-node backend.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct LogState {
    entries: std::collections::VecDeque<LogEntry>,
    next_offset: Offset,
}

/// A non-durable, single-process [`ReplicatedLog`] keyed by `String`.
///
/// It is **always the owner** and "replicates" only to itself: `append` always
/// succeeds (never `NotOwner` / `NoQuorum`), assigns a per-key monotonic offset,
/// and the contract above collapses to its single-node projection. This is the
/// development/test/non-clustered backend ADR 0006 §3 ships now; it proves the
/// `SessionStore`-over-`ReplicatedLog` layering (workstream E step 2) before any
/// network code exists. All state is lost when the process exits.
#[derive(Debug, Default)]
pub struct InMemoryReplicatedLog {
    logs: Mutex<HashMap<String, LogState>>,
}

impl InMemoryReplicatedLog {
    /// Create an empty in-memory log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, LogState>> {
        // Short, non-tearing critical sections: recover from a poisoned lock
        // rather than cascading a panic (as MemorySessionStore does).
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl ReplicatedLog for InMemoryReplicatedLog {
    type Key = String;

    async fn append(&self, key: &String, record: Vec<u8>) -> Result<Offset, ReplError> {
        let mut map = self.lock();
        let state = map.entry(key.clone()).or_default();
        // 1-based offsets so `0` is a valid "nothing yet" sentinel.
        state.next_offset += 1;
        let offset = state.next_offset;
        state.entries.push_back(LogEntry { offset, record });
        Ok(offset)
    }

    async fn read(
        &self,
        key: &String,
        after: Offset,
        limit: usize,
    ) -> Result<Vec<LogEntry>, ReplError> {
        Ok(self
            .lock()
            .get(key)
            .map(|s| {
                s.entries
                    .iter()
                    .filter(|e| e.offset > after)
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn truncate(&self, key: &String, up_to: Offset) -> Result<(), ReplError> {
        let mut map = self.lock();
        if let Some(state) = map.get_mut(key) {
            while state.entries.front().is_some_and(|e| e.offset <= up_to) {
                state.entries.pop_front();
            }
        }
        Ok(())
    }

    async fn remove(&self, key: &String) -> Result<(), ReplError> {
        self.lock().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryReplicatedLog, LogEntry, Offset, ReplicatedLog};

    fn rec(b: &[u8]) -> Vec<u8> {
        b.to_vec()
    }

    /// Offsets are 1-based, per-key, and monotonic; `read(after)` replays the tail.
    #[tokio::test]
    async fn append_assigns_monotonic_offsets_per_key() {
        let log = InMemoryReplicatedLog::new();
        let a = "a".to_string();
        let b = "b".to_string();

        assert_eq!(log.append(&a, rec(b"0")).await.unwrap(), 1);
        assert_eq!(log.append(&a, rec(b"1")).await.unwrap(), 2);
        // A different key has its own independent offset space.
        assert_eq!(log.append(&b, rec(b"0")).await.unwrap(), 1);
        assert_eq!(log.append(&a, rec(b"2")).await.unwrap(), 3);

        let all = log.read(&a, 0, 100).await.unwrap();
        assert_eq!(
            all,
            vec![
                LogEntry {
                    offset: 1,
                    record: rec(b"0")
                },
                LogEntry {
                    offset: 2,
                    record: rec(b"1")
                },
                LogEntry {
                    offset: 3,
                    record: rec(b"2")
                },
            ]
        );
        // `b`'s log is untouched by `a`'s appends.
        assert_eq!(log.read(&b, 0, 100).await.unwrap().len(), 1);
    }

    /// `read` honors both the `after` cursor and the `limit`.
    #[tokio::test]
    async fn read_filters_by_after_and_limit() {
        let log = InMemoryReplicatedLog::new();
        let k = "k".to_string();
        for _ in 0..5 {
            log.append(&k, rec(b"x")).await.unwrap();
        }
        let after2 = log.read(&k, 2, 100).await.unwrap();
        assert_eq!(
            after2.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert_eq!(log.read(&k, 0, 2).await.unwrap().len(), 2);
        // Reading an unknown key is empty, not an error.
        assert!(log
            .read(&"missing".to_string(), 0, 100)
            .await
            .unwrap()
            .is_empty());
    }

    /// Walking page by page (the reconnect-replay pattern) visits every entry
    /// once, in offset order.
    #[tokio::test]
    async fn read_pages_through_the_log_in_order() {
        let log = InMemoryReplicatedLog::new();
        let k = "k".to_string();
        for _ in 0..5 {
            log.append(&k, rec(b"x")).await.unwrap();
        }
        let mut seen: Vec<Offset> = Vec::new();
        let mut after = 0;
        loop {
            let page = log.read(&k, after, 2).await.unwrap();
            if page.is_empty() {
                break;
            }
            after = page.last().unwrap().offset;
            seen.extend(page.into_iter().map(|e| e.offset));
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn truncate_removes_up_to_inclusive() {
        let log = InMemoryReplicatedLog::new();
        let k = "k".to_string();
        for _ in 0..5 {
            log.append(&k, rec(b"x")).await.unwrap();
        }
        log.truncate(&k, 2).await.unwrap();
        let remaining = log.read(&k, 0, 100).await.unwrap();
        assert_eq!(
            remaining.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }

    /// Truncation is idempotent and ignores stale / already-truncated offsets —
    /// failovers replay truncations, and an over-truncate would lose messages.
    #[tokio::test]
    async fn truncate_is_idempotent_and_ignores_stale_offsets() {
        let log = InMemoryReplicatedLog::new();
        let k = "k".to_string();
        for _ in 0..3 {
            log.append(&k, rec(b"x")).await.unwrap();
        }
        log.truncate(&k, 2).await.unwrap();
        log.truncate(&k, 2).await.unwrap(); // repeat
        log.truncate(&k, 1).await.unwrap(); // stale, already gone
        let remaining = log.read(&k, 0, 100).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].offset, 3);
        // Truncating an unknown key is a harmless no-op.
        log.truncate(&"missing".to_string(), 5).await.unwrap();
    }

    /// Offsets never go backwards across truncation — a fresh append after a
    /// truncate continues the sequence rather than reusing a freed offset.
    #[tokio::test]
    async fn offsets_stay_monotonic_across_truncation() {
        let log = InMemoryReplicatedLog::new();
        let k = "k".to_string();
        for _ in 0..3 {
            log.append(&k, rec(b"x")).await.unwrap();
        }
        log.truncate(&k, 3).await.unwrap(); // empty the log
        assert!(log.read(&k, 0, 100).await.unwrap().is_empty());
        // Next append is offset 4, not 1.
        assert_eq!(log.append(&k, rec(b"x")).await.unwrap(), 4);
    }

    #[tokio::test]
    async fn remove_drops_the_key() {
        let log = InMemoryReplicatedLog::new();
        let k = "k".to_string();
        log.append(&k, rec(b"x")).await.unwrap();
        log.remove(&k).await.unwrap();
        assert!(log.read(&k, 0, 100).await.unwrap().is_empty());
        // Re-appending after remove starts a fresh offset space.
        assert_eq!(log.append(&k, rec(b"y")).await.unwrap(), 1);
    }
}
