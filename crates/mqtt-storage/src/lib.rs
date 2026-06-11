//! Pluggable persistence boundaries for broker state.
//!
//! These traits are the seam that lets the broker run with an in-memory backend
//! on a single node, an embedded durable store, or a replicated cluster backend —
//! without the core knowing which. Keeping them narrow is what preserves
//! **linear scalability**: nothing here implies a global lock or coordinator.
//!
//! The [`SessionStore`] interface is **incremental** (`enqueue` / `pending` /
//! `ack`) rather than load-whole / save-whole. This matches the clustered design
//! in [ADR 0001](../../../docs/adr/0001-session-durability.md): the durable,
//! quorum-replicated write is the per-message `enqueue`; acknowledgements truncate
//! the queue lazily and may replicate asynchronously. Building the single-node
//! store against this same shape means the broker needs no second refactor when
//! the clustered backend lands.

use async_trait::async_trait;
use mqtt_core::{topic_matches, ClientId, Message, Subscription};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// A monotonically increasing position within a single session's queue log.
///
/// Offsets are per-session. In a clustered backend the offset is assigned by the
/// session's replicated log; here it is a simple counter.
pub type Offset = u64;

/// Errors from a storage backend.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The requested session does not exist.
    #[error("not found")]
    NotFound,
    /// A backend-specific failure (I/O, replication quorum not reached, ...).
    #[error("storage backend error: {0}")]
    Backend(String),
}

/// A queued message together with the offset it was assigned on `enqueue`.
#[derive(Debug, Clone)]
pub struct QueuedMessage {
    /// The log offset assigned to this message.
    pub offset: Offset,
    /// The message payload and metadata.
    pub message: Message,
}

/// Durable storage for MQTT persistent sessions.
///
/// Implementations must be safe to shard by [`ClientId`]. The durability contract
/// is: once [`enqueue`](SessionStore::enqueue) returns `Ok`, the message survives
/// the failure of a single node (in a replicated backend), and a producer's
/// QoS≥1 PUBACK may be released. See [ADR 0001].
///
/// [ADR 0001]: ../../../docs/adr/0001-session-durability.md
#[async_trait]
pub trait SessionStore: Send + Sync + std::fmt::Debug {
    /// Ensure a persistent session record exists for `client`.
    ///
    /// Returns `true` if a session already existed (used to set the CONNACK
    /// `session_present` flag), `false` if a fresh one was created.
    async fn ensure_session(&self, client: &ClientId) -> Result<bool, StorageError>;

    /// Replace the stored subscription set for a client.
    async fn set_subscriptions(
        &self,
        client: &ClientId,
        subscriptions: &[Subscription],
    ) -> Result<(), StorageError>;

    /// Load a client's stored subscriptions (empty if none / no session).
    async fn subscriptions(&self, client: &ClientId) -> Result<Vec<Subscription>, StorageError>;

    /// Append a message to the client's offline queue, returning its offset.
    ///
    /// This is the **durability-critical** write. A clustered backend
    /// quorum-replicates before returning; the producer's QoS≥1 PUBACK should be
    /// gated on it.
    async fn enqueue(&self, client: &ClientId, message: &Message) -> Result<Offset, StorageError>;

    /// Replay undelivered messages with offset strictly greater than `after`, up
    /// to `limit` items, in offset order. Used on reconnect / takeover.
    ///
    /// Pass `after = 0` to start from the beginning of the retained log.
    async fn pending(
        &self,
        client: &ClientId,
        after: Offset,
        limit: usize,
    ) -> Result<Vec<QueuedMessage>, StorageError>;

    /// Acknowledge delivery up to and including `up_to`, permitting the log to be
    /// truncated. Local-first; may replicate asynchronously. Redelivery of
    /// not-yet-truncated messages after a failover is spec-legal for `QoS` 1.
    async fn ack(&self, client: &ClientId, up_to: Offset) -> Result<(), StorageError>;

    /// Remove the session and its queue entirely (clean start / session expiry).
    async fn remove(&self, client: &ClientId) -> Result<(), StorageError>;
}

/// Stores retained messages keyed by topic.
#[async_trait]
pub trait RetainedStore: Send + Sync + std::fmt::Debug {
    /// Set the retained message for a topic, or clear it if the payload is empty
    /// (per MQTT semantics, a zero-length retained PUBLISH deletes the retained
    /// message for that topic).
    async fn set(&self, message: &Message) -> Result<(), StorageError>;

    /// Return all retained messages whose topic matches the given filter.
    async fn matching(&self, filter: &str) -> Result<Vec<Message>, StorageError>;
}

// ---------------------------------------------------------------------------
// In-memory single-node implementations.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct SessionEntry {
    subscriptions: Vec<Subscription>,
    queue: VecDeque<QueuedMessage>,
    next_offset: Offset,
}

/// A non-durable, single-process [`SessionStore`] backed by in-memory maps.
///
/// Suitable for single-node operation and tests. It satisfies the interface the
/// clustered backend will implement, but offers no cross-node durability: all
/// state is lost when the process exits.
#[derive(Debug, Default)]
pub struct MemorySessionStore {
    sessions: Mutex<HashMap<ClientId, SessionEntry>>,
}

impl MemorySessionStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ClientId, SessionEntry>> {
        // A poisoned lock means another thread panicked while holding it; recover
        // the guard rather than propagating the panic, since session state is not
        // left in a torn state by our short critical sections.
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl SessionStore for MemorySessionStore {
    async fn ensure_session(&self, client: &ClientId) -> Result<bool, StorageError> {
        let mut map = self.lock();
        let existed = map.contains_key(client);
        map.entry(client.clone()).or_default();
        Ok(existed)
    }

    async fn set_subscriptions(
        &self,
        client: &ClientId,
        subscriptions: &[Subscription],
    ) -> Result<(), StorageError> {
        let mut map = self.lock();
        map.entry(client.clone()).or_default().subscriptions = subscriptions.to_vec();
        Ok(())
    }

    async fn subscriptions(&self, client: &ClientId) -> Result<Vec<Subscription>, StorageError> {
        Ok(self
            .lock()
            .get(client)
            .map(|e| e.subscriptions.clone())
            .unwrap_or_default())
    }

    async fn enqueue(&self, client: &ClientId, message: &Message) -> Result<Offset, StorageError> {
        let mut map = self.lock();
        let entry = map.entry(client.clone()).or_default();
        // Offsets are 1-based so that `0` is a valid "nothing yet" sentinel for
        // both `after` (pending) and `up_to` (ack).
        entry.next_offset += 1;
        let offset = entry.next_offset;
        entry.queue.push_back(QueuedMessage {
            offset,
            message: message.clone(),
        });
        Ok(offset)
    }

    async fn pending(
        &self,
        client: &ClientId,
        after: Offset,
        limit: usize,
    ) -> Result<Vec<QueuedMessage>, StorageError> {
        Ok(self
            .lock()
            .get(client)
            .map(|e| {
                e.queue
                    .iter()
                    .filter(|m| m.offset > after)
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn ack(&self, client: &ClientId, up_to: Offset) -> Result<(), StorageError> {
        let mut map = self.lock();
        let entry = map.get_mut(client).ok_or(StorageError::NotFound)?;
        while entry.queue.front().is_some_and(|m| m.offset <= up_to) {
            entry.queue.pop_front();
        }
        Ok(())
    }

    async fn remove(&self, client: &ClientId) -> Result<(), StorageError> {
        self.lock().remove(client);
        Ok(())
    }
}

/// A non-durable, single-process [`RetainedStore`].
#[derive(Debug, Default)]
pub struct MemoryRetainedStore {
    by_topic: Mutex<HashMap<String, Message>>,
}

impl MemoryRetainedStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RetainedStore for MemoryRetainedStore {
    async fn set(&self, message: &Message) -> Result<(), StorageError> {
        let mut map = self
            .by_topic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if message.payload.is_empty() {
            map.remove(&message.topic);
        } else {
            map.insert(message.topic.clone(), message.clone());
        }
        Ok(())
    }

    async fn matching(&self, filter: &str) -> Result<Vec<Message>, StorageError> {
        let map = self
            .by_topic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(map
            .values()
            .filter(|m| topic_matches(filter, &m.topic))
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryRetainedStore, MemorySessionStore, RetainedStore, SessionStore};
    use mqtt_core::{ClientId, Message, QoS};

    fn cid(s: &str) -> ClientId {
        ClientId(s.to_string())
    }

    fn msg(topic: &str, payload: &'static [u8]) -> Message {
        Message {
            topic: topic.to_string(),
            payload: bytes::Bytes::from_static(payload),
            qos: QoS::AtLeastOnce,
            retain: false,
        }
    }

    #[tokio::test]
    async fn enqueue_assigns_monotonic_offsets_and_replays() {
        let store = MemorySessionStore::new();
        let c = cid("client");
        assert!(!store.ensure_session(&c).await.unwrap(), "fresh session");
        assert!(store.ensure_session(&c).await.unwrap(), "now it exists");

        let o0 = store.enqueue(&c, &msg("a", b"0")).await.unwrap();
        let o1 = store.enqueue(&c, &msg("a", b"1")).await.unwrap();
        let o2 = store.enqueue(&c, &msg("a", b"2")).await.unwrap();
        assert_eq!((o0, o1, o2), (1, 2, 3));

        let all = store.pending(&c, 0, 100).await.unwrap();
        assert_eq!(all.len(), 3);

        // Replay only what's after o0.
        let after_first = store.pending(&c, o0, 100).await.unwrap();
        assert_eq!(after_first.len(), 2);
        assert_eq!(after_first[0].offset, 2);

        // Limit is honored.
        assert_eq!(store.pending(&c, 0, 2).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn ack_truncates_the_log() {
        let store = MemorySessionStore::new();
        let c = cid("client");
        store.ensure_session(&c).await.unwrap();
        for i in 0..5u8 {
            store
                .enqueue(&c, &msg("a", b"x"))
                .await
                .map(|o| assert_eq!(u64::from(i) + 1, o)) // 1-based: 1..=5
                .unwrap();
        }
        store.ack(&c, 2).await.unwrap(); // ack offsets 1,2
        let remaining = store.pending(&c, 0, 100).await.unwrap();
        assert_eq!(remaining.len(), 3); // 3,4,5 remain
        assert_eq!(remaining[0].offset, 3);
    }

    #[tokio::test]
    async fn remove_clears_session() {
        let store = MemorySessionStore::new();
        let c = cid("client");
        store.enqueue(&c, &msg("a", b"x")).await.unwrap();
        store.remove(&c).await.unwrap();
        assert!(!store.ensure_session(&c).await.unwrap(), "session gone");
        assert!(store.pending(&c, 0, 100).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn retained_set_get_and_clear() {
        let store = MemoryRetainedStore::new();
        store.set(&msg("sensors/a/temp", b"21")).await.unwrap();
        store.set(&msg("sensors/b/temp", b"22")).await.unwrap();

        assert_eq!(store.matching("sensors/+/temp").await.unwrap().len(), 2);
        assert_eq!(store.matching("sensors/a/temp").await.unwrap().len(), 1);

        // Empty payload clears the retained message.
        store.set(&msg("sensors/a/temp", b"")).await.unwrap();
        assert_eq!(store.matching("sensors/+/temp").await.unwrap().len(), 1);
    }

    /// A persistent session that is being torn down behaves as a clustered
    /// failover would expect: unacked messages remain replayable.
    #[tokio::test]
    async fn unacked_messages_survive_for_replay() {
        let store = MemorySessionStore::new();
        let c = cid("client");
        store.ensure_session(&c).await.unwrap();
        let o = store.enqueue(&c, &msg("a", b"important")).await.unwrap();
        // Simulate delivery attempt without ack (client dropped before PUBACK).
        let replay = store.pending(&c, 0, 100).await.unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].offset, o);
        // Still there until acked.
        assert_eq!(store.pending(&c, 0, 100).await.unwrap().len(), 1);
    }
}
