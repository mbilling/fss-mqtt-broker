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
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;

pub mod data_dir;
pub mod logged;
pub mod persistent_log;
pub mod persistent_retained;
pub mod repl;

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
    /// The backend cannot answer authoritatively *right now*, but the condition is
    /// **transient and self-healing**: the ownership lease is being reassigned, or a
    /// replication quorum is momentarily unreachable (ADR 0017). The caller must treat
    /// this as "not ready, retry" — never as "no session" — because a real, recoverable
    /// session may be on the other side of it. Distinct from [`Self::Backend`], which is
    /// a terminal failure.
    #[error("storage temporarily unavailable: {0}")]
    Unavailable(String),
    /// A backend-specific, **terminal** failure (I/O, serialization, engine error, ...).
    #[error("storage backend error: {0}")]
    Backend(String),
}

impl StorageError {
    /// Whether this error is the transient, retry-able [`Self::Unavailable`] condition
    /// (lease in flux / quorum momentarily unreachable) rather than a terminal failure.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

/// A queued message together with the offset it was assigned on `enqueue`.
#[derive(Debug, Clone)]
pub struct QueuedMessage {
    /// The log offset assigned to this message.
    pub offset: Offset,
    /// The message payload and metadata.
    pub message: Message,
    /// Absolute expiry deadline in Unix epoch seconds (MQTT 5.0 Message Expiry
    /// Interval), or `None` for no expiry. A message past its deadline is dropped on
    /// replay rather than delivered (ADR 0009 §3).
    pub expiry_at: Option<u64>,
}

/// What a session's offline queue does when it is full (ADR 0001 §6 — a
/// dead-but-persistent client must not grow a queue without bound).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverflowPolicy {
    /// Evict the oldest queued message(s) to make room for the new one
    /// (freshest-wins — the default; matches common broker behaviour and keeps
    /// the latest state available on reconnect).
    #[default]
    DropOldest,
    /// Keep the queue intact and drop the newly-arriving message.
    RejectNewest,
}

/// Per-session offline-queue bounds. A message-count cap (not a byte cap) is the
/// standard MQTT broker lever and the granularity a clustered backend shards on.
#[derive(Debug, Clone, Copy)]
pub struct QueueLimits {
    /// Maximum messages retained per session before `overflow` applies. Treated
    /// as at least 1.
    pub max_messages: usize,
    /// What happens to the message that would exceed `max_messages`.
    pub overflow: OverflowPolicy,
}

impl Default for QueueLimits {
    fn default() -> Self {
        // Bounded by default (anti-OOM) but generous enough not to surprise
        // legitimate large offline queues; operators tune it down.
        Self {
            max_messages: 100_000,
            overflow: OverflowPolicy::DropOldest,
        }
    }
}

/// The outcome of [`SessionStore::enqueue`]: whether the message was stored, and
/// what the queue cap cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enqueued {
    /// Stored at `offset`. `evicted` oldest messages were dropped to stay within
    /// the cap (0 unless the drop-oldest policy fired).
    Stored {
        /// The offset assigned to the stored message.
        offset: Offset,
        /// How many oldest messages were evicted to make room.
        evicted: u64,
    },
    /// The queue was full and the reject-newest policy dropped this message.
    Rejected,
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

    /// Append a message to the client's offline queue with no expiry. Convenience
    /// over [`enqueue_with_expiry`](Self::enqueue_with_expiry).
    async fn enqueue(
        &self,
        client: &ClientId,
        message: &Message,
    ) -> Result<Enqueued, StorageError> {
        self.enqueue_with_expiry(client, message, None).await
    }

    /// Append a message with an optional absolute expiry deadline (Unix epoch
    /// seconds — the MQTT 5.0 Message Expiry Interval applied to receipt time).
    ///
    /// This is the **durability-critical** write. A clustered backend
    /// quorum-replicates before returning; the producer's QoS≥1 PUBACK should be
    /// gated on it. The returned [`Enqueued`] reports whether the per-session
    /// queue cap evicted older messages or rejected this one (ADR 0001 §6). A
    /// message past its deadline is dropped on replay, not delivered (ADR 0009 §3).
    async fn enqueue_with_expiry(
        &self,
        client: &ClientId,
        message: &Message,
        expiry_at: Option<u64>,
    ) -> Result<Enqueued, StorageError>;

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

    /// Record receipt of an inbound QoS-2 PUBLISH with `packet_id` (the
    /// exactly-once dedup window). Returns `true` if newly recorded, `false` if
    /// `packet_id` was already present — a duplicate re-send the broker must not
    /// deliver again.
    ///
    /// This is **replicated session state**: surviving failover is what keeps
    /// exactly-once holding across an owner change (ADR 0001 §5, ADR 0006 §4).
    async fn record_received(
        &self,
        client: &ClientId,
        packet_id: u16,
    ) -> Result<bool, StorageError>;

    /// Clear `packet_id` from the QoS-2 dedup window once its PUBREL completes.
    async fn clear_received(&self, client: &ClientId, packet_id: u16) -> Result<(), StorageError>;

    /// The QoS-2 packet ids currently received-but-not-completed, ascending — for
    /// resume / takeover reconciliation.
    async fn received(&self, client: &ClientId) -> Result<Vec<u16>, StorageError>;

    /// Allocate the next outbound packet id (`1..=65535`, wrapping, never `0`),
    /// persisting the advance so ids do not collide with still-in-flight ones after
    /// a failover.
    async fn next_packet_id(&self, client: &ClientId) -> Result<u16, StorageError>;

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

    /// Return every retained message (including `$`-rooted topics), for the
    /// cross-node retained snapshot a peer sends on link-up (ADR 0014 §3).
    async fn all(&self) -> Result<Vec<Message>, StorageError>;
}

// ---------------------------------------------------------------------------
// In-memory single-node implementations.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct SessionEntry {
    subscriptions: Vec<Subscription>,
    queue: VecDeque<QueuedMessage>,
    next_offset: Offset,
    /// QoS-2 inbound packet ids received but not yet PUBREL-completed (dedup).
    received_qos2: BTreeSet<u16>,
    /// Last outbound packet id allocated (0 = none yet).
    last_packet_id: u16,
}

/// A non-durable, single-process [`SessionStore`] backed by in-memory maps.
///
/// Suitable for single-node operation and tests. It satisfies the interface the
/// clustered backend will implement, but offers no cross-node durability: all
/// state is lost when the process exits.
#[derive(Debug, Default)]
pub struct MemorySessionStore {
    sessions: Mutex<HashMap<ClientId, SessionEntry>>,
    limits: QueueLimits,
}

impl MemorySessionStore {
    /// Create an empty store with default (bounded) queue limits.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an empty store with explicit per-session queue limits.
    #[must_use]
    pub fn with_limits(limits: QueueLimits) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            limits,
        }
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

    async fn enqueue_with_expiry(
        &self,
        client: &ClientId,
        message: &Message,
        expiry_at: Option<u64>,
    ) -> Result<Enqueued, StorageError> {
        let cap = self.limits.max_messages.max(1);
        let mut map = self.lock();
        let entry = map.entry(client.clone()).or_default();

        // Reject-newest: a full queue drops the arriving message, preserving the
        // offsets and order already present.
        if entry.queue.len() >= cap && self.limits.overflow == OverflowPolicy::RejectNewest {
            return Ok(Enqueued::Rejected);
        }
        // Drop-oldest: evict from the front until there is room for one more.
        // Offsets stay monotonic (next_offset never goes backwards), so `pending`
        // and `ack` remain correct across eviction.
        let mut evicted = 0u64;
        while entry.queue.len() >= cap {
            if entry.queue.pop_front().is_none() {
                break;
            }
            evicted += 1;
        }

        // Offsets are 1-based so that `0` is a valid "nothing yet" sentinel for
        // both `after` (pending) and `up_to` (ack).
        entry.next_offset += 1;
        let offset = entry.next_offset;
        entry.queue.push_back(QueuedMessage {
            offset,
            message: message.clone(),
            expiry_at,
        });
        Ok(Enqueued::Stored { offset, evicted })
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

    async fn record_received(
        &self,
        client: &ClientId,
        packet_id: u16,
    ) -> Result<bool, StorageError> {
        let mut map = self.lock();
        Ok(map
            .entry(client.clone())
            .or_default()
            .received_qos2
            .insert(packet_id))
    }

    async fn clear_received(&self, client: &ClientId, packet_id: u16) -> Result<(), StorageError> {
        if let Some(entry) = self.lock().get_mut(client) {
            entry.received_qos2.remove(&packet_id);
        }
        Ok(())
    }

    async fn received(&self, client: &ClientId) -> Result<Vec<u16>, StorageError> {
        Ok(self
            .lock()
            .get(client)
            .map(|e| e.received_qos2.iter().copied().collect())
            .unwrap_or_default())
    }

    async fn next_packet_id(&self, client: &ClientId) -> Result<u16, StorageError> {
        let mut map = self.lock();
        let entry = map.entry(client.clone()).or_default();
        // 1..=65535, wrapping, never 0.
        entry.last_packet_id = if entry.last_packet_id == u16::MAX {
            1
        } else {
            entry.last_packet_id + 1
        };
        Ok(entry.last_packet_id)
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

    async fn all(&self) -> Result<Vec<Message>, StorageError> {
        let map = self
            .by_topic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(map.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Enqueued, MemoryRetainedStore, MemorySessionStore, Offset, OverflowPolicy, QueueLimits,
        RetainedStore, SessionStore,
    };
    use mqtt_core::{ClientId, Message, QoS};

    fn cid(s: &str) -> ClientId {
        ClientId(s.to_string())
    }

    /// Offset of a `Stored` outcome; panics on `Rejected` (the tests that expect
    /// rejection assert it explicitly).
    fn offset_of(e: Enqueued) -> Offset {
        match e {
            Enqueued::Stored { offset, .. } => offset,
            Enqueued::Rejected => panic!("unexpected reject"),
        }
    }

    /// Current queue offsets for a client (oldest first).
    async fn offsets(store: &MemorySessionStore, c: &ClientId) -> Vec<Offset> {
        store
            .pending(c, 0, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.offset)
            .collect()
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

        let o0 = offset_of(store.enqueue(&c, &msg("a", b"0")).await.unwrap());
        let o1 = offset_of(store.enqueue(&c, &msg("a", b"1")).await.unwrap());
        let o2 = offset_of(store.enqueue(&c, &msg("a", b"2")).await.unwrap());
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
            let o = offset_of(store.enqueue(&c, &msg("a", b"x")).await.unwrap());
            assert_eq!(u64::from(i) + 1, o); // 1-based: 1..=5
        }
        store.ack(&c, 2).await.unwrap(); // ack offsets 1,2
        let remaining = store.pending(&c, 0, 100).await.unwrap();
        assert_eq!(remaining.len(), 3); // 3,4,5 remain
        assert_eq!(remaining[0].offset, 3);
    }

    /// Re-delivering an ack (or a stale, already-truncated offset) must be a
    /// no-op — failovers replay acks, and a panic or over-truncation here would
    /// lose messages.
    #[tokio::test]
    async fn ack_is_idempotent_and_ignores_stale_offsets() {
        let store = MemorySessionStore::new();
        let c = cid("client");
        store.ensure_session(&c).await.unwrap();
        for _ in 0..3 {
            store.enqueue(&c, &msg("a", b"x")).await.unwrap();
        }
        store.ack(&c, 2).await.unwrap();
        store.ack(&c, 2).await.unwrap(); // repeat
        store.ack(&c, 1).await.unwrap(); // stale
        let remaining = store.pending(&c, 0, 100).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].offset, 3);
    }

    /// Walking the queue page by page (the reconnect-replay pattern) visits
    /// every message exactly once, in offset order.
    #[tokio::test]
    async fn pending_pages_through_the_log_in_offset_order() {
        let store = MemorySessionStore::new();
        let c = cid("client");
        store.ensure_session(&c).await.unwrap();
        for _ in 0..5 {
            store.enqueue(&c, &msg("a", b"x")).await.unwrap();
        }
        let mut seen = Vec::new();
        let mut after = 0;
        loop {
            let page = store.pending(&c, after, 2).await.unwrap();
            if page.is_empty() {
                break;
            }
            after = page.last().unwrap().offset;
            seen.extend(page.into_iter().map(|m| m.offset));
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5]);
    }

    /// Subscriptions are replaced wholesale and survive until the session is
    /// removed (broker-restart reconciliation depends on this).
    #[tokio::test]
    async fn subscriptions_roundtrip_and_replace() {
        let store = MemorySessionStore::new();
        let c = cid("client");
        let sub = |f: &str| mqtt_core::Subscription {
            filter: f.to_string(),
            max_qos: QoS::AtMostOnce,
            no_local: false,
        };
        store
            .set_subscriptions(&c, &[sub("a/#"), sub("b/+")])
            .await
            .unwrap();
        let got = store.subscriptions(&c).await.unwrap();
        assert_eq!(got.len(), 2);

        // Replacement is wholesale, not a merge.
        store.set_subscriptions(&c, &[sub("c")]).await.unwrap();
        let got = store.subscriptions(&c).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].filter, "c");

        store.remove(&c).await.unwrap();
        assert!(store.subscriptions(&c).await.unwrap().is_empty());
    }

    /// The QoS-2 dedup window and the outbound packet-id counter — the exactly-once
    /// state (ADR 0006 §4).
    #[tokio::test]
    async fn qos2_dedup_and_packet_id_allocation() {
        let store = MemorySessionStore::new();
        let c = cid("client");
        // First receipt of a packet id is new; a duplicate re-send is not.
        assert!(store.record_received(&c, 7).await.unwrap());
        assert!(!store.record_received(&c, 7).await.unwrap());
        assert!(store.record_received(&c, 9).await.unwrap());
        assert_eq!(store.received(&c).await.unwrap(), vec![7, 9]);
        // Completing one frees it; a later re-use is new again.
        store.clear_received(&c, 7).await.unwrap();
        assert_eq!(store.received(&c).await.unwrap(), vec![9]);
        assert!(store.record_received(&c, 7).await.unwrap());

        // Outbound packet ids advance 1, 2, 3, ... (never 0).
        let p = cid("producer");
        assert_eq!(store.next_packet_id(&p).await.unwrap(), 1);
        assert_eq!(store.next_packet_id(&p).await.unwrap(), 2);
        assert_eq!(store.next_packet_id(&p).await.unwrap(), 3);

        // Removing the session clears its dedup window.
        store.remove(&c).await.unwrap();
        assert!(store.received(&c).await.unwrap().is_empty());
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

    /// Drop-oldest (the default) caps the queue at `max_messages`, evicting the
    /// oldest to make room; offsets stay monotonic and the cap holds.
    #[tokio::test]
    async fn drop_oldest_evicts_oldest_and_keeps_newest() {
        let store = MemorySessionStore::with_limits(QueueLimits {
            max_messages: 3,
            overflow: OverflowPolicy::DropOldest,
        });
        let c = cid("client");

        // First three fit without eviction.
        for expected in 1..=3 {
            assert_eq!(
                store.enqueue(&c, &msg("a", b"x")).await.unwrap(),
                Enqueued::Stored {
                    offset: expected,
                    evicted: 0,
                },
            );
        }
        assert_eq!(offsets(&store, &c).await, vec![1, 2, 3]);

        // The fourth evicts offset 1; the fifth evicts offset 2.
        assert_eq!(
            store.enqueue(&c, &msg("a", b"x")).await.unwrap(),
            Enqueued::Stored {
                offset: 4,
                evicted: 1
            }
        );
        assert_eq!(
            store.enqueue(&c, &msg("a", b"x")).await.unwrap(),
            Enqueued::Stored {
                offset: 5,
                evicted: 1
            }
        );
        // Cap held; newest three retained; offsets still monotonic.
        assert_eq!(offsets(&store, &c).await, vec![3, 4, 5]);

        // Ack of an already-evicted offset is a harmless no-op.
        store.ack(&c, 2).await.unwrap();
        assert_eq!(offsets(&store, &c).await, vec![3, 4, 5]);
        // Acking a live offset still truncates.
        store.ack(&c, 4).await.unwrap();
        assert_eq!(offsets(&store, &c).await, vec![5]);
    }

    /// Reject-newest keeps the queue intact and drops the arriving message once
    /// the cap is reached.
    #[tokio::test]
    async fn reject_newest_keeps_oldest_and_drops_new() {
        let store = MemorySessionStore::with_limits(QueueLimits {
            max_messages: 3,
            overflow: OverflowPolicy::RejectNewest,
        });
        let c = cid("client");
        for _ in 0..3 {
            assert!(matches!(
                store.enqueue(&c, &msg("a", b"x")).await.unwrap(),
                Enqueued::Stored { .. }
            ));
        }
        // Full now: further enqueues are rejected, queue unchanged.
        assert_eq!(
            store.enqueue(&c, &msg("a", b"new")).await.unwrap(),
            Enqueued::Rejected
        );
        assert_eq!(
            store.enqueue(&c, &msg("a", b"new")).await.unwrap(),
            Enqueued::Rejected
        );
        assert_eq!(offsets(&store, &c).await, vec![1, 2, 3]);

        // After an ack frees room, enqueue succeeds again.
        store.ack(&c, 1).await.unwrap();
        assert!(matches!(
            store.enqueue(&c, &msg("a", b"x")).await.unwrap(),
            Enqueued::Stored { offset: 4, .. }
        ));
        assert_eq!(offsets(&store, &c).await, vec![2, 3, 4]);
    }

    /// A persistent session that is being torn down behaves as a clustered
    /// failover would expect: unacked messages remain replayable.
    #[tokio::test]
    async fn unacked_messages_survive_for_replay() {
        let store = MemorySessionStore::new();
        let c = cid("client");
        store.ensure_session(&c).await.unwrap();
        let o = offset_of(store.enqueue(&c, &msg("a", b"important")).await.unwrap());
        // Simulate delivery attempt without ack (client dropped before PUBACK).
        let replay = store.pending(&c, 0, 100).await.unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].offset, o);
        // Still there until acked.
        assert_eq!(store.pending(&c, 0, 100).await.unwrap().len(), 1);
    }
}
