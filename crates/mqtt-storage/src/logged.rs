//! [`SessionStore`] expressed over a [`ReplicatedLog`] — the layering proof.
//!
//! [ADR 0006](../../../docs/adr/0006-consensus-and-replication.md) §3 makes
//! `ReplicatedLog` the seam between MQTT session/queue semantics and the
//! replication mechanism. [`ReplicatedSessionStore`] is the realization of that
//! split: it implements the full [`SessionStore`] interface using **only** a
//! `ReplicatedLog`, holding **no durable state of its own**. Point it at the
//! single-node [`InMemoryReplicatedLog`](crate::repl::InMemoryReplicatedLog) today
//! (development, tests, non-clustered deployments); point the identical code at the
//! consensus-backed log later and sessions become durable with no change to this
//! layer. That is the end-to-end layering ADR 0006's workstream-E step 2 calls for,
//! validated *before* any network code exists.
//!
//! ## Everything durable goes through the log
//!
//! The store keeps nothing in process memory between calls — every query is
//! answered from the log, every mutation is a log write. Two key spaces per client:
//!
//! - **`q/{client}`** — the offline message queue. `enqueue` appends an encoded
//!   message (the durability-critical write); `pending` reads it back; `ack`
//!   truncates it. The log's per-key offset *is* the message offset.
//! - **`m/{client}`** — a single session-metadata snapshot (subscriptions, the
//!   QoS-2 inbound dedup window, and the outbound packet-id counter). Each mutation
//!   reads-modifies-writes the snapshot (append the new one, truncate the prior), so
//!   a read takes the latest. Metadata is small and low-churn, so a snapshot is the
//!   right shape; only the high-churn queue stays incremental.
//!
//! Because no state hides in the store, a *second* `ReplicatedSessionStore` over
//! the same log observes the first's sessions in full — the test that pins the
//! layering (a durable log ⇒ durable sessions).
//!
//! ## Queue cap (ADR 0001 §6) lives here, above the seam
//!
//! The log is a pure, unbounded append-log; the per-session [`QueueLimits`] are a
//! `SessionStore` policy and are enforced in this layer by reading the queue and
//! truncating (drop-oldest) or refusing (reject-newest). Enforcement is exact when
//! appends to one key are serialized — which the production backend guarantees via
//! the ownership lease, and which holds trivially for sequential callers. The
//! single-node backend reads the whole queue to count; the consensus backend will
//! maintain an in-memory index (a rebuildable accelerator, not durable state).

use crate::repl::{ReplError, ReplicatedLog};
use crate::{Enqueued, QueueLimits, QueuedMessage, SessionStore, StorageError};
use async_trait::async_trait;
use mqtt_core::{ClientId, Message, QoS, Subscription};
use std::collections::BTreeSet;

impl From<ReplError> for StorageError {
    fn from(e: ReplError) -> Self {
        match e {
            // The session-store contract has no "not owner" / "no quorum" surface;
            // they collapse into a backend failure the caller already handles
            // (and which gates the QoS≥1 PUBACK exactly as a dropped append would).
            ReplError::NotOwner | ReplError::NoQuorum => StorageError::Backend(e.to_string()),
            ReplError::Backend(m) => StorageError::Backend(m),
        }
    }
}

/// A [`SessionStore`] backed entirely by a [`ReplicatedLog`].
///
/// Durable across exactly what the log is durable across: nothing for
/// [`InMemoryReplicatedLog`](crate::repl::InMemoryReplicatedLog), a single-node
/// loss for the consensus-backed log. See the module docs.
#[derive(Debug)]
pub struct ReplicatedSessionStore<L: ReplicatedLog<Key = String>> {
    log: L,
    limits: QueueLimits,
}

impl<L: ReplicatedLog<Key = String>> ReplicatedSessionStore<L> {
    /// Wrap `log` with default (bounded) queue limits.
    pub fn new(log: L) -> Self {
        Self {
            log,
            limits: QueueLimits::default(),
        }
    }

    /// Wrap `log` with explicit per-session queue limits.
    pub fn with_limits(log: L, limits: QueueLimits) -> Self {
        Self { log, limits }
    }

    fn queue_key(client: &ClientId) -> String {
        // The 'q'/'m' prefix byte makes the two key spaces disjoint, and the full
        // client id follows, so distinct clients never collide.
        format!("q/{}", client.0)
    }

    fn meta_key(client: &ClientId) -> String {
        format!("m/{}", client.0)
    }

    /// Read the session's metadata snapshot (`None` if no session record exists).
    ///
    /// The metadata log holds exactly one record — the latest snapshot — so we read
    /// it whole. Metadata is small and low-churn (subscriptions, the dedup window,
    /// the packet-id counter), so a read-modify-write snapshot is the right shape;
    /// the high-churn queue stays incremental.
    async fn load_meta(&self, client: &ClientId) -> Result<Option<SessionMeta>, StorageError> {
        let mkey = Self::meta_key(client);
        match self.log.read(&mkey, 0, usize::MAX).await?.last() {
            Some(entry) => Ok(Some(decode_session_meta(&entry.record)?)),
            None => Ok(None),
        }
    }

    /// Write the session's metadata snapshot, keeping exactly one record (append the
    /// new snapshot, truncate the prior one).
    async fn store_meta(&self, client: &ClientId, meta: &SessionMeta) -> Result<(), StorageError> {
        let mkey = Self::meta_key(client);
        let offset = self.log.append(&mkey, encode_session_meta(meta)).await?;
        if offset > 1 {
            self.log.truncate(&mkey, offset - 1).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<L: ReplicatedLog<Key = String>> SessionStore for ReplicatedSessionStore<L> {
    async fn ensure_session(&self, client: &ClientId) -> Result<bool, StorageError> {
        let qkey = Self::queue_key(client);
        // A session "exists" if a metadata snapshot or any queued message is present.
        let existed = self.load_meta(client).await?.is_some()
            || !self.log.read(&qkey, 0, 1).await?.is_empty();
        if !existed {
            // Persist a default snapshot so a subscription-less, message-less
            // persistent session still round-trips as present.
            self.store_meta(client, &SessionMeta::default()).await?;
        }
        Ok(existed)
    }

    async fn set_subscriptions(
        &self,
        client: &ClientId,
        subscriptions: &[Subscription],
    ) -> Result<(), StorageError> {
        // Read-modify-write the snapshot so the dedup window and packet-id counter
        // survive a subscription change.
        let mut meta = self.load_meta(client).await?.unwrap_or_default();
        meta.subscriptions = subscriptions.to_vec();
        self.store_meta(client, &meta).await
    }

    async fn subscriptions(&self, client: &ClientId) -> Result<Vec<Subscription>, StorageError> {
        Ok(self
            .load_meta(client)
            .await?
            .map(|m| m.subscriptions)
            .unwrap_or_default())
    }

    async fn enqueue(
        &self,
        client: &ClientId,
        message: &Message,
    ) -> Result<Enqueued, StorageError> {
        let qkey = Self::queue_key(client);
        let cap = self.limits.max_messages.max(1);

        // Apply the queue cap before appending (ADR 0001 §6). The log itself is
        // unbounded; the policy lives in this layer.
        let live = self.log.read(&qkey, 0, usize::MAX).await?;
        let mut evicted = 0u64;
        if live.len() >= cap {
            match self.limits.overflow {
                crate::OverflowPolicy::RejectNewest => return Ok(Enqueued::Rejected),
                crate::OverflowPolicy::DropOldest => {
                    // Evict the oldest entries so that, after the append, the queue
                    // holds exactly `cap`. They are the lowest offsets; one
                    // truncate up to the highest evicted offset drops them all.
                    let evict_count = live.len() - cap + 1;
                    let up_to = live[evict_count - 1].offset;
                    self.log.truncate(&qkey, up_to).await?;
                    evicted = evict_count as u64;
                }
            }
        }

        let offset = self.log.append(&qkey, encode_message(message)).await?;
        Ok(Enqueued::Stored { offset, evicted })
    }

    async fn pending(
        &self,
        client: &ClientId,
        after: crate::Offset,
        limit: usize,
    ) -> Result<Vec<QueuedMessage>, StorageError> {
        let qkey = Self::queue_key(client);
        let mut out = Vec::new();
        for entry in self.log.read(&qkey, after, limit).await? {
            out.push(QueuedMessage {
                offset: entry.offset,
                message: decode_message(&entry.record)?,
            });
        }
        Ok(out)
    }

    async fn ack(&self, client: &ClientId, up_to: crate::Offset) -> Result<(), StorageError> {
        // Local-first, lazy, idempotent — truncation tolerates stale offsets.
        self.log.truncate(&Self::queue_key(client), up_to).await?;
        Ok(())
    }

    async fn record_received(
        &self,
        client: &ClientId,
        packet_id: u16,
    ) -> Result<bool, StorageError> {
        let mut meta = self.load_meta(client).await?.unwrap_or_default();
        let newly = meta.received_qos2.insert(packet_id);
        // Always persist: even a duplicate must have materialized the session, and
        // the durable write is what gates the QoS-2 PUBREC.
        self.store_meta(client, &meta).await?;
        Ok(newly)
    }

    async fn clear_received(&self, client: &ClientId, packet_id: u16) -> Result<(), StorageError> {
        if let Some(mut meta) = self.load_meta(client).await? {
            if meta.received_qos2.remove(&packet_id) {
                self.store_meta(client, &meta).await?;
            }
        }
        Ok(())
    }

    async fn received(&self, client: &ClientId) -> Result<Vec<u16>, StorageError> {
        Ok(self
            .load_meta(client)
            .await?
            .map(|m| m.received_qos2.into_iter().collect())
            .unwrap_or_default())
    }

    async fn next_packet_id(&self, client: &ClientId) -> Result<u16, StorageError> {
        let mut meta = self.load_meta(client).await?.unwrap_or_default();
        meta.last_packet_id = if meta.last_packet_id == u16::MAX {
            1
        } else {
            meta.last_packet_id + 1
        };
        self.store_meta(client, &meta).await?;
        Ok(meta.last_packet_id)
    }

    async fn remove(&self, client: &ClientId) -> Result<(), StorageError> {
        self.log.remove(&Self::queue_key(client)).await?;
        self.log.remove(&Self::meta_key(client)).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Record codec.
//
// Records are opaque bytes to the log, so this layer owns their encoding. The
// project deliberately keeps `serde` off the `mqtt-core` domain types (the peer
// wire flattens them too), so this is a small, self-contained length-prefixed
// codec rather than a derive. Decode is defensive: a malformed record is a
// backend error, never a panic (`#![forbid(unsafe_code)]` discipline).
// ---------------------------------------------------------------------------

/// The whole of a session's durable metadata, stored as one snapshot record in the
/// `m/{client}` log: subscriptions, the QoS-2 inbound dedup window, and the outbound
/// packet-id counter. (The queue is separate, in the `q/{client}` log.)
#[derive(Default)]
struct SessionMeta {
    subscriptions: Vec<Subscription>,
    /// QoS-2 inbound packet ids received but not yet PUBREL-completed (dedup).
    received_qos2: BTreeSet<u16>,
    /// Last outbound packet id allocated (0 = none yet).
    last_packet_id: u16,
}

fn qos_to_u8(q: QoS) -> u8 {
    match q {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    // Records are internal and far below 4 GiB; a length that does not fit u32
    // would be a programming error, so saturate rather than fail the write path.
    let len = u32::try_from(b.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&b[..len as usize]);
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, s.as_bytes());
}

fn encode_message(m: &Message) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, &m.topic);
    put_bytes(&mut out, &m.payload);
    out.push(qos_to_u8(m.qos));
    out.push(u8::from(m.retain));
    out
}

fn encode_session_meta(m: &SessionMeta) -> Vec<u8> {
    let mut out = Vec::new();
    let n = u32::try_from(m.subscriptions.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&n.to_be_bytes());
    for s in m.subscriptions.iter().take(n as usize) {
        put_str(&mut out, &s.filter);
        out.push(qos_to_u8(s.max_qos));
        out.push(u8::from(s.no_local));
    }
    let rn = u32::try_from(m.received_qos2.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&rn.to_be_bytes());
    for id in m.received_qos2.iter().take(rn as usize) {
        out.extend_from_slice(&id.to_be_bytes());
    }
    out.extend_from_slice(&m.last_packet_id.to_be_bytes());
    out
}

/// A cursor over a record body that never reads out of bounds.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], StorageError> {
        let end = self.pos.checked_add(n).filter(|e| *e <= self.buf.len());
        match end {
            Some(end) => {
                let s = &self.buf[self.pos..end];
                self.pos = end;
                Ok(s)
            }
            None => Err(corrupt()),
        }
    }

    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, StorageError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn bytes(&mut self) -> Result<&'a [u8], StorageError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn string(&mut self) -> Result<String, StorageError> {
        let b = self.bytes()?;
        std::str::from_utf8(b)
            .map(str::to_owned)
            .map_err(|_| corrupt())
    }
}

fn corrupt() -> StorageError {
    StorageError::Backend("corrupt log record".to_string())
}

fn qos_from_u8(v: u8) -> Result<QoS, StorageError> {
    QoS::from_u8(v).ok_or_else(corrupt)
}

fn decode_message(buf: &[u8]) -> Result<Message, StorageError> {
    let mut r = Reader::new(buf);
    let topic = r.string()?;
    let payload = bytes::Bytes::copy_from_slice(r.bytes()?);
    let qos = qos_from_u8(r.u8()?)?;
    let retain = r.u8()? != 0;
    Ok(Message {
        topic,
        payload,
        qos,
        retain,
    })
}

fn decode_session_meta(buf: &[u8]) -> Result<SessionMeta, StorageError> {
    let mut r = Reader::new(buf);
    let n = r.u32()? as usize;
    let mut subscriptions = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let filter = r.string()?;
        let max_qos = qos_from_u8(r.u8()?)?;
        let no_local = r.u8()? != 0;
        subscriptions.push(Subscription {
            filter,
            max_qos,
            no_local,
        });
    }
    let rn = r.u32()? as usize;
    let mut received_qos2 = BTreeSet::new();
    for _ in 0..rn {
        received_qos2.insert(r.u16()?);
    }
    let last_packet_id = r.u16()?;
    Ok(SessionMeta {
        subscriptions,
        received_qos2,
        last_packet_id,
    })
}

#[cfg(test)]
mod tests {
    use super::ReplicatedSessionStore;
    use crate::repl::InMemoryReplicatedLog;
    use crate::{Enqueued, Offset, OverflowPolicy, QueueLimits, SessionStore};
    use mqtt_core::{ClientId, Message, QoS, Subscription};
    use std::sync::Arc;

    fn cid(s: &str) -> ClientId {
        ClientId(s.to_string())
    }

    fn store() -> ReplicatedSessionStore<InMemoryReplicatedLog> {
        ReplicatedSessionStore::new(InMemoryReplicatedLog::new())
    }

    fn msg(topic: &str, payload: &'static [u8], qos: QoS) -> Message {
        Message {
            topic: topic.to_string(),
            payload: bytes::Bytes::from_static(payload),
            qos,
            retain: false,
        }
    }

    fn sub(filter: &str, max_qos: QoS, no_local: bool) -> Subscription {
        Subscription {
            filter: filter.to_string(),
            max_qos,
            no_local,
        }
    }

    fn offset_of(e: Enqueued) -> Offset {
        match e {
            Enqueued::Stored { offset, .. } => offset,
            Enqueued::Rejected => panic!("unexpected reject"),
        }
    }

    async fn offsets<L: crate::repl::ReplicatedLog<Key = String>>(
        store: &ReplicatedSessionStore<L>,
        c: &ClientId,
    ) -> Vec<Offset> {
        store
            .pending(c, 0, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.offset)
            .collect()
    }

    #[tokio::test]
    async fn ensure_session_reports_existence() {
        let s = store();
        let c = cid("c");
        assert!(!s.ensure_session(&c).await.unwrap(), "fresh");
        assert!(s.ensure_session(&c).await.unwrap(), "now present");
    }

    /// An enqueue alone materializes the session (matches `MemorySessionStore`).
    #[tokio::test]
    async fn enqueue_materializes_session() {
        let s = store();
        let c = cid("c");
        s.enqueue(&c, &msg("a", b"x", QoS::AtLeastOnce))
            .await
            .unwrap();
        assert!(s.ensure_session(&c).await.unwrap(), "enqueue created it");
    }

    #[tokio::test]
    async fn enqueue_assigns_monotonic_offsets_and_replays_with_payload() {
        let s = store();
        let c = cid("c");
        let o1 = offset_of(
            s.enqueue(&c, &msg("a", b"0", QoS::AtLeastOnce))
                .await
                .unwrap(),
        );
        let o2 = offset_of(
            s.enqueue(&c, &msg("b", b"11", QoS::ExactlyOnce))
                .await
                .unwrap(),
        );
        assert_eq!((o1, o2), (1, 2));

        let all = s.pending(&c, 0, 100).await.unwrap();
        assert_eq!(all.len(), 2);
        // The message survives the encode/decode round-trip intact.
        assert_eq!(all[0].message.topic, "a");
        assert_eq!(&all[0].message.payload[..], b"0");
        assert_eq!(all[0].message.qos, QoS::AtLeastOnce);
        assert_eq!(all[1].message.topic, "b");
        assert_eq!(&all[1].message.payload[..], b"11");
        assert_eq!(all[1].message.qos, QoS::ExactlyOnce);

        // `after` cursor and `limit` both honored.
        assert_eq!(s.pending(&c, o1, 100).await.unwrap().len(), 1);
        assert_eq!(s.pending(&c, 0, 1).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ack_truncates_and_is_idempotent() {
        let s = store();
        let c = cid("c");
        for _ in 0..5 {
            s.enqueue(&c, &msg("a", b"x", QoS::AtLeastOnce))
                .await
                .unwrap();
        }
        s.ack(&c, 2).await.unwrap();
        assert_eq!(offsets(&s, &c).await, vec![3, 4, 5]);
        // Repeat + stale offset are no-ops (failovers replay acks).
        s.ack(&c, 2).await.unwrap();
        s.ack(&c, 1).await.unwrap();
        assert_eq!(offsets(&s, &c).await, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn drop_oldest_caps_the_queue() {
        let s = ReplicatedSessionStore::with_limits(
            InMemoryReplicatedLog::new(),
            QueueLimits {
                max_messages: 3,
                overflow: OverflowPolicy::DropOldest,
            },
        );
        let c = cid("c");
        for expected in 1..=3 {
            assert_eq!(
                s.enqueue(&c, &msg("a", b"x", QoS::AtLeastOnce))
                    .await
                    .unwrap(),
                Enqueued::Stored {
                    offset: expected,
                    evicted: 0
                },
            );
        }
        assert_eq!(offsets(&s, &c).await, vec![1, 2, 3]);
        // Fourth evicts offset 1; fifth evicts offset 2. Offsets stay monotonic.
        assert_eq!(
            s.enqueue(&c, &msg("a", b"x", QoS::AtLeastOnce))
                .await
                .unwrap(),
            Enqueued::Stored {
                offset: 4,
                evicted: 1
            },
        );
        assert_eq!(
            s.enqueue(&c, &msg("a", b"x", QoS::AtLeastOnce))
                .await
                .unwrap(),
            Enqueued::Stored {
                offset: 5,
                evicted: 1
            },
        );
        assert_eq!(offsets(&s, &c).await, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn reject_newest_keeps_oldest() {
        let s = ReplicatedSessionStore::with_limits(
            InMemoryReplicatedLog::new(),
            QueueLimits {
                max_messages: 2,
                overflow: OverflowPolicy::RejectNewest,
            },
        );
        let c = cid("c");
        s.enqueue(&c, &msg("a", b"1", QoS::AtLeastOnce))
            .await
            .unwrap();
        s.enqueue(&c, &msg("a", b"2", QoS::AtLeastOnce))
            .await
            .unwrap();
        assert_eq!(
            s.enqueue(&c, &msg("a", b"3", QoS::AtLeastOnce))
                .await
                .unwrap(),
            Enqueued::Rejected
        );
        assert_eq!(offsets(&s, &c).await, vec![1, 2]);
        // Freeing room lets the next enqueue land.
        s.ack(&c, 1).await.unwrap();
        assert!(matches!(
            s.enqueue(&c, &msg("a", b"4", QoS::AtLeastOnce))
                .await
                .unwrap(),
            Enqueued::Stored { offset: 3, .. }
        ));
        assert_eq!(offsets(&s, &c).await, vec![2, 3]);
    }

    #[tokio::test]
    async fn subscriptions_roundtrip_replace_and_survive_remove() {
        let s = store();
        let c = cid("c");
        s.set_subscriptions(
            &c,
            &[
                sub("a/#", QoS::AtLeastOnce, false),
                sub("b/+", QoS::ExactlyOnce, true),
            ],
        )
        .await
        .unwrap();
        let got = s.subscriptions(&c).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].filter, "a/#");
        assert_eq!(got[0].max_qos, QoS::AtLeastOnce);
        assert!(got[1].no_local);

        // Replacement is wholesale, not a merge.
        s.set_subscriptions(&c, &[sub("c", QoS::AtMostOnce, false)])
            .await
            .unwrap();
        let got = s.subscriptions(&c).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].filter, "c");

        s.remove(&c).await.unwrap();
        assert!(s.subscriptions(&c).await.unwrap().is_empty());
        assert!(
            !s.ensure_session(&c).await.unwrap(),
            "session gone after remove"
        );
    }

    #[tokio::test]
    async fn remove_clears_queue_and_meta() {
        let s = store();
        let c = cid("c");
        s.set_subscriptions(&c, &[sub("a", QoS::AtMostOnce, false)])
            .await
            .unwrap();
        s.enqueue(&c, &msg("a", b"x", QoS::AtLeastOnce))
            .await
            .unwrap();
        s.remove(&c).await.unwrap();
        assert!(s.pending(&c, 0, 100).await.unwrap().is_empty());
        assert!(s.subscriptions(&c).await.unwrap().is_empty());
    }

    /// The layering proof: a second store over the **same log** sees the first
    /// store's session in full. Nothing durable hides in store-local memory, so a
    /// durable log yields durable sessions — exactly what workstream E builds on.
    #[tokio::test]
    async fn state_lives_in_the_log_not_the_store() {
        let log = Arc::new(InMemoryReplicatedLog::new());
        let c = cid("c");

        let writer = ReplicatedSessionStore::new(log.clone());
        writer.ensure_session(&c).await.unwrap();
        writer
            .set_subscriptions(&c, &[sub("sensors/#", QoS::AtLeastOnce, false)])
            .await
            .unwrap();
        writer
            .enqueue(&c, &msg("sensors/a", b"21", QoS::AtLeastOnce))
            .await
            .unwrap();
        writer
            .enqueue(&c, &msg("sensors/b", b"22", QoS::AtLeastOnce))
            .await
            .unwrap();

        // A fresh store sharing only the log — no shared in-process state.
        let reader = ReplicatedSessionStore::new(log.clone());
        assert!(
            reader.ensure_session(&c).await.unwrap(),
            "session is in the log"
        );
        let subs = reader.subscriptions(&c).await.unwrap();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].filter, "sensors/#");
        let pending = reader.pending(&c, 0, 100).await.unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(&pending[0].message.payload[..], b"21");
        assert_eq!(&pending[1].message.payload[..], b"22");

        // An ack through the reader is visible to the writer — one shared log.
        reader.ack(&c, 1).await.unwrap();
        assert_eq!(offsets(&writer, &c).await, vec![2]);
    }

    /// The exactly-once state replicates through the log: a second store over the
    /// same log sees the dedup window and the packet-id counter the first wrote —
    /// so exactly-once survives a failover to that replica (ADR 0006 §4).
    #[tokio::test]
    async fn qos2_state_replicates_through_the_log() {
        let log = Arc::new(InMemoryReplicatedLog::new());
        let c = cid("c");

        let writer = ReplicatedSessionStore::new(log.clone());
        assert!(
            writer.record_received(&c, 5).await.unwrap(),
            "first receipt"
        );
        assert!(!writer.record_received(&c, 5).await.unwrap(), "duplicate");
        assert_eq!(writer.next_packet_id(&c).await.unwrap(), 1);
        assert_eq!(writer.next_packet_id(&c).await.unwrap(), 2);

        // A fresh store (the failover replica) sharing only the log.
        let reader = ReplicatedSessionStore::new(log.clone());
        // The dedup window survived: 5 is still "received".
        assert_eq!(reader.received(&c).await.unwrap(), vec![5]);
        assert!(
            !reader.record_received(&c, 5).await.unwrap(),
            "5 is a duplicate to the replica too — no re-delivery after failover",
        );
        // The packet-id counter survived: allocation continues at 3, no collision.
        assert_eq!(reader.next_packet_id(&c).await.unwrap(), 3);

        // Clearing on the reader is visible to the writer (one shared log).
        reader.clear_received(&c, 5).await.unwrap();
        assert!(writer.received(&c).await.unwrap().is_empty());
    }
}
