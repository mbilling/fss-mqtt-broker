//! The replicated **retained-message keyspace** over a [`ReplicatedLog`]
//! ([ADR 0037](../../../docs/adr/0037-durable-retained-messages.md) P2).
//!
//! Each topic's retained state is one logical log key, `r/<topic>` (the 2-byte prefix
//! matches the `q/`/`m/` session keys, which the group router relies on to recover the
//! placement key). A retained *set* appends the value and **compacts** the key to that
//! last record; a *clear* appends a **versioned tombstone** the same way — so the key
//! always holds exactly one live record, whose `(epoch, offset)` is the clock-free
//! convergence token every cache/back-fill decision reduces to. Conflicts cannot form:
//! on a clustered backend the append commits through the topic's group lease-holder
//! (quorum-fenced), and a non-owner gets `NotOwner` rather than a divergent local write.
//!
//! Like [`ReplicatedSessionStore`](crate::logged::ReplicatedSessionStore), this type
//! holds **no state of its own** — everything lives in the log, so owner takeover and
//! restart recovery are inherited from the log's machinery.

use crate::app_props::AppProps;
use crate::repl::ReplicatedLog;
use crate::{Offset, StorageError};
use async_trait::async_trait;

/// One committed retained value (or tombstone) with its convergence token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedEntry {
    /// The retained payload (empty for a tombstone).
    pub payload: Vec<u8>,
    /// The publish `QoS` as its 2-bit wire value.
    pub qos: u8,
    /// Whether this is a **clear** (zero-length retained publish, MQTT-3.3.1-10),
    /// versioned like any value so a clear also wins/loses by token, never by luck.
    pub tombstone: bool,
    /// The publisher's forwardable MQTT 5 application properties (ADR 0038 T3), so a
    /// replay from the durable record carries them exactly as published
    /// (MQTT-3.3.2-17). Empty for a tombstone.
    pub props: AppProps,
    /// The lease epoch the write was routed under (see [`ReplicatedRetained::set`] for
    /// the benign skew note). `0` on single-node backends.
    pub epoch: u64,
    /// The record's committed log offset — strictly increasing per topic.
    pub offset: Offset,
}

impl RetainedEntry {
    /// The convergence token: lexicographic `(epoch, offset)` — epochs are globally
    /// monotonic (consensus-issued) and offsets strictly increase per key, so a higher
    /// token is a strictly later committed write.
    #[must_use]
    pub fn token(&self) -> (u64, Offset) {
        (self.epoch, self.offset)
    }
}

/// The retained keyspace over any [`ReplicatedLog`] (ADR 0037 P2).
#[derive(Debug)]
pub struct ReplicatedRetained<L: ReplicatedLog<Key = String>> {
    log: L,
}

/// The log key for `topic`'s retained state. A 2-byte prefix, like `q/`/`m/`, so the
/// group router's placement-key recovery (`key[2..]`) yields the topic.
fn retained_key(topic: &str) -> String {
    format!("r/{topic}")
}

impl<L: ReplicatedLog<Key = String>> ReplicatedRetained<L> {
    /// Wrap `log`; all state lives in the log.
    pub fn new(log: L) -> Self {
        Self { log }
    }

    /// Commit `topic`'s retained value, compact the key to it, and return the
    /// `(epoch, offset)` token.
    ///
    /// The epoch is read from the same route the append commits through. If the lease
    /// moves *between* the two calls the append itself is fenced (or commits under a
    /// **higher** epoch than reported) — the token can only understate, never overstate,
    /// so token ordering is unaffected: offsets strictly increase per key regardless.
    ///
    /// # Errors
    /// `NotOwner` when this node does not hold the topic's group lease (the caller
    /// routes/queues per ADR 0037 §5); `NoQuorum`/`Backend` as for any durable write.
    pub async fn set(
        &self,
        topic: &str,
        payload: &[u8],
        qos: u8,
        props: &AppProps,
    ) -> Result<(u64, Offset), StorageError> {
        self.write(topic, payload, qos, props, false).await
    }

    /// Commit a **versioned tombstone** for `topic` (a zero-length retained publish
    /// clears the value, MQTT-3.3.1-10) and return its token. The tombstone stays as
    /// the key's single live record so a heal compares clears by token like any value.
    ///
    /// # Errors
    /// As for [`set`](Self::set).
    pub async fn clear(&self, topic: &str) -> Result<(u64, Offset), StorageError> {
        self.write(topic, &[], 0, &AppProps::default(), true).await
    }

    async fn write(
        &self,
        topic: &str,
        payload: &[u8],
        qos: u8,
        props: &AppProps,
        tombstone: bool,
    ) -> Result<(u64, Offset), StorageError> {
        let key = retained_key(topic);
        let epoch = self.log.epoch_for(&key).await?;
        let record = encode_retained(payload, qos, props, tombstone, epoch);
        let offset = self.log.append(&key, record).await?;
        // Last-value compaction: exactly one live record per topic. Local-first/lazy
        // (like every truncate); a reader between append and truncate still takes the
        // *last* record, so compaction lag is invisible.
        if offset > 1 {
            self.log.truncate(&key, offset - 1).await?;
        }
        Ok((epoch, offset))
    }

    /// The current committed retained state of `topic`: the value, a tombstone, or
    /// `None` if never written.
    ///
    /// # Errors
    /// Storage/routing errors as for any durable read.
    pub async fn get(&self, topic: &str) -> Result<Option<RetainedEntry>, StorageError> {
        let key = retained_key(topic);
        let last = self.log.read(&key, 0, usize::MAX).await?.into_iter().last();
        Ok(last.and_then(|e| decode_retained(&e.record, e.offset)))
    }
}

/// Object-safe handle to the durable retained keyspace (ADR 0037), so the broker can
/// hold the authority without naming the backend log type — the same seam
/// [`RetainedStore`](crate::RetainedStore) provides for the node-local cache.
///
/// Implemented by [`ReplicatedRetained`] over any log; the broker routes a retained
/// mutation here (on the topic's group lease-owner) *in addition to* the local cache
/// write, and gets back the `(epoch, offset)` convergence token.
#[async_trait]
pub trait DurableRetained: Send + Sync + std::fmt::Debug {
    /// Commit `topic`'s retained value; returns its `(epoch, offset)` token.
    ///
    /// # Errors
    /// As [`ReplicatedRetained::set`].
    async fn set(
        &self,
        topic: &str,
        payload: &[u8],
        qos: u8,
        props: &AppProps,
    ) -> Result<(u64, Offset), StorageError>;

    /// Commit a versioned tombstone for `topic`; returns its token.
    ///
    /// # Errors
    /// As [`ReplicatedRetained::clear`].
    async fn clear(&self, topic: &str) -> Result<(u64, Offset), StorageError>;

    /// The committed retained state of `topic` (value, tombstone, or never written).
    ///
    /// # Errors
    /// As [`ReplicatedRetained::get`].
    async fn get(&self, topic: &str) -> Result<Option<RetainedEntry>, StorageError>;
}

#[async_trait]
impl<L: ReplicatedLog<Key = String>> DurableRetained for ReplicatedRetained<L> {
    async fn set(
        &self,
        topic: &str,
        payload: &[u8],
        qos: u8,
        props: &AppProps,
    ) -> Result<(u64, Offset), StorageError> {
        Self::set(self, topic, payload, qos, props).await
    }

    async fn clear(&self, topic: &str) -> Result<(u64, Offset), StorageError> {
        Self::clear(self, topic).await
    }

    async fn get(&self, topic: &str) -> Result<Option<RetainedEntry>, StorageError> {
        Self::get(self, topic).await
    }
}

/// Record layout: `[epoch u64][qos u8][tombstone u8][props_len u32][props][payload …]`
/// (big-endian). The properties block is [`AppProps::encode`]'s output,
/// length-prefixed; the payload runs to the end of the record — no payload length
/// field to disagree with reality. Layout v2 (ADR 0038 T3): the stores holding these
/// records bumped their schema stamps, so a pre-props file fails closed at the gate
/// instead of silently decoding garbage.
fn encode_retained(
    payload: &[u8],
    qos: u8,
    props: &AppProps,
    tombstone: bool,
    epoch: u64,
) -> Vec<u8> {
    let props = props.encode();
    let mut out = Vec::with_capacity(14 + props.len() + payload.len());
    out.extend_from_slice(&epoch.to_be_bytes());
    out.push(qos);
    out.push(u8::from(tombstone));
    out.extend_from_slice(&u32::try_from(props.len()).unwrap_or(u32::MAX).to_be_bytes());
    out.extend_from_slice(&props);
    out.extend_from_slice(payload);
    out
}

/// Decode a retained record; `None` (treated as absent, fail-closed) on a short or
/// malformed record.
fn decode_retained(record: &[u8], offset: Offset) -> Option<RetainedEntry> {
    let epoch = u64::from_be_bytes(record.get(0..8)?.try_into().ok()?);
    let qos = *record.get(8)?;
    let tombstone = match record.get(9)? {
        0 => false,
        1 => true,
        _ => return None,
    };
    let props_len = u32::from_be_bytes(record.get(10..14)?.try_into().ok()?) as usize;
    let props = AppProps::decode(record.get(14..14 + props_len)?)?;
    Some(RetainedEntry {
        payload: record.get(14 + props_len..)?.to_vec(),
        qos,
        tombstone,
        props,
        epoch,
        offset,
    })
}

#[cfg(test)]
mod tests {
    use super::{decode_retained, encode_retained, AppProps, ReplicatedRetained};
    use crate::persistent_log::PersistentLog;
    use crate::repl::{InMemoryReplicatedLog, ReplicatedLog};

    fn store() -> ReplicatedRetained<InMemoryReplicatedLog> {
        ReplicatedRetained::new(InMemoryReplicatedLog::new())
    }

    #[test]
    fn the_record_codec_roundtrips_values_and_tombstones() {
        let props = AppProps {
            content_type: Some("application/json".into()),
            user_properties: vec![("trace".into(), "abc".into())],
            ..AppProps::default()
        };
        let rec = encode_retained(b"state", 1, &props, false, 7);
        let e = decode_retained(&rec, 3).unwrap();
        assert_eq!(
            (e.payload.as_slice(), e.qos, e.tombstone, e.epoch, e.offset),
            (b"state".as_ref(), 1, false, 7, 3)
        );
        assert_eq!(e.props, props, "application properties survive the codec");
        let tomb = encode_retained(b"", 0, &AppProps::default(), true, 9);
        let t = decode_retained(&tomb, 4).unwrap();
        assert!(t.tombstone && t.payload.is_empty() && t.props.is_empty());
        // Short/malformed records are absent, not garbage.
        assert!(decode_retained(&rec[..9], 1).is_none());
        assert!(
            decode_retained(&rec[..13], 1).is_none(),
            "truncated props length"
        );
        assert!(
            decode_retained(&[0xFF; 14], 1).is_none(),
            "bad tombstone flag"
        );
    }

    #[tokio::test]
    async fn set_then_get_returns_the_value_with_its_token() {
        let r = store();
        let (epoch, offset) = r
            .set("dev/1", b"open", 1, &AppProps::default())
            .await
            .unwrap();
        assert_eq!(
            (epoch, offset),
            (0, 1),
            "single-node epoch 0, first offset 1"
        );
        let e = r.get("dev/1").await.unwrap().unwrap();
        assert_eq!(e.payload, b"open");
        assert_eq!(e.qos, 1);
        assert!(!e.tombstone);
        assert_eq!(e.token(), (0, 1));
    }

    #[tokio::test]
    async fn a_second_set_compacts_the_key_to_the_last_value() {
        let r = store();
        r.set("t", b"v1", 0, &AppProps::default()).await.unwrap();
        let (_, off2) = r.set("t", b"v2", 0, &AppProps::default()).await.unwrap();
        assert_eq!(off2, 2, "offsets strictly increase per topic");
        let e = r.get("t").await.unwrap().unwrap();
        assert_eq!(e.payload, b"v2");
        assert_eq!(e.offset, 2);
        // Compaction: exactly one live record remains.
        let range = r.log.live_range(&"r/t".to_string()).await.unwrap();
        assert_eq!(range, Some((2, 2)), "the prior record is truncated away");
    }

    #[tokio::test]
    async fn a_clear_is_a_versioned_tombstone_not_an_absence() {
        let r = store();
        r.set("t", b"v", 0, &AppProps::default()).await.unwrap();
        let (_, off) = r.clear("t").await.unwrap();
        assert_eq!(off, 2, "the clear takes the next offset like any write");
        let e = r.get("t").await.unwrap().unwrap();
        assert!(
            e.tombstone,
            "a clear is a value with a token, not a deletion"
        );
        assert_eq!(e.token(), (0, 2));
        let range = r.log.live_range(&"r/t".to_string()).await.unwrap();
        assert_eq!(
            range,
            Some((2, 2)),
            "the tombstone is the single live record"
        );
    }

    #[tokio::test]
    async fn topics_are_independent_keys() {
        let r = store();
        r.set("a", b"1", 0, &AppProps::default()).await.unwrap();
        r.set("b", b"2", 0, &AppProps::default()).await.unwrap();
        r.clear("a").await.unwrap();
        assert!(r.get("a").await.unwrap().unwrap().tombstone);
        assert_eq!(r.get("b").await.unwrap().unwrap().payload, b"2");
        assert!(r.get("never-written").await.unwrap().is_none());
    }

    /// Restart recovery (ADR 0018): the retained value, its token, and the offset
    /// high-water all come back from the persisted log — a post-restart write cannot
    /// reuse an offset and regress the token.
    #[tokio::test]
    async fn a_restart_recovers_the_value_token_and_offset_high_water() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.redb");
        let props = AppProps {
            content_type: Some("text/plain".into()),
            ..AppProps::default()
        };
        {
            let r = ReplicatedRetained::new(PersistentLog::open(&path).unwrap());
            r.set("t", b"v1", 1, &AppProps::default()).await.unwrap();
            assert_eq!(r.set("t", b"v2", 1, &props).await.unwrap(), (0, 2));
        }
        // Reopen: the compacted last value, its token, and its application
        // properties (ADR 0038 T3) survived the restart.
        let r = ReplicatedRetained::new(PersistentLog::open(&path).unwrap());
        let e = r.get("t").await.unwrap().unwrap();
        assert_eq!(e.payload, b"v2");
        assert_eq!(e.token(), (0, 2));
        assert_eq!(e.props, props);
        // Writes continue after the recovered high-water — no offset reuse.
        assert_eq!(
            r.set("t", b"v3", 1, &AppProps::default()).await.unwrap(),
            (0, 3)
        );
    }

    #[tokio::test]
    async fn tokens_strictly_increase_across_sets_and_clears() {
        let r = store();
        let mut last = (0, 0);
        for i in 0..5u8 {
            let tok = if i % 2 == 0 {
                r.set("t", &[i], 0, &AppProps::default()).await.unwrap()
            } else {
                r.clear("t").await.unwrap()
            };
            assert!(
                tok > last,
                "token must strictly increase: {tok:?} vs {last:?}"
            );
            last = tok;
        }
    }
}
