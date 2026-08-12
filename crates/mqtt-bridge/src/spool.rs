//! Bounded store-and-forward spool
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §7, T7).
//!
//! When a side is momentarily unreachable, messages destined for it are held here and
//! replayed on reconnect. The spool is **bounded** (never grows without limit, like the
//! broker's offline queues, ADR 0001 §6) and, when a directory is configured, **disk-backed**
//! (`redb`), so a brief bridge restart does not lose them. At the cap the [`Overflow`] policy
//! decides: **refuse** the new message (the default, and what a `QoS`≥1 rule uses — everything
//! already spooled was acknowledged to the source, so shedding it would lose a message the
//! source believes was delivered) or **drop the oldest** (`QoS` 0, which promises nothing).
//! Delivery is at-least-once for `QoS` ≥ 1 (§7); a replayed message keeps its topic,
//! payload, `QoS`, and User Properties (incl. the hop count) intact.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use redb::{Database, Durability, ReadableTable, ReadableTableMetadata, TableDefinition};
use tracing::info;

/// One spooled message (already transformed by the forwarding policy).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpooledMessage {
    /// Destination topic.
    pub topic: String,
    /// Application payload.
    pub payload: Vec<u8>,
    /// Delivery `QoS` wire value.
    pub qos: u8,
    /// Whether to forward with the RETAIN flag set (issue #189).
    pub retain: bool,
    /// User Properties to forward (includes the incremented hop count).
    pub user_properties: Vec<(String, String)>,
}

const SPOOL: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("spool");

/// A bounded FIFO spool, in memory or disk-backed.
#[derive(Debug)]
pub struct Spool {
    cap: usize,
    inner: Mutex<Inner>,
    /// Messages discarded because the spool was already at `cap` under
    /// [`Overflow::DropOldest`].
    ///
    /// Shedding used to happen in total silence: `push` evicted and returned `Ok(())` with no
    /// counter, no log, and no error. Store-and-forward is the bridge's durability story, so
    /// the moment it starts shedding is exactly the moment an operator needs to know. Counted
    /// here and exported as `fss_bridge_dropped_total{reason="spool-full"}`, and audited
    /// per-message (ADR 0060 T5). A [`Overflow::Refuse`] rejection is **not** counted here —
    /// nothing was lost; the source keeps the message and redelivers it.
    dropped: AtomicU64,
    /// What to do at the cap (ADR 0060 T5). Default [`Overflow::Refuse`].
    overflow: Overflow,
}

#[derive(Debug)]
enum Inner {
    Mem(VecDeque<SpooledMessage>),
    Disk { db: Database, next: u64 },
}

/// A spool error (disk I/O / codec).
#[derive(Debug, thiserror::Error)]
pub enum SpoolError {
    /// An underlying `redb` / I/O failure.
    #[error("spool: {0}")]
    Backend(String),
    /// The spool is at its cap and the overflow policy is [`Overflow::Refuse`] — the message
    /// was **not** accepted, so the caller must not acknowledge it (ADR 0060 T2/T5).
    #[error("spool full")]
    Full,
}

/// What a bounded spool does when it is at its cap (ADR 0060 T5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overflow {
    /// Refuse the new message (return [`SpoolError::Full`]) and keep what is already spooled.
    /// The default for a `QoS`≥1 rule: everything already spooled was **acknowledged to the
    /// source**, so shedding it would lose a message the source believes was delivered. The
    /// new message is simply not acked, and the source redelivers it.
    #[default]
    Refuse,
    /// Drop the oldest spooled message to make room. Appropriate for `QoS` 0, which promises
    /// nothing — a stalled crossing would be worse than a shed at-most-once message.
    DropOldest,
}

fn backend(e: impl std::fmt::Display) -> SpoolError {
    SpoolError::Backend(e.to_string())
}

impl Spool {
    /// An in-memory spool bounded to `cap` messages (drop-oldest past it).
    #[must_use]
    pub fn in_memory(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            inner: Mutex::new(Inner::Mem(VecDeque::new())),
            dropped: AtomicU64::new(0),
            overflow: Overflow::default(),
        }
    }

    /// A disk-backed spool at `path`, bounded to `cap` messages. Reopens an existing file,
    /// so messages spooled before a restart are still there to replay.
    ///
    /// # Errors
    /// [`SpoolError::Backend`] if the database cannot be opened.
    pub fn on_disk(path: &Path, cap: usize) -> Result<Self, SpoolError> {
        let db = mqtt_storage::open::create_with_lock_retry(path).map_err(backend)?;
        // Find the highest existing key so new pushes continue past it.
        let next = {
            let tx = db.begin_read().map_err(backend)?;
            match tx.open_table(SPOOL) {
                Ok(t) => t.last().map_err(backend)?.map_or(0, |(k, _)| k.value() + 1),
                Err(redb::TableError::TableDoesNotExist(_)) => 0,
                Err(e) => return Err(backend(e)),
            }
        };
        Ok(Self {
            cap: cap.max(1),
            overflow: Overflow::default(),
            inner: Mutex::new(Inner::Disk { db, next }),
            dropped: AtomicU64::new(0),
        })
    }

    /// Use `policy` at the cap instead of the default [`Overflow::Refuse`].
    #[must_use]
    pub fn with_overflow(mut self, policy: Overflow) -> Self {
        self.overflow = policy;
        self
    }

    /// Append `msg`. At the cap the [`Overflow`] policy decides: refuse the new message, or
    /// drop the oldest to make room.
    ///
    /// # Errors
    /// [`SpoolError::Full`] when at the cap under [`Overflow::Refuse`] — the message was **not**
    /// accepted, so the caller must not acknowledge it (ADR 0060 T2).
    /// [`SpoolError::Backend`] on a disk failure.
    pub fn push(&self, msg: &SpooledMessage) -> Result<(), SpoolError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *inner {
            Inner::Mem(q) => {
                if q.len() >= self.cap {
                    if self.overflow == Overflow::Refuse {
                        info!(
                            target: "bridge::audit",
                            topic = %msg.topic,
                            reason = "spool-full",
                            "refused a message at the spool cap; the source is not acknowledged"
                        );
                        return Err(SpoolError::Full);
                    }
                    if let Some(shed) = q.pop_front() {
                        info!(
                            target: "bridge::audit",
                            topic = %shed.topic,
                            reason = "spool-full",
                            "dropped a spooled message at the cap"
                        );
                    }
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
                q.push_back(msg.clone());
                Ok(())
            }
            Inner::Disk { db, next } => {
                // Refuse before writing anything: everything already spooled was acknowledged
                // to the source, so shedding it would lose a message the source believes was
                // delivered (ADR 0060 T2/T5).
                if self.overflow == Overflow::Refuse {
                    let rtx = db.begin_read().map_err(backend)?;
                    let len = match rtx.open_table(SPOOL) {
                        Ok(t) => usize::try_from(t.len().map_err(backend)?).unwrap_or(usize::MAX),
                        Err(redb::TableError::TableDoesNotExist(_)) => 0,
                        Err(e) => return Err(backend(e)),
                    };
                    drop(rtx);
                    if len >= self.cap {
                        info!(
                            target: "bridge::audit",
                            topic = %msg.topic,
                            reason = "spool-full",
                            "refused a message at the spool cap; the source is not acknowledged"
                        );
                        return Err(SpoolError::Full);
                    }
                }
                let mut wtx = db.begin_write().map_err(backend)?;
                // Explicit fsync-on-commit (ADR 0060 T3): a QoS≥1 source ack is gated on this
                // returning, so durability must not be left to a redb default. Mirrors the
                // broker's own durable stores (ADR 0018).
                wtx.set_durability(Durability::Immediate);
                {
                    let mut t = wtx.open_table(SPOOL).map_err(backend)?;
                    let encoded = encode(msg);
                    t.insert(*next, encoded.as_slice()).map_err(backend)?;
                    *next += 1;
                    // Enforce the cap: drop oldest keys until within `cap`.
                    while usize::try_from(t.len().map_err(backend)?).unwrap_or(usize::MAX)
                        > self.cap
                    {
                        let oldest = t.first().map_err(backend)?.map(|(k, v)| {
                            // Audit the loss (ADR 0060 T5): a spool-full drop is a lost
                            // auditable-crossing message, so record WHAT was shed, not just a
                            // counter. The dropped message was already forwarded-or-acked.
                            if let Some(m) = decode(v.value()) {
                                info!(
                                    target: "bridge::audit",
                                    topic = %m.topic,
                                    reason = "spool-full",
                                    "dropped a spooled message at the cap"
                                );
                            }
                            k.value()
                        });
                        if let Some(k) = oldest {
                            t.remove(k).map_err(backend)?;
                            self.dropped.fetch_add(1, Ordering::Relaxed);
                        } else {
                            break;
                        }
                    }
                }
                wtx.commit().map_err(backend)?;
                Ok(())
            }
        }
    }

    /// Remove and return every spooled message, oldest first (a full replay).
    ///
    /// # Errors
    /// [`SpoolError::Backend`] on a disk failure.
    pub fn drain(&self) -> Result<Vec<SpooledMessage>, SpoolError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *inner {
            Inner::Mem(q) => Ok(q.drain(..).collect()),
            Inner::Disk { db, .. } => {
                let mut out = Vec::new();
                let mut wtx = db.begin_write().map_err(backend)?;
                wtx.set_durability(Durability::Immediate); // fsync the removal (ADR 0060 T3)
                {
                    let mut t = wtx.open_table(SPOOL).map_err(backend)?;
                    let keys: Vec<u64> = t
                        .iter()
                        .map_err(backend)?
                        .filter_map(Result::ok)
                        .map(|(k, v)| {
                            let m = decode(v.value());
                            (k.value(), m)
                        })
                        .filter_map(|(k, m)| m.map(|m| (k, m)))
                        .map(|(k, m)| {
                            out.push(m);
                            k
                        })
                        .collect();
                    for k in keys {
                        t.remove(k).map_err(backend)?;
                    }
                }
                wtx.commit().map_err(backend)?;
                Ok(out)
            }
        }
    }

    /// The number of spooled messages.
    ///
    /// # Errors
    /// [`SpoolError::Backend`] on a disk failure.
    pub fn len(&self) -> Result<usize, SpoolError> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*inner {
            Inner::Mem(q) => Ok(q.len()),
            Inner::Disk { db, .. } => {
                let tx = db.begin_read().map_err(backend)?;
                match tx.open_table(SPOOL) {
                    Ok(t) => Ok(usize::try_from(t.len().map_err(backend)?).unwrap_or(usize::MAX)),
                    Err(redb::TableError::TableDoesNotExist(_)) => Ok(0),
                    Err(e) => Err(backend(e)),
                }
            }
        }
    }

    /// Whether the spool is empty.
    ///
    /// # Errors
    /// [`SpoolError::Backend`] on a disk failure.
    /// How many messages this spool has discarded to stay within `cap`.
    ///
    /// Non-zero means store-and-forward is shedding: the side has been down long
    /// enough, or busy enough, that the bound was reached and the oldest queued
    /// messages were thrown away. Exported so that is visible rather than silent.
    #[must_use]
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The configured bound, so a depth reading can be read as a fraction of it.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Whether the spool holds nothing.
    ///
    /// # Errors
    /// [`SpoolError::Backend`] on a disk failure.
    pub fn is_empty(&self) -> Result<bool, SpoolError> {
        Ok(self.len()? == 0)
    }
}

// --- a small length-prefixed codec for a spooled message ---------------------------------

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    let len = u32::try_from(b.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&b[..len as usize]);
}

fn encode(m: &SpooledMessage) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, m.topic.as_bytes());
    put_bytes(&mut out, &m.payload);
    out.push(m.qos);
    out.push(u8::from(m.retain));
    let n = u32::try_from(m.user_properties.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&n.to_be_bytes());
    for (k, v) in m.user_properties.iter().take(n as usize) {
        put_bytes(&mut out, k.as_bytes());
        put_bytes(&mut out, v.as_bytes());
    }
    out
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn u32(&mut self) -> Option<u32> {
        let end = self.pos.checked_add(4)?;
        let v = u32::from_be_bytes(self.buf.get(self.pos..end)?.try_into().ok()?);
        self.pos = end;
        Some(v)
    }
    fn u8(&mut self) -> Option<u8> {
        let v = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(v)
    }
    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.u32()? as usize;
        let end = self.pos.checked_add(len)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }
    fn string(&mut self) -> Option<String> {
        Some(String::from_utf8_lossy(self.bytes()?).into_owned())
    }
}

fn decode(buf: &[u8]) -> Option<SpooledMessage> {
    let mut r = Reader { buf, pos: 0 };
    let topic = r.string()?;
    let payload = r.bytes()?.to_vec();
    let qos = r.u8()?;
    let retain = r.u8()? != 0;
    let n = r.u32()?;
    let mut user_properties = Vec::new();
    for _ in 0..n {
        let k = r.string()?;
        let v = r.string()?;
        user_properties.push((k, v));
    }
    Some(SpooledMessage {
        topic,
        payload,
        qos,
        retain,
        user_properties,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(topic: &str, payload: &[u8]) -> SpooledMessage {
        SpooledMessage {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos: 1,
            retain: false,
            user_properties: vec![("fss-bridge-hop-count".into(), "1".into())],
        }
    }

    #[test]
    fn encode_decode_round_trips_the_retain_flag() {
        // #189: a spooled forward must preserve the retain bit across encode/decode so a
        // replay after reconnect still lands retained.
        for retain in [false, true] {
            let m = SpooledMessage {
                topic: "t/x".to_string(),
                payload: vec![1, 2, 3],
                qos: 1,
                retain,
                user_properties: vec![("k".into(), "v".into())],
            };
            let round = decode(&encode(&m)).expect("decodes");
            assert_eq!(round, m);
            assert_eq!(round.retain, retain);
        }
    }

    #[test]
    fn the_refuse_policy_keeps_what_it_accepted_and_rejects_the_newcomer() {
        // ADR 0060 T2/T5: everything spooled was already acked to the source, so at the cap the
        // NEW message is refused (and left unacked, so the source redelivers) rather than the
        // old one being shed. Refusals are not counted as drops — nothing was lost.
        for s in [
            Spool::in_memory(2),
            Spool::on_disk(&tempfile::tempdir().unwrap().path().join("r.redb"), 2).unwrap(),
        ] {
            s.push(&msg("t", b"a")).unwrap();
            s.push(&msg("t", b"b")).unwrap();
            let err = s.push(&msg("t", b"c")).unwrap_err();
            assert!(
                matches!(err, SpoolError::Full),
                "expected Full, got {err:?}"
            );
            assert_eq!(s.dropped_count(), 0, "a refusal is not a drop");
            let payloads: Vec<Vec<u8>> = s
                .drain()
                .unwrap()
                .iter()
                .map(|m| m.payload.clone())
                .collect();
            assert_eq!(
                payloads,
                vec![b"a".to_vec(), b"b".to_vec()],
                "the accepted messages must survive the refusal"
            );
        }
    }

    #[test]
    fn in_memory_spool_is_bounded_drop_oldest_and_replays_in_order() {
        let s = Spool::in_memory(3).with_overflow(Overflow::DropOldest);
        for i in 0..5 {
            s.push(&msg("t", format!("m{i}").as_bytes())).unwrap();
        }
        // Cap 3 → only the last three survive, oldest dropped.
        let drained = s.drain().unwrap();
        let payloads: Vec<Vec<u8>> = drained.iter().map(|m| m.payload.clone()).collect();
        assert_eq!(
            payloads,
            vec![b"m2".to_vec(), b"m3".to_vec(), b"m4".to_vec()]
        );
        assert!(s.is_empty().unwrap());
    }

    #[test]
    fn the_codec_round_trips_a_message_with_user_properties() {
        let m = msg("a/b", b"hello");
        assert_eq!(decode(&encode(&m)), Some(m));
    }

    #[test]
    fn a_disk_spool_survives_a_reopen_and_replays() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.redb");
        {
            let s = Spool::on_disk(&path, 10).unwrap();
            s.push(&msg("t", b"a")).unwrap();
            s.push(&msg("t", b"b")).unwrap();
            assert_eq!(s.len().unwrap(), 2);
        }
        // Reopen the same file: the messages are still there (disk-backed, §7).
        let s = Spool::on_disk(&path, 10).unwrap();
        let drained = s.drain().unwrap();
        let payloads: Vec<Vec<u8>> = drained.iter().map(|m| m.payload.clone()).collect();
        assert_eq!(payloads, vec![b"a".to_vec(), b"b".to_vec()]);
        // A push after reopen continues past the restored keys (no overwrite).
        s.push(&msg("t", b"c")).unwrap();
        assert_eq!(s.len().unwrap(), 1);
    }

    #[test]
    fn a_disk_spool_enforces_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let s = Spool::on_disk(&dir.path().join("s.redb"), 2)
            .unwrap()
            .with_overflow(Overflow::DropOldest);
        for i in 0..5 {
            s.push(&msg("t", format!("m{i}").as_bytes())).unwrap();
        }
        assert_eq!(s.len().unwrap(), 2);
        let drained = s.drain().unwrap();
        let payloads: Vec<Vec<u8>> = drained.iter().map(|m| m.payload.clone()).collect();
        assert_eq!(payloads, vec![b"m3".to_vec(), b"m4".to_vec()]);
    }
}
