//! Bounded store-and-forward spool
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §7, T7;
//! [ADR 0041](../../../docs/adr/0041-resource-governance.md) T7).
//!
//! When a side is momentarily unreachable, messages destined for it are held here and
//! replayed on reconnect. The spool is **bounded** (never grows without limit, like the
//! broker's offline queues, ADR 0001 §6) and, when a directory is configured, **disk-backed**
//! (`redb`), so a brief bridge restart does not lose them. Two bounds join: a message
//! **count** (`max_messages`) and an accounted-**byte** budget (`max_bytes`; unset / 0 =
//! off). At either cap the [`Overflow`] policy decides: **refuse** the new message (the
//! default, and what a `QoS`≥1 rule uses — everything already spooled was acknowledged
//! to the source, so shedding it would lose a message the source believes was delivered)
//! or **drop the oldest** (`QoS` 0, which promises nothing). A message larger than the
//! entire byte budget can never fit: it is dropped or refused on its own, counted, and
//! the spool is left intact — emptying the spool to make room would lose every already-
//! accepted crossing for a message that still would not fit. Delivery is at-least-once
//! for `QoS` ≥ 1 (§7); a replayed message keeps its topic, payload, `QoS`, and User
//! Properties (incl. the hop count) intact.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use redb::{Database, Durability, ReadableTable, ReadableTableMetadata, TableDefinition};
use tracing::{info, warn};

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

/// Per-entry envelope charged on top of a message's variable-length bytes.
///
/// Mirrors `crates/mqttd/src/backpressure.rs` `ENTRY_OVERHEAD` / `message_bytes` —
/// the broker's RAM definition, cited rather than imported so this crate does not
/// take a production dependency on `mqttd`. The spool persists only topic, payload
/// and user properties (see [`encode`]/[`decode`]), so [`message_bytes`] counts
/// those three plus this envelope, which is the same formula as
/// `ENTRY_OVERHEAD + topic + payload + AppProperties::accounted_bytes()` for a
/// property block that holds only user properties.
pub const ENTRY_OVERHEAD: usize = 256;

const _: () = assert!(std::mem::size_of::<SpooledMessage>() <= ENTRY_OVERHEAD);

/// Accounted size of a spooled message. See [`ENTRY_OVERHEAD`].
#[must_use]
pub fn message_bytes(m: &SpooledMessage) -> usize {
    ENTRY_OVERHEAD
        + m.topic.len()
        + m.payload.len()
        + m.user_properties
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
}

/// Which bound a push hit — a log field, never a metric label (the counter stays
/// `fss_bridge_dropped_total{reason="spool-full"}` with its existing label set).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundHit {
    Messages,
    Bytes,
    /// Both the count and the byte budget are already at or over.
    Both,
}

impl BoundHit {
    fn as_str(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::Bytes => "bytes",
            Self::Both => "messages+bytes",
        }
    }
}

const SPOOL: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("spool");

/// `spool*.redb`'s on-disk layout version (ADR 0038 T2 / ADR 0058). Version 1 is the
/// layout the v1.0.0 tag shipped: the `spool` table of hand-encoded records
/// ([`encode`]/[`decode`] below). Gated on open like the broker's four stores — a spool written by a
/// newer layout refuses to open instead of being misread. Files from before the gate
/// existed carry no stamp and are adopted as version 1 (their layout IS version 1);
/// `gate_or_migrate` stamps them on first open.
pub const SPOOL_SCHEMA_VERSION: u32 = 1;

/// In-place migrations for `spool*.redb` (ADR 0058). Empty by design at 1.0 — the
/// first post-1.0 schema bump lands its `MigrationStep` here in the same PR, or the
/// coverage test fails.
const SPOOL_MIGRATIONS: &[mqtt_storage::schema::MigrationStep] = &[];

/// Oldest `spool*.redb` version the contract migrates from (ADR 0058). Pinned to the
/// layout the v1.0.0 tag shipped — a literal, not [`SPOOL_SCHEMA_VERSION`], so a
/// version raise without its `MigrationStep` fails the coverage test rather than
/// moving the floor with the ceiling. Raised only when a release retires migrations
/// (ADR 0039).
#[cfg(test)]
const SPOOL_MIGRATE_FLOOR: u32 = 1;

/// A bounded FIFO spool, in memory or disk-backed.
#[derive(Debug)]
pub struct Spool {
    cap: usize,
    /// Accounted-byte budget. `None` = unbounded (the count bound is the only
    /// active bound). Runtime state — not persisted; do not bump
    /// [`SPOOL_SCHEMA_VERSION`] for this.
    max_bytes: Option<usize>,
    /// Side name used in overflow audit lines (empty in unit tests).
    label: String,
    inner: Mutex<Inner>,
    /// Messages discarded because the spool was already at a bound under
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
    Mem {
        q: VecDeque<SpooledMessage>,
        bytes: usize,
    },
    Disk {
        db: Database,
        next: u64,
        bytes: usize,
    },
}

impl Inner {
    fn len_and_bytes(&self) -> Result<(usize, usize), SpoolError> {
        match self {
            Self::Mem { q, bytes } => Ok((q.len(), *bytes)),
            Self::Disk { db, bytes, .. } => {
                let tx = db.begin_read().map_err(backend)?;
                let len = match tx.open_table(SPOOL) {
                    Ok(t) => usize::try_from(t.len().map_err(backend)?).unwrap_or(usize::MAX),
                    Err(redb::TableError::TableDoesNotExist(_)) => 0,
                    Err(e) => return Err(backend(e)),
                };
                Ok((len, *bytes))
            }
        }
    }
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
    /// An in-memory spool bounded to `cap` messages. The byte budget is off
    /// until [`Self::with_max_bytes`] — today's behaviour.
    #[must_use]
    pub fn in_memory(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            max_bytes: None,
            label: String::new(),
            inner: Mutex::new(Inner::Mem {
                q: VecDeque::new(),
                bytes: 0,
            }),
            dropped: AtomicU64::new(0),
            overflow: Overflow::default(),
        }
    }

    /// A disk-backed spool at `path`, bounded to `cap` messages. Reopens an existing file,
    /// so messages spooled before a restart are still there to replay. The running byte
    /// total is reconstructed from the v1 records on open — runtime state, not an
    /// on-disk layout change ([`SPOOL_SCHEMA_VERSION`] stays 1).
    ///
    /// # Errors
    /// [`SpoolError::Backend`] if the database cannot be opened.
    pub fn on_disk(path: &Path, cap: usize) -> Result<Self, SpoolError> {
        let db = mqtt_storage::open::create_with_lock_retry(path).map_err(backend)?;
        // The ADR 0058 schema gate: refuse a spool stamped by a newer layout instead
        // of misreading it; stamp fresh (and pre-gate) files as the current version.
        mqtt_storage::schema::gate_or_migrate(&db, "spool", SPOOL_SCHEMA_VERSION, SPOOL_MIGRATIONS)
            .map_err(backend)?;
        // Find the highest existing key so new pushes continue past it, and sum
        // accounted bytes so the running total starts exact.
        let (next, bytes) = {
            let tx = db.begin_read().map_err(backend)?;
            match tx.open_table(SPOOL) {
                Ok(t) => {
                    let next = t.last().map_err(backend)?.map_or(0, |(k, _)| k.value() + 1);
                    let mut bytes = 0usize;
                    for entry in t.iter().map_err(backend)? {
                        let (_, v) = entry.map_err(backend)?;
                        match decode(v.value()) {
                            // The accounted-byte invariant (see `drain`): only
                            // decodable residents contribute. An undecodable
                            // record still occupies a count slot until it is
                            // drained or evicted — surface it, do not hide it.
                            Some(m) => bytes += message_bytes(&m),
                            None => warn!(
                                "spool reopen found an UNDECODABLE record \
                                 (corruption or a foreign layout): it consumes a \
                                 count slot and no accounted bytes, and a drain \
                                 or eviction will remove it"
                            ),
                        }
                    }
                    (next, bytes)
                }
                Err(redb::TableError::TableDoesNotExist(_)) => (0, 0),
                Err(e) => return Err(backend(e)),
            }
        };
        Ok(Self {
            cap: cap.max(1),
            max_bytes: None,
            label: String::new(),
            overflow: Overflow::default(),
            inner: Mutex::new(Inner::Disk { db, next, bytes }),
            dropped: AtomicU64::new(0),
        })
    }

    /// Use `policy` at the cap instead of the default [`Overflow::Refuse`].
    #[must_use]
    pub fn with_overflow(mut self, policy: Overflow) -> Self {
        self.overflow = policy;
        self
    }

    /// Set the accounted-byte budget. `0` leaves the byte bound off.
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = if max_bytes == 0 {
            None
        } else {
            Some(usize::try_from(max_bytes).unwrap_or(usize::MAX))
        };
        self
    }

    /// Name this spool in overflow audit lines (the side it buffers for).
    #[must_use]
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }

    /// Append `msg`. At either bound the [`Overflow`] policy decides: refuse the new
    /// message, or drop the oldest until the newcomer fits **both** bounds.
    ///
    /// A message whose accounted size is larger than the entire byte budget can
    /// never fit. Do **not** empty the spool trying: drop ([`Overflow::DropOldest`], counted) or
    /// refuse ([`Overflow::Refuse`], not counted — nothing already accepted is lost) that one
    /// message and leave the spool intact.
    ///
    /// # Errors
    /// [`SpoolError::Full`] when at a bound under [`Overflow::Refuse`] — the message was **not**
    /// accepted, so the caller must not acknowledge it (ADR 0060 T2).
    /// [`SpoolError::Backend`] on a disk failure.
    pub fn push(&self, msg: &SpooledMessage) -> Result<(), SpoolError> {
        let incoming = message_bytes(msg);
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (len, bytes) = inner.len_and_bytes()?;

        // Oversized vs the whole budget: never evict standing state for a message
        // that still would not fit after the spool was emptied.
        if self.max_bytes.is_some_and(|cap| incoming > cap) {
            self.log_overflow(msg, incoming, len, bytes, BoundHit::Bytes, true);
            if self.overflow == Overflow::Refuse {
                return Err(SpoolError::Full);
            }
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        let over_count = len >= self.cap;
        let over_bytes = self
            .max_bytes
            .is_some_and(|cap| bytes.saturating_add(incoming) > cap);
        if over_count || over_bytes {
            let bound = match (over_count, over_bytes) {
                (true, true) => BoundHit::Both,
                (true, false) => BoundHit::Messages,
                (false, true) => BoundHit::Bytes,
                (false, false) => unreachable!("over_count || over_bytes"),
            };
            if self.overflow == Overflow::Refuse {
                self.log_overflow(msg, incoming, len, bytes, bound, false);
                return Err(SpoolError::Full);
            }
            match &mut *inner {
                Inner::Mem { q, bytes } => {
                    while q.len() >= self.cap
                        || self
                            .max_bytes
                            .is_some_and(|cap| bytes.saturating_add(incoming) > cap)
                    {
                        let Some(shed) = q.pop_front() else {
                            break;
                        };
                        let shed_bytes = message_bytes(&shed);
                        *bytes = bytes.saturating_sub(shed_bytes);
                        let evicted_bound = if q.len() + 1 >= self.cap {
                            BoundHit::Messages
                        } else {
                            BoundHit::Bytes
                        };
                        self.log_drop(&shed, shed_bytes, q.len(), *bytes, evicted_bound);
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                    q.push_back(msg.clone());
                    *bytes = bytes.saturating_add(incoming);
                    Ok(())
                }
                Inner::Disk {
                    db,
                    next,
                    bytes: spool_bytes,
                } => self.push_disk_drop_oldest(db, next, spool_bytes, msg, incoming),
            }
        } else {
            match &mut *inner {
                Inner::Mem { q, bytes } => {
                    q.push_back(msg.clone());
                    *bytes = bytes.saturating_add(incoming);
                    Ok(())
                }
                Inner::Disk {
                    db,
                    next,
                    bytes: spool_bytes,
                } => {
                    let mut wtx = db.begin_write().map_err(backend)?;
                    // Explicit fsync-on-commit (ADR 0060 T3): a QoS≥1 source ack is gated
                    // on this returning, so durability must not be left to a redb default.
                    wtx.set_durability(Durability::Immediate);
                    {
                        let mut t = wtx.open_table(SPOOL).map_err(backend)?;
                        let encoded = encode(msg);
                        t.insert(*next, encoded.as_slice()).map_err(backend)?;
                        *next += 1;
                    }
                    wtx.commit().map_err(backend)?;
                    *spool_bytes = spool_bytes.saturating_add(incoming);
                    Ok(())
                }
            }
        }
    }

    fn push_disk_drop_oldest(
        &self,
        db: &Database,
        next: &mut u64,
        spool_bytes: &mut usize,
        msg: &SpooledMessage,
        incoming: usize,
    ) -> Result<(), SpoolError> {
        let mut wtx = db.begin_write().map_err(backend)?;
        wtx.set_durability(Durability::Immediate);
        {
            let mut t = wtx.open_table(SPOOL).map_err(backend)?;
            // Evict oldest until the newcomer fits both bounds, then append.
            loop {
                let len = usize::try_from(t.len().map_err(backend)?).unwrap_or(usize::MAX);
                let need_count = len >= self.cap;
                let need_bytes = self
                    .max_bytes
                    .is_some_and(|cap| spool_bytes.saturating_add(incoming) > cap);
                if !need_count && !need_bytes {
                    break;
                }
                let bound = if need_count {
                    BoundHit::Messages
                } else {
                    BoundHit::Bytes
                };
                let Some((key, decoded)) = t
                    .first()
                    .map_err(backend)?
                    .map(|(k, v)| (k.value(), decode(v.value())))
                else {
                    break;
                };
                if let Some(m) = decoded {
                    let shed_bytes = message_bytes(&m);
                    *spool_bytes = spool_bytes.saturating_sub(shed_bytes);
                    self.log_drop(&m, shed_bytes, len.saturating_sub(1), *spool_bytes, bound);
                } else {
                    // Corruption, not an accounted record: the total never
                    // included it (see the invariant on `drain`), so removing it
                    // correctly changes no bytes — but the operator must see it.
                    warn!(
                        key,
                        "spool evicted an UNDECODABLE record (corruption or a \
                         foreign layout); it consumed a count slot and no \
                         accounted bytes"
                    );
                }
                t.remove(key).map_err(backend)?;
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            let encoded = encode(msg);
            t.insert(*next, encoded.as_slice()).map_err(backend)?;
            *next += 1;
        }
        wtx.commit().map_err(backend)?;
        *spool_bytes = spool_bytes.saturating_add(incoming);
        Ok(())
    }

    fn log_overflow(
        &self,
        msg: &SpooledMessage,
        incoming: usize,
        len: usize,
        bytes: usize,
        bound: BoundHit,
        oversized: bool,
    ) {
        let max_bytes = self.max_bytes.unwrap_or(0);
        if self.overflow == Overflow::Refuse {
            info!(
                target: "bridge::audit",
                side = %self.label,
                bound = bound.as_str(),
                incoming_bytes = incoming,
                spool_bytes = bytes,
                max_bytes,
                spool_len = len,
                max_messages = self.cap,
                topic = %msg.topic,
                reason = "spool-full",
                oversized,
                "refused a message at the spool bound; the source is not acknowledged"
            );
        } else {
            info!(
                target: "bridge::audit",
                side = %self.label,
                bound = bound.as_str(),
                incoming_bytes = incoming,
                spool_bytes = bytes,
                max_bytes,
                spool_len = len,
                max_messages = self.cap,
                topic = %msg.topic,
                reason = "spool-full",
                oversized,
                "dropped a message larger than the spool byte budget; the spool is unchanged"
            );
        }
    }

    fn log_drop(
        &self,
        shed: &SpooledMessage,
        shed_bytes: usize,
        spool_len: usize,
        spool_bytes: usize,
        bound: BoundHit,
    ) {
        info!(
            target: "bridge::audit",
            side = %self.label,
            bound = bound.as_str(),
            incoming_bytes = shed_bytes,
            spool_bytes,
            max_bytes = self.max_bytes.unwrap_or(0),
            spool_len,
            max_messages = self.cap,
            topic = %shed.topic,
            reason = "spool-full",
            "dropped a spooled message at the bound"
        );
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
            Inner::Mem { q, bytes } => {
                *bytes = 0;
                Ok(q.drain(..).collect())
            }
            Inner::Disk { db, bytes, .. } => {
                let mut out = Vec::new();
                let mut undecodable = 0usize;
                let mut wtx = db.begin_write().map_err(backend)?;
                wtx.set_durability(Durability::Immediate); // fsync the removal (ADR 0060 T3)
                {
                    let mut t = wtx.open_table(SPOOL).map_err(backend)?;
                    // EVERY key is removed — an undecodable record must not linger:
                    // before this fix it stayed in the table forever (occupying a
                    // count slot, never replayed) while the byte total was zeroed,
                    // so the spool could grow past `max_bytes` without limit. The
                    // accounted-byte invariant is "bytes() = Σ accounted bytes of
                    // DECODABLE residents": a corrupt record contributes no
                    // accounted bytes anywhere (open, evict, drain), so zeroing
                    // the total over an emptied table stays exact.
                    let keys: Vec<u64> = t
                        .iter()
                        .map_err(backend)?
                        .filter_map(Result::ok)
                        .map(|(k, v)| {
                            match decode(v.value()) {
                                Some(m) => out.push(m),
                                None => undecodable += 1,
                            }
                            k.value()
                        })
                        .collect();
                    for k in keys {
                        t.remove(k).map_err(backend)?;
                    }
                }
                wtx.commit().map_err(backend)?;
                if undecodable > 0 {
                    warn!(
                        records = undecodable,
                        "spool drain removed UNDECODABLE records (corruption or a \
                         foreign layout): they were never replayable and consumed \
                         count slots; the byte total never accounted them"
                    );
                }
                *bytes = 0;
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
            Inner::Mem { q, .. } => Ok(q.len()),
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

    /// The configured count bound, so a depth reading can be read as a fraction of it.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Accounted bytes currently held (the running total).
    ///
    /// # Errors
    /// [`SpoolError::Backend`] on a disk failure.
    pub fn bytes(&self) -> Result<usize, SpoolError> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(inner.len_and_bytes()?.1)
    }

    /// The total recomputed from the resident entries — the independent witness
    /// the exactness test compares [`bytes`](Self::bytes) against. Same invariant
    /// as the running total (see `drain`): Σ accounted bytes of DECODABLE
    /// residents; undecodable records contribute to neither side, so the two
    /// agree exactly when the invariant holds.
    #[cfg(test)]
    fn recomputed_bytes(&self) -> Result<usize, SpoolError> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*inner {
            Inner::Mem { q, .. } => Ok(q.iter().map(message_bytes).sum()),
            Inner::Disk { db, .. } => {
                let tx = db.begin_read().map_err(backend)?;
                match tx.open_table(SPOOL) {
                    Ok(t) => {
                        let mut sum = 0usize;
                        for entry in t.iter().map_err(backend)? {
                            let (_, v) = entry.map_err(backend)?;
                            if let Some(m) = decode(v.value()) {
                                sum += message_bytes(&m);
                            }
                        }
                        Ok(sum)
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => Ok(0),
                    Err(e) => Err(backend(e)),
                }
            }
        }
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

    /// ADR 0058 T2: the spool migration registry must cover the contract range, so a
    /// future schema bump without its migration fails here, not at an operator's upgrade.
    #[test]
    fn the_migration_registry_covers_the_contract_range() {
        mqtt_storage::schema::assert_migrations_cover(
            SPOOL_MIGRATE_FLOOR,
            SPOOL_SCHEMA_VERSION,
            SPOOL_MIGRATIONS,
        )
        .expect("spool migration registry has a gap");
    }

    /// ADR 0038 T2: a spool stamped by a foreign (newer) layout version refuses to
    /// open, naming both versions — never silently misreading bytes.
    #[test]
    fn a_foreign_schema_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.redb");
        drop(Spool::on_disk(&path, 4).unwrap()); // stamped current
        {
            let db = redb::Database::create(&path).unwrap();
            mqtt_storage::schema::force_version(&db, 999).unwrap();
        }
        let err = Spool::on_disk(&path, 4).unwrap_err().to_string();
        assert!(err.contains("v999") && err.contains("expects v1"), "{err}");
    }

    /// The adoption path the gate promises: a spool written before the gate existed
    /// (no schema stamp) opens, keeps its messages, and is stamped as version 1.
    #[test]
    fn an_unstamped_pre_gate_spool_is_adopted_with_its_messages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.redb");
        {
            // Write a message the pre-gate way: raw table, no stamp.
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            {
                let mut t = tx.open_table(SPOOL).unwrap();
                t.insert(0u64, encode(&msg("t/held", b"kept")).as_slice())
                    .unwrap();
            }
            tx.commit().unwrap();
        }
        let spool = Spool::on_disk(&path, 4).unwrap();
        assert_eq!(spool.drain().unwrap().len(), 1, "pre-gate message survives");
    }

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

    /// A message whose accounted size is exactly `want` bytes (no user properties).
    fn sized(topic: &str, want: usize) -> SpooledMessage {
        let fixed = ENTRY_OVERHEAD + topic.len();
        assert!(
            want >= fixed,
            "asked for a message smaller than its envelope ({want} < {fixed})"
        );
        SpooledMessage {
            topic: topic.to_string(),
            payload: vec![0u8; want - fixed],
            qos: 0,
            retain: false,
            user_properties: vec![],
        }
    }

    fn check_bytes(s: &Spool, step: &str) {
        assert_eq!(
            s.bytes().unwrap(),
            s.recomputed_bytes().unwrap(),
            "counter drifted after {step}"
        );
    }

    /// The corruption contract the byte bound made visible (review round on #605):
    /// a record that does not decode is a count-slot occupant with NO accounted
    /// bytes, at every site that touches the table:
    /// * reopen sums only decodable residents, and the running total still equals
    ///   the recomputed witness;
    /// * drain removes EVERY key — before the fix an undecodable record stayed in
    ///   the table forever (never replayed, never removed) while the total was
    ///   zeroed, so the spool could grow past `max_bytes` without limit;
    /// * eviction terminates past an undecodable OLDEST and leaves the spool
    ///   consistent.
    #[test]
    fn an_undecodable_record_is_drained_evicted_and_never_accounted() {
        fn inject_garbage(spool: &Spool) {
            let Inner::Disk { db, .. } = &*spool.inner.lock().unwrap() else {
                panic!("this test drives the disk-backed variant");
            };
            let mut wtx = db.begin_write().unwrap();
            {
                let mut t = wtx.open_table(SPOOL).unwrap();
                t.insert(9_999, &[0xffu8; 8][..]).unwrap(); // undecodable: u32 length overruns
            }
            wtx.commit().unwrap();
        }

        // --- reopen: the corrupt record is seen, warned, and not accounted ---
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        {
            let s = Spool::on_disk(&path, 8).unwrap();
            s.push(&sample("kept")).unwrap();
        }
        inject_garbage_db(&path); // a record the CURRENT decode cannot read
        {
            let s = Spool::on_disk(&path, 8).unwrap();
            assert_eq!(
                s.len().unwrap(),
                2,
                "the corrupt record occupies a count slot"
            );
            assert_eq!(
                s.bytes().unwrap(),
                s.recomputed_bytes().unwrap(),
                "the running total and the witness must agree over a corrupt resident"
            );
            assert_eq!(
                s.bytes().unwrap(),
                message_bytes(&sample("kept")),
                "only the decodable resident is accounted"
            );

            // --- drain removes EVERY key, including the corrupt one ---
            let drained = s.drain().unwrap();
            assert_eq!(drained.len(), 1, "only the decodable record is replayed");
            assert_eq!(s.len().unwrap(), 0, "the corrupt record must not linger");
            assert_eq!(s.bytes().unwrap(), 0);
            assert_eq!(s.bytes().unwrap(), s.recomputed_bytes().unwrap());
        }
        {
            let s = Spool::on_disk(&path, 8).unwrap();
            assert_eq!(
                s.len().unwrap(),
                0,
                "drain emptied the table across a reopen"
            );
        }

        // --- eviction terminates past an undecodable OLDEST ---
        let dir2 = tempfile::tempdir().unwrap();
        let path2 = dir2.path().join("e.redb");
        {
            let s = Spool::on_disk(&path2, 8).unwrap();
            s.push(&sample("kept")).unwrap();
        }
        inject_garbage_db(&path2);
        // One resident (326 accounted) + a budget that fits only ONE more record:
        // every push under DropOldest MUST evict, and the corrupt record sits at
        // the OLDEST key — the evictor must get past it (it frees no accounted
        // bytes) without looping forever.
        let budget = (message_bytes(&sample("kept")) + 100) as u64;
        {
            let s = Spool::on_disk(&path2, 8)
                .unwrap()
                .with_max_bytes(budget)
                .with_overflow(Overflow::DropOldest);
            for i in 0..3 {
                s.push(&sample(&format!("after-{i}"))).unwrap();
            }
            assert_eq!(s.bytes().unwrap(), s.recomputed_bytes().unwrap());
            assert!(
                s.len().unwrap() <= 2,
                "the byte budget must still bound the spool past a corrupt resident"
            );
        }
        {
            // The corrupt record was evicted too: a fresh reopen sees one
            // decodable resident and nothing undecodable left to warn about.
            let s = Spool::on_disk(&path2, 8).unwrap();
            assert_eq!(s.len().unwrap(), 1);
            assert_eq!(s.bytes().unwrap(), s.recomputed_bytes().unwrap());
            assert_eq!(s.drain().unwrap().len(), 1);
            assert_eq!(s.len().unwrap(), 0);
        }
    }

    /// A valid record and, at a HIGHER key, a raw record the current decode cannot
    /// read — written straight into the table so no encode step can make it valid.
    fn inject_garbage_db(path: &std::path::Path) {
        let db = Database::open(path).unwrap();
        let mut wtx = db.begin_write().unwrap();
        {
            let mut t = wtx.open_table(SPOOL).unwrap();
            let last = t.last().unwrap().map(|(k, _)| k.value()).unwrap_or(0);
            t.insert(last + 1, &[0xffu8; 8][..]).unwrap(); // u32 length 0xffffffff overruns
        }
        wtx.commit().unwrap();
    }

    fn sample(id: &str) -> SpooledMessage {
        SpooledMessage {
            topic: format!("t/{id}"),
            payload: vec![b'x'; 64],
            qos: 1,
            retain: false,
            user_properties: Vec::new(),
        }
    }

    fn for_each_backend(cap: usize, max_bytes: u64, overflow: Overflow, f: impl Fn(&Spool)) {
        let mem = Spool::in_memory(cap)
            .with_max_bytes(max_bytes)
            .with_overflow(overflow)
            .with_label("mem");
        f(&mem);
        let dir = tempfile::tempdir().unwrap();
        let disk = Spool::on_disk(&dir.path().join("s.redb"), cap)
            .unwrap()
            .with_max_bytes(max_bytes)
            .with_overflow(overflow)
            .with_label("disk");
        f(&disk);
    }

    #[test]
    fn message_bytes_counts_topic_payload_and_user_properties() {
        // Mirrors mqttd::backpressure::message_bytes: envelope + topic + payload +
        // AppProperties::accounted_bytes (user-property keys and values).
        let m = SpooledMessage {
            topic: "a/b/c".to_string(), // 5
            payload: vec![7u8; 100],
            qos: 1,
            retain: false,
            user_properties: vec![
                ("k1".to_string(), "value-one".to_string()), // 2 + 9
                ("k2".to_string(), "v2".to_string()),        // 2 + 2
            ],
        };
        let props = (2 + 9) + (2 + 2);
        assert_eq!(message_bytes(&m), ENTRY_OVERHEAD + 5 + 100 + props);
        assert_ne!(
            message_bytes(&m),
            ENTRY_OVERHEAD + 5 + 100,
            "property bytes must be counted"
        );
    }

    #[test]
    fn the_byte_bound_triggers_independently_of_the_count_bound() {
        // 8 × 1 KiB fits an 8 KiB budget; the count cap is nowhere near.
        for_each_backend(10_000, 8 * 1024, Overflow::DropOldest, |s| {
            let kib = 1024;
            for i in 0..40 {
                s.push(&sized(&format!("t{i}"), kib)).unwrap();
                check_bytes(s, "byte-bound push");
            }
            assert_eq!(s.len().unwrap(), 8, "the spool rests at the byte bound");
            assert_eq!(s.bytes().unwrap(), 8 * kib);
            assert_eq!(s.dropped_count(), 32);
            let drained = s.drain().unwrap();
            assert_eq!(drained.first().unwrap().topic, "t32");
            assert_eq!(drained.last().unwrap().topic, "t39");
        });
    }

    #[test]
    fn the_count_bound_still_bites_when_it_is_the_tighter_one() {
        for_each_backend(3, 1 << 20, Overflow::DropOldest, |s| {
            for i in 0..6 {
                s.push(&sized(&format!("t{i}"), 512)).unwrap();
                check_bytes(s, "count-bound push");
            }
            assert_eq!(s.len().unwrap(), 3);
            assert_eq!(s.dropped_count(), 3);
            let drained = s.drain().unwrap();
            assert_eq!(drained.first().unwrap().topic, "t3");
            assert_eq!(drained.last().unwrap().topic, "t5");
        });
    }

    #[test]
    fn both_bounds_together_the_first_reached_wins() {
        // Count cap 4, byte cap 2000. Four 400-byte messages sit under the byte
        // budget; the fifth trips the count. Then a 1500-byte arrival trips bytes
        // and evicts several.
        for_each_backend(4, 2000, Overflow::DropOldest, |s| {
            for i in 0..4 {
                s.push(&sized(&format!("a{i}"), 400)).unwrap();
            }
            assert_eq!(s.len().unwrap(), 4);
            assert_eq!(s.dropped_count(), 0);
            s.push(&sized("count", 400)).unwrap();
            assert_eq!(s.len().unwrap(), 4, "count bound evicted exactly one");
            assert_eq!(s.dropped_count(), 1);
            check_bytes(s, "after count trip");
            s.push(&sized("big", 1500)).unwrap();
            check_bytes(s, "after byte trip");
            assert!(s.bytes().unwrap() <= 2000);
            assert!(s.len().unwrap() <= 4);
            assert!(s.dropped_count() >= 2, "the byte arrival evicted more");
        });
    }

    #[test]
    fn a_message_larger_than_the_byte_budget_does_not_empty_the_spool() {
        // A message larger than the entire byte budget can never fit. Do not
        // empty the spool trying: drop/refuse that one message, counted under
        // DropOldest, and keep what was already accepted.
        for_each_backend(10, 1000, Overflow::DropOldest, |s| {
            s.push(&sized("keep-a", 300)).unwrap();
            s.push(&sized("keep-b", 300)).unwrap();
            s.push(&sized("huge", 64 * 1024)).unwrap();
            assert_eq!(s.dropped_count(), 1, "the oversized arrival is counted");
            let topics: Vec<String> = s.drain().unwrap().into_iter().map(|m| m.topic).collect();
            assert_eq!(
                topics,
                vec!["keep-a".to_string(), "keep-b".to_string()],
                "the spool must stay intact — emptying it would lose accepted crossings \
                 for a message that still would not fit"
            );
        });
        for_each_backend(10, 1000, Overflow::Refuse, |s| {
            s.push(&sized("keep-a", 300)).unwrap();
            s.push(&sized("keep-b", 300)).unwrap();
            let err = s.push(&sized("huge", 64 * 1024)).unwrap_err();
            assert!(matches!(err, SpoolError::Full));
            assert_eq!(s.dropped_count(), 0, "a refusal is not a drop");
            assert_eq!(s.len().unwrap(), 2, "Refuse keeps the spool intact");
        });
    }

    #[test]
    fn the_byte_counter_equals_a_recomputed_sum_after_every_mutation() {
        for_each_backend(4, 4096, Overflow::DropOldest, |s| {
            s.push(&sized("a", 300)).unwrap();
            check_bytes(s, "push a/300");
            s.push(&sized("bb", 700)).unwrap();
            check_bytes(s, "push bb/700");
            s.push(&sized("ccc", 1500)).unwrap();
            check_bytes(s, "push ccc/1500");
            s.push(&sized("big", 3000)).unwrap();
            check_bytes(s, "push big/3000");
            assert!(
                s.dropped_count() >= 2,
                "the byte bound evicts as many as it needs: {}",
                s.dropped_count()
            );
            for i in 0..6 {
                s.push(&sized(&format!("z{i}"), 300)).unwrap();
                check_bytes(s, "count-bound push");
            }
            assert_eq!(s.len().unwrap(), 4);
            let all = s.drain().unwrap();
            assert!(!all.is_empty());
            check_bytes(s, "drain");
            assert_eq!(s.bytes().unwrap(), 0, "an emptied spool holds zero bytes");
            s.push(&sized("after", 900)).unwrap();
            check_bytes(s, "push after the drain");
            assert_eq!(s.bytes().unwrap(), 900);
        });
    }

    #[test]
    fn the_default_byte_bound_is_off_so_behaviour_matches_today() {
        // Unset max_bytes must not silently shrink a deployment: five 1 MiB
        // messages fit a count cap of 5, and the sixth is refused (default
        // Overflow::Refuse) — identical to the pre-#540 count-only spool.
        for_each_backend(5, 0, Overflow::Refuse, |s| {
            for i in 0..5 {
                s.push(&sized(&format!("t{i}"), 1024 * 1024)).unwrap();
                check_bytes(s, "default-off push");
            }
            assert_eq!(s.len().unwrap(), 5);
            assert_eq!(s.dropped_count(), 0);
            let err = s.push(&sized("overflow", 1024 * 1024)).unwrap_err();
            assert!(matches!(err, SpoolError::Full));
            assert_eq!(s.dropped_count(), 0);
            assert_eq!(s.len().unwrap(), 5);
        });
        // And with DropOldest, the 6th evicts exactly one — still the count
        // bound, never a hidden byte default.
        for_each_backend(5, 0, Overflow::DropOldest, |s| {
            for i in 0..5 {
                s.push(&sized(&format!("t{i}"), 1024 * 1024)).unwrap();
            }
            s.push(&sized("overflow", 1024 * 1024)).unwrap();
            assert_eq!(s.len().unwrap(), 5);
            assert_eq!(s.dropped_count(), 1);
            assert_eq!(s.drain().unwrap().last().unwrap().topic, "overflow");
        });
    }

    #[test]
    fn refuse_at_the_byte_bound_keeps_accepted_messages() {
        for_each_backend(10, 800, Overflow::Refuse, |s| {
            s.push(&sized("a", 300)).unwrap();
            s.push(&sized("b", 300)).unwrap();
            let err = s.push(&sized("c", 300)).unwrap_err();
            assert!(matches!(err, SpoolError::Full));
            assert_eq!(s.dropped_count(), 0);
            let topics: Vec<String> = s.drain().unwrap().into_iter().map(|m| m.topic).collect();
            assert_eq!(topics, vec!["a".to_string(), "b".to_string()]);
        });
    }

    #[test]
    fn a_v1_disk_record_round_trips_under_the_byte_bound() {
        // The byte bound is runtime state, not an on-disk layout change.
        // SPOOL_SCHEMA_VERSION stays 1: a spool*.redb written by the current
        // (v1) codec must still open, keep its messages, and reconstruct an
        // exact running total from those records.
        assert_eq!(
            SPOOL_SCHEMA_VERSION, 1,
            "do not bump SPOOL_SCHEMA_VERSION for a runtime byte bound"
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.redb");
        let held = sized("t/held", 400);
        {
            // Write a v1 record the same way today's (and the v1.0.0 tag's)
            // encode does — no schema bump, no extra columns.
            let db = redb::Database::create(&path).unwrap();
            mqtt_storage::schema::gate_or_migrate(
                &db,
                "spool",
                SPOOL_SCHEMA_VERSION,
                SPOOL_MIGRATIONS,
            )
            .unwrap();
            let tx = db.begin_write().unwrap();
            {
                let mut t = tx.open_table(SPOOL).unwrap();
                t.insert(0u64, encode(&held).as_slice()).unwrap();
            }
            tx.commit().unwrap();
        }
        let s = Spool::on_disk(&path, 10)
            .unwrap()
            .with_max_bytes(4096)
            .with_label("reopen");
        assert_eq!(s.len().unwrap(), 1, "v1 record survives");
        check_bytes(&s, "reopen v1");
        assert_eq!(s.bytes().unwrap(), message_bytes(&held));
        let drained = s.drain().unwrap();
        assert_eq!(drained, vec![held]);
        check_bytes(&s, "drain after v1 reopen");
    }
}
