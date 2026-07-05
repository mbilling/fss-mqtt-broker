//! On-disk, single-node [`ReplicatedLog`] backed by `redb` (ADR 0018, phase 1).
//!
//! This is the persistent counterpart to
//! [`InMemoryReplicatedLog`](crate::repl::InMemoryReplicatedLog): same contract and
//! semantics, but every mutation is committed to a `redb` database with
//! [`Durability::Immediate`] (fsync) before it returns. A QoS≥1 `append` is therefore
//! durable on disk by the time the PUBACK is released, and all session state — metadata,
//! subscriptions, offline queues, the QoS-2 dedup window — survives a process restart.
//!
//! It is the **owner of itself** (single node), so `append` never returns
//! `NotOwner`/`NoQuorum`; the clustered durability story (a disk-backed *replicated* log)
//! is a later phase of ADR 0018. Here "durable" means "survives this process restarting".
//!
//! ## On-disk layout
//!
//! Two tables in one database file:
//! - `entries`: key = `len(key) ++ key ++ offset_be`, value = the record bytes. The
//!   length-prefix isolates each logical key's range regardless of its bytes, and the
//!   big-endian offset suffix orders a key's entries ascending — so `read`/`truncate`/
//!   `live_range` are range scans.
//! - `next_offset`: key = the logical key, value = the highest offset assigned. Kept
//!   independently of `entries` so the per-key offset counter stays **monotonic across
//!   truncation** (an emptied queue does not reuse offsets); `remove` clears it so a
//!   re-created key starts fresh — matching the in-memory backend exactly.

use crate::repl::{LogEntry, ReplError, ReplicatedLog};
use crate::Offset;
use async_trait::async_trait;
use redb::{Database, Durability, ReadableTable, TableDefinition};
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;

/// The session store's on-disk layout version (ADR 0038 T2).
const SCHEMA_VERSION: u32 = 1;

const ENTRIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("entries");
const NEXT_OFFSET: TableDefinition<&str, u64> = TableDefinition::new("next_offset");

/// Map any `redb` error into the storage contract's backend error.
fn backend<E: Display>(e: E) -> ReplError {
    ReplError::Backend(e.to_string())
}

/// A durable, single-node [`ReplicatedLog`] persisting to a `redb` file (ADR 0018).
#[derive(Debug, Clone)]
pub struct PersistentLog {
    db: Arc<Database>,
}

impl PersistentLog {
    /// Open (creating if absent) the log database at `path`. The tables are created
    /// eagerly so later reads never race a not-yet-created table.
    ///
    /// # Errors
    /// Returns [`ReplError::Backend`] if the database cannot be opened or initialised.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReplError> {
        let db = Database::create(path).map_err(backend)?;
        // Layout version gate (ADR 0038 T2): stamp fresh, fail closed on foreign.
        crate::schema::gate(&db, "sessions.redb", SCHEMA_VERSION).map_err(backend)?;
        // Create both tables once so read transactions always find them.
        let txn = db.begin_write().map_err(backend)?;
        {
            let _ = txn.open_table(ENTRIES).map_err(backend)?;
            let _ = txn.open_table(NEXT_OFFSET).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Run a closure on a blocking thread with a cloned database handle, so the
    /// synchronous `redb` work (including the fsync on commit) never blocks an async
    /// worker.
    async fn run<T, F>(&self, f: F) -> Result<T, ReplError>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> Result<T, ReplError> + Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db))
            .await
            .map_err(backend)?
    }
}

/// Encode an entry key: `len(key) ++ key ++ offset_be`. Length-prefixing isolates each
/// logical key's range; the big-endian offset orders entries within a key.
fn entry_key(key: &str, offset: Offset) -> Vec<u8> {
    let kb = key.as_bytes();
    let mut out = Vec::with_capacity(4 + kb.len() + 8);
    out.extend_from_slice(&(u32::try_from(kb.len()).unwrap_or(u32::MAX)).to_be_bytes());
    out.extend_from_slice(kb);
    out.extend_from_slice(&offset.to_be_bytes());
    out
}

/// The inclusive `[lo, hi]` entry-key bounds covering a logical key's offsets in
/// `(after, u64::MAX]` (use `after = 0` for the whole key).
fn entry_bounds(key: &str, after: Offset) -> (Vec<u8>, Vec<u8>) {
    (
        entry_key(key, after.saturating_add(1)),
        entry_key(key, Offset::MAX),
    )
}

/// Decode the offset suffix (last 8 bytes) of an entry key.
fn decode_offset(entry_key: &[u8]) -> Offset {
    let n = entry_key.len();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&entry_key[n - 8..]);
    Offset::from_be_bytes(buf)
}

#[async_trait]
impl ReplicatedLog for PersistentLog {
    type Key = String;

    async fn append(&self, key: &String, record: Vec<u8>) -> Result<Offset, ReplError> {
        let key = key.clone();
        self.run(move |db| {
            let mut txn = db.begin_write().map_err(backend)?;
            txn.set_durability(Durability::Immediate); // fsync on commit (ADR 0018)
            let offset = {
                let mut counters = txn.open_table(NEXT_OFFSET).map_err(backend)?;
                // 1-based, monotonic per key (survives truncation; reset only by remove).
                let next = counters
                    .get(key.as_str())
                    .map_err(backend)?
                    .map_or(0, |g| g.value())
                    + 1;
                counters.insert(key.as_str(), next).map_err(backend)?;
                next
            };
            {
                let mut entries = txn.open_table(ENTRIES).map_err(backend)?;
                entries
                    .insert(entry_key(&key, offset).as_slice(), record.as_slice())
                    .map_err(backend)?;
            }
            txn.commit().map_err(backend)?;
            Ok(offset)
        })
        .await
    }

    async fn read(
        &self,
        key: &String,
        after: Offset,
        limit: usize,
    ) -> Result<Vec<LogEntry>, ReplError> {
        let key = key.clone();
        self.run(move |db| {
            let txn = db.begin_read().map_err(backend)?;
            let entries = txn.open_table(ENTRIES).map_err(backend)?;
            let (lo, hi) = entry_bounds(&key, after);
            let mut out = Vec::new();
            for item in entries
                .range(lo.as_slice()..=hi.as_slice())
                .map_err(backend)?
            {
                if out.len() >= limit {
                    break;
                }
                let (k, v) = item.map_err(backend)?;
                out.push(LogEntry {
                    offset: decode_offset(k.value()),
                    record: v.value().to_vec(),
                });
            }
            Ok(out)
        })
        .await
    }

    async fn live_range(&self, key: &String) -> Result<Option<(Offset, Offset)>, ReplError> {
        let key = key.clone();
        self.run(move |db| {
            let txn = db.begin_read().map_err(backend)?;
            let entries = txn.open_table(ENTRIES).map_err(backend)?;
            let (lo, hi) = entry_bounds(&key, 0);
            let mut range = entries
                .range(lo.as_slice()..=hi.as_slice())
                .map_err(backend)?;
            let first = range.next().transpose().map_err(backend)?;
            let last = range.next_back().transpose().map_err(backend)?;
            Ok(match (first, last) {
                (None, _) => None,
                (Some((k, _)), None) => {
                    let o = decode_offset(k.value());
                    Some((o, o))
                }
                (Some((lo_k, _)), Some((hi_k, _))) => {
                    Some((decode_offset(lo_k.value()), decode_offset(hi_k.value())))
                }
            })
        })
        .await
    }

    async fn truncate(&self, key: &String, up_to: Offset) -> Result<(), ReplError> {
        let key = key.clone();
        self.run(move |db| {
            let mut txn = db.begin_write().map_err(backend)?;
            txn.set_durability(Durability::Immediate);
            {
                let mut entries = txn.open_table(ENTRIES).map_err(backend)?;
                let lo = entry_key(&key, 0);
                let hi = entry_key(&key, up_to);
                // Collect the keys to drop, then remove (the range borrow ends first).
                let doomed: Vec<Vec<u8>> = entries
                    .range(lo.as_slice()..=hi.as_slice())
                    .map_err(backend)?
                    .map(|item| item.map(|(k, _)| k.value().to_vec()))
                    .collect::<Result<_, _>>()
                    .map_err(backend)?;
                for k in doomed {
                    entries.remove(k.as_slice()).map_err(backend)?;
                }
            }
            txn.commit().map_err(backend)?;
            Ok(())
        })
        .await
    }

    async fn remove(&self, key: &String) -> Result<(), ReplError> {
        let key = key.clone();
        self.run(move |db| {
            let mut txn = db.begin_write().map_err(backend)?;
            txn.set_durability(Durability::Immediate);
            {
                let mut entries = txn.open_table(ENTRIES).map_err(backend)?;
                let (lo, hi) = entry_bounds(&key, 0);
                let doomed: Vec<Vec<u8>> = entries
                    .range(lo.as_slice()..=hi.as_slice())
                    .map_err(backend)?
                    .map(|item| item.map(|(k, _)| k.value().to_vec()))
                    .collect::<Result<_, _>>()
                    .map_err(backend)?;
                for k in doomed {
                    entries.remove(k.as_slice()).map_err(backend)?;
                }
            }
            {
                // Reset the offset counter so a re-created key starts fresh (matches the
                // in-memory backend: `remove` clears the whole key, `truncate` does not).
                let mut counters = txn.open_table(NEXT_OFFSET).map_err(backend)?;
                counters.remove(key.as_str()).map_err(backend)?;
            }
            txn.commit().map_err(backend)?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::PersistentLog;
    use crate::repl::ReplicatedLog;

    fn rec(b: &[u8]) -> Vec<u8> {
        b.to_vec()
    }

    /// ADR 0038 T2: a session store stamped by a foreign layout version refuses to
    /// open, naming both versions — never silently misreading bytes.
    #[test]
    fn a_foreign_schema_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.redb");
        drop(PersistentLog::open(&path).unwrap()); // stamped v1
        {
            let db = redb::Database::create(&path).unwrap();
            crate::schema::force_version(&db, 999).unwrap();
        }
        let err = PersistentLog::open(&path).unwrap_err().to_string();
        assert!(err.contains("v999") && err.contains("expects v1"), "{err}");
    }

    fn temp_log() -> (tempfile::TempDir, PersistentLog) {
        let dir = tempfile::tempdir().unwrap();
        let log = PersistentLog::open(dir.path().join("log.redb")).unwrap();
        (dir, log)
    }

    /// Offsets are 1-based, per-key, and monotonic; `read(after)` replays the tail —
    /// the same contract the in-memory backend is tested against.
    #[tokio::test]
    async fn append_assigns_monotonic_offsets_per_key() {
        let (_dir, log) = temp_log();
        let (a, b) = ("q/a".to_string(), "q/b".to_string());

        assert_eq!(log.append(&a, rec(b"0")).await.unwrap(), 1);
        assert_eq!(log.append(&a, rec(b"1")).await.unwrap(), 2);
        assert_eq!(log.append(&b, rec(b"0")).await.unwrap(), 1);

        let entries = log.read(&a, 0, 10).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].offset, 1);
        assert_eq!(&entries[0].record, b"0");
        assert_eq!(entries[1].offset, 2);
        // `after` skips the replayed prefix.
        assert_eq!(log.read(&a, 1, 10).await.unwrap().len(), 1);
        // `b` is independent.
        assert_eq!(log.read(&b, 0, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn live_range_and_truncate_keep_offsets_monotonic() {
        let (_dir, log) = temp_log();
        let k = "q/c".to_string();
        for i in 0..5u8 {
            log.append(&k, rec(&[i])).await.unwrap();
        }
        assert_eq!(log.live_range(&k).await.unwrap(), Some((1, 5)));

        // Truncate the first three; the live range shifts but offsets do not rewind.
        log.truncate(&k, 3).await.unwrap();
        assert_eq!(log.live_range(&k).await.unwrap(), Some((4, 5)));
        assert_eq!(log.read(&k, 0, 10).await.unwrap()[0].offset, 4);
        // A new append continues monotonically (no offset reuse after truncation).
        assert_eq!(log.append(&k, rec(b"x")).await.unwrap(), 6);
    }

    #[tokio::test]
    async fn remove_clears_the_key_and_resets_offsets() {
        let (_dir, log) = temp_log();
        let k = "m/d".to_string();
        log.append(&k, rec(b"0")).await.unwrap();
        log.append(&k, rec(b"1")).await.unwrap();
        log.remove(&k).await.unwrap();

        assert!(log.read(&k, 0, 10).await.unwrap().is_empty());
        assert_eq!(log.live_range(&k).await.unwrap(), None);
        // After a full remove the key is fresh: offsets restart at 1.
        assert_eq!(log.append(&k, rec(b"new")).await.unwrap(), 1);
    }

    /// The durability claim: committed state survives the database being closed and
    /// reopened, and the per-key offset counter is preserved across the reopen.
    #[tokio::test]
    async fn state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable.redb");
        let k = "q/keep".to_string();
        {
            let log = PersistentLog::open(&path).unwrap();
            log.append(&k, rec(b"a")).await.unwrap();
            log.append(&k, rec(b"b")).await.unwrap();
            log.truncate(&k, 1).await.unwrap(); // drop offset 1, keep 2
                                                // drop closes the database
        }
        let log = PersistentLog::open(&path).unwrap();
        let entries = log.read(&k, 0, 10).await.unwrap();
        assert_eq!(
            entries.len(),
            1,
            "the surviving entry is recovered after reopen"
        );
        assert_eq!(entries[0].offset, 2);
        assert_eq!(&entries[0].record, b"b");
        // The offset counter persisted: the next append does not reuse offset 2.
        assert_eq!(log.append(&k, rec(b"c")).await.unwrap(), 3);
    }
}
