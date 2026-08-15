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
/// v2: retained records (`r/` keys) carry application properties (ADR 0038 T3) —
/// the row bytes' meaning changed, so a v1 file fails closed at the gate.
pub const SCHEMA_VERSION: u32 = 2;

/// In-place migrations for `sessions.redb` (ADR 0058). **Empty by design at 1.0**: the
/// first post-1.0 schema bump must land its `MigrationStep` here in the same PR, or
/// `a_schema_bump_without_its_migration_is_caught` fails. Wired now so the 1.0 tag
/// changes no open-path code.
const SESSION_MIGRATIONS: &[crate::schema::MigrationStep] = &[];

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
        let db = crate::open::create_with_lock_retry(path).map_err(backend)?;
        // Layout version gate (ADR 0038 T2): stamp fresh, fail closed on foreign.
        crate::schema::gate_or_migrate(&db, "sessions.redb", SCHEMA_VERSION, SESSION_MIGRATIONS)
            .map_err(backend)?;
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

/// Decode the logical key from an entry key (`len ++ key ++ offset_be`), or `None` if the
/// bytes are not a well-formed entry key.
fn decode_entry_key(entry_key: &[u8]) -> Option<String> {
    let len = usize::try_from(u32::from_be_bytes(entry_key.get(..4)?.try_into().ok()?)).ok()?;
    let bytes = entry_key.get(4..4 + len)?;
    String::from_utf8(bytes.to_vec()).ok()
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

    async fn keys(&self) -> Result<Vec<String>, ReplError> {
        // Every logical key with at least one LIVE entry, decoded from the entry-key
        // prefix. Without this the trait default (`empty`) applied to the single-node
        // persistent store, and everything built on key enumeration read as "this node
        // holds no sessions": the ADR 0009 expiry sweep, the ADR 0042 T9 takeover
        // materialisation — and, once ADR 0062 landed, an ONLINE BACKUP that would have
        // reported success over an empty file. A backup that silently omits every session
        // is worse than no backup, which is why this is implemented rather than refused.
        self.run(move |db| {
            let txn = db.begin_read().map_err(backend)?;
            let entries = txn.open_table(ENTRIES).map_err(backend)?;
            let mut out = std::collections::BTreeSet::new();
            for item in entries.range::<&[u8]>(..).map_err(backend)? {
                let (k, _) = item.map_err(backend)?;
                if let Some(key) = decode_entry_key(k.value()) {
                    out.insert(key);
                }
            }
            Ok(out.into_iter().collect())
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

/// The oldest `sessions.redb` version the 1.0 stability contract migrates from (ADR
/// 0058). Pre-1.0 this equals [`SCHEMA_VERSION`], so the covered range is empty and the
/// empty [`SESSION_MIGRATIONS`] registry is correct. At the 1.0 tag this pins to the 1.0
/// schema version, and from then on raising `SCHEMA_VERSION` without adding a
/// `MigrationStep` fails the coverage test below.
#[cfg(test)]
const MIGRATE_FLOOR: u32 = SCHEMA_VERSION;

#[cfg(test)]
mod tests {
    use super::PersistentLog;
    use super::{MIGRATE_FLOOR, SCHEMA_VERSION, SESSION_MIGRATIONS};
    use crate::repl::ReplicatedLog;

    fn rec(b: &[u8]) -> Vec<u8> {
        b.to_vec()
    }

    /// ADR 0058 T2: the sessions-store migration registry must cover every version from
    /// the contract floor to the current layout. This is the guard that catches a schema
    /// bump landing without its migration — today the range is empty (pre-1.0), but the
    /// moment `MIGRATE_FLOOR` pins below `SCHEMA_VERSION` at 1.0, a version raise with no
    /// matching step fails HERE rather than at some operator's upgrade.
    #[test]
    fn the_migration_registry_covers_the_contract_range() {
        crate::schema::assert_migrations_cover(MIGRATE_FLOOR, SCHEMA_VERSION, SESSION_MIGRATIONS)
            .expect("sessions.redb migration registry has a gap");
    }

    /// ADR 0038 T2: a session store stamped by a foreign layout version refuses to
    /// open, naming both versions — never silently misreading bytes.
    #[test]
    fn a_foreign_schema_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.redb");
        drop(PersistentLog::open(&path).unwrap()); // stamped with the current version
        {
            let db = redb::Database::create(&path).unwrap();
            crate::schema::force_version(&db, 999).unwrap();
        }
        let err = PersistentLog::open(&path).unwrap_err().to_string();
        let expected = format!("expects v{}", super::SCHEMA_VERSION);
        assert!(err.contains("v999") && err.contains(&expected), "{err}");
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
    /// Key enumeration on the single-node persistent store — absent until ADR 0062 needed
    /// it, and the trait default (`empty`) is a silent wrong answer: every feature built on
    /// enumeration (the ADR 0009 expiry sweep, ADR 0042 T9 takeover materialisation, and an
    /// online BACKUP) read "this node holds no sessions".
    #[tokio::test]
    async fn keys_enumerates_every_live_key_and_forgets_removed_ones() {
        let (_dir, log) = temp_log();
        for key in ["m/a", "q/a", "m/b"] {
            log.append(&key.to_string(), rec(b"x")).await.unwrap();
        }
        let mut keys = log.keys().await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["m/a", "m/b", "q/a"]);

        // A removed key is gone; a TRUNCATED one with no live entries is gone too (the
        // contract is "holds a non-empty log"), and the survivors are unaffected.
        log.remove(&"m/b".to_string()).await.unwrap();
        log.truncate(&"q/a".to_string(), 1).await.unwrap();
        let mut keys = log.keys().await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["m/a"]);
    }

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
