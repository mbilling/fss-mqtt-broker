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
const SCHEMA_VERSION: u32 = 2;

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

/// Decode the logical key out of an entry key (`len(key) ++ key ++ offset_be`), or `None`
/// if the bytes are not shaped like one — a foreign row must be skipped, never guessed at.
fn decode_logical_key(entry_key: &[u8]) -> Option<String> {
    let len = usize::try_from(u32::from_be_bytes(entry_key.get(..4)?.try_into().ok()?)).ok()?;
    // 4 length bytes + the key + the 8-byte offset suffix, exactly.
    if entry_key.len() != 4 + len + 8 {
        return None;
    }
    std::str::from_utf8(entry_key.get(4..4 + len)?)
        .ok()
        .map(ToString::to_string)
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

    /// Every logical key this file holds a non-empty log for.
    ///
    /// **This override is load-bearing, and its absence was a silent hole** (issue #299):
    /// [`ReplicatedSessionStore::all_sessions`](crate::logged::ReplicatedSessionStore) and
    /// `expiring_sessions` enumerate through here, so a backend that takes the trait's
    /// empty default makes every INHERIT path inert — ADR 0009 §3's persisted session
    /// expiry deadline and issue #299's persisted pending Will alike. In this mode (ADR
    /// 0018 phase 1: on-disk, single node) that meant a restart over the same data dir
    /// read back *nothing at all*: sessions never expired, and a delayed will armed before
    /// the restart was lost even though its bytes were on disk. The in-memory and
    /// clustered backends always enumerated; this one did not, and no test covered the
    /// mode operators run on one node.
    ///
    /// Entry keys are `len(key) ++ key ++ offset_be`, so one logical key owns a contiguous
    /// range and the scan **skips** from each key's first entry past its last rather than
    /// walking every queued message: `O(distinct keys · log n)`, not `O(entries)`. That
    /// matters because a session's offline queue can hold thousands of entries while
    /// contributing exactly one key. The length prefix makes the range self-delimiting —
    /// no logical key's prefix can extend another's — so `prefix ++ FF..FF ++ 00` is
    /// strictly greater than every entry of that key and strictly less than any other
    /// key's.
    async fn keys(&self) -> Result<Vec<String>, ReplError> {
        self.run(move |db| {
            let txn = db.begin_read().map_err(backend)?;
            let entries = txn.open_table(ENTRIES).map_err(backend)?;
            let mut out = Vec::new();
            let mut cursor: Vec<u8> = Vec::new();
            loop {
                let mut range = entries
                    .range::<&[u8]>((
                        std::ops::Bound::Included(cursor.as_slice()),
                        std::ops::Bound::Unbounded,
                    ))
                    .map_err(backend)?;
                let Some(item) = range.next() else { break };
                let (raw, _) = item.map_err(backend)?;
                let Some(key) = decode_logical_key(raw.value()) else {
                    // Not one of our entry keys (or not UTF-8): step one byte past it
                    // rather than looping forever on it.
                    cursor = raw.value().to_vec();
                    cursor.push(0);
                    continue;
                };
                cursor = entry_key(&key, Offset::MAX);
                cursor.push(0);
                out.push(key);
            }
            Ok(out)
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

    /// Issue #299: **this backend must be able to list its own keys, or every inherit
    /// path over it is silently inert.**
    ///
    /// `ReplicatedSessionStore::all_sessions` / `expiring_sessions` enumerate through
    /// `keys()`. This log took the trait's `Ok(Vec::new())` default, so on a single-node
    /// on-disk broker (ADR 0018 phase 1 — `MQTTD_DATA_DIR`, no cluster) a restart read
    /// back NOTHING: ADR 0009 §3's persisted session-expiry deadlines never fired, and a
    /// delayed Will persisted at arm time was lost across the restart even though its
    /// bytes were on disk. Only the in-memory and clustered backends enumerated, so no
    /// test covered the mode operators run on one node.
    ///
    /// Also pinned here: only NON-EMPTY logs are reported (matching
    /// `InMemoryReplicatedLog`), the queue-key scan does not depend on how many entries a
    /// key holds, and the listing survives a reopen — which is the whole point.
    #[tokio::test]
    async fn keys_enumerates_every_logical_key_and_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keys.redb");
        let (meta, queue, other) = (
            "m/dev-1".to_string(),
            "q/dev-1".to_string(),
            "m/dev-2".to_string(),
        );
        {
            let log = PersistentLog::open(&path).unwrap();
            log.append(&meta, rec(b"snapshot")).await.unwrap();
            log.append(&other, rec(b"snapshot")).await.unwrap();
            // A key with MANY entries contributes exactly one key, and the scan must not
            // walk them: a session's offline queue is routinely thousands of messages.
            for i in 0..64u8 {
                log.append(&queue, rec(&[i])).await.unwrap();
            }
            let mut keys = log.keys().await.unwrap();
            keys.sort();
            assert_eq!(
                keys,
                vec![meta.clone(), other.clone(), queue.clone()],
                "every logical key with entries is listed, exactly once each"
            );
        }

        // The reopen is the case that was broken end to end: a restarted node reads the
        // session metadata keys back out of the file it just opened.
        let log = PersistentLog::open(&path).unwrap();
        let mut keys = log.keys().await.unwrap();
        keys.sort();
        assert_eq!(
            keys,
            vec![meta.clone(), other.clone(), queue.clone()],
            "a restart over the same data dir must find the sessions on disk — without \
             this, ADR 0009 §3's expiry deadlines and issue #299's pending wills are both \
             read back as if the file were empty"
        );

        // A fully-drained queue is effectively absent (the in-memory backend's rule), and
        // a removed key is gone.
        log.truncate(&queue, 64).await.unwrap();
        log.remove(&other).await.unwrap();
        assert_eq!(
            log.keys().await.unwrap(),
            vec![meta],
            "only keys with live entries are reported"
        );
    }
}
