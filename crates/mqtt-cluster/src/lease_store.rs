//! openraft storage for the lease consensus group
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md) workstream E step
//! 3b-ii; on-disk persistence is [ADR 0018](../../../docs/adr/0018-on-disk-persistence.md)
//! phase 2).
//!
//! [`LeaseStore`] implements openraft's `RaftStorage` over the [`LeaseMap`] state
//! machine ([`lease_raft`](crate::lease_raft)). It keeps the Raft log, vote, applied
//! state machine, and snapshot in an in-memory cache for fast reads, and — when opened
//! with [`LeaseStore::open`] — **mirrors every mutation to an on-disk `redb` database**,
//! fsynced before the call returns. The in-memory variant ([`LeaseStore::new`]) is the
//! non-persistent default for tests and ephemeral clusters.
//!
//! Persistence is **persist-before-acknowledge**: each write durably commits to disk
//! *before* the in-memory cache is updated and `Ok` is returned, so disk and cache never
//! diverge and openraft's storage contract holds. This is what makes the lease group
//! survive a restart and — crucially — restores Raft **safety**: the persisted vote
//! prevents a crashed-and-restarted voter from voting twice in a term. The lease group
//! is tiny and low-traffic (one assignment per ownership change), so a synchronous,
//! fsync-on-commit store on a blocking thread is appropriate.
//!
//! It is validated by openraft's own conformance `Suite` (run against both the in-memory
//! and the persistent variant) plus a restart-recovery test.

// openraft's `StorageError` is a large enum that pervades every storage signature here;
// boxing it would fight the trait contract for no benefit.
#![allow(clippy::result_large_err)]

use crate::lease_raft::{GroupId, LeaseConfig, LeaseMap, LeaseRecord, LeaseResponse};
use openraft::{
    Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use redb::{Database, Durability, ReadableTable, TableDefinition};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};

type NodeId = u64;

/// The lease store's on-disk layout version (ADR 0038 T2).
const LEASE_SCHEMA_VERSION: u32 = 1;

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

// Meta-table keys for the single-value fields.
const K_VOTE: &str = "vote";
const K_LAST_PURGED: &str = "last_purged";
const K_LAST_APPLIED: &str = "last_applied";
const K_LAST_MEMBERSHIP: &str = "last_membership";
const K_SM: &str = "sm";
const K_SNAP_META: &str = "snapshot_meta";
const K_SNAP_DATA: &str = "snapshot_data";
const K_SNAP_IDX: &str = "snapshot_idx";

/// A built snapshot: its metadata and the serialized [`LeaseMap`] bytes.
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

#[derive(Debug, Default)]
struct Inner {
    // --- Raft log ---
    vote: Option<Vote<NodeId>>,
    log: BTreeMap<u64, Entry<LeaseConfig>>,
    last_purged: Option<LogId<NodeId>>,
    // --- state machine ---
    sm: LeaseMap,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    // --- snapshot ---
    current_snapshot: Option<StoredSnapshot>,
    snapshot_idx: u64,
}

/// A backend-error string carried into openraft's `StorageError`.
#[derive(Debug)]
struct PersistError(String);
impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lease-store persistence error: {}", self.0)
    }
}
impl std::error::Error for PersistError {}

fn pe<E: std::fmt::Display>(e: E) -> PersistError {
    PersistError(e.to_string())
}

#[allow(clippy::needless_pass_by_value)] // an error sink: takes ownership, wraps a borrow
fn io(e: PersistError) -> StorageError<NodeId> {
    StorageIOError::write_logs(&e).into()
}

/// A single durable mutation, batched into one fsynced transaction.
enum WriteOp {
    PutMeta(&'static str, Vec<u8>),
    PutLog(u64, Vec<u8>),
    /// Delete the inclusive log-index range `[lo, hi]`.
    DelLog(u64, u64),
}

/// openraft store for the lease group. Cheaply cloneable (shared cache + db handle), so
/// it doubles as its own `LogReader` and `SnapshotBuilder`.
#[derive(Debug, Clone, Default)]
pub struct LeaseStore {
    inner: Arc<Mutex<Inner>>,
    /// `Some` when persisting to disk (ADR 0018 phase 2); `None` for the in-memory
    /// default.
    db: Option<Arc<Database>>,
    /// Fault injection (ADR 0026): an artificial per-commit delay in **milliseconds**,
    /// simulating slow-fsync storage deterministically (without a real slow disk) so the
    /// lease-group timing can be tested against it. Shared + atomic so a test can toggle it
    /// at runtime (form the group fast, then switch the latency on). `None` in production.
    commit_delay: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl LeaseStore {
    /// A fresh, **in-memory** store (non-persistent). State is lost on restart.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inject a shared, runtime-toggleable per-commit delay (ADR 0026 test fault injection),
    /// simulating a slow-fsync durable store. Applies even to the in-memory variant, so a
    /// multi-node lease group can be driven against slow-storage latency deterministically.
    #[must_use]
    pub fn with_commit_delay(mut self, delay: Option<Arc<std::sync::atomic::AtomicU64>>) -> Self {
        self.commit_delay = delay;
        self
    }

    /// Open (creating if absent) a **persistent** store at `path`, recovering any prior
    /// vote, log, applied state machine, and snapshot from disk.
    ///
    /// # Errors
    /// Returns a `StorageError` if the database cannot be opened or its contents decoded.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError<NodeId>> {
        let db = Database::create(path).map_err(|e| io(pe(e)))?;
        // Layout version gate (ADR 0038 T2): stamp fresh, fail closed on foreign.
        mqtt_storage::schema::gate(&db, "lease.redb", LEASE_SCHEMA_VERSION)
            .map_err(|e| io(pe(e)))?;
        // Create both tables so reads never race a missing table.
        let txn = db.begin_write().map_err(|e| io(pe(e)))?;
        {
            let _ = txn.open_table(LOG).map_err(|e| io(pe(e)))?;
            let _ = txn.open_table(META).map_err(|e| io(pe(e)))?;
        }
        txn.commit().map_err(|e| io(pe(e)))?;
        let inner = load(&db).map_err(io)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            db: Some(Arc::new(db)),
            commit_delay: None,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The lease currently assigned to `group` in the applied state machine.
    #[must_use]
    pub fn current_lease(&self, group: GroupId) -> Option<LeaseRecord> {
        self.lock().sm.get(group)
    }

    /// Durably apply a batch of mutations (one fsynced transaction), or a no-op when
    /// in-memory. Runs the blocking `redb` work off the async worker.
    async fn persist(&self, ops: Vec<WriteOp>) -> Result<(), StorageError<NodeId>> {
        // Simulate slow-storage commit latency before the write is acknowledged (ADR 0026).
        // openraft awaits this, so the delay reproduces a slow fsync on the raft hot path.
        if let Some(h) = &self.commit_delay {
            let ms = h.load(std::sync::atomic::Ordering::Relaxed);
            if ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            }
        }
        let Some(db) = self.db.clone() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || apply_ops(&db, &ops))
            .await
            .map_err(|e| io(pe(e)))?
            .map_err(io)
    }
}

/// Execute a batch of [`WriteOp`]s in one fsync-on-commit transaction.
fn apply_ops(db: &Database, ops: &[WriteOp]) -> Result<(), PersistError> {
    let mut txn = db.begin_write().map_err(pe)?;
    txn.set_durability(Durability::Immediate); // fsync on commit (ADR 0018)
    {
        let mut log = txn.open_table(LOG).map_err(pe)?;
        let mut meta = txn.open_table(META).map_err(pe)?;
        for op in ops {
            match op {
                WriteOp::PutMeta(k, v) => {
                    meta.insert(*k, v.as_slice()).map_err(pe)?;
                }
                WriteOp::PutLog(i, v) => {
                    log.insert(*i, v.as_slice()).map_err(pe)?;
                }
                WriteOp::DelLog(lo, hi) => {
                    let doomed: Vec<u64> = log
                        .range(*lo..=*hi)
                        .map_err(pe)?
                        .map(|item| item.map(|(k, _)| k.value()))
                        .collect::<Result<_, _>>()
                        .map_err(pe)?;
                    for k in doomed {
                        log.remove(k).map_err(pe)?;
                    }
                }
            }
        }
    }
    txn.commit().map_err(pe)?;
    Ok(())
}

/// Load the full store state from disk into an in-memory [`Inner`] cache.
fn load(db: &Database) -> Result<Inner, PersistError> {
    let txn = db.begin_read().map_err(pe)?;
    let mut inner = Inner::default();
    {
        let log = txn.open_table(LOG).map_err(pe)?;
        for item in log.range::<u64>(..).map_err(pe)? {
            let (k, v) = item.map_err(pe)?;
            let entry: Entry<LeaseConfig> = bincode::deserialize(v.value()).map_err(pe)?;
            inner.log.insert(k.value(), entry);
        }
    }
    {
        let meta = txn.open_table(META).map_err(pe)?;
        let get = |k: &str| -> Result<Option<Vec<u8>>, PersistError> {
            Ok(meta.get(k).map_err(pe)?.map(|g| g.value().to_vec()))
        };
        if let Some(v) = get(K_VOTE)? {
            inner.vote = Some(bincode::deserialize(&v).map_err(pe)?);
        }
        if let Some(v) = get(K_LAST_PURGED)? {
            inner.last_purged = Some(bincode::deserialize(&v).map_err(pe)?);
        }
        if let Some(v) = get(K_LAST_APPLIED)? {
            inner.last_applied = Some(bincode::deserialize(&v).map_err(pe)?);
        }
        if let Some(v) = get(K_LAST_MEMBERSHIP)? {
            inner.last_membership = bincode::deserialize(&v).map_err(pe)?;
        }
        if let Some(v) = get(K_SM)? {
            inner.sm = bincode::deserialize(&v).map_err(pe)?;
        }
        if let Some(v) = get(K_SNAP_IDX)? {
            inner.snapshot_idx = bincode::deserialize(&v).map_err(pe)?;
        }
        if let (Some(m), Some(d)) = (get(K_SNAP_META)?, get(K_SNAP_DATA)?) {
            inner.current_snapshot = Some(StoredSnapshot {
                meta: bincode::deserialize(&m).map_err(pe)?,
                data: d,
            });
        }
    }
    Ok(inner)
}

fn ser<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, StorageError<NodeId>> {
    bincode::serialize(v).map_err(|e| io(pe(e)))
}

impl RaftLogReader<LeaseConfig> for LeaseStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<LeaseConfig>>, StorageError<NodeId>> {
        let inner = self.lock();
        Ok(inner
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftSnapshotBuilder<LeaseConfig> for LeaseStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<LeaseConfig>, StorageError<NodeId>> {
        // Prepare under the lock, persist, then commit the snapshot to the cache.
        let (data, meta, new_idx) = {
            let inner = self.lock();
            let data = bincode::serialize(&inner.sm)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let new_idx = inner.snapshot_idx + 1;
            let snapshot_id = format!("{}-{}", inner.last_applied.map_or(0, |l| l.index), new_idx);
            let meta = SnapshotMeta {
                last_log_id: inner.last_applied,
                last_membership: inner.last_membership.clone(),
                snapshot_id,
            };
            (data, meta, new_idx)
        };

        self.persist(vec![
            WriteOp::PutMeta(K_SNAP_META, ser(&meta)?),
            WriteOp::PutMeta(K_SNAP_DATA, data.clone()),
            WriteOp::PutMeta(K_SNAP_IDX, ser(&new_idx)?),
        ])
        .await?;

        {
            let mut inner = self.lock();
            inner.snapshot_idx = new_idx;
            inner.current_snapshot = Some(StoredSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            });
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<LeaseConfig> for LeaseStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Persist-before-acknowledge: the vote must be durable before we return, or a
        // crashed-and-restarted voter could vote twice in a term (Raft safety).
        self.persist(vec![WriteOp::PutMeta(K_VOTE, ser(vote)?)])
            .await?;
        self.lock().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.lock().vote)
    }

    async fn get_log_state(&mut self) -> Result<LogState<LeaseConfig>, StorageError<NodeId>> {
        let inner = self.lock();
        let last_log_id = inner
            .log
            .values()
            .next_back()
            .map(|e| e.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<LeaseConfig>> + Send,
    {
        let entries: Vec<Entry<LeaseConfig>> = entries.into_iter().collect();
        let mut ops = Vec::with_capacity(entries.len());
        for e in &entries {
            ops.push(WriteOp::PutLog(e.log_id.index, ser(e)?));
        }
        self.persist(ops).await?;
        let mut inner = self.lock();
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        // Remove everything at or after `log_id` (a conflicting suffix).
        self.persist(vec![WriteOp::DelLog(log_id.index, u64::MAX)])
            .await?;
        self.lock().log.retain(|&index, _| index < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.persist(vec![
            WriteOp::DelLog(0, log_id.index),
            WriteOp::PutMeta(K_LAST_PURGED, ser(&log_id)?),
        ])
        .await?;
        let mut inner = self.lock();
        inner.log.retain(|&index, _| index > log_id.index);
        inner.last_purged = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let inner = self.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<LeaseConfig>],
    ) -> Result<Vec<Option<LeaseResponse>>, StorageError<NodeId>> {
        // Apply to a clone, persist the result, then commit the clone to the cache, so a
        // persist failure leaves the applied state machine unchanged.
        let (sm, last_applied, last_membership, responses) = {
            let inner = self.lock();
            let mut sm = inner.sm.clone();
            let mut last_applied = inner.last_applied;
            let mut last_membership = inner.last_membership.clone();
            let mut responses = Vec::with_capacity(entries.len());
            for entry in entries {
                last_applied = Some(entry.log_id);
                match &entry.payload {
                    EntryPayload::Blank => responses.push(None),
                    EntryPayload::Normal(req) => responses.push(sm.apply(req)),
                    EntryPayload::Membership(membership) => {
                        last_membership =
                            StoredMembership::new(Some(entry.log_id), membership.clone());
                        responses.push(None);
                    }
                }
            }
            (sm, last_applied, last_membership, responses)
        };

        let mut ops = vec![
            WriteOp::PutMeta(K_SM, ser(&sm)?),
            WriteOp::PutMeta(K_LAST_MEMBERSHIP, ser(&last_membership)?),
        ];
        if let Some(la) = last_applied {
            ops.push(WriteOp::PutMeta(K_LAST_APPLIED, ser(&la)?));
        }
        self.persist(ops).await?;

        let mut inner = self.lock();
        inner.sm = sm;
        inner.last_applied = last_applied;
        inner.last_membership = last_membership;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = (*snapshot).into_inner();
        let sm: LeaseMap = bincode::deserialize(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let mut ops = vec![
            WriteOp::PutMeta(K_SM, ser(&sm)?),
            WriteOp::PutMeta(K_LAST_MEMBERSHIP, ser(&meta.last_membership)?),
            WriteOp::PutMeta(K_SNAP_META, ser(meta)?),
            WriteOp::PutMeta(K_SNAP_DATA, data.clone()),
        ];
        if let Some(la) = meta.last_log_id {
            ops.push(WriteOp::PutMeta(K_LAST_APPLIED, ser(&la)?));
        }
        self.persist(ops).await?;

        let mut inner = self.lock();
        inner.sm = sm;
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LeaseConfig>>, StorageError<NodeId>> {
        Ok(self.lock().current_snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::LeaseStore;
    use crate::lease_raft::{GroupId, LeaseRequest, RaftNodeId};
    use openraft::StorageError;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// ADR 0038 T2: a lease store stamped by a foreign layout version refuses to
    /// open, naming both versions.
    #[test]
    fn a_foreign_lease_schema_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease.redb");
        drop(LeaseStore::open(&path).unwrap()); // stamped v1
        {
            let db = redb::Database::create(&path).unwrap();
            mqtt_storage::schema::force_version(&db, 999).unwrap();
        }
        let err = format!("{:?}", LeaseStore::open(&path).unwrap_err());
        assert!(err.contains("v999") && err.contains("expects v1"), "{err}");
    }

    /// openraft's conformance suite against the **in-memory** store.
    #[test]
    fn passes_openraft_conformance_suite_in_memory() {
        openraft::testing::Suite::test_all(|| async { LeaseStore::new() }).unwrap();
    }

    /// openraft's conformance suite against the **persistent** store — every storage
    /// method, now exercised through the disk write/read paths (ADR 0018 phase 2).
    #[test]
    fn passes_openraft_conformance_suite_persistent() {
        let dir = tempfile::tempdir().unwrap();
        let n = AtomicU64::new(0);
        openraft::testing::Suite::test_all(|| {
            let path = dir
                .path()
                .join(format!("lease-{}.redb", n.fetch_add(1, Ordering::Relaxed)));
            async move { LeaseStore::open(path).unwrap() }
        })
        .unwrap();
    }

    /// The persistence claim: a saved vote and an assigned lease survive the store being
    /// closed and reopened — restoring Raft safety (the vote) and the applied state (the
    /// lease) across a restart.
    #[tokio::test]
    async fn vote_and_lease_survive_reopen() -> Result<(), StorageError<RaftNodeId>> {
        use openraft::{
            CommittedLeaderId, Entry, EntryPayload, LogId, RaftLogReader, RaftStorage, Vote,
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recover.redb");
        let group: GroupId = 7;

        {
            let mut store = LeaseStore::open(&path)?;
            store.save_vote(&Vote::new(3, 1)).await?;
            // Append + apply a lease assignment for `group` to node 1.
            let entry = Entry {
                log_id: LogId::new(CommittedLeaderId::new(3, 1), 1),
                payload: EntryPayload::Normal(LeaseRequest::Assign { group, node: 1 }),
            };
            store.append_to_log([entry.clone()]).await?;
            store.apply_to_state_machine(&[entry]).await?;
            assert!(store.current_lease(group).is_some());
        }

        // Reopen: the vote, the log, and the applied lease are all recovered.
        let mut store = LeaseStore::open(&path)?;
        assert_eq!(store.read_vote().await?, Some(Vote::new(3, 1)));
        assert_eq!(store.try_get_log_entries(..).await?.len(), 1);
        let lease = store
            .current_lease(group)
            .expect("the lease survived reopen");
        assert_eq!(lease.holder, 1);
        Ok(())
    }
}
