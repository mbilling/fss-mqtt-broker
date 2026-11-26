//! openraft storage for the lease consensus group
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md) workstream E step
//! 3b-ii; on-disk persistence is [ADR 0018](../../../docs/adr/0018-on-disk-persistence.md)
//! phase 2).
//!
//! [`LeaseStore`] is both halves of openraft's storage, the log store
//! (`RaftLogStorage`) and the state machine (`RaftStateMachine`), over the [`LeaseMap`]
//! state machine ([`lease_raft`](crate::lease_raft)). It keeps the Raft log, vote, applied
//! state machine, and snapshot in an in-memory cache for fast reads, and — when opened
//! with [`LeaseStore::open`] — **mirrors every mutation to an on-disk `redb` database**,
//! fsynced before the call returns. The in-memory variant ([`LeaseStore::new`]) is the
//! non-persistent default for tests and ephemeral clusters.
//!
//! Persistence is **persist-before-acknowledge**: each write durably commits to disk
//! *before* the in-memory cache is updated and openraft is told, so disk and cache never
//! diverge and openraft's storage contract holds. This is what makes the lease group
//! survive a restart and — crucially — restores Raft **safety**: the persisted vote
//! prevents a crashed-and-restarted voter from voting twice in a term. The lease group
//! is tiny and low-traffic (one assignment per ownership change), so a synchronous,
//! fsync-on-commit store on a blocking thread is appropriate.
//!
//! The stored values are postcard encodings of openraft types, and they are byte for
//! byte what mqttd 1.3 (openraft 0.9) writes: openraft 0.10 kept the layouts, and the
//! snapshot metadata is stored in openraft-legacy's 0.9-shaped form, which still
//! carries a snapshot id. A lease.redb therefore opens unchanged in either direction
//! of a rolling upgrade (ADR 0058); golden-bytes tests below pin it.
//!
//! It is validated by openraft's own conformance `Suite` (run against both the in-memory
//! and the persistent variant) plus a restart-recovery test.

use crate::lease_raft::{
    decode_state, encode_state, GroupId, LeaseConfig, LeaseMap, LeaseRecord, ReplicationRecord,
};
use futures_util::{Stream, TryStreamExt};
use openraft::storage::{
    EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder,
    RaftStateMachine,
};
use openraft::type_config::alias::{
    EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use openraft::{EntryPayload, OptionalSend, StoredMembership};
use openraft_legacy::network_v1::SnapshotReceiverFactory;
use redb::{Database, Durability, ReadableTable, TableDefinition};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};

type Entry = EntryOf<LeaseConfig>;
type LogId = LogIdOf<LeaseConfig>;
type Vote = VoteOf<LeaseConfig>;
type Membership = StoredMembershipOf<LeaseConfig>;
type Meta = SnapshotMetaOf<LeaseConfig>;
/// Snapshot metadata in openraft 0.9's layout, with the snapshot id: the form
/// lease.redb stores (see the module docs).
type StoredMeta = openraft_legacy::network_v1::SnapshotMeta<LeaseConfig>;

/// A snapshot's bytes: the serialized [`LeaseMap`], in a seekable buffer, as the
/// chunked snapshot transfer reads and writes it.
pub type SnapshotData = Cursor<Vec<u8>>;
type Snapshot = SnapshotOf<LeaseConfig, SnapshotData>;

/// The lease store's on-disk layout version (ADR 0038 T2). Still 1 across the
/// postcard codec succession (ADR 0052, pre-release baseline reset): v1 now means
/// postcard-encoded values. A pre-ADR-0052 dev store carries bincode bytes under
/// the same stamp and fails at value decode (not at the gate) — wipe dev data
/// dirs; nothing is released, so no upgrade path is owed.
pub const LEASE_SCHEMA_VERSION: u32 = 1;

/// In-place migrations for `lease.redb` (ADR 0058). Empty by design at 1.0 — the first
/// post-1.0 schema bump lands its `MigrationStep` here in the same PR, or the coverage
/// test fails.
const LEASE_MIGRATIONS: &[mqtt_storage::schema::MigrationStep] = &[];
/// Oldest `lease.redb` version the contract migrates from (ADR 0058). Pinned to the
/// layout the v1.0.0 tag shipped — a literal, not [`LEASE_SCHEMA_VERSION`], so a
/// version raise without its `MigrationStep` fails the coverage test rather than
/// moving the floor with the ceiling. Raised only when a release retires migrations
/// (ADR 0039).
#[cfg(test)]
const LEASE_MIGRATE_FLOOR: u32 = 1;

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
    meta: Meta,
    data: Vec<u8>,
}

#[derive(Debug, Default)]
struct Inner {
    // --- Raft log ---
    vote: Option<Vote>,
    log: BTreeMap<u64, Entry>,
    last_purged: Option<LogId>,
    // --- state machine ---
    sm: LeaseMap,
    last_applied: Option<LogId>,
    last_membership: Membership,
    // --- snapshot ---
    current_snapshot: Option<StoredSnapshot>,
    /// Counts the snapshots this store built; it numbers their stored ids, as
    /// mqttd 1.3 did (a store rolled back to 1.3 goes on counting from here).
    snapshot_idx: u64,
}

/// A backend-error string, handed to openraft as an [`io::Error`].
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

fn io_err(e: PersistError) -> io::Error {
    io::Error::other(e)
}

/// A single durable mutation, batched into one fsynced transaction.
enum WriteOp {
    PutMeta(&'static str, Vec<u8>),
    PutLog(u64, Vec<u8>),
    /// Delete the inclusive log-index range `[lo, hi]`.
    DelLog(u64, u64),
}

/// openraft store for the lease group. Cheaply cloneable (shared cache + db handle), so
/// one store is both the log store and the state machine, and its own `LogReader` and
/// `SnapshotBuilder`.
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
    /// Returns an error if the database cannot be opened or its contents decoded.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let db = mqtt_storage::open::create_with_lock_retry(path).map_err(|e| io_err(pe(e)))?;
        // Layout version gate (ADR 0038 T2): stamp fresh, fail closed on foreign.
        mqtt_storage::schema::gate_or_migrate(
            &db,
            "lease.redb",
            LEASE_SCHEMA_VERSION,
            LEASE_MIGRATIONS,
        )
        .map_err(|e| io_err(pe(e)))?;
        // Create both tables so reads never race a missing table.
        let txn = db.begin_write().map_err(|e| io_err(pe(e)))?;
        {
            let _ = txn.open_table(LOG).map_err(|e| io_err(pe(e)))?;
            let _ = txn.open_table(META).map_err(|e| io_err(pe(e)))?;
        }
        txn.commit().map_err(|e| io_err(pe(e)))?;
        let inner = load(&db).map_err(io_err)?;
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

    /// The highest lease epoch ever minted in the applied state machine: 0 on a
    /// cluster that has never assigned a lease (ADR 0080 founding reads it).
    #[must_use]
    pub fn high_epoch(&self) -> crate::lease::Epoch {
        self.lock().sm.high_epoch()
    }

    /// `group`'s current lease together with the replication record, read from
    /// ONE applied state (ADR 0080 §4): a lease epoch minted when a factor change
    /// opened is never seen without that change.
    #[must_use]
    pub fn lease_with_replication(
        &self,
        group: GroupId,
    ) -> (Option<LeaseRecord>, ReplicationRecord) {
        let guard = self.lock();
        (guard.sm.get(group), guard.sm.replication())
    }

    /// The replication factor in the applied state machine (ADR 0080).
    #[must_use]
    pub fn replication(&self) -> ReplicationRecord {
        self.lock().sm.replication()
    }

    /// Durably apply a batch of mutations (one fsynced transaction), or a no-op when
    /// in-memory. Runs the blocking `redb` work off the async worker.
    async fn persist(&self, ops: Vec<WriteOp>) -> io::Result<()> {
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
            .map_err(|e| io_err(pe(e)))?
            .map_err(io_err)
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

/// Strict postcard decode for stored values (ADR 0052): exactly one `T`, no
/// trailing bytes — a corrupt or foreign-format value fails closed.
fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, PersistError> {
    match postcard::take_from_bytes(bytes) {
        Ok((v, [])) => Ok(v),
        Ok(_) => Err(PersistError("trailing bytes in stored value".into())),
        Err(e) => Err(pe(e)),
    }
}

/// Load the full store state from disk into an in-memory [`Inner`] cache.
fn load(db: &Database) -> Result<Inner, PersistError> {
    let txn = db.begin_read().map_err(pe)?;
    let mut inner = Inner::default();
    {
        let log = txn.open_table(LOG).map_err(pe)?;
        for item in log.range::<u64>(..).map_err(pe)? {
            let (k, v) = item.map_err(pe)?;
            let entry: Entry = de(v.value())?;
            inner.log.insert(k.value(), entry);
        }
    }
    {
        let meta = txn.open_table(META).map_err(pe)?;
        let get = |k: &str| -> Result<Option<Vec<u8>>, PersistError> {
            Ok(meta.get(k).map_err(pe)?.map(|g| g.value().to_vec()))
        };
        if let Some(v) = get(K_VOTE)? {
            inner.vote = Some(de(&v)?);
        }
        if let Some(v) = get(K_LAST_PURGED)? {
            inner.last_purged = Some(de(&v)?);
        }
        if let Some(v) = get(K_LAST_APPLIED)? {
            inner.last_applied = Some(de(&v)?);
        }
        if let Some(v) = get(K_LAST_MEMBERSHIP)? {
            inner.last_membership = de(&v)?;
        }
        if let Some(v) = get(K_SM)? {
            inner.sm = decode_state(&v).map_err(pe)?;
        }
        if let Some(v) = get(K_SNAP_IDX)? {
            inner.snapshot_idx = de(&v)?;
        }
        if let (Some(m), Some(d)) = (get(K_SNAP_META)?, get(K_SNAP_DATA)?) {
            inner.current_snapshot = Some(StoredSnapshot {
                meta: de::<StoredMeta>(&m)?.into_meta(),
                data: d,
            });
        }
    }
    Ok(inner)
}

fn ser<T: serde::Serialize>(v: &T) -> io::Result<Vec<u8>> {
    postcard::to_allocvec(v).map_err(|e| io_err(pe(e)))
}

/// The state machine's own encoding (ADR 0080): the legacy lease-table shape
/// until a replication factor is recorded — see [`encode_state`].
fn ser_sm(sm: &LeaseMap) -> io::Result<Vec<u8>> {
    encode_state(sm).map_err(|e| io_err(pe(e)))
}

/// Snapshot metadata as lease.redb stores it: 0.9's layout, with the id mqttd 1.3
/// gave a snapshot (`<last applied index>-<snapshot count>`).
fn stored_meta(meta: &Meta, snapshot_idx: u64) -> io::Result<Vec<u8>> {
    let id = format!("{}-{snapshot_idx}", meta.last_log_id.map_or(0, |l| l.index));
    ser(&StoredMeta::new(meta.clone(), id))
}

impl RaftLogReader<LeaseConfig> for LeaseStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<Entry>> {
        let inner = self.lock();
        Ok(inner
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
        Ok(self.lock().vote)
    }
}

impl RaftLogStorage<LeaseConfig> for LeaseStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<LeaseConfig>> {
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

    async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
        // Persist-before-acknowledge: the vote must be durable before we return, or a
        // crashed-and-restarted voter could vote twice in a term (Raft safety).
        self.persist(vec![WriteOp::PutMeta(K_VOTE, ser(vote)?)])
            .await?;
        self.lock().vote = Some(*vote);
        Ok(())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<LeaseConfig>) -> io::Result<()>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        let mut ops = Vec::with_capacity(entries.len());
        for e in &entries {
            ops.push(WriteOp::PutLog(e.log_id.index, ser(e)?));
        }
        // A failed persist is returned, and openraft stops this node; the callback
        // reports only entries that are on disk.
        self.persist(ops).await?;
        {
            let mut inner = self.lock();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> io::Result<()> {
        // Keep the log up to and including `last_log_id`; remove the conflicting suffix.
        let from = last_log_id.map_or(0, |l| l.index + 1);
        self.persist(vec![WriteOp::DelLog(from, u64::MAX)]).await?;
        self.lock().log.retain(|&index, _| index < from);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> io::Result<()> {
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
}

impl RaftSnapshotBuilder<LeaseConfig> for LeaseStore {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> io::Result<Snapshot> {
        // Prepare under the lock, persist, then commit the snapshot to the cache.
        let (data, meta, new_idx) = {
            let inner = self.lock();
            let data = ser_sm(&inner.sm)?;
            let meta = Meta {
                last_log_id: inner.last_applied,
                last_membership: inner.last_membership.clone(),
            };
            (data, meta, inner.snapshot_idx + 1)
        };

        self.persist(vec![
            WriteOp::PutMeta(K_SNAP_META, stored_meta(&meta, new_idx)?),
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
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftStateMachine<LeaseConfig> for LeaseStore {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> io::Result<(Option<LogId>, Membership)> {
        let inner = self.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<LeaseConfig>>> + Unpin + OptionalSend,
    {
        // openraft hands over the committed entries it has ready as one finite stream;
        // take them all, so the batch is applied and persisted in one transaction.
        let batch: Vec<EntryResponder<LeaseConfig>> = entries.try_collect().await?;
        if batch.is_empty() {
            return Ok(());
        }
        // Apply to a clone, persist the result, then commit the clone to the cache, so a
        // persist failure leaves the applied state machine unchanged.
        let (sm, last_applied, last_membership, answers) = {
            let inner = self.lock();
            let mut sm = inner.sm.clone();
            let mut last_applied = inner.last_applied;
            let mut last_membership = inner.last_membership.clone();
            let mut answers = Vec::with_capacity(batch.len());
            for (entry, responder) in batch {
                last_applied = Some(entry.log_id);
                let response = match &entry.payload {
                    EntryPayload::Blank => None,
                    EntryPayload::Normal(req) => sm.apply(req),
                    EntryPayload::Membership(membership) => {
                        last_membership =
                            StoredMembership::new(Some(entry.log_id), membership.clone());
                        None
                    }
                };
                answers.push((responder, response));
            }
            (sm, last_applied, last_membership, answers)
        };

        let mut ops = vec![
            WriteOp::PutMeta(K_SM, ser_sm(&sm)?),
            WriteOp::PutMeta(K_LAST_MEMBERSHIP, ser(&last_membership)?),
        ];
        if let Some(la) = &last_applied {
            ops.push(WriteOp::PutMeta(K_LAST_APPLIED, ser(la)?));
        }
        self.persist(ops).await?;

        {
            let mut inner = self.lock();
            inner.sm = sm;
            inner.last_applied = last_applied;
            inner.last_membership = last_membership;
        }
        // Answer the writers only once the applied state is durable.
        for (responder, response) in answers {
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(&mut self, meta: &Meta, snapshot: SnapshotData) -> io::Result<()> {
        let data = snapshot.into_inner();
        // These bytes arrived from a peer over InstallSnapshot — bound them like
        // any other raft RPC payload (ADR 0052) before decoding.
        if data.len() > crate::raft_mesh::MAX_RAFT_RPC {
            return Err(io_err(PersistError(
                "snapshot exceeds raft rpc size bound".into(),
            )));
        }
        let sm: LeaseMap = decode_state(&data).map_err(|e| io_err(pe(e)))?;

        let snapshot_idx = self.lock().snapshot_idx;
        let mut ops = vec![
            WriteOp::PutMeta(K_SM, ser_sm(&sm)?),
            WriteOp::PutMeta(K_LAST_MEMBERSHIP, ser(&meta.last_membership)?),
            WriteOp::PutMeta(K_SNAP_META, stored_meta(meta, snapshot_idx)?),
            WriteOp::PutMeta(K_SNAP_DATA, data.clone()),
        ];
        if let Some(la) = &meta.last_log_id {
            ops.push(WriteOp::PutMeta(K_LAST_APPLIED, ser(la)?));
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

    async fn get_current_snapshot(&mut self) -> io::Result<Option<Snapshot>> {
        Ok(self.lock().current_snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Cursor::new(s.data.clone()),
        }))
    }
}

/// Where a snapshot sent in chunks (openraft-legacy's v1 transfer, the one mqttd 1.3
/// speaks) is assembled before [`install_snapshot`](RaftStateMachine::install_snapshot).
impl SnapshotReceiverFactory<LeaseConfig> for LeaseStore {
    type SnapshotReceiver = SnapshotData;

    async fn begin_receiving_snapshot(&mut self) -> io::Result<SnapshotData> {
        Ok(Cursor::new(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    /// ADR 0058 T2: the lease.redb migration registry must cover the contract range, so a
    /// future schema bump without its migration fails here, not at an operator's upgrade.
    #[test]
    fn the_migration_registry_covers_the_contract_range() {
        mqtt_storage::schema::assert_migrations_cover(
            super::LEASE_MIGRATE_FLOOR,
            super::LEASE_SCHEMA_VERSION,
            super::LEASE_MIGRATIONS,
        )
        .expect("lease.redb migration registry has a gap");
    }

    use super::{de, stored_meta, Entry, LeaseStore, Meta, StoredMeta};
    use crate::lease_raft::testing::{committed_vote, hex, log_id, membership, unhex, vote};
    use crate::lease_raft::{GroupId, LeaseConfig, LeaseRequest};
    use openraft::storage::{
        RaftLogReader, RaftLogStorage, RaftLogStorageExt, RaftSnapshotBuilder, RaftStateMachine,
    };
    use openraft::testing::log::{StoreBuilder, Suite};
    use openraft::type_config::alias::{StoredMembershipOf, VoteOf};
    use openraft::{EntryPayload, StorageError, StoredMembership};
    use std::sync::atomic::{AtomicU64, Ordering};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// Apply `entries` through the state machine, as openraft's apply worker does.
    async fn apply(store: &mut LeaseStore, entries: Vec<Entry>) -> std::io::Result<()> {
        let items: Vec<std::io::Result<_>> = entries.into_iter().map(|e| Ok((e, None))).collect();
        store.apply(futures_util::stream::iter(items)).await
    }

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

    /// Builds the in-memory store for openraft's conformance suite.
    struct InMemory;

    impl StoreBuilder<LeaseConfig, LeaseStore, LeaseStore> for InMemory {
        async fn build(&self) -> Result<((), LeaseStore, LeaseStore), StorageError<LeaseConfig>> {
            let store = LeaseStore::new();
            Ok(((), store.clone(), store))
        }
    }

    /// Builds a fresh on-disk store per case for openraft's conformance suite.
    struct OnDisk {
        dir: tempfile::TempDir,
        n: AtomicU64,
    }

    impl StoreBuilder<LeaseConfig, LeaseStore, LeaseStore> for OnDisk {
        async fn build(&self) -> Result<((), LeaseStore, LeaseStore), StorageError<LeaseConfig>> {
            let path = self.dir.path().join(format!(
                "lease-{}.redb",
                self.n.fetch_add(1, Ordering::Relaxed)
            ));
            let store = LeaseStore::open(path).expect("open a fresh lease store");
            Ok(((), store.clone(), store))
        }
    }

    /// openraft's conformance suite against the **in-memory** store.
    #[tokio::test]
    async fn passes_openraft_conformance_suite_in_memory() {
        Suite::<LeaseConfig, LeaseStore, LeaseStore, InMemory, ()>::test_all(InMemory)
            .await
            .unwrap();
    }

    /// openraft's conformance suite against the **persistent** store — every storage
    /// method, now exercised through the disk write/read paths (ADR 0018 phase 2).
    #[tokio::test]
    async fn passes_openraft_conformance_suite_persistent() {
        let builder = OnDisk {
            dir: tempfile::tempdir().unwrap(),
            n: AtomicU64::new(0),
        };
        Suite::<LeaseConfig, LeaseStore, LeaseStore, OnDisk, ()>::test_all(builder)
            .await
            .unwrap();
    }

    /// The persistence claim: a saved vote and an assigned lease survive the store being
    /// closed and reopened — restoring Raft safety (the vote) and the applied state (the
    /// lease) across a restart.
    #[tokio::test]
    async fn vote_and_lease_survive_reopen() -> TestResult {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recover.redb");
        let group: GroupId = 7;

        {
            let mut store = LeaseStore::open(&path)?;
            store.save_vote(&vote(3, 1)).await?;
            // Append + apply a lease assignment for `group` to node 1.
            let entry = Entry {
                log_id: log_id(3, 1, 1),
                payload: EntryPayload::Normal(LeaseRequest::Assign { group, node: 1 }),
            };
            store.blocking_append([entry.clone()]).await?;
            apply(&mut store, vec![entry]).await?;
            assert!(store.current_lease(group).is_some());
        }

        // Reopen: the vote, the log, and the applied lease are all recovered.
        let mut store = LeaseStore::open(&path)?;
        assert_eq!(store.read_vote().await?, Some(vote(3, 1)));
        assert_eq!(store.try_get_log_entries(..).await?.len(), 1);
        let lease = store
            .current_lease(group)
            .expect("the lease survived reopen");
        assert_eq!(lease.holder, 1);
        Ok(())
    }

    /// ADR 0080: a committed replication factor is part of the applied state — it
    /// survives the store being reopened and travels in a built snapshot to a
    /// store that installs it.
    #[tokio::test]
    async fn the_replication_factor_survives_reopen_and_snapshot_install() -> TestResult {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replicas.redb");
        {
            let mut store = LeaseStore::open(&path)?;
            let entry = Entry {
                log_id: log_id(1, 1, 1),
                payload: EntryPayload::Normal(LeaseRequest::SetReplicas { r: 2 }),
            };
            store.blocking_append([entry.clone()]).await?;
            apply(&mut store, vec![entry]).await?;
            assert_eq!(store.replication().replicas, Some(2));
        }
        let mut store = LeaseStore::open(&path)?;
        assert_eq!(store.replication().replicas, Some(2), "survived reopen");

        let snapshot = store.build_snapshot().await?;
        let mut other = LeaseStore::new();
        assert!(other.replication().is_unset());
        other
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await?;
        assert_eq!(
            other.replication().replicas,
            Some(2),
            "travelled in the snapshot"
        );
        Ok(())
    }

    // The bytes mqttd 1.3 (openraft 0.9.25, postcard 1.1.3) wrote into lease.redb for
    // these values, from a program built on those versions with 1.3's lease types.
    const V13_VOTE: &str = "030101";
    const V13_LOG_ID: &str = "030107";
    const V13_MEMBERSHIP: &str = "01030109010201020201026e3102026e32";
    const V13_SNAPSHOT_META: &str = "0103010a01030109010201020201026e3102026e320431302d31";
    const V13_ENTRIES: [&str; 3] = [
        "03010801000901",
        "03010902010201020201026e3102026e32",
        "03010a00",
    ];

    fn entries() -> [Entry; 3] {
        [
            Entry {
                log_id: log_id(3, 1, 8),
                payload: EntryPayload::Normal(LeaseRequest::Assign { group: 9, node: 1 }),
            },
            Entry {
                log_id: log_id(3, 1, 9),
                payload: EntryPayload::Membership(membership()),
            },
            Entry {
                log_id: log_id(3, 1, 10),
                payload: EntryPayload::Blank,
            },
        ]
    }

    /// Every value lease.redb holds reads back from the bytes mqttd 1.3 wrote, and is
    /// written as the same bytes (ADR 0058: one store, opened by either version during
    /// a rolling upgrade or a rollback).
    #[test]
    fn lease_redb_values_keep_the_1_3_bytes() {
        let vote = committed_vote(3, 1);
        assert_eq!(hex(&postcard::to_allocvec(&vote).unwrap()), V13_VOTE);
        assert_eq!(de::<VoteOf<LeaseConfig>>(&unhex(V13_VOTE)).unwrap(), vote);

        let id = log_id(3, 1, 7);
        assert_eq!(hex(&postcard::to_allocvec(&id).unwrap()), V13_LOG_ID);
        assert_eq!(de::<super::LogId>(&unhex(V13_LOG_ID)).unwrap(), id);

        let stored = StoredMembership::new(Some(log_id(3, 1, 9)), membership());
        assert_eq!(
            hex(&postcard::to_allocvec(&stored).unwrap()),
            V13_MEMBERSHIP
        );
        assert_eq!(
            de::<StoredMembershipOf<LeaseConfig>>(&unhex(V13_MEMBERSHIP)).unwrap(),
            stored
        );

        let meta = Meta {
            last_log_id: Some(log_id(3, 1, 10)),
            last_membership: stored,
        };
        // 1.3 named the snapshot "<last applied index>-<snapshot count>"; so does this store.
        assert_eq!(hex(&stored_meta(&meta, 1).unwrap()), V13_SNAPSHOT_META);
        let back = de::<StoredMeta>(&unhex(V13_SNAPSHOT_META)).unwrap();
        assert_eq!(back.snapshot_id, "10-1");
        assert_eq!(back.into_meta(), meta);

        for (entry, bytes) in entries().iter().zip(V13_ENTRIES) {
            assert_eq!(hex(&postcard::to_allocvec(entry).unwrap()), bytes);
            let back: Entry = de(&unhex(bytes)).unwrap();
            assert_eq!(back.log_id, entry.log_id);
            assert_eq!(back.payload, entry.payload);
        }
    }

    /// A lease.redb that mqttd 1.3 left behind, rebuilt from its stored bytes, opens
    /// with its vote, log, applied state and snapshot intact.
    #[tokio::test]
    async fn a_lease_redb_written_by_1_3_opens() -> TestResult {
        use super::{K_LAST_APPLIED, K_LAST_MEMBERSHIP, K_SNAP_DATA, K_SNAP_IDX, K_SNAP_META};
        use super::{K_SM, K_VOTE, LOG, META};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease.redb");
        drop(LeaseStore::open(&path)?); // stamped v1, tables created
        let mut applied = LeaseStore::new();
        apply(&mut applied, entries().to_vec()).await?;
        let lease_map = super::encode_state(&applied.lock().sm)?;
        {
            let db = redb::Database::create(&path)?;
            let txn = db.begin_write()?;
            {
                let mut log = txn.open_table(LOG)?;
                for (index, bytes) in (8..).zip(V13_ENTRIES) {
                    log.insert(index, unhex(bytes).as_slice())?;
                }
                let mut meta = txn.open_table(META)?;
                meta.insert(K_VOTE, unhex(V13_VOTE).as_slice())?;
                meta.insert(K_LAST_APPLIED, unhex("03010a").as_slice())?;
                meta.insert(K_LAST_MEMBERSHIP, unhex(V13_MEMBERSHIP).as_slice())?;
                meta.insert(K_SM, lease_map.as_slice())?;
                meta.insert(K_SNAP_META, unhex(V13_SNAPSHOT_META).as_slice())?;
                meta.insert(K_SNAP_DATA, lease_map.as_slice())?;
                meta.insert(K_SNAP_IDX, [1u8].as_slice())?;
            }
            txn.commit()?;
        }

        let mut store = LeaseStore::open(&path)?;
        assert_eq!(store.read_vote().await?, Some(committed_vote(3, 1)));
        assert_eq!(store.try_get_log_entries(8..11).await?.len(), 3);
        let (applied, stored) = store.applied_state().await?;
        assert_eq!(applied, Some(log_id(3, 1, 10)));
        assert_eq!(stored.log_id(), &Some(log_id(3, 1, 9)));
        assert_eq!(store.current_lease(9).map(|l| l.holder), Some(1));
        let snapshot = store.get_current_snapshot().await?.expect("the snapshot");
        assert_eq!(snapshot.meta.last_log_id, Some(log_id(3, 1, 10)));
        // The next snapshot continues 1.3's count.
        store.build_snapshot().await?;
        assert_eq!(store.lock().snapshot_idx, 2);
        Ok(())
    }
}
