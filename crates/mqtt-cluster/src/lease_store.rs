//! In-memory openraft storage for the lease consensus group
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md), workstream E
//! step 3b-ii).
//!
//! [`LeaseStore`] implements openraft's `RaftStorage` over the [`LeaseMap`] state
//! machine ([`lease_raft`](crate::lease_raft)): the Raft log, the persisted vote,
//! the applied state machine, and snapshots — all in memory. The lease group is
//! tiny and low-traffic (one assignment per ownership change), so an in-memory
//! store is appropriate; a node rebuilds its lease view from peers on restart.
//!
//! It is validated by openraft's own conformance `Suite` (see the tests), which
//! exercises every storage method against the protocol's correctness requirements
//! — far stronger coverage than hand-written cases, and the idiomatic way to trust
//! a Raft store. The network layer that turns this into a live, multi-node group is
//! the next sub-step.

use crate::lease_raft::{GroupId, LeaseConfig, LeaseMap, LeaseRecord, LeaseResponse};
use openraft::{
    Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

type NodeId = u64;

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

/// In-memory openraft store for the lease group. Cheaply cloneable (shared state),
/// so it doubles as its own `LogReader` and `SnapshotBuilder`.
#[derive(Debug, Clone, Default)]
pub struct LeaseStore {
    inner: Arc<Mutex<Inner>>,
}

impl LeaseStore {
    /// A fresh, empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The lease currently assigned to `group` in the applied state machine.
    ///
    /// Reads the committed-and-applied view (what consensus has agreed); for
    /// inspection and for the wiring layer to learn the epoch a `ClusterLog` runs at.
    #[must_use]
    pub fn current_lease(&self, group: GroupId) -> Option<LeaseRecord> {
        self.lock().sm.get(group)
    }
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
        let mut inner = self.lock();

        let data =
            bincode::serialize(&inner.sm).map_err(|e| StorageIOError::read_state_machine(&e))?;
        let last_applied = inner.last_applied;
        let last_membership = inner.last_membership.clone();

        inner.snapshot_idx += 1;
        let snapshot_id = format!(
            "{}-{}",
            last_applied.map_or(0, |l| l.index),
            inner.snapshot_idx
        );
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

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
        self.lock().log.retain(|&index, _| index < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
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
        let mut inner = self.lock();
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            match &entry.payload {
                // A no-op leader entry: nothing to apply.
                EntryPayload::Blank => responses.push(None),
                // A lease assignment (single or batched): drive the state machine,
                // which yields the resulting lease (or `None` for an empty batch).
                EntryPayload::Normal(req) => {
                    let response = inner.sm.apply(req);
                    responses.push(response);
                }
                // A membership change: record it; no business logic.
                EntryPayload::Membership(membership) => {
                    inner.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    responses.push(None);
                }
            }
        }
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

    /// Run openraft's full storage conformance suite against `LeaseStore`. This
    /// exercises every `RaftStorage` method against the protocol's correctness
    /// requirements (log holes, vote persistence, snapshot install, membership
    /// tracking, ...) — the idiomatic, high-coverage way to trust a Raft store.
    #[test]
    fn passes_openraft_conformance_suite() {
        openraft::testing::Suite::test_all(|| async { LeaseStore::new() }).unwrap();
    }
}
