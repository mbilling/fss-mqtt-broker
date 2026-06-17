//! Epoch-fenced, quorum-replicated [`ReplicatedLog`] — the durable backend's core
//! ([ADR 0006](../../../docs/adr/0006-consensus-and-replication.md) §1, workstream
//! E step 3).
//!
//! ADR 0006 scopes consensus to the *ownership lease* (rare, low-traffic — managed
//! by openraft, workstream E step 3b). The per-session append-log itself is **not**
//! pushed through per-entry consensus; the lease-holder replicates it by
//! **epoch-fenced quorum-append** — one quorum round-trip per append, never a leader
//! election per entry. That is what this module implements, and it is *our* code on
//! top of the engine's leadership term, not the engine's.
//!
//! It is sans-I/O: the leader ([`ClusterLog`]) talks to replicas only through the
//! [`ReplicaTransport`] seam, and the follower side ([`ReplicaState`]) is a pure
//! apply function. Tests drive a deterministic in-memory transport with injectable
//! reachability (the SWIM-sim discipline), so the durability contract is pinned
//! without a network. Step 3b supplies the real transport over the mTLS peer mesh.
//!
//! ## Durability contract (what the tests pin)
//!
//! - [`append`](ClusterLog::append) returns `Ok(offset)` **only** once the record is
//!   durable on a quorum (leader + enough followers). At R=3 / quorum=2 it survives
//!   one replica loss; below quorum it returns [`ReplError::NoQuorum`] and the
//!   producer's QoS≥1 PUBACK must not be released.
//! - A superseded lease-holder is **fenced**: followers that moved to a newer epoch
//!   reject its appends, so it cannot reach quorum and cannot diverge the log.
//! - A failed append does **not** advance the commit watermark; the next append
//!   retries at the same offset (no committed holes). Any minority-stored orphan is
//!   reconciled at takeover (workstream F).
//! - [`truncate`](ClusterLog::truncate) is local-first and lazy — propagated
//!   best-effort, never gated on a cross-node round-trip.

use crate::lease::{Epoch, OwnershipLease};
use crate::NodeId;
use async_trait::async_trait;
use mqtt_storage::repl::{LogEntry, ReplError, ReplicatedLog};
use mqtt_storage::Offset;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included};

/// A replication operation the lease-holder ships to a replica. Carried with the
/// holder's [`Epoch`] so the replica can fence a stale holder.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReplOp {
    /// Store `record` at the leader-assigned `offset` in `key`'s log.
    Append {
        /// The log this entry belongs to.
        key: String,
        /// The leader-assigned offset.
        offset: Offset,
        /// The opaque record bytes.
        record: Vec<u8>,
    },
    /// Drop `key`'s entries with offset `<= up_to`.
    Truncate {
        /// The log to truncate.
        key: String,
        /// Inclusive high offset to drop up to.
        up_to: Offset,
    },
    /// Drop `key`'s log entirely.
    Remove {
        /// The log to remove.
        key: String,
    },
}

/// The follower side of replication: a replica's stored copy plus its fence epoch.
///
/// Pure — [`apply`](ReplicaState::apply) is the entire follower protocol. The real
/// transport (step 3b) calls it on the receiving node; the test transport calls it
/// in-process.
#[derive(Debug, Default)]
pub struct ReplicaState {
    fence: Epoch,
    logs: BTreeMap<String, BTreeMap<Offset, Vec<u8>>>,
}

impl ReplicaState {
    /// A fresh, empty replica.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The highest leadership epoch this replica has acknowledged.
    #[must_use]
    pub fn fence(&self) -> Epoch {
        self.fence
    }

    /// Apply a lease-holder's `op` sent at `epoch`.
    ///
    /// Returns `false` (fenced) without mutating if `epoch` is stale (`<` the
    /// replica's acknowledged epoch). Otherwise the replica learns `epoch`
    /// (monotonically) and applies the op, returning `true`.
    pub fn apply(&mut self, epoch: Epoch, op: &ReplOp) -> bool {
        if epoch < self.fence {
            return false;
        }
        self.fence = epoch;
        match op {
            ReplOp::Append {
                key,
                offset,
                record,
            } => {
                self.logs
                    .entry(key.clone())
                    .or_default()
                    .insert(*offset, record.clone());
            }
            ReplOp::Truncate { key, up_to } => {
                if let Some(log) = self.logs.get_mut(key) {
                    log.retain(|o, _| o > up_to);
                }
            }
            ReplOp::Remove { key } => {
                self.logs.remove(key);
            }
        }
        true
    }

    /// This replica's stored entries for `key`, in offset order (for takeover /
    /// tests). Followers store what they are sent; commit is the leader's notion.
    #[must_use]
    pub fn entries(&self, key: &str) -> Vec<LogEntry> {
        self.logs
            .get(key)
            .map(|log| {
                log.iter()
                    .map(|(offset, record)| LogEntry {
                        offset: *offset,
                        record: record.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Sends replication ops from the lease-holder to its followers.
///
/// The seam between the sans-I/O quorum logic and the wire. A `deliver` returns
/// `true` iff the replica accepted (was reachable and not fenced); the leader
/// counts acks to decide quorum. Best-effort: an unreachable replica is a `false`,
/// not an error.
#[async_trait]
pub trait ReplicaTransport: Send + Sync {
    /// Deliver `op` to `replica` at `epoch`; return whether it accepted.
    async fn deliver(&self, replica: &NodeId, epoch: Epoch, op: &ReplOp) -> bool;

    /// Read `replica`'s stored log for `key`, for a new owner to rebuild the
    /// committed log on takeover (workstream F). Returns `None` if the replica is
    /// unreachable. The default supports no recovery-reads (single-node transports).
    async fn read_replica(&self, _replica: &NodeId, _key: &str) -> Option<Vec<LogEntry>> {
        None
    }
}

/// Forward through an [`Arc`](std::sync::Arc) so a test can hold the transport to
/// inject reachability while the [`ClusterLog`] also owns it.
#[async_trait]
impl<T: ReplicaTransport + ?Sized> ReplicaTransport for std::sync::Arc<T> {
    async fn deliver(&self, replica: &NodeId, epoch: Epoch, op: &ReplOp) -> bool {
        (**self).deliver(replica, epoch, op).await
    }

    async fn read_replica(&self, replica: &NodeId, key: &str) -> Option<Vec<LogEntry>> {
        (**self).read_replica(replica, key).await
    }
}

/// The leader's per-key state: its own copy of the log and the commit watermark.
#[derive(Debug, Default)]
struct KeyState {
    /// Leader's copy (may hold an uncommitted tail at `committed + 1`).
    entries: BTreeMap<Offset, Vec<u8>>,
    /// Highest quorum-durable offset. Reads never expose beyond this.
    committed: Offset,
    /// Entries with offset `<= truncated` have been dropped.
    truncated: Offset,
}

/// The lease-holder's quorum-replicated [`ReplicatedLog`].
///
/// Constructed for the node that currently holds the group's [`OwnershipLease`];
/// it assigns offsets and quorum-replicates each append across the replica set
/// before committing. See the module docs for the contract.
pub struct ClusterLog<T: ReplicaTransport> {
    local: NodeId,
    lease: OwnershipLease,
    followers: Vec<NodeId>,
    quorum: usize,
    transport: T,
    state: tokio::sync::Mutex<BTreeMap<String, KeyState>>,
}

// Manual Debug so the transport `T` need not be `Debug` (the trait requires
// `ClusterLog` itself to be); the transport is opaque to the log's identity.
impl<T: ReplicaTransport> std::fmt::Debug for ClusterLog<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterLog")
            .field("local", &self.local)
            .field("lease", &self.lease)
            .field("followers", &self.followers)
            .field("quorum", &self.quorum)
            .finish_non_exhaustive()
    }
}

impl<T: ReplicaTransport> ClusterLog<T> {
    /// A log for `lease.holder` (which must be `local`) over `replica_set` (which
    /// includes `local`), with a majority quorum.
    ///
    /// # Panics
    /// Panics if `local` is not in `replica_set`, or is not the lease-holder — a
    /// `ClusterLog` only exists on the owner.
    #[must_use]
    pub fn new(local: NodeId, lease: OwnershipLease, replica_set: &[NodeId], transport: T) -> Self {
        assert!(
            replica_set.contains(&local),
            "the local node must be in its own replica set"
        );
        assert_eq!(
            lease.holder, local,
            "a ClusterLog is the lease-holder's log"
        );
        let quorum = replica_set.len() / 2 + 1;
        let followers = replica_set
            .iter()
            .filter(|n| **n != local)
            .cloned()
            .collect();
        Self {
            local,
            lease,
            followers,
            quorum,
            transport,
            state: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }

    /// A log whose committed state is **recovered** from `logs` — per key, the
    /// entries a new owner gathered from a quorum of the replica set after a
    /// takeover (workstream F). Each key's commit watermark is the highest recovered
    /// offset and the truncation low-water is just below the lowest, so reads return
    /// exactly the recovered live range and appends continue from the watermark.
    ///
    /// # Panics
    /// As [`new`](Self::new).
    #[must_use]
    pub fn recovered(
        local: NodeId,
        lease: OwnershipLease,
        replica_set: &[NodeId],
        transport: T,
        logs: BTreeMap<String, Vec<LogEntry>>,
    ) -> Self {
        assert!(
            replica_set.contains(&local),
            "the local node must be in its own replica set"
        );
        assert_eq!(
            lease.holder, local,
            "a ClusterLog is the lease-holder's log"
        );
        let quorum = replica_set.len() / 2 + 1;
        let followers = replica_set
            .iter()
            .filter(|n| **n != local)
            .cloned()
            .collect();
        let mut state = BTreeMap::new();
        for (key, entries) in logs {
            let mut ks = KeyState::default();
            let mut lowest: Option<Offset> = None;
            for entry in entries {
                lowest = Some(lowest.map_or(entry.offset, |l| l.min(entry.offset)));
                ks.committed = ks.committed.max(entry.offset);
                ks.entries.insert(entry.offset, entry.record);
            }
            ks.truncated = lowest.map_or(0, |l| l.saturating_sub(1));
            state.insert(key, ks);
        }
        Self {
            local,
            lease,
            followers,
            quorum,
            transport,
            state: tokio::sync::Mutex::new(state),
        }
    }

    /// The quorum size for this group.
    #[must_use]
    pub fn quorum(&self) -> usize {
        self.quorum
    }

    /// The leadership epoch this log writes at (its lease's epoch).
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.lease.epoch
    }

    /// Seed `key`'s committed state from `entries` recovered from a quorum of
    /// replicas on takeover (workstream F). Idempotent and non-clobbering: only a
    /// key with no state yet is seeded, so a re-recovery or a concurrent builder is
    /// a no-op. Appends then continue from the recovered watermark.
    pub async fn seed_key(&self, key: &str, entries: Vec<LogEntry>) {
        if entries.is_empty() {
            return;
        }
        let mut state = self.state.lock().await;
        let ks = state.entry(key.to_string()).or_default();
        if !ks.entries.is_empty() || ks.committed != 0 {
            return; // already has state — do not clobber
        }
        let mut lowest: Option<Offset> = None;
        for entry in entries {
            lowest = Some(lowest.map_or(entry.offset, |l| l.min(entry.offset)));
            ks.committed = ks.committed.max(entry.offset);
            ks.entries.insert(entry.offset, entry.record);
        }
        ks.truncated = lowest.map_or(0, |l| l.saturating_sub(1));
    }
}

/// Merge per-replica reads of one key's log into its recovered committed log: the
/// union of entries by offset, then the contiguous run from the lowest offset
/// present, stopping at the first gap.
///
/// A gap marks an uncommitted tail: the owner commits offsets in order, so it cannot
/// have committed past a missing offset; and reading from a **quorum** guarantees
/// every committed entry is seen (any committed entry is on ≥ quorum replicas, which
/// intersect any quorum read). A truncated prefix (acked, dropped) simply means the
/// run starts above 1.
#[must_use]
pub fn merge_replica_logs(reads: &[Vec<LogEntry>]) -> Vec<LogEntry> {
    let mut by_offset: BTreeMap<Offset, Vec<u8>> = BTreeMap::new();
    for read in reads {
        for entry in read {
            by_offset
                .entry(entry.offset)
                .or_insert_with(|| entry.record.clone());
        }
    }
    let mut out = Vec::new();
    let mut expected: Option<Offset> = None;
    for (offset, record) in by_offset {
        if let Some(e) = expected {
            if offset != e {
                break; // a gap — the rest is an uncommitted tail
            }
        }
        expected = Some(offset + 1);
        out.push(LogEntry { offset, record });
    }
    out
}

#[async_trait]
impl<T: ReplicaTransport + Clone + 'static> ReplicatedLog for ClusterLog<T> {
    type Key = String;

    async fn append(&self, key: &String, record: Vec<u8>) -> Result<Offset, ReplError> {
        let mut state = self.state.lock().await;
        let ks = state.entry(key.clone()).or_default();
        // Offset is committed + 1: a failed append leaves no committed hole, and a
        // retry reuses the same offset (idempotent on any follower that stored it).
        let offset = ks.committed + 1;
        ks.entries.insert(offset, record.clone());

        let op = ReplOp::Append {
            key: key.clone(),
            offset,
            record,
        };

        // Fan out to every follower **concurrently** and commit as soon as a quorum
        // has accepted — the leader's own copy is one ack. A slow or wedged replica
        // no longer serializes the append: once quorum is met the remaining
        // deliveries are abandoned (their frames were already sent, so a reachable
        // replica still applies them for best-effort spread; the transport reaps the
        // in-flight entry on timeout). Any committed entry is therefore on ≥ quorum
        // replicas, which a quorum recovery-read is guaranteed to intersect.
        let mut acks = 1usize;
        if acks < self.quorum {
            let mut inflight = tokio::task::JoinSet::new();
            for follower in &self.followers {
                let transport = self.transport.clone();
                let follower = follower.clone();
                let epoch = self.lease.epoch;
                let op = op.clone();
                inflight.spawn(async move { transport.deliver(&follower, epoch, &op).await });
            }
            while acks < self.quorum {
                match inflight.join_next().await {
                    Some(Ok(true)) => acks += 1,
                    // A reject/unreachable, or a delivery task that failed — keep
                    // waiting for the other followers.
                    Some(Ok(false) | Err(_)) => {}
                    // Every follower has reported; quorum was not reached.
                    None => break,
                }
            }
        }

        if acks >= self.quorum {
            ks.committed = offset;
            Ok(offset)
        } else {
            // Not durable: do not advance the commit watermark. The uncommitted
            // entry stays in the leader's copy (invisible to reads, which gate on
            // `committed`) and is overwritten when the next append reuses `offset`.
            Err(ReplError::NoQuorum)
        }
    }

    async fn read(
        &self,
        key: &String,
        after: Offset,
        limit: usize,
    ) -> Result<Vec<LogEntry>, ReplError> {
        let state = self.state.lock().await;
        let Some(ks) = state.get(key) else {
            return Ok(Vec::new());
        };
        // Only committed entries are visible, and only above `after`.
        Ok(ks
            .entries
            .range((Excluded(after), Included(ks.committed)))
            .take(limit)
            .map(|(offset, record)| LogEntry {
                offset: *offset,
                record: record.clone(),
            })
            .collect())
    }

    async fn truncate(&self, key: &String, up_to: Offset) -> Result<(), ReplError> {
        let mut state = self.state.lock().await;
        let mut op = None;
        if let Some(ks) = state.get_mut(key) {
            // Never truncate past the commit watermark.
            let up = up_to.min(ks.committed);
            ks.entries.retain(|o, _| *o > up);
            ks.truncated = ks.truncated.max(up);
            op = Some(ReplOp::Truncate {
                key: key.clone(),
                up_to: up,
            });
        }
        drop(state);
        // Local-first and lazy: propagate best-effort, do not gate on acks.
        if let Some(op) = op {
            for follower in &self.followers {
                let _ = self
                    .transport
                    .deliver(follower, self.lease.epoch, &op)
                    .await;
            }
        }
        Ok(())
    }

    async fn remove(&self, key: &String) -> Result<(), ReplError> {
        {
            let mut state = self.state.lock().await;
            state.remove(key);
        }
        let op = ReplOp::Remove { key: key.clone() };
        for follower in &self.followers {
            let _ = self
                .transport
                .deliver(follower, self.lease.epoch, &op)
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{merge_replica_logs, ClusterLog, ReplOp, ReplicaState, ReplicaTransport};
    use crate::lease::{Epoch, OwnershipLease};
    use crate::NodeId;
    use async_trait::async_trait;
    use mqtt_storage::logged::ReplicatedSessionStore;
    use mqtt_storage::repl::{LogEntry, ReplicatedLog};
    use mqtt_storage::SessionStore;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex};

    fn n(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    /// Deterministic in-process transport holding the follower replicas, with an
    /// injectable reachable-set (partition / kill a replica).
    #[derive(Debug)]
    struct SimCluster {
        replicas: Mutex<BTreeMap<NodeId, ReplicaState>>,
        reachable: Mutex<BTreeSet<NodeId>>,
    }

    impl SimCluster {
        fn new(followers: &[NodeId]) -> Arc<Self> {
            let replicas = followers
                .iter()
                .map(|f| (f.clone(), ReplicaState::new()))
                .collect();
            let reachable = followers.iter().cloned().collect();
            Arc::new(Self {
                replicas: Mutex::new(replicas),
                reachable: Mutex::new(reachable),
            })
        }

        /// Take `node` offline (its deliveries now fail) — a crash or partition.
        fn down(&self, node: &NodeId) {
            self.reachable.lock().unwrap().remove(node);
        }

        /// Stored entries on a replica (for assertions).
        fn entries(&self, node: &NodeId, key: &str) -> Vec<u64> {
            self.replicas
                .lock()
                .unwrap()
                .get(node)
                .map(|r| r.entries(key).into_iter().map(|e| e.offset).collect())
                .unwrap_or_default()
        }
    }

    #[async_trait]
    impl ReplicaTransport for SimCluster {
        async fn deliver(&self, replica: &NodeId, epoch: Epoch, op: &ReplOp) -> bool {
            if !self.reachable.lock().unwrap().contains(replica) {
                return false; // unreachable
            }
            let mut replicas = self.replicas.lock().unwrap();
            replicas
                .get_mut(replica)
                .is_some_and(|r| r.apply(epoch, op))
        }
    }

    /// R=3, quorum=2: a leader plus two followers, the leader being the holder.
    fn group(epoch: Epoch) -> (ClusterLog<Arc<SimCluster>>, Arc<SimCluster>, Vec<NodeId>) {
        let local = n("a");
        let followers = vec![n("b"), n("c")];
        let set = vec![n("a"), n("b"), n("c")];
        let sim = SimCluster::new(&followers);
        let lease = OwnershipLease {
            holder: local.clone(),
            epoch,
        };
        let log = ClusterLog::new(local, lease, &set, sim.clone());
        (log, sim, followers)
    }

    #[tokio::test]
    async fn append_is_quorum_durable_and_assigns_offsets() {
        let (log, _sim, _f) = group(1);
        let k = "x".to_string();
        assert_eq!(log.quorum(), 2);
        assert_eq!(log.append(&k, b"0".to_vec()).await.unwrap(), 1);
        assert_eq!(log.append(&k, b"1".to_vec()).await.unwrap(), 2);
        let all = log.read(&k, 0, 100).await.unwrap();
        assert_eq!(all.iter().map(|e| e.offset).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(&all[0].record, b"0");
    }

    /// The headline durability test: with one of three replicas down, the leader
    /// plus the surviving follower still form a quorum, so the append commits.
    #[tokio::test]
    async fn append_survives_single_replica_loss() {
        let (log, sim, followers) = group(1);
        let k = "x".to_string();
        sim.down(&followers[0]); // b is gone; {a (leader), c} remain = quorum 2
        let off = log.append(&k, b"hi".to_vec()).await.unwrap();
        assert_eq!(off, 1);
        // The surviving follower has it; reads see it.
        assert_eq!(sim.entries(&followers[1], &k), vec![1]);
        assert_eq!(log.read(&k, 0, 100).await.unwrap().len(), 1);
    }

    /// Below quorum (two of three down) the append is rejected and leaves no
    /// committed entry; the next append, once quorum returns, reuses the offset.
    #[tokio::test]
    async fn append_below_quorum_is_rejected_and_leaves_no_committed_hole() {
        let (log, sim, followers) = group(1);
        let k = "x".to_string();
        sim.down(&followers[0]);
        sim.down(&followers[1]); // only the leader remains: 1 < quorum 2
        assert!(matches!(
            log.append(&k, b"lost".to_vec()).await,
            Err(mqtt_storage::repl::ReplError::NoQuorum)
        ));
        // Nothing committed — reads are empty.
        assert!(log.read(&k, 0, 100).await.unwrap().is_empty());
        // Quorum returns; the retry reuses offset 1 (no hole).
        // (bring one follower back)
        sim.reachable.lock().unwrap().insert(followers[1].clone());
        assert_eq!(log.append(&k, b"kept".to_vec()).await.unwrap(), 1);
        let all = log.read(&k, 0, 100).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(&all[0].record, b"kept");
    }

    /// A wedged follower (its delivery never completes — a half-open link) does not
    /// block an append: the leader plus the healthy follower form a quorum, so the
    /// append commits promptly and the stuck delivery is abandoned. With the old
    /// sequential fan-out this would hang on the wedged replica.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn append_commits_at_quorum_without_waiting_on_a_wedged_replica() {
        /// Delivers instantly to every replica except `wedged`, whose delivery never
        /// resolves.
        #[derive(Clone)]
        struct WedgedFor {
            wedged: NodeId,
        }
        #[async_trait]
        impl ReplicaTransport for WedgedFor {
            async fn deliver(&self, replica: &NodeId, _epoch: Epoch, _op: &ReplOp) -> bool {
                if *replica == self.wedged {
                    std::future::pending::<()>().await;
                }
                true
            }
        }

        let local = n("a");
        let set = vec![n("a"), n("b"), n("c")]; // R=3, quorum=2
        let lease = OwnershipLease {
            holder: local.clone(),
            epoch: 1,
        };
        // b is wedged; the leader + c still make quorum.
        let log = ClusterLog::new(local, lease, &set, WedgedFor { wedged: n("b") });

        let k = "x".to_string();
        let appended = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            log.append(&k, b"v".to_vec()),
        )
        .await
        .expect("append must not hang on the wedged replica");
        assert_eq!(appended.unwrap(), 1);
        assert_eq!(log.read(&k, 0, 100).await.unwrap().len(), 1);
    }

    /// A superseded lease-holder is fenced: once a quorum of followers has moved to
    /// a newer epoch, the old holder cannot commit.
    #[tokio::test]
    async fn stale_leader_is_fenced() {
        let local = n("a");
        let set = vec![n("a"), n("b"), n("c")];
        let followers = vec![n("b"), n("c")];
        let sim = SimCluster::new(&followers);

        // The new leader (epoch 2) commits first, advancing the followers' fence.
        let new = ClusterLog::new(
            local.clone(),
            OwnershipLease {
                holder: local.clone(),
                epoch: 2,
            },
            &set,
            sim.clone(),
        );
        let k = "x".to_string();
        new.append(&k, b"new".to_vec()).await.unwrap();

        // The stale leader (epoch 1) cannot reach quorum: both followers reject it.
        let stale = ClusterLog::new(
            local.clone(),
            OwnershipLease {
                holder: local,
                epoch: 1,
            },
            &set,
            sim.clone(),
        );
        assert!(matches!(
            stale.append(&k, b"stale".to_vec()).await,
            Err(mqtt_storage::repl::ReplError::NoQuorum)
        ));
    }

    #[tokio::test]
    async fn truncate_is_local_first_and_propagates() {
        let (log, sim, followers) = group(1);
        let k = "x".to_string();
        for _ in 0..5 {
            log.append(&k, b"x".to_vec()).await.unwrap();
        }
        log.truncate(&k, 2).await.unwrap();
        assert_eq!(
            log.read(&k, 0, 100)
                .await
                .unwrap()
                .iter()
                .map(|e| e.offset)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        // Followers received the truncate too.
        assert_eq!(sim.entries(&followers[0], &k), vec![3, 4, 5]);
    }

    /// Truncate still succeeds (local-first, lazy) when all followers are down — it
    /// never gates on a cross-node round-trip.
    #[tokio::test]
    async fn truncate_succeeds_with_followers_down() {
        let (log, sim, followers) = group(1);
        let k = "x".to_string();
        for _ in 0..3 {
            log.append(&k, b"x".to_vec()).await.unwrap();
        }
        sim.down(&followers[0]);
        sim.down(&followers[1]);
        assert!(log.truncate(&k, 2).await.is_ok());
        assert_eq!(log.read(&k, 0, 100).await.unwrap().len(), 1);
    }

    /// End to end: the step-2 `ReplicatedSessionStore` runs unchanged over the
    /// quorum-replicated cluster log, and an enqueue survives one replica down —
    /// durable sessions, the whole stack composed.
    #[tokio::test]
    async fn session_store_over_cluster_log_survives_replica_loss() {
        use mqtt_core::{ClientId, Message, QoS};
        let (log, sim, followers) = group(1);
        sim.down(&followers[0]); // one replica down, quorum still met

        let store = ReplicatedSessionStore::new(log);
        let c = ClientId("client".to_string());
        let msg = Message {
            topic: "t".to_string(),
            payload: bytes::Bytes::from_static(b"payload"),
            qos: QoS::AtLeastOnce,
            retain: false,
        };
        store.ensure_session(&c).await.unwrap();
        store.enqueue(&c, &msg).await.unwrap();

        let pending = store.pending(&c, 0, 100).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0].message.payload[..], b"payload");
    }

    fn entry(offset: u64, record: &[u8]) -> LogEntry {
        LogEntry {
            offset,
            record: record.to_vec(),
        }
    }

    /// The quorum-recovery merge takes the contiguous run from the union of reads.
    #[test]
    fn merge_takes_the_contiguous_run_from_a_quorum() {
        // Two overlapping replica reads; their union is contiguous 1..=4.
        let merged = merge_replica_logs(&[
            vec![entry(1, b"a"), entry(2, b"b"), entry(3, b"c")],
            vec![entry(2, b"b"), entry(3, b"c"), entry(4, b"d")],
        ]);
        assert_eq!(
            merged.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    /// A gap drops the uncommitted tail beyond it; a truncated prefix starts above
    /// 1; nothing merges to nothing.
    #[test]
    fn merge_stops_at_gaps_and_handles_truncation() {
        // 1,2 then a gap at 3 with an uncommitted 4 → recovered [1,2].
        assert_eq!(
            merge_replica_logs(&[vec![entry(1, b"a"), entry(2, b"b"), entry(4, b"d")]])
                .iter()
                .map(|e| e.offset)
                .collect::<Vec<_>>(),
            vec![1, 2],
        );
        // Truncated to start at 5 (acked) → recovered [5,6].
        assert_eq!(
            merge_replica_logs(&[vec![entry(5, b"e"), entry(6, b"f")]])
                .iter()
                .map(|e| e.offset)
                .collect::<Vec<_>>(),
            vec![5, 6],
        );
        assert!(merge_replica_logs(&[]).is_empty());
        assert!(merge_replica_logs(&[vec![]]).is_empty());
    }

    /// A recovered `ClusterLog` serves its seeded committed entries and continues
    /// appending after the recovered watermark — the takeover rebuild (workstream F).
    #[tokio::test]
    async fn recovered_log_serves_seeded_entries_and_continues() {
        let local = n("a");
        let set = vec![n("a")]; // 1-node group, quorum 1
        let sim = SimCluster::new(&[]);
        let lease = OwnershipLease {
            holder: local.clone(),
            epoch: 5,
        };
        let mut logs = BTreeMap::new();
        logs.insert("q/c".to_string(), vec![entry(1, b"m1"), entry(2, b"m2")]);
        let log = ClusterLog::recovered(local, lease, &set, sim, logs);

        let k = "q/c".to_string();
        // The recovered entries replay.
        let got = log.read(&k, 0, 100).await.unwrap();
        assert_eq!(got.iter().map(|e| e.offset).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(&got[1].record, b"m2");
        // Appends continue after the recovered watermark, committing locally.
        assert_eq!(log.append(&k, b"m3".to_vec()).await.unwrap(), 3);
        assert_eq!(log.read(&k, 0, 100).await.unwrap().len(), 3);
    }
}
