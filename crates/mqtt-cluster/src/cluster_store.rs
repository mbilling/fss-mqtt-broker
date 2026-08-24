//! The durable cluster session store: assembling lease → epoch → per-group
//! `ClusterLog` → `ReplicatedSessionStore`
//! ([ADR 0007](../../../docs/adr/0007-durable-store-integration.md) §1/§3,
//! workstream E step 4e).
//!
//! A [`ReplicatedSessionStore`](mqtt_storage::logged::ReplicatedSessionStore) runs
//! over a single [`ReplicatedLog`], but every *group* has its own
//! [`ClusterLog`](crate::cluster_log::ClusterLog) — a different epoch and replica
//! set. [`GroupRoutedLog`] bridges the two: it implements `ReplicatedLog` and routes
//! each key to its **group's** `ClusterLog`, building that log lazily the first time
//! the group is touched (acquiring the group's ownership lease then). Wrap it in a
//! `ReplicatedSessionStore` and the result is a durable, consensus-backed
//! `SessionStore` — the headline ADR 0001 guarantee, assembled.
//!
//! - **group routing**: a key `q/<client>` / `m/<client>` → `group_of(client)` →
//!   that group's `ClusterLog`. All of one client's keys (and all clients in a
//!   group) share one log, one lease, one replica set.
//! - **lazy lease acquisition**: on first touch of an owned group, the
//!   [`LeaseSource`] yields the group's epoch; the `ClusterLog` is built at that
//!   epoch over the group's replica set and cached. A key whose group this node does
//!   **not** own returns [`ReplError::NotOwner`] — the session should have been
//!   relocated to the owner ([ADR 0005](0005-session-affinity.md)).
//! - **the lease is abstracted** ([`LeaseSource`]) so this layer is exercised
//!   without standing up the consensus group; the openraft-backed source is wired in
//!   at step 4f.

use crate::cluster_log::{merge_replica_logs, ClusterLog, ReplicaState, ReplicaTransport};
use crate::lease::{Epoch, OwnershipLease};
use crate::lease_raft::{GroupId, RaftNodeId};
use crate::lease_store::LeaseStore;
use crate::placement::{group_of_key, Placement};
use crate::NodeId;
use async_trait::async_trait;
use mqtt_storage::repl::{DurabilityTier, LogEntry, PendingAppend, ReplError, ReplicatedLog};
use mqtt_storage::Offset;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock};

/// Yields the current ownership-lease epoch for a group this node owns.
///
/// The caller has already confirmed ownership; the source establishes/refreshes the
/// lease (via the consensus group, step 4f) and returns its epoch. Abstracted so the
/// store layer is testable without the lease group.
#[async_trait]
pub trait LeaseSource: Send + Sync {
    /// The current lease epoch for `group` (this node is the owner).
    ///
    /// # Errors
    /// [`ReplError`] if the lease cannot be acquired (no quorum / not owner).
    async fn epoch_for(&self, group: GroupId) -> Result<Epoch, ReplError>;
}

/// The production [`LeaseSource`]: reads the group's lease epoch from this node's
/// own replicated [`LeaseStore`].
///
/// Per ADR 0007, leases are **leader-driven**: the lease-group leader assigns each
/// group's lease to the group's placement owner (the lease state machine mints the
/// epoch). That assignment replicates to every node, so the owner reads its epoch
/// straight from its applied `LeaseStore` — no app-level forwarding of a write to
/// the leader. A group whose lease has not (yet) been assigned to this node returns
/// [`ReplError::NotOwner`] (the caller serves it ephemerally until the leader
/// assigns it — ADR 0005 degrade-don't-refuse).
#[derive(Debug, Clone)]
pub struct LocalLeaseSource {
    store: LeaseStore,
    local: RaftNodeId,
}

impl LocalLeaseSource {
    /// A source reading `store` (this node's lease map) for leases held by `local`.
    #[must_use]
    pub fn new(store: LeaseStore, local: RaftNodeId) -> Self {
        Self { store, local }
    }
}

#[async_trait]
impl LeaseSource for LocalLeaseSource {
    async fn epoch_for(&self, group: GroupId) -> Result<Epoch, ReplError> {
        let current = self.store.current_lease(group);
        match &current {
            // The lease is assigned to us — return the epoch we hold it at.
            Some(rec) if rec.holder == self.local => Ok(rec.epoch),
            // Assigned to another node, or not yet assigned: we cannot write durably.
            // This path is only reached AFTER `owns_group` passed (the placement ring
            // says this node owns the group), so a refusal here is the ring/lease split
            // (2026-07-20 post-mortem). Expected only transiently during convergence;
            // debug-logs who actually holds it (or that it is unassigned).
            _ => {
                tracing::debug!(
                    group,
                    local = self.local,
                    lease_holder = ?current.as_ref().map(|r| r.holder),
                    lease_epoch = ?current.as_ref().map(|r| r.epoch),
                    "lease store refuses group epoch to its placement-ring owner"
                );
                Err(ReplError::NotOwner)
            }
        }
    }
}

/// A group's cached `ClusterLog` together with the keys already recovered against
/// it. The recovery markers live **with** the log so that rebuilding the log on an
/// epoch change (below) atomically resets recovery for the whole group — every key
/// must re-recover against the new lease's replica set, not trust a marker set under
/// the old epoch.
struct GroupEntry<T: ReplicaTransport> {
    log: Arc<ClusterLog<T>>,
    recovered: Mutex<BTreeSet<String>>,
    /// The replica set the log was built over. A membership change that moves the
    /// group's replicas (ADR 0043 P1) rebuilds the entry so appends and re-commits
    /// fan out to the **current** set — a cached log pinned to the old set would
    /// never deliver to a joiner, leaving it hollow forever.
    replica_set: Vec<NodeId>,
}

/// A [`ReplicatedLog`] that routes each key to its placement group's
/// [`ClusterLog`], building (and caching) that log lazily on first touch,
/// **recovering** each key from a quorum of replicas the first time it is served
/// after a takeover (workstream F), and **rebuilding** the cached log when the
/// group's lease epoch advances (ownership lost and regained — the stale log would
/// self-fence forever).
pub struct GroupRoutedLog<S: LeaseSource, T: ReplicaTransport + Clone + 'static> {
    local: NodeId,
    placement: Arc<RwLock<Placement>>,
    transport: T,
    leases: S,
    /// This node's own follower copy of the session logs (shared with the
    /// `DurablePlane`). On takeover this node was a replica, so its committed copy of
    /// a key lives here — it is one of the quorum reads recovery merges.
    local_replicas: Arc<Mutex<ReplicaState>>,
    /// Per-group cached log + recovery markers, built lazily and rebuilt when the
    /// lease epoch advances. Cached so a group's offset state is stable across calls.
    groups: Mutex<BTreeMap<GroupId, Arc<GroupEntry<T>>>>,
    /// The node-wide durable-write serializer (ADR 0071), handed to every group's
    /// `ClusterLog` so owner local acks group-commit with each other (and with the
    /// follower's replica applies) instead of paying one fsync each.
    owner_writer: Option<tokio::sync::mpsc::UnboundedSender<crate::cluster_log::DurableWrite>>,
}

impl<S: LeaseSource, T: ReplicaTransport + Clone + 'static> std::fmt::Debug
    for GroupRoutedLog<S, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupRoutedLog")
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

impl<S: LeaseSource, T: ReplicaTransport + Clone + 'static> GroupRoutedLog<S, T> {
    /// Build a group-routed log for `local`, resolving ownership/replica-sets from
    /// `placement`, replicating over `transport`, acquiring leases from `leases`, and
    /// recovering from `local_replicas` + peer reads on takeover.
    #[must_use]
    pub fn new(
        local: NodeId,
        placement: Arc<RwLock<Placement>>,
        transport: T,
        leases: S,
        local_replicas: Arc<Mutex<ReplicaState>>,
    ) -> Self {
        Self {
            local,
            placement,
            transport,
            leases,
            local_replicas,
            groups: Mutex::new(BTreeMap::new()),
            owner_writer: None,
        }
    }

    /// Route every group's owner-side local acks through the node-wide
    /// durable-write serializer (ADR 0071) — see [`ClusterLog::with_owner_writer`].
    #[must_use]
    pub fn with_owner_writer(
        mut self,
        writer: tokio::sync::mpsc::UnboundedSender<crate::cluster_log::DurableWrite>,
    ) -> Self {
        self.owner_writer = Some(writer);
        self
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, BTreeMap<GroupId, Arc<GroupEntry<T>>>> {
        self.groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The `ClusterLog` for `key`'s group, built and recovered lazily. Errors with
    /// [`ReplError::NotOwner`] if this node does not own the group, or
    /// [`ReplError::NoQuorum`] if recovery cannot reach a quorum of replicas.
    ///
    /// `gate_write_floor` applies the min-replicas write floor (issues #167, #239) to the
    /// replica set this call resolves — set only by [`ReplicatedLog::append`], the one
    /// operation that makes a NEW durability promise. It lives here, rather than in
    /// `append`, so the floor is judged against the very set the returned `ClusterLog` is
    /// built over, read under a SINGLE placement-lock acquisition: checking the floor
    /// under one guard and rebuilding the set under another let a membership shrink land
    /// between them and commit the append over the smaller set anyway.
    async fn log_for_key(
        &self,
        key: &str,
        gate_write_floor: bool,
    ) -> Result<Arc<ClusterLog<T>>, ReplError> {
        // Keys carry a 2-byte kind prefix (`q/`/`m/`/`r/`) ahead of the placement key.
        let group = group_of_key(key);

        // Resolve ownership + replica set without holding a lock across an await.
        // Capture an ownership snapshot for diagnostics (DIAG 0047-T5): the ring owner,
        // the voter set, and the eligible members this node sees — so a split between
        // the placement ring (routing) and the lease store (commit) is visible.
        let (replica_set, diag_owner, diag_voters, diag_members) = {
            let placement = self
                .placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !placement.owns_group(group) {
                return Err(ReplError::NotOwner);
            }
            let replica_set = placement.group_replica_set(group);
            // The min-replicas write floor (issues #167, #239): replica sets truncate to
            // min(R, members), so a shrinking cluster silently commits new durability
            // promises on fewer copies than the operator configured — down to
            // quorum-of-1. Below the resolved floor, REFUSE instead: transient like
            // NoQuorum (capacity returning lifts it), and gating only NEW promises —
            // reads, acked-driven truncation and removal come through here with
            // `gate_write_floor = false`, so the node degrades to serving what it already
            // promised rather than dying. A resolved floor of 1 = no refusal: a lone node
            // stays healthy when alone.
            if gate_write_floor {
                let floor = placement.min_replicas();
                if floor > 1 && replica_set.len() < floor {
                    return Err(ReplError::UnderReplicated {
                        actual: replica_set.len(),
                        floor,
                    });
                }
            }
            (
                replica_set,
                placement.group_owner(group),
                placement.voter_ids(),
                placement.members(),
            )
        };

        // Read the group's current lease epoch on every call. A higher epoch than the
        // cached log's means ownership was lost and regained: the cached log writes at
        // the old epoch and would be fenced by followers forever, so it must be rebuilt
        // (and the group's keys re-recovered) against the new lease.
        let epoch = match self.leases.epoch_for(group).await {
            Ok(epoch) => epoch,
            // This node OWNS the group by the placement ring (it passed `owns_group`
            // above) yet the lease store refuses the epoch — the ring and the
            // lease-assigner disagree about who holds `group`. Steady-state this must not
            // happen; it is expected only transiently while a fresh node's gossip/lease
            // views converge. A persistent occurrence is the 2026-07-20 durable-ownership
            // split — kept as a debug signal for that class.
            Err(e) => {
                tracing::debug!(
                    key,
                    group,
                    local = %self.local.0,
                    ring_owner = %diag_owner.0,
                    voters = ?diag_voters.iter().map(|n| &n.0).collect::<Vec<_>>(),
                    members = ?diag_members.iter().map(|n| &n.0).collect::<Vec<_>>(),
                    error = ?e,
                    "durable ownership split: placement ring owns this group but the lease store refuses its epoch"
                );
                return Err(e);
            }
        };

        // Get-or-(re)build the group entry at the current epoch **and replica set**.
        // A replica-set change rebuilds (and re-recovers, which re-commits to the
        // new set — ADR 0043 P1) even under an unchanged lease. Resolve to an owned
        // `Arc` and drop the guard before any await (the guard is not `Send`).
        let entry = {
            let mut cache = self.cache();
            match cache.get(&group) {
                Some(entry) if entry.log.epoch() == epoch && entry.replica_set == replica_set => {
                    entry.clone()
                }
                _ => {
                    let lease = OwnershipLease {
                        holder: self.local.clone(),
                        epoch,
                    };
                    let entry = Arc::new(GroupEntry {
                        log: Arc::new(
                            ClusterLog::new(
                                self.local.clone(),
                                lease,
                                &replica_set,
                                self.transport.clone(),
                            )
                            // The owner's self-ack is durable (ADR 0042 T8): it
                            // counts toward quorum only once the op is applied to
                            // this node's own replica copy — the same copy its
                            // recovery reads consult after a restart. With a
                            // writer attached (ADR 0071) those applies
                            // group-commit through the shared serializer.
                            .with_local_store(self.local_replicas.clone())
                            .maybe_owner_writer(self.owner_writer.clone()),
                        ),
                        recovered: Mutex::new(BTreeSet::new()),
                        replica_set: replica_set.clone(),
                    });
                    cache.insert(group, entry.clone());
                    entry
                }
            }
        };

        // Recover this key once per epoch: a new owner was a replica, so the committed
        // log lives in the replica set (its own copy + peers). Seeding it lets the
        // recovered queue replay (a fresh session simply recovers to empty). The marker
        // lives in the entry, so an epoch rebuild above resets it for the group.
        let recover = !entry
            .recovered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(key);
        if recover {
            let (recovered, floor) = self.recover_key(key, &replica_set).await?;
            // Re-commit the recovered base to a write quorum at the new epoch
            // BEFORE serving or appending (ADR 0042 T6, exhibit ②): a merge can
            // adopt a single-replica orphan, and building on it un-replicated lets
            // the next takeover gap out the acked tail above it. A NoQuorum here
            // leaves the recovery marker unset, so the next touch retries. The
            // floor keeps the offset space above every read replica's durable
            // truncation watermark (the exhibit's second face: an empty merge
            // must not restart a truncated queue's offsets at 1).
            entry.log.recommit_key(key, &recovered).await?;
            entry.log.seed_key(key, recovered, floor).await;
            entry
                .recovered
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key.to_string());
        }
        Ok(entry.log.clone())
    }

    /// Recover `key`'s committed log by reading a quorum of the replica set: this
    /// node's own follower copy plus the reachable peers'. Errors with
    /// [`ReplError::NoQuorum`] if fewer than a quorum can be read (recovery is unsafe
    /// — a committed entry might live only on an unread replica), or if no read in
    /// the merge is **complete** (ADR 0043 P1): a quorum assembled entirely from
    /// hollow joiners — replicas that entered the set mid-history and hold entries
    /// above a hole — could silently truncate the history none of them received.
    /// At least one gap-free copy must anchor the merge.
    async fn recover_key(
        &self,
        key: &str,
        replica_set: &[NodeId],
    ) -> Result<(Vec<LogEntry>, Offset), ReplError> {
        let quorum = replica_set.len() / 2 + 1;
        let enough = |reads: &[crate::cluster_log::ReplicaRead]| {
            reads.len() >= quorum && reads.iter().any(|r| r.complete)
        };
        let group = group_of_key(key);
        // Local copy first (sync; the guard is dropped before any await). Each read
        // carries the replica's truncation low-water so the merge cannot resurrect an
        // already-acked prefix from a stale replica (ADR 0018 §3b).
        let mut reads = vec![{
            let r = self
                .local_replicas
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            crate::cluster_log::ReplicaRead {
                watermark: r.watermark(key),
                // Our own copy anchors the merge only if it is gap-free AND we are
                // stamped current for the group's replica set (ADR 0043 P1): an
                // owner that is itself a fresh joiner must not treat its empty
                // copy as authority that the key has no history.
                complete: r.complete(key) && r.group_current(group, replica_set),
                entries: r.epoch_entries(key),
            }
        }];
        // Read peers **concurrently** (like the append fan-out) and stop as soon as
        // enough have responded — a quorum, at least one of it complete — so a slow
        // or just-died replica's RPC timeout does not serialize recovery when quorum
        // is reachable from faster replicas.
        if !enough(&reads) {
            let mut inflight = tokio::task::JoinSet::new();
            for replica in replica_set.iter().filter(|n| **n != self.local) {
                let transport = self.transport.clone();
                let replica = replica.clone();
                let key = key.to_string();
                inflight.spawn(async move { transport.read_replica(&replica, &key).await });
            }
            while !enough(&reads) {
                match inflight.join_next().await {
                    Some(Ok(Some(read))) => reads.push(read),
                    // A replica that did not respond (or a join error): keep waiting.
                    Some(Ok(None) | Err(_)) => {}
                    None => break, // every replica reported; not enough
                }
            }
        }
        if reads.len() < quorum {
            return Err(ReplError::NoQuorum);
        }
        if !reads.iter().any(|r| r.complete) {
            // Issue #390 — the grow hand-off race: a quorum of the CURRENT set
            // responded but every copy is hollow (fresh joiners), because the
            // lease moved onto data-less newcomers before the old owner's eager
            // hand-off re-committed the history. The history still exists — on
            // roster members OUTSIDE the current set — and reads cannot violate
            // epoch fencing, so recover it by reading, not by letting a stale
            // owner write. The fallback is deliberately stricter than the fast
            // path: EVERY known roster member must answer (a full sweep — only
            // when nothing is left unread can no committed entry be silently
            // missing from the union), the roster must have no unobservable
            // members, and the merged UNION must be gap-free (a union gap after
            // a full sweep is genuine loss, which must refuse, never truncate).
            return self.recover_key_from_roster(key, replica_set, reads).await;
        }
        // The floor: the highest truncation low-water across the reads. The merge
        // applies it to the entries; the caller also needs it to keep the key's
        // offset space above it (ADR 0042 T6).
        let floor = reads.iter().map(|r| r.watermark).max().unwrap_or(0);
        Ok((merge_replica_logs(&reads), floor))
    }

    /// The issue #390 fallback of [`recover_key`](Self::recover_key): the full-roster
    /// sweep. See the call site for the safety argument; the invariants enforced here:
    ///
    /// 1. the durable roster is fully known (an unobservable member might hold
    ///    history — refuse);
    /// 2. EVERY roster member outside the current set answers the read (one silent
    ///    member could hold the only copy of a committed entry — refuse);
    /// 3. the merged union is gap-free: no read may hold an entry above what the
    ///    merge served (a swallowed tail after a full sweep is genuine loss — refuse,
    ///    fail closed, exactly like `NoQuorum` always has).
    async fn recover_key_from_roster(
        &self,
        key: &str,
        replica_set: &[NodeId],
        mut reads: Vec<crate::cluster_log::ReplicaRead>,
    ) -> Result<(Vec<LogEntry>, Offset), ReplError> {
        let Some((known, unknown)) = self
            .placement
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .durable_roster()
            .cloned()
        else {
            return Err(ReplError::NoQuorum); // roster not known yet: cannot sweep
        };
        if unknown > 0 {
            return Err(ReplError::NoQuorum); // an unobservable member might hold history
        }
        let extras: Vec<NodeId> = known
            .iter()
            .filter(|n| **n != self.local && !replica_set.contains(n))
            .cloned()
            .collect();
        let mut inflight = tokio::task::JoinSet::new();
        for member in &extras {
            let transport = self.transport.clone();
            let member = member.clone();
            let key = key.to_string();
            inflight.spawn(async move { transport.read_replica(&member, &key).await });
        }
        let mut answered = 0usize;
        while let Some(res) = inflight.join_next().await {
            // A member that did not answer breaks the full-sweep guarantee.
            if let Ok(Some(read)) = res {
                answered += 1;
                reads.push(read);
            }
        }
        if answered < extras.len() {
            return Err(ReplError::NoQuorum);
        }
        let floor = reads.iter().map(|r| r.watermark).max().unwrap_or(0);
        let merged = merge_replica_logs(&reads);
        // Union gap check: the merge stops at a gap; anything any read holds above
        // the served prefix would then be silently truncated. After a full sweep
        // that is genuine loss — refuse.
        let served_high = floor + merged.len() as Offset;
        let holds_above = reads
            .iter()
            .flat_map(|r| r.entries.iter())
            .any(|e| e.offset > served_high);
        if holds_above {
            return Err(ReplError::NoQuorum);
        }
        tracing::info!(
            key,
            swept = extras.len(),
            recovered = merged.len(),
            "recovery fell back to a full roster sweep (issue #390): the current \
             replica set was all-hollow; history recovered read-only from former \
             holders and will re-commit at this owner's epoch"
        );
        Ok((merged, floor))
    }
}

#[async_trait]
impl<S: LeaseSource, T: ReplicaTransport + Clone + 'static> crate::durable_plane::CatchUpSource
    for GroupRoutedLog<S, T>
{
    async fn catch_up_key(&self, key: &str) {
        // Route to the key's group log — which recovers the key (quorum read +
        // re-commit) on first touch per epoch/replica-set as usual — then re-spread
        // its committed log to the CURRENT replica set (ADR 0043 P1). Idempotent:
        // same offsets, same bytes, fenced at this owner's epoch; a follower that
        // already holds an entry just re-applies it. Best-effort — the requesting
        // replica's sweep retries while its copy stays hollow.
        let log = match self.log_for_key(key, false).await {
            Ok(log) => log,
            Err(e) => {
                tracing::debug!(key, error = ?e, "catch-up: cannot route/recover key; requester will retry");
                return;
            }
        };
        let entries = log.committed_entries(key).await;
        if let Err(e) = log.recommit_key(key, &entries).await {
            tracing::debug!(key, error = ?e, "catch-up: re-commit fell short of quorum; requester will retry");
            return;
        }
        // Fan the truncation low-water too (best-effort, like every truncate): a
        // back-filled copy whose entries start above offset 1 is gap-free only
        // once its own watermark records the acked-away prefix.
        let floor = log.committed_floor(key).await;
        if floor > 0 {
            let _ = log.truncate(&key.to_string(), floor).await;
        }
    }

    async fn catch_up_key_to(&self, key: &str, target: &NodeId) {
        // Route (recovering on first touch as usual), then hand the committed
        // log to the ONE requested node — the decommission drain's targeted
        // re-commit (ADR 0043 P3). Best-effort: the drain verifies and re-asks.
        let log = match self.log_for_key(key, false).await {
            Ok(log) => log,
            Err(e) => {
                tracing::debug!(key, error = ?e, "targeted catch-up: cannot route/recover key; drain will retry");
                return;
            }
        };
        log.recommit_key_to(key, target).await;
    }
}

#[async_trait]
impl<S: LeaseSource, T: ReplicaTransport + Clone + 'static> ReplicatedLog for GroupRoutedLog<S, T> {
    type Key = String;

    async fn append(&self, key: &String, record: Vec<u8>) -> Result<Offset, ReplError> {
        // `gate_write_floor = true`: the min-replicas write floor (issues #167, #239) is
        // applied inside `log_for_key`, against the replica set the returned log is built
        // over and under the same placement-lock acquisition — see the note there for why
        // a separate pre-check here was a race. This is the ONLY caller that gates.
        self.log_for_key(key, true).await?.append(key, record).await
    }

    async fn append_tiered(
        &self,
        key: &String,
        record: Vec<u8>,
        tier: DurabilityTier,
    ) -> Result<Offset, ReplError> {
        // The min-replicas write floor still gates EVERY tier: `Local` weakens
        // what the ack waits for, not which replica sets are allowed to accept
        // writes — a group below the floor refuses relaxed publishes too
        // (ADR 0072; the floor is an operator safety rail, not a latency knob).
        self.log_for_key(key, true)
            .await?
            .append_tiered(key, record, tier)
            .await
    }

    async fn submit_tiered(
        &self,
        key: &String,
        record: Vec<u8>,
        tier: DurabilityTier,
    ) -> PendingAppend {
        // Two-phase append (ADR 0075), same floor gating as `append_tiered`:
        // routing and the min-replicas write floor are decided at submit time;
        // only the durability wait rides in the pending.
        match self.log_for_key(key, true).await {
            Ok(log) => log.submit_tiered(key, record, tier).await,
            Err(e) => PendingAppend::ready(Err(e)),
        }
    }

    async fn read(
        &self,
        key: &String,
        after: Offset,
        limit: usize,
    ) -> Result<Vec<LogEntry>, ReplError> {
        self.log_for_key(key, false)
            .await?
            .read(key, after, limit)
            .await
    }

    async fn live_range(&self, key: &String) -> Result<Option<(Offset, Offset)>, ReplError> {
        self.log_for_key(key, false).await?.live_range(key).await
    }

    async fn truncate(&self, key: &String, up_to: Offset) -> Result<(), ReplError> {
        self.log_for_key(key, false)
            .await?
            .truncate(key, up_to)
            .await
    }

    async fn remove(&self, key: &String) -> Result<(), ReplError> {
        self.log_for_key(key, false).await?.remove(key).await
    }

    async fn keys(&self) -> Result<Vec<String>, ReplError> {
        // The replicated copies this node holds (ADR 0009 §3): the session metadata a new
        // owner inherited at takeover, and (on a persistent backend) what reopened from
        // disk after a restart, live here — UNIONED with every reachable peer's key set
        // (ADR 0042 T9, exhibit ⑥): quorum appends mean any single replica may lack a
        // key, and a new owner that never held a copy of a group would otherwise never
        // discover its sessions. Values are still read per key via quorum recovery.
        let mut keys = self
            .local_replicas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys();
        keys.append(&mut self.transport.list_remote_keys().await);
        keys.sort_unstable();
        keys.dedup();
        Ok(keys)
    }

    async fn epoch_for(&self, key: &String) -> Result<u64, ReplError> {
        // The routed log is rebuilt per lease epoch, so this is exactly the epoch an
        // `append` through the same route would stamp its ops with (ADR 0037 token).
        Ok(self.log_for_key(key, false).await?.epoch())
    }
}

#[cfg(test)]
mod tests {
    use super::{GroupRoutedLog, LeaseSource};
    use crate::cluster_log::{ReplOp, ReplicaState};
    use crate::lease::Epoch;
    use crate::lease_raft::GroupId;
    use crate::peer::PeerMessage;
    use crate::placement::{group_of, Placement, DEFAULT_REPLICAS, NUM_GROUPS};
    use crate::repl_net::PeerReplicaTransport;
    use crate::swim::MemberState;
    use crate::NodeId;
    use async_trait::async_trait;
    use mqtt_core::{ClientId, Message, QoS};
    use mqtt_storage::logged::ReplicatedSessionStore;
    use mqtt_storage::repl::ReplicatedLog;
    use mqtt_storage::retained_log::ReplicatedRetained;
    use mqtt_storage::SessionStore;
    use std::sync::{Arc, Mutex, RwLock};
    use tokio::sync::mpsc;

    fn nid(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    /// A fixed-epoch lease source (the real one talks to the consensus group, 4f).
    #[derive(Debug)]
    struct FixedLease(Epoch);

    #[async_trait]
    impl LeaseSource for FixedLease {
        async fn epoch_for(&self, _group: GroupId) -> Result<Epoch, mqtt_storage::repl::ReplError> {
            Ok(self.0)
        }
    }

    /// A lease source whose epoch can be bumped to simulate ownership lost + regained.
    #[derive(Clone)]
    struct BumpableLease(Arc<std::sync::atomic::AtomicU64>);

    #[async_trait]
    impl LeaseSource for BumpableLease {
        async fn epoch_for(&self, _group: GroupId) -> Result<Epoch, mqtt_storage::repl::ReplError> {
            Ok(self.0.load(std::sync::atomic::Ordering::Relaxed))
        }
    }

    /// A follower: read replication frames off its link channel, apply to its
    /// `ReplicaState`, and ack on the owner's transport (in-process stand-in for the
    /// peer link, which the wire tests already cover). Also answers recovery-reads
    /// (`ReplicaRead`) from its stored copy, as the real plane does.
    fn spawn_follower(
        transport: Arc<PeerReplicaTransport>,
        state: Arc<Mutex<ReplicaState>>,
        mut rx: mpsc::UnboundedReceiver<PeerMessage>,
    ) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    PeerMessage::Replicate { req_id, epoch, op } => {
                        let accepted = state.lock().unwrap().apply(epoch, &op);
                        transport.complete_ack(req_id, accepted);
                    }
                    // The recovery-read, with the completeness verdict
                    // (ADR 0043 P1) — what the plane serves on a real link.
                    PeerMessage::ReplicaRead { req_id, key } => {
                        let (watermark, complete, entries) = {
                            let s = state.lock().unwrap();
                            (
                                s.watermark(&key),
                                s.complete(&key),
                                s.epoch_entries(&key)
                                    .into_iter()
                                    .map(|e| crate::peer::ReplicaEntryWire {
                                        offset: e.offset,
                                        epoch: e.epoch,
                                        seq: e.seq,
                                        record: e.record,
                                    })
                                    .collect(),
                            )
                        };
                        transport.complete_read(req_id, watermark, complete, entries);
                    }
                    _ => {}
                }
            }
        });
    }

    /// Stamp `replicas` caught-up for every group at its **current** replica set —
    /// the state a completed catch-up sweep leaves behind (ADR 0043 P1). Tests
    /// that model an established (non-joiner) node start from here, exactly as a
    /// real node does after its boot sweep.
    fn stamp_current(replicas: &Arc<Mutex<ReplicaState>>, placement: &Placement) {
        let stamps: Vec<_> = (0..NUM_GROUPS)
            .map(|g| (g, placement.group_replica_set(g)))
            .collect();
        replicas.lock().unwrap().mark_groups_current(&stamps);
    }

    /// Find a placement group `owner` owns, and a client id that hashes to it.
    fn owned_group_and_client(p: &Placement) -> (GroupId, ClientId) {
        let group = (0..NUM_GROUPS)
            .find(|g| p.owns_group(*g))
            .expect("owns some group");
        for i in 0..100_000 {
            let c = format!("client-{i}");
            if group_of(&c) == group {
                return (group, ClientId(c));
            }
        }
        panic!("no client hashes to the owned group");
    }

    /// A client whose group `owner` does NOT own (for the not-owner path).
    fn foreign_client(p: &Placement) -> ClientId {
        for i in 0..100_000 {
            let c = format!("client-{i}");
            if !p.owns_group(group_of(&c)) {
                return ClientId(c);
            }
        }
        panic!("this node owns every group");
    }

    /// An enqueue through the durable store quorum-replicates to a follower — so the
    /// message survives the owner's loss (a replica has it). The headline durability
    /// claim, assembled end to end (store → group routing → `ClusterLog` → replication).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn enqueue_replicates_to_a_follower() {
        let owner = nid("owner");
        // A 3-node ring (R=3, quorum=2): owner + two followers.
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));

        // Owner's transport, wired to two in-process followers.
        let transport = Arc::new(PeerReplicaTransport::new());
        let f1_state = Arc::new(Mutex::new(ReplicaState::new()));
        let f2_state = Arc::new(Mutex::new(ReplicaState::new()));
        for (node, state) in [(nid("f1"), &f1_state), (nid("f2"), &f2_state)] {
            let (tx, rx) = mpsc::unbounded_channel();
            transport.register(node, tx);
            spawn_follower(transport.clone(), state.clone(), rx);
        }

        let store = ReplicatedSessionStore::new(GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            transport.clone(),
            FixedLease(1),
            Arc::new(Mutex::new(ReplicaState::new())),
        ));

        let (group, client) = owned_group_and_client(&placement.read().unwrap());
        let msg = Message::new(
            "t".to_string(),
            bytes::Bytes::from_static(b"durable"),
            QoS::AtLeastOnce,
            false,
        );
        store.ensure_session(&client).await.unwrap();
        store.enqueue(&client, &msg).await.unwrap();

        // The message is durable: a follower's replicated copy holds the queue entry.
        let qkey = format!("q/{}", client.0);
        let on_f1 = f1_state.lock().unwrap().entries(&qkey).len();
        let on_f2 = f2_state.lock().unwrap().entries(&qkey).len();
        assert!(
            on_f1 + on_f2 >= 1,
            "the enqueue should have replicated to at least one follower (group {group})"
        );
        // The owner can read its own committed queue back.
        assert_eq!(store.pending(&client, 0, 100).await.unwrap().len(), 1);
    }

    /// Issue #167: replica sets truncate to `min(R, members)`, so a lone node with
    /// R=3 acks durable writes with ONE copy while the operator believes three —
    /// silently. With the min-replicas write floor set, a group below the floor
    /// REFUSES the append (transient, exactly like `NoQuorum`); reads stay served
    /// (the node degrades to serving what it already promised, not to dying); and
    /// the same append commits once capacity returns. The default floor of 1 keeps
    /// the standing lone-node-stays-healthy behaviour — which every other test in
    /// this module exercises implicitly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn appends_below_the_min_replicas_floor_are_refused_until_capacity_returns() {
        use mqtt_storage::repl::ReplError;

        let owner = nid("owner");
        // Alone with R=3 and a floor of 2: every group's set is min(3, 1) = 1 < 2.
        let p = Placement::new(owner.clone(), DEFAULT_REPLICAS).with_min_replicas(2);
        let placement = Arc::new(RwLock::new(p));
        let transport = Arc::new(PeerReplicaTransport::new());
        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            transport.clone(),
            FixedLease(1),
            Arc::new(Mutex::new(ReplicaState::new())),
        );

        let key = "q/floor-client".to_string();
        let err = log.append(&key, vec![1]).await.unwrap_err();
        assert!(
            matches!(
                err,
                ReplError::UnderReplicated {
                    actual: 1,
                    floor: 2
                }
            ),
            "a lone node with floor 2 must refuse the durable append, got {err:?}"
        );

        // Capacity returns: one follower brings the set to the floor; the append
        // commits (and needs the follower's quorum ack — 2 of 2 — to do so).
        let f1_state = Arc::new(Mutex::new(ReplicaState::new()));
        let (tx, rx) = mpsc::unbounded_channel();
        transport.register(nid("f1"), tx);
        spawn_follower(transport.clone(), f1_state.clone(), rx);
        placement
            .write()
            .unwrap()
            .observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        log.append(&key, vec![1])
            .await
            .expect("at the floor, the append commits");
        assert_eq!(
            log.read(&key, 0, usize::MAX).await.unwrap().len(),
            1,
            "the committed entry reads back"
        );

        // The cluster shrinks again: NEW promises refuse. (Only `append` carries the
        // gate — reads, truncation and removal are structurally untouched, so the
        // node degrades to serving what it already promised, not to dying. Serving
        // reads across the shrink itself is the ADR 0043 P1 catch-up machinery's
        // job, driven by the reconcile loop this unit harness does not run.)
        placement
            .write()
            .unwrap()
            .observe(&nid("f1"), MemberState::Dead, "f1:7000", None);
        assert!(
            matches!(
                log.append(&key, vec![2]).await,
                Err(ReplError::UnderReplicated { .. })
            ),
            "below the floor again, the next append must refuse"
        );
    }

    /// Issue #239, the defect itself: with the SHIPPED default posture (the derived
    /// majority floor), a node that knows it belongs to a three-member group and has
    /// buried both peers must REFUSE new durable promises instead of acking them on its
    /// own single copy while the operator believes R=3. Reads of what was already
    /// committed stay served — the node degrades to keeping its old promises, not to
    /// dying.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lone_survivor_of_a_known_three_node_group_refuses_appends_by_default() {
        use crate::placement::WriteFloor;
        use mqtt_storage::repl::ReplError;
        use std::collections::BTreeSet;

        let owner = nid("owner");
        // The default posture: no explicit floor, `declared` at its default of 1 — every
        // bit of the arming comes from the membership the node actually knows.
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS)
            .with_write_floor(WriteFloor::Majority { declared: 1 });
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        // The quorum-committed roster of the three-member durable group (#229).
        p.set_durable_roster(
            [nid("owner"), nid("f1"), nid("f2")]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            0,
        );
        assert_eq!(p.min_replicas(), 2, "three known members ⇒ a floor of 2");
        let placement = Arc::new(RwLock::new(p));

        let transport = Arc::new(PeerReplicaTransport::new());
        for node in [nid("f1"), nid("f2")] {
            let (tx, rx) = mpsc::unbounded_channel();
            transport.register(node, tx);
            spawn_follower(
                transport.clone(),
                Arc::new(Mutex::new(ReplicaState::new())),
                rx,
            );
        }
        // An established (non-joiner) node: its catch-up sweep completed long ago.
        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        stamp_current(&replicas, &placement.read().unwrap());
        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            transport.clone(),
            FixedLease(1),
            replicas.clone(),
        );

        // Healthy: three members, floor 2, the append commits at quorum.
        let (_, client) = owned_group_and_client(&placement.read().unwrap());
        let key = format!("q/{}", client.0);
        log.append(&key, vec![7])
            .await
            .expect("a healthy three-member group commits under the default floor");
        assert_eq!(
            log.read(&key, 0, usize::MAX).await.unwrap().len(),
            1,
            "the committed entry reads back"
        );

        // Both peers die. The roster is unchanged (a CRASHED member stays on it), so the
        // node still knows it owes three copies — and refuses to promise on one.
        {
            let mut p = placement.write().unwrap();
            p.observe(&nid("f1"), MemberState::Dead, "f1:7000", None);
            p.observe(&nid("f2"), MemberState::Dead, "f2:7000", None);
        }
        // Model the CONVERGED post-shrink state, not the instant after it: re-stamp this
        // node's own replica copy as current for the shrunken sets, which is what the
        // reconcile driver's catch-up sweep does (ADR 0043 P1) and what this unit harness
        // does not run. That matters for what this test proves — without it, the append
        // below fails with `NoQuorum` (a one-member recovery quorum with no *complete*
        // read) whatever the floor says, and the assertion would pass on the old default
        // for the wrong reason. With it, the floor is the only thing standing between a
        // lone survivor and a single-copy `Ok` — which is exactly issue #239.
        stamp_current(&replicas, &placement.read().unwrap());
        let err = log.append(&key, vec![8]).await.unwrap_err();
        assert!(
            matches!(
                err,
                ReplError::UnderReplicated {
                    actual: 1,
                    floor: 2
                }
            ),
            "the lone survivor of a known 3-node group must refuse, got {err:?}"
        );

        // Only NEW promises are gated. The read path is structurally ungated by the
        // floor — whatever it reports across a shrink (serving reads through one is the
        // ADR 0043 P1 catch-up machinery's job, and this unit harness runs no reconcile
        // loop), it is never the floor's refusal. The node degrades to keeping the
        // promises it made, not to dying.
        assert!(
            !matches!(
                log.read(&key, 0, usize::MAX).await,
                Err(ReplError::UnderReplicated { .. })
            ),
            "the write floor must never gate a read"
        );
    }

    /// The standing requirement, pinned permanently: a node that has never known a
    /// cluster still acks durable writes under the SHIPPED default. Non-vacuous against
    /// the resolver, not against the old default — it fails the moment the derived floor
    /// forgets the degenerate case (resolving the majority over `R` instead of over the
    /// witness, or seeding the witness at `R`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_node_that_never_knew_a_cluster_acks_durable_writes_under_the_default_floor() {
        use crate::placement::WriteFloor;
        use std::collections::BTreeSet;

        let owner = nid("solo");
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS)
            .with_write_floor(WriteFloor::Majority { declared: 1 });
        // A single-node durable group: the roster names only this node.
        p.set_durable_roster([nid("solo")].into_iter().collect::<BTreeSet<_>>(), 0);
        assert_eq!(p.min_replicas(), 1, "alone ⇒ no floor");
        let placement = Arc::new(RwLock::new(p));

        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        stamp_current(&replicas, &placement.read().unwrap());
        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            FixedLease(1),
            replicas,
        );

        for key in ["q/solo-client", "r/solo/topic"] {
            let key = key.to_string();
            let first = log
                .append(&key, vec![1])
                .await
                .expect("a lone node must keep acking durable writes");
            let second = log
                .append(&key, vec![2])
                .await
                .expect("and keep acking the next one");
            assert!(second > first, "offsets stay monotone: {first} -> {second}");
        }
    }

    /// `LocalLeaseSource` reads a lease the (leader-driven) consensus group assigned
    /// to this node, and reports `NotOwner` for a group not assigned to it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_lease_source_reads_an_assigned_lease() {
        use crate::cluster_store::LocalLeaseSource;
        use crate::lease_group::config;
        use crate::lease_raft::LeaseRequest;
        use crate::lease_store::LeaseStore;
        use crate::node_registry::raft_id;
        use crate::raft_mesh::MeshRaftNetwork;
        use openraft::storage::Adaptor;
        use openraft::{BasicNode, Raft, ServerState};
        use std::collections::BTreeMap;
        use std::time::Duration;

        let local = raft_id(&nid("lease-node"));
        let store = LeaseStore::new();
        let source = LocalLeaseSource::new(store.clone(), local);
        let (ls, sm) = Adaptor::new(store);
        let raft = Raft::new(local, config(), MeshRaftNetwork::new(), ls, sm)
            .await
            .unwrap();
        raft.initialize(BTreeMap::from([(local, BasicNode::default())]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "leader")
            .await
            .unwrap();

        // No lease for group 7 yet → NotOwner.
        assert!(source.epoch_for(7).await.is_err());

        // Assign group 7 to this node; the owner reads its epoch locally.
        let resp = raft
            .client_write(LeaseRequest::Assign {
                group: 7,
                node: local,
            })
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(resp.log_id.index), "applied")
            .await
            .unwrap();
        assert_eq!(source.epoch_for(7).await.unwrap(), resp.data.unwrap().epoch);

        // A group assigned to another node is NotOwner to us.
        let resp = raft
            .client_write(LeaseRequest::Assign {
                group: 8,
                node: local.wrapping_add(1),
            })
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(resp.log_id.index), "applied")
            .await
            .unwrap();
        assert!(source.epoch_for(8).await.is_err());

        raft.shutdown().await.unwrap();
    }

    /// A session whose group this node does not own is refused (it belongs on the
    /// group owner — relocation, ADR 0005), surfaced as the transient `Unavailable`
    /// error (the lease may yet land here) rather than a terminal failure (ADR 0017).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn foreign_group_is_not_owned() {
        let owner = nid("owner");
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));

        let store = ReplicatedSessionStore::new(GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            FixedLease(1),
            Arc::new(Mutex::new(ReplicaState::new())),
        ));

        let foreign = foreign_client(&placement.read().unwrap());
        let msg = Message::new(
            "t".to_string(),
            bytes::Bytes::from_static(b"x"),
            QoS::AtLeastOnce,
            false,
        );
        let err = store.enqueue(&foreign, &msg).await.unwrap_err();
        assert!(
            err.is_transient(),
            "a non-owned group must be refused as transient (lease may land here), got {err:?}"
        );
    }

    /// Takeover recovery: a new owner was a replica, so its committed copy of a key
    /// lives in the shared follower `ReplicaState`. On the key's first touch the
    /// store recovers it from a quorum of the replica set (here just this node's own
    /// copy), replays the recovered entries, and continues appending after the
    /// recovered watermark — the heart of workstream F.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn takeover_recovers_a_keys_log_from_the_shared_replica_state() {
        let owner = nid("owner");
        // A single-node group (quorum 1): recovery reads only this node's own copy.
        let placement = Arc::new(RwLock::new(Placement::new(owner.clone(), DEFAULT_REPLICAS)));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        // This node was a replica: its follower copy already holds the committed
        // queue (offsets 1, 2), applied at some prior epoch.
        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        {
            let mut r = replicas.lock().unwrap();
            for (offset, record) in [(1u64, b"m1".to_vec()), (2u64, b"m2".to_vec())] {
                r.apply(
                    3,
                    &ReplOp::Append {
                        key: qkey.clone(),
                        offset,
                        seq: offset,
                        record,
                    },
                );
            }
        }

        // This node completed its catch-up sweep long ago (it held the copy).
        stamp_current(&replicas, &placement.read().unwrap());
        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            FixedLease(7),
            replicas,
        );

        // First touch recovers the committed copy; the recovered entries replay.
        let got = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(got.iter().map(|e| e.offset).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(&got[1].record, b"m2");

        // Appends continue after the recovered watermark (no offset reuse).
        assert_eq!(log.append(&qkey, b"m3".to_vec()).await.unwrap(), 3);
        assert_eq!(log.read(&qkey, 0, 100).await.unwrap().len(), 3);
    }

    /// Exhibit ② regression (ADR 0042 T6): an entry the takeover merge ADOPTS
    /// from a single copy — here an orphan a crashed owner's partial fan-out left
    /// on this node's own follower copy — is **re-committed to a write quorum**
    /// before the key is served, at the new owner's epoch. Without the re-commit
    /// the new owner builds acked entries on top of a base only it can see, and
    /// the NEXT takeover (a read quorum missing this node) gaps out at the orphan
    /// and discards the acked tail above it. Found by the T2 simulation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn takeover_recommits_an_adopted_orphan_to_a_write_quorum() {
        let owner = nid("owner");
        // A 3-node ring (R=3, quorum=2): owner + two live followers.
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        let transport = Arc::new(PeerReplicaTransport::new());
        let mut follower_states = Vec::new();
        for node in [nid("f1"), nid("f2")] {
            let state = Arc::new(Mutex::new(ReplicaState::new()));
            let (tx, rx) = mpsc::unbounded_channel();
            transport.register(node, tx);
            spawn_follower(transport.clone(), state.clone(), rx);
            follower_states.push(state);
        }

        // The old owner (epoch 1) crashed mid-fan-out: its orphan reached only
        // THIS node's follower copy. The recovery merge will adopt it (it is
        // contiguous — indistinguishable from a committed entry).
        let local = Arc::new(Mutex::new(ReplicaState::new()));
        assert!(local.lock().unwrap().apply(
            1,
            &ReplOp::Append {
                key: qkey.clone(),
                offset: 1,
                seq: 1,
                record: b"orphan".to_vec(),
            }
        ));

        let log = GroupRoutedLog::new(owner, placement, transport, FixedLease(2), local);

        // First touch after the takeover: the orphan is adopted AND re-committed.
        let served = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(served.iter().map(|e| e.offset).collect::<Vec<_>>(), vec![1]);
        assert_eq!(&served[0].record, b"orphan");

        // The adopted base now lives on BOTH followers, at the new epoch's fence:
        // any future read quorum sees it — no gap, no discarded acked tail.
        for (i, follower) in follower_states.iter().enumerate() {
            let f = follower.lock().unwrap();
            let entries = f.entries(&qkey);
            assert_eq!(
                entries.iter().map(|e| e.offset).collect::<Vec<_>>(),
                vec![1],
                "follower {i} must hold the re-committed orphan"
            );
            assert_eq!(&entries[0].record, b"orphan");
            assert_eq!(
                f.fence_for_key(&qkey),
                2,
                "the re-commit advances follower {i}'s group fence to the new epoch"
            );
        }
    }

    /// Exhibit ④ regression (ADR 0042 T8): the owner's quorum self-ack is
    /// **durable** — an append acked via {owner, one follower} survives the
    /// owner's crash-and-restart even when that follower is down at recovery,
    /// because the owner's own durable replica copy (which its recovery read
    /// consults) received the entry before the self-ack counted. Before the fix
    /// the self-ack was the volatile `ClusterLog` cache: the same recovery read
    /// {restarted-owner(empty), other-follower(empty)} merged empty and the
    /// acked entry was gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_acked_append_survives_owner_restart_via_its_durable_self_copy() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let owner = nid("owner");
        // A 3-node ring (R=3, quorum=2): owner + f1 (up) + f2 (down for now).
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        let transport = Arc::new(PeerReplicaTransport::new());
        let f1_state = Arc::new(Mutex::new(ReplicaState::new()));
        let (f1_tx, f1_rx) = mpsc::unbounded_channel();
        transport.register(nid("f1"), f1_tx);
        spawn_follower(transport.clone(), f1_state.clone(), f1_rx);

        // The node's own durable replica copy — it survives the "restart" below.
        let local = Arc::new(Mutex::new(ReplicaState::new()));

        let epoch = Arc::new(AtomicU64::new(2));
        let log = GroupRoutedLog::new(
            owner,
            placement,
            transport.clone(),
            BumpableLease(epoch.clone()),
            local.clone(),
        );

        // Acked with f2 down: quorum = the owner's DURABLE self-ack + f1.
        assert_eq!(log.append(&qkey, b"precious".to_vec()).await.unwrap(), 1);
        assert_eq!(
            local.lock().unwrap().entries(&qkey).len(),
            1,
            "the self-ack was durable: the entry is in the owner's own copy"
        );

        // Owner crashes and restarts; f1 is now down and f2 up — the recovery
        // read is {own durable copy, f2}, which before the fix held nothing.
        transport.fail_node(&nid("f1"));
        let f2_state = Arc::new(Mutex::new(ReplicaState::new()));
        let (f2_tx, f2_rx) = mpsc::unbounded_channel();
        transport.register(nid("f2"), f2_tx);
        spawn_follower(transport.clone(), f2_state.clone(), f2_rx);
        epoch.store(3, Ordering::Relaxed); // the restart won a fresh lease epoch

        let served = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(
            served.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1],
            "the acked entry survived the restart via the owner's durable copy"
        );
        assert_eq!(&served[0].record, b"precious");
        // And the takeover re-commit (T6) spread it back to a quorum: f2 has it.
        assert_eq!(f2_state.lock().unwrap().entries(&qkey).len(), 1);
    }

    /// Exhibit ②'s second face (ADR 0042 T6, found when the T2 sim's waiver
    /// lifted): a fully-truncated queue legitimately merges **empty**, but the
    /// new owner must NOT restart its offset space at 1 — some replica's durable
    /// truncation watermark sits above it, and any later recovery reading that
    /// replica silently drops every new acked write at or below the watermark.
    /// The recovered offset space continues above the reads' truncation floor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_of_a_truncated_key_continues_offsets_above_the_watermark() {
        let owner = nid("owner");
        // A single-node group (quorum 1): recovery reads only this node's copy.
        let placement = Arc::new(RwLock::new(Placement::new(owner.clone(), DEFAULT_REPLICAS)));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        // The queue was fully drained under the old owner: entries 1..=2 acked by
        // the client and truncated away — the follower copy holds only the
        // truncation watermark.
        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        {
            let mut r = replicas.lock().unwrap();
            for (offset, record) in [(1u64, b"m1".to_vec()), (2u64, b"m2".to_vec())] {
                r.apply(
                    3,
                    &ReplOp::Append {
                        key: qkey.clone(),
                        offset,
                        seq: offset,
                        record,
                    },
                );
            }
            r.apply(
                3,
                &ReplOp::Truncate {
                    key: qkey.clone(),
                    up_to: 2,
                },
            );
            assert_eq!(r.watermark(&qkey), 2);
            assert!(r.entries(&qkey).is_empty());
        }

        // An established node (its sweep completed while it held the copy).
        stamp_current(&replicas, &placement.read().unwrap());
        let log = GroupRoutedLog::new(
            owner,
            placement,
            Arc::new(PeerReplicaTransport::new()),
            FixedLease(7),
            replicas,
        );

        // The recovery merge is empty — correct — but the next append must land
        // ABOVE the watermark, not restart at 1 (which any later merge including
        // this replica would silently drop).
        assert_eq!(log.read(&qkey, 0, 100).await.unwrap().len(), 0);
        assert_eq!(log.append(&qkey, b"m3".to_vec()).await.unwrap(), 3);
        let served = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(served.iter().map(|e| e.offset).collect::<Vec<_>>(), vec![3]);
    }

    /// Recovery **fails closed**: when a quorum of the replica set cannot be read, the new
    /// owner must NOT fabricate an empty (clean) log — it returns `NoQuorum` so the attach
    /// retries (the error is classified transient, so ADR 0017's `recover_until_ready` waits
    /// rather than downgrading). A committed entry could live only on an unreachable replica;
    /// serving an empty log would silently drop it and wipe a durable session. This guards
    /// the "merge a quorum or refuse" contract in [`recover_key`](super::GroupRoutedLog).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_below_quorum_fails_closed_with_noquorum() {
        let owner = nid("owner");
        // A 3-node ring (R=3, quorum=2): owner + two followers that are NOT reachable.
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        // This node's own committed copy DOES hold an entry — so recovery is not blocked by
        // missing local data; it is blocked purely by being unable to reach a quorum, which
        // is exactly the case where fabricating an empty log would be unsafe.
        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        replicas.lock().unwrap().apply(
            3,
            &ReplOp::Append {
                key: qkey.clone(),
                offset: 1,
                seq: 1,
                record: b"m1".to_vec(),
            },
        );

        // A transport with NO followers registered: each peer recovery-read resolves to
        // `None` immediately ("replica not connected", repl_net.rs), so only this node's own
        // read is available — 1 of the required 2.
        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            FixedLease(7),
            replicas,
        );

        // First touch attempts recovery, cannot reach a quorum, and refuses to serve.
        let err = log.read(&qkey, 0, 100).await.unwrap_err();
        assert!(
            matches!(err, mqtt_storage::repl::ReplError::NoQuorum),
            "below quorum, recovery must fail closed with NoQuorum (never an empty log); got {err:?}"
        );
    }

    /// ADR 0043 P1, the empty-joiner face of the hazard: an owner that has not
    /// completed its catch-up sweep (no durable caught-up stamp for the group)
    /// must not treat its own EMPTY copy as proof a key has no history — the
    /// history may live entirely on nodes it has not caught up from. Recovery
    /// fails closed until the boot sweep stamps the group; then the same read
    /// serves the (actually) fresh key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unstamped_owner_cannot_fabricate_an_empty_log() {
        let owner = nid("owner");
        let placement = Arc::new(RwLock::new(Placement::new(owner.clone(), DEFAULT_REPLICAS)));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            FixedLease(1),
            replicas.clone(),
        );

        // Before the sweep stamps: the empty copy is not an anchor. Fail closed.
        let err = log.read(&qkey, 0, 100).await.unwrap_err();
        assert!(
            matches!(err, mqtt_storage::repl::ReplError::NoQuorum),
            "an unstamped empty copy must not fabricate a clean log, got {err:?}"
        );

        // The boot sweep stamps the (self-only) sets; the fresh key now serves.
        stamp_current(&replicas, &placement.read().unwrap());
        assert_eq!(log.read(&qkey, 0, 100).await.unwrap().len(), 0);
        assert_eq!(log.append(&qkey, b"m1".to_vec()).await.unwrap(), 1);
    }

    /// ADR 0043 P1: recovery refuses a quorum assembled entirely from HOLLOW copies.
    /// Every read reaching quorum here has a gap (entries above a hole none of them
    /// received); merging them would silently truncate the missing history, so the
    /// gate fails closed with `NoQuorum` — until a complete copy joins the reads,
    /// at which point the same recovery serves the full log.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_refuses_a_quorum_of_hollow_replicas() {
        let owner = nid("owner");
        // A 3-node ring (R=3, quorum=2): owner + two followers.
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        let hollow = |key: &str| {
            // Offset 2 above a hole at 1: a joiner that caught only the newest append.
            let mut s = ReplicaState::new();
            assert!(s.apply(
                3,
                &ReplOp::Append {
                    key: key.to_string(),
                    offset: 2,
                    seq: 2,
                    record: b"m2".to_vec(),
                }
            ));
            assert!(!s.complete(key));
            s
        };

        // This node's own copy is hollow, and so is the one reachable follower.
        let transport = Arc::new(PeerReplicaTransport::new());
        let f1_state = Arc::new(Mutex::new(hollow(&qkey)));
        let (f1_tx, f1_rx) = mpsc::unbounded_channel();
        transport.register(nid("f1"), f1_tx);
        spawn_follower(transport.clone(), f1_state.clone(), f1_rx);

        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            transport.clone(),
            FixedLease(7),
            Arc::new(Mutex::new(hollow(&qkey))),
        );

        // Quorum (2 of 3) is readable — but every read is hollow. Fail closed.
        let err = log.read(&qkey, 0, 100).await.unwrap_err();
        assert!(
            matches!(err, mqtt_storage::repl::ReplError::NoQuorum),
            "a merge with no complete read must refuse (ADR 0043 P1), got {err:?}"
        );

        // A complete copy (f2, holding 1..=2) joins the reads: recovery now serves
        // the FULL history, including the offset every hollow copy lacked.
        let mut full = ReplicaState::new();
        for off in 1..=2u64 {
            assert!(full.apply(
                3,
                &ReplOp::Append {
                    key: qkey.clone(),
                    offset: off,
                    seq: off,
                    record: format!("m{off}").into_bytes(),
                }
            ));
        }
        let (f2_tx, f2_rx) = mpsc::unbounded_channel();
        transport.register(nid("f2"), f2_tx);
        spawn_follower(transport.clone(), Arc::new(Mutex::new(full)), f2_rx);

        let served = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(
            served.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1, 2],
            "with a complete anchor the merge serves the whole history"
        );
    }

    /// ADR 0043 P1, the laptop→server sell at the store seam: a group written when
    /// the cluster was ONE node (replica set = the owner, quorum 1) re-replicates
    /// its history when the ring grows to three — the replica-set change rebuilds
    /// the cached group log, recovery re-commits to the NEW set, and the joiners'
    /// copies end up complete. This is the same path a `ReplicaCatchUp` request
    /// drives through [`CatchUpSource::catch_up_key`].
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn growing_a_one_node_group_back_fills_the_joiners() {
        use crate::durable_plane::CatchUpSource;

        let owner = nid("owner");
        // Pick a client whose group the founder still OWNS in the grown 3-node
        // ring (growth moves ~2/3 of the groups to the joiners — those move under
        // P2's eager migration; P1 is about the groups whose owner stays put but
        // whose replica set gains members).
        let (_group, client) = {
            let mut grown = Placement::new(owner.clone(), DEFAULT_REPLICAS);
            grown.observe(&nid("f1"), MemberState::Alive, "x:7000", None);
            grown.observe(&nid("f2"), MemberState::Alive, "x:7000", None);
            owned_group_and_client(&grown)
        };
        let qkey = format!("q/{}", client.0);
        // Lifetime as a laptop: single member, so every group's set is {owner}.
        let placement = Arc::new(RwLock::new(Placement::new(owner.clone(), DEFAULT_REPLICAS)));

        let transport = Arc::new(PeerReplicaTransport::new());
        let local = Arc::new(Mutex::new(ReplicaState::new()));
        // The laptop's boot sweep stamped its (self-only) sets long ago.
        stamp_current(&local, &placement.read().unwrap());
        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            transport.clone(),
            FixedLease(1),
            local.clone(),
        );

        // Two entries committed at quorum 1 — history only the laptop holds.
        assert_eq!(log.append(&qkey, b"m1".to_vec()).await.unwrap(), 1);
        assert_eq!(log.append(&qkey, b"m2".to_vec()).await.unwrap(), 2);

        // The cluster grows 1→3; the two joiners connect.
        let mut follower_states = Vec::new();
        for node in [nid("f1"), nid("f2")] {
            placement
                .write()
                .unwrap()
                .observe(&node, MemberState::Alive, "x:7000", None);
            let state = Arc::new(Mutex::new(ReplicaState::new()));
            let (tx, rx) = mpsc::unbounded_channel();
            transport.register(node, tx);
            spawn_follower(transport.clone(), state.clone(), rx);
            follower_states.push(state);
        }
        // The founder's sweep re-stamps against the grown set: its copies are
        // gap-free and the joiners answered discovery, so it stays the anchor
        // recovery needs while the joiners are still hollow.
        stamp_current(&local, &placement.read().unwrap());

        // A joiner's sweep asks the owner to catch the key up (the plane routes
        // `ReplicaCatchUp` here): the ring change rebuilds the cached log over the
        // new set and the committed history re-commits to it.
        log.catch_up_key(&qkey).await;

        for (i, state) in follower_states.iter().enumerate() {
            let s = state.lock().unwrap();
            assert_eq!(
                s.entries(&qkey)
                    .iter()
                    .map(|e| e.offset)
                    .collect::<Vec<_>>(),
                vec![1, 2],
                "joiner f{} must hold the laptop-era history",
                i + 1
            );
            assert!(s.complete(&qkey), "the back-filled copy is complete");
        }

        // And the grown group still serves + appends (now at quorum 2).
        assert_eq!(log.append(&qkey, b"m3".to_vec()).await.unwrap(), 3);
        assert_eq!(log.read(&qkey, 0, 100).await.unwrap().len(), 3);
    }

    /// When a group's lease epoch advances (ownership lost then regained), the cached
    /// `ClusterLog` — which writes at the stale epoch and would be fenced by followers
    /// forever — is rebuilt at the new epoch and the group's keys are re-recovered
    /// from the replica set. Without the rebuild the second read would return the
    /// stale recovered range, never seeing the entry committed under the new owner.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_higher_lease_epoch_rebuilds_and_re_recovers_the_log() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let owner = nid("owner");
        // Single-node group (quorum 1): recovery reads only this node's own copy.
        let placement = Arc::new(RwLock::new(Placement::new(owner.clone(), DEFAULT_REPLICAS)));
        let (_group, client) = owned_group_and_client(&placement.read().unwrap());
        let qkey = format!("q/{}", client.0);

        // This node's follower copy holds the committed queue (offsets 1, 2).
        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        {
            let mut r = replicas.lock().unwrap();
            for (offset, record) in [(1u64, b"m1".to_vec()), (2u64, b"m2".to_vec())] {
                r.apply(
                    3,
                    &ReplOp::Append {
                        key: qkey.clone(),
                        offset,
                        seq: offset,
                        record,
                    },
                );
            }
        }

        // An established node (its sweep completed while it held the copy).
        stamp_current(&replicas, &placement.read().unwrap());
        let epoch = Arc::new(AtomicU64::new(3));
        let log = GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            BumpableLease(epoch.clone()),
            replicas.clone(),
        );

        // At epoch 3 the log recovers [1, 2].
        let got = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(got.iter().map(|e| e.offset).collect::<Vec<_>>(), vec![1, 2]);

        // Ownership was lost then regained at a higher epoch; meanwhile a committed
        // entry (offset 3) landed in this node's replica copy. Bumping the epoch must
        // rebuild the log and re-recover, picking up offset 3 the stale cached log
        // would never see.
        replicas.lock().unwrap().apply(
            7,
            &ReplOp::Append {
                key: qkey.clone(),
                offset: 3,
                seq: 3,
                record: b"m3".to_vec(),
            },
        );
        epoch.store(7, Ordering::Relaxed);

        let got = log.read(&qkey, 0, 100).await.unwrap();
        assert_eq!(
            got.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(&got[2].record, b"m3");
    }

    // -----------------------------------------------------------------------
    // ADR 0037 P2: the retained keyspace (`r/<topic>`) over the group log.
    // Topics route by the same string hashing as client ids (the router recovers
    // the placement key from `key[2..]`), so the owned/foreign pickers above
    // double as topic pickers via the client string.
    // -----------------------------------------------------------------------

    /// A retained set commits through the topic's group: quorum-replicated to the
    /// followers, compacted to the last value (on the followers too — the truncate
    /// propagates), and reporting the `(epoch, offset)` convergence token of the
    /// lease it committed under.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_retained_set_commits_through_the_group_and_replicates() {
        let owner = nid("owner");
        // A 3-node ring (R=3, quorum=2): owner + two followers.
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));

        let transport = Arc::new(PeerReplicaTransport::new());
        let f1_state = Arc::new(Mutex::new(ReplicaState::new()));
        let f2_state = Arc::new(Mutex::new(ReplicaState::new()));
        for (node, state) in [(nid("f1"), &f1_state), (nid("f2"), &f2_state)] {
            let (tx, rx) = mpsc::unbounded_channel();
            transport.register(node, tx);
            spawn_follower(transport.clone(), state.clone(), rx);
        }

        let log = Arc::new(GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            transport.clone(),
            FixedLease(1),
            Arc::new(Mutex::new(ReplicaState::new())),
        ));
        let retained = ReplicatedRetained::new(log.clone());
        let topic = owned_group_and_client(&placement.read().unwrap()).1 .0;

        assert_eq!(
            retained
                .set(
                    &topic,
                    b"v1",
                    1,
                    &mqtt_storage::app_props::AppProps::default(),
                    None,
                )
                .await
                .unwrap(),
            (1, 1),
            "the token is the committing lease epoch and the assigned offset"
        );
        assert_eq!(
            retained
                .set(
                    &topic,
                    b"v2",
                    1,
                    &mqtt_storage::app_props::AppProps::default(),
                    None,
                )
                .await
                .unwrap(),
            (1, 2)
        );

        // The owner serves exactly the compacted last value, under its token.
        let e = retained.get(&topic).await.unwrap().unwrap();
        assert_eq!(e.payload, b"v2");
        assert_eq!(e.token(), (1, 2));
        let rkey = format!("r/{topic}");
        assert_eq!(log.live_range(&rkey).await.unwrap(), Some((2, 2)));

        // Durable on the followers: the commit reached a quorum — the leader plus at
        // least ONE follower — so at least one follower holds the compacted record.
        // The other copy is best-effort spread: the fan-out abandons leftover
        // deliveries once quorum is met, so that follower's append frame may never
        // have been sent, and the (awaited, FIFO-ordered) truncate then leaves its
        // copy empty. What can never happen is a follower holding only the stale
        // superseded record: any append frame that WAS sent precedes the truncate on
        // its link, so the truncate always lands after it.
        let mut holders = 0;
        for (name, state) in [("f1", &f1_state), ("f2", &f2_state)] {
            let offsets: Vec<_> = state
                .lock()
                .unwrap()
                .entries(&rkey)
                .iter()
                .map(|e| e.offset)
                .collect();
            match offsets.as_slice() {
                [2] => holders += 1,
                [] => {} // missed the abandoned append; truncate emptied the copy
                other => panic!("{name} holds a stale/uncompacted copy: {other:?}"),
            }
        }
        assert!(
            holders >= 1,
            "the committed compacted record must be on at least one follower"
        );
    }

    /// The conflict-prevention invariant at the storage boundary: a retained write
    /// for a topic whose group this node does not own is refused as `NotOwner`
    /// (transient — route/queue per ADR 0037 §5), never applied locally where it
    /// could diverge from the owner's committed value.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_foreign_topics_retained_write_is_refused_not_diverged() {
        let owner = nid("owner");
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));

        let retained = ReplicatedRetained::new(GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            Arc::new(PeerReplicaTransport::new()),
            FixedLease(1),
            Arc::new(Mutex::new(ReplicaState::new())),
        ));

        let topic = foreign_client(&placement.read().unwrap()).0;
        let err = retained
            .set(
                &topic,
                b"x",
                0,
                &mqtt_storage::app_props::AppProps::default(),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, mqtt_storage::StorageError::NotOwner),
            "a foreign topic must refuse as NotOwner, got {err:?}"
        );
    }

    /// Stale-epoch fencing: a superseded lease-holder — its followers have moved to
    /// a newer epoch — cannot commit a retained write. The set fails (`NoQuorum`,
    /// transient) instead of committing a value a fenced owner could never converge.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stale_epoch_retained_write_is_fenced_not_committed() {
        let owner = nid("owner");
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));

        let topic = owned_group_and_client(&placement.read().unwrap()).1 .0;

        // Both followers already learned epoch 9 **for this topic's group** (a newer
        // owner wrote through them — fences are group-scoped), so this owner's
        // epoch-1 appends are fenced and cannot reach quorum. The marker key shares
        // the topic's placement key, hence its group.
        let transport = Arc::new(PeerReplicaTransport::new());
        for node in [nid("f1"), nid("f2")] {
            let state = Arc::new(Mutex::new(ReplicaState::new()));
            state.lock().unwrap().apply(
                9,
                &ReplOp::Append {
                    key: format!("q/{topic}"),
                    offset: 1,
                    seq: 1,
                    record: b"newer-owner".to_vec(),
                },
            );
            let (tx, rx) = mpsc::unbounded_channel();
            transport.register(node, tx);
            spawn_follower(transport.clone(), state, rx);
        }

        let retained = ReplicatedRetained::new(GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            transport,
            FixedLease(1),
            Arc::new(Mutex::new(ReplicaState::new())),
        ));
        let err = retained
            .set(
                &topic,
                b"stale",
                0,
                &mqtt_storage::app_props::AppProps::default(),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, mqtt_storage::StorageError::NoQuorum),
            "a fenced owner must fail the retained write with NoQuorum, got {err:?}"
        );
    }

    /// Owner takeover recovers the retained high-water: after the lease epoch
    /// advances (ownership lost and regained), the rebuilt log re-recovers the
    /// compacted retained value **with its original token** from the replica set,
    /// and the next write's token is strictly higher — no offset reuse, no token
    /// regression across the takeover.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_takeover_recovers_the_retained_value_and_its_token() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let owner = nid("owner");
        // A 3-node ring (R=3, quorum=2): owner + two followers.
        let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
        p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
        p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
        let placement = Arc::new(RwLock::new(p));

        let transport = Arc::new(PeerReplicaTransport::new());
        for node in [nid("f1"), nid("f2")] {
            let state = Arc::new(Mutex::new(ReplicaState::new()));
            let (tx, rx) = mpsc::unbounded_channel();
            transport.register(node, tx);
            spawn_follower(transport.clone(), state, rx);
        }

        let epoch = Arc::new(AtomicU64::new(2));
        let retained = ReplicatedRetained::new(GroupRoutedLog::new(
            owner.clone(),
            placement.clone(),
            transport,
            BumpableLease(epoch.clone()),
            Arc::new(Mutex::new(ReplicaState::new())),
        ));
        let topic = owned_group_and_client(&placement.read().unwrap()).1 .0;

        // Every token this owner's cache generation applies goes through the
        // ADR 0042 T1 catalog: strictly increasing per topic, or it's a violation.
        let mut tokens = crate::invariants::TokenLog::new();

        let t1 = retained
            .set(
                &topic,
                b"v1",
                1,
                &mqtt_storage::app_props::AppProps::default(),
                None,
            )
            .await
            .unwrap();
        tokens.observe_applied(&topic, t1);
        let t2 = retained
            .set(
                &topic,
                b"v2",
                1,
                &mqtt_storage::app_props::AppProps::default(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(t2, (2, 2));
        tokens.observe_applied(&topic, t2);

        // Ownership lost and regained at a higher epoch: the cached group log is
        // rebuilt and the key re-recovered from the followers' committed copies
        // (this node's own follower copy is empty — the old owner state is gone).
        epoch.store(5, Ordering::Relaxed);

        let e = retained.get(&topic).await.unwrap().unwrap();
        assert_eq!(
            e.payload, b"v2",
            "the committed value survives the takeover"
        );
        assert_eq!(
            e.token(),
            (2, 2),
            "with its original token, not a reissued one"
        );

        // The next write commits under the new epoch, after the recovered
        // high-water: strictly increasing tokens across the takeover.
        let tok = retained
            .set(
                &topic,
                b"v3",
                1,
                &mqtt_storage::app_props::AppProps::default(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(tok, (5, 3));
        assert!(tok > e.token());

        // The recovered token continues the pre-takeover cache's history: feeding
        // the post-takeover applications into the SAME log proves no token was
        // reissued or regressed across the takeover (ADR 0042 T1). The recovered
        // (2, 2) is a re-read of the held high-water, not a new application.
        tokens.observe_applied(&topic, tok);
        crate::invariants::assert_holds(&tokens.verify());
    }
}
