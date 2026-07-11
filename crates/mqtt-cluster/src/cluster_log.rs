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
use crate::lease_raft::GroupId;
use crate::placement::group_of_key;
use crate::NodeId;
use async_trait::async_trait;
use mqtt_storage::repl::{LogEntry, ReplError, ReplicatedLog};
use mqtt_storage::Offset;
use redb::{Database, Durability, ReadableTable, TableDefinition};
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included};
use std::path::Path;
use std::sync::Arc;

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
        /// The leader's per-key **write attempt** counter (ADR 0042 T7). Strictly
        /// increasing per append call within an owner generation, so two attempts
        /// that reused one offset (a failed quorum, then the next append) are
        /// ordered: a replica keeps the higher `(epoch, seq)` version of an
        /// offset, and the recovery merge resolves cross-replica conflicts the
        /// same way — divergence at an acked offset cannot survive.
        seq: u64,
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

/// The replica store's on-disk layout version (ADR 0038 T2). v1 includes the
/// per-group fence rows (ADR 0037 P4).
/// v2: retained rows (`r/` keys) carry application properties (ADR 0038 T3) —
/// the row bytes' meaning changed, so a v1 file fails closed at the gate.
/// v3: every entry row is prefixed with its writing `(epoch, seq)` (ADR 0042 T7)
/// — the tags the recovery merge resolves offset conflicts and stale tails with;
/// a v2 file fails closed at the gate.
const R_SCHEMA_VERSION: u32 = 3;

const R_ENTRIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("replica_entries");
const R_META: TableDefinition<&str, u64> = TableDefinition::new("replica_meta");
/// Per logical key, the highest truncation low-water this replica has applied (ADR 0018
/// phase 3b): entries at or below it are already-acked, so a recovery must not resurrect
/// them from a stale replica that missed the truncation.
const R_TRUNC: TableDefinition<&str, u64> = TableDefinition::new("replica_trunc");
/// `R_META` key prefix for a placement group's fence row (`fence/<group>`). The fence
/// is **per group**: lease epochs are minted from one globally-monotonic counter, so
/// two healthy groups hold different epochs at all times — a single cross-group fence
/// would let whichever group carries the highest epoch permanently fence out every
/// other group's (perfectly current) lease-holder on this replica.
const R_FENCE_PREFIX: &str = "fence/";

/// A replica's stored entries: per logical key, offset → the entry's writing
/// `(epoch, seq)` tags (ADR 0042 T7) and record bytes.
type ReplicaLogs = BTreeMap<String, BTreeMap<Offset, ((Epoch, u64), Vec<u8>)>>;

/// Encode an entry row's value: `epoch_be ++ seq_be ++ record` (schema v3).
fn r_entry_value(epoch: Epoch, seq: u64, record: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + record.len());
    out.extend_from_slice(&epoch.to_be_bytes());
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(record);
    out
}

/// Decode an entry row's value into `((epoch, seq), record)`. A short row (a
/// corrupt write) decodes as tag zero with the raw bytes, never a panic.
fn r_decode_value(v: &[u8]) -> ((Epoch, u64), Vec<u8>) {
    if v.len() < 16 {
        return ((0, 0), v.to_vec());
    }
    let epoch = Epoch::from_be_bytes(v[..8].try_into().expect("checked length"));
    let seq = u64::from_be_bytes(v[8..16].try_into().expect("checked length"));
    ((epoch, seq), v[16..].to_vec())
}

/// Per placement group, the highest leadership epoch this replica has acknowledged.
type Fences = BTreeMap<GroupId, Epoch>;

/// The state recovered from a persistent replica's tables.
struct Loaded {
    fences: Fences,
    logs: ReplicaLogs,
    truncated: BTreeMap<String, Offset>,
}

/// The logical key an op addresses (every op carries exactly one).
fn op_key(op: &ReplOp) -> &str {
    match op {
        ReplOp::Append { key, .. } | ReplOp::Truncate { key, .. } | ReplOp::Remove { key } => key,
    }
}

/// Map a `redb` error into the storage backend error.
fn rdb<E: std::fmt::Display>(e: E) -> ReplError {
    ReplError::Backend(e.to_string())
}

/// Encode a replica entry key: `len(key) ++ key ++ offset_be` (range-scannable per key).
fn r_entry_key(key: &str, offset: Offset) -> Vec<u8> {
    let kb = key.as_bytes();
    let mut out = Vec::with_capacity(4 + kb.len() + 8);
    out.extend_from_slice(&(u32::try_from(kb.len()).unwrap_or(u32::MAX)).to_be_bytes());
    out.extend_from_slice(kb);
    out.extend_from_slice(&offset.to_be_bytes());
    out
}

/// Delete a logical key's entries in the inclusive offset range `[lo, hi]`.
fn delete_entry_range(
    entries: &mut redb::Table<'_, &[u8], &[u8]>,
    key: &str,
    lo: Offset,
    hi: Offset,
) -> Result<(), ReplError> {
    let lo_k = r_entry_key(key, lo);
    let hi_k = r_entry_key(key, hi);
    let doomed: Vec<Vec<u8>> = entries
        .range(lo_k.as_slice()..=hi_k.as_slice())
        .map_err(rdb)?
        .map(|item| item.map(|(k, _)| k.value().to_vec()))
        .collect::<Result<_, _>>()
        .map_err(rdb)?;
    for k in doomed {
        entries.remove(k.as_slice()).map_err(rdb)?;
    }
    Ok(())
}

/// Decode `(key, offset)` from a replica entry key.
fn r_decode_key(bytes: &[u8]) -> (String, Offset) {
    let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let key = String::from_utf8_lossy(&bytes[4..4 + len]).into_owned();
    let mut o = [0u8; 8];
    o.copy_from_slice(&bytes[4 + len..]);
    (key, Offset::from_be_bytes(o))
}

/// The follower side of replication: a replica's stored copy plus its fence epochs.
///
/// [`apply`](ReplicaState::apply) is the entire follower protocol. The in-memory `logs`
/// map and `fences` are the source of truth for reads; when opened with
/// [`open`](ReplicaState::open) (ADR 0018 phase 3) every accepted `apply` is also
/// **write-through fsync'd** to a `redb` database (persist-before-mutate), so the
/// follower's committed copy survives a restart — what a clustered durable session needs
/// to survive a *full-cluster* restart, not only a single-node failure.
///
/// The fence is **per placement group** (the group derived from the op's key): an
/// epoch is a *group's* leadership term, and terms from one shared, globally-monotonic
/// counter mean two healthy groups always hold different epochs — fencing across
/// groups would reject a current lease-holder because an unrelated group is newer.
#[derive(Debug, Default)]
pub struct ReplicaState {
    fences: Fences,
    logs: ReplicaLogs,
    /// Per-key truncation low-water (ADR 0018 phase 3b): the highest acked offset this
    /// replica knows was dropped. Propagated on recovery so a stale replica cannot
    /// resurrect a truncated prefix.
    truncated: BTreeMap<String, Offset>,
    /// `Some` when persisting to disk; `None` for the in-memory default.
    db: Option<Arc<Database>>,
}

impl ReplicaState {
    /// A fresh, empty **in-memory** replica (non-persistent).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open (creating if absent) a **persistent** replica at `path`, recovering its
    /// fence and stored entries from disk.
    ///
    /// # Errors
    /// [`ReplError::Backend`] if the database cannot be opened or its contents decoded.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReplError> {
        let db = Database::create(path).map_err(rdb)?;
        // Layout version gate (ADR 0038 T2): stamp fresh, fail closed on foreign.
        mqtt_storage::schema::gate(&db, "replicas.redb", R_SCHEMA_VERSION).map_err(rdb)?;
        let txn = db.begin_write().map_err(rdb)?;
        {
            let _ = txn.open_table(R_ENTRIES).map_err(rdb)?;
            let _ = txn.open_table(R_META).map_err(rdb)?;
            let _ = txn.open_table(R_TRUNC).map_err(rdb)?;
        }
        txn.commit().map_err(rdb)?;
        let Loaded {
            fences,
            logs,
            truncated,
        } = Self::load(&db)?;
        Ok(Self {
            fences,
            logs,
            truncated,
            db: Some(Arc::new(db)),
        })
    }

    /// Reconstruct the in-memory cache from the on-disk tables.
    fn load(db: &Database) -> Result<Loaded, ReplError> {
        let txn = db.begin_read().map_err(rdb)?;
        let mut fences = Fences::new();
        for item in txn
            .open_table(R_META)
            .map_err(rdb)?
            .range::<&str>(..)
            .map_err(rdb)?
        {
            let (k, v) = item.map_err(rdb)?;
            if let Some(group) = k.value().strip_prefix(R_FENCE_PREFIX) {
                if let Ok(group) = group.parse::<GroupId>() {
                    fences.insert(group, v.value());
                }
            }
        }
        let mut logs: ReplicaLogs = BTreeMap::new();
        let entries = txn.open_table(R_ENTRIES).map_err(rdb)?;
        for item in entries.range::<&[u8]>(..).map_err(rdb)? {
            let (k, v) = item.map_err(rdb)?;
            let (key, offset) = r_decode_key(k.value());
            logs.entry(key)
                .or_default()
                .insert(offset, r_decode_value(v.value()));
        }
        let mut truncated = BTreeMap::new();
        let trunc = txn.open_table(R_TRUNC).map_err(rdb)?;
        for item in trunc.range::<&str>(..).map_err(rdb)? {
            let (k, v) = item.map_err(rdb)?;
            truncated.insert(k.value().to_string(), v.value());
        }
        Ok(Loaded {
            fences,
            logs,
            truncated,
        })
    }

    /// Durably apply a batch of ops (with the per-group fences they advanced to) in
    /// **one** fsync'd transaction — the on-disk mirror of the in-memory mutations,
    /// coalesced so a burst of replicated messages costs a single fsync rather than one
    /// each (ADR 0027). Ops apply in slice order. No-op (`Ok`) when in-memory.
    fn persist_batch(&self, fences: &Fences, ops: &[(Epoch, &ReplOp)]) -> Result<(), ReplError> {
        let Some(db) = &self.db else {
            return Ok(());
        };
        let mut txn = db.begin_write().map_err(rdb)?;
        txn.set_durability(Durability::Immediate); // one fsync for the whole batch (ADR 0018/0027)
        {
            let mut meta = txn.open_table(R_META).map_err(rdb)?;
            for (group, epoch) in fences {
                meta.insert(format!("{R_FENCE_PREFIX}{group}").as_str(), *epoch)
                    .map_err(rdb)?;
            }
            let mut entries = txn.open_table(R_ENTRIES).map_err(rdb)?;
            let mut trunc = txn.open_table(R_TRUNC).map_err(rdb)?;
            // Running per-key truncation low-water across this batch, overlaying the
            // committed `self.truncated`, so successive truncates in one batch compound.
            let mut wm: BTreeMap<&str, Offset> = BTreeMap::new();
            for (epoch, op) in ops {
                match op {
                    ReplOp::Append {
                        key,
                        offset,
                        seq,
                        record,
                    } => {
                        entries
                            .insert(
                                r_entry_key(key, *offset).as_slice(),
                                r_entry_value(*epoch, *seq, record).as_slice(),
                            )
                            .map_err(rdb)?;
                    }
                    ReplOp::Truncate { key, up_to } => {
                        delete_entry_range(&mut entries, key, 0, *up_to)?;
                        // Persist the monotonic per-key truncation low-water (phase 3b).
                        let base = wm
                            .get(key.as_str())
                            .copied()
                            .unwrap_or_else(|| self.watermark(key));
                        let new_wm = base.max(*up_to);
                        trunc.insert(key.as_str(), new_wm).map_err(rdb)?;
                        wm.insert(key.as_str(), new_wm);
                    }
                    ReplOp::Remove { key } => {
                        delete_entry_range(&mut entries, key, 0, Offset::MAX)?;
                        trunc.remove(key.as_str()).map_err(rdb)?;
                        wm.remove(key.as_str());
                    }
                }
            }
        }
        txn.commit().map_err(rdb)?;
        Ok(())
    }

    /// Apply one op's mutation to the in-memory copy (after it is durable on disk).
    fn apply_in_memory(&mut self, epoch: Epoch, op: &ReplOp) {
        match op {
            ReplOp::Append {
                key,
                offset,
                seq,
                record,
            } => {
                self.logs
                    .entry(key.clone())
                    .or_default()
                    .insert(*offset, ((epoch, *seq), record.clone()));
            }
            ReplOp::Truncate { key, up_to } => {
                if let Some(log) = self.logs.get_mut(key) {
                    log.retain(|o, _| o > up_to);
                }
                let wm = self.truncated.entry(key.clone()).or_default();
                *wm = (*wm).max(*up_to);
            }
            ReplOp::Remove { key } => {
                self.logs.remove(key);
                self.truncated.remove(key);
            }
        }
    }

    /// The highest leadership epoch this replica has acknowledged **for `key`'s
    /// placement group** (`0` if it has accepted nothing for that group). Fences are
    /// group-scoped because epochs are group-scoped leadership terms minted from one
    /// shared counter — see the type docs.
    #[must_use]
    pub fn fence_for_key(&self, key: &str) -> Epoch {
        self.fences.get(&group_of_key(key)).copied().unwrap_or(0)
    }

    /// Apply a lease-holder's `op` sent at `epoch`.
    ///
    /// Returns `false` (fenced) without mutating if `epoch` is stale (`<` the epoch
    /// this replica has acknowledged **for the op's placement group**). Otherwise it
    /// durably persists the op (when persistent, **before** mutating the in-memory
    /// copy), learns `epoch` (monotonically, for that group), applies the op, and
    /// returns `true`. A persist failure also returns `false` (the op was not durably
    /// stored, so the follower must not ack it).
    pub fn apply(&mut self, epoch: Epoch, op: &ReplOp) -> bool {
        let group = group_of_key(op_key(op));
        if epoch < self.fences.get(&group).copied().unwrap_or(0) {
            return false;
        }
        // Stale-attempt guard (ADR 0042 T7): an Append for an offset this replica
        // already holds at a HIGHER `(epoch, seq)` is a late duplicate of an older
        // attempt — discharged idempotently (accepted, nothing overwritten): the
        // newer version stands, on disk and in memory alike.
        if self.is_stale_attempt(epoch, op) {
            self.fences.insert(
                group,
                epoch.max(self.fences.get(&group).copied().unwrap_or(0)),
            );
            return true;
        }
        // Persist-before-mutate: a `true` ack means the op is on disk (ADR 0018 phase 3).
        let advanced = Fences::from([(group, epoch)]);
        if let Err(e) = self.persist_batch(&advanced, &[(epoch, op)]) {
            tracing::warn!(error = %e, "replica persist failed; not acking the replication op");
            return false;
        }
        self.fences.insert(group, epoch);
        self.apply_in_memory(epoch, op);
        true
    }

    /// Whether `op` is an Append for an offset already held at a higher
    /// `(epoch, seq)` — a late duplicate of a superseded attempt (ADR 0042 T7).
    fn is_stale_attempt(&self, epoch: Epoch, op: &ReplOp) -> bool {
        if let ReplOp::Append {
            key, offset, seq, ..
        } = op
        {
            if let Some((held, _)) = self.logs.get(key).and_then(|log| log.get(offset)) {
                return *held > (epoch, *seq);
            }
        }
        false
    }

    /// Durably apply a **batch** of `(epoch, op)` in one fsync'd transaction (ADR 0027).
    ///
    /// Each op is fence-checked in slice order with exactly the semantics of [`apply`]:
    /// an op whose `epoch` is older than the fence reached so far **for its group** is
    /// rejected (its slot in the returned vec is `false`) and not persisted; an accepted
    /// op advances its group's fence. All accepted ops are persisted in a **single**
    /// `Durability::Immediate` transaction, so the per-message fsync cost collapses to
    /// one per batch under load. The persist-before-ack invariant holds at batch
    /// granularity: every `true` means the op is on disk (the batch transaction
    /// committed); if that commit fails the whole batch is rejected (all `false`), since
    /// nothing was durably stored.
    pub fn apply_batch(&mut self, batch: &[(Epoch, ReplOp)]) -> Vec<bool> {
        let mut accepted = Vec::with_capacity(batch.len());
        let mut stale = vec![false; batch.len()];
        let mut advanced = Fences::new();
        let mut to_persist: Vec<(Epoch, &ReplOp)> = Vec::new();
        for (i, (epoch, op)) in batch.iter().enumerate() {
            let group = group_of_key(op_key(op));
            let running = advanced
                .get(&group)
                .or_else(|| self.fences.get(&group))
                .copied()
                .unwrap_or(0);
            if *epoch < running {
                accepted.push(false);
            } else if self.is_stale_attempt(*epoch, op) {
                // A late duplicate of a superseded attempt: accepted (idempotent
                // discharge) but neither persisted nor applied (ADR 0042 T7).
                advanced.insert(group, *epoch);
                stale[i] = true;
                accepted.push(true);
            } else {
                advanced.insert(group, *epoch);
                to_persist.push((*epoch, op));
                accepted.push(true);
            }
        }
        if to_persist.is_empty() {
            self.fences.extend(advanced);
            return accepted;
        }
        // One fsync for every accepted op in the batch (persist-before-mutate).
        if let Err(e) = self.persist_batch(&advanced, &to_persist) {
            tracing::warn!(error = %e, "replica batch persist failed; not acking the batch");
            return vec![false; batch.len()];
        }
        self.fences.extend(advanced);
        for (i, ((epoch, op), ok)) in batch.iter().zip(&accepted).enumerate() {
            if *ok && !stale[i] {
                self.apply_in_memory(*epoch, op);
            }
        }
        accepted
    }

    /// The truncation low-water for `key`: the highest acked offset this replica knows
    /// was dropped (ADR 0018 phase 3b). `0` if it has applied no truncation for the key.
    #[must_use]
    pub fn watermark(&self, key: &str) -> Offset {
        self.truncated.get(key).copied().unwrap_or(0)
    }

    /// Every logical key this replica currently holds entries for (ADR 0009 §3): a new
    /// owner reads these at takeover to find the session metadata it inherited, so it can
    /// schedule those sessions' expiry. Only non-empty logs are reported.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.logs
            .iter()
            .filter(|(_, log)| !log.is_empty())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// This replica's stored entries for `key`, in offset order (for takeover /
    /// tests). Followers store what they are sent; commit is the leader's notion.
    #[must_use]
    pub fn entries(&self, key: &str) -> Vec<LogEntry> {
        self.logs
            .get(key)
            .map(|log| {
                log.iter()
                    .map(|(offset, (_, record))| LogEntry {
                        offset: *offset,
                        record: record.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// This replica's stored entries for `key` **with their writing tags**, in
    /// offset order — what a recovery read carries so the merge can resolve
    /// same-offset conflicts and stale tails (ADR 0042 T7).
    #[must_use]
    pub fn epoch_entries(&self, key: &str) -> Vec<EpochEntry> {
        self.logs
            .get(key)
            .map(|log| {
                log.iter()
                    .map(|(offset, ((epoch, seq), record))| EpochEntry {
                        epoch: *epoch,
                        seq: *seq,
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

    /// Read `replica`'s stored log for `key` (with its truncation low-water), for a new
    /// owner to rebuild the committed log on takeover (workstream F). Returns `None` if
    /// the replica is unreachable. The default supports no recovery-reads (single-node
    /// transports).
    async fn read_replica(&self, _replica: &NodeId, _key: &str) -> Option<ReplicaRead> {
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

    async fn read_replica(&self, replica: &NodeId, key: &str) -> Option<ReplicaRead> {
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
    /// The per-key write-attempt counter (ADR 0042 T7): bumped on every append
    /// call — including failed ones, so a reused offset's next attempt carries a
    /// higher `seq` and supersedes any replica still holding the failed one.
    seq: u64,
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
                // The re-commit convention (ADR 0042 T7): recovered entries were
                // re-delivered with seqs 1..=n, so appends continue above them.
                ks.seq += 1;
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
    /// replicas on takeover (workstream F), with `floor` the highest **truncation
    /// low-water** seen across the recovery reads. Idempotent and non-clobbering:
    /// only a key with no state yet is seeded, so a re-recovery or a concurrent
    /// builder is a no-op.
    ///
    /// Appends continue from the recovered watermark **or `floor`, whichever is
    /// higher** (ADR 0042 T6, exhibit ②): a fully-truncated queue legitimately
    /// merges *empty*, but restarting its offset space at 1 would put every new
    /// acked write at or below some replica's durable truncation watermark — and
    /// any later recovery reading that replica silently drops those offsets. The
    /// key's offset space is monotonic across owners, truncation included.
    pub async fn seed_key(&self, key: &str, entries: Vec<LogEntry>, floor: Offset) {
        if entries.is_empty() && floor == 0 {
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
            // The re-commit convention (ADR 0042 T7): recovered entries were
            // re-delivered with seqs 1..=n, so appends continue above them.
            ks.seq += 1;
        }
        // Recovered entries all sit above `floor` (the merge dropped anything at
        // or below it), so both watermarks are at least `floor`.
        ks.committed = ks.committed.max(floor);
        ks.truncated = lowest.map_or(floor, |l| l.saturating_sub(1)).max(floor);
    }
}

impl<T: ReplicaTransport + Clone + 'static> ClusterLog<T> {
    /// Re-commit a recovered log to a **write quorum** at this owner's epoch
    /// ([ADR 0042](../../../docs/adr/0042-durable-plane-stress-harness.md) T6,
    /// exhibit ②).
    ///
    /// A takeover merge can *adopt* an entry that lives on a single replica — an
    /// orphan a crashed owner's partial fan-out left behind, contiguous with the
    /// committed run and therefore indistinguishable from it. Building on an
    /// adopted base without restoring its quorum spread lets the **next** takeover
    /// gap out at that offset when its read quorum misses the orphan-holder, and
    /// the merge's contiguity rule then discards the acked tail above the gap.
    ///
    /// So before a recovered key is served or appended to, every recovered entry
    /// is re-delivered to the followers (idempotent: same offset, same bytes) at
    /// this owner's **new epoch** — which also advances the followers' group
    /// fences — and each entry must reach the write quorum (the owner's own copy
    /// counts as one ack, as on [`append`](Self::append)). An owner that cannot
    /// re-commit its base must not serve it: the caller propagates
    /// [`ReplError::NoQuorum`] and recovery is retried on the next touch, the
    /// ADR 0017 posture.
    ///
    /// # Errors
    /// [`ReplError::NoQuorum`] if any recovered entry cannot reach the quorum.
    pub async fn recommit_key(&self, key: &str, entries: &[LogEntry]) -> Result<(), ReplError> {
        if entries.is_empty() {
            return Ok(());
        }
        // Fan every (entry, follower) delivery out concurrently — recovery-time,
        // one wave — and count per-entry acks, the owner's copy included. Each
        // entry is re-tagged at THIS owner's epoch with seqs 1..=n in offset
        // order (ADR 0042 T7): the re-committed base supersedes every older copy
        // of those offsets, and `seed_key` continues the seq counter above n.
        let mut acks = vec![1usize; entries.len()];
        let mut inflight = tokio::task::JoinSet::new();
        for follower in &self.followers {
            for (i, entry) in entries.iter().enumerate() {
                let transport = self.transport.clone();
                let follower = follower.clone();
                let epoch = self.lease.epoch;
                let op = ReplOp::Append {
                    key: key.to_string(),
                    offset: entry.offset,
                    seq: u64::try_from(i).unwrap_or(u64::MAX).saturating_add(1),
                    record: entry.record.clone(),
                };
                inflight.spawn(async move { (i, transport.deliver(&follower, epoch, &op).await) });
            }
        }
        while let Some(res) = inflight.join_next().await {
            if let Ok((i, true)) = res {
                acks[i] += 1;
            }
        }
        if acks.iter().all(|a| *a >= self.quorum) {
            Ok(())
        } else {
            tracing::warn!(
                key,
                epoch = self.lease.epoch,
                "takeover re-commit could not reach quorum; refusing to serve the recovered log (ADR 0042 T6)"
            );
            Err(ReplError::NoQuorum)
        }
    }
}

/// One stored entry together with its writing tags (ADR 0042 T7): the leadership
/// `epoch` it was delivered under and the leader's per-key attempt `seq`. The
/// tags order every version of an offset — across owners (epoch) and across
/// attempts that reused an offset within one owner (seq) — which is what lets a
/// recovery merge pick the surviving version instead of trusting read order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EpochEntry {
    /// The leadership epoch the entry was delivered under.
    pub epoch: Epoch,
    /// The leader's per-key write-attempt counter at delivery.
    pub seq: u64,
    /// The entry's offset in the key's log.
    pub offset: Offset,
    /// The record bytes.
    pub record: Vec<u8>,
}

/// One replica's read for recovery: its truncation low-water and its stored
/// entries, tagged (ADR 0042 T7).
#[derive(Debug, Clone, Default)]
pub struct ReplicaRead {
    /// The replica's truncation low-water for the key (ADR 0018 phase 3b).
    pub watermark: Offset,
    /// The replica's stored entries for the key, in offset order, with tags.
    pub entries: Vec<EpochEntry>,
}

/// Merge per-replica reads of one key's log into its recovered committed log: drop any
/// entry at or below the **highest truncation low-water** seen, take the union of the
/// rest by offset — resolving a same-offset conflict to the **highest `(epoch, seq)`**
/// version (ADR 0042 T7) — and keep the contiguous run from the lowest present,
/// stopping at the first gap **or the first `(epoch, seq)` regression**.
///
/// A gap marks an uncommitted tail: the owner commits offsets in order, so it cannot
/// have committed past a missing offset; reading from a **quorum** guarantees every
/// committed entry is seen (any committed entry is on ≥ quorum replicas, which intersect
/// any quorum read). The low-water filter is what stops a **stale replica** — one that
/// was down and missed a truncation — from resurrecting an already-acked prefix: the
/// recovering owner's own (current) watermark is among the reads, so a truncated offset
/// is excluded even if the stale replica still holds it (ADR 0018 phase 3b).
///
/// The tag rules are the log-matching property (ADR 0042 T7, exhibit ③): a committed
/// log's tags are non-decreasing along offsets, so (a) when two replicas hold
/// **different versions of one offset** — a failed attempt whose reuse one replica
/// missed, or a deposed owner's write superseded at a higher epoch — the higher tag is
/// the surviving version, never whichever replica happened to be read first; and (b) an
/// entry whose tag **regresses** below its predecessor's is a deposed owner's orphan
/// adopted above a newer owner's write — a stale tail, truncated rather than served
/// (the retained face: a recovered retained value can never regress behind an acked
/// one).
#[must_use]
pub fn merge_replica_logs(reads: &[ReplicaRead]) -> Vec<LogEntry> {
    let low_water = reads.iter().map(|r| r.watermark).max().unwrap_or(0);
    let mut by_offset: BTreeMap<Offset, (Epoch, u64, Vec<u8>)> = BTreeMap::new();
    for r in reads {
        for entry in &r.entries {
            if entry.offset > low_water {
                let newer = match by_offset.get(&entry.offset) {
                    Some((epoch, seq, _)) => (entry.epoch, entry.seq) > (*epoch, *seq),
                    None => true,
                };
                if newer {
                    by_offset.insert(entry.offset, (entry.epoch, entry.seq, entry.record.clone()));
                }
            }
        }
    }
    let mut out = Vec::new();
    let mut expected: Option<Offset> = None;
    let mut high: Option<(Epoch, u64)> = None;
    for (offset, (epoch, seq, record)) in by_offset {
        if let Some(e) = expected {
            if offset != e {
                break; // a gap — the rest is an uncommitted tail
            }
        }
        if let Some(h) = high {
            if (epoch, seq) < h {
                break; // a tag regression — a deposed owner's stale orphan tail
            }
        }
        expected = Some(offset + 1);
        high = Some((epoch, seq));
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
        // Every attempt gets a fresh seq (ADR 0042 T7) — a retry that reuses this
        // offset after a failed quorum supersedes the failed attempt everywhere.
        ks.seq += 1;

        let op = ReplOp::Append {
            key: key.clone(),
            offset,
            seq: ks.seq,
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

    async fn live_range(&self, key: &String) -> Result<Option<(Offset, Offset)>, ReplError> {
        // O(1) from the watermarks: the live committed range is (truncated,
        // committed], i.e. low = truncated + 1, high = committed.
        let state = self.state.lock().await;
        Ok(state.get(key).and_then(|ks| {
            (ks.committed > ks.truncated).then_some((ks.truncated + 1, ks.committed))
        }))
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

    async fn epoch_for(&self, _key: &String) -> Result<u64, ReplError> {
        // One `ClusterLog` instance exists per lease epoch; every op it fans out is
        // stamped with exactly this value (ADR 0037 token).
        Ok(self.lease.epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        merge_replica_logs, ClusterLog, ReplOp, ReplicaRead, ReplicaState, ReplicaTransport,
    };
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

    /// ADR 0038 T2: a replica store stamped by a foreign layout version refuses to
    /// open, naming both versions.
    #[test]
    fn a_foreign_replica_schema_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replicas.redb");
        drop(ReplicaState::open(&path).unwrap()); // stamped with the current version
        {
            let db = redb::Database::create(&path).unwrap();
            mqtt_storage::schema::force_version(&db, 999).unwrap();
        }
        let err = ReplicaState::open(&path).unwrap_err().to_string();
        let expected = format!("expects v{}", super::R_SCHEMA_VERSION);
        assert!(err.contains("v999") && err.contains(&expected), "{err}");
    }

    /// ADR 0018 phase 3: a persistent replica's stored entries and fence epoch survive
    /// the database being closed and reopened — what lets a clustered durable session
    /// be recovered after a *full-cluster* restart, not only a single-node failure.
    #[test]
    fn replica_state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replica.redb");
        let ap = |key: &str, offset, rec: &[u8]| ReplOp::Append {
            key: key.to_string(),
            offset,
            seq: offset,
            record: rec.to_vec(),
        };
        {
            let mut r = ReplicaState::open(&path).unwrap();
            assert!(r.apply(2, &ap("q/c", 1, b"a")));
            assert!(r.apply(2, &ap("q/c", 2, b"b")));
            assert!(r.apply(
                2,
                &ReplOp::Truncate {
                    key: "q/c".into(),
                    up_to: 1
                }
            )); // drop offset 1, keep 2
            assert_eq!(r.fence_for_key("q/c"), 2);
            // drop closes the database
        }

        let mut r = ReplicaState::open(&path).unwrap();
        // The fence persisted: a stale-epoch op is still fenced after reopen.
        assert!(
            !r.apply(1, &ap("q/c", 9, b"stale")),
            "fence survived reopen"
        );
        // The surviving entry (offset 2) is recovered.
        let entries = r.entries("q/c");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].offset, 2);
        assert_eq!(&entries[0].record, b"b");
        // The truncation low-water persisted too (ADR 0018 §3b), so recovery still
        // fences the acked prefix after a restart.
        assert_eq!(r.watermark("q/c"), 1);
    }

    fn ap(key: &str, offset: u64, rec: &[u8]) -> ReplOp {
        ReplOp::Append {
            key: key.to_string(),
            offset,
            seq: offset,
            record: rec.to_vec(),
        }
    }

    /// ADR 0027: a batch of appends applied in one `apply_batch` is durable (survives a
    /// reopen) and accepts every op — the group-commit equivalent of N single `apply`s.
    #[test]
    fn apply_batch_persists_a_whole_burst_in_one_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replica.redb");
        {
            let mut r = ReplicaState::open(&path).unwrap();
            let batch = vec![
                (5, ap("q/a", 1, b"a1")),
                (5, ap("q/a", 2, b"a2")),
                (5, ap("q/b", 1, b"b1")),
            ];
            assert_eq!(r.apply_batch(&batch), vec![true, true, true]);
            assert_eq!(r.fence_for_key("q/a"), 5);
            assert_eq!(r.fence_for_key("q/b"), 5);
        }
        // All three survive the reopen, on their respective keys.
        let r = ReplicaState::open(&path).unwrap();
        assert_eq!(
            r.entries("q/a")
                .iter()
                .map(|e| e.offset)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(r.entries("q/b").len(), 1);
        // Both groups' fences survive the reopen.
        assert_eq!(r.fence_for_key("q/a"), 5);
        assert_eq!(r.fence_for_key("q/b"), 5);
    }

    /// Per-op fencing inside a batch matches sequential `apply`: an op older than the
    /// fence reached so far is rejected in place, the rest still apply, and the fence
    /// advances to the last accepted epoch.
    #[test]
    fn apply_batch_fences_stale_ops_in_slice_order() {
        let mut r = ReplicaState::new();
        let batch = vec![
            (3, ap("k", 1, b"x")), // accept, fence -> 3
            (2, ap("k", 2, b"y")), // 2 < 3 -> reject
            (4, ap("k", 3, b"z")), // accept, fence -> 4
        ];
        assert_eq!(r.apply_batch(&batch), vec![true, false, true]);
        assert_eq!(r.fence_for_key("k"), 4);
        // Only the accepted offsets are present (the rejected one never applied).
        assert_eq!(
            r.entries("k").iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    /// Ops apply in slice order within the batch, including a truncate after appends —
    /// and the truncation low-water survives a reopen (ADR 0018 §3b under ADR 0027).
    #[test]
    fn apply_batch_applies_in_order_with_a_trailing_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replica.redb");
        {
            let mut r = ReplicaState::open(&path).unwrap();
            let batch = vec![
                (7, ap("q/c", 1, b"a")),
                (7, ap("q/c", 2, b"b")),
                (7, ap("q/c", 3, b"c")),
                (
                    7,
                    ReplOp::Truncate {
                        key: "q/c".into(),
                        up_to: 2,
                    },
                ),
            ];
            assert_eq!(r.apply_batch(&batch), vec![true, true, true, true]);
            // Only offset 3 survives the in-batch truncate.
            assert_eq!(
                r.entries("q/c")
                    .iter()
                    .map(|e| e.offset)
                    .collect::<Vec<_>>(),
                vec![3]
            );
            assert_eq!(r.watermark("q/c"), 2);
        }
        // ...and that state is exactly what reopens from disk (one fsync'd commit).
        let r = ReplicaState::open(&path).unwrap();
        assert_eq!(
            r.entries("q/c")
                .iter()
                .map(|e| e.offset)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(r.watermark("q/c"), 2);
    }

    /// Fences are **per placement group**. Lease epochs are minted from ONE
    /// globally-monotonic counter, so two healthy groups always run at different
    /// epochs — a shared cross-group fence would let whichever group carries the
    /// highest epoch permanently reject every other group's (perfectly current)
    /// lease-holder on this replica. This is the regression test for exactly that
    /// bug, exposed by the first workload replicating two groups through one
    /// follower (ADR 0037 P4's everyday-race test).
    #[test]
    fn a_groups_fence_does_not_reject_another_groups_older_epoch() {
        use crate::placement::group_of_key;
        // Two keys in different placement groups.
        let ka = "q/k0".to_string();
        let kb = (1..10_000)
            .map(|i| format!("q/k{i}"))
            .find(|k| group_of_key(k) != group_of_key(&ka))
            .expect("some key lands in another group");

        let mut r = ReplicaState::new();
        // Group A's holder writes at a late epoch (minted after many assignments)...
        assert!(r.apply(200, &ap(&ka, 1, b"hot")));
        // ...which must NOT fence group B's current holder at its own, older epoch.
        assert!(
            r.apply(60, &ap(&kb, 1, b"current")),
            "another group's current lease-holder must not be fenced"
        );
        // Within one group the fence still holds exactly as before.
        assert!(
            !r.apply(199, &ap(&ka, 2, b"stale")),
            "same-group fence holds"
        );
        assert!(r.apply(201, &ap(&ka, 2, b"newer")));
        assert_eq!(r.fence_for_key(&ka), 201);
        assert_eq!(r.fence_for_key(&kb), 60);
    }

    /// A single-element `apply_batch` is exactly `apply` — same accept and same fence.
    #[test]
    fn apply_batch_of_one_equals_apply() {
        let mut a = ReplicaState::new();
        let mut b = ReplicaState::new();
        assert_eq!(
            a.apply(9, &ap("k", 1, b"v")),
            b.apply_batch(&[(9, ap("k", 1, b"v"))])[0]
        );
        assert_eq!(a.fence_for_key("k"), b.fence_for_key("k"));
        assert_eq!(a.entries("k"), b.entries("k"));
    }

    /// Deterministic in-process transport holding the follower replicas, with an
    /// injectable reachable-set (partition / kill a replica). Every reachable
    /// delivery's accept/refuse decision is recorded into a [`FenceLog`], so any
    /// test over this transport can close with the ADR 0042 fencing check.
    #[derive(Debug)]
    struct SimCluster {
        replicas: Mutex<BTreeMap<NodeId, ReplicaState>>,
        reachable: Mutex<BTreeSet<NodeId>>,
        fences: Mutex<crate::invariants::FenceLog>,
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
                fences: Mutex::new(crate::invariants::FenceLog::new()),
            })
        }

        /// Assert the epoch-fencing invariant over every replica decision this
        /// transport has carried (ADR 0042 T1 catalog).
        fn assert_fencing_held(&self) {
            crate::invariants::assert_holds(&self.fences.lock().unwrap().verify());
        }

        /// Take `node` offline (its deliveries now fail) — a crash or partition.
        fn down(&self, node: &NodeId) {
            self.reachable.lock().unwrap().remove(node);
        }

        /// Bring `node` back online (a heal).
        fn up(&self, node: &NodeId) {
            self.reachable.lock().unwrap().insert(node.clone());
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
                return false; // unreachable, not a fencing decision
            }
            let mut replicas = self.replicas.lock().unwrap();
            let accepted = replicas
                .get_mut(replica)
                .is_some_and(|r| r.apply(epoch, op));
            self.fences.lock().unwrap().observe(
                crate::placement::group_of_key(super::op_key(op)),
                epoch,
                accepted,
            );
            accepted
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
        // The catalog states the same thing once, over every decision the
        // transport carried: no replica accepted a stale epoch (ADR 0042 T1).
        sim.assert_fencing_held();
    }

    /// Takeover re-commit (ADR 0042 T6, exhibit ②): an owner that cannot restore
    /// its recovered base to a write quorum refuses to serve it (`NoQuorum`), and
    /// once the followers heal the same re-commit succeeds — landing the adopted
    /// base on every reachable replica at the owner's epoch (advancing fences).
    #[tokio::test]
    async fn recommit_requires_a_write_quorum_for_the_recovered_base() {
        let (log, sim, followers) = group(5);
        let k = "x".to_string();
        let base = vec![
            LogEntry {
                offset: 1,
                record: b"adopted-orphan".to_vec(),
            },
            LogEntry {
                offset: 2,
                record: b"acked".to_vec(),
            },
        ];

        // Both followers down: only the owner's own (volatile) ack — no quorum,
        // no service.
        sim.down(&followers[0]);
        sim.down(&followers[1]);
        assert!(matches!(
            log.recommit_key(&k, &base).await,
            Err(mqtt_storage::repl::ReplError::NoQuorum)
        ));

        // One follower heals: owner + one replica = quorum. The base spreads to
        // the reachable follower and its group fence advances to the new epoch.
        sim.up(&followers[0]);
        log.recommit_key(&k, &base).await.unwrap();
        assert_eq!(sim.entries(&followers[0], &k), vec![1, 2]);
        assert!(sim.entries(&followers[1], &k).is_empty());
        sim.assert_fencing_held();
    }

    /// `live_range` reflects the committed watermarks: an uncommitted (below-quorum)
    /// append is excluded, and truncation advances the low edge. This is the O(1)
    /// count the queue cap relies on.
    #[tokio::test]
    async fn live_range_reflects_committed_watermarks() {
        let (log, sim, followers) = group(1);
        let k = "x".to_string();
        assert_eq!(log.live_range(&k).await.unwrap(), None);

        for _ in 0..4 {
            log.append(&k, b"x".to_vec()).await.unwrap();
        }
        assert_eq!(log.live_range(&k).await.unwrap(), Some((1, 4)));

        // An append that cannot reach quorum does not commit → range unchanged.
        sim.down(&followers[0]);
        sim.down(&followers[1]);
        assert!(log.append(&k, b"lost".to_vec()).await.is_err());
        assert_eq!(log.live_range(&k).await.unwrap(), Some((1, 4)));

        // Truncation advances the low edge (local-first, no quorum needed).
        log.truncate(&k, 2).await.unwrap();
        assert_eq!(log.live_range(&k).await.unwrap(), Some((3, 4)));
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
        let msg = Message::new(
            "t".to_string(),
            bytes::Bytes::from_static(b"payload"),
            QoS::AtLeastOnce,
            false,
        );
        store.ensure_session(&c).await.unwrap();
        store.enqueue(&c, &msg).await.unwrap();

        let pending = store.pending(&c, 0, 100).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0].message.payload[..], b"payload");
    }

    /// A tagged input entry for merge tests: epoch 1, seq = offset (one attempt
    /// per offset — tags non-decreasing along offsets, a legal committed log).
    fn entry(offset: u64, record: &[u8]) -> super::EpochEntry {
        super::EpochEntry {
            epoch: 1,
            seq: offset,
            offset,
            record: record.to_vec(),
        }
    }

    /// A replica read with no truncation (`watermark = 0`).
    fn rd(entries: Vec<super::EpochEntry>) -> ReplicaRead {
        ReplicaRead {
            watermark: 0,
            entries,
        }
    }

    fn offsets(entries: &[super::LogEntry]) -> Vec<u64> {
        entries.iter().map(|e| e.offset).collect()
    }

    /// The quorum-recovery merge takes the contiguous run from the union of reads.
    #[test]
    fn merge_takes_the_contiguous_run_from_a_quorum() {
        // Two overlapping replica reads; their union is contiguous 1..=4.
        let merged = merge_replica_logs(&[
            rd(vec![entry(1, b"a"), entry(2, b"b"), entry(3, b"c")]),
            rd(vec![entry(2, b"b"), entry(3, b"c"), entry(4, b"d")]),
        ]);
        assert_eq!(offsets(&merged), vec![1, 2, 3, 4]);
    }

    /// A gap drops the uncommitted tail beyond it; a truncated prefix starts above
    /// 1; nothing merges to nothing.
    #[test]
    fn merge_stops_at_gaps_and_handles_truncation() {
        // 1,2 then a gap at 3 with an uncommitted 4 → recovered [1,2].
        assert_eq!(
            offsets(&merge_replica_logs(&[rd(vec![
                entry(1, b"a"),
                entry(2, b"b"),
                entry(4, b"d")
            ])])),
            vec![1, 2],
        );
        // Truncated to start at 5 (acked) → recovered [5,6].
        assert_eq!(
            offsets(&merge_replica_logs(&[rd(vec![
                entry(5, b"e"),
                entry(6, b"f")
            ])])),
            vec![5, 6],
        );
        assert!(merge_replica_logs(&[]).is_empty());
        assert!(merge_replica_logs(&[rd(vec![])]).is_empty());
    }

    /// Exhibit ③ regression (ADR 0042 T7): when two replicas hold **different
    /// versions of one offset** — a failed attempt whose reuse one replica missed
    /// — the merge picks the higher `(epoch, seq)` version, whichever replica is
    /// read first. Before the tags, read order decided, and a never-acked record
    /// could shadow the acked one.
    #[test]
    fn merge_resolves_a_same_offset_conflict_by_tag_not_read_order() {
        let tagged = |epoch, seq, offset, rec: &[u8]| super::EpochEntry {
            epoch,
            seq,
            offset,
            record: rec.to_vec(),
        };
        // Replica A holds the failed attempt (seq 1); replica B holds the acked
        // reuse (seq 2, different bytes) plus the next acked entry.
        let a = rd(vec![tagged(1, 1, 1, b"failed-attempt")]);
        let b = rd(vec![
            tagged(1, 2, 1, b"acked-reuse"),
            tagged(1, 3, 2, b"next"),
        ]);
        for reads in [[a.clone(), b.clone()], [b, a]] {
            let merged = merge_replica_logs(&reads);
            assert_eq!(offsets(&merged), vec![1, 2]);
            assert_eq!(
                &merged[0].record, b"acked-reuse",
                "the higher (epoch, seq) version wins under either read order"
            );
        }
    }

    /// Exhibit ③'s retained face (ADR 0042 T7): a deposed owner's never-acked
    /// orphans sitting at offsets **above** a newer owner's write are a stale
    /// tail — the tags regress along offsets, so the merge truncates them
    /// (log matching) instead of adopting them over the acked value.
    #[test]
    fn merge_truncates_a_tail_whose_tag_regresses() {
        let tagged = |epoch, seq, offset, rec: &[u8]| super::EpochEntry {
            epoch,
            seq,
            offset,
            record: rec.to_vec(),
        };
        // Replica A: the new owner's re-committed base (epoch 3). Replica B: a
        // deposed owner's unacked orphans at offsets 2..3 (epoch 2).
        let a = rd(vec![tagged(3, 1, 1, b"acked-e3")]);
        let b = rd(vec![
            tagged(2, 5, 2, b"stale-orphan"),
            tagged(2, 6, 3, b"stale-orphan-2"),
        ]);
        for reads in [[a.clone(), b.clone()], [b, a]] {
            let merged = merge_replica_logs(&reads);
            assert_eq!(
                offsets(&merged),
                vec![1],
                "the epoch-regressing tail is truncated, not adopted"
            );
            assert_eq!(&merged[0].record, b"acked-e3");
        }
    }

    /// The replica-side stale-attempt guard (ADR 0042 T7): a late duplicate of a
    /// superseded attempt — same offset, lower `(epoch, seq)` — is discharged
    /// idempotently: accepted, but the newer version stands, across a reopen too.
    #[test]
    fn a_replica_keeps_the_newer_version_of_an_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replica.redb");
        let ap = |seq, rec: &[u8]| ReplOp::Append {
            key: "q/c".to_string(),
            offset: 1,
            seq,
            record: rec.to_vec(),
        };
        {
            let mut r = ReplicaState::open(&path).unwrap();
            assert!(
                r.apply(2, &ap(4, b"acked-reuse")),
                "the current attempt lands"
            );
            assert!(
                r.apply(2, &ap(3, b"late-duplicate")),
                "a stale attempt is accepted (idempotent discharge)..."
            );
            assert_eq!(
                &r.entries("q/c")[0].record,
                b"acked-reuse",
                "...but never overwrites the newer version"
            );
        }
        // The guard held on disk too: the reopened replica serves the newer bytes.
        let r = ReplicaState::open(&path).unwrap();
        assert_eq!(&r.entries("q/c")[0].record, b"acked-reuse");
        assert_eq!(r.epoch_entries("q/c")[0].seq, 4);
    }

    /// ADR 0018 phase 3b: a stale replica that missed a truncation must not resurrect
    /// the already-acked prefix. The recovery merge drops every entry at or below the
    /// highest truncation low-water seen across the quorum.
    #[test]
    fn merge_does_not_resurrect_a_stale_replicas_truncated_prefix() {
        // A live replica truncated up to 5 (watermark 5) holding [6,7]; a stale replica
        // (down through the truncation, watermark 0) still holds [1..7].
        let live = ReplicaRead {
            watermark: 5,
            entries: vec![entry(6, b"f"), entry(7, b"g")],
        };
        let stale = ReplicaRead {
            watermark: 0,
            entries: vec![
                entry(1, b"a"),
                entry(2, b"b"),
                entry(3, b"c"),
                entry(4, b"d"),
                entry(5, b"e"),
                entry(6, b"f"),
                entry(7, b"g"),
            ],
        };
        // Whatever order the quorum read returns them, the acked prefix [1..5] is gone.
        let merged = merge_replica_logs(&[stale.clone(), live.clone()]);
        assert_eq!(
            offsets(&merged),
            vec![6, 7],
            "the truncated prefix must not be resurrected"
        );
        assert_eq!(offsets(&merge_replica_logs(&[live, stale])), vec![6, 7]);
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
        let le = |offset, rec: &[u8]| LogEntry {
            offset,
            record: rec.to_vec(),
        };
        logs.insert("q/c".to_string(), vec![le(1, b"m1"), le(2, b"m2")]);
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

    /// ADR 0018 phase 5: a durable session's log survives a **full-cluster restart**.
    /// Committed entries replicated to a quorum of *persistent* replicas are recovered
    /// from disk after every node restarts, and a new owner serves them. (Restart
    /// durability needs R≥2 — a 1-node group keeps committed data in the leader's
    /// in-memory log until a follower has it; this is exactly why followers persist.)
    #[tokio::test]
    async fn a_durable_session_log_survives_a_full_restart_via_persisted_replicas() {
        let tmp = tempfile::tempdir().unwrap();
        let (pb, pc) = (tmp.path().join("b.redb"), tmp.path().join("c.redb"));
        let k = "q/client".to_string();
        let ap = |offset, rec: &[u8]| ReplOp::Append {
            key: k.clone(),
            offset,
            seq: offset,
            record: rec.to_vec(),
        };

        // Everything acknowledged goes in the ledger; the recovery is verified
        // against it below (ADR 0042 T1: acked durability, no resurrection).
        let mut ledger = crate::invariants::AckLedger::new();

        // The owner replicated two committed entries (epoch 7) to a quorum of persistent
        // followers; their on-disk replica copies fsync'd before the ack.
        {
            let mut b = ReplicaState::open(&pb).unwrap();
            let mut c = ReplicaState::open(&pc).unwrap();
            for (off, rec) in [(1u64, b"m1".as_slice()), (2, b"m2")] {
                assert!(b.apply(7, &ap(off, rec)));
                assert!(c.apply(7, &ap(off, rec)));
                ledger.ack_append(&k, off, rec);
            }
            // drop → close the databases (every node is now "down")
        }

        // --- full-cluster restart: reopen the persisted replicas from disk ---
        let b = ReplicaState::open(&pb).unwrap();
        let c = ReplicaState::open(&pc).unwrap();

        // A surviving node takes over and recovers the group from a quorum of the
        // reopened replicas...
        let recovered = merge_replica_logs(&[
            ReplicaRead {
                watermark: b.watermark(&k),
                entries: b.epoch_entries(&k),
            },
            ReplicaRead {
                watermark: c.watermark(&k),
                entries: c.epoch_entries(&k),
            },
        ]);
        assert_eq!(
            recovered.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1, 2],
            "the committed log is recovered from the persisted replicas"
        );
        crate::invariants::assert_holds(&ledger.verify_recovered(&k, &recovered));

        // ...and serves it through a recovered ClusterLog, continuing to append.
        let owner = n("d");
        let set = vec![n("d"), n("b"), n("c")];
        let lease = OwnershipLease {
            holder: owner.clone(),
            epoch: 8, // a fresh epoch fences the old owner
        };
        let mut logs = BTreeMap::new();
        logs.insert(k.clone(), recovered);
        let log =
            ClusterLog::recovered(owner, lease, &set, SimCluster::new(&[n("b"), n("c")]), logs);
        let served = log.read(&k, 0, 100).await.unwrap();
        assert_eq!(
            served.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(&served[1].record, b"m2");
        crate::invariants::assert_holds(&ledger.verify_recovered(&k, &served));
    }
}
