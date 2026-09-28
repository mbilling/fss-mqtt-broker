//! The replica store on the segment log (ADR 0078 T2).
//!
//! `ReplicaState` keeps every live entry, fence, truncation low-water and
//! caught-up set in memory; its store only has to record, in order, what
//! changed. This module is that record, on [`SegmentLog`]: one directory per
//! shard under `<data dir>/replicas-log/`, one record per change, and a replay
//! that rebuilds exactly the state the redb tables would have held.
//!
//! Every record mirrors one redb row operation, so the two backends cannot mean
//! different things:
//!
//! | record | redb equivalent |
//! |---|---|
//! | `Format{version, shards}` | the schema stamp and the committed shard count |
//! | `Append{key, offset, epoch, seq, record}` | `replica_entries` insert |
//! | `Truncate{key, up_to, low_water}` | delete `(key, 0..=up_to)`, then `replica_trunc[key] = low_water` |
//! | `Remove{key}` | delete every entry of `key` and its `replica_trunc` row |
//! | `Fence{group, epoch}` | `replica_meta["fence/<group>"] = epoch` |
//! | `Caught{group, members}` | `replica_caught_up[group] = members` |
//!
//! A `Truncate` carries both bounds because they differ: a late, lower ack
//! deletes through its own `up_to` while the low-water stays at the committed
//! maximum, and an entry left between the two (a stale leftover) must survive a
//! replay exactly as it survives in the table.

use super::{drop_through, CaughtUp, Fences, Loaded, ReplicaLogs, ReplicaState, R_MAX_SHARDS};
use crate::lease::Epoch;
use crate::lease_raft::GroupId;
use crate::segment_log::{LogError, SegmentLog};
use crate::NodeId;
use mqtt_storage::repl::ReplError;
use mqtt_storage::Offset;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The log store's directory under the data dir.
pub const LOG_DIR: &str = "replicas-log";
/// Where an import from redb is written before it is renamed into place.
const STAGING_DIR: &str = "replicas-log.importing";
/// The log format this build reads and writes.
pub const FORMAT_VERSION: u32 = 1;

const K_FORMAT: u8 = 1;
const K_APPEND: u8 = 2;
const K_TRUNCATE: u8 = 3;
const K_REMOVE: u8 = 4;
const K_FENCE: u8 = 5;
const K_CAUGHT: u8 = 6;

/// Records per flush during an import: bounded, so a large store is not one
/// giant buffer, and big enough that the import is not flush-bound.
const IMPORT_CHUNK: usize = 4096;

fn lg(e: &LogError) -> ReplError {
    ReplError::Backend(e.to_string())
}

/// A shard's directory.
#[must_use]
pub fn shard_dir(root: &Path, shard: usize) -> PathBuf {
    root.join(LOG_DIR).join(format!("shard-{shard}"))
}

// ── encoding ─────────────────────────────────────────────────────────────────

/// One change, as the log records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rec<'a> {
    /// The format stamp: version, and the store's committed shard count.
    Format {
        /// Log format version.
        version: u32,
        /// Shards in this store.
        shards: u32,
    },
    /// An entry.
    Append {
        /// The logical key.
        key: &'a str,
        /// Its offset.
        offset: Offset,
        /// The leadership epoch it was written under.
        epoch: Epoch,
        /// Its attempt sequence (ADR 0042 T7).
        seq: u64,
        /// The payload.
        record: &'a [u8],
    },
    /// Entries of `key` through `up_to` dropped; its low-water is `low_water`.
    Truncate {
        /// The logical key.
        key: &'a str,
        /// Delete entries at or below this offset.
        up_to: Offset,
        /// The resulting monotonic low-water.
        low_water: Offset,
    },
    /// Every entry of `key` and its low-water dropped.
    Remove {
        /// The logical key.
        key: &'a str,
    },
    /// A group's fence.
    Fence {
        /// The placement group.
        group: GroupId,
        /// Its fence epoch.
        epoch: Epoch,
    },
    /// A group's caught-up replica set.
    Caught {
        /// The placement group.
        group: GroupId,
        /// The node ids.
        members: Vec<String>,
    },
}

fn put_str(b: &mut Vec<u8>, s: &[u8]) {
    let len = u32::try_from(s.len()).expect("a key or record is bounded by the log's MAX_PAYLOAD");
    b.extend_from_slice(&len.to_be_bytes());
    b.extend_from_slice(s);
}

impl Rec<'_> {
    /// `(kind, payload)` for the log.
    #[must_use]
    pub fn encode(&self) -> (u8, Vec<u8>) {
        let mut b = Vec::new();
        let kind = match self {
            Rec::Format { version, shards } => {
                b.extend_from_slice(&version.to_be_bytes());
                b.extend_from_slice(&shards.to_be_bytes());
                K_FORMAT
            }
            Rec::Append {
                key,
                offset,
                epoch,
                seq,
                record,
            } => {
                b.extend_from_slice(&offset.to_be_bytes());
                b.extend_from_slice(&epoch.to_be_bytes());
                b.extend_from_slice(&seq.to_be_bytes());
                put_str(&mut b, key.as_bytes());
                b.extend_from_slice(record);
                K_APPEND
            }
            Rec::Truncate {
                key,
                up_to,
                low_water,
            } => {
                b.extend_from_slice(&up_to.to_be_bytes());
                b.extend_from_slice(&low_water.to_be_bytes());
                b.extend_from_slice(key.as_bytes());
                K_TRUNCATE
            }
            Rec::Remove { key } => {
                b.extend_from_slice(key.as_bytes());
                K_REMOVE
            }
            Rec::Fence { group, epoch } => {
                b.extend_from_slice(&group.to_be_bytes());
                b.extend_from_slice(&epoch.to_be_bytes());
                K_FENCE
            }
            Rec::Caught { group, members } => {
                b.extend_from_slice(&group.to_be_bytes());
                for m in members {
                    put_str(&mut b, m.as_bytes());
                }
                K_CAUGHT
            }
        };
        (kind, b)
    }
}

/// A bounds-checked reader over one payload: a malformed record is an error,
/// never a guess.
struct Reader<'a> {
    b: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.b.len() < n {
            return Err(format!("{n} bytes wanted, {} left", self.b.len()));
        }
        let (head, rest) = self.b.split_at(n);
        self.b = rest;
        Ok(head)
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("4")))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("8")))
    }
    fn prefixed(&mut self) -> Result<&'a [u8], String> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.b)
    }
    fn utf8(b: &'a [u8]) -> Result<&'a str, String> {
        std::str::from_utf8(b).map_err(|e| format!("a key that is not UTF-8: {e}"))
    }
}

/// Decode one record.
///
/// # Errors
/// A payload that does not decode as `kind`.
pub fn decode(kind: u8, payload: &[u8]) -> Result<Rec<'_>, String> {
    let mut r = Reader { b: payload };
    let rec = match kind {
        K_FORMAT => Rec::Format {
            version: r.u32()?,
            shards: r.u32()?,
        },
        K_APPEND => {
            let offset = r.u64()?;
            let epoch = r.u64()?;
            let seq = r.u64()?;
            let key = Reader::utf8(r.prefixed()?)?;
            Rec::Append {
                key,
                offset,
                epoch,
                seq,
                record: r.rest(),
            }
        }
        K_TRUNCATE => {
            let up_to = r.u64()?;
            let low_water = r.u64()?;
            Rec::Truncate {
                key: Reader::utf8(r.rest())?,
                up_to,
                low_water,
            }
        }
        K_REMOVE => Rec::Remove {
            key: Reader::utf8(r.rest())?,
        },
        K_FENCE => Rec::Fence {
            group: r.u64()?,
            epoch: r.u64()?,
        },
        K_CAUGHT => {
            let group = r.u64()?;
            let mut members = Vec::new();
            while !r.b.is_empty() {
                members.push(Reader::utf8(r.prefixed()?)?.to_string());
            }
            Rec::Caught { group, members }
        }
        other => return Err(format!("unknown record kind {other}")),
    };
    Ok(rec)
}

// ── replay ───────────────────────────────────────────────────────────────────

/// What a replay rebuilds, before it becomes a `ReplicaState`.
#[derive(Debug, Default)]
struct Replay {
    fences: Fences,
    logs: ReplicaLogs,
    truncated: BTreeMap<String, Offset>,
    caught_up: CaughtUp,
    format: Option<(u32, u32)>,
}

impl Replay {
    /// Apply one record with its redb row's exact meaning (module docs).
    fn apply(&mut self, rec: Rec<'_>) {
        match rec {
            Rec::Format { version, shards } => self.format = Some((version, shards)),
            Rec::Append {
                key,
                offset,
                epoch,
                seq,
                record,
            } => {
                self.logs
                    .entry(key.to_string())
                    .or_default()
                    .insert(offset, ((epoch, seq), record.to_vec()));
            }
            Rec::Truncate {
                key,
                up_to,
                low_water,
            } => {
                if let Some(log) = self.logs.get_mut(key) {
                    drop_through(log, up_to);
                    if log.is_empty() {
                        // A table holds no rows for a key with no entries; the
                        // rebuilt map must not hold an empty one either.
                        self.logs.remove(key);
                    }
                }
                self.truncated.insert(key.to_string(), low_water);
            }
            Rec::Remove { key } => {
                self.logs.remove(key);
                self.truncated.remove(key);
            }
            Rec::Fence { group, epoch } => {
                self.fences.insert(group, epoch);
            }
            Rec::Caught { group, members } => {
                self.caught_up.insert(
                    group,
                    members.into_iter().map(NodeId).collect::<BTreeSet<_>>(),
                );
            }
        }
    }
}

/// An opened log store: one log per shard, and the state they replayed to.
#[derive(Debug)]
pub(super) struct Opened {
    /// One log per shard, in shard order.
    pub(super) logs: Vec<SegmentLog>,
    /// The state the logs replay to.
    pub(super) loaded: Loaded,
}

fn open_shard(dir: &Path, segment_bytes: u64) -> Result<(SegmentLog, Replay), ReplError> {
    let mut replay = Replay::default();
    let (log, recovery) = SegmentLog::open(dir, segment_bytes, |lsn, kind, payload| {
        let rec = decode(kind, payload).map_err(|detail| LogError::Corrupt {
            segment: dir.display().to_string(),
            offset: lsn,
            detail: format!("record {lsn}: {detail}"),
        })?;
        replay.apply(rec);
        Ok(())
    })
    .map_err(|e| lg(&e))?;
    if recovery.torn_bytes > 0 || recovery.empty_segments_removed > 0 {
        tracing::warn!(
            shard = %dir.display(),
            torn_bytes = recovery.torn_bytes,
            empty_segments_removed = recovery.empty_segments_removed,
            "replica log recovered from an interrupted write: the torn tail was never \
             acknowledged and is discarded"
        );
    }
    Ok((log, replay))
}

/// Open the log store under `root` (the data dir), creating a fresh one with
/// `shards` shards if none exists.
///
/// # Errors
/// A shard that is missing, corrupt, of another format version, or stamped
/// with a different shard count.
pub(super) fn open(root: &Path, shards: usize, segment_bytes: u64) -> Result<Opened, ReplError> {
    let base = root.join(LOG_DIR);
    // Snapshot BEFORE anything is created (as the redb path does): once shard 0
    // exists, the store is committed and a missing shard is lost data.
    let committed = shard_dir(root, 0).exists();
    std::fs::create_dir_all(&base)
        .map_err(|e| ReplError::Backend(format!("creating {}: {e}", base.display())))?;
    // Shard 0 is replayed once: its format stamp is where the committed shard
    // count lives, and that — never the argument — decides K (ADR 0076 T2).
    let (log0, replay0) = open_shard(&shard_dir(root, 0), segment_bytes)?;
    let k = match replay0.format {
        Some((_, n)) => (n as usize).max(1),
        None if log0.next_lsn() == 1 => shards.clamp(1, R_MAX_SHARDS),
        None => {
            return Err(ReplError::Backend(format!(
                "{} holds records but no format stamp",
                shard_dir(root, 0).display()
            )))
        }
    };
    let mut loaded = Loaded {
        fences: Fences::new(),
        logs: ReplicaLogs::new(),
        truncated: BTreeMap::new(),
        caught_up: CaughtUp::new(),
    };
    let mut shard_logs = Vec::with_capacity(k);
    let mut first_shard = Some((log0, replay0));
    for shard in 0..k {
        let dir = shard_dir(root, shard);
        let (mut log, replay) = if let Some(opened) = first_shard.take() {
            opened
        } else {
            if committed && !dir.exists() {
                return Err(ReplError::Backend(format!(
                    "replica log at {} has {k} shards but {} is missing — a shard is \
                     not optional: restore it (or the whole data dir) from backup",
                    base.display(),
                    dir.display()
                )));
            }
            open_shard(&dir, segment_bytes)?
        };
        match replay.format {
            Some((FORMAT_VERSION, n)) if n as usize == k => {}
            Some((FORMAT_VERSION, n)) => {
                return Err(ReplError::Backend(format!(
                    "{} is stamped as one of {n} shards, but the store has {k}",
                    dir.display()
                )))
            }
            Some((v, _)) => {
                return Err(ReplError::Backend(format!(
                    "{} is replica log format {v}; this build reads format {FORMAT_VERSION} \
                     — fail closed rather than misread it",
                    dir.display()
                )))
            }
            None if log.next_lsn() == 1 => {
                // Fresh, or created and never stamped before a crash: stamp it.
                let (kind, payload) = Rec::Format {
                    version: FORMAT_VERSION,
                    shards: u32::try_from(k).unwrap_or(1),
                }
                .encode();
                log.append(&[(kind, &payload)]).map_err(|e| lg(&e))?;
            }
            None => {
                return Err(ReplError::Backend(format!(
                    "{} holds records but no format stamp",
                    dir.display()
                )))
            }
        }
        loaded.fences.extend(replay.fences);
        loaded.logs.extend(replay.logs);
        loaded.truncated.extend(replay.truncated);
        loaded.caught_up.extend(replay.caught_up);
        shard_logs.push(log);
    }
    Ok(Opened {
        logs: shard_logs,
        loaded,
    })
}

// ── import from redb ─────────────────────────────────────────────────────────

/// Write `state` (a redb store, already loaded) into a fresh log store under
/// `root` with the same shard count, flushed, then rename it into place.
///
/// Written to a staging directory first: a crash mid-import leaves the staging
/// directory — deleted and redone on the next open — and never a half-written
/// `replicas-log`. The redb files are left untouched; the caller renames them
/// once this returns.
///
/// # Errors
/// I/O failures writing, flushing or renaming.
pub(super) fn import(
    root: &Path,
    state: &ReplicaState,
    segment_bytes: u64,
) -> Result<(), ReplError> {
    let staging = root.join(STAGING_DIR);
    let io = |what: &str, e: std::io::Error| ReplError::Backend(format!("import: {what}: {e}"));
    if staging.exists() {
        std::fs::remove_dir_all(&staging).map_err(|e| io("clearing a stale staging dir", e))?;
    }
    let k = state.shard_count();
    let shard_of = |group: GroupId| super::shard_of_group(group, k);
    for shard in 0..k {
        let (mut log, _) = SegmentLog::open(
            staging.join(format!("shard-{shard}")),
            segment_bytes,
            |_, _, _| Ok(()),
        )
        .map_err(|e| lg(&e))?;
        let mut recs: Vec<(u8, Vec<u8>)> = vec![Rec::Format {
            version: FORMAT_VERSION,
            shards: u32::try_from(k).unwrap_or(1),
        }
        .encode()];
        for (group, epoch) in state.fences.iter().filter(|(g, _)| shard_of(**g) == shard) {
            recs.push(
                Rec::Fence {
                    group: *group,
                    epoch: *epoch,
                }
                .encode(),
            );
        }
        for (group, set) in state
            .caught_up
            .iter()
            .filter(|(g, _)| shard_of(**g) == shard)
        {
            recs.push(
                Rec::Caught {
                    group: *group,
                    members: set.iter().map(|n| n.0.clone()).collect(),
                }
                .encode(),
            );
        }
        let keys: BTreeSet<&String> = state.logs.keys().chain(state.truncated.keys()).collect();
        for key in keys {
            if shard_of(crate::placement::group_of_key(key)) != shard {
                continue;
            }
            // The low-water first: it deletes nothing yet (no entry precedes it
            // in this log) and sets the row; the entries then land exactly as
            // the table held them, stale leftovers below the low-water included.
            if let Some(lw) = state.truncated.get(key) {
                recs.push(
                    Rec::Truncate {
                        key,
                        up_to: *lw,
                        low_water: *lw,
                    }
                    .encode(),
                );
            }
            for (offset, ((epoch, seq), record)) in state.logs.get(key).into_iter().flatten() {
                recs.push(
                    Rec::Append {
                        key,
                        offset: *offset,
                        epoch: *epoch,
                        seq: *seq,
                        record,
                    }
                    .encode(),
                );
                if recs.len() >= IMPORT_CHUNK {
                    flush(&mut log, &mut recs)?;
                }
            }
        }
        flush(&mut log, &mut recs)?;
    }
    let target = root.join(LOG_DIR);
    std::fs::rename(&staging, &target)
        .map_err(|e| io("renaming the staged store into place", e))?;
    #[cfg(unix)]
    std::fs::File::open(root)
        .and_then(|d| d.sync_all())
        .map_err(|e| io("flushing the data dir", e))?;
    Ok(())
}

fn flush(log: &mut SegmentLog, recs: &mut Vec<(u8, Vec<u8>)>) -> Result<(), ReplError> {
    let batch: Vec<(u8, &[u8])> = recs.iter().map(|(k, p)| (*k, p.as_slice())).collect();
    log.append(&batch).map_err(|e| lg(&e))?;
    recs.clear();
    Ok(())
}

/// Whether an interrupted import left its staging directory behind.
#[must_use]
pub fn staging_exists(root: &Path) -> bool {
    root.join(STAGING_DIR).exists()
}

#[cfg(test)]
mod tests {
    use super::super::{Epoch, Offset, ReplOp, ReplicaState, StoreBackend};
    use super::*;

    type Snapshot = (Fences, ReplicaLogs, BTreeMap<String, Offset>, CaughtUp);

    fn snap(r: &ReplicaState) -> Snapshot {
        (
            r.fences.clone(),
            r.logs.clone(),
            r.truncated.clone(),
            r.caught_up.clone(),
        )
    }

    fn ap(key: &str, offset: u64, seq: u64, rec: &[u8]) -> ReplOp {
        ReplOp::Append {
            key: key.to_string(),
            offset,
            seq,
            record: rec.to_vec(),
        }
    }

    fn tr(key: &str, up_to: u64) -> ReplOp {
        ReplOp::Truncate {
            key: key.into(),
            up_to,
        }
    }

    /// Every kind of change the store records: appends across many keys (so a
    /// sharded store fans them out), a superseded attempt, truncates coalesced
    /// in one batch, a late lower ack, a Remove after a truncate, a fence that
    /// moves, caught-up stamps — through all three apply paths.
    fn workload(state: &std::sync::Mutex<ReplicaState>) {
        let keys: Vec<String> = (0..24).map(|i| format!("q/k{i}")).collect();
        let shards = state.lock().unwrap().shard_count();
        let run = |batch: &[(Epoch, ReplOp)]| {
            // Route each op to its shard's writer, as the durable plane does.
            let mut by_shard: BTreeMap<usize, Vec<(Epoch, ReplOp)>> = BTreeMap::new();
            for (e, op) in batch {
                let g = crate::placement::group_of_key(super::super::op_key(op));
                by_shard
                    .entry(super::super::shard_of_group(g, shards))
                    .or_default()
                    .push((*e, op.clone()));
            }
            for (shard, ops) in by_shard {
                let out = ReplicaState::apply_batch_sharded(state, shard, &ops);
                assert!(out.iter().all(|ok| *ok), "every op accepted: {out:?}");
            }
        };
        let mut b = Vec::new();
        for k in &keys {
            for o in 1..=6 {
                b.push((3, ap(k, o, o, format!("{k}/{o}").as_bytes())));
            }
        }
        run(&b);
        run(&keys.iter().map(|k| (3, tr(k, 2))).collect::<Vec<_>>());
        run(&[
            (3, tr("q/k0", 3)),
            (3, tr("q/k0", 1)), // late, lower: the low-water stays at 3
            (3, ap("q/k0", 7, 7, b"k0/7")),
            (3, tr("q/k0", 4)),
            (5, ap("q/k1", 7, 1, b"k1/7@5")), // the fence moves to 5
            (3, tr("q/k2", 6)),
            (3, ReplOp::Remove { key: "q/k2".into() }),
        ]);
        // A stale leftover: a late re-append BELOW the low-water (4), then a
        // late lower ack. The ack deletes through its own up_to (2), not the
        // low-water, so offset 3 survives — in the table, and so in the log.
        run(&[(5, ap("q/k0", 3, 9, b"k0/3 late")), (5, tr("q/k0", 2))]);
        {
            // The single-op and whole-batch paths (ADR 0018 / 0027) too.
            let mut r = state.lock().unwrap();
            assert!(r.apply(5, &ap("q/k3", 7, 1, b"k3/7")));
            // A superseded attempt: accepted, and nothing overwritten.
            // Same epoch, lower seq: a late duplicate of an older attempt.
            assert!(r.apply(5, &ap("q/k3", 7, 0, b"stale")));
            assert!(r
                .apply_batch(&[(5, tr("q/k4", 5)), (5, ap("q/k4", 7, 1, b"k4/7"))])
                .iter()
                .all(|x| *x));
            r.mark_groups_current(&[
                (
                    crate::placement::group_of_key("q/k0"),
                    vec![crate::NodeId("a".into()), crate::NodeId("b".into())],
                ),
                (
                    crate::placement::group_of_key("q/k9"),
                    vec![crate::NodeId("c".into())],
                ),
            ]);
        }
    }

    fn run_on(dir: &Path, shards: usize, backend: StoreBackend) -> (Snapshot, Snapshot) {
        let live = {
            let state = std::sync::Mutex::new(
                ReplicaState::open_store_with(dir, shards, backend, 4096).unwrap(),
            );
            workload(&state);
            let r = state.lock().unwrap();
            snap(&r)
        };
        let reopened = ReplicaState::open_store_with(dir, shards, backend, 4096).unwrap();
        (live, snap(&reopened))
    }

    /// The log is a second engine for ONE meaning: the same changes, applied
    /// through every path, give the same state as redb — live, and after a
    /// reopen, which is where the log's replay has to agree with the tables.
    #[test]
    fn the_log_store_equals_the_redb_store_live_and_after_reopen() {
        for shards in [1, 4] {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (redb_live, redb_reopened) = run_on(a.path(), shards, StoreBackend::Redb);
            let (log_live, log_reopened) = run_on(b.path(), shards, StoreBackend::Log);
            assert_eq!(
                redb_live, redb_reopened,
                "redb round-trips ({shards} shards)"
            );
            assert_eq!(
                log_live, redb_live,
                "same changes, same state ({shards} shards)"
            );
            assert_eq!(
                log_reopened, redb_reopened,
                "same state after a reopen ({shards} shards)"
            );
            assert_eq!(log_reopened.2.get("q/k0"), Some(&4));
            assert!(
                !log_reopened.1.contains_key("q/k2"),
                "Remove survived the replay"
            );
            assert!(b.path().join(LOG_DIR).exists());
        }
    }

    /// A data dir that holds a redb store is imported once: the log opens to
    /// the same state, the redb files are kept under `*.imported`, a second
    /// open reads the log, and redb is then refused rather than read empty.
    #[test]
    fn a_redb_store_is_imported_once_and_retired() {
        for shards in [1, 4] {
            let dir = tempfile::tempdir().unwrap();
            let (_, before) = run_on(dir.path(), shards, StoreBackend::Redb);
            let imported =
                ReplicaState::open_store_with(dir.path(), shards, StoreBackend::Log, 4096).unwrap();
            assert_eq!(
                snap(&imported),
                before,
                "the import carries every row ({shards} shards)"
            );
            assert_eq!(imported.shard_count(), shards);
            drop(imported);
            assert!(
                ReplicaState::redb_is_fresh(dir.path()),
                "the redb files are retired"
            );
            let kept = if shards == 1 {
                super::super::R_LEGACY_FILE.to_string()
            } else {
                super::super::shard_file_name(0)
            };
            assert!(dir.path().join(format!("{kept}.imported")).exists());
            let again =
                ReplicaState::open_store_with(dir.path(), shards, StoreBackend::Log, 4096).unwrap();
            assert_eq!(snap(&again), before);
            drop(again);
            let err = ReplicaState::open_store_with(dir.path(), shards, StoreBackend::Redb, 4096)
                .unwrap_err();
            assert!(err.to_string().contains("segment-log"), "{err}");
        }
    }

    /// An import a crash interrupted leaves its staging dir and the untouched
    /// redb store; the next open redoes it from the redb files.
    #[test]
    fn an_interrupted_import_is_redone() {
        let dir = tempfile::tempdir().unwrap();
        let (_, before) = run_on(dir.path(), 1, StoreBackend::Redb);
        let staging = dir.path().join(STAGING_DIR).join("shard-0");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(
            staging.join("seg-00000000000000000001.log"),
            b"half an import",
        )
        .unwrap();
        let r = ReplicaState::open_store_with(dir.path(), 1, StoreBackend::Log, 4096).unwrap();
        assert_eq!(snap(&r), before);
        assert!(!staging_exists(dir.path()));
    }

    /// A crash that tears the last batch loses exactly that batch — never
    /// acked — and the store reopens to the state before it.
    #[test]
    fn a_torn_last_batch_reopens_to_the_state_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let before = {
            let state = std::sync::Mutex::new(
                ReplicaState::open_store_with(dir.path(), 1, StoreBackend::Log, 1 << 20).unwrap(),
            );
            assert!(ReplicaState::apply_batch_sharded(&state, 0, &[(2, ap("q/t", 1, 1, b"a"))])[0]);
            let before = snap(&state.lock().unwrap());
            assert!(
                ReplicaState::apply_batch_sharded(&state, 0, &[(2, ap("q/t", 2, 2, b"torn"))])[0]
            );
            before
        };
        let seg = std::fs::read_dir(shard_dir(dir.path(), 0))
            .unwrap()
            .map(|e| e.unwrap().path())
            .next()
            .unwrap();
        let bytes = std::fs::read(&seg).unwrap();
        let used = bytes.iter().rposition(|b| *b != 0).unwrap() + 1;
        let mut torn = bytes.clone();
        torn[used - 1] ^= 0xff; // the last batch's final byte never reached the disk
        std::fs::write(&seg, &torn).unwrap();
        let r = ReplicaState::open_store_with(dir.path(), 1, StoreBackend::Log, 1 << 20).unwrap();
        assert_eq!(snap(&r), before);
    }

    #[test]
    fn a_foreign_format_or_shard_count_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        drop(ReplicaState::open_store_with(dir.path(), 1, StoreBackend::Log, 4096).unwrap());
        let (mut log, _) =
            SegmentLog::open(shard_dir(dir.path(), 0), 4096, |_, _, _| Ok(())).unwrap();
        let (k, p) = Rec::Format {
            version: 99,
            shards: 1,
        }
        .encode();
        log.append(&[(k, &p)]).unwrap();
        drop(log);
        let err =
            ReplicaState::open_store_with(dir.path(), 1, StoreBackend::Log, 4096).unwrap_err();
        assert!(err.to_string().contains("format 99"), "{err}");
    }

    #[test]
    fn records_round_trip() {
        let recs = [
            Rec::Format {
                version: 1,
                shards: 4,
            },
            Rec::Append {
                key: "q/ünïcode",
                offset: 9,
                epoch: 3,
                seq: 2,
                record: b"",
            },
            Rec::Append {
                key: "q/b",
                offset: u64::MAX,
                epoch: 1,
                seq: 0,
                record: b"payload",
            },
            Rec::Truncate {
                key: "q/b",
                up_to: 4,
                low_water: 7,
            },
            Rec::Remove { key: "q/b" },
            Rec::Fence {
                group: 255,
                epoch: 17,
            },
            Rec::Caught {
                group: 1,
                members: vec!["n1".into(), "n-2".into()],
            },
            Rec::Caught {
                group: 2,
                members: vec![],
            },
        ];
        for rec in recs {
            let (kind, payload) = rec.encode();
            assert_eq!(decode(kind, &payload).unwrap(), rec);
        }
        assert!(
            decode(K_APPEND, &[0u8; 5]).is_err(),
            "a short record is an error, not a guess"
        );
        assert!(decode(42, b"").is_err());
    }
}
