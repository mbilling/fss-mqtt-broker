//! Changing the replication factor of a running cluster
//! ([ADR 0080](../../../docs/adr/0080-replication-factor.md) §4).
//!
//! An operator edits `durable.replicas` and reloads. The edit is a **proposal**
//! ([`ReplicaChangeControl::propose`]): the lease leader turns it into
//! `BeginReplicaChange { from, to }` when the cluster can take it, and refuses it
//! otherwise ([`decide_proposal`]). Only the leader can write to the lease group,
//! so a reload on every node (a config-map roll) is what starts a change; a
//! reload on one follower is held until that node leads or the change lands.
//!
//! Opening the change re-mints every lease above `since`. From then on each
//! owner builds its logs over the larger of the two replica sets and counts acks
//! by the joint rule (a majority of the larger set and of its prefix, the
//! smaller set), so every entry written at an epoch above `since` is held by a
//! majority of the new set. Only entries at or below `since` can fall short.
//!
//! The leader checks exactly those ([`ChangeVerifier::round`]): for every key of
//! every group it reads a joint quorum of the larger set, merges the reads as a
//! recovery would, and counts, for each merged entry at or below `since`, the
//! members of the new set that hold it. A key that falls short is handed to its
//! group's owner to recover and re-commit, which re-writes it at the owner's
//! current epoch under the joint rule. When a round finds nothing short, the
//! leader commits `CommitReplicaChange { to }` and quorums use the new set only.
//!
//! After a shrink commits, each node deletes its copies of the groups it left
//! ([`collect_round`]), but only once every member of the group's new set is
//! verified to hold everything the copy holds, the check the decommission
//! drain makes before a node leaves (ADR 0043 P3).

use crate::cluster_log::{
    merge_replica_logs_tagged, Quorum, ReplicaRead, ReplicaState, ReplicaTransport,
};
use crate::durable_plane::CatchUpSource;
use crate::lease_raft::{GroupId, ReplicaChange, ReplicationRecord, REPLICAS_MAX, REPLICAS_MIN};
use crate::placement::{group_of_key, Placement, NUM_GROUPS};
use crate::repl_net::PeerReplicaTransport;
use crate::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// The operator-facing side of a live change: the pending proposal, and the
/// progress `/statusz` reports. Shared by the reload path, the durable driver
/// and the status endpoint.
#[derive(Debug, Default)]
pub struct ReplicaChangeControl {
    /// The factor a reload asked for; 0 when nothing is pending.
    proposed: AtomicU8,
    /// The committed record, mirrored by the driver every tick.
    record: Mutex<ReplicationRecord>,
    /// Why the last proposal was refused or is held, if it was.
    note: Mutex<Option<String>>,
    /// Groups the leader's last round verified, of [`NUM_GROUPS`].
    verified_groups: AtomicUsize,
    /// Keys the leader's last round found short.
    pending_keys: AtomicUsize,
    /// Verification rounds the leader has run for the open change.
    rounds: AtomicU64,
    /// Keys this node still holds for groups a committed shrink took it out of.
    collect_pending: AtomicUsize,
}

impl ReplicaChangeControl {
    /// A control with nothing pending.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ask for the cluster to move to `replicas` copies (a reload of
    /// `durable.replicas`). Replaces any earlier proposal.
    pub fn propose(&self, replicas: u8) {
        self.proposed.store(replicas, Ordering::Release);
        *lock(&self.note) = None;
    }

    /// The pending proposal, if any.
    #[must_use]
    pub fn proposed(&self) -> Option<u8> {
        match self.proposed.load(Ordering::Acquire) {
            0 => None,
            r => Some(r),
        }
    }

    /// Drop the pending proposal (it landed, or was refused).
    pub(crate) fn clear(&self, note: Option<String>) {
        self.proposed.store(0, Ordering::Release);
        *lock(&self.note) = note;
    }

    /// Record why the pending proposal waits; returns whether the reason is new.
    pub(crate) fn hold(&self, why: &str) -> bool {
        let mut note = lock(&self.note);
        if note.as_deref() == Some(why) {
            return false;
        }
        *note = Some(why.to_string());
        true
    }

    /// The committed record as of the driver's last tick.
    #[must_use]
    pub fn record(&self) -> ReplicationRecord {
        *lock(&self.record)
    }

    pub(crate) fn set_record(&self, record: ReplicationRecord) {
        let mut held = lock(&self.record);
        if held.change.is_some() && record.change.is_none() {
            self.verified_groups.store(0, Ordering::Release);
            self.pending_keys.store(0, Ordering::Release);
            self.rounds.store(0, Ordering::Release);
        }
        *held = record;
    }

    pub(crate) fn record_round(&self, outcome: RoundOutcome) {
        self.verified_groups
            .store(outcome.verified_groups, Ordering::Release);
        self.pending_keys.store(outcome.pending, Ordering::Release);
        self.rounds.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn set_collect_pending(&self, keys: usize) {
        self.collect_pending.store(keys, Ordering::Release);
    }

    /// The `/statusz` object: the committed factor, an open change and its
    /// progress, a pending proposal and why it waits or was refused, and the
    /// copies left to collect.
    #[must_use]
    pub fn statusz_fragment(&self) -> String {
        let record = self.record();
        let mut s = String::new();
        let _ = write!(s, "{{\"replicas\":{}", record.effective());
        if let Some(c) = record.change {
            let _ = write!(
                s,
                ",\"change\":{{\"from\":{},\"to\":{},\"rounds\":{},\"verified_groups\":{},\
                 \"groups\":{NUM_GROUPS},\"pending_keys\":{}}}",
                c.from,
                c.to,
                self.rounds.load(Ordering::Acquire),
                self.verified_groups.load(Ordering::Acquire),
                self.pending_keys.load(Ordering::Acquire),
            );
        }
        if let Some(r) = self.proposed() {
            let _ = write!(s, ",\"proposed\":{r}");
        }
        if let Some(note) = lock(&self.note).as_deref() {
            let _ = write!(s, ",\"note\":\"{}\"", json_escape(note));
        }
        let _ = write!(
            s,
            ",\"collect_pending_keys\":{}}}",
            self.collect_pending.load(Ordering::Acquire)
        );
        s
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// What a pending proposal turns into this tick ([`decide_proposal`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalAction {
    /// Nothing to do: nothing pending, or the factor is already in force or
    /// being moved to. The proposal is dropped.
    Done,
    /// Propose `BeginReplicaChange { from, to }`.
    Begin {
        /// The factor in force.
        from: u8,
        /// The proposed factor.
        to: u8,
    },
    /// Refused, with the reason. The proposal is dropped.
    Refuse(String),
    /// Kept for a later tick, with the reason.
    Hold(&'static str),
}

/// Decide what to do with `proposed` (ADR 0080 §4): a reload is a proposal, not
/// an order. It is refused while another change runs or when fewer than
/// `proposed` nodes are eligible to hold a copy, held while a member cannot
/// decode the change or this node does not lead the lease group, and otherwise
/// opens the change.
#[must_use]
pub fn decide_proposal(
    proposed: Option<u8>,
    record: ReplicationRecord,
    eligible: usize,
    capable: bool,
    leader: bool,
) -> ProposalAction {
    let Some(to) = proposed else {
        return ProposalAction::Done;
    };
    if let Some(c) = record.change {
        return if c.to == to {
            ProposalAction::Done
        } else {
            ProposalAction::Refuse(format!(
                "a change from {} to {} is still running; propose again once it commits",
                c.from, c.to
            ))
        };
    }
    let from = record.effective();
    if to == from {
        return ProposalAction::Done;
    }
    if !(REPLICAS_MIN..=REPLICAS_MAX).contains(&to) {
        return ProposalAction::Refuse(format!(
            "{to} copies is outside {REPLICAS_MIN}..={REPLICAS_MAX}"
        ));
    }
    if eligible < usize::from(to) {
        return ProposalAction::Refuse(format!(
            "{eligible} eligible nodes cannot hold {to} copies of every group"
        ));
    }
    if !capable {
        return ProposalAction::Hold("a member does not speak the replication-factor protocol yet");
    }
    if !leader {
        return ProposalAction::Hold(
            "only the lease leader proposes a change; it does so when its own configuration asks for it",
        );
    }
    ProposalAction::Begin { from, to }
}

/// One verification round's result.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoundOutcome {
    /// Groups with nothing short.
    pub verified_groups: usize,
    /// Keys found short, or groups and keys that could not be read enough to
    /// judge. Zero means the change can commit.
    pub pending: usize,
}

/// The lease leader's check that an open change can commit (module docs).
pub struct ChangeVerifier {
    /// This node.
    pub node: NodeId,
    /// The live placement, which must have adopted the change.
    pub placement: Arc<RwLock<Placement>>,
    /// Reads and catch-up requests to peers.
    pub transport: Arc<PeerReplicaTransport>,
    /// This node's own replica copy.
    pub replicas: Arc<Mutex<ReplicaState>>,
    /// Serves the re-commit for groups this node owns.
    pub source: Arc<dyn CatchUpSource>,
}

impl std::fmt::Debug for ChangeVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeVerifier")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

/// Whether `read` holds `entry`: the same record bytes at its offset, or the
/// offset already truncated there (acked away).
fn holds(read: &ReplicaRead, offset: u64, record: &[u8]) -> bool {
    offset <= read.watermark
        || read
            .entries
            .iter()
            .any(|e| e.offset == offset && e.record == record)
}

impl ChangeVerifier {
    /// This node's read of `key`, built as a recovery read is.
    fn local_read(&self, key: &str) -> ReplicaRead {
        let r = self
            .replicas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ReplicaRead {
            watermark: r.watermark(key),
            complete: r.complete(key),
            entries: r.epoch_entries(key),
        }
    }

    /// Every key held by this node or by a peer that answered, and who answered.
    async fn discover(
        &self,
        members: &[NodeId],
    ) -> (BTreeMap<GroupId, BTreeSet<String>>, BTreeSet<NodeId>) {
        let mut answered = BTreeSet::from([self.node.clone()]);
        let mut keys: Vec<String> = self
            .replicas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys();
        let mut asks = tokio::task::JoinSet::new();
        for peer in members.iter().filter(|m| **m != self.node) {
            let transport = self.transport.clone();
            let peer = peer.clone();
            asks.spawn(async move {
                let keys = transport.keys_of(&peer).await;
                (peer, keys)
            });
        }
        while let Some(res) = asks.join_next().await {
            if let Ok((peer, Some(theirs))) = res {
                answered.insert(peer);
                keys.extend(theirs);
            }
        }
        let mut by_group: BTreeMap<GroupId, BTreeSet<String>> = BTreeMap::new();
        for key in keys {
            by_group.entry(group_of_key(&key)).or_default().insert(key);
        }
        (by_group, answered)
    }

    /// Check every group once; ask owners to re-commit what falls short.
    pub async fn round(&self, change: ReplicaChange) -> RoundOutcome {
        let width = usize::from(change.joint_width());
        let (adopted, members) = {
            let p = self
                .placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                p.desired_replicas() == width
                    && p.joint_prefix()
                        .is_none_or(|prefix| prefix == usize::from(change.prefix_width())),
                p.members(),
            )
        };
        if !adopted {
            // This node's placement has not taken the change yet: its sets
            // would be the old ones. Next tick.
            return RoundOutcome {
                verified_groups: 0,
                pending: usize::try_from(NUM_GROUPS).unwrap_or(usize::MAX),
            };
        }
        let (keys, answered) = self.discover(&members).await;
        let mut outcome = RoundOutcome::default();
        for group in 0..NUM_GROUPS {
            let (set, owner) = {
                let p = self
                    .placement
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (p.group_replica_set(group), p.group_owner(group))
            };
            let prefix = usize::from(change.prefix_width()).min(set.len());
            let quorum = Quorum::joint(set.len(), prefix);
            // Growing, the new set is the whole set; shrinking, its prefix.
            let new_len = if change.to < change.from {
                prefix
            } else {
                set.len()
            };
            let need = new_len / 2 + 1;
            // Discovery must have heard a joint quorum: any key with an entry
            // committed under the old set is then known.
            let heard = set
                .iter()
                .enumerate()
                .filter(|(_, n)| answered.contains(*n))
                .map(|(i, _)| i);
            if !quorum.met_by(heard) {
                outcome.pending += 1;
                continue;
            }
            let mut short = 0;
            for key in keys.get(&group).into_iter().flatten() {
                if !self
                    .key_is_safe(key, &set, &quorum, new_len, need, change.since)
                    .await
                {
                    short += 1;
                    if owner == self.node {
                        self.source.catch_up_key(key).await;
                    } else {
                        self.transport.request_catch_up(&owner, key);
                    }
                }
            }
            if short == 0 {
                outcome.verified_groups += 1;
            }
            outcome.pending += short;
        }
        outcome
    }

    /// Whether every entry of `key` at or below `since` that a recovery over a
    /// joint quorum of `set` would return is held by `need` of the first
    /// `new_len` members. An unreadable key is not safe.
    async fn key_is_safe(
        &self,
        key: &str,
        set: &[NodeId],
        quorum: &Quorum,
        new_len: usize,
        need: usize,
        since: u64,
    ) -> bool {
        let mut reads: Vec<Option<ReplicaRead>> = vec![None; set.len()];
        let mut inflight = tokio::task::JoinSet::new();
        for (i, member) in set.iter().enumerate() {
            if *member == self.node {
                reads[i] = Some(self.local_read(key));
                continue;
            }
            let transport = self.transport.clone();
            let member = member.clone();
            let key = key.to_string();
            inflight.spawn(async move { (i, transport.read_replica(&member, &key).await) });
        }
        while let Some(res) = inflight.join_next().await {
            if let Ok((i, read)) = res {
                reads[i] = read;
            }
        }
        if !quorum.met_by((0..set.len()).filter(|i| reads[*i].is_some())) {
            return false;
        }
        let answered: Vec<ReplicaRead> = reads.iter().flatten().cloned().collect();
        merge_replica_logs_tagged(&answered)
            .iter()
            .filter(|e| e.epoch <= since)
            .all(|e| {
                reads[..new_len]
                    .iter()
                    .flatten()
                    .filter(|r| holds(r, e.offset, &e.record))
                    .count()
                    >= need
            })
    }
}

/// One collection pass over this node's copies of `groups` (the groups a
/// committed shrink took it out of): a key is deleted once every member of its
/// group's current set holds everything this copy holds; a member that falls
/// short is handed the key by the group's owner. A group this node is back in
/// the set of is dropped from `groups` untouched. Returns the keys left.
pub async fn collect_round(
    node: &NodeId,
    placement: &Arc<RwLock<Placement>>,
    transport: &Arc<PeerReplicaTransport>,
    replicas: &Arc<Mutex<ReplicaState>>,
    groups: &mut BTreeSet<GroupId>,
) -> usize {
    let lock_replicas = || {
        replicas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    };
    let mut left = 0;
    let keys = lock_replicas().keys();
    for key in keys {
        let group = group_of_key(&key);
        if !groups.contains(&group) {
            continue;
        }
        let (set, owner) = {
            let p = placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (p.group_replica_set(group), p.group_owner(group))
        };
        if set.contains(node) {
            // Back in the set: the copy is live again, not ours to collect.
            groups.remove(&group);
            continue;
        }
        let (watermark, entries) = {
            let r = lock_replicas();
            (r.watermark(&key), r.epoch_entries(&key))
        };
        let mut sufficient = true;
        for member in &set {
            let held = match transport.read_replica(member, &key).await {
                Some(theirs) => {
                    theirs.watermark >= watermark
                        && entries.iter().all(|e| holds(&theirs, e.offset, &e.record))
                }
                None => false,
            };
            if !held {
                sufficient = false;
                transport.request_catch_up_to(&owner, &key, member);
            }
        }
        if !sufficient {
            left += 1;
            continue;
        }
        // Delete only the copy that was verified: a write that reached this
        // node since (it re-entered the set) leaves it alone.
        let forgotten = {
            let mut r = lock_replicas();
            r.watermark(&key) == watermark && r.epoch_entries(&key) == entries && r.forget(&key)
        };
        if !forgotten {
            left += 1;
        }
    }
    if left == 0 {
        groups.clear();
    }
    left
}

#[cfg(test)]
mod tests {
    use super::{
        collect_round, decide_proposal, ChangeVerifier, ProposalAction, ReplicaChangeControl,
    };
    use crate::cluster_log::{ReplOp, ReplicaState};
    use crate::cluster_store::{GroupRoutedLog, LeaseSource};
    use crate::lease::Epoch;
    use crate::lease_raft::{GroupId, ReplicaChange, ReplicationRecord};
    use crate::peer::PeerMessage;
    use crate::placement::{group_of_key, Placement};
    use crate::repl_net::PeerReplicaTransport;
    use crate::swim::MemberState;
    use crate::NodeId;
    use async_trait::async_trait;
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex, RwLock};
    use tokio::sync::mpsc;

    fn nid(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    /// The lease as the lease store reports it: an epoch with its record.
    struct Lease(Epoch, ReplicationRecord);

    #[async_trait]
    impl LeaseSource for Lease {
        async fn epoch_for(&self, _group: GroupId) -> Result<Epoch, mqtt_storage::repl::ReplError> {
            Ok(self.0)
        }

        async fn lease_for(
            &self,
            _group: GroupId,
        ) -> Result<(Epoch, Option<ReplicationRecord>), mqtt_storage::repl::ReplError> {
            Ok((self.0, Some(self.1)))
        }
    }

    /// A peer in-process: applies replication, answers recovery reads and key
    /// discovery from its state, as a real plane does on its link.
    fn spawn_peer(
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
                    PeerMessage::ReplicaKeys { req_id } => {
                        let keys = state.lock().unwrap().keys();
                        transport.complete_keys(req_id, keys);
                    }
                    _ => {}
                }
            }
        });
    }

    fn append(state: &Arc<Mutex<ReplicaState>>, epoch: Epoch, key: &str, record: &[u8]) {
        assert!(state.lock().unwrap().apply(
            epoch,
            &ReplOp::Append {
                key: key.to_string(),
                offset: 1,
                seq: 1,
                record: record.to_vec(),
            }
        ));
    }

    /// Three nodes, each placement at `width` with `prefix`, and a queue key
    /// whose group `local` owns.
    fn ring(local: &NodeId, width: usize, prefix: Option<usize>) -> Placement {
        let mut p = Placement::new(local.clone(), width);
        for peer in ["own", "kept", "gone"] {
            if peer != local.0 {
                p.observe(
                    &nid(peer),
                    MemberState::Alive,
                    &format!("{peer}:7000"),
                    None,
                );
            }
        }
        p.set_replication(width, prefix);
        p
    }

    /// ADR 0080 §4, the shrink hazard: an entry acked at two of three before
    /// the change can sit on the owner and the member being dropped only. The
    /// leader's round finds it short of the new set, the owner's re-commit under
    /// the joint rule puts it on the kept member at an epoch above `since`, and
    /// the next round passes every group.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_round_finds_an_entry_short_of_the_new_set_and_the_recommit_fixes_it() {
        let own = nid("own");
        let p = ring(&own, 3, Some(2));
        let key = (0..100_000)
            .map(|i| format!("q/rc-{i}"))
            .find(|k| {
                let set = p.group_replica_set(group_of_key(k));
                set[0] == own && set.len() == 3
            })
            .expect("a key whose group `own` leads");
        let group = group_of_key(&key);
        let set = p.group_replica_set(group);
        let (kept, gone) = (set[1].clone(), set[2].clone());
        let placement = Arc::new(RwLock::new(p));

        let change = ReplicaChange {
            from: 3,
            to: 2,
            since: 1,
        };
        // Acked at epoch 1 on the owner and the member being dropped only.
        let own_state = Arc::new(Mutex::new(ReplicaState::new()));
        append(&own_state, 1, &key, b"m1");
        own_state
            .lock()
            .unwrap()
            .mark_groups_current(&[(group, set.clone())]);
        let transport = Arc::new(PeerReplicaTransport::new());
        let kept_state = Arc::new(Mutex::new(ReplicaState::new()));
        let gone_state = Arc::new(Mutex::new(ReplicaState::new()));
        append(&gone_state, 1, &key, b"m1");
        for (node, state) in [(&kept, &kept_state), (&gone, &gone_state)] {
            let (tx, rx) = mpsc::unbounded_channel();
            transport.register(node.clone(), tx);
            spawn_peer(transport.clone(), state.clone(), rx);
        }
        let source = Arc::new(GroupRoutedLog::new(
            own.clone(),
            placement.clone(),
            transport.clone(),
            Lease(
                2,
                ReplicationRecord {
                    replicas: Some(3),
                    change: Some(change),
                },
            ),
            own_state.clone(),
        ));
        let verifier = ChangeVerifier {
            node: own,
            placement,
            transport,
            replicas: own_state,
            source,
        };

        let first = verifier.round(change).await;
        assert_eq!(
            first.pending, 1,
            "the entry is on one of the two kept members"
        );
        assert_eq!(first.verified_groups, 255);
        assert_eq!(
            kept_state.lock().unwrap().epoch_entries(&key)[0].epoch,
            2,
            "the owner re-committed it to the kept member at its current epoch"
        );
        let second = verifier.round(change).await;
        assert_eq!(second.pending, 0);
        assert_eq!(second.verified_groups, 256);
    }

    /// Collection deletes a dropped copy only once every member of the new set
    /// holds what it holds; a member short of it keeps the copy in place.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dropped_copy_is_collected_only_once_the_new_set_holds_it() {
        let gone = nid("gone");
        let p = ring(&gone, 2, None);
        let key = (0..100_000)
            .map(|i| format!("q/cl-{i}"))
            .find(|k| !p.group_replica_set(group_of_key(k)).contains(&gone))
            .expect("a key whose two-member set excludes `gone`");
        let group = group_of_key(&key);
        let set = p.group_replica_set(group);
        let placement = Arc::new(RwLock::new(p));

        let gone_state = Arc::new(Mutex::new(ReplicaState::new()));
        append(&gone_state, 3, &key, b"m1");
        let transport = Arc::new(PeerReplicaTransport::new());
        let states: Vec<_> = set
            .iter()
            .map(|node| {
                let state = Arc::new(Mutex::new(ReplicaState::new()));
                let (tx, rx) = mpsc::unbounded_channel();
                transport.register(node.clone(), tx);
                spawn_peer(transport.clone(), state.clone(), rx);
                state
            })
            .collect();
        append(&states[0], 3, &key, b"m1");

        let mut groups = BTreeSet::from([group]);
        let left = collect_round(&gone, &placement, &transport, &gone_state, &mut groups).await;
        assert_eq!(left, 1, "one member of the new set lacks the entry");
        assert_eq!(gone_state.lock().unwrap().keys(), vec![key.clone()]);

        append(&states[1], 3, &key, b"m1");
        let left = collect_round(&gone, &placement, &transport, &gone_state, &mut groups).await;
        assert_eq!(left, 0);
        assert!(groups.is_empty());
        assert!(gone_state.lock().unwrap().keys().is_empty(), "collected");
        assert_eq!(
            gone_state.lock().unwrap().fence_for_key(&key),
            3,
            "deleting leaves the fence where it was"
        );
    }

    fn at(replicas: u8) -> ReplicationRecord {
        ReplicationRecord {
            replicas: Some(replicas),
            change: None,
        }
    }

    /// ADR 0080 §4: a reload is a proposal. It opens a change only on the
    /// leader, with every member capable and enough eligible nodes; a change
    /// already running refuses a different target.
    #[test]
    fn a_proposal_opens_a_change_only_when_the_cluster_can_take_it() {
        assert_eq!(
            decide_proposal(None, at(2), 3, true, true),
            ProposalAction::Done
        );
        assert_eq!(
            decide_proposal(Some(2), at(2), 3, true, true),
            ProposalAction::Done
        );
        assert_eq!(
            decide_proposal(Some(3), at(2), 3, true, true),
            ProposalAction::Begin { from: 2, to: 3 }
        );
        // An unset record is the legacy 3.
        assert_eq!(
            decide_proposal(Some(2), ReplicationRecord::default(), 3, true, true),
            ProposalAction::Begin { from: 3, to: 2 }
        );
        assert!(matches!(
            decide_proposal(Some(4), at(2), 3, true, true),
            ProposalAction::Refuse(_)
        ));
        assert!(matches!(
            decide_proposal(Some(3), at(2), 3, false, true),
            ProposalAction::Hold(_)
        ));
        assert!(matches!(
            decide_proposal(Some(3), at(2), 3, true, false),
            ProposalAction::Hold(_)
        ));
        let running = ReplicationRecord {
            replicas: Some(2),
            change: Some(ReplicaChange {
                from: 2,
                to: 3,
                since: 9,
            }),
        };
        assert_eq!(
            decide_proposal(Some(3), running, 3, true, true),
            ProposalAction::Done
        );
        assert!(matches!(
            decide_proposal(Some(2), running, 3, true, true),
            ProposalAction::Refuse(_)
        ));
    }

    /// `/statusz` shows the factor, the open change's progress, a pending
    /// proposal with its note, and the copies left to collect.
    #[test]
    fn the_status_fragment_reports_progress_and_notes() {
        let c = ReplicaChangeControl::new();
        c.set_record(at(2));
        assert_eq!(
            c.statusz_fragment(),
            "{\"replicas\":2,\"collect_pending_keys\":0}"
        );
        c.propose(3);
        c.hold("only the \"leader\"");
        c.set_record(ReplicationRecord {
            replicas: Some(2),
            change: Some(ReplicaChange {
                from: 2,
                to: 3,
                since: 9,
            }),
        });
        c.record_round(super::RoundOutcome {
            verified_groups: 200,
            pending: 7,
        });
        assert_eq!(
            c.statusz_fragment(),
            "{\"replicas\":2,\"change\":{\"from\":2,\"to\":3,\"rounds\":1,\
             \"verified_groups\":200,\"groups\":256,\"pending_keys\":7},\
             \"proposed\":3,\"note\":\"only the \\\"leader\\\"\",\"collect_pending_keys\":0}"
        );
        c.set_record(at(3));
        c.clear(None);
        assert_eq!(
            c.statusz_fragment(),
            "{\"replicas\":3,\"collect_pending_keys\":0}"
        );
    }
}
