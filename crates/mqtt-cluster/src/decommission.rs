//! The decommission drain ([ADR 0043](../../../docs/adr/0043-elastic-cluster-resize.md)
//! P3): making a node's **voluntary** departure lossless before it leaves.
//!
//! Pulling a node's plug is crash semantics — the survivors recover from their
//! replicas, and anything whose only copy was on the dead node is gone. A
//! decommission must be better than that: it *waits* until every key this node
//! holds is content-complete on the replica set each group will have **after**
//! the departure, and only then hands control to the ordinary graceful-shutdown
//! leave ([ADR 0019](../../../docs/adr/0019-graceful-shutdown.md)), whose SWIM
//! departure triggers voter demotion ([ADR 0021](../../../docs/adr/0021-bounded-lease-voters.md))
//! and the eager migration of the moved groups (0043 P2).
//!
//! The drain cannot push data itself: a non-owner writing entries at old epochs
//! would be fenced by any replica that has seen newer appends, and minting
//! epochs is the owner's privilege. So each round it **verifies** its stored
//! copy of every key against every member of the key's post-departure replica
//! set (plain recovery reads), and for each shortfall asks the key's group
//! **owner** to re-commit the key to that one node
//! ([`ReplicaCatchUpTo`](crate::peer::PeerMessage::ReplicaCatchUpTo), proto 5 —
//! the targeted sibling of P1's catch-up). Rounds repeat until nothing falls
//! short; live writes landing mid-round are the owner's ordinary quorum
//! business and are re-verified next round.
//!
//! **Interruptible by construction**: the drain mutates nothing on this node —
//! a crash (or an operator's second signal) mid-drain simply is a crash, and
//! the survivors recover exactly as for any death. Progress is observable via
//! [`DrainStatus`] (surfaced on the health endpoint).

use crate::cluster_log::{ReplicaState, ReplicaTransport};
use crate::placement::{group_of_key, Placement};
use crate::repl_net::PeerReplicaTransport;
use crate::NodeId;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

/// Delay between drain rounds: long enough for the owners' targeted re-commits
/// (and any concurrent P1 catch-up traffic) to land, short enough that a
/// routine decommission completes in seconds.
const ROUND_DELAY: Duration = Duration::from_secs(1);

/// Observable decommission progress (ADR 0043 P3), surfaced by the health
/// endpoint so an operator can watch the drain instead of guessing.
#[derive(Debug, Default)]
pub struct DrainStatus {
    /// Set once a drain has been requested on this node.
    pub active: AtomicBool,
    /// `(key, successor)` pairs still falling short of content-complete, as of
    /// the last completed round.
    pub pending: AtomicUsize,
    /// Completed verification rounds.
    pub rounds: AtomicU64,
    /// Set when the drain verified everything: the node may now leave.
    pub complete: AtomicBool,
}

/// The decommission drain for one node. Runs beside normal service; see the
/// module docs for the protocol.
pub struct Drain {
    // (Debug below is manual: the catch-up source is a trait object.)
    node: NodeId,
    placement: Arc<RwLock<Placement>>,
    transport: Arc<PeerReplicaTransport>,
    replicas: Arc<Mutex<ReplicaState>>,
    source: Option<Arc<dyn crate::durable_plane::CatchUpSource>>,
    status: Arc<DrainStatus>,
}

impl std::fmt::Debug for Drain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Drain")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl Drain {
    /// Assemble a drain over the durable plane's shared handles. `source` serves
    /// the targeted re-commit locally for groups this node itself owns (the same
    /// seam inbound `ReplicaCatchUpTo` requests use).
    #[must_use]
    pub fn new(
        node: NodeId,
        placement: Arc<RwLock<Placement>>,
        transport: Arc<PeerReplicaTransport>,
        replicas: Arc<Mutex<ReplicaState>>,
        source: Option<Arc<dyn crate::durable_plane::CatchUpSource>>,
    ) -> Self {
        Self {
            node,
            placement,
            transport,
            replicas,
            source,
            status: Arc::new(DrainStatus::default()),
        }
    }

    /// The observable progress handle (share with the health endpoint).
    #[must_use]
    pub fn status(&self) -> Arc<DrainStatus> {
        self.status.clone()
    }

    /// Run the drain to completion: verification rounds until every key this
    /// node holds is content-complete on its group's post-departure replica
    /// set. Returns when it is safe to leave. Never gives up on its own — an
    /// unreachable successor keeps the drain (honestly) pending, and the
    /// operator can always escalate to a plain shutdown (crash semantics).
    pub async fn run(&self) {
        self.status.active.store(true, Ordering::Release);
        loop {
            let pending = self.round().await;
            self.status.pending.store(pending, Ordering::Release);
            self.status.rounds.fetch_add(1, Ordering::Release);
            if pending == 0 {
                self.status.complete.store(true, Ordering::Release);
                tracing::info!(
                    rounds = self.status.rounds.load(Ordering::Acquire),
                    "decommission drain complete: every held key is content-complete \
                     on its post-departure replica set (ADR 0043 P3)"
                );
                return;
            }
            tracing::info!(
                pending,
                "decommission drain: hand-offs still short; retrying"
            );
            tokio::time::sleep(ROUND_DELAY).await;
        }
    }

    /// One verification round. For every key this node stores: read each member
    /// of the key's post-departure replica set and check **content
    /// sufficiency** (below); ask the group owner for a targeted re-commit
    /// wherever it falls short. Returns the number of `(key, successor)` pairs
    /// still short.
    ///
    /// Content sufficiency of a successor's copy against ours: its truncation
    /// watermark has reached ours (so a prefix we saw acked away can never be
    /// resurrected from a merge that no longer includes us), and every entry we
    /// hold is either at-or-below the successor's watermark (acked away there)
    /// or present in its log. Keys this node does NOT store cannot lose
    /// anything by this node leaving and are not this drain's business.
    pub async fn round(&self) -> usize {
        // A single-member "cluster" has nowhere to hand data to: a solo
        // decommission is just a shutdown, and holding it hostage would help
        // nobody. The operator keeps the data dir.
        let solo = {
            let p = self
                .placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            p.member_count() <= 1
        };
        if solo {
            return 0;
        }

        let keys = self.lock_replicas().keys();
        let mut pending = 0;
        for key in keys {
            let group = group_of_key(&key);
            let (successors, owner) = {
                let p = self
                    .placement
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (
                    p.group_replica_set_without(group, &self.node),
                    p.group_owner(group),
                )
            };
            let (our_watermark, our_offsets) = {
                let r = self.lock_replicas();
                (
                    r.watermark(&key),
                    r.epoch_entries(&key)
                        .into_iter()
                        .map(|e| e.offset)
                        .collect::<Vec<_>>(),
                )
            };
            for successor in &successors {
                debug_assert_ne!(*successor, self.node);
                let sufficient = match self.transport.read_replica(successor, &key).await {
                    Some(theirs) => {
                        let their_offsets: std::collections::BTreeSet<u64> =
                            theirs.entries.iter().map(|e| e.offset).collect();
                        theirs.watermark >= our_watermark
                            && our_offsets
                                .iter()
                                .all(|o| *o <= theirs.watermark || their_offsets.contains(o))
                    }
                    // Unreachable (no link / timed out): not verified this round.
                    None => false,
                };
                if sufficient {
                    continue;
                }
                pending += 1;
                if owner == self.node {
                    // We own the group: hand the key off ourselves.
                    if let Some(source) = &self.source {
                        source.catch_up_key_to(&key, successor).await;
                    }
                } else {
                    self.transport.request_catch_up_to(&owner, &key, successor);
                }
            }
        }
        pending
    }

    fn lock_replicas(&self) -> std::sync::MutexGuard<'_, ReplicaState> {
        self.replicas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::Drain;
    use crate::cluster_log::{ReplOp, ReplicaState};
    use crate::cluster_store::{GroupRoutedLog, LeaseSource};
    use crate::lease_raft::GroupId;
    use crate::peer::PeerMessage;
    use crate::placement::{Placement, DEFAULT_REPLICAS, NUM_GROUPS};
    use crate::repl_net::PeerReplicaTransport;
    use crate::swim::MemberState;
    use crate::NodeId;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;

    fn nid(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    #[derive(Debug)]
    struct FixedLease(u64);

    #[async_trait]
    impl LeaseSource for FixedLease {
        async fn epoch_for(&self, _group: GroupId) -> Result<u64, mqtt_storage::repl::ReplError> {
            Ok(self.0)
        }
    }

    /// A successor node: applies replication frames into its state and answers
    /// recovery reads from it — the wire behaviour of a real plane, in-process.
    fn spawn_successor(
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
                    PeerMessage::ReplicaRead2 { req_id, key } => {
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
                        transport.complete_read2(req_id, watermark, complete, entries);
                    }
                    _ => {}
                }
            }
        });
    }

    /// A solo node has nowhere to hand data to: the drain completes at once
    /// (a solo decommission is just a shutdown; the operator keeps the disk).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_solo_drain_completes_immediately() {
        let node = nid("solo");
        let placement = Arc::new(RwLock::new(Placement::new(node.clone(), DEFAULT_REPLICAS)));
        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        replicas.lock().unwrap().apply(
            1,
            &ReplOp::Append {
                key: "q/c".into(),
                offset: 1,
                seq: 1,
                record: b"m".to_vec(),
            },
        );
        let drain = Drain::new(
            node,
            placement,
            Arc::new(PeerReplicaTransport::new()),
            replicas,
            None,
        );
        assert_eq!(drain.round().await, 0);
    }

    /// ADR 0043 P3, the drain protocol end to end at the store seam: a
    /// two-node ring where the leaver owns a truncated key the successor has
    /// nothing of. Round one finds the shortfall and hands the key off through
    /// the owner-side targeted re-commit; a later round verifies the successor
    /// holds every entry AND the truncation watermark, and reports clean.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_drain_hands_off_and_verifies_a_held_key() {
        let leaver = nid("leaver");
        let succ = nid("succ");
        // A key whose group the LEAVER owns in the two-node ring.
        let client = {
            let mut two = Placement::new(leaver.clone(), DEFAULT_REPLICAS);
            two.observe(&succ, MemberState::Alive, "s:7000", None);
            (0..100_000)
                .map(|i| format!("dc-{i}"))
                .find(|c| two.owner(c) == leaver)
                .expect("some client is owned by the leaver")
        };
        let qkey = format!("q/{client}");

        let mut p = Placement::new(leaver.clone(), DEFAULT_REPLICAS);
        p.observe(&succ, MemberState::Alive, "s:7000", None);
        let placement = Arc::new(RwLock::new(p));

        // The leaver's durable copy: entries 1..=3, offset 1 acked away.
        let replicas = Arc::new(Mutex::new(ReplicaState::new()));
        {
            let mut r = replicas.lock().unwrap();
            for off in 1..=3u64 {
                assert!(r.apply(
                    1,
                    &ReplOp::Append {
                        key: qkey.clone(),
                        offset: off,
                        seq: off,
                        record: format!("m{off}").into_bytes(),
                    }
                ));
            }
            assert!(r.apply(
                1,
                &ReplOp::Truncate {
                    key: qkey.clone(),
                    up_to: 1
                }
            ));
            // Its boot sweep stamped it long ago — the recovery anchor.
            let stamps: Vec<_> = (0..NUM_GROUPS)
                .map(|g| (g, placement.read().unwrap().group_replica_set(g)))
                .collect();
            r.mark_groups_current(&stamps);
        }

        // The successor, wired over the shared transport, starts EMPTY.
        let transport = Arc::new(PeerReplicaTransport::new());
        let succ_state = Arc::new(Mutex::new(ReplicaState::new()));
        let (tx, rx) = mpsc::unbounded_channel();
        transport.register(succ.clone(), tx, crate::peer::PROTO_MAX);
        spawn_successor(transport.clone(), succ_state.clone(), rx);

        // The owner-side catch-up source: the real group-routed store.
        let source = Arc::new(GroupRoutedLog::new(
            leaver.clone(),
            placement.clone(),
            transport.clone(),
            FixedLease(1),
            replicas.clone(),
        ));

        let drain = Drain::new(leaver, placement, transport, replicas, Some(source));

        // The first round must find the successor short and ask the hand-off.
        assert!(
            drain.round().await > 0,
            "an empty successor cannot verify; the drain must have work"
        );
        // Rounds converge: the targeted re-commit lands and verification passes.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if drain.round().await == 0 {
                break;
            }
            assert!(Instant::now() < deadline, "the drain never converged");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // The successor now holds the leaver's live entries AND its watermark:
        // nothing the leaver held can be lost or resurrected once it leaves.
        let s = succ_state.lock().unwrap();
        assert_eq!(
            s.entries(&qkey)
                .iter()
                .map(|e| e.offset)
                .collect::<Vec<_>>(),
            vec![2, 3],
            "the successor holds every live entry"
        );
        assert!(
            s.watermark(&qkey) >= 1,
            "the truncation watermark was handed off too (no resurrection)"
        );
    }
}
