//! Assembling a node's durable cluster session store
//! ([ADR 0007](../../../docs/adr/0007-durable-store-integration.md), workstream E
//! step 4f).
//!
//! [`build_durable_node`] ties the workstream-E components into one node's durable
//! stack and a background driver:
//!
//! - the **lease consensus group** — a [`MeshRaftNetwork`] + [`LeaseStore`] +
//!   openraft [`Raft`], bundled with a replication transport + follower state into a
//!   [`DurablePlane`] (the endpoint the broker routes peer frames to);
//! - the **durable store** — a [`ReplicatedSessionStore`] over a [`GroupRoutedLog`]
//!   that routes each session to its group's `ClusterLog`, the epoch read from the
//!   local lease map ([`LocalLeaseSource`]);
//! - the **driver** — a task that, on a tick over the live [`Placement`] membership,
//!   reconciles the lease group's voters ([`MembershipReconciler`]) and assigns each
//!   group's lease to its placement owner ([`LeaseAssigner`]) when this node leads.
//!
//! The replication transport is **shared** between the plane (which the broker
//! registers peers on) and the store (which replicates over it), so a session-log
//! append fans out to the same peer links the consensus RPCs use. The group-routed
//! log is shared too: the **durable retained keyspace** (ADR 0037) commits through
//! the same leases and replica links as the session keys. Returns the store and the
//! retained handle (to hand the hub) and the plane (to attach to the hub).

use crate::cluster_log::ReplicaState;
use crate::cluster_store::{GroupRoutedLog, LocalLeaseSource};
use crate::durable_plane::DurablePlane;
use crate::lease_assign::LeaseAssigner;
use crate::lease_group::{config as lease_config, LeaseRaft};
use crate::lease_membership::{apply_action, raft_view, FailureDomain, MembershipReconciler};
use crate::lease_raft::{GroupId, RaftNodeId};
use crate::lease_store::LeaseStore;
use crate::node_registry::raft_id;
use crate::placement::{group_of_key, Placement, NUM_GROUPS};
use crate::raft_mesh::MeshRaftNetwork;
use crate::repl_net::PeerReplicaTransport;
use crate::NodeId;
use mqtt_storage::logged::ReplicatedSessionStore;
use mqtt_storage::retained_log::{DurableRetained, ReplicatedRetained};
use mqtt_storage::SessionStore;
use openraft::storage::Adaptor;
use openraft::Raft;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tracing::{debug, warn};

/// How often the driver reconciles membership + lease assignment against the live
/// placement ring. The work is a no-op in steady state (a membership read), so the tick
/// only needs to be brisk enough to react to a membership change within a second or so.
/// It is kept at ~1s rather than sub-second on purpose (ADR 0026 §2): on a persistent
/// store every reconfiguration fsyncs, and a fast tick that re-fires the same
/// membership/lease change before the previous one commits amplifies durable-store churn.
const DRIVER_TICK: Duration = Duration::from_secs(1);

/// Consecutive ticks the lease voter set must hold steady before durable ownership
/// is restricted to it (ADR 0049). Bridges the founder-bootstrap growth window
/// (sole voter → `voter_cap`) so ownership is never concentrated on the founder and
/// then thrashed out by mass migration; until then, ownership falls back to the
/// eligible members.
const VOTER_STABLE_TICKS: u32 = 3;

/// Driver ticks between catch-up sweeps while one is armed (ADR 0043 P1): brisk
/// enough that a joiner back-fills within seconds of entering a replica set, slow
/// enough that the owner is not asked to re-commit the same key more often than a
/// round of re-commits can complete.
const CATCH_UP_SWEEP_EVERY: u32 = 5;

/// Sweeps to attempt per membership change before standing down. A copy that stays
/// hollow this long (~2 minutes) is not going to heal by asking again — the next
/// membership event re-arms the sweep anyway. Bounds steady-state chatter from a
/// key whose only remaining copies are stale leftovers the owner has superseded.
const CATCH_UP_SWEEP_BUDGET: u32 = 24;

/// Open a persistent store, briefly retrying transient file-lock contention.
///
/// On a fast restart over the same data dir, the previous holder's `redb` lock (for
/// instance the replica-writer's copy of `replicas.redb`, ADR 0027, or a prior process
/// during a rolling restart) may not be released for a few milliseconds. Retry the open
/// over a bounded window so the restart does not spuriously fail; a genuinely unusable
/// store still panics, after the window.
async fn open_retrying<T, E: std::fmt::Display>(
    mut open: impl FnMut() -> Result<T, E>,
    what: &str,
) -> T {
    let mut last = String::new();
    for _ in 0..30 {
        match open() {
            Ok(v) => return v,
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("{what}: still locked after retrying: {last}");
}

/// Build a node's durable session store, durable retained keyspace, lease-group
/// endpoint, and background driver. Returns the store and retained handle (for the
/// hub) and the [`DurablePlane`] (to attach to the hub so it routes peer
/// consensus/replication frames).
///
/// `can_bootstrap` marks this node a **founder** (started with no SWIM seeds): only
/// a founder creates the lease group; joiners wait to be added by the founder's
/// leader. Exactly one founder per cluster (see [`MembershipReconciler::new`]).
///
/// `voter_cap` bounds the lease-group voter set (ADR 0021): at most that many members
/// vote, every other member joins as a learner that still receives the lease log.
///
/// # Panics
/// Panics if the lease `Raft` fails to start (a programming/config error at boot).
#[allow(clippy::too_many_arguments)]
pub async fn build_durable_node(
    node_id: NodeId,
    placement: Arc<RwLock<Placement>>,
    can_bootstrap: bool,
    voter_cap: usize,
    failure_domains: &BTreeMap<NodeId, FailureDomain>,
    data_dir: Option<&std::path::Path>,
    commit_delay: Option<Arc<std::sync::atomic::AtomicU64>>,
) -> (
    Arc<dyn SessionStore>,
    Arc<dyn DurableRetained>,
    DurablePlane,
    tokio::task::JoinHandle<()>,
) {
    let local = raft_id(&node_id);

    // --- lease consensus group + durable-plane endpoint ---
    let network = MeshRaftNetwork::new();
    // On-disk persistence when a data dir is given (ADR 0018): the lease store (phase 2 —
    // restoring Raft safety via the persisted vote) and the follower replica copy (phase
    // 3 — clustered sessions survive a full-cluster restart); otherwise both in-memory.
    let lease_store = match data_dir {
        Some(dir) => {
            open_retrying(
                || LeaseStore::open(dir.join("lease.redb")).map_err(|e| e.to_string()),
                "open the lease store",
            )
            .await
        }
        None => LeaseStore::new(),
    }
    .with_commit_delay(commit_delay);
    let (log_store, state_machine) = Adaptor::new(lease_store.clone());
    let raft: LeaseRaft = Raft::new(
        local,
        lease_config(),
        network.clone(),
        log_store,
        state_machine,
    )
    .await
    .expect("lease raft starts");
    let transport = Arc::new(PeerReplicaTransport::new());
    // One follower-copy `ReplicaState`, shared: the plane applies inbound Replicates
    // into it, and the store reads it back for takeover recovery (workstream F).
    // Persistent (ADR 0018 phase 3) when a data dir is given, so the committed copy
    // survives a restart.
    let replicas = Arc::new(Mutex::new(match data_dir {
        Some(dir) => {
            open_retrying(
                || ReplicaState::open(dir.join("replicas.redb")).map_err(|e| e.to_string()),
                "open the replica store",
            )
            .await
        }
        None => ReplicaState::new(),
    }));
    let plane = DurablePlane::new(
        raft.clone(),
        network.clone(),
        transport.clone(),
        replicas.clone(),
        placement.clone(),
    );

    // --- durable store over the shared transport ---
    let lease_source = LocalLeaseSource::new(lease_store.clone(), local);
    // One group-routed log, shared: the session store replicates queue/meta keys
    // through it, and the retained keyspace (ADR 0037) commits `r/<topic>` keys through
    // the same leases, epochs, and replica links — one durable plane, two keyspaces.
    let group_log = Arc::new(GroupRoutedLog::new(
        node_id.clone(),
        placement.clone(),
        transport.clone(),
        lease_source,
        replicas.clone(),
    ));
    // The plane serves inbound catch-up requests (ADR 0043 P1) through the store's
    // group routing: a hollow replica asks, the owner re-commits the key's log.
    plane.set_catch_up_source(group_log.clone());
    let store: Arc<dyn SessionStore> = Arc::new(ReplicatedSessionStore::new(group_log.clone()));
    let retained: Arc<dyn DurableRetained> = Arc::new(ReplicatedRetained::new(group_log.clone()));

    // --- driver: membership + lease assignment over the live placement ---
    // The handle is returned so the caller can stop the driver on shutdown (otherwise
    // the loop outlives `raft`, spinning against a shut-down consensus handle) and so a
    // restart can release the on-disk lease/replica locks (ADR 0018 phase 5).
    // Re-key the operator-supplied failure-domain topology by raft id (ADR 0016 T4). This is
    // the static *seed*: `run_driver` overlays it each tick with the labels nodes advertise
    // over gossip (ADR 0016 T5), so the map self-assembles and a static cluster-uniform table
    // is no longer required. A node absent from both is its own singleton domain (no spread
    // constraint).
    let domains: BTreeMap<RaftNodeId, FailureDomain> = failure_domains
        .iter()
        .map(|(node, dom)| (raft_id(node), dom.clone()))
        .collect();
    let driver = tokio::spawn(run_driver(
        raft,
        network,
        local,
        lease_store,
        placement.clone(),
        MembershipReconciler::new(local, can_bootstrap, voter_cap),
        LeaseAssigner::new(placement),
        domains,
        CatchUp {
            node: node_id,
            transport,
            replicas,
            source: group_log,
        },
    ));

    (store, retained, plane, driver)
}

/// The desired lease-group voter set: placement `members`, with a member **admitted** only
/// once its raft link is up (ADR 0028).
///
/// Admitting a voter the leader cannot reach loses the quorum lease and churns the group
/// through elections until the mesh converges — the multi-minute formation churn that made
/// durable unusable on every startup. The gate is **admission-only**: a node already a voter
/// stays one across a transient link blip (it must drop its link *and* be evicted from
/// `members` by SWIM to be removed), so this never amplifies a flap. `local` is always
/// reachable to itself. A member removed from `members` (SWIM declared it dead) drops out
/// even if it was a voter.
fn admit_desired(
    members: &[RaftNodeId],
    local: RaftNodeId,
    voters: &BTreeSet<RaftNodeId>,
    is_connected: impl Fn(RaftNodeId) -> bool,
) -> BTreeSet<RaftNodeId> {
    members
        .iter()
        .copied()
        .filter(|id| *id == local || voters.contains(id) || is_connected(*id))
        .collect()
}

/// The handles the **catch-up sweep** (ADR 0043 P1) runs over: this node's
/// identity, the shared replication transport (to discover keys and ask owners to
/// re-commit), its own follower copy (to judge hollowness and stamp the durable
/// caught-up watermark), and the local catch-up source (to heal keys of groups
/// this node itself owns).
struct CatchUp {
    node: NodeId,
    transport: Arc<PeerReplicaTransport>,
    replicas: Arc<Mutex<ReplicaState>>,
    source: Arc<dyn crate::durable_plane::CatchUpSource>,
}

impl CatchUp {
    /// One sweep over every group this node replicates, driving each toward its
    /// durable **caught-up stamp** (the watermark recovery reads trust):
    ///
    /// - stamp matches the current replica set → current, nothing to do;
    /// - stamped for a **superset** of the current set (a pure shrink — every
    ///   current member was in the cohort this node was already current with) →
    ///   re-stamp immediately, no data moved;
    /// - otherwise (never stamped, or new cohort members) → full catch-up: every
    ///   other member of the set must answer key discovery, every discovered key
    ///   of the group must be locally gap-free (hollow keys are healed by asking
    ///   the owner to re-commit — or re-committing ourselves when we own the
    ///   group), and only then is the group stamped.
    ///
    /// Returns how many groups are still pending (0 = swept clean).
    async fn sweep(&self, placement: &Arc<RwLock<Placement>>) -> usize {
        // Key discovery, attributed per peer: a group is only stamped once every
        // OTHER member of its replica set has been heard (an unheard member may
        // hold keys we would otherwise never learn we are missing).
        let mut responses: BTreeMap<NodeId, Vec<String>> = BTreeMap::new();
        for peer in self.transport.connected() {
            if let Some(keys) = self.transport.keys_of(&peer).await {
                responses.insert(peer, keys);
            }
        }
        // Keys per group, unioned across our copy and every heard peer's.
        let mut group_keys: BTreeMap<GroupId, BTreeSet<String>> = BTreeMap::new();
        for key in self
            .lock_replicas()
            .keys()
            .into_iter()
            .chain(responses.values().flatten().cloned())
        {
            group_keys
                .entry(group_of_key(&key))
                .or_default()
                .insert(key);
        }

        let mut pending = 0;
        let mut stamps: Vec<(GroupId, Vec<NodeId>)> = Vec::new();
        for group in 0..NUM_GROUPS {
            let (set, owner) = {
                let p = placement
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (p.group_replica_set(group), p.group_owner(group))
            };
            if !set.contains(&self.node) {
                continue; // not ours to hold
            }
            let shrink_of_known_cohort = {
                let r = self.lock_replicas();
                if r.group_current(group, &set) {
                    continue; // stamp already current
                }
                // A pure shrink keeps custody: we were current with a cohort that
                // contained every remaining member, so our copy's completeness
                // story is unchanged — no unknown-history member entered.
                r.caught_up_set(group)
                    .is_some_and(|stored| set.iter().all(|n| stored.contains(n)))
            };
            if shrink_of_known_cohort {
                stamps.push((group, set));
                continue;
            }
            // Full catch-up: every other member must have answered discovery.
            if !set
                .iter()
                .filter(|n| **n != self.node)
                .all(|n| responses.contains_key(n))
            {
                pending += 1;
                continue;
            }
            // Heal every hollow key of the group; stamp only when none is left.
            let mut hollow_keys = Vec::new();
            for key in group_keys.get(&group).into_iter().flatten() {
                let hollow = {
                    let r = self.lock_replicas();
                    // A discovered key we hold nothing of (no entries, no
                    // truncation watermark) is hollow; so is a gappy copy.
                    !r.complete(key) || (r.entries(key).is_empty() && r.watermark(key) == 0)
                };
                if hollow {
                    hollow_keys.push(key.clone());
                }
            }
            if hollow_keys.is_empty() {
                stamps.push((group, set));
                continue;
            }
            pending += 1;
            for key in hollow_keys {
                if owner == self.node {
                    // We own the group: recover + re-commit locally (the same
                    // path an inbound ReplicaCatchUp drives).
                    self.source.catch_up_key(&key).await;
                } else {
                    self.transport.request_catch_up(&owner, &key);
                }
            }
        }
        if !stamps.is_empty() {
            debug!(
                stamped = stamps.len(),
                pending, "catch-up sweep stamped groups current (ADR 0043 P1)"
            );
            // The stamp is one fsync'd batch; run it off the async worker.
            let replicas = self.replicas.clone();
            let _ = tokio::task::spawn_blocking(move || {
                replicas
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .mark_groups_current(&stamps);
            })
            .await;
        }
        pending
    }

    fn lock_replicas(&self) -> std::sync::MutexGuard<'_, ReplicaState> {
        self.replicas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The lease-group control loop: on each tick, reconcile the voter set toward the
/// live membership and (as leader) keep each group's lease on its placement owner.
/// A membership change (and boot) also arms the replica catch-up sweep (ADR 0043
/// P1), which back-fills this node's copies of the groups it newly replicates.
#[allow(clippy::too_many_arguments)]
async fn run_driver(
    raft: LeaseRaft,
    network: MeshRaftNetwork,
    local: RaftNodeId,
    lease_store: LeaseStore,
    placement: Arc<RwLock<Placement>>,
    reconciler: MembershipReconciler,
    assigner: LeaseAssigner,
    domains: BTreeMap<RaftNodeId, FailureDomain>,
    catch_up: CatchUp,
) {
    // A one-tick debounce: only act once the desired set is stable across a tick, so
    // a flapping member does not churn the voter set.
    let mut prev_desired: BTreeSet<RaftNodeId> = BTreeSet::new();
    // Catch-up sweep state: armed (with a budget) on boot and on every placement
    // membership change, run every few ticks until nothing is hollow or the budget
    // is spent. `prev_members` starts empty so the first tick always arms.
    let mut prev_members: BTreeSet<RaftNodeId> = BTreeSet::new();
    let mut sweeps_left: u32 = 0;
    let mut ticks_to_sweep: u32 = 0;
    // Static-seed overrides already warned about, so the mismatch is loud once per
    // (node, label) rather than per tick (ADR 0016 T6 loudness rule).
    let mut warned_overrides: BTreeSet<(RaftNodeId, FailureDomain)> = BTreeSet::new();
    // Voter-set stability tracking (ADR 0049): durable ownership is restricted to the
    // voters, but ONLY once the set has settled. During founder bootstrap the voter set
    // grows (sole voter → voter_cap); restricting mid-growth would concentrate every
    // group's ownership on the founder and then thrash it back out via mass migration.
    // While the set is still moving we push an empty set (Placement falls back to the
    // eligible members — the pre-ADR-0049 behaviour), so no concentration ever happens.
    // By the time it settles in a small all-voter cluster, voters == eligible, so the
    // restriction is a no-op there; a bounded cluster gets the restriction once stable.
    let mut prev_voter_rids: BTreeSet<RaftNodeId> = BTreeSet::new();
    let mut voter_stable_ticks: u32 = 0;
    // A raft-id → NodeId map, accumulated across ticks and NEVER forgotten, used to
    // resolve committed lease holders back to node ids for the data-path owner map
    // (2026-07-20 post-mortem). Keeping departed nodes mapped means a lease still held
    // by a momentarily-ungossiped voter — the exact skew that split ownership — remains
    // resolvable until the assigner moves it.
    let mut id_map: BTreeMap<RaftNodeId, NodeId> = BTreeMap::new();
    loop {
        tokio::time::sleep(DRIVER_TICK).await;

        let view = raft_view(&raft);

        // Push the current lease voter set into Placement so durable ownership is
        // restricted to lease-eligible nodes (ADR 0049 P1): a learner cannot serve
        // a durable group, so it must never be selected as an owner. The voter set
        // is committed raft membership (identical across nodes), keeping owner
        // selection deterministic. Empty until the group forms → eligible fallback.
        {
            // Read the committed voter ids from the same accessor the readiness
            // signal trusts (`DurablePlane::voter_count`), not `raft_view` — the
            // latter reads a base membership that under a bounded voter set (ADR
            // 0021) does not reflect the effective voters.
            let voter_rids: BTreeSet<RaftNodeId> = raft
                .metrics()
                .borrow()
                .membership_config
                .voter_ids()
                .collect();
            if voter_rids == prev_voter_rids {
                voter_stable_ticks = voter_stable_ticks.saturating_add(1);
            } else {
                voter_stable_ticks = 0;
                prev_voter_rids.clone_from(&voter_rids);
            }
            // Restrict only once the voter set has held steady for a couple of ticks;
            // otherwise fall back to eligible (empty set) to avoid bootstrap churn.
            let voter_nodes: BTreeSet<NodeId> = if voter_stable_ticks >= VOTER_STABLE_TICKS {
                let p = placement
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                p.members()
                    .into_iter()
                    .filter(|n| voter_rids.contains(&raft_id(n)))
                    .collect()
            } else {
                BTreeSet::new()
            };
            placement
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .set_voters(voter_nodes);
        }

        // Push the COMMITTED durable owner map (group → holder) so the data path routes
        // and gates durable ownership by the replicated lease — the single agreed truth —
        // rather than recomputing it from the gossip-derived HRW ring, which a transient
        // membership skew can split from the lease into a permanent NotOwner (2026-07-20
        // post-mortem). Unlike `set_voters` this is NOT gated on stability: the committed
        // lease is authoritative whenever it exists; a group with no lease yet simply
        // falls back to the HRW owner in Placement. The lease map is identical on every
        // node (replicated), so ownership stays deterministic across the cluster.
        {
            let members = placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .members();
            for n in &members {
                id_map.entry(raft_id(n)).or_insert_with(|| n.clone());
            }
            let mut owners: BTreeMap<GroupId, NodeId> = BTreeMap::new();
            for group in 0..NUM_GROUPS {
                if let Some(holder) = lease_store.current_lease(group).map(|rec| rec.holder) {
                    if let Some(nid) = id_map.get(&holder) {
                        owners.insert(group, nid.clone());
                    }
                }
            }
            placement
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .set_lease_owners(owners);
        }

        let (members, live_domains): (Vec<RaftNodeId>, BTreeMap<RaftNodeId, FailureDomain>) = {
            let p = placement
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let members = p.members().iter().map(raft_id).collect();
            // Gossip-propagated labels (ADR 0016 T5), re-keyed by raft id.
            let live = p
                .domains()
                .into_iter()
                .map(|(node, dom)| (raft_id(&node), dom))
                .collect();
            (members, live)
        };
        let desired = admit_desired(&members, local, &view.voters, |id| network.is_connected(id));

        if desired == prev_desired {
            // Effective topology = the static seed (ADR 0016 T4, cluster-uniform config)
            // overlaid with self-advertised labels learned from gossip (T5), which win.
            // A live label that contradicts the static seed is loud (once per label):
            // one of the two configs is stale, and silent divergence would let an
            // operator trust a map that is not the one being enforced.
            for (id, live) in &live_domains {
                if let Some(seed) = domains.get(id) {
                    if seed != live && warned_overrides.insert((*id, live.clone())) {
                        warn!(
                            node = *id,
                            static_label = %seed,
                            gossiped_label = %live,
                            "gossiped failure domain overrides the static MQTTD_FAILURE_DOMAINS entry"
                        );
                    }
                }
            }
            let mut effective = domains.clone();
            effective.extend(live_domains);
            let action = reconciler.decide_with_domains(&view, &desired, &effective);
            if let Err(e) = apply_action(&raft, &action).await {
                warn!(error = %e, "lease-group membership reconcile failed");
            }
            match assigner.reconcile(&raft, &lease_store).await {
                Ok(n) if n > 0 => debug!(assigned = n, "lease assignments reconciled"),
                Ok(_) => {}
                Err(e) => warn!(error = %e, "lease assignment reconcile failed"),
            }
        }
        prev_desired = desired;

        // --- replica catch-up (ADR 0043 P1) ---
        let member_set: BTreeSet<RaftNodeId> = members.iter().copied().collect();
        if member_set != prev_members {
            prev_members = member_set;
            sweeps_left = CATCH_UP_SWEEP_BUDGET;
            ticks_to_sweep = 0;
        }
        if sweeps_left > 0 {
            if ticks_to_sweep == 0 {
                // Inline on the driver: bounded by per-peer RPC timeouts against
                // *registered* (live-link) peers only, and it runs only around
                // membership events — a short reconcile pause there is fine.
                let pending = catch_up.sweep(&placement).await;
                sweeps_left = if pending == 0 { 0 } else { sweeps_left - 1 };
                ticks_to_sweep = CATCH_UP_SWEEP_EVERY;
            } else {
                ticks_to_sweep -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{admit_desired, build_durable_node};
    use crate::lease_raft::RaftNodeId;
    use crate::placement::{Placement, DEFAULT_REPLICAS};
    use crate::NodeId;
    use mqtt_core::{ClientId, Message, QoS};
    use mqtt_storage::SessionStore;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant};

    fn set(ids: &[RaftNodeId]) -> BTreeSet<RaftNodeId> {
        ids.iter().copied().collect()
    }

    /// ADR 0028 admission gate. The local node and any **reachable** member are admitted;
    /// a member that is neither reachable nor already a voter is held out (the formation-churn
    /// fix — never admit a voter the leader cannot reach).
    #[test]
    fn admit_desired_admits_local_and_reachable_members_only() {
        let members = [1, 2, 3];
        let voters = set(&[1]); // only the founder is a voter so far
                                // Only node 2's link is up; node 3's is not.
        let connected = |id: RaftNodeId| id == 2;
        // local (1) always; 2 is reachable → admitted; 3 is unreachable + not a voter → held out.
        assert_eq!(admit_desired(&members, 1, &voters, connected), set(&[1, 2]));
    }

    /// The gate is admission-only: a node already a voter is **not** dropped when its link
    /// blips (it must also leave `members` — SWIM eviction — to be removed), so a transient
    /// blip never churns the voter set.
    #[test]
    fn admit_desired_keeps_a_current_voter_through_a_link_blip() {
        let members = [1, 2, 3];
        let voters = set(&[1, 2, 3]); // all three are voters
        let connected = |_id: RaftNodeId| false; // every link is momentarily down
                                                 // All stay, because they are current voters still present in `members`.
        assert_eq!(
            admit_desired(&members, 1, &voters, connected),
            set(&[1, 2, 3])
        );
    }

    /// A member SWIM evicted from `members` (declared dead) drops out even if it was a voter.
    #[test]
    fn admit_desired_drops_a_member_evicted_from_placement() {
        let members = [1, 2]; // node 3 was evicted (dead)
        let voters = set(&[1, 2, 3]); // 3 was a voter
        let connected = |_id: RaftNodeId| true;
        // 3 is gone from `members`, so it is not in the desired set regardless of voter status.
        assert_eq!(admit_desired(&members, 1, &voters, connected), set(&[1, 2]));
    }

    /// A single node's durable stack bootstraps itself (the driver elects the lease
    /// group and assigns leases), after which an enqueue commits and replays — the
    /// whole assembly wired together end to end on one node. The shared group log
    /// serves the retained keyspace too (ADR 0037 P3): a retained commit through the
    /// returned handle lands under the real, consensus-minted lease epoch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_node_durable_store_bootstraps_and_serves() {
        let node = NodeId("durable-solo".to_string());
        let placement = Arc::new(RwLock::new(Placement::new(node.clone(), DEFAULT_REPLICAS)));
        let (store, retained, _plane, _driver) =
            build_durable_node(node, placement, true, 5, &BTreeMap::new(), None, None).await;

        let client = ClientId("c".to_string());
        let msg = Message::new(
            "t".to_string(),
            bytes::Bytes::from_static(b"durable"),
            QoS::AtLeastOnce,
            false,
        );

        // Poll until the driver has bootstrapped the lease group and assigned this
        // node its groups' leases, at which point the enqueue commits.
        wait_writable(&store, &client, &msg).await;

        // The committed message replays.
        let pending = store.pending(&client, 0, 100).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0].message.payload[..], b"durable");

        // The retained keyspace commits through the same plane: the token's epoch is
        // the group's real lease epoch (consensus-minted, ≥ 1 — never the 0 of an
        // un-leased backend), and the committed value reads back.
        let (epoch, offset) = retained
            .set(
                "dev/1/state",
                b"open",
                1,
                &mqtt_storage::app_props::AppProps::default(),
            )
            .await
            .unwrap();
        assert!(
            epoch >= 1,
            "the epoch is minted by the lease plane, got {epoch}"
        );
        assert_eq!(offset, 1, "first write to the topic's retained key");
        let e = retained.get("dev/1/state").await.unwrap().unwrap();
        assert_eq!(e.payload, b"open");
        assert_eq!(e.token(), (epoch, offset));
    }

    /// Poll an enqueue until the durable store is writable (lease bootstrapped and
    /// assigned to this node), or fail after a generous deadline.
    async fn wait_writable(store: &Arc<dyn SessionStore>, client: &ClientId, msg: &Message) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if store.enqueue(client, msg).await.is_ok() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "durable store never became writable (lease not assigned)"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// ADR 0018 phase 5 (cluster-path node-level restart). A persistent durable node
    /// bootstraps its lease group on disk, is **fully torn down** (driver aborted, raft
    /// shut down, store + plane dropped — which releases the `lease.redb`/`replicas.redb`
    /// file locks), and is then **rebuilt from the same data directory**: it reopens its
    /// persisted lease state without a double-init, re-leads, and becomes writable again.
    ///
    /// This is the assembly-level proof that graceful shutdown (ADR 0019) makes the
    /// durable plane cleanly restartable. It deliberately does **not** assert the
    /// pre-restart message survives: a *single* durable node holds committed session
    /// entries in the leader's in-memory log until a follower has them, so session
    /// restart-durability needs R≥2 (proven at the store level in `cluster_log`); here
    /// the persistent state that must survive is the **lease vote/membership**.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_persistent_durable_node_restarts_from_its_data_dir() {
        let dir = tempfile::tempdir().unwrap();
        let node = NodeId("durable-restart".to_string());
        let client = ClientId("c".to_string());
        let msg = Message::new(
            "t".to_string(),
            bytes::Bytes::from_static(b"durable"),
            QoS::AtLeastOnce,
            false,
        );

        // --- lifetime #1: bootstrap on disk, become writable ---
        let placement = Arc::new(RwLock::new(Placement::new(node.clone(), DEFAULT_REPLICAS)));
        let (store, retained, plane, driver) = build_durable_node(
            node.clone(),
            placement,
            true,
            5,
            &BTreeMap::new(),
            Some(dir.path()),
            None,
        )
        .await;
        wait_writable(&store, &client, &msg).await;

        // --- teardown: release the on-disk locks (the part ADR 0019 unblocks) ---
        driver.abort();
        let _ = driver.await;
        plane.raft().shutdown().await.unwrap();
        drop(store);
        // The retained handle shares the group log (and with it the lease/replica
        // database handles), so it must drop too before the files can reopen.
        drop(retained);
        drop(plane);
        // The last `Database` handle drops synchronously above; give any in-flight
        // blocking apply a moment to release before reopening the same files.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // --- lifetime #2: a fresh node over the SAME directory recovers and re-leads ---
        let placement = Arc::new(RwLock::new(Placement::new(node.clone(), DEFAULT_REPLICAS)));
        let (store, _retained, plane, driver) = build_durable_node(
            node,
            placement,
            true,
            5,
            &BTreeMap::new(),
            Some(dir.path()),
            None,
        )
        .await;
        // Becoming writable again proves the persisted lease store reopened (no
        // "Database already open" lock, no double-init panic) and the node re-led.
        wait_writable(&store, &client, &msg).await;

        driver.abort();
        let _ = driver.await;
        plane.raft().shutdown().await.unwrap();
    }
}
