//! Session placement over live membership (ADR 0001 §1, [ADR 0007](../../../docs/adr/0007-durable-store-integration.md)).
//!
//! Wraps the deterministic [`crate::hrw`] primitives with the *current* eligible
//! member set — this node plus every peer not believed `Dead` — and recomputes
//! as SWIM membership changes. It is pure and sans-I/O: feed it
//! [`observe`](Placement::observe) for each membership change and query
//! [`owner`](Placement::owner) / [`replica_set`](Placement::replica_set) /
//! [`owns`](Placement::owns). The replica set is bounded at `R` (default 3) —
//! the small group ADR 0001 scopes durability/consensus to, not the whole
//! cluster.
//!
//! ## Placement groups ([ADR 0007](../../../docs/adr/0007-durable-store-integration.md) §1)
//!
//! Ownership granularity is the **placement group** (shard), not the individual
//! client: `group(client) = stable_hash(client) % `[`NUM_GROUPS`]. A group's owner
//! and replica set are HRW over the *group* key, so every session in a group shares
//! one owner, one replica set, and (in the durable backend) one lease/epoch — which
//! bounds the number of leases and replica sets to `NUM_GROUPS` regardless of how
//! many sessions exist. The per-client queries below resolve through the client's
//! group, so a session is owned by — and relocated to — its **group** owner.
//!
//! `Suspect` members stay in the ring: a transiently-slow node should not
//! trigger ownership churn (and the reassignment it would reverse on
//! refutation). Only a confirmed `Dead` removes a node, which is exactly the
//! ADR 0001 takeover trigger.

use crate::lease_raft::GroupId;
use crate::swim::MemberState;
use crate::{hrw, NodeId};
use std::collections::{BTreeMap, BTreeSet};

/// Default replication factor: each session's replica set spans R nodes
/// (ADR 0001 §1).
pub const DEFAULT_REPLICAS: usize = 3;

/// The number of placement groups (shards) the keyspace is partitioned into
/// (ADR 0007 §1). A cluster-wide constant: changing it reshuffles group ownership,
/// so every node must agree. Bounds the lease/replica-set count to this regardless
/// of session count.
pub const NUM_GROUPS: u64 = 256;

/// A snapshot of cluster-wide replication health (#167): the configured replication
/// factor and the smallest replica set any placement group currently has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationHealth {
    /// The configured replication factor R.
    pub desired: usize,
    /// The smallest replica-set size across all groups right now — the worst-case
    /// durability. `min_actual < desired` means at least one group commits with fewer
    /// copies than configured.
    pub min_actual: usize,
}

impl ReplicationHealth {
    /// Whether any group is under-replicated (committing on fewer copies than configured).
    #[must_use]
    pub fn is_under_replicated(&self) -> bool {
        self.min_actual < self.desired
    }
}

/// The placement group a `client` belongs to — a deterministic, version-stable hash
/// of its id modulo [`NUM_GROUPS`], identical on every node.
#[must_use]
pub fn group_of(client: &str) -> GroupId {
    hrw::stable_id(client.as_bytes()) % NUM_GROUPS
}

/// The placement group a durable **log key** belongs to. Log keys carry a 2-byte
/// kind prefix (`q/`/`m/` session keys, `r/` retained keys) ahead of the placement
/// key; this strips it and hashes what follows. The single derivation shared by the
/// group router and the replica fence, so they can never disagree about a key's
/// group (a prefix-less key hashes as itself).
#[must_use]
pub fn group_of_key(key: &str) -> GroupId {
    group_of(key.get(2..).unwrap_or(key))
}

/// The HRW key for a placement group (so groups hash independently of any client).
fn group_key(group: GroupId) -> String {
    format!("group/{group}")
}

/// The placement ring for one node: maps client ids to their owner and replica
/// set over the current eligible membership.
#[derive(Debug, Clone)]
pub struct Placement {
    local: NodeId,
    replicas: usize,
    /// Nodes eligible to own sessions: this node plus non-`Dead` peers. A
    /// `BTreeSet` keeps the derived node list deterministic across calls.
    eligible: BTreeSet<NodeId>,
    /// The current lease-consensus voter set (ADR 0049), pushed each reconcile
    /// tick by the durable driver. Durable *ownership* is restricted to these
    /// nodes — a learner cannot hold a servable lease, so a group owned by one
    /// refuses every persistent attach forever (the 2026-07-14 post-mortem).
    /// Empty means "not yet known" (bootstrap / non-durable), in which case
    /// ownership falls back to the full eligible set exactly as before. Data
    /// *replication* still spans the eligible set — only ownership is bounded
    /// (ADR 0021 keeps replication independent of the voter cap).
    voters: BTreeSet<NodeId>,
    /// The **committed** durable owner of each group — `group -> holder`, read from
    /// the replicated lease map and pushed each reconcile tick by the durable driver
    /// (2026-07-20 post-mortem). This is the *actual* ownership the data path must
    /// follow: the HRW ring below is only the *desired* topology the lease assigner
    /// drives toward. Routing durable writes by HRW instead of the committed lease is
    /// what let a transient membership skew split ownership from the lease into a
    /// permanent `NotOwner`. A group absent here (no lease assigned yet, or non-durable
    /// bootstrap) falls back to the HRW owner, so behaviour is unchanged until the
    /// driver has a committed lease to report.
    lease_owners: BTreeMap<GroupId, NodeId>,
    /// Each peer's inter-node (peer-link) address, so the owner of a session can
    /// be reached for session relocation (ADR 0005).
    addrs: BTreeMap<NodeId, String>,
    /// This node's own failure-domain label (ADR 0016 T5), if configured. Kept here
    /// so [`domains`](Self::domains) reports it without waiting for gossip to round-trip.
    local_domain: Option<String>,
    /// Each peer's self-advertised failure-domain label, learned from gossip
    /// (ADR 0016 T5). Populated from membership observations so the lease-voter
    /// selection topology assembles itself instead of a static cluster-uniform map.
    domains: BTreeMap<NodeId, String>,
    /// The durable raft membership roster (issue #229): every node the lease
    /// consensus still counts — voters AND learners — as `(known ids, unmappable
    /// count)`. A CRASHED node stays on it (it may return holding pre-clear
    /// retained state); a DECOMMISSIONED node left it deliberately. `None` until
    /// the reconcile driver first pushes it. The tombstone reap gates on this
    /// roster, never on live gossip membership, exactly because gossip forgets
    /// the absent.
    durable_roster: Option<(BTreeSet<NodeId>, usize)>,
    /// The policy behind the smallest replica set a durable append may commit on
    /// (issue #167, defaulted ON in issue #239). Replica sets truncate to
    /// `min(R, members)`, so a shrinking cluster silently trades the configured
    /// durability for availability — down to quorum-of-1. Groups below the resolved
    /// floor REFUSE durable writes instead. `Fixed(1)` (this type's constructor
    /// default) keeps the pre-#167 behaviour: a lone node stays healthy when alone.
    floor: WriteFloor,
    /// High-water mark of `eligible.len()` — the largest membership this node has ever
    /// observed. The *fallback* witness for the derived floor, used only until the
    /// durable roster is first pushed. A high-water mark, not a live count, precisely
    /// because a liveness-following witness would disarm the floor during the very
    /// outage it exists to refuse.
    peak_members: usize,
}

/// The min-replicas write-floor policy (issues #167, #239) — what
/// [`Placement::min_replicas`] resolves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteFloor {
    /// #167's absolute floor: exactly this many copies, whatever the topology says.
    /// `1` means no refusal (single-copy acks accepted) — the documented opt-out.
    Fixed(usize),
    /// The derived default: a majority of the members this node *knows* about, capped at
    /// the replication factor. `declared` is the operator's `runtime.ready_min_members`,
    /// folded in as a floor on the witness so the policy is armed on a node's very first
    /// write — before any gossip observation or roster push.
    Majority { declared: usize },
    /// There is no durable plane at all (`durable.enabled = false`), so no write can be
    /// refused for want of replicas. Gates identically to `Fixed(1)`; it exists as its own
    /// variant so `/statusz` can say *why* the floor is 1 instead of mislabelling it as an
    /// operator-chosen floor (issue #239) — an operator running the default
    /// `min_replicas = "majority"` never "configured" a floor of 1.
    DurableOff,
}

impl Placement {
    /// Create a ring containing only this node. `replicas` is clamped to at
    /// least 1.
    #[must_use]
    pub fn new(local: NodeId, replicas: usize) -> Self {
        let mut eligible = BTreeSet::new();
        eligible.insert(local.clone());
        Self {
            local,
            replicas: replicas.max(1),
            eligible,
            voters: BTreeSet::new(),
            lease_owners: BTreeMap::new(),
            addrs: BTreeMap::new(),
            local_domain: None,
            domains: BTreeMap::new(),
            // The constructor keeps the pre-#167 posture (no refusal); the operator's
            // policy is injected at assembly via `with_write_floor` /
            // `with_min_replicas`, so every direct constructor — every unit harness
            // included — keeps its existing meaning.
            floor: WriteFloor::Fixed(1),
            peak_members: 1,
            durable_roster: None,
        }
    }

    /// Set this node's own failure-domain label (ADR 0016 T5), reported by
    /// [`domains`](Self::domains) alongside the gossip-learned peer labels. Builder-style
    /// so it can be chained onto [`new`](Self::new) at startup.
    #[must_use]
    pub fn with_local_domain(mut self, domain: Option<String>) -> Self {
        self.local_domain = domain;
        self
    }

    /// Set an ABSOLUTE min-replicas write floor (issue #167), clamped to at least 1.
    /// Builder-style, set once at startup from the operator's configuration.
    #[must_use]
    pub fn with_min_replicas(mut self, floor: usize) -> Self {
        self.floor = WriteFloor::Fixed(floor.max(1));
        self
    }

    /// Set the min-replicas write-floor POLICY (issue #239) — the general form of
    /// [`with_min_replicas`](Self::with_min_replicas), used at assembly to install the
    /// derived-majority default. Builder-style, set once at startup.
    #[must_use]
    pub fn with_write_floor(mut self, floor: WriteFloor) -> Self {
        self.floor = floor;
        self
    }

    /// The RESOLVED min-replicas write floor: the smallest replica set a durable append
    /// may commit on right now (1 = no refusal). The single resolution point the data
    /// path's gate reads.
    ///
    /// For [`WriteFloor::Majority`] the floor is `min(R, witness) / 2 + 1`, where the
    /// witness is, in order of authority:
    ///
    /// 1. the **durable raft roster** (issue #229), when it has been pushed *and is
    ///    non-empty*. This is the authority for a reason: it is *quorum-committed*, so it
    ///    cannot shrink while a majority is down — the floor therefore cannot self-disarm
    ///    during the very outage it guards — it survives restart via the on-disk lease
    ///    state (so a node that cold-boots alone out of a three-member group still
    ///    refuses), and it *does* shrink on a consented decommission, so a deliberate
    ///    resize needs no operator edit. An **empty** roster is not a membership signal at
    ///    all (a group cannot have zero members; the reconcile driver pushes one while the
    ///    raft group is still uninitialised), so it is ignored rather than allowed to
    ///    disarm the floor and discard the fallback witness below;
    /// 2. otherwise the high-water observed membership ([`peak_members`](Self::peak_members)
    ///    — pre-roster fallback only);
    ///
    /// with `declared` (`runtime.ready_min_members`) as a lower bound on either, closing
    /// the boot window before the first observation or roster push.
    ///
    /// Capping the witness at `R` is what makes the derived floor always satisfiable: a
    /// majority of R=3 is 2, and the write quorum is already `len/2 + 1`, so at 2 or 3
    /// live members this floor refuses nothing a one-at-a-time roll did not already
    /// require. A witness of 1 resolves to `1/2 + 1 = 1`: the lone, never-peered node
    /// keeps acking.
    ///
    /// What this returns is a bound on the replica-set **size** the append gate demands,
    /// not on the number of copies the commit itself waits for (`ClusterLog`'s write
    /// quorum is `replica_set.len() / 2 + 1`). For the derived floor the two coincide at
    /// every satisfiable size (a set of 2 or 3 needs 2 acks, and the floor is 2), which is
    /// why the default promise — "no group acks a durable write on a single copy once this
    /// node knows it has peers" — is exactly what the gate enforces.
    ///
    /// A pure function of the fields — both callers already hold the placement lock.
    #[must_use]
    pub fn min_replicas(&self) -> usize {
        match self.floor {
            WriteFloor::Fixed(n) => n,
            WriteFloor::Majority { declared } => {
                let witness = self
                    .durable_roster
                    .as_ref()
                    .map(|(known, unknown)| known.len() + unknown)
                    // An empty roster is an uninitialised raft group, not a one-member
                    // cluster: fall through to the gossip high-water witness.
                    .filter(|&members| members > 0)
                    .unwrap_or(self.peak_members)
                    .max(declared)
                    .min(self.replicas);
                witness / 2 + 1
            }
            // No durable plane: nothing can be refused for want of replicas.
            WriteFloor::DurableOff => 1,
        }
    }

    /// How the write floor in [`Self::min_replicas`] came to be, for `/statusz` (issue #239).
    ///
    /// Three-valued on purpose: a floor of `1` means something different in each case, and
    /// reporting `configured` for a node that never configured anything is the mislabel this
    /// replaced — `durable-off` says the floor is 1 because there is no durable plane, not
    /// because an operator asked for single-copy acks.
    #[must_use]
    pub fn write_floor_source(&self) -> &'static str {
        match self.floor {
            WriteFloor::Majority { .. } => "derived",
            WriteFloor::Fixed(_) => "configured",
            WriteFloor::DurableOff => "durable-off",
        }
    }

    /// Whether the resolved floor is DERIVED from known membership (the default) rather
    /// than an explicit operator integer. Reported on `/statusz` so a floor of 1 reads
    /// unambiguously: "this node is alone" versus "someone disabled the floor".
    #[must_use]
    pub fn write_floor_is_derived(&self) -> bool {
        matches!(self.floor, WriteFloor::Majority { .. })
    }

    /// Push the durable raft membership roster (issue #229): `known` are the
    /// members the node registry could name; `unknown` counts raft members with no
    /// known `NodeId` yet (e.g. a node crashed before this process ever observed
    /// it) — anything the reap gate must treat as "may still return".
    pub fn set_durable_roster(&mut self, known: BTreeSet<NodeId>, unknown: usize) {
        self.durable_roster = Some((known, unknown));
    }

    /// The durable membership roster, or `None` until first pushed.
    #[must_use]
    pub fn durable_roster(&self) -> Option<&(BTreeSet<NodeId>, usize)> {
        self.durable_roster.as_ref()
    }

    /// Apply an observed membership state. A non-`Dead` peer becomes eligible
    /// for placement (recording its peer-link `addr` for relocation); a `Dead`
    /// peer is removed. This node is always eligible and is never removed — it
    /// cannot hand off its own participation.
    pub fn observe(&mut self, id: &NodeId, state: MemberState, addr: &str, domain: Option<&str>) {
        if id == &self.local {
            return;
        }
        match state {
            MemberState::Dead => {
                self.eligible.remove(id);
                self.addrs.remove(id);
                self.domains.remove(id);
            }
            MemberState::Alive | MemberState::Suspect => {
                self.eligible.insert(id.clone());
                if !addr.is_empty() {
                    self.addrs.insert(id.clone(), addr.to_string());
                }
                // Learn the peer's failure-domain label; a membership event that never
                // carried one must not erase a label we already learned (ADR 0016 T5).
                if let Some(d) = domain {
                    if !d.is_empty() {
                        self.domains.insert(id.clone(), d.to_string());
                    }
                }
            }
        }
        // The high-water witness for the derived write floor (issue #239): once this node
        // has seen it belongs to an N-member cluster, burying those peers must not
        // un-know it. Only a quorum-committed roster shrink (or an explicit floor) does.
        self.peak_members = self.peak_members.max(self.eligible.len());
    }

    /// A snapshot of the placement-eligible membership for the `/statusz` body
    /// (ADR 0054): `(node id, peer-link addr if known, failure domain if known)`,
    /// deterministic order. This is the *placement view* (self plus non-dead
    /// peers) — per-member SWIM suspicion detail stays in the aggregate
    /// `members{state}` gauges.
    #[must_use]
    pub fn members_snapshot(&self) -> Vec<(NodeId, Option<String>, Option<String>)> {
        self.eligible
            .iter()
            .map(|id| {
                let addr = self.addrs.get(id).cloned();
                let domain = if *id == self.local {
                    self.local_domain.clone()
                } else {
                    self.domains.get(id).cloned()
                };
                (id.clone(), addr, domain)
            })
            .collect()
    }

    /// The current failure-domain topology (ADR 0016 T5): this node's own label plus
    /// every peer label learned from gossip. Feeds the lease-voter domain-balancing
    /// (ADR 0016 T4) with a *live*, self-assembling map — a node with no known label is
    /// simply absent (treated as its own singleton domain by the selector).
    #[must_use]
    pub fn domains(&self) -> BTreeMap<NodeId, String> {
        let mut out = self.domains.clone();
        if let Some(d) = &self.local_domain {
            out.insert(self.local.clone(), d.clone());
        }
        out
    }

    fn nodes(&self) -> Vec<NodeId> {
        self.eligible.iter().cloned().collect()
    }

    /// Replace the current lease voter set (ADR 0049). Called each reconcile tick
    /// by the durable driver with the committed voters (mapped back to `NodeId`).
    /// An empty set means "unknown" and ownership falls back to the eligible set.
    pub fn set_voters(&mut self, voters: BTreeSet<NodeId>) {
        self.voters = voters;
    }

    /// Replace the committed durable owner map — `group -> holder` from the replicated
    /// lease store, pushed each reconcile tick by the durable driver (2026-07-20
    /// post-mortem). This is the ACTUAL ownership the data path follows; a group absent
    /// from the map falls back to the desired HRW owner. Passing an empty map (e.g. a
    /// non-durable node) restores pure-HRW routing.
    pub fn set_lease_owners(&mut self, owners: BTreeMap<GroupId, NodeId>) {
        self.lease_owners = owners;
    }

    /// The committed durable owner of `group`, if a lease has been reported for it —
    /// exposed so ownership convergence can be observed (tests, diagnostics).
    #[must_use]
    pub fn committed_owner(&self, group: GroupId) -> Option<NodeId> {
        self.lease_owners.get(&group).cloned()
    }

    /// The current lease voter set as seen by this ring (ADR 0049) — empty until
    /// the durable driver has pushed it. Exposed so ownership convergence can be
    /// observed (tests, diagnostics).
    #[must_use]
    pub fn voter_ids(&self) -> Vec<NodeId> {
        self.voters.iter().cloned().collect()
    }

    /// The owner of `group` over a given candidate node list, restricted to the
    /// lease voter set (ADR 0049) when it is known. A learner cannot serve durable
    /// ownership, so owners are drawn from the voters ∩ `nodes`; if that
    /// intersection is empty (voters unknown, or momentarily none of them
    /// eligible) it falls back to `nodes` so the ring never has no owner.
    fn owner_over(&self, group: GroupId, nodes: &[NodeId]) -> NodeId {
        let voter_pool: Vec<NodeId> = if self.voters.is_empty() {
            Vec::new()
        } else {
            nodes
                .iter()
                .filter(|n| self.voters.contains(*n))
                .cloned()
                .collect()
        };
        let pool: &[NodeId] = if voter_pool.is_empty() {
            nodes
        } else {
            &voter_pool
        };
        hrw::owner(group_key(group).as_bytes(), pool)
            .cloned()
            .unwrap_or_else(|| self.local.clone())
    }

    /// The ordered replica set of `group` over a candidate node list: the
    /// voter-eligible owner (ADR 0049 §1) leads, followed by the HRW replica set
    /// over the full `nodes` — so ownership is bounded to voters while data
    /// replication still spans every eligible node (ADR 0021 §2). Owner-first,
    /// deduplicated, capped at `R`. The owner is always present, preserving the
    /// invariant that a group's owner holds its data.
    fn owner_led_replica_set(
        &self,
        group: GroupId,
        nodes: &[NodeId],
        owner: NodeId,
    ) -> Vec<NodeId> {
        let mut set = hrw::replica_set(group_key(group).as_bytes(), nodes, self.replicas);
        set.retain(|n| n != &owner);
        set.insert(0, owner);
        set.truncate(self.replicas.max(1));
        set
    }

    /// The **desired** HRW owner of a placement `group` (voter-restricted, ADR 0049) —
    /// the topology the lease assigner drives ownership toward. This is the assigner's
    /// input ONLY: the data path must resolve ownership through the committed lease
    /// ([`group_owner`](Self::group_owner)), or a transient HRW/lease disagreement
    /// splits routing from the commit gate into a permanent `NotOwner` (2026-07-20
    /// post-mortem).
    #[must_use]
    pub fn hrw_owner(&self, group: GroupId) -> NodeId {
        self.owner_over(group, &self.nodes())
    }

    /// The **actual** owner of a placement `group`: the holder of its committed lease,
    /// falling back to the desired HRW owner when no lease is assigned yet (bootstrap /
    /// non-durable). The data path routes and gates durable ownership here, so it always
    /// agrees with the lease the commit is fenced against. There is always an owner (this
    /// node is always eligible; the HRW fallback never has an empty ring).
    ///
    /// **Liveness rule:** the committed lease is honored only while its holder is still
    /// an eligible (non-`Dead`) member. A lease held by a corpse falls back to HRW at the
    /// same tick SWIM declares the death — the data path never routes to a dead node,
    /// and (crucially) the group's replica set settles together with membership,
    /// preserving the catch-up sweep's arming invariant (ADR 0043 P1: the sweep arms on
    /// membership change; without this filter the dead node's groups change replica set
    /// *again* when their leases migrate ticks later — after the sweep already ran —
    /// leaving them unstamped and their takeover recovery permanently `NoQuorum`). A
    /// holder *falsely* declared dead degrades to the transient fail-closed ring/lease
    /// split (`NotOwner`, retried) until the assigner reconciles; the permanent-split
    /// hazard this overlay fixes needs a *live* skewed holder, which the filter
    /// deliberately leaves lease-first.
    #[must_use]
    pub fn group_owner(&self, group: GroupId) -> NodeId {
        self.lease_owners
            .get(&group)
            .filter(|holder| self.eligible.contains(*holder))
            .cloned()
            .unwrap_or_else(|| self.hrw_owner(group))
    }

    /// The ordered replica set of a placement `group` — the **committed** owner leads
    /// (so the lease holder always holds the group's data), followed by the HRW replica
    /// set, capped at `R` and at the current member count.
    #[must_use]
    pub fn group_replica_set(&self, group: GroupId) -> Vec<NodeId> {
        self.owner_led_replica_set(group, &self.nodes(), self.group_owner(group))
    }

    /// Whether this node holds placement `group`'s committed lease (its actual owner).
    #[must_use]
    pub fn owns_group(&self, group: GroupId) -> bool {
        self.group_owner(group) == self.local
    }

    /// The configured replication factor R — the placement width, [`DEFAULT_REPLICAS`]
    /// at assembly. (Independent of the lease-voter cap: ADR 0021 keeps replication
    /// width and the voter set separate.)
    #[must_use]
    pub fn desired_replicas(&self) -> usize {
        self.replicas
    }

    /// Replication health across every placement group (#167): the configured R and the
    /// **smallest** replica set any group currently has. When a set truncates below R
    /// because too few nodes are alive, a durable append commits on that group with a
    /// smaller quorum than the operator configured — silently, before this. The smallest
    /// set is the worst-case durability right now; `min_actual < desired` is the signal an
    /// operator needs but was never given.
    ///
    /// Pure over the current membership snapshot, so it is unit-testable without a cluster
    /// and cheap enough to poll from the reconcile loop.
    #[must_use]
    pub fn replication_health(&self) -> ReplicationHealth {
        let desired = self.replicas;
        let nodes = self.nodes();
        let min_actual = (0..NUM_GROUPS)
            .map(|g| {
                self.owner_led_replica_set(g, &nodes, self.group_owner(g))
                    .len()
            })
            .min()
            .unwrap_or(0);
        ReplicationHealth {
            desired,
            min_actual,
        }
    }

    /// The replica set `group` will have once `leaving` departs (ADR 0043 P3):
    /// the HRW selection over the current members minus that node. What the
    /// decommission drain hands each group's data to — computed BEFORE the leave,
    /// so the hand-off completes while the leaver still serves reads. HRW
    /// monotonicity means every current member of the set (other than the
    /// leaver) stays in it.
    #[must_use]
    pub fn group_replica_set_without(&self, group: GroupId, leaving: &NodeId) -> Vec<NodeId> {
        let nodes: Vec<NodeId> = self
            .eligible
            .iter()
            .filter(|n| *n != leaving)
            .cloned()
            .collect();
        // Owner-led by the DESIRED (HRW) post-leave owner (ADR 0049): the drain computes
        // where each group's data WILL go once `leaving` departs — the topology the
        // assigner then makes the committed lease — so it leads with the HRW owner over
        // the post-leave members, not a lease that still names the departing node.
        let owner = self.owner_over(group, &nodes);
        self.owner_led_replica_set(group, &nodes, owner)
    }

    /// The owner node for `client` — the owner of its placement group.
    #[must_use]
    pub fn owner(&self, client: &str) -> NodeId {
        self.group_owner(group_of(client))
    }

    /// The ordered replica set for `client` (owner first) — its group's replica set.
    #[must_use]
    pub fn replica_set(&self, client: &str) -> Vec<NodeId> {
        self.group_replica_set(group_of(client))
    }

    /// Whether this node owns `client` (i.e. owns its group).
    #[must_use]
    pub fn owns(&self, client: &str) -> bool {
        self.owner(client) == self.local
    }

    /// Where to relocate `client`'s session: `Some((owner, peer_addr))` when the
    /// owner is another node whose address is known, `None` when this node is the
    /// owner (no relocation) or the owner's address is not yet learned (serve
    /// locally — ADR 0005 degrade-don't-refuse).
    #[must_use]
    pub fn owner_route(&self, client: &str) -> Option<(NodeId, String)> {
        let owner = self.owner(client);
        if owner == self.local {
            return None;
        }
        self.addrs.get(&owner).map(|addr| (owner, addr.clone()))
    }

    /// Whether this node is in `client`'s replica set (owner or a failover
    /// replica).
    #[must_use]
    pub fn is_replica(&self, client: &str) -> bool {
        self.replica_set(client).iter().any(|n| n == &self.local)
    }

    /// The number of nodes currently eligible for placement (always ≥ 1).
    #[must_use]
    pub fn member_count(&self) -> usize {
        self.eligible.len()
    }

    /// The current eligible member set (this node plus non-`Dead` peers), in
    /// deterministic order — e.g. for the lease group to track desired voters.
    #[must_use]
    pub fn members(&self) -> Vec<NodeId> {
        self.eligible.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Placement, DEFAULT_REPLICAS};
    use crate::swim::MemberState;
    use crate::NodeId;

    fn node(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    /// Build a ring for `local` that has observed each of `peers` as Alive
    /// (each with a synthetic `<peer>:7000` peer-link address).
    fn ring(local: &str, peers: &[&str]) -> Placement {
        let mut p = Placement::new(node(local), DEFAULT_REPLICAS);
        for peer in peers {
            p.observe(
                &node(peer),
                MemberState::Alive,
                &format!("{peer}:7000"),
                None,
            );
        }
        p
    }

    // --- 2026-07-20 post-mortem: the data path follows the committed lease ---

    /// The committed lease (pushed via `set_lease_owners`) is the ACTUAL owner the data
    /// path resolves — it overrides the desired HRW ring in both directions (granting and
    /// revoking local ownership), and the committed owner leads its replica set so the
    /// lease holder always holds the group's data. The assigner's `hrw_owner` view is
    /// untouched, so reconcile keeps driving the lease toward HRW instead of freezing on
    /// the value it just read back.
    #[test]
    fn the_committed_lease_overrides_the_hrw_ring_on_the_data_path() {
        use super::NUM_GROUPS;
        use std::collections::BTreeMap;
        let mut p = ring("a", &["b", "c"]);
        // A group the HRW ring assigns to a PEER, and one it assigns to us.
        let g_peer = (0..NUM_GROUPS)
            .find(|g| p.hrw_owner(*g) == node("b"))
            .unwrap();
        let g_self = (0..NUM_GROUPS)
            .find(|g| p.hrw_owner(*g) == node("a"))
            .unwrap();

        // The committed lease says the OPPOSITE of HRW for each.
        let mut leases = BTreeMap::new();
        leases.insert(g_peer, node("a"));
        leases.insert(g_self, node("c"));
        p.set_lease_owners(leases);

        // Data path follows the committed lease — grant and revoke.
        assert_eq!(p.group_owner(g_peer), node("a"));
        assert!(
            p.owns_group(g_peer),
            "the committed lease grants us the group"
        );
        assert_eq!(p.group_owner(g_self), node("c"));
        assert!(
            !p.owns_group(g_self),
            "the committed lease moved our HRW group to c"
        );
        // The committed owner leads its replica set (holder holds the data).
        assert_eq!(p.group_replica_set(g_peer)[0], node("a"));
        assert_eq!(p.group_replica_set(g_self)[0], node("c"));

        // The assigner's desired-state view is unchanged (still HRW).
        assert_eq!(p.hrw_owner(g_peer), node("b"));
        assert_eq!(p.hrw_owner(g_self), node("a"));

        // A group with no committed lease still falls back to HRW.
        let g_unassigned = (0..NUM_GROUPS)
            .find(|g| *g != g_peer && *g != g_self)
            .unwrap();
        assert_eq!(p.group_owner(g_unassigned), p.hrw_owner(g_unassigned));
        assert_eq!(p.committed_owner(g_unassigned), None);
    }

    /// The liveness rule: a committed lease held by a node SWIM has declared **Dead** is
    /// not honored — the group falls back to HRW (over the survivors) at the same tick
    /// the death lands in placement, so the data path never routes to a corpse and the
    /// replica set settles together with membership (the catch-up sweep's arming
    /// invariant, ADR 0043 P1 — without this, a dead owner's groups change replica set
    /// again when their leases migrate ticks later, after the sweep already ran, leaving
    /// takeover recovery permanently `NoQuorum`). A merely-Suspect holder stays
    /// lease-first: the overlay's whole point is a *live* holder the gossip view has
    /// momentarily skewed away from.
    #[test]
    fn a_dead_holders_lease_falls_back_to_hrw_a_suspect_holders_does_not() {
        use super::NUM_GROUPS;
        use std::collections::BTreeMap;
        let mut p = ring("a", &["b", "c"]);
        // A group HRW-owned by us, lease-held by b.
        let g = (0..NUM_GROUPS)
            .find(|g| p.hrw_owner(*g) == node("a"))
            .unwrap();
        let mut leases = BTreeMap::new();
        leases.insert(g, node("b"));
        p.set_lease_owners(leases);
        assert_eq!(p.group_owner(g), node("b"), "live holder: lease-first");

        // Suspect is NOT dead — the lease still rules.
        p.observe(&node("b"), MemberState::Suspect, "b:7000", None);
        assert_eq!(p.group_owner(g), node("b"), "suspect holder: lease-first");

        // Dead: the lease is a corpse's — fall back to HRW over the survivors, and the
        // replica set is led by the fallback owner (no dead node in it).
        p.observe(&node("b"), MemberState::Dead, "", None);
        assert_eq!(
            p.group_owner(g),
            p.hrw_owner(g),
            "dead holder: HRW fallback"
        );
        assert_ne!(p.group_owner(g), node("b"));
        assert!(
            !p.group_replica_set(g).contains(&node("b")),
            "a dead holder must not appear in the replica set"
        );
        // The raw committed record is still observable (diagnostics), just not honored.
        assert_eq!(p.committed_owner(g), Some(node("b")));

        // The holder rejoining (Alive) restores lease-first ownership.
        p.observe(&node("b"), MemberState::Alive, "b:7000", None);
        assert_eq!(p.group_owner(g), node("b"), "rejoined holder: lease-first");
    }

    /// Until the durable driver reports a committed lease, the data path is exactly the
    /// HRW ring — so every existing (non-durable / pre-lease) behaviour is preserved.
    #[test]
    fn an_empty_lease_map_leaves_the_hrw_ring_unchanged() {
        use super::NUM_GROUPS;
        let p = ring("a", &["b", "c"]);
        for g in 0..NUM_GROUPS {
            assert_eq!(p.group_owner(g), p.hrw_owner(g));
            assert_eq!(p.owns_group(g), p.hrw_owner(g) == node("a"));
            assert_eq!(p.committed_owner(g), None);
        }
    }

    // --- ADR 0049: durable ownership restricted to lease voters ---

    /// Every group's owner is a voter, and no session id maps to a learner owner —
    /// the invariant that closes the placement × voter-cap availability bug.
    #[test]
    fn voter_restricted_owner_is_always_a_voter() {
        use super::NUM_GROUPS;
        use std::collections::BTreeSet;
        // 7-node cluster, voter_cap 5: b..f are voters, g/h are permanent learners.
        let mut p = ring("a", &["b", "c", "d", "e", "f", "g"]);
        p.observe(&node("h"), MemberState::Alive, "h:7000", None);
        assert_eq!(p.member_count(), 8);
        let voters: BTreeSet<NodeId> = ["a", "b", "c", "d", "e"].iter().map(|s| node(s)).collect();
        p.set_voters(voters.clone());

        for g in 0..NUM_GROUPS {
            let owner = p.group_owner(g);
            assert!(
                voters.contains(&owner),
                "group {g} owner {owner:?} is not a voter"
            );
            // The owner always holds the group's data (owner ∈ replica set).
            assert!(
                p.group_replica_set(g).contains(&owner),
                "group {g} replica set missing its owner"
            );
        }
    }

    /// Ownership is voter-bounded, but data replication still spans learners —
    /// ADR 0021 §2's decoupling of replication from the voter cap is preserved.
    #[test]
    fn replicas_still_span_learners() {
        use super::NUM_GROUPS;
        use std::collections::BTreeSet;
        let mut p = ring("a", &["b", "c", "d", "e", "f", "g"]);
        let voters: BTreeSet<NodeId> = ["a", "b", "c"].iter().map(|s| node(s)).collect();
        p.set_voters(voters);
        let learners = [node("d"), node("e"), node("f"), node("g")];
        // At least one group replicates onto a learner (data domain > voter set).
        let hits_learner =
            (0..NUM_GROUPS).any(|g| p.group_replica_set(g).iter().any(|n| learners.contains(n)));
        assert!(
            hits_learner,
            "no group replicates to a learner — spread collapsed to voters"
        );
    }

    /// With no voter set known (bootstrap / non-durable), ownership falls back to
    /// the full eligible set — identical to pre-ADR-0049 behaviour.
    #[test]
    fn empty_voters_falls_back_to_eligible() {
        use super::NUM_GROUPS;
        let p = ring("a", &["b", "c", "d"]);
        // No set_voters call → voters empty → owner over all eligible.
        for g in 0..NUM_GROUPS {
            let owner = p.group_owner(g);
            assert!(p.members().contains(&owner));
            assert!(p.group_replica_set(g).contains(&owner));
        }
    }

    #[test]
    fn alone_this_node_owns_everything() {
        let p = Placement::new(node("a"), DEFAULT_REPLICAS);
        assert_eq!(p.member_count(), 1);
        for c in ["x", "y", "session-42"] {
            assert_eq!(p.owner(c), node("a"));
            assert!(p.owns(c));
            assert_eq!(p.replica_set(c), vec![node("a")]);
        }
    }

    #[test]
    fn alive_and_suspect_are_eligible_dead_is_removed() {
        let mut p = ring("a", &["b", "c"]);
        assert_eq!(p.member_count(), 3);

        // Suspect keeps the node in the ring (no churn on a transient blip).
        p.observe(&node("b"), MemberState::Suspect, "b:7000", None);
        assert_eq!(p.member_count(), 3);

        // Dead removes it.
        p.observe(&node("c"), MemberState::Dead, "", None);
        assert_eq!(p.member_count(), 2);

        // A node first seen as Suspect is still a member.
        p.observe(&node("d"), MemberState::Suspect, "d:7000", None);
        assert_eq!(p.member_count(), 3);
    }

    #[test]
    fn this_node_is_never_removed() {
        let mut p = ring("a", &["b"]);
        // Even a (spurious) Dead about ourselves must not drop us.
        p.observe(&node("a"), MemberState::Dead, "", None);
        assert_eq!(p.member_count(), 2);
        // We can still own keys.
        assert!(["x", "y", "z", "w"].iter().any(|c| p.owns(c)));
    }

    #[test]
    fn owner_route_points_at_a_remote_owner_and_is_none_when_local() {
        let p = ring("a", &["b", "c", "d", "e"]);
        let mut remote = 0;
        for i in 0..200 {
            let c = format!("client-{i}");
            match p.owner_route(&c) {
                None => {
                    // No route iff this node is the owner.
                    assert!(p.owns(&c), "no route for {c} but it is not local-owned");
                }
                Some((owner, addr)) => {
                    assert_ne!(owner, node("a"));
                    assert_eq!(owner, p.owner(&c));
                    assert_eq!(addr, format!("{}:7000", owner.0));
                    remote += 1;
                }
            }
        }
        assert!(remote > 0, "some sessions should route to a remote owner");
    }

    #[test]
    fn owner_route_is_none_until_the_owner_address_is_known() {
        // A peer eligible for placement but with no address yet cannot be a relay
        // target — serve locally rather than guess.
        let mut p = Placement::new(node("a"), DEFAULT_REPLICAS);
        p.observe(&node("b"), MemberState::Alive, "", None); // eligible, address unknown
        for i in 0..200 {
            let c = format!("client-{i}");
            if p.owner(&c) == node("b") {
                assert_eq!(
                    p.owner_route(&c),
                    None,
                    "no address → no route → serve local"
                );
            }
        }
    }

    #[test]
    fn replica_set_shrinks_gracefully_below_r() {
        let p = ring("a", &["b"]); // 2 members, R = 3
        let rs = p.replica_set("session-x");
        assert_eq!(rs.len(), 2, "replica set capped at the member count");
        assert_eq!(rs[0], p.owner("session-x"), "owner leads the replica set");
        // R is honored once enough members exist.
        let p = ring("a", &["b", "c", "d", "e"]); // 5 members
        assert_eq!(p.replica_set("session-x").len(), 3);
    }

    #[test]
    fn owns_and_is_replica_agree_with_the_ring() {
        let p = ring("a", &["b", "c", "d", "e"]);
        for i in 0..200 {
            let c = format!("client-{i}");
            let rs = p.replica_set(&c);
            assert_eq!(p.owns(&c), rs.first() == Some(&node("a")));
            assert_eq!(p.is_replica(&c), rs.contains(&node("a")));
            // The owner is always the head of the replica set.
            assert_eq!(p.owner(&c), rs[0]);
        }
    }

    #[test]
    fn a_dead_node_only_moves_the_keys_it_owned() {
        let before = ring("a", &["b", "c", "d"]); // 4 members
        let mut after = before.clone();
        after.observe(&node("d"), MemberState::Dead, "", None); // 3 members

        let mut moved = 0;
        let mut moved_were_ds = 0;
        let total = 2_000;
        for i in 0..total {
            let c = format!("client-{i}");
            let o0 = before.owner(&c);
            let o1 = after.owner(&c);
            if o0 != o1 {
                moved += 1;
                // The only keys that may move are those d owned.
                assert_eq!(o0, node("d"), "a non-owned key was reassigned");
                if o0 == node("d") {
                    moved_were_ds += 1;
                }
            }
        }
        assert_eq!(moved, moved_were_ds);
        assert!(moved > 0, "removing a node should move its keys");
        // d held ~1/4 of keys; nothing else should have moved.
        assert!(
            moved < total / 2,
            "far too many keys moved: {moved}/{total}"
        );
    }

    #[test]
    fn a_joining_node_moves_only_a_minority() {
        let before = ring("a", &["b", "c", "d"]);
        let mut after = before.clone();
        after.observe(&node("e"), MemberState::Alive, "e:7000", None);

        let total = 2_000;
        let moved = (0..total)
            .filter(|i| {
                let c = format!("client-{i}");
                before.owner(&c) != after.owner(&c)
            })
            .count();
        // Ideal is ~1/5 (the new node's share); assert well under half — the
        // rendezvous property the durability design relies on.
        assert!(
            moved < total / 3,
            "too many keys moved on join: {moved}/{total}"
        );
    }

    /// `group_of` is a deterministic hash into `[0, NUM_GROUPS)`.
    #[test]
    fn group_of_is_deterministic_and_in_range() {
        use super::{group_of, NUM_GROUPS};
        for i in 0..1_000 {
            let c = format!("client-{i}");
            let g = group_of(&c);
            assert!(g < NUM_GROUPS);
            assert_eq!(g, group_of(&c), "deterministic");
        }
        // The hash spreads across many groups (not all clients in one).
        let groups: std::collections::BTreeSet<u64> =
            (0..1_000).map(|i| group_of(&format!("c{i}"))).collect();
        assert!(groups.len() > 100, "clients spread across groups");
    }

    /// Every client in a group shares that group's owner and replica set — the
    /// locality the durable backend relies on (one lease/replica-set per group).
    #[test]
    fn clients_in_a_group_share_owner_and_replica_set() {
        use super::group_of;
        let p = ring("a", &["b", "c", "d", "e"]);
        // Bucket clients by group, then check each bucket agrees internally.
        let mut by_group: std::collections::BTreeMap<u64, Vec<String>> =
            std::collections::BTreeMap::new();
        for i in 0..2_000 {
            let c = format!("client-{i}");
            by_group.entry(group_of(&c)).or_default().push(c);
        }
        for (group, clients) in by_group.iter().filter(|(_, c)| c.len() >= 2) {
            let owner = p.group_owner(*group);
            let rs = p.group_replica_set(*group);
            for c in clients {
                assert_eq!(p.owner(c), owner, "client owner == its group owner");
                assert_eq!(p.replica_set(c), rs, "client replica set == its group's");
            }
        }
    }

    /// `owns_group` / `group_owner` / `group_replica_set` are mutually consistent,
    /// and a client's owner is the head of its group's replica set.
    #[test]
    fn group_queries_are_consistent() {
        use super::{group_of, NUM_GROUPS};
        let p = ring("a", &["b", "c", "d", "e"]);
        for group in 0..NUM_GROUPS {
            let rs = p.group_replica_set(group);
            assert_eq!(p.group_owner(group), rs[0], "owner leads the replica set");
            assert_eq!(p.owns_group(group), rs[0] == node("a"));
        }
        // A client routes through its group.
        let c = "client-123";
        assert_eq!(p.owner(c), p.group_owner(group_of(c)));
        assert_eq!(p.owns(c), p.owns_group(group_of(c)));
    }

    #[test]
    fn domains_reports_local_and_gossip_learned_labels() {
        let mut p =
            Placement::new(node("a"), DEFAULT_REPLICAS).with_local_domain(Some("z1".into()));
        p.observe(&node("b"), MemberState::Alive, "b:7000", Some("z2"));
        p.observe(&node("c"), MemberState::Suspect, "c:7000", Some("z2"));
        let d = p.domains();
        assert_eq!(d.get(&node("a")).map(String::as_str), Some("z1")); // own label
        assert_eq!(d.get(&node("b")).map(String::as_str), Some("z2"));
        assert_eq!(d.get(&node("c")).map(String::as_str), Some("z2")); // Suspect still counts
    }

    #[test]
    fn a_dead_peer_drops_its_domain() {
        let mut p = Placement::new(node("a"), DEFAULT_REPLICAS);
        p.observe(&node("b"), MemberState::Alive, "b:7000", Some("z2"));
        assert_eq!(p.domains().get(&node("b")).map(String::as_str), Some("z2"));
        p.observe(&node("b"), MemberState::Dead, "", None);
        assert!(!p.domains().contains_key(&node("b")));
    }

    #[test]
    fn an_unlabelled_observation_does_not_erase_a_known_domain() {
        let mut p = Placement::new(node("a"), DEFAULT_REPLICAS);
        p.observe(&node("b"), MemberState::Alive, "b:7000", Some("z2"));
        // A later membership event with no label (e.g. a relay that never learned it)
        // must not blank the label we already hold.
        p.observe(&node("b"), MemberState::Alive, "b:7000", None);
        assert_eq!(p.domains().get(&node("b")).map(String::as_str), Some("z2"));
    }

    #[test]
    fn an_unlabelled_node_is_absent_from_the_domain_map() {
        // No own label, no peer labels: the map is empty (each node its own singleton
        // domain, reproducing the pre-T5 id-ordered selection).
        let mut p = Placement::new(node("a"), DEFAULT_REPLICAS);
        p.observe(&node("b"), MemberState::Alive, "b:7000", None);
        assert!(p.domains().is_empty());
    }

    /// #167 — replication health reflects when too few nodes are alive to hold R copies.
    #[test]
    fn replication_health_reports_under_replication_below_r() {
        // A lone node with R=3: every group's replica set is just itself (1 < 3).
        let p = Placement::new(node("a"), 3);
        let h = p.replication_health();
        assert_eq!(h.desired, 3);
        assert_eq!(h.min_actual, 1, "a lone node holds one copy per group");
        assert!(
            h.is_under_replicated(),
            "1 of 3 configured copies is under-replicated"
        );

        // Three alive nodes: every group can hold the full R, so not under-replicated.
        let mut full = Placement::new(node("a"), 3);
        full.observe(&node("b"), MemberState::Alive, "b:7000", None);
        full.observe(&node("c"), MemberState::Alive, "c:7000", None);
        let h = full.replication_health();
        assert_eq!(h.min_actual, 3, "three nodes fill an R=3 set");
        assert!(!h.is_under_replicated());
    }

    // --- Issue #239: the DERIVED write floor (the shipped default) ---

    /// The default posture: 1 while this node has never known a peer (a fresh single
    /// node stays fully operational — the standing requirement), 2 the moment it knows
    /// it is part of a cluster of two or more. It never exceeds the majority of R, and
    /// the witness never follows liveness *down*: a node that saw two peers and then
    /// buried them still knows it belongs to a three-member cluster, which is exactly
    /// the state the floor exists to refuse.
    #[test]
    fn the_derived_write_floor_is_one_alone_and_two_once_the_cluster_is_known() {
        let mut p = Placement::new(node("a"), DEFAULT_REPLICAS)
            .with_write_floor(super::WriteFloor::Majority { declared: 1 });
        assert_eq!(p.min_replicas(), 1, "alone and never peered: no floor");
        assert!(p.write_floor_is_derived());

        p.observe(&node("b"), MemberState::Alive, "b:7000", None);
        assert_eq!(p.min_replicas(), 2, "a two-member cluster needs two copies");

        p.observe(&node("b"), MemberState::Dead, "b:7000", None);
        assert_eq!(
            p.min_replicas(),
            2,
            "the witness must not self-disarm when the peer it witnessed dies"
        );

        let mut three = Placement::new(node("a"), DEFAULT_REPLICAS)
            .with_write_floor(super::WriteFloor::Majority { declared: 1 });
        three.observe(&node("b"), MemberState::Alive, "b:7000", None);
        three.observe(&node("c"), MemberState::Alive, "c:7000", None);
        assert_eq!(
            three.min_replicas(),
            2,
            "majority of R=3 is 2 — the floor is never the full R"
        );
    }

    /// The quorum-committed durable roster (#229) is the authority: it arms the floor
    /// even on a node that boots alone and has observed nobody (the cold-restart hole a
    /// gossip-only latch leaves open), and — because shrinking it needs a lease quorum —
    /// a consented decommission down to one member disarms it with no operator edit.
    #[test]
    fn the_durable_roster_is_the_authority_for_the_derived_floor() {
        use std::collections::BTreeSet;
        let roster = |ids: &[&str]| ids.iter().map(|i| node(i)).collect::<BTreeSet<_>>();

        // Booted alone, gossip shows only self — but the roster says three.
        let mut p = Placement::new(node("a"), DEFAULT_REPLICAS)
            .with_write_floor(super::WriteFloor::Majority { declared: 1 });
        p.set_durable_roster(roster(&["a", "b", "c"]), 0);
        assert_eq!(
            p.min_replicas(),
            2,
            "the roster arms the restart-alone case"
        );

        // A deliberate shrink to one member: the roster is committed, so it overrides
        // the (higher) observed high-water mark.
        let mut shrunk = Placement::new(node("a"), DEFAULT_REPLICAS)
            .with_write_floor(super::WriteFloor::Majority { declared: 1 });
        shrunk.observe(&node("b"), MemberState::Alive, "b:7000", None);
        shrunk.observe(&node("c"), MemberState::Alive, "c:7000", None);
        assert_eq!(shrunk.min_replicas(), 2);
        shrunk.set_durable_roster(roster(&["a"]), 0);
        assert_eq!(
            shrunk.min_replicas(),
            1,
            "a quorum-committed shrink to one member disarms the floor by itself"
        );

        // An unmappable roster member still counts as a member.
        let mut unknown = Placement::new(node("a"), DEFAULT_REPLICAS)
            .with_write_floor(super::WriteFloor::Majority { declared: 1 });
        unknown.set_durable_roster(roster(&["a"]), 1);
        assert_eq!(unknown.min_replicas(), 2);
    }

    /// The operator-declared readiness member count is folded in as a `max()`, so the
    /// floor is armed on a bare-metal node's very first write — before any gossip
    /// observation or roster push. It is still capped at the majority of R.
    #[test]
    fn a_declared_member_count_arms_the_write_floor_before_gossip_or_the_roster() {
        let p = Placement::new(node("a"), DEFAULT_REPLICAS)
            .with_write_floor(super::WriteFloor::Majority { declared: 2 });
        assert_eq!(
            p.min_replicas(),
            2,
            "a node that declares it expects two members must not ack on one copy"
        );

        let wide = Placement::new(node("a"), DEFAULT_REPLICAS)
            .with_write_floor(super::WriteFloor::Majority { declared: 4 });
        assert_eq!(
            wide.min_replicas(),
            2,
            "the witness is capped at R, so the floor is never unsatisfiable"
        );
    }

    /// An explicit integer keeps #167's absolute meaning in BOTH directions: `1` is the
    /// documented opt-out even with a three-member roster, and `3` is a hard floor even
    /// on a lone node. The derived path must never shadow an explicit choice.
    #[test]
    fn an_explicit_floor_overrides_the_derived_one_in_both_directions() {
        use std::collections::BTreeSet;
        let mut off = Placement::new(node("a"), DEFAULT_REPLICAS).with_min_replicas(1);
        off.set_durable_roster(
            ["a", "b", "c"]
                .iter()
                .map(|i| node(i))
                .collect::<BTreeSet<_>>(),
            0,
        );
        off.observe(&node("b"), MemberState::Alive, "b:7000", None);
        assert_eq!(
            off.min_replicas(),
            1,
            "the opt-out survives a known cluster"
        );
        assert!(!off.write_floor_is_derived());

        let hard = Placement::new(node("a"), DEFAULT_REPLICAS).with_min_replicas(3);
        assert_eq!(hard.min_replicas(), 3, "an absolute floor stays absolute");
        assert!(!hard.write_floor_is_derived());
    }

    /// An EMPTY durable roster is not a membership signal — a raft group cannot have zero
    /// members, and the reconcile driver pushes exactly that while the group is still
    /// uninitialised or this node is unadmitted. It must neither disarm the floor nor
    /// throw away the gossip high-water witness the latch exists to preserve. Non-vacuous
    /// against the `.filter(|&members| members > 0)` in `min_replicas`: without it the
    /// witness collapses to `declared` (1) and both assertions below read 1.
    #[test]
    fn an_empty_durable_roster_is_not_a_membership_witness() {
        use std::collections::BTreeSet;

        // Two peers observed, then an uninitialised-group roster push.
        let mut p = Placement::new(node("a"), DEFAULT_REPLICAS)
            .with_write_floor(super::WriteFloor::Majority { declared: 1 });
        p.observe(&node("b"), MemberState::Alive, "b:7000", None);
        p.observe(&node("c"), MemberState::Alive, "c:7000", None);
        assert_eq!(p.min_replicas(), 2);
        p.set_durable_roster(BTreeSet::new(), 0);
        assert_eq!(
            p.min_replicas(),
            2,
            "an empty roster must not disarm the floor or discard the peak witness"
        );

        // And with nothing else to fall back on it is simply inert: the never-peered
        // node still resolves to 1 (the standing lone-node requirement), from
        // `peak_members`, not from a roster that claims a zero-member cluster.
        let mut alone = Placement::new(node("a"), DEFAULT_REPLICAS)
            .with_write_floor(super::WriteFloor::Majority { declared: 1 });
        alone.set_durable_roster(BTreeSet::new(), 0);
        assert_eq!(alone.min_replicas(), 1);
    }
}
