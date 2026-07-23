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
    #[must_use]
    pub fn group_owner(&self, group: GroupId) -> NodeId {
        self.lease_owners
            .get(&group)
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
}
