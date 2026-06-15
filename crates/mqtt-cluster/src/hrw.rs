//! Rendezvous (Highest-Random-Weight) hashing for placement.
//!
//! For a key (e.g. a client id) and a set of nodes, HRW deterministically picks
//! the node — and an ordered replica set — with the highest weight. Its defining
//! property is **minimal disruption**: adding or removing a node only remaps the
//! keys that were (or become) owned by that node, leaving the rest untouched.
//! This is what lets session ownership rebalance cheaply as the cluster changes
//! (see ADR 0001).
//!
//! The weight function is a fixed FNV-1a hash rather than [`std::hash`], because
//! placement **must be identical on every node** regardless of Rust version or
//! per-process hasher seeds. A version-stable hash guarantees that.

use crate::NodeId;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Streaming FNV-1a over `bytes`, continuing from `seed`.
fn fnv1a(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = seed;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// `SplitMix64` finalizer: a strong avalanche mix. FNV-1a on its own has weak bit
/// diffusion, which skews HRW placement; mixing its output evens the distribution.
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A version-stable 64-bit hash of `bytes`, identical on every node and across
/// process restarts (FNV-1a + a `SplitMix64` avalanche finalize).
///
/// Used where a value must hash the *same* everywhere — e.g. deriving a consensus
/// node id from a cluster node id (`node_registry`) — which `std::hash` cannot
/// guarantee (its hasher is randomly seeded per process).
#[must_use]
pub fn stable_id(bytes: &[u8]) -> u64 {
    mix64(fnv1a(FNV_OFFSET, bytes))
}

/// The rendezvous weight of `node` for `key`. Higher wins.
fn weight(node: &NodeId, key: &[u8]) -> u64 {
    // Hash the node id, then continue into the key, so each (node, key) pair gets
    // an independent pseudo-random weight; finalize with a strong avalanche mix.
    let h = fnv1a(FNV_OFFSET, node.0.as_bytes());
    mix64(fnv1a(h, key))
}

/// The owner of `key`: the node with the highest weight.
///
/// Ties (astronomically unlikely with a 64-bit weight) break toward the larger
/// node id, so all nodes agree. Returns `None` if `nodes` is empty.
#[must_use]
pub fn owner<'a>(key: &[u8], nodes: &'a [NodeId]) -> Option<&'a NodeId> {
    nodes.iter().max_by(|a, b| {
        weight(a, key)
            .cmp(&weight(b, key))
            .then_with(|| a.0.cmp(&b.0))
    })
}

/// The ordered replica set for `key`: up to `r` nodes, highest weight first.
///
/// The first element is the [`owner`]; the remainder are the failover replicas in
/// preference order.
#[must_use]
pub fn replica_set(key: &[u8], nodes: &[NodeId], r: usize) -> Vec<NodeId> {
    let mut scored: Vec<(u64, &NodeId)> = nodes.iter().map(|n| (weight(n, key), n)).collect();
    // Sort by weight descending, breaking ties toward the larger node id (matching
    // `owner`'s tie-break) so the ordering is identical on every node.
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1 .0.cmp(&a.1 .0)));
    scored.into_iter().take(r).map(|(_, n)| n.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::{owner, replica_set};
    use crate::NodeId;

    fn nodes(ids: &[&str]) -> Vec<NodeId> {
        ids.iter().map(|s| NodeId((*s).to_string())).collect()
    }

    #[test]
    fn owner_is_deterministic() {
        let ns = nodes(&["a", "b", "c", "d"]);
        let first = owner(b"client-42", &ns).unwrap().clone();
        // Order of the node list must not change the result.
        let mut shuffled = ns.clone();
        shuffled.reverse();
        assert_eq!(owner(b"client-42", &shuffled).unwrap(), &first);
    }

    #[test]
    fn empty_has_no_owner() {
        assert!(owner(b"k", &[]).is_none());
    }

    #[test]
    fn replica_set_starts_with_owner_and_has_no_dups() {
        let ns = nodes(&["a", "b", "c", "d", "e"]);
        let rs = replica_set(b"session-x", &ns, 3);
        assert_eq!(rs.len(), 3);
        assert_eq!(&rs[0], owner(b"session-x", &ns).unwrap());
        let unique: std::collections::HashSet<_> = rs.iter().collect();
        assert_eq!(unique.len(), 3, "replica set must not repeat a node");
    }

    #[test]
    fn replica_set_caps_at_node_count() {
        let ns = nodes(&["a", "b"]);
        assert_eq!(replica_set(b"k", &ns, 5).len(), 2);
    }

    #[test]
    fn distribution_is_roughly_even() {
        let ns = nodes(&["a", "b", "c", "d"]);
        let mut counts = std::collections::HashMap::new();
        for i in 0..10_000 {
            let key = format!("client-{i}");
            let o = owner(key.as_bytes(), &ns).unwrap().clone();
            *counts.entry(o).or_insert(0u32) += 1;
        }
        // Each of 4 nodes should get roughly 25% (2500); with a strong mix the
        // spread is tight (binomial stddev ~43).
        for (_, c) in counts {
            assert!((2200..2800).contains(&c), "uneven distribution: {c}");
        }
    }

    #[test]
    fn adding_a_node_moves_only_a_minority_of_keys() {
        let before = nodes(&["a", "b", "c", "d"]);
        let after = nodes(&["a", "b", "c", "d", "e"]);
        let mut moved = 0;
        let total = 10_000;
        for i in 0..total {
            let key = format!("client-{i}");
            if owner(key.as_bytes(), &before) != owner(key.as_bytes(), &after) {
                moved += 1;
            }
        }
        // Ideal is ~1/5 (the share of the new node). Assert well under half — the
        // hallmark of rendezvous hashing vs. a naive modulo scheme.
        assert!(moved < total / 3, "too many keys moved: {moved}/{total}");
    }
}
