//! Shared-subscription registry (ADR 0010): named groups of sessions over which
//! a matching message is delivered to exactly **one** member, round-robin.
//!
//! This is a pure, runtime-agnostic structure. It owns group membership and the
//! round-robin cursor; it does **not** know which members are online — selecting
//! the actual recipient (online-preference, offline fallback) is the hub's job, so
//! `rotations` hands back each matching group's members in rotated priority order
//! and lets the caller pick.

use crate::{topic_matches, ClientId, QoS, TopicFilter};
use std::collections::HashMap;

/// Parse a `$share/{ShareName}/{filter}` shared-subscription filter into its
/// `(group, filter)` parts.
///
/// Returns `None` if `filter` is not a well-formed shared filter: it must start
/// with `$share/`, carry a non-empty `ShareName` containing no `/`, `+`, or `#`,
/// and a non-empty remaining topic filter.
#[must_use]
pub fn parse_shared(filter: &str) -> Option<(&str, &str)> {
    let rest = filter.strip_prefix("$share/")?;
    // The ShareName runs to the first '/', so it can never itself contain one.
    let (group, topic) = rest.split_once('/')?;
    if group.is_empty() || topic.is_empty() || group.contains(['+', '#']) {
        return None;
    }
    Some((group, topic))
}

/// Returns whether `filter` uses the shared-subscription `$share/` prefix,
/// regardless of whether the rest is well-formed.
#[must_use]
pub fn is_shared_filter(filter: &str) -> bool {
    filter.starts_with("$share/")
}

/// A single shared group: its members (with granted `QoS`) in insertion order,
/// plus the round-robin cursor pointing at the next member to prefer.
#[derive(Debug, Default)]
struct Group {
    members: Vec<(ClientId, QoS)>,
    cursor: usize,
}

/// Maps `(ShareName, filter)` to the group of sessions sharing it.
#[derive(Debug, Default)]
pub struct SharedSubscriptionTable {
    groups: HashMap<(String, TopicFilter), Group>,
}

impl SharedSubscriptionTable {
    /// Create an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `client` to the `(group, filter)` shared subscription at `max_qos`.
    /// Re-subscribing updates the granted `QoS` in place and keeps the member's
    /// position (so the rotation is stable).
    pub fn subscribe(&mut self, client: ClientId, group: &str, filter: &str, max_qos: QoS) {
        let g = self
            .groups
            .entry((group.to_string(), filter.to_string()))
            .or_default();
        if let Some(m) = g.members.iter_mut().find(|(c, _)| *c == client) {
            m.1 = max_qos;
        } else {
            g.members.push((client, max_qos));
        }
    }

    /// Remove `client` from one `(group, filter)` shared subscription.
    pub fn unsubscribe(&mut self, client: &ClientId, group: &str, filter: &str) {
        let key = (group.to_string(), filter.to_string());
        if let Some(g) = self.groups.get_mut(&key) {
            remove_member(g, client);
            if g.members.is_empty() {
                self.groups.remove(&key);
            }
        }
    }

    /// Remove `client` from every shared group (called on disconnect/discard).
    pub fn remove_client(&mut self, client: &ClientId) {
        self.groups.retain(|_, g| {
            remove_member(g, client);
            !g.members.is_empty()
        });
    }

    /// For every group whose `{filter}` matches `topic`, advance that group's
    /// round-robin cursor by one and return its members in rotated priority order
    /// (the newly-selected member first). The caller picks the first reachable one.
    ///
    /// Each returned `Vec` is one group; the outer order is unspecified.
    pub fn rotations(&mut self, topic: &str) -> Vec<Vec<(ClientId, QoS)>> {
        let mut out = Vec::new();
        for ((_, filter), g) in &mut self.groups {
            if !topic_matches(filter, topic) {
                continue;
            }
            let n = g.members.len();
            debug_assert!(n > 0, "empty groups are pruned on unsubscribe/remove");
            let start = g.cursor % n;
            // Advance for the next match so successive deliveries rotate.
            g.cursor = (start + 1) % n;
            let rotated = g
                .members
                .iter()
                .cycle()
                .skip(start)
                .take(n)
                .cloned()
                .collect();
            out.push(rotated);
        }
        out
    }

    /// The distinct underlying `{filter}` parts across all groups, for the cluster
    /// interest snapshot — peers route matching publishes to us by topic filter.
    #[must_use]
    pub fn filters(&self) -> Vec<TopicFilter> {
        let mut seen: Vec<TopicFilter> = Vec::new();
        for (_, filter) in self.groups.keys() {
            if !seen.contains(filter) {
                seen.push(filter.clone());
            }
        }
        seen
    }

    /// Number of distinct shared groups currently registered.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

/// Remove a member from a group, keeping the cursor pointing at the same logical
/// position so the rotation does not skip or repeat after a departure.
fn remove_member(g: &mut Group, client: &ClientId) {
    if let Some(idx) = g.members.iter().position(|(c, _)| c == client) {
        g.members.remove(idx);
        if g.members.is_empty() {
            g.cursor = 0;
        } else {
            if idx < g.cursor {
                g.cursor -= 1;
            }
            g.cursor %= g.members.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{is_shared_filter, parse_shared, SharedSubscriptionTable};
    use crate::{ClientId, QoS};

    fn cid(s: &str) -> ClientId {
        ClientId(s.to_string())
    }

    fn picks(t: &mut SharedSubscriptionTable, topic: &str) -> Vec<String> {
        t.rotations(topic)
            .into_iter()
            .map(|g| g[0].0 .0.clone())
            .collect()
    }

    #[test]
    fn parse_accepts_wellformed_and_rejects_malformed() {
        assert_eq!(parse_shared("$share/g/a/b"), Some(("g", "a/b")));
        assert_eq!(
            parse_shared("$share/g/sensors/+/t"),
            Some(("g", "sensors/+/t"))
        );
        assert_eq!(parse_shared("$share/g/#"), Some(("g", "#")));
        // Not shared at all.
        assert_eq!(parse_shared("a/b"), None);
        assert_eq!(parse_shared("$SYS/x"), None);
        // Malformed shared filters.
        assert_eq!(parse_shared("$share/g"), None, "no filter part");
        assert_eq!(parse_shared("$share/g/"), None, "empty filter");
        assert_eq!(parse_shared("$share//f"), None, "empty group");
        assert_eq!(parse_shared("$share/g+/f"), None, "wildcard in group");
        assert_eq!(parse_shared("$share/g#/f"), None, "wildcard in group");
        assert!(is_shared_filter("$share/g/f") && !is_shared_filter("g/f"));
    }

    #[test]
    fn delivers_to_one_member_round_robin() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t/+", QoS::AtMostOnce);
        t.subscribe(cid("b"), "grp", "t/+", QoS::AtMostOnce);
        t.subscribe(cid("c"), "grp", "t/+", QoS::AtMostOnce);
        assert_eq!(t.group_count(), 1);

        // One group matches, so each call yields exactly one rotation, and the
        // preferred member cycles a -> b -> c -> a.
        assert_eq!(picks(&mut t, "t/x"), vec!["a"]);
        assert_eq!(picks(&mut t, "t/x"), vec!["b"]);
        assert_eq!(picks(&mut t, "t/x"), vec!["c"]);
        assert_eq!(picks(&mut t, "t/x"), vec!["a"]);
    }

    #[test]
    fn rotation_lists_all_members_for_offline_fallback() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t", QoS::AtLeastOnce);
        t.subscribe(cid("b"), "grp", "t", QoS::AtLeastOnce);
        let rot = t.rotations("t");
        assert_eq!(rot.len(), 1);
        // The full membership is returned (preferred first) so the hub can fall
        // back past an offline member; QoS rides along.
        assert_eq!(rot[0].len(), 2);
        assert_eq!(rot[0][0], (cid("a"), QoS::AtLeastOnce));
        assert_eq!(rot[0][1], (cid("b"), QoS::AtLeastOnce));
    }

    #[test]
    fn distinct_groups_each_get_one_delivery() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g1", "t", QoS::AtMostOnce);
        t.subscribe(cid("b"), "g2", "t", QoS::AtMostOnce);
        let mut chosen = picks(&mut t, "t");
        chosen.sort();
        assert_eq!(chosen, vec!["a", "b"], "one per group");
    }

    #[test]
    fn non_matching_topic_yields_nothing() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t/+", QoS::AtMostOnce);
        assert!(t.rotations("other").is_empty());
    }

    #[test]
    fn unsubscribe_and_remove_client_prune_groups() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t", QoS::AtMostOnce);
        t.subscribe(cid("b"), "grp", "t", QoS::AtMostOnce);
        t.unsubscribe(&cid("a"), "grp", "t");
        assert_eq!(picks(&mut t, "t"), vec!["b"]);
        t.remove_client(&cid("b"));
        assert_eq!(t.group_count(), 0);
        assert!(t.rotations("t").is_empty());
    }

    /// Removing the member the cursor points at (or one before it) must not make
    /// the rotation skip the survivor.
    #[test]
    fn removal_keeps_rotation_stable() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g", "t", QoS::AtMostOnce);
        t.subscribe(cid("b"), "g", "t", QoS::AtMostOnce);
        t.subscribe(cid("c"), "g", "t", QoS::AtMostOnce);
        assert_eq!(picks(&mut t, "t"), vec!["a"]); // cursor now at b
        t.unsubscribe(&cid("a"), "g", "t"); // remove before cursor
                                            // b is still next, then c, then b again.
        assert_eq!(picks(&mut t, "t"), vec!["b"]);
        assert_eq!(picks(&mut t, "t"), vec!["c"]);
        assert_eq!(picks(&mut t, "t"), vec!["b"]);
    }

    #[test]
    fn resubscribe_updates_qos_in_place() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g", "t", QoS::AtMostOnce);
        t.subscribe(cid("a"), "g", "t", QoS::ExactlyOnce);
        let rot = t.rotations("t");
        assert_eq!(rot[0].len(), 1, "still one member");
        assert_eq!(rot[0][0].1, QoS::ExactlyOnce, "QoS updated");
    }

    #[test]
    fn filters_are_distinct_underlying_topics() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g1", "t/+", QoS::AtMostOnce);
        t.subscribe(cid("b"), "g2", "t/+", QoS::AtMostOnce);
        t.subscribe(cid("c"), "g1", "other", QoS::AtMostOnce);
        let mut f = t.filters();
        f.sort();
        assert_eq!(f, vec!["other".to_string(), "t/+".to_string()]);
    }
}
