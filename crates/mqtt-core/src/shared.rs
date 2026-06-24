//! Shared-subscription registry (ADR 0010): named groups of sessions over which a
//! matching message is delivered to exactly **one** member.
//!
//! This is a pure, runtime-agnostic structure that owns only this node's group
//! membership. It does **not** select the recipient or hold a round-robin cursor:
//! cluster-wide selection (combining this membership with peers', preferring online
//! members, advancing the cursor) is the hub's job (ADR 0015), so the table just
//! reports matching groups and their members via [`SharedSubscriptionTable::matching`].

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

/// One shared group: its `(ShareName, filter)` and members (with granted `QoS`) in
/// insertion order. Round-robin selection lives in the hub, which combines this
/// local membership with members gossiped from peers (ADR 0015).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedGroup {
    /// The share name.
    pub group: String,
    /// The underlying topic filter.
    pub filter: TopicFilter,
    /// Members and their granted `QoS`, in insertion order.
    pub members: Vec<(ClientId, QoS)>,
}

/// Maps `(ShareName, filter)` to its ordered members.
#[derive(Debug, Default)]
pub struct SharedSubscriptionTable {
    groups: HashMap<(String, TopicFilter), Vec<(ClientId, QoS)>>,
}

impl SharedSubscriptionTable {
    /// Create an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `client` to the `(group, filter)` shared subscription at `max_qos`.
    /// Re-subscribing updates the granted `QoS` in place and keeps the member's
    /// insertion position.
    pub fn subscribe(&mut self, client: ClientId, group: &str, filter: &str, max_qos: QoS) {
        let members = self
            .groups
            .entry((group.to_string(), filter.to_string()))
            .or_default();
        if let Some(m) = members.iter_mut().find(|(c, _)| *c == client) {
            m.1 = max_qos;
        } else {
            members.push((client, max_qos));
        }
    }

    /// Remove `client` from one `(group, filter)` shared subscription.
    pub fn unsubscribe(&mut self, client: &ClientId, group: &str, filter: &str) {
        let key = (group.to_string(), filter.to_string());
        if let Some(members) = self.groups.get_mut(&key) {
            members.retain(|(c, _)| c != client);
            if members.is_empty() {
                self.groups.remove(&key);
            }
        }
    }

    /// Remove `client` from every shared group (called on disconnect/discard).
    pub fn remove_client(&mut self, client: &ClientId) {
        self.groups.retain(|_, members| {
            members.retain(|(c, _)| c != client);
            !members.is_empty()
        });
    }

    /// Visit each group whose `{filter}` matches `topic`, **by reference** — the
    /// per-publish selection path (ADR 0010 T8). Unlike [`matching`](Self::matching) this
    /// allocates nothing and clones no member list: the caller (the hub's shared selector)
    /// borrows `(group, filter, members)` and copies only what it actually keeps. `f` is
    /// invoked once per matching group, in arbitrary order.
    pub fn for_each_matching<F>(&self, topic: &str, mut f: F)
    where
        F: FnMut(&str, &str, &[(ClientId, QoS)]),
    {
        for ((group, filter), members) in &self.groups {
            if topic_matches(filter, topic) {
                f(group, filter, members);
            }
        }
    }

    /// Every group whose `{filter}` matches `topic`, with its members (an owned snapshot).
    /// The hub merges these with peer members and selects one per group (ADR 0015). The
    /// per-publish hot path uses [`for_each_matching`](Self::for_each_matching) to avoid
    /// the per-group clone; this owned form remains for callers that need to retain it.
    #[must_use]
    pub fn matching(&self, topic: &str) -> Vec<SharedGroup> {
        let mut out = Vec::new();
        self.for_each_matching(topic, |group, filter, members| {
            out.push(SharedGroup {
                group: group.to_string(),
                filter: filter.to_string(),
                members: members.to_vec(),
            });
        });
        out
    }

    /// Every shared group with its members — the snapshot gossiped to peers so they
    /// know this node's shared membership (ADR 0015 §2).
    #[must_use]
    pub fn snapshot(&self) -> Vec<SharedGroup> {
        self.groups
            .iter()
            .map(|((group, filter), members)| SharedGroup {
                group: group.clone(),
                filter: filter.clone(),
                members: members.clone(),
            })
            .collect()
    }

    /// Number of distinct shared groups currently registered.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{is_shared_filter, parse_shared, SharedSubscriptionTable};
    use crate::{ClientId, QoS};

    fn cid(s: &str) -> ClientId {
        ClientId(s.to_string())
    }

    /// Member client ids of the single group matching `topic`, sorted.
    fn member_ids(t: &SharedSubscriptionTable, topic: &str) -> Vec<String> {
        let groups = t.matching(topic);
        assert!(groups.len() <= 1, "tests use one matching group");
        let mut ids: Vec<String> = groups
            .first()
            .map(|g| g.members.iter().map(|(c, _)| c.0.clone()).collect())
            .unwrap_or_default();
        ids.sort();
        ids
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
    fn matching_reports_group_members_in_order_with_qos() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t/+", QoS::AtLeastOnce);
        t.subscribe(cid("b"), "grp", "t/+", QoS::AtLeastOnce);
        assert_eq!(t.group_count(), 1);

        let groups = t.matching("t/x");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group, "grp");
        assert_eq!(groups[0].filter, "t/+");
        // Members are reported in insertion order, with granted QoS — the hub does
        // the round-robin selection over this list (ADR 0015).
        assert_eq!(groups[0].members[0], (cid("a"), QoS::AtLeastOnce));
        assert_eq!(groups[0].members[1], (cid("b"), QoS::AtLeastOnce));
    }

    #[test]
    fn for_each_matching_visits_the_same_groups_without_cloning() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t/+", QoS::AtLeastOnce);
        t.subscribe(cid("b"), "grp", "t/+", QoS::ExactlyOnce);
        t.subscribe(cid("c"), "other", "z/#", QoS::AtMostOnce); // does not match

        // The borrowing visitor yields exactly the matching group, by reference.
        let mut visits = 0;
        let mut seen_group = String::new();
        let mut seen_members: Vec<(ClientId, QoS)> = Vec::new();
        t.for_each_matching("t/x", |group, filter, members| {
            visits += 1;
            assert_eq!(filter, "t/+");
            seen_group = group.to_string();
            seen_members = members.to_vec();
        });
        assert_eq!(visits, 1, "only the matching group is visited");
        assert_eq!(seen_group, "grp");
        assert_eq!(
            seen_members,
            vec![(cid("a"), QoS::AtLeastOnce), (cid("b"), QoS::ExactlyOnce)],
            "members in insertion order with granted QoS"
        );

        // `matching` is the owned form of the same data.
        let owned = t.matching("t/x");
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].group, seen_group);
        assert_eq!(owned[0].members, seen_members);
    }

    #[test]
    fn distinct_groups_are_reported_separately() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g1", "t", QoS::AtMostOnce);
        t.subscribe(cid("b"), "g2", "t", QoS::AtMostOnce);
        let mut ids: Vec<String> = t
            .matching("t")
            .iter()
            .flat_map(|g| g.members.iter().map(|(c, _)| c.0.clone()))
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"], "both groups match");
        assert_eq!(t.matching("t").len(), 2);
    }

    #[test]
    fn non_matching_topic_yields_nothing() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t/+", QoS::AtMostOnce);
        assert!(t.matching("other").is_empty());
    }

    #[test]
    fn unsubscribe_and_remove_client_prune_groups() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t", QoS::AtMostOnce);
        t.subscribe(cid("b"), "grp", "t", QoS::AtMostOnce);
        t.unsubscribe(&cid("a"), "grp", "t");
        assert_eq!(member_ids(&t, "t"), vec!["b"]);
        t.remove_client(&cid("b"));
        assert_eq!(t.group_count(), 0);
        assert!(t.matching("t").is_empty());
    }

    #[test]
    fn resubscribe_updates_qos_in_place() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g", "t", QoS::AtMostOnce);
        t.subscribe(cid("a"), "g", "t", QoS::ExactlyOnce);
        let groups = t.matching("t");
        assert_eq!(groups[0].members.len(), 1, "still one member");
        assert_eq!(groups[0].members[0].1, QoS::ExactlyOnce, "QoS updated");
    }

    #[test]
    fn snapshot_lists_every_group_with_members() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g1", "t/+", QoS::AtMostOnce);
        t.subscribe(cid("b"), "g2", "other", QoS::AtLeastOnce);
        let mut snap = t.snapshot();
        snap.sort_by(|x, y| x.group.cmp(&y.group));
        assert_eq!(snap.len(), 2);
        assert_eq!(
            (snap[0].group.as_str(), snap[0].filter.as_str()),
            ("g1", "t/+")
        );
        assert_eq!(snap[1].members, vec![(cid("b"), QoS::AtLeastOnce)]);
    }
}
