//! Shared-subscription registry (ADR 0010): named groups of sessions over which a
//! matching message is delivered to exactly **one** member.
//!
//! This is a pure, runtime-agnostic structure that owns only this node's group
//! membership. It does **not** select the recipient or hold a round-robin cursor:
//! cluster-wide selection (combining this membership with peers', preferring online
//! members, advancing the cursor) is the hub's job (ADR 0015), so the table just
//! reports matching groups and their members via [`SharedSubscriptionTable::matching`].

use crate::filter_index::FilterIndex;
use crate::{ClientId, FilterKey, QoS, TopicFilter};
use std::collections::hash_map::Entry;
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

/// One matching group as the per-publish path borrows it: `(share name, filter,
/// members)`, all tied to the table's lifetime — nothing cloned.
pub type MatchedGroup<'a> = (&'a str, &'a str, &'a GroupMembers);

/// Maps a topic filter to the share-named groups over it, each with its ordered
/// members.
///
/// **Filter-first, and indexed** (ADR 0077 T4 follow-up). It used to key on
/// `(ShareName, filter)` and test every group with `topic_matches` per publish;
/// with the filter outermost the derived [`FilterIndex`] answers "which filters
/// match this topic" in one walk of the TOPIC's levels, and the groups under a
/// matched filter are then reached directly — no scan, no allocation. This is
/// the same index issue #445 gave ordinary subscriptions and shared ones never
/// received.
/// One shared group's members, with the ONLINE ones held as a prefix.
///
/// Selecting a recipient used to be linear in group size: the hub built a
/// candidate per member on every publish, probing a hash map for each one's
/// liveness, in order to pick exactly one. That is invisible at the six members
/// a fan-in tenant uses and it is milliseconds per message at the tens of
/// thousands a broadcast fan-out group legitimately holds — and a `$share`
/// group is platform machinery that has to serve both.
///
/// The invariant is simply that `all[..online]` are the members currently
/// online. Selection is then an index into that prefix, constant time at any
/// size. A member changing state is one swap across the boundary; a member
/// leaving is one `swap_remove`. Nothing here is per-publish work.
#[derive(Debug, Clone, Default)]
pub struct GroupMembers {
    /// Every member. `all[..online]` are online; the rest are not.
    all: Vec<(ClientId, QoS)>,
    /// How many of the leading entries are online.
    online: usize,
}

impl std::ops::Deref for GroupMembers {
    type Target = [(ClientId, QoS)];
    /// Every member, online or not — what the gossip snapshot and the remote
    /// fallback path both want. Readers that do not care about liveness are
    /// unaffected by the partition.
    fn deref(&self) -> &Self::Target {
        &self.all
    }
}

impl GroupMembers {
    /// How many members are online and therefore immediately deliverable.
    #[must_use]
    pub fn online_count(&self) -> usize {
        self.online
    }

    /// The `start`-th online member, rotating within the online set — the
    /// per-publish selection, in constant time.
    ///
    /// Rotation is over the ONLINE members only, which is what keeps round-robin
    /// fair among the members that can actually take a message: an offline
    /// member does not consume a turn.
    #[must_use]
    pub fn select_online(&self, start: usize) -> Option<&(ClientId, QoS)> {
        if self.online == 0 {
            return None;
        }
        self.all.get(start % self.online)
    }

    /// Whether `client` is a member, and if so where it sits.
    fn position(&self, client: &ClientId) -> Option<usize> {
        self.all.iter().position(|(c, _)| c == client)
    }

    /// Move the member at `i` across the online boundary, keeping the partition.
    fn set_at(&mut self, i: usize, online: bool) {
        let is_online = i < self.online;
        if is_online == online {
            return;
        }
        if online {
            // Offline -> online: swap it to the first offline slot, which then
            // becomes the last online one.
            self.all.swap(i, self.online);
            self.online += 1;
        } else {
            // Online -> offline: swap it with the last online slot and shrink.
            self.online -= 1;
            self.all.swap(i, self.online);
        }
    }

    /// Add `client` at `max_qos`, or update the granted `QoS` if already a member.
    /// Re-subscribing never changes a member's liveness.
    fn insert(&mut self, client: ClientId, max_qos: QoS, online: bool) {
        if let Some(i) = self.position(&client) {
            self.all[i].1 = max_qos;
            self.set_at(i, online);
            return;
        }
        self.all.push((client, max_qos));
        if online {
            self.set_at(self.all.len() - 1, true);
        }
    }

    /// Drop `client`. Returns whether it was a member.
    fn remove(&mut self, client: &ClientId) -> bool {
        let Some(i) = self.position(client) else {
            return false;
        };
        // Leave the partition intact: take it offline first, so the hole is
        // always in the offline region and `swap_remove` cannot move an online
        // member across the boundary.
        self.set_at(i, false);
        let last = self.all.len() - 1;
        let i = self.position(client).unwrap_or(last);
        self.all.swap_remove(i);
        true
    }

    /// Mark `client` online or offline. Returns whether it was a member.
    fn set_online(&mut self, client: &ClientId, online: bool) -> bool {
        match self.position(client) {
            Some(i) => {
                self.set_at(i, online);
                true
            }
            None => false,
        }
    }

    /// Debug-only: the partition is exactly what it claims to be.
    #[cfg(debug_assertions)]
    fn assert_partitioned(&self) {
        debug_assert!(
            self.online <= self.all.len(),
            "online count {} exceeds {} members",
            self.online,
            self.all.len()
        );
    }
}

#[derive(Debug, Default)]
pub struct SharedSubscriptionTable {
    /// Source of truth: filter → share-name → members, online ones first.
    groups: HashMap<FilterKey, HashMap<String, GroupMembers>>,
    /// Which groups each client belongs to. Attaching or detaching flips one
    /// client's liveness in every group it is in, and without this that meant
    /// walking every group in the table; with it, it costs only the handful of
    /// groups that client actually joined. It also makes `remove_client` a
    /// targeted removal rather than a full scan.
    by_client: HashMap<ClientId, Vec<(FilterKey, String)>>,
    /// Derived match index over `groups`' keys. A filter enters when its first
    /// group appears and leaves when its last one goes, so the two cannot
    /// disagree about which filters exist.
    index: FilterIndex,
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
    pub fn subscribe(
        &mut self,
        client: ClientId,
        group: &str,
        filter: &str,
        max_qos: QoS,
        online: bool,
    ) {
        // Disjoint field borrows let the index be updated from inside the entry,
        // exactly as `SubscriptionTable::subscribe` does — the filter enters the
        // index precisely when its first group appears, so the two cannot
        // disagree about which filters exist. This is a per-SUBSCRIBE path, so
        // the one key allocation is not on any hot path.
        let index = &mut self.index;
        let by_group = match self.groups.entry(FilterKey::from(filter)) {
            Entry::Vacant(v) => {
                index.insert(v.key());
                v.insert(HashMap::new())
            }
            Entry::Occupied(o) => o.into_mut(),
        };
        let key = FilterKey::from(filter);
        let members = by_group.entry(group.to_string()).or_default();
        let fresh = members.position(&client).is_none();
        members.insert(client.clone(), max_qos, online);
        #[cfg(debug_assertions)]
        members.assert_partitioned();
        if fresh {
            self.by_client
                .entry(client)
                .or_default()
                .push((key, group.to_string()));
        }
    }

    /// Mark `client` online or offline in every group it belongs to — the
    /// attach/detach hook that keeps each group's online prefix true.
    ///
    /// Costs the number of groups this one client joined, not the size of any
    /// of them, which is what makes the per-publish selection constant time
    /// without making connection churn expensive.
    pub fn set_client_online(&mut self, client: &ClientId, online: bool) {
        let Some(memberships) = self.by_client.get(client) else {
            return;
        };
        for (filter, group) in memberships.clone() {
            if let Some(members) = self
                .groups
                .get_mut(&filter)
                .and_then(|by_group| by_group.get_mut(&group))
            {
                members.set_online(client, online);
                #[cfg(debug_assertions)]
                members.assert_partitioned();
            }
        }
    }

    /// Remove `client` from one `(group, filter)` shared subscription.
    pub fn unsubscribe(&mut self, client: &ClientId, group: &str, filter: &str) {
        let Some(by_group) = self.groups.get_mut(filter) else {
            return;
        };
        if let Some(members) = by_group.get_mut(group) {
            if members.remove(client) {
                if let Some(m) = self.by_client.get_mut(client) {
                    m.retain(|(f, g)| &**f != filter || g != group);
                    if m.is_empty() {
                        self.by_client.remove(client);
                    }
                }
            }
            #[cfg(debug_assertions)]
            members.assert_partitioned();
            if members.is_empty() {
                by_group.remove(group);
            }
        }
        if by_group.is_empty() {
            self.groups.remove(filter);
            self.index.remove(filter);
        }
    }

    /// Remove `client` from every shared group (called on disconnect/discard).
    pub fn remove_client(&mut self, client: &ClientId) {
        // Targeted: the reverse index names exactly the groups this client is
        // in, so a disconnect no longer walks every group in the table.
        let Some(memberships) = self.by_client.remove(client) else {
            return;
        };
        for (filter, group) in memberships {
            let Some(by_group) = self.groups.get_mut(&filter) else {
                continue;
            };
            if let Some(members) = by_group.get_mut(&group) {
                members.remove(client);
                #[cfg(debug_assertions)]
                members.assert_partitioned();
                if members.is_empty() {
                    by_group.remove(&group);
                }
            }
            if by_group.is_empty() {
                self.groups.remove(&filter);
                self.index.remove(&filter);
            }
        }
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
        self.index.for_each_matching(topic, |filter| {
            if let Some(by_group) = self.groups.get(filter) {
                for (group, members) in by_group {
                    f(group, filter, members);
                }
            }
        });
    }

    /// Iterator form of [`for_each_matching`](Self::for_each_matching) (issue #376):
    /// the borrowed `(group, filter, members)` tuples live as long as `&self`, so the
    /// per-publish selection path can hold them across its plan pass without a closure
    /// and without cloning a single member. Arbitrary order, like `for_each_matching`.
    ///
    /// Collects into a `Vec` because the index walk is callback-shaped; the vector
    /// holds borrows only — no member list is cloned, which is the cost #376 removed.
    pub fn matching_refs<'a>(
        &'a self,
        topic: &'a str,
    ) -> impl Iterator<Item = MatchedGroup<'a>> + 'a {
        let mut out: Vec<MatchedGroup<'a>> = Vec::new();
        // Vec::new() does not allocate, so an empty table costs nothing here.
        self.index.for_each_matching(topic, |filter| {
            if let Some(by_group) = self.groups.get(filter) {
                for (group, members) in by_group {
                    out.push((group.as_str(), &**filter, members));
                }
            }
        });
        out.into_iter()
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
            .flat_map(|(filter, by_group)| {
                by_group.iter().map(move |(group, members)| SharedGroup {
                    group: group.clone(),
                    // `String::from(&**filter)`, not `filter.to_string()`: the key
                    // is a `FilterKey` (`Arc<str>`) since this table became
                    // filter-first, and `Arc<str>` has no `ToString`
                    // specialization — `to_string` formats through
                    // `Formatter::pad` instead of copying the bytes. The sibling
                    // `SubscriptionTable::filters` documents the same trap for the
                    // same reason: this feeds the peer gossip (ADR 0015 §2) and
                    // runs whenever shared membership changes. It was a cheap
                    // `String` clone before the key changed type.
                    filter: String::from(&**filter),
                    members: members.to_vec(),
                })
            })
            .collect()
    }

    /// Number of distinct shared groups currently registered.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.groups.values().map(HashMap::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::{is_shared_filter, parse_shared, SharedSubscriptionTable};
    use crate::{ClientId, QoS};

    fn cid(s: &str) -> ClientId {
        ClientId(s.into())
    }

    /// Member client ids of the single group matching `topic`, sorted.
    fn member_ids(t: &SharedSubscriptionTable, topic: &str) -> Vec<String> {
        let groups = t.matching(topic);
        assert!(groups.len() <= 1, "tests use one matching group");
        let mut ids: Vec<String> = groups
            .first()
            .map(|g| g.members.iter().map(|(c, _)| c.0.to_string()).collect())
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
        t.subscribe(cid("a"), "grp", "t/+", QoS::AtLeastOnce, true);
        t.subscribe(cid("b"), "grp", "t/+", QoS::AtLeastOnce, true);
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
        t.subscribe(cid("a"), "grp", "t/+", QoS::AtLeastOnce, true);
        t.subscribe(cid("b"), "grp", "t/+", QoS::ExactlyOnce, true);
        t.subscribe(cid("c"), "other", "z/#", QoS::AtMostOnce, true); // does not match

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
        t.subscribe(cid("a"), "g1", "t", QoS::AtMostOnce, true);
        t.subscribe(cid("b"), "g2", "t", QoS::AtMostOnce, true);
        let mut ids: Vec<String> = t
            .matching("t")
            .iter()
            .flat_map(|g| g.members.iter().map(|(c, _)| c.0.to_string()))
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"], "both groups match");
        assert_eq!(t.matching("t").len(), 2);
    }

    #[test]
    fn non_matching_topic_yields_nothing() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t/+", QoS::AtMostOnce, true);
        assert!(t.matching("other").is_empty());
    }

    #[test]
    fn unsubscribe_and_remove_client_prune_groups() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "grp", "t", QoS::AtMostOnce, true);
        t.subscribe(cid("b"), "grp", "t", QoS::AtMostOnce, true);
        t.unsubscribe(&cid("a"), "grp", "t");
        assert_eq!(member_ids(&t, "t"), vec!["b"]);
        t.remove_client(&cid("b"));
        assert_eq!(t.group_count(), 0);
        assert!(t.matching("t").is_empty());
    }

    #[test]
    fn resubscribe_updates_qos_in_place() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g", "t", QoS::AtMostOnce, true);
        t.subscribe(cid("a"), "g", "t", QoS::ExactlyOnce, true);
        let groups = t.matching("t");
        assert_eq!(groups[0].members.len(), 1, "still one member");
        assert_eq!(groups[0].members[0].1, QoS::ExactlyOnce, "QoS updated");
    }

    #[test]
    fn snapshot_lists_every_group_with_members() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g1", "t/+", QoS::AtMostOnce, true);
        t.subscribe(cid("b"), "g2", "other", QoS::AtLeastOnce, true);
        let mut snap = t.snapshot();
        snap.sort_by(|x, y| x.group.cmp(&y.group));
        assert_eq!(snap.len(), 2);
        assert_eq!(
            (snap[0].group.as_str(), snap[0].filter.as_str()),
            ("g1", "t/+")
        );
        assert_eq!(snap[1].members, vec![(cid("b"), QoS::AtLeastOnce)]);
    }
    /// The online prefix is what selection reads, so it must stay exact through
    /// every order of subscribe, attach, detach and unsubscribe.
    #[test]
    fn the_online_prefix_survives_every_transition_order() {
        let mut t = SharedSubscriptionTable::new();
        for c in ["a", "b", "c", "d"] {
            t.subscribe(cid(c), "g", "sport/#", QoS::AtMostOnce, false);
        }
        let members = |t: &SharedSubscriptionTable| {
            t.matching_refs("sport/tennis").next().map(|(_, _, m)| {
                (
                    m.online_count(),
                    m.len(),
                    m.iter()
                        .take(m.online_count())
                        .map(|(c, _)| c.0.to_string())
                        .collect::<Vec<_>>(),
                )
            })
        };
        assert_eq!(members(&t).unwrap().0, 0, "nobody attached yet");

        t.set_client_online(&cid("b"), true);
        t.set_client_online(&cid("d"), true);
        let (online, total, names) = members(&t).unwrap();
        assert_eq!((online, total), (2, 4));
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["b", "d"], "exactly the attached members lead");

        // A detach must take one OUT of the prefix without disturbing the other.
        t.set_client_online(&cid("b"), false);
        let (online, total, names) = members(&t).unwrap();
        assert_eq!((online, total), (1, 4));
        assert_eq!(names, vec!["d"]);

        // Unsubscribing an OFFLINE member cannot move an online one across.
        t.unsubscribe(&cid("a"), "g", "sport/#");
        let (online, total, names) = members(&t).unwrap();
        assert_eq!((online, total), (1, 3));
        assert_eq!(names, vec!["d"]);

        // Unsubscribing the online one empties the prefix, not the group.
        t.unsubscribe(&cid("d"), "g", "sport/#");
        let (online, total, _) = members(&t).unwrap();
        assert_eq!((online, total), (0, 2));
    }

    /// Selection rotates over the ONLINE members only: an offline member never
    /// consumes a turn, and every online one takes an equal share.
    #[test]
    fn selection_rotates_only_over_online_members() {
        let mut t = SharedSubscriptionTable::new();
        for c in ["a", "b", "c"] {
            t.subscribe(cid(c), "g", "sport/#", QoS::AtMostOnce, false);
        }
        t.set_client_online(&cid("a"), true);
        t.set_client_online(&cid("c"), true);

        let (_, _, m) = t.matching_refs("sport/tennis").next().unwrap();
        let mut seen = std::collections::HashMap::new();
        for cursor in 0..600 {
            let (client, _) = m.select_online(cursor).expect("an online member");
            *seen.entry(client.0.to_string()).or_insert(0) += 1;
        }
        assert_eq!(seen.len(), 2, "offline 'b' was never selected: {seen:?}");
        assert_eq!(seen["a"], 300);
        assert_eq!(seen["c"], 300);
    }

    /// With nobody online there is nothing to select — the caller falls back to
    /// a persistent local or a remote member, exactly as before.
    #[test]
    fn selection_is_empty_when_no_member_is_online() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g", "sport/#", QoS::AtMostOnce, false);
        let (_, _, m) = t.matching_refs("sport/tennis").next().unwrap();
        assert!(m.select_online(0).is_none());
        assert_eq!(
            m.len(),
            1,
            "the member is still there, just not deliverable"
        );
    }

    /// A disconnect removes the client from every group it joined and leaves no
    /// membership behind in the reverse index — otherwise a later attach would
    /// resurrect it into an online prefix it no longer belongs to.
    #[test]
    fn removing_a_client_clears_it_from_every_group_it_joined() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g1", "sport/#", QoS::AtMostOnce, true);
        t.subscribe(cid("a"), "g2", "news/#", QoS::AtMostOnce, true);
        t.subscribe(cid("b"), "g1", "sport/#", QoS::AtMostOnce, true);

        t.remove_client(&cid("a"));
        let (_, _, m) = t.matching_refs("sport/tennis").next().unwrap();
        assert_eq!(m.len(), 1, "only b remains in the sport group");
        assert_eq!(m.online_count(), 1);
        assert!(
            t.matching_refs("news/today").next().is_none(),
            "the emptied group is gone"
        );

        // The stale membership must not come back on a later attach.
        t.set_client_online(&cid("a"), true);
        let (_, _, m) = t.matching_refs("sport/tennis").next().unwrap();
        assert_eq!(
            m.online_count(),
            1,
            "a is gone and cannot re-enter a prefix"
        );
    }

    /// Re-subscribing updates the granted `QoS` without duplicating the member or
    /// disturbing the partition.
    #[test]
    fn resubscribing_updates_qos_without_duplicating_or_reordering() {
        let mut t = SharedSubscriptionTable::new();
        t.subscribe(cid("a"), "g", "sport/#", QoS::AtMostOnce, true);
        t.subscribe(cid("a"), "g", "sport/#", QoS::AtLeastOnce, true);
        let (_, _, m) = t.matching_refs("sport/tennis").next().unwrap();
        assert_eq!(m.len(), 1, "one member, not two");
        assert_eq!(m.online_count(), 1);
        assert_eq!(
            m[0].1,
            QoS::AtLeastOnce,
            "the granted QoS was updated in place"
        );
    }
}
