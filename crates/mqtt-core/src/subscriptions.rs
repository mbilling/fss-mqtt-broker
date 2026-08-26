//! In-memory subscription registry mapping topic filters to subscribed clients.
//!
//! This is a pure, runtime-agnostic data structure: the async broker hub drives
//! it, and a clustered build will gossip a digest derived from it. Matching uses
//! the same rules as [`crate::topic_matches`], so wildcard and `$SYS` behaviour
//! stay consistent — pinned by an equivalence test against a linear reference.
//!
//! ## Shape (issue #445)
//!
//! Two structures, one authority. `by_filter` (filter → clients) is the source of
//! truth: registration, teardown, `filters()` and `filter_count()` read and write
//! it exactly as they always have. The **trie** is a derived match index over the
//! same filters — one node per topic level, a filter's string stored at its
//! terminal node — so `matching_clients` walks the TOPIC's levels instead of
//! scanning every filter in the broker: O(topic depth) instead of O(all filters),
//! which was the per-publish cost this replaces. A filter enters the trie when its
//! first subscriber appears and leaves when its last one goes, so the two
//! structures cannot disagree about which filters exist.
//!
//! **Precondition:** filters are [`crate::valid_filter`]-valid (the SUBSCRIBE path
//! enforces this before the table is touched), so `#` only ever terminates a
//! filter. `$share/...` envelopes may be stored verbatim; they contain no
//! wildcard in their first level and behave as literals here, exactly as they
//! did under the linear scan.

use crate::{ClientId, TopicFilter};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

/// One topic level in the match index. `+` and `#` are stored as ordinary keys;
/// their wildcard meaning lives in the walk, not the structure.
#[derive(Debug, Default)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    /// Set when a registered filter ends at this node: the filter string itself,
    /// which is the key back into `by_filter`.
    terminal: Option<TopicFilter>,
}

impl Drop for TrieNode {
    /// Dismantle the subtree iteratively. Node depth tracks the (client-supplied)
    /// filter length, so the derived recursive drop of nested `HashMap`s would
    /// otherwise overflow the stack and abort the broker when a deep filter's
    /// path is pruned or the table is torn down — the same hazard the insert,
    /// match, and remove walks avoid by staying iterative.
    fn drop(&mut self) {
        let mut stack: Vec<TrieNode> = self.children.drain().map(|(_, n)| n).collect();
        while let Some(mut node) = stack.pop() {
            stack.extend(node.children.drain().map(|(_, n)| n));
            // `node` now has no children; its own drop recurses no further.
        }
    }
}

fn trie_insert(root: &mut TrieNode, filter: &str) {
    let mut node = root;
    for level in filter.split('/') {
        node = node.children.entry(level.to_string()).or_default();
    }
    node.terminal = Some(filter.to_string());
}

/// Remove `filter`'s terminal and prune the trailing chain of nodes it leaves
/// empty. Two iterative passes — depth tracks the filter's level count, which is
/// client-controlled input, so recursion here would let one deep SUBSCRIBE
/// overflow the stack and abort the broker (the insert and match walks are
/// iterative for the same reason).
fn trie_remove(root: &mut TrieNode, filter: &str) {
    let levels: Vec<&str> = filter.split('/').collect();
    // Pass 1 (immutable): confirm the path exists and find the deepest node on it
    // that OUTLIVES the removal — one holding its own terminal or a second child
    // once this filter's leaf is gone. The root always outlives it.
    let mut node = &*root;
    let mut cut = 0usize; // index, on the path, of the deepest surviving node
    for (depth, level) in levels.iter().enumerate() {
        let Some(child) = node.children.get(*level) else {
            return; // filter not registered — nothing to remove
        };
        if depth == 0 || node.terminal.is_some() || node.children.len() > 1 {
            cut = depth;
        }
        node = child;
    }
    // `node` is the leaf (index `levels.len()`); it outlives the removal iff it
    // still has children once we drop this terminal.
    if !node.children.is_empty() {
        cut = levels.len();
    }
    // Pass 2 (mutable): clear the terminal if the leaf survives, else detach the
    // whole dead chain at the surviving node — one child removal drops it all.
    let mut node = &mut *root;
    if cut == levels.len() {
        for level in &levels {
            node = node
                .children
                .get_mut(*level)
                .expect("path verified in pass 1");
        }
        node.terminal = None;
    } else {
        for level in &levels[..cut] {
            node = node
                .children
                .get_mut(*level)
                .expect("path verified in pass 1");
        }
        node.children.remove(levels[cut]);
    }
}

/// Maps topic filters to the set of clients subscribed to each.
#[derive(Debug, Default)]
pub struct SubscriptionTable {
    by_filter: HashMap<TopicFilter, HashSet<ClientId>>,
    root: TrieNode,
}

impl SubscriptionTable {
    /// Create an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `client` is subscribed to `filter`. Idempotent.
    pub fn subscribe(&mut self, client: ClientId, filter: TopicFilter) {
        match self.by_filter.entry(filter) {
            Entry::Vacant(v) => {
                trie_insert(&mut self.root, v.key());
                v.insert(HashSet::from([client]));
            }
            Entry::Occupied(mut o) => {
                o.get_mut().insert(client);
            }
        }
    }

    /// Remove `client`'s subscription to `filter`, if present.
    pub fn unsubscribe(&mut self, client: &ClientId, filter: &str) {
        if let Some(clients) = self.by_filter.get_mut(filter) {
            clients.remove(client);
            if clients.is_empty() {
                self.by_filter.remove(filter);
                trie_remove(&mut self.root, filter);
            }
        }
    }

    /// Remove all of `client`'s subscriptions (called on disconnect).
    pub fn remove_client(&mut self, client: &ClientId) {
        let mut emptied: Vec<TopicFilter> = Vec::new();
        self.by_filter.retain(|filter, clients| {
            clients.remove(client);
            if clients.is_empty() {
                emptied.push(filter.clone());
                false
            } else {
                true
            }
        });
        for filter in emptied {
            trie_remove(&mut self.root, &filter);
        }
    }

    /// Every registered filter matching `topic`, via one walk of the topic's
    /// levels. The callback may fire in any order and each filter at most once
    /// (a filter is one trie path).
    fn for_each_matching_filter<'a>(&'a self, topic: &str, mut cb: impl FnMut(&'a TopicFilter)) {
        let levels: Vec<&str> = topic.split('/').collect();
        // [MQTT-4.7.2-1]: a filter whose FIRST level is a wildcard never matches a
        // `$`-rooted topic. Deeper levels are unaffected, so the skip applies to
        // the root frame only.
        let skip_root_wildcards = topic.starts_with('$');
        // Iterative: recursion depth would otherwise track topic depth, which is
        // client-controlled input.
        let mut stack: Vec<(&TrieNode, usize, bool)> = vec![(&self.root, 0, skip_root_wildcards)];
        while let Some((node, i, skip_wildcards)) = stack.pop() {
            if !skip_wildcards {
                // A `#` child matches from its parent's level down — INCLUDING the
                // parent level itself: "a/#" matches "a" (its terminal, checked
                // here, not its subtree).
                if let Some(h) = node.children.get("#") {
                    if let Some(f) = &h.terminal {
                        cb(f);
                    }
                }
            }
            if i == levels.len() {
                if let Some(f) = &node.terminal {
                    cb(f);
                }
                continue;
            }
            if !skip_wildcards {
                if let Some(p) = node.children.get("+") {
                    stack.push((p, i + 1, false));
                }
            }
            let level = levels[i];
            // A topic level spelled "+" or "#" was already answered by the wildcard
            // arms above (those keys ARE the wildcard children); looking it up as a
            // literal would visit the same node twice.
            if level != "+" && level != "#" {
                if let Some(c) = node.children.get(level) {
                    stack.push((c, i + 1, false));
                }
            }
        }
    }

    /// Return the de-duplicated set of clients whose filters match `topic`.
    ///
    /// A client subscribed via several overlapping filters appears once.
    #[must_use]
    pub fn matching_clients(&self, topic: &str) -> HashSet<ClientId> {
        let mut out = HashSet::new();
        self.for_each_matching_filter(topic, |filter| {
            if let Some(clients) = self.by_filter.get(filter) {
                out.extend(clients.iter().cloned());
            }
        });
        out
    }

    /// Number of distinct topic filters currently registered.
    #[must_use]
    pub fn filter_count(&self) -> usize {
        self.by_filter.len()
    }

    /// All distinct topic filters with at least one subscriber.
    ///
    /// Used to build the interest snapshot a node gossips to its cluster peers.
    #[must_use]
    pub fn filters(&self) -> Vec<TopicFilter> {
        self.by_filter.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::SubscriptionTable;
    use crate::{topic_matches, valid_filter, ClientId};
    use std::collections::HashSet;

    fn cid(s: &str) -> ClientId {
        ClientId(s.into())
    }

    #[test]
    fn routes_to_matching_subscribers_only() {
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("a"), "sensors/+/temp".into());
        t.subscribe(cid("b"), "sensors/#".into());
        t.subscribe(cid("c"), "other".into());

        let m = t.matching_clients("sensors/kitchen/temp");
        assert!(m.contains(&cid("a")));
        assert!(m.contains(&cid("b")));
        assert!(!m.contains(&cid("c")));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn client_with_overlapping_filters_appears_once() {
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("a"), "a/#".into());
        t.subscribe(cid("a"), "a/b".into());
        assert_eq!(t.matching_clients("a/b").len(), 1);
    }

    #[test]
    fn unsubscribe_and_remove_client() {
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("a"), "x".into());
        t.subscribe(cid("b"), "x".into());
        t.unsubscribe(&cid("a"), "x");
        assert_eq!(t.matching_clients("x").len(), 1);

        t.remove_client(&cid("b"));
        assert!(t.matching_clients("x").is_empty());
        assert_eq!(t.filter_count(), 0);
    }

    /// Resubscribing is idempotent, and the gossiped interest snapshot lists
    /// each filter once no matter how many clients share it.
    #[test]
    fn resubscribe_is_idempotent_and_filters_are_distinct() {
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("a"), "x".into());
        t.subscribe(cid("a"), "x".into());
        t.subscribe(cid("b"), "x".into());
        assert_eq!(t.matching_clients("x").len(), 2);
        assert_eq!(t.filters(), vec!["x".to_string()]);

        // Unsubscribing a filter that was never held is harmless.
        t.unsubscribe(&cid("a"), "never-subscribed");
        assert_eq!(t.matching_clients("x").len(), 2);
    }

    /// The `#` boundary the trie must get right without a linear scan to fall
    /// back on: `a/#` matches its own parent level `a` [MQTT-4.7.1-2], but `a/+`
    /// does not, and neither matches the bare sibling `b`.
    #[test]
    fn multilevel_wildcard_matches_its_parent_level() {
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("hash"), "a/#".into());
        t.subscribe(cid("plus"), "a/+".into());
        assert_eq!(t.matching_clients("a"), HashSet::from([cid("hash")]));
        assert!(t.matching_clients("b").is_empty());
        assert_eq!(t.matching_clients("a/x").len(), 2);
    }

    /// [MQTT-4.7.2-1]: a first-level wildcard never reaches a `$`-rooted topic,
    /// while an explicit `$SYS/...` filter does — and the rule applies at the
    /// ROOT only ("a/+" happily matches "a/$weird").
    #[test]
    fn dollar_topics_are_hidden_from_leading_wildcards_only() {
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("hash"), "#".into());
        t.subscribe(cid("plus"), "+/broker".into());
        t.subscribe(cid("sys"), "$SYS/#".into());
        assert_eq!(
            t.matching_clients("$SYS/broker"),
            HashSet::from([cid("sys")])
        );

        t.subscribe(cid("deep"), "a/+".into());
        assert!(t.matching_clients("a/$weird").contains(&cid("deep")));
    }

    /// Empty levels are real levels ("sport//x"), and a topic level spelled "+"
    /// is answered by the wildcard child exactly once, not double-counted as a
    /// literal too.
    #[test]
    fn empty_levels_and_literal_wildcard_spellings() {
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("empty"), "sport//x".into());
        t.subscribe(cid("plus"), "sport/+/x".into());
        assert_eq!(t.matching_clients("sport//x").len(), 2);
        assert!(t.matching_clients("sport/x").is_empty());

        let mut t2 = SubscriptionTable::new();
        t2.subscribe(cid("w"), "a/+".into());
        assert_eq!(t2.matching_clients("a/+").len(), 1);
    }

    /// The trie is a DERIVED index over `by_filter`: after any churn sequence the
    /// two must agree, or a pruned-too-eagerly node silently loses routing. The
    /// churn here shares prefixes on purpose ("a/b" under "a/b/c") so pruning one
    /// filter must not orphan the other.
    #[test]
    fn shared_prefix_pruning_keeps_the_survivor_routable() {
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("x"), "a/b".into());
        t.subscribe(cid("y"), "a/b/c".into());
        t.unsubscribe(&cid("y"), "a/b/c");
        assert_eq!(t.matching_clients("a/b"), HashSet::from([cid("x")]));
        assert!(t.matching_clients("a/b/c").is_empty());

        t.subscribe(cid("y"), "a/b/c".into());
        t.remove_client(&cid("x"));
        assert!(t.matching_clients("a/b").is_empty());
        assert_eq!(t.matching_clients("a/b/c"), HashSet::from([cid("y")]));
    }

    /// Removal must not recurse per topic level (the insert and match walks are
    /// iterative for exactly this reason): a filter deep enough to blow a worker
    /// thread's stack under recursive teardown is subscribed, then torn down by
    /// BOTH removal paths. The test reaching its asserts at all is the proof —
    /// recursive teardown would abort the process here.
    #[test]
    fn deep_filter_teardown_stays_iterative() {
        let deep = vec!["a"; 100_000].join("/");
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("x"), deep.clone());
        t.subscribe(cid("y"), deep.clone());
        assert_eq!(
            t.matching_clients(&deep),
            HashSet::from([cid("x"), cid("y")])
        );
        t.unsubscribe(&cid("x"), &deep); // unsubscribe path
        t.remove_client(&cid("y")); // remove_client path
        assert!(t.matching_clients(&deep).is_empty());
        // Fully pruned: the shallow prefix left behind matches nothing either.
        assert!(t.matching_clients("a").is_empty());
    }

    /// The net under everything above: the trie walk agrees with the linear
    /// `topic_matches` reference on EVERY (valid filter, topic) pair drawn from a
    /// generated corpus — wildcards at every position, empty levels, depths
    /// 1..=4 from the alphabet, plus an explicit block of `$`-rooted filters and
    /// topics (which the alphabet cannot spell) — through interleaved
    /// `subscribe`/`unsubscribe`/`remove_client` churn.
    #[test]
    fn trie_matches_exactly_what_the_linear_reference_matches() {
        let alphabet = ["a", "b", "+", "#", ""];
        let mut filters: Vec<String> = Vec::new();
        let mut topics: Vec<String> = vec!["$SYS/broker".into(), "$share/g/a".into(), "$x".into()];
        for d1 in alphabet {
            filters.push(d1.to_string());
            topics.push(d1.to_string());
            for d2 in alphabet {
                filters.push(format!("{d1}/{d2}"));
                topics.push(format!("{d1}/{d2}"));
                for d3 in alphabet {
                    filters.push(format!("{d1}/{d2}/{d3}"));
                    topics.push(format!("{d1}/{d2}/{d3}"));
                    for d4 in alphabet {
                        filters.push(format!("{d1}/{d2}/{d3}/{d4}"));
                        topics.push(format!("{d1}/{d2}/{d3}/{d4}"));
                    }
                }
            }
        }
        // `$`-rooted FILTERS the alphabet can never spell: [MQTT-4.7.2-1] excludes
        // only a filter whose FIRST level is a wildcard, so a literal `$SYS` root
        // with a deeper wildcard must still match, and a leading `+`/`#` must not.
        for f in [
            "$SYS",
            "$SYS/#",
            "$SYS/+",
            "$SYS/broker",
            "$SYS/+/uptime",
            "$share/g/a",
            "+/broker",
            "#",
        ] {
            filters.push(f.to_string());
        }
        for t in ["$SYS", "$SYS/broker", "$SYS/broker/uptime", "$share/g/a"] {
            topics.push(t.to_string());
        }
        filters.retain(|f| valid_filter(f));
        // `by_filter` is keyed by filter STRING, so a duplicate filter would make
        // the per-index reference reconstruction below diverge from the shared
        // table state — dedup keeps one index per distinct filter.
        filters.sort();
        filters.dedup();

        let mut table = SubscriptionTable::new();
        // Interleave subscribe with both removal paths so they run against a
        // live, already-populated trie rather than in separate phases. The final
        // state is still "c{i} gone iff i%3==0, everyone gone iff i%5==0", which
        // the reference loop reconstructs.
        for (i, f) in filters.iter().enumerate() {
            table.subscribe(cid(&format!("c{i}")), f.clone());
            table.subscribe(cid("everyone"), f.clone());
            if i % 3 == 0 {
                table.remove_client(&cid(&format!("c{i}")));
            }
            if i % 5 == 0 {
                table.unsubscribe(&cid("everyone"), f);
            }
        }

        for topic in &topics {
            let got = table.matching_clients(topic);
            let mut want: HashSet<ClientId> = HashSet::new();
            for (i, f) in filters.iter().enumerate() {
                if topic_matches(f, topic) {
                    if i % 3 != 0 {
                        want.insert(cid(&format!("c{i}")));
                    }
                    if i % 5 != 0 {
                        want.insert(cid("everyone"));
                    }
                }
            }
            assert_eq!(got, want, "trie and reference disagree on topic {topic:?}");
        }
    }

    /// A filter torn down to its last subscriber and back: removal must actually
    /// prune the trie path, and a later subscribe must rebuild it — a husk left
    /// behind would either keep matching after removal or refuse to re-register.
    #[test]
    fn a_fully_removed_filter_can_be_resubscribed() {
        let mut t = SubscriptionTable::new();
        t.subscribe(cid("a"), "x/+/z".into());
        assert_eq!(t.matching_clients("x/y/z"), HashSet::from([cid("a")]));
        t.remove_client(&cid("a"));
        assert!(t.matching_clients("x/y/z").is_empty());
        t.subscribe(cid("b"), "x/+/z".into());
        assert_eq!(t.matching_clients("x/y/z"), HashSet::from([cid("b")]));
    }
}
