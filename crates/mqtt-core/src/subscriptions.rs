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

use crate::filter_index::FilterIndex;
use crate::{ClientId, FilterKey, TopicFilter};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

/// Maps topic filters to the set of clients subscribed to each.
#[derive(Debug, Default)]
pub struct SubscriptionTable {
    by_filter: HashMap<FilterKey, HashSet<ClientId>>,
    /// The derived match index (issue #445). Lives in [`crate::filter_index`] so
    /// the local and remote `$share` populations can share one walk rather than
    /// each scanning their filters per publish.
    index: FilterIndex,
}

impl SubscriptionTable {
    /// Create an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `client` is subscribed to `filter`. Idempotent.
    ///
    /// Takes an owned [`FilterKey`]: pass [`intern`](Self::intern)'s result so the
    /// table, the trie terminal and the caller's own per-client bookkeeping all
    /// share one allocation.
    pub fn subscribe(&mut self, client: ClientId, filter: FilterKey) {
        match self.by_filter.entry(filter) {
            Entry::Vacant(v) => {
                self.index.insert(v.key());
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
                self.index.remove(filter);
            }
        }
    }

    /// Remove all of `client`'s subscriptions (called on disconnect).
    pub fn remove_client(&mut self, client: &ClientId) {
        let mut emptied: Vec<FilterKey> = Vec::new();
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
            self.index.remove(&filter);
        }
    }

    /// Every registered filter matching `topic`. Delegates to the shared
    /// [`FilterIndex`] walk.
    fn for_each_matching_filter<'a>(&'a self, topic: &str, cb: impl FnMut(&'a FilterKey)) {
        self.index.for_each_matching(topic, cb);
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
    ///
    /// `String::from(&**k)`, deliberately, not `k.to_string()`: `Arc<str>` has no
    /// `ToString` specialization, so `to_string` formats through `Formatter::pad`
    /// instead of copying the bytes. This runs on every SUBSCRIBE and UNSUBSCRIBE
    /// via the interest gossip, so the difference is not academic.
    #[must_use]
    pub fn filters(&self) -> Vec<TopicFilter> {
        self.by_filter.keys().map(|k| String::from(&**k)).collect()
    }

    /// The canonical [`FilterKey`] for `filter` — the existing allocation if this
    /// table already holds the filter, otherwise a fresh one.
    ///
    /// Callers should intern **before** [`subscribe`](Self::subscribe) and reuse the
    /// result for their own per-client bookkeeping, so one filter is one allocation
    /// no matter how many clients subscribe to it.
    ///
    /// Best-effort by construction: a filter interned while its last subscriber is
    /// being removed yields a new `Arc`. Equality is therefore always by value —
    /// never `Arc::ptr_eq`.
    #[must_use]
    pub fn intern(&self, filter: &str) -> FilterKey {
        self.by_filter
            .get_key_value(filter)
            .map_or_else(|| FilterKey::from(filter), |(k, _)| FilterKey::clone(k))
    }
}

#[cfg(test)]
mod tests {
    use super::SubscriptionTable;
    use crate::{topic_matches, valid_filter, ClientId, FilterKey};
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
        t.subscribe(cid("x"), deep.as_str().into());
        t.subscribe(cid("y"), deep.as_str().into());
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
            table.subscribe(cid(&format!("c{i}")), f.as_str().into());
            table.subscribe(cid("everyone"), f.as_str().into());
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

    /// The point of interning: one distinct filter is ONE allocation, shared by the
    /// table's key and the trie's terminal, no matter how many clients subscribe.
    ///
    /// `ptr_eq` is the right tool *here* — this asserts sharing, not equality. Callers
    /// must still compare filters by value (see [`FilterKey`]).
    #[test]
    fn a_distinct_filter_is_allocated_once_however_many_subscribers() {
        let mut t = SubscriptionTable::new();
        let k = t.intern("sensors/+/temp");
        t.subscribe(cid("a"), FilterKey::clone(&k));

        // The table already holds it, so a second subscriber gets the SAME allocation
        // rather than minting another.
        let k2 = t.intern("sensors/+/temp");
        assert!(
            FilterKey::ptr_eq(&k, &k2),
            "intern handed back a copy instead of the stored allocation"
        );
        t.subscribe(cid("b"), k2);

        // Our handle + the by_filter key + the trie terminal. Before interning, the
        // terminal was an independent String, so this was 2, not 3.
        assert_eq!(
            FilterKey::strong_count(&k),
            3,
            "expected the by_filter key and the trie terminal to share one allocation"
        );

        assert_eq!(
            t.matching_clients("sensors/kitchen/temp"),
            HashSet::from([cid("a"), cid("b")]),
            "interning must not change matching"
        );

        // And the allocation is released when the last subscriber goes.
        t.unsubscribe(&cid("a"), "sensors/+/temp");
        t.unsubscribe(&cid("b"), "sensors/+/temp");
        assert_eq!(
            FilterKey::strong_count(&k),
            1,
            "table and trie must both drop their handle on the last unsubscribe"
        );
    }
}
