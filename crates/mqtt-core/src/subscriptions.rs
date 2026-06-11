//! In-memory subscription registry mapping topic filters to subscribed clients.
//!
//! This is a pure, runtime-agnostic data structure: the async broker hub drives
//! it, and a clustered build will gossip a digest derived from it. Matching uses
//! [`crate::topic_matches`], so wildcard and `$SYS` rules are applied consistently.

use crate::{topic_matches, ClientId, TopicFilter};
use std::collections::{HashMap, HashSet};

/// Maps topic filters to the set of clients subscribed to each.
#[derive(Debug, Default)]
pub struct SubscriptionTable {
    by_filter: HashMap<TopicFilter, HashSet<ClientId>>,
}

impl SubscriptionTable {
    /// Create an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `client` is subscribed to `filter`. Idempotent.
    pub fn subscribe(&mut self, client: ClientId, filter: TopicFilter) {
        self.by_filter.entry(filter).or_default().insert(client);
    }

    /// Remove `client`'s subscription to `filter`, if present.
    pub fn unsubscribe(&mut self, client: &ClientId, filter: &str) {
        if let Some(clients) = self.by_filter.get_mut(filter) {
            clients.remove(client);
            if clients.is_empty() {
                self.by_filter.remove(filter);
            }
        }
    }

    /// Remove all of `client`'s subscriptions (called on disconnect).
    pub fn remove_client(&mut self, client: &ClientId) {
        self.by_filter.retain(|_, clients| {
            clients.remove(client);
            !clients.is_empty()
        });
    }

    /// Return the de-duplicated set of clients whose filters match `topic`.
    ///
    /// A client subscribed via several overlapping filters appears once.
    #[must_use]
    pub fn matching_clients(&self, topic: &str) -> HashSet<ClientId> {
        let mut out = HashSet::new();
        for (filter, clients) in &self.by_filter {
            if topic_matches(filter, topic) {
                out.extend(clients.iter().cloned());
            }
        }
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
    use crate::ClientId;

    fn cid(s: &str) -> ClientId {
        ClientId(s.to_string())
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
}
