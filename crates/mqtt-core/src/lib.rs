//! Broker core domain logic: client sessions, the subscription tree, retained
//! message store, and `QoS` delivery state machines.
//!
//! This crate is transport- and storage-agnostic. State backends are injected via
//! traits (see [`mqtt-storage`](../mqtt_storage)) so the same core runs single-node,
//! embedded, or clustered without change.

pub use mqtt_codec::QoS;

pub mod subscriptions;
pub use subscriptions::SubscriptionTable;

/// A normalized topic name (no wildcards) as published.
pub type TopicName = String;

/// A topic filter that may contain `+` (single level) or `#` (multi level) wildcards.
pub type TopicFilter = String;

/// Stable identifier for a connected client.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

/// A subscription request from a client.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// The topic filter to match published messages against.
    pub filter: TopicFilter,
    /// Maximum `QoS` the broker may use to deliver matching messages.
    pub max_qos: QoS,
    /// MQTT 5 no-local: do not echo a client's own publications back to it.
    pub no_local: bool,
}

/// A message in flight through the broker.
#[derive(Debug, Clone)]
pub struct Message {
    /// Destination topic (no wildcards).
    pub topic: TopicName,
    /// Opaque application payload.
    pub payload: bytes::Bytes,
    /// Delivery `QoS`.
    pub qos: QoS,
    /// Whether the broker should retain this message for the topic.
    pub retain: bool,
}

/// Returns whether a wildcard topic `filter` matches a concrete `topic`.
///
/// Implements MQTT topic-matching rules including `+`/`#` wildcards and the
/// `$`-prefixed system-topic exclusion.
#[must_use]
// The `+`-wildcard and literal-equal cases are distinct matching rules that
// happen to share a "keep walking" body; keeping them separate aids readability.
#[allow(clippy::match_same_arms)]
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    // System topics ($SYS, $share, ...) are not matched by leading wildcards.
    if topic.starts_with('$') && matches!(filter.chars().next(), Some('+' | '#')) {
        return false;
    }

    let mut f = filter.split('/');
    let mut t = topic.split('/');

    loop {
        match (f.next(), t.next()) {
            // `#` matches this level and all deeper levels.
            (Some("#"), _) => return true,
            // `+` matches exactly one level; a literal level must be equal.
            (Some("+"), Some(_)) => {}
            (Some(fl), Some(tl)) if fl == tl => {}
            // Both filter and topic exhausted at the same depth: a match.
            (None, None) => return true,
            // Any length/value mismatch: not a match.
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::topic_matches;

    #[test]
    fn exact_match() {
        assert!(topic_matches("a/b/c", "a/b/c"));
        assert!(!topic_matches("a/b/c", "a/b/d"));
    }

    #[test]
    fn single_level_wildcard() {
        assert!(topic_matches("a/+/c", "a/b/c"));
        assert!(!topic_matches("a/+/c", "a/b/c/d"));
        assert!(!topic_matches("a/+", "a/b/c"));
    }

    #[test]
    fn multi_level_wildcard() {
        assert!(topic_matches("a/#", "a/b/c"));
        assert!(topic_matches("a/#", "a"));
        assert!(topic_matches("#", "a/b/c"));
    }

    #[test]
    fn system_topics_excluded_from_leading_wildcards() {
        assert!(!topic_matches("#", "$SYS/broker/uptime"));
        assert!(!topic_matches("+/broker", "$SYS/broker"));
        assert!(topic_matches("$SYS/#", "$SYS/broker/uptime"));
    }
}
