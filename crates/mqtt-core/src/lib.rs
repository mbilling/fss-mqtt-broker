//! Broker core domain logic: client sessions, the subscription tree, retained
//! message store, and `QoS` delivery state machines.
//!
//! This crate is transport- and storage-agnostic. State backends are injected via
//! traits (see [`mqtt-storage`](../mqtt_storage)) so the same core runs single-node,
//! embedded, or clustered without change.

pub use mqtt_codec::QoS;

pub mod subscriptions;
pub use subscriptions::SubscriptionTable;

pub mod shared;
pub use shared::{is_shared_filter, parse_shared, SharedSubscriptionTable};

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

/// Returns whether every topic matched by `request` is also matched by
/// `pattern` (filter subsumption). Used for ACL **allow** rules: a granted
/// pattern must cover the requested subscription entirely.
#[must_use]
// As in `topic_matches`: the "covered, stop" arms are distinct rules that
// happen to share a body; keeping them separate aids readability.
#[allow(clippy::match_same_arms)]
pub fn filter_covers(pattern: &str, request: &str) -> bool {
    // Mirror `topic_matches`: leading wildcards never reach `$`-rooted topics,
    // so they cannot cover a `$`-rooted request either.
    if request.starts_with('$') && matches!(pattern.chars().next(), Some('+' | '#')) {
        return false;
    }

    let mut p = pattern.split('/');
    let mut r = request.split('/');

    loop {
        match (p.next(), r.next()) {
            // `#` covers this level and everything below, including the
            // request ending exactly here (as "a/#" matches "a").
            (Some("#"), _) => return true,
            // A request `#` here matches deeper topics than any non-`#`
            // pattern level can.
            (Some(_), Some("#")) => return false,
            // `+` covers any single request level: a literal or `+` itself.
            (Some("+"), Some(_)) => {}
            // A literal covers only the identical literal (`+` was consumed
            // by the arm above, so `rl` here is never a wildcard).
            (Some(pl), Some(rl)) if pl == rl => {}
            // Both exhausted at the same depth: covered.
            (None, None) => return true,
            // Any other length/value mismatch: not covered.
            _ => return false,
        }
    }
}

/// Returns whether some concrete topic is matched by **both** filters. Used
/// for ACL **deny** rules: a denied pattern blocks any subscription that could
/// receive a matching message, however broad.
#[must_use]
// As in `topic_matches`: the "overlap found, stop" arms are distinct rules
// that happen to share a body; keeping them separate aids readability.
#[allow(clippy::match_same_arms)]
pub fn filters_overlap(a: &str, b: &str) -> bool {
    // Mirror `topic_matches`: a leading-wildcard filter matches no `$`-rooted
    // topic, so it shares nothing with a `$`-literal-rooted filter.
    fn dollar_disjoint(wild: &str, rooted: &str) -> bool {
        rooted.starts_with('$') && matches!(wild.chars().next(), Some('+' | '#'))
    }
    if dollar_disjoint(a, b) || dollar_disjoint(b, a) {
        return false;
    }

    let mut ai = a.split('/');
    let mut bi = b.split('/');

    loop {
        match (ai.next(), bi.next()) {
            // `#` on either side matches everything from here down, including
            // the other side being exhausted (as "a/#" matches "a").
            (Some("#"), _) | (_, Some("#")) => return true,
            // `+` is compatible with any single level on the other side.
            (Some("+"), Some(_)) | (Some(_), Some("+")) => {}
            // Identical literals: keep walking.
            (Some(al), Some(bl)) if al == bl => {}
            // Both exhausted at the same depth: some shared topic exists.
            (None, None) => return true,
            // Differing literals, or one side exhausted with non-`#` levels
            // remaining on the other: disjoint.
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{filter_covers, filters_overlap, topic_matches};

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

    /// Empty levels are real levels per the spec: `"a/"` is two levels
    /// (`"a"`, `""`) and distinct from `"a"`.
    #[test]
    fn empty_levels_are_distinct_levels() {
        assert!(topic_matches("a/+", "a/"));
        assert!(!topic_matches("a/b/", "a/b"));
        assert!(!topic_matches("a/b", "a/b/"));
        assert!(topic_matches("+/+", "/finance"));
        assert!(topic_matches("/+", "/finance"));
        assert!(!topic_matches("+", "/finance"));
    }

    #[test]
    fn covers_hash_covers_everything_non_system() {
        assert!(filter_covers("#", "a/b"));
        assert!(filter_covers("#", "a/#"));
        assert!(filter_covers("#", "+"));
    }

    #[test]
    fn covers_hash_tail_covers_parent_and_deeper_filters() {
        assert!(filter_covers("a/#", "a"));
        assert!(filter_covers("a/#", "a/b/#"));
        assert!(filter_covers("a/#", "a/b/c"));
        assert!(filter_covers("a/#", "a/+"));
    }

    #[test]
    fn covers_plus_covers_single_levels_only() {
        assert!(filter_covers("a/+/c", "a/b/c"));
        assert!(filter_covers("a/+/c", "a/+/c"));
        assert!(!filter_covers("+", "#"));
        assert!(!filter_covers("a/+", "a/#"));
        assert!(!filter_covers("devices/+/state", "devices/#"));
    }

    #[test]
    fn covers_literal_levels_are_strict() {
        assert!(filter_covers("a/b/c", "a/b/c"));
        assert!(!filter_covers("a/b/c", "a/+/c"));
        assert!(!filter_covers("a/b/c", "a/b"));
        assert!(!filter_covers("a/b", "a/b/c"));
    }

    #[test]
    fn covers_system_topics_excluded_from_leading_wildcards() {
        assert!(!filter_covers("#", "$SYS/x"));
        assert!(!filter_covers("+/x", "$SYS/x"));
        assert!(filter_covers("$SYS/#", "$SYS/x"));
        assert!(filter_covers("$SYS/#", "$SYS/#"));
    }

    #[test]
    fn covers_empty_levels_consistent_with_topic_matches() {
        assert!(filter_covers("a/+", "a/"));
        assert!(!filter_covers("a/b", "a/b/"));
        assert!(!filter_covers("a/b/", "a/b"));
        assert!(filter_covers("+/+", "/finance"));
        assert!(!filter_covers("+", "/finance"));
    }

    #[test]
    fn overlap_hash_overlaps_any_non_system_filter() {
        assert!(filters_overlap("secret/#", "#"));
        assert!(filters_overlap("#", "secret/#"));
        assert!(filters_overlap("#", "#"));
        assert!(filters_overlap("a/#", "a/b/c/#"));
        assert!(filters_overlap("a/b/c/#", "a/#"));
    }

    #[test]
    fn overlap_plus_is_compatible_with_any_single_level() {
        assert!(filters_overlap("a/+", "a/b"));
        assert!(filters_overlap("a/b", "a/+"));
        assert!(filters_overlap("a/+", "a/+"));
        assert!(filters_overlap("a/+", "a/"));
    }

    #[test]
    fn overlap_disjoint_literals_do_not_overlap() {
        assert!(!filters_overlap("a/b", "a/c"));
        assert!(!filters_overlap("a/c", "a/b"));
    }

    #[test]
    fn overlap_depth_mismatch_only_bridged_by_hash() {
        assert!(filters_overlap("a", "a/#"));
        assert!(filters_overlap("a/#", "a"));
        assert!(!filters_overlap("a", "a/b"));
        assert!(!filters_overlap("a/b", "a"));
        assert!(!filters_overlap("a", "a/+"));
        assert!(!filters_overlap("a/+", "a"));
    }

    #[test]
    fn overlap_system_topics_excluded_from_leading_wildcards() {
        assert!(!filters_overlap("#", "$SYS/#"));
        assert!(!filters_overlap("$SYS/#", "#"));
        assert!(!filters_overlap("+", "$SYS"));
        assert!(filters_overlap("$SYS/#", "$SYS/+"));
        assert!(filters_overlap("$SYS/+", "$SYS/#"));
    }

    #[test]
    fn covers_dollar_root_does_not_cover_plain_wildcard_request() {
        // The inverse $-direction: "$SYS/#" matches only $-rooted topics, so it
        // cannot cover "#", which also matches non-$ topics.
        assert!(!filter_covers("$SYS/#", "#"));
        assert!(!filter_covers("$SYS/#", "+"));
    }

    // --- exhaustive cross-check of the security-relevant directions ----------
    //
    // `filter_covers` and `filters_overlap` are security primitives, and what
    // matters is the SAFE direction of any imprecision, not exact equality:
    //
    //   * `filter_covers` gates ACL ALLOW rules, so it must be SOUND — if it
    //     reports coverage, every topic the request matches must really be
    //     covered (never over-grant). A conservative under-report is safe.
    //   * `filters_overlap` gates ACL DENY rules, so it must be COMPLETE —
    //     every genuine overlap must be reported (never let a deny be
    //     bypassed). A conservative over-report is safe.
    //
    // Ground truth comes from `topic_matches` over every filter/topic the
    // alphabet {a, b, "", +, #} generates up to depth 3 — a few hundred
    // thousand pairs — so the hand-picked cases above are backed by brute
    // force in exactly the direction that protects the broker.

    /// All level tokens used to build filters; `""` exercises empty levels.
    const FILTER_TOKENS: &[&str] = &["a", "b", "", "+", "#"];
    /// Concrete-topic tokens (no wildcards) for the ground-truth universe.
    const TOPIC_TOKENS: &[&str] = &["a", "b", "", "$x"];

    /// Every syntactically-sensible filter up to `max_depth` levels. `#` is
    /// only legal as the final level, so a token list ending in a non-final
    /// `#` is skipped.
    fn gen_filters(max_depth: usize) -> Vec<String> {
        let mut out = Vec::new();
        gen_rec(FILTER_TOKENS, max_depth, &mut Vec::new(), &mut out, true);
        out
    }

    fn gen_topics(max_depth: usize) -> Vec<String> {
        let mut out = Vec::new();
        gen_rec(TOPIC_TOKENS, max_depth, &mut Vec::new(), &mut out, false);
        out
    }

    fn gen_rec(
        tokens: &[&'static str],
        depth: usize,
        cur: &mut Vec<&'static str>,
        out: &mut Vec<String>,
        filters: bool,
    ) {
        if !cur.is_empty() {
            out.push(cur.join("/"));
        }
        if depth == 0 {
            return;
        }
        for &tok in tokens {
            cur.push(tok);
            // `#` must be the last level of a filter; never appears in topics.
            if filters && tok == "#" {
                out.push(cur.join("/"));
            } else {
                gen_rec(tokens, depth - 1, cur, out, filters);
            }
            cur.pop();
        }
    }

    #[test]
    fn filter_relations_are_sound_and_complete_in_the_safe_direction() {
        let filters = gen_filters(3);
        let topics = gen_topics(3);
        assert!(
            filters.len() > 100 && topics.len() > 20,
            "universe too small"
        );

        let mut checked = 0u64;
        for p in &filters {
            for r in &filters {
                let matched: Vec<&String> = topics.iter().filter(|t| topic_matches(r, t)).collect();
                let truly_covers =
                    !matched.is_empty() && matched.iter().all(|t| topic_matches(p, t));
                let truly_overlaps = topics
                    .iter()
                    .any(|t| topic_matches(p, t) && topic_matches(r, t));

                // SOUNDNESS of allow: a reported coverage must be real.
                if filter_covers(p, r) && !matched.is_empty() {
                    assert!(
                        truly_covers,
                        "filter_covers({p:?}, {r:?}) over-grants: claims coverage the \
                         definition denies"
                    );
                }
                // COMPLETENESS of deny: a real overlap must be reported.
                if truly_overlaps {
                    assert!(
                        filters_overlap(p, r),
                        "filters_overlap({p:?}, {r:?}) misses a real overlap — a deny \
                         rule could be bypassed"
                    );
                }
                // Overlap is defined symmetrically; the implementation must be too.
                assert_eq!(
                    filters_overlap(p, r),
                    filters_overlap(r, p),
                    "filters_overlap is not symmetric for {p:?}, {r:?}"
                );
                checked += 1;
            }
        }
        // ~105 filters squared; each pair is checked against the whole topic
        // universe, so this is ~10k filter pairs over ~30 topics.
        assert!(
            checked > 10_000,
            "expected a large cross-check, ran {checked}"
        );
    }
}
