//! File-based topic ACLs (ADR 0004 step 3): deny-by-default authorization
//! evaluated per identity, action, and topic.
//!
//! ## Policy format (TOML)
//! ```toml
//! default = "deny"              # optional; "deny" (the default) or "allow"
//!
//! [[rules]]
//! identities = ["device-*"]     # any-of globs on the identity subject
//! groups = ["ops"]              # any-of group names; a rule matches a
//!                               # principal if EITHER list hits (both empty
//!                               # or omitted = everyone)
//! actions = ["publish"]         # non-empty subset of publish|subscribe
//! effect = "allow"              # optional; "allow" (default) or "deny"
//! topics = ["devices/%i/#"]     # MQTT filter patterns; %i substitutes the
//!                               # identity subject (%c is deferred until the
//!                               # Authorizer trait carries the client id)
//! ```
//!
//! ## Decision semantics
//! Among the rules matching the principal and action: any matching **deny**
//! rule wins; otherwise any matching **allow** rule permits; otherwise the
//! `default` applies. Topic matching is deliberately asymmetric:
//! - **allow** rules use *coverage* ([`mqtt_core::filter_covers`]): a granted
//!   pattern must subsume the requested subscription, so allowing
//!   `devices/+/state` does not admit a `devices/#` subscription;
//! - **deny** rules use *overlap* ([`mqtt_core::filters_overlap`]): a denied
//!   pattern blocks any subscription that could receive a matching message,
//!   so denying `secret/#` also blocks a `#` subscription.
//!
//! Publish targets are concrete topics and use plain MQTT filter matching.

use crate::{Action, Authorizer, Identity};
use mqtt_core::{TopicFilter, TopicName};
use serde::Deserialize;

/// Errors from parsing or validating an ACL policy.
#[derive(Debug, thiserror::Error)]
pub enum AclError {
    /// The policy file is not valid TOML or violates the schema.
    #[error("invalid ACL policy: {0}")]
    Invalid(String),
}

/// Raw policy document as deserialized from TOML, before validation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPolicy {
    default: Option<String>,
    #[serde(default)]
    rules: Vec<RawRule>,
}

/// Raw rule as deserialized from TOML, before validation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRule {
    #[serde(default)]
    identities: Vec<String>,
    #[serde(default)]
    groups: Vec<String>,
    #[serde(default)]
    actions: Vec<String>,
    effect: Option<String>,
    #[serde(default)]
    topics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Effect {
    Allow,
    Deny,
}

/// A validated rule in evaluation form.
#[derive(Debug)]
struct Rule {
    identities: Vec<String>,
    groups: Vec<String>,
    publish: bool,
    subscribe: bool,
    effect: Effect,
    topics: Vec<String>,
}

impl Rule {
    fn applies_to(&self, action: Action) -> bool {
        match action {
            Action::Publish => self.publish,
            Action::Subscribe => self.subscribe,
        }
    }

    /// Both lists empty means "everyone"; otherwise either list may hit.
    fn matches_principal(&self, identity: &Identity) -> bool {
        if self.identities.is_empty() && self.groups.is_empty() {
            return true;
        }
        self.identities
            .iter()
            .any(|glob| glob_match(glob, &identity.subject))
            || self
                .groups
                .iter()
                .any(|g| identity.groups.iter().any(|m| m == g))
    }
}

/// Matches `text` against a glob `pattern` where `*` matches any run of
/// characters (including none) and every other character is literal.
fn glob_match(pattern: &str, text: &str) -> bool {
    let p = pattern.as_bytes();
    let t = text.as_bytes();
    let (mut pi, mut ti) = (0, 0);
    // Most recent `*`: (pattern index after it, text index it has consumed to).
    let mut star: Option<(usize, usize)> = None;

    while ti < t.len() {
        if pi < p.len() && p[pi] == b'*' {
            star = Some((pi + 1, ti));
            pi += 1;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some((after_star, consumed)) = star {
            // Backtrack: let the last `*` swallow one more character.
            pi = after_star;
            ti = consumed + 1;
            star = Some((after_star, consumed + 1));
        } else {
            return false;
        }
    }
    // Only trailing stars may remain unconsumed.
    p[pi..].iter().all(|&c| c == b'*')
}

/// A parsed, validated ACL policy. Build with [`AclPolicy::from_toml_str`].
#[derive(Debug)]
pub struct AclPolicy {
    default_allow: bool,
    rules: Vec<Rule>,
}

impl AclPolicy {
    /// Parse and validate a policy from TOML text.
    ///
    /// # Errors
    /// [`AclError::Invalid`] on TOML syntax errors, unknown fields/values,
    /// empty `actions` or `topics` lists, or an invalid `default`/`effect`.
    pub fn from_toml_str(input: &str) -> Result<Self, AclError> {
        let raw: RawPolicy = toml::from_str(input).map_err(|e| AclError::Invalid(e.to_string()))?;

        let default_allow = match raw.default.as_deref() {
            None | Some("deny") => false,
            Some("allow") => true,
            Some(other) => {
                return Err(AclError::Invalid(format!(
                    "unknown default \"{other}\" (expected \"allow\" or \"deny\")"
                )));
            }
        };

        let rules = raw
            .rules
            .into_iter()
            .enumerate()
            .map(|(i, r)| validate_rule(i, r))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            default_allow,
            rules,
        })
    }

    /// Applies the documented decision order: among rules matching the
    /// principal and action, any deny hit refuses, else any allow hit
    /// permits, else the policy default applies.
    fn evaluate(&self, identity: &Identity, action: Action, target: &str) -> bool {
        let mut allow_hit = false;
        for rule in &self.rules {
            if !rule.applies_to(action) || !rule.matches_principal(identity) {
                continue;
            }
            for pattern in &rule.topics {
                // `%i` substitution fails closed (ADR 0004): the subject is an
                // untrusted certificate CN, and substituting one that carries
                // topic metacharacters could broaden the pattern across
                // namespaces. When it cannot be substituted safely, an allow
                // grants nothing and a deny denies the action outright.
                let pattern = if pattern.contains("%i") {
                    if subject_safe_for_substitution(&identity.subject) {
                        pattern.replace("%i", &identity.subject)
                    } else if rule.effect == Effect::Deny {
                        return false;
                    } else {
                        continue;
                    }
                } else {
                    pattern.clone()
                };
                let hit = match (action, rule.effect) {
                    // Publish targets are concrete topics: plain matching.
                    (Action::Publish, _) => mqtt_core::topic_matches(&pattern, target),
                    // An allow must subsume the requested subscription...
                    (Action::Subscribe, Effect::Allow) => {
                        mqtt_core::filter_covers(&pattern, target)
                    }
                    // ...while a deny blocks anything that could touch it.
                    (Action::Subscribe, Effect::Deny) => {
                        mqtt_core::filters_overlap(&pattern, target)
                    }
                };
                if hit {
                    match rule.effect {
                        Effect::Deny => return false,
                        Effect::Allow => allow_hit = true,
                    }
                }
            }
        }
        allow_hit || self.default_allow
    }
}

/// Whether `subject` is a single, wildcard-free topic level safe to substitute
/// for `%i`. An empty subject, or one containing a level separator or a topic
/// wildcard, is not — substituting it could broaden a rule across namespaces.
fn subject_safe_for_substitution(subject: &str) -> bool {
    !subject.is_empty() && !subject.contains(['/', '+', '#'])
}

fn validate_rule(index: usize, raw: RawRule) -> Result<Rule, AclError> {
    let effect = match raw.effect.as_deref() {
        None | Some("allow") => Effect::Allow,
        Some("deny") => Effect::Deny,
        Some(other) => {
            return Err(AclError::Invalid(format!(
                "rule {index}: unknown effect \"{other}\" (expected \"allow\" or \"deny\")"
            )));
        }
    };

    if raw.actions.is_empty() {
        return Err(AclError::Invalid(format!(
            "rule {index}: `actions` must not be empty"
        )));
    }
    let (mut publish, mut subscribe) = (false, false);
    for action in &raw.actions {
        match action.as_str() {
            "publish" => publish = true,
            "subscribe" => subscribe = true,
            other => {
                return Err(AclError::Invalid(format!(
                    "rule {index}: unknown action \"{other}\" \
                     (expected \"publish\" or \"subscribe\")"
                )));
            }
        }
    }

    if raw.topics.is_empty() {
        return Err(AclError::Invalid(format!(
            "rule {index}: `topics` must not be empty"
        )));
    }

    Ok(Rule {
        identities: raw.identities,
        groups: raw.groups,
        publish,
        subscribe,
        effect,
        topics: raw.topics,
    })
}

impl Authorizer for AclPolicy {
    fn authorize_publish(&self, identity: &Identity, topic: &TopicName) -> bool {
        self.evaluate(identity, Action::Publish, topic)
    }
    fn authorize_subscribe(&self, identity: &Identity, filter: &TopicFilter) -> bool {
        self.evaluate(identity, Action::Subscribe, filter)
    }
}

#[cfg(test)]
mod tests {
    use super::{AclError, AclPolicy};
    use crate::{Authorizer, Identity};

    fn ident(subject: &str, groups: &[&str]) -> Identity {
        Identity {
            subject: subject.to_string(),
            groups: groups.iter().map(ToString::to_string).collect(),
        }
    }

    fn err_msg(input: &str) -> String {
        match AclPolicy::from_toml_str(input) {
            Err(AclError::Invalid(msg)) => msg,
            Ok(_) => panic!("expected parse failure for: {input}"),
        }
    }

    fn can_pub(p: &AclPolicy, id: &Identity, topic: &str) -> bool {
        p.authorize_publish(id, &topic.to_string())
    }

    fn can_sub(p: &AclPolicy, id: &Identity, filter: &str) -> bool {
        p.authorize_subscribe(id, &filter.to_string())
    }

    // ----- parse / validation failures -----

    #[test]
    fn invalid_toml_is_rejected() {
        let msg = err_msg("default = [unclosed");
        assert!(
            msg.contains("invalid") || msg.contains("expected"),
            "message should name the syntax problem: {msg}"
        );
    }

    #[test]
    fn unknown_field_is_rejected() {
        let msg = err_msg(
            r#"
            [[rules]]
            idenities = ["device-*"]
            actions = ["publish"]
            topics = ["a/#"]
            "#,
        );
        assert!(
            msg.contains("idenities"),
            "message should name the unknown field: {msg}"
        );
    }

    #[test]
    fn unknown_action_is_rejected() {
        let msg = err_msg(
            r#"
            [[rules]]
            actions = ["pub"]
            topics = ["a/#"]
            "#,
        );
        assert!(
            msg.contains("action") && msg.contains("\"pub\""),
            "message should name the bad action: {msg}"
        );
    }

    #[test]
    fn unknown_effect_is_rejected() {
        let msg = err_msg(
            r#"
            [[rules]]
            actions = ["publish"]
            effect = "block"
            topics = ["a/#"]
            "#,
        );
        assert!(
            msg.contains("effect") && msg.contains("\"block\""),
            "message should name the bad effect: {msg}"
        );
    }

    #[test]
    fn unknown_default_is_rejected() {
        let msg = err_msg(r#"default = "open""#);
        assert!(
            msg.contains("default") && msg.contains("\"open\""),
            "message should name the bad default: {msg}"
        );
    }

    #[test]
    fn empty_actions_are_rejected() {
        let msg = err_msg(
            r#"
            [[rules]]
            actions = []
            topics = ["a/#"]
            "#,
        );
        assert!(
            msg.contains("actions"),
            "message should name the empty list: {msg}"
        );
    }

    #[test]
    fn empty_topics_are_rejected() {
        let msg = err_msg(
            r#"
            [[rules]]
            actions = ["publish"]
            topics = []
            "#,
        );
        assert!(
            msg.contains("topics"),
            "message should name the empty list: {msg}"
        );
    }

    // ----- defaults -----

    #[test]
    fn no_rules_denies_everything_by_default() {
        let p = AclPolicy::from_toml_str("").unwrap();
        let id = ident("alice", &[]);
        assert!(!can_pub(&p, &id, "a/b"));
        assert!(!can_sub(&p, &id, "a/#"));
    }

    #[test]
    fn explicit_default_allow_permits_everything_absent_rules() {
        let p = AclPolicy::from_toml_str(r#"default = "allow""#).unwrap();
        let id = ident("alice", &[]);
        assert!(can_pub(&p, &id, "a/b"));
        assert!(can_sub(&p, &id, "a/#"));
    }

    // ----- principal matching -----

    #[test]
    fn everyone_rule_applies_to_any_subject() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish", "subscribe"]
            topics = ["lobby/#"]
            "#,
        )
        .unwrap();
        assert!(can_pub(&p, &ident("anyone", &[]), "lobby/hi"));
        assert!(can_sub(&p, &ident("someone-else", &["g"]), "lobby/#"));
        assert!(!can_pub(&p, &ident("anyone", &[]), "elsewhere/x"));
    }

    #[test]
    fn identity_glob_prefix_matching() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            identities = ["device-*"]
            actions = ["publish"]
            topics = ["t"]
            "#,
        )
        .unwrap();
        assert!(can_pub(&p, &ident("device-7", &[]), "t"));
        assert!(can_pub(&p, &ident("device-", &[]), "t"));
        assert!(!can_pub(&p, &ident("sensor-7", &[]), "t"));
        assert!(!can_pub(&p, &ident("a-device-7", &[]), "t"));
    }

    #[test]
    fn identity_glob_star_matches_anything() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            identities = ["*"]
            actions = ["publish"]
            topics = ["t"]
            "#,
        )
        .unwrap();
        assert!(can_pub(&p, &ident("", &[]), "t"));
        assert!(can_pub(&p, &ident("anything at all", &[]), "t"));
    }

    #[test]
    fn identity_glob_multiple_stars() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            identities = ["a*b*c"]
            actions = ["publish"]
            topics = ["t"]
            "#,
        )
        .unwrap();
        assert!(can_pub(&p, &ident("abc", &[]), "t"));
        assert!(can_pub(&p, &ident("aXbYc", &[]), "t"));
        // The first `*` must backtrack past the early `b` to find a later one.
        assert!(can_pub(&p, &ident("a-b-x-b-c", &[]), "t"));
        assert!(!can_pub(&p, &ident("acb", &[]), "t"));
        assert!(!can_pub(&p, &ident("ab", &[]), "t"));
        assert!(!can_pub(&p, &ident("abcX", &[]), "t"));
    }

    #[test]
    fn identity_without_star_requires_exact_equality() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            identities = ["alice"]
            actions = ["publish"]
            topics = ["t"]
            "#,
        )
        .unwrap();
        assert!(can_pub(&p, &ident("alice", &[]), "t"));
        assert!(!can_pub(&p, &ident("alicee", &[]), "t"));
        assert!(!can_pub(&p, &ident("alic", &[]), "t"));
        assert!(!can_pub(&p, &ident("ALICE", &[]), "t"));
    }

    #[test]
    fn regex_special_characters_in_globs_are_literal() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            identities = ["us.east[1](prod)", "node.*"]
            actions = ["publish"]
            topics = ["t"]
            "#,
        )
        .unwrap();
        assert!(can_pub(&p, &ident("us.east[1](prod)", &[]), "t"));
        // `.` is not a wildcard.
        assert!(!can_pub(&p, &ident("usXeast[1](prod)", &[]), "t"));
        assert!(can_pub(&p, &ident("node.7", &[]), "t"));
        assert!(!can_pub(&p, &ident("nodeX7", &[]), "t"));
    }

    #[test]
    fn group_membership_matches() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            groups = ["ops"]
            actions = ["subscribe"]
            topics = ["metrics/#"]
            "#,
        )
        .unwrap();
        assert!(can_sub(&p, &ident("carol", &["ops"]), "metrics/#"));
        assert!(can_sub(&p, &ident("dave", &["dev", "ops"]), "metrics/#"));
        assert!(!can_sub(&p, &ident("eve", &["dev"]), "metrics/#"));
        assert!(!can_sub(&p, &ident("ops", &[]), "metrics/#"));
    }

    #[test]
    fn identities_or_groups_either_list_may_hit() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            identities = ["alice"]
            groups = ["ops"]
            actions = ["publish"]
            topics = ["t"]
            "#,
        )
        .unwrap();
        assert!(can_pub(&p, &ident("alice", &[]), "t"));
        assert!(can_pub(&p, &ident("bob", &["ops"]), "t"));
        assert!(!can_pub(&p, &ident("bob", &["dev"]), "t"));
    }

    // ----- %i substitution -----

    #[test]
    fn percent_i_scopes_topics_to_the_subject() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish", "subscribe"]
            topics = ["dev/%i/#"]
            "#,
        )
        .unwrap();
        let alpha = ident("alpha", &[]);
        assert!(can_pub(&p, &alpha, "dev/alpha/x"));
        assert!(!can_pub(&p, &alpha, "dev/beta/x"));
        assert!(can_sub(&p, &alpha, "dev/alpha/#"));
        assert!(can_sub(&p, &alpha, "dev/alpha/state"));
        assert!(!can_sub(&p, &alpha, "dev/beta/#"));
        // Coverage, not overlap: a broader filter is refused outright.
        assert!(!can_sub(&p, &alpha, "dev/#"));
    }

    // ----- deny precedence and asymmetric topic tests -----

    #[test]
    fn deny_wins_over_allow_for_its_action_only() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish", "subscribe"]
            topics = ["a/#"]

            [[rules]]
            actions = ["publish"]
            effect = "deny"
            topics = ["a/secret"]
            "#,
        )
        .unwrap();
        let id = ident("alice", &[]);
        assert!(can_pub(&p, &id, "a/x"));
        assert!(!can_pub(&p, &id, "a/secret"));
        // The deny is publish-only: subscribing across it is still fine.
        assert!(can_sub(&p, &id, "a/#"));
    }

    #[test]
    fn deny_blocks_any_overlapping_subscription() {
        let p = AclPolicy::from_toml_str(
            r##"
            [[rules]]
            actions = ["subscribe"]
            topics = ["#"]

            [[rules]]
            actions = ["subscribe"]
            effect = "deny"
            topics = ["secret/#"]
            "##,
        )
        .unwrap();
        let id = ident("alice", &[]);
        // `#` could receive secret/* messages, so it is refused even though
        // the allow rule covers it.
        assert!(!can_sub(&p, &id, "#"));
        assert!(!can_sub(&p, &id, "secret/x"));
        assert!(can_sub(&p, &id, "public/x"));
        assert!(can_sub(&p, &id, "public/#"));
    }

    #[test]
    fn deny_overlap_applies_under_default_allow_too() {
        let p = AclPolicy::from_toml_str(
            r#"
            default = "allow"

            [[rules]]
            actions = ["subscribe"]
            effect = "deny"
            topics = ["secret/#"]
            "#,
        )
        .unwrap();
        let id = ident("alice", &[]);
        assert!(!can_sub(&p, &id, "#"));
        assert!(can_sub(&p, &id, "public/x"));
    }

    // ----- action scoping -----

    #[test]
    fn publish_only_allow_does_not_grant_subscribe_and_vice_versa() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish"]
            topics = ["up/#"]

            [[rules]]
            actions = ["subscribe"]
            topics = ["down/#"]
            "#,
        )
        .unwrap();
        let id = ident("alice", &[]);
        assert!(can_pub(&p, &id, "up/x"));
        assert!(!can_sub(&p, &id, "up/x"));
        assert!(can_sub(&p, &id, "down/x"));
        assert!(!can_pub(&p, &id, "down/x"));
    }

    /// An identity subject is a certificate CN — untrusted text. If it carries
    /// topic metacharacters, `%i` substitution must NOT broaden a grant across
    /// namespaces; substitution fails closed.
    #[test]
    fn percent_i_substitution_fails_closed_for_unsafe_subjects() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish", "subscribe"]
            topics = ["dev/%i/#"]
            "#,
        )
        .unwrap();

        // Subject "+" must not turn "dev/%i/#" into the wildcard "dev/+/#".
        let plus = ident("+", &[]);
        assert!(!can_pub(&p, &plus, "dev/victim/data"));
        assert!(!can_sub(&p, &plus, "dev/victim/#"));

        // A subject with "/" must not inject extra levels ("dev/a/b/#").
        let slashed = ident("a/b", &[]);
        assert!(!can_pub(&p, &slashed, "dev/a/b/data"));
        assert!(!can_pub(&p, &slashed, "dev/other/data"));

        // "#" and an empty subject are equally unusable.
        assert!(!can_pub(&p, &ident("#", &[]), "dev/anything"));
        assert!(!can_pub(&p, &ident("", &[]), "dev/anything"));

        // The legitimate case still works.
        assert!(can_pub(&p, &ident("alpha", &[]), "dev/alpha/data"));
    }

    /// Failing `%i` closed is scoped to `%i` patterns: a `/`-bearing subject
    /// (e.g. a future SAN/SPIFFE identity) is still governed normally by rules
    /// with literal topics.
    #[test]
    fn unsafe_subject_still_governed_by_non_substituting_rules() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            identities = ["*"]
            actions = ["publish"]
            topics = ["public/#"]
            "#,
        )
        .unwrap();
        assert!(can_pub(&p, &ident("a/b", &[]), "public/x"));
        assert!(!can_pub(&p, &ident("a/b", &[]), "private/x"));
    }

    #[test]
    fn allowed_narrow_subscribe_does_not_cover_broader_request() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["subscribe"]
            topics = ["devices/+/state"]
            "#,
        )
        .unwrap();
        let id = ident("alice", &[]);
        assert!(can_sub(&p, &id, "devices/d1/state"));
        assert!(can_sub(&p, &id, "devices/+/state"));
        assert!(!can_sub(&p, &id, "devices/#"));
        assert!(!can_sub(&p, &id, "devices/d1/#"));
    }
}
