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
//! actions = ["publish"]         # publish|subscribe (topic rule) OR connect
//!                               # (client-id rule); the two kinds don't mix
//! effect = "allow"              # optional; "allow" (default) or "deny"
//! topics = ["devices/%i/#"]     # MQTT filter patterns; %i substitutes the
//!                               # identity subject, %c the client id
//!
//! [[rules]]                     # a connect rule (ADR 0031): which client ids
//! identities = ["tenant-a-*"]   # an identity may claim
//! actions = ["connect"]
//! clients = ["tenant-a/%i/*"]   # client-id globs; %i substitutes the subject
//! effect = "allow"
//! ```
//!
//! ## Connect rules (ADR 0031, opt-in)
//! `connect` rules constrain which **client ids** an identity may use, via `clients` globs
//! (not `topics`). They are **opt-in**: with no `connect` rule, every connect is permitted;
//! once any exists, a connect needs a matching allow (deny wins), so an operator can namespace
//! client ids per tenant. This is layered on top of the secure-by-default session-owner guard
//! (which binds a session to its creator with no configuration).
//!
//! ## Substitution: `%i` and `%c` (ADR 0004 T12)
//! In `topics` patterns, `%i` expands to the identity subject and `%c` to the connecting
//! client id. Both fail closed: an empty value, or one containing `/`, `+` or `#`, makes
//! the pattern unusable — an **allow** then grants nothing and a **deny** refuses the
//! action outright. A rule is only exposed to a placeholder it actually names, so a
//! hostile client id cannot spoil a rule that never says `%c`.
//!
//! **`%i` and `%c` are not interchangeable, and `%c` is the weaker of the two.** The
//! subject is established by the server (a verified certificate field, a password
//! record, a token claim); the client id is chosen outright by the client. The
//! session-owner guard (ADR 0031) stops a client from *taking over another identity's*
//! session, but nothing stops it from picking any unused id it likes. So
//! `topics = ["dev/%c/#"]` scopes a grant to the **session handle**, not to a principal:
//! absent other constraints it grants the union over every id that client could choose.
//!
//! Use `%c` to separate a principal's own sessions (per-device telemetry under one fleet
//! identity, say), not as a tenant boundary. To make it an isolation boundary, pair it
//! with a `connect` rule that constrains which ids the identity may claim — then the
//! reachable set of `%c` values is exactly what the policy admits:
//! ```toml
//! [[rules]]                          # only these ids are claimable...
//! identities = ["fleet-a"]
//! actions = ["connect"]
//! clients = ["fleet-a-*"]
//!
//! [[rules]]                          # ...so %c can only expand within them
//! identities = ["fleet-a"]
//! actions = ["publish"]
//! topics = ["telemetry/%c/#"]
//! ```
//! `%c` is rejected in a `connect` rule's `clients` globs: there it would match the
//! client id against itself and allow every id, which is the opposite of what such a
//! rule is for.
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
use mqtt_core::{ClientId, TopicFilter, TopicName};
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
    /// Client-id glob patterns for a `connect` rule (ADR 0031 option B); `%i` substitutes the
    /// identity subject. Mutually exclusive with `topics`.
    #[serde(default)]
    clients: Vec<String>,
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
    /// A `connect` rule (ADR 0031): constrains which client ids the principal may claim. Uses
    /// `clients` (glob patterns) rather than `topics`, so it is evaluated on its own path.
    connect: bool,
    effect: Effect,
    topics: Vec<String>,
    clients: Vec<String>,
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
    fn evaluate(&self, identity: &Identity, client_id: &str, action: Action, target: &str) -> bool {
        let mut allow_hit = false;
        for rule in &self.rules {
            if !rule.applies_to(action) || !rule.matches_principal(identity) {
                continue;
            }
            for pattern in &rule.topics {
                // `%i`/`%c` substitution fails closed (ADR 0004): both values are
                // untrusted — the subject is a certificate CN or SAN, the client id is
                // chosen outright by the client — and substituting one that carries
                // topic metacharacters could broaden the pattern across namespaces.
                // When a pattern cannot be substituted safely, an allow grants nothing
                // and a deny denies the action outright.
                let Some(pattern) = substitute(pattern, &identity.subject, client_id) else {
                    if rule.effect == Effect::Deny {
                        return false;
                    }
                    continue;
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

    /// Decide a connect against the `connect` rules (ADR 0031 option B). Connect enforcement is
    /// **opt-in**: if the policy declares no `connect` rules, every connect is permitted (the
    /// secure-by-default session-owner guard still applies independently). Once any `connect`
    /// rule exists, the usual order applies among matching ones — a deny wins, else an allow
    /// permits, else the connect is refused (deny-by-default within the connect namespace).
    fn evaluate_connect(&self, identity: &Identity, client_id: &str) -> bool {
        // Enforcement is keyed on whether the *policy* declares any connect rule — not on
        // whether one matches this principal — so defining connect rules for known tenants
        // denies every identity that matches none (deny-by-default within the namespace).
        let has_connect_rules = self.rules.iter().any(|r| r.connect);
        let mut allow_hit = false;
        for rule in &self.rules {
            if !rule.connect || !rule.matches_principal(identity) {
                continue;
            }
            for pattern in &rule.clients {
                // `%i` substitution fails closed, as for topics: an unsubstitutable subject
                // grants nothing on an allow and refuses outright on a deny. `%c` is not
                // substituted here — matching the client id against itself always succeeds,
                // so it is rejected at validation rather than silently allowing everything.
                let pattern = if pattern.contains("%i") {
                    if safe_for_substitution(&identity.subject) {
                        pattern.replace("%i", &identity.subject)
                    } else if rule.effect == Effect::Deny {
                        return false;
                    } else {
                        continue;
                    }
                } else {
                    pattern.clone()
                };
                if glob_match(&pattern, client_id) {
                    match rule.effect {
                        Effect::Deny => return false,
                        Effect::Allow => allow_hit = true,
                    }
                }
            }
        }
        // No connect rules at all → unrestricted; otherwise require an allow hit.
        allow_hit || !has_connect_rules
    }
}

/// Expand `%i` (identity subject) and `%c` (client id) in a pattern, or `None` if the
/// pattern names a placeholder whose value is unsafe to substitute.
///
/// A pattern mentioning neither placeholder is returned unchanged. A pattern is only
/// rejected for the placeholders it actually uses: a hostile client id cannot poison a
/// rule that never says `%c`.
/// Substitution is a **single left-to-right pass**: substituted text is never rescanned
/// for further placeholders. Doing it as two `replace` passes would let a subject of
/// literally `%c` expand into the client id — letting the client, not the policy, choose
/// the namespace — and the flaw would be silently one-directional (whichever placeholder
/// is expanded first). Here a `%` in a value is inert.
fn substitute(pattern: &str, subject: &str, client_id: &str) -> Option<String> {
    if !pattern.contains('%') {
        return Some(pattern.to_string());
    }
    let mut out = String::with_capacity(pattern.len());
    let mut rest = pattern;
    while let Some(i) = rest.find('%') {
        out.push_str(&rest[..i]);
        let at = &rest[i..];
        let value = if at.starts_with("%i") {
            Some(subject)
        } else if at.starts_with("%c") {
            Some(client_id)
        } else {
            None
        };
        match value {
            // A pattern is only exposed to the placeholders it actually names, so a
            // hostile client id cannot poison a rule that never says `%c`.
            Some(v) if !safe_for_substitution(v) => return None,
            Some(v) => {
                out.push_str(v);
                rest = &at[2..];
            }
            // Not a placeholder: `%` is an ordinary character in a topic.
            None => {
                out.push('%');
                rest = &at[1..];
            }
        }
    }
    out.push_str(rest);
    Some(out)
}

/// Whether `value` is a single, wildcard-free topic level safe to substitute for `%i`
/// or `%c`. An empty value, or one containing a level separator or a topic wildcard, is
/// not — substituting it could broaden a rule across namespaces.
fn safe_for_substitution(value: &str) -> bool {
    !value.is_empty() && !value.contains(['/', '+', '#'])
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
    let (mut publish, mut subscribe, mut connect) = (false, false, false);
    for action in &raw.actions {
        match action.as_str() {
            "publish" => publish = true,
            "subscribe" => subscribe = true,
            "connect" => connect = true,
            other => {
                return Err(AclError::Invalid(format!(
                    "rule {index}: unknown action \"{other}\" \
                     (expected \"publish\", \"subscribe\", or \"connect\")"
                )));
            }
        }
    }

    // A `connect` rule matches client ids (`clients`); a publish/subscribe rule matches topics
    // (`topics`). They are kept separate so each rule is unambiguous about what it constrains.
    if connect {
        if publish || subscribe {
            return Err(AclError::Invalid(format!(
                "rule {index}: `connect` cannot be combined with publish/subscribe in one rule"
            )));
        }
        if !raw.topics.is_empty() {
            return Err(AclError::Invalid(format!(
                "rule {index}: a `connect` rule uses `clients`, not `topics`"
            )));
        }
        if raw.clients.is_empty() {
            return Err(AclError::Invalid(format!(
                "rule {index}: a `connect` rule must list `clients`"
            )));
        }
        // `%c` here would match the client id against itself and always succeed, so a
        // rule meant to *constrain* which ids an identity may claim would silently
        // permit every id. Refuse the policy rather than accept a tautology (ADR 0004 T12).
        if raw.clients.iter().any(|c| c.contains("%c")) {
            return Err(AclError::Invalid(format!(
                "rule {index}: `%c` is meaningless in `clients` (it matches the client id \
                 against itself and would allow every id); use a literal glob or `%i`"
            )));
        }
    } else {
        if !raw.clients.is_empty() {
            return Err(AclError::Invalid(format!(
                "rule {index}: `clients` is only valid on a `connect` rule"
            )));
        }
        if raw.topics.is_empty() {
            return Err(AclError::Invalid(format!(
                "rule {index}: `topics` must not be empty"
            )));
        }
    }

    Ok(Rule {
        identities: raw.identities,
        groups: raw.groups,
        publish,
        subscribe,
        connect,
        effect,
        topics: raw.topics,
        clients: raw.clients,
    })
}

impl Authorizer for AclPolicy {
    fn authorize_publish(
        &self,
        identity: &Identity,
        client_id: &ClientId,
        topic: &TopicName,
    ) -> bool {
        self.evaluate(identity, &client_id.0, Action::Publish, topic)
    }
    fn authorize_subscribe(
        &self,
        identity: &Identity,
        client_id: &ClientId,
        filter: &TopicFilter,
    ) -> bool {
        self.evaluate(identity, &client_id.0, Action::Subscribe, filter)
    }
    fn authorize_connect(&self, identity: &Identity, client_id: &ClientId) -> bool {
        self.evaluate_connect(identity, &client_id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{AclError, AclPolicy};
    use crate::{Authorizer, Identity};
    use mqtt_core::ClientId;

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

    /// The client id used by tests that are not about `%c`. Deliberately a value that
    /// would be *visible* if it ever leaked into a pattern that never named `%c`.
    const ANY_CLIENT: &str = "some-client";

    fn can_pub(p: &AclPolicy, id: &Identity, topic: &str) -> bool {
        can_pub_as(p, id, ANY_CLIENT, topic)
    }

    fn can_sub(p: &AclPolicy, id: &Identity, filter: &str) -> bool {
        can_sub_as(p, id, ANY_CLIENT, filter)
    }

    fn can_pub_as(p: &AclPolicy, id: &Identity, client: &str, topic: &str) -> bool {
        p.authorize_publish(id, &ClientId(client.into()), &topic.to_string())
    }

    fn can_sub_as(p: &AclPolicy, id: &Identity, client: &str, filter: &str) -> bool {
        p.authorize_subscribe(id, &ClientId(client.into()), &filter.to_string())
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

    // ----- %c substitution (ADR 0004 T12) -----

    #[test]
    fn percent_c_scopes_topics_to_the_client_id() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish", "subscribe"]
            topics = ["dev/%c/#"]
            "#,
        )
        .unwrap();
        let id = ident("alpha", &[]);
        assert!(can_pub_as(&p, &id, "probe-1", "dev/probe-1/x"));
        assert!(!can_pub_as(&p, &id, "probe-1", "dev/probe-2/x"));
        assert!(can_sub_as(&p, &id, "probe-1", "dev/probe-1/#"));
        assert!(!can_sub_as(&p, &id, "probe-1", "dev/probe-2/#"));
        // Coverage, not overlap, exactly as for `%i`.
        assert!(!can_sub_as(&p, &id, "probe-1", "dev/#"));
    }

    /// The whole point of T12: one identity, two sessions, disjoint grants. Without
    /// `%c` a per-session split is inexpressible — `%i` is the same for both.
    #[test]
    fn one_identity_two_client_ids_get_disjoint_grants() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish"]
            topics = ["telemetry/%i/%c"]
            "#,
        )
        .unwrap();
        let fleet = ident("fleet-a", &[]);
        assert!(can_pub_as(
            &p,
            &fleet,
            "sensor-1",
            "telemetry/fleet-a/sensor-1"
        ));
        assert!(!can_pub_as(
            &p,
            &fleet,
            "sensor-1",
            "telemetry/fleet-a/sensor-2"
        ));
        assert!(can_pub_as(
            &p,
            &fleet,
            "sensor-2",
            "telemetry/fleet-a/sensor-2"
        ));
        // The identity half still binds: another subject cannot reach this namespace.
        assert!(!can_pub_as(
            &p,
            &ident("fleet-b", &[]),
            "sensor-1",
            "telemetry/fleet-a/sensor-1"
        ));
    }

    /// Client ids are chosen outright by the client — the most attacker-controlled
    /// value the engine substitutes. A metacharacter must not broaden the grant.
    #[test]
    fn a_hostile_client_id_cannot_broaden_a_grant() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish", "subscribe"]
            topics = ["dev/%c/#"]
            "#,
        )
        .unwrap();
        let id = ident("alpha", &[]);
        for hostile in ["+", "#", "a/b", ""] {
            assert!(
                !can_sub_as(&p, &id, hostile, "dev/+/#"),
                "client id {hostile:?} must not turn dev/%c/# into a wildcard grant"
            );
            assert!(
                !can_sub_as(&p, &id, hostile, "dev/other/#"),
                "client id {hostile:?} must not reach another client's namespace"
            );
            assert!(
                !can_pub_as(&p, &id, hostile, "dev/other/x"),
                "client id {hostile:?} must not publish into another namespace"
            );
        }
    }

    /// Failing closed cuts both ways: on a **deny** rule an unsubstitutable client id
    /// refuses the action outright rather than letting the deny evaporate.
    #[test]
    fn an_unsubstitutable_client_id_makes_a_deny_refuse_outright() {
        let p = AclPolicy::from_toml_str(
            r##"
            [[rules]]
            actions = ["publish"]
            topics = ["#"]

            [[rules]]
            actions = ["publish"]
            effect = "deny"
            topics = ["dev/%c/secret"]
            "##,
        )
        .unwrap();
        let id = ident("alpha", &[]);
        // A well-formed id: the deny applies to its own namespace only.
        assert!(!can_pub_as(&p, &id, "probe-1", "dev/probe-1/secret"));
        assert!(can_pub_as(&p, &id, "probe-1", "dev/probe-1/public"));
        // A hostile id cannot dodge the deny by making it unsubstitutable.
        assert!(!can_pub_as(&p, &id, "a/b", "dev/probe-1/public"));
    }

    /// A rule that never names `%c` is untouched by an unsubstitutable client id —
    /// failing closed is scoped to the placeholder actually used, so one bad session
    /// handle cannot take down unrelated grants.
    #[test]
    fn a_bad_client_id_does_not_disturb_rules_that_never_use_it() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish", "subscribe"]
            topics = ["shared/#", "dev/%i/#"]
            "#,
        )
        .unwrap();
        let id = ident("alpha", &[]);
        assert!(can_pub_as(&p, &id, "+", "shared/x"));
        assert!(can_pub_as(&p, &id, "a/b", "dev/alpha/x"));
    }

    /// Substitution does not rescan its own output. A subject of literally `%c` must
    /// stay the literal text `%c`, not expand into the client id — otherwise the client
    /// would pick the namespace a `%i` rule resolves to.
    #[test]
    fn a_substituted_value_is_never_rescanned_for_placeholders() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish"]
            topics = ["dev/%i/#"]
            "#,
        )
        .unwrap();
        // Subject is the literal "%c" — legal (no `/`, `+`, `#`), so the rule expands
        // to `dev/%c/#` and stops there.
        let tricky = ident("%c", &[]);
        assert!(
            can_pub_as(&p, &tricky, "chosen", "dev/%c/x"),
            "the pattern must resolve to the literal namespace dev/%c"
        );
        assert!(
            !can_pub_as(&p, &tricky, "chosen", "dev/chosen/x"),
            "a %c in the SUBJECT must not expand into the client id"
        );
    }

    /// A `%` that starts no placeholder is an ordinary topic character, and an unknown
    /// `%x` is left alone rather than silently swallowed.
    #[test]
    fn a_bare_percent_is_literal() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["publish"]
            topics = ["odd/100%/%x/%i"]
            "#,
        )
        .unwrap();
        assert!(can_pub(&p, &ident("alpha", &[]), "odd/100%/%x/alpha"));
    }

    /// `%c` in a `connect` rule's `clients` glob would match the client id against
    /// itself and allow every id — the opposite of what the rule is for.
    #[test]
    fn percent_c_is_rejected_in_connect_client_globs() {
        let msg = err_msg(
            r#"
            [[rules]]
            actions = ["connect"]
            clients = ["tenant-a/%c"]
            "#,
        );
        assert!(
            msg.contains("%c"),
            "message should name the offending placeholder: {msg}"
        );
    }

    /// The documented way to make `%c` an isolation boundary: a connect rule fixes the
    /// set of claimable ids, so the reachable `%c` values are exactly what it admits.
    #[test]
    fn a_connect_rule_bounds_the_client_ids_percent_c_can_expand_to() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            identities = ["fleet-a"]
            actions = ["connect"]
            clients = ["fleet-a-*"]

            [[rules]]
            identities = ["fleet-a"]
            actions = ["publish"]
            topics = ["telemetry/%c/#"]
            "#,
        )
        .unwrap();
        let fleet = ident("fleet-a", &[]);
        assert!(p.authorize_connect(&fleet, &ClientId("fleet-a-1".into())));
        // An id outside the connect glob never gets a session, so `telemetry/evil/#`
        // is unreachable even though the topic rule alone would have expanded to it.
        assert!(!p.authorize_connect(&fleet, &ClientId("evil".into())));
        assert!(can_pub_as(&p, &fleet, "fleet-a-1", "telemetry/fleet-a-1/x"));
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

    // ----- connect rules (ADR 0031 option B) -----

    fn can_connect(p: &AclPolicy, id: &Identity, client_id: &str) -> bool {
        p.authorize_connect(id, &mqtt_core::ClientId(client_id.into()))
    }

    #[test]
    fn connect_is_unrestricted_without_connect_rules() {
        // A policy with only topic rules does not gate connect at all (opt-in).
        let p = AclPolicy::from_toml_str(
            r#"
            default = "deny"
            [[rules]]
            actions = ["publish"]
            topics = ["t/#"]
            "#,
        )
        .unwrap();
        assert!(can_connect(&p, &ident("anyone", &[]), "any-client-id"));
    }

    #[test]
    fn connect_rules_namespace_client_ids_per_identity() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            identities = ["tenant-a-*"]
            actions = ["connect"]
            clients = ["tenant-a/%i/*"]
            "#,
        )
        .unwrap();
        let a = ident("tenant-a-alice", &[]);
        // The identity may claim its own namespaced ids...
        assert!(can_connect(&p, &a, "tenant-a/tenant-a-alice/sensor1"));
        // ...but not another tenant's, nor an unprefixed id.
        assert!(!can_connect(&p, &a, "tenant-b/x"));
        assert!(!can_connect(&p, &a, "tenant-a/someone-else/x"));
        // An identity that matches no connect rule, once any connect rule exists, is denied.
        assert!(!can_connect(&p, &ident("outsider", &[]), "whatever"));
    }

    #[test]
    fn a_connect_deny_rule_wins() {
        let p = AclPolicy::from_toml_str(
            r#"
            [[rules]]
            actions = ["connect"]
            clients = ["*"]
            effect = "allow"
            [[rules]]
            identities = ["banned"]
            actions = ["connect"]
            clients = ["*"]
            effect = "deny"
            "#,
        )
        .unwrap();
        assert!(can_connect(&p, &ident("alice", &[]), "anything"));
        assert!(!can_connect(&p, &ident("banned", &[]), "anything"));
    }

    #[test]
    fn connect_cannot_mix_with_topic_actions() {
        let msg = err_msg(
            r#"
            [[rules]]
            actions = ["connect", "publish"]
            clients = ["*"]
            topics = ["t"]
            "#,
        );
        assert!(msg.contains("connect"), "should name the conflict: {msg}");
    }

    #[test]
    fn a_connect_rule_requires_clients_not_topics() {
        assert!(err_msg(
            r#"
            [[rules]]
            actions = ["connect"]
            topics = ["t"]
            "#,
        )
        .contains("clients"));
        assert!(err_msg(
            r#"
            [[rules]]
            actions = ["connect"]
            "#,
        )
        .contains("clients"));
    }

    #[test]
    fn clients_is_rejected_on_a_topic_rule() {
        let msg = err_msg(
            r#"
            [[rules]]
            actions = ["publish"]
            topics = ["t"]
            clients = ["x"]
            "#,
        );
        assert!(
            msg.contains("clients"),
            "should reject clients on topic rule: {msg}"
        );
    }
}
