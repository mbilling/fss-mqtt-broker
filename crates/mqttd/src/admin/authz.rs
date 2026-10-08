//! The authorization dry run (ADR 0081 T5): would this principal be allowed to publish,
//! subscribe or connect, and which rule decides?
//!
//! It asks the **live** authorizer — the policy the last reload published, the one every
//! connection consults — through [`mqtt_auth::Authorizer::explain`], which shares its
//! evaluator with enforcement. It changes nothing.
//!
//! A publish into the broker's reserved `$SYS/` tree is refused before the policy is
//! asked (ADR 0084), and so is it here: the dry run never says allowed for one.

use super::http::Request;
use super::routes::{error, Answer};
use super::AdminState;
use mqtt_auth::{Authorizer, CheckedAction, Explanation, Identity};
use mqtt_core::ClientId;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

/// The live authorizer (the reloadable policy's receiver).
pub type LiveAuthorizer = watch::Receiver<Arc<dyn Authorizer>>;

/// Why a publish into `$SYS/` is refused whatever the policy says (ADR 0084).
pub const RESERVED_REASON: &str = "reserved: $SYS/ is the broker's (ADR 0084)";

/// `GET /admin/v1/authz?user=&action=publish|subscribe|connect&target=&groups=&client=`
///
/// `target` is the topic (publish), the filter (subscribe) or the client id (connect).
/// `groups` (comma-separated) are the principal's groups as its authenticator would report
/// them; `client` is the client id `%c` expands to, defaulting to `user`.
pub fn check(state: &AdminState, req: &Request) -> Answer {
    let Some(live) = state.authz.as_ref() else {
        return error(
            503,
            "unavailable",
            "the authorizer is not wired on this node",
        );
    };
    let (Some(user), Some(action), Some(target)) =
        (req.param("user"), req.param("action"), req.param("target"))
    else {
        return error(400, "bad-request", "user, action and target are required");
    };
    let action = match action {
        "publish" => CheckedAction::Publish,
        "subscribe" => CheckedAction::Subscribe,
        "connect" => CheckedAction::Connect,
        other => {
            return error(
                400,
                "bad-request",
                &format!("action must be publish, subscribe or connect (got {other:?})"),
            )
        }
    };
    if target.is_empty() {
        return error(400, "bad-request", "target must not be empty");
    }
    if action == CheckedAction::Publish && target.contains(['+', '#']) {
        return error(
            400,
            "bad-request",
            "a publish target is a topic name: no + or # wildcards",
        );
    }
    // A filter the broker would refuse at SUBSCRIBE (`0x8F`) never reaches the policy;
    // the dry run must not give it a verdict the broker never would.
    if action == CheckedAction::Subscribe && !mqtt_core::valid_filter(target) {
        return error(
            400,
            "bad-request",
            "not a valid topic filter: the broker refuses it before authorization",
        );
    }
    let groups: Vec<String> = req
        .param("groups")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|g| !g.is_empty())
        .map(String::from)
        .collect();
    let client = match action {
        CheckedAction::Connect => target.to_string(),
        _ => req
            .param("client")
            .filter(|c| !c.is_empty())
            .unwrap_or(user)
            .to_string(),
    };
    let identity = Identity {
        subject: user.to_string(),
        groups,
    };
    let explanation = if action == CheckedAction::Publish && mqtt_core::is_reserved_topic(target) {
        Explanation {
            allowed: false,
            rule: None,
            reason: RESERVED_REASON.to_string(),
        }
    } else {
        let authorizer = live.borrow().clone();
        authorizer.explain(&identity, &ClientId(client.as_str().into()), action, target)
    };
    let mut notes = vec![
        "groups are as given here; at runtime they come from the authenticator (token claims, \
         the HTTP hook)",
    ];
    if action == CheckedAction::Connect {
        notes.push(
            "the session-owner guard (ADR 0031) applies independently: a client id already \
             bound to another principal's session is refused whatever the policy says",
        );
    }
    (
        200,
        json!({
            "allowed": explanation.allowed,
            "rule": explanation.rule,
            "reason": explanation.reason,
            "user": identity.subject,
            "groups": identity.groups,
            "client_id": client,
            "action": req.param("action"),
            "target": target,
            "notes": notes,
        })
        .to_string(),
    )
}
