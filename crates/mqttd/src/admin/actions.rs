//! Kick and purge (ADR 0081 T7): the operator actions on one client.
//!
//! A persistent session lives on its placement owner (a relocated connection is proxied
//! there, ADR 0005), but a clean session stays on the node the client connected to. So the
//! node asked acts on what it holds; if it holds nothing, it forwards to the owner, then to
//! every other member, node to node under the `peer` role, carrying the caller's subject
//! and role. Both nodes audit it (`admin.request` records the full target, forwarding
//! parameters included). A forwarded request is never forwarded again.

use super::client::{self, Target};
use super::http::{percent_encode, Request};
use super::routes::{error, Answer};
use super::{AdminState, Caller, Role};
use crate::hub::admin::{ActionOutcome, AdminRequest};
use crate::hub::HubCommand;
use serde_json::{json, Value};
use std::fmt::Write;
use tokio::sync::oneshot;

/// Which action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Disconnect; the session stays.
    Kick,
    /// Disconnect and delete the session.
    Purge,
}

impl Action {
    fn name(self) -> &'static str {
        match self {
            Action::Kick => "kick",
            Action::Purge => "purge",
        }
    }
}

/// The query parameter that marks a forwarded request, naming the original caller.
pub const FORWARDED_FOR: &str = "forwarded_for";

/// `POST /admin/v1/kick?client=` / `POST /admin/v1/purge?client=`
///
/// Where the client is: a persistent session lives on its placement owner (a relocated
/// connection is proxied there, ADR 0005), but a clean session stays on the node the
/// client connected to. So this node acts on what it holds first; if it holds nothing, it
/// asks the owner, then every other member, and relays the first that acted. A forwarded
/// request acts locally or answers 404; it is never forwarded again.
pub async fn act(
    state: &AdminState,
    caller: &Caller,
    role: Role,
    req: &Request,
    action: Action,
) -> Answer {
    let Some(access) = state.sessions.as_deref() else {
        return error(
            503,
            "unavailable",
            "session state is not wired on this node",
        );
    };
    let Some(client) = req.param("client").filter(|c| !c.is_empty()) else {
        return error(400, "bad-request", "client is required");
    };
    let forwarded_for = req.param(FORWARDED_FOR);
    let (owner, owner_route) = match access.placement.as_ref() {
        Some(p) => {
            let p = p.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            (p.owner(client).0, p.owner_route(client))
        }
        None => (state.node_id.clone(), None),
    };

    // 1. What this node holds.
    let (tx, rx) = oneshot::channel();
    let request = match action {
        Action::Kick => AdminRequest::Kick {
            client: client.to_string(),
            reply: tx,
        },
        Action::Purge => AdminRequest::Purge {
            client: client.to_string(),
            owner: owner == state.node_id,
            reply: tx,
        },
    };
    if access.hub.send(HubCommand::Admin(request)).is_err() {
        return error(503, "unavailable", "the hub is not running");
    }
    let Ok(ActionOutcome {
        disconnected,
        session_found,
    }) = rx.await
    else {
        return error(503, "unavailable", "the hub dropped the request");
    };
    if disconnected || session_found {
        return (
            200,
            json!({
                "action": action.name(),
                "client_id": client,
                "node": state.node_id,
                "disconnected": disconnected,
                "session_found": session_found,
                "forwarded_for": forwarded_for,
            })
            .to_string(),
        );
    }
    let nothing_here = || {
        error(
            404,
            "not-found",
            &format!(
                "this node ({}) holds no session or connection for {client:?}",
                state.node_id
            ),
        )
    };
    if forwarded_for.is_some() {
        return nothing_here();
    }

    // 2. The owner first, then every other member this node knows.
    ask_others(state, caller, role, action, client, owner_route).await
}

/// Forward `action` to the owner (when it is not this node), then to every other member,
/// and relay the first node that acted; 404 naming every node asked when none did.
async fn ask_others(
    state: &AdminState,
    caller: &Caller,
    role: Role,
    action: Action,
    client: &str,
    owner_route: Option<(mqtt_cluster::NodeId, String)>,
) -> Answer {
    let mut candidates: Vec<(String, String)> = owner_route
        .map(|(id, addr)| (id.0, addr))
        .into_iter()
        .collect();
    let status = state.local_status().await;
    for member in status
        .get("members")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let (Some(id), Some(addr)) = (
            member.get("id").and_then(Value::as_str),
            member.get("addr").and_then(Value::as_str),
        ) else {
            continue;
        };
        if id != state.node_id && !candidates.iter().any(|(c, _)| c == id) {
            candidates.push((id.to_string(), addr.to_string()));
        }
    }
    if candidates.is_empty() {
        return error(
            404,
            "not-found",
            &format!(
                "this node ({}) holds no session or connection for {client:?}",
                state.node_id
            ),
        );
    }
    if state.peers.is_none() {
        return error(
            503,
            "unavailable",
            &format!(
                "this node ({}) holds nothing for {client:?} and cannot reach the other nodes' \
                 admin listeners (no cluster TLS); run the action on the node that has it",
                state.node_id
            ),
        );
    }
    let mut asked = vec![state.node_id.clone()];
    let mut unreachable = Vec::new();
    for (id, addr) in &candidates {
        let (status, body) = forward(state, caller, role, action, client, id, addr).await;
        match status {
            404 => asked.push(id.clone()),
            s if (200..300).contains(&s) => return (status, body),
            _ => unreachable.push(format!("{id} ({status})")),
        }
    }
    let mut message = format!(
        "no node holds a session or connection for {client:?} (asked: {})",
        asked.join(", ")
    );
    if !unreachable.is_empty() {
        let _ = write!(message, "; no answer from: {}", unreachable.join(", "));
    }
    error(404, "not-found", &message)
}

/// `POST /admin/v1/cordon` (`on`) / `POST /admin/v1/uncordon`: this node only. Cordoned,
/// it refuses every new connection at accept and reports not-ready on `/readyz`, so load
/// balancers stop routing to it; connected sessions stay. Not persisted: a restart
/// clears it (ADR 0081 §4).
pub fn cordon(state: &AdminState, on: bool) -> Answer {
    let Some(flag) = state.cordon.as_ref() else {
        return error(503, "unavailable", "cordon is not wired on this node");
    };
    let was = flag.swap(on, std::sync::atomic::Ordering::AcqRel);
    if was != on {
        tracing::warn!(
            node = %state.node_id,
            cordoned = on,
            "{}",
            if on {
                "node CORDONED by an admin action: refusing new connections, reporting \
                 not-ready; connected sessions stay"
            } else {
                "node uncordoned by an admin action: accepting new connections again"
            }
        );
    }
    (
        200,
        json!({
            "node": state.node_id,
            "cordoned": on,
            "changed": was != on,
        })
        .to_string(),
    )
}

/// `GET /admin/v1/log-level`: the configured filter and any override.
pub(super) fn log_level() -> Answer {
    match crate::log_filter::global() {
        Some(f) => (200, json!(f.status()).to_string()),
        None => error(
            503,
            "unavailable",
            "the log filter is not reloadable in this process",
        ),
    }
}

/// `POST /admin/v1/log-level?filter=&ttl=`: replace the log filter for `ttl` seconds
/// (default 600, at most 3600), then restore the configured one. The audit target always
/// keeps logging.
pub(super) fn set_log_level(req: &Request) -> Answer {
    let Some(f) = crate::log_filter::global() else {
        return error(
            503,
            "unavailable",
            "the log filter is not reloadable in this process",
        );
    };
    let Some(filter) = req.param("filter").filter(|v| !v.is_empty()) else {
        return error(
            400,
            "bad-request",
            "filter is required (e.g. mqttd::hub=debug)",
        );
    };
    let ttl = match req.param("ttl") {
        None => 600,
        Some(v) => match v.parse::<u64>() {
            Ok(n) if n > 0 => n,
            _ => {
                return error(
                    400,
                    "bad-request",
                    "ttl must be a positive number of seconds",
                )
            }
        },
    };
    match f.set(filter, std::time::Duration::from_secs(ttl)) {
        Ok(_) => (200, json!(f.status()).to_string()),
        Err(e) => error(400, "bad-request", &e),
    }
}

/// `POST /admin/v1/log-level/reset`: restore the configured filter now.
pub(super) fn reset_log_level() -> Answer {
    let Some(f) = crate::log_filter::global() else {
        return error(
            503,
            "unavailable",
            "the log filter is not reloadable in this process",
        );
    };
    match f.reset() {
        Ok(()) => (200, json!(f.status()).to_string()),
        Err(e) => error(503, "unavailable", &e),
    }
}

/// Send the action to the owner's admin listener and relay its answer.
async fn forward(
    state: &AdminState,
    caller: &Caller,
    role: Role,
    action: Action,
    client: &str,
    owner: &str,
    peer_addr: &str,
) -> Answer {
    let Some(peers) = state.peers.as_deref() else {
        return error(
            503,
            "unavailable",
            &format!(
                "{client:?} is owned by {owner}, and this node cannot reach other admin \
                 listeners (no cluster TLS); run the action there"
            ),
        );
    };
    let Some(addr) = (peers.admin_addr)(owner, peer_addr) else {
        return error(503, "unavailable", &format!("no admin address for {owner}"));
    };
    let path = format!(
        "/admin/v1/{}?client={}&{FORWARDED_FOR}={}&forwarded_role={}&forwarded_from={}",
        action.name(),
        percent_encode(client),
        percent_encode(&caller.subject),
        role.as_str(),
        percent_encode(&state.node_id),
    );
    let target = Target {
        server_name: client::host_of(&addr),
        addr: addr.clone(),
        connector: peers.connector.clone(),
        timeout: peers.timeout,
    };
    match client::call(&target, "POST", &path, Some("")).await {
        Ok((status, body)) => {
            let mut body: Value = serde_json::from_str(&body).unwrap_or_else(|_| json!({}));
            body["forwarded_to"] = json!(owner);
            (status, body.to_string())
        }
        Err(e) => error(
            503,
            "unavailable",
            &format!("{client:?} is owned by {owner}, whose admin listener did not answer: {e}"),
        ),
    }
}
