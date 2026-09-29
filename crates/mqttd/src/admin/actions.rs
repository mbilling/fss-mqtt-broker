//! Kick and purge (ADR 0081 T7): the operator actions on one client.
//!
//! A session lives on its placement owner, and a relocated connection is proxied there
//! (ADR 0005), so the owner's hub is where both actions must run. A request that lands on
//! another node is forwarded to the owner's admin listener, node to node under the `peer`
//! role, carrying the caller's subject and role; both nodes audit it (`admin.request`
//! records the full target, forwarding parameters included). A forwarded request is
//! never forwarded again: the owner acts on what it holds.

use super::client::{self, Target};
use super::http::{percent_encode, Request};
use super::routes::{error, Answer};
use super::{AdminState, Caller, Role};
use crate::hub::admin::{ActionOutcome, AdminRequest};
use crate::hub::HubCommand;
use serde_json::{json, Value};
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
    if forwarded_for.is_none() {
        let owner = access.placement.as_ref().and_then(|p| {
            p.read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .owner_route(client)
        });
        if let Some((owner, peer_addr)) = owner {
            return forward(state, caller, role, action, client, &owner.0, &peer_addr).await;
        }
    }
    let (tx, rx) = oneshot::channel();
    let request = match action {
        Action::Kick => AdminRequest::Kick {
            client: client.to_string(),
            reply: tx,
        },
        Action::Purge => AdminRequest::Purge {
            client: client.to_string(),
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
    if !disconnected && !session_found {
        return error(
            404,
            "not-found",
            &format!(
                "this node ({}) holds no session or connection for {client:?}",
                state.node_id
            ),
        );
    }
    (
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
    )
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
