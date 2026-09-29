//! Clients, sessions, subscribers, backlog and retained messages (ADR 0081 T4).
//!
//! The hub answers from its own state ([`crate::hub::admin`]); this layer adds what
//! lives outside it — the owning node from placement, an offline session's queue depth
//! from the session store, retained messages from the hub's retained store (a handle the
//! hub lends) — so no store I/O runs on the hub loop.
//!
//! Every answer is this node's view: the sessions it holds, the retained messages it
//! caches. `mqttd --admin cluster` lists every node's admin address.

use super::http::Request;
use super::routes::{error, Answer};
use super::AdminState;
use crate::hub::admin::{AdminRequest, SessionFilter};
use crate::hub::HubCommand;
use mqtt_cluster::placement::Placement;
use mqtt_core::ClientId;
use mqtt_storage::SessionStore;
use serde_json::{json, Value};
use std::fmt::Write;
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, oneshot};

/// Default page size when a request names none.
const DEFAULT_LIMIT: usize = 100;
/// The most queued messages counted for one offline session; beyond it the answer says
/// "at least this many" rather than reading the whole log.
const QUEUE_COUNT_CAP: usize = 10_000;

/// What the session endpoints read, beside the hub.
#[derive(Clone)]
pub struct SessionAccess {
    /// The hub's command channel.
    pub hub: mpsc::UnboundedSender<HubCommand>,
    /// The session store, for an offline session's queued messages.
    pub store: Arc<dyn SessionStore>,
    /// Placement, for a session's owning node (`None` on a standalone node).
    pub placement: Option<Arc<RwLock<Placement>>>,
}

impl std::fmt::Debug for SessionAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionAccess").finish_non_exhaustive()
    }
}

fn access(state: &AdminState) -> Result<&SessionAccess, Answer> {
    state.sessions.as_deref().ok_or_else(|| {
        error(
            503,
            "unavailable",
            "session state is not wired on this node",
        )
    })
}

/// A `limit`/`top` parameter: absent → `default`; not a positive integer → 400.
fn count_param(req: &Request, name: &str, default: usize) -> Result<usize, Answer> {
    match req.param(name) {
        None => Ok(default),
        Some(v) => match v.parse::<usize>() {
            Ok(n) if n > 0 => Ok(n.min(crate::hub::admin::MAX_LIMIT)),
            _ => Err(error(
                400,
                "bad-request",
                &format!("{name} must be a positive integer"),
            )),
        },
    }
}

fn non_empty(req: &Request, name: &str) -> Option<String> {
    req.param(name).filter(|v| !v.is_empty()).map(String::from)
}

/// Send a request to the hub and wait for its answer.
async fn ask<T>(
    hub: &mpsc::UnboundedSender<HubCommand>,
    build: impl FnOnce(oneshot::Sender<T>) -> AdminRequest,
) -> Result<T, Answer> {
    let (tx, rx) = oneshot::channel();
    hub.send(HubCommand::Admin(build(tx)))
        .map_err(|_| error(503, "unavailable", "the hub is not running"))?;
    rx.await
        .map_err(|_| error(503, "unavailable", "the hub dropped the request"))
}

fn to_json<T: serde::Serialize>(value: &T) -> Answer {
    match serde_json::to_string(value) {
        Ok(body) => (200, body),
        Err(e) => error(
            503,
            "unavailable",
            &format!("could not encode the answer: {e}"),
        ),
    }
}

/// `GET /admin/v1/clients?prefix=&user=&source=&limit=&cursor=`
pub async fn clients(state: &AdminState, req: &Request) -> Answer {
    let access = match access(state) {
        Ok(a) => a,
        Err(e) => return e,
    };
    let limit = match count_param(req, "limit", DEFAULT_LIMIT) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let filter = SessionFilter {
        prefix: non_empty(req, "prefix"),
        user: non_empty(req, "user"),
        source: non_empty(req, "source"),
    };
    let after = non_empty(req, "cursor");
    match ask(&access.hub, |reply| AdminRequest::Sessions {
        filter,
        after,
        limit,
        reply,
    })
    .await
    {
        Ok(page) => to_json(&page),
        Err(e) => e,
    }
}

/// `GET /admin/v1/session?client=`
pub async fn session(state: &AdminState, req: &Request) -> Answer {
    let access = match access(state) {
        Ok(a) => a,
        Err(e) => return e,
    };
    let Some(client) = non_empty(req, "client") else {
        return error(400, "bad-request", "client is required");
    };
    let owner = access.placement.as_ref().map(|p| {
        p.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .owner(&client)
            .0
    });
    let detail = match ask(&access.hub, |reply| AdminRequest::Session {
        client: client.clone(),
        reply,
    })
    .await
    {
        Ok(d) => d,
        Err(e) => return e,
    };
    let Some(detail) = detail else {
        let mut message = format!(
            "this node ({}) holds no session for {client:?}",
            state.node_id
        );
        if let Some(owner) = owner.as_ref().filter(|o| **o != state.node_id) {
            let _ = write!(message, "; placement puts it on {owner}");
        }
        return error(404, "not-found", &message);
    };
    let mut body = match serde_json::to_value(&detail) {
        Ok(v) => v,
        Err(e) => {
            return error(
                503,
                "unavailable",
                &format!("could not encode the answer: {e}"),
            )
        }
    };
    body["owner_node"] = json!(owner);
    body["node"] = json!(state.node_id);
    // A disconnected session's queue lives in the store, not the hub.
    if !detail.summary.connected && detail.summary.persistent {
        match access
            .store
            .pending(&ClientId(client.as_str().into()), 0, QUEUE_COUNT_CAP + 1)
            .await
        {
            Ok(msgs) => {
                body["queued"] = json!(msgs.len().min(QUEUE_COUNT_CAP));
                body["queued_capped"] = json!(msgs.len() > QUEUE_COUNT_CAP);
            }
            Err(e) => body["queued_error"] = json!(e.to_string()),
        }
    }
    (200, body.to_string())
}

/// `GET /admin/v1/subscribers?topic=&limit=`
pub async fn subscribers(state: &AdminState, req: &Request) -> Answer {
    let access = match access(state) {
        Ok(a) => a,
        Err(e) => return e,
    };
    let Some(topic) = non_empty(req, "topic") else {
        return error(400, "bad-request", "topic is required");
    };
    if topic.contains(['+', '#']) {
        return error(
            400,
            "bad-request",
            "topic is a topic name, not a filter: no + or # wildcards",
        );
    }
    let limit = match count_param(req, "limit", DEFAULT_LIMIT) {
        Ok(n) => n,
        Err(e) => return e,
    };
    match ask(&access.hub, |reply| AdminRequest::Subscribers {
        topic,
        limit,
        reply,
    })
    .await
    {
        Ok(list) => to_json(&list),
        Err(e) => e,
    }
}

/// `GET /admin/v1/backlog?top=`
pub async fn backlog(state: &AdminState, req: &Request) -> Answer {
    let access = match access(state) {
        Ok(a) => a,
        Err(e) => return e,
    };
    let top = match count_param(req, "top", 20) {
        Ok(n) => n,
        Err(e) => return e,
    };
    match ask(&access.hub, |reply| AdminRequest::Backlog { top, reply }).await {
        Ok(list) => to_json(&json!({ "sessions": list })),
        Err(e) => e,
    }
}

/// `GET /admin/v1/retained?prefix=&limit=&cursor=`: retained messages whose topic starts
/// with `prefix`, by topic, without payloads; plus the count and bytes of all of them.
pub async fn retained(state: &AdminState, req: &Request) -> Answer {
    let access = match access(state) {
        Ok(a) => a,
        Err(e) => return e,
    };
    let limit = match count_param(req, "limit", DEFAULT_LIMIT) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let prefix = req.param("prefix").unwrap_or_default().to_string();
    let after = non_empty(req, "cursor");
    let store = match ask(&access.hub, |reply| AdminRequest::RetainedStore { reply }).await {
        Ok(s) => s,
        Err(e) => return e,
    };
    let all = match store.all().await {
        Ok(all) => all,
        Err(e) => return error(503, "unavailable", &format!("retained store: {e}")),
    };
    let mut matching: Vec<_> = all
        .iter()
        .filter(|m| m.topic.as_str().starts_with(prefix.as_str()))
        .collect();
    matching.sort_by(|a, b| a.topic.as_str().cmp(b.topic.as_str()));
    let count = matching.len();
    let bytes: usize = matching.iter().map(|m| m.payload.len()).sum();
    let page: Vec<Value> = matching
        .iter()
        .filter(|m| after.as_deref().is_none_or(|a| m.topic.as_str() > a))
        .take(limit + 1)
        .map(|m| {
            json!({
                "topic": m.topic.as_str(),
                "qos": m.qos as u8,
                "payload_bytes": m.payload.len(),
                "expires_at": m.expires_at,
            })
        })
        .collect();
    let more = page.len() > limit;
    let page: Vec<Value> = page.into_iter().take(limit).collect();
    let next_cursor = if more {
        page.last()
            .and_then(|m| m["topic"].as_str())
            .map(String::from)
    } else {
        None
    };
    (
        200,
        json!({
            "prefix": prefix,
            "count": count,
            "payload_bytes": bytes,
            "retained": page,
            "next_cursor": next_cursor,
        })
        .to_string(),
    )
}
