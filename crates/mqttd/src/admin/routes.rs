//! The `/admin/v1/` endpoints and the role each needs.

use super::http::Request;
use super::{AdminState, Caller, Role};
use serde_json::{json, Value};

/// A `(status, JSON body)` answer.
pub type Answer = (u16, String);

/// A refusal: `{"error":{"code":…,"message":…}}`. `code` is stable; scripts match on it.
pub fn error(status: u16, code: &str, message: &str) -> Answer {
    (
        status,
        json!({ "error": { "code": code, "message": message } }).to_string(),
    )
}

fn ok(body: &Value) -> Answer {
    (200, body.to_string())
}

/// One endpoint: its method, path, and the least role that may call it.
struct Endpoint {
    method: &'static str,
    path: &'static str,
    min_role: Role,
}

/// Every endpoint. A path in this table answered with the wrong method is a 405; a path
/// not in it is a 404.
const ENDPOINTS: &[Endpoint] = &[
    Endpoint {
        method: "GET",
        path: "/admin/v1/whoami",
        min_role: Role::Peer,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/node",
        min_role: Role::Peer,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/cluster",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/placement",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/authz",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/clients",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/session",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/subscribers",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/backlog",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/retained",
        min_role: Role::Viewer,
    },
];

/// Route one authorized request.
pub async fn route(state: &AdminState, caller: &Caller, role: Role, req: &Request) -> Answer {
    let mut path_known = false;
    let endpoint = ENDPOINTS.iter().find(|e| {
        let hit = e.path == req.path;
        path_known |= hit;
        hit && e.method == req.method
    });
    let Some(endpoint) = endpoint else {
        return if path_known {
            error(
                405,
                "method-not-allowed",
                "this endpoint does not take that method",
            )
        } else {
            error(404, "not-found", "no such admin endpoint")
        };
    };
    if role < endpoint.min_role {
        return error(
            403,
            "forbidden",
            &format!(
                "this endpoint needs the {} role; this certificate has {}",
                endpoint.min_role.as_str(),
                role.as_str()
            ),
        );
    }
    match endpoint.path {
        "/admin/v1/whoami" => ok(&json!({
            "subject": caller.subject,
            "cn": caller.cn,
            "role": role.as_str(),
            "node_id": state.node_id,
        })),
        "/admin/v1/node" => ok(&state.local_status().await),
        "/admin/v1/cluster" => super::cluster::cluster(state).await,
        "/admin/v1/placement" => super::cluster::placement(state).await,
        "/admin/v1/authz" => super::authz::check(state, req),
        "/admin/v1/clients" => super::sessions::clients(state, req).await,
        "/admin/v1/session" => super::sessions::session(state, req).await,
        "/admin/v1/subscribers" => super::sessions::subscribers(state, req).await,
        "/admin/v1/backlog" => super::sessions::backlog(state, req).await,
        "/admin/v1/retained" => super::sessions::retained(state, req).await,
        _ => error(404, "not-found", "no such admin endpoint"),
    }
}
