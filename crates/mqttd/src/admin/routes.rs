//! The `/admin/v1/` endpoints and the role each needs.

use super::http::{Request, MAX_BODY, MAX_RULES_BODY};
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
        path: "/admin/v1/config",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "POST",
        path: "/admin/v1/reload",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/log-level",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "POST",
        path: "/admin/v1/log-level",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "POST",
        path: "/admin/v1/log-level/reset",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "POST",
        path: "/admin/v1/cordon",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "POST",
        path: "/admin/v1/uncordon",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "POST",
        path: "/admin/v1/kick",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "POST",
        path: "/admin/v1/purge",
        min_role: Role::Operator,
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
    Endpoint {
        method: "GET",
        path: "/admin/v1/rules",
        min_role: Role::Viewer,
    },
    Endpoint {
        method: "PUT",
        path: "/admin/v1/rules",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "GET",
        path: "/admin/v1/rules/source",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "POST",
        path: "/admin/v1/rules/check",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "POST",
        path: "/admin/v1/rules/test",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "PUT",
        path: "/admin/v1/rule",
        min_role: Role::Operator,
    },
    Endpoint {
        method: "DELETE",
        path: "/admin/v1/rule",
        min_role: Role::Operator,
    },
];

/// The endpoints whose body can be a whole rules file (ADR 0084): up to
/// [`MAX_RULES_BODY`] for a caller whose role may call them.
const RULES_BODIES: &[(&str, &str)] = &[
    ("PUT", "/admin/v1/rules"),
    ("POST", "/admin/v1/rules/check"),
    ("POST", "/admin/v1/rules/test"),
    ("PUT", "/admin/v1/rule"),
];

/// The most body bytes a request for `method path` may carry from a caller with `role`,
/// decided before the body is read: [`MAX_RULES_BODY`] for a rules route the role may
/// call, [`MAX_BODY`] for everything and everyone else — so a certificate in no list, or
/// a viewer, never makes the node buffer a rules file.
#[must_use]
pub fn body_limit(role: Option<Role>, method: &str, path: &str) -> usize {
    let may_call = ENDPOINTS
        .iter()
        .find(|e| e.method == method && e.path == path)
        .zip(role)
        .is_some_and(|(e, role)| role >= e.min_role);
    if may_call && RULES_BODIES.contains(&(method, path)) {
        MAX_RULES_BODY
    } else {
        MAX_BODY
    }
}

/// The actions a node may forward to a session's owner as the `peer` role.
const FORWARDABLE: &[&str] = &["/admin/v1/kick", "/admin/v1/purge"];

/// A read that may be asked of one node or, with `scope=cluster`, of all of them.
async fn scoped(
    state: &AdminState,
    caller: &Caller,
    role: Role,
    req: &Request,
    path: &str,
) -> Answer {
    use super::{scope, sessions};
    let cluster = match scope::wants_cluster(req) {
        Ok(c) => c,
        Err(e) => return e,
    };
    match (path, cluster) {
        ("/admin/v1/clients", false) => sessions::clients(state, req).await,
        ("/admin/v1/clients", true) => scope::clients(state, caller, role, req).await,
        ("/admin/v1/session", false) => sessions::session(state, req).await,
        ("/admin/v1/session", true) => scope::session(state, caller, role, req).await,
        ("/admin/v1/subscribers", false) => sessions::subscribers(state, req).await,
        (_, true) => scope::subscribers(state, caller, role, req).await,
        _ => error(404, "not-found", "no such admin endpoint"),
    }
}

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
    // A node forwards an operator's action to the session's owner under its own
    // certificate (T7): the `peer` role may call exactly these, and only as a forward.
    // Likewise a cluster-scope read (T17): the `peer` role may read these, and only as
    // a forward, which is always answered at node scope.
    let forwarded = role == Role::Peer
        && (FORWARDABLE.contains(&endpoint.path)
            || super::scope::FORWARDABLE_READS.contains(&endpoint.path))
        && req.param(super::actions::FORWARDED_FOR).is_some();
    if role < endpoint.min_role && !forwarded {
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
        "/admin/v1/config" => super::config::config(state).await,
        "/admin/v1/reload" => super::config::reload(state).await,
        "/admin/v1/log-level" if req.method == "GET" => super::actions::log_level(),
        "/admin/v1/log-level" => super::actions::set_log_level(req),
        "/admin/v1/log-level/reset" => super::actions::reset_log_level(),
        "/admin/v1/cordon" => super::actions::cordon(state, true),
        "/admin/v1/uncordon" => super::actions::cordon(state, false),
        "/admin/v1/kick" => {
            super::actions::act(state, caller, role, req, super::actions::Action::Kick).await
        }
        "/admin/v1/purge" => {
            super::actions::act(state, caller, role, req, super::actions::Action::Purge).await
        }
        "/admin/v1/authz" => super::authz::check(state, req),
        "/admin/v1/clients" | "/admin/v1/session" | "/admin/v1/subscribers" => {
            scoped(state, caller, role, req, endpoint.path).await
        }
        "/admin/v1/backlog" => super::sessions::backlog(state, req).await,
        "/admin/v1/retained" => super::sessions::retained(state, req).await,
        "/admin/v1/rules" if req.method == "GET" => super::rules::list(state, caller, role).await,
        "/admin/v1/rules" => super::rules::put_file(state, caller, req).await,
        "/admin/v1/rules/source" => super::rules::source(state).await,
        "/admin/v1/rules/check" => super::rules::check(state, req).await,
        "/admin/v1/rules/test" => super::rules::test(state, req).await,
        "/admin/v1/rule" if req.method == "PUT" => super::rules::put_rule(state, caller, req).await,
        "/admin/v1/rule" => super::rules::delete_rule(state, caller, req).await,
        _ => error(404, "not-found", "no such admin endpoint"),
    }
}
