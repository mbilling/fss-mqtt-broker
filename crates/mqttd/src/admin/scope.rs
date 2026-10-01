//! Cluster-wide reads (ADR 0081 T17): `scope=cluster` on `clients`, `session` and
//! `subscribers`, so an operator does not need to know which node holds a client.
//!
//! The node asked answers for itself, asks every other member for its node-scope answer
//! in parallel — over the admin listeners, presenting its cluster certificate with a
//! `forwarded_for` marker, as a forwarded kick does — and merges the answers. Every
//! cluster-scope answer carries `nodes`: one row per node with `replied`, and the reason
//! when a node did not answer, so a partial answer is never mistaken for a complete one.
//! A forwarded request is always answered at node scope; it never fans out again.

use super::actions::FORWARDED_FOR;
use super::client::{self, Target};
use super::http::{percent_encode, Request};
use super::routes::{error, Answer};
use super::{AdminState, Caller, Role};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::fmt::Write;

/// The query parameter that selects the scope: `node` (the default) or `cluster`.
pub const SCOPE: &str = "scope";

/// The reads a node may forward to its peers as the `peer` role.
pub const FORWARDABLE_READS: &[&str] = &[
    "/admin/v1/clients",
    "/admin/v1/session",
    "/admin/v1/subscribers",
];

/// Whether this request asks for the cluster scope. A forwarded request never does, so
/// a fan-out cannot recurse.
///
/// # Errors
/// A 400 for a scope that is neither `node` nor `cluster`.
pub fn wants_cluster(req: &Request) -> Result<bool, Answer> {
    match req.param(SCOPE) {
        None | Some("" | "node") => Ok(false),
        Some("cluster") => Ok(req.param(FORWARDED_FOR).is_none()),
        Some(_) => Err(error(400, "bad-request", "scope must be node or cluster")),
    }
}

/// One node's node-scope answer, or why there is none.
#[derive(Debug, Clone)]
pub struct NodeAnswer {
    pub node_id: String,
    pub outcome: Result<(u16, Value), String>,
}

impl NodeAnswer {
    fn body(&self, status: u16) -> Option<&Value> {
        match &self.outcome {
            Ok((s, body)) if *s == status => Some(body),
            _ => None,
        }
    }
}

/// `local` (this node's node-scope answer), then every other member's answer to the same
/// request, by node id.
async fn gather(
    state: &AdminState,
    caller: &Caller,
    role: Role,
    req: &Request,
    local: Answer,
) -> Vec<NodeAnswer> {
    let mut answers = vec![NodeAnswer {
        node_id: state.node_id.clone(),
        outcome: Ok((
            local.0,
            serde_json::from_str(&local.1).unwrap_or(Value::Null),
        )),
    }];
    let status = state.local_status().await;
    let members: Vec<(String, Option<String>)> = status
        .get("members")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|m| {
            let id = m.get("id")?.as_str()?;
            (id != state.node_id).then(|| {
                (
                    id.to_string(),
                    m.get("addr").and_then(Value::as_str).map(String::from),
                )
            })
        })
        .collect();
    let path = forwarded_path(state, caller, role, req);
    let asks = members.into_iter().map(|(id, peer_addr)| {
        let path = path.clone();
        async move {
            let outcome = ask(state, &id, peer_addr.as_deref(), &path).await;
            NodeAnswer {
                node_id: id,
                outcome,
            }
        }
    });
    let mut others = futures_util::future::join_all(asks).await;
    others.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    answers.extend(others);
    answers
}

/// The request as a peer receives it: the same path and parameters, without `scope`,
/// plus who it is forwarded for (audited on the peer).
fn forwarded_path(state: &AdminState, caller: &Caller, role: Role, req: &Request) -> String {
    let mut path = req.path.clone();
    let params = req
        .query
        .iter()
        .filter(|(k, _)| k != SCOPE && k != FORWARDED_FOR)
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .chain([
            (FORWARDED_FOR, caller.subject.as_str()),
            ("forwarded_role", role.as_str()),
            ("forwarded_from", state.node_id.as_str()),
        ]);
    for (i, (k, v)) in params.enumerate() {
        path.push(if i == 0 { '?' } else { '&' });
        let _ = write!(path, "{}={}", percent_encode(k), percent_encode(v));
    }
    path
}

async fn ask(
    state: &AdminState,
    node_id: &str,
    peer_addr: Option<&str>,
    path: &str,
) -> Result<(u16, Value), String> {
    let peers = state
        .peers
        .as_deref()
        .ok_or("not queryable: this node has no cluster TLS to present to its peers")?;
    let peer_addr =
        peer_addr.ok_or("not queryable: no cluster-bus address known for this member")?;
    let addr = (peers.admin_addr)(node_id, peer_addr)
        .ok_or_else(|| format!("not queryable: no admin address for {peer_addr}"))?;
    let target = Target {
        server_name: client::host_of(&addr),
        addr,
        connector: peers.connector.clone(),
        timeout: peers.timeout,
    };
    let (status, body) = client::call(&target, "GET", path, None).await?;
    let body = serde_json::from_str(&body).map_err(|_| "answered with a body that is not JSON")?;
    Ok((status, body))
}

/// The `nodes` rows: which nodes answered, and why the others did not. `expected` is the
/// status that counts as an answer; `also` statuses count too (a 404 from `session`).
fn node_rows(answers: &[NodeAnswer], also: &[u16]) -> Value {
    answers
        .iter()
        .map(|a| match &a.outcome {
            Ok((200, _)) => json!({ "node_id": a.node_id, "replied": true }),
            Ok((status, _)) if also.contains(status) => {
                json!({ "node_id": a.node_id, "replied": true })
            }
            Ok((status, body)) => {
                let code = body
                    .pointer("/error/code")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                json!({
                    "node_id": a.node_id,
                    "replied": false,
                    "error": format!("refused: {status} {code}").trim_end().to_string(),
                })
            }
            Err(e) => json!({ "node_id": a.node_id, "replied": false, "error": e }),
        })
        .collect()
}

fn text<'a>(row: &'a Value, key: &str) -> &'a str {
    row.get(key).and_then(Value::as_str).unwrap_or("")
}

/// Rows from each answering node's `key` array, each tagged with its `node`, sorted by
/// `(client_id, node)`.
fn tagged_rows(answers: &[NodeAnswer], key: &str) -> Vec<Value> {
    let mut rows = Vec::new();
    for a in answers {
        let Some(body) = a.body(200) else { continue };
        for row in body
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let mut row = row.clone();
            row["node"] = json!(a.node_id);
            rows.push(row);
        }
    }
    rows.sort_by(
        |a, b| match text(a, "client_id").cmp(text(b, "client_id")) {
            Ordering::Equal => text(a, "node").cmp(text(b, "node")),
            other => other,
        },
    );
    rows
}

/// Merge `clients` pages. Each node was asked for `limit` sessions after the same cursor,
/// so the first `limit` of the merged rows are the cluster's first `limit`. A client id
/// held on two nodes is never split across pages (the cursor is a client id), so a page
/// can run a row or two past `limit` to keep both.
#[must_use]
pub fn merge_clients(answer_by: &str, answers: &[NodeAnswer], limit: usize) -> Value {
    let mut rows = tagged_rows(answers, "sessions");
    let mut more = answers
        .iter()
        .filter_map(|a| a.body(200))
        .any(|b| !b["next_cursor"].is_null());
    if rows.len() > limit {
        more = true;
        let last = text(&rows[limit - 1], "client_id").to_string();
        let keep = limit
            + rows[limit..]
                .iter()
                .take_while(|r| text(r, "client_id") == last)
                .count();
        rows.truncate(keep);
    }
    let matched: u64 = answers
        .iter()
        .filter_map(|a| a.body(200))
        .filter_map(|b| b["matched"].as_u64())
        .sum();
    let next_cursor = rows
        .last()
        .filter(|_| more)
        .map(|r| text(r, "client_id").to_string());
    json!({
        "scope": "cluster",
        "answered_by": answer_by,
        "matched": matched,
        "next_cursor": next_cursor,
        "sessions": rows,
        "nodes": node_rows(answers, &[]),
    })
}

/// Merge `subscribers` lists: every node's subscribers to the topic, tagged by node.
#[must_use]
pub fn merge_subscribers(
    answer_by: &str,
    answers: &[NodeAnswer],
    topic: &str,
    limit: usize,
) -> Value {
    let mut rows = tagged_rows(answers, "subscribers");
    let mut truncated = answers
        .iter()
        .filter_map(|a| a.body(200))
        .any(|b| b["truncated"] == true);
    if rows.len() > limit {
        truncated = true;
        rows.truncate(limit);
    }
    json!({
        "scope": "cluster",
        "answered_by": answer_by,
        "topic": topic,
        "subscribers": rows,
        "truncated": truncated,
        "nodes": node_rows(answers, &[]),
    })
}

/// Merge `session` answers: the copy an operator means — the connected one, else the
/// owner's, else the first by node — with `found_on` naming every node holding one. A 404
/// names every node asked, and any that did not answer.
#[must_use]
pub fn merge_session(answer_by: &str, answers: &[NodeAnswer], client: &str) -> Answer {
    let found: Vec<(&str, &Value)> = answers
        .iter()
        .filter_map(|a| a.body(200).map(|b| (a.node_id.as_str(), b)))
        .collect();
    let silent: Vec<String> = answers
        .iter()
        .filter(|a| a.body(200).is_none() && a.body(404).is_none())
        .map(|a| a.node_id.clone())
        .collect();
    let primary = found
        .iter()
        .find(|(_, b)| b["connected"] == true)
        .or_else(|| found.iter().find(|(n, b)| b["owner_node"] == *n))
        .or_else(|| found.first());
    let Some((_, primary)) = primary else {
        let asked: Vec<&str> = answers
            .iter()
            .filter(|a| a.body(404).is_some())
            .map(|a| a.node_id.as_str())
            .collect();
        let mut message = format!(
            "no node holds a session for {client:?} (asked: {})",
            asked.join(", ")
        );
        if !silent.is_empty() {
            let _ = write!(message, "; no answer from: {}", silent.join(", "));
        }
        let mut body: Value = serde_json::from_str(&error(404, "not-found", &message).1)
            .unwrap_or_else(|_| json!({}));
        body["nodes"] = node_rows(answers, &[404]);
        return (404, body.to_string());
    };
    let mut body = (*primary).clone();
    body["scope"] = json!("cluster");
    body["answered_by"] = json!(answer_by);
    body["found_on"] = json!(found.iter().map(|(n, _)| *n).collect::<Vec<_>>());
    body["nodes"] = node_rows(answers, &[404]);
    (200, body.to_string())
}

/// `GET /admin/v1/clients?scope=cluster&…`
pub async fn clients(state: &AdminState, caller: &Caller, role: Role, req: &Request) -> Answer {
    let limit = match super::sessions::count_param(req, "limit", super::sessions::DEFAULT_LIMIT) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let local = super::sessions::clients(state, req).await;
    if local.0 == 400 {
        return local;
    }
    let answers = gather(state, caller, role, req, local).await;
    (
        200,
        merge_clients(&state.node_id, &answers, limit).to_string(),
    )
}

/// `GET /admin/v1/session?client=…&scope=cluster`
pub async fn session(state: &AdminState, caller: &Caller, role: Role, req: &Request) -> Answer {
    let local = super::sessions::session(state, req).await;
    if local.0 == 400 {
        return local;
    }
    let client = req.param("client").unwrap_or_default().to_string();
    let answers = gather(state, caller, role, req, local).await;
    merge_session(&state.node_id, &answers, &client)
}

/// `GET /admin/v1/subscribers?topic=…&scope=cluster`
pub async fn subscribers(state: &AdminState, caller: &Caller, role: Role, req: &Request) -> Answer {
    let limit = match super::sessions::count_param(req, "limit", super::sessions::DEFAULT_LIMIT) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let local = super::sessions::subscribers(state, req).await;
    if local.0 == 400 {
        return local;
    }
    let topic = req.param("topic").unwrap_or_default().to_string();
    let answers = gather(state, caller, role, req, local).await;
    (
        200,
        merge_subscribers(&state.node_id, &answers, &topic, limit).to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(node: &str, body: Value) -> NodeAnswer {
        NodeAnswer {
            node_id: node.into(),
            outcome: Ok((200, body)),
        }
    }

    fn page(ids: &[&str], next: Option<&str>) -> Value {
        json!({
            "sessions": ids.iter().map(|c| json!({ "client_id": c })).collect::<Vec<_>>(),
            "next_cursor": next,
            "matched": ids.len(),
        })
    }

    fn ids(v: &Value, key: &str) -> Vec<String> {
        v[key]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| format!("{}@{}", text(r, "client_id"), text(r, "node")))
            .collect()
    }

    #[test]
    fn client_pages_merge_by_client_id_and_page_across_the_cluster() {
        let answers = [
            ok("n1", page(&["a", "d"], None)),
            ok("n2", page(&["b", "c"], Some("c"))),
            NodeAnswer {
                node_id: "n3".into(),
                outcome: Err("connect failed".into()),
            },
        ];
        let v = merge_clients("n1", &answers, 3);
        assert_eq!(ids(&v, "sessions"), ["a@n1", "b@n2", "c@n2"]);
        assert_eq!(v["next_cursor"], "c", "{v}");
        assert_eq!(v["matched"], 4);
        assert_eq!(v["nodes"][2]["replied"], false);
        assert_eq!(v["nodes"][2]["error"], "connect failed");

        // Everything fits and no node has more: no cursor.
        let v = merge_clients("n1", &answers[..1], 3);
        assert_eq!(v["next_cursor"], Value::Null, "{v}");

        // A client id on two nodes is not split by the page boundary.
        let answers = [
            ok("n1", page(&["a", "b"], None)),
            ok("n2", page(&["b"], None)),
        ];
        let v = merge_clients("n1", &answers, 2);
        assert_eq!(ids(&v, "sessions"), ["a@n1", "b@n1", "b@n2"]);
        assert_eq!(v["next_cursor"], "b");
    }

    #[test]
    fn subscribers_merge_with_their_node_and_honour_the_limit() {
        let list = |c: &[&str], truncated: bool| {
            json!({
                "topic": "t",
                "subscribers": c.iter().map(|c| json!({ "client_id": c, "filter": "#" })).collect::<Vec<_>>(),
                "truncated": truncated,
            })
        };
        let answers = [
            ok("n1", list(&["x"], false)),
            ok("n2", list(&["w", "y"], false)),
        ];
        let v = merge_subscribers("n2", &answers, "t", 10);
        assert_eq!(ids(&v, "subscribers"), ["w@n2", "x@n1", "y@n2"]);
        assert_eq!(v["truncated"], false);
        let v = merge_subscribers("n2", &answers, "t", 2);
        assert_eq!(v["truncated"], true);
        assert_eq!(v["subscribers"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn a_session_is_the_connected_copy_else_the_owners_and_a_miss_names_every_node() {
        let not_here = |node: &str| NodeAnswer {
            node_id: node.into(),
            outcome: Ok((404, json!({ "error": { "code": "not-found" } }))),
        };
        let copy = |node: &str, connected: bool| {
            ok(
                node,
                json!({ "client_id": "c", "node": node, "connected": connected, "owner_node": "n2" }),
            )
        };
        let (status, body) = merge_session(
            "n1",
            &[not_here("n1"), copy("n2", false), copy("n3", true)],
            "c",
        );
        let body: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(status, 200);
        assert_eq!(body["node"], "n3", "the connected copy wins: {body}");
        assert_eq!(body["found_on"], json!(["n2", "n3"]));
        assert_eq!(body["nodes"][0]["replied"], true, "a 404 is an answer");

        let (_, body) = merge_session(
            "n1",
            &[not_here("n1"), copy("n2", false), copy("n3", false)],
            "c",
        );
        let body: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(body["node"], "n2", "else the owner's copy: {body}");

        let silent = NodeAnswer {
            node_id: "n3".into(),
            outcome: Err("timed out".into()),
        };
        let (status, body) = merge_session("n1", &[not_here("n1"), not_here("n2"), silent], "c");
        let body: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(status, 404);
        let message = body["error"]["message"].as_str().unwrap();
        assert!(message.contains("asked: n1, n2"), "{message}");
        assert!(message.contains("no answer from: n3"), "{message}");
        assert_eq!(body["nodes"][2]["replied"], false);
    }

    #[test]
    fn only_node_and_cluster_are_scopes_and_a_forward_is_always_node_scope() {
        let req = |q: &[(&str, &str)]| Request {
            method: "GET".into(),
            path: "/admin/v1/clients".into(),
            query: q.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect(),
            body: Vec::new(),
        };
        assert!(!wants_cluster(&req(&[])).unwrap());
        assert!(!wants_cluster(&req(&[("scope", "node")])).unwrap());
        assert!(wants_cluster(&req(&[("scope", "cluster")])).unwrap());
        assert!(!wants_cluster(&req(&[("scope", "cluster"), (FORWARDED_FOR, "CN=x")])).unwrap());
        assert_eq!(
            wants_cluster(&req(&[("scope", "everywhere")]))
                .unwrap_err()
                .0,
            400
        );
    }
}
