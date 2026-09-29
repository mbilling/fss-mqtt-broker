//! The cluster view (ADR 0081 §2, as amended): any node answers for all of them.
//!
//! The answering node reads its own membership from `/statusz`, asks every other member's
//! admin listener for `/admin/v1/node` in parallel — node to node over mTLS, presenting its
//! cluster certificate, which the peer admits as the `peer` role — and merges the answers.
//! A node that does not answer is listed as not replying, with the reason, never dropped
//! and never shown as healthy: a partitioned node is exactly the one an operator needs to
//! see.
//!
//! This goes through the admin listeners rather than the peer bus on purpose. The peer
//! bus is the data plane: a new frame there needs a protocol version negotiated per link,
//! and the admin plane should not share a version gate (or a failure) with message
//! delivery.

use super::client::{self, Target};
use super::routes::Answer;
use super::AdminState;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_rustls::TlsConnector;

/// How long the answering node waits for each peer.
pub const PEER_TIMEOUT: Duration = Duration::from_secs(3);

/// Maps a member `(node id, cluster-bus address)` to its admin listener `host:port`.
pub type AdminAddr = Arc<dyn Fn(&str, &str) -> Option<String> + Send + Sync>;

/// How this node reaches its peers' admin listeners.
#[derive(Clone)]
pub struct PeerAccess {
    pub(super) connector: TlsConnector,
    pub(super) admin_addr: AdminAddr,
    pub(super) timeout: Duration,
}

impl std::fmt::Debug for PeerAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerAccess").finish_non_exhaustive()
    }
}

impl PeerAccess {
    /// `connector` presents this node's cluster certificate and trusts the CAs that
    /// issue admin server certificates; `admin_addr` finds a peer's admin listener.
    #[must_use]
    pub fn new(connector: TlsConnector, admin_addr: AdminAddr) -> Self {
        Self {
            connector,
            admin_addr,
            timeout: PEER_TIMEOUT,
        }
    }

    /// The production mapping: the host of the peer's cluster-bus address, and `port`.
    #[must_use]
    pub fn same_host(port: u16) -> AdminAddr {
        Arc::new(move |_, peer_addr| {
            let host = client::host_of(peer_addr);
            if host.is_empty() {
                return None;
            }
            Some(if host.contains(':') {
                format!("[{host}]:{port}")
            } else {
                format!("{host}:{port}")
            })
        })
    }
}

/// One member as this node's membership view lists it.
struct Member {
    id: String,
    peer_addr: Option<String>,
}

/// What one node said, or why it said nothing.
enum Reply {
    Status {
        body: Value,
        elapsed_ms: u128,
    },
    Silent {
        admin_addr: Option<String>,
        reason: String,
    },
}

/// Ask every member for its state; `(self status, [(member, reply)])`.
async fn gather(state: &AdminState) -> (Value, Vec<(String, Option<String>, Reply)>) {
    let local = state.local_status().await;
    let members: Vec<Member> = local
        .get("members")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|m| {
                    Some(Member {
                        id: m.get("id")?.as_str()?.to_string(),
                        peer_addr: m.get("addr").and_then(Value::as_str).map(String::from),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let mut asks = Vec::new();
    for member in members {
        if member.id == state.node_id {
            continue;
        }
        let peers = state.peers.clone();
        asks.push(async move {
            let reply = ask(peers.as_deref(), &member).await;
            (member.id, member.peer_addr, reply)
        });
    }
    let mut replies = futures_util::future::join_all(asks).await;
    replies.sort_by(|a, b| a.0.cmp(&b.0));
    (local, replies)
}

async fn ask(peers: Option<&PeerAccess>, member: &Member) -> Reply {
    let Some(peers) = peers else {
        return Reply::Silent {
            admin_addr: None,
            reason: "not queryable: this node has no cluster TLS to present to its peers".into(),
        };
    };
    let Some(peer_addr) = &member.peer_addr else {
        return Reply::Silent {
            admin_addr: None,
            reason: "not queryable: no cluster-bus address known for this member".into(),
        };
    };
    let Some(addr) = (peers.admin_addr)(&member.id, peer_addr) else {
        return Reply::Silent {
            admin_addr: None,
            reason: format!("not queryable: no admin address for {peer_addr}"),
        };
    };
    let target = Target {
        server_name: client::host_of(&addr),
        addr: addr.clone(),
        connector: peers.connector.clone(),
        timeout: peers.timeout,
    };
    let started = Instant::now();
    match client::call(&target, "GET", "/admin/v1/node", None).await {
        Ok((200, body)) => match serde_json::from_str::<Value>(&body) {
            Ok(body) => Reply::Status {
                body,
                elapsed_ms: started.elapsed().as_millis(),
            },
            Err(_) => Reply::Silent {
                admin_addr: Some(addr),
                reason: "answered with a body that is not JSON".into(),
            },
        },
        Ok((status, body)) => {
            let code = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| {
                    v.pointer("/error/code")
                        .and_then(Value::as_str)
                        .map(String::from)
                })
                .unwrap_or_default();
            Reply::Silent {
                admin_addr: Some(addr),
                reason: format!("refused: {status} {code}"),
            }
        }
        Err(e) => Reply::Silent {
            admin_addr: Some(addr),
            reason: e,
        },
    }
}

/// The per-node row: the fields an operator compares across a fleet.
fn row(node_id: &str, admin_addr: Option<&str>, reply: &Reply, is_self: bool) -> Value {
    let mut r = Map::new();
    r.insert("node_id".into(), json!(node_id));
    match reply {
        Reply::Silent {
            admin_addr: a,
            reason,
        } => {
            r.insert("replied".into(), json!(false));
            r.insert("admin_addr".into(), json!(a.as_deref().or(admin_addr)));
            r.insert("error".into(), json!(reason));
        }
        Reply::Status { body, elapsed_ms } => {
            r.insert("replied".into(), json!(true));
            if !is_self {
                r.insert("admin_addr".into(), json!(admin_addr));
            }
            r.insert("elapsed_ms".into(), json!(elapsed_ms));
            let pick = |path: &str| body.pointer(path).cloned().unwrap_or(Value::Null);
            r.insert("version".into(), pick("/version"));
            r.insert("ready".into(), pick("/ready"));
            r.insert("live".into(), pick("/live"));
            r.insert("cluster_id".into(), pick("/cluster_id"));
            r.insert(
                "members".into(),
                json!(body.get("members").and_then(Value::as_array).map(Vec::len)),
            );
            r.insert("lease_leader".into(), pick("/lease/leader"));
            r.insert("lease_epoch".into(), pick("/lease/epoch"));
            let lag = match (
                body.pointer("/lease/replica_groups/tracked")
                    .and_then(Value::as_u64),
                body.pointer("/lease/replica_groups/current")
                    .and_then(Value::as_u64),
            ) {
                (Some(t), Some(c)) => json!(t.saturating_sub(c)),
                _ => Value::Null,
            };
            r.insert("replica_lag_groups".into(), lag);
            r.insert(
                "under_replicated".into(),
                pick("/replication/under_replicated"),
            );
            r.insert("brownout".into(), pick("/brownout/disk"));
            r.insert(
                "quarantined".into(),
                json!(body
                    .pointer("/quarantine/active")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)),
            );
            r.insert(
                "swim_isolated".into(),
                json!(body
                    .get("swim_isolated")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)),
            );
            r.insert("decommissioning".into(), pick("/decommission/active"));
            r.insert("config_checksum".into(), pick("/config/checksum"));
            r.insert("proto_max".into(), pick("/proto/max"));
        }
    }
    Value::Object(r)
}

fn member_ids(status: &Value) -> Vec<String> {
    let mut ids: Vec<String> = status
        .get("members")
        .and_then(Value::as_array)
        .map(|l| {
            l.iter()
                .filter_map(|m| m.get("id").and_then(Value::as_str).map(String::from))
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

/// Whether every replying node reports the same value at `path` (`None` = nobody did).
fn agree<'a>(statuses: impl Iterator<Item = &'a Value>, path: &str) -> Value {
    let mut seen: Vec<Value> = Vec::new();
    for s in statuses {
        let v = s.pointer(path).cloned().unwrap_or(Value::Null);
        if !seen.contains(&v) {
            seen.push(v);
        }
    }
    json!(seen.len() <= 1)
}

/// `GET /admin/v1/cluster`.
pub async fn cluster(state: &AdminState) -> Answer {
    let (local, replies) = gather(state).await;
    let self_reply = Reply::Status {
        body: local.clone(),
        elapsed_ms: 0,
    };
    let mut nodes = vec![row(&state.node_id, None, &self_reply, true)];
    let mut statuses = vec![&local];
    for (id, peer_addr, reply) in &replies {
        let admin_addr = match reply {
            Reply::Silent { admin_addr, .. } => admin_addr.clone(),
            Reply::Status { .. } => state
                .peers
                .as_ref()
                .and_then(|p| peer_addr.as_deref().and_then(|a| (p.admin_addr)(id, a))),
        };
        nodes.push(row(id, admin_addr.as_deref(), reply, false));
        if let Reply::Status { body, .. } = reply {
            statuses.push(body);
        }
    }
    let local_members = member_ids(&local);
    let membership_agrees = statuses.iter().all(|s| member_ids(s) == local_members);
    let ready = statuses
        .iter()
        .filter(|s| s.get("ready").and_then(Value::as_bool) == Some(true))
        .count();
    let body = json!({
        "answered_by": state.node_id,
        "summary": {
            "nodes": nodes.len(),
            "replied": statuses.len(),
            "ready": ready,
            "same_cluster_id": agree(statuses.iter().copied(), "/cluster_id"),
            "same_version": agree(statuses.iter().copied(), "/version"),
            "same_config": agree(statuses.iter().copied(), "/config/checksum"),
            "same_membership": membership_agrees,
        },
        "nodes": nodes,
    });
    (200, body.to_string())
}

/// `GET /admin/v1/placement`: this node's placement view, and whether every other node's
/// membership view matches it.
pub async fn placement(state: &AdminState) -> Answer {
    let (local, replies) = gather(state).await;
    let local_members = member_ids(&local);
    let views: Vec<Value> = replies
        .iter()
        .map(|(id, _, reply)| match reply {
            Reply::Status { body, .. } => {
                let theirs = member_ids(body);
                json!({
                    "node_id": id,
                    "replied": true,
                    "same_membership": theirs == local_members,
                    "members": theirs.len(),
                })
            }
            Reply::Silent { reason, .. } => json!({
                "node_id": id,
                "replied": false,
                "error": reason,
            }),
        })
        .collect();
    let body = json!({
        "answered_by": state.node_id,
        "members": local.get("members").cloned().unwrap_or_else(|| json!([])),
        "replication": local.get("replication").cloned().unwrap_or(Value::Null),
        "lease": local.get("lease").cloned().unwrap_or(Value::Null),
        "views": views,
    });
    (200, body.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_production_mapping_keeps_the_host_and_swaps_the_port() {
        let f = PeerAccess::same_host(9443);
        assert_eq!(f("n", "10.0.0.7:7000").as_deref(), Some("10.0.0.7:9443"));
        assert_eq!(
            f("n", "mqttd-1.mqttd.ns.svc:7000").as_deref(),
            Some("mqttd-1.mqttd.ns.svc:9443")
        );
        assert_eq!(f("n", "[fd00::7]:7000").as_deref(), Some("[fd00::7]:9443"));
    }

    #[test]
    fn a_silent_node_is_a_row_with_its_reason_not_a_healthy_one() {
        let r = row(
            "n2",
            Some("10.0.0.2:9443"),
            &Reply::Silent {
                admin_addr: None,
                reason: "timed out".into(),
            },
            false,
        );
        assert_eq!(r["replied"], false);
        assert_eq!(r["error"], "timed out");
        assert_eq!(r["admin_addr"], "10.0.0.2:9443");
        assert!(r.get("ready").is_none());
    }
}
