//! Reading each broker's `/statusz` ([ADR 0055](../../../docs/adr/0055-kubernetes-operator.md) T3).
//!
//! The operator observes through the contract the broker already publishes
//! ([ADR 0054](../../../docs/adr/0054-operator-facing-state-surface.md)) — no new
//! control surface, no admin API. `/statusz` is unauthenticated on the ops network
//! by design and carries no secret material, so a plain HTTP GET per pod is the
//! whole mechanism.
//!
//! Every field is optional: a broker that is starting, non-durable, or older than a
//! given signal simply omits it. An unreachable pod is *absent evidence*, never a
//! failure verdict — that distinction is what keeps [`crate::observe`] from calling
//! a rolling restart a split brain.

use serde::Deserialize;
use std::time::Duration;

/// How long to wait for one pod's `/statusz`.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
/// The health/metrics port the chart and the operator both expose.
const HEALTH_PORT: u16 = 8080;

/// One broker's `/statusz` body (the subset the operator reasons about).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PodStatus {
    /// The broker's node id (the pod name).
    #[serde(default)]
    pub node_id: String,
    /// The cluster identity every node must agree on (ADR 0054 T2).
    #[serde(default)]
    pub cluster_id: Option<String>,
    /// Whether this node founded the cluster (started seedless).
    #[serde(default)]
    pub founder: bool,
    /// Whether the node currently passes readiness.
    #[serde(default)]
    pub ready: bool,
    /// The placement membership view.
    #[serde(default)]
    pub members: Vec<Member>,
    /// Lease-group detail (durable mode only).
    #[serde(default)]
    pub lease: Option<Lease>,
    /// Brownout state (ADR 0041 §5).
    #[serde(default)]
    pub brownout: Option<Brownout>,
    /// Decommission drain progress (ADR 0043).
    #[serde(default)]
    pub decommission: Option<Decommission>,
    /// Accepted gossip-key fingerprints (rotation posture).
    #[serde(default)]
    pub keys: Option<Keys>,
    /// The applied config's identity (convergence check).
    #[serde(default)]
    pub config: Option<ConfigStamp>,
}

/// One member of a node's placement view.
#[derive(Debug, Clone, Deserialize)]
pub struct Member {
    /// The member's node id.
    pub id: String,
}

/// Lease-group detail.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Lease {
    /// Whether this node is the lease-group leader.
    #[serde(default)]
    pub leader: bool,
    /// Whether the group can serve durable sessions here.
    #[serde(default)]
    pub group_ready: bool,
    /// The current voter identities.
    #[serde(default)]
    pub voters: Vec<String>,
}

/// Brownout state.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Brownout {
    /// Whether disk growth-writes are being refused.
    #[serde(default)]
    pub disk: bool,
}

/// Decommission drain progress.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Decommission {
    /// Whether a drain is running.
    #[serde(default)]
    pub active: bool,
    /// Hand-offs still outstanding.
    #[serde(default)]
    pub pending: u64,
    /// Whether the drain finished.
    #[serde(default)]
    pub complete: bool,
}

/// Gossip-key rotation posture (fingerprints only — never material).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Keys {
    /// How many keys this node accepts: 1 steady, 2 = rotation window open.
    #[serde(default)]
    pub count: u64,
}

/// The applied config's identity.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ConfigStamp {
    /// sha-256 of the loaded config file.
    #[serde(default)]
    pub checksum: String,
}

/// What one pod contributed to this observation.
#[derive(Debug, Clone)]
pub enum PodProbe {
    /// The pod answered; this is its state.
    Reached(Box<PodStatus>),
    /// The pod did not answer (starting, restarting, no IP yet, network).
    /// Absence of evidence — the aggregator must not read it as bad news.
    Unreachable {
        /// The pod's name, for the status message.
        pod: String,
    },
}

/// Probe one pod's `/statusz`.
async fn probe_one(http: &reqwest::Client, pod: &str, ip: &str) -> PodProbe {
    let url = format!("http://{ip}:{HEALTH_PORT}/statusz");
    // Body read as text + serde_json rather than reqwest's `json` feature: the
    // workspace pins reqwest with default-features off (one TLS stack, ADR 0053),
    // and a malformed body must read as "unreachable", not as a parse panic.
    let Ok(resp) = http.get(&url).timeout(PROBE_TIMEOUT).send().await else {
        return PodProbe::Unreachable { pod: pod.into() };
    };
    let Ok(body) = resp.text().await else {
        return PodProbe::Unreachable { pod: pod.into() };
    };
    match serde_json::from_str::<PodStatus>(&body) {
        Ok(status) => PodProbe::Reached(Box::new(status)),
        Err(_) => PodProbe::Unreachable { pod: pod.into() },
    }
}

/// Probe every `(pod name, pod IP)` concurrently.
pub async fn probe_all(http: &reqwest::Client, pods: &[(String, String)]) -> Vec<PodProbe> {
    let futures = pods.iter().map(|(name, ip)| probe_one(http, name, ip));
    futures_util::future::join_all(futures).await
}

#[cfg(test)]
mod tests {
    use super::PodStatus;

    /// A real `/statusz` body parses, and a minimal one (early startup, before the
    /// cluster identity or lease exist) parses too — every field is optional.
    #[test]
    fn parses_full_and_minimal_bodies() {
        let full: PodStatus = serde_json::from_str(
            r#"{"node_id":"mqttd-0","version":"0.9.0","founder":true,"ready":true,
                "cluster_id":"91e0f098ebf2a1d8",
                "members":[{"id":"mqttd-0"},{"id":"mqttd-1","addr":"mqttd-1:7001"}],
                "lease":{"leader":true,"epoch":1,"group_ready":true,
                         "voters":["mqttd-0","mqttd-1"],"quorum_ack_age_ms":1,
                         "replica_groups":{"current":256,"tracked":256}},
                "decommission":{"active":false,"pending":0,"rounds":0,"complete":false},
                "brownout":{"disk":false},
                "store":{"sessions":1024,"max_bytes":16000000000},
                "keys":{"count":1,"fingerprints":["aabbccdd"]},
                "config":{"checksum":"deadbeef","generation":1},
                "proto":{"min":6,"max":6}}"#,
        )
        .expect("full body parses");
        assert_eq!(full.node_id, "mqttd-0");
        assert_eq!(full.cluster_id.as_deref(), Some("91e0f098ebf2a1d8"));
        assert_eq!(full.members.len(), 2);
        assert!(full.lease.expect("lease").leader);
        assert_eq!(full.keys.expect("keys").count, 1);
        assert_eq!(full.config.expect("config").checksum, "deadbeef");

        let minimal: PodStatus =
            serde_json::from_str(r#"{"node_id":"mqttd-0","founder":false,"ready":false}"#)
                .expect("minimal body parses");
        assert_eq!(minimal.cluster_id, None, "no identity adopted yet");
        assert!(minimal.members.is_empty());
        assert!(minimal.lease.is_none(), "non-durable or not yet formed");
    }
}
