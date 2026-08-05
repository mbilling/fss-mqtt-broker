//! Lease-based leader gate (ADR 0055 §1).
//!
//! One active reconciler at a time: replicas contend for a
//! `coordination.k8s.io/v1` Lease and only the holder runs the controller.
//! Deliberately minimal — acquire-or-wait with periodic renewal, no
//! third-party election crate (supply-chain posture) — because the operator is
//! stateless between reconciliations: losing the lease and re-acquiring later
//! is always safe (reconciliation is idempotent by design).

use k8s_openapi::api::coordination::v1::Lease;
use kube::api::{Api, Patch, PatchParams};
use kube::Client;
use std::time::Duration;
use tracing::{debug, info, warn};

/// The Lease name replicas contend for.
const LEASE_NAME: &str = "mqttd-operator";
/// Holder lease duration; a crashed holder's lease expires after this.
const LEASE_SECS: i32 = 30;
/// Renewal cadence (well inside the lease duration).
const RENEW: Duration = Duration::from_secs(10);
/// Retry cadence while another holder is active.
const CONTEND: Duration = Duration::from_secs(15);

/// Block until this replica holds the lease, then keep renewing it in a
/// background task. Returns once leadership is held. `identity` should be the
/// pod name; `ns` the operator's namespace.
pub async fn acquire_and_hold(client: Client, ns: &str, identity: String) {
    let api: Api<Lease> = Api::namespaced(client, ns);
    loop {
        match try_acquire(&api, &identity).await {
            Ok(true) => {
                info!(%identity, "acquired the operator lease; this replica leads");
                let api = api.clone();
                let holder = identity.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(RENEW).await;
                        if let Err(e) = try_acquire(&api, &holder).await {
                            warn!(error = %e, "lease renewal failed; will retry");
                        }
                    }
                });
                return;
            }
            Ok(false) => debug!("another replica holds the operator lease; waiting"),
            Err(e) => warn!(error = %e, "lease acquisition failed; will retry"),
        }
        tokio::time::sleep(CONTEND).await;
    }
}

/// Acquire or renew the lease for `identity`. `Ok(true)` = we hold it now.
async fn try_acquire(api: &Api<Lease>, identity: &str) -> Result<bool, kube::Error> {
    let now = chrono_now();
    let current = api.get_opt(LEASE_NAME).await?;
    if let Some(lease) = &current {
        if let Some(spec) = &lease.spec {
            let holder = spec.holder_identity.as_deref().unwrap_or("");
            if holder != identity && !expired(spec) {
                return Ok(false); // someone else holds a live lease
            }
        }
    }
    let patch = serde_json::json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": { "name": LEASE_NAME },
        "spec": {
            "holderIdentity": identity,
            "leaseDurationSeconds": LEASE_SECS,
            "renewTime": now,
        }
    });
    api.patch(
        LEASE_NAME,
        &PatchParams::apply("mqttd-operator-leader").force(),
        &Patch::Apply(&patch),
    )
    .await?;
    Ok(true)
}

/// Whether a lease's renew time is older than its duration (holder presumed dead).
fn expired(spec: &k8s_openapi::api::coordination::v1::LeaseSpec) -> bool {
    let Some(renew) = &spec.renew_time else {
        return true;
    };
    let duration = i64::from(spec.lease_duration_seconds.unwrap_or(LEASE_SECS));
    let now = k8s_openapi::jiff::Timestamp::now();
    (now.as_second() - renew.0.as_second()) > duration
}

/// The current time as an RFC 3339 string the Lease `renewTime` accepts
/// (second precision is sufficient for a 30 s lease).
fn chrono_now() -> String {
    k8s_openapi::jiff::Timestamp::now()
        .strftime("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}
