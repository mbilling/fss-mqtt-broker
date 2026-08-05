//! The reconcile loop (0055-T1 skeleton: observe + status stamp).
//!
//! T1 deliberately performs **no mutations beyond the CR's own status**: it
//! watches `MqttdCluster` resources, stamps `observedGeneration`, and sets a
//! `Reconciled` condition — proving the watch → reconcile → status machinery
//! end to end before any resource rendering (T2) or remediation (T4) exists.

use crate::crd::{MqttdCluster, MqttdClusterStatus, StatusCondition};
use futures_util::StreamExt;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Client, ResourceExt};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// How long to wait before re-reconciling a healthy resource.
const REQUEUE: Duration = Duration::from_secs(60);
/// Backoff after a reconcile error.
const ERROR_REQUEUE: Duration = Duration::from_secs(15);
/// The field-manager name for server-side apply patches.
const MANAGER: &str = "mqttd-operator";

/// Reconciliation errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The Kubernetes API rejected an operation.
    #[error("kubernetes api error: {0}")]
    Kube(#[from] kube::Error),
}

/// Shared context for reconciliations.
#[derive(Clone)]
pub struct Context {
    /// The Kubernetes client.
    pub client: Client,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context").finish_non_exhaustive()
    }
}

/// One reconciliation: stamp `observedGeneration` + the `Reconciled` condition.
///
/// # Errors
/// [`Error::Kube`] if the status patch is rejected.
pub async fn reconcile(obj: Arc<MqttdCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let api: Api<MqttdCluster> = Api::namespaced(ctx.client.clone(), &ns);

    let status = MqttdClusterStatus {
        phase: Some("Pending".into()),
        observed_generation: obj.metadata.generation,
        conditions: vec![StatusCondition {
            r#type: "Reconciled".into(),
            status: "True".into(),
            reason: Some("Scaffold".into()),
            message: Some(
                "observed by the 0055-T1 scaffold; resource rendering lands in 0055-T2".into(),
            ),
            last_transition_time: None,
        }],
        ..MqttdClusterStatus::default()
    };
    let patch = serde_json::json!({
        "apiVersion": "mqttd.io/v1alpha1",
        "kind": "MqttdCluster",
        "status": status,
    });
    api.patch_status(&name, &PatchParams::apply(MANAGER), &Patch::Merge(&patch))
        .await?;
    info!(namespace = %ns, resource = %name, generation = ?obj.metadata.generation,
        "reconciled (scaffold: status stamped)");
    Ok(Action::requeue(REQUEUE))
}

/// Error policy: log and requeue with backoff.
#[allow(clippy::needless_pass_by_value)] // the signature kube's runtime expects
pub fn error_policy(obj: Arc<MqttdCluster>, err: &Error, _ctx: Arc<Context>) -> Action {
    warn!(resource = %obj.name_any(), error = %err, "reconcile failed; requeueing");
    Action::requeue(ERROR_REQUEUE)
}

/// Run the controller until the watch stream ends.
pub async fn run(client: Client) {
    let api: Api<MqttdCluster> = Api::all(client.clone());
    let ctx = Arc::new(Context { client });
    Controller::new(api, watcher::Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _)) => info!(resource = %obj.name, "reconcile ok"),
                Err(e) => warn!(error = %e, "controller stream error"),
            }
        })
        .await;
}
