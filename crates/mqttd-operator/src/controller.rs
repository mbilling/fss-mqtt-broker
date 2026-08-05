//! The reconcile loop (ADR 0055 T3: observe → status/conditions/Events).
//!
//! T3 is deliberately **alert-only**: the reconciler reads every broker's
//! `/statusz`, aggregates it ([`crate::observe`]), writes the verdict to the CR's
//! status, and raises Events — but changes nothing in the cluster. Rendering the
//! owned objects (T2's [`crate::render`]) is applied in T4 alongside the opt-in
//! remediations, so an operator installed today is a *reporter*: exactly as
//! conservative as having no operator, which is the ADR 0055 default posture.

use crate::crd::MqttdCluster;
use crate::observe::observe;
use crate::probe::probe_all;
use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::runtime::watcher;
use kube::{Client, Resource, ResourceExt};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// How long to wait before re-observing a healthy resource.
const REQUEUE: Duration = Duration::from_secs(30);
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
    /// HTTP client for `/statusz` probes.
    pub http: reqwest::Client,
    /// Event reporter identity.
    pub reporter: Reporter,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context").finish_non_exhaustive()
    }
}

/// The pods belonging to `cr`, as `(name, ip)` — pods without an IP yet are
/// skipped here and surface as absent evidence in the aggregate.
async fn cluster_pods(client: &Client, cr: &MqttdCluster) -> Result<Vec<(String, String)>, Error> {
    let ns = cr.namespace().unwrap_or_else(|| "default".into());
    let api: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let selector = format!(
        "app.kubernetes.io/name=mqttd,app.kubernetes.io/instance={}",
        cr.name_any()
    );
    let pods = api.list(&ListParams::default().labels(&selector)).await?;
    Ok(pods
        .into_iter()
        .filter_map(|p| {
            let name = p.name_any();
            p.status.and_then(|s| s.pod_ip).map(|ip| (name, ip))
        })
        .collect())
}

/// One reconciliation: observe every pod, patch the verdict, raise Events.
///
/// # Errors
/// [`Error::Kube`] if listing pods or patching the status is rejected.
pub async fn reconcile(obj: Arc<MqttdCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let api: Api<MqttdCluster> = Api::namespaced(ctx.client.clone(), &ns);

    let pods = cluster_pods(&ctx.client, &obj).await?;
    let probes = probe_all(&ctx.http, &pods).await;
    let observation = observe(&probes, obj.spec.replicas, obj.metadata.generation);

    let patch = serde_json::json!({
        "apiVersion": "mqttd.io/v1alpha1",
        "kind": "MqttdCluster",
        "status": observation.status,
    });
    api.patch_status(&name, &PatchParams::apply(MANAGER), &Patch::Merge(&patch))
        .await?;

    // Events make an incident visible in `kubectl describe` without diffing status.
    if !observation.events.is_empty() {
        let recorder = Recorder::new(ctx.client.clone(), ctx.reporter.clone());
        for (reason, note) in &observation.events {
            let ev = Event {
                type_: EventType::Warning,
                reason: reason.clone(),
                note: Some(note.clone()),
                action: "Observed".into(),
                secondary: None,
            };
            if let Err(e) = recorder.publish(&ev, &obj.object_ref(&())).await {
                warn!(error = %e, "failed to publish event");
            }
        }
    }

    info!(
        namespace = %ns, resource = %name,
        phase = observation.status.phase.as_deref().unwrap_or("?"),
        pods = pods.len(),
        "observed"
    );
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
    let ctx = Arc::new(Context {
        client,
        http: reqwest::Client::new(),
        reporter: Reporter::from(MANAGER),
    });
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
