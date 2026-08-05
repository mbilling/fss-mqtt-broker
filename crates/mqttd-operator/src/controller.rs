//! The reconcile loop (ADR 0055 T3: observe → status/conditions/Events).
//!
//! The reconciler reads every broker's `/statusz`, aggregates it
//! ([`crate::observe`]), writes the verdict to the CR's status, raises Events —
//! and then, **only if the CR opted in**, carries out a remediation
//! ([`crate::remediate`], T4). The rendered objects of T2's [`crate::render`]
//! are server-side applied with an owner reference, so deleting the CR
//! garbage-collects the cluster and an operator restart converges rather than
//! conflicts. Remediations remain opt-in: with the default spec the operator
//! creates and reports, and touches nothing else.

use crate::crd::MqttdCluster;
use crate::observe::observe;
use crate::probe::probe_all;
use futures_util::StreamExt;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::api::{Api, DynamicObject, ListParams, Patch, PatchParams};
use kube::core::{ApiResource, GroupVersionKind};
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

/// The PVCs the operator has already quarantined.
///
/// A fenced node must be fenced ONCE (see [`crate::remediate::plan`]): its volume
/// still holds the divergent state, so it re-founds on every restart, and killing
/// it again on every pass would only hide the incident behind a restart loop.
async fn quarantined_pvcs(client: &Client, ns: &str) -> Result<Vec<String>, Error> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), ns);
    let selector = format!("{}=true", crate::remediate::QUARANTINE_LABEL);
    let claims = api.list(&ListParams::default().labels(&selector)).await?;
    Ok(claims.into_iter().map(|c| c.name_any()).collect())
}

/// The owner reference that ties every rendered object to its CR: deleting the
/// `MqttdCluster` garbage-collects the whole cluster, and no object the operator
/// owns can outlive its spec.
fn owner_reference(cr: &MqttdCluster) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "mqttd.io/v1alpha1",
        "kind": "MqttdCluster",
        "name": cr.name_any(),
        "uid": cr.uid().unwrap_or_default(),
        "controller": true,
        "blockOwnerDeletion": true,
    })
}

/// The `GroupVersionKind` for each object the operator renders. Static rather
/// than discovered: the set is fixed by [`crate::render`], and a typo becomes a
/// compile-adjacent test failure instead of a runtime discovery surprise.
fn gvk_for(kind: &str) -> Option<GroupVersionKind> {
    Some(match kind {
        "StatefulSet" => GroupVersionKind::gvk("apps", "v1", "StatefulSet"),
        "Service" => GroupVersionKind::gvk("", "v1", "Service"),
        "ConfigMap" => GroupVersionKind::gvk("", "v1", "ConfigMap"),
        "ServiceAccount" => GroupVersionKind::gvk("", "v1", "ServiceAccount"),
        "PodDisruptionBudget" => GroupVersionKind::gvk("policy", "v1", "PodDisruptionBudget"),
        _ => return None,
    })
}

/// Server-side apply every rendered object, owned by the CR.
///
/// Apply (not create) so a reconcile is idempotent and an operator restart
/// converges rather than conflicts; the rendered shape is the one the chart
/// produces, held there by the CI render-parity gate.
async fn apply_owned(client: &Client, cr: &MqttdCluster, ns: &str) -> Result<usize, Error> {
    let rendered = crate::render::render(cr);
    let owner = owner_reference(cr);
    let params = PatchParams::apply(MANAGER);
    let mut applied = 0;
    for object in rendered.all() {
        let Some(kind) = object["kind"].as_str() else {
            continue;
        };
        let Some(gvk) = gvk_for(kind) else {
            warn!(
                kind,
                "rendered an object the operator has no GVK for; skipping"
            );
            continue;
        };
        let mut object = object.clone();
        object["metadata"]["ownerReferences"] = serde_json::json!([owner]);
        let name = object["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let api: Api<DynamicObject> =
            Api::namespaced_with(client.clone(), ns, &ApiResource::from_gvk(&gvk));
        api.patch(&name, &params, &Patch::Apply(&object)).await?;
        applied += 1;
    }
    Ok(applied)
}

/// One reconciliation: observe every pod, patch the verdict, raise Events.
///
/// # Errors
/// [`Error::Kube`] if listing pods or patching the status is rejected.
pub async fn reconcile(obj: Arc<MqttdCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let api: Api<MqttdCluster> = Api::namespaced(ctx.client.clone(), &ns);

    // Own the objects first: the cluster this CR describes must exist before
    // there is anything to observe.
    let applied = apply_owned(&ctx.client, &obj, &ns).await?;

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

    // Opt-in remediation (T4). Planning is pure and guarded; with the default
    // spec this returns nothing at all.
    let pvc_names: Vec<String> = pods.iter().map(|(pod, _)| format!("data-{pod}")).collect();
    let quarantined = quarantined_pvcs(&ctx.client, &ns).await?;
    let planned = crate::remediate::plan(&obj, &probes, &pvc_names, &quarantined);
    for action in &planned {
        let (reason, note) = crate::remediate::describe(action);
        match crate::remediate::execute(&ctx.client, &ns, action).await {
            Ok(()) => {
                let recorder = Recorder::new(ctx.client.clone(), ctx.reporter.clone());
                let ev = Event {
                    type_: EventType::Warning,
                    reason,
                    note: Some(note),
                    action: "Remediated".into(),
                    secondary: None,
                };
                if let Err(e) = recorder.publish(&ev, &obj.object_ref(&())).await {
                    warn!(error = %e, "failed to publish remediation event");
                }
            }
            // A refused remediation is reported, never retried in a tight loop:
            // the next reconcile re-plans from fresh evidence.
            Err(e) => warn!(error = %e, ?action, "remediation failed"),
        }
    }

    info!(
        namespace = %ns, resource = %name,
        phase = observation.status.phase.as_deref().unwrap_or("?"),
        pods = pods.len(),
        applied,
        remediations = planned.len(),
        "reconciled"
    );
    Ok(Action::requeue(REQUEUE))
}

/// Error policy: log and requeue with backoff.
#[allow(clippy::needless_pass_by_value)] // the signature kube's runtime expects
pub fn error_policy(obj: Arc<MqttdCluster>, err: &Error, _ctx: Arc<Context>) -> Action {
    warn!(resource = %obj.name_any(), error = %err, "reconcile failed; requeueing");
    Action::requeue(ERROR_REQUEUE)
}

/// The HTTP client for `/statusz` probes.
///
/// `reqwest::Client::new()` PANICS in a minimal image ("No CA certificates were
/// loaded from the system") because it tries to load system trust roots at
/// construction — found by the T7 e2e, since the operator image carries no CA
/// bundle. The probe target is plaintext by design (ADR 0020: the health
/// listener is unauthenticated HTTP on the ops network), so this client is built
/// with an EMPTY root store: it never needs a trust anchor, and if anyone ever
/// points it at `https` it fails closed instead of trusting something unexpected.
fn status_http_client() -> reqwest::Client {
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .build()
        .unwrap_or_default()
}

/// Run the controller until the watch stream ends.
///
/// Watches **one namespace** — the operator's own. ADR 0055 §4 promises
/// namespaced RBAC (no cluster-admin, no cluster-scoped reads), and a
/// cluster-wide watch would quietly require a `ClusterRole` to match. The T7 e2e
/// caught the mismatch as a 403 storm: the code is aligned to the posture, not
/// the posture widened to the code.
pub async fn run(client: Client, namespace: &str) {
    let api: Api<MqttdCluster> = Api::namespaced(client.clone(), namespace);
    let ctx = Arc::new(Context {
        client,
        http: status_http_client(),
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
