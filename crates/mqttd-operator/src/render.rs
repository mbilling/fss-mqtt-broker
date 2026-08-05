//! Resource rendering ([ADR 0055](../../../docs/adr/0055-kubernetes-operator.md) §2, T2).
//!
//! The operator renders **the same objects the Helm chart renders** —
//! `StatefulSet`, headless + client `Service`s, config `ConfigMap`,
//! `PodDisruptionBudget` — from an
//! [`MqttdCluster`](crate::crd::MqttdCluster). The two deployment paths must never
//! disagree, so `scripts/k8s/render-parity.sh` diffs this module's output against
//! `helm template` for equivalent inputs in CI: **that test, not discipline, is what
//! keeps the duplicated shape honest.** Anything load-bearing that drifts fails the
//! `helm` job.
//!
//! Objects are `serde_json::Value` rather than typed k8s-openapi structs: server-side
//! apply (`Patch::Apply`) takes JSON directly, the shapes stay readable next to the
//! templates they mirror, and the parity diff operates on the same representation the
//! cluster receives.

use crate::crd::MqttdCluster;
use kube::ResourceExt;
use serde_json::{json, Value};

/// Hex sha-256 of the config template — the pod annotation that makes a config
/// edit roll the `StatefulSet` (the chart's `checksum/config`). Without it a
/// changed `ConfigMap` would be projected into running pods with no restart, which is only
/// correct for the file-watched policy paths, not for restart-only settings.
fn config_checksum(config: &str) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, config.as_bytes());
    digest.as_ref().iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// The per-pod config render script. Byte-identical to the chart's init container
/// (deploy/helm/mqttd/templates/statefulset.yaml) — the parity test compares it.
const RENDER_SCRIPT: &str = r#"set -eu
ORD="${POD_NAME##*-}"
STS="${POD_NAME%-*}"
SEED0="\"${STS}-0.${HEADLESS_DOMAIN}:7946\""
SEED1="\"${STS}-1.${HEADLESS_DOMAIN}:7946\""
if [ "$ORD" = "0" ]; then
  SEEDS=""
  READY_MIN=1
else
  if [ "$ORD" = "1" ]; then SEEDS="$SEED0"; else SEEDS="${SEED0}, ${SEED1}"; fi
  READY_MIN=$(( REPLICAS / 2 + 1 ))
fi
# Every pod must ADVERTISE a routable peer-bus address, not its 0.0.0.0 bind — peers
# dial the advertised address over the headless service, and 0.0.0.0 is unroutable
# (the mesh never links). Per-pod DNS is <pod>.<headless>.<ns>.svc.cluster.local.
PEER_ADVERTISE="${POD_NAME}.${HEADLESS_DOMAIN}:7001"
sed -e "s/__NODE_ID__/${POD_NAME}/g" -e "s|__SEEDS__|${SEEDS}|g" \
  -e "s|__PEER_ADVERTISE__|${PEER_ADVERTISE}|g" \
  -e "s/__READY_MIN__/${READY_MIN}/g" \
  /tmpl/mqttd.toml.tmpl > /config/mqttd.toml
echo "rendered config for ${POD_NAME} (ordinal ${ORD}):"
cat /config/mqttd.toml
"#;

/// Default broker image when the CR does not pin one.
const DEFAULT_IMAGE: &str = concat!(
    "ghcr.io/mbilling/fss-mqtt-broker:",
    env!("CARGO_PKG_VERSION")
);
/// The shell image for the render init container (the broker image is distroless).
const INIT_IMAGE: &str = "busybox:1.36";
/// Drain + graceful-shutdown allowance (chart default).
const TERMINATION_GRACE_SECONDS: i64 = 300;
/// Default PVC size when the CR does not set one.
const DEFAULT_PVC_SIZE: &str = "10Gi";

/// Every object the operator owns for one `MqttdCluster`.
#[derive(Debug)]
pub struct Rendered {
    /// The broker `StatefulSet`.
    pub statefulset: Value,
    /// Headless (peer/gossip) then client-facing Service.
    pub services: Vec<Value>,
    /// The config-template `ConfigMap`.
    pub configmap: Value,
    /// The `PodDisruptionBudget` (always rendered; the chart makes it optional).
    pub pdb: Value,
}

impl Rendered {
    /// Every object as one list, in apply order.
    #[must_use]
    pub fn all(&self) -> Vec<&Value> {
        let mut v = vec![&self.configmap];
        v.extend(self.services.iter());
        v.push(&self.pdb);
        v.push(&self.statefulset);
        v
    }
}

/// Names derived from the resource, mirroring the chart's `_helpers.tpl`.
struct Names {
    full: String,
    headless: String,
    namespace: String,
}

impl Names {
    fn of(cr: &MqttdCluster) -> Self {
        let full = cr.name_any();
        Self {
            headless: format!("{full}-headless"),
            namespace: cr.namespace().unwrap_or_else(|| "default".into()),
            full,
        }
    }
}

/// Selector labels — must equal the chart's `mqttd.selectorLabels`.
fn selector_labels(names: &Names) -> Value {
    json!({
        "app.kubernetes.io/name": "mqttd",
        "app.kubernetes.io/instance": names.full,
    })
}

/// Common labels. `managed-by` differs from the chart by construction (the parity
/// test normalizes `managed-by` and the chart-only `helm.sh/chart` away).
fn labels(names: &Names) -> Value {
    json!({
        "app.kubernetes.io/name": "mqttd",
        "app.kubernetes.io/instance": names.full,
        "app.kubernetes.io/version": env!("CARGO_PKG_VERSION"),
        "app.kubernetes.io/managed-by": "mqttd-operator",
        "app.kubernetes.io/component": "broker",
    })
}

/// Render every object for `cr`.
#[must_use]
pub fn render(cr: &MqttdCluster) -> Rendered {
    let names = Names::of(cr);
    Rendered {
        configmap: configmap(cr, &names),
        services: vec![headless_service(&names), client_service(&names)],
        pdb: pdb(&names),
        statefulset: statefulset(cr, &names),
    }
}

fn configmap(cr: &MqttdCluster, names: &Names) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": format!("{}-config", names.full),
            "namespace": names.namespace,
            "labels": labels(names),
        },
        "data": { "mqttd.toml.tmpl": cr.spec.config },
    })
}

fn headless_service(names: &Names) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": names.headless,
            "namespace": names.namespace,
            "labels": labels(names),
        },
        "spec": {
            "clusterIP": "None",
            // Gossip must reach a still-joining peer (ADR 0016).
            "publishNotReadyAddresses": true,
            "selector": selector_labels(names),
            "ports": [
                { "name": "peer", "port": 7001, "targetPort": "peer", "protocol": "TCP" },
                { "name": "gossip", "port": 7946, "targetPort": "gossip", "protocol": "UDP" },
            ],
        },
    })
}

fn client_service(names: &Names) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": names.full,
            "namespace": names.namespace,
            "labels": labels(names),
        },
        "spec": {
            "type": "ClusterIP",
            "selector": selector_labels(names),
            "ports": [
                { "name": "mqtt-tls", "port": 8883, "targetPort": "mqtt-tls" },
                { "name": "health", "port": 8080, "targetPort": "health", "protocol": "TCP" },
            ],
        },
    })
}

fn pdb(names: &Names) -> Value {
    json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": names.full,
            "namespace": names.namespace,
            "labels": labels(names),
        },
        "spec": {
            "maxUnavailable": 1,
            "selector": { "matchLabels": selector_labels(names) },
        },
    })
}

/// Volume mounts + volumes for whichever secret references the CR sets, mirroring
/// the chart's `secrets` block (by PATH — the operator never reads the material).
fn secret_wiring(cr: &MqttdCluster) -> (Vec<Value>, Vec<Value>) {
    let mut mounts = Vec::new();
    let mut volumes = Vec::new();
    let s = cr.spec.secrets.as_ref();
    let mut add = |name: &str, path: &str, source: Value| {
        mounts.push(json!({ "name": name, "mountPath": path, "readOnly": true }));
        volumes.push(source);
    };
    if let Some(tls) = s.and_then(|s| s.tls.as_ref()) {
        add(
            "tls",
            "/etc/mqttd/tls",
            json!({ "name": "tls", "secret": { "secretName": tls } }),
        );
    }
    if let Some(acl) = s.and_then(|s| s.acl.as_ref()) {
        add(
            "acl",
            "/etc/mqttd/acl",
            json!({ "name": "acl", "configMap": { "name": acl } }),
        );
    }
    if let Some(peer) = s.and_then(|s| s.peer_tls.as_ref()) {
        add(
            "peer-tls",
            "/etc/mqttd/cluster",
            json!({ "name": "peer-tls", "secret": { "secretName": peer } }),
        );
    }
    if let Some(key) = s.and_then(|s| s.gossip_key.as_ref()) {
        add(
            "gossip-key",
            "/etc/mqttd/gossip",
            json!({ "name": "gossip-key", "secret": { "secretName": key } }),
        );
    }
    (mounts, volumes)
}

#[allow(clippy::too_many_lines)] // one flat manifest mirroring the chart template
fn statefulset(cr: &MqttdCluster, names: &Names) -> Value {
    let image = cr
        .spec
        .image
        .clone()
        .unwrap_or_else(|| DEFAULT_IMAGE.into());
    let headless_domain = format!("{}.{}.svc.cluster.local", names.headless, names.namespace);
    let (secret_mounts, secret_volumes) = secret_wiring(cr);
    let persistence = cr.spec.persistence.as_ref();
    let size = persistence
        .and_then(|p| p.size.clone())
        .unwrap_or_else(|| DEFAULT_PVC_SIZE.into());

    let mut container_mounts = vec![
        json!({ "name": "config", "mountPath": "/config" }),
        json!({ "name": "data", "mountPath": "/var/lib/mqttd" }),
    ];
    container_mounts.extend(secret_mounts);
    let mut volumes = vec![
        json!({ "name": "config-tmpl", "configMap": { "name": format!("{}-config", names.full) } }),
        json!({ "name": "config", "emptyDir": {} }),
    ];
    volumes.extend(secret_volumes);

    let mut pvc_spec = json!({
        "accessModes": ["ReadWriteOnce"],
        "resources": { "requests": { "storage": size } },
    });
    if let Some(class) = persistence.and_then(|p| p.storage_class_name.clone()) {
        pvc_spec["storageClassName"] = json!(class);
    }

    json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": names.full,
            "namespace": names.namespace,
            "labels": labels(names),
        },
        "spec": {
            "serviceName": names.headless,
            "replicas": cr.spec.replicas,
            // One pod at a time, in order (ADR 0039's rolling-upgrade motion).
            "podManagementPolicy": "OrderedReady",
            "updateStrategy": { "type": "RollingUpdate" },
            // Post-drain data is migrated, so deleting a scaled-down pod's PVC is
            // correct and closes the stale-rejoin trap (k8s >= 1.27).
            "persistentVolumeClaimRetentionPolicy": {
                "whenDeleted": "Retain",
                "whenScaled": "Delete",
            },
            "selector": { "matchLabels": selector_labels(names) },
            "template": {
                "metadata": {
                    "labels": labels(names),
                    "annotations": {
                        "checksum/config": config_checksum(&cr.spec.config),
                        "prometheus.io/scrape": "true",
                        "prometheus.io/port": "8080",
                        "prometheus.io/path": "/metrics",
                    },
                },
                "spec": {
                    "serviceAccountName": names.full,
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 65532,
                        "fsGroup": 65532,
                    },
                    "terminationGracePeriodSeconds": TERMINATION_GRACE_SECONDS,
                    "initContainers": [
                        {
                            "name": "render-config",
                            "image": INIT_IMAGE,
                            "imagePullPolicy": "IfNotPresent",
                            "securityContext": security_context(),
                            "command": ["/bin/sh", "-c"],
                            "args": [RENDER_SCRIPT],
                            "env": [
                                { "name": "POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } },
                                { "name": "REPLICAS", "value": cr.spec.replicas.to_string() },
                                { "name": "HEADLESS_DOMAIN", "value": headless_domain },
                            ],
                            "volumeMounts": [
                                { "name": "config-tmpl", "mountPath": "/tmpl" },
                                { "name": "config", "mountPath": "/config" },
                            ],
                        },
                        {
                            "name": "check-config",
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "securityContext": security_context(),
                            "args": ["--check-config", "--config", "/config/mqttd.toml"],
                            "volumeMounts": [{ "name": "config", "mountPath": "/config" }],
                        },
                    ],
                    "containers": [{
                        "name": "mqttd",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "securityContext": security_context(),
                        "args": ["--config", "/config/mqttd.toml"],
                        "ports": [
                            { "name": "mqtt-tls", "containerPort": 8883 },
                            { "name": "health", "containerPort": 8080 },
                            { "name": "peer", "containerPort": 7001 },
                            { "name": "gossip", "containerPort": 7946, "protocol": "UDP" },
                        ],
                        // Scale-down is a decommission drain, not a crash (ADR 0043).
                        "lifecycle": {
                            "preStop": {
                                "exec": {
                                    "command": [
                                        "/usr/local/bin/mqttd",
                                        "--decommission",
                                        "--timeout",
                                        TERMINATION_GRACE_SECONDS.to_string(),
                                    ],
                                },
                            },
                        },
                        "startupProbe": probe("/livez", 5, 60),
                        "readinessProbe": probe("/readyz", 5, 3),
                        "livenessProbe": probe("/livez", 10, 6),
                        "resources": {},
                        "volumeMounts": container_mounts,
                    }],
                    "volumes": volumes,
                },
            },
            "volumeClaimTemplates": [{
                "metadata": { "name": "data", "labels": labels(names) },
                "spec": pvc_spec,
            }],
        },
    })
}

fn security_context() -> Value {
    json!({
        "allowPrivilegeEscalation": false,
        "readOnlyRootFilesystem": true,
        "capabilities": { "drop": ["ALL"] },
    })
}

fn probe(path: &str, period: i64, failure_threshold: i64) -> Value {
    json!({
        "httpGet": { "path": path, "port": "health" },
        "periodSeconds": period,
        "failureThreshold": failure_threshold,
    })
}

#[cfg(test)]
mod tests {
    use super::render;
    use crate::crd::MqttdCluster;

    fn sample() -> MqttdCluster {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "mqttd.io/v1alpha1",
            "kind": "MqttdCluster",
            "metadata": { "name": "mqttd", "namespace": "default" },
            "spec": {
                "replicas": 3,
                "image": "ghcr.io/mbilling/fss-mqtt-broker:0.9.0",
                "config": "[node]\nid = \"__NODE_ID__\"\n",
            }
        }))
        .expect("sample CR")
    }

    /// The rendered set is the chart's object set, named the chart's way.
    #[test]
    fn renders_the_chart_object_set() {
        let r = render(&sample());
        assert_eq!(
            r.all().len(),
            5,
            "configmap + 2 services + pdb + statefulset"
        );
        assert_eq!(r.statefulset["metadata"]["name"], "mqttd");
        assert_eq!(r.statefulset["spec"]["serviceName"], "mqttd-headless");
        assert_eq!(r.statefulset["spec"]["replicas"], 3);
        assert_eq!(r.services[0]["metadata"]["name"], "mqttd-headless");
        assert_eq!(r.services[0]["spec"]["clusterIP"], "None");
        assert_eq!(r.services[1]["metadata"]["name"], "mqttd");
        assert_eq!(r.configmap["metadata"]["name"], "mqttd-config");
        assert_eq!(r.pdb["spec"]["maxUnavailable"], 1);
    }

    /// The load-bearing operational contracts survive rendering: ordered rolls,
    /// the decommission preStop, PVC retention, and the per-pod render env.
    #[test]
    fn preserves_the_operational_contracts() {
        let r = render(&sample());
        let spec = &r.statefulset["spec"];
        assert_eq!(spec["podManagementPolicy"], "OrderedReady");
        assert_eq!(
            spec["persistentVolumeClaimRetentionPolicy"]["whenScaled"],
            "Delete"
        );
        assert_eq!(
            spec["persistentVolumeClaimRetentionPolicy"]["whenDeleted"],
            "Retain"
        );
        let pod = &spec["template"]["spec"];
        let prestop = &pod["containers"][0]["lifecycle"]["preStop"]["exec"]["command"];
        assert_eq!(prestop[1], "--decommission");
        assert_eq!(pod["terminationGracePeriodSeconds"], 300);
        let env = &pod["initContainers"][0]["env"];
        assert_eq!(env[1]["name"], "REPLICAS");
        assert_eq!(
            env[1]["value"], "3",
            "replicas reach the readiness-floor math"
        );
        assert_eq!(
            env[2]["value"], "mqttd-headless.default.svc.cluster.local",
            "per-pod DNS domain for seeds + peer advertise"
        );
    }

    /// Secret references are mounted by PATH only, and absent ones add nothing.
    #[test]
    fn secret_references_mount_by_path_and_are_optional() {
        let bare = render(&sample());
        let vols = bare.statefulset["spec"]["template"]["spec"]["volumes"]
            .as_array()
            .unwrap();
        assert_eq!(vols.len(), 2, "config-tmpl + config only");

        let mut cr = sample();
        cr.spec.secrets = Some(crate::crd::SecretRefs {
            tls: Some("mqttd-tls".into()),
            acl: None,
            peer_tls: None,
            gossip_key: Some("swim-key".into()),
        });
        let wired = render(&cr);
        let pod = &wired.statefulset["spec"]["template"]["spec"];
        let names: Vec<_> = pod["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["config-tmpl", "config", "tls", "gossip-key"]);
        let mounts: Vec<_> = pod["containers"][0]["volumeMounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["mountPath"].as_str().unwrap())
            .collect();
        assert!(mounts.contains(&"/etc/mqttd/tls"));
        assert!(mounts.contains(&"/etc/mqttd/gossip"));
    }
}
