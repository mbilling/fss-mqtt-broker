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

use crate::crd::{BootstrapPolicy, MqttdCluster};
use kube::ResourceExt;
use serde_json::{json, Value};

/// Hex sha-256 of the config template — the pod annotation that makes a config
/// edit roll the `StatefulSet` (the chart's `checksum/config`). Without it a
/// changed `ConfigMap` would be projected into running pods with no restart, which is only
/// correct for the file-watched policy paths, not for restart-only settings.
///
/// Deliberate local copy of `mqttd::reload::sha256_hex`: the operator carries no
/// `mqtt-*` dependencies. Both are pinned to the same NIST vector — see
/// `checksum_is_sha256_lowercase_hex` here and in `mqttd/src/reload.rs`.
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
# The cluster-bus certificate is PER POD (issue #262): the bus binds node
# identity to the certificate's Subject CN, so one leaf shared by every pod
# drops every peer link. The Secret therefore carries one leaf per node id,
# keyed by pod name — and an ordinal with no leaf is a scale-up that outran
# its PKI. Fail HERE, naming the fix, rather than let the broker crash-loop
# on an unreadable path or (worse) form no mesh while looking healthy.
if [ -n "${PEER_TLS_DIR:-}" ]; then
  for f in "${POD_NAME}.crt" "${POD_NAME}.key" "ca.crt"; do
    if [ ! -s "${PEER_TLS_DIR}/${f}" ]; then
      echo "FATAL: no cluster-bus certificate material for ${POD_NAME}:" >&2
      echo "  ${PEER_TLS_DIR}/${f} is missing or empty." >&2
      echo "  The chart derives the peer-TLS env paths from the Secret, so it must" >&2
      echo "  carry ca.crt plus ${POD_NAME}.crt AND ${POD_NAME}.key (CN = the node id)." >&2
      echo "  An operator-supplied Secret with differently-named keys fails HERE," >&2
      echo "  by name, instead of as a bare broker filesystem error." >&2
      echo "  Mint or fix the leaves, re-apply the Secret, and retry:" >&2
      echo "    REPLICAS=${REPLICAS} ./deploy/helm/mqttd/bootstrap.sh" >&2
      exit 1
    fi
  done
fi
SEED0="\"${STS}-0.${HEADLESS_DOMAIN}:7946\""
SEED1="\"${STS}-1.${HEADLESS_DOMAIN}:7946\""
SEED2="\"${STS}-2.${HEADLESS_DOMAIN}:7946\""
if [ "$ORD" = "0" ]; then
  # Ordinal 0 founds the cluster precisely BECAUSE it renders no seeds. Once a cluster
  # exists, founding again is the split-brain bug: a pod-0 whose volume was lost would
  # mint a SECOND identity beside the live one. CLUSTER_ESTABLISHED disarms the founder
  # so pod-0 seeds to its peers like any joiner and a wiped volume REJOINS instead.
  # A 1-replica cluster has no survivor to split from, so it may still found.
  if [ "${CLUSTER_ESTABLISHED:-}" = "true" ] && [ "$REPLICAS" -gt 1 ]; then
    if [ "$REPLICAS" = "2" ]; then SEEDS="$SEED1"; else SEEDS="${SEED1}, ${SEED2}"; fi
    # An armed ordinal 0 is a JOINER in every respect, so it takes the joiner's
    # readiness floor too. Leaving it at 1 was justified as "with seeds it mints
    # nothing and bootstraps no lease group, so readiness gates on real admission" —
    # true only when DURABLE is on. With durable off there is no lease gate at all,
    # and member_count() counts self, so a floor of 1 let an isolated pod-0 report
    # Ready and serve clients an empty store. The floor is the only protection there.
    READY_MIN=$(( REPLICAS / 2 + 1 ))
  else
    SEEDS=""
    # Founder, or a single-node cluster: it must be able to come Ready ALONE, or
    # ordered bring-up never starts and a 1-replica deployment is never healthy.
    READY_MIN=1
  fi
else
  if [ "$ORD" = "1" ]; then SEEDS="$SEED0"; else SEEDS="${SEED0}, ${SEED1}"; fi
  READY_MIN=$(( REPLICAS / 2 + 1 ))
fi
# Every pod must ADVERTISE a routable peer-bus address, not its 0.0.0.0 bind — peers
# dial the advertised address over the headless service, and 0.0.0.0 is unroutable
# (the mesh never links). Per-pod DNS is <pod>.<headless>.<ns>.svc.cluster.local.
PEER_ADVERTISE="${POD_NAME}.${HEADLESS_DOMAIN}:7001"
# Same rule for the gossip plane (issue #396): a self-claimed 0.0.0.0 in gossip
# loops third parties back to their own socket.
SWIM_ADVERTISE="${POD_NAME}.${HEADLESS_DOMAIN}:7946"
sed -e "s/__NODE_ID__/${POD_NAME}/g" -e "s|__SEEDS__|${SEEDS}|g" \
  -e "s|__PEER_ADVERTISE__|${PEER_ADVERTISE}|g" \
  -e "s|__SWIM_ADVERTISE__|${SWIM_ADVERTISE}|g" \
  -e "s/__READY_MIN__/${READY_MIN}/g" \
  -e "s/__STORE_MAX_BYTES__/${STORE_MAX_BYTES:-0}/g" \
  "${TMPL_DIR:-/tmpl}/mqttd.toml.tmpl" > "${OUT_DIR:-/config}/mqttd.toml"
# A short write — e.g. a full ephemeral-storage volume — leaves sed exit 0 with a
# 0-byte file, and the broker then boots on pure DEFAULTS (no listeners, node id
# "node-local"): alive as a process, bound to nothing. An empty render is never a
# valid config, so fail the init container loudly instead of starting a broker
# that can only fail its probes.
if [ ! -s "${OUT_DIR:-/config}/mqttd.toml" ]; then
  echo "FATAL: rendered config is empty — is /config out of space?" >&2
  exit 1
fi
echo "rendered config for ${POD_NAME} (ordinal ${ORD}):"
cat "${OUT_DIR:-/config}/mqttd.toml"
"#;

/// Whether ordinal 0 must be stopped from founding (ADR 0055 T9).
///
/// True once the operator has observed a cluster identity (`status.bootstrapped`) and
/// the human has not asked to re-bootstrap. Deliberately a function of the CR alone, so
/// `render` stays pure and the parity gate can drive both states.
fn founding_disarmed(cr: &MqttdCluster) -> bool {
    cr.spec.bootstrap_policy == BootstrapPolicy::Guarded
        && cr.status.as_ref().and_then(|s| s.bootstrapped) == Some(true)
}

/// Default broker image when the CR does not pin one.
const DEFAULT_IMAGE: &str = concat!(
    "ghcr.io/mbilling/fss-mqtt-broker:",
    env!("CARGO_PKG_VERSION")
);
/// The shell image for the render init container (the broker image is distroless).
const INIT_IMAGE: &str =
    "busybox@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662";
/// Drain + graceful-shutdown allowance (chart default).
const TERMINATION_GRACE_SECONDS: i64 = 300;
/// Default PVC size when the CR does not set one.
const DEFAULT_PVC_SIZE: &str = "10Gi";
/// Mount path for the cluster-bus mTLS Secret — the chart's
/// `secrets.peerTls.mountPath` default, and the prefix of the `MQTTD_PEER_TLS_*`
/// paths the broker container is given.
const PEER_TLS_PATH: &str = "/etc/mqttd/cluster";
/// Mount path for the gossip-key Secret (the chart's `secrets.gossipKey.mountPath`).
const GOSSIP_PATH: &str = "/etc/mqttd/gossip";

/// Every object the operator owns for one `MqttdCluster`.
#[derive(Debug)]
pub struct Rendered {
    /// The pods' `ServiceAccount` — the `StatefulSet` names it, so the operator
    /// must create it (a `StatefulSet` referencing an absent SA cannot start a
    /// single pod; the T7 e2e caught exactly that).
    pub serviceaccount: Value,
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
        let mut v = vec![&self.serviceaccount, &self.configmap];
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
pub fn render(cr: &MqttdCluster, observed_pvc_max: Option<u64>) -> Rendered {
    let names = Names::of(cr);
    Rendered {
        serviceaccount: serviceaccount(&names),
        configmap: configmap(cr, &names),
        services: vec![headless_service(&names), client_service(&names)],
        pdb: pdb(&names),
        statefulset: statefulset(cr, &names, observed_pvc_max),
    }
}

fn serviceaccount(names: &Names) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": names.full,
            "namespace": names.namespace,
            "labels": labels(names),
        },
    })
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
            PEER_TLS_PATH,
            json!({ "name": "peer-tls", "secret": { "secretName": peer } }),
        );
    }
    if let Some(key) = s.and_then(|s| s.gossip_key.as_ref()) {
        add(
            "gossip-key",
            GOSSIP_PATH,
            json!({ "name": "gossip-key", "secret": { "secretName": key } }),
        );
    }
    (mounts, volumes)
}

#[allow(clippy::too_many_lines)] // one flat manifest mirroring the chart template
/// The watermark the config template's `__STORE_MAX_BYTES__` renders to (issue #228):
/// `storeMaxBytesPercent` of the LARGEST current data-PVC request, floored by the
/// declared `persistence.size` — so the watermark follows the volume the broker
/// actually has, including after a brownout-driven expansion, and a fresh cluster
/// (nothing observed yet) starts from the spec. `None` = the knob is unset and the
/// placeholder is not fed (templates that do not use it are unaffected).
fn store_max_bytes(cr: &MqttdCluster, observed_pvc_max: Option<u64>) -> Option<u64> {
    let persistence = cr.spec.persistence.as_ref()?;
    let percent = u64::from(persistence.store_max_bytes_percent?.clamp(1, 100));
    let spec = crate::remediate::parse_quantity(persistence.size.as_deref()?)?;
    let base = observed_pvc_max.map_or(spec, |o| o.max(spec));
    Some(base * percent / 100)
}

// One flat JSON object literal mirroring the chart's StatefulSet — long by field
// count, not complexity (the configmap/service renders keep the same shape).
#[allow(clippy::too_many_lines)]
fn statefulset(cr: &MqttdCluster, names: &Names, observed_pvc_max: Option<u64>) -> Value {
    let image = cr
        .spec
        .image
        .clone()
        .unwrap_or_else(|| DEFAULT_IMAGE.into());
    let headless_domain = format!("{}.{}.svc.cluster.local", names.headless, names.namespace);
    // The render init container's environment. CLUSTER_ESTABLISHED is appended only
    // once a cluster demonstrably exists (ADR 0055 T9), so a fresh CR renders exactly
    // what the chart does with its default — and can still bootstrap.
    let mut init_env = vec![
        json!({ "name": "POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } }),
        json!({ "name": "REPLICAS", "value": cr.spec.replicas.to_string() }),
        json!({ "name": "HEADLESS_DOMAIN", "value": headless_domain }),
    ];
    // Issue #228 (0055-T10): the watermark follows the volume. An env change rolls
    // the StatefulSet, the init container re-renders the config, and the broker
    // starts with the raised watermark — the brownout the expansion existed to
    // clear actually clears.
    if let Some(bytes) = store_max_bytes(cr, observed_pvc_max) {
        init_env.push(json!({ "name": "STORE_MAX_BYTES", "value": bytes.to_string() }));
    }
    if founding_disarmed(cr) {
        init_env.push(json!({ "name": "CLUSTER_ESTABLISHED", "value": "true" }));
    }
    let peer_tls = cr
        .spec
        .secrets
        .as_ref()
        .and_then(|s| s.peer_tls.as_ref())
        .is_some();
    let gossip_key = cr
        .spec
        .secrets
        .as_ref()
        .and_then(|s| s.gossip_key.as_ref())
        .is_some();
    // The render init container also mounts the peer-bus Secret, purely to arm the
    // per-pod-leaf preflight in RENDER_SCRIPT (issue #262).
    let mut init_mounts = vec![
        json!({ "name": "config-tmpl", "mountPath": "/tmpl" }),
        json!({ "name": "config", "mountPath": "/config" }),
    ];
    if peer_tls {
        init_env.push(json!({ "name": "PEER_TLS_DIR", "value": PEER_TLS_PATH }));
        init_mounts
            .push(json!({ "name": "peer-tls", "mountPath": PEER_TLS_PATH, "readOnly": true }));
    }
    // Secret material is referenced BY PATH, and the paths are DERIVED from the secret
    // names here rather than left to the CR author — the fix for issue #262, mirroring
    // the chart's `mqttd.brokerEnv`. Before it, both paths MOUNTED the cluster-bus
    // Secret and referenced nothing in it, so the bus stayed plaintext while the
    // deployment looked mutually authenticated. Each pod gets ITS OWN leaf through
    // Kubernetes' dependent-env expansion of $(POD_NAME): a StatefulSet pod template
    // cannot select a Secret per ordinal, but every pod can select a KEY within one.
    let mut broker_env: Vec<Value> = Vec::new();
    if peer_tls {
        broker_env.push(
            json!({ "name": "POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } }),
        );
        broker_env.push(
            json!({ "name": "MQTTD_PEER_TLS_CA", "value": format!("{PEER_TLS_PATH}/ca.crt") }),
        );
        broker_env.push(
            json!({ "name": "MQTTD_PEER_TLS_CERT", "value": format!("{PEER_TLS_PATH}/$(POD_NAME).crt") }),
        );
        broker_env.push(
            json!({ "name": "MQTTD_PEER_TLS_KEY", "value": format!("{PEER_TLS_PATH}/$(POD_NAME).key") }),
        );
    }
    if gossip_key {
        broker_env.push(
            json!({ "name": "MQTTD_SWIM_KEY_FILE", "value": format!("{GOSSIP_PATH}/swim-key") }),
        );
    }
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

    let mut sts = json!({
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
            // Both Retain (issue #97). Deleting a scaled-down pod's PVC is correct
            // only while a SURVIVOR remains to receive the ADR 0043 drain, and
            // Kubernetes applies the policy uniformly — at replicas=0 the same
            // setting erases the only copy of every session and retained message.
            "persistentVolumeClaimRetentionPolicy": {
                "whenDeleted": "Retain",
                "whenScaled": "Retain",
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
                            "env": init_env,
                            "volumeMounts": init_mounts,
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
    });
    // Emitted only when there is something to emit, so a secret-less CR renders a
    // container with NO env key — exactly what the chart's `{{- with ... }}` does.
    if !broker_env.is_empty() {
        sts["spec"]["template"]["spec"]["containers"][0]["env"] = json!(broker_env);
    }
    sts
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
    use super::{render, Rendered, RENDER_SCRIPT};
    use crate::crd::{BootstrapPolicy, MqttdCluster};

    /// Issue #228 (0055-T10): the `__STORE_MAX_BYTES__` watermark env follows the
    /// volume — the declared percent of the LARGEST observed data-PVC request,
    /// floored by the spec size (a fresh cluster starts from the spec; k8s never
    /// shrinks a PVC, so the value never regresses). Knob unset = no env, so
    /// templates that do not use the placeholder are unaffected.
    #[test]
    fn the_watermark_env_follows_the_observed_pvc_size() {
        fn env_of(r: &Rendered) -> String {
            r.statefulset["spec"]["template"]["spec"]["initContainers"][0]["env"].to_string()
        }
        const GI: u64 = 1024 * 1024 * 1024;
        let mut cr = sample();
        cr.spec.persistence = Some(crate::crd::Persistence {
            store_max_bytes_percent: Some(80),
            size: Some("10Gi".into()),
            storage_class_name: None,
            expansion_max_size: Some("20Gi".into()),
        });
        // Nothing observed yet: the spec size is the base — 80% of 10Gi.
        assert!(
            env_of(&render(&cr, None)).contains(&(8 * GI).to_string()),
            "fresh cluster: watermark from the spec size"
        );
        // Observed 20Gi (after a brownout expansion): the watermark rises with it.
        assert!(
            env_of(&render(&cr, Some(20 * GI))).contains(&(16 * GI).to_string()),
            "expanded: watermark follows the observed size"
        );
        // An observed value below the spec never lowers the watermark.
        assert!(
            env_of(&render(&cr, Some(5 * GI))).contains(&(8 * GI).to_string()),
            "the spec size floors the base"
        );
        // Knob unset: the env is absent and the template is untouched.
        cr.spec
            .persistence
            .as_mut()
            .expect("persistence set above")
            .store_max_bytes_percent = None;
        assert!(
            !env_of(&render(&cr, Some(20 * GI))).contains("STORE_MAX_BYTES"),
            "no knob, no env"
        );
    }

    /// Known-answer parity pin (NIST SHA-256 vector for "abc"): the operator's
    /// `config_checksum` is a deliberate copy of `mqttd::reload::sha256_hex`
    /// (the operator carries no `mqtt-*` deps). The same vector is pinned in
    /// `mqttd/src/reload.rs`, so the two cannot silently diverge in algorithm
    /// or hex case — the chart's `checksum/config` annotation and the broker's
    /// config stamp must agree.
    #[test]
    fn checksum_is_sha256_lowercase_hex() {
        assert_eq!(
            super::config_checksum("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

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

    /// Run the REAL init script with the given inputs; return the rendered config.
    ///
    /// The script is the mechanism — it decides, in shell, which pods may found a cluster
    /// and what each one's readiness floor is — and it is byte-identical in the chart and
    /// the operator, so until now nothing exercised it outside a kind cluster.
    fn render_script_output(ord: &str, established: Option<&str>, replicas: &str) -> String {
        // Unique per invocation: tests run in parallel threads and several share input
        // combinations, so a name derived from the inputs alone would have one test
        // wiping the directory another is mid-render in.
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "mqttd-render-{ord}-{}-{replicas}-{}",
            established.unwrap_or("unset"),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(
            dir.join("mqttd.toml.tmpl"),
            "seeds = [__SEEDS__]\nready_min_members = __READY_MIN__\nid = \"__NODE_ID__\"\npeer = \"__PEER_ADVERTISE__\"\n",
        )
        .expect("template");
        let mut cmd = std::process::Command::new("sh");
        cmd.arg("-c")
            .arg(RENDER_SCRIPT)
            .env("POD_NAME", format!("mqttd-{ord}"))
            .env("REPLICAS", replicas)
            .env("HEADLESS_DOMAIN", "mqttd-headless.ns.svc.cluster.local")
            .env("TMPL_DIR", &dir)
            .env("OUT_DIR", &dir);
        if let Some(v) = established {
            cmd.env("CLUSTER_ESTABLISHED", v);
        }
        let out = cmd.output().expect("run the render script");
        assert!(
            out.status.success(),
            "script failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::fs::read_to_string(dir.join("mqttd.toml")).expect("rendered config")
    }

    /// Just the `seeds = [...]` line. The node's OWN address also appears in the rendered
    /// config (as `peer_advertise`), so a whole-file search cannot tell "seeds to itself"
    /// from "advertises itself".
    fn seeds_line(config: &str) -> String {
        config
            .lines()
            .find(|l| l.starts_with("seeds ="))
            .unwrap_or_else(|| panic!("no seeds line in: {config}"))
            .to_string()
    }

    /// Which pods may FOUND a cluster, and who each one seeds to.
    #[test]
    fn the_render_script_decides_seeds_by_ordinal_and_guard() {
        let render = render_script_output;

        // Ordinal 0, guard OFF: seedless, so it founds — the first-bootstrap path that
        // must keep working, or nothing ever forms.
        let c = render("0", None, "3");
        assert_eq!(
            seeds_line(&c),
            "seeds = []",
            "pod-0 must found a fresh cluster"
        );
        assert!(c.contains("ready_min_members = 1"), "{c}");

        // Ordinal 0, guard ON: seeded to its PEERS (1 and 2, never itself), so a wiped
        // volume rejoins instead of founding a second cluster.
        let c = render("0", Some("true"), "3");
        let seeds = seeds_line(&c);
        assert!(
            seeds.contains("mqttd-1.mqttd-headless"),
            "must seed to pod-1: {seeds}"
        );
        assert!(
            seeds.contains("mqttd-2.mqttd-headless"),
            "must seed to pod-2: {seeds}"
        );
        assert!(
            !seeds.contains("mqttd-0.mqttd-headless"),
            "must NOT seed to itself — a node seeded only to itself still founds: {seeds}"
        );
        assert!(
            c.contains("ready_min_members = 2"),
            "an armed ordinal 0 is a joiner and takes the joiner's majority floor: {c}"
        );

        // Two replicas: the only peer is ordinal 1.
        let c = render("0", Some("true"), "2");
        let seeds = seeds_line(&c);
        assert!(seeds.contains("mqttd-1.mqttd-headless"), "{seeds}");
        assert!(
            !seeds.contains("mqttd-2.mqttd-headless"),
            "no pod-2 exists: {seeds}"
        );

        // ONE replica: no survivor to split from, so the guard must not strand it.
        let c = render("0", Some("true"), "1");
        assert_eq!(
            seeds_line(&c),
            "seeds = []",
            "a single-node cluster has no survivor to split from and must still found"
        );

        // Joiners are unaffected by the guard, armed or not.
        for guard in [None, Some("true")] {
            let c = render("1", guard, "3");
            assert!(seeds_line(&c).contains("mqttd-0.mqttd-headless"), "{c}");
            assert!(c.contains("ready_min_members = 2"), "majority floor: {c}");
            let c = render("2", guard, "3");
            let seeds = seeds_line(&c);
            assert!(seeds.contains("mqttd-0.mqttd-headless"), "{seeds}");
            assert!(seeds.contains("mqttd-1.mqttd-headless"), "{seeds}");
        }
    }

    /// The readiness floor follows the guard, and getting it wrong is what let an
    /// isolated pod-0 serve clients an empty store.
    ///
    /// An UNARMED ordinal 0 is the founder: it must come Ready alone or ordered bring-up
    /// never starts, and a single-node deployment is never healthy. An ARMED ordinal 0 is
    /// a joiner and takes the joiner's MAJORITY floor. The floor was previously left at 1
    /// for ordinal 0 on the reasoning that an unadmitted node is caught by the lease gate
    /// — true only with durable sessions ON. Without a durable plane `lease_group_ready`
    /// is `None` and cannot gate, and `member_count()` counts self, so the floor is the only
    /// protection there.
    #[test]
    fn the_readiness_floor_follows_the_founder_guard() {
        fn floor(ord: &str, established: Option<&str>, replicas: &str) -> String {
            let c = render_script_output(ord, established, replicas);
            c.lines()
                .find(|l| l.starts_with("ready_min_members ="))
                .unwrap_or_else(|| panic!("no floor line in: {c}"))
                .to_string()
        }

        // Founder, and the lone single node: healthy by itself.
        assert_eq!(floor("0", None, "3"), "ready_min_members = 1");
        assert_eq!(floor("0", Some("true"), "1"), "ready_min_members = 1");

        // Armed ordinal 0 = joiner = majority.
        assert_eq!(floor("0", Some("true"), "3"), "ready_min_members = 2");
        assert_eq!(floor("0", Some("true"), "4"), "ready_min_members = 3");
        assert_eq!(floor("0", Some("true"), "5"), "ready_min_members = 3");

        // Joiners are unchanged, armed or not.
        for guard in [None, Some("true")] {
            assert_eq!(floor("1", guard, "3"), "ready_min_members = 2");
            assert_eq!(floor("2", guard, "5"), "ready_min_members = 3");
        }
    }

    /// The env that arms the guard is emitted ONLY when the CR proves a cluster exists,
    /// so a fresh CR renders exactly what the chart's default does — and can bootstrap.
    #[test]
    fn cluster_established_is_rendered_only_once_bootstrapped() {
        fn env_of(r: &Rendered) -> String {
            r.statefulset["spec"]["template"]["spec"]["initContainers"][0]["env"].to_string()
        }
        let fresh = sample();
        assert!(
            !env_of(&render(&fresh, None)).contains("CLUSTER_ESTABLISHED"),
            "a CR with no status must render the bootstrap-capable form"
        );

        let mut latched = sample();
        latched.status = Some(crate::crd::MqttdClusterStatus {
            bootstrapped: Some(true),
            ..Default::default()
        });
        assert!(
            env_of(&render(&latched, None)).contains("CLUSTER_ESTABLISHED"),
            "once an identity has been observed, ordinal 0 must not found again"
        );

        // Break glass: the human asked to re-bootstrap, so the guard comes off even
        // though the latch is set.
        let mut broken_glass = latched.clone();
        broken_glass.spec.bootstrap_policy = BootstrapPolicy::AllowRebootstrap;
        assert!(
            !env_of(&render(&broken_glass, None)).contains("CLUSTER_ESTABLISHED"),
            "AllowRebootstrap must re-permit founding"
        );
    }

    /// The rendered set is the chart's object set, named the chart's way.
    #[test]
    fn renders_the_chart_object_set() {
        let r = render(&sample(), None);
        assert_eq!(
            r.all().len(),
            6,
            "serviceaccount + configmap + 2 services + pdb + statefulset"
        );
        assert_eq!(
            r.serviceaccount["metadata"]["name"],
            r.statefulset["spec"]["template"]["spec"]["serviceAccountName"],
            "the StatefulSet must reference an SA the operator actually creates"
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
        let r = render(&sample(), None);
        let spec = &r.statefulset["spec"];
        assert_eq!(spec["podManagementPolicy"], "OrderedReady");
        assert_eq!(
            spec["persistentVolumeClaimRetentionPolicy"]["whenScaled"], "Retain",
            "a scale-down must not be able to destroy the only copy (issue #97)"
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
        let bare = render(&sample(), None);
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
        let wired = render(&cr, None);
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
