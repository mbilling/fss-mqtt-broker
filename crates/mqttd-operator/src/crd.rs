//! The `MqttdCluster` custom resource ([ADR 0055](../../../docs/adr/0055-kubernetes-operator.md) §2).
//!
//! `v1alpha1` says what it means (the Kubernetes alpha-API convention): this
//! schema may change until promoted to `v1beta1` — a promotion act of its own,
//! versioned independently of the broker's semver, which froze at v1.0.0.
//! The spec deliberately mirrors the Helm chart's contract (same TOML config
//! template, same by-path secret references, same persistence knobs) — the
//! render-parity test (0055-T2) keeps the two deployment paths identical.
//! Every **destructive** remediation is opt-in per scenario: the defaults are
//! exactly as conservative as having no operator (`Alert`, never `Act`).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The desired state of one mqttd cluster.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "mqttd.io",
    version = "v1alpha1",
    kind = "MqttdCluster",
    namespaced,
    status = "MqttdClusterStatus",
    shortname = "mqc",
    doc = "An mqttd broker cluster managed by the operator (ADR 0055)",
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"ClusterId","type":"string","jsonPath":".status.clusterId"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct MqttdClusterSpec {
    /// Cluster size. Pod 0 is the gossip founder; use an odd number >= 3 for a
    /// durable quorum (the same contract as the chart's `replicaCount`).
    pub replicas: i32,
    /// The broker image (repository:tag). Defaults to the operator's pinned
    /// release when empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// The broker config TOML template — the SAME contract as the chart's
    /// `values.config` (ADR 0046): `__NODE_ID__`, `__SEEDS__`,
    /// `__PEER_ADVERTISE__`, and `__READY_MIN__` are rendered per pod.
    ///
    /// A config that omits `[node] data_dir` with durable sessions on (the
    /// default) now CRASHLOOPS with the issue #240 refusal — the check-config
    /// init container rejects it before the broker starts. Set `data_dir`
    /// (the chart default `/var/lib/mqttd` plus a PVC already does), or add
    /// `[durable] allow_ephemeral = true` for a throwaway cluster.
    pub config: String,
    /// Secret material by PATH (ADR 0046 T5), mirroring the chart's `secrets`
    /// block. The operator only ever references these Secrets — it never reads
    /// key material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<SecretRefs>,
    /// Per-pod persistent volume settings (mirrors the chart's `persistence`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<Persistence>,
    /// Remediation switches (ADR 0055 §3): every destructive action is opt-in;
    /// the default posture is Alert-only.
    #[serde(default)]
    pub remediation: Remediation,
    /// Whether ordinal 0 may found a cluster (ADR 0055 §3.5, T9).
    ///
    /// NOT a remediation: the guard is a rendering property that must hold before a
    /// pod starts, and it applies with the default Alert-only posture — an operator
    /// installed with defaults must still be protected from a lost pod-0 volume.
    #[serde(default)]
    pub bootstrap_policy: BootstrapPolicy,
    /// Presence starts an unattended three-phase SWIM key rotation
    /// (ADR 0003 / 0055 §3.3): the operator stages `newKeySecretRef`, flips the
    /// sealer, and closes the window, gating each phase on the ADR 0054
    /// verification signals. Cleared by the operator when the rotation completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gossip_key_rotation: Option<GossipKeyRotation>,
}

/// By-path secret references, mirroring the chart's `secrets` values block.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretRefs {
    /// Server TLS keypair (a `kubernetes.io/tls` Secret).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<String>,
    /// Topic ACL policy (`ConfigMap` name, key `acl.toml`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acl: Option<String>,
    /// Cluster-bus mTLS material: a Secret with `ca.crt` plus ONE LEAF PER POD, keyed by
    /// pod name (`<pod>.crt` / `<pod>.key`). Each pod is given its own by path, because the
    /// bus binds node identity to the certificate's Subject CN — one shared leaf drops every
    /// peer link (issue #262). Every leaf needs CN = the pod name, a SAN covering that pod's
    /// `<pod>.<headless>.<ns>.svc.cluster.local` advertise host, both serverAuth and
    /// clientAuth EKUs, and an ECDSA P-256/P-384 or Ed25519 PKCS#8 key (never RSA — the same
    /// key signs gossip). A new ordinal needs its own leaf before it can start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_tls: Option<String>,
    /// Gossip key file (Secret with key `swim-key`). Setting this together with `peerTls`
    /// arms signed per-node gossip (ADR 0022).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gossip_key: Option<String>,
}

/// Per-pod persistent volume settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Persistence {
    /// PVC size (e.g. `10Gi`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    /// `StorageClass`; empty = the cluster default. Brownout `ExpandPvc`
    /// remediation requires a class with `allowVolumeExpansion`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    /// Renders `__STORE_MAX_BYTES__` in the config template as this percentage of
    /// the LARGEST current data-PVC request (floored by `size`), so the broker's
    /// disk watermark follows the volume it actually has — including after a
    /// brownout-driven expansion (0055-T10, issue #228). Unset = the placeholder is
    /// not fed (templates that do not use it are unaffected). Pair with
    /// `store_max_bytes = __STORE_MAX_BYTES__` under `[durable]` in the template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_max_bytes_percent: Option<u8>,
    /// Upper bound for brownout-driven PVC expansion (ADR 0055 §3.2). Unset =
    /// expansion remediation refuses to act (alert only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expansion_max_size: Option<String>,
}

/// Per-scenario remediation switches. `Alert` sets conditions + Events only;
/// `Act` additionally performs the (bounded, documented) remediation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Remediation {
    /// Split-brain (two cluster identities observed, ADR 0054 T2):
    /// `Alert` (default) or `Fence` — delete the new-founder pod, quarantine
    /// its PVC by label (never delete data), seed-override recovery.
    #[serde(default)]
    pub split_brain: SplitBrainAction,
    /// Disk brownout (ADR 0041 §5): `Alert` (default) or `ExpandPvc` — grow PVC
    /// sizes up to `persistence.expansionMaxSize`. Pair with
    /// `persistence.storeMaxBytesPercent` + a `__STORE_MAX_BYTES__` placeholder in
    /// the config template so the broker's disk watermark rises WITH the volume
    /// (0055-T10, issue #228) and the brownout actually clears; without them the
    /// watermark stays where the template put it — a manual config roll.
    #[serde(default)]
    pub brownout: BrownoutAction,
}

/// What to do on a detected split-brain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SplitBrainAction {
    /// Conditions + Events only (the gossip-level containment of ADR 0054 T2
    /// is already limiting the blast radius).
    #[default]
    Alert,
    /// Fence the new founder: delete its pod and quarantine its PVC by label
    /// (never deleting data). Fires at most once per node. Preventing the
    /// re-founding in the first place is `bootstrapPolicy`, not this.
    Fence,
}

/// Whether ordinal 0 is still allowed to found a cluster.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum BootstrapPolicy {
    /// Once an identity has been observed (`status.bootstrapped`), ordinal 0 renders
    /// SEEDS and can only ever join. A pod-0 whose volume is lost rejoins and
    /// back-fills instead of founding a second cluster beside the survivors.
    #[default]
    Guarded,
    /// Break glass: let ordinal 0 found again, and clear the latch. For total loss of
    /// every volume, or a deliberate re-bootstrap beside a cluster being abandoned.
    /// The operator raises a condition and an Event every reconcile while this is set,
    /// so leaving it on is loud — it disables the protection entirely.
    AllowRebootstrap,
}

/// What to do on a disk brownout.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum BrownoutAction {
    /// Conditions + Events only.
    #[default]
    Alert,
    /// Grow PVCs (bounded by `persistence.expansionMaxSize`). With
    /// `persistence.storeMaxBytesPercent` set and a `__STORE_MAX_BYTES__`
    /// placeholder in the config template, the broker's disk watermark rises with
    /// the volume on the next reconcile's roll and the brownout clears end to end
    /// (0055-T10, issue #228); without them the watermark stays where the template
    /// put it and the broker keeps refusing writes on the larger volume.
    ExpandPvc,
}

/// An unattended gossip key rotation request (ADR 0055 §3.3).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GossipKeyRotation {
    /// The Secret holding the NEW key (key `swim-key`). The operator re-points
    /// references and rolls; it never reads the material.
    pub new_key_secret_ref: String,
}

/// The observed state, aggregated from `/statusz` + metrics (ADR 0054).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MqttdClusterStatus {
    /// Coarse lifecycle: `Pending`, `Forming`, `Ready`, `Degraded`, `SplitBrain`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Pods currently passing readiness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// Mesh members observed (from `/statusz`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub members: Option<i32>,
    /// The cluster identity every pod must agree on (ADR 0054 T2). Two values
    /// across the fleet is the `SplitBrain` condition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    /// Whether any pod reports disk brownout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brownout: Option<bool>,
    /// Whether a cluster has ever been observed to exist (ADR 0055 T9).
    ///
    /// Latched the first time exactly one cluster identity is seen from a REACHABLE
    /// pod, and monotonic thereafter — `patch_status` merges, and this field is
    /// omitted when there is no opinion, so a pass that observes nothing leaves the
    /// prior value alone. Once latched, ordinal 0 renders seeds and can no longer
    /// found. Cleared only under `bootstrapPolicy: AllowRebootstrap`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrapped: Option<bool>,
    /// The spec generation this status reflects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Kubernetes-conventional conditions (`SplitBrain`, `Converged`,
    /// `RotationInProgress`, `Reconciled`, …).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<StatusCondition>,
}

/// A minimal Kubernetes-style condition (own type: k8s-openapi's `Condition`
/// does not derive `JsonSchema`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StatusCondition {
    /// Condition type, e.g. `Reconciled`, `SplitBrain`.
    pub r#type: String,
    /// `True` / `False` / `Unknown`.
    pub status: String,
    /// Machine-readable reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Human-readable detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// RFC 3339 transition timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::MqttdCluster;
    use kube::CustomResourceExt;

    /// The committed CRD manifest (deploy/crds/) must equal the generated one —
    /// the schema cannot drift from the Rust types (regenerate with
    /// `cargo run -p mqttd-operator --bin gen_crd`).
    #[test]
    fn the_committed_crd_matches_the_generated_schema() {
        let generated = serde_json::to_string_pretty(&MqttdCluster::crd()).unwrap();
        let committed = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../deploy/crds/mqttd.io_mqttdclusters.json"
        ));
        assert_eq!(
            committed.trim(),
            generated.trim(),
            "CRD drift: regenerate with `cargo run -p mqttd-operator --bin gen_crd > deploy/crds/mqttd.io_mqttdclusters.json`"
        );
    }

    /// The operator Helm chart ships the CRD too (0055-T8, issue #252), as a
    /// byte-identical copy — JSON is valid YAML, so the chart's `crds/` file is
    /// the same bytes under a `.yaml` name, and this pin makes drift between
    /// the installable copy and the generated schema impossible. Regenerate
    /// both from the same source:
    /// `cargo run -p mqttd-operator --bin gen_crd | tee deploy/crds/mqttd.io_mqttdclusters.json > deploy/helm/mqttd-operator/crds/mqttdclusters.yaml`
    #[test]
    fn the_chart_crd_is_the_same_bytes_as_the_generated_schema() {
        let generated = serde_json::to_string_pretty(&MqttdCluster::crd()).unwrap();
        let chart_copy = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../deploy/helm/mqttd-operator/crds/mqttdclusters.yaml"
        ));
        assert_eq!(
            chart_copy.trim(),
            generated.trim(),
            "the chart's crds/mqttdclusters.yaml drifted from the generated CRD — \
             regenerate it together with deploy/crds/mqttd.io_mqttdclusters.json"
        );
    }

    /// Destructive remediations default OFF (ADR 0055: Alert, never Act).
    #[test]
    fn remediation_defaults_are_alert_only() {
        let spec: super::MqttdClusterSpec = serde_json::from_value(serde_json::json!({
            "replicas": 3,
            "config": "[node]\n"
        }))
        .unwrap();
        assert_eq!(spec.remediation.split_brain, super::SplitBrainAction::Alert);
        assert_eq!(spec.remediation.brownout, super::BrownoutAction::Alert);
    }
}
