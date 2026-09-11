# mqttd-operator Helm chart

**Verified against `v1.0.16` (2026-09-11).** Installs the `MqttdCluster`
reconciler ([ADR 0055](../../../docs/adr/0055-kubernetes-operator.md)). It
renders the same objects as the [mqttd chart](../mqttd/README.md) (render
parity is CI-gated), reports status from `/statusz`, and runs opt-in
remediations. Alert-only by default; no remediation deletes data.

Routed entry: [`docs/KUBERNETES.md`](../../../docs/KUBERNETES.md).

## Install

```sh
# CRDs ship in crds/ and are applied on first install.
helm install mqttd-operator deploy/helm/mqttd-operator -n mqttd
# Mint the same Secrets the broker chart needs, then:
kubectl -n mqttd apply -f deploy/helm/mqttd-operator/example-mqttdcluster.yaml
kubectl -n mqttd get mqc mqttd -w
```

RBAC is a **namespaced Role**. The controller watches only its own namespace.
Install one release per namespace that runs brokers.

## Values reference

| Key | Default | Purpose |
|---|---|---|
| `image` | `ghcr.io/mbilling/fss-mqtt-broker-operator`, tag = chart `appVersion` | Operator image (same release train as the broker). |
| `replicaCount` | `1` | Extra replicas are cold standbys behind a `coordination.k8s.io` Lease. |
| `logLevel` | `info` | `tracing_subscriber` EnvFilter. |
| `resources` | 50m CPU / 64Mi request, 256Mi limit | Operator budget. |
| `nodeSelector` / `tolerations` / `affinity` | empty | Placement. |

## `MqttdCluster` CRD reference

Group `mqttd.io`, kind `MqttdCluster`, shortName `mqc`, namespaced,
`v1alpha1`. Schema source:
[`crds/mqttdclusters.yaml`](crds/mqttdclusters.yaml).

### `spec` (required: `config`, `replicas`)

| Field | Type | Purpose |
|---|---|---|
| `replicas` | int | Cluster size. Pod 0 is the gossip founder. Odd ≥ 3 for durable quorum. |
| `config` | string | Broker TOML template — **the same contract as the chart's `values.config`**. Placeholders `__NODE_ID__`, `__SEEDS__`, `__PEER_ADVERTISE__`, `__READY_MIN__` (and optional `__STORE_MAX_BYTES__`) are rendered per pod. Durable-on with no `data_dir` crashloops (issue #240). |
| `image` | string | Broker image. Empty = the operator's pinned release. |
| `bootstrapPolicy` | `Guarded` (default) / `AllowRebootstrap` | Whether ordinal 0 may found. Not a remediation — must hold before a pod starts. |
| `persistence.size` / `storageClassName` | string | Per-pod PVC. |
| `persistence.expansionMaxSize` | string | Cap for brownout `ExpandPvc`. Unset = expansion refuses (alert only). |
| `persistence.storeMaxBytesPercent` | uint8 | Feeds `__STORE_MAX_BYTES__` as this % of the largest data-PVC so the watermark follows the volume. |
| `secrets.tls` / `acl` / `peerTls` / `gossipKey` | string | Secret/ConfigMap **names**. The operator never reads key material. |
| `gossipKeyRotation.newKeySecretRef` | string | Unattended three-phase SWIM rotation. Cleared when complete. |
| `remediation.brownout` | `Alert` (default) / `ExpandPvc` | Disk brownout. |
| `remediation.splitBrain` | `Alert` (default) / `Fence` | Two cluster identities. Fence deletes the new-founder **pod** and labels the PVC; it cannot delete the volume. |

### `status`

| Field | Purpose |
|---|---|
| `phase` | Printer column. |
| `clusterId` | Identity of the formed cluster. |
| `readyReplicas` | Ready broker pods. |
| `bootstrapped` | Founder has formed a cluster. |
| `brownout` | Disk/memory axis currently refusing growth. |
| `conditions[]` | `SplitBrain`, `Converged`, `RotationInProgress`, `Reconciled`, … |

Example: [`example-mqttdcluster.yaml`](example-mqttdcluster.yaml).

## Upgrading CRDs

`helm upgrade` updates the operator Deployment. Helm does **not** upgrade CRDs
it installed from `crds/`. After a chart upgrade, apply the CRD yourself:

```sh
kubectl apply -f deploy/helm/mqttd-operator/crds/mqttdclusters.yaml
```

Schema is pinned in CI against the operator's types.
