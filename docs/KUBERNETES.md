# Kubernetes

**Verified against `v1.0.16` (2026-09-11).** The Kubernetes user's primary
document (ADR 0070 T5). Charts, values, and the `MqttdCluster` CRD live next to
the manifests; this page is the routed entry.

| Surface | Where |
|---|---|
| Broker Helm chart | [`deploy/helm/mqttd/README.md`](../deploy/helm/mqttd/README.md) |
| Operator Helm chart | [`deploy/helm/mqttd-operator/README.md`](../deploy/helm/mqttd-operator/README.md) |
| Values reference (broker) | the broker chart README, generated from `values.yaml` comments |
| `MqttdCluster` CRD | [`deploy/helm/mqttd-operator/crds/mqttdclusters.yaml`](../deploy/helm/mqttd-operator/crds/mqttdclusters.yaml) and the operator chart README |
| Example CR | [`deploy/helm/mqttd-operator/example-mqttdcluster.yaml`](../deploy/helm/mqttd-operator/example-mqttdcluster.yaml) |
| Day-2 procedures | [OPERATIONS.md](OPERATIONS.md) |
| Shipped alerts / dashboards | [OPERATIONS.md](OPERATIONS.md#shipped-alerting), [`deploy/observability/`](../deploy/observability/) |

## Two install paths, one object graph

- **`helm install` of `deploy/helm/mqttd`** renders a StatefulSet, headless
  Service, ConfigMap, optional ServiceMonitor / PrometheusRule, and the
  decommission `preStop`. `bootstrap.sh` mints the gossip key, server TLS,
  per-pod cluster-bus certificates and a starter ACL.
- **`MqttdCluster` via the operator** renders the **same** objects (render
  parity is CI-gated, ADR 0055). The CRD's `spec.config` is the same TOML
  template as the chart's `values.config`, with `__NODE_ID__`, `__SEEDS__`,
  `__PEER_ADVERTISE__`, `__READY_MIN__` filled per pod.

Pick one path per namespace. The operator's RBAC is namespaced; install one
operator release per namespace that runs brokers. Helm installs CRDs but does
**not** upgrade them — see the operator README.

## Defaults that surprise people

- **`replicaCount` / `spec.replicas` must be odd and ≥ 3** for a durable
  cluster. **2 is worse than 1**: a quorum of two *is* two, so losing either
  node blocks durable writes.
- Durable-on with no `data_dir` **crashloops** (issue #240) unless
  `allow_ephemeral` is set. The chart default already sets
  `data_dir = "/var/lib/mqttd"` plus a PVC.
- Every destructive operator remediation is **opt-in**. Alert-only is the
  default; the operator's RBAC cannot delete a PVC.
- `resources: {}` — set a memory limit; the broker's watermark is not a
  cgroup ceiling ([SIZING.md](SIZING.md)).

## Quick start

```sh
NS=mqttd REPLICAS=3 ./deploy/helm/mqttd/bootstrap.sh
helm install mqttd deploy/helm/mqttd -n mqttd \
  --set replicaCount=3 \
  --set secrets.tls.secretName=mqttd-tls \
  --set secrets.peerTls.secretName=mqttd-peer-tls \
  --set secrets.gossipKey.secretName=mqttd-gossip
```

Operator path: run the same `bootstrap.sh`, install
`deploy/helm/mqttd-operator`, then `kubectl apply -f deploy/helm/mqttd-operator/example-mqttdcluster.yaml`.
