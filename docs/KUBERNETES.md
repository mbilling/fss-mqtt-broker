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

## Admin API

Both paths can turn on the admin API ([ADMIN-API.md](ADMIN-API.md), CLI:
[ADMIN-CLI.md](ADMIN-CLI.md)): the chart's `admin` values or the CR's `spec.admin`. Every
pod serves it on port 9443 with its own cluster-bus certificate, so `secrets.peerTls` is
required. The port is on the headless Service only. Admin client certificates come from a
dedicated CA in a Secret with key `ca.crt`. Setup and the port-forward recipe are in the
[chart README](../deploy/helm/mqttd/README.md#admin-api).

## NetworkPolicy

Both paths can render an ingress `NetworkPolicy` for the broker pods: the chart's
`networkPolicy` values or the CR's `spec.networkPolicy`. It admits the peer bus, gossip and
admin ports only between the broker pods. Clients and health/metrics are admitted from the
peers you list, or from anywhere when a list is empty. Egress is not restricted, and it
needs an enforcing CNI. Details: [chart README](../deploy/helm/mqttd/README.md#networkpolicy).

## Rules

The rule engine ([RULES.md](RULES.md)) reads one TOML file, named by `MQTTD_RULES_FILE`.
It is unreleased (no release has it yet), so the chart's default image does not have it
yet: until a release does, set `image.repository` and `image.tag` to an image built from
source ([RULES.md § Get a build](RULES.md#1-get-a-build-that-has-the-rule-engine)).

Ship the file as a ConfigMap, and wire it in with the chart's extension values
`extraVolumes`, `extraVolumeMounts` and `extraEnv`:

```sh
mqttd --check-rules rules.toml     # a file that does not load refuses a pod's boot
kubectl -n mqttd create configmap mqttd-rules --from-file=rules.toml
```

```yaml
# rules-values.yaml: add `-f rules-values.yaml` to helm install / helm upgrade
extraVolumes:
  - name: rules
    configMap:
      name: mqttd-rules
extraVolumeMounts:
  - name: rules
    mountPath: /etc/mqttd/rules
    readOnly: true
extraEnv:
  - name: MQTTD_RULES_FILE
    value: /etc/mqttd/rules/rules.toml
```

Mount the ConfigMap as a directory, without `subPath`. The kubelet then updates the file in
place when the ConfigMap changes, and the chart's `config_watch_secs = 30` reloads the
rules on every pod without a restart; a `subPath` mount never sees the change. Running pods
reject a file that does not load, keep their rules and log `security reload REJECTED`
(holding back any ACL change in the same reload); a pod that starts with it does not boot.
The chart's `check-config` init container does not read the rules file, so run
`mqttd --check-rules` before you apply a change, and compare `mqttd_rules_info{checksum}`
across pods afterwards ([OPERATIONS.md](OPERATIONS.md#rules-adr-0083)).

The operator path cannot do this yet: the `MqttdCluster` CRD has no field that mounts a
volume or sets an environment variable beside its fixed secrets, so a rules file needs the
chart.

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
