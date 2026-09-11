# mqttd Helm chart

**Verified against `v1.0.16` (2026-09-11).** Deploys the broker as a StatefulSet
with per-pod volumes, a self-forming gossip mesh, decommission-drain on
scale-down, and a quorum-safe one-at-a-time rollout
([ADR 0047](../../../docs/adr/0047-kubernetes-deployment.md)).

The routed Kubernetes page is [`docs/KUBERNETES.md`](../../../docs/KUBERNETES.md).
Day-2 procedures: [`docs/OPERATIONS.md`](../../../docs/OPERATIONS.md).

## Install

```sh
NS=mqttd REPLICAS=3 ./deploy/helm/mqttd/bootstrap.sh
helm install mqttd deploy/helm/mqttd -n mqttd \
  --set replicaCount=3 \
  --set secrets.tls.secretName=mqttd-tls \
  --set secrets.peerTls.secretName=mqttd-peer-tls \
  --set secrets.gossipKey.secretName=mqttd-gossip
```

`bootstrap.sh` mints the gossip key, server TLS, **one cluster-bus certificate
per node**, and a starter ACL, then prints the `--set` flags. Replace the
throwaway PKI before production.

## Values reference

Every key is commented in [`values.yaml`](values.yaml). Top-level groups:

| Key | Default | Purpose |
|---|---|---|
| `image` | `ghcr.io/mbilling/fss-mqtt-broker`, tag = chart `appVersion` | Broker image. |
| `initImage` | busybox, pinned by digest | Renders per-pod config (distroless broker has no shell). |
| `replicaCount` | `3` | Cluster size. Odd and ≥ 3 for durable quorum. **2 is worse than 1.** |
| `config` | TOML template | Becomes the ConfigMap. Placeholders `__NODE_ID__`, `__SEEDS__`, `__PEER_ADVERTISE__`, `__READY_MIN__` are filled per pod. Keep secrets out of here. |
| `secrets.tls` | unset | `kubernetes.io/tls` Secret (`tls.crt` / `tls.key`). |
| `secrets.acl` | unset | ConfigMap/Secret key `acl.toml`. Missing ACL is deny-open and logged `INSECURE`. |
| `secrets.peerTls` | unset | `ca.crt` plus `<pod>.crt` / `<pod>.key` per ordinal. One shared cert cannot work. |
| `secrets.gossipKey` | unset | Secret key `swim-key`. Together with `peerTls` arms signed gossip. |
| `extraEnv` | `[]` | Extra `MQTTD_*` (or any) env on the broker container. |
| `clusterEstablished` | `false` | `false` on first install so pod-0 may found. Set `true` after the cluster is healthy so a lost pod-0 volume cannot re-bootstrap. |
| `persistence` | enabled, `10Gi` | Per-pod PVC for `/var/lib/mqttd`. |
| `terminationGracePeriodSeconds` | matches decommission drain | `preStop` runs `mqttd --decommission`. |
| `podDisruptionBudget` | enabled | At most one disruption. |
| `readinessProbe` / `livenessProbe` / `startupProbe` | `/readyz`, `/livez` | Health bind is `8080` in the default config. |
| `checkConfig` | enabled | Init container runs `mqttd --check-config`. |
| `service` | TLS 8883 | Client Service. |
| `metrics.podAnnotations` | `true` | `prometheus.io/scrape` on the pod. |
| `metrics.serviceMonitor.enabled` | `false` | Prometheus Operator scrape of `/metrics`. |
| `metrics.prometheusRule.enabled` | `false` | Shipped alerting rules (ADR 0070 T4). Enable when the PrometheusRule CRD is present. |
| `resources` | `{}` | **Set a memory limit.** The broker watermark is not a cgroup ceiling. |
| `bridge` | disabled | Optional in-chart boundary bridge. |

Full commented defaults: [`values.yaml`](values.yaml). Broker knobs inside
`config:` match [`docs/CONFIGURATION.md`](../../../docs/CONFIGURATION.md).

## Shipped alerting

With `metrics.prometheusRule.enabled=true` the chart installs a `PrometheusRule`
whose expressions match the runbooks in
[`docs/OPERATIONS.md`](../../../docs/OPERATIONS.md#shipped-alerting). Production
Grafana dashboards live in
[`deploy/observability/grafana/`](../../observability/grafana/) — not only inside
the experimental `demo/` stack.

## Related

- Operator / CRD: [`../mqttd-operator/README.md`](../mqttd-operator/README.md)
- Compose / systemd (no Kubernetes): [`../../README.md`](../../README.md)
