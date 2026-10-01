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
| `admin` | disabled | The authenticated admin API on port 9443 of every pod ([below](#admin-api)). Needs `secrets.peerTls`. |
| `networkPolicy` | disabled | Ingress `NetworkPolicy` for the broker pods ([below](#networkpolicy)). |
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

## Admin API

`admin.enabled=true` turns on the admin API ([`docs/ADMIN-API.md`](../../../docs/ADMIN-API.md),
CLI: [`docs/ADMIN-CLI.md`](../../../docs/ADMIN-CLI.md)) on every pod. Each pod serves it on
port 9443 with **its own cluster-bus certificate**, so `secrets.peerTls` is required: that
certificate already names the pod and chains to the cluster CA, which is what lets any pod
answer the cluster view and forward kick/purge to the pod holding a client. The port is on
the headless Service only, never on the client Service.

```yaml
admin:
  enabled: true
  viewers: ["CN=oncall"]
  operators: ["CN=sre-lead, O=example"]
  clientCa:
    secretName: mqttd-admin-ca   # key ca.crt: the CA that issues admin CLIENT certificates
```

Use a **dedicated** client CA. With the cluster CA, every unlisted node certificate is
admitted as the read-only `peer` role instead of refused.

Put the subject lists in a values file. `helm --set` splits on commas, so
`--set admin.operators[0]=CN=sre-lead, O=example` breaks the subject in two; escape the
comma (`\,`) if you must use `--set`.

Reach it from your workstation with a port-forward. `--server-name` is the pod's DNS name,
because that is what its certificate names, and `--ca` is the **cluster** CA, which issued it
(`bootstrap.sh` keeps it as `ca/cluster-ca.pem` under its PKI directory):

```sh
kubectl -n mqttd port-forward pod/mqttd-0 19443:9443 &
mqttd --admin --url https://127.0.0.1:19443 \
  --server-name mqttd-0.mqttd-headless.mqttd.svc.cluster.local \
  --ca cluster-ca.pem --cert me.pem --key me.key cluster
```

The scripted check is in `scripts/k8s/kind-smoke.sh`: it mints a client CA, enables the API
and expects `whoami` to return `operator` and the cluster view to show 3 of 3 replied.

## NetworkPolicy

`networkPolicy.enabled=true` renders an ingress `NetworkPolicy` for the broker pods:

| Port | Admitted from |
|---|---|
| peer bus 7001/TCP, gossip 7946/UDP, admin 9443/TCP | this release's broker pods only |
| client ports (`service.ports`) | `networkPolicy.clientFrom`, or anywhere when empty |
| health and metrics 8080 | `networkPolicy.healthFrom`, or anywhere when empty (kubelet probes, Prometheus) |
| admin 9443/TCP, from outside the pod mesh | `networkPolicy.adminFrom` (with `admin.enabled`) |

Each list holds standard `NetworkPolicyPeer` objects:

```yaml
networkPolicy:
  enabled: true
  clientFrom:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: iot
  healthFrom:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: monitoring
```

With no lists, the policy still closes the cluster ports to everything but the broker pods,
while clients, probes and scrapes keep working. `kubectl port-forward` to the admin port is
unaffected. Egress is not restricted. The policy needs a CNI that enforces NetworkPolicy
(Calico, Cilium, kindnet, …). Without one, Kubernetes accepts the object and it has no
effect. `scripts/k8s/kind-smoke.sh` runs the whole smoke with the policy on and checks that a
non-broker pod reaches health but is refused on 7001 and 9443.

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
