---
adr: "0047"
title: "Kubernetes deployment (Helm chart, StatefulSet, safe scale-down)"
adr_status: Proposed
tasks:
  - id: 0047-T1
    title: StatefulSet + per-pod PVC — volumeClaimTemplate for the redb data dir; node id from the stable pod name; headless service backs gossip discovery so the mesh self-forms
    status: done
    date: 2026-07-19
    evidence: "Helm chart deploy/helm/mqttd: a StatefulSet with a `data` volumeClaimTemplate (ReadWriteOnce, per-pod PV for /var/lib/mqttd, so a rescheduled pod reattaches its volume and recovers durable state — ADR 0018), serviceName = the headless service, OrderedReady + RollingUpdate. Per-pod identity is solved for a distroless image (no shell) by a `render-config` init container: it reads $POD_NAME (Downward API) and writes /config/mqttd.toml with node.id = the pod name and cluster.swim.seeds = [] for pod-0 (the gossip founder) or [<sts>-0.<headless>:7946] for pods 1..N — so exactly one founder bootstraps the lease group and the rest self-form the mesh over the headless service (ADR 0016), mirroring the demo's founder/seed pattern. publishNotReadyAddresses on the headless service lets gossip reach a still-joining peer. Verified locally: the render logic yields empty seeds for -0 and the pod-0 seed for -3, and both rendered configs pass `mqttd --check-config`. Structurally validated in CI (helm lint + template + kubeconform, offline)."
  - id: 0047-T2
    title: ConfigMap-mounted config (ADR 0046) + Secret-mounted TLS/keys/gossip-key by path; --check-config as an init container so a bad config fails the rollout before serving
    status: done
    date: 2026-07-19
    evidence: "The config (ADR 0046) is a ConfigMap-mounted TOML template (values.config) rendered per-pod by the init container; secrets are referenced BY PATH and mounted read-only from operator-managed Secrets/ConfigMaps (values.secrets.{tls,acl,peerTls,gossipKey}) — none inlined (ADR 0046 T5). A `check-config` init container runs `mqttd --check-config --config /config/mqttd.toml` on the rendered file, so an invalid config fails the pod BEFORE it serves (ADR 0046 T3). A checksum/config pod annotation rolls pods when the template changes. Verified: the rendered founder + non-founder configs both `--check-config`-validate."
  - id: 0047-T3
    title: Probes + services wired — readinessProbe /readyz, livenessProbe /livez, /metrics scrape annotation/ServiceMonitor; client Service + headless peer/gossip Service
    status: done
    date: 2026-07-19
    evidence: "startup/liveness on /livez (generous startupProbe so a catching-up joiner is not liveness-killed), readiness on /readyz (mesh + lease-group readiness + decommission progress, ADR 0020). Two Services: a client Service (TLS 8883 + a health/metrics 8080 port) and a headless Service (peer 7001/TCP + gossip 7946/UDP, publishNotReadyAddresses). Metrics via prometheus.io/scrape pod annotations (default) or an optional ServiceMonitor. Structurally validated in CI."
  - id: 0047-T4
    title: Safe scale-down — preStop hook sends SIGUSR1 (ADR 0043 decommission drain); terminationGracePeriodSeconds sized for drain + ADR 0019 graceful shutdown; hard-kill falls back to crash semantics survivors handle
    status: done
    date: 2026-07-19
    evidence: "The distroless image has no shell/`kill`, so the preStop needs a broker-provided way to signal itself — added `mqttd --decommission [--pid <n>] [--timeout <secs>]` (rustix's safe kill wrappers; the crate forbids unsafe): it sends SIGUSR1 to the running broker (default PID 1, the container entrypoint) to begin the ADR 0043 decommission drain, then BLOCKS until that process exits (Linux: reads /proc/<pid>/stat, treating a zombie/dead/missing state as exited — a bare kill(pid,0) would call an unreaped zombie 'alive'), so k8s holds the pod open for the whole drain. Exit 0 = drained, 1 = timeout (yields to grace/SIGTERM), 2 = usage/signal error. The chart's preStop = `mqttd --decommission --timeout <terminationGracePeriodSeconds>`; the grace (default 300s) covers the drain + ADR 0019 graceful shutdown. tests/decommission.rs (3): nonexistent pid → exit 2, pid 0 → usage error, and a target that exits on SIGUSR1 is signalled + waited-for → exit 0 + 'drain complete'. mqttd lib 147 + bin 6 + check_config 5 + decommission 3 green; clippy -D warnings + fmt clean."
  - id: 0047-T5
    title: Quorum-safe rollout — StatefulSet one-at-a-time RollingUpdate (ADR 0039 motion) + PodDisruptionBudget maxUnavailable 1; a kind/k3d smoke test in CI stands up a cluster, scales, and rolls, asserting no acked loss
    status: in-progress
    notes: "Rollout config authored in the chart: StatefulSet OrderedReady + RollingUpdate (one pod at a time, each rejoining before the next — ADR 0039), and a PodDisruptionBudget maxUnavailable 1 (a node drain can't take two brokers / quorum). A CI `helm` job lints + templates + kubeconform-validates the whole chart offline. Remaining: the live kind/k3d runtime smoke that stands up a cluster, scales down (asserting the decommission drain runs), and rolls (asserting no acked fact is lost) — it needs the image built + a kind cluster, so it lands in the nightly tier."
---

# Delivery: ADR 0047 — Kubernetes deployment

[ADR 0047](../adr/0047-kubernetes-deployment.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0047 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0047-T1 | ✅ done | 2026-07-19 | "Helm chart deploy/helm/mqttd: a StatefulSet with a `data` volumeClaimTemplate (ReadWriteOnce, per-pod PV for /var/lib/mqttd, so a rescheduled pod reattaches its volume and recovers durable state — ADR 0018), serviceName = the headless service, OrderedReady + RollingUpdate. Per-pod identity is solved for a distroless image (no shell) by a `render-config` init container: it reads $POD_NAME (Downward API) and writes /config/mqttd.toml with node.id = the pod name and cluster.swim.seeds = [] for pod-0 (the gossip founder) or [<sts>-0.<headless>:7946] for pods 1..N — so exactly one founder bootstraps the lease group and the rest self-form the mesh over the headless service (ADR 0016), mirroring the demo's founder/seed pattern. publishNotReadyAddresses on the headless service lets gossip reach a still-joining peer. Verified locally: the render logic yields empty seeds for -0 and the pod-0 seed for -3, and both rendered configs pass `mqttd --check-config`. Structurally validated in CI (helm lint + template + kubeconform, offline)." |
| 0047-T2 | ✅ done | 2026-07-19 | "The config (ADR 0046) is a ConfigMap-mounted TOML template (values.config) rendered per-pod by the init container; secrets are referenced BY PATH and mounted read-only from operator-managed Secrets/ConfigMaps (values.secrets.{tls,acl,peerTls,gossipKey}) — none inlined (ADR 0046 T5). A `check-config` init container runs `mqttd --check-config --config /config/mqttd.toml` on the rendered file, so an invalid config fails the pod BEFORE it serves (ADR 0046 T3). A checksum/config pod annotation rolls pods when the template changes. Verified: the rendered founder + non-founder configs both `--check-config`-validate." |
| 0047-T3 | ✅ done | 2026-07-19 | "startup/liveness on /livez (generous startupProbe so a catching-up joiner is not liveness-killed), readiness on /readyz (mesh + lease-group readiness + decommission progress, ADR 0020). Two Services: a client Service (TLS 8883 + a health/metrics 8080 port) and a headless Service (peer 7001/TCP + gossip 7946/UDP, publishNotReadyAddresses). Metrics via prometheus.io/scrape pod annotations (default) or an optional ServiceMonitor. Structurally validated in CI." |
| 0047-T4 | ✅ done | 2026-07-19 | "The distroless image has no shell/`kill`, so the preStop needs a broker-provided way to signal itself — added `mqttd --decommission [--pid <n>] [--timeout <secs>]` (rustix's safe kill wrappers; the crate forbids unsafe): it sends SIGUSR1 to the running broker (default PID 1, the container entrypoint) to begin the ADR 0043 decommission drain, then BLOCKS until that process exits (Linux: reads /proc/<pid>/stat, treating a zombie/dead/missing state as exited — a bare kill(pid,0) would call an unreaped zombie 'alive'), so k8s holds the pod open for the whole drain. Exit 0 = drained, 1 = timeout (yields to grace/SIGTERM), 2 = usage/signal error. The chart's preStop = `mqttd --decommission --timeout <terminationGracePeriodSeconds>`; the grace (default 300s) covers the drain + ADR 0019 graceful shutdown. tests/decommission.rs (3): nonexistent pid → exit 2, pid 0 → usage error, and a target that exits on SIGUSR1 is signalled + waited-for → exit 0 + 'drain complete'. mqttd lib 147 + bin 6 + check_config 5 + decommission 3 green; clippy -D warnings + fmt clean." |
| 0047-T5 | 🚧 in-progress | — | "Rollout config authored in the chart: StatefulSet OrderedReady + RollingUpdate (one pod at a time, each rejoining before the next — ADR 0039), and a PodDisruptionBudget maxUnavailable 1 (a node drain can't take two brokers / quorum). A CI `helm` job lints + templates + kubeconform-validates the whole chart offline. Remaining: the live kind/k3d runtime smoke that stands up a cluster, scales down (asserting the decommission drain runs), and rolls (asserting no acked fact is lost) — it needs the image built + a kind cluster, so it lands in the nightly tier." |
<!-- /status-table:0047 -->

## Plan

| Task | Done means |
|---|---|
| **0047-T1** StatefulSet + PVC | Each pod has a stable id and its own PersistentVolume; a rescheduled pod recovers its durable state; the mesh forms via the headless service. |
| **0047-T2** Config + secrets | The config file mounts from a ConfigMap and secrets from a Secret (by path); `--check-config` gates the rollout. |
| **0047-T3** Probes + services | `/readyz`/`/livez` drive readiness/liveness, `/metrics` is scraped; client and headless peer services front the broker. |
| **0047-T4** Safe scale-down | Removing a replica drains via `preStop → SIGUSR1` within the grace period — a lossless decommission, not a crash. |
| **0047-T5** Quorum-safe rollout | One-at-a-time updates + a PDB (`maxUnavailable: 1`); a kind/k3d CI smoke stands up, scales, and rolls a cluster with zero acked loss. |

Order: T1 → T2 → T3 → T4 → T5. Depends on the ADR 0045 image and ADR 0046 config file.

## Changelog

- **2026-07-17** — ADR 0047 drafted. Adoption enabler: "runs on Kubernetes" must preserve
  the durability/safe-shrink/safe-upgrade guarantees (ADR 0018/0043/0039) rather than lose
  them to misconfiguration (ephemeral storage, crash-on-scale, quorum-breaking rollouts).
  Priority **P1**.
- **2026-07-17** — Alternatives sharpened: added "N single-replica Deployments, each with its
  own PVC" (the *manual StatefulSet*) as a named, fairly-argued rejected option — it gets
  per-node durable storage right but rebuilds ordered rollout and template PVC provisioning by
  hand and walks into the `ReadWriteOnce` Multi-Attach rollout trap. Decision unchanged: keep
  the StatefulSet.
- **2026-07-19** — **T1–T4 done; T5 in-progress.** The Helm chart (`deploy/helm/mqttd`) ships: a
  StatefulSet with a per-pod `volumeClaimTemplate` (durable state survives reschedule), a
  `render-config` init container that solves per-pod identity for the *distroless* image (no shell)
  by writing node id = pod name and seeds = `[]` for the founder pod-0 / the pod-0 seed for the
  rest, a `check-config` init container that fails a bad config before it serves, `/readyz`+`/livez`
  probes, client + headless Services, a PDB (`maxUnavailable: 1`), and one-at-a-time RollingUpdate.
  The safe scale-down needed a broker primitive the ADR under-specified: `mqttd --decommission`
  (sends SIGUSR1 to the running broker — PID 1 — and blocks until it drains + exits), wired into the
  chart's `preStop`; unit+integration tested. The chart is validated offline in a new CI `helm` job
  (lint + template + kubeconform) and the rendered configs pass `--check-config`. Remaining (T5): the
  live kind/k3d runtime smoke (stand up → scale → roll, asserting no acked loss), which needs the
  image + a cluster and lands in the nightly tier — so the ADR stays Proposed until it is green.
