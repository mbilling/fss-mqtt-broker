---
adr: "0047"
title: "Kubernetes deployment (Helm chart, StatefulSet, safe scale-down)"
adr_status: Proposed
tasks:
  - id: 0047-T1
    title: StatefulSet + per-pod PVC — volumeClaimTemplate for the redb data dir; node id from the stable pod name; headless service backs gossip discovery so the mesh self-forms
    status: planned
  - id: 0047-T2
    title: ConfigMap-mounted config (ADR 0046) + Secret-mounted TLS/keys/gossip-key by path; --check-config as an init container so a bad config fails the rollout before serving
    status: planned
  - id: 0047-T3
    title: Probes + services wired — readinessProbe /readyz, livenessProbe /livez, /metrics scrape annotation/ServiceMonitor; client Service + headless peer/gossip Service
    status: planned
  - id: 0047-T4
    title: Safe scale-down — preStop hook sends SIGUSR1 (ADR 0043 decommission drain); terminationGracePeriodSeconds sized for drain + ADR 0019 graceful shutdown; hard-kill falls back to crash semantics survivors handle
    status: planned
  - id: 0047-T5
    title: Quorum-safe rollout — StatefulSet one-at-a-time RollingUpdate (ADR 0039 motion) + PodDisruptionBudget maxUnavailable 1; a kind/k3d smoke test in CI stands up a cluster, scales, and rolls, asserting no acked loss
    status: planned
---

# Delivery: ADR 0047 — Kubernetes deployment

[ADR 0047](../adr/0047-kubernetes-deployment.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0047 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0047-T1 | ⬜ planned | — |  |
| 0047-T2 | ⬜ planned | — |  |
| 0047-T3 | ⬜ planned | — |  |
| 0047-T4 | ⬜ planned | — |  |
| 0047-T5 | ⬜ planned | — |  |
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
