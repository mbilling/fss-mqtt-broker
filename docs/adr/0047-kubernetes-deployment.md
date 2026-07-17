# ADR 0047 — Kubernetes deployment (Helm chart, StatefulSet, safe scale-down)

- **Status:** Proposed
- **Date:** 2026-07-17
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0047-kubernetes-deployment.md](../delivery/0047-kubernetes-deployment.md) — plan, progress, and changelog
- **Related:** [ADR 0045](0045-release-engineering-and-distribution.md) (the hardened image
  this deploys), [ADR 0046](0046-file-based-configuration.md) (the config file mounted from
  a ConfigMap), [ADR 0043](0043-elastic-cluster-resize.md) (the decommission drain a
  scale-down must trigger — pulling a pod is a planned removal, not a crash), [ADR 0019](0019-graceful-shutdown.md)
  (the graceful shutdown a pod termination must honor), [ADR 0020](0020-metrics-and-observability.md)
  (the `/livez` + `/readyz` probes and `/metrics` a k8s deployment wires), [ADR 0018](0018-on-disk-persistence.md)
  (the redb data dir that needs a PersistentVolume per pod), [ADR 0039](0039-versioning-and-upgrade-policy.md)
  (the one-at-a-time rolling upgrade a k8s rollout must respect)

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0047-kubernetes-deployment.md).

## Context

The broker is cluster-native and the image (ADR 0045) will exist, but there is no supported
way to **run a cluster on Kubernetes** — the platform most operators will reach for. Naively
deploying it wrong loses the very guarantees the broker works hard to provide:

- A **`Deployment` with ephemeral storage** throws away the durable session store (ADR 0018)
  on every pod reschedule — turning a data-safe broker into a lossy one by misconfiguration.
- **Scaling down a `Deployment`/`StatefulSet`** by deleting a pod is, to the cluster, a node
  *crash* — survivors recover (they must), but it skips the ADR 0043 **decommission drain**
  that makes a *planned* removal lose nothing and demote cleanly. A shrink should drain, not
  crash.
- A **rollout that replaces pods too fast** violates ADR 0039's one-node-at-a-time upgrade
  motion, and a rollout with no `PodDisruptionBudget` lets a node drain take out quorum.
- The health probes (ADR 0020) and the config file (ADR 0046) exist but nothing wires them
  into the k8s primitives (readiness gating, ConfigMap mount, Secret mount) out of the box.

The result today is that "runs on Kubernetes" is true only for an expert who assembles all
of this by hand — and gets it subtly wrong in ways that surface as data loss under
scale/upgrade.

## Decision

A **supported Kubernetes deployment** ships — a Helm chart (and plain manifests) that
encode the broker's operational contract so the safe path is the default. Five parts:

### 1. StatefulSet with per-pod persistent storage

The broker runs as a **StatefulSet** with a `volumeClaimTemplate`, so each pod gets a stable
identity and its **own PersistentVolume** for the redb data dir (ADR 0018). A rescheduled
pod reattaches its volume and recovers its durable state — never the ephemeral-storage
data-loss trap. `MQTTD_NODE_ID` is derived from the stable pod name; gossip seeds point at
the headless service, so the mesh forms itself (ADR 0016).

### 2. Config via ConfigMap, secrets via Secret

The config file (ADR 0046) is a **ConfigMap** mounted at a path, so a `helm upgrade` /
GitOps commit is the unit of change; TLS material, password/JWT keys, and the gossip key are
**Secret** mounts referenced by path. `--check-config` runs as an init container or CI gate,
so a bad config fails the rollout before a pod serves.

### 3. Probes and services wired to the broker's real signals

`readinessProbe` → `/readyz` (which already reports membership + lease-group readiness +
decommission progress, ADR 0020), `livenessProbe` → `/livez`, and a `ServiceMonitor`/scrape
annotation for `/metrics`. A `Service` fronts the client listeners; a **headless** Service
backs gossip discovery and the peer mesh.

### 4. Scale-down is a decommission, not a crash

Removing a replica triggers the ADR 0043 **decommission drain**: a `preStop` hook sends
`SIGUSR1` (drain — hand every held key to the post-departure replica set, verify, then leave
gracefully) and the pod's `terminationGracePeriodSeconds` is set long enough for the drain
plus the ADR 0019 graceful shutdown to complete. A scale-down therefore loses nothing and
demotes voters cleanly, exactly as `SIGUSR1` does outside k8s; a hard kill (grace exceeded)
falls back to crash semantics the survivors already handle.

### 5. Upgrades and disruption respect quorum

The StatefulSet's **`RollingUpdate` with `partition`/one-at-a-time** ordering enacts
ADR 0039's one-node-at-a-time motion — each pod rolls, rejoins, and reaches the caught-up
watermark before the next (the ADR 0044 P3 rolling-upgrade test proves the broker survives
exactly this). A **`PodDisruptionBudget`** (`maxUnavailable: 1`) stops a node drain or
voluntary disruption from taking two nodes — and thus quorum — at once.

## Consequences

- "Runs on Kubernetes" becomes true by default, with the durability, safe-shrink, and
  safe-upgrade guarantees intact rather than lost to misconfiguration.
- The chart is executable operator documentation: the ADR 0043/0039/0019 contracts become
  `preStop` hooks, grace periods, update strategy, and a PDB — checked by a kind/k3d smoke
  test in CI (an out-of-cluster analog of the ADR 0044 quickstart-as-test).
- The broker gains a Kubernetes dependency *surface* (chart maintenance, k8s version skew)
  but no code coupling — the chart drives the same binary and signals as any other operator;
  bare-metal/systemd/Docker-Compose deployments stay first-class.
- A StatefulSet's stable-identity model fits HRW placement (ADR 0001) well: a pod keeps its
  id and volume across reschedule, so ownership and durable state move together.

## Alternatives considered

- **Deployment + ephemeral storage:** the common default, and wrong here — it discards the
  durable store on reschedule. A StatefulSet with a PVC is the only correct shape for a
  stateful, durable broker. Rejected.
- **N single-replica Deployments, each with its own dedicated PVC (the "manual
  StatefulSet"):** a legitimate pattern, and it gets the thing that matters most right — each
  node keeps a stable, dedicated PersistentVolume that survives reschedule, so on the
  *durability* axis it is a wash with a StatefulSet. It loses on the *lifecycle mechanics*,
  and in three concrete ways. (a) The redb data dir is a `ReadWriteOnce` volume, and a
  Deployment's default `RollingUpdate` surges the new pod up before the old releases the
  volume — a Multi-Attach deadlock that forces `strategy: Recreate` (our replication makes the
  resulting per-node gap survivable, but it is a trap you must know to avoid). (b) Nothing
  coordinates independent Deployments, so a single `apply`/GitOps sync rolls all of them at
  once and takes out quorum — the one-at-a-time ordering ADR 0039 and part 5 depend on must be
  re-imposed by hand (sync-waves, `dependsOn`, a per-Deployment PDB). (c) Scaling becomes
  hand-authored boilerplate — a new node is a whole new Deployment *and* PVC manifest rather
  than one replica count against a `volumeClaimTemplate`. A StatefulSet packages exactly our
  topology — *N pods, each with a stable identity and its own volume, updated one at a time* —
  so the manual approach rebuilds ordered rollout and template provisioning worse. Our
  replication makes each failure mode gentler (a quorum survives a clumsy rollout; a lost
  volume triggers catch-up, not data loss), so this is not *wrong* — just more moving parts for
  guarantees the StatefulSet gives in one object. Rejected.
- **A custom operator (CRD + controller) instead of a Helm chart:** more power (automated
  decommission on scale, orchestrated upgrades), but a large surface to build and maintain
  before there are users. A Helm chart that encodes the contracts covers the need now; an
  operator is a plausible post-1.0 follow-on if demand appears. Deferred, not rejected.
- **Letting scale-down be a plain crash and relying on survivor recovery:** correctness
  holds (survivors do recover), but it needlessly forfeits the ADR 0043 clean-drain
  guarantee and can transiently degrade under load. Wiring `preStop → SIGUSR1` makes the
  intended, lossless path the default. Rejected.
- **No `PodDisruptionBudget`:** simpler, but a routine node drain could evict two brokers at
  once and lose quorum — precisely the failure the broker's durability model assumes cannot
  happen silently. A PDB is not optional for a quorum system. Rejected.
