# ADR 0055 — The mqttd Kubernetes operator (`MqttdCluster` CRD, kube-rs controller)

- **Status:** Accepted
- **Date:** 2026-08-05 (accepted 2026-08-05 — the maintainer started implementation)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0055-kubernetes-operator.md](../delivery/0055-kubernetes-operator.md) — plan, progress, and changelog
- **Related:** [ADR 0047](0047-kubernetes-deployment.md) (the chart whose contracts this
  wraps; its amendment's reopen triggers are the mandate),
  [ADR 0054](0054-operator-facing-state-surface.md) (the acting-signals this consumes —
  cluster identity, brownout/drain gauges, `/statusz`),
  [ADR 0043](0043-elastic-cluster-resize.md) (the decommission drain this orchestrates),
  [ADR 0003](0003-gossip-authentication.md) (the key rotation this automates),
  [ADR 0039](0039-versioning-and-upgrade-policy.md) (versioning posture; the CRD gets
  its own alpha track)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0055-kubernetes-operator.md).

## Context

ADR 0047 deferred an operator; its 2026-08-04 amendment named the reopen triggers and
the maintainer engaged the path on 2026-08-05: split-brain and brownout handling should
be reconciled automatically. The prerequisite landed first (ADR 0054): cluster identity
with gossip-level containment, brownout/drain/replication-lag gauges, and `/statusz` —
so a controller now has tested state to act on instead of inference. What remains
operator-shaped, from the amendment's own analysis: acting on split-brain (fencing),
acting on brownout (storage remediation), unattended three-phase key rotation, drain
cost on rolls, and founder-PVC-loss protection — all multi-step sequences reacting to
observed state, which Helm structurally cannot run.

## Decision

**Build a first-party operator: a `MqttdCluster` custom resource (group
`mqttd.io`, version `v1alpha1`) reconciled by a kube-rs controller in a new workspace
crate `mqttd-operator`, wrapping the existing operational contracts — drain via
SIGUSR1/`preStop`, readiness via `/readyz`, observation via `/statusz` + metrics,
config via the same TOML — never inventing new control surfaces on the broker.**

### 1. Rust + kube-rs, one workspace

The controller is a workspace crate (`crates/mqttd-operator`, its own binary + image),
built on `kube`/`kube-runtime` with leader election. One language keeps the
supply-chain posture uniform (same toolchain pin, `cargo-deny`/`cargo-audit` gates,
`forbid(unsafe)` where applicable, one CI); Go/controller-runtime's scaffolding
convenience does not outweigh splitting the codebase's assurance story.

### 2. The CRD owns the resources (the chart remains the no-operator path)

`MqttdCluster` is the API; the operator renders and owns the StatefulSet, Services,
ConfigMap, and PDB — the same objects the chart renders, kept behaviorally identical
by a **render-parity test**: in CI, the operator's rendered manifests are diffed
against `helm template` for equivalent inputs, so the two deployment paths cannot
drift. The Helm chart stays fully supported for operators who do not want a
controller; the operator is additive, never required.

`spec` (initial shape): `replicas`, `image`, `version`, `config` (the same TOML
template contract as the chart, ADR 0046), `secrets` (same by-path references),
`persistence` (size/class/retention + `expansion.maxSize` for brownout remediation),
`limits` passthrough, and `remediation` — per-scenario switches, every destructive
action **opt-in** (`Alert` default, `Act` explicit):
`remediation.splitBrain: Alert|Fence`, `remediation.brownout: Alert|ExpandPVC`,
`gossipKeyRotation: { newKeySecretRef }` (presence starts the orchestration).

`status`: `phase`, `members`, `clusterId`, `readyReplicas`, `brownout`,
`decommission`, `conditions` (`SplitBrain`, `Converged`, `RotationInProgress`, …) —
aggregated from `/statusz` + metrics so `kubectl get mqttdclusters` answers the
first page of any incident.

### 3. The v1 reconciliations (exactly the amendment's list)

1. **Split-brain**: poll `/statusz` per pod; two `cluster_id` values (or
   `foundings_total` movement after bootstrap) sets the `SplitBrain` condition +
   Event. With `Fence`: delete the *new-founder* pod (identified by `founder` +
   `minted` + minority id), quarantine its PVC by label (never auto-delete data),
   and re-render its seeds for the ADR 0043 replace motion — the automated form of
   OPERATIONS.md's manual recovery. Gossip-level containment (ADR 0054 T2) means the
   fence is cleanup, not the safety mechanism. **The fence fires at most once per
   node** (T7 amendment): the fenced pod returns with the same volume, still holding
   the divergent state, so it re-founds and reads as split-brained again. Re-killing
   it fixes nothing — only the documented manual wipe does — and it *hides* the
   incident, because each delete removes the divergent pod from the observation and
   drops `SplitBrain` back to False within milliseconds. An already-quarantined PVC
   therefore suppresses further fencing and the condition stays raised.
2. **Brownout**: `mqttd_brownout == 1` or utilization past a threshold sets the
   condition + Event; with `ExpandPVC` and an expansion-capable StorageClass, patch
   PVC sizes (bounded by `expansion.maxSize`) and raise `store_max_bytes`
   accordingly through the config contract (a rolling config apply).
3. **Key rotation, unattended**: drive the three `key_accept` phases as config/Secret
   rolls, gating each phase on the ADR 0054 verification signals
   (`swim_keys_accepted` returns to 1, checksums converge) before the next.
4. **Drain-aware rolls**: the operator distinguishes a shrink (full ADR 0043 drain,
   then delete) from a roll (skip the drain; rejoin-and-catch-up per ADR 0039).
   Mechanism: an operator-set pod annotation surfaced via the Downward API; the
   `preStop` consults it (a small chart/entrypoint hook, kept identical in both
   deployment paths). Chart-only deployments keep today's always-drain default.
5. **Founder-PVC-loss guard**: before (re)creating pod-0, the operator checks for a
   live cluster whose identity pod-0's volume no longer carries; if so it applies
   the seeds-override recovery instead of letting a seedless founder re-bootstrap.

### 4. Versioning and trust posture

`v1alpha1` says what it means: the CRD schema may break until the broker's 1.0
(the pre-1.0 CRD-stickiness concern recorded in ADR 0047's amendment is handled by
alpha semantics, not by waiting). The operator needs only namespaced RBAC over its
own resources plus pod/PVC patch — no cluster-admin, no secret *values* (it references
the same Secrets the chart does; the rotation orchestration re-points references and
rolls, it never reads key material). The operator image ships through the ADR 0045
pipeline: signed, reproducible, SBOM-attested.

### 5. Testing (the ADR 0044 bar applies)

Reconcile logic unit-tested against a mocked API server; the **render-parity** gate
per PR; a nightly **kind e2e**: install operator → apply `MqttdCluster` → cluster
forms → scale up/down (drain asserted) → roll (no drain) → induced split-brain
(founder PVC wipe) detected, and fenced when `Fence` is set — the ADR 0054 signals
asserted end to end. *T7 note:* the scale cycle was briefly withheld — it reproduced
a broker-side membership flap after any roll (issue #92) that left a pod NotReady
forever — and is asserted again now that ADR 0016's generation amendment fixed it.
The drain assertion on the scale remains T6's. The acked-facts oracle remains the
broker suites' job; the operator e2e asserts orchestration, not message durability.

## Alternatives considered

- **Go + controller-runtime/kubebuilder.** The ecosystem default, better scaffolding
  and docs. Rejected: splits the toolchain, supply-chain gates, and assurance story
  for convenience the small controller surface does not need.
- **Operator embeds the Helm chart (helm-operator style).** Less duplication, but the
  reconciler then acts through a templating indirection it cannot reason about;
  render-parity testing gives the same no-drift guarantee with native objects.
- **Acting through a broker admin API.** Rejected long ago and stays rejected
  (ADR 0047: signal-driven ops, read-only health surface); every operator action is
  a Kubernetes-object action or an existing contract (signals, config, Secrets).
- **Waiting for 1.0 to avoid CRD churn.** The alpha group/version exists precisely
  for this; deferring forfeits operator experience during the period the project can
  still change cheaply.

## Consequences

- A second deliverable (controller image + CRD + operator chart) enters the release
  pipeline and the comparison story (EMQX's "Operator + Helm" cell gains an mqttd
  counterpart when this ships).
- The chart and operator must not drift: the render-parity test is CI-gated, and the
  drain-hook change ships in both paths.
- Destructive remediations are opt-in per scenario, alert-only by default — the
  operator's defaults are exactly as conservative as having no operator.
- New failure surface: the controller itself. Leader election, idempotent
  reconciliation, and the e2e suite are the mitigations; the broker never depends on
  the operator being alive (it only makes day-2 unattended).
