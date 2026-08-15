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
   PVC sizes (bounded by `expansion.maxSize`). **As delivered, this grows the volume
   only — it does not yet raise `store_max_bytes`** (that lives in the config and is
   tracked by 0055-T10), so the broker can keep refusing writes on the larger volume
   until the watermark is rolled. The config-roll half is planned, not shipped.
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

## Amendment (2026-08-06): the founder guard is a render-time property, not a remediation (T9)

§3.1 listed three fence actions, the third being "re-render its seeds for the ADR 0043
replace motion", and §3.5 promised a founder-PVC-loss guard: "before (re)creating pod-0,
the operator checks for a live cluster whose identity pod-0's volume no longer carries".
Neither was built. Both were nominally owned by T4, whose title named them and whose
evidence did not — so nothing open tracked them, and the fence shipped doing two of its
three jobs. The consequence was not cosmetic: a fenced pod-0 came straight back, re-founded,
and (until the ADR 0054 amendment) served clients from an empty store.

§3.5 is also **unimplementable as written**. The operator does not create pods — the
StatefulSet controller does — and there is no hook between "pod-0 is about to be created"
and "pod-0 starts". Any check at that moment races the kubelet. The guarantee has to be a
standing property of what is rendered, true before the pod exists.

**So the guard is render-time.** The per-pod init script gains `CLUSTER_ESTABLISHED`:
when set, ordinal 0 renders seeds pointing at its peers instead of the empty list that
makes it a founder, and a pod-0 whose volume is lost REJOINS and back-fills instead of
minting a second identity. The chart exposes it as `clusterEstablished` (default false —
a first install must be able to bootstrap); the operator sets it from `status.bootstrapped`,
latched the first time exactly one cluster identity is observed from a reachable pod and
monotonic thereafter, so absent evidence can never re-arm founding.

Two carve-outs, both load-bearing: `replicaCount < 2` still founds (a single-node cluster
has no survivor to split from), and `spec.bootstrapPolicy: AllowRebootstrap` is the
break-glass for total volume loss or a deliberate re-bootstrap — it clears the latch
rather than leaving it stale, and is loud while set.

`READY_MIN` follows the guard. An **unarmed** ordinal 0 keeps 1 — it is the founder and
must come Ready alone, or ordered bring-up never starts and a single-node deployment is
never healthy. An **armed** ordinal 0 is a joiner in every respect and takes the joiner's
majority floor.

*Corrected 2026-08-07.* This originally read "`READY_MIN` stays 1 for ordinal 0; the
safety comes from the seed list being non-empty — with seeds the node mints nothing and
bootstraps no lease group, so readiness gates on real admission." That is true **only when
durable sessions are enabled**: `lease_group_ready` is `None` without a durable plane and
cannot gate, and `member_count()` counts self, so an isolated pod-0 reported `members = 1`
against a floor of 1 and served clients an empty store. The floor is the only protection
there. The unstated precondition is the same defect shape as `whenScaled: Delete` ("safe
because the drain hands state to survivors" — only while a survivor remains) and ADR
0047's founder-rule claim; it was written into the justification while fixing that very
class of error.

**Scope: automatic seed lists are a Kubernetes affordance.** Deriving a joiner's seeds
from stable ordinals works because a StatefulSet guarantees them — ordinals are stable and
scale-down removes the highest first, so a seed target cannot be decommissioned out from
under a joiner. Outside an orchestrated environment there is no such guarantee, and
maintaining `MQTTD_SWIM_SEEDS` — including after decommissioning a node named in it — is
the operator's responsibility. The broker deliberately does not try to rewrite seed lists
at runtime.

`SplitBrainAction` gains no variant. The guard must apply under the default `Alert`
posture — an operator installed with defaults must still be protected — and a "rejoin"
remediation would imply the operator wiping a data dir, which the missing PVC-delete verb
in its Role exists to forbid.

§3.1's third action is therefore satisfied by construction rather than by the fence, and
the fence keeps doing what it does: quarantine the PVC by label, delete the pod, once.

## Amendment (2026-08-14): the operator is installable, and the CRD's stability posture is stated (T8, issue #252)

Until now the operator was the honest paradox a review panel called a credibility drag:
built, end-to-end tested nightly, and impossible to run — no published image, a manifest
pinned to a kind-local tag. Issue #252 accepted either publishing it or recording
"development artifact" as a scope decision. Publishing was always this record's own plan
(T8), so that is what landed:

- **The operator image joins the ADR 0045 release pipeline** — reproducible static musl
  binary (both arches), cosign-signed image + blobs, SLSA provenance, its own CycloneDX
  SBOM — `ghcr.io/mbilling/fss-mqtt-broker-operator:X.Y.Z`, cut by the same `vX.Y.Z` tag
  as the broker. One version train, deliberately: the operator renders the broker's
  objects, and the render-parity gate already couples the two at every commit.
- **An install chart, `deploy/helm/mqttd-operator`** — the CRD packaged in `crds/`
  (byte-identical to the generated schema, pinned by a golden test), the namespaced RBAC
  the nightly e2e proved sufficient (no cluster-admin, no Secret access, no PVC delete),
  a single-replica Deployment behind the Lease leader gate, and an annotated secure
  example `MqttdCluster`. `deploy/operator/operator.yaml` remains what it always was —
  the dev/e2e manifest — and now says so while pointing at the chart.
- **A forward pin, gate-proven** (the issue #263 discipline): no operator image predates
  the next release, so the chart's `appVersion` pins it — and
  `scripts/check-deploy-image-pin.sh` now holds the operator chart and the compose
  default to the SAME forward tag. Fixing that gate also surfaced and closed a latent
  #263 residue: the compose pin used the git-tag form (`:v0.9.1`) while the pipeline
  publishes registry-form tags (`:0.9.1` — verified against GHCR, where `0.9.0` exists
  and `v0.9.0` is a 404), so the default would have 404'd on release day and the
  nightly default-image lane's manifest-inspect skip would have skipped forever. Both
  artifacts now pin the registry form; the gate maps it to the git tag.

**CRD stability posture, stated** (the issue's second acceptance criterion):
`mqttd.io/v1alpha1`. Within a release the installed schema is exactly the tested one —
the golden test regenerates it from the operator's own types and CI fails on any drift,
chart copy included. Between releases, pre-1.0, `v1alpha1` means what it says: the
schema may change with the release train, called out in release notes, and Helm's
own CRD semantics (installed from `crds/`, never upgraded) make the upgrade step
explicit — documented in OPERATIONS.md and the chart NOTES. The chart-only path remains
fully supported and is the stability-conservative choice until the CRD graduates.

The acceptance closes fully at the next release tag: the repository side (pipeline,
chart, gates, docs) is complete, and pushing `v0.9.1` publishes the image that makes
the chart installable as-is — the same posture, and the same single remaining
maintainer action, as the compose pin (0047-T12).
