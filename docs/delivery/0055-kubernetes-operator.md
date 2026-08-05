---
adr: "0055"
title: "The mqttd Kubernetes operator (MqttdCluster CRD, kube-rs controller)"
adr_status: Accepted
tasks:
  - id: 0055-T1
    title: Scaffold — crates/mqttd-operator (kube-rs, leader election, reconcile loop skeleton), MqttdCluster v1alpha1 spec/status schema (CRD YAML generated from Rust types), CI wiring (build/test/clippy/deny + CRD schema validated in the helm job)
    status: done
    date: 2026-08-05
    evidence: "crates/mqttd-operator: kube 4 / k8s-openapi 0.28 (jiff time), rustls-tls (single stack, ring ban still holds — cargo tree -i ring empty, deny all four gates ok with the kube tree); MqttdCluster v1alpha1 spec/status with alert-only remediation defaults (unit-tested), printcolumns, shortname mqc; CRD manifest generated as JSON (deploy/crds/, kubectl-appliable, avoids the unmaintained serde_yaml) and golden-tested against the Rust types (regenerate: cargo run -p mqttd-operator --bin gen_crd); reconcile skeleton stamps observedGeneration + Reconciled condition via patch_status; Lease-based leader gate (coordination.k8s.io, no third-party election crate). CI = workspace membership (fmt/clippy/deny/test) + the golden test; kubeconform lacks a CRD meta-schema, so live validation is the T7 kind e2e's kubectl apply. NOTE deviation from the T1 title: CRD committed as JSON not YAML, and the helm-job kubeconform idea replaced by the golden test — both recorded here"

  - id: 0055-T2
    title: Resource rendering with render-parity — operator renders StatefulSet/Services/ConfigMap/PDB from the CR; CI diffs against helm template for equivalent inputs so chart and operator cannot drift
    status: planned
  - id: 0055-T3
    title: Observed state — /statusz + metrics polling into CR status (phase, members, clusterId, brownout, decommission) and conditions (SplitBrain, Converged, RotationInProgress) + Events; alert-only detection for split-brain and brownout
    status: planned
  - id: 0055-T4
    title: Opt-in remediations — splitBrain Fence (delete new-founder pod, quarantine PVC by label, seeds-override recovery), brownout ExpandPVC (bounded by expansion.maxSize, watermark raised via config roll), founder-PVC-loss guard before pod-0 recreation
    status: planned
  - id: 0055-T5
    title: Unattended gossip key rotation — three key_accept phases as Secret/config rolls, each gated on swim_keys_accepted returning to 1 and config checksum convergence
    status: planned
  - id: 0055-T6
    title: Drain-aware rolls — operator-set annotation via Downward API consulted by preStop (hook shipped identically in chart and operator paths); shrink = full drain, roll = rejoin-and-catch-up
    status: planned
  - id: 0055-T7
    title: Nightly kind e2e — install operator, apply CR, cluster forms, scale up/down with drain assertion, roll without drain, induced split-brain (founder PVC wipe) detected and fenced under Fence
    status: planned
  - id: 0055-T8
    title: Packaging + docs — deploy/helm/mqttd-operator chart (Deployment + CRD + namespaced RBAC), operator image in the ADR 0045 release pipeline (signed/reproducible/SBOM), OPERATIONS.md operator mode, COMPARISON.md Kubernetes cell update
    status: planned
---

# 0055 — Kubernetes operator: delivery

**Decision:** [ADR 0055](../adr/0055-kubernetes-operator.md). One-line story: the
ADR 0054 signals get their reconciler — a kube-rs `MqttdCluster` controller that
wraps the existing contracts (drain, `/readyz`, `/statusz`, TOML config), with every
destructive remediation opt-in and alert-only by default; the Helm chart remains the
fully-supported no-operator path, pinned to the operator by a render-parity test.

<!-- status-table:0055 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0055-T1 | ✅ done | 2026-08-05 | "crates/mqttd-operator: kube 4 / k8s-openapi 0.28 (jiff time), rustls-tls (single stack, ring ban still holds — cargo tree -i ring empty, deny all four gates ok with the kube tree); MqttdCluster v1alpha1 spec/status with alert-only remediation defaults (unit-tested), printcolumns, shortname mqc; CRD manifest generated as JSON (deploy/crds/, kubectl-appliable, avoids the unmaintained serde_yaml) and golden-tested against the Rust types (regenerate: cargo run -p mqttd-operator --bin gen_crd); reconcile skeleton stamps observedGeneration + Reconciled condition via patch_status; Lease-based leader gate (coordination.k8s.io, no third-party election crate). CI = workspace membership (fmt/clippy/deny/test) + the golden test; kubeconform lacks a CRD meta-schema, so live validation is the T7 kind e2e's kubectl apply. NOTE deviation from the T1 title: CRD committed as JSON not YAML, and the helm-job kubeconform idea replaced by the golden test — both recorded here" |
| 0055-T2 | ⬜ planned | — |  |
| 0055-T3 | ⬜ planned | — |  |
| 0055-T4 | ⬜ planned | — |  |
| 0055-T5 | ⬜ planned | — |  |
| 0055-T6 | ⬜ planned | — |  |
| 0055-T7 | ⬜ planned | — |  |
| 0055-T8 | ⬜ planned | — |  |
<!-- /status-table:0055 -->

## Notes

- 2026-08-05 — Planned as the follow-on cycle to ADR 0054 (signals-first sequencing
  per the ADR 0047 amendment). Suggested implementation order is T1→T8 as numbered;
  T1–T3 make the operator *useful* (status + alerting), T4–T6 make it *act*, T7–T8
  make it *shippable*. Each task is one reviewable PR against the standing
  fmt/clippy/deny/test gates.
