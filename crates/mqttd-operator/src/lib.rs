//! The mqttd Kubernetes operator ([ADR 0055](../../docs/adr/0055-kubernetes-operator.md)).
//!
//! A kube-rs controller reconciling [`crd::MqttdCluster`] resources by wrapping
//! the broker's existing operational contracts — the SIGUSR1 decommission drain,
//! `/readyz` readiness, `/statusz` + metrics observation (ADR 0054), the TOML
//! config, and by-path Secret references. It never invents a new control
//! surface on the broker, never reads secret material, and every destructive
//! remediation is opt-in with Alert-only defaults.
//!
//! This crate is the 0055-T1 scaffold: the CRD types (schema golden-tested
//! against the committed manifest), the reconcile skeleton (observe + status
//! stamp), and a Lease-based leader gate. Rendering parity, observed-state
//! aggregation, and the remediations land as 0055-T2..T6.

pub mod controller;
pub mod crd;
pub mod leader;
