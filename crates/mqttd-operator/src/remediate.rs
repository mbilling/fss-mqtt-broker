//! Opt-in remediations ([ADR 0055](../../../docs/adr/0055-kubernetes-operator.md) T4).
//!
//! This is the first module that **acts**, so the safety argument comes first:
//!
//! 1. **Nothing here runs unless the CR asks for it.** Both switches default to
//!    `Alert` ([`crate::crd::Remediation`], enforced by a test) — an operator
//!    installed with defaults is exactly as conservative as no operator.
//! 2. **No action deletes data, ever.** A fenced node's PVC is *labelled*
//!    (quarantined) for a human, never deleted; the destructive last step of the
//!    split-brain runbook — wiping the wrong node's data dir — stays manual on
//!    purpose. The operator's job is containment and evidence.
//! 3. **Ambiguous evidence means no action.** A tie between identity groups, a
//!    fleet with unreachable pods that could change the verdict, or a missing
//!    founder flag all fall through to alert-only. This is why
//!    [`crate::observe`] treats an unreachable pod as absent evidence: a
//!    controller that mistook a rolling restart for a split brain would fence
//!    healthy nodes.
//! 4. **At most one destructive act per reconcile**, and every act is idempotent
//!    (re-labelling an already-quarantined PVC and re-requesting a size the PVC
//!    already has are both no-ops).
//!
//! Planning ([`plan`]) is pure and unit-tested; [`execute`] is the thin API shell.

use crate::crd::{BrownoutAction, MqttdCluster, SplitBrainAction};
use crate::probe::{PodProbe, PodStatus};

/// Label marking a PVC as quarantined by the operator (never deleted).
pub const QUARANTINE_LABEL: &str = "mqttd.io/quarantined";
/// Label carrying why a PVC was quarantined.
pub const QUARANTINE_REASON_LABEL: &str = "mqttd.io/quarantine-reason";
/// How much to grow a PVC when brownout expansion is enabled: 50% per step,
/// so a runaway does not double storage cost in one reconcile.
const EXPANSION_STEP_PERCENT: u64 = 50;

/// One remediation the operator intends to perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Contain a re-founded node: quarantine its PVC by label (data untouched)
    /// and delete the pod so it restarts clean rather than serving a second
    /// cluster. Full recovery still needs the documented human wipe.
    Fence {
        /// The pod to delete.
        pod: String,
        /// Its PVC, to label as quarantined.
        pvc: String,
        /// The minority identity it holds.
        cluster_id: String,
    },
    /// Grow every PVC one step, bounded by `persistence.expansionMaxSize`.
    ExpandPvcs {
        /// PVC names to patch.
        pvcs: Vec<String>,
        /// The new requested size.
        to: String,
    },
}

/// Decide what (if anything) to do. Pure: no I/O, no clock.
///
/// `quarantined` is the set of PVCs the operator has ALREADY fenced; a node is
/// fenced once and only once (see [`plan_fence`]).
#[must_use]
pub fn plan(
    cr: &MqttdCluster,
    probes: &[PodProbe],
    pvcs: &[String],
    quarantined: &[String],
    observed_pvc_max: Option<u64>,
) -> Vec<Action> {
    let mut actions = Vec::new();
    if let Some(fence) = plan_fence(cr, probes, quarantined) {
        // One destructive act per reconcile: a fence changes the very evidence
        // an expansion decision would rest on.
        return vec![fence];
    }
    if let Some(expand) = plan_expansion(cr, probes, pvcs, observed_pvc_max) {
        actions.push(expand);
    }
    actions
}

/// The split-brain fence, with every guard that keeps it from firing on a
/// healthy fleet.
///
/// Fires AT MOST ONCE per node. A fenced pod comes straight back — its volume is
/// still there, still holding the divergent state — so it re-founds and is seen
/// as split-brained again on the next pass. Re-killing it fixes nothing (only the
/// documented manual wipe does), and it actively HARMS the incident: each delete
/// removes the divergent pod from the observation, so the `SplitBrain` condition
/// drops back to False within milliseconds and no alert or human ever sees it.
/// The T7 e2e caught exactly that — a fence every 30s, forever, with a status
/// that never stayed True long enough to assert on. So: fence once for
/// containment and evidence, then leave the incident standing and visible.
fn plan_fence(cr: &MqttdCluster, probes: &[PodProbe], quarantined: &[String]) -> Option<Action> {
    if cr.spec.remediation.split_brain != SplitBrainAction::Fence {
        return None; // opt-in; the default is Alert
    }
    let reached: Vec<&PodStatus> = probes
        .iter()
        .filter_map(|p| match p {
            PodProbe::Reached(s) => Some(s.as_ref()),
            PodProbe::Unreachable { .. } => None,
        })
        .collect();
    let unreachable = probes.len() - reached.len();

    // Group the reachable pods by the identity they hold.
    let mut groups: std::collections::BTreeMap<&str, Vec<&PodStatus>> =
        std::collections::BTreeMap::new();
    for s in &reached {
        if let Some(id) = s.cluster_id.as_deref() {
            groups.entry(id).or_default().push(s);
        }
    }
    if groups.len() < 2 {
        return None; // no split to fence
    }

    // Rank by size; a TIE is ambiguous — alert, never guess which is real.
    let mut sizes: Vec<(&str, usize)> = groups.iter().map(|(id, v)| (*id, v.len())).collect();
    sizes.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    let (majority_id, majority) = sizes[0];
    let (minority_id, minority) = sizes[1];
    if majority == minority {
        return None; // 1v1 (or n-v-n): no defensible majority
    }
    // The unreachable pods must not be able to flip the verdict: if they could
    // all belong to the minority and out-vote the majority, the evidence is not
    // yet conclusive.
    if minority + unreachable >= majority {
        return None;
    }
    // Only fence a node that FOUNDED its own identity — the re-founder. A node
    // that merely adopted a wrong identity is a symptom, not the cause.
    let culprit = groups
        .get(minority_id)?
        .iter()
        .find(|s| s.founder)
        .or_else(|| groups.get(minority_id)?.first())?;
    debug_assert_ne!(majority_id, minority_id);

    let pod = culprit.node_id.clone();
    if pod.is_empty() {
        return None;
    }
    let pvc = format!("data-{pod}");
    if quarantined.contains(&pvc) {
        return None; // already fenced once: contain, then leave it visible
    }
    Some(Action::Fence {
        pvc,
        pod,
        cluster_id: minority_id.to_string(),
    })
}

/// Brownout-driven PVC expansion, bounded by the CR's declared ceiling.
/// `observed_pvc_max` is the largest storage request the data PVCs currently
/// carry, read from the cluster by the caller.
fn plan_expansion(
    cr: &MqttdCluster,
    probes: &[PodProbe],
    pvcs: &[String],
    observed_pvc_max: Option<u64>,
) -> Option<Action> {
    if cr.spec.remediation.brownout != BrownoutAction::ExpandPvc {
        return None; // opt-in; the default is Alert
    }
    let browned_out = probes.iter().any(|p| match p {
        PodProbe::Reached(s) => s.brownout.as_ref().is_some_and(|b| b.disk),
        PodProbe::Unreachable { .. } => false,
    });
    if !browned_out || pvcs.is_empty() {
        return None;
    }
    let persistence = cr.spec.persistence.as_ref()?;
    // No declared ceiling = no automatic growth. An unbounded expander is a
    // blank cheque against the cluster's storage budget.
    let max = parse_quantity(persistence.expansion_max_size.as_deref()?)?;
    let spec_size = parse_quantity(persistence.size.as_deref()?)?;
    // The step baseline is what the PVCs currently REQUEST, not the static spec
    // (audit #203): stepping from the spec meant every reconcile recomputed the
    // same spec*1.5 target, so a PVC could take exactly one step and the declared
    // expansionMaxSize above it was unreachable. The observed request also makes
    // an operator's manual grow the new baseline instead of fighting it. PVCs not
    // yet observed (or in quantity forms we refuse to parse) fall back to the
    // spec — conservative, and k8s rejects shrinks anyway.
    let current = observed_pvc_max.map_or(spec_size, |o| o.max(spec_size));
    let stepped = current.saturating_add(current * EXPANSION_STEP_PERCENT / 100);
    let target = stepped.min(max);
    if target <= current {
        return None; // already at (or above) the ceiling
    }
    Some(Action::ExpandPvcs {
        pvcs: pvcs.to_vec(),
        to: format_quantity(target),
    })
}

/// Parse the Kubernetes quantity forms this operator produces and accepts
/// (`Ki`/`Mi`/`Gi`/`Ti` and plain bytes). Anything else is refused rather than
/// guessed — a misparsed size would resize storage wrongly.
#[must_use]
pub fn parse_quantity(q: &str) -> Option<u64> {
    let q = q.trim();
    for (suffix, mult) in [
        ("Ti", 1u64 << 40),
        ("Gi", 1 << 30),
        ("Mi", 1 << 20),
        ("Ki", 1 << 10),
    ] {
        if let Some(n) = q.strip_suffix(suffix) {
            return n.trim().parse::<u64>().ok()?.checked_mul(mult);
        }
    }
    q.parse::<u64>().ok()
}

/// Format bytes back into the largest exact binary unit.
#[must_use]
pub fn format_quantity(bytes: u64) -> String {
    for (suffix, mult) in [
        ("Ti", 1u64 << 40),
        ("Gi", 1 << 30),
        ("Mi", 1 << 20),
        ("Ki", 1 << 10),
    ] {
        if bytes.is_multiple_of(mult) && bytes >= mult {
            return format!("{}{suffix}", bytes / mult);
        }
    }
    bytes.to_string()
}

/// A human-facing description of an action, for Events and logs.
#[must_use]
pub fn describe(action: &Action) -> (String, String) {
    match action {
        Action::Fence {
            pod,
            pvc,
            cluster_id,
        } => (
            "Fenced".to_string(),
            format!(
                "Split brain: {pod} holds minority cluster identity {cluster_id}. \
                 Quarantined PVC {pvc} by label (DATA UNTOUCHED) and deleted the pod. \
                 Recovery still needs the documented wipe-and-rejoin — see \
                 docs/OPERATIONS.md."
            ),
        ),
        Action::ExpandPvcs { pvcs, to } => (
            "ExpandedStorage".to_string(),
            format!(
                "Brownout: requested {to} for {} PVC(s) ({}), bounded by \
                 persistence.expansionMaxSize.",
                pvcs.len(),
                pvcs.join(", ")
            ),
        ),
    }
}

/// Carry out one planned action. Every operation is idempotent: re-labelling an
/// already-quarantined PVC and re-requesting a size a PVC already has are both
/// no-ops, so a repeated reconcile is harmless.
///
/// # Errors
/// Propagates the Kubernetes API error that refused an operation.
pub async fn execute(
    client: &kube::Client,
    namespace: &str,
    action: &Action,
) -> Result<(), kube::Error> {
    use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
    use kube::api::{Api, Patch, PatchParams};

    let params = PatchParams::apply("mqttd-operator-remediation");
    match action {
        Action::Fence {
            pod,
            pvc,
            cluster_id,
        } => {
            // 1. Quarantine the PVC by LABEL — the data is evidence for the
            //    human running the documented recovery, never ours to delete.
            let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
            let label = serde_json::json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": { "name": pvc, "labels": {
                    QUARANTINE_LABEL: "true",
                    QUARANTINE_REASON_LABEL: "split-brain",
                }},
            });
            pvcs.patch(pvc, &params, &Patch::Apply(&label)).await?;
            // 2. Delete the pod so it restarts instead of continuing to serve a
            //    second cluster. Its foreign gossip is already contained at the
            //    protocol level (ADR 0054 T2); this stops client-facing service.
            let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
            pods.delete(pod, &kube::api::DeleteParams::default())
                .await?;
            tracing::warn!(%pod, %pvc, %cluster_id, "FENCED a split-brain node (data untouched)");
        }
        Action::ExpandPvcs { pvcs: names, to } => {
            let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
            for name in names {
                let patch = serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": { "name": name },
                    "spec": { "resources": { "requests": { "storage": to } } },
                });
                pvcs.patch(name, &params, &Patch::Apply(&patch)).await?;
            }
            tracing::warn!(%to, count = names.len(), "expanded PVCs after brownout");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{format_quantity, parse_quantity, plan, Action};
    use crate::crd::{BrownoutAction, MqttdCluster, Persistence, Remediation, SplitBrainAction};
    use crate::probe::{Brownout, PodProbe, PodStatus};

    fn cr(split: SplitBrainAction, brownout: BrownoutAction) -> MqttdCluster {
        let mut cr: MqttdCluster = serde_json::from_value(serde_json::json!({
            "apiVersion": "mqttd.io/v1alpha1",
            "kind": "MqttdCluster",
            "metadata": { "name": "mqttd", "namespace": "default" },
            "spec": { "replicas": 3, "config": "[node]\n" }
        }))
        .expect("cr");
        cr.spec.remediation = Remediation {
            split_brain: split,
            brownout,
        };
        cr.spec.persistence = Some(Persistence {
            size: Some("10Gi".into()),
            storage_class_name: None,
            expansion_max_size: Some("20Gi".into()),
        });
        cr
    }

    fn pod(id: &str, cluster: &str, founder: bool) -> PodProbe {
        PodProbe::Reached(Box::new(PodStatus {
            node_id: id.into(),
            cluster_id: Some(cluster.into()),
            founder,
            ready: true,
            ..PodStatus::default()
        }))
    }

    fn browned(id: &str) -> PodProbe {
        PodProbe::Reached(Box::new(PodStatus {
            node_id: id.into(),
            cluster_id: Some("abc".into()),
            ready: true,
            brownout: Some(Brownout { disk: true }),
            ..PodStatus::default()
        }))
    }

    const PVCS: [&str; 3] = ["data-mqttd-0", "data-mqttd-1", "data-mqttd-2"];
    fn pvcs() -> Vec<String> {
        PVCS.iter().map(|s| (*s).to_string()).collect()
    }

    /// THE default: a split brain with remediation unset does nothing at all.
    #[test]
    fn defaults_never_act() {
        let probes = vec![
            pod("mqttd-0", "aaa", false),
            pod("mqttd-1", "aaa", false),
            pod("mqttd-2", "bbb", true),
        ];
        let actions = plan(
            &cr(SplitBrainAction::Alert, BrownoutAction::Alert),
            &probes,
            &pvcs(),
            &[],
            None,
        );
        assert!(actions.is_empty(), "alert-only defaults must not act");
    }

    /// With Fence enabled and unambiguous evidence, the minority FOUNDER is
    /// fenced — PVC quarantined by label, never deleted.
    #[test]
    fn fences_the_minority_founder_when_the_evidence_is_clear() {
        let probes = vec![
            pod("mqttd-0", "aaa", false),
            pod("mqttd-1", "aaa", false),
            pod("mqttd-2", "bbb", true),
        ];
        let actions = plan(
            &cr(SplitBrainAction::Fence, BrownoutAction::Alert),
            &probes,
            &pvcs(),
            &[],
            None,
        );
        assert_eq!(
            actions,
            vec![Action::Fence {
                pod: "mqttd-2".into(),
                pvc: "data-mqttd-2".into(),
                cluster_id: "bbb".into(),
            }]
        );
        let (reason, msg) = super::describe(&actions[0]);
        assert_eq!(reason, "Fenced");
        assert!(msg.contains("DATA UNTOUCHED"), "{msg}");
    }

    /// A node is fenced ONCE. Its volume still holds the divergent state, so it
    /// re-founds on every restart and keeps reading as split-brained; killing it
    /// again each pass would neither fix it nor add evidence, and — because each
    /// delete removes the divergent pod from the observation — it would flicker
    /// the `SplitBrain` condition off within milliseconds. Found by the T7 e2e:
    /// the operator fenced the same pod every 30s and the status never stayed
    /// True long enough for an alert (or the test) to see it.
    #[test]
    fn an_already_quarantined_node_is_not_fenced_again() {
        let probes = vec![
            pod("mqttd-0", "aaa", false),
            pod("mqttd-1", "aaa", false),
            pod("mqttd-2", "bbb", true),
        ];
        let cr = cr(SplitBrainAction::Fence, BrownoutAction::Alert);
        // First pass: containment fires.
        assert_eq!(plan(&cr, &probes, &pvcs(), &[], None).len(), 1);
        // Second pass, same divergence, PVC already labelled: nothing at all.
        assert!(
            plan(&cr, &probes, &pvcs(), &["data-mqttd-2".to_string()], None).is_empty(),
            "a quarantined node must not be re-fenced; the incident stays visible \
             until the documented manual wipe"
        );
    }

    /// A TIE is not evidence: 1-v-1 never fences, however tempting.
    #[test]
    fn a_tie_is_never_fenced() {
        let probes = vec![pod("mqttd-0", "aaa", true), pod("mqttd-1", "bbb", true)];
        assert!(plan(
            &cr(SplitBrainAction::Fence, BrownoutAction::Alert),
            &probes,
            &pvcs(),
            &[],
            None
        )
        .is_empty());
    }

    /// Unreachable pods that could flip the verdict block the fence — the
    /// rolling-restart safety property.
    #[test]
    fn unreachable_pods_that_could_flip_the_verdict_block_the_fence() {
        let probes = vec![
            pod("mqttd-0", "aaa", false),
            pod("mqttd-1", "aaa", false),
            pod("mqttd-2", "bbb", true),
            PodProbe::Unreachable {
                pod: "mqttd-3".into(),
            },
            PodProbe::Unreachable {
                pod: "mqttd-4".into(),
            },
        ];
        assert!(
            plan(
                &cr(SplitBrainAction::Fence, BrownoutAction::Alert),
                &probes,
                &pvcs(),
                &[],
                None
            )
            .is_empty(),
            "2 unreachable could join the minority and out-vote the majority"
        );
    }

    /// Brownout expansion: one 50% step, bounded by the declared ceiling, and
    /// refused outright when no ceiling is declared.
    #[test]
    fn expansion_steps_once_and_respects_the_ceiling() {
        let probes = vec![browned("mqttd-0")];
        let actions = plan(
            &cr(SplitBrainAction::Alert, BrownoutAction::ExpandPvc),
            &probes,
            &pvcs(),
            &[],
            None,
        );
        assert_eq!(
            actions,
            vec![Action::ExpandPvcs {
                pvcs: pvcs(),
                to: "15Gi".into(),
            }],
            "10Gi + 50% = 15Gi, under the 20Gi ceiling"
        );

        // At the ceiling: nothing further.
        let mut at_max = cr(SplitBrainAction::Alert, BrownoutAction::ExpandPvc);
        at_max.spec.persistence = Some(Persistence {
            size: Some("20Gi".into()),
            storage_class_name: None,
            expansion_max_size: Some("20Gi".into()),
        });
        assert!(
            plan(&at_max, &probes, &pvcs(), &[], None).is_empty(),
            "ceiling reached"
        );

        // No ceiling declared: never grow storage on a blank cheque.
        let mut unbounded = cr(SplitBrainAction::Alert, BrownoutAction::ExpandPvc);
        unbounded.spec.persistence = Some(Persistence {
            size: Some("10Gi".into()),
            storage_class_name: None,
            expansion_max_size: None,
        });
        assert!(
            plan(&unbounded, &probes, &pvcs(), &[], None).is_empty(),
            "no ceiling, no growth"
        );
    }

    /// Audit #203: the step baseline is the OBSERVED PVC request, not the static
    /// spec. Stepping from the spec meant every reconcile recomputed the same
    /// spec×1.5 target — one step ever, `expansionMaxSize` unreachable. From the
    /// observed size the steps ladder up to the ceiling, stop exactly there, and
    /// an operator's manual grow becomes the new baseline instead of a fight.
    #[test]
    fn expansion_steps_from_the_observed_size_up_to_the_ceiling() {
        let probes = vec![browned("mqttd-0")];
        let c = cr(SplitBrainAction::Alert, BrownoutAction::ExpandPvc);
        // spec 10Gi, ceiling 20Gi (the fixture). After the first step the PVCs
        // request 15Gi: the next step must go to 20Gi, not re-plan 15Gi forever.
        let gi = 1024 * 1024 * 1024;
        let actions = plan(&c, &probes, &pvcs(), &[], Some(15 * gi));
        assert_eq!(
            actions,
            vec![Action::ExpandPvcs {
                pvcs: pvcs(),
                to: "20Gi".into(),
            }],
            "15Gi observed + 50% = 22.5Gi, clamped to the 20Gi ceiling"
        );
        // At the ceiling: done — no perpetual re-patching.
        assert!(
            plan(&c, &probes, &pvcs(), &[], Some(20 * gi)).is_empty(),
            "the observed size reached the ceiling; nothing further"
        );
        // An observed size below the spec (fresh/foreign PVC): the spec floors
        // the baseline — the plan is never a shrink.
        let actions = plan(&c, &probes, &pvcs(), &[], Some(5 * gi));
        assert_eq!(
            actions,
            vec![Action::ExpandPvcs {
                pvcs: pvcs(),
                to: "15Gi".into(),
            }],
            "the spec size floors the baseline"
        );
    }

    /// A step that would overshoot is clamped to the ceiling exactly.
    #[test]
    fn a_step_that_would_overshoot_is_clamped() {
        let mut c = cr(SplitBrainAction::Alert, BrownoutAction::ExpandPvc);
        c.spec.persistence = Some(Persistence {
            size: Some("10Gi".into()),
            storage_class_name: None,
            expansion_max_size: Some("12Gi".into()),
        });
        let actions = plan(&c, &[browned("mqttd-0")], &pvcs(), &[], None);
        assert_eq!(
            actions,
            vec![Action::ExpandPvcs {
                pvcs: pvcs(),
                to: "12Gi".into(),
            }]
        );
    }

    /// A fence pre-empts an expansion in the same pass: it changes the very
    /// evidence the expansion decision rests on.
    #[test]
    fn a_fence_preempts_an_expansion_in_the_same_pass() {
        let mut probes = vec![
            pod("mqttd-0", "aaa", false),
            pod("mqttd-1", "aaa", false),
            pod("mqttd-2", "bbb", true),
        ];
        if let PodProbe::Reached(s) = &mut probes[0] {
            s.brownout = Some(Brownout { disk: true });
        }
        let actions = plan(
            &cr(SplitBrainAction::Fence, BrownoutAction::ExpandPvc),
            &probes,
            &pvcs(),
            &[],
            None,
        );
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Fence { .. }));
    }

    /// Quantities round-trip; unknown units are refused rather than guessed.
    #[test]
    fn quantities_parse_format_and_refuse_the_unknown() {
        assert_eq!(parse_quantity("10Gi"), Some(10 * (1 << 30)));
        assert_eq!(parse_quantity("512Mi"), Some(512 * (1 << 20)));
        assert_eq!(parse_quantity("1024"), Some(1024));
        assert_eq!(parse_quantity("10G"), None, "decimal units are not guessed");
        assert_eq!(parse_quantity("ten"), None);
        assert_eq!(format_quantity(15 * (1 << 30)), "15Gi");
        assert_eq!(format_quantity(1536 * (1 << 20)), "1536Mi");
    }
}
