//! Turning per-pod `/statusz` into one cluster verdict
//! ([ADR 0055](../../../docs/adr/0055-kubernetes-operator.md) T3).
//!
//! Pure functions over [`PodProbe`]s: no I/O, no client, no clock — so every
//! judgement below is unit-testable, and the reconciler stays a thin shell around
//! it. T3 is **alert-only**: this module decides *what is true*, never what to do
//! about it; the opt-in remediations read these conditions in T4.
//!
//! The rule that shapes everything: **an unreachable pod is absent evidence, not
//! bad news.** A rolling restart, a slow start, a pod without an IP yet — none of
//! those are a split brain or a lost quorum, and a controller that confuses the two
//! would fence healthy nodes during routine operations.

use crate::crd::{BootstrapPolicy, MqttdClusterStatus, StatusCondition};
use crate::probe::PodProbe;
use std::collections::BTreeSet;

/// A condition an operator (or an alert) acts on.
pub const COND_RECONCILED: &str = "Reconciled";
/// Two or more distinct cluster identities are live (ADR 0054 T2).
pub const COND_SPLIT_BRAIN: &str = "SplitBrain";
/// Every reachable pod reports the same applied-config checksum.
pub const COND_CONVERGED: &str = "Converged";
/// A gossip-key rotation window is open somewhere (accepted keys > 1).
pub const COND_ROTATION: &str = "RotationInProgress";
/// At least one pod is refusing growth writes (ADR 0041 §5).
pub const COND_BROWNOUT: &str = "Brownout";
/// A decommission drain is running.
pub const COND_DRAINING: &str = "Draining";

/// The aggregate verdict plus the Events worth emitting for it.
#[derive(Debug)]
pub struct Observation {
    /// The status to patch onto the CR.
    pub status: MqttdClusterStatus,
    /// Human-facing Events (reason, message) for conditions that just became true.
    pub events: Vec<(String, String)>,
}

/// The facts one observation established, before they are phrased as conditions.
struct Facts<'a> {
    identities: BTreeSet<&'a str>,
    checksums: BTreeSet<&'a str>,
    reached: usize,
    unreachable: Vec<&'a str>,
    ready_replicas: i32,
    members: Option<i32>,
    brownout: bool,
    draining: bool,
    rotating: bool,
}

impl Facts<'_> {
    fn split_brain(&self) -> bool {
        self.identities.len() > 1
    }
    fn converged(&self) -> bool {
        self.checksums.len() <= 1
    }
    fn cluster_id(&self) -> Option<String> {
        (self.identities.len() == 1).then(|| {
            self.identities
                .iter()
                .next()
                .copied()
                .unwrap_or_default()
                .to_string()
        })
    }
    fn identity_list(&self) -> String {
        self.identities
            .iter()
            .copied()
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Whether ordinal 0 must be stopped from founding, given what we just observed.
///
/// `prev` is `status.bootstrapped` from the last pass. The latch closes the first time
/// exactly ONE identity is seen from a reachable pod — proof a cluster exists — and
/// never re-opens on its own:
///
/// * zero identities is absent evidence (an unreachable or still-starting fleet), never
///   a reason to un-latch — that would re-arm founding at the worst moment;
/// * a SPLIT BRAIN (two identities) is likewise no evidence about whether a cluster
///   exists, so it leaves the latch exactly as it was;
/// * `AllowRebootstrap` is the only thing that clears it, and only because a human
///   asked for that in the spec.
#[must_use]
pub fn latch_bootstrapped(
    prev: Option<bool>,
    policy: BootstrapPolicy,
    identities_seen: usize,
) -> Option<bool> {
    if policy == BootstrapPolicy::AllowRebootstrap {
        // Explicitly cleared, so the state is honest rather than stale: an operator
        // reading `bootstrapped: true` while founding is permitted would be misled.
        return None;
    }
    if identities_seen == 1 {
        return Some(true);
    }
    prev
}

/// Aggregate `probes` for a cluster whose spec asks for `desired` replicas.
#[must_use]
pub fn observe(probes: &[PodProbe], desired: i32, generation: Option<i64>) -> Observation {
    let facts = gather(probes);
    let phase = phase_of(
        facts.split_brain(),
        facts.ready_replicas,
        desired,
        facts.reached,
        facts.draining,
    );
    Observation {
        status: MqttdClusterStatus {
            phase: Some(phase.to_string()),
            ready_replicas: Some(facts.ready_replicas),
            members: facts.members,
            cluster_id: facts.cluster_id(),
            brownout: Some(facts.brownout),
            observed_generation: generation,
            // Filled by the reconciler, which owns the previous value and the spec.
            bootstrapped: None,
            conditions: conditions_for(&facts),
        },
        events: events_for(&facts),
    }
}

/// Extract the facts from the probes. An unreachable pod contributes only its
/// name — never a vote on identity, convergence, or health.
fn gather(probes: &[PodProbe]) -> Facts<'_> {
    let reached: Vec<_> = probes
        .iter()
        .filter_map(|p| match p {
            PodProbe::Reached(s) => Some(s.as_ref()),
            PodProbe::Unreachable { .. } => None,
        })
        .collect();
    Facts {
        // Only pods that have ADOPTED an identity vote: a joiner that has not yet
        // heard from the founder is silent here, not dissenting.
        identities: reached
            .iter()
            .filter_map(|s| s.cluster_id.as_deref())
            .collect(),
        // Pods reporting no checksum (older build, no config file) do not make the
        // fleet look divergent.
        checksums: reached
            .iter()
            .filter_map(|s| s.config.as_ref().map(|c| c.checksum.as_str()))
            .filter(|c| !c.is_empty())
            .collect(),
        unreachable: probes
            .iter()
            .filter_map(|p| match p {
                PodProbe::Unreachable { pod } => Some(pod.as_str()),
                PodProbe::Reached(_) => None,
            })
            .collect(),
        ready_replicas: i32::try_from(reached.iter().filter(|s| s.ready).count())
            .unwrap_or(i32::MAX),
        members: reached
            .iter()
            .map(|s| i32::try_from(s.members.len()).unwrap_or(i32::MAX))
            .max(),
        brownout: reached
            .iter()
            .any(|s| s.brownout.as_ref().is_some_and(|b| b.disk)),
        draining: reached.iter().any(|s| {
            s.decommission
                .as_ref()
                .is_some_and(|d| d.active && !d.complete)
        }),
        rotating: reached
            .iter()
            .any(|s| s.keys.as_ref().is_some_and(|k| k.count > 1)),
        reached: reached.len(),
    }
}

/// Phrase the facts as Kubernetes conditions.
fn conditions_for(f: &Facts<'_>) -> Vec<StatusCondition> {
    let mut out = vec![
        cond(
            COND_RECONCILED,
            true,
            "Observed",
            &observed_message(f.reached, &f.unreachable),
        ),
        cond(
            COND_SPLIT_BRAIN,
            f.split_brain(),
            if f.split_brain() {
                "MultipleClusterIds"
            } else {
                "SingleClusterId"
            },
            &if f.split_brain() {
                format!(
                    "{} distinct cluster identities across reachable pods: {}",
                    f.identities.len(),
                    f.identity_list()
                )
            } else {
                "all reachable pods agree on one cluster identity".into()
            },
        ),
        cond(
            COND_CONVERGED,
            f.converged(),
            if f.converged() {
                "OneChecksum"
            } else {
                "ChecksumDivergence"
            },
            &if f.converged() {
                "all reachable pods report the same applied config".into()
            } else {
                format!("{} distinct config checksums are live", f.checksums.len())
            },
        ),
    ];
    if f.rotating {
        out.push(cond(
            COND_ROTATION,
            true,
            "KeyWindowOpen",
            "a gossip-key rotation window is open (accepted keys > 1)",
        ));
    }
    if f.brownout {
        out.push(cond(
            COND_BROWNOUT,
            true,
            "DiskWatermark",
            "at least one pod is refusing growth writes (ADR 0041 brownout)",
        ));
    }
    if f.draining {
        out.push(cond(
            COND_DRAINING,
            true,
            "Decommission",
            "a decommission drain is running",
        ));
    }
    out
}

/// The Events worth raising — what an operator would want in `kubectl describe`
/// without diffing status. T3 alerts; T4 acts.
fn events_for(f: &Facts<'_>) -> Vec<(String, String)> {
    let mut events = Vec::new();
    if f.split_brain() {
        events.push((
            "SplitBrain".to_string(),
            format!(
                "SPLIT BRAIN: {} distinct cluster identities live ({}). Fence the \
                 re-founded node — see docs/OPERATIONS.md.",
                f.identities.len(),
                f.identity_list()
            ),
        ));
    }
    if f.brownout {
        events.push((
            "Brownout".to_string(),
            "Disk watermark exceeded on at least one pod: growth writes refused; \
             QoS>=1 publishers are refused rather than acked, so re-delivery is \
             theirs to decide (reads, subscriber acks and resumes continue). \
             Expand storage or raise the watermark."
                .to_string(),
        ));
    }
    if !f.converged() {
        events.push((
            "ConfigDivergence".to_string(),
            format!(
                "{} distinct config checksums are live; a config roll has not converged",
                f.checksums.len()
            ),
        ));
    }
    events
}

/// The coarse lifecycle phase. Split brain outranks everything: it is the one
/// state where "mostly ready" is actively misleading.
fn phase_of(
    split_brain: bool,
    ready: i32,
    desired: i32,
    reached: usize,
    draining: bool,
) -> &'static str {
    if split_brain {
        return "SplitBrain";
    }
    if reached == 0 {
        return "Pending";
    }
    if draining {
        return "Draining";
    }
    if ready >= desired {
        "Ready"
    } else if ready > 0 {
        "Degraded"
    } else {
        "Forming"
    }
}

fn observed_message(reached: usize, unreachable: &[&str]) -> String {
    if unreachable.is_empty() {
        format!("{reached} pods observed")
    } else {
        format!(
            "{reached} pods observed; {} unreachable ({}) — treated as unknown, not unhealthy",
            unreachable.len(),
            unreachable.join(", ")
        )
    }
}

fn cond(kind: &str, status: bool, reason: &str, message: &str) -> StatusCondition {
    StatusCondition {
        r#type: kind.to_string(),
        status: if status { "True" } else { "False" }.to_string(),
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
        last_transition_time: None,
    }
}

#[cfg(test)]
mod tests {

    /// The latch closes on proof a cluster exists, and never re-opens by itself
    /// (ADR 0055 T9). Everything here is about NOT re-arming founding by accident:
    /// absent evidence, a split brain, and a still-forming fleet must all leave the
    /// previous verdict alone.
    #[test]
    fn the_bootstrap_latch_is_monotonic_and_closes_on_one_identity() {
        use super::latch_bootstrapped as latch;
        use crate::crd::BootstrapPolicy::{AllowRebootstrap, Guarded};

        // One identity = a cluster exists. Latch.
        assert_eq!(latch(None, Guarded, 1), Some(true));
        assert_eq!(latch(Some(true), Guarded, 1), Some(true));

        // Zero identities is ABSENT evidence — an unreachable or still-starting fleet,
        // not proof the cluster is gone. Re-arming founding here is the whole bug.
        assert_eq!(latch(Some(true), Guarded, 0), Some(true));
        assert_eq!(
            latch(None, Guarded, 0),
            None,
            "nothing observed, no opinion"
        );

        // A split brain says nothing about whether a cluster exists; hold the verdict.
        assert_eq!(latch(Some(true), Guarded, 2), Some(true));

        // Only a human asking clears it — and then it is CLEARED, not left stale, so
        // `bootstrapped: true` never coexists with founding being permitted.
        assert_eq!(latch(Some(true), AllowRebootstrap, 1), None);
        assert_eq!(latch(None, AllowRebootstrap, 0), None);
    }
    use super::{observe, COND_BROWNOUT, COND_CONVERGED, COND_DRAINING, COND_SPLIT_BRAIN};
    use crate::probe::{Brownout, ConfigStamp, Decommission, Keys, Member, PodProbe, PodStatus};

    fn pod(id: &str, cluster: Option<&str>, ready: bool) -> PodProbe {
        PodProbe::Reached(Box::new(PodStatus {
            node_id: id.into(),
            cluster_id: cluster.map(Into::into),
            ready,
            members: vec![Member { id: id.into() }],
            config: Some(ConfigStamp {
                checksum: "same".into(),
            }),
            ..PodStatus::default()
        }))
    }

    fn condition<'a>(o: &'a super::Observation, kind: &str) -> Option<&'a str> {
        o.status
            .conditions
            .iter()
            .find(|c| c.r#type == kind)
            .map(|c| c.status.as_str())
    }

    /// A healthy 3-node cluster: one identity, all ready, converged, no alarms.
    #[test]
    fn a_healthy_cluster_is_ready_and_quiet() {
        let probes = vec![
            pod("mqttd-0", Some("abc"), true),
            pod("mqttd-1", Some("abc"), true),
            pod("mqttd-2", Some("abc"), true),
        ];
        let o = observe(&probes, 3, Some(7));
        assert_eq!(o.status.phase.as_deref(), Some("Ready"));
        assert_eq!(o.status.ready_replicas, Some(3));
        assert_eq!(o.status.cluster_id.as_deref(), Some("abc"));
        assert_eq!(o.status.observed_generation, Some(7));
        assert_eq!(condition(&o, COND_SPLIT_BRAIN), Some("False"));
        assert_eq!(condition(&o, COND_CONVERGED), Some("True"));
        assert!(o.events.is_empty(), "a healthy cluster raises nothing");
    }

    /// Two identities = split brain: the condition, the phase, and an Event that
    /// names both ids. This outranks readiness — every pod here is "ready".
    #[test]
    fn two_cluster_identities_are_a_split_brain() {
        let probes = vec![
            pod("mqttd-0", Some("aaa"), true),
            pod("mqttd-1", Some("bbb"), true),
        ];
        let o = observe(&probes, 2, None);
        assert_eq!(o.status.phase.as_deref(), Some("SplitBrain"));
        assert_eq!(condition(&o, COND_SPLIT_BRAIN), Some("True"));
        assert_eq!(o.status.cluster_id, None, "no single identity to report");
        let (reason, msg) = &o.events[0];
        assert_eq!(reason, "SplitBrain");
        assert!(msg.contains("aaa") && msg.contains("bbb"), "{msg}");
    }

    /// THE regression that matters: unreachable pods are absent evidence. A pod
    /// mid-restart must not look like a second cluster or a lost quorum.
    #[test]
    fn unreachable_pods_are_unknown_not_unhealthy() {
        let probes = vec![
            pod("mqttd-0", Some("abc"), true),
            PodProbe::Unreachable {
                pod: "mqttd-1".into(),
            },
            PodProbe::Unreachable {
                pod: "mqttd-2".into(),
            },
        ];
        let o = observe(&probes, 3, None);
        assert_eq!(condition(&o, COND_SPLIT_BRAIN), Some("False"));
        assert_eq!(condition(&o, COND_CONVERGED), Some("True"));
        assert_eq!(o.status.cluster_id.as_deref(), Some("abc"));
        assert_eq!(
            o.status.phase.as_deref(),
            Some("Degraded"),
            "not SplitBrain"
        );
        assert!(o.events.is_empty());
        let reconciled = o
            .status
            .conditions
            .iter()
            .find(|c| c.r#type == super::COND_RECONCILED)
            .expect("reconciled condition");
        let msg = reconciled.message.as_deref().unwrap_or_default();
        assert!(
            msg.contains("unreachable") && msg.contains("mqttd-1"),
            "{msg}"
        );
    }

    /// A joiner that has not adopted an identity yet is silent, not dissenting.
    #[test]
    fn a_pod_without_an_identity_yet_does_not_split_the_verdict() {
        let probes = vec![
            pod("mqttd-0", Some("abc"), true),
            pod("mqttd-1", None, false),
        ];
        let o = observe(&probes, 2, None);
        assert_eq!(condition(&o, COND_SPLIT_BRAIN), Some("False"));
        assert_eq!(o.status.cluster_id.as_deref(), Some("abc"));
        assert_eq!(o.status.phase.as_deref(), Some("Degraded"));
    }

    /// Brownout and drain surface as conditions + an Event (brownout), and a drain
    /// takes the phase — a shrinking cluster is not "Degraded", it is draining.
    #[test]
    fn brownout_and_drain_surface_without_acting() {
        let mut probes = vec![
            pod("mqttd-0", Some("abc"), true),
            pod("mqttd-1", Some("abc"), true),
        ];
        if let PodProbe::Reached(s) = &mut probes[0] {
            s.brownout = Some(Brownout { disk: true });
        }
        if let PodProbe::Reached(s) = &mut probes[1] {
            s.decommission = Some(Decommission {
                active: true,
                pending: 4,
                complete: false,
            });
        }
        let o = observe(&probes, 2, None);
        assert_eq!(condition(&o, COND_BROWNOUT), Some("True"));
        assert_eq!(condition(&o, COND_DRAINING), Some("True"));
        assert_eq!(o.status.brownout, Some(true));
        assert_eq!(o.status.phase.as_deref(), Some("Draining"));
        assert!(o.events.iter().any(|(r, _)| r == "Brownout"));
    }

    /// A config roll that has not converged, and an open rotation window.
    #[test]
    fn divergent_checksums_and_open_rotation_windows_are_reported() {
        let mut probes = vec![
            pod("mqttd-0", Some("abc"), true),
            pod("mqttd-1", Some("abc"), true),
        ];
        if let PodProbe::Reached(s) = &mut probes[1] {
            s.config = Some(ConfigStamp {
                checksum: "different".into(),
            });
            s.keys = Some(Keys { count: 2 });
        }
        let o = observe(&probes, 2, None);
        assert_eq!(condition(&o, COND_CONVERGED), Some("False"));
        assert_eq!(condition(&o, super::COND_ROTATION), Some("True"));
        assert!(o.events.iter().any(|(r, _)| r == "ConfigDivergence"));
    }

    /// Nothing reachable yet: Pending, and no alarms invented from silence.
    #[test]
    fn no_reachable_pods_is_pending() {
        let probes = vec![PodProbe::Unreachable {
            pod: "mqttd-0".into(),
        }];
        let o = observe(&probes, 3, None);
        assert_eq!(o.status.phase.as_deref(), Some("Pending"));
        assert_eq!(o.status.ready_replicas, Some(0));
        assert!(o.events.is_empty());
    }
}
