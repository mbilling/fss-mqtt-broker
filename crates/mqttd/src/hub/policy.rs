//! Resource-governance policy at the publish seam — brownout axes and the refusal
//! plan (ADR 0041; issue #258 slice 2: moved verbatim from `hub/mod.rs`).
//!
//! **The invariant this module owns:** a refusal is a POLICY decision made
//! on-loop, before any effect (issue #238's plan pass): `plan_refusal` answers
//! from the already-known brownout axes and quota state, never from I/O, so a
//! refused publish is effect-free and a retry is idempotent. Brownout is an
//! edge-triggered growth gate — axes flip via `set_brownout_axis`, reads and
//! acks continue — and `quota_full` is the sessions-cap admission gate. What a
//! refusal is TOLD to each protocol version lives with `PublishRefusal` in
//! `hub/mod.rs`; what the store did lives in the lanes (ADR 0061).

#[allow(clippy::wildcard_imports)] // an intra-hub module split (#258): the five
// siblings share one type/state vocabulary by design, and enumerating it would
// re-couple every future hub change to six import lists. Scoped to these files.
use super::*;

/// A resource whose watermark can put the broker into brownout (ADR 0041).
///
/// Separate axes because they are independent watchers with independent watermarks, and
/// the effective state is their OR. Collapsing them into one flag would let the disk
/// watcher's "I am fine now" lift a brownout that memory pressure is still asking for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BrownoutAxis {
    /// On-disk store bytes over `MQTTD_STORE_MAX_BYTES` (T5).
    Disk,
    /// Process resident memory over `MQTTD_MEMORY_MAX_BYTES` (T8).
    Memory,
}

impl BrownoutAxis {
    /// The metric label — also the word used in logs and `/statusz`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disk => "disk",
            Self::Memory => "memory",
        }
    }
}

impl Hub {
    /// Attach the shared brownout status the `/statusz` body reads (ADR 0054).
    pub fn attach_brownout_status(&mut self, status: Arc<crate::health::BrownoutStatus>) {
        self.brownout_status = Some(status);
    }

    /// Record that `axis` is (or is no longer) over its watermark, and recompute the
    /// effective brownout state as the OR across axes (ADR 0041 T5/T8).
    ///
    /// The per-axis gauge always follows its own axis, so an operator can see *which*
    /// resource is under pressure. The aggregate — the flag that refuses growth writes,
    /// and the `/statusz` state — flips only when the OR changes, so a second axis going
    /// over while the first already has is not logged as a fresh brownout, and the first
    /// recovering does not lift a brownout the second is still asking for.
    pub(super) fn set_brownout_axis(&mut self, axis: BrownoutAxis, on: bool) {
        if on {
            self.brownout_axes.insert(axis);
        } else {
            self.brownout_axes.remove(&axis);
        }
        // Per-axis visibility, regardless of whether the aggregate moved.
        if let Some(m) = &self.metrics {
            m.set_brownout(axis.as_str(), on);
        }

        let effective = !self.brownout_axes.is_empty();
        if effective != self.brownout {
            if effective {
                warn!(
                    axis = axis.as_str(),
                    "watermark exceeded: BROWNOUT — growth writes refused (ADR 0041)"
                );
            } else {
                info!(
                    axis = axis.as_str(),
                    "back under every watermark: brownout lifted (ADR 0041)"
                );
            }
            // ADR 0054: brownout is a STATE, not just symptoms — flip the shared
            // /statusz flag on every aggregate transition.
            if let Some(s) = &self.brownout_status {
                s.set(effective);
            }
        } else if on {
            // Already browned out on another axis: worth a line, because the operator
            // needs to fix BOTH before growth writes resume.
            warn!(
                axis = axis.as_str(),
                "a second resource is over its watermark; brownout continues (ADR 0041)"
            );
        }
        self.brownout = effective;
    }

    /// PLAN, DECIDE, COMMIT (issue #238): the refusal a fan-out that owes a durable
    /// append would hit, decided BEFORE the fan-out takes any side effect.
    ///
    /// This is what makes a refusal EFFECT-FREE, and therefore what makes the
    /// publisher's retry idempotent: nothing is retained, nothing is appended, nothing
    /// reaches a subscriber's wire and no peer forward leaves, so a resend (a v3.1.1
    /// client's mandatory one included) re-decides a decision with no residue rather
    /// than duplicating half a fan-out.
    ///
    /// Atomicity: the decision is FROZEN across the plan-and-submit span — this
    /// `self.brownout` read through the last lane submission
    /// ([`Hub::submit_append`], issue #242) — because the whole span runs inside ONE
    /// dispatch: `run()` awaits each dispatch to completion, so its internal awaits
    /// never interleave another command, and `self.brownout` is written only by the
    /// `SetBrownout` handler, i.e. by another command. The off-loop half executes
    /// only data frozen into its [`AppendJob`], against a worker that structurally
    /// cannot read hub state, and reports in a vocabulary ([`LaneOutcome`]) that
    /// cannot even express a refusal. A flag flip queued behind this dispatch
    /// therefore governs the NEXT publish, never this one's admitted jobs — every
    /// interleaving linearizes to "publish committed first, then the flag flipped".
    /// Anything that splits the span across commands, lets a watcher write
    /// `brownout` directly, or lets a lane decide policy breaks this silently; the
    /// `debug_assert!` in [`submit_append`](Self::submit_append) and
    /// [`AppendDone`](HubCommand::AppendDone) being the ONLY writer of completion
    /// state are the tripwires.
    pub(super) fn plan_refusal(&self, owes_durable: bool) -> Option<PublishRefusal> {
        (owes_durable && self.brownout).then_some(PublishRefusal::Brownout)
    }

    /// Count a refusal the publisher is TOLD about. Deliberately NOT
    /// `publish_dropped`: the publisher can act on the answer, so counting it as a
    /// loss would over-report losses that never happened (see the metric's own
    /// rustdoc). A refusal nobody is told about is the opposite case, and
    /// [`durable_append`](Self::durable_append) counts that one as the drop it is.
    pub(super) fn count_refusal(&self, r: PublishRefusal) {
        if let Some(m) = &self.metrics {
            match r {
                PublishRefusal::Brownout => m.quota_rejected("brownout-publish"),
                PublishRefusal::RetainedQuota => m.quota_rejected("retained"),
            }
        }
    }

    /// Whether this publish's shared selection would land on a LOCAL member that owes a
    /// durable append — the shared half of the PLAN pass (issue #238).
    ///
    /// PEEKS the selection rather than making it: `select_shared` advances the group's
    /// round-robin cursor, and a publish that is about to be refused must not consume a
    /// member's turn. Remote members are excluded because their durability is decided on
    /// their own node, by its own plan pass.
    pub(super) fn shared_plan_owes_durable(&self, topic: &str, qos: QoS) -> bool {
        self.shared_candidates(topic).into_iter().any(|(key, cs)| {
            self.peek_shared(&key, &cs).is_some_and(|c| {
                c.node.is_none() && self.owes_durable(&c.client, min_qos(qos, c.qos))
            })
        })
    }
}
