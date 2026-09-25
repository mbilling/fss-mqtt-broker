//! The settle-gate ack decision a gated publish passes through (issue #613,
//! wave A item 2.1).
//!
//! A PURE function: it takes facts the hub has already computed and returns a
//! verdict. That is what lets the RULE be unit-tested without a hub, a cluster
//! or a clock, and what keeps `hub/mod.rs` down to a one-line call site.
//!
//! It may never be the thing that RELEASES an ack — it only says when one must
//! be HELD. The release stays in `try_complete_pending`, against recorded
//! evidence, exactly as before.
//!
//! Item 2.4's admission verdict used to live here too. It was withdrawn when this
//! branch merged the byte-bounded pending table (3571682); see
//! `Hub::register_pending` for why.

/// Whether a gated publish must HOLD its acknowledgement until this node's
/// routing view settles (item 2.1).
///
/// The rule used to be decided in `register_pending`, BEFORE the fan-out ran,
/// and therefore had to assume the worst: every gated publish arriving during a
/// takeover or membership window held its ack, including the ones that landed
/// squarely on a local subscriber that was never in any doubt. The evidence the
/// gate actually wants — "did this fan-out reach anybody?" — only exists AFTER
/// the fan-out, which is why the RECEIVER-side twin (the `RemotePublish` arm:
/// `matched == 0 && routing_unsettled()`) has always used it and the sender side
/// never did.
///
/// `matched` is the local fan-out's match count. `shared_placed` is whether any
/// shared group put this message somewhere — a local member or a peer member.
/// Either being non-zero is recorded evidence that the publish reached a
/// subscriber, so the ack proceeds on its ordinary obligations (appends, peer
/// verdicts, the retained commit), each of which still holds it on its own.
/// Only a fan-out that reached NOBODY, while this node admits its view is
/// incomplete, is a conclusion it cannot stand behind — and that case is
/// unchanged.
///
/// **This decides the ACK only.** It never decides whether the settle pass still
/// owes this publish a re-delivery or a re-route: that set is selected by
/// `PendingPublish::awaiting_settle`, it is chosen at registration from
/// `routing_unsettled()` alone, and it must not be narrowed by anything here
/// (issue #613, CORRECTION 1 — narrowing it drops deliveries with no compile
/// error and no failing test).
#[must_use]
pub(super) fn awaits_settle(matched: usize, shared_placed: bool, routing_unsettled: bool) -> bool {
    matched == 0 && !shared_placed && routing_unsettled
}

#[cfg(test)]
mod tests {
    use super::awaits_settle;

    /// The whole point of item 2.1: a publish that REACHED a local subscriber is
    /// not held, even though the routing view is unsettled. This is the case the
    /// old registration-time rule got wrong for every publish in a takeover
    /// window.
    #[test]
    fn a_matched_fan_out_never_waits_for_settle() {
        assert!(!awaits_settle(1, false, true));
        assert!(!awaits_settle(7, false, true));
    }

    /// A shared group placing the message is evidence too — including when it
    /// placed it on a PEER, which is why `deliver_shared` has to report it.
    #[test]
    fn a_shared_placement_is_evidence() {
        assert!(!awaits_settle(0, true, true));
    }

    /// The case the gate exists for is untouched: nobody, anywhere, while the
    /// view is admittedly incomplete.
    #[test]
    fn a_zero_match_fan_out_on_an_unsettled_view_still_holds() {
        assert!(awaits_settle(0, false, true));
    }

    /// A settled view never holds, whatever the fan-out found.
    #[test]
    fn a_settled_view_never_holds() {
        assert!(!awaits_settle(0, false, false));
    }
}
