//! The two ack-honesty decisions a gated publish passes through (issue #613,
//! wave A items 2.1 and 2.4).
//!
//! Both are PURE functions: they take facts the hub has already computed and
//! return a verdict. That is what lets the RULE be unit-tested without a hub, a
//! cluster or a clock, and what keeps `hub/mod.rs` and `hub/forwarding.rs` down
//! to a one-line call site each.
//!
//! Neither function may ever be the thing that RELEASES an ack — they only say
//! when one must be HELD or REFUSED. The release stays in
//! `try_complete_pending`, against recorded evidence, exactly as before.

use std::time::Duration;

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

/// What `register_pending` must do with a NEW gated publish when the pending
/// ledger is at `PENDING_PUBLISH_CAP` (item 2.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Admission {
    /// There is room. Store it.
    Admit,
    /// The cap is full of entries that are all still YOUNG — genuine
    /// back-pressure, not a stuck obligation. REFUSE the arriving publish (v5
    /// `0x97`, v3.1.1 close).
    ///
    /// Refusing the ARRIVAL is the only refusal that is trivially effect-free,
    /// which is what `refuse_pending`'s contract demands: nothing of this
    /// publish is stored anywhere yet, so "nothing was stored, retry" is exactly
    /// true. Evicting the OLDEST instead withheld the ack of a publish that may
    /// already have been durably owed to a subscriber — a claim-nothing answer
    /// whose retry duplicates a message that IS stored, and it punished the
    /// publisher that had waited longest.
    Refuse,
    /// Liveness backstop: the oldest entry is older than `max_age`, i.e. it is
    /// stuck rather than merely queued. Evict it (ack WITHHELD, never refused —
    /// it may be stored) and admit the arrival. Counted separately from
    /// [`Refuse`](Self::Refuse) so the two conditions are never one number.
    EvictOldest,
    /// The preferred victim (issue #613 item 2.1 x 2.4): an entry whose publisher
    /// has ALREADY been answered, still held only for the settle window's replay.
    /// Evicting it costs no publisher an answer — it withholds nothing and refuses
    /// nothing — so it is always taken ahead of refusing an arriving publish or
    /// withholding an old one. This is what keeps item 2.1's longer entry lifetimes
    /// from turning into a stream of `0x97`s during a long takeover window.
    EvictReplayOnly,
}

/// Decide admission. `oldest_age` is the age of the oldest pending entry, or
/// `None` when the ledger is empty. `has_replay_only` is whether any entry has
/// already been acknowledged and survives only for the settle window's replay.
///
/// The order is the whole policy: never make a publisher pay while a slot is held
/// by an entry nobody is waiting on.
#[must_use]
pub(super) fn admit_pending(
    len: usize,
    cap: usize,
    has_replay_only: bool,
    oldest_age: Option<Duration>,
    max_age: Duration,
) -> Admission {
    if len < cap {
        return Admission::Admit;
    }
    if has_replay_only {
        return Admission::EvictReplayOnly;
    }
    match oldest_age {
        Some(age) if age >= max_age => Admission::EvictOldest,
        _ => Admission::Refuse,
    }
}

#[cfg(test)]
mod tests {
    use super::{admit_pending, awaits_settle, Admission};
    use std::time::Duration;

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

    /// Item 2.4: at the cap with young entries and nothing already answered, the
    /// NEW publish is refused — the old entry, which may already be stored, is
    /// left alone.
    #[test]
    fn a_full_ledger_of_young_entries_refuses_the_arrival() {
        let max = Duration::from_secs(60);
        assert_eq!(
            admit_pending(4096, 4096, false, Some(Duration::from_secs(1)), max),
            Admission::Refuse
        );
        assert_eq!(
            admit_pending(4096, 4096, false, None, max),
            Admission::Refuse,
            "no oldest to judge: refuse, never evict blindly"
        );
    }

    /// The backstop still fires, so one stuck obligation cannot wedge the
    /// ledger shut forever.
    #[test]
    fn a_stuck_oldest_entry_is_still_evicted() {
        let max = Duration::from_secs(60);
        assert_eq!(
            admit_pending(4096, 4096, false, Some(Duration::from_secs(60)), max),
            Admission::EvictOldest
        );
    }

    #[test]
    fn below_the_cap_always_admits() {
        assert_eq!(
            admit_pending(
                4095,
                4096,
                false,
                Some(Duration::from_secs(999)),
                Duration::from_secs(60)
            ),
            Admission::Admit
        );
    }

    /// Item 2.1 x 2.4: an entry whose publisher was already answered is the
    /// victim, ahead of both refusing the arrival and withholding the oldest.
    /// Without this, item 2.1's longer entry lifetimes turn directly into
    /// refused publishers during a long takeover window.
    #[test]
    fn a_full_ledger_prefers_an_already_acked_victim() {
        let max = Duration::from_secs(60);
        assert_eq!(
            admit_pending(4096, 4096, true, Some(Duration::from_secs(1)), max),
            Admission::EvictReplayOnly
        );
        assert_eq!(
            admit_pending(4096, 4096, true, Some(Duration::from_secs(999)), max),
            Admission::EvictReplayOnly,
            "an answered entry is cheaper to lose than a withheld ack, at any age"
        );
    }
}
