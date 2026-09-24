//! Cached mesh-wholeness state (issue #613, wave B item 1.1).
//!
//! [`Hub::mesh_whole`](super::Hub::mesh_whole) and
//! [`Hub::mesh_settled`](super::Hub::mesh_settled) resolve through `peers_all`,
//! which calls `Placement::members()` — one `String` clone per member, built and
//! dropped — and `routing_unsettled` calls `mesh_settled` on EVERY gated publish.
//! The answer only changes at five observable transitions, so it is computed
//! there and read here.
//!
//! **The cache may never claim settled when it is not.** Membership is written by
//! SWIM on another thread, so a member can join between two transitions and the
//! live answer would flip to "not whole" with nothing here noticing. Every cached
//! answer therefore carries a cheap fingerprint of the placement view it was
//! computed against, and a read whose LIVE fingerprint differs answers `false` —
//! not whole, not settled — until the next recompute, at most one sweep tick
//! away. The failure direction is over-conservatism: an ack held one tick longer
//! than it had to be, never an ack released against a mesh that had grown a
//! member with no link.
//!
//! This does NOT fork `peers_all`'s definition of which members count (the stated
//! invariant): `peers_all` is still the only thing that computes the answer. This
//! module only remembers what it said and when it was entitled to.

/// The stamp a cached answer carries: the placement's member COUNT and its
/// committed ownership EPOCH. `None` means there is no placement at all — a
/// standalone broker, where `peers_all` is vacuously true and nothing can
/// invalidate it.
pub(super) type MeshFingerprint = Option<(usize, u64)>;

#[derive(Debug, Default)]
pub(super) struct MeshCache {
    whole: bool,
    settled: bool,
    /// The fingerprint `whole`/`settled` were computed against. `None` until the
    /// first recompute — and `None` never equals a live `Some(..)`, so a
    /// clustered hub reads conservatively until its first transition lands.
    stamped: MeshFingerprint,
}

impl MeshCache {
    /// Record a freshly computed pair against the view it was computed on.
    pub(super) fn store(&mut self, fp: MeshFingerprint, whole: bool, settled: bool) {
        self.stamped = fp;
        self.whole = whole;
        self.settled = settled;
    }

    /// Whether every membership-alive peer has a live link, as of a view that is
    /// still current. A moved membership answers `false`.
    pub(super) fn whole(&self, live: MeshFingerprint) -> bool {
        live.is_none() || (self.stamped == live && self.whole)
    }

    /// [`whole`](Self::whole), strengthened with "and has sent an interest
    /// snapshot". Same staleness rule, same failure direction.
    pub(super) fn settled(&self, live: MeshFingerprint) -> bool {
        live.is_none() || (self.stamped == live && self.settled)
    }
}

#[cfg(test)]
mod tests {
    use super::MeshCache;

    /// With no placement there is nothing to invalidate and nothing to cache:
    /// a standalone broker reads `true` from an empty cache, exactly as
    /// `peers_all`'s no-placement early return always answered. Without this the
    /// default (`false`) would silently make a standalone node's interest gossip
    /// non-authoritative forever.
    #[test]
    fn standalone_reads_true_from_an_untouched_cache() {
        let c = MeshCache::default();
        assert!(c.whole(None));
        assert!(c.settled(None));
    }

    /// The invariant: a cached "whole and settled" is refused the moment the
    /// placement view it was computed on is no longer the live one.
    #[test]
    fn a_moved_membership_invalidates_a_positive_answer() {
        let mut c = MeshCache::default();
        c.store(Some((3, 7)), true, true);
        assert!(c.whole(Some((3, 7))), "same view: the cache answers");
        assert!(c.settled(Some((3, 7))));
        assert!(!c.whole(Some((4, 7))), "a member joined: not whole");
        assert!(!c.settled(Some((4, 7))));
        assert!(!c.whole(Some((3, 8))), "ownership moved: not whole");
        assert!(!c.settled(Some((3, 8))));
    }

    /// A clustered hub that has never recomputed reads conservatively rather
    /// than reading the `bool` default.
    #[test]
    fn an_unstamped_cache_is_never_whole_on_a_cluster() {
        let c = MeshCache::default();
        assert!(!c.whole(Some((2, 0))));
        assert!(!c.settled(Some((2, 0))));
    }
}
