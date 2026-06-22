//! Gossip anti-replay primitives ([ADR 0023](../../../docs/adr/0023-gossip-anti-replay.md)):
//! a per-sender sliding replay window and a per-node monotonic sequence allocator made
//! restart-safe by block-reserved persistence. Both are pure (no I/O, no clock); the file
//! persistence and the wire/driver wiring build on them in later phases.

/// A sliding replay window over 64-bit sequence numbers (RFC 6479 style). Tracks the highest
/// sequence accepted from one sender and a 64-entry bitmap of recently-seen sequences, so a
/// replayed or duplicated sequence is rejected while out-of-order delivery within the window
/// is tolerated.
#[derive(Debug, Default)]
pub struct ReplayWindow {
    /// Highest sequence accepted so far (meaningful once `seeded`).
    high: u64,
    /// Bit `i` set ⇒ sequence `high - i` has been seen. Bit 0 is `high` itself.
    bitmap: u64,
    /// Whether any sequence has been accepted yet (the first seeds the window).
    seeded: bool,
}

/// The window covers the highest sequence and the 63 below it; anything older is rejected.
const WINDOW: u64 = 64;

impl ReplayWindow {
    /// A fresh window that has seen nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `seq` if it is fresh, returning `true`; return `false` (without recording) if it
    /// is a replay — a duplicate within the window, or at/below the window's low edge. The
    /// first sequence ever seen seeds the window and is always accepted.
    pub fn check_and_set(&mut self, seq: u64) -> bool {
        if !self.seeded {
            self.seeded = true;
            self.high = seq;
            self.bitmap = 1; // bit 0 = high
            return true;
        }
        if seq > self.high {
            // New high: shift the window up by the gap and mark the new high (bit 0).
            let shift = seq - self.high;
            self.bitmap = if shift >= WINDOW {
                1
            } else {
                (self.bitmap << shift) | 1
            };
            self.high = seq;
            return true;
        }
        // seq <= high: within the window iff the gap is < WINDOW.
        let offset = self.high - seq;
        if offset >= WINDOW {
            return false; // too old — below the window
        }
        let mask = 1u64 << offset;
        if self.bitmap & mask != 0 {
            return false; // already seen
        }
        self.bitmap |= mask;
        true
    }
}

/// Persistence for the sequence high-water mark. The allocator stores the exclusive upper
/// bound of the block it has reserved; on reopen it resumes from there, so a sequence is
/// never reused across restarts. One [`persist`](SeqStore::persist) is a durable (fsync'd)
/// write in the real implementation.
pub trait SeqStore: Send {
    /// The last persisted reserved-upper-bound, or `0` if none has ever been written.
    fn reserved(&self) -> u64;
    /// Durably record a new reserved-upper-bound.
    fn persist(&mut self, reserved_until: u64);
}

/// Forwarding impl so the driver can hold a `SequenceAllocator<Box<dyn SeqStore>>` (the
/// concrete file store is injected by the broker).
impl SeqStore for Box<dyn SeqStore> {
    fn reserved(&self) -> u64 {
        (**self).reserved()
    }
    fn persist(&mut self, reserved_until: u64) {
        (**self).persist(reserved_until);
    }
}

/// Hands out strictly increasing 64-bit sequence numbers, persisting in blocks so that across
/// a process restart it resumes **above** the last reserved block — never reissuing a number.
/// Monotonicity comes from persistence, not from any clock (ADR 0023).
#[derive(Debug)]
pub struct SequenceAllocator<S: SeqStore> {
    next: u64,
    reserved_until: u64,
    block: u64,
    store: S,
}

impl<S: SeqStore> SequenceAllocator<S> {
    /// Open over `store`, reserving `block` numbers at a time. Resumes from the last reserved
    /// upper bound, so the first number handed out is never one a previous run could have used.
    ///
    /// # Panics
    /// Panics if `block` is zero (a reservation must make progress).
    pub fn open(store: S, block: u64) -> Self {
        assert!(block > 0, "reservation block must be non-zero");
        let resume = store.reserved();
        Self {
            next: resume,
            reserved_until: resume,
            block,
            store,
        }
    }

    /// The next sequence number, reserving (and persisting) a new block when the current one
    /// is exhausted — so the persisted bound always stays ahead of every number handed out.
    pub fn allocate(&mut self) -> u64 {
        if self.next >= self.reserved_until {
            self.reserved_until = self.next + self.block;
            self.store.persist(self.reserved_until);
        }
        let seq = self.next;
        self.next += 1;
        seq
    }
}

#[cfg(test)]
mod tests {
    use super::{ReplayWindow, SeqStore, SequenceAllocator, WINDOW};

    // --- P1: sliding replay window ---

    #[test]
    fn strictly_increasing_sequences_are_all_fresh() {
        let mut w = ReplayWindow::new();
        for seq in 1..1000 {
            assert!(w.check_and_set(seq), "seq {seq} should be fresh");
        }
    }

    #[test]
    fn an_exact_duplicate_is_rejected() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(10));
        assert!(!w.check_and_set(10), "replay of the current high rejected");
        assert!(w.check_and_set(11));
        assert!(!w.check_and_set(11));
    }

    #[test]
    fn out_of_order_within_the_window_is_accepted_once_then_rejected() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(100)); // high = 100
        assert!(w.check_and_set(98), "98 is within the window, unseen");
        assert!(!w.check_and_set(98), "98 again is a replay");
        assert!(w.check_and_set(99));
        assert!(!w.check_and_set(100), "100 was the seed, now a replay");
    }

    #[test]
    fn a_sequence_below_the_window_is_rejected() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(1000));
        // 1000 - WINDOW is exactly at/below the low edge → rejected.
        assert!(!w.check_and_set(1000 - WINDOW));
        assert!(!w.check_and_set(1));
    }

    #[test]
    fn a_large_forward_gap_slides_the_window_and_accepts() {
        let mut w = ReplayWindow::new();
        assert!(w.check_and_set(5));
        assert!(w.check_and_set(5 + 10_000), "a big jump forward is fresh");
        // Everything from the old window is now far below → rejected.
        assert!(!w.check_and_set(5));
        assert!(!w.check_and_set(6));
        // ...and the new high is a replay.
        assert!(!w.check_and_set(5 + 10_000));
    }

    // --- P2: persisted monotonic sequence allocator ---

    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use std::sync::Arc;

    /// An in-memory `SeqStore` (Send, since `SeqStore: Send`) that records every persist, so a
    /// test can simulate a restart by reopening over the same backing value and assert the
    /// fsync (persist) frequency.
    #[derive(Default, Clone)]
    struct MemStore {
        reserved: Arc<AtomicU64>,
        persists: Arc<AtomicU32>,
    }
    impl SeqStore for MemStore {
        fn reserved(&self) -> u64 {
            self.reserved.load(Ordering::Relaxed)
        }
        fn persist(&mut self, reserved_until: u64) {
            self.reserved.store(reserved_until, Ordering::Relaxed);
            self.persists.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn allocates_strictly_increasing_from_zero() {
        let mut a = SequenceAllocator::open(MemStore::default(), 64);
        let seqs: Vec<u64> = (0..5).map(|_| a.allocate()).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn reserves_one_block_per_block_of_numbers() {
        let store = MemStore::default();
        let persists = store.persists.clone();
        let mut a = SequenceAllocator::open(store, 4);
        for _ in 0..8 {
            a.allocate();
        }
        // 8 numbers, block of 4 → exactly two reservations (fsyncs).
        assert_eq!(persists.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn reopening_resumes_above_the_last_reserved_block_never_reusing() {
        let store = MemStore::default();
        let mut a = SequenceAllocator::open(store.clone(), 64);
        let used: Vec<u64> = (0..3).map(|_| a.allocate()).collect();
        assert_eq!(used, vec![0, 1, 2]); // reserved up to 64
        drop(a); // "restart"

        let mut b = SequenceAllocator::open(store, 64);
        let after = b.allocate();
        assert_eq!(
            after, 64,
            "resumes above the reserved block, skipping the unused tail"
        );
        assert!(
            after > *used.iter().max().unwrap(),
            "never reissues a number a previous run reserved"
        );
    }
}
