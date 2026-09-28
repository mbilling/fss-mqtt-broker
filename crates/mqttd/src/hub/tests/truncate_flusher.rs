//! The truncate flusher's queue is fair (0078 T4 calibration, 2026-09-28): a
//! session that keeps acknowledging must not keep every other session's
//! watermark from reaching the store. The old picker took the next session from
//! a `HashMap`'s iteration order, which is fixed per map, so the sessions early
//! in it won every free slot while they stayed dirty; the rest were never
//! flushed, their logs reached the queue cap, and every later append evicted an
//! already-delivered entry as a `queue-overflow` drop.

use super::super::FlushQueue;
use mqtt_core::ClientId;
use std::collections::HashSet;

fn cid(i: usize) -> ClientId {
    ClientId(format!("c{i}").into())
}

/// Forty sessions that ack between every flush round, eight slots, each round
/// long enough for every flush to finish: every session is flushed within
/// `ceil(40 / 8) = 5` rounds, and no session twice before all have had one.
#[test]
fn every_busy_session_is_flushed_in_turn() {
    const SESSIONS: usize = 40;
    const SLOTS: usize = 8;
    let mut q = FlushQueue::default();
    let mut offset = 0;
    let mut flushed = HashSet::new();
    for _round in 0..SESSIONS.div_ceil(SLOTS) {
        offset += 1;
        for i in 0..SESSIONS {
            q.mark(cid(i), offset);
        }
        let mut running = Vec::new();
        while running.len() < SLOTS {
            let Some((client, _)) = q.next() else { break };
            assert!(
                flushed.insert(client.clone()),
                "{client:?} flushed again before every session had a turn"
            );
            running.push(client);
        }
        for client in running {
            q.finished(client);
        }
    }
    assert_eq!(flushed.len(), SESSIONS, "every session reached the store");
}

/// A burst coalesces to its highest watermark, and a lower late one never
/// lowers it.
#[test]
fn a_burst_flushes_once_at_its_highest_watermark() {
    let mut q = FlushQueue::default();
    q.mark(cid(1), 5);
    q.mark(cid(1), 9);
    q.mark(cid(1), 7);
    assert_eq!(q.next(), Some((cid(1), 9)));
    assert_eq!(q.next(), None, "one flush for the whole burst");
}

/// A session acking while its flush runs is not flushed twice at once: it
/// waits for the running flush, then rejoins BEHIND the sessions already
/// waiting, with the newer watermark.
#[test]
fn a_session_in_flight_rejoins_behind_the_waiting_ones() {
    let mut q = FlushQueue::default();
    q.mark(cid(1), 1);
    assert_eq!(q.next(), Some((cid(1), 1)));
    q.mark(cid(1), 2); // acked again while in flight
    q.mark(cid(2), 1);
    assert_eq!(q.next(), Some((cid(2), 1)), "c1 is in flight, so c2 goes");
    assert_eq!(q.next(), None, "c1 must not start a second flush");
    q.finished(cid(1));
    q.finished(cid(2));
    assert_eq!(q.next(), Some((cid(1), 2)), "c1 rejoined with its newer watermark");
    assert_eq!(q.next(), None);
}

/// A session whose flush finished with nothing new owed leaves the queue.
#[test]
fn a_clean_session_leaves_the_queue() {
    let mut q = FlushQueue::default();
    q.mark(cid(1), 3);
    let (client, _) = q.next().unwrap();
    q.finished(client);
    assert_eq!(q.next(), None);
}
