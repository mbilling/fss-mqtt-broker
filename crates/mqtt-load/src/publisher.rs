//! A paced, open-loop publisher with an in-flight window (ADR 0090 D2).
//!
//! Two tasks per connection. The **writer** follows the schedule: whenever messages
//! are due it writes them — as many as are due, up to the free send credit — stamping
//! each with the time it is written. The **reader** reads PUBACKs, frees their packet
//! ids, and records ack latency. The writer never waits for an ack to decide *when* a
//! message is due, only *whether* it may be written yet, so a slow broker shows up as
//! ack latency, as credit stall, and as a backlog of due-but-unsent messages — never as
//! a lower offer.
//!
//! Every send's delay from its due time is recorded. A send that nothing outside the
//! generator held back (no credit wait, no full socket buffer, since it fell due) is
//! the generator's own lateness — timer, scheduling and CPU — and goes into the
//! scheduling-lag histogram that judges generator saturation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use mqtt_codec::Packet;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Notify;

use crate::client::{self, VERSION};
use crate::clock::{now_ns, sleep_until_ns};
use crate::ctx::Ctx;
use crate::payload::{encode_publish, Header};
use crate::schedule::Cursor;

/// Most messages written in one batch (one `write`).
const MAX_BATCH: usize = 64;

#[derive(Debug, Clone, Copy, Default)]
struct Slot {
    /// The ledger publisher the message is from: the connection itself, or one of its
    /// topics' logical publishers when the connection is a bridge.
    publisher: u32,
    seq: u64,
    send_ns: u64,
    used: bool,
}

/// Packet ids in flight.
#[derive(Debug)]
pub struct Inflight {
    slots: Vec<Slot>,
    free: Vec<u16>,
    window: u16,
}

impl Inflight {
    /// A window of `window` packet ids (1..=window).
    #[must_use]
    pub fn new(window: u16) -> Self {
        Self {
            slots: vec![Slot::default(); usize::from(window) + 1],
            free: (1..=window).rev().collect(),
            window,
        }
    }

    /// Free packet ids.
    #[must_use]
    pub fn free(&self) -> usize {
        self.free.len()
    }

    /// Packet ids in use.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        usize::from(self.window) - self.free.len()
    }

    /// Take a packet id for ledger publisher `publisher`'s `seq`, sent at `send_ns`.
    pub fn take(&mut self, publisher: u32, seq: u64, send_ns: u64) -> Option<u16> {
        let pkid = self.free.pop()?;
        self.slots[usize::from(pkid)] = Slot {
            publisher,
            seq,
            send_ns,
            used: true,
        };
        Some(pkid)
    }

    /// Release `pkid`: its (publisher, seq, `send_ns`), or `None` if nothing was in
    /// flight on it.
    pub fn release(&mut self, pkid: u16) -> Option<(u32, u64, u64)> {
        let slot = self.slots.get_mut(usize::from(pkid))?;
        if !slot.used {
            return None;
        }
        slot.used = false;
        self.free.push(pkid);
        Some((slot.publisher, slot.seq, slot.send_ns))
    }
}

/// What a publisher's two tasks share.
#[derive(Debug)]
pub struct PubShared {
    /// Packet ids in flight.
    pub inflight: Mutex<Inflight>,
    /// Signalled when credit frees up (or the connection closes).
    pub credit: Notify,
    /// The connection is gone.
    pub closed: AtomicBool,
    /// The writer has written everything its schedule offered (it still pings).
    schedule_done: AtomicBool,
    /// The run is ending: the writer stops pinging and returns its write half.
    stop: AtomicBool,
}

impl PubShared {
    /// A publisher with a `window`-message in-flight window.
    #[must_use]
    pub fn new(window: u16) -> Self {
        Self {
            inflight: Mutex::new(Inflight::new(window)),
            credit: Notify::new(),
            closed: AtomicBool::new(false),
            schedule_done: AtomicBool::new(false),
            stop: AtomicBool::new(false),
        }
    }

    /// Whether the writer has written all its schedule offered (or its connection is
    /// gone): the drain's "nothing more will be sent".
    #[must_use]
    pub fn schedule_done(&self) -> bool {
        self.schedule_done.load(Ordering::Relaxed)
    }

    /// End the writer's keep-alive pings: it returns its write half.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.credit.notify_one();
    }

    /// Messages in flight.
    ///
    /// # Panics
    /// If a task panicked holding the lock.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.inflight
            .lock()
            .expect("inflight poisoned")
            .outstanding()
    }

    fn free(&self) -> usize {
        self.inflight.lock().expect("inflight poisoned").free()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.credit.notify_one();
    }

    /// Whether the connection is gone.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }
}

/// Write all of `buf`; returns (times the socket was full, ns spent waiting on it).
async fn write_tracked(w: &mut OwnedWriteHalf, mut buf: &[u8]) -> std::io::Result<(u64, u64)> {
    let (mut blocked, mut blocked_ns) = (0, 0);
    while !buf.is_empty() {
        match w.try_write(buf) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(n) => buf = &buf[n..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                blocked += 1;
                let t = now_ns();
                w.writable().await?;
                blocked_ns += now_ns() - t;
            }
            Err(e) => return Err(e),
        }
    }
    Ok((blocked, blocked_ns))
}

/// The writer: follows `cursor` until the schedule ends or the connection closes,
/// then keeps the connection alive with PINGREQs until [`PubShared::stop`], and returns
/// the write half (for the closing DISCONNECT). Without the pings a publisher is silent
/// through the drain, and a broker closes it after 1.5 x the keep-alive: on the
/// 10-node run of 2026-10-27 a 106 s drain lost all 5,200 publishers of one generator.
///
/// # Panics
/// If another task panicked holding the in-flight lock.
#[allow(clippy::too_many_lines)]
pub async fn writer(
    ctx: Arc<Ctx>,
    id: u32,
    state: Arc<PubShared>,
    mut w: OwnedWriteHalf,
    mut cursor: Cursor,
    shard: usize,
) -> OwnedWriteHalf {
    let cfg = &ctx.cfg;
    // The connection's lanes: (topic index, topic name, ledger publisher). One, on
    // topic `id % topics`; as a bridge, one per topic, its messages dealt round-robin.
    let lanes: Vec<(usize, Vec<u8>, u32)> = if cfg.bridge {
        (0..cfg.topics)
            .map(|t| (t, cfg.topic(t).into_bytes(), cfg.logical(id, t)))
            .collect()
    } else {
        let t = id as usize % cfg.topics;
        vec![(t, cfg.topic(t).into_bytes(), id)]
    };
    let mut seqs = vec![0u64; lanes.len()];
    let mut batch_sent = vec![0u64; lanes.len()];
    let mut lane = 0usize;
    let longest_topic = lanes.iter().map(|(_, t, _)| t.len()).max().unwrap_or(0);
    let qos1 = cfg.qos == 1;
    let ka_ns = u64::from(cfg.keepalive) * 500_000_000; // ping at half the keep-alive
    let mut buf = Vec::with_capacity(MAX_BATCH * (cfg.payload + longest_topic + 8));
    let mut delays: Vec<(u64, bool)> = Vec::with_capacity(MAX_BATCH);
    let mut next = cursor.next();
    // Messages due before this instant were held back by something outside the
    // generator (credit or the socket).
    let mut held_until = 0u64;
    let mut last_write = now_ns();

    while let Some(due) = next {
        if state.is_closed() {
            break;
        }
        let now = now_ns();
        if due > now {
            if ka_ns > 0 && now >= last_write + ka_ns {
                if write_tracked(&mut w, &client::PINGREQ).await.is_err() {
                    break;
                }
                last_write = now_ns();
                continue;
            }
            let wake = if ka_ns > 0 {
                due.min(last_write + ka_ns)
            } else {
                due
            };
            sleep_until_ns(wake).await;
            continue;
        }
        let credit = if qos1 { state.free() } else { MAX_BATCH };
        if credit == 0 {
            let t0 = now_ns();
            // Wait for an ack to free a packet id. Bounded by the keep-alive, so a
            // long stall still pings the broker.
            let wait = if ka_ns > 0 { ka_ns } else { 1_000_000_000 };
            let _ = tokio::time::timeout(Duration::from_nanos(wait), state.credit.notified()).await;
            let t1 = now_ns();
            held_until = t1;
            ctx.stats.shard(shard).credit_stall_ns += t1 - t0;
            if state.free() == 0 && t1 >= last_write + ka_ns && ka_ns > 0 {
                if write_tracked(&mut w, &client::PINGREQ).await.is_err() {
                    break;
                }
                last_write = now_ns();
            }
            continue;
        }
        buf.clear();
        delays.clear();
        let stamp = now_ns();
        {
            let mut inflight = qos1.then(|| state.inflight.lock().expect("inflight poisoned"));
            while let Some(d) = next {
                if d > stamp || delays.len() >= credit.min(MAX_BATCH) {
                    break;
                }
                let (_, topic, publisher) = &lanes[lane];
                let seq = seqs[lane];
                let pkid = match inflight.as_mut() {
                    Some(inf) => inf.take(*publisher, seq, stamp),
                    None => None,
                };
                let _ = ctx.ledger.publishers[*publisher as usize].sent.set(seq);
                let header = Header {
                    run: ctx.run,
                    publisher: *publisher,
                    seq,
                    send_ns: stamp,
                };
                encode_publish(
                    &mut buf,
                    topic,
                    cfg.qos,
                    pkid,
                    true,
                    &header,
                    cfg.payload,
                    &ctx.filler,
                )
                .expect("a validated workload encodes");
                delays.push((stamp - d, d < held_until));
                seqs[lane] += 1;
                batch_sent[lane] += 1;
                lane = (lane + 1) % lanes.len();
                next = cursor.next();
            }
        }
        let n = delays.len() as u64;
        let written = write_tracked(&mut w, &buf).await;
        for (k, sent) in batch_sent.iter_mut().enumerate() {
            if *sent > 0 {
                ctx.topic_sent[lanes[k].0].fetch_add(*sent, Ordering::Relaxed);
                *sent = 0;
            }
        }
        let Ok((blocked, blocked_ns)) = written else {
            if !ctx.publishers_stopping() {
                ctx.errors.write.fetch_add(1, Ordering::Relaxed);
            }
            state.close();
            break;
        };
        last_write = now_ns();
        if blocked > 0 {
            held_until = last_write;
        }
        let mut s = ctx.stats.shard(shard);
        s.sent += n;
        s.socket_blocked += blocked;
        s.socket_blocked_ns += blocked_ns;
        for &(d, held) in &delays {
            s.send_delay.record_ns(d);
            if !held {
                s.sched_lag.record_ns(d);
            }
        }
    }
    state.schedule_done.store(true, Ordering::Relaxed);
    // The drain: nothing more to send, but the connection must outlive it.
    while !state.is_closed() && !state.stop.load(Ordering::Relaxed) {
        let now = now_ns();
        if ka_ns > 0 && now >= last_write + ka_ns {
            if write_tracked(&mut w, &client::PINGREQ).await.is_err() {
                break;
            }
            last_write = now_ns();
            continue;
        }
        // Woken by stop() (or an ack: the loop checks again).
        let wait = if ka_ns > 0 {
            last_write + ka_ns - now
        } else {
            1_000_000_000
        };
        let _ = tokio::time::timeout(Duration::from_nanos(wait), state.credit.notified()).await;
    }
    w
}

/// The reader: PUBACKs until the connection closes.
///
/// # Panics
/// If another task panicked holding the in-flight lock.
pub async fn reader(
    ctx: Arc<Ctx>,
    state: Arc<PubShared>,
    mut r: OwnedReadHalf,
    mut buf: BytesMut,
    shard: usize,
) {
    let mut acks: Vec<(u16, u8)> = Vec::with_capacity(256);
    let mut lat: Vec<u64> = Vec::with_capacity(256);
    loop {
        loop {
            match Packet::decode(&mut buf, VERSION) {
                Ok(Some(Packet::PubAck(a))) => acks.push((a.pkid, a.reason)),
                Ok(Some(Packet::PingResp)) => {}
                Ok(Some(Packet::Disconnect(_))) => {
                    ctx.errors
                        .server_disconnects
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(Some(_)) => {
                    ctx.errors.protocol.fetch_add(1, Ordering::Relaxed);
                }
                Ok(None) => break,
                Err(_) => {
                    ctx.errors.protocol.fetch_add(1, Ordering::Relaxed);
                    state.close();
                    return;
                }
            }
        }
        if !acks.is_empty() {
            let now = now_ns();
            {
                let mut inf = state.inflight.lock().expect("inflight poisoned");
                for &(pkid, reason) in &acks {
                    match inf.release(pkid) {
                        Some((publisher, seq, sent)) if reason < 0x80 => {
                            let _ = ctx.ledger.publishers[publisher as usize].acked.set(seq);
                            lat.push(now.saturating_sub(sent));
                        }
                        Some(_) => {
                            ctx.ledger.negative_acks.fetch_add(1, Ordering::Relaxed);
                        }
                        None => {
                            ctx.ledger.unexpected_acks.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            state.credit.notify_one();
            ctx.progress(now);
            let mut s = ctx.stats.shard(shard);
            s.acked += lat.len() as u64;
            for &l in &lat {
                s.ack.record_ns(l);
            }
            drop(s);
            acks.clear();
            lat.clear();
        }
        buf.reserve(16 * 1024);
        match r.read_buf(&mut buf).await {
            Ok(n) if n > 0 => {}
            _ => {
                if !ctx.publishers_stopping() {
                    ctx.errors.disconnects.fetch_add(1, Ordering::Relaxed);
                }
                state.close();
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inflight_window_hands_out_and_takes_back_packet_ids() {
        let mut inf = Inflight::new(3);
        let a = inf.take(0, 10, 100).unwrap();
        let b = inf.take(4, 11, 101).unwrap();
        let c = inf.take(0, 12, 102).unwrap();
        assert_eq!(inf.take(0, 13, 103), None, "window full");
        assert_eq!(inf.outstanding(), 3);
        let mut ids = [a, b, c];
        ids.sort_unstable();
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(
            inf.release(b),
            Some((4, 11, 101)),
            "the slot gives back the ledger publisher it was taken for"
        );
        assert_eq!(
            inf.release(b),
            None,
            "a second ack for the same id is unexpected"
        );
        assert_eq!(inf.release(0), None);
        assert_eq!(inf.release(999), None);
        assert_eq!(inf.free(), 1);
        assert_eq!(inf.take(0, 13, 103), Some(b));
    }
}
