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

    /// Take a packet id for `seq`, sent at `send_ns`.
    pub fn take(&mut self, seq: u64, send_ns: u64) -> Option<u16> {
        let pkid = self.free.pop()?;
        self.slots[usize::from(pkid)] = Slot {
            seq,
            send_ns,
            used: true,
        };
        Some(pkid)
    }

    /// Release `pkid`: its (seq, `send_ns`), or `None` if nothing was in flight on it.
    pub fn release(&mut self, pkid: u16) -> Option<(u64, u64)> {
        let slot = self.slots.get_mut(usize::from(pkid))?;
        if !slot.used {
            return None;
        }
        slot.used = false;
        self.free.push(pkid);
        Some((slot.seq, slot.send_ns))
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
}

impl PubShared {
    /// A publisher with a `window`-message in-flight window.
    #[must_use]
    pub fn new(window: u16) -> Self {
        Self {
            inflight: Mutex::new(Inflight::new(window)),
            credit: Notify::new(),
            closed: AtomicBool::new(false),
        }
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

/// The writer: follows `cursor` until the schedule ends or the connection closes, and
/// returns the write half (for the closing DISCONNECT).
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
    let topic_idx = id as usize % cfg.topics;
    let topic = cfg.topic(topic_idx).into_bytes();
    let qos1 = cfg.qos == 1;
    let ledger = &ctx.ledger.publishers[id as usize];
    let ka_ns = u64::from(cfg.keepalive) * 500_000_000; // ping at half the keep-alive
    let mut buf = Vec::with_capacity(MAX_BATCH * (cfg.payload + topic.len() + 8));
    let mut delays: Vec<(u64, bool)> = Vec::with_capacity(MAX_BATCH);
    let mut next = cursor.next();
    let mut seq = 0u64;
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
                let pkid = match inflight.as_mut() {
                    Some(inf) => inf.take(seq, stamp),
                    None => None,
                };
                let _ = ledger.sent.set(seq);
                let header = Header {
                    run: ctx.run,
                    publisher: id,
                    seq,
                    send_ns: stamp,
                };
                encode_publish(
                    &mut buf,
                    &topic,
                    cfg.qos,
                    pkid,
                    true,
                    &header,
                    cfg.payload,
                    &ctx.filler,
                )
                .expect("a validated workload encodes");
                delays.push((stamp - d, d < held_until));
                seq += 1;
                next = cursor.next();
            }
        }
        let n = delays.len() as u64;
        let written = write_tracked(&mut w, &buf).await;
        ctx.topic_sent[topic_idx].fetch_add(n, Ordering::Relaxed);
        let Ok((blocked, blocked_ns)) = written else {
            if !ctx.is_stopping() {
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
    w
}

/// The reader: PUBACKs until the connection closes.
///
/// # Panics
/// If another task panicked holding the in-flight lock.
pub async fn reader(
    ctx: Arc<Ctx>,
    id: u32,
    state: Arc<PubShared>,
    mut r: OwnedReadHalf,
    mut buf: BytesMut,
    shard: usize,
) {
    let ledger = &ctx.ledger.publishers[id as usize];
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
                        Some((seq, sent)) if reason < 0x80 => {
                            let _ = ledger.acked.set(seq);
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
                if !ctx.is_stopping() {
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
        let a = inf.take(10, 100).unwrap();
        let b = inf.take(11, 101).unwrap();
        let c = inf.take(12, 102).unwrap();
        assert_eq!(inf.take(13, 103), None, "window full");
        assert_eq!(inf.outstanding(), 3);
        let mut ids = [a, b, c];
        ids.sort_unstable();
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(inf.release(b), Some((11, 101)));
        assert_eq!(
            inf.release(b),
            None,
            "a second ack for the same id is unexpected"
        );
        assert_eq!(inf.release(0), None);
        assert_eq!(inf.release(999), None);
        assert_eq!(inf.free(), 1);
        assert_eq!(inf.take(13, 103), Some(b));
    }
}
