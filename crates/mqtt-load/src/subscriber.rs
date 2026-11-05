//! A consumer: reads deliveries, acks them at `QoS` 1, records end-to-end latency and
//! each delivery's identity. With `--reconnect` ([`Resume`]) it gets its connection
//! back when the broker closes it, and resumes its session.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use mqtt_codec::{packet::Publish, Packet, QoS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::client::{self, ClientError, ConnectOpts, VERSION};
use crate::clock::now_ns;
use crate::ctx::{close_label, Ctx};
use crate::payload::{Decoded, Header};

/// What one batch of deliveries did, before it is recorded.
#[derive(Debug, Default)]
struct Batch {
    /// The connection's previous `QoS` 1 packet id.
    last_pkid: Option<u16>,
    e2e: Vec<u64>,
    acks: Vec<u8>,
    received: u64,
    /// PUBACKs written in the last write: each took the batch's turnaround.
    acked: usize,
    turnaround_ns: u64,
}

/// The packet id a broker allocating sequentially uses after `last` (0 is not an id).
fn next_pkid(last: u16) -> u16 {
    last.checked_add(1).unwrap_or(1)
}

fn on_publish(ctx: &Ctx, stream: usize, p: &Publish, now: u64, batch: &mut Batch) {
    if let Some(pkid) = p.pkid {
        // A redelivery (DUP) after a resume carries the id it was first sent with, out
        // of the sequence: only first deliveries are checked for gaps.
        if !p.dup {
            if batch.last_pkid.is_some_and(|last| pkid != next_pkid(last)) {
                ctx.ledger.pkid_gaps.fetch_add(1, Ordering::Relaxed);
            }
            batch.last_pkid = Some(pkid);
        }
        batch.acks.extend_from_slice(&client::puback(pkid));
    }
    match Header::decode(&p.payload, ctx.run) {
        Decoded::Ours(h) => {
            ctx.ledger.receive(stream, h.publisher, h.seq);
            batch.e2e.push(now.saturating_sub(h.send_ns));
            batch.received += 1;
        }
        Decoded::OtherRun => {
            ctx.ledger.other_run.fetch_add(1, Ordering::Relaxed);
        }
        Decoded::Foreign => {
            ctx.ledger.foreign.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// What a consumer needs to get its connection back (`--reconnect`): the broker it
/// connected to, and the CONNECT and SUBSCRIBE it made there.
#[derive(Debug, Clone)]
pub struct Resume {
    /// The broker.
    pub addr: SocketAddr,
    /// The consumer's client id.
    pub client_id: String,
    /// A durable session (clean start off, a session expiry): resumed, not subscribed
    /// again, unless the broker no longer has it.
    pub durable: bool,
    /// Session expiry, seconds.
    pub session_expiry: u32,
    /// The subscription.
    pub filter: String,
    /// Its `QoS`.
    pub qos: QoS,
    /// Each handshake step's timeout.
    pub timeout: Duration,
    /// How long to keep trying, ns ([`crate::config::Config::reconnect_within_ns`]).
    pub within_ns: u64,
}

/// How one connection's read loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    /// The run stopped it.
    Stopped,
    /// The broker sent bytes that do not decode: its error, never reconnected.
    Protocol,
    /// The connection is gone: the broker closed it (with the reason of the DISCONNECT
    /// it sent first, if it sent one), or a write to it failed.
    Lost {
        reason: Option<u8>,
        server_disconnects: u64,
        write_failed: bool,
    },
}

/// Run a subscribed consumer until `stop` turns true; `early` are deliveries read
/// during the SUBSCRIBE exchange. With `resume`, a connection the broker closes is
/// connected again (see [`Resume`]); without it, its loss is the run's error.
#[allow(clippy::too_many_arguments)]
pub async fn consumer(
    ctx: Arc<Ctx>,
    stream: TcpStream,
    buf: BytesMut,
    early: Vec<Publish>,
    ledger_stream: usize,
    shard: usize,
    end_session: bool,
    mut stop: watch::Receiver<bool>,
    resume: Option<Resume>,
) {
    let (mut stream, mut buf, mut early) = (stream, buf, early);
    loop {
        let ended = serve(
            &ctx,
            stream,
            buf,
            &early,
            ledger_stream,
            shard,
            end_session,
            &mut stop,
        )
        .await;
        let Ended::Lost {
            reason,
            server_disconnects,
            write_failed,
        } = ended
        else {
            return;
        };
        let Some(r) = resume.as_ref() else {
            ctx.errors
                .server_disconnects
                .fetch_add(server_disconnects, Ordering::Relaxed);
            if write_failed {
                ctx.errors.write.fetch_add(1, Ordering::Relaxed);
            } else if !ctx.is_stopping() {
                ctx.errors.disconnects.fetch_add(1, Ordering::Relaxed);
            }
            return;
        };
        if ctx.is_stopping() {
            return;
        }
        ctx.reconnects.closed(close_label(reason, write_failed));
        match reconnect(&ctx, r, &mut stop, shard).await {
            Some((s, b, e)) => (stream, buf, early) = (s, b, e),
            None => return,
        }
    }
}

/// One connection's life: deliveries read, recorded and acked until the run stops it
/// or the connection is gone.
#[allow(clippy::too_many_arguments)]
async fn serve(
    ctx: &Ctx,
    stream: TcpStream,
    mut buf: BytesMut,
    early: &[Publish],
    ledger_stream: usize,
    shard: usize,
    end_session: bool,
    stop: &mut watch::Receiver<bool>,
) -> Ended {
    let (mut r, mut w) = stream.into_split();
    let mut batch = Batch::default();
    let (mut reason, mut server_disconnects) = (None, 0u64);
    let lost = |reason, server_disconnects, write_failed| Ended::Lost {
        reason,
        server_disconnects,
        write_failed,
    };
    let now = now_ns();
    for p in early {
        on_publish(ctx, ledger_stream, p, now, &mut batch);
    }
    let ping_every = Duration::from_millis(u64::from(ctx.cfg.keepalive.max(2)) * 500);
    let mut ping = tokio::time::interval(ping_every);
    ping.tick().await;
    loop {
        let now = now_ns();
        loop {
            match Packet::decode(&mut buf, VERSION) {
                Ok(Some(Packet::Publish(p))) => {
                    on_publish(ctx, ledger_stream, &p, now, &mut batch);
                }
                Ok(Some(Packet::PingResp)) => {}
                Ok(Some(Packet::Disconnect(d))) => {
                    reason = Some(d.reason);
                    server_disconnects += 1;
                }
                Ok(Some(_)) => {
                    ctx.errors.protocol.fetch_add(1, Ordering::Relaxed);
                }
                Ok(None) => break,
                Err(_) => {
                    ctx.errors.protocol.fetch_add(1, Ordering::Relaxed);
                    return Ended::Protocol;
                }
            }
        }
        if !batch.acks.is_empty() {
            if w.write_all(&batch.acks).await.is_err() {
                let said = said_before_close(&mut r, &mut buf, reason).await;
                return lost(said, server_disconnects + u64::from(said != reason), true);
            }
            // From the read that brought these deliveries to their PUBACKs written.
            batch.acked = batch.acks.len() / client::puback(1).len();
            batch.turnaround_ns = now_ns().saturating_sub(now);
            batch.acks.clear();
        }
        if batch.acked > 0 {
            let mut s = ctx.stats.shard(shard);
            for _ in 0..batch.acked {
                s.puback_turnaround.record_ns(batch.turnaround_ns);
            }
            drop(s);
            batch.acked = 0;
        }
        if batch.received > 0 {
            ctx.progress(now);
            // The stats shard is the consumer's own index.
            ctx.consumer_received[shard].fetch_add(batch.received, Ordering::Relaxed);
            let mut s = ctx.stats.shard(shard);
            s.received += batch.received;
            for &l in &batch.e2e {
                s.e2e.record_ns(l);
            }
            drop(s);
            batch.received = 0;
            batch.e2e.clear();
        }
        buf.reserve(64 * 1024);
        tokio::select! {
            read = r.read_buf(&mut buf) => {
                if !matches!(read, Ok(n) if n > 0) {
                    return lost(reason, server_disconnects, false);
                }
            }
            _ = ping.tick() => {
                if w.write_all(&client::PINGREQ).await.is_err() {
                    let said = said_before_close(&mut r, &mut buf, reason).await;
                    return lost(said, server_disconnects + u64::from(said != reason), true);
                }
            }
            changed = stop.changed() => {
                if stopped(changed.is_err(), stop) {
                    let _ = w.write_all(&client::disconnect_bytes(end_session)).await;
                    let _ = w.shutdown().await;
                    return Ended::Stopped;
                }
            }
        }
    }
}

/// After a failed write, the reason the broker gave before it closed: a consumer
/// writing its PUBACKs when the broker sent DISCONNECT and closed fails the write
/// before it reads the DISCONNECT, which may still wait in the socket. Read for at
/// most 100 ms; the deliveries found on the way are not recorded, as the broker,
/// which never got their PUBACKs, delivers them again on the resumed session.
async fn said_before_close(
    r: &mut OwnedReadHalf,
    buf: &mut BytesMut,
    reason: Option<u8>,
) -> Option<u8> {
    if reason.is_some() {
        return reason;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    loop {
        while let Ok(Some(p)) = Packet::decode(buf, VERSION) {
            if let Packet::Disconnect(d) = p {
                return Some(d.reason);
            }
        }
        buf.reserve(16 * 1024);
        match tokio::time::timeout_at(deadline, r.read_buf(buf)).await {
            Ok(Ok(n)) if n > 0 => {}
            _ => return None,
        }
    }
}

/// Whether `stop` says the run is over: its sender is gone (`sender_gone`), or it is true.
fn stopped(sender_gone: bool, stop: &mut watch::Receiver<bool>) -> bool {
    sender_gone || *stop.borrow_and_update()
}

/// Get the connection back: connect again with the same client id, backing off from
/// 20 ms to 1 s between tries, until the broker takes it (a durable session resumed,
/// or a clean one subscribed again), the run stops, or the reconnect budget runs out.
/// Every outcome is counted in [`Ctx::reconnects`]; giving up is also a disconnect.
///
/// # Panics
/// If another task panicked holding the reconnect gaps' lock.
async fn reconnect(
    ctx: &Ctx,
    r: &Resume,
    stop: &mut watch::Receiver<bool>,
    shard: usize,
) -> Option<(TcpStream, BytesMut, Vec<Publish>)> {
    let lost_at = now_ns();
    let give_up = lost_at.saturating_add(r.within_ns);
    let mut backoff = Duration::from_millis(20);
    loop {
        let c = &ctx.cfg;
        let opts = ConnectOpts {
            client_id: &r.client_id,
            clean: !r.durable,
            session_expiry: if r.durable { r.session_expiry } else { 0 },
            keepalive: c.keepalive,
            receive_maximum: u16::MAX,
            username: c.username.as_deref(),
            password: c.password.as_deref(),
        };
        let attempt = async {
            let mut conn = client::connect(r.addr, &opts, r.timeout).await?;
            let resumed = r.durable && conn.granted.session_present;
            let early = if resumed {
                Vec::new()
            } else {
                client::subscribe(&mut conn, &r.filter, r.qos, r.timeout).await?
            };
            Ok::<_, ClientError>((conn, resumed, early))
        };
        tokio::select! {
            res = attempt => {
                if let Ok((conn, resumed, early)) = res {
                    if r.durable && !resumed {
                        ctx.reconnects.sessions_lost.fetch_add(1, Ordering::Relaxed);
                    }
                    ctx.reconnects.reconnects.fetch_add(1, Ordering::Relaxed);
                    ctx.reconnects
                        .gaps
                        .lock()
                        .expect("reconnect gaps poisoned")
                        .record_ns(now_ns().saturating_sub(lost_at));
                    ctx.stats.shard(shard).reconnects += 1;
                    return Some((conn.stream, conn.buf, early));
                }
            }
            changed = stop.changed() => {
                if stopped(changed.is_err(), stop) {
                    return None;
                }
            }
        }
        if ctx.is_stopping() {
            return None;
        }
        if now_ns() >= give_up {
            ctx.reconnects.failed.fetch_add(1, Ordering::Relaxed);
            ctx.errors.disconnects.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        tokio::select! {
            () = tokio::time::sleep(backoff) => {}
            changed = stop.changed() => {
                if stopped(changed.is_err(), stop) {
                    return None;
                }
            }
        }
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_ids_follow_in_order_and_skip_zero() {
        assert_eq!(next_pkid(1), 2);
        assert_eq!(next_pkid(1024), 1025);
        assert_eq!(next_pkid(u16::MAX), 1);
    }
}
