//! A consumer: reads deliveries, acks them at `QoS` 1, records end-to-end latency and
//! each delivery's identity.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use mqtt_codec::{packet::Publish, Packet};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::client::{self, VERSION};
use crate::clock::now_ns;
use crate::ctx::Ctx;
use crate::payload::{Decoded, Header};

/// What one batch of deliveries did, before it is recorded.
#[derive(Debug, Default)]
struct Batch {
    e2e: Vec<u64>,
    acks: Vec<u8>,
    received: u64,
}

fn on_publish(ctx: &Ctx, stream: usize, p: &Publish, now: u64, batch: &mut Batch) {
    if let Some(pkid) = p.pkid {
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

/// Run a subscribed consumer until `stop` turns true; `early` are deliveries read
/// during the SUBSCRIBE exchange.
#[allow(clippy::too_many_arguments)]
pub async fn consumer(
    ctx: Arc<Ctx>,
    stream: TcpStream,
    mut buf: BytesMut,
    early: Vec<Publish>,
    ledger_stream: usize,
    shard: usize,
    end_session: bool,
    mut stop: watch::Receiver<bool>,
) {
    let (mut r, mut w) = stream.into_split();
    let mut batch = Batch::default();
    let now = now_ns();
    for p in &early {
        on_publish(&ctx, ledger_stream, p, now, &mut batch);
    }
    let ping_every = Duration::from_millis(u64::from(ctx.cfg.keepalive.max(2)) * 500);
    let mut ping = tokio::time::interval(ping_every);
    ping.tick().await;
    loop {
        let now = now_ns();
        loop {
            match Packet::decode(&mut buf, VERSION) {
                Ok(Some(Packet::Publish(p))) => {
                    on_publish(&ctx, ledger_stream, &p, now, &mut batch)
                }
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
                    return;
                }
            }
        }
        if !batch.acks.is_empty() {
            if w.write_all(&batch.acks).await.is_err() {
                ctx.errors.write.fetch_add(1, Ordering::Relaxed);
                return;
            }
            batch.acks.clear();
        }
        if batch.received > 0 {
            ctx.progress(now);
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
                    if !ctx.is_stopping() {
                        ctx.errors.disconnects.fetch_add(1, Ordering::Relaxed);
                    }
                    return;
                }
            }
            _ = ping.tick() => {
                if w.write_all(&client::PINGREQ).await.is_err() {
                    ctx.errors.write.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
            changed = stop.changed() => {
                let stopping = changed.is_err() || *stop.borrow_and_update();
                if stopping {
                    let _ = w.write_all(&client::disconnect_bytes(end_session)).await;
                    let _ = w.shutdown().await;
                    return;
                }
            }
        }
    }
}
