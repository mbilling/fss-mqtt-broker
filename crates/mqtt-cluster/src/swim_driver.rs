//! Async UDP driver for the [`crate::swim`] state machine.
//!
//! This is the thin I/O shell around the pure protocol: it owns a UDP socket and
//! a tick timer, feeds `tick(now)` and `handle(msg, now)` into the [`Swim`] state
//! machine, serializes outgoing datagrams, and republishes membership changes on
//! an event channel for the routing layer to consume.
//!
//! With a [`SwimAuth`] key, every outgoing datagram is sealed and every incoming
//! one is verified **before** deserialization (ADR 0003) — unauthenticated bytes
//! never reach the protocol state machine.

use crate::replay::{ReplayWindow, SeqStore, SequenceAllocator};
use crate::swim::{Action, MemberState, Message, Swim};
use crate::swim_auth::{Opened, SwimAuth};
use crate::NodeId;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, trace};

/// The per-node monotonic sequence source for outgoing v3 datagrams (ADR 0023). The driver
/// holds it behind a boxed [`SeqStore`] so the broker can inject the file-backed persistence.
pub type SeqAlloc = SequenceAllocator<Box<dyn SeqStore>>;

/// Maximum SWIM datagram size we are willing to receive.
const RECV_BUF: usize = 64 * 1024;

/// A membership change observed by SWIM, emitted to the routing layer.
#[derive(Debug, Clone)]
pub struct MembershipEvent {
    /// The member whose state changed.
    pub id: NodeId,
    /// Its SWIM datagram address.
    pub addr: String,
    /// Its inter-node routing (TCP peer-link) address; empty if not yet learned.
    pub peer_addr: String,
    /// Its new state.
    pub state: MemberState,
}

/// Drive SWIM over `socket` until `shutdown` resolves.
///
/// `tick_interval` should be no larger than the configured ack timeout so probe
/// deadlines are observed promptly. Membership changes are sent on `events`.
///
/// With `auth`, datagrams failing authentication are dropped before decode
/// (logged at `debug` — per-packet `warn` would be a log-flooding lever; a
/// rejected-datagram counter is planned observability work). `None` runs the
/// gossip plane unauthenticated, which the caller must opt into loudly.
///
/// When `shutdown` resolves, the node announces a **graceful leave** (ADR 0019 §2):
/// it gossips itself `Dead` directly to its peers so they drop it from the ring at
/// once, then the driver returns. Pass `std::future::pending()` for a driver that
/// only stops by being aborted.
pub async fn run(
    socket: UdpSocket,
    mut swim: Swim,
    tick_interval: Duration,
    events: mpsc::UnboundedSender<MembershipEvent>,
    auth: Option<SwimAuth>,
    mut seq_alloc: Option<SeqAlloc>,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let start = Instant::now();
    let mut ticker = tokio::time::interval(tick_interval);
    let mut buf = vec![0u8; RECV_BUF];
    // Per-sender replay windows, keyed by the authenticated sender id (ADR 0023).
    let mut windows: HashMap<String, ReplayWindow> = HashMap::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => {
                // Graceful leave: announce our departure, flush it, and stop driving.
                for action in swim.leave() {
                    apply(&socket, action, &events, auth.as_ref(), &mut seq_alloc).await;
                }
                debug!("SWIM graceful leave announced; stopping driver");
                return;
            }
            _ = ticker.tick() => {
                let now = elapsed_ms(start);
                for action in swim.tick(now) {
                    apply(&socket, action, &events, auth.as_ref(), &mut seq_alloc).await;
                }
            }
            recv = socket.recv_from(&mut buf) => {
                let Ok((n, src)) = recv else { continue };
                // Authenticate before a single byte is deserialized.
                let opened = if let Some(a) = &auth {
                    let Some(o) = a.open(&buf[..n]) else {
                        debug!(%src, "dropping SWIM datagram failing authentication");
                        continue;
                    };
                    o
                } else {
                    Opened { payload: &buf[..n], identity: None, seq: None }
                };
                let Ok(msg) = bincode::deserialize::<Message>(opened.payload) else {
                    trace!(%src, "dropping undecodable SWIM datagram");
                    continue;
                };
                // A signed datagram binds to its sender: the authenticated certificate
                // Common Name must equal the claimed `from`, so a node cannot gossip as
                // another (ADR 0022). Unsigned datagrams (no identity) fall through.
                if let Some(cn) = &opened.identity {
                    if cn != &msg.from {
                        debug!(%src, claimed = %msg.from, authenticated = %cn,
                            "dropping SWIM datagram whose signed identity does not match its sender");
                        continue;
                    }
                }
                // Anti-replay (ADR 0023): a sequenced datagram (always authenticated, so the
                // window cannot be poisoned) is dropped if its sequence is a replay for that
                // sender. The first sequence per sender seeds the window.
                if let Some(seq) = opened.seq {
                    let fresh = windows.entry(msg.from.clone()).or_default().check_and_set(seq);
                    if !fresh {
                        debug!(%src, from = %msg.from, seq, "dropping replayed SWIM datagram");
                        continue;
                    }
                }
                let now = elapsed_ms(start);
                for action in swim.handle(msg, now) {
                    apply(&socket, action, &events, auth.as_ref(), &mut seq_alloc).await;
                }
            }
        }
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

async fn apply(
    socket: &UdpSocket,
    action: Action,
    events: &mpsc::UnboundedSender<MembershipEvent>,
    auth: Option<&SwimAuth>,
    seq_alloc: &mut Option<SeqAlloc>,
) {
    match action {
        Action::Send { to, msg } => {
            if let Ok(bytes) = bincode::serialize(&msg) {
                // With a sequence allocator configured, seal a signed+sequenced v3 datagram
                // (ADR 0023); otherwise the signed/plain seal (ADR 0022/0003).
                let datagram = match (auth, seq_alloc.as_mut()) {
                    (Some(a), Some(alloc)) => a.seal_sequenced(&bytes, alloc.allocate()),
                    (Some(a), None) => a.seal(&bytes),
                    (None, _) => bytes,
                };
                let _ = socket.send_to(&datagram, &to).await;
            }
        }
        Action::StateChange {
            id,
            addr,
            peer_addr,
            state,
        } => {
            let _ = events.send(MembershipEvent {
                id,
                addr,
                peer_addr,
                state,
            });
        }
    }
}
