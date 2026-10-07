//! Inter-node peer transport.
//!
//! Each node both **listens** for incoming peer links and **dials** every
//! statically-configured peer (with retry), forming a full mesh. On each link a
//! `Hello` is exchanged to learn the remote node id, then the link is registered
//! with the hub and pumps messages both ways until it drops.
//!
//! Security: links run **mutual TLS** against a dedicated cluster CA when a
//! [`PeerTls`] context is supplied (ADR 0002) — the listener requires a client
//! certificate and the dialer verifies the server certificate, so possession of
//! a cluster-CA-issued cert is what admits a node to the mesh. Plaintext links
//! remain possible only when no context is configured (loudly logged in `main`).
//!
//! Version gating: the NEGOTIATED proto of each link is carried to the hub
//! ([`HubCommand::PeerConnected`]'s `proto`), because frame choice must be per-link.
//! The peer codec is strict — an unknown variant index, or trailing bytes from a field
//! an older build does not know, is a decode error that `read_frame` reports as an
//! `io::Error`, which tears the link down and redials. So "send the new frame and let
//! old peers ignore it" is not available: it is a flap loop. Additive frames therefore
//! ship under a raised `PROTO_MAX` and are sent only when the link negotiated it
//! (0041-T12, issue #238).

use crate::conn::ConnPolicy;
use crate::hub::{HubCommand, PeerOutbound};
use crate::ingress::{IngressCredit, IngressPermit};
use bytes::BytesMut;
use mqtt_cluster::durable_plane::DurablePlane;
use mqtt_cluster::peer::{self, PeerMessage};
use mqtt_cluster::stage_timing;
use mqtt_cluster::NodeId;
use mqtt_net::tls;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, warn};

/// The Subject Common Name of the verified peer certificate, if the link is
/// mTLS and the leaf carries a usable CN.
///
/// The mTLS verifier has already required a cluster-CA-issued certificate; this
/// only reads the CN that the [`handle`] binding check compares against the
/// peer's `Hello`. A `None` here (plaintext link, no presented cert, or a
/// CN-less cert) means *no binding* — see [`handle`] for the policy.
///
/// Works for both the accepting (`server::TlsStream`) and dialing
/// (`client::TlsStream`) sides: both expose the verified chain through the same
/// `CommonState` returned by their `get_ref().1`. We name it through the
/// `tokio_rustls` re-export because `rustls` itself is only a dev-dependency.
fn peer_cert_cn(state: &tokio_rustls::rustls::CommonState) -> Option<String> {
    let leaf = state.peer_certificates()?.first()?;
    match mqtt_auth::mtls::identity_from_cert(leaf) {
        Ok(identity) => Some(identity.subject),
        Err(e) => {
            warn!(error = %e, "peer certificate verified but has no usable Common Name; no node-id binding");
            None
        }
    }
}

/// The remote leaf certificate's serial from an established peer TLS session —
/// the fact a cluster CRL names, recorded with the link so a revocation sweep can
/// re-check it (ADR 0040 T4).
fn peer_cert_serial(state: &tokio_rustls::rustls::CommonState) -> Option<Vec<u8>> {
    let leaf = state.peer_certificates()?.first()?;
    mqtt_auth::mtls::serial_from_cert(leaf)
}

/// Gate a freshly-handshaken peer link on the **live** cluster CRL (ADR 0040 T4):
/// the same slot the gossip verifier reads per datagram, so a republished CRL
/// refuses new links immediately — on the ACCEPT side (a revoked node dialing us)
/// and on the DIAL side (us dialing a node whose cert was revoked; the TLS
/// verifier alone would still admit it, since the connector checks chain + name,
/// not revocation). Returns the remote serial for the link's registration, or
/// `Err(())` when the link must be dropped.
fn admit_peer_link(
    state: &tokio_rustls::rustls::CommonState,
    crl: &crate::reload::SwimCrlSlot,
) -> Result<Option<Vec<u8>>, ()> {
    let serial = peer_cert_serial(state);
    if let (Some(serial), Some(list)) = (
        &serial,
        crl.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref(),
    ) {
        if list.contains(serial) {
            warn!("peer certificate is revoked by the cluster CRL; dropping link (ADR 0040)");
            return Err(());
        }
    }
    Ok(serial)
}

/// The cluster bus mTLS context, built once from the cluster CA + node cert and
/// shared by every peer link (both accepting and dialing sides).
#[derive(Clone)]
pub struct PeerTls {
    /// Accepts inbound links, requiring a cluster-CA-issued client certificate.
    /// Behind a `watch` (ADR 0040 T4): read per accept, so a reload's rebuilt
    /// acceptor (rotated cluster cert/key/CA) is served on the next handshake.
    /// Use [`fixed_acceptor`] where no reloader exists.
    pub acceptor: tokio::sync::watch::Receiver<TlsAcceptor>,
    /// Dials outbound links, presenting our certificate and verifying the peer's.
    /// Behind a `watch` (ADR 0040 T4): read per dial. Use [`fixed_connector`]
    /// where no reloader exists.
    pub connector: tokio::sync::watch::Receiver<TlsConnector>,
    /// Cluster CA certificate (DER) — verifies inbound signed-gossip certs (ADR 0022).
    pub ca_der: Vec<u8>,
    /// This node's leaf certificate (DER) — embedded inline in signed gossip.
    pub cert_der: Vec<u8>,
    /// This node's private key (DER) — signs outgoing gossip.
    pub key_der: Vec<u8>,
    /// The live cluster-bus revocation list the gossip verifier consults per datagram
    /// (ADR 0022 T7): `None` when `MQTTD_PEER_TLS_CRL` is unset. Shared with the reloader,
    /// which swaps a freshly-parsed list in on SIGHUP/watch reload.
    pub gossip_crl: crate::reload::SwimCrlSlot,
    /// The configured CRL path, kept so the reload closure and the file watcher re-read it.
    pub crl_path: Option<std::path::PathBuf>,
}

/// A fixed (never-reloading) acceptor handle — for tests and setups without a
/// [`Reloader`](crate::reload::Reloader).
#[must_use]
pub fn fixed_acceptor(acceptor: TlsAcceptor) -> tokio::sync::watch::Receiver<TlsAcceptor> {
    tokio::sync::watch::channel(acceptor).1
}

/// A fixed (never-reloading) connector handle — for tests and setups without a
/// [`Reloader`](crate::reload::Reloader).
#[must_use]
pub fn fixed_connector(connector: TlsConnector) -> tokio::sync::watch::Receiver<TlsConnector> {
    tokio::sync::watch::channel(connector).1
}

impl std::fmt::Debug for PeerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTls").finish_non_exhaustive()
    }
}

/// Read buffer ceiling per peer link.
const MAX_BUFFERED: usize = 32 * 1024 * 1024;
/// Delay between reconnection attempts to a peer.
const REDIAL_DELAY: Duration = Duration::from_millis(500);

static PEER_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Per-link work counters (#662). One task carries a whole peer link (both
/// directions, the TLS session, encode and decode), so its busy share is the
/// link's capacity question: `busy_ns` is the time the task spent being polled
/// (on CPU: crypto, syscalls, codec, dispatch; never its waiting), `polls` how
/// often it ran. Frames and I/O calls give the batching per syscall both ways.
/// Cumulative since the link connected; read them as deltas over a window.
#[derive(Debug, Default)]
pub struct LinkStats {
    pub busy_ns: AtomicU64,
    pub polls: AtomicU64,
    pub frames_out: AtomicU64,
    pub writes: AtomicU64,
    pub frames_in: AtomicU64,
    pub reads: AtomicU64,
}

type LinkKey = (String, String); // (local node, remote node)
static LINKS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<LinkKey, Arc<LinkStats>>>,
> = std::sync::OnceLock::new();

fn links() -> &'static std::sync::Mutex<std::collections::BTreeMap<LinkKey, Arc<LinkStats>>> {
    LINKS.get_or_init(Default::default)
}

/// The live links of `local` (keyed by local node, so in-process clusters in
/// tests do not see each other's links), for the hub's metrics tick.
pub fn link_stats(local: &NodeId) -> Vec<(String, Arc<LinkStats>)> {
    links()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|((l, _), _)| *l == local.0)
        .map(|((_, r), st)| (r.clone(), st.clone()))
        .collect()
}

/// Counts read/write calls that moved bytes on one half of a link.
struct Counted<'a, T> {
    inner: T,
    stats: &'a LinkStats,
}

impl<T: AsyncRead + Unpin> AsyncRead for Counted<'_, T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(r, std::task::Poll::Ready(Ok(()))) && buf.filled().len() > before {
            self.stats.reads.fetch_add(1, Ordering::Relaxed);
        }
        r
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Counted<'_, T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let r = std::pin::Pin::new(&mut self.inner).poll_write(cx, data);
        if matches!(r, std::task::Poll::Ready(Ok(n)) if n > 0) {
            self.stats.writes.fetch_add(1, Ordering::Relaxed);
        }
        r
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Accept incoming peer links on `listener` forever.
///
/// With a [`PeerTls`] context, every link must complete an mTLS handshake
/// (cluster-CA-issued client certificate) before a single frame is read.
/// `client_policy` is used to serve sessions relocated here over the bus
/// (ADR 0005): a `ProxyHello` connection runs a real client session under this
/// policy. `None` declines proxied sessions (they are dropped).
pub async fn serve_listener(
    listener: TcpListener,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<PeerTls>,
    client_policy: Option<Arc<ConnPolicy>>,
    plane: Option<DurablePlane>,
) {
    serve_listener_with_ingress(listener, local, hub, tls, client_policy, plane, None).await;
}

/// [`serve_listener`], with the node's ingress credit charged for inbound peer `QoS` 0
/// publishes (ADR 0082 T4). `None` charges nothing.
pub async fn serve_listener_with_ingress(
    listener: TcpListener,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<PeerTls>,
    client_policy: Option<Arc<ConnPolicy>>,
    plane: Option<DurablePlane>,
    ingress: Option<Arc<IngressCredit>>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let local = local.clone();
                let hub = hub.clone();
                let tls = tls.clone();
                let policy = client_policy.clone();
                let plane = plane.clone();
                let ingress = ingress.clone();
                tokio::spawn(async move {
                    let _ = stream.set_nodelay(true);
                    // Read the CURRENT acceptor per accept (ADR 0040 T4): a reload's
                    // rebuilt material is served on the next handshake. (Bound first:
                    // the watch guard must not live across an await.)
                    let acceptor = tls.as_ref().map(|t| t.acceptor.borrow().clone());
                    let result = match (&tls, acceptor) {
                        (Some(t), Some(acceptor)) => match acceptor.accept(stream).await {
                            Ok(s) => {
                                let Ok(serial) = admit_peer_link(s.get_ref().1, &t.gossip_crl)
                                else {
                                    return; // revoked: fail closed (ADR 0040 T4)
                                };
                                let expected_cn = peer_cert_cn(s.get_ref().1);
                                handle(
                                    s,
                                    local,
                                    hub,
                                    false,
                                    expected_cn,
                                    serial,
                                    policy,
                                    plane,
                                    ingress,
                                )
                                .await
                            }
                            Err(e) => {
                                debug!(error = %e, "peer mTLS handshake failed; link rejected");
                                return;
                            }
                        },
                        _ => {
                            handle(
                                stream, local, hub, false, None, None, policy, plane, ingress,
                            )
                            .await
                        }
                    };
                    if let Err(e) = result {
                        debug!(error = %e, "inbound peer link ended");
                    }
                });
            }
            // Issue #504: an fd squeeze must not end the cluster-bus listener — that
            // would leave this node unable to accept any peer link for good.
            Err(e) => crate::accept::pause_after_error(&e, "peer").await,
        }
    }
}

/// Dial `addr` and keep the link up, redialing on failure, forever.
///
/// With a [`PeerTls`] context the link is mTLS: the remote's certificate is
/// verified against the cluster CA (its host name/IP taken from `addr`) and our
/// certificate is presented.
///
/// If the handshake reveals that *this* direction is the redundant one (the peer
/// has the lower node id, so it owns the link), dialing stops permanently: the
/// other node maintains the single surviving link.
pub async fn dial_forever(
    addr: String,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<PeerTls>,
    plane: Option<DurablePlane>,
) {
    dial_forever_with_ingress(addr, local, hub, tls, plane, None).await;
}

/// [`dial_forever`], with the node's ingress credit charged for inbound peer `QoS` 0
/// publishes (ADR 0082 T4). `None` charges nothing.
pub async fn dial_forever_with_ingress(
    addr: String,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<PeerTls>,
    plane: Option<DurablePlane>,
    ingress: Option<Arc<IngressCredit>>,
) {
    // An undialable name is permanent; retrying would only spin.
    let server_name = match tls.as_ref().map(|_| tls::server_name(&addr)).transpose() {
        Ok(name) => name,
        Err(e) => {
            warn!(%addr, error = %e, "cannot dial peer over TLS; giving up");
            return;
        }
    };
    loop {
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                debug!(%addr, "dialed peer");
                let _ = stream.set_nodelay(true);
                let outcome = match (&tls, &server_name) {
                    (Some(t), Some(name)) => {
                        // Read the CURRENT connector per dial (ADR 0040 T4).
                        let connector = t.connector.borrow().clone();
                        match connector.connect(name.clone(), stream).await {
                            Ok(s) => {
                                let Ok(serial) = admit_peer_link(s.get_ref().1, &t.gossip_crl)
                                else {
                                    // Revoked (ADR 0040 T4): fail closed, but keep
                                    // redialing — an un-revocation (new CRL) heals.
                                    tokio::time::sleep(REDIAL_DELAY).await;
                                    continue;
                                };
                                let expected_cn = peer_cert_cn(s.get_ref().1);
                                handle(
                                    s,
                                    local.clone(),
                                    hub.clone(),
                                    true,
                                    expected_cn,
                                    serial,
                                    None,
                                    plane.clone(),
                                    ingress.clone(),
                                )
                                .await
                            }
                            Err(e) => {
                                debug!(%addr, error = %e, "peer mTLS handshake failed; will retry");
                                tokio::time::sleep(REDIAL_DELAY).await;
                                continue;
                            }
                        }
                    }
                    _ => {
                        handle(
                            stream,
                            local.clone(),
                            hub.clone(),
                            true,
                            None,
                            None,
                            None,
                            plane.clone(),
                            ingress.clone(),
                        )
                        .await
                    }
                };
                match outcome {
                    Ok(LinkOutcome::Redundant) => {
                        debug!(%addr, "not the owning side for this peer; stopping dial");
                        return;
                    }
                    Ok(LinkOutcome::Closed) => {}
                    Err(e) => debug!(%addr, error = %e, "outbound peer link ended"),
                }
            }
            Err(e) => debug!(%addr, error = %e, "peer dial failed; will retry"),
        }
        tokio::time::sleep(REDIAL_DELAY).await;
    }
}

/// Why a peer link ended, so the dialer knows whether to retry.
enum LinkOutcome {
    /// The link served and then closed; the dialer should re-establish it.
    Closed,
    /// This direction was the redundant one (deduped by the tie-break); the dialer
    /// should stop, because the peer owns the single link.
    Redundant,
}

/// The version this link speaks, or `None` when the peer's announced range does not
/// overlap ours (ADR 0038) — logged loudly, and the link must then be dropped: a node
/// that cannot agree on a protocol version must not half-join the mesh.
///
/// The negotiated value is KEPT rather than discarded (0041-T12, issue #238): the hub
/// chooses per-link between a proto-6 and a proto-7 frame with it. That gating is forced
/// by the codec, not a style preference — [`peer::decode`] is strict, so an unknown
/// variant index (or trailing bytes from an added field) is a `Serde` error, which
/// `read_frame` turns into an `io::Error`, which tears the link down and redials: a flap
/// loop, not a graceful "old peers ignore what they do not know".
fn negotiated_proto(node_id: &str, proto_min: u32, proto_max: u32) -> Option<u32> {
    let negotiated =
        peer::negotiate_proto((peer::PROTO_MIN, peer::PROTO_MAX), (proto_min, proto_max));
    if negotiated.is_none() {
        warn!(
            peer = %node_id,
            ours = ?(peer::PROTO_MIN, peer::PROTO_MAX),
            theirs = ?(proto_min, proto_max),
            "peer speaks an incompatible peer-bus protocol range; dropping link (ADR 0038)"
        );
    }
    negotiated
}

/// Run a single peer link: handshake, dedup, register, then pump until it closes.
///
/// `initiated` is true when we dialed (vs. accepted). To guarantee exactly one
/// link per node pair, the surviving link is the one whose initiating side has the
/// **smaller node id**; the other direction is dropped right after the handshake.
///
/// `expected_cn` binds the peer's claimed identity to its certificate (ADR 0004
/// step 5; resolves a deferred item from ADR 0002): when `Some(cn)`, the remote
/// `Hello`'s `node_id` MUST equal `cn` (the Subject CN of the verified peer
/// certificate), otherwise the link is dropped — a cluster-CA cert no longer
/// admits a node under an arbitrary id. `None` (plaintext mesh, or a CN-less
/// cert) applies no binding, keeping the unauthenticated mesh working.
// The handshake/dedup ladder is one linear flow; splitting it would scatter the
// link-rejection cases.
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn handle<S>(
    stream: S,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    initiated: bool,
    expected_cn: Option<String>,
    cert_serial: Option<Vec<u8>>,
    client_policy: Option<Arc<ConnPolicy>>,
    plane: Option<DurablePlane>,
    ingress: Option<Arc<IngressCredit>>,
) -> Result<LinkOutcome, std::io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut rh, mut wh) = tokio::io::split(stream);
    let mut buf = BytesMut::with_capacity(4096);

    // The dialer announces itself first; the accept side reads first so it can
    // detect a session-proxy connection (ADR 0005) before announcing itself —
    // a proxied client expects raw MQTT back, not our peer Hello.
    let (remote, proto) = if initiated {
        write_frame(
            &mut wh,
            &PeerMessage::Hello {
                node_id: local.0.clone(),
                proto_min: peer::PROTO_MIN,
                proto_max: peer::PROTO_MAX,
            },
        )
        .await?;
        match read_frame(&mut rh, &mut buf).await? {
            Some(PeerMessage::Hello {
                node_id,
                proto_min,
                proto_max,
            }) => {
                let Some(proto) = negotiated_proto(&node_id, proto_min, proto_max) else {
                    return Ok(LinkOutcome::Closed);
                };
                (NodeId(node_id), proto)
            }
            Some(_) => {
                warn!("peer did not send Hello first; dropping link");
                return Ok(LinkOutcome::Closed);
            }
            None => return Ok(LinkOutcome::Closed),
        }
    } else {
        match read_frame(&mut rh, &mut buf).await? {
            // A persistent session relocated here by its landing node (ADR 0005).
            // The connection arrived over the mTLS bus, so the sender is a
            // verified mesh member; we serve the vouched identity. The leftover
            // `buf` holds the client's MQTT stream (its CONNECT onward).
            Some(PeerMessage::ProxyHello { identity, via }) => {
                let Some(policy) = client_policy else {
                    debug!("ProxyHello received but no client policy configured; dropping");
                    return Ok(LinkOutcome::Closed);
                };
                let identity = identity.map(|subject| mqtt_auth::Identity {
                    subject,
                    groups: Vec::new(),
                });
                crate::conn::serve_proxied(rh, wh, None, identity, policy, hub, buf, via).await;
                return Ok(LinkOutcome::Closed);
            }
            Some(PeerMessage::Hello {
                node_id,
                proto_min,
                proto_max,
            }) => {
                // Reject BEFORE announcing ourselves: an incompatible build gets a
                // clean close, not half a handshake.
                let Some(proto) = negotiated_proto(&node_id, proto_min, proto_max) else {
                    return Ok(LinkOutcome::Closed);
                };
                write_frame(
                    &mut wh,
                    &PeerMessage::Hello {
                        node_id: local.0.clone(),
                        proto_min: peer::PROTO_MIN,
                        proto_max: peer::PROTO_MAX,
                    },
                )
                .await?;
                (NodeId(node_id), proto)
            }
            Some(_) => {
                warn!("peer did not send Hello first; dropping link");
                return Ok(LinkOutcome::Closed);
            }
            None => return Ok(LinkOutcome::Closed),
        }
    };

    // Node-id ↔ certificate binding: the peer may only claim the node id that
    // its certificate's Subject CN attests to. Enforced before the self-connect
    // and tie-break checks so an impersonator is dropped regardless of either.
    // For the dialer, a permanent mismatch simply redials, which is acceptable.
    if let Some(cn) = &expected_cn {
        if cn != &remote.0 {
            warn!(
                cert_cn = %cn,
                claimed = %remote.0,
                "peer Hello node id does not match its certificate Common Name; dropping link"
            );
            return Ok(LinkOutcome::Closed);
        }
    }

    if remote == local {
        debug!("ignoring self-connection");
        return Ok(LinkOutcome::Redundant);
    }

    // Keep exactly one link per pair: the one initiated by the smaller-id node.
    let owns_link = initiated == (local.0 < remote.0);
    if !owns_link {
        debug!(peer = %remote.0, "dropping redundant peer link (tie-break)");
        return Ok(LinkOutcome::Redundant);
    }

    let conn_id = PEER_CONN_ID.fetch_add(1, Ordering::Relaxed);
    let (out_tx, mut out_rx): (PeerOutbound, _) = mpsc::unbounded_channel();
    // The CONTROL lane (issue #358): raft RPCs and replication acks are small and
    // deadline-bound (500 ms heartbeat / 5 s replication RPC), while the bulk lane
    // carries forwarded publishes, shared deliveries, retained snapshots — frames
    // up to 16 MiB. One FIFO for both meant a heartbeat could sit behind megabytes
    // of data until openraft's deadline passed: elections churned on healthy links
    // and spread-ownership durable appends starved. Two queues, one socket — the
    // pump drains control first; the wire format is untouched.
    let (ctl_tx, mut ctl_rx): (PeerOutbound, _) = mpsc::unbounded_channel();
    // Issue #504: both lanes are UNBOUNDED, so a frame the hub has queued but the
    // wire has not taken is reported by nothing — it is not `delivered`, nothing
    // dropped it, and it is not in `backlog_bytes` (the per-subscriber egress
    // queue). On the lane E ladder that gap held a fifth of the stream while
    // every drop counter read zero. An `UnboundedSender` cannot be asked its
    // depth, only the receiver can, so the PUMP publishes it here and the hub
    // reads it for the gauge. Relaxed ordering throughout: this is a diagnostic,
    // and a gauge one loop iteration stale is worth none of a fence.
    let depth = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // The pump keeps WEAK sender handles so inbound durable-plane requests can
    // spawn their handling and route the reply straight back onto the link's
    // lanes — never through the hub command queue (issue #358). Weak, so the
    // pump does not hold its own receivers open: when the hub drops this link
    // (takeover, disconnect), the lanes still close and the pump still exits.
    let (reply_ctl, reply_bulk) = (ctl_tx.downgrade(), out_tx.downgrade());
    if hub
        .send(HubCommand::PeerConnected {
            node: remote.clone(),
            conn_id,
            tx: out_tx,
            ctl: ctl_tx,
            depth: depth.clone(),
            cert_serial,
            proto,
        })
        .is_err()
    {
        return Ok(LinkOutcome::Closed);
    }

    let stats = Arc::new(LinkStats::default());
    links()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert((local.0.clone(), remote.0.clone()), stats.clone());
    let mut rh = Counted {
        inner: rh,
        stats: &stats,
    };
    let mut wh = Counted {
        inner: wh,
        stats: &stats,
    };
    // Scoped: the pump borrows `remote`, which the disconnect below moves.
    let result = {
        let link = pump(
            &mut rh,
            &mut wh,
            &mut buf,
            &hub,
            &remote,
            &mut ctl_rx,
            &mut out_rx,
            &reply_ctl,
            &reply_bulk,
            &depth,
            &stats,
            plane.as_ref(),
            ingress.as_deref(),
        );
        // On-CPU time of the link task: every poll, timed, and none of its waiting.
        let mut link = std::pin::pin!(link);
        std::future::poll_fn(|cx| {
            let started = std::time::Instant::now();
            let r = std::future::Future::poll(link.as_mut(), cx);
            let ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            stats.busy_ns.fetch_add(ns, Ordering::Relaxed);
            stats.polls.fetch_add(1, Ordering::Relaxed);
            r
        })
        .await
    };
    {
        let mut map = links()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = (local.0.clone(), remote.0.clone());
        if map.get(&key).is_some_and(|st| Arc::ptr_eq(st, &stats)) {
            map.remove(&key);
        }
    }
    let _ = hub.send(HubCommand::PeerDisconnected {
        node: remote,
        conn_id,
    });
    result.map(|()| LinkOutcome::Closed)
}

#[allow(clippy::too_many_arguments)] // one straight-line link loop; a struct would hide the shape
async fn pump<R, W>(
    rh: &mut R,
    wh: &mut W,
    buf: &mut BytesMut,
    hub: &mpsc::UnboundedSender<HubCommand>,
    remote: &NodeId,
    ctl_rx: &mut mpsc::UnboundedReceiver<PeerMessage>,
    out_rx: &mut mpsc::UnboundedReceiver<PeerMessage>,
    reply_ctl: &mpsc::WeakUnboundedSender<PeerMessage>,
    reply_bulk: &mpsc::WeakUnboundedSender<PeerMessage>,
    depth: &std::sync::atomic::AtomicUsize,
    stats: &LinkStats,
    plane: Option<&DurablePlane>,
    ingress: Option<&IngressCredit>,
) -> Result<(), std::io::Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // Owned by the link and reused for its lifetime (issue #7): `write_batch`
    // only ever clears them, so steady state costs no allocation per frame. One
    // per lane, since both can be mid-batch across a `select!` iteration.
    let mut out_buf: Vec<u8> = Vec::with_capacity(PEER_WRITE_BUDGET);
    let mut ctl_buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let (out_buf, ctl_buf) = (&mut out_buf, &mut ctl_buf);
    // The stamped frames of the batch in hand, timed once it is written (#662).
    // Reused the same way: they grow to the largest batch's sample and stay.
    let (mut out_stamps, mut ctl_stamps) = (Vec::new(), Vec::new());
    loop {
        // What the hub's `peer_forwards_in_flight` gauge reads (issue #504).
        // Updated here rather than at the hub's send sites because only the
        // RECEIVER can report its own depth.
        depth.store(
            ctl_rx.len() + out_rx.len(),
            std::sync::atomic::Ordering::Relaxed,
        );
        // `biased` makes the polling order the PRIORITY order: control frames
        // (raft RPCs, replication acks — small by construction) always drain
        // before bulk data. Starving bulk is not a concern — control traffic is
        // a few frames per heartbeat interval, not a stream. Inbound reads sit
        // between the lanes so a flood in either direction cannot starve the
        // other side's acks.
        tokio::select! {
            biased;
            maybe_ctl = ctl_rx.recv() => {
                match maybe_ctl {
                    // The control lane batches too: with durable sessions on,
                    // ReplicateAck is per-record, so a replication burst is
                    // exactly the case that was paying one syscall per ack.
                    // Priority is unaffected — `biased` still drains this lane
                    // first, and a batch only ever contains frames already
                    // queued on it.
                    Some(msg) => {
                        let n = write_batch(wh, ctl_buf, &mut ctl_stamps, &msg, ctl_rx, remote).await?;
                        stats.frames_out.fetch_add(n, Ordering::Relaxed);
                    }
                    None => return Ok(()), // taken over or hub gone
                }
            }
            inbound = read_frame(rh, buf) => {
                match inbound? {
                    None => return Ok(()), // peer closed
                    Some(msg) => {
                        stats.frames_in.fetch_add(1, Ordering::Relaxed);
                        forward_inbound(msg, hub, remote, plane, ingress, reply_ctl, reply_bulk);
                    }
                }
            }
            maybe_out = out_rx.recv() => {
                match maybe_out {
                    // An oversized frame is dropped with a warning rather than killing
                    // the link: losing one best-effort message is strictly better than
                    // severing every message on the link (and a link-up back-fill that
                    // dies on send would die again on every reconnect). Other I/O
                    // errors still end the link as before.
                    Some(msg) => {
                        let n = write_batch(wh, out_buf, &mut out_stamps, &msg, out_rx, remote).await?;
                        stats.frames_out.fetch_add(n, Ordering::Relaxed);
                    }
                    None => return Ok(()), // taken over or hub gone
                }
            }
        }
    }
}

/// The credit an inbound peer publish takes before it is queued for the hub (ADR 0082
/// T4, §3). Only a non-retained `QoS` 0 publish is charged, against the node pool with
/// no per-link cap; `Ok(None)` = uncharged. `Err(())` = no credit: the caller drops it,
/// counted. Never paused — a peer link also carries consensus and replication, which
/// must not stall behind best-effort data. `QoS` 1 and 2 were promised upstream, and a
/// retained copy is this node's only record of the topic's state, so neither is shed.
fn peer_credit(
    ingress: Option<&IngressCredit>,
    qos: mqtt_codec::QoS,
    retain: bool,
    topic_len: usize,
    body_len: usize,
) -> Result<Option<IngressPermit>, ()> {
    let Some(ingress) = ingress else {
        return Ok(None);
    };
    if qos != mqtt_codec::QoS::AtMostOnce || retain {
        return Ok(None);
    }
    let permit = ingress.try_acquire_pool(ingress.cost(topic_len, body_len));
    if permit.is_none() {
        ingress.note_peer_shed();
        return Err(());
    }
    Ok(permit)
}

/// Translate an inbound peer message into a hub command.
// One arm per wire variant — a flat dispatch table, not a refactor smell.
#[allow(clippy::too_many_lines)]
fn forward_inbound(
    msg: PeerMessage,
    hub: &mpsc::UnboundedSender<HubCommand>,
    remote: &NodeId,
    plane: Option<&DurablePlane>,
    ingress: Option<&IngressCredit>,
    reply_ctl: &mpsc::WeakUnboundedSender<PeerMessage>,
    reply_bulk: &mpsc::WeakUnboundedSender<PeerMessage>,
) {
    match msg {
        // The replication data path, with no task per frame. It carries every
        // durable message twice per node — a `Replicate` in and its ack out as a
        // follower, the acks in as an owner — and a spawned task per frame (plus
        // a oneshot and the wake-ups around it) was the largest scheduling cost a
        // durable node paid. A `Replicate` goes straight to its shard's writer,
        // which puts the `ReplicateAck` on the control lane when the batch
        // commits; an ack just wakes the waiting append.
        PeerMessage::Replicate {
            req_id, epoch, op, ..
        } if plane.is_some() => {
            if let Some(plane) = plane {
                plane.submit_replicate(req_id, epoch, op, reply_ctl.clone());
            }
        }
        PeerMessage::ReplicateAck {
            req_id, accepted, ..
        } if plane.is_some() => {
            if let Some(plane) = plane {
                plane.complete_replicate_ack(req_id, accepted);
            }
        }
        // EVERY durable-plane frame routes DIRECTLY to the plane, bypassing the
        // hub command queue. Replies were first (ADR 0042 T9, exhibit ⑩): an
        // on-loop durable append awaits exactly these acks, which would sit
        // queued behind the very dispatch that is waiting. REQUESTS followed
        // (issue #358): under spread ownership every node is a leader AND a
        // follower — a hub loop that (transitively) waits on its own append
        // quorum cannot dispatch the inbound `Replicate`s the OTHER leaders'
        // quorums need, and the three loops starve each other until the 5 s RPC
        // bound fires (measured: frames on the wire in µs, dequeued from the
        // follower's hub queue 5–10 s later, on an idle cluster). The plane
        // handles frames without touching hub state, so nothing here needs the
        // loop; replies ride back on the link's lanes directly — consensus
        // replies and acks on the control lane, data-bearing replies on bulk.
        frame @ (PeerMessage::Replicate { .. }
        | PeerMessage::ReplicateAck { .. }
        | PeerMessage::RaftRpc { .. }
        | PeerMessage::RaftRpcReply { .. }
        | PeerMessage::ReplicaRead { .. }
        | PeerMessage::ReplicaReadReply { .. }
        | PeerMessage::ReplicaReadFrom { .. }
        | PeerMessage::ReplicaReadChunk { .. }
        | PeerMessage::ReplicaCatchUp { .. }
        | PeerMessage::ReplicaCatchUpTo { .. }
        | PeerMessage::ReplicaKeys { .. }
        | PeerMessage::ReplicaKeysReply { .. })
            if plane.is_some() =>
        {
            if let Some(plane) = plane {
                let plane = plane.clone();
                let ctl = reply_ctl.clone();
                let bulk = reply_bulk.clone();
                tokio::spawn(async move {
                    if let Some(reply) = plane.handle(frame).await {
                        let lane = match &reply {
                            PeerMessage::RaftRpcReply { .. } | PeerMessage::ReplicateAck { .. } => {
                                &ctl
                            }
                            _ => &bulk,
                        };
                        // A dead upgrade means the hub already dropped this link
                        // (takeover/disconnect) — the reply has nowhere to go, and
                        // the peer's RPC timeout is the designed recovery.
                        if let Some(lane) = lane.upgrade() {
                            let _ = lane.send(reply);
                        }
                    }
                });
            }
        }
        PeerMessage::Interest { filters } => {
            let _ = hub.send(HubCommand::RemoteInterest {
                node: remote.clone(),
                filters,
            });
        }
        PeerMessage::Publish {
            topic,
            payload,
            qos,
            retain,
            message_expiry,
            app,
        } => {
            let qos = mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce);
            let app = crate::hub::app_from_wire(app);
            let body = payload.len() + app.accounted_bytes();
            let Ok(credit) = peer_credit(ingress, qos, retain, topic.len(), body) else {
                return;
            };
            let _ = hub.send(HubCommand::RemotePublish {
                topic,
                payload: payload.into(),
                qos,
                retain,
                message_expiry,
                app,
                credit,
            });
        }
        PeerMessage::PublishAcked {
            seq,
            topic,
            payload,
            qos,
            retain,
            message_expiry,
            app,
        } => {
            let _ = hub.send(HubCommand::RemotePublishAcked {
                node: remote.clone(),
                seq,
                topic,
                payload: payload.into(),
                qos: mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce),
                retain,
                message_expiry,
                app: crate::hub::app_from_wire(app),
                origin: None,
                replay: false,
            });
        }
        PeerMessage::PublishAckedTagged {
            seq,
            origin,
            replay,
            topic,
            payload,
            qos,
            retain,
            message_expiry,
            app,
        } => {
            let _ = hub.send(HubCommand::RemotePublishAcked {
                node: remote.clone(),
                seq,
                topic,
                payload: payload.into(),
                qos: mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce),
                retain,
                message_expiry,
                app: crate::hub::app_from_wire(app),
                origin: Some(origin),
                replay,
            });
        }
        PeerMessage::PublishAck { seq, ok } => {
            let _ = hub.send(HubCommand::RemotePublishAck {
                node: remote.clone(),
                seq,
                ok,
            });
        }
        PeerMessage::PublishVerdict { seq, verdict } => {
            let _ = hub.send(HubCommand::RemotePublishVerdict {
                node: remote.clone(),
                seq,
                verdict,
            });
        }
        PeerMessage::SharedDeliverAcked {
            seq,
            client,
            topic,
            payload,
            qos,
            message_expiry,
            app,
        } => {
            let _ = hub.send(HubCommand::RemoteSharedDeliverAcked {
                node: remote.clone(),
                seq,
                client: mqtt_core::ClientId(client.into()),
                topic,
                payload: payload.into(),
                qos: mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce),
                message_expiry,
                app: crate::hub::app_from_wire(app),
            });
        }
        PeerMessage::SharedInterest { groups } => {
            let groups = groups
                .into_iter()
                .map(|g| crate::hub::RemoteSharedGroup {
                    group: g.group,
                    filter: g.filter,
                    members: g
                        .members
                        .into_iter()
                        .map(|m| {
                            (
                                mqtt_core::ClientId(m.client.into()),
                                mqtt_codec::QoS::from_u8(m.qos)
                                    .unwrap_or(mqtt_codec::QoS::AtMostOnce),
                                m.online,
                            )
                        })
                        .collect(),
                })
                .collect();
            let _ = hub.send(HubCommand::RemoteSharedInterest {
                node: remote.clone(),
                groups,
            });
        }
        PeerMessage::SharedDeliver {
            client,
            topic,
            payload,
            qos,
            message_expiry,
            app,
        } => {
            let qos = mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce);
            let app = crate::hub::app_from_wire(app);
            let body = payload.len() + app.accounted_bytes();
            let Ok(credit) = peer_credit(ingress, qos, false, topic.len(), body) else {
                return;
            };
            let _ = hub.send(HubCommand::RemoteSharedDeliver {
                client: mqtt_core::ClientId(client.into()),
                topic,
                payload: payload.into(),
                qos,
                message_expiry,
                app,
                credit,
            });
        }
        PeerMessage::RetainedSnapshot { messages } => {
            let _ = hub.send(HubCommand::RemoteRetainedSnapshot {
                node: remote.clone(),
                messages,
            });
        }
        PeerMessage::RetainedDigest {
            count,
            hash,
            value_hash,
        } => {
            let _ = hub.send(HubCommand::RemoteRetainedDigest {
                node: remote.clone(),
                count,
                hash,
                value_hash,
            });
        }
        PeerMessage::RetainedRequest => {
            let _ = hub.send(HubCommand::RemoteRetainedRequest {
                node: remote.clone(),
            });
        }
        PeerMessage::RetainedCommit {
            topic,
            payload,
            qos,
            props,
            seq,
            expires_at,
        } => {
            let _ = hub.send(HubCommand::RemoteRetainedCommit {
                node: remote.clone(),
                topic,
                payload: payload.into(),
                qos,
                app: crate::hub::app_from_wire(props),
                seq,
                expires_at,
            });
        }
        PeerMessage::RetainedCommitAck { seq, token } => {
            let _ = hub.send(HubCommand::RemoteRetainedCommitAck {
                node: remote.clone(),
                seq,
                token,
            });
        }
        PeerMessage::RetainedUpdate {
            topic,
            payload,
            qos,
            epoch,
            offset,
            props,
            expires_at,
        } => {
            let _ = hub.send(HubCommand::RemoteRetainedUpdate {
                topic,
                payload: payload.into(),
                qos,
                epoch,
                offset,
                app: crate::hub::app_from_wire(props),
                expires_at,
            });
        }
        PeerMessage::Hello { .. } => {
            warn!("unexpected duplicate Hello on established peer link");
        }
        PeerMessage::ProxyHello { .. } => {
            // A ProxyHello is only valid as the first frame of a session-proxy
            // connection (ADR 0005), handled at accept time — never mid-link.
            warn!("unexpected ProxyHello on established peer link");
        }
        frame @ (PeerMessage::Replicate { .. }
        | PeerMessage::ReplicateAck { .. }
        | PeerMessage::RaftRpc { .. }
        | PeerMessage::RaftRpcReply { .. }
        | PeerMessage::ReplicaRead { .. }
        | PeerMessage::ReplicaReadReply { .. }
        | PeerMessage::ReplicaReadFrom { .. }
        | PeerMessage::ReplicaReadChunk { .. }
        | PeerMessage::ReplicaCatchUp { .. }
        | PeerMessage::ReplicaCatchUpTo { .. }
        | PeerMessage::ReplicaKeys { .. }
        | PeerMessage::ReplicaKeysReply { .. }) => {
            // Durable-plane frames (ADR 0006/0007): consensus RPCs and session-log
            // replication. Routed to the hub, which dispatches them to the
            // `DurablePlane` (a no-op until durable sessions are enabled, step 4f).
            let _ = hub.send(HubCommand::DurableFrame {
                node: remote.clone(),
                frame,
            });
        }
    }
}

/// How many bytes of frames one `write_all` may carry (issue #7 / ADR 0077).
///
/// A budget rather than a frame count, because peer frames span four orders of
/// magnitude — a `SharedDeliverAcked` is tens of bytes, a `RetainedSnapshot`
/// chunk is megabytes — so "K frames" would mean wildly different syscall sizes.
/// 256 KiB is comfortably above a TCP window and far below `MAX_FRAME`, so a
/// batch is always one write and never an unbounded stall.
const PEER_WRITE_BUDGET: usize = 256 * 1024;

/// Drain what is already queued and write it as ONE syscall.
///
/// The pump previously took one message per loop iteration and paid a
/// `write_all` + `flush` for each: its own TLS record, its own TCP segment. On
/// the ADR 0077 lane E ladder ~80% of publishes crossed the bus at N=5, and each
/// carried an ack back, so that was roughly half a million syscalls a second
/// across the cluster — visible as `sys` time comparable to user time at a
/// plateau where no core was saturated.
///
/// SELF-ADAPTIVE by construction: `try_recv` returns nothing when the queue is
/// empty, so an idle link writes a batch of one and behaves exactly as before.
/// Batching engages only under backlog, which is when it is worth anything.
///
/// One YIELD before draining when the queue is empty. Without it, a backlog of
/// frames produced concurrently by other tasks (every durable append's
/// `Replicate`) never forms: the pump wakes on the first frame and writes it
/// before the next producer has run. Measured on a 3-node local cluster (#662):
/// the owner's Replicate stream averaged 3.1 frames per write syscall. With one
/// `yield_now` it was 15.1, peer write syscalls fell 78%, and owner throughput
/// rose 9.6% at the same CPU. A yield is a reschedule, not a timer, so it costs
/// an idle link no fixed delay.
///
/// `buf` is owned by the link and only ever `clear()`ed, so steady state is zero
/// allocations per frame. It is shrunk back after an oversized batch: `MAX_FRAME`
/// is 16 MiB and a retained snapshot would otherwise leave that capacity
/// resident on the link for the rest of its life. `stamps` is the same kind of
/// link-owned scratch, for the batch's stamped replication frames.
async fn write_batch<W: AsyncWrite + Unpin>(
    wh: &mut W,
    buf: &mut Vec<u8>,
    stamps: &mut Vec<(stage_timing::Stage, std::time::Instant)>,
    first: &PeerMessage,
    rx: &mut mpsc::UnboundedReceiver<PeerMessage>,
    remote: &NodeId,
) -> Result<u64, std::io::Error> {
    buf.clear();
    stamps.clear();
    let mut frames = 1u64;
    let mut encode_into = |buf: &mut Vec<u8>, msg: &PeerMessage| {
        let stamped = match msg {
            PeerMessage::Replicate { req_id, queued, .. } => {
                tracing::debug!(req_id, peer = %remote.0, "replicate: writing to wire");
                queued
                    .stamped_at()
                    .map(|at| (stage_timing::Stage::ReplicateQueue, at))
            }
            PeerMessage::ReplicateAck { queued, .. } => queued
                .stamped_at()
                .map(|at| (stage_timing::Stage::AckQueue, at)),
            _ => None,
        };
        // An oversized or unencodable frame is skipped, not fatal: losing one
        // best-effort message beats severing every message on the link (and a
        // link-up back-fill that died on send would die again on every
        // reconnect). `encode` truncates its partial write, so the frames
        // already batched here are untouched and still go out.
        match peer::encode(msg, buf) {
            Ok(()) => stamps.extend(stamped),
            Err(e) => {
                warn!(error = %e, peer = %remote.0, "dropping oversized/unencodable peer frame");
            }
        }
    };
    encode_into(buf, first);
    if rx.is_empty() {
        tokio::task::yield_now().await;
    }
    while buf.len() < PEER_WRITE_BUDGET {
        match rx.try_recv() {
            Ok(msg) => {
                frames += 1;
                encode_into(buf, &msg);
            }
            Err(_) => break, // empty, or closed — the closed case is seen by recv() next loop
        }
    }
    if buf.is_empty() {
        return Ok(0); // every frame in the batch was refused
    }
    wh.write_all(buf).await?;
    wh.flush().await?;
    // Replication transit up to the kernel (#662): queued on this link until
    // the kernel has the bytes. Timed here, not at encode, so the yield,
    // the rest of the batch and a wait on a full send buffer count too.
    if !stamps.is_empty() {
        let written = std::time::Instant::now();
        for &(stage, queued) in stamps.iter() {
            stage_timing::record(stage, written.saturating_duration_since(queued));
        }
    }
    // Give back the capacity a huge frame forced us to take.
    if buf.capacity() > PEER_WRITE_BUDGET * 2 {
        buf.shrink_to(PEER_WRITE_BUDGET);
    }
    Ok(frames)
}

async fn write_frame<W: AsyncWrite + Unpin>(
    wh: &mut W,
    msg: &PeerMessage,
) -> Result<(), std::io::Error> {
    let mut out = Vec::new();
    peer::encode(msg, &mut out)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    wh.write_all(&out).await?;
    wh.flush().await
}

async fn read_frame<R: AsyncRead + Unpin>(
    rh: &mut R,
    buf: &mut BytesMut,
) -> Result<Option<PeerMessage>, std::io::Error> {
    loop {
        match peer::decode(buf) {
            Ok(Some(msg)) => return Ok(Some(msg)),
            Ok(None) => {}
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        }
        if buf.len() > MAX_BUFFERED {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "peer frame buffer overflow",
            ));
        }
        let n = rh.read_buf(buf).await?;
        if n == 0 {
            return Ok(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    /// A frame produced while the writer holds the first one leaves in the SAME
    /// write: `write_batch` yields once before draining, so a concurrent producer
    /// (another append's `Replicate`) runs and its frame joins the batch instead
    /// of costing its own syscall and segment. Single-threaded runtime, so the
    /// order is deterministic: the producer can only run at that yield.
    #[tokio::test(flavor = "current_thread")]
    async fn a_frame_produced_during_the_yield_joins_the_same_write() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        /// Counts `poll_write` calls that wrote bytes.
        #[derive(Default)]
        struct CountingWriter {
            writes: usize,
            bytes: Vec<u8>,
        }
        impl tokio::io::AsyncWrite for CountingWriter {
            fn poll_write(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
                b: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                self.writes += 1;
                self.bytes.extend_from_slice(b);
                Poll::Ready(Ok(b.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        let first = PeerMessage::ReplicateAck {
            req_id: 1,
            accepted: true,
            queued: mqtt_cluster::peer::Queued::default(),
        };
        // The producer is spawned but cannot run until the writer yields.
        let producer = tokio::spawn(async move {
            tx.send(PeerMessage::ReplicateAck {
                req_id: 2,
                accepted: true,
                queued: mqtt_cluster::peer::Queued::default(),
            })
            .unwrap();
        });
        let mut w = CountingWriter::default();
        let (mut buf, mut stamps) = (Vec::new(), Vec::new());
        write_batch(
            &mut w,
            &mut buf,
            &mut stamps,
            &first,
            &mut rx,
            &NodeId("peer".into()),
        )
        .await
        .unwrap();
        producer.await.unwrap();
        assert_eq!(w.writes, 1, "both frames must leave in one write");
        let mut wire = BytesMut::from(&w.bytes[..]);
        let mut ids = Vec::new();
        while let Some(PeerMessage::ReplicateAck { req_id, .. }) = peer::decode(&mut wire).unwrap()
        {
            ids.push(req_id);
        }
        assert_eq!(ids, vec![1, 2]);
    }

    /// The control lane is drained before the bulk lane (issue #358): with both
    /// queues pre-loaded, every control frame must reach the wire before any bulk
    /// frame — a raft heartbeat's 500 ms deadline cannot survive queueing behind
    /// a data backlog, which is exactly what churned elections on healthy links.
    #[tokio::test]
    async fn control_frames_jump_the_bulk_queue() {
        let (mut ours, theirs) = tokio::io::duplex(1 << 20);
        let (hub_tx, _hub_rx) = mpsc::unbounded_channel();
        let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();

        // Bulk backlog FIRST, then the control frame — arrival order must lose
        // to lane priority.
        for i in 0..3 {
            out_tx
                .send(PeerMessage::Interest {
                    filters: vec![format!("bulk/{i}")],
                })
                .unwrap();
        }
        ctl_tx
            .send(PeerMessage::RaftRpc {
                req_id: 42,
                payload: vec![1, 2, 3],
            })
            .unwrap();

        let remote = NodeId("peer-under-test".into());
        let reply_ctl = ctl_tx.downgrade();
        let reply_bulk = out_tx.downgrade();
        let pump_task = tokio::spawn(async move {
            let (mut rh, mut wh) = tokio::io::split(theirs);
            let mut buf = BytesMut::new();
            pump(
                &mut rh,
                &mut wh,
                &mut buf,
                &hub_tx,
                &remote,
                &mut ctl_rx,
                &mut out_rx,
                &reply_ctl,
                &reply_bulk,
                &std::sync::atomic::AtomicUsize::new(0),
                &LinkStats::default(),
                None,
                None,
            )
            .await
        });

        let mut buf = BytesMut::new();
        let first = read_frame(&mut ours, &mut buf)
            .await
            .expect("read")
            .expect("frame");
        assert!(
            matches!(first, PeerMessage::RaftRpc { req_id: 42, .. }),
            "the control frame must be written before the pre-queued bulk backlog, got {first:?}"
        );
        for i in 0..3 {
            let next = read_frame(&mut ours, &mut buf)
                .await
                .expect("read")
                .expect("frame");
            assert!(
                matches!(&next, PeerMessage::Interest { filters } if filters == &vec![format!("bulk/{i}")]),
                "bulk frames follow in order, got {next:?}"
            );
        }

        // Dropping the lane senders ends the pump cleanly.
        drop(ctl_tx);
        drop(out_tx);
        pump_task.await.expect("pump task").expect("pump exits Ok");
    }

    /// ADR 0082 T4 (§3): with the node pool exhausted, an inbound peer `QoS` 0 publish
    /// or shared delivery is shed and counted, never queued for the hub, and the reader
    /// never waits. Retained and `QoS` 1 publishes still pass, uncharged. With credit,
    /// a `QoS` 0 forward carries its permit into the hub, which returns it on drop.
    #[test]
    fn peer_qos0_is_shed_when_the_pool_is_full_and_nothing_else_is() {
        use crate::ingress::OverloadMode;
        use mqtt_cluster::peer::WireAppProps;

        let publish = |qos: u8, retain: bool| PeerMessage::Publish {
            topic: "t".into(),
            payload: vec![0; 200],
            qos,
            retain,
            message_expiry: None,
            app: WireAppProps::default(),
        };
        let shared = |qos: u8| PeerMessage::SharedDeliver {
            client: "c1".into(),
            topic: "t".into(),
            payload: vec![0; 200],
            qos,
            message_expiry: None,
            app: WireAppProps::default(),
        };
        let (hub, mut rx) = mpsc::unbounded_channel();
        let (ctl, _ctl_rx) = mpsc::unbounded_channel::<PeerMessage>();
        let (bulk, _bulk_rx) = mpsc::unbounded_channel::<PeerMessage>();
        let (ctl, bulk) = (ctl.downgrade(), bulk.downgrade());
        let remote = NodeId("n2".into());
        let unit = 1 + 200 + crate::ingress::COMMAND_OVERHEAD; // topic + payload + overhead
        let credit = IngressCredit::new(4 * unit, 2 * unit, OverloadMode::Pause);
        let forward = |msg| forward_inbound(msg, &hub, &remote, None, Some(&credit), &ctl, &bulk);

        // With credit: the permit rides inside the command until the hub drops it.
        forward(publish(0, false));
        let cmd = rx.try_recv().expect("forwarded");
        assert!(matches!(
            cmd,
            HubCommand::RemotePublish {
                credit: Some(_),
                ..
            }
        ));
        assert_eq!(credit.in_use(), unit, "topic + payload + overhead");
        drop(cmd);
        assert_eq!(credit.in_use(), 0, "dispatching returns the credit");

        // The pool exhausted: QoS 0 is shed, and counted.
        let full = credit
            .try_acquire_pool(u32::try_from(4 * unit).unwrap())
            .unwrap();
        forward(publish(0, false));
        forward(shared(0));
        assert!(rx.try_recv().is_err(), "nothing queued for the hub");
        assert_eq!(credit.take_peer_shed(), 2);

        // A retained copy, QoS 1 and a QoS 1 shared delivery pass, uncharged.
        forward(publish(0, true));
        forward(publish(1, false));
        forward(shared(1));
        for _ in 0..3 {
            let cmd = rx.try_recv().expect("never shed");
            assert!(
                matches!(
                    cmd,
                    HubCommand::RemotePublish { credit: None, .. }
                        | HubCommand::RemoteSharedDeliver { credit: None, .. }
                ),
                "uncharged"
            );
        }
        assert_eq!(credit.take_peer_shed(), 0);
        drop(full);

        // No credit configured: nothing is charged or shed.
        forward_inbound(publish(0, false), &hub, &remote, None, None, &ctl, &bulk);
        assert!(matches!(
            rx.try_recv(),
            Ok(HubCommand::RemotePublish { credit: None, .. })
        ));
    }
}
