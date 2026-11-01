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
//!
//! Rule state (ADR 0085 D15): on a link that negotiated proto 14, the link's own task
//! also sends what changed in this node's `eventual` namespaces and applies what the
//! peer sends of its own, through a [`StateLink`]. Those frames are read and applied
//! here: they are never a hub command and wait in none of the hub's lanes.

use crate::conn::ConnPolicy;
use crate::hub::{HubCommand, HubTx, PeerOutbound};
use crate::ingress::{IngressCredit, IngressPermit};
use crate::rule_state::{StateLink, Store as RuleState};
use bytes::BytesMut;
use mqtt_cluster::durable_plane::DurablePlane;
use mqtt_cluster::peer::{self, PeerMessage};
use mqtt_cluster::stage_timing;
use mqtt_cluster::NodeId;
use mqtt_net::tls;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, info, warn};

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

/// The remote leaf certificate from an established peer TLS session, as DER.
fn peer_leaf(state: &tokio_rustls::rustls::CommonState) -> Option<&[u8]> {
    Some(state.peer_certificates()?.first()?.as_ref())
}

pub(crate) use crate::hub::LinkCert;

/// This node's own cluster-bus certificate's `notAfter`, Unix seconds — `i64::MAX` when
/// unknown. Set at startup and by every reload that commits a new peer certificate
/// ([`Reloader::attach_peer_tls`](crate::reload::Reloader::attach_peer_tls)); read at each
/// peer handshake (ADR 0088 D4).
pub type OwnCertEnd = Arc<std::sync::atomic::AtomicI64>;

/// An [`OwnCertEnd`] that knows of no end: for tests and setups whose own certificate
/// is not tracked.
#[must_use]
pub fn unknown_cert_end() -> OwnCertEnd {
    Arc::new(std::sync::atomic::AtomicI64::new(i64::MAX))
}

/// The wall clock in Unix seconds.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// When a refused link was last logged at WARN, Unix seconds — a node whose certificate
/// expired would otherwise log every redial.
static LAST_EXPIRY_REFUSAL_LOG: AtomicI64 = AtomicI64::new(i64::MIN);

/// Log a link refused for an expired certificate at WARN at most once a minute, and at
/// DEBUG otherwise.
fn log_expiry_refusal(now: i64, whose: &'static str) {
    let last = LAST_EXPIRY_REFUSAL_LOG.load(Ordering::Relaxed);
    let warn_now = now.saturating_sub(last) >= 60
        && LAST_EXPIRY_REFUSAL_LOG
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok();
    match (warn_now, whose == UNREADABLE_END) {
        (true, false) => warn!(
            whose,
            "peer link refused: a cluster-bus certificate has expired; no grace (ADR 0088 D4)"
        ),
        (true, true) => warn!(
            "peer link refused: the peer's certificate end (notAfter) cannot be read \
             (ADR 0088 D3)"
        ),
        (false, false) => debug!(
            whose,
            "peer link refused: a cluster-bus certificate has expired"
        ),
        (false, true) => debug!("peer link refused: the peer's certificate end cannot be read"),
    }
}

/// [`log_expiry_refusal`]'s `whose` for a peer leaf whose `notAfter` cannot be read.
const UNREADABLE_END: &str = "peer-end-unreadable";

/// Gate a freshly-handshaken peer link, on the ACCEPT side (a node dialing us) and on
/// the DIAL side (us dialing a node), and return what the link is admitted under, or
/// `Err(())` when it must be dropped:
///
/// - on the **live** cluster CRL (ADR 0040 T4): the same slot the gossip verifier reads
///   per datagram, so a republished CRL refuses new links immediately — the TLS
///   verifier alone would still admit a revoked node, since it checks chain and name,
///   not revocation;
/// - on **expiry** (ADR 0088 D4): the peer's leaf and this node's own certificate must
///   both be within `notAfter`. The TLS verifier has checked the peer's at a full
///   handshake already; this holds whatever the handshake was, and a node whose own
///   certificate has run out neither dials nor accepts.
fn admit_peer_link(
    state: &tokio_rustls::rustls::CommonState,
    crl: &crate::reload::SwimCrlSlot,
    own_end: &OwnCertEnd,
) -> Result<LinkCert, ()> {
    let leaf = peer_leaf(state);
    let serial = leaf.and_then(mqtt_auth::mtls::serial_from_cert);
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
    let now = unix_now();
    let own = Some(own_end.load(Ordering::Relaxed)).filter(|end| *end != i64::MAX);
    if own.is_some_and(|end| now > end) {
        log_expiry_refusal(now, "own");
        return Err(());
    }
    let peer = leaf.and_then(mqtt_auth::mtls::not_after_from_cert);
    // A leaf whose end cannot be read would give a link that never ends at the peer's
    // expiry: refused, as an expired one is (issue #1016). The TLS verifier parsed the same
    // certificate, so this is a certificate x509-parser reads differently, not a normal one.
    if leaf.is_some() && peer.is_none() {
        log_expiry_refusal(now, UNREADABLE_END);
        return Err(());
    }
    if peer.is_some_and(|end| now > end) {
        log_expiry_refusal(now, "peer");
        return Err(());
    }
    // A plaintext mesh has no certificates, and so no ends.
    let ends = if leaf.is_some() {
        crate::hub::LinkCertEnds { peer, own }
    } else {
        crate::hub::LinkCertEnds::default()
    };
    Ok(LinkCert { serial, ends })
}

/// [`admit_peer_link`] for the landing node's connection to a session's owner (ADR 0005):
/// a relay stream is a connection on the cluster bus, dialed under this node's
/// certificate, and is admitted and held to its certificates as a dialed link is
/// (issue #1016). `None` for the CRL: none configured; for the own end: not tracked.
pub(crate) fn admit_relay(
    state: &tokio_rustls::rustls::CommonState,
    crl: Option<&crate::reload::SwimCrlSlot>,
    own_end: Option<&OwnCertEnd>,
) -> Result<LinkCert, ()> {
    let no_crl = crate::reload::SwimCrlSlot::default();
    let unknown = unknown_cert_end();
    admit_peer_link(state, crl.unwrap_or(&no_crl), own_end.unwrap_or(&unknown))
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
    /// When this node's own certificate ends (ADR 0088 D4): a link is admitted only while
    /// it has not, and ends when it does. Shared with the reloader.
    pub own_cert_end: OwnCertEnd,
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
/// The longest delay between attempts while the peer is held `Dead` (ADR 0087 D5): each
/// attempt that serves no link doubles the delay from [`REDIAL_DELAY`] up to this.
const HELD_DEAD_REDIAL_MAX: Duration = Duration::from_secs(5);
/// How long the TCP connect of one attempt may take, for a membership-driven dialer
/// ([`dial_member`]) — whatever membership says of the peer when the attempt starts.
///
/// A host that is gone often answers nothing at all, and the operating system keeps
/// one such attempt pending for minutes, its retransmissions spaced ever further
/// apart. The attempt that matters most is the one already under way when a verdict
/// lands: the link dropped while the peer was still in good standing, the redial sits
/// in those retransmissions, and — since a verdict no longer ends the dialer — it is
/// that attempt a returning peer would have to wait out.
const MEMBER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a dialer stays away from an address where a node other than its member
/// answered ([`LinkOutcome::WrongNode`]), unless the member's standing changes first:
/// a `Dead` verdict lands, or one is withdrawn. (A further `Alive` for a member
/// already in good standing at this same address changes nothing, and does not end
/// the wait.)
///
/// The dialer hangs up on that node, but the node cannot know why it was dialed: its
/// accept path takes the connection for this node's link to IT, registers it — in
/// place of the real one, which it closes — and only then sees the hang-up. So every
/// attempt at a member's stale address costs the pair that really lives there one
/// link flap. An `Alive` that names the member elsewhere replaces this dialer, and
/// a change of standing ends the wait; this is what bounds the flaps when neither
/// comes. (Refusing before the other node registers needs the expected node id in
/// the `Hello`, which is a wire change — ADR 0087 T5's protocol step is the place.)
const WRONG_NODE_REDIAL_DELAY: Duration = Duration::from_mins(1);
/// How long each half of a dialed link's handshake may take once TCP is connected: the
/// TLS handshake, and then the exchange of `Hello`s. For every dialer, whatever
/// membership says of its peer.
///
/// A dialer has exactly one attempt under way and does nothing else until it returns.
/// A peer whose kernel completed the connection and whose host then stopped — hung, or
/// hard-reset before it could say so — answers nothing and sends no reset, so without
/// a deadline that attempt never returns: this node, the only side that may dial the
/// pair, would not link the peer again until it restarted itself. (While a `Dead`
/// verdict still stopped the dialer, the `Alive` that followed started a fresh one and
/// hid this; a static peer's dialer has always been exposed to it.) A link that is
/// established is another matter and needs no deadline here: this node writes to it,
/// and a host that has gone or come back answers those writes with a reset.
const DIAL_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// What membership says about the peer a dialer is aimed at (ADR 0087 D5). The link
/// manager ([`crate::cluster`]) writes it; the dialer reads it between attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialStanding {
    /// A member in good standing: redial every [`REDIAL_DELAY`], for ever.
    Member,
    /// Held `Dead`. The verdict may be false, and a link is the only way this node
    /// finds out on the data path — so the dialer keeps dialing, backing off, until
    /// membership confirms the peer gone ([`Gone`](Self::Gone)) or withdraws the
    /// verdict.
    HeldDead,
    /// Confirmed gone (ADR 0087 D2): the tombstone was pruned with the parting
    /// greeting unanswered, or a new generation of the id joined. The dialer dials
    /// no more — a link it is serving is served to its end — and waits for this
    /// standing to change: the next `Alive` sets it dialing again.
    Gone,
}

/// The peer a membership-driven dialer is aimed at ([`dial_member`]).
#[derive(Debug)]
pub struct DialTarget {
    /// The node the dialed address belongs to. A link whose `Hello` names any other
    /// node is dropped: a dialer that outlives a `Dead` verdict may be dialing an
    /// address its peer no longer holds, and whoever answers there is not the node
    /// this link is for.
    pub node: NodeId,
    /// Membership's standing of that node, kept current by the link manager.
    pub standing: tokio::sync::watch::Receiver<DialStanding>,
}

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
    /// Wall time inside each batch's socket `write_all` + `flush`, pending included,
    /// and nothing else (not the encode, not the batching yield): a write blocked on
    /// TCP backpressure shows here and not in `busy_ns`, while the write's own CPU
    /// (TLS, syscall) is in both. Added when the write completes, so a write still
    /// pending is not counted yet.
    pub write_wait_ns: AtomicU64,
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
    hub: HubTx,
    tls: Option<PeerTls>,
    client_policy: Option<Arc<ConnPolicy>>,
    plane: Option<DurablePlane>,
) {
    serve_listener_with_ingress(listener, local, hub, tls, client_policy, plane, None, None).await;
}

/// [`serve_listener`], with the node's ingress credit charged for inbound peer `QoS` 0
/// publishes (ADR 0082 T4; `None` charges nothing), and with the store whose
/// `eventual` namespaces the links carry (ADR 0085; `None`: the links carry no rule
/// state).
#[allow(clippy::too_many_arguments)]
pub async fn serve_listener_with_ingress(
    listener: TcpListener,
    local: NodeId,
    hub: HubTx,
    tls: Option<PeerTls>,
    client_policy: Option<Arc<ConnPolicy>>,
    plane: Option<DurablePlane>,
    ingress: Option<Arc<IngressCredit>>,
    state: Option<Arc<RuleState>>,
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
                let state = state.clone();
                tokio::spawn(async move {
                    let _ = stream.set_nodelay(true);
                    // Read the CURRENT acceptor per accept (ADR 0040 T4): a reload's
                    // rebuilt material is served on the next handshake. (Bound first:
                    // the watch guard must not live across an await.)
                    let acceptor = tls.as_ref().map(|t| t.acceptor.borrow().clone());
                    let result = match (&tls, acceptor) {
                        (Some(t), Some(acceptor)) => match acceptor.accept(stream).await {
                            Ok(s) => {
                                let Ok(cert) =
                                    admit_peer_link(s.get_ref().1, &t.gossip_crl, &t.own_cert_end)
                                else {
                                    // Revoked or expired: fail closed (ADR 0040 T4, 0088 D4).
                                    return;
                                };
                                let expected_cn = peer_cert_cn(s.get_ref().1);
                                handle(
                                    s,
                                    local,
                                    hub,
                                    false,
                                    expected_cn,
                                    None,
                                    cert,
                                    policy,
                                    plane,
                                    ingress,
                                    state,
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
                                stream,
                                local,
                                hub,
                                false,
                                None,
                                None,
                                LinkCert::default(),
                                policy,
                                plane,
                                ingress,
                                state,
                            )
                            .await
                        }
                    };
                    if let Err(e) = result {
                        debug!(error = %e, "inbound peer handshake failed");
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
    hub: HubTx,
    tls: Option<PeerTls>,
    plane: Option<DurablePlane>,
) {
    dial_forever_with_ingress(addr, local, hub, tls, plane, None, None).await;
}

/// [`dial_forever`], with the node's ingress credit charged for inbound peer `QoS` 0
/// publishes (ADR 0082 T4; `None` charges nothing), and with the store whose
/// `eventual` namespaces the link carries (ADR 0085; `None`: it carries no rule state).
pub async fn dial_forever_with_ingress(
    addr: String,
    local: NodeId,
    hub: HubTx,
    tls: Option<PeerTls>,
    plane: Option<DurablePlane>,
    ingress: Option<Arc<IngressCredit>>,
    state: Option<Arc<RuleState>>,
) {
    dial(
        addr,
        local,
        hub,
        tls,
        plane,
        ingress,
        state,
        None,
        DIAL_HANDSHAKE_TIMEOUT,
    )
    .await;
}

/// Dial the member `target` at `addr` and keep the link up, as
/// [`dial_forever_with_ingress`] does — and keep doing so while membership holds the
/// member `Dead` (ADR 0087 D5).
///
/// A `Dead` verdict can be false: the peer is alive, its clients are connected and
/// the network works. Stopping the dialer at the verdict made such a verdict last on
/// the data path for as long as membership held it (30 s) when the node that was
/// wrong happened to own the link, and about a second when the other node did. So
/// the dialer keeps going, and what the verdict changes is its pace and its end:
///
/// - each attempt that serves no link doubles the delay, from [`REDIAL_DELAY`] to
///   [`HELD_DEAD_REDIAL_MAX`], so a peer that is really gone costs one connection
///   attempt every five to ten seconds, not two a second;
/// - an attempt is bounded at every step, verdict or not — the TCP connect by
///   [`MEMBER_CONNECT_TIMEOUT`], the TLS handshake and the `Hello` exchange by
///   [`DIAL_HANDSHAKE_TIMEOUT`] each — and a connect still under way when
///   membership changes its mind about the peer is dropped for a fresh one;
/// - an address where another node answers is left alone for
///   [`WRONG_NODE_REDIAL_DELAY`], or until the member's standing changes;
/// - once membership confirms the peer gone ([`DialStanding::Gone`], ADR 0087 D2),
///   the dialer dials no more. It does not end: it waits for its standing to
///   change, and the next `Alive` sets it dialing again. (A dialer that ended
///   itself here could do so in the instant the link manager, seeing it still
///   running, handed it that `Alive` — and the member would be alive with no dialer
///   and no link.)
///
/// A link served under the verdict is served like any other and resets the delay.
#[allow(clippy::too_many_arguments)] // `dial_forever_with_ingress`, plus the target
pub async fn dial_member(
    addr: String,
    local: NodeId,
    hub: HubTx,
    tls: Option<PeerTls>,
    plane: Option<DurablePlane>,
    ingress: Option<Arc<IngressCredit>>,
    state: Option<Arc<RuleState>>,
    target: DialTarget,
) {
    dial(
        addr,
        local,
        hub,
        tls,
        plane,
        ingress,
        state,
        Some(target),
        DIAL_HANDSHAKE_TIMEOUT,
    )
    .await;
}

/// What a dialed link's handshake is held to ([`handle`]); the accept side has none.
struct Dialed<'a> {
    /// The member the dialer was aimed at ([`DialTarget::node`]); `None` for a
    /// static peer, whose node id is whatever answers.
    expect: Option<&'a NodeId>,
    /// How long the exchange of `Hello`s may take ([`DIAL_HANDSHAKE_TIMEOUT`]).
    hello_within: Duration,
}

/// How one dial attempt ended, as the pacing of the next one needs it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Attempt {
    /// A link was registered with the hub and has since closed.
    Served,
    /// No link: the connection, the TLS handshake or the peer handshake failed.
    Failed,
    /// No link: a node other than the member this dialer is aimed at answered.
    WrongNode,
}

/// Whether the dialer's target is held `Dead` (or confirmed gone); `false` for a
/// member in good standing and for a static peer, which has no membership behind it.
///
/// Reading marks the standing seen, so the dialer's wait between attempts is cut
/// short only by a change that happens during that wait.
fn held_dead(target: Option<&mut DialTarget>) -> bool {
    target.is_some_and(|t| *t.standing.borrow_and_update() != DialStanding::Member)
}

/// The most the moves of one renewal's links are spread over (issue #1016).
pub(crate) const MOVE_SPREAD_MAX_MS: i64 = 5 * 60 * 1000;
/// The most a link's last planned move keeps clear of its end: O3's minute of clock
/// difference, since the peer ends the link by its own clock.
const MOVE_MARGIN_MAX_MS: i64 = 60 * 1000;

/// When a link's move to later-ending certificates is tried (issue #1016).
///
/// A TLS session keeps the certificates of its handshake, so a link ends at the earlier of
/// the two it was made with (ADR 0088 D3), whatever has been renewed since. Nodes minted
/// together share a `notAfter`, so without a move every link made before a renewal would
/// end in the same second and every node would redial all of its peers at once. Instead the
/// node that dials the link (the smaller id; the other side never dials it) makes a second
/// link alongside it, and that link replaces the old one in the hub
/// ([`HubCommand::PeerConnected`]) while the old one is let go quietly. A move is tried:
///
/// - **after this node's own renewal** — a committed reload stores a later own end than
///   the link was made with: at a uniformly random moment within half the time left
///   before the end window's margin, at most [`MOVE_SPREAD_MAX_MS`], so one node's links
///   move one at a time and not in the same second;
/// - **in the end window** — the last quarter of the link's life, at most
///   [`MOVE_SPREAD_MAX_MS`], ending [`MOVE_MARGIN_MAX_MS`] (at most an eighth of the life)
///   before the end: at a uniformly random moment in it, and again every tenth of it
///   until the end. This is what moves a link whose *peer* renewed (this node cannot see
///   that without a handshake) and a link whose first try came before the peer had
///   renewed.
///
/// A try that does not get certificates ending later than the link's is closed before
/// the peer handshake, and nothing changes. The hard end of ADR 0088 D3 stays the backstop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MovePlan {
    /// The earliest end of the link's certificates, Unix milliseconds.
    pub(crate) end_ms: i64,
    /// When the link was made, Unix milliseconds.
    pub(crate) made_ms: i64,
}

impl MovePlan {
    /// The end window's length and the margin it keeps before the end.
    fn window(self) -> (i64, i64) {
        let life = (self.end_ms - self.made_ms).max(0);
        (
            MOVE_SPREAD_MAX_MS.min(life / 4),
            MOVE_MARGIN_MAX_MS.min(life / 8),
        )
    }

    /// When the end window opens.
    fn window_opens(self) -> i64 {
        let (spread, margin) = self.window();
        self.end_ms - margin - spread
    }

    /// The first try in the end window, `jitter` in `[0, 1)` placing it.
    pub(crate) fn in_end_window(self, jitter: f64) -> i64 {
        let (spread, _) = self.window();
        self.window_opens() + scale(spread, jitter)
    }

    /// The try after a renewal of this node's certificate seen at `now`.
    pub(crate) fn after_renewal(self, now: i64, jitter: f64) -> i64 {
        let (_, margin) = self.window();
        let room = self.end_ms - margin - now;
        if room <= 0 {
            return now;
        }
        now + scale(MOVE_SPREAD_MAX_MS.min(room / 2), jitter)
    }

    /// The next try after one at `now` that moved nothing.
    fn retry(self, now: i64, jitter: f64) -> i64 {
        let opens = self.window_opens();
        if now < opens {
            return self.in_end_window(jitter);
        }
        let (spread, _) = self.window();
        now + (spread / 10).max(1000)
    }
}

/// `span` scaled by `jitter` in `[0, 1)`.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)] // ms, < 2^53
fn scale(span: i64, jitter: f64) -> i64 {
    (span as f64 * jitter.clamp(0.0, 1.0)) as i64
}

/// A uniformly random number in `[0, 1)`, from the process's hasher keys.
#[allow(clippy::cast_precision_loss)] // 53 bits, exactly
pub(crate) fn jitter() -> f64 {
    use std::hash::BuildHasher;
    static DRAWS: AtomicU64 = AtomicU64::new(0);
    let bits = std::hash::RandomState::new().hash_one(DRAWS.fetch_add(1, Ordering::Relaxed));
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

/// The wall clock in Unix milliseconds.
fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// One dialed link's moves to renewed certificates ([`MovePlan`]): what the link holds,
/// what renewal of this node's own certificate has been acted on, and the next try.
struct Mover {
    /// The ends of the link being served.
    ends: crate::hub::LinkCertEnds,
    made_ms: i64,
    /// This node's own end that a try has already been planned for.
    own_seen: Option<i64>,
    /// The next try, Unix milliseconds; `None` until planned.
    next_ms: Option<i64>,
}

impl Mover {
    fn new(ends: crate::hub::LinkCertEnds) -> Self {
        Self {
            ends,
            made_ms: unix_now_ms(),
            own_seen: ends.own,
            next_ms: None,
        }
    }

    fn plan(&self) -> Option<MovePlan> {
        Some(MovePlan {
            end_ms: self.ends.earliest()?.saturating_mul(1000),
            made_ms: self.made_ms,
        })
    }

    /// When the next try is due, at `now`, with this node's own end now `own` — or
    /// `None` when the link has nothing to move to (no end, or past it: the hard end's).
    fn due(&mut self, now: i64, own: Option<i64>) -> Option<i64> {
        let plan = self.plan()?;
        if now > plan.end_ms {
            return None;
        }
        // This node's certificate was renewed since the link formed: a move can be had
        // now, if the peer's is not what holds the end.
        if own.is_some_and(|own| Some(own) > self.own_seen) {
            self.own_seen = own;
            let at = plan.after_renewal(now, jitter());
            self.next_ms = Some(self.next_ms.map_or(at, |next| next.min(at)));
        }
        Some(
            *self
                .next_ms
                .get_or_insert_with(|| plan.in_end_window(jitter())),
        )
    }

    /// A try at `now` got nothing that ends later.
    fn missed(&mut self, now: i64) {
        self.next_ms = self.plan().map(|plan| plan.retry(now, jitter()));
    }

    /// Wait for the next try and make it: a connection to `addr` with certificates that end
    /// later than the link's, past the TLS handshake and admission, not yet a link. Never
    /// returns while the link has nothing to move to.
    async fn next_link(
        &mut self,
        addr: &str,
        name: &tokio_rustls::rustls::pki_types::ServerName<'static>,
        t: &PeerTls,
        handshake_within: Duration,
    ) -> (tokio_rustls::client::TlsStream<TcpStream>, LinkCert) {
        loop {
            let own = Some(t.own_cert_end.load(Ordering::Relaxed)).filter(|e| *e != i64::MAX);
            let now = unix_now_ms();
            let Some(due) = self.due(now, own) else {
                // Nothing to move to: wait for a renewal of this node's own certificate,
                // which `due` re-reads.
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            if now < due {
                // Looked at again each second: a renewal can move the try earlier.
                let wait = u64::try_from((due - now).min(1000)).unwrap_or(1000);
                tokio::time::sleep(Duration::from_millis(wait)).await;
                continue;
            }
            match try_renewed_link(addr, name, t, handshake_within, self.ends).await {
                Some(found) => return found,
                None => self.missed(unix_now_ms()),
            }
        }
    }
}

/// One try at a link whose certificates end later than `ends`: connect, handshake,
/// admit, compare. `None` when any of it fails or nothing ends later; the connection is
/// then closed before the peer handshake, which the peer reads as an ended connection.
async fn try_renewed_link(
    addr: &str,
    name: &tokio_rustls::rustls::pki_types::ServerName<'static>,
    t: &PeerTls,
    handshake_within: Duration,
    ends: crate::hub::LinkCertEnds,
) -> Option<(tokio_rustls::client::TlsStream<TcpStream>, LinkCert)> {
    let stream = tokio::time::timeout(MEMBER_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .ok()?
        .ok()?;
    let _ = stream.set_nodelay(true);
    let connector = t.connector.borrow().clone();
    let secured = tokio::time::timeout(handshake_within, connector.connect(name.clone(), stream))
        .await
        .ok()?
        .ok()?;
    let cert = admit_peer_link(secured.get_ref().1, &t.gossip_crl, &t.own_cert_end).ok()?;
    let later = cert.ends.earliest() > ends.earliest();
    debug!(
        %addr,
        old_end = ?ends.earliest(),
        new_end = ?cert.ends.earliest(),
        later,
        "tried a peer link on renewed certificates"
    );
    later.then_some((secured, cert))
}

/// A dialed link's future, boxed so a link and the one replacing it can be held side by
/// side ([`serve_dialed_tls`]).
type LinkFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<LinkOutcome, std::io::Error>> + Send + 'a>,
>;

/// Serve a dialed mTLS link, and move it to renewed certificates before its end
/// ([`MovePlan`], issue #1016): make-before-break. The new link is made alongside, its
/// registration replaces the old one in the hub — which keeps the old one open a few
/// seconds for what was in flight on it and then closes it — and from then on the new
/// one is the link served here. A new link that never registers (its peer handshake
/// failed) leaves the old one the link. Returns how the link served last ended.
#[allow(clippy::too_many_arguments)] // `dial`'s handles, passed through
async fn serve_dialed_tls<'a>(
    first: tokio_rustls::client::TlsStream<TcpStream>,
    cert: LinkCert,
    addr: &str,
    name: &tokio_rustls::rustls::pki_types::ServerName<'static>,
    t: &PeerTls,
    local: &'a NodeId,
    hub: &'a HubTx,
    plane: Option<&'a DurablePlane>,
    ingress: Option<&'a Arc<IngressCredit>>,
    state: Option<&'a Arc<RuleState>>,
    expect: Option<&'a NodeId>,
    handshake_within: Duration,
) -> Result<LinkOutcome, std::io::Error> {
    let serve = |s: tokio_rustls::client::TlsStream<TcpStream>, cert: LinkCert| -> LinkFuture<'a> {
        let expected_cn = peer_cert_cn(s.get_ref().1);
        Box::pin(handle(
            s,
            local.clone(),
            hub.clone(),
            true,
            expected_cn,
            Some(Dialed {
                expect,
                hello_within: handshake_within,
            }),
            cert,
            None,
            plane.cloned(),
            ingress.cloned(),
            state.cloned(),
        ))
    };
    let mut mover = Mover::new(cert.ends);
    let mut current = serve(first, cert);
    // The link being replaced, with what the mover held for it, until it has closed.
    let mut retiring: Option<(LinkFuture<'a>, Mover)> = None;
    loop {
        tokio::select! {
            outcome = &mut current => {
                // A replacement whose peer handshake failed never became the link: the old
                // one still is, and a later try may do better.
                if !matches!(outcome, Ok(LinkOutcome::Served)) {
                    if let Some((old, mut was)) = retiring.take() {
                        debug!(%addr, "a renewed peer link did not form; keeping the old one");
                        was.missed(unix_now_ms());
                        current = old;
                        mover = was;
                        continue;
                    }
                }
                return outcome;
            }
            () = async {
                if let Some((old, _)) = retiring.as_mut() {
                    let _ = old.await;
                }
            }, if retiring.is_some() => {
                retiring = None;
            }
            // One move at a time: a link whose replacement's own window opens while the
            // old one is still retiring (only with certificates of seconds) waits for it.
            (s, cert) = mover.next_link(addr, name, t, handshake_within), if retiring.is_none() => {
                info!(
                    %addr,
                    old_end = ?mover.ends.earliest(),
                    new_end = ?cert.ends.earliest(),
                    "moving a peer link to renewed certificates (make-before-break)"
                );
                let was = std::mem::replace(&mut mover, Mover::new(cert.ends));
                let old = std::mem::replace(&mut current, serve(s, cert));
                retiring = Some((old, was));
            }
        }
    }
}

/// The dial loop behind [`dial_forever_with_ingress`] (`target` is `None`: a static
/// peer, redialed at a fixed pace for ever) and [`dial_member`]. `handshake_within`
/// is [`DIAL_HANDSHAKE_TIMEOUT`] everywhere but in tests.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)] // one loop: connect, TLS, serve, pace the next attempt
async fn dial(
    addr: String,
    local: NodeId,
    hub: HubTx,
    tls: Option<PeerTls>,
    plane: Option<DurablePlane>,
    ingress: Option<Arc<IngressCredit>>,
    state: Option<Arc<RuleState>>,
    mut target: Option<DialTarget>,
    handshake_within: Duration,
) {
    // An undialable name is permanent; retrying would only spin.
    let server_name = match tls.as_ref().map(|_| tls::server_name(&addr)).transpose() {
        Ok(name) => name,
        Err(e) => {
            warn!(%addr, error = %e, "cannot dial peer over TLS; giving up");
            return;
        }
    };
    let mut delay = REDIAL_DELAY;
    // Said once per dialer, not once per attempt.
    let mut wrong_node_logged = false;
    loop {
        // A peer confirmed gone is dialed no more (ADR 0087 D2). The dialer does not
        // end — the link manager hands an `Alive` to a dialer it sees running, so one
        // that ended itself here could miss the one event that would have started
        // another. It waits: the standing read here is marked seen, so whatever the
        // link manager writes from now on wakes it, and it dials again.
        if let Some(target) = target.as_mut() {
            if *target.standing.borrow_and_update() == DialStanding::Gone {
                info!(%addr, "peer confirmed gone; dialing stops until membership holds it alive again");
                if target.standing.changed().await.is_err() {
                    return; // the link manager is gone
                }
                delay = REDIAL_DELAY;
                continue;
            }
        }
        let connected = match target.as_mut() {
            // A static peer: as ever, for as long as the operating system tries.
            None => TcpStream::connect(&addr).await,
            Some(target) => {
                // What membership has said up to here is taken as read; what it says
                // while this connect is under way cuts it short. A connect begun for
                // a member in good standing can sit in SYN retransmissions when a
                // verdict lands — and when the verdict is withdrawn again — and the
                // peer that has just returned must not wait for the next one.
                target.standing.borrow_and_update();
                let connecting =
                    tokio::time::timeout(MEMBER_CONNECT_TIMEOUT, TcpStream::connect(&addr));
                tokio::pin!(connecting);
                let timed_out = || Err(std::io::ErrorKind::TimedOut.into());
                tokio::select! {
                    connected = &mut connecting => connected.unwrap_or_else(|_| timed_out()),
                    changed = target.standing.changed() => match changed {
                        Ok(()) => continue,
                        // The link manager is gone; it aborts this task as it goes.
                        Err(_) => connecting.await.unwrap_or_else(|_| timed_out()),
                    },
                }
            }
        };
        let attempt = match connected {
            Ok(stream) => {
                debug!(%addr, "dialed peer");
                let _ = stream.set_nodelay(true);
                let expected_node = target.as_ref().map(|t| &t.node);
                let dialed = || Dialed {
                    expect: expected_node,
                    hello_within: handshake_within,
                };
                let outcome = match (&tls, &server_name) {
                    (Some(t), Some(name)) => {
                        // Read the CURRENT connector per dial (ADR 0040 T4).
                        let connector = t.connector.borrow().clone();
                        let secured = tokio::time::timeout(
                            handshake_within,
                            connector.connect(name.clone(), stream),
                        )
                        .await
                        .unwrap_or_else(|_| Err(std::io::ErrorKind::TimedOut.into()));
                        match secured {
                            // Revoked or expired (ADR 0040 T4, 0088 D4): fail closed,
                            // but keep redialing — a new CRL or a renewed certificate
                            // heals.
                            // Served with its moves to renewed certificates (#1016).
                            Ok(s) => {
                                match admit_peer_link(s.get_ref().1, &t.gossip_crl, &t.own_cert_end)
                                {
                                    Ok(cert) => {
                                        serve_dialed_tls(
                                            s,
                                            cert,
                                            &addr,
                                            name,
                                            t,
                                            &local,
                                            &hub,
                                            plane.as_ref(),
                                            ingress.as_ref(),
                                            state.as_ref(),
                                            expected_node,
                                            handshake_within,
                                        )
                                        .await
                                    }
                                    Err(()) => Ok(LinkOutcome::Closed),
                                }
                            }
                            Err(e) => {
                                debug!(%addr, error = %e, "peer mTLS handshake failed; will retry");
                                Ok(LinkOutcome::Closed)
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
                            Some(dialed()),
                            LinkCert::default(),
                            None,
                            plane.clone(),
                            ingress.clone(),
                            state.clone(),
                        )
                        .await
                    }
                };
                match outcome {
                    Ok(LinkOutcome::Redundant) => {
                        debug!(%addr, "not the owning side for this peer; stopping dial");
                        return;
                    }
                    Ok(LinkOutcome::Served) => Attempt::Served,
                    Ok(LinkOutcome::Closed) => Attempt::Failed,
                    Ok(LinkOutcome::WrongNode(answered)) => {
                        if !std::mem::replace(&mut wrong_node_logged, true) {
                            warn!(
                                %addr,
                                expected = %expected_node.map_or("", |n| n.0.as_str()),
                                answered = %answered.0,
                                "the node answering at a member's routing address is not that \
                                 member; no link (the address has changed hands — the dialer \
                                 stays away for a minute, or until the member's standing changes)"
                            );
                        }
                        Attempt::WrongNode
                    }
                    Err(e) => {
                        debug!(%addr, error = %e, "outbound peer handshake failed");
                        Attempt::Failed
                    }
                }
            }
            Err(e) => {
                debug!(%addr, error = %e, "peer dial failed; will retry");
                Attempt::Failed
            }
        };
        if attempt == Attempt::WrongNode {
            // Each attempt here flaps the link of the pair that lives at this
            // address ([`WRONG_NODE_REDIAL_DELAY`]): stay away until the member's
            // standing changes (a verdict lands or is withdrawn), or for a minute.
            if let Some(standing) = target.as_mut().map(|t| &mut t.standing) {
                standing.borrow_and_update();
                tokio::select! {
                    () = tokio::time::sleep(WRONG_NODE_REDIAL_DELAY) => {}
                    changed = standing.changed() => {
                        if changed.is_err() {
                            return; // the link manager is gone
                        }
                    }
                }
            }
            // Then the ordinary pacing below.
        }
        match (held_dead(target.as_mut()), attempt) {
            // A member in good standing, or a static peer: the fixed pace; and a
            // link was served under the verdict: the peer was there a moment ago.
            (false, _) | (true, Attempt::Served) => delay = REDIAL_DELAY,
            (true, Attempt::Failed | Attempt::WrongNode) => {
                delay = (delay * 2).min(HELD_DEAD_REDIAL_MAX);
            }
        }
        // Wait out the delay — or less, when membership changes its mind about the
        // peer: a withdrawn verdict must not find the dialer asleep in its backoff.
        match target.as_mut().map(|t| &mut t.standing) {
            Some(standing) => {
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    changed = standing.changed() => {
                        let now_member = *standing.borrow() == DialStanding::Member;
                        match changed {
                            // The verdict was withdrawn: dial now, at the ordinary pace.
                            Ok(()) if now_member => delay = REDIAL_DELAY,
                            // A verdict arrived while this dialer was between
                            // attempts: the next one keeps the ordinary pace.
                            Ok(()) => {
                                delay = REDIAL_DELAY;
                                tokio::time::sleep(delay).await;
                            }
                            // The link manager is gone, and aborts its dialers as it
                            // goes; until that lands, do not spin on a closed channel.
                            Err(_) => tokio::time::sleep(delay).await,
                        }
                    }
                }
            }
            None => tokio::time::sleep(delay).await,
        }
    }
}

/// Why a peer link ended, so the dialer knows whether to retry.
enum LinkOutcome {
    /// The link was registered with the hub, served, and then closed; the dialer
    /// should re-establish it.
    Served,
    /// The handshake ended the connection before any link existed (no `Hello`, an
    /// incompatible protocol range, a node id its certificate does not attest); the
    /// dialer tries again.
    Closed,
    /// This direction was the redundant one (deduped by the tie-break); the dialer
    /// should stop, because the peer owns the single link.
    Redundant,
    /// The dialer was aimed at one member and this other node answered
    /// ([`DialTarget::node`]). No link; the dialer tries again.
    WrongNode(NodeId),
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
    hub: HubTx,
    initiated: bool,
    expected_cn: Option<String>,
    dialed: Option<Dialed<'_>>,
    cert: LinkCert,
    client_policy: Option<Arc<ConnPolicy>>,
    plane: Option<DurablePlane>,
    ingress: Option<Arc<IngressCredit>>,
    rule_state: Option<Arc<RuleState>>,
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
        let hellos = async {
            write_frame(
                &mut wh,
                &PeerMessage::Hello {
                    node_id: local.0.clone(),
                    proto_min: peer::PROTO_MIN,
                    proto_max: peer::PROTO_MAX,
                },
            )
            .await?;
            read_frame(&mut rh, &mut buf).await
        };
        // A dialer waits for the peer's `Hello` no longer than its deadline: a peer
        // that accepted the connection and then went silent must not hold the
        // pair's only dialer for good ([`DIAL_HANDSHAKE_TIMEOUT`]).
        let answer = match dialed.as_ref().map(|d| d.hello_within) {
            Some(within) => tokio::time::timeout(within, hellos)
                .await
                .unwrap_or_else(|_| Err(std::io::ErrorKind::TimedOut.into()))?,
            None => hellos.await?,
        };
        match answer {
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
                // Held to the certificates this connection was admitted under, as a peer
                // link is: at their end and at revocation (issue #1016).
                let relay = Some(cert).filter(|c| !c.is_empty());
                crate::conn::serve_proxied(rh, wh, None, identity, policy, hub, buf, via, relay)
                    .await;
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

    // A membership-driven dialer knows which member it dialed (ADR 0087 D5). Whoever
    // else answers at that address — it has changed hands since membership last
    // spoke — is a node this dialer has no business linking: that node's link is
    // another dialer's, and a second one would replace it in the hub, the two then
    // taking turns closing each other.
    if let Some(expected) = dialed.as_ref().and_then(|d| d.expect) {
        if expected != &remote {
            return Ok(LinkOutcome::WrongNode(remote));
        }
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
            cert_serial: cert.serial,
            cert_ends: cert.ends,
            proto,
        })
        .is_err()
    {
        return Ok(LinkOutcome::Closed);
    }

    // Rule state travels only on a link that negotiated it (ADR 0085 D15): a peer on
    // proto 13 holds none, and would drop the link on a frame it does not know.
    let mut rule_state = rule_state
        .filter(|_| proto >= peer::PROTO_RULE_STATE)
        .map(|store| StateLink::new(store, &local.0, &remote.0));
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
            conn_id,
            &mut ctl_rx,
            &mut out_rx,
            &reply_ctl,
            &reply_bulk,
            &depth,
            &stats,
            plane.as_ref(),
            ingress.as_deref(),
            rule_state.as_mut(),
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
    // The link's own end is said here, as it was by each caller, so that an `Err`
    // out of this function is always a handshake that never produced a link.
    if let Err(e) = &result {
        if initiated {
            debug!(peer = %remote.0, error = %e, "outbound peer link ended");
        } else {
            debug!(peer = %remote.0, error = %e, "inbound peer link ended");
        }
    }
    let _ = hub.send(HubCommand::PeerDisconnected {
        node: remote,
        conn_id,
    });
    Ok(LinkOutcome::Served)
}

#[allow(clippy::too_many_arguments)] // one straight-line link loop; a struct would hide the shape
async fn pump<R, W>(
    rh: &mut R,
    wh: &mut W,
    buf: &mut BytesMut,
    hub: &HubTx,
    remote: &NodeId,
    conn_id: u64,
    ctl_rx: &mut mpsc::UnboundedReceiver<PeerMessage>,
    out_rx: &mut mpsc::UnboundedReceiver<PeerMessage>,
    reply_ctl: &mpsc::WeakUnboundedSender<PeerMessage>,
    reply_bulk: &mpsc::WeakUnboundedSender<PeerMessage>,
    depth: &std::sync::atomic::AtomicUsize,
    stats: &LinkStats,
    plane: Option<&DurablePlane>,
    ingress: Option<&IngressCredit>,
    mut rule_state: Option<&mut StateLink>,
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
        // other side's acks. Rule state comes last: it is sent when nothing else is
        // waiting, and gets one frame after each bulk batch when something always is.
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
                        let n = write_batch(wh, ctl_buf, &mut ctl_stamps, &msg, ctl_rx, remote, stats).await?;
                        stats.frames_out.fetch_add(n, Ordering::Relaxed);
                    }
                    None => return Ok(()), // taken over or hub gone
                }
            }
            inbound = read_frame(rh, buf) => {
                match inbound? {
                    None => return Ok(()), // peer closed
                    // Rule state is applied here, by the link's reader, to the store
                    // (ADR 0085 D15): a map operation per key, and no hub command, so
                    // it waits in no lane and can neither delay one nor be shed with
                    // one (ADR 0082). Without a store there is nothing to apply it to.
                    Some(
                        msg @ (PeerMessage::StateKeys { .. }
                        | PeerMessage::StateDigest { .. }
                        | PeerMessage::StateWant { .. }
                        | PeerMessage::StateExpiry { .. }),
                    ) => {
                        stats.frames_in.fetch_add(1, Ordering::Relaxed);
                        if let Some(link) = rule_state.as_mut() {
                            link.receive(msg);
                        }
                    }
                    Some(msg) => {
                        stats.frames_in.fetch_add(1, Ordering::Relaxed);
                        forward_inbound(msg, hub, remote, conn_id, plane, ingress, reply_ctl, reply_bulk);
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
                        let n = write_batch(wh, out_buf, &mut out_stamps, &msg, out_rx, remote, stats).await?;
                        stats.frames_out.fetch_add(n, Ordering::Relaxed);
                        // A lane that never empties must not keep state waiting for
                        // good: one frame of it for each batch.
                        if let Some(frame) = rule_state.as_mut().and_then(|link| link.try_next()) {
                            write_state(wh, out_buf, &frame, remote, stats).await?;
                        }
                    }
                    None => return Ok(()), // taken over or hub gone
                }
            }
            frame = next_state(rule_state.as_deref_mut()) => {
                write_state(wh, out_buf, &frame, remote, stats).await?;
            }
        }
    }
}

/// The link's next rule-state frame, when it has one; never, on a link that carries
/// none.
async fn next_state(link: Option<&mut StateLink>) -> PeerMessage {
    match link {
        Some(link) => link.next().await,
        None => std::future::pending().await,
    }
}

/// Write one rule-state frame. While it waits on a full socket the link's task waits
/// with it, which is the backpressure: nothing is queued behind it (ADR 0085 D15).
async fn write_state<W: AsyncWrite + Unpin>(
    wh: &mut W,
    buf: &mut Vec<u8>,
    frame: &PeerMessage,
    remote: &NodeId,
    stats: &LinkStats,
) -> Result<(), std::io::Error> {
    buf.clear();
    if let Err(e) = peer::encode(frame, buf) {
        // Not fatal, as for any frame: the keys are offered again by the next repair.
        warn!(error = %e, peer = %remote.0, "dropping an unencodable rule-state frame");
        return Ok(());
    }
    let started = std::time::Instant::now();
    wh.write_all(buf).await?;
    wh.flush().await?;
    let waited = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    stats.write_wait_ns.fetch_add(waited, Ordering::Relaxed);
    stats.frames_out.fetch_add(1, Ordering::Relaxed);
    if buf.capacity() > PEER_WRITE_BUDGET * 2 {
        buf.shrink_to(PEER_WRITE_BUDGET);
    }
    Ok(())
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
#[allow(clippy::too_many_arguments)] // the link's handles, as `pump` holds them
fn forward_inbound(
    msg: PeerMessage,
    hub: &HubTx,
    remote: &NodeId,
    conn_id: u64,
    plane: Option<&DurablePlane>,
    ingress: Option<&IngressCredit>,
    reply_ctl: &mpsc::WeakUnboundedSender<PeerMessage>,
    reply_bulk: &mpsc::WeakUnboundedSender<PeerMessage>,
) {
    // A publish carrying its origin (ADR 0083; proto 13) is handled as the frame it
    // stands for; the origin joins the message's properties, which is how it travels
    // with the message from here on.
    let (msg, origin) = if matches!(msg, PeerMessage::OriginPublish(_)) {
        let (plain, origin) = msg.into_plain();
        (plain, origin.map(crate::hub::origin_from_wire))
    } else {
        (msg, None)
    };
    let app_of = |app| {
        let mut app = crate::hub::app_from_wire(app);
        app.origin.clone_from(&origin);
        app
    };
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
                conn_id: Some(conn_id),
                filters,
            });
        }
        PeerMessage::MessageEvents { wanted } => {
            let _ = hub.send(HubCommand::RemoteMessageEvents {
                node: remote.clone(),
                wanted,
            });
        }
        // Unwrapped above: `into_plain` never returns one. Rule state (ADR 0085) never
        // gets here: the link's reader applies it to the store.
        PeerMessage::OriginPublish(_)
        | PeerMessage::StateKeys { .. }
        | PeerMessage::StateDigest { .. }
        | PeerMessage::StateWant { .. }
        | PeerMessage::StateExpiry { .. } => {}
        PeerMessage::Publish {
            topic,
            payload,
            qos,
            retain,
            message_expiry,
            app,
        } => {
            let qos = mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce);
            let app = app_of(app);
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
                app: app_of(app),
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
                app: app_of(app),
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
                app: app_of(app),
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
                conn_id: Some(conn_id),
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
            let app = app_of(app);
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
///
/// Only the `write_all` + `flush` is timed into `stats.write_wait_ns`: the encode
/// and the yield above are not socket time, and on an idle link that yield is a
/// run-queue trip per batch that would read as backpressure that is not there.
async fn write_batch<W: AsyncWrite + Unpin>(
    wh: &mut W,
    buf: &mut Vec<u8>,
    stamps: &mut Vec<(stage_timing::Stage, std::time::Instant)>,
    first: &PeerMessage,
    rx: &mut mpsc::UnboundedReceiver<PeerMessage>,
    remote: &NodeId,
    stats: &LinkStats,
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
    let started = std::time::Instant::now();
    wh.write_all(buf).await?;
    wh.flush().await?;
    let waited = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    stats.write_wait_ns.fetch_add(waited, Ordering::Relaxed);
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
            &LinkStats::default(),
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
        let hub_tx = HubTx::from(hub_tx);
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
                1,
                &mut ctl_rx,
                &mut out_rx,
                &reply_ctl,
                &reply_bulk,
                &std::sync::atomic::AtomicUsize::new(0),
                &LinkStats::default(),
                None,
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

    /// `write_wait_ns` measures socket pushback: a write held by a peer that does not
    /// read accrues the hold, a write the peer drains at once accrues next to nothing.
    #[tokio::test]
    async fn write_wait_counts_pushback_and_not_a_prompt_write() {
        /// Writes one bulk frame with `filter` over a duplex of `capacity`, holds the
        /// reader off for `hold`, then drains it. Returns the link's `write_wait_ns`.
        async fn wait_ns(capacity: usize, filter: String, hold: Duration) -> u64 {
            let (mut ours, theirs) = tokio::io::duplex(capacity);
            let (hub_tx, _hub_rx) = mpsc::unbounded_channel();
            let hub_tx = HubTx::from(hub_tx);
            let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel::<PeerMessage>();
            let (out_tx, mut out_rx) = mpsc::unbounded_channel();
            out_tx
                .send(PeerMessage::Interest {
                    filters: vec![filter],
                })
                .unwrap();
            let stats = Arc::new(LinkStats::default());
            let pump_stats = Arc::clone(&stats);
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
                    &NodeId("peer-under-test".into()),
                    1,
                    &mut ctl_rx,
                    &mut out_rx,
                    &reply_ctl,
                    &reply_bulk,
                    &std::sync::atomic::AtomicUsize::new(0),
                    &pump_stats,
                    None,
                    None,
                    None,
                )
                .await
            });
            if !hold.is_zero() {
                // SETTLE(peer-write-wait-hold): the hold IS the pushback under test —
                // the pump's write sits blocked on a full duplex for this long, and the
                // assertion is that write_wait_ns accrued it.
                tokio::time::sleep(hold).await;
            }
            let mut buf = BytesMut::new();
            read_frame(&mut ours, &mut buf)
                .await
                .expect("read")
                .expect("frame");
            // frames_out is bumped after write_wait_ns, so once it shows the batch the
            // wait is recorded too.
            tokio::time::timeout(Duration::from_secs(10), async {
                while stats.frames_out.load(Ordering::Relaxed) == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("pump records the batch");
            drop(ctl_tx);
            drop(out_tx);
            pump_task.await.expect("pump task").expect("pump exits Ok");
            stats.write_wait_ns.load(Ordering::Relaxed)
        }

        let hold = Duration::from_millis(300);
        // Case A: a 4 KiB frame into a 64-byte pipe nobody reads until `hold` is over.
        let blocked = wait_ns(64, "x".repeat(4096), hold).await;
        // Case B: a small frame into a roomy pipe, read at once.
        let prompt = wait_ns(1 << 20, "t".into(), Duration::ZERO).await;

        let hold_ns = u64::try_from(hold.as_nanos()).unwrap();
        assert!(
            blocked >= hold_ns / 2,
            "a write held for {hold:?} must accrue most of it, got {blocked} ns"
        );
        assert!(
            prompt < blocked / 4,
            "a prompt write must accrue far less than a blocked one: {prompt} ns vs {blocked} ns"
        );
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
        let hub = HubTx::from(hub);
        let (ctl, _ctl_rx) = mpsc::unbounded_channel::<PeerMessage>();
        let (bulk, _bulk_rx) = mpsc::unbounded_channel::<PeerMessage>();
        let (ctl, bulk) = (ctl.downgrade(), bulk.downgrade());
        let remote = NodeId("n2".into());
        let unit = 1 + 200 + crate::ingress::COMMAND_OVERHEAD; // topic + payload + overhead
        let credit = IngressCredit::new(4 * unit, 2 * unit, OverloadMode::Pause);
        let forward =
            |msg| forward_inbound(msg, &hub, &remote, 1, None, Some(&credit), &ctl, &bulk);

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
        forward_inbound(publish(0, false), &hub, &remote, 1, None, None, &ctl, &bulk);
        assert!(matches!(
            rx.try_recv(),
            Ok(HubCommand::RemotePublish { credit: None, .. })
        ));
    }

    /// A store with one `eventual` namespace, and that namespace as rules call it.
    fn rule_state(node: &str) -> (Arc<RuleState>, Arc<dyn mqtt_rules::state::StateNamespace>) {
        use mqtt_rules::state::{Consistency, NamespaceSettings, StateStore};
        let store = RuleState::new(64 << 20);
        store.set_node(node);
        let settings = NamespaceSettings {
            consistency: Consistency::Eventual,
            ..NamespaceSettings::defaults(64 << 20)
        };
        let namespace = store.open("shared", &settings).unwrap();
        (store, namespace)
    }

    fn set(namespace: &dyn mqtt_rules::state::StateNamespace, key: &str) {
        let value = mqtt_rules::Value::from("v");
        namespace
            .call(&mqtt_rules::state::StateOp::Set {
                key: key.as_bytes(),
                value: crate::rule_state::stored(&value),
                ttl: None,
            })
            .unwrap();
    }

    /// ADR 0085 D15, ADR 0082: rule state is applied by the link's reader and is no
    /// hub command. Two link tasks over one stream carry 5,000 keys each way; both
    /// stores end up with all of them, neither hub was sent anything for it, and a
    /// frame that is the hub's, sent in the middle of it, still reaches the hub.
    #[tokio::test]
    async fn rule_state_is_applied_by_the_link_and_enters_no_hub_lane() {
        const KEYS: usize = 5_000;
        let (a, a_ns) = rule_state("a");
        let (b, b_ns) = rule_state("b");
        let (stream_a, stream_b) = tokio::io::duplex(64 * 1024);
        let mut hubs = Vec::new();
        let mut lanes = Vec::new();
        let mut tasks = Vec::new();
        for (stream, store, local, remote) in [
            (stream_a, a.clone(), "a", "b"),
            (stream_b, b.clone(), "b", "a"),
        ] {
            let (hub_tx, hub_rx) = mpsc::unbounded_channel();
            let hub_tx = HubTx::from(hub_tx);
            let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel();
            let (out_tx, mut out_rx) = mpsc::unbounded_channel::<PeerMessage>();
            let (reply_ctl, reply_bulk) = (ctl_tx.downgrade(), out_tx.downgrade());
            let remote = NodeId(remote.into());
            let mut link = StateLink::new(store, local, &remote.0);
            tasks.push(tokio::spawn(async move {
                let (mut rh, mut wh) = tokio::io::split(stream);
                let mut buf = BytesMut::new();
                pump(
                    &mut rh,
                    &mut wh,
                    &mut buf,
                    &hub_tx,
                    &remote,
                    1,
                    &mut ctl_rx,
                    &mut out_rx,
                    &reply_ctl,
                    &reply_bulk,
                    &std::sync::atomic::AtomicUsize::new(0),
                    &LinkStats::default(),
                    None,
                    None,
                    Some(&mut link),
                )
                .await
            }));
            hubs.push(hub_rx);
            lanes.push((ctl_tx, out_tx));
        }
        for i in 0..KEYS {
            set(&*a_ns, &format!("a:{i}"));
            set(&*b_ns, &format!("b:{i}"));
            if i == KEYS / 2 {
                // The hub's own traffic, in the middle of the flood.
                lanes[0]
                    .1
                    .send(PeerMessage::Interest {
                        filters: vec!["sensors/#".into()],
                    })
                    .unwrap();
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let keys = |store: &RuleState| store.stats("shared").unwrap().keys;
        while keys(&a) != 2 * KEYS || keys(&b) != 2 * KEYS {
            assert!(
                std::time::Instant::now() < deadline,
                "a holds {} keys and b {}, of {}",
                keys(&a),
                keys(&b),
                2 * KEYS
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // B's hub got the one frame that was its own, and nothing else; A's nothing.
        let mut for_b = Vec::new();
        while let Ok(command) = hubs[1].try_recv() {
            for_b.push(command);
        }
        assert!(
            matches!(&for_b[..], [HubCommand::RemoteInterest { filters, .. }] if filters == &vec!["sensors/#".to_string()]),
            "{for_b:?}"
        );
        assert!(hubs[0].try_recv().is_err(), "nothing for A's hub");
        assert_eq!(b.replication("shared").unwrap().refused, [0; 6]);
        // Dropping the lanes ends the links.
        drop(lanes);
        for task in tasks {
            task.await.unwrap().unwrap();
        }
    }

    /// ADR 0085 D15: rule state travels only on a link that negotiated proto 14. A
    /// peer that announces 13 is sent none of it, however long the link stays up and
    /// whatever is written; one that announces 14 is sent the namespace's digest at
    /// once and what is written after.
    #[tokio::test(start_paused = true)]
    async fn a_link_that_negotiated_13_carries_no_rule_state() {
        for (announced, carries) in [(peer::PROTO_MESSAGE_ORIGIN, false), (peer::PROTO_MAX, true)] {
            let (store, namespace) = rule_state("a");
            set(&*namespace, "before");
            let (mut ours, theirs) = tokio::io::duplex(1 << 20);
            let (hub_tx, mut hub_rx) = mpsc::unbounded_channel();
            let hub_tx = HubTx::from(hub_tx);
            let link = tokio::spawn(handle(
                theirs,
                NodeId("a".into()),
                hub_tx,
                true,
                None,
                None,
                LinkCert::default(),
                None,
                None,
                None,
                Some(store.clone()),
            ));
            // The peer "b": it reads our Hello and answers with what it speaks.
            let mut buf = BytesMut::new();
            let hello = read_frame(&mut ours, &mut buf).await.unwrap().unwrap();
            assert!(
                matches!(hello, PeerMessage::Hello { proto_max, .. } if proto_max == peer::PROTO_MAX)
            );
            write_frame(
                &mut ours,
                &PeerMessage::Hello {
                    node_id: "b".into(),
                    proto_min: peer::PROTO_MIN,
                    proto_max: announced,
                },
            )
            .await
            .unwrap();
            // The hub is told what the link negotiated; it holds the link's lanes.
            let Some(HubCommand::PeerConnected { proto, tx, ctl, .. }) = hub_rx.recv().await else {
                panic!("the link did not register");
            };
            assert_eq!(proto, announced);
            set(&*namespace, "after");
            let mut seen = Vec::new();
            // Ten minutes of the link's time: many times the interval at which two
            // nodes compare (the clock is the test's, so this takes no time).
            let quiet = tokio::time::timeout(Duration::from_mins(10), async {
                loop {
                    let frame = read_frame(&mut ours, &mut buf).await.unwrap().unwrap();
                    seen.push(frame);
                    if seen.len() == 2 {
                        break;
                    }
                }
            })
            .await;
            if carries {
                assert!(quiet.is_ok(), "a proto-14 link carries state: {seen:?}");
                assert!(
                    matches!(&seen[0], PeerMessage::StateDigest { namespace, parts, .. }
                        if namespace == "shared" && parts.is_empty()),
                    "{seen:?}"
                );
                assert!(
                    matches!(&seen[1], PeerMessage::StateKeys { entries, .. }
                        if entries.len() == 1 && entries[0].0 == b"after"),
                    "{seen:?}"
                );
            } else {
                assert!(quiet.is_err(), "a proto-13 link was sent {seen:?}");
                assert!(seen.is_empty(), "a proto-13 link was sent {seen:?}");
                assert_eq!(store.replication("shared").unwrap().links, 0);
            }
            drop((tx, ctl));
            link.await.unwrap().unwrap();
        }
    }

    // ---- ADR 0087 D5: the dialer of a peer membership holds `Dead` -----------
    //
    // These tests run on the real clock against real sockets, so every bound in them
    // is one-sided or generous: a "no attempt for N seconds" is longer than any wait
    // the dialer could be in, and an "attempt arrives" has twenty seconds to do so.
    // A loaded runner makes them slower, not wrong.

    /// A dial target and the handle its standing is changed through.
    fn dial_target(
        node: &str,
        standing: DialStanding,
    ) -> (tokio::sync::watch::Sender<DialStanding>, DialTarget) {
        let (tx, rx) = tokio::sync::watch::channel(standing);
        let target = DialTarget {
            node: NodeId(node.into()),
            standing: rx,
        };
        (tx, target)
    }

    fn held_dead_now() -> DialStanding {
        DialStanding::HeldDead
    }

    /// Dial `addr` as node `a`, aimed at `target`; the hub's end of the dialer.
    fn spawn_dialer(
        addr: std::net::SocketAddr,
        target: DialTarget,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::UnboundedReceiver<HubCommand>,
    ) {
        spawn_dialer_with_handshake(addr, target, DIAL_HANDSHAKE_TIMEOUT)
    }

    /// [`spawn_dialer`], with the handshake deadline a test wants.
    fn spawn_dialer_with_handshake(
        addr: std::net::SocketAddr,
        target: DialTarget,
        handshake_within: Duration,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::UnboundedReceiver<HubCommand>,
    ) {
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        let hub_tx = HubTx::from(hub_tx);
        let dialer = tokio::spawn(dial(
            addr.to_string(),
            NodeId("a".into()),
            hub_tx,
            None,
            None,
            None,
            None,
            Some(target),
            handshake_within,
        ));
        (dialer, hub_rx)
    }

    /// How long a test waits for something that must happen.
    const MUST: Duration = Duration::from_secs(20);
    /// Longer than any wait a dialer can be in between two attempts (the backoff's
    /// ceiling): this long with no attempt, and the dialer has stopped dialing.
    const QUIET: Duration = Duration::from_secs(6);

    /// The peer's end: the next connection the dialer makes, hung up on at once —
    /// an attempt that serves no link.
    async fn hang_up_on_the_next_attempt(listener: &TcpListener) {
        let (stream, _) = tokio::time::timeout(MUST, listener.accept())
            .await
            .expect("the dialer never came")
            .unwrap();
        drop(stream);
    }

    /// Accept the dialer's next connection and answer its `Hello` as node `id`.
    async fn accept_as(listener: &TcpListener, id: &str) -> (TcpStream, BytesMut) {
        let (mut stream, _) = tokio::time::timeout(MUST, listener.accept())
            .await
            .expect("the dialer never came")
            .unwrap();
        let mut buf = BytesMut::new();
        let hello = read_frame(&mut stream, &mut buf).await.unwrap();
        assert!(
            matches!(hello, Some(PeerMessage::Hello { .. })),
            "{hello:?}"
        );
        write_frame(
            &mut stream,
            &PeerMessage::Hello {
                node_id: id.into(),
                proto_min: peer::PROTO_MIN,
                proto_max: peer::PROTO_MAX,
            },
        )
        .await
        .unwrap();
        (stream, buf)
    }

    /// The acceptance of 0087-T4's last clause, as 0087-T3 settles it: the dialer of
    /// a peer held `Dead` dials for as long as the verdict stands, and stops when
    /// membership confirms the peer gone (ADR 0087 D2) — but it does not END: its
    /// task stays, and the `Alive` of a later life sets that same task dialing again.
    ///
    /// That is what closes a race an earlier form of this had. The dialer used to
    /// return when it gave up, and the link manager started another on the next
    /// `Alive` — if it saw the task finished. An `Alive` handled between the
    /// dialer's decision and its task ending found a running task, handed IT the
    /// news, and the member was alive with no dialer. Now there is no decision to
    /// end, so there is nothing to race: every `Alive` reaches a dialer that can act
    /// on it.
    #[tokio::test]
    async fn a_dialer_stops_dialing_a_peer_confirmed_gone_and_is_set_going_again_by_alive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (standing, target) = dial_target("b", DialStanding::Member);
        let (dialer, _hub) = spawn_dialer(listener.local_addr().unwrap(), target);

        // In good standing it is redialed without end: three attempts, for a start.
        for _ in 0..3 {
            hang_up_on_the_next_attempt(&listener).await;
        }

        // Held dead: it goes on dialing, with no window of its own to run out.
        standing.send_replace(held_dead_now());
        for _ in 0..3 {
            hang_up_on_the_next_attempt(&listener).await;
        }

        // Confirmed gone: no more attempts, longer than any backoff.
        standing.send_replace(DialStanding::Gone);
        let mut after_confirmation = 0;
        while let Ok(accepted) = tokio::time::timeout(QUIET, listener.accept()).await {
            drop(accepted.unwrap());
            after_confirmation += 1;
            assert!(
                after_confirmation <= 1,
                "the dialer dials a peer confirmed gone (only the attempt under way \
                 when the confirmation arrived may land)"
            );
        }
        assert!(!dialer.is_finished(), "it stopped dialing; it did not end");

        // A later life is alive: the same task hears of it.
        standing.send_replace(DialStanding::Member);
        hang_up_on_the_next_attempt(&listener).await;
        hang_up_on_the_next_attempt(&listener).await;
        assert!(!dialer.is_finished());
        dialer.abort();
    }

    /// The hand-over from the other side: confirmation and a later `Alive` both
    /// arrive while an attempt is still under way. The peer here holds that attempt
    /// open, and only then fails it: the dialer finds its standing already `Member`,
    /// and dials on.
    ///
    /// Between this order and the one above lies the instant neither test can reach
    /// from outside — the `Alive` landing after the dialer has read its standing and
    /// before it waits. It is covered by construction: the dialer waits on the same
    /// `watch` it read, the read marks what it saw, and a value written since then
    /// ends the wait at once.
    #[tokio::test]
    async fn an_alive_that_arrives_during_an_attempt_after_confirmation_is_not_lost() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (standing, target) = dial_target("b", held_dead_now());
        let (dialer, _hub) = spawn_dialer(listener.local_addr().unwrap(), target);

        // The attempt is under way: connected, its `Hello` sent, no answer yet.
        let (held_open, _) = tokio::time::timeout(MUST, listener.accept())
            .await
            .expect("a peer held dead is still dialed")
            .unwrap();
        standing.send_replace(DialStanding::Gone);
        standing.send_replace(DialStanding::Member);
        drop(held_open);

        hang_up_on_the_next_attempt(&listener).await;
        hang_up_on_the_next_attempt(&listener).await;
        assert!(!dialer.is_finished());
        dialer.abort();
    }

    /// What a peer that is really gone costs: under a verdict each attempt that
    /// serves no link doubles the wait before the next (500 ms, then 1 s, 2 s, 4 s,
    /// and 5 s from there on), where a member in good standing is redialed twice a
    /// second. A withdrawn verdict does not wait that backoff out.
    #[tokio::test]
    async fn a_peer_held_dead_is_redialed_with_a_growing_delay_until_the_verdict_is_withdrawn() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (standing, target) = dial_target("b", held_dead_now());
        let (dialer, _hub) = spawn_dialer(listener.local_addr().unwrap(), target);

        // The peer accepts and hangs up: an attempt that serves no link. The gaps
        // between attempts are lower bounds — no load can shorten a wait.
        let mut accepted = Vec::new();
        for _ in 0..4 {
            hang_up_on_the_next_attempt(&listener).await;
            accepted.push(tokio::time::Instant::now());
        }
        let gaps: Vec<Duration> = accepted.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(
            gaps[0] >= REDIAL_DELAY * 2
                && gaps[1] >= REDIAL_DELAY * 4
                && gaps[2] >= REDIAL_DELAY * 8,
            "the delay doubles: {gaps:?}"
        );

        // The fourth failure set a five-second wait. Membership withdraws the
        // verdict, and the dialer is back in less than HALF of that — two and a half
        // seconds of margin for a starved runner, where it takes no time at all.
        standing.send_replace(DialStanding::Member);
        let withdrawn = tokio::time::Instant::now();
        hang_up_on_the_next_attempt(&listener).await;
        assert!(
            withdrawn.elapsed() < HELD_DEAD_REDIAL_MAX / 2,
            "it did not wait out the backoff: {:?}",
            withdrawn.elapsed()
        );
        dialer.abort();
    }

    /// A link served when the confirmation arrives is served to its end: the peer is
    /// there (a live peer that was pruned answers the parting greeting, and an
    /// `Alive` follows). Only when it has ended does the dialer stop.
    #[tokio::test]
    async fn a_link_served_when_the_peer_is_confirmed_gone_is_served_to_its_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (standing, target) = dial_target("b", held_dead_now());
        let (dialer, mut hub) = spawn_dialer(listener.local_addr().unwrap(), target);

        let (mut stream, mut buf) = accept_as(&listener, "b").await;
        let registered = tokio::time::timeout(MUST, hub.recv())
            .await
            .expect("the hub is told of the link");
        assert!(
            matches!(&registered, Some(HubCommand::PeerConnected { node, .. }) if node.0 == "b"),
            "{registered:?}"
        );
        standing.send_replace(DialStanding::Gone);
        // The link is still served: nothing closes it from the dialer's side.
        let read = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match read_frame(&mut stream, &mut buf).await {
                    Ok(Some(_)) => {}
                    other => return other,
                }
            }
        })
        .await;
        assert!(read.is_err(), "the link stayed open: {read:?}");
        drop(stream);
        assert!(
            tokio::time::timeout(QUIET, listener.accept())
                .await
                .is_err(),
            "once the link has ended, a peer confirmed gone is not dialed again"
        );
        assert!(!dialer.is_finished());
        dialer.abort();
    }

    /// A peer that completes the TCP connection and then says nothing — its kernel
    /// answered, its process hung, or its host was reset before it could refuse —
    /// does not hold the dialer: the wait for its `Hello` has a deadline, and the
    /// dialer comes again. Without one the attempt never returned, and since this
    /// node is the only side that may dial the pair, the two never linked again
    /// until this node restarted. Under a verdict, and for a member in good
    /// standing alike.
    #[tokio::test]
    async fn a_peer_that_accepts_and_never_speaks_does_not_wedge_the_dialer() {
        for standing in [held_dead_now(), DialStanding::Member] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (_standing, target) = dial_target("b", standing);
            let (dialer, mut hub) = spawn_dialer_with_handshake(
                listener.local_addr().unwrap(),
                target,
                Duration::from_millis(300),
            );

            // Three connections, each accepted, read from, and never answered. All
            // three are kept open: only the dialer's own deadline ends an attempt.
            let mut silent = Vec::new();
            for attempt in 1..=3 {
                let (mut stream, _) = tokio::time::timeout(MUST, listener.accept())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "{standing:?}: the dialer is wedged in attempt {}",
                            attempt - 1
                        )
                    })
                    .unwrap();
                let mut buf = BytesMut::new();
                let hello = read_frame(&mut stream, &mut buf).await.unwrap();
                assert!(matches!(hello, Some(PeerMessage::Hello { .. })));
                silent.push(stream);
            }
            assert!(hub.try_recv().is_err(), "no link was ever registered");

            // The peer comes to: the next attempt is answered, and linked.
            let (_stream, _buf) = accept_as(&listener, "b").await;
            let registered = tokio::time::timeout(MUST, hub.recv())
                .await
                .expect("the hub is told of the link");
            assert!(
                matches!(&registered, Some(HubCommand::PeerConnected { node, .. }) if node.0 == "b"),
                "{standing:?}: {registered:?}"
            );
            dialer.abort();
        }
    }

    /// A dialer that outlives a verdict may be dialing an address its member no
    /// longer holds. Whoever else answers there is not linked — the hub hears
    /// nothing — and is then left alone: the node that answered took the connection
    /// for its own link to this node and replaced the real one with it, so every
    /// further attempt would flap that pair. The dialer returns when membership
    /// speaks of the member again, and the member itself is linked.
    #[tokio::test]
    async fn a_dialer_links_only_the_member_it_was_aimed_at() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (standing, target) = dial_target("b", held_dead_now());
        let (dialer, mut hub) = spawn_dialer(listener.local_addr().unwrap(), target);

        // Another node of the cluster answers at b's address.
        let (mut stranger, mut buf) = accept_as(&listener, "c").await;
        let closed =
            tokio::time::timeout(Duration::from_secs(20), read_frame(&mut stranger, &mut buf))
                .await
                .expect("the dialer hangs up on a node it did not dial");
        assert!(matches!(closed, Ok(None) | Err(_)), "{closed:?}");
        assert!(
            hub.try_recv().is_err(),
            "no link to the node that answered instead"
        );
        // And the address is left alone: longer than any backoff, and no attempt.
        assert!(
            tokio::time::timeout(QUIET, listener.accept())
                .await
                .is_err(),
            "the dialer came back to an address another node answers on"
        );

        // Membership speaks of the member again; it is there now, at this address.
        standing.send_replace(DialStanding::Member);
        let (_member, _buf) = accept_as(&listener, "b").await;
        let registered = tokio::time::timeout(Duration::from_secs(20), hub.recv())
            .await
            .expect("the member is linked");
        assert!(
            matches!(&registered, Some(HubCommand::PeerConnected { node, .. }) if node.0 == "b"),
            "{registered:?}"
        );
        dialer.abort();
    }

    // ---- Moving a link to renewed certificates (issue #1016) ----

    const HOUR_MS: i64 = 3_600_000;

    /// A day-long certificate: the end window is the cap, five minutes, a minute clear of
    /// the end; a two-minute one (the binary test's) scales both down.
    #[test]
    fn the_end_window_is_a_quarter_of_the_life_capped_at_five_minutes() {
        let day = MovePlan {
            end_ms: 24 * HOUR_MS,
            made_ms: 0,
        };
        assert_eq!(day.window(), (5 * 60_000, 60_000));
        assert_eq!(day.window_opens(), 24 * HOUR_MS - 6 * 60_000);
        assert_eq!(day.in_end_window(0.0), 24 * HOUR_MS - 6 * 60_000);
        assert!(day.in_end_window(0.999_999) < 24 * HOUR_MS - 60_000);

        let short = MovePlan {
            end_ms: 120_000,
            made_ms: 0,
        };
        assert_eq!(short.window(), (30_000, 15_000));
        assert_eq!(short.in_end_window(0.5), 120_000 - 15_000 - 15_000);
    }

    /// After a renewal the try lands within half the room left before the margin, at
    /// most five minutes away; with no room left it is at once.
    #[test]
    fn a_renewal_is_acted_on_within_half_the_room_left_capped() {
        let day = MovePlan {
            end_ms: 24 * HOUR_MS,
            made_ms: 0,
        };
        let renewed_at = 16 * HOUR_MS;
        assert_eq!(day.after_renewal(renewed_at, 0.0), renewed_at);
        let latest = day.after_renewal(renewed_at, 0.999_999);
        assert!(latest > renewed_at && latest < renewed_at + 5 * 60_000);

        let short = MovePlan {
            end_ms: 120_000,
            made_ms: 0,
        };
        // 120 s life, renewed at 30 s: 75 s of room before the 15 s margin, so 37.5 s.
        assert_eq!(short.after_renewal(30_000, 0.5), 30_000 + 18_750);
        assert_eq!(
            short.after_renewal(110_000, 0.5),
            110_000,
            "no room: at once"
        );
    }

    /// A try that moved nothing is tried again in the end window — at its jittered moment
    /// when that is still ahead, and a tenth of the window later (at least a second) once
    /// in it — and never planned past the end.
    #[test]
    fn a_missed_try_is_retried_in_the_end_window() {
        let day = MovePlan {
            end_ms: 24 * HOUR_MS,
            made_ms: 0,
        };
        let opens = day.window_opens();
        assert_eq!(day.retry(16 * HOUR_MS, 0.0), opens);
        assert_eq!(day.retry(opens + 1, 0.0), opens + 1 + 30_000);
        let short = MovePlan {
            end_ms: 8_000,
            made_ms: 0,
        };
        assert_eq!(short.retry(7_000, 0.0), 8_000, "at least a second apart");
    }

    fn ends(peer: i64, own: i64) -> crate::hub::LinkCertEnds {
        crate::hub::LinkCertEnds {
            peer: Some(peer),
            own: Some(own),
        }
    }

    /// A link's mover plans its first try in the end window; a renewal of this node's own
    /// certificate brings it forward, once per renewal; past the end there is nothing to
    /// move to (the hard end is the backstop); a plaintext link never moves.
    #[test]
    fn a_mover_plans_from_the_link_and_from_its_own_renewals() {
        let now = 1_000_000_000_i64;
        let end = now / 1000 + 24 * 3600; // seconds
        let mut mover = Mover {
            ends: ends(end, end),
            made_ms: now,
            own_seen: Some(end),
            next_ms: None,
        };
        let first = mover.due(now, Some(end)).unwrap();
        let plan = mover.plan().unwrap();
        assert!(first >= plan.window_opens() && first < end * 1000);
        assert_eq!(mover.due(now + 1, Some(end)), Some(first), "planned once");

        let renewed = end + 24 * 3600;
        let after = mover.due(now + 2, Some(renewed)).unwrap();
        assert!(after >= now + 2 && after <= now + 2 + MOVE_SPREAD_MAX_MS);
        assert_eq!(
            mover.due(now + 3, Some(renewed)),
            Some(after),
            "once per renewal"
        );

        mover.missed(after);
        let retried = mover.due(after, Some(renewed)).unwrap();
        assert!(
            retried >= plan.window_opens(),
            "a miss waits for the end window"
        );

        assert_eq!(mover.due(end * 1000 + 1, Some(renewed)), None);
        let mut plaintext = Mover::new(crate::hub::LinkCertEnds::default());
        assert_eq!(plaintext.due(now, None), None);
    }

    /// The point of the jitter: the links of nodes minted together, renewed together, move
    /// at different moments spread over the window — not in the same second.
    #[test]
    fn the_moves_of_one_renewal_are_spread() {
        let now = 1_000_000_000_i64;
        let end = now / 1000 + 8 * 3600;
        let renewed = end + 24 * 3600;
        let tries: Vec<i64> = (0..32)
            .map(|_| {
                let mut mover = Mover {
                    ends: ends(end, end),
                    made_ms: now - 16 * HOUR_MS,
                    own_seen: Some(end),
                    next_ms: None,
                };
                mover.due(now, Some(renewed)).unwrap()
            })
            .collect();
        let earliest = *tries.iter().min().unwrap();
        let latest = *tries.iter().max().unwrap();
        assert!(earliest >= now && latest <= now + MOVE_SPREAD_MAX_MS);
        // 32 uniform draws over five minutes span well over a minute but for a chance
        // far below one in a billion.
        assert!(latest - earliest > 60_000, "{tries:?}");
        let mut seconds: Vec<i64> = tries.iter().map(|t| t / 1000).collect();
        seconds.dedup();
        assert!(seconds.len() > 1);
    }
}
