//! Per-connection task: CONNECT handshake, then a select loop multiplexing
//! inbound client packets, outbound packets delivered by the hub, and the
//! keepalive deadline.
//!
//! Keepalive [MQTT-3.1.2-24]: with a non-zero keepalive, the server closes the
//! connection if nothing arrives from the client within 1.5x the interval; the
//! deadline resets on *inbound* traffic only (outbound deliveries must not keep
//! a dead client alive). An ungraceful end — EOF, error, keepalive expiry —
//! publishes the client's will; a clean DISCONNECT discards it.

use crate::aliases::{InboundAliases, OutboundAliases};
use crate::hub::{Admission, AttachOutcome, AuthMethod, HubCommand, Outbound};
use bytes::Bytes;
use mqtt_auth::{
    basic::BasicAuthenticator, mtls::IdentitySource, AllowAll, AuthSession, AuthStep,
    Authenticator, Authorizer, Credentials, EnhancedAuthenticator, Identity,
};
use mqtt_cluster::placement::Placement;
use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Auth, ConnAck, Connect, Disconnect, Publish, SubAck},
    reason, Packet, ProtocolVersion, QoS,
};
use mqtt_core::{is_shared_filter, parse_shared, AppProperties, ClientId, Message};
use mqtt_net::{FrameReader, FrameWriter, NetError};
use mqtt_observability::{AuditLog, AuditSink};
use mqtt_storage::InboundSighting;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

/// Keepalive grace factor: the spec allows one and a half keepalive periods.
const KEEPALIVE_GRACE_NUM: u64 = 3;
const KEEPALIVE_GRACE_DEN: u64 = 2;

// MQTT 3.1.1 CONNACK **return codes** (0x01–0x05) — a code space distinct from v5 reason
// codes (e.g. v3 return code 1 = unacceptable protocol; v5 reason 0x01 = Granted QoS 1), so
// these stay as their own constants rather than aliasing `mqtt_codec::reason`.
/// CONNACK return code: unacceptable protocol version (MQTT 3.1.1 return code 1).
const CONNACK_UNACCEPTABLE_PROTOCOL: u8 = 0x01;
/// CONNACK return code: identifier rejected (MQTT 3.1.1 return code 2).
const CONNACK_IDENTIFIER_REJECTED: u8 = 0x02;
/// CONNACK return code: the server is temporarily unavailable (MQTT 3.1.1 return code 3) —
/// used when a durable session cannot be recovered yet (lease handoff / no quorum) so
/// the client retries rather than starting clean (ADR 0017).
const CONNACK_SERVER_UNAVAILABLE: u8 = 0x03;
/// CONNACK return code: bad user name or password (MQTT 3.1.1 return code 4).
const CONNACK_BAD_CREDENTIALS: u8 = 0x04;
/// CONNACK return code: not authorized (MQTT 3.1.1 return code 5).
const CONNACK_NOT_AUTHORIZED: u8 = 0x05;

// v5 reason codes — sourced from the shared `mqtt_codec::reason` catalogue (ADR 0008 T8) so
// there is a single audited definition of each wire value. The broker-context aliases below
// keep call sites self-documenting (e.g. `SUBACK_FAILURE` reads better than `UNSPECIFIED_ERROR`
// in a SUBACK).
/// SUBACK return code: failure (subscription refused) — v5 `0x80` Unspecified error.
const SUBACK_FAILURE: u8 = reason::UNSPECIFIED_ERROR;
/// AUTH reason code: success (the enhanced-auth exchange completed) (ADR 0013).
const AUTH_SUCCESS: u8 = reason::SUCCESS;
/// AUTH reason code: continue the enhanced-authentication exchange (ADR 0013).
const AUTH_CONTINUE: u8 = reason::CONTINUE_AUTHENTICATION;
/// AUTH reason code: re-authenticate an established session (ADR 0013 §4).
const AUTH_REAUTH: u8 = reason::REAUTHENTICATE;
/// CONNACK reason (v5): not authorized.
const CONNACK_V5_NOT_AUTHORIZED: u8 = reason::NOT_AUTHORIZED;
/// CONNACK reason (v5): the requested authentication method is not supported.
const CONNACK_V5_BAD_AUTH_METHOD: u8 = reason::BAD_AUTHENTICATION_METHOD;
/// DISCONNECT reason (v5): protocol error.
const DISCONNECT_PROTOCOL_ERROR: u8 = reason::PROTOCOL_ERROR;
/// DISCONNECT reason (v5): not authorized (a failed re-authentication, ADR 0013 §4).
const DISCONNECT_NOT_AUTHORIZED: u8 = reason::NOT_AUTHORIZED;
/// DISCONNECT reason (v5): the server is shutting down (graceful drain, ADR 0019).
const DISCONNECT_SERVER_SHUTTING_DOWN: u8 = reason::SERVER_SHUTTING_DOWN;
/// DISCONNECT reason (v5): topic alias invalid (out of range / unmapped, ADR 0011 §2).
const DISCONNECT_TOPIC_ALIAS_INVALID: u8 = reason::TOPIC_ALIAS_INVALID;
/// DISCONNECT reason (v5): the client exceeded the server's Receive Maximum (ADR 0012 §3).
const DISCONNECT_RECEIVE_MAXIMUM_EXCEEDED: u8 = reason::RECEIVE_MAXIMUM_EXCEEDED;
/// DISCONNECT reason (v5): the client used Subscription Identifiers, which this server does
/// not support (MQTT 5.0 §3.2.2.3.12, `[MQTT-4.13.1-1]`). `0xA1` — **not** `0xA2`, which is
/// Wildcard Subscriptions not supported.
const DISCONNECT_SUBSCRIPTION_IDS_NOT_SUPPORTED: u8 =
    reason::SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED;

/// Whether this server supports MQTT 5 Subscription Identifiers (issue #245).
///
/// MQTT 5.0 §3.2.2.3.12, verbatim: "If not present, then Subscription Identifiers are
/// supported." An absent CONNACK property `0x29` is therefore an affirmative claim of
/// support, so a server that does not deliver identifiers must say `0` on the wire —
/// silence is a lie a client cannot detect.
///
/// Both halves of the posture read this one constant so they cannot drift: the CONNACK
/// advertisement in [`negotiate_v5_properties`] and the SUBSCRIBE refusal in [`serve`].
/// Flipping it to `true` is step 1 of actually delivering identifiers (see the follow-up
/// issue), and will turn the two guard tests red on purpose.
const SUB_IDS_SUPPORTED: bool = false;
/// Server-advertised MQTT 5.0 wire limits, configurable at startup (ADR 0011/0012/0013).
/// These are genuinely server-wide (the same maxima are advertised to every connection),
/// so they live in one process-wide value set once from config rather than per-connection.
#[derive(Debug, Clone, Copy)]
pub struct WireLimits {
    /// Topic Alias Maximum advertised to v5 clients (ADR 0011 §2): the highest inbound
    /// topic alias the server accepts. `0` disables inbound topic aliases.
    pub topic_alias_max: u16,
    /// Receive Maximum advertised to v5 clients (ADR 0012 §3): the most unacked `QoS` > 0
    /// publishes a client may have outstanding **to** the server before it is disconnected
    /// with reason `0x93`.
    pub receive_maximum: u16,
    /// How long the server waits for the client's reply in each round of the enhanced-auth
    /// exchange before aborting it (ADR 0013 §3) — bounds a stalled half-open auth.
    pub auth_round_timeout: Duration,
    /// Per-connection inbound publish rate (messages/second, ADR 0041 T3); an
    /// over-rate publisher is slowed by **pausing the socket read** (TCP
    /// backpressure) — no drops, no disconnect. `None` = unlimited.
    pub publish_rate: Option<u32>,
    /// The inbound packet ceiling, advertised to v5 clients as the MQTT 5
    /// Maximum Packet Size (ADR 0041 T4). Also installed as the transport frame
    /// cap (`mqtt_net::set_max_packet_bytes`) by the binary.
    pub max_packet_size: u32,
}

impl Default for WireLimits {
    fn default() -> Self {
        Self {
            topic_alias_max: 16,
            receive_maximum: 256,
            auth_round_timeout: Duration::from_secs(10),
            publish_rate: None,
            max_packet_size: 1024 * 1024,
        }
    }
}

static WIRE_LIMITS: std::sync::OnceLock<WireLimits> = std::sync::OnceLock::new();

/// Set the process-wide [`WireLimits`] once, at startup before any connection is served
/// (a no-op if already set). Production reads them from env in `main`; tests use the default.
pub fn set_wire_limits(limits: WireLimits) {
    let _ = WIRE_LIMITS.set(limits);
}

/// The configured [`WireLimits`], or the default if [`set_wire_limits`] was never called.
fn wire_limits() -> WireLimits {
    *WIRE_LIMITS.get_or_init(WireLimits::default)
}

/// Monotonic source of unique connection ids (distinct from client ids).
static CONN_ID: AtomicU64 = AtomicU64::new(1);
/// Counter for server-assigned client ids (empty-id clients).
static AUTO_ID: AtomicU64 = AtomicU64::new(1);

/// A TLS-verified client-certificate admission (ADR 0004): the extracted identity
/// plus the leaf's serial number — the server-side fact a CRL revocation sweep
/// re-checks against a reloaded policy (ADR 0040 T1).
#[derive(Debug, Clone)]
pub struct CertAdmission {
    /// The broker identity of the verified leaf — its Subject CN, or the SAN the
    /// operator selected (ADR 0004 T11).
    pub identity: Identity,
    /// The leaf's serial number (big-endian bytes as encoded in the certificate);
    /// `None` when no live TLS leaf exists at this hop (a vouched proxied session,
    /// ADR 0005 — the landing node holds the actual TLS session).
    pub serial: Option<Vec<u8>>,
}

/// Extract the mTLS admission (ADR 0004/0040) from an accepted server-side TLS
/// stream: the chain-verified leaf certificate's identity field and serial.
///
/// Returns `None` when no client certificate was presented, or when a verified
/// certificate carries no usable identity in the configured `source` (logged — such a
/// client can only proceed as anonymous, which the default policy denies).
pub fn tls_admission<S>(
    tls: &tokio_rustls::server::TlsStream<S>,
    source: IdentitySource,
) -> Option<CertAdmission> {
    let leaf = tls.get_ref().1.peer_certificates()?.first()?;
    cert_admission(leaf, source)
}

/// Build a [`CertAdmission`] from a chain-verified DER leaf certificate (shared by
/// the TLS/WSS listeners and the QUIC handshake), reading the identity from the field
/// `source` selects (ADR 0004 T11; [`IdentitySource::CommonName`] is the default).
pub fn cert_admission(leaf: &[u8], source: IdentitySource) -> Option<CertAdmission> {
    match mqtt_auth::mtls::identity_from_cert_with(leaf, source) {
        Ok(identity) => Some(CertAdmission {
            identity,
            serial: mqtt_auth::mtls::serial_from_cert(leaf),
        }),
        Err(e) => {
            // No fallback to another field: a certificate that does not carry the
            // configured identity is not identified, full stop (ADR 0004 T11).
            warn!(
                error = %e,
                %source,
                "client certificate verified but carries no usable identity for the configured source"
            );
            None
        }
    }
}

/// What a landing node needs to relocate a persistent session to its placement
/// owner (ADR 0005): the live ring (to find the owner and its address) and the
/// cluster-bus connector (to reach the owner's peer listener over mTLS;
/// `None` = plaintext mesh).
#[derive(Clone)]
pub struct ProxyContext {
    /// This node's id — sent in the `ProxyHello` so the owner can attribute the
    /// relocated session to the node that vouched for it (audit `via`).
    pub node: NodeId,
    /// The live session-placement ring.
    pub placement: Arc<RwLock<Placement>>,
    /// mTLS connector for dialing the owner's peer listener; `None` = plaintext.
    /// Behind a `watch` (ADR 0040 T4): read per relocation dial, so a reload's
    /// rebuilt connector (rotated cluster cert) is used on the next proxy.
    pub connector: Option<tokio::sync::watch::Receiver<TlsConnector>>,
}

impl std::fmt::Debug for ProxyContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyContext")
            .field("node", &self.node.0)
            .field("mtls", &self.connector.is_some())
            .finish_non_exhaustive()
    }
}

/// The policy a connection consults: who may connect ([`Authenticator`]), what
/// they may do ([`Authorizer`]), where security decisions are audited
/// ([`AuditSink`], ADR 0004 step 4), and — when clustered — how to relocate a
/// persistent session to its owner ([`ProxyContext`], ADR 0005).
pub struct ConnPolicy {
    /// Authenticates the CONNECT credentials. Held behind a [`watch::Receiver`] so a
    /// SIGHUP reload (ADR 0032) can swap the authenticator under live connections; each
    /// CONNECT reads the **current** value ([`ConnPolicy::authenticator`]).
    pub auth: watch::Receiver<Arc<dyn Authenticator>>,
    /// Optional MQTT 5.0 enhanced-authentication mechanism (ADR 0013): runs the
    /// SASL-style AUTH exchange when a CONNECT names its method. `None` disables it.
    pub enhanced: Option<Arc<dyn EnhancedAuthenticator>>,
    /// Authorizes publish/subscribe topics. Held behind a [`watch::Receiver`] so a reload
    /// reaches **live** connections — each publish/subscribe reads the current value
    /// ([`ConnPolicy::authorizer`]), so a tightened ACL denies an already-subscribed
    /// client's next operation (ADR 0032).
    pub authz: watch::Receiver<Arc<dyn Authorizer>>,
    /// Which field of a verified client certificate is the identity (ADR 0004 T11).
    /// Deliberately **not** behind a `watch`: re-keying every ACL under live sessions is
    /// a restart-level change, and the config reload path reports an edit to it as
    /// requires-restart (ADR 0046 T4) rather than applying half of it.
    pub identity_source: IdentitySource,
    /// Records auth and authorization decisions.
    pub audit: Arc<dyn AuditSink>,
    /// Session relocation context; `None` outside a cluster (serve locally).
    pub proxy: Option<ProxyContext>,
    /// This node's id, when it has one.
    ///
    /// Deliberately its **own** field rather than read off [`ProxyContext`]. The
    /// first version of the assigned-client-id fix took it from there, since a
    /// proxy context happens to carry one — but that ties "what am I called" to
    /// "is session relocation configured", two things that are equal today and
    /// need not stay that way. A deployment that clustered without a proxy context
    /// would have silently gone back to handing out colliding ids, which is the
    /// class of unstated precondition this codebase keeps paying for.
    pub node: Option<NodeId>,
    /// The session store, shared with the hub, backing the **durable** QoS-2 inbound
    /// dedup window (ADR 0007 §5): `record_received` quorum-replicates the packet id
    /// before PUBREC, so exactly-once survives a failover. `None` falls back to a
    /// per-connection in-memory window (lost on disconnect — fine for clean sessions
    /// and the in-memory backend).
    pub store: Option<Arc<dyn mqtt_storage::SessionStore>>,
    /// How long a freshly-accepted connection has to send its CONNECT before the
    /// broker closes it. Bounds the unauthenticated half-open / slow-loris surface:
    /// the keepalive timer only starts after CONNECT, so without this a client that
    /// connects and stalls would hold a connection task indefinitely.
    pub connect_timeout: Duration,
    /// Graceful-shutdown signal (ADR 0019). When set and cancelled, an established
    /// connection finishes its current packet, then closes **without firing the will**
    /// (the server is going away, the client is not — its session is retained for
    /// reconnect). `None` disables draining (tests, and the in-process test harness).
    pub shutdown: Option<tokio_util::sync::CancellationToken>,
    /// Prometheus metrics (ADR 0020), when enabled: the connection lifecycle updates the
    /// active-connections gauge and the per-protocol total. `None` in tests.
    pub metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
}

/// Default [`ConnPolicy::connect_timeout`]: generous for a real handshake on a slow
/// link, but bounded so an idle/stalled pre-CONNECT connection cannot live forever.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

impl std::fmt::Debug for ConnPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnPolicy").finish_non_exhaustive()
    }
}

impl ConnPolicy {
    /// The **current** authenticator — re-read on every CONNECT so a SIGHUP reload (ADR
    /// 0032) takes effect without restarting.
    #[must_use]
    pub fn authenticator(&self) -> Arc<dyn Authenticator> {
        self.auth.borrow().clone()
    }
    /// The **current** authorizer — re-read on every publish/subscribe so a reload reaches
    /// live connections (ADR 0032).
    #[must_use]
    pub fn authorizer(&self) -> Arc<dyn Authorizer> {
        self.authz.borrow().clone()
    }
}

/// A read-only [`watch::Receiver`] around a fixed authenticator, for tests and
/// non-reloadable callers (the sender is dropped; `borrow()` still returns the value).
#[must_use]
pub fn auth_handle(a: Arc<dyn Authenticator>) -> watch::Receiver<Arc<dyn Authenticator>> {
    watch::channel(a).1
}

/// A read-only [`watch::Receiver`] around a fixed authorizer (see [`auth_handle`]).
#[must_use]
pub fn authz_handle(a: Arc<dyn Authorizer>) -> watch::Receiver<Arc<dyn Authorizer>> {
    watch::channel(a).1
}

/// Drive one accepted plaintext TCP connection to completion, logging any error.
///
/// Test-only convenience path: anonymous clients are permitted, no transport
/// identity is attached, and authorization is open. Production listeners go
/// through [`handle_stream`] with the operator-configured [`ConnPolicy`].
pub async fn handle(stream: TcpStream, hub: mpsc::UnboundedSender<HubCommand>) {
    let peer = stream.peer_addr().ok();
    let policy = Arc::new(ConnPolicy {
        auth: auth_handle(Arc::new(BasicAuthenticator {
            allow_anonymous: true,
        })),
        authz: authz_handle(Arc::new(AllowAll)),
        identity_source: IdentitySource::default(),
        audit: Arc::new(AuditLog::new()),
        proxy: None,
        node: None,
        store: None,
        connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        enhanced: None,
        shutdown: None,
        metrics: None,
    });
    handle_stream(stream, peer, None, policy, hub).await;
}

/// How a connection ended, for the accept-loop wrapper: today just whether the
/// CONNECT failed **authentication** (never authorization) — the fact the
/// auth-failure penalty box records per source address (ADR 0041 T2).
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnOutcome {
    /// The connection's CONNECT was rejected by the authenticator.
    pub auth_failed: bool,
}

/// Drive one accepted connection over any transport (TCP, TLS) to completion,
/// logging any error. `peer` is the remote address, for diagnostics only.
/// `cert` is the TLS-verified mTLS admission (identity + leaf serial), `None` on
/// plaintext or no-client-cert connections; `policy` decides authentication,
/// authorization, and auditing. Returns the [`ConnOutcome`] the admission gate
/// consumes (ADR 0041 T2).
pub async fn handle_stream<S>(
    stream: S,
    peer: Option<SocketAddr>,
    cert: Option<CertAdmission>,
    policy: Arc<ConnPolicy>,
    hub: mpsc::UnboundedSender<HubCommand>,
) -> ConnOutcome
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let auth_failed = std::sync::atomic::AtomicBool::new(false);
    if let Err(e) = run(stream, cert, &policy, hub, &auth_failed).await {
        warn!(?peer, error = %e, "connection ended with error");
    }
    ConnOutcome {
        auth_failed: auth_failed.load(Ordering::Relaxed),
    }
}

async fn run<S>(
    stream: S,
    cert: Option<CertAdmission>,
    policy: &ConnPolicy,
    hub: mpsc::UnboundedSender<HubCommand>,
    auth_failed: &std::sync::atomic::AtomicBool,
) -> Result<(), NetError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (rh, wh) = tokio::io::split(stream);
    let reader = FrameReader::new(rh, ProtocolVersion::V311);
    let writer = FrameWriter::new(wh, ProtocolVersion::V311);
    // A directly-accepted client may be relocated to its placement owner; it has no
    // relaying node (`via = None`).
    run_framed(reader, writer, cert, policy, hub, true, None, auth_failed).await
}

/// Serve an MQTT connection over already-framed halves. `allow_proxy` is `true`
/// for a directly-accepted client (which may be relocated to its owner,
/// ADR 0005) and `false` for a session already proxied here (it is served
/// locally — this node is the owner; re-proxying would loop).
/// Translate a CONNECT into the version-agnostic session policy the hub speaks
/// (ADR 0009): `(clean_start, session_expiry)`. v3.1.1 `clean_session` maps to clean
/// start plus an expiry of 0 (discard at disconnect) or `u32::MAX` (keep forever); v5
/// carries clean start in the same flag and the Session Expiry Interval as a property
/// (absent = 0).
fn session_policy(connect: &Connect) -> (bool, u32) {
    let clean_start = connect.clean_session;
    let session_expiry = match connect.protocol {
        ProtocolVersion::V5 => connect.properties.session_expiry_interval().unwrap_or(0),
        ProtocolVersion::V311 => {
            if clean_start {
                0
            } else {
                u32::MAX
            }
        }
    };
    (clean_start, session_expiry)
}

/// Resolve whether a CONNECT should be relocated to another node (ADR 0005):
/// `Some((proxy, owner, addr))` when proxying is allowed, the session is retained
/// (survives disconnect), this node has a `ProxyContext`, and the placement ring names
/// a remote owner whose address is known. `None` keeps the session local.
fn relocation_target<'a>(
    policy: &'a ConnPolicy,
    client: &ClientId,
    allow_proxy: bool,
    persistent: bool,
) -> Option<(&'a ProxyContext, NodeId, String)> {
    if !allow_proxy || !persistent {
        return None;
    }
    let proxy = policy.proxy.as_ref()?;
    let (owner, addr) = proxy
        .placement
        .read()
        .ok()
        .and_then(|p| p.owner_route(&client.0))?;
    Some((proxy, owner, addr))
}

/// If the CONNECT carries a will whose topic the client may not publish to, send the
/// rejecting CONNACK and return `true` (the caller must close). `false` when there is
/// no will or it is authorized.
async fn will_rejected<W: AsyncWrite + Unpin>(
    writer: &mut FrameWriter<W>,
    connect: &Connect,
    client: &ClientId,
    principal: &Identity,
    policy: &ConnPolicy,
) -> Result<bool, NetError> {
    let Some(w) = &connect.last_will else {
        return Ok(false);
    };
    if policy
        .authorizer()
        .authorize_publish(principal, client, &w.topic)
    {
        return Ok(false);
    }
    warn!(client = %client.0, topic = %w.topic, "CONNECT rejected: will topic not authorized");
    count_connection_error(policy, "acl");
    policy.audit.record(
        "acl.deny.will",
        Some(&principal.subject),
        &format!("will topic {}", w.topic),
    );
    writer
        .send(&Packet::ConnAck(ConnAck {
            properties: mqtt_codec::Properties::new(),
            session_present: false,
            code: connack_code(CONNACK_NOT_AUTHORIZED, connect.protocol),
        }))
        .await?;
    Ok(true)
}

/// Read the connection's first packet, which must be a CONNECT arriving within
/// `deadline`. `Ok(None)` means the connection must close — a timeout, EOF, or a
/// non-CONNECT first packet; `Err` is a transport/codec error.
async fn read_connect<R: AsyncRead + Unpin>(
    reader: &mut FrameReader<R>,
    deadline: Duration,
) -> Result<Option<Connect>, NetError> {
    let Ok(framed) = tokio::time::timeout(deadline, reader.next_packet()).await else {
        warn!(
            timeout_s = deadline.as_secs(),
            "no CONNECT before deadline; closing"
        );
        return Ok(None);
    };
    match framed? {
        Some(Packet::Connect(c)) => Ok(Some(c)),
        Some(other) => {
            warn!(packet = ?other.packet_type(), "first packet was not CONNECT; closing");
            Ok(None)
        }
        None => Ok(None),
    }
}

// A flat CONNECT→serve sequence: long by the number of handshake/attach outcomes it maps,
// not by branching complexity.
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn run_framed<R, W>(
    mut reader: FrameReader<R>,
    mut writer: FrameWriter<W>,
    cert: Option<CertAdmission>,
    policy: &ConnPolicy,
    hub: mpsc::UnboundedSender<HubCommand>,
    allow_proxy: bool,
    via: Option<String>,
    auth_failed: &std::sync::atomic::AtomicBool,
) -> Result<(), NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // CONNECT must be the first packet and arrive within the connect deadline, else
    // the connection is closed (bounds the unauthenticated half-open / slow-loris
    // surface; the keepalive timer only starts after CONNECT).
    let Some(connect) = read_connect(&mut reader, policy.connect_timeout).await? else {
        return Ok(());
    };

    // Negotiate the version the CONNECT declared; every later packet and the CONNACK
    // is framed at it (a no-op for the v3.1.1 readers/writers created above).
    reader.set_version(connect.protocol);
    writer.set_version(connect.protocol);

    // Client-id validation may already reject the CONNECT.
    let Some((client, server_assigned_id)) =
        validate_connect(&mut writer, &connect, policy.node.as_ref()).await?
    else {
        return Ok(());
    };

    // Authentication gate: verify credentials BEFORE attaching to the hub, so a
    // rejected client never touches session state (enhanced exchange or single-shot).
    let Some((principal, auth_method)) = authenticate(
        &mut reader,
        &mut writer,
        &client,
        &connect,
        cert.as_ref().map(|c| &c.identity),
        policy,
        via,
    )
    .await?
    else {
        // The penalty box (ADR 0041 T2) keys on this: authentication failed —
        // authorization denials below never set it.
        auth_failed.store(true, Ordering::Relaxed);
        return Ok(()); // rejected; CONNACK/close already handled
    };

    // Optional connect ACL (ADR 0031 option B): the policy may constrain which client ids this
    // identity may claim. Checked before relocation/attach so a refused connect never touches
    // session state. Default policy permits every connect, so this is a no-op unless configured.
    if !policy.authorizer().authorize_connect(&principal, &client) {
        info!(
            client = %client.0, identity = %principal.subject,
            "rejecting CONNECT: connect ACL denies this client id for the identity (ADR 0031)"
        );
        policy.audit.record(
            "acl.deny.connect",
            Some(&principal.subject),
            &format!("client {}", client.0),
        );
        let code = connack_code(CONNACK_NOT_AUTHORIZED, connect.protocol);
        return reject_connack(&mut writer, code).await;
    }

    // Version-agnostic session policy (ADR 0009): clean-start + retention interval.
    let (clean_start, session_expiry) = session_policy(&connect);

    // Session affinity (ADR 0005): a retained session whose placement owner is another
    // node is relocated there. The owner serves it (CONNACK onward); this node only
    // relays. Non-retained sessions and owner-is-self stay local.
    if let Some((proxy, owner, addr)) =
        relocation_target(policy, &client, allow_proxy, session_expiry != 0)
    {
        info!(client = %client.0, owner = %owner.0, "relocating persistent session to its owner (ADR 0005)");
        return proxy_to_owner(reader, writer, &connect, &principal, proxy, &addr).await;
    }

    // A will is a deferred publish: authorize it at CONNECT, not at the moment of
    // death (ADR 0004 step 3). An unauthorized will closes with a rejecting CONNACK.
    if will_rejected(&mut writer, &connect, &client, &principal, policy).await? {
        return Ok(());
    }

    let conn_id = CONN_ID.fetch_add(1, Ordering::Relaxed);
    let will = connect.last_will.map(into_will);
    // The writer half owns `out_depth` and decrements it as it drains, so the hub
    // can see how far behind this client is and shed `QoS 0` rather than queue it
    // without limit (#123).
    let (raw_out_tx, mut out_rx) = mpsc::unbounded_channel();
    let (out_tx, out_depth) = Outbound::new(raw_out_tx);
    let (reply_tx, reply_rx) = oneshot::channel();
    // The client's Receive Maximum bounds how many unacked QoS>0 PUBLISHes the hub
    // may have outstanding to it (ADR 0012); 0/absent means unlimited.
    let receive_maximum = client_receive_maximum(connect.protocol, &connect.properties);
    // Attach before sending CONNACK so we cannot miss a publish that races in, and
    // so the hub can tell us whether a session was already present.
    if hub
        .send(HubCommand::Attach {
            client: client.clone(),
            admission: Admission {
                identity: principal.clone(),
                method: auth_method,
                cert_serial: cert.and_then(|c| c.serial),
                protocol: connect.protocol,
            },
            conn_id,
            clean_start,
            session_expiry,
            receive_maximum,
            will,
            outbound: out_tx,
            reply: reply_tx,
        })
        .is_err()
    {
        return Ok(()); // hub shut down
    }
    let session_present = match reply_rx.await {
        Ok(AttachOutcome::Present(present)) => present,
        Ok(AttachOutcome::Unavailable) => {
            // Durable session not recoverable yet (lease handoff / no quorum): reject
            // with Server unavailable so the client retries, rather than fabricate a
            // clean session over a recoverable one (ADR 0017).
            info!(client = %client.0, "rejecting CONNECT: durable session unavailable, retry");
            let code = connack_code(CONNACK_SERVER_UNAVAILABLE, connect.protocol);
            return reject_connack(&mut writer, code).await;
        }
        Ok(AttachOutcome::QuotaExceeded) => {
            // Creating a new session would exceed the node's session quota
            // (ADR 0041 T4); resumes are never refused for quota, so the client's
            // best move is another node (or later). v5 gets the honest 0x97;
            // v3.1.1 has no quota code — Server unavailable says "try elsewhere".
            info!(client = %client.0, "rejecting CONNECT: session quota exceeded (ADR 0041)");
            let code = if connect.protocol == ProtocolVersion::V5 {
                reason::QUOTA_EXCEEDED
            } else {
                CONNACK_SERVER_UNAVAILABLE
            };
            return reject_connack(&mut writer, code).await;
        }
        Ok(AttachOutcome::OwnerMismatch) => {
            // The persistent session belongs to a different authenticated identity; this
            // principal may not resume or take it over (ADR 0031). Reject Not-authorized
            // and record it in the tamper-evident audit chain.
            info!(
                client = %client.0, identity = %principal.subject,
                "rejecting CONNECT: session bound to a different identity (ADR 0031)"
            );
            policy.audit.record(
                "session.bind.mismatch",
                Some(&principal.subject),
                &format!("client {} is owned by another identity", client.0),
            );
            let code = connack_code(CONNACK_NOT_AUTHORIZED, connect.protocol);
            return reject_connack(&mut writer, code).await;
        }
        Err(_) => return Ok(()), // hub dropped the reply (shutdown or superseded)
    };
    // Build the v5 CONNACK properties (Topic Alias Maximum, Receive Maximum) and the
    // per-connection alias maps (ADR 0011, ADR 0012).
    let (mut connack_props, mut inbound_aliases, mut outbound_aliases) =
        negotiate_v5_properties(connect.protocol, &connect.properties);
    // MQTT 5.0 §3.2.2.3.7: a client that connects with a zero-length id MUST be told
    // the id the server picked — otherwise it cannot correlate its own session, and
    // anything it reads back (audit, `%c` ACL substitution, our logs) names an
    // identity it has never seen. v3.1.1 has no property to carry it.
    if server_assigned_id && connect.protocol == ProtocolVersion::V5 {
        connack_props
            .0
            .push(mqtt_codec::Property::AssignedClientIdentifier(
                client.0.clone(),
            ));
    }
    writer
        .send(&Packet::ConnAck(ConnAck {
            properties: connack_props,
            session_present,
            code: 0,
        }))
        .await?;
    debug!(client = %client.0, session_present, "CONNECT accepted");
    count_connection_opened(policy, connect.protocol);

    // The connect Authentication Method (if any) bounds a later re-auth (ADR 0013 §4).
    let auth_method = connect
        .properties
        .authentication_method()
        .map(str::to_string);
    let result = serve(
        &mut reader,
        &mut writer,
        &hub,
        &client,
        principal,
        auth_method,
        policy,
        &mut out_rx,
        &out_depth,
        connect.keep_alive,
        connect.protocol == ProtocolVersion::V5,
        // The client's advertised MQTT 5 Maximum Packet Size (ADR 0041 T4): the
        // server must not send it a larger packet [MQTT-3.1.2-24-style contract].
        (connect.protocol == ProtocolVersion::V5)
            .then(|| connect.properties.maximum_packet_size())
            .flatten(),
        &mut inbound_aliases,
        &mut outbound_aliases,
    )
    .await;
    count_connection_closed(policy);
    // Always deregister, even on error. The hub ignores this if we were taken
    // over. Only a clean DISCONNECT (and the graceful-shutdown drain, where the
    // SERVER is going away and the session is retained) is graceful; anything else
    // — EOF, keepalive expiry, protocol violation, a refusal v3.1.1 can only say by
    // hanging up — fires the will [MQTT-3.14.4-3]. `serve` narrows that to a single
    // bool by way of [`PacketOutcome`], so a broker-initiated close can never be
    // reported as a client DISCONNECT (issue #238).
    let graceful = matches!(result, Ok(true));
    let _ = hub.send(HubCommand::Detach {
        client,
        conn_id,
        graceful,
    });
    result.map(|_| ())
}

/// Relocate an authenticated persistent session to its owner (ADR 0005): open a
/// connection to the owner's peer listener, vouch for the client's identity with
/// a [`PeerMessage::ProxyHello`], replay the original CONNECT and any buffered
/// client bytes, then splice the client stream to the owner — which serves the
/// real session. This node never attaches the session locally.
#[allow(clippy::similar_names)] // client_rh/client_wh and owner_rh/owner_wh are clear half names
async fn proxy_to_owner<R, W>(
    reader: FrameReader<R>,
    writer: FrameWriter<W>,
    connect: &Connect,
    principal: &Identity,
    proxy: &ProxyContext,
    addr: &str,
) -> Result<(), NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let (client_rh, leftover) = reader.into_parts();
    let client_wh = writer.into_inner();

    // The owner reads: the ProxyHello frame (vouching the identity this node
    // authenticated — including the "anonymous" principal, so the owner applies
    // the same decision), then the raw MQTT stream (the original CONNECT, any
    // already-buffered client bytes, and via the splice everything next).
    let mut prelude = Vec::new();
    mqtt_cluster::peer::encode(
        &mqtt_cluster::peer::PeerMessage::ProxyHello {
            identity: Some(principal.subject.clone()),
            via: Some(proxy.node.0.clone()),
        },
        &mut prelude,
    )
    .map_err(|e| NetError::Io(std::io::Error::other(e.to_string())))?;
    // Re-encode the CONNECT at its own negotiated version so the owner sees (and
    // serves) the same v3.1.1 or v5 session the client opened.
    Packet::Connect(connect.clone()).encode(&mut prelude, connect.protocol)?;
    prelude.extend_from_slice(&leftover);

    if let Some(connector) = proxy.connector.as_ref().map(|w| w.borrow().clone()) {
        let name = mqtt_net::tls::server_name(addr)?;
        let tcp = TcpStream::connect(addr).await?;
        let _ = tcp.set_nodelay(true);
        let owner = connector.connect(name, tcp).await?;
        splice(client_rh, client_wh, prelude, owner).await
    } else {
        let owner = TcpStream::connect(addr).await?;
        let _ = owner.set_nodelay(true);
        splice(client_rh, client_wh, prelude, owner).await
    }
}

/// Write `prelude` to the owner, then relay the client and owner streams in both
/// directions with **proper half-close**: when one side reaches EOF its peer's
/// write half is shut down, but the other direction keeps relaying until it too
/// closes. So a final PUBLISH/PUBACK/DISCONNECT the owner sends after the client
/// has stopped writing still reaches the client — the previous select-of-two-copies
/// dropped it the instant either direction ended.
#[allow(clippy::similar_names)] // client_rh/client_wh are clear half names
async fn splice<R, W, O>(
    client_rh: R,
    client_wh: W,
    prelude: Vec<u8>,
    mut owner: O,
) -> Result<(), NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    O: AsyncRead + AsyncWrite + Unpin,
{
    owner.write_all(&prelude).await?;
    owner.flush().await?;
    // Rejoin the client halves into one duplex stream so copy_bidirectional can
    // drive (and half-close) both directions. A reset/error at teardown is not
    // failure-worthy — the session simply ended — so the relay result is ignored.
    let mut client = tokio::io::join(client_rh, client_wh);
    let _ = tokio::io::copy_bidirectional(&mut client, &mut owner).await;
    Ok(())
}

/// Serve a session proxied to this node by another (ADR 0005): this node is the
/// session's owner. `prefix` holds the client's MQTT bytes already read past the
/// [`PeerMessage::ProxyHello`] marker; `identity` is the vouched, already-
/// authenticated client identity. The session is served locally and never
/// re-proxied.
// A thin wiring shim onto run_framed; every arg is the stream/identity/policy it
// needs to serve the relocated session, so the count is inherent.
#[allow(clippy::similar_names, clippy::too_many_arguments)]
pub async fn serve_proxied<R, W>(
    client_rh: R,
    client_wh: W,
    peer: Option<SocketAddr>,
    identity: Option<Identity>,
    policy: Arc<ConnPolicy>,
    hub: mpsc::UnboundedSender<HubCommand>,
    prefix: bytes::BytesMut,
    via: Option<String>,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let reader = FrameReader::with_buffer(client_rh, ProtocolVersion::V311, prefix);
    let writer = FrameWriter::new(client_wh, ProtocolVersion::V311);
    // A proxied session is never re-proxied (`allow_proxy = false`); `via` is the
    // relaying node, recorded in the auth audit. The vouched identity carries no
    // leaf serial — the landing node holds the actual TLS session (ADR 0040 T1).
    let cert = identity.map(|identity| CertAdmission {
        identity,
        serial: None,
    });
    // The auth-failure flag is deliberately dropped here: a proxied stream's peer
    // address is the RELAYING NODE (ADR 0005), which must never be penalized for
    // a client's bad credentials (ADR 0041 T2).
    let auth_failed = std::sync::atomic::AtomicBool::new(false);
    if let Err(e) = run_framed(reader, writer, cert, &policy, hub, false, via, &auth_failed).await {
        warn!(?peer, error = %e, "proxied session ended with error");
    }
}

/// The bounded `{protocol}` metric label for a negotiated version (ADR 0020).
fn protocol_label(version: ProtocolVersion) -> &'static str {
    match version {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5",
    }
}

/// Record a successful CONNACK on the shared metrics registry, if enabled (ADR 0020).
fn count_connection_opened(policy: &ConnPolicy, version: ProtocolVersion) {
    if let Some(m) = &policy.metrics {
        m.connection_opened(protocol_label(version));
    }
}

/// Record a connection teardown on the shared metrics registry, if enabled (ADR 0020).
fn count_connection_closed(policy: &ConnPolicy) {
    if let Some(m) = &policy.metrics {
        m.connection_closed();
    }
}

/// Record a failed handshake on the shared metrics registry, if enabled; `reason`
/// is a bounded class (`"auth"`, `"acl"`, …) — never a per-client value (ADR 0020).
fn count_connection_error(policy: &ConnPolicy, reason: &str) {
    if let Some(m) = &policy.metrics {
        m.connection_error(reason);
    }
}

/// Map a v3.1.1 CONNACK return code to the MQTT 5.0 reason code for the same
/// failure (the two code spaces differ); a no-op for v3.1.1 and for success (0x00).
fn connack_code(v3: u8, version: ProtocolVersion) -> u8 {
    if version != ProtocolVersion::V5 {
        return v3;
    }
    match v3 {
        CONNACK_UNACCEPTABLE_PROTOCOL => 0x84, // Unsupported Protocol Version
        CONNACK_SERVER_UNAVAILABLE => 0x88,    // Server unavailable
        CONNACK_IDENTIFIER_REJECTED => 0x85,   // Client Identifier not valid
        CONNACK_BAD_CREDENTIALS => 0x86,       // Bad User Name or Password
        CONNACK_NOT_AUTHORIZED => 0x87,        // Not authorized
        other => other,
    }
}

/// Validate the client id of a CONNECT, replying with the rejecting CONNACK and
/// returning `None` when it must close. An empty client id is only valid with clean
/// session (the server assigns an id); pairing it with a persistent session is
/// rejected per spec. The protocol version itself is already negotiated (v3.1.1 and
/// v5 are both accepted; an unknown level is refused at the codec).
/// Build the id for a client that sent none.
///
/// `node` is this node's id when clustered (`None` when not). It is part of the id
/// because [`AUTO_ID`] is **per process**: every node started its counter at 1, so
/// every node's first zero-id client was `auto-1`, and two unrelated clients on two
/// nodes ended up sharing one session identity — which is exactly what the cluster
/// keys session ownership by. Unclustered there is one process, so the counter
/// alone is already unique among live clients.
///
/// Uniqueness only has to hold among *live* clients: a zero-length id is legal only
/// with `clean_session`, so nothing is ever persisted under one of these.
fn assigned_client_id(node: Option<&NodeId>, n: u64) -> String {
    match node {
        Some(node) => format!("auto-{}-{n}", node.0),
        None => format!("auto-{n}"),
    }
}

/// Validate the CONNECT's client id, assigning one when the client sent none.
///
/// Returns the id and whether the **server** assigned it — the caller needs that
/// to satisfy MQTT 5.0 §3.2.2.3.7, which requires an assigned id to be handed back
/// in the CONNACK.
///
/// `node` is this node's id when clustered. It is part of an assigned id because
/// the counter behind it is **per process**: two nodes each started at 1 and each
/// handed out `auto-1`, so two unrelated clients on different nodes ended up
/// sharing one session identity — and session ownership across the cluster is keyed
/// by exactly that. Uniqueness among *live* clients is all that is required here
/// (an empty id is only legal with `clean_session`, so nothing is persisted under
/// it), and node id + monotonic counter gives that: the counter is unique within a
/// node's lifetime, and the node id is unique within the cluster.
async fn validate_connect<W>(
    writer: &mut FrameWriter<W>,
    connect: &Connect,
    node: Option<&NodeId>,
) -> Result<Option<(ClientId, bool)>, NetError>
where
    W: AsyncWrite + Unpin,
{
    if connect.client_id.is_empty() {
        if !connect.clean_session {
            // A zero-length id has no session to resume, so a persistent session
            // cannot be built on one — the spec's Identifier Rejected case.
            writer
                .send(&Packet::ConnAck(ConnAck {
                    properties: mqtt_codec::Properties::new(),
                    session_present: false,
                    code: connack_code(CONNACK_IDENTIFIER_REJECTED, connect.protocol),
                }))
                .await?;
            return Ok(None);
        }
        let n = AUTO_ID.fetch_add(1, Ordering::Relaxed);
        return Ok(Some((ClientId(assigned_client_id(node, n)), true)));
    }
    Ok(Some((ClientId(connect.client_id.clone()), false)))
}

/// The authentication gate: run the MQTT 5.0 enhanced (AUTH) exchange when the
/// CONNECT names an Authentication Method (ADR 0013), otherwise the single-shot
/// credential check. Returns `None` (with the rejecting CONNACK/close already sent)
/// when the client is refused.
#[allow(clippy::too_many_arguments)] // the full authentication context
async fn authenticate<R, W>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    client: &ClientId,
    connect: &Connect,
    identity: Option<&Identity>,
    policy: &ConnPolicy,
    via: Option<String>,
) -> Result<Option<(Identity, AuthMethod)>, NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if let Some(method) = connect.properties.authentication_method() {
        Ok(
            enhanced_auth(reader, writer, client, connect, method, policy)
                .await?
                .map(|id| (id, AuthMethod::Enhanced)),
        )
    } else {
        authenticate_connect(writer, client, connect, identity, policy, via).await
    }
}

/// If `password` is a compact-JWS-shaped bearer token — three non-empty
/// base64url segments separated by two dots, valid UTF-8 — return it as `&str`.
/// This is the shape a JWT has on the wire (`header.payload.signature`); it is the
/// trigger for carrying a password as [`Credentials::Token`] (ADR 0050). Deliberately
/// structural, not a decode: the authenticator does the real verification, and a
/// non-token password can never accidentally match (a bcrypt/plain password has no
/// two-dot base64url structure).
fn jwt_password_str(password: &[u8]) -> Option<&str> {
    let s = std::str::from_utf8(password).ok()?;
    let mut parts = s.split('.');
    let (a, b, c, rest) = (parts.next()?, parts.next()?, parts.next()?, parts.next());
    if rest.is_some() || a.is_empty() || b.is_empty() || c.is_empty() {
        return None;
    }
    let base64url = |seg: &str| {
        seg.bytes()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == b'-' || ch == b'_')
    };
    (base64url(a) && base64url(b) && base64url(c)).then_some(s)
}

/// Authenticate the CONNECT against the listener policy. Credentials priority:
/// a TLS-verified certificate identity wins; otherwise a JWT-shaped password when a
/// token verifier is configured (ADR 0050); otherwise CONNECT username/password;
/// otherwise anonymous (only honored when the policy opts in). On failure this sends
/// the rejecting CONNACK — 0x04 (bad user name or password) for password credentials,
/// 0x05 (not authorized) otherwise — and returns `Ok(None)`: the caller must close
/// without attaching to the hub.
async fn authenticate_connect<W>(
    writer: &mut FrameWriter<W>,
    client: &ClientId,
    connect: &Connect,
    identity: Option<&Identity>,
    policy: &ConnPolicy,
    via: Option<String>,
) -> Result<Option<(Identity, AuthMethod)>, NetError>
where
    W: AsyncWrite + Unpin,
{
    // mTLS identity outranks any wire credential (ADR 0004). Otherwise, when a token
    // verifier is configured, a JWT-shaped password is carried as a bearer token — the
    // ecosystem convention (EMQX/HiveMQ: the JWT rides in the password field), and the
    // only path by which a real client can reach the token/OIDC authenticators
    // (ADR 0050): there is no other `Credentials::Token` construction site. The shape
    // check gates on a token authenticator being present, so password-auth deployments
    // are untouched; a misroute only ever fails auth (fail-closed), never escalates.
    let token = connect
        .password
        .as_deref()
        .filter(|_| policy.authenticator().handles_token())
        .and_then(jwt_password_str);
    let creds = match (identity, token, &connect.username) {
        (Some(id), _, _) => Credentials::ClientCert {
            subject: &id.subject,
        },
        (None, Some(jwt), _) => Credentials::Token(jwt),
        (None, None, Some(username)) => Credentials::Password {
            username,
            password: connect.password.as_deref().unwrap_or(&[]),
        },
        (None, None, None) => Credentials::Anonymous,
    };
    let auth_method = match creds {
        Credentials::ClientCert { .. } => AuthMethod::Certificate,
        Credentials::Password { .. } => AuthMethod::Password,
        Credentials::Token(_) => AuthMethod::Token,
        Credentials::Anonymous => AuthMethod::Anonymous,
    };
    let method = match creds {
        Credentials::ClientCert { .. } => "certificate",
        Credentials::Password { .. } => "password",
        Credentials::Token(_) => "token",
        Credentials::Anonymous => "anonymous",
    };
    // Awaited, not blocked on: an authenticator may be remote (HTTP hook, LDAP, token
    // introspection). Nothing here bounds how long it takes — an I/O-backed
    // implementation owns its own timeout, and must fail closed when it expires.
    match policy.authenticator().authenticate(client, &creds).await {
        Ok(id) => {
            // For a relocated session, attribute it to the node that vouched (ADR
            // 0005); a direct client has no `via`.
            let relayed = via.map_or_else(String::new, |node| format!(" (relayed by node {node})"));
            policy.audit.record(
                "auth.success",
                Some(&id.subject),
                &format!("client {} via {method}{relayed}", client.0),
            );
            Ok(Some((id, auth_method)))
        }
        Err(e) => {
            let code = if matches!(creds, Credentials::Password { .. }) {
                CONNACK_BAD_CREDENTIALS
            } else {
                CONNACK_NOT_AUTHORIZED
            };
            warn!(client = %client.0, error = %e, "CONNECT rejected: authentication failed");
            count_connection_error(policy, "auth");
            // The subject is the client id, not a credential — never log secrets.
            policy.audit.record(
                "auth.failure",
                Some(&client.0),
                &format!("rejected {method} credentials"),
            );
            writer
                .send(&Packet::ConnAck(ConnAck {
                    properties: mqtt_codec::Properties::new(),
                    session_present: false,
                    code: connack_code(code, connect.protocol),
                }))
                .await?;
            Ok(None)
        }
    }
}

/// Send a v5 CONNACK that refuses the connection with `code` and no session.
async fn reject_connack<W: AsyncWrite + Unpin>(
    writer: &mut FrameWriter<W>,
    code: u8,
) -> Result<(), NetError> {
    writer
        .send(&Packet::ConnAck(ConnAck {
            properties: mqtt_codec::Properties::new(),
            session_present: false,
            code,
        }))
        .await
}

/// Run the MQTT 5.0 enhanced-authentication (AUTH) exchange for a CONNECT that named
/// an Authentication Method (ADR 0013). Returns the authenticated [`Identity`], or
/// `None` when the connection was rejected/closed (the CONNACK or close is handled
/// here). The exchange runs before the CONNACK, so a failure never attaches a session.
async fn enhanced_auth<R, W>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    client: &ClientId,
    connect: &Connect,
    method: &str,
    policy: &ConnPolicy,
) -> Result<Option<Identity>, NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // The named method must match a configured mechanism, else Bad Auth Method.
    let Some(authenticator) = policy.enhanced.as_ref().filter(|a| a.method() == method) else {
        warn!(client = %client.0, method, "unsupported authentication method");
        reject_connack(writer, CONNACK_V5_BAD_AUTH_METHOD).await?;
        return Ok(None);
    };

    let mut session = authenticator.start();
    // The CONNECT's initial Authentication Data seeds the exchange.
    let first = session.step(
        client,
        connect.properties.authentication_data().unwrap_or_default(),
    );
    match drive_auth_exchange(reader, writer, client, method, &mut session, first).await? {
        ExchangeResult::Success(id) => {
            policy.audit.record(
                "auth.success",
                Some(&id.subject),
                &format!("client {} via enhanced:{method}", client.0),
            );
            Ok(Some(id))
        }
        ExchangeResult::Failed => {
            warn!(client = %client.0, method, "enhanced authentication failed");
            count_connection_error(policy, "auth");
            policy.audit.record(
                "auth.failure",
                Some(&client.0),
                &format!("rejected enhanced:{method}"),
            );
            reject_connack(writer, CONNACK_V5_NOT_AUTHORIZED).await?;
            Ok(None)
        }
        ExchangeResult::Aborted => Ok(None),
    }
}

/// The terminal outcome of an enhanced-auth challenge/response exchange (ADR 0013).
enum ExchangeResult {
    /// The mechanism authenticated the client as this identity.
    Success(Identity),
    /// The mechanism rejected the client.
    Failed,
    /// The exchange could not complete (EOF, an unexpected packet, or the method
    /// changed mid-exchange): the caller must just close, with no further packet.
    Aborted,
}

/// Drive the challenge/response rounds of an exchange, starting from `first` (the
/// step the mechanism produced from the initiating packet's data). Sends
/// AUTH(Continue) challenges and reads the client's AUTH(Continue) replies,
/// enforcing that the Authentication Method is held constant, until the mechanism
/// resolves. Shared by connect-time enhanced auth and mid-session re-auth.
async fn drive_auth_exchange<R, W>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    client: &ClientId,
    method: &str,
    session: &mut Box<dyn AuthSession>,
    first: AuthStep,
) -> Result<ExchangeResult, NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut step = first;
    loop {
        match step {
            AuthStep::Success(id) => return Ok(ExchangeResult::Success(id)),
            AuthStep::Failure => return Ok(ExchangeResult::Failed),
            AuthStep::Challenge(data) => {
                let mut props = mqtt_codec::Properties::new();
                props.0.push(mqtt_codec::Property::AuthenticationMethod(
                    method.to_string(),
                ));
                props
                    .0
                    .push(mqtt_codec::Property::AuthenticationData(Bytes::from(data)));
                writer
                    .send(&Packet::Auth(Auth {
                        reason: AUTH_CONTINUE,
                        properties: props,
                    }))
                    .await?;

                // The reply must be an AUTH(Continue) keeping the same method, and must
                // arrive within the per-round auth timeout (ADR 0013 §3) — a stalled round
                // must not pin the connection (the keepalive timer is not yet running).
                let next =
                    tokio::time::timeout(wire_limits().auth_round_timeout, reader.next_packet())
                        .await;
                let reply = match next {
                    Err(_elapsed) => {
                        warn!(client = %client.0, "enhanced-auth round timed out; aborting");
                        return Ok(ExchangeResult::Aborted);
                    }
                    Ok(inbound) => match inbound? {
                        Some(Packet::Auth(a)) if a.reason == AUTH_CONTINUE => a,
                        Some(other) => {
                            warn!(client = %client.0, packet = ?other.packet_type(),
                                  "expected AUTH(continue); aborting auth exchange");
                            return Ok(ExchangeResult::Aborted);
                        }
                        None => return Ok(ExchangeResult::Aborted), // EOF mid-exchange
                    },
                };
                if reply.properties.authentication_method() != Some(method) {
                    warn!(client = %client.0, "AUTH method changed mid-exchange; aborting");
                    return Ok(ExchangeResult::Aborted);
                }
                step = session.step(
                    client,
                    reply.properties.authentication_data().unwrap_or_default(),
                );
            }
        }
    }
}

/// Convert a CONNECT's Last Will into a deferred will [`Message`], carrying the will's
/// application properties so a published will forwards them too (MQTT-3.3.2-17, ADR 0030).
fn into_will(w: mqtt_codec::packet::LastWill) -> Message {
    let app = app_properties(&w.properties);
    Message {
        topic: w.topic,
        payload: w.payload,
        qos: w.qos,
        retain: w.retain,
        app,
        // A will's Message Expiry Interval counts from PUBLICATION, which has not
        // happened yet — the publish path stamps the deadline then (issue #227
        // keeps will semantics unchanged).
        expires_at: None,
    }
}

/// Extract the forwardable MQTT 5 application properties from a property block (ADR 0030):
/// Payload Format Indicator, Content Type, Response Topic, Correlation Data, and User
/// Properties (in wire order). Connection/subscription-scoped properties (Topic Alias,
/// Subscription Identifier) and Message Expiry (handled separately) are not included.
///
/// Since issue #245 an inbound Subscription Identifier never reaches here to be dropped:
/// `handle_publish` refuses the packet outright per `[MQTT-3.3.4-6]`, so the `_ => {}` arm
/// below is no longer load-bearing for `0x0B`.
fn app_properties(props: &mqtt_codec::Properties) -> AppProperties {
    use mqtt_codec::Property;
    let mut app = AppProperties::default();
    for p in &props.0 {
        match p {
            Property::PayloadFormatIndicator(v) => app.payload_format = Some(*v),
            Property::ContentType(s) => app.content_type = Some(s.clone()),
            Property::ResponseTopic(s) => app.response_topic = Some(s.clone()),
            Property::CorrelationData(b) => app.correlation_data = Some(b.clone()),
            Property::UserProperty(k, v) => app.user_properties.push((k.clone(), v.clone())),
            _ => {}
        }
    }
    app
}

/// Send a v5 DISCONNECT with `reason` and no properties.
async fn disconnect<W: AsyncWrite + Unpin>(
    writer: &mut FrameWriter<W>,
    reason: u8,
) -> Result<(), NetError> {
    writer
        .send(&Packet::Disconnect(Disconnect {
            reason,
            properties: mqtt_codec::Properties::new(),
        }))
        .await
}

/// Handle a client-initiated re-authentication (AUTH `0x19`) on an established
/// session (ADR 0013 §4). On success, updates `principal` and answers AUTH(Success);
/// on failure or protocol violation, sends DISCONNECT. Returns `Ok(true)` to keep
/// serving, `Ok(false)` to close.
async fn reauthenticate<R, W>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    client: &ClientId,
    auth: &Auth,
    connect_method: Option<&str>,
    principal: &mut Identity,
    policy: &ConnPolicy,
) -> Result<bool, NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // Only a Re-authenticate (0x19) initiates an exchange in the serve loop; any
    // other AUTH is a protocol error.
    if auth.reason != AUTH_REAUTH {
        warn!(client = %client.0, reason = auth.reason, "unexpected AUTH on established session");
        disconnect(writer, DISCONNECT_PROTOCOL_ERROR).await?;
        return Ok(false);
    }
    // Re-auth requires that connect used enhanced auth, with the **same** method
    // [MQTT-4.12.1-1].
    let Some(method) = auth
        .properties
        .authentication_method()
        .filter(|m| connect_method == Some(*m))
    else {
        warn!(client = %client.0, "re-auth method missing or changed; disconnecting");
        disconnect(writer, DISCONNECT_PROTOCOL_ERROR).await?;
        return Ok(false);
    };
    let Some(authenticator) = policy.enhanced.as_ref().filter(|a| a.method() == method) else {
        disconnect(writer, DISCONNECT_PROTOCOL_ERROR).await?;
        return Ok(false);
    };

    let mut session = authenticator.start();
    let first = session.step(
        client,
        auth.properties.authentication_data().unwrap_or_default(),
    );
    match drive_auth_exchange(reader, writer, client, method, &mut session, first).await? {
        ExchangeResult::Success(id) => {
            info!(client = %client.0, subject = %id.subject, "re-authenticated");
            policy.audit.record(
                "auth.reauth",
                Some(&id.subject),
                &format!("client {} via enhanced:{method}", client.0),
            );
            *principal = id;
            let mut props = mqtt_codec::Properties::new();
            props.0.push(mqtt_codec::Property::AuthenticationMethod(
                method.to_string(),
            ));
            writer
                .send(&Packet::Auth(Auth {
                    reason: AUTH_SUCCESS,
                    properties: props,
                }))
                .await?;
            Ok(true)
        }
        ExchangeResult::Failed => {
            warn!(client = %client.0, "re-authentication failed; disconnecting");
            policy.audit.record(
                "auth.reauth.failure",
                Some(&client.0),
                &format!("rejected enhanced:{method}"),
            );
            disconnect(writer, DISCONNECT_NOT_AUTHORIZED).await?;
            Ok(false)
        }
        ExchangeResult::Aborted => Ok(false),
    }
}

/// Serve the connection until it ends. Returns `Ok(true)` only for a clean
/// client DISCONNECT; every other end (EOF, keepalive expiry, takeover) is
/// ungraceful and will publish the client's will.
/// The outbound Receive Maximum quota for a connection (ADR 0012): the client's
/// advertised value, treating 0/absent as unlimited. v3.1.1 has no such property.
fn client_receive_maximum(protocol: ProtocolVersion, properties: &mqtt_codec::Properties) -> u16 {
    if protocol == ProtocolVersion::V5 {
        properties
            .receive_maximum()
            .filter(|&v| v > 0)
            .unwrap_or(u16::MAX)
    } else {
        u16::MAX
    }
}

/// Build the v5 CONNACK property block and the per-connection topic-alias maps.
///
/// The block is exactly four properties, and no others: Receive Maximum (ADR 0012),
/// Maximum Packet Size (ADR 0041 T4), Topic Alias Maximum when non-zero (ADR 0011), and
/// Subscription Identifiers Available (issue #245). Adding a fifth must also update
/// `v5_connack_advertises_exactly_the_four_negotiated_properties`, which pins the set.
///
/// v3.1.1 has none of these features, so the maps come out disabled and the property
/// block empty.
fn negotiate_v5_properties(
    protocol: ProtocolVersion,
    properties: &mqtt_codec::Properties,
) -> (mqtt_codec::Properties, InboundAliases, OutboundAliases) {
    let is_v5 = protocol == ProtocolVersion::V5;
    let limits = wire_limits();
    let server_alias_max = if is_v5 { limits.topic_alias_max } else { 0 };
    let client_alias_max = if is_v5 {
        properties.topic_alias_maximum().unwrap_or(0)
    } else {
        0
    };
    let mut props = mqtt_codec::Properties::new();
    if is_v5 {
        props
            .0
            .push(mqtt_codec::Property::ReceiveMaximum(limits.receive_maximum));
        // The transport frame cap, stated as the spec's own contract
        // (ADR 0041 T4): a packet beyond it closes the connection, and now the
        // client knows the number instead of discovering the constant.
        props.0.push(mqtt_codec::Property::MaximumPacketSize(
            limits.max_packet_size,
        ));
        // §3.2.2.3.12: omitting 0x29 MEANS "Subscription Identifiers are supported", so
        // state the truth explicitly (issue #245). Inside `if is_v5` is what keeps a
        // v3.1.1 CONNACK byte-identical.
        props
            .0
            .push(mqtt_codec::Property::SubscriptionIdentifierAvailable(
                u8::from(SUB_IDS_SUPPORTED),
            ));
    }
    if server_alias_max > 0 {
        props
            .0
            .push(mqtt_codec::Property::TopicAliasMaximum(server_alias_max));
    }
    (
        props,
        InboundAliases::new(server_alias_max),
        OutboundAliases::new(client_alias_max),
    )
}

#[allow(clippy::too_many_arguments)] // a connection's full serving context
async fn serve<R, W>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
    mut principal: Identity,
    auth_method: Option<String>,
    policy: &ConnPolicy,
    out_rx: &mut mpsc::UnboundedReceiver<Packet>,
    // Decremented as packets are drained, so the hub can see this client's
    // backlog and shed `QoS 0` rather than queue it without limit (#123).
    out_depth: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
    keep_alive: u16,
    is_v5: bool,
    client_max_packet: Option<u32>,
    inbound_aliases: &mut InboundAliases,
    outbound_aliases: &mut OutboundAliases,
) -> Result<bool, NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // [MQTT-3.1.2-24]: close after 1.5x the keepalive with no inbound traffic.
    let grace = (keep_alive > 0).then(|| {
        Duration::from_secs(u64::from(keep_alive) * KEEPALIVE_GRACE_NUM / KEEPALIVE_GRACE_DEN)
    });
    let mut deadline = grace.map(|g| Instant::now() + g);
    // Inbound QoS 2 ids held but not yet PUBREL-released, each with whether its PUBREC
    // was RELEASED: forwarding only on first sight of an ACKNOWLEDGED id is what makes
    // inbound QoS 2 exactly-once [MQTT-4.3.3-2] without fabricating success for a flow
    // the broker never acknowledged (issue #238). The per-connection fallback only ever
    // matters for a clean session, whose window is not required to outlive its
    // connection; a persistent session's window lives in the store.
    let mut qos2_inbound: HashMap<u16, bool> = HashMap::new();
    // Count of distinct unreleased inbound QoS>0 publishes — the client's outstanding
    // window against the server's Receive Maximum (ADR 0012). QoS 1 is acked inline so
    // never accumulates; QoS 2 holds a slot from PUBLISH until PUBREL. Overrun → 0x93.
    let mut qos2_inflight: usize = 0;
    // Per-connection publish-rate limiter (ADR 0041 T3); `None` = unlimited.
    let mut publish_rate = wire_limits().publish_rate.map(PublishRateLimiter::new);

    loop {
        let idle = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            inbound = reader.next_packet() => {
                // Any client packet resets the keepalive deadline.
                deadline = grace.map(|g| Instant::now() + g);
                match inbound? {
                    None => return Ok(false), // EOF without DISCONNECT
                    // An AUTH on an established session is a re-authentication
                    // (ADR 0013 §4); it may update the principal used for ACL checks.
                    Some(Packet::Auth(auth)) => {
                        if !reauthenticate(reader, writer, client, &auth, auth_method.as_deref(), &mut principal, policy).await? {
                            return Ok(false);
                        }
                    }
                    Some(packet) => {
                        // Publish-rate throttle (ADR 0041 T3): an empty bucket pauses
                        // HERE — before processing, with the socket unread — so an
                        // over-rate publisher backs up its own TCP window. Only
                        // publishes take tokens; acks/pings/subscribes flow freely.
                        if let (Some(limiter), Packet::Publish(_)) = (&mut publish_rate, &packet) {
                            limiter.acquire().await;
                        }
                        // Only a client DISCONNECT with reason 0x00 is graceful. A
                        // broker-initiated close — protocol violation, hub gone, or
                        // a refusal v3.1.1 can only say by hanging up — is
                        // UNgraceful, so the Will still fires (issue #238,
                        // [MQTT-3.14.4-3]); so is a v5 DISCONNECT with a non-zero
                        // reason, where the CLIENT asks for its Will (issue #265,
                        // [MQTT-3.1.2-10]).
                        match handle_inbound(packet, writer, hub, client, &principal, policy, &mut qos2_inbound, &mut qos2_inflight, is_v5, inbound_aliases).await? {
                            PacketOutcome::Continue => {}
                            PacketOutcome::ClientDisconnect => return Ok(true),
                            PacketOutcome::ClientDisconnectWithWill
                            | PacketOutcome::BrokerClose => return Ok(false),
                        }
                    }
                }
            }
            maybe_out = out_rx.recv() => {
                if maybe_out.is_some() {
                    out_depth.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                }
                match maybe_out {
                    // Rewrite outbound PUBLISHes to use topic aliases where the
                    // client allowed them (ADR 0011 §3); other packets pass through.
                    Some(mut pkt) => {
                        if let Packet::Publish(p) = &mut pkt {
                            outbound_aliases.apply(p);
                        }
                        // The client's Maximum Packet Size (ADR 0041 T4): a message
                        // too large for THIS subscriber is dropped for it alone,
                        // per spec — measured after the alias rewrite (which only
                        // shrinks), counted, never a connection error.
                        if let (Some(max), Packet::Publish(_)) = (client_max_packet, &pkt) {
                            let mut encoded = Vec::new();
                            let version = if is_v5 {
                                ProtocolVersion::V5
                            } else {
                                ProtocolVersion::V311
                            };
                            if pkt.encode(&mut encoded, version).is_ok()
                                && encoded.len() > max as usize
                            {
                                debug!(client = %client.0, size = encoded.len(), max,
                                       "outbound publish exceeds the client's Maximum Packet Size; dropped for this subscriber");
                                if let Some(m) = &policy.metrics {
                                    m.publish_dropped("too-large");
                                }
                                continue;
                            }
                        }
                        writer.send(&pkt).await?;
                    }
                    // The hub dropped our sender: we were taken over by a new
                    // connection for the same client id, or the hub shut down.
                    None => return Ok(false),
                }
            }
            () = idle => {
                debug!(client = %client.0, keep_alive, "keepalive expired; closing connection");
                count_connection_error(policy, "keepalive");
                return Ok(false);
            }
            () = drain_signal(policy) => {
                // Graceful shutdown (ADR 0019): close cleanly without firing the will —
                // the server is going away, not the client; its session is retained.
                // v5 clients are told *why* with a Server-shutting-down DISCONNECT so they
                // reconnect promptly rather than waiting out a keepalive; v3.1.1 has no
                // server-sent DISCONNECT, so we just close the socket.
                debug!(client = %client.0, "draining connection for shutdown");
                if writer.version() == ProtocolVersion::V5 {
                    // Best-effort: a write failure here just means the client is already
                    // gone, which the graceful close handles anyway.
                    let _ = disconnect(writer, DISCONNECT_SERVER_SHUTTING_DOWN).await;
                }
                return Ok(true);
            }
        }
    }
}

/// Resolve to ready once the policy's shutdown token is cancelled; pends forever when no
/// token is set (so the `select!` arm is a no-op outside graceful shutdown).
async fn drain_signal(policy: &ConnPolicy) {
    match &policy.shutdown {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}

/// Handle one inbound PUBLISH: topic validation, ACL gate, inbound `QoS`
/// handshakes, and the exactly-once dedup window.
///
/// Every close this can ask for is [`PacketOutcome::BrokerClose`] — a protocol
/// violation, a hub that went away, or a refusal v3.1.1 has no reason byte to carry.
/// None of them is a client DISCONNECT, so the client's Will fires (issue #238;
/// [MQTT-3.14.4-3]).
// One arm per (QoS, ack) shape; a flat dispatch, not a refactor smell.
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)] // a connection's full publish-handling context
async fn handle_publish<W: AsyncWrite + Unpin>(
    publish: Publish,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
    principal: &Identity,
    policy: &ConnPolicy,
    qos2_inbound: &mut HashMap<u16, bool>,
    qos2_inflight: &mut usize,
    is_v5: bool,
    inbound_aliases: &mut InboundAliases,
) -> Result<PacketOutcome, NetError> {
    // The MQTT 5.0 Message Expiry Interval (if the publisher set one) bounds how long
    // a queued copy is deliverable (ADR 0009 §3).
    let message_expiry = publish.properties.message_expiry_interval();
    // The publisher's forwardable application properties (User Properties + Content Type,
    // Response Topic, Correlation Data, Payload Format), forwarded unaltered to subscribers
    // (MQTT-3.3.2-17, ADR 0030). Empty for v3.1.1 / a publish without any.
    let app = app_properties(&publish.properties);
    // [MQTT-3.3.4-6], verbatim: "A PUBLISH packet sent from a Client to a Server MUST NOT
    // contain a Subscription Identifier." Until issue #245 this property was silently
    // swallowed by `app_properties` and the publish was acked.
    //
    // 0x82 (Protocol Error) and not 0xA1: §4.13.1 says use 0x81/0x82 "unless a more
    // specific Reason Code has been defined", and 0xA1 means "I do not support the
    // feature", which is not this client's mistake. Unconditional on SUB_IDS_SUPPORTED —
    // this stays a Protocol Error even once the server delivers identifiers.
    if is_v5 && publish.properties.has_subscription_identifier() {
        warn!(client = %client.0,
              "client PUBLISH carries a Subscription Identifier [MQTT-3.3.4-6]; DISCONNECT 0x82");
        disconnect(writer, DISCONNECT_PROTOCOL_ERROR).await?;
        // A broker-initiated close (the client did not DISCONNECT): the Will fires.
        return Ok(PacketOutcome::BrokerClose);
    }
    // Resolve any topic alias to the full topic name before anything else sees it
    // (ADR 0011 §2). An invalid alias is a protocol violation: close the connection.
    let alias = publish.properties.topic_alias();
    let Ok(topic) = inbound_aliases.resolve(&publish.topic, alias) else {
        // An out-of-range or unmapped alias is a protocol error: tell the v5 client why
        // (DISCONNECT 0x94 Topic Alias Invalid, ADR 0011 §2) rather than a bare close. The
        // alias property is v5-only, so reaching here implies a v5 connection.
        warn!(client = %client.0, alias = ?alias, "invalid topic alias; DISCONNECT 0x94");
        disconnect(writer, DISCONNECT_TOPIC_ALIAS_INVALID).await?;
        return Ok(PacketOutcome::BrokerClose);
    };
    let Publish {
        qos,
        pkid,
        payload,
        retain,
        ..
    } = publish;
    // [MQTT-3.3.2-2]: a PUBLISH topic name MUST NOT contain wildcards. This is
    // a protocol violation, not an ACL decision — close the connection rather
    // than letting a `+`/`#` topic reach routing or ACL matching.
    if topic.contains(['+', '#']) {
        warn!(client = %client.0, topic = %topic, "PUBLISH topic contains wildcards; closing connection");
        return Ok(PacketOutcome::BrokerClose);
    }
    // ACL gate (ADR 0004 step 3): an unauthorized publish is dropped before the
    // hub ever sees it, and the denial is audited. What the publisher is TOLD is
    // per version (issue #246): v5 has a reason byte, so its `QoS` 1/2 answer is
    // `0x87 Not authorized` (set at each ack arm below, where `forward` returned
    // `None`); v3.1.1 has no negative PUBACK, and not acking would leave a
    // conforming publisher retrying forever — a retry cannot change an ACL
    // decision — so it keeps the plain success ack. `QoS` 0 has nothing to answer
    // in either version.
    //
    // This deliberately does NOT reuse `PublishRefusal`: that enum is the hub's
    // refusal vocabulary and travels the peer bus under a wire code, while the ACL
    // decision is made right here, before `forward` — it never crosses the hub or
    // the bus, so a variant there would be dead weight in both.
    let authorized = policy
        .authorizer()
        .authorize_publish(principal, client, &topic);
    if !authorized {
        debug!(client = %client.0, identity = %principal.subject, topic = %topic,
               "publish denied by ACL; dropping");
        policy
            .audit
            .record("acl.deny.publish", Some(&principal.subject), &topic);
    }
    // Forward to the hub; the returned receiver resolves once the hub's fan-out —
    // including any durable (fsync'd) offline-queue appends — has completed, so a
    // QoS ≥ 1 acknowledgement can be released only for a message the broker durably
    // owns (ADR 0018). `None` when the publish was dropped by the ACL.
    let forward = |hub: &mpsc::UnboundedSender<HubCommand>|
     -> Option<oneshot::Receiver<crate::hub::PublishOutcome>> {
        if authorized {
            let (done_tx, done_rx) = oneshot::channel();
            let _ = hub.send(HubCommand::Publish {
                topic,
                payload,
                qos,
                retain,
                message_expiry,
                app,
                done: Some(done_tx),
                v5: is_v5,
                publisher: Some(client.clone()), // #198: No Local excludes this publisher
            });
            Some(done_rx)
        } else {
            None
        }
    };
    match (qos, pkid) {
        (QoS::AtMostOnce, _) => {
            let _ = forward(hub); // nothing to acknowledge, nothing to gate
        }
        (QoS::AtLeastOnce, Some(id)) => {
            // Receive Maximum counts QoS 1 and QoS 2 publications TOGETHER
            // [MQTT-3.3.4]: with `qos2_inflight` windows already open, this QoS 1
            // publication would be one more concurrent unacknowledged message —
            // beyond the advertised quota it is a flow-control breach (ADR 0041 T3,
            // finishing the ADR 0012 §3 deferral). v5 only, as for QoS 2.
            if is_v5 && *qos2_inflight >= wire_limits().receive_maximum as usize {
                warn!(client = %client.0, limit = wire_limits().receive_maximum,
                      "QoS 1 publish beyond Receive Maximum; DISCONNECT 0x93");
                disconnect(writer, DISCONNECT_RECEIVE_MAXIMUM_EXCEEDED).await?;
                return Ok(PacketOutcome::BrokerClose);
            }
            let mut ack = mqtt_codec::packet::Ack::from(id);
            if let Some(done) = forward(hub) {
                // The hub disappearing mid-shutdown means the message may never be
                // stored: close without a PUBACK (the publisher retries) rather than
                // acknowledge a message that could be lost.
                match done.await {
                    Err(_) => return Ok(PacketOutcome::BrokerClose),
                    Ok(crate::hub::PublishOutcome::Accepted) => {}
                    // The hub refused the publish under a stated policy (ADR 0041
                    // T4 retained quota, T11 brownout). v5 carries the reason on the
                    // PUBACK; v3.1.1 has no reason byte, so each refusal declares
                    // whether it is sayable as a plain ack or only as no-ack-and-close.
                    // No Reason String property is attached: nothing here tracks
                    // Request Problem Information, and sending one unconditionally
                    // would violate [MQTT-3.1.2-29].
                    Ok(crate::hub::PublishOutcome::Refused(r)) => {
                        if is_v5 {
                            ack.reason = r.v5_reason();
                        } else if r.v311() == crate::hub::Refusal311::CloseNoAck {
                            warn!(client = %client.0, refusal = r.as_str(),
                                  "publish refused and v3.1.1 cannot say so; closing \
                                   without a PUBACK (the publisher retries)");
                            return Ok(PacketOutcome::BrokerClose);
                        }
                    }
                }
            } else if is_v5 {
                // The ACL denied this publish (`forward` returned `None`) and v5
                // can say so: PUBACK 0x87 Not authorized (issue #246). Per-publish,
                // not a connection verdict — the connection stays open. v3.1.1
                // falls through to the plain ack: it has no reason byte, and a
                // retry cannot change an ACL decision.
                ack.reason = mqtt_codec::reason::NOT_AUTHORIZED;
            }
            writer.send(&Packet::PubAck(ack)).await?;
        }
        (QoS::ExactlyOnce, Some(id)) => {
            // Exactly-once inbound [MQTT-4.3.3-2]: forward only the first
            // sighting of this packet id; re-sent copies (DUP) before the
            // PUBREL release are acknowledged but not re-delivered. The dedup
            // window is the durable session store when present (so it survives a
            // failover), else a per-connection set.
            //
            // A store ERROR fails CLOSED (#165): we cannot tell first-sighting from
            // duplicate, so forwarding-and-PUBRECing would silently degrade exactly-once
            // to at-least-once for the duration of the incident — a duplicate delivered
            // under a clean PUBREC. Instead withhold the PUBREC and drop the connection,
            // exactly as a failed durable fan-out does below; the publisher reconnects
            // and re-sends, and once the store recovers the dedup is correct. (An earlier
            // version `unwrap_or(true)`'d the error into "first sighting" — the defect.)
            //
            // The window remembers TWO facts (issue #238): that the id is held, and
            // whether its PUBREC was ever RELEASED. Only an ACKNOWLEDGED flow may answer
            // a DUP from the window — that is the only scope in which [MQTT-4.3.3-2]'s
            // promise was ever made. A held-but-unacknowledged id (the broker told the
            // client nothing) re-enters the gate exactly like a first sighting, so the
            // client's mandatory resend [MQTT-4.4.0-1] is RE-DECIDED rather than answered
            // with a fabricated success PUBREC for a message nothing stored. That is safe
            // to re-decide because the hub decides a refusal before taking any side
            // effect, so a refused attempt leaves nothing for the resend to duplicate.
            let sighting = match &policy.store {
                Some(store) => match store.record_received(client, id).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(client = %client.0, id, error = %e,
                              "QoS2 dedup store write failed; withholding PUBREC (fail closed)");
                        return Ok(PacketOutcome::BrokerClose);
                    }
                },
                // Read first, insert only when fresh: a blind insert would
                // overwrite the acked bit, and a SECOND in-connection DUP of an
                // already-PUBREC'd id would then re-fan-out — the very duplicate
                // the window exists to prevent.
                None => match qos2_inbound.get(&id).copied() {
                    None => {
                        qos2_inbound.insert(id, false);
                        InboundSighting::Fresh
                    }
                    Some(true) => InboundSighting::HeldAcked,
                    Some(false) => InboundSighting::HeldUnacked,
                },
            };
            if sighting != InboundSighting::HeldAcked {
                // Receive Maximum (ADR 0012 §3): a *new* unreleased QoS 2 id beyond the
                // server's advertised window is a flow-control breach — DISCONNECT 0x93.
                // A DUP of an already-held id does not consume a new slot. v5 only (3.1.1
                // has no Receive Maximum and the server does not send DISCONNECT).
                if is_v5 && *qos2_inflight >= wire_limits().receive_maximum as usize {
                    warn!(client = %client.0, limit = wire_limits().receive_maximum,
                          "client exceeded Receive Maximum; DISCONNECT 0x93");
                    disconnect(writer, DISCONNECT_RECEIVE_MAXIMUM_EXCEEDED).await?;
                    return Ok(PacketOutcome::BrokerClose);
                }
                let mut rec = mqtt_codec::packet::Ack::from(id);
                if let Some(done) = forward(hub) {
                    // As for QoS 1: PUBREC promises the broker owns the message, so
                    // it is released only after the durable fan-out completes.
                    //
                    // The obligation is NOT "release the record on every non-PUBREC
                    // exit" (issue #238): a success PUBREC is the only thing that may
                    // mark the record ACKED, and only an acked record may answer a DUP as
                    // a duplicate. The two exits that tell the client NOTHING therefore
                    // LEAVE the record held-unacked — that is the truth — so the client's
                    // mandatory resend is re-decided instead of being answered from a
                    // window whose entry never earned a PUBREC.
                    match done.await {
                        // Hub gone mid-shutdown: nothing was said, so nothing is acked.
                        // The record stays held-unacked and the resend re-attempts.
                        Err(_) => return Ok(PacketOutcome::BrokerClose),
                        Ok(crate::hub::PublishOutcome::Accepted) => {}
                        // Refused under a stated policy (ADR 0041 T4 retained quota,
                        // T11 brownout). v5: a PUBREC >= 0x80 ends the flow — no slot
                        // is consumed and the id stays reusable. v3.1.1 has no reason
                        // byte, so a refusal it cannot say becomes no-PUBREC + close.
                        Ok(crate::hub::PublishOutcome::Refused(r)) => {
                            if is_v5 {
                                rec.reason = r.v5_reason();
                            } else if r.v311() == crate::hub::Refusal311::CloseNoAck {
                                warn!(client = %client.0, id, refusal = r.as_str(),
                                      "QoS 2 publish refused and v3.1.1 cannot say so; \
                                       closing without a PUBREC (the publisher retries)");
                                return Ok(PacketOutcome::BrokerClose);
                            }
                        }
                    }
                } else if is_v5 {
                    // The ACL denied this publish and v5 can say so: PUBREC 0x87
                    // Not authorized (issue #246). A reason >= 0x80 ends the flow
                    // by spec, so this takes the release arm below — the id is
                    // freed entirely and a DUP resend is a fresh (re-denied)
                    // decision. v3.1.1 falls through to the plain PUBREC.
                    rec.reason = mqtt_codec::reason::NOT_AUTHORIZED;
                }
                if rec.reason == 0 {
                    // Only a genuinely new id consumes a Receive-Maximum slot; a
                    // held-unacked resend re-uses the one it already holds.
                    if sighting == InboundSighting::Fresh {
                        *qos2_inflight += 1;
                    }
                    // ACK THE RECORD BEFORE THE PUBREC REACHES THE WIRE — the
                    // write-before-send rule of ADR 0057 / #124. Acking after the send
                    // would let a crash in the PUBREC→PUBREL window lose the fact that we
                    // acked, and the post-failover resend would re-fan-out: the very
                    // duplicate the acked bit exists to prevent. A FAILED write fails
                    // closed for the same reason: a PUBREC the durable record cannot
                    // back is the fabricated-success lie in crash-shaped form.
                    if !ack_qos2_dedup(policy, qos2_inbound, client, id).await {
                        warn!(client = %client.0, id,
                              "QoS2 dedup ack write failed; withholding PUBREC (fail closed)");
                        return Ok(PacketOutcome::BrokerClose);
                    }
                } else {
                    // A v5 PUBREC >= 0x80 ends the flow BY SPEC — both sides agree the id
                    // is free — so the record must go entirely, not merely stay unacked:
                    // a later publish under this id is genuinely new, and a lingering
                    // record would make it look like a retry of a different message.
                    release_qos2_dedup(policy, qos2_inbound, client, id).await;
                }
                writer.send(&Packet::PubRec(rec)).await?;
                return Ok(PacketOutcome::Continue);
            }
            writer.send(&Packet::PubRec(id.into())).await?;
        }
        _ => debug!(client = %client.0, "dropping QoS>0 publish without packet id"),
    }
    Ok(PacketOutcome::Continue)
}

/// FULLY release a `QoS` 2 packet id's inbound dedup record — the durable one when a
/// session store is configured (so it survives a failover), else the per-connection map.
///
/// One caller, and only one is correct (issue #238): the v5 `PUBREC >= 0x80` exit, where
/// the flow has ENDED by spec and both sides consider the id free, so a later publish
/// under it is genuinely new. The exits that say nothing to the client must NOT come
/// here — they leave the record held-unacked, which is what makes the client's mandatory
/// resend re-decidable instead of answerable as a duplicate.
async fn release_qos2_dedup(
    policy: &ConnPolicy,
    qos2_inbound: &mut HashMap<u16, bool>,
    client: &ClientId,
    id: u16,
) {
    match &policy.store {
        Some(store) => {
            let _ = store.clear_received(client, id).await;
        }
        None => {
            qos2_inbound.remove(&id);
        }
    }
}

/// Mark a `QoS` 2 packet id's dedup record ACKNOWLEDGED — the one fact that licenses
/// answering a later DUP of that id from the window rather than fanning it out again
/// (issue #238).
///
/// Must complete BEFORE the success PUBREC reaches the wire (ADR 0057's write-before-send
/// rule): a crash in the PUBREC→PUBREL window that lost this bit would make the
/// post-failover resend re-fan-out.
///
/// Returns `false` when the durable write failed — the caller must then WITHHOLD the
/// PUBREC and close (the same fail-closed posture `record_received` takes), because a
/// success PUBREC the durable record cannot back would re-fan-out after a failover.
async fn ack_qos2_dedup(
    policy: &ConnPolicy,
    qos2_inbound: &mut HashMap<u16, bool>,
    client: &ClientId,
    id: u16,
) -> bool {
    if let Some(store) = &policy.store {
        store.ack_received(client, id).await.is_ok()
    } else {
        qos2_inbound.insert(id, true);
        true
    }
}

/// Per-connection inbound publish-rate limiter (ADR 0041 T3): a token bucket
/// with a one-second burst. An empty bucket **pauses the read** — TCP
/// backpressure, the transport's native flow control — so a bursty-but-compliant
/// client just slows down and an abusive one saturates its own connection, not
/// the broker. Nothing is dropped and nothing is disconnected.
struct PublishRateLimiter {
    tokens: f64,
    last: std::time::Instant,
    rate: f64,
}

impl PublishRateLimiter {
    fn new(rate: u32) -> Self {
        PublishRateLimiter {
            tokens: f64::from(rate),
            last: std::time::Instant::now(),
            rate: f64::from(rate),
        }
    }

    /// Take one token, sleeping (pausing this connection's reads) until one is due.
    async fn acquire(&mut self) {
        let now = std::time::Instant::now();
        self.tokens =
            (self.tokens + now.duration_since(self.last).as_secs_f64() * self.rate).min(self.rate);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return;
        }
        let wait = Duration::from_secs_f64((1.0 - self.tokens) / self.rate);
        tokio::time::sleep(wait).await;
        self.last = std::time::Instant::now();
        self.tokens = 0.0;
    }
}

/// What an inbound packet's handling means for the connection — and therefore
/// whether the client's Will fires when it ends.
///
/// The distinction is load-bearing, not bookkeeping (issue #238): `Hub::detach`
/// publishes the Will on any end that is **not** a client DISCONNECT
/// [MQTT-3.14.4-3], so collapsing "the client asked to go" and "the broker hung up
/// on it" into one `bool` suppresses the Will on every broker-initiated close — a
/// protocol violation, and one that a refusal-under-brownout makes routine (a
/// v3.1.1 publisher's refused publish closes the connection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PacketOutcome {
    /// Keep serving.
    Continue,
    /// The client asked to close (DISCONNECT reason `0x00`): graceful, and the
    /// Will is discarded [MQTT-3.14.4-3].
    ClientDisconnect,
    /// The client asked to close AND asked for its Will (issue #265): a v5
    /// DISCONNECT with a **non-zero** reason — `0x04 Disconnect with Will
    /// Message` explicitly, and any error reason implicitly, since only reason
    /// `0x00` discards the Will [MQTT-3.1.2-10]. The socket close is as clean as
    /// [`ClientDisconnect`](Self::ClientDisconnect)'s, but the detach is
    /// un-graceful so the Will fires. Unreachable on v3.1.1, whose DISCONNECT
    /// has no reason byte and always decodes as `0`.
    ClientDisconnectWithWill,
    /// The BROKER is closing: a protocol violation, a refusal this protocol version
    /// cannot say any other way, or a hub that went away. Un-graceful — the Will
    /// fires, exactly as for an EOF or a keepalive expiry.
    BrokerClose,
}

/// Handle one inbound packet.
// One arm per packet type; a flat dispatch table, not a refactor smell.
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)] // a connection's full inbound-handling context
async fn handle_inbound<W: AsyncWrite + Unpin>(
    packet: Packet,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
    principal: &Identity,
    policy: &ConnPolicy,
    qos2_inbound: &mut HashMap<u16, bool>,
    qos2_inflight: &mut usize,
    is_v5: bool,
    inbound_aliases: &mut InboundAliases,
) -> Result<PacketOutcome, NetError> {
    match packet {
        Packet::Publish(publish) => {
            // A wildcard topic is a protocol violation: close the connection.
            // Never `ClientDisconnect` — every close a publish can cause is the
            // BROKER's, so the Will must fire.
            match handle_publish(
                publish,
                writer,
                hub,
                client,
                principal,
                policy,
                qos2_inbound,
                qos2_inflight,
                is_v5,
                inbound_aliases,
            )
            .await?
            {
                PacketOutcome::Continue => {}
                end => return Ok(end),
            }
        }
        // QoS 2 publisher-side release: the id may be reused afterwards. (A v5
        // reason code on these acks is not acted on yet — workstream G.)
        Packet::PubRel(ack) => {
            let id = ack.pkid;
            // Clears the record in EITHER state (today's lenient behaviour, unchanged):
            // a PUBREL for an id we never acked still ends the flow the client believes
            // in, and answering PUBCOMP is the only forward-progress move.
            release_qos2_dedup(policy, qos2_inbound, client, id).await;
            // The QoS 2 id is released — free its Receive-Maximum slot (ADR 0012).
            *qos2_inflight = qos2_inflight.saturating_sub(1);
            writer.send(&Packet::PubComp(id.into())).await?;
        }
        // Subscriber-side acknowledgements for our downstream deliveries.
        Packet::PubAck(ack) => {
            let _ = hub.send(HubCommand::PubAck {
                client: client.clone(),
                pkid: ack.pkid,
            });
        }
        Packet::PubRec(ack) => {
            let _ = hub.send(HubCommand::PubRec {
                client: client.clone(),
                pkid: ack.pkid,
            });
        }
        Packet::PubComp(ack) => {
            let _ = hub.send(HubCommand::PubComp {
                client: client.clone(),
                pkid: ack.pkid,
            });
        }
        Packet::Subscribe(s) => {
            // MQTT 5.0 §3.2.2.3.12: "If the Server receives a SUBSCRIBE packet containing
            // Subscription Identifier and it does not support Subscription Identifiers,
            // this is a Protocol Error. The Server uses DISCONNECT with Reason Code of
            // 0xA1 (Subscription Identifiers not supported)." Note DISCONNECT, not a
            // SUBACK: SUBACK 0xA1 is for a server that supports the feature and declines
            // one filter. Closing is the MUST ([MQTT-4.13.1-1]); the DISCONNECT packet
            // carrying the reason is the SHOULD, and we send it (issue #245).
            //
            // Deliberately BEFORE the ACL loop: a Protocol Error precedes authorization,
            // so the client gets one 0xA1 rather than a mix of per-filter 0x80s — which
            // also means an identifier-bearing SUBSCRIBE to a forbidden topic records no
            // `acl.deny.subscribe` audit entry.
            //
            // `is_v5 &&` is belt-and-braces: `decode_subscribe` only decodes properties
            // for v5, so a v4 SUBSCRIBE can never carry one — but DISCONNECT is a v5-only
            // packet, so the gate stays explicit.
            if is_v5 && !SUB_IDS_SUPPORTED && s.properties.has_subscription_identifier() {
                warn!(client = %client.0,
                      "SUBSCRIBE carries a Subscription Identifier, which this server does not support; DISCONNECT 0xA1");
                disconnect(writer, DISCONNECT_SUBSCRIPTION_IDS_NOT_SUPPORTED).await?;
                // A broker-initiated close (the client did not DISCONNECT): the Will fires.
                return Ok(PacketOutcome::BrokerClose);
            }
            // ACL gate per filter (ADR 0004 step 3): denied filters answer
            // 0x80 [MQTT-3.9.3] and never reach the hub; granted filters get
            // the requested QoS [MQTT-3.8.4-5/6].
            let mut granted: Vec<(String, QoS)> = Vec::new();
            let mut no_local_filters: Vec<String> = Vec::new();
            let mut rap_filters: Vec<String> = Vec::new();
            let mut retain_handling: Vec<u8> = Vec::new();
            let mut return_codes: Vec<u8> = Vec::with_capacity(s.filters.len());
            for f in &s.filters {
                // A malformed `$share/...` filter (bad share name / empty filter) is
                // rejected outright (ADR 0010 §1) before the ACL even sees it.
                if is_shared_filter(&f.path) && parse_shared(&f.path).is_none() {
                    debug!(client = %client.0, filter = %f.path, "malformed shared subscription");
                    return_codes.push(SUBACK_FAILURE);
                } else if policy
                    .authorizer()
                    .authorize_subscribe(principal, client, &f.path)
                {
                    granted.push((f.path.clone(), f.qos));
                    if f.options.no_local {
                        no_local_filters.push(f.path.clone()); // #198
                    }
                    if f.options.retain_as_published {
                        rap_filters.push(f.path.clone()); // #198
                    }
                    // Parallel to `granted` (#198): 0 send at subscribe, 1 only-if-new, 2 never.
                    retain_handling.push(f.options.retain_handling);
                    return_codes.push(f.qos as u8);
                } else {
                    debug!(client = %client.0, identity = %principal.subject, filter = %f.path,
                           "subscription denied by ACL");
                    policy
                        .audit
                        .record("acl.deny.subscribe", Some(&principal.subject), &f.path);
                    return_codes.push(SUBACK_FAILURE);
                }
            }
            if !granted.is_empty() {
                // Subscription quota (ADR 0041 T3): the hub answers one verdict per
                // ACL-granted filter BEFORE any retained replay, so the SUBACK below
                // still precedes replayed publishes on the wire. A quota-denied
                // filter answers 0x97 Quota exceeded (v5) / 0x80 (v3.1.1) in its slot.
                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = hub.send(HubCommand::Subscribe {
                    client: client.clone(),
                    filters: granted,
                    no_local_filters,
                    rap_filters,
                    retain_handling,
                    reply: Some(reply_tx),
                });
                let Ok(verdicts) = reply_rx.await else {
                    // Hub shut down mid-subscribe: the BROKER closes, so this is
                    // not a graceful client end.
                    return Ok(PacketOutcome::BrokerClose);
                };
                let denied_code = if is_v5 {
                    reason::QUOTA_EXCEEDED
                } else {
                    SUBACK_FAILURE
                };
                // Walk the granted slots (the codes currently carrying a QoS) and
                // overwrite the ones the quota denied, in order.
                let mut v = verdicts.iter();
                for code in &mut return_codes {
                    if *code != SUBACK_FAILURE && !v.next().copied().unwrap_or(true) {
                        *code = denied_code;
                    }
                }
            }
            writer
                .send(&Packet::SubAck(SubAck {
                    pkid: s.pkid,
                    return_codes,
                    properties: mqtt_codec::Properties::new(),
                }))
                .await?;
        }
        Packet::Unsubscribe(u) => {
            let _ = hub.send(HubCommand::Unsubscribe {
                client: client.clone(),
                filters: u.filters.clone(),
            });
            writer.send(&Packet::UnsubAck(u.pkid.into())).await?;
        }
        Packet::PingReq => writer.send(&Packet::PingResp).await?,
        Packet::Disconnect(d) => {
            // Only reason 0x00 discards the Will [MQTT-3.1.2-10]: 0x04 is an
            // explicit "Disconnect with Will Message", and any other non-zero
            // reason is an abnormal end the client is reporting — either way the
            // Will fires (issue #265). v3.1.1's DISCONNECT has no reason byte and
            // decodes as 0, so it always lands in the graceful arm.
            return Ok(if d.reason == 0 {
                PacketOutcome::ClientDisconnect
            } else {
                PacketOutcome::ClientDisconnectWithWill
            });
        }
        other => debug!(packet = ?other.packet_type(), "ignoring unexpected packet"),
    }
    Ok(PacketOutcome::Continue)
}

#[cfg(test)]
mod tests {
    use super::{assigned_client_id, NodeId};

    /// Two nodes must never hand out the same server-assigned id.
    ///
    /// `AUTO_ID` is per PROCESS, so every node's counter starts at 1 and every
    /// node's first zero-id client was called `auto-1`. Session ownership across
    /// the cluster is keyed by client id, so two unrelated clients on two nodes
    /// were one session as far as the cluster was concerned.
    ///
    /// This is asserted on the naming rule rather than end to end on purpose: two
    /// "nodes" in one test process SHARE the counter, so they draw different values
    /// and their ids differ even with the bug present. An in-process test of this
    /// would pass either way — it would prove nothing. What guarantees it across
    /// real processes is that the id carries the node's own id, which ADR 0016 makes
    /// unique within a cluster by binding it to the peer certificate's CN.
    #[test]
    fn an_assigned_id_is_unique_across_nodes_not_just_within_one() {
        let a = assigned_client_id(Some(&NodeId("node-a".into())), 1);
        let b = assigned_client_id(Some(&NodeId("node-b".into())), 1);
        assert_ne!(
            a, b,
            "both nodes' first client got {a} — one shared session identity"
        );
        assert!(a.contains("node-a"), "{a} must carry its node id");

        // Within one node the counter still separates clients.
        assert_ne!(
            assigned_client_id(Some(&NodeId("node-a".into())), 1),
            assigned_client_id(Some(&NodeId("node-a".into())), 2)
        );
    }

    /// Unclustered there is a single process, so the counter alone is unique and
    /// the id stays short.
    #[test]
    fn an_unclustered_assigned_id_needs_no_node_qualifier() {
        assert_eq!(assigned_client_id(None, 7), "auto-7");
    }

    use super::{
        auth_handle, authz_handle, handle_stream, jwt_password_str, wire_limits, ConnPolicy,
        DEFAULT_CONNECT_TIMEOUT,
    };
    use crate::hub::{AttachOutcome, HubCommand, Outbound};
    use bytes::Bytes;

    #[test]
    fn jwt_password_shape_detection() {
        // Compact JWS: three non-empty base64url segments.
        assert!(jwt_password_str(b"aGVhZGVy.cGF5bG9hZA.c2ln").is_some());
        assert!(jwt_password_str(b"eyJ0-_9.eyJ0-_9.c2ln").is_some()); // - and _ are base64url
                                                                      // Not tokens: wrong segment count, empty segments, non-base64url bytes, non-UTF8.
        assert!(jwt_password_str(b"only.two").is_none());
        assert!(jwt_password_str(b"a.b.c.d").is_none());
        assert!(jwt_password_str(b"a..c").is_none());
        assert!(jwt_password_str(b"plain-password").is_none());
        assert!(jwt_password_str(b"has+slash/.b.c").is_none()); // + and / are base64, not base64url
        assert!(jwt_password_str(b"a.b.c\xff").is_none()); // invalid UTF-8
        assert!(jwt_password_str(b"").is_none());
    }
    use mqtt_auth::basic::BasicAuthenticator;
    use mqtt_codec::{
        packet::{Auth, ConnAck, Connect, Disconnect, Publish, SubAck, Subscribe, SubscribeFilter},
        Packet, Properties, Property, ProtocolVersion, QoS,
    };
    use mqtt_net::{FrameReader, FrameWriter};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::timeout;

    const V4: ProtocolVersion = ProtocolVersion::V311;
    const V5: ProtocolVersion = ProtocolVersion::V5;

    type Reader = FrameReader<ReadHalf<DuplexStream>>;
    type Writer = FrameWriter<WriteHalf<DuplexStream>>;

    /// A wide-open policy so these tests exercise the protocol paths, not the
    /// gate (covered in tests/auth.rs, tests/acl.rs, and mqtt-auth's tests).
    fn permissive() -> Arc<ConnPolicy> {
        Arc::new(ConnPolicy {
            auth: auth_handle(Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            })),
            authz: authz_handle(Arc::new(mqtt_auth::AllowAll)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            node: None,
            store: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            enhanced: None,
            shutdown: None,
            metrics: None,
        })
    }

    /// Start a connection task over an in-memory duplex; returns the client's
    /// framed I/O and the hub command stream the connection produces.
    fn start_conn() -> (Reader, Writer, mpsc::UnboundedReceiver<HubCommand>) {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        tokio::spawn(handle_stream(server, None, None, permissive(), hub_tx));
        let (rh, wh) = tokio::io::split(client);
        (FrameReader::new(rh, V4), FrameWriter::new(wh, V4), hub_rx)
    }

    /// [`start_conn`] at v5 with a caller-supplied session store attached to the policy,
    /// so a test can drive the QoS-2 dedup path against a failing store (#165).
    fn start_conn_with_store(
        store: Arc<dyn mqtt_storage::SessionStore>,
    ) -> (Reader, Writer, mpsc::UnboundedReceiver<HubCommand>) {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        let policy = Arc::new(ConnPolicy {
            auth: auth_handle(Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            })),
            authz: authz_handle(Arc::new(mqtt_auth::AllowAll)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            node: None,
            store: Some(store),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            enhanced: None,
            shutdown: None,
            metrics: None,
        });
        tokio::spawn(handle_stream(server, None, None, policy, hub_tx));
        let (rh, wh) = tokio::io::split(client);
        (FrameReader::new(rh, V5), FrameWriter::new(wh, V5), hub_rx)
    }

    /// A `SessionStore` that delegates to an in-memory store but can be flipped to fail
    /// every `record_received` — the fault seam for the #165 QoS-2 dedup test.
    #[derive(Debug)]
    struct RecordReceivedFails {
        inner: mqtt_storage::MemorySessionStore,
        fail: std::sync::atomic::AtomicBool,
    }

    impl RecordReceivedFails {
        fn new() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                inner: mqtt_storage::MemorySessionStore::new(),
                fail: std::sync::atomic::AtomicBool::new(false),
            })
        }
        fn fail_from_now(&self) {
            self.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl mqtt_storage::SessionStore for RecordReceivedFails {
        async fn ensure_session(
            &self,
            client: &mqtt_core::ClientId,
        ) -> Result<bool, mqtt_storage::StorageError> {
            self.inner.ensure_session(client).await
        }
        async fn claim_session(
            &self,
            client: &mqtt_core::ClientId,
            owner: &str,
        ) -> Result<mqtt_storage::SessionClaim, mqtt_storage::StorageError> {
            self.inner.claim_session(client, owner).await
        }
        async fn set_subscriptions(
            &self,
            client: &mqtt_core::ClientId,
            subscriptions: &[mqtt_core::Subscription],
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.set_subscriptions(client, subscriptions).await
        }
        async fn subscriptions(
            &self,
            client: &mqtt_core::ClientId,
        ) -> Result<Vec<mqtt_core::Subscription>, mqtt_storage::StorageError> {
            self.inner.subscriptions(client).await
        }
        async fn enqueue_with_expiry(
            &self,
            client: &mqtt_core::ClientId,
            message: &mqtt_core::Message,
            expiry_at: Option<u64>,
        ) -> Result<mqtt_storage::Enqueued, mqtt_storage::StorageError> {
            self.inner
                .enqueue_with_expiry(client, message, expiry_at)
                .await
        }
        async fn pending(
            &self,
            client: &mqtt_core::ClientId,
            after: mqtt_storage::Offset,
            limit: usize,
        ) -> Result<Vec<mqtt_storage::QueuedMessage>, mqtt_storage::StorageError> {
            self.inner.pending(client, after, limit).await
        }
        async fn ack(
            &self,
            client: &mqtt_core::ClientId,
            up_to: mqtt_storage::Offset,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.ack(client, up_to).await
        }
        async fn record_received(
            &self,
            client: &mqtt_core::ClientId,
            packet_id: u16,
        ) -> Result<mqtt_storage::InboundSighting, mqtt_storage::StorageError> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(mqtt_storage::StorageError::NoQuorum);
            }
            self.inner.record_received(client, packet_id).await
        }
        async fn ack_received(
            &self,
            client: &mqtt_core::ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.ack_received(client, packet_id).await
        }
        async fn clear_received(
            &self,
            client: &mqtt_core::ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.clear_received(client, packet_id).await
        }
        async fn received(
            &self,
            client: &mqtt_core::ClientId,
        ) -> Result<Vec<u16>, mqtt_storage::StorageError> {
            self.inner.received(client).await
        }
        async fn record_outbound(
            &self,
            client: &mqtt_core::ClientId,
            packet_id: u16,
            offset: mqtt_storage::Offset,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.record_outbound(client, packet_id, offset).await
        }
        async fn advance_outbound(
            &self,
            client: &mqtt_core::ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.advance_outbound(client, packet_id).await
        }
        async fn clear_outbound(
            &self,
            client: &mqtt_core::ClientId,
            packet_id: u16,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.clear_outbound(client, packet_id).await
        }
        async fn outbound(
            &self,
            client: &mqtt_core::ClientId,
        ) -> Result<Vec<mqtt_storage::OutboundInflight>, mqtt_storage::StorageError> {
            self.inner.outbound(client).await
        }
        async fn next_packet_id(
            &self,
            client: &mqtt_core::ClientId,
        ) -> Result<u16, mqtt_storage::StorageError> {
            self.inner.next_packet_id(client).await
        }
        async fn reserve_packet_ids(
            &self,
            client: &mqtt_core::ClientId,
            count: u16,
        ) -> Result<u16, mqtt_storage::StorageError> {
            self.inner.reserve_packet_ids(client, count).await
        }
        async fn remove(
            &self,
            client: &mqtt_core::ClientId,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.remove(client).await
        }
        async fn set_session_expiry(
            &self,
            client: &mqtt_core::ClientId,
            deadline: Option<u64>,
        ) -> Result<(), mqtt_storage::StorageError> {
            self.inner.set_session_expiry(client, deadline).await
        }
        async fn expiring_sessions(
            &self,
        ) -> Result<Vec<(mqtt_core::ClientId, u64)>, mqtt_storage::StorageError> {
            self.inner.expiring_sessions().await
        }
        async fn all_sessions(
            &self,
        ) -> Result<mqtt_storage::SessionScan, mqtt_storage::StorageError> {
            self.inner.all_sessions().await
        }
    }

    /// Like [`start_conn`], but the policy carries a graceful-shutdown token (ADR 0019),
    /// framed at `version` so both the v3.1.1 and v5 drain behaviours can be exercised.
    fn start_conn_with_shutdown(
        shutdown: tokio_util::sync::CancellationToken,
        version: ProtocolVersion,
    ) -> (Reader, Writer, mpsc::UnboundedReceiver<HubCommand>) {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        let policy = Arc::new(ConnPolicy {
            auth: auth_handle(Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            })),
            authz: authz_handle(Arc::new(mqtt_auth::AllowAll)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            node: None,
            store: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            enhanced: None,
            shutdown: Some(shutdown),
            metrics: None,
        });
        tokio::spawn(handle_stream(server, None, None, policy, hub_tx));
        let (rh, wh) = tokio::io::split(client);
        (
            FrameReader::new(rh, version),
            FrameWriter::new(wh, version),
            hub_rx,
        )
    }

    /// ADR 0019: cancelling the shutdown token drains an established connection — the
    /// broker closes it cleanly rather than holding it until a kill or the keepalive.
    #[tokio::test]
    async fn graceful_shutdown_drains_an_established_connection() {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (mut reader, mut writer, hub_rx) = start_conn_with_shutdown(shutdown.clone(), V4);
        stub_hub(hub_rx);

        writer
            .send(&connect_packet("drain-me", true))
            .await
            .unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        // The shutdown signal closes the connection promptly (EOF, no further packets).
        shutdown.cancel();
        assert!(
            recv(&mut reader).await.is_none(),
            "the connection drained and closed on shutdown"
        );
    }

    /// ADR 0019: a draining v5 connection is told *why* — a Server-shutting-down (0x8B)
    /// DISCONNECT — before the socket closes, so the client reconnects promptly.
    #[tokio::test]
    async fn graceful_shutdown_sends_v5_server_shutting_down_disconnect() {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (mut reader, mut writer, hub_rx) = start_conn_with_shutdown(shutdown.clone(), V5);
        stub_hub(hub_rx);

        writer.send(&connect_v5("drain-v5", vec![])).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        shutdown.cancel();
        match recv(&mut reader).await {
            Some(Packet::Disconnect(d)) => assert_eq!(
                d.reason,
                super::DISCONNECT_SERVER_SHUTTING_DOWN,
                "v5 drain must use reason 0x8B (Server shutting down)"
            ),
            other => panic!("expected a v5 DISCONNECT on drain, got {other:?}"),
        }
        // ...then the socket closes.
        assert!(
            recv(&mut reader).await.is_none(),
            "the connection closes after the DISCONNECT"
        );
    }

    /// [MQTT-3.1.2-24] Deterministic keepalive enforcement via paused virtual time:
    /// a client that negotiates `keep_alive=1` and then goes silent is closed once
    /// 1.5x the interval elapses. No real wall-clock wait — the in-memory duplex
    /// carries no traffic, so the runtime is idle and auto-advances the clock to the
    /// broker's keepalive deadline. This is the time-injected unit-level counterpart
    /// to the real-TCP keepalive integration tests.
    #[tokio::test(start_paused = true)]
    async fn idle_connection_is_closed_after_keepalive_grace() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        stub_hub(hub_rx);

        // keep_alive = 1s; the broker closes after 1.5x with no inbound traffic.
        let connect = Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 1,
            client_id: "idle".into(),
            last_will: None,
            username: None,
            password: None,
        });
        writer.send(&connect).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        // Advance past the 1.5s grace; the keepalive deadline fires and closes the conn.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            recv(&mut reader).await.is_none(),
            "an idle keep_alive=1 connection must close once the grace elapses"
        );
    }

    /// ADR 0020-T3: a full connect/teardown moves the connection metrics — the
    /// per-protocol total increments and the active gauge returns to zero on close.
    #[tokio::test]
    async fn connection_lifecycle_moves_the_metrics_counters() {
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("test"));
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        stub_hub(hub_rx);
        let policy = Arc::new(ConnPolicy {
            auth: auth_handle(Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            })),
            authz: authz_handle(Arc::new(mqtt_auth::AllowAll)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            node: None,
            store: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            enhanced: None,
            shutdown: None,
            metrics: Some(metrics.clone()),
        });
        let conn = tokio::spawn(handle_stream(server, None, None, policy, hub_tx));
        let (rh, wh) = tokio::io::split(client);
        let mut reader = FrameReader::new(rh, V4);
        let mut writer = FrameWriter::new(wh, V4);

        writer
            .send(&connect_packet("metric-me", true))
            .await
            .unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        // Client half-close → the server connection task runs to completion.
        drop(writer);
        drop(reader);
        timeout(Duration::from_secs(10), conn)
            .await
            .expect("connection task should finish promptly")
            .expect("connection task should not panic");

        let text = metrics.render();
        assert!(
            text.contains("mqttd_connections_total{protocol=\"3.1.1\"} 1"),
            "the per-protocol connection total should read 1:\n{text}"
        );
        assert!(
            text.contains("mqttd_connections_active 0"),
            "the active-connections gauge should return to zero:\n{text}"
        );
    }

    /// ADR 0020-T3: a rejected handshake increments the bounded `connection_errors`
    /// counter under the `auth` reason class (and never opens a connection).
    #[tokio::test]
    async fn rejected_auth_increments_the_error_counter() {
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("test"));
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        stub_hub(hub_rx);
        let policy = Arc::new(ConnPolicy {
            auth: auth_handle(Arc::new(BasicAuthenticator {
                allow_anonymous: false,
            })),
            authz: authz_handle(Arc::new(mqtt_auth::AllowAll)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            node: None,
            store: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            enhanced: None,
            shutdown: None,
            metrics: Some(metrics.clone()),
        });
        let conn = tokio::spawn(handle_stream(server, None, None, policy, hub_tx));
        let (rh, wh) = tokio::io::split(client);
        let mut reader = FrameReader::new(rh, V4);
        let mut writer = FrameWriter::new(wh, V4);

        // Anonymous CONNECT against a policy that forbids it: rejected at the gate.
        writer.send(&connect_packet("anon", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
        drop(writer);
        drop(reader);
        timeout(Duration::from_secs(10), conn)
            .await
            .expect("connection task should finish promptly")
            .expect("connection task should not panic");

        let text = metrics.render();
        assert!(
            text.contains("mqttd_connection_errors_total{reason=\"auth\"} 1"),
            "a rejected handshake should count one auth error:\n{text}"
        );
        assert!(
            text.contains("mqttd_connections_active 0"),
            "a rejected handshake never opens a connection:\n{text}"
        );
    }

    /// Minimal hub stub: accepts every Attach with `session_present = false`,
    /// records the client ids it sees, and keeps outbound senders alive so the
    /// connection's writer loop stays up.
    fn stub_hub(mut hub_rx: mpsc::UnboundedReceiver<HubCommand>) -> Arc<Mutex<Vec<String>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = seen.clone();
        tokio::spawn(async move {
            let mut keep_alive = Vec::new();
            while let Some(cmd) = hub_rx.recv().await {
                match cmd {
                    HubCommand::Attach {
                        client,
                        outbound,
                        reply,
                        ..
                    } => {
                        record.lock().unwrap().push(client.0.clone());
                        keep_alive.push(outbound);
                        let _ = reply.send(AttachOutcome::Present(false));
                    }
                    // Release any gated acknowledgement, as the real hub would.
                    HubCommand::Publish {
                        done: Some(done), ..
                    } => {
                        let _ = done.send(crate::hub::PublishOutcome::Accepted);
                    }
                    // Grant every quota verdict, as an uncapped hub would (ADR 0041 T3).
                    HubCommand::Subscribe {
                        filters,
                        reply: Some(reply),
                        ..
                    } => {
                        let _ = reply.send(vec![true; filters.len()]);
                    }
                    _ => {}
                }
            }
        });
        seen
    }

    /// A v5 connection over an in-memory duplex (the v5 analogue of `start_conn`).
    fn v5_pipe() -> (Reader, Writer, mpsc::UnboundedReceiver<HubCommand>) {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        tokio::spawn(handle_stream(server, None, None, permissive(), hub_tx));
        let (rh, wh) = tokio::io::split(client);
        (FrameReader::new(rh, V5), FrameWriter::new(wh, V5), hub_rx)
    }

    /// A v5 connection whose policy has an enhanced HMAC-SHA256 authenticator
    /// configured with one subject ("alice"). The hub stub accepts every Attach.
    fn enhanced_conn() -> (Reader, Writer) {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        let _seen = stub_hub(hub_rx);
        let mut secrets = std::collections::HashMap::new();
        secrets.insert("alice".to_string(), b"alice-secret".to_vec());
        let policy = Arc::new(ConnPolicy {
            auth: auth_handle(Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            })),
            enhanced: Some(Arc::new(mqtt_auth::HmacChallengeAuthenticator::new(
                secrets,
            ))),
            authz: authz_handle(Arc::new(mqtt_auth::AllowAll)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            node: None,
            store: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            shutdown: None,
            metrics: None,
        });
        tokio::spawn(handle_stream(server, None, None, policy, hub_tx));
        let (rh, wh) = tokio::io::split(client);
        (FrameReader::new(rh, V5), FrameWriter::new(wh, V5))
    }

    /// An AUTH packet for the HMAC-SHA256 method with the given reason and data.
    fn hmac_auth(reason: u8, data: &[u8]) -> Packet {
        Packet::Auth(Auth {
            reason,
            properties: Properties(vec![
                Property::AuthenticationMethod("HMAC-SHA256".into()),
                Property::AuthenticationData(Bytes::copy_from_slice(data)),
            ]),
        })
    }

    /// HMAC-SHA256 proof over `nonce` with alice's secret.
    fn alice_proof(nonce: &[u8]) -> Vec<u8> {
        let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, b"alice-secret");
        aws_lc_rs::hmac::sign(&key, nonce).as_ref().to_vec()
    }

    /// Drive a full connect-time HMAC enhanced-auth handshake to a successful CONNACK.
    async fn connect_and_authenticate(reader: &mut Reader, writer: &mut Writer) {
        writer
            .send(&connect_v5(
                "c",
                vec![
                    Property::AuthenticationMethod("HMAC-SHA256".into()),
                    Property::AuthenticationData(Bytes::from_static(b"alice")),
                ],
            ))
            .await
            .unwrap();
        let nonce = match recv(reader).await {
            Some(Packet::Auth(a)) => a.properties.authentication_data().unwrap().to_vec(),
            other => panic!("expected AUTH challenge, got {other:?}"),
        };
        writer
            .send(&hmac_auth(0x18, &alice_proof(&nonce)))
            .await
            .unwrap();
        match recv(reader).await {
            Some(Packet::ConnAck(a)) => assert_eq!(a.code, 0, "connect auth succeeds"),
            other => panic!("expected CONNACK, got {other:?}"),
        }
    }

    fn connect_v5(id: &str, properties: Vec<Property>) -> Packet {
        Packet::Connect(Connect {
            properties: Properties(properties),
            protocol: V5,
            clean_session: true,
            keep_alive: 30,
            client_id: id.to_string(),
            last_will: None,
            username: None,
            password: None,
        })
    }

    fn server_publish(topic: &str) -> Packet {
        Packet::Publish(Publish {
            properties: Properties::new(),
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            pkid: None,
            payload: Bytes::from_static(b"p"),
        })
    }

    /// Hub stub that answers Attach and republishes each `Publish` command's topic
    /// on a channel, so a test can assert what (fully-resolved) topic reached routing.
    fn stub_hub_topics(
        mut hub_rx: mpsc::UnboundedReceiver<HubCommand>,
    ) -> mpsc::UnboundedReceiver<String> {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut keep_alive = Vec::new();
            while let Some(cmd) = hub_rx.recv().await {
                match cmd {
                    HubCommand::Attach {
                        outbound, reply, ..
                    } => {
                        keep_alive.push(outbound);
                        let _ = reply.send(AttachOutcome::Present(false));
                    }
                    HubCommand::Publish { topic, done, .. } => {
                        let _ = tx.send(topic);
                        // Release any gated acknowledgement, as the real hub would.
                        if let Some(done) = done {
                            let _ = done.send(crate::hub::PublishOutcome::Accepted);
                        }
                    }
                    _ => {}
                }
            }
        });
        rx
    }

    /// Hub stub that answers Attach and hands the connection's outbound sender back
    /// to the test, so it can drive server→client publishes through the writer path.
    fn stub_hub_capture_outbound(
        mut hub_rx: mpsc::UnboundedReceiver<HubCommand>,
    ) -> oneshot::Receiver<Outbound> {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut sender = Some(tx);
            let mut keep_alive = Vec::new();
            while let Some(cmd) = hub_rx.recv().await {
                if let HubCommand::Attach {
                    outbound, reply, ..
                } = cmd
                {
                    let _ = reply.send(AttachOutcome::Present(false));
                    if let Some(s) = sender.take() {
                        let _ = s.send(outbound.clone());
                    }
                    keep_alive.push(outbound);
                }
            }
        });
        rx
    }

    /// Hub stub that answers Attach and reports the Receive Maximum it carried, so a
    /// test can assert the connection translated the CONNECT property correctly.
    fn stub_hub_capture_receive_maximum(
        mut hub_rx: mpsc::UnboundedReceiver<HubCommand>,
    ) -> oneshot::Receiver<u16> {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut sender = Some(tx);
            let mut keep_alive = Vec::new();
            while let Some(cmd) = hub_rx.recv().await {
                if let HubCommand::Attach {
                    outbound,
                    reply,
                    receive_maximum,
                    ..
                } = cmd
                {
                    let _ = reply.send(AttachOutcome::Present(false));
                    if let Some(s) = sender.take() {
                        let _ = s.send(receive_maximum);
                    }
                    keep_alive.push(outbound);
                }
            }
        });
        rx
    }

    fn connect_packet(id: &str, clean_session: bool) -> Packet {
        Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session,
            keep_alive: 30,
            client_id: id.to_string(),
            last_will: None,
            username: None,
            password: None,
        })
    }

    /// Next packet within a short window; transport errors and EOF both map to
    /// `None` (the assertions only care whether an MQTT packet arrived).
    async fn recv(reader: &mut Reader) -> Option<Packet> {
        timeout(Duration::from_millis(500), reader.next_packet())
            .await
            .expect("connection neither answered nor closed")
            .unwrap_or(None)
    }

    /// `splice` half-closes correctly: after the client stops writing (EOF toward
    /// the owner), bytes the owner sends back still reach the client instead of being
    /// truncated at teardown — the regression the select-of-two-copies had.
    #[tokio::test]
    async fn splice_relays_owner_bytes_after_client_half_close() {
        use tokio::io::AsyncReadExt;

        let (mut client_end, splice_client) = tokio::io::duplex(1024);
        let (splice_owner, mut owner_end) = tokio::io::duplex(1024);
        let (read_half, write_half) = tokio::io::split(splice_client);

        let task = tokio::spawn(super::splice(
            read_half,
            write_half,
            b"PRELUDE".to_vec(),
            splice_owner,
        ));

        // The owner first receives the prelude this node writes ahead of the splice.
        let mut pre = [0u8; 7];
        owner_end.read_exact(&mut pre).await.unwrap();
        assert_eq!(&pre, b"PRELUDE");

        // The client sends a request, then half-closes its write side (EOF → owner).
        client_end.write_all(b"req").await.unwrap();
        client_end.shutdown().await.unwrap();
        let mut got = [0u8; 3];
        owner_end.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"req");

        // AFTER the client's EOF, the owner sends a final reply; it must still arrive
        // (and then both sides close).
        owner_end.write_all(b"reply").await.unwrap();
        owner_end.shutdown().await.unwrap();
        let mut reply = Vec::new();
        client_end.read_to_end(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn non_connect_first_packet_closes_without_connack() {
        let (mut reader, mut writer, _hub_rx) = start_conn();
        writer.send(&Packet::PingReq).await.unwrap();
        assert_eq!(recv(&mut reader).await, None);
    }

    /// A session relocated here by another node records that node in the auth audit
    /// (`via`), so a vouched relocation is attributable (ADR 0005 / ADR 0004 audit).
    #[tokio::test]
    async fn proxied_session_records_the_relaying_node_in_the_audit() {
        let audit = Arc::new(mqtt_observability::RecordingAuditSink::new());
        let policy = Arc::new(ConnPolicy {
            auth: auth_handle(Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            })),
            authz: authz_handle(Arc::new(mqtt_auth::AllowAll)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: audit.clone(),
            proxy: None,
            node: None,
            store: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            enhanced: None,
            shutdown: None,
            metrics: None,
        });

        let (client, owner_side) = tokio::io::duplex(4096);
        let (owner_read, owner_write) = tokio::io::split(owner_side);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        let _seen = stub_hub(hub_rx);

        // The owner serves a session "node-a" relayed here, vouching "device-7".
        tokio::spawn(super::serve_proxied(
            owner_read,
            owner_write,
            None,
            Some(mqtt_auth::Identity {
                subject: "device-7".to_string(),
                groups: Vec::new(),
            }),
            policy,
            hub_tx,
            bytes::BytesMut::new(),
            Some("node-a".to_string()),
        ));

        // Drive the proxied client's persistent CONNECT; the owner answers CONNACK.
        let (client_read, client_write) = tokio::io::split(client);
        let mut reader: Reader = FrameReader::new(client_read, V4);
        let mut writer: Writer = FrameWriter::new(client_write, V4);
        writer
            .send(&connect_packet("device-7", false))
            .await
            .unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        // The auth.success event names the relaying node.
        let events = audit.events();
        let auth = events
            .iter()
            .find(|e| e.kind == "auth.success")
            .expect("auth.success recorded");
        assert_eq!(auth.subject.as_deref(), Some("device-7"));
        assert!(
            auth.detail.contains("relayed by node node-a"),
            "audit detail should attribute the relaying node, got: {}",
            auth.detail
        );
    }

    #[tokio::test]
    async fn unknown_protocol_version_closes_without_connack() {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, _hub_rx) = mpsc::unbounded_channel();
        tokio::spawn(handle_stream(server, None, None, permissive(), hub_tx));
        let (rh, mut wh) = tokio::io::split(client);

        // A CONNECT claiming protocol level 3 (neither v3.1.1 nor v5): name "MQTT",
        // level 0x03, clean-session flags, keepalive 60, client id "x". The codec
        // refuses the unknown level, so the connection closes with no CONNACK.
        let frame: &[u8] = &[
            0x10, 0x0D, // CONNECT, remaining length 13
            0x00, 0x04, b'M', b'Q', b'T', b'T', 0x03, 0x02, 0x00, 0x3C, // var header
            0x00, 0x01, b'x', // client id
        ];
        wh.write_all(frame).await.unwrap();

        let mut reader: Reader = FrameReader::new(rh, V4);
        assert_eq!(
            recv(&mut reader).await,
            None,
            "an unknown protocol version must never reach CONNACK 0x00"
        );
    }

    /// An MQTT 5.0 client connects, the broker answers a v5 CONNACK, a v5 SUBSCRIBE
    /// (with subscription options) is answered with a v5 SUBACK, and a v5 DISCONNECT
    /// closes the session — the whole v5 path negotiated end to end.
    ///
    /// This test used to send `SubscriptionIdentifier(5)` here and assert a granted
    /// SUBACK, which pinned the issue #245 defect. The identifier moved to its own
    /// refusal test (`v5_subscribe_with_a_subscription_identifier_disconnects_instead_of_subacking`);
    /// what stays here is the handshake coverage, unchanged.
    #[tokio::test]
    async fn v5_client_connects_subscribes_and_disconnects() {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        let _seen = stub_hub(hub_rx);
        tokio::spawn(handle_stream(server, None, None, permissive(), hub_tx));
        let (rh, wh) = tokio::io::split(client);
        // The client speaks v5; the broker negotiates it from the CONNECT.
        let mut reader: Reader = FrameReader::new(rh, V5);
        let mut writer: Writer = FrameWriter::new(wh, V5);

        writer
            .send(&Packet::Connect(Connect {
                protocol: V5,
                clean_session: true,
                keep_alive: 30,
                client_id: "v5-client".into(),
                last_will: None,
                username: None,
                password: None,
                properties: Properties(vec![Property::SessionExpiryInterval(120)]),
            }))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => {
                assert_eq!(a.code, 0, "v5 CONNACK success");
                assert!(!a.session_present);
            }
            other => panic!("expected v5 CONNACK, got {other:?}"),
        }

        writer
            .send(&Packet::Subscribe(Subscribe {
                pkid: 1,
                filters: vec![SubscribeFilter {
                    path: "a/b".into(),
                    qos: QoS::AtLeastOnce,
                    options: mqtt_codec::SubscriptionOptions {
                        no_local: true,
                        ..Default::default()
                    },
                }],
                // A NON-EMPTY property block, deliberately: this is the only accepted-SUBSCRIBE
                // test in the tree whose properties are populated, so it is what pins the
                // `0xA1` guard's NARROWNESS. Broaden the guard from
                // `has_subscription_identifier()` to "any properties present" and this test
                // goes red instead of the refusal silently swallowing every v5 SUBSCRIBE that
                // carries a User Property.
                properties: Properties(vec![Property::UserProperty(
                    "tenant".into(),
                    "acme".into(),
                )]),
            }))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::SubAck(SubAck {
                pkid, return_codes, ..
            })) => {
                assert_eq!(pkid, 1);
                assert_eq!(return_codes, vec![QoS::AtLeastOnce as u8]);
            }
            other => panic!("expected v5 SUBACK, got {other:?}"),
        }

        writer
            .send(&Packet::Disconnect(Disconnect::default()))
            .await
            .unwrap();
        assert_eq!(
            recv(&mut reader).await,
            None,
            "DISCONNECT closes the session"
        );
    }

    // ---- subscription-identifier wire posture (issue #245) ----
    //
    // MQTT 5.0 §3.2.2.3.12, verbatim: "If not present, then Subscription Identifiers are
    // supported." Omitting CONNACK property 0x29 is therefore an affirmative claim of
    // support, not a silence — so a server that does not deliver them must say 0.

    /// The v5 CONNACK carries `Subscription Identifiers Available = 0` (§3.2.2.3.12).
    /// Asserted on the literal 0, not on `u8::from(SUB_IDS_SUPPORTED)`, so this is a fact
    /// about the wire rather than a tautology about the constant.
    #[tokio::test]
    async fn v5_connack_advertises_subscription_identifiers_unavailable() {
        let (mut reader, mut writer, hub_rx) = v5_pipe();
        let _seen = stub_hub(hub_rx);
        writer.send(&connect_v5("c", vec![])).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => assert!(
                a.properties
                    .0
                    .contains(&Property::SubscriptionIdentifierAvailable(0)),
                "v5 CONNACK must advertise 0x29 = 0, got {:?}",
                a.properties.0
            ),
            other => panic!("expected v5 CONNACK, got {other:?}"),
        }
    }

    /// The v5 CONNACK's property-id multiset is exactly the four properties
    /// `negotiate_v5_properties` is allowed to emit. Order is not contractual, so the ids
    /// are sorted before comparison. This is the "must not start advertising anything else
    /// by accident" guard: the other CONNACK tests use typed accessors and would not
    /// notice a fifth property appearing.
    #[tokio::test]
    async fn v5_connack_advertises_exactly_the_four_negotiated_properties() {
        let (mut reader, mut writer, hub_rx) = v5_pipe();
        let _seen = stub_hub(hub_rx);
        writer.send(&connect_v5("c", vec![])).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => {
                let mut ids: Vec<u8> = a.properties.0.iter().map(Property::id).collect();
                ids.sort_unstable();
                assert_eq!(
                    ids,
                    vec![
                        0x21, // Receive Maximum (ADR 0012)
                        0x22, // Topic Alias Maximum (ADR 0011)
                        0x27, // Maximum Packet Size (ADR 0041 T4)
                        0x29, // Subscription Identifiers Available (issue #245)
                    ],
                    "the v5 CONNACK property set is contractual"
                );
            }
            other => panic!("expected v5 CONNACK, got {other:?}"),
        }
    }

    /// Guard (passes today, must keep passing): a v3.1.1 CONNACK carries no properties at
    /// all. It fails the moment the `SubscriptionIdentifierAvailable` push is moved
    /// outside `negotiate_v5_properties`'s `if is_v5` block. Not red-first evidence.
    #[tokio::test]
    async fn v311_connack_carries_no_properties() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let _seen = stub_hub(hub_rx);
        writer.send(&connect_packet("c", true)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => assert!(
                a.properties.0.is_empty(),
                "v3.1.1 has no properties, got {:?}",
                a.properties.0
            ),
            other => panic!("expected CONNACK, got {other:?}"),
        }
    }

    /// A v5 SUBSCRIBE carrying a Subscription Identifier is a Protocol Error for a server
    /// that does not support them: §3.2.2.3.12 prescribes DISCONNECT with reason 0xA1
    /// (Subscription Identifiers not supported), and `[MQTT-4.13.1-1]` makes closing the
    /// connection the MUST. The next packet read must BE the DISCONNECT — no SUBACK may
    /// precede it, since the guard runs before the ACL loop.
    #[tokio::test]
    async fn v5_subscribe_with_a_subscription_identifier_disconnects_instead_of_subacking() {
        let (mut reader, mut writer, hub_rx) = v5_pipe();
        let _seen = stub_hub(hub_rx);
        writer.send(&connect_v5("c", vec![])).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => assert_eq!(a.code, 0),
            other => panic!("expected v5 CONNACK, got {other:?}"),
        }

        writer
            .send(&Packet::Subscribe(Subscribe {
                pkid: 1,
                filters: vec![SubscribeFilter {
                    path: "a/b".into(),
                    qos: QoS::AtLeastOnce,
                    options: mqtt_codec::SubscriptionOptions::default(),
                }],
                properties: Properties(vec![Property::SubscriptionIdentifier(5)]),
            }))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::Disconnect(d)) => assert_eq!(
                d.reason, 0xA1,
                "0xA1, not the 0xA2 that means Wildcard Subscriptions not supported"
            ),
            other => panic!("expected DISCONNECT 0xa1, got {other:?}"),
        }
        assert_eq!(
            recv(&mut reader).await,
            None,
            "[MQTT-4.13.1-1]: the connection must close"
        );
    }

    /// A malformed `$share/...` filter is answered with 0x80 in the SUBACK
    /// (ADR 0010 §1), while a well-formed one and an ordinary filter are granted.
    #[tokio::test]
    async fn malformed_shared_subscription_is_rejected_in_suback() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let _seen = stub_hub(hub_rx);
        writer.send(&connect_packet("c", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        let filter = |path: &str| SubscribeFilter {
            path: path.into(),
            qos: QoS::AtLeastOnce,
            options: mqtt_codec::SubscriptionOptions::default(),
        };
        writer
            .send(&Packet::Subscribe(Subscribe {
                pkid: 7,
                filters: vec![
                    filter("$share/g/t"), // valid shared
                    filter("plain/t"),    // ordinary
                    filter("$share/g"),   // malformed: no filter part
                    filter("$share//f"),  // malformed: empty share name
                ],
                properties: Properties::new(),
            }))
            .await
            .unwrap();

        match recv(&mut reader).await {
            Some(Packet::SubAck(SubAck {
                pkid, return_codes, ..
            })) => {
                assert_eq!(pkid, 7);
                assert_eq!(
                    return_codes,
                    vec![
                        QoS::AtLeastOnce as u8,
                        QoS::AtLeastOnce as u8,
                        super::SUBACK_FAILURE,
                        super::SUBACK_FAILURE,
                    ]
                );
            }
            other => panic!("expected SUBACK, got {other:?}"),
        }
    }

    /// The v5 CONNACK advertises the server's inbound Topic Alias Maximum, and an
    /// inbound PUBLISH that establishes an alias then references it (empty topic)
    /// resolves to the full topic name before reaching routing (ADR 0011 §2).
    #[tokio::test]
    async fn v5_inbound_topic_alias_resolves_to_full_topic() {
        let (mut reader, mut writer, hub_rx) = v5_pipe();
        let mut topics = stub_hub_topics(hub_rx);
        writer.send(&connect_v5("c", vec![])).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => assert_eq!(
                a.properties.topic_alias_maximum(),
                Some(wire_limits().topic_alias_max),
                "CONNACK advertises our inbound maximum"
            ),
            other => panic!("expected CONNACK, got {other:?}"),
        }

        let publish_alias = |topic: &str| {
            Packet::Publish(Publish {
                properties: Properties(vec![Property::TopicAlias(3)]),
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: topic.into(),
                pkid: None,
                payload: Bytes::from_static(b"x"),
            })
        };
        // Establish 3 -> "sensors/t", then reference it with an empty topic name.
        writer.send(&publish_alias("sensors/t")).await.unwrap();
        writer.send(&publish_alias("")).await.unwrap();

        let first = timeout(Duration::from_millis(500), topics.recv())
            .await
            .expect("a forwarded publish")
            .unwrap();
        let second = timeout(Duration::from_millis(500), topics.recv())
            .await
            .expect("a forwarded publish")
            .unwrap();
        assert_eq!(first, "sensors/t", "establishing PUBLISH carries the topic");
        assert_eq!(second, "sensors/t", "reference resolves to the same topic");
    }

    /// Referencing a topic alias that was never established is a protocol error: the
    /// server sends DISCONNECT 0x94 (Topic Alias Invalid) and then closes (ADR 0011 §2).
    #[tokio::test]
    async fn v5_invalid_topic_alias_disconnects_0x94() {
        let (mut reader, mut writer, hub_rx) = v5_pipe();
        let _topics = stub_hub_topics(hub_rx);
        writer.send(&connect_v5("c", vec![])).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer
            .send(&Packet::Publish(Publish {
                properties: Properties(vec![Property::TopicAlias(7)]),
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: String::new(), // reference, but 7 was never set
                pkid: None,
                payload: Bytes::from_static(b"x"),
            }))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::Disconnect(d)) => assert_eq!(
                d.reason,
                mqtt_codec::reason::TOPIC_ALIAS_INVALID,
                "unmapped alias yields DISCONNECT 0x94"
            ),
            other => panic!("expected DISCONNECT 0x94, got {other:?}"),
        }
        assert_eq!(recv(&mut reader).await, None, "then the connection closes");
    }

    /// `#165` — a `QoS` 2 dedup store error FAILS CLOSED: the broker withholds the `PUBREC`
    /// and closes, rather than forwarding it (which would silently degrade exactly-once to
    /// at-least-once for the duration of the store incident). The first publish succeeds to
    /// prove the path works; the second, under a now-failing store, must get no `PUBREC`
    /// and a closed connection.
    #[tokio::test]
    async fn v5_qos2_dedup_store_error_withholds_pubrec_and_closes() {
        let store = RecordReceivedFails::new();
        let (mut reader, mut writer, hub_rx) = start_conn_with_store(store.clone());
        let _seen = stub_hub(hub_rx);
        writer.send(&connect_v5("c", vec![])).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        let qos2 = |id: u16| {
            Packet::Publish(Publish {
                properties: Properties(vec![]),
                dup: false,
                qos: QoS::ExactlyOnce,
                retain: false,
                topic: "t".into(),
                pkid: Some(id),
                payload: Bytes::from_static(b"x"),
            })
        };

        // Healthy store: the first QoS 2 publish is answered with PUBREC.
        writer.send(&qos2(1)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubRec(a)) => assert_eq!(a.pkid, 1),
            other => panic!("expected PUBREC for the first publish, got {other:?}"),
        }

        // Store now fails every record_received. The next QoS 2 publish must NOT be
        // PUBRECed — the connection closes instead (fail closed, #165).
        store.fail_from_now();
        writer.send(&qos2(2)).await.unwrap();
        assert_eq!(
            recv(&mut reader).await,
            None,
            "a QoS2 dedup store error must withhold the PUBREC and close, not ack a \
             message whose dedup window does not exist"
        );
    }

    /// Hub stub that REFUSES every gated publish with `r`, reporting each topic it
    /// saw on the returned channel — the seam for the issue #238 / 0041-T11
    /// per-version refusal mapping (the real hub's brownout answer).
    fn stub_hub_refusing(
        hub_rx: mpsc::UnboundedReceiver<HubCommand>,
        r: crate::hub::PublishRefusal,
    ) -> mpsc::UnboundedReceiver<String> {
        stub_hub_refusing_watching_detach(hub_rx, Some(r)).0
    }

    /// [`stub_hub_refusing`], plus the `graceful` flag of each `Detach` it sees — the seam
    /// for issue #238's R1: a broker-initiated close must NOT be reported as a clean client
    /// DISCONNECT, or `Hub::detach` skips the Will [MQTT-3.14.4-3]. `None` accepts every
    /// publish (the control direction).
    #[allow(clippy::type_complexity)]
    fn stub_hub_refusing_watching_detach(
        mut hub_rx: mpsc::UnboundedReceiver<HubCommand>,
        r: Option<crate::hub::PublishRefusal>,
    ) -> (
        mpsc::UnboundedReceiver<String>,
        mpsc::UnboundedReceiver<bool>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (detach_tx, detach_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut keep_alive = Vec::new();
            while let Some(cmd) = hub_rx.recv().await {
                match cmd {
                    HubCommand::Attach {
                        outbound, reply, ..
                    } => {
                        keep_alive.push(outbound);
                        let _ = reply.send(AttachOutcome::Present(false));
                    }
                    HubCommand::Publish { topic, done, .. } => {
                        let _ = tx.send(topic);
                        if let Some(done) = done {
                            let _ = done.send(match r {
                                Some(r) => crate::hub::PublishOutcome::Refused(r),
                                None => crate::hub::PublishOutcome::Accepted,
                            });
                        }
                    }
                    HubCommand::Detach { graceful, .. } => {
                        let _ = detach_tx.send(graceful);
                    }
                    _ => {}
                }
            }
        });
        (rx, detach_rx)
    }

    /// A connection over an in-memory duplex framed at `version`, with a durable
    /// session store (so the `QoS` 2 dedup window survives the connection).
    fn conn_with_store_at(
        store: Arc<dyn mqtt_storage::SessionStore>,
        version: ProtocolVersion,
    ) -> (Reader, Writer, mpsc::UnboundedReceiver<HubCommand>) {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        let policy = Arc::new(ConnPolicy {
            auth: auth_handle(Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            })),
            authz: authz_handle(Arc::new(mqtt_auth::AllowAll)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            node: None,
            store: Some(store),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            enhanced: None,
            shutdown: None,
            metrics: None,
        });
        tokio::spawn(handle_stream(server, None, None, policy, hub_tx));
        let (rh, wh) = tokio::io::split(client);
        (
            FrameReader::new(rh, version),
            FrameWriter::new(wh, version),
            hub_rx,
        )
    }

    /// The next topic the refusing stub saw, bounded so a regression fails fast
    /// instead of hanging until the channel closes.
    async fn next_topic(rx: &mut mpsc::UnboundedReceiver<String>) -> Option<String> {
        timeout(Duration::from_millis(500), rx.recv())
            .await
            .ok()
            .flatten()
    }

    fn qos1_publish(id: u16) -> Packet {
        Packet::Publish(Publish {
            properties: mqtt_codec::Properties::new(),
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "t".to_string(),
            pkid: Some(id),
            payload: Bytes::from_static(b"x"),
        })
    }

    /// Issue #238 / 0041-T11 — a v5 publisher whose publish the hub REFUSES is told
    /// `0x97 Quota exceeded` on the PUBACK, and the connection stays open. A reason
    /// >= 0x80 ends the flow: the Receive-Maximum slot is released and the packet id
    /// is reusable, so the refusal is bounded and immediately actionable.
    #[tokio::test]
    async fn a_v5_publisher_is_answered_0x97_when_the_hub_refuses_the_publish() {
        let (mut reader, mut writer, hub_rx) = v5_pipe();
        let _topics = stub_hub_refusing(hub_rx, crate::hub::PublishRefusal::Brownout);
        writer.send(&connect_v5("c", vec![])).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer.send(&qos1_publish(1)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubAck(a)) => {
                assert_eq!(a.pkid, 1);
                assert_eq!(
                    a.reason,
                    mqtt_codec::reason::QUOTA_EXCEEDED,
                    "a refused publish is answered 0x97, not acked"
                );
            }
            other => panic!("expected PUBACK 0x97, got {other:?}"),
        }
        // Still open: the refusal is per-publish, not a connection verdict.
        writer.send(&qos1_publish(2)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubAck(a)) => assert_eq!(a.pkid, 2),
            other => panic!("the connection must stay open, got {other:?}"),
        }
    }

    /// Issue #238 — v3.1.1 has no PUBACK reason byte, so each refusal must state
    /// whether it is *sayable* as a plain ack. `Brownout` is not: the message was
    /// not stored, so the honest answer is no ack and a close, and the publisher
    /// retries per [MQTT-4.4.0-1]. `RetainedQuota` is: the value was deliberately
    /// not retained and a retry would change nothing, so the plain ack stands.
    #[tokio::test]
    async fn a_v311_publisher_is_closed_without_a_puback_when_the_hub_refuses_a_brownout_publish() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let _topics = stub_hub_refusing(hub_rx, crate::hub::PublishRefusal::Brownout);
        writer.send(&connect_packet("c", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer.send(&qos1_publish(1)).await.unwrap();
        assert_eq!(
            recv(&mut reader).await,
            None,
            "a v3.1.1 publisher must NOT be acked for a message brownout refused; \
             the connection closes and it retries"
        );

        // The sibling disposition must not be conflated with it.
        let (mut reader, mut writer, hub_rx) = start_conn();
        let _topics = stub_hub_refusing(hub_rx, crate::hub::PublishRefusal::RetainedQuota);
        writer.send(&connect_packet("c", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
        writer.send(&qos1_publish(1)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubAck(a)) => {
                assert_eq!(a.pkid, 1);
                assert_eq!(a.reason, 0, "v3.1.1 has no reason byte to carry");
            }
            other => panic!("expected a plain PUBACK, got {other:?}"),
        }
        writer.send(&qos1_publish(2)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubAck(a)) => assert_eq!(a.pkid, 2, "and the connection stays open"),
            other => panic!("the connection must stay open, got {other:?}"),
        }
    }

    /// Issue #238, the `QoS` 2 half. REPLACES an earlier test
    /// (`a_refused_qos2_publish_releases_the_packet_id_for_both_protocol_versions`) which
    /// asserted that the two CLOSE exits erase the dedup record. Erasing it re-fans-out
    /// the client's mandatory resend [MQTT-4.4.0-1] — and the first attempt may already
    /// have stored or delivered copies — so that test codified the defect rather than
    /// guarding against it. The record must stay HELD-UNACKED instead: the truth is that
    /// the broker said nothing.
    #[tokio::test]
    async fn a_withheld_qos2_publish_stays_held_but_unacked_so_its_resend_is_never_answered_as_a_duplicate(
    ) {
        let store: Arc<dyn mqtt_storage::SessionStore> =
            Arc::new(mqtt_storage::MemorySessionStore::new());
        let cid = mqtt_core::ClientId("q2".into());
        let (mut reader, mut writer, hub_rx) = conn_with_store_at(store.clone(), V4);
        let mut topics = stub_hub_refusing(hub_rx, crate::hub::PublishRefusal::Brownout);
        writer.send(&connect_packet("q2", false)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer.send(&qos2_publish(1)).await.unwrap();
        assert_eq!(next_topic(&mut topics).await.as_deref(), Some("t"));
        assert_eq!(
            recv(&mut reader).await,
            None,
            "no PUBREC for a refused v3.1.1 QoS 2 publish; the connection closes"
        );
        assert_eq!(
            store.received(&cid).await.unwrap(),
            vec![1],
            "the id stays HELD: the broker has seen it"
        );
        assert_eq!(
            store.record_received(&cid, 1).await.unwrap(),
            mqtt_storage::InboundSighting::HeldUnacked,
            "and UNACKNOWLEDGED: nothing was ever said about it"
        );

        // The resend on a fresh connection must reach ROUTING again — re-decided, not
        // answered from a window whose entry never earned a PUBREC.
        let (mut reader, mut writer, hub_rx) = conn_with_store_at(store, V4);
        let mut topics = stub_hub_refusing(hub_rx, crate::hub::PublishRefusal::Brownout);
        writer.send(&connect_packet("q2", false)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
        writer.send(&qos2_publish(1)).await.unwrap();
        assert_eq!(
            next_topic(&mut topics).await.as_deref(),
            Some("t"),
            "the resend after the close must be re-decided, not answered as a duplicate"
        );
        assert_eq!(
            recv(&mut reader).await,
            None,
            "and still refused: no fabricated success PUBREC"
        );
    }

    /// The anti-vacuity partner: the fix must not simply stop deduplicating. An ACCEPTED
    /// `QoS` 2 publish marks its id acked BEFORE the PUBREC reaches the wire, so its DUP
    /// is answered from the window with no second fan-out [MQTT-4.3.3-2].
    #[tokio::test]
    async fn an_accepted_qos2_publish_marks_the_id_acked_before_the_pubrec_so_its_dup_is_answered_from_the_window(
    ) {
        let store: Arc<dyn mqtt_storage::SessionStore> =
            Arc::new(mqtt_storage::MemorySessionStore::new());
        let cid = mqtt_core::ClientId("q2ok".into());
        let (mut reader, mut writer, hub_rx) = conn_with_store_at(store.clone(), V4);
        let (mut topics, _detach) = stub_hub_refusing_watching_detach(hub_rx, None);
        writer.send(&connect_packet("q2ok", false)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer.send(&qos2_publish(1)).await.unwrap();
        assert_eq!(next_topic(&mut topics).await.as_deref(), Some("t"));
        assert_eq!(recv(&mut reader).await, Some(Packet::PubRec(1.into())));
        assert_eq!(
            store.record_received(&cid, 1).await.unwrap(),
            mqtt_storage::InboundSighting::HeldAcked,
            "the acked bit lands with (in fact before) the PUBREC"
        );

        // A DUP of an acknowledged flow: PUBREC again, and NO second fan-out.
        writer.send(&qos2_publish(1)).await.unwrap();
        assert_eq!(recv(&mut reader).await, Some(Packet::PubRec(1.into())));
        assert_eq!(
            next_topic(&mut topics).await,
            None,
            "an acknowledged id must never be fanned out twice"
        );

        // PUBREL frees the id entirely.
        writer
            .send(&Packet::PubRel(mqtt_codec::packet::Ack::from(1)))
            .await
            .unwrap();
        assert_eq!(recv(&mut reader).await, Some(Packet::PubComp(1.into())));
        assert!(store.received(&cid).await.unwrap().is_empty());
    }

    /// A v5 `PUBREC >= 0x80` ENDS the flow by spec, so the id must be freed COMPLETELY —
    /// not merely left unacked. Both sides consider it finished; a lingering record would
    /// make a legitimate reuse of the id look like a retry of a different message.
    #[tokio::test]
    async fn a_v5_qos2_refusal_ends_the_flow_and_frees_the_id_completely() {
        let store: Arc<dyn mqtt_storage::SessionStore> =
            Arc::new(mqtt_storage::MemorySessionStore::new());
        let cid = mqtt_core::ClientId("q2v5".into());
        let (mut reader, mut writer, hub_rx) = conn_with_store_at(store.clone(), V5);
        let mut topics = stub_hub_refusing(hub_rx, crate::hub::PublishRefusal::Brownout);
        writer.send(&connect_v5("q2v5", vec![])).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer.send(&qos2_publish(1)).await.unwrap();
        assert_eq!(next_topic(&mut topics).await.as_deref(), Some("t"));
        match recv(&mut reader).await {
            Some(Packet::PubRec(a)) => {
                assert_eq!(a.pkid, 1);
                assert_eq!(a.reason, mqtt_codec::reason::QUOTA_EXCEEDED);
            }
            other => panic!("expected PUBREC 0x97, got {other:?}"),
        }
        assert!(
            store.received(&cid).await.unwrap().is_empty(),
            "a reason >= 0x80 releases the packet id entirely"
        );
        // A later publish under the same id is a genuinely NEW sighting and reaches
        // routing.
        writer.send(&qos2_publish(1)).await.unwrap();
        assert_eq!(next_topic(&mut topics).await.as_deref(), Some("t"));
    }

    /// An authorizer that denies publishing to exactly one topic (`"secret"`) and
    /// allows everything else — the issue #246 seam: one connection can exercise
    /// both the denied and the allowed path, so "the ACL still denies" and "the
    /// same packet id works when allowed" are testable side by side.
    #[derive(Debug)]
    struct DenySecret;

    impl mqtt_auth::Authorizer for DenySecret {
        fn authorize_publish(
            &self,
            _id: &mqtt_auth::Identity,
            _client: &mqtt_core::ClientId,
            topic: &mqtt_core::TopicName,
        ) -> bool {
            topic != "secret"
        }
        fn authorize_subscribe(
            &self,
            _id: &mqtt_auth::Identity,
            _client: &mqtt_core::ClientId,
            _f: &mqtt_core::TopicFilter,
        ) -> bool {
            true
        }
    }

    /// A connection at `version` whose policy DENIES publishes to `"secret"`,
    /// records every audit event, and optionally carries a durable session store
    /// (for the `QoS` 2 dedup assertions) — the issue #246 harness.
    #[allow(clippy::type_complexity)]
    fn conn_denying_secret(
        version: ProtocolVersion,
        store: Option<Arc<dyn mqtt_storage::SessionStore>>,
    ) -> (
        Reader,
        Writer,
        mpsc::UnboundedReceiver<HubCommand>,
        Arc<mqtt_observability::RecordingAuditSink>,
    ) {
        let audit = Arc::new(mqtt_observability::RecordingAuditSink::new());
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        let policy = Arc::new(ConnPolicy {
            auth: auth_handle(Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            })),
            authz: authz_handle(Arc::new(DenySecret)),
            identity_source: mqtt_auth::mtls::IdentitySource::default(),
            audit: audit.clone(),
            proxy: None,
            node: None,
            store,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            enhanced: None,
            shutdown: None,
            metrics: None,
        });
        tokio::spawn(handle_stream(server, None, None, policy, hub_tx));
        let (rh, wh) = tokio::io::split(client);
        (
            FrameReader::new(rh, version),
            FrameWriter::new(wh, version),
            hub_rx,
            audit,
        )
    }

    /// A PUBLISH to `topic` at `qos` — the topic-parameterised sibling of
    /// [`qos1_publish`]/[`qos2_publish`], for the ACL tests that need both a
    /// denied and an allowed topic on one connection.
    fn publish_to(topic: &str, qos: QoS, id: Option<u16>, dup: bool) -> Packet {
        Packet::Publish(Publish {
            properties: mqtt_codec::Properties::new(),
            dup,
            qos,
            retain: false,
            topic: topic.to_string(),
            pkid: id,
            payload: Bytes::from_static(b"x"),
        })
    }

    /// Issue #246 — an ACL-denied v5 `QoS` 1 publish is answered PUBACK `0x87 Not
    /// authorized`, not acknowledged as success. The connection stays open (a
    /// refusal is per-publish, never a connection verdict), the denial is still
    /// audited, and the message never reaches the hub.
    #[tokio::test]
    async fn a_v5_qos1_publish_denied_by_the_acl_is_answered_puback_0x87() {
        let (mut reader, mut writer, hub_rx, audit) = conn_denying_secret(V5, None);
        let (mut topics, _detach) = stub_hub_refusing_watching_detach(hub_rx, None);
        writer.send(&connect_v5("acl1", vec![])).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer
            .send(&publish_to("secret", QoS::AtLeastOnce, Some(1), false))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubAck(a)) => {
                assert_eq!(a.pkid, 1);
                assert_eq!(
                    a.reason,
                    mqtt_codec::reason::NOT_AUTHORIZED,
                    "a denied v5 publish must be told 0x87, not acked as success"
                );
            }
            other => panic!("expected PUBACK 0x87, got {other:?}"),
        }
        // Still open, and an allowed publish on the same connection works normally.
        writer
            .send(&publish_to("t", QoS::AtLeastOnce, Some(2), false))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubAck(a)) => {
                assert_eq!(a.pkid, 2);
                assert_eq!(a.reason, 0);
            }
            other => panic!("the connection must stay open, got {other:?}"),
        }
        assert_eq!(
            next_topic(&mut topics).await.as_deref(),
            Some("t"),
            "only the allowed publish reaches the hub"
        );
        assert!(
            audit.kinds().iter().any(|k| k == "acl.deny.publish"),
            "the denial stays audited"
        );
    }

    /// Issue #246, the `QoS` 2 half — a denied v5 `QoS` 2 publish is answered
    /// PUBREC `0x87`, which ends the flow BY SPEC: the dedup record is released
    /// entirely, a DUP resend is a FRESH decision (re-denied while the ACL still
    /// denies — never answered as a duplicate of a message nothing accepted), and
    /// the same id under an allowed topic is genuinely new and reaches routing.
    #[tokio::test]
    async fn a_v5_qos2_publish_denied_by_the_acl_ends_the_flow_with_pubrec_0x87() {
        let store: Arc<dyn mqtt_storage::SessionStore> =
            Arc::new(mqtt_storage::MemorySessionStore::new());
        let cid = mqtt_core::ClientId("aclq2".into());
        let (mut reader, mut writer, hub_rx, audit) = conn_denying_secret(V5, Some(store.clone()));
        let (mut topics, _detach) = stub_hub_refusing_watching_detach(hub_rx, None);
        writer.send(&connect_v5("aclq2", vec![])).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer
            .send(&publish_to("secret", QoS::ExactlyOnce, Some(1), false))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubRec(a)) => {
                assert_eq!(a.pkid, 1);
                assert_eq!(
                    a.reason,
                    mqtt_codec::reason::NOT_AUTHORIZED,
                    "a denied v5 QoS 2 publish is answered PUBREC 0x87"
                );
            }
            other => panic!("expected PUBREC 0x87, got {other:?}"),
        }
        assert!(
            store.received(&cid).await.unwrap().is_empty(),
            "a PUBREC >= 0x80 ends the flow by spec: the id is released entirely"
        );

        // A DUP resend of the denied id is a FRESH decision: re-denied 0x87 while
        // the ACL still denies, never a fabricated answer from a released window.
        writer
            .send(&publish_to("secret", QoS::ExactlyOnce, Some(1), true))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubRec(a)) => {
                assert_eq!(a.reason, mqtt_codec::reason::NOT_AUTHORIZED);
            }
            other => panic!("expected the DUP re-denied with PUBREC 0x87, got {other:?}"),
        }
        assert_eq!(
            next_topic(&mut topics).await,
            None,
            "nothing denied ever reached the hub"
        );

        // The same id under an ALLOWED topic is genuinely new: success, and routed.
        writer
            .send(&publish_to("t", QoS::ExactlyOnce, Some(1), false))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubRec(a)) => {
                assert_eq!(
                    a.reason, 0,
                    "the allowed reuse of the id is a fresh success"
                );
            }
            other => panic!("expected a success PUBREC, got {other:?}"),
        }
        assert_eq!(next_topic(&mut topics).await.as_deref(), Some("t"));
        assert!(audit.kinds().iter().any(|k| k == "acl.deny.publish"));
    }

    /// Issue #246 — v3.1.1 is UNCHANGED: it has no per-publish reason code, so a
    /// denied publish stays drop-and-audit with a plain success ack (withholding
    /// the ack would strand a conforming publisher in retry), the denied `QoS` 2
    /// id keeps its #238-era ACKED dedup record (the broker DID acknowledge it,
    /// so a DUP is honestly answerable as a duplicate), and the flow completes
    /// normally.
    #[tokio::test]
    async fn a_v311_denied_publish_keeps_its_plain_ack_and_audit_for_both_qos_levels() {
        let store: Arc<dyn mqtt_storage::SessionStore> =
            Arc::new(mqtt_storage::MemorySessionStore::new());
        let cid = mqtt_core::ClientId("acl311".into());
        let (mut reader, mut writer, hub_rx, audit) = conn_denying_secret(V4, Some(store.clone()));
        let (mut topics, _detach) = stub_hub_refusing_watching_detach(hub_rx, None);
        writer.send(&connect_packet("acl311", false)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer
            .send(&publish_to("secret", QoS::AtLeastOnce, Some(1), false))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubAck(a)) => {
                assert_eq!(a.pkid, 1);
                assert_eq!(
                    a.reason, 0,
                    "v3.1.1 has no reason byte: the plain ack stands"
                );
            }
            other => panic!("expected a plain PUBACK, got {other:?}"),
        }
        writer
            .send(&publish_to("secret", QoS::ExactlyOnce, Some(2), false))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::PubRec(a)) => {
                assert_eq!(a.pkid, 2);
                assert_eq!(a.reason, 0);
            }
            other => panic!("expected a plain PUBREC, got {other:?}"),
        }
        // The plain PUBREC *was* an acknowledgement, so the id keeps its ACKED
        // dedup record — v3.1.1's #238-era treatment, unchanged by #246 (only a
        // v5 reason >= 0x80 ends a flow and releases the id).
        assert_eq!(
            store.record_received(&cid, 2).await.unwrap(),
            mqtt_storage::InboundSighting::HeldAcked,
            "a v3.1.1 denied QoS 2 id stays held-and-acked"
        );
        // The v3.1.1 QoS 2 flow still completes normally after the drop.
        writer
            .send(&Packet::PubRel(mqtt_codec::packet::Ack::from(2)))
            .await
            .unwrap();
        assert_eq!(recv(&mut reader).await, Some(Packet::PubComp(2.into())));
        assert_eq!(
            next_topic(&mut topics).await,
            None,
            "denied publishes never reach the hub"
        );
        assert_eq!(
            audit
                .kinds()
                .iter()
                .filter(|k| k.as_str() == "acl.deny.publish")
                .count(),
            2,
            "both denials are audited"
        );
    }

    /// Issue #246 — `QoS` 0 has nothing to answer: a denied `QoS` 0 publish stays
    /// a silent (audited) drop for v5 as for v3.1.1, and the connection stays open.
    #[tokio::test]
    async fn a_denied_qos0_publish_stays_a_silent_drop_in_both_versions() {
        for version in [V5, V4] {
            let (mut reader, mut writer, hub_rx, audit) = conn_denying_secret(version, None);
            let (mut topics, _detach) = stub_hub_refusing_watching_detach(hub_rx, None);
            if version == V5 {
                writer.send(&connect_v5("aclq0", vec![])).await.unwrap();
            } else {
                writer.send(&connect_packet("aclq0", true)).await.unwrap();
            }
            assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

            writer
                .send(&publish_to("secret", QoS::AtMostOnce, None, false))
                .await
                .unwrap();
            // Nothing is answered for QoS 0; the next (allowed) publish proves the
            // connection is open and that the denied one was dropped before the hub.
            writer
                .send(&publish_to("t", QoS::AtLeastOnce, Some(1), false))
                .await
                .unwrap();
            match recv(&mut reader).await {
                Some(Packet::PubAck(a)) => assert_eq!(a.pkid, 1),
                other => panic!("expected the follow-up PUBACK, got {other:?}"),
            }
            assert_eq!(next_topic(&mut topics).await.as_deref(), Some("t"));
            assert_eq!(next_topic(&mut topics).await, None);
            assert!(audit.kinds().iter().any(|k| k == "acl.deny.publish"));
        }
    }

    /// The pre-existing hub-gone hole, now guarded (issue #238): the `Err(_)` arm at the
    /// ack gate had no test at all — deleting its behaviour left every test green. A
    /// withheld PUBREC says nothing, so the record must stay held-unacked and the resend
    /// must be re-attempted.
    #[tokio::test]
    async fn a_qos2_publish_whose_durable_fan_out_failed_stays_held_unacked_and_is_re_attempted() {
        let store: Arc<dyn mqtt_storage::SessionStore> =
            Arc::new(mqtt_storage::MemorySessionStore::new());
        let cid = mqtt_core::ClientId("q2drop".into());
        let (mut reader, mut writer, mut hub_rx) = conn_with_store_at(store.clone(), V4);
        // A hub that DROPS the `done` sender for every publish — the withhold path.
        let (seen_tx, mut seen) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut keep_alive = Vec::new();
            while let Some(cmd) = hub_rx.recv().await {
                match cmd {
                    HubCommand::Attach {
                        outbound, reply, ..
                    } => {
                        keep_alive.push(outbound);
                        let _ = reply.send(AttachOutcome::Present(false));
                    }
                    HubCommand::Publish { topic, done, .. } => {
                        let _ = seen_tx.send(topic);
                        drop(done); // withhold: the fan-out failed terminally
                    }
                    _ => {}
                }
            }
        });
        writer.send(&connect_packet("q2drop", false)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer.send(&qos2_publish(1)).await.unwrap();
        assert_eq!(next_topic(&mut seen).await.as_deref(), Some("t"));
        assert_eq!(
            recv(&mut reader).await,
            None,
            "no PUBREC, connection closed"
        );
        assert_eq!(
            store.record_received(&cid, 1).await.unwrap(),
            mqtt_storage::InboundSighting::HeldUnacked,
            "a withheld PUBREC leaves the id held-but-unacknowledged"
        );
    }

    /// Issue #238 (R1) — a broker-initiated close must NOT be reported to the hub as a
    /// graceful client DISCONNECT, or `Hub::detach` skips the Will [MQTT-3.14.4-3].
    ///
    /// Concretely: a v3.1.1 device publishing `QoS` 1 telemetry into a browned-out node is
    /// disconnected with no PUBACK. If that close is `graceful`, its LWT never fires and
    /// every dashboard keeps showing the device as online — through exactly the incident
    /// the Will exists for.
    #[tokio::test]
    async fn a_v311_publisher_closed_by_a_refusal_detaches_ungracefully_so_its_will_fires() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let (_topics, mut detaches) =
            stub_hub_refusing_watching_detach(hub_rx, Some(crate::hub::PublishRefusal::Brownout));
        writer.send(&connect_packet("willy", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer.send(&qos1_publish(1)).await.unwrap();
        assert_eq!(recv(&mut reader).await, None, "refused: no PUBACK, closed");
        assert!(
            !timeout(Duration::from_millis(500), detaches.recv())
                .await
                .expect("a Detach reaches the hub")
                .expect("the channel is open"),
            "the broker hung up, so this is NOT a clean client DISCONNECT — the Will \
             must fire [MQTT-3.14.4-3]"
        );
    }

    /// The other side of R1: a client that actually sends DISCONNECT IS graceful, so its
    /// Will is discarded. Without this, "report every close as ungraceful" would pass the
    /// test above while breaking the spec in the other direction.
    #[tokio::test]
    async fn a_client_disconnect_is_still_reported_as_graceful() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let (_topics, mut detaches) = stub_hub_refusing_watching_detach(hub_rx, None);
        writer.send(&connect_packet("bye", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer
            .send(&Packet::Disconnect(mqtt_codec::packet::Disconnect {
                reason: 0,
                properties: mqtt_codec::Properties::new(),
            }))
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_millis(500), detaches.recv())
                .await
                .expect("a Detach reaches the hub")
                .expect("the channel is open"),
            "a client DISCONNECT discards the will [MQTT-3.14.4-3]"
        );
    }

    /// Issue #265 — a v5 DISCONNECT with a **non-zero** reason is a will-firing
    /// close: `0x04` is an explicit "Disconnect with Will Message", and any error
    /// reason is an abnormal end, since only reason `0x00` discards the Will
    /// [MQTT-3.1.2-10]. The detach must be un-graceful for both.
    #[tokio::test]
    async fn a_v5_disconnect_with_a_non_zero_reason_detaches_ungracefully_so_the_will_fires() {
        for reason in [mqtt_codec::reason::DISCONNECT_WITH_WILL, 0x81] {
            let (mut reader, mut writer, hub_rx) = v5_pipe();
            let (_topics, mut detaches) = stub_hub_refusing_watching_detach(hub_rx, None);
            writer.send(&connect_v5("willful", vec![])).await.unwrap();
            assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

            writer
                .send(&Packet::Disconnect(mqtt_codec::packet::Disconnect {
                    reason,
                    properties: mqtt_codec::Properties::new(),
                }))
                .await
                .unwrap();
            assert!(
                !timeout(Duration::from_millis(500), detaches.recv())
                    .await
                    .expect("a Detach reaches the hub")
                    .expect("the channel is open"),
                "a v5 DISCONNECT with reason {reason:#04x} asks for its Will \
                 [MQTT-3.1.2-10] — the detach must be un-graceful"
            );
        }
    }

    /// Issue #265 — a protocol-violation close (here: a wildcard PUBLISH topic) is
    /// broker-initiated and must detach un-gracefully so the Will fires
    /// [MQTT-3.14.4-3]. The refusal-close sibling is covered by the #238 test
    /// above; this pins the violation class.
    #[tokio::test]
    async fn a_protocol_violation_close_detaches_ungracefully_so_the_will_fires() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let (_topics, mut detaches) = stub_hub_refusing_watching_detach(hub_rx, None);
        writer.send(&connect_packet("viol", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer
            .send(&Packet::Publish(Publish {
                properties: mqtt_codec::Properties::new(),
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: "a/#".to_string(),
                pkid: None,
                payload: bytes::Bytes::from_static(b"x"),
            }))
            .await
            .unwrap();
        assert_eq!(
            recv(&mut reader).await,
            None,
            "the violation closes the conn"
        );
        assert!(
            !timeout(Duration::from_millis(500), detaches.recv())
                .await
                .expect("a Detach reaches the hub")
                .expect("the channel is open"),
            "a broker-initiated protocol-violation close must fire the Will"
        );
    }

    /// Issue #265 — an EOF without a DISCONNECT (the client vanished: power loss,
    /// network drop, crash) is the canonical will-firing end [MQTT-3.14.4-3].
    #[tokio::test]
    async fn an_eof_without_disconnect_detaches_ungracefully_so_the_will_fires() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let (_topics, mut detaches) = stub_hub_refusing_watching_detach(hub_rx, None);
        writer.send(&connect_packet("gone", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        // Drop both halves of the client socket: the broker sees a bare EOF.
        drop(reader);
        drop(writer);
        assert!(
            !timeout(Duration::from_millis(500), detaches.recv())
                .await
                .expect("a Detach reaches the hub")
                .expect("the channel is open"),
            "an EOF without DISCONNECT must fire the Will"
        );
    }

    /// Issue #265 — a keepalive expiry is a will-firing end [MQTT-3.1.2-24 +
    /// MQTT-3.14.4-3]: the client went silent, which is exactly what the Will
    /// exists to report. Paused virtual time, as in the keepalive-close test.
    #[tokio::test(start_paused = true)]
    async fn a_keepalive_expiry_detaches_ungracefully_so_the_will_fires() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let (_topics, mut detaches) = stub_hub_refusing_watching_detach(hub_rx, None);
        let connect = Packet::Connect(Connect {
            properties: mqtt_codec::Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 1,
            client_id: "quiet".into(),
            last_will: None,
            username: None,
            password: None,
        });
        writer.send(&connect).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        // Idle past the 1.5x grace; the auto-advancing clock fires the deadline.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            !timeout(Duration::from_millis(500), detaches.recv())
                .await
                .expect("a Detach reaches the hub")
                .expect("the channel is open"),
            "a keepalive expiry must fire the Will"
        );
    }

    fn qos2_publish(id: u16) -> Packet {
        Packet::Publish(Publish {
            properties: mqtt_codec::Properties::new(),
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: "t".to_string(),
            pkid: Some(id),
            payload: bytes::Bytes::from_static(b"x"),
        })
    }

    /// A client must not have more unreleased `QoS` 2 publishes outstanding than the
    /// server's Receive Maximum; the publish that exceeds the window is answered with
    /// DISCONNECT 0x93 (Receive Maximum exceeded), ADR 0012 §3.
    #[tokio::test]
    async fn v5_receive_maximum_exceeded_disconnects_0x93() {
        let (mut reader, mut writer, hub_rx) = v5_pipe();
        let _seen = stub_hub(hub_rx);
        writer.send(&connect_v5("c", vec![])).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        // Fill the window: `limit` distinct QoS 2 ids, each PUBREC'd, none PUBREL'd (so they
        // stay outstanding). Send-then-read each to avoid filling the duplex pipe buffer.
        let limit = wire_limits().receive_maximum;
        for id in 1..=limit {
            writer.send(&qos2_publish(id)).await.unwrap();
            assert_eq!(recv(&mut reader).await, Some(Packet::PubRec(id.into())));
        }
        // One more distinct unreleased id exceeds the window.
        writer.send(&qos2_publish(limit + 1)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::Disconnect(d)) => assert_eq!(
                d.reason,
                mqtt_codec::reason::RECEIVE_MAXIMUM_EXCEEDED,
                "exceeding Receive Maximum yields DISCONNECT 0x93"
            ),
            other => panic!("expected DISCONNECT 0x93, got {other:?}"),
        }
    }

    /// When the client advertises a Topic Alias Maximum, the server assigns an alias
    /// on the first PUBLISH of a topic (full name + alias) and references it on the
    /// next (empty name + alias) — ADR 0011 §3.
    #[tokio::test]
    async fn v5_outbound_topic_alias_assigns_then_references() {
        let (mut reader, mut writer, hub_rx) = v5_pipe();
        let out_rx = stub_hub_capture_outbound(hub_rx);
        writer
            .send(&connect_v5("c", vec![Property::TopicAliasMaximum(5)]))
            .await
            .unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
        let out = timeout(Duration::from_millis(500), out_rx)
            .await
            .expect("attach")
            .expect("outbound sender");

        assert!(out.send(server_publish("room/temp")));
        match recv(&mut reader).await {
            Some(Packet::Publish(p)) => {
                assert_eq!(p.topic, "room/temp", "first send keeps the full topic");
                assert_eq!(p.properties.topic_alias(), Some(1));
            }
            other => panic!("expected PUBLISH, got {other:?}"),
        }

        assert!(out.send(server_publish("room/temp")));
        match recv(&mut reader).await {
            Some(Packet::Publish(p)) => {
                assert_eq!(p.topic, "", "second send references the alias");
                assert_eq!(p.properties.topic_alias(), Some(1));
            }
            other => panic!("expected PUBLISH, got {other:?}"),
        }
    }

    /// The v5 CONNACK advertises the server's Receive Maximum, and the client's
    /// CONNECT Receive Maximum is forwarded to the hub as the outbound quota (ADR 0012).
    #[tokio::test]
    async fn v5_receive_maximum_is_advertised_and_forwarded() {
        let (mut reader, mut writer, hub_rx) = v5_pipe();
        let rx_max = stub_hub_capture_receive_maximum(hub_rx);
        writer
            .send(&connect_v5("c", vec![Property::ReceiveMaximum(7)]))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => assert_eq!(
                a.properties.receive_maximum(),
                Some(wire_limits().receive_maximum),
                "CONNACK advertises our inbound Receive Maximum"
            ),
            other => panic!("expected CONNACK, got {other:?}"),
        }
        let forwarded = timeout(Duration::from_millis(500), rx_max)
            .await
            .expect("attach")
            .expect("receive maximum");
        assert_eq!(
            forwarded, 7,
            "the client's Receive Maximum drives the outbound quota"
        );
    }

    /// A v3.1.1 connection has no Receive Maximum property, so the hub gets the
    /// unlimited default.
    #[tokio::test]
    async fn v311_receive_maximum_defaults_to_unlimited() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let rx_max = stub_hub_capture_receive_maximum(hub_rx);
        writer.send(&connect_packet("c", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
        let forwarded = timeout(Duration::from_millis(500), rx_max)
            .await
            .expect("attach")
            .expect("receive maximum");
        assert_eq!(forwarded, u16::MAX, "v3.1.1 imposes no outbound quota");
    }

    /// A v5 connection with an enhanced HMAC-SHA256 authenticator: the broker
    /// challenges with a nonce, the client returns a correct HMAC, and the CONNACK
    /// succeeds (ADR 0013).
    #[tokio::test]
    async fn v5_enhanced_auth_hmac_succeeds() {
        let (mut reader, mut writer) = enhanced_conn();
        writer
            .send(&connect_v5(
                "c",
                vec![
                    Property::AuthenticationMethod("HMAC-SHA256".into()),
                    Property::AuthenticationData(Bytes::from_static(b"alice")),
                ],
            ))
            .await
            .unwrap();

        let nonce = match recv(&mut reader).await {
            Some(Packet::Auth(a)) => {
                assert_eq!(a.reason, 0x18, "AUTH continue");
                assert_eq!(a.properties.authentication_method(), Some("HMAC-SHA256"));
                a.properties.authentication_data().unwrap().to_vec()
            }
            other => panic!("expected AUTH challenge, got {other:?}"),
        };

        let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, b"alice-secret");
        let proof = aws_lc_rs::hmac::sign(&key, &nonce);
        writer
            .send(&Packet::Auth(Auth {
                reason: 0x18,
                properties: Properties(vec![
                    Property::AuthenticationMethod("HMAC-SHA256".into()),
                    Property::AuthenticationData(Bytes::copy_from_slice(proof.as_ref())),
                ]),
            }))
            .await
            .unwrap();

        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => assert_eq!(a.code, 0, "enhanced auth accepted"),
            other => panic!("expected CONNACK success, got {other:?}"),
        }
    }

    /// A wrong HMAC proof is rejected with CONNACK 0x87 (Not authorized).
    #[tokio::test]
    async fn v5_enhanced_auth_wrong_proof_is_rejected() {
        let (mut reader, mut writer) = enhanced_conn();
        writer
            .send(&connect_v5(
                "c",
                vec![
                    Property::AuthenticationMethod("HMAC-SHA256".into()),
                    Property::AuthenticationData(Bytes::from_static(b"alice")),
                ],
            ))
            .await
            .unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::Auth(_))));

        // A proof under the wrong key.
        let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, b"guessed");
        let proof = aws_lc_rs::hmac::sign(&key, b"any-nonce");
        writer
            .send(&Packet::Auth(Auth {
                reason: 0x18,
                properties: Properties(vec![
                    Property::AuthenticationMethod("HMAC-SHA256".into()),
                    Property::AuthenticationData(Bytes::copy_from_slice(proof.as_ref())),
                ]),
            }))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => {
                assert_eq!(a.code, super::CONNACK_V5_NOT_AUTHORIZED, "rejected");
            }
            other => panic!("expected rejecting CONNACK, got {other:?}"),
        }
    }

    /// A method with no configured mechanism is refused with CONNACK 0x8C.
    #[tokio::test]
    async fn v5_enhanced_auth_unknown_method_is_rejected() {
        let (mut reader, mut writer) = enhanced_conn();
        writer
            .send(&connect_v5(
                "c",
                vec![Property::AuthenticationMethod("SCRAM-SHA-1".into())],
            ))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(a)) => {
                assert_eq!(a.code, super::CONNACK_V5_BAD_AUTH_METHOD, "bad auth method");
            }
            other => panic!("expected rejecting CONNACK, got {other:?}"),
        }
    }

    /// After an enhanced-auth connect, a client-initiated AUTH `0x19` runs a fresh
    /// exchange and the broker answers AUTH `0x00` on success (ADR 0013 §4).
    #[tokio::test]
    async fn v5_reauthentication_succeeds() {
        let (mut reader, mut writer) = enhanced_conn();
        connect_and_authenticate(&mut reader, &mut writer).await;

        writer.send(&hmac_auth(0x19, b"alice")).await.unwrap();
        let nonce = match recv(&mut reader).await {
            Some(Packet::Auth(a)) => {
                assert_eq!(a.reason, 0x18, "re-auth challenge");
                a.properties.authentication_data().unwrap().to_vec()
            }
            other => panic!("expected AUTH challenge, got {other:?}"),
        };
        writer
            .send(&hmac_auth(0x18, &alice_proof(&nonce)))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::Auth(a)) => assert_eq!(a.reason, 0x00, "re-auth succeeded"),
            other => panic!("expected AUTH success, got {other:?}"),
        }
    }

    /// A wrong proof during re-auth disconnects the established session (0x87).
    #[tokio::test]
    async fn v5_reauthentication_wrong_proof_disconnects() {
        let (mut reader, mut writer) = enhanced_conn();
        connect_and_authenticate(&mut reader, &mut writer).await;

        writer.send(&hmac_auth(0x19, b"alice")).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::Auth(_))));
        // Proof under the wrong key.
        let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, b"wrong");
        let proof = aws_lc_rs::hmac::sign(&key, b"x");
        writer.send(&hmac_auth(0x18, proof.as_ref())).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::Disconnect(d)) => {
                assert_eq!(
                    d.reason,
                    super::DISCONNECT_NOT_AUTHORIZED,
                    "failed re-auth disconnects"
                );
            }
            other => panic!("expected DISCONNECT, got {other:?}"),
        }
    }

    /// Re-authenticating with a different method than connect is a protocol error
    /// (DISCONNECT 0x82) — the method must not change [MQTT-4.12.1-1].
    #[tokio::test]
    async fn v5_reauthentication_method_change_is_protocol_error() {
        let (mut reader, mut writer) = enhanced_conn();
        connect_and_authenticate(&mut reader, &mut writer).await;

        writer
            .send(&Packet::Auth(Auth {
                reason: 0x19,
                properties: Properties(vec![Property::AuthenticationMethod("SCRAM-SHA-1".into())]),
            }))
            .await
            .unwrap();
        match recv(&mut reader).await {
            Some(Packet::Disconnect(d)) => {
                assert_eq!(
                    d.reason,
                    super::DISCONNECT_PROTOCOL_ERROR,
                    "method must not change"
                );
            }
            other => panic!("expected DISCONNECT, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_client_id_with_persistent_session_is_rejected() {
        let (mut reader, mut writer, _hub_rx) = start_conn();
        writer.send(&connect_packet("", false)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(ConnAck {
                session_present,
                code,
                ..
            })) => {
                assert_eq!(code, 0x02, "identifier rejected");
                assert!(!session_present);
            }
            other => panic!("expected CONNACK 0x02, got {other:?}"),
        }
        assert_eq!(recv(&mut reader).await, None, "connection must close");
    }

    #[tokio::test]
    async fn empty_client_id_with_clean_session_gets_auto_id() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let seen = stub_hub(hub_rx);
        writer.send(&connect_packet("", true)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(ConnAck { code: 0, .. })) => {}
            other => panic!("expected CONNACK 0x00, got {other:?}"),
        }
        let ids = seen.lock().unwrap().clone();
        assert_eq!(ids.len(), 1);
        assert!(
            ids[0].starts_with("auto-"),
            "server must assign an id, got {:?}",
            ids[0]
        );
    }

    #[tokio::test]
    async fn pingreq_and_qos2_release_are_answered() {
        let (mut reader, mut writer, hub_rx) = start_conn();
        let _seen = stub_hub(hub_rx);
        writer.send(&connect_packet("k1", true)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

        writer.send(&Packet::PingReq).await.unwrap();
        assert_eq!(recv(&mut reader).await, Some(Packet::PingResp));

        writer.send(&Packet::PubRel(7.into())).await.unwrap();
        assert_eq!(recv(&mut reader).await, Some(Packet::PubComp(7.into())));
    }

    /// [MQTT-3.3.2-2]: a PUBLISH topic must not contain wildcards. Such a
    /// packet is a protocol violation — the broker closes the connection and
    /// never forwards it to the hub.
    #[tokio::test]
    async fn wildcard_publish_topic_closes_connection() {
        for bad in ["a/+/b", "a/#", "#", "+"] {
            let (mut reader, mut writer, hub_rx) = start_conn();
            let _seen = stub_hub(hub_rx);
            writer.send(&connect_packet("w", true)).await.unwrap();
            assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));

            writer
                .send(&Packet::Publish(Publish {
                    properties: mqtt_codec::Properties::new(),
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic: bad.to_string(),
                    pkid: None,
                    payload: bytes::Bytes::from_static(b"x"),
                }))
                .await
                .unwrap();

            // The check runs before any forward, so closing the connection
            // also guarantees the publish never reached routing.
            assert_eq!(
                recv(&mut reader).await,
                None,
                "a wildcard PUBLISH topic ({bad:?}) must close the connection"
            );
        }
    }
}
