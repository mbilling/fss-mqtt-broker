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
use crate::hub::{HubCommand, Outbound};
use bytes::Bytes;
use mqtt_auth::{
    basic::BasicAuthenticator, AllowAll, AuthStep, Authenticator, Authorizer, Credentials,
    EnhancedAuthenticator, Identity,
};
use mqtt_cluster::placement::Placement;
use mqtt_cluster::NodeId;
use mqtt_codec::{
    packet::{Auth, ConnAck, Connect, Publish, SubAck},
    Packet, ProtocolVersion, QoS,
};
use mqtt_core::{is_shared_filter, parse_shared, ClientId, Message};
use mqtt_net::{FrameReader, FrameWriter, NetError};
use mqtt_observability::{AuditLog, AuditSink};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

/// Keepalive grace factor: the spec allows one and a half keepalive periods.
const KEEPALIVE_GRACE_NUM: u64 = 3;
const KEEPALIVE_GRACE_DEN: u64 = 2;

/// CONNACK reason: unacceptable protocol version (MQTT 3.1.1 return code 1).
const CONNACK_UNACCEPTABLE_PROTOCOL: u8 = 0x01;
/// CONNACK reason: identifier rejected (MQTT 3.1.1 return code 2).
const CONNACK_IDENTIFIER_REJECTED: u8 = 0x02;
/// CONNACK reason: bad user name or password (MQTT 3.1.1 return code 4).
const CONNACK_BAD_CREDENTIALS: u8 = 0x04;
/// CONNACK reason: not authorized (MQTT 3.1.1 return code 5).
const CONNACK_NOT_AUTHORIZED: u8 = 0x05;
/// SUBACK return code: failure (subscription refused) [MQTT-3.9.3].
const SUBACK_FAILURE: u8 = 0x80;
/// AUTH reason code: continue the enhanced-authentication exchange (ADR 0013).
const AUTH_CONTINUE: u8 = 0x18;
/// CONNACK reason (v5): not authorized.
const CONNACK_V5_NOT_AUTHORIZED: u8 = 0x87;
/// CONNACK reason (v5): the requested authentication method is not supported.
const CONNACK_V5_BAD_AUTH_METHOD: u8 = 0x8C;
/// Topic Alias Maximum advertised to v5 clients (ADR 0011 §2): the highest inbound
/// topic alias the server will accept on a connection.
const SERVER_TOPIC_ALIAS_MAX: u16 = 16;
/// Receive Maximum advertised to v5 clients (ADR 0012 §3): the most unacked
/// `QoS` > 0 publishes the server invites the client to have outstanding to it.
const SERVER_RECEIVE_MAXIMUM: u16 = 256;

/// Monotonic source of unique connection ids (distinct from client ids).
static CONN_ID: AtomicU64 = AtomicU64::new(1);
/// Counter for server-assigned client ids (empty-id clients).
static AUTO_ID: AtomicU64 = AtomicU64::new(1);

/// Extract the mTLS identity (ADR 0004) from an accepted server-side TLS
/// stream: the chain-verified leaf certificate's Subject Common Name.
///
/// Returns `None` when no client certificate was presented, or when a verified
/// certificate carries no usable CN (logged — such a client can only proceed
/// as anonymous, which the default policy denies).
pub fn tls_identity<S>(tls: &tokio_rustls::server::TlsStream<S>) -> Option<Identity> {
    let leaf = tls.get_ref().1.peer_certificates()?.first()?;
    match mqtt_auth::mtls::identity_from_cert(leaf) {
        Ok(identity) => Some(identity),
        Err(e) => {
            warn!(error = %e, "client certificate verified but has no usable Common Name");
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
    pub connector: Option<TlsConnector>,
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
    /// Authenticates the CONNECT credentials.
    pub auth: Arc<dyn Authenticator>,
    /// Optional MQTT 5.0 enhanced-authentication mechanism (ADR 0013): runs the
    /// SASL-style AUTH exchange when a CONNECT names its method. `None` disables it.
    pub enhanced: Option<Arc<dyn EnhancedAuthenticator>>,
    /// Authorizes publish/subscribe topics.
    pub authz: Arc<dyn Authorizer>,
    /// Records auth and authorization decisions.
    pub audit: Arc<dyn AuditSink>,
    /// Session relocation context; `None` outside a cluster (serve locally).
    pub proxy: Option<ProxyContext>,
    /// The session store, shared with the hub, backing the **durable** QoS-2 inbound
    /// dedup window (ADR 0007 §5): `record_received` quorum-replicates the packet id
    /// before PUBREC, so exactly-once survives a failover. `None` falls back to a
    /// per-connection in-memory window (lost on disconnect — fine for clean sessions
    /// and the in-memory backend).
    pub store: Option<Arc<dyn mqtt_storage::SessionStore>>,
}

impl std::fmt::Debug for ConnPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnPolicy").finish_non_exhaustive()
    }
}

/// Drive one accepted plaintext TCP connection to completion, logging any error.
///
/// Test-only convenience path: anonymous clients are permitted, no transport
/// identity is attached, and authorization is open. Production listeners go
/// through [`handle_stream`] with the operator-configured [`ConnPolicy`].
pub async fn handle(stream: TcpStream, hub: mpsc::UnboundedSender<HubCommand>) {
    let peer = stream.peer_addr().ok();
    let policy = Arc::new(ConnPolicy {
        auth: Arc::new(BasicAuthenticator {
            allow_anonymous: true,
        }),
        authz: Arc::new(AllowAll),
        audit: Arc::new(AuditLog::new()),
        proxy: None,
        store: None,
        enhanced: None,
    });
    handle_stream(stream, peer, None, policy, hub).await;
}

/// Drive one accepted connection over any transport (TCP, TLS) to completion,
/// logging any error. `peer` is the remote address, for diagnostics only.
/// `identity` is the TLS-verified mTLS identity, `None` on plaintext or
/// no-client-cert connections; `policy` decides authentication, authorization,
/// and auditing.
pub async fn handle_stream<S>(
    stream: S,
    peer: Option<SocketAddr>,
    identity: Option<Identity>,
    policy: Arc<ConnPolicy>,
    hub: mpsc::UnboundedSender<HubCommand>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Err(e) = run(stream, identity, &policy, hub).await {
        warn!(?peer, error = %e, "connection ended with error");
    }
}

async fn run<S>(
    stream: S,
    identity: Option<Identity>,
    policy: &ConnPolicy,
    hub: mpsc::UnboundedSender<HubCommand>,
) -> Result<(), NetError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (rh, wh) = tokio::io::split(stream);
    let reader = FrameReader::new(rh, ProtocolVersion::V311);
    let writer = FrameWriter::new(wh, ProtocolVersion::V311);
    // A directly-accepted client may be relocated to its placement owner; it has no
    // relaying node (`via = None`).
    run_framed(reader, writer, identity, policy, hub, true, None).await
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
    if policy.authz.authorize_publish(principal, &w.topic) {
        return Ok(false);
    }
    warn!(client = %client.0, topic = %w.topic, "CONNECT rejected: will topic not authorized");
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

async fn run_framed<R, W>(
    mut reader: FrameReader<R>,
    mut writer: FrameWriter<W>,
    identity: Option<Identity>,
    policy: &ConnPolicy,
    hub: mpsc::UnboundedSender<HubCommand>,
    allow_proxy: bool,
    via: Option<String>,
) -> Result<(), NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // Protocol requires CONNECT as the first packet on a connection.
    let connect = match reader.next_packet().await? {
        Some(Packet::Connect(c)) => c,
        Some(other) => {
            warn!(packet = ?other.packet_type(), "first packet was not CONNECT; closing");
            return Ok(());
        }
        None => return Ok(()),
    };

    // Negotiate the version the CONNECT declared; every later packet and the CONNACK
    // is framed at it (a no-op for the v3.1.1 readers/writers created above).
    reader.set_version(connect.protocol);
    writer.set_version(connect.protocol);

    // Client-id validation may already reject the CONNECT.
    let Some(client) = validate_connect(&mut writer, &connect).await? else {
        return Ok(());
    };

    // Authentication gate: verify credentials BEFORE attaching to the hub, so a
    // rejected client never touches session state (enhanced exchange or single-shot).
    let Some(principal) = authenticate(
        &mut reader,
        &mut writer,
        &client,
        &connect,
        identity.as_ref(),
        policy,
        via,
    )
    .await?
    else {
        return Ok(()); // rejected; CONNACK/close already handled
    };

    // The version-agnostic session policy (ADR 0009): whether to start clean, and how
    // long the session is retained after disconnect.
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
    let will = connect.last_will.map(|w| Message {
        topic: w.topic,
        payload: w.payload,
        qos: w.qos,
        retain: w.retain,
    });
    let (out_tx, mut out_rx): (Outbound, _) = mpsc::unbounded_channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    // The client's Receive Maximum bounds how many unacked QoS>0 PUBLISHes the hub
    // may have outstanding to it (ADR 0012); 0/absent means unlimited.
    let receive_maximum = client_receive_maximum(connect.protocol, &connect.properties);
    // Attach before sending CONNACK so we cannot miss a publish that races in, and
    // so the hub can tell us whether a session was already present.
    if hub
        .send(HubCommand::Attach {
            client: client.clone(),
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
    let Ok(session_present) = reply_rx.await else {
        return Ok(()); // hub dropped the reply
    };
    // Build the v5 CONNACK properties (Topic Alias Maximum, Receive Maximum) and the
    // per-connection alias maps (ADR 0011, ADR 0012).
    let (connack_props, mut inbound_aliases, mut outbound_aliases) =
        negotiate_v5_properties(connect.protocol, &connect.properties);
    writer
        .send(&Packet::ConnAck(ConnAck {
            properties: connack_props,
            session_present,
            code: 0,
        }))
        .await?;
    debug!(client = %client.0, session_present, "CONNECT accepted");

    let result = serve(
        &mut reader,
        &mut writer,
        &hub,
        &client,
        &principal,
        policy,
        &mut out_rx,
        connect.keep_alive,
        &mut inbound_aliases,
        &mut outbound_aliases,
    )
    .await;
    // Always deregister, even on error. The hub ignores this if we were taken
    // over. Only a clean DISCONNECT is graceful; anything else fires the will.
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

    if let Some(connector) = &proxy.connector {
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
    // relaying node, recorded in the auth audit.
    if let Err(e) = run_framed(reader, writer, identity, &policy, hub, false, via).await {
        warn!(?peer, error = %e, "proxied session ended with error");
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
async fn validate_connect<W>(
    writer: &mut FrameWriter<W>,
    connect: &Connect,
) -> Result<Option<ClientId>, NetError>
where
    W: AsyncWrite + Unpin,
{
    if connect.client_id.is_empty() {
        if !connect.clean_session {
            writer
                .send(&Packet::ConnAck(ConnAck {
                    properties: mqtt_codec::Properties::new(),
                    session_present: false,
                    code: connack_code(CONNACK_IDENTIFIER_REJECTED, connect.protocol),
                }))
                .await?;
            return Ok(None);
        }
        return Ok(Some(ClientId(format!(
            "auto-{}",
            AUTO_ID.fetch_add(1, Ordering::Relaxed)
        ))));
    }
    Ok(Some(ClientId(connect.client_id.clone())))
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
) -> Result<Option<Identity>, NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if let Some(method) = connect.properties.authentication_method() {
        enhanced_auth(reader, writer, client, connect, method, policy).await
    } else {
        authenticate_connect(writer, client, connect, identity, policy, via).await
    }
}

/// Authenticate the CONNECT against the listener policy. Credentials priority:
/// a TLS-verified certificate identity wins; otherwise CONNECT
/// username/password; otherwise anonymous (only honored when the policy opts
/// in). On failure this sends the rejecting CONNACK — 0x04 (bad user name or
/// password) for password credentials, 0x05 (not authorized) otherwise — and
/// returns `Ok(None)`: the caller must close without attaching to the hub.
async fn authenticate_connect<W>(
    writer: &mut FrameWriter<W>,
    client: &ClientId,
    connect: &Connect,
    identity: Option<&Identity>,
    policy: &ConnPolicy,
    via: Option<String>,
) -> Result<Option<Identity>, NetError>
where
    W: AsyncWrite + Unpin,
{
    let creds = match (identity, &connect.username) {
        (Some(id), _) => Credentials::ClientCert {
            subject: &id.subject,
        },
        (None, Some(username)) => Credentials::Password {
            username,
            password: connect.password.as_deref().unwrap_or(&[]),
        },
        (None, None) => Credentials::Anonymous,
    };
    let method = match creds {
        Credentials::ClientCert { .. } => "certificate",
        Credentials::Password { .. } => "password",
        Credentials::Token(_) => "token",
        Credentials::Anonymous => "anonymous",
    };
    match policy.auth.authenticate(client, &creds) {
        Ok(id) => {
            // For a relocated session, attribute it to the node that vouched (ADR
            // 0005); a direct client has no `via`.
            let relayed = via.map_or_else(String::new, |node| format!(" (relayed by node {node})"));
            policy.audit.record(
                "auth.success",
                Some(&id.subject),
                &format!("client {} via {method}{relayed}", client.0),
            );
            Ok(Some(id))
        }
        Err(e) => {
            let code = if matches!(creds, Credentials::Password { .. }) {
                CONNACK_BAD_CREDENTIALS
            } else {
                CONNACK_NOT_AUTHORIZED
            };
            warn!(client = %client.0, error = %e, "CONNECT rejected: authentication failed");
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
    let mut step = session.step(
        client,
        connect.properties.authentication_data().unwrap_or_default(),
    );
    loop {
        match step {
            AuthStep::Success(id) => {
                policy.audit.record(
                    "auth.success",
                    Some(&id.subject),
                    &format!("client {} via enhanced:{method}", client.0),
                );
                return Ok(Some(id));
            }
            AuthStep::Failure => {
                warn!(client = %client.0, method, "enhanced authentication failed");
                policy.audit.record(
                    "auth.failure",
                    Some(&client.0),
                    &format!("rejected enhanced:{method}"),
                );
                reject_connack(writer, CONNACK_V5_NOT_AUTHORIZED).await?;
                return Ok(None);
            }
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

                // The reply must be an AUTH(Continue) keeping the same method.
                let reply = match reader.next_packet().await? {
                    Some(Packet::Auth(a)) => a,
                    Some(other) => {
                        warn!(client = %client.0, packet = ?other.packet_type(),
                              "expected AUTH during enhanced auth; closing");
                        return Ok(None);
                    }
                    None => return Ok(None), // EOF mid-exchange
                };
                if reply.reason != AUTH_CONTINUE {
                    warn!(client = %client.0, reason = reply.reason, "unexpected AUTH reason; closing");
                    return Ok(None);
                }
                if reply.properties.authentication_method() != Some(method) {
                    warn!(client = %client.0, "AUTH method changed mid-exchange; closing");
                    reject_connack(writer, CONNACK_V5_BAD_AUTH_METHOD).await?;
                    return Ok(None);
                }
                step = session.step(
                    client,
                    reply.properties.authentication_data().unwrap_or_default(),
                );
            }
        }
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

/// Build the v5 CONNACK property block — Topic Alias Maximum (ADR 0011) and Receive
/// Maximum (ADR 0012) — and the per-connection topic-alias maps. v3.1.1 has neither
/// feature, so the maps come out disabled and the property block empty.
fn negotiate_v5_properties(
    protocol: ProtocolVersion,
    properties: &mqtt_codec::Properties,
) -> (mqtt_codec::Properties, InboundAliases, OutboundAliases) {
    let is_v5 = protocol == ProtocolVersion::V5;
    let server_alias_max = if is_v5 { SERVER_TOPIC_ALIAS_MAX } else { 0 };
    let client_alias_max = if is_v5 {
        properties.topic_alias_maximum().unwrap_or(0)
    } else {
        0
    };
    let mut props = mqtt_codec::Properties::new();
    if is_v5 {
        props
            .0
            .push(mqtt_codec::Property::ReceiveMaximum(SERVER_RECEIVE_MAXIMUM));
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
    principal: &Identity,
    policy: &ConnPolicy,
    out_rx: &mut mpsc::UnboundedReceiver<Packet>,
    keep_alive: u16,
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
    // Inbound QoS 2 packet ids seen but not yet released (PUBREL); forwarding
    // only on first sight is what makes inbound QoS 2 exactly-once
    // [MQTT-4.3.3-2].
    let mut qos2_inbound: HashSet<u16> = HashSet::new();

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
                    Some(packet) => {
                        if handle_inbound(packet, writer, hub, client, principal, policy, &mut qos2_inbound, inbound_aliases).await? {
                            return Ok(true); // client sent DISCONNECT
                        }
                    }
                }
            }
            maybe_out = out_rx.recv() => {
                match maybe_out {
                    // Rewrite outbound PUBLISHes to use topic aliases where the
                    // client allowed them (ADR 0011 §3); other packets pass through.
                    Some(mut pkt) => {
                        if let Packet::Publish(p) = &mut pkt {
                            outbound_aliases.apply(p);
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
                return Ok(false);
            }
        }
    }
}

/// Handle one inbound PUBLISH: topic validation, ACL gate, inbound `QoS`
/// handshakes, and the exactly-once dedup window. Returns `Ok(true)` if the
/// connection must close (a protocol violation).
#[allow(clippy::too_many_arguments)] // a connection's full publish-handling context
async fn handle_publish<W: AsyncWrite + Unpin>(
    publish: Publish,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
    principal: &Identity,
    policy: &ConnPolicy,
    qos2_inbound: &mut HashSet<u16>,
    inbound_aliases: &mut InboundAliases,
) -> Result<bool, NetError> {
    // The MQTT 5.0 Message Expiry Interval (if the publisher set one) bounds how long
    // a queued copy is deliverable (ADR 0009 §3).
    let message_expiry = publish.properties.message_expiry_interval();
    // Resolve any topic alias to the full topic name before anything else sees it
    // (ADR 0011 §2). An invalid alias is a protocol violation: close the connection.
    let alias = publish.properties.topic_alias();
    let Ok(topic) = inbound_aliases.resolve(&publish.topic, alias) else {
        warn!(client = %client.0, alias = ?alias, "invalid topic alias; closing connection");
        return Ok(true);
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
        return Ok(true);
    }
    // ACL gate (ADR 0004 step 3): an unauthorized publish is dropped but still
    // acknowledged — 3.1.1 has no negative PUBACK, and not acking would leave
    // conforming publishers retrying forever.
    let authorized = policy.authz.authorize_publish(principal, &topic);
    if !authorized {
        debug!(client = %client.0, identity = %principal.subject, topic = %topic,
               "publish denied by ACL; dropping");
        policy
            .audit
            .record("acl.deny.publish", Some(&principal.subject), &topic);
    }
    let forward = |hub: &mpsc::UnboundedSender<HubCommand>| {
        if authorized {
            let _ = hub.send(HubCommand::Publish {
                topic,
                payload,
                qos,
                retain,
                message_expiry,
            });
        }
    };
    match (qos, pkid) {
        (QoS::AtMostOnce, _) => forward(hub),
        (QoS::AtLeastOnce, Some(id)) => {
            forward(hub);
            writer.send(&Packet::PubAck(id.into())).await?;
        }
        (QoS::ExactlyOnce, Some(id)) => {
            // Exactly-once inbound [MQTT-4.3.3-2]: forward only the first
            // sighting of this packet id; re-sent copies (DUP) before the
            // PUBREL release are acknowledged but not re-delivered. The dedup
            // window is the durable session store when present (so it survives a
            // failover), else a per-connection set. A store error degrades to
            // forwarding (at-least-once) rather than dropping the flow.
            let first_sighting = match &policy.store {
                Some(store) => store.record_received(client, id).await.unwrap_or(true),
                None => qos2_inbound.insert(id),
            };
            if first_sighting {
                forward(hub);
            }
            writer.send(&Packet::PubRec(id.into())).await?;
        }
        _ => debug!(client = %client.0, "dropping QoS>0 publish without packet id"),
    }
    Ok(false)
}

/// Handle one inbound packet. Returns `Ok(true)` if the connection should close.
#[allow(clippy::too_many_arguments)] // a connection's full inbound-handling context
async fn handle_inbound<W: AsyncWrite + Unpin>(
    packet: Packet,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
    principal: &Identity,
    policy: &ConnPolicy,
    qos2_inbound: &mut HashSet<u16>,
    inbound_aliases: &mut InboundAliases,
) -> Result<bool, NetError> {
    match packet {
        Packet::Publish(publish) => {
            // A wildcard topic is a protocol violation: close the connection.
            if handle_publish(
                publish,
                writer,
                hub,
                client,
                principal,
                policy,
                qos2_inbound,
                inbound_aliases,
            )
            .await?
            {
                return Ok(true);
            }
        }
        // QoS 2 publisher-side release: the id may be reused afterwards. (A v5
        // reason code on these acks is not acted on yet — workstream G.)
        Packet::PubRel(ack) => {
            let id = ack.pkid;
            match &policy.store {
                Some(store) => {
                    let _ = store.clear_received(client, id).await;
                }
                None => {
                    qos2_inbound.remove(&id);
                }
            }
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
            // ACL gate per filter (ADR 0004 step 3): denied filters answer
            // 0x80 [MQTT-3.9.3] and never reach the hub; granted filters get
            // the requested QoS [MQTT-3.8.4-5/6].
            let mut granted: Vec<(String, QoS)> = Vec::new();
            let mut return_codes: Vec<u8> = Vec::with_capacity(s.filters.len());
            for f in &s.filters {
                // A malformed `$share/...` filter (bad share name / empty filter) is
                // rejected outright (ADR 0010 §1) before the ACL even sees it.
                if is_shared_filter(&f.path) && parse_shared(&f.path).is_none() {
                    debug!(client = %client.0, filter = %f.path, "malformed shared subscription");
                    return_codes.push(SUBACK_FAILURE);
                } else if policy.authz.authorize_subscribe(principal, &f.path) {
                    granted.push((f.path.clone(), f.qos));
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
                let _ = hub.send(HubCommand::Subscribe {
                    client: client.clone(),
                    filters: granted,
                });
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
        Packet::Disconnect(_) => return Ok(true),
        other => debug!(packet = ?other.packet_type(), "ignoring unexpected packet"),
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::{handle_stream, ConnPolicy, SERVER_RECEIVE_MAXIMUM, SERVER_TOPIC_ALIAS_MAX};
    use crate::hub::{HubCommand, Outbound};
    use bytes::Bytes;
    use mqtt_auth::basic::BasicAuthenticator;
    use mqtt_codec::{
        packet::{Auth, ConnAck, Connect, Disconnect, Publish, SubAck, Subscribe, SubscribeFilter},
        Packet, Properties, Property, ProtocolVersion, QoS,
    };
    use mqtt_core::ClientId;
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
            auth: Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            }),
            authz: Arc::new(mqtt_auth::AllowAll),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            store: None,
            enhanced: None,
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

    /// Minimal hub stub: accepts every Attach with `session_present = false`,
    /// records the client ids it sees, and keeps outbound senders alive so the
    /// connection's writer loop stays up.
    fn stub_hub(mut hub_rx: mpsc::UnboundedReceiver<HubCommand>) -> Arc<Mutex<Vec<String>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = seen.clone();
        tokio::spawn(async move {
            let mut keep_alive = Vec::new();
            while let Some(cmd) = hub_rx.recv().await {
                if let HubCommand::Attach {
                    client,
                    outbound,
                    reply,
                    ..
                } = cmd
                {
                    record.lock().unwrap().push(client.0.clone());
                    keep_alive.push(outbound);
                    let _ = reply.send(false);
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
            auth: Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            }),
            enhanced: Some(Arc::new(mqtt_auth::HmacChallengeAuthenticator::new(
                secrets,
            ))),
            authz: Arc::new(mqtt_auth::AllowAll),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            store: None,
        });
        tokio::spawn(handle_stream(server, None, None, policy, hub_tx));
        let (rh, wh) = tokio::io::split(client);
        (FrameReader::new(rh, V5), FrameWriter::new(wh, V5))
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
                        let _ = reply.send(false);
                    }
                    HubCommand::Publish { topic, .. } => {
                        let _ = tx.send(topic);
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
                    let _ = reply.send(false);
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
                    let _ = reply.send(false);
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
            auth: Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            }),
            authz: Arc::new(mqtt_auth::AllowAll),
            audit: audit.clone(),
            proxy: None,
            store: None,
            enhanced: None,
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
    /// (with options + a subscription identifier) is answered with a v5 SUBACK, and a
    /// v5 DISCONNECT closes the session — the whole v5 path negotiated end to end.
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
                properties: Properties(vec![Property::SubscriptionIdentifier(5)]),
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
                Some(SERVER_TOPIC_ALIAS_MAX),
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

    /// Referencing a topic alias that was never established is a protocol error and
    /// closes the connection (ADR 0011 §2).
    #[tokio::test]
    async fn v5_invalid_topic_alias_closes_connection() {
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
        assert_eq!(
            recv(&mut reader).await,
            None,
            "an unmapped alias reference closes the connection"
        );
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

        out.send(server_publish("room/temp")).unwrap();
        match recv(&mut reader).await {
            Some(Packet::Publish(p)) => {
                assert_eq!(p.topic, "room/temp", "first send keeps the full topic");
                assert_eq!(p.properties.topic_alias(), Some(1));
            }
            other => panic!("expected PUBLISH, got {other:?}"),
        }

        out.send(server_publish("room/temp")).unwrap();
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
                Some(SERVER_RECEIVE_MAXIMUM),
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

        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"alice-secret");
        let proof = ring::hmac::sign(&key, &nonce);
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
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"guessed");
        let proof = ring::hmac::sign(&key, b"any-nonce");
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

    /// Start a connection whose policy backs the QoS-2 dedup window with `store`.
    fn start_conn_with_store(
        store: Arc<dyn mqtt_storage::SessionStore>,
    ) -> (Reader, Writer, mpsc::UnboundedReceiver<HubCommand>) {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        let policy = Arc::new(ConnPolicy {
            auth: Arc::new(BasicAuthenticator {
                allow_anonymous: true,
            }),
            authz: Arc::new(mqtt_auth::AllowAll),
            audit: Arc::new(mqtt_observability::AuditLog::new()),
            proxy: None,
            store: Some(store),
            enhanced: None,
        });
        tokio::spawn(handle_stream(server, None, None, policy, hub_tx));
        let (rh, wh) = tokio::io::split(client);
        (FrameReader::new(rh, V4), FrameWriter::new(wh, V4), hub_rx)
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

    /// When the policy carries a session store, the QoS-2 inbound dedup window lives
    /// in the **store** (not a per-connection set), so it is durable: the packet id
    /// is recorded on PUBLISH (before PUBREC) and cleared on PUBREL.
    #[tokio::test]
    async fn qos2_dedup_window_is_backed_by_the_store() {
        let store: Arc<dyn mqtt_storage::SessionStore> =
            Arc::new(mqtt_storage::MemorySessionStore::new());
        let (mut reader, mut writer, hub_rx) = start_conn_with_store(store.clone());
        let _seen = stub_hub(hub_rx);

        writer.send(&connect_packet("c1", false)).await.unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
        let client = ClientId("c1".to_string());

        // First QoS-2 PUBLISH: recorded in the store before PUBREC.
        writer.send(&qos2_publish(5)).await.unwrap();
        assert_eq!(recv(&mut reader).await, Some(Packet::PubRec(5.into())));
        assert_eq!(store.received(&client).await.unwrap(), vec![5]);

        // A duplicate (same id) is still acknowledged; the window is unchanged.
        writer.send(&qos2_publish(5)).await.unwrap();
        assert_eq!(recv(&mut reader).await, Some(Packet::PubRec(5.into())));
        assert_eq!(store.received(&client).await.unwrap(), vec![5]);

        // PUBREL completes the flow and clears the id from the (durable) window.
        writer.send(&Packet::PubRel(5.into())).await.unwrap();
        assert_eq!(recv(&mut reader).await, Some(Packet::PubComp(5.into())));
        assert!(store.received(&client).await.unwrap().is_empty());
    }
}
