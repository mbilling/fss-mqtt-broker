//! Per-connection task: CONNECT handshake, then a select loop multiplexing
//! inbound client packets, outbound packets delivered by the hub, and the
//! keepalive deadline.
//!
//! Keepalive [MQTT-3.1.2-24]: with a non-zero keepalive, the server closes the
//! connection if nothing arrives from the client within 1.5x the interval; the
//! deadline resets on *inbound* traffic only (outbound deliveries must not keep
//! a dead client alive). An ungraceful end — EOF, error, keepalive expiry —
//! publishes the client's will; a clean DISCONNECT discards it.

use crate::hub::{HubCommand, Outbound};
use mqtt_auth::{
    basic::BasicAuthenticator, AllowAll, Authenticator, Authorizer, Credentials, Identity,
};
use mqtt_cluster::placement::Placement;
use mqtt_codec::{
    packet::{ConnAck, Connect, Publish, SubAck},
    Packet, ProtocolVersion, QoS,
};
use mqtt_core::{ClientId, Message};
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
    /// The live session-placement ring.
    pub placement: Arc<RwLock<Placement>>,
    /// mTLS connector for dialing the owner's peer listener; `None` = plaintext.
    pub connector: Option<TlsConnector>,
}

impl std::fmt::Debug for ProxyContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyContext")
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
    /// Authorizes publish/subscribe topics.
    pub authz: Arc<dyn Authorizer>,
    /// Records auth and authorization decisions.
    pub audit: Arc<dyn AuditSink>,
    /// Session relocation context; `None` outside a cluster (serve locally).
    pub proxy: Option<ProxyContext>,
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
    // A directly-accepted client may be relocated to its placement owner.
    run_framed(reader, writer, identity, policy, hub, true).await
}

/// Serve an MQTT connection over already-framed halves. `allow_proxy` is `true`
/// for a directly-accepted client (which may be relocated to its owner,
/// ADR 0005) and `false` for a session already proxied here (it is served
/// locally — this node is the owner; re-proxying would loop).
async fn run_framed<R, W>(
    mut reader: FrameReader<R>,
    mut writer: FrameWriter<W>,
    identity: Option<Identity>,
    policy: &ConnPolicy,
    hub: mpsc::UnboundedSender<HubCommand>,
    allow_proxy: bool,
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

    // Protocol-version and client-id validation may already reject the CONNECT.
    let Some(client) = validate_connect(&mut writer, &connect).await? else {
        return Ok(());
    };

    // Authentication gate: verify credentials BEFORE attaching to the hub, so
    // a rejected client never touches session state.
    let Some(principal) =
        authenticate_connect(&mut writer, &client, &connect, identity.as_ref(), policy).await?
    else {
        return Ok(()); // rejected; the CONNACK was already sent
    };

    // Session affinity (ADR 0005): a persistent session whose placement owner is
    // another node is relocated there. The owner serves it (CONNACK onward);
    // this node only relays. Clean sessions and owner-is-self stay local.
    if allow_proxy && !connect.clean_session {
        if let Some(proxy) = &policy.proxy {
            let route = proxy
                .placement
                .read()
                .ok()
                .and_then(|p| p.owner_route(&client.0));
            if let Some((owner, addr)) = route {
                info!(client = %client.0, owner = %owner.0, "relocating persistent session to its owner (ADR 0005)");
                return proxy_to_owner(reader, writer, &connect, &principal, proxy, &addr).await;
            }
        }
    }

    // A will is a deferred publish: it must be authorized at CONNECT, not at
    // the moment of death (ADR 0004 step 3).
    if let Some(w) = &connect.last_will {
        if !policy.authz.authorize_publish(&principal, &w.topic) {
            warn!(client = %client.0, topic = %w.topic, "CONNECT rejected: will topic not authorized");
            policy.audit.record(
                "acl.deny.will",
                Some(&principal.subject),
                &format!("will topic {}", w.topic),
            );
            writer
                .send(&Packet::ConnAck(ConnAck {
                    session_present: false,
                    code: CONNACK_NOT_AUTHORIZED,
                }))
                .await?;
            return Ok(());
        }
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
    // Attach before sending CONNACK so we cannot miss a publish that races in, and
    // so the hub can tell us whether a session was already present.
    if hub
        .send(HubCommand::Attach {
            client: client.clone(),
            conn_id,
            clean_session: connect.clean_session,
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
    writer
        .send(&Packet::ConnAck(ConnAck {
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
        },
        &mut prelude,
    )
    .map_err(|e| NetError::Io(std::io::Error::other(e.to_string())))?;
    Packet::Connect(connect.clone()).encode(&mut prelude, ProtocolVersion::V311)?;
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

/// Write `prelude` to the owner, then relay the client stream and the owner
/// stream in both directions until either side closes.
#[allow(clippy::similar_names)] // client_rh/client_wh and owner_rh/owner_wh are clear half names
async fn splice<R, W, O>(
    mut client_rh: R,
    mut client_wh: W,
    prelude: Vec<u8>,
    owner: O,
) -> Result<(), NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    O: AsyncRead + AsyncWrite + Unpin,
{
    let (mut owner_rh, mut owner_wh) = tokio::io::split(owner);
    owner_wh.write_all(&prelude).await?;
    owner_wh.flush().await?;
    // Either direction reaching EOF ends the session; dropping the halves on
    // return closes both connections.
    tokio::select! {
        _ = tokio::io::copy(&mut client_rh, &mut owner_wh) => {}
        _ = tokio::io::copy(&mut owner_rh, &mut client_wh) => {}
    }
    Ok(())
}

/// Serve a session proxied to this node by another (ADR 0005): this node is the
/// session's owner. `prefix` holds the client's MQTT bytes already read past the
/// [`PeerMessage::ProxyHello`] marker; `identity` is the vouched, already-
/// authenticated client identity. The session is served locally and never
/// re-proxied.
#[allow(clippy::similar_names)] // client_rh/client_wh are clear half names
pub async fn serve_proxied<R, W>(
    client_rh: R,
    client_wh: W,
    peer: Option<SocketAddr>,
    identity: Option<Identity>,
    policy: Arc<ConnPolicy>,
    hub: mpsc::UnboundedSender<HubCommand>,
    prefix: bytes::BytesMut,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let reader = FrameReader::with_buffer(client_rh, ProtocolVersion::V311, prefix);
    let writer = FrameWriter::new(client_wh, ProtocolVersion::V311);
    if let Err(e) = run_framed(reader, writer, identity, &policy, hub, false).await {
        warn!(?peer, error = %e, "proxied session ended with error");
    }
}

/// Validate the protocol version and client id of a CONNECT, replying with the
/// rejecting CONNACK (0x01 / 0x02) and returning `None` when it must close.
/// An empty client id is only valid with clean session (the server assigns an
/// id); pairing it with a persistent session is rejected per spec.
async fn validate_connect<W>(
    writer: &mut FrameWriter<W>,
    connect: &Connect,
) -> Result<Option<ClientId>, NetError>
where
    W: AsyncWrite + Unpin,
{
    // This milestone speaks only MQTT 3.1.1.
    if connect.protocol != ProtocolVersion::V311 {
        writer
            .send(&Packet::ConnAck(ConnAck {
                session_present: false,
                code: CONNACK_UNACCEPTABLE_PROTOCOL,
            }))
            .await?;
        return Ok(None);
    }
    if connect.client_id.is_empty() {
        if !connect.clean_session {
            writer
                .send(&Packet::ConnAck(ConnAck {
                    session_present: false,
                    code: CONNACK_IDENTIFIER_REJECTED,
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
            policy.audit.record(
                "auth.success",
                Some(&id.subject),
                &format!("client {} via {method}", client.0),
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
                    session_present: false,
                    code,
                }))
                .await?;
            Ok(None)
        }
    }
}

/// Serve the connection until it ends. Returns `Ok(true)` only for a clean
/// client DISCONNECT; every other end (EOF, keepalive expiry, takeover) is
/// ungraceful and will publish the client's will.
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
                        if handle_inbound(packet, writer, hub, client, principal, policy, &mut qos2_inbound).await? {
                            return Ok(true); // client sent DISCONNECT
                        }
                    }
                }
            }
            maybe_out = out_rx.recv() => {
                match maybe_out {
                    Some(pkt) => writer.send(&pkt).await?,
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
async fn handle_publish<W: AsyncWrite + Unpin>(
    publish: Publish,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
    principal: &Identity,
    policy: &ConnPolicy,
    qos2_inbound: &mut HashSet<u16>,
) -> Result<bool, NetError> {
    let Publish {
        qos,
        pkid,
        topic,
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
            });
        }
    };
    match (qos, pkid) {
        (QoS::AtMostOnce, _) => forward(hub),
        (QoS::AtLeastOnce, Some(id)) => {
            forward(hub);
            writer.send(&Packet::PubAck(id)).await?;
        }
        (QoS::ExactlyOnce, Some(id)) => {
            // Exactly-once inbound [MQTT-4.3.3-2]: forward only the first
            // sighting of this packet id; re-sent copies (DUP) before the
            // PUBREL release are acknowledged but not re-delivered.
            if qos2_inbound.insert(id) {
                forward(hub);
            }
            writer.send(&Packet::PubRec(id)).await?;
        }
        _ => debug!(client = %client.0, "dropping QoS>0 publish without packet id"),
    }
    Ok(false)
}

/// Handle one inbound packet. Returns `Ok(true)` if the connection should close.
async fn handle_inbound<W: AsyncWrite + Unpin>(
    packet: Packet,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
    principal: &Identity,
    policy: &ConnPolicy,
    qos2_inbound: &mut HashSet<u16>,
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
            )
            .await?
            {
                return Ok(true);
            }
        }
        // QoS 2 publisher-side release: the id may be reused afterwards.
        Packet::PubRel(id) => {
            qos2_inbound.remove(&id);
            writer.send(&Packet::PubComp(id)).await?;
        }
        // Subscriber-side acknowledgements for our downstream deliveries.
        Packet::PubAck(id) => {
            let _ = hub.send(HubCommand::PubAck {
                client: client.clone(),
                pkid: id,
            });
        }
        Packet::PubRec(id) => {
            let _ = hub.send(HubCommand::PubRec {
                client: client.clone(),
                pkid: id,
            });
        }
        Packet::PubComp(id) => {
            let _ = hub.send(HubCommand::PubComp {
                client: client.clone(),
                pkid: id,
            });
        }
        Packet::Subscribe(s) => {
            // ACL gate per filter (ADR 0004 step 3): denied filters answer
            // 0x80 [MQTT-3.9.3] and never reach the hub; granted filters get
            // the requested QoS [MQTT-3.8.4-5/6].
            let mut granted: Vec<(String, QoS)> = Vec::new();
            let mut return_codes: Vec<u8> = Vec::with_capacity(s.filters.len());
            for f in &s.filters {
                if policy.authz.authorize_subscribe(principal, &f.path) {
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
                }))
                .await?;
        }
        Packet::Unsubscribe(u) => {
            let _ = hub.send(HubCommand::Unsubscribe {
                client: client.clone(),
                filters: u.filters.clone(),
            });
            writer.send(&Packet::UnsubAck(u.pkid)).await?;
        }
        Packet::PingReq => writer.send(&Packet::PingResp).await?,
        Packet::Disconnect => return Ok(true),
        other => debug!(packet = ?other.packet_type(), "ignoring unexpected packet"),
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::{handle_stream, ConnPolicy};
    use crate::hub::HubCommand;
    use mqtt_auth::basic::BasicAuthenticator;
    use mqtt_codec::{
        packet::{ConnAck, Connect, Publish},
        Packet, ProtocolVersion, QoS,
    };
    use mqtt_net::{FrameReader, FrameWriter};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    const V4: ProtocolVersion = ProtocolVersion::V311;

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

    fn connect_packet(id: &str, clean_session: bool) -> Packet {
        Packet::Connect(Connect {
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

    #[tokio::test]
    async fn non_connect_first_packet_closes_without_connack() {
        let (mut reader, mut writer, _hub_rx) = start_conn();
        writer.send(&Packet::PingReq).await.unwrap();
        assert_eq!(recv(&mut reader).await, None);
    }

    #[tokio::test]
    async fn unsupported_protocol_version_closes_without_connack() {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, _hub_rx) = mpsc::unbounded_channel();
        tokio::spawn(handle_stream(server, None, None, permissive(), hub_tx));
        let (rh, mut wh) = tokio::io::split(client);

        // A CONNECT claiming protocol level 5: name "MQTT", level 0x05,
        // clean-session flags, keepalive 60, client id "x".
        let frame: &[u8] = &[
            0x10, 0x0D, // CONNECT, remaining length 13
            0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, // var header
            0x00, 0x01, b'x', // client id
        ];
        wh.write_all(frame).await.unwrap();

        let mut reader: Reader = FrameReader::new(rh, V4);
        assert_eq!(
            recv(&mut reader).await,
            None,
            "an unsupported protocol version must never reach CONNACK 0x00"
        );
    }

    #[tokio::test]
    async fn empty_client_id_with_persistent_session_is_rejected() {
        let (mut reader, mut writer, _hub_rx) = start_conn();
        writer.send(&connect_packet("", false)).await.unwrap();
        match recv(&mut reader).await {
            Some(Packet::ConnAck(ConnAck {
                session_present,
                code,
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

        writer.send(&Packet::PubRel(7)).await.unwrap();
        assert_eq!(recv(&mut reader).await, Some(Packet::PubComp(7)));
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
