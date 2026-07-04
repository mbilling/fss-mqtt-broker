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

use crate::conn::ConnPolicy;
use crate::hub::{HubCommand, PeerOutbound};
use bytes::BytesMut;
use mqtt_cluster::peer::{self, PeerMessage};
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

/// The cluster bus mTLS context, built once from the cluster CA + node cert and
/// shared by every peer link (both accepting and dialing sides).
#[derive(Clone)]
pub struct PeerTls {
    /// Accepts inbound links, requiring a cluster-CA-issued client certificate.
    pub acceptor: TlsAcceptor,
    /// Dials outbound links, presenting our certificate and verifying the peer's.
    pub connector: TlsConnector,
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
) {
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let local = local.clone();
                let hub = hub.clone();
                let tls = tls.clone();
                let policy = client_policy.clone();
                tokio::spawn(async move {
                    let _ = stream.set_nodelay(true);
                    let result = match &tls {
                        Some(t) => match t.acceptor.accept(stream).await {
                            Ok(s) => {
                                let expected_cn = peer_cert_cn(s.get_ref().1);
                                handle(s, local, hub, false, expected_cn, policy).await
                            }
                            Err(e) => {
                                debug!(error = %e, "peer mTLS handshake failed; link rejected");
                                return;
                            }
                        },
                        None => handle(stream, local, hub, false, None, policy).await,
                    };
                    if let Err(e) = result {
                        debug!(error = %e, "inbound peer link ended");
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "peer listener accept failed");
                return;
            }
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
                        match t.connector.connect(name.clone(), stream).await {
                            Ok(s) => {
                                let expected_cn = peer_cert_cn(s.get_ref().1);
                                handle(s, local.clone(), hub.clone(), true, expected_cn, None).await
                            }
                            Err(e) => {
                                debug!(%addr, error = %e, "peer mTLS handshake failed; will retry");
                                tokio::time::sleep(REDIAL_DELAY).await;
                                continue;
                            }
                        }
                    }
                    _ => handle(stream, local.clone(), hub.clone(), true, None, None).await,
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

/// Whether the peer's announced protocol range overlaps ours (ADR 0038). A
/// disjoint range is logged loudly and the link must be dropped — a node that
/// cannot agree on a protocol version must not half-join the mesh.
fn proto_compatible(node_id: &str, proto_min: u32, proto_max: u32) -> bool {
    if peer::negotiate_proto((peer::PROTO_MIN, peer::PROTO_MAX), (proto_min, proto_max)).is_some() {
        true
    } else {
        warn!(
            peer = %node_id,
            ours = ?(peer::PROTO_MIN, peer::PROTO_MAX),
            theirs = ?(proto_min, proto_max),
            "peer speaks an incompatible peer-bus protocol range; dropping link (ADR 0038)"
        );
        false
    }
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
async fn handle<S>(
    stream: S,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    initiated: bool,
    expected_cn: Option<String>,
    client_policy: Option<Arc<ConnPolicy>>,
) -> Result<LinkOutcome, std::io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut rh, mut wh) = tokio::io::split(stream);
    let mut buf = BytesMut::with_capacity(4096);

    // The dialer announces itself first; the accept side reads first so it can
    // detect a session-proxy connection (ADR 0005) before announcing itself —
    // a proxied client expects raw MQTT back, not our peer Hello.
    let remote = if initiated {
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
                if !proto_compatible(&node_id, proto_min, proto_max) {
                    return Ok(LinkOutcome::Closed);
                }
                NodeId(node_id)
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
                if !proto_compatible(&node_id, proto_min, proto_max) {
                    return Ok(LinkOutcome::Closed);
                }
                write_frame(
                    &mut wh,
                    &PeerMessage::Hello {
                        node_id: local.0.clone(),
                        proto_min: peer::PROTO_MIN,
                        proto_max: peer::PROTO_MAX,
                    },
                )
                .await?;
                NodeId(node_id)
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
    if hub
        .send(HubCommand::PeerConnected {
            node: remote.clone(),
            conn_id,
            tx: out_tx,
        })
        .is_err()
    {
        return Ok(LinkOutcome::Closed);
    }

    let result = pump(&mut rh, &mut wh, &mut buf, &hub, &remote, &mut out_rx).await;
    let _ = hub.send(HubCommand::PeerDisconnected {
        node: remote,
        conn_id,
    });
    result.map(|()| LinkOutcome::Closed)
}

async fn pump<R, W>(
    rh: &mut R,
    wh: &mut W,
    buf: &mut BytesMut,
    hub: &mpsc::UnboundedSender<HubCommand>,
    remote: &NodeId,
    out_rx: &mut mpsc::UnboundedReceiver<PeerMessage>,
) -> Result<(), std::io::Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            inbound = read_frame(rh, buf) => {
                match inbound? {
                    None => return Ok(()), // peer closed
                    Some(msg) => forward_inbound(msg, hub, remote),
                }
            }
            maybe_out = out_rx.recv() => {
                match maybe_out {
                    // An oversized frame is dropped with a warning rather than killing
                    // the link: losing one best-effort message is strictly better than
                    // severing every message on the link (and a link-up back-fill that
                    // dies on send would die again on every reconnect). Other I/O
                    // errors still end the link as before.
                    Some(msg) => match write_frame(wh, &msg).await {
                        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                            warn!(error = %e, "dropping oversized/unencodable peer frame");
                        }
                        other => other?,
                    },
                    None => return Ok(()), // taken over or hub gone
                }
            }
        }
    }
}

/// Translate an inbound peer message into a hub command.
// One arm per wire variant — a flat dispatch table, not a refactor smell.
#[allow(clippy::too_many_lines)]
fn forward_inbound(msg: PeerMessage, hub: &mpsc::UnboundedSender<HubCommand>, remote: &NodeId) {
    match msg {
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
            let _ = hub.send(HubCommand::RemotePublish {
                topic,
                payload: payload.into(),
                qos: mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce),
                retain,
                message_expiry,
                app: crate::hub::app_from_wire(app),
            });
        }
        PeerMessage::SharedInterest { groups } => {
            let groups = groups
                .into_iter()
                .map(|(group, filter, members)| crate::hub::RemoteSharedGroup {
                    group,
                    filter,
                    members: members
                        .into_iter()
                        .map(|(c, q, online)| {
                            (
                                mqtt_core::ClientId(c),
                                mqtt_codec::QoS::from_u8(q).unwrap_or(mqtt_codec::QoS::AtMostOnce),
                                online,
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
            let _ = hub.send(HubCommand::RemoteSharedDeliver {
                client: mqtt_core::ClientId(client),
                topic,
                payload: payload.into(),
                qos: mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce),
                message_expiry,
                app: crate::hub::app_from_wire(app),
            });
        }
        PeerMessage::RetainedSnapshot { messages } => {
            let messages = messages
                .into_iter()
                .map(|(topic, payload, qos, epoch, offset)| {
                    (
                        topic,
                        payload.into(),
                        mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce),
                        epoch,
                        offset,
                    )
                })
                .collect();
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
            seq,
        } => {
            let _ = hub.send(HubCommand::RemoteRetainedCommit {
                node: remote.clone(),
                topic,
                payload: payload.into(),
                qos,
                seq,
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
        } => {
            let _ = hub.send(HubCommand::RemoteRetainedUpdate {
                topic,
                payload: payload.into(),
                qos,
                epoch,
                offset,
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
        | PeerMessage::ReplicaReadReply { .. }) => {
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
