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

use crate::hub::{HubCommand, PeerOutbound};
use bytes::BytesMut;
use mqtt_cluster::peer::{self, PeerMessage};
use mqtt_cluster::NodeId;
use mqtt_net::tls;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, warn};

/// The cluster bus mTLS context, built once from the cluster CA + node cert and
/// shared by every peer link (both accepting and dialing sides).
#[derive(Clone)]
pub struct PeerTls {
    /// Accepts inbound links, requiring a cluster-CA-issued client certificate.
    pub acceptor: TlsAcceptor,
    /// Dials outbound links, presenting our certificate and verifying the peer's.
    pub connector: TlsConnector,
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
pub async fn serve_listener(
    listener: TcpListener,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    tls: Option<PeerTls>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let local = local.clone();
                let hub = hub.clone();
                let tls = tls.clone();
                tokio::spawn(async move {
                    let _ = stream.set_nodelay(true);
                    let result = match &tls {
                        Some(t) => match t.acceptor.accept(stream).await {
                            Ok(s) => handle(s, local, hub, false).await,
                            Err(e) => {
                                debug!(error = %e, "peer mTLS handshake failed; link rejected");
                                return;
                            }
                        },
                        None => handle(stream, local, hub, false).await,
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
                            Ok(s) => handle(s, local.clone(), hub.clone(), true).await,
                            Err(e) => {
                                debug!(%addr, error = %e, "peer mTLS handshake failed; will retry");
                                tokio::time::sleep(REDIAL_DELAY).await;
                                continue;
                            }
                        }
                    }
                    _ => handle(stream, local.clone(), hub.clone(), true).await,
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

/// Run a single peer link: handshake, dedup, register, then pump until it closes.
///
/// `initiated` is true when we dialed (vs. accepted). To guarantee exactly one
/// link per node pair, the surviving link is the one whose initiating side has the
/// **smaller node id**; the other direction is dropped right after the handshake.
async fn handle<S>(
    stream: S,
    local: NodeId,
    hub: mpsc::UnboundedSender<HubCommand>,
    initiated: bool,
) -> Result<LinkOutcome, std::io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut rh, mut wh) = tokio::io::split(stream);

    // Send our Hello, then read the peer's Hello to learn its node id.
    write_frame(
        &mut wh,
        &PeerMessage::Hello {
            node_id: local.0.clone(),
        },
    )
    .await?;

    let mut buf = BytesMut::with_capacity(4096);
    let remote = match read_frame(&mut rh, &mut buf).await? {
        Some(PeerMessage::Hello { node_id }) => NodeId(node_id),
        Some(_) => {
            warn!("peer did not send Hello first; dropping link");
            return Ok(LinkOutcome::Closed);
        }
        None => return Ok(LinkOutcome::Closed),
    };
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
                    Some(msg) => write_frame(wh, &msg).await?,
                    None => return Ok(()), // taken over or hub gone
                }
            }
        }
    }
}

/// Translate an inbound peer message into a hub command.
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
            retain: _, // retained-state replication is Phase 3 work
        } => {
            let _ = hub.send(HubCommand::RemotePublish {
                topic,
                payload: payload.into(),
                qos: mqtt_codec::QoS::from_u8(qos).unwrap_or(mqtt_codec::QoS::AtMostOnce),
            });
        }
        PeerMessage::Hello { .. } => {
            warn!("unexpected duplicate Hello on established peer link");
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
