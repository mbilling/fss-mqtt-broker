//! Per-connection task: CONNECT handshake, then a select loop multiplexing
//! inbound client packets and outbound packets delivered by the hub.

use crate::hub::{HubCommand, Outbound};
use mqtt_codec::{
    packet::{ConnAck, Publish, SubAck},
    Packet, ProtocolVersion, QoS,
};
use mqtt_core::ClientId;
use mqtt_net::{FrameReader, FrameWriter, NetError};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

/// CONNACK reason: unacceptable protocol version (MQTT 3.1.1 return code 1).
const CONNACK_UNACCEPTABLE_PROTOCOL: u8 = 0x01;
/// CONNACK reason: identifier rejected (MQTT 3.1.1 return code 2).
const CONNACK_IDENTIFIER_REJECTED: u8 = 0x02;

/// Monotonic source of unique connection ids (distinct from client ids).
static CONN_ID: AtomicU64 = AtomicU64::new(1);
/// Counter for server-assigned client ids (empty-id clients).
static AUTO_ID: AtomicU64 = AtomicU64::new(1);

/// Drive one accepted plaintext TCP connection to completion, logging any error.
pub async fn handle(stream: TcpStream, hub: mpsc::UnboundedSender<HubCommand>) {
    let peer = stream.peer_addr().ok();
    handle_stream(stream, peer, hub).await;
}

/// Drive one accepted connection over any transport (TCP, TLS) to completion,
/// logging any error. `peer` is the remote address, for diagnostics only.
pub async fn handle_stream<S>(
    stream: S,
    peer: Option<SocketAddr>,
    hub: mpsc::UnboundedSender<HubCommand>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Err(e) = run(stream, hub).await {
        warn!(?peer, error = %e, "connection ended with error");
    }
}

async fn run<S>(stream: S, hub: mpsc::UnboundedSender<HubCommand>) -> Result<(), NetError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (rh, wh) = tokio::io::split(stream);
    let mut reader = FrameReader::new(rh, ProtocolVersion::V311);
    let mut writer = FrameWriter::new(wh, ProtocolVersion::V311);

    // Protocol requires CONNECT as the first packet on a connection.
    let connect = match reader.next_packet().await? {
        Some(Packet::Connect(c)) => c,
        Some(other) => {
            warn!(packet = ?other.packet_type(), "first packet was not CONNECT; closing");
            return Ok(());
        }
        None => return Ok(()),
    };

    // This milestone speaks only MQTT 3.1.1.
    if connect.protocol != ProtocolVersion::V311 {
        writer
            .send(&Packet::ConnAck(ConnAck {
                session_present: false,
                code: CONNACK_UNACCEPTABLE_PROTOCOL,
            }))
            .await?;
        return Ok(());
    }

    // An empty client id is only valid with clean session (the server assigns an
    // id). Pairing an empty id with a persistent session is rejected per spec.
    let client = if connect.client_id.is_empty() {
        if !connect.clean_session {
            writer
                .send(&Packet::ConnAck(ConnAck {
                    session_present: false,
                    code: CONNACK_IDENTIFIER_REJECTED,
                }))
                .await?;
            return Ok(());
        }
        ClientId(format!("auto-{}", AUTO_ID.fetch_add(1, Ordering::Relaxed)))
    } else {
        ClientId(connect.client_id.clone())
    };

    let conn_id = CONN_ID.fetch_add(1, Ordering::Relaxed);
    let (out_tx, mut out_rx): (Outbound, _) = mpsc::unbounded_channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    // Attach before sending CONNACK so we cannot miss a publish that races in, and
    // so the hub can tell us whether a session was already present.
    if hub
        .send(HubCommand::Attach {
            client: client.clone(),
            conn_id,
            clean_session: connect.clean_session,
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

    let result = serve(&mut reader, &mut writer, &hub, &client, &mut out_rx).await;
    // Always deregister, even on error. The hub ignores this if we were taken over.
    let _ = hub.send(HubCommand::Detach { client, conn_id });
    result
}

async fn serve<R, W>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
    out_rx: &mut mpsc::UnboundedReceiver<Packet>,
) -> Result<(), NetError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            inbound = reader.next_packet() => {
                match inbound? {
                    None => return Ok(()), // clean EOF
                    Some(packet) => {
                        if handle_inbound(packet, writer, hub, client).await? {
                            return Ok(()); // client sent DISCONNECT
                        }
                    }
                }
            }
            maybe_out = out_rx.recv() => {
                match maybe_out {
                    Some(pkt) => writer.send(&pkt).await?,
                    // The hub dropped our sender: we were taken over by a new
                    // connection for the same client id, or the hub shut down.
                    None => return Ok(()),
                }
            }
        }
    }
}

/// Handle one inbound packet. Returns `Ok(true)` if the connection should close.
async fn handle_inbound<W: AsyncWrite + Unpin>(
    packet: Packet,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
) -> Result<bool, NetError> {
    match packet {
        Packet::Publish(Publish {
            qos,
            pkid,
            topic,
            payload,
            ..
        }) => {
            // All delivery is QoS 0 downstream in this milestone; we still honor
            // the inbound QoS handshake so standard clients are not left retrying.
            let _ = hub.send(HubCommand::Publish { topic, payload });
            match (qos, pkid) {
                (QoS::AtLeastOnce, Some(id)) => writer.send(&Packet::PubAck(id)).await?,
                (QoS::ExactlyOnce, Some(id)) => writer.send(&Packet::PubRec(id)).await?,
                _ => {}
            }
        }
        // QoS 2 publisher-side completion: PUBREL -> PUBCOMP.
        Packet::PubRel(id) => writer.send(&Packet::PubComp(id)).await?,
        Packet::Subscribe(s) => {
            let filters: Vec<String> = s.filters.iter().map(|f| f.path.clone()).collect();
            // We grant QoS 0 for every filter (return code 0x00).
            let return_codes = vec![0x00u8; filters.len()];
            let _ = hub.send(HubCommand::Subscribe {
                client: client.clone(),
                filters,
            });
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
