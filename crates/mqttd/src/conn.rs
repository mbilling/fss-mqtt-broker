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

#[cfg(test)]
mod tests {
    use super::handle_stream;
    use crate::hub::HubCommand;
    use mqtt_codec::{
        packet::{ConnAck, Connect},
        Packet, ProtocolVersion,
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

    /// Start a connection task over an in-memory duplex; returns the client's
    /// framed I/O and the hub command stream the connection produces.
    fn start_conn() -> (Reader, Writer, mpsc::UnboundedReceiver<HubCommand>) {
        let (client, server) = tokio::io::duplex(4096);
        let (hub_tx, hub_rx) = mpsc::unbounded_channel();
        tokio::spawn(handle_stream(server, None, hub_tx));
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
        tokio::spawn(handle_stream(server, None, hub_tx));
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
}
