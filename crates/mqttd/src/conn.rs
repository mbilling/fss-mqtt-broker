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
use mqtt_auth::{basic::BasicAuthenticator, Authenticator, Credentials, Identity};
use mqtt_codec::{
    packet::{ConnAck, Connect, Publish, SubAck},
    Packet, ProtocolVersion, QoS,
};
use mqtt_core::{ClientId, Message};
use mqtt_net::{FrameReader, FrameWriter, NetError};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, warn};

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

/// Drive one accepted plaintext TCP connection to completion, logging any error.
///
/// Test-only convenience path: anonymous clients are permitted and no transport
/// identity is attached. Production listeners go through [`handle_stream`] with
/// the operator-configured policy.
pub async fn handle(stream: TcpStream, hub: mpsc::UnboundedSender<HubCommand>) {
    let peer = stream.peer_addr().ok();
    let auth: Arc<dyn Authenticator> = Arc::new(BasicAuthenticator {
        allow_anonymous: true,
    });
    handle_stream(stream, peer, None, auth, hub).await;
}

/// Drive one accepted connection over any transport (TCP, TLS) to completion,
/// logging any error. `peer` is the remote address, for diagnostics only.
/// `identity` is the TLS-verified mTLS identity, `None` on plaintext or
/// no-client-cert connections; `auth` decides whether the CONNECT may proceed.
pub async fn handle_stream<S>(
    stream: S,
    peer: Option<SocketAddr>,
    identity: Option<Identity>,
    auth: Arc<dyn Authenticator>,
    hub: mpsc::UnboundedSender<HubCommand>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Err(e) = run(stream, identity, auth, hub).await {
        warn!(?peer, error = %e, "connection ended with error");
    }
}

async fn run<S>(
    stream: S,
    identity: Option<Identity>,
    auth: Arc<dyn Authenticator>,
    hub: mpsc::UnboundedSender<HubCommand>,
) -> Result<(), NetError>
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

    // Authentication gate: verify credentials BEFORE attaching to the hub, so
    // a rejected client never touches session state.
    // TODO(step 3): hand to the authorizer once ACLs land.
    let Some(_authenticated) =
        authenticate_connect(&mut writer, &client, &connect, identity.as_ref(), &*auth).await?
    else {
        return Ok(()); // rejected; the CONNACK was already sent
    };

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
    auth: &dyn Authenticator,
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
    match auth.authenticate(client, &creds) {
        Ok(id) => Ok(Some(id)),
        Err(e) => {
            let code = if matches!(creds, Credentials::Password { .. }) {
                CONNACK_BAD_CREDENTIALS
            } else {
                CONNACK_NOT_AUTHORIZED
            };
            warn!(client = %client.0, error = %e, "CONNECT rejected: authentication failed");
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
async fn serve<R, W>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
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
                        if handle_inbound(packet, writer, hub, client, &mut qos2_inbound).await? {
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

/// Handle one inbound packet. Returns `Ok(true)` if the connection should close.
async fn handle_inbound<W: AsyncWrite + Unpin>(
    packet: Packet,
    writer: &mut FrameWriter<W>,
    hub: &mpsc::UnboundedSender<HubCommand>,
    client: &ClientId,
    qos2_inbound: &mut HashSet<u16>,
) -> Result<bool, NetError> {
    match packet {
        Packet::Publish(Publish {
            qos,
            pkid,
            topic,
            payload,
            retain,
            ..
        }) => {
            let forward = |hub: &mpsc::UnboundedSender<HubCommand>| {
                let _ = hub.send(HubCommand::Publish {
                    topic,
                    payload,
                    qos,
                    retain,
                });
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
            // Grant the requested QoS [MQTT-3.8.4-5/6]; the broker supports the
            // full range, so the return code echoes the request.
            let filters: Vec<(String, QoS)> =
                s.filters.iter().map(|f| (f.path.clone(), f.qos)).collect();
            let return_codes: Vec<u8> = filters.iter().map(|(_, q)| *q as u8).collect();
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
    use mqtt_auth::{basic::BasicAuthenticator, Authenticator};
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

    /// A wide-open authenticator so these tests exercise the protocol paths,
    /// not the gate (covered in tests/auth.rs and mqtt-auth's unit tests).
    fn permissive() -> Arc<dyn Authenticator> {
        Arc::new(BasicAuthenticator {
            allow_anonymous: true,
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
}
