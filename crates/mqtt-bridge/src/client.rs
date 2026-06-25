//! A small async MQTT client built on `mqtt-codec` + `mqtt-net`
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §1–2).
//!
//! The bridge is a *client* to both the local cluster and each external broker — not a
//! second broker — so this is a deliberately minimal client: connect (plain TCP or
//! TLS/mTLS), CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH (`QoS` 0/1, both directions),
//! PUBACK, periodic PINGREQ keepalive, and DISCONNECT. `QoS` 2 is intentionally not
//! offered: the bridge promises at-least-once for `QoS`≥1 across two independent brokers
//! (ADR 0025 §7), so it downgrades `QoS` 2 rules to `QoS` 1 rather than run a cross-broker
//! exactly-once handshake it cannot honour.
//!
//! The client owns the socket; callers drive it by reading [`MqttClient::next_event`] in a
//! loop and calling [`MqttClient::publish`] / [`MqttClient::subscribe`]. Reconnection and
//! forwarding policy live above this layer (the engine).

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use mqtt_codec::packet::{Ack, Connect, Packet, Publish, Subscribe, SubscribeFilter};
use mqtt_codec::properties::Properties;
use mqtt_codec::{ProtocolVersion, QoS};
use mqtt_net::tls::{client_connector, server_name};
use mqtt_net::{FrameReader, FrameWriter};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// Any byte stream the client can run over (plain TCP or a TLS session).
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// How to secure an upstream/cluster connection.
#[derive(Debug, Clone)]
pub enum Transport {
    /// Plain TCP — only for a trusted/loopback hop; never across the boundary itself.
    Plain,
    /// TLS 1.3 with a client identity (mTLS): a per-upstream CA bundle, certificate
    /// chain, and private key (ADR 0002/0025 §8).
    Tls {
        /// PEM bundle of CAs trusted to sign the *server* certificate.
        ca: PathBuf,
        /// Client certificate chain (PEM) — this bridge's identity to the broker.
        cert: PathBuf,
        /// Client private key (PEM).
        key: PathBuf,
    },
}

/// Errors from the client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// TCP connect / IO failure.
    #[error("connect to {addr}: {source}")]
    Connect {
        /// The address dialled.
        addr: String,
        /// Underlying IO error.
        source: std::io::Error,
    },
    /// TLS setup or handshake failure.
    #[error("tls: {0}")]
    Tls(String),
    /// A wire-protocol/codec error from the framed transport.
    #[error("protocol: {0}")]
    Protocol(String),
    /// The broker rejected CONNECT with a non-zero reason code.
    #[error("connection refused by broker: reason 0x{0:02x}")]
    Refused(u8),
    /// The peer closed the connection.
    #[error("connection closed by peer")]
    Closed,
}

/// A connected MQTT client session.
pub struct MqttClient {
    reader: FrameReader<ReadHalf<Box<dyn Io>>>,
    writer: FrameWriter<WriteHalf<Box<dyn Io>>>,
}

impl std::fmt::Debug for MqttClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttClient").finish_non_exhaustive()
    }
}

/// Parameters for opening a client session.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// `host:port` to dial.
    pub addr: String,
    /// Transport security.
    pub transport: Transport,
    /// Protocol version to speak. v5 carries the hop-count user property (ADR 0025 §6).
    pub version: ProtocolVersion,
    /// MQTT client id.
    pub client_id: String,
    /// Optional username (least-privilege bridge account, ADR 0025 §8).
    pub username: Option<String>,
    /// Optional password.
    pub password: Option<Bytes>,
    /// Keep-alive seconds advertised to the broker; the client pings at half this.
    pub keep_alive: u16,
    /// `clean_session` (v3.1.1) / `clean_start` (v5). The bridge uses a **persistent**
    /// session on the cluster side for HA (ADR 0025 §5), so this is configurable.
    pub clean_start: bool,
}

/// An event surfaced from the broker while pumping the client.
#[derive(Debug)]
pub enum Event {
    /// An inbound application message (a delivered PUBLISH).
    Publish(Publish),
    /// The broker acknowledged one of our QoS-1 publishes.
    PubAck(u16),
    /// A keepalive response — no action needed.
    PingResp,
    /// A SUBACK with the granted return codes for `pkid`.
    SubAck {
        /// The subscribe packet id this acknowledges.
        pkid: u16,
        /// Per-filter granted `QoS` / `0x80` failure codes.
        return_codes: Vec<u8>,
    },
}

/// A command driven into a running client's **writer** side by the forwarding engine. The
/// engine has already applied the rule's direction, remap, `QoS`, and hop count before
/// issuing a [`Command::Publish`]; the client just serializes it.
#[derive(Debug)]
pub enum Command {
    /// Subscribe to `filter` at `qos` with subscribe id `pkid`.
    Subscribe {
        /// Subscribe packet id.
        pkid: u16,
        /// Topic filter to subscribe to.
        filter: String,
        /// Requested maximum `QoS`.
        qos: QoS,
    },
    /// Publish a message (already transformed by the engine).
    Publish {
        /// Destination topic.
        topic: String,
        /// Application payload.
        payload: Bytes,
        /// Delivery `QoS`.
        qos: QoS,
        /// Packet id for `QoS` ≥ 1 (ignored for `QoS` 0).
        pkid: Option<u16>,
        /// MQTT 5 properties (e.g. the incremented hop-count user property).
        properties: Properties,
    },
}

impl MqttClient {
    /// Dial, perform the MQTT CONNECT handshake, and return a live session.
    ///
    /// # Errors
    /// [`ClientError`] on dial/TLS/handshake failure or a non-zero CONNACK reason.
    pub async fn connect(opts: &ConnectOptions) -> Result<Self, ClientError> {
        let tcp = TcpStream::connect(&opts.addr)
            .await
            .map_err(|source| ClientError::Connect {
                addr: opts.addr.clone(),
                source,
            })?;
        tcp.set_nodelay(true).ok();
        let stream: Box<dyn Io> = match &opts.transport {
            Transport::Plain => Box::new(tcp),
            Transport::Tls { ca, cert, key } => {
                let connector =
                    client_connector(ca, cert, key).map_err(|e| ClientError::Tls(e.to_string()))?;
                let sni = server_name(&opts.addr).map_err(|e| ClientError::Tls(e.to_string()))?;
                let tls = connector
                    .connect(sni, tcp)
                    .await
                    .map_err(|e| ClientError::Tls(e.to_string()))?;
                Box::new(tls)
            }
        };

        let (rh, wh) = tokio::io::split(stream);
        let mut reader = FrameReader::new(rh, opts.version);
        let mut writer = FrameWriter::new(wh, opts.version);

        writer
            .send(&Packet::Connect(Connect {
                properties: Properties::new(),
                protocol: opts.version,
                clean_session: opts.clean_start,
                keep_alive: opts.keep_alive,
                client_id: opts.client_id.clone(),
                last_will: None,
                username: opts.username.clone(),
                password: opts.password.clone(),
            }))
            .await
            .map_err(|e| ClientError::Protocol(e.to_string()))?;

        match reader.next_packet().await {
            Ok(Some(Packet::ConnAck(ack))) if ack.code == 0 => Ok(Self { reader, writer }),
            Ok(Some(Packet::ConnAck(ack))) => Err(ClientError::Refused(ack.code)),
            Ok(Some(_)) => Err(ClientError::Protocol("expected CONNACK".into())),
            Ok(None) => Err(ClientError::Closed),
            Err(e) => Err(ClientError::Protocol(e.to_string())),
        }
    }

    /// Subscribe to one `filter` at `qos` with subscribe packet id `pkid`. Returns once the
    /// SUBSCRIBE is written; the matching [`Event::SubAck`] arrives via [`Self::next_event`].
    ///
    /// # Errors
    /// [`ClientError::Protocol`] if the write fails.
    pub async fn subscribe(
        &mut self,
        pkid: u16,
        filter: &str,
        qos: QoS,
    ) -> Result<(), ClientError> {
        self.writer
            .send(&Packet::Subscribe(Subscribe {
                properties: Properties::new(),
                pkid,
                filters: vec![SubscribeFilter {
                    path: filter.to_string(),
                    qos,
                    options: mqtt_codec::packet::SubscriptionOptions::default(),
                }],
            }))
            .await
            .map_err(|e| ClientError::Protocol(e.to_string()))
    }

    /// Publish `payload` to `topic` at `qos` with the given v5 `properties` (e.g. the
    /// hop-count user property). For `QoS` 1 the caller supplies a `pkid`; `QoS` 0 ignores it.
    ///
    /// # Errors
    /// [`ClientError::Protocol`] if the write fails.
    pub async fn publish(
        &mut self,
        topic: &str,
        payload: Bytes,
        qos: QoS,
        pkid: Option<u16>,
        properties: Properties,
    ) -> Result<(), ClientError> {
        self.writer
            .send(&Packet::Publish(Publish {
                dup: false,
                qos,
                retain: false,
                topic: topic.to_string(),
                pkid: if qos == QoS::AtMostOnce { None } else { pkid },
                properties,
                payload,
            }))
            .await
            .map_err(|e| ClientError::Protocol(e.to_string()))
    }

    /// Acknowledge a received QoS-1 PUBLISH.
    ///
    /// # Errors
    /// [`ClientError::Protocol`] if the write fails.
    pub async fn puback(&mut self, pkid: u16) -> Result<(), ClientError> {
        self.writer
            .send(&Packet::PubAck(Ack::new(pkid)))
            .await
            .map_err(|e| ClientError::Protocol(e.to_string()))
    }

    /// Send a PINGREQ keepalive.
    ///
    /// # Errors
    /// [`ClientError::Protocol`] if the write fails.
    pub async fn ping(&mut self) -> Result<(), ClientError> {
        self.writer
            .send(&Packet::PingReq)
            .await
            .map_err(|e| ClientError::Protocol(e.to_string()))
    }

    /// Read the next broker event, translating raw packets into [`Event`]s and silently
    /// answering protocol housekeeping (a PUBREC/anything unexpected is reported as a
    /// protocol error). Returns [`ClientError::Closed`] on a clean EOF.
    ///
    /// # Errors
    /// [`ClientError`] on a closed connection or a protocol/codec error.
    pub async fn next_event(&mut self) -> Result<Event, ClientError> {
        loop {
            match self.reader.next_packet().await {
                Ok(Some(Packet::Publish(p))) => return Ok(Event::Publish(p)),
                Ok(Some(Packet::PubAck(a))) => return Ok(Event::PubAck(a.pkid)),
                Ok(Some(Packet::PingResp)) => return Ok(Event::PingResp),
                Ok(Some(Packet::SubAck(s))) => {
                    return Ok(Event::SubAck {
                        pkid: s.pkid,
                        return_codes: s.return_codes,
                    })
                }
                // PINGREQ from a broker should not happen; ignore other server housekeeping
                // we do not model (the broker drives QoS-2 we never initiate).
                Ok(Some(_)) => {}
                Ok(None) => return Err(ClientError::Closed),
                Err(e) => return Err(ClientError::Protocol(e.to_string())),
            }
        }
    }

    /// Send a graceful DISCONNECT (best-effort).
    pub async fn disconnect(&mut self) {
        let _ = self
            .writer
            .send(&Packet::Disconnect(mqtt_codec::packet::Disconnect {
                reason: 0,
                properties: Properties::new(),
            }))
            .await;
    }

    /// Drive this client until the connection ends, consuming it.
    ///
    /// Concurrently: reads inbound packets — emitting each delivered PUBLISH to `inbound`
    /// and **auto-acking** a `QoS` 1 delivery — while serializing `commands` from the
    /// engine onto the wire and sending a periodic keepalive PING. Reading and writing run
    /// in one `select!` over the split reader/writer, so a connection used for only one
    /// direction (a one-way rule) still answers pings and never blocks. Returns the
    /// terminal [`ClientError`] (so a supervisor can reconnect with backoff).
    pub async fn run(
        self,
        commands: &mut mpsc::UnboundedReceiver<Command>,
        inbound: &mpsc::UnboundedSender<Publish>,
        keep_alive: u16,
    ) -> ClientError {
        let Self {
            mut reader,
            mut writer,
        } = self;
        let mut ping = tokio::time::interval(ping_interval(keep_alive));
        // One packet-id space per connection: the run loop assigns ids for QoS ≥ 1 so live
        // and replayed (spooled) publishes never need the caller to track them.
        let mut next_pkid: u16 = 0;
        loop {
            tokio::select! {
                packet = reader.next_packet() => match packet {
                    Ok(Some(Packet::Publish(p))) => {
                        // Acknowledge a QoS 1 delivery so the source broker releases it
                        // (at-least-once; the cross-broker spool is ADR 0025 T7).
                        if p.qos == QoS::AtLeastOnce {
                            if let Some(id) = p.pkid {
                                if writer.send(&Packet::PubAck(Ack::new(id))).await.is_err() {
                                    return ClientError::Closed;
                                }
                            }
                        }
                        if inbound.send(p).is_err() {
                            return ClientError::Closed; // the engine is gone
                        }
                    }
                    // SUBACK / PUBACK / PINGRESP: housekeeping we do not act on here.
                    Ok(Some(_)) => {}
                    Ok(None) => return ClientError::Closed,
                    Err(e) => return ClientError::Protocol(e.to_string()),
                },
                cmd = commands.recv() => match cmd {
                    Some(Command::Subscribe { pkid, filter, qos }) => {
                        let pkt = Packet::Subscribe(Subscribe {
                            properties: Properties::new(),
                            pkid,
                            filters: vec![SubscribeFilter {
                                path: filter,
                                qos,
                                options: mqtt_codec::packet::SubscriptionOptions::default(),
                            }],
                        });
                        if writer.send(&pkt).await.is_err() {
                            return ClientError::Closed;
                        }
                    }
                    Some(Command::Publish { topic, payload, qos, pkid, properties }) => {
                        let pkid = if qos == QoS::AtMostOnce {
                            None
                        } else {
                            pkid.or_else(|| {
                                next_pkid = next_pkid.wrapping_add(1).max(1);
                                Some(next_pkid)
                            })
                        };
                        let pkt = Packet::Publish(Publish {
                            dup: false,
                            qos,
                            retain: false,
                            topic,
                            pkid,
                            properties,
                            payload,
                        });
                        if writer.send(&pkt).await.is_err() {
                            return ClientError::Closed;
                        }
                    }
                    None => return ClientError::Closed, // the engine dropped our command channel
                },
                _ = ping.tick() => {
                    if writer.send(&Packet::PingReq).await.is_err() {
                        return ClientError::Closed;
                    }
                }
            }
        }
    }
}

/// The keepalive ping interval for a given advertised keep-alive: half the window, floored
/// at 1s, so a ping always reaches the broker before the 1.5× keep-alive timeout.
#[must_use]
pub fn ping_interval(keep_alive: u16) -> Duration {
    Duration::from_secs(u64::from(keep_alive).max(2) / 2)
}
