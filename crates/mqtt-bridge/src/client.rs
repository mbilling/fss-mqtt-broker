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
//! Two ways to drive it. Step by step: [`MqttClient::next_event`],
//! [`MqttClient::publish`], [`MqttClient::subscribe`] (the tests, and the short
//! session-provisioning connect). Or as a running connection: [`MqttClient::spawn`] moves
//! the socket into its own task and hands back a [`Session`] — commands in, events out —
//! which is what the engine's links use. The spawned task also enforces the keepalive
//! ([ADR 0060](../../../docs/adr/0060-bridge-durability-and-ack-contract.md) Amendment 1
//! §4): a connection that has received nothing for one keepalive interval, or whose write
//! has blocked that long, is closed instead of being trusted until TCP gives up.

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use mqtt_codec::packet::{
    Ack, Connect, Disconnect, Packet, Publish, Subscribe, SubscribeFilter, SubscriptionOptions,
};
use mqtt_codec::properties::{Properties, Property};
use mqtt_codec::{ProtocolVersion, QoS};
use mqtt_net::tls::{client_connector, server_name};
use mqtt_net::{FrameReader, FrameWriter};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};

/// How long a connect attempt (TCP, TLS and the CONNECT/CONNACK exchange) may take. A
/// broker behind a firewall that drops packets would otherwise hold the attempt for as long
/// as the OS retries SYNs.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// The connect attempt did not finish within [`CONNECT_TIMEOUT`].
    #[error("connect to {0}: no CONNACK within {CONNECT_TIMEOUT:?}")]
    ConnectTimeout(String),
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
    /// The broker sent DISCONNECT with this reason code.
    #[error("the broker disconnected: reason 0x{0:02x}")]
    Disconnected(u8),
    /// Nothing arrived from the broker for one keepalive interval, though the client kept
    /// sending PINGREQ: the connection is treated as dead.
    #[error("no packet from the broker within the keepalive")]
    KeepAlive,
    /// A write blocked for longer than the keepalive (a peer that stopped reading).
    #[error("a write blocked for longer than the keepalive")]
    WriteStalled,
}

/// What the broker's CONNACK granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Granted {
    /// Whether the broker resumed a stored session (`session_present`).
    pub session_present: bool,
    /// How many unacknowledged `QoS` 1 publishes the broker accepts from this client at
    /// once: its Receive Maximum, 65,535 when the CONNACK carries none.
    pub receive_maximum: u16,
    /// The keepalive to use, in seconds: the broker's Server Keep Alive when it sent one,
    /// otherwise the one this client asked for.
    pub keep_alive: u16,
}

/// A connected MQTT client session.
pub struct MqttClient {
    reader: FrameReader<ReadHalf<Box<dyn Io>>>,
    writer: FrameWriter<WriteHalf<Box<dyn Io>>>,
    granted: Granted,
}

impl std::fmt::Debug for MqttClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttClient")
            .field("granted", &self.granted)
            .finish_non_exhaustive()
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
    /// session on the cluster side (ADR 0060 Amendment 1), so this is configurable.
    pub clean_start: bool,
    /// MQTT 5 Session Expiry Interval, in seconds. `0` sends none, and MQTT 5 then ends the
    /// session when the connection ends, whatever `clean_start` says [MQTT-3.1.2-11.2];
    /// `u32::MAX` means the session never expires.
    pub session_expiry: u32,
    /// MQTT 5 Receive Maximum to advertise: how many unacknowledged `QoS` 1 publishes the
    /// broker may send at once. `None` sends none, which means 65,535.
    pub receive_maximum: Option<u16>,
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

/// A command to a running connection ([`MqttClient::spawn`]).
#[derive(Debug)]
pub enum Outgoing {
    /// Send this PUBLISH. For `QoS` 1 the caller has assigned the packet id.
    Publish(Publish),
    /// Acknowledge the inbound `QoS` 1 PUBLISH with this packet id.
    PubAck(u16),
    /// Subscribe to one filter.
    Subscribe {
        /// Subscribe packet id.
        pkid: u16,
        /// Topic filter.
        filter: String,
        /// Requested maximum `QoS`.
        qos: QoS,
        /// MQTT 5 subscription options.
        options: SubscriptionOptions,
    },
    /// Stop reading from the broker (`true`) or read again (`false`). While reads are
    /// paused the broker's messages wait in the socket and the broker, and the keepalive
    /// deadline does not run.
    PauseReads(bool),
    /// Send DISCONNECT and close.
    Disconnect,
}

/// What a running connection reports ([`MqttClient::spawn`]).
#[derive(Debug)]
pub enum Incoming {
    /// A delivered PUBLISH.
    Publish(Publish),
    /// The broker acknowledged our `QoS` 1 PUBLISH `pkid` with `reason` (0 = success; MQTT 5
    /// codes ≥ 0x80 are refusals).
    PubAck {
        /// The packet id acknowledged.
        pkid: u16,
        /// The PUBACK reason code.
        reason: u8,
    },
    /// A SUBACK with the granted return codes for `pkid`.
    SubAck {
        /// The subscribe packet id this acknowledges.
        pkid: u16,
        /// Per-filter granted `QoS` / `0x80`+ failure codes.
        return_codes: Vec<u8>,
    },
    /// The connection ended. Nothing follows.
    Closed(ClientError),
}

/// A connection running in its own task: send [`Outgoing`] on `tx`, read [`Incoming`] from
/// `rx`. Dropping `tx` closes the connection.
#[derive(Debug)]
pub struct Session {
    /// Commands to the connection.
    pub tx: mpsc::UnboundedSender<Outgoing>,
    /// Events from the connection, ending with [`Incoming::Closed`].
    pub rx: mpsc::UnboundedReceiver<Incoming>,
    /// What the CONNACK granted.
    pub granted: Granted,
}

/// The MQTT 5 subscription options for a bridge subscription to `filter` (issues #189,
/// #191), with `retain_handling` chosen by the caller.
///
/// The bridge is always a subscriber that re-forwards, so it needs **Retain As Published**:
/// with RAP a live message forwarded because it matched the subscription keeps the RETAIN flag
/// it was published with, instead of the broker clearing it for established-subscription
/// delivery [MQTT-3.3.1-9]. That is what lets retained state cross the boundary. Retain
/// Handling is the caller's: 0 sends the retained messages at subscribe, 1 only when the
/// subscription is new, 2 never (ADR 0060 Amendment 1 §6).
///
/// **No Local** (`no_local = true`) is the primary, *unforgeable* loop defence — the mechanism
/// Mosquitto (`try_private`) and EMQX (`bridge_mode`, "equivalent to No Local = 1") both use: the
/// broker never echoes back a message this connection itself published, so a `both`-direction
/// rule cannot loop through its own forward. Unlike the `fss-bridge-hop-count` user property
/// (which any publisher can set, #191), NL is a property of the *subscription*, so it cannot be
/// forged by a publisher. The hop counter stays as the multi-broker-cycle backstop (A→B→C→A,
/// which NL alone cannot catch), matching `HiveMQ`. NL needs the broker to honour the option;
/// against a broker that ignores it the structural direction + remap defence (§6) still holds.
/// It is never set on a `$share` subscription, where MQTT 5 makes it a protocol error
/// [MQTT-3.8.3-4].
#[must_use]
pub fn subscription_options(filter: &str, retain_handling: u8) -> SubscriptionOptions {
    SubscriptionOptions {
        no_local: !filter.starts_with("$share/"),
        retain_as_published: true,
        retain_handling,
    }
}

impl MqttClient {
    /// Dial, perform the MQTT CONNECT handshake, and return a live session. The whole
    /// attempt is bounded by [`CONNECT_TIMEOUT`].
    ///
    /// # Errors
    /// [`ClientError`] on dial/TLS/handshake failure, a non-zero CONNACK reason, or the
    /// timeout.
    pub async fn connect(opts: &ConnectOptions) -> Result<Self, ClientError> {
        match tokio::time::timeout(CONNECT_TIMEOUT, Self::connect_inner(opts)).await {
            Ok(result) => result,
            Err(_) => Err(ClientError::ConnectTimeout(opts.addr.clone())),
        }
    }

    async fn connect_inner(opts: &ConnectOptions) -> Result<Self, ClientError> {
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

        let mut properties = Properties::new();
        if opts.version == ProtocolVersion::V5 {
            if opts.session_expiry > 0 {
                properties
                    .0
                    .push(Property::SessionExpiryInterval(opts.session_expiry));
            }
            if let Some(max) = opts.receive_maximum {
                properties.0.push(Property::ReceiveMaximum(max.max(1)));
            }
        }
        writer
            .send(&Packet::Connect(Connect {
                properties,
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
            Ok(Some(Packet::ConnAck(ack))) if ack.code == 0 => {
                let receive_maximum = ack
                    .properties
                    .receive_maximum()
                    .filter(|max| *max > 0)
                    .unwrap_or(u16::MAX);
                let keep_alive = ack
                    .properties
                    .0
                    .iter()
                    .find_map(|p| match p {
                        Property::ServerKeepAlive(v) => Some(*v),
                        _ => None,
                    })
                    .unwrap_or(opts.keep_alive);
                Ok(Self {
                    reader,
                    writer,
                    granted: Granted {
                        session_present: ack.session_present,
                        receive_maximum,
                        keep_alive,
                    },
                })
            }
            Ok(Some(Packet::ConnAck(ack))) => Err(ClientError::Refused(ack.code)),
            Ok(Some(_)) => Err(ClientError::Protocol("expected CONNACK".into())),
            Ok(None) => Err(ClientError::Closed),
            Err(e) => Err(ClientError::Protocol(e.to_string())),
        }
    }

    /// What the broker's CONNACK granted.
    #[must_use]
    pub fn granted(&self) -> Granted {
        self.granted
    }

    /// Subscribe to one `filter` at `qos` with subscribe packet id `pkid`, with the bridge's
    /// subscription options and retain handling 0. Returns once the SUBSCRIBE is written;
    /// the matching [`Event::SubAck`] arrives via [`Self::next_event`].
    ///
    /// # Errors
    /// [`ClientError::Protocol`] if the write fails.
    pub async fn subscribe(
        &mut self,
        pkid: u16,
        filter: &str,
        qos: QoS,
    ) -> Result<(), ClientError> {
        self.subscribe_with(pkid, filter, qos, subscription_options(filter, 0))
            .await
    }

    /// Subscribe to one `filter` at `qos` with explicit subscription `options`.
    ///
    /// # Errors
    /// [`ClientError::Protocol`] if the write fails.
    pub async fn subscribe_with(
        &mut self,
        pkid: u16,
        filter: &str,
        qos: QoS,
        options: SubscriptionOptions,
    ) -> Result<(), ClientError> {
        self.writer
            .send(&Packet::Subscribe(Subscribe {
                properties: Properties::new(),
                pkid,
                filters: vec![SubscribeFilter {
                    path: filter.to_string(),
                    qos,
                    options,
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
        retain: bool,
        pkid: Option<u16>,
        properties: Properties,
    ) -> Result<(), ClientError> {
        self.writer
            .send(&Packet::Publish(Publish {
                dup: false,
                qos,
                retain,
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
            .send(&Packet::Disconnect(Disconnect {
                reason: 0,
                properties: Properties::new(),
            }))
            .await;
    }

    /// Run this connection in its own task and return its [`Session`].
    ///
    /// The task reads packets and reports them as [`Incoming`] events, writes the
    /// [`Outgoing`] commands, and sends PINGREQ every half keepalive. It closes the
    /// connection, reporting [`Incoming::Closed`], when:
    ///
    /// - nothing has arrived from the broker for a full keepalive interval while reads were
    ///   not paused ([`ClientError::KeepAlive`]) — a broker that vanished without a reset;
    /// - a write blocks for that long ([`ClientError::WriteStalled`]) — a broker that stopped
    ///   reading;
    /// - the broker closes or sends DISCONNECT, a packet fails to decode, or `tx` is dropped.
    ///
    /// Must be called inside a Tokio runtime.
    #[must_use]
    pub fn spawn(self) -> Session {
        let (tx, commands) = mpsc::unbounded_channel();
        let (events, rx) = mpsc::unbounded_channel();
        let granted = self.granted;
        tokio::spawn(run_connection(self, commands, events));
        Session { tx, rx, granted }
    }
}

/// The body of a spawned connection (see [`MqttClient::spawn`]).
async fn run_connection(
    client: MqttClient,
    mut commands: mpsc::UnboundedReceiver<Outgoing>,
    events: mpsc::UnboundedSender<Incoming>,
) {
    let MqttClient {
        mut reader,
        mut writer,
        granted,
    } = client;
    let every = ping_interval(granted.keep_alive);
    // Two ping intervals: a broker that answers has always had a full PINGREQ round trip
    // inside the window, even with a 1-second keepalive.
    let deadline = every * 2;
    let mut ping = tokio::time::interval_at(Instant::now() + every, every);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_rx = Instant::now();
    let mut paused = false;

    let end = loop {
        tokio::select! {
            packet = reader.next_packet(), if !paused => match packet {
                Ok(Some(packet)) => {
                    last_rx = Instant::now();
                    let event = match packet {
                        Packet::Publish(p) => Incoming::Publish(p),
                        Packet::PubAck(a) => Incoming::PubAck { pkid: a.pkid, reason: a.reason },
                        Packet::SubAck(s) => Incoming::SubAck {
                            pkid: s.pkid,
                            return_codes: s.return_codes,
                        },
                        Packet::Disconnect(d) => break ClientError::Disconnected(d.reason),
                        // PINGRESP refreshed `last_rx` above; nothing else needs an answer.
                        _ => continue,
                    };
                    if events.send(event).is_err() {
                        break ClientError::Closed; // the owner is gone
                    }
                }
                Ok(None) => break ClientError::Closed,
                Err(e) => break ClientError::Protocol(e.to_string()),
            },
            command = commands.recv() => {
                let packet = match command {
                    None => break ClientError::Closed, // the owner dropped the session
                    Some(Outgoing::PauseReads(pause)) => {
                        if paused && !pause {
                            // The deadline restarts: nothing was read while paused.
                            last_rx = Instant::now();
                        }
                        paused = pause;
                        continue;
                    }
                    Some(Outgoing::Disconnect) => {
                        let bye = Packet::Disconnect(Disconnect {
                            reason: 0,
                            properties: Properties::new(),
                        });
                        let _ = write(&mut writer, &bye, deadline).await;
                        break ClientError::Closed;
                    }
                    Some(Outgoing::Publish(p)) => Packet::Publish(p),
                    Some(Outgoing::PubAck(pkid)) => Packet::PubAck(Ack::new(pkid)),
                    Some(Outgoing::Subscribe { pkid, filter, qos, options }) => {
                        Packet::Subscribe(Subscribe {
                            properties: Properties::new(),
                            pkid,
                            filters: vec![SubscribeFilter { path: filter, qos, options }],
                        })
                    }
                };
                if let Err(e) = write(&mut writer, &packet, deadline).await {
                    break e;
                }
            },
            _ = ping.tick() => {
                if !paused && last_rx.elapsed() >= deadline {
                    break ClientError::KeepAlive;
                }
                if let Err(e) = write(&mut writer, &Packet::PingReq, deadline).await {
                    break e;
                }
            }
        }
    };
    let _ = events.send(Incoming::Closed(end));
}

/// Write one packet, giving up after `limit`.
async fn write(
    writer: &mut FrameWriter<WriteHalf<Box<dyn Io>>>,
    packet: &Packet,
    limit: Duration,
) -> Result<(), ClientError> {
    match tokio::time::timeout(limit, writer.send(packet)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(ClientError::Protocol(e.to_string())),
        Err(_) => Err(ClientError::WriteStalled),
    }
}

/// The keepalive ping interval for a given advertised keep-alive: half the window, floored
/// at 1s, so a ping always reaches the broker before the 1.5× keep-alive timeout.
#[must_use]
pub fn ping_interval(keep_alive: u16) -> Duration {
    Duration::from_secs(u64::from(keep_alive).max(2) / 2)
}

#[cfg(test)]
mod tests {
    use super::subscription_options;

    #[test]
    fn bridge_subscriptions_set_no_local_and_retain_as_published() {
        let o = subscription_options("a/#", 1);
        // No Local is the primary, unforgeable loop defence (#191) — Mosquitto/EMQX parity.
        assert!(o.no_local, "bridge subscriptions must set No Local");
        // Retain As Published lets retained state cross the boundary (#189).
        assert!(o.retain_as_published, "bridge subscriptions must set RAP");
        // Retain handling is the caller's choice (ADR 0060 Amendment 1 §6).
        assert_eq!(o.retain_handling, 1);
    }

    #[test]
    fn a_share_subscription_never_sets_no_local() {
        // MQTT 5 makes No Local on a shared subscription a protocol error [MQTT-3.8.3-4].
        let o = subscription_options("$share/g/a/#", 0);
        assert!(!o.no_local);
        assert!(o.retain_as_published);
    }
}
