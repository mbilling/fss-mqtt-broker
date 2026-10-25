//! The MQTT 5 handshake on the workspace codec: CONNECT/CONNACK, SUBSCRIBE/SUBACK,
//! DISCONNECT.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscribeFilter};
use mqtt_codec::{
    Disconnect, Packet, Properties, Property, ProtocolVersion, QoS, SubscriptionOptions,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The protocol every connection speaks: MQTT 5 (Receive Maximum and session expiry
/// are MQTT 5 properties).
pub const VERSION: ProtocolVersion = ProtocolVersion::V5;

/// A connection failure.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Socket error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Malformed traffic from the broker.
    #[error("codec: {0}")]
    Codec(#[from] mqtt_codec::CodecError),
    /// The broker refused or closed.
    #[error("{0}")]
    Refused(String),
    /// A handshake step did not complete in time.
    #[error("timed out: {0}")]
    Timeout(&'static str),
}

/// What the CONNACK granted.
#[derive(Debug, Clone, Copy)]
pub struct Granted {
    /// The broker's Receive Maximum (65535 if it sent none).
    pub receive_maximum: u16,
    /// The broker's keep-alive override, if any.
    pub server_keepalive: Option<u16>,
    /// Whether the broker had a session for this client id.
    pub session_present: bool,
}

/// An established connection: the stream, what the broker granted, and any bytes
/// read past the last handshake packet.
#[derive(Debug)]
pub struct Conn {
    /// The socket.
    pub stream: TcpStream,
    /// CONNACK grants.
    pub granted: Granted,
    /// Bytes already read and not yet decoded.
    pub buf: BytesMut,
}

/// Connection parameters.
#[derive(Debug, Clone)]
pub struct ConnectOpts<'a> {
    /// Client id.
    pub client_id: &'a str,
    /// Clean start.
    pub clean: bool,
    /// Session expiry, seconds (0 = ends with the connection).
    pub session_expiry: u32,
    /// Keep-alive, seconds.
    pub keepalive: u16,
    /// Our Receive Maximum: how many `QoS` 1 deliveries the broker may have in flight
    /// to us.
    pub receive_maximum: u16,
    /// Username.
    pub username: Option<&'a str>,
    /// Password.
    pub password: Option<&'a str>,
}

async fn write_packet(stream: &mut TcpStream, p: &Packet) -> Result<(), ClientError> {
    let mut out = Vec::with_capacity(64);
    p.encode(&mut out, VERSION)?;
    stream.write_all(&out).await?;
    Ok(())
}

/// Read packets until one satisfies `want`; anything else is an error.
async fn read_until<T>(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    mut want: impl FnMut(Packet) -> Result<Option<T>, ClientError>,
) -> Result<T, ClientError> {
    loop {
        while let Some(p) = Packet::decode(buf, VERSION)? {
            if let Some(t) = want(p)? {
                return Ok(t);
            }
        }
        buf.reserve(4096);
        if stream.read_buf(buf).await? == 0 {
            return Err(ClientError::Refused(
                "connection closed during handshake".into(),
            ));
        }
    }
}

/// Connect and complete the CONNECT/CONNACK exchange.
///
/// # Errors
/// Socket, codec, refusal or timeout.
pub async fn connect(
    addr: SocketAddr,
    opts: &ConnectOpts<'_>,
    timeout: Duration,
) -> Result<Conn, ClientError> {
    tokio::time::timeout(timeout, connect_inner(addr, opts))
        .await
        .map_err(|_| ClientError::Timeout("CONNACK"))?
}

async fn connect_inner(addr: SocketAddr, opts: &ConnectOpts<'_>) -> Result<Conn, ClientError> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let mut props = vec![Property::ReceiveMaximum(opts.receive_maximum)];
    if opts.session_expiry > 0 {
        props.push(Property::SessionExpiryInterval(opts.session_expiry));
    }
    let connect = Packet::Connect(Connect {
        protocol: VERSION,
        clean_session: opts.clean,
        keep_alive: opts.keepalive,
        client_id: opts.client_id.to_string(),
        last_will: None,
        username: opts.username.map(str::to_string),
        password: opts.password.map(|p| Bytes::copy_from_slice(p.as_bytes())),
        properties: props.into(),
    });
    write_packet(&mut stream, &connect).await?;
    let mut buf = BytesMut::with_capacity(8192);
    let granted = read_until(&mut stream, &mut buf, |p| match p {
        Packet::ConnAck(ack) if ack.code == 0 => Ok(Some(Granted {
            receive_maximum: ack.properties.receive_maximum().unwrap_or(u16::MAX),
            server_keepalive: server_keepalive(&ack.properties),
            session_present: ack.session_present,
        })),
        Packet::ConnAck(ack) => Err(ClientError::Refused(format!(
            "CONNACK reason 0x{:02x}",
            ack.code
        ))),
        other => Err(ClientError::Refused(format!(
            "expected CONNACK, got {:?}",
            other.packet_type()
        ))),
    })
    .await?;
    Ok(Conn {
        stream,
        granted,
        buf,
    })
}

fn server_keepalive(props: &Properties) -> Option<u16> {
    props.0.iter().find_map(|p| match p {
        Property::ServerKeepAlive(k) => Some(*k),
        _ => None,
    })
}

/// Subscribe to `filter` at `qos` and wait for a successful SUBACK. Returns the
/// deliveries read before the SUBACK (a durable session's queue can start draining as
/// soon as the session resumes); anything after it stays in `conn.buf`.
///
/// # Errors
/// Socket, codec, refusal or timeout.
pub async fn subscribe(
    conn: &mut Conn,
    filter: &str,
    qos: QoS,
    timeout: Duration,
) -> Result<Vec<Publish>, ClientError> {
    let sub = Packet::Subscribe(Subscribe {
        pkid: 1,
        filters: vec![SubscribeFilter {
            path: filter.to_string(),
            qos,
            options: SubscriptionOptions::default(),
        }],
        properties: Properties::new(),
    });
    write_packet(&mut conn.stream, &sub).await?;
    let mut early = Vec::new();
    tokio::time::timeout(
        timeout,
        read_until(&mut conn.stream, &mut conn.buf, |p| match p {
            Packet::SubAck(ack) if ack.pkid == 1 => match ack.return_codes.first() {
                Some(&code) if code < 0x80 => Ok(Some(())),
                code => Err(ClientError::Refused(format!(
                    "SUBACK for {filter} refused: {code:?}"
                ))),
            },
            Packet::Publish(p) => {
                early.push(p);
                Ok(None)
            }
            Packet::PingResp => Ok(None),
            other => Err(ClientError::Refused(format!(
                "expected SUBACK, got {:?}",
                other.packet_type()
            ))),
        }),
    )
    .await
    .map_err(|_| ClientError::Timeout("SUBACK"))??;
    Ok(early)
}

/// The bytes of a DISCONNECT; with `end_session`, it carries session expiry 0 so the
/// broker discards the session.
///
/// # Panics
/// Never: a DISCONNECT with at most one integer property always encodes.
#[must_use]
pub fn disconnect_bytes(end_session: bool) -> Vec<u8> {
    let props: Properties = if end_session {
        vec![Property::SessionExpiryInterval(0)].into()
    } else {
        Properties::new()
    };
    let mut out = Vec::new();
    Packet::Disconnect(Disconnect {
        reason: 0,
        properties: props,
    })
    .encode(&mut out, VERSION)
    .expect("DISCONNECT encodes");
    out
}

/// The bytes of a PINGREQ.
pub const PINGREQ: [u8; 2] = [0xC0, 0x00];

/// The bytes of a PUBACK for `pkid` (the MQTT 5 short form: success, no properties).
#[must_use]
pub fn puback(pkid: u16) -> [u8; 4] {
    let [hi, lo] = pkid.to_be_bytes();
    [0x40, 0x02, hi, lo]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_packets_decode_as_themselves() {
        let mut b = BytesMut::from(&puback(513)[..]);
        assert!(
            matches!(Packet::decode(&mut b, VERSION).unwrap(), Some(Packet::PubAck(a)) if a.pkid == 513)
        );
        let mut b = BytesMut::from(&PINGREQ[..]);
        assert!(matches!(
            Packet::decode(&mut b, VERSION).unwrap(),
            Some(Packet::PingReq)
        ));
        let mut b = BytesMut::from(&disconnect_bytes(true)[..]);
        let Some(Packet::Disconnect(d)) = Packet::decode(&mut b, VERSION).unwrap() else {
            panic!("not a disconnect");
        };
        assert_eq!(d.properties.session_expiry_interval(), Some(0));
    }
}
