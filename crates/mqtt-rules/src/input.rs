//! What a rule reads from its trigger, under EMQX's field names.
//!
//! A message trigger ([`PublishInput`]) is resolved field by field on demand, so a
//! rule that reads `topic` and `qos` never builds the `pub_props` map or renders the
//! peer address. An event trigger ([`EventInput`]) is rare enough to be a plain map.

use std::cell::OnceCell;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use mqtt_core::AppProperties;

use crate::value::{Map, Value};
use crate::Input;

/// A client/session event a rule can select `FROM` (`"$events/…"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// `$events/client/connected` (also `$events/client_connected`).
    ClientConnected,
    /// `$events/client/disconnected` (also `$events/client_disconnected`).
    ClientDisconnected,
    /// `$events/session/subscribed` (also `$events/session_subscribed`).
    SessionSubscribed,
    /// `$events/session/unsubscribed` (also `$events/session_unsubscribed`).
    SessionUnsubscribed,
}

impl EventKind {
    /// Every kind, in a fixed order (the index into per-kind tables).
    pub const ALL: [EventKind; 4] = [
        EventKind::ClientConnected,
        EventKind::ClientDisconnected,
        EventKind::SessionSubscribed,
        EventKind::SessionUnsubscribed,
    ];

    /// Parse a `FROM` entry. Both EMQX spellings (5.10+ namespaced and the older
    /// underscore form) are accepted.
    #[must_use]
    pub fn from_topic(t: &str) -> Option<Self> {
        Some(match t.strip_prefix("$events/")? {
            "client/connected" | "client_connected" => Self::ClientConnected,
            "client/disconnected" | "client_disconnected" => Self::ClientDisconnected,
            "session/subscribed" | "session_subscribed" => Self::SessionSubscribed,
            "session/unsubscribed" | "session_unsubscribed" => Self::SessionUnsubscribed,
            _ => return None,
        })
    }

    /// The `event` field's value (EMQX's hook name).
    #[must_use]
    pub fn event_name(self) -> &'static str {
        match self {
            Self::ClientConnected => "client.connected",
            Self::ClientDisconnected => "client.disconnected",
            Self::SessionSubscribed => "session.subscribed",
            Self::SessionUnsubscribed => "session.unsubscribed",
        }
    }

    pub(crate) fn index(self) -> usize {
        match self {
            Self::ClientConnected => 0,
            Self::ClientDisconnected => 1,
            Self::SessionSubscribed => 2,
            Self::SessionUnsubscribed => 3,
        }
    }
}

/// Milliseconds since the Unix epoch.
#[must_use]
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// EMQX's message `id`: 128 bits rendered as 32 upper-case hex digits — here the
/// microsecond clock, a per-process salt and a counter, so ids are unique per node
/// without a random draw per message. Generated only when a rule reads `id`.
fn message_id() -> Arc<str> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static SALT: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    let salt = *SALT.get_or_init(|| {
        let mut b = [0u8; 4];
        let _ = aws_lc_rs::rand::fill(&mut b);
        u32::from_le_bytes(b)
    });
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX));
    // Wrapping is fine: uniqueness comes from the clock + counter together.
    #[allow(clippy::cast_possible_truncation)]
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed) as u32;
    Arc::from(format!("{micros:016X}{salt:08X}{seq:08X}"))
}

fn opt_str(s: Option<&str>) -> Value {
    s.map_or(Value::Undefined, Value::from)
}

fn host(peer: Option<SocketAddr>) -> Value {
    peer.map_or(Value::Undefined, |p| Value::from(p.ip().to_string()))
}

fn sock_name(peer: Option<SocketAddr>) -> Value {
    peer.map_or(Value::Undefined, |p| Value::from(p.to_string()))
}

/// EMQX's `pub_props`: the MQTT 5 properties under their spec names. `User-Property`
/// is a map (a repeated key keeps its last value) and `User-Property-Pairs` keeps
/// every pair in wire order.
#[must_use]
pub fn pub_props(props: &AppProperties, message_expiry: Option<u32>) -> Map {
    let mut m = Map::new();
    let user = Map::from_pairs(
        props
            .user_properties
            .iter()
            .map(|(k, v)| (Arc::from(k.as_str()), Value::from(v.as_str())))
            .collect(),
    );
    m.insert("User-Property", Value::from(user));
    if !props.user_properties.is_empty() {
        let pairs: Vec<Value> = props
            .user_properties
            .iter()
            .map(|(k, v)| {
                let mut p = Map::with_capacity(2);
                p.insert("key", Value::from(k.as_str()));
                p.insert("value", Value::from(v.as_str()));
                Value::from(p)
            })
            .collect();
        m.insert("User-Property-Pairs", Value::from(pairs));
    }
    if let Some(n) = props.payload_format {
        m.insert("Payload-Format-Indicator", Value::Int(i64::from(n)));
    }
    if let Some(n) = message_expiry {
        m.insert("Message-Expiry-Interval", Value::Int(i64::from(n)));
    }
    if let Some(s) = &props.content_type {
        m.insert("Content-Type", Value::from(s.as_str()));
    }
    if let Some(s) = &props.response_topic {
        m.insert("Response-Topic", Value::from(s.as_str()));
    }
    if let Some(b) = &props.correlation_data {
        m.insert("Correlation-Data", Value::from_bytes(b));
    }
    m
}

/// A published message, as a rule's `FROM "<topic filter>"` sees it.
#[derive(Debug)]
pub struct PublishInput<'a> {
    /// The publisher's client id.
    pub clientid: &'a str,
    /// The CONNECT username, when one was sent.
    pub username: Option<&'a str>,
    /// The publisher's address, when the listener knows it.
    pub peer: Option<SocketAddr>,
    /// The topic (aliases resolved).
    pub topic: &'a str,
    /// The payload.
    pub payload: &'a Bytes,
    /// The publish `QoS`.
    pub qos: u8,
    /// The RETAIN flag.
    pub retain: bool,
    /// The DUP flag.
    pub dup: bool,
    /// MQTT 5 application properties.
    pub props: &'a AppProperties,
    /// MQTT 5 Message Expiry Interval.
    pub message_expiry: Option<u32>,
    /// When the broker received it (ms since the epoch); `None` reads the clock when
    /// a rule first asks, so a publish no rule selects never does.
    pub received_at_ms: Option<i64>,
    /// This node's id.
    pub node: &'a str,
    id: OnceCell<Arc<str>>,
    received: OnceCell<i64>,
    /// `timestamp`, read once per message so every reference agrees.
    stamped: OnceCell<i64>,
    /// `pub_props`, built once per message however many references read it.
    pub_props: OnceCell<Value>,
}

impl<'a> PublishInput<'a> {
    /// A message with the given essentials; the optional fields start unset
    /// (assign the public fields to fill them in).
    #[must_use]
    pub fn new(
        clientid: &'a str,
        topic: &'a str,
        payload: &'a Bytes,
        qos: u8,
        props: &'a AppProperties,
    ) -> Self {
        Self {
            clientid,
            username: None,
            peer: None,
            topic,
            payload,
            qos,
            retain: false,
            dup: false,
            props,
            message_expiry: None,
            received_at_ms: None,
            node: "",
            id: OnceCell::new(),
            received: OnceCell::new(),
            stamped: OnceCell::new(),
            pub_props: OnceCell::new(),
        }
    }

    fn received_at(&self) -> i64 {
        self.received_at_ms
            .unwrap_or_else(|| *self.received.get_or_init(now_ms))
    }

    fn flags(&self) -> Value {
        let mut f = Map::with_capacity(2);
        f.insert("dup", Value::Bool(self.dup));
        f.insert("retain", Value::Bool(self.retain));
        Value::from(f)
    }
}

const PUBLISH_FIELDS: &[&str] = &[
    "id",
    "clientid",
    "username",
    "payload",
    "peerhost",
    "peername",
    "topic",
    "qos",
    "flags",
    "pub_props",
    "publish_received_at",
    "client_attrs",
    "event",
    "timestamp",
    "node",
];

impl Input for PublishInput<'_> {
    fn field(&self, name: &str) -> Value {
        match name {
            "id" => Value::Str(self.id.get_or_init(message_id).clone()),
            "clientid" => Value::from(self.clientid),
            "username" => opt_str(self.username),
            "payload" => Value::from_bytes(self.payload),
            "peerhost" => host(self.peer),
            "peername" => sock_name(self.peer),
            "topic" => Value::from(self.topic),
            "qos" => Value::Int(i64::from(self.qos)),
            "flags" => self.flags(),
            "pub_props" => self
                .pub_props
                .get_or_init(|| Value::from(pub_props(self.props, self.message_expiry)))
                .clone(),
            "publish_received_at" => Value::Int(self.received_at()),
            "client_attrs" => Value::from(Map::new()),
            "event" => Value::from("message.publish"),
            // EMQX's event time: when the rule engine first looked, never before the
            // message arrived, and the same for every reference in every rule.
            "timestamp" => Value::Int(
                *self
                    .stamped
                    .get_or_init(|| now_ms().max(self.received_at())),
            ),
            "node" => Value::from(self.node),
            _ => Value::Undefined,
        }
    }

    fn all_fields(&self) -> Map {
        let mut m = Map::with_capacity(PUBLISH_FIELDS.len() + 1);
        for f in PUBLISH_FIELDS {
            let v = self.field(f);
            if !v.is_undefined() {
                m.insert(*f, v);
            }
        }
        m
    }

    fn payload(&self) -> Option<&Bytes> {
        Some(self.payload)
    }

    fn user_properties(&self) -> &[(String, String)] {
        &self.props.user_properties
    }
}

/// Who an event is about.
#[derive(Debug, Clone, Copy)]
pub struct ClientInfo<'a> {
    /// The client id.
    pub clientid: &'a str,
    /// The CONNECT username, when one was sent.
    pub username: Option<&'a str>,
    /// The client's address.
    pub peer: Option<SocketAddr>,
    /// The listener address it connected to.
    pub sockname: Option<SocketAddr>,
    /// This node's id.
    pub node: &'a str,
}

/// A client/session event, as a rule's `FROM "$events/…"` sees it.
#[derive(Debug, Clone)]
pub struct EventInput {
    kind: EventKind,
    fields: Map,
}

impl EventInput {
    fn base(kind: EventKind, c: &ClientInfo) -> Map {
        let mut m = Map::with_capacity(16);
        m.insert("event", Value::from(kind.event_name()));
        m.insert("clientid", Value::from(c.clientid));
        if let Some(u) = c.username {
            m.insert("username", Value::from(u));
        }
        m.insert("timestamp", Value::Int(now_ms()));
        m.insert("node", Value::from(c.node));
        m.insert("client_attrs", Value::from(Map::new()));
        m
    }

    /// The kind of event.
    #[must_use]
    pub fn kind(&self) -> EventKind {
        self.kind
    }

    /// `$events/client/connected`.
    #[must_use]
    pub fn client_connected(
        c: &ClientInfo,
        proto_ver: u8,
        keepalive: u16,
        clean_start: bool,
        expiry_interval: u32,
        connected_at_ms: i64,
    ) -> Self {
        let kind = EventKind::ClientConnected;
        let mut m = Self::base(kind, c);
        insert_addr(&mut m, "peername", c.peer);
        insert_addr(&mut m, "sockname", c.sockname);
        m.insert("proto_name", Value::from("MQTT"));
        m.insert("proto_ver", Value::Int(i64::from(proto_ver)));
        m.insert("keepalive", Value::Int(i64::from(keepalive)));
        m.insert("clean_start", Value::Bool(clean_start));
        m.insert("expiry_interval", Value::Int(i64::from(expiry_interval)));
        m.insert("is_bridge", Value::Bool(false));
        m.insert("connected_at", Value::Int(connected_at_ms));
        m.insert("conn_props", Value::from(Map::new()));
        Self { kind, fields: m }
    }

    /// `$events/client/disconnected`. `reason` uses EMQX's vocabulary (`normal`,
    /// `takenover`, `keepalive_timeout`, `tcp_closed`, `kicked`, …).
    #[must_use]
    pub fn client_disconnected(c: &ClientInfo, reason: &str, connected_at_ms: i64) -> Self {
        let kind = EventKind::ClientDisconnected;
        let mut m = Self::base(kind, c);
        insert_addr(&mut m, "peername", c.peer);
        insert_addr(&mut m, "sockname", c.sockname);
        m.insert("reason", Value::from(reason));
        m.insert("connected_at", Value::Int(connected_at_ms));
        let at = m.get("timestamp").cloned().unwrap_or_default();
        m.insert("disconnected_at", at);
        m.insert("disconn_props", Value::from(Map::new()));
        Self { kind, fields: m }
    }

    /// `$events/session/subscribed`, one per granted filter.
    #[must_use]
    pub fn session_subscribed(c: &ClientInfo, topic: &str, qos: u8) -> Self {
        let kind = EventKind::SessionSubscribed;
        let mut m = Self::base(kind, c);
        if let Some(p) = c.peer {
            m.insert("peerhost", Value::from(p.ip().to_string()));
        }
        m.insert("topic", Value::from(topic));
        m.insert("qos", Value::Int(i64::from(qos)));
        m.insert("sub_props", Value::from(Map::new()));
        Self { kind, fields: m }
    }

    /// `$events/session/unsubscribed`, one per removed filter.
    #[must_use]
    pub fn session_unsubscribed(c: &ClientInfo, topic: &str) -> Self {
        let kind = EventKind::SessionUnsubscribed;
        let mut m = Self::base(kind, c);
        if let Some(p) = c.peer {
            m.insert("peerhost", Value::from(p.ip().to_string()));
        }
        m.insert("topic", Value::from(topic));
        m.insert("unsub_props", Value::from(Map::new()));
        Self { kind, fields: m }
    }

    /// A sample of `kind` for `mqttd --rule-test`, EMQX's "SQL test" for event rules:
    /// the client's own fields, the given `topic` and `qos` for the subscribe events,
    /// and plausible values for the rest (MQTT 5, keepalive 60, a clean start, a
    /// `normal` disconnect one second after connecting).
    #[must_use]
    pub fn sample(kind: EventKind, c: &ClientInfo, topic: &str, qos: u8) -> Self {
        let now = now_ms();
        match kind {
            EventKind::ClientConnected => Self::client_connected(c, 5, 60, true, 0, now),
            EventKind::ClientDisconnected => Self::client_disconnected(c, "normal", now - 1000),
            EventKind::SessionSubscribed => Self::session_subscribed(c, topic, qos),
            EventKind::SessionUnsubscribed => Self::session_unsubscribed(c, topic),
        }
    }
}

fn insert_addr(m: &mut Map, key: &str, addr: Option<SocketAddr>) {
    if let Some(a) = addr {
        m.insert(key, Value::from(a.to_string()));
    }
}

impl Input for EventInput {
    fn field(&self, name: &str) -> Value {
        self.fields.get(name).cloned().unwrap_or_default()
    }

    fn all_fields(&self) -> Map {
        self.fields.clone()
    }
}
