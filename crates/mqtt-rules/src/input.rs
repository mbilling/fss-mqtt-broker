//! What a rule reads from its trigger, under EMQX's field names.
//!
//! A message trigger ([`PublishInput`]) is resolved field by field on demand, so a
//! rule that reads `topic` and `qos` never builds the `pub_props` map or renders the
//! peer address. An event trigger ([`EventInput`]) is rare enough to be a plain map.
//!
//! The oracle for every field name, type and value here is EMQX's source:
//! `apps/emqx_rule_engine/src/emqx_rule_events.erl` (emqx/emqx), its `eventmsg_*`
//! builders and `with_basic_columns/3`.

use std::cell::OnceCell;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use mqtt_core::AppProperties;

use crate::value::{Map, Value};
use crate::Input;

/// A client/session event a rule can select `FROM` (`"$events/…"`).
///
/// The set, names and topics are EMQX's (`emqx_rule_events:event_names/0`,
/// `event_topics_enum/0`, `event_name/1`). The events EMQX raises that mqttd does not
/// are listed, as unsupported, in [`EMQX_EVENT_TOPICS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// `$events/client/connected` (also `$events/client_connected`).
    ClientConnected,
    /// `$events/client/disconnected` (also `$events/client_disconnected`).
    ClientDisconnected,
    /// `$events/client/connack` (also `$events/client_connack`): every CONNACK, a
    /// refusal included.
    ClientConnack,
    /// `$events/client/ping`: a PINGREQ. EMQX has no underscore spelling for it.
    ClientPing,
    /// `$events/auth/check_authn_complete` (also
    /// `$events/client_check_authn_complete`).
    CheckAuthnComplete,
    /// `$events/auth/check_authz_complete` (also
    /// `$events/client_check_authz_complete`).
    CheckAuthzComplete,
    /// `$events/session/subscribed` (also `$events/session_subscribed`).
    SessionSubscribed,
    /// `$events/session/unsubscribed` (also `$events/session_unsubscribed`).
    SessionUnsubscribed,
}

/// Every event topic EMQX accepts in a `FROM` — `emqx_rule_events:event_topics_enum/0`,
/// the 5.10+ namespaced topics and then the older underscore ones — with the event
/// mqttd raises for it, `None` where it raises none. A wildcard `FROM "$events/…"`
/// filter is matched against these, as EMQX's `match_event_names/1` matches it with
/// `emqx_topic:match/2`.
pub const EMQX_EVENT_TOPICS: &[(&str, Option<EventKind>)] = &[
    ("$events/sys/alarm_activated", None),
    ("$events/sys/alarm_deactivated", None),
    ("$events/client/connected", Some(EventKind::ClientConnected)),
    (
        "$events/client/disconnected",
        Some(EventKind::ClientDisconnected),
    ),
    ("$events/client/connack", Some(EventKind::ClientConnack)),
    ("$events/client/ping", Some(EventKind::ClientPing)),
    (
        "$events/auth/check_authn_complete",
        Some(EventKind::CheckAuthnComplete),
    ),
    (
        "$events/auth/check_authz_complete",
        Some(EventKind::CheckAuthzComplete),
    ),
    (
        "$events/session/subscribed",
        Some(EventKind::SessionSubscribed),
    ),
    (
        "$events/session/unsubscribed",
        Some(EventKind::SessionUnsubscribed),
    ),
    ("$events/message/delivered", None),
    ("$events/message/acked", None),
    ("$events/message/dropped", None),
    ("$events/message/delivery_dropped", None),
    ("$events/message_transformation/failed", None),
    ("$events/schema_validation/failed", None),
    ("$events/client_connected", Some(EventKind::ClientConnected)),
    (
        "$events/client_disconnected",
        Some(EventKind::ClientDisconnected),
    ),
    ("$events/client_connack", Some(EventKind::ClientConnack)),
    (
        "$events/client_check_authn_complete",
        Some(EventKind::CheckAuthnComplete),
    ),
    (
        "$events/client_check_authz_complete",
        Some(EventKind::CheckAuthzComplete),
    ),
    (
        "$events/session_subscribed",
        Some(EventKind::SessionSubscribed),
    ),
    (
        "$events/session_unsubscribed",
        Some(EventKind::SessionUnsubscribed),
    ),
    ("$events/message_delivered", None),
    ("$events/message_acked", None),
    ("$events/message_dropped", None),
    ("$events/delivery_dropped", None),
    ("$events/message_transformation_failed", None),
    ("$events/schema_validation_failed", None),
];

/// What a wildcard `FROM "$events/…"` filter selects ([`EventKind::matching`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventMatch {
    /// The events mqttd raises that it matches, each once, in [`EventKind::ALL`] order.
    pub kinds: Vec<EventKind>,
    /// The EMQX event topics it matches that mqttd raises no event for.
    pub unsupported: Vec<&'static str>,
}

impl EventKind {
    /// How many kinds there are.
    pub const COUNT: usize = 8;

    /// Every kind, in a fixed order (the index into per-kind tables).
    pub const ALL: [EventKind; Self::COUNT] = [
        EventKind::ClientConnected,
        EventKind::ClientDisconnected,
        EventKind::ClientConnack,
        EventKind::ClientPing,
        EventKind::CheckAuthnComplete,
        EventKind::CheckAuthzComplete,
        EventKind::SessionSubscribed,
        EventKind::SessionUnsubscribed,
    ];

    /// Parse a `FROM` entry naming one event. Both EMQX spellings (5.10+ namespaced and
    /// the older underscore form) are accepted wherever EMQX accepts both
    /// (`emqx_rule_events:event_name/1`).
    #[must_use]
    pub fn from_topic(t: &str) -> Option<Self> {
        EMQX_EVENT_TOPICS
            .iter()
            .find(|(topic, _)| *topic == t)
            .and_then(|(_, kind)| *kind)
    }

    /// The events a wildcard `FROM "$events/…"` filter selects: every EMQX event topic
    /// it matches (`emqx_rule_events:match_event_names/1`), split into the ones mqttd
    /// raises and the ones it does not.
    #[must_use]
    pub fn matching(filter: &str) -> EventMatch {
        let mut m = EventMatch::default();
        for (topic, kind) in EMQX_EVENT_TOPICS {
            if !mqtt_core::topic_matches(filter, topic) {
                continue;
            }
            match kind {
                Some(k) if !m.kinds.contains(k) => m.kinds.push(*k),
                Some(_) => {}
                None => m.unsupported.push(topic),
            }
        }
        m.kinds.sort_by_key(|k| k.index());
        m
    }

    /// The kind an event is named by: its `event` value (`client.connack`) or its topic,
    /// with or without the `$events/` prefix (`client/connack`, `client_connack`).
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        let short = name.strip_prefix("$events/").unwrap_or(name);
        Self::ALL
            .into_iter()
            .find(|k| k.event_name() == name)
            .or_else(|| Self::from_topic(&format!("$events/{short}")))
    }

    /// The event's topic, in EMQX's 5.10+ spelling (`emqx_rule_events:event_topic/1`).
    #[must_use]
    pub fn topic(self) -> &'static str {
        match self {
            Self::ClientConnected => "$events/client/connected",
            Self::ClientDisconnected => "$events/client/disconnected",
            Self::ClientConnack => "$events/client/connack",
            Self::ClientPing => "$events/client/ping",
            Self::CheckAuthnComplete => "$events/auth/check_authn_complete",
            Self::CheckAuthzComplete => "$events/auth/check_authz_complete",
            Self::SessionSubscribed => "$events/session/subscribed",
            Self::SessionUnsubscribed => "$events/session/unsubscribed",
        }
    }

    /// The `event` field's value (EMQX's hook name).
    #[must_use]
    pub fn event_name(self) -> &'static str {
        match self {
            Self::ClientConnected => "client.connected",
            Self::ClientDisconnected => "client.disconnected",
            Self::ClientConnack => "client.connack",
            Self::ClientPing => "client.ping",
            Self::CheckAuthnComplete => "client.check_authn_complete",
            Self::CheckAuthzComplete => "client.check_authz_complete",
            Self::SessionSubscribed => "session.subscribed",
            Self::SessionUnsubscribed => "session.unsubscribed",
        }
    }

    pub(crate) fn index(self) -> usize {
        match self {
            Self::ClientConnected => 0,
            Self::ClientDisconnected => 1,
            Self::ClientConnack => 2,
            Self::ClientPing => 3,
            Self::CheckAuthnComplete => 4,
            Self::CheckAuthzComplete => 5,
            Self::SessionSubscribed => 6,
            Self::SessionUnsubscribed => 7,
        }
    }
}

/// EMQX's name for an MQTT 5 reason code (`emqx_reason_codes:name/1`): what the
/// `reason_code` of `$events/client/connack` says, and the `reason` of
/// `$events/client/disconnected` for a DISCONNECT carrying the code. `0x00` is
/// `success`; a code EMQX does not name is `unknown_error`, as in EMQX.
#[must_use]
pub fn reason_code_name(code: u8) -> &'static str {
    match code {
        0x00 => "success",
        0x01 => "granted_qos1",
        0x02 => "granted_qos2",
        0x04 => "disconnect_with_will_message",
        0x10 => "no_matching_subscribers",
        0x11 => "no_subscription_existed",
        0x18 => "continue_authentication",
        0x19 => "re_authenticate",
        0x80 => "unspecified_error",
        0x81 => "malformed_packet",
        0x82 => "protocol_error",
        0x83 => "implementation_specific_error",
        0x84 => "unsupported_protocol_version",
        0x85 => "client_identifier_not_valid",
        0x86 => "bad_username_or_password",
        0x87 => "not_authorized",
        0x88 => "server_unavailable",
        0x89 => "server_busy",
        0x8A => "banned",
        0x8B => "server_shutting_down",
        0x8C => "bad_authentication_method",
        0x8D => "keepalive_timeout",
        0x8E => "session_taken_over",
        0x8F => "topic_filter_invalid",
        0x90 => "topic_name_invalid",
        0x91 => "packet_identifier_inuse",
        0x92 => "packet_identifier_not_found",
        0x93 => "receive_maximum_exceeded",
        0x94 => "topic_alias_invalid",
        0x95 => "packet_too_large",
        0x96 => "message_rate_too_high",
        0x97 => "quota_exceeded",
        0x98 => "administrative_action",
        0x99 => "payload_format_invalid",
        0x9A => "retain_not_supported",
        0x9B => "qos_not_supported",
        0x9C => "use_another_server",
        0x9D => "server_moved",
        0x9E => "shared_subscriptions_not_supported",
        0x9F => "connection_rate_exceeded",
        0xA0 => "maximum_connect_time",
        0xA1 => "subscription_identifiers_not_supported",
        0xA2 => "wildcard_subscriptions_not_supported",
        _ => "unknown_error",
    }
}

/// The `reason` EMQX gives a connection closed by a DISCONNECT carrying `code`
/// (`emqx_channel:disconnect_reason/1`): `normal` for `0x00`, else the code's
/// [`reason_code_name`]. The same whichever side sent it, and for an MQTT 3.1.1 client
/// the broker closes for the reason it would have sent.
#[must_use]
pub fn disconnect_reason(code: u8) -> &'static str {
    if code == 0 {
        "normal"
    } else {
        reason_code_name(code)
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

/// An address as EMQX prints one (`emqx_utils:ntoa/1`): an IPv4-mapped IPv6 address as
/// the IPv4 address it carries, any other IPv6 address unbracketed.
#[must_use]
pub fn ntoa_ip(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => v6.to_string(),
        },
    }
}

/// `ip:port` as EMQX prints it (`emqx_utils:ntoa/1`): `127.0.0.1:1883`, `::1:1883`.
#[must_use]
pub fn ntoa(addr: SocketAddr) -> String {
    format!("{}:{}", ntoa_ip(addr.ip()), addr.port())
}

fn host(peer: Option<SocketAddr>) -> Value {
    peer.map_or(Value::Undefined, |p| Value::from(ntoa_ip(p.ip())))
}

fn sock_name(peer: Option<SocketAddr>) -> Value {
    peer.map_or(Value::Undefined, |p| Value::from(ntoa(p)))
}

/// EMQX's printed MQTT 5 property map (`emqx_utils_maps:printable_props/1`), the shape of
/// `pub_props`, `conn_props`, `disconn_props`, `sub_props` and `unsub_props`: `rest`
/// under their spec names, `User-Property` as a map (a repeated key keeps its last
/// value) — present, empty, even when there were none — and `User-Property-Pairs`, every
/// pair in wire order as `{"key": …, "value": …}`, when there was at least one.
#[must_use]
pub fn printable_props<K: AsRef<str>, V: AsRef<str>>(
    user: &[(K, V)],
    rest: impl IntoIterator<Item = (&'static str, Value)>,
) -> Map {
    let mut m = Map::new();
    let user_map = Map::from_pairs(
        user.iter()
            .map(|(k, v)| (Arc::from(k.as_ref()), Value::from(v.as_ref())))
            .collect(),
    );
    m.insert("User-Property", Value::from(user_map));
    if !user.is_empty() {
        let pairs: Vec<Value> = user
            .iter()
            .map(|(k, v)| {
                let mut p = Map::with_capacity(2);
                p.insert("key", Value::from(k.as_ref()));
                p.insert("value", Value::from(v.as_ref()));
                Value::from(p)
            })
            .collect();
        m.insert("User-Property-Pairs", Value::from(pairs));
    }
    for (k, v) in rest {
        m.insert(k, v);
    }
    m
}

/// EMQX's `pub_props`: the MQTT 5 properties under their spec names, as
/// [`printable_props`] prints them.
#[must_use]
pub fn pub_props(props: &AppProperties, message_expiry: Option<u32>) -> Map {
    let mut rest = Vec::new();
    if let Some(n) = props.payload_format {
        rest.push(("Payload-Format-Indicator", Value::Int(i64::from(n))));
    }
    if let Some(n) = message_expiry {
        rest.push(("Message-Expiry-Interval", Value::Int(i64::from(n))));
    }
    if let Some(s) = &props.content_type {
        rest.push(("Content-Type", Value::from(s.as_str())));
    }
    if let Some(s) = &props.response_topic {
        rest.push(("Response-Topic", Value::from(s.as_str())));
    }
    if let Some(b) = &props.correlation_data {
        rest.push(("Correlation-Data", Value::from_bytes(b)));
    }
    printable_props(&props.user_properties, rest)
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

    /// `flags`. `dup` is always `false`: EMQX runs the publish hook on
    /// `emqx_message:clean_dup(Msg)` (`emqx_broker:publish/1`), so a rule never sees
    /// the DUP flag a publisher set — a resent message is the same message.
    fn flags(&self) -> Value {
        let mut f = Map::with_capacity(2);
        f.insert("dup", Value::Bool(false));
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

/// What a client's CONNECT established, as EMQX's `conninfo` holds it for the
/// connection events.
#[derive(Debug, Clone)]
pub struct ConnInfo {
    /// 4 for MQTT 3.1.1, 5 for MQTT 5.
    pub proto_ver: u8,
    /// The keepalive, seconds.
    pub keepalive: u16,
    /// Clean start (MQTT 5) / clean session (MQTT 3.1.1).
    pub clean_start: bool,
    /// The Session Expiry Interval, seconds.
    pub expiry_interval: u32,
    /// How many unacknowledged `QoS` 1/2 messages the broker may send it: the
    /// client's Receive Maximum, capped by the node's limit.
    pub receive_maximum: u16,
    /// The CONNECT's properties, as [`printable_props`] prints them.
    pub conn_props: Map,
}

impl ConnInfo {
    /// A sample connection: MQTT 5, keepalive 60, a clean start with no session
    /// expiry, the default Receive Maximum and no properties.
    #[must_use]
    pub fn sample() -> Self {
        Self {
            proto_ver: 5,
            keepalive: 60,
            clean_start: true,
            expiry_interval: 0,
            receive_maximum: u16::MAX,
            conn_props: printable_props::<&str, &str>(&[], []),
        }
    }
}

/// A client/session event, as a rule's `FROM "$events/…"` sees it.
#[derive(Debug, Clone)]
pub struct EventInput {
    kind: EventKind,
    fields: Map,
}

fn int(n: impl Into<i64>) -> Value {
    Value::Int(n.into())
}

impl EventInput {
    /// What every event has: EMQX's `with_basic_columns/3` (`event`, `timestamp`,
    /// `node`) and the `clientid` and `username` every client event's builder adds —
    /// `username` even when the client sent none, as `undefined`, so `SELECT *` shows
    /// `"username":"undefined"` as EMQX's does.
    fn base(kind: EventKind, c: &ClientInfo) -> Map {
        let mut m = Map::with_capacity(20);
        m.insert("event", Value::from(kind.event_name()));
        m.insert("clientid", Value::from(c.clientid));
        m.insert("username", opt_str(c.username));
        m.insert("timestamp", Value::Int(now_ms()));
        m.insert("node", Value::from(c.node));
        m
    }

    /// `client_attrs`, on the events whose EMQX builder sets it: `{}`, since mqttd has
    /// no client attributes.
    fn attrs(m: &mut Map) {
        m.insert("client_attrs", Value::from(Map::new()));
    }

    /// `peername`, `sockname`, `proto_name` and `proto_ver`, which every connection
    /// event carries.
    fn conn_base(m: &mut Map, c: &ClientInfo, proto_ver: u8) {
        insert_addr(m, "peername", c.peer);
        insert_addr(m, "sockname", c.sockname);
        // "MQTT" for both 3.1.1 and 5; only MQTT 3.1 ("MQIsdp") differs, and mqttd
        // does not speak it.
        m.insert("proto_name", Value::from("MQTT"));
        m.insert("proto_ver", int(proto_ver));
    }

    /// The kind of event.
    #[must_use]
    pub fn kind(&self) -> EventKind {
        self.kind
    }

    /// `$events/client/connected` (`eventmsg_connected/2`). `expiry_interval` is in
    /// seconds here (EMQX divides its milliseconds by 1000 for this event only).
    #[must_use]
    pub fn client_connected(c: &ClientInfo, conn: &ConnInfo, connected_at_ms: i64) -> Self {
        let kind = EventKind::ClientConnected;
        let mut m = Self::base(kind, c);
        Self::conn_base(&mut m, c, conn.proto_ver);
        m.insert("keepalive", int(conn.keepalive));
        m.insert("clean_start", Value::Bool(conn.clean_start));
        m.insert("receive_maximum", int(conn.receive_maximum));
        m.insert("expiry_interval", int(conn.expiry_interval));
        // mqttd refuses the bridge-mode protocol levels (0x83/0x84) at the codec, so no
        // client it serves ever set the bit EMQX reads this from.
        m.insert("is_bridge", Value::Bool(false));
        m.insert("conn_props", Value::from(conn.conn_props.clone()));
        m.insert("connected_at", Value::Int(connected_at_ms));
        Self::attrs(&mut m);
        Self { kind, fields: m }
    }

    /// `$events/client/disconnected` (`eventmsg_disconnected/3`). `reason` uses
    /// EMQX's vocabulary (`normal`, `takenover`, `discarded`, `kicked`,
    /// `keepalive_timeout`, `tcp_closed`, …); `disconn_props` are the properties of
    /// the client's DISCONNECT, printed (only `User-Property: {}` when it sent none).
    #[must_use]
    pub fn client_disconnected(
        c: &ClientInfo,
        proto_ver: u8,
        reason: &str,
        disconn_props: Map,
        connected_at_ms: i64,
    ) -> Self {
        let kind = EventKind::ClientDisconnected;
        let mut m = Self::base(kind, c);
        Self::conn_base(&mut m, c, proto_ver);
        m.insert("reason", Value::from(reason));
        m.insert("disconn_props", Value::from(disconn_props));
        m.insert("connected_at", Value::Int(connected_at_ms));
        let at = m.get("timestamp").cloned().unwrap_or_default();
        m.insert("disconnected_at", at);
        Self::attrs(&mut m);
        Self { kind, fields: m }
    }

    /// `$events/client/connack` (`eventmsg_connack/2`). `reason_code` is EMQX's name for
    /// the code ([`reason_code_name`]: `success`, `not_authorized`, …) — the MQTT 5
    /// name for an MQTT 3.1.1 client too. `connected_at` is set only for a success.
    /// Unlike `client.connected`, `expiry_interval` is in milliseconds here, as EMQX's
    /// `conninfo` holds it, and there is no `client_attrs`.
    #[must_use]
    pub fn client_connack(
        c: &ClientInfo,
        conn: &ConnInfo,
        reason_code: &str,
        connected_at_ms: Option<i64>,
    ) -> Self {
        let kind = EventKind::ClientConnack;
        let mut m = Self::base(kind, c);
        Self::conn_base(&mut m, c, conn.proto_ver);
        m.insert("reason_code", Value::from(reason_code));
        m.insert("clean_start", Value::Bool(conn.clean_start));
        m.insert("keepalive", int(conn.keepalive));
        m.insert(
            "expiry_interval",
            Value::Int(i64::from(conn.expiry_interval) * 1000),
        );
        if let Some(at) = connected_at_ms {
            m.insert("connected_at", Value::Int(at));
        }
        m.insert("conn_props", Value::from(conn.conn_props.clone()));
        Self { kind, fields: m }
    }

    /// `$events/client/ping` (`eventmsg_ping/2`): the connection's facts, with
    /// `expiry_interval` in milliseconds as for `client.connack`, and no
    /// `connected_at` or `client_attrs`.
    #[must_use]
    pub fn client_ping(c: &ClientInfo, conn: &ConnInfo) -> Self {
        let kind = EventKind::ClientPing;
        let mut m = Self::base(kind, c);
        Self::conn_base(&mut m, c, conn.proto_ver);
        m.insert("clean_start", Value::Bool(conn.clean_start));
        m.insert("keepalive", int(conn.keepalive));
        m.insert(
            "expiry_interval",
            Value::Int(i64::from(conn.expiry_interval) * 1000),
        );
        m.insert("conn_props", Value::from(conn.conn_props.clone()));
        Self { kind, fields: m }
    }

    /// `$events/auth/check_authn_complete` (`eventmsg_check_authn_complete/2`):
    /// `reason_code` is `success` or why it failed (`bad_username_or_password`,
    /// `not_authorized`, …); `is_anonymous` whether no credential was checked;
    /// `is_superuser` is always `false` — mqttd has no superusers.
    #[must_use]
    pub fn check_authn_complete(c: &ClientInfo, reason_code: &str, is_anonymous: bool) -> Self {
        let kind = EventKind::CheckAuthnComplete;
        let mut m = Self::base(kind, c);
        insert_addr(&mut m, "peername", c.peer);
        m.insert("reason_code", Value::from(reason_code));
        m.insert("is_anonymous", Value::Bool(is_anonymous));
        m.insert("is_superuser", Value::Bool(false));
        Self::attrs(&mut m);
        Self { kind, fields: m }
    }

    /// `$events/auth/check_authz_complete` (`eventmsg_check_authz_complete/5`):
    /// `action` is `publish` or `subscribe`, `result` `allow` or `deny`, and
    /// `authz_source` what decided — EMQX's source names: `file` for a rule of the ACL
    /// file, `default` when no rule matched (`authorization.no_match`).
    #[must_use]
    pub fn check_authz_complete(
        c: &ClientInfo,
        topic: &str,
        action: &str,
        authz_source: &str,
        allowed: bool,
    ) -> Self {
        let kind = EventKind::CheckAuthzComplete;
        let mut m = Self::base(kind, c);
        insert_addr(&mut m, "peername", c.peer);
        if let Some(p) = c.peer {
            m.insert("peerhost", Value::from(ntoa_ip(p.ip())));
        }
        m.insert("topic", Value::from(topic));
        m.insert("action", Value::from(action));
        m.insert("authz_source", Value::from(authz_source));
        m.insert(
            "result",
            Value::from(if allowed { "allow" } else { "deny" }),
        );
        Self::attrs(&mut m);
        Self { kind, fields: m }
    }

    /// `session.subscribed` and `session.unsubscribed` (`eventmsg_sub_or_unsub/4`).
    fn sub_or_unsub(
        kind: EventKind,
        c: &ClientInfo,
        topic: &str,
        qos: u8,
        props_key: &str,
        props: Map,
    ) -> Self {
        let mut m = Self::base(kind, c);
        if let Some(p) = c.peer {
            m.insert("peerhost", Value::from(ntoa_ip(p.ip())));
        }
        insert_addr(&mut m, "peername", c.peer);
        m.insert("topic", Value::from(topic));
        m.insert("qos", int(qos));
        m.insert(props_key, Value::from(props));
        Self::attrs(&mut m);
        Self { kind, fields: m }
    }

    /// `$events/session/subscribed`, one per granted filter: `qos` is the granted
    /// `QoS`, `sub_props` the SUBSCRIBE's properties, printed.
    #[must_use]
    pub fn session_subscribed(c: &ClientInfo, topic: &str, qos: u8, sub_props: Map) -> Self {
        Self::sub_or_unsub(
            EventKind::SessionSubscribed,
            c,
            topic,
            qos,
            "sub_props",
            sub_props,
        )
    }

    /// `$events/session/unsubscribed`, one per removed filter: `qos` is the `QoS` the
    /// removed subscription had been granted, `unsub_props` the UNSUBSCRIBE's
    /// properties, printed.
    #[must_use]
    pub fn session_unsubscribed(c: &ClientInfo, topic: &str, qos: u8, unsub_props: Map) -> Self {
        Self::sub_or_unsub(
            EventKind::SessionUnsubscribed,
            c,
            topic,
            qos,
            "unsub_props",
            unsub_props,
        )
    }

    /// A sample of `kind` for a dry run (`mqttd --rule-test`, the admin API), EMQX's
    /// "SQL test" for event rules: the client's own fields, the given `topic` and `qos`
    /// for the subscribe and authorization events, and plausible values for the rest
    /// ([`ConnInfo::sample`], a `normal` disconnect one second after connecting, a
    /// `success` CONNACK and authentication, a publish the ACL file allowed).
    #[must_use]
    pub fn sample(kind: EventKind, c: &ClientInfo, topic: &str, qos: u8) -> Self {
        let now = now_ms();
        let conn = ConnInfo::sample();
        let no_props = || printable_props::<&str, &str>(&[], []);
        match kind {
            EventKind::ClientConnected => Self::client_connected(c, &conn, now),
            EventKind::ClientDisconnected => {
                Self::client_disconnected(c, conn.proto_ver, "normal", no_props(), now - 1000)
            }
            EventKind::ClientConnack => Self::client_connack(c, &conn, "success", Some(now)),
            EventKind::ClientPing => Self::client_ping(c, &conn),
            EventKind::CheckAuthnComplete => Self::check_authn_complete(c, "success", false),
            EventKind::CheckAuthzComplete => {
                Self::check_authz_complete(c, topic, "publish", "file", true)
            }
            EventKind::SessionSubscribed => Self::session_subscribed(c, topic, qos, no_props()),
            EventKind::SessionUnsubscribed => Self::session_unsubscribed(c, topic, qos, no_props()),
        }
    }
}

fn insert_addr(m: &mut Map, key: &str, addr: Option<SocketAddr>) {
    if let Some(a) = addr {
        m.insert(key, Value::from(ntoa(a)));
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
