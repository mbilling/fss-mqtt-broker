//! A rule's actions: `republish` and `console`, with EMQX's argument names and
//! defaults.
//!
//! | `republish` arg | default | |
//! |---|---|---|
//! | `topic` | — (required) | template |
//! | `qos` | `"${qos}"` | 0/1/2 or a placeholder; a missing value is 0 |
//! | `retain` | `"${retain}"` | bool or a placeholder; a missing value is false |
//! | `payload` | `"${payload}"` | template; empty = the whole output as JSON |
//! | `user_properties` | `"${user_properties}"` | a placeholder naming a map; `${pub_props.'User-Property'}` = the publisher's, in wire order |
//! | `mqtt_properties` | none | `Payload-Format-Indicator`, `Message-Expiry-Interval`, `Content-Type`, `Response-Topic`, `Correlation-Data` |
//! | `direct_dispatch` | — | accepted; mqttd *always* dispatches directly (a republished message never re-enters the rule engine) |
//!
//! There are no external sinks (Kafka, HTTP, databases): ADR 0083 keeps them out, and
//! an action naming one is refused at load.

use bytes::Bytes;
use mqtt_core::AppProperties;

use crate::template::{lookup, Step, Template};
use crate::value::{Map, Value};
use crate::{EvalError, Input};

/// One configured action.
#[derive(Debug, Clone)]
pub(crate) enum Action {
    Republish(Box<RepublishSpec>),
    Console,
}

/// A `qos` / `retain` argument: a literal, or one placeholder.
#[derive(Debug, Clone)]
enum Simple {
    Const(Value),
    Var(Vec<Step>),
}

#[derive(Debug, Clone)]
enum UserProps {
    /// No user properties.
    None,
    /// The publisher's own, in wire order (`${pub_props.'User-Property'}`).
    Original,
    /// A selected map (or EMQX's `[{key, value}]` pairs list).
    Var(Vec<Step>),
}

#[derive(Debug, Clone, Copy)]
enum Prop {
    PayloadFormat,
    MessageExpiry,
    ContentType,
    ResponseTopic,
    CorrelationData,
}

/// A parsed `republish`.
#[derive(Debug, Clone)]
pub(crate) struct RepublishSpec {
    topic: Template,
    qos: Simple,
    retain: Simple,
    payload: Template,
    user_properties: UserProps,
    props: Vec<(Prop, Template)>,
}

/// A message an action asks the broker to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Republish {
    /// Destination topic (validated: no wildcards, not empty, not `$share/…`).
    pub topic: String,
    /// The rendered payload.
    pub payload: Bytes,
    /// 0, 1 or 2.
    pub qos: u8,
    /// Whether to retain it.
    pub retain: bool,
    /// MQTT 5 application properties for the new message.
    pub app: AppProperties,
    /// MQTT 5 Message Expiry Interval, when the action set one.
    pub message_expiry: Option<u32>,
}

/// What an action produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Publish this message.
    Republish(Republish),
    /// Log this rule output (the `console` action; the JSON of the selected fields).
    Console(String),
}

fn simple(v: Option<&toml::Value>, default: &str, what: &str) -> Result<Simple, String> {
    match v {
        None => simple(Some(&toml::Value::String(default.into())), default, what),
        Some(toml::Value::Integer(n)) => Ok(Simple::Const(Value::Int(*n))),
        Some(toml::Value::Boolean(b)) => Ok(Simple::Const(Value::Bool(*b))),
        Some(toml::Value::String(s)) => {
            let t = Template::parse(s)?;
            if let Some(path) = t.sole_var() {
                Ok(Simple::Var(path.to_vec()))
            } else if t.is_literal() {
                Ok(Simple::Const(Value::from(s.as_str())))
            } else {
                Err(format!(
                    "{what} must be a literal or exactly one placeholder, not \"{s}\""
                ))
            }
        }
        Some(other) => Err(format!(
            "{what} must be a number, boolean or string, not {other}"
        )),
    }
}

/// A republish's `qos`: 0, 1 or 2, as a number or its text; undefined is 0.
fn qos_of(v: &Value) -> Result<u8, EvalError> {
    match v {
        Value::Undefined => Ok(0),
        Value::Int(n @ 0..=2) => Ok(u8::try_from(*n).unwrap_or(0)),
        v => match v.as_str() {
            Some("0") => Ok(0),
            Some("1") => Ok(1),
            Some("2") => Ok(2),
            _ => Err(EvalError::new(format!(
                "qos must be 0, 1 or 2, got {}",
                v.to_text()?
            ))),
        },
    }
}

/// A republish's `retain`: a boolean, 0/1, or their text; undefined is false.
fn retain_of(v: &Value) -> Result<bool, EvalError> {
    match v {
        Value::Undefined | Value::Bool(false) | Value::Int(0) => Ok(false),
        Value::Bool(true) | Value::Int(1) => Ok(true),
        v => match v.as_str() {
            Some("true") => Ok(true),
            Some("false") => Ok(false),
            _ => Err(EvalError::new(format!(
                "retain must be a boolean, got {}",
                v.to_text()?
            ))),
        },
    }
}

/// A `qos` or `retain` written as a literal is checked when the file loads, the way
/// a rendered one is checked per message: a literal that can never be valid would
/// otherwise fail the action on every message.
fn checked(s: Simple, check: impl Fn(&Value) -> Result<(), EvalError>) -> Result<Simple, String> {
    if let Simple::Const(v) = &s {
        check(v).map_err(|e| e.to_string())?;
    }
    Ok(s)
}

/// Parse one entry of a rule's `actions` list.
pub(crate) fn parse_action(v: &toml::Value, warnings: &mut Vec<String>) -> Result<Action, String> {
    let table = match v {
        toml::Value::Table(t) => t,
        toml::Value::String(id) => {
            return Err(format!(
                "action \"{id}\" references a data bridge / sink; mqttd has no external sinks — \
                 republish to a topic and consume it with a $share group (docs/INTEGRATION.md)"
            ))
        }
        other => {
            return Err(format!(
                "an action is a table with a `function`, not {other}"
            ))
        }
    };
    for k in table.keys() {
        if k != "function" && k != "args" {
            return Err(format!(
                "unknown action key `{k}` (expected `function`, `args`)"
            ));
        }
    }
    let function = table
        .get("function")
        .and_then(toml::Value::as_str)
        .ok_or("an action needs `function = \"republish\"` or `function = \"console\"`")?;
    let empty = toml::map::Map::new();
    let args = match table.get("args") {
        None => &empty,
        Some(toml::Value::Table(t)) => t,
        Some(_) => return Err("`args` must be a table".into()),
    };
    match function {
        "console" => Ok(Action::Console),
        "republish" => parse_republish(args, warnings).map(|r| Action::Republish(Box::new(r))),
        other => Err(format!(
            "unsupported action function \"{other}\" (mqttd supports republish and console)"
        )),
    }
}

fn parse_republish(
    args: &toml::map::Map<String, toml::Value>,
    warnings: &mut Vec<String>,
) -> Result<RepublishSpec, String> {
    const KNOWN: &[&str] = &[
        "topic",
        "qos",
        "retain",
        "payload",
        "user_properties",
        "mqtt_properties",
        "direct_dispatch",
    ];
    for k in args.keys() {
        if !KNOWN.contains(&k.as_str()) {
            return Err(format!("unknown republish argument `{k}`"));
        }
    }
    let topic = match args.get("topic") {
        Some(toml::Value::String(s)) if !s.is_empty() => Template::parse(s)?,
        _ => return Err("republish needs a non-empty `topic`".into()),
    };
    let payload = match args.get("payload") {
        None => Template::parse("${payload}")?,
        Some(toml::Value::String(s)) if s.is_empty() => Template::this(),
        Some(toml::Value::String(s)) => Template::parse(s)?,
        Some(_) => return Err("`payload` must be a string template".into()),
    };
    let user_properties = match args.get("user_properties") {
        None => UserProps::Var(vec![Step::Key("user_properties".into())]),
        Some(toml::Value::String(s)) => {
            let t = s.trim();
            if t == "${pub_props.'User-Property'}" || t == "${.pub_props.'User-Property'}" {
                UserProps::Original
            } else if t.is_empty() {
                UserProps::None
            } else if let Some(path) = Template::parse(t)?.sole_var() {
                UserProps::Var(path.to_vec())
            } else {
                // EMQX silently discards a non-placeholder here; say so instead.
                warnings.push(format!(
                    "user_properties \"{s}\" is not a single ${{…}} placeholder and is ignored \
                     (as in EMQX)"
                ));
                UserProps::None
            }
        }
        Some(_) => return Err("`user_properties` must be a string placeholder".into()),
    };
    let mut props = Vec::new();
    if let Some(v) = args.get("mqtt_properties") {
        let toml::Value::Table(t) = v else {
            return Err("`mqtt_properties` must be a table".into());
        };
        for (k, v) in t {
            let prop = match k.as_str() {
                "Payload-Format-Indicator" => Prop::PayloadFormat,
                "Message-Expiry-Interval" => Prop::MessageExpiry,
                "Content-Type" => Prop::ContentType,
                "Response-Topic" => Prop::ResponseTopic,
                "Correlation-Data" => Prop::CorrelationData,
                other => {
                    return Err(format!(
                        "unsupported mqtt_properties key \"{other}\" (Payload-Format-Indicator, \
                         Message-Expiry-Interval, Content-Type, Response-Topic, Correlation-Data)"
                    ))
                }
            };
            let text = match v {
                toml::Value::String(s) => s.clone(),
                toml::Value::Integer(n) => n.to_string(),
                _ => return Err(format!("mqtt_properties.{k} must be a string or integer")),
            };
            props.push((prop, Template::parse(&text)?));
        }
    }
    match args.get("direct_dispatch") {
        None | Some(toml::Value::Boolean(true)) => {}
        Some(toml::Value::Boolean(false)) => warnings.push(
            "direct_dispatch = false is accepted but has no effect: mqttd never feeds a \
             republished message back into the rule engine, so a rule cannot loop (ADR 0083)"
                .into(),
        ),
        Some(_) => return Err("`direct_dispatch` must be a boolean".into()),
    }
    Ok(RepublishSpec {
        topic,
        qos: checked(simple(args.get("qos"), "${qos}", "qos")?, |v| {
            qos_of(v).map(drop)
        })?,
        retain: checked(simple(args.get("retain"), "${retain}", "retain")?, |v| {
            retain_of(v).map(drop)
        })?,
        payload,
        user_properties,
        props,
    })
}

impl RepublishSpec {
    /// Render this action for one rule output.
    pub(crate) fn render(&self, out: &Map, input: &dyn Input) -> Result<Republish, EvalError> {
        let topic = String::from_utf8(self.topic.render(out)?)
            .map_err(|_| EvalError::new("rendered topic is not UTF-8"))?;
        if !mqtt_core::valid_topic_name(&topic) || topic.len() > usize::from(u16::MAX) {
            return Err(EvalError::new(format!(
                "rendered topic \"{topic}\" is not a valid topic name (empty, too long, or \
                 containing + # or NUL)"
            )));
        }
        if topic.starts_with("$share/") {
            return Err(EvalError::new(format!(
                "rendered topic \"{topic}\" is a shared-subscription filter, not a topic"
            )));
        }
        let qos = qos_of(&resolve(&self.qos, out))?;
        let retain = retain_of(&resolve(&self.retain, out))?;
        let payload = Bytes::from(self.payload.render(out)?);
        let mut app = AppProperties::default();
        match &self.user_properties {
            UserProps::None => {}
            UserProps::Original => app.user_properties = input.user_properties().to_vec(),
            UserProps::Var(path) => match lookup(out, path) {
                Value::Undefined => {}
                Value::Map(m) => {
                    for (k, v) in m.iter() {
                        app.user_properties.push((k.to_string(), v.to_text()?));
                    }
                }
                // EMQX's `User-Property-Pairs` shape: [{key, value}, …].
                Value::Array(pairs) => {
                    for p in pairs.iter() {
                        if let Value::Map(m) = p {
                            if let (Some(k), Some(v)) = (m.get("key"), m.get("value")) {
                                app.user_properties.push((k.to_text()?, v.to_text()?));
                            }
                        }
                    }
                }
                v => {
                    return Err(EvalError::new(format!(
                        "user_properties must name a map, got a {}",
                        v.type_name()
                    )))
                }
            },
        }
        let mut message_expiry = None;
        // A property that renders badly is dropped, as EMQX drops it (with a debug
        // log there); the message still goes out.
        for (prop, t) in &self.props {
            let Ok(raw) = t.render(out) else { continue };
            let text = String::from_utf8(raw.clone()).ok();
            match prop {
                Prop::PayloadFormat => {
                    app.payload_format = text
                        .and_then(|s| s.trim().parse::<u8>().ok())
                        .filter(|n| *n <= 1);
                }
                Prop::MessageExpiry => {
                    message_expiry = text.and_then(|s| s.trim().parse::<u32>().ok());
                }
                Prop::ContentType => app.content_type = text,
                Prop::ResponseTopic => {
                    app.response_topic = text.filter(|t| mqtt_core::valid_topic_name(t));
                }
                Prop::CorrelationData => app.correlation_data = Some(Bytes::from(raw)),
            }
        }
        Ok(Republish {
            topic,
            payload,
            qos,
            retain,
            app,
            message_expiry,
        })
    }
}

fn resolve(s: &Simple, out: &Map) -> Value {
    match s {
        Simple::Const(v) => v.clone(),
        Simple::Var(path) => lookup(out, path),
    }
}
