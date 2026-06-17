//! Per-connection MQTT 5.0 topic-alias maps (ADR 0011).
//!
//! Aliases are a connection-edge concern: inbound PUBLISH packets are resolved to
//! full topic names before they reach the hub, and outbound ones are rewritten to
//! use aliases after they leave it. Nothing about aliases is persisted, gossiped,
//! or seen by routing. Both maps are owned by the connection task and die with it,
//! which is exactly the per-network-connection reset the spec requires.

use mqtt_codec::{packet::Publish, Property};
use std::collections::HashMap;

/// A topic-alias protocol violation; closing the connection is the response
/// (ADR 0011 §2). The MQTT 5.0 reason code is Topic Alias Invalid (`0x94`).
#[derive(Debug, PartialEq, Eq)]
pub struct AliasError;

/// Inbound (client → server) topic-alias map for one connection.
#[derive(Debug)]
pub struct InboundAliases {
    /// The Topic Alias Maximum we advertised; `0` disables inbound aliases.
    max: u16,
    map: HashMap<u16, String>,
}

impl InboundAliases {
    /// Create a resolver advertising `max` as the highest alias we accept.
    #[must_use]
    pub fn new(max: u16) -> Self {
        Self {
            max,
            map: HashMap::new(),
        }
    }

    /// Resolve a PUBLISH's `(topic, alias)` to the effective full topic name,
    /// updating the map when the PUBLISH establishes a mapping.
    ///
    /// # Errors
    /// [`AliasError`] on a protocol violation: an alias of `0`, an alias above the
    /// advertised maximum, an alias when none were advertised, or a reference
    /// (empty topic) to an alias that was never set.
    pub fn resolve(&mut self, topic: &str, alias: Option<u16>) -> Result<String, AliasError> {
        let Some(alias) = alias else {
            // No alias in play: the topic stands on its own.
            return Ok(topic.to_string());
        };
        if alias == 0 || alias > self.max {
            return Err(AliasError);
        }
        if topic.is_empty() {
            // A reference to a previously-established mapping.
            self.map.get(&alias).cloned().ok_or(AliasError)
        } else {
            // Establish (or replace) the mapping and use the carried topic.
            self.map.insert(alias, topic.to_string());
            Ok(topic.to_string())
        }
    }
}

/// Outbound (server → client) topic-alias map for one connection.
#[derive(Debug)]
pub struct OutboundAliases {
    /// The client's advertised Topic Alias Maximum; `0` disables outbound aliases.
    max: u16,
    map: HashMap<String, u16>,
    /// The next alias to hand out (`1..=max`); once it exceeds `max` the table is
    /// full and further new topics are sent un-aliased (assign-until-full, ADR 0011 §3).
    next: u16,
}

impl OutboundAliases {
    /// Create an assigner bounded by the client's advertised `max`.
    #[must_use]
    pub fn new(max: u16) -> Self {
        Self {
            max,
            map: HashMap::new(),
            next: 1,
        }
    }

    /// Rewrite `publish` in place to use a topic alias when possible. A no-op when
    /// the client advertised no maximum or the topic is empty.
    pub fn apply(&mut self, publish: &mut Publish) {
        if self.max == 0 || publish.topic.is_empty() {
            return;
        }
        if let Some(&alias) = self.map.get(&publish.topic) {
            // Established mapping: reference it with an empty topic name.
            publish.topic.clear();
            set_alias(publish, alias);
        } else if self.next <= self.max {
            // Free slot: establish a new mapping, keeping the topic name this once.
            let alias = self.next;
            self.next += 1;
            self.map.insert(publish.topic.clone(), alias);
            set_alias(publish, alias);
        }
        // Table full and topic unknown: leave the full topic name, no alias.
    }
}

/// Attach a Topic Alias property to a PUBLISH (the hub never sets one, so there is
/// nothing to replace).
fn set_alias(publish: &mut Publish, alias: u16) {
    publish.properties.0.push(Property::TopicAlias(alias));
}

#[cfg(test)]
mod tests {
    use super::{AliasError, InboundAliases, OutboundAliases};
    use bytes::Bytes;
    use mqtt_codec::{packet::Publish, Properties, QoS};

    fn publish(topic: &str) -> Publish {
        Publish {
            properties: Properties::new(),
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.to_string(),
            pkid: None,
            payload: Bytes::from_static(b"x"),
        }
    }

    fn alias_of(p: &Publish) -> Option<u16> {
        p.properties.topic_alias()
    }

    #[test]
    fn inbound_passes_through_when_no_alias() {
        let mut a = InboundAliases::new(10);
        assert_eq!(a.resolve("sensors/t", None).unwrap(), "sensors/t");
    }

    #[test]
    fn inbound_establishes_then_references() {
        let mut a = InboundAliases::new(10);
        // Set: topic + alias.
        assert_eq!(a.resolve("sensors/t", Some(3)).unwrap(), "sensors/t");
        // Reference: empty topic + same alias resolves back to the topic.
        assert_eq!(a.resolve("", Some(3)).unwrap(), "sensors/t");
    }

    #[test]
    fn inbound_replaces_mapping_on_reset() {
        let mut a = InboundAliases::new(10);
        a.resolve("first", Some(1)).unwrap();
        a.resolve("second", Some(1)).unwrap(); // re-set alias 1
        assert_eq!(a.resolve("", Some(1)).unwrap(), "second");
    }

    #[test]
    fn inbound_rejects_out_of_range_zero_and_unknown() {
        let mut a = InboundAliases::new(5);
        assert_eq!(a.resolve("t", Some(0)), Err(AliasError), "alias 0 invalid");
        assert_eq!(a.resolve("t", Some(6)), Err(AliasError), "above maximum");
        assert_eq!(
            a.resolve("", Some(2)),
            Err(AliasError),
            "unmapped reference"
        );
    }

    #[test]
    fn inbound_with_zero_max_rejects_any_alias() {
        let mut a = InboundAliases::new(0);
        assert_eq!(a.resolve("t", Some(1)), Err(AliasError));
        // ...but a plain topic is still fine.
        assert_eq!(a.resolve("t", None).unwrap(), "t");
    }

    #[test]
    fn outbound_disabled_when_max_zero() {
        let mut o = OutboundAliases::new(0);
        let mut p = publish("a/b");
        o.apply(&mut p);
        assert_eq!(p.topic, "a/b");
        assert_eq!(alias_of(&p), None);
    }

    #[test]
    fn outbound_assigns_then_reuses() {
        let mut o = OutboundAliases::new(10);
        // First send of a topic: full name + a freshly assigned alias.
        let mut p1 = publish("a/b");
        o.apply(&mut p1);
        assert_eq!(p1.topic, "a/b");
        assert_eq!(alias_of(&p1), Some(1));

        // Second send of the same topic: empty name, same alias.
        let mut p2 = publish("a/b");
        o.apply(&mut p2);
        assert_eq!(p2.topic, "");
        assert_eq!(alias_of(&p2), Some(1));

        // A different topic gets the next alias.
        let mut p3 = publish("c/d");
        o.apply(&mut p3);
        assert_eq!(p3.topic, "c/d");
        assert_eq!(alias_of(&p3), Some(2));
    }

    #[test]
    fn outbound_stops_assigning_when_full_but_keeps_existing() {
        let mut o = OutboundAliases::new(1);
        let mut p1 = publish("a");
        o.apply(&mut p1);
        assert_eq!(alias_of(&p1), Some(1), "the one slot is assigned");

        // Table full: a new topic is sent un-aliased.
        let mut p2 = publish("b");
        o.apply(&mut p2);
        assert_eq!(p2.topic, "b");
        assert_eq!(alias_of(&p2), None);

        // The established mapping still works.
        let mut p3 = publish("a");
        o.apply(&mut p3);
        assert_eq!(p3.topic, "");
        assert_eq!(alias_of(&p3), Some(1));
    }
}
