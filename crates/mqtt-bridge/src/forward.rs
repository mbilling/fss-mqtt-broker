//! The pure forwarding policy — direction, remap, and loop bounding
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §4–6).
//!
//! This module decides, for a message that arrived on one side, **where (if anywhere) it
//! is forwarded** — with no I/O, so the security-critical rules are exhaustively unit
//! testable. The guarantees it encodes:
//!
//! - **Directionality (§4).** A message from the *local* side is only ever forwarded to an
//!   upstream (via an `out`/`both` rule); a message from an *upstream* is only ever
//!   forwarded to local (via that upstream's `in`/`both` rule). A one-way rule therefore
//!   **cannot** produce a forward in its closed direction — independent of, and in addition
//!   to, the fact that the engine never subscribes on the closed side.
//! - **No upstream↔upstream.** Upstreams only ever exchange traffic *through* local, never
//!   directly — the bridge is hub-and-spoke.
//! - **Loop bounding (§6).** A message whose hop count has reached `hop_count_limit` is
//!   dropped; otherwise it is forwarded with the count incremented.

use mqtt_codec::properties::{Properties, Property};
use mqtt_core::topic_matches;

use crate::config::{BridgeConfig, HaMode, Remap};

/// The MQTT 5 user-property key carrying how many fss bridges a message has traversed.
pub const HOP_COUNT_KEY: &str = "fss-bridge-hop-count";

/// Read the `fss-bridge-hop-count` user property as a number — 0 if absent or unparseable
/// (an absent count means this is the first bridge to see the message, §6).
#[must_use]
pub fn read_hop_count(props: &Properties) -> u32 {
    props
        .0
        .iter()
        .find_map(|p| match p {
            Property::UserProperty(k, v) if k == HOP_COUNT_KEY => v.parse().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

/// Build the outgoing property set: forward the publisher's User Properties unaltered
/// **except** the hop count, which is (re)set to `hop`. Non-user properties (topic alias,
/// etc.) are connection-scoped and dropped.
#[must_use]
pub fn set_hop_count(props: &Properties, hop: u32) -> Properties {
    let mut out: Vec<Property> = props
        .0
        .iter()
        .filter(|p| !matches!(p, Property::UserProperty(k, _) if k == HOP_COUNT_KEY))
        .filter(|p| matches!(p, Property::UserProperty(_, _)))
        .cloned()
        .collect();
    out.push(Property::UserProperty(
        HOP_COUNT_KEY.to_string(),
        hop.to_string(),
    ));
    Properties(out)
}

/// Which side of the bridge a message is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// The local cluster.
    Local,
    /// The upstream at this index in [`BridgeConfig::upstreams`].
    Upstream(usize),
}

/// One resolved forward: send to `dest` on topic `topic` at `qos`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Forward {
    /// Where to send it.
    pub dest: Side,
    /// The (possibly remapped) destination topic.
    pub topic: String,
    /// The delivery `QoS` wire value (`0` or `1`; a rule's `2` is downgraded, §7).
    pub qos: u8,
    /// Whether this rule permits the RETAIN flag to cross (issue #189): the source retain bit
    /// is forwarded only when the matching rule's `outgoing_retain` is set (the default).
    pub retain_ok: bool,
}

/// Downgrade a rule `QoS` to what the bridge delivers: `2` becomes `1` (at-least-once across
/// two independent brokers, §7); `0`/`1` are unchanged.
#[must_use]
pub fn delivered_qos(rule_qos: u8) -> u8 {
    if rule_qos >= 2 {
        1
    } else {
        rule_qos
    }
}

/// Apply a rule's remap to a source `topic`: strip `strip_prefix` if present, then prepend
/// `prefix`. A remap keeps a forwarded message from matching the rule that would send it
/// straight back (§6).
#[must_use]
pub fn apply_remap(remap: Option<&Remap>, topic: &str) -> String {
    let Some(remap) = remap else {
        return topic.to_string();
    };
    let stripped = match &remap.strip_prefix {
        Some(sp) => topic.strip_prefix(sp.as_str()).unwrap_or(topic),
        None => topic,
    };
    match &remap.prefix {
        Some(p) => format!("{p}{stripped}"),
        None => stripped.to_string(),
    }
}

/// Whether the instance `instance` of `total` owns `topic` under partitioned HA (ADR 0059).
///
/// `total <= 1` → always true (a lone instance owns everything). Otherwise ownership is a
/// stable hash of the topic mod `total`, so **exactly one** instance forwards each topic —
/// giving exactly-once inbound and per-topic ordering (one owner = one ordered stream) without
/// any coordinator or shared state. Every instance still *subscribes* the full filter; this
/// decides only whether to *forward*.
#[must_use]
pub fn owns(topic: &str, total: usize, instance: usize) -> bool {
    if total <= 1 {
        return true;
    }
    fnv1a(topic.as_bytes()) % total as u64 == instance as u64
}

/// FNV-1a, 64-bit — a stable, dependency-free, non-cryptographic hash for topic partitioning.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Decide where a message that arrived on `source` with topic `topic` and the given
/// `hop_count` is forwarded. Empty = dropped (hop limit reached, or no rule matches).
///
/// `hop_count` is the value read from the inbound `fss-bridge-hop-count` user property (0
/// if absent). The returned forwards carry the *destination* topic; the caller stamps the
/// outgoing hop count as `hop_count + 1`.
#[must_use]
pub fn plan_forwards(
    cfg: &BridgeConfig,
    source: Side,
    topic: &str,
    hop_count: u32,
) -> Vec<Forward> {
    // Loop bounding (§6): refuse to forward a message that has already traversed the limit.
    if hop_count >= cfg.hop_count_limit {
        return Vec::new();
    }
    let mut forwards = Vec::new();
    match source {
        // Local-origin: only ever out to upstreams, via out/both rules.
        Side::Local => {
            for (i, up) in cfg.upstreams.iter().enumerate() {
                for rule in &up.rules {
                    if rule.direction.allows_out() && topic_matches(&rule.filter, topic) {
                        forwards.push(Forward {
                            dest: Side::Upstream(i),
                            topic: apply_remap(rule.remap.as_ref(), topic),
                            qos: delivered_qos(rule.qos),
                            retain_ok: rule.outgoing_retain,
                        });
                    }
                }
            }
        }
        // Upstream-origin: only ever in to local, via that upstream's in/both rules.
        Side::Upstream(i) => {
            if let Some(up) = cfg.upstreams.get(i) {
                for rule in &up.rules {
                    if rule.direction.allows_in() && topic_matches(&rule.filter, topic) {
                        forwards.push(Forward {
                            dest: Side::Local,
                            topic: apply_remap(rule.remap.as_ref(), topic),
                            qos: delivered_qos(rule.qos),
                            retain_ok: rule.outgoing_retain,
                        });
                    }
                }
            }
        }
    }
    forwards
}

/// The filters the **local** connection must subscribe to: every `out`/`both` rule's filter
/// across all upstreams (the only way local-origin messages reach the bridge). Each filter
/// is paired with the `QoS` to subscribe at. A one-way `in` rule contributes nothing here —
/// the closed direction is never subscribed (§4).
///
/// Under `ha = "shared"` (only) each filter is wrapped as `$share/<group>/<filter>` so ≥2
/// instances load-balance the local stream (ADR 0025 §5). Under the default `partitioned`
/// mode the subscription is **plain** — dedup is by topic ownership ([`owns`]), not `$share`,
/// which also lets retained state cross (a `$share` sub receives no retained messages). The
/// share wrapper affects only the *subscription*; inbound publishes arrive on the real topic,
/// which [`plan_forwards`] matches against the bare rule filter.
#[must_use]
pub fn local_subscriptions(cfg: &BridgeConfig) -> Vec<(String, u8)> {
    let use_share = cfg.ha == HaMode::Shared && !cfg.share_group.is_empty();
    let mut subs = Vec::new();
    for up in &cfg.upstreams {
        for rule in &up.rules {
            if rule.direction.allows_out() {
                let filter = if use_share {
                    format!("$share/{}/{}", cfg.share_group, rule.filter)
                } else {
                    rule.filter.clone()
                };
                subs.push((filter, delivered_qos(rule.qos)));
            }
        }
    }
    subs
}

/// The filters **upstream `i`** must subscribe to: its `in`/`both` rule filters. A one-way
/// `out` rule contributes nothing — the bridge never subscribes on the upstream for it, so
/// the reverse path is never opened (§4).
#[must_use]
pub fn upstream_subscriptions(cfg: &BridgeConfig, i: usize) -> Vec<(String, u8)> {
    let mut subs = Vec::new();
    if let Some(up) = cfg.upstreams.get(i) {
        for rule in &up.rules {
            if rule.direction.allows_in() {
                subs.push((rule.filter.clone(), delivered_qos(rule.qos)));
            }
        }
    }
    subs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(toml: &str) -> BridgeConfig {
        BridgeConfig::parse_toml(toml).unwrap()
    }

    // A two-upstream config: upstream "a" is one-way out, "b" is one-way in, plus a
    // bidirectional remap rule on "a".
    fn sample() -> BridgeConfig {
        cfg(r#"
            hop_count_limit = 3
            share_group = ""
            [local]
            url = "local:1883"

            [[upstreams]]
            name = "a"
            url = "a:1883"
            [[upstreams.rules]]
            direction = "out"
            filter = "telemetry/#"
            qos = 1
            remap = { strip_prefix = "telemetry/", prefix = "org/telemetry/" }
            [[upstreams.rules]]
            direction = "both"
            filter = "shared/+"

            [[upstreams]]
            name = "b"
            url = "b:1883"
            [[upstreams.rules]]
            direction = "in"
            filter = "commands/#"
            qos = 2
        "#)
    }

    #[test]
    fn delivered_qos_downgrades_two_to_one() {
        assert_eq!(delivered_qos(0), 0);
        assert_eq!(delivered_qos(1), 1);
        assert_eq!(delivered_qos(2), 1);
    }

    #[test]
    fn remap_strips_then_prefixes() {
        let r = Remap {
            strip_prefix: Some("telemetry/".into()),
            prefix: Some("org/telemetry/".into()),
        };
        assert_eq!(apply_remap(Some(&r), "telemetry/x/y"), "org/telemetry/x/y");
        // A topic that does not start with the strip prefix is just prefixed.
        assert_eq!(apply_remap(Some(&r), "other/z"), "org/telemetry/other/z");
        // No remap → identity.
        assert_eq!(apply_remap(None, "a/b"), "a/b");
    }

    #[test]
    fn a_local_message_forwards_out_with_remap_and_qos_downgrade() {
        let c = sample();
        // telemetry/# (out, qos1, remap) → upstream a as org/telemetry/...
        let f = plan_forwards(&c, Side::Local, "telemetry/room/temp", 0);
        assert_eq!(
            f,
            vec![Forward {
                dest: Side::Upstream(0),
                topic: "org/telemetry/room/temp".into(),
                qos: 1,
                retain_ok: true,
            }]
        );
    }

    #[test]
    fn a_one_way_out_rule_never_forwards_in() {
        let c = sample();
        // Upstream "a" (index 0) sends on telemetry/# — but that rule is `out` only, so an
        // upstream-origin message must NOT be forwarded to local. (And the bridge never
        // subscribes upstream to it; this is the routing-side half of that guarantee.)
        let f = plan_forwards(&c, Side::Upstream(0), "telemetry/room/temp", 0);
        assert!(f.is_empty(), "a one-way out rule must never leak inbound");
    }

    #[test]
    fn a_one_way_in_rule_never_forwards_out() {
        let c = sample();
        // Upstream "b" (index 1) is `in` only. A LOCAL message on commands/# must not be
        // forwarded out to b.
        let f = plan_forwards(&c, Side::Local, "commands/reboot", 0);
        assert!(f.is_empty(), "a one-way in rule must never leak outbound");
        // But an upstream-b message on commands/# IS forwarded in to local (qos 2→1).
        let f = plan_forwards(&c, Side::Upstream(1), "commands/reboot", 0);
        assert_eq!(
            f,
            vec![Forward {
                dest: Side::Local,
                topic: "commands/reboot".into(),
                qos: 1,
                retain_ok: true,
            }]
        );
    }

    #[test]
    fn a_both_rule_forwards_either_way() {
        let c = sample();
        // shared/+ is `both` on upstream a (index 0), no remap.
        let out = plan_forwards(&c, Side::Local, "shared/x", 0);
        assert_eq!(
            out,
            vec![Forward {
                dest: Side::Upstream(0),
                topic: "shared/x".into(),
                qos: 0,
                retain_ok: true,
            }]
        );
        let inn = plan_forwards(&c, Side::Upstream(0), "shared/x", 0);
        assert_eq!(
            inn,
            vec![Forward {
                dest: Side::Local,
                topic: "shared/x".into(),
                qos: 0,
                retain_ok: true,
            }]
        );
    }

    #[test]
    fn retain_crosses_by_default_and_the_escape_suppresses_it() {
        // Default: outgoing_retain is true, so a forward permits the retain flag (#189).
        let c = cfg(r#"
            share_group = ""
            [local]
            url = "local:1883"
            [[upstreams]]
            name = "a"
            url = "a:1883"
            [[upstreams.rules]]
            direction = "out"
            filter = "sensors/#"
        "#);
        let f = plan_forwards(&c, Side::Local, "sensors/x", 0);
        assert_eq!(f.len(), 1);
        assert!(f[0].retain_ok, "retain crosses by default");

        // The escape: outgoing_retain = false strips the retain flag on egress.
        let c = cfg(r#"
            share_group = ""
            [local]
            url = "local:1883"
            [[upstreams]]
            name = "a"
            url = "a:1883"
            [[upstreams.rules]]
            direction = "out"
            filter = "sensors/#"
            outgoing_retain = false
        "#);
        let f = plan_forwards(&c, Side::Local, "sensors/x", 0);
        assert_eq!(f.len(), 1);
        assert!(
            !f[0].retain_ok,
            "outgoing_retain=false suppresses the retain flag"
        );
    }

    #[test]
    fn ownership_partitions_every_topic_to_exactly_one_instance() {
        // A lone instance owns everything.
        assert!(owns("a/b/c", 1, 0));
        assert!(owns("anything", 1, 0));

        // With total=3, every topic is owned by exactly one of the three instances, and the
        // decision is stable (same topic → same owner every call).
        for i in 0..1000u32 {
            let topic = format!("dev/{i}/state");
            let owners: Vec<usize> = (0..3).filter(|k| owns(&topic, 3, *k)).collect();
            assert_eq!(owners.len(), 1, "topic {topic} must have exactly one owner");
            assert!(owns(&topic, 3, owners[0]), "ownership must be stable");
        }
    }

    #[test]
    fn ownership_spreads_load_across_instances() {
        // Over many topics all instances get a non-trivial share (not everything on one).
        let total = 4;
        let mut counts = [0usize; 4];
        for i in 0..4000u32 {
            let topic = format!("dev/{i}/state");
            let owner = (0..total).find(|k| owns(&topic, total, *k)).unwrap();
            counts[owner] += 1;
        }
        for (k, c) in counts.iter().enumerate() {
            assert!(*c > 500, "instance {k} owns too few topics: {c}");
        }
    }

    #[test]
    fn the_hop_limit_drops_a_message_at_the_limit() {
        let c = sample(); // hop_count_limit = 3
                          // Below the limit: forwarded.
        assert!(!plan_forwards(&c, Side::Local, "shared/x", 2).is_empty());
        // At the limit: dropped.
        assert!(plan_forwards(&c, Side::Local, "shared/x", 3).is_empty());
        // Above the limit: dropped.
        assert!(plan_forwards(&c, Side::Local, "shared/x", 9).is_empty());
    }

    #[test]
    fn an_upstream_message_never_forwards_to_another_upstream() {
        let c = sample();
        // No matter the topic, an upstream-origin message only ever targets Local.
        for topic in ["telemetry/x", "shared/x", "commands/y", "anything"] {
            for f in plan_forwards(&c, Side::Upstream(0), topic, 0) {
                assert_eq!(
                    f.dest,
                    Side::Local,
                    "upstream traffic must hub through local"
                );
            }
            for f in plan_forwards(&c, Side::Upstream(1), topic, 0) {
                assert_eq!(f.dest, Side::Local);
            }
        }
    }

    #[test]
    fn hop_count_reads_default_zero_and_increments_preserving_other_props() {
        // Absent → 0.
        assert_eq!(read_hop_count(&Properties::new()), 0);
        // Present → parsed; other user properties preserved on set, hop replaced.
        let props = Properties(vec![
            Property::UserProperty("trace".into(), "abc".into()),
            Property::UserProperty(HOP_COUNT_KEY.into(), "2".into()),
            Property::TopicAlias(5), // a connection-scoped prop, must be dropped on forward
        ]);
        assert_eq!(read_hop_count(&props), 2);
        let next = set_hop_count(&props, 3);
        // The trace property survives; the hop is now 3; the topic alias is gone.
        assert_eq!(
            next.0,
            vec![
                Property::UserProperty("trace".into(), "abc".into()),
                Property::UserProperty(HOP_COUNT_KEY.into(), "3".into()),
            ]
        );
    }

    #[test]
    fn subscriptions_follow_direction() {
        let c = sample();
        // Local subscribes to every out/both filter: telemetry/# (out) + shared/+ (both).
        let mut local = local_subscriptions(&c);
        local.sort();
        assert_eq!(
            local,
            vec![("shared/+".to_string(), 0), ("telemetry/#".to_string(), 1)]
        );
        // Upstream a (0) subscribes to its in/both filters: only shared/+ (both).
        assert_eq!(
            upstream_subscriptions(&c, 0),
            vec![("shared/+".to_string(), 0)]
        );
        // Upstream b (1) subscribes to commands/# (in, qos 2→1).
        assert_eq!(
            upstream_subscriptions(&c, 1),
            vec![("commands/#".to_string(), 1)]
        );
        // The closed direction is never subscribed: upstream a is NOT subscribed to
        // telemetry/# (its out-only rule), and local is NOT subscribed to commands/#.
        assert!(!upstream_subscriptions(&c, 0)
            .iter()
            .any(|(f, _)| f == "telemetry/#"));
        assert!(!local_subscriptions(&c)
            .iter()
            .any(|(f, _)| f == "commands/#"));
    }

    #[test]
    fn local_subscriptions_wrap_in_share_only_under_shared_ha() {
        // Under ha = "shared", local subscriptions wrap in $share so ≥2 instances load-balance.
        let c = cfg(r#"
            ha = "shared"
            share_group = "edge-bridges"
            [local]
            url = "local:1883"
            [[upstreams]]
            name = "a"
            url = "a:1883"
            [[upstreams.rules]]
            direction = "out"
            filter = "telemetry/#"
        "#);
        assert_eq!(
            local_subscriptions(&c),
            vec![("$share/edge-bridges/telemetry/#".to_string(), 0)]
        );
        // Under the DEFAULT (partitioned) mode the subscription is PLAIN even with a
        // share_group set — dedup is by topic ownership, and a plain sub is what lets retained
        // state cross (a $share sub receives no retained messages).
        let d = cfg(r#"
            share_group = "edge-bridges"
            [local]
            url = "local:1883"
            [[upstreams]]
            name = "a"
            url = "a:1883"
            [[upstreams.rules]]
            direction = "out"
            filter = "t/#"
        "#);
        assert_eq!(local_subscriptions(&d), vec![("t/#".to_string(), 0)]);
    }
}
