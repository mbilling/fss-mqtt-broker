//! Bridge configuration model and validation
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §3).
//!
//! A TOML document declares **one local-cluster connection** and **N upstreams**, each
//! upstream carrying a list of **topic mapping rules**. Forwarding is **deny-by-default**:
//! only a topic matching a configured rule crosses the boundary, and only in that rule's
//! configured `direction` — the headline security control (§4). Validation rejects the
//! clear footguns (a zero hop limit, a malformed topic filter, an mTLS half-identity,
//! both inline and file passwords) before any connection is opened.

use std::collections::HashSet;

use serde::Deserialize;

/// The whole bridge configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeConfig {
    /// The connection to the local cluster (a node address or a service VIP).
    pub local: Endpoint,
    /// The external brokers to bridge to. Empty is valid (a no-op bridge).
    #[serde(default)]
    pub upstreams: Vec<Upstream>,
    /// The loop-bounding hop limit (§6): a message whose `fss-bridge-hop-count` reaches
    /// this is dropped. Default 8; must be ≥ 1.
    #[serde(default = "default_hop_limit")]
    pub hop_count_limit: u32,
    /// The cluster-side shared-subscription group for HA (§5): ≥2 bridge instances with the
    /// same group load-balance the local stream (dedup for free). Default `fss-bridge`; set
    /// empty to disable sharing (a single non-shared instance). Each instance still needs a
    /// distinct `local.client_id` (a persistent session is per client).
    #[serde(default = "default_share_group")]
    pub share_group: String,
}

fn default_hop_limit() -> u32 {
    8
}

fn default_share_group() -> String {
    "fss-bridge".to_string()
}

/// A connection to one broker (the local cluster, or an upstream).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    /// `host:port` to connect to.
    pub url: String,
    /// MQTT client id. Empty → the engine derives a stable one.
    #[serde(default)]
    pub client_id: String,
    /// TLS/mTLS settings; absent → plain TCP (only safe on a trusted/loopback hop).
    #[serde(default)]
    pub tls: Option<Tls>,
    /// Optional username (the least-privilege bridge account, §8).
    #[serde(default)]
    pub username: Option<String>,
    /// Inline password — discouraged; prefer `password_file`. Mutually exclusive with it.
    #[serde(default)]
    pub password: Option<String>,
    /// Path to a file holding the password (read at startup; never logged).
    #[serde(default)]
    pub password_file: Option<String>,
}

/// TLS material for an endpoint. A client certificate + key together select mTLS (§8); a
/// CA alone is server-authentication only.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// PEM bundle of CAs trusted to sign the broker's (server) certificate.
    pub ca: String,
    /// Client certificate chain (PEM) — this bridge's mTLS identity. With `key`.
    #[serde(default)]
    pub cert: Option<String>,
    /// Client private key (PEM). With `cert`.
    #[serde(default)]
    pub key: Option<String>,
}

/// One external broker with its mapping rules.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    /// A stable, unique name for this upstream (used in logs, metrics, audit).
    pub name: String,
    /// The connection to this upstream.
    #[serde(flatten)]
    pub endpoint: Endpoint,
    /// The topic mapping rules. Forwarding is deny-by-default: only matching topics cross.
    #[serde(default)]
    pub rules: Vec<Rule>,
}

/// Which way a rule forwards. A one-way rule (`Out`/`In`) is a primary security control:
/// the engine never opens the reverse path for it (§4).
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// Local cluster → upstream only.
    Out,
    /// Upstream → local cluster only.
    In,
    /// Both directions.
    Both,
}

impl Direction {
    /// Whether this rule forwards local→upstream.
    #[must_use]
    pub fn allows_out(self) -> bool {
        matches!(self, Direction::Out | Direction::Both)
    }
    /// Whether this rule forwards upstream→local.
    #[must_use]
    pub fn allows_in(self) -> bool {
        matches!(self, Direction::In | Direction::Both)
    }
}

/// One topic mapping rule on an upstream.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// The allowed direction(s) of flow.
    pub direction: Direction,
    /// The topic filter selecting which messages this rule forwards.
    pub filter: String,
    /// Optional topic remap applied when forwarding (strip a prefix, add a prefix).
    #[serde(default)]
    pub remap: Option<Remap>,
    /// The `QoS` to forward at (`0..=2`; the engine downgrades 2→1, §7). Default 0.
    #[serde(default)]
    pub qos: u8,
}

/// A topic remap: strip `strip_prefix` from the source topic (if present), then prepend
/// `prefix`. A remap keeps a forwarded message from matching the rule that would send it
/// straight back (the structural loop defence, §6).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Remap {
    /// A prefix stripped from the source topic before forwarding, if it starts with it.
    #[serde(default)]
    pub strip_prefix: Option<String>,
    /// A prefix prepended to the (possibly stripped) topic when forwarding.
    #[serde(default)]
    pub prefix: Option<String>,
}

/// A configuration error (parse or validation).
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The TOML failed to parse or a rule/endpoint is invalid.
    #[error("invalid bridge config: {0}")]
    Invalid(String),
}

fn invalid(msg: impl Into<String>) -> ConfigError {
    ConfigError::Invalid(msg.into())
}

impl BridgeConfig {
    /// Parse and validate a TOML configuration document.
    ///
    /// # Errors
    /// [`ConfigError::Invalid`] if the TOML is malformed or a value fails validation.
    pub fn parse_toml(s: &str) -> Result<Self, ConfigError> {
        let cfg: BridgeConfig = toml::from_str(s).map_err(|e| invalid(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate a parsed configuration: hop limit ≥ 1, a sane local endpoint, unique
    /// non-empty upstream names, and each rule's filter/`QoS` well-formed.
    ///
    /// # Errors
    /// [`ConfigError::Invalid`] describing the first problem found.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.hop_count_limit == 0 {
            return Err(invalid("hop_count_limit must be >= 1"));
        }
        validate_endpoint("local", &self.local)?;
        let mut seen = HashSet::new();
        for up in &self.upstreams {
            if up.name.trim().is_empty() {
                return Err(invalid("an upstream name must be non-empty"));
            }
            if !seen.insert(up.name.as_str()) {
                return Err(invalid(format!("duplicate upstream name {:?}", up.name)));
            }
            validate_endpoint(&up.name, &up.endpoint)?;
            for rule in &up.rules {
                if !valid_topic_filter(&rule.filter) {
                    return Err(invalid(format!(
                        "upstream {:?}: invalid topic filter {:?}",
                        up.name, rule.filter
                    )));
                }
                if rule.qos > 2 {
                    return Err(invalid(format!(
                        "upstream {:?}: qos must be 0..=2, got {}",
                        up.name, rule.qos
                    )));
                }
            }
        }
        Ok(())
    }
}

fn validate_endpoint(label: &str, ep: &Endpoint) -> Result<(), ConfigError> {
    if ep.url.trim().is_empty() {
        return Err(invalid(format!("{label}: url must be non-empty")));
    }
    if ep.password.is_some() && ep.password_file.is_some() {
        return Err(invalid(format!(
            "{label}: set password OR password_file, not both"
        )));
    }
    if let Some(tls) = &ep.tls {
        if tls.ca.trim().is_empty() {
            return Err(invalid(format!("{label}: tls.ca must be non-empty")));
        }
        if tls.cert.is_some() != tls.key.is_some() {
            return Err(invalid(format!(
                "{label}: tls.cert and tls.key must be set together (mTLS identity)"
            )));
        }
    }
    Ok(())
}

/// Whether `filter` is a syntactically valid MQTT topic filter: non-empty, `#` only as the
/// final level and alone in it, `+` alone in any level it appears in.
#[must_use]
pub fn valid_topic_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }
    let levels: Vec<&str> = filter.split('/').collect();
    let last = levels.len() - 1;
    for (i, level) in levels.iter().enumerate() {
        if level.contains('#') {
            // '#' must be the entire final level.
            if *level != "#" || i != last {
                return false;
            }
        }
        if level.contains('+') && *level != "+" {
            // '+' must occupy the whole level.
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_filters_pass_and_malformed_ones_fail() {
        for ok in ["a", "a/b", "a/+/c", "a/#", "#", "+", "+/+", "$share/g/a/+"] {
            assert!(valid_topic_filter(ok), "{ok} should be valid");
        }
        for bad in ["", "a/#/b", "a#", "a+/b", "+a", "a/b+"] {
            assert!(!valid_topic_filter(bad), "{bad} should be invalid");
        }
    }

    #[test]
    fn a_minimal_config_parses_with_defaults() {
        let cfg = BridgeConfig::parse_toml(
            r#"
            [local]
            url = "127.0.0.1:1883"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.local.url, "127.0.0.1:1883");
        assert_eq!(cfg.hop_count_limit, 8, "default hop limit");
        assert!(cfg.upstreams.is_empty());
    }

    #[test]
    fn a_full_config_parses_rules_directions_and_remap() {
        let cfg = BridgeConfig::parse_toml(
            r#"
            hop_count_limit = 4

            [local]
            url = "cluster:1883"

            [[upstreams]]
            name = "partner"
            url = "partner.example:8883"
            username = "bridge"
            password_file = "/run/secrets/up"

            [upstreams.tls]
            ca = "/etc/ca.pem"
            cert = "/etc/bridge.pem"
            key = "/etc/bridge.key"

            [[upstreams.rules]]
            direction = "out"
            filter = "telemetry/#"
            qos = 1
            remap = { strip_prefix = "telemetry/", prefix = "ourorg/telemetry/" }

            [[upstreams.rules]]
            direction = "in"
            filter = "commands/+"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.hop_count_limit, 4);
        let up = &cfg.upstreams[0];
        assert_eq!(up.name, "partner");
        assert_eq!(up.endpoint.url, "partner.example:8883");
        assert!(up.endpoint.tls.is_some());
        assert_eq!(up.rules.len(), 2);
        assert_eq!(up.rules[0].direction, Direction::Out);
        assert!(up.rules[0].direction.allows_out() && !up.rules[0].direction.allows_in());
        assert_eq!(up.rules[0].qos, 1);
        let remap = up.rules[0].remap.as_ref().unwrap();
        assert_eq!(remap.strip_prefix.as_deref(), Some("telemetry/"));
        assert_eq!(remap.prefix.as_deref(), Some("ourorg/telemetry/"));
        assert_eq!(up.rules[1].direction, Direction::In);
        assert_eq!(up.rules[1].qos, 0, "default qos");
    }

    #[test]
    fn a_zero_hop_limit_is_rejected() {
        let err = BridgeConfig::parse_toml("hop_count_limit = 0\n[local]\nurl = \"x:1883\"\n")
            .unwrap_err();
        assert!(err.to_string().contains("hop_count_limit"));
    }

    #[test]
    fn duplicate_upstream_names_are_rejected() {
        let err = BridgeConfig::parse_toml(
            r#"
            [local]
            url = "x:1883"
            [[upstreams]]
            name = "dup"
            url = "a:1"
            [[upstreams]]
            name = "dup"
            url = "b:2"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate upstream name"));
    }

    #[test]
    fn an_mtls_half_identity_is_rejected() {
        let err = BridgeConfig::parse_toml(
            r#"
            [local]
            url = "x:1883"
            [local.tls]
            ca = "/ca.pem"
            cert = "/c.pem"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("must be set together"));
    }

    #[test]
    fn both_password_sources_are_rejected() {
        let err = BridgeConfig::parse_toml(
            r#"
            [local]
            url = "x:1883"
            password = "inline"
            password_file = "/run/secret"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("password OR password_file"));
    }

    #[test]
    fn an_invalid_rule_filter_is_rejected() {
        let err = BridgeConfig::parse_toml(
            r#"
            [local]
            url = "x:1883"
            [[upstreams]]
            name = "u"
            url = "a:1"
            [[upstreams.rules]]
            direction = "both"
            filter = "a/#/b"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid topic filter"));
    }

    #[test]
    fn an_out_of_range_qos_is_rejected() {
        let err = BridgeConfig::parse_toml(
            r#"
            [local]
            url = "x:1883"
            [[upstreams]]
            name = "u"
            url = "a:1"
            [[upstreams.rules]]
            direction = "out"
            filter = "t"
            qos = 3
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("qos must be 0..=2"));
    }
}
