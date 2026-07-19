//! Typed broker configuration with **secure defaults** (ADR 0046).
//!
//! This is the strict TOML schema for `mqttd`: one struct per concern, mirroring the
//! `MQTTD_*` environment surface documented in `mqttd`'s `main.rs`. It is
//! **deserialize-strict** — every table rejects unknown keys (`deny_unknown_fields`) so a
//! typo fails the load instead of being silently ignored — and every default encodes the
//! project's security posture (TLS-only, anonymous off, deny-by-default authz, mTLS on).
//! Insecure options exist but must be turned on deliberately.
//!
//! The schema is the *shape*; how a file layers under env vars and flags
//! (defaults < file < env < flags) is ADR 0046 T2. Secret material is referenced **by path
//! only** (T5) — this struct carries file paths, never inlined keys.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level broker configuration. Every section defaults to a secure, minimal posture;
/// `#[serde(default)]` lets a file set only what it overrides.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Node identity and on-disk location.
    pub node: Node,
    /// Network listener bind addresses (all opt-in; unset = that listener is off).
    pub listeners: Listeners,
    /// TLS material for the client listeners (paths).
    pub tls: Tls,
    /// Authentication / authorization policy.
    pub security: Security,
    /// Cluster transport + membership.
    pub cluster: Cluster,
    /// Durable (consensus-backed) session storage.
    pub durable: Durable,
    /// Resource-governance caps and quotas (ADR 0041).
    pub limits: Limits,
    /// Metrics export (ADR 0020).
    pub observability: Observability,
    /// Runtime behaviour (shutdown, readiness, reload).
    pub runtime: Runtime,
}

/// Node identity and data directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Node {
    /// Stable node id (`MQTTD_NODE_ID`). Default `node-local`.
    pub id: String,
    /// Durable-plane data directory (`MQTTD_DATA_DIR`). Unset → in-memory only.
    pub data_dir: Option<String>,
    /// This node's self-advertised failure-domain label (`MQTTD_FAILURE_DOMAIN`, ADR 0016).
    pub failure_domain: Option<String>,
    /// Static `node-id → domain` failure-domain topology (`MQTTD_FAILURE_DOMAINS`).
    pub failure_domains: BTreeMap<String, String>,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            id: "node-local".to_string(),
            data_dir: None,
            failure_domain: None,
            failure_domains: BTreeMap::new(),
        }
    }
}

/// Listener bind addresses. Every listener is **opt-in**: `None` means that transport is
/// not served. TLS is the intended default; plaintext/WS are for local testing or a fronted
/// deployment and are loudly logged as insecure when enabled.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Listeners {
    /// TLS client listener (`MQTTD_TLS_BIND`), e.g. `0.0.0.0:8883`. Needs [`Tls::cert`]/[`Tls::key`].
    pub tls_bind: Option<String>,
    /// Insecure plaintext client listener (`MQTTD_PLAINTEXT_BIND`), e.g. `127.0.0.1:1883`.
    pub plaintext_bind: Option<String>,
    /// MQTT-over-WebSocket (`ws://`) listener (`MQTTD_WS_BIND`).
    pub ws_bind: Option<String>,
    /// MQTT-over-WebSocket-Secure (`wss://`) listener (`MQTTD_WSS_BIND`); shares the TLS material.
    pub wss_bind: Option<String>,
    /// MQTT-over-QUIC (UDP) listener (`MQTTD_QUIC_BIND`).
    pub quic_bind: Option<String>,
    /// HTTP health/probe listener (`MQTTD_HEALTH_BIND`): `/livez`, `/readyz`, `/metrics`.
    pub health_bind: Option<String>,
    /// Optional separate `/metrics` listener (`MQTTD_METRICS_BIND`), to isolate the scrape.
    pub metrics_bind: Option<String>,
}

/// TLS material for the client listeners. Paths, never inlined key bytes (ADR 0046 T5).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Tls {
    /// Server certificate chain PEM (`MQTTD_TLS_CERT`).
    pub cert: Option<String>,
    /// Server private key PEM (`MQTTD_TLS_KEY`).
    pub key: Option<String>,
    /// Client-CA bundle PEM (`MQTTD_TLS_CLIENT_CA`); when set, clients must present a cert
    /// it issued (mTLS).
    pub client_ca: Option<String>,
    /// Client-certificate revocation list PEM (`MQTTD_TLS_CRL`); requires [`Tls::client_ca`].
    pub crl: Option<String>,
}

/// Authentication + authorization policy. Secure by default: no anonymous access, mTLS
/// required, deny-by-default authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Security {
    /// Permit clients presenting no credentials (`MQTTD_ALLOW_ANONYMOUS`). Default `false`.
    pub allow_anonymous: bool,
    /// Require a client certificate (mTLS) on TLS listeners. Default `true`.
    pub require_client_cert: bool,
    /// Argon2id `username:phc-hash` password file (`MQTTD_PASSWORD_FILE`).
    pub password_file: Option<String>,
    /// Topic-ACL TOML policy file (`MQTTD_ACL_FILE`); without it authorization is not
    /// enforced and loudly logged.
    pub acl_file: Option<String>,
    /// JWT verification (ADR 0013).
    pub jwt: Jwt,
    /// Seconds a client may take to authenticate before the connection is dropped
    /// (`MQTTD_AUTH_TIMEOUT`).
    pub auth_timeout_secs: Option<u64>,
    /// Repeated-auth-failure penalty box (`MQTTD_AUTH_PENALTY_*`, ADR 0041 T2).
    pub auth_penalty: AuthPenalty,
}

impl Default for Security {
    fn default() -> Self {
        Self {
            allow_anonymous: false,
            require_client_cert: true,
            password_file: None,
            acl_file: None,
            jwt: Jwt::default(),
            auth_timeout_secs: None,
            auth_penalty: AuthPenalty::default(),
        }
    }
}

/// JWT verification key + optional claim constraints (`MQTTD_JWT_*`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Jwt {
    /// HS256 shared-secret file (`MQTTD_JWT_HS`).
    pub hs256_secret_file: Option<String>,
    /// RS256 public-key PEM (`MQTTD_JWT_RS`).
    pub rs256_pem_file: Option<String>,
    /// Required `iss` claim (`MQTTD_JWT_ISSUER`).
    pub issuer: Option<String>,
    /// Required `aud` claim (`MQTTD_JWT_AUDIENCE`).
    pub audience: Option<String>,
}

/// Auth-failure penalty box (`MQTTD_AUTH_PENALTY_*`, ADR 0041 T2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthPenalty {
    /// Failures from one IP before it is penalty-boxed (`MQTTD_AUTH_PENALTY_THRESHOLD`).
    pub threshold: Option<u32>,
    /// Seconds a penalty decays over (`MQTTD_AUTH_PENALTY_DECAY_SECS`).
    pub decay_secs: Option<u64>,
}

/// Cluster transport (peer links) and SWIM membership.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cluster {
    /// Inter-node listener bind (`MQTTD_PEER_BIND`).
    pub peer_bind: Option<String>,
    /// Peer-link address gossip advertises (`MQTTD_PEER_ADVERTISE`); default the bind.
    pub peer_advertise: Option<String>,
    /// Static peer addresses to dial (`MQTTD_PEERS`).
    pub peers: Vec<String>,
    /// Cluster-bus mTLS material (`MQTTD_PEER_TLS_*`); set all three or none.
    pub peer_tls: PeerTls,
    /// SWIM gossip membership.
    pub swim: Swim,
}

/// Cluster-bus (peer link) mTLS material (`MQTTD_PEER_TLS_*`). Paths only.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PeerTls {
    /// Cluster CA bundle PEM (`MQTTD_PEER_TLS_CA`).
    pub ca: Option<String>,
    /// Cluster-bus leaf certificate PEM (`MQTTD_PEER_TLS_CERT`).
    pub cert: Option<String>,
    /// Cluster-bus leaf key PEM (`MQTTD_PEER_TLS_KEY`).
    pub key: Option<String>,
    /// Cluster-bus CRL PEM (`MQTTD_PEER_TLS_CRL`); requires the three above.
    pub crl: Option<String>,
}

/// SWIM gossip membership (`MQTTD_SWIM_*`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Swim {
    /// Gossip UDP bind (`MQTTD_SWIM_BIND`); requires [`Cluster::peer_bind`].
    pub bind: Option<String>,
    /// Seed member gossip addresses (`MQTTD_SWIM_SEEDS`).
    pub seeds: Vec<String>,
    /// 64-hex cluster gossip key file/value (`MQTTD_SWIM_KEY`).
    pub key: Option<String>,
    /// Extra accepted gossip keys for zero-downtime rotation (`MQTTD_SWIM_KEY_ACCEPT`).
    pub key_accept: Vec<String>,
    /// Per-node gossip signature posture (`MQTTD_SWIM_SIGNED`): `require` or `off`.
    pub signed: Option<String>,
    /// Gossip anti-replay posture (`MQTTD_SWIM_REPLAY`): `require` or `off`.
    pub replay: Option<String>,
}

/// Durable (consensus-backed) session storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Durable {
    /// Whether durable sessions are enabled (`MQTTD_DURABLE_SESSIONS`). Default `true`
    /// (ADR 0029): durable is the secure, data-safe default.
    pub enabled: bool,
    /// Bounded lease-consensus voter set size (`MQTTD_LEASE_VOTERS`, ADR 0021). Default 5.
    pub lease_voters: u32,
    /// Disk high-water byte cap for the durable store (`MQTTD_STORE_MAX_BYTES`, ADR 0041 T5).
    pub store_max_bytes: Option<u64>,
}

impl Default for Durable {
    fn default() -> Self {
        Self {
            enabled: true,
            lease_voters: 5,
            store_max_bytes: None,
        }
    }
}

/// Resource-governance caps + quotas (ADR 0041). `None`/`0` generally means unbounded,
/// matching the env behaviour.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Global connection cap (`MQTTD_MAX_CONNECTIONS`).
    pub max_connections: Option<u64>,
    /// Per-source-IP connection cap (`MQTTD_MAX_CONNECTIONS_PER_IP`).
    pub max_connections_per_ip: Option<u64>,
    /// Largest accepted MQTT packet, bytes (`MQTTD_MAX_PACKET_SIZE`).
    pub max_packet_size: Option<u64>,
    /// Per-client publish-rate cap, msg/s (`MQTTD_MAX_PUBLISH_RATE`).
    pub max_publish_rate: Option<u64>,
    /// Per-client offline-queue depth (`MQTTD_MAX_QUEUED_MESSAGES`).
    pub max_queued_messages: Option<u64>,
    /// Global retained-message cap (`MQTTD_MAX_RETAINED_MESSAGES`).
    pub max_retained_messages: Option<u64>,
    /// Global session cap (`MQTTD_MAX_SESSIONS`).
    pub max_sessions: Option<u64>,
    /// Per-client subscription cap (`MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT`).
    pub max_subscriptions_per_client: Option<u64>,
    /// MQTT 5 Receive Maximum granted to clients (`MQTTD_RECEIVE_MAXIMUM`).
    pub receive_maximum: Option<u16>,
    /// MQTT 5 Topic Alias Maximum granted to clients (`MQTTD_TOPIC_ALIAS_MAX`).
    pub topic_alias_max: Option<u16>,
    /// Offline-queue overflow policy (`MQTTD_QUEUE_OVERFLOW`): `drop-oldest` or `drop-new`.
    pub queue_overflow: Option<String>,
}

/// Metrics export (ADR 0020).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Observability {
    /// OTLP/HTTP collector base URL (`MQTTD_OTLP_ENDPOINT`); enables OTLP push export.
    pub otlp_endpoint: Option<String>,
    /// OTLP push interval, seconds (`MQTTD_OTLP_INTERVAL`). Default 10.
    pub otlp_interval_secs: u64,
}

impl Default for Observability {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            otlp_interval_secs: 10,
        }
    }
}

/// Runtime behaviour: shutdown, readiness gating, config auto-reload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Runtime {
    /// Graceful-shutdown drain window, seconds (`MQTTD_SHUTDOWN_GRACE`, ADR 0019). Default 30.
    pub shutdown_grace_secs: u64,
    /// Smallest mesh size `/readyz` accepts (`MQTTD_READY_MIN_MEMBERS`). Default 1.
    pub ready_min_members: usize,
    /// Filesystem config-watch poll interval, seconds (`MQTTD_CONFIG_WATCH`, ADR 0033).
    /// `0`/unset = signal-only (SIGHUP), the default.
    pub config_watch_secs: u64,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            shutdown_grace_secs: 30,
            ready_min_members: 1,
            config_watch_secs: 0,
        }
    }
}

/// Errors from parsing or validating configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The TOML did not parse, or carried an unknown/mistyped key.
    #[error("config parse error: {0}")]
    Parse(String),
    /// A combination of options is internally inconsistent or out of range.
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl Config {
    /// Parse a strict TOML document into a `Config` and [`validate`](Self::validate) it.
    /// Unknown keys, type mismatches, and out-of-range values all fail here with a located
    /// message — nothing is silently ignored.
    ///
    /// # Errors
    /// [`ConfigError::Parse`] on a TOML/shape error, [`ConfigError::Invalid`] on a semantic one.
    pub fn from_toml(s: &str) -> Result<Self, ConfigError> {
        let cfg: Config = toml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate that the configuration is internally consistent, in range, and that every
    /// insecure combination has been explicitly opted into.
    ///
    /// # Errors
    /// [`ConfigError::Invalid`] describing the first problem found.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Plaintext / WS listeners are insecure; allowed, but never without a bind that
        // makes the intent explicit (the presence of the bind IS the opt-in, loudly logged
        // at runtime). Range checks below catch nonsensical values early.
        if self.durable.enabled && self.durable.lease_voters == 0 {
            return Err(ConfigError::Invalid(
                "durable.lease_voters must be >= 1".to_string(),
            ));
        }
        if self.runtime.shutdown_grace_secs == 0 {
            return Err(ConfigError::Invalid(
                "runtime.shutdown_grace_secs must be >= 1".to_string(),
            ));
        }
        if self.runtime.ready_min_members == 0 {
            return Err(ConfigError::Invalid(
                "runtime.ready_min_members must be >= 1".to_string(),
            ));
        }
        if self.observability.otlp_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "observability.otlp_interval_secs must be >= 1".to_string(),
            ));
        }
        // mTLS CRL needs a client CA to check against.
        if self.tls.crl.is_some() && self.tls.client_ca.is_none() {
            return Err(ConfigError::Invalid(
                "tls.crl requires tls.client_ca".to_string(),
            ));
        }
        if self.cluster.peer_tls.crl.is_some()
            && (self.cluster.peer_tls.ca.is_none()
                || self.cluster.peer_tls.cert.is_none()
                || self.cluster.peer_tls.key.is_none())
        {
            return Err(ConfigError::Invalid(
                "cluster.peer_tls.crl requires ca + cert + key".to_string(),
            ));
        }
        for (field, v) in [
            ("swim.signed", self.cluster.swim.signed.as_deref()),
            ("swim.replay", self.cluster.swim.replay.as_deref()),
        ] {
            if let Some(v) = v {
                if v != "require" && v != "off" {
                    return Err(ConfigError::Invalid(format!(
                        "cluster.{field} must be \"require\" or \"off\", got {v:?}"
                    )));
                }
            }
        }
        if let Some(p) = &self.limits.queue_overflow {
            if p != "drop-oldest" && p != "drop-new" {
                return Err(ConfigError::Invalid(format!(
                    "limits.queue_overflow must be \"drop-oldest\" or \"drop-new\", got {p:?}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn defaults_are_secure() {
        let c = Config::default();
        assert!(!c.security.allow_anonymous);
        assert!(c.security.require_client_cert);
        assert!(c.listeners.plaintext_bind.is_none());
        assert!(c.listeners.tls_bind.is_none());
        assert!(c.durable.enabled, "durable is the default (ADR 0029)");
        assert_eq!(c.durable.lease_voters, 5);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn a_full_toml_round_trips() {
        let toml = r#"
            [node]
            id = "n1"
            data_dir = "/data"

            [listeners]
            tls_bind = "0.0.0.0:8883"
            plaintext_bind = "127.0.0.1:1883"

            [tls]
            cert = "/etc/mqttd/cert.pem"
            key = "/etc/mqttd/key.pem"
            client_ca = "/etc/mqttd/ca.pem"

            [security]
            allow_anonymous = false

            [durable]
            lease_voters = 3

            [limits]
            max_connections = 10000
            queue_overflow = "drop-oldest"
        "#;
        let c = Config::from_toml(toml).expect("valid config");
        assert_eq!(c.node.id, "n1");
        assert_eq!(c.listeners.tls_bind.as_deref(), Some("0.0.0.0:8883"));
        assert_eq!(c.durable.lease_voters, 3);
        assert_eq!(c.limits.max_connections, Some(10000));
    }

    #[test]
    fn an_unknown_key_is_rejected() {
        // A typo must fail the load, not be silently ignored.
        let err = Config::from_toml("[security]\nallow_anonymus = true\n")
            .expect_err("unknown key must be rejected");
        assert!(matches!(err, super::ConfigError::Parse(_)));
    }

    #[test]
    fn an_unknown_top_level_table_is_rejected() {
        let err = Config::from_toml("[nonsense]\nx = 1\n").expect_err("unknown table rejected");
        assert!(matches!(err, super::ConfigError::Parse(_)));
    }

    #[test]
    fn a_type_mismatch_is_rejected() {
        let err = Config::from_toml("[durable]\nlease_voters = \"three\"\n")
            .expect_err("string for an int must be rejected");
        assert!(matches!(err, super::ConfigError::Parse(_)));
    }

    #[test]
    fn out_of_range_values_are_rejected() {
        assert!(Config::from_toml("[durable]\nlease_voters = 0\n").is_err());
        assert!(Config::from_toml("[runtime]\nshutdown_grace_secs = 0\n").is_err());
        assert!(Config::from_toml("[runtime]\nready_min_members = 0\n").is_err());
    }

    #[test]
    fn a_crl_without_its_ca_is_rejected() {
        let err =
            Config::from_toml("[tls]\ncrl = \"/etc/crl.pem\"\n").expect_err("crl needs client_ca");
        assert!(matches!(err, super::ConfigError::Invalid(_)));
    }

    #[test]
    fn a_bad_enum_value_is_rejected() {
        assert!(Config::from_toml("[cluster.swim]\nsigned = \"maybe\"\n").is_err());
        assert!(Config::from_toml("[limits]\nqueue_overflow = \"drop-middle\"\n").is_err());
    }
}
