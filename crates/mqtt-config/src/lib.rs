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
    /// HS256 shared-secret file (`MQTTD_JWT_HS256_SECRET`).
    pub hs256_secret_file: Option<String>,
    /// RS256 public-key PEM (`MQTTD_JWT_RS256_PEM`).
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
    /// Offline-queue overflow policy (`MQTTD_QUEUE_OVERFLOW`): `drop-oldest` or `reject-newest`.
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
    /// Graceful-shutdown drain window, seconds (`MQTTD_SHUTDOWN_GRACE`, ADR 0019). Default 30;
    /// `0` drains immediately (no wait for in-flight connections).
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

    /// Load the layered configuration in ADR 0046 precedence order:
    /// **defaults → the TOML file at `path` (if any) → `MQTTD_*` environment overlay**,
    /// then [`validate`](Self::validate). (CLI flags, the highest layer, are applied by the
    /// caller after this returns.) Env wins over the file, which wins over defaults.
    ///
    /// # Errors
    /// [`ConfigError::Parse`] if the file is unreadable or malformed; [`ConfigError::Invalid`]
    /// if an env value is unparseable or the result fails validation.
    pub fn load(path: Option<&std::path::Path>) -> Result<Self, ConfigError> {
        let mut cfg = match path {
            Some(p) => {
                let s = std::fs::read_to_string(p)
                    .map_err(|e| ConfigError::Parse(format!("reading {}: {e}", p.display())))?;
                toml::from_str(&s).map_err(|e| ConfigError::Parse(e.to_string()))?
            }
            None => Config::default(),
        };
        cfg.overlay_env()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Overlay the process's `MQTTD_*` environment onto this config (env is the higher layer).
    ///
    /// # Errors
    /// [`ConfigError::Invalid`] if a numeric env var holds an unparseable value.
    pub fn overlay_env(&mut self) -> Result<(), ConfigError> {
        self.overlay_from(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
    }

    /// Overlay from an arbitrary getter (each key → its non-empty value, or `None`). This is
    /// the single place `MQTTD_*` ↔ typed-field conversions live — including the *per-var*
    /// boolean conventions (`MQTTD_ALLOW_ANONYMOUS`: any value = on; `MQTTD_DURABLE_SESSIONS`:
    /// `0/false/off/no` = off) that make a naive string flatten unsafe. Injectable so the
    /// mapping is unit-testable without touching the process environment.
    ///
    /// # Errors
    /// [`ConfigError::Invalid`] if a numeric var holds an unparseable value.
    // One linear field-by-field mapping (the single source of env↔typed truth); splitting it
    // would only scatter the surface it enumerates.
    #[allow(clippy::too_many_lines)]
    pub fn overlay_from<F>(&mut self, get: F) -> Result<(), ConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        /// Parse a numeric env var or fail with a located error.
        fn num<T: std::str::FromStr>(key: &str, v: &str) -> Result<T, ConfigError>
        where
            T::Err: std::fmt::Display,
        {
            v.parse::<T>()
                .map_err(|e| ConfigError::Invalid(format!("{key}: invalid value {v:?}: {e}")))
        }
        fn list(v: &str) -> Vec<String> {
            v.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        }
        // Convenience: run `f` with the value if the key is set.
        macro_rules! on {
            ($key:literal, $v:ident, $body:block) => {
                if let Some($v) = get($key) {
                    $body
                }
            };
        }

        // -- node --
        on!("MQTTD_NODE_ID", v, {
            self.node.id = v;
        });
        on!("MQTTD_DATA_DIR", v, {
            self.node.data_dir = Some(v);
        });
        on!("MQTTD_FAILURE_DOMAIN", v, {
            self.node.failure_domain = Some(v);
        });
        on!("MQTTD_FAILURE_DOMAINS", v, {
            let mut m = BTreeMap::new();
            for pair in v.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                let (k, d) = pair.split_once('=').ok_or_else(|| {
                    ConfigError::Invalid(format!(
                        "MQTTD_FAILURE_DOMAINS entry {pair:?} is not node-id=domain"
                    ))
                })?;
                m.insert(k.trim().to_string(), d.trim().to_string());
            }
            self.node.failure_domains = m;
        });

        // -- listeners --
        on!("MQTTD_TLS_BIND", v, {
            self.listeners.tls_bind = Some(v);
        });
        on!("MQTTD_PLAINTEXT_BIND", v, {
            self.listeners.plaintext_bind = Some(v);
        });
        on!("MQTTD_WS_BIND", v, {
            self.listeners.ws_bind = Some(v);
        });
        on!("MQTTD_WSS_BIND", v, {
            self.listeners.wss_bind = Some(v);
        });
        on!("MQTTD_QUIC_BIND", v, {
            self.listeners.quic_bind = Some(v);
        });
        on!("MQTTD_HEALTH_BIND", v, {
            self.listeners.health_bind = Some(v);
        });
        on!("MQTTD_METRICS_BIND", v, {
            self.listeners.metrics_bind = Some(v);
        });

        // -- tls --
        on!("MQTTD_TLS_CERT", v, {
            self.tls.cert = Some(v);
        });
        on!("MQTTD_TLS_KEY", v, {
            self.tls.key = Some(v);
        });
        on!("MQTTD_TLS_CLIENT_CA", v, {
            self.tls.client_ca = Some(v);
        });
        on!("MQTTD_TLS_CRL", v, {
            self.tls.crl = Some(v);
        });

        // -- security -- (MQTTD_ALLOW_ANONYMOUS: presence = on; require_client_cert is derived,
        // has no env var by design)
        if get("MQTTD_ALLOW_ANONYMOUS").is_some() {
            self.security.allow_anonymous = true;
        }
        on!("MQTTD_PASSWORD_FILE", v, {
            self.security.password_file = Some(v);
        });
        on!("MQTTD_ACL_FILE", v, {
            self.security.acl_file = Some(v);
        });
        on!("MQTTD_JWT_HS256_SECRET", v, {
            self.security.jwt.hs256_secret_file = Some(v);
        });
        on!("MQTTD_JWT_RS256_PEM", v, {
            self.security.jwt.rs256_pem_file = Some(v);
        });
        on!("MQTTD_JWT_ISSUER", v, {
            self.security.jwt.issuer = Some(v);
        });
        on!("MQTTD_JWT_AUDIENCE", v, {
            self.security.jwt.audience = Some(v);
        });
        on!("MQTTD_AUTH_TIMEOUT", v, {
            self.security.auth_timeout_secs = Some(num("MQTTD_AUTH_TIMEOUT", &v)?);
        });
        on!("MQTTD_AUTH_PENALTY_THRESHOLD", v, {
            self.security.auth_penalty.threshold = Some(num("MQTTD_AUTH_PENALTY_THRESHOLD", &v)?);
        });
        on!("MQTTD_AUTH_PENALTY_DECAY_SECS", v, {
            self.security.auth_penalty.decay_secs = Some(num("MQTTD_AUTH_PENALTY_DECAY_SECS", &v)?);
        });

        // -- cluster --
        on!("MQTTD_PEER_BIND", v, {
            self.cluster.peer_bind = Some(v);
        });
        on!("MQTTD_PEER_ADVERTISE", v, {
            self.cluster.peer_advertise = Some(v);
        });
        on!("MQTTD_PEERS", v, {
            self.cluster.peers = list(&v);
        });
        on!("MQTTD_PEER_TLS_CA", v, {
            self.cluster.peer_tls.ca = Some(v);
        });
        on!("MQTTD_PEER_TLS_CERT", v, {
            self.cluster.peer_tls.cert = Some(v);
        });
        on!("MQTTD_PEER_TLS_KEY", v, {
            self.cluster.peer_tls.key = Some(v);
        });
        on!("MQTTD_PEER_TLS_CRL", v, {
            self.cluster.peer_tls.crl = Some(v);
        });
        on!("MQTTD_SWIM_BIND", v, {
            self.cluster.swim.bind = Some(v);
        });
        on!("MQTTD_SWIM_SEEDS", v, {
            self.cluster.swim.seeds = list(&v);
        });
        on!("MQTTD_SWIM_KEY", v, {
            self.cluster.swim.key = Some(v);
        });
        on!("MQTTD_SWIM_KEY_ACCEPT", v, {
            self.cluster.swim.key_accept = list(&v);
        });
        on!("MQTTD_SWIM_SIGNED", v, {
            self.cluster.swim.signed = Some(v);
        });
        on!("MQTTD_SWIM_REPLAY", v, {
            self.cluster.swim.replay = Some(v);
        });

        // -- durable -- (MQTTD_DURABLE_SESSIONS: 0/false/off/no = off, else on)
        on!("MQTTD_DURABLE_SESSIONS", v, {
            self.durable.enabled = !matches!(
                v.to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            );
        });
        on!("MQTTD_LEASE_VOTERS", v, {
            self.durable.lease_voters = num("MQTTD_LEASE_VOTERS", &v)?;
        });
        on!("MQTTD_STORE_MAX_BYTES", v, {
            self.durable.store_max_bytes = Some(num("MQTTD_STORE_MAX_BYTES", &v)?);
        });

        // -- limits --
        on!("MQTTD_MAX_CONNECTIONS", v, {
            self.limits.max_connections = Some(num("MQTTD_MAX_CONNECTIONS", &v)?);
        });
        on!("MQTTD_MAX_CONNECTIONS_PER_IP", v, {
            self.limits.max_connections_per_ip = Some(num("MQTTD_MAX_CONNECTIONS_PER_IP", &v)?);
        });
        on!("MQTTD_MAX_PACKET_SIZE", v, {
            self.limits.max_packet_size = Some(num("MQTTD_MAX_PACKET_SIZE", &v)?);
        });
        on!("MQTTD_MAX_PUBLISH_RATE", v, {
            self.limits.max_publish_rate = Some(num("MQTTD_MAX_PUBLISH_RATE", &v)?);
        });
        on!("MQTTD_MAX_QUEUED_MESSAGES", v, {
            self.limits.max_queued_messages = Some(num("MQTTD_MAX_QUEUED_MESSAGES", &v)?);
        });
        on!("MQTTD_MAX_RETAINED_MESSAGES", v, {
            self.limits.max_retained_messages = Some(num("MQTTD_MAX_RETAINED_MESSAGES", &v)?);
        });
        on!("MQTTD_MAX_SESSIONS", v, {
            self.limits.max_sessions = Some(num("MQTTD_MAX_SESSIONS", &v)?);
        });
        on!("MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT", v, {
            self.limits.max_subscriptions_per_client =
                Some(num("MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT", &v)?);
        });
        on!("MQTTD_RECEIVE_MAXIMUM", v, {
            self.limits.receive_maximum = Some(num("MQTTD_RECEIVE_MAXIMUM", &v)?);
        });
        on!("MQTTD_TOPIC_ALIAS_MAX", v, {
            self.limits.topic_alias_max = Some(num("MQTTD_TOPIC_ALIAS_MAX", &v)?);
        });
        on!("MQTTD_QUEUE_OVERFLOW", v, {
            self.limits.queue_overflow = Some(v);
        });

        // -- observability --
        on!("MQTTD_OTLP_ENDPOINT", v, {
            self.observability.otlp_endpoint = Some(v);
        });
        on!("MQTTD_OTLP_INTERVAL", v, {
            self.observability.otlp_interval_secs = num("MQTTD_OTLP_INTERVAL", &v)?;
        });

        // -- runtime --
        on!("MQTTD_SHUTDOWN_GRACE", v, {
            self.runtime.shutdown_grace_secs = num("MQTTD_SHUTDOWN_GRACE", &v)?;
        });
        on!("MQTTD_READY_MIN_MEMBERS", v, {
            self.runtime.ready_min_members = num("MQTTD_READY_MIN_MEMBERS", &v)?;
        });
        on!("MQTTD_CONFIG_WATCH", v, {
            self.runtime.config_watch_secs = num("MQTTD_CONFIG_WATCH", &v)?;
        });

        Ok(())
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
        // shutdown_grace_secs == 0 is valid and meaningful: drain immediately (no wait for
        // in-flight connections), the ADR 0019 fast-teardown value the test harness relies on.
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
            if p != "drop-oldest" && p != "reject-newest" {
                return Err(ConfigError::Invalid(format!(
                    "limits.queue_overflow must be \"drop-oldest\" or \"reject-newest\", got {p:?}"
                )));
            }
        }
        Ok(())
    }
}

/// The authoritative `MQTTD_*` environment surface — every variable
/// [`Config::overlay_from`] consumes, in declaration order. This is the single list the
/// binary's env↔config mapping is checked against (the bijection test below). Adding a config
/// field that an env var should set means adding the var here *and* wiring it in `overlay_from`.
///
/// **Documented exceptions** (a config key with no env var, or an env var with no config key):
/// - [`Security::require_client_cert`] is *derived*, not env-set — it has no variable by design.
/// - `MQTTD_CONFIG` is the meta variable naming the config *file*; it is read by the binary to
///   locate the file, not overlaid as a field, so it is deliberately absent here.
pub const ENV_VARS: &[&str] = &[
    // node
    "MQTTD_NODE_ID",
    "MQTTD_DATA_DIR",
    "MQTTD_FAILURE_DOMAIN",
    "MQTTD_FAILURE_DOMAINS",
    // listeners
    "MQTTD_TLS_BIND",
    "MQTTD_PLAINTEXT_BIND",
    "MQTTD_WS_BIND",
    "MQTTD_WSS_BIND",
    "MQTTD_QUIC_BIND",
    "MQTTD_HEALTH_BIND",
    "MQTTD_METRICS_BIND",
    // tls
    "MQTTD_TLS_CERT",
    "MQTTD_TLS_KEY",
    "MQTTD_TLS_CLIENT_CA",
    "MQTTD_TLS_CRL",
    // security
    "MQTTD_ALLOW_ANONYMOUS",
    "MQTTD_PASSWORD_FILE",
    "MQTTD_ACL_FILE",
    "MQTTD_JWT_HS256_SECRET",
    "MQTTD_JWT_RS256_PEM",
    "MQTTD_JWT_ISSUER",
    "MQTTD_JWT_AUDIENCE",
    "MQTTD_AUTH_TIMEOUT",
    "MQTTD_AUTH_PENALTY_THRESHOLD",
    "MQTTD_AUTH_PENALTY_DECAY_SECS",
    // cluster
    "MQTTD_PEER_BIND",
    "MQTTD_PEER_ADVERTISE",
    "MQTTD_PEERS",
    "MQTTD_PEER_TLS_CA",
    "MQTTD_PEER_TLS_CERT",
    "MQTTD_PEER_TLS_KEY",
    "MQTTD_PEER_TLS_CRL",
    "MQTTD_SWIM_BIND",
    "MQTTD_SWIM_SEEDS",
    "MQTTD_SWIM_KEY",
    "MQTTD_SWIM_KEY_ACCEPT",
    "MQTTD_SWIM_SIGNED",
    "MQTTD_SWIM_REPLAY",
    // durable
    "MQTTD_DURABLE_SESSIONS",
    "MQTTD_LEASE_VOTERS",
    "MQTTD_STORE_MAX_BYTES",
    // limits
    "MQTTD_MAX_CONNECTIONS",
    "MQTTD_MAX_CONNECTIONS_PER_IP",
    "MQTTD_MAX_PACKET_SIZE",
    "MQTTD_MAX_PUBLISH_RATE",
    "MQTTD_MAX_QUEUED_MESSAGES",
    "MQTTD_MAX_RETAINED_MESSAGES",
    "MQTTD_MAX_SESSIONS",
    "MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT",
    "MQTTD_RECEIVE_MAXIMUM",
    "MQTTD_TOPIC_ALIAS_MAX",
    "MQTTD_QUEUE_OVERFLOW",
    // observability
    "MQTTD_OTLP_ENDPOINT",
    "MQTTD_OTLP_INTERVAL",
    // runtime
    "MQTTD_SHUTDOWN_GRACE",
    "MQTTD_READY_MIN_MEMBERS",
    "MQTTD_CONFIG_WATCH",
];

#[cfg(test)]
mod tests {
    use super::{Config, ENV_VARS};

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
        assert!(Config::from_toml("[runtime]\nready_min_members = 0\n").is_err());
        // shutdown_grace_secs = 0 is *valid* (drain immediately) — not out of range.
        assert!(Config::from_toml("[runtime]\nshutdown_grace_secs = 0\n").is_ok());
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
        assert!(Config::from_toml("[limits]\nqueue_overflow = \"reject-newest\"\n").is_ok());
    }

    // --- ADR 0046 T2: env overlay + precedence ---

    /// Build a getter from key→value pairs, for injecting an environment without touching
    /// the real process env.
    fn getter<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| {
            pairs
                .iter()
                .find(|(kk, _)| *kk == k)
                .map(|(_, v)| (*v).to_string())
        }
    }

    #[test]
    fn env_overlay_wins_over_the_file() {
        // File sets node id + lease voters; env overrides both (env is the higher layer).
        let mut c =
            Config::from_toml("[node]\nid = \"from-file\"\n[durable]\nlease_voters = 3\n").unwrap();
        c.overlay_from(getter(&[
            ("MQTTD_NODE_ID", "from-env"),
            ("MQTTD_LEASE_VOTERS", "5"),
        ]))
        .unwrap();
        assert_eq!(c.node.id, "from-env");
        assert_eq!(c.durable.lease_voters, 5);
    }

    #[test]
    fn per_var_boolean_conventions_are_honoured() {
        // MQTTD_ALLOW_ANONYMOUS: *any* value means "on" (the footgun a naive flatten hits).
        let mut c = Config::default();
        c.overlay_from(getter(&[("MQTTD_ALLOW_ANONYMOUS", "0")]))
            .unwrap();
        assert!(c.security.allow_anonymous, "any value enables anonymous");

        // MQTTD_DURABLE_SESSIONS: 0/false/off/no = off, anything else = on.
        for (v, want) in [
            ("0", false),
            ("false", false),
            ("OFF", false),
            ("no", false),
            ("1", true),
            ("yes", true),
        ] {
            let mut c = Config::default();
            c.overlay_from(getter(&[("MQTTD_DURABLE_SESSIONS", v)]))
                .unwrap();
            assert_eq!(c.durable.enabled, want, "MQTTD_DURABLE_SESSIONS={v:?}");
        }
    }

    #[test]
    fn comma_lists_and_the_domain_map_parse() {
        let mut c = Config::default();
        c.overlay_from(getter(&[
            ("MQTTD_PEERS", "a:1, b:2 ,c:3"),
            ("MQTTD_SWIM_SEEDS", "s1:7946,s2:7946"),
            ("MQTTD_FAILURE_DOMAINS", "n1=rack-a, n2=rack-b"),
        ]))
        .unwrap();
        assert_eq!(c.cluster.peers, vec!["a:1", "b:2", "c:3"]);
        assert_eq!(c.cluster.swim.seeds.len(), 2);
        assert_eq!(
            c.node.failure_domains.get("n1").map(String::as_str),
            Some("rack-a")
        );
        assert_eq!(
            c.node.failure_domains.get("n2").map(String::as_str),
            Some("rack-b")
        );
    }

    #[test]
    fn a_bad_numeric_env_value_is_a_located_error() {
        let mut c = Config::default();
        let err = c
            .overlay_from(getter(&[("MQTTD_LEASE_VOTERS", "five")]))
            .expect_err("non-numeric must fail");
        match err {
            super::ConfigError::Invalid(m) => assert!(m.contains("MQTTD_LEASE_VOTERS")),
            super::ConfigError::Parse(m) => panic!("wrong error kind: {m}"),
        }
    }

    #[test]
    fn an_unset_env_leaves_file_and_defaults_intact() {
        // Overlaying an empty environment changes nothing.
        let base =
            Config::from_toml("[node]\nid = \"keep\"\n[limits]\nmax_connections = 42\n").unwrap();
        let mut c = base.clone();
        c.overlay_from(getter(&[])).unwrap();
        assert_eq!(c, base);
    }

    /// A value guaranteed to *differ from the default* for `var`, so overlaying it alone must
    /// mutate the config. Booleans/enums need a specific opposite-of-default value; numerics need
    /// a parseable one; everything else takes an arbitrary non-empty string.
    fn distinct_value(var: &str) -> &'static str {
        match var {
            // Durable is on by default — the only value that *changes* it is a falsey one.
            "MQTTD_DURABLE_SESSIONS" => "off",
            // Presence flips anonymous on (default off).
            "MQTTD_ALLOW_ANONYMOUS" => "1",
            // Enums: any valid, non-default (default None) member.
            "MQTTD_SWIM_SIGNED" | "MQTTD_SWIM_REPLAY" => "require",
            "MQTTD_QUEUE_OVERFLOW" => "reject-newest",
            // The node=domain map needs a well-formed entry.
            "MQTTD_FAILURE_DOMAINS" => "n1=rack-a",
            // Numerics (all widths parse "7").
            "MQTTD_AUTH_TIMEOUT"
            | "MQTTD_AUTH_PENALTY_THRESHOLD"
            | "MQTTD_AUTH_PENALTY_DECAY_SECS"
            | "MQTTD_LEASE_VOTERS"
            | "MQTTD_STORE_MAX_BYTES"
            | "MQTTD_MAX_CONNECTIONS"
            | "MQTTD_MAX_CONNECTIONS_PER_IP"
            | "MQTTD_MAX_PACKET_SIZE"
            | "MQTTD_MAX_PUBLISH_RATE"
            | "MQTTD_MAX_QUEUED_MESSAGES"
            | "MQTTD_MAX_RETAINED_MESSAGES"
            | "MQTTD_MAX_SESSIONS"
            | "MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT"
            | "MQTTD_RECEIVE_MAXIMUM"
            | "MQTTD_TOPIC_ALIAS_MAX"
            | "MQTTD_OTLP_INTERVAL"
            | "MQTTD_SHUTDOWN_GRACE"
            | "MQTTD_READY_MIN_MEMBERS"
            | "MQTTD_CONFIG_WATCH" => "7",
            // Paths / addresses / lists / keys.
            _ => "x-sentinel",
        }
    }

    #[test]
    fn the_env_surface_is_a_deduplicated_curated_list() {
        // Every var appears exactly once — a duplicate would be a copy/paste bug that hides a
        // missing mapping.
        let mut seen = std::collections::BTreeSet::new();
        for v in ENV_VARS {
            assert!(seen.insert(*v), "{v} is listed twice in ENV_VARS");
            assert!(v.starts_with("MQTTD_"), "{v} is not an MQTTD_* var");
        }
        // Guards the count so adding/removing a field forces a deliberate list update.
        assert_eq!(
            seen.len(),
            57,
            "the MQTTD_* surface changed — update ENV_VARS"
        );
    }

    #[test]
    fn every_env_var_maps_to_a_config_key() {
        // Totality (env → config): setting *one* listed var, alone, must move the config off its
        // default. If overlay_from ever dropped a mapping, that var's overlay would be a no-op
        // and this fails — the var would silently do nothing.
        for var in ENV_VARS {
            let mut c = Config::default();
            c.overlay_from(getter(&[(var, distinct_value(var))]))
                .unwrap_or_else(|e| panic!("overlay of {var} errored: {e}"));
            assert_ne!(
                c,
                Config::default(),
                "{var} is in ENV_VARS but overlaying it changed nothing — the mapping is missing"
            );
        }
    }

    #[test]
    fn the_whole_env_surface_overlays_without_collision() {
        // Setting the entire surface at once produces a config that differs from default in every
        // section and still round-trips through validate for the numeric/enistence-only fields
        // (the relational checks that a full env would trip — crl-without-ca etc. — are exercised
        // by the dedicated tests above; here every var carries a self-consistent value).
        let pairs: Vec<(&str, &str)> = ENV_VARS.iter().map(|v| (*v, distinct_value(v))).collect();
        let mut c = Config::default();
        c.overlay_from(getter(&pairs)).unwrap();
        // A representative field from each section moved.
        assert_eq!(c.node.id, "x-sentinel");
        assert!(c.listeners.tls_bind.is_some());
        assert!(c.tls.cert.is_some());
        assert!(c.security.allow_anonymous);
        assert!(c.cluster.peer_bind.is_some());
        assert!(!c.durable.enabled);
        assert_eq!(c.limits.max_connections, Some(7));
        assert!(c.observability.otlp_endpoint.is_some());
        assert_eq!(c.runtime.ready_min_members, 7);
    }
}
