//! Typed broker configuration with **secure defaults** (ADR 0046).
//!
//! This is the strict TOML schema for `mqttd`: one struct per concern, mirroring the
//! `MQTTD_*` environment surface documented in `mqttd`'s `main.rs`. It is
//! **deserialize-strict by default** — unknown keys fail the load, ALL of them listed
//! at once, so a typo fails loudly instead of being silently ignored — with one
//! deliberate escape hatch (`runtime.config_unknown_keys = "warn"`, issue #230 /
//! ADR 0058 T4): a config written for a NEWER broker must be able to boot an older
//! binary during a rollback or mixed-version window, unknown keys ignored LOUDLY
//! instead of crash-looping the fleet. Every default encodes the project's security
//! posture (TLS-only, anonymous off, deny-by-default authz, mTLS on). Insecure
//! options exist but must be turned on deliberately.
//!
//! The schema is the *shape*; how a file layers under env vars and flags
//! (defaults < file < env < flags) is ADR 0046 T2. Secret material is referenced **by path
//! only** (T5) — this struct carries file paths, never inlined keys.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level broker configuration. Every section defaults to a secure, minimal posture;
/// `#[serde(default)]` lets a file set only what it overrides.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
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
    /// Online backup + restore (ADR 0062).
    pub backup: Backup,
    /// Audit-trail export (ADR 0066 T3).
    pub audit: Audit,
    /// The unknown key paths the last parse IGNORED under
    /// [`UnknownConfigKeys::Warn`] (issue #230) — carried here so the caller can
    /// log them loudly without a signature change. Never serialized; empty under
    /// `refuse` (the load fails instead).
    #[serde(skip)]
    pub ignored_keys: Vec<String>,
}

/// Node identity and data directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Node {
    /// Stable node id (`MQTTD_NODE_ID`). Default `node-local`.
    pub id: String,
    /// Durable-plane data directory (`MQTTD_DATA_DIR`). Unset with durable ON (the
    /// default) is refused at validation unless `durable.allow_ephemeral` opts in
    /// (issue #240); unset with durable off is simply the in-memory store.
    pub data_dir: Option<String>,
    /// This node's self-advertised failure-domain label (`MQTTD_FAILURE_DOMAIN`, ADR 0016).
    pub failure_domain: Option<String>,
    /// Static `node-id → domain` failure-domain topology (`MQTTD_FAILURE_DOMAINS`).
    pub failure_domains: BTreeMap<String, String>,
}

/// The node id a broker uses when the operator sets none.
///
/// Fine for a single node; **wrong for every node in a cluster**, since they would
/// all answer to it. Exposed so startup can warn when clustering is configured and
/// this is still the value.
pub const DEFAULT_NODE_ID: &str = "node-local";

impl Default for Node {
    fn default() -> Self {
        Self {
            id: DEFAULT_NODE_ID.to_string(),
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
#[serde(default)]
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
#[serde(default)]
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
    /// TLS 1.3 session-resumption cache size per listener (`MQTTD_TLS_SESSION_CACHE`).
    /// `None` uses the broker default (32k entries — rustls' own 256 is no resumption at
    /// fleet scale); `0` disables resumption so every connection is fully re-verified.
    pub session_cache: Option<usize>,
    /// Admit TLS 1.2 clients on the client-facing TLS listener
    /// (`MQTTD_TLS_ALLOW_TLS12`). **Off by default and loudly logged when on** — a
    /// reduced posture for fleets whose device firmware cannot negotiate 1.3. Never
    /// affects the cluster bus or QUIC (1.3 by protocol). Even when on, 1.2 is
    /// HARDENED: ECDHE+AEAD suites only, and Extended Master Secret (RFC 7627)
    /// required — see [`Tls::allow_unsafe_tls12_features`].
    pub allow_tls12: bool,
    /// Relax the hardened TLS 1.2 posture (`MQTTD_TLS_ALLOW_UNSAFE_TLS12_FEATURES`):
    /// admits legacy clients that cannot do Extended Master Secret, reopening the
    /// triple-handshake surface for exactly those clients. **Off by default**, loudly
    /// logged when on, and a configuration ERROR without [`Tls::allow_tls12`] — a
    /// relaxation of something that is off cannot mean anything.
    pub allow_unsafe_tls12_features: bool,
}

/// Authentication + authorization policy. Secure by default: no anonymous access, mTLS
/// required, deny-by-default authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Security {
    /// Permit clients presenting no credentials (`MQTTD_ALLOW_ANONYMOUS`). Default `false`.
    pub allow_anonymous: bool,
    /// Require a client certificate (mTLS) on TLS listeners. Default `true`.
    pub require_client_cert: bool,
    /// Which field of a verified client certificate is the identity
    /// (`MQTTD_MTLS_IDENTITY_SOURCE`, ADR 0004 T11): `"cn"` (default), `"san-dns"`,
    /// `"san-uri"`, or `"san-email"`. `None` means `"cn"`. Applies to client listeners
    /// only — the cluster bus binds peer node ids to the Common Name by definition
    /// (ADR 0004 T7).
    pub mtls_identity_source: Option<String>,
    /// Argon2id `username:phc-hash` password file (`MQTTD_PASSWORD_FILE`).
    pub password_file: Option<String>,
    /// Topic-ACL TOML policy file (`MQTTD_ACL_FILE`); without it authorization is not
    /// enforced and loudly logged.
    pub acl_file: Option<String>,
    /// JWT verification (ADR 0013).
    pub jwt: Jwt,
    /// OIDC-mode token verification (ADR 0050).
    pub oidc: Oidc,
    /// Remote HTTP authentication hook (ADR 0004 T16).
    pub http_auth: HttpAuth,
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
            mtls_identity_source: None,
            password_file: None,
            acl_file: None,
            jwt: Jwt::default(),
            oidc: Oidc::default(),
            http_auth: HttpAuth::default(),
            auth_timeout_secs: None,
            auth_penalty: AuthPenalty::default(),
        }
    }
}

/// JWT verification key + optional claim constraints (`MQTTD_JWT_*`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Jwt {
    /// Path to a file holding the HS256 shared secret (`MQTTD_JWT_HS256_SECRET_FILE`, ADR 0046
    /// T5): secret-by-reference, so the HMAC key is mounted from a Secret, never inlined.
    pub hs256_secret_file: Option<String>,
    /// RS256 public-key PEM (`MQTTD_JWT_RS256_PEM`).
    pub rs256_pem_file: Option<String>,
    /// Required `iss` claim (`MQTTD_JWT_ISSUER`).
    pub issuer: Option<String>,
    /// Required `aud` claim (`MQTTD_JWT_AUDIENCE`).
    pub audience: Option<String>,
}

/// OIDC-mode token verification (`MQTTD_OIDC_*`, ADR 0050): issuer-URL discovery,
/// JWKS rotation followed live. Distinct from the static-key `Jwt` section — the two
/// are separate authenticators and are not mixed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Oidc {
    /// Issuer URL (`MQTTD_OIDC_ISSUER`); enables OIDC mode. Must be https unless
    /// `allow_http` (testing) is set. Also the required `iss` claim value.
    pub issuer: Option<String>,
    /// Required `aud` claim (`MQTTD_OIDC_AUDIENCE`); mandatory in OIDC mode.
    pub audience: Option<String>,
    /// JWKS background-refresh interval in seconds (`MQTTD_OIDC_JWKS_REFRESH`, default 300).
    pub jwks_refresh_secs: Option<u64>,
    /// Staleness window in seconds before fail-closed (`MQTTD_OIDC_MAX_STALE`, default 86400).
    pub max_stale_secs: Option<u64>,
    /// Permit an http:// issuer (`MQTTD_OIDC_ALLOW_HTTP` — INSECURE, loudly logged; tests).
    pub allow_http: bool,
    /// Claim to read group memberships from (`MQTTD_OIDC_GROUPS_CLAIM`, default `groups`).
    pub groups_claim: Option<String>,
}

/// Remote HTTP authentication hook (`MQTTD_HTTP_AUTH_*`, ADR 0004 T16).
///
/// One hook reaches every backend the broker will never implement natively — LDAP,
/// `OAuth2` introspection, a bespoke user table. The broker `POST`s the credential to `url`
/// and reads the **HTTP status** as the verdict.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpAuth {
    /// Endpoint to POST credentials to (`MQTTD_HTTP_AUTH_URL`); enables the hook.
    /// Must be `https` unless [`allow_http`](Self::allow_http) is set.
    pub url: Option<String>,
    /// Per-request timeout in seconds (`MQTTD_HTTP_AUTH_TIMEOUT`, default 5).
    ///
    /// The broker applies no timeout of its own around an authenticator, so this is the
    /// only bound on how long a CONNECT can wait. It expires **closed**.
    pub timeout_secs: Option<u64>,
    /// Seconds to cache an ACCEPTED credential (`MQTTD_HTTP_AUTH_CACHE_SECS`, default 0
    /// = no caching). Rejections are never cached: a fixed password must take effect at
    /// once, and caching denials would turn a hook outage into a lasting one.
    pub cache_secs: Option<u64>,
    /// Most accepted credentials to hold in the cache (`MQTTD_HTTP_AUTH_CACHE_MAX`,
    /// default 10000). The cache sits on an attacker-reachable path, so it is bounded.
    pub cache_max: Option<u64>,
    /// Permit an `http://` hook URL (`MQTTD_HTTP_AUTH_ALLOW_HTTP` — INSECURE, loudly
    /// logged; tests only). Credentials cross this link.
    pub allow_http: bool,
}

/// Auth-failure penalty box (`MQTTD_AUTH_PENALTY_*`, ADR 0041 T2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthPenalty {
    /// Failures from one IP before it is penalty-boxed (`MQTTD_AUTH_PENALTY_THRESHOLD`).
    pub threshold: Option<u32>,
    /// Seconds a penalty decays over (`MQTTD_AUTH_PENALTY_DECAY_SECS`).
    pub decay_secs: Option<u64>,
}

/// Cluster transport (peer links) and SWIM membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
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
    /// Refuse to serve after re-founding a cluster beside a live one
    /// (`MQTTD_REFOUND_GUARD`, default `true`).
    ///
    /// A node founds a cluster precisely because it starts with no seeds, and a node
    /// whose data dir was lost cannot tell "first ever bootstrap" from "my volume was
    /// wiped while the cluster kept running" — the wipe deleted exactly the state that
    /// would have differed. So it mints a second identity beside the live one and, with
    /// nothing to hold it back, passes readiness and starts serving clients an empty
    /// session and retained store.
    ///
    /// The guard keys on the one signal only the *second* case produces: surviving peers
    /// keep greeting this node, their datagrams carry the other identity, and they are
    /// dropped as `cluster-mismatch`. On a genuine first bootstrap no peer exists to send
    /// one, so the guard cannot fire. When it does fire the node latches `NotReady` for the
    /// rest of the process, staying out of load-balancer rotation until a human runs the
    /// documented wipe-and-rejoin.
    ///
    /// Set `false` only to re-bootstrap deliberately beside a cluster you are abandoning.
    pub refound_guard: bool,
    /// Prefer a **local** `$share` member when one is online
    /// (`MQTTD_SHARED_PREFER_LOCAL`, default `true`; set `0`/`false`/`off`/`no`
    /// for plain round-robin over every online member, local or remote).
    ///
    /// Why it exists: shared selection is round-robin across the whole group, so
    /// a group spread over N nodes picks a REMOTE member roughly (N-1)/N of the
    /// time and every one of those publishes crosses the cluster bus. For a
    /// workload that is already partitioned by topic — one tenant per group, its
    /// consumers present on every node — that forwarding is pure overhead: a
    /// local member could have served the message with no network hop at all.
    /// Measured on the ADR 0077 lane E tenancy ladder (issue #508): a 5-node
    /// cluster carried **12 sites / 360,000 msg/s** at p99 <=1s with round-robin
    /// and **17 sites / 510,000 msg/s** with this on — **+42%** — and past the
    /// knee it degrades gracefully (98.9% delivered at 594k) where round-robin
    /// collapsed to 53%. Per node that is 102,000 msg/s against a single-node
    /// knee of 90,000-120,000, i.e. capacity that scales with the cluster. The
    /// ceiling it removes is the cluster bus itself: round-robin over a group
    /// spread across N nodes picks a REMOTE member roughly (N-1)/N of the time,
    /// so at N=5 about 80% of publishes crossed the network for no reason.
    ///
    /// What it costs, and why it is still worth defaulting ON: round-robin is
    /// what makes a shared subscription *fair* — every member takes an equal
    /// share regardless of where publishers connect. Local-first keeps that
    /// fairness WITHIN a node and gives it up ACROSS nodes, so an uneven
    /// publisher spread produces an uneven consumer load. MQTT 5 does not
    /// require even distribution among shared subscribers, so this is a
    /// spec-legal trade, but it IS a behaviour change: a deployment that relies
    /// on equal shares across nodes should set this off.
    ///
    /// It is not always a win. The preference only applies when the publishing
    /// node hosts an online member of that group; a group with no local member
    /// falls back to remote exactly as before (it never drops). So a deployment
    /// with few consumers per group spread thinly over many nodes — say two
    /// members across ten nodes — finds no local member on most nodes and gains
    /// nothing, while paying the fairness cost. Structural partition ownership,
    /// not this knob, is what that case needs.
    pub shared_prefer_local: bool,
}

impl Default for Cluster {
    fn default() -> Self {
        Self {
            peer_bind: None,
            peer_advertise: None,
            peers: Vec::new(),
            peer_tls: PeerTls::default(),
            swim: Swim::default(),
            // Data-safe default (as with `durable.enabled`): a node that re-founds beside
            // a live cluster must not serve from an empty store.
            refound_guard: true,
            // Off: round-robin is the fair behaviour and the one every existing
            // deployment already has. Locality preference trades that fairness for
            // throughput and must be asked for.
            shared_prefer_local: true,
        }
    }
}

/// Cluster-bus (peer link) mTLS material (`MQTTD_PEER_TLS_*`). Paths only.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
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
#[serde(default)]
pub struct Swim {
    /// Gossip UDP bind (`MQTTD_SWIM_BIND`); requires [`Cluster::peer_bind`].
    pub bind: Option<String>,
    /// Gossip address this node ADVERTISES as its own (`MQTTD_SWIM_ADVERTISE`);
    /// default the bind. Set it where the dialable address differs from the bound
    /// one — NAT, container port mapping — or whenever the bind is the unspecified
    /// host (`0.0.0.0`), which peers cannot dial (issue #396): without it they rely
    /// on learning this node's address from its datagram sources, which is correct
    /// on symmetric networks and wrong behind NAT. The unspecified host is refused
    /// here by `validate()` for the same reason.
    pub advertise: Option<String>,
    /// Seed member gossip addresses (`MQTTD_SWIM_SEEDS`).
    pub seeds: Vec<String>,
    /// 64-hex cluster gossip key, **inline** (`MQTTD_SWIM_KEY`). A raw secret; prefer
    /// [`Swim::key_file`] to keep it out of the config file (ADR 0046 T5). Mutually exclusive
    /// with `key_file`.
    pub key: Option<String>,
    /// Path to a file holding the 64-hex cluster gossip key (`MQTTD_SWIM_KEY_FILE`, ADR 0046 T5):
    /// the secret-by-reference form, mountable from a Kubernetes Secret so it never sits in the
    /// committed config file. Mutually exclusive with the inline `key`.
    pub key_file: Option<String>,
    /// Extra accepted gossip keys for zero-downtime rotation (`MQTTD_SWIM_KEY_ACCEPT`).
    pub key_accept: Vec<String>,
    /// Per-node gossip signature posture (`MQTTD_SWIM_SIGNED`): `require` or `off`.
    pub signed: Option<String>,
    /// Gossip anti-replay posture (`MQTTD_SWIM_REPLAY`): `require` or `off`.
    pub replay: Option<String>,
}

/// Durable (consensus-backed) session storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Durable {
    /// Whether durable sessions are enabled (`MQTTD_DURABLE_SESSIONS`). Default `true`
    /// (ADR 0029): durable is the secure, data-safe default.
    pub enabled: bool,
    /// Bounded lease-consensus voter set size (`MQTTD_LEASE_VOTERS`, ADR 0021). Default 5.
    pub lease_voters: u32,
    /// Disk high-water byte cap for the durable store (`MQTTD_STORE_MAX_BYTES`, ADR 0041 T5).
    pub store_max_bytes: Option<u64>,
    /// Min-replicas write floor (`MQTTD_MIN_REPLICAS`, issue #167; on by default
    /// since issue #239): a placement group whose replica set has shrunk below the
    /// floor REFUSES durable writes (QoS>=1 acks withheld, retained mutations queue)
    /// instead of silently promising less durability than configured — down to
    /// quorum-of-1 without this.
    ///
    /// Default [`MinReplicas::Majority`]: the floor is **derived** from the membership
    /// this node knows — `min(R, witness) / 2 + 1`, where the witness is the
    /// quorum-committed durable roster (or, before it is first pushed, the high-water
    /// observed membership and `runtime.ready_min_members`). Capped at the replication
    /// factor, so it is **1** while the node has never known a peer (a fresh single node
    /// stays fully operational) and **2** once it knows it belongs to a cluster of two or
    /// more — which is what the write quorum (`len/2+1`) already needs, so it costs no
    /// availability. The cap also means it stays 2 in a wider topology: on 5 or 7 nodes a
    /// group down to 2 copies still commits.
    ///
    /// An explicit integer keeps #167's absolute-floor meaning; `1` is the documented
    /// opt-out (accept single-copy acks). A floor above the replication factor is
    /// rejected at startup as unsatisfiable. With `enabled = false` there is no durable
    /// plane and no floor at all.
    pub min_replicas: MinReplicas,
    /// Opt in to **ephemeral durability** (`MQTTD_ALLOW_EPHEMERAL_DURABILITY`,
    /// presence = on; ADR 0029 as-delivered, issue #240): durable sessions ON with no
    /// `node.data_dir`, so the consensus-replicated state lives only in MEMORY and a
    /// correlated restart of a quorum LOSES acknowledged messages. **Off by default**:
    /// without it that combination REFUSES to start (and fails `--check-config` and a
    /// live reload). For development and tests only, loudly **warned** while active —
    /// styled on the bridge's `spool.allow_ephemeral_spool` (ADR 0060 T4). Durable
    /// explicitly off (`enabled = false`) never needs it: the lightweight in-memory
    /// store is an explicit choice already.
    pub allow_ephemeral: bool,
    /// Opt in to **per-message durability selection** (`MQTTD_ALLOW_RELAXED_PUBLISH`,
    /// presence = on; ADR 0072): an MQTT 5 publisher may weaken ITS OWN ack's
    /// meaning per message via the `mqttd-durability` user property
    /// (`local` = ack after the owner's fsync without the quorum wait;
    /// `relaxed` = ack after accept+submit, durability best-effort). **Off by
    /// default**: without it the property is ignored and every publish gets the
    /// full ack-after-quorum path — strictly stronger than requested, never
    /// weaker. The reservation this delivers is ADR 0018's: a relaxed mode "MAY
    /// be offered later as an opt-in, loudly logged".
    pub allow_relaxed_publish: bool,
    /// Durable **ownership domain** (`MQTTD_OWNERSHIP_DOMAIN`, ADR 0073):
    /// `"members"` (default) lets every admitted member own durable groups once the
    /// whole cluster advertises the capability (peer proto >= 8) — capacity scales
    /// with nodes; `"voters"` keeps ADR 0049's restriction to the lease-voter set —
    /// the loud opt-out escape hatch, and the automatic posture whenever ANY member
    /// (e.g. a rolled-back binary) lacks the capability.
    pub ownership_domain: OwnershipDomain,
}

/// The durable ownership domain (`durable.ownership_domain`, ADR 0073).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OwnershipDomain {
    /// Every admitted member may own durable groups (capability-gated cluster-wide).
    Members,
    /// Ownership restricted to the lease-voter set (ADR 0049's posture).
    Voters,
}

/// Online backup + restore of the durable state ([ADR 0062](../../../docs/adr/0062-online-backup-and-restore.md)).
///
/// The export is taken from the LIVE node — nothing is stopped — and is **per node**: a
/// cluster backup is the set of every node's export. Off by default (`every_secs = 0`),
/// because a scheduled backup with no destination would be a promise the broker cannot
/// keep; `mqttd --backup` triggers one on demand whenever `dir` is set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Audit {
    /// RFC 5424 syslog endpoint for the audit-chain export (`MQTTD_AUDIT_SYSLOG`),
    /// `host:port` over TCP. Unset = no export; the chain still lands in the broker
    /// log either way. The export sheds-and-counts when the endpoint is slower than
    /// the audit rate — see docs/AUDIT-SCHEMA.md for the record format, the SIEM
    /// boundary invariant, and the verification procedure.
    pub syslog: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Backup {
    /// Directory the export files are written to (`MQTTD_BACKUP_DIR`). Must NOT be inside
    /// `node.data_dir`: exports there grow the volume the disk watermark protects while
    /// being counted by nothing (the watcher stats only the four store files).
    pub dir: Option<String>,
    /// Seconds between scheduled exports (`MQTTD_BACKUP_EVERY`); `0` (the default) = no
    /// schedule. With a schedule, this is the RPO's cadence term.
    pub every_secs: u64,
    /// Export files kept per node id before the oldest is deleted (`MQTTD_BACKUP_KEEP`,
    /// default 7). Retention is per node so a directory shared by several nodes cannot
    /// have one node's rotation delete another's backups.
    pub keep: u32,
    /// A backup FILE or a DIRECTORY of them to import at startup (`MQTTD_RESTORE_FROM`).
    /// Only into a node whose data dir holds no store files yet — a restore never merges
    /// into a serving cluster.
    pub restore_from: Option<String>,
    /// Seconds the restore waits for the durable plane to become ready before giving up
    /// (`MQTTD_RESTORE_TIMEOUT`, default 300). A cluster restore must place sessions by
    /// the CONVERGED ring, so it waits for the mesh rather than importing into a
    /// single-member view.
    pub restore_timeout_secs: u64,
    /// Import a set that is MISSING a cluster member's export, forfeiting that node's
    /// sessions (`MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS`, default `false` = refuse).
    ///
    /// The default refuses, because a restore that silently drops a third of a cluster's
    /// sessions is the failure the coverage check exists to prevent. But the disaster this
    /// feature is for can take a node's data *and* its export together, and an
    /// all-or-nothing check then makes the SURVIVING nodes' backups unrestorable too — so
    /// there has to be a way to say "I know, restore the rest". The name states the
    /// consequence: data is lost. Every forfeited node and session id is named in the log,
    /// in `/statusz`, and in the on-disk `restored-from` stamp, permanently.
    pub restore_partial_accept_data_loss: bool,
}

impl Default for Backup {
    fn default() -> Self {
        Self {
            dir: None,
            every_secs: 0,
            keep: 7,
            restore_from: None,
            restore_timeout_secs: 300,
            restore_partial_accept_data_loss: false,
        }
    }
}

impl Default for Durable {
    fn default() -> Self {
        Self {
            enabled: true,
            lease_voters: 5,
            store_max_bytes: None,
            min_replicas: MinReplicas::Majority,
            allow_ephemeral: false,
            allow_relaxed_publish: false,
            ownership_domain: OwnershipDomain::Members,
        }
    }
}

/// The min-replicas write-floor posture (`durable.min_replicas`, issue #239): either an
/// absolute copy count or the derived majority-of-known-membership floor.
///
/// Spelled in TOML/env as an integer (`min_replicas = 2`) or the word `"majority"`
/// (the default). The two are not interchangeable: an integer is a promise the operator
/// makes about the topology, the word is a promise the *node* derives from the topology
/// it can actually witness — which is why the word, not a number, is the safe default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MinReplicasRaw", into = "MinReplicasRaw")]
pub enum MinReplicas {
    /// An absolute floor on the replica-set SIZE an append replicates over: `n` members,
    /// whatever the topology says. `1` disables the refusal (single-copy acks are
    /// accepted); it can never exceed the replication factor.
    ///
    /// It bounds the set, not the acks: `ClusterLog`'s write quorum is `len/2 + 1`, so
    /// `Count(3)` over a 3-member set still commits once 2 of the 3 copies hold the
    /// record. For the derived floor below the two coincide at every satisfiable size
    /// (a set of 2 or 3 needs 2 acks and the floor is 2), which is why the default
    /// promise — no single-copy durable acks once the node knows it has peers — is
    /// exactly what the gate enforces.
    Count(u32),
    /// The derived floor: a majority of the members this node knows about, capped at the
    /// replication factor. Resolved per node at write time, not at parse time.
    Majority,
}

/// The wire shape of [`MinReplicas`]: TOML/JSON sees an integer or a string, and the
/// curated `TryFrom` below is the only door in — an unrecognised word is a loud error,
/// never a silent posture.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum MinReplicasRaw {
    Count(u32),
    Word(String),
}

impl TryFrom<MinReplicasRaw> for MinReplicas {
    type Error = String;

    fn try_from(raw: MinReplicasRaw) -> Result<Self, Self::Error> {
        match raw {
            MinReplicasRaw::Count(0) => {
                Err("durable.min_replicas must be >= 1 (1 = no floor) or \"majority\"".to_string())
            }
            MinReplicasRaw::Count(n) => Ok(Self::Count(n)),
            MinReplicasRaw::Word(w) if w.eq_ignore_ascii_case("majority") => Ok(Self::Majority),
            MinReplicasRaw::Word(w) => Err(format!(
                "durable.min_replicas must be an integer >= 1 or \"majority\", got {w:?}"
            )),
        }
    }
}

impl From<MinReplicas> for MinReplicasRaw {
    fn from(v: MinReplicas) -> Self {
        match v {
            MinReplicas::Count(n) => Self::Count(n),
            MinReplicas::Majority => Self::Word("majority".to_string()),
        }
    }
}

impl MinReplicas {
    /// Parse the env spelling (`MQTTD_MIN_REPLICAS`): `majority` (any case) or an
    /// integer >= 1. Anything else is an error — the same curated door as the file.
    pub fn parse(v: &str) -> Result<Self, String> {
        let v = v.trim();
        if v.eq_ignore_ascii_case("majority") {
            return Ok(Self::Majority);
        }
        let n: u32 = v
            .parse()
            .map_err(|_| format!("expected an integer >= 1 or \"majority\", got {v:?}"))?;
        if n == 0 {
            return Err("must be >= 1 (1 = no floor) or \"majority\"".to_string());
        }
        Ok(Self::Count(n))
    }
}

impl std::fmt::Display for MinReplicas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count(n) => write!(f, "{n}"),
            Self::Majority => f.write_str("majority"),
        }
    }
}

/// Resource-governance caps + quotas (ADR 0041). `None`/`0` generally means unbounded,
/// matching the env behaviour.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Limits {
    /// Global connection cap (`MQTTD_MAX_CONNECTIONS`).
    pub max_connections: Option<u64>,
    /// Per-source-IP connection cap (`MQTTD_MAX_CONNECTIONS_PER_IP`).
    pub max_connections_per_ip: Option<u64>,
    /// Largest accepted MQTT packet, bytes (`MQTTD_MAX_PACKET_SIZE`).
    pub max_packet_size: Option<u64>,
    /// Per-client publish-rate cap, msg/s (`MQTTD_MAX_PUBLISH_RATE`).
    pub max_publish_rate: Option<u64>,
    /// Per-client offline-queue depth (`MQTTD_MAX_QUEUED_MESSAGES`). Bounds **disk**
    /// (the durable per-session queue), not the in-memory flow-control backlog.
    pub max_queued_messages: Option<u64>,
    /// Messages one online subscriber's **in-memory flow-control backlog** may hold
    /// before drop-oldest evicts (`MQTTD_MAX_BACKLOG_MESSAGES`, issue #241). Default
    /// 10 000 — the former hard-coded `MAX_BACKLOG`. Range `1..=10_000_000`; **0 is
    /// refused**, because ADR 0012 requires this structure be bounded and there is
    /// deliberately no "unbounded" setting. Bounds RAM, per subscriber, per node — never
    /// disk.
    pub max_backlog_messages: Option<u64>,
    /// Accounted bytes one online subscriber's in-memory flow-control backlog may hold
    /// before drop-oldest evicts (`MQTTD_MAX_BACKLOG_BYTES`, issue #241). Unset = **off**,
    /// which is exactly the pre-#241 behaviour; if set, at least 4096. Bounds RAM, per
    /// subscriber, per node — never disk.
    ///
    /// The eviction sheds messages that were already stored and already acked, so a value
    /// below `max_packet_size` makes that shed routine rather than exceptional (startup
    /// warns). `max_inflight_messages` is the loss-free lever.
    pub max_backlog_bytes: Option<u64>,
    /// Accounted bytes that may sit unwritten in one client's **outbound socket channel**
    /// before `QoS` 0 is shed (`MQTTD_MAX_OUTBOUND_BYTES`, issue #241). Unset = off; if
    /// set, at least 4096. The fixed 10 000-packet cap applies either way. Only the
    /// at-most-once class is shed — control packets and `QoS` 1/2 always flow. Bounds RAM.
    pub max_outbound_bytes: Option<u64>,
    /// A ceiling on the **effective outbound Receive Maximum**
    /// (`MQTTD_MAX_INFLIGHT_MESSAGES`, issue #241): the broker keeps at most
    /// `min(client Receive Maximum, this)` unacked `QoS` > 0 publishes per subscriber.
    /// Unset = the client's own value verbatim, i.e. 65 535 for every v3.1.1 client and
    /// any v5 client that sends no property. Range `1..=65_535`.
    ///
    /// A pure GATE: the surplus waits in the flow-control backlog, nothing is dropped —
    /// so this is the loss-free way to bound per-subscriber RAM. It costs throughput for
    /// a fast subscriber that legitimately keeps thousands in flight, and the deferred
    /// traffic lands in the backlog, so set it deliberately.
    ///
    /// Distinct from `receive_maximum`, which is the **inbound** grant the broker
    /// advertises to publishers.
    pub max_inflight_messages: Option<u16>,
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
    /// Process-memory high-water mark in bytes (`MQTTD_MEMORY_MAX_BYTES`, ADR 0041 T8).
    /// Above it the broker enters **brownout**: growth writes are refused while
    /// *subscriber* acks, reads, deletes, expiry and resumes continue. A **publisher's**
    /// `QoS` >= 1 ack is refused with the write it needed (v5 `0x97`, v3.1.1 no ack and a
    /// close), never granted for a message that was not stored. Unset = off.
    ///
    /// In a cluster the refusing node may be a PEER (an offline persistent subscriber's
    /// session usually lives on one); the refusal crosses the peer bus as a verdict, so
    /// the same answer reaches the publisher once every node on the message's path runs
    /// this release or newer. Mid-roll from an older build, a v5 publisher may still see
    /// the older withheld-ack close instead of `0x97`.
    ///
    /// A watermark, not a ceiling: nothing here can stop memory rising, so the container
    /// or cgroup limit remains the hard bound. What it buys is that pressure building
    /// over minutes degrades to read-mostly with a metric and a log line, instead of
    /// arriving as an OOM kill.
    pub memory_max_bytes: Option<u64>,
    /// How often BOTH watermark watchers sample their axis, seconds
    /// (`MQTTD_WATERMARK_POLL`, ADR 0041 T14). Default 10; a value outside `1..=300`
    /// is a startup error.
    ///
    /// This is the **detection-lag knob**, and it bounds the overshoot the watermark
    /// mechanism cannot prevent: `overshoot <= poll x peak growth rate` plus the one
    /// write already in flight. Within 10% of a mark the watchers re-check every
    /// `poll / 10` (floor 1 s), which is also how long a *cleared* brownout takes to
    /// lift — the publish outage above the mark outlives the pressure by at most that
    /// interval.
    ///
    /// One knob for both axes on purpose: a `/proc/self/status` read and four `stat`
    /// calls cost the same nothing, and no operator has a reason to sample disk and
    /// memory at different rates. Read at startup only — a reload reports the `limits`
    /// section as requires-restart (ADR 0041 §6).
    pub watermark_poll_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: None,
            max_connections_per_ip: None,
            max_packet_size: None,
            max_publish_rate: None,
            max_queued_messages: None,
            max_backlog_messages: None,
            max_backlog_bytes: None,
            max_outbound_bytes: None,
            max_inflight_messages: None,
            max_retained_messages: None,
            max_sessions: None,
            max_subscriptions_per_client: None,
            receive_maximum: None,
            topic_alias_max: None,
            queue_overflow: None,
            memory_max_bytes: None,
            watermark_poll_secs: 10,
        }
    }
}

/// Metrics export (ADR 0020).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
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

/// What to do with config keys this binary does not know (issue #230, ADR 0058 T4).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnknownConfigKeys {
    /// Fail the load, listing every unknown key (the typo net; the default).
    #[default]
    Refuse,
    /// Boot anyway; the loader reports each ignored key for the caller to log
    /// loudly. For the window where a config written for a NEWER broker reaches an
    /// older binary — a rollback within a major (the ADR 0039 promise), or a
    /// mixed-version fleet sharing one rendered config. Typos are ignored too while
    /// this is set: the posture deliberately trades the typo net for rollback
    /// safety, which is why it is NOT the default and why the chart's
    /// `--check-config` gate (strict) still fails a pod before it serves.
    Warn,
}

/// Runtime behaviour: shutdown, readiness gating, config auto-reload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Runtime {
    /// Graceful-shutdown drain window, seconds (`MQTTD_SHUTDOWN_GRACE`, ADR 0019). Default 30;
    /// `0` drains immediately (no wait for in-flight connections).
    pub shutdown_grace_secs: u64,
    /// Smallest mesh size `/readyz` accepts (`MQTTD_READY_MIN_MEMBERS`). Default 1.
    pub ready_min_members: usize,
    /// Filesystem config-watch poll interval, seconds (`MQTTD_CONFIG_WATCH`, ADR 0033).
    /// `0`/unset = signal-only (SIGHUP), the default.
    pub config_watch_secs: u64,
    /// Unknown-config-key policy (`MQTTD_CONFIG_UNKNOWN_KEYS`, issue #230 /
    /// ADR 0058 T4): `refuse` (default — typos fail the load, all listed) or
    /// `warn` (boot anyway, ignored keys logged — the rollback / mixed-version
    /// posture). The env var wins over the file for THIS knob, since the file
    /// carrying an unknown-to-this-binary key is the very situation it governs.
    pub config_unknown_keys: UnknownConfigKeys,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            shutdown_grace_secs: 30,
            ready_min_members: 1,
            config_watch_secs: 0,
            config_unknown_keys: UnknownConfigKeys::default(),
        }
    }
}

/// Parse `MQTTD_CONFIG_UNKNOWN_KEYS` (issue #230): `refuse` or `warn`.
fn parse_unknown_keys_policy(v: &str) -> Result<UnknownConfigKeys, ConfigError> {
    match v.to_ascii_lowercase().as_str() {
        "refuse" => Ok(UnknownConfigKeys::Refuse),
        "warn" => Ok(UnknownConfigKeys::Warn),
        other => Err(ConfigError::Invalid(format!(
            "MQTTD_CONFIG_UNKNOWN_KEYS must be \"refuse\" or \"warn\", got {other:?}"
        ))),
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

/// The cluster peer-frame body limit, mirrored from `mqtt_cluster::peer::MAX_FRAME`.
///
/// Duplicated rather than imported because `mqtt-config` does not depend on
/// `mqtt-cluster` and should not gain a peer-bus dependency to read one number.
/// `mqttd` sees both crates and asserts they are equal, so the copy cannot drift
/// silently — which is the only reason duplicating a protocol constant is
/// acceptable here.
pub const PEER_MAX_FRAME_BYTES: u64 = 16 * 1024 * 1024;

impl Config {
    /// Whether durability is **ephemeral** (#166): durable sessions are ON but no
    /// `data_dir` is set, so the consensus-backed replicated state lives only in memory.
    /// It survives a single node's loss (peers still hold it) but not a correlated restart
    /// of a quorum — acknowledged facts are then lost. The broker logs this loudly at
    /// startup; exposed as a predicate so the decision has one testable home rather than
    /// living only in a log-line condition.
    #[must_use]
    pub fn durability_is_ephemeral(&self) -> bool {
        self.durable.enabled && self.node.data_dir.is_none()
    }

    /// The issue #240 refusal, in one testable home shared by every gate: ephemeral
    /// durability ([`durability_is_ephemeral`](Self::durability_is_ephemeral)) without
    /// the explicit opt-in (`durable.allow_ephemeral`) is an invalid configuration.
    /// Called from [`validate`](Self::validate) — so startup, `--check-config`, and the
    /// reload acceptance gate all reject it by construction — and duplicated
    /// belt-and-braces in the broker's `runtime_precheck`, through this same helper so
    /// the message can never drift between gates.
    ///
    /// # Errors
    /// The refusal message, naming both remedies, when the config is ephemeral-durable
    /// without the opt-in.
    pub fn refuse_unopted_ephemeral_durability(&self) -> Result<(), String> {
        if self.durability_is_ephemeral() && !self.durable.allow_ephemeral {
            return Err(
                "EPHEMERAL durability REFUSED: durable sessions are ON (the default) but no \
                 data dir is set — the replicated state would live only in MEMORY, and a \
                 correlated restart of a quorum LOSES acknowledged messages. Either set \
                 MQTTD_DATA_DIR ([node] data_dir) and mount a volume for real durability, or \
                 opt into ephemeral operation for development/tests with \
                 MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 ([durable] allow_ephemeral = true). \
                 (MQTTD_DURABLE_SESSIONS=0 — the lightweight in-memory store — needs no \
                 opt-in.)"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Parse a strict TOML document into a `Config` and [`validate`](Self::validate) it.
    /// Unknown keys, type mismatches, and out-of-range values all fail here with a located
    /// message — nothing is silently ignored.
    ///
    /// # Errors
    /// [`ConfigError::Parse`] on a TOML/shape error, [`ConfigError::Invalid`] on a semantic one.
    pub fn from_toml(s: &str) -> Result<Self, ConfigError> {
        let cfg = Self::parse_toml(s, None)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse TOML with the unknown-key policy applied (issue #230, ADR 0058 T4).
    ///
    /// One tolerant pass collects the path of EVERY key the schema does not know;
    /// the policy — `env_policy` if given (the `MQTTD_CONFIG_UNKNOWN_KEYS` layer,
    /// which must beat a file that may itself be unreadable-strictly), else the
    /// file's own `runtime.config_unknown_keys`, else `refuse` — then decides:
    /// `refuse` fails listing all of them (the typo net, now with the complete
    /// list instead of first-error), `warn` returns the config with
    /// [`Config::ignored_keys`] filled for the caller to log. Type mismatches and
    /// malformed TOML always fail regardless of policy.
    fn parse_toml(s: &str, env_policy: Option<UnknownConfigKeys>) -> Result<Self, ConfigError> {
        let mut unknown: Vec<String> = Vec::new();
        // toml 1.x: constructing the deserializer parses the document, so the
        // syntax-error case surfaces here rather than inside serde_ignored.
        let de = toml::de::Deserializer::parse(s).map_err(|e| ConfigError::Parse(e.to_string()))?;
        let mut cfg: Config = serde_ignored::deserialize(de, |path| {
            unknown.push(path.to_string());
        })
        .map_err(|e| ConfigError::Parse(e.to_string()))?;
        if unknown.is_empty() {
            return Ok(cfg);
        }
        match env_policy.unwrap_or(cfg.runtime.config_unknown_keys) {
            UnknownConfigKeys::Refuse => Err(ConfigError::Parse(format!(
                "unknown config key(s): {} — a typo, or a config written for a NEWER \
                 broker version; set runtime.config_unknown_keys = \"warn\" (or \
                 MQTTD_CONFIG_UNKNOWN_KEYS=warn) to boot anyway during a rollback or \
                 mixed-version window, ignored keys logged (ADR 0058 T4)",
                unknown.join(", ")
            ))),
            UnknownConfigKeys::Warn => {
                cfg.ignored_keys = unknown;
                Ok(cfg)
            }
        }
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
        // The unknown-key policy from the ENV layer is peeked before the file
        // parse: env beats file everywhere else, and for THIS knob the file may be
        // exactly the thing that cannot be read strictly (issue #230).
        let env_policy = match std::env::var("MQTTD_CONFIG_UNKNOWN_KEYS")
            .ok()
            .filter(|v| !v.is_empty())
        {
            None => None,
            Some(v) => Some(parse_unknown_keys_policy(&v)?),
        };
        let mut cfg = match path {
            Some(p) => {
                let s = std::fs::read_to_string(p)
                    .map_err(|e| ConfigError::Parse(format!("reading {}: {e}", p.display())))?;
                Config::parse_toml(&s, env_policy)?
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
        on!("MQTTD_TLS_SESSION_CACHE", v, {
            self.tls.session_cache = v.parse().ok();
        });
        if get("MQTTD_TLS_ALLOW_TLS12").is_some() {
            self.tls.allow_tls12 = true;
        }
        if get("MQTTD_TLS_ALLOW_UNSAFE_TLS12_FEATURES").is_some() {
            self.tls.allow_unsafe_tls12_features = true;
        }

        // -- security -- (MQTTD_ALLOW_ANONYMOUS: presence = on; require_client_cert is derived,
        // has no env var by design)
        if get("MQTTD_ALLOW_ANONYMOUS").is_some() {
            self.security.allow_anonymous = true;
        }
        on!("MQTTD_MTLS_IDENTITY_SOURCE", v, {
            self.security.mtls_identity_source = Some(v);
        });
        on!("MQTTD_PASSWORD_FILE", v, {
            self.security.password_file = Some(v);
        });
        on!("MQTTD_ACL_FILE", v, {
            self.security.acl_file = Some(v);
        });
        on!("MQTTD_JWT_HS256_SECRET_FILE", v, {
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
        on!("MQTTD_OIDC_ISSUER", v, {
            self.security.oidc.issuer = Some(v);
        });
        on!("MQTTD_OIDC_AUDIENCE", v, {
            self.security.oidc.audience = Some(v);
        });
        on!("MQTTD_OIDC_JWKS_REFRESH", v, {
            self.security.oidc.jwks_refresh_secs = Some(num("MQTTD_OIDC_JWKS_REFRESH", &v)?);
        });
        on!("MQTTD_OIDC_MAX_STALE", v, {
            self.security.oidc.max_stale_secs = Some(num("MQTTD_OIDC_MAX_STALE", &v)?);
        });
        on!("MQTTD_OIDC_ALLOW_HTTP", _v, {
            // Presence = on, matching MQTTD_ALLOW_ANONYMOUS's convention for
            // loudly-insecure toggles.
            self.security.oidc.allow_http = true;
        });
        on!("MQTTD_OIDC_GROUPS_CLAIM", v, {
            self.security.oidc.groups_claim = Some(v);
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
        on!("MQTTD_SWIM_ADVERTISE", v, {
            self.cluster.swim.advertise = Some(v);
        });
        on!("MQTTD_SWIM_SEEDS", v, {
            self.cluster.swim.seeds = list(&v);
        });
        on!("MQTTD_SWIM_KEY", v, {
            self.cluster.swim.key = Some(v);
        });
        on!("MQTTD_SWIM_KEY_FILE", v, {
            self.cluster.swim.key_file = Some(v);
        });
        on!("MQTTD_SWIM_KEY_ACCEPT", v, {
            self.cluster.swim.key_accept = list(&v);
        });
        on!("MQTTD_REFOUND_GUARD", v, {
            self.cluster.refound_guard = !matches!(
                v.to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            );
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
        on!("MQTTD_MIN_REPLICAS", v, {
            self.durable.min_replicas = MinReplicas::parse(&v)
                .map_err(|e| ConfigError::Invalid(format!("MQTTD_MIN_REPLICAS: {e}")))?;
        });
        // Presence = on, matching MQTTD_ALLOW_ANONYMOUS / MQTTD_OIDC_ALLOW_HTTP: a
        // loudly-dangerous opt-in should not hinge on parsing "false" (issue #240).
        if get("MQTTD_ALLOW_EPHEMERAL_DURABILITY").is_some() {
            self.durable.allow_ephemeral = true;
        }
        // Same presence-=-on rule (ADR 0072): weakening ack semantics, even
        // publisher-requested, must not hinge on parsing "false".
        if get("MQTTD_ALLOW_RELAXED_PUBLISH").is_some() {
            self.durable.allow_relaxed_publish = true;
        }
        // Default ON since #508, so this must be able to express OFF — a
        // presence-only flag could no longer turn it off at all. Same falsey set
        // as MQTTD_REFOUND_GUARD and MQTTD_DURABLE_SESSIONS, the other
        // default-on switches.
        on!("MQTTD_SHARED_PREFER_LOCAL", v, {
            self.cluster.shared_prefer_local = !matches!(
                v.to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            );
        });
        on!("MQTTD_OWNERSHIP_DOMAIN", v, {
            self.durable.ownership_domain = match v.as_str() {
                "members" => OwnershipDomain::Members,
                "voters" => OwnershipDomain::Voters,
                other => {
                    return Err(ConfigError::Invalid(format!(
                        "MQTTD_OWNERSHIP_DOMAIN must be 'members' or 'voters', got {other:?}"
                    )))
                }
            };
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
        on!("MQTTD_MAX_BACKLOG_MESSAGES", v, {
            self.limits.max_backlog_messages = Some(num("MQTTD_MAX_BACKLOG_MESSAGES", &v)?);
        });
        on!("MQTTD_MAX_BACKLOG_BYTES", v, {
            self.limits.max_backlog_bytes = Some(num("MQTTD_MAX_BACKLOG_BYTES", &v)?);
        });
        on!("MQTTD_MAX_OUTBOUND_BYTES", v, {
            self.limits.max_outbound_bytes = Some(num("MQTTD_MAX_OUTBOUND_BYTES", &v)?);
        });
        on!("MQTTD_MAX_INFLIGHT_MESSAGES", v, {
            self.limits.max_inflight_messages = Some(num("MQTTD_MAX_INFLIGHT_MESSAGES", &v)?);
        });
        on!("MQTTD_MAX_RETAINED_MESSAGES", v, {
            self.limits.max_retained_messages = Some(num("MQTTD_MAX_RETAINED_MESSAGES", &v)?);
        });
        on!("MQTTD_HTTP_AUTH_URL", v, {
            self.security.http_auth.url = Some(v);
        });
        on!("MQTTD_HTTP_AUTH_TIMEOUT", v, {
            self.security.http_auth.timeout_secs = Some(num("MQTTD_HTTP_AUTH_TIMEOUT", &v)?);
        });
        on!("MQTTD_HTTP_AUTH_CACHE_SECS", v, {
            self.security.http_auth.cache_secs = Some(num("MQTTD_HTTP_AUTH_CACHE_SECS", &v)?);
        });
        on!("MQTTD_HTTP_AUTH_CACHE_MAX", v, {
            self.security.http_auth.cache_max = Some(num("MQTTD_HTTP_AUTH_CACHE_MAX", &v)?);
        });
        on!("MQTTD_HTTP_AUTH_ALLOW_HTTP", _v, {
            // Presence = on, matching MQTTD_OIDC_ALLOW_HTTP and MQTTD_ALLOW_ANONYMOUS:
            // a loudly-insecure toggle should not hinge on parsing "false".
            self.security.http_auth.allow_http = true;
        });
        on!("MQTTD_MEMORY_MAX_BYTES", v, {
            self.limits.memory_max_bytes = Some(num("MQTTD_MEMORY_MAX_BYTES", &v)?);
        });
        on!("MQTTD_WATERMARK_POLL", v, {
            self.limits.watermark_poll_secs = num("MQTTD_WATERMARK_POLL", &v)?;
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
        on!("MQTTD_CONFIG_UNKNOWN_KEYS", v, {
            self.runtime.config_unknown_keys = parse_unknown_keys_policy(&v)?;
        });
        on!("MQTTD_CONFIG_WATCH", v, {
            self.runtime.config_watch_secs = num("MQTTD_CONFIG_WATCH", &v)?;
        });

        // -- backup (ADR 0062) --
        on!("MQTTD_AUDIT_SYSLOG", v, {
            self.audit.syslog = Some(v);
        });
        on!("MQTTD_BACKUP_DIR", v, {
            self.backup.dir = Some(v);
        });
        on!("MQTTD_BACKUP_EVERY", v, {
            self.backup.every_secs = num("MQTTD_BACKUP_EVERY", &v)?;
        });
        on!("MQTTD_BACKUP_KEEP", v, {
            self.backup.keep = num("MQTTD_BACKUP_KEEP", &v)?;
        });
        on!("MQTTD_RESTORE_FROM", v, {
            self.backup.restore_from = Some(v);
        });
        on!("MQTTD_RESTORE_TIMEOUT", v, {
            self.backup.restore_timeout_secs = num("MQTTD_RESTORE_TIMEOUT", &v)?;
        });
        // A flag that FORFEITS data is turned on deliberately or not at all: unlike the
        // presence-flips-on flags, only an explicit truthy value counts, so an empty or
        // stray value cannot silently license a lossy restore.
        on!("MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS", v, {
            self.backup.restore_partial_accept_data_loss =
                matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes");
        });

        Ok(())
    }

    /// Validate that the configuration is internally consistent, in range, and that every
    /// insecure combination has been explicitly opted into.
    ///
    /// # Errors
    /// [`ConfigError::Invalid`] describing the first problem found.
    // One linear list of refusals — long by the number of settings it checks, not by
    // branching complexity (the same shape `overlay_from` carries above).
    #[allow(clippy::too_many_lines)]
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
        // 0 would refuse every durable write unconditionally; the meaningful "no floor"
        // is 1. The default is the derived `majority` posture (#239), which is always
        // satisfiable. The upper bound (<= replication factor) is checked at assembly,
        // where the factor is known.
        // Issue #513: a packet the broker will ACCEPT but cannot FORWARD.
        //
        // `MQTTD_MAX_PACKET_SIZE` has no upper bound while the peer frame body is
        // capped at 16 MiB (`mqtt_cluster::peer::MAX_FRAME`, "to bound memory from
        // a bad peer"). Above that the broker takes the packet, delivers it to
        // LOCAL subscribers, and the peer link refuses the frame — dropped with a
        // warning, no ack withheld, no retry at QoS 0. Remote subscribers silently
        // miss a message local ones received, which reads as a cluster-consistency
        // bug rather than a size limit.
        //
        // Refused rather than warned, and only when this node is CLUSTERED: a
        // standalone broker with a large packet size is perfectly valid and must
        // stay so. Both remedies are named, as #240's refusal does.
        //
        // The EFFECTIVE ceiling is what matters, not the raw Option: the field is
        // None unless an operator set it, while the enforced value is
        // WireLimits::default() (1 MiB). Gating on the Option is the bug main.rs
        // already documents at its own max_packet_size comparison — a check that
        // could not fire in the default configuration.
        let clustered = self.cluster.peer_bind.is_some() || !self.cluster.peers.is_empty();
        if clustered {
            if let Some(max) = self.limits.max_packet_size {
                if max > PEER_MAX_FRAME_BYTES {
                    return Err(ConfigError::Invalid(format!(
                        "MQTTD_MAX_PACKET_SIZE is {max} bytes, above the {PEER_MAX_FRAME_BYTES}-byte \
                         cluster peer-frame limit: packets larger than that are accepted from \
                         clients but cannot be forwarded to other nodes, so remote subscribers \
                         would silently miss messages local ones received. Lower it to \
                         {PEER_MAX_FRAME_BYTES} or below, or run this node standalone (no \
                         MQTTD_PEER_BIND, no MQTTD_PEERS)."
                    )));
                }
            }
        }
        if self.durable.min_replicas == MinReplicas::Count(0) {
            return Err(ConfigError::Invalid(
                "durable.min_replicas must be >= 1 (1 = no floor) or \"majority\"".to_string(),
            ));
        }
        // The watermark watchers' cadence (issue #243). Both ends are refusals of an
        // instruction that cannot have been meant: 0 would spin, and a mark sampled
        // less often than every five minutes cannot bound the overshoot the mechanism
        // has already conceded (ADR 0041 §6 — a nonsensical value is a startup error).
        if !(1..=300).contains(&self.limits.watermark_poll_secs) {
            return Err(ConfigError::Invalid(
                "limits.watermark_poll_secs must be between 1 and 300 seconds (default 10): \
                 below 1 s the poll cannot bound overshoot the mechanism has already lost, \
                 and a watermark sampled less often than every 5 minutes is decoration"
                    .to_string(),
            ));
        }
        if self.observability.otlp_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "observability.otlp_interval_secs must be >= 1".to_string(),
            ));
        }
        // Backup (ADR 0062). Each of these is a rollout-time refusal rather than a 03:00
        // surprise, which is the whole reason `--check-config` exists.
        if self.backup.every_secs > 0 && self.backup.dir.is_none() {
            return Err(ConfigError::Invalid(
                "backup.every_secs > 0 requires backup.dir (MQTTD_BACKUP_DIR): a scheduled \
                 backup with no destination would report success and write nothing"
                    .to_string(),
            ));
        }
        // Nothing durable to export: a scheduled or on-demand backup of a node with no data
        // dir would either write an empty file or refuse at run time. Refuse at the gate
        // instead, so `--check-config` says it before a rollout.
        if self.backup.dir.is_some() && self.node.data_dir.is_none() {
            return Err(ConfigError::Invalid(
                "backup.dir requires node.data_dir (MQTTD_DATA_DIR): a node with no durable \
                 store has nothing to export, and a file that looks like a backup of nothing \
                 is worse than an error"
                    .to_string(),
            ));
        }
        if self.backup.dir.is_some() && self.backup.keep == 0 {
            return Err(ConfigError::Invalid(
                "backup.keep must be >= 1 (MQTTD_BACKUP_KEEP): retention that keeps nothing \
                 deletes the export it just wrote"
                    .to_string(),
            ));
        }
        // A backup directory inside the data dir would grow the very volume the disk
        // watermark protects — and invisibly, since the watcher stats only the four store
        // files, so the node browns out (or fills the PV) from BACKUPS rather than data.
        if let (Some(backup_dir), Some(data_dir)) = (&self.backup.dir, &self.node.data_dir) {
            let backup = std::path::Path::new(backup_dir);
            let data = std::path::Path::new(data_dir);
            if backup == data || backup.starts_with(data) {
                return Err(ConfigError::Invalid(format!(
                    "backup.dir ({backup_dir}) is inside node.data_dir ({data_dir}): exports \
                     there grow the volume the disk watermark protects and are counted by \
                     nothing (store_watch stats only the four store files). Put backup.dir on \
                     a separate volume"
                )));
            }
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
        // Which certificate field is the principal is not a setting to get wrong quietly:
        // an unrecognised value must not degrade to the CN default, or a SAN-keyed ACL
        // would silently start matching against a CA-chosen Common Name (ADR 0004 T11).
        // The spellings are duplicated here rather than depending on mqtt-auth — this crate
        // deliberately has no broker dependencies; mtls::IdentitySource::parse is the one
        // that decides at use, and a test in mqtt-auth pins the two lists together.
        if let Some(s) = &self.security.mtls_identity_source {
            if !["cn", "san-dns", "san-uri", "san-email"]
                .contains(&s.trim().to_lowercase().as_str())
            {
                return Err(ConfigError::Invalid(format!(
                    "security.mtls_identity_source must be one of \"cn\", \"san-dns\", \
                     \"san-uri\", \"san-email\", got {s:?}"
                )));
            }
        }
        // The per-subscriber in-memory bounds (issue #241). Refused in `validate()` —
        // which startup, `--check-config` and the reload precheck all run — so all three
        // gates are covered by construction, with no separate startup-only check.
        self.refuse_out_of_range_subscriber_bounds()
            .map_err(ConfigError::Invalid)?;
        if let Some(p) = &self.limits.queue_overflow {
            if p != "drop-oldest" && p != "reject-newest" {
                return Err(ConfigError::Invalid(format!(
                    "limits.queue_overflow must be \"drop-oldest\" or \"reject-newest\", got {p:?}"
                )));
            }
        }
        // An advertised gossip address exists to be dialed by PEERS — the
        // unspecified host cannot be (issue #396: it loops back to the dialer's
        // own socket). Refuse it here rather than let it circulate.
        if let Some(adv) = &self.cluster.swim.advertise {
            let host = adv.rsplit_once(':').map_or(adv.as_str(), |(h, _)| h);
            if host.is_empty() || host == "0.0.0.0" || host == "[::]" || host == "::" {
                return Err(ConfigError::Invalid(format!(
                    "cluster.swim.advertise (MQTTD_SWIM_ADVERTISE) must be an address \
                     peers can dial, got {adv:?}: the unspecified host loops back to \
                     the dialer's own socket (issue #396)"
                )));
            }
        }
        // The gossip key is inline XOR by-reference — not both (ADR 0046 T5).
        if self.cluster.swim.key.is_some() && self.cluster.swim.key_file.is_some() {
            return Err(ConfigError::Invalid(
                "cluster.swim.key and cluster.swim.key_file are mutually exclusive \
                 (inline secret vs secret-by-reference)"
                    .to_string(),
            ));
        }
        // Ephemeral durability without the explicit opt-in (issue #240, ADR 0029
        // as-delivered): durable ON + no data_dir is quorum-of-RAM — refused rather
        // than warned. Checked last so a config broken in a more specific way is
        // reported for that reason first.
        self.refuse_unopted_ephemeral_durability()
            .map_err(ConfigError::Invalid)?;
        Ok(())
    }
}

impl Config {
    /// The per-subscriber in-memory bounds' ranges (issue #241, ADR 0041 T10).
    ///
    /// Its own function so [`Config::validate`] stays readable, and so every gate reaches
    /// the same refusal through the same helper — the messages cannot drift.
    fn refuse_out_of_range_subscriber_bounds(&self) -> Result<(), String> {
        if let Some(n) = self.limits.max_backlog_messages {
            if n == 0 || n > 10_000_000 {
                return Err(format!(
                    "limits.max_backlog_messages must be in 1..=10000000, got {n} \
                     (ADR 0012 requires the flow-control backlog be bounded: there is no \
                     unbounded setting, and 0 would evict every message it received)"
                ));
            }
        }
        for (field, v) in [
            ("max_backlog_bytes", self.limits.max_backlog_bytes),
            ("max_outbound_bytes", self.limits.max_outbound_bytes),
        ] {
            if let Some(n) = v {
                if n < 4096 {
                    return Err(format!(
                        "limits.{field} must be at least 4096 bytes, got {n} (a cap below one \
                         message evicts the whole queue on every arrival)"
                    ));
                }
            }
        }
        // The upper end is the type: `u16` cannot hold more than the protocol maximum.
        if self.limits.max_inflight_messages == Some(0) {
            return Err(
                "limits.max_inflight_messages must be in 1..=65535, got 0 (a zero \
                        in-flight window could never put a QoS>0 message on the wire)"
                    .to_string(),
            );
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
    "MQTTD_MTLS_IDENTITY_SOURCE",
    "MQTTD_PASSWORD_FILE",
    "MQTTD_ACL_FILE",
    "MQTTD_JWT_HS256_SECRET_FILE",
    "MQTTD_JWT_RS256_PEM",
    "MQTTD_JWT_ISSUER",
    "MQTTD_JWT_AUDIENCE",
    "MQTTD_OIDC_ISSUER",
    "MQTTD_OIDC_AUDIENCE",
    "MQTTD_OIDC_JWKS_REFRESH",
    "MQTTD_OIDC_MAX_STALE",
    "MQTTD_OIDC_ALLOW_HTTP",
    "MQTTD_OIDC_GROUPS_CLAIM",
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
    "MQTTD_SWIM_ADVERTISE",
    "MQTTD_SWIM_SEEDS",
    "MQTTD_SWIM_KEY",
    "MQTTD_SWIM_KEY_FILE",
    "MQTTD_SWIM_KEY_ACCEPT",
    "MQTTD_SWIM_SIGNED",
    "MQTTD_SWIM_REPLAY",
    "MQTTD_REFOUND_GUARD",
    // durable
    "MQTTD_DURABLE_SESSIONS",
    "MQTTD_LEASE_VOTERS",
    "MQTTD_MIN_REPLICAS",
    "MQTTD_STORE_MAX_BYTES",
    "MQTTD_ALLOW_EPHEMERAL_DURABILITY",
    "MQTTD_ALLOW_RELAXED_PUBLISH",
    "MQTTD_SHARED_PREFER_LOCAL",
    "MQTTD_OWNERSHIP_DOMAIN",
    // limits
    "MQTTD_MAX_CONNECTIONS",
    "MQTTD_MAX_CONNECTIONS_PER_IP",
    "MQTTD_MAX_PACKET_SIZE",
    "MQTTD_MAX_PUBLISH_RATE",
    "MQTTD_MAX_QUEUED_MESSAGES",
    "MQTTD_MAX_BACKLOG_MESSAGES",
    "MQTTD_MAX_BACKLOG_BYTES",
    "MQTTD_MAX_OUTBOUND_BYTES",
    "MQTTD_MAX_INFLIGHT_MESSAGES",
    "MQTTD_MAX_RETAINED_MESSAGES",
    "MQTTD_MAX_SESSIONS",
    "MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT",
    "MQTTD_RECEIVE_MAXIMUM",
    "MQTTD_TOPIC_ALIAS_MAX",
    "MQTTD_QUEUE_OVERFLOW",
    "MQTTD_MEMORY_MAX_BYTES",
    "MQTTD_WATERMARK_POLL",
    "MQTTD_HTTP_AUTH_URL",
    "MQTTD_HTTP_AUTH_TIMEOUT",
    "MQTTD_HTTP_AUTH_CACHE_SECS",
    "MQTTD_HTTP_AUTH_CACHE_MAX",
    "MQTTD_HTTP_AUTH_ALLOW_HTTP",
    // observability
    "MQTTD_OTLP_ENDPOINT",
    "MQTTD_OTLP_INTERVAL",
    // runtime
    "MQTTD_SHUTDOWN_GRACE",
    "MQTTD_READY_MIN_MEMBERS",
    "MQTTD_CONFIG_WATCH",
    // backup (ADR 0062)
    "MQTTD_BACKUP_DIR",
    "MQTTD_BACKUP_EVERY",
    "MQTTD_BACKUP_KEEP",
    "MQTTD_RESTORE_FROM",
    "MQTTD_RESTORE_TIMEOUT",
    "MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS",
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
        assert!(
            !c.durable.allow_ephemeral,
            "ephemeral durability is opt-in (#240)"
        );
        // Bare defaults are durable-on with no data dir — ephemeral durability, REFUSED
        // since #240 (the dedicated test below pins the message). With a data dir the
        // same defaults validate.
        assert!(c.validate().is_err());
        let mut c = c;
        c.node.data_dir = Some("/var/lib/mqttd".into());
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
            max_backlog_messages = 20000
            max_backlog_bytes = 67108864
            max_outbound_bytes = 33554432
            max_inflight_messages = 64
        "#;
        let c = Config::from_toml(toml).expect("valid config");
        assert_eq!(c.node.id, "n1");
        assert_eq!(c.listeners.tls_bind.as_deref(), Some("0.0.0.0:8883"));
        assert_eq!(c.durable.lease_voters, 3);
        assert_eq!(c.limits.max_connections, Some(10000));
        assert_eq!(c.limits.max_backlog_messages, Some(20000));
        assert_eq!(c.limits.max_backlog_bytes, Some(67_108_864));
        assert_eq!(c.limits.max_outbound_bytes, Some(33_554_432));
        assert_eq!(c.limits.max_inflight_messages, Some(64));
    }

    #[test]
    fn an_unknown_key_is_rejected() {
        // A typo must fail the load, not be silently ignored — and the error must
        // NAME the key and the escape hatch (issue #230).
        let err = Config::from_toml("[security]\nallow_anonymus = true\n")
            .expect_err("unknown key must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("security.allow_anonymus"), "{msg}");
        assert!(msg.contains("config_unknown_keys"), "{msg}");
    }

    /// Issue #230 / ADR 0058 T4: the refusal lists EVERY unknown key at once —
    /// first-error-only made fixing a rolled-back fleet's config a guess loop.
    #[test]
    fn the_refusal_lists_all_unknown_keys() {
        let err = Config::from_toml("[security]\nallow_anonymus = true\nfuture_knob = 1\n")
            .expect_err("unknown keys must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("security.allow_anonymus") && msg.contains("security.future_knob"),
            "{msg}"
        );
    }

    /// Issue #230 / ADR 0058 T4, the rollback shape: a config written for a NEWER
    /// broker (a key this binary has never heard of) boots an OLDER binary when the
    /// warn posture is set — in the file itself, or by the env layer — with the
    /// ignored keys reported for loud logging. Type mismatches still always fail.
    #[test]
    fn warn_mode_boots_a_newer_config_and_reports_the_ignored_keys() {
        // The knob in the file: the newer config ships its own skew posture.
        let c = Config::from_toml(
            "[runtime]\nconfig_unknown_keys = \"warn\"\n[durable]\nallow_ephemeral = true\n\
             knob_from_the_future = 7\n",
        )
        .expect("warn mode must boot a newer config");
        assert_eq!(c.ignored_keys, vec!["durable.knob_from_the_future"]);
        // The env layer wins even when the file says nothing.
        let c = Config::parse_toml(
            "[durable]\nknob_from_the_future = 7\n",
            Some(super::UnknownConfigKeys::Warn),
        )
        .expect("the env policy must apply to the file parse");
        assert_eq!(c.ignored_keys, vec!["durable.knob_from_the_future"]);
        // A type mismatch is never ignorable — it is not an unknown key.
        assert!(Config::parse_toml(
            "[durable]\nlease_voters = \"three\"\n",
            Some(super::UnknownConfigKeys::Warn)
        )
        .is_err());
        // And a bad env value is a loud error, not a silent default.
        assert!(super::parse_unknown_keys_policy("wran").is_err());
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
        // 0 would refuse every durable write unconditionally; "no floor" is 1 (#167).
        assert!(Config::from_toml("[durable]\nmin_replicas = 0\n").is_err());
        // Only the exact word `majority` is a valid non-numeric floor (issue #239) —
        // a plausible-sounding synonym must not silently become some other posture.
        assert!(Config::from_toml("[durable]\nmin_replicas = \"quorum\"\n").is_err());
        // shutdown_grace_secs = 0 is *valid* (drain immediately) — not out of range.
        assert!(Config::from_toml(
            "[runtime]\nshutdown_grace_secs = 0\n[durable]\nallow_ephemeral = true\n"
        )
        .is_ok());
    }

    /// Issue #241 — the four per-subscriber in-memory bounds are refused out of range in
    /// `validate()`, which is what makes startup, `--check-config` and the reload
    /// precheck all covered by construction (they each run `validate()`; there is
    /// deliberately no separate startup-only check).
    #[test]
    fn the_new_per_subscriber_bounds_are_refused_out_of_range() {
        let cfg = |body: &str| {
            let mut c =
                Config::from_toml("[durable]\nallow_ephemeral = true\n").expect("base parses");
            let extra: super::Limits =
                toml::from_str(body).expect("the limits fragment must parse");
            c.limits = super::Limits {
                max_backlog_messages: extra.max_backlog_messages,
                max_backlog_bytes: extra.max_backlog_bytes,
                max_outbound_bytes: extra.max_outbound_bytes,
                max_inflight_messages: extra.max_inflight_messages,
                ..c.limits
            };
            c
        };

        // A zero count is refused, and the message says WHY there is no unbounded
        // setting — an operator reaching for 0 wants "off", and off does not exist here.
        let err = cfg("max_backlog_messages = 0")
            .validate()
            .expect_err("0 must be refused");
        let msg = err.to_string();
        assert!(msg.contains("max_backlog_messages"), "{msg}");
        assert!(msg.contains("ADR 0012"), "{msg}");
        assert!(cfg("max_backlog_messages = 10000001").validate().is_err());
        // Both boundaries accept.
        assert!(cfg("max_backlog_messages = 1").validate().is_ok());
        assert!(cfg("max_backlog_messages = 10000000").validate().is_ok());

        // A byte cap below one message is a mistake, not a tight budget.
        for field in ["max_backlog_bytes", "max_outbound_bytes"] {
            let err = cfg(&format!("{field} = 1024"))
                .validate()
                .expect_err("a sub-4096 byte cap must be refused");
            assert!(err.to_string().contains(field), "{err}");
            assert!(cfg(&format!("{field} = 4096")).validate().is_ok());
        }

        // The in-flight ceiling: 0 could never put a QoS>0 message on the wire, and
        // 65 535 (the protocol maximum) is the top of the range.
        assert!(cfg("max_inflight_messages = 0").validate().is_err());
        assert!(cfg("max_inflight_messages = 1").validate().is_ok());
        assert!(cfg("max_inflight_messages = 65535").validate().is_ok());
        // Above u16 the TOML does not even deserialise — the type is the check.
        assert!(toml::from_str::<super::Limits>("max_inflight_messages = 65536").is_err());

        // And the unset shape — every deployment that changes nothing — validates.
        assert!(cfg("").validate().is_ok());
    }

    /// Issue #239 — the min-replicas write floor is ON by default, as the *derived*
    /// majority posture (`"majority"`), and an explicit integer still sets #167's
    /// absolute floor. Both spellings come from TOML and from the env.
    #[test]
    fn min_replicas_defaults_to_the_derived_majority_floor_and_accepts_explicit_floors() {
        assert_eq!(
            Config::default().durable.min_replicas,
            super::MinReplicas::Majority,
            "the shipped default must be the derived majority floor, not 1"
        );
        let c = Config::from_toml("[durable]\nallow_ephemeral = true\nmin_replicas = 2\n").unwrap();
        assert_eq!(c.durable.min_replicas, super::MinReplicas::Count(2));
        let c =
            Config::from_toml("[durable]\nallow_ephemeral = true\nmin_replicas = \"majority\"\n")
                .unwrap();
        assert_eq!(c.durable.min_replicas, super::MinReplicas::Majority);
        // The opt-out is still spelled 1.
        let c = Config::from_toml("[durable]\nallow_ephemeral = true\nmin_replicas = 1\n").unwrap();
        assert_eq!(c.durable.min_replicas, super::MinReplicas::Count(1));

        let env = |v: &'static str| {
            let mut c = Config::default();
            c.overlay_from(|k| (k == "MQTTD_MIN_REPLICAS").then(|| v.to_string()))
                .map(|()| c.durable.min_replicas)
        };
        assert_eq!(env("3").unwrap(), super::MinReplicas::Count(3));
        assert_eq!(env("majority").unwrap(), super::MinReplicas::Majority);
        assert_eq!(env("MAJORITY").unwrap(), super::MinReplicas::Majority);
        assert!(env("sometimes").is_err(), "a bad env word must be loud");
        assert!(env("0").is_err(), "0 is not a floor");

        // Display round-trips into logs and error text.
        assert_eq!(super::MinReplicas::Majority.to_string(), "majority");
        assert_eq!(super::MinReplicas::Count(2).to_string(), "2");
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
        assert!(Config::from_toml(
            "[limits]\nqueue_overflow = \"reject-newest\"\n[durable]\nallow_ephemeral = true\n"
        )
        .is_ok());
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
        let mut c = Config::from_toml(
            "[node]\nid = \"from-file\"\n[durable]\nallow_ephemeral = true\nlease_voters = 3\n",
        )
        .unwrap();
        c.overlay_from(getter(&[
            ("MQTTD_NODE_ID", "from-env"),
            ("MQTTD_LEASE_VOTERS", "5"),
        ]))
        .unwrap();
        assert_eq!(c.node.id, "from-env");
        assert_eq!(c.durable.lease_voters, 5);
    }

    /// The watermark poll is the operator's detection-lag knob (issue #243), so a
    /// nonsensical value must be a startup error rather than a silently useless
    /// watcher: `0` would spin, and a watermark sampled less often than every five
    /// minutes is decoration. `validate()` is the single refusal point, which is what
    /// makes it cover startup, `--check-config` and the reload precheck alike.
    #[test]
    fn the_watermark_poll_is_bounded_between_one_second_and_five_minutes() {
        let toml = |v: &str| {
            format!("[limits]\nwatermark_poll_secs = {v}\n[durable]\nallow_ephemeral = true\n")
        };
        for bad in ["0", "301", "86400"] {
            let err = Config::from_toml(&toml(bad)).expect_err("must be refused");
            assert!(
                err.to_string()
                    .contains("watermark_poll_secs must be between 1 and 300"),
                "the error must name the field and the range: {err}"
            );
        }
        for good in ["1", "10", "300"] {
            assert!(
                Config::from_toml(&toml(good)).is_ok(),
                "{good} is inside the range"
            );
        }
        assert_eq!(
            Config::default().limits.watermark_poll_secs,
            10,
            "the default cadence is the one the docs and the ADR quote"
        );
    }

    /// Env → config for the poll knob, plus the numeric-parse refusal. The count and
    /// `distinct_value` companions live in the two sweep tests below; this one pins the
    /// value itself.
    #[test]
    fn the_watermark_poll_overlays_from_the_environment() {
        let mut c = Config::default();
        c.overlay_from(getter(&[("MQTTD_WATERMARK_POLL", "3")]))
            .unwrap();
        assert_eq!(c.limits.watermark_poll_secs, 3);
        let mut c = Config::default();
        let err = c
            .overlay_from(getter(&[("MQTTD_WATERMARK_POLL", "soon")]))
            .expect_err("a non-numeric cadence must not be ignored");
        assert!(
            err.to_string().contains("MQTTD_WATERMARK_POLL"),
            "the error must name the variable: {err}"
        );
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

    /// The re-found guard is ON by default and takes the same falsey vocabulary. It must
    /// be reachable by ENV specifically: the escape hatch is used mid-incident, when
    /// editing a mounted config and rolling the pod is the thing you cannot do.
    #[test]
    fn the_refound_guard_defaults_on_and_env_can_disable_it() {
        assert!(
            Config::default().cluster.refound_guard,
            "a node must not serve from an empty store after re-founding unless told to"
        );
        for (v, want) in [
            ("0", false),
            ("false", false),
            ("OFF", false),
            ("no", false),
            ("1", true),
            ("yes", true),
        ] {
            let mut c = Config::default();
            c.overlay_from(getter(&[("MQTTD_REFOUND_GUARD", v)]))
                .unwrap();
            assert_eq!(c.cluster.refound_guard, want, "MQTTD_REFOUND_GUARD={v:?}");
        }
        // And from the file, for a deliberate, reviewed opt-out.
        let c = Config::from_toml(
            "[cluster]\nrefound_guard = false\n[durable]\nallow_ephemeral = true\n",
        )
        .unwrap();
        assert!(!c.cluster.refound_guard);
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

    /// ADR 0073: `MQTTD_OWNERSHIP_DOMAIN` accepts exactly "members" (the default)
    /// and "voters" (the escape hatch); anything else is refused naming the values.
    #[test]
    fn ownership_domain_parses_both_values_and_refuses_others() {
        let mut c = Config::default();
        c.overlay_from(getter(&[("MQTTD_OWNERSHIP_DOMAIN", "voters")]))
            .unwrap();
        assert_eq!(c.durable.ownership_domain, super::OwnershipDomain::Voters);
        c.overlay_from(getter(&[("MQTTD_OWNERSHIP_DOMAIN", "members")]))
            .unwrap();
        assert_eq!(c.durable.ownership_domain, super::OwnershipDomain::Members);
        let err = Config::default()
            .overlay_from(getter(&[("MQTTD_OWNERSHIP_DOMAIN", "domains")]))
            .expect_err("an unknown domain must be refused");
        let msg = err.to_string();
        assert!(msg.contains("members") && msg.contains("voters"), "{msg}");
        // And the TOML side round-trips the same enum.
        let c = Config::from_toml(
            "[node]\ndata_dir = \"/tmp/x\"\n[durable]\nownership_domain = \"voters\"\n",
        )
        .unwrap();
        assert_eq!(c.durable.ownership_domain, super::OwnershipDomain::Voters);
    }

    #[test]
    fn an_unset_env_leaves_file_and_defaults_intact() {
        // Overlaying an empty environment changes nothing.
        let base = Config::from_toml(
            "[node]\nid = \"keep\"\n[limits]\nmax_connections = 42\n\
                 [durable]\nallow_ephemeral = true\n",
        )
        .unwrap();
        let mut c = base.clone();
        c.overlay_from(getter(&[])).unwrap();
        assert_eq!(c, base);
    }

    /// A value guaranteed to *differ from the default* for `var`, so overlaying it alone must
    /// mutate the config. Booleans/enums need a specific opposite-of-default value; numerics need
    /// a parseable one; everything else takes an arbitrary non-empty string.
    fn distinct_value(var: &str) -> &'static str {
        match var {
            // Data-safe defaults are ON, so only a falsey value *changes* them.
            "MQTTD_DURABLE_SESSIONS" | "MQTTD_REFOUND_GUARD" | "MQTTD_SHARED_PREFER_LOCAL" => "off",
            // Presence flips these on (default off).
            "MQTTD_ALLOW_ANONYMOUS"
            | "MQTTD_OIDC_ALLOW_HTTP"
            | "MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS"
            | "MQTTD_ALLOW_EPHEMERAL_DURABILITY"
            | "MQTTD_ALLOW_RELAXED_PUBLISH" => "1",
            // Enums: any valid, non-default (default None) member.
            "MQTTD_SWIM_SIGNED" | "MQTTD_SWIM_REPLAY" => "require",
            "MQTTD_QUEUE_OVERFLOW" => "reject-newest",
            "MQTTD_MTLS_IDENTITY_SOURCE" => "san-dns",
            // Default is "members" (ADR 0073), so only the escape hatch *changes* it.
            "MQTTD_OWNERSHIP_DOMAIN" => "voters",
            // The default is the derived `majority` posture (#239), so only an
            // explicit integer *changes* it.
            "MQTTD_MIN_REPLICAS" => "2",
            // Byte caps are refused below 4096 (a value under one message is a
            // configuration mistake, not a tight budget), so "7" would not validate.
            "MQTTD_MAX_BACKLOG_BYTES" | "MQTTD_MAX_OUTBOUND_BYTES" => "8192",
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
            | "MQTTD_MAX_INFLIGHT_MESSAGES"
            | "MQTTD_MAX_BACKLOG_MESSAGES"
            | "MQTTD_MAX_RETAINED_MESSAGES"
            | "MQTTD_MAX_SESSIONS"
            | "MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT"
            | "MQTTD_RECEIVE_MAXIMUM"
            | "MQTTD_TOPIC_ALIAS_MAX"
            | "MQTTD_MEMORY_MAX_BYTES"
            | "MQTTD_WATERMARK_POLL"
            | "MQTTD_HTTP_AUTH_TIMEOUT"
            | "MQTTD_HTTP_AUTH_CACHE_SECS"
            | "MQTTD_HTTP_AUTH_CACHE_MAX"
            | "MQTTD_OTLP_INTERVAL"
            | "MQTTD_SHUTDOWN_GRACE"
            | "MQTTD_READY_MIN_MEMBERS"
            | "MQTTD_CONFIG_WATCH"
            | "MQTTD_OIDC_JWKS_REFRESH"
            | "MQTTD_OIDC_MAX_STALE"
            | "MQTTD_BACKUP_EVERY" => "7",
            // The default is already 7 (backup.keep) / 300 (restore timeout), so "7" would
            // change nothing and the totality sweep would read as a missing mapping.
            "MQTTD_BACKUP_KEEP" | "MQTTD_RESTORE_TIMEOUT" => "3",
            // Paths / addresses / lists / keys.
            _ => "x-sentinel",
        }
    }

    #[test]
    fn oidc_env_maps_and_is_https_gated_at_use_not_parse() {
        let mut c = Config::default();
        c.overlay_from(|k| match k {
            "MQTTD_OIDC_ISSUER" => Some("https://idp.test/realms/iot".to_string()),
            "MQTTD_OIDC_AUDIENCE" => Some("mqttd".to_string()),
            "MQTTD_OIDC_JWKS_REFRESH" => Some("120".to_string()),
            "MQTTD_OIDC_MAX_STALE" => Some("3600".to_string()),
            "MQTTD_OIDC_GROUPS_CLAIM" => Some("roles".to_string()),
            "MQTTD_OIDC_ALLOW_HTTP" => Some("1".to_string()),
            _ => None,
        })
        .unwrap();
        assert_eq!(
            c.security.oidc.issuer.as_deref(),
            Some("https://idp.test/realms/iot")
        );
        assert_eq!(c.security.oidc.audience.as_deref(), Some("mqttd"));
        assert_eq!(c.security.oidc.jwks_refresh_secs, Some(120));
        assert_eq!(c.security.oidc.max_stale_secs, Some(3600));
        assert_eq!(c.security.oidc.groups_claim.as_deref(), Some("roles"));
        assert!(c.security.oidc.allow_http, "presence sets the flag");
    }

    /// A typo in the identity source must be a startup error, never a silent fall back to
    /// the CN default — a SAN-keyed ACL evaluated against a CA-chosen Common Name is a
    /// privilege change, not a cosmetic one (ADR 0004 T11).
    #[test]
    fn an_unknown_mtls_identity_source_is_rejected_rather_than_defaulted() {
        for good in ["cn", "san-dns", "san-uri", "san-email", " SAN-DNS "] {
            let toml = format!(
                "[security]\nmtls_identity_source = \"{good}\"\n[durable]\nallow_ephemeral = true\n"
            );
            assert!(Config::from_toml(&toml).is_ok(), "{good:?} should be valid");
        }
        for bad in ["", "san", "dns", "common-name", "san_dns"] {
            let toml = format!("[security]\nmtls_identity_source = \"{bad}\"\n");
            let err = Config::from_toml(&toml).expect_err("must be rejected");
            assert!(
                err.to_string().contains("mtls_identity_source"),
                "error should name the field: {err}"
            );
        }
        // Unset is the CN default, and stays representable as "absent" rather than a string.
        assert!(Config::default().security.mtls_identity_source.is_none());
        let mut c = Config::default();
        c.overlay_from(|k| (k == "MQTTD_MTLS_IDENTITY_SOURCE").then(|| "san-uri".to_string()))
            .unwrap();
        assert_eq!(c.security.mtls_identity_source.as_deref(), Some("san-uri"));
    }

    #[test]
    fn the_gossip_key_is_inline_xor_by_reference() {
        // Either form alone validates; both together is rejected (ADR 0046 T5).
        let flag = "\n[durable]\nallow_ephemeral = true\n";
        assert!(Config::from_toml(&format!("[cluster.swim]\nkey = \"deadbeef\"{flag}")).is_ok());
        assert!(Config::from_toml(&format!(
            "[cluster.swim]\nkey_file = \"/run/secrets/swim\"{flag}"
        ))
        .is_ok());
        let err = Config::from_toml(
            "[cluster.swim]\nkey = \"deadbeef\"\nkey_file = \"/run/secrets/swim\"\n",
        )
        .unwrap_err();
        match err {
            super::ConfigError::Invalid(m) => assert!(m.contains("mutually exclusive")),
            super::ConfigError::Parse(m) => panic!("wrong error kind: {m}"),
        }
    }

    #[test]
    fn an_unspecified_swim_advertise_is_refused() {
        // The advertise exists to be dialed by PEERS (issue #396): the unspecified
        // host loops back to the dialer's own socket, so claiming it is always a
        // misconfiguration. A routable advertise validates; 0.0.0.0/[::] do not.
        let flag = "\n[durable]\nallow_ephemeral = true\n";
        assert!(Config::from_toml(&format!(
            "[cluster.swim]\nadvertise = \"node-1.internal:7946\"{flag}"
        ))
        .is_ok());
        for bad in ["0.0.0.0:7946", "[::]:7946", ":::7946"] {
            let err = Config::from_toml(&format!("[cluster.swim]\nadvertise = \"{bad}\"{flag}"))
                .unwrap_err();
            match err {
                super::ConfigError::Invalid(m) => {
                    assert!(m.contains("peers can dial"), "{bad}: {m}");
                }
                super::ConfigError::Parse(m) => panic!("wrong error kind for {bad}: {m}"),
            }
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
            // 79 before #249 (which itself included #241's four backlog/in-flight knobs)
            // plus this change's six MQTTD_BACKUP_* / MQTTD_RESTORE_* variables,
            // plus MQTTD_ALLOW_RELAXED_PUBLISH (ADR 0072),
            // plus MQTTD_OWNERSHIP_DOMAIN (ADR 0073),
            // plus MQTTD_SWIM_ADVERTISE (issue #396),
            // plus MQTTD_SHARED_PREFER_LOCAL (ADR 0077 T4 follow-up).
            89,
            "the MQTTD_* surface changed — update ENV_VARS"
        );
        // Issue #239: MQTTD_MIN_REPLICAS was wired in `overlay_from` but never
        // inventoried here, so the totality sweep below never touched it.
        assert!(seen.contains("MQTTD_MIN_REPLICAS"));
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

    /// Issue #240: durable ON (the default) with no data dir is REFUSED at validation —
    /// a warning log is not a substitute for refusing the configuration — and the
    /// refusal names both ways out.
    /// Issue #513: a clustered node may not accept packets it cannot forward.
    ///
    /// The three cases matter separately. STANDALONE with a huge packet size is
    /// valid and must stay valid — the refusal is about the cluster bus, not about
    /// large messages. CLUSTERED at or below the frame limit is fine. Only
    /// clustered ABOVE it is refused, because there the broker would deliver to
    /// local subscribers and silently drop the peer frame, which presents as a
    /// consistency bug rather than a limit.
    #[test]
    fn a_clustered_node_refuses_a_packet_size_it_cannot_forward() {
        const OVER: u64 = 32 * 1024 * 1024;
        const AT_LIMIT: u64 = 16 * 1024 * 1024;

        let mut standalone = Config::default();
        standalone.node.data_dir = Some("/var/lib/mqttd".into());
        standalone.limits.max_packet_size = Some(OVER);
        assert!(
            standalone.validate().is_ok(),
            "a standalone broker may accept packets larger than a peer frame"
        );

        let mut clustered = standalone.clone();
        clustered.cluster.peer_bind = Some("0.0.0.0:7001".into());
        let err = clustered
            .validate()
            .expect_err("clustered + oversized must be refused");
        let msg = err.to_string();
        for named in ["MQTTD_MAX_PACKET_SIZE", "16777216", "standalone"] {
            assert!(msg.contains(named), "the refusal must name {named}: {msg}");
        }

        let mut at_limit = clustered.clone();
        at_limit.limits.max_packet_size = Some(AT_LIMIT);
        assert!(
            at_limit.validate().is_ok(),
            "exactly at the frame limit is forwardable, so it is allowed"
        );

        // The DEFAULT clustered configuration must pass: `max_packet_size` is None
        // there and the enforced ceiling is 1 MiB, far under the limit. A check
        // that refused the default would be the mirror of the bug main.rs
        // documents — one that could never fire in it.
        let mut default_clustered = Config::default();
        default_clustered.node.data_dir = Some("/var/lib/mqttd".into());
        default_clustered.cluster.peer_bind = Some("0.0.0.0:7001".into());
        assert!(
            default_clustered.validate().is_ok(),
            "the default clustered configuration must not be refused"
        );
    }

    #[test]
    fn durable_on_without_a_data_dir_refuses_naming_both_remedies() {
        let err = Config::default()
            .validate()
            .expect_err("bare defaults are ephemeral durability and must be refused");
        assert!(matches!(err, super::ConfigError::Invalid(_)));
        let msg = err.to_string();
        for remedy in [
            "MQTTD_DATA_DIR",
            "MQTTD_ALLOW_EPHEMERAL_DURABILITY",
            "allow_ephemeral",
        ] {
            assert!(msg.contains(remedy), "must name {remedy}; message: {msg}");
        }
    }

    /// Issue #240: each documented remedy, alone, unblocks validation — the opt-in
    /// flag, a real data dir, or durable explicitly off (which needs no flag). Guards
    /// each against being accidentally narrowed later.
    #[test]
    fn each_remedy_individually_unblocks_validation() {
        let mut c = Config::default();
        c.durable.allow_ephemeral = true;
        assert!(c.validate().is_ok(), "the explicit ephemeral opt-in passes");

        let mut c = Config::default();
        c.node.data_dir = Some("/var/lib/mqttd".into());
        assert!(c.validate().is_ok(), "a data dir is real durability");

        let mut c = Config::default();
        c.durable.enabled = false;
        assert!(
            c.validate().is_ok(),
            "durable OFF is an explicit choice already — no flag needed"
        );

        // And from the env, the same three postures (the flag is presence = on).
        for pairs in [
            &[("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "1")][..],
            &[("MQTTD_DATA_DIR", "/var/lib/mqttd")][..],
            &[("MQTTD_DURABLE_SESSIONS", "0")][..],
        ] {
            let mut c = Config::default();
            c.overlay_from(getter(pairs)).unwrap();
            assert!(c.validate().is_ok(), "{pairs:?} must validate");
        }
        // Presence = on even for a falsey-looking value, per the pinned dangerous-
        // opt-in convention (MQTTD_ALLOW_ANONYMOUS / MQTTD_OIDC_ALLOW_HTTP).
        let mut c = Config::default();
        c.overlay_from(getter(&[("MQTTD_ALLOW_EPHEMERAL_DURABILITY", "0")]))
            .unwrap();
        assert!(c.durable.allow_ephemeral, "any value enables the opt-in");
    }

    /// #166 — the ephemeral-durability predicate: durable ON + no `data_dir` is the one
    /// dangerous combination (quorum-of-RAM), and only that one.
    #[test]
    fn ephemeral_durability_is_exactly_durable_on_without_a_data_dir() {
        let mut c = Config::default(); // durable defaults ON, data_dir None
        assert!(c.node.data_dir.is_none() && c.durable.enabled);
        assert!(
            c.durability_is_ephemeral(),
            "durable ON + no data_dir = ephemeral"
        );

        // #240: the opt-in flag PERMITS ephemeral mode — it must not redefine it, or
        // the startup EPHEMERAL warning would fall silent exactly when it matters.
        c.durable.allow_ephemeral = true;
        assert!(
            c.durability_is_ephemeral(),
            "the opt-in permits ephemeral mode; it does not redefine it"
        );
        c.durable.allow_ephemeral = false;

        c.node.data_dir = Some("/var/lib/mqttd".into());
        assert!(
            !c.durability_is_ephemeral(),
            "a data_dir makes it real durability"
        );

        c.node.data_dir = None;
        c.durable.enabled = false;
        assert!(
            !c.durability_is_ephemeral(),
            "durable OFF is not ephemeral — it is in-memory by design"
        );
    }

    /// ADR 0062: the three backup misconfigurations `--check-config` must catch before a
    /// rollout, each naming the keys involved. The containment check is the load-bearing
    /// one: exports written inside the data dir grow the volume the disk watermark
    /// protects while being counted by nothing, so the node browns out from BACKUPS.
    #[test]
    fn a_backup_dir_inside_the_data_dir_is_a_config_error() {
        let base = || {
            let mut c = Config::default();
            c.node.data_dir = Some("/var/lib/mqttd".into());
            c
        };

        // Equal to the data dir.
        let mut c = base();
        c.backup.dir = Some("/var/lib/mqttd".into());
        let msg = c
            .validate()
            .expect_err("equal paths must refuse")
            .to_string();
        assert!(msg.contains("backup.dir"), "{msg}");
        assert!(msg.contains("node.data_dir"), "{msg}");

        // Nested under it.
        let mut c = base();
        c.backup.dir = Some("/var/lib/mqttd/backups".into());
        let msg = c
            .validate()
            .expect_err("a nested path must refuse")
            .to_string();
        assert!(msg.contains("/var/lib/mqttd/backups"), "{msg}");

        // Beside it: fine.
        let mut c = base();
        c.backup.dir = Some("/var/backups/mqttd".into());
        c.validate()
            .expect("a separate volume is the supported shape");

        // A destination on a node with no durable store.
        let mut c = Config::default();
        c.node.data_dir = None;
        c.durable.enabled = false;
        c.backup.dir = Some("/var/backups/mqttd".into());
        let msg = c
            .validate()
            .expect_err("a backup of a node with no durable store must refuse")
            .to_string();
        assert!(msg.contains("backup.dir requires node.data_dir"), "{msg}");

        // A schedule with no destination.
        let mut c = base();
        c.backup.every_secs = 900;
        let msg = c
            .validate()
            .expect_err("a schedule with no destination must refuse")
            .to_string();
        assert!(msg.contains("backup.every_secs"), "{msg}");
        assert!(msg.contains("MQTTD_BACKUP_DIR"), "{msg}");

        // Retention that keeps nothing.
        let mut c = base();
        c.backup.dir = Some("/var/backups/mqttd".into());
        c.backup.keep = 0;
        let msg = c.validate().expect_err("keep = 0 must refuse").to_string();
        assert!(msg.contains("backup.keep"), "{msg}");
    }
}
