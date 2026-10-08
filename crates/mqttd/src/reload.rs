//! Hot-reloadable security policy ([ADR 0032](../../../docs/adr/0032-hot-reloadable-security-policy.md)).
//!
//! The authorizer and authenticator live behind [`tokio::sync::watch`] channels; the
//! connection reads the **current** value on every check ([`crate::conn::ConnPolicy`]), so a
//! reload reaches **live** connections. A [`Reloader`] holds the senders and a `build`
//! closure that re-reads the configured files; [`Reloader::reload`] swaps the policy in
//! place — **atomically and fail-safe**: it builds the new values first and publishes them
//! only if the build succeeds, so a malformed/missing file leaves the running policy
//! unchanged (never fail open, never brick). Every reload is audited.
//!
//! The `build` closure is injected (the binary supplies one that re-reads the `MQTTD_*`
//! files), so the swap logic is testable without touching the filesystem or environment.

use std::sync::{Arc, RwLock};

use mqtt_auth::signed_gossip::RevocationList;
use mqtt_auth::{Authenticator, Authorizer};
use mqtt_observability::metrics::Metrics;
use mqtt_observability::AuditSink;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

/// Poison-tolerant read of a shared `RwLock` (a panic mid-write must not brick reloads).
fn read_lock<T>(l: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}
/// Poison-tolerant write of a shared `RwLock`.
fn write_lock<T>(l: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// What a `build` closure returns: the freshly-read `(authorizer, authenticator)`, or an
/// error string (a missing/unparseable file) that **aborts** the swap.
pub type BuildResult = Result<(Arc<dyn Authorizer>, Arc<dyn Authenticator>), String>;

/// A [`ConfigSource`] runtime-acceptance gate: `Err` if the freshly-loaded config cannot be
/// built into the broker's derived runtime values (ADR 0046 T4).
pub type ConfigPrecheck = Box<dyn Fn(&mqtt_config::Config) -> Result<(), String> + Send + Sync>;

/// A [`ConfigSource`] live-apply hook, called `(old, new)` on a committed reload (ADR 0046 T4).
/// Returns the changed sections that need a restart to take effect, for the
/// [`ReloadOutcome`].
pub type ConfigApply =
    Box<dyn Fn(&mqtt_config::Config, &mqtt_config::Config) -> Vec<String> + Send + Sync>;

/// What one reload did (ADR 0081 T6): the admin API returns it, so an operator learns
/// whether the change took without reading the log.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ReloadOutcome {
    /// Why the reload ran (`signal`, `watch`, `admin`).
    pub trigger: String,
    /// Whether the new config and policy were swapped in.
    pub applied: bool,
    /// Why not, when not applied. The running config and policy are unchanged.
    pub error: Option<String>,
    /// The top-level config sections that differ from the previous config.
    pub changed_sections: Vec<String>,
    /// Of those, the ones that need a restart to take effect (staged, not live).
    pub requires_restart: Vec<String>,
}

/// How many rules in `set` are enabled (the `mqttd_rules_loaded` gauge).
#[must_use]
pub fn enabled_rules(set: &mqtt_rules::RuleSet) -> usize {
    set.rules().iter().filter(|r| r.enabled()).count()
}

/// The top-level sections of `new` that differ from `old`, by their TOML names.
fn changed_sections(old: &mqtt_config::Config, new: &mqtt_config::Config) -> Vec<String> {
    let (Ok(serde_json::Value::Object(o)), Ok(serde_json::Value::Object(n))) =
        (serde_json::to_value(old), serde_json::to_value(new))
    else {
        return Vec::new();
    };
    n.iter()
        .filter(|(k, v)| o.get(*k) != Some(*v))
        .map(|(k, _)| k.clone())
        .collect()
}

/// What a TLS `build` closure returns: a freshly-built acceptor from the renewed
/// cert/key/client-CA, or an error string (a missing/unparseable file) that aborts the swap.
pub type TlsBuildResult = Result<TlsAcceptor, String>;

/// The live cluster-bus revocation list the gossip verifier consults per datagram
/// (ADR 0022 T7): `None` until a CRL is configured. Shared between the verifier and the
/// [`Reloader`], which swaps a freshly-parsed list in on reload.
pub type SwimCrlSlot = Arc<RwLock<Option<RevocationList>>>;

/// What a gossip-CRL `build` closure returns: the freshly-parsed, CA-verified revocation
/// list, or an error string (a missing/unparseable/unsigned CRL) that aborts the swap.
pub type SwimCrlBuildResult = Result<RevocationList, String>;

/// What a client-CRL `build` closure returns for the identity sweep (ADR 0040 T2): the
/// freshly-parsed revoked-serial list from `MQTTD_TLS_CRL` (whose signature the TLS
/// verifier enforces per handshake), or an error string that aborts the swap.
pub type ClientCrlBuildResult = Result<RevocationList, String>;

/// What a peer-bus TLS `build` closure returns (ADR 0040 T4): a freshly-built
/// acceptor + connector from the re-read cluster CA / node cert / key, or an error
/// string that aborts the swap.
pub type PeerTlsBuildResult = Result<(TlsAcceptor, tokio_rustls::TlsConnector), String>;

/// What an admin-TLS build returns (ADR 0081 T18): the rebuilt material, as a commit that
/// swaps it in once every other build of the same reload has succeeded, or why it could
/// not be built.
pub type AdminTlsBuildResult = Result<Box<dyn FnOnce() + Send>, String>;

/// What a gossip-signer `build` closure returns (issue #269): a freshly-built signer over
/// the re-read peer-bus leaf certificate + key, or an error string that aborts the swap.
/// Swapped into the live [`SignerSlot`](mqtt_cluster::swim_auth::SignerSlot) the SWIM
/// driver reads per datagram, so a rotated leaf signs — and is embedded in — the next
/// outgoing gossip datagram.
pub type GossipSignerBuildResult = Result<Arc<dyn mqtt_cluster::swim_auth::GossipSign>, String>;

/// What a rules `build` closure returns (ADR 0083): the freshly-loaded rule set, or why
/// the rules file does not load — which aborts the whole reload, keeping the running
/// rules (and everything else) in force.
pub type RulesBuildResult = Result<Arc<mqtt_rules::RuleSet>, String>;

/// Whole-config hot reload (ADR 0046 T4). When a [`ConfigSource`] is attached, every
/// [`Reloader::reload`] first re-loads the config file (defaults < file < `MQTTD_*` env),
/// validates it, and swaps it into the shared `live` cell **before** the policy is rebuilt —
/// so the policy build (which reads paths from `live`) sees the new config. Validate-before-swap
/// is preserved end to end: if the new config is invalid, or the policy build against it fails,
/// the live config is rolled back and nothing changes.
pub struct ConfigSource {
    /// The running config, shared with the binary's policy `build` closures (they read the
    /// current snapshot each reload). Swapped on a committed reload, rolled back on rejection.
    pub live: Arc<RwLock<mqtt_config::Config>>,
    /// The config-file path (`--config` / `MQTTD_CONFIG`), or `None` for defaults + env only.
    pub path: Option<std::path::PathBuf>,
    /// Runtime acceptance gate: returns `Err` if the freshly-loaded config could not be built
    /// into the derived runtime values the broker boots with (wire/queue limits, quotas) — so a
    /// config that would not start is never swapped in live. Supplied by the binary.
    pub precheck: ConfigPrecheck,
    /// Applied on a committed reload `(old, new)`: pushes the live-swappable settings (quotas)
    /// to the hub and logs every changed non-live section as requires-restart. Supplied by the
    /// binary (it owns the runtime the settings feed).
    pub apply: ConfigApply,
}

impl std::fmt::Debug for ConfigSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigSource")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// The `watch` receivers to wire into [`crate::conn::ConnPolicy`].
pub struct Handles {
    /// Current authorizer; re-read per publish/subscribe.
    pub authz: watch::Receiver<Arc<dyn Authorizer>>,
    /// Current authenticator; re-read per CONNECT.
    pub auth: watch::Receiver<Arc<dyn Authenticator>>,
    /// Current TLS acceptor; read per accept by the TLS listener. `None` until a TLS
    /// listener registers one via [`Reloader::attach_tls`].
    pub tls: Option<watch::Receiver<TlsAcceptor>>,
}

impl std::fmt::Debug for Handles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handles").finish_non_exhaustive()
    }
}

/// The loaded config's identity (ADR 0054 T3): sha-256 of the config file's bytes
/// plus a per-process load generation. Shared with `/statusz`; the convergence
/// check across a fleet is "same checksum everywhere". Without a config file
/// (env-only), the checksum is of the empty input — still stable per build of
/// the env, and the generation still counts applied reloads.
#[derive(Debug, Default)]
pub struct ConfigStamp {
    checksum: std::sync::RwLock<String>,
    generation: std::sync::atomic::AtomicU64,
}

impl ConfigStamp {
    /// Record a successful load of `file_bytes` (empty when no config file).
    pub fn record(&self, file_bytes: &[u8]) {
        let sum = sha256_hex(file_bytes);
        *self
            .checksum
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = sum;
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    /// `(checksum, generation)`; empty checksum = nothing recorded yet.
    #[must_use]
    pub fn read(&self) -> (String, u64) {
        (
            self.checksum
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            self.generation.load(std::sync::atomic::Ordering::Acquire),
        )
    }
}

/// One reload attempt, as the rule statistics and the admin API report it (ADR 0084).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadRecord {
    /// When it ran.
    pub at: std::time::SystemTime,
    /// Why (`signal`, `watch`, `admin`, `admin-rules`).
    pub trigger: String,
    /// Whether it was applied.
    pub applied: bool,
    /// Why not, when not: the full text, which can quote a config line holding a secret.
    pub error: Option<String>,
    /// How many rejected attempts in a row before this one were rejected with the same
    /// trigger and error (a broken file the watcher retries every poll repeats; it does
    /// not flood). 0 for an applied one: two applied reloads are two changes.
    pub repeats: u32,
}

impl ReloadRecord {
    /// The part of the reload that failed, never the text (ADR 0084): the component the
    /// reloader names in front of its error (`config`, `rules`, `tls`, `admin tls`, …),
    /// or `policy` for the ACL and authenticator build, whose errors have no such name.
    #[must_use]
    pub fn error_kind(&self) -> Option<&'static str> {
        self.error.as_deref().map(error_kind)
    }
}

/// The component names the reloader puts in front of an error, `"<name>: …"`.
const ERROR_KINDS: [&str; 8] = [
    "config",
    "tls",
    "gossip crl",
    "client crl",
    "peer tls",
    "gossip signer",
    "admin tls",
    "rules",
];

/// See [`ReloadRecord::error_kind`].
#[must_use]
pub fn error_kind(error: &str) -> &'static str {
    let head = error.split_once(':').map_or("", |(head, _)| head);
    ERROR_KINDS
        .into_iter()
        .find(|k| *k == head)
        .unwrap_or("policy")
}

/// The last reload attempt (ADR 0084), written by the [`Reloader`] under its reload lock
/// — so attempts are recorded in the order they ran — and read by the rule statistics and
/// the admin API.
#[derive(Debug, Default)]
pub struct LastReload(std::sync::Mutex<Option<ReloadRecord>>);

impl LastReload {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<ReloadRecord>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record an attempt.
    pub fn record(&self, trigger: &str, applied: bool, error: Option<&str>) {
        let mut last = self.lock();
        let repeats = match &*last {
            Some(prev)
                if !applied
                    && !prev.applied
                    && prev.trigger == trigger
                    && prev.error.as_deref() == error =>
            {
                prev.repeats.saturating_add(1)
            }
            _ => 0,
        };
        *last = Some(ReloadRecord {
            at: std::time::SystemTime::now(),
            trigger: trigger.to_string(),
            applied,
            error: error.map(String::from),
            repeats,
        });
    }

    /// The last attempt, if any ran since the process started.
    #[must_use]
    pub fn get(&self) -> Option<ReloadRecord> {
        self.lock().clone()
    }
}

/// Hex sha-256 of `bytes` (the config-checksum hash, ADR 0054 T3).
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
    mqtt_core::hex_lower(digest.as_ref())
}

/// Holds the swap channels + the file-rereading `build` closure for SIGHUP reload.
pub struct Reloader {
    authz_tx: watch::Sender<Arc<dyn Authorizer>>,
    auth_tx: watch::Sender<Arc<dyn Authenticator>>,
    audit: Arc<dyn AuditSink>,
    metrics: Option<Arc<Metrics>>,
    /// Set by [`attach_config_stamp`](Self::attach_config_stamp): updated (and
    /// mirrored to `config_info`) on every successful reload (ADR 0054 T3).
    config_stamp: Option<Arc<ConfigStamp>>,
    /// Set by [`attach_last_reload`](Self::attach_last_reload): every attempt, applied
    /// or rejected (ADR 0084).
    last_reload: Option<Arc<LastReload>>,
    build: Box<dyn Fn() -> BuildResult + Send + Sync>,
    /// Set by [`attach_tls`](Self::attach_tls) when a TLS listener is active; the acceptor
    /// is rebuilt and swapped as part of the same atomic, validate-before-swap reload.
    tls_tx: Option<watch::Sender<TlsAcceptor>>,
    tls_build: Option<Box<dyn Fn() -> TlsBuildResult + Send + Sync>>,
    /// Set by [`attach_swim_crl`](Self::attach_swim_crl) when a cluster-bus CRL is
    /// configured (ADR 0022 T7); rebuilt and swapped in the same atomic reload.
    swim_crl: Option<SwimCrlSlot>,
    swim_crl_build: Option<Box<dyn Fn() -> SwimCrlBuildResult + Send + Sync>>,
    /// Set by [`attach_identity_sweep`](Self::attach_identity_sweep): after a
    /// successful swap, the hub sweeps live sessions against the new policy
    /// (ADR 0040 T2).
    sweep_hub: Option<tokio::sync::mpsc::UnboundedSender<crate::hub::HubCommand>>,
    /// Re-reads the client-listener CRL's revoked serials for the sweep; `None` when
    /// no `MQTTD_TLS_CRL` is configured (the sweep then checks users + connect-ACL only).
    client_crl_build: Option<Box<dyn Fn() -> ClientCrlBuildResult + Send + Sync>>,
    /// Set by [`attach_peer_tls`](Self::attach_peer_tls) (ADR 0040 T4): the peer-bus
    /// acceptor/connector senders + rebuild closure, swapped in the same atomic reload.
    peer_tls: Option<(
        watch::Sender<TlsAcceptor>,
        watch::Sender<tokio_rustls::TlsConnector>,
    )>,
    peer_tls_build: Option<Box<dyn Fn() -> PeerTlsBuildResult + Send + Sync>>,
    /// Set by [`attach_admin_tls`](Self::attach_admin_tls): rebuilds the admin listener's
    /// TLS material from the live config (ADR 0081 T18).
    admin_tls_build: Option<Box<dyn Fn() -> AdminTlsBuildResult + Send + Sync>>,
    /// Set by [`attach_gossip_signer`](Self::attach_gossip_signer) (issue #269): the SWIM
    /// driver's live signing identity, rebuilt from the re-read peer-bus leaf + key and
    /// swapped in the same atomic reload — a rotated leaf is embedded in the next
    /// outgoing gossip datagram instead of surviving as a startup snapshot.
    gossip_signer: Option<Arc<mqtt_cluster::swim_auth::SignerSlot>>,
    gossip_signer_build: Option<Box<dyn Fn() -> GossipSignerBuildResult + Send + Sync>>,
    /// Set by [`attach_rules`](Self::attach_rules) (ADR 0083): the live rule set the
    /// connections read per publish, and the closure that re-reads the rules file.
    rules_tx: Option<watch::Sender<Arc<mqtt_rules::RuleSet>>>,
    rules_build: Option<Box<dyn Fn() -> RulesBuildResult + Send + Sync>>,
    /// Set by [`attach_config_source`](Self::attach_config_source) (ADR 0046 T4): the whole
    /// config is re-loaded, validated, and swapped ahead of the policy rebuild.
    config_source: Option<ConfigSource>,
    /// Serializes reloads: SIGHUP, the file watcher and the admin API can fire at once,
    /// and the config swap / rollback must not interleave.
    in_progress: std::sync::Mutex<()>,
}

impl std::fmt::Debug for Reloader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reloader").finish_non_exhaustive()
    }
}

impl Reloader {
    /// Create the reloader and the [`Handles`] to wire into the connection policy. `initial`
    /// is the startup-built `(authorizer, authenticator)`; `build` re-reads the sources on
    /// each [`reload`](Self::reload).
    pub fn new(
        initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>),
        audit: Arc<dyn AuditSink>,
        build: impl Fn() -> BuildResult + Send + Sync + 'static,
    ) -> (Self, Handles) {
        Self::with_metrics(initial, audit, None, build)
    }

    /// Like [`new`](Self::new), but also increments the `security_reloads` metric (by
    /// outcome) on every reload. `metrics` is `None` in tests that don't assert on it.
    pub fn with_metrics(
        initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>),
        audit: Arc<dyn AuditSink>,
        metrics: Option<Arc<Metrics>>,
        build: impl Fn() -> BuildResult + Send + Sync + 'static,
    ) -> (Self, Handles) {
        let (authz_tx, authz) = watch::channel(initial.0);
        let (auth_tx, auth) = watch::channel(initial.1);
        (
            Reloader {
                config_stamp: None,
                last_reload: None,
                authz_tx,
                auth_tx,
                audit,
                metrics,
                build: Box::new(build),
                tls_tx: None,
                tls_build: None,
                swim_crl: None,
                swim_crl_build: None,
                sweep_hub: None,
                client_crl_build: None,
                peer_tls: None,
                peer_tls_build: None,
                admin_tls_build: None,
                gossip_signer: None,
                gossip_signer_build: None,
                rules_tx: None,
                rules_build: None,
                config_source: None,
                in_progress: std::sync::Mutex::new(()),
            },
            Handles {
                authz,
                auth,
                tls: None,
            },
        )
    }

    /// Register a reloadable TLS acceptor so the next SIGHUP also rebuilds it from the
    /// renewed cert/key/client-CA. `initial` is the startup-built acceptor; `build`
    /// re-reads the PEM files on each reload. Returns the [`watch::Receiver`] the TLS
    /// accept loop reads per accept (so the renewed material is served on the next
    /// handshake; in-flight TLS sessions, already past their handshake, are undisturbed).
    ///
    /// The acceptor reload is folded into the *same* atomic, validate-before-swap reload as
    /// the ACL/authenticator: if any of the three fails to build, none is swapped.
    /// Attach the shared config stamp (ADR 0054 T3), updated on every successful
    /// reload and mirrored to `config_info{checksum}`.
    pub fn attach_config_stamp(&mut self, stamp: Arc<ConfigStamp>) {
        self.config_stamp = Some(stamp);
    }

    /// Record every reload attempt in `last` (ADR 0084): the rule statistics and the
    /// admin API report the last one.
    pub fn attach_last_reload(&mut self, last: Arc<LastReload>) {
        self.last_reload = Some(last);
    }

    pub fn attach_tls(
        &mut self,
        initial: TlsAcceptor,
        build: impl Fn() -> TlsBuildResult + Send + Sync + 'static,
    ) -> watch::Receiver<TlsAcceptor> {
        let (tx, rx) = watch::channel(initial);
        self.tls_tx = Some(tx);
        self.tls_build = Some(Box::new(build));
        rx
    }

    /// Register the cluster-bus gossip CRL (ADR 0022 T7) so a reload re-reads and swaps it
    /// through the same atomic, validate-before-swap path. `slot` is the live list the
    /// gossip verifier consults per datagram; `build` re-reads and CA-verifies the CRL
    /// file. A freshly-published CRL therefore revokes a node's gossip on the next
    /// datagram after the reload — no restart.
    pub fn attach_swim_crl(
        &mut self,
        slot: SwimCrlSlot,
        build: impl Fn() -> SwimCrlBuildResult + Send + Sync + 'static,
    ) {
        self.swim_crl = Some(slot);
        self.swim_crl_build = Some(Box::new(build));
    }

    /// Register the identity sweep (ADR 0040 T2): after every **successful** reload the
    /// hub receives the new policy and re-evaluates live sessions against it, evicting
    /// identity-revoked ones (CRL'd certificate, removed password user, connect-ACL
    /// deny). `client_crl_build` re-reads the client-listener CRL's serials — `None`
    /// when no client CRL is configured. A failed reload sweeps nothing (the running
    /// policy did not change).
    pub fn attach_identity_sweep(
        &mut self,
        hub: tokio::sync::mpsc::UnboundedSender<crate::hub::HubCommand>,
        client_crl_build: Option<Box<dyn Fn() -> ClientCrlBuildResult + Send + Sync>>,
    ) {
        self.sweep_hub = Some(hub);
        self.client_crl_build = client_crl_build;
    }

    /// Register the peer-bus TLS material for reload (ADR 0040 T4, paying the
    /// ADR 0032 deferred item): `build` re-reads the cluster CA / node cert / key,
    /// and both sides of the bus read the current value per handshake — a rotated
    /// cluster cert is served on the next peer handshake, folded into the same
    /// atomic validate-before-swap reload as everything else.
    pub fn attach_peer_tls(
        &mut self,
        acceptor_tx: watch::Sender<TlsAcceptor>,
        connector_tx: watch::Sender<tokio_rustls::TlsConnector>,
        build: Box<dyn Fn() -> PeerTlsBuildResult + Send + Sync>,
    ) {
        self.peer_tls = Some((acceptor_tx, connector_tx));
        self.peer_tls_build = Some(build);
    }

    /// Register the admin listener's TLS material for reload (ADR 0081 T18): `build`
    /// re-reads `admin.cert` / `admin.key` / `admin.client_ca` (and the cluster CA and
    /// node certificate the admin plane also uses) from the live config, which a config
    /// reload has already swapped, and returns a commit. Folded into the same atomic
    /// validate-before-swap reload: a bad certificate rejects the whole reload and the
    /// running admin TLS stays in force.
    pub fn attach_admin_tls(
        &mut self,
        build: impl Fn() -> AdminTlsBuildResult + Send + Sync + 'static,
    ) {
        self.admin_tls_build = Some(Box::new(build));
    }

    /// Register the SWIM gossip signing identity for reload (issue #269, the second half
    /// of hot peer-bus rotation): `build` re-reads the peer-bus leaf certificate + key and
    /// constructs a fresh signer; on a successful reload it is swapped into `slot` — the
    /// live cell the SWIM driver reads **per datagram** — so the rotated leaf signs, and
    /// is embedded in, the very next outgoing gossip datagram. Folded into the same
    /// atomic validate-before-swap reload: a bad leaf/key rejects the whole reload and
    /// the running signer (like every other policy object) stays in force.
    pub fn attach_gossip_signer(
        &mut self,
        slot: Arc<mqtt_cluster::swim_auth::SignerSlot>,
        build: impl Fn() -> GossipSignerBuildResult + Send + Sync + 'static,
    ) {
        self.gossip_signer = Some(slot);
        self.gossip_signer_build = Some(Box::new(build));
    }

    /// Register the rule engine for reload (ADR 0083): `build` re-reads the rules file
    /// named by the live config, and a clean load is swapped into `tx` — the rule set
    /// every connection reads per publish — so the next publish runs the new rules. Folded
    /// into the same atomic validate-before-swap reload: a rules file that does not load
    /// rejects the whole reload and the running rules stay in force.
    pub fn attach_rules(
        &mut self,
        tx: watch::Sender<Arc<mqtt_rules::RuleSet>>,
        build: impl Fn() -> RulesBuildResult + Send + Sync + 'static,
    ) {
        self.rules_tx = Some(tx);
        self.rules_build = Some(Box::new(build));
    }

    /// Register the whole-config source for reload (ADR 0046 T4): each [`reload`](Self::reload)
    /// re-loads the config file, validates it, and swaps it into the shared `live` cell before
    /// the policy is rebuilt — so a config-file edit (a new ACL path, a changed quota) takes
    /// effect on `SIGHUP`/watch, folded into the same atomic validate-before-swap.
    pub fn attach_config_source(&mut self, source: ConfigSource) {
        self.config_source = Some(source);
    }

    /// Re-read the sources and swap the policy in place — **validate-before-swap**: build the
    /// new authorizer, authenticator, *and* (if a TLS listener is attached) the TLS acceptor
    /// first; publish them only if **every** build succeeded. On any failure nothing is
    /// swapped and the running policy is left untouched (never fail open, never brick). Every
    /// outcome is audited (`security.reload`) and metered. Returns whether the swap applied.
    ///
    /// `trigger` records *why* the reload fired — `"signal"` for `SIGHUP`, `"watch"` for the
    /// filesystem watcher (ADR 0033) — carried into the audit event and the metric label so an
    /// operator can tell a manual reload from an auto-applied one.
    pub fn reload(&self, trigger: &str) -> bool {
        self.reload_with_outcome(trigger).applied
    }

    /// The committed config and its stamp `(checksum, generation)`, read under the reload
    /// lock: a reload stages its candidate in the shared cell before validating it, so a
    /// reader that did not wait could see a config that is then rejected (ADR 0081 T6).
    /// `None` without a config source.
    pub fn committed_config(&self) -> Option<(mqtt_config::Config, (String, u64))> {
        let _no_reload_in_flight = self
            .in_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cs = self.config_source.as_ref()?;
        let config = read_lock(&cs.live).clone();
        let stamp = self
            .config_stamp
            .as_ref()
            .map(|s| s.read())
            .unwrap_or_default();
        Some((config, stamp))
    }

    /// [`reload`](Self::reload), reporting what happened: whether it applied, why not,
    /// and which config sections changed and which of those need a restart (ADR 0081 T6).
    #[allow(clippy::too_many_lines)]
    pub fn reload_with_outcome(&self, trigger: &str) -> ReloadOutcome {
        let _one_at_a_time = self
            .in_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // ADR 0046 T4: when a config source is attached, re-load + validate the whole config and
        // swap it into `live` *before* rebuilding the policy (which reads paths from `live`).
        // `committed` carries the (old, new) pair so any downstream build failure can roll back —
        // validate-before-swap holds for the config too.
        let committed = match &self.config_source {
            None => None,
            Some(cs) => {
                let new = match mqtt_config::Config::load(cs.path.as_deref()) {
                    Ok(c) => c,
                    Err(e) => return self.reject(trigger, &format!("config: {e}")),
                };
                // Same loud line the boot path emits (issue #230): a hot-reload
                // under the warn posture must not ignore keys more quietly.
                for key in &new.ignored_keys {
                    tracing::warn!(
                        key = %key,
                        "config key IGNORED on reload (runtime.config_unknown_keys = \
                         \"warn\") — unknown to this broker version (ADR 0058 T4)"
                    );
                }
                if let Err(e) = (cs.precheck)(&new) {
                    return self.reject(trigger, &format!("config: {e}"));
                }
                let old = read_lock(&cs.live).clone();
                *write_lock(&cs.live) = new.clone();
                Some((old, new))
            }
        };
        // Roll the live config back to `old` (used before every post-swap rejection).
        let rollback = || {
            if let (Some(cs), Some((old, _))) = (&self.config_source, &committed) {
                *write_lock(&cs.live) = old.clone();
            }
        };

        // Build everything up front; only an all-clean build is allowed to publish.
        let policy = (self.build)();
        let tls = self.tls_build.as_ref().map(|b| b());
        let crl = self.swim_crl_build.as_ref().map(|b| b());
        let client_crl = self.client_crl_build.as_ref().map(|b| b());
        let peer_tls = self.peer_tls_build.as_ref().map(|b| b());
        let gossip_signer = self.gossip_signer_build.as_ref().map(|b| b());
        let admin_tls = self.admin_tls_build.as_ref().map(|b| b());
        let rules = self.rules_build.as_ref().map(|b| b());
        // A configured TLS or CRL build failed: reject the whole reload, swap nothing.
        if let Some(Err(e)) = &tls {
            rollback();
            return self.reject(trigger, &format!("tls: {e}"));
        }
        if let Some(Err(e)) = &crl {
            rollback();
            return self.reject(trigger, &format!("gossip crl: {e}"));
        }
        if let Some(Err(e)) = &client_crl {
            rollback();
            return self.reject(trigger, &format!("client crl: {e}"));
        }
        if let Some(Err(e)) = &peer_tls {
            rollback();
            return self.reject(trigger, &format!("peer tls: {e}"));
        }
        if let Some(Err(e)) = &gossip_signer {
            rollback();
            return self.reject(trigger, &format!("gossip signer: {e}"));
        }
        if let Some(Err(e)) = &admin_tls {
            rollback();
            return self.reject(trigger, &format!("admin tls: {e}"));
        }
        if let Some(Err(e)) = &rules {
            rollback();
            return self.reject(trigger, &format!("rules: {e}"));
        }
        match policy {
            // The ACL/authenticator build failed: reject, swap nothing.
            Err(e) => {
                rollback();
                self.reject(trigger, &e)
            }
            // Everything built cleanly: publish atomically. The connection/accept loop reads
            // whichever it reaches first on its next check; all are mutually consistent.
            Ok((authz, auth)) => {
                let _ = self.authz_tx.send(authz.clone());
                let _ = self.auth_tx.send(auth.clone());
                if let (Some(tx), Some(Ok(acceptor))) = (&self.tls_tx, tls) {
                    let _ = tx.send(acceptor);
                }
                if let (Some(slot), Some(Ok(list))) = (&self.swim_crl, crl) {
                    *slot
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(list);
                }
                if let (Some((acc_tx, conn_tx)), Some(Ok((acceptor, connector)))) =
                    (&self.peer_tls, peer_tls)
                {
                    let _ = acc_tx.send(acceptor);
                    let _ = conn_tx.send(connector);
                }
                // Gossip signing identity (issue #269): the SWIM driver reads the slot
                // per datagram, so the rotated leaf is embedded on the next send.
                if let (Some(slot), Some(Ok(signer))) = (&self.gossip_signer, gossip_signer) {
                    slot.swap(signer);
                }
                // The admin listener (ADR 0081 T18): the next admin handshake serves it.
                if let Some(Ok(commit)) = admin_tls {
                    commit();
                }
                // The rule engine (ADR 0083): the next publish runs the new rules.
                if let (Some(tx), Some(Ok(set))) = (&self.rules_tx, rules) {
                    if let Some(m) = &self.metrics {
                        m.set_rules_loaded(enabled_rules(&set), set.digest());
                    }
                    info!(
                        rules = set.len(),
                        enabled = enabled_rules(&set),
                        digest = %set.digest(),
                        "rules reloaded (ADR 0083)"
                    );
                    let _ = tx.send(set);
                }
                // Revocation reaches live state (ADR 0040 T2/T3/T4): the hub
                // re-evaluates every online session, subscription grant, and peer
                // link against exactly the policy just published.
                if let Some(hub) = &self.sweep_hub {
                    let peer_revoked = self
                        .swim_crl
                        .as_ref()
                        .and_then(|slot| {
                            slot.read()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone()
                        })
                        .unwrap_or_default();
                    let _ = hub.send(crate::hub::HubCommand::SweepIdentities(
                        crate::hub::SweepPolicy {
                            authorizer: authz,
                            authenticator: auth,
                            revoked: match client_crl {
                                Some(Ok(list)) => list,
                                _ => RevocationList::default(),
                            },
                            peer_revoked,
                            trigger: trigger.to_string(),
                            audit: self.audit.clone(),
                        },
                    ));
                }
                info!(
                    trigger,
                    "security policy reloaded: ACL + authenticator (+ TLS, gossip CRL) swapped"
                );
                self.audit
                    .record("security.reload", None, &format!("ok (trigger={trigger})"));
                if let Some(m) = &self.metrics {
                    m.security_reload("ok", trigger);
                }
                // ADR 0054 T3: stamp the applied config (checksum + generation) so
                // /statusz and config_info{checksum} reflect what is actually live.
                if let Some(stamp) = &self.config_stamp {
                    let bytes = self
                        .config_source
                        .as_ref()
                        .and_then(|cs| cs.path.as_deref())
                        .and_then(|p| std::fs::read(p).ok())
                        .unwrap_or_default();
                    stamp.record(&bytes);
                    if let Some(m) = &self.metrics {
                        let (sum, _) = stamp.read();
                        m.set_config_info(&sum);
                    }
                }
                // ADR 0046 T4: the config swap is committed — push the live-swappable settings
                // (quotas) to the hub and log every changed non-live section as requires-restart.
                let mut outcome = ReloadOutcome {
                    trigger: trigger.to_string(),
                    applied: true,
                    ..ReloadOutcome::default()
                };
                if let (Some(cs), Some((old, new))) = (&self.config_source, &committed) {
                    outcome.requires_restart = (cs.apply)(old, new);
                    outcome.changed_sections = changed_sections(old, new);
                }
                if let Some(last) = &self.last_reload {
                    last.record(trigger, true, None);
                }
                outcome
            }
        }
    }

    /// Record a rejected reload (audit + metric + log) and report it as not applied.
    fn reject(&self, trigger: &str, error: &str) -> ReloadOutcome {
        warn!(trigger, %error, "security reload REJECTED — keeping the running policy");
        self.audit.record(
            "security.reload",
            None,
            &format!("rejected (trigger={trigger}): {error}"),
        );
        if let Some(m) = &self.metrics {
            m.security_reload("rejected", trigger);
        }
        if let Some(last) = &self.last_reload {
            last.record(trigger, false, Some(error));
        }
        ReloadOutcome {
            trigger: trigger.to_string(),
            applied: false,
            error: Some(error.to_string()),
            ..ReloadOutcome::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqtt_auth::{AllowAll, DenyAll};
    use mqtt_observability::AuditLog;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn audit() -> Arc<dyn AuditSink> {
        Arc::new(AuditLog::new())
    }

    /// Known-answer parity pin (NIST SHA-256 vector for "abc"): the operator's
    /// `config_checksum` (`mqttd-operator/src/render.rs`) is a deliberate copy
    /// of this function, pinned to the same vector there — the chart's
    /// `checksum/config` annotation and the broker's config stamp must agree.
    #[test]
    fn checksum_is_sha256_lowercase_hex() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// ADR 0040 T4: a successful reload rebuilds and swaps the peer-bus
    /// acceptor/connector (rotated cluster material is served on the next peer
    /// handshake), and a failing peer-TLS build rejects the whole reload —
    /// validate-before-swap covers the peer bus too.
    #[test]
    fn a_reload_swaps_the_peer_bus_tls_material() {
        // Throwaway self-signed material to build real acceptors/connectors from.
        let dir = std::env::temp_dir().join(format!("mqttd-peer-reload-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).unwrap();
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();

        let build_tls = {
            let (cert_path, key_path) = (cert_path.clone(), key_path.clone());
            move || -> PeerTlsBuildResult {
                Ok((
                    mqtt_net::tls::server_acceptor(&cert_path, &key_path, Some(&cert_path))
                        .map_err(|e| e.to_string())?,
                    mqtt_net::tls::client_connector(&cert_path, &cert_path, &key_path)
                        .map_err(|e| e.to_string())?,
                ))
            }
        };
        let ok_policy = || -> BuildResult {
            Ok((
                Arc::new(AllowAll) as Arc<dyn Authorizer>,
                Arc::new(mqtt_auth::basic::BasicAuthenticator {
                    allow_anonymous: true,
                }) as Arc<dyn Authenticator>,
            ))
        };

        // Success: both watch values are replaced.
        let (initial_acc, initial_conn) = build_tls().unwrap();
        let (acc_tx, mut acc_rx) = watch::channel(initial_acc);
        let (conn_tx, mut conn_rx) = watch::channel(initial_conn);
        acc_rx.mark_unchanged();
        conn_rx.mark_unchanged();
        let (mut reloader, _handles) = Reloader::new(ok_policy().unwrap(), audit(), ok_policy);
        reloader.attach_peer_tls(acc_tx, conn_tx, Box::new(build_tls.clone()));
        assert!(reloader.reload("signal"));
        assert!(
            acc_rx.has_changed().unwrap(),
            "the peer acceptor must be rebuilt and swapped"
        );
        assert!(
            conn_rx.has_changed().unwrap(),
            "the peer connector must be rebuilt and swapped"
        );

        // Failure: a bad peer-TLS build rejects the WHOLE reload — nothing swaps.
        let (initial_acc, initial_conn) = build_tls().unwrap();
        let (acc_tx, mut acc_rx) = watch::channel(initial_acc);
        let (conn_tx, _conn_rx) = watch::channel(initial_conn);
        acc_rx.mark_unchanged();
        let (mut reloader, handles) = Reloader::new(ok_policy().unwrap(), audit(), ok_policy);
        reloader.attach_peer_tls(
            acc_tx,
            conn_tx,
            Box::new(|| Err("peer cert: unreadable".into())),
        );
        assert!(!reloader.reload("signal"), "the reload must be rejected");
        assert!(
            !acc_rx.has_changed().unwrap(),
            "a rejected reload must swap nothing"
        );
        drop(handles);
    }

    /// A stand-in gossip signer whose "certificate" is a recognizable byte string, so a
    /// sealed datagram can be checked for which cert it embeds (issue #269).
    struct TestSigner {
        cert: Vec<u8>,
    }
    impl mqtt_cluster::swim_auth::GossipSign for TestSigner {
        fn cert_der(&self) -> &[u8] {
            &self.cert
        }
        fn sign(&self, _payload: &[u8]) -> Vec<u8> {
            vec![0xAB]
        }
    }
    struct NoVerify;
    impl mqtt_cluster::swim_auth::GossipVerify for NoVerify {
        fn verify(
            &self,
            _cert_der: &[u8],
            _payload: &[u8],
            _sig: &[u8],
        ) -> Result<mqtt_cluster::swim_auth::VerifiedIdentity, mqtt_cluster::swim_auth::OpenReject>
        {
            Err(mqtt_cluster::swim_auth::OpenReject::Auth)
        }
    }

    fn embeds(datagram: &[u8], cert: &[u8]) -> bool {
        datagram.windows(cert.len()).any(|w| w == cert)
    }

    /// Issue #269: a successful reload rebuilds the gossip signer from the (re-read)
    /// peer-bus leaf + key and swaps it into the live slot — the next sealed datagram
    /// embeds the NEW certificate. A failing signer build rejects the WHOLE reload
    /// (validate-before-swap): the slot keeps the running signer and nothing else swaps.
    #[test]
    fn a_reload_swaps_the_gossip_signer_and_a_bad_build_rejects_everything() {
        use mqtt_cluster::swim_auth::{GossipSign, SwimAuth, KEY_LEN};

        let auth_ctx = SwimAuth::new(&[7; KEY_LEN]).with_signing(
            Arc::new(TestSigner {
                cert: b"cert-OLD".to_vec(),
            }),
            Arc::new(NoVerify),
        );
        let slot = auth_ctx.signer_slot().expect("signed posture has a slot");
        assert!(embeds(&auth_ctx.seal(b"x", true), b"cert-OLD"));

        // Success: the freshly-built signer is live on the very next datagram.
        let (mut reloader, _h) = Reloader::new(ok_auth_pair().unwrap(), audit(), ok_auth_pair);
        reloader.attach_gossip_signer(slot.clone(), || {
            Ok(Arc::new(TestSigner {
                cert: b"cert-NEW".to_vec(),
            }) as Arc<dyn GossipSign>)
        });
        assert!(reloader.reload("signal"));
        assert!(
            embeds(&auth_ctx.seal(b"x", true), b"cert-NEW"),
            "the rotated leaf must be embedded in the next sealed datagram"
        );

        // Failure: an unreadable leaf/key rejects the whole reload — the signer keeps
        // running and the ACL/authenticator are not swapped either (all-or-nothing).
        let (mut reloader, handles) =
            Reloader::new(ok_auth_pair().unwrap(), audit(), || -> BuildResult {
                Ok((
                    Arc::new(DenyAll) as Arc<dyn Authorizer>,
                    Arc::new(mqtt_auth::basic::BasicAuthenticator {
                        allow_anonymous: false,
                    }) as Arc<dyn Authenticator>,
                ))
            });
        reloader.attach_gossip_signer(slot, || Err("peer key: unreadable".into()));
        assert!(!reloader.reload("signal"), "a bad signer build must reject");
        assert!(
            embeds(&auth_ctx.seal(b"x", true), b"cert-NEW"),
            "a rejected reload must leave the running signer in force"
        );
        assert!(
            handles.authz.borrow().authorize_publish(
                &id(),
                &mqtt_core::ClientId("c".into()),
                &"t".to_string()
            ),
            "the authorizer must not swap when the signer build fails (all-or-nothing)"
        );
    }

    /// ADR 0040 T2: a successful reload hands the hub the freshly-published policy
    /// for the identity sweep; a rejected reload sweeps nothing; a bad client CRL
    /// rejects the whole reload (validate-before-swap).
    #[tokio::test]
    async fn a_reload_dispatches_the_identity_sweep_only_on_success() {
        let ok_build = || -> BuildResult {
            Ok((
                Arc::new(AllowAll) as Arc<dyn Authorizer>,
                Arc::new(mqtt_auth::basic::BasicAuthenticator {
                    allow_anonymous: true,
                }) as Arc<dyn Authenticator>,
            ))
        };
        let initial = ok_build().unwrap();
        let (mut reloader, _handles) = Reloader::new(initial, audit(), ok_build);
        let (hub_tx, mut hub_rx) = tokio::sync::mpsc::unbounded_channel();
        reloader.attach_identity_sweep(
            hub_tx,
            Some(Box::new(|| Ok(RevocationList::from_serials([vec![0x42]])))),
        );

        assert!(reloader.reload("signal"));
        match hub_rx.try_recv() {
            Ok(crate::hub::HubCommand::SweepIdentities(policy)) => {
                assert!(policy.revoked.contains(&[0x42]));
                assert_eq!(policy.trigger, "signal");
            }
            other => panic!("expected a SweepIdentities command, got {other:?}"),
        }

        // A rejected reload (bad ACL build) must not sweep.
        let (mut reloader, _handles) = Reloader::new(ok_build().unwrap(), audit(), || {
            Err("acl: parse error".into())
        });
        let (hub_tx, mut hub_rx) = tokio::sync::mpsc::unbounded_channel();
        reloader.attach_identity_sweep(hub_tx, None);
        assert!(!reloader.reload("signal"));
        assert!(
            hub_rx.try_recv().is_err(),
            "a rejected reload must not dispatch a sweep"
        );

        // A bad client CRL rejects the whole reload — and no sweep fires.
        let (mut reloader, handles) = Reloader::new(ok_build().unwrap(), audit(), ok_build);
        let (hub_tx, mut hub_rx) = tokio::sync::mpsc::unbounded_channel();
        reloader.attach_identity_sweep(
            hub_tx,
            Some(Box::new(|| Err("client crl: parse error".into()))),
        );
        assert!(!reloader.reload("signal"));
        assert!(hub_rx.try_recv().is_err());
        drop(handles);
    }

    /// A successful reload swaps the value the receivers observe.
    #[test]
    fn a_successful_reload_swaps_the_policy() {
        let initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>) = (
            Arc::new(AllowAll),
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }),
        );
        let (reloader, handles) = Reloader::new(initial, audit(), || {
            Ok((
                Arc::new(DenyAll) as Arc<dyn Authorizer>,
                Arc::new(mqtt_auth::basic::BasicAuthenticator {
                    allow_anonymous: false,
                }) as Arc<dyn Authenticator>,
            ))
        });

        // Before reload: the initial (AllowAll) authorizer permits.
        assert!(handles.authz.borrow().authorize_publish(
            &id(),
            &mqtt_core::ClientId("c".into()),
            &"t".to_string()
        ));

        assert!(reloader.reload("signal"), "the reload should apply");

        // After reload: the live receiver now sees DenyAll.
        assert!(!handles.authz.borrow().authorize_publish(
            &id(),
            &mqtt_core::ClientId("c".into()),
            &"t".to_string()
        ));
    }

    /// A failed build (a bad file) leaves the running policy unchanged — never fail open.
    #[test]
    fn a_failed_reload_keeps_the_running_policy() {
        let initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>) = (
            Arc::new(AllowAll),
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }),
        );
        let attempted = Arc::new(AtomicBool::new(false));
        let a2 = attempted.clone();
        let (reloader, handles) = Reloader::new(initial, audit(), move || {
            a2.store(true, Ordering::SeqCst);
            Err("acl file: parse error at line 3".to_string())
        });

        assert!(!reloader.reload("signal"), "a failed build must not apply");
        assert!(attempted.load(Ordering::SeqCst), "the build was attempted");
        // The running policy is still the permissive initial one — not swapped, not emptied.
        assert!(handles.authz.borrow().authorize_publish(
            &id(),
            &mqtt_core::ClientId("c".into()),
            &"t".to_string()
        ));
    }

    /// Each reload increments `security_reloads_total`, labelled by outcome.
    #[test]
    fn reload_increments_the_metric_by_outcome() {
        let metrics = Arc::new(Metrics::new("test"));
        let initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>) = (
            Arc::new(AllowAll),
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }),
        );
        let attempt = Arc::new(AtomicBool::new(false));
        let a2 = attempt.clone();
        let (reloader, _handles) =
            Reloader::with_metrics(initial, audit(), Some(metrics.clone()), move || {
                // First call succeeds; second fails — exercising both outcome labels.
                if a2.swap(true, Ordering::SeqCst) {
                    Err("bad file".to_string())
                } else {
                    Ok((
                        Arc::new(DenyAll) as Arc<dyn Authorizer>,
                        Arc::new(mqtt_auth::basic::BasicAuthenticator {
                            allow_anonymous: false,
                        }) as Arc<dyn Authenticator>,
                    ))
                }
            });

        assert!(reloader.reload("signal"));
        assert!(!reloader.reload("signal"));

        let text = metrics.render();
        assert!(
            text.contains("security_reloads_total{outcome=\"ok\",trigger=\"signal\"} 1"),
            "a successful reload counts under outcome=ok:\n{text}"
        );
        assert!(
            text.contains("security_reloads_total{outcome=\"rejected\",trigger=\"signal\"} 1"),
            "a rejected reload counts under outcome=rejected:\n{text}"
        );
    }

    /// ADR 0083: a reload swaps a freshly-loaded rule set into the channel the
    /// connections read, and moves `mqttd_rules_info`; a rules file that does not load
    /// rejects the WHOLE reload — the running rules, and the running policy, stay.
    #[test]
    fn a_reload_swaps_the_rules_and_a_bad_rules_file_keeps_the_running_ones() {
        let metrics = Arc::new(Metrics::new("test"));
        let initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>) = (
            Arc::new(AllowAll),
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }),
        );
        let (mut reloader, _handles) =
            Reloader::with_metrics(initial, audit(), Some(metrics.clone()), || {
                Ok((
                    Arc::new(AllowAll) as Arc<dyn Authorizer>,
                    Arc::new(mqtt_auth::basic::BasicAuthenticator {
                        allow_anonymous: true,
                    }) as Arc<dyn Authenticator>,
                ))
            });
        let text = Arc::new(RwLock::new(
            "[rules.a]\nsql = 'SELECT 1 AS n FROM \"t/#\"'\n".to_string(),
        ));
        let (tx, rx) = watch::channel(Arc::new(mqtt_rules::RuleSet::empty()));
        reloader.attach_rules(tx, {
            let text = text.clone();
            move || -> RulesBuildResult {
                mqtt_rules::RuleSet::parse(&read_lock(&text))
                    .map(|l| Arc::new(l.rules))
                    .map_err(|e| e.to_string())
            }
        });

        assert!(reloader.reload("signal"));
        assert_eq!(
            rx.borrow().len(),
            1,
            "the new rules are what connections now read"
        );
        let digest = rx.borrow().digest().to_string();
        assert!(
            metrics
                .render()
                .contains(&format!("mqttd_rules_info{{checksum=\"{digest}\"}} 1")),
            "{}",
            metrics.render()
        );

        *write_lock(&text) = "[rules.a]\nsql = 'SELECT nope( FROM \"t/#\"'\n".to_string();
        let outcome = reloader.reload_with_outcome("signal");
        assert!(!outcome.applied);
        assert!(
            outcome
                .error
                .as_deref()
                .unwrap_or_default()
                .starts_with("rules: "),
            "{outcome:?}"
        );
        assert_eq!(rx.borrow().digest(), digest, "the running rules are kept");
    }

    /// A reload swaps a freshly-built gossip CRL into the shared slot (ADR 0022 T7).
    #[test]
    fn a_reload_swaps_the_gossip_crl_into_the_live_slot() {
        let initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>) = (
            Arc::new(AllowAll),
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }),
        );
        let (mut reloader, _handles) = Reloader::new(initial, audit(), || {
            Ok((
                Arc::new(AllowAll) as Arc<dyn Authorizer>,
                Arc::new(mqtt_auth::basic::BasicAuthenticator {
                    allow_anonymous: true,
                }) as Arc<dyn Authenticator>,
            ))
        });
        let slot: SwimCrlSlot = Arc::new(RwLock::new(None));
        reloader.attach_swim_crl(slot.clone(), || Ok(RevocationList::default()));

        assert!(slot.read().unwrap().is_none(), "empty before the reload");
        assert!(reloader.reload("signal"));
        assert!(
            slot.read().unwrap().is_some(),
            "the reload must publish the freshly-built CRL"
        );
    }

    /// A CRL that fails to build rejects the whole reload — the live list is untouched
    /// and the ACL/authenticator are not swapped either (all-or-nothing).
    #[test]
    fn a_bad_gossip_crl_rejects_the_whole_reload() {
        let initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>) = (
            Arc::new(AllowAll),
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }),
        );
        let (mut reloader, handles) = Reloader::new(initial, audit(), || {
            Ok((
                Arc::new(DenyAll) as Arc<dyn Authorizer>,
                Arc::new(mqtt_auth::basic::BasicAuthenticator {
                    allow_anonymous: false,
                }) as Arc<dyn Authenticator>,
            ))
        });
        let slot: SwimCrlSlot = Arc::new(RwLock::new(None));
        reloader.attach_swim_crl(slot.clone(), || Err("crl: not signed by the CA".into()));

        assert!(
            !reloader.reload("signal"),
            "a bad CRL must reject the reload"
        );
        assert!(slot.read().unwrap().is_none(), "the slot is untouched");
        // The authorizer was not swapped either — all-or-nothing held.
        assert!(handles.authz.borrow().authorize_publish(
            &id(),
            &mqtt_core::ClientId("c".into()),
            &"t".to_string()
        ));
    }

    fn id() -> mqtt_auth::Identity {
        mqtt_auth::Identity {
            subject: "u".to_string(),
            groups: Vec::new(),
        }
    }

    /// ADR 0084: every reload attempt is recorded, applied or rejected, with how many
    /// rejected attempts in a row before it had the same trigger and error — so the
    /// watcher retrying a broken file every poll reads as one failure repeating, and two
    /// applied writes read as two changes.
    #[test]
    fn every_reload_attempt_is_recorded_with_its_repeats() {
        let failing = Arc::new(AtomicBool::new(false));
        let (mut reloader, _h) = Reloader::new(ok_auth_pair().unwrap(), audit(), ok_auth_pair);
        let (rules_tx, _rules_rx) = watch::channel(Arc::new(mqtt_rules::RuleSet::empty()));
        let f = failing.clone();
        reloader.attach_rules(rules_tx, move || {
            if f.load(Ordering::SeqCst) {
                Err("rule `salted`: bad".into())
            } else {
                Ok(Arc::new(mqtt_rules::RuleSet::empty()))
            }
        });
        let last = Arc::new(LastReload::default());
        reloader.attach_last_reload(last.clone());
        assert_eq!(last.get(), None, "nothing until the first attempt");

        assert!(reloader.reload("signal"));
        let r = last.get().unwrap();
        assert_eq!(
            (r.trigger.as_str(), r.applied, r.repeats),
            ("signal", true, 0)
        );
        assert_eq!((r.error.as_deref(), r.error_kind()), (None, None));

        failing.store(true, Ordering::SeqCst);
        for repeats in 0..3 {
            assert!(!reloader.reload("watch"));
            let r = last.get().unwrap();
            assert_eq!(
                (r.trigger.as_str(), r.applied, r.repeats),
                ("watch", false, repeats)
            );
            assert_eq!(r.error.as_deref(), Some("rules: rule `salted`: bad"));
            assert_eq!(r.error_kind(), Some("rules"));
        }
        // Another trigger, or the same one succeeding, is a new run.
        assert!(!reloader.reload("admin"));
        assert_eq!(last.get().unwrap().repeats, 0);
        failing.store(false, Ordering::SeqCst);
        assert!(reloader.reload("admin"));
        let r = last.get().unwrap();
        assert_eq!((r.applied, r.repeats, r.error), (true, 0, None));
        // Applied attempts in a row with one trigger are each a change of their own.
        for _ in 0..2 {
            assert!(reloader.reload("admin-rules"));
            let r = last.get().unwrap();
            assert_eq!(
                (r.trigger.as_str(), r.applied, r.repeats),
                ("admin-rules", true, 0)
            );
        }
    }

    /// ADR 0084: what a reload failure publishes is the part that failed, never its text:
    /// a config error quotes the offending line, which can hold a secret.
    #[test]
    fn a_reload_error_kind_is_the_failing_part_never_the_text() {
        for (error, kind) in [
            (
                "config: TOML parse error at line 9\n9 | key = \"s3cret",
                "config",
            ),
            ("rules: rule `x`: bad", "rules"),
            ("admin tls: no such file", "admin tls"),
            ("gossip crl: expired", "gossip crl"),
            (
                "cannot read MQTTD_ACL_FILE (/etc/mqttd/acl.toml): denied",
                "policy",
            ),
            ("no colon at all", "policy"),
            // Only the exact component name counts: a policy error whose text happens to
            // start like one is still the policy's.
            ("config file /etc/mqttd/users.toml: denied", "policy"),
        ] {
            assert_eq!(error_kind(error), kind, "{error}");
        }
    }

    #[allow(clippy::unnecessary_wraps)] // must match the `build: Fn() -> BuildResult` signature
    fn ok_auth_pair() -> BuildResult {
        Ok((
            Arc::new(AllowAll) as Arc<dyn Authorizer>,
            Arc::new(mqtt_auth::basic::BasicAuthenticator {
                allow_anonymous: true,
            }) as Arc<dyn Authenticator>,
        ))
    }

    /// ADR 0081 T6 (review): a reader of the committed config never sees a reload's staged
    /// candidate. The build is held mid-reload; the shared cell already holds the
    /// candidate, `committed_config` waits, and when the build then fails it returns the
    /// config that stayed committed.
    #[test]
    fn committed_config_never_returns_a_rejected_candidate() {
        use std::sync::mpsc;
        let dir = std::env::temp_dir().join(format!("mqttd-committed-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cfg.toml");
        let base = "[durable]\nallow_ephemeral = true\n";
        std::fs::write(&path, format!("[node]\nid = \"old\"\n{base}")).unwrap();
        let live = Arc::new(RwLock::new(mqtt_config::Config::load(Some(&path)).unwrap()));

        // The policy build: the first call (startup) passes; the next blocks until told
        // whether to fail.
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (verdict_tx, verdict_rx) = mpsc::channel::<bool>();
        let verdict_rx = std::sync::Mutex::new(verdict_rx);
        let started_tx = std::sync::Mutex::new(started_tx);
        let build = move || -> BuildResult {
            started_tx.lock().unwrap().send(()).unwrap();
            if verdict_rx.lock().unwrap().recv().unwrap() {
                ok_auth_pair()
            } else {
                Err("policy rejected".into())
            }
        };
        let (mut reloader, _h) = Reloader::new(ok_auth_pair().unwrap(), audit(), build);
        reloader.attach_config_source(ConfigSource {
            live: live.clone(),
            path: Some(path.clone()),
            precheck: Box::new(|_| Ok(())),
            apply: Box::new(|_, _| Vec::new()),
        });
        let reloader = Arc::new(reloader);

        std::fs::write(&path, format!("[node]\nid = \"candidate\"\n{base}")).unwrap();
        let r = reloader.clone();
        let reload = std::thread::spawn(move || r.reload_with_outcome("admin"));
        started_rx.recv().unwrap();
        assert_eq!(
            read_lock(&live).node.id,
            "candidate",
            "the candidate is staged"
        );

        let r = reloader.clone();
        let reader = std::thread::spawn(move || r.committed_config());
        // SETTLE(committed-config-reader-blocks): the claim is an absence — the reader has
        // NOT returned while the reload holds its lock — and an absence has no observable to
        // poll; 50ms is ample for an unblocked read of an in-memory cell to finish.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!reader.is_finished(), "the reader waits out the reload");

        verdict_tx.send(false).unwrap();
        assert!(!reload.join().unwrap().applied);
        let (committed, _) = reader.join().unwrap().unwrap();
        assert_eq!(
            committed.node.id, "old",
            "the rejected candidate was never visible"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// ADR 0081 T6: the outcome says what the reload did — the changed sections, which of
    /// them the apply hook flagged as needing a restart, or the rejection and its reason.
    #[test]
    fn a_reload_reports_its_outcome() {
        let dir = std::env::temp_dir().join(format!("mqttd-outcome-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cfg.toml");
        let base = "[durable]\nallow_ephemeral = true\n";
        std::fs::write(&path, format!("[node]\nid = \"n1\"\n{base}")).unwrap();
        let live = Arc::new(RwLock::new(mqtt_config::Config::default()));
        let (mut reloader, _h) = Reloader::new(ok_auth_pair().unwrap(), audit(), ok_auth_pair);
        reloader.attach_config_source(ConfigSource {
            live: live.clone(),
            path: Some(path.clone()),
            precheck: Box::new(|_| Ok(())),
            // Stands in for the binary's requires-restart classification.
            apply: Box::new(|old, new| {
                if old.node == new.node {
                    Vec::new()
                } else {
                    vec!["node".to_string()]
                }
            }),
        });
        // First load: from defaults to the file.
        let first = reloader.reload_with_outcome("admin");
        assert!(first.applied && first.error.is_none(), "{first:?}");
        assert_eq!(first.trigger, "admin");
        assert!(
            first.changed_sections.contains(&"node".to_string()),
            "{first:?}"
        );
        assert_eq!(first.requires_restart, vec!["node".to_string()]);

        // A live-swappable edit: changed, nothing to restart.
        std::fs::write(
            &path,
            format!("[node]\nid = \"n1\"\n[limits]\nmax_sessions = 10\n{base}"),
        )
        .unwrap();
        let live_edit = reloader.reload_with_outcome("admin");
        assert!(live_edit.applied);
        assert_eq!(live_edit.changed_sections, vec!["limits".to_string()]);
        assert!(live_edit.requires_restart.is_empty());

        // Nothing changed: applied, no sections.
        let same = reloader.reload_with_outcome("admin");
        assert!(same.applied && same.changed_sections.is_empty(), "{same:?}");

        // A broken file: not applied, the reason reported, the running config kept.
        std::fs::write(&path, "[node\n").unwrap();
        let broken = reloader.reload_with_outcome("admin");
        assert!(!broken.applied);
        assert!(
            broken.error.as_deref().unwrap().starts_with("config:"),
            "{broken:?}"
        );
        assert!(broken.changed_sections.is_empty());
        assert_eq!(read_lock(&live).limits.max_sessions, Some(10));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// ADR 0046 T4: a reload re-loads the whole config file and swaps it into the shared `live`
    /// cell (validate-before-swap), running the injected precheck + apply. An invalid config, a
    /// precheck failure, or a failed policy build all keep the running config unchanged.
    #[test]
    fn a_config_reload_swaps_live_and_keeps_it_on_any_failure() {
        let dir = std::env::temp_dir().join(format!("mqttd-cfgreload-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cfg.toml");
        // The ephemeral opt-in (#240) keeps these configs valid; the subjects here are
        // the swap/rollback mechanics, not the durability posture.
        std::fs::write(
            &path,
            "[node]\nid = \"first\"\n[durable]\nallow_ephemeral = true\n",
        )
        .unwrap();

        let live = Arc::new(RwLock::new(mqtt_config::Config::default()));
        let applied = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Valid reload: live swaps to the file's contents and apply runs.
        let (mut reloader, _h) = Reloader::new(ok_auth_pair().unwrap(), audit(), ok_auth_pair);
        let applied_c = applied.clone();
        reloader.attach_config_source(ConfigSource {
            live: live.clone(),
            path: Some(path.clone()),
            precheck: Box::new(|_| Ok(())),
            apply: Box::new(move |_, _| {
                applied_c.store(true, Ordering::SeqCst);
                Vec::new()
            }),
        });
        assert!(reloader.reload("signal"));
        assert_eq!(read_lock(&live).node.id, "first");
        assert!(
            applied.load(Ordering::SeqCst),
            "apply runs on a committed reload"
        );

        // Invalid config (unknown key): rejected, running config kept, apply not run.
        applied.store(false, Ordering::SeqCst);
        std::fs::write(
            &path,
            "[node]\nid = \"second\"\nbogus = 1\n[durable]\nallow_ephemeral = true\n",
        )
        .unwrap();
        assert!(!reloader.reload("signal"));
        assert_eq!(
            read_lock(&live).node.id,
            "first",
            "an invalid edit is kept out"
        );
        assert!(
            !applied.load(Ordering::SeqCst),
            "apply is skipped on rejection"
        );

        // Precheck failure: rejected, running config kept.
        std::fs::write(
            &path,
            "[node]\nid = \"third\"\n[durable]\nallow_ephemeral = true\n",
        )
        .unwrap();
        let (mut reloader, _h) = Reloader::new(ok_auth_pair().unwrap(), audit(), ok_auth_pair);
        reloader.attach_config_source(ConfigSource {
            live: live.clone(),
            path: Some(path.clone()),
            precheck: Box::new(|_| Err("precheck says no".into())),
            apply: Box::new(|_, _| Vec::new()),
        });
        assert!(!reloader.reload("signal"));
        assert_eq!(
            read_lock(&live).node.id,
            "first",
            "a precheck failure keeps it"
        );

        // Config valid but the POLICY build fails: the swapped-in config is rolled back.
        let (mut reloader, _h) =
            Reloader::new(ok_auth_pair().unwrap(), audit(), || Err("acl: bad".into()));
        reloader.attach_config_source(ConfigSource {
            live: live.clone(),
            path: Some(path.clone()),
            precheck: Box::new(|_| Ok(())),
            apply: Box::new(|_, _| Vec::new()),
        });
        assert!(!reloader.reload("signal"));
        assert_eq!(
            read_lock(&live).node.id,
            "first",
            "a failed policy build rolls the config back"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Issue #240, the third gate: a reload that would edit the running broker into
    /// unopted ephemeral durability (durable on, no data dir, no flag) is REJECTED —
    /// `Config::load` runs `validate()`, so the reload path refuses exactly what
    /// startup and `--check-config` refuse — and the running config is kept.
    #[test]
    fn a_reload_into_ephemeral_durability_is_rejected_and_the_running_config_kept() {
        let dir = std::env::temp_dir().join(format!("mqttd-ephreload-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cfg.toml");
        // Start from a valid posture: a real data dir.
        std::fs::write(
            &path,
            format!("[node]\nid = \"kept\"\ndata_dir = \"{}\"\n", dir.display()),
        )
        .unwrap();

        let live = Arc::new(RwLock::new(mqtt_config::Config::default()));
        let (mut reloader, _h) = Reloader::new(ok_auth_pair().unwrap(), audit(), ok_auth_pair);
        reloader.attach_config_source(ConfigSource {
            live: live.clone(),
            path: Some(path.clone()),
            precheck: Box::new(|_| Ok(())),
            apply: Box::new(|_, _| Vec::new()),
        });
        assert!(
            reloader.reload("signal"),
            "the durable-on-disk config loads"
        );
        assert_eq!(read_lock(&live).node.id, "kept");

        // The bad edit: drop the data dir with durable still on and no opt-in.
        std::fs::write(&path, "[node]\nid = \"ephemeral\"\n").unwrap();
        assert!(
            !reloader.reload("signal"),
            "a reload into unopted ephemeral durability must be rejected"
        );
        let kept = read_lock(&live).clone();
        assert_eq!(kept.node.id, "kept", "the running config is kept");
        assert!(
            kept.node.data_dir.is_some(),
            "the running data dir survives the rejected edit"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
