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
pub type ConfigApply = Box<dyn Fn(&mqtt_config::Config, &mqtt_config::Config) + Send + Sync>;

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

/// Holds the swap channels + the file-rereading `build` closure for SIGHUP reload.
pub struct Reloader {
    authz_tx: watch::Sender<Arc<dyn Authorizer>>,
    auth_tx: watch::Sender<Arc<dyn Authenticator>>,
    audit: Arc<dyn AuditSink>,
    metrics: Option<Arc<Metrics>>,
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
    /// Set by [`attach_config_source`](Self::attach_config_source) (ADR 0046 T4): the whole
    /// config is re-loaded, validated, and swapped ahead of the policy rebuild.
    config_source: Option<ConfigSource>,
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
                config_source: None,
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
    #[allow(clippy::too_many_lines)]
    pub fn reload(&self, trigger: &str) -> bool {
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
                // ADR 0046 T4: the config swap is committed — push the live-swappable settings
                // (quotas) to the hub and log every changed non-live section as requires-restart.
                if let (Some(cs), Some((old, new))) = (&self.config_source, &committed) {
                    (cs.apply)(old, new);
                }
                true
            }
        }
    }

    /// Record a rejected reload (audit + metric + log) and report it as not applied.
    fn reject(&self, trigger: &str, error: &str) -> bool {
        warn!(trigger, %error, "security reload REJECTED — keeping the running policy");
        self.audit.record(
            "security.reload",
            None,
            &format!("rejected (trigger={trigger}): {error}"),
        );
        if let Some(m) = &self.metrics {
            m.security_reload("rejected", trigger);
        }
        false
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
        assert!(handles
            .authz
            .borrow()
            .authorize_publish(&id(), &"t".to_string()));

        assert!(reloader.reload("signal"), "the reload should apply");

        // After reload: the live receiver now sees DenyAll.
        assert!(!handles
            .authz
            .borrow()
            .authorize_publish(&id(), &"t".to_string()));
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
        assert!(handles
            .authz
            .borrow()
            .authorize_publish(&id(), &"t".to_string()));
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
        assert!(handles
            .authz
            .borrow()
            .authorize_publish(&id(), &"t".to_string()));
    }

    fn id() -> mqtt_auth::Identity {
        mqtt_auth::Identity {
            subject: "u".to_string(),
            groups: Vec::new(),
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

    /// ADR 0046 T4: a reload re-loads the whole config file and swaps it into the shared `live`
    /// cell (validate-before-swap), running the injected precheck + apply. An invalid config, a
    /// precheck failure, or a failed policy build all keep the running config unchanged.
    #[test]
    fn a_config_reload_swaps_live_and_keeps_it_on_any_failure() {
        let dir = std::env::temp_dir().join(format!("mqttd-cfgreload-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cfg.toml");
        std::fs::write(&path, "[node]\nid = \"first\"\n").unwrap();

        let live = Arc::new(RwLock::new(mqtt_config::Config::default()));
        let applied = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Valid reload: live swaps to the file's contents and apply runs.
        let (mut reloader, _h) = Reloader::new(ok_auth_pair().unwrap(), audit(), ok_auth_pair);
        let applied_c = applied.clone();
        reloader.attach_config_source(ConfigSource {
            live: live.clone(),
            path: Some(path.clone()),
            precheck: Box::new(|_| Ok(())),
            apply: Box::new(move |_, _| applied_c.store(true, Ordering::SeqCst)),
        });
        assert!(reloader.reload("signal"));
        assert_eq!(read_lock(&live).node.id, "first");
        assert!(
            applied.load(Ordering::SeqCst),
            "apply runs on a committed reload"
        );

        // Invalid config (unknown key): rejected, running config kept, apply not run.
        applied.store(false, Ordering::SeqCst);
        std::fs::write(&path, "[node]\nid = \"second\"\nbogus = 1\n").unwrap();
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
        std::fs::write(&path, "[node]\nid = \"third\"\n").unwrap();
        let (mut reloader, _h) = Reloader::new(ok_auth_pair().unwrap(), audit(), ok_auth_pair);
        reloader.attach_config_source(ConfigSource {
            live: live.clone(),
            path: Some(path.clone()),
            precheck: Box::new(|_| Err("precheck says no".into())),
            apply: Box::new(|_, _| {}),
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
            apply: Box::new(|_, _| {}),
        });
        assert!(!reloader.reload("signal"));
        assert_eq!(
            read_lock(&live).node.id,
            "first",
            "a failed policy build rolls the config back"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
