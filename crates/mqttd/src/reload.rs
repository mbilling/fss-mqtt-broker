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

/// What a `build` closure returns: the freshly-read `(authorizer, authenticator)`, or an
/// error string (a missing/unparseable file) that **aborts** the swap.
pub type BuildResult = Result<(Arc<dyn Authorizer>, Arc<dyn Authenticator>), String>;

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
        // Build everything up front; only an all-clean build is allowed to publish.
        let policy = (self.build)();
        let tls = self.tls_build.as_ref().map(|b| b());
        let crl = self.swim_crl_build.as_ref().map(|b| b());
        let client_crl = self.client_crl_build.as_ref().map(|b| b());
        // A configured TLS or CRL build failed: reject the whole reload, swap nothing.
        if let Some(Err(e)) = &tls {
            return self.reject(trigger, &format!("tls: {e}"));
        }
        if let Some(Err(e)) = &crl {
            return self.reject(trigger, &format!("gossip crl: {e}"));
        }
        if let Some(Err(e)) = &client_crl {
            return self.reject(trigger, &format!("client crl: {e}"));
        }
        match policy {
            // The ACL/authenticator build failed: reject, swap nothing.
            Err(e) => self.reject(trigger, &e),
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
                // Revocation reaches live state (ADR 0040 T2): the hub re-evaluates
                // every online session against exactly the policy just published.
                if let Some(hub) = &self.sweep_hub {
                    let _ = hub.send(crate::hub::HubCommand::SweepIdentities(
                        crate::hub::SweepPolicy {
                            authorizer: authz,
                            authenticator: auth,
                            revoked: match client_crl {
                                Some(Ok(list)) => list,
                                _ => RevocationList::default(),
                            },
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
}
