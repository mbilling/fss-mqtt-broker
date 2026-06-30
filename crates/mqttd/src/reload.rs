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

use std::sync::Arc;

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
        match (policy, tls) {
            // A configured TLS build failed: reject the whole reload, swap nothing.
            (_, Some(Err(e))) => self.reject(trigger, &format!("tls: {e}")),
            // The ACL/authenticator build failed: reject, swap nothing.
            (Err(e), _) => self.reject(trigger, &e),
            // Everything built cleanly: publish atomically. The connection/accept loop reads
            // whichever it reaches first on its next check; all are mutually consistent.
            (Ok((authz, auth)), tls_ok) => {
                let _ = self.authz_tx.send(authz);
                let _ = self.auth_tx.send(auth);
                if let (Some(tx), Some(Ok(acceptor))) = (&self.tls_tx, tls_ok) {
                    let _ = tx.send(acceptor);
                }
                info!(
                    trigger,
                    "security policy reloaded: ACL + authenticator (+ TLS) swapped"
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

    fn id() -> mqtt_auth::Identity {
        mqtt_auth::Identity {
            subject: "u".to_string(),
            groups: Vec::new(),
        }
    }
}
