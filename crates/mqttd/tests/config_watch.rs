//! Filesystem-watch auto-reload, end to end (ADR 0033 — T5).
//!
//! Drives the real [`reload::Reloader`] (the same one `SIGHUP` uses) through the
//! [`ConfigWatcher`] over a real on-disk ACL file, proving the two properties that matter:
//!
//! * **Auto-apply without a signal:** editing the file and polling once tightens the *live*
//!   authorizer — no `SIGHUP`.
//! * **Validate-before-swap + retry-until-parse:** a malformed (partial) write is rejected and
//!   the running policy is kept, then a clean write applies — through the inherited ADR 0032
//!   fail-safe.

use std::path::PathBuf;
use std::sync::Arc;

use mqtt_auth::acl::AclPolicy;
use mqtt_auth::basic::BasicAuthenticator;
use mqtt_auth::{Authenticator, Authorizer, Identity};
use mqttd::config_watch::ConfigWatcher;
use mqttd::reload;

/// Anyone may publish + subscribe across `room/#`.
const PERMISSIVE: &str = r#"
[[rules]]
actions = ["publish", "subscribe"]
topics = ["room/#"]
"#;

/// Subscribe still allowed, publishing to `room/#` denied.
const TIGHTENED: &str = r#"
[[rules]]
actions = ["subscribe"]
topics = ["room/#"]
"#;

fn temp_acl(initial: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("mqttd-cfgwatch-{}-{n}.toml", std::process::id()));
    std::fs::write(&path, initial).unwrap();
    path
}

/// Build a `Reloader` whose closure re-reads `acl_path` (exactly as the binary wires SIGHUP),
/// plus the live `authz` receiver to observe swaps. Returns `(Arc<Reloader>, authz handle)`.
fn reloadable_acl(
    acl_path: PathBuf,
) -> (
    Arc<reload::Reloader>,
    tokio::sync::watch::Receiver<Arc<dyn Authorizer>>,
) {
    let build = move || -> reload::BuildResult {
        let toml = std::fs::read_to_string(&acl_path).map_err(|e| format!("read acl: {e}"))?;
        let acl = AclPolicy::from_toml_str(&toml).map_err(|e| format!("parse acl: {e}"))?;
        Ok((
            Arc::new(acl) as Arc<dyn Authorizer>,
            Arc::new(BasicAuthenticator {
                allow_anonymous: false,
            }) as Arc<dyn Authenticator>,
        ))
    };
    let initial = build().expect("the initial ACL builds");
    let audit = Arc::new(mqtt_observability::AuditLog::new());
    let (reloader, handles) = reload::Reloader::new(initial, audit, build);
    (Arc::new(reloader), handles.authz)
}

fn alice() -> Identity {
    Identity {
        subject: "alice".into(),
        groups: vec![],
    }
}

fn may_publish(authz: &tokio::sync::watch::Receiver<Arc<dyn Authorizer>>) -> bool {
    authz
        .borrow()
        .authorize_publish(&alice(), &"room/x".to_string())
}

#[test]
fn a_file_edit_auto_applies_without_a_signal() {
    let path = temp_acl(PERMISSIVE);
    let (reloader, authz) = reloadable_acl(path.clone());
    let mut watcher = ConfigWatcher::new(vec![path.clone()]);

    // Initially permissive, and an unchanged file does not reload.
    assert!(may_publish(&authz), "permissive policy allows publish");
    assert!(
        !watcher.tick(|| reloader.reload("watch")),
        "no change → no reload"
    );
    assert!(may_publish(&authz));

    // Tighten the file on disk — NO SIGHUP — and poll once.
    std::fs::write(&path, TIGHTENED).unwrap();
    assert!(
        watcher.tick(|| reloader.reload("watch")),
        "the edit is detected and applied"
    );

    // The live authorizer now denies publish: the edit reached live state with no signal.
    assert!(
        !may_publish(&authz),
        "the tightened ACL auto-applied to the live policy"
    );

    let _ = std::fs::remove_file(&path);
}

#[test]
fn a_partial_write_is_rejected_then_a_clean_write_applies() {
    let path = temp_acl(PERMISSIVE);
    let (reloader, authz) = reloadable_acl(path.clone());
    let mut watcher = ConfigWatcher::new(vec![path.clone()]);

    // A malformed (half-written) file: detected, but the reload is rejected and the running
    // permissive policy is kept (never fail open).
    std::fs::write(&path, "[[rules]]\nactions = [\"publ").unwrap();
    assert!(watcher.tick(|| reloader.reload("watch")), "change detected");
    assert!(
        may_publish(&authz),
        "a malformed write is rejected; the running policy is kept"
    );
    // Still unparseable on the next poll → retried, still rejected, still permissive.
    assert!(
        watcher.tick(|| reloader.reload("watch")),
        "the rejected reload is retried, not skipped"
    );
    assert!(may_publish(&authz));

    // The whole, valid file lands → it applies exactly once.
    std::fs::write(&path, TIGHTENED).unwrap();
    assert!(
        watcher.tick(|| reloader.reload("watch")),
        "clean write applies"
    );
    assert!(!may_publish(&authz), "the tightened ACL is now live");
    assert!(
        !watcher.tick(|| reloader.reload("watch")),
        "settled file does not reload again"
    );

    let _ = std::fs::remove_file(&path);
}
