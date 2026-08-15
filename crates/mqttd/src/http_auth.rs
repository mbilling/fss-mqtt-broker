//! Remote HTTP authentication hook (ADR 0004 T16).
//!
//! One hook reaches every backend the broker will never implement natively — LDAP, `OAuth2`
//! introspection, a bespoke user table, a legacy directory nobody will describe. Writing
//! an integration per backend does not scale and never finishes; writing one endpoint does.
//!
//! It lives here rather than in `mqtt-auth` because that crate is deliberately **I/O-free**
//! — the same split ADR 0050 uses, where the crate holds a pure JWKS verifier and the
//! binary does the fetching. `mqtt_auth::Authenticator` became async (T15) precisely so
//! this could exist without blocking a runtime worker on every CONNECT.
//!
//! ## The contract
//!
//! The broker `POST`s JSON and reads the **HTTP status** as the verdict:
//!
//! ```text
//! POST /auth  { "client_id": "...", "username": "...", "password": "...", "method": "password" }
//!
//!   200        → allow. An optional JSON body {"groups":["a","b"]} enriches the identity.
//!   401 / 403  → deny.
//!   anything else, a timeout, or an unreachable host → DENY (fail closed).
//! ```
//!
//! Status-only, deliberately. EMQX also lets a `200` body say `{"result":"deny"}`; two
//! verdict channels means a hook can accidentally allow by returning `200` with a body the
//! broker did not parse the way its author assumed. One channel cannot be misread.
//!
//! ## Fail closed, and what that costs
//!
//! A hook that is unreachable has not authenticated anybody, so an outage denies every
//! *new* connection that depends on it. Established sessions are untouched — this runs on
//! CONNECT only — but it does mean the hook is a hard dependency of new connections. That
//! is the correct direction to fail and it is not free; run the hook where the broker runs,
//! and use the accepted-credential cache to ride out a blip.
//!
//! ## Caching
//!
//! Accepted credentials only, for `cache_secs` (default off). Rejections are **never**
//! cached: a fixed password must take effect at once, and caching denials would turn a
//! blip into a lasting outage. The cache key is a hash of the credential, never the
//! credential; the cache is bounded because it sits on an attacker-reachable path.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aws_lc_rs::digest;
use mqtt_auth::{AuthError, Authenticator, Credentials, Identity};
use mqtt_core::ClientId;
use tracing::{debug, warn};

/// Default per-request timeout. The broker applies none of its own around an
/// authenticator, so this is the only bound on how long a CONNECT waits.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
/// Default cache ceiling. Bounded because the cache is filled from a network-facing path.
const DEFAULT_CACHE_MAX: usize = 10_000;

/// How the hook was configured.
#[derive(Debug, Clone)]
pub struct HttpAuthConfig {
    /// Endpoint to POST credentials to.
    pub url: String,
    /// Per-request timeout.
    pub timeout: Duration,
    /// How long an ACCEPTED credential stays cached; zero disables caching.
    pub cache_ttl: Duration,
    /// Most accepted credentials to hold.
    pub cache_max: usize,
}

impl HttpAuthConfig {
    /// Build from the broker config, validating the URL scheme.
    ///
    /// # Errors
    /// If the URL is not `https` and `allow_http` is not set — credentials cross this
    /// link, so plaintext is a deliberate, loudly-logged choice rather than a default.
    pub fn from_config(cfg: &mqtt_config::HttpAuth) -> Result<Option<Self>, String> {
        let Some(url) = cfg.url.clone() else {
            return Ok(None);
        };
        if !url.starts_with("https://") {
            if !cfg.allow_http {
                return Err(format!(
                    "http_auth.url must be https (got {url}); set \
                     MQTTD_HTTP_AUTH_ALLOW_HTTP to permit plaintext — the client's \
                     password crosses this link"
                ));
            }
            warn!(%url, "INSECURE: the HTTP auth hook is plaintext — credentials cross it in the clear");
        }
        Ok(Some(Self {
            url,
            timeout: cfg
                .timeout_secs
                .map_or(DEFAULT_TIMEOUT, Duration::from_secs),
            cache_ttl: Duration::from_secs(cfg.cache_secs.unwrap_or(0)),
            cache_max: usize::try_from(cfg.cache_max.unwrap_or(DEFAULT_CACHE_MAX as u64))
                .unwrap_or(DEFAULT_CACHE_MAX),
        }))
    }
}

/// An accepted credential, remembered until it expires.
#[derive(Debug, Clone)]
struct CachedAllow {
    identity: Identity,
    until: Instant,
}

/// Authenticates by asking a remote endpoint.
#[derive(Debug)]
pub struct HttpAuthenticator {
    config: HttpAuthConfig,
    client: reqwest::Client,
    /// Accepted credentials, keyed by a **hash** of the credential — the broker holds no
    /// replayable secret, in memory or anywhere else (ADR 0004).
    cache: Mutex<HashMap<[u8; 32], CachedAllow>>,
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
}

impl HttpAuthenticator {
    /// Build the hook. The HTTP client is created once and reused, so connections are
    /// pooled rather than re-established per CONNECT.
    ///
    /// # Errors
    /// If the HTTP client cannot be constructed.
    pub fn new(
        config: HttpAuthConfig,
        metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    ) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| format!("could not build the HTTP auth client: {e}"))?;
        Ok(Self {
            config,
            client,
            cache: Mutex::new(HashMap::new()),
            metrics,
        })
    }

    /// The cache key: a SHA-256 over the credential kind, client id, username and secret.
    ///
    /// Hashed so a memory dump does not yield a password, and so two users with the same
    /// password do not collide. The client id is included because a hook may legitimately
    /// answer differently for the same user on a different client id.
    fn cache_key(client: &ClientId, method: &str, username: &str, secret: &[u8]) -> [u8; 32] {
        let mut ctx = digest::Context::new(&digest::SHA256);
        // Length-prefixed so ("ab","c") and ("a","bc") cannot hash alike.
        for part in [
            method.as_bytes(),
            client.0.as_bytes(),
            username.as_bytes(),
            secret,
        ] {
            ctx.update(&(part.len() as u64).to_be_bytes());
            ctx.update(part);
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(ctx.finish().as_ref());
        key
    }

    /// A cached acceptance, if one is still live. Expired entries are dropped on sight.
    fn cached(&self, key: &[u8; 32]) -> Option<Identity> {
        if self.config.cache_ttl.is_zero() {
            return None;
        }
        let mut cache = self.cache.lock().ok()?;
        match cache.get(key) {
            Some(hit) if hit.until > Instant::now() => Some(hit.identity.clone()),
            Some(_) => {
                cache.remove(key);
                None
            }
            None => None,
        }
    }

    /// Remember an acceptance. Bounded: at the ceiling, expired entries are swept and —
    /// if that frees nothing — the insert is skipped rather than growing the map.
    fn remember(&self, key: [u8; 32], identity: &Identity) {
        if self.config.cache_ttl.is_zero() {
            return;
        }
        let Ok(mut cache) = self.cache.lock() else {
            return;
        };
        if cache.len() >= self.config.cache_max {
            let now = Instant::now();
            cache.retain(|_, v| v.until > now);
            if cache.len() >= self.config.cache_max {
                debug!(
                    max = self.config.cache_max,
                    "HTTP auth cache is full of live entries; not caching this acceptance"
                );
                return;
            }
        }
        cache.insert(
            key,
            CachedAllow {
                identity: identity.clone(),
                until: Instant::now() + self.config.cache_ttl,
            },
        );
    }

    /// Ask the hook. Any outcome that is not an explicit allow or deny is a **deny**.
    async fn ask(
        &self,
        client: &ClientId,
        method: &str,
        username: &str,
        secret: &[u8],
    ) -> Result<Identity, AuthError> {
        let body = serde_json::json!({
            "client_id": client.0,
            "username": username,
            "password": String::from_utf8_lossy(secret),
            "method": method,
        });

        let started = Instant::now();
        let response = self.client.post(&self.config.url).json(&body).send().await;
        let elapsed = started.elapsed();
        if let Some(m) = &self.metrics {
            m.observe_http_auth_latency(elapsed.as_secs_f64());
        }

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                // Unreachable, TLS failure, or the timeout expiring. NEVER logged with the
                // credential; the client id is enough to correlate.
                warn!(
                    client = %client.0, error = %e, timeout_s = self.config.timeout.as_secs_f64(),
                    "HTTP auth hook did not answer — DENYING (fail closed)"
                );
                self.count("error");
                return Err(AuthError::Backend("auth hook unreachable".into()));
            }
        };

        let status = response.status();
        if status.is_success() {
            // An optional body enriches the identity; a body we cannot parse is not a
            // reason to reject an explicit 200, so groups simply come back empty.
            let groups = response
                .json::<HookBody>()
                .await
                .ok()
                .and_then(|b| b.groups)
                .unwrap_or_default();
            self.count("allow");
            return Ok(Identity {
                subject: username.to_string(),
                groups,
            });
        }

        if status.as_u16() == 401 || status.as_u16() == 403 {
            debug!(client = %client.0, %status, "HTTP auth hook denied");
            self.count("deny");
            return Err(AuthError::Rejected);
        }

        // 5xx, a redirect, 404 at a mistyped URL: the hook did not say yes, and an
        // ambiguous answer is not an acceptance.
        warn!(
            client = %client.0, %status,
            "HTTP auth hook answered with an unexpected status — DENYING (fail closed)"
        );
        self.count("error");
        Err(AuthError::Backend(format!("auth hook status {status}")))
    }

    fn count(&self, outcome: &str) {
        if let Some(m) = &self.metrics {
            m.http_auth_outcome(outcome);
        }
    }
}

/// The optional JSON body of a `200`.
#[derive(Debug, serde::Deserialize)]
struct HookBody {
    /// Group memberships to attach to the identity, for ACL rules that match on groups.
    groups: Option<Vec<String>>,
}

#[async_trait::async_trait]
impl Authenticator for HttpAuthenticator {
    async fn authenticate(
        &self,
        client: &ClientId,
        creds: &Credentials<'_>,
    ) -> Result<Identity, AuthError> {
        // Only username/password reaches the hook. A client certificate is verified by the
        // TLS layer and a token by its own verifier; abstaining lets the chain use them.
        let Credentials::Password { username, password } = creds else {
            return Err(AuthError::NotPermitted);
        };

        let key = Self::cache_key(client, "password", username, password);
        if let Some(identity) = self.cached(&key) {
            self.count("cache-hit");
            return Ok(identity);
        }

        let identity = self.ask(client, "password", username, password).await?;
        self.remember(key, &identity);
        Ok(identity)
    }

    fn password_subject_exists(&self, _subject: &str) -> bool {
        // The identity sweep (ADR 0040 T2) asks whether a subject still exists so a live
        // session can be evicted when its user is removed. Answering would mean asking the
        // hook, which is a network call per live session per reload — and a hook outage
        // would then evict every session it could not vouch for. `true` (no opinion) keeps
        // the sweep's other members authoritative; a hook-backed session is bounded by its
        // credential's own lifetime and by the ACL sweep, as tokens and certificates are.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(url: &str) -> HttpAuthConfig {
        HttpAuthConfig {
            url: url.to_string(),
            timeout: Duration::from_millis(250),
            cache_ttl: Duration::ZERO,
            cache_max: 16,
        }
    }

    fn client() -> ClientId {
        ClientId("c1".into())
    }

    fn creds<'a>(username: &'a str, password: &'a [u8]) -> Credentials<'a> {
        Credentials::Password { username, password }
    }

    #[test]
    fn an_http_url_is_refused_unless_explicitly_allowed() {
        let mut c = mqtt_config::HttpAuth {
            url: Some("http://hook.example/auth".into()),
            ..Default::default()
        };
        assert!(
            HttpAuthConfig::from_config(&c).is_err(),
            "plaintext must be opt-in — the password crosses this link"
        );
        c.allow_http = true;
        assert!(HttpAuthConfig::from_config(&c).is_ok());
    }

    #[test]
    fn no_url_means_no_hook() {
        let c = mqtt_config::HttpAuth::default();
        assert!(HttpAuthConfig::from_config(&c).expect("valid").is_none());
    }

    /// The key must separate fields, or `("ab","c")` and `("a","bc")` would collide and one
    /// user's acceptance could be served to another.
    #[test]
    fn the_cache_key_is_unambiguous_across_field_boundaries() {
        let a = HttpAuthenticator::cache_key(&client(), "password", "ab", b"c");
        let b = HttpAuthenticator::cache_key(&client(), "password", "a", b"bc");
        assert_ne!(a, b);

        // And it is sensitive to every field.
        let base = HttpAuthenticator::cache_key(&client(), "password", "u", b"p");
        assert_ne!(
            base,
            HttpAuthenticator::cache_key(&client(), "password", "u", b"q")
        );
        assert_ne!(
            base,
            HttpAuthenticator::cache_key(&client(), "password", "v", b"p")
        );
        assert_ne!(
            base,
            HttpAuthenticator::cache_key(&ClientId("c2".into()), "password", "u", b"p")
        );
    }

    /// A credential the hook never saw must not be served from cache, and a non-password
    /// credential must ABSTAIN so the chain can try a certificate or token verifier.
    #[tokio::test]
    async fn a_non_password_credential_abstains_without_calling_the_hook() {
        // Port 1 is reserved and nothing listens: any actual request fails the test by
        // taking the fail-closed path instead of abstaining.
        let auth = HttpAuthenticator::new(cfg("https://127.0.0.1:1/auth"), None).expect("build");
        assert!(matches!(
            auth.authenticate(&client(), &Credentials::Anonymous).await,
            Err(AuthError::NotPermitted)
        ));
    }

    /// An unreachable hook denies — it does not abstain (which would let a later chain
    /// member accept) and does not accept.
    #[tokio::test]
    async fn an_unreachable_hook_fails_closed() {
        let auth = HttpAuthenticator::new(cfg("https://127.0.0.1:1/auth"), None).expect("build");
        match auth.authenticate(&client(), &creds("u", b"p")).await {
            Err(AuthError::Backend(_)) => {}
            other => panic!("an unreachable hook must deny with Backend, got {other:?}"),
        }
    }

    /// The cache holds acceptances and expires them; it never holds a rejection.
    #[test]
    fn the_cache_expires_and_is_bounded() {
        let mut c = cfg("https://hook.example/auth");
        c.cache_ttl = Duration::from_millis(50);
        c.cache_max = 2;
        let auth = HttpAuthenticator::new(c, None).expect("build");
        let id = Identity {
            subject: "u".into(),
            groups: vec![],
        };

        let k1 = HttpAuthenticator::cache_key(&client(), "password", "u", b"p");
        auth.remember(k1, &id);
        assert!(auth.cached(&k1).is_some(), "a fresh acceptance is served");

        // SETTLE(http-auth-cache-ttl): the cache entry's `until` is a `std::time::Instant`, which
        // neither `crate::clock` (epoch seconds only) nor tokio's paused clock can move, so the
        // 50 ms TTL configured above has to expire in real time. 80 ms is 1.6x it. One-sided
        // failure mode: on a slow machine more time has passed, so the entry is more certainly
        // expired — too short gives a false FAILURE, never a false pass. Deterministic form
        // needs a monotonic clock seam on `HttpAuthenticator`; issue #260 records the ask.
        std::thread::sleep(Duration::from_millis(80));
        assert!(auth.cached(&k1).is_none(), "an expired acceptance is not");

        // Bounded: past the ceiling with everything live, further inserts are dropped
        // rather than growing a map fed from an attacker-reachable path.
        let mut long = cfg("https://hook.example/auth");
        long.cache_ttl = Duration::from_secs(60);
        long.cache_max = 2;
        let auth = HttpAuthenticator::new(long, None).expect("build");
        for i in 0..10u8 {
            auth.remember(
                HttpAuthenticator::cache_key(&client(), "password", "u", &[i]),
                &id,
            );
        }
        assert!(
            auth.cache.lock().expect("lock").len() <= 2,
            "the cache must not grow past its ceiling"
        );
    }

    /// Caching off (the default) means every CONNECT asks the hook — so a revoked
    /// credential stops working immediately.
    #[test]
    fn caching_is_off_by_default() {
        let auth = HttpAuthenticator::new(cfg("https://hook.example/auth"), None).expect("build");
        let id = Identity {
            subject: "u".into(),
            groups: vec![],
        };
        let k = HttpAuthenticator::cache_key(&client(), "password", "u", b"p");
        auth.remember(k, &id);
        assert!(
            auth.cached(&k).is_none(),
            "with cache_ttl zero nothing is remembered"
        );
    }
}
