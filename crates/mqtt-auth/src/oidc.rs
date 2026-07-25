//! OIDC-mode token authentication (ADR 0050) — the pure, synchronous half.
//!
//! This crate stays runtime-free: everything network lives in the host (mqttd's fetch
//! loop, which discovers the JWKS endpoint and pushes key sets in via
//! [`OidcAuthenticator::install_jwks`], validate-before-swap). This module owns what is
//! deterministic and security-critical:
//!
//! - **JWKS parsing with an asymmetric-only allow-list** — `RS256`/`ES256`; `HS*` is
//!   refused outright (a public JWKS must never feed an HMAC verify — key confusion),
//!   as is anything else `alg` could smuggle.
//! - **`kid`-selected validation** with required `iss` + `aud` and bounded clock skew.
//! - **Staleness fail-closed** — beyond `max_stale` without a successful refresh the
//!   authenticator rejects rather than trust keys it can no longer corroborate
//!   (ADR 0017's philosophy applied to trust material).
//! - **The unknown-`kid` refresh hint** — a bounded, non-blocking signal to the host's
//!   fetch loop. `authenticate` runs inline on the connection's async task, so it never
//!   blocks waiting for a refetch: a just-rotated client is rejected once, the refetch
//!   races its (universal) connect retry, and the retry lands on the new keys.

use crate::{AuthError, Authenticator, Credentials, Identity};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use mqtt_core::ClientId;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// OIDC-mode configuration. Unlike the static-key [`crate::token::TokenConfig`],
/// issuer and audience are **required**: an OIDC deployment that skips them is
/// accepting tokens minted for someone else.
#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// The issuer URL; must equal the token's `iss` exactly.
    pub issuer: String,
    /// Required audience (`aud`).
    pub audience: String,
    /// Claim name to read group memberships from (default `groups`).
    pub groups_claim: String,
    /// How stale the cached JWKS may grow (no successful refresh) before token
    /// authentication fails closed.
    pub max_stale: Duration,
    /// Allowed clock skew for `exp`/`nbf`, in seconds.
    pub leeway_secs: u64,
}

impl OidcConfig {
    /// A config with the decision defaults (ADR 0050): `groups` claim, 24 h staleness
    /// window, 60 s leeway.
    #[must_use]
    pub fn new(issuer: String, audience: String) -> Self {
        Self {
            issuer,
            audience,
            groups_claim: "groups".to_string(),
            max_stale: Duration::from_secs(24 * 3600),
            leeway_secs: 60,
        }
    }
}

/// One verification key from the JWKS.
struct NamedKey {
    kid: Option<String>,
    alg: Algorithm,
    key: DecodingKey,
}

/// The currently-installed key set and when it was (successfully) fetched.
struct LoadedKeys {
    keys: Vec<NamedKey>,
    fetched: Instant,
}

/// Claims we read out of a verified token (same shape as the static-key verifier).
#[derive(serde::Deserialize)]
struct Claims {
    #[serde(default)]
    sub: String,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

/// Verifies [`Credentials::Token`] against a JWKS the host keeps fresh. Other
/// credential kinds return [`AuthError::NotPermitted`] so it chains.
pub struct OidcAuthenticator {
    cfg: OidcConfig,
    /// `None` until the first successful [`install_jwks`](Self::install_jwks) — the
    /// fail-closed startup state.
    keys: RwLock<Option<LoadedKeys>>,
    /// Bounded(1) hint channel to the host's fetch loop: `try_send` makes an unknown
    /// `kid` (or observed staleness) request one refetch without ever blocking or
    /// stampeding — a full channel IS the debounce.
    refresh_hint: SyncSender<()>,
}

impl std::fmt::Debug for OidcAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcAuthenticator")
            .field("issuer", &self.cfg.issuer)
            .field("audience", &self.cfg.audience)
            .finish_non_exhaustive()
    }
}

impl OidcAuthenticator {
    /// Build the authenticator plus the receiver half of the refresh-hint channel,
    /// which the host's fetch loop selects on alongside its TTL timer.
    #[must_use]
    pub fn new(cfg: OidcConfig) -> (std::sync::Arc<Self>, Receiver<()>) {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        (
            std::sync::Arc::new(Self {
                cfg,
                keys: RwLock::new(None),
                refresh_hint: tx,
            }),
            rx,
        )
    }

    /// Parse a JWKS document and — only if it yields at least one usable key —
    /// atomically replace the installed set (validate-before-swap: a garbled fetch
    /// leaves the last-known-good keys serving). Returns the number of keys installed.
    ///
    /// # Errors
    /// [`AuthError::Backend`] if the document does not parse or contains no key on
    /// the `RS256`/`ES256` allow-list.
    pub fn install_jwks(&self, jwks: &[u8]) -> Result<usize, AuthError> {
        let keys = parse_jwks(jwks)?;
        let n = keys.len();
        let mut slot = self
            .keys
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = Some(LoadedKeys {
            keys,
            fetched: Instant::now(),
        });
        Ok(n)
    }

    /// Whether the installed key set has outlived the staleness window (or was never
    /// installed). Exposed so the host can log the fail-closed transition loudly.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        let slot = self
            .keys
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match slot.as_ref() {
            None => true,
            Some(l) => l.fetched.elapsed() > self.cfg.max_stale,
        }
    }

    /// Nudge the host's fetch loop; never blocks (a full channel means a refetch is
    /// already owed — exactly the debounce we want).
    fn hint_refresh(&self) {
        let _ = self.refresh_hint.try_send(());
    }

    /// Rewind the installed set's fetch stamp (staleness tests).
    #[cfg(test)]
    fn age_keys_for_test(&self, by: Duration) {
        let mut slot = self
            .keys
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(l) = slot.as_mut() {
            l.fetched = Instant::now()
                .checked_sub(by)
                .expect("test age within Instant range");
        }
    }
}

/// Parse and allow-list a JWKS document into verification keys.
fn parse_jwks(bytes: &[u8]) -> Result<Vec<NamedKey>, AuthError> {
    let set: JwkSet = serde_json::from_slice(bytes)
        .map_err(|e| AuthError::Backend(format!("JWKS does not parse: {e}")))?;
    let mut out = Vec::new();
    for jwk in &set.keys {
        // The asymmetric-only allow-list: RS256/ES256. Keys advertising anything
        // else — including symmetric algorithms a hostile JWKS could inject — are
        // skipped, not errors: an IdP may legitimately publish keys for other uses.
        use jsonwebtoken::jwk::KeyAlgorithm;
        let alg = match jwk.common.key_algorithm {
            Some(KeyAlgorithm::RS256) => Algorithm::RS256,
            Some(KeyAlgorithm::ES256) => Algorithm::ES256,
            _ => continue,
        };
        let Ok(key) = DecodingKey::from_jwk(jwk) else {
            continue; // malformed key material: skip, never trust
        };
        out.push(NamedKey {
            kid: jwk.common.key_id.clone(),
            alg,
            key,
        });
    }
    if out.is_empty() {
        return Err(AuthError::Backend(
            "JWKS contains no RS256/ES256 verification key".to_string(),
        ));
    }
    Ok(out)
}

impl Authenticator for OidcAuthenticator {
    fn authenticate(
        &self,
        _client: &ClientId,
        creds: &Credentials<'_>,
    ) -> Result<Identity, AuthError> {
        let Credentials::Token(jwt) = creds else {
            return Err(AuthError::NotPermitted);
        };
        // Header first: the algorithm allow-list is enforced BEFORE any key is
        // consulted, so `alg` can never steer key selection (`none` and `HS*`
        // included — the classic confusion attacks die here).
        let header = jsonwebtoken::decode_header(jwt).map_err(|_| AuthError::Rejected)?;
        if !matches!(header.alg, Algorithm::RS256 | Algorithm::ES256) {
            return Err(AuthError::Rejected);
        }

        let slot = self
            .keys
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(loaded) = slot.as_ref() else {
            // Startup fail-closed: no keys have ever been installed.
            self.hint_refresh();
            return Err(AuthError::Backend("OIDC keys not loaded".to_string()));
        };
        if loaded.fetched.elapsed() > self.cfg.max_stale {
            // The staleness floor (ADR 0050 §3): last-known-good has expired and the
            // fetch loop has not refreshed — stop trusting it.
            self.hint_refresh();
            return Err(AuthError::Backend(
                "OIDC key set stale beyond the trust window".to_string(),
            ));
        }

        // kid-selected candidates: same algorithm, and matching kid when both sides
        // carry one (a kid-less token may try every key of its algorithm — the set is
        // small and each try is one signature check).
        let candidates: Vec<&NamedKey> = loaded
            .keys
            .iter()
            .filter(|k| k.alg == header.alg)
            .filter(|k| match (&header.kid, &k.kid) {
                (Some(t), Some(have)) => t == have,
                _ => true,
            })
            .collect();
        if candidates.is_empty() {
            // Unknown kid — the just-rotated-IdP signature. Ask the host to refetch
            // (non-blocking, debounced) and reject this attempt; the client's connect
            // retry races the refetch and lands on the new key set.
            self.hint_refresh();
            return Err(AuthError::Rejected);
        }

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.cfg.issuer]);
        validation.set_audience(&[&self.cfg.audience]);
        validation.leeway = self.cfg.leeway_secs;

        for cand in candidates {
            if let Ok(data) = jsonwebtoken::decode::<Claims>(jwt, &cand.key, &validation) {
                let claims = data.claims;
                if claims.sub.is_empty() {
                    return Err(AuthError::Rejected);
                }
                let groups = claims
                    .extra
                    .get(&self.cfg.groups_claim)
                    .and_then(serde_json::Value::as_array)
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                return Ok(Identity {
                    subject: claims.sub,
                    groups,
                });
            }
        }
        Err(AuthError::Rejected)
    }

    fn handles_token(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header};
    use serde_json::json;

    const PRIVATE_PEM: &[u8] = include_bytes!("testdata/rs256_private.pem");
    const JWKS: &[u8] = include_bytes!("testdata/oidc_jwks.json");
    const ISSUER: &str = "https://idp.test/realms/bench";
    const AUD: &str = "mqttd";

    fn authenticator() -> (std::sync::Arc<OidcAuthenticator>, Receiver<()>) {
        OidcAuthenticator::new(OidcConfig::new(ISSUER.to_string(), AUD.to_string()))
    }

    fn mint(kid: &str, claims: &serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        let key = EncodingKey::from_rsa_pem(PRIVATE_PEM).unwrap();
        jsonwebtoken::encode(&header, claims, &key).unwrap()
    }

    fn exp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 600
    }

    fn good_claims() -> serde_json::Value {
        json!({"sub": "device-7", "iss": ISSUER, "aud": AUD, "exp": exp(),
               "groups": ["sensors"]})
    }

    fn auth(a: &OidcAuthenticator, token: &str) -> Result<Identity, AuthError> {
        a.authenticate(&ClientId("c1".into()), &Credentials::Token(token))
    }

    #[test]
    fn a_valid_token_authenticates_with_subject_and_groups() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        let id = auth(&a, &mint("t1", &good_claims())).unwrap();
        assert_eq!(id.subject, "device-7");
        assert_eq!(id.groups, vec!["sensors".to_string()]);
    }

    #[test]
    fn no_keys_installed_fails_closed_and_hints_refresh() {
        let (a, rx) = authenticator();
        let err = auth(&a, &mint("t1", &good_claims())).unwrap_err();
        assert!(matches!(err, AuthError::Backend(_)));
        assert!(rx.try_recv().is_ok(), "startup miss must hint a refetch");
    }

    #[test]
    fn wrong_issuer_audience_and_expiry_are_rejected() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        for claims in [
            json!({"sub":"d","iss":"https://evil.test","aud":AUD,"exp":exp()}),
            json!({"sub":"d","iss":ISSUER,"aud":"other","exp":exp()}),
            json!({"sub":"d","iss":ISSUER,"aud":AUD,"exp": 1_000}),
        ] {
            assert!(matches!(
                auth(&a, &mint("t1", &claims)).unwrap_err(),
                AuthError::Rejected
            ));
        }
    }

    #[test]
    fn an_unknown_kid_rejects_and_hints_exactly_one_refresh() {
        let (a, rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        // Two misses in a row: both reject; the bounded(1) channel holds ONE hint —
        // the debounce is the channel capacity, not a timer.
        for _ in 0..2 {
            assert!(matches!(
                auth(&a, &mint("rotated-away", &good_claims())).unwrap_err(),
                AuthError::Rejected
            ));
        }
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err(), "hint channel must debounce");
    }

    #[test]
    fn hs256_is_refused_even_if_it_matches_a_key() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        // An HS256 token signed with... anything: refused on the allow-list before
        // key selection (key-confusion guard).
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &good_claims(),
            &EncodingKey::from_secret(b"whatever"),
        )
        .unwrap();
        assert!(matches!(auth(&a, &token).unwrap_err(), AuthError::Rejected));
    }

    #[test]
    fn staleness_beyond_the_window_fails_closed_then_reinstall_recovers() {
        let (a, rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        a.age_keys_for_test(Duration::from_secs(25 * 3600));
        assert!(a.is_stale());
        let err = auth(&a, &mint("t1", &good_claims())).unwrap_err();
        assert!(matches!(err, AuthError::Backend(_)));
        assert!(rx.try_recv().is_ok(), "staleness must hint a refetch");
        // A fresh install (the fetch loop recovering) restores service.
        a.install_jwks(JWKS).unwrap();
        assert!(auth(&a, &mint("t1", &good_claims())).is_ok());
    }

    #[test]
    fn a_garbled_jwks_never_replaces_the_last_known_good_set() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        assert!(a.install_jwks(b"{ not json").is_err());
        assert!(a.install_jwks(br#"{"keys":[]}"#).is_err());
        // The original keys still serve (validate-before-swap).
        assert!(auth(&a, &mint("t1", &good_claims())).is_ok());
    }

    #[test]
    fn non_token_credentials_chain_through() {
        let (a, _rx) = authenticator();
        assert!(matches!(
            auth_creds(&a, &Credentials::Anonymous).unwrap_err(),
            AuthError::NotPermitted
        ));
    }

    fn auth_creds(a: &OidcAuthenticator, c: &Credentials<'_>) -> Result<Identity, AuthError> {
        a.authenticate(&ClientId("c1".into()), c)
    }
}
