//! OIDC-mode token authentication (ADR 0050) — the pure, synchronous half.
//!
//! This crate stays runtime-free: everything network lives in the host (mqttd's fetch
//! loop, which discovers the JWKS endpoint and pushes key sets in via
//! [`OidcAuthenticator::install_jwks`], validate-before-swap). This module owns what is
//! deterministic and security-critical:
//!
//! - **JWKS parsing with an asymmetric-only allow-list** — `RS256`/`ES256`; `HS*` is
//!   refused outright (a public JWKS must never feed an HMAC verify — key confusion),
//!   as is anything else `alg` could smuggle. A key without `alg` is used for the one
//!   allowed algorithm its type implies, and a key published for encryption never is.
//! - **`kid`-selected validation** with required `iss` + `aud`, `exp` and `nbf` under
//!   bounded clock skew, and, when configured, RFC 9068 access tokens only (`at+jwt`).
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
    /// Accept only JWT access tokens as RFC 9068 types them (header `typ` `at+jwt`), so
    /// an ID token cannot stand in for one (RFC 8725 §3.11). Off by default: most
    /// providers do not type their access tokens that way.
    pub require_at_jwt: bool,
}

impl OidcConfig {
    /// A config with the decision defaults (ADR 0050): `groups` claim, 24 h staleness
    /// window, 60 s leeway, any token type.
    #[must_use]
    pub fn new(issuer: String, audience: String) -> Self {
        Self {
            issuer,
            audience,
            groups_claim: "groups".to_string(),
            max_stale: Duration::from_hours(24),
            leeway_secs: 60,
            require_at_jwt: false,
        }
    }
}

/// RFC 9068 §4: an access token's `typ` is `at+jwt` or `application/at+jwt`. Media types
/// compare without regard to case.
fn is_at_jwt(typ: &str) -> bool {
    typ.eq_ignore_ascii_case("at+jwt") || typ.eq_ignore_ascii_case("application/at+jwt")
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
        use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, KeyAlgorithm, PublicKeyUse};
        // A key published for encryption never verifies a signature (RFC 7517 §4.2).
        if jwk.common.public_key_use == Some(PublicKeyUse::Encryption) {
            continue;
        }
        // The asymmetric-only allow-list: RS256/ES256. Keys advertising anything
        // else — including symmetric algorithms a hostile JWKS could inject — are
        // skipped, not errors: an IdP may legitimately publish keys for other uses.
        // `alg` is optional in a JWK (RFC 7517 §4.4); a key without it is used for the
        // one allowed algorithm its type implies: RS256 for RSA, ES256 for P-256.
        let alg = match (&jwk.common.key_algorithm, &jwk.algorithm) {
            (Some(KeyAlgorithm::RS256), _) | (None, AlgorithmParameters::RSA(_)) => {
                Algorithm::RS256
            }
            (Some(KeyAlgorithm::ES256), _) => Algorithm::ES256,
            (None, AlgorithmParameters::EllipticCurve(ec)) if ec.curve == EllipticCurve::P256 => {
                Algorithm::ES256
            }
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

#[async_trait::async_trait]
impl Authenticator for OidcAuthenticator {
    async fn authenticate(
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
        // Explicit typing, when configured (RFC 9068 §4, RFC 8725 §3.11): only a token
        // typed as an access token passes, so an ID token for this audience does not.
        if self.cfg.require_at_jwt && !header.typ.as_deref().is_some_and(is_at_jwt) {
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
        // jsonwebtoken compares `iss` and `aud` only when the token carries them, so they are
        // also required claims: a token without either is refused (ADR 0050).
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        // A token is not accepted before its `nbf` (RFC 7519 §4.1.5); jsonwebtoken skips
        // that check unless asked.
        validation.validate_nbf = true;
        validation.leeway = self.cfg.leeway_secs;

        for cand in candidates {
            if let Ok(data) = jsonwebtoken::decode::<Claims>(jwt, &cand.key, &validation) {
                let claims = data.claims;
                // `iss` is one string (RFC 7519 §4.1.1). jsonwebtoken also passes an array
                // that contains the expected issuer; such a token is malformed.
                if claims.sub.is_empty()
                    || claims.extra.get("iss").is_some_and(|iss| !iss.is_string())
                {
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

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn exp() -> u64 {
        now() + 600
    }

    fn good_claims() -> serde_json::Value {
        json!({"sub": "device-7", "iss": ISSUER, "aud": AUD, "exp": exp(),
               "groups": ["sensors"]})
    }

    async fn auth(a: &OidcAuthenticator, token: &str) -> Result<Identity, AuthError> {
        a.authenticate(&ClientId("c1".into()), &Credentials::Token(token))
            .await
    }

    #[tokio::test]
    async fn a_valid_token_authenticates_with_subject_and_groups() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        let id = auth(&a, &mint("t1", &good_claims())).await.unwrap();
        assert_eq!(id.subject, "device-7");
        assert_eq!(id.groups, vec!["sensors".to_string()]);
    }

    #[tokio::test]
    async fn no_keys_installed_fails_closed_and_hints_refresh() {
        let (a, rx) = authenticator();
        let err = auth(&a, &mint("t1", &good_claims())).await.unwrap_err();
        assert!(matches!(err, AuthError::Backend(_)));
        assert!(rx.try_recv().is_ok(), "startup miss must hint a refetch");
    }

    #[tokio::test]
    async fn wrong_issuer_audience_and_expiry_are_rejected() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        for claims in [
            json!({"sub":"d","iss":"https://evil.test","aud":AUD,"exp":exp()}),
            json!({"sub":"d","iss":ISSUER,"aud":"other","exp":exp()}),
            json!({"sub":"d","iss":ISSUER,"aud":AUD,"exp": 1_000}),
        ] {
            assert!(matches!(
                auth(&a, &mint("t1", &claims)).await.unwrap_err(),
                AuthError::Rejected
            ));
        }
    }

    /// An access token shaped like a real identity provider's (Keycloak's): `aud` is an
    /// array that names this broker beside another client, and the provider's own claims
    /// ride along. The array form authenticates; an array that does not name the broker is
    /// refused.
    #[tokio::test]
    async fn an_idp_shaped_token_with_an_audience_array_authenticates() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        let issued = exp() - 600;
        let token = mint(
            "t1",
            &json!({
                "exp": exp(), "iat": issued, "auth_time": issued,
                "jti": "0d6f3a51-4b7e-4c55-9a51-6c1f0e2b9d7e",
                "iss": ISSUER, "aud": [AUD, "account"], "sub": "device-7",
                "typ": "Bearer", "azp": "plant-gateway", "acr": "1",
                "realm_access": {"roles": ["offline_access", "uma_authorization"]},
                "scope": "openid profile", "email_verified": false,
                "preferred_username": "device-7", "groups": ["sensors"],
            }),
        );
        let id = auth(&a, &token).await.unwrap();
        assert_eq!(id.subject, "device-7");
        assert_eq!(id.groups, vec!["sensors".to_string()]);

        let elsewhere = mint(
            "t1",
            &json!({"sub":"device-7","iss":ISSUER,"aud":["account","other"],"exp":exp()}),
        );
        assert!(matches!(
            auth(&a, &elsewhere).await.unwrap_err(),
            AuthError::Rejected
        ));
    }

    /// The issuer and the audience are required in OIDC mode (ADR 0050): a token that does
    /// not carry `iss` or `aud` is refused, like one that carries the wrong value.
    #[tokio::test]
    async fn a_token_without_an_issuer_or_an_audience_is_rejected() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        for claims in [
            json!({"sub":"d","iss":ISSUER,"exp":exp()}),
            json!({"sub":"d","aud":AUD,"exp":exp()}),
            json!({"sub":"d","exp":exp()}),
        ] {
            assert!(
                matches!(
                    auth(&a, &mint("t1", &claims)).await.unwrap_err(),
                    AuthError::Rejected
                ),
                "{claims}"
            );
        }
    }

    /// An `iss`, `aud` or `sub` of the wrong type is refused, whatever the signature.
    #[tokio::test]
    async fn claims_of_the_wrong_type_are_rejected() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        for claims in [
            json!({"sub":"d","iss":ISSUER,"aud":1234,"exp":exp()}),
            json!({"sub":"d","iss":ISSUER,"aud":{"aud":AUD},"exp":exp()}),
            json!({"sub":"d","iss":1234,"aud":AUD,"exp":exp()}),
            json!({"sub":42,"iss":ISSUER,"aud":AUD,"exp":exp()}),
        ] {
            assert!(
                matches!(
                    auth(&a, &mint("t1", &claims)).await.unwrap_err(),
                    AuthError::Rejected
                ),
                "{claims}"
            );
        }
    }

    /// A header that lists a critical extension the verifier does not implement
    /// (RFC 7515 §4.1.11) is refused even though the signature checks out.
    #[tokio::test]
    async fn a_token_naming_an_unknown_critical_extension_is_rejected() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("t1".to_string());
        header.crit = Some(vec!["urn:example:must-understand".to_string()]);
        let key = EncodingKey::from_rsa_pem(PRIVATE_PEM).unwrap();
        let token = jsonwebtoken::encode(&header, &good_claims(), &key).unwrap();
        assert!(matches!(
            auth(&a, &token).await.unwrap_err(),
            AuthError::Rejected
        ));
    }

    /// A token is not accepted before its `nbf` (RFC 7519 §4.1.5), beyond the clock-skew
    /// leeway. One whose `nbf` has passed authenticates.
    #[tokio::test]
    async fn a_token_that_is_not_yet_valid_is_rejected() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        let mut early = good_claims();
        early["nbf"] = json!(now() + 3600);
        assert!(matches!(
            auth(&a, &mint("t1", &early)).await.unwrap_err(),
            AuthError::Rejected
        ));
        let mut started = good_claims();
        started["nbf"] = json!(now() - 60);
        assert!(auth(&a, &mint("t1", &started)).await.is_ok());
    }

    /// `iss` is a single string (RFC 7519 §4.1.1): an array is refused, even one that holds
    /// the expected issuer.
    #[tokio::test]
    async fn an_issuer_that_is_not_a_single_string_is_rejected() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        for iss in [json!([ISSUER]), json!([ISSUER, "https://other.test"])] {
            let mut claims = good_claims();
            claims["iss"] = iss;
            assert!(
                matches!(
                    auth(&a, &mint("t1", &claims)).await.unwrap_err(),
                    AuthError::Rejected
                ),
                "{claims}"
            );
        }
    }

    /// With `require_at_jwt`, only a token typed as an RFC 9068 access token authenticates:
    /// `at+jwt` or `application/at+jwt`, in any case. An ID token's `JWT`, and a token with
    /// no `typ`, are refused. The default accepts any type.
    #[tokio::test]
    async fn with_at_jwt_required_only_an_access_token_type_authenticates() {
        let typed = |typ: Option<&str>| {
            let mut header = Header::new(Algorithm::RS256);
            header.kid = Some("t1".to_string());
            header.typ = typ.map(str::to_string);
            let key = EncodingKey::from_rsa_pem(PRIVATE_PEM).unwrap();
            jsonwebtoken::encode(&header, &good_claims(), &key).unwrap()
        };
        let mut cfg = OidcConfig::new(ISSUER.to_string(), AUD.to_string());
        cfg.require_at_jwt = true;
        let (a, _rx) = OidcAuthenticator::new(cfg);
        a.install_jwks(JWKS).unwrap();
        for typ in ["at+jwt", "application/at+jwt", "AT+JWT"] {
            assert!(auth(&a, &typed(Some(typ))).await.is_ok(), "{typ}");
        }
        for typ in [Some("JWT"), None] {
            assert!(
                matches!(
                    auth(&a, &typed(typ)).await.unwrap_err(),
                    AuthError::Rejected
                ),
                "{typ:?}"
            );
        }

        let (any_type, _rx) = authenticator();
        any_type.install_jwks(JWKS).unwrap();
        assert!(auth(&any_type, &typed(Some("JWT"))).await.is_ok());
    }

    /// `alg` is optional in a JWK (RFC 7517 §4.4). A key without it verifies the one
    /// allowed algorithm its type implies: RS256 for an RSA key, ES256 for a P-256 key.
    #[tokio::test]
    async fn a_key_without_alg_verifies_the_algorithm_its_type_implies() {
        let mut jwks: serde_json::Value = serde_json::from_slice(JWKS).unwrap();
        jwks["keys"][0].as_object_mut().unwrap().remove("alg");
        let (a, _rx) = authenticator();
        a.install_jwks(&serde_json::to_vec(&jwks).unwrap()).unwrap();
        assert!(auth(&a, &mint("t1", &good_claims())).await.is_ok());

        let pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let signing = EncodingKey::from_ec_pem(pair.serialize_pem().as_bytes()).unwrap();
        let mut jwk =
            jsonwebtoken::jwk::Jwk::from_encoding_key(&signing, Algorithm::ES256).unwrap();
        jwk.common.key_id = Some("e1".to_string());
        jwk.common.key_algorithm = None;
        let (a, _rx) = authenticator();
        a.install_jwks(&serde_json::to_vec(&json!({ "keys": [jwk] })).unwrap())
            .unwrap();
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some("e1".to_string());
        let token = jsonwebtoken::encode(&header, &good_claims(), &signing).unwrap();
        assert!(auth(&a, &token).await.is_ok());
    }

    /// A key published for encryption (`use: enc`) never verifies a signature (RFC 7517
    /// §4.2), so a JWKS that holds only such keys installs nothing.
    #[test]
    fn a_key_published_for_encryption_is_not_used() {
        let mut jwks: serde_json::Value = serde_json::from_slice(JWKS).unwrap();
        jwks["keys"][0]["use"] = json!("enc");
        let (a, _rx) = authenticator();
        assert!(a.install_jwks(&serde_json::to_vec(&jwks).unwrap()).is_err());
    }

    #[tokio::test]
    async fn an_unknown_kid_rejects_and_hints_exactly_one_refresh() {
        let (a, rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        // Two misses in a row: both reject; the bounded(1) channel holds ONE hint —
        // the debounce is the channel capacity, not a timer.
        for _ in 0..2 {
            assert!(matches!(
                auth(&a, &mint("rotated-away", &good_claims()))
                    .await
                    .unwrap_err(),
                AuthError::Rejected
            ));
        }
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err(), "hint channel must debounce");
    }

    #[tokio::test]
    async fn hs256_is_refused_even_if_it_matches_a_key() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        // An HS256 token signed with... anything: refused on the allow-list before
        // key selection (key-confusion guard).
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &good_claims(),
            &EncodingKey::from_secret(b"whatever").expect("a non-empty secret"),
        )
        .unwrap();
        assert!(matches!(
            auth(&a, &token).await.unwrap_err(),
            AuthError::Rejected
        ));
    }

    #[tokio::test]
    async fn staleness_beyond_the_window_fails_closed_then_reinstall_recovers() {
        let (a, rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        a.age_keys_for_test(Duration::from_hours(25));
        assert!(a.is_stale());
        let err = auth(&a, &mint("t1", &good_claims())).await.unwrap_err();
        assert!(matches!(err, AuthError::Backend(_)));
        assert!(rx.try_recv().is_ok(), "staleness must hint a refetch");
        // A fresh install (the fetch loop recovering) restores service.
        a.install_jwks(JWKS).unwrap();
        assert!(auth(&a, &mint("t1", &good_claims())).await.is_ok());
    }

    #[tokio::test]
    async fn a_garbled_jwks_never_replaces_the_last_known_good_set() {
        let (a, _rx) = authenticator();
        a.install_jwks(JWKS).unwrap();
        assert!(a.install_jwks(b"{ not json").is_err());
        assert!(a.install_jwks(br#"{"keys":[]}"#).is_err());
        // The original keys still serve (validate-before-swap).
        assert!(auth(&a, &mint("t1", &good_claims())).await.is_ok());
    }

    #[tokio::test]
    async fn non_token_credentials_chain_through() {
        let (a, _rx) = authenticator();
        assert!(matches!(
            auth_creds(&a, &Credentials::Anonymous).await.unwrap_err(),
            AuthError::NotPermitted
        ));
    }

    async fn auth_creds(a: &OidcAuthenticator, c: &Credentials<'_>) -> Result<Identity, AuthError> {
        a.authenticate(&ClientId("c1".into()), c).await
    }
}
