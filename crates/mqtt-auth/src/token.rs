//! JWT bearer-token authentication (ADR 0004 step 6).
//!
//! Verifies a [`crate::Credentials::Token`] against a configured key
//! (HS256 shared secret or an RS256 public key), checking signature, `exp`,
//! and—when configured—`iss`/`aud`. The identity subject is the `sub` claim;
//! groups come from a configurable claim. Full OIDC discovery / JWKS rotation
//! is a later step; this takes a single static verification key.

use crate::{AuthError, Authenticator, Credentials, Identity};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use mqtt_core::ClientId;
use serde::Deserialize;

/// JWT verification configuration.
#[derive(Debug, Clone)]
pub struct TokenConfig {
    /// Required issuer (`iss`), if any.
    pub issuer: Option<String>,
    /// Required audience (`aud`), if any.
    pub audience: Option<String>,
    /// Claim name to read group memberships from (default `groups`).
    pub groups_claim: String,
}

impl Default for TokenConfig {
    fn default() -> Self {
        Self {
            issuer: None,
            audience: None,
            groups_claim: "groups".to_string(),
        }
    }
}

/// Claims we read out of a verified token. `exp` is validated by the library
/// (it is a required spec claim by default); the configurable groups claim is
/// captured via the flattened `extra` map and read by name at authenticate
/// time.
#[derive(Debug, Deserialize)]
struct Claims {
    #[serde(default)]
    sub: String,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

/// Verifies [`Credentials::Token`]; other kinds return
/// [`AuthError::NotPermitted`] so it can chain behind other authenticators.
pub struct TokenAuthenticator {
    // `DecodingKey` holds secret/key material and does not implement `Debug`;
    // it is deliberately omitted from the `Debug` impl below so it never logs.
    key: DecodingKey,
    validation: Validation,
    groups_claim: String,
}

impl std::fmt::Debug for TokenAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenAuthenticator")
            .field("algorithm", &self.validation.algorithms)
            .field("groups_claim", &self.groups_claim)
            .finish_non_exhaustive()
    }
}

impl TokenAuthenticator {
    fn build(key: DecodingKey, algorithm: Algorithm, config: TokenConfig) -> Self {
        let mut validation = Validation::new(algorithm);
        if let Some(iss) = &config.issuer {
            validation.set_issuer(&[iss]);
        }
        if let Some(aud) = &config.audience {
            validation.set_audience(&[aud]);
        } else {
            // No audience configured: don't require/validate `aud`.
            validation.validate_aud = false;
        }
        Self {
            key,
            validation,
            groups_claim: config.groups_claim,
        }
    }

    /// Build an HS256 verifier from a shared secret.
    #[must_use]
    pub fn hs256(secret: &[u8], config: TokenConfig) -> Self {
        Self::build(DecodingKey::from_secret(secret), Algorithm::HS256, config)
    }

    /// Build an RS256 verifier from a PEM-encoded public key.
    ///
    /// # Errors
    /// [`AuthError::Backend`] if the PEM key cannot be parsed.
    pub fn rs256_pem(pem: &[u8], config: TokenConfig) -> Result<Self, AuthError> {
        let key = DecodingKey::from_rsa_pem(pem)
            .map_err(|e| AuthError::Backend(format!("invalid RS256 PEM key: {e}")))?;
        Ok(Self::build(key, Algorithm::RS256, config))
    }
}

impl Authenticator for TokenAuthenticator {
    fn authenticate(
        &self,
        _client: &ClientId,
        creds: &Credentials<'_>,
    ) -> Result<Identity, AuthError> {
        let Credentials::Token(jwt) = creds else {
            return Err(AuthError::NotPermitted);
        };
        // Signature, `exp`, and (when configured) `iss`/`aud` are all checked
        // here; any failure is an indistinguishable rejection.
        let data = jsonwebtoken::decode::<Claims>(jwt, &self.key, &self.validation)
            .map_err(|_| AuthError::Rejected)?;
        let claims = data.claims;
        if claims.sub.is_empty() {
            return Err(AuthError::Rejected);
        }
        let groups = claims
            .extra
            .get(&self.groups_claim)
            .and_then(serde_json::Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Identity {
            subject: claims.sub,
            groups,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    const SECRET: &[u8] = b"super-secret-hs256-key";

    fn client() -> ClientId {
        ClientId("c1".into())
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs()
    }

    /// Sign an arbitrary claims object as an HS256 token with `SECRET`.
    fn hs256_token(claims: &serde_json::Value) -> String {
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(SECRET),
        )
        .expect("encode")
    }

    #[test]
    fn valid_hs256_token_authenticates_with_sub_as_subject() {
        let auth = TokenAuthenticator::hs256(SECRET, TokenConfig::default());
        let token = hs256_token(&json!({ "sub": "alice", "exp": now() + 3600 }));
        let id = auth
            .authenticate(&client(), &Credentials::Token(&token))
            .expect("valid token must authenticate");
        assert_eq!(id.subject, "alice");
        assert!(id.groups.is_empty());
    }

    #[test]
    fn groups_are_parsed_from_the_default_claim() {
        let auth = TokenAuthenticator::hs256(SECRET, TokenConfig::default());
        let token = hs256_token(&json!({
            "sub": "alice",
            "exp": now() + 3600,
            "groups": ["admins", "ops"],
        }));
        let id = auth
            .authenticate(&client(), &Credentials::Token(&token))
            .expect("valid token");
        assert_eq!(id.groups, vec!["admins".to_string(), "ops".to_string()]);
    }

    #[test]
    fn a_custom_groups_claim_name_is_honored() {
        let config = TokenConfig {
            groups_claim: "roles".to_string(),
            ..TokenConfig::default()
        };
        let auth = TokenAuthenticator::hs256(SECRET, config);
        let token = hs256_token(&json!({
            "sub": "alice",
            "exp": now() + 3600,
            // The default "groups" claim is ignored; only "roles" is read.
            "groups": ["ignored"],
            "roles": ["reader"],
        }));
        let id = auth
            .authenticate(&client(), &Credentials::Token(&token))
            .expect("valid token");
        assert_eq!(id.groups, vec!["reader".to_string()]);
    }

    #[test]
    fn tampered_or_wrong_secret_signature_is_rejected() {
        let auth = TokenAuthenticator::hs256(SECRET, TokenConfig::default());
        let forged = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &json!({ "sub": "alice", "exp": now() + 3600 }),
            &EncodingKey::from_secret(b"a-different-secret"),
        )
        .expect("encode");
        assert!(matches!(
            auth.authenticate(&client(), &Credentials::Token(&forged)),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn expired_token_is_rejected() {
        let auth = TokenAuthenticator::hs256(SECRET, TokenConfig::default());
        // exp well beyond the library's default 60s leeway.
        let token = hs256_token(&json!({ "sub": "alice", "exp": now() - 3600 }));
        assert!(matches!(
            auth.authenticate(&client(), &Credentials::Token(&token)),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn wrong_issuer_is_rejected_when_issuer_is_configured() {
        let config = TokenConfig {
            issuer: Some("https://issuer.example".to_string()),
            ..TokenConfig::default()
        };
        let auth = TokenAuthenticator::hs256(SECRET, config);
        let token = hs256_token(&json!({
            "sub": "alice",
            "exp": now() + 3600,
            "iss": "https://evil.example",
        }));
        assert!(matches!(
            auth.authenticate(&client(), &Credentials::Token(&token)),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn correct_issuer_is_accepted_when_issuer_is_configured() {
        let config = TokenConfig {
            issuer: Some("https://issuer.example".to_string()),
            ..TokenConfig::default()
        };
        let auth = TokenAuthenticator::hs256(SECRET, config);
        let token = hs256_token(&json!({
            "sub": "alice",
            "exp": now() + 3600,
            "iss": "https://issuer.example",
        }));
        assert!(auth
            .authenticate(&client(), &Credentials::Token(&token))
            .is_ok());
    }

    #[test]
    fn wrong_audience_is_rejected_when_audience_is_configured() {
        let config = TokenConfig {
            audience: Some("broker".to_string()),
            ..TokenConfig::default()
        };
        let auth = TokenAuthenticator::hs256(SECRET, config);
        let token = hs256_token(&json!({
            "sub": "alice",
            "exp": now() + 3600,
            "aud": "someone-else",
        }));
        assert!(matches!(
            auth.authenticate(&client(), &Credentials::Token(&token)),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn correct_audience_is_accepted_when_audience_is_configured() {
        let config = TokenConfig {
            audience: Some("broker".to_string()),
            ..TokenConfig::default()
        };
        let auth = TokenAuthenticator::hs256(SECRET, config);
        let token = hs256_token(&json!({
            "sub": "alice",
            "exp": now() + 3600,
            "aud": "broker",
        }));
        assert!(auth
            .authenticate(&client(), &Credentials::Token(&token))
            .is_ok());
    }

    #[test]
    fn missing_sub_is_rejected() {
        let auth = TokenAuthenticator::hs256(SECRET, TokenConfig::default());
        let token = hs256_token(&json!({ "exp": now() + 3600 }));
        assert!(matches!(
            auth.authenticate(&client(), &Credentials::Token(&token)),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn empty_sub_is_rejected() {
        let auth = TokenAuthenticator::hs256(SECRET, TokenConfig::default());
        let token = hs256_token(&json!({ "sub": "", "exp": now() + 3600 }));
        assert!(matches!(
            auth.authenticate(&client(), &Credentials::Token(&token)),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn non_token_credentials_are_not_permitted() {
        let auth = TokenAuthenticator::hs256(SECRET, TokenConfig::default());
        for creds in [
            Credentials::Anonymous,
            Credentials::Password {
                username: "a",
                password: b"b",
            },
            Credentials::ClientCert { subject: "cn" },
        ] {
            assert!(matches!(
                auth.authenticate(&client(), &creds),
                Err(AuthError::NotPermitted)
            ));
        }
    }

    // --- RS256 ---------------------------------------------------------------
    //
    // TEST-ONLY 2048-bit RSA keypair. rcgen's `ring` backend cannot *generate*
    // RSA keys, so an embedded known-good PEM pair is used to mint and verify a
    // token. These keys protect nothing; they exist solely for this test.

    const TEST_RSA_PRIVATE_PEM: &[u8] = include_bytes!("testdata/rs256_private.pem");
    const TEST_RSA_PUBLIC_PEM: &[u8] = include_bytes!("testdata/rs256_public.pem");

    #[test]
    fn rs256_pem_rejects_garbage_pem() {
        assert!(matches!(
            TokenAuthenticator::rs256_pem(b"not a pem at all", TokenConfig::default()),
            Err(AuthError::Backend(_))
        ));
    }

    #[test]
    fn rs256_pem_parses_a_valid_public_key() {
        assert!(TokenAuthenticator::rs256_pem(TEST_RSA_PUBLIC_PEM, TokenConfig::default()).is_ok());
    }

    #[test]
    fn rs256_token_signed_by_matching_private_key_verifies() {
        let auth = TokenAuthenticator::rs256_pem(TEST_RSA_PUBLIC_PEM, TokenConfig::default())
            .expect("valid public PEM");
        let key = EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_PEM).expect("valid private PEM");
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::RS256),
            &json!({ "sub": "alice", "exp": now() + 3600, "groups": ["ops"] }),
            &key,
        )
        .expect("encode");
        let id = auth
            .authenticate(&client(), &Credentials::Token(&token))
            .expect("RS256 token signed by matching key must verify");
        assert_eq!(id.subject, "alice");
        assert_eq!(id.groups, vec!["ops".to_string()]);
    }
}
