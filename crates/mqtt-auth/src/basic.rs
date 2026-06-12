//! The built-in authentication policy (ADR 0004): certificate identities are
//! accepted as-is (the TLS layer already verified the chain), anonymous access
//! requires an explicit opt-in, and credential types without a configured
//! verifier are refused.

use crate::{AuthError, Authenticator, Credentials, Identity};
use mqtt_core::ClientId;

/// Identity policy for listeners without password/token verifiers configured.
#[derive(Debug)]
pub struct BasicAuthenticator {
    /// Permit connections that present no credentials at all. Default-off;
    /// enabling it is an explicit, loudly-logged operator decision.
    pub allow_anonymous: bool,
}

impl Authenticator for BasicAuthenticator {
    fn authenticate(
        &self,
        _client: &ClientId,
        creds: &Credentials<'_>,
    ) -> Result<Identity, AuthError> {
        match creds {
            // The TLS layer already verified the certificate chain; the
            // extracted subject is trusted as-is.
            Credentials::ClientCert { subject } => Ok(Identity {
                subject: (*subject).to_string(),
                groups: vec![],
            }),
            Credentials::Anonymous if self.allow_anonymous => Ok(Identity {
                subject: "anonymous".into(),
                groups: vec![],
            }),
            Credentials::Anonymous => Err(AuthError::Rejected),
            // No password/token verifier is configured at this step; presented
            // credentials fail closed instead of falling back to anonymous.
            Credentials::Password { .. } | Credentials::Token(_) => Err(AuthError::NotPermitted),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> ClientId {
        ClientId("c1".into())
    }

    #[test]
    fn client_cert_subject_is_accepted_verbatim() {
        let auth = BasicAuthenticator {
            allow_anonymous: false,
        };
        let id = auth
            .authenticate(
                &client(),
                &Credentials::ClientCert {
                    subject: "device-7",
                },
            )
            .expect("TLS-verified certificate subjects must be accepted");
        assert_eq!(
            id,
            Identity {
                subject: "device-7".into(),
                groups: vec![],
            }
        );
    }

    #[test]
    fn anonymous_is_rejected_by_default() {
        let auth = BasicAuthenticator {
            allow_anonymous: false,
        };
        assert!(matches!(
            auth.authenticate(&client(), &Credentials::Anonymous),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn anonymous_is_accepted_when_opted_in() {
        let auth = BasicAuthenticator {
            allow_anonymous: true,
        };
        let id = auth
            .authenticate(&client(), &Credentials::Anonymous)
            .expect("anonymous must be accepted when explicitly allowed");
        assert_eq!(id.subject, "anonymous");
        assert!(id.groups.is_empty());
    }

    #[test]
    fn password_is_not_permitted_without_a_verifier() {
        // Even with anonymous allowed: credentials we cannot verify must fail
        // closed rather than silently fall back to anonymous.
        let auth = BasicAuthenticator {
            allow_anonymous: true,
        };
        assert!(matches!(
            auth.authenticate(
                &client(),
                &Credentials::Password {
                    username: "alice",
                    password: b"secret",
                },
            ),
            Err(AuthError::NotPermitted)
        ));
    }

    #[test]
    fn token_is_not_permitted_without_a_verifier() {
        let auth = BasicAuthenticator {
            allow_anonymous: true,
        };
        assert!(matches!(
            auth.authenticate(&client(), &Credentials::Token("ey.fake.jwt")),
            Err(AuthError::NotPermitted)
        ));
    }
}
