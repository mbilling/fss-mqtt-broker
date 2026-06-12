//! Authentication and authorization for the broker.
//!
//! Security posture: **deny by default**. Both authentication and authorization
//! are pluggable via traits so operators can wire mTLS, password, JWT/OIDC, LDAP,
//! or custom identity providers without forking the broker.

pub mod basic;
pub mod mtls;

use mqtt_core::{ClientId, TopicFilter, TopicName};

/// An authenticated principal's identity within the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Stable subject name (e.g. cert CN/SAN, username, or token subject).
    pub subject: String,
    /// Optional group/role memberships used by the authorizer.
    pub groups: Vec<String>,
}

/// Credentials presented at connection time, before authentication.
#[derive(Debug)]
pub enum Credentials<'a> {
    /// Anonymous connection (only honored if explicitly allowed).
    Anonymous,
    /// Username/password (password is verified against an Argon2id hash).
    Password {
        username: &'a str,
        password: &'a [u8],
    },
    /// A bearer token (JWT/OIDC).
    Token(&'a str),
    /// A verified client-certificate subject from mTLS.
    ClientCert { subject: &'a str },
}

/// Errors returned by authentication.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// Credentials were rejected.
    #[error("authentication failed")]
    Rejected,
    /// The presented credential type is not enabled on this listener.
    #[error("credential type not permitted")]
    NotPermitted,
    /// A transient backend failure (`IdP` unreachable, etc.).
    #[error("authentication backend error: {0}")]
    Backend(String),
}

/// An action a client wishes to perform, for authorization checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Publish to a concrete topic.
    Publish,
    /// Subscribe to a topic filter.
    Subscribe,
}

/// Verifies client credentials and returns an [`Identity`].
///
/// Implementations MUST be constant-time where they compare secrets and MUST NOT
/// log credential material.
pub trait Authenticator: Send + Sync {
    /// Authenticate the given credentials for a client id.
    ///
    /// # Errors
    /// Returns [`AuthError`] if the credentials are rejected or cannot be verified.
    fn authenticate(
        &self,
        client: &ClientId,
        creds: &Credentials<'_>,
    ) -> Result<Identity, AuthError>;
}

/// Decides whether an authenticated [`Identity`] may perform an [`Action`] on a topic.
pub trait Authorizer: Send + Sync {
    /// Returns `true` if the action is permitted. Default policy should be deny.
    fn authorize_publish(&self, identity: &Identity, topic: &TopicName) -> bool;
    /// Returns `true` if subscribing to `filter` is permitted.
    fn authorize_subscribe(&self, identity: &Identity, filter: &TopicFilter) -> bool;
}

/// A default-deny authorizer used until a real policy is configured.
#[derive(Debug, Default)]
pub struct DenyAll;

impl Authorizer for DenyAll {
    fn authorize_publish(&self, _identity: &Identity, _topic: &TopicName) -> bool {
        false
    }
    fn authorize_subscribe(&self, _identity: &Identity, _filter: &TopicFilter) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deny_all_denies() {
        let id = Identity {
            subject: "alice".into(),
            groups: vec![],
        };
        let z = DenyAll;
        assert!(!z.authorize_publish(&id, &"a/b".to_string()));
        assert!(!z.authorize_subscribe(&id, &"a/#".to_string()));
    }

    /// The allow path through the trait object: a custom policy can grant
    /// per-identity access where `DenyAll` would refuse (the shape every real
    /// `Authorizer` implementation will take).
    #[test]
    fn custom_authorizer_allow_path_works_through_the_trait() {
        struct PrefixPolicy;
        impl Authorizer for PrefixPolicy {
            fn authorize_publish(&self, identity: &Identity, topic: &TopicName) -> bool {
                topic.starts_with(&format!("users/{}/", identity.subject))
            }
            fn authorize_subscribe(&self, identity: &Identity, filter: &TopicFilter) -> bool {
                identity.groups.iter().any(|g| g == "readers")
                    || self.authorize_publish(identity, filter)
            }
        }

        let alice = Identity {
            subject: "alice".into(),
            groups: vec![],
        };
        let reader = Identity {
            subject: "bob".into(),
            groups: vec!["readers".into()],
        };
        let z: &dyn Authorizer = &PrefixPolicy;

        assert!(z.authorize_publish(&alice, &"users/alice/state".to_string()));
        assert!(!z.authorize_publish(&alice, &"users/eve/state".to_string()));
        assert!(z.authorize_subscribe(&reader, &"anything/#".to_string()));
        assert!(!z.authorize_subscribe(&alice, &"anything/#".to_string()));
    }
}
