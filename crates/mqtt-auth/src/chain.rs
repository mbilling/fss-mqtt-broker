//! Compose authenticators (ADR 0004 step 6).
//!
//! A [`ChainAuthenticator`] tries each member in order. The first that accepts
//! wins; a member that returns [`AuthError::NotPermitted`] (it does not handle
//! this credential kind) is skipped; any other error (a genuine rejection or a
//! backend failure from the authenticator that *does* handle the credential) is
//! final. If every member abstains, the chain returns `NotPermitted`.
//!
//! Typical order: a certificate/anonymous baseline, then password, then token.

use crate::{AuthError, Authenticator, Credentials, Identity};
use mqtt_core::ClientId;
use std::sync::Arc;

/// An ordered list of authenticators tried in sequence.
pub struct ChainAuthenticator {
    members: Vec<Arc<dyn Authenticator>>,
}

impl std::fmt::Debug for ChainAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainAuthenticator")
            .field("members", &self.members.len())
            .finish()
    }
}

impl ChainAuthenticator {
    /// Build a chain from an ordered list of authenticators.
    #[must_use]
    pub fn new(members: Vec<Arc<dyn Authenticator>>) -> Self {
        Self { members }
    }
}

impl Authenticator for ChainAuthenticator {
    fn authenticate(
        &self,
        client: &ClientId,
        creds: &Credentials<'_>,
    ) -> Result<Identity, AuthError> {
        for member in &self.members {
            match member.authenticate(client, creds) {
                // First acceptance wins.
                Ok(identity) => return Ok(identity),
                // Member doesn't handle this credential kind: try the next.
                Err(AuthError::NotPermitted) => {}
                // A member that *does* handle this credential has spoken
                // (rejection or backend failure): that verdict is final.
                Err(other) => return Err(other),
            }
        }
        Err(AuthError::NotPermitted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> ClientId {
        ClientId("c1".into())
    }

    /// Always returns the same fixed result.
    struct Fixed(Result<Identity, AuthError>);
    impl Authenticator for Fixed {
        fn authenticate(
            &self,
            _client: &ClientId,
            _creds: &Credentials<'_>,
        ) -> Result<Identity, AuthError> {
            match &self.0 {
                Ok(id) => Ok(id.clone()),
                Err(AuthError::Rejected) => Err(AuthError::Rejected),
                Err(AuthError::NotPermitted) => Err(AuthError::NotPermitted),
                Err(AuthError::Backend(m)) => Err(AuthError::Backend(m.clone())),
            }
        }
    }

    fn abstains() -> Arc<dyn Authenticator> {
        Arc::new(Fixed(Err(AuthError::NotPermitted)))
    }

    fn accepts(subject: &str) -> Arc<dyn Authenticator> {
        Arc::new(Fixed(Ok(Identity {
            subject: subject.to_string(),
            groups: vec![],
        })))
    }

    fn rejects() -> Arc<dyn Authenticator> {
        Arc::new(Fixed(Err(AuthError::Rejected)))
    }

    /// Panics if it is ever consulted — proves the chain stopped earlier.
    struct Explodes;
    impl Authenticator for Explodes {
        fn authenticate(
            &self,
            _client: &ClientId,
            _creds: &Credentials<'_>,
        ) -> Result<Identity, AuthError> {
            panic!("chain must not consult members after a final verdict");
        }
    }

    #[test]
    fn first_abstains_second_accepts_yields_ok() {
        let chain = ChainAuthenticator::new(vec![abstains(), accepts("alice")]);
        let id = chain
            .authenticate(&client(), &Credentials::Anonymous)
            .expect("second member must accept");
        assert_eq!(id.subject, "alice");
    }

    #[test]
    fn first_rejection_is_final_and_short_circuits() {
        // The exploding member must never be consulted once a member rejects.
        let chain = ChainAuthenticator::new(vec![rejects(), Arc::new(Explodes)]);
        assert!(matches!(
            chain.authenticate(&client(), &Credentials::Anonymous),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn first_backend_error_is_final_and_short_circuits() {
        let chain = ChainAuthenticator::new(vec![
            Arc::new(Fixed(Err(AuthError::Backend("idp down".into())))),
            Arc::new(Explodes),
        ]);
        assert!(matches!(
            chain.authenticate(&client(), &Credentials::Anonymous),
            Err(AuthError::Backend(_))
        ));
    }

    #[test]
    fn all_abstain_yields_not_permitted() {
        let chain = ChainAuthenticator::new(vec![abstains(), abstains()]);
        assert!(matches!(
            chain.authenticate(&client(), &Credentials::Anonymous),
            Err(AuthError::NotPermitted)
        ));
    }

    #[test]
    fn empty_chain_yields_not_permitted() {
        let chain = ChainAuthenticator::new(vec![]);
        assert!(matches!(
            chain.authenticate(&client(), &Credentials::Anonymous),
            Err(AuthError::NotPermitted)
        ));
    }
}
