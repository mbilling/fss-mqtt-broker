//! MQTT 5.0 enhanced authentication: a SASL-style, multi-round AUTH exchange
//! (ADR 0013).
//!
//! The single-shot [`Authenticator`](crate::Authenticator) verifies a credential
//! once. Enhanced auth instead runs a challenge/response negotiated by the
//! Authentication Method/Data properties and carried by AUTH packets, letting a
//! client prove a secret without transmitting it. This module owns the
//! mechanism-agnostic abstraction plus one reference mechanism (HMAC-SHA256).

use crate::Identity;
use mqtt_core::ClientId;
use ring::{hmac, rand::SecureRandom};
use std::collections::HashMap;
use std::sync::Arc;

/// The outcome of one step of an enhanced-auth exchange.
#[derive(Debug)]
pub enum AuthStep {
    /// Send this data to the client in an AUTH (Continue, `0x18`) and await its reply.
    Challenge(Vec<u8>),
    /// The client is authenticated as this identity; proceed to CONNACK success.
    Success(Identity),
    /// Authentication failed; reject the connection.
    Failure,
}

/// A registered enhanced-authentication mechanism, selected by its method name.
pub trait EnhancedAuthenticator: Send + Sync {
    /// The Authentication Method name this implements (matched against the CONNECT's).
    fn method(&self) -> &str;
    /// Begin a fresh exchange for one connection.
    fn start(&self) -> Box<dyn AuthSession>;
}

/// One in-flight enhanced-auth exchange for a single connection. It holds the
/// per-exchange state (nonces, round counter) the mechanism needs.
pub trait AuthSession: Send {
    /// Process auth data from the client — the CONNECT's initial Authentication Data
    /// first, then each subsequent AUTH packet's — and decide the next step.
    fn step(&mut self, client: &ClientId, data: &[u8]) -> AuthStep;
}

/// Length of the random challenge nonce, in bytes.
const NONCE_LEN: usize = 32;

/// An HMAC-SHA256 challenge/response mechanism (ADR 0013 §3).
///
/// The client names itself in the CONNECT's initial Authentication Data; the server
/// replies with a random nonce; the client returns `HMAC-SHA256(secret, nonce)`,
/// which the server verifies constant-time against the subject's shared secret. The
/// secret never crosses the wire.
pub struct HmacChallengeAuthenticator {
    secrets: Arc<HashMap<String, Vec<u8>>>,
}

// Hand-written so the shared secrets never appear in debug output.
impl std::fmt::Debug for HmacChallengeAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmacChallengeAuthenticator")
            .field("subjects", &self.secrets.len())
            .finish_non_exhaustive()
    }
}

impl HmacChallengeAuthenticator {
    /// The Authentication Method name this mechanism answers to.
    pub const METHOD: &'static str = "HMAC-SHA256";

    /// Build the mechanism from a map of `subject -> shared secret`.
    #[must_use]
    pub fn new(secrets: HashMap<String, Vec<u8>>) -> Self {
        Self {
            secrets: Arc::new(secrets),
        }
    }
}

impl EnhancedAuthenticator for HmacChallengeAuthenticator {
    fn method(&self) -> &str {
        Self::METHOD
    }

    fn start(&self) -> Box<dyn AuthSession> {
        Box::new(HmacSession {
            secrets: Arc::clone(&self.secrets),
            rng: ring::rand::SystemRandom::new(),
            pending: None,
        })
    }
}

/// One HMAC challenge/response exchange.
struct HmacSession {
    secrets: Arc<HashMap<String, Vec<u8>>>,
    rng: ring::rand::SystemRandom,
    /// `(subject, nonce)` once a challenge has been issued and we await the proof.
    pending: Option<(String, [u8; NONCE_LEN])>,
}

impl AuthSession for HmacSession {
    fn step(&mut self, _client: &ClientId, data: &[u8]) -> AuthStep {
        match self.pending.take() {
            // First step: `data` is the subject the client claims. Issue a nonce —
            // even for an unknown subject, to blunt user enumeration.
            None => {
                let Ok(subject) = std::str::from_utf8(data) else {
                    return AuthStep::Failure;
                };
                if subject.is_empty() {
                    return AuthStep::Failure;
                }
                let mut nonce = [0u8; NONCE_LEN];
                if self.rng.fill(&mut nonce).is_err() {
                    return AuthStep::Failure;
                }
                self.pending = Some((subject.to_string(), nonce));
                AuthStep::Challenge(nonce.to_vec())
            }
            // Second step: `data` is the client's HMAC over the nonce.
            Some((subject, nonce)) => {
                let Some(secret) = self.secrets.get(&subject) else {
                    return AuthStep::Failure;
                };
                let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
                match hmac::verify(&key, &nonce, data) {
                    Ok(()) => AuthStep::Success(Identity {
                        subject,
                        groups: Vec::new(),
                    }),
                    Err(_) => AuthStep::Failure,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthStep, EnhancedAuthenticator, HmacChallengeAuthenticator};
    use mqtt_core::ClientId;
    use ring::hmac;
    use std::collections::HashMap;

    fn cid() -> ClientId {
        ClientId("c".into())
    }

    fn authenticator() -> HmacChallengeAuthenticator {
        let mut secrets = HashMap::new();
        secrets.insert("alice".to_string(), b"alice-secret".to_vec());
        HmacChallengeAuthenticator::new(secrets)
    }

    /// A correct HMAC over the issued nonce authenticates the named subject.
    #[test]
    fn correct_proof_succeeds() {
        let a = authenticator();
        let mut s = a.start();
        let AuthStep::Challenge(nonce) = s.step(&cid(), b"alice") else {
            panic!("expected a challenge");
        };
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"alice-secret");
        let proof = hmac::sign(&key, &nonce);
        match s.step(&cid(), proof.as_ref()) {
            AuthStep::Success(id) => assert_eq!(id.subject, "alice"),
            _ => panic!("expected success"),
        }
    }

    /// A proof computed with the wrong secret is rejected.
    #[test]
    fn wrong_secret_fails() {
        let a = authenticator();
        let mut s = a.start();
        let AuthStep::Challenge(nonce) = s.step(&cid(), b"alice") else {
            panic!("expected a challenge");
        };
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"not-the-secret");
        let proof = hmac::sign(&key, &nonce);
        assert!(matches!(s.step(&cid(), proof.as_ref()), AuthStep::Failure));
    }

    /// An unknown subject is still challenged (enumeration resistance) but then fails.
    #[test]
    fn unknown_subject_is_challenged_then_fails() {
        let a = authenticator();
        let mut s = a.start();
        let AuthStep::Challenge(nonce) = s.step(&cid(), b"eve") else {
            panic!("an unknown subject still gets a challenge");
        };
        // Even a "correct-looking" HMAC under any key cannot pass: there is no secret.
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"eve-guess");
        let proof = hmac::sign(&key, &nonce);
        assert!(matches!(s.step(&cid(), proof.as_ref()), AuthStep::Failure));
    }

    #[test]
    fn empty_subject_fails_immediately() {
        let a = authenticator();
        let mut s = a.start();
        assert!(matches!(s.step(&cid(), b""), AuthStep::Failure));
    }

    #[test]
    fn method_name_is_advertised() {
        assert_eq!(authenticator().method(), "HMAC-SHA256");
    }
}
