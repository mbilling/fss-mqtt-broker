//! Username/password authentication with Argon2id (ADR 0004 step 6).
//!
//! Passwords are verified against Argon2id PHC hashes — never plaintext, never
//! a fast hash. The credential store maps a username to its hash string
//! (`$argon2id$v=19$...`); a password file is one `username:phc-hash` per line.

use crate::{AuthError, Authenticator, Credentials, Identity};
use argon2::password_hash::{Salt, SaltString};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use mqtt_core::ClientId;
use std::collections::HashMap;

/// Hash `password` into an Argon2id PHC string suitable for a password file line.
///
/// The salt is freshly random per call, so hashing the same password twice yields
/// different strings — both verify. Parameters are `Argon2::default()`, the same
/// configuration [`PasswordAuthenticator::authenticate`] verifies against, so a hash
/// produced here is always accepted there.
///
/// Exposed because it was otherwise impossible to *use* password authentication: the
/// broker verifies Argon2id hashes and the documentation tells operators to write
/// `username:hash` lines, but nothing shipped could produce one. `mosquitto_passwd`
/// output is not compatible. `mqttd --hash-password` is the front end for this.
///
/// The salt comes from the workspace's single crypto provider (`aws-lc-rs`, ADR 0053),
/// not from a second RNG crate pulled in for this one call.
///
/// # Errors
/// [`AuthError::Backend`] if the system RNG or the hasher fails.
pub fn hash_password(password: &str) -> Result<String, AuthError> {
    let mut bytes = [0u8; Salt::RECOMMENDED_LENGTH];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| AuthError::Backend("system RNG unavailable for the password salt".into()))?;
    let salt = SaltString::encode_b64(&bytes)
        .map_err(|e| AuthError::Backend(format!("failed to encode the password salt: {e}")))?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| AuthError::Backend(format!("failed to hash password: {e}")))
}

/// Verifies [`Credentials::Password`] against stored Argon2id hashes. Other
/// credential kinds return [`AuthError::NotPermitted`] so this can sit in a
/// [`crate::chain::ChainAuthenticator`] behind a certificate authenticator.
#[derive(Debug, Default)]
pub struct PasswordAuthenticator {
    hashes: HashMap<String, String>,
}

impl PasswordAuthenticator {
    /// Build from a username -> Argon2id PHC hash map.
    #[must_use]
    pub fn new(hashes: HashMap<String, String>) -> Self {
        Self { hashes }
    }

    /// Parse a password file: one `username:phc-hash` per line; `#` comments
    /// and blank lines ignored.
    ///
    /// # Errors
    /// [`AuthError::Backend`] if a non-blank, non-comment line lacks a `:`
    /// separator or repeats a username.
    pub fn from_file_contents(text: &str) -> Result<Self, AuthError> {
        let mut hashes = HashMap::new();
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (username, hash) = line
                .split_once(':')
                .ok_or_else(|| AuthError::Backend("password line missing ':' separator".into()))?;
            if hashes
                .insert(username.to_string(), hash.to_string())
                .is_some()
            {
                return Err(AuthError::Backend(format!(
                    "duplicate username in password file: {username}"
                )));
            }
        }
        Ok(Self { hashes })
    }
}

impl Authenticator for PasswordAuthenticator {
    fn password_subject_exists(&self, subject: &str) -> bool {
        // The subject of a password admission is the username (see `authenticate`).
        self.hashes.contains_key(subject)
    }

    fn authenticate(
        &self,
        _client: &ClientId,
        creds: &Credentials<'_>,
    ) -> Result<Identity, AuthError> {
        let Credentials::Password { username, password } = creds else {
            return Err(AuthError::NotPermitted);
        };
        // Unknown username and verification failure must be indistinguishable
        // (no username-enumeration oracle): both map to `Rejected`.
        let stored = self.hashes.get(*username).ok_or(AuthError::Rejected)?;
        let parsed = PasswordHash::new(stored).map_err(|_| AuthError::Rejected)?;
        Argon2::default()
            .verify_password(password, &parsed)
            .map_err(|_| AuthError::Rejected)?;
        Ok(Identity {
            subject: (*username).to_string(),
            groups: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};

    fn client() -> ClientId {
        ClientId("c1".into())
    }

    /// Produce a real Argon2id PHC hash for `password` with a FIXED salt, so tests
    /// that want a deterministic string get one. Named apart from the crate's
    /// `hash_password` (random salt, the shipping path) so neither shadows the other.
    fn fixed_salt_hash(password: &[u8]) -> String {
        let salt = SaltString::encode_b64(b"fixed-salt-bytes").expect("valid salt");
        Argon2::default()
            .hash_password(password, &salt)
            .expect("hash")
            .to_string()
    }

    fn store(username: &str, password: &[u8]) -> PasswordAuthenticator {
        let mut m = HashMap::new();
        m.insert(username.to_string(), fixed_salt_hash(password));
        PasswordAuthenticator::new(m)
    }

    #[test]
    fn correct_password_authenticates_with_username_as_subject() {
        let auth = store("alice", b"correct horse");
        let id = auth
            .authenticate(
                &client(),
                &Credentials::Password {
                    username: "alice",
                    password: b"correct horse",
                },
            )
            .expect("correct password must authenticate");
        assert_eq!(
            id,
            Identity {
                subject: "alice".into(),
                groups: vec![],
            }
        );
    }

    #[test]
    fn wrong_password_is_rejected() {
        let auth = store("alice", b"correct horse");
        assert!(matches!(
            auth.authenticate(
                &client(),
                &Credentials::Password {
                    username: "alice",
                    password: b"wrong",
                },
            ),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn unknown_username_is_rejected_indistinguishably_from_wrong_password() {
        let auth = store("alice", b"correct horse");
        let unknown = auth.authenticate(
            &client(),
            &Credentials::Password {
                username: "mallory",
                password: b"whatever",
            },
        );
        let wrong = auth.authenticate(
            &client(),
            &Credentials::Password {
                username: "alice",
                password: b"wrong",
            },
        );
        // No username-enumeration oracle: both failures look identical.
        assert!(matches!(unknown, Err(AuthError::Rejected)));
        assert!(matches!(wrong, Err(AuthError::Rejected)));
        assert_eq!(format!("{unknown:?}"), format!("{wrong:?}"));
    }

    #[test]
    fn non_password_credentials_are_not_permitted() {
        let auth = store("alice", b"correct horse");
        for creds in [
            Credentials::Anonymous,
            Credentials::Token("ey.fake.jwt"),
            Credentials::ClientCert { subject: "cn" },
        ] {
            assert!(matches!(
                auth.authenticate(&client(), &creds),
                Err(AuthError::NotPermitted)
            ));
        }
    }

    #[test]
    fn file_parsing_handles_comments_and_blank_lines() {
        let alice = fixed_salt_hash(b"a-secret");
        let bob = fixed_salt_hash(b"b-secret");
        let text = format!("# users\n\nalice:{alice}\n\n# bob below\nbob:{bob}\n");
        let auth = PasswordAuthenticator::from_file_contents(&text).expect("good file parses");
        assert!(auth
            .authenticate(
                &client(),
                &Credentials::Password {
                    username: "alice",
                    password: b"a-secret",
                },
            )
            .is_ok());
        assert!(auth
            .authenticate(
                &client(),
                &Credentials::Password {
                    username: "bob",
                    password: b"b-secret",
                },
            )
            .is_ok());
    }

    #[test]
    fn file_parsing_rejects_a_line_without_a_colon() {
        let text = "alice:somehash\nno-colon-here\n";
        assert!(matches!(
            PasswordAuthenticator::from_file_contents(text),
            Err(AuthError::Backend(_))
        ));
    }

    #[test]
    fn file_parsing_rejects_a_duplicate_username() {
        let h = fixed_salt_hash(b"x");
        let text = format!("alice:{h}\nalice:{h}\n");
        assert!(matches!(
            PasswordAuthenticator::from_file_contents(&text),
            Err(AuthError::Backend(_))
        ));
    }

    /// The point of shipping [`hash_password`]: what it produces must be a password
    /// file line the broker actually accepts. A hasher whose output the verifier
    /// rejects would be worse than none — it would look like it worked.
    #[test]
    fn a_generated_hash_authenticates_through_a_password_file() {
        let hash = hash_password("correct horse battery staple").expect("hashing must succeed");
        let auth = PasswordAuthenticator::from_file_contents(&format!("alice:{hash}\n"))
            .expect("a generated hash must parse as a password file line");

        let id = auth
            .authenticate(
                &client(),
                &Credentials::Password {
                    username: "alice",
                    password: b"correct horse battery staple",
                },
            )
            .expect("the password that was hashed must authenticate against it");
        assert_eq!(id.subject, "alice");

        assert!(
            matches!(
                auth.authenticate(
                    &client(),
                    &Credentials::Password {
                        username: "alice",
                        password: b"correct horse battery stapl",
                    },
                ),
                Err(AuthError::Rejected)
            ),
            "a near-miss password must still be rejected — otherwise the test above \
             proves nothing about the hash"
        );
    }

    /// The salt is per call, so the same password never yields the same string twice.
    /// Equal hashes would mean a fixed salt shipped by accident, which makes the whole
    /// file rainbow-table-able.
    #[test]
    fn hashing_the_same_password_twice_gives_different_strings_that_both_verify() {
        let a = hash_password("same-password").expect("hash a");
        let b = hash_password("same-password").expect("hash b");
        assert_ne!(a, b, "each hash must carry its own random salt");

        for hash in [a, b] {
            let auth =
                PasswordAuthenticator::from_file_contents(&format!("u:{hash}\n")).expect("parses");
            assert!(
                auth.authenticate(
                    &client(),
                    &Credentials::Password {
                        username: "u",
                        password: b"same-password",
                    },
                )
                .is_ok(),
                "both independently-salted hashes must verify"
            );
        }
    }
}
