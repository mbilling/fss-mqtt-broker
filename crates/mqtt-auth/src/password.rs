//! Username/password authentication with Argon2id (ADR 0004 step 6).
//!
//! Passwords are verified against Argon2id PHC hashes — never plaintext, never
//! a fast hash. The credential store maps a username to its hash string
//! (`$argon2id$v=19$...`); a password file is one `username:phc-hash` per line.

use crate::{AuthError, Authenticator, Credentials, Identity};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use mqtt_core::ClientId;
use std::collections::HashMap;

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

    /// Produce a real Argon2id PHC hash for `password` with a fixed salt so the
    /// test is deterministic. (Verification uses the salt embedded in the hash.)
    fn hash_password(password: &[u8]) -> String {
        let salt = SaltString::encode_b64(b"fixed-salt-bytes").expect("valid salt");
        Argon2::default()
            .hash_password(password, &salt)
            .expect("hash")
            .to_string()
    }

    fn store(username: &str, password: &[u8]) -> PasswordAuthenticator {
        let mut m = HashMap::new();
        m.insert(username.to_string(), hash_password(password));
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
        let alice = hash_password(b"a-secret");
        let bob = hash_password(b"b-secret");
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
        let h = hash_password(b"x");
        let text = format!("alice:{h}\nalice:{h}\n");
        assert!(matches!(
            PasswordAuthenticator::from_file_contents(&text),
            Err(AuthError::Backend(_))
        ));
    }
}
