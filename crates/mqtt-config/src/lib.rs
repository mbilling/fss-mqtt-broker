//! Typed broker configuration with **secure defaults**.
//!
//! The defaults here encode the project's security posture: TLS-only listeners,
//! anonymous access disabled, deny-by-default authorization. Insecure options
//! exist but must be turned on deliberately.

use serde::{Deserialize, Serialize};

/// Top-level broker configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Network listeners.
    pub listeners: Listeners,
    /// Security policy.
    pub security: Security,
}

/// Configured listeners.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Listeners {
    /// TLS listener bind address (the default, secure listener).
    pub tls_bind: String,
    /// Optional plaintext listener bind address. `None` means disabled.
    pub plaintext_bind: Option<String>,
}

/// Security-related policy toggles.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Security {
    /// Whether anonymous connections are permitted. Default: `false`.
    pub allow_anonymous: bool,
    /// Whether a plaintext (non-TLS) listener may be enabled at all. Default: `false`.
    pub allow_plaintext: bool,
    /// Require client certificates (mTLS) on TLS listeners. Default: `true`.
    pub require_client_cert: bool,
}

impl Default for Listeners {
    fn default() -> Self {
        Self {
            tls_bind: "0.0.0.0:8883".to_string(),
            plaintext_bind: None,
        }
    }
}

impl Default for Security {
    fn default() -> Self {
        Self {
            allow_anonymous: false,
            allow_plaintext: false,
            require_client_cert: true,
        }
    }
}

/// Errors from configuration validation.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A combination of options is internally inconsistent.
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl Config {
    /// Validate that the configuration is internally consistent and that any
    /// insecure combination has been explicitly opted into.
    ///
    /// # Errors
    /// Returns [`ConfigError::Invalid`] if a plaintext listener is configured
    /// without the matching `allow_plaintext` opt-in.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.listeners.plaintext_bind.is_some() && !self.security.allow_plaintext {
            return Err(ConfigError::Invalid(
                "a plaintext listener is configured but security.allow_plaintext is false"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn defaults_are_secure() {
        let c = Config::default();
        assert!(!c.security.allow_anonymous);
        assert!(!c.security.allow_plaintext);
        assert!(c.security.require_client_cert);
        assert!(c.listeners.plaintext_bind.is_none());
        assert!(c.validate().is_ok());
    }

    #[test]
    fn plaintext_requires_opt_in() {
        let mut c = Config::default();
        c.listeners.plaintext_bind = Some("0.0.0.0:1883".into());
        assert!(c.validate().is_err());
        c.security.allow_plaintext = true;
        assert!(c.validate().is_ok());
    }
}
