//! The effective config as an operator may see it ([ADR 0081](../../../docs/adr/0081-admin-api.md)):
//! `mqttd --print-config` (T11) and the admin API's `GET /admin/v1/config` (T6) both
//! render through here, so neither can print a secret the other would hide.

use mqtt_config::Config;

/// The fingerprint that stands in for a secret value: `sha256:` plus the first 8 bytes of
/// its SHA-256 as hex — the same truncation the SWIM key fingerprints on `/statusz` use
/// (ADR 0054), so a key printed here can be matched against the rotation posture there.
#[must_use]
pub fn secret_fingerprint(secret: &str) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, secret.as_bytes());
    format!("sha256:{}", mqtt_core::hex_lower(&digest.as_ref()[..8]))
}

/// `config` with every secret replaced by its [`secret_fingerprint`]
/// (see [`Config::redacted`] for what counts as a secret).
#[must_use]
pub fn redacted(config: &Config) -> Config {
    config.redacted(&secret_fingerprint)
}

/// [`redacted`], rendered as TOML — a document `--config` would accept back, apart from
/// the fingerprinted values.
///
/// # Errors
/// The serializer's message if the config cannot be rendered (not expected: every
/// section is plain data).
pub fn redacted_toml(config: &Config) -> Result<String, String> {
    toml::to_string(&redacted(config)).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::{redacted_toml, secret_fingerprint};

    #[test]
    fn fingerprint_is_stable_and_hides_the_value() {
        let fp = secret_fingerprint("hunter2");
        assert_eq!(fp, secret_fingerprint("hunter2"));
        assert!(fp.starts_with("sha256:") && fp.len() == "sha256:".len() + 16);
        assert!(!fp.contains("hunter2"));
    }

    #[test]
    fn the_printed_config_carries_no_inline_key() {
        let mut c = mqtt_config::Config::default();
        let key = "ab".repeat(32);
        c.cluster.swim.key = Some(key.clone());
        let out = redacted_toml(&c).unwrap();
        assert!(!out.contains(&key));
        assert!(out.contains(&secret_fingerprint(&key)));
    }
}
