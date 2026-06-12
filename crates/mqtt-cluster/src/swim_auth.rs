//! Gossip-plane authentication (ADR 0003): a keyed MAC over every SWIM
//! datagram, verified **before** any byte reaches the protocol state machine.
//!
//! Wire layout: `[VERSION][32-byte HMAC-SHA256 tag][payload]`. The tag is
//! computed over the payload with a cluster-shared 32-byte key; verification is
//! constant-time. Replay of captured datagrams is accepted and bounded by
//! SWIM's incarnation/refutation mechanism — see the ADR for the argument.
//!
//! The pure [`crate::swim`] module stays crypto-free; this seals/opens at the
//! I/O boundary only ([`crate::swim_driver`]).

use ring::hmac;

/// Format version byte, bumped on any change to the sealed layout.
const VERSION: u8 = 1;
/// HMAC-SHA256 tag length.
const TAG_LEN: usize = 32;
/// Required key length in bytes (64 hex characters).
pub const KEY_LEN: usize = 32;

/// A gossip key failed validation at startup.
#[derive(Debug, thiserror::Error)]
#[error("invalid SWIM gossip key: {0}")]
pub struct InvalidKey(&'static str);

/// Seals and opens SWIM datagrams with a cluster-shared key.
pub struct SwimAuth {
    key: hmac::Key,
}

impl std::fmt::Debug for SwimAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never expose key material, even via Debug.
        f.debug_struct("SwimAuth").finish_non_exhaustive()
    }
}

impl SwimAuth {
    /// Create from raw key bytes.
    #[must_use]
    pub fn new(key: &[u8; KEY_LEN]) -> Self {
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, key),
        }
    }

    /// Create from a 64-hex-character key string (e.g. `openssl rand -hex 32`).
    ///
    /// # Errors
    /// [`InvalidKey`] if the string is not exactly [`KEY_LEN`] bytes of hex —
    /// there is deliberately no weak-key or short-key mode.
    pub fn from_hex_key(hex: &str) -> Result<Self, InvalidKey> {
        let hex = hex.trim();
        let bytes = decode_hex(hex).ok_or(InvalidKey("not valid hexadecimal"))?;
        let key: [u8; KEY_LEN] = bytes
            .try_into()
            .map_err(|_| InvalidKey("must be exactly 32 bytes (64 hex characters)"))?;
        Ok(Self::new(&key))
    }

    /// Wrap a serialized SWIM message for the wire: version, tag, payload.
    #[must_use]
    pub fn seal(&self, payload: &[u8]) -> Vec<u8> {
        let tag = hmac::sign(&self.key, payload);
        let mut out = Vec::with_capacity(1 + TAG_LEN + payload.len());
        out.push(VERSION);
        out.extend_from_slice(tag.as_ref());
        out.extend_from_slice(payload);
        out
    }

    /// Verify a received datagram and return its payload, or `None` if it is
    /// malformed, the wrong version, or fails authentication (constant-time).
    #[must_use]
    pub fn open<'a>(&self, datagram: &'a [u8]) -> Option<&'a [u8]> {
        if datagram.len() < 1 + TAG_LEN || datagram[0] != VERSION {
            return None;
        }
        let (tag, payload) = datagram[1..].split_at(TAG_LEN);
        hmac::verify(&self.key, payload, tag).ok()?;
        Some(payload)
    }
}

/// Minimal hex decoder (avoids a dependency for one call site).
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{SwimAuth, KEY_LEN, TAG_LEN};
    use std::fmt::Write as _;

    fn auth(byte: u8) -> SwimAuth {
        SwimAuth::new(&[byte; KEY_LEN])
    }

    #[test]
    fn seal_then_open_roundtrips() {
        let a = auth(7);
        let sealed = a.seal(b"membership update");
        assert_eq!(a.open(&sealed), Some(&b"membership update"[..]));
    }

    /// Known-answer test pinning the wire format as a compatibility contract:
    /// version byte, HMAC-SHA256 (computed independently with Python's `hmac`),
    /// and the `[version][tag][payload]` layout. Changing the algorithm, tag
    /// length, or field order breaks rolling upgrades — this test makes such a
    /// change loud instead of silent.
    #[test]
    fn sealed_wire_format_matches_known_answer() {
        let mut key = [0u8; KEY_LEN];
        for (i, b) in key.iter_mut().enumerate() {
            *b = u8::try_from(i).unwrap();
        }
        let a = SwimAuth::new(&key);
        let sealed = a.seal(b"swim wire format v1");

        let expected = "01\
             339e989207e2b8bf79837d88e29490ed95e4e3a5b08219301b4033b966d49509\
             7377696d207769726520666f726d6174207631";
        let rendered = sealed.iter().fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        });
        assert_eq!(rendered, expected);
    }

    /// The empty payload sits exactly on the minimum-length boundary the
    /// `open` length check guards; it must seal and open cleanly (the decode
    /// layer above rejects it as non-SWIM, which is not this layer's job).
    #[test]
    fn empty_payload_is_the_minimum_valid_datagram() {
        let a = auth(7);
        let sealed = a.seal(b"");
        assert_eq!(sealed.len(), 1 + TAG_LEN);
        assert_eq!(a.open(&sealed), Some(&b""[..]));
    }

    #[test]
    fn any_flipped_bit_is_rejected() {
        let a = auth(7);
        let sealed = a.seal(b"payload");
        // Version, every tag byte, and every payload byte.
        for i in 0..sealed.len() {
            let mut tampered = sealed.clone();
            tampered[i] ^= 0x01;
            assert!(a.open(&tampered).is_none(), "bit flip at {i} accepted");
        }
    }

    #[test]
    fn wrong_key_is_rejected() {
        let sealed = auth(7).seal(b"payload");
        assert!(auth(8).open(&sealed).is_none());
    }

    #[test]
    fn truncated_and_empty_datagrams_are_rejected() {
        let a = auth(7);
        let sealed = a.seal(b"payload");
        // Below the minimum length: rejected by the length guard.
        assert!(a.open(&[]).is_none());
        assert!(a.open(&sealed[..TAG_LEN]).is_none());
        // At or above the minimum length but with the payload cut: the length
        // guard passes, so rejection must come from the MAC itself.
        assert!(a.open(&sealed[..=TAG_LEN]).is_none()); // payload fully cut
        assert!(a.open(&sealed[..sealed.len() - 1]).is_none()); // short by one
    }

    #[test]
    fn hex_key_parsing_enforces_exact_length() {
        let ok = "ab".repeat(KEY_LEN);
        assert!(SwimAuth::from_hex_key(&ok).is_ok());
        assert!(SwimAuth::from_hex_key(&format!("  {ok}\n")).is_ok()); // trimmed

        // Each rejection must carry the right diagnostic — operators debug key
        // mistakes from these messages.
        let reason = |s: &str| {
            SwimAuth::from_hex_key(s)
                .map(|_| ())
                .unwrap_err()
                .to_string()
        };
        assert!(reason("deadbeef").contains("exactly 32 bytes")); // too short
        assert!(reason(&"ab".repeat(KEY_LEN + 1)).contains("exactly 32 bytes")); // too long
        assert!(reason(&"zz".repeat(KEY_LEN)).contains("not valid hexadecimal"));
        let odd_length = "a".repeat(KEY_LEN * 2 - 1);
        assert!(reason(&odd_length).contains("not valid hexadecimal"));
    }

    #[test]
    fn debug_output_redacts_key_material() {
        // Pin the exact rendering: nothing key-derived may ever appear, in any
        // radix or formatting.
        assert_eq!(format!("{:?}", auth(0x42)), "SwimAuth { .. }");
    }
}
