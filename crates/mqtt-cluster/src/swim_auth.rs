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
use std::sync::Arc;

/// Shared-key-only datagram (ADR 0003): `[1][tag][payload]`.
const VERSION_V1: u8 = 1;
/// Signed datagram (ADR 0022): `[2][tag][cert_len][cert][sig_len][sig][payload]`, the tag
/// covering everything after it.
const VERSION_V2: u8 = 2;
/// HMAC-SHA256 tag length.
const TAG_LEN: usize = 32;
/// Required key length in bytes (64 hex characters).
pub const KEY_LEN: usize = 32;

/// A gossip key failed validation at startup.
#[derive(Debug, thiserror::Error)]
#[error("invalid SWIM gossip key: {0}")]
pub struct InvalidKey(&'static str);

/// Signs an outgoing gossip payload with this node's cluster-bus key and supplies the leaf
/// certificate to embed (ADR 0022). Implemented in the broker, which holds the PKI material.
pub trait GossipSign: Send + Sync {
    /// This node's leaf certificate (DER), carried inline so receivers can chain-verify it.
    fn cert_der(&self) -> &[u8];
    /// A signature over `payload` with this node's private key.
    fn sign(&self, payload: &[u8]) -> Vec<u8>;
}

/// Verifies an inbound signed datagram: the inline certificate must chain to the cluster CA
/// and the signature must be valid over the payload (ADR 0022). Returns the certificate's
/// authenticated identity (Common Name) on success, `None` to reject.
pub trait GossipVerify: Send + Sync {
    /// Verify `cert_der` chains to the cluster CA and `sig` is valid over `payload`;
    /// return the certificate's Common Name (the authenticated sender identity).
    fn verify(&self, cert_der: &[u8], payload: &[u8], sig: &[u8]) -> Option<String>;
}

/// A successfully opened datagram: its payload and, when the datagram was signed and
/// verified, the authenticated sender identity for the driver to bind to the SWIM `from`.
#[derive(Debug, PartialEq, Eq)]
pub struct Opened<'a> {
    /// The serialized SWIM message.
    pub payload: &'a [u8],
    /// The authenticated node identity, when the datagram carried a verified signature.
    pub identity: Option<String>,
}

/// Seals and opens SWIM datagrams: always a cluster-shared-key HMAC (ADR 0003), and — when
/// a signer/verifier is configured — an additional per-node signature layer (ADR 0022).
pub struct SwimAuth {
    key: hmac::Key,
    /// When set, outgoing datagrams are signed (v2).
    signer: Option<Arc<dyn GossipSign>>,
    /// When set, an incoming v2 datagram's signature is verified and its identity returned.
    verifier: Option<Arc<dyn GossipVerify>>,
    /// When true, an unsigned (v1) datagram is rejected — the strict end state.
    require_signed: bool,
}

impl std::fmt::Debug for SwimAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never expose key material, even via Debug.
        f.debug_struct("SwimAuth")
            .field("signed", &self.signer.is_some())
            .field("require_signed", &self.require_signed)
            .finish_non_exhaustive()
    }
}

impl SwimAuth {
    /// Create a shared-key-only context from raw key bytes (ADR 0003).
    #[must_use]
    pub fn new(key: &[u8; KEY_LEN]) -> Self {
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, key),
            signer: None,
            verifier: None,
            require_signed: false,
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

    /// Add the per-node signature layer (ADR 0022): sign outgoing datagrams, and verify the
    /// signature on incoming ones. `require_signed` rejects unsigned (v1) datagrams — the
    /// strict mode; `false` keeps accepting v1 for a node-by-node rollout.
    #[must_use]
    pub fn with_signing(
        mut self,
        signer: Arc<dyn GossipSign>,
        verifier: Arc<dyn GossipVerify>,
        require_signed: bool,
    ) -> Self {
        self.signer = Some(signer);
        self.verifier = Some(verifier);
        self.require_signed = require_signed;
        self
    }

    /// Wrap a serialized SWIM message for the wire. With a signer, produces a v2 (signed)
    /// datagram; otherwise the v1 shared-key-only datagram.
    ///
    /// # Panics
    /// Panics if the leaf certificate or signature exceeds 64 KiB (a `u16` length field) —
    /// far beyond any real certificate, so this is a provisioning invariant, not input.
    #[must_use]
    pub fn seal(&self, payload: &[u8]) -> Vec<u8> {
        let Some(signer) = &self.signer else {
            return self.frame(VERSION_V1, payload);
        };
        // v2 body: [cert_len][cert][sig_len][sig][payload], tag computed over the whole body.
        let cert = signer.cert_der();
        let sig = signer.sign(payload);
        let cert_len = u16::try_from(cert.len()).expect("leaf certificate fits u16");
        let sig_len = u16::try_from(sig.len()).expect("signature fits u16");
        let mut body = Vec::with_capacity(2 + cert.len() + 2 + sig.len() + payload.len());
        body.extend_from_slice(&cert_len.to_be_bytes());
        body.extend_from_slice(cert);
        body.extend_from_slice(&sig_len.to_be_bytes());
        body.extend_from_slice(&sig);
        body.extend_from_slice(payload);
        self.frame(VERSION_V2, &body)
    }

    /// `[version][HMAC(body)][body]`.
    fn frame(&self, version: u8, body: &[u8]) -> Vec<u8> {
        let tag = hmac::sign(&self.key, body);
        let mut out = Vec::with_capacity(1 + TAG_LEN + body.len());
        out.push(version);
        out.extend_from_slice(tag.as_ref());
        out.extend_from_slice(body);
        out
    }

    /// Verify a received datagram and return its payload (plus authenticated identity for a
    /// signed datagram), or `None` if it is malformed, the wrong version, or fails any
    /// check. The shared-key HMAC is always required; the signature is required when
    /// `require_signed`, and verified whenever a verifier is configured.
    #[must_use]
    pub fn open<'a>(&self, datagram: &'a [u8]) -> Option<Opened<'a>> {
        if datagram.len() < 1 + TAG_LEN {
            return None;
        }
        let version = datagram[0];
        let (tag, body) = datagram[1..].split_at(TAG_LEN);
        // Shared-key gate + whole-datagram integrity (constant-time), before any parsing.
        hmac::verify(&self.key, body, tag).ok()?;

        match version {
            VERSION_V1 => {
                // Unsigned: only acceptable when we are not requiring signatures.
                if self.require_signed {
                    return None;
                }
                Some(Opened {
                    payload: body,
                    identity: None,
                })
            }
            VERSION_V2 => {
                let (cert, sig, payload) = parse_v2(body)?;
                match &self.verifier {
                    // Verify the signature and bind the authenticated identity.
                    Some(v) => {
                        let cn = v.verify(cert, payload, sig)?;
                        Some(Opened {
                            payload,
                            identity: Some(cn),
                        })
                    }
                    // No verifier (signatures off): the HMAC already authenticated the
                    // datagram as cluster-internal; accept the payload, unauthenticated id.
                    None => Some(Opened {
                        payload,
                        identity: None,
                    }),
                }
            }
            _ => None,
        }
    }
}

/// Parse a v2 body `[cert_len u16][cert][sig_len u16][sig][payload]`. Any short/overrunning
/// field yields `None` (rejected). Lengths are bounds-checked against the remaining slice.
fn parse_v2(body: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
    let cert_len = u16::from_be_bytes(body.get(0..2)?.try_into().ok()?) as usize;
    let rest = body.get(2..)?;
    let cert = rest.get(..cert_len)?;
    let rest = rest.get(cert_len..)?;
    let sig_len = u16::from_be_bytes(rest.get(0..2)?.try_into().ok()?) as usize;
    let rest = rest.get(2..)?;
    let sig = rest.get(..sig_len)?;
    let payload = rest.get(sig_len..)?;
    Some((cert, sig, payload))
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
    use super::{parse_v2, GossipSign, GossipVerify, SwimAuth, KEY_LEN, TAG_LEN, VERSION_V2};
    use std::fmt::Write as _;
    use std::sync::Arc;

    fn auth(byte: u8) -> SwimAuth {
        SwimAuth::new(&[byte; KEY_LEN])
    }

    /// A deterministic stand-in for the real PKI signer/verifier (those are exercised in
    /// `mqtt-auth` and `mqttd`); here we only test the seal/open/identity plumbing.
    struct FakeSigner {
        cert: Vec<u8>,
    }
    impl GossipSign for FakeSigner {
        fn cert_der(&self) -> &[u8] {
            &self.cert
        }
        fn sign(&self, payload: &[u8]) -> Vec<u8> {
            let mut s = b"SIG:".to_vec();
            s.extend_from_slice(payload);
            s
        }
    }
    struct FakeVerifier {
        cert: Vec<u8>,
        cn: String,
    }
    impl GossipVerify for FakeVerifier {
        fn verify(&self, cert_der: &[u8], payload: &[u8], sig: &[u8]) -> Option<String> {
            let expected: Vec<u8> = [b"SIG:".as_ref(), payload].concat();
            (cert_der == self.cert && sig == expected).then(|| self.cn.clone())
        }
    }

    /// A signing auth (v2) keyed `byte`, claiming CN `cn`, optionally requiring signatures.
    fn signing_auth(byte: u8, cn: &str, require: bool) -> SwimAuth {
        let cert = format!("cert-of-{cn}").into_bytes();
        SwimAuth::new(&[byte; KEY_LEN]).with_signing(
            Arc::new(FakeSigner { cert: cert.clone() }),
            Arc::new(FakeVerifier {
                cert,
                cn: cn.to_string(),
            }),
            require,
        )
    }

    #[test]
    fn seal_then_open_roundtrips() {
        let a = auth(7);
        let sealed = a.seal(b"membership update");
        let opened = a.open(&sealed).expect("opens");
        assert_eq!(opened.payload, b"membership update");
        assert_eq!(opened.identity, None); // v1: no authenticated identity
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
        assert_eq!(a.open(&sealed).expect("opens").payload, b"");
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
        // radix or formatting (the two booleans are configuration, not secret).
        assert_eq!(
            format!("{:?}", auth(0x42)),
            "SwimAuth { signed: false, require_signed: false, .. }"
        );
    }

    // --- ADR 0022: signed (v2) datagrams ---

    #[test]
    fn signed_seal_open_roundtrips_and_returns_the_identity() {
        let a = signing_auth(7, "node-a", false);
        let sealed = a.seal(b"membership update");
        assert_eq!(sealed[0], VERSION_V2);
        let opened = a.open(&sealed).expect("opens");
        assert_eq!(opened.payload, b"membership update");
        assert_eq!(opened.identity.as_deref(), Some("node-a"));
    }

    /// Pin the v2 framing: after the version byte and tag, the body parses to the embedded
    /// certificate, signature, and payload in that order.
    #[test]
    fn v2_body_framing_is_pinned() {
        let a = signing_auth(7, "node-a", false);
        let sealed = a.seal(b"PAYLOAD");
        assert_eq!(sealed[0], VERSION_V2);
        let body = &sealed[1 + TAG_LEN..];
        let (cert, sig, payload) = parse_v2(body).expect("v2 body parses");
        assert_eq!(cert, b"cert-of-node-a");
        assert_eq!(sig, b"SIG:PAYLOAD");
        assert_eq!(payload, b"PAYLOAD");
    }

    #[test]
    fn require_signed_rejects_an_unsigned_v1_datagram() {
        // Same key, but the v1 sender does not sign.
        let v1 = auth(7).seal(b"unsigned");
        let strict = signing_auth(7, "node-a", true);
        assert!(strict.open(&v1).is_none(), "strict mode must reject v1");
    }

    #[test]
    fn prefer_mode_still_accepts_a_v1_datagram_during_rollout() {
        let v1 = auth(7).seal(b"unsigned");
        let lenient = signing_auth(7, "node-a", false);
        let opened = lenient.open(&v1).expect("prefer mode accepts v1");
        assert_eq!(opened.payload, b"unsigned");
        assert_eq!(opened.identity, None);
    }

    #[test]
    fn a_signature_that_fails_verification_is_rejected() {
        // Seal as node-a, but open with a verifier expecting node-b's cert: verify() → None.
        let sender = signing_auth(7, "node-a", false);
        let sealed = sender.seal(b"msg");
        let receiver = signing_auth(7, "node-b", true); // expects cert-of-node-b
        assert!(receiver.open(&sealed).is_none());
    }

    #[test]
    fn tampering_any_v2_byte_is_rejected_by_the_hmac() {
        let a = signing_auth(7, "node-a", false);
        let sealed = a.seal(b"payload");
        for i in 0..sealed.len() {
            let mut t = sealed.clone();
            t[i] ^= 0x01;
            assert!(a.open(&t).is_none(), "v2 bit flip at {i} accepted");
        }
    }

    #[test]
    fn an_off_mode_node_interoperates_with_a_signed_datagram() {
        // A node with no verifier (ADR 0003 behaviour) still accepts a v2 datagram from a
        // signing peer on the same key — it just cannot authenticate the identity.
        let signed = signing_auth(7, "node-a", false).seal(b"hello");
        let off = auth(7);
        let opened = off.open(&signed).expect("off mode opens v2 via the HMAC");
        assert_eq!(opened.payload, b"hello");
        assert_eq!(opened.identity, None);
    }
}
