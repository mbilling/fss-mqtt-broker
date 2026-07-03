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

use ring::{digest, hmac};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// Shared-key-only datagram (ADR 0003): `[1][tag][payload]`.
const VERSION_V1: u8 = 1;
/// Signed datagram (ADR 0022): `[2][tag][cert_ref][sig_len][sig][payload]`, the tag
/// covering everything after it. The cert-ref is the full leaf certificate or its
/// SHA-256 fingerprint (0022-T6).
const VERSION_V2: u8 = 2;
/// Signed + sequenced datagram (ADR 0023): `[3][tag][seq(8)][cert_ref][sig_len][sig][payload]`,
/// the tag covering everything after it. Anti-replay builds on the v2 signature.
const VERSION_V3: u8 = 3;
/// HMAC-SHA256 tag length.
const TAG_LEN: usize = 32;
/// Required key length in bytes (64 hex characters).
pub const KEY_LEN: usize = 32;

/// Cert-ref marker: the full leaf certificate follows (`[len u16][cert]`), 0022-T6.
const CERT_REF_FULL: u8 = 1;
/// Cert-ref marker: a SHA-256 fingerprint of the leaf certificate follows (32 bytes).
const CERT_REF_FP: u8 = 2;
/// SHA-256 fingerprint length.
const FP_LEN: usize = 32;
/// Send the full certificate every Nth sealed datagram even between priming moments
/// (0022-T6): recovery bound for a receiver that lost the priming datagram (UDP) or
/// restarted its cache. Every other datagram carries only the 32-byte fingerprint,
/// keeping signed gossip under a typical 1500-byte MTU.
const FULL_CERT_EVERY: u64 = 16;
/// Bound on cached peer certificates: comfortably above any real cluster size, small
/// enough that a key-holding flooder cannot balloon memory. On overflow the cache is
/// cleared and re-primes from the periodic full-cert datagrams.
const CERT_CACHE_CAP: usize = 128;

/// SHA-256 fingerprint of a certificate's DER bytes (0022-T6).
fn fingerprint(cert_der: &[u8]) -> [u8; FP_LEN] {
    digest::digest(&digest::SHA256, cert_der)
        .as_ref()
        .try_into()
        .expect("SHA-256 yields 32 bytes")
}

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

/// Why an inbound datagram was rejected — a **bounded** reason set the driver feeds its
/// drop counter (ADR 0003-T6). `Auth` covers every parse/HMAC/chain/signature failure;
/// `Expired` and `Revoked` are distinct because they are the certificate-lifecycle drops
/// an operator acts on (renew / investigate) rather than noise (ADR 0022 T7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenReject {
    /// Malformed, wrong posture, bad HMAC, bad chain/signature, or unusable identity.
    Auth,
    /// The sender's certificate is outside its validity window.
    Expired,
    /// The sender's certificate is revoked by the cluster CRL.
    Revoked,
    /// A fingerprint-only datagram referenced a certificate this node has not cached yet
    /// (0022-T6). Recoverable, not adversarial: the sender's next priming (Join/Sync) or
    /// periodic full-cert datagram re-primes the cache and gossip converges.
    UnknownCert,
}

impl OpenReject {
    /// The bounded metric label for this rejection.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
            Self::UnknownCert => "cert-miss",
        }
    }
}

/// A verified sender identity: the certificate's Common Name and, when the certificate
/// carries one, the CA-attested failure-domain label (ADR 0016 T6).
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedIdentity {
    /// The authenticated node identity (the leaf certificate's Common Name).
    pub cn: String,
    /// The failure-domain label the cluster CA attested in the certificate, if any.
    pub failure_domain: Option<String>,
}

/// Verifies an inbound signed datagram: the inline certificate must chain to the cluster CA,
/// be within its validity window, not be revoked, and the signature must be valid over the
/// payload (ADR 0022/0016-T6/0022-T7). Returns the authenticated identity (plus any
/// CA-attested failure domain) on success, a bounded [`OpenReject`] to reject.
pub trait GossipVerify: Send + Sync {
    /// Verify `cert_der` chains to the cluster CA and `sig` is valid over `payload`.
    ///
    /// # Errors
    /// An [`OpenReject`] naming the bounded rejection class.
    fn verify(
        &self,
        cert_der: &[u8],
        payload: &[u8],
        sig: &[u8],
    ) -> Result<VerifiedIdentity, OpenReject>;
}

/// A successfully opened datagram: its payload and, when the datagram was signed and
/// verified, the authenticated sender identity for the driver to bind to the SWIM `from`.
#[derive(Debug, PartialEq, Eq)]
pub struct Opened<'a> {
    /// The serialized SWIM message.
    pub payload: &'a [u8],
    /// The authenticated node identity, when the datagram carried a verified signature.
    pub identity: Option<String>,
    /// The anti-replay sequence number, when the datagram was sequenced *and* its identity
    /// was authenticated (so the receiver can safely window it by sender — ADR 0023).
    pub seq: Option<u64>,
    /// The sender's CA-attested failure-domain label, when its verified certificate
    /// carries one (ADR 0016 T6) — authoritative over any self-claimed label.
    pub domain: Option<String>,
}

/// Seals and opens SWIM datagrams: always a cluster-shared-key HMAC (ADR 0003), and — when
/// a signer/verifier is configured — an additional per-node signature layer (ADR 0022) and an
/// anti-replay sequence (ADR 0023).
///
/// A node speaks **exactly one** wire posture, and accepts **only that posture** on the wire
/// (strict). The posture is a function of the configured layers:
///
/// - shared-key only (`signer` unset) → **v1** `[1][tag][payload]`;
/// - signed (`signer` set, `sequenced` false) → **v2** `[2][tag][cert][sig][payload]`;
/// - signed + sequenced (`signer` set, `sequenced` true) → **v3** `[3][tag][seq][cert][sig][payload]`.
///
/// There is no cross-posture acceptance: a uniformly-configured cluster never sends a node a
/// datagram of a different posture, so anything but its own format is rejected. (The
/// per-node rollout coexistence the wire versions originally carried was removed before any
/// release — see ADR 0022/0023.)
pub struct SwimAuth {
    /// Shared HMAC keys: `keys[0]` is the primary (used to seal), and any further keys are
    /// additional keys an incoming datagram may have been sealed with — the dual-key window
    /// that rotates the gossip key without downtime (ADR 0003).
    keys: Vec<hmac::Key>,
    /// When set, this node signs outgoing datagrams (v2/v3) and **requires** a verified
    /// signature on every incoming one — an unsigned (v1) datagram is rejected.
    signer: Option<Arc<dyn GossipSign>>,
    /// Verifies the inline certificate + signature on an incoming signed datagram.
    verifier: Option<Arc<dyn GossipVerify>>,
    /// When true (implies `signer` is set), this node is in the anti-replay posture: it
    /// sequences its outgoing datagrams (v3) and accepts **only** v3 (ADR 0023).
    sequenced: bool,
    /// This node's certificate fingerprint, precomputed at
    /// [`with_signing`](Self::with_signing) (0022-T6).
    own_fp: Option<[u8; FP_LEN]>,
    /// Outgoing-datagram counter driving the periodic full-cert refresh (0022-T6).
    sends: AtomicU64,
    /// Verified peer certificates by fingerprint (0022-T6). Only certificates from
    /// datagrams that passed the **full** verification (chain, validity, revocation,
    /// signature) are inserted, so a key-holder cannot stuff the cache with garbage.
    /// Fingerprinting is pure wire compression: every datagram — fingerprint-form
    /// included — still runs the full verification against the (cached) DER.
    cert_cache: Mutex<HashMap<[u8; FP_LEN], Vec<u8>>>,
}

impl std::fmt::Debug for SwimAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never expose key material, even via Debug.
        f.debug_struct("SwimAuth")
            .field("signed", &self.signer.is_some())
            .field("sequenced", &self.sequenced)
            .finish_non_exhaustive()
    }
}

impl SwimAuth {
    /// Create a shared-key-only context from raw key bytes (ADR 0003).
    #[must_use]
    pub fn new(key: &[u8; KEY_LEN]) -> Self {
        Self {
            keys: vec![hmac::Key::new(hmac::HMAC_SHA256, key)],
            signer: None,
            verifier: None,
            sequenced: false,
            own_fp: None,
            sends: AtomicU64::new(0),
            cert_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Also accept datagrams sealed with `key`, without using it to seal. This is the
    /// rotation window: stage the new key as an additional accepted key on every node
    /// first, then promote it to primary, then drop the old one — no node ever rejects a
    /// peer mid-rotation (ADR 0003). Outgoing datagrams always use the primary key.
    #[must_use]
    pub fn accept_also(mut self, key: &[u8; KEY_LEN]) -> Self {
        self.keys.push(hmac::Key::new(hmac::HMAC_SHA256, key));
        self
    }

    /// Like [`accept_also`](Self::accept_also), from a 64-hex-character key string.
    ///
    /// # Errors
    /// [`InvalidKey`] if the string is not exactly [`KEY_LEN`] bytes of hex.
    pub fn accept_also_hex(self, hex: &str) -> Result<Self, InvalidKey> {
        let bytes = decode_hex(hex.trim()).ok_or(InvalidKey("not valid hexadecimal"))?;
        let key: [u8; KEY_LEN] = bytes
            .try_into()
            .map_err(|_| InvalidKey("must be exactly 32 bytes (64 hex characters)"))?;
        Ok(self.accept_also(&key))
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

    /// Add the per-node signature layer (ADR 0022): sign outgoing datagrams (v2), and require a
    /// verified signature on every incoming one — an unsigned (v1) datagram is rejected.
    #[must_use]
    pub fn with_signing(
        mut self,
        signer: Arc<dyn GossipSign>,
        verifier: Arc<dyn GossipVerify>,
    ) -> Self {
        self.own_fp = Some(fingerprint(signer.cert_der()));
        self.signer = Some(signer);
        self.verifier = Some(verifier);
        self
    }

    /// Add the anti-replay layer (ADR 0023): sequence outgoing datagrams (v3) and require a
    /// fresh sequence on every incoming one (the receiver windows them) — anything that is not
    /// v3 is rejected. Outgoing v3 datagrams are produced by
    /// [`seal_sequenced`](Self::seal_sequenced); the driver supplies the monotonic sequence.
    /// Sequencing implies signing, so call after [`with_signing`](Self::with_signing).
    #[must_use]
    pub fn with_sequencing(mut self) -> Self {
        self.sequenced = true;
        self
    }

    /// Whether this node is in the signed + sequenced (v3) posture — i.e. it windows every
    /// accepted datagram. The driver consults this to decide whether to sequence its own
    /// outgoing datagrams.
    #[must_use]
    pub fn sequenced(&self) -> bool {
        self.sequenced
    }

    /// Wrap a serialized SWIM message for the wire. With a signer, produces a v2 (signed)
    /// datagram; otherwise the v1 shared-key-only datagram.
    ///
    /// `prime` forces the **full certificate** into the datagram (0022-T6). The driver
    /// sets it on first-contact/full-state message kinds (Join/Sync), so a peer is primed
    /// deterministically before it sees fingerprint-only datagrams; between priming
    /// moments the full cert still rides every [`FULL_CERT_EVERY`]th datagram as a
    /// loss-recovery bound, and everything else carries the 32-byte fingerprint —
    /// keeping signed gossip under a typical MTU. Ignored in the v1 posture.
    ///
    /// # Panics
    /// Panics if the leaf certificate or signature exceeds 64 KiB (a `u16` length field) —
    /// far beyond any real certificate, so this is a provisioning invariant, not input.
    #[must_use]
    pub fn seal(&self, payload: &[u8], prime: bool) -> Vec<u8> {
        let Some(signer) = &self.signer else {
            return self.frame(VERSION_V1, payload);
        };
        // v2 body: [cert_ref][sig_len][sig][payload], tag computed over the whole body.
        let sig = signer.sign(payload);
        let sig_len = u16::try_from(sig.len()).expect("signature fits u16");
        let mut body = self.cert_ref(signer.as_ref(), prime);
        body.extend_from_slice(&sig_len.to_be_bytes());
        body.extend_from_slice(&sig);
        body.extend_from_slice(payload);
        self.frame(VERSION_V2, &body)
    }

    /// Wrap a serialized SWIM message as a signed + sequenced v3 datagram (ADR 0023), the
    /// sequence supplied by the driver's monotonic allocator. `prime` as in
    /// [`seal`](Self::seal). Falls back to [`seal`](Self::seal) if no signer is configured
    /// (a misconfiguration — sequencing requires signing — that a sequenced receiver
    /// would reject anyway).
    ///
    /// # Panics
    /// Panics if the certificate or signature exceeds 64 KiB (a `u16` length field).
    #[must_use]
    pub fn seal_sequenced(&self, payload: &[u8], seq: u64, prime: bool) -> Vec<u8> {
        let Some(signer) = &self.signer else {
            return self.seal(payload, prime);
        };
        // v3 body: [seq(8)][cert_ref][sig_len][sig][payload].
        let sig = signer.sign(payload);
        let sig_len = u16::try_from(sig.len()).expect("signature fits u16");
        let mut body = Vec::with_capacity(8);
        body.extend_from_slice(&seq.to_be_bytes());
        body.extend_from_slice(&self.cert_ref(signer.as_ref(), prime));
        body.extend_from_slice(&sig_len.to_be_bytes());
        body.extend_from_slice(&sig);
        body.extend_from_slice(payload);
        self.frame(VERSION_V3, &body)
    }

    /// The cert-ref field for an outgoing signed datagram (0022-T6): the full leaf
    /// certificate when `prime` is set or on every [`FULL_CERT_EVERY`]th send, else the
    /// 32-byte SHA-256 fingerprint.
    fn cert_ref(&self, signer: &dyn GossipSign, prime: bool) -> Vec<u8> {
        let nth = self.sends.fetch_add(1, Ordering::Relaxed);
        if prime || nth % FULL_CERT_EVERY == 0 {
            let cert = signer.cert_der();
            let cert_len = u16::try_from(cert.len()).expect("leaf certificate fits u16");
            let mut out = Vec::with_capacity(3 + cert.len());
            out.push(CERT_REF_FULL);
            out.extend_from_slice(&cert_len.to_be_bytes());
            out.extend_from_slice(cert);
            out
        } else {
            let fp = self
                .own_fp
                .as_ref()
                .expect("signer set implies fingerprint");
            let mut out = Vec::with_capacity(1 + FP_LEN);
            out.push(CERT_REF_FP);
            out.extend_from_slice(fp);
            out
        }
    }

    /// `[version][HMAC(body)][body]`, sealed with the primary key.
    fn frame(&self, version: u8, body: &[u8]) -> Vec<u8> {
        let tag = hmac::sign(&self.keys[0], body);
        let mut out = Vec::with_capacity(1 + TAG_LEN + body.len());
        out.push(version);
        out.extend_from_slice(tag.as_ref());
        out.extend_from_slice(body);
        out
    }

    /// Verify a received datagram and return its payload (plus authenticated identity,
    /// sequence, and CA-attested domain for a signed datagram), or a bounded
    /// [`OpenReject`] if it is malformed or not **this node's own posture**. The shared-key
    /// HMAC is always required first; then the datagram's version must match the configured
    /// posture exactly — a v1 node accepts only v1, a signed node only v2, a sequenced node
    /// only v3 (no cross-posture acceptance; ADR 0022/0023).
    ///
    /// # Errors
    /// [`OpenReject::Auth`] for any malformed/foreign/unverifiable datagram;
    /// [`OpenReject::Expired`]/[`OpenReject::Revoked`] when the sender's certificate
    /// failed its lifecycle checks (ADR 0022 T7).
    pub fn open<'a>(&self, datagram: &'a [u8]) -> Result<Opened<'a>, OpenReject> {
        if datagram.len() < 1 + TAG_LEN {
            return Err(OpenReject::Auth);
        }
        let version = datagram[0];
        let (tag, body) = datagram[1..].split_at(TAG_LEN);
        // Shared-key gate + whole-datagram integrity (each verify constant-time), before any
        // parsing. Any key in the ring may have sealed it (the rotation window, ADR 0003).
        if !self.keys.iter().any(|k| hmac::verify(k, body, tag).is_ok()) {
            return Err(OpenReject::Auth);
        }

        match version {
            // Shared-key-only posture: accept v1 only.
            VERSION_V1 if self.signer.is_none() => Ok(Opened {
                payload: body,
                identity: None,
                seq: None,
                domain: None,
            }),
            // Signed posture: accept v2 only (verify the signature, bind the identity).
            VERSION_V2 if self.signer.is_some() && !self.sequenced => {
                let (cref, sig, payload) = parse_v2(body).ok_or(OpenReject::Auth)?;
                let v = self.verify_ref(&cref, payload, sig)?;
                Ok(Opened {
                    payload,
                    identity: Some(v.cn),
                    seq: None,
                    domain: v.failure_domain,
                })
            }
            // Signed + sequenced posture: accept v3 only (verify + carry the windowed sequence).
            VERSION_V3 if self.sequenced => {
                let (seq, cref, sig, payload) = parse_v3(body).ok_or(OpenReject::Auth)?;
                let v = self.verify_ref(&cref, payload, sig)?;
                Ok(Opened {
                    payload,
                    identity: Some(v.cn),
                    seq: Some(seq),
                    domain: v.failure_domain,
                })
            }
            // Any other (version, posture) pair is a foreign format — rejected.
            _ => Err(OpenReject::Auth),
        }
    }

    /// Resolve a cert-ref and run the **full** verification against it (0022-T6). A full
    /// certificate that verifies is cached under its fingerprint; a fingerprint resolves
    /// through the cache (miss ⇒ [`OpenReject::UnknownCert`], recoverable) and the cached
    /// DER is then verified exactly like an inline one — chain, validity window,
    /// revocation, and signature all run per datagram, so a CRL/expiry change applies to
    /// fingerprint-form datagrams immediately.
    fn verify_ref(
        &self,
        cref: &CertRef<'_>,
        payload: &[u8],
        sig: &[u8],
    ) -> Result<VerifiedIdentity, OpenReject> {
        let verifier = self.verifier.as_ref().ok_or(OpenReject::Auth)?;
        match cref {
            CertRef::Full(cert) => {
                let v = verifier.verify(cert, payload, sig)?;
                let mut cache = self
                    .cert_cache
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let fp = fingerprint(cert);
                if cache.len() >= CERT_CACHE_CAP && !cache.contains_key(&fp) {
                    // Overflow: clear and re-prime from periodic full-cert datagrams.
                    // Only fully-verified certs get here, so this is churn control, not
                    // a poisoning vector.
                    cache.clear();
                }
                cache.insert(fp, cert.to_vec());
                Ok(v)
            }
            CertRef::Fp(fp) => {
                let cert = {
                    let cache = self
                        .cert_cache
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    cache.get(fp).cloned()
                }
                .ok_or(OpenReject::UnknownCert)?;
                verifier.verify(&cert, payload, sig)
            }
        }
    }
}

/// How a signed datagram identifies its sender's certificate (0022-T6).
enum CertRef<'a> {
    /// The full leaf certificate DER, inline.
    Full(&'a [u8]),
    /// A SHA-256 fingerprint of a previously-seen certificate.
    Fp([u8; FP_LEN]),
}

/// Parse the leading cert-ref of a v2/v3 body: `[1][cert_len u16][cert]` (full) or
/// `[2][fp; 32]` (fingerprint). Returns the ref and the remaining bytes.
fn parse_cert_ref(body: &[u8]) -> Option<(CertRef<'_>, &[u8])> {
    match *body.first()? {
        CERT_REF_FULL => {
            let cert_len = u16::from_be_bytes(body.get(1..3)?.try_into().ok()?) as usize;
            let cert = body.get(3..3 + cert_len)?;
            Some((CertRef::Full(cert), body.get(3 + cert_len..)?))
        }
        CERT_REF_FP => {
            let fp: [u8; FP_LEN] = body.get(1..1 + FP_LEN)?.try_into().ok()?;
            Some((CertRef::Fp(fp), body.get(1 + FP_LEN..)?))
        }
        _ => None,
    }
}

/// Parse a v2 body `[cert_ref][sig_len u16][sig][payload]`. Any short/overrunning field
/// yields `None` (rejected). Lengths are bounds-checked against the remaining slice.
fn parse_v2(body: &[u8]) -> Option<(CertRef<'_>, &[u8], &[u8])> {
    let (cref, rest) = parse_cert_ref(body)?;
    let sig_len = u16::from_be_bytes(rest.get(0..2)?.try_into().ok()?) as usize;
    let rest = rest.get(2..)?;
    let sig = rest.get(..sig_len)?;
    let payload = rest.get(sig_len..)?;
    Some((cref, sig, payload))
}

/// Parse a v3 body `[seq u64][cert_ref][sig_len u16][sig][payload]`: the 8-byte
/// sequence prefix, then the v2 body. `None` on any short/overrunning field.
#[allow(clippy::type_complexity)] // (seq, cref, sig, payload) — a flat parse result, not nested
fn parse_v3(body: &[u8]) -> Option<(u64, CertRef<'_>, &[u8], &[u8])> {
    let seq = u64::from_be_bytes(body.get(0..8)?.try_into().ok()?);
    let (cref, sig, payload) = parse_v2(body.get(8..)?)?;
    Some((seq, cref, sig, payload))
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
    use super::{
        parse_v2, parse_v3, CertRef, GossipSign, GossipVerify, OpenReject, SwimAuth,
        VerifiedIdentity, KEY_LEN, TAG_LEN, VERSION_V2, VERSION_V3,
    };
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
        domain: Option<String>,
    }
    impl GossipVerify for FakeVerifier {
        fn verify(
            &self,
            cert_der: &[u8],
            payload: &[u8],
            sig: &[u8],
        ) -> Result<VerifiedIdentity, OpenReject> {
            let expected: Vec<u8> = [b"SIG:".as_ref(), payload].concat();
            if cert_der == self.cert && sig == expected {
                Ok(VerifiedIdentity {
                    cn: self.cn.clone(),
                    failure_domain: self.domain.clone(),
                })
            } else {
                Err(OpenReject::Auth)
            }
        }
    }

    /// A signing auth (v2 posture) keyed `byte`, claiming CN `cn`. Signed posture is now always
    /// strict (it rejects unsigned v1), so there is no longer a leniency parameter.
    fn signing_auth(byte: u8, cn: &str) -> SwimAuth {
        let cert = format!("cert-of-{cn}").into_bytes();
        SwimAuth::new(&[byte; KEY_LEN]).with_signing(
            Arc::new(FakeSigner { cert: cert.clone() }),
            Arc::new(FakeVerifier {
                cert,
                cn: cn.to_string(),
                domain: None,
            }),
        )
    }

    #[test]
    fn seal_then_open_roundtrips() {
        let a = auth(7);
        let sealed = a.seal(b"membership update", false);
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
        let sealed = a.seal(b"swim wire format v1", false);

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
        let sealed = a.seal(b"", false);
        assert_eq!(sealed.len(), 1 + TAG_LEN);
        assert_eq!(a.open(&sealed).expect("opens").payload, b"");
    }

    #[test]
    fn any_flipped_bit_is_rejected() {
        let a = auth(7);
        let sealed = a.seal(b"payload", false);
        // Version, every tag byte, and every payload byte.
        for i in 0..sealed.len() {
            let mut tampered = sealed.clone();
            tampered[i] ^= 0x01;
            assert!(a.open(&tampered).is_err(), "bit flip at {i} accepted");
        }
    }

    #[test]
    fn wrong_key_is_rejected() {
        let sealed = auth(7).seal(b"payload", false);
        assert!(auth(8).open(&sealed).is_err());
    }

    #[test]
    fn truncated_and_empty_datagrams_are_rejected() {
        let a = auth(7);
        let sealed = a.seal(b"payload", false);
        // Below the minimum length: rejected by the length guard.
        assert!(a.open(&[]).is_err());
        assert!(a.open(&sealed[..TAG_LEN]).is_err());
        // At or above the minimum length but with the payload cut: the length
        // guard passes, so rejection must come from the MAC itself.
        assert!(a.open(&sealed[..=TAG_LEN]).is_err()); // payload fully cut
        assert!(a.open(&sealed[..sealed.len() - 1]).is_err()); // short by one
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
            "SwimAuth { signed: false, sequenced: false, .. }"
        );
    }

    // --- ADR 0022: signed (v2) datagrams ---

    #[test]
    fn signed_seal_open_roundtrips_and_returns_the_identity() {
        let a = signing_auth(7, "node-a");
        let sealed = a.seal(b"membership update", false);
        assert_eq!(sealed[0], VERSION_V2);
        let opened = a.open(&sealed).expect("opens");
        assert_eq!(opened.payload, b"membership update");
        assert_eq!(opened.identity.as_deref(), Some("node-a"));
    }

    /// Pin the v2 framing: after the version byte and tag, the body parses to the embedded
    /// cert-ref (the first datagram carries the full certificate), signature, and payload
    /// in that order.
    #[test]
    fn v2_body_framing_is_pinned() {
        let a = signing_auth(7, "node-a");
        let sealed = a.seal(b"PAYLOAD", false);
        assert_eq!(sealed[0], VERSION_V2);
        let body = &sealed[1 + TAG_LEN..];
        assert_eq!(body[0], super::CERT_REF_FULL, "first datagram: full cert");
        let (cref, sig, payload) = parse_v2(body).expect("v2 body parses");
        let CertRef::Full(cert) = cref else {
            panic!("first datagram must carry the full certificate");
        };
        assert_eq!(cert, b"cert-of-node-a");
        assert_eq!(sig, b"SIG:PAYLOAD");
        assert_eq!(payload, b"PAYLOAD");
    }

    #[test]
    fn a_signed_node_rejects_an_unsigned_v1_datagram() {
        // The signed posture is strict: a v1 (shared-key-only) datagram on the same key is
        // rejected, not accepted — there is no rollout leniency.
        let v1 = auth(7).seal(b"unsigned", false);
        let signed = signing_auth(7, "node-a");
        assert!(signed.open(&v1).is_err(), "a signed node must reject v1");
    }

    #[test]
    fn a_signature_that_fails_verification_is_rejected() {
        // Seal as node-a, but open with a verifier expecting node-b's cert: verify() → None.
        let sender = signing_auth(7, "node-a");
        let sealed = sender.seal(b"msg", false);
        let receiver = signing_auth(7, "node-b"); // expects cert-of-node-b
        assert!(receiver.open(&sealed).is_err());
    }

    #[test]
    fn tampering_any_v2_byte_is_rejected_by_the_hmac() {
        let a = signing_auth(7, "node-a");
        let sealed = a.seal(b"payload", false);
        for i in 0..sealed.len() {
            let mut t = sealed.clone();
            t[i] ^= 0x01;
            assert!(a.open(&t).is_err(), "v2 bit flip at {i} accepted");
        }
    }

    #[test]
    fn a_shared_key_node_rejects_a_signed_datagram() {
        // Strict posture, the other direction: an off (v1) node rejects a v2 datagram even on
        // the same key — a uniform cluster never mixes postures, so a foreign format is dropped.
        let signed = signing_auth(7, "node-a").seal(b"hello", false);
        let off = auth(7);
        assert!(off.open(&signed).is_err(), "a v1 node must reject v2");
    }

    // --- ADR 0003: dual-key rotation window ---

    #[test]
    fn a_datagram_sealed_with_an_accepted_secondary_key_opens() {
        // Receiver: primary A, also accepts B (the old key being rotated out).
        let receiver = SwimAuth::new(&[0xAA; KEY_LEN]).accept_also(&[0xBB; KEY_LEN]);
        let sealed_with_b = SwimAuth::new(&[0xBB; KEY_LEN]).seal(b"rolling", false);
        assert_eq!(
            receiver.open(&sealed_with_b).expect("opens").payload,
            b"rolling"
        );
        // ...and the primary still opens during the window.
        let sealed_with_a = SwimAuth::new(&[0xAA; KEY_LEN]).seal(b"primary", false);
        assert_eq!(
            receiver.open(&sealed_with_a).expect("opens").payload,
            b"primary"
        );
    }

    #[test]
    fn a_key_outside_the_ring_is_rejected() {
        let receiver = SwimAuth::new(&[0xAA; KEY_LEN]).accept_also(&[0xBB; KEY_LEN]);
        let sealed_with_c = SwimAuth::new(&[0xCC; KEY_LEN]).seal(b"intruder", false);
        assert!(receiver.open(&sealed_with_c).is_err());
    }

    #[test]
    fn seal_always_uses_the_primary_key_not_a_secondary() {
        let node = SwimAuth::new(&[0xAA; KEY_LEN]).accept_also(&[0xBB; KEY_LEN]);
        let sealed = node.seal(b"x", false);
        // A peer holding only the secondary must not open it; the primary must.
        assert!(SwimAuth::new(&[0xBB; KEY_LEN]).open(&sealed).is_err());
        assert!(SwimAuth::new(&[0xAA; KEY_LEN]).open(&sealed).is_ok());
    }

    #[test]
    fn accept_also_hex_parses_and_accepts_the_key() {
        let receiver = SwimAuth::from_hex_key(&"ab".repeat(KEY_LEN))
            .unwrap()
            .accept_also_hex(&"cd".repeat(KEY_LEN))
            .unwrap();
        let sealed = SwimAuth::new(&[0xCD; KEY_LEN]).seal(b"hi", false);
        assert_eq!(receiver.open(&sealed).expect("opens").payload, b"hi");
        assert!(SwimAuth::from_hex_key(&"ab".repeat(KEY_LEN))
            .unwrap()
            .accept_also_hex("nothex")
            .is_err());
    }

    #[test]
    fn a_signed_v2_datagram_sealed_with_a_secondary_key_opens() {
        // The rotation window covers signed datagrams too: the HMAC tries the ring, then
        // the (key-independent) signature is verified.
        let sealed = signing_auth(0xBB, "node-a").seal(b"signed-roll", false);
        let receiver = signing_auth(0xAA, "node-a").accept_also(&[0xBB; KEY_LEN]);
        let opened = receiver.open(&sealed).expect("opens");
        assert_eq!(opened.payload, b"signed-roll");
        assert_eq!(opened.identity.as_deref(), Some("node-a"));
    }

    // --- ADR 0023: signed + sequenced (v3) datagrams ---

    #[test]
    fn sequenced_seal_open_roundtrips_with_seq_and_identity() {
        let a = signing_auth(7, "node-a").with_sequencing();
        let sealed = a.seal_sequenced(b"update", 42, false);
        assert_eq!(sealed[0], VERSION_V3);
        let opened = a.open(&sealed).expect("opens");
        assert_eq!(opened.payload, b"update");
        assert_eq!(opened.identity.as_deref(), Some("node-a"));
        assert_eq!(opened.seq, Some(42));
    }

    #[test]
    fn v3_body_framing_is_pinned() {
        let a = signing_auth(7, "node-a");
        let sealed = a.seal_sequenced(b"PAYLOAD", 7, false);
        assert_eq!(sealed[0], VERSION_V3);
        let body = &sealed[1 + TAG_LEN..];
        let (seq, cref, sig, payload) = parse_v3(body).expect("v3 body parses");
        assert_eq!(seq, 7);
        let CertRef::Full(cert) = cref else {
            panic!("first datagram must carry the full certificate");
        };
        assert_eq!(cert, b"cert-of-node-a");
        assert_eq!(sig, b"SIG:PAYLOAD");
        assert_eq!(payload, b"PAYLOAD");
    }

    #[test]
    fn a_sequenced_node_rejects_v1_and_v2_but_accepts_v3() {
        // The sequenced posture is strict: it accepts only v3, rejecting both a v1 and a v2
        // datagram on the same key.
        let sequenced = signing_auth(7, "node-a").with_sequencing();
        let v1 = auth(7).seal(b"x", false);
        let v2 = signing_auth(7, "node-a").seal(b"x", false);
        assert!(sequenced.open(&v1).is_err(), "a sequenced node rejects v1");
        assert!(sequenced.open(&v2).is_err(), "a sequenced node rejects v2");
        let v3 = signing_auth(7, "node-a")
            .with_sequencing()
            .seal_sequenced(b"x", 1, false);
        assert_eq!(sequenced.open(&v3).expect("opens").seq, Some(1));
    }

    #[test]
    fn tampering_any_v3_byte_is_rejected_by_the_hmac() {
        let a = signing_auth(7, "node-a").with_sequencing();
        let sealed = a.seal_sequenced(b"payload", 9, false);
        for i in 0..sealed.len() {
            let mut t = sealed.clone();
            t[i] ^= 0x01;
            assert!(a.open(&t).is_err(), "v3 bit flip at {i} accepted");
        }
    }

    #[test]
    fn a_non_sequenced_node_rejects_a_v3_datagram() {
        // Strict posture: a v2 (signed-only) node rejects a v3 datagram, and an off (v1) node
        // does too — only a sequenced node accepts v3.
        let v3 = signing_auth(7, "node-a")
            .with_sequencing()
            .seal_sequenced(b"hello", 5, false);
        assert!(
            signing_auth(7, "node-a").open(&v3).is_err(),
            "a signed-only node rejects v3"
        );
        assert!(auth(7).open(&v3).is_err(), "a shared-key node rejects v3");
    }

    // --- fingerprint cert caching (0022-T6) ---

    /// A signing auth over a realistically-sized (600-byte) stub certificate, so the
    /// size assertions reflect the real MTU win rather than the tiny default stub.
    fn fat_cert_auth(byte: u8) -> SwimAuth {
        let cert = vec![0xC5; 600];
        SwimAuth::new(&[byte; KEY_LEN]).with_signing(
            Arc::new(FakeSigner { cert: cert.clone() }),
            Arc::new(FakeVerifier {
                cert,
                cn: "node-a".to_string(),
                domain: None,
            }),
        )
    }

    /// After a full-cert datagram primes the receiver, a fingerprint-form datagram opens
    /// with the identity intact — and it is materially smaller (the MTU win).
    #[test]
    fn a_primed_receiver_opens_a_fingerprint_datagram() {
        let sender = fat_cert_auth(7);
        let receiver = fat_cert_auth(7); // fresh instance = unprimed cache
        let full = sender.seal(b"first", false); // send #0: full cert
        let fp = sender.seal(b"second", false); // send #1: fingerprint
        assert!(
            fp.len() + 500 < full.len(),
            "the fingerprint form must shed the ~600-byte certificate"
        );

        assert_eq!(
            receiver
                .open(&full)
                .expect("full form opens")
                .identity
                .as_deref(),
            Some("node-a")
        );
        let opened = receiver
            .open(&fp)
            .expect("fingerprint form opens once primed");
        assert_eq!(opened.identity.as_deref(), Some("node-a"));
        assert_eq!(opened.payload, b"second");
    }

    /// An unprimed receiver rejects a fingerprint-form datagram under the bounded,
    /// recoverable `cert-miss` reason — and recovers as soon as a full-cert datagram
    /// arrives.
    #[test]
    fn an_unprimed_receiver_misses_then_recovers() {
        let sender = signing_auth(7, "node-a");
        let _burn = sender.seal(b"0", false); // send #0: full (not delivered)
        let fp = sender.seal(b"1", false); // send #1: fingerprint
        let receiver = signing_auth(7, "node-a"); // fresh instance = unprimed cache
        let err = receiver
            .open(&fp)
            .expect_err("unprimed fingerprint must miss");
        assert_eq!(err, OpenReject::UnknownCert);
        assert_eq!(err.reason(), "cert-miss");

        // A priming datagram arrives (Join/Sync in the driver, `prime` here) — after
        // which the same fingerprint datagram opens.
        let priming = sender.seal(b"2", true);
        assert!(receiver.open(&priming).is_ok());
        assert!(receiver.open(&fp).is_ok(), "primed now; the fp form opens");
    }

    /// The full certificate rides every [`super::FULL_CERT_EVERY`]th datagram as the
    /// loss-recovery bound; everything between is fingerprint-form.
    #[test]
    fn the_full_cert_recurs_periodically() {
        let period = usize::try_from(super::FULL_CERT_EVERY).unwrap();
        let sender = signing_auth(7, "node-a");
        let mut kinds = Vec::new();
        for i in 0..=period {
            let sealed = sender.seal(format!("{i}").as_bytes(), false);
            kinds.push(sealed[1 + TAG_LEN]); // first body byte = cert-ref marker
        }
        assert_eq!(kinds[0], super::CERT_REF_FULL, "send #0 is full");
        for (i, k) in kinds.iter().enumerate().take(period).skip(1) {
            assert_eq!(*k, super::CERT_REF_FP, "send #{i} is fingerprint-form");
        }
        assert_eq!(
            kinds[period],
            super::CERT_REF_FULL,
            "the cycle restarts with a full cert"
        );
    }

    /// `prime` forces the full certificate regardless of the counter (the driver sets it
    /// on Join/Sync — the first-contact moments).
    #[test]
    fn priming_forces_the_full_certificate() {
        let sender = signing_auth(7, "node-a");
        let _burn = sender.seal(b"0", false); // counter now off the full slot
        let primed = sender.seal(b"1", true);
        assert_eq!(primed[1 + TAG_LEN], super::CERT_REF_FULL);
    }

    /// The fingerprint path still runs the FULL verification per datagram: a tampered
    /// fingerprint-form datagram is rejected, and the sequenced (v3) form carries the
    /// sequence through the fingerprint path too.
    #[test]
    fn the_fingerprint_path_verifies_and_sequences_like_the_full_path() {
        let sender = signing_auth(7, "node-a").with_sequencing();
        let receiver = signing_auth(7, "node-a").with_sequencing(); // fresh cache
        let full = sender.seal_sequenced(b"first", 1, false);
        let fp = sender.seal_sequenced(b"second", 2, false);
        assert!(receiver.open(&full).is_ok());
        let opened = receiver.open(&fp).expect("fp v3 opens");
        assert_eq!(opened.seq, Some(2));

        // Tamper with every byte of the fingerprint-form datagram: all rejected (HMAC).
        for i in 0..fp.len() {
            let mut t = fp.clone();
            t[i] ^= 0x01;
            assert!(receiver.open(&t).is_err(), "fp bit flip at {i} accepted");
        }
    }
}
