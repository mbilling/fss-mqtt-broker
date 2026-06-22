//! Per-node signatures for SWIM gossip ([ADR 0022](../../../docs/adr/0022-signed-gossip.md)).
//!
//! A node signs its gossip payloads with its cluster-bus leaf key; a receiver verifies a
//! datagram by checking the inline leaf certificate **chains to the cluster CA** and that
//! the signature is valid, then reads the certificate's Common Name. The driver binds that
//! authenticated CN to the SWIM `from` id, so a datagram cannot impersonate another node.
//!
//! This reuses the cluster PKI (ADR 0002/0004) and the crypto already in the tree: `ring`
//! for signing/verification and `x509-parser` (with its `verify` feature) for the chain
//! check and Common-Name extraction — no new dependency. Supported leaf key types are
//! **ECDSA P-256, ECDSA P-384, and Ed25519**; anything else fails closed.
//!
//! Inputs are DER (the PEM I/O lives in the broker, which already loads these files for
//! mTLS), so this module stays free of file/PEM handling.

use ring::rand::SystemRandom;
use ring::signature::{
    self, EcdsaKeyPair, Ed25519KeyPair, UnparsedPublicKey, VerificationAlgorithm,
};
use x509_parser::oid_registry::{OID_KEY_TYPE_EC_PUBLIC_KEY, OID_SIG_ED25519};
use x509_parser::prelude::{FromDer, X509Certificate};

/// The signing key could not be loaded as a supported PKCS#8 key.
#[derive(Debug, thiserror::Error)]
#[error("unsupported or unparseable gossip signing key (need PKCS#8 ECDSA P-256/P-384 or Ed25519)")]
pub struct SignerError;

/// A received datagram's certificate or signature failed verification.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    /// The certificate (or CA certificate) does not parse as DER X.509.
    #[error("certificate does not parse")]
    Parse,
    /// The leaf certificate is not signed by the cluster CA.
    #[error("certificate does not chain to the cluster CA")]
    Chain,
    /// The leaf certificate's key type is not one we verify.
    #[error("unsupported certificate key type")]
    UnsupportedKey,
    /// The signature does not verify under the leaf certificate's public key.
    #[error("signature is invalid")]
    Signature,
    /// The leaf certificate carries no usable Common Name to bind an identity to.
    #[error("certificate has no usable Common Name")]
    NoCommonName,
}

enum Key {
    Ecdsa(EcdsaKeyPair),
    Ed25519(Ed25519KeyPair),
}

/// Signs gossip payloads with a node's cluster-bus private key.
pub struct GossipSigner {
    key: Key,
    rng: SystemRandom,
}

impl std::fmt::Debug for GossipSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never expose key material.
        f.debug_struct("GossipSigner").finish_non_exhaustive()
    }
}

impl GossipSigner {
    /// Load a signer from a PKCS#8 DER private key, trying each supported algorithm.
    ///
    /// # Errors
    /// [`SignerError`] if the bytes are not a supported PKCS#8 key.
    pub fn from_pkcs8_der(der: &[u8]) -> Result<Self, SignerError> {
        let rng = SystemRandom::new();
        if let Ok(k) =
            EcdsaKeyPair::from_pkcs8(&signature::ECDSA_P256_SHA256_ASN1_SIGNING, der, &rng)
        {
            return Ok(Self {
                key: Key::Ecdsa(k),
                rng,
            });
        }
        if let Ok(k) =
            EcdsaKeyPair::from_pkcs8(&signature::ECDSA_P384_SHA384_ASN1_SIGNING, der, &rng)
        {
            return Ok(Self {
                key: Key::Ecdsa(k),
                rng,
            });
        }
        if let Ok(k) = Ed25519KeyPair::from_pkcs8(der) {
            return Ok(Self {
                key: Key::Ed25519(k),
                rng,
            });
        }
        Err(SignerError)
    }

    /// Sign `msg`, returning the signature bytes (ASN.1 DER for ECDSA, raw for Ed25519).
    ///
    /// # Panics
    /// Panics only if the system RNG fails, which `ring` treats as unrecoverable.
    #[must_use]
    pub fn sign(&self, msg: &[u8]) -> Vec<u8> {
        match &self.key {
            Key::Ecdsa(k) => k
                .sign(&self.rng, msg)
                .expect("ECDSA signing")
                .as_ref()
                .to_vec(),
            Key::Ed25519(k) => k.sign(msg).as_ref().to_vec(),
        }
    }
}

/// Verify a signed gossip datagram: the `cert_der` leaf must chain to `ca_der`, and `sig`
/// must be a valid signature over `msg` under the leaf's public key. On success, returns
/// the leaf's Common Name — the authenticated node identity the caller binds to `from`.
///
/// # Errors
/// A [`VerifyError`] variant for each failure mode (parse, chain, key type, signature, CN).
pub fn verify(
    ca_der: &[u8],
    cert_der: &[u8],
    msg: &[u8],
    sig: &[u8],
) -> Result<String, VerifyError> {
    let (_, leaf) = X509Certificate::from_der(cert_der).map_err(|_| VerifyError::Parse)?;
    let (_, ca) = X509Certificate::from_der(ca_der).map_err(|_| VerifyError::Parse)?;

    // 1. The leaf is signed by the cluster CA (chain of one — peer certs are CA-issued).
    leaf.verify_signature(Some(ca.public_key()))
        .map_err(|_| VerifyError::Chain)?;

    // 2. The gossip signature verifies under the leaf's public key. The ring algorithm is
    //    chosen from the SPKI: EC by point length (P-256 = 65 B, P-384 = 97 B), or Ed25519.
    let spki = leaf.public_key();
    let key_bytes: &[u8] = &spki.subject_public_key.data;
    let alg: &dyn VerificationAlgorithm = if spki.algorithm.algorithm == OID_KEY_TYPE_EC_PUBLIC_KEY
    {
        match key_bytes.len() {
            65 => &signature::ECDSA_P256_SHA256_ASN1,
            97 => &signature::ECDSA_P384_SHA384_ASN1,
            _ => return Err(VerifyError::UnsupportedKey),
        }
    } else if spki.algorithm.algorithm == OID_SIG_ED25519 {
        &signature::ED25519
    } else {
        return Err(VerifyError::UnsupportedKey);
    };
    UnparsedPublicKey::new(alg, key_bytes)
        .verify(msg, sig)
        .map_err(|_| VerifyError::Signature)?;

    // 3. The authenticated identity is the leaf's Common Name.
    let cn = leaf
        .subject()
        .iter_common_name()
        .next()
        .and_then(|a| a.as_str().ok())
        .ok_or(VerifyError::NoCommonName)?;
    if cn.is_empty() {
        return Err(VerifyError::NoCommonName);
    }
    Ok(cn.to_string())
}

#[cfg(test)]
mod tests {
    use super::{verify, GossipSigner, VerifyError};

    struct Ca {
        cert: rcgen::Certificate,
        key: rcgen::KeyPair,
    }

    fn new_ca() -> Ca {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).unwrap();
        Ca { cert, key }
    }

    /// Mint a CA-signed leaf with Common Name `cn`, using `alg` for the leaf key.
    /// Returns the leaf cert DER and a signer over its key.
    fn leaf(ca: &Ca, cn: &str, alg: &'static rcgen::SignatureAlgorithm) -> (Vec<u8>, GossipSigner) {
        let key = rcgen::KeyPair::generate_for(alg).unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        let cert = params.signed_by(&key, &ca.cert, &ca.key).unwrap();
        let signer = GossipSigner::from_pkcs8_der(&key.serialize_der()).unwrap();
        (cert.der().to_vec(), signer)
    }

    #[test]
    fn ecdsa_p256_sign_then_verify_returns_the_cn() {
        let ca = new_ca();
        let (cert, signer) = leaf(&ca, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"membership update");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"membership update", &sig),
            Ok("node-a".to_string())
        );
    }

    #[test]
    fn ed25519_sign_then_verify_roundtrips() {
        let ca = new_ca();
        let (cert, signer) = leaf(&ca, "node-ed", &rcgen::PKCS_ED25519);
        let sig = signer.sign(b"payload");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"payload", &sig),
            Ok("node-ed".to_string())
        );
    }

    #[test]
    fn a_tampered_payload_fails_the_signature() {
        let ca = new_ca();
        let (cert, signer) = leaf(&ca, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"original");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"tampered", &sig),
            Err(VerifyError::Signature)
        );
    }

    #[test]
    fn a_cert_not_chaining_to_the_ca_is_rejected() {
        let ca = new_ca();
        let other_ca = new_ca();
        let (cert, signer) = leaf(&ca, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"msg");
        // Present the real cert but verify against a different CA: chain check fails.
        assert_eq!(
            verify(other_ca.cert.der(), &cert, b"msg", &sig),
            Err(VerifyError::Chain)
        );
    }

    #[test]
    fn a_signature_from_another_key_is_rejected() {
        let ca = new_ca();
        let (cert_a, _signer_a) = leaf(&ca, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let (_cert_b, signer_b) = leaf(&ca, "node-b", &rcgen::PKCS_ECDSA_P256_SHA256);
        // node-b signs, but we present node-a's certificate: the signature does not match
        // node-a's key. (This is the forged-identity vector, caught at the signature.)
        let sig_b = signer_b.sign(b"msg");
        assert_eq!(
            verify(ca.cert.der(), &cert_a, b"msg", &sig_b),
            Err(VerifyError::Signature)
        );
    }

    #[test]
    fn garbage_certificate_bytes_do_not_panic() {
        let ca = new_ca();
        assert_eq!(
            verify(ca.cert.der(), b"not a certificate", b"msg", b"sig"),
            Err(VerifyError::Parse)
        );
    }

    #[test]
    fn an_unsupported_signing_key_is_rejected() {
        // 32 zero bytes is not a valid PKCS#8 key of any supported type.
        assert!(GossipSigner::from_pkcs8_der(&[0u8; 32]).is_err());
    }
}
