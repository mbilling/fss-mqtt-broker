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

use std::collections::BTreeSet;

use ring::rand::SystemRandom;
use ring::signature::{
    self, EcdsaKeyPair, Ed25519KeyPair, UnparsedPublicKey, VerificationAlgorithm,
};
use x509_parser::oid_registry::{OID_KEY_TYPE_EC_PUBLIC_KEY, OID_SIG_ED25519};
use x509_parser::prelude::{
    ASN1Time, CertificateRevocationList, FromDer, GeneralName, X509Certificate,
};

/// The SAN URI prefix that carries a CA-attested failure-domain label (ADR 0016 T6):
/// a leaf certificate with `URI:urn:fss:failure-domain:<label>` among its Subject
/// Alternative Names asserts — with the CA's authority — that its holder lives in
/// failure domain `<label>`.
pub const FAILURE_DOMAIN_URN_PREFIX: &str = "urn:fss:failure-domain:";

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
    /// The leaf certificate is outside its validity window (expired or not yet valid).
    #[error("certificate is outside its validity window")]
    Expired,
    /// The leaf certificate's serial is listed on the cluster CRL (ADR 0022 T7).
    #[error("certificate is revoked")]
    Revoked,
    /// The leaf certificate's key type is not one we verify.
    #[error("unsupported certificate key type")]
    UnsupportedKey,
    /// The signature does not verify under the leaf certificate's public key.
    #[error("signature is invalid")]
    Signature,
    /// The leaf certificate carries no usable Common Name to bind an identity to.
    #[error("certificate has no usable Common Name")]
    NoCommonName,
    /// The certificate carries a failure-domain URN with an empty label — a malformed
    /// attestation is rejected, not silently ignored (deny-by-default).
    #[error("certificate carries a malformed failure-domain attestation")]
    BadFailureDomain,
}

/// The cluster CRL could not be loaded.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CrlError {
    /// The CRL (or CA certificate) does not parse as DER.
    #[error("CRL does not parse")]
    Parse,
    /// The CRL is not signed by the cluster CA — an unauthenticated revocation list
    /// could revoke (deny service to) arbitrary healthy nodes, so it is rejected.
    #[error("CRL is not signed by the cluster CA")]
    Chain,
}

/// The revoked-serial set from a cluster-CA-signed CRL, checked on every inbound signed
/// gossip datagram (ADR 0022 T7). Built once per load/reload; lookups are by the
/// certificate serial's canonical big-endian bytes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RevocationList {
    serials: BTreeSet<Vec<u8>>,
}

impl RevocationList {
    /// Parse a DER CRL, verify it is signed by the cluster CA (`ca_der`), and extract
    /// the revoked serial numbers.
    ///
    /// # Errors
    /// [`CrlError::Parse`] if the CRL or CA does not parse; [`CrlError::Chain`] if the
    /// CRL's signature does not verify under the CA key.
    pub fn from_der(crl_der: &[u8], ca_der: &[u8]) -> Result<Self, CrlError> {
        let (_, crl) = CertificateRevocationList::from_der(crl_der).map_err(|_| CrlError::Parse)?;
        let (_, ca) = X509Certificate::from_der(ca_der).map_err(|_| CrlError::Parse)?;
        crl.verify_signature(ca.public_key())
            .map_err(|_| CrlError::Chain)?;
        let serials = crl
            .iter_revoked_certificates()
            .map(|r| r.user_certificate.to_bytes_be())
            .collect();
        Ok(Self { serials })
    }

    /// Whether the given big-endian serial bytes are revoked.
    #[must_use]
    pub fn contains(&self, serial_be: &[u8]) -> bool {
        self.serials.contains(serial_be)
    }

    /// How many serials the list revokes (for load-time logging).
    #[must_use]
    pub fn len(&self) -> usize {
        self.serials.len()
    }

    /// Whether the list revokes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.serials.is_empty()
    }
}

/// A successfully verified signed-gossip datagram: the authenticated sender identity and,
/// when the certificate carries one, the CA-attested failure-domain label (ADR 0016 T6).
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedGossip {
    /// The leaf certificate's Common Name — the authenticated node identity.
    pub cn: String,
    /// The failure-domain label the cluster CA attested by embedding
    /// `urn:fss:failure-domain:<label>` in the certificate's SANs, if present.
    pub failure_domain: Option<String>,
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

/// Verify a signed gossip datagram: the `cert_der` leaf must chain to `ca_der`, be inside
/// its validity window at `now_unix`, not be revoked by `crl` (when one is loaded), and
/// `sig` must be a valid signature over `msg` under the leaf's public key. On success,
/// returns the leaf's Common Name — the authenticated node identity the caller binds to
/// `from` — plus the CA-attested failure-domain label when the certificate carries one
/// (ADR 0016 T6).
///
/// `now_unix` is the caller's clock (epoch seconds), injected so validity is testable.
///
/// # Errors
/// A [`VerifyError`] variant for each failure mode (parse, chain, expiry, revocation,
/// key type, signature, CN, malformed domain attestation).
pub fn verify(
    ca_der: &[u8],
    cert_der: &[u8],
    msg: &[u8],
    sig: &[u8],
    now_unix: i64,
    crl: Option<&RevocationList>,
) -> Result<VerifiedGossip, VerifyError> {
    let (_, leaf) = X509Certificate::from_der(cert_der).map_err(|_| VerifyError::Parse)?;
    let (_, ca) = X509Certificate::from_der(ca_der).map_err(|_| VerifyError::Parse)?;

    // 1. The leaf is signed by the cluster CA (chain of one — peer certs are CA-issued).
    leaf.verify_signature(Some(ca.public_key()))
        .map_err(|_| VerifyError::Chain)?;

    // 2. The leaf is inside its validity window (ADR 0022 T7). An unrepresentable clock
    //    fails closed as Expired rather than skipping the check.
    let now = ASN1Time::from_timestamp(now_unix).map_err(|_| VerifyError::Expired)?;
    if !leaf.validity().is_valid_at(now) {
        return Err(VerifyError::Expired);
    }

    // 3. The leaf is not revoked (ADR 0022 T7). The CRL was chain-verified at load.
    if let Some(crl) = crl {
        if crl.contains(&leaf.tbs_certificate.serial.to_bytes_be()) {
            return Err(VerifyError::Revoked);
        }
    }

    // 4. The gossip signature verifies under the leaf's public key. The ring algorithm is
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

    // 5. The authenticated identity is the leaf's Common Name.
    let cn = leaf
        .subject()
        .iter_common_name()
        .next()
        .and_then(|a| a.as_str().ok())
        .ok_or(VerifyError::NoCommonName)?;
    if cn.is_empty() {
        return Err(VerifyError::NoCommonName);
    }

    // 6. The CA-attested failure-domain label, when present (ADR 0016 T6): the first SAN
    //    URI with the failure-domain URN prefix. A present-but-empty label is malformed
    //    and rejected rather than ignored.
    let failure_domain = failure_domain_of(&leaf)?;

    Ok(VerifiedGossip {
        cn: cn.to_string(),
        failure_domain,
    })
}

/// Extract the CA-attested failure-domain label from a leaf's SAN URIs, if any.
fn failure_domain_of(leaf: &X509Certificate<'_>) -> Result<Option<String>, VerifyError> {
    let Ok(Some(san)) = leaf.subject_alternative_name() else {
        return Ok(None);
    };
    for name in &san.value.general_names {
        if let GeneralName::URI(uri) = name {
            if let Some(label) = uri.strip_prefix(FAILURE_DOMAIN_URN_PREFIX) {
                if label.is_empty() {
                    return Err(VerifyError::BadFailureDomain);
                }
                return Ok(Some(label.to_string()));
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::{
        verify, CrlError, GossipSigner, RevocationList, VerifiedGossip, VerifyError,
        FAILURE_DOMAIN_URN_PREFIX,
    };

    /// A fixed "now" (2025-06-18T…Z) inside the default rcgen validity window
    /// (1975-01-01 .. 4096-01-01), so tests are deterministic.
    const NOW: i64 = 1_750_000_000;

    struct Ca {
        cert: rcgen::Certificate,
        key: rcgen::KeyPair,
    }

    fn new_ca() -> Ca {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let cert = params.self_signed(&key).unwrap();
        Ca { cert, key }
    }

    /// Mint a CA-signed leaf from prepared `params`, returning its DER and a signer.
    fn leaf_from(
        ca: &Ca,
        mut params: rcgen::CertificateParams,
        cn: &str,
        alg: &'static rcgen::SignatureAlgorithm,
    ) -> (Vec<u8>, GossipSigner) {
        let key = rcgen::KeyPair::generate_for(alg).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        let cert = params.signed_by(&key, &ca.cert, &ca.key).unwrap();
        let signer = GossipSigner::from_pkcs8_der(&key.serialize_der()).unwrap();
        (cert.der().to_vec(), signer)
    }

    /// Mint a CA-signed leaf with Common Name `cn`, using `alg` for the leaf key.
    fn leaf(ca: &Ca, cn: &str, alg: &'static rcgen::SignatureAlgorithm) -> (Vec<u8>, GossipSigner) {
        leaf_from(
            ca,
            rcgen::CertificateParams::new(Vec::new()).unwrap(),
            cn,
            alg,
        )
    }

    /// A verified identity with no attested domain.
    fn plain(cn: &str) -> VerifiedGossip {
        VerifiedGossip {
            cn: cn.to_string(),
            failure_domain: None,
        }
    }

    #[test]
    fn ecdsa_p256_sign_then_verify_returns_the_cn() {
        let ca = new_ca();
        let (cert, signer) = leaf(&ca, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"membership update");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"membership update", &sig, NOW, None),
            Ok(plain("node-a"))
        );
    }

    #[test]
    fn ed25519_sign_then_verify_roundtrips() {
        let ca = new_ca();
        let (cert, signer) = leaf(&ca, "node-ed", &rcgen::PKCS_ED25519);
        let sig = signer.sign(b"payload");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"payload", &sig, NOW, None),
            Ok(plain("node-ed"))
        );
    }

    #[test]
    fn a_tampered_payload_fails_the_signature() {
        let ca = new_ca();
        let (cert, signer) = leaf(&ca, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"original");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"tampered", &sig, NOW, None),
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
            verify(other_ca.cert.der(), &cert, b"msg", &sig, NOW, None),
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
            verify(ca.cert.der(), &cert_a, b"msg", &sig_b, NOW, None),
            Err(VerifyError::Signature)
        );
    }

    #[test]
    fn garbage_certificate_bytes_do_not_panic() {
        let ca = new_ca();
        assert_eq!(
            verify(
                ca.cert.der(),
                b"not a certificate",
                b"msg",
                b"sig",
                NOW,
                None
            ),
            Err(VerifyError::Parse)
        );
    }

    #[test]
    fn an_unsupported_signing_key_is_rejected() {
        // 32 zero bytes is not a valid PKCS#8 key of any supported type.
        assert!(GossipSigner::from_pkcs8_der(&[0u8; 32]).is_err());
    }

    // --- validity window (ADR 0022 T7) ---

    #[test]
    fn an_expired_certificate_is_rejected() {
        let ca = new_ca();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.not_before = rcgen::date_time_ymd(2020, 1, 1);
        params.not_after = rcgen::date_time_ymd(2021, 1, 1);
        let (cert, signer) = leaf_from(&ca, params, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"msg");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"msg", &sig, NOW, None),
            Err(VerifyError::Expired)
        );
    }

    #[test]
    fn a_not_yet_valid_certificate_is_rejected() {
        let ca = new_ca();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.not_before = rcgen::date_time_ymd(4000, 1, 1);
        params.not_after = rcgen::date_time_ymd(4001, 1, 1);
        let (cert, signer) = leaf_from(&ca, params, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"msg");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"msg", &sig, NOW, None),
            Err(VerifyError::Expired)
        );
    }

    // --- revocation (ADR 0022 T7) ---

    /// Mint a CA-signed CRL revoking the given serials.
    fn crl_der(ca: &Ca, serials: &[u64]) -> Vec<u8> {
        let params = rcgen::CertificateRevocationListParams {
            this_update: rcgen::date_time_ymd(2025, 1, 1),
            next_update: rcgen::date_time_ymd(2035, 1, 1),
            crl_number: rcgen::SerialNumber::from(1u64),
            issuing_distribution_point: None,
            revoked_certs: serials
                .iter()
                .map(|s| rcgen::RevokedCertParams {
                    serial_number: rcgen::SerialNumber::from(*s),
                    revocation_time: rcgen::date_time_ymd(2025, 1, 1),
                    reason_code: Some(rcgen::RevocationReason::KeyCompromise),
                    invalidity_date: None,
                })
                .collect(),
            key_identifier_method: rcgen::KeyIdMethod::Sha256,
        };
        params.signed_by(&ca.cert, &ca.key).unwrap().der().to_vec()
    }

    #[test]
    fn a_revoked_certificate_is_rejected() {
        let ca = new_ca();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.serial_number = Some(rcgen::SerialNumber::from(7u64));
        let (cert, signer) = leaf_from(&ca, params, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"msg");
        let crl = RevocationList::from_der(&crl_der(&ca, &[7]), ca.cert.der()).unwrap();
        assert_eq!(
            verify(ca.cert.der(), &cert, b"msg", &sig, NOW, Some(&crl)),
            Err(VerifyError::Revoked)
        );
    }

    #[test]
    fn an_unlisted_certificate_passes_with_a_crl_loaded() {
        let ca = new_ca();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.serial_number = Some(rcgen::SerialNumber::from(8u64));
        let (cert, signer) = leaf_from(&ca, params, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"msg");
        let crl = RevocationList::from_der(&crl_der(&ca, &[7]), ca.cert.der()).unwrap();
        assert_eq!(
            verify(ca.cert.der(), &cert, b"msg", &sig, NOW, Some(&crl)),
            Ok(plain("node-a"))
        );
    }

    #[test]
    fn a_crl_not_signed_by_the_cluster_ca_is_rejected_at_load() {
        let ca = new_ca();
        let other_ca = new_ca();
        assert_eq!(
            RevocationList::from_der(&crl_der(&other_ca, &[7]), ca.cert.der()),
            Err(CrlError::Chain)
        );
    }

    #[test]
    fn garbage_crl_bytes_do_not_panic() {
        let ca = new_ca();
        assert_eq!(
            RevocationList::from_der(b"not a crl", ca.cert.der()),
            Err(CrlError::Parse)
        );
    }

    // --- CA-attested failure domain (ADR 0016 T6) ---

    #[test]
    fn a_failure_domain_urn_in_the_san_is_returned() {
        let ca = new_ca();
        let mut params = rcgen::CertificateParams::new(vec!["node-a.cluster".into()]).unwrap();
        params.subject_alt_names.push(rcgen::SanType::URI(
            format!("{FAILURE_DOMAIN_URN_PREFIX}rack-a")
                .try_into()
                .unwrap(),
        ));
        let (cert, signer) = leaf_from(&ca, params, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"msg");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"msg", &sig, NOW, None),
            Ok(VerifiedGossip {
                cn: "node-a".to_string(),
                failure_domain: Some("rack-a".to_string()),
            })
        );
    }

    #[test]
    fn a_cert_without_the_urn_attests_no_domain() {
        let ca = new_ca();
        // A SAN with DNS names and an unrelated URI must not be read as a domain label.
        let mut params = rcgen::CertificateParams::new(vec!["node-a.cluster".into()]).unwrap();
        params
            .subject_alt_names
            .push(rcgen::SanType::URI("urn:example:other".try_into().unwrap()));
        let (cert, signer) = leaf_from(&ca, params, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"msg");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"msg", &sig, NOW, None),
            Ok(plain("node-a"))
        );
    }

    #[test]
    fn an_empty_failure_domain_label_is_rejected() {
        let ca = new_ca();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.subject_alt_names.push(rcgen::SanType::URI(
            FAILURE_DOMAIN_URN_PREFIX.try_into().unwrap(),
        ));
        let (cert, signer) = leaf_from(&ca, params, "node-a", &rcgen::PKCS_ECDSA_P256_SHA256);
        let sig = signer.sign(b"msg");
        assert_eq!(
            verify(ca.cert.der(), &cert, b"msg", &sig, NOW, None),
            Err(VerifyError::BadFailureDomain)
        );
    }
}
