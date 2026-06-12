//! mTLS identity extraction (ADR 0004): the broker identity of a
//! certificate-authenticated client is its leaf certificate's Subject Common
//! Name. The TLS layer has already verified the chain; this module only maps
//! the verified certificate to an [`Identity`].

use crate::{AuthError, Identity};

/// Extract the broker [`Identity`] (Subject CN) from a DER-encoded, already
/// chain-verified X.509 leaf certificate.
///
/// # Errors
/// [`AuthError::Rejected`] if the certificate cannot be parsed or carries no
/// non-empty Common Name.
pub fn identity_from_cert(der: &[u8]) -> Result<Identity, AuthError> {
    let (rest, cert) = x509_parser::parse_x509_certificate(der).map_err(|_| AuthError::Rejected)?;
    if !rest.is_empty() {
        // Trailing garbage after the certificate: not a clean DER encoding.
        return Err(AuthError::Rejected);
    }
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .ok_or(AuthError::Rejected)?
        .as_str()
        .map_err(|_| AuthError::Rejected)?;
    // Reject empty CNs and any carrying topic metacharacters: such a subject
    // is unusable for routing and, substituted into a `%i` ACL pattern, could
    // broaden a grant across namespaces (ADR 0004). Fail closed at the door.
    if cn.is_empty() || cn.contains(['/', '+', '#']) {
        return Err(AuthError::Rejected);
    }
    Ok(Identity {
        subject: cn.to_string(),
        groups: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mint a self-signed certificate whose distinguished name carries exactly
    /// the given attributes, returning its DER encoding.
    fn cert_der(attributes: &[(rcgen::DnType, &str)]) -> Vec<u8> {
        let key = rcgen::KeyPair::generate().expect("key generation");
        let mut params = rcgen::CertificateParams::default();
        // `CertificateParams::default()` pre-populates CN "rcgen self signed
        // cert"; start from an empty DN so tests control it exactly.
        params.distinguished_name = rcgen::DistinguishedName::new();
        for (dn_type, value) in attributes {
            params
                .distinguished_name
                .push(dn_type.clone(), (*value).to_string());
        }
        let cert = params.self_signed(&key).expect("self-sign");
        cert.der().to_vec()
    }

    #[test]
    fn extracts_common_name_as_subject() {
        let der = cert_der(&[(rcgen::DnType::CommonName, "device-42")]);
        let identity = identity_from_cert(&der).expect("CN present");
        assert_eq!(
            identity,
            Identity {
                subject: "device-42".to_string(),
                groups: vec![],
            }
        );
    }

    #[test]
    fn non_ascii_utf8_common_name_survives_intact() {
        let der = cert_der(&[(rcgen::DnType::CommonName, "gerät-7")]);
        let identity = identity_from_cert(&der).expect("CN present");
        assert_eq!(identity.subject, "gerät-7");
        assert!(identity.groups.is_empty());
    }

    #[test]
    fn missing_common_name_is_rejected() {
        let der = cert_der(&[(rcgen::DnType::OrganizationName, "Acme")]);
        assert!(matches!(identity_from_cert(&der), Err(AuthError::Rejected)));
    }

    #[test]
    fn empty_common_name_is_rejected() {
        let der = cert_der(&[(rcgen::DnType::CommonName, "")]);
        assert!(matches!(identity_from_cert(&der), Err(AuthError::Rejected)));
    }

    /// A CN carrying topic metacharacters is unusable as a broker identity and
    /// a substitution hazard (ADR 0004): reject it at the door so it can never
    /// reach the ACL engine. The CA controls CNs, but a sloppy or multi-tenant
    /// CA must not be able to mint a wildcard-injecting identity.
    #[test]
    fn common_name_with_topic_metacharacters_is_rejected() {
        for cn in ["+", "#", "a/b", "dev/+/x", "has#hash", "trailing/"] {
            let der = cert_der(&[(rcgen::DnType::CommonName, cn)]);
            assert!(
                matches!(identity_from_cert(&der), Err(AuthError::Rejected)),
                "CN {cn:?} should be rejected as an unsafe identity"
            );
        }
        // A normal CN with other punctuation is still fine.
        let der = cert_der(&[(rcgen::DnType::CommonName, "device-7.eu.example")]);
        assert!(identity_from_cert(&der).is_ok());
    }

    #[test]
    fn garbage_bytes_are_rejected_without_panic() {
        assert!(matches!(
            identity_from_cert(b"not a certificate"),
            Err(AuthError::Rejected)
        ));
    }

    #[test]
    fn truncated_der_is_rejected_without_panic() {
        let der = cert_der(&[(rcgen::DnType::CommonName, "device-42")]);
        let truncated = &der[..der.len() / 2];
        assert!(matches!(
            identity_from_cert(truncated),
            Err(AuthError::Rejected)
        ));
    }
}
