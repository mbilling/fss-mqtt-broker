//! mTLS identity extraction (ADR 0004): the broker identity of a
//! certificate-authenticated client is a field of its leaf certificate — the
//! Subject Common Name by default, or a Subject Alternative Name of an
//! operator-chosen kind (ADR 0004 T11). The TLS layer has already verified the
//! chain; this module only maps the verified certificate to an [`Identity`].
//!
//! Whichever source is selected, extraction is **fail-closed**: absent, empty,
//! unparseable, or *ambiguous* (present more than once) yields no identity at
//! all, never a guess. A certificate that yields no identity falls under the
//! anonymous policy, which denies by default.

use crate::{AuthError, Identity};
use x509_parser::extensions::GeneralName;

/// Which field of the verified leaf certificate is the broker identity (ADR 0004 T11).
///
/// The default, [`CommonName`](Self::CommonName), is the historical behaviour. The SAN
/// variants exist for PKIs that leave the CN empty or decorative — RFC 6125 has deprecated
/// CN-as-identity for a decade, and SPIFFE-style deployments put the workload identity in a
/// URI SAN.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IdentitySource {
    /// The Subject Distinguished Name's Common Name attribute (default).
    #[default]
    CommonName,
    /// A `dNSName` Subject Alternative Name.
    SanDns,
    /// A `uniformResourceIdentifier` SAN — e.g. a SPIFFE ID.
    SanUri,
    /// An `rfc822Name` (email) SAN.
    SanEmail,
}

impl IdentitySource {
    /// The accepted configuration spellings, for error messages.
    pub const SPELLINGS: &'static str = r#""cn", "san-dns", "san-uri", "san-email""#;

    /// Parse a configuration value. Deliberately exact — no aliases, no case folding
    /// beyond ASCII lowercase — so a typo is a startup error, not a silent fallback to
    /// a different identity model.
    ///
    /// # Errors
    /// The unrecognised value, for the caller to render into a config error.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "cn" => Ok(Self::CommonName),
            "san-dns" => Ok(Self::SanDns),
            "san-uri" => Ok(Self::SanUri),
            "san-email" => Ok(Self::SanEmail),
            other => Err(other.to_string()),
        }
    }

    /// The configuration spelling of this source (round-trips through [`parse`](Self::parse)).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CommonName => "cn",
            Self::SanDns => "san-dns",
            Self::SanUri => "san-uri",
            Self::SanEmail => "san-email",
        }
    }

    /// Whether a subject drawn from this source may legitimately contain `/`.
    ///
    /// Only URIs can: `spiffe://trust.example/ns/prod/sa/device` is one identity, not a
    /// topic path. Such a subject is still refused for `%i` substitution by the ACL engine
    /// (which fails closed on `/`), so it can never inject topic levels into a pattern — it
    /// is usable in explicit `identities` globs. A CN, DNS name, or email address never
    /// legitimately contains `/`, so one there is a malformed or hostile identity.
    fn allows_slash(self) -> bool {
        matches!(self, Self::SanUri)
    }
}

impl std::fmt::Display for IdentitySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Extract the broker [`Identity`] (Subject CN) from a DER-encoded, already
/// chain-verified X.509 leaf certificate.
///
/// The CN-sourced form, kept as the name the cluster bus uses: peer node-id binding
/// (ADR 0004 T7) is defined against the *Common Name* and is not operator-selectable.
/// Client listeners go through [`identity_from_cert_with`].
///
/// # Errors
/// [`AuthError::Rejected`] if the certificate cannot be parsed or carries no
/// single, usable Common Name.
pub fn identity_from_cert(der: &[u8]) -> Result<Identity, AuthError> {
    identity_from_cert_with(der, IdentitySource::CommonName)
}

/// Extract the broker [`Identity`] from a DER-encoded, already chain-verified X.509 leaf,
/// reading the field `source` selects (ADR 0004 T11).
///
/// # Errors
/// [`AuthError::Rejected`] if the certificate cannot be parsed, carries trailing DER
/// garbage, or does not carry **exactly one** usable value of the selected kind. Two
/// candidates are as fatal as none: picking one would let the certificate's issuer decide
/// which identity the broker sees by ordering, and a first-wins rule would make that
/// choice silently.
pub fn identity_from_cert_with(der: &[u8], source: IdentitySource) -> Result<Identity, AuthError> {
    let (rest, cert) = x509_parser::parse_x509_certificate(der).map_err(|_| AuthError::Rejected)?;
    if !rest.is_empty() {
        // Trailing garbage after the certificate: not a clean DER encoding.
        return Err(AuthError::Rejected);
    }

    let subject = match source {
        IdentitySource::CommonName => {
            let mut names = cert.subject().iter_common_name();
            let first = names.next().ok_or(AuthError::Rejected)?;
            if names.next().is_some() {
                // A multi-CN Distinguished Name is ambiguous; fail closed rather than
                // let attribute order pick the principal.
                return Err(AuthError::Rejected);
            }
            first.as_str().map_err(|_| AuthError::Rejected)?
        }
        IdentitySource::SanDns | IdentitySource::SanUri | IdentitySource::SanEmail => {
            // A *malformed* SAN extension is rejected outright (Err), as is a certificate
            // with no SAN at all when a SAN was asked for (Ok(None)).
            let san = cert
                .subject_alternative_name()
                .map_err(|_| AuthError::Rejected)?
                .ok_or(AuthError::Rejected)?;
            let mut matching = san.value.general_names.iter().filter_map(|gn| {
                match (source, gn) {
                    (IdentitySource::SanDns, GeneralName::DNSName(v))
                    | (IdentitySource::SanUri, GeneralName::URI(v))
                    | (IdentitySource::SanEmail, GeneralName::RFC822Name(v)) => Some(*v),
                    // Other SAN kinds are simply not this source; an IP-address SAN
                    // alongside a dNSName is normal and must not count as ambiguity.
                    _ => None,
                }
            });
            let first = matching.next().ok_or(AuthError::Rejected)?;
            if matching.next().is_some() {
                return Err(AuthError::Rejected);
            }
            first
        }
    };

    // Reject empty subjects and any carrying MQTT wildcard characters: such a subject
    // is unusable for routing and, substituted into a `%i` ACL pattern, could broaden a
    // grant across namespaces (ADR 0004). `/` is a level separator with the same hazard,
    // refused everywhere except a URI SAN where it is structural. Fail closed at the door.
    if subject.is_empty()
        || subject.contains(['+', '#'])
        || (!source.allows_slash() && subject.contains('/'))
    {
        return Err(AuthError::Rejected);
    }
    Ok(Identity {
        subject: subject.to_string(),
        groups: vec![],
    })
}

/// Extract the serial number (big-endian bytes as encoded in the certificate) from
/// a DER-encoded, already chain-verified X.509 leaf — the fact a CRL names, kept
/// with the connection so a revocation sweep can re-check it (ADR 0040 T1).
///
/// `None` if the certificate cannot be parsed (the identity extraction will have
/// rejected such a leaf already).
#[must_use]
pub fn serial_from_cert(der: &[u8]) -> Option<Vec<u8>> {
    let (rest, cert) = x509_parser::parse_x509_certificate(der).ok()?;
    if !rest.is_empty() {
        return None;
    }
    Some(cert.raw_serial().to_vec())
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

    // ----- SAN-based identity selection (ADR 0004 T11) -----

    /// Mint a self-signed certificate with the given CN and SAN entries.
    fn cert_with_sans(cn: &str, sans: &[rcgen::SanType]) -> Vec<u8> {
        let key = rcgen::KeyPair::generate().expect("key generation");
        let mut params = rcgen::CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        if !cn.is_empty() {
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, cn.to_string());
        }
        params.subject_alt_names = sans.to_vec();
        let cert = params.self_signed(&key).expect("self-sign");
        cert.der().to_vec()
    }

    fn dns(v: &str) -> rcgen::SanType {
        rcgen::SanType::DnsName(v.to_string().try_into().expect("IA5 string"))
    }
    fn uri(v: &str) -> rcgen::SanType {
        rcgen::SanType::URI(v.to_string().try_into().expect("IA5 string"))
    }
    fn email(v: &str) -> rcgen::SanType {
        rcgen::SanType::Rfc822Name(v.to_string().try_into().expect("IA5 string"))
    }

    /// The whole point of the option: a PKI that leaves the CN decorative still yields the
    /// right principal, and the *same* certificate yields a different subject per source —
    /// so the setting genuinely selects, rather than falling back to the CN.
    #[test]
    fn each_san_source_selects_its_own_field_of_one_certificate() {
        let der = cert_with_sans(
            "unused-cn",
            &[
                dns("device-42.fleet.example"),
                uri("spiffe://fleet.example/ns/prod/sa/device-42"),
                email("device-42@fleet.example"),
            ],
        );
        for (source, expected) in [
            (IdentitySource::CommonName, "unused-cn"),
            (IdentitySource::SanDns, "device-42.fleet.example"),
            (
                IdentitySource::SanUri,
                "spiffe://fleet.example/ns/prod/sa/device-42",
            ),
            (IdentitySource::SanEmail, "device-42@fleet.example"),
        ] {
            let id = identity_from_cert_with(&der, source).expect("source present");
            assert_eq!(id.subject, expected, "source {source}");
            assert!(id.groups.is_empty());
        }
    }

    /// The historical entry point must keep meaning exactly "the CN", so the cluster-bus
    /// node-id binding (T7) is unaffected by whatever a client listener is configured with.
    #[test]
    fn the_default_source_is_the_common_name() {
        assert_eq!(IdentitySource::default(), IdentitySource::CommonName);
        let der = cert_with_sans("the-cn", &[dns("the-dns-name")]);
        assert_eq!(
            identity_from_cert(&der).expect("CN present").subject,
            "the-cn"
        );
    }

    /// Fail closed, half one: the selected source is simply not in the certificate. A
    /// SAN-configured broker must NOT quietly fall back to the CN — that would let a
    /// CA that can mint any CN impersonate a SAN-named workload.
    #[test]
    fn a_missing_selected_source_yields_no_identity_and_never_falls_back() {
        // CN present, no SAN extension at all.
        let der = cert_with_sans("device-42", &[]);
        for source in [
            IdentitySource::SanDns,
            IdentitySource::SanUri,
            IdentitySource::SanEmail,
        ] {
            assert!(
                matches!(
                    identity_from_cert_with(&der, source),
                    Err(AuthError::Rejected)
                ),
                "{source} must not fall back to the CN"
            );
        }
        // A SAN extension that carries only *other* kinds is equally not a match.
        let dns_only = cert_with_sans("device-42", &[dns("device-42.example")]);
        assert!(matches!(
            identity_from_cert_with(&dns_only, IdentitySource::SanUri),
            Err(AuthError::Rejected)
        ));
        // ...and the CN source is unaffected by the presence of SANs.
        assert!(identity_from_cert_with(&dns_only, IdentitySource::CommonName).is_ok());
        // A cert with SANs but no CN yields nothing for the CN source.
        let no_cn = cert_with_sans("", &[dns("device-42.example")]);
        assert!(matches!(
            identity_from_cert_with(&no_cn, IdentitySource::CommonName),
            Err(AuthError::Rejected)
        ));
    }

    /// Fail closed, half two: two candidates are as fatal as none. A first-wins rule would
    /// hand the choice of principal to whoever ordered the certificate's fields.
    #[test]
    fn an_ambiguous_source_yields_no_identity() {
        let two_dns = cert_with_sans("cn", &[dns("a.example"), dns("b.example")]);
        assert!(matches!(
            identity_from_cert_with(&two_dns, IdentitySource::SanDns),
            Err(AuthError::Rejected)
        ));
        let two_uri = cert_with_sans("cn", &[uri("spiffe://x/a"), uri("spiffe://x/b")]);
        assert!(matches!(
            identity_from_cert_with(&two_uri, IdentitySource::SanUri),
            Err(AuthError::Rejected)
        ));
    }

    /// The same rule for the CN source. `rcgen`'s Distinguished Name is keyed by attribute
    /// type, so it cannot mint a two-CN subject at all — this uses a committed fixture
    /// minted by OpenSSL (`-subj "/CN=alice/CN=bob"`), which real CAs can and do issue.
    #[test]
    fn a_multi_common_name_subject_yields_no_identity() {
        let pem = include_bytes!("testdata/multi_cn_leaf.pem");
        let (_, parsed) = x509_parser::pem::parse_x509_pem(pem).expect("fixture is valid PEM");
        // Guard the fixture itself: if it ever stops carrying two CNs the test below would
        // pass vacuously for the wrong reason.
        let cert = parsed.parse_x509().expect("fixture is a valid certificate");
        assert_eq!(
            cert.subject().iter_common_name().count(),
            2,
            "fixture must carry exactly the ambiguity under test"
        );
        assert!(matches!(
            identity_from_cert(&parsed.contents),
            Err(AuthError::Rejected)
        ));
    }

    /// Ambiguity is per *kind*: a certificate carrying one dNSName next to a URI and an
    /// email is the normal shape, and each source still resolves.
    #[test]
    fn other_san_kinds_alongside_do_not_count_as_ambiguity() {
        let der = cert_with_sans(
            "cn",
            &[
                dns("one.example"),
                uri("spiffe://x/one"),
                email("one@example"),
                rcgen::SanType::IpAddress(std::net::IpAddr::from([10, 0, 0, 1])),
            ],
        );
        assert_eq!(
            identity_from_cert_with(&der, IdentitySource::SanDns)
                .expect("single dNSName")
                .subject,
            "one.example"
        );
    }

    /// The metacharacter guard travels with every source: a SAN is CA-controlled text just
    /// like a CN, and must not be able to inject MQTT wildcards into a `%i` pattern.
    #[test]
    fn san_subjects_with_topic_wildcards_are_rejected() {
        for bad in ["+", "#", "has#hash", "wild+card"] {
            let der = cert_with_sans("cn", &[dns(bad), uri(bad), email(bad)]);
            for source in [
                IdentitySource::SanDns,
                IdentitySource::SanUri,
                IdentitySource::SanEmail,
            ] {
                assert!(
                    matches!(
                        identity_from_cert_with(&der, source),
                        Err(AuthError::Rejected)
                    ),
                    "{source} must reject {bad:?}"
                );
            }
        }
    }

    /// `/` is structural in a URI and hostile anywhere else. A SPIFFE ID must survive
    /// intact; a dNSName or email carrying a level separator must not.
    #[test]
    fn only_a_uri_san_may_carry_a_level_separator() {
        let der = cert_with_sans(
            "cn",
            &[
                dns("a/b"),
                uri("spiffe://trust.example/ns/prod/sa/device"),
                email("a/b@example"),
            ],
        );
        assert_eq!(
            identity_from_cert_with(&der, IdentitySource::SanUri)
                .expect("URI SAN keeps its path")
                .subject,
            "spiffe://trust.example/ns/prod/sa/device"
        );
        for source in [IdentitySource::SanDns, IdentitySource::SanEmail] {
            assert!(
                matches!(
                    identity_from_cert_with(&der, source),
                    Err(AuthError::Rejected)
                ),
                "{source} must reject a `/`-bearing value"
            );
        }
    }

    /// A `/`-bearing URI subject is safe *because* the ACL engine refuses to substitute it:
    /// this pins the interlock the source rule above depends on (see
    /// `acl::percent_i_substitution_fails_closed_for_unsafe_subjects`).
    #[test]
    fn a_uri_subject_can_never_be_substituted_into_a_topic_pattern() {
        let policy = crate::acl::AclPolicy::from_toml_str(
            r#"
            [[rules]]
            effect = "allow"
            actions = ["subscribe"]
            topics = ["dev/%i/#"]
            "#,
        )
        .expect("policy parses");
        let der = cert_with_sans("cn", &[uri("spiffe://trust.example/ns/prod/sa/device")]);
        let id = identity_from_cert_with(&der, IdentitySource::SanUri).expect("URI SAN");
        // The pattern is unusable for this subject, so the rule grants nothing at all —
        // it must never expand into extra topic levels.
        assert!(!crate::Authorizer::authorize_subscribe(
            &policy,
            &id,
            &"dev/spiffe:/trust.example/ns/prod/sa/device/#".to_string()
        ));
        assert!(!crate::Authorizer::authorize_subscribe(
            &policy,
            &id,
            &"dev/+/#".to_string()
        ));
    }

    /// Configuration spellings are exact and round-trip; a typo is an error, never a
    /// silent fallback to a different identity model.
    #[test]
    fn identity_source_spellings_round_trip_and_reject_typos() {
        for source in [
            IdentitySource::CommonName,
            IdentitySource::SanDns,
            IdentitySource::SanUri,
            IdentitySource::SanEmail,
        ] {
            assert_eq!(IdentitySource::parse(source.as_str()), Ok(source));
            // Surrounding whitespace and case are tolerated; nothing else is.
            assert_eq!(
                IdentitySource::parse(&format!("  {}  ", source.as_str().to_uppercase())),
                Ok(source)
            );
        }
        for typo in ["", "san", "dns", "common-name", "san_dns", "sanDNS!"] {
            assert!(
                IdentitySource::parse(typo).is_err(),
                "{typo:?} must not parse"
            );
        }
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
