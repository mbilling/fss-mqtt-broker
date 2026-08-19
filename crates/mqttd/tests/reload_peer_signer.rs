//! Hot rotation of the gossip signing identity (issue #269), end to end over real files
//! and real PKI: the peer-bus leaf + key are rotated **on disk**, one filesystem-watch
//! tick (ADR 0033) drives the same validate-before-swap reload as `SIGHUP` (ADR 0032),
//! and the very next sealed gossip datagram is signed with — and embeds — the NEW leaf,
//! chain-verified by a peer against the unchanged cluster CA.
//!
//! Before #269 the signer was a startup snapshot: the rotated node kept signing with the
//! old key and embedding the old cert, which still chain-verified against the CA, so
//! nothing failed until the OLD cert's `notAfter` — long after the rotation, with no
//! correlating change to point at.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use mqtt_auth::basic::BasicAuthenticator;
use mqtt_auth::{AllowAll, Authenticator, Authorizer};
use mqtt_cluster::swim_auth::{
    GossipSign, GossipVerify, OpenReject, SwimAuth, VerifiedIdentity, KEY_LEN,
};
use mqttd::config_watch::ConfigWatcher;
use mqttd::reload;

/// The binary's `NodeGossipSigner`, reproduced at the test seam: signs with the peer-bus
/// key and carries the leaf so receivers chain-verify it (ADR 0022).
struct NodeSigner {
    cert_der: Vec<u8>,
    signer: mqtt_auth::signed_gossip::GossipSigner,
}
impl GossipSign for NodeSigner {
    fn cert_der(&self) -> &[u8] {
        &self.cert_der
    }
    fn sign(&self, payload: &[u8]) -> Vec<u8> {
        self.signer.sign(payload)
    }
}

/// The binary's `CaGossipVerifier`, plus a capture of the last cert it verified — the
/// observable that discriminates a hot-rotated signer from a stale snapshot (a stale
/// old-leaf datagram ALSO chain-verifies, so "it opens" alone proves nothing).
struct CapturingCaVerifier {
    ca_der: Vec<u8>,
    seen_cert: Mutex<Option<Vec<u8>>>,
}
impl GossipVerify for CapturingCaVerifier {
    fn verify(
        &self,
        cert_der: &[u8],
        payload: &[u8],
        sig: &[u8],
    ) -> Result<VerifiedIdentity, OpenReject> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        match mqtt_auth::signed_gossip::verify(&self.ca_der, cert_der, payload, sig, now, None) {
            Ok(v) => {
                *self.seen_cert.lock().unwrap() = Some(cert_der.to_vec());
                Ok(VerifiedIdentity {
                    cn: v.cn,
                    failure_domain: v.failure_domain,
                })
            }
            Err(_) => Err(OpenReject::Auth),
        }
    }
}

/// Mint a throwaway cluster CA and return `(dir, ca_der)`.
fn mint_ca(tag: &str) -> (PathBuf, rcgen::CertifiedIssuer<'static, rcgen::KeyPair>) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static UNIQUE: AtomicU64 = AtomicU64::new(0);
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("mqttd-signer-rot-{}-{tag}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
    let ca_cert = rcgen::CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();
    (dir, ca_cert)
}

/// Mint a CA-signed leaf for CN `cn`, returning `(cert_pem, key_pem)`.
fn mint_leaf(ca: &rcgen::CertifiedIssuer<'static, rcgen::KeyPair>, cn: &str) -> (String, String) {
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    let leaf = params.signed_by(&leaf_key, ca).unwrap();
    (leaf.pem(), leaf_key.serialize_pem())
}

/// Build a signer from the on-disk leaf + key — the exact closure shape the binary hands
/// [`reload::Reloader::attach_gossip_signer`].
fn signer_from_files(cert: &Path, key: &Path) -> reload::GossipSignerBuildResult {
    let cert_der = mqtt_net::tls::first_cert_der(cert).map_err(|e| format!("peer cert: {e}"))?;
    let key_der = mqtt_net::tls::private_key_der(key).map_err(|e| format!("peer key: {e}"))?;
    let signer = mqtt_auth::signed_gossip::GossipSigner::from_pkcs8_der(&key_der)
        .map_err(|e| format!("gossip signing key: {e}"))?;
    Ok(Arc::new(NodeSigner { cert_der, signer }) as Arc<dyn GossipSign>)
}

#[allow(clippy::unnecessary_wraps)] // must match the `build: Fn() -> BuildResult` signature
fn ok_policy() -> reload::BuildResult {
    Ok((
        Arc::new(AllowAll) as Arc<dyn Authorizer>,
        Arc::new(BasicAuthenticator {
            allow_anonymous: true,
        }) as Arc<dyn Authenticator>,
    ))
}

/// Rotating the leaf + key on disk lands on the gossip plane within ONE watch tick: the
/// next sealed datagram embeds the new cert (verified by a peer against the unchanged CA),
/// while an old-leaf datagram from a not-yet-rotated peer still verifies (mid-rotation
/// coexistence). A garbage half-written key is REJECTED by the same validate-before-swap
/// reload, keeping the running signer, and is retried until the write settles.
#[test]
fn a_leaf_rotated_on_disk_is_signing_gossip_within_one_watch_tick() {
    let (dir, ca_cert) = mint_ca("hot");
    let ca_der = ca_cert.der().to_vec();
    let cert_path = dir.join("node.crt");
    let key_path = dir.join("node.key");
    let (old_cert_pem, old_key_pem) = mint_leaf(&ca_cert, "node-a");
    std::fs::write(&cert_path, &old_cert_pem).unwrap();
    std::fs::write(&key_path, &old_key_pem).unwrap();

    // The sender's SwimAuth, built exactly as the binary builds it at startup.
    let initial = signer_from_files(&cert_path, &key_path).expect("startup signer builds");
    let old_der = mqtt_net::tls::first_cert_der(&cert_path).unwrap();
    let sender = SwimAuth::new(&[7; KEY_LEN]).with_signing(
        initial,
        Arc::new(CapturingCaVerifier {
            ca_der: ca_der.clone(),
            seen_cert: Mutex::new(None),
        }),
    );
    let slot = sender.signer_slot().expect("signed posture has a slot");

    // A peer that verifies against the SAME CA, capturing which cert it saw.
    let receiver_verifier = Arc::new(CapturingCaVerifier {
        ca_der,
        seen_cert: Mutex::new(None),
    });
    let receiver = SwimAuth::new(&[7; KEY_LEN]).with_signing(
        signer_from_files(&cert_path, &key_path).unwrap(),
        receiver_verifier.clone(),
    );

    // The reloader + watcher, wired as the binary wires them: the leaf and key are in
    // the watch scope, and the reload rebuilds the signer from the re-read files.
    let audit = Arc::new(mqtt_observability::AuditLog::new());
    let (mut reloader, _handles) = reload::Reloader::new(ok_policy().unwrap(), audit, ok_policy);
    {
        let (cert_path, key_path) = (cert_path.clone(), key_path.clone());
        reloader.attach_gossip_signer(slot, move || signer_from_files(&cert_path, &key_path));
    }
    let mut watcher = ConfigWatcher::new(vec![cert_path.clone(), key_path.clone()]);

    // Pre-rotation: the sealed datagram carries the OLD leaf.
    let sealed = sender.seal(b"before", true);
    receiver.open(&sealed).expect("old leaf chain-verifies");
    assert_eq!(
        receiver_verifier.seen_cert.lock().unwrap().as_deref(),
        Some(old_der.as_slice())
    );

    // Rotate on disk (what cert-manager / a re-mounted Secret does), then one watch tick.
    let (new_cert_pem, new_key_pem) = mint_leaf(&ca_cert, "node-a");
    std::fs::write(&cert_path, &new_cert_pem).unwrap();
    std::fs::write(&key_path, &new_key_pem).unwrap();
    let new_der = mqtt_net::tls::first_cert_der(&cert_path).unwrap();
    assert_ne!(old_der, new_der, "the mint must actually rotate the leaf");
    assert!(
        watcher.tick(|| reloader.reload("watch")),
        "the on-disk rotation must be detected within one tick"
    );

    // Post-rotation: the very next datagram is signed with and embeds the NEW leaf.
    let sealed = sender.seal(b"after", true);
    receiver
        .open(&sealed)
        .expect("the rotated leaf chain-verifies against the unchanged CA");
    assert_eq!(
        receiver_verifier.seen_cert.lock().unwrap().as_deref(),
        Some(new_der.as_slice()),
        "the NEW cert must be embedded in gossip within a watch tick"
    );

    // Mid-rotation coexistence: a not-yet-rotated peer's OLD-leaf datagram still opens
    // (per-datagram chain verification against the CA — nothing pins a single leaf).
    let old_key_path = dir.join("old.key");
    std::fs::write(&old_key_path, &old_key_pem).unwrap();
    let laggard = SwimAuth::new(&[7; KEY_LEN]).with_signing(
        Arc::new(NodeSigner {
            cert_der: old_der.clone(),
            signer: mqtt_auth::signed_gossip::GossipSigner::from_pkcs8_der(
                &mqtt_net::tls::private_key_der(&old_key_path).unwrap(),
            )
            .unwrap(),
        }),
        receiver_verifier.clone(),
    );
    receiver
        .open(&laggard.seal(b"laggard", true))
        .expect("an old-leaf datagram still verifies mid-rotation");

    // Fail-safe: a garbage (half-written) key is rejected — the running signer is kept —
    // and the watcher retries until the write settles.
    std::fs::write(&key_path, b"not a pem").unwrap();
    assert!(watcher.tick(|| reloader.reload("watch")), "change detected");
    let sealed = sender.seal(b"still-new", true);
    receiver.open(&sealed).expect("still verifies");
    assert_eq!(
        receiver_verifier.seen_cert.lock().unwrap().as_deref(),
        Some(new_der.as_slice()),
        "a rejected reload must keep the running signer"
    );
    std::fs::write(&key_path, &new_key_pem).unwrap();
    assert!(
        watcher.tick(|| reloader.reload("watch")),
        "the settled write is retried and applies"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
