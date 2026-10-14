//! A peer link and its certificates' expiry
//! ([ADR 0088](../../docs/adr/0088-credential-lifetimes-and-rotation.md) D3, D4), as real
//! processes.
//!
//! Two spawned `mqttd` binaries form a cluster over mutual TLS. Node A's certificate
//! lives for days; node B's is minted to end a short while after the mesh has formed.
//! The broker reads the real clock, so the lifetime is real, not a faked clock.
//!
//! CHARACTERISATION (before T3): these assert what the code does today. With gossip
//! unsigned nothing but the handshake reads a certificate, and the link stays up past
//! `notAfter` for as long as anyone watches. With gossip signed, b's datagrams are
//! dropped as `expired` and the failure detector's verdict drops the link — which comes
//! straight back, because the dialer resumes its TLS session and a resumed handshake does
//! not verify the certificate again.

mod common;
mod proc_common;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use proc_common::*;

/// How long node B's certificate lives from the moment it is minted: long enough for two
/// debug binaries to start and form their link on a loaded runner, short enough that the
/// test does not wait long for the end.
const SHORT_LIFETIME: Duration = Duration::from_secs(40);

/// What ADR 0088 D3 allows between `notAfter` and the end of the link.
const WITHIN: Duration = Duration::from_mins(1);

struct Pki {
    dir: PathBuf,
    ca: rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
}

impl Pki {
    fn new(dir: &Path) -> Self {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        let ca = rcgen::CertifiedIssuer::self_signed(params, ca_key).unwrap();
        std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
        Self {
            dir: dir.to_path_buf(),
            ca,
        }
    }

    /// The peer-bus environment for node `cn`, its leaf valid from a minute ago (as
    /// `gen-certs.sh` mints) until `not_after`.
    fn env_for(&self, cn: &str, not_after: SystemTime) -> Vec<(String, String)> {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.not_before = (SystemTime::now() - Duration::from_mins(1)).into();
        params.not_after = not_after.into();
        let leaf = params.signed_by(&key, &self.ca).unwrap();
        let cert = self.dir.join(format!("{cn}.pem"));
        let key_path = self.dir.join(format!("{cn}.key"));
        std::fs::write(&cert, leaf.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        let path = |p: &Path| p.to_string_lossy().into_owned();
        vec![
            ("MQTTD_PEER_TLS_CA".into(), path(&self.dir.join("ca.pem"))),
            ("MQTTD_PEER_TLS_CERT".into(), path(&cert)),
            ("MQTTD_PEER_TLS_KEY".into(), path(&key_path)),
        ]
    }
}

/// Two nodes, `a` and `b`, each seeded with the other. `b`'s certificate ends at the
/// returned instant; `a`'s in a week. `extra` is added to both nodes' environment.
async fn build(root: &Path, extra: &[(&str, &str)]) -> (Vec<ProcNode>, SystemTime) {
    let pki = Pki::new(root);
    let swim: Vec<String> = (0..2)
        .map(|_| format!("127.0.0.1:{}", free_udp_port()))
        .collect();
    let b_ends = SystemTime::now() + SHORT_LIFETIME;
    let mut nodes = Vec::new();
    for (i, name) in ["a", "b"].into_iter().enumerate() {
        let id = format!("exp{}-{name}", std::process::id());
        let data_dir = root.join(&id);
        std::fs::create_dir_all(&data_dir).unwrap();
        let peer_bind = SocketAddr::from(([127, 0, 0, 1], free_tcp_port()));
        let (relay_addr, relay, relay_abort) = spawn_relay(peer_bind).await;
        let not_after = if i == 1 {
            b_ends
        } else {
            SystemTime::now() + Duration::from_hours(24 * 7)
        };
        let mut extra_env = pki.env_for(&id, not_after);
        extra_env.push(("MQTTD_DURABLE_SESSIONS".into(), "0".into()));
        extra_env.extend(extra.iter().map(|(k, v)| ((*k).into(), (*v).into())));
        nodes.push(ProcNode {
            log_path: root.join(format!("{id}.log")),
            id,
            child: None,
            data_dir,
            client_addr: SocketAddr::from(([127, 0, 0, 1], free_tcp_port())),
            peer_bind,
            swim_bind: swim[i].clone(),
            health_addr: SocketAddr::from(([127, 0, 0, 1], free_tcp_port())),
            swim_seeds: swim[1 - i].clone(),
            relay_addr,
            relay,
            relay_abort,
            file_size_limit_blocks: None,
            extra_env,
            binary: PathBuf::from(env!("CARGO_BIN_EXE_mqttd")),
        });
    }
    (nodes, b_ends)
}

/// `(members, peer links)` of each node; `None` where `/metrics` did not answer.
async fn survey(nodes: &[ProcNode]) -> Vec<Option<(u64, u64)>> {
    let mut out = Vec::new();
    for n in nodes {
        let members = n.metric("mqttd_cluster_members").await;
        let links = n.metric("mqttd_peer_links").await;
        out.push(members.zip(links));
    }
    out
}

fn dump(nodes: &[ProcNode]) -> String {
    nodes
        .iter()
        .map(|n| format!("---- {} ----\n{}", n.id, log_notables(&n.log_path, 40)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Start both nodes and wait for the link, which must form before `b_ends`.
async fn form(nodes: &mut [ProcNode], b_ends: SystemTime) {
    for n in nodes.iter_mut() {
        n.spawn();
    }
    loop {
        for n in nodes.iter_mut() {
            if let Some(status) = n.exited() {
                panic!("{} exited ({status})\n{}", n.id, log_tail(&n.log_path));
            }
        }
        if survey(nodes).await.iter().all(|v| *v == Some((2, 1))) {
            return;
        }
        assert!(
            SystemTime::now() + Duration::from_secs(5) < b_ends,
            "the two nodes did not link before b's certificate ran out — the runner is too \
             slow for SHORT_LIFETIME ({}s)\n{}",
            SHORT_LIFETIME.as_secs(),
            dump(nodes)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Sleep until wall-clock `at`.
async fn sleep_until(at: SystemTime) {
    if let Ok(left) = at.duration_since(SystemTime::now()) {
        tokio::time::sleep(left).await;
    }
}

/// CHARACTERISATION of the code before 0088-T3: with unsigned gossip, nothing but the
/// handshake reads a peer's certificate, and an established link outlives it. Watched
/// for a minute past `notAfter` plus a margin — longer than D3 allows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_link_outlives_its_peers_certificate() {
    let root = tempfile::tempdir().unwrap();
    let (mut nodes, b_ends) = build(root.path(), &[("MQTTD_SWIM_SIGNED", "off")]).await;
    form(&mut nodes, b_ends).await;

    sleep_until(b_ends + WITHIN + Duration::from_secs(5)).await;
    let started = Instant::now();
    let view = survey(&nodes).await;
    assert_eq!(
        view,
        vec![Some((2, 1)), Some((2, 1))],
        "the link was expected to outlive b's certificate on this code; (members, links) \
         = {view:?} {:?} after the minute\n{}",
        started.elapsed(),
        dump(&nodes)
    );
}

/// How many times `node` has logged a link established.
fn links_established(node: &ProcNode) -> usize {
    std::fs::read_to_string(&node.log_path)
        .unwrap_or_default()
        .matches("peer link established")
        .count()
}

/// CHARACTERISATION of the code before 0088-T3, with gossip signed (the default): a's
/// failure detector stops hearing b at `notAfter` (b's datagrams are dropped as
/// `expired`) and its verdict drops the link — and the link comes straight back, because
/// a's dialer RESUMES its TLS session with b, and a resumed handshake does not look at
/// b's certificate again. Admission checks only the revocation list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_link_to_an_expired_peer_is_re_established() {
    let root = tempfile::tempdir().unwrap();
    let (mut nodes, b_ends) = build(root.path(), &[]).await;
    form(&mut nodes, b_ends).await;
    let before = links_established(&nodes[0]);

    sleep_until(b_ends + WITHIN + Duration::from_secs(5)).await;
    let expired = nodes[0]
        .metric("mqttd_gossip_rejected_total{reason=\"expired\"}")
        .await
        .unwrap_or(0);
    assert!(
        expired > 0,
        "a drops b's gossip as expired\n{}",
        dump(&nodes)
    );
    let after = links_established(&nodes[0]);
    assert!(
        after > before,
        "a was expected to link b again after b's certificate ran out (links established: \
         {before} before, {after} after)\n{}",
        dump(&nodes)
    );
}
