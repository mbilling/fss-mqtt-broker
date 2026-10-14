//! A peer link and its certificates' expiry
//! ([ADR 0088](../../docs/adr/0088-credential-lifetimes-and-rotation.md) D3, D4), as real
//! processes.
//!
//! Two spawned `mqttd` binaries form a cluster over mutual TLS. Node A's certificate
//! lives for days; node B's is minted to end a short while after the mesh has formed.
//! The broker reads the real clock, so the lifetime is real, not a faked clock.
//!
//! Before 0088-T3 the link outlived the certificate. With gossip unsigned nothing but
//! the handshake read a certificate, and the link stayed up past `notAfter` for as long
//! as anyone watched. With gossip signed (the default), b's datagrams were dropped as
//! `expired` and the failure detector's verdict dropped the link — which came straight
//! back, because the dialer RESUMED its TLS session, and a resumed handshake does not
//! verify the certificate again. (This file's first commit asserted both.)
//!
//! Now the link ends within a minute of `notAfter` — within a second, on each side, by
//! the hub's sweep — whatever the gossip posture; neither node relinks, since the peer
//! bus no longer resumes sessions and admission checks both certificates' ends; and an
//! expired node's gossip stays dropped, so membership holds it out.

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

/// How many times `node` has logged a link established.
fn links_established(node: &ProcNode) -> usize {
    std::fs::read_to_string(&node.log_path)
        .unwrap_or_default()
        .matches("peer link established")
        .count()
}

/// Wait until neither node has a link, at most until a minute past `b_ends`. Returns
/// when that was seen, relative to `b_ends` (negative: before it).
async fn await_unlinked(nodes: &mut [ProcNode], b_ends: SystemTime) -> Duration {
    loop {
        for n in nodes.iter_mut() {
            if let Some(status) = n.exited() {
                panic!("{} exited ({status})\n{}", n.id, log_tail(&n.log_path));
            }
        }
        let view = survey(nodes).await;
        if view.iter().all(|v| v.is_some_and(|(_, links)| links == 0)) {
            let at = SystemTime::now();
            assert!(
                at >= b_ends,
                "the link ended {:?} BEFORE b's certificate did\n{}",
                b_ends.duration_since(at).unwrap_or_default(),
                dump(nodes)
            );
            return at.duration_since(b_ends).unwrap_or_default();
        }
        assert!(
            SystemTime::now() < b_ends + WITHIN,
            "the link to b outlived b's certificate by more than a minute (ADR 0088 D3); \
             (members, links) = {view:?}\n{}",
            dump(nodes)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The counter of links or sessions ended for `reason` on `node`.
async fn ended_for(node: &ProcNode, reason: &str) -> u64 {
    node.metric(&format!(
        "mqttd_revocation_evictions_total{{reason=\"{reason}\"}}"
    ))
    .await
    .unwrap_or(0)
}

/// D3 by itself: with gossip unsigned, nothing but the link's own expiry check can end
/// it. It is ended within a minute of b's `notAfter`, and not before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_link_ends_within_a_minute_of_its_peers_certificate() {
    let root = tempfile::tempdir().unwrap();
    let (mut nodes, b_ends) = build(root.path(), &[("MQTTD_SWIM_SIGNED", "off")]).await;
    form(&mut nodes, b_ends).await;

    let late = await_unlinked(&mut nodes, b_ends).await;
    eprintln!("the link ended {late:?} after b's notAfter");
    // Each side's sweep looks once a second; whichever looks first ends the link, and
    // the other sees it lost. (That each side ends it by itself is the hub's unit tests.)
    let by_a = ended_for(&nodes[0], "peer-cert-expired").await;
    let by_b = ended_for(&nodes[1], "own-cert-expired").await;
    assert!(
        by_a + by_b >= 1,
        "the link was ended for expiry — by a on b's certificate or by b on its own \
         (a: {by_a}, b: {by_b})\n{}",
        dump(&nodes)
    );
}

/// D4: an expired node is out. Its gossip is dropped and counted, the link ends, and it
/// is not linked again — not by a's dialer, which keeps dialing a peer it holds dead
/// (ADR 0087 D5) and used to come back on a resumed TLS session, and not by b's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_node_is_out_and_stays_out() {
    let root = tempfile::tempdir().unwrap();
    let (mut nodes, b_ends) = build(root.path(), &[]).await;
    form(&mut nodes, b_ends).await;

    await_unlinked(&mut nodes, b_ends).await;
    let established = links_established(&nodes[0]);
    // a stops hearing b, and its failure detector says so.
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let expired = nodes[0]
            .metric("mqttd_gossip_rejected_total{reason=\"expired\"}")
            .await
            .unwrap_or(0);
        let members = nodes[0].metric("mqttd_cluster_members").await;
        if expired > 0 && members == Some(1) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a still holds b a member (members {members:?}, expired drops {expired})\n{}",
            dump(&nodes)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // SETTLE(expired-peer-stays-unlinked): the property is a NEGATIVE — no link forms
    // again. a redials a peer it holds dead at most every 5 s (HELD_DEAD_REDIAL_MAX) and b
    // redials too; twelve seconds is two of a's longest waits plus margin.
    tokio::time::sleep(Duration::from_secs(12)).await;
    let view = survey(&nodes).await;
    assert!(
        view.iter().all(|v| v.is_some_and(|(_, links)| links == 0))
            && links_established(&nodes[0]) == established,
        "a link to the expired node formed again; (members, links) = {view:?}\n{}",
        dump(&nodes)
    );
}
