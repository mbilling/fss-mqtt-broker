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
        // CrlSign: the relay-stream tests publish a cluster CRL signed by it.
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
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
        self.env_with_serial(cn, not_after, None)
    }

    /// [`env_for`](Self::env_for), with the leaf's serial chosen — the fact a CRL names
    /// (ADR 0040 T4) — where `serial` is given, and random otherwise.
    fn env_with_serial(
        &self,
        cn: &str,
        not_after: SystemTime,
        serial: Option<u64>,
    ) -> Vec<(String, String)> {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.serial_number = serial.map(rcgen::SerialNumber::from);
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

    /// Write the cluster CRL, CA-signed, revoking `serials`, and return its path.
    fn write_crl(&self, serials: &[u64]) -> PathBuf {
        let params = rcgen::CertificateRevocationListParams {
            this_update: rcgen::date_time_ymd(2020, 1, 1),
            next_update: rcgen::date_time_ymd(2099, 1, 1),
            crl_number: rcgen::SerialNumber::from(serials.len() as u64 + 1),
            issuing_distribution_point: None,
            revoked_certs: serials
                .iter()
                .map(|serial| rcgen::RevokedCertParams {
                    serial_number: rcgen::SerialNumber::from(*serial),
                    revocation_time: rcgen::date_time_ymd(2020, 1, 2),
                    reason_code: Some(rcgen::RevocationReason::KeyCompromise),
                    invalidity_date: None,
                })
                .collect(),
            key_identifier_method: rcgen::KeyIdMethod::Sha256,
        };
        let crl = params.signed_by(&self.ca).unwrap();
        let path = self.dir.join("crl.pem");
        std::fs::write(&path, crl.pem().unwrap()).unwrap();
        path
    }
}

/// Two nodes, `a` and `b`, each seeded with the other. `b`'s certificate ends at the
/// returned instant; `a`'s in a week. `extra` is added to both nodes' environment.
async fn build(root: &Path, extra: &[(&str, &str)]) -> (Vec<ProcNode>, SystemTime) {
    let (nodes, _, b_ends) = build_pair(root, extra, SHORT_LIFETIME, None).await;
    (nodes, b_ends)
}

/// [`build`], with `b`'s certificate living `b_lifetime` and, where `crl` is given, `b`'s
/// leaf minted with serial `crl.0` and `a` started with a cluster CRL (`crl.1`, which
/// must not name it yet). Returns the PKI as well, to rewrite that CRL.
async fn build_pair(
    root: &Path,
    extra: &[(&str, &str)],
    b_lifetime: Duration,
    crl: Option<(u64, &[u64])>,
) -> (Vec<ProcNode>, Pki, SystemTime) {
    let pki = Pki::new(root);
    let swim: Vec<String> = (0..2)
        .map(|_| format!("127.0.0.1:{}", free_udp_port()))
        .collect();
    let b_ends = SystemTime::now() + b_lifetime;
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
        let serial = crl.filter(|_| i == 1).map(|(serial, _)| serial);
        let mut extra_env = pki.env_with_serial(&id, not_after, serial);
        if let Some((_, revoked)) = crl.filter(|_| i == 0) {
            let path = pki.write_crl(revoked);
            extra_env.push((
                "MQTTD_PEER_TLS_CRL".into(),
                path.to_string_lossy().into_owned(),
            ));
        }
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
    (nodes, pki, b_ends)
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

// ---- Relay streams (ADR 0005): the part of issue #1016 that #1018 left open ----
//
// A persistent session that lands on a node other than its placement owner is relocated:
// the landing node dials the owner's PEER listener, presents its own peer certificate,
// sends a `ProxyHello` and splices the client's bytes onto that connection (`conn.rs`
// `proxy_to_owner`). The owner admits the connection with `admit_peer_link` and serves it
// as a client session (`peer.rs` `handle`, the `ProxyHello` arm, `conn::serve_proxied`).
// Before the fix the `LinkCert` that admission returned was dropped and the stream was
// never seen by either sweep: it outlived the certificate it was admitted under, and its
// revocation. Now the owner's hub holds the relocated session to that `LinkCert`, and the
// landing node's hub holds its splice to the one its own dial was admitted under
// (`hub::relays`).
//
// The two tests below assert what D3 and ADR 0040 T4 ask of every connection on the
// peer listener, as they ask it of a peer link: it ends when a certificate it was admitted
// under expires, or is revoked. Gossip is unsigned in both, so membership never notices
// `b` (an expired or revoked signer's datagrams would be dropped and `b` declared dead):
// the session's owner stays `a` throughout, and the only thing that may end the stream
// is the check under test. In each the peer link between the two ends first, as the
// control: the sweep did run, and ended what it covers.

/// `b`'s leaf serial in the relay-stream tests, for the CRL to name.
const B_SERIAL: u64 = 0x1016;
/// An unrelated serial, so `a` starts with a CRL that is loaded and names nobody here.
const UNRELATED_SERIAL: u64 = 0x0975;

/// A persistent session of a client connected to `b` and relocated by `b` to its owner
/// `a` (ADR 0005), live through the relay. Client ids are tried until one is owned by
/// `a`, which `b`'s log says when it relocates the session.
async fn relayed_client(nodes: &[ProcNode]) -> (common::Client, String) {
    for k in 0..64 {
        let id = format!("relay{}-{k}x", std::process::id());
        let mark = log_len(&nodes[1]);
        let (mut client, _) = common::Client::connect_v311(nodes[1].client_addr, &id, false).await;
        // The CONNACK came from the owner when the session was relocated; b logged that
        // before it dialed.
        let relocated = log_since(&nodes[1], mark)
            .lines()
            .any(|l| l.contains("relocating persistent session") && l.contains(&id));
        if relocated {
            client.send(&mqtt_codec::Packet::PingReq).await;
            assert_eq!(
                client.recv().await,
                mqtt_codec::Packet::PingResp,
                "the relayed session does not answer"
            );
            return (client, id);
        }
        client.disconnect().await;
    }
    panic!(
        "no client id of 64 was owned by a; placement never relocated a session from b\n{}",
        dump(nodes)
    );
}

/// Wait until `client`'s connection is closed, at most until `deadline`. On the deadline,
/// fail, saying whether the session still answers through the relay.
async fn await_closed(
    client: &mut common::Client,
    deadline: SystemTime,
    what: &str,
    nodes: &[ProcNode],
) -> Duration {
    let from = SystemTime::now();
    while SystemTime::now() < deadline {
        match client.recv_bounded(Duration::from_millis(500)).await {
            common::Recv::Closed => return from.elapsed().unwrap_or_default(),
            common::Recv::Quiet | common::Recv::Packet(_) => {}
        }
    }
    client.send(&mqtt_codec::Packet::PingReq).await;
    let answer = client.recv_bounded(Duration::from_secs(5)).await;
    panic!(
        "{what}; the relayed session, asked a PINGREQ now, answered {answer:?}\n{}",
        dump(nodes)
    );
}

/// Issue #1016 (relay streams): a relay stream admitted under `b`'s certificate is ended
/// when that certificate passes its `notAfter`, as `b`'s peer link is (ADR 0088 D3: a
/// connection is not kept past its certificate's end; within a minute).
///
/// Before the fix the peer link ended 1 to 3 seconds after `b`'s `notAfter` and the relay
/// stream did not end at all: the client on `b` was still served by `a` a minute later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_stream_ends_when_the_certificate_it_was_admitted_under_expires() {
    let root = tempfile::tempdir().unwrap();
    let (mut nodes, _pki, b_ends) = build_pair(
        root.path(),
        &[("MQTTD_SWIM_SIGNED", "off")],
        SHORT_LIFETIME,
        None,
    )
    .await;
    form(&mut nodes, b_ends).await;
    let (mut client, id) = relayed_client(&nodes).await;
    assert!(
        SystemTime::now() < b_ends,
        "the session was relocated only after b's certificate had ended; the runner is too \
         slow for SHORT_LIFETIME ({}s)",
        SHORT_LIFETIME.as_secs()
    );

    // The control: the peer link between the same two nodes ends at that expiry.
    let late = await_unlinked(&mut nodes, b_ends).await;
    eprintln!("the peer link ended {late:?} after b's notAfter");

    let waited = await_closed(
        &mut client,
        b_ends + WITHIN,
        &format!(
            "the relay stream carrying {id} outlived b's certificate by more than a minute \
             (ADR 0088 D3), while the peer link ended {late:?} after it"
        ),
        &nodes,
    )
    .await;
    eprintln!("the relay stream ended {waited:?} after the peer link");
    // Whichever side looked first: a on b's certificate, or b on its own.
    let by_a = ended_for(&nodes[0], "relay-peer-cert-expired").await;
    let by_b = ended_for(&nodes[1], "relay-own-cert-expired").await;
    assert!(
        by_a + by_b >= 1,
        "the relay stream was ended for expiry (a: {by_a}, b: {by_b})\n{}",
        dump(&nodes)
    );
}

/// Issue #1016 (relay streams): a relay stream whose peer certificate the cluster CRL
/// revokes is ended by the reload that publishes the CRL, as the peer link is (ADR 0040
/// T4: a reload's sweep re-evaluates every established peer connection against the list
/// it just loaded).
///
/// Before the fix the reload's sweep ended `b`'s peer link at once and `b` was refused
/// when it redialed, but the relay stream admitted under the same certificate kept serving
/// the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_stream_ends_when_the_crl_revokes_the_certificate_it_was_admitted_under() {
    let root = tempfile::tempdir().unwrap();
    let (mut nodes, pki, b_ends) = build_pair(
        root.path(),
        &[("MQTTD_SWIM_SIGNED", "off")],
        Duration::from_hours(24 * 7),
        Some((B_SERIAL, &[UNRELATED_SERIAL])),
    )
    .await;
    form(&mut nodes, b_ends).await;
    let (mut client, id) = relayed_client(&nodes).await;

    // Revoke b's certificate and reload a.
    pki.write_crl(&[UNRELATED_SERIAL, B_SERIAL]);
    let revoked_at = SystemTime::now();
    let process = nodes[0].pid().expect("running").to_string();
    let sent = std::process::Command::new("kill")
        .args(["-HUP", &process])
        .status()
        .expect("run kill");
    assert!(sent.success());

    // The control: a's sweep ends the peer link, for revocation.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let revoked = ended_for(&nodes[0], "peer-revoked").await;
        let links = nodes[0].metric("mqttd_peer_links").await;
        if revoked >= 1 && links == Some(0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a's reload did not end b's peer link (peer-revoked {revoked}, links {links:?})\n{}",
            dump(&nodes)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let late = revoked_at.elapsed().unwrap_or_default();
    eprintln!("the peer link ended {late:?} after the reload");

    // The sweep is one pass over the hub's state; ten seconds is generous for a stream it
    // ends in the same pass.
    let waited = await_closed(
        &mut client,
        SystemTime::now() + Duration::from_secs(10),
        &format!(
            "the relay stream carrying {id} outlived the revocation of b's certificate by \
             more than ten seconds after the peer link ended (ADR 0040 T4)"
        ),
        &nodes,
    )
    .await;
    eprintln!("the relay stream ended {waited:?} after the peer link");
    let revoked = ended_for(&nodes[0], "relay-peer-revoked").await;
    assert_eq!(
        revoked,
        1,
        "a's reload ended the one relay stream, for revocation\n{}",
        dump(&nodes)
    );
}

// ---- Renewal: the links move before the old end (issue #1016) ----

/// How long the certificates of the renewal test's nodes live from their minting: room
/// for three debug binaries to start and mesh on a slow runner (they must within half of
/// it), then for the moves after the renewal and the end window before the old end.
const MINTED_LIFETIME: Duration = Duration::from_secs(100);

/// Three nodes, `a`, `b` and `c`, minted together — one `notAfter` for all, as
/// `gen-certs.sh` or a first cert-manager issue gives — and seeded with each other.
async fn build_minted_together(root: &Path, durable: bool) -> (Vec<ProcNode>, Pki, SystemTime) {
    let pki = Pki::new(root);
    let ends = SystemTime::now() + MINTED_LIFETIME;
    let swim: Vec<String> = (0..3)
        .map(|_| format!("127.0.0.1:{}", free_udp_port()))
        .collect();
    let mut nodes = Vec::new();
    for (i, name) in ["a", "b", "c"].into_iter().enumerate() {
        let id = format!(
            "ren{}{}-{name}",
            std::process::id(),
            if durable { "d" } else { "" }
        );
        let data_dir = root.join(&id);
        std::fs::create_dir_all(&data_dir).unwrap();
        let peer_bind = SocketAddr::from(([127, 0, 0, 1], free_tcp_port()));
        let (relay_addr, relay, relay_abort) = spawn_relay(peer_bind).await;
        let mut extra_env = pki.env_for(&id, ends);
        let durable = if durable { "1" } else { "0" };
        extra_env.push(("MQTTD_DURABLE_SESSIONS".into(), durable.into()));
        nodes.push(ProcNode {
            log_path: root.join(format!("{id}.log")),
            id,
            child: None,
            data_dir,
            client_addr: SocketAddr::from(([127, 0, 0, 1], free_tcp_port())),
            peer_bind,
            swim_bind: swim[i].clone(),
            health_addr: SocketAddr::from(([127, 0, 0, 1], free_tcp_port())),
            swim_seeds: swim
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, s)| s.clone())
                .collect::<Vec<_>>()
                .join(","),
            relay_addr,
            relay,
            relay_abort,
            file_size_limit_blocks: None,
            extra_env,
            binary: PathBuf::from(env!("CARGO_BIN_EXE_mqttd")),
        });
    }
    (nodes, pki, ends)
}

/// What `node` has logged since byte `from` of its log.
fn log_since(node: &ProcNode, from: usize) -> String {
    let log = std::fs::read_to_string(&node.log_path).unwrap_or_default();
    log.get(from..).unwrap_or_default().to_string()
}

fn log_len(node: &ProcNode) -> usize {
    std::fs::metadata(&node.log_path).map_or(0, |m| usize::try_from(m.len()).unwrap_or(0))
}

/// Issue #1016's acceptance. Three nodes minted together are renewed together. Before,
/// every link made before the renewal ended in the same second, at the old `notAfter`, and
/// every node redialled all of its peers. Now each link's dialer moves it to the renewed
/// certificates before that end — a new link made alongside, which replaces the old one —
/// so:
///
/// - every link has moved (each node registered a link to each peer again) before the old
///   end, and no link was ended for expiry at it;
/// - no node was ever without its links: none logged a lost link, a verdict or an expiry
///   end, and the link gauge read two on every node at every poll;
/// - a `QoS` 1 stream from `a` to a subscriber on `c`, published throughout, lost nothing
///   that was acknowledged.
///
/// That the moves are spread over a window, not made in one second, is the jitter's, and
/// is asserted in `peer.rs`'s unit tests; here the moments are printed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewed_nodes_move_their_links_before_the_old_end_without_a_gap() {
    renewal_moves_links(false).await;
}

/// The same with durable sessions on, so the lease group's consensus and the session-log
/// replication run over the links while they move: on top of the above, no node's lease
/// epoch moves — no election followed a move.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewed_nodes_move_their_links_without_an_election() {
    renewal_moves_links(true).await;
}

#[allow(clippy::too_many_lines)] // one scenario: mesh, stream, renew, watch, account
async fn renewal_moves_links(durable: bool) {
    use mqtt_codec::{Packet, QoS};
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    let root = tempfile::tempdir().unwrap();
    let (mut nodes, pki, old_end) = build_minted_together(root.path(), durable).await;
    for n in &mut nodes {
        n.spawn();
    }
    loop {
        for n in &mut nodes {
            if let Some(status) = n.exited() {
                panic!("{} exited ({status})\n{}", n.id, log_tail(&n.log_path));
            }
        }
        if survey(&nodes).await.iter().all(|v| *v == Some((3, 2))) {
            break;
        }
        assert!(
            SystemTime::now() + MINTED_LIFETIME / 2 < old_end,
            "the three nodes did not mesh within half of MINTED_LIFETIME ({}s)\n{}",
            MINTED_LIFETIME.as_secs(),
            dump(&nodes)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // A subscriber on c, acknowledging what it gets; a publisher on a.
    let mut sub = common::Client::connect(nodes[2].client_addr, "mv-sub").await;
    sub.subscribe(1, "mv/#", QoS::AtLeastOnce).await;
    let received: Arc<Mutex<BTreeSet<Vec<u8>>>> = Arc::default();
    let reader = {
        let received = received.clone();
        tokio::spawn(async move {
            loop {
                match sub.recv_bounded(Duration::from_millis(500)).await {
                    common::Recv::Packet(Packet::Publish(p)) => {
                        received.lock().unwrap().insert(p.payload.to_vec());
                        if let Some(pkid) = p.pkid {
                            sub.puback(pkid).await;
                        }
                    }
                    common::Recv::Closed => return,
                    _ => {}
                }
            }
        })
    };
    let mut publisher = common::Client::connect(nodes[0].client_addr, "mv-pub").await;
    let mut pkid: u16 = 0;
    let mut acked: Vec<Vec<u8>> = Vec::new();
    let mut seq = 0u32;
    let mut publish_one = async |publisher: &mut common::Client, acked: &mut Vec<Vec<u8>>| {
        seq += 1;
        pkid = pkid.wrapping_add(1).max(1);
        let payload = format!("m{seq}").into_bytes();
        publisher
            .publish("mv/t", &payload, QoS::AtLeastOnce, Some(pkid), Vec::new())
            .await;
        if let common::Recv::Packet(Packet::PubAck(ack)) =
            publisher.recv_bounded(Duration::from_secs(10)).await
        {
            assert_eq!(ack.pkid, pkid);
            acked.push(payload);
        }
    };
    // The subscription reaches a before the stream counts.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        publish_one(&mut publisher, &mut acked).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        if !received.lock().unwrap().is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "c's subscription never reached a"
        );
    }

    // The renewal: every node's certificate and key replaced, then every node reloaded.
    let marks: Vec<usize> = nodes.iter().map(log_len).collect();
    let established: Vec<usize> = nodes.iter().map(links_established).collect();
    for n in &nodes {
        let _ = pki.env_for(&n.id, SystemTime::now() + Duration::from_hours(24 * 7));
    }
    let renewed_at = SystemTime::now();
    for n in &nodes {
        let process = n.pid().expect("running").to_string();
        let sent = std::process::Command::new("kill")
            .args(["-HUP", &process])
            .status()
            .expect("run kill");
        assert!(sent.success());
    }

    // The lease group's epoch on each node, before the renewal (durable sessions on).
    let epochs = |nodes: &[ProcNode]| {
        let nodes: Vec<_> = nodes.iter().map(|n| n.health_addr).collect();
        async move {
            let mut out = Vec::new();
            for addr in nodes {
                let body = http_get(addr, "/metrics").await.unwrap_or_default();
                out.push(
                    body.lines()
                        .find(|l| l.starts_with("mqttd_lease_epoch "))
                        .map(str::to_string),
                );
            }
            out
        }
    };
    let epochs_before = epochs(&nodes).await;

    // Publish and watch the links through the old end and a few seconds past it. A poll a
    // node's health endpoint did not answer is counted: the no-gap claim rests on the rest.
    let mut polls = 0;
    let mut unanswered = 0;
    while SystemTime::now() < old_end + Duration::from_secs(5) {
        publish_one(&mut publisher, &mut acked).await;
        let view = survey(&nodes).await;
        unanswered += view.iter().filter(|v| v.is_none()).count();
        assert!(
            view.iter()
                .all(|v| v.is_none_or(|(members, links)| members == 3 && links == 2)),
            "a node was without a link or a member {:?} after the renewal; \
             (members, links) = {view:?}\n{}",
            renewed_at.elapsed().unwrap_or_default(),
            dump(&nodes)
        );
        polls += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        unanswered * 20 <= polls,
        "{unanswered} of {polls} polls went unanswered: too few to say there was no gap"
    );
    if durable {
        let after = epochs(&nodes).await;
        assert!(
            epochs_before.iter().all(Option::is_some),
            "{epochs_before:?}"
        );
        assert_eq!(
            epochs_before,
            after,
            "a lease epoch moved across the moves\n{}",
            dump(&nodes)
        );
    }

    // Every link moved, and before the old end.
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            links_established(n) >= established[i] + 2,
            "{} did not register both of its links again after the renewal\n{}",
            n.id,
            dump(&nodes)
        );
        let since = log_since(n, marks[i]);
        for gap in [
            "peer link lost",
            "peer declared dead",
            "peer link ended: a certificate",
        ] {
            assert!(
                !since.contains(gap),
                "{} logged `{gap}` after the renewal\n{}",
                n.id,
                dump(&nodes)
            );
        }
        for reason in ["peer-cert-expired", "own-cert-expired"] {
            assert_eq!(ended_for(n, reason).await, 0, "{}: {reason}", n.id);
        }
        for line in since.lines().filter(|l| l.contains("moving a peer link")) {
            eprintln!("{}: {line}", n.id);
        }
    }
    let moves: usize = nodes
        .iter()
        .zip(&marks)
        .map(|(n, from)| log_since(n, *from).matches("moving a peer link").count())
        .sum();
    assert!(moves >= 3, "three links, {moves} moves\n{}", dump(&nodes));

    // Nothing acknowledged was lost.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let missing: Vec<String> = {
            let got = received.lock().unwrap();
            acked
                .iter()
                .filter(|m| !got.contains(*m))
                .map(|m| String::from_utf8_lossy(m).into_owned())
                .collect()
        };
        if missing.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{} of {} acknowledged messages never reached the subscriber: {missing:?}\n{}",
            missing.len(),
            acked.len(),
            dump(&nodes)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!(
        "{moves} moves; {} acknowledged messages delivered; {polls} polls, {unanswered} \
         unanswered",
        acked.len()
    );
    reader.abort();
}
