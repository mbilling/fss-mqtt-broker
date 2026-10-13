//! Formation of a signed-gossip cluster **above ten nodes**, as real processes
//! (issue #971).
//!
//! Twelve spawned `mqttd` binaries on loopback, configured the way the scale rig
//! configures them (`bench/scale/bootstrap-cluster.sh`): a cluster CA, a peer
//! certificate per node (so gossip is signed per node, ADR 0022, and the peer bus is
//! mTLS), the shared gossip key, a founder with no seeds and every other node seeded
//! to two — then the founder restarted with seeds, as the rig "arms" it.
//!
//! On v1.1.0 this formation was a storm: outside the seeds, two nodes learned each
//! other's certificate only when one of them happened to send the other its every-16th
//! datagram, and until then each dropped everything the other sent (`cert-miss`),
//! suspected it, and often buried it. Twelve brokers logged 28-55 false suspicions and
//! over a hundred `cert-miss` drops EACH, and took a minute or more to settle (not at
//! all, some runs).
//!
//! The simulation (`mqtt-cluster/tests/swim_formation_sim.rs`) asserts the exact
//! property — no false suspicion at any size — in virtual time. This test is its
//! counterpart on real sockets and real certificates, so it asserts what does not
//! depend on how fast the runner is: the mesh becomes whole, and the `cert-miss`
//! counter of every broker stays within a handful. (A starved CI runner can make a
//! healthy broker miss an ack deadline; it cannot make one drop datagrams for want of
//! a certificate.) Run against a binary built without the change it fails on that
//! counter every time: 10-23 drops on a third of the brokers by the moment the mesh
//! is first whole.
//!
//! The second test here is about a member of that mesh that goes silent for two
//! seconds and comes back (issue #975): no broker may declare it dead. That one does
//! depend on the runner keeping time — a verdict is a matter of deadlines — so it is
//! `#[ignore]`d and runs in the nightly tier, beside the other slow cluster tests.

mod common;
mod proc_common;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use proc_common::*;

/// Above ten: where the rig's formations first took a minute or more.
const NODES: usize = 12;

/// `cert-miss` drops one broker may have counted by the time the mesh is first whole.
/// Measured: 0 on every broker. v1.1.0 at the same moment: 10-19 on a third of the
/// brokers, on its way to the 60-190 each that the storm ends at.
const FORMATION_CERT_MISS_BOUND: u64 = 3;

/// The same once the founder has been restarted. It comes back with an empty cache; a
/// member that has not yet heard of its new life may send it one fingerprint datagram,
/// which it drops and answers with a request for the certificate. So: at most about
/// one per member, on the founder alone. Measured: 0-2.
const RESTART_CERT_MISS_BOUND: u64 = NODES as u64;

/// The mesh must be whole within this long of the last process start. Measured: 8 s
/// with release binaries; the debug binaries of a loaded CI runner get a wide margin.
const SETTLE_BUDGET: Duration = Duration::from_secs(150);

/// One CA and one peer leaf per node id (CN = the node id, SAN = loopback, both EKUs).
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

    /// The peer-bus environment for node `cn`: the CA, its leaf and its key.
    fn env_for(&self, cn: &str) -> Vec<(String, String)> {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
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

/// The rig's seeding (`seeds_for`): the founder none; every other node the first two
/// other nodes' gossip addresses.
fn seeds_for(i: usize, swim: &[String]) -> String {
    if i == 0 {
        return String::new();
    }
    (0..swim.len())
        .filter(|j| *j != i)
        .take(2)
        .map(|j| swim[j].clone())
        .collect::<Vec<_>>()
        .join(",")
}

async fn build(root: &Path) -> Vec<ProcNode> {
    let pki = Pki::new(root);
    let peer_binds: Vec<SocketAddr> = (0..NODES)
        .map(|_| SocketAddr::from(([127, 0, 0, 1], free_tcp_port())))
        .collect();
    let swim: Vec<String> = (0..NODES)
        .map(|_| format!("127.0.0.1:{}", free_udp_port()))
        .collect();
    let mut nodes = Vec::new();
    for i in 0..NODES {
        // Zero-padded so the ids sort as the nodes are numbered.
        let id = format!("form{}-{:02}", std::process::id(), i + 1);
        let data_dir = root.join(&id);
        std::fs::create_dir_all(&data_dir).unwrap();
        let (relay_addr, relay, relay_abort) = spawn_relay(peer_binds[i]).await;
        let mut extra_env = pki.env_for(&id);
        // Membership and the mesh are the subject; the durable plane (a Raft group per
        // node, a dozen of them on one runner) is not, and the rig forms its
        // non-durable lanes exactly so.
        extra_env.push(("MQTTD_DURABLE_SESSIONS".into(), "0".into()));
        extra_env.push(("RUST_LOG".into(), "warn".into()));
        nodes.push(ProcNode {
            log_path: root.join(format!("{id}.log")),
            id,
            child: None,
            data_dir,
            client_addr: SocketAddr::from(([127, 0, 0, 1], free_tcp_port())),
            peer_bind: peer_binds[i],
            swim_bind: swim[i].clone(),
            health_addr: SocketAddr::from(([127, 0, 0, 1], free_tcp_port())),
            swim_seeds: seeds_for(i, &swim),
            relay_addr,
            relay,
            relay_abort,
            file_size_limit_blocks: None,
            extra_env,
            binary: PathBuf::from(env!("CARGO_BIN_EXE_mqttd")),
        });
    }
    nodes
}

/// What every broker reports: `(members, peer links, cert-miss drops)`; `None` for a
/// broker whose metrics endpoint did not answer.
async fn survey(nodes: &[ProcNode]) -> Vec<Option<(u64, u64, u64)>> {
    let mut out = Vec::new();
    for n in nodes {
        let members = n.metric("mqttd_cluster_members").await;
        let links = n.metric("mqttd_peer_links").await;
        // An untouched label set is not rendered: absent means zero so far.
        let miss = n
            .metric("mqttd_gossip_rejected_total{reason=\"cert-miss\"}")
            .await
            .unwrap_or(0);
        out.push(members.zip(links).map(|(m, l)| (m, l, miss)));
    }
    out
}

/// Wait until every broker holds all `NODES` members and a link to each of the others.
/// Fails — with every broker's figures and its warnings — if a broker exits or the
/// budget runs out.
async fn await_whole_mesh(nodes: &mut [ProcNode], phase: &str) -> Duration {
    let started = Instant::now();
    loop {
        for n in nodes.iter_mut() {
            if let Some(status) = n.exited() {
                panic!(
                    "{phase}: {} exited ({status})\n{}",
                    n.id,
                    log_tail(&n.log_path)
                );
            }
        }
        let view = survey(nodes).await;
        let whole = (NODES as u64, NODES as u64 - 1);
        if view
            .iter()
            .all(|v| v.is_some_and(|(m, l, _)| (m, l) == whole))
        {
            return started.elapsed();
        }
        if started.elapsed() >= SETTLE_BUDGET {
            for (n, v) in nodes.iter().zip(&view) {
                eprintln!(
                    "---- {} (members, links, cert-miss) = {v:?} ----\n{}",
                    n.id,
                    log_notables(&n.log_path, 40)
                );
            }
            panic!(
                "{phase}: the mesh of {NODES} was not whole within {}s — every broker must \
                 report {NODES} members and {} peer links (figures and warnings above)",
                SETTLE_BUDGET.as_secs(),
                NODES - 1
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn assert_no_certificate_storm(nodes: &[ProcNode], phase: &str, bound: u64) {
    let view = survey(nodes).await;
    for (n, v) in nodes.iter().zip(&view) {
        let (_, _, miss) = v.unwrap_or_else(|| panic!("{phase}: {} stopped answering", n.id));
        assert!(
            miss <= bound,
            "{phase}: {} has dropped {miss} datagrams as cert-miss (bound {bound}). \
             (members, links, cert-miss) of every broker: {view:?}",
            n.id
        );
    }
}

/// Start the brokers as the rig does and wait for the mesh: the founder first, then
/// the followers one after another, each started once the one before it answers — the
/// stagger of a scripted bring-up, which is what makes two non-seed nodes meet through
/// gossip rather than at once. Returns how long the mesh took after the last start.
async fn form(nodes: &mut [ProcNode]) -> Duration {
    for n in nodes.iter_mut() {
        n.spawn();
        let started = Instant::now();
        while n.readyz().await.is_none() {
            assert!(
                n.exited().is_none() && started.elapsed() < Duration::from_secs(45),
                "{} did not come up\n{}",
                n.id,
                log_tail(&n.log_path)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    await_whole_mesh(nodes, "formation").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twelve_signed_brokers_form_a_whole_mesh_without_a_certificate_storm() {
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build(disk.path()).await;

    let formed = form(&mut nodes).await;
    assert_no_certificate_storm(&nodes, "formation", FORMATION_CERT_MISS_BOUND).await;

    // "Arm" the founder: a graceful stop and a restart WITH seeds — a new process
    // life with an empty certificate cache, in a cluster where everyone remembers
    // having shown the old one their certificate.
    nodes[0].terminate().await;
    nodes[0].swim_seeds = format!("{},{}", nodes[1].swim_bind, nodes[NODES - 1].swim_bind);
    nodes[0].spawn();
    let rearmed = await_whole_mesh(&mut nodes, "after the founder's restart").await;
    assert_no_certificate_storm(
        &nodes,
        "after the founder's restart",
        RESTART_CERT_MISS_BOUND,
    )
    .await;

    eprintln!(
        "mesh of {NODES} whole {formed:?} after the last start, and {rearmed:?} after the \
         founder's restart; (members, links, cert-miss) per broker: {:?}",
        survey(&nodes).await
    );
    for n in &mut nodes {
        n.kill().await;
    }
}

/// How long the member is stopped: longer than it takes to be suspected (a probe and
/// its indirect round, 0.5 s), longer than the suspicion window's floor (1.5 s, with
/// three suspecters), shorter than its ceiling (4 s).
const STOP: Duration = Duration::from_secs(2);

/// How many times it is stopped. One stop is noticed by its peers only if one of them
/// happens to probe it early enough in those two seconds; the test needs at least one
/// that was.
const EPISODES: usize = 8;

/// A verdict about the stopped member, if one is coming, comes within the suspicion
/// window (4 s) and the last probe's ack timeout (0.25 s) of the last suspicion. The
/// mesh is "quiet" once no broker has reported a suspect for this long.
const QUIET: Duration = Duration::from_secs(6);

fn signal(pid: u32, sig: &str) {
    let ok = std::process::Command::new("kill")
        .args([sig, &pid.to_string()])
        .status()
        .is_ok_and(|s| s.success());
    assert!(ok, "kill {sig} {pid} failed");
}

/// The membership transitions the brokers have logged so far (WARN, `cluster.rs`):
/// `(SUSPECT raised about node `about` by any other broker, every DEAD line as
/// "<who logged it>: <line>")`.
fn verdicts(nodes: &[ProcNode], about: usize) -> (usize, Vec<String>) {
    // The id alone: the log is coloured, so `peer=<id>` is not contiguous in it. Ids
    // are zero-padded, so one is never a prefix of another. The line names one member
    // only — the one it is about, never who suspected it (`cluster.rs`) — so a line
    // with this id in it is a suspicion OF this broker, logged by the broker whose view
    // changed.
    let subject = nodes[about].id.clone();
    let mut suspected = 0;
    let mut dead = Vec::new();
    for (i, n) in nodes.iter().enumerate() {
        let log = std::fs::read_to_string(&n.log_path).unwrap_or_default();
        for line in log.lines() {
            if line.contains("membership: peer DEAD") {
                dead.push(format!("{}: {line}", n.id));
            }
            if i != about && line.contains("membership: peer SUSPECT") && line.contains(&subject) {
                suspected += 1;
            }
        }
    }
    (suspected, dead)
}

/// Wait until the mesh is whole and no broker has held any member suspect or dead for
/// [`QUIET`]: every suspicion raised has by then been refuted or has run its course.
async fn await_quiet(nodes: &mut [ProcNode], phase: &str) {
    let started = Instant::now();
    let mut quiet_since: Option<Instant> = None;
    loop {
        await_whole_mesh(nodes, phase).await;
        let mut calm = true;
        for n in nodes.iter() {
            for state in ["suspect", "dead"] {
                let series = format!("mqttd_members{{state=\"{state}\"}}");
                calm &= n.metric(&series).await == Some(0);
            }
        }
        if !calm {
            quiet_since = None;
        }
        let since = *quiet_since.get_or_insert_with(Instant::now);
        if calm && since.elapsed() >= QUIET {
            return;
        }
        assert!(
            started.elapsed() < SETTLE_BUDGET,
            "{phase}: some broker still holds a member suspect or dead after {}s\n{:?}",
            SETTLE_BUDGET.as_secs(),
            verdicts(nodes, 0).1
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Issue #975, on real processes: one of twelve brokers is stopped (`SIGSTOP`) for
/// two seconds and resumed. Its peers notice — at least one logs it SUSPECT — and no
/// broker declares it, or anyone, DEAD: when a suspicion's window runs out the broker
/// asks the suspect once more, directly, and by then it answers (ADR 0016's amendment
/// of 2026-10-13). With that last probe off the same stop ends in a DEAD on some
/// broker in about half the simulated schedules at this size, which then stands for
/// the tombstone's 30 s.
///
/// The simulation holds the bound over seeds, in virtual time
/// (`a_member_cut_off_for_up_to_two_seconds_is_not_declared_dead`); this is the
/// counterpart on real sockets, clocks and certificates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing-sensitive on a loaded runner: twelve brokers and a verdict that is a \
            matter of deadlines; runs in the nightly tier"]
async fn a_broker_stopped_for_two_seconds_is_declared_dead_by_no_one() {
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build(disk.path()).await;
    form(&mut nodes).await;
    await_quiet(&mut nodes, "after formation").await;

    // Neither the founder nor a seed: a member the others know through gossip.
    let victim = NODES / 2;
    let pid = nodes[victim].pid().expect("the victim is running");
    let (_, dead_before) = verdicts(&nodes, victim);
    let mut noticed = 0;
    for episode in 1..=EPISODES {
        let (suspected_before, _) = verdicts(&nodes, victim);
        signal(pid, "-STOP");
        // SETTLE(swim-stop-two-seconds): the fault IS a duration — the broker stays stopped this
        // long, and what its peers conclude about a silence of that length is the subject.
        tokio::time::sleep(STOP).await;
        signal(pid, "-CONT");
        await_quiet(&mut nodes, &format!("after stop {episode}")).await;

        let (suspected, dead) = verdicts(&nodes, victim);
        assert_eq!(
            dead.len(),
            dead_before.len(),
            "stop {episode}: a broker stopped for {STOP:?} was declared DEAD (or declared \
             someone DEAD):\n{}",
            dead.join("\n")
        );
        if suspected > suspected_before {
            noticed += 1;
        }
    }
    assert!(
        noticed > 0,
        "none of {EPISODES} stops of {STOP:?} was noticed by any broker: the test proved nothing"
    );
    eprintln!(
        "{} stopped {EPISODES} times for {STOP:?}: suspected in {noticed} of them, declared \
         DEAD in none",
        nodes[victim].id
    );
    for n in &mut nodes {
        n.kill().await;
    }
}
