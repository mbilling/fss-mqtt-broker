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

/// Where a test's brokers keep their files, logs included: a temporary directory,
/// removed when the test ends — or, when `MQTTD_SWIM_LOG_DIR` is set, a directory
/// named for the test under it, which stays. The nightly job sets it and uploads the
/// `*.log` files when a test fails: a verdict that should not have been reached is
/// explained by the brokers' own lines around it, and those used to go with the
/// temporary directory.
struct Root {
    path: PathBuf,
    _temporary: Option<tempfile::TempDir>,
}

impl Root {
    fn for_test(name: &str) -> Self {
        if let Some(kept) = std::env::var_os("MQTTD_SWIM_LOG_DIR") {
            let path = PathBuf::from(kept).join(name);
            // A fresh one each run: the brokers append to their logs.
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("the log directory");
            return Self {
                path,
                _temporary: None,
            };
        }
        let temporary = tempfile::tempdir().expect("tempdir");
        Self {
            path: temporary.path().to_path_buf(),
            _temporary: Some(temporary),
        }
    }
}

async fn build(root: &Path, count: usize) -> Vec<ProcNode> {
    let pki = Pki::new(root);
    let peer_binds: Vec<SocketAddr> = (0..count)
        .map(|_| SocketAddr::from(([127, 0, 0, 1], free_tcp_port())))
        .collect();
    let swim: Vec<String> = (0..count)
        .map(|_| format!("127.0.0.1:{}", free_udp_port()))
        .collect();
    let mut nodes = Vec::new();
    for i in 0..count {
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

/// Wait until every broker holds every broker as a member and a link to each of the
/// others. Fails — with every broker's figures and its warnings — if a broker exits or
/// the budget runs out.
async fn await_whole_mesh(nodes: &mut [ProcNode], phase: &str) -> Duration {
    let started = Instant::now();
    let count = nodes.len();
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
        let whole = (count as u64, count as u64 - 1);
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
                "{phase}: the mesh of {count} was not whole within {}s — every broker must \
                 report {count} members and {} peer links (figures and warnings above)",
                SETTLE_BUDGET.as_secs(),
                count - 1
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
    let disk = Root::for_test("formation");
    let mut nodes = build(&disk.path, NODES).await;

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
/// its indirect round, 0.5 s), longer than the suspicion window's floor was up to 1.2.0
/// (1.5 s, with three suspecters; 2.5 s since 0087-T10b), shorter than its ceiling
/// (4 s).
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
/// (`a_member_cut_off_for_up_to_three_seconds_is_not_declared_dead`); this is the
/// counterpart on real sockets, clocks and certificates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing-sensitive on a loaded runner: twelve brokers and a verdict that is a \
            matter of deadlines; runs in the nightly tier"]
async fn a_broker_stopped_for_two_seconds_is_declared_dead_by_no_one() {
    let disk = Root::for_test("suspect-stopped");
    let mut nodes = build(&disk.path, NODES).await;
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

/// How long the PROBER is stopped with its last probe out: six ack timeouts, so that a
/// broker which trusted its clock on waking would find the probe long given up on.
const PROBER_STOP: Duration = Duration::from_millis(1500);

/// How many times. One is enough to show it; a few, because an episode proves nothing
/// when this test is too slow to stop the prober inside the probe's 250 ms.
const PROBER_EPISODES: usize = 4;

/// A step of an episode must be observed within this long: the suspect is suspected a
/// probe period and two ack timeouts after it stops, and asked once more a window (4 s,
/// one suspecter) after that.
const STEP_BUDGET: Duration = Duration::from_secs(30);

/// One of the broker's last-probe counters (`sent`, `spared`, `silent`); an untouched
/// one is not rendered and is zero.
async fn last_probes(node: &ProcNode, outcome: &str) -> u64 {
    node.metric(&format!(
        "mqttd_swim_last_probes_total{{outcome=\"{outcome}\"}}"
    ))
    .await
    .unwrap_or(0)
}

/// Whether process `pid` is stopped (`T` in `ps`), which is when it can no longer write
/// to its log.
fn is_stopped(pid: u32) -> bool {
    std::process::Command::new("ps")
        .args(["-o", "state=", "-p", &pid.to_string()])
        .output()
        .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).trim().starts_with('T'))
}

/// The `peer DEAD` lines in `log`.
fn dead_lines(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|l| l.contains("membership: peer DEAD"))
        .collect()
}

/// The ack timeout of a last probe on a healthy node. A node that has lately been
/// suspected itself waits longer, never less (ADR 0016 §2).
const LAST_PROBE_ACK_TIMEOUT: Duration = Duration::from_millis(250);

/// One episode of the test below: steps 1 to 4, and what the prober did on waking.
/// Returns whether the prober was stopped in time — `false` sets the episode aside.
///
/// "In time" is decided on what the SWIM task itself did and on this test's own
/// clock, not on a log line (the membership lines are written by another task, a
/// moment after the verdict):
///
/// - the probe was sent after the last scrape that did not show it began, and its
///   deadline is at least an ack timeout after it was sent; so if the prober is seen
///   stopped within an ack timeout of that scrape, it was stopped with the probe out;
/// - and whatever the clock says, a probe counted `spared` was out when the prober
///   stopped: the suspect was only resumed afterwards, so its answer was read after.
///
/// An episode that is neither is one this test was too slow for: the probe ran out on
/// a suspect that was still stopped, rightly.
async fn stop_the_prober_on_its_last_probe(
    nodes: &mut [ProcNode],
    prober: usize,
    suspect: usize,
    episode: usize,
) -> bool {
    let prober_pid = nodes[prober].pid().expect("the prober is running");
    let suspect_pid = nodes[suspect].pid().expect("the suspect is running");
    let prober_log = nodes[prober].log_path.clone();
    let sent_before = last_probes(&nodes[prober], "sent").await;
    let spared_before = last_probes(&nodes[prober], "spared").await;
    let silent_before = last_probes(&nodes[prober], "silent").await;

    // 1. The suspect goes silent, and in time the prober asks it once more.
    signal(suspect_pid, "-STOP");
    let started = Instant::now();
    let mut not_sent_at = Instant::now();
    loop {
        let scrape_began = Instant::now();
        if last_probes(&nodes[prober], "sent").await != sent_before {
            break;
        }
        not_sent_at = scrape_began;
        assert!(
            started.elapsed() < STEP_BUDGET,
            "episode {episode}: the prober sent no last probe in {STEP_BUDGET:?}\n{}",
            log_notables(&prober_log, 40)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // 2. The prober is stopped with that probe out, and writes nothing from here.
    signal(prober_pid, "-STOP");
    let started = Instant::now();
    while !is_stopped(prober_pid) {
        assert!(
            started.elapsed() < STEP_BUDGET,
            "episode {episode}: the prober did not stop"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stopped_within = not_sent_at.elapsed();
    let written_before_the_stop = std::fs::read(&prober_log).unwrap_or_default().len();

    // 3. The suspect runs again, and answers what queued while it was stopped.
    signal(suspect_pid, "-CONT");
    let started = Instant::now();
    while nodes[suspect].readyz().await.is_none() {
        assert!(
            started.elapsed() < STEP_BUDGET,
            "episode {episode}: the suspect did not answer after it was resumed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // SETTLE(swim-prober-stop): the fault IS a duration — the prober stays stopped for six
    // ack timeouts with its last probe out, and what it concludes on waking is the subject.
    tokio::time::sleep(PROBER_STOP).await;
    signal(prober_pid, "-CONT");
    await_quiet(nodes, &format!("after prober stop {episode}")).await;

    let spared = last_probes(&nodes[prober], "spared").await > spared_before;
    let silent = last_probes(&nodes[prober], "silent").await > silent_before;
    if stopped_within >= LAST_PROBE_ACK_TIMEOUT && !spared {
        eprintln!(
            "episode {episode}: the prober was seen stopped {stopped_within:?} after its last \
             probe may have gone out, and the probe ran out first; set aside"
        );
        return false;
    }
    let log = std::fs::read(&prober_log).unwrap_or_default();
    let after = String::from_utf8_lossy(&log[written_before_the_stop.min(log.len())..]);
    assert!(
        !silent && spared,
        "episode {episode}: stopped for {PROBER_STOP:?} within {stopped_within:?} of sending \
         its last probe, the prober woke and gave up on a suspect that had answered \
         (spared: {spared}, silent: {silent}). Its log since the stop:\n{}",
        dead_lines(&after).join("\n")
    );
    let stalled_ms = nodes[prober]
        .metric("mqttd_swim_stalled_milliseconds_total")
        .await
        .unwrap_or(0);
    // A stall is the gap between two wake-ups less the tick that was due anyway, and
    // the stop began part-way through a tick: two ticks short of the stop, at most.
    let nearly_the_stop = u64::try_from(PROBER_STOP.as_millis()).expect("small") - 200;
    assert!(
        stalled_ms >= nearly_the_stop,
        "episode {episode}: stopped for {PROBER_STOP:?}, the prober counted {stalled_ms} ms \
         of stalls"
    );
    true
}

/// The stalled PROBER, on real processes (issue #1002): a broker whose last direct
/// probe of a suspect is out is stopped (`SIGSTOP`) for six ack timeouts, while the
/// suspect answers. When it wakes, the tick that would give up on the probe is overdue
/// and the suspect's `Ack` is in its socket. It must not declare the suspect DEAD.
///
/// Two brokers, so that there is one prober and its probe is the only one: with more,
/// every other broker's own last probe of the stopped suspect would run out too, and
/// rightly.
///
/// Every step waits on something a broker reports, except the stop itself:
///
/// 1. the suspect is stopped; the prober suspects it and, a window later, sends its
///    last probe — seen as `mqttd_swim_last_probes_total{outcome="sent"}` going up;
/// 2. the prober is stopped at once, and the time is taken once `ps` shows it stopped;
/// 3. the suspect is resumed, and answers what queued on its socket, the probe included;
/// 4. the prober stays stopped for [`PROBER_STOP`], is resumed, and the pair settles.
///
/// Step 2 races the probe's 250 ms: if this test is too slow, the prober gives up on a
/// suspect that really was silent, and rightly declares it DEAD. Such an episode is
/// recognised from the prober's own counters and this test's clock
/// ([`stop_the_prober_on_its_last_probe`]) and set aside, not failed. A probe that was
/// out when the prober stopped and is counted `silent` is the failure. At least one
/// episode must have got the prober stopped in time.
///
/// Before the driver read its socket ahead of a tick, the waking prober took the tick
/// or the `Ack` at random: half of such episodes ended in a DEAD. The driver's own test of this
/// (`a_prober_stalled_past_the_ack_timeout_reads_the_ack_before_it_gives_up`, every
/// tier) decides the order exactly; this is the same thing with a kernel in between.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing-sensitive on a loaded runner: the prober has to be stopped within its \
            last probe's 250 ms; runs in the nightly tier"]
async fn a_prober_stopped_with_its_last_probe_out_does_not_declare_the_suspect_dead() {
    let disk = Root::for_test("prober-stopped");
    let mut nodes = build(&disk.path, 2).await;
    form(&mut nodes).await;
    await_quiet(&mut nodes, "after formation").await;
    let (prober, suspect) = (0, 1);

    let mut in_time = 0;
    for episode in 1..=PROBER_EPISODES {
        if stop_the_prober_on_its_last_probe(&mut nodes, prober, suspect, episode).await {
            in_time += 1;
        }
    }
    let suspect_log = std::fs::read_to_string(&nodes[suspect].log_path).unwrap_or_default();
    assert!(
        dead_lines(&suspect_log).is_empty(),
        "the suspect declared the prober DEAD:\n{}",
        dead_lines(&suspect_log).join("\n")
    );
    assert!(
        in_time > 0,
        "in none of {PROBER_EPISODES} episodes was the prober stopped inside its last \
         probe's ack timeout: the test proved nothing"
    );
    eprintln!(
        "{} stopped for {PROBER_STOP:?} with its last probe out, {in_time} times of \
         {PROBER_EPISODES}: declared DEAD in none",
        nodes[prober].id
    );
    for n in &mut nodes {
        n.kill().await;
    }
}
