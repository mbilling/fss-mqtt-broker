//! The acceptance criterion of issue #249, over REAL PROCESSES: export from a LIVE
//! cluster, restore into a FRESH one, and prove that sessions, retained values and
//! previously-ACKED facts survive ([ADR 0062](../../docs/adr/0062-online-backup-and-restore.md)).
//!
//! Why this tier and not a unit fixture. The claim under test is an operational one — "you
//! can back up a running 24/7 cluster and rebuild it" — and every part of it that could be
//! faked in-process is exactly the part that matters:
//!
//! - the export is taken by `mqttd --backup`, a SEPARATE process signalling the running
//!   broker, while clients stay connected and nothing is stopped;
//! - the stores are the production ones, held under `redb`'s exclusive `flock`, so a test
//!   that reads them any other way than through the running node could not run at all;
//! - the restore is configured through the documented `MQTTD_RESTORE_FROM` surface and runs
//!   inside `main.rs`, before any client listener binds;
//! - the oracle is what a CLIENT sees: `session_present`, delivered payloads, retained
//!   values, and a QoS-2 duplicate that must not be re-delivered.
//!
//! The three facts a backup exists to preserve, and where each is asserted:
//! **acked messages** (an acked message that does not come back makes the whole feature a
//! lie), **retained values with their properties**, and **acknowledged QoS-2 flows** (the
//! dedup window's acked bit, issue #238 — the fact that is invisible unless you re-send).

mod common;
mod proc_common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use mqtt_codec::{packet::Publish, Packet, Properties, Property, QoS};
use proc_common::*;

/// One spawned cluster at a time (this test stands up two, in sequence).
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The completed exports in `dir`, sorted.
fn exports(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("mqttd-backup") && n.ends_with(".ndjson"))
        })
        .collect();
    files.sort();
    files
}

/// Run `mqttd --backup --pid <pid>` against a live node, exactly as a cron job or a
/// `kubectl exec` would, and return its stdout+stderr for the failure message.
fn take_backup(pid: u32, dir: &Path, timeout_secs: u64) -> (bool, String) {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_mqttd"))
        .args(["--backup", "--pid", &pid.to_string(), "--timeout"])
        .arg(timeout_secs.to_string())
        .env("MQTTD_BACKUP_DIR", dir)
        // The CLI loads the config only to learn where to WATCH; a data dir keeps that
        // load valid (durable is on by default and refuses an ephemeral config).
        // (`--backup` never opens a store; the path only has to satisfy config validation.)
        .env("MQTTD_DATA_DIR", dir.join("cli-unused"))
        .output()
        .expect("run mqttd --backup");
    let text = format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

/// Every `restore`/`backup` line from a node's log — the diagnosis a failure needs without
/// a re-run (the temp dirs vanish with the unwind).
fn restore_lines(nodes: &[ProcNode]) -> String {
    nodes
        .iter()
        .map(|n| {
            let text = std::fs::read_to_string(&n.log_path).unwrap_or_default();
            let lines: Vec<&str> = text
                .lines()
                .filter(|l| l.contains("restore") || l.contains("backup") || l.contains("ERROR"))
                .collect();
            format!("---- {} ----\n{}", n.id, lines.join("\n"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Wait for one node's `/readyz` to report ready (the operator's own signal).
async fn wait_ready(node: &ProcNode, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some((ready, _, _)) = node.readyz().await {
            if ready {
                return true;
            }
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Publish one retained value at `QoS` 1 with v5 user properties, and wait for its PUBACK.
async fn publish_retained_v5(client: &mut common::Client, topic: &str, payload: &[u8], pkid: u16) {
    client
        .send(&Packet::Publish(Publish {
            properties: Properties(vec![
                Property::UserProperty("unit".to_string(), "celsius".to_string()),
                Property::ContentType("application/json".to_string()),
            ]),
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: true,
            topic: topic.into(),
            pkid: Some(pkid),
            payload: bytes::Bytes::copy_from_slice(payload),
        }))
        .await;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match client
            .recv_bounded(deadline.saturating_duration_since(Instant::now()))
            .await
        {
            common::Recv::Packet(Packet::PubAck(a)) if a.pkid == pkid => return,
            common::Recv::Packet(_) => {}
            common::Recv::Quiet | common::Recv::Closed => {
                panic!("retained publish to {topic} was never acked")
            }
        }
    }
}

/// Subscribe as a fresh clean session and collect the retained `PUBLISH`es that arrive.
async fn retained_snapshot(addr: std::net::SocketAddr, id: &str, filter: &str) -> Vec<Publish> {
    let (mut client, _) =
        common::Client::connect_v311_within(addr, id, true, Duration::from_secs(10))
            .await
            .expect("retained reader connects");
    client.subscribe(1, filter, QoS::AtLeastOnce).await;
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client
            .recv_bounded(deadline.saturating_duration_since(Instant::now()))
            .await
        {
            common::Recv::Packet(Packet::Publish(p)) => {
                if let Some(pkid) = p.pkid {
                    client.puback(pkid).await;
                }
                seen.push(p);
            }
            common::Recv::Packet(_) => {}
            common::Recv::Quiet | common::Recv::Closed => return seen,
        }
    }
}

/// **The acceptance criterion.** A live 3-node durable cluster is backed up node by node
/// with clients connected; a brand-new 3-node cluster is started with
/// `MQTTD_RESTORE_FROM` pointed at those files; and every fact the broker had
/// ACKNOWLEDGED is then verified through the client protocol.
// One long linear scenario: build the state, back it up, rebuild, then verify each fact in
// the order a client would meet it. Splitting it would hide the sequence, which IS the test.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_cluster_export_restores_sessions_retained_and_acked_facts() {
    let _serial = SERIAL.lock().await;
    let seed = 249u64;
    let root = tempfile::tempdir().expect("test root");
    let backups = root.path().join("backups");
    std::fs::create_dir_all(&backups).unwrap();

    // ---------------- the live cluster ----------------
    let source_root = root.path().join("source");
    std::fs::create_dir_all(&source_root).unwrap();
    let mut nodes = build_topology(seed, &source_root).await;
    for node in &mut nodes {
        node.extra_env = vec![(
            "MQTTD_BACKUP_DIR".to_string(),
            backups.to_string_lossy().into_owned(),
        )];
        node.spawn();
    }
    wait_all_ready(&mut nodes, seed).await;

    let sub_id = format!("psub-{seed}");
    let topic = format!("pr/{seed}/queue");
    let retained_topic = format!("rt/{seed}/temp");
    let q2_id = format!("q2pub-{seed}");
    let q2_topic = format!("pr/{seed}/q2");
    // A retained topic the durable subscriber's filter MATCHES — the shape that made the
    // restore inject messages. Retained topics are where deployments keep desired state, and
    // durable wildcard subscribers are how devices read it, so this is the common case, not a
    // corner: `cfg/#` over `cfg/<n>/desired`.
    let cfg_filter = format!("cfg/{seed}/#");
    let cfg_topic = format!("cfg/{seed}/desired");

    // A persistent subscriber, subscribed at QoS 1 on two topics, then taken OFFLINE so
    // everything published to it queues durably.
    {
        let (mut sub, present) = common::Client::connect_v311_within(
            nodes[0].client_addr,
            &sub_id,
            false,
            Duration::from_secs(10),
        )
        .await
        .expect("subscriber connects");
        assert!(!present, "a brand-new session is not present");
        for filter in [&topic, &q2_topic, &cfg_filter] {
            let ack = sub.subscribe(1, filter, QoS::AtLeastOnce).await;
            assert!(
                ack.return_codes.iter().all(|c| *c != 0x80),
                "durable SUBSCRIBE to {filter} was refused"
            );
        }
        sub.disconnect().await;
    }

    // Acked QoS 1 publishes — the hard obligations. Each ack means "durable, cluster-wide"
    // (ADR 0018), which is exactly the promise a backup must carry across a restore.
    let mut owed: Vec<Vec<u8>> = Vec::new();
    for i in 0..6u32 {
        let via = usize::try_from(i).unwrap() % nodes.len();
        let payload = format!("m-{seed}-{i}").into_bytes();
        let (mut publisher, _) = common::Client::connect_v311_within(
            nodes[via].client_addr,
            &format!("pub-{seed}-{i}"),
            true,
            Duration::from_secs(10),
        )
        .await
        .expect("publisher connects");
        publisher
            .publish(&topic, &payload, QoS::AtLeastOnce, Some(7), vec![])
            .await;
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut acked = false;
        loop {
            match publisher
                .recv_bounded(deadline.saturating_duration_since(Instant::now()))
                .await
            {
                common::Recv::Packet(Packet::PubAck(a)) if a.pkid == 7 => {
                    acked = true;
                    break;
                }
                common::Recv::Packet(_) => {}
                common::Recv::Quiet | common::Recv::Closed => break,
            }
        }
        assert!(
            acked,
            "publish {i} was not acked; the obligation ledger needs it"
        );
        owed.push(payload);
        publisher.disconnect().await;
    }

    // A retained value with v5 application properties (ADR 0030): the properties are part
    // of the value, so a restore that drops them has lost data an operator will notice.
    {
        let mut rpub =
            common::Client::connect_v5_ok(nodes[1].client_addr, &format!("rpub-{seed}")).await;
        publish_retained_v5(&mut rpub, &retained_topic, b"{\"t\":21}", 9).await;
        // And one on a topic the OFFLINE durable subscriber matches. A retained publish is
        // also an ordinary publish, so exactly ONE copy queues for that session — a fact the
        // export carries, and the restore must reproduce exactly once, not once per node.
        publish_retained_v5(&mut rpub, &cfg_topic, b"{\"mode\":\"eco\"}", 10).await;
        rpub.disconnect().await;
    }
    owed.push(b"{\"mode\":\"eco\"}".to_vec());

    // An inbound QoS 2 flow left HELD-ACKED: the broker released the PUBREC (so it promised
    // exactly-once) and the client never sent PUBREL. That acked bit is the whole of issue
    // #238's QoS-2 half, and it is invisible unless a duplicate is re-sent later.
    {
        let (mut q2, _) = common::Client::connect_v311_within(
            nodes[2].client_addr,
            &q2_id,
            false,
            Duration::from_secs(10),
        )
        .await
        .expect("qos2 publisher connects");
        q2.publish(
            &q2_topic,
            b"exactly-once",
            QoS::ExactlyOnce,
            Some(11),
            vec![],
        )
        .await;
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut pubrec = false;
        loop {
            match q2
                .recv_bounded(deadline.saturating_duration_since(Instant::now()))
                .await
            {
                common::Recv::Packet(Packet::PubRec(r)) if r.pkid == 11 => {
                    pubrec = true;
                    break;
                }
                common::Recv::Packet(_) => {}
                common::Recv::Quiet | common::Recv::Closed => break,
            }
        }
        assert!(pubrec, "the QoS 2 PUBREC never arrived");
        // No PUBREL: the flow stays half-open across the backup, deliberately.
        q2.disconnect().await;
    }

    // ---------------- the online backup, with clients connected ----------------
    // A live connection stays open for the whole export, so nothing here is a
    // stop-the-node snapshot in disguise.
    let (mut live, _) = common::Client::connect_v311_within(
        nodes[0].client_addr,
        &format!("live-{seed}"),
        true,
        Duration::from_secs(10),
    )
    .await
    .expect("a live client stays connected across the backup");

    for node in &nodes {
        let pid = node.pid().expect("node is running");
        let (ok, output) = take_backup(pid, &backups, 60);
        assert!(
            ok,
            "mqttd --backup against {} failed:\n{output}\n---- node log ----\n{}",
            node.id,
            log_tail(&node.log_path)
        );
    }
    let files = exports(&backups);
    assert_eq!(
        files.len(),
        nodes.len(),
        "one export per node (a cluster backup is the SET of per-node exports); found {files:?}"
    );
    // The live client is still there: the export did not disturb it.
    live.publish(
        &format!("pr/{seed}/live"),
        b"still-here",
        QoS::AtMostOnce,
        None,
        vec![],
    )
    .await;
    live.disconnect().await;

    // The owner binding (ADR 0031) travels in the file. With the harness running
    // `MQTTD_ALLOW_ANONYMOUS`, the principal IS `anonymous` — the point of the assertion is
    // that the field is exported at all, since a record with no owner is adopted by its
    // next claimant. The refusal itself is pinned by the unit test in `backup.rs`.
    let all_text: String = files
        .iter()
        .map(|f| std::fs::read_to_string(f).unwrap())
        .collect();
    assert!(
        all_text.contains(&format!("\"client\":\"{sub_id}\"")),
        "the subscriber's session must be in some node's export"
    );
    assert!(
        all_text.contains("\"owner\":\"anonymous\""),
        "the session's owning identity must be exported (ADR 0031)"
    );
    assert!(
        all_text.contains("\"acked\":true"),
        "the QoS-2 dedup window's ACKED bit must be exported (issue #238)"
    );
    // The queued, ACKED messages are in the files — checked here, before the restore, so a
    // failure downstream cannot be blamed on the exporter.
    let queued_total: u64 = all_text
        .split("\"queued\":")
        .skip(1)
        .filter_map(|t| t.split([',', '}']).next())
        .filter_map(|t| t.trim().parse::<u64>().ok())
        .sum();
    assert!(
        queued_total >= owed.len() as u64,
        "the exports must carry every acked queued message: trailers report {queued_total} \
         queued, {} were acked. Files:\n{}",
        owed.len(),
        files
            .iter()
            .map(|f| {
                let t = std::fs::read_to_string(f).unwrap_or_default();
                format!("== {} ==\n{}", f.display(), &t[..t.len().min(4000)])
            })
            .collect::<Vec<_>>()
            .join("\n")
    );

    // Every retained record carries its `(epoch, offset)` convergence token — the evidence
    // the import's recency rule reads. Asserted on files written by real nodes because a rule
    // that never gets data is dead code, and the failure mode it exists to stop (a restore
    // rolling a retained topic back to the highest-sorting NODE ID's copy) would come back
    // silently through the file-time fallback.
    {
        let plan = mqttd::backup::load(&backups).expect("the export set loads");
        assert!(
            !plan.retained.is_empty(),
            "the set must carry the retained values"
        );
        let untokened: Vec<&str> = plan
            .retained
            .iter()
            .filter(|r| r.token.is_none())
            .map(|r| r.topic.as_str())
            .collect();
        assert!(
            untokened.is_empty(),
            "a live durable node must export each retained value with its convergence token; \
             these had none: {untokened:?}"
        );
    }

    // A node must be restartable over its own data dir the moment an export finishes: the
    // exporter borrows the live handle and holds no `redb` handle of its own, so nothing is
    // left locked (ADR 0061 / issue #242 — a leaked handle fails the next start with
    // "Database already open", and CI has caught exactly that before).
    nodes[0].terminate().await;
    nodes[0].spawn();
    let restarted = wait_ready(&nodes[0], Duration::from_secs(60)).await;
    assert!(
        restarted,
        "a node did not come back over its own data dir straight after an export — check for \
         \"Database already open\" (a leaked store handle):\n{}",
        log_tail(&nodes[0].log_path)
    );

    // Stop the source cluster: the restore target must be a fresh cluster, not this one.
    for node in &mut nodes {
        node.terminate().await;
    }

    // ---------------- the fresh cluster, restored ----------------
    let target_root = root.path().join("target");
    std::fs::create_dir_all(&target_root).unwrap();
    let mut fresh = build_topology(seed + 1, &target_root).await;
    for node in &mut fresh {
        node.extra_env = vec![
            (
                "MQTTD_RESTORE_FROM".to_string(),
                backups.to_string_lossy().into_owned(),
            ),
            // A cluster restore must import against the SETTLED ring: with the default
            // ready_min_members of 1 a node would import as soon as its own lease group is
            // ready, while the placement view still holds one member — and then own (and
            // import) keys the assembled cluster places elsewhere. This is the operator's
            // knob for "the cluster is assembled", and OPERATIONS says to set it.
            ("MQTTD_READY_MIN_MEMBERS".to_string(), "3".to_string()),
        ];
        node.spawn();
    }
    wait_all_ready(&mut fresh, seed + 1).await;

    // (1) The session is present — the resume promise of ADR 0017.
    let (mut sub, present) = common::Client::connect_v311_within(
        fresh[0].client_addr,
        &sub_id,
        false,
        Duration::from_secs(20),
    )
    .await
    .expect("the restored subscriber reconnects");
    assert!(
        present,
        "session_present must be TRUE after a restore — the session is the state a backup \
         exists to carry\n{}",
        restore_lines(&fresh)
    );

    // (2) The restored queue EQUALS the exported queue — every acked payload, and nothing
    // else. An equality, not a containment: the defect this replaced a set-comparison to
    // catch was the restore INVENTING messages (its retained set re-published on every node,
    // fanning out into exactly this session's queue), and a `BTreeSet` of payloads cannot see
    // a duplicate of a value that legitimately belongs there.
    let mut received: Vec<Vec<u8>> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        match sub.recv_bounded(Duration::from_millis(700)).await {
            common::Recv::Packet(Packet::Publish(p)) => {
                if let Some(pkid) = p.pkid {
                    sub.puback(pkid).await;
                }
                received.push(p.payload.to_vec());
            }
            common::Recv::Packet(_) => {}
            common::Recv::Closed => break,
            common::Recv::Quiet => {
                // Keep listening a little past completeness so an EXTRA copy is seen rather
                // than raced past: the injected copies arrive with the rest of the replay.
                if owed.iter().all(|p| received.contains(p)) {
                    break;
                }
            }
        }
    }
    // Drain whatever else the broker has to say for this session, so duplicates count.
    while let common::Recv::Packet(p) = sub.recv_bounded(Duration::from_millis(900)).await {
        if let Packet::Publish(pubm) = p {
            if let Some(pkid) = pubm.pkid {
                sub.puback(pkid).await;
            }
            received.push(pubm.payload.to_vec());
        }
    }
    // What the EXPORT says this session's queue was — read from the files themselves, through
    // the shipped importer, so the oracle is the backup and not the test's own bookkeeping.
    let exported_queue: Vec<Vec<u8>> = {
        let plan = mqttd::backup::load(&backups).expect("the export set loads");
        let session = plan
            .sessions
            .iter()
            .find(|s| s.client == sub_id)
            .expect("the subscriber's session is in the set");
        let mut payloads: Vec<Vec<u8>> = session
            .queue
            .iter()
            .map(|q| {
                mqttd::backup::queued_message(q)
                    .expect("a queued record decodes")
                    .payload
                    .to_vec()
            })
            .collect();
        payloads.sort();
        payloads
    };
    let mut delivered = received.clone();
    delivered.sort();
    let render = |v: &[Vec<u8>]| {
        v.iter()
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect::<Vec<_>>()
    };
    let missing: Vec<String> = owed
        .iter()
        .filter(|p| !received.contains(*p))
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .collect();
    assert!(
        missing.is_empty(),
        "acked durability violated ACROSS THE RESTORE: {} of {} acked payload(s) never \
         delivered: {missing:?}\n---- node logs ----\n{}",
        missing.len(),
        owed.len(),
        fresh
            .iter()
            .map(|n| format!("{}:\n{}", n.id, log_notables(&n.log_path, 20)))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(
        render(&delivered),
        render(&exported_queue),
        "the restored queue must EQUAL the exported queue exactly. Extra copies mean the \
         restore invented messages (a retained value re-published on every node fans out into \
         every restored session whose filter matches it); missing ones mean it lost \
         them.\n---- node logs ----\n{}",
        fresh
            .iter()
            .map(|n| format!("{}:\n{}", n.id, log_notables(&n.log_path, 20)))
            .collect::<Vec<_>>()
            .join("\n")
    );
    sub.disconnect().await;

    // (3) The retained values, with their application properties, on every node — including
    // the one a restored session subscribes to, which must be RETAINED STATE and not merely a
    // message that once passed through a queue.
    for node in &fresh {
        for (i, want) in [
            (&retained_topic, b"{\"t\":21}".to_vec()),
            (&cfg_topic, b"{\"mode\":\"eco\"}".to_vec()),
        ]
        .into_iter()
        .enumerate()
        {
            let arrived = retained_snapshot(
                node.client_addr,
                &format!("cread-{}-{}-{}", seed, node.id, i),
                want.0,
            )
            .await;
            let found = arrived
                .iter()
                .find(|p| &p.topic == want.0)
                .unwrap_or_else(|| {
                    panic!(
                        "the restored retained value for {} is missing on {} (saw {:?})",
                        want.0,
                        node.id,
                        arrived.iter().map(|p| p.topic.clone()).collect::<Vec<_>>()
                    )
                });
            assert_eq!(found.payload.as_ref(), want.1.as_slice());
        }
        let arrived = retained_snapshot(
            node.client_addr,
            &format!("rread-{}-{}", seed, node.id),
            &retained_topic,
        )
        .await;
        let found = arrived
            .iter()
            .find(|p| p.topic == retained_topic)
            .unwrap_or_else(|| {
                panic!(
                    "the restored retained value is missing on {} (saw {:?})",
                    node.id,
                    arrived.iter().map(|p| p.topic.clone()).collect::<Vec<_>>()
                )
            });
        assert_eq!(found.payload.as_ref(), b"{\"t\":21}");
    }
    // The v5 properties survive: read them back over a v5 connection, where they are
    // actually sent on the wire.
    {
        let mut v5 =
            common::Client::connect_v5_ok(fresh[1].client_addr, &format!("v5r-{seed}")).await;
        v5.subscribe(1, &retained_topic, QoS::AtLeastOnce).await;
        let mut props = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match v5.recv_bounded(Duration::from_secs(1)).await {
                common::Recv::Packet(Packet::Publish(p)) if p.topic == retained_topic => {
                    if let Some(pkid) = p.pkid {
                        v5.puback(pkid).await;
                    }
                    props = Some(p.properties);
                    break;
                }
                common::Recv::Packet(_) => {}
                common::Recv::Quiet | common::Recv::Closed => break,
            }
        }
        let props = props.expect("a v5 subscriber sees the restored retained value");
        let has_user_property = props
            .0
            .iter()
            .any(|p| matches!(p, Property::UserProperty(k, v) if k == "unit" && v == "celsius"));
        let has_content_type = props
            .0
            .iter()
            .any(|p| matches!(p, Property::ContentType(t) if t == "application/json"));
        assert!(
            has_user_property && has_content_type,
            "the retained value's application properties must survive the restore (ADR 0030); \
             got {:?}",
            props.0
        );
    }

    // (4) The acknowledged QoS-2 flow: a DUP of the exported packet id earns a plain PUBREC
    // and NO second delivery. Without the acked bit the broker would re-fan-out the message
    // and exactly-once would have quietly become at-least-once across the restore.
    {
        let (mut watcher, present) = common::Client::connect_v311_within(
            fresh[0].client_addr,
            &sub_id,
            false,
            Duration::from_secs(20),
        )
        .await
        .expect("the subscriber reconnects to watch for a duplicate");
        assert!(present, "the session is still present");
        // Drain anything still owed so a duplicate is unambiguous.
        while let common::Recv::Packet(p) = watcher.recv_bounded(Duration::from_millis(700)).await {
            if let Packet::Publish(pubm) = p {
                if let Some(pkid) = pubm.pkid {
                    watcher.puback(pkid).await;
                }
            }
        }

        let (mut q2, present) = common::Client::connect_v311_within(
            fresh[2].client_addr,
            &q2_id,
            false,
            Duration::from_secs(20),
        )
        .await
        .expect("the QoS 2 publisher's session was restored");
        assert!(
            present,
            "the QoS-2 publisher's persistent session must be present after the restore — its \
             dedup window is the state that makes the resend safe"
        );
        q2.send(&Packet::Publish(Publish {
            properties: Properties::new(),
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: q2_topic.clone(),
            pkid: Some(11),
            payload: bytes::Bytes::from_static(b"exactly-once"),
        }))
        .await;
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut pubrec = false;
        loop {
            match q2
                .recv_bounded(deadline.saturating_duration_since(Instant::now()))
                .await
            {
                common::Recv::Packet(Packet::PubRec(r)) if r.pkid == 11 => {
                    pubrec = true;
                    break;
                }
                common::Recv::Packet(_) => {}
                common::Recv::Quiet | common::Recv::Closed => break,
            }
        }
        assert!(
            pubrec,
            "the restored dedup window must answer a DUP with a PUBREC:\n{}",
            log_notables(&fresh[2].log_path, 20)
        );
        // And nothing new is delivered: the flow was already acknowledged.
        let mut duplicates = Vec::new();
        while let common::Recv::Packet(p) = watcher.recv_bounded(Duration::from_millis(900)).await {
            if let Packet::Publish(pubm) = p {
                if let Some(pkid) = pubm.pkid {
                    watcher.puback(pkid).await;
                }
                duplicates.push(String::from_utf8_lossy(&pubm.payload).into_owned());
            }
        }
        assert!(
            duplicates.is_empty(),
            "a DUP of an ACKNOWLEDGED QoS-2 packet id must NOT be fanned out again after a \
             restore; delivered {duplicates:?}"
        );
    }

    for node in &mut fresh {
        node.terminate().await;
    }
}

/// A restore whose format stamp does not match REFUSES loudly, and the process exits
/// non-zero rather than starting a broker with a partial store.
///
/// The unit tests in `backup.rs` pin the refusal logic; this pins that the refusal reaches
/// `main` — that a mismatched stamp is a failed START, not a warning an operator scrolls
/// past while the node serves an empty session set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restore_whose_format_stamp_does_not_match_refuses_and_the_node_does_not_start() {
    let _serial = SERIAL.lock().await;
    let root = tempfile::tempdir().expect("test root");
    let backups = root.path().join("backups");
    std::fs::create_dir_all(&backups).unwrap();
    // A well-formed file from a FUTURE build: correct digest, unknown format_version.
    let header = "{\"kind\":\"header\",\"format\":\"mqttd-backup\",\"format_version\":99,\
        \"binary_version\":\"99.0.0\",\"created_at\":\"2026-08-15T00:00:00Z\",\
        \"created_unix_ms\":1,\"node_id\":\"a\",\"cluster_id\":null,\"durable\":true,\
        \"store_schema\":{},\"members\":[\"a\"]}";
    std::fs::write(
        backups.join("mqttd-backup-a-2026-08-15_000000.ndjson"),
        format!(
            "{header}\n{{\"kind\":\"trailer\",\"complete\":true,\"sessions\":0,\"queued\":0,\
             \"retained\":0,\"not_owned\":[],\"started_unix_ms\":1,\"finished_unix_ms\":2,\
             \"sha256\":\"unchecked\"}}\n"
        ),
    )
    .unwrap();

    let data_dir = root.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_mqttd"))
        .env("MQTTD_NODE_ID", "restore-refuse")
        .env("MQTTD_DATA_DIR", &data_dir)
        .env("MQTTD_RESTORE_FROM", &backups)
        .env(
            "MQTTD_PLAINTEXT_BIND",
            format!("127.0.0.1:{}", free_tcp_port()),
        )
        .env("MQTTD_ALLOW_ANONYMOUS", "1")
        .env("MQTTD_SHUTDOWN_GRACE", "0")
        .output()
        .expect("run mqttd with a foreign backup");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "a broker must NOT start after a refused restore; it exited {:?}\n{text}",
        out.status.code()
    );
    assert!(
        text.contains("format_version 99") && text.contains("99.0.0"),
        "the refusal must name the found version AND the build that wrote it: {text}"
    );
}

/// A standalone durable node, configured exactly through the documented `MQTTD_*` surface.
///
/// Deliberately not `build_topology`: the two facts below are about ONE node's process
/// lifecycle — a signal it must survive, and a restart it must survive — and a 3-node mesh
/// would add nothing but ways to be flaky.
struct Standalone {
    child: Option<std::process::Child>,
    health: std::net::SocketAddr,
    client: std::net::SocketAddr,
    data_dir: PathBuf,
    log_path: PathBuf,
    env: Vec<(String, String)>,
}

impl Standalone {
    fn new(root: &Path, id: &str, env: &[(&str, &str)]) -> Self {
        let data_dir = root.join(format!("{id}-data"));
        std::fs::create_dir_all(&data_dir).unwrap();
        Self {
            child: None,
            health: format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap(),
            client: format!("127.0.0.1:{}", free_tcp_port()).parse().unwrap(),
            data_dir,
            log_path: root.join(format!("{id}.log")),
            env: env
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    fn spawn(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .unwrap();
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_mqttd"))
            .env("MQTTD_NODE_ID", "solo")
            .env("MQTTD_PLAINTEXT_BIND", self.client.to_string())
            .env("MQTTD_HEALTH_BIND", self.health.to_string())
            .env("MQTTD_DATA_DIR", &self.data_dir)
            .env("MQTTD_ALLOW_ANONYMOUS", "1")
            .env("MQTTD_SHUTDOWN_GRACE", "0")
            .env("RUST_LOG", "info")
            .envs(self.env.iter().map(|(k, v)| (k.clone(), v.clone())))
            .stdout(std::process::Stdio::from(log.try_clone().unwrap()))
            .stderr(std::process::Stdio::from(log))
            .spawn()
            .expect("spawn mqttd");
        self.child = Some(child);
    }

    fn pid(&self) -> u32 {
        self.child.as_ref().expect("running").id()
    }

    /// `Some(status)` once the process has left, without blocking.
    fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child
            .as_mut()
            .and_then(|c| c.try_wait().ok().flatten())
    }

    async fn wait_ready(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(body) = http_get(self.health, "/readyz").await {
                if body.contains("\"ready\":true") {
                    return true;
                }
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    fn stop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// **`SIGUSR2` must never kill a broker.** With the handler installed inside the backup task
/// — which only exists when `[backup] dir` is configured — the signal kept its DEFAULT
/// disposition on a default-configured node, so `kill -USR2` terminated a serving broker with
/// crash semantics: no drain, no readiness fail-first, in-flight publishes lost. The docs
/// advertise that signal, and a monitoring or cron rollout can easily land before the config.
///
/// So: a node with NO backup dir, signalled twice, must still be serving — and must say what
/// was missing, because "nothing happened" is not a diagnosis at 03:00.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigusr2_on_a_node_with_no_backup_dir_is_a_no_op_not_a_death() {
    let _serial = SERIAL.lock().await;
    let root = tempfile::tempdir().expect("test root");
    let mut node = Standalone::new(root.path(), "nobackup", &[]);
    node.spawn();
    assert!(
        node.wait_ready(Duration::from_secs(60)).await,
        "the node never became ready:\n{}",
        node.log()
    );

    for _ in 0..2 {
        let ok = std::process::Command::new("kill")
            .args(["-USR2", &node.pid().to_string()])
            .status()
            .expect("send SIGUSR2")
            .success();
        assert!(ok, "kill -USR2 failed");
        // SETTLE(sigusr2-default-disposition-is-death): this asserts a NEGATIVE — that the
        // signal did not kill the broker — and a negative has no state to poll for. The
        // failure it guards is instant and unmistakable when it happens (SIGUSR2's default
        // disposition is Term, so an uninstalled handler ends the process on delivery), so
        // the only question is whether enough time passed for delivery and reaping. 600 ms is
        // roughly three orders of magnitude more than signal delivery needs, and the failure
        // mode is one-sided: waiting LONGER only gives a dying process more time to die, so a
        // too-short wait produces a false PASS — which is why the loop sends twice and the
        // aliveness assertion is made after both, rather than trusting one delivery.
        tokio::time::sleep(Duration::from_millis(600)).await;
    }

    assert!(
        node.exited().is_none(),
        "the broker was KILLED by the online-backup signal on a node with no [backup] dir \
         (exit {:?}); SIGUSR2's default disposition is terminate, so the handler must be \
         installed unconditionally:\n{}",
        node.exited(),
        node.log()
    );
    assert!(
        node.wait_ready(Duration::from_secs(10)).await,
        "the node stopped answering /readyz after SIGUSR2:\n{}",
        node.log()
    );
    let log = node.log();
    assert!(
        log.contains("no [backup] dir is configured"),
        "the signal must be answered with the missing setting, not silence:\n{log}"
    );
    node.stop();
}

/// **A node that was restored must be able to restart with its own unchanged environment.**
///
/// `MQTTD_RESTORE_FROM` lives in the pod spec, so the node meets it again on every ordinary
/// reschedule, OOM kill and rolling upgrade. Refusing there (the data dir now holds stores)
/// made the first restart of a recovered cluster a `CrashLoopBackOff` whose printed remedy —
/// "delete the volume's contents" — destroys the data just restored. Restored data must
/// survive that second boot, and the setting must be inert rather than fatal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restored_node_restarts_with_its_own_unchanged_environment() {
    let _serial = SERIAL.lock().await;
    let root = tempfile::tempdir().expect("test root");
    let backups = root.path().join("backups");
    std::fs::create_dir_all(&backups).unwrap();
    let backup_env = backups.to_string_lossy().into_owned();

    // ---- a live node with state, backed up online ----
    let mut source = Standalone::new(root.path(), "source", &[("MQTTD_BACKUP_DIR", &backup_env)]);
    source.spawn();
    assert!(
        source.wait_ready(Duration::from_secs(60)).await,
        "source node never ready:\n{}",
        source.log()
    );
    let sub_id = "restart-psub";
    {
        let (mut sub, _) = common::Client::connect_v311_within(
            source.client,
            sub_id,
            false,
            Duration::from_secs(10),
        )
        .await
        .expect("durable subscriber connects");
        sub.subscribe(1, "rr/#", QoS::AtLeastOnce).await;
        sub.disconnect().await;
    }
    let (ok, out) = take_backup(source.pid(), &backups, 60);
    assert!(ok, "the online backup failed:\n{out}\n{}", source.log());
    assert_eq!(exports(&backups).len(), 1);
    source.stop();

    // ---- restored into a fresh node, WITH the restore setting in its environment ----
    let mut restored = Standalone::new(
        root.path(),
        "restored",
        &[("MQTTD_RESTORE_FROM", &backup_env)],
    );
    restored.spawn();
    assert!(
        restored.wait_ready(Duration::from_secs(90)).await,
        "the restore never completed:\n{}",
        restored.log()
    );
    assert!(
        restored.log().contains("restore complete"),
        "the first boot must actually import:\n{}",
        restored.log()
    );
    restored.stop();

    // ---- the ordinary restart: same node, same environment, nothing edited ----
    restored.spawn();
    let ready = restored.wait_ready(Duration::from_secs(90)).await;
    assert!(
        restored.exited().is_none(),
        "a successfully restored node EXITED on its next ordinary start with its own \
         unchanged environment (exit {:?}) — every reschedule of a recovered cluster would \
         CrashLoopBackOff:\n{}",
        restored.exited(),
        log_notables(&restored.log_path, 40)
    );
    assert!(
        ready,
        "the restarted node never became ready:\n{}",
        log_notables(&restored.log_path, 40)
    );
    assert!(
        restored.log().contains("INERT this boot"),
        "the second boot must say the setting is inert rather than importing again:\n{}",
        log_notables(&restored.log_path, 40)
    );

    // And the restored state survived the restart — the reason the boot had to succeed.
    let (_sub, present) = common::Client::connect_v311_within(
        restored.client,
        sub_id,
        false,
        Duration::from_secs(20),
    )
    .await
    .expect("the restored subscriber reconnects after the restart");
    assert!(
        present,
        "the restored session must still be there after an ordinary restart:\n{}",
        log_notables(&restored.log_path, 40)
    );
    restored.stop();
}
