//! What one rolled node actually costs (issue #248): the measured answer to
//! "a rolling upgrade pays a full drain on every pod — a reconnect storm per
//! rolled node".
//!
//! This drives the exact Kubernetes motion over real spawned processes: the
//! chart's `preStop` sends `SIGUSR1` (the ADR 0043 decommission drain), the
//! process exits, the pod restarts over the same data dir and rejoins. The
//! claims under test — the numbers OPERATIONS.md states — are:
//!
//! * **Reconnects per roll = the sessions the rolled node HOSTED, not the
//!   cluster.** That is its directly-connected clients plus the clients other
//!   nodes relocated to it at CONNECT time (ADR 0005 places a persistent
//!   session on its owner; the landing node proxies) — measured here as 4 of
//!   9 clients (3 direct + 1 relocated). Every other client keeps its TCP
//!   connection through the whole roll and keeps receiving publishes while
//!   the rolled node is away. The "mass reconnects per rolled node" fear is
//!   refuted structurally: the blast radius is ~hosted-sessions/N-th of the
//!   fleet, not the fleet.
//! * The dropped clients resume with `session_present=1` on a survivor and
//!   their subscriptions still deliver. One measured wart on top (filed as
//!   issue #284, see the post-roll section): a client that resumes in the
//!   seconds around the rolled node's readmission can be routed onto a stale
//!   placement and sit undeliverable until it reconnects once more — the
//!   keepalive-driven second reconnect is modelled and counted here.
//! * `QoS` 1 publishes into a group mid-ownership-move are REFUSED (ack
//!   withheld, never silently dropped) for a measured seconds-scale window.
//! * The drain-to-exit and restart-to-readmission times are bounded and
//!   printed, so the OPERATIONS numbers are reproducible from this test.

mod common;
mod proc_common;

use std::time::{Duration, Instant};

use mqtt_codec::{Packet, QoS};
use proc_common::{build_topology, wait_all_ready, ProcNode};

/// One measured client: its live connection, its private topic, and the node
/// it connected through.
struct RollClient {
    id: String,
    topic: String,
    conn: common::Client,
    via: usize,
}

/// Publish `payload` to `topic` through node `via` at `QoS` 1 and wait for the
/// PUBACK — the message is a hard fact once acked. `None` when no ack arrived
/// within `within` (retrying with fresh publisher connections throughout):
/// the broker is REFUSING the durability promise (ack withheld), which some
/// callers assert against and the post-roll caller treats as the signal to do
/// what a real client does — reconnect.
async fn try_publish_acked(
    nodes: &[ProcNode],
    via: usize,
    topic: &str,
    payload: &[u8],
    within: Duration,
) -> Option<Duration> {
    let started = Instant::now();
    let deadline = started + within;
    loop {
        if Instant::now() >= deadline {
            return None;
        }
        if let Some((mut publisher, _)) = common::Client::connect_v311_within(
            nodes[via].client_addr,
            &format!("rcpub-{topic}-{via}", topic = topic.replace('/', "-")),
            true,
            Duration::from_secs(10),
        )
        .await
        {
            publisher
                .publish(topic, payload, QoS::AtLeastOnce, Some(9), vec![])
                .await;
            let ack_deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let left = ack_deadline.saturating_duration_since(Instant::now());
                match publisher.recv_bounded(left).await {
                    common::Recv::Packet(Packet::PubAck(_)) => return Some(started.elapsed()),
                    common::Recv::Packet(_) => {}
                    common::Recv::Quiet | common::Recv::Closed => break,
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// [`try_publish_acked`], asserted: the ack must arrive. Returns the stall.
async fn publish_acked(
    nodes: &[ProcNode],
    via: usize,
    topic: &str,
    payload: &[u8],
    within: Duration,
) -> Duration {
    let Some(stall) = try_publish_acked(nodes, via, topic, payload, within).await else {
        panic!("no PUBACK for {topic} via node {via} within {within:?}");
    };
    stall
}

/// Wait until `client` receives `payload` on its own topic (acking `QoS` 1 as
/// it goes). Returns `false` if the connection closed instead — the caller
/// decides whether that is the expected disconnect or a broken promise.
async fn receives(client: &mut common::Client, payload: &[u8], within: Duration) -> bool {
    let deadline = Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match client.recv_bounded(left).await {
            common::Recv::Packet(Packet::Publish(p)) => {
                if let Some(pkid) = p.pkid {
                    client.puback(pkid).await;
                }
                if p.payload.as_ref() == payload {
                    return true;
                }
            }
            common::Recv::Packet(_) => {}
            common::Recv::Closed => return false,
            common::Recv::Quiet => {
                if Instant::now() >= deadline {
                    return false;
                }
            }
        }
    }
}

/// The measured k8s-style roll: 9 durable clients spread 3-per-node, node `b`
/// is rolled with the chart's exact motion, and only the sessions it hosted
/// pay a reconnect. Prints the drain / readmission timings the docs cite.
// One linear measured story (setup, roll, classify, restart, heal) — splitting it
// would scatter the numbers from the assertions; index loops are deliberate, the
// body both reads `clients[i]` fields and mutably swaps `clients[i].conn`.
#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_rolled_node_disconnects_only_its_own_clients() {
    let seed = 4848u64;
    let disk = tempfile::tempdir().expect("tempdir");
    let mut nodes = build_topology(seed, disk.path()).await;
    for n in &mut nodes {
        n.spawn();
    }
    wait_all_ready(&mut nodes, seed).await;

    // 3 durable QoS 1 subscribers per node, each on its own topic.
    let mut clients: Vec<RollClient> = Vec::new();
    for via in 0..nodes.len() {
        for k in 0..3 {
            let id = format!("rc-{seed}-{via}-{k}");
            let topic = format!("rc/{seed}/{via}/{k}");
            let deadline = Instant::now() + Duration::from_secs(30);
            let conn = loop {
                if let Some((mut conn, present)) = common::Client::connect_v311_within(
                    nodes[via].client_addr,
                    &id,
                    false,
                    Duration::from_secs(10),
                )
                .await
                {
                    assert!(!present, "{id}: fresh session must not be present");
                    let ack = conn.subscribe(1, &topic, QoS::AtLeastOnce).await;
                    assert!(
                        ack.return_codes.iter().all(|c| *c != 0x80),
                        "{id}: SUBSCRIBE refused"
                    );
                    break conn;
                }
                assert!(Instant::now() < deadline, "{id} never connected");
            };
            clients.push(RollClient {
                id,
                topic,
                conn,
                via,
            });
        }
    }

    // Warm-up: cross-node routing works for every client (publish through the
    // NEXT node over, so interest has provably propagated across the mesh).
    for i in 0..clients.len() {
        let via = (clients[i].via + 1) % nodes.len();
        let warm = format!("warm-{seed}-{i}").into_bytes();
        publish_acked(
            &nodes,
            via,
            &clients[i].topic,
            &warm,
            Duration::from_secs(30),
        )
        .await;
        assert!(
            receives(&mut clients[i].conn, &warm, Duration::from_secs(20)).await,
            "warm-up delivery to {} failed",
            clients[i].id
        );
    }

    // ---- The roll, exactly as the chart does it -----------------------------
    let rolled = 1usize;
    let rolled_id = nodes[rolled].id.clone();

    // preStop: SIGUSR1 → ADR 0043 drain → ADR 0019 graceful leave → exit.
    let drain = nodes[rolled].drain_stop().await;

    // Classify every client after the drain: closed (it pays a reconnect) or
    // still connected. The disconnect set is the sessions HOSTED on the rolled
    // node — its directly-connected clients plus any client another node
    // relocated to it at CONNECT time (ADR 0005: the session serves on its
    // placement owner; the frontend proxies) — NOT the whole cluster.
    let mut closed: Vec<usize> = Vec::new();
    for (i, c) in clients.iter_mut().enumerate() {
        let deadline = Instant::now() + Duration::from_secs(8);
        let is_closed = loop {
            match c.conn.recv_bounded(Duration::from_secs(2)).await {
                common::Recv::Closed => break true,
                common::Recv::Packet(Packet::Publish(p)) => {
                    if let Some(pkid) = p.pkid {
                        c.conn.puback(pkid).await;
                    }
                }
                common::Recv::Packet(_) => {}
                common::Recv::Quiet => break false,
            }
            if Instant::now() >= deadline {
                break false;
            }
        };
        if is_closed {
            closed.push(i);
        }
    }
    for c in clients.iter().filter(|c| c.via == rolled) {
        assert!(
            closed.iter().any(|&i| clients[i].id == c.id),
            "{}: a client connected THROUGH the rolled node must observe the close",
            c.id
        );
    }
    assert!(
        closed.len() < clients.len(),
        "the roll must not disconnect the whole cluster's clients"
    );
    // Composition of the reconnect set, captured before the resumes reassign `via`.
    let direct = clients
        .iter()
        .enumerate()
        .filter(|(i, c)| closed.contains(i) && c.via == rolled)
        .count();

    let mut mid_roll_stall = Duration::ZERO;
    let mut post_roll_stall = Duration::ZERO;

    // Every client that KEPT its connection also keeps receiving while the
    // rolled node is away — the roll's blast radius is the sessions the rolled
    // node hosted, and nothing else.
    for i in 0..clients.len() {
        if closed.contains(&i) {
            continue;
        }
        let via = clients[i].via;
        let probe = format!("mid-roll-{seed}-{i}").into_bytes();
        // A publish into a group mid-takeover is REFUSED (ack withheld) until the
        // survivor's recovery completes — the harness's own resume path budgets
        // 90s for exactly this window; measure it rather than assume it away.
        let stall = publish_acked(
            &nodes,
            via,
            &clients[i].topic,
            &probe,
            Duration::from_secs(90),
        )
        .await;
        mid_roll_stall = mid_roll_stall.max(stall);
        assert!(
            receives(&mut clients[i].conn, &probe, Duration::from_secs(30)).await,
            "{}: a client the roll left connected must keep receiving",
            clients[i].id
        );
    }

    // Restart over the same data dir (the pod's PV) and rejoin via the whole
    // topology, as a restarted pod does.
    let restart = Instant::now();
    nodes[rolled].swim_seeds = nodes
        .iter()
        .enumerate()
        .filter(|(j, _)| *j != rolled)
        .map(|(_, n)| n.swim_bind.clone())
        .collect::<Vec<_>>()
        .join(",");
    nodes[rolled].spawn();
    wait_all_ready(&mut nodes, seed).await;
    let readmit = restart.elapsed();

    // The dropped clients resume durably — once each, on whatever node the
    // Service offers (here: a survivor), with their session intact.
    for &i in &closed {
        let via = (rolled + 1) % nodes.len();
        let deadline = Instant::now() + Duration::from_secs(30);
        let conn = loop {
            if let Some((conn, present)) = common::Client::connect_v311_within(
                nodes[via].client_addr,
                &clients[i].id,
                false,
                Duration::from_secs(10),
            )
            .await
            {
                assert!(
                    present,
                    "{}: durable session must survive the roll (resume says present)",
                    clients[i].id
                );
                break conn;
            }
            assert!(
                Instant::now() < deadline,
                "{} could not resume after the roll",
                clients[i].id
            );
        };
        clients[i].conn = conn;
        clients[i].via = via;
    }

    // Post-roll: every client (resumed and undisturbed alike) still delivers.
    //
    // With one measured wart: a client that RESUMED in the seconds around the
    // rolled node's readmission can be routed to a stale/interim owner — its
    // session then sits on a node the placement no longer maps to the group, so
    // every QoS 1 publish to it is refused (`not the owning node for this
    // group`, ack withheld) and it receives nothing, indefinitely. A real
    // client's keepalive fires on that dead air and it RECONNECTS, which
    // re-relocates the session and heals it — modelled here (and counted, so
    // the docs can state the double-reconnect cost). Filed as issue #284;
    // this loop is the measurement, not an endorsement.
    let mut second_reconnects = 0usize;
    for i in 0..clients.len() {
        let after = format!("after-{seed}-{i}").into_bytes();
        let overall = Instant::now() + Duration::from_secs(180);
        loop {
            let via = (clients[i].via + 1) % nodes.len();
            let delivered = match try_publish_acked(
                &nodes,
                via,
                &clients[i].topic,
                &after,
                Duration::from_secs(20),
            )
            .await
            {
                Some(stall) => {
                    post_roll_stall = post_roll_stall.max(stall);
                    receives(&mut clients[i].conn, &after, Duration::from_secs(20)).await
                }
                None => false,
            };
            if delivered {
                break;
            }
            assert!(
                Instant::now() < overall,
                "{}: still wedged after reconnect retries",
                clients[i].id
            );
            // The real client's move on dead air: drop and reconnect.
            second_reconnects += 1;
            let rvia = (clients[i].via + 2) % nodes.len();
            let deadline = Instant::now() + Duration::from_secs(30);
            let conn = loop {
                if let Some((conn, present)) = common::Client::connect_v311_within(
                    nodes[rvia].client_addr,
                    &clients[i].id,
                    false,
                    Duration::from_secs(10),
                )
                .await
                {
                    assert!(present, "{}: session lost during rehome", clients[i].id);
                    break conn;
                }
                assert!(
                    Instant::now() < deadline,
                    "{} could not reconnect during rehome",
                    clients[i].id
                );
            };
            clients[i].conn = conn;
            clients[i].via = rvia;
        }
    }

    eprintln!(
        "roll_cost: rolled {rolled_id}: reconnects={} of {} clients \
         ({direct} connected through it + {} relocated to it as session owner; \
         every other connection stayed open and receiving), \
         drain-to-exit={:.1}s, restart-to-full-readmission={:.1}s, \
         worst QoS1 ack stall: mid-roll={:.1}s post-roll={:.1}s, \
         clients needing a second (keepalive-style) reconnect: {second_reconnects}",
        closed.len(),
        clients.len(),
        closed.len() - direct,
        drain.as_secs_f64(),
        readmit.as_secs_f64(),
        mid_roll_stall.as_secs_f64(),
        post_roll_stall.as_secs_f64(),
    );

    for node in &mut nodes {
        node.kill().await;
    }
}
