//! Ingress credit at the connection (ADR 0082 T3).
//!
//! The test plays the hub: it answers `Attach` at once, as the control lane would, and
//! holds every publish in a queue it drains when it chooses. That queue IS the hub's
//! data lane — commands sent and not yet dispatched — so its length is the quantity the
//! credit exists to bound, measured exactly rather than sampled from a gauge.

use super::*;
use crate::ingress::{IngressCredit, OverloadMode, COMMAND_OVERHEAD};
use mqtt_observability::metrics::Metrics;

const TOPIC: &str = "load/t";
const PAYLOAD: usize = 200;
/// What one test publish costs: topic, payload and the fixed overhead.
const COST: usize = TOPIC.len() + PAYLOAD + COMMAND_OVERHEAD;

fn policy(credit: Option<Arc<IngressCredit>>, metrics: Option<Arc<Metrics>>) -> Arc<ConnPolicy> {
    let base = permissive();
    Arc::new(ConnPolicy {
        ingress: credit,
        metrics,
        ..(*base).clone()
    })
}

fn open(
    policy: &Arc<ConnPolicy>,
    hub_tx: &mpsc::UnboundedSender<HubCommand>,
    version: ProtocolVersion,
) -> (Reader, Writer) {
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(handle_stream(
        server,
        None,
        None,
        policy.clone(),
        hub_tx.clone(),
    ));
    let (rh, wh) = tokio::io::split(client);
    (FrameReader::new(rh, version), FrameWriter::new(wh, version))
}

/// The stalled hub: `Attach` answered at once (keeping each connection's outbound
/// sender, handed to the test too), every other command queued until the test
/// dispatches it with [`dispatch`].
fn stalled_hub(
    mut hub_rx: mpsc::UnboundedReceiver<HubCommand>,
) -> (
    mpsc::UnboundedReceiver<HubCommand>,
    mpsc::UnboundedReceiver<Outbound>,
) {
    let (queue_tx, queue_rx) = mpsc::unbounded_channel();
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(cmd) = hub_rx.recv().await {
            match cmd {
                HubCommand::Attach {
                    outbound, reply, ..
                } => {
                    let _ = out_tx.send(outbound);
                    let _ = reply.send(AttachOutcome::Present(false));
                }
                other => {
                    let _ = queue_tx.send(other);
                }
            }
        }
    });
    (queue_rx, out_rx)
}

/// Dispatch one queued command as the hub would: answer a gated publish, then drop
/// it — which is what returns its credit. Returns the publish's payload, if it was one.
fn dispatch(cmd: HubCommand) -> Option<Bytes> {
    match cmd {
        HubCommand::Publish { payload, done, .. } => {
            if let Some(done) = done {
                let _ = done.send(crate::hub::PublishOutcome::Accepted);
            }
            Some(payload)
        }
        _ => None,
    }
}

fn publish(qos: QoS, pkid: Option<u16>, publisher: usize, seq: usize) -> Packet {
    let mut payload = vec![0u8; PAYLOAD];
    payload[..16].copy_from_slice(&(publisher as u64).to_be_bytes().repeat(2));
    payload[16..24].copy_from_slice(&(seq as u64).to_be_bytes());
    Packet::Publish(Publish {
        properties: Properties::new(),
        dup: false,
        qos,
        retain: false,
        topic: TOPIC.into(),
        pkid,
        payload: Bytes::from(payload),
    })
}

/// Wait until the stalled hub's queue stops growing (nothing new for 200 ms).
async fn settle(queue: &mpsc::UnboundedReceiver<HubCommand>) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut last = queue.len();
    let mut quiet = 0;
    while quiet < 10 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the stalled hub's queue never stopped growing: {last} commands"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = queue.len();
        if now == last {
            quiet += 1;
        } else {
            quiet = 0;
            last = now;
        }
    }
    last
}

fn counter(metrics: &Metrics, series: &str) -> u64 {
    metrics
        .render()
        .lines()
        .find_map(|l| l.strip_prefix(series)?.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

/// What one overload run measured.
#[derive(Debug)]
struct Overload {
    /// Commands the stalled hub held once every publisher had stopped.
    stalled_queue: usize,
    /// The deepest the queue got while the hub drained it slowly.
    max_queue: usize,
    /// Distinct publishes the hub dispatched.
    dispatched: usize,
    /// PUBACKs the publishers received.
    acked: usize,
    /// `publish_dropped{reason="hub-ingress"}`.
    shed: u64,
    /// `ingress_paused_total`.
    pauses: u64,
}

const PUBLISHERS: usize = 8;
const POOL: usize = 64 * 1024;
const CONN_CAP: usize = 16 * 1024;

/// `PUBLISHERS` connections each write `per` publishes at `qos` as fast as the socket
/// takes them, against a hub that first stalls (until every publisher has stopped)
/// and then drains one command at a time. `credit: None` is the broker before T3.
async fn overload(credit: Option<OverloadMode>, qos: QoS, per: usize) -> Overload {
    let credit = credit.map(|mode| Arc::new(IngressCredit::new(POOL, CONN_CAP, mode)));
    let metrics = Arc::new(Metrics::new("test"));
    let policy = policy(credit, Some(metrics.clone()));
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let (mut queue, _outbound) = stalled_hub(hub_rx);

    let acked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut writers = Vec::new();
    for p in 0..PUBLISHERS {
        let (mut reader, mut writer) = open(&policy, &hub_tx, V4);
        writer
            .send(&connect_packet(&format!("pub-{p}"), true))
            .await
            .unwrap();
        assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
        let acked = acked.clone();
        tokio::spawn(async move {
            while let Ok(Some(pkt)) = reader.next_packet().await {
                if matches!(pkt, Packet::PubAck(_)) {
                    acked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });
        writers.push(tokio::spawn(async move {
            for seq in 0..per {
                let pkid =
                    (qos != QoS::AtMostOnce).then(|| u16::try_from(seq % 65_535 + 1).unwrap());
                if writer.send(&publish(qos, pkid, p, seq)).await.is_err() {
                    return;
                }
            }
            // Hold the connection open; dropping the writer would close it.
            std::future::pending::<()>().await;
        }));
    }

    let stalled_queue = settle(&queue).await;
    let mut max_queue = stalled_queue;
    let mut seen = std::collections::HashSet::new();
    let expected = if qos == QoS::AtMostOnce {
        None
    } else {
        Some(PUBLISHERS * per)
    };
    // Drain slowly, as an overloaded hub would, until nothing more arrives.
    while let Ok(Some(cmd)) = timeout(Duration::from_millis(500), queue.recv()).await {
        max_queue = max_queue.max(queue.len() + 1);
        if let Some(payload) = dispatch(cmd) {
            assert!(seen.insert(payload), "a publish was dispatched twice");
        }
        if seen.len() % 16 == 0 {
            tokio::task::yield_now().await;
        }
        if expected == Some(seen.len()) {
            break;
        }
    }
    // Let the last PUBACKs land.
    if let Some(n) = expected {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while acked.load(std::sync::atomic::Ordering::Relaxed) < n
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    for w in writers {
        w.abort();
    }
    Overload {
        stalled_queue,
        max_queue,
        dispatched: seen.len(),
        acked: acked.load(std::sync::atomic::Ordering::Relaxed),
        shed: counter(
            &metrics,
            "mqttd_publish_dropped_total{reason=\"hub-ingress\"}",
        ),
        pauses: counter(&metrics, "mqttd_ingress_paused_total"),
    }
}

/// The most publishes the pool can hold at once.
const BOUND: usize = POOL / COST;

/// ADR 0082 T3, `pause`: a stalled hub holds no more than the pool, every `QoS` 1
/// publish still arrives exactly once and is acknowledged, and nothing is shed.
///
/// Mutation-proven twice: [`without_credit_a_stalled_hub_queues_without_bound`] runs
/// this rig with no credit, and making `admit` always grant fails this test with the
/// queue at 2,056 commands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_pause_the_queue_stays_within_the_pool_and_qos1_loses_nothing() {
    let per = 300;
    let r = overload(Some(OverloadMode::Pause), QoS::AtLeastOnce, per).await;
    eprintln!("pause/QoS 1: {r:?}, bound {BOUND}");
    assert!(
        r.max_queue <= BOUND,
        "the hub queue reached {} commands; the pool holds {BOUND}",
        r.max_queue
    );
    assert!(
        r.stalled_queue >= BOUND - PUBLISHERS,
        "the pool, not something else, stopped the publishers: {} of {BOUND}",
        r.stalled_queue
    );
    assert_eq!(
        r.dispatched,
        PUBLISHERS * per,
        "every QoS 1 publish arrived"
    );
    assert_eq!(r.acked, PUBLISHERS * per, "and every one was acknowledged");
    assert_eq!(r.shed, 0, "pause sheds nothing");
    assert!(
        r.pauses >= PUBLISHERS as u64,
        "every publisher paused: {}",
        r.pauses
    );
}

/// ADR 0082 T3, `pause` at `QoS` 0: lossless too — the publishers wait in TCP.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_pause_qos0_waits_rather_than_drops() {
    let per = 300;
    let r = overload(Some(OverloadMode::Pause), QoS::AtMostOnce, per).await;
    eprintln!("pause/QoS 0: {r:?}, bound {BOUND}");
    assert!(r.max_queue <= BOUND, "queue {} > pool {BOUND}", r.max_queue);
    assert_eq!(r.dispatched, PUBLISHERS * per, "nothing lost under pause");
    assert_eq!(r.shed, 0);
}

/// ADR 0082 §2a, `shed-qos0`: the queue stays within the pool, and the `QoS` 0
/// publishes it had no room for are dropped and counted — every one accounted for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_shed_qos0_the_queue_stays_within_the_pool_and_every_drop_is_counted() {
    let per = 2_000;
    let r = overload(Some(OverloadMode::ShedQos0), QoS::AtMostOnce, per).await;
    eprintln!("shed-qos0/QoS 0: {r:?}, bound {BOUND}");
    assert!(r.max_queue <= BOUND, "queue {} > pool {BOUND}", r.max_queue);
    assert!(r.shed > 0, "a stalled hub under shed-qos0 sheds");
    assert_eq!(
        r.dispatched as u64 + r.shed,
        (PUBLISHERS * per) as u64,
        "every publish was either dispatched or counted as shed"
    );
    assert_eq!(r.pauses, 0, "QoS 0 never pauses under shed-qos0");
}

/// ADR 0082 §2a: `shed-qos0` never sheds an acknowledged publish — `QoS` 1 pauses
/// exactly as under `pause`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_shed_qos0_qos1_still_pauses_and_loses_nothing() {
    let per = 300;
    let r = overload(Some(OverloadMode::ShedQos0), QoS::AtLeastOnce, per).await;
    eprintln!("shed-qos0/QoS 1: {r:?}, bound {BOUND}");
    assert!(r.max_queue <= BOUND, "queue {} > pool {BOUND}", r.max_queue);
    assert_eq!(r.dispatched, PUBLISHERS * per);
    assert_eq!(r.acked, PUBLISHERS * per);
    assert_eq!(r.shed, 0, "QoS 1 is never shed");
    assert!(r.pauses > 0);
}

/// The rig's own mutation proof: with no credit — the broker before T3 — the same
/// stalled hub queues far past the bound, so the bounded results above are the credit's
/// doing. (`QoS` 1 is capped only by the per-connection ack pipeline, 256 + 1 each.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_credit_a_stalled_hub_queues_without_bound() {
    let r = overload(None, QoS::AtLeastOnce, 300).await;
    eprintln!("no credit/QoS 1: {r:?}, bound {BOUND}");
    assert!(
        r.stalled_queue > 10 * BOUND,
        "without credit the queue should run far past {BOUND}: {}",
        r.stalled_queue
    );
    let r = overload(None, QoS::AtMostOnce, 2_000).await;
    eprintln!("no credit/QoS 0: {r:?}, bound {BOUND}");
    assert_eq!(
        r.stalled_queue,
        PUBLISHERS * 2_000,
        "every QoS 0 publish queued"
    );
}

/// ADR 0082 T3: a publisher paused for credit resumes when it frees, and the pause
/// coexists with the client's own `QoS` 1 window and the server's Receive Maximum:
/// a v5 client that keeps `WINDOW` publishes outstanding against a pool of three runs
/// to completion. While it is paused, deliveries to it still flow.
#[tokio::test]
async fn a_paused_publisher_resumes_and_still_receives_while_paused() {
    const WINDOW: usize = 20;
    const TOTAL: usize = 200;
    let credit = Arc::new(IngressCredit::new(3 * COST, 3 * COST, OverloadMode::Pause));
    let policy = policy(Some(credit.clone()), None);
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let (mut queue, mut outbound) = stalled_hub(hub_rx);
    let (mut reader, mut writer) = open(&policy, &hub_tx, V5);
    writer.send(&connect_v5("paused", vec![])).await.unwrap();
    assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
    let out = outbound.recv().await.unwrap();

    // Fill the window: three publishes take the pool, the fourth parks the connection.
    for seq in 0..WINDOW {
        writer
            .send(&publish(
                QoS::AtLeastOnce,
                Some(u16::try_from(seq + 1).unwrap()),
                0,
                seq,
            ))
            .await
            .unwrap();
    }
    let stalled = settle(&queue).await;
    assert_eq!(stalled, 3, "the pool holds three publishes");
    assert_eq!(credit.in_use(), 3 * COST);

    // Paused, the connection still delivers to its client.
    let delivery = publish(QoS::AtMostOnce, None, 9, 9);
    assert!(out.send(delivery.clone()));
    assert_eq!(
        recv(&mut reader).await,
        Some(delivery),
        "outbound flows while paused"
    );

    // Now drain: each dispatch frees credit, the parked publish proceeds, the client
    // gets its PUBACK and sends the next — window, Receive Maximum and credit together.
    let mut sent = WINDOW;
    let mut acked = 0;
    let mut dispatched = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while acked < TOTAL {
        tokio::select! {
            Some(cmd) = queue.recv() => {
                if dispatch(cmd).is_some() {
                    dispatched += 1;
                }
            }
            pkt = reader.next_packet() => match pkt.unwrap() {
                Some(Packet::PubAck(_)) => {
                    acked += 1;
                    if sent < TOTAL {
                        writer
                            .send(&publish(QoS::AtLeastOnce, Some(u16::try_from(sent % 65_535 + 1).unwrap()), 0, sent))
                            .await
                            .unwrap();
                        sent += 1;
                    }
                }
                other => panic!("unexpected {other:?}"),
            },
            () = tokio::time::sleep_until(deadline) => {
                panic!("deadlock: {acked} acked, {dispatched} dispatched, {sent} sent");
            }
        }
    }
    assert_eq!(dispatched, TOTAL);
    assert_eq!(credit.in_use(), 0, "every credit returned");
}

/// ADR 0082 T3: keepalive is not enforced against a connection the broker paused. The
/// client is silent because the broker stopped reading it, not because it went away;
/// once reading resumes the keepalive applies again from that moment.
///
/// Mutation-proven: enforcing the idle timer while parked closes the connection inside
/// the pause and this test fails.
#[tokio::test(start_paused = true)]
async fn keepalive_is_not_enforced_while_the_broker_pauses_the_connection() {
    let credit = Arc::new(IngressCredit::new(COST, COST, OverloadMode::Pause));
    let policy = policy(Some(credit), None);
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let (mut queue, _outbound) = stalled_hub(hub_rx);
    let (mut reader, mut writer) = open(&policy, &hub_tx, V4);
    writer
        .send(&Packet::Connect(Connect {
            properties: Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 1,
            client_id: "ka".into(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await
        .unwrap();
    assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
    // The first takes the whole pool; the second parks the connection.
    writer
        .send(&publish(QoS::AtMostOnce, None, 0, 0))
        .await
        .unwrap();
    writer
        .send(&publish(QoS::AtMostOnce, None, 0, 1))
        .await
        .unwrap();
    let first = queue.recv().await.unwrap();

    // Ten keepalive graces pass while paused: the connection stays.
    tokio::time::sleep(Duration::from_secs(15)).await;
    match queue.try_recv() {
        Err(_) => {} // still parked, still connected
        Ok(HubCommand::Detach { .. }) => {
            panic!("the connection was closed during the broker's pause")
        }
        Ok(_) => panic!("the parked publish went through without credit"),
    }
    // Free the credit: the parked publish proceeds — the connection was alive.
    dispatch(first);
    let second = timeout(Duration::from_secs(1), queue.recv())
        .await
        .expect("the parked publish proceeds once credit frees")
        .unwrap();
    dispatch(second);
    // And with reading resumed, a silent client is closed after the grace again.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        recv(&mut reader).await.is_none(),
        "keepalive applies again once the broker reads"
    );
}
