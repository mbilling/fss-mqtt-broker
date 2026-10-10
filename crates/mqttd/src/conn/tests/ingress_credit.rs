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
        rules: None,
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

/// ADR 0082 T1: the charge is for what the hub will hold. An alias-only PUBLISH pays for
/// the topic its alias stands for, not its empty wire topic, and its properties are
/// charged with the payload: a small payload with large user properties is not cheap.
#[test]
fn an_alias_only_publish_pays_for_its_topic_and_its_properties() {
    use crate::aliases::InboundAliases;
    use crate::conn::{admit, IngressAdmit};
    let node = Arc::new(IngressCredit::new(1 << 20, 1 << 20, OverloadMode::Pause));
    let conn = node.connection();
    let mut aliases = InboundAliases::new(8);
    aliases.resolve(TOPIC, Some(1)).unwrap();
    let properties = Properties(vec![
        mqtt_codec::Property::TopicAlias(1),
        mqtt_codec::Property::UserProperty("k".into(), "v".repeat(4_000)),
    ]);
    let publish = Publish {
        properties,
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: String::new(),
        pkid: None,
        payload: Bytes::from(vec![0u8; PAYLOAD]),
    };
    let Ok(IngressAdmit::Credit(Some(_permit))) = admit(&conn, &publish, &aliases) else {
        panic!("credit is free, so the publish is admitted at once");
    };
    assert_eq!(
        node.in_use(),
        COST + 4_001,
        "resolved topic and properties charged"
    );
}

/// A connection whose rules copy every publish to `copies` topics of the same length.
fn policy_with_copies(credit: Arc<IngressCredit>, copies: usize) -> Arc<ConnPolicy> {
    let actions: Vec<String> = (1..=copies)
        .map(|i| format!(r#"{{ function = "republish", args = {{ topic = "copy/{i}" }} }}"#))
        .collect();
    let text = format!(
        "[rules.copy]\nsql = 'SELECT * FROM \"load/#\"'\nactions = [{}]\n",
        actions.join(", ")
    );
    let set = mqtt_rules::RuleSet::parse(&text)
        .expect("test rules load")
        .rules;
    let (_tx, rx) = tokio::sync::watch::channel(Arc::new(set));
    let base = permissive();
    Arc::new(ConnPolicy {
        ingress: Some(credit),
        rules: Some(crate::rules::Rules::new(rx, Arc::from("n"), None)),
        ..(*base).clone()
    })
}

/// Derived messages are queued for the hub like any publish, so they are charged to
/// the connection's credit like one (ADR 0083, ADR 0082 T3): a rule that copies a
/// publish to three topics makes it cost four publishes until the hub has dispatched
/// the batch — which is what keeps a rule's amplification inside the pool.
#[tokio::test]
async fn a_publishs_derived_messages_are_charged_to_its_credit() {
    let credit = Arc::new(IngressCredit::new(POOL, CONN_CAP, OverloadMode::Pause));
    let policy = policy_with_copies(credit.clone(), 3);
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let (mut queue, _outbound) = stalled_hub(hub_rx);
    let (mut reader, mut writer) = open(&policy, &hub_tx, V4);
    writer.send(&connect_packet("copier", true)).await.unwrap();
    assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
    writer
        .send(&publish(QoS::AtMostOnce, None, 0, 0))
        .await
        .unwrap();
    let batch = queue.recv().await.expect("the publish reaches the hub");
    assert!(matches!(batch, HubCommand::PublishBatch(_)), "{batch:?}");
    // "copy/N" is as long as "load/t", and the copies carry the original's payload.
    assert_eq!(credit.in_use(), 4 * COST, "the original and three copies");
    drop(batch);
    assert_eq!(credit.in_use(), 0, "dispatched: all of it returns");
}

/// The charge is clamped to what the connection's cap leaves beside the original, so
/// a batch that alone costs more than the cap still proceeds, as the largest single
/// message does.
#[tokio::test]
async fn a_derived_charge_beyond_the_connection_cap_is_clamped_and_proceeds() {
    let cap = 2 * COST;
    let credit = Arc::new(IngressCredit::new(POOL, cap, OverloadMode::Pause));
    let policy = policy_with_copies(credit.clone(), 5);
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let (mut queue, _outbound) = stalled_hub(hub_rx);
    let (mut reader, mut writer) = open(&policy, &hub_tx, V4);
    writer
        .send(&connect_packet("big-copier", true))
        .await
        .unwrap();
    assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
    writer
        .send(&publish(QoS::AtMostOnce, None, 0, 0))
        .await
        .unwrap();
    let batch = tokio::time::timeout(Duration::from_secs(10), queue.recv())
        .await
        .expect("a batch costing more than the cap must not wait forever")
        .expect("the publish reaches the hub");
    assert!(matches!(batch, HubCommand::PublishBatch(_)));
    assert_eq!(credit.in_use(), cap, "charged up to the cap, no further");
}

/// Answer every gate in a batch as the hub would on success, then drop it — which
/// returns its credit.
fn accept_batch(cmd: HubCommand) {
    for done in dispatch_batch(cmd) {
        let _ = done.send(crate::hub::PublishOutcome::Accepted);
    }
}

/// Dispatch a batch without answering it yet: its credit is returned (the batch is
/// dropped), and the gates it carried are handed back for the test to answer.
fn dispatch_batch(cmd: HubCommand) -> Vec<oneshot::Sender<crate::hub::PublishOutcome>> {
    let HubCommand::PublishBatch(batch) = cmd else {
        panic!("expected a batch, got {cmd:?}");
    };
    let crate::hub::PublishBatch {
        original, derived, ..
    } = *batch;
    std::iter::once(original)
        .chain(derived.into_iter().map(|d| d.publish))
        .filter_map(|p| match p {
            HubCommand::Publish { done, .. } => done,
            _ => None,
        })
        .collect()
}

/// Connect a client whose keepalive is 1 s, so a close for silence would come fast.
async fn connect_impatient(writer: &mut Writer, reader: &mut Reader, id: &str) {
    writer
        .send(&Packet::Connect(Connect {
            properties: Properties::new(),
            protocol: V4,
            clean_session: true,
            keep_alive: 1,
            client_id: id.into(),
            last_will: None,
            username: None,
            password: None,
        }))
        .await
        .unwrap();
    assert!(matches!(recv(reader).await, Some(Packet::ConnAck(_))));
}

/// ADR 0083 with ADR 0082 T3 (review of PR #871): a batch whose derived messages find
/// no credit waits parked, the way a publish waiting for credit does: the connection
/// keeps delivering to its client, its keepalive is not enforced, and the batch is not
/// sent until its whole charge is there. Once credit frees, the batch goes out and the
/// publisher is acknowledged. (That it holds none of its own credit while it waits is
/// `a_batch_waiting_for_credit_releases_its_originals_credit_first`.)
#[tokio::test(start_paused = true)]
async fn a_batch_waiting_for_derived_credit_waits_parked_and_keeps_delivering() {
    let credit = Arc::new(IngressCredit::new(4 * COST, 4 * COST, OverloadMode::Pause));
    let filler_policy = policy(Some(credit.clone()), None);
    let copier_policy = policy_with_copies(credit.clone(), 3);
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let (mut queue, mut outbound) = stalled_hub(hub_rx);

    // A publish without rules holds one cost of the pool of four.
    let (mut filler_reader, mut filler_writer) = open(&filler_policy, &hub_tx, V4);
    filler_writer
        .send(&connect_packet("filler", true))
        .await
        .unwrap();
    assert!(matches!(
        recv(&mut filler_reader).await,
        Some(Packet::ConnAck(_))
    ));
    let _filler_out = outbound.recv().await.unwrap();
    filler_writer
        .send(&publish(QoS::AtMostOnce, None, 1, 0))
        .await
        .unwrap();
    let held = queue.recv().await.unwrap();
    assert_eq!(credit.in_use(), COST);

    // The copier (keepalive 1 s): its original fits in what is left, its three copies
    // do not.
    let (mut reader, mut writer) = open(&copier_policy, &hub_tx, V4);
    connect_impatient(&mut writer, &mut reader, "copier").await;
    let out = outbound.recv().await.unwrap();
    writer
        .send(&publish(QoS::AtLeastOnce, Some(1), 0, 0))
        .await
        .unwrap();
    // Let the copier reach its wait. Virtual time: nothing else can move meanwhile.
    // (The pool's FIFO semaphore hands the free bytes to the waiter at its head as they
    // come, so `in_use` here counts the wait's partial grant, not a held permit.)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Parked, the connection still delivers to its client...
    let delivery = publish(QoS::AtMostOnce, None, 9, 9);
    assert!(out.send(delivery.clone()));
    assert_eq!(recv(&mut reader).await, Some(delivery), "outbound flows");
    // ...and ten keepalive graces pass without closing it or sending the batch.
    tokio::time::sleep(Duration::from_secs(15)).await;
    match queue.try_recv() {
        Err(_) => {}
        Ok(HubCommand::Detach { .. }) => panic!("closed while the broker held it"),
        Ok(other) => panic!("the batch went out without its credit: {other:?}"),
    }

    // Free the filler's credit: the batch goes out holding its whole charge.
    drop(held);
    let batch = timeout(Duration::from_secs(1), queue.recv())
        .await
        .expect("the parked batch proceeds once credit frees")
        .unwrap();
    assert_eq!(credit.in_use(), 4 * COST, "the original and three copies");
    accept_batch(batch);
    match recv(&mut reader).await {
        Some(Packet::PubAck(a)) => assert_eq!(a.pkid, 1),
        other => panic!("expected the PUBACK, got {other:?}"),
    }
    assert_eq!(credit.in_use(), 0, "every credit returned");
}

/// A `QoS` 2 batch whose derived messages find no credit waits in place: its PUBREC
/// waits for the hub's answer inside the packet's handling, so the batch cannot be
/// parked behind it. The broker reads nothing from the client meanwhile, so the
/// keepalive restarts once the wait is over rather than closing a client the broker
/// itself kept waiting, and the wait is counted as a pause (review of PR #871).
#[tokio::test(start_paused = true)]
async fn a_qos2_batch_waiting_for_credit_is_not_closed_for_the_brokers_wait() {
    let credit = Arc::new(IngressCredit::new(4 * COST, 4 * COST, OverloadMode::Pause));
    let filler_policy = policy(Some(credit.clone()), None);
    let metrics = Arc::new(Metrics::new("test"));
    let copier_policy = {
        let p = policy_with_copies(credit.clone(), 3);
        Arc::new(ConnPolicy {
            metrics: Some(metrics.clone()),
            ..(*p).clone()
        })
    };
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let (mut queue, mut outbound) = stalled_hub(hub_rx);

    let (mut filler_reader, mut filler_writer) = open(&filler_policy, &hub_tx, V4);
    filler_writer
        .send(&connect_packet("filler", true))
        .await
        .unwrap();
    assert!(matches!(
        recv(&mut filler_reader).await,
        Some(Packet::ConnAck(_))
    ));
    let _filler_out = outbound.recv().await.unwrap();
    filler_writer
        .send(&publish(QoS::AtMostOnce, None, 1, 0))
        .await
        .unwrap();
    let held = queue.recv().await.unwrap();

    let (mut reader, mut writer) = open(&copier_policy, &hub_tx, V4);
    connect_impatient(&mut writer, &mut reader, "copier").await;
    let _out = outbound.recv().await.unwrap();
    writer
        .send(&publish(QoS::ExactlyOnce, Some(1), 0, 0))
        .await
        .unwrap();
    // Ten keepalive graces pass with the batch waiting for credit.
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert!(queue.try_recv().is_err(), "nothing sent without its credit");

    drop(held);
    let batch = timeout(Duration::from_secs(1), queue.recv())
        .await
        .expect("the batch proceeds once credit frees")
        .unwrap();
    accept_batch(batch);
    match recv(&mut reader).await {
        Some(Packet::PubRec(a)) => assert_eq!(a.pkid, 1),
        other => panic!("expected the PUBREC, got {other:?}"),
    }
    // Still open: the keepalive restarted when the wait ended.
    tokio::time::sleep(Duration::from_millis(500)).await;
    match queue.try_recv() {
        Err(_) => {}
        Ok(HubCommand::Detach { .. }) => panic!("closed for the broker's own wait"),
        Ok(other) => panic!("unexpected command {other:?}"),
    }
    writer.send(&Packet::PingReq).await.unwrap();
    assert_eq!(recv(&mut reader).await, Some(Packet::PingResp));
    // The wait is a pause like any other, for the operator's pause alert.
    assert_eq!(counter(&metrics, "mqttd_ingress_paused_total"), 1);
}

/// The same in-place wait reached the other way: a `QoS` 2 publish that first waited
/// parked for its own credit, resumed, and then found none for its derived messages.
/// The keepalive restarts after that wait too, so the client is not closed for it
/// (review of PR #871), and each wait is counted as a pause.
#[tokio::test(start_paused = true)]
async fn a_resumed_qos2_publish_waiting_for_derived_credit_is_not_closed_for_it() {
    let credit = Arc::new(IngressCredit::new(4 * COST, 4 * COST, OverloadMode::Pause));
    let filler_policy = policy(Some(credit.clone()), None);
    let metrics = Arc::new(Metrics::new("test"));
    let copier_policy = {
        let p = policy_with_copies(credit.clone(), 3);
        Arc::new(ConnPolicy {
            metrics: Some(metrics.clone()),
            ..(*p).clone()
        })
    };
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let (mut queue, mut outbound) = stalled_hub(hub_rx);

    // The filler takes the whole pool, one cost per publish.
    let (mut filler_reader, mut filler_writer) = open(&filler_policy, &hub_tx, V4);
    filler_writer
        .send(&connect_packet("filler", true))
        .await
        .unwrap();
    assert!(matches!(
        recv(&mut filler_reader).await,
        Some(Packet::ConnAck(_))
    ));
    let _filler_out = outbound.recv().await.unwrap();
    let mut held = Vec::new();
    for seq in 0..4 {
        filler_writer
            .send(&publish(QoS::AtMostOnce, None, 1, seq))
            .await
            .unwrap();
        held.push(queue.recv().await.unwrap());
    }
    assert_eq!(credit.in_use(), 4 * COST);

    // The copier's QoS 2 publish waits parked for its own cost...
    let (mut reader, mut writer) = open(&copier_policy, &hub_tx, V4);
    connect_impatient(&mut writer, &mut reader, "copier").await;
    let _out = outbound.recv().await.unwrap();
    writer
        .send(&publish(QoS::ExactlyOnce, Some(1), 0, 0))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // ...gets it, and then waits in place for its copies' credit, for ten graces.
    drop(held.pop());
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert!(queue.try_recv().is_err(), "nothing sent without its credit");

    held.clear();
    let batch = timeout(Duration::from_secs(1), queue.recv())
        .await
        .expect("the batch proceeds once credit frees")
        .unwrap();
    accept_batch(batch);
    match recv(&mut reader).await {
        Some(Packet::PubRec(a)) => assert_eq!(a.pkid, 1),
        other => panic!("expected the PUBREC, got {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    match queue.try_recv() {
        Err(_) => {}
        Ok(HubCommand::Detach { .. }) => panic!("closed for the broker's own wait"),
        Ok(other) => panic!("unexpected command {other:?}"),
    }
    writer.send(&Packet::PingReq).await.unwrap();
    assert_eq!(recv(&mut reader).await, Some(Packet::PingResp));
    assert_eq!(
        counter(&metrics, "mqttd_ingress_paused_total"),
        2,
        "the parked wait for its own credit and the in-place wait for its copies'"
    );
}

/// A parked batch keeps its publish's place in the acknowledgement order: a `QoS` 1
/// publish acknowledged after an earlier one whose batch was answered later still gets
/// its PUBACK second, and no PUBACK leaves before the hub has answered it.
#[tokio::test(start_paused = true)]
async fn a_parked_batch_keeps_its_place_in_the_puback_order() {
    let credit = Arc::new(IngressCredit::new(8 * COST, 8 * COST, OverloadMode::Pause));
    let filler_policy = policy(Some(credit.clone()), None);
    let copier_policy = policy_with_copies(credit.clone(), 3);
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    let (mut queue, mut outbound) = stalled_hub(hub_rx);

    let (mut filler_reader, mut filler_writer) = open(&filler_policy, &hub_tx, V4);
    filler_writer
        .send(&connect_packet("filler", true))
        .await
        .unwrap();
    assert!(matches!(
        recv(&mut filler_reader).await,
        Some(Packet::ConnAck(_))
    ));
    let _filler_out = outbound.recv().await.unwrap();
    filler_writer
        .send(&publish(QoS::AtMostOnce, None, 1, 0))
        .await
        .unwrap();
    let _held = queue.recv().await.unwrap();

    let (mut reader, mut writer) = open(&copier_policy, &hub_tx, V4);
    writer.send(&connect_packet("copier", true)).await.unwrap();
    assert!(matches!(recv(&mut reader).await, Some(Packet::ConnAck(_))));
    let _out = outbound.recv().await.unwrap();
    // The first publish and its copies fit (five costs of eight in use)...
    writer
        .send(&publish(QoS::AtLeastOnce, Some(1), 0, 1))
        .await
        .unwrap();
    let first = queue.recv().await.unwrap();
    // ...the second's original would, its copies would not: it parks.
    writer
        .send(&publish(QoS::AtLeastOnce, Some(2), 0, 2))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        queue.try_recv().is_err(),
        "the second batch waits for its credit"
    );

    // The hub dispatches the first batch, freeing its credit, but answers it last.
    let first_gates = dispatch_batch(first);
    let second = timeout(Duration::from_secs(1), queue.recv())
        .await
        .expect("the parked batch proceeds once the first is dispatched")
        .unwrap();
    accept_batch(second);
    assert!(
        timeout(Duration::from_millis(200), reader.next_packet())
            .await
            .is_err(),
        "no PUBACK may overtake the first publish's"
    );
    for done in first_gates {
        let _ = done.send(crate::hub::PublishOutcome::Accepted);
    }
    for pkid in [1, 2] {
        match recv(&mut reader).await {
            Some(Packet::PubAck(a)) => assert_eq!(a.pkid, pkid),
            other => panic!("expected PUBACK {pkid}, got {other:?}"),
        }
    }
}

/// No connection holds credit while it waits for more (ADR 0082: there is no credit
/// cycle). A batch whose derived messages find no credit releases its original's own
/// permit BEFORE its wait for the whole charge begins; otherwise enough connections
/// each holding an original whose command the hub never received could hold the whole
/// pool, and nothing would ever free it (review of PR #871). Checked deterministically,
/// before anything polls the wait: only the other holder's credit is in use.
#[tokio::test]
async fn a_batch_waiting_for_credit_releases_its_originals_credit_first() {
    use crate::conn::{give_batch_credit, send_forwarded, Forwarded, PendingBatch, RuleConn};
    use mqtt_core::{AppProperties, ClientId};
    let credit = Arc::new(IngressCredit::new(2 * COST, 2 * COST, OverloadMode::Pause));
    let other = credit
        .connection()
        .try_acquire(u32::try_from(COST).unwrap())
        .expect("the pool is empty");
    let conn = credit.connection();
    let original_permit = conn
        .try_acquire(u32::try_from(COST).unwrap())
        .expect("the last cost of the pool");
    assert_eq!(credit.in_use(), 2 * COST, "the pool is full");

    let set = mqtt_rules::RuleSet::parse(
        "[rules.copy]\nsql = 'SELECT * FROM \"load/#\"'\nactions = [{ function = \"republish\", args = { topic = \"copy/1\" } }]\n",
    )
    .unwrap()
    .rules;
    let (_tx, rx) = tokio::sync::watch::channel(Arc::new(set));
    let rules = crate::rules::Rules::new(rx, Arc::from("n"), None).for_connection();
    let client = ClientId("copier".into());
    let payload = Bytes::from(vec![0u8; PAYLOAD]);
    let app = AppProperties::default();
    let publisher = crate::rules::Publisher::default();
    let (derived, _) = rules.on_publish(&crate::rules::PublishFacts {
        client: &client,
        publisher: &publisher,
        topic: TOPIC,
        payload: &payload,
        qos: QoS::AtLeastOnce,
        retain: false,
        app: &app,
        message_expiry: None,
    });
    assert_eq!(derived.len(), 1);
    let no_props = mqtt_codec::Properties::new();
    let rule_conn = RuleConn {
        publisher,
        arrival: crate::conn::Arrival::default(),
        close_reason: std::sync::Mutex::new(None),
        disconn_props: std::sync::Mutex::new(None),
        conn_props: &no_props,
        proto_ver: 5,
        keepalive: 0,
        clean_start: true,
        expiry_interval: 0,
        receive_maximum: u16::MAX,
        rules: Some(rules),
        parked_batch: std::sync::Mutex::new(None),
        unacked: std::sync::Mutex::new(std::collections::HashMap::new()),
        unacked_len: std::sync::atomic::AtomicUsize::new(0),
    };
    let (done_tx, done_rx) = oneshot::channel();
    let original = HubCommand::Publish {
        topic: TOPIC.into(),
        payload,
        qos: QoS::AtLeastOnce,
        retain: false,
        message_expiry: None,
        app,
        done: Some(done_tx),
        v5: false,
        publisher: Some(client),
        credit: None,
    };
    let (hub_tx, mut hub_rx) = mpsc::unbounded_channel();
    let (rx, holds) = send_forwarded(
        Forwarded::Batch(Box::new(PendingBatch {
            original,
            done: Some(done_rx),
            derived,
            credit: Some(original_permit),
            charged: u32::try_from(COST).unwrap(),
        })),
        &hub_tx,
        Some(&conn),
        &rule_conn,
        QoS::AtLeastOnce,
        None,
    )
    .await;
    assert!(rx.is_some() && holds == 2, "the ack waits on the batch");
    assert!(
        hub_rx.try_recv().is_err(),
        "not sent: its credit is not there"
    );
    assert_eq!(
        credit.in_use(),
        COST,
        "waiting, the batch holds none of its own credit — only the other holder's is in use"
    );

    // The wait is for the whole charge; freeing the other holder's credit lets it end.
    let (batch, wait) = rule_conn.take_parked().expect("the batch is parked");
    drop(other);
    let permit = timeout(Duration::from_secs(5), wait)
        .await
        .expect("the whole charge fits under the cap, so the wait ends");
    let mut batch = batch;
    give_batch_credit(&mut batch, permit);
    assert_eq!(credit.in_use(), 2 * COST, "the original and its copy");
    drop(batch);
    assert_eq!(credit.in_use(), 0);
}
