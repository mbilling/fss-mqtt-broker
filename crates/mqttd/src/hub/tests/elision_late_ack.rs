//! Write elision (ADR 0089) under PUBACKs that come late, into a `$share` group of
//! persistent consumers: the shape of the h3 durable-qos1 45,000 msg/s step of
//! 2026-10-29, whose verdict reported 297 acknowledged messages "that no broker drop
//! counter or queue accounts for". The interleavings the window allows run here at
//! once — a PUBACK inside the window (discharge), after it and before the fallback
//! write lands, after the write lands; sessions backed off (A3) and written directly
//! while earlier fallback writes are still in their lane — and every publish the hub
//! accepted must reach a consumer. It does: the 297 were still held by the broker, as
//! unacked deliveries, until the generator ended its sessions; the verdict read the
//! broker's gauges after that (bench/scale/rig2/verdict.py, `held_at_cutoff`).

use super::*;
use std::collections::HashSet;
use std::sync::Mutex;

fn publish_owned(tx: &HubTx, topic: &str, payload: Bytes) -> oneshot::Receiver<PublishOutcome> {
    let (done_tx, done_rx) = oneshot::channel();
    tx.send(HubCommand::Publish {
        topic: topic.into(),
        payload,
        qos: QoS::AtLeastOnce,
        retain: false,
        message_expiry: None,
        app: mqtt_core::AppProperties::default(),
        done: Some(done_tx),
        v5: true,
        publisher: None,
        credit: None,
    })
    .unwrap();
    done_rx
}

fn counter(metrics: &mqtt_observability::metrics::Metrics, name: &str) -> f64 {
    metrics
        .render()
        .lines()
        .filter(|l| l.starts_with(name))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_pubacks_into_a_shared_group_lose_nothing_a_publisher_was_acked_for() {
    const CONSUMERS: usize = 8;
    const MESSAGES: usize = 6000;
    let store = FlakyStore::new(0);
    // The write a timed-out hold falls back to takes a while, so late PUBACKs land
    // both before and after it.
    store
        .write_delay_ms
        .store(4, std::sync::atomic::Ordering::SeqCst);
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new(
        "elision-late-ack",
    ));
    let (mut hub, tx) = Hub::with_config(NodeId("hub-test".into()), store.clone());
    hub.set_write_elision_ms(5);
    hub.attach_metrics(metrics.clone());
    let hub_task = tokio::spawn(hub.run());

    let received: Arc<Mutex<HashSet<Vec<u8>>>> = Arc::default();
    let mut consumers = Vec::new();
    for i in 0..CONSUMERS {
        let name = format!("c{i}");
        let (mut rx, _) = attach_full(&tx, &name, 1 + i as u64, false, 30, 256).await;
        subscribe_qos(&tx, &name, "$share/g/t", QoS::AtLeastOnce);
        let (tx, received) = (tx.clone(), received.clone());
        consumers.push(tokio::spawn(async move {
            // Each delivery is acknowledged 0-11 ms after it arrives, against a 5 ms
            // window and a 4 ms write: about half the holds are discharged, the rest
            // time out with their PUBACK landing before or after the fallback write,
            // and a session misses the window often enough to be backed off.
            let mut seed: u64 = 0x9E37_79B9_7F4A_7C15 ^ (i as u64 + 1);
            while let Some(out) = rx.recv().await {
                let Some(Packet::Publish(p)) = out.into_packet() else {
                    continue;
                };
                received.lock().unwrap().insert(p.payload.to_vec());
                let pkid = p.pkid.expect("a QoS 1 delivery");
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let delay = Duration::from_millis(seed % 12);
                let (tx, client) = (tx.clone(), ClientId(name.clone().into()));
                tokio::spawn(async move {
                    // SETTLE(elision-late-puback): the delay IS the fault under test —
                    // a subscriber's PUBACK landing before, inside or after the window
                    // and the fallback write; there is no state to poll for instead.
                    tokio::time::sleep(delay).await;
                    // On the control lane, as the connection sends it (ADR 0082 T2).
                    let _ = tx.send(HubCommand::PubAck { client, pkid });
                });
            }
        }));
    }
    let _ = attach(&tx, "barrier", 999, true).await;

    let mut dones = Vec::new();
    for n in 0..MESSAGES {
        let payload = Bytes::from(format!("m{n}").into_bytes());
        dones.push((payload.clone(), publish_owned(&tx, "t", payload)));
        if n % 2 == 1 {
            // SETTLE(elision-late-ack-pacing): the pause is the publish RATE (~2 per ms),
            // not a wait: holds, timeouts and writes must overlap, not run in one burst.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    let mut accepted = Vec::new();
    for (payload, done) in dones {
        if matches!(
            timeout(Duration::from_secs(30), done).await,
            Ok(Ok(PublishOutcome::Accepted))
        ) {
            accepted.push(payload);
        }
    }
    assert!(
        accepted.len() > MESSAGES / 2,
        "only {} of {MESSAGES} publishes were accepted",
        accepted.len()
    );
    // Every delivery went on the wire at once or after its write; give the last
    // writes time to land.
    let deadline = Instant::now() + Duration::from_secs(10);
    let missing: Vec<Bytes> = loop {
        let missing: Vec<Bytes> = {
            let got = received.lock().unwrap();
            accepted
                .iter()
                .filter(|p| !got.contains(p.as_ref()))
                .cloned()
                .collect()
        };
        if missing.is_empty() || Instant::now() > deadline {
            break missing;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        missing.is_empty(),
        "{} of {} accepted publishes reached no consumer, e.g. {:?}",
        missing.len(),
        accepted.len(),
        missing.iter().take(10).collect::<Vec<_>>()
    );
    // The interleavings this is about all ran.
    let discharged = counter(&metrics, "mqttd_durable_elided_total");
    let timed_out = counter(
        &metrics,
        "mqttd_durable_elision_fallback_total{reason=\"timeout\"}",
    );
    let backoffs = counter(&metrics, "mqttd_durable_elision_backoff_total");
    assert!(
        discharged > 0.0 && timed_out > 0.0 && backoffs > 0.0,
        "discharged {discharged}, timed out {timed_out}, backed off {backoffs}"
    );
    for c in consumers {
        c.abort();
    }
    hub_task.abort();
}
