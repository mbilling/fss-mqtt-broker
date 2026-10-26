//! A subscriber that falls behind gets every message it is owed, once its
//! connection's outbound queue holds more than one write batch.
//!
//! Found by the rig2 msg/s ramp (ADR 0090): clean-session `QoS` 1 through `$share`
//! groups at ~100k msg/s acknowledged every publish, counted every message
//! `delivered` and nothing dropped, and the consumers still missed a few. Each
//! missing message is a skipped packet id on its consumer's connection, one every
//! 1025 ids while that connection is behind: the connection task's outbound drain
//! (`conn.rs`, the `out_rx.recv()` arm) coalesces up to `OUTBOUND_BATCH_MAX` (1024)
//! packets per write, and on the last turn of its loop it takes one more packet off
//! the channel with `try_recv` and then leaves the loop, dropping it unwritten.
//!
//! This test makes that happen deterministically: a `QoS` 1 subscriber with a large
//! Receive Maximum stops reading, a publisher's messages are all acknowledged and
//! queued for it, then it reads them all. Every message must arrive, with no gap in
//! its packet ids, and the broker must count nothing dropped.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscribeFilter};
use mqtt_codec::{Packet, Properties, Property, ProtocolVersion, QoS, SubscriptionOptions};
use mqttd::hub::Hub;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

const V5: ProtocolVersion = ProtocolVersion::V5;
/// More than four write batches' worth, so the drain runs several full batches.
const MESSAGES: u32 = 5000;
/// Large enough that the messages cannot all sit in the kernel's socket buffers: the
/// broker's writer blocks and the rest wait in the outbound queue.
const PAYLOAD: usize = 4096;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

async fn start_broker() -> (
    std::net::SocketAddr,
    Arc<mqtt_observability::metrics::Metrics>,
) {
    let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("ob"));
    let (mut hub, hub_tx) = Hub::new();
    hub.attach_metrics(metrics.clone());
    tokio::spawn(hub.run());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(mqttd::conn::handle(stream, hub_tx.clone()));
        }
    });
    (addr, metrics)
}

async fn send(stream: &mut TcpStream, p: &Packet) {
    let mut out = Vec::new();
    p.encode(&mut out, V5).unwrap();
    stream.write_all(&out).await.unwrap();
}

async fn recv(stream: &mut TcpStream, buf: &mut BytesMut) -> Packet {
    tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            if let Some(p) = Packet::decode(buf, V5).unwrap() {
                return p;
            }
            assert!(stream.read_buf(buf).await.unwrap() > 0, "broker closed");
        }
    })
    .await
    .expect("a packet within the timeout")
}

/// CONNECT (clean, MQTT 5, Receive Maximum 65535) and CONNACK; returns the broker's
/// Receive Maximum.
async fn connect(stream: &mut TcpStream, buf: &mut BytesMut, id: &str) -> u16 {
    send(
        stream,
        &Packet::Connect(Connect {
            protocol: V5,
            clean_session: true,
            keep_alive: 60,
            client_id: id.into(),
            last_will: None,
            username: None,
            password: None,
            properties: vec![Property::ReceiveMaximum(u16::MAX)].into(),
        }),
    )
    .await;
    match recv(stream, buf).await {
        Packet::ConnAck(a) if a.code == 0 => a.properties.receive_maximum().unwrap_or(u16::MAX),
        other => panic!("expected a successful CONNACK, got {other:?}"),
    }
}

fn dropped_total(metrics: &mqtt_observability::metrics::Metrics) -> Vec<String> {
    metrics
        .render()
        .lines()
        .filter(|l| l.starts_with("mqttd_publish_dropped_total") && !l.ends_with(" 0"))
        .map(str::to_string)
        .collect()
}

fn delivered_total(metrics: &mqtt_observability::metrics::Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .filter(|l| l.starts_with("mqttd_publish_delivered_total"))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

/// Publish `MESSAGES` `QoS` 1 messages (payload: the index, big-endian, then zeros),
/// a window at a time, each acknowledged before the next window.
async fn publish_all(addr: std::net::SocketAddr) {
    let mut publ = TcpStream::connect(addr).await.unwrap();
    let mut buf = BytesMut::new();
    let window = u32::from(connect(&mut publ, &mut buf, "ob-pub").await.min(100));
    let mut next = 0u32;
    while next < MESSAGES {
        let n = window.min(MESSAGES - next);
        let mut out = Vec::new();
        for i in next..next + n {
            let mut payload = vec![0u8; PAYLOAD];
            payload[..4].copy_from_slice(&i.to_be_bytes());
            Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: "ob/t".into(),
                pkid: Some(u16::try_from(i % 60_000 + 1).unwrap()),
                payload: Bytes::from(payload),
                properties: Properties::new(),
            })
            .encode(&mut out, V5)
            .unwrap();
        }
        publ.write_all(&out).await.unwrap();
        for _ in 0..n {
            match recv(&mut publ, &mut buf).await {
                Packet::PubAck(a) => assert!(a.reason < 0x80, "PUBACK refused: {a:?}"),
                other => panic!("expected PUBACK, got {other:?}"),
            }
        }
        next += n;
    }
}

/// Read deliveries, acking each, until every message is in or nothing has arrived
/// for a while. Returns the indices received and the packet-id gaps (last, next).
async fn read_all(sub: &mut TcpStream, buf: &mut BytesMut) -> (BTreeSet<u32>, Vec<(u16, u16)>) {
    let mut got = BTreeSet::new();
    let mut gaps = Vec::new();
    let mut last_pkid: Option<u16> = None;
    let quiet = Duration::from_secs(3);
    while got.len() < MESSAGES as usize {
        let p = loop {
            if let Some(p) = Packet::decode(buf, V5).unwrap() {
                break Some(p);
            }
            match tokio::time::timeout(quiet, sub.read_buf(buf)).await {
                Ok(Ok(n)) if n > 0 => {}
                _ => break None,
            }
        };
        let Some(Packet::Publish(p)) = p else {
            break;
        };
        let pkid = p.pkid.expect("QoS 1 delivery has a packet id");
        if let Some(last) = last_pkid {
            if pkid != last.checked_add(1).unwrap_or(1) {
                gaps.push((last, pkid));
            }
        }
        last_pkid = Some(pkid);
        got.insert(u32::from_be_bytes(p.payload[..4].try_into().unwrap()));
        let [hi, lo] = pkid.to_be_bytes();
        sub.write_all(&[0x40, 0x02, hi, lo]).await.unwrap();
    }
    (got, gaps)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_behind_by_more_than_a_write_batch_gets_every_acked_qos1_message() {
    let (addr, metrics) = start_broker().await;

    // The consumer: a small receive buffer, so the broker's writer blocks early.
    let sock = TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(8 * 1024).unwrap();
    let mut sub = sock.connect(addr).await.unwrap();
    let mut sub_buf = BytesMut::new();
    connect(&mut sub, &mut sub_buf, "ob-sub").await;
    send(
        &mut sub,
        &Packet::Subscribe(Subscribe {
            pkid: 1,
            filters: vec![SubscribeFilter {
                path: "$share/g/ob/t".into(),
                qos: QoS::AtLeastOnce,
                options: SubscriptionOptions::default(),
            }],
            properties: Properties::new(),
        }),
    )
    .await;
    match recv(&mut sub, &mut sub_buf).await {
        Packet::SubAck(a) => assert_eq!(a.return_codes, vec![1], "QoS 1 granted"),
        other => panic!("expected SUBACK, got {other:?}"),
    }

    // Every message acknowledged to its publisher; the consumer reads nothing meanwhile.
    publish_all(addr).await;

    // Bounded poll: the hub has handed every message to the consumer's connection.
    let deadline = Instant::now() + IO_TIMEOUT;
    while delivered_total(&metrics) < u64::from(MESSAGES) {
        assert!(
            Instant::now() < deadline,
            "the hub never counted every message delivered: {}",
            delivered_total(&metrics)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let (got, gaps) = read_all(&mut sub, &mut sub_buf).await;
    let missing: Vec<u32> = (0..MESSAGES).filter(|i| !got.contains(i)).collect();
    assert!(
        missing.is_empty() && gaps.is_empty(),
        "{} of {MESSAGES} acknowledged messages never reached the subscriber \
         (first: {:?}); packet-id gaps (last, next): {:?}; broker drop counters: {:?}; \
         delivered counted: {}",
        missing.len(),
        &missing[..missing.len().min(10)],
        &gaps[..gaps.len().min(10)],
        dropped_total(&metrics),
        delivered_total(&metrics),
    );
    assert!(
        dropped_total(&metrics).is_empty(),
        "{:?}",
        dropped_total(&metrics)
    );
}
