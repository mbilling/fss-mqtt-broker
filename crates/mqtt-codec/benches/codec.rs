//! Micro-benchmarks for the MQTT codec hot path (ADR 0044 P6).
//!
//! Encoding and decoding a PUBLISH runs on **every** delivered message, twice
//! (in on one connection, out on each subscriber's) — it is the single most
//! executed CPU path in the broker, so it is the first thing to watch for
//! regression. CONNECT and SUBSCRIBE decode are the connection-setup path.
//! Pure CPU, no I/O: these numbers are stable enough to gate on and honest
//! enough to publish (see docs/benchmarks/BASELINE.md).

use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscribeFilter, SubscriptionOptions};
use mqtt_codec::{Packet, Properties, ProtocolVersion, QoS};

fn publish(payload_len: usize) -> Packet {
    Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "sensors/room/42/temperature".to_string(),
        pkid: Some(7),
        properties: Properties::new(),
        payload: Bytes::from(vec![0xABu8; payload_len]),
    })
}

fn connect() -> Packet {
    Packet::Connect(Connect {
        properties: Properties::new(),
        protocol: ProtocolVersion::V5,
        clean_session: true,
        keep_alive: 60,
        client_id: "device-000000000000".to_string(),
        last_will: None,
        username: Some("svc/ingest".to_string()),
        password: Some(Bytes::from_static(b"s3cr3t-token")),
    })
}

fn subscribe() -> Packet {
    Packet::Subscribe(Subscribe {
        pkid: 1,
        properties: Properties::new(),
        filters: vec![
            SubscribeFilter {
                path: "sensors/+/+/temperature".to_string(),
                qos: QoS::AtLeastOnce,
                options: SubscriptionOptions::default(),
            },
            SubscribeFilter {
                path: "actuators/#".to_string(),
                qos: QoS::AtLeastOnce,
                options: SubscriptionOptions::default(),
            },
        ],
    })
}

fn encode_to(packet: &Packet) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    packet.encode(&mut out, ProtocolVersion::V5).unwrap();
    out
}

fn bench_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish");
    // The message sizes an IoT fleet actually sends: a tiny sensor reading, a
    // typical JSON telemetry blob, a fat batched payload.
    for &len in &[16usize, 256, 4096] {
        let pkt = publish(len);
        let bytes = encode_to(&pkt);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(BenchmarkId::new("encode", len), &pkt, |b, pkt| {
            b.iter(|| {
                let mut out = Vec::with_capacity(256);
                pkt.encode(&mut out, ProtocolVersion::V5).unwrap();
                out
            });
        });
        group.bench_with_input(BenchmarkId::new("decode", len), &bytes, |b, bytes| {
            b.iter_batched(
                || BytesMut::from(&bytes[..]),
                |mut buf| Packet::decode(&mut buf, ProtocolVersion::V5).unwrap(),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_setup(c: &mut Criterion) {
    let mut group = c.benchmark_group("setup");
    for (name, pkt) in [("connect", connect()), ("subscribe", subscribe())] {
        let bytes = encode_to(&pkt);
        group.bench_function(BenchmarkId::new("decode", name), |b| {
            b.iter_batched(
                || BytesMut::from(&bytes[..]),
                |mut buf| Packet::decode(&mut buf, ProtocolVersion::V5).unwrap(),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_publish, bench_setup);
criterion_main!(benches);
