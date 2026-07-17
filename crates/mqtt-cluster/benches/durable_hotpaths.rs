//! Micro-benchmarks for the durable-plane hot paths (ADR 0044 P6).
//!
//! Two paths run per replicated message and are worth watching for regression:
//!
//! - **replica apply** — every quorum-replicated append lands here on each
//!   follower ([`ReplicaState::apply`]); the in-memory apply is the CPU cost
//!   before the fsync batch (ADR 0027 amortizes the disk).
//! - **peer frame codec** — every cross-node frame (replication, recovery,
//!   forwarded publish) is `bincode`-encoded on send and decoded on receive.
//!
//! In-memory only (no redb, no fsync): these isolate the CPU cost, which is
//! what a code change can regress; the disk cost is the hardware's.

use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mqtt_cluster::cluster_log::{ReplOp, ReplicaState};
use mqtt_cluster::peer::{decode, encode, PeerMessage};

fn append(offset: u64, record_len: usize) -> ReplOp {
    ReplOp::Append {
        key: "q/device-000000000000".to_string(),
        offset,
        seq: offset,
        record: vec![0x5Au8; record_len],
    }
}

fn bench_replica_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("replica_apply");
    for &len in &[64usize, 1024] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(len), &len, |b, &len| {
            // Fresh state per batch so offsets stay monotonic without the bench
            // measuring unbounded growth.
            let mut state = ReplicaState::new();
            let mut offset = 0u64;
            b.iter(|| {
                offset += 1;
                state.apply(1, &append(offset, len))
            });
        });
    }
    group.finish();
}

fn bench_peer_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("peer_codec");
    let frame = PeerMessage::Replicate {
        req_id: 42,
        epoch: 7,
        op: append(1, 256),
    };
    let mut encoded = Vec::new();
    encode(&frame, &mut encoded).unwrap();
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function("encode", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(320);
            encode(&frame, &mut out).unwrap();
            out
        });
    });
    group.bench_function("decode", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&encoded[..]);
            decode(&mut buf).unwrap()
        });
    });
    group.finish();
}

criterion_group!(benches, bench_replica_apply, bench_peer_codec);
criterion_main!(benches);
