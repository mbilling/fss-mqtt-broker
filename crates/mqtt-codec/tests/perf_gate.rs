//! Performance regression **gate** for the codec hot path (ADR 0044 P6).
//!
//! The criterion benches (`benches/codec.rs`) produce precise numbers for
//! humans and the nightly baseline; this is the automatic guard that runs on
//! every PR. It measures PUBLISH encode→decode round-trip throughput and
//! asserts a **floor**, so a gross regression (an allocation added to the hot
//! loop, an accidental O(n²)) fails the build immediately.
//!
//! This test runs in the ordinary (unoptimized) **test profile**, so its
//! absolute number is far below the release benches — that is expected. The
//! floor is set ~4× below what the unoptimized path sustains on a CI runner,
//! so machine variance never trips it while a real algorithmic regression,
//! which shows in any profile, does.
//!
//! It is a floor, not a target: a green here means "not catastrophically
//! slower", and the real numbers live in the benches. If this ever flakes on a
//! genuinely slow runner, lower `FLOOR_OPS_PER_SEC` — do not delete the gate.

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::Publish;
use mqtt_codec::{Packet, Properties, ProtocolVersion, QoS};

/// The floor: PUBLISH encode+decode round-trips per second the hot path must
/// clear **in the unoptimized test profile**. Observed ~650k/s on a 2026 CI
/// runner (the release benches, opt-level 3, run ~5–7× faster); this floor is
/// ~4× below that, wide enough that variance never trips it.
const FLOOR_OPS_PER_SEC: f64 = 150_000.0;

/// How many round-trips to time. Large enough to amortize the clock read and
/// swamp scheduler noise; small enough to finish in well under a second.
const ITERS: u32 = 200_000;

#[test]
fn publish_codec_round_trip_clears_the_floor() {
    let pkt = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "sensors/room/42/temperature".to_string(),
        pkid: Some(7),
        properties: Properties::new(),
        payload: Bytes::from(vec![0xABu8; 256]),
    });

    // Warm the caches / branch predictors before timing.
    for _ in 0..10_000 {
        let mut out = Vec::with_capacity(320);
        pkt.encode(&mut out, ProtocolVersion::V5).unwrap();
        let mut buf = BytesMut::from(&out[..]);
        std::hint::black_box(Packet::decode(&mut buf, ProtocolVersion::V5).unwrap());
    }

    let start = Instant::now();
    for _ in 0..ITERS {
        let mut out = Vec::with_capacity(320);
        pkt.encode(&mut out, ProtocolVersion::V5).unwrap();
        let mut buf = BytesMut::from(&out[..]);
        std::hint::black_box(Packet::decode(&mut buf, ProtocolVersion::V5).unwrap());
    }
    let elapsed = start.elapsed();
    let ops_per_sec = f64::from(ITERS) / elapsed.as_secs_f64();

    let ns_per_op = elapsed.as_secs_f64() * 1e9 / f64::from(ITERS);
    eprintln!(
        "codec round-trip: {ops_per_sec:.0} ops/s ({ns_per_op:.0} ns/op), floor {FLOOR_OPS_PER_SEC:.0}"
    );
    assert!(
        ops_per_sec >= FLOOR_OPS_PER_SEC,
        "codec hot-path regression: {ops_per_sec:.0} round-trips/s is below the \
         {FLOOR_OPS_PER_SEC:.0}/s floor (see docs/benchmarks/BASELINE.md; run \
         `cargo bench -p mqtt-codec` to investigate)"
    );
}
