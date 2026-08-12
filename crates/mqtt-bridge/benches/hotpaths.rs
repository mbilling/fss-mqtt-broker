//! Micro-benchmarks for the bridge's per-message hot paths (ADR 0060 T7, ADR 0044 P6).
//!
//! Every message that crosses the boundary pays these costs, so they are the ones a code
//! change can regress. They exist as the **baseline for ADR 0060 T8** — the pending-ack model
//! adds an obligation insert/lookup alongside the routing decision, and §5 claims that costs
//! latency, not throughput. A claim about a hot path needs a number before and after.
//!
//! - **route** — [`plan_forwards`] runs for every inbound message on every side: the hop-limit
//!   check, the per-rule direction + topic match, and the remap. Pure and allocation-light by
//!   design (the security-relevant decisions live here, ADR 0025 §4–6).
//! - **stamp** — [`set_hop_count`] rebuilds the outgoing property set per forward (§6).
//! - **own** — [`owns`] decides partitioned-HA ownership per message (ADR 0059).
//! - **spool** — the in-memory push/drain path, measured under **both** overflow policies:
//!   `Refuse` (the `QoS`≥1 default — reject the newcomer, keep what was acked) and
//!   `DropOldest` (`QoS` 0). At the cap these do different work, worth watching separately.
//!
//! In-memory only (no redb, no fsync), matching `mqtt-cluster/benches/durable_hotpaths.rs`:
//! this isolates the CPU cost a code change can regress; the disk cost is the hardware's. The
//! fast-path *throughput* claim in ADR 0060 §5 is network-bound and belongs to the macro
//! harness (`bench/`, ADR 0048), not here.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mqtt_bridge::config::BridgeConfig;
use mqtt_bridge::forward::{owns, plan_forwards, set_hop_count, Side};
use mqtt_bridge::spool::{Overflow, Spool, SpooledMessage};
use mqtt_codec::properties::{Properties, Property};

/// A representative config: one upstream, a wildcard `out` rule with a remap (the shape the
/// demo and the docs use), plus an `in` rule so both directions are exercised.
fn cfg() -> BridgeConfig {
    BridgeConfig::parse_toml(
        r#"
        share_group = ""
        [local]
        url = "local:1883"
        [spool]
        allow_ephemeral_spool = true
        [[upstreams]]
        name = "partner"
        url = "partner:8883"
        [[upstreams.rules]]
        direction = "out"
        filter = "telemetry/#"
        qos = 1
        remap = { strip_prefix = "telemetry/", prefix = "org/telemetry/" }
        [[upstreams.rules]]
        direction = "in"
        filter = "commands/+"
        "#,
    )
    .expect("bench config")
}

fn bench_route(c: &mut Criterion) {
    let cfg = cfg();
    let mut group = c.benchmark_group("route");
    group.throughput(Throughput::Elements(1));
    // A matching local-origin topic (forwards, with a remap) and a non-matching one (the
    // deny-by-default miss, which every unrelated message on the bridge's stream pays).
    for (name, topic) in [
        ("match_out", "telemetry/room/temperature"),
        ("miss", "unrelated/room/temperature"),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| std::hint::black_box(plan_forwards(&cfg, Side::Local, topic, 0)));
        });
    }
    group.finish();
}

fn bench_stamp(c: &mut Criterion) {
    // The publisher's user properties ride along (ADR 0030) with the hop count replaced (§6),
    // so the cost scales with how many properties the publisher set.
    let mut group = c.benchmark_group("stamp_hop_count");
    for &n in &[0usize, 4] {
        let mut props: Vec<Property> = (0..n)
            .map(|i| Property::UserProperty(format!("k{i}"), format!("v{i}")))
            .collect();
        props.push(Property::UserProperty(
            "fss-bridge-hop-count".into(),
            "1".into(),
        ));
        let props = Properties(props);
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &props, |b, props| {
            b.iter(|| std::hint::black_box(set_hop_count(props, 2)));
        });
    }
    group.finish();
}

fn bench_own(c: &mut Criterion) {
    // Partitioned HA (ADR 0059) runs this for every message on every instance.
    c.bench_function("owns_partitioned", |b| {
        b.iter(|| std::hint::black_box(owns("telemetry/room/temperature", 4, 1)));
    });
}

fn msg(payload_len: usize) -> SpooledMessage {
    SpooledMessage {
        topic: "org/telemetry/room/temperature".to_string(),
        payload: vec![0x5A; payload_len],
        qos: 1,
        retain: false,
        user_properties: vec![("fss-bridge-hop-count".to_string(), "1".to_string())],
    }
}

fn bench_spool(c: &mut Criterion) {
    let mut group = c.benchmark_group("spool_mem");
    group.throughput(Throughput::Elements(1));
    // Below the cap both policies do the same work: this is the ordinary store-and-forward
    // push while a side is down.
    group.bench_function("push_below_cap", |b| {
        let s = Spool::in_memory(4096);
        let m = msg(256);
        b.iter(|| {
            let _ = s.push(&m);
        });
    });
    // At the cap the policies diverge: Refuse rejects without touching the queue (the QoS≥1
    // default, ADR 0060 T2/T5); DropOldest evicts and audits, then appends.
    for (name, policy) in [
        ("push_at_cap_refuse", Overflow::Refuse),
        ("push_at_cap_drop_oldest", Overflow::DropOldest),
    ] {
        group.bench_function(name, |b| {
            let s = Spool::in_memory(1).with_overflow(policy);
            let m = msg(256);
            let _ = s.push(&m); // fill it, so every measured push is at the cap
            b.iter(|| {
                let _ = s.push(&m);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_route, bench_stamp, bench_own, bench_spool);
criterion_main!(benches);
