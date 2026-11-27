//! What a hub shard pays to publish its routing (ADR 0093 Amendment 4): one copy of its
//! subscription table and one of its `$share` table, made at the end of each dispatch
//! batch in which either changed, as a function of how many subscriptions it holds.
//!
//! The copy is proportional to the table. A shard whose tables change in most batches
//! (a reconnect storm, device churn) pays it per batch, so these numbers say how large
//! a shard's routing can grow before publishing it shows in the hub's time. The node's
//! default, one shard, never copies anything.
//!
//! Shapes:
//! - `subscription_table/N`: N devices, each subscribed to its own command topic
//!   (`devices/<i>/cmd`), the widest trie a fleet builds;
//! - `shared_table/N`: N `$share` members over 100 groups of one wildcard filter.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use mqtt_core::{ClientId, QoS, SharedSubscriptionTable, SubscriptionTable};

fn devices(n: usize) -> SubscriptionTable {
    let mut table = SubscriptionTable::new();
    for i in 0..n {
        let filter = table.intern(&format!("devices/{i}/cmd"));
        table.subscribe(ClientId(format!("device-{i}").into()), filter);
    }
    table
}

fn members(n: usize) -> SharedSubscriptionTable {
    let mut table = SharedSubscriptionTable::new();
    for i in 0..n {
        table.subscribe(
            ClientId(format!("member-{i}").into()),
            &format!("g{}", i % 100),
            "plant/+/telemetry",
            QoS::AtLeastOnce,
            true,
        );
    }
    table
}

fn routing_copy(c: &mut Criterion) {
    let mut group = c.benchmark_group("routing_copy");
    for n in [1_000, 10_000, 100_000] {
        let table = devices(n);
        group.bench_with_input(
            BenchmarkId::new("subscription_table", n),
            &table,
            |b, table| b.iter(|| table.clone()),
        );
        let shared = members(n);
        group.bench_with_input(BenchmarkId::new("shared_table", n), &shared, |b, shared| {
            b.iter(|| shared.clone());
        });
    }
    group.finish();
}

criterion_group!(benches, routing_copy);
criterion_main!(benches);
