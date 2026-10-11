//! What a rule's state call costs (ADR 0085 D14): on the publish path, alone, and
//! under contention from several worker threads.
//!
//! - `publish/*` — one inbound publish evaluated against one rule, as a connection
//!   task does it: the same statement without a state call, then with one. The
//!   difference is what the call adds to a publish.
//! - `op/*` — one call on the store, uncontended: a map operation under one shard's
//!   lock.
//! - `contended/<what>/<threads>` — every thread calling at once, as connection tasks
//!   on a multi-threaded runtime do: `hot_key` is all of them incrementing one key (one
//!   shard, one lock, the worst case), `own_keys` each on keys of its own (the usual
//!   case: a key per device), `lru_full` each inserting new keys into a full `lru`
//!   namespace (an eviction per call, which looks at every shard). The time is per
//!   call, as each thread sees it.
//! - `clock/stamp/<threads>` — one stamp of the hybrid logical clock (D6), which every
//!   write to a replicated namespace will take: alone, and with every thread taking
//!   them. A `local` namespace takes none.
//!
//! `cargo bench -p mqttd --bench rule_state`. The numbers in `docs/RULES.md` are from
//! this.

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use mqtt_core::AppProperties;
use mqtt_rules::crdt::{stable_hash, Hlc};
use mqtt_rules::state::{Eviction, NamespaceSettings, StateNamespace, StateOp, StateStore};
use mqtt_rules::{Functions, PublishInput, RuleSet, Value};
use mqttd::rule_state::{stored, Store};

const CEILING: u64 = 1 << 30;

fn publish(c: &mut Criterion, name: &str, sql: &str) {
    let store = Store::new(CEILING);
    let functions = Functions::default().with_state(store);
    let text = format!(
        "[rules.r]\nsql = '''{sql}'''\nactions = [{{ function = \"republish\", args = {{ topic = \
         \"alerts/${{clientid}}\", payload = \"${{.}}\" }} }}]\n"
    );
    let set = RuleSet::parse_with(&text, &functions)
        .expect("bench rules load")
        .rules;
    let payload = Bytes::from_static(br#"{"temp":35.5,"humidity":40,"site":"plant-7","seq":1}"#);
    let props = AppProperties::default();
    let mut out = Vec::with_capacity(4);
    c.bench_function(&format!("publish/{name}"), |b| {
        b.iter(|| {
            out.clear();
            let mut input = PublishInput::new(
                "device-0042",
                "sensors/device-0042/data",
                &payload,
                1,
                &props,
            );
            input.username = Some("device-0042");
            input.node = "node-0";
            set.on_publish(black_box(&input), &mut |_, _| {}, &mut out);
            black_box(out.len());
        });
    });
}

fn namespace(store: &Arc<Store>, settings: &NamespaceSettings) -> Arc<dyn StateNamespace> {
    store.open("bench", settings).expect("a namespace opens")
}

fn ops(c: &mut Criterion) {
    let store = Store::new(CEILING);
    let ns = namespace(&store, &NamespaceSettings::defaults(CEILING));
    let value = Value::from("a value of a few dozen bytes, as a status is");
    let keys: Vec<String> = (0..10_000)
        .map(|i| format!("state:device-{i:05}"))
        .collect();
    for key in &keys {
        ns.call(&StateOp::Set {
            key: key.as_bytes(),
            value: stored(&value),
            ttl: None,
        })
        .expect("room for the keys");
    }
    let mut i = 0usize;
    let mut next = || {
        i = (i + 1) % keys.len();
        keys[i].as_bytes()
    };
    c.bench_function("op/get", |b| {
        b.iter(|| black_box(ns.call(&StateOp::Get { key: next() })));
    });
    c.bench_function("op/get_missing", |b| {
        b.iter(|| {
            black_box(ns.call(&StateOp::Get {
                key: b"no such key",
            }))
        });
    });
    c.bench_function("op/set", |b| {
        b.iter(|| {
            black_box(ns.call(&StateOp::Set {
                key: next(),
                value: stored(&value),
                ttl: None,
            }))
        });
    });
    c.bench_function("op/set_with_ttl", |b| {
        b.iter(|| {
            black_box(ns.call(&StateOp::Set {
                key: next(),
                value: stored(&value),
                ttl: Some(600),
            }))
        });
    });
    c.bench_function("op/changed_same", |b| {
        b.iter(|| {
            black_box(ns.call(&StateOp::Changed {
                key: next(),
                value: stored(&value),
                ttl: None,
            }))
        });
    });
    let counters = namespace(&store, &NamespaceSettings::defaults(CEILING));
    c.bench_function("op/incr", |b| {
        b.iter(|| {
            black_box(counters.call(&StateOp::Incr {
                key: b"count:device-00042",
                by: 1,
                ttl: None,
            }))
        });
    });
    // A sketch that has seen every value already (the steady state of a distinct
    // count), and one given values it has not seen.
    let hashes: Vec<u64> = keys.iter().map(|key| stable_hash(key.as_bytes())).collect();
    let mut at = 0usize;
    c.bench_function("op/hll_add", |b| {
        b.iter(|| {
            at = (at + 1) % hashes.len();
            black_box(counters.call(&StateOp::HllAdd {
                key: b"devices:plant-7",
                hash: hashes[at],
                ttl: None,
            }))
        });
    });
    let mut fresh = 0u64;
    c.bench_function("op/hll_add_new_value", |b| {
        b.iter(|| {
            fresh += 1;
            black_box(counters.call(&StateOp::HllAdd {
                key: b"devices:plant-8",
                hash: stable_hash(&fresh.to_be_bytes()),
                ttl: None,
            }))
        });
    });
    c.bench_function("op/held", |b| {
        b.iter(|| {
            black_box(counters.call(&StateOp::Held {
                key: b"hot:device-00042",
                holds: true,
                ttl: Some(3600),
            }))
        });
    });
}

/// Which keys the threads of a contended case call.
#[derive(Clone, Copy)]
enum Keys {
    /// All of them one key.
    Hot,
    /// Each a thousand of its own.
    Own,
    /// Each new keys, into a full `lru` namespace.
    LruFull,
}

fn contended(c: &mut Criterion, name: &str, keys: Keys, threads: usize) {
    let store = Store::new(CEILING);
    let settings = match keys {
        Keys::LruFull => NamespaceSettings {
            max_keys: 10_000,
            eviction: Eviction::Lru,
            ..NamespaceSettings::defaults(CEILING)
        },
        _ => NamespaceSettings::defaults(CEILING),
    };
    let ns = namespace(&store, &settings);
    let one = Value::Int(1);
    if matches!(keys, Keys::LruFull) {
        for i in 0..10_000 {
            let key = format!("fill:{i}");
            ns.call(&StateOp::Set {
                key: key.as_bytes(),
                value: stored(&one),
                ttl: None,
            })
            .expect("room to fill");
        }
    }
    c.bench_function(&format!("contended/{name}/{threads}"), |b| {
        b.iter_custom(|iters| {
            // Every thread makes `iters` calls; what is measured is how long each
            // thread took for its own, averaged: the cost of a call as a connection
            // sees it while the others are calling too.
            let barrier = Arc::new(Barrier::new(threads));
            let total: Duration = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..threads)
                    .map(|t| {
                        let (ns, barrier, one) = (ns.clone(), barrier.clone(), &one);
                        scope.spawn(move || {
                            let own: Vec<String> =
                                (0..1_000).map(|i| format!("count:{t}:{i}")).collect();
                            barrier.wait();
                            let started = Instant::now();
                            for i in 0..iters {
                                let n = usize::try_from(i).unwrap_or(0);
                                match keys {
                                    Keys::Hot => {
                                        black_box(ns.call(&StateOp::Incr {
                                            key: b"count:hot",
                                            by: 1,
                                            ttl: None,
                                        }))
                                        .ok();
                                    }
                                    Keys::Own => {
                                        black_box(ns.call(&StateOp::Incr {
                                            key: own[n % own.len()].as_bytes(),
                                            by: 1,
                                            ttl: None,
                                        }))
                                        .ok();
                                    }
                                    Keys::LruFull => {
                                        let key = format!("new:{t}:{i}");
                                        black_box(ns.call(&StateOp::Set {
                                            key: key.as_bytes(),
                                            value: stored(one),
                                            ttl: None,
                                        }))
                                        .ok();
                                    }
                                }
                            }
                            started.elapsed()
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap_or_default())
                    .sum()
            });
            total / u32::try_from(threads).unwrap_or(1)
        });
    });
}

/// One stamp of the clock, as each of `threads` threads taking them at once sees it.
fn clock(c: &mut Criterion, threads: usize) {
    let clock = Hlc::new("node-0");
    c.bench_function(&format!("clock/stamp/{threads}"), |b| {
        b.iter_custom(|iters| {
            let barrier = Barrier::new(threads);
            let total: Duration = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..threads)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            let started = Instant::now();
                            for _ in 0..iters {
                                black_box(clock.now(black_box(1_800_000_000_000)));
                            }
                            started.elapsed()
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap_or_default())
                    .sum()
            });
            total / u32::try_from(threads).unwrap_or(1)
        });
    });
}

fn bench(c: &mut Criterion) {
    publish(
        c,
        "no_state",
        "SELECT clientid, payload.temp AS temp FROM \"sensors/+/data\" WHERE payload.temp > 30",
    );
    publish(
        c,
        "kv_incr",
        "SELECT clientid, payload.temp AS temp, kv_incr('n:' + clientid) AS n FROM \
         \"sensors/+/data\" WHERE payload.temp > 30",
    );
    publish(
        c,
        "kv_held",
        "SELECT clientid, payload.temp AS temp, kv_held('hot:' + clientid, payload.temp > 30, \
         3600) AS hot_for FROM \"sensors/+/data\" WHERE hot_for >= 0",
    );
    publish(
        c,
        "kv_changed",
        "SELECT clientid, payload.temp AS temp FROM \"sensors/+/data\" WHERE \
         kv_changed('site:' + clientid, payload.site) = false",
    );
    publish(
        c,
        "kv_store_put_get",
        "SELECT clientid, kv_store_get(clientid, 0) + 1 AS n, kv_store_put(clientid, n) AS p \
         FROM \"sensors/+/data\" WHERE payload.temp > 30",
    );
    publish(
        c,
        "kv_hll_add",
        "SELECT clientid, payload.temp AS temp, kv_hll_add('devices:' + payload.site, clientid) \
         AS devices FROM \"sensors/+/data\" WHERE payload.temp > 30",
    );
    ops(c);
    let cores = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    let mut counts = vec![1, 2, 4, 8, cores];
    counts.retain(|threads| *threads <= cores);
    counts.sort_unstable();
    counts.dedup();
    for threads in counts {
        contended(c, "hot_key", Keys::Hot, threads);
        contended(c, "own_keys", Keys::Own, threads);
        contended(c, "lru_full", Keys::LruFull, threads);
        clock(c, threads);
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1));
    targets = bench
}
criterion_main!(benches);
