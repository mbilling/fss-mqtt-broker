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
//! - `eventual/*` — the same calls in an `eventual` namespace (D5): stamped, kept as
//!   the state that merges, and put in the order a peer link reads. `…/alone` is a
//!   node no link reads from (a key keeps its place in that order once it has one);
//!   `…/shipped` has a link reading all the while, as in a cluster that keeps up, so
//!   a key goes to the end of the order again each time it is sent.
//! - `replication/*` — two stores and a real peer link over loopback TCP, each side
//!   the link task the broker runs: `catch_up_100k` is a node that comes up empty
//!   beside one that holds 100,000 keys, timed until it holds them all;
//!   `write_to_read` one write on one node, timed until the other reads it. The
//!   bytes a key takes on the wire are printed once, for a few kinds of key.
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
use mqtt_rules::state::{
    term_key, Consistency, Eviction, NamespaceSettings, StateNamespace, StateOp, StateStore,
};
use mqtt_rules::{Functions, PublishInput, RuleSet, Value};
use mqttd::rule_state::{stored, StateLink, Store};

const CEILING: u64 = 1 << 30;

fn eventual() -> NamespaceSettings {
    NamespaceSettings {
        consistency: Consistency::Eventual,
        max_keys: 1_000_000,
        ..NamespaceSettings::defaults(CEILING)
    }
}

fn publish(c: &mut Criterion, name: &str, sql: &str) {
    publish_in(c, name, "", sql);
}

/// [`publish`], the rule's own namespace being as `tables` declare it.
fn publish_in(c: &mut Criterion, name: &str, tables: &str, sql: &str) {
    let store = Store::new(CEILING);
    store.set_node("node-0");
    let functions = Functions::default().with_state(store);
    let text = format!(
        "{tables}[rules.r]\nsql = '''{sql}'''\nactions = [{{ function = \"republish\", args = {{ topic = \
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

/// A link that reads everything a store has to send, for as long as it lives: a
/// peer that keeps up, without the peer.
struct Reader {
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Reader {
    fn of(store: &Arc<Store>) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut link = StateLink::new(store.clone(), "node-0", "node-1");
        let stopped = stop.clone();
        let thread = std::thread::spawn(move || {
            while !stopped.load(std::sync::atomic::Ordering::Relaxed) {
                if link.try_next().is_none() {
                    std::thread::sleep(Duration::from_micros(200));
                }
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The calls of `ops`, in an `eventual` namespace; `shipped`: with a link reading.
fn eventual_ops(c: &mut Criterion, shipped: bool) {
    let store = Store::new(CEILING);
    store.set_node("node-0");
    let ns = namespace(&store, &eventual());
    let _reader = shipped.then(|| Reader::of(&store));
    let how = if shipped { "shipped" } else { "alone" };
    let value = Value::from("a value of a few dozen bytes, as a status is");
    let keys: Vec<String> = (0..10_000)
        .map(|i| format!("state:device-{i:05}"))
        .collect();
    let counters: Vec<String> = (0..10_000).map(|i| format!("n:device-{i:05}")).collect();
    for (key, counter) in keys.iter().zip(&counters) {
        ns.call(&StateOp::Set {
            key: key.as_bytes(),
            value: stored(&value),
            ttl: None,
        })
        .expect("room for the keys");
        ns.call(&StateOp::Incr {
            key: counter.as_bytes(),
            by: 1,
            ttl: None,
        })
        .expect("room for the keys");
    }
    let mut i = 0usize;
    let mut next = || {
        i = (i + 1) % keys.len();
        i
    };
    c.bench_function(&format!("eventual/get/{how}"), |b| {
        b.iter(|| {
            black_box(ns.call(&StateOp::Get {
                key: keys[next()].as_bytes(),
            }))
        });
    });
    c.bench_function(&format!("eventual/set/{how}"), |b| {
        b.iter(|| {
            black_box(ns.call(&StateOp::Set {
                key: keys[next()].as_bytes(),
                value: stored(&value),
                ttl: None,
            }))
        });
    });
    c.bench_function(&format!("eventual/set_with_ttl/{how}"), |b| {
        b.iter(|| {
            black_box(ns.call(&StateOp::Set {
                key: keys[next()].as_bytes(),
                value: stored(&value),
                ttl: Some(600),
            }))
        });
    });
    c.bench_function(&format!("eventual/incr/{how}"), |b| {
        b.iter(|| {
            black_box(ns.call(&StateOp::Incr {
                key: counters[next()].as_bytes(),
                by: 1,
                ttl: None,
            }))
        });
    });
    let seen = stable_hash(b"device-0042");
    ns.call(&StateOp::HllAdd {
        key: b"devices",
        hash: seen,
        ttl: None,
    })
    .expect("room for the sketch");
    c.bench_function(&format!("eventual/hll_add_seen/{how}"), |b| {
        b.iter(|| {
            black_box(ns.call(&StateOp::HllAdd {
                key: b"devices",
                hash: seen,
                ttl: None,
            }))
        });
    });
}

/// What the threads of `eventual_contended` do.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Work {
    /// All of them increment one key.
    HotKey,
    /// Each increments keys of its own: no stamp is taken.
    OwnCounters,
    /// Each sets keys of its own: every write takes a stamp of the one clock.
    OwnValues,
}

/// `contended`'s two usual cases, in an `eventual` namespace with a link reading, and
/// a third in which every write is stamped.
fn eventual_contended(c: &mut Criterion, name: &str, work: Work, threads: usize) {
    let store = Store::new(CEILING);
    store.set_node("node-0");
    let ns = namespace(&store, &eventual());
    let _reader = Reader::of(&store);
    c.bench_function(&format!("eventual/contended/{name}/{threads}"), |b| {
        b.iter_custom(|iters| {
            let barrier = Arc::new(Barrier::new(threads));
            let total: Duration = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..threads)
                    .map(|t| {
                        let (ns, barrier) = (ns.clone(), barrier.clone());
                        scope.spawn(move || {
                            let own: Vec<String> =
                                (0..1_000).map(|i| format!("count:{t}:{i}")).collect();
                            let value = Value::Int(1);
                            barrier.wait();
                            let started = Instant::now();
                            for i in 0..iters {
                                let n = usize::try_from(i).unwrap_or(0);
                                let key = if work == Work::HotKey {
                                    &b"count:hot"[..]
                                } else {
                                    own[n % own.len()].as_bytes()
                                };
                                if work == Work::OwnValues {
                                    black_box(ns.call(&StateOp::Set {
                                        key,
                                        value: stored(&value),
                                        ttl: None,
                                    }))
                                    .ok();
                                } else {
                                    black_box(ns.call(&StateOp::Incr {
                                        key,
                                        by: 1,
                                        ttl: None,
                                    }))
                                    .ok();
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

/// What a key takes on the wire: the frame a link sends for it, as the peer bus
/// encodes it, less the frame's own few bytes.
fn wire_bytes() {
    type Fill<'a> = &'a dyn Fn(&dyn StateNamespace, &[u8]);
    let cases: [(&str, Fill<'_>); 6] = [
        ("a counter one node counts", &|ns, key| {
            ns.call(&StateOp::Incr {
                key,
                by: 1,
                ttl: None,
            })
            .ok();
        }),
        ("a 44-byte value (kv_set)", &|ns, key| {
            let value = Value::from("a value of a few dozen bytes, as a status is");
            ns.call(&StateOp::Set {
                key,
                value: stored(&value),
                ttl: None,
            })
            .ok();
        }),
        ("a first time (kv_held)", &|ns, key| {
            ns.call(&StateOp::Held {
                key,
                holds: true,
                ttl: Some(3600),
            })
            .ok();
        }),
        ("a deleted key", &|ns, key| {
            let value = Value::Int(1);
            ns.call(&StateOp::Set {
                key,
                value: stored(&value),
                ttl: None,
            })
            .ok();
            ns.call(&StateOp::Del { key }).ok();
        }),
        ("a set of 10 client ids", &|ns, key| {
            for i in 0..10 {
                let element = Value::from(format!("device-{i:04}"));
                ns.call(&StateOp::Add {
                    key,
                    element: stored(&element),
                    identity: &term_key(&element),
                    ttl: None,
                })
                .ok();
            }
        }),
        ("a sketch of 1,000 values", &|ns, key| {
            for i in 0..1_000 {
                ns.call(&StateOp::HllAdd {
                    key,
                    hash: stable_hash(format!("device-{i:04}").as_bytes()),
                    ttl: None,
                })
                .ok();
            }
        }),
    ];
    println!("rule state on the wire, per key (a 17-byte key, in a frame of 100 keys):");
    for (what, write) in cases {
        let store = Store::new(CEILING);
        store.set_node("node-0");
        let ns = namespace(&store, &eventual());
        let mut link = StateLink::new(store.clone(), "node-0", "node-1");
        // The digest's root, sent when a link comes up.
        let _ = link.try_next();
        for i in 0..100 {
            write(&*ns, format!("state:device-{i:04}").as_bytes());
        }
        let mut bytes = Vec::new();
        while let Some(frame) = link.try_next() {
            mqtt_cluster::peer::encode(&frame, &mut bytes).expect("a frame encodes");
        }
        println!("  {what}: {} bytes", bytes.len() / 100);
    }
}

/// Two stores joined by one real peer link over loopback, each end the link task
/// the broker runs. The hub's side of a link is a task that keeps what it is sent.
struct Pair {
    runtime: tokio::runtime::Runtime,
    a: Arc<Store>,
    a_ns: Arc<dyn StateNamespace>,
}

impl Pair {
    fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("a runtime");
        let a = Store::new(CEILING);
        a.set_node("node-a");
        let a_ns = namespace(&a, &eventual());
        Self { runtime, a, a_ns }
    }

    /// A node "node-b" that comes up with nothing, linked to A. The tasks to stop.
    fn join(
        &self,
    ) -> (
        Arc<Store>,
        Arc<dyn StateNamespace>,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        use mqtt_cluster::NodeId;
        let b = Store::new(CEILING);
        b.set_node("node-b");
        let b_ns = namespace(&b, &eventual());
        let (a, b_store) = (self.a.clone(), b.clone());
        let tasks = self.runtime.block_on(async move {
            let hub = || {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let kept = tokio::spawn(async move {
                    let mut held = Vec::new();
                    while let Some(command) = rx.recv().await {
                        held.push(command);
                    }
                });
                (tx, kept)
            };
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a port");
            let addr = listener.local_addr().expect("an address");
            let (hub_a, kept_a) = hub();
            let (hub_b, kept_b) = hub();
            let serve = tokio::spawn(mqttd::peer::serve_listener_with_ingress(
                listener,
                NodeId("node-b".into()),
                hub_b,
                None,
                None,
                None,
                None,
                Some(b_store),
            ));
            let dial = tokio::spawn(mqttd::peer::dial_forever_with_ingress(
                addr.to_string(),
                NodeId("node-a".into()),
                hub_a,
                None,
                None,
                None,
                Some(a),
            ));
            vec![serve, dial, kept_a, kept_b]
        });
        (b, b_ns, tasks)
    }
}

fn replication(c: &mut Criterion) {
    const KEYS: usize = 100_000;
    wire_bytes();
    let pair = Pair::new();
    let value = Value::from("a value of a few dozen bytes, as a status is");
    for i in 0..KEYS {
        let key = format!("state:device-{i:06}");
        pair.a_ns
            .call(&StateOp::Set {
                key: key.as_bytes(),
                value: stored(&value),
                ttl: None,
            })
            .expect("room for the keys");
    }
    let mut group = c.benchmark_group("replication");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("catch_up_100k", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let started = Instant::now();
                let (store, _ns, tasks) = pair.join();
                while store.stats("bench").map_or(0, |stats| stats.keys) < KEYS {
                    std::thread::sleep(Duration::from_micros(200));
                }
                total += started.elapsed();
                for task in tasks {
                    task.abort();
                }
            }
            total
        });
    });
    // One write on A until B reads it, the link being idle before each.
    let (b_store, b_ns, tasks) = pair.join();
    while b_store.stats("bench").map_or(0, |stats| stats.keys) < KEYS {
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut round = 0i64;
    group.bench_function("write_to_read", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                round += 1;
                let wanted = Value::Int(round);
                // Past the pause a link takes after it has sent everything.
                std::thread::sleep(Duration::from_millis(8));
                let started = Instant::now();
                pair.a_ns
                    .call(&StateOp::Set {
                        key: b"latest",
                        value: stored(&wanted),
                        ttl: None,
                    })
                    .expect("a write");
                loop {
                    let read = b_ns.call(&StateOp::Get { key: b"latest" });
                    if read.is_ok_and(|reply| reply.value.is_some_and(|v| v.exact_eq(&wanted))) {
                        break;
                    }
                    std::hint::spin_loop();
                }
                total += started.elapsed();
            }
            total
        });
    });
    group.finish();
    for task in tasks {
        task.abort();
    }
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
    let eventual_default = "[state.default]\nconsistency = \"eventual\"\n";
    publish_in(
        c,
        "kv_incr_eventual",
        eventual_default,
        "SELECT clientid, payload.temp AS temp, kv_incr('n:' + clientid) AS n FROM \
         \"sensors/+/data\" WHERE payload.temp > 30",
    );
    publish_in(
        c,
        "kv_held_eventual",
        eventual_default,
        "SELECT clientid, payload.temp AS temp, kv_held('hot:' + clientid, payload.temp > 30, \
         3600) AS hot_for FROM \"sensors/+/data\" WHERE hot_for >= 0",
    );
    publish_in(
        c,
        "kv_hll_add_eventual",
        eventual_default,
        "SELECT clientid, payload.temp AS temp, kv_hll_add('devices:' + payload.site, clientid) \
         AS devices FROM \"sensors/+/data\" WHERE payload.temp > 30",
    );
    ops(c);
    eventual_ops(c, false);
    eventual_ops(c, true);
    replication(c);
    let cores = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    let mut counts = vec![1, 2, 4, 8, cores];
    counts.retain(|threads| *threads <= cores);
    counts.sort_unstable();
    counts.dedup();
    for threads in counts {
        contended(c, "hot_key", Keys::Hot, threads);
        contended(c, "own_keys", Keys::Own, threads);
        contended(c, "lru_full", Keys::LruFull, threads);
        eventual_contended(c, "hot_key", Work::HotKey, threads);
        eventual_contended(c, "own_keys", Work::OwnCounters, threads);
        eventual_contended(c, "own_keys_stamped", Work::OwnValues, threads);
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
