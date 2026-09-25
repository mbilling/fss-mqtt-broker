//! #613: separate matching-location cost from connected-peer cost at fixed local
//! channel or TCP shared receipts. Both include endpoint/oracle overhead and use
//! synthetic peer interest: neither is independent-machine scaling evidence.
#[path = "shared_membership/paced.rs"]
mod paced;
#[path = "shared_membership/rig.rs"]
mod rig;
#[path = "shared_membership/tcp.rs"]
mod tcp;

use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use rig::{Rig, Shape, BURST};
use tcp::TcpRig;

// Which completion boundary an arm measures. Chosen once per process by
// MEMBERSHIP_TRANSPORT / MEMBERSHIP_GATED and never mixed inside one group:
// a delivered packet and a released publisher ack are different events, and a
// group whose arms fenced on different events would compare nothing.
#[derive(Clone, Copy)]
enum Mode {
    Channel,
    Gated { routed: bool },
    Tcp,
}

// One matrix/oracle contract, three deliberately named completion boundaries.
enum Fixture {
    Channel(Rig),
    // The gated twin (item 1.6). `routed` publishes on the shared topic, where
    // the receipt oracle still applies because the members' GRANT is QoS 0;
    // unrouted matches nothing, which is what keeps `routing_unsettled()` — and
    // therefore `peers_all`/`Placement::members()` — on the publish hot path
    // whichever side of item 2.1 the hub is on.
    Gated { rig: Rig, routed: bool },
    Tcp(TcpRig),
}

impl Fixture {
    async fn new(drain: &tokio::runtime::Runtime, shape: Shape, mode: Mode) -> Self {
        match mode {
            Mode::Tcp => Self::Tcp(TcpRig::new(drain, shape).await),
            Mode::Channel => Self::Channel(Rig::new(drain, shape).await),
            Mode::Gated { routed } => Self::Gated {
                rig: Rig::gated(drain, shape).await,
                routed,
            },
        }
    }

    async fn burst(&mut self, direct: bool) {
        match self {
            Self::Channel(rig) => rig.burst(direct).await,
            Self::Gated { rig, routed } => {
                assert!(!direct, "the gated fence is an ack, not a channel bypass");
                rig.burst_gated(*routed).await;
            }
            Self::Tcp(rig) => {
                assert!(
                    !direct,
                    "channel bypass does not certify TCP endpoint headroom"
                );
                rig.burst().await;
            }
        }
    }

    async fn verify_peers(&self) {
        match self {
            Self::Channel(rig) | Self::Gated { rig, .. } => rig.verify_peers().await,
            Self::Tcp(rig) => rig.verify_peers().await,
        }
    }
}

// Optional Linux-only placement. Failure is fatal rather than silently running
// an allegedly pinned experiment unpinned. Invoked only during runtime setup.
fn pin_thread(variable: &str) {
    let Ok(cpus) = std::env::var(variable) else {
        return;
    };
    let thread = std::fs::read_link("/proc/thread-self").expect("CPU pinning requires Linux");
    let tid = thread.file_name().expect("thread id");
    assert!(
        std::process::Command::new("taskset")
            .args(["-pc", &cpus])
            .arg(tid)
            .status()
            .expect("CPU pinning requires taskset")
            .success(),
        "failed to apply {variable}"
    );
}

fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    pin_thread("MEMBERSHIP_HUB_CPUS");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let drain_threads = std::env::var("MEMBERSHIP_DRAIN_THREADS")
        .map_or(Ok(2), |s| s.parse::<usize>())
        .expect("MEMBERSHIP_DRAIN_THREADS must be a positive integer");
    assert!(
        drain_threads > 0,
        "at least one endpoint thread is required"
    );
    let drain = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(drain_threads)
        .on_thread_start(|| pin_thread("MEMBERSHIP_DRAIN_CPUS"))
        .enable_all()
        .build()
        .unwrap();
    (rt, drain)
}

fn bench(c: &mut Criterion) {
    let (rt, drain) = runtimes();
    let tcp = match std::env::var("MEMBERSHIP_TRANSPORT").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("channel") => false,
        Ok("tcp") => true,
        _ => panic!("MEMBERSHIP_TRANSPORT must be channel or tcp"),
    };
    // The gated matrix pays ~10 wall seconds of SETTLE per arm (a clustered hub
    // starts with takeover_reconcile_ticks = 8 at one tick per second), so it is
    // opt-in: it must never silently slow the QoS 0 run it is compared against.
    let gated = match std::env::var("MEMBERSHIP_GATED").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("0") => false,
        Ok("1") => true,
        _ => panic!("MEMBERSHIP_GATED must be 0 or 1"),
    };
    assert!(
        !(gated && tcp),
        "the gated matrix is channel-only: a TCP client's PUBACK is a subscriber-side \
         wire event, not the publisher-side ack gate this arm fences on"
    );
    let mut group = c.benchmark_group(if gated {
        "gated_membership_acks"
    } else if tcp {
        "qos0_tcp_membership_receipts"
    } else {
        "qos0_membership_receipts"
    });
    group.throughput(Throughput::Elements(BURST as u64));
    // Change only ordering across repeated processes; retain the seed in the run
    // manifest. No dependency/randomness inside measured bursts.
    let mut seed: u64 = std::env::var("MEMBERSHIP_SEED")
        .map_or(Ok(613), |s| s.parse())
        .expect("MEMBERSHIP_SEED must be an unsigned integer");
    let mut shapes = Vec::new();
    for peers in [0, 2, 4, 9] {
        for (matching, connected) in [(false, false), (true, false), (false, true), (true, true)] {
            shapes.push(Shape {
                peers,
                matching,
                connected,
            });
        }
    }
    for i in (1..shapes.len()).rev() {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let j = usize::try_from(seed % u64::try_from(i + 1).unwrap()).unwrap();
        shapes.swap(i, j);
    }
    let baseline = Shape {
        peers: 0,
        matching: false,
        connected: false,
    };
    // Opening/closing controls expose drift; bypass is only an overhead control,
    // not independent certification of generator/consumer headroom.
    // Bypass is a CHANNEL-only overhead control; the gated arms have no bypass
    // because there is no way to release a publisher ack without the hub.
    let controls: &[&str] = if tcp || gated {
        &["opening"]
    } else {
        &["opening", "direct-bypass"]
    };
    // Unrouted FIRST: it is the arm that keeps `matched == 0` on the hot path
    // under every shape of the settle gate, so it is the one a truncated run
    // still has. Routed second, as the shared-selection twin.
    let modes: &[(Mode, &str)] = if gated {
        &[
            (Mode::Gated { routed: false }, "-gated-unrouted"),
            (Mode::Gated { routed: true }, "-gated-routed"),
        ]
    } else if tcp {
        &[(Mode::Tcp, "")]
    } else {
        &[(Mode::Channel, "")]
    };
    for &label in controls {
        let mut rig = rt.block_on(Fixture::new(&drain, baseline, modes[0].0));
        group.bench_function(label, |b| {
            b.iter(|| rt.block_on(rig.burst(label == "direct-bypass")));
        });
        rt.block_on(rig.verify_peers());
    }
    for shape in shapes {
        for &(mode, suffix) in modes {
            let mut rig = rt.block_on(Fixture::new(&drain, shape, mode));
            group.bench_with_input(
                BenchmarkId::new(format!("{}{suffix}", shape.arm()), shape.peers),
                &shape,
                |b, _| {
                    b.iter(|| rt.block_on(rig.burst(false)));
                },
            );
            rt.block_on(rig.verify_peers());
        }
    }
    let mut rig = rt.block_on(Fixture::new(&drain, baseline, modes[0].0));
    group.bench_function("closing", |b| b.iter(|| rt.block_on(rig.burst(false))));
    rt.block_on(rig.verify_peers());
    group.finish();
}

criterion_group!(benches, bench);
fn main() {
    match std::env::var("MEMBERSHIP_PACED").as_deref() {
        Ok("1") => {
            let (rt, endpoints) = runtimes();
            paced::run(&rt, &endpoints);
        }
        Err(std::env::VarError::NotPresent) | Ok("0") => {
            benches();
            Criterion::default().configure_from_args().final_summary();
        }
        _ => panic!("MEMBERSHIP_PACED must be 0 or 1"),
    }
}
