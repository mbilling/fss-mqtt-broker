//! Fixed useful work and aligned CPU/wall windows, separate from Criterion's
//! adaptive burst ladder. Linux /proc accounting requires no privileged profiler.
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::json;
use tokio::runtime::Runtime;

use super::rig::{Shape, BURST};
use super::tcp::TcpRig;

#[cfg(target_os = "linux")]
#[path = "perf_control.rs"]
pub mod perf_control;

#[derive(Clone, Copy)]
struct Cpu {
    hot: u64,
    process: u64,
}

fn ticks(path: &str) -> u64 {
    let stat = std::fs::read_to_string(path).expect("paced CPU accounting requires Linux /proc");
    parse_ticks(&stat)
}

pub fn parse_ticks(stat: &str) -> u64 {
    // comm may itself contain spaces/parentheses; fields following its LAST ')'
    // start at state (field 3). utime/stime are fields 14/15, not child CPU.
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .expect("proc stat comm")
        .1
        .split_whitespace()
        .collect();
    fields[11]
        .parse::<u64>()
        .unwrap()
        .checked_add(fields[12].parse().unwrap())
        .unwrap()
}

impl Cpu {
    fn now() -> Self {
        Self {
            hot: ticks("/proc/thread-self/stat"),
            process: ticks("/proc/self/stat"),
        }
    }
}

fn number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .map_or(Ok(default), |v| v.parse())
        .expect("invalid unsigned measurement setting")
}

pub fn run(rt: &Runtime, endpoints: &Runtime) {
    let rate = number("MEMBERSHIP_RATE", 100_000);
    let seconds = number("MEMBERSHIP_SECONDS", 60);
    let warmup = number("MEMBERSHIP_WARMUP", 10);
    assert!(warmup > 0 && seconds > 0);
    let hz = String::from_utf8(
        std::process::Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .expect("getconf CLK_TCK")
            .stdout,
    )
    .unwrap()
    .trim()
    .parse::<u32>()
    .unwrap();
    assert!(hz > 0);
    let peers = usize::try_from(number("MEMBERSHIP_PEERS", 9)).unwrap();
    let mut shapes = vec![
        (
            "C-miss",
            Shape {
                peers,
                matching: false,
                connected: true,
            },
        ),
        (
            "D-hit",
            Shape {
                peers,
                matching: true,
                connected: true,
            },
        ),
    ];
    if number("MEMBERSHIP_SEED", 613).is_multiple_of(2) {
        shapes.reverse();
    }
    let baseline = Shape {
        peers: 0,
        matching: false,
        connected: false,
    };
    shapes.insert(0, ("opening", baseline));
    shapes.push(("closing", baseline));
    let selected = std::env::var("MEMBERSHIP_CASE").ok();
    if let Some(selected) = &selected {
        assert!(
            shapes.iter().any(|(name, _)| name == selected),
            "unknown MEMBERSHIP_CASE"
        );
        shapes.retain(|(name, _)| name == selected);
    }
    #[cfg(target_os = "linux")]
    let mut perf = perf_control::PerfControl::from_env();
    #[cfg(target_os = "linux")]
    assert!(
        perf.is_none() || selected.is_some(),
        "perf totals require one explicit MEMBERSHIP_CASE"
    );
    for (name, shape) in shapes {
        rt.block_on(async {
            let mut rig = TcpRig::new(endpoints, shape).await;
            // Warm-up is paced too; it is excluded from every reported window.
            let _ = window(&mut rig, rate, warmup).await;
            println!("{}", json!({"event":"window_prepare", "arm":name,
                "unix_ns": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().to_string(),
                "rate":rate, "seconds":seconds, "peers":shape.peers, "clock_ticks_per_second":hz}));
            #[cfg(target_os = "linux")]
            if let Some(perf) = &mut perf { perf.command(b"enable\n"); }
            let result = window(&mut rig, rate, seconds).await;
            #[cfg(target_os = "linux")]
            if let Some(perf) = &mut perf { perf.command(b"disable\n"); }
            rig.verify_peers().await;
            println!("{}", json!({"event":"window_end", "arm":name,
                "unix_ns": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().to_string(),
                "measurement_start_unix_ns":result.start_unix_ns.to_string(),
                "measurement_end_unix_ns":result.end_unix_ns.to_string(),
                "data_receipts":result.receipts, "elapsed_seconds":result.elapsed,
                "hot_thread_cpu_ticks": result.hot_ticks,
                "process_cpu_ticks": result.process_ticks,
                "burst_ns":result.bursts, "start_lateness_ns":result.lateness,
                "scope":"TCP client sockets, synthetic peer channels; not cluster capacity"}));
        });
    }
}

struct Window {
    start_unix_ns: u128,
    end_unix_ns: u128,
    receipts: u64,
    elapsed: f64,
    hot_ticks: u64,
    process_ticks: u64,
    bursts: Vec<u64>,
    lateness: Vec<u64>,
}

async fn window(rig: &mut TcpRig, rate: u64, seconds: u64) -> Window {
    let burst = u64::try_from(BURST).unwrap();
    let receipts = rate.checked_mul(seconds).unwrap();
    assert!(
        rate > 0 && receipts > 0 && receipts.is_multiple_of(burst),
        "whole scheduled bursts required"
    );
    let period = Duration::from_nanos(burst.checked_mul(1_000_000_000).unwrap() / rate);
    assert!(!period.is_zero());
    let count = usize::try_from(receipts / burst).unwrap();
    let mut bursts = Vec::with_capacity(count);
    let mut lateness = Vec::with_capacity(count);
    let cpu = Cpu::now();
    let started = Instant::now();
    let start_unix_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut next = started;
    for _ in 0..count {
        tokio::time::sleep_until(next.into()).await;
        let actual = Instant::now();
        lateness.push(u64::try_from(actual.saturating_duration_since(next).as_nanos()).unwrap());
        rig.burst().await;
        bursts.push(u64::try_from(actual.elapsed().as_nanos()).unwrap());
        next += period;
    }
    // Early completion cannot inflate the achieved rate. If receipts run past
    // the scheduled end, report actual elapsed time and under-offer; never skip
    // slow bursts or silently lower the requested rate.
    tokio::time::sleep_until((started + Duration::from_secs(seconds)).into()).await;
    let elapsed = started.elapsed().as_secs_f64();
    let end = Cpu::now();
    let end_unix_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Window {
        start_unix_ns,
        end_unix_ns,
        receipts,
        elapsed,
        hot_ticks: end.hot.checked_sub(cpu.hot).unwrap(),
        process_ticks: end.process.checked_sub(cpu.process).unwrap(),
        bursts,
        lateness,
    }
}
