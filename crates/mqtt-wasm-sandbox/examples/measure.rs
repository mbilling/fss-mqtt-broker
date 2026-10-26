//! The measurements of ADR 0086: what loading, instantiating, compiling and running
//! cost, what a warm instance holds, and how the limits behave. Build with `--release`.
//!
//! ```text
//! cargo run --release -p mqtt-wasm-sandbox --example measure -- jq.wasm payload-1k.json payload-100k.json
//! ```
//!
//! `MEASURE_EXECUTION=interpreted` measures the interpreter instead of machine code,
//! `MEASURE_FUEL=1` compiles with fuel accounting, `MEASURE_LOADS=n` loads the module
//! `n` times (5).

use std::time::{Duration, Instant};

use mqtt_wasm_sandbox::jq::{Jq, Tag};
use mqtt_wasm_sandbox::{Arg, Execution, Grants, Limits, Sandbox};

const FILTERS: [&str; 6] = [
    ".",
    ".a.b",
    ".readings | map(select(.temp > 50)) | length",
    ".readings | group_by(.id) | map({id: .[0].id, n: length})",
    ".readings | map(select(.id | test(\"^dev-[0-4]$\"))) | length",
    "[.readings[] | select(.ok) | .temp] | add / length",
];

fn median(mut times: Vec<Duration>) -> Duration {
    times.sort();
    times[times.len() / 2]
}

fn micros(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

/// The resident memory of this process in KiB, as the operating system counts it.
fn resident_kib() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        return status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
            .unwrap_or(0);
    }
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok()?.trim().parse().ok())
        .unwrap_or(0)
}

#[allow(clippy::too_many_lines)] // one measurement after another
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [wasm, payloads @ ..] = args.as_slice() else {
        eprintln!("usage: measure <jq.wasm> <payload.json>...");
        std::process::exit(2);
    };
    let wasm = std::fs::read(wasm).expect("the module file");
    let grants = Grants {
        clock: true,
        environment: Vec::new(),
    };
    let execution = match std::env::var("MEASURE_EXECUTION").as_deref() {
        Ok("interpreted") => Execution::Interpreted,
        _ => Execution::Compiled,
    };
    let bounds = Limits {
        max_fuel: std::env::var_os("MEASURE_FUEL").map(|_| u64::MAX / 2),
        ..Limits::default()
    };
    let count = std::env::var("MEASURE_LOADS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(5);

    // Loading: validation and compilation of the whole module, once per process. The
    // first load is the one a broker pays; the others give a median.
    let before = resident_kib();
    let at = Instant::now();
    let sandbox = Sandbox::load_as(&wasm, execution, bounds.clone(), grants.clone())
        .expect("the module loads");
    let first = at.elapsed();
    let loaded = resident_kib();
    let mut loads = vec![first];
    for _ in 1..count {
        let at = Instant::now();
        Sandbox::load_as(&wasm, execution, bounds.clone(), grants.clone())
            .expect("the module loads");
        loads.push(at.elapsed());
    }
    println!(
        "module\t{} bytes\t{execution:?}\tload_ms={:.1}\tfirst_load_ms={:.1}",
        wasm.len(),
        micros(median(loads)) / 1e3,
        micros(first) / 1e3,
    );
    let settled = resident_kib();
    // What warm instances hold, as the operating system sees it: each has run one
    // call on a 1 KB payload.
    let mut instances: Vec<Jq> = Vec::new();
    for _ in 0..16 {
        let mut jq = Jq::from_sandbox(sandbox.clone());
        jq.eval(b".a", Arg::Binary(br#"{"a":[1,2,3]}"#), None)
            .expect("it runs");
        instances.push(jq);
    }
    println!(
        "resident\tbefore_load_kib={before}\tafter_first_load_kib={loaded}\t\
         per_warm_instance_kib={}",
        (resident_kib().saturating_sub(settled)) / 16
    );
    drop(instances);
    println!(
        "describe\t{}",
        String::from_utf8_lossy(sandbox.description())
    );
    let instantiations: Vec<Duration> = (0..200)
        .map(|_| {
            let at = Instant::now();
            let instance = sandbox.instantiate().expect("an instance");
            let took = at.elapsed();
            drop(instance);
            took
        })
        .collect();
    let fresh = sandbox.instantiate().expect("an instance");
    println!(
        "instantiate\tmedian_us={:.0}\tmemory_bytes={}",
        micros(median(instantiations)),
        fresh.memory_bytes()
    );

    for path in payloads {
        let payload = std::fs::read(path).expect("the payload file");
        let iterations = if payload.len() > 10_000 { 40 } else { 2000 };
        for filter in FILTERS {
            let mut jq = Jq::from_sandbox(sandbox.clone());
            let first = jq
                .eval(filter.as_bytes(), Arg::Binary(&payload), None)
                .expect("it runs");
            let compile = jq.last.compile;
            let mut runs = Vec::with_capacity(iterations);
            let mut fuel = 0;
            for _ in 0..iterations {
                let before = jq.fuel_used();
                jq.eval(filter.as_bytes(), Arg::Binary(&payload), None)
                    .expect("it runs");
                runs.push(jq.last.run);
                fuel = jq.fuel_used() - before;
            }
            // The host's share: reading the outputs with the rule engine's reader.
            let decodes: Vec<Duration> = (0..iterations)
                .map(|_| {
                    let at = Instant::now();
                    std::hint::black_box(mqtt_rules::json_decode(&first).expect("exact JSON"));
                    at.elapsed()
                })
                .collect();
            let run = median(runs);
            println!(
                "wasm\t{filter}\t{}\tcompile_us={:.0}\trun_us={:.2}\tdecode_us={:.2}\t\
                 fuel={fuel}\tout_bytes={}\tmemory_bytes={}",
                payload.len(),
                micros(compile),
                micros(run),
                micros(median(decodes)),
                first.len(),
                jq.memory_bytes(),
            );
        }
        // The other way in: a value the rule already holds, written out by the engine.
        let value = mqtt_rules::json_decode(&payload).expect("the payload is JSON");
        let encodes: Vec<Duration> = (0..iterations)
            .map(|_| {
                let at = Instant::now();
                std::hint::black_box(value.to_json().expect("it encodes"));
                at.elapsed()
            })
            .collect();
        println!(
            "host\tto_json\t{}\tencode_us={:.2}",
            payload.len(),
            micros(median(encodes))
        );
    }

    cache(&sandbox);
    limits(&sandbox);
}

/// What a warm instance holds: the memory after 1, 8 and 32 compiled programs, and
/// the fuel one compilation burns (the same on every machine).
fn cache(sandbox: &Sandbox) {
    let mut jq = Jq::from_sandbox(sandbox.clone());
    for n in 0..32u32 {
        let program = format!(".readings | map(select(.temp > {n})) | length");
        let before = jq.fuel_used();
        jq.eval(program.as_bytes(), Arg::Binary(br#"{"readings":[]}"#), None)
            .expect("it runs");
        if [0, 7, 31].contains(&n) {
            println!(
                "cache\tprograms={}\tmemory_bytes={}\tfuel_last_compile_and_run={}",
                n + 1,
                jq.memory_bytes(),
                jq.fuel_used() - before
            );
        }
    }
}

/// The limits, as a rule would meet them.
fn limits(sandbox: &Sandbox) {
    let mut jq = Jq::from_sandbox(sandbox.clone());
    jq.eval(b". + 1", Arg::Binary(b"1"), None).expect("it runs");
    for (program, ms) in [
        ("def f: f; f", 100),
        ("repeat(.)", 100),
        ("[repeat(.)]", 50),
        ("last(range(1e12))", 150),
        ("def f: f; f", 10),
        ("def f: f; f", 1),
        ("\"a\" * 40 | test(\"(a*)*b\")", 100),
        // One regex match over eight megabytes: a single C call, cut off at the
        // deadline like every other program here.
        ("\"a\" * 8000000 | test(\"a+b\")", 30),
    ] {
        let made = jq.instances;
        let at = Instant::now();
        let outcome = jq.eval(
            program.as_bytes(),
            Arg::Binary(b"1"),
            Some(Duration::from_millis(ms)),
        );
        let took = at.elapsed();
        let at = Instant::now();
        let next = jq.eval(b". + 1", Arg::Binary(b"1"), None);
        println!(
            "timeout\t{program}\tset_ms={ms}\ttook_ms={:.2}\t{}\tnext_call={}\tnext_ms={:.2}\t\
             instance_replaced={}",
            micros(took) / 1e3,
            outcome.map_or_else(
                |e| e.to_string(),
                |o| String::from_utf8_lossy(&o).into_owned()
            ),
            next.map_or_else(
                |e| e.to_string(),
                |o| String::from_utf8_lossy(&o).into_owned()
            ),
            micros(at.elapsed()) / 1e3,
            jq.instances > made,
        );
    }
    for program in [
        "[range(1e9)]",
        "[range(300000)]",
        "range(1e7)",
        "\"x\" * 1e9",
        "[limit(1e6; repeat(\"x\" * 1000))] | length",
    ] {
        let at = Instant::now();
        let outcome = jq.eval(
            program.as_bytes(),
            Arg::Binary(b"1"),
            Some(Duration::from_mins(1)),
        );
        let took = at.elapsed();
        let stopped = matches!(&outcome, Err(e) if e.tag == Tag::Limit);
        let next = jq.eval(b". + 1", Arg::Binary(b"1"), None);
        println!(
            "limit\t{program}\ttook_ms={:.1}\tstopped_by_limit={stopped}\t{}\tnext_call={}",
            micros(took) / 1e3,
            outcome.map_or_else(
                |e| e.to_string(),
                |o| format!("{} bytes of output", o.len())
            ),
            next.map_or_else(
                |e| e.to_string(),
                |o| String::from_utf8_lossy(&o).into_owned()
            ),
        );
    }
    println!("instances\t{}", jq.instances);
}
