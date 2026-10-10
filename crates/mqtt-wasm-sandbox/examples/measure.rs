//! The spike's measurements (ADR 0086): what loading, instantiating, compiling and
//! running cost, and how the limits behave. Build with `--release`.
//!
//! ```text
//! cargo run --release -p mqtt-wasm-sandbox --example measure -- jq.wasm payload-1k.json payload-100k.json
//! ```

use std::time::{Duration, Instant};

use mqtt_wasm_sandbox::jq::{Jq, Tag};
use mqtt_wasm_sandbox::{Arg, Grants, Limits, Sandbox};

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

    // Loading: validation and translation of the whole module, once per process.
    let loads: Vec<Duration> = (0..5)
        .map(|_| {
            let at = Instant::now();
            Sandbox::load(&wasm, Limits::default(), grants.clone()).expect("the module loads");
            at.elapsed()
        })
        .collect();
    println!(
        "module\t{} bytes\tload_ms={:.1}",
        wasm.len(),
        micros(median(loads)) / 1e3
    );

    let sandbox = Sandbox::load(&wasm, Limits::default(), grants).expect("the module loads");
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
        ("[repeat(.)]", 200),
        ("last(range(1e12))", 150),
        ("def f: f; f", 10),
        ("def f: f; f", 1),
        ("\"a\" * 40 | test(\"(a*)*b\")", 100),
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
            Some(Duration::from_secs(60)),
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
