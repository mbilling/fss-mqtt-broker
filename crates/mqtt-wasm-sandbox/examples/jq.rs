//! Runs one jq program on one input through the sandbox, as the rule function would.
//!
//! ```text
//! cargo run --release -p mqtt-wasm-sandbox --example jq -- jq.wasm '.a' '{"a":1}' [timeout-ms]
//! ```
//!
//! The input is passed as a binary (jq parses it); prefix it with `json:` to pass it
//! as an already encoded value.

use std::time::Duration;

use mqtt_wasm_sandbox::jq::{Jq, DEFAULT_TIMEOUT};
use mqtt_wasm_sandbox::{Arg, Grants};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [wasm, program, input, rest @ ..] = args.as_slice() else {
        eprintln!("usage: jq <jq.wasm> <program> <input> [timeout-ms]");
        std::process::exit(2);
    };
    let timeout = rest
        .first()
        .and_then(|ms| ms.parse().ok())
        .map_or(DEFAULT_TIMEOUT, Duration::from_millis);
    let wasm = std::fs::read(wasm).expect("the module file");
    let grants = Grants {
        clock: true,
        environment: Vec::new(),
    };
    let mut jq = Jq::new(&wasm, grants).expect("the module loads");
    let input = match input.strip_prefix("json:") {
        Some(json) => Arg::Json(json),
        None => Arg::Binary(input.as_bytes()),
    };
    match jq.eval(program.as_bytes(), input, Some(timeout)) {
        Ok(outputs) => println!("{}", String::from_utf8_lossy(&outputs)),
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
    eprintln!(
        "{:?}, {} bytes of module memory",
        jq.last,
        jq.memory_bytes()
    );
}
