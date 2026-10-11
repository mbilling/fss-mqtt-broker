//! Says what this host lets the sandbox do (ADR 0086): whether a module can run as
//! machine code, which needs memory the process may map executable, and whether the
//! interpreter runs. With an expectation it is a check, for a hardened environment:
//!
//! ```text
//! cargo run --release -p mqtt-wasm-sandbox --example probe
//! systemd-run --wait --pipe -p MemoryDenyWriteExecute=yes \
//!     target/release/examples/probe compiled=refused interpreted=runs
//! ```
//!
//! Exits 0 when every expectation holds, 1 when one does not.

use mqtt_wasm_sandbox::Execution;

fn main() {
    let expected: Vec<String> = std::env::args().skip(1).collect();
    let mut failed = false;
    for (name, execution) in [
        ("compiled", Execution::Compiled),
        ("interpreted", Execution::Interpreted),
    ] {
        let outcome = execution.probe();
        let word = if outcome.is_ok() { "runs" } else { "refused" };
        match &outcome {
            Ok(()) => println!("{name}: runs"),
            Err(e) => println!("{name}: refused — {e}"),
        }
        for expectation in &expected {
            if let Some(want) = expectation
                .strip_prefix(name)
                .and_then(|rest| rest.strip_prefix('='))
            {
                if want != word {
                    eprintln!("probe: expected {name}={want}, found {word}");
                    failed = true;
                }
            }
        }
    }
    if failed {
        std::process::exit(1);
    }
}
