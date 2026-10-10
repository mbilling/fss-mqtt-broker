//! Runs every case of `tests/emqx-jq-oracle.txt` through the jq module and prints
//! each one whose answer differs from EMQX's (in full with `ORACLE_FULL=1`).
//!
//! ```text
//! cargo run --release -p mqtt-wasm-sandbox --example oracle -- jq.wasm tests/emqx-jq-oracle.txt
//! ```

use mqtt_wasm_sandbox::jq::{Jq, DEFAULT_TIMEOUT};
use mqtt_wasm_sandbox::Grants;

#[path = "../tests/oracle/mod.rs"]
mod oracle;
use oracle::Verdict;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [wasm, cases] = args.as_slice() else {
        eprintln!("usage: oracle <jq.wasm> <emqx-jq-oracle.txt>");
        std::process::exit(2);
    };
    let wasm = std::fs::read(wasm).expect("the module file");
    let text = std::fs::read_to_string(cases).expect("the oracle file");
    let grants = Grants {
        clock: true,
        environment: oracle::environment(),
    };
    let mut jq = Jq::new(&wasm, grants).expect("the module loads");
    let (mut same, mut differ, mut survived) = (0, 0, 0);
    for case in oracle::parse(&text) {
        let got = jq.eval(&case.program, case.input_arg(), Some(DEFAULT_TIMEOUT));
        match oracle::compare(&case, &got) {
            Verdict::Same => same += 1,
            Verdict::EmqxWentDown(ours) => {
                survived += 1;
                println!("EMQX NODE DOWN  {}\n  here: {ours}", case.describe());
            }
            Verdict::Differs { emqx, ours } => {
                differ += 1;
                println!(
                    "DIFFERS  {}\n  emqx: {emqx}\n  here: {ours}",
                    case.describe()
                );
            }
        }
    }
    println!(
        "{same} the same, {differ} differ, {survived} took the EMQX node down; \
         {} instances used",
        jq.instances
    );
}
