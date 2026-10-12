//! The real jq, as a WebAssembly module, behind the sandbox (ADR 0086 spike).
//!
//! These tests need `jq.wasm`, which is built by `jq-module/build.sh` and not checked
//! in, so the whole file is behind the `jq-module` feature:
//!
//! ```text
//! crates/mqtt-wasm-sandbox/jq-module/build.sh
//! cargo test --release -p mqtt-wasm-sandbox --features jq-module
//! ```
//!
//! `--release`: the host's own share (and the whole of the interpreted variant) is an
//! order of magnitude slower without optimisation, and two of the oracle's cases run
//! into EMQX's ten-second timeout on purpose.
//! `MQTTD_JQ_WASM` names another module file than `jq-module/jq.wasm`.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use mqtt_rules::{json_decode, Value};
use mqtt_wasm_sandbox::jq::{Jq, JqError, Pool, Tag, COMPILE_TIMEOUT, DEFAULT_TIMEOUT};
use mqtt_wasm_sandbox::{Arg, CallError, Execution, Grants, Limits, Sandbox};

mod oracle;
use oracle::{Expected, Verdict};

fn module_path() -> PathBuf {
    std::env::var_os("MQTTD_JQ_WASM").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("jq-module/jq.wasm"),
        PathBuf::from,
    )
}

fn module() -> Vec<u8> {
    let path = module_path();
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "{}: {e} — build it with crates/mqtt-wasm-sandbox/jq-module/build.sh",
            path.display()
        )
    })
}

fn jq_with(grants: Grants) -> Jq {
    Jq::new(&module(), grants).expect("the jq module loads")
}

fn jq() -> Jq {
    jq_with(Grants {
        clock: true,
        environment: oracle::environment(),
    })
}

fn text(result: Result<Vec<u8>, JqError>) -> String {
    match result {
        Ok(outputs) => String::from_utf8(outputs).expect("jq prints UTF-8"),
        Err(e) => panic!("{e}"),
    }
}

/// The module file is the build the source tree names: same pinned sources, same
/// compiler, same flags, byte for byte.
#[test]
fn the_module_is_the_recorded_build() {
    use sha2::Digest as _;
    use std::fmt::Write as _;
    let recorded = include_str!("../jq-module/jq.wasm.sha256");
    let recorded = recorded.split_whitespace().next().expect("a hash");
    let digest = sha2::Sha256::digest(module());
    let found = digest.iter().fold(String::new(), |mut hex, byte| {
        write!(hex, "{byte:02x}").expect("writing to a string");
        hex
    });
    assert_eq!(
        found, recorded,
        "jq.wasm is not what jq-module/build.sh builds at this revision"
    );
}

/// Every case EMQX 6.3.1 answered, byte for byte — the outputs as jq printed them,
/// the error tags, the error texts. The cases that differ are named here with the
/// reason; a case that starts or stops differing fails the test.
#[test]
fn the_module_answers_as_emqx_does_except_where_recorded() {
    // (a word of the program, why it differs)
    const KNOWN: [(&str, &str); 6] = [
        (
            "[acosh, gamma, lgamma, tgamma",
            "libm: tgamma(5.5) differs in the last digit (musl / glibc)",
        ),
        (
            "| sin, cos, exp, log, tan, sqrt]",
            "libm: tan(10.1) differs in the last digit",
        ),
        (
            "| pow(.; 1.5), pow(.; -0.3), exp10",
            "libm: exp10, log10 and cbrt differ in the last digit",
        ),
        (
            "| gamma, tgamma, erf, j0, y0",
            "libm: tgamma, lgamma, j0 and y0 differ in the last digit",
        ),
        ("%T %z", "strptime: wasi-libc has no %z"),
        (
            "strftime(\"%Z %z\")",
            "strftime %Z of a broken-down UTC time: glibc says GMT, musl UTC",
        ),
    ];
    let cases = oracle::parse(include_str!("emqx-jq-oracle.txt"));
    assert!(
        cases.len() >= 280,
        "the oracle file lost cases: {}",
        cases.len()
    );
    let mut jq = jq();
    let (mut same, mut differing, mut emqx_down) = (0, Vec::new(), Vec::new());
    for case in &cases {
        let got = jq.eval(&case.program, case.input_arg(), Some(DEFAULT_TIMEOUT));
        match oracle::compare(case, &got) {
            Verdict::Same => same += 1,
            Verdict::Differs { emqx, ours } => {
                let program = String::from_utf8_lossy(&case.program).into_owned();
                assert!(
                    KNOWN.iter().any(|(word, _)| program.contains(word)),
                    "{} differs from EMQX and is not recorded\n  emqx: {emqx}\n  here: {ours}",
                    case.describe()
                );
                differing.push(program);
            }
            Verdict::EmqxWentDown(ours) => emqx_down.push((case.describe(), ours)),
        }
    }
    assert_eq!(
        differing.len(),
        KNOWN.len(),
        "recorded differences no longer differ: {differing:?}"
    );
    assert_eq!(same + differing.len() + emqx_down.len(), cases.len());
    // The one program that takes an EMQX node down (the NIF overflows the C stack
    // freeing a 20000-deep array) has an answer here.
    assert_eq!(emqx_down.len(), 1, "{emqx_down:?}");
    assert_eq!(emqx_down[0].1, "[1]");
}

/// `emqx_rule_funcs:jq/2` on rule values: a binary is handed to jq as text, any other
/// value is JSON-encoded first, and every output is decoded — here with the rule
/// engine's own writer and reader, so integers of any size stay exact.
fn rule_jq(jq: &mut Jq, program: &str, input: &Value) -> Result<Value, String> {
    let outputs = if input.is_binary() {
        jq.eval(
            program.as_bytes(),
            Arg::Binary(input.as_bytes().expect("a binary")),
            None,
        )
    } else {
        let encoded = input.to_json().map_err(|e| e.to_string())?;
        jq.eval(program.as_bytes(), Arg::Json(&encoded), None)
    }
    .map_err(|e| e.to_string())?;
    json_decode(&outputs).map_err(|e| {
        format!(
            "the rule fails decoding {:?}: {e}",
            String::from_utf8_lossy(&outputs)
        )
    })
}

fn value(json: &str) -> Value {
    json_decode(json.as_bytes()).expect("JSON")
}

fn binary(text: &str) -> Value {
    Value::Str(text.into())
}

/// The pinned transcript of `emqx_rule_funcs:jq/2` on EMQX 6.3.1 (probe p3), case by
/// case: the left side is what the rule engine's values give through the module, the
/// right side what EMQX returned, written as the JSON its terms encode to.
#[test]
#[allow(clippy::too_many_lines)] // one line per pinned case
fn values_cross_as_a_rule_passes_them() {
    let mut jq = jq();
    let mut same = |program: &str, input: Value, emqx: &str| {
        let got = rule_jq(&mut jq, program, &input).unwrap_or_else(|e| panic!("{program}: {e}"));
        assert_eq!(
            format!("{got:?}"),
            format!("{:?}", value(emqx)),
            "jq({program:?}, {input:?})"
        );
    };
    // A binary is JSON text for jq; a map, a number, a list are encoded first.
    same(".", binary(r#"{"a":1}"#), r#"[{"a":1}]"#);
    same(".", value(r#"{"a":1}"#), r#"[{"a":1}]"#);
    same(".a", value(r#"{"a":1}"#), "[1]");
    same(".", Value::Int(5), "[5]");
    same(".", Value::Float(1.5), "[1.5]");
    same(".", binary(r#""abc""#), r#"["abc"]"#);
    same(".", value("[1,2]"), "[[1,2]]");
    same(".", Value::Bool(true), "[true]");
    same(".", Value::Null, "[null]");
    same(
        ".[]",
        binary(r#"[1,"a",null,{"k":[true]}]"#),
        r#"[1,"a",null,{"k":[true]}]"#,
    );
    same("empty", binary("1"), "[]");
    same("map(select(.>1))", binary("[1,2,3]"), "[[2,3]]");
    // Integers: a literal of any size comes back exact; arithmetic goes through a double.
    same(
        ".",
        value("123456789012345678901234567890"),
        "[123456789012345678901234567890]",
    );
    same(
        ".",
        binary("123456789012345678901234567890"),
        "[123456789012345678901234567890]",
    );
    same(
        ".+1",
        binary("123456789012345678901234567890"),
        "[123456789012345680000000000000]",
    );
    same(".+1", binary("9007199254740993"), "[9007199254740992]");
    same(".", binary("9007199254740993"), "[9007199254740993]");
    same(".[0]", binary("[9007199254740993]"), "[9007199254740993]");
    // Floats stay floats, integers integers.
    same(".", binary("1.0"), "[1.0]");
    same(".", binary("1e2"), "[100.0]");
    same(".", binary("100000000000000000000.0"), "[1.0e20]");
    same(".", binary("0.1"), "[0.1]");
    same(".", binary("-0"), "[0]");
    same(".", binary("-0.0"), "[-0.0]");
    same(".*1", binary("-0.0"), "[0]");
    same(
        "3.0, 3, 1e2, 1.0e2, 0.1+0.2, 1/3, 1e17, 1e17+0, 12345678901234567890+0, 1e-5, \
         1.5e300*1.5e300, -(1.5e300*1.5e300), nan, infinite, -infinite, [nan]",
        binary("null"),
        "[3.0,3,100.0,100.0,0.30000000000000004,0.3333333333333333,1.0e17,1.0e17,\
         12345678901234567000,1.0e-5,1.7976931348623157e308,-1.7976931348623157e308,\
         null,1.7976931348623157e308,-1.7976931348623157e308,[null]]",
    );
    // Text.
    same(".", binary(r#""é😀""#), "[\"\u{e9}\u{1f600}\"]");
    same(
        ".|length, utf8bytelength, explode",
        binary(r#""hé😀""#),
        "[3,7,[104,233,128512]]",
    );
    same(".", binary(r#""a\u0000b""#), r#"["a\u0000b"]"#);
    same(r#""\u0000""#, binary("1"), r#"["\u0000"]"#);
    // A repeated key: the last one wins, in jq as in EMQX's own decoder.
    same(".", binary(r#"{"b":1,"a":2,"b":3}"#), r#"[{"b":3,"a":2}]"#);
    same(
        "keys, to_entries",
        binary(r#"{"b":1,"a":2}"#),
        r#"[["a","b"],[{"key":"b","value":1},{"key":"a","value":2}]]"#,
    );
    same(".", binary("nan"), "[null]");
    // Program and input both end at the first NUL, as the C strings they are in the NIF.
    same(".", binary("1\u{0}2"), "[1]");
    same(".\u{0} garbage", binary("1"), "[1]");
    // A binary that is not UTF-8, inside a map: jq reads the bytes as U+FFFD.
    same(
        ".",
        Value::Bin(vec![b'"', 0xff, b'"'].into()),
        "[\"\u{fffd}\"]",
    );

    // And the ways it fails, tag and text.
    let mut fails = |program: &str, input: Value, emqx: &str| {
        let error = rule_jq(&mut jq, program, &input).expect_err(program);
        assert_eq!(error, emqx, "jq({program:?}, {input:?})");
    };
    fails(
        ".",
        binary("abc"),
        "jq_err_parse: Invalid numeric literal at EOF at line 1, column 3 (while parsing 'abc')",
    );
    fails(
        ".",
        binary(""),
        "jq_err_parse: Expected JSON value (while parsing '')",
    );
    fails(
        ".",
        binary("1 2 3"),
        "jq_err_parse: Unexpected extra JSON values (while parsing '1 2 3')",
    );
    fails(
        ".a",
        binary("[1]"),
        "jq_err_process: jq error: Cannot index array with string \"a\"",
    );
    fails(
        "error(\"boom\")",
        binary("1"),
        "jq_err_process: jq error: boom",
    );
    // Outputs before an error are gone with it.
    fails(
        "1, error(\"boom\")",
        binary("1"),
        "jq_err_process: jq error: boom",
    );
    fails(
        "",
        binary("1"),
        "jq_err_compile: jq: error: Top-level program not given (try \".\")jq: 1 compile error",
    );
    fails(
        "%%%",
        binary("1"),
        "jq_err_compile: jq: error: syntax error, unexpected '%', expecting end of file at \
         <top-level>, line 1, column 1:\n    %%%\n    ^jq: 1 compile error",
    );
    // jq prints 1e1000 as `1E+1000`; EMQX's decoder refuses it (`{range, <<"1E+1000">>}`)
    // and the rule fails. The rule engine's reader refuses it too.
    let error = rule_jq(&mut jq, ".", &binary("1e1000")).expect_err("out of a float's range");
    assert!(error.contains("1E+1000"), "{error}");
}

/// The timeout of `jq/3`: a program that never ends is stopped within a millisecond or
/// two of it, with EMQX's error, and the caller's next call is served as if nothing
/// had happened.
#[test]
fn a_runaway_program_stops_at_the_timeout_and_the_next_call_is_unaffected() {
    // The grace a program has to end by itself once asked is two milliseconds by
    // default, which a thread that loses its processor on a busy machine can sleep
    // through. Here it is long, so that what is counted below (which programs cost
    // their instance) does not depend on the machine's load.
    let patient = Limits {
        cancel_grace: Duration::from_secs(30),
        ..Limits::default()
    };
    let grants = Grants {
        clock: true,
        environment: oracle::environment(),
    };
    let mut jq =
        Jq::from_sandbox(Sandbox::load(&module(), patient, grants).expect("the module loads"));
    assert_eq!(text(jq.eval(b". + 1", Arg::Binary(b"1"), None)), "[2]");
    for (program, ms) in [
        ("def f: f; f", 100),
        ("repeat(.)", 100),
        ("[repeat(.)]", 50),
        ("last(range(1e12))", 150),
        ("def f: f; f", 5),
    ] {
        let timeout = Duration::from_millis(ms);
        let started = Instant::now();
        let error = jq
            .eval(program.as_bytes(), Arg::Binary(b"1"), Some(timeout))
            .expect_err(program);
        let took = started.elapsed();
        assert_eq!(error.tag, Tag::Timeout, "{program}: {error}");
        assert_eq!(
            String::from_utf8_lossy(&error.message),
            format!(
                "jq program canceled as it took too long time to execute (timeout set to {ms} ms)"
            )
        );
        // The clock starts with the call, as EMQX's timer does.
        assert!(took >= timeout, "{program}: stopped early, after {took:?}");
        assert!(
            took < timeout + Duration::from_millis(250),
            "{program}: stopped late, after {took:?}"
        );
        assert_eq!(
            text(jq.eval(b". + 1", Arg::Binary(b"1"), None)),
            "[2]",
            "after {program}"
        );
    }
    // A program that reaches its next step ends by itself and keeps the instance; only
    // `[repeat(.)]`, which had grown the memory past the recycling mark, cost one.
    assert!(jq.instances <= 2, "{} instances", jq.instances);
    // `jq/3` with a timeout of 0 still runs what is quick, as in EMQX.
    assert_eq!(
        text(jq.eval(b".", Arg::Binary(b"1"), Some(Duration::ZERO))),
        "[1]"
    );
}

/// The two ways a call ends at its deadline. A program between two of its steps sees
/// the cancel flag and ends by itself: the NIF's timeout status, the instance whole.
/// A program inside one long C call — here one regex match over eight megabytes,
/// which EMQX cannot stop at all — never looks at the flag: its fuel is cut off and
/// the instance is gone.
#[test]
fn a_program_that_cannot_be_asked_to_stop_is_stopped_anyway() {
    let load = |limits| Sandbox::load(&module(), limits, Grants::default()).expect("it loads");
    let hasty = load(Limits::default());
    // For the program that does end by itself, a grace no busy machine outlasts.
    let patient = load(Limits {
        cancel_grace: Duration::from_secs(30),
        ..Limits::default()
    });
    let run = |sandbox: &Sandbox, program: &[u8]| {
        let mut instance = sandbox.instantiate().expect("an instance");
        instance
            .call("jq_compile", &[Arg::Binary(program)], None)
            .expect("it compiles");
        let deadline = Instant::now() + Duration::from_millis(30);
        let outcome = instance.call(
            "jq",
            &[Arg::Binary(program), Arg::Binary(b"1")],
            Some(deadline),
        );
        (outcome, instance.is_poisoned())
    };
    let (outcome, poisoned) = run(&patient, b"def f: f; f");
    assert!(
        matches!(outcome, Err(CallError::Module { status: 7, .. })),
        "{outcome:?}"
    );
    assert!(!poisoned);
    let stuck = br#""a" * 8000000 | test("a+b")"#;
    let (outcome, poisoned) = run(&hasty, stuck);
    assert_eq!(outcome, Err(CallError::Timeout));
    assert!(poisoned);
    // Through the function it is EMQX's timeout either way, and the next call is served.
    let mut jq = jq();
    let started = Instant::now();
    let error = jq
        .eval(stuck, Arg::Binary(b"1"), Some(Duration::from_millis(30)))
        .expect_err("stuck");
    assert_eq!(error.tag, Tag::Timeout);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(text(jq.eval(b". + 1", Arg::Binary(b"1"), None)), "[2]");
    assert_eq!(jq.instances, 2);
}

/// Memory and output are bounded, whatever the payload or the program asks for.
#[test]
fn memory_and_output_are_bounded() {
    let mut jq = jq();
    for program in [
        "[range(1e9)]",
        "\"x\" * 1e9",
        "\"a\" * 8000000 | explode | length",
    ] {
        let error = jq
            .eval(program.as_bytes(), Arg::Binary(b"1"), Some(DEFAULT_TIMEOUT))
            .expect_err(program);
        assert_eq!(error.tag, Tag::Limit, "{program}: {error}");
        assert!(
            error.to_string().contains("memory limit"),
            "{program}: {error}"
        );
        assert_eq!(
            text(jq.eval(b". + 1", Arg::Binary(b"1"), None)),
            "[2]",
            "after {program}"
        );
    }
    // More than a mebibyte of result, as one output and as many.
    for program in ["[range(300000)]", "range(1e7)", "\"x\" * 2e6"] {
        let error = jq
            .eval(program.as_bytes(), Arg::Binary(b"1"), Some(DEFAULT_TIMEOUT))
            .expect_err(program);
        assert_eq!(error.tag, Tag::Limit, "{program}: {error}");
        assert!(
            error.to_string().contains("larger than the limit"),
            "{program}: {error}"
        );
    }
    // Just under it passes.
    let outputs = text(jq.eval(b"\"x\" * 1000000", Arg::Binary(b"1"), None));
    assert_eq!(outputs.len(), 1_000_004);
    // A structure deeper than jq's C code can walk on its stack ends the call, not more.
    let error = jq
        .eval(
            b"reduce range(300000) as $i (null; [.]) | 1",
            Arg::Binary(b"1"),
            Some(DEFAULT_TIMEOUT),
        )
        .expect_err("too deep");
    assert_eq!(error.tag, Tag::System, "{error}");
    assert_eq!(text(jq.eval(b". + 1", Arg::Binary(b"1"), None)), "[2]");
}

/// A warm instance keeps compiled programs and nothing else: a sequence of calls
/// gives, call by call, what each gives on an instance of its own.
#[test]
fn one_call_leaves_nothing_for_the_next() {
    let calls: [(&str, &str); 14] = [
        ("first(range(10))", "1"),
        ("first(range(10))", "2"),
        ("1, halt, 2", "3"),
        ("1, halt, 2", "4"),
        (". as $x | [$x, $__loc__.line]", "5"),
        ("limit(1; .[])", "[1,2,3]"),
        ("limit(1; .[])", "[4,5,6]"),
        ("error(.)", "\"a\""),
        ("error(.)", "\"b\""),
        ("reduce .[] as $x (0; . + $x)", "[1,2,3]"),
        ("reduce .[] as $x (0; . + $x)", "[10]"),
        ("input", "1"),
        ("[.[] | tostring] | join(\"-\")", "[1,\"a\",null]"),
        ("first(range(10))", "3"),
    ];
    let show = |r: Result<Vec<u8>, JqError>| match r {
        Ok(o) => String::from_utf8(o).expect("UTF-8"),
        Err(e) => e.to_string(),
    };
    let mut warm = jq();
    let reused: Vec<String> = calls
        .iter()
        .map(|(program, input)| {
            show(warm.eval(program.as_bytes(), Arg::Binary(input.as_bytes()), None))
        })
        .collect();
    assert_eq!(warm.instances, 1);
    // 14 runs, 8 distinct programs: each compiled once.
    let stats: u64 = warm.cache_stats().expect("the module's counters");
    assert_eq!(
        (stats >> 32, stats & 0xffff_ffff),
        (14, 8),
        "(runs, compilations)"
    );
    let mut alone = jq();
    let fresh: Vec<String> = calls
        .iter()
        .map(|(program, input)| {
            alone.reset();
            show(alone.eval(program.as_bytes(), Arg::Binary(input.as_bytes()), None))
        })
        .collect();
    assert_eq!(alone.instances, 14);
    assert_eq!(reused, fresh);
    assert_eq!(reused[0], "[0]");
    assert_eq!(reused[3], "[1]");
    assert_eq!(reused[8], "jq_err_process: jq error: b");
}

/// More programs than the module's cache holds: the oldest is compiled again, and
/// every answer is still right.
#[test]
fn the_program_cache_is_bounded() {
    let mut jq = jq();
    for round in 0..2 {
        for n in 0..40 {
            let program = format!(". + {n}");
            assert_eq!(
                text(jq.eval(program.as_bytes(), Arg::Binary(b"1"), None)),
                format!("[{}]", n + 1),
                "round {round}"
            );
        }
    }
    let stats = jq.cache_stats().expect("the module's counters");
    // 32 slots, 40 programs, twice round: nothing of the first round is still there
    // when its turn comes again.
    assert_eq!(stats & 0xffff_ffff, 80, "compilations");
    assert_eq!(jq.instances, 1);
}

/// The module sees the clock and the environment only as granted: by default `now` is
/// zero and `$ENV` is empty, whatever the broker's own environment holds.
#[test]
fn the_module_sees_only_what_it_is_granted() {
    let mut closed = jq_with(Grants::default());
    assert_eq!(
        text(closed.eval(b"now, $ENV, env, (now | todate)", Arg::Binary(b"1"), None)),
        "[0,{},{},\"1970-01-01T00:00:00Z\"]"
    );
    let grants = Grants {
        clock: true,
        environment: vec![b"SITE=plant-3".to_vec(), b"EMPTY=".to_vec()],
    };
    let mut open = jq_with(grants);
    assert_eq!(
        text(open.eval(
            b"(now > 1700000000), $ENV, env.SITE, $ENV.PATH",
            Arg::Binary(b"1"),
            None
        )),
        r#"[true,{"SITE":"plant-3","EMPTY":""},"plant-3",null]"#
    );
    // No files, whatever the path: `include` and `import` find nothing, as in EMQX.
    for program in ["include \"/etc/passwd\"; .", "import \"x\" as x; ."] {
        let error = open
            .eval(program.as_bytes(), Arg::Binary(b"1"), None)
            .expect_err(program);
        assert_eq!(error.tag, Tag::Compile);
        assert!(error.to_string().contains("module not found"), "{error}");
    }
}

/// The interpreter (for hosts that forbid executable memory) runs the same module to
/// the same answers as the compiled code, byte for byte: every case of the oracle
/// that EMQX answered within its timeout.
#[test]
fn interpreted_the_module_answers_exactly_as_compiled() {
    let grants = || Grants {
        clock: true,
        environment: oracle::environment(),
    };
    let mut compiled = jq();
    let mut interpreted =
        Jq::new_as(&module(), Execution::Interpreted, grants()).expect("the jq module loads");
    let show = |r: Result<Vec<u8>, JqError>| match r {
        Ok(o) => String::from_utf8_lossy(&o).into_owned(),
        Err(e) => e.to_string(),
    };
    let mut cases = 0;
    for case in oracle::parse(include_str!("emqx-jq-oracle.txt")) {
        let timed_out = matches!(&case.expected, Expected::Error { tag, .. } if tag == "timeout");
        // `now` reads the clock, and `input_line_number` nothing either run controls.
        let clock = String::from_utf8_lossy(&case.program).contains("now");
        if timed_out || clock {
            continue;
        }
        let timeout = Some(Duration::from_mins(2));
        let native = show(compiled.eval(&case.program, case.input_arg(), timeout));
        let portable = show(interpreted.eval(&case.program, case.input_arg(), timeout));
        assert_eq!(native, portable, "{}", case.describe());
        cases += 1;
    }
    assert!(cases >= 270, "{cases} cases compared");
    // And the limits hold there too.
    let started = Instant::now();
    let error = interpreted
        .eval(
            b"def f: f; f",
            Arg::Binary(b"1"),
            Some(Duration::from_millis(50)),
        )
        .expect_err("endless");
    assert_eq!(error.tag, Tag::Timeout);
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert!(started.elapsed() < Duration::from_secs(5));
    let error = interpreted
        .eval(b"\"x\" * 1e9", Arg::Binary(b"1"), Some(DEFAULT_TIMEOUT))
        .expect_err("too large");
    assert_eq!(error.tag, Tag::Limit);
    assert_eq!(
        text(interpreted.eval(b". + 1", Arg::Binary(b"1"), None)),
        "[2]"
    );
}

/// Programs and inputs nobody wrote: bytes and jq fragments drawn at random, the same
/// ones every run. Whatever they are, a call ends with outputs or one of the error
/// kinds within its timeout, and the caller's next call is served.
#[test]
#[allow(clippy::too_many_lines)] // the tables the programs are drawn from
fn garbage_programs_and_inputs_end_in_an_answer_or_an_error() {
    // Whole filters, which compile when joined, and pieces, which mostly do not.
    const FILTERS: [&str; 30] = [
        ".",
        ".a",
        ".[]",
        "..",
        ".[0]",
        "[.]",
        "{a: .}",
        "length",
        "keys",
        "add",
        "tostring",
        "tojson",
        "fromjson",
        "range(1e6)",
        "last(range(1e9))",
        "repeat(.)",
        "[repeat(.)]",
        "recurse",
        "def f: f; f",
        "test(\"(a*)*b\")",
        "\"a\" * 100000",
        "error",
        "try error catch .",
        "input",
        "$ENV",
        "now",
        "@base64",
        "1e1000",
        "nan",
        "limit(3; .[])",
    ];
    const JOINTS: [&str; 5] = [" | ", ", ", " + ", " // ", " as $x | "];
    const PIECES: [&str; 16] = [
        "|", ",", "[", "]", "{", "}", "(", ")", "\"", "\\", "$x", "reduce ", "foreach ", "try ",
        "catch ", "?",
    ];
    const INPUTS: [&str; 8] = [
        "1",
        "null",
        "\"x\"",
        "[1,[2,[3]]]",
        "{\"a\":{\"b\":[1,2,3]}}",
        "[[[[[[[[",
        "1e999",
        "\"\\ud800\"",
    ];
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut next = move |below: usize| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        usize::try_from(state % u64::try_from(below).expect("small")).expect("small")
    };
    let mut jq = jq();
    let mut kinds = std::collections::BTreeMap::new();
    for round in 0..400 {
        let mut program = Vec::new();
        let mut input = INPUTS[next(INPUTS.len())].as_bytes().to_vec();
        match round % 4 {
            0 => {
                // Bytes of any value, in the program and in the input.
                program.extend((0..next(40)).map(|_| u8::try_from(next(256)).expect("a byte")));
                input = (0..next(40))
                    .map(|_| u8::try_from(next(256)).expect("a byte"))
                    .collect();
            }
            1 => {
                for _ in 0..=next(8) {
                    let piece = if next(2) == 0 {
                        PIECES[next(PIECES.len())]
                    } else {
                        FILTERS[next(FILTERS.len())]
                    };
                    program.extend_from_slice(piece.as_bytes());
                }
            }
            _ => {
                program.extend_from_slice(FILTERS[next(FILTERS.len())].as_bytes());
                for _ in 0..next(4) {
                    program.extend_from_slice(JOINTS[next(JOINTS.len())].as_bytes());
                    program.extend_from_slice(FILTERS[next(FILTERS.len())].as_bytes());
                }
            }
        }
        let timeout = Duration::from_millis(10);
        let started = Instant::now();
        let outcome = jq.eval(&program, Arg::Binary(&input), Some(timeout));
        let took = started.elapsed();
        let shown = String::from_utf8_lossy(&program).into_owned();
        // Compiling a program is outside the timeout, as in EMQX, and takes a
        // millisecond or two; ten seconds is far beyond both.
        assert!(took < Duration::from_secs(10), "{shown:?} took {took:?}");
        let kind = match &outcome {
            Ok(_) => "outputs",
            Err(e) => e.tag.as_str(),
        };
        *kinds.entry(kind).or_insert(0u32) += 1;
        assert_eq!(
            text(jq.eval(b". + 1", Arg::Binary(b"1"), None)),
            "[2]",
            "after {shown:?}"
        );
    }
    // The run met every way a call ends but the one that means jq itself broke.
    for kind in [
        "outputs",
        "jq_err_compile",
        "jq_err_parse",
        "jq_err_process",
        "timeout",
    ] {
        assert!(kinds.contains_key(kind), "no {kind} among {kinds:?}");
    }
    assert!(!kinds.contains_key("jq_err_system"), "{kinds:?}");
}

/// What one of the pool test's threads does: twenty-five answers of its own, and one
/// program that never ends.
fn pool_caller(pool: &Pool, thread: u32) {
    for n in 0..25u32 {
        let input = format!("{}", thread * 1000 + n);
        let outputs = pool.eval(b". * 2", Arg::Binary(input.as_bytes()), None);
        assert_eq!(text(outputs), format!("[{}]", (thread * 1000 + n) * 2));
    }
    let endless = pool.eval(
        b"def f: f; f",
        Arg::Binary(b"1"),
        Some(Duration::from_millis(20)),
    );
    assert_eq!(endless.expect_err("endless").tag, Tag::Timeout);
}

/// The pool a broker holds: nothing is compiled until the first call, callers on
/// several threads share warm instances, and each gets its own answer.
#[test]
fn a_pool_serves_callers_on_several_threads() {
    let pool = Pool::new(
        module(),
        Execution::Compiled,
        Limits::default(),
        Grants::default(),
        4,
    );
    assert!(!pool.is_loaded());
    std::thread::scope(|scope| {
        for thread in 0..8u32 {
            let pool = &pool;
            scope.spawn(move || pool_caller(pool, thread));
        }
    });
    assert!(pool.is_loaded());
    assert!((1..=4).contains(&pool.idle()), "{} idle", pool.idle());
}

fn jq_under(max_call: Option<Duration>) -> Jq {
    let limits = Limits {
        max_call,
        ..Limits::default()
    };
    Jq::from_sandbox(Sandbox::load(&module(), limits, Grants::default()).expect("it loads"))
}

/// A module loaded with a ceiling ([`Limits::max_call`]) holds every call to it,
/// whatever timeout the call names: an hour, or none at all (EMQX's `infinity`, which
/// a timeout of 2^63 ms or more amounts to). The call runs under the shorter of the
/// two and a call stopped there fails as any timeout does, with EMQX's tag and text,
/// naming what it ran under. A timeout below the ceiling is the call's own, and a
/// program that ends in time answers exactly as it does without a ceiling.
#[test]
fn a_ceiling_holds_a_call_whatever_timeout_it_names() {
    // Half a minute of ceiling for the programs that end: no busy machine makes
    // them late, and every timeout below is above it.
    let mut roomy = jq_under(Some(Duration::from_secs(30)));
    let mut free = jq_under(None);
    for (program, input) in [
        (". + 1", "1"),
        (".a[] | select(. > 1)", r#"{"a":[1,2,3]}"#),
        ("[limit(1000; repeat(.))] | length", "7"),
    ] {
        for timeout in [Some(Duration::from_hours(1)), None] {
            assert_eq!(
                roomy.eval(program.as_bytes(), Arg::Binary(input.as_bytes()), timeout),
                free.eval(program.as_bytes(), Arg::Binary(input.as_bytes()), timeout),
                "{program} under {timeout:?}"
            );
        }
    }
    let ceiling = Duration::from_millis(80);
    let mut jq = jq_under(Some(ceiling));
    for (asked, ran_under) in [
        (Some(Duration::from_hours(1)), 80),
        (Some(Duration::from_millis(u64::MAX)), 80),
        (None, 80),
        (Some(ceiling), 80),
        (Some(Duration::from_millis(20)), 20),
    ] {
        for program in ["repeat(.)", "def f: f; f"] {
            let started = Instant::now();
            let error = jq
                .eval(program.as_bytes(), Arg::Binary(b"1"), asked)
                .expect_err(program);
            let took = started.elapsed();
            assert_eq!(
                (error.tag, error.kind),
                (Tag::Timeout, "timeout"),
                "{error}"
            );
            assert_eq!(
                String::from_utf8_lossy(&error.message),
                format!(
                    "jq program canceled as it took too long time to execute (timeout set to \
                     {ran_under} ms)"
                ),
                "{program} asked for {asked:?}"
            );
            assert!(
                took >= Duration::from_millis(ran_under),
                "{program} asked for {asked:?}: stopped early, after {took:?}"
            );
            assert!(
                took < Duration::from_millis(ran_under) + Duration::from_secs(5),
                "{program} asked for {asked:?}: stopped late, after {took:?}"
            );
        }
    }
    // One long C call that never looks at the cancel flag is stopped there as well.
    let stuck = br#""a" * 8000000 | test("a+b")"#;
    let error = jq
        .eval(stuck, Arg::Binary(b"1"), Some(Duration::from_hours(1)))
        .expect_err("stuck");
    assert_eq!(error.tag, Tag::Timeout);
    assert_eq!(text(jq.eval(b". + 1", Arg::Binary(b"1"), None)), "[2]");
}

/// The ceiling covers the whole call, and so the compiling of a program the instance
/// has not seen, which a timeout does not cover in EMQX or here (compiling has the
/// thirty seconds of `COMPILE_TIMEOUT` instead). A program that takes tens of
/// milliseconds to compile is stopped in the compiler under a ceiling of one
/// millisecond, and the error says which bound it met; without a ceiling it compiles
/// and runs.
///
/// One millisecond is there to stop the long compilation, not to time anything: no
/// assertion below depends on how fast this machine is. The call after it runs under
/// a ceiling it can meet.
#[test]
fn a_ceiling_covers_compiling_the_program() {
    // An array of six thousand additions: near the largest function jq compiles.
    let program = format!("[{}] | length", vec![".+1"; 6_000].join(","));
    let mut free = jq_under(None);
    let started = Instant::now();
    assert_eq!(
        text(free.eval(program.as_bytes(), Arg::Binary(b"1"), Some(DEFAULT_TIMEOUT))),
        "[6000]"
    );
    let compiling = started.elapsed();
    assert!(
        free.last.compile > Duration::from_millis(20),
        "the program was meant to take long to compile: {:?}",
        free.last.compile
    );
    let mut jq = jq_under(Some(Duration::from_millis(1)));
    let started = Instant::now();
    let error = jq
        .eval(
            program.as_bytes(),
            Arg::Binary(b"1"),
            Some(Duration::from_hours(1)),
        )
        .expect_err("the compiler runs into the ceiling");
    let took = started.elapsed();
    assert_eq!((error.tag, error.kind), (Tag::Limit, "compile_timeout"));
    assert_eq!(
        String::from_utf8_lossy(&error.message),
        "jq stopped: compiling the program ran into the 1 ms one call may take"
    );
    // Stopped by the ceiling and not by the compiler's own thirty seconds: a bound
    // generous enough that a stalled runner cannot miss it.
    assert!(
        took < COMPILE_TIMEOUT / 2,
        "compiling alone takes {compiling:?}; under a ceiling of 1 ms the call took {took:?}"
    );
    assert_eq!((jq.instances, jq.replaced), (1, 1));
    // The instance the compiler was stopped in is gone; the next call has a new one.
    // It is made under a ceiling a call can meet: even a one-character program takes
    // about a millisecond to compile, so one millisecond would stop this call too.
    let mut jq = jq_under(Some(Duration::from_secs(20)));
    let error = jq
        .eval(
            program.as_bytes(),
            Arg::Binary(b"1"),
            Some(Duration::from_hours(1)),
        )
        .map(|_| ())
        .err();
    assert_eq!(error.map(|e| e.kind), None, "twenty seconds are enough");
    assert_eq!(text(jq.eval(b".", Arg::Binary(b"1"), None)), "[1]");
    assert_eq!((jq.instances, jq.replaced), (1, 0));
}
