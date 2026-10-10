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
//! `--release`: the interpreter is an order of magnitude slower without optimisation,
//! and two of the oracle's cases run into EMQX's ten-second timeout on purpose.
//! `MQTTD_JQ_WASM` names another module file than `jq-module/jq.wasm`.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use mqtt_rules::{json_decode, Value};
use mqtt_wasm_sandbox::jq::{Jq, JqError, Tag, COMPILE_FUEL, DEFAULT_TIMEOUT};
use mqtt_wasm_sandbox::{Arg, CallError, Grants, Limits, Sandbox};

mod oracle;
use oracle::Verdict;

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
    let mut jq = jq();
    assert_eq!(text(jq.eval(b". + 1", Arg::Binary(b"1"), None)), "[2]");
    for (program, ms) in [
        ("def f: f; f", 100),
        ("repeat(.)", 100),
        ("[repeat(.)]", 200),
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
    let sandbox =
        Sandbox::load(&module(), Limits::default(), Grants::default()).expect("the module loads");
    let run = |program: &[u8]| {
        let mut instance = sandbox.instantiate().expect("an instance");
        instance
            .call_with_fuel("jq_compile", &[Arg::Binary(program)], COMPILE_FUEL)
            .expect("it compiles");
        let deadline = Instant::now() + Duration::from_millis(30);
        let outcome = instance.call(
            "jq",
            &[Arg::Binary(program), Arg::Binary(b"1")],
            Some(deadline),
        );
        (outcome, instance.is_poisoned())
    };
    let (outcome, poisoned) = run(b"def f: f; f");
    assert!(
        matches!(outcome, Err(CallError::Module { status: 7, .. })),
        "{outcome:?}"
    );
    assert!(!poisoned);
    let stuck = br#""a" * 8000000 | test("a+b")"#;
    let (outcome, poisoned) = run(stuck);
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
