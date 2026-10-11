//! Conditions EMQX's runtime fails and this engine evaluates: a `WHERE`, an `INCASE`
//! and the `WHEN`s of a `CASE`.
//!
//! EMQX's grammar and its runtime disagree about what a condition is. The grammar takes
//! any expression after `WHERE`, `INCASE` and `WHEN`; the runtime's
//! `match_conditions/2` has clauses for `AND`, `OR`, `NOT`, `IN`, a function call, a
//! comparison and the constants `true` and `false`. So a rule whose condition is a
//! field or a number is created and then fails on each message that reaches it
//! (`function_clause`), and since `NOT` evaluates its operand with `eval/2`, which has
//! no clause for `AND`, `OR`, `NOT` or `IN`, so do `NOT (a AND b)`, `NOT NOT a` and
//! `x NOT IN (…)`, which the grammar reads as `NOT (x IN (…))`.
//!
//! This engine keeps evaluating those forms (ADR 0083, amendment of 2026-10-11: kept
//! as documented differences, since no rule that works in EMQX can hold one). Each row
//! here pins what mqttd gives, and carries what EMQX 6.3.1 gave for the same statement
//! and the same message (`emqx/emqx:latest`, rulesql 0.2.1; `emqx_rule_sqltester:test/1`
//! on topic `t/1` and payload [`PAYLOAD`]; counters from real rules and a publish: a
//! failure there is `failed` + `failed.exception`, a false condition `failed.no_result`).
//! A row marked [`SAME`] is one where the two agree, kept beside its neighbours to show
//! where the difference starts and stops.
//!
//! The one place where a rule that *works* in EMQX could tell the engines apart, the
//! evaluation order of `CASE x WHEN v`, is not a difference: it is EMQX's here too, and
//! pinned by `tests_state::a_case_operand_is_evaluated_after_each_when_as_emqx_does`.

use super::*;

/// The message every statement here runs on.
const PAYLOAD: &str = r#"{"flag":true,"f":false,"n":1,"z":0,"s":"a","nul":null,"t":"true","arr":[1,2,3],"m":{"k":1},"bools":[true,false,1]}"#;

/// What a statement gives for the message, in mqttd.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Want {
    /// The condition is true: `{"a":1}`.
    Pass,
    /// The condition is false: no output, counted `no_result`.
    No,
    /// These outputs.
    Out(&'static [&'static str]),
}
use Want::{No, Out, Pass};

/// What EMQX 6.3.1 gave for a row, as probed.
type Emqx = &'static str;
/// The same outcome as mqttd's.
const SAME: Emqx = "the same";
/// `function_clause` in `match_conditions/2`: a value where a condition belongs.
const VALUE: Emqx = "the rule fails: function_clause in match_conditions/2";
/// `function_clause` in `eval/2`: `AND`, `OR`, `NOT` or `IN` under a `NOT`, or as the
/// `WHEN` of a `CASE` with an operand.
const LOGIC: Emqx = "the rule fails: function_clause in eval/2";
/// `case_clause` where the `WHERE` or `INCASE` reads a result that is neither `true`
/// nor `false`, or `{badarg, V}` from `andalso`/`orelse` on such a left side.
const BOOL: Emqx = "the rule fails: case_clause or badarg, not a boolean";
/// EMQX runs the rule, with a different result: `TRUE` is a field name there.
const FIELD_NO: Emqx = "the condition is false: TRUE is a field, undefined";
const FIELD_PASS: Emqx = "the condition is true: TRUE is a field, undefined";

/// A field `a` that came out as `'y'` or `'n'`.
const Y: Want = Out(&[r#"{"a":"y"}"#]);
const N: Want = Out(&[r#"{"a":"n"}"#]);

fn run(sql: &str) -> Result<Vec<String>, String> {
    let payload = Bytes::from(PAYLOAD);
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c1", "t/1", &payload, 1, &props);
    test_sql(sql, &input)
}

/// Check every row, reporting all that differ at once. No row fails the rule in mqttd,
/// and none draws a warning when it is loaded.
fn check(rows: &[(&str, Want, Emqx)], sql_of: impl Fn(&str) -> String) {
    let mut wrong = Vec::new();
    for (text, want, emqx) in rows {
        let sql = sql_of(text);
        let got = run(&sql);
        let ok = match (&got, want) {
            (Ok(out), Pass) => out == &[r#"{"a":1}"#],
            (Ok(out), No) => out.is_empty(),
            (Ok(out), Out(w)) => out == w,
            (Err(_), _) => false,
        };
        if !ok {
            wrong.push(format!(
                "{sql}\n    want {want:?}\n    got  {got:?}\n    EMQX: {emqx}"
            ));
        }
        let warnings = check_sql(&sql).unwrap_or_else(|e| panic!("{sql} does not load: {e}"));
        if !warnings.is_empty() {
            wrong.push(format!("{sql}\n    warns: {warnings:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} differ from the kept behaviour:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

fn where_(cond: &str) -> String {
    format!("SELECT 1 AS a FROM \"t/#\" WHERE {cond}")
}

/// A bare value as the whole condition passes exactly when it is the boolean `true`.
/// EMQX creates the rule and fails it on every message, `true` or not.
#[test]
fn a_bare_value_as_a_condition_passes_only_when_it_is_true() {
    check(
        &[
            ("payload.flag", Pass, VALUE),
            ("(payload.flag)", Pass, VALUE),
            ("payload.f", No, VALUE),
            ("payload.missing", No, VALUE),
            ("flag", No, VALUE),
            ("null", No, VALUE),
            ("1", No, VALUE),
            ("(1)", No, VALUE),
            ("0", No, VALUE),
            ("'true'", No, VALUE),
            ("1.5", No, VALUE),
            ("payload.n + 1", No, VALUE),
            ("-payload.n", No, VALUE),
            ("[1,2]", No, VALUE),
            ("[1..2]", No, VALUE),
            ("payload.arr[1]", No, VALUE),
            ("payload.arr[1..2]", No, VALUE),
            // Compared, it is a condition in both.
            ("payload.flag = true", Pass, SAME),
            ("payload.flag IN (true)", Pass, SAME),
        ],
        where_,
    );
    // The `WHERE` reads the selection too, and a selected boolean is as bare.
    check(
        &[
            (
                r#"SELECT payload.flag AS flag FROM "t/#" WHERE flag"#,
                Out(&[r#"{"flag":true}"#]),
                VALUE,
            ),
            (
                r#"SELECT payload.flag AS flag FROM "t/#" WHERE flag = true"#,
                Out(&[r#"{"flag":true}"#]),
                SAME,
            ),
        ],
        str::to_string,
    );
}

/// A bare value as an operand of `AND` or `OR` reads as "not true" unless it is `true`.
/// EMQX's `andalso`/`orelse` fail the rule when they reach one: on the left always, on
/// the right only when the left side lets the right one run.
#[test]
fn a_bare_operand_of_and_or_or_reads_as_not_true_unless_it_is() {
    check(
        &[
            ("payload.flag AND payload.n = 1", Pass, VALUE),
            ("payload.n = 1 AND payload.flag", Pass, VALUE),
            ("(payload.n = 1) AND (payload.flag)", Pass, VALUE),
            ("payload.flag OR payload.n = 1", Pass, VALUE),
            ("payload.s OR payload.n = 1", Pass, VALUE),
            ("payload.n = 2 OR payload.flag", Pass, VALUE),
            ("payload.n = 1 AND 1", No, VALUE),
            ("1 AND payload.n = 1", No, VALUE),
            ("payload.n = 2 OR 1", No, VALUE),
            ("1 OR payload.n = 1", Pass, VALUE),
            ("false OR payload.flag", Pass, VALUE),
            ("true AND payload.flag", Pass, VALUE),
            // Not reached: EMQX does not fail these either.
            ("payload.n = 2 AND payload.flag", No, SAME),
            ("payload.n = 1 OR payload.flag", Pass, SAME),
            ("payload.n IN (1) OR payload.flag", Pass, SAME),
            ("payload.n = 2 AND 1", No, SAME),
            ("payload.n = 1 OR 1", Pass, SAME),
            ("true OR payload.flag", Pass, SAME),
            ("false AND payload.flag", No, SAME),
        ],
        where_,
    );
}

/// `x NOT IN (…)` is the negation of `IN`, and `NOT` takes `AND`, `OR` and another
/// `NOT`. EMQX's `NOT` evaluates its operand with `eval/2`, which has no clause for any
/// of the four: the rule fails when the `NOT` is reached, before any part of the
/// operand is evaluated.
#[test]
fn not_in_and_not_over_and_or_not_are_evaluated() {
    check(
        &[
            ("payload.n NOT IN (1, 2)", No, LOGIC),
            ("payload.n NOT IN (5, 6)", Pass, LOGIC),
            ("payload.n nOt iN (3)", Pass, LOGIC),
            ("NOT (payload.n IN (1, 2))", No, LOGIC),
            ("NOT payload.n IN (5, 6)", Pass, LOGIC),
            ("NOT NOT payload.flag", Pass, LOGIC),
            ("NOT (NOT payload.flag)", Pass, LOGIC),
            ("NOT (payload.n = 1 AND payload.z = 0)", No, LOGIC),
            ("NOT (payload.n = 2 OR payload.z = 1)", Pass, LOGIC),
            ("payload.n = 1 AND payload.s NOT IN ('x')", Pass, LOGIC),
            ("payload.n = 2 OR payload.s NOT IN ('x')", Pass, LOGIC),
            ("payload.s NOT IN ('x') OR payload.n = 1", Pass, LOGIC),
            ("NOT payload.n IN (1) AND payload.z = 0", No, LOGIC),
            (
                "payload.n = 1 AND NOT payload.n IN (5) OR payload.z = 0",
                Pass,
                LOGIC,
            ),
            // Not reached: EMQX does not fail these either.
            ("payload.n = 2 AND payload.s NOT IN ('x')", No, SAME),
            ("payload.n = 1 OR payload.s NOT IN ('x')", Pass, SAME),
            ("payload.z = 1 AND NOT payload.n IN (1)", No, SAME),
            (
                "payload.z = 0 OR NOT (payload.n = 1 AND payload.n = 1)",
                Pass,
                SAME,
            ),
        ],
        where_,
    );
}

/// The forms that give the same result in both engines, for a rule that has to run in
/// EMQX too (each probed there): `NOT contains(x, […])` for `x NOT IN (…)` (`contains`
/// is `lists:member/2`, exact as `IN` is, and a function call is something `NOT`
/// takes); De Morgan's for `NOT` over `AND`/`OR`; `v = true` or `v IN (true)` for a
/// bare `v`. `IN (true)` passes only the boolean, as the bare value does here; `= true`
/// also passes the string `'true'` (a boolean against a string compares their text).
#[test]
fn the_portable_forms_give_the_same_in_both_engines() {
    check(
        &[
            ("NOT contains(payload.s, ['x', 'y'])", Pass, SAME),
            ("NOT contains(payload.s, ['a', 'y'])", No, SAME),
            ("NOT contains(payload.missing, ['a', 'y'])", Pass, SAME),
            ("NOT contains(payload.n, [1.0])", Pass, SAME),
            ("contains(payload.s, ['a'])", Pass, SAME),
            ("payload.n IN (1.0)", No, SAME),
            ("NOT payload.n = 1 OR NOT payload.z = 0", No, SAME),
            ("NOT payload.n = 2 AND NOT payload.z = 1", Pass, SAME),
            ("payload.flag IN (true)", Pass, SAME),
            ("payload.t IN (true)", No, SAME),
            ("payload.missing IN (true)", No, SAME),
            ("payload.t = true", Pass, SAME),
            ("payload.flag = 'true'", Pass, SAME),
            ("payload.s = true", No, SAME),
            ("payload.n = true", No, SAME),
            ("payload.nul = true", No, SAME),
        ],
        where_,
    );
}

/// A function that does not return a boolean is "not true": the condition is false
/// (`no_result`). In EMQX the rule fails (`failed.exception`) when the result reaches
/// the `WHERE`'s `case … of true; false`, or the left side of an `AND`/`OR`. Where the
/// result is the same "no output", only the counter differs.
#[test]
fn a_function_that_does_not_return_a_boolean_reads_as_not_true() {
    check(
        &[
            ("abs(1)", No, BOOL),
            ("lower('A')", No, BOOL),
            ("nth(1, payload.arr)", No, BOOL),
            ("map_get('nope', payload.m)", No, BOOL),
            ("payload.n = 1 AND abs(1)", No, BOOL),
            ("abs(1) AND payload.n = 1", No, BOOL),
            ("abs(1) OR payload.n = 1", Pass, BOOL),
            ("payload.n = 2 OR abs(1)", No, BOOL),
            ("(payload.n = 1 AND abs(1)) OR payload.z = 0", Pass, BOOL),
            ("payload.n = 1 AND abs(1) AND payload.z = 0", No, BOOL),
            // Not reached, or a boolean: the same.
            ("payload.n = 2 AND abs(1)", No, SAME),
            ("payload.n = 1 OR abs(1)", Pass, SAME),
            ("is_bool(payload.flag) AND payload.n = 1", Pass, SAME),
            ("is_null(payload.n)", No, SAME),
        ],
        where_,
    );
}

/// `TRUE`, `True` and `FALSE` are the booleans, as `true` and `false` are. EMQX's lexer
/// has the two words in lower case only, so any other spelling is a field name there,
/// which no message has: `undefined`. The one form here where EMQX runs the rule and
/// gives a different result, instead of failing it.
#[test]
fn true_and_false_are_booleans_in_any_case() {
    check(
        &[
            ("TRUE", Pass, VALUE),
            ("payload.flag = TRUE", Pass, FIELD_NO),
            ("payload.missing = TRUE", No, FIELD_PASS),
            // `NOT` of `undefined` is false in EMQX, as `NOT true` is.
            ("NOT TRUE", No, SAME),
            ("payload.flag = true", Pass, SAME),
        ],
        where_,
    );
    // EMQX: {"a":"undefined","b":"undefined","c":true,"d":"undefined","e":false}.
    assert_eq!(
        run(r#"SELECT TRUE AS a, True AS b, true AS c, FALSE AS d, false AS e FROM "t/#""#)
            .unwrap(),
        [r#"{"a":true,"b":true,"c":true,"d":false,"e":false}"#]
    );
}

/// A `CASE WHEN` takes its branch exactly when the condition is the boolean `true`, and
/// goes on to the next `WHEN` otherwise. EMQX's `eval_case_clauses` goes on too for a
/// function's number or string, but its `match_conditions/2` still fails the rule on a
/// bare value and on `NOT` over `AND`, `OR`, `NOT` and `IN`, in the `WHEN` reached.
#[test]
fn a_case_when_takes_any_expression_and_goes_on_unless_it_is_true() {
    let case =
        |cond: &str| format!("SELECT CASE WHEN {cond} THEN 'y' ELSE 'n' END AS a FROM \"t/#\"");
    check(
        &[
            ("payload.flag", Y, VALUE),
            ("(payload.flag)", Y, VALUE),
            ("payload.f", N, VALUE),
            ("payload.missing", N, VALUE),
            ("1", N, VALUE),
            ("'true'", N, VALUE),
            ("payload.n + 1", N, VALUE),
            ("payload.flag AND payload.n = 1", Y, VALUE),
            ("payload.n = 1 AND payload.flag", Y, VALUE),
            ("payload.n = 2 OR payload.flag", Y, VALUE),
            ("payload.n NOT IN (5, 6)", Y, LOGIC),
            ("payload.n NOT IN (1, 6)", N, LOGIC),
            ("NOT (payload.n IN (5, 6))", Y, LOGIC),
            ("NOT (payload.n = 1 AND payload.z = 0)", N, LOGIC),
            ("NOT NOT payload.flag", Y, LOGIC),
            ("abs(1) AND payload.n = 1", N, BOOL),
            ("abs(1) OR payload.n = 1", Y, BOOL),
            // A function's number as the whole `WHEN`, or on the right: EMQX goes on too.
            ("abs(1)", N, SAME),
            ("payload.n = 1 AND abs(1)", N, SAME),
            ("payload.n = 2 OR abs(1)", N, SAME),
            ("payload.n = 2 AND payload.flag", N, SAME),
            ("payload.n = 1 OR payload.flag", Y, SAME),
            ("NOT payload.f", Y, SAME),
            ("NOT payload.n", N, SAME),
        ],
        case,
    );
    check(
        &[
            (
                r#"SELECT CASE WHEN payload.n = 2 THEN 'a' WHEN payload.flag THEN 'b' ELSE 'n' END AS a FROM "t/#""#,
                Out(&[r#"{"a":"b"}"#]),
                VALUE,
            ),
            // A `WHEN` after the one taken is not reached in either.
            (
                r#"SELECT CASE WHEN payload.n = 1 THEN 'a' WHEN payload.flag THEN 'b' ELSE 'n' END AS a FROM "t/#""#,
                Out(&[r#"{"a":"a"}"#]),
                SAME,
            ),
        ],
        str::to_string,
    );
}

/// `CASE x WHEN v` matches `x` against the value of `v`, and here `AND`, `OR`, `NOT` and
/// `IN` are values (booleans) like any other. EMQX evaluates `v` with `eval/2`, where a
/// comparison is a value and those four fail the rule, in the `WHEN` reached.
#[test]
fn a_case_with_an_operand_takes_a_logical_when_as_a_boolean() {
    let case = |rest: &str| format!("SELECT CASE {rest} END AS a FROM \"t/#\"");
    check(
        &[
            (
                "payload.flag WHEN payload.n IN (1) THEN 'y' ELSE 'n'",
                Y,
                LOGIC,
            ),
            (
                "payload.flag WHEN NOT payload.f THEN 'y' ELSE 'n'",
                Y,
                LOGIC,
            ),
            (
                "payload.flag WHEN payload.n = 1 AND payload.z = 0 THEN 'y' ELSE 'n'",
                Y,
                LOGIC,
            ),
            (
                "payload.n WHEN payload.n IN (1) THEN 'y' WHEN 1 THEN 'z' ELSE 'n'",
                Out(&[r#"{"a":"z"}"#]),
                LOGIC,
            ),
            // A comparison is a value in both, and a `WHEN` after the match is not
            // reached in either.
            ("payload.flag WHEN payload.n = 1 THEN 'y' ELSE 'n'", Y, SAME),
            ("payload.n WHEN payload.n = 1 THEN 'y' ELSE 'n'", N, SAME),
            (
                "payload.n WHEN 1 THEN 'z' WHEN payload.n IN (1) THEN 'y' ELSE 'n'",
                Out(&[r#"{"a":"z"}"#]),
                SAME,
            ),
        ],
        case,
    );
}

/// An `INCASE` is a `WHERE` per element, with the same reading: an element passes
/// exactly when the condition is the boolean `true`. EMQX fails the rule at the first
/// element whose `INCASE` is a bare value, a `NOT IN`, or a function's number.
#[test]
fn an_incase_reads_its_condition_as_a_where_does() {
    check(
        &[
            (
                r#"FOREACH payload.bools DO item AS v INCASE item FROM "t/#""#,
                Out(&[r#"{"v":true}"#]),
                VALUE,
            ),
            (
                r#"FOREACH payload.arr DO item AS v INCASE item FROM "t/#""#,
                No,
                VALUE,
            ),
            (
                r#"FOREACH payload.arr DO item AS v INCASE 1 FROM "t/#""#,
                No,
                VALUE,
            ),
            (
                r#"FOREACH payload.arr DO item AS v INCASE item = 1 OR item FROM "t/#""#,
                Out(&[r#"{"v":1}"#]),
                VALUE,
            ),
            (
                r#"FOREACH [] DO item AS v FROM "t/#" WHERE payload.flag"#,
                No,
                VALUE,
            ),
            (
                r#"FOREACH payload.bools DO item AS v INCASE item NOT IN (false) FROM "t/#""#,
                Out(&[r#"{"v":true}"#, r#"{"v":1}"#]),
                LOGIC,
            ),
            (
                r#"FOREACH payload.bools DO item AS v INCASE NOT (item IN (false)) FROM "t/#""#,
                Out(&[r#"{"v":true}"#, r#"{"v":1}"#]),
                LOGIC,
            ),
            (
                r#"FOREACH payload.arr DO item AS v INCASE item > 2 OR item NOT IN (1) FROM "t/#""#,
                Out(&[r#"{"v":2}"#, r#"{"v":3}"#]),
                LOGIC,
            ),
            (
                r#"FOREACH payload.arr DO item AS v INCASE abs(item) FROM "t/#""#,
                No,
                BOOL,
            ),
            (
                r#"FOREACH payload.arr DO item AS v INCASE item > 1 AND abs(item) FROM "t/#""#,
                No,
                BOOL,
            ),
            (
                r#"FOREACH payload.arr DO CASE WHEN item THEN 1 ELSE 2 END AS v FROM "t/#""#,
                Out(&[r#"{"v":2}"#, r#"{"v":2}"#, r#"{"v":2}"#]),
                VALUE,
            ),
            // No element to try, or a `WHERE` that turns the message away first: the
            // `INCASE` is never looked at, in either.
            (
                r#"FOREACH [] DO item AS v INCASE item FROM "t/#""#,
                No,
                SAME,
            ),
            (
                r#"FOREACH payload.arr DO item AS v INCASE item NOT IN (1) FROM "t/#" WHERE payload.n = 2"#,
                No,
                SAME,
            ),
            (
                r#"FOREACH payload.bools DO item AS v INCASE item = true FROM "t/#""#,
                Out(&[r#"{"v":true}"#]),
                SAME,
            ),
            (
                r#"FOREACH payload.bools DO item AS v INCASE NOT item FROM "t/#""#,
                Out(&[r#"{"v":false}"#]),
                SAME,
            ),
        ],
        str::to_string,
    );
}
