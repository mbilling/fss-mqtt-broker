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
    check_rows(rows, sql_of, false);
}

/// The warning a statement draws for each spelling of `TRUE` or `FALSE` it reads as a
/// value.
const BOOLEAN_NAME: &str = "is a field name here, as in EMQX; write `";

/// [`check`], for rows that read a field spelt like a boolean: each draws that warning
/// when it is loaded, and no other.
fn check_warned(rows: &[(&str, Want, Emqx)], sql_of: impl Fn(&str) -> String) {
    check_rows(rows, sql_of, true);
}

fn check_rows(rows: &[(&str, Want, Emqx)], sql_of: impl Fn(&str) -> String, warned: bool) {
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
        let as_wanted = if warned {
            !warnings.is_empty() && warnings.iter().all(|w| w.contains(BOOLEAN_NAME))
        } else {
            warnings.is_empty()
        };
        if !as_wanted {
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

/// Only `true` and `false` in lower case are the booleans. EMQX's parser makes the
/// constant of those two words alone (`unwrap_var/1` in `rulesql.yrl`), so `TRUE`,
/// `True` and `FALSE` are names like `payload`: a field, which no message has, so
/// `undefined`. It is the same here (ADR 0083, second amendment of 2026-10-11), in every
/// place a value is read; each row is EMQX 6.3.1's outcome for the same statement. So
/// `payload.flag = TRUE` is false for a message whose `flag` is `true`, and true for one
/// without `flag`. Where EMQX fails the rule, the bare field is a value where a
/// condition belongs, as for any other field.
#[test]
fn true_and_false_in_capitals_are_field_names_as_in_emqx() {
    check_warned(
        &[
            ("TRUE", No, VALUE),
            ("FALSE", No, VALUE),
            // `NOT` of `undefined` is false in EMQX, as it is of any non-boolean.
            ("NOT TRUE", No, SAME),
            ("NOT FALSE", No, SAME),
            ("payload.flag = TRUE", No, SAME),
            ("payload.f = FALSE", No, SAME),
            ("payload.flag != TRUE", No, SAME),
            // `undefined` equals `undefined`: the messages *without* the field pass.
            ("payload.missing = TRUE", Pass, SAME),
            ("payload.missing = False", Pass, SAME),
            ("TRUE = FALSE", Pass, SAME),
            ("TRUE = true", No, SAME),
            ("payload.n = 1 AND TRUE", No, VALUE),
            ("payload.n = 2 AND TRUE", No, SAME),
            ("payload.n = 1 OR TRUE", Pass, SAME),
            ("payload.flag IN (TRUE)", No, SAME),
            ("payload.flag IN (TRUE, true)", Pass, SAME),
            ("payload.missing IN (TRUE)", Pass, SAME),
            ("TRUE IN (payload.missing)", Pass, SAME),
        ],
        where_,
    );
    // Where a value of the wrong type fails the rule, `undefined` does, in both:
    // `bool(TRUE)` is `badarg` in EMQX, `-TRUE` is `badarith`.
    for sql in [
        r#"SELECT bool(TRUE) AS a FROM "t/#""#,
        r#"SELECT -TRUE AS a FROM "t/#""#,
        r#"SELECT TRUE + 1 AS a FROM "t/#""#,
    ] {
        let failed = run(sql).expect_err(sql);
        assert!(failed.contains("undefined"), "{sql}: {failed}");
    }
}

/// The same reading in every other place a value is read, each with EMQX 6.3.1's
/// outcome: a `SELECT` field, a `CASE` (its condition, branches, operand and a `WHEN`'s
/// value), a `FOREACH` (its array, its `DO` and its `INCASE`), a function's argument, an
/// array's element and a computed index.
#[test]
#[allow(clippy::too_many_lines)] // one row per probed position
fn true_and_false_in_capitals_are_fields_wherever_a_value_is_read() {
    let undefined3: Want = Out(&[
        r#"{"v":"undefined"}"#,
        r#"{"v":"undefined"}"#,
        r#"{"v":"undefined"}"#,
    ]);
    let false3: Want = Out(&[r#"{"v":false}"#, r#"{"v":false}"#, r#"{"v":false}"#]);
    check_warned(
        &[
            // A field of a `SELECT`: any spelling but the lower-case one.
            (
                r#"SELECT TRUE AS a, True AS b, true AS c, FALSE AS d, false AS e, tRUE AS f, False AS g FROM "t/#""#,
                Out(&[
                    r#"{"a":"undefined","b":"undefined","c":true,"d":"undefined","e":false,"f":"undefined","g":"undefined"}"#,
                ]),
                SAME,
            ),
            // Unaliased, it is stored under its own name, as a field is.
            (
                r#"SELECT TRUE, FALSE FROM "t/#""#,
                Out(&[r#"{"TRUE":"undefined","FALSE":"undefined"}"#]),
                SAME,
            ),
            // A `CASE`: its condition, its branches, its operand and a `WHEN`'s value.
            (
                r#"SELECT CASE WHEN TRUE THEN 'y' ELSE 'n' END AS a FROM "t/#""#,
                N,
                VALUE,
            ),
            (
                r#"SELECT CASE WHEN payload.flag = TRUE THEN 'y' ELSE 'n' END AS a FROM "t/#""#,
                N,
                SAME,
            ),
            (
                r#"SELECT CASE WHEN payload.n = 1 THEN TRUE ELSE FALSE END AS a FROM "t/#""#,
                Out(&[r#"{"a":"undefined"}"#]),
                SAME,
            ),
            (
                r#"SELECT CASE WHEN payload.n = 2 THEN 1 ELSE FALSE END AS a FROM "t/#""#,
                Out(&[r#"{"a":"undefined"}"#]),
                SAME,
            ),
            (
                r#"SELECT CASE payload.flag WHEN TRUE THEN 'y' ELSE 'n' END AS a FROM "t/#""#,
                N,
                SAME,
            ),
            (
                r#"SELECT CASE payload.missing WHEN TRUE THEN 'y' ELSE 'n' END AS a FROM "t/#""#,
                Y,
                SAME,
            ),
            (
                r#"SELECT CASE TRUE WHEN true THEN 'y' ELSE 'n' END AS a FROM "t/#""#,
                N,
                SAME,
            ),
            (
                r#"SELECT CASE TRUE WHEN payload.missing THEN 'y' ELSE 'n' END AS a FROM "t/#""#,
                Y,
                SAME,
            ),
            // A `FOREACH`: its array, its `DO` and its `INCASE`.
            (r#"FOREACH TRUE FROM "t/#""#, No, SAME),
            (
                r#"FOREACH payload.bools DO TRUE AS v FROM "t/#""#,
                undefined3,
                SAME,
            ),
            (
                r#"FOREACH payload.bools DO item = TRUE AS v FROM "t/#""#,
                false3,
                SAME,
            ),
            (
                r#"FOREACH payload.bools DO item AS v INCASE item = TRUE FROM "t/#""#,
                No,
                SAME,
            ),
            (
                r#"FOREACH payload.bools DO item AS v INCASE TRUE FROM "t/#""#,
                No,
                VALUE,
            ),
            // A function's argument, an array's element and a computed index.
            (
                r#"SELECT is_bool(TRUE) AS a, is_bool(true) AS b, is_null(TRUE) AS c, is_null(FALSE) AS d FROM "t/#""#,
                Out(&[r#"{"a":false,"b":true,"c":true,"d":true}"#]),
                SAME,
            ),
            (
                r#"SELECT str(TRUE) AS a, str(true) AS b FROM "t/#""#,
                Out(&[r#"{"a":"undefined","b":"true"}"#]),
                SAME,
            ),
            (
                r#"SELECT coalesce(TRUE, 'x') AS a FROM "t/#""#,
                Out(&[r#"{"a":"x"}"#]),
                SAME,
            ),
            (
                r#"SELECT [TRUE, true] AS a FROM "t/#""#,
                Out(&[r#"{"a":["undefined",true]}"#]),
                SAME,
            ),
            (
                r#"SELECT payload.arr[TRUE] AS a FROM "t/#""#,
                Out(&[r#"{"a":"undefined"}"#]),
                SAME,
            ),
        ],
        str::to_string,
    );
}

/// Where `TRUE` can only be a name, it was one before and nothing is said: an alias, a
/// path's head or segment, and a quoted identifier. A statement that reads an alias it
/// selected itself is not warned about either. Each row is EMQX 6.3.1's outcome too.
#[test]
fn true_and_false_as_an_alias_a_path_or_a_quoted_name_draw_no_warning() {
    check(
        &[
            (
                r#"SELECT 1 AS TRUE FROM "t/#""#,
                Out(&[r#"{"TRUE":1}"#]),
                SAME,
            ),
            (r#"SELECT 1 TRUE FROM "t/#""#, Out(&[r#"{"TRUE":1}"#]), SAME),
            (
                r#"SELECT 1 AS TRUE FROM "t/#" WHERE TRUE = 1"#,
                Out(&[r#"{"TRUE":1}"#]),
                SAME,
            ),
            (
                r#"SELECT payload.n AS TRUE.x FROM "t/#""#,
                Out(&[r#"{"TRUE":{"x":1}}"#]),
                SAME,
            ),
            (
                r#"SELECT payload.n AS x.TRUE FROM "t/#""#,
                Out(&[r#"{"x":{"TRUE":1}}"#]),
                SAME,
            ),
            (
                r#"SELECT TRUE.x AS a, payload.TRUE AS b, TRUE[1] AS c FROM "t/#""#,
                Out(&[r#"{"a":"undefined","b":"undefined","c":"undefined"}"#]),
                SAME,
            ),
            (
                r#"SELECT "TRUE" AS a, "true" AS b, 'true' AS c FROM "t/#""#,
                Out(&[r#"{"a":"undefined","b":"undefined","c":"true"}"#]),
                SAME,
            ),
        ],
        str::to_string,
    );
}

/// Names only mqttd takes, probed with the rest: lower-case `true` and `false` where a
/// name belongs, and `if` and `elsif`, which EMQX's lexer reserves and its grammar never
/// uses. EMQX does not create a rule with `payload.true`, `true.x`, `AS x.true`, `if` or
/// `payload.if` (its parser fails), and creates one with `AS true` that fails on every
/// message (`nested_put` has no clause for a constant as a key). Here each is the name
/// it reads as, so no rule that works in EMQX is affected.
#[test]
fn lower_case_booleans_and_if_are_names_where_emqx_takes_no_name() {
    check(
        &[
            (
                r#"SELECT 1 AS true FROM "t/#""#,
                Out(&[r#"{"true":1}"#]),
                "the rule fails: function_clause in emqx_rule_maps:nested_put/3",
            ),
            (
                r#"SELECT payload.n AS x.true FROM "t/#""#,
                Out(&[r#"{"x":{"true":1}}"#]),
                "not created: the parser fails",
            ),
            (
                r#"SELECT payload.true AS a, true.x AS b, true[1] AS c, payload.div AS d FROM "t/#""#,
                Out(&[r#"{"a":"undefined","b":"undefined","c":"undefined","d":"undefined"}"#]),
                "not created: the parser fails",
            ),
            (
                r#"SELECT if AS a, ELSIF AS b, payload.if AS c, 1 AS if FROM "t/#""#,
                Out(&[r#"{"a":"undefined","b":"undefined","c":"undefined","if":1}"#]),
                "not created: IF and ELSIF are reserved words",
            ),
        ],
        str::to_string,
    );
}

/// The warning names the word as written and the boolean to write instead, once per
/// spelling however often the statement reads it, and comes with the rule's id through
/// a load's warnings (`--check-rules`, a load, a reload and the admin API show those).
/// No other name draws it: not `true` and `false` themselves, not `null`, `NULL` or
/// `undefined` (names in EMQX in any case, and here), not a longer name.
#[test]
fn a_field_spelt_like_a_boolean_is_warned_about_once_and_no_other_name_is() {
    assert_eq!(
        check_sql(r#"SELECT TRUE AS a FROM "t/#" WHERE payload.x = TRUE OR payload.y = False"#)
            .unwrap(),
        [
            "`TRUE` is a field name here, as in EMQX; write `true` for the boolean",
            "`False` is a field name here, as in EMQX; write `false` for the boolean",
        ]
    );
    let loaded = RuleSet::parse(
        r#"
        [rules.caps]
        sql = 'SELECT payload.v AS v FROM "t/#" WHERE payload.ok = TRUE'
        [rules.lower]
        sql = 'SELECT payload.v AS v FROM "t/#" WHERE payload.ok = true'
        "#,
    )
    .unwrap();
    assert_eq!(
        loaded.warnings,
        ["rule `caps`: `TRUE` is a field name here, as in EMQX; write `true` for the boolean"]
    );
    assert_eq!(loaded.rules.warnings(), loaded.warnings.as_slice());
    for sql in [
        r#"SELECT true AS a, false AS b FROM "t/#" WHERE payload.ok = true"#,
        r#"SELECT null AS a, NULL AS b, Null AS c, undefined AS d, UNDEFINED AS e FROM "t/#""#,
        r#"SELECT truth AS a, TRUEISH AS b, FALSEHOOD AS c, T AS d, F AS e, yes AS f FROM "t/#""#,
        r#"SELECT payload.flag AS flag FROM "t/#" WHERE flag = payload.flag AND 'TRUE' = 'TRUE'"#,
        r#"SELECT upper(payload.s) AS TRUE FROM "t/#" WHERE TRUE = 'A'"#,
        r#"FOREACH payload.arr AS FALSE DO FALSE AS v INCASE FALSE > 2 FROM "t/#""#,
    ] {
        assert_eq!(check_sql(sql).unwrap(), [""; 0], "{sql}");
    }
    // `null` and `undefined` are fields in any case too, in both engines: `undefined`,
    // which equals a missing field and not a JSON `null`.
    check(
        &[
            ("payload.missing = null", Pass, SAME),
            ("payload.missing = UNDEFINED", Pass, SAME),
            ("payload.nul = null", No, SAME),
            ("payload.nul = NULL", No, SAME),
            ("payload.missing = 'undefined'", No, SAME),
        ],
        where_,
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
